using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Server.Runtime;

/// <summary>Outcome of a successful client-side handshake.</summary>
/// <param name="Challenge">The server's challenge (cluster identity, server identity, negotiated version and features).</param>
/// <param name="Result">The server's accepting result.</param>
/// <param name="ServerIdentity">Authenticated identity of the server (also stored on the connection).</param>
internal sealed record HandshakeOutcome(HelloChallengeMessage Challenge, HelloResultMessage Result, RpcPeerIdentity ServerIdentity);

/// <summary>Identity a node presents when it opens a connection to another node.</summary>
/// <param name="ExpectedClusterId">Cluster the node belongs to (<see cref="Guid.Empty"/> while seed-joining).</param>
/// <param name="Name">Node name.</param>
/// <param name="NodeId">Node id.</param>
/// <param name="Incarnation">Current incarnation.</param>
/// <param name="BootId">Process boot id.</param>
/// <param name="NodeAuthKey">HKDF-derived node authentication key (proves knowledge of the cluster secret).</param>
/// <param name="Features">Features this node's release supports.</param>
/// <param name="ProtocolMin">Lowest protocol version this node's release negotiates.</param>
/// <param name="ProtocolMax">Highest protocol version this node's release negotiates.</param>
internal sealed record NodeCredentials(Guid ExpectedClusterId, string Name, NodeId NodeId, ulong Incarnation, UInt128 BootId,
    byte[] NodeAuthKey, IReadOnlySet<string> Features, int ProtocolMin = ProtocolInfo.MinSupportedVersion, int ProtocolMax = ProtocolInfo.CurrentVersion)
{
    /// <summary>
    /// The certificate this node's transport listener presents (QUIC), declared in the hello so that a pinning peer can
    /// refuse a node with an unknown or expired certificate also on connections that node opens (SEC-001); null without one.
    /// </summary>
    public RpcCertificateInfo? TransportCertificate { get; init; }
}

/// <summary>
/// Peer side of the connection handshake (PROTOCOL.md §3): <c>Hello → HelloChallenge → HelloProof → HelloResult</c>,
/// for node peers (HKDF node key, label-separated MACs) and principals (SCRAM-SHA-256 key schedule). The server's
/// cluster identity and mutual-authentication MAC are verified before the connection is used.
/// </summary>
internal static class HandshakeClient
{
    /// <summary>Authenticates this node to the server at the other end of <paramref name="connection"/>.</summary>
    /// <param name="connection">A fresh connection.</param>
    /// <param name="credentials">This node's identity and key.</param>
    /// <param name="expectedPeer">Node the endpoint is believed to belong to (null for seeds and leader hints).</param>
    /// <param name="random">Nonce source.</param>
    /// <param name="timeout">Per-request timeout.</param>
    /// <param name="ct">Cancellation.</param>
    /// <exception cref="DfsClusterIdentityException">The server belongs to another cluster.</exception>
    /// <exception cref="DfsAuthenticationException">Authentication failed in either direction.</exception>
    /// <exception cref="DfsException">The server rejected the node (for example AccessDenied or StaleIncarnation).</exception>
    public static async Task<HandshakeOutcome> AuthenticateNodeAsync(IRpcConnection connection, NodeCredentials credentials, NodeId? expectedPeer,
        IRandomSource random, TimeSpan timeout, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(credentials);
        var hello = new HelloMessage(credentials.ExpectedClusterId, (ushort)credentials.ProtocolMin, (ushort)credentials.ProtocolMax, credentials.Features,
            PeerRole.Node, credentials.Name, credentials.NodeId, credentials.Incarnation, null, random.NextBytes(ProtocolInfo.NonceSize), credentials.BootId);
        if (credentials.TransportCertificate is { } certificate)
            hello = hello.WithTransportCertificate(certificate.SpkiSha256, certificate.NotBefore, certificate.NotAfter);
        byte[] helloFrame = MessageCodec.Encode(hello);
        (HelloChallengeMessage challenge, byte[] challengeFrame) = await ExchangeHelloAsync(connection, helloFrame, credentials.ExpectedClusterId, timeout, ct);
        if (challenge.ServerRole != PeerRole.Node)
            throw new DfsAuthenticationException($"The server at {connection.Remote} is not a live node ({challenge.ServerRole}).");
        if (expectedPeer is { } peer && challenge.ServerNodeId != peer)
            throw new DfsAuthenticationException($"The server at {connection.Remote} is {challenge.ServerName}, not the expected node {peer}.");

        byte[] transcript = HandshakeAuth.ComputeTranscript(helloFrame, challengeFrame);
        byte[] proof = HandshakeAuth.ComputeNodeProof(credentials.NodeAuthKey, transcript);
        HelloResultMessage result = await ExchangeProofAsync(connection, proof, timeout, ct);
        if (!HandshakeAuth.VerifyNodeServerMac(credentials.NodeAuthKey, transcript, result.ServerMac.Span))
            throw new DfsAuthenticationException($"The server at {connection.Remote} failed mutual authentication.");

        var identity = new RpcPeerIdentity(challenge.ServerName, challenge.ServerNodeId, challenge.ServerIncarnation, null, IsNode: true, IsBackup: false);
        connection.Identity = identity;
        return new HandshakeOutcome(challenge, result, identity);
    }

    /// <summary>Authenticates a principal (client role, or a backup node presenting principal credentials).</summary>
    /// <exception cref="DfsClusterIdentityException">The server belongs to another cluster than <paramref name="expectedClusterId"/>.</exception>
    /// <exception cref="DfsAuthenticationException">Authentication failed in either direction.</exception>
    public static async Task<HandshakeOutcome> AuthenticatePrincipalAsync(IRpcConnection connection, Guid expectedClusterId, string clientName,
        DfsCredentials credentials, IRandomSource random, TimeSpan timeout, PeerRole role = PeerRole.Client, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(credentials);
        if (role == PeerRole.Node)
            throw new ArgumentException("Principals authenticate as clients or backup nodes.", nameof(role));
        var hello = new HelloMessage(expectedClusterId, ProtocolInfo.MinSupportedVersion, ProtocolInfo.CurrentVersion, ProtocolFeatures.Supported,
            role, clientName, null, 0, credentials.Principal, random.NextBytes(ProtocolInfo.NonceSize));
        byte[] helloFrame = MessageCodec.Encode(hello);
        (HelloChallengeMessage challenge, byte[] challengeFrame) = await ExchangeHelloAsync(connection, helloFrame, expectedClusterId, timeout, ct);
        if (challenge.Pbkdf2Iterations < 1 || challenge.Pbkdf2Iterations > HandshakeAuth.MaxIterations)
            throw new DfsAuthenticationException($"The server at {connection.Remote} asked for an unacceptable PBKDF2 iteration count.");

        byte[] transcript = HandshakeAuth.ComputeTranscript(helloFrame, challengeFrame);
        ClientKeys keys = HandshakeAuth.DeriveClientKeys(credentials.Secret, challenge.PrincipalSalt.Span, challenge.Pbkdf2Iterations);
        byte[] proof = HandshakeAuth.ComputeClientProof(keys, transcript);
        HelloResultMessage result = await ExchangeProofAsync(connection, proof, timeout, ct);
        if (!HandshakeAuth.VerifyServerMac(keys, transcript, result.ServerMac.Span))
            throw new DfsAuthenticationException($"The server at {connection.Remote} failed mutual authentication.");

        var identity = new RpcPeerIdentity(challenge.ServerName, challenge.ServerNodeId, challenge.ServerIncarnation, null,
            IsNode: challenge.ServerRole == PeerRole.Node, IsBackup: challenge.ServerRole == PeerRole.Backup);
        connection.Identity = identity;
        return new HandshakeOutcome(challenge, result, identity);
    }

    private static async Task<(HelloChallengeMessage Challenge, byte[] Frame)> ExchangeHelloAsync(IRpcConnection connection, byte[] helloFrame,
        Guid expectedClusterId, TimeSpan timeout, CancellationToken ct)
    {
        ReadOnlyMemory<byte> reply = await connection.CallAsync(helloFrame, timeout, ct);
        IWireMessage message = MessageCodec.Decode(reply.Span);
        if (message is ErrorReplyMessage error)
            throw error.ToException();
        if (message is not HelloChallengeMessage challenge)
            throw new DfsProtocolException($"Expected HelloChallenge, received {message.Type}.");
        if (expectedClusterId != Guid.Empty && challenge.ClusterId != expectedClusterId)
        {
            throw new DfsClusterIdentityException(
                $"The server at {connection.Remote} belongs to cluster {challenge.ClusterId}, not the expected cluster {expectedClusterId}.");
        }

        return (challenge, reply.ToArray());
    }

    private static async Task<HelloResultMessage> ExchangeProofAsync(IRpcConnection connection, byte[] proof, TimeSpan timeout, CancellationToken ct)
    {
        ReadOnlyMemory<byte> reply = await connection.CallAsync(MessageCodec.Encode(new HelloProofMessage(proof)), timeout, ct);
        IWireMessage message = MessageCodec.Decode(reply.Span);
        if (message is ErrorReplyMessage error)
            throw error.ToException();
        if (message is not HelloResultMessage result)
            throw new DfsProtocolException($"Expected HelloResult, received {message.Type}.");
        if (!result.Accepted)
        {
            DfsErrorCode code = result.Error == DfsErrorCode.None ? DfsErrorCode.AuthenticationFailed : result.Error;
            throw DfsErrors.ToException(code, string.IsNullOrEmpty(result.Message) ? "The server rejected the handshake." : result.Message);
        }

        return result;
    }
}
