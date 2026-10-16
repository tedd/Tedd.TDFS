using System.Security.Cryptography;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Client.Internal.Runtime;

/// <summary>
/// One authenticated connection of the client to a node (live or backup): the transport connection plus the handshake
/// outcome (PROTOCOL.md §3). Requests are multiplexed by the transport; a connection is never shared between clients.
/// </summary>
internal sealed class ClientConnection
{
    private int _poisoned;
    private int _closed;
    private long _lastReply;
    private long _roundTripTicks;

    private ClientConnection(IRpcConnection raw, string endpoint, HelloChallengeMessage challenge, HelloResultMessage result, long openedAt,
        TimeSpan handshakeRoundTrip)
    {
        _lastReply = openedAt;
        _roundTripTicks = handshakeRoundTrip.Ticks > 0 ? handshakeRoundTrip.Ticks : 0;
        Raw = raw;
        Endpoint = endpoint;
        Challenge = challenge;
        Result = result;
        raw.Closed += (_, _) => Volatile.Write(ref _poisoned, 1);
    }

    /// <summary>Transport connection.</summary>
    public IRpcConnection Raw { get; }

    /// <summary>Endpoint the connection was opened to.</summary>
    public string Endpoint { get; }

    /// <summary>The server's challenge (cluster identity, server identity, negotiated version and features).</summary>
    public HelloChallengeMessage Challenge { get; }

    /// <summary>The accepting handshake result (principal, cluster permissions, capabilities).</summary>
    public HelloResultMessage Result { get; }

    /// <summary>Node id of the server, when it reported one.</summary>
    public NodeId? ServerNodeId => Challenge.ServerNodeId;

    /// <summary>True when the server is a read-only backup host.</summary>
    public bool IsBackupHost => Challenge.ServerRole == PeerRole.Backup;

    /// <summary>False once the transport closed or the connection was declared dead (stale pooled connection).</summary>
    public bool IsUsable => Volatile.Read(ref _poisoned) == 0 && Raw.IsOpen;

    /// <summary>Time-provider timestamp of the last reply received on this connection (initially its handshake).</summary>
    public long LastReply => Volatile.Read(ref _lastReply);

    /// <summary>
    /// Smoothed round-trip time of the requests answered on this connection, starting from the handshake's: how long a slow
    /// but answering server takes, so that short attempts (keep-alives) are not cut below it.
    /// </summary>
    public TimeSpan RoundTrip => TimeSpan.FromTicks(Volatile.Read(ref _roundTripTicks));

    /// <summary>Records that a reply arrived on this connection at <paramref name="timestamp"/>.</summary>
    public void NoteReply(long timestamp)
    {
        long current = Volatile.Read(ref _lastReply);
        while (timestamp > current)
        {
            long seen = Interlocked.CompareExchange(ref _lastReply, timestamp, current);
            if (seen == current)
                return;
            current = seen;
        }
    }

    /// <summary>Records a reply at <paramref name="timestamp"/> to a request sent at <paramref name="sentAt"/> (both time-provider timestamps).</summary>
    public void NoteReply(long timestamp, long sentAt, TimeProvider time)
    {
        ArgumentNullException.ThrowIfNull(time);
        NoteReply(timestamp);
        long sample = time.GetElapsedTime(sentAt, timestamp).Ticks;
        if (sample < 0)
            return;
        long current = Volatile.Read(ref _roundTripTicks);
        while (true)
        {
            long smoothed = current == 0 ? sample : current + ((sample - current) / 4);
            long seen = Interlocked.CompareExchange(ref _roundTripTicks, smoothed, current);
            if (seen == current)
                return;
            current = seen;
        }
    }

    /// <summary>
    /// Sends <paramref name="frame"/> and decodes the reply. Transport failures (<see cref="TimeoutException"/>,
    /// <see cref="RpcConnectionException"/>) and cancellation propagate unchanged.
    /// </summary>
    public async Task<IWireMessage> CallAsync(ReadOnlyMemory<byte> frame, TimeSpan timeout, CancellationToken ct)
    {
        ReadOnlyMemory<byte> reply = await Raw.CallAsync(frame, timeout, ct).ConfigureAwait(false);
        return MessageCodec.Decode(reply.Span);
    }

    /// <summary>Starts a call; the returned task carries the raw reply (used when the caller must know whether it was sent).</summary>
    public Task<ReadOnlyMemory<byte>> StartCall(ReadOnlyMemory<byte> frame, TimeSpan timeout, CancellationToken ct) => Raw.CallAsync(frame, timeout, ct);

    /// <summary>Declares the connection dead (silently dead links, protocol violations) and closes it in the background.</summary>
    public void Poison()
    {
        Volatile.Write(ref _poisoned, 1);
        if (Interlocked.Exchange(ref _closed, 1) == 0)
            CloseQuietly(Raw);
    }

    /// <summary>Closes a transport connection, ignoring failures (the connection is being discarded).</summary>
    public static void CloseQuietly(IRpcConnection connection)
    {
        ValueTask close;
        try
        {
            close = connection.DisposeAsync();
        }
        catch (Exception ex) when (ex is IOException or ObjectDisposedException or InvalidOperationException)
        {
            return;
        }

        if (!close.IsCompletedSuccessfully)
            Waits.ObserveQuietly(close.AsTask());
    }

    /// <summary>
    /// Opens a transport connection to <paramref name="endpoint"/> and authenticates the client principal
    /// (Hello → HelloChallenge → HelloProof → HelloResult). The cluster identity is checked before any proof is sent and
    /// the server's mutual-authentication MAC before the connection is returned. Failures close the connection.
    /// </summary>
    /// <exception cref="DfsClusterIdentityException">The node belongs to another cluster.</exception>
    /// <exception cref="DfsAuthenticationException">Authentication failed in either direction.</exception>
    public static async Task<ClientConnection> OpenAsync(ClientRuntime runtime, string endpoint, Guid expectedClusterId, TimeSpan timeout, CancellationToken ct)
    {
        IRpcConnection raw = await runtime.Transport.ConnectAsync(endpoint, ct).ConfigureAwait(false);
        try
        {
            return await AuthenticateAsync(runtime, raw, endpoint, expectedClusterId, timeout, ct).ConfigureAwait(false);
        }
        catch
        {
            CloseQuietly(raw);
            throw;
        }
    }

    /// <summary>Client wire stages of the handshake (F03/F14, operation <c>Authenticate</c>; occurrence 0 = Hello, 1 = proof).</summary>
    private static ValueTask HitAsync(ClientRuntime runtime, FaultStage stage, string site, string endpoint, int occurrence, CancellationToken ct) =>
        runtime.Faults.IsEnabled
            ? runtime.Faults.HitAsync(new FaultContext(stage, site, runtime.NetworkName, null, DfsOperationKind.Authenticate, Occurrence: occurrence,
                Detail: endpoint), ct)
            : ValueTask.CompletedTask;

    private static async Task<ClientConnection> AuthenticateAsync(ClientRuntime runtime, IRpcConnection raw, string endpoint, Guid expectedClusterId,
        TimeSpan timeout, CancellationToken ct)
    {
        DfsCredentials credentials = runtime.Options.Credentials;
        var hello = new HelloMessage(expectedClusterId, ProtocolInfo.MinSupportedVersion, ProtocolInfo.CurrentVersion, ClientRuntime.SupportedFeatures,
            PeerRole.Client, runtime.Options.ClientName, null, 0, credentials.Principal, runtime.Random.NextBytes(ProtocolInfo.NonceSize));
        byte[] helloFrame = MessageCodec.Encode(hello);
        await HitAsync(runtime, FaultStage.F03, FaultSites.ClientRequestSend, endpoint, 0, ct).ConfigureAwait(false);
        ReadOnlyMemory<byte> challengeReply = await raw.CallAsync(helloFrame, timeout, ct).ConfigureAwait(false);
        await HitAsync(runtime, FaultStage.F14, FaultSites.ClientReplyReceived, endpoint, 0, ct).ConfigureAwait(false);
        IWireMessage challengeMessage = MessageCodec.Decode(challengeReply.Span);
        if (challengeMessage is ErrorReplyMessage error)
            throw error.ToException();
        if (challengeMessage is not HelloChallengeMessage challenge)
            throw new DfsProtocolException($"Expected HelloChallenge from {endpoint}, received {challengeMessage.Type}.");
        if (expectedClusterId != Guid.Empty && challenge.ClusterId != expectedClusterId)
        {
            throw new DfsClusterIdentityException(
                $"The node at {endpoint} belongs to cluster {challenge.ClusterId}, not the expected cluster {expectedClusterId}.");
        }

        if (challenge.ProtocolVersion < ProtocolInfo.MinSupportedVersion || challenge.ProtocolVersion > ProtocolInfo.CurrentVersion)
        {
            throw new DfsProtocolException(
                $"The node at {endpoint} selected protocol version {challenge.ProtocolVersion}; this SDK supports {ProtocolInfo.MinSupportedVersion}..{ProtocolInfo.CurrentVersion}.");
        }

        if (challenge.Pbkdf2Iterations < 1 || challenge.Pbkdf2Iterations > HandshakeAuth.MaxIterations)
            throw new DfsAuthenticationException($"The node at {endpoint} asked for an unacceptable key-derivation iteration count.");

        byte[] transcript = HandshakeAuth.ComputeTranscript(helloFrame, challengeReply.Span);
        ClientKeys keys = runtime.GetClientKeys(challenge.PrincipalSalt.Span, challenge.Pbkdf2Iterations);
        byte[] proof = HandshakeAuth.ComputeClientProof(keys, transcript);
        await HitAsync(runtime, FaultStage.F03, FaultSites.ClientRequestSend, endpoint, 1, ct).ConfigureAwait(false);
        long proofSentAt = runtime.Time.GetTimestamp();
        ReadOnlyMemory<byte> resultReply = await raw.CallAsync(MessageCodec.Encode(new HelloProofMessage(proof)), timeout, ct).ConfigureAwait(false);
        TimeSpan handshakeRoundTrip = runtime.Time.GetElapsedTime(proofSentAt);
        await HitAsync(runtime, FaultStage.F14, FaultSites.ClientReplyReceived, endpoint, 1, ct).ConfigureAwait(false);
        CryptographicOperations.ZeroMemory(proof);
        IWireMessage resultMessage = MessageCodec.Decode(resultReply.Span);
        if (resultMessage is ErrorReplyMessage resultError)
            throw resultError.ToException();
        if (resultMessage is not HelloResultMessage result)
            throw new DfsProtocolException($"Expected HelloResult from {endpoint}, received {resultMessage.Type}.");
        if (!result.Accepted)
        {
            DfsErrorCode code = result.Error == DfsErrorCode.None ? DfsErrorCode.AuthenticationFailed : result.Error;
            string message = string.IsNullOrEmpty(result.Message) ? "The node rejected the handshake." : result.Message;
            throw DfsErrors.ToException(code, $"{message} (node {endpoint})");
        }

        if (!HandshakeAuth.VerifyServerMac(keys, transcript, result.ServerMac.Span))
            throw new DfsAuthenticationException($"The node at {endpoint} failed mutual authentication.");

        raw.Identity = new RpcPeerIdentity(challenge.ServerName, challenge.ServerNodeId, challenge.ServerIncarnation, null,
            IsNode: challenge.ServerRole == PeerRole.Node, IsBackup: challenge.ServerRole == PeerRole.Backup);
        return new ClientConnection(raw, endpoint, challenge, result, runtime.Time.GetTimestamp(), handshakeRoundTrip);
    }
}
