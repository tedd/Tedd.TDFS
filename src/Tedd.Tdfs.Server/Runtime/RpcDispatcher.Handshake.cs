using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Control;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Server.Runtime;

/// <summary>Server side of the connection handshake (PROTOCOL.md §3).</summary>
internal sealed partial class RpcDispatcher
{
    private IWireMessage HandleHello(ConnectionState state, HelloMessage hello, byte[] frame)
    {
        if (state.Phase != HandshakePhase.AwaitingHello)
        {
            Reject(state);
            return new ErrorReplyMessage(DfsErrorCode.ProtocolError, "Hello was already received on this connection.");
        }

        if (hello.ExpectedClusterId != Guid.Empty && hello.ExpectedClusterId != _s.ClusterId)
        {
            Reject(state);
            return new ErrorReplyMessage(DfsErrorCode.ClusterMismatch,
                $"This node belongs to cluster {_s.ClusterId}, not {hello.ExpectedClusterId}.");
        }

        int low = Math.Max((int)hello.ProtocolMin, ProtocolInfo.MinSupportedVersion);
        int high = Math.Min((int)hello.ProtocolMax, ProtocolInfo.CurrentVersion);
        if (low > high)
        {
            Reject(state);
            return new ErrorReplyMessage(DfsErrorCode.Unsupported,
                $"No common protocol version: peer {hello.ProtocolMin}..{hello.ProtocolMax}, server {ProtocolInfo.MinSupportedVersion}..{ProtocolInfo.CurrentVersion}.");
        }

        ControlState control = _s.Control.State;
        byte[] salt = [];
        int iterations = 0;
        switch (hello.Role)
        {
            case PeerRole.Node:
                if (hello.NodeId is not { IsEmpty: false } || string.IsNullOrEmpty(hello.Name))
                {
                    Reject(state);
                    return new ErrorReplyMessage(DfsErrorCode.AuthenticationFailed, "Node peers must present a node id and name.");
                }

                break;
            case PeerRole.Client:
            case PeerRole.Backup:
            {
                if (string.IsNullOrEmpty(hello.Principal))
                {
                    Reject(state);
                    return new ErrorReplyMessage(DfsErrorCode.AuthenticationFailed, "A principal is required; anonymous access is not possible.");
                }

                if (!control.IsBootstrapped)
                {
                    // No control state yet (a node rebuilding a replaced disk, or one still applying its bootstrap): it
                    // cannot judge any principal, which is not a failed authentication. The client tries another node.
                    Reject(state);
                    return new ErrorReplyMessage(DfsErrorCode.Unavailable, $"{_s.NodeName} is catching up with the cluster.", Retryable: true);
                }

                // An unknown principal gets a fake verifier (deterministic fake salt, default iterations) that the proof is
                // checked against like a real one: existence is disclosed neither by the challenge nor by the reply timing.
                PrincipalRecord? principal = control.FindPrincipal(hello.Principal);
                state.PrincipalKnown = principal is not null;
                state.Verifier = principal?.Verifier ?? _s.Keys.FakeVerifier(hello.Principal);
                salt = state.Verifier.Salt;
                iterations = state.Verifier.Iterations;
                break;
            }

            default:
                Reject(state);
                return new ErrorReplyMessage(DfsErrorCode.AuthenticationFailed, "Unknown peer role.");
        }

        IReadOnlySet<string> features = control.Features.Where(hello.Features.Contains).ToHashSet(StringComparer.Ordinal);
        var challenge = new HelloChallengeMessage(_s.ClusterId, control.ClusterName, (ushort)high, features, _s.NodeName, PeerRole.Node, _s.NodeId,
            _s.Incarnation, _s.Random.NextBytes(ProtocolInfo.NonceSize), salt, iterations);
        byte[] challengeFrame = MessageCodec.Encode(challenge);
        state.Hello = hello;
        state.HelloFrame = frame;
        state.ChallengeFrame = challengeFrame;
        state.BootId = hello.Role == PeerRole.Node ? hello.BootId : UInt128.Zero;
        state.Phase = HandshakePhase.AwaitingProof;
        return challenge;
    }

    private IWireMessage HandleProof(ConnectionState state, HelloProofMessage proof)
    {
        if (state.Phase != HandshakePhase.AwaitingProof || state.Hello is not { } hello || state.HelloFrame is null || state.ChallengeFrame is null)
        {
            Reject(state);
            return new ErrorReplyMessage(DfsErrorCode.ProtocolError, "HelloProof must follow Hello.");
        }

        byte[] transcript = HandshakeAuth.ComputeTranscript(state.HelloFrame, state.ChallengeFrame);
        DfsServerCapabilities capabilities = _s.Control.BuildCapabilities();
        if (hello.Role == PeerRole.Node)
        {
            if (!HandshakeAuth.VerifyNodeProof(_s.Keys.NodeAuthKey, transcript, proof.Proof.Span))
                return Refuse(state, DfsErrorCode.AuthenticationFailed, "Node authentication failed.", hello.Name);
            NodeId id = hello.NodeId!.Value;
            MemberRecord? member = _s.Control.State.FindMember(id);
            if (member is not null)
            {
                if (!member.IsLive)
                    return Refuse(state, DfsErrorCode.AccessDenied, $"Node {member.Name} was removed from the cluster; re-admission requires an explicit join.", hello.Name);
                if (!string.Equals(member.Name, hello.Name, StringComparison.Ordinal))
                    return Refuse(state, DfsErrorCode.AccessDenied, $"Node id {id} belongs to member '{member.Name}', not '{hello.Name}'.", hello.Name);
                if (hello.Incarnation < member.Incarnation
                    || (hello.Incarnation == member.Incarnation && member.BootId != UInt128.Zero && hello.BootId != member.BootId))
                {
                    return Refuse(state, DfsErrorCode.StaleIncarnation,
                        $"Incarnation {hello.Incarnation} of {member.Name} is stale (registered {member.Incarnation}).", hello.Name);
                }
            }

            state.Identity = new RpcPeerIdentity(hello.Name, id, hello.Incarnation, null, IsNode: true, IsBackup: false);
            state.Connection.Identity = state.Identity;
            state.Phase = HandshakePhase.Authenticated;
            _s.Peers?.NotePeerIncarnation(id, hello.Incarnation);
            return new HelloResultMessage(true, DfsErrorCode.None, string.Empty, hello.Name, DfsPermissions.None, capabilities,
                HandshakeAuth.ComputeNodeServerMac(_s.Keys.NodeAuthKey, transcript));
        }

        PrincipalVerifier? verifier = state.Verifier;
        bool proven = verifier is not null && HandshakeAuth.VerifyClientProof(verifier, transcript, proof.Proof.Span);
        if (!proven || !state.PrincipalKnown || verifier is null)
            return Refuse(state, DfsErrorCode.AuthenticationFailed, "Authentication failed.", hello.Principal);
        string principal = hello.Principal!;
        bool isBackup = hello.Role == PeerRole.Backup;
        state.Identity = new RpcPeerIdentity(null, null, 0, principal, IsNode: false, IsBackup: isBackup);
        state.Connection.Identity = state.Identity;
        state.Phase = HandshakePhase.Authenticated;
        DfsPermissions clusterPermissions = _s.Control.State.IsClusterAdmin(principal) ? DfsPermissions.ClusterAdmin : DfsPermissions.None;
        return new HelloResultMessage(true, DfsErrorCode.None, string.Empty, principal, clusterPermissions, capabilities,
            HandshakeAuth.ComputeServerMac(verifier, transcript));
    }

    private HelloResultMessage Refuse(ConnectionState state, DfsErrorCode code, string message, string? who)
    {
        Reject(state);
        _s.Warn("auth", NodeServices.Inv($"Rejected the handshake of {who ?? "an unnamed peer"}: {code}."));
        _s.Control.ReportSecurityEvent("authenticate", "denied:" + code, "peer=" + (who ?? string.Empty));
        return new HelloResultMessage(false, code, message, string.Empty, DfsPermissions.None, null, ReadOnlyMemory<byte>.Empty);
    }
}
