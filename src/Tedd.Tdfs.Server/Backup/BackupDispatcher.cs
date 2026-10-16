using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Server.Backup;

/// <summary>
/// Inbound request dispatch of a backup node: requests are posted to the backup node's loop, the principal handshake
/// (PROTOCOL.md §3, server role <see cref="PeerRole.Backup"/>, cluster identity of the source) is enforced before anything
/// else, and every request is answered by the read-only service, the export receiver, or refused. Nothing a backup node
/// serves takes a lease, a lock, a writer generation or mutates snapshot contents (FSD §12.1, INV-15): every mutating
/// request is answered with <see cref="DfsErrorCode.ReadOnly"/>, node-only traffic with <see cref="DfsErrorCode.AccessDenied"/>.
/// </summary>
internal sealed class BackupDispatcher : IRpcHandler
{
    private static readonly TimeSpan CloseGrace = TimeSpan.FromMilliseconds(200);
    private const int MaxProtocolErrors = 3;

    private readonly BackupServices _s;
    private readonly Dictionary<IRpcConnection, ConnectionInfo> _connections = new(ReferenceEqualityComparer.Instance);
    private int _inFlight;
    private CancellationTokenSource? _drained;

    public BackupDispatcher(BackupServices services)
    {
        ArgumentNullException.ThrowIfNull(services);
        _s = services;
    }

    /// <summary>Transport entry point (any thread): copies the frame and runs dispatch on the backup node's loop.</summary>
    public ValueTask<ReadOnlyMemory<byte>?> HandleAsync(IRpcConnection connection, ReadOnlyMemory<byte> request, bool expectsReply, CancellationToken ct)
    {
        byte[] frame = request.ToArray();
        return new ValueTask<ReadOnlyMemory<byte>?>(_s.Loop.InvokeAsync(() => DispatchAsync(connection, frame, expectsReply, ct)));
    }

    /// <summary>Completes once no request is being handled (graceful stop).</summary>
    public async Task WaitForDrainAsync(TimeSpan timeout, TimeProvider time)
    {
        if (_inFlight == 0)
            return;
        _drained ??= new CancellationTokenSource();
        try
        {
            await Task.Delay(timeout, time, _drained.Token);
        }
        catch (OperationCanceledException)
        {
            // Drained.
        }
    }

    /// <summary>Closes every inbound connection (stop).</summary>
    public async Task CloseAllAsync()
    {
        IRpcConnection[] connections = [.. _connections.Keys];
        _connections.Clear();
        foreach (IRpcConnection connection in connections)
        {
            try
            {
                await connection.DisposeAsync();
            }
            catch (Exception ex) when (ex is IOException or ObjectDisposedException or RpcConnectionException)
            {
                // Closing anyway.
            }
        }
    }

    private async Task<ReadOnlyMemory<byte>?> DispatchAsync(IRpcConnection connection, byte[] frame, bool expectsReply, CancellationToken ct)
    {
        _inFlight++;
        try
        {
            return await DispatchCoreAsync(connection, frame, expectsReply, ct);
        }
        finally
        {
            if (--_inFlight == 0 && _drained is { } drained)
            {
                _drained = null;
                drained.Cancel();
                drained.Dispose();
            }
        }
    }

    private async Task<ReadOnlyMemory<byte>?> DispatchCoreAsync(IRpcConnection connection, byte[] frame, bool expectsReply, CancellationToken ct)
    {
        if (!_s.IsServing || _s.IsStopping)
        {
            return expectsReply
                ? Error(new ErrorReplyMessage(DfsErrorCode.Unavailable, _s.IsStopping ? $"{_s.Name} is stopping." : $"{_s.Name} is starting.", Retryable: true))
                : null;
        }

        ConnectionInfo state = GetState(connection);
        if (state.Phase == Phase.Rejected)
            return expectsReply ? Error(new ErrorReplyMessage(state.RejectedWith, "The connection is not authenticated.")) : null;

        IWireMessage message;
        try
        {
            message = MessageCodec.Decode(frame);
        }
        catch (DfsProtocolException ex)
        {
            state.ProtocolErrors++;
            if (state.Phase != Phase.Authenticated || state.ProtocolErrors >= MaxProtocolErrors)
            {
                if (state.Phase == Phase.Authenticated)
                    state.RejectedWith = DfsErrorCode.ProtocolError;
                Reject(state);
            }

            return expectsReply ? Error(new ErrorReplyMessage(ex.ErrorCode, ex.Message)) : null;
        }

        MessageType type = message.Type;
        if (!MessageTypes.IsRequest(type))
        {
            state.ProtocolErrors++;
            Reject(state);
            return expectsReply ? Error(new ErrorReplyMessage(DfsErrorCode.ProtocolError, $"{type} is not a request.")) : null;
        }

        if (type is MessageType.Hello or MessageType.HelloProof)
        {
            if (!expectsReply)
                return null;
            IWireMessage reply = type == MessageType.Hello ? HandleHello(state, (HelloMessage)message, frame) : HandleProof(state, (HelloProofMessage)message);
            return MessageCodec.Encode(reply);
        }

        if (state.Phase != Phase.Authenticated || state.Principal is null)
        {
            Reject(state);
            return expectsReply ? Error(new ErrorReplyMessage(DfsErrorCode.AuthenticationFailed, "Authenticate before sending requests.")) : null;
        }

        // The request-path stages of the RPC boundary (test plan §8.2 F04/F13), reported like a live node's dispatcher does:
        // the export pipeline (BackupTransfer, BackupPublish) and the read-only service are request/response exchanges whose
        // acceptance and reply transmission are real fault boundaries (NET-020).
        DfsOperationKind operation = MessageTypes.OperationOf(type);
        string? volume = (message as IVolumeRequest)?.Header.Volume;
        IWireMessage result;
        try
        {
            await _s.HitAsync(FaultStage.F04, FaultSites.RpcRequestAccepted, volume, operation, ct: ct);
            result = await HandleRequestAsync(state.Principal, message, ct);
        }
        catch (FaultReplyDroppedException)
        {
            return null;
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            return null;
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            Exception mapped = Map(ex);
            if (DfsErrors.ToCode(mapped) == DfsErrorCode.Internal)
                _s.Log(NodeLogLevel.Error, "rpc", BackupServices.Inv($"{type} from principal {state.Principal} failed: {ex}"));
            result = ErrorReplyMessage.FromException(mapped, (message as IVolumeRequest)?.Header.Operation is { IsEmpty: false } own ? own : null);
        }

        if (!expectsReply)
            return null;
        try
        {
            await _s.HitAsync(FaultStage.F13, FaultSites.RpcReplySend, volume, operation, ct: ct);
        }
        catch (FaultReplyDroppedException)
        {
            return null;
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            return null;
        }

        return MessageCodec.Encode(result);
    }

    private static Exception Map(Exception ex) => ex switch
    {
        _ when StorageErrors.IsDiskFull(ex) => new DfsCapacityException("The backup node's storage is full.", ex),
        DfsException or UnauthorizedAccessException or ArgumentException or FileNotFoundException or DirectoryNotFoundException => ex,
        IOException io => new DfsUnavailableException(DfsErrorCode.Unavailable, "The backup node's storage failed: " + io.Message, io),
        _ => ex,
    };

    /// <summary>Routes one authenticated request.</summary>
    private async ValueTask<IWireMessage> HandleRequestAsync(string principal, IWireMessage message, CancellationToken ct)
    {
        BackupReader reader = _s.Reader;
        BackupReceiver receiver = _s.Receiver;
        switch (message)
        {
            // Discovery and sessions.
            case GetClusterMapMessage:
                return reader.GetClusterMap(principal);
            case GetVolumeRouteMessage m:
                return reader.GetVolumeRoute(principal, m);
            case OpenSessionMessage m:
                return reader.OpenSession(principal, m);
            case KeepAliveMessage m:
                return reader.KeepAlive(principal, m);
            case ReserveOperationIdsMessage m:
                return reader.ReserveOperationIds(principal, m);
            case QueryOperationMessage m:
                return reader.QueryOperation(principal, m);

            // Snapshots and views.
            case ListSnapshotsMessage m:
                return reader.ListSnapshots(principal, m);
            case OpenSnapshotMessage m:
                return reader.OpenSnapshot(principal, m);
            case CloseSnapshotMessage m:
                return await reader.CloseSnapshotAsync(principal, m, ct);
            case SetRetentionMessage m:
                return await reader.SetRetentionAsync(principal, m, ct);
            case DeleteSnapshotMessage m:
                return await reader.DeleteSnapshotAsync(principal, m, ct);
            case GetInfoMessage m:
                return reader.GetInfo(principal, m);
            case ListDirectoryMessage m:
                return reader.ListDirectory(principal, m);
            case ReadRangeMessage m:
                return reader.ReadRange(principal, m);
            case ListStreamsMessage m:
                return reader.ListStreams(principal, m);
            case GetAccessControlMessage m:
                return reader.GetAccessControl(principal, m);
            case GetEffectiveRightsMessage m:
                return reader.GetEffectiveRights(principal, m);
            case GetObjectMessage m:
                return await reader.GetObjectAsync(principal, m, ct);
            case LocateObjectMessage m:
                return reader.LocateObject(principal, m);

            // Export protocol (receiver side) and restore export.
            case BackupBeginMessage m:
                return await receiver.BeginAsync(principal, m, ct);
            case BackupInventoryMessage m:
                return receiver.Inventory(principal, m);
            case BackupPutObjectMessage m:
                return await receiver.PutObjectAsync(principal, m, ct);
            case BackupPutManifestMessage m:
                return await receiver.PutManifestAsync(principal, m, ct);
            case BackupPublishMessage m:
                return await receiver.PublishAsync(principal, m, ct);
            case BackupRetentionMessage m:
                return await receiver.RetentionAsync(principal, m, ct);
            case BackupExportRestoreMessage m:
                return await reader.ExportRestoreAsync(principal, m, ct);
        }

        throw Refusal(message.Type);
    }

    /// <summary>The refusal of a request a backup node never serves.</summary>
    private Exception Refusal(MessageType type) => type switch
    {
        MessageType.OpenHandle or MessageType.CloseHandle or MessageType.CreateStaging or MessageType.CommitFile or MessageType.WriteAll
            or MessageType.Append or MessageType.Copy or MessageType.Move or MessageType.Delete or MessageType.CreateDirectory or MessageType.SetPolicy
            or MessageType.SetAccess or MessageType.SetAccessControl or MessageType.SetOwner or MessageType.TxBegin or MessageType.TxOp
            or MessageType.TxCommit or MessageType.TxAbort or MessageType.CreateSnapshot or MessageType.RestoreSnapshot or MessageType.RestoreVolume
            or MessageType.RestoreInstall or MessageType.PutObject or MessageType.BackupRegister or MessageType.BackupRequestSync
            => new DfsReadOnlyException($"{_s.Name} is a backup host: it serves published snapshots read-only and grants no handles, leases or locks ({type})."),
        MessageType.BackupStatus or MessageType.BackupAwait
            => new DfsException(DfsErrorCode.Unsupported, $"{type} is answered by the source volume's leader, not by a backup host."),
        _ => new UnauthorizedAccessException($"{_s.Name} is a backup host; {type} is not served to principals here."),
    };

    // ------------------------------------------------------------------------------------------------- handshake

    private IWireMessage HandleHello(ConnectionInfo state, HelloMessage hello, byte[] frame)
    {
        if (state.Phase != Phase.AwaitingHello)
        {
            Reject(state);
            return new ErrorReplyMessage(DfsErrorCode.ProtocolError, "Hello was already received on this connection.");
        }

        Guid clusterId = _s.Options.SourceClusterId;
        if (hello.ExpectedClusterId != Guid.Empty && hello.ExpectedClusterId != clusterId)
        {
            Reject(state);
            return new ErrorReplyMessage(DfsErrorCode.ClusterMismatch, $"This backup node holds backups of cluster {clusterId}, not {hello.ExpectedClusterId}.");
        }

        int low = Math.Max((int)hello.ProtocolMin, ProtocolInfo.MinSupportedVersion);
        int high = Math.Min((int)hello.ProtocolMax, ProtocolInfo.CurrentVersion);
        if (low > high)
        {
            Reject(state);
            return new ErrorReplyMessage(DfsErrorCode.Unsupported,
                $"No common protocol version: peer {hello.ProtocolMin}..{hello.ProtocolMax}, server {ProtocolInfo.MinSupportedVersion}..{ProtocolInfo.CurrentVersion}.");
        }

        if (hello.Role is not (PeerRole.Client or PeerRole.Backup))
        {
            // Backup nodes are never cluster members: node identities (cluster secret) mean nothing here.
            Reject(state);
            return new ErrorReplyMessage(DfsErrorCode.AuthenticationFailed, "A backup node authenticates principals only.");
        }

        if (string.IsNullOrEmpty(hello.Principal))
        {
            Reject(state);
            return new ErrorReplyMessage(DfsErrorCode.AuthenticationFailed, "A principal is required; anonymous access is not possible.");
        }

        PrincipalVerifier? known = _s.Principals.Find(hello.Principal);
        state.PrincipalKnown = known is not null;
        state.Verifier = known ?? _s.Principals.FakeVerifier(hello.Principal);
        var challenge = new HelloChallengeMessage(clusterId, _s.Reader.ClusterName, (ushort)high, new HashSet<string>(StringComparer.Ordinal), _s.Name,
            PeerRole.Backup, _s.Keys.NodeId, 1, _s.Random.NextBytes(ProtocolInfo.NonceSize), state.Verifier.Salt, state.Verifier.Iterations);
        byte[] challengeFrame = MessageCodec.Encode(challenge);
        state.Hello = hello;
        state.HelloFrame = frame;
        state.ChallengeFrame = challengeFrame;
        state.Phase = Phase.AwaitingProof;
        return challenge;
    }

    private IWireMessage HandleProof(ConnectionInfo state, HelloProofMessage proof)
    {
        if (state.Phase != Phase.AwaitingProof || state.Hello is not { } hello || state.HelloFrame is null || state.ChallengeFrame is null)
        {
            Reject(state);
            return new ErrorReplyMessage(DfsErrorCode.ProtocolError, "HelloProof must follow Hello.");
        }

        byte[] transcript = HandshakeAuth.ComputeTranscript(state.HelloFrame, state.ChallengeFrame);
        PrincipalVerifier? verifier = state.Verifier;
        bool proven = verifier is not null && HandshakeAuth.VerifyClientProof(verifier, transcript, proof.Proof.Span);
        if (!proven || !state.PrincipalKnown || verifier is null)
        {
            Reject(state);
            _s.Warn("auth", BackupServices.Inv($"Rejected the handshake of principal {hello.Principal}."));
            return new HelloResultMessage(false, DfsErrorCode.AuthenticationFailed, "Authentication failed.", string.Empty, DfsPermissions.None, null,
                ReadOnlyMemory<byte>.Empty);
        }

        string principal = hello.Principal!;
        var identity = new RpcPeerIdentity(null, null, 0, principal, IsNode: false, IsBackup: hello.Role == PeerRole.Backup);
        state.Connection.Identity = identity;
        state.Principal = principal;
        state.Phase = Phase.Authenticated;
        return new HelloResultMessage(true, DfsErrorCode.None, string.Empty, principal, DfsPermissions.None, _s.Reader.Capabilities,
            HandshakeAuth.ComputeServerMac(verifier, transcript));
    }

    // ------------------------------------------------------------------------------------------------- connections

    private ConnectionInfo GetState(IRpcConnection connection)
    {
        if (_connections.TryGetValue(connection, out ConnectionInfo? state))
            return state;
        state = new ConnectionInfo(connection);
        _connections[connection] = state;
        connection.Closed += (c, _) => _s.Loop.Post(() => _connections.Remove(c));
        return state;
    }

    private void Reject(ConnectionInfo state)
    {
        state.Phase = Phase.Rejected;
        _ = CloseAfterGraceAsync(state.Connection);
    }

    private async Task CloseAfterGraceAsync(IRpcConnection connection)
    {
        try
        {
            await Task.Delay(CloseGrace, _s.Time);
        }
        catch (ObjectDisposedException)
        {
            // Time source gone with the node.
        }

        _connections.Remove(connection);
        try
        {
            await connection.DisposeAsync();
        }
        catch (Exception ex) when (ex is IOException or ObjectDisposedException or RpcConnectionException)
        {
            // Already gone.
        }
    }

    private static ReadOnlyMemory<byte> Error(ErrorReplyMessage error) => MessageCodec.Encode(error);

    private enum Phase
    {
        AwaitingHello,
        AwaitingProof,
        Authenticated,
        Rejected,
    }

    private sealed class ConnectionInfo(IRpcConnection connection)
    {
        public IRpcConnection Connection { get; } = connection;

        public Phase Phase { get; set; }

        public byte[]? HelloFrame { get; set; }

        public byte[]? ChallengeFrame { get; set; }

        public HelloMessage? Hello { get; set; }

        public PrincipalVerifier? Verifier { get; set; }

        public bool PrincipalKnown { get; set; }

        public string? Principal { get; set; }

        public int ProtocolErrors { get; set; }

        public DfsErrorCode RejectedWith { get; set; } = DfsErrorCode.AuthenticationFailed;
    }
}
