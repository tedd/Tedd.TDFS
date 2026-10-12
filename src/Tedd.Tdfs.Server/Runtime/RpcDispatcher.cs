using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Server.Runtime;

/// <summary>Peer roles a handler accepts (checked against the connection's authenticated identity on every request).</summary>
[Flags]
internal enum RpcAccess
{
    /// <summary>Handshake messages only (never registered for ordinary requests).</summary>
    Anonymous = 0,

    /// <summary>Authenticated principals (role Client); the principal must still exist in the control state.</summary>
    Client = 1,

    /// <summary>Authenticated member nodes (not removed, current incarnation).</summary>
    Node = 2,

    /// <summary>Backup nodes authenticated with principal credentials.</summary>
    Backup = 4,

    /// <summary>
    /// Nodes that proved the cluster secret but are unknown to this node's (possibly lagging) view of the membership: the
    /// seed-join flow and Raft traffic (a learner's view is empty until the log reaches it).
    /// </summary>
    JoiningNode = 8,

    /// <summary>Clients and member nodes.</summary>
    NodeOrClient = Client | Node,

    /// <summary>Every authenticated role.</summary>
    Any = Client | Node | Backup,
}

/// <summary>Handler of one request type; runs on the node loop.</summary>
internal delegate ValueTask<IWireMessage> RpcHandler(RpcContext context, IWireMessage request);

/// <summary>Per-request context handed to handlers.</summary>
internal sealed class RpcContext
{
    public required IRpcConnection Connection { get; init; }

    public required RpcPeerIdentity Identity { get; init; }

    public required ConnectionState State { get; init; }

    /// <summary>Cancelled when the connection closes.</summary>
    public required CancellationToken Cancellation { get; init; }

    public required MessageType Type { get; init; }

    /// <summary>The request frame exactly as received.</summary>
    public required ReadOnlyMemory<byte> Frame { get; init; }

    public required bool ExpectsReply { get; init; }

    /// <summary>Authenticated principal (clients and backup nodes), else null.</summary>
    public string? Principal => Identity.Principal;

    /// <summary>True for node peers.</summary>
    public bool IsNode => Identity.IsNode;

    /// <summary>Authenticated node id of a node peer.</summary>
    public NodeId? PeerNode => Identity.NodeId;
}

/// <summary>Handshake progress of a connection.</summary>
internal enum HandshakePhase
{
    AwaitingHello,
    AwaitingProof,
    Authenticated,
    Rejected,
}

/// <summary>Server-side state of one connection (inbound, or an outbound one authenticated by this node's handshake).</summary>
internal sealed class ConnectionState(IRpcConnection connection, bool outbound)
{
    public IRpcConnection Connection { get; } = connection;

    /// <summary>True for connections this node opened (the peer is the server of the handshake).</summary>
    public bool Outbound { get; } = outbound;

    public HandshakePhase Phase { get; set; }

    public byte[]? HelloFrame { get; set; }

    public byte[]? ChallengeFrame { get; set; }

    public HelloMessage? Hello { get; set; }

    public PrincipalVerifier? Verifier { get; set; }

    /// <summary>True when <see cref="Verifier"/> belongs to an existing principal (otherwise it is the fake verifier).</summary>
    public bool PrincipalKnown { get; set; }

    public RpcPeerIdentity? Identity { get; set; }

    /// <summary>Boot id announced by a node peer (0 for clients and outbound connections).</summary>
    public UInt128 BootId { get; set; }

    public int ProtocolErrors { get; set; }
}

/// <summary>
/// Inbound request dispatch (INTERNALS.md §1.1): requests arrive on transport threads and are posted to the node loop;
/// frames are decoded strictly, the handshake is enforced before anything else, every request is authorized against the
/// connection's authenticated identity, and handler exceptions become <see cref="ErrorReplyMessage"/>s mapped with
/// <see cref="DfsErrors"/> (leader hints preserved).
/// </summary>
internal sealed partial class RpcDispatcher : IRpcHandler
{
    private static readonly TimeSpan CloseGrace = TimeSpan.FromMilliseconds(200);
    private const int MaxProtocolErrors = 3;

    private readonly NodeServices _s;
    private readonly Dictionary<MessageType, Registration> _handlers = [];
    private readonly Dictionary<IRpcConnection, ConnectionState> _connections = new(ReferenceEqualityComparer.Instance);
    private int _inFlight;
    private TaskCompletionSource? _drained;

    public RpcDispatcher(NodeServices services)
    {
        ArgumentNullException.ThrowIfNull(services);
        _s = services;
    }

    /// <summary>Requests currently being handled.</summary>
    public int InFlight => _inFlight;

    /// <summary>Registers the handler of <typeparamref name="TRequest"/>; replacing an existing registration is an error.</summary>
    public void Register<TRequest>(RpcHandler handler, RpcAccess access) where TRequest : IWireMessage<TRequest>
    {
        ArgumentNullException.ThrowIfNull(handler);
        MessageType type = TRequest.WireType;
        if (!MessageTypes.IsRequest(type) || type is MessageType.Hello or MessageType.HelloProof)
            throw new ArgumentException($"{type} cannot be registered as a request handler.", nameof(handler));
        if (access == RpcAccess.Anonymous)
            throw new ArgumentException("Ordinary requests require an authenticated role.", nameof(access));
        if (!_handlers.TryAdd(type, new Registration(handler, access)))
            throw new InvalidOperationException($"A handler for {type} is already registered.");
    }

    /// <summary>True when a handler for <paramref name="type"/> is registered.</summary>
    public bool IsRegistered(MessageType type) => _handlers.ContainsKey(type);

    /// <summary>Records an outbound connection authenticated by this node as a trusted channel for inbound messages.</summary>
    public void RegisterOutbound(IRpcConnection connection, RpcPeerIdentity identity)
    {
        ConnectionState state = GetState(connection, outbound: true);
        state.Identity = identity;
        state.Phase = HandshakePhase.Authenticated;
    }

    /// <summary>An open, authenticated connection that <paramref name="node"/> opened to this node, or null.</summary>
    public IRpcConnection? FindInbound(NodeId node)
    {
        foreach (ConnectionState state in _connections.Values)
        {
            if (!state.Outbound && state.Phase == HandshakePhase.Authenticated && state.Identity is { IsNode: true } identity
                && identity.NodeId == node && state.Connection.IsOpen)
            {
                return state.Connection;
            }
        }

        return null;
    }

    /// <summary>Transport entry point (any thread): copies the frame and runs dispatch on the node loop.</summary>
    public ValueTask<ReadOnlyMemory<byte>?> HandleAsync(IRpcConnection connection, ReadOnlyMemory<byte> request, bool expectsReply, CancellationToken ct)
    {
        byte[] frame = request.ToArray();
        return new ValueTask<ReadOnlyMemory<byte>?>(_s.Loop.InvokeAsync(() => DispatchAsync(connection, frame, expectsReply, ct)));
    }

    /// <summary>Completes once no request is being handled (graceful stop).</summary>
    public Task WaitForDrainAsync()
    {
        if (_inFlight == 0)
            return Task.CompletedTask;
        _drained ??= new TaskCompletionSource();
        return _drained.Task;
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

    /// <summary>Closes a connection after a short grace period so the reply being sent still reaches the peer.</summary>
    public void CloseLater(IRpcConnection connection)
    {
        _ = CloseAfterGraceAsync(connection);
    }

    private async Task CloseAfterGraceAsync(IRpcConnection connection)
    {
        await Task.Delay(CloseGrace, _s.Time);
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
                drained.TrySetResult();
            }
        }
    }

    private async Task<ReadOnlyMemory<byte>?> DispatchCoreAsync(IRpcConnection connection, byte[] frame, bool expectsReply, CancellationToken ct)
    {
        if (!_s.IsServing || _s.IsStopping)
        {
            return expectsReply
                ? Error(new ErrorReplyMessage(DfsErrorCode.Unavailable, _s.IsStopping ? $"{_s.NodeName} is stopping." : $"{_s.NodeName} is starting.", Retryable: true))
                : null;
        }

        ConnectionState state = GetState(connection, outbound: false);
        if (state.Phase == HandshakePhase.Rejected)
            return expectsReply ? Error(new ErrorReplyMessage(DfsErrorCode.AuthenticationFailed, "The connection is not authenticated.")) : null;

        IWireMessage message;
        MessageType type;
        try
        {
            message = MessageCodec.Decode(frame);
            type = message.Type;
        }
        catch (DfsProtocolException ex)
        {
            state.ProtocolErrors++;
            if (state.Phase != HandshakePhase.Authenticated || state.ProtocolErrors >= MaxProtocolErrors)
                Reject(state);
            return expectsReply ? Error(new ErrorReplyMessage(ex.ErrorCode, ex.Message)) : null;
        }

        if (!MessageTypes.IsRequest(type))
        {
            state.ProtocolErrors++;
            Reject(state);
            return expectsReply ? Error(new ErrorReplyMessage(DfsErrorCode.ProtocolError, $"{type} is not a request.")) : null;
        }

        if (type is MessageType.Hello or MessageType.HelloProof)
        {
            if (!expectsReply || state.Outbound)
                return null;
            IWireMessage reply = type == MessageType.Hello
                ? HandleHello(state, (HelloMessage)message, frame)
                : HandleProof(state, (HelloProofMessage)message);
            return MessageCodec.Encode(reply);
        }

        if (state.Phase != HandshakePhase.Authenticated || state.Identity is null)
        {
            Reject(state);
            return expectsReply ? Error(new ErrorReplyMessage(DfsErrorCode.AuthenticationFailed, "Authenticate before sending requests.")) : null;
        }

        if (!_handlers.TryGetValue(type, out Registration registration))
            return expectsReply ? Error(new ErrorReplyMessage(DfsErrorCode.Unsupported, $"{_s.NodeName} does not serve {type}.")) : null;

        if (CheckAccess(state, registration.Access) is { } denied)
            return expectsReply ? Error(denied) : null;

        _s.Metrics.RpcHandled();
        DfsOperationKind operation = MessageTypes.OperationOf(type);
        bool observed = IsObservedForFaults(type);
        DfsOperationId? operationId = (message as IVolumeRequest)?.Header.Operation is { IsEmpty: false } op ? op : null;
        string? volume = (message as IVolumeRequest)?.Header.Volume;
        IWireMessage result;
        try
        {
            if (observed)
                await _s.HitAsync(FaultStage.F04, FaultSites.RpcRequestAccepted, volume, operation, operationId, ct: ct);
            var context = new RpcContext
            {
                Connection = connection,
                Identity = state.Identity,
                State = state,
                Cancellation = ct,
                Type = type,
                Frame = frame,
                ExpectsReply = expectsReply,
            };
            result = await registration.Handler(context, message);
        }
        catch (FaultReplyDroppedException)
        {
            return null;
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            return null;
        }
        catch (Exception ex)
        {
            if (DfsErrors.ToCode(ex) == DfsErrorCode.Internal)
                _s.Error("rpc", NodeServices.Inv($"{type} from {Describe(state)} failed: {ex}"));
            result = ErrorReplyMessage.FromException(ex, operationId);
        }

        if (!expectsReply)
            return null;
        if (observed)
        {
            try
            {
                await _s.HitAsync(FaultStage.F13, FaultSites.RpcReplySend, volume, operation, operationId, ct: ct);
            }
            catch (FaultReplyDroppedException)
            {
                return null;
            }
        }

        return MessageCodec.Encode(result);
    }

    /// <summary>Authorizes a request; null when allowed, otherwise the error reply (and the connection may be closed).</summary>
    private ErrorReplyMessage? CheckAccess(ConnectionState state, RpcAccess access)
    {
        RpcPeerIdentity identity = state.Identity!;
        Control.ControlState control = _s.Control.State;
        if (identity.IsNode)
        {
            NodeId id = identity.NodeId ?? default;
            Control.MemberRecord? member = control.FindMember(id);
            if (member is null)
            {
                return (access & RpcAccess.JoiningNode) != 0
                    ? null
                    : new ErrorReplyMessage(DfsErrorCode.AccessDenied, $"Node {identity.NodeName} is not a member of the cluster.");
            }

            if (!member.IsLive)
            {
                Reject(state);
                return new ErrorReplyMessage(DfsErrorCode.AccessDenied, $"Node {member.Name} was removed from the cluster.");
            }

            if (identity.Incarnation < member.Incarnation
                || (!state.Outbound && identity.Incarnation == member.Incarnation && member.BootId != UInt128.Zero && state.BootId != member.BootId))
            {
                Reject(state);
                return new ErrorReplyMessage(DfsErrorCode.StaleIncarnation,
                    $"Incarnation {identity.Incarnation} of {member.Name} is stale (registered {member.Incarnation}).");
            }

            return (access & (RpcAccess.Node | RpcAccess.JoiningNode)) != 0
                ? null
                : new ErrorReplyMessage(DfsErrorCode.AccessDenied, $"{nameof(RpcAccess.Node)} peers may not send this request.");
        }

        RpcAccess role = identity.IsBackup ? RpcAccess.Backup : RpcAccess.Client;
        if ((access & role) == 0)
            return new ErrorReplyMessage(DfsErrorCode.AccessDenied, "This request is reserved to cluster nodes.");
        if (identity.Principal is null || control.FindPrincipal(identity.Principal) is null)
            return new ErrorReplyMessage(DfsErrorCode.AccessDenied, $"Principal '{identity.Principal}' no longer exists.");
        return null;
    }

    private ConnectionState GetState(IRpcConnection connection, bool outbound)
    {
        if (_connections.TryGetValue(connection, out ConnectionState? state))
            return state;
        state = new ConnectionState(connection, outbound);
        _connections[connection] = state;
        connection.Closed += (c, _) => _s.Loop.Post(() => _connections.Remove(c));
        return state;
    }

    private void Reject(ConnectionState state)
    {
        if (state.Outbound)
            return;
        state.Phase = HandshakePhase.Rejected;
        CloseLater(state.Connection);
    }

    private static string Describe(ConnectionState state)
        => state.Identity is { } id ? (id.IsNode ? "node " + id.NodeName : "principal " + id.Principal) : "an unauthenticated peer";

    private static ReadOnlyMemory<byte> Error(ErrorReplyMessage error) => MessageCodec.Encode(error);

    /// <summary>Consensus and handshake traffic is not reported at request-path fault sites (it is not an operation).</summary>
    private static bool IsObservedForFaults(MessageType type)
        => type is not (MessageType.RaftBatch or MessageType.RaftSnapshotChunk);

    private readonly record struct Registration(RpcHandler Handler, RpcAccess Access);
}
