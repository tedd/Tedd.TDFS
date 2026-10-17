using System.Buffers.Binary;
using System.Globalization;
using Tedd.Tdfs.Data;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Testing;

/// <summary>Creates fabric transports for one identity, wrapped in the harness interposition layer.</summary>
internal sealed class HarnessTransportFactory : IRpcTransportFactory
{
    public HarnessTransportFactory(NetworkFabric fabric, IRpcTransportFactory inner, TransportIdentity identity)
    {
        Fabric = fabric;
        Inner = inner;
        Identity = identity;
    }

    public NetworkFabric Fabric { get; }

    public IRpcTransportFactory Inner { get; }

    public TransportIdentity Identity { get; }

    /// <inheritdoc />
    public IRpcTransport Create(string localName, string listenEndpoint)
    {
        ArgumentException.ThrowIfNullOrEmpty(localName);
        if (Fabric.Sim is { } sim && Identity.Clock is { } clock)
            sim.SetClock(localName, clock);
        string endpoint = string.IsNullOrEmpty(listenEndpoint) ? ConnectOnlyEndpoint(localName) : listenEndpoint;
        IRpcTransport inner = Inner.Create(localName, endpoint);
        var transport = new HarnessTransport(this, inner, localName);
        Fabric.AddTransport(transport);
        return transport;
    }

    /// <summary>A listen endpoint for a transport that only connects (clients): never registered unless it starts.</summary>
    private string ConnectOnlyEndpoint(string localName)
    {
        if (Fabric.Sim is not null)
            return string.Empty;
        if (Fabric.Kind == TransportKind.Quic)
            return "quic://127.0.0.1:0";
        var sb = new System.Text.StringBuilder("mem://");
        foreach (char c in localName)
            sb.Append(char.IsAsciiLetterOrDigit(c) || c is '-' or '_' or '.' ? c : '-');
        return sb.ToString();
    }
}

/// <summary>A fabric transport: the inner transport (simulated, in-memory or QUIC) behind the harness interposition layer.</summary>
internal sealed class HarnessTransport : IRpcTransport, IRpcTransportControl, IRpcTransportIdentity
{
    public HarnessTransport(HarnessTransportFactory factory, IRpcTransport inner, string localName)
    {
        Factory = factory;
        Inner = inner;
        LocalName = localName;
    }

    public HarnessTransportFactory Factory { get; }

    public IRpcTransport Inner { get; }

    public NetworkFabric Fabric => Factory.Fabric;

    public TransportIdentity Identity => Factory.Identity;

    /// <inheritdoc />
    public string LocalName { get; }

    /// <inheritdoc />
    public string LocalEndpoint => Inner.LocalEndpoint;

    /// <summary>The inner transport's control interface (null for the simulated network).</summary>
    public IRpcTransportControl? InnerControl => Inner as IRpcTransportControl;

    /// <inheritdoc />
    public RpcCertificateInfo? LocalCertificate => (Inner as IRpcTransportIdentity)?.LocalCertificate;

    /// <inheritdoc />
    public bool PinsPeers => Inner is IRpcTransportIdentity { PinsPeers: true };

    /// <inheritdoc />
    public bool TrustsPin(ReadOnlySpan<byte> spkiSha256) => Inner is IRpcTransportIdentity identity && identity.TrustsPin(spkiSha256);

    /// <inheritdoc />
    public int OpenConnectionCount => InnerControl?.OpenConnectionCount ?? Fabric.CountOpenConnections(LocalName);

    /// <summary>Clock for silent-death timeouts.</summary>
    public TimeProvider Clock => Identity.Clock ?? Fabric.Runtime.Time;

    /// <inheritdoc />
    public Task StartAsync(IRpcHandler handler, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(handler);
        Task started = Inner.StartAsync(new HarnessHandler(this, handler), ct);
        if (started.IsCompletedSuccessfully)
        {
            Fabric.RegisterEndpoint(Inner.LocalEndpoint, LocalName);
            return started;
        }

        return CompleteStartAsync(started);
    }

    /// <inheritdoc />
    public async Task<IRpcConnection> ConnectAsync(string endpoint, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(endpoint);
        string? target = Fabric.NameOfEndpoint(endpoint);
        if (!Identity.IsHarness)
            Fabric.CountAttempt(target ?? NetworkFabric.NormalizeEndpoint(endpoint));
        DatagramRelay? relay = null;
        IRpcConnection inner;
        if (Fabric.UsesDatagramRelays && RpcAddress.TryParse(endpoint, out RpcAddress address) && address.IsQuic)
        {
            // The connection's encrypted datagrams travel through its own relay (TamperNext alters them there).
            relay = await Fabric.CreateRelayAsync(endpoint, ct).ConfigureAwait(false);
            try
            {
                inner = await Inner.ConnectAsync(relay.FrontEndpoint, ct).ConfigureAwait(false);
            }
            catch
            {
                Fabric.ReleaseRelay(relay);
                throw;
            }
        }
        else
        {
            inner = await Inner.ConnectAsync(endpoint, ct).ConfigureAwait(false);
        }

        string remote = NetworkFabric.RemoteNameOf(inner) ?? target ?? inner.Remote.ToString();
        var connection = new HarnessConnection(this, inner, remote, isClientSide: true, endpoint) { Relay = relay };
        Fabric.AddConnection(connection);
        connection.AttachClosed();
        return connection;
    }

    /// <inheritdoc />
    public void ResetConnections(string? remoteName = null)
    {
        if (InnerControl is { } control)
        {
            control.ResetConnections(remoteName);
            return;
        }

        if (Fabric.Sim is { } sim)
            sim.ResetConnections(remoteName ?? LocalName);
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        Fabric.RemoveTransport(this);
        return Inner.DisposeAsync();
    }

    /// <inheritdoc />
    public override string ToString() => $"HarnessTransport({LocalName}, {LocalEndpoint})";

    private async Task CompleteStartAsync(Task started)
    {
        await started.ConfigureAwait(false);
        Fabric.RegisterEndpoint(Inner.LocalEndpoint, LocalName);
    }
}

/// <summary>
/// One end of a fabric connection seen through the interposition layer: counts attempts, applies sender-side rules
/// (truncation, tampering), silent death, and remembers the handshake identity for replays.
/// </summary>
internal sealed class HarnessConnection : IRpcConnection
{
    private readonly object _gate = new();
    private Action<IRpcConnection, Exception?>? _closed;
    private Exception? _closeError;
    private bool _isClosed;
    private volatile bool _silentlyDead;
    private volatile bool _authenticated;

    public HarnessConnection(HarnessTransport owner, IRpcConnection inner, string remoteName, bool isClientSide, string? endpoint)
    {
        Owner = owner;
        Inner = inner;
        RemoteName = remoteName;
        IsClientSide = isClientSide;
        Endpoint = endpoint;
    }

    public HarnessTransport Owner { get; }

    public IRpcConnection Inner { get; }

    public string LocalName => Owner.LocalName;

    public string RemoteName { get; }

    public bool IsClientSide { get; }

    /// <summary>The endpoint a client-side connection was opened to.</summary>
    public string? Endpoint { get; }

    /// <summary>The UDP relay a relayed client-side QUIC connection runs through (<see cref="NetworkFabric.UsesDatagramRelays"/>).</summary>
    public DatagramRelay? Relay { get; init; }

    /// <summary>Role declared by the first <c>Hello</c> sent on this (client-side) connection.</summary>
    public PeerRole? HelloRole { get; private set; }

    /// <summary>Principal declared by the first <c>Hello</c>.</summary>
    public string? HelloPrincipal { get; private set; }

    /// <summary>Peer name declared by the first <c>Hello</c>.</summary>
    public string? HelloName { get; private set; }

    /// <summary>True once the handshake on this (client-side) connection was accepted.</summary>
    public bool IsAuthenticated => _authenticated;

    /// <summary>True after <see cref="TestNetwork.KillConnectionsSilently"/>: frames vanish both ways, the connection looks open.</summary>
    public bool SilentlyDead
    {
        get => _silentlyDead;
        set => _silentlyDead = value;
    }

    /// <inheritdoc />
    public RpcAddress Remote => Inner.Remote;

    /// <inheritdoc />
    public bool IsOpen => Inner.IsOpen;

    /// <inheritdoc />
    public RpcPeerIdentity? Identity
    {
        get => Inner.Identity;
        set => Inner.Identity = value;
    }

    private NetworkFabric Fabric => Owner.Fabric;

    /// <inheritdoc />
    public event Action<IRpcConnection, Exception?>? Closed
    {
        add
        {
            if (value is null)
                return;
            Exception? error;
            lock (_gate)
            {
                if (!_isClosed)
                {
                    _closed += value;
                    return;
                }

                error = _closeError;
            }

            value(this, error);
        }
        remove
        {
            lock (_gate)
                _closed -= value;
        }
    }

    /// <summary>Subscribes to the inner connection's close (after registration).</summary>
    public void AttachClosed() => Inner.Closed += OnInnerClosed;

    /// <inheritdoc />
    public Task<ReadOnlyMemory<byte>> CallAsync(ReadOnlyMemory<byte> request, TimeSpan timeout, CancellationToken ct = default)
    {
        if (!Owner.Identity.IsHarness)
            Fabric.CountAttempt(RemoteName);
        if (_silentlyDead)
            return SilentCallAsync(timeout, ct);

        MessageType type = TypeOf(request.Span);
        if (IsClientSide && type == MessageType.Hello && HelloRole is null)
            ObserveHello(request.Span);

        if (Fabric.Policy.TryTakeRule(MessageRuleKind.Truncate, LocalName, RemoteName, LinkDirection.Request, request.Span) is { } truncate)
            return TruncatedCall(request, truncate, timeout, ct);
        if (Fabric.UsesDatagramRelays)
            ArmRelayedTamper(LinkDirection.Request, request.Span);
        else if (Fabric.Policy.TryTakeRule(MessageRuleKind.Tamper, LocalName, RemoteName, LinkDirection.Request, request.Span) is { } tamper)
            return TamperedCall(request, tamper, timeout, ct);

        Task<ReadOnlyMemory<byte>> call = Inner.CallAsync(request, timeout, ct);
        if (IsClientSide && type == MessageType.HelloProof)
            ObserveHelloResult(call);
        return call;
    }

    /// <inheritdoc />
    public ValueTask SendAsync(ReadOnlyMemory<byte> message, CancellationToken ct = default)
    {
        if (!Owner.Identity.IsHarness)
            Fabric.CountAttempt(RemoteName);
        if (_silentlyDead)
            return ct.IsCancellationRequested ? ValueTask.FromCanceled(ct) : ValueTask.CompletedTask;
        if (Fabric.Policy.TryTakeRule(MessageRuleKind.Truncate, LocalName, RemoteName, LinkDirection.OneWay, message.Span) is { } truncate)
        {
            int keep = Math.Min(message.Length, truncate.KeepBytes);
            Fabric.Trace($"truncated one-way {TypeOf(message.Span)} {LocalName}->{RemoteName} to {keep.ToString(CultureInfo.InvariantCulture)} of {message.Length.ToString(CultureInfo.InvariantCulture)} byte(s), then closed");
            if (keep >= message.Length)
            {
                ValueTask sent = Inner.SendAsync(message, ct);
                _ = Inner.DisposeAsync();
                return sent;
            }

            // A frame cut short by the end of the stream never reaches the receiving application: its transport discards
            // the partial frame when the connection closes. The bytes were handed to the transport, so the send itself
            // completes; the connection is gone afterwards.
            _ = Inner.DisposeAsync();
            return ValueTask.CompletedTask;
        }

        if (Fabric.UsesDatagramRelays)
        {
            ArmRelayedTamper(LinkDirection.OneWay, message.Span);
        }
        else if (Fabric.Policy.TryTakeRule(MessageRuleKind.Tamper, LocalName, RemoteName, LinkDirection.OneWay, message.Span) is { } tamper)
        {
            Fabric.Trace($"tampered one-way {TypeOf(message.Span)} {LocalName}->{RemoteName} below integrity protection ({tamper.Tamper}): {TamperOutcome(tamper.Tamper)}");
            if (tamper.Tamper == TransportTamper.Replay)
                return Inner.SendAsync(message, ct); // the replayed copy is discarded by the receiver's anti-replay check
            _ = Inner.DisposeAsync();
            return ValueTask.CompletedTask;
        }

        return Inner.SendAsync(message, ct);
    }

    /// <summary>
    /// Relayed QUIC: arms the relay carrying this side's transmissions to alter the encrypted datagram of a transmission that
    /// matches an active tamper rule (<see cref="NetworkFabric.ArmRelay"/>); the rule is consumed only when a datagram is
    /// altered. The frame itself is sent unchanged: QUIC's packet protection must reject the altered packet and its loss
    /// recovery resend the data.
    /// </summary>
    internal void ArmRelayedTamper(LinkDirection direction, ReadOnlySpan<byte> frame)
    {
        if (Fabric.Policy.FindActiveRule(MessageRuleKind.Tamper, LocalName, RemoteName, direction, frame) is not { } rule)
            return;
        string description = $"{direction} {TypeOf(frame)} {LocalName}->{RemoteName}";
        if (!Fabric.ArmRelay(this, rule, description))
            Fabric.Trace($"tamper of {description} not applied: the connection does not run through a datagram relay");
    }

    /// <summary>What the receiver's transport does with a transmission tampered below its integrity protection.</summary>
    internal static string TamperOutcome(TransportTamper mode) => mode == TransportTamper.Replay
        ? "the original is delivered once, the replayed copy is discarded by the receiver's anti-replay check"
        : "the receiver's integrity check rejects it, nothing reaches the application and the connection is torn down";

    /// <inheritdoc />
    public ValueTask DisposeAsync() => Inner.DisposeAsync();

    /// <inheritdoc />
    public override string ToString() => $"{(IsClientSide ? "->" : "<-")} {LocalName}/{RemoteName} ({Inner})";

    internal static MessageType TypeOf(ReadOnlySpan<byte> frame) =>
        frame.Length >= 2 ? (MessageType)BinaryPrimitives.ReadUInt16LittleEndian(frame) : 0;

    private void ObserveHello(ReadOnlySpan<byte> frame)
    {
        try
        {
            if (MessageCodec.Decode(frame) is HelloMessage hello)
            {
                HelloRole = hello.Role;
                HelloPrincipal = hello.Principal;
                HelloName = hello.Name;
            }
        }
        catch (DfsProtocolException)
        {
            // A raw client sending a malformed Hello: nothing to remember.
        }
    }

    private void ObserveHelloResult(Task<ReadOnlyMemory<byte>> call) =>
        _ = call.ContinueWith(static (task, state) =>
        {
            if (!task.IsCompletedSuccessfully)
                return;
            try
            {
                if (MessageCodec.Decode(task.Result.Span) is HelloResultMessage { Accepted: true })
                    ((HarnessConnection)state!)._authenticated = true;
            }
            catch (DfsProtocolException)
            {
                // Not a handshake result.
            }
        }, this, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);

    /// <summary>
    /// A request whose frame is cut after <see cref="MessageRule.KeepBytes"/> bytes by the end of the stream
    /// (<see cref="TestNetwork.TruncateNext"/>). A partial frame is never a message: the receiver's transport discards it when
    /// the connection closes, so the receiving application never sees it and the caller sees the connection close (an
    /// unknown outcome), never a reply to a shortened request. On the simulated network the partial frame really travels
    /// (one fragment, then the close); the realtime transports cannot write part of a frame, so they close at once, which
    /// is what the receiver observes either way. <c>KeepBytes</c> at or above the frame length delivers it, then closes.
    /// </summary>
    private Task<ReadOnlyMemory<byte>> TruncatedCall(ReadOnlyMemory<byte> request, MessageRule rule, TimeSpan timeout, CancellationToken ct)
    {
        int keep = Math.Min(request.Length, rule.KeepBytes);
        Fabric.Trace($"truncated request {TypeOf(request.Span)} {LocalName}->{RemoteName} to {keep.ToString(CultureInfo.InvariantCulture)} of {request.Length.ToString(CultureInfo.InvariantCulture)} byte(s), then closed");
        if (keep >= request.Length)
        {
            Task<ReadOnlyMemory<byte>> whole = Inner.CallAsync(request, timeout, ct);
            _ = Inner.DisposeAsync();
            return whole;
        }

        if (Inner is Simulation.SimConnectionEnd sim)
            return sim.CallInPiecesAsync(request, keep > 0 ? [keep] : [], timeout, closeAfterPieces: true);
        _ = Inner.DisposeAsync();
        return Task.FromException<ReadOnlyMemory<byte>>(new RpcConnectionException(
            $"The connection to {RemoteName} closed after {keep} of {request.Length} request byte(s) (truncated by the harness); the request may not have been received."));
    }

    /// <summary>
    /// A request tampered with below the transport's integrity protection (<see cref="TestNetwork.TamperNext"/>). The
    /// in-memory and simulated transports model the protection the real transport has: an altered protected envelope fails
    /// the receiver's integrity check, so nothing reaches the receiving application and the connection is torn down (a fatal
    /// integrity error); a replayed envelope is discarded by the receiver's anti-replay check, so the original is delivered
    /// exactly once. The application frame itself is never altered (that would test a different property).
    /// </summary>
    private Task<ReadOnlyMemory<byte>> TamperedCall(ReadOnlyMemory<byte> request, MessageRule rule, TimeSpan timeout, CancellationToken ct)
    {
        Fabric.Trace($"tampered request {TypeOf(request.Span)} {LocalName}->{RemoteName} below integrity protection ({rule.Tamper}): {TamperOutcome(rule.Tamper)}");
        if (rule.Tamper == TransportTamper.Replay)
            return Inner.CallAsync(request, timeout, ct);
        _ = Inner.DisposeAsync();
        return Task.FromException<ReadOnlyMemory<byte>>(new RpcConnectionException(
            $"The connection to {RemoteName} was torn down: a transmission failed the receiver's integrity check (tampered by the harness)."));
    }

    /// <summary>A call on a silently dead connection: nothing arrives; the caller's own timeout or cancellation decides.</summary>
    private Task<ReadOnlyMemory<byte>> SilentCallAsync(TimeSpan timeout, CancellationToken ct)
    {
        ClusterRuntime runtime = Fabric.Runtime;
        if (ct.IsCancellationRequested)
            return Task.FromCanceled<ReadOnlyMemory<byte>>(ct);
        var result = new TaskCompletionSource<ReadOnlyMemory<byte>>();
        ITimer? timer = null;
        CancellationTokenRegistration registration = default;
        void Finish(Action complete)
        {
            timer?.Dispose();
            registration.Dispose();
            runtime.Complete(complete);
        }

        if (timeout != Timeout.InfiniteTimeSpan)
        {
            timer = Owner.Clock.CreateTimer(_ => Finish(() => result.TrySetException(new TimeoutException(
                $"The call to {Remote} received no reply within {timeout} (the request may have been executed)."))), null, timeout, Timeout.InfiniteTimeSpan);
        }

        if (ct.CanBeCanceled)
            registration = ct.UnsafeRegister(static (state, token) => ((Action<CancellationToken>)state!)(token), (Action<CancellationToken>)(token => Finish(() => result.TrySetCanceled(token))));
        return result.Task;
    }

    private void OnInnerClosed(IRpcConnection inner, Exception? error)
    {
        Action<IRpcConnection, Exception?>? handlers;
        lock (_gate)
        {
            if (_isClosed)
                return;
            _isClosed = true;
            _closeError = error;
            handlers = _closed;
            _closed = null;
        }

        Fabric.RemoveConnection(this);
        if (Relay is { } relay)
            Fabric.ReleaseRelay(relay);
        if (handlers is null)
            return;
        foreach (Delegate handler in handlers.GetInvocationList())
        {
            try
            {
                ((Action<IRpcConnection, Exception?>)handler)(this, error);
            }
            catch (Exception ex) when (ex is not OutOfMemoryException)
            {
                Fabric.Trace($"a Closed handler of {this} failed: {ex.GetType().Name}: {ex.Message}");
            }
        }
    }
}

/// <summary>
/// The handler wrapper of a fabric transport: holds inbound requests/one-way messages and outgoing replies, drops replies,
/// rewrites read results for <see cref="TestFaults.LimitReadResultBytes"/>, and truncates or tampers replies.
/// </summary>
internal sealed class HarnessHandler : IRpcHandler
{
    private readonly HarnessTransport _transport;
    private readonly IRpcHandler _inner;

    public HarnessHandler(HarnessTransport transport, IRpcHandler inner)
    {
        _transport = transport;
        _inner = inner;
    }

    private NetworkFabric Fabric => _transport.Fabric;

    /// <inheritdoc />
    public ValueTask<ReadOnlyMemory<byte>?> HandleAsync(IRpcConnection connection, ReadOnlyMemory<byte> request, bool expectsReply, CancellationToken ct)
    {
        HarnessConnection wrapped = Fabric.GetServerConnection(_transport, connection);
        if (wrapped.SilentlyDead)
            return new ValueTask<ReadOnlyMemory<byte>?>((ReadOnlyMemory<byte>?)null);
        LinkDirection direction = expectsReply ? LinkDirection.Request : LinkDirection.OneWay;
        if (Fabric.TryHold(wrapped.RemoteName, _transport.LocalName, direction, request.Span, wrapped) is { } held)
            return new ValueTask<ReadOnlyMemory<byte>?>(DispatchWhenReleased(held, wrapped, expectsReply, ct));
        return Dispatch(wrapped, request, expectsReply, ct);
    }

    private ValueTask<ReadOnlyMemory<byte>?> Dispatch(HarnessConnection connection, ReadOnlyMemory<byte> request, bool expectsReply, CancellationToken ct)
    {
        MessageRule? dropReply = null;
        if (expectsReply)
        {
            if (!_transport.Identity.IsHarness)
                Fabric.CountAttempt(connection.RemoteName);
            dropReply = Fabric.Policy.TryTakeRule(MessageRuleKind.DropReply, connection.RemoteName, _transport.LocalName, LinkDirection.Request, request.Span);
        }

        ValueTask<ReadOnlyMemory<byte>?> pending;
        try
        {
            pending = _inner.HandleAsync(connection, request, expectsReply, ct);
        }
        catch (Exception ex)
        {
            return ValueTask.FromException<ReadOnlyMemory<byte>?>(ex);
        }

        if (!expectsReply || (dropReply is null && !Fabric.NeedsReplyInterposition))
            return pending;
        return new ValueTask<ReadOnlyMemory<byte>?>(ProcessReply(pending, connection, request, dropReply));
    }

    private Task<ReadOnlyMemory<byte>?> DispatchWhenReleased(HeldMessage held, HarnessConnection connection, bool expectsReply, CancellationToken ct)
    {
        var result = new TaskCompletionSource<ReadOnlyMemory<byte>?>();
        ReadOnlyMemory<byte> frame = held.Message.Frame;
        _ = held.Decision.Task.ContinueWith(decision =>
        {
            if (!decision.Result || !connection.IsOpen)
            {
                Fabric.Runtime.Complete(() => result.TrySetResult(null));
                return;
            }

            ValueTask<ReadOnlyMemory<byte>?> dispatched = Dispatch(connection, frame, expectsReply, ct);
            Propagate(dispatched, result);
        }, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
        return result.Task;
    }

    private Task<ReadOnlyMemory<byte>?> ProcessReply(ValueTask<ReadOnlyMemory<byte>?> pending, HarnessConnection connection, ReadOnlyMemory<byte> request,
        MessageRule? dropReply)
    {
        var result = new TaskCompletionSource<ReadOnlyMemory<byte>?>();
        _ = pending.AsTask().ContinueWith(task =>
        {
            if (!task.IsCompletedSuccessfully)
            {
                Propagate(new ValueTask<ReadOnlyMemory<byte>?>(task), result);
                return;
            }

            if (task.Result is not { } reply)
            {
                Fabric.Runtime.Complete(() => result.TrySetResult(null));
                return;
            }

            if (dropReply is not null)
            {
                Fabric.Trace($"dropped reply {HarnessConnection.TypeOf(reply.Span)} {_transport.LocalName}->{connection.RemoteName}");
                Fabric.Runtime.Complete(() => result.TrySetResult(null));
                return;
            }

            if (Fabric.TryHold(_transport.LocalName, connection.RemoteName, LinkDirection.Reply, reply.Span, connection) is { } held)
            {
                ReadOnlyMemory<byte> frame = held.Message.Frame;
                _ = held.Decision.Task.ContinueWith(decision =>
                {
                    if (decision.Result && connection.IsOpen)
                        FinishReply(connection, request, frame, result);
                    else
                        Fabric.Runtime.Complete(() => result.TrySetResult(null));
                }, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
                return;
            }

            FinishReply(connection, request, reply, result);
        }, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
        return result.Task;
    }

    private void FinishReply(HarnessConnection connection, ReadOnlyMemory<byte> request, ReadOnlyMemory<byte> reply, TaskCompletionSource<ReadOnlyMemory<byte>?> result)
    {
        reply = LimitReadResult(connection, request, reply);
        bool close = false;
        bool deliver = true;
        if (Fabric.Policy.TryTakeRule(MessageRuleKind.Truncate, _transport.LocalName, connection.RemoteName, LinkDirection.Reply, reply.Span) is { } truncate)
        {
            // A reply frame cut short by the end of the stream is discarded by the caller's transport when the connection
            // closes: the caller sees the connection close, never a shortened reply (TruncatedCall explains the model).
            int keep = Math.Min(reply.Length, truncate.KeepBytes);
            Fabric.Trace($"truncated reply {HarnessConnection.TypeOf(reply.Span)} {_transport.LocalName}->{connection.RemoteName} to {keep.ToString(CultureInfo.InvariantCulture)} of {reply.Length.ToString(CultureInfo.InvariantCulture)} byte(s), then closed");
            deliver = keep >= reply.Length;
            close = true;
        }
        else if (Fabric.UsesDatagramRelays)
        {
            connection.ArmRelayedTamper(LinkDirection.Reply, reply.Span);
        }
        else if (Fabric.Policy.TryTakeRule(MessageRuleKind.Tamper, _transport.LocalName, connection.RemoteName, LinkDirection.Reply, reply.Span) is { } tamper)
        {
            // Below integrity protection: an altered envelope is rejected by the caller's transport (connection torn down),
            // a replayed one is discarded by its anti-replay check (the original reply is delivered once).
            Fabric.Trace($"tampered reply {HarnessConnection.TypeOf(reply.Span)} {_transport.LocalName}->{connection.RemoteName} below integrity protection ({tamper.Tamper}): {HarnessConnection.TamperOutcome(tamper.Tamper)}");
            if (tamper.Tamper != TransportTamper.Replay)
            {
                deliver = false;
                close = true;
            }
        }

        ReadOnlyMemory<byte>? final = deliver ? reply : (ReadOnlyMemory<byte>?)null; // null = no reply (an untyped null would convert to an empty frame)
        Fabric.Runtime.Complete(() => result.TrySetResult(final));
        if (close)
            _ = connection.Inner.DisposeAsync();
    }

    /// <summary>
    /// Rewrites replies for <see cref="TestFaults.LimitReadResultBytes"/> (read results sent to client principals) and
    /// <see cref="TestBackupNode.SetFalseInventoryClaimsAsync"/> (inventory hints of a backup node).
    /// </summary>
    private ReadOnlyMemory<byte> LimitReadResult(HarnessConnection connection, ReadOnlyMemory<byte> request, ReadOnlyMemory<byte> reply)
    {
        MessageType type = HarnessConnection.TypeOf(reply.Span);
        if (type == MessageType.BackupInventoryResult)
            return ClaimInventory(request, reply);
        if (type is not (MessageType.ReadRangeResult or MessageType.GetObjectResult))
            return reply;
        if (connection.Inner.Identity is not { IsNode: false, IsBackup: false, Principal: not null })
            return reply;
        try
        {
            IWireMessage requestMessage = MessageCodec.Decode(request.Span);
            string? volume = requestMessage switch
            {
                ReadRangeMessage read => read.Header.Volume,
                GetObjectMessage get => Fabric.VolumeNameOf(get.Volume),
                _ => null,
            };
            if (Fabric.FindReadLimit(_transport.LocalName, volume) is not { } rule)
                return reply;
            IWireMessage replyMessage = MessageCodec.Decode(reply.Span);
            IWireMessage? shortened = replyMessage switch
            {
                ReadRangeResultMessage range => ShortenRange(range, rule.MaxBytesPerResult),
                GetObjectResultMessage obj when requestMessage is GetObjectMessage get => ShortenObject(obj, get.Offset, rule.MaxBytesPerResult),
                _ => null,
            };
            if (shortened is null)
                return reply;
            rule.CountApplied();
            return MessageCodec.Encode(shortened);
        }
        catch (DfsProtocolException)
        {
            return reply;
        }
    }

    private ReadOnlyMemory<byte> ClaimInventory(ReadOnlyMemory<byte> request, ReadOnlyMemory<byte> reply)
    {
        try
        {
            if (MessageCodec.Decode(request.Span) is not BackupInventoryMessage inventory)
                return reply;
            IReadOnlySet<ObjectId>? claims = Fabric.InventoryClaimsFor(_transport.LocalName, Fabric.VolumeNameOf(inventory.Key.Volume));
            if (claims is null || MessageCodec.Decode(reply.Span) is not BackupInventoryResultMessage result)
                return reply;
            ObjectId[] missing = result.Missing.Where(id => !claims.Contains(id)).ToArray();
            if (missing.Length == result.Missing.Count)
                return reply;
            return MessageCodec.Encode(result with { Missing = missing });
        }
        catch (DfsProtocolException)
        {
            return reply;
        }
    }

    private static ReadRangeResultMessage? ShortenRange(ReadRangeResultMessage range, int max)
    {
        if (!range.IsInline || range.InlineData.Length <= max)
            return null;
        return range with { InlineData = range.InlineData[..max], Length = max };
    }

    private static GetObjectResultMessage? ShortenObject(GetObjectResultMessage result, long requestedOffset, int max)
    {
        // The reply may start before the requested offset (aligned down to a verification unit): keep the bytes up to
        // `max` bytes past the requested offset, so the result is a prefix of the *requested* range (plus the alignment
        // bytes in front of it) and every call makes progress.
        long lead = requestedOffset - result.Offset;
        if (lead < 0 || lead > result.Data.Length)
            return null;
        long keepLong = lead + max;
        if (result.Data.Length <= keepLong)
            return null;
        int keep = (int)keepLong;
        ReadOnlyMemory<byte> data = result.Data[..keep];
        if (result.UnitSize > 0)
        {
            int units = (keep + result.UnitSize - 1) / result.UnitSize;
            var checksums = new uint[units];
            for (int i = 0; i < units; i++)
            {
                int start = i * result.UnitSize;
                int length = Math.Min(result.UnitSize, keep - start);
                checksums[i] = length == result.UnitSize && i < result.UnitChecksums.Count
                    ? result.UnitChecksums[i]
                    : ObjectChecksum.Crc32C(data.Span.Slice(start, length));
            }

            return result with { Data = data, UnitChecksums = checksums };
        }

        // Without unit checksums only a whole-object read can be verified; a prefix starting at offset 0 becomes a one-unit
        // result whose checksum covers exactly the returned bytes.
        if (result.Offset != 0)
            return null;
        return result with { Data = data, UnitSize = keep, UnitChecksums = [ObjectChecksum.Crc32C(data.Span)] };
    }

    private void Propagate(ValueTask<ReadOnlyMemory<byte>?> source, TaskCompletionSource<ReadOnlyMemory<byte>?> target)
    {
        ClusterRuntime runtime = Fabric.Runtime;
        if (source.IsCompleted)
        {
            Complete(source.AsTask(), target, runtime);
            return;
        }

        _ = source.AsTask().ContinueWith(task => Complete(task, target, runtime), CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);

        static void Complete(Task<ReadOnlyMemory<byte>?> task, TaskCompletionSource<ReadOnlyMemory<byte>?> target, ClusterRuntime runtime)
        {
            if (task.IsCompletedSuccessfully)
                runtime.Complete(() => target.TrySetResult(task.Result));
            else if (task.IsCanceled)
                runtime.Complete(() => target.TrySetCanceled());
            else
                runtime.Complete(() => target.TrySetException(task.Exception!.InnerExceptions));
        }
    }
}
