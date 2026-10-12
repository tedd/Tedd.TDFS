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
internal sealed class HarnessTransport : IRpcTransport, IRpcTransportControl
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
        IRpcConnection inner = await Inner.ConnectAsync(endpoint, ct).ConfigureAwait(false);
        string remote = NetworkFabric.RemoteNameOf(inner) ?? target ?? inner.Remote.ToString();
        var connection = new HarnessConnection(this, inner, remote, isClientSide: true, endpoint);
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

    /// <summary>True when <paramref name="other"/> authenticated with the same role and principal.</summary>
    public bool HasSameIdentity(HarnessConnection other) =>
        HelloRole == other.HelloRole && string.Equals(HelloPrincipal, other.HelloPrincipal, StringComparison.Ordinal);

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
        if (Fabric.Policy.TryTakeRule(MessageRuleKind.Tamper, LocalName, RemoteName, LinkDirection.Request, request.Span) is { } tamper)
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
            Fabric.Trace($"truncated one-way {TypeOf(message.Span)} {LocalName}->{RemoteName} to {truncate.KeepBytes.ToString(CultureInfo.InvariantCulture)} byte(s)");
            ValueTask sent = Inner.SendAsync(message[..Math.Min(message.Length, truncate.KeepBytes)], ct);
            _ = Inner.DisposeAsync();
            return sent;
        }

        if (Fabric.Policy.TryTakeRule(MessageRuleKind.Tamper, LocalName, RemoteName, LinkDirection.OneWay, message.Span) is { } tamper)
        {
            Fabric.Trace($"tampered one-way {TypeOf(message.Span)} {LocalName}->{RemoteName} ({tamper.Tamper})");
            if (tamper.Tamper == TransportTamper.Replay)
            {
                _ = Inner.SendAsync(message, ct);
                return Inner.SendAsync(message, ct);
            }

            return Inner.SendAsync(Fabric.Tamper(message.Span, tamper.Tamper), ct);
        }

        return Inner.SendAsync(message, ct);
    }

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

    private Task<ReadOnlyMemory<byte>> TruncatedCall(ReadOnlyMemory<byte> request, MessageRule rule, TimeSpan timeout, CancellationToken ct)
    {
        int keep = Math.Min(request.Length, rule.KeepBytes);
        Fabric.Trace($"truncated request {TypeOf(request.Span)} {LocalName}->{RemoteName} to {keep.ToString(CultureInfo.InvariantCulture)} of {request.Length.ToString(CultureInfo.InvariantCulture)} byte(s), then closed");
        Task<ReadOnlyMemory<byte>> call = Inner.CallAsync(request[..keep], timeout, ct);
        _ = Inner.DisposeAsync();
        return call;
    }

    private Task<ReadOnlyMemory<byte>> TamperedCall(ReadOnlyMemory<byte> request, MessageRule rule, TimeSpan timeout, CancellationToken ct)
    {
        Fabric.Trace($"tampered request {TypeOf(request.Span)} {LocalName}->{RemoteName} ({rule.Tamper})");
        if (rule.Tamper == TransportTamper.Replay)
        {
            Task<ReadOnlyMemory<byte>> first = Inner.CallAsync(request, timeout, ct);
            Task<ReadOnlyMemory<byte>> replayed = Inner.CallAsync(request, timeout, ct);
            _ = replayed.ContinueWith(static t => _ = t.Exception, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
            return first;
        }

        return Inner.CallAsync(Fabric.Tamper(request.Span, rule.Tamper), timeout, ct);
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
        if (Fabric.Policy.TryTakeRule(MessageRuleKind.Truncate, _transport.LocalName, connection.RemoteName, LinkDirection.Reply, reply.Span) is { } truncate)
        {
            int keep = Math.Min(reply.Length, truncate.KeepBytes);
            Fabric.Trace($"truncated reply {HarnessConnection.TypeOf(reply.Span)} {_transport.LocalName}->{connection.RemoteName} to {keep.ToString(CultureInfo.InvariantCulture)} of {reply.Length.ToString(CultureInfo.InvariantCulture)} byte(s), then closed");
            reply = reply[..keep];
            close = true;
        }
        else if (Fabric.Policy.TryTakeRule(MessageRuleKind.Tamper, _transport.LocalName, connection.RemoteName, LinkDirection.Reply, reply.Span) is { } tamper)
        {
            // A replayed reply is discarded by the caller's transport (its correlation is already answered).
            Fabric.Trace($"tampered reply {HarnessConnection.TypeOf(reply.Span)} {_transport.LocalName}->{connection.RemoteName} ({tamper.Tamper})");
            if (tamper.Tamper != TransportTamper.Replay)
                reply = Fabric.Tamper(reply.Span, tamper.Tamper);
        }

        ReadOnlyMemory<byte> final = reply;
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
                GetObjectResultMessage obj => ShortenObject(obj, rule.MaxBytesPerResult),
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

    private static GetObjectResultMessage? ShortenObject(GetObjectResultMessage result, int max)
    {
        if (result.Data.Length <= max)
            return null;
        ReadOnlyMemory<byte> data = result.Data[..max];
        if (result.UnitSize > 0)
        {
            int units = (max + result.UnitSize - 1) / result.UnitSize;
            var checksums = new uint[units];
            for (int i = 0; i < units; i++)
            {
                int start = i * result.UnitSize;
                int length = Math.Min(result.UnitSize, max - start);
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
        return result with { Data = data, UnitSize = max, UnitChecksums = [ObjectChecksum.Crc32C(data.Span)] };
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
