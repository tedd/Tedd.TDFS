using System.Globalization;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Testing.Simulation;

/// <summary>Kinds of frames travelling on a simulated connection.</summary>
internal enum SimEnvelopeKind
{
    Request,
    Reply,
    OneWay,
    Close,
}

/// <summary>One frame on a simulated connection. The payload is a private copy taken at send time.</summary>
internal sealed class SimEnvelope(SimEnvelopeKind kind, long correlation, byte[] payload, Exception? error = null)
{
    public SimEnvelopeKind Kind { get; } = kind;

    public long Correlation { get; } = correlation;

    public byte[] Payload { get; } = payload;

    /// <summary>For <see cref="SimEnvelopeKind.Close"/>: null for a graceful close, the reset error otherwise.</summary>
    public Exception? Error { get; } = error;

    public string Describe(long connectionId, string from, string to) =>
        string.Create(CultureInfo.InvariantCulture, $"{Kind} c{connectionId}#{Correlation} {from}->{to} ({Payload.Length} B)");
}

/// <summary>A transport created by <see cref="SimNetwork"/>. Mutable state is touched on the simulation thread.</summary>
internal sealed class SimTransport : IRpcTransport
{
    private readonly SimNetwork _network;
    private readonly List<SimConnectionEnd> _connections = [];
    private readonly List<Action> _connectAborts = [];

    public SimTransport(SimNetwork network, string localName, string localEndpoint, SimProcess owner)
    {
        _network = network;
        LocalName = localName;
        LocalEndpoint = localEndpoint;
        Owner = owner;
    }

    public string LocalName { get; }

    public string LocalEndpoint { get; }

    public SimProcess Owner { get; }

    public IRpcHandler? Handler { get; set; }

    public bool IsStarted { get; set; }

    public bool IsDisposed { get; set; }

    /// <summary>True once the owning process was killed.</summary>
    public bool IsDead { get; set; }

    public bool IsAccepting => IsStarted && !IsDisposed && !IsDead && Owner.State != SimProcessState.Killed;

    public Task StartAsync(IRpcHandler handler, CancellationToken ct = default) => _network.Start(this, handler, ct);

    public Task<IRpcConnection> ConnectAsync(string endpoint, CancellationToken ct = default) => _network.Connect(this, endpoint, ct);

    public ValueTask DisposeAsync()
    {
        _network.DisposeTransport(this);
        return ValueTask.CompletedTask;
    }

    public SimConnectionEnd[] SnapshotConnections()
    {
        lock (_connections)
            return [.. _connections];
    }

    public void AddConnection(SimConnectionEnd end)
    {
        lock (_connections)
            _connections.Add(end);
    }

    public void RemoveConnection(SimConnectionEnd end)
    {
        lock (_connections)
            _connections.Remove(end);
    }

    /// <summary>Registers the abort callback of an outstanding connect (run when the transport is disposed).</summary>
    public void AddConnectAbort(Action abort)
    {
        lock (_connections)
            _connectAborts.Add(abort);
    }

    public void RemoveConnectAbort(Action abort)
    {
        lock (_connections)
            _connectAborts.Remove(abort);
    }

    /// <summary>Removes and returns the abort callbacks of every outstanding connect, in start order.</summary>
    public Action[] TakeConnectAborts()
    {
        lock (_connections)
        {
            Action[] aborts = [.. _connectAborts];
            _connectAborts.Clear();
            return aborts;
        }
    }

    public override string ToString() => $"SimTransport({LocalName}, {LocalEndpoint})";
}

/// <summary>One end of a simulated connection.</summary>
internal sealed class SimConnectionEnd : IRpcConnection
{
    private readonly object _sync = new();
    private readonly SimNetwork _network;
    private readonly SortedDictionary<long, PendingCall> _pending = [];
    private readonly CancellationTokenSource _handlerCts = new();
    private Action<IRpcConnection, Exception?>? _closedHandlers;
    private long _nextCorrelation;
    private long _lastArrival;
    private bool _closed;

    public SimConnectionEnd(SimNetwork network, SimTransport transport, long connectionId, RpcAddress remote, SimLane inboundLane)
    {
        _network = network;
        Transport = transport;
        ConnectionId = connectionId;
        Remote = remote;
        InboundLane = inboundLane;
    }

    public SimTransport Transport { get; }

    public long ConnectionId { get; }

    /// <summary>FIFO lane (owned by this end's process) on which frames addressed to this end are processed.</summary>
    public SimLane InboundLane { get; }

    public SimConnectionEnd Peer { get; set; } = null!;

    public RpcAddress Remote { get; }

    public bool IsOpen
    {
        get
        {
            lock (_sync)
                return !_closed;
        }
    }

    public RpcPeerIdentity? Identity { get; set; }

    public event Action<IRpcConnection, Exception?>? Closed
    {
        add
        {
            lock (_sync)
                _closedHandlers += value;
        }
        remove
        {
            lock (_sync)
                _closedHandlers -= value;
        }
    }

    public Task<ReadOnlyMemory<byte>> CallAsync(ReadOnlyMemory<byte> request, TimeSpan timeout, CancellationToken ct = default)
    {
        if (timeout <= TimeSpan.Zero && timeout != Timeout.InfiniteTimeSpan)
            throw new ArgumentOutOfRangeException(nameof(timeout), timeout, "The timeout must be positive or Timeout.InfiniteTimeSpan.");
        _network.CheckThread("CallAsync");
        if (ct.IsCancellationRequested)
            return Task.FromCanceled<ReadOnlyMemory<byte>>(ct);

        PendingCall call;
        lock (_sync)
        {
            if (_closed)
                return Task.FromException<ReadOnlyMemory<byte>>(new RpcConnectionException($"{Describe()} is closed."));
            call = new PendingCall(this, ++_nextCorrelation, timeout, ct);
            _pending.Add(call.Correlation, call);
        }

        if (timeout != Timeout.InfiniteTimeSpan)
        {
            TimeProvider clock = _network.GetClock(Transport.LocalName);
            call.Timer = clock.CreateTimer(static s => ((PendingCall)s!).OnTimeout(), call, timeout, Timeout.InfiniteTimeSpan);
        }

        if (ct.CanBeCanceled)
            call.Registration = ct.Register(static s => ((PendingCall)s!).OnCanceled(), call);

        if (!call.Task.IsCompleted)
            _network.Transmit(this, new SimEnvelope(SimEnvelopeKind.Request, call.Correlation, request.ToArray()));
        return call.Task;
    }

    public ValueTask SendAsync(ReadOnlyMemory<byte> message, CancellationToken ct = default)
    {
        _network.CheckThread("SendAsync");
        if (ct.IsCancellationRequested)
            return ValueTask.FromCanceled(ct);
        if (!IsOpen)
            return ValueTask.FromException(new RpcConnectionException($"{Describe()} is closed."));
        _network.Transmit(this, new SimEnvelope(SimEnvelopeKind.OneWay, 0, message.ToArray()));
        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        _network.CheckThread("dispose connection");
        CloseLocal(null, notifyPeer: true);
        return ValueTask.CompletedTask;
    }

    public override string ToString() => Describe();

    /// <summary>Returns the arrival time of the next FIFO frame for this end (never earlier than the previous one).</summary>
    public long ReserveArrival(long arrival)
    {
        lock (_sync)
        {
            if (arrival < _lastArrival)
                arrival = _lastArrival;
            _lastArrival = arrival;
            return arrival;
        }
    }

    /// <summary>Processes one frame addressed to this end (runs as work of this end's process).</summary>
    public void Receive(SimEnvelope envelope)
    {
        if (!IsOpen || Transport.IsDead)
        {
            _network.CountDroppedAtClosedEnd();
            return;
        }

        switch (envelope.Kind)
        {
            case SimEnvelopeKind.Request:
                _network.CountDelivered();
                Dispatch(envelope, expectsReply: true);
                break;
            case SimEnvelopeKind.OneWay:
                _network.CountDelivered();
                Dispatch(envelope, expectsReply: false);
                break;
            case SimEnvelopeKind.Reply:
                _network.CountDelivered();
                PendingCall? call;
                lock (_sync)
                    _pending.Remove(envelope.Correlation, out call);
                if (call is null)
                    _network.CountLateReply();
                else
                    call.Complete(envelope.Payload);
                break;
            case SimEnvelopeKind.Close:
                CloseLocal(envelope.Error, notifyPeer: false);
                break;
        }
    }

    /// <summary>
    /// Closes this end: pending calls fail with <see cref="RpcConnectionException"/>, in-flight handlers are cancelled,
    /// <see cref="Closed"/> is raised once and (when <paramref name="notifyPeer"/>) the peer is told after the frames
    /// already sent.
    /// </summary>
    public void CloseLocal(Exception? error, bool notifyPeer)
    {
        PendingCall[] calls;
        Action<IRpcConnection, Exception?>? handlers;
        lock (_sync)
        {
            if (_closed)
                return;
            _closed = true;
            calls = [.. _pending.Values];
            _pending.Clear();
            handlers = _closedHandlers;
            _closedHandlers = null;
        }

        Transport.RemoveConnection(this);
        if (notifyPeer && !Transport.IsDead)
            _network.Transmit(this, new SimEnvelope(SimEnvelopeKind.Close, 0, [], error));

        // Cancelling the handler token, failing calls and raising Closed complete tasks that SDK-style code awaits with
        // ConfigureAwait(false). The close may be initiated from a simulated main lane (DisposeAsync on a node loop or the
        // client context); without removing that context the runtime would queue those continuations to the thread pool.
        SimScheduler.CompleteWithoutContext(() => NotifyClosed(error, calls, handlers));
    }

    /// <summary>
    /// Marks the end closed because its process died: nothing observable happens on this side (its code no longer runs).
    /// Pending calls stay registered so callers that live in another process (a test body using this transport) still
    /// complete through their own timeout or cancellation; replies are no longer accepted. Returns true when it was open.
    /// </summary>
    public bool MarkDead()
    {
        lock (_sync)
        {
            if (_closed)
                return false;
            _closed = true;
            _closedHandlers = null;
            return true;
        }
    }

    private void NotifyClosed(Exception? error, PendingCall[] calls, Action<IRpcConnection, Exception?>? handlers)
    {
        try
        {
            _handlerCts.Cancel();
        }
        catch (Exception ex)
        {
            _network.ReportCallbackFailure(this, "handler cancellation callback", ex);
        }

        string reason = error is null ? "closed" : "reset: " + error.Message;
        foreach (PendingCall call in calls)
            call.Fail(new RpcConnectionException($"{Describe()} {reason}", error));

        if (handlers is null)
            return;
        foreach (Delegate handler in handlers.GetInvocationList())
        {
            try
            {
                ((Action<IRpcConnection, Exception?>)handler)(this, error);
            }
            catch (Exception ex)
            {
                _network.ReportCallbackFailure(this, "Closed handler", ex);
            }
        }
    }

    private bool RemovePending(long correlation)
    {
        lock (_sync)
            return _pending.Remove(correlation);
    }

    private string Describe() =>
        string.Create(CultureInfo.InvariantCulture, $"Connection {ConnectionId} ({Transport.LocalName} -> {Remote})");

    private void Dispatch(SimEnvelope envelope, bool expectsReply)
    {
        IRpcHandler? handler = Transport.Handler;
        if (handler is null)
        {
            // A transport that never started has nobody to answer requests.
            if (expectsReply)
                _network.ResetPair(this, $"{Transport.LocalEndpoint} does not accept requests (its transport was not started).");
            return;
        }

        ValueTask<ReadOnlyMemory<byte>?> pending;
        try
        {
            pending = handler.HandleAsync(this, envelope.Payload, expectsReply, _handlerCts.Token);
        }
        catch (Exception ex)
        {
            _network.ReportHandlerFailure(this, ex);
            return;
        }

        if (pending.IsCompletedSuccessfully)
        {
            ReadOnlyMemory<byte>? reply = pending.Result;
            if (expectsReply && reply is { } payload)
                SendReply(envelope.Correlation, payload);
            return;
        }

        // Continue synchronously wherever the handler's task completes (normally on a simulated loop). ContinueWith with
        // ExecuteSynchronously inlines even when the completing thread has a synchronization context installed, unlike a
        // ConfigureAwait(false) await, which the runtime would hand to the thread pool there.
        pending.AsTask().ContinueWith(
            static (task, state) =>
            {
                var (end, correlation, reply) = ((SimConnectionEnd, long, bool))state!;
                end.OnHandlerCompleted(task, correlation, reply);
            },
            (this, envelope.Correlation, expectsReply),
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }

    private void OnHandlerCompleted(Task<ReadOnlyMemory<byte>?> task, long correlation, bool expectsReply)
    {
        if (task.IsCanceled && _handlerCts.IsCancellationRequested)
            return;
        if (!task.IsCompletedSuccessfully)
        {
            if (IsOpen && !Transport.IsDead)
            {
                Exception error = task.Exception?.GetBaseException() ?? new OperationCanceledException("The RPC handler was cancelled.");
                _network.ReportHandlerFailure(this, error);
            }

            return;
        }

        if (expectsReply && task.Result is { } payload)
            SendReply(correlation, payload);
    }

    private void SendReply(long correlation, ReadOnlyMemory<byte> payload)
    {
        if (!IsOpen || Transport.IsDead)
            return;
        _network.Transmit(this, new SimEnvelope(SimEnvelopeKind.Reply, correlation, payload.ToArray()));
    }

    /// <summary>An outstanding <see cref="CallAsync"/>.</summary>
    private sealed class PendingCall
    {
        private readonly SimConnectionEnd _end;
        private readonly TimeSpan _timeout;
        private readonly CancellationToken _ct;
        private readonly TaskCompletionSource<ReadOnlyMemory<byte>> _source = new();

        public PendingCall(SimConnectionEnd end, long correlation, TimeSpan timeout, CancellationToken ct)
        {
            _end = end;
            Correlation = correlation;
            _timeout = timeout;
            _ct = ct;
        }

        public long Correlation { get; }

        public ITimer? Timer { get; set; }

        public CancellationTokenRegistration Registration { get; set; }

        public Task<ReadOnlyMemory<byte>> Task => _source.Task;

        public void Complete(byte[] payload)
        {
            Cleanup();
            SimScheduler.CompleteWithoutContext(() => _source.TrySetResult(payload));
        }

        public void Fail(Exception exception)
        {
            Cleanup();
            SimScheduler.CompleteWithoutContext(() => _source.TrySetException(exception));
        }

        public void OnTimeout()
        {
            if (!_end.RemovePending(Correlation))
                return;
            Cleanup();
            var error = new TimeoutException(
                $"No reply from {_end.Remote} within {_timeout} on connection {_end.ConnectionId}; the request may have been executed.");
            SimScheduler.CompleteWithoutContext(() => _source.TrySetException(error));
        }

        public void OnCanceled()
        {
            if (!_end.RemovePending(Correlation))
                return;
            Cleanup();
            SimScheduler.CompleteWithoutContext(() => _source.TrySetCanceled(_ct));
        }

        private void Cleanup()
        {
            ITimer? timer = Timer;
            Timer = null;
            timer?.Dispose();
            Registration.Dispose();
        }
    }
}
