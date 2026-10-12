using System.Collections.Concurrent;
using Tedd.Tdfs.Protocol;

namespace Tedd.Tdfs.Transport.Internal;

/// <summary>Why a connection closed.</summary>
internal enum RpcCloseOrigin
{
    /// <summary>The connection's owner disposed it (graceful; <see cref="IRpcConnection.Closed"/> carries null).</summary>
    Local,

    /// <summary>The owning transport was disposed (graceful from this end; the remote end is told).</summary>
    TransportDisposed,

    /// <summary>An explicit reset (<see cref="IRpcTransportControl.ResetConnections"/>); both ends see an exception.</summary>
    Reset,

    /// <summary>The remote end closed the connection or the link failed; the remote end is not told.</summary>
    Remote,

    /// <summary>This end detected a failure (protocol violation, send failure); the remote end is told.</summary>
    Fault,
}

/// <summary>
/// Transport-independent half of an <see cref="IRpcConnection"/>: correlation of calls and replies with timeouts,
/// one-way messages, link-policy interposition on receipt, in-order dispatch of inbound messages to the
/// <see cref="IRpcHandler"/> on the thread pool, error replies for failing handlers, and close semantics.
/// </summary>
/// <remarks>
/// <para><b>Delivery.</b> Inbound items are processed by a per-connection <see cref="SerialExecutor{T}"/>: the link
/// policy is consulted (from = remote identity, to = local identity) and the message is delivered in arrival order.
/// Handlers are invoked in order but not awaited before the next message is dispatched, so a slow request does not
/// block the connection. Delayed messages are re-posted when their delay elapses and may be overtaken.</para>
/// <para><b>Completion.</b> Call results complete with <see cref="TaskCreationOptions.RunContinuationsAsynchronously"/>,
/// so a caller's continuation never runs on a transport thread. This class is used by the realtime transports only;
/// deterministic simulation has its own transport.</para>
/// </remarks>
internal abstract class RpcConnectionBase : IRpcConnection
{
    /// <summary>Largest timer due time accepted by <see cref="TimeProvider.CreateTimer"/>; longer timeouts are clamped.</summary>
    private static readonly TimeSpan MaxTimerDue = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

    private const int StateOpen = 0;
    private const int StateClosed = 1;

    private readonly ConcurrentDictionary<ulong, PendingCall> _pending = new();
    private readonly ConcurrentDictionary<DelayedDelivery, byte> _delayed = new(ReferenceEqualityComparer.Instance);
    private readonly SerialExecutor<InboundItem> _inbound;
    private readonly CancellationTokenSource _closing = new();
    private readonly object _gate = new();
    private Action<IRpcConnection, Exception?>? _closed;
    private Exception? _closeError;
    private int _state;
    private long _nextCorrelation;
    private string? _remoteName;
    private RpcPeerIdentity? _identity;

    /// <summary>Creates the connection.</summary>
    /// <param name="id">Transport-unique id.</param>
    /// <param name="remote">The remote endpoint.</param>
    /// <param name="remoteName">The remote network identity when already known.</param>
    protected RpcConnectionBase(long id, RpcAddress remote, string? remoteName)
    {
        Id = id;
        Remote = remote;
        _remoteName = remoteName;
        _inbound = new SerialExecutor<InboundItem>(ProcessInbound, ex => ReportFault("Inbound message processing failed.", ex));
    }

    /// <summary>Transport-unique connection id.</summary>
    public long Id { get; }

    /// <inheritdoc />
    public RpcAddress Remote { get; }

    /// <summary>The remote network identity (link-policy name), or null while unknown.</summary>
    public string? RemoteName
    {
        get => Volatile.Read(ref _remoteName);
        protected set => Volatile.Write(ref _remoteName, value);
    }

    /// <inheritdoc />
    public bool IsOpen => Volatile.Read(ref _state) == StateOpen;

    /// <inheritdoc />
    public RpcPeerIdentity? Identity
    {
        get => Volatile.Read(ref _identity);
        set => Volatile.Write(ref _identity, value);
    }

    /// <inheritdoc />
    /// <remarks>
    /// Handlers run on the thread pool. A handler added after the connection closed is invoked (asynchronously) with the
    /// close exception, so a close that races with the subscription is never missed.
    /// </remarks>
    public event Action<IRpcConnection, Exception?>? Closed
    {
        add
        {
            if (value is null)
                return;
            Exception? error;
            lock (_gate)
            {
                if (_state == StateOpen)
                {
                    _closed += value;
                    return;
                }

                error = _closeError;
            }

            RaiseClosed(value, error);
        }
        remove
        {
            lock (_gate)
            {
                _closed -= value;
            }
        }
    }

    /// <summary>Number of calls waiting for a reply.</summary>
    internal int PendingCallCount => _pending.Count;

    /// <summary>The exception the connection closed with (null while open or after a graceful local close).</summary>
    internal Exception? CloseError
    {
        get
        {
            lock (_gate)
            {
                return _closeError;
            }
        }
    }

    /// <summary>Cancelled when the connection closes; passed to handlers.</summary>
    protected CancellationToken ClosingToken => _closing.Token;

    /// <summary>The local network identity.</summary>
    protected abstract string LocalName { get; }

    /// <summary>Time source for call timeouts and link delays.</summary>
    protected abstract TimeProvider Time { get; }

    /// <summary>The link policy consulted for every inbound application message.</summary>
    protected abstract ILinkPolicy Policy { get; }

    /// <summary>The handler for inbound requests and one-way messages, or null when the transport does not accept them.</summary>
    protected abstract IRpcHandler? Handler { get; }

    /// <summary>
    /// Hands one message to the transport. Implementations copy <paramref name="payload"/> before returning.
    /// </summary>
    /// <param name="kind">Request, reply or one-way.</param>
    /// <param name="correlation">Correlation id.</param>
    /// <param name="payload">The payload (owned by the caller).</param>
    /// <param name="awaitTransmission">
    /// True to return a task that completes when the message has been handed to the network (backpressure for
    /// <see cref="SendAsync"/>); false to return a completed task.
    /// </param>
    /// <exception cref="RpcConnectionException">The connection is closed.</exception>
    protected abstract ValueTask TransmitAsync(RpcMessageKind kind, ulong correlation, ReadOnlyMemory<byte> payload, bool awaitTransmission);

    /// <summary>Transport-specific close work (notify the remote end, release resources). Called once.</summary>
    protected abstract void OnClosed(RpcCloseOrigin origin, Exception? error);

    /// <summary>Validates an outbound payload length.</summary>
    /// <exception cref="ArgumentException">The payload is larger than the transport supports.</exception>
    protected virtual void ValidatePayload(int length)
    {
    }

    /// <summary>Reports an unexpected internal fault (never thrown to callers).</summary>
    protected virtual void ReportFault(string message, Exception? exception)
    {
    }

    /// <inheritdoc />
    public Task<ReadOnlyMemory<byte>> CallAsync(ReadOnlyMemory<byte> request, TimeSpan timeout, CancellationToken ct = default)
    {
        if (timeout < TimeSpan.Zero && timeout != Timeout.InfiniteTimeSpan)
            throw new ArgumentOutOfRangeException(nameof(timeout), timeout, "The timeout must be non-negative or Timeout.InfiniteTimeSpan.");
        ValidatePayload(request.Length);
        if (ct.IsCancellationRequested)
            return Task.FromCanceled<ReadOnlyMemory<byte>>(ct);
        if (!IsOpen)
            return Task.FromException<ReadOnlyMemory<byte>>(CreateClosedException());
        if (timeout == TimeSpan.Zero)
            return Task.FromException<ReadOnlyMemory<byte>>(CreateTimeoutException(timeout));

        ulong correlation = NextCorrelation();
        var call = new PendingCall(this, correlation, timeout);
        _pending[correlation] = call;
        if (!IsOpen)
        {
            // Closed between the check and the registration: the close may not have seen this call.
            if (_pending.TryRemove(new KeyValuePair<ulong, PendingCall>(correlation, call)))
                call.TrySetException(CreateClosedException());
            return call.Task;
        }

        call.Arm(Time, ct);
        try
        {
            ValueTask transmitted = TransmitAsync(RpcMessageKind.Request, correlation, request, awaitTransmission: false);
            if (!transmitted.IsCompleted)
                _ = ObserveAsync(transmitted);
        }
        catch (Exception ex) when (ex is RpcConnectionException or ObjectDisposedException)
        {
            if (TryTakePending(correlation, call))
            {
                call.Cleanup();
                call.TrySetException(ex as RpcConnectionException ?? CreateClosedException());
            }
        }

        return call.Task;
    }

    /// <inheritdoc />
    public ValueTask SendAsync(ReadOnlyMemory<byte> message, CancellationToken ct = default)
    {
        ValidatePayload(message.Length);
        if (ct.IsCancellationRequested)
            return ValueTask.FromCanceled(ct);
        if (!IsOpen)
            return ValueTask.FromException(CreateClosedException());

        ValueTask sent;
        try
        {
            sent = TransmitAsync(RpcMessageKind.OneWay, NextCorrelation(), message, awaitTransmission: true);
        }
        catch (Exception ex) when (ex is RpcConnectionException or ObjectDisposedException)
        {
            return ValueTask.FromException(ex as RpcConnectionException ?? CreateClosedException());
        }

        if (sent.IsCompleted || !ct.CanBeCanceled)
            return sent;
        return new ValueTask(sent.AsTask().WaitAsync(ct));
    }

    /// <inheritdoc />
    /// <remarks>Graceful local close: pending calls fail with <see cref="RpcConnectionException"/>, the remote end
    /// observes the close, and <see cref="Closed"/> is raised with null.</remarks>
    public ValueTask DisposeAsync()
    {
        TryClose(null, RpcCloseOrigin.Local);
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Closes the connection once: fails pending calls, cancels handler tokens and delayed deliveries, runs
    /// <see cref="OnClosed"/> and raises <see cref="Closed"/> (asynchronously).
    /// </summary>
    /// <param name="error">The close exception reported to <see cref="Closed"/>; null for a graceful local close.</param>
    /// <param name="origin">Why the connection closes.</param>
    /// <returns>True when this call closed the connection.</returns>
    internal bool TryClose(Exception? error, RpcCloseOrigin origin)
    {
        Action<IRpcConnection, Exception?>? handlers;
        lock (_gate)
        {
            if (_state != StateOpen)
                return false;
            _state = StateClosed;
            _closeError = error;
            handlers = _closed;
            _closed = null;
        }

        // The token flips synchronously, but its callbacks run on the thread pool, and with them every handler continuation
        // they resume inline (a handler awaiting a TaskCompletionSource it cancels from ct.Register, say): handler code must
        // never run on the closing thread, which may be the QUIC pump or a caller disposing the connection.
        Task cancelled = _closing.CancelAsync();
        if (!cancelled.IsCompleted)
        {
            _ = cancelled.ContinueWith(
                static (task, state) => ((RpcConnectionBase)state!).ReportFault("A cancellation callback registered by a handler threw.", task.Exception),
                this,
                CancellationToken.None,
                TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }
        else if (cancelled.IsFaulted)
        {
            ReportFault("A cancellation callback registered by a handler threw.", cancelled.Exception);
        }

        RpcConnectionException failure = CreateClosedException();
        foreach (ulong correlation in _pending.Keys)
        {
            if (_pending.TryRemove(correlation, out PendingCall? call))
            {
                call.Cleanup();
                call.TrySetException(failure);
            }
        }

        foreach (DelayedDelivery delayed in _delayed.Keys)
        {
            if (_delayed.TryRemove(delayed, out _))
                delayed.Cancel();
        }

        try
        {
            OnClosed(origin, error);
        }
        catch (Exception ex)
        {
            ReportFault("Transport close processing failed.", ex);
        }

        if (handlers is not null)
            RaiseClosed(handlers, error);
        return true;
    }

    /// <summary>Queues an inbound application message for policy evaluation and in-order delivery.</summary>
    /// <param name="kind">Request, reply or one-way.</param>
    /// <param name="correlation">Correlation id.</param>
    /// <param name="payload">The payload (owned by the connection from now on).</param>
    internal void PostInbound(RpcMessageKind kind, ulong correlation, ReadOnlyMemory<byte> payload)
        => _inbound.Post(new InboundItem(InboundItemType.Message, kind, correlation, payload, Decided: false, Error: null));

    /// <summary>Queues a remote close behind every message received before it.</summary>
    /// <param name="error">The exception reported to <see cref="Closed"/>.</param>
    internal void PostRemoteClose(Exception error)
        => _inbound.Post(new InboundItem(InboundItemType.RemoteClose, default, 0, default, Decided: true, error));

    /// <summary>Creates the exception that pending and new operations fail with once the connection is closed.</summary>
    protected RpcConnectionException CreateClosedException()
    {
        Exception? error = CloseError;
        string message = error is null
            ? $"The connection to {Remote} is closed."
            : $"The connection to {Remote} is closed: {error.Message}";
        return new RpcConnectionException(message, error);
    }

    private TimeoutException CreateTimeoutException(TimeSpan timeout)
        => new($"The call to {Remote} received no reply within {timeout} (the request may have been executed).");

    private ulong NextCorrelation() => (ulong)Interlocked.Increment(ref _nextCorrelation);

    private bool TryTakePending(ulong correlation, PendingCall call)
        => _pending.TryRemove(new KeyValuePair<ulong, PendingCall>(correlation, call));

    private async Task ObserveAsync(ValueTask transmitted)
    {
        try
        {
            await transmitted.ConfigureAwait(false);
        }
        catch (Exception)
        {
            // A request that could not be transmitted fails through the connection close.
        }
    }

    private void RaiseClosed(Action<IRpcConnection, Exception?> handlers, Exception? error)
    {
        ThreadPool.UnsafeQueueUserWorkItem(static state =>
        {
            (RpcConnectionBase connection, Action<IRpcConnection, Exception?> list, Exception? closeError) = state;
            foreach (Delegate handler in list.GetInvocationList())
            {
                try
                {
                    ((Action<IRpcConnection, Exception?>)handler)(connection, closeError);
                }
                catch (Exception ex)
                {
                    connection.ReportFault("An IRpcConnection.Closed handler threw.", ex);
                }
            }
        }, (this, handlers, error), preferLocal: false);
    }

    private void ProcessInbound(InboundItem item)
    {
        if (item.Type == InboundItemType.RemoteClose)
        {
            TryClose(item.Error, RpcCloseOrigin.Remote);
            return;
        }

        if (!IsOpen)
            return;

        if (item.Decided)
        {
            Deliver(item);
            return;
        }

        LinkDecision decision = Decide(item);
        switch (decision.Kind)
        {
            case LinkDecisionKind.Drop:
            case LinkDecisionKind.BlackHole:
                return;
            case LinkDecisionKind.Delay when decision.Delay > TimeSpan.Zero:
                ScheduleDelayed(item, decision.Delay);
                return;
            case LinkDecisionKind.Duplicate:
                Deliver(item);
                if (IsOpen)
                    Deliver(item);
                return;
            default:
                Deliver(item);
                return;
        }
    }

    private LinkDecision Decide(InboundItem item)
    {
        try
        {
            return Policy.Decide(RemoteName ?? Remote.ToString(), LocalName, RpcMessageKinds.ToDirection(item.Kind), item.Payload.Span);
        }
        catch (Exception ex)
        {
            ReportFault("The link policy threw; the message is delivered.", ex);
            return LinkDecision.Deliver;
        }
    }

    private void ScheduleDelayed(InboundItem item, TimeSpan delay)
    {
        var delayed = new DelayedDelivery(this, item with { Decided = true });
        _delayed[delayed] = 0;
        if (!IsOpen)
        {
            if (_delayed.TryRemove(delayed, out _))
                delayed.Cancel();
            return;
        }

        delayed.Start(Time, delay < MaxTimerDue ? delay : MaxTimerDue);
    }

    private void OnDelayElapsed(DelayedDelivery delayed)
    {
        if (!_delayed.TryRemove(delayed, out _))
            return;
        delayed.Cancel();
        if (IsOpen)
            _inbound.Post(delayed.Item);
    }

    private void Deliver(InboundItem item)
    {
        switch (item.Kind)
        {
            case RpcMessageKind.Reply:
                if (_pending.TryRemove(item.Correlation, out PendingCall? call))
                {
                    call.Cleanup();
                    call.TrySetResult(item.Payload);
                }

                // A reply without a pending call is late (timed out, cancelled) or duplicated: discard it.
                return;
            case RpcMessageKind.Request:
                InvokeHandler(item, expectsReply: true);
                return;
            case RpcMessageKind.OneWay:
                InvokeHandler(item, expectsReply: false);
                return;
            default:
                ReportFault($"Unexpected inbound message kind {item.Kind}.", null);
                return;
        }
    }

    private void InvokeHandler(InboundItem item, bool expectsReply)
    {
        IRpcHandler? handler = Handler;
        if (handler is null)
        {
            if (expectsReply)
            {
                SendReply(item.Correlation, ErrorCodec.Encode(DfsErrorCode.Unavailable,
                    $"The endpoint '{LocalName}' does not accept requests.", null, retryable: true));
            }

            return;
        }

        ValueTask<ReadOnlyMemory<byte>?> pending;
        try
        {
            pending = handler.HandleAsync(this, item.Payload, expectsReply, _closing.Token);
        }
        catch (Exception ex)
        {
            HandlerFailed(item.Correlation, expectsReply, ex);
            return;
        }

        if (pending.IsCompletedSuccessfully)
        {
            ReadOnlyMemory<byte>? reply = pending.Result;
            if (expectsReply)
                SendReply(item.Correlation, reply);
            return;
        }

        _ = CompleteHandlerAsync(pending, item.Correlation, expectsReply);
    }

    private async Task CompleteHandlerAsync(ValueTask<ReadOnlyMemory<byte>?> pending, ulong correlation, bool expectsReply)
    {
        ReadOnlyMemory<byte>? reply;
        try
        {
            reply = await pending.ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            HandlerFailed(correlation, expectsReply, ex);
            return;
        }

        if (expectsReply)
            SendReply(correlation, reply);
    }

    private void HandlerFailed(ulong correlation, bool expectsReply, Exception ex)
    {
        if (ex is OperationCanceledException && _closing.IsCancellationRequested)
            return; // the connection closed under the handler; no reply can be sent
        if (!expectsReply)
        {
            ReportFault("A one-way message handler failed.", ex);
            return;
        }

        SendReply(correlation, ErrorCodec.Encode(DfsErrorCode.Internal,
            $"The request handler failed: {ex.GetType().FullName}: {ex.Message}", null, retryable: false));
    }

    private void SendReply(ulong correlation, ReadOnlyMemory<byte>? reply)
    {
        if (reply is null || !IsOpen)
            return; // null: the handler chose not to reply (the caller times out)
        ReadOnlyMemory<byte> payload = reply.Value;
        try
        {
            ValidatePayload(payload.Length);
        }
        catch (ArgumentException ex)
        {
            ReportFault("A reply exceeded the transport's message size limit; an error reply is sent instead.", ex);
            payload = ErrorCodec.Encode(DfsErrorCode.LimitExceeded, ex.Message, null, retryable: false);
        }

        try
        {
            ValueTask transmitted = TransmitAsync(RpcMessageKind.Reply, correlation, payload, awaitTransmission: false);
            if (!transmitted.IsCompleted)
                _ = ObserveAsync(transmitted);
        }
        catch (Exception ex) when (ex is RpcConnectionException or ObjectDisposedException)
        {
            // Closed while the handler ran: the caller's call fails through its own connection close.
        }
    }

    private enum InboundItemType : byte
    {
        Message,
        RemoteClose,
    }

    /// <summary>One item of the inbound queue: a message (possibly already decided by the link policy) or a remote close.</summary>
    private readonly record struct InboundItem(
        InboundItemType Type,
        RpcMessageKind Kind,
        ulong Correlation,
        ReadOnlyMemory<byte> Payload,
        bool Decided,
        Exception? Error);

    /// <summary>A call waiting for its reply.</summary>
    private sealed class PendingCall : TaskCompletionSource<ReadOnlyMemory<byte>>
    {
        private readonly RpcConnectionBase _owner;
        private readonly ulong _correlation;
        private readonly TimeSpan _timeout;
        private readonly object _sync = new();
        private ITimer? _timer;
        private CancellationTokenRegistration _registration;
        private bool _cleaned;

        public PendingCall(RpcConnectionBase owner, ulong correlation, TimeSpan timeout)
            : base(TaskCreationOptions.RunContinuationsAsynchronously)
        {
            _owner = owner;
            _correlation = correlation;
            _timeout = timeout;
        }

        /// <summary>Starts the timeout timer and the cancellation registration.</summary>
        public void Arm(TimeProvider time, CancellationToken ct)
        {
            ITimer? timer = null;
            if (_timeout != Timeout.InfiniteTimeSpan)
            {
                TimeSpan due = _timeout < MaxTimerDue ? _timeout : MaxTimerDue;
                timer = time.CreateTimer(static state => ((PendingCall)state!).OnTimeout(), this, due, Timeout.InfiniteTimeSpan);
            }

            CancellationTokenRegistration registration = ct.CanBeCanceled
                ? ct.UnsafeRegister(static (state, token) => ((PendingCall)state!).OnCanceled(token), this)
                : default;
            bool cleaned;
            lock (_sync)
            {
                cleaned = _cleaned;
                if (!cleaned)
                {
                    _timer = timer;
                    _registration = registration;
                }
            }

            if (cleaned)
            {
                timer?.Dispose();
                registration.Unregister();
            }
        }

        /// <summary>Releases the timer and the cancellation registration (idempotent).</summary>
        public void Cleanup()
        {
            ITimer? timer;
            CancellationTokenRegistration registration;
            lock (_sync)
            {
                if (_cleaned)
                    return;
                _cleaned = true;
                timer = _timer;
                registration = _registration;
                _timer = null;
                _registration = default;
            }

            timer?.Dispose();
            registration.Unregister();
        }

        private void OnTimeout()
        {
            if (!_owner.TryTakePending(_correlation, this))
                return;
            Cleanup();
            TrySetException(_owner.CreateTimeoutException(_timeout));
        }

        private void OnCanceled(CancellationToken token)
        {
            if (!_owner.TryTakePending(_correlation, this))
                return;
            Cleanup();
            TrySetCanceled(token);
        }
    }

    /// <summary>An inbound message held back by a <see cref="LinkDecisionKind.Delay"/> decision.</summary>
    private sealed class DelayedDelivery
    {
        private readonly RpcConnectionBase _owner;
        private readonly object _sync = new();
        private ITimer? _timer;
        private bool _cancelled;

        public DelayedDelivery(RpcConnectionBase owner, InboundItem item)
        {
            _owner = owner;
            Item = item;
        }

        public InboundItem Item { get; }

        public void Start(TimeProvider time, TimeSpan delay)
        {
            ITimer timer = time.CreateTimer(static state =>
            {
                var self = (DelayedDelivery)state!;
                self._owner.OnDelayElapsed(self);
            }, this, delay, Timeout.InfiniteTimeSpan);
            bool cancelled;
            lock (_sync)
            {
                cancelled = _cancelled;
                if (!cancelled)
                    _timer = timer;
            }

            if (cancelled)
                timer.Dispose();
        }

        public void Cancel()
        {
            ITimer? timer;
            lock (_sync)
            {
                _cancelled = true;
                timer = _timer;
                _timer = null;
            }

            timer?.Dispose();
        }
    }
}
