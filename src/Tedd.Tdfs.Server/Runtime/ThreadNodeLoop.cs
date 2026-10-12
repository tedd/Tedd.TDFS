using System.Runtime.ExceptionServices;

namespace Tedd.Tdfs.Server;

/// <summary>
/// Production <see cref="INodeLoop"/>: a dedicated background thread executing a lock-protected FIFO queue, woken by a
/// <see cref="ManualResetEventSlim"/> (ARCHITECTURE §3.3). The loop's <see cref="SynchronizationContext"/> is installed on
/// the thread, so awaits in node code resume on the loop.
/// </summary>
/// <remarks>
/// This class is the sanctioned owner of a raw thread in server code: node logic itself never creates threads, it runs
/// on this loop. Each work item runs with the loop's base <see cref="ExecutionContext"/> restored afterwards, so ambient
/// state set by one item cannot leak into the next.
/// </remarks>
internal sealed class ThreadNodeLoop : INodeLoop
{
    private readonly object _lock = new();
    private readonly Queue<WorkItem> _queue = new();
    private readonly ManualResetEventSlim _signal = new(false);
    private readonly List<DrainWaiter> _drainWaiters = [];
    private readonly TaskCompletionSource _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly NodeLoopInvoker _invoker;
    private readonly INodeLog _log;
    private readonly Thread _thread;
    private readonly int _threadId;
    private Action<INodeLoop, Exception>? _unhandled;
    private bool _paused;
    private bool _stopRequested;
    private bool _draining;
    private volatile bool _exited;
    private long _executed;
    private long _discarded;
    private long _unhandledCount;

    /// <summary>Creates and starts the loop thread.</summary>
    public ThreadNodeLoop(string name, INodeLog log)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        ArgumentNullException.ThrowIfNull(log);
        Name = name;
        _log = log;
        Context = new LoopSynchronizationContext(this);
        _invoker = new NodeLoopInvoker(this, TaskCreationOptions.RunContinuationsAsynchronously);
        _thread = new Thread(ThreadMain)
        {
            IsBackground = true,
            Name = "tdfs-loop:" + name,
        };
        _threadId = _thread.ManagedThreadId;
        _thread.Start();
    }

    /// <inheritdoc />
    public string Name { get; }

    /// <inheritdoc />
    public SynchronizationContext Context { get; }

    /// <inheritdoc />
    public bool IsOnLoop => Environment.CurrentManagedThreadId == _threadId && !_exited;

    /// <inheritdoc />
    public bool IsPaused
    {
        get
        {
            lock (_lock)
                return _paused && !_stopRequested;
        }
    }

    /// <inheritdoc />
    public bool IsStopped
    {
        get
        {
            lock (_lock)
                return _stopRequested;
        }
    }

    /// <inheritdoc />
    public Task Completion => _completion.Task;

    /// <summary>Managed thread id of the loop thread.</summary>
    public int ThreadId => _threadId;

    /// <summary>Number of work items executed so far.</summary>
    public long ExecutedCount => Interlocked.Read(ref _executed);

    /// <summary>Number of work items discarded because the loop stopped.</summary>
    public long DiscardedCount => Interlocked.Read(ref _discarded);

    /// <summary>Number of work items that threw.</summary>
    public long UnhandledExceptionCount => Interlocked.Read(ref _unhandledCount);

    /// <summary>Number of queued, not yet executed work items.</summary>
    public int QueuedCount
    {
        get
        {
            lock (_lock)
                return _queue.Count;
        }
    }

    /// <inheritdoc />
    public event Action<INodeLoop, Exception>? UnhandledException
    {
        add
        {
            lock (_lock)
                _unhandled += value;
        }
        remove
        {
            lock (_lock)
                _unhandled -= value;
        }
    }

    /// <inheritdoc />
    public void Post(Action work)
    {
        ArgumentNullException.ThrowIfNull(work);
        Enqueue(new WorkItem(work, null, null, null));
    }

    /// <inheritdoc />
    public Task InvokeAsync(Action work) => _invoker.InvokeAsync(work);

    /// <inheritdoc />
    public Task<T> InvokeAsync<T>(Func<T> work) => _invoker.InvokeAsync(work);

    /// <inheritdoc />
    public Task InvokeAsync(Func<Task> work) => _invoker.InvokeAsync(work);

    /// <inheritdoc />
    public Task<T> InvokeAsync<T>(Func<Task<T>> work) => _invoker.InvokeAsync(work);

    /// <inheritdoc />
    public void Pause()
    {
        lock (_lock)
        {
            if (!_stopRequested)
                _paused = true;
        }
    }

    /// <inheritdoc />
    public void Resume()
    {
        lock (_lock)
        {
            if (!_paused)
                return;
            _paused = false;
            if (!_stopRequested)
                _signal.Set();
        }
    }

    /// <inheritdoc />
    public void Stop()
    {
        List<Action>? discardHooks = null;
        lock (_lock)
        {
            if (_stopRequested)
                return;
            _stopRequested = true;
            while (_queue.Count > 0)
            {
                WorkItem item = _queue.Dequeue();
                _discarded++;
                if (item.OnDiscard is not null)
                    (discardHooks ??= []).Add(item.OnDiscard);
            }

            _signal.Set();
        }

        RunDiscardHooks(discardHooks);
        _invoker.Close();
    }

    /// <inheritdoc />
    public Task DrainAsync(CancellationToken ct = default)
    {
        DrainWaiter waiter;
        lock (_lock)
        {
            if (_exited)
                return Task.CompletedTask;
            if (_stopRequested && !_draining)
                return _completion.Task; // abrupt stop already in progress: nothing left to drain
            _draining = true;
            _paused = false;
            waiter = new DrainWaiter(ct);
            _drainWaiters.Add(waiter);
            _signal.Set();
        }

        if (ct.CanBeCanceled)
        {
            waiter.Registration = ct.Register(static state =>
            {
                var (loop, w) = ((ThreadNodeLoop, DrainWaiter))state!;
                w.Canceled = true;
                loop.Stop();
            }, (this, waiter));
        }

        // The thread may have exited before the registration existed; complete the waiter in that case.
        if (_exited)
            waiter.Complete();
        return waiter.Task;
    }

    /// <inheritdoc />
    public override string ToString() => $"ThreadNodeLoop({Name})";

    /// <summary>Queues an item; returns false when the loop has stopped (the item is discarded).</summary>
    private bool Enqueue(WorkItem item)
    {
        lock (_lock)
        {
            if (_stopRequested)
            {
                _discarded++;
                return false;
            }

            _queue.Enqueue(item);
            if (!_paused)
                _signal.Set();
            return true;
        }
    }

    private void ThreadMain()
    {
        SynchronizationContext.SetSynchronizationContext(Context);
        ExecutionContext? baseContext = ExecutionContext.Capture();
        try
        {
            while (true)
            {
                WorkItem item = default;
                bool run = false;
                lock (_lock)
                {
                    if (_stopRequested)
                        break;
                    if (!_paused && _queue.Count > 0)
                    {
                        item = _queue.Dequeue();
                        run = true;
                    }
                    else if (_draining && !_paused)
                    {
                        // Graceful stop: the queue was observed empty.
                        _stopRequested = true;
                        break;
                    }
                    else
                    {
                        _signal.Reset();
                    }
                }

                if (!run)
                {
                    _signal.Wait();
                    continue;
                }

                Execute(item);
                if (baseContext is not null)
                    ExecutionContext.Restore(baseContext);
                if (!ReferenceEquals(SynchronizationContext.Current, Context))
                    SynchronizationContext.SetSynchronizationContext(Context);
            }
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(null);
            OnExited();
        }
    }

    private void Execute(in WorkItem item)
    {
        try
        {
            item.Invoke();
        }
        catch (Exception ex)
        {
            ReportUnhandled(ex);
        }
        finally
        {
            Interlocked.Increment(ref _executed);
        }
    }

    private void ReportUnhandled(Exception exception)
    {
        Interlocked.Increment(ref _unhandledCount);
        TryLog($"Unhandled exception in a node loop work item: {exception.GetType().FullName}: {exception.Message}");

        Action<INodeLoop, Exception>? handlers;
        lock (_lock)
            handlers = _unhandled;
        if (handlers is null)
            return;
        foreach (Delegate handler in handlers.GetInvocationList())
        {
            try
            {
                ((Action<INodeLoop, Exception>)handler)(this, exception);
            }
            catch (Exception handlerError)
            {
                TryLog($"An UnhandledException handler threw: {handlerError.GetType().FullName}: {handlerError.Message}");
            }
        }
    }

    private void TryLog(string message)
    {
        try
        {
            _log.Write(NodeLogLevel.Error, Name, "loop", message);
        }
        catch
        {
            // A failing log sink must not take the loop down.
        }
    }

    private void OnExited()
    {
        DrainWaiter[] waiters;
        List<Action>? discardHooks = null;
        lock (_lock)
        {
            _stopRequested = true;
            _exited = true;
            while (_queue.Count > 0)
            {
                WorkItem item = _queue.Dequeue();
                _discarded++;
                if (item.OnDiscard is not null)
                    (discardHooks ??= []).Add(item.OnDiscard);
            }

            waiters = [.. _drainWaiters];
            _drainWaiters.Clear();
        }

        RunDiscardHooks(discardHooks);
        _invoker.Close();
        _completion.TrySetResult();
        foreach (DrainWaiter waiter in waiters)
            waiter.Complete();
    }

    private void RunDiscardHooks(List<Action>? hooks)
    {
        if (hooks is null)
            return;
        foreach (Action hook in hooks)
        {
            try
            {
                hook();
            }
            catch (Exception ex)
            {
                TryLog($"A discard callback threw: {ex.GetType().FullName}: {ex.Message}");
            }
        }
    }

    /// <summary>Executes <paramref name="callback"/> synchronously on the loop (inline when already on it).</summary>
    private void Send(SendOrPostCallback callback, object? state)
    {
        ArgumentNullException.ThrowIfNull(callback);
        if (IsOnLoop)
        {
            callback(state);
            return;
        }

        // Not disposed on purpose: the loop thread may still be inside Set() when the waiter wakes up, and a
        // ManualResetEventSlim whose WaitHandle is never touched owns no unmanaged resources.
        var done = new ManualResetEventSlim(false);
        ExceptionDispatchInfo? error = null;
        bool discarded = false;
        var item = new WorkItem(
            () =>
            {
                try
                {
                    callback(state);
                }
                catch (Exception ex)
                {
                    error = ExceptionDispatchInfo.Capture(ex);
                }
                finally
                {
                    done.Set();
                }
            },
            null,
            null,
            () =>
            {
                discarded = true;
                done.Set();
            });
        if (!Enqueue(item))
            throw new NodeLoopStoppedException(Name);
        done.Wait();
        if (discarded)
            throw new NodeLoopStoppedException(Name);
        error?.Throw();
    }

    private readonly struct WorkItem(Action? action, SendOrPostCallback? callback, object? state, Action? onDiscard)
    {
        public Action? OnDiscard { get; } = onDiscard;

        public void Invoke()
        {
            if (action is not null)
                action();
            else
                callback!(state);
        }
    }

    private sealed class DrainWaiter(CancellationToken ct)
    {
        private readonly TaskCompletionSource _source = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public CancellationTokenRegistration Registration { get; set; }

        public volatile bool Canceled;

        public Task Task => _source.Task;

        public void Complete()
        {
            Registration.Dispose();
            if (Canceled)
                _source.TrySetCanceled(ct);
            else
                _source.TrySetResult();
        }
    }

    private sealed class LoopSynchronizationContext(ThreadNodeLoop loop) : SynchronizationContext
    {
        public override void Post(SendOrPostCallback d, object? state)
        {
            ArgumentNullException.ThrowIfNull(d);
            loop.Enqueue(new WorkItem(null, d, state, null));
        }

        public override void Send(SendOrPostCallback d, object? state) => loop.Send(d, state);

        public override SynchronizationContext CreateCopy() => this;

        public override string ToString() => $"NodeLoopContext({loop.Name})";
    }
}
