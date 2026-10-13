namespace Tedd.Tdfs.Client.Internal.Runtime;

/// <summary>
/// Simulation-compatible waiting primitives on the injected <see cref="TimeProvider"/> (ARCHITECTURE §9.6): delays and
/// cancellable waits are completion sources completed by the provider's timer or by a token registration, always
/// without a synchronization context (<see cref="ContextFree"/>). Never <c>Task.Delay(…, ct)</c> (its cancellation runs
/// continuations on the thread pool) and never <c>RunContinuationsAsynchronously</c>.
/// </summary>
internal static class Waits
{
    /// <summary>Completes after <paramref name="delay"/> on <paramref name="time"/>; throws <see cref="OperationCanceledException"/> when <paramref name="ct"/> fires first.</summary>
    public static Task DelayAsync(TimeProvider time, TimeSpan delay, CancellationToken ct)
    {
        if (ct.IsCancellationRequested)
            return Task.FromCanceled(ct);
        if (delay <= TimeSpan.Zero)
            return Task.CompletedTask;
        var promise = new DelayPromise(ct);
        promise.Start(time, delay);
        return promise.Task;
    }

    /// <summary>
    /// Waits for <paramref name="task"/> and returns its result; when <paramref name="ct"/> fires first the wait ends with
    /// <see cref="OperationCanceledException"/> (the task itself keeps running).
    /// </summary>
    public static Task<T> WaitAsync<T>(Task<T> task, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(task);
        if (task.IsCompleted || !ct.CanBeCanceled)
            return task;
        if (ct.IsCancellationRequested)
            return Task.FromCanceled<T>(ct);
        var promise = new WaitPromise<T>(ct);
        promise.Attach(task);
        return promise.Task;
    }

    /// <summary>Waits for <paramref name="task"/>; when <paramref name="ct"/> fires first the wait ends with <see cref="OperationCanceledException"/>.</summary>
    public static Task WaitAsync(Task task, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(task);
        if (task.IsCompleted || !ct.CanBeCanceled)
            return task;
        if (ct.IsCancellationRequested)
            return Task.FromCanceled(ct);
        var promise = new WaitPromise<bool>(ct);
        promise.Attach(task.ContinueWith(static t =>
        {
            t.GetAwaiter().GetResult();
            return true;
        }, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default));
        return promise.Task;
    }

    /// <summary>
    /// Waits up to <paramref name="timeout"/> (on <paramref name="time"/>) for <paramref name="task"/> to complete. Returns
    /// true when it completed (successfully or not; its outcome is not observed here), false on timeout.
    /// </summary>
    public static async Task<bool> CompletesWithinAsync(Task task, TimeSpan timeout, TimeProvider time, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(task);
        if (task.IsCompleted)
            return true;
        if (timeout <= TimeSpan.Zero)
            return false;
        using var timer = new CancellationTokenSource();
        using CancellationTokenRegistration link = ct.CanBeCanceled ? ct.UnsafeRegister(static s => ContextFree.Cancel((CancellationTokenSource)s!), timer) : default;
        Task delay = DelayAsync(time, timeout, timer.Token);
        Task winner = await Task.WhenAny(task, delay).ConfigureAwait(false);
        if (!ReferenceEquals(winner, task))
        {
            ct.ThrowIfCancellationRequested();
            return task.IsCompleted;
        }

        ContextFree.Cancel(timer);
        ObserveQuietly(delay);
        return true;
    }

    /// <summary>Observes a task's exception so it never surfaces as an unobserved task exception.</summary>
    public static void ObserveQuietly(Task task)
    {
        if (task.IsCompleted)
        {
            _ = task.Exception;
            return;
        }

        task.ContinueWith(static t => _ = t.Exception, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously | TaskContinuationOptions.OnlyOnFaulted,
            TaskScheduler.Default);
    }

    private sealed class DelayPromise
    {
        private readonly TaskCompletionSource _source = new(TaskCreationOptions.None);
        private readonly CancellationToken _ct;
        private ITimer? _timer;
        private CancellationTokenRegistration _registration;
        private int _done;

        public DelayPromise(CancellationToken ct) => _ct = ct;

        public Task Task => _source.Task;

        public void Start(TimeProvider time, TimeSpan delay)
        {
            _timer = time.CreateTimer(static s => ((DelayPromise)s!).Finish(cancelled: false), this, delay, Timeout.InfiniteTimeSpan);
            if (_ct.CanBeCanceled)
                _registration = _ct.UnsafeRegister(static s => ((DelayPromise)s!).Finish(cancelled: true), this);
            if (Volatile.Read(ref _done) != 0)
                Cleanup();
        }

        private void Finish(bool cancelled)
        {
            if (Interlocked.Exchange(ref _done, 1) != 0)
                return;
            Cleanup();
            ContextFree.Run(static p =>
            {
                if (p.Cancelled)
                    p.Promise._source.TrySetCanceled(p.Promise._ct);
                else
                    p.Promise._source.TrySetResult();
            }, (Promise: this, Cancelled: cancelled));
        }

        private void Cleanup()
        {
            _timer?.Dispose();
            _registration.Unregister();
        }
    }

    private sealed class WaitPromise<T>
    {
        private readonly TaskCompletionSource<T> _source = new(TaskCreationOptions.None);
        private readonly CancellationToken _ct;
        private CancellationTokenRegistration _registration;

        public WaitPromise(CancellationToken ct) => _ct = ct;

        public Task<T> Task => _source.Task;

        public void Attach(Task<T> task)
        {
            _registration = _ct.UnsafeRegister(static s =>
            {
                var p = (WaitPromise<T>)s!;
                ContextFree.Run(static q => q._source.TrySetCanceled(q._ct), p);
            }, this);
            task.ContinueWith(static (t, s) =>
            {
                var p = (WaitPromise<T>)s!;
                p._registration.Unregister();
                ContextFree.Run(static q =>
                {
                    (WaitPromise<T> promise, Task<T> completed) = q;
                    if (completed.IsCanceled)
                    {
                        CancellationToken token;
                        try
                        {
                            completed.GetAwaiter().GetResult();
                            token = CancellationToken.None;
                        }
                        catch (OperationCanceledException oce)
                        {
                            token = oce.CancellationToken;
                        }

                        promise._source.TrySetCanceled(token);
                    }
                    else if (completed.IsFaulted)
                    {
                        promise._source.TrySetException(completed.Exception!.InnerExceptions);
                    }
                    else
                    {
                        promise._source.TrySetResult(completed.Result);
                    }
                }, (p, t));
            }, this, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
        }
    }
}
