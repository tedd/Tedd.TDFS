using Tedd.Tdfs.Server;

namespace Tedd.Tdfs.Testing.Simulation;

/// <summary>
/// <see cref="INodeLoop"/> of a simulated node: the main lane of a <see cref="SimProcess"/>. Posts and await continuations
/// go into the <see cref="SimScheduler"/> (FIFO within the loop); <see cref="Pause"/> holds the whole process,
/// <see cref="Stop"/> kills it (crash: queued work discarded, later posts dropped, timers dead, transports reset).
/// </summary>
/// <remarks>
/// <see cref="InvokeAsync(Action)"/> completes its task inline on the simulation thread (no
/// <c>RunContinuationsAsynchronously</c>) with the loop's context removed for the completion, so callers awaiting it with
/// a simulated context are posted back to it and <c>ConfigureAwait(false)</c> callers continue inline — both
/// deterministically.
/// Exceptions escaping loop work are raised through <see cref="UnhandledException"/> and recorded by the scheduler
/// (see <see cref="SimSchedulerOptions.ThrowOnUnhandledException"/>).
/// </remarks>
public sealed class SimNodeLoop : INodeLoop
{
    private readonly NodeLoopInvoker _invoker;
    private readonly List<DrainWaiter> _drainWaiters = [];

    internal SimNodeLoop(SimProcess process)
    {
        Process = process;
        _invoker = new NodeLoopInvoker(this, TaskCreationOptions.None, completeWithoutSynchronizationContext: true);
    }

    /// <summary>The process whose main lane this loop is.</summary>
    public SimProcess Process { get; }

    /// <inheritdoc />
    public string Name => Process.Name;

    /// <inheritdoc />
    public SynchronizationContext Context => Process.Context;

    /// <inheritdoc />
    public bool IsOnLoop => Process.IsOnMainLane;

    /// <inheritdoc />
    public bool IsPaused => Process.IsPaused;

    /// <inheritdoc />
    public bool IsStopped => Process.IsKilled;

    /// <inheritdoc />
    public Task Completion => Process.Exited;

    /// <summary>Number of <see cref="InvokeAsync(Action)"/> calls not yet completed.</summary>
    public int PendingInvocationCount => _invoker.PendingCount;

    internal bool DrainRequested { get; private set; }

    /// <inheritdoc />
    public event Action<INodeLoop, Exception>? UnhandledException;

    /// <inheritdoc />
    public void Post(Action work)
    {
        ArgumentNullException.ThrowIfNull(work);
        Process.Post(work);
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
    public void Pause() => Process.Pause();

    /// <inheritdoc />
    public void Resume() => Process.Resume();

    /// <inheritdoc />
    public void Stop() => Process.Kill();

    /// <inheritdoc />
    public Task DrainAsync(CancellationToken ct = default)
    {
        if (Process.IsKilled)
            return Task.CompletedTask;
        var waiter = new DrainWaiter(ct);
        _drainWaiters.Add(waiter);
        DrainRequested = true;
        if (ct.CanBeCanceled)
        {
            waiter.Registration = ct.Register(static state =>
            {
                var (loop, w) = ((SimNodeLoop, DrainWaiter))state!;
                w.Canceled = true;
                loop.Stop();
            }, (this, waiter));
        }

        if (!Process.IsKilled)
        {
            Process.Resume();
            // On the loop itself the check happens after the current item returns (the scheduler re-checks after every
            // main-lane item); elsewhere the loop may already be empty.
            if (!IsOnLoop)
                Process.Scheduler.CompleteDrainIfEmpty(Process);
        }

        return waiter.Task;
    }

    /// <inheritdoc />
    public override string ToString() => $"SimNodeLoop({Process.Id})";

    internal void RaiseUnhandled(Exception exception)
    {
        Action<INodeLoop, Exception>? handlers = UnhandledException;
        if (handlers is null)
            return;
        foreach (Delegate handler in handlers.GetInvocationList())
        {
            try
            {
                ((Action<INodeLoop, Exception>)handler)(this, exception);
            }
            catch
            {
                // Matches the production loop: handler failures are swallowed.
            }
        }
    }

    internal void OnProcessKilled()
    {
        _invoker.Close();
        DrainWaiter[] waiters = [.. _drainWaiters];
        _drainWaiters.Clear();
        foreach (DrainWaiter waiter in waiters)
            waiter.Complete();
    }

    private sealed class DrainWaiter(CancellationToken ct)
    {
        private readonly TaskCompletionSource _source = new();

        public CancellationTokenRegistration Registration { get; set; }

        public bool Canceled { get; set; }

        public Task Task => _source.Task;

        public void Complete()
        {
            Registration.Dispose();
            SimScheduler.CompleteWithoutContext(() =>
            {
                if (Canceled)
                    _source.TrySetCanceled(ct);
                else
                    _source.TrySetResult();
            });
        }
    }
}
