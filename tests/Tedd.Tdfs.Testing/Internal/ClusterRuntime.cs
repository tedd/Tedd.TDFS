using System.Globalization;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing.Simulation;

namespace Tedd.Tdfs.Testing;

/// <summary>
/// The execution context of a test cluster: real threads and the system clock (<see cref="ClusterMode.Realtime"/>) or one
/// <see cref="SimScheduler"/> driving every node loop, clock, network delivery and the scenario itself
/// (<see cref="ClusterMode.Deterministic"/>). Harness code obtains every clock, loop factory, completion source and delay
/// from here so that the same harness code is correct (and, in deterministic mode, replayable) in both modes.
/// </summary>
internal abstract class ClusterRuntime
{
    /// <summary>The execution mode.</summary>
    public abstract ClusterMode Mode { get; }

    /// <summary>The harness clock (<see cref="TestCluster.Time"/>): the system clock, or the global virtual clock.</summary>
    public abstract TimeProvider Time { get; }

    /// <summary>The scheduler in deterministic mode; null in realtime mode.</summary>
    public abstract SimScheduler? Scheduler { get; }

    /// <summary>Creates the node-loop factory for nodes of a cluster.</summary>
    public abstract INodeLoopFactory CreateLoopFactory(INodeLog log);

    /// <summary>
    /// Creates the controllable clock of one simulated process (node, backup node or client). The clock is independent of
    /// every other clock (HAR-003); in deterministic mode its timers belong to the process named <paramref name="processName"/>.
    /// </summary>
    public abstract TestClock CreateProcessClock(string processName);

    /// <summary>Creates a completion source suitable for waking harness or product code in this mode.</summary>
    public abstract TaskCompletionSource CreateCompletion();

    /// <summary>Creates a completion source suitable for waking harness or product code in this mode.</summary>
    public abstract TaskCompletionSource<T> CreateCompletion<T>();

    /// <summary>
    /// Runs <paramref name="completion"/> (completing tasks awaited by product code) so that continuations stay inside the
    /// execution context: in deterministic mode without a synchronization context installed (see
    /// <see cref="SimScheduler.CompleteWithoutContext"/>), in realtime mode directly.
    /// </summary>
    public abstract void Complete(Action completion);

    /// <summary>Prefix of trace lines: virtual time in deterministic mode, elapsed real time since the runtime started otherwise.</summary>
    public abstract string FormatNow();

    /// <summary>A delay on the harness clock (virtual in deterministic mode).</summary>
    public Task DelayAsync(TimeSpan delay, CancellationToken ct = default) =>
        delay <= TimeSpan.Zero ? Task.CompletedTask : Task.Delay(delay, Time, ct);

    /// <summary>
    /// Waits for <paramref name="task"/> at most <paramref name="timeout"/> of harness time; true when it completed. Never
    /// throws for the task's own failure and never uses a cancellable delay (simulation-compatible).
    /// </summary>
    public async Task<bool> WaitAsync(Task task, TimeSpan timeout)
    {
        ArgumentNullException.ThrowIfNull(task);
        if (task.IsCompleted)
            return true;
        if (timeout == Timeout.InfiniteTimeSpan)
        {
            await task.ContinueWith(static _ => { }, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
            return true;
        }

        TaskCompletionSource expired = CreateCompletion();
        using ITimer timer = Time.CreateTimer(static state => ((Action)state!)(), (Action)(() => Complete(() => expired.TrySetResult())),
            timeout < TimeSpan.Zero ? TimeSpan.Zero : timeout, Timeout.InfiniteTimeSpan);
        Task first = await Task.WhenAny(task, expired.Task);
        return ReferenceEquals(first, task);
    }

    /// <summary>The realtime runtime (shared: it has no per-run state besides its start time).</summary>
    public static ClusterRuntime CreateRealtime() => new RealtimeRuntime();

    /// <summary>A deterministic runtime over <paramref name="scheduler"/>.</summary>
    public static ClusterRuntime CreateDeterministic(SimScheduler scheduler) => new DeterministicRuntime(scheduler);

    private sealed class RealtimeRuntime : ClusterRuntime
    {
        private readonly long _start = TimeProvider.System.GetTimestamp();

        public override ClusterMode Mode => ClusterMode.Realtime;

        public override TimeProvider Time => TimeProvider.System;

        public override SimScheduler? Scheduler => null;

        public override INodeLoopFactory CreateLoopFactory(INodeLog log) => new ThreadNodeLoopFactory(log);

        public override TestClock CreateProcessClock(string processName) => new();

        public override TaskCompletionSource CreateCompletion() => new(TaskCreationOptions.RunContinuationsAsynchronously);

        public override TaskCompletionSource<T> CreateCompletion<T>() => new(TaskCreationOptions.RunContinuationsAsynchronously);

        public override void Complete(Action completion) => completion();

        public override string FormatNow() =>
            TimeProvider.System.GetElapsedTime(_start).TotalSeconds.ToString("0.000000", CultureInfo.InvariantCulture) + "s";
    }

    private sealed class DeterministicRuntime : ClusterRuntime
    {
        private readonly SimScheduler _scheduler;

        public DeterministicRuntime(SimScheduler scheduler)
        {
            _scheduler = scheduler;
        }

        public override ClusterMode Mode => ClusterMode.Deterministic;

        public override TimeProvider Time => _scheduler.Clock;

        public override SimScheduler? Scheduler => _scheduler;

        public override INodeLoopFactory CreateLoopFactory(INodeLog log) => _scheduler.LoopFactory;

        public override TestClock CreateProcessClock(string processName)
        {
            SimTimeProvider baseClock = _scheduler.FindClock(processName) ?? _scheduler.CreateClock(processName);
            return new TestClock(baseClock) { CallbackInvoker = SimScheduler.CompleteWithoutContext };
        }

        // No RunContinuationsAsynchronously: continuations must stay on the simulation thread.
        public override TaskCompletionSource CreateCompletion() => new();

        public override TaskCompletionSource<T> CreateCompletion<T>() => new();

        public override void Complete(Action completion) => SimScheduler.CompleteWithoutContext(completion);

        public override string FormatNow() => SimScheduler.FormatTime(_scheduler.Now);
    }
}
