namespace Tedd.Tdfs.Testing.Simulation;

/// <summary>
/// A simulated process inside a <see cref="SimScheduler"/>: one incarnation of a node, a client, or the built-in
/// client process. It owns a main lane (its <see cref="Context"/>; the node loop of a <see cref="SimNodeLoop"/>) and
/// background lanes (timer callbacks of clocks bound to it, network deliveries to its transports).
/// </summary>
/// <remarks>
/// <see cref="Pause"/> freezes the whole process like SIGSTOP: every lane is held and timers that come due fire once on
/// <see cref="Resume"/>. <see cref="Kill"/> is a crash: queued work is discarded, later work is dropped, its timers never
/// fire again, its network transports die (peers may observe a reset) and pending <see cref="SimNodeLoop.InvokeAsync(Action)"/>
/// calls fault. A killed process never comes back; restarting a node creates a new incarnation with the same name.
/// </remarks>
public sealed class SimProcess
{
    private readonly TaskCompletionSource _exited = new();

    internal SimProcess(SimScheduler scheduler, string name, int incarnation)
    {
        Scheduler = scheduler;
        Name = name;
        Incarnation = incarnation;
        Id = string.Concat(name, "#", incarnation.ToString(System.Globalization.CultureInfo.InvariantCulture));
        var context = new SimSynchronizationContext(this);
        Context = context;
        MainLane = scheduler.CreateLane(this, Id + "/main", context, transient: false);
    }

    /// <summary>The owning scheduler.</summary>
    public SimScheduler Scheduler { get; }

    /// <summary>Process name (node or client name).</summary>
    public string Name { get; }

    /// <summary>1 for the first process with this name, incremented for every later incarnation.</summary>
    public int Incarnation { get; }

    /// <summary><c>name#incarnation</c>.</summary>
    public string Id { get; }

    /// <summary>Synchronization context posting to the main lane.</summary>
    public SynchronizationContext Context { get; }

    /// <summary>Current state.</summary>
    public SimProcessState State
    {
        get
        {
            lock (Scheduler.SyncRoot)
                return StateLocked;
        }
    }

    /// <summary>True while running.</summary>
    public bool IsRunning => State == SimProcessState.Running;

    /// <summary>True while paused.</summary>
    public bool IsPaused => State == SimProcessState.Paused;

    /// <summary>True once killed.</summary>
    public bool IsKilled => State == SimProcessState.Killed;

    /// <summary>True when the driving thread is executing a work item (any lane) of this process.</summary>
    public bool IsCurrent => ReferenceEquals(Scheduler.CurrentProcess, this);

    /// <summary>True when the driving thread is executing a work item of this process's main lane.</summary>
    public bool IsOnMainLane => ReferenceEquals(Scheduler.CurrentLane, MainLane);

    /// <summary>The node loop attached to this process, if one was created by <see cref="SimNodeLoopFactory"/>.</summary>
    public SimNodeLoop? Loop { get; internal set; }

    /// <summary>Completes when the process is killed.</summary>
    public Task Exited => _exited.Task;

    /// <summary>Number of work items discarded because the process was killed (queued at the kill or posted later).</summary>
    public long DiscardedWorkCount
    {
        get
        {
            lock (Scheduler.SyncRoot)
                return DiscardedLocked;
        }
    }

    /// <summary>Number of queued (runnable or held) work items on all lanes of this process.</summary>
    public int QueuedWorkCount
    {
        get
        {
            lock (Scheduler.SyncRoot)
                return Scheduler.CountQueuedLocked(this);
        }
    }

    internal SimLane MainLane { get; }

    internal SimProcessState StateLocked { get; set; }

    internal long DiscardedLocked { get; set; }

    /// <summary>Lanes holding work while the process is paused (in the order they were held).</summary>
    internal List<SimLane> HeldLanes { get; } = [];

    /// <summary>Timers that came due while the process was paused.</summary>
    internal List<(SimTimeProvider.SimTimer Timer, long Version)> ParkedTimers { get; } = [];

    /// <summary>Queues work on the main lane (discarded when the process is dead).</summary>
    public void Post(Action work, string? description = null)
    {
        ArgumentNullException.ThrowIfNull(work);
        Scheduler.Enqueue(MainLane, new SimWorkItem(work, description), "post work");
    }

    /// <summary>Freezes the process (all of its lanes are held).</summary>
    public void Pause() => Scheduler.PauseProcess(this);

    /// <summary>Unfreezes the process; timers that came due meanwhile fire once.</summary>
    public void Resume() => Scheduler.ResumeProcess(this);

    /// <summary>Kills the process (crash). A work item of this process that is executing keeps running until it returns.</summary>
    public void Kill() => Scheduler.KillProcess(this, "killed");

    /// <summary>
    /// Kills the process and, when called from one of its own work items, throws <see cref="SimProcessKilledException"/> so
    /// the crashing code does not continue (the scheduler ignores exceptions escaping work of a killed process).
    /// </summary>
    public void Crash()
    {
        bool current = IsCurrent;
        Kill();
        if (current)
            throw new SimProcessKilledException(Id);
    }

    /// <inheritdoc />
    public override string ToString() => Id;

    internal void OnKilled()
    {
        SimScheduler.CompleteWithoutContext(() => _exited.TrySetResult());
        Loop?.OnProcessKilled();
    }
}

/// <summary>Synchronization context of a <see cref="SimProcess"/>: posts to its main lane.</summary>
internal sealed class SimSynchronizationContext : SynchronizationContext
{
    private readonly SimProcess _process;

    public SimSynchronizationContext(SimProcess process)
    {
        _process = process;
    }

    public SimProcess Process => _process;

    public override void Post(SendOrPostCallback d, object? state)
    {
        ArgumentNullException.ThrowIfNull(d);
        _process.Scheduler.Enqueue(_process.MainLane, new SimWorkItem(d, state, null), "post to a simulated synchronization context");
    }

    public override void Send(SendOrPostCallback d, object? state)
    {
        ArgumentNullException.ThrowIfNull(d);
        _process.Scheduler.SendInline(_process.MainLane, d, state);
    }

    public override SynchronizationContext CreateCopy() => this;

    public override string ToString() => $"SimContext({_process.Id})";
}
