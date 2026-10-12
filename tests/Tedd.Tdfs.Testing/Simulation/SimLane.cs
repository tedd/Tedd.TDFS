namespace Tedd.Tdfs.Testing.Simulation;

/// <summary>
/// A FIFO sequence of work items inside a <see cref="SimScheduler"/>. Items of one lane always run in the order they
/// were queued; the scheduler interleaves different lanes. A process has a main lane (its loop / synchronization
/// context) and background lanes (timer callbacks, network deliveries per connection direction). Guarded by the
/// scheduler lock.
/// </summary>
internal sealed class SimLane
{
    public SimLane(long id, string name, SimProcess? owner, SynchronizationContext? context, bool transient)
    {
        Id = id;
        Name = name;
        Owner = owner;
        Context = context;
        Transient = transient;
    }

    /// <summary>Deterministic creation number (mixed into the schedule fingerprint).</summary>
    public long Id { get; }

    /// <summary>Diagnostic name, for example <c>n0#1/main</c>.</summary>
    public string Name { get; }

    /// <summary>Owning process; null for global (harness) lanes, which are never paused or killed.</summary>
    public SimProcess? Owner { get; }

    /// <summary>Synchronization context installed while an item of this lane runs (null for background lanes).</summary>
    public SynchronizationContext? Context { get; }

    /// <summary>True for single-use lanes (one timer firing, one independent delivery).</summary>
    public bool Transient { get; }

    /// <summary>Queued items.</summary>
    public Queue<SimWorkItem> Items { get; } = new();

    /// <summary>Index in the scheduler's runnable list, or -1.</summary>
    public int RunnableIndex { get; set; } = -1;

    /// <summary>True while parked in the owner's held list because the owner is paused.</summary>
    public bool Held { get; set; }

    /// <inheritdoc />
    public override string ToString() => Name;
}

/// <summary>One unit of work queued on a <see cref="SimLane"/>.</summary>
internal struct SimWorkItem
{
    public SimWorkItem(Action action, string? description, Action? onDiscard = null)
    {
        Action = action;
        Callback = null;
        State = null;
        Description = description;
        OnDiscard = onDiscard;
        Seq = 0;
    }

    public SimWorkItem(SendOrPostCallback callback, object? state, string? description, Action? onDiscard = null)
    {
        Action = null;
        Callback = callback;
        State = state;
        Description = description;
        OnDiscard = onDiscard;
        Seq = 0;
    }

    public Action? Action { get; }

    public SendOrPostCallback? Callback { get; }

    public object? State { get; }

    /// <summary>Optional description for step traces.</summary>
    public string? Description { get; }

    /// <summary>Invoked (outside the scheduler lock) when the item is discarded because its owner was killed.</summary>
    public Action? OnDiscard { get; }

    /// <summary>Global sequence number assigned when queued.</summary>
    public long Seq { get; set; }

    public readonly void Invoke()
    {
        if (Action is not null)
            Action();
        else
            Callback!(State);
    }
}

/// <summary>An entry of the scheduler's virtual-time queue. <see cref="OnDue"/> runs on the driving thread, outside any work item and outside the lock.</summary>
internal abstract class SimTimedEntry
{
    /// <summary>Virtual due time (scheduler elapsed ticks). Set when scheduled.</summary>
    public long Due { get; set; }

    /// <summary>Tie-breaking sequence number. Set when scheduled.</summary>
    public long Seq { get; set; }

    /// <summary>Lazily deleted entries are skipped when they come due.</summary>
    public bool Cancelled { get; set; }

    public abstract void OnDue(SimScheduler scheduler);
}

/// <summary>Queues a work item onto a fixed lane when due (FIFO network deliveries).</summary>
internal sealed class SimLaneEntry(SimLane lane, SimWorkItem item) : SimTimedEntry
{
    public override void OnDue(SimScheduler scheduler) => scheduler.EnqueueFromTimer(lane, item);
}

/// <summary>Queues a work item onto a fresh transient lane of <paramref name="owner"/> when due.</summary>
internal sealed class SimActionEntry(SimProcess? owner, string laneName, SimWorkItem item) : SimTimedEntry
{
    public override void OnDue(SimScheduler scheduler)
    {
        SimLane lane = scheduler.CreateLane(owner, laneName, context: null, transient: true);
        scheduler.EnqueueFromTimer(lane, item);
    }
}

/// <summary>Cancellation handle of a scheduled global action.</summary>
internal sealed class SimScheduledHandle(SimScheduler scheduler, SimTimedEntry entry) : IDisposable
{
    public void Dispose() => scheduler.Cancel(entry);
}
