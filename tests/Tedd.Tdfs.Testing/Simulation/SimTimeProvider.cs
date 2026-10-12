using System.Globalization;

namespace Tedd.Tdfs.Testing.Simulation;

/// <summary>
/// A <see cref="TimeProvider"/> derived from the virtual time of a <see cref="SimScheduler"/>. The global clock
/// (<see cref="SimScheduler.Clock"/>) advances exactly with virtual time; per-node clocks
/// (<see cref="SimScheduler.CreateClock"/>) add an independent wall offset, drift rate, freezing and forward jumps (HAR-003).
/// </summary>
/// <remarks>
/// <para>
/// <b>Monotonic reading</b> (<see cref="GetTimestamp"/>, frequency = <see cref="TimeSpan.TicksPerSecond"/>): advances with
/// global virtual time scaled by <see cref="Rate"/>, stops while <see cref="IsFrozen"/>, jumps on <see cref="Advance"/>,
/// never decreases. <b>Wall reading</b> (<see cref="GetUtcNow"/>): moves with the monotonic reading;
/// <see cref="JumpWall"/> steps it either way without touching the monotonic reading or timers.
/// </para>
/// <para>
/// <b>Timers</b> are due on the monotonic reading and are translated into global virtual time for the scheduler (and
/// re-translated whenever the rate, freezing or a jump changes the mapping). A timer belongs to a process (see
/// <see cref="SimClockOptions.BindToProcess"/>); its callback runs as a background work item of that process (no
/// synchronization context, like a thread-pool timer callback), is held while the process is paused (firing once on
/// resume; missed periods are not replayed) and never runs after the process was killed. Periodic timers are re-armed when
/// they fire, one period after the previous due time (or after now if that is already past).
/// </para>
/// </remarks>
public sealed class SimTimeProvider : TimeProvider
{
    private readonly SimScheduler _scheduler;
    private readonly SortedDictionary<long, SimTimer> _armed = new();
    private readonly DateTimeOffset _wallStart;
    private readonly long _monoStart;
    private long _anchorGlobal;
    private long _anchorLocal;
    private long _lastLocal;
    private long _wallJumpTicks;
    private long _nextTimerId;
    private double _rate;
    private bool _frozen;

    internal SimTimeProvider(SimScheduler scheduler, string name, bool isGlobal, string? processName, DateTimeOffset wallStart, long monoStart, double rate)
    {
        _scheduler = scheduler;
        Name = name;
        IsGlobal = isGlobal;
        ProcessName = processName;
        _wallStart = wallStart;
        _monoStart = monoStart;
        _anchorGlobal = scheduler.NowTicksLocked;
        _anchorLocal = monoStart;
        _lastLocal = monoStart;
        _rate = rate;
    }

    /// <summary>The owning scheduler.</summary>
    public SimScheduler Scheduler => _scheduler;

    /// <summary>Clock name ("global" for <see cref="SimScheduler.Clock"/>).</summary>
    public string Name { get; }

    /// <summary>True for the scheduler's global clock, which cannot be offset, drifted, frozen or jumped.</summary>
    public bool IsGlobal { get; }

    /// <summary>Name of the process timers are bound to, or null when they belong to their creator's process.</summary>
    public string? ProcessName { get; }

    /// <summary>Current drift rate.</summary>
    public double Rate
    {
        get
        {
            lock (_scheduler.SyncRoot)
                return _rate;
        }
    }

    /// <summary>True while frozen.</summary>
    public bool IsFrozen
    {
        get
        {
            lock (_scheduler.SyncRoot)
                return _frozen;
        }
    }

    /// <summary>Current difference between this clock's wall reading and the global virtual wall clock.</summary>
    public TimeSpan WallOffset
    {
        get
        {
            lock (_scheduler.SyncRoot)
                return GetUtcNowLocked() - _scheduler.Options.StartTimeUtc.AddTicks(_scheduler.NowTicksLocked);
        }
    }

    /// <summary>Monotonic time elapsed on this clock since it was created.</summary>
    public TimeSpan Elapsed
    {
        get
        {
            lock (_scheduler.SyncRoot)
                return TimeSpan.FromTicks(LocalNowLocked() - _monoStart);
        }
    }

    /// <summary>Number of armed timers (diagnostics).</summary>
    public int ActiveTimerCount
    {
        get
        {
            lock (_scheduler.SyncRoot)
                return _armed.Count;
        }
    }

    /// <inheritdoc />
    public override long TimestampFrequency => TimeSpan.TicksPerSecond;

    /// <inheritdoc />
    public override TimeZoneInfo LocalTimeZone => TimeZoneInfo.Utc;

    /// <inheritdoc />
    public override long GetTimestamp()
    {
        lock (_scheduler.SyncRoot)
            return LocalNowLocked();
    }

    /// <inheritdoc />
    public override DateTimeOffset GetUtcNow()
    {
        lock (_scheduler.SyncRoot)
            return GetUtcNowLocked();
    }

    /// <inheritdoc />
    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
    {
        ArgumentNullException.ThrowIfNull(callback);
        ValidateTimerSpan(dueTime, nameof(dueTime));
        ValidateTimerSpan(period, nameof(period));
        _scheduler.CheckThread("create timer");
        SimProcess? owner = ResolveOwner();
        long id;
        lock (_scheduler.SyncRoot)
            id = ++_nextTimerId;
        var timer = new SimTimer(this, id, owner, callback, state, ExecutionContext.Capture());
        ChangeTimer(timer, dueTime, period);
        return timer;
    }

    /// <summary>Jumps the clock forward (monotonic and wall); timers that become due fire at the current virtual instant.</summary>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="delta"/> is negative.</exception>
    /// <exception cref="InvalidOperationException">This is the global clock.</exception>
    public void Advance(TimeSpan delta)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(delta, TimeSpan.Zero);
        ThrowIfGlobal(nameof(Advance));
        _scheduler.CheckThread("advance clock");
        lock (_scheduler.SyncRoot)
        {
            ReAnchorLocked();
            _anchorLocal = SaturatingAdd(_anchorLocal, delta.Ticks);
            if (_anchorLocal > _lastLocal)
                _lastLocal = _anchorLocal;
            RescheduleAllLocked();
            _scheduler.TraceLocked($"clock {Name} advanced by {delta}");
        }
    }

    /// <summary>Steps the wall reading only (either direction); the monotonic reading and timers are unaffected.</summary>
    /// <exception cref="InvalidOperationException">This is the global clock.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The wall reading would leave the representable range.</exception>
    public void JumpWall(TimeSpan delta)
    {
        ThrowIfGlobal(nameof(JumpWall));
        _scheduler.CheckThread("jump wall clock");
        lock (_scheduler.SyncRoot)
        {
            long jump = checked(_wallJumpTicks + delta.Ticks);
            long local = LocalNowLocked();
            try
            {
                _ = _wallStart.AddTicks(local - _monoStart + jump);
            }
            catch (ArgumentOutOfRangeException ex)
            {
                throw new ArgumentOutOfRangeException(nameof(delta), delta, $"The wall clock would leave the representable range: {ex.Message}");
            }

            _wallJumpTicks = jump;
            _scheduler.TraceLocked($"clock {Name} wall stepped by {delta}");
        }
    }

    /// <summary>Stops the clock (monotonic and wall); its timers stop firing until <see cref="Unfreeze"/>.</summary>
    /// <exception cref="InvalidOperationException">This is the global clock.</exception>
    public void Freeze()
    {
        ThrowIfGlobal(nameof(Freeze));
        _scheduler.CheckThread("freeze clock");
        lock (_scheduler.SyncRoot)
        {
            if (_frozen)
                return;
            ReAnchorLocked();
            _frozen = true;
            RescheduleAllLocked();
            _scheduler.TraceLocked($"clock {Name} frozen");
        }
    }

    /// <summary>Resumes advancing from the frozen reading (no jump).</summary>
    /// <exception cref="InvalidOperationException">This is the global clock.</exception>
    public void Unfreeze()
    {
        ThrowIfGlobal(nameof(Unfreeze));
        _scheduler.CheckThread("unfreeze clock");
        lock (_scheduler.SyncRoot)
        {
            if (!_frozen)
                return;
            _anchorGlobal = _scheduler.NowTicksLocked;
            _frozen = false;
            RescheduleAllLocked();
            _scheduler.TraceLocked($"clock {Name} unfrozen");
        }
    }

    /// <summary>Sets the drift rate relative to global virtual time (for example 1.01 runs 1 % fast).</summary>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="rate"/> is not a finite positive number.</exception>
    /// <exception cref="InvalidOperationException">This is the global clock.</exception>
    public void SetRate(double rate)
    {
        if (!double.IsFinite(rate) || rate <= 0)
            throw new ArgumentOutOfRangeException(nameof(rate), rate, "Rate must be a finite positive number.");
        ThrowIfGlobal(nameof(SetRate));
        _scheduler.CheckThread("set clock rate");
        lock (_scheduler.SyncRoot)
        {
            ReAnchorLocked();
            _rate = rate;
            RescheduleAllLocked();
            _scheduler.TraceLocked($"clock {Name} rate set to {rate.ToString("R", CultureInfo.InvariantCulture)}");
        }
    }

    /// <inheritdoc />
    public override string ToString() => $"SimTimeProvider({Name})";

    /// <summary>Fires a timer that came due while its process was paused (called on resume).</summary>
    internal void FireParked(SimTimer timer, long version)
    {
        lock (_scheduler.SyncRoot)
        {
            if (timer.Disposed || timer.Version != version || !timer.Parked)
                return;
            timer.Parked = false;
            if (timer.Owner is { StateLocked: SimProcessState.Killed })
            {
                DisarmLocked(timer);
                return;
            }

            PrepareFireLocked(timer);
        }

        EnqueueCallback(timer);
    }

    /// <summary>Disarms every timer owned by <paramref name="process"/> (it was killed). Caller holds the scheduler lock.</summary>
    internal void DisarmOwnedByLocked(SimProcess process)
    {
        List<SimTimer>? owned = null;
        foreach (SimTimer timer in _armed.Values)
        {
            if (ReferenceEquals(timer.Owner, process))
                (owned ??= []).Add(timer);
        }

        if (owned is null)
            return;
        foreach (SimTimer timer in owned)
        {
            CancelEntryLocked(timer);
            timer.Parked = false;
            DisarmLocked(timer);
        }
    }

    internal bool ChangeTimer(SimTimer timer, TimeSpan dueTime, TimeSpan period)
    {
        ValidateTimerSpan(dueTime, nameof(dueTime));
        ValidateTimerSpan(period, nameof(period));
        _scheduler.CheckThread("change timer");
        lock (_scheduler.SyncRoot)
        {
            if (timer.Disposed)
                return false;
            CancelEntryLocked(timer);
            timer.Version++;
            timer.Parked = false; // a parked firing (if any) is superseded: its version no longer matches
            if (dueTime == Timeout.InfiniteTimeSpan)
            {
                DisarmLocked(timer);
            }
            else
            {
                timer.DueLocal = SaturatingAdd(LocalNowLocked(), dueTime.Ticks);
                timer.PeriodTicks = period == Timeout.InfiniteTimeSpan || period == TimeSpan.Zero ? 0 : period.Ticks;
                _armed[timer.Id] = timer;
                ScheduleTimerLocked(timer);
            }
        }

        return true;
    }

    internal void DisposeTimer(SimTimer timer)
    {
        _scheduler.CheckThread("dispose timer");
        lock (_scheduler.SyncRoot)
        {
            if (timer.Disposed)
                return;
            timer.Disposed = true;
            timer.Version++;
            timer.Parked = false;
            CancelEntryLocked(timer);
            DisarmLocked(timer);
        }
    }

    internal void OnTimerDue(SimTimer timer, long version)
    {
        lock (_scheduler.SyncRoot)
        {
            if (timer.Disposed || timer.Version != version)
                return;
            timer.Entry = null;
            SimProcess? owner = timer.Owner;
            if (owner is not null)
            {
                if (owner.StateLocked == SimProcessState.Killed)
                {
                    DisarmLocked(timer);
                    return;
                }

                if (owner.StateLocked == SimProcessState.Paused)
                {
                    timer.Parked = true;
                    owner.ParkedTimers.Add((timer, version));
                    return;
                }
            }

            PrepareFireLocked(timer);
        }

        EnqueueCallback(timer);
    }

    private static void ValidateTimerSpan(TimeSpan value, string name)
    {
        if (value < TimeSpan.Zero && value != Timeout.InfiniteTimeSpan)
            throw new ArgumentOutOfRangeException(name, value, "Timer spans must be nonnegative or Timeout.InfiniteTimeSpan.");
    }

    private static long SaturatingAdd(long a, long b)
    {
        long r = unchecked(a + b);
        if (b > 0 && r < a)
            return long.MaxValue - 1;
        if (b < 0 && r > a)
            return long.MinValue;
        return r;
    }

    private void ThrowIfGlobal(string operation)
    {
        if (IsGlobal)
        {
            throw new InvalidOperationException(
                $"{operation} is not supported on the global simulation clock: it advances only with virtual time. Use a per-node clock (SimScheduler.CreateClock).");
        }
    }

    private SimProcess? ResolveOwner()
    {
        SimProcess? current = _scheduler.CurrentProcess;
        if (ProcessName is null)
            return current;
        if (current is not null && string.Equals(current.Name, ProcessName, StringComparison.Ordinal))
            return current; // the running incarnation (or a killed one still unwinding: its timers never fire)
        return _scheduler.GetOrCreateProcess(ProcessName);
    }

    private DateTimeOffset GetUtcNowLocked() => _wallStart.AddTicks(LocalNowLocked() - _monoStart + _wallJumpTicks);

    private long Scale(long globalElapsed) =>
        _rate == 1.0 ? globalElapsed : (long)Math.Floor(globalElapsed * _rate);

    private long LocalAtLocked(long globalTicks)
    {
        long elapsed = globalTicks - _anchorGlobal;
        return elapsed > 0 ? SaturatingAdd(_anchorLocal, Scale(elapsed)) : _anchorLocal;
    }

    private long LocalNowLocked()
    {
        long local = _frozen ? _anchorLocal : LocalAtLocked(_scheduler.NowTicksLocked);
        if (local < _lastLocal)
            return _lastLocal;
        _lastLocal = local;
        return local;
    }

    private void ReAnchorLocked()
    {
        long local = LocalNowLocked();
        _anchorLocal = local;
        _anchorGlobal = _scheduler.NowTicksLocked;
    }

    /// <summary>
    /// Smallest global virtual time at which this clock reads at least <paramref name="localDue"/>, or
    /// <see cref="long.MaxValue"/> when it never will under the current mapping (frozen, or beyond the representable range).
    /// </summary>
    private long GlobalDueLocked(long localDue)
    {
        if (_frozen || localDue >= long.MaxValue - 1)
            return long.MaxValue;
        long now = _scheduler.NowTicksLocked;
        if (localDue <= LocalNowLocked())
            return now;
        double need = (localDue - _anchorLocal) / _rate;
        if (need >= (double)(long.MaxValue / 4) - _anchorGlobal)
            return long.MaxValue;
        long g = _anchorGlobal + (long)Math.Ceiling(need);
        while (g > now && LocalAtLocked(g - 1) >= localDue)
            g--;
        while (LocalAtLocked(g) < localDue)
            g++;
        return g < now ? now : g;
    }

    private void RescheduleAllLocked()
    {
        foreach (SimTimer timer in _armed.Values)
            ScheduleTimerLocked(timer);
    }

    private void ScheduleTimerLocked(SimTimer timer)
    {
        CancelEntryLocked(timer);
        if (timer.Disposed || timer.Parked || timer.DueLocal == long.MaxValue)
            return;
        long due = GlobalDueLocked(timer.DueLocal);
        if (due == long.MaxValue)
            return; // frozen or unreachable: rescheduled when the mapping changes
        var entry = new TimerEntry(timer, timer.Version);
        timer.Entry = entry;
        _scheduler.ScheduleLocked(entry, due);
    }

    private static void CancelEntryLocked(SimTimer timer)
    {
        if (timer.Entry is { } entry)
        {
            entry.Cancelled = true;
            timer.Entry = null;
        }
    }

    private void DisarmLocked(SimTimer timer)
    {
        timer.DueLocal = long.MaxValue;
        timer.PeriodTicks = 0;
        _armed.Remove(timer.Id);
    }

    private void PrepareFireLocked(SimTimer timer)
    {
        if (timer.PeriodTicks > 0)
        {
            long now = LocalNowLocked();
            long next = SaturatingAdd(timer.DueLocal, timer.PeriodTicks);
            timer.DueLocal = next > now ? next : SaturatingAdd(now, timer.PeriodTicks);
            ScheduleTimerLocked(timer);
        }
        else
        {
            DisarmLocked(timer);
        }
    }

    private void EnqueueCallback(SimTimer timer)
    {
        SimProcess? owner = timer.Owner;
        SimLane lane = _scheduler.CreateLane(owner, (owner?.Id ?? "global") + "/timer", context: null, transient: true);
        _scheduler.EnqueueFromTimer(lane, new SimWorkItem(timer.Fire, timer.Description));
    }

    /// <summary>A timer of a <see cref="SimTimeProvider"/>. State is guarded by the scheduler lock.</summary>
    internal sealed class SimTimer : ITimer
    {
        private readonly TimerCallback _callback;
        private readonly object? _state;
        private readonly ExecutionContext? _context;

        public SimTimer(SimTimeProvider clock, long id, SimProcess? owner, TimerCallback callback, object? state, ExecutionContext? context)
        {
            Clock = clock;
            Id = id;
            Owner = owner;
            _callback = callback;
            _state = state;
            _context = context;
            Fire = Invoke;
            Description = string.Concat("timer ", clock.Name, "#", id.ToString(CultureInfo.InvariantCulture));
        }

        public SimTimeProvider Clock { get; }

        public long Id { get; }

        public SimProcess? Owner { get; }

        public Action Fire { get; }

        public string Description { get; }

        public long Version { get; set; }

        /// <summary>Due time on the clock's monotonic reading; <see cref="long.MaxValue"/> when disarmed.</summary>
        public long DueLocal { get; set; } = long.MaxValue;

        /// <summary>Period in ticks; 0 for one-shot.</summary>
        public long PeriodTicks { get; set; }

        public bool Disposed { get; set; }

        /// <summary>True while a firing is parked because the owner is paused.</summary>
        public bool Parked { get; set; }

        public TimerEntry? Entry { get; set; }

        public bool Change(TimeSpan dueTime, TimeSpan period) => Clock.ChangeTimer(this, dueTime, period);

        public void Dispose() => Clock.DisposeTimer(this);

        public ValueTask DisposeAsync()
        {
            Dispose();
            return ValueTask.CompletedTask;
        }

        public override string ToString() => Description;

        private void Invoke()
        {
            if (_context is null)
                _callback(_state);
            else
                ExecutionContext.Run(_context, static s => { var t = (SimTimer)s!; t._callback(t._state); }, this);
        }
    }

    /// <summary>Virtual-time queue entry of one scheduled firing.</summary>
    internal sealed class TimerEntry(SimTimer timer, long version) : SimTimedEntry
    {
        public override void OnDue(SimScheduler scheduler) => timer.Clock.OnTimerDue(timer, version);
    }
}
