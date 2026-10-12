namespace Tedd.Tdfs.Testing;

/// <summary>
/// A per-node/per-client controllable clock layered over a base <see cref="TimeProvider"/> (default
/// <see cref="TimeProvider.System"/>).
/// </summary>
/// <remarks>
/// <para>
/// The clock keeps a <b>monotonic</b> reading (exposed through <see cref="GetTimestamp"/> with
/// <see cref="TimestampFrequency"/> = <see cref="TimeSpan.TicksPerSecond"/>) that advances with the base clock scaled by
/// <see cref="Rate"/>, stops while <see cref="IsFrozen"/>, and jumps forward on <see cref="Advance"/>. It never decreases.
/// </para>
/// <para>
/// The <b>wall</b> reading (<see cref="GetUtcNow"/>) starts at the base wall time at construction and moves with the
/// monotonic reading; <see cref="JumpWall"/> additionally steps it (forwards or backwards) without touching the
/// monotonic reading, so ordering and timers are unaffected by wall steps.
/// </para>
/// <para>
/// Timers created by <see cref="CreateTimer"/> are scheduled on the monotonic reading: they do not fire while the clock
/// is frozen (except for timers that are already due), they fire sooner/later under a changed rate, and
/// <see cref="Advance"/> synchronously fires every timer that became due (each once; periodic timers are re-armed one
/// period after the new time rather than replaying missed periods).
/// </para>
/// All members are thread-safe.
/// </remarks>
public sealed class TestClock : TimeProvider
{
    private static readonly long MaxBaseTimerTicks = TimeSpan.FromMilliseconds(uint.MaxValue - 1L).Ticks;

    private readonly object _lock = new();
    private readonly TimeProvider _base;
    private readonly DateTimeOffset _wallEpoch;
    private readonly long _monoEpochTicks;
    private readonly HashSet<ClockTimer> _armed = [];
    private long _baseAnchorTimestamp;
    private long _monoAnchorTicks;
    private long _lastMonoTicks;
    private long _wallJumpTicks;
    private double _rate = 1.0;
    private bool _frozen;
    private long _timerSequence;

    /// <summary>Creates a clock that initially reads the same wall time as <paramref name="baseProvider"/>.</summary>
    /// <param name="baseProvider">Underlying time source; null means <see cref="TimeProvider.System"/>.</param>
    public TestClock(TimeProvider? baseProvider = null)
    {
        _base = baseProvider ?? System;
        _baseAnchorTimestamp = _base.GetTimestamp();
        _wallEpoch = _base.GetUtcNow();
        long frequency = _base.TimestampFrequency;
        _monoEpochTicks = frequency == TimeSpan.TicksPerSecond
            ? _baseAnchorTimestamp
            : (long)((Int128)_baseAnchorTimestamp * TimeSpan.TicksPerSecond / frequency);
        _monoAnchorTicks = _monoEpochTicks;
        _lastMonoTicks = _monoEpochTicks;
    }

    /// <summary>True while the clock is frozen.</summary>
    public bool IsFrozen
    {
        get
        {
            lock (_lock)
                return _frozen;
        }
    }

    /// <summary>Current rate relative to the base clock (1.0 = real speed).</summary>
    public double Rate
    {
        get
        {
            lock (_lock)
                return _rate;
        }
    }

    /// <inheritdoc />
    public override long TimestampFrequency => TimeSpan.TicksPerSecond;

    /// <inheritdoc />
    public override TimeZoneInfo LocalTimeZone => _base.LocalTimeZone;

    /// <inheritdoc />
    public override long GetTimestamp()
    {
        lock (_lock)
            return CurrentMonoTicksLocked();
    }

    /// <inheritdoc />
    public override DateTimeOffset GetUtcNow()
    {
        lock (_lock)
        {
            long mono = CurrentMonoTicksLocked();
            return _wallEpoch.AddTicks(mono - _monoEpochTicks + _wallJumpTicks);
        }
    }

    /// <summary>Jumps forward (wall and monotonic) and synchronously fires every timer that became due.</summary>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="delta"/> is negative.</exception>
    /// <exception cref="AggregateException">One or more timer callbacks threw (all due callbacks still ran).</exception>
    public void Advance(TimeSpan delta)
    {
        if (delta < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(delta), delta, "Advance requires a nonnegative delta; use JumpWall to step the wall clock backwards.");

        List<(ClockTimer Timer, long Due, long Sequence)> due = [];
        lock (_lock)
        {
            ReAnchorLocked();
            _monoAnchorTicks = checked(_monoAnchorTicks + delta.Ticks);
            if (_monoAnchorTicks > _lastMonoTicks)
                _lastMonoTicks = _monoAnchorTicks;
            long now = CurrentMonoTicksLocked();

            foreach (ClockTimer timer in _armed.ToArray())
            {
                if (timer.DueTicks <= now)
                {
                    due.Add((timer, timer.DueTicks, timer.Sequence));
                    timer.OnFiredLocked(now);
                    ScheduleLocked(timer, now); // disarms the base timer of fired one-shot timers
                }
            }

            foreach (ClockTimer timer in _armed)
                ScheduleLocked(timer, now);
        }

        due.Sort(static (a, b) => a.Due != b.Due ? a.Due.CompareTo(b.Due) : a.Sequence.CompareTo(b.Sequence));
        List<Exception>? errors = null;
        foreach ((ClockTimer timer, _, _) in due)
        {
            try
            {
                timer.InvokeCallback();
            }
            catch (Exception ex)
            {
                (errors ??= []).Add(ex);
            }
        }

        if (errors is not null)
            throw new AggregateException("One or more timer callbacks failed during TestClock.Advance.", errors);
    }

    /// <summary>Steps the wall clock only (may be negative); the monotonic reading and timers are unaffected.</summary>
    public void JumpWall(TimeSpan delta)
    {
        lock (_lock)
            _wallJumpTicks = checked(_wallJumpTicks + delta.Ticks);
    }

    /// <summary>Stops the clock from advancing with the base clock; timers stop firing (already-due timers still fire).</summary>
    public void Freeze()
    {
        lock (_lock)
        {
            if (_frozen)
                return;
            ReAnchorLocked();
            _frozen = true;
            long now = CurrentMonoTicksLocked();
            foreach (ClockTimer timer in _armed)
                ScheduleLocked(timer, now);
        }
    }

    /// <summary>Resumes advancing with the base clock from the frozen reading (no jump).</summary>
    public void Unfreeze()
    {
        lock (_lock)
        {
            if (!_frozen)
                return;
            _baseAnchorTimestamp = _base.GetTimestamp();
            _frozen = false;
            long now = CurrentMonoTicksLocked();
            foreach (ClockTimer timer in _armed)
                ScheduleLocked(timer, now);
        }
    }

    /// <summary>Sets the drift rate relative to the base clock (for example 1.01 runs 1 % fast).</summary>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="rate"/> is not a finite positive number.</exception>
    public void SetRate(double rate)
    {
        if (!double.IsFinite(rate) || rate <= 0)
            throw new ArgumentOutOfRangeException(nameof(rate), rate, "Rate must be a finite positive number.");
        lock (_lock)
        {
            ReAnchorLocked();
            _rate = rate;
            long now = CurrentMonoTicksLocked();
            foreach (ClockTimer timer in _armed)
                ScheduleLocked(timer, now);
        }
    }

    /// <inheritdoc />
    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
    {
        ArgumentNullException.ThrowIfNull(callback);
        ValidateTimerSpan(dueTime, nameof(dueTime));
        ValidateTimerSpan(period, nameof(period));
        long sequence;
        lock (_lock)
            sequence = ++_timerSequence;
        var timer = new ClockTimer(this, callback, state, sequence);
        timer.Change(dueTime, period);
        return timer;
    }

    private static void ValidateTimerSpan(TimeSpan value, string name)
    {
        if (value < TimeSpan.Zero && value != Timeout.InfiniteTimeSpan)
            throw new ArgumentOutOfRangeException(name, value, "Timer spans must be nonnegative or Timeout.InfiniteTimeSpan.");
    }

    private long CurrentMonoTicksLocked()
    {
        long ticks = _monoAnchorTicks;
        if (!_frozen)
        {
            long elapsed = _base.GetElapsedTime(_baseAnchorTimestamp, _base.GetTimestamp()).Ticks;
            if (elapsed > 0)
                ticks += _rate == 1.0 ? elapsed : (long)(elapsed * _rate);
        }

        if (ticks < _lastMonoTicks)
            return _lastMonoTicks;
        _lastMonoTicks = ticks;
        return ticks;
    }

    private void ReAnchorLocked()
    {
        long baseNow = _base.GetTimestamp();
        long mono = _monoAnchorTicks;
        if (!_frozen)
        {
            long elapsed = _base.GetElapsedTime(_baseAnchorTimestamp, baseNow).Ticks;
            if (elapsed > 0)
                mono += _rate == 1.0 ? elapsed : (long)(elapsed * _rate);
        }

        if (mono < _lastMonoTicks)
            mono = _lastMonoTicks;
        _lastMonoTicks = mono;
        _monoAnchorTicks = mono;
        _baseAnchorTimestamp = baseNow;
    }

    private void ScheduleLocked(ClockTimer timer, long now)
    {
        TimeSpan realDelay;
        if (timer.DueTicks == long.MaxValue || timer.Disposed)
            realDelay = Timeout.InfiniteTimeSpan;
        else if (timer.DueTicks <= now)
            realDelay = TimeSpan.Zero;
        else if (_frozen)
            realDelay = Timeout.InfiniteTimeSpan;
        else
        {
            double real = Math.Ceiling((timer.DueTicks - now) / _rate);
            long realTicks = real >= MaxBaseTimerTicks ? MaxBaseTimerTicks : Math.Max(1L, (long)real);
            realDelay = TimeSpan.FromTicks(realTicks);
        }

        timer.BaseTimer.Change(realDelay, Timeout.InfiniteTimeSpan);
    }

    private sealed class ClockTimer : ITimer
    {
        private readonly TestClock _clock;
        private readonly TimerCallback _callback;
        private readonly object? _state;
        private readonly ExecutionContext? _context;

        public ClockTimer(TestClock clock, TimerCallback callback, object? state, long sequence)
        {
            _clock = clock;
            _callback = callback;
            _state = state;
            _context = ExecutionContext.Capture();
            Sequence = sequence;
            BaseTimer = clock._base.CreateTimer(static s => ((ClockTimer)s!).OnBaseTimer(), this, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
        }

        public ITimer BaseTimer { get; }

        public long Sequence { get; }

        /// <summary>Monotonic due time in ticks; <see cref="long.MaxValue"/> when disarmed. Guarded by the clock lock.</summary>
        public long DueTicks { get; private set; } = long.MaxValue;

        /// <summary>Period in ticks; 0 for one-shot. Guarded by the clock lock.</summary>
        public long PeriodTicks { get; private set; }

        public bool Disposed { get; private set; }

        public bool Change(TimeSpan dueTime, TimeSpan period)
        {
            ValidateTimerSpan(dueTime, nameof(dueTime));
            ValidateTimerSpan(period, nameof(period));
            lock (_clock._lock)
            {
                if (Disposed)
                    return false;
                long now = _clock.CurrentMonoTicksLocked();
                if (dueTime == Timeout.InfiniteTimeSpan)
                {
                    DueTicks = long.MaxValue;
                    PeriodTicks = 0;
                    _clock._armed.Remove(this);
                }
                else
                {
                    DueTicks = SaturatingAdd(now, dueTime.Ticks);
                    PeriodTicks = period == Timeout.InfiniteTimeSpan || period == TimeSpan.Zero ? 0 : period.Ticks;
                    _clock._armed.Add(this);
                }

                _clock.ScheduleLocked(this, now);
                return true;
            }
        }

        /// <summary>Updates the due time after a firing decision taken at <paramref name="now"/>. Caller holds the clock lock.</summary>
        public void OnFiredLocked(long now)
        {
            if (PeriodTicks > 0)
            {
                long next = SaturatingAdd(DueTicks, PeriodTicks);
                DueTicks = next > now ? next : SaturatingAdd(now, PeriodTicks);
            }
            else
            {
                DueTicks = long.MaxValue;
                _clock._armed.Remove(this);
            }
        }

        public void InvokeCallback()
        {
            if (_context is null)
                _callback(_state);
            else
                ExecutionContext.Run(_context, static s => { var t = (ClockTimer)s!; t._callback(t._state); }, this);
        }

        private void OnBaseTimer()
        {
            bool fire = false;
            lock (_clock._lock)
            {
                if (Disposed || DueTicks == long.MaxValue)
                    return;
                long now = _clock.CurrentMonoTicksLocked();
                if (now >= DueTicks)
                {
                    OnFiredLocked(now);
                    fire = true;
                }

                _clock.ScheduleLocked(this, now);
            }

            if (fire)
                _callback(_state); // the base timer already flows the execution context captured at creation
        }

        public void Dispose()
        {
            lock (_clock._lock)
            {
                if (Disposed)
                    return;
                Disposed = true;
                DueTicks = long.MaxValue;
                _clock._armed.Remove(this);
            }

            BaseTimer.Dispose();
        }

        public ValueTask DisposeAsync()
        {
            Dispose();
            return ValueTask.CompletedTask;
        }

        private static long SaturatingAdd(long a, long b)
        {
            long r = unchecked(a + b);
            return b > 0 && r < a ? long.MaxValue - 1 : r;
        }
    }
}
