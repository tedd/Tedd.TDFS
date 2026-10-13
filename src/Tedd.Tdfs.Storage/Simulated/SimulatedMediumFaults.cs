namespace Tedd.Tdfs.Storage;

/// <summary>
/// Fault switches of a <see cref="SimulatedMedium"/>. Counters are consumed one per matching operation.
/// <see cref="PathFilter"/> restricts every fault except <see cref="CapacityOverride"/> (which is medium-wide) to the
/// matching medium-relative paths. Safe to change from a test thread while the node is running.
/// </summary>
public sealed class SimulatedMediumFaults
{
    private volatile bool _readOnly;
    private volatile bool _dishonestFlush;
    private long _capacityOverride = -1;
    private int _failNextFlushes;
    private int _failNextWrites;
    private int _shortNextWrites;
    private int _failNextReads;
    private long _ioDelayTicks = -1;
    private volatile Func<string, bool>? _pathFilter;

    /// <summary>When true every modification (write, length change, create, rename, delete, flush) fails with a read-only <see cref="IOException"/>; reads still work.</summary>
    public bool ReadOnly
    {
        get => _readOnly;
        set => _readOnly = value;
    }

    /// <summary>Overrides the medium capacity; writes that would exceed it fail with a disk-full <see cref="IOException"/> (ENOSPC).</summary>
    public long? CapacityOverride
    {
        get
        {
            long v = Interlocked.Read(ref _capacityOverride);
            return v < 0 ? null : v;
        }
        set
        {
            if (value is < 0)
                throw new ArgumentOutOfRangeException(nameof(value), "Capacity must not be negative.");
            Interlocked.Exchange(ref _capacityOverride, value ?? -1);
        }
    }

    /// <summary>The next N file or directory flushes fail with <see cref="IOException"/>; nothing becomes durable.</summary>
    public int FailNextFlushes
    {
        get => Volatile.Read(ref _failNextFlushes);
        set => Volatile.Write(ref _failNextFlushes, Math.Max(0, value));
    }

    /// <summary>The next N writes fail with <see cref="IOException"/> (EIO) without effect.</summary>
    public int FailNextWrites
    {
        get => Volatile.Read(ref _failNextWrites);
        set => Volatile.Write(ref _failNextWrites, Math.Max(0, value));
    }

    /// <summary>The next N writes are short: only the first half (rounded up) of the bytes is written and reported.</summary>
    public int ShortNextWrites
    {
        get => Volatile.Read(ref _shortNextWrites);
        set => Volatile.Write(ref _shortNextWrites, Math.Max(0, value));
    }

    /// <summary>The next N reads fail with <see cref="IOException"/>.</summary>
    public int FailNextReads
    {
        get => Volatile.Read(ref _failNextReads);
        set => Volatile.Write(ref _failNextReads, Math.Max(0, value));
    }

    /// <summary>Delay added to every read, write and flush, measured with the medium's <see cref="TimeProvider"/>.</summary>
    public TimeSpan? IoDelay
    {
        get
        {
            long v = Interlocked.Read(ref _ioDelayTicks);
            return v < 0 ? null : TimeSpan.FromTicks(v);
        }
        set
        {
            if (value is { } d && d < TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(value), "Delay must not be negative.");
            Interlocked.Exchange(ref _ioDelayTicks, value?.Ticks ?? -1);
        }
    }

    /// <summary>
    /// Out-of-model storage (HAR-018): while true, every file flush and directory flush reports success immediately but leaves
    /// the affected writes and namespace changes volatile, so a later crash loses them exactly as if they had never been
    /// flushed. Turning it off persists nothing retroactively; only later flushes are honest again. Not restricted by
    /// <see cref="PathFilter"/> (the whole device lies). Successful dishonest flushes are not counted as durability barriers.
    /// </summary>
    public bool DishonestFlush
    {
        get => _dishonestFlush;
        set => _dishonestFlush = value;
    }

    /// <summary>Restricts faults to paths for which the predicate returns true (null = all paths).</summary>
    public Func<string, bool>? PathFilter
    {
        get => _pathFilter;
        set => _pathFilter = value;
    }

    /// <summary>Clears every fault.</summary>
    public void Reset()
    {
        ReadOnly = false;
        DishonestFlush = false;
        CapacityOverride = null;
        FailNextFlushes = 0;
        FailNextWrites = 0;
        ShortNextWrites = 0;
        FailNextReads = 0;
        IoDelay = null;
        PathFilter = null;
    }

    internal bool Matches(string path)
    {
        Func<string, bool>? filter = _pathFilter;
        return filter is null || filter(path);
    }

    internal bool IsReadOnlyFor(string path) => _readOnly && Matches(path);

    internal TimeSpan? DelayFor(string path) => IoDelay is { } d && d > TimeSpan.Zero && Matches(path) ? d : null;

    internal bool TryConsumeFailFlush(string path) => Matches(path) && TryConsume(ref _failNextFlushes);

    internal bool TryConsumeFailWrite(string path) => Matches(path) && TryConsume(ref _failNextWrites);

    internal bool TryConsumeShortWrite(string path) => Matches(path) && TryConsume(ref _shortNextWrites);

    internal bool TryConsumeFailRead(string path) => Matches(path) && TryConsume(ref _failNextReads);

    private static bool TryConsume(ref int counter)
    {
        while (true)
        {
            int current = Volatile.Read(ref counter);
            if (current <= 0)
                return false;
            if (Interlocked.CompareExchange(ref counter, current - 1, current) == current)
                return true;
        }
    }
}
