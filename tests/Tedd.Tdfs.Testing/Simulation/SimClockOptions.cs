namespace Tedd.Tdfs.Testing.Simulation;

/// <summary>Initial configuration of a per-node clock created by <see cref="SimScheduler.CreateClock"/>.</summary>
public sealed record SimClockOptions
{
    /// <summary>Default options: no offsets, rate 1, bound to the process of the same name.</summary>
    public static SimClockOptions Default { get; } = new();

    /// <summary>Initial wall-clock offset relative to the global virtual wall clock (may be negative).</summary>
    public TimeSpan WallOffset { get; init; }

    /// <summary>Offset of the clock's monotonic timestamp origin relative to the global timestamp (may be negative).</summary>
    public TimeSpan MonotonicOffset { get; init; }

    /// <summary>Initial drift rate relative to global virtual time (1.0 = exact).</summary>
    public double Rate { get; init; } = 1.0;

    /// <summary>
    /// When true (default) timers belong to the live <see cref="SimProcess"/> named like the clock: they are held while it
    /// is paused and die with it. When false they belong to whichever process creates them.
    /// </summary>
    public bool BindToProcess { get; init; } = true;

    /// <summary>Validates the options.</summary>
    /// <exception cref="ArgumentException">The rate is not a finite positive number.</exception>
    public void Validate()
    {
        if (!double.IsFinite(Rate) || Rate <= 0)
            throw new ArgumentException("Rate must be a finite positive number.", nameof(Rate));
    }
}
