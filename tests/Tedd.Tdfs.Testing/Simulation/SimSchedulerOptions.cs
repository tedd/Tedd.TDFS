namespace Tedd.Tdfs.Testing.Simulation;

/// <summary>Configuration of a <see cref="SimScheduler"/>.</summary>
public sealed record SimSchedulerOptions
{
    /// <summary>Interleaving strategy (default <see cref="SimScheduleMode.Random"/>, seeded).</summary>
    public SimScheduleMode Mode { get; init; } = SimScheduleMode.Random;

    /// <summary>Wall-clock reading of the global virtual clock at virtual time zero (fixed, never the real time).</summary>
    public DateTimeOffset StartTimeUtc { get; init; } = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    /// <summary>
    /// Monotonic timestamp (in <see cref="TimeSpan"/> ticks) of the global clock at virtual time zero. Nonzero by default
    /// so code that wrongly assumes timestamps start at zero is exposed.
    /// </summary>
    public long StartTimestamp { get; init; } = TimeSpan.FromDays(1).Ticks;

    /// <summary>
    /// When true (default) a detected uncontrolled dependency (work posted from a foreign thread, HAR-002) makes the
    /// driving <c>Run*</c> call throw <see cref="SimulationViolationException"/>. The violation is always recorded in
    /// <see cref="SimScheduler.Violations"/>; the foreign caller itself is never thrown at (it may be a thread-pool
    /// continuation whose exception would crash the process).
    /// </summary>
    public bool ThrowOnViolation { get; init; } = true;

    /// <summary>
    /// When true (default) an exception escaping a work item of a live process makes the driving <c>Run*</c> call throw
    /// <see cref="SimulationException"/> (after it was recorded and reported). When false it is only recorded in
    /// <see cref="SimScheduler.UnhandledExceptions"/> and raised through the events.
    /// </summary>
    public bool ThrowOnUnhandledException { get; init; } = true;

    /// <summary>
    /// When true (default), once the scheduler has been driven, any interaction from a thread other than the one that
    /// last drove it is a violation even while it is idle. Before the first run any thread may set the simulation up.
    /// </summary>
    public bool StrictThreadAffinity { get; init; } = true;

    /// <summary>Records one trace line per executed work item (in addition to explicit and lifecycle events).</summary>
    public bool RecordSteps { get; init; }

    /// <summary>Maximum number of trace lines kept; recording stops (and <see cref="SimScheduler.TraceTruncated"/> is set) beyond it.</summary>
    public int MaxTraceEntries { get; init; } = 1_000_000;

    /// <summary>
    /// Maximum number of work items executed without virtual time advancing before the run is declared a livelock.
    /// </summary>
    public long MaxStepsPerInstant { get; init; } = 5_000_000;

    /// <summary>
    /// Real time a stalled run (no runnable work, no timers, condition unmet) waits for work posted by foreign threads
    /// before reporting the stall, so uncontrolled dependencies are reported as such instead of as a deadlock.
    /// </summary>
    public TimeSpan StallGracePeriod { get; init; } = TimeSpan.FromMilliseconds(200);

    /// <summary>Virtual-time budget of a <c>Run*</c> call that does not specify one.</summary>
    public TimeSpan DefaultRunTimeout { get; init; } = TimeSpan.FromMinutes(10);

    /// <summary>Optional real (wall-clock) budget of a single <c>Run*</c> call; null means unlimited.</summary>
    public TimeSpan? MaxRealTimePerRun { get; init; }

    /// <summary>Name of the built-in client process whose context <see cref="SimScheduler.RunAsync(Func{Task}, TimeSpan?)"/> uses.</summary>
    public string ClientProcessName { get; init; } = "client";

    /// <summary>Validates the options.</summary>
    /// <exception cref="ArgumentException">A value is out of range.</exception>
    public void Validate()
    {
        if (!Enum.IsDefined(Mode))
            throw new ArgumentException($"Unknown schedule mode {Mode}.", nameof(Mode));
        if (StartTimestamp < 0)
            throw new ArgumentException("StartTimestamp must not be negative.", nameof(StartTimestamp));
        if (MaxTraceEntries < 0)
            throw new ArgumentException("MaxTraceEntries must not be negative.", nameof(MaxTraceEntries));
        if (MaxStepsPerInstant <= 0)
            throw new ArgumentException("MaxStepsPerInstant must be positive.", nameof(MaxStepsPerInstant));
        if (StallGracePeriod < TimeSpan.Zero)
            throw new ArgumentException("StallGracePeriod must not be negative.", nameof(StallGracePeriod));
        if (DefaultRunTimeout <= TimeSpan.Zero)
            throw new ArgumentException("DefaultRunTimeout must be positive.", nameof(DefaultRunTimeout));
        if (MaxRealTimePerRun is { } real && real <= TimeSpan.Zero)
            throw new ArgumentException("MaxRealTimePerRun must be positive when set.", nameof(MaxRealTimePerRun));
        if (string.IsNullOrWhiteSpace(ClientProcessName))
            throw new ArgumentException("ClientProcessName must not be empty.", nameof(ClientProcessName));
    }
}
