namespace Tedd.Tdfs.Testing.Simulation;

/// <summary>Kind of uncontrolled dependency.</summary>
public enum SimViolationKind
{
    /// <summary>The simulation was used from a thread other than the one driving it (thread pool, real timer, I/O, ...).</summary>
    ForeignThread,

    /// <summary>Reported explicitly through <see cref="SimScheduler.ReportViolation"/> by another detector.</summary>
    Reported,
}

/// <summary>One detected uncontrolled dependency (HAR-002).</summary>
/// <param name="Kind">Kind of violation.</param>
/// <param name="Operation">The simulation operation that was attempted (for example "post work").</param>
/// <param name="Description">Human-readable details.</param>
/// <param name="ThreadId">Managed id of the offending thread.</param>
/// <param name="ThreadName">Name of the offending thread, if any.</param>
/// <param name="IsThreadPoolThread">True when the offending thread belongs to the thread pool.</param>
/// <param name="VirtualTime">Virtual time when it was detected.</param>
/// <param name="StackTrace">Stack trace of the offending call.</param>
public sealed record SimViolation(
    SimViolationKind Kind,
    string Operation,
    string Description,
    int ThreadId,
    string? ThreadName,
    bool IsThreadPoolThread,
    TimeSpan VirtualTime,
    string? StackTrace)
{
    /// <summary>
    /// Name of the simulated process the interaction addressed (for example the node whose loop received a foreign post,
    /// or the client process running the scenario), or null when it addressed the harness or the simulation as a whole.
    /// </summary>
    public string? Target { get; init; }

    /// <summary>One-line summary (without the stack trace).</summary>
    public override string ToString() =>
        $"{Kind} at t={SimScheduler.FormatTime(VirtualTime)}: {Operation} from thread {ThreadId}" +
        $"{(ThreadName is null ? string.Empty : $" '{ThreadName}'")}{(IsThreadPoolThread ? " (thread pool)" : string.Empty)}: {Description}";
}
