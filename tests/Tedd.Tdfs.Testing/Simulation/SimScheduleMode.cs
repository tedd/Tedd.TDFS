namespace Tedd.Tdfs.Testing.Simulation;

/// <summary>How <see cref="SimScheduler"/> picks the next work item among runnable lanes at one virtual instant.</summary>
public enum SimScheduleMode
{
    /// <summary>
    /// Seeded uniform choice among runnable lanes (each lane stays FIFO). Different seeds explore different
    /// interleavings; the same seed always replays the same interleaving.
    /// </summary>
    Random,

    /// <summary>Global FIFO: the oldest queued item (across all runnable lanes) runs first.</summary>
    Fifo,
}
