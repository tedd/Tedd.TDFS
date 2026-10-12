namespace Tedd.Tdfs.Testing.Simulation;

/// <summary>Lifecycle state of a <see cref="SimProcess"/>.</summary>
public enum SimProcessState
{
    /// <summary>Its work items are eligible to run.</summary>
    Running,

    /// <summary>Frozen: its work items (loop work, timer callbacks, network deliveries) are held, nothing is lost.</summary>
    Paused,

    /// <summary>Dead: its queued work was discarded and later work is dropped. A process never comes back to life.</summary>
    Killed,
}
