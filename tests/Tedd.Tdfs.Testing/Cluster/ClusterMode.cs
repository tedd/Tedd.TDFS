namespace Tedd.Tdfs.Testing;

/// <summary>How a test cluster executes.</summary>
public enum ClusterMode
{
    /// <summary>Real threads, real time (per-node <see cref="TestClock"/> over the system clock).</summary>
    Realtime,

    /// <summary>Single-threaded deterministic simulation with virtual time; replays exactly from a seed.</summary>
    Deterministic,
}
