namespace Tedd.Tdfs.Testing;

/// <summary>Storage medium used by test nodes.</summary>
public enum StorageKind
{
    /// <summary>Crash-faithful in-memory medium (<see cref="Storage.SimulatedMedium"/>).</summary>
    Simulated,

    /// <summary>Real files under the test data root.</summary>
    Disk,
}
