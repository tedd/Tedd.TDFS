namespace Tedd.Tdfs;

/// <summary>Consistency level recorded for a snapshot.</summary>
public enum DfsConsistencyLabel
{
    /// <summary>Crash-consistent filesystem state at the snapshot boundary.</summary>
    FilesystemConsistent,

    /// <summary>Every declared adapter prepared successfully before capture.</summary>
    ApplicationConsistent,
}
