namespace Tedd.Tdfs;

/// <summary>Per-volume grants (and the cluster-level <see cref="ClusterAdmin"/> flag).</summary>
[Flags]
public enum DfsPermissions
{
    /// <summary>No permissions.</summary>
    None = 0,
    /// <summary>Read files, directories and metadata.</summary>
    Read = 1,
    /// <summary>Mutate files and directories.</summary>
    Write = 2,
    /// <summary>Create and delete snapshots.</summary>
    Snapshot = 4,
    /// <summary>Export snapshots to backup targets.</summary>
    BackupExport = 8,
    /// <summary>Manage backup targets.</summary>
    BackupAdmin = 16,
    /// <summary>Manage retention (including protected history).</summary>
    RetentionAdmin = 32,
    /// <summary>Administer the volume (policies, grants, keys).</summary>
    VolumeAdmin = 64,
    /// <summary>Cluster administration.</summary>
    ClusterAdmin = 128,
    /// <summary>Every permission.</summary>
    All = 255,
}
