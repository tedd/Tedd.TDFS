namespace Tedd.Tdfs;

/// <summary>State of a backup target's synchronisation.</summary>
public enum DfsBackupState
{
    /// <summary>Up to date or waiting for the next scheduled sync.</summary>
    Idle,

    /// <summary>Transferring objects.</summary>
    Transferring,

    /// <summary>The target is verifying the dependency closure.</summary>
    Verifying,

    /// <summary>The target is publishing the snapshot.</summary>
    Publishing,

    /// <summary>The target is unreachable.</summary>
    Offline,

    /// <summary>The transfer lineage was abandoned; the next sync is a full reseed.</summary>
    ReseedRequired,

    /// <summary>Synchronisation failed; see <see cref="DfsBackupStatus.LastError"/>.</summary>
    Failed,
}
