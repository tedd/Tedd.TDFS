namespace Tedd.Tdfs;

/// <summary>Progress of a backup target (received, durable and published boundaries are distinct — BAK-016).</summary>
/// <param name="Target">Target name.</param>
/// <param name="State">Synchronisation state.</param>
/// <param name="ReceivedBytes">Bytes received by the target in the current transfer.</param>
/// <param name="DurableBoundary">Boundary durably stored on the target (not necessarily published).</param>
/// <param name="PublishedBoundary">Boundary of the latest published snapshot.</param>
/// <param name="LatestPublished">Latest published checkpoint, if any.</param>
/// <param name="RecoveryPointAge">Age of the latest published recovery point, if any.</param>
/// <param name="ReseedRequired">True when the next sync must be a full reseed.</param>
/// <param name="PinnedSourceBytes">Source bytes pinned for this target.</param>
/// <param name="LastError">Last synchronisation error, if any.</param>
/// <param name="LineageId">Current transfer lineage id.</param>
public sealed record DfsBackupStatus(
    string Target,
    DfsBackupState State,
    long ReceivedBytes,
    CommitToken DurableBoundary,
    CommitToken PublishedBoundary,
    DfsBackupCheckpoint? LatestPublished,
    TimeSpan? RecoveryPointAge,
    bool ReseedRequired,
    long PinnedSourceBytes,
    string? LastError,
    Guid LineageId);
