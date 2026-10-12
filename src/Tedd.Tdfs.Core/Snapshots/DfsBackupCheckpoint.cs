namespace Tedd.Tdfs;

/// <summary>
/// A snapshot durably published by a backup target. Defined in Core (not Client) because
/// <see cref="DfsBackupNotReachedException"/> and the server's backup exporter both need it.
/// </summary>
/// <param name="Target">Backup target name.</param>
/// <param name="SnapshotId">Published snapshot id.</param>
/// <param name="SnapshotName">Published snapshot name.</param>
/// <param name="CoveredCommit">Commit boundary covered by the snapshot.</param>
/// <param name="RetentionExpiryUtc">Retention expiry on the target, if any.</param>
/// <param name="Held">True when the snapshot is under a retention hold.</param>
/// <param name="IsExactSnapshot">True when the checkpoint is the exact snapshot requested (AwaitBackupSnapshot).</param>
public sealed record DfsBackupCheckpoint(
    string Target,
    SnapshotId SnapshotId,
    string SnapshotName,
    CommitToken CoveredCommit,
    DateTimeOffset? RetentionExpiryUtc,
    bool Held,
    bool IsExactSnapshot);
