namespace Tedd.Tdfs;

/// <summary>Request to restore a published backup snapshot into a new live volume.</summary>
/// <param name="BackupEndpoints">Backup node endpoints.</param>
/// <param name="BackupCredentials">Credentials for reading from the backup node.</param>
/// <param name="SourceVolume">Volume name on the backup node.</param>
/// <param name="SnapshotName">Published snapshot to restore.</param>
/// <param name="NewVolumeName">Name of the new live volume.</param>
/// <param name="RecoveryKey">Exported recovery key, when the cluster key material is not otherwise available.</param>
public sealed record DfsRestoreFromBackupRequest(
    IReadOnlyList<string> BackupEndpoints,
    DfsCredentials BackupCredentials,
    string SourceVolume,
    string SnapshotName,
    string NewVolumeName,
    byte[]? RecoveryKey = null);
