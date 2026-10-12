using Tedd.Tdfs.Storage;

namespace Tedd.Tdfs.Server;

/// <summary>Read-only diagnostics of a backup node.</summary>
public sealed partial class BackupInspector
{
    internal BackupInspector()
    {
    }

    /// <summary>Names of the published snapshots of a volume.</summary>
    public Task<IReadOnlyList<string>> GetPublishedSnapshotsAsync(string volume)
        => throw new NotImplementedException("BackupInspector.GetPublishedSnapshotsAsync: implemented in a later phase");

    /// <summary>The snapshot the published pointer of a volume currently designates, or null.</summary>
    public Task<string?> GetPublishedPointerAsync(string volume)
        => throw new NotImplementedException("BackupInspector.GetPublishedPointerAsync: implemented in a later phase");

    /// <summary>Objects stored by the backup node.</summary>
    public Task<IReadOnlyList<ObjectRecordInfo>> ListObjectsAsync()
        => throw new NotImplementedException("BackupInspector.ListObjectsAsync: implemented in a later phase");

    /// <summary>Bytes received but not yet part of a published snapshot.</summary>
    public Task<long> GetStagedBytesAsync()
        => throw new NotImplementedException("BackupInspector.GetStagedBytesAsync: implemented in a later phase");
}
