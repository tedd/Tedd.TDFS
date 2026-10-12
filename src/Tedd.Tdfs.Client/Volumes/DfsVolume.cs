using Tedd.Tdfs.Client.Internal;

namespace Tedd.Tdfs;

/// <summary>A volume of the connected cluster: entry point to its file, directory, transaction, snapshot and backup facades.</summary>
public sealed class DfsVolume
{
    internal DfsVolume(DfsClient client, string name, bool isReadOnly = false)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(name);
        Client = client;
        Name = name;
        IsReadOnly = isReadOnly;
        Files = new DfsFileClient(this, isReadOnly);
        Directories = new DfsDirectoryClient(this, isReadOnly);
        Transactions = new DfsTransactionClient(this);
        Snapshots = new DfsSnapshotClient(this, isReadOnly);
        Backups = new DfsBackupClient(this);
    }

    /// <summary>True when the volume is served read-only (backup hosts).</summary>
    internal bool IsReadOnly { get; }

    /// <summary>Volume name.</summary>
    public string Name { get; }

    /// <summary>The owning client.</summary>
    public DfsClient Client { get; }

    /// <summary>File operations.</summary>
    public DfsFileClient Files { get; }

    /// <summary>Directory operations.</summary>
    public DfsDirectoryClient Directories { get; }

    /// <summary>Transactions.</summary>
    public DfsTransactionClient Transactions { get; }

    /// <summary>Snapshots.</summary>
    public DfsSnapshotClient Snapshots { get; }

    /// <summary>Late-sync backup targets.</summary>
    public DfsBackupClient Backups { get; }

    /// <summary>Reads the volume's metadata.</summary>
    public Task<DfsVolumeInfo> GetInfoAsync(CancellationToken ct = default) =>
        throw new NotImplementedException("DfsVolume.GetInfoAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="GetInfoAsync"/>.</summary>
    public DfsVolumeInfo GetInfo() =>
        SyncBridge.Run(() => GetInfoAsync(CancellationToken.None));

    /// <summary>Reserves an operation id in this client's session, for persisting before use (FSD §13).</summary>
    public Task<DfsOperationId> NewOperationIdAsync(CancellationToken ct = default) =>
        throw new NotImplementedException("DfsVolume.NewOperationIdAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="NewOperationIdAsync"/>.</summary>
    public DfsOperationId NewOperationId() =>
        SyncBridge.Run(() => NewOperationIdAsync(CancellationToken.None));

    /// <summary>
    /// Queries the outcome of an operation. With <paramref name="resolve"/> the volume fences an unfinished operation so
    /// the answer is definite (Committed or Aborted).
    /// </summary>
    public Task<DfsOperationOutcome> QueryOperationAsync(DfsOperationId id, bool resolve = false, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsVolume.QueryOperationAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="QueryOperationAsync"/>.</summary>
    public DfsOperationOutcome QueryOperation(DfsOperationId id, bool resolve = false) =>
        SyncBridge.Run(() => QueryOperationAsync(id, resolve, CancellationToken.None));
}
