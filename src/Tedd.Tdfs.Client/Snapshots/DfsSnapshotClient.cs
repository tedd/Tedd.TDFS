using Tedd.Tdfs.Client.Internal;

namespace Tedd.Tdfs;

/// <summary>Volume snapshots (FSD §11).</summary>
public sealed class DfsSnapshotClient
{
    internal DfsSnapshotClient(DfsVolume volume, bool isReadOnly = false)
    {
        ArgumentNullException.ThrowIfNull(volume);
        Volume = volume;
        IsReadOnly = isReadOnly;
    }

    /// <summary>The volume this facade operates on.</summary>
    internal DfsVolume Volume { get; }

    /// <summary>True on backup hosts: create, delete, retention changes and restore are rejected.</summary>
    public bool IsReadOnly { get; }

    /// <summary>Creates a snapshot (running adapters, honouring <see cref="DfsSnapshotOptions.MinimumCommit"/>).</summary>
    public Task<DfsSnapshotInfo> CreateAsync(string name, DfsSnapshotOptions? options = null, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsSnapshotClient.CreateAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="CreateAsync"/>.</summary>
    public DfsSnapshotInfo Create(string name, DfsSnapshotOptions? options = null) =>
        SyncBridge.Run(() => CreateAsync(name, options, CancellationToken.None));

    /// <summary>Lists the volume's snapshots.</summary>
    public Task<IReadOnlyList<DfsSnapshotInfo>> ListAsync(CancellationToken ct = default) =>
        throw new NotImplementedException("DfsSnapshotClient.ListAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="ListAsync"/>.</summary>
    public IReadOnlyList<DfsSnapshotInfo> List() =>
        SyncBridge.Run(() => ListAsync(CancellationToken.None));

    /// <summary>Reads one snapshot's metadata; <see cref="DfsException"/> with <see cref="DfsErrorCode.SnapshotNotFound"/> when absent.</summary>
    public Task<DfsSnapshotInfo> GetAsync(string name, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsSnapshotClient.GetAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="GetAsync"/>.</summary>
    public DfsSnapshotInfo Get(string name) =>
        SyncBridge.Run(() => GetAsync(name, CancellationToken.None));

    /// <summary>Opens a read-only view of the snapshot under a reader pin (released when the view is disposed).</summary>
    public Task<DfsSnapshotView> OpenAsync(string name, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsSnapshotClient.OpenAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="OpenAsync"/>.</summary>
    public DfsSnapshotView Open(string name) =>
        SyncBridge.Run(() => OpenAsync(name, CancellationToken.None));

    /// <summary>Deletes the snapshot subject to its retention (removes the retention reference only).</summary>
    public Task DeleteAsync(string name, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsSnapshotClient.DeleteAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="DeleteAsync"/>.</summary>
    public void Delete(string name) =>
        SyncBridge.Run(() => DeleteAsync(name, CancellationToken.None));

    /// <summary>Changes the snapshot's retention.</summary>
    public Task SetRetentionAsync(string name, DfsRetention retention, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsSnapshotClient.SetRetentionAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="SetRetentionAsync"/>.</summary>
    public void SetRetention(string name, DfsRetention retention) =>
        SyncBridge.Run(() => SetRetentionAsync(name, retention, CancellationToken.None));

    /// <summary>Restores the snapshot into a new volume (new id, epoch 1).</summary>
    public Task<DfsVolumeInfo> RestoreAsync(string name, string newVolumeName, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsSnapshotClient.RestoreAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="RestoreAsync"/>.</summary>
    public DfsVolumeInfo Restore(string name, string newVolumeName) =>
        SyncBridge.Run(() => RestoreAsync(name, newVolumeName, CancellationToken.None));
}
