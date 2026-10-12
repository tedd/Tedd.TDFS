namespace Tedd.Tdfs;

/// <summary>A read-only view of one snapshot, held by a reader pin until disposed.</summary>
public sealed class DfsSnapshotView : IAsyncDisposable, IDisposable
{
    internal DfsSnapshotView(DfsVolume volume, DfsSnapshotInfo info)
    {
        ArgumentNullException.ThrowIfNull(volume);
        ArgumentNullException.ThrowIfNull(info);
        Volume = volume;
        Info = info;
        Files = new DfsFileClient(volume, isReadOnly: true);
        Directories = new DfsDirectoryClient(volume, isReadOnly: true);
    }

    /// <summary>The volume the snapshot belongs to.</summary>
    internal DfsVolume Volume { get; }

    /// <summary>The snapshot's metadata.</summary>
    public DfsSnapshotInfo Info { get; }

    /// <summary>Read-only file operations on the snapshot; mutators throw <see cref="DfsReadOnlyException"/>.</summary>
    public DfsFileClient Files { get; }

    /// <summary>Read-only directory operations on the snapshot; mutators throw <see cref="DfsReadOnlyException"/>.</summary>
    public DfsDirectoryClient Directories { get; }

    /// <summary>Releases the reader pin. Idempotent.</summary>
    public ValueTask DisposeAsync() =>
        throw new NotImplementedException("DfsSnapshotView.DisposeAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="DisposeAsync"/>.</summary>
    public void Dispose() =>
        throw new NotImplementedException("DfsSnapshotView.Dispose: implemented in a later phase");
}
