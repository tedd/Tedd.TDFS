using Tedd.Tdfs.Client.Internal;
using Tedd.Tdfs.Client.Internal.Runtime;
using Tedd.Tdfs.Client.Internal.Sessions;

namespace Tedd.Tdfs;

/// <summary>
/// A read-only view of a snapshot (FSD §11.4): paths resolve inside the snapshot, reads return its bytes. The view holds
/// a reader pin (renewed while open) until it is disposed.
/// </summary>
public sealed class DfsSnapshotView : IAsyncDisposable, IDisposable, IPinHolder
{
    private int _disposed;
    private Exception? _expired;

    internal DfsSnapshotView(DfsVolume volume, DfsSnapshotInfo info, ulong pin)
    {
        ArgumentNullException.ThrowIfNull(volume);
        ArgumentNullException.ThrowIfNull(info);
        Volume = volume;
        Info = info;
        Pin = pin;
        Files = new DfsFileClient(volume, isReadOnly: true, transaction: null, view: this);
        Directories = new DfsDirectoryClient(volume, isReadOnly: true, transaction: null, view: this);
    }

    /// <summary>The owning volume facade.</summary>
    internal DfsVolume Volume { get; }

    /// <summary>The reader pin holding the snapshot.</summary>
    internal ulong Pin { get; }

    /// <summary>The snapshot's metadata.</summary>
    public DfsSnapshotInfo Info { get; }

    /// <summary>Read-only file operations inside the snapshot.</summary>
    public DfsFileClient Files { get; }

    /// <summary>Read-only directory operations inside the snapshot.</summary>
    public DfsDirectoryClient Directories { get; }

    /// <summary>The expiry of the reader pin, once the leader reported it (reads then fail with <see cref="DfsPinExpiredException"/>).</summary>
    internal Exception? Expired => Volatile.Read(ref _expired);

    void IPinHolder.OnPinExpired(Exception reason) => Interlocked.CompareExchange(ref _expired, reason, null);

    /// <summary>Releases the reader pin (idempotent).</summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;
        Volume.Context.UnregisterPin(Pin);
        ClientRuntime runtime = Volume.Runtime;
        if (runtime.IsDisposed)
            return;
        try
        {
            using OperationScope scope = runtime.BeginOperation(DfsOperationKind.OpenSnapshot, Volume.Name, null, CancellationToken.None);
            await DfsDirectoryClient.CloseSnapshotPinAsync(Volume.Context, Pin, scope).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            // An unreleased pin expires on its own once it is no longer renewed.
            runtime.Emit("snapshot.close-failed", $"Releasing snapshot pin {Pin} failed ({ex.GetType().Name}).", volume: Volume.Name, exception: ex);
        }
    }

    /// <summary>Synchronous counterpart of <see cref="DisposeAsync"/>.</summary>
    public void Dispose() => SyncBridge.RunValueTask(DisposeAsync);
}
