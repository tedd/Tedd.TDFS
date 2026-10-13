using Tedd.Tdfs.Server.Backup;
using Tedd.Tdfs.Server.Runtime;
using Tedd.Tdfs.Storage;

namespace Tedd.Tdfs.Server;

/// <summary>Read-only diagnostics of a backup node.</summary>
/// <remarks>Every member runs on the backup node's loop and never changes retained, published or staged state.</remarks>
public sealed partial class BackupInspector
{
    private readonly INodeLoop _loop;
    private readonly BackupRuntime _runtime;

    internal BackupInspector(INodeLoop loop, BackupRuntime runtime)
    {
        _loop = loop;
        _runtime = runtime;
    }

    private BackupServices S => _runtime.Services;

    /// <summary>Names of the published snapshots of a volume.</summary>
    public Task<IReadOnlyList<string>> GetPublishedSnapshotsAsync(string volume)
    {
        ArgumentNullException.ThrowIfNull(volume);
        return _loop.InvokeAsync<IReadOnlyList<string>>(() =>
            S.Catalog.Find(volume) is { } v ? v.Visible.Select(s => s.Name).ToArray() : []);
    }

    /// <summary>The snapshot the published pointer of a volume currently designates, or null.</summary>
    public Task<string?> GetPublishedPointerAsync(string volume)
    {
        ArgumentNullException.ThrowIfNull(volume);
        return _loop.InvokeAsync(() => S.Catalog.Find(volume)?.Pointer?.Name);
    }

    /// <summary>Objects stored by the backup node.</summary>
    public Task<IReadOnlyList<ObjectRecordInfo>> ListObjectsAsync()
        => _loop.InvokeAsync<IReadOnlyList<ObjectRecordInfo>>(() => S.Store is { } store ? store.List().ToArray() : []);

    /// <summary>Bytes received but not yet part of a published snapshot.</summary>
    public Task<long> GetStagedBytesAsync() => _loop.InvokeAsync(() => S.Catalog.StagedBytes());

    /// <summary>Where the store keeps an object's record and its plaintext length (corruption tests), or null when not held.</summary>
    internal Task<(ObjectPhysicalLocation Location, long PlaintextLength)?> LocateObjectAsync(ObjectId id) => _loop.InvokeAsync(() =>
    {
        if (S.Store is not { } store || store.TryGetInfo(id) is not { } info || store.GetPhysicalLocation(id) is not { } location)
            return ((ObjectPhysicalLocation, long)?)null;
        return (location, info.Length);
    });

    /// <summary>Deletes the local copy of an object without telling anyone (silent loss); false when it was not held.</summary>
    internal Task<bool> DeleteObjectLocallyAsync(ObjectId id) => _loop.InvokeAsync(async () =>
    {
        if (S.Store is not { } store || store.TryGetInfo(id) is null)
            return false;
        await store.DeleteAsync(id);
        return true;
    });
}
