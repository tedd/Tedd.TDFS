namespace Tedd.Tdfs.Storage;

/// <summary>
/// Packed, append-only object store: objects are records in segment files with a self-describing, checksummed header;
/// the index is rebuilt from the segments on open. Writes are durable when <see cref="PutAsync"/> returns.
/// </summary>
public sealed class ObjectStore : IAsyncDisposable
{
    private ObjectStore()
    {
    }

    /// <summary>Total bytes of live (referenced by the index) records.</summary>
    public long LiveBytes => throw new NotImplementedException("ObjectStore.LiveBytes: implemented in a later phase");

    /// <summary>Total bytes of all segment files, including dead records awaiting compaction.</summary>
    public long TotalBytes => throw new NotImplementedException("ObjectStore.TotalBytes: implemented in a later phase");

    /// <summary>Opens (or creates) the store in <paramref name="directory"/>, rebuilding the index from the segments.</summary>
    public static ValueTask<ObjectStore> OpenAsync(IStorageMedium medium, string directory, ObjectStoreOptions options, IFaultInjector faults, CancellationToken ct = default)
        => throw new NotImplementedException("ObjectStore.OpenAsync: implemented in a later phase");

    /// <summary>Stores an object durably (flush barrier before return). Idempotent for identical content; conflicting content for the same id throws <see cref="DfsIntegrityException"/>.</summary>
    public ValueTask<ObjectRecordInfo> PutAsync(ObjectId id, VolumeId volume, StagingId staging, ReadOnlyMemory<byte> plaintext, CancellationToken ct = default)
        => throw new NotImplementedException("ObjectStore.PutAsync: implemented in a later phase");

    /// <summary>Reads verified plaintext bytes; corruption or a missing object throws <see cref="DfsIntegrityException"/>.</summary>
    public ValueTask<int> ReadAsync(ObjectId id, long offset, Memory<byte> destination, CancellationToken ct = default)
        => throw new NotImplementedException("ObjectStore.ReadAsync: implemented in a later phase");

    /// <summary>True when the store holds a record for <paramref name="id"/>.</summary>
    public ValueTask<bool> ContainsAsync(ObjectId id, CancellationToken ct = default)
        => throw new NotImplementedException("ObjectStore.ContainsAsync: implemented in a later phase");

    /// <summary>Deletes an object durably (tombstone).</summary>
    public ValueTask DeleteAsync(ObjectId id, CancellationToken ct = default)
        => throw new NotImplementedException("ObjectStore.DeleteAsync: implemented in a later phase");

    /// <summary>Lists the index, optionally for one volume.</summary>
    public IReadOnlyList<ObjectRecordInfo> List(VolumeId? volume = null)
        => throw new NotImplementedException("ObjectStore.List: implemented in a later phase");

    /// <summary>Verifies up to <paramref name="maxObjects"/> stored objects and quarantines corrupt ones.</summary>
    public ValueTask<ScrubResult> ScrubAsync(int maxObjects, CancellationToken ct = default)
        => throw new NotImplementedException("ObjectStore.ScrubAsync: implemented in a later phase");

    /// <summary>Copies live records out of sparse segments and deletes the old segments (crash-safe).</summary>
    public ValueTask CompactAsync(CancellationToken ct = default)
        => throw new NotImplementedException("ObjectStore.CompactAsync: implemented in a later phase");

    /// <summary>Closes the segment files.</summary>
    public ValueTask DisposeAsync()
        => throw new NotImplementedException("ObjectStore.DisposeAsync: implemented in a later phase");
}
