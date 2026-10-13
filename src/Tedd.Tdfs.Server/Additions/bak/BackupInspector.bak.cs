using Tedd.Tdfs.Data;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Backup;
using Tedd.Tdfs.Storage;

namespace Tedd.Tdfs.Server;

// Test-visible additions requested by the BAK suite (docs/api-additions/bak.md). All members are read-only
// diagnostics executed on the backup node loop; none of them may change replicated, retained or published state.
public sealed partial class BackupInspector
{
    /// <summary>
    /// Returns every snapshot this backup node currently retains for <paramref name="volume"/> (source volume name),
    /// across all lineages and epochs, ordered by (lineage start, covered commit). Only completely verified,
    /// durably persisted snapshots that were published at least once are listed; staged/in-progress transfers are
    /// reported by <see cref="GetReceiveStateAsync"/> instead. The <see cref="BackupSnapshotInspection.Closure"/> is
    /// the snapshot's complete object dependency closure as recorded in its persisted manifest (sorted, distinct).
    /// Returns an empty list when the volume is unknown. Used by BAK-005, BAK-009, BAK-020, BAK-021, BAK-024,
    /// BAK-026, BAK-027.
    /// </summary>
    public Task<IReadOnlyList<BackupSnapshotInspection>> GetSnapshotsAsync(string volume)
    {
        ArgumentNullException.ThrowIfNull(volume);
        return _loop.InvokeAsync<IReadOnlyList<BackupSnapshotInspection>>(() =>
        {
            if (S.Catalog.Find(volume) is not { } v)
                return [];
            BackupSnapshot? pointer = v.Pointer;
            return v.Snapshots
                .Select(s => new BackupSnapshotInspection(s.Name, s.Id, s.Covered, s.Lineage, ReferenceEquals(s, pointer), s.Retention,
                    s.Closure.Distinct().OrderBy(o => o.Value).ToArray(), s.ReaderPins))
                .ToArray();
        });
    }

    /// <summary>
    /// Returns the state of the in-progress (not yet published) incoming transfer for <paramref name="volume"/>, or
    /// <c>null</c> when no transfer is staged. Objects are counted as <em>received</em> once their bytes arrived and as
    /// <em>durable</em> only after the destination flush barrier completed and the object verified; the
    /// <see cref="BackupReceiveInspection.MissingObjects"/> set is computed by the node itself from the received
    /// manifest (never from inventory hints) and is empty only when the full closure is durable. Used by BAK-004,
    /// BAK-006, BAK-007, BAK-009, BAK-016.
    /// </summary>
    public Task<BackupReceiveInspection?> GetReceiveStateAsync(string volume)
    {
        ArgumentNullException.ThrowIfNull(volume);
        return _loop.InvokeAsync(() =>
        {
            if (S.Catalog.Find(volume)?.Staged is not { } staged)
                return null;
            var missing = new HashSet<ObjectId>();
            if (staged.Manifest is { } manifest)
            {
                foreach (BackupObjectRef obj in manifest.Closure)
                {
                    if (!staged.Durable.Contains(obj.Id) || S.Store?.TryGetInfo(obj.Id) is not { } info || info.Length != obj.Length
                        || info.Checksum != obj.Checksum)
                    {
                        missing.Add(obj.Id);
                    }
                }
            }

            return new BackupReceiveInspection(staged.SnapshotName, staged.Key.LineageId, staged.Key.Epoch, staged.Manifest is not null,
                new HashSet<ObjectId>(staged.Received), new HashSet<ObjectId>(staged.Durable), missing, staged.ReceivedBytes);
        });
    }

    /// <summary>
    /// Returns, for every object stored on this backup node, the number of retained snapshots (all volumes and
    /// lineages, published or staged-manifest) whose dependency closure references it, as the node's retention/GC
    /// logic accounts it (tracing implementations report the traced count). An object that is stored but referenced
    /// by nothing reports 0. Duplicate deliveries of the same immutable object must never change the count. Used by
    /// BAK-009, BAK-027.
    /// </summary>
    public Task<IReadOnlyDictionary<ObjectId, int>> GetObjectReferenceCountsAsync() => _loop.InvokeAsync(() => S.Catalog.ReferenceCounts());

    /// <summary>
    /// Independently re-reads and verifies (identity, length, encoding and checksum) every object in the dependency
    /// closure of the retained snapshot <paramref name="snapshotName"/> of <paramref name="volume"/> from the durable
    /// store, and returns the ids that are missing or fail verification (empty when the closure is complete). Throws
    /// <see cref="DfsException"/> with <see cref="DfsErrorCode.SnapshotNotFound"/> when no such snapshot is retained.
    /// Used by BAK-006, BAK-008, BAK-010, BAK-012, BAK-020, BAK-024.
    /// </summary>
    public Task<IReadOnlyList<ObjectId>> VerifyClosureAsync(string volume, string snapshotName)
    {
        ArgumentNullException.ThrowIfNull(volume);
        ArgumentNullException.ThrowIfNull(snapshotName);
        return _loop.InvokeAsync<IReadOnlyList<ObjectId>>(async () =>
        {
            BackupSnapshot snapshot = S.Catalog.Find(volume)?.Snapshots.LastOrDefault(s => string.Equals(s.Name, snapshotName, StringComparison.Ordinal))
                ?? throw new DfsException(DfsErrorCode.SnapshotNotFound, $"Snapshot '{snapshotName}' of volume '{volume}' is not retained on {S.Name}.");
            BackupManifest manifest = snapshot.Manifest
                ?? throw new DfsRecoveryKeyMissingException($"The manifest of snapshot '{snapshotName}' cannot be decrypted without the recovery key.");
            ObjectStore store = S.RequireStore("verifying a closure");
            var failed = new List<ObjectId>();
            foreach (BackupObjectRef obj in manifest.Closure)
            {
                if (!await VerifyObjectAsync(store, obj))
                    failed.Add(obj.Id);
            }

            return failed;
        });
    }

    private static async Task<bool> VerifyObjectAsync(ObjectStore store, BackupObjectRef obj)
    {
        if (store.TryGetInfo(obj.Id) is not { } info || info.Length != obj.Length || info.Checksum != obj.Checksum || obj.Encoding != ObjectEncodings.Plain)
            return false;
        try
        {
            var data = new byte[info.Length];
            int read = data.Length == 0 ? 0 : await store.ReadAsync(obj.Id, 0, data, CancellationToken.None);
            return read == data.Length && ObjectChecksum.Compute(data) == obj.Checksum;
        }
        catch (Exception ex) when (ex is DfsException or IOException or ObjectDisposedException)
        {
            return false;
        }
    }
}

/// <summary>A snapshot retained by a backup node (BAK additions).</summary>
/// <param name="Name">Snapshot name (as created on the source).</param>
/// <param name="Id">Source snapshot id.</param>
/// <param name="CoveredCommit">Source commit boundary of the snapshot (cluster, volume id, epoch, index).</param>
/// <param name="Lineage">Transfer lineage id the snapshot was received under.</param>
/// <param name="IsPublishedPointer">True when this snapshot is the current published pointer of the volume.</param>
/// <param name="Retention">Effective destination retention (expiry/hold) as durably recorded on the backup node.</param>
/// <param name="Closure">Complete object dependency closure (sorted, distinct).</param>
/// <param name="ReaderPins">Number of currently valid local reader pins on this snapshot.</param>
public sealed record BackupSnapshotInspection(string Name, SnapshotId Id, CommitToken CoveredCommit, Guid Lineage,
    bool IsPublishedPointer, DfsRetention Retention, IReadOnlyList<ObjectId> Closure, int ReaderPins);

/// <summary>An in-progress incoming transfer on a backup node (BAK additions).</summary>
/// <param name="SnapshotName">Snapshot being received.</param>
/// <param name="Lineage">Transfer lineage id.</param>
/// <param name="VolumeEpoch">Source volume epoch of the transfer.</param>
/// <param name="ManifestReceived">True once the manifest was received and parsed.</param>
/// <param name="ReceivedObjects">Objects whose bytes arrived (durable or not).</param>
/// <param name="DurableObjects">Objects durably persisted and verified on this node.</param>
/// <param name="MissingObjects">Closure objects not yet durable (computed by the node; empty before the manifest arrives).</param>
/// <param name="ReceivedBytes">Payload bytes received for this transfer.</param>
public sealed record BackupReceiveInspection(string SnapshotName, Guid Lineage, ulong VolumeEpoch, bool ManifestReceived,
    IReadOnlySet<ObjectId> ReceivedObjects, IReadOnlySet<ObjectId> DurableObjects, IReadOnlySet<ObjectId> MissingObjects,
    long ReceivedBytes);
