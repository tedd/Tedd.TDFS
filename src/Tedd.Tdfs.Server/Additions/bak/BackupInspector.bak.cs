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
        => throw new NotImplementedException("BackupInspector.GetSnapshotsAsync: implemented in a later phase");

    /// <summary>
    /// Returns the state of the in-progress (not yet published) incoming transfer for <paramref name="volume"/>, or
    /// <c>null</c> when no transfer is staged. Objects are counted as <em>received</em> once their bytes arrived and as
    /// <em>durable</em> only after the destination flush barrier completed and the object verified; the
    /// <see cref="BackupReceiveInspection.MissingObjects"/> set is computed by the node itself from the received
    /// manifest (never from inventory hints) and is empty only when the full closure is durable. Used by BAK-004,
    /// BAK-006, BAK-007, BAK-009, BAK-016.
    /// </summary>
    public Task<BackupReceiveInspection?> GetReceiveStateAsync(string volume)
        => throw new NotImplementedException("BackupInspector.GetReceiveStateAsync: implemented in a later phase");

    /// <summary>
    /// Returns, for every object stored on this backup node, the number of retained snapshots (all volumes and
    /// lineages, published or staged-manifest) whose dependency closure references it, as the node's retention/GC
    /// logic accounts it (tracing implementations report the traced count). An object that is stored but referenced
    /// by nothing reports 0. Duplicate deliveries of the same immutable object must never change the count. Used by
    /// BAK-009, BAK-027.
    /// </summary>
    public Task<IReadOnlyDictionary<ObjectId, int>> GetObjectReferenceCountsAsync()
        => throw new NotImplementedException("BackupInspector.GetObjectReferenceCountsAsync: implemented in a later phase");

    /// <summary>
    /// Independently re-reads and verifies (identity, length, encoding and checksum) every object in the dependency
    /// closure of the retained snapshot <paramref name="snapshotName"/> of <paramref name="volume"/> from the durable
    /// store, and returns the ids that are missing or fail verification (empty when the closure is complete). Throws
    /// <see cref="DfsException"/> with <see cref="DfsErrorCode.SnapshotNotFound"/> when no such snapshot is retained.
    /// Used by BAK-006, BAK-008, BAK-010, BAK-012, BAK-020, BAK-024.
    /// </summary>
    public Task<IReadOnlyList<ObjectId>> VerifyClosureAsync(string volume, string snapshotName)
        => throw new NotImplementedException("BackupInspector.VerifyClosureAsync: implemented in a later phase");
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
