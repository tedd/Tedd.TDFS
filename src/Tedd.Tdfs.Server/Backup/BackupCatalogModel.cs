using System.Collections.Immutable;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Volumes.State;

namespace Tedd.Tdfs.Server.Backup;

/// <summary>A source authority (volume identity and epoch) whose exports a backup node accepts or has retired.</summary>
/// <param name="Volume">Source volume id.</param>
/// <param name="Epoch">Source volume epoch.</param>
internal readonly record struct BackupAuthority(VolumeId Volume, ulong Epoch);

/// <summary>
/// Everything a backup node holds for one source volume name: the current source authority and transfer lineage, retired
/// authorities (their exports are rejected, BAK-026), the retained published snapshots of every lineage and the staged
/// (not yet published) transfer.
/// </summary>
internal sealed class BackupVolume(string name, string key)
{
    /// <summary>Source volume name.</summary>
    public string Name { get; } = name;

    /// <summary>File-system safe key of the name (directory names).</summary>
    public string Key { get; } = key;

    /// <summary>The authority whose exports are accepted (null before the first transfer).</summary>
    public BackupAuthority? Authority { get; set; }

    /// <summary>The lineage the node currently follows (empty before the first transfer).</summary>
    public Guid CurrentLineage { get; set; }

    /// <summary>Authorities superseded by a newer one (ordered by retirement).</summary>
    public List<BackupAuthority> Retired { get; } = [];

    /// <summary>Next publication sequence (orders snapshots across lineages).</summary>
    public ulong NextSequence { get; set; } = 1;

    /// <summary>Retained published snapshots (including condemned ones still held by readers), ascending publication sequence.</summary>
    public List<BackupSnapshot> Snapshots { get; } = [];

    /// <summary>The staged incoming transfer, or null.</summary>
    public StagedTransfer? Staged { get; set; }

    /// <summary>True when the volume state was persisted at least once.</summary>
    public bool Persisted { get; set; }

    /// <summary>Snapshots visible to clients (published, not condemned), ascending publication sequence.</summary>
    public IEnumerable<BackupSnapshot> Visible => Snapshots.Where(s => !s.Condemned);

    /// <summary>The published pointer: the most recently published visible snapshot.</summary>
    public BackupSnapshot? Pointer => Visible.LastOrDefault();

    /// <summary>The visible snapshot named <paramref name="name"/> (the most recent one when several authorities used the name).</summary>
    public BackupSnapshot? FindVisible(string name) => Visible.LastOrDefault(s => string.Equals(s.Name, name, StringComparison.Ordinal));

    /// <summary>The retained snapshot with source id <paramref name="id"/> (condemned ones included).</summary>
    public BackupSnapshot? FindById(SnapshotId id) => Snapshots.LastOrDefault(s => s.Id == id);

    /// <summary>True when <paramref name="authority"/> was superseded.</summary>
    public bool IsRetired(BackupAuthority authority) => Retired.Contains(authority);
}

/// <summary>One published snapshot retained by a backup node.</summary>
internal sealed class BackupSnapshot
{
    private ImmutableSortedDictionary<ulong, InodeRecord>? _root;

    public required string Key { get; init; }

    public required SnapshotId Id { get; init; }

    public required string Name { get; init; }

    /// <summary>Source commit boundary (cluster, volume, epoch, index).</summary>
    public required CommitToken Covered { get; init; }

    public required Guid Lineage { get; init; }

    /// <summary>Publication sequence (catalog order).</summary>
    public required ulong Sequence { get; init; }

    public required DateTimeOffset CreatedUtc { get; init; }

    public required DfsConsistencyLabel Label { get; init; }

    public required IReadOnlyList<AdapterRecordData> Adapters { get; init; }

    /// <summary>Retention enforced by this node (durable).</summary>
    public DfsRetention Retention { get; set; } = new(null, false);

    /// <summary>The decoded manifest (null while the node is locked).</summary>
    public BackupManifest? Manifest { get; set; }

    /// <summary>Valid local reader pins.</summary>
    public int ReaderPins { get; set; }

    /// <summary>Removed from the published set (deleted or pruned) but still held by a reader.</summary>
    public bool Condemned { get; set; }

    /// <summary>The source authority of the snapshot.</summary>
    public BackupAuthority Authority => new(Covered.VolumeId, Covered.VolumeEpoch);

    /// <summary>The namespace (inode map) of the snapshot.</summary>
    public ImmutableSortedDictionary<ulong, InodeRecord> Root
        => _root ??= (Manifest ?? throw new DfsRecoveryKeyMissingException($"The contents of snapshot '{Name}' cannot be decrypted without the recovery key.")).Root();

    /// <summary>The dependency closure (empty while locked).</summary>
    public IReadOnlyList<ObjectId> Closure => Manifest?.ClosureIds ?? [];

    /// <summary>Wire description (retention as enforced by this node).</summary>
    public SnapshotInfoData Describe(string volume)
        => new(Id, Name, volume, Covered, Label, Adapters, CreatedUtc, Retention);

    /// <summary>True when retention protects the snapshot at <paramref name="now"/>.</summary>
    public bool IsProtected(DateTimeOffset now) => Retention.Hold || (Retention.ExpiresUtc is { } e && e > now);
}

/// <summary>
/// A staged (resumable, not yet published) incoming transfer of one snapshot: the transfer identity and the snapshot's
/// description from <c>BackupBegin</c>, the manifest as received, and which objects arrived and are durable.
/// </summary>
internal sealed class StagedTransfer
{
    public required BackupTransferKey Key { get; init; }

    public required string VolumeName { get; init; }

    public required string ClusterName { get; init; }

    public required string SnapshotName { get; init; }

    public required CommitToken Boundary { get; init; }

    public required DateTimeOffset CreatedUtc { get; init; }

    public required DfsConsistencyLabel Label { get; init; }

    public required IReadOnlyList<AdapterRecordData> Adapters { get; init; }

    public required DfsRetention RequestedRetention { get; init; }

    /// <summary>The principal that began the transfer (its secret keys the transit encryption).</summary>
    public required string Exporter { get; set; }

    /// <summary>Partial manifest bytes being received (sealed form, as transferred).</summary>
    public byte[]? ManifestBuffer { get; set; }

    /// <summary>Bytes of <see cref="ManifestBuffer"/> received so far.</summary>
    public long ManifestReceived { get; set; }

    /// <summary>Announced total length of the manifest being received.</summary>
    public long ManifestTotal { get; set; }

    /// <summary>Announced checksum of the manifest being received.</summary>
    public UInt128 ManifestChecksum { get; set; }

    /// <summary>The verified, durably staged manifest (null until complete).</summary>
    public BackupManifest? Manifest { get; set; }

    /// <summary>Checksum and length of the sealed manifest that produced <see cref="Manifest"/>.</summary>
    public (UInt128 Checksum, long Length) ManifestIdentity { get; set; }

    /// <summary>Objects whose bytes arrived in this transfer.</summary>
    public HashSet<ObjectId> Received { get; } = [];

    /// <summary>Objects durably persisted and verified in this transfer.</summary>
    public HashSet<ObjectId> Durable { get; } = [];

    /// <summary>Payload bytes received in this transfer.</summary>
    public long ReceivedBytes { get; set; }

    /// <summary>True once the publication verified the complete closure durable (the durable recovery boundary, BAK-016).</summary>
    public bool Verified { get; set; }

    /// <summary>A running publication (duplicate publish requests join it).</summary>
    public Task<PublishOutcome>? Publishing { get; set; }

    /// <summary>The source authority of the transfer.</summary>
    public BackupAuthority Authority => new(Key.Volume, Key.Epoch);
}

/// <summary>Result of one publication attempt.</summary>
/// <param name="Published">True when the snapshot is published.</param>
/// <param name="Missing">Closure objects missing or invalid (when not published).</param>
/// <param name="ManifestIncomplete">True when no verified manifest is staged.</param>
/// <param name="Snapshot">The published snapshot.</param>
internal sealed record PublishOutcome(bool Published, IReadOnlyList<ObjectId> Missing, bool ManifestIncomplete, BackupSnapshot? Snapshot);
