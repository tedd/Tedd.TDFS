namespace Tedd.Tdfs.Protocol.Messages;

/// <summary>
/// Backup schedule (mirrors the SDK's <c>DfsBackupSchedule</c>).
/// </summary>
/// <param name="Interval">Periodic synchronization interval; null = none.</param>
/// <param name="OnEverySnapshot">Synchronize every new snapshot.</param>
public sealed record BackupScheduleData(
    TimeSpan? Interval,
    bool OnEverySnapshot) : IWireRecord<BackupScheduleData>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 3;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteOptionalTimeSpan(Interval);
        writer.WriteBool(OnEverySnapshot);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static BackupScheduleData Read(ref WireReader reader)
    {
        TimeSpan? interval = reader.ReadOptionalTimeSpan();
        bool onEverySnapshot = reader.ReadBool();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new BackupScheduleData(interval, onEverySnapshot) { Extensions = extensions };
    }
}

/// <summary>
/// Backup resource budgets (mirrors the SDK's <c>DfsBackupBudgets</c>).
/// </summary>
/// <param name="MaxPinAge">Maximum age of a transfer pin before the lineage is abandoned.</param>
/// <param name="MaxPinnedBytes">Maximum source bytes held by transfer pins.</param>
/// <param name="MaxBytesPerSecond">Transfer bandwidth limit.</param>
/// <param name="MaxConcurrentObjects">Concurrent object transfers.</param>
public sealed record BackupBudgetsData(
    TimeSpan MaxPinAge,
    long MaxPinnedBytes,
    long MaxBytesPerSecond,
    int MaxConcurrentObjects) : IWireRecord<BackupBudgetsData>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 5;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteTimeSpan(MaxPinAge);
        writer.WriteVarInt(MaxPinnedBytes);
        writer.WriteVarInt(MaxBytesPerSecond);
        writer.WriteVarInt32(MaxConcurrentObjects);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static BackupBudgetsData Read(ref WireReader reader)
    {
        TimeSpan maxPinAge = reader.ReadTimeSpan();
        long maxPinnedBytes = reader.ReadVarInt();
        long maxBytesPerSecond = reader.ReadVarInt();
        int maxConcurrentObjects = reader.ReadVarInt32();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new BackupBudgetsData(maxPinAge, maxPinnedBytes, maxBytesPerSecond, maxConcurrentObjects) { Extensions = extensions };
    }
}

/// <summary>
/// Identity of one snapshot transfer to a backup node.
/// </summary>
/// <param name="SourceClusterId">Source cluster.</param>
/// <param name="Volume">Source volume.</param>
/// <param name="Epoch">Source volume epoch (older epochs are rejected, BAK-026).</param>
/// <param name="LineageId">Incremental transfer lineage.</param>
/// <param name="Snapshot">Snapshot being transferred.</param>
public sealed record BackupTransferKey(
    Guid SourceClusterId,
    VolumeId Volume,
    ulong Epoch,
    Guid LineageId,
    SnapshotId Snapshot) : IWireRecord<BackupTransferKey>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 66;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteGuid(SourceClusterId);
        writer.WriteVolumeId(Volume);
        writer.WriteVarUInt(Epoch);
        writer.WriteGuid(LineageId);
        writer.WriteSnapshotId(Snapshot);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static BackupTransferKey Read(ref WireReader reader)
    {
        Guid sourceClusterId = reader.ReadGuid();
        VolumeId volume = reader.ReadVolumeId();
        ulong epoch = reader.ReadVarUInt();
        Guid lineageId = reader.ReadGuid();
        SnapshotId snapshot = reader.ReadSnapshotId();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new BackupTransferKey(sourceClusterId, volume, epoch, lineageId, snapshot) { Extensions = extensions };
    }
}

/// <summary>
/// One snapshot retained by a backup node.
/// </summary>
/// <param name="Snapshot">Snapshot identity.</param>
/// <param name="SnapshotName">Snapshot name.</param>
/// <param name="Covered">Source commit covered.</param>
/// <param name="Retention">Retention enforced by the backup node.</param>
/// <param name="Published">True when the snapshot is published (complete and readable).</param>
public sealed record BackupRetainedSnapshotData(
    SnapshotId Snapshot,
    string SnapshotName,
    CommitToken Covered,
    DfsRetention Retention,
    bool Published) : IWireRecord<BackupRetainedSnapshotData>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 55;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteSnapshotId(Snapshot);
        writer.WriteString(SnapshotName);
        writer.WriteCommitToken(Covered);
        writer.WriteRetention(Retention);
        writer.WriteBool(Published);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static BackupRetainedSnapshotData Read(ref WireReader reader)
    {
        SnapshotId snapshot = reader.ReadSnapshotId();
        string snapshotName = reader.ReadString();
        CommitToken covered = reader.ReadCommitToken();
        DfsRetention retention = reader.ReadRetention();
        bool published = reader.ReadBool();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new BackupRetainedSnapshotData(snapshot, snapshotName, covered, retention, published) { Extensions = extensions };
    }
}

/// <summary>
/// Description of a published backup snapshot for restore.
/// </summary>
/// <param name="Snapshot">The snapshot.</param>
/// <param name="SourceClusterId">Source cluster.</param>
/// <param name="SourceVolumeId">Source volume identity.</param>
/// <param name="SourceEpoch">Source volume epoch.</param>
/// <param name="ManifestLength">Manifest length in bytes.</param>
/// <param name="ManifestChecksum">XxHash128 of the manifest.</param>
/// <param name="ObjectCount">Objects in the dependency closure.</param>
/// <param name="TotalObjectBytes">Total plaintext bytes of those objects.</param>
public sealed record BackupRestoreDescriptor(
    SnapshotInfoData Snapshot,
    Guid SourceClusterId,
    VolumeId SourceVolumeId,
    ulong SourceEpoch,
    long ManifestLength,
    UInt128 ManifestChecksum,
    long ObjectCount,
    long TotalObjectBytes) : IWireRecord<BackupRestoreDescriptor>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 111;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Snapshot);
        writer.WriteGuid(SourceClusterId);
        writer.WriteVolumeId(SourceVolumeId);
        writer.WriteVarUInt(SourceEpoch);
        writer.WriteVarInt(ManifestLength);
        writer.WriteUInt128(ManifestChecksum);
        writer.WriteVarInt(ObjectCount);
        writer.WriteVarInt(TotalObjectBytes);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static BackupRestoreDescriptor Read(ref WireReader reader)
    {
        SnapshotInfoData snapshot = reader.ReadRecord<SnapshotInfoData>();
        Guid sourceClusterId = reader.ReadGuid();
        VolumeId sourceVolumeId = reader.ReadVolumeId();
        ulong sourceEpoch = reader.ReadVarUInt();
        long manifestLength = reader.ReadVarInt();
        UInt128 manifestChecksum = reader.ReadUInt128();
        long objectCount = reader.ReadVarInt();
        long totalObjectBytes = reader.ReadVarInt();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new BackupRestoreDescriptor(snapshot, sourceClusterId, sourceVolumeId, sourceEpoch, manifestLength, manifestChecksum, objectCount, totalObjectBytes) { Extensions = extensions };
    }
}
