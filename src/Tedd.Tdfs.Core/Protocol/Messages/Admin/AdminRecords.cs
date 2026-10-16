namespace Tedd.Tdfs.Protocol.Messages;

/// <summary>
/// Status of one Raft group (control or volume).
/// </summary>
/// <param name="Group">Group name (control, or the volume name).</param>
/// <param name="Voters">Voter node names.</param>
/// <param name="Learners">Learner node names.</param>
/// <param name="Leader">Leader node name, if any.</param>
/// <param name="Term">Current term.</param>
/// <param name="CommitIndex">Commit index.</param>
/// <param name="AppliedIndex">Applied index of the answering replica.</param>
/// <param name="HasQuorum">True when a majority of configured voters is reachable from the leader.</param>
public sealed record GroupStatusData(
    string Group,
    IReadOnlyList<string> Voters,
    IReadOnlyList<string> Learners,
    string? Leader,
    ulong Term,
    ulong CommitIndex,
    ulong AppliedIndex,
    bool HasQuorum) : IWireRecord<GroupStatusData>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 9;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteString(Group);
        writer.WriteStringList(Voters);
        writer.WriteStringList(Learners);
        writer.WriteOptionalString(Leader);
        writer.WriteVarUInt(Term);
        writer.WriteVarUInt(CommitIndex);
        writer.WriteVarUInt(AppliedIndex);
        writer.WriteBool(HasQuorum);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static GroupStatusData Read(ref WireReader reader)
    {
        string group = reader.ReadString();
        IReadOnlyList<string> voters = reader.ReadStringList();
        IReadOnlyList<string> learners = reader.ReadStringList();
        string? leader = reader.ReadOptionalString();
        ulong term = reader.ReadVarUInt();
        ulong commitIndex = reader.ReadVarUInt();
        ulong appliedIndex = reader.ReadVarUInt();
        bool hasQuorum = reader.ReadBool();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new GroupStatusData(group, voters, learners, leader, term, commitIndex, appliedIndex, hasQuorum) { Extensions = extensions };
    }
}

/// <summary>
/// Replica health of a volume (mirrors the SDK's <c>DfsReplicaHealth</c>).
/// </summary>
/// <param name="Objects">Referenced objects.</param>
/// <param name="UnderReplicatedObjects">Objects with fewer verified copies than the target.</param>
/// <param name="ZoneDeficitObjects">Objects in fewer zones than the target.</param>
/// <param name="MissingObjects">Objects with no verified copy.</param>
/// <param name="PlacementViolations">Copies violating placement rules.</param>
/// <param name="RepairBacklogBytes">Bytes waiting for repair.</param>
/// <param name="PendingGcObjects">Objects waiting for garbage collection.</param>
/// <param name="StagedBytes">Bytes in open staging sessions.</param>
/// <param name="PinnedBytes">Bytes the current namespace no longer references that snapshots or pins retain.</param>
public sealed record ReplicaHealthData(
    long Objects,
    long UnderReplicatedObjects,
    long ZoneDeficitObjects,
    long MissingObjects,
    long PlacementViolations,
    long RepairBacklogBytes,
    long PendingGcObjects,
    long StagedBytes,
    long PinnedBytes) : IWireRecord<ReplicaHealthData>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 10;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteVarInt(Objects);
        writer.WriteVarInt(UnderReplicatedObjects);
        writer.WriteVarInt(ZoneDeficitObjects);
        writer.WriteVarInt(MissingObjects);
        writer.WriteVarInt(PlacementViolations);
        writer.WriteVarInt(RepairBacklogBytes);
        writer.WriteVarInt(PendingGcObjects);
        writer.WriteVarInt(StagedBytes);
        writer.WriteVarInt(PinnedBytes);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static ReplicaHealthData Read(ref WireReader reader)
    {
        long objects = reader.ReadVarInt();
        long underReplicatedObjects = reader.ReadVarInt();
        long zoneDeficitObjects = reader.ReadVarInt();
        long missingObjects = reader.ReadVarInt();
        long placementViolations = reader.ReadVarInt();
        long repairBacklogBytes = reader.ReadVarInt();
        long pendingGcObjects = reader.ReadVarInt();
        long stagedBytes = reader.ReadVarInt();
        long pinnedBytes = reader.ReadVarInt();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new ReplicaHealthData(objects, underReplicatedObjects, zoneDeficitObjects, missingObjects, placementViolations, repairBacklogBytes, pendingGcObjects, stagedBytes, pinnedBytes) { Extensions = extensions };
    }
}

/// <summary>
/// Status of one backup target (mirrors the SDK's <c>DfsBackupStatus</c>).
/// </summary>
/// <param name="Target">Target name.</param>
/// <param name="State">Transfer state.</param>
/// <param name="ReceivedBytes">Bytes received by the target in the current transfer.</param>
/// <param name="DurableBoundary">Highest boundary whose objects and manifest are durable on the target.</param>
/// <param name="PublishedBoundary">Highest boundary published (readable) on the target.</param>
/// <param name="LatestPublished">Latest published checkpoint.</param>
/// <param name="RecoveryPointAge">Age of the latest published recovery point (leader clock).</param>
/// <param name="ReseedRequired">True when the incremental lineage was abandoned.</param>
/// <param name="PinnedSourceBytes">Source bytes held by transfer pins for this target.</param>
/// <param name="LastError">Last transfer error.</param>
/// <param name="LineageId">Current transfer lineage.</param>
public sealed record BackupStatusData(
    string Target,
    WireBackupState State,
    long ReceivedBytes,
    CommitToken DurableBoundary,
    CommitToken PublishedBoundary,
    DfsBackupCheckpoint? LatestPublished,
    TimeSpan? RecoveryPointAge,
    bool ReseedRequired,
    long PinnedSourceBytes,
    string? LastError,
    Guid LineageId) : IWireRecord<BackupStatusData>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 93;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteString(Target);
        writer.WriteDefinedEnum(State);
        writer.WriteVarInt(ReceivedBytes);
        writer.WriteCommitToken(DurableBoundary);
        writer.WriteCommitToken(PublishedBoundary);
        writer.WriteOptionalBackupCheckpoint(LatestPublished);
        writer.WriteOptionalTimeSpan(RecoveryPointAge);
        writer.WriteBool(ReseedRequired);
        writer.WriteVarInt(PinnedSourceBytes);
        writer.WriteOptionalString(LastError);
        writer.WriteGuid(LineageId);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static BackupStatusData Read(ref WireReader reader)
    {
        string target = reader.ReadString();
        WireBackupState state = reader.ReadEnum<WireBackupState>();
        long receivedBytes = reader.ReadVarInt();
        CommitToken durableBoundary = reader.ReadCommitToken();
        CommitToken publishedBoundary = reader.ReadCommitToken();
        DfsBackupCheckpoint? latestPublished = reader.ReadOptionalBackupCheckpoint();
        TimeSpan? recoveryPointAge = reader.ReadOptionalTimeSpan();
        bool reseedRequired = reader.ReadBool();
        long pinnedSourceBytes = reader.ReadVarInt();
        string? lastError = reader.ReadOptionalString();
        Guid lineageId = reader.ReadGuid();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new BackupStatusData(target, state, receivedBytes, durableBoundary, publishedBoundary, latestPublished, recoveryPointAge, reseedRequired, pinnedSourceBytes, lastError, lineageId) { Extensions = extensions };
    }
}
