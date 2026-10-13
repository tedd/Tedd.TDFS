namespace Tedd.Tdfs.Protocol.Messages;

/// <summary>
/// Periodic usage and maintenance report (storage node → control leader, node-authenticated). Every node reports its
/// device usage; a node that leads volume groups also summarizes each volume's replica state (per-pool stored bytes and
/// replica health, per-node holdings, policy migration progress). The control leader keeps the latest report of each
/// node in memory: it backs node and pool usage, pool replica health, the Draining → Drained transition, the safety
/// checks of a non-forced <c>RemoveNode</c> and the completion of default-policy migrations. Nothing in a report is
/// replicated; a new control leader rebuilds the picture from the next reports.
/// </summary>
/// <param name="Node">Reporting node.</param>
/// <param name="Incarnation">Reporting node incarnation (reports of a stale incarnation are rejected).</param>
/// <param name="CapacityBytes">Usable device capacity (the smaller of the configured and the device capacity).</param>
/// <param name="UsedBytes">Device bytes in use (objects, logs, checkpoints, everything).</param>
/// <param name="AvailableBytes">Bytes client payload could still add before admission refuses it (capacity minus the reserve).</param>
/// <param name="RepairAvailableBytes">Bytes repair copies could still add (they may use the repair part of the reserve).</param>
/// <param name="ObjectBytes">Bytes of live object records in the node's object store.</param>
/// <param name="Volumes">Summaries of the volumes this node leads (empty when it leads none).</param>
public sealed record ReportUsageMessage(
    NodeId Node,
    ulong Incarnation,
    long CapacityBytes,
    long UsedBytes,
    long AvailableBytes,
    long RepairAvailableBytes,
    long ObjectBytes,
    IReadOnlyList<VolumeUsageReport> Volumes) : IWireMessage<ReportUsageMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.ReportUsage;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteNodeId(Node);
        writer.WriteVarUInt(Incarnation);
        writer.WriteVarInt(CapacityBytes);
        writer.WriteVarInt(UsedBytes);
        writer.WriteVarInt(AvailableBytes);
        writer.WriteVarInt(RepairAvailableBytes);
        writer.WriteVarInt(ObjectBytes);
        writer.WriteList(Volumes);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static ReportUsageMessage Read(ref WireReader reader)
    {
        NodeId node = reader.ReadNodeId();
        ulong incarnation = reader.ReadVarUInt();
        long capacityBytes = reader.ReadVarInt();
        long usedBytes = reader.ReadVarInt();
        long availableBytes = reader.ReadVarInt();
        long repairAvailableBytes = reader.ReadVarInt();
        long objectBytes = reader.ReadVarInt();
        IReadOnlyList<VolumeUsageReport> volumes = reader.ReadList<VolumeUsageReport>();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new ReportUsageMessage(node, incarnation, capacityBytes, usedBytes, availableBytes, repairAvailableBytes, objectBytes, volumes)
        {
            Extensions = extensions,
        };
    }
}

/// <summary>
/// Reply to <see cref="ReportUsageMessage"/>.
/// </summary>
/// <param name="ScrubRequest">Sequence number of the latest scrub requested for the reporting node by <c>AdminOp.StartScrub</c>
/// (0 = none); the node starts a full scrub pass when it sees a number it has not acted on yet.</param>
public sealed record ReportUsageResultMessage(
    ulong ScrubRequest) : IWireMessage<ReportUsageResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.ReportUsageResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteVarUInt(ScrubRequest);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static ReportUsageResultMessage Read(ref WireReader reader)
    {
        ulong scrubRequest = reader.ReadVarUInt();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new ReportUsageResultMessage(scrubRequest) { Extensions = extensions };
    }
}

/// <summary>
/// Replica-state summary of one volume, computed by its leader from storage-node inventories, repair results and the
/// applied volume state (part of <see cref="ReportUsageMessage"/>).
/// </summary>
/// <param name="Volume">Summarized volume.</param>
/// <param name="VolumeEpoch">Volume epoch the leader serves.</param>
/// <param name="Term">Raft term of the volume group in which the reporting node leads it: a summary of an older term (a
/// deposed leader that has not noticed yet) never replaces a current summary of a newer term.</param>
/// <param name="PlacementEpoch">Placement epoch the summary was computed under.</param>
/// <param name="CompletedMigration">The default-policy migration target every default-policy object now meets (null when no
/// migration is pending or it is not complete yet); the control leader then makes the policy effective.</param>
/// <param name="Pools">Stored bytes and replica health per storage pool the volume's objects are placed in.</param>
/// <param name="Nodes">Holdings per node whose complete inventory (or last-known inventory, for a node that went down)
/// the leader has; a node of the volume's pool that is missing here has not reported yet.</param>
public sealed record VolumeUsageReport(
    VolumeId Volume,
    ulong VolumeEpoch,
    ulong Term,
    ulong PlacementEpoch,
    DfsStoragePolicy? CompletedMigration,
    IReadOnlyList<PoolUsageReport> Pools,
    IReadOnlyList<NodeHoldingReport> Nodes) : IWireRecord<VolumeUsageReport>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 23;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteVolumeId(Volume);
        writer.WriteVarUInt(VolumeEpoch);
        writer.WriteVarUInt(Term);
        writer.WriteVarUInt(PlacementEpoch);
        writer.WriteOptionalStoragePolicy(CompletedMigration);
        writer.WriteList(Pools);
        writer.WriteList(Nodes);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static VolumeUsageReport Read(ref WireReader reader)
    {
        VolumeId volume = reader.ReadVolumeId();
        ulong volumeEpoch = reader.ReadVarUInt();
        ulong term = reader.ReadVarUInt();
        ulong placementEpoch = reader.ReadVarUInt();
        DfsStoragePolicy? completedMigration = reader.ReadOptionalStoragePolicy();
        IReadOnlyList<PoolUsageReport> pools = reader.ReadList<PoolUsageReport>();
        IReadOnlyList<NodeHoldingReport> nodes = reader.ReadList<NodeHoldingReport>();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new VolumeUsageReport(volume, volumeEpoch, term, placementEpoch, completedMigration, pools, nodes) { Extensions = extensions };
    }
}

/// <summary>
/// Stored bytes and replica health of the objects of one volume placed through one storage pool (part of
/// <see cref="VolumeUsageReport"/>). Objects are attributed to the pool of the strongest policy referencing them.
/// </summary>
/// <param name="Pool">Storage pool.</param>
/// <param name="StoredBytes">Plaintext bytes of every copy of the volume's objects attributed to the pool, held on the pool's
/// members (all copies the inventories report, garbage included).</param>
/// <param name="Health">Replica health of the referenced objects attributed to the pool, judged against the pool's members
/// that are up.</param>
public sealed record PoolUsageReport(
    string Pool,
    long StoredBytes,
    ReplicaHealthData Health) : IWireRecord<PoolUsageReport>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 13;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteString(Pool);
        writer.WriteVarInt(StoredBytes);
        writer.WriteRecord(Health);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static PoolUsageReport Read(ref WireReader reader)
    {
        string pool = reader.ReadString();
        long storedBytes = reader.ReadVarInt();
        ReplicaHealthData health = reader.ReadRecord<ReplicaHealthData>();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new PoolUsageReport(pool, storedBytes, health) { Extensions = extensions };
    }
}

/// <summary>
/// One node's holdings of a volume's objects, as its volume leader knows them (part of <see cref="VolumeUsageReport"/>).
/// </summary>
/// <param name="Node">Holder.</param>
/// <param name="Up">True when the node's inventory is current (it reported recently under its registered incarnation);
/// false for the last-known holdings of a node that went silent.</param>
/// <param name="Objects">Copies of objects the volume keeps (referenced, protected or staged) on the node.</param>
/// <param name="Bytes">Plaintext bytes of those copies.</param>
/// <param name="NeededForAck">Referenced objects whose copies on the other nodes that are up (not counting copies whose
/// excess-replica deletion is in flight) fall below their policy's acknowledgement copies or zones: losing this node would
/// lose acknowledged durability.</param>
public sealed record NodeHoldingReport(
    NodeId Node,
    bool Up,
    long Objects,
    long Bytes,
    long NeededForAck) : IWireRecord<NodeHoldingReport>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 21;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteNodeId(Node);
        writer.WriteBool(Up);
        writer.WriteVarInt(Objects);
        writer.WriteVarInt(Bytes);
        writer.WriteVarInt(NeededForAck);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static NodeHoldingReport Read(ref WireReader reader)
    {
        NodeId node = reader.ReadNodeId();
        bool up = reader.ReadBool();
        long objects = reader.ReadVarInt();
        long bytes = reader.ReadVarInt();
        long neededForAck = reader.ReadVarInt();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new NodeHoldingReport(node, up, objects, bytes, neededForAck) { Extensions = extensions };
    }
}
