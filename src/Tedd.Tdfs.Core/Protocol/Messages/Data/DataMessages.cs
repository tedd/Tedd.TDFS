namespace Tedd.Tdfs.Protocol.Messages;

/// <summary>
/// Stores one immutable object (client or node → storage node). The node verifies the staging capability (MAC,
/// cluster, volume, epoch, staging session, principal, expiry), the declared length and checksum against the payload,
/// and capacity admission (CapacityExhausted before touching reserves), writes the record and flushes, and only then
/// returns a signed receipt. Idempotent: re-uploading identical content returns a receipt; the same id with different
/// content is rejected with <see cref="DfsErrorCode.Integrity"/>. Objects are at most <c>MaxObjectSize</c> bytes.
/// </summary>
/// <param name="Capability">Encoded staging <see cref="DataCapability"/> (may be empty only for authenticated node peers).</param>
/// <param name="Volume">Owning volume.</param>
/// <param name="Staging">Staging session (must match the capability).</param>
/// <param name="Object">Object id chosen by the uploader (random, non-zero).</param>
/// <param name="Checksum">XxHash128 of <paramref name="Data"/>.</param>
/// <param name="Length">Length of <paramref name="Data"/>.</param>
/// <param name="Encoding">Object encoding (<see cref="ObjectEncodings"/>).</param>
/// <param name="PlacementEpoch">Placement epoch the uploader computed targets under.</param>
/// <param name="Data">Object payload (plaintext; nodes encrypt at rest).</param>
public sealed record PutObjectMessage(
    ReadOnlyMemory<byte> Capability,
    VolumeId Volume,
    StagingId Staging,
    ObjectId Object,
    UInt128 Checksum,
    long Length,
    byte Encoding,
    ulong PlacementEpoch,
    ReadOnlyMemory<byte> Data) : IWireMessage<PutObjectMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.PutObject;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteBytes(Capability.Span);
        writer.WriteVolumeId(Volume);
        writer.WriteStagingId(Staging);
        writer.WriteObjectId(Object);
        writer.WriteUInt128(Checksum);
        writer.WriteVarInt(Length);
        writer.WriteByte(Encoding);
        writer.WriteVarUInt(PlacementEpoch);
        writer.WriteBytes(Data.Span);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static PutObjectMessage Read(ref WireReader reader)
    {
        ReadOnlyMemory<byte> capability = reader.ReadBytesCopy();
        VolumeId volume = reader.ReadVolumeId();
        StagingId staging = reader.ReadStagingId();
        ObjectId @object = reader.ReadObjectId();
        UInt128 checksum = reader.ReadUInt128();
        long length = reader.ReadVarInt();
        byte encoding = reader.ReadByte();
        ulong placementEpoch = reader.ReadVarUInt();
        ReadOnlyMemory<byte> data = reader.ReadBytesCopy();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new PutObjectMessage(capability, volume, staging, @object, checksum, length, encoding, placementEpoch, data) { Extensions = extensions };
    }
}

/// <summary>
/// Reply to <see cref="PutObjectMessage"/>.
/// </summary>
/// <param name="Receipt">Signed durability receipt.</param>
/// <param name="AlreadyPresent">True when an identical copy was already stored.</param>
public sealed record PutObjectResultMessage(
    ObjectReceipt Receipt,
    bool AlreadyPresent) : IWireMessage<PutObjectResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.PutObjectResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Receipt);
        writer.WriteBool(AlreadyPresent);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static PutObjectResultMessage Read(ref WireReader reader)
    {
        ObjectReceipt receipt = reader.ReadRecord<ObjectReceipt>();
        bool alreadyPresent = reader.ReadBool();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new PutObjectResultMessage(receipt, alreadyPresent) { Extensions = extensions };
    }
}

/// <summary>
/// Reads a byte range of an object (client or node → storage node). Requires a read capability listing the object
/// (nodes may omit it); object ids alone grant nothing (SEC-004). The node verifies the stored record before replying
/// (corruption → <see cref="DfsErrorCode.Integrity"/>, the client tries another replica); a missing object is <see
/// cref="DfsErrorCode.Integrity"/> with detail "missing", never zeroes.
/// </summary>
/// <param name="Capability">Encoded read <see cref="DataCapability"/>.</param>
/// <param name="Volume">Owning volume.</param>
/// <param name="Object">Object to read.</param>
/// <param name="Offset">First byte to read.</param>
/// <param name="Length">Bytes to read (clipped to the object end).</param>
public sealed record GetObjectMessage(
    ReadOnlyMemory<byte> Capability,
    VolumeId Volume,
    ObjectId Object,
    long Offset,
    long Length) : IWireMessage<GetObjectMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.GetObject;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteBytes(Capability.Span);
        writer.WriteVolumeId(Volume);
        writer.WriteObjectId(Object);
        writer.WriteVarInt(Offset);
        writer.WriteVarInt(Length);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static GetObjectMessage Read(ref WireReader reader)
    {
        ReadOnlyMemory<byte> capability = reader.ReadBytesCopy();
        VolumeId volume = reader.ReadVolumeId();
        ObjectId @object = reader.ReadObjectId();
        long offset = reader.ReadVarInt();
        long length = reader.ReadVarInt();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new GetObjectMessage(capability, volume, @object, offset, length) { Extensions = extensions };
    }
}

/// <summary>
/// Reply to <see cref="GetObjectMessage"/>. When <c>UnitSize</c> is positive, <c>Offset</c> is aligned down to a unit
/// boundary, <c>Data</c> covers the requested range extended to unit boundaries (clipped to the object end), and
/// <c>UnitChecksums[i]</c> is the CRC32C of unit i of <c>Data</c>, letting the client verify a partial range; a
/// whole-object read is verified against <c>Checksum</c>. When <c>UnitSize</c> is 0, <c>Offset</c> is the requested
/// offset and no unit checksums are supplied.
/// </summary>
/// <param name="ObjectLength">Full plaintext length of the object.</param>
/// <param name="Checksum">XxHash128 of the whole object plaintext.</param>
/// <param name="Encoding">Object encoding.</param>
/// <param name="Offset">Object offset of the first byte of <c>Data</c>.</param>
/// <param name="Data">Object bytes.</param>
/// <param name="UnitSize">Verification unit size (0 = none).</param>
/// <param name="UnitChecksums">CRC32C of each unit of <c>Data</c>.</param>
public sealed record GetObjectResultMessage(
    long ObjectLength,
    UInt128 Checksum,
    byte Encoding,
    long Offset,
    ReadOnlyMemory<byte> Data,
    int UnitSize,
    IReadOnlyList<uint> UnitChecksums) : IWireMessage<GetObjectResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.GetObjectResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteVarInt(ObjectLength);
        writer.WriteUInt128(Checksum);
        writer.WriteByte(Encoding);
        writer.WriteVarInt(Offset);
        writer.WriteBytes(Data.Span);
        writer.WriteVarInt32(UnitSize);
        writer.WriteUInt32List(UnitChecksums);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static GetObjectResultMessage Read(ref WireReader reader)
    {
        long objectLength = reader.ReadVarInt();
        UInt128 checksum = reader.ReadUInt128();
        byte encoding = reader.ReadByte();
        long offset = reader.ReadVarInt();
        ReadOnlyMemory<byte> data = reader.ReadBytesCopy();
        int unitSize = reader.ReadVarInt32();
        IReadOnlyList<uint> unitChecksums = reader.ReadUInt32List();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new GetObjectResultMessage(objectLength, checksum, encoding, offset, data, unitSize, unitChecksums) { Extensions = extensions };
    }
}

/// <summary>
/// Asks a storage node which of the listed objects it holds (client or node → storage node; read fallback broadcast
/// and repair). Requires Read permission on the volume.
/// </summary>
/// <param name="Volume">Owning volume.</param>
/// <param name="Objects">Objects to look up.</param>
public sealed record LocateObjectMessage(
    VolumeId Volume,
    IReadOnlyList<ObjectId> Objects) : IWireMessage<LocateObjectMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.LocateObject;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteVolumeId(Volume);
        writer.WriteObjectIdList(Objects);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static LocateObjectMessage Read(ref WireReader reader)
    {
        VolumeId volume = reader.ReadVolumeId();
        IReadOnlyList<ObjectId> objects = reader.ReadObjectIdList();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new LocateObjectMessage(volume, objects) { Extensions = extensions };
    }
}

/// <summary>
/// Reply to <see cref="LocateObjectMessage"/>.
/// </summary>
/// <param name="Locations">One answer per requested object, in request order.</param>
public sealed record LocateObjectResultMessage(
    IReadOnlyList<ObjectLocation> Locations) : IWireMessage<LocateObjectResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.LocateObjectResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteList(Locations);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static LocateObjectResultMessage Read(ref WireReader reader)
    {
        IReadOnlyList<ObjectLocation> locations = reader.ReadList<ObjectLocation>();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new LocateObjectResultMessage(locations) { Extensions = extensions };
    }
}

/// <summary>
/// Repair pull (volume leader → target storage node): fetch the object from one of the sources (node-authenticated
/// <c>GetObject</c>), verify length and checksum, store it durably and return a receipt. Idempotent.
/// </summary>
/// <param name="Volume">Owning volume.</param>
/// <param name="Object">Object to replicate.</param>
/// <param name="Staging">Staging session the object was originally stored under (kept for GC decisions).</param>
/// <param name="Checksum">Expected XxHash128 of the plaintext.</param>
/// <param name="Length">Expected plaintext length.</param>
/// <param name="Encoding">Object encoding.</param>
/// <param name="PlacementEpoch">Placement epoch of the repair decision.</param>
/// <param name="Sources">Nodes believed to hold verified copies, preferred first.</param>
public sealed record ReplicateObjectMessage(
    VolumeId Volume,
    ObjectId Object,
    StagingId Staging,
    UInt128 Checksum,
    long Length,
    byte Encoding,
    ulong PlacementEpoch,
    IReadOnlyList<NodeId> Sources) : IWireMessage<ReplicateObjectMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.ReplicateObject;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteVolumeId(Volume);
        writer.WriteObjectId(Object);
        writer.WriteStagingId(Staging);
        writer.WriteUInt128(Checksum);
        writer.WriteVarInt(Length);
        writer.WriteByte(Encoding);
        writer.WriteVarUInt(PlacementEpoch);
        writer.WriteNodeIdList(Sources);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static ReplicateObjectMessage Read(ref WireReader reader)
    {
        VolumeId volume = reader.ReadVolumeId();
        ObjectId @object = reader.ReadObjectId();
        StagingId staging = reader.ReadStagingId();
        UInt128 checksum = reader.ReadUInt128();
        long length = reader.ReadVarInt();
        byte encoding = reader.ReadByte();
        ulong placementEpoch = reader.ReadVarUInt();
        IReadOnlyList<NodeId> sources = reader.ReadNodeIdList();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new ReplicateObjectMessage(volume, @object, staging, checksum, length, encoding, placementEpoch, sources) { Extensions = extensions };
    }
}

/// <summary>
/// Reply to <see cref="ReplicateObjectMessage"/>.
/// </summary>
/// <param name="Receipt">Durability receipt of the new copy.</param>
/// <param name="AlreadyPresent">True when the node already held a verified copy.</param>
public sealed record ReplicateObjectResultMessage(
    ObjectReceipt Receipt,
    bool AlreadyPresent) : IWireMessage<ReplicateObjectResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.ReplicateObjectResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Receipt);
        writer.WriteBool(AlreadyPresent);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static ReplicateObjectResultMessage Read(ref WireReader reader)
    {
        ObjectReceipt receipt = reader.ReadRecord<ObjectReceipt>();
        bool alreadyPresent = reader.ReadBool();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new ReplicateObjectResultMessage(receipt, alreadyPresent) { Extensions = extensions };
    }
}

/// <summary>
/// Deletes objects (volume leader → storage node): garbage the leader proved unreachable at a committed index with a
/// closed staging session (GC), or excess replicas after repair verified the targets (REP-016). Rejected for an older
/// volume epoch. Idempotent.
/// </summary>
/// <param name="Volume">Owning volume.</param>
/// <param name="VolumeEpoch">Volume epoch of the decision.</param>
/// <param name="DecisionIndex">Applied index at which deletability was decided.</param>
/// <param name="Objects">Objects to delete.</param>
/// <param name="ExcessReplicas">True for excess-replica cleanup rather than garbage collection.</param>
public sealed record DeleteObjectsMessage(
    VolumeId Volume,
    ulong VolumeEpoch,
    ulong DecisionIndex,
    IReadOnlyList<ObjectId> Objects,
    bool ExcessReplicas) : IWireMessage<DeleteObjectsMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.DeleteObjects;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteVolumeId(Volume);
        writer.WriteVarUInt(VolumeEpoch);
        writer.WriteVarUInt(DecisionIndex);
        writer.WriteObjectIdList(Objects);
        writer.WriteBool(ExcessReplicas);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static DeleteObjectsMessage Read(ref WireReader reader)
    {
        VolumeId volume = reader.ReadVolumeId();
        ulong volumeEpoch = reader.ReadVarUInt();
        ulong decisionIndex = reader.ReadVarUInt();
        IReadOnlyList<ObjectId> objects = reader.ReadObjectIdList();
        bool excessReplicas = reader.ReadBool();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new DeleteObjectsMessage(volume, volumeEpoch, decisionIndex, objects, excessReplicas) { Extensions = extensions };
    }
}

/// <summary>
/// Reply to <see cref="DeleteObjectsMessage"/>.
/// </summary>
/// <param name="Deleted">Objects deleted.</param>
/// <param name="Absent">Objects that were not present.</param>
public sealed record DeleteObjectsResultMessage(
    int Deleted,
    int Absent) : IWireMessage<DeleteObjectsResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.DeleteObjectsResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteVarInt32(Deleted);
        writer.WriteVarInt32(Absent);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static DeleteObjectsResultMessage Read(ref WireReader reader)
    {
        int deleted = reader.ReadVarInt32();
        int absent = reader.ReadVarInt32();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new DeleteObjectsResultMessage(deleted, absent) { Extensions = extensions };
    }
}

/// <summary>
/// Anti-entropy inventory (storage node → volume leader), paged. The leader updates its object-location map,
/// schedules repair for deficits, and after the last page answers which reported objects are deletable garbage.
/// </summary>
/// <param name="Node">Reporting node.</param>
/// <param name="Incarnation">Reporting node incarnation (stale incarnations are rejected).</param>
/// <param name="Volume">Volume whose objects are reported.</param>
/// <param name="ReportId">Identifier of this full report (all pages share it).</param>
/// <param name="Page">Page index, starting at 0.</param>
/// <param name="LastPage">True on the final page.</param>
/// <param name="Items">Objects on this page.</param>
public sealed record InventoryReportMessage(
    NodeId Node,
    ulong Incarnation,
    VolumeId Volume,
    ulong ReportId,
    int Page,
    bool LastPage,
    IReadOnlyList<InventoryItem> Items) : IWireMessage<InventoryReportMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.InventoryReport;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteNodeId(Node);
        writer.WriteVarUInt(Incarnation);
        writer.WriteVolumeId(Volume);
        writer.WriteVarUInt(ReportId);
        writer.WriteVarInt32(Page);
        writer.WriteBool(LastPage);
        writer.WriteList(Items);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static InventoryReportMessage Read(ref WireReader reader)
    {
        NodeId node = reader.ReadNodeId();
        ulong incarnation = reader.ReadVarUInt();
        VolumeId volume = reader.ReadVolumeId();
        ulong reportId = reader.ReadVarUInt();
        int page = reader.ReadVarInt32();
        bool lastPage = reader.ReadBool();
        IReadOnlyList<InventoryItem> items = reader.ReadList<InventoryItem>();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new InventoryReportMessage(node, incarnation, volume, reportId, page, lastPage, items) { Extensions = extensions };
    }
}

/// <summary>
/// Reply to <see cref="InventoryReportMessage"/>.
/// </summary>
/// <param name="Delete">Reported objects that are garbage at <c>DecisionIndex</c> (only on the last page).</param>
/// <param name="Unknown">Reported objects the leader cannot classify yet (for example a staging id newer than its applied state); keep them.</param>
/// <param name="DecisionIndex">Applied index of the GC decision (0 when <c>Delete</c> is empty).</param>
public sealed record InventoryReportResultMessage(
    IReadOnlyList<ObjectId> Delete,
    IReadOnlyList<ObjectId> Unknown,
    ulong DecisionIndex) : IWireMessage<InventoryReportResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.InventoryReportResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteObjectIdList(Delete);
        writer.WriteObjectIdList(Unknown);
        writer.WriteVarUInt(DecisionIndex);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static InventoryReportResultMessage Read(ref WireReader reader)
    {
        IReadOnlyList<ObjectId> delete = reader.ReadObjectIdList();
        IReadOnlyList<ObjectId> unknown = reader.ReadObjectIdList();
        ulong decisionIndex = reader.ReadVarUInt();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new InventoryReportResultMessage(delete, unknown, decisionIndex) { Extensions = extensions };
    }
}

/// <summary>
/// Server-side object copy for restore (restore coordinator → storage node holding the sources): each source object
/// is copied locally to a new id owned by the target volume and stored durably. Idempotent per (from, to).
/// </summary>
/// <param name="SourceVolume">Volume owning the source objects.</param>
/// <param name="TargetVolume">Volume that will own the copies.</param>
/// <param name="TargetStaging">Staging session of the target volume the copies are stored under.</param>
/// <param name="PlacementEpoch">Placement epoch of the copies.</param>
/// <param name="Copies">Copies to make.</param>
public sealed record CopyObjectsMessage(
    VolumeId SourceVolume,
    VolumeId TargetVolume,
    StagingId TargetStaging,
    ulong PlacementEpoch,
    IReadOnlyList<ObjectCopy> Copies) : IWireMessage<CopyObjectsMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.CopyObjects;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteVolumeId(SourceVolume);
        writer.WriteVolumeId(TargetVolume);
        writer.WriteStagingId(TargetStaging);
        writer.WriteVarUInt(PlacementEpoch);
        writer.WriteList(Copies);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static CopyObjectsMessage Read(ref WireReader reader)
    {
        VolumeId sourceVolume = reader.ReadVolumeId();
        VolumeId targetVolume = reader.ReadVolumeId();
        StagingId targetStaging = reader.ReadStagingId();
        ulong placementEpoch = reader.ReadVarUInt();
        IReadOnlyList<ObjectCopy> copies = reader.ReadList<ObjectCopy>();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new CopyObjectsMessage(sourceVolume, targetVolume, targetStaging, placementEpoch, copies) { Extensions = extensions };
    }
}

/// <summary>
/// Reply to <see cref="CopyObjectsMessage"/>.
/// </summary>
/// <param name="Receipts">Receipts of the stored copies.</param>
/// <param name="MissingSources">Source objects this node does not hold.</param>
public sealed record CopyObjectsResultMessage(
    IReadOnlyList<ObjectReceipt> Receipts,
    IReadOnlyList<ObjectId> MissingSources) : IWireMessage<CopyObjectsResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.CopyObjectsResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteList(Receipts);
        writer.WriteObjectIdList(MissingSources);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static CopyObjectsResultMessage Read(ref WireReader reader)
    {
        IReadOnlyList<ObjectReceipt> receipts = reader.ReadList<ObjectReceipt>();
        IReadOnlyList<ObjectId> missingSources = reader.ReadObjectIdList();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new CopyObjectsResultMessage(receipts, missingSources) { Extensions = extensions };
    }
}
