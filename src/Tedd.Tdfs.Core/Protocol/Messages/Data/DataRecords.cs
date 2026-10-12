namespace Tedd.Tdfs.Protocol.Messages;

/// <summary>
/// Answer for one object of a <see cref="LocateObjectMessage"/>.
/// </summary>
/// <param name="Object">Object asked about.</param>
/// <param name="Present">True when this node holds a verified copy.</param>
/// <param name="Length">Plaintext length of the held copy (0 when absent).</param>
/// <param name="Checksum">Checksum of the held copy (zero when absent).</param>
/// <param name="Staging">Staging session the copy was stored under (empty when absent).</param>
public sealed record ObjectLocation(
    ObjectId Object,
    bool Present,
    long Length,
    UInt128 Checksum,
    StagingId Staging) : IWireRecord<ObjectLocation>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 36;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteObjectId(Object);
        writer.WriteBool(Present);
        writer.WriteVarInt(Length);
        writer.WriteUInt128(Checksum);
        writer.WriteStagingId(Staging);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static ObjectLocation Read(ref WireReader reader)
    {
        ObjectId @object = reader.ReadObjectId();
        bool present = reader.ReadBool();
        long length = reader.ReadVarInt();
        UInt128 checksum = reader.ReadUInt128();
        StagingId staging = reader.ReadStagingId();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new ObjectLocation(@object, present, length, checksum, staging) { Extensions = extensions };
    }
}

/// <summary>
/// One stored object reported in an <see cref="InventoryReportMessage"/>.
/// </summary>
/// <param name="Object">Stored object.</param>
/// <param name="Staging">Staging session it was uploaded under.</param>
/// <param name="Length">Plaintext length.</param>
/// <param name="Checksum">XxHash128 of the plaintext.</param>
/// <param name="Encoding">Object encoding.</param>
public sealed record InventoryItem(
    ObjectId Object,
    StagingId Staging,
    long Length,
    UInt128 Checksum,
    byte Encoding) : IWireRecord<InventoryItem>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 36;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteObjectId(Object);
        writer.WriteStagingId(Staging);
        writer.WriteVarInt(Length);
        writer.WriteUInt128(Checksum);
        writer.WriteByte(Encoding);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static InventoryItem Read(ref WireReader reader)
    {
        ObjectId @object = reader.ReadObjectId();
        StagingId staging = reader.ReadStagingId();
        long length = reader.ReadVarInt();
        UInt128 checksum = reader.ReadUInt128();
        byte encoding = reader.ReadByte();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new InventoryItem(@object, staging, length, checksum, encoding) { Extensions = extensions };
    }
}

/// <summary>
/// A stored copy identified by object and staging session: the unit of garbage-collection decisions (ARCHITECTURE §8,
/// GC-005). Deletability is decided for an (object, staging) pair whose staging session is closed, so a storage node
/// deletes a copy only while its stored record still carries <paramref name="Staging"/>; a copy that a later upload
/// re-bound to another (possibly open, possibly committed) staging session is kept.
/// </summary>
/// <param name="Object">Stored object.</param>
/// <param name="Staging">Staging session the decision was taken for.</param>
public sealed record ObjectStagingRef(
    ObjectId Object,
    StagingId Staging) : IWireRecord<ObjectStagingRef>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 18;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteObjectId(Object);
        writer.WriteStagingId(Staging);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static ObjectStagingRef Read(ref WireReader reader)
    {
        ObjectId @object = reader.ReadObjectId();
        StagingId staging = reader.ReadStagingId();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new ObjectStagingRef(@object, staging) { Extensions = extensions };
    }
}

/// <summary>
/// One server-side object copy of a <see cref="CopyObjectsMessage"/>.
/// </summary>
/// <param name="From">Source object (owned by the source volume).</param>
/// <param name="To">New object id (owned by the target volume).</param>
public sealed record ObjectCopy(
    ObjectId From,
    ObjectId To) : IWireRecord<ObjectCopy>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 33;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteObjectId(From);
        writer.WriteObjectId(To);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static ObjectCopy Read(ref WireReader reader)
    {
        ObjectId from = reader.ReadObjectId();
        ObjectId to = reader.ReadObjectId();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new ObjectCopy(from, to) { Extensions = extensions };
    }
}
