namespace Tedd.Tdfs.Protocol.Messages;

/// <summary>Metadata of a file or directory (FSD NS-04), used by open, get-info and create-directory replies.</summary>
/// <param name="Path">Normalized path the entry was resolved through (empty for a detached inode reached through a handle).</param>
/// <param name="Kind">Entry kind.</param>
/// <param name="Inode">Inode identity.</param>
/// <param name="Revision">Revision (log index of the last modification).</param>
/// <param name="Length">File length (0 for directories).</param>
/// <param name="CreatedUtc">Creation time (leader wall clock carried in the log entry).</param>
/// <param name="ModifiedUtc">Last modification time (leader wall clock carried in the log entry).</param>
/// <param name="Owner">Owning principal.</param>
/// <param name="OwnerOnly">Inode ACL: only the owner may access.</param>
/// <param name="ReadOnlyForOthers">Inode ACL: principals other than the owner may only read.</param>
/// <param name="StoragePolicy">Effective storage policy name (empty = the volume default).</param>
/// <param name="IsInline">True when the contents are stored inline in metadata.</param>
/// <param name="IsSparse">True when the file has holes (ranges without extents below its length).</param>
/// <param name="ExtentCount">Number of extents (0 for inline files and directories).</param>
/// <param name="EntryCount">Number of children (directories only).</param>
/// <param name="WriterGeneration">Current writer fencing generation of the inode (files only).</param>
public sealed record FileInfoData(
    string Path,
    WireEntryKind Kind,
    InodeId Inode,
    ulong Revision,
    long Length,
    DateTimeOffset CreatedUtc,
    DateTimeOffset ModifiedUtc,
    string Owner,
    bool OwnerOnly,
    bool ReadOnlyForOthers,
    string StoragePolicy,
    bool IsInline,
    bool IsSparse,
    int ExtentCount,
    int EntryCount,
    ulong WriterGeneration) : IWireRecord<FileInfoData>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 17;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteString(Path);
        writer.WriteDefinedEnum(Kind);
        writer.WriteInodeId(Inode);
        writer.WriteVarUInt(Revision);
        writer.WriteVarInt(Length);
        writer.WriteDateTimeOffset(CreatedUtc);
        writer.WriteDateTimeOffset(ModifiedUtc);
        writer.WriteString(Owner);
        writer.WriteBool(OwnerOnly);
        writer.WriteBool(ReadOnlyForOthers);
        writer.WriteString(StoragePolicy);
        writer.WriteBool(IsInline);
        writer.WriteBool(IsSparse);
        writer.WriteVarInt32(ExtentCount);
        writer.WriteVarInt32(EntryCount);
        writer.WriteVarUInt(WriterGeneration);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static FileInfoData Read(ref WireReader reader)
    {
        string path = reader.ReadString();
        WireEntryKind kind = reader.ReadEnum<WireEntryKind>();
        InodeId inode = reader.ReadInodeId();
        ulong revision = reader.ReadVarUInt();
        long length = reader.ReadVarInt();
        DateTimeOffset created = reader.ReadDateTimeOffset();
        DateTimeOffset modified = reader.ReadDateTimeOffset();
        string owner = reader.ReadString();
        bool ownerOnly = reader.ReadBool();
        bool readOnlyForOthers = reader.ReadBool();
        string policy = reader.ReadString();
        bool isInline = reader.ReadBool();
        bool isSparse = reader.ReadBool();
        int extentCount = reader.ReadVarInt32();
        int entryCount = reader.ReadVarInt32();
        ulong generation = reader.ReadVarUInt();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new FileInfoData(path, kind, inode, revision, length, created, modified, owner, ownerOnly, readOnlyForOthers, policy,
            isInline, isSparse, extentCount, entryCount, generation)
        {
            Extensions = extensions,
        };
    }
}

/// <summary>One entry of a directory listing page.</summary>
/// <param name="Name">Entry name (NFC, one component).</param>
/// <param name="Kind">Entry kind.</param>
/// <param name="Inode">Inode identity.</param>
/// <param name="Revision">Revision of the entry's inode.</param>
/// <param name="Length">File length (0 for directories).</param>
/// <param name="ModifiedUtc">Last modification time.</param>
public sealed record DirectoryEntryData(
    string Name,
    WireEntryKind Kind,
    InodeId Inode,
    ulong Revision,
    long Length,
    DateTimeOffset ModifiedUtc) : IWireRecord<DirectoryEntryData>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 7;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteString(Name);
        writer.WriteDefinedEnum(Kind);
        writer.WriteInodeId(Inode);
        writer.WriteVarUInt(Revision);
        writer.WriteVarInt(Length);
        writer.WriteDateTimeOffset(ModifiedUtc);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static DirectoryEntryData Read(ref WireReader reader)
    {
        string name = reader.ReadString();
        WireEntryKind kind = reader.ReadEnum<WireEntryKind>();
        InodeId inode = reader.ReadInodeId();
        ulong revision = reader.ReadVarUInt();
        long length = reader.ReadVarInt();
        DateTimeOffset modified = reader.ReadDateTimeOffset();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new DirectoryEntryData(name, kind, inode, revision, length, modified) { Extensions = extensions };
    }
}

/// <summary>
/// One extent of a committed file revision as returned by reads: logical range <c>[Offset, Offset + Length)</c> maps to
/// object bytes <c>[ObjectOffset, ObjectOffset + Length)</c>. Ranges below the file length without an extent are holes
/// (zeroes).
/// </summary>
/// <param name="Offset">Logical file offset.</param>
/// <param name="Length">Logical length.</param>
/// <param name="Object">Object holding the bytes.</param>
/// <param name="ObjectOffset">Offset of the extent's first byte inside the object.</param>
/// <param name="ObjectLength">Full plaintext length of the object (needed to verify <paramref name="Checksum"/>).</param>
/// <param name="Checksum">XxHash128 of the whole object plaintext.</param>
/// <param name="Encoding">Object encoding (<see cref="ObjectEncodings"/>).</param>
/// <param name="PlacementEpoch">Placement epoch the object was placed under (HRW targets are computed from it).</param>
/// <param name="Locations">Nodes known to hold verified copies (tried first), most recent knowledge first.</param>
public sealed record ExtentData(
    long Offset,
    long Length,
    ObjectId Object,
    long ObjectOffset,
    long ObjectLength,
    UInt128 Checksum,
    byte Encoding,
    ulong PlacementEpoch,
    IReadOnlyList<NodeId> Locations) : IWireRecord<ExtentData>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 1 + 1 + 16 + 1 + 1 + 16 + 1 + 1 + 1 + 1;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteVarInt(Offset);
        writer.WriteVarInt(Length);
        writer.WriteObjectId(Object);
        writer.WriteVarInt(ObjectOffset);
        writer.WriteVarInt(ObjectLength);
        writer.WriteUInt128(Checksum);
        writer.WriteByte(Encoding);
        writer.WriteVarUInt(PlacementEpoch);
        writer.WriteNodeIdList(Locations);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static ExtentData Read(ref WireReader reader)
    {
        long offset = reader.ReadVarInt();
        long length = reader.ReadVarInt();
        ObjectId obj = reader.ReadObjectId();
        long objectOffset = reader.ReadVarInt();
        long objectLength = reader.ReadVarInt();
        UInt128 checksum = reader.ReadUInt128();
        byte encoding = reader.ReadByte();
        ulong placementEpoch = reader.ReadVarUInt();
        IReadOnlyList<NodeId> locations = reader.ReadNodeIdList();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new ExtentData(offset, length, obj, objectOffset, objectLength, checksum, encoding, placementEpoch, locations)
        {
            Extensions = extensions,
        };
    }
}

/// <summary>
/// One change of a publication (<c>CommitFile</c>, <c>WriteAll</c>, <c>Append</c>). Every field is always encoded;
/// fields irrelevant to <see cref="Kind"/> must be zero/empty.
/// </summary>
/// <param name="Offset">Logical offset (absolute, except in <c>Append</c> where it is relative to the old end of file).</param>
/// <param name="Length">Logical length of the changed range (&gt; 0).</param>
/// <param name="Kind">Change kind.</param>
/// <param name="Object">Object holding the bytes (<see cref="RangeChangeKind.Data"/>).</param>
/// <param name="ObjectOffset">Offset of the range's first byte inside the object (<see cref="RangeChangeKind.Data"/>).</param>
/// <param name="ObjectLength">Full plaintext length of the object (<see cref="RangeChangeKind.Data"/>).</param>
/// <param name="Checksum">XxHash128 of the whole object plaintext (<see cref="RangeChangeKind.Data"/>).</param>
/// <param name="Encoding">Object encoding (<see cref="RangeChangeKind.Data"/>).</param>
/// <param name="InlineData">The bytes of the range (<see cref="RangeChangeKind.Inline"/>; length must equal <paramref name="Length"/>).</param>
public sealed record RangeChange(
    long Offset,
    long Length,
    RangeChangeKind Kind,
    ObjectId Object,
    long ObjectOffset,
    long ObjectLength,
    UInt128 Checksum,
    byte Encoding,
    ReadOnlyMemory<byte> InlineData) : IWireRecord<RangeChange>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 1 + 1 + 1 + 16 + 1 + 1 + 16 + 1 + 1 + 1;

    /// <summary>Creates a <see cref="RangeChangeKind.Data"/> change.</summary>
    public static RangeChange ForData(long offset, long length, ObjectId obj, long objectOffset, long objectLength, UInt128 checksum, byte encoding = ObjectEncodings.Plain)
        => new(offset, length, RangeChangeKind.Data, obj, objectOffset, objectLength, checksum, encoding, ReadOnlyMemory<byte>.Empty);

    /// <summary>Creates a <see cref="RangeChangeKind.Zero"/> change (hole).</summary>
    public static RangeChange ForZero(long offset, long length)
        => new(offset, length, RangeChangeKind.Zero, default, 0, 0, UInt128.Zero, ObjectEncodings.Plain, ReadOnlyMemory<byte>.Empty);

    /// <summary>Creates a <see cref="RangeChangeKind.Inline"/> change carrying <paramref name="data"/>.</summary>
    public static RangeChange ForInline(long offset, ReadOnlyMemory<byte> data)
        => new(offset, data.Length, RangeChangeKind.Inline, default, 0, 0, UInt128.Zero, ObjectEncodings.Plain, data);

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteVarInt(Offset);
        writer.WriteVarInt(Length);
        writer.WriteDefinedEnum(Kind);
        writer.WriteObjectId(Object);
        writer.WriteVarInt(ObjectOffset);
        writer.WriteVarInt(ObjectLength);
        writer.WriteUInt128(Checksum);
        writer.WriteByte(Encoding);
        writer.WriteBytes(InlineData.Span);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static RangeChange Read(ref WireReader reader)
    {
        long offset = reader.ReadVarInt();
        long length = reader.ReadVarInt();
        RangeChangeKind kind = reader.ReadEnum<RangeChangeKind>();
        ObjectId obj = reader.ReadObjectId();
        long objectOffset = reader.ReadVarInt();
        long objectLength = reader.ReadVarInt();
        UInt128 checksum = reader.ReadUInt128();
        byte encoding = reader.ReadByte();
        ReadOnlyMemory<byte> inline = reader.ReadBytesCopy();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new RangeChange(offset, length, kind, obj, objectOffset, objectLength, checksum, encoding, inline) { Extensions = extensions };
    }
}
