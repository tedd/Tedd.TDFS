namespace Tedd.Tdfs.Protocol.Messages;

/// <summary>
/// Opens a handle on a file (client → volume leader; one committed <c>OpenHandle</c> entry). Performs path
/// resolution, permission check, <see cref="FileMode"/> semantics (create/truncate), sharing checks against every
/// active handle in both directions, writer exclusivity, and allocates the handle with a lease (and, for
/// version-pinned reads, a pin). Writable opens increment the inode writer generation. Creating/truncating opens must
/// carry an operation id; retries return the stored outcome. Transaction-bound opens (header transaction set) are
/// private to the transaction. Errors: FileNotFound, DirectoryNotFound, AlreadyExists, IsADirectory, AccessDenied,
/// SharingViolation, WriterConflict, InvalidArgument (unknown mode/access/share/options bits, Inheritable, writable
/// handle granting FileShare.Write, VersionPinned on a writable handle), InvalidPath, LimitExceeded (handles per
/// session).
/// </summary>
/// <param name="Header">Request header; <see cref="RequestHeader.Operation"/> identifies the mutation (retries reuse it).</param>
/// <param name="Path">Path to open (re-validated and NFC-normalized by the server).</param>
/// <param name="Mode">Open mode (validated by the server).</param>
/// <param name="Access">Requested access (validated by the server).</param>
/// <param name="Share">Sharing granted to other handles (validated by the server).</param>
/// <param name="Options">File options; anything but None/Asynchronous/SequentialScan/RandomAccess is rejected.</param>
/// <param name="VersionPinned">True for a version-pinned read handle: all reads use the revision current at open.</param>
/// <param name="WriteThrough">Informational: the client publishes on every write.</param>
/// <param name="StoragePolicy">Named storage policy for a newly created file; null = the volume default.</param>
public sealed record OpenHandleMessage(
    RequestHeader Header,
    string Path,
    FileMode Mode,
    FileAccess Access,
    FileShare Share,
    FileOptions Options,
    bool VersionPinned,
    bool WriteThrough,
    string? StoragePolicy) : IWireMessage<OpenHandleMessage>, IVolumeRequest
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.OpenHandle;

    /// <inheritdoc />
    public IVolumeRequest WithHeader(RequestHeader header) => this with { Header = header };

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Header);
        writer.WriteString(Path);
        writer.WriteEnum(Mode);
        writer.WriteEnum(Access);
        writer.WriteEnum(Share);
        writer.WriteEnum(Options);
        writer.WriteBool(VersionPinned);
        writer.WriteBool(WriteThrough);
        writer.WriteOptionalString(StoragePolicy);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static OpenHandleMessage Read(ref WireReader reader)
    {
        RequestHeader header = reader.ReadRecord<RequestHeader>();
        string path = reader.ReadString();
        FileMode mode = reader.ReadEnumRaw<FileMode>();
        FileAccess access = reader.ReadEnumRaw<FileAccess>();
        FileShare share = reader.ReadEnumRaw<FileShare>();
        FileOptions options = reader.ReadEnumRaw<FileOptions>();
        bool versionPinned = reader.ReadBool();
        bool writeThrough = reader.ReadBool();
        string? storagePolicy = reader.ReadOptionalString();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new OpenHandleMessage(header, path, mode, access, share, options, versionPinned, writeThrough, storagePolicy) { Extensions = extensions };
    }
}

/// <summary>
/// Reply to <see cref="OpenHandleMessage"/>.
/// </summary>
/// <param name="Handle">The new handle.</param>
/// <param name="Info">Metadata of the opened file at open time (inode, revision, length, ...).</param>
/// <param name="WriterGeneration">Writer generation bound to the handle (0 for read-only handles); every publication through the handle must carry it.</param>
/// <param name="PinIndex">GC-protection index of the handle (the revision pinned for version-pinned handles).</param>
/// <param name="LeaseDuration">Lease duration; renew with <c>KeepAlive</c>.</param>
/// <param name="Created">True when the open created the file.</param>
/// <param name="Truncated">True when the open truncated the file.</param>
/// <param name="Token">Commit token of the decision (<see cref="CommitToken.None"/> when transaction-bound: nothing is committed before <c>TxCommit</c>).</param>
public sealed record OpenHandleResultMessage(
    HandleId Handle,
    FileInfoData Info,
    ulong WriterGeneration,
    ulong PinIndex,
    TimeSpan LeaseDuration,
    bool Created,
    bool Truncated,
    CommitToken Token) : IWireMessage<OpenHandleResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.OpenHandleResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteHandleId(Handle);
        writer.WriteRecord(Info);
        writer.WriteVarUInt(WriterGeneration);
        writer.WriteVarUInt(PinIndex);
        writer.WriteTimeSpan(LeaseDuration);
        writer.WriteBool(Created);
        writer.WriteBool(Truncated);
        writer.WriteCommitToken(Token);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static OpenHandleResultMessage Read(ref WireReader reader)
    {
        HandleId handle = reader.ReadHandleId();
        FileInfoData info = reader.ReadRecord<FileInfoData>();
        ulong writerGeneration = reader.ReadVarUInt();
        ulong pinIndex = reader.ReadVarUInt();
        TimeSpan leaseDuration = reader.ReadTimeSpan();
        bool created = reader.ReadBool();
        bool truncated = reader.ReadBool();
        CommitToken token = reader.ReadCommitToken();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new OpenHandleResultMessage(handle, info, writerGeneration, pinIndex, leaseDuration, created, truncated, token) { Extensions = extensions };
    }
}

/// <summary>
/// Closes a handle (client → volume leader; committed <c>CloseHandle</c> entry): releases its sharing reservation,
/// writer authority, pin and bound staging session. Staged but unpublished data is discarded. Idempotent: closing an
/// unknown or already closed handle succeeds with <c>WasOpen = false</c>.
/// </summary>
/// <param name="Header">Request header.</param>
/// <param name="Handle">Handle to close.</param>
/// <param name="Generation">Writer generation of the handle (0 for read-only handles); a mismatch is ignored for closing but reported via <c>WasOpen = false</c>.</param>
public sealed record CloseHandleMessage(
    RequestHeader Header,
    HandleId Handle,
    ulong Generation) : IWireMessage<CloseHandleMessage>, IVolumeRequest
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.CloseHandle;

    /// <inheritdoc />
    public IVolumeRequest WithHeader(RequestHeader header) => this with { Header = header };

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Header);
        writer.WriteHandleId(Handle);
        writer.WriteVarUInt(Generation);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static CloseHandleMessage Read(ref WireReader reader)
    {
        RequestHeader header = reader.ReadRecord<RequestHeader>();
        HandleId handle = reader.ReadHandleId();
        ulong generation = reader.ReadVarUInt();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new CloseHandleMessage(header, handle, generation) { Extensions = extensions };
    }
}

/// <summary>
/// Reply to <see cref="CloseHandleMessage"/>.
/// </summary>
/// <param name="WasOpen">True when the handle was active and is now closed.</param>
/// <param name="Token">Commit of the close entry (<see cref="CommitToken.None"/> when nothing was proposed).</param>
public sealed record CloseHandleResultMessage(
    bool WasOpen,
    CommitToken Token) : IWireMessage<CloseHandleResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.CloseHandleResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteBool(WasOpen);
        writer.WriteCommitToken(Token);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static CloseHandleResultMessage Read(ref WireReader reader)
    {
        bool wasOpen = reader.ReadBool();
        CommitToken token = reader.ReadCommitToken();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new CloseHandleResultMessage(wasOpen, token) { Extensions = extensions };
    }
}

/// <summary>
/// Resolves the committed bytes of a file range (client → volume leader, or backup node for snapshots). Exactly one
/// addressing mode: a handle (live reads run a ReadIndex barrier; version-pinned and transaction-bound handles read
/// their fixed view), or <c>(SnapshotPin, Inode)</c> for snapshot views. Live reads by inode without a handle are
/// rejected (they would bypass sharing). One call uses exactly one revision (STR-014). The reply carries inline bytes
/// or the extents covering the range plus a read capability for those objects; the client fetches object bytes from
/// storage nodes and verifies checksums. Errors: InvalidHandle, LeaseLost, PinExpired, InvalidArgument (negative
/// offset/length, offset + length overflow), Integrity.
/// </summary>
/// <param name="Header">Request header.</param>
/// <param name="Handle">Handle to read through (empty when reading a snapshot by inode).</param>
/// <param name="SnapshotPin">Snapshot reader pin (from <c>OpenSnapshot</c>); 0 = none.</param>
/// <param name="Inode">Inode to read when addressing by snapshot pin; empty otherwise.</param>
/// <param name="Offset">First byte to read.</param>
/// <param name="Length">Number of bytes requested.</param>
public sealed record ReadRangeMessage(
    RequestHeader Header,
    HandleId Handle,
    ulong SnapshotPin,
    InodeId Inode,
    long Offset,
    long Length) : IWireMessage<ReadRangeMessage>, IVolumeRequest
{
    /// <summary>
    /// Named stream to read when addressing a snapshot by <c>(SnapshotPin, Inode)</c> (Addendum A, ADS-020; trailing field):
    /// null = the default stream. Must be null for handle reads (a handle is bound to its stream).
    /// </summary>
    public string? Stream { get; init; }

    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.ReadRange;

    /// <inheritdoc />
    public IVolumeRequest WithHeader(RequestHeader header) => this with { Header = header };

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Header);
        writer.WriteHandleId(Handle);
        writer.WriteVarUInt(SnapshotPin);
        writer.WriteInodeId(Inode);
        writer.WriteVarInt(Offset);
        writer.WriteVarInt(Length);
        writer.WriteOptionalString(Stream);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static ReadRangeMessage Read(ref WireReader reader)
    {
        RequestHeader header = reader.ReadRecord<RequestHeader>();
        HandleId handle = reader.ReadHandleId();
        ulong snapshotPin = reader.ReadVarUInt();
        InodeId inode = reader.ReadInodeId();
        long offset = reader.ReadVarInt();
        long length = reader.ReadVarInt();
        string? stream = reader.ReadOptionalString();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new ReadRangeMessage(header, handle, snapshotPin, inode, offset, length) { Stream = stream, Extensions = extensions };
    }
}

/// <summary>
/// Reply to <see cref="ReadRangeMessage"/>. The covered range is <c>[Offset, Offset + Length)</c>, clipped to the
/// file length and possibly shorter than requested (bounded extent count); a zero <c>Length</c> at <c>Offset</c>
/// &gt;= <c>FileLength</c> is end of file. Bytes of the covered range not covered by an extent are holes (zeroes).
/// </summary>
/// <param name="Revision">Revision the read was served from.</param>
/// <param name="FileLength">File length at that revision.</param>
/// <param name="Offset">Start of the covered range (equals the requested offset).</param>
/// <param name="Length">Length of the covered range.</param>
/// <param name="IsInline">True when the file is stored inline; <c>InlineData</c> then holds the covered bytes.</param>
/// <param name="InlineData">Covered bytes of an inline file (empty otherwise).</param>
/// <param name="Extents">Extents intersecting the covered range, in ascending offset order (whole extents; the client clips).</param>
/// <param name="ReadCapability">Encoded <see cref="DataCapability"/> allowing <c>GetObject</c> on exactly the listed objects (empty for inline files).</param>
/// <param name="CapabilityExpiry">Expiry of the read capability.</param>
/// <param name="ObservedAt">Commit position the read observed (for live reads: at or after the ReadIndex point).</param>
public sealed record ReadRangeResultMessage(
    ulong Revision,
    long FileLength,
    long Offset,
    long Length,
    bool IsInline,
    ReadOnlyMemory<byte> InlineData,
    IReadOnlyList<ExtentData> Extents,
    ReadOnlyMemory<byte> ReadCapability,
    DateTimeOffset CapabilityExpiry,
    CommitToken ObservedAt) : IWireMessage<ReadRangeResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.ReadRangeResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteVarUInt(Revision);
        writer.WriteVarInt(FileLength);
        writer.WriteVarInt(Offset);
        writer.WriteVarInt(Length);
        writer.WriteBool(IsInline);
        writer.WriteBytes(InlineData.Span);
        writer.WriteList(Extents);
        writer.WriteBytes(ReadCapability.Span);
        writer.WriteDateTimeOffset(CapabilityExpiry);
        writer.WriteCommitToken(ObservedAt);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static ReadRangeResultMessage Read(ref WireReader reader)
    {
        ulong revision = reader.ReadVarUInt();
        long fileLength = reader.ReadVarInt();
        long offset = reader.ReadVarInt();
        long length = reader.ReadVarInt();
        bool isInline = reader.ReadBool();
        ReadOnlyMemory<byte> inlineData = reader.ReadBytesCopy();
        IReadOnlyList<ExtentData> extents = reader.ReadList<ExtentData>();
        ReadOnlyMemory<byte> readCapability = reader.ReadBytesCopy();
        DateTimeOffset capabilityExpiry = reader.ReadDateTimeOffset();
        CommitToken observedAt = reader.ReadCommitToken();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new ReadRangeResultMessage(revision, fileLength, offset, length, isInline, inlineData, extents, readCapability, capabilityExpiry, observedAt) { Extensions = extensions };
    }
}

/// <summary>
/// Returns metadata of a path, of the inode behind a handle, or of a path inside a snapshot (client → volume leader
/// or backup node; ReadIndex read). Transaction-bound requests read the transaction view and record the read (or the
/// absent name) for conflict validation. Absence is not an error: <c>Exists = false</c> (FSD §5.4); authorization,
/// availability and integrity failures are errors.
/// </summary>
/// <param name="Header">Request header.</param>
/// <param name="Path">Path to describe (ignored when <c>Handle</c> is set).</param>
/// <param name="Handle">Handle whose inode to describe (live view for live handles, the pinned revision for version-pinned handles); empty = by path.</param>
/// <param name="SnapshotPin">Resolve <c>Path</c> inside the snapshot held by this reader pin; 0 = live (or transaction) view.</param>
public sealed record GetInfoMessage(
    RequestHeader Header,
    string Path,
    HandleId Handle,
    ulong SnapshotPin) : IWireMessage<GetInfoMessage>, IVolumeRequest
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.GetInfo;

    /// <inheritdoc />
    public IVolumeRequest WithHeader(RequestHeader header) => this with { Header = header };

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Header);
        writer.WriteString(Path);
        writer.WriteHandleId(Handle);
        writer.WriteVarUInt(SnapshotPin);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static GetInfoMessage Read(ref WireReader reader)
    {
        RequestHeader header = reader.ReadRecord<RequestHeader>();
        string path = reader.ReadString();
        HandleId handle = reader.ReadHandleId();
        ulong snapshotPin = reader.ReadVarUInt();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new GetInfoMessage(header, path, handle, snapshotPin) { Extensions = extensions };
    }
}

/// <summary>
/// Reply to <see cref="GetInfoMessage"/>.
/// </summary>
/// <param name="Exists">False only for genuine absence (missing entry or missing/non-directory ancestor).</param>
/// <param name="Info">Metadata when the entry exists.</param>
/// <param name="ObservedAt">Commit position the answer is based on.</param>
public sealed record GetInfoResultMessage(
    bool Exists,
    FileInfoData? Info,
    CommitToken ObservedAt) : IWireMessage<GetInfoResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.GetInfoResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteBool(Exists);
        writer.WriteOptional(Info);
        writer.WriteCommitToken(ObservedAt);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static GetInfoResultMessage Read(ref WireReader reader)
    {
        bool exists = reader.ReadBool();
        FileInfoData? info = reader.ReadOptional<FileInfoData>();
        CommitToken observedAt = reader.ReadCommitToken();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new GetInfoResultMessage(exists, info, observedAt) { Extensions = extensions };
    }
}

/// <summary>
/// Lists one page of a directory at a pinned namespace version (client → volume leader or backup node). The first
/// page pins the directory version (an enumeration pin, renewed by <c>KeepAlive</c> and by subsequent pages); the
/// opaque continuation binds volume, epoch, pin and position, and an expired or foreign continuation fails with <see
/// cref="DfsErrorCode.ContinuationExpired"/> instead of restarting against a newer version (NS-07). Transaction-bound
/// listings record the enumeration predicate. Errors: DirectoryNotFound, NotADirectory, ContinuationExpired,
/// InvalidArgument (page size outside 1..10000, empty kinds, malformed pattern).
/// </summary>
/// <param name="Header">Request header.</param>
/// <param name="Path">Directory path.</param>
/// <param name="Pattern">Name pattern with <c>*</c> and <c>?</c> wildcards (ordinal).</param>
/// <param name="Kinds">Entry kinds to return.</param>
/// <param name="PageSize">Maximum entries in the page (1..10000).</param>
/// <param name="Continuation">Continuation from the previous page; null for the first page.</param>
/// <param name="SnapshotPin">List inside the snapshot held by this reader pin; 0 = live (or transaction) view.</param>
public sealed record ListDirectoryMessage(
    RequestHeader Header,
    string Path,
    string Pattern,
    WireEntryKinds Kinds,
    int PageSize,
    string? Continuation,
    ulong SnapshotPin) : IWireMessage<ListDirectoryMessage>, IVolumeRequest
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.ListDirectory;

    /// <inheritdoc />
    public IVolumeRequest WithHeader(RequestHeader header) => this with { Header = header };

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Header);
        writer.WriteString(Path);
        writer.WriteString(Pattern);
        writer.WriteEnum(Kinds);
        writer.WriteVarInt32(PageSize);
        writer.WriteOptionalString(Continuation);
        writer.WriteVarUInt(SnapshotPin);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static ListDirectoryMessage Read(ref WireReader reader)
    {
        RequestHeader header = reader.ReadRecord<RequestHeader>();
        string path = reader.ReadString();
        string pattern = reader.ReadString();
        WireEntryKinds kinds = reader.ReadEnumRaw<WireEntryKinds>();
        int pageSize = reader.ReadVarInt32();
        string? continuation = reader.ReadOptionalString();
        ulong snapshotPin = reader.ReadVarUInt();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new ListDirectoryMessage(header, path, pattern, kinds, pageSize, continuation, snapshotPin) { Extensions = extensions };
    }
}

/// <summary>
/// Reply to <see cref="ListDirectoryMessage"/>.
/// </summary>
/// <param name="Entries">Entries of the page in ascending ordinal name order.</param>
/// <param name="Continuation">Continuation for the next page; null when the listing is complete.</param>
/// <param name="PinnedAt">Namespace version the whole listing is pinned to.</param>
/// <param name="EnumerationPin">Enumeration pin holding that version (renew it with <c>KeepAlive</c> reader pins, release it early with <c>CloseSnapshot</c>); 0 when the listing completed in this page.</param>
/// <param name="Directory">Inode of the listed directory.</param>
/// <param name="DirectoryRevision">Revision of the listed directory at the pinned version.</param>
public sealed record ListDirectoryResultMessage(
    IReadOnlyList<DirectoryEntryData> Entries,
    string? Continuation,
    CommitToken PinnedAt,
    ulong EnumerationPin,
    InodeId Directory,
    ulong DirectoryRevision) : IWireMessage<ListDirectoryResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.ListDirectoryResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteList(Entries);
        writer.WriteOptionalString(Continuation);
        writer.WriteCommitToken(PinnedAt);
        writer.WriteVarUInt(EnumerationPin);
        writer.WriteInodeId(Directory);
        writer.WriteVarUInt(DirectoryRevision);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static ListDirectoryResultMessage Read(ref WireReader reader)
    {
        IReadOnlyList<DirectoryEntryData> entries = reader.ReadList<DirectoryEntryData>();
        string? continuation = reader.ReadOptionalString();
        CommitToken pinnedAt = reader.ReadCommitToken();
        ulong enumerationPin = reader.ReadVarUInt();
        InodeId directory = reader.ReadInodeId();
        ulong directoryRevision = reader.ReadVarUInt();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new ListDirectoryResultMessage(entries, continuation, pinnedAt, enumerationPin, directory, directoryRevision) { Extensions = extensions };
    }
}

/// <summary>
/// Opens a staging session for uploads (client → volume leader; committed <c>CreateStaging</c> entry) and returns a
/// staging capability for <c>PutObject</c>. A session bound to a handle closes with the handle; an unbound session
/// belongs to the caller's session (and is closed when it expires). Setting <c>Renew</c> re-issues a capability for
/// an existing open session without a new entry. A closed staging session is the authoritative decision that its
/// unreferenced objects are garbage (ARCHITECTURE §8).
/// </summary>
/// <param name="Header">Request header; <see cref="RequestHeader.Operation"/> identifies the mutation (retries reuse it).</param>
/// <param name="Handle">Writable handle to bind the session to; empty = bound to the caller's session.</param>
/// <param name="Renew">Existing open staging session whose capability is renewed; empty = create a new session.</param>
public sealed record CreateStagingMessage(
    RequestHeader Header,
    HandleId Handle,
    StagingId Renew) : IWireMessage<CreateStagingMessage>, IVolumeRequest
{
    /// <summary>
    /// Extension field id of the <b>close</b> request (required, empty value): instead of creating or renewing, close the
    /// caller's unbound staging session named by <see cref="Renew"/> (a whole-file helper that will not publish what it
    /// staged; FILE-016). A closed staging session is the authoritative "no further publication" decision.
    /// </summary>
    public const uint CloseExtension = 1;

    private static readonly System.Collections.Frozen.FrozenSet<uint> UnderstoodExtensions =
        System.Collections.Frozen.FrozenSet.ToFrozenSet([CloseExtension]);

    /// <summary>Extensions preserved from the wire (re-emitted on encode), including <see cref="CloseExtension"/>.</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <summary>True when the request closes the staging session named by <see cref="Renew"/> (<see cref="CloseExtension"/>).</summary>
    public bool Close => Extensions.Any(e => e.FieldId == CloseExtension);

    /// <summary>A request that closes the caller's unbound staging session <paramref name="staging"/>.</summary>
    public static CreateStagingMessage CloseRequest(RequestHeader header, StagingId staging) =>
        new(header, default, staging) { Extensions = [new WireExtension(CloseExtension, Required: true, ReadOnlyMemory<byte>.Empty)] };

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.CreateStaging;

    /// <inheritdoc />
    public IVolumeRequest WithHeader(RequestHeader header) => this with { Header = header };

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Header);
        writer.WriteHandleId(Handle);
        writer.WriteStagingId(Renew);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static CreateStagingMessage Read(ref WireReader reader)
    {
        RequestHeader header = reader.ReadRecord<RequestHeader>();
        HandleId handle = reader.ReadHandleId();
        StagingId renew = reader.ReadStagingId();
        List<WireExtension>? list = null;
        reader.ReadExtensions(UnderstoodExtensions, e => (list ??= []).Add(e));
        IReadOnlyList<WireExtension> extensions = list is null ? [] : list.ToArray();
        return new CreateStagingMessage(header, handle, renew) { Extensions = extensions };
    }
}

/// <summary>
/// Reply to <see cref="CreateStagingMessage"/>.
/// </summary>
/// <param name="Staging">Staging session id.</param>
/// <param name="Capability">Encoded staging <see cref="DataCapability"/>.</param>
/// <param name="CapabilityExpiry">Capability expiry (renew before it).</param>
/// <param name="PlacementEpoch">Placement epoch to place uploads under.</param>
/// <param name="Token">Commit of the staging entry (<see cref="CommitToken.None"/> for a renewal).</param>
public sealed record CreateStagingResultMessage(
    StagingId Staging,
    ReadOnlyMemory<byte> Capability,
    DateTimeOffset CapabilityExpiry,
    ulong PlacementEpoch,
    CommitToken Token) : IWireMessage<CreateStagingResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.CreateStagingResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteStagingId(Staging);
        writer.WriteBytes(Capability.Span);
        writer.WriteDateTimeOffset(CapabilityExpiry);
        writer.WriteVarUInt(PlacementEpoch);
        writer.WriteCommitToken(Token);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static CreateStagingResultMessage Read(ref WireReader reader)
    {
        StagingId staging = reader.ReadStagingId();
        ReadOnlyMemory<byte> capability = reader.ReadBytesCopy();
        DateTimeOffset capabilityExpiry = reader.ReadDateTimeOffset();
        ulong placementEpoch = reader.ReadVarUInt();
        CommitToken token = reader.ReadCommitToken();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new CreateStagingResultMessage(staging, capability, capabilityExpiry, placementEpoch, token) { Extensions = extensions };
    }
}

/// <summary>
/// Publishes one complete new revision of a file through a writable handle (client → volume leader; committed
/// <c>CommitFile</c> entry, FSD §10.3). Before proposing, the leader validates the handle and writer generation
/// (LOCK-04), authorization, base revision, the receipts (MAC, binding, incarnation, placement epoch, durable
/// copies/zones per policy) and limits; insufficient receipts fail with PlacementUnavailable and nothing is proposed.
/// Apply order: clip at <c>TruncateTo</c> (when ≥ 0), apply <c>Changes</c> in order, set the length to
/// <c>NewLength</c> (extension creates a hole). When the result turns an inline file into an extent-based one, bytes
/// not covered by changes are taken from the old inline content. Transaction-bound commits stage into the
/// transaction. Errors: LeaseLost, InvalidHandle, WriterConflict (base revision), PlacementUnavailable,
/// LimitExceeded, InvalidArgument, AccessDenied.
/// </summary>
/// <param name="Header">Request header; <see cref="RequestHeader.Operation"/> identifies the mutation (retries reuse it).</param>
/// <param name="Handle">Writable handle.</param>
/// <param name="Generation">Writer generation bound to the handle.</param>
/// <param name="BaseRevision">Revision the staged changes were made against; 0 = do not check.</param>
/// <param name="TruncateTo">Lowest length the staged view reached since the base revision (−1 = no truncation); content beyond it is discarded before changes apply, so extension never reveals discarded bytes (STR-011).</param>
/// <param name="NewLength">File length after publication.</param>
/// <param name="Changes">Changed ranges with absolute offsets, applied in order.</param>
/// <param name="Receipts">Durability receipts for every object referenced by a Data change.</param>
/// <param name="Staging">Staging session the referenced objects were uploaded under.</param>
/// <param name="SnapshotName">Commit-and-snapshot: when set, a snapshot with this name is created in the same metadata decision as the mutation (FSD §11.2); null for none.</param>
public sealed record CommitFileMessage(
    RequestHeader Header,
    HandleId Handle,
    ulong Generation,
    ulong BaseRevision,
    long TruncateTo,
    long NewLength,
    IReadOnlyList<RangeChange> Changes,
    IReadOnlyList<ObjectReceipt> Receipts,
    StagingId Staging,
    string? SnapshotName) : IWireMessage<CommitFileMessage>, IVolumeRequest
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.CommitFile;

    /// <inheritdoc />
    public IVolumeRequest WithHeader(RequestHeader header) => this with { Header = header };

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Header);
        writer.WriteHandleId(Handle);
        writer.WriteVarUInt(Generation);
        writer.WriteVarUInt(BaseRevision);
        writer.WriteVarInt(TruncateTo);
        writer.WriteVarInt(NewLength);
        writer.WriteList(Changes);
        writer.WriteList(Receipts);
        writer.WriteStagingId(Staging);
        writer.WriteOptionalString(SnapshotName);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static CommitFileMessage Read(ref WireReader reader)
    {
        RequestHeader header = reader.ReadRecord<RequestHeader>();
        HandleId handle = reader.ReadHandleId();
        ulong generation = reader.ReadVarUInt();
        ulong baseRevision = reader.ReadVarUInt();
        long truncateTo = reader.ReadVarInt();
        long newLength = reader.ReadVarInt();
        IReadOnlyList<RangeChange> changes = reader.ReadList<RangeChange>();
        IReadOnlyList<ObjectReceipt> receipts = reader.ReadList<ObjectReceipt>();
        StagingId staging = reader.ReadStagingId();
        string? snapshotName = reader.ReadOptionalString();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new CommitFileMessage(header, handle, generation, baseRevision, truncateTo, newLength, changes, receipts, staging, snapshotName) { Extensions = extensions };
    }
}

/// <summary>
/// Reply to <see cref="CommitFileMessage"/>.
/// </summary>
/// <param name="Inode">Inode published.</param>
/// <param name="Revision">New revision (0 when transaction-bound).</param>
/// <param name="NewLength">File length after publication.</param>
/// <param name="Token">Commit token of the decision (<see cref="CommitToken.None"/> when transaction-bound: nothing is committed before <c>TxCommit</c>).</param>
/// <param name="Snapshot">The snapshot created by commit-and-snapshot, or null.</param>
public sealed record CommitFileResultMessage(
    InodeId Inode,
    ulong Revision,
    long NewLength,
    CommitToken Token,
    SnapshotInfoData? Snapshot) : IWireMessage<CommitFileResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.CommitFileResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteInodeId(Inode);
        writer.WriteVarUInt(Revision);
        writer.WriteVarInt(NewLength);
        writer.WriteCommitToken(Token);
        writer.WriteOptional(Snapshot);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static CommitFileResultMessage Read(ref WireReader reader)
    {
        InodeId inode = reader.ReadInodeId();
        ulong revision = reader.ReadVarUInt();
        long newLength = reader.ReadVarInt();
        CommitToken token = reader.ReadCommitToken();
        SnapshotInfoData? snapshot = reader.ReadOptional<SnapshotInfoData>();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new CommitFileResultMessage(inode, revision, newLength, token, snapshot) { Extensions = extensions };
    }
}

/// <summary>
/// Atomically replaces the complete contents of a file by path, creating it if absent, preserving the inode of an
/// existing file (client → volume leader; one committed <c>WriteAll</c> entry with a short implicit writer
/// reservation). The new content is <c>[0, Length)</c> described by <c>Changes</c> (uncovered ranges are holes);
/// small files use one Inline change. Receipts are validated as for <c>CommitFile</c>. Errors:
/// SharingViolation/WriterConflict (another writer holds the inode), IsADirectory, DirectoryNotFound (parent missing
/// and not <c>CreateParents</c>), PlacementUnavailable, LimitExceeded, AccessDenied.
/// </summary>
/// <param name="Header">Request header; <see cref="RequestHeader.Operation"/> identifies the mutation (retries reuse it).</param>
/// <param name="Path">Destination path.</param>
/// <param name="Length">New file length.</param>
/// <param name="Changes">Content ranges (absolute offsets).</param>
/// <param name="Receipts">Durability receipts for every referenced object.</param>
/// <param name="Staging">Staging session of the uploads (empty when all content is inline).</param>
/// <param name="StoragePolicy">Named storage policy when the file is created; null = volume default.</param>
/// <param name="SnapshotName">Commit-and-snapshot: when set, a snapshot with this name is created in the same metadata decision as the mutation (FSD §11.2); null for none.</param>
/// <param name="CreateParents">True to create missing parent directories.</param>
/// <param name="ExpectedRevision">Replace only if the current revision equals this value (0 = unconditional); mismatch → WriterConflict.</param>
public sealed record WriteAllMessage(
    RequestHeader Header,
    string Path,
    long Length,
    IReadOnlyList<RangeChange> Changes,
    IReadOnlyList<ObjectReceipt> Receipts,
    StagingId Staging,
    string? StoragePolicy,
    string? SnapshotName,
    bool CreateParents,
    ulong ExpectedRevision) : IWireMessage<WriteAllMessage>, IVolumeRequest
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.WriteAll;

    /// <inheritdoc />
    public IVolumeRequest WithHeader(RequestHeader header) => this with { Header = header };

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Header);
        writer.WriteString(Path);
        writer.WriteVarInt(Length);
        writer.WriteList(Changes);
        writer.WriteList(Receipts);
        writer.WriteStagingId(Staging);
        writer.WriteOptionalString(StoragePolicy);
        writer.WriteOptionalString(SnapshotName);
        writer.WriteBool(CreateParents);
        writer.WriteVarUInt(ExpectedRevision);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static WriteAllMessage Read(ref WireReader reader)
    {
        RequestHeader header = reader.ReadRecord<RequestHeader>();
        string path = reader.ReadString();
        long length = reader.ReadVarInt();
        IReadOnlyList<RangeChange> changes = reader.ReadList<RangeChange>();
        IReadOnlyList<ObjectReceipt> receipts = reader.ReadList<ObjectReceipt>();
        StagingId staging = reader.ReadStagingId();
        string? storagePolicy = reader.ReadOptionalString();
        string? snapshotName = reader.ReadOptionalString();
        bool createParents = reader.ReadBool();
        ulong expectedRevision = reader.ReadVarUInt();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new WriteAllMessage(header, path, length, changes, receipts, staging, storagePolicy, snapshotName, createParents, expectedRevision) { Extensions = extensions };
    }
}

/// <summary>
/// Reply to <see cref="WriteAllMessage"/>.
/// </summary>
/// <param name="Inode">Inode written (preserved when the file existed).</param>
/// <param name="Revision">New revision (0 when transaction-bound).</param>
/// <param name="Length">New length.</param>
/// <param name="Created">True when the file was created.</param>
/// <param name="Token">Commit token of the decision (<see cref="CommitToken.None"/> when transaction-bound: nothing is committed before <c>TxCommit</c>).</param>
/// <param name="Snapshot">The snapshot created by commit-and-snapshot, or null.</param>
public sealed record WriteAllResultMessage(
    InodeId Inode,
    ulong Revision,
    long Length,
    bool Created,
    CommitToken Token,
    SnapshotInfoData? Snapshot) : IWireMessage<WriteAllResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.WriteAllResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteInodeId(Inode);
        writer.WriteVarUInt(Revision);
        writer.WriteVarInt(Length);
        writer.WriteBool(Created);
        writer.WriteCommitToken(Token);
        writer.WriteOptional(Snapshot);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static WriteAllResultMessage Read(ref WireReader reader)
    {
        InodeId inode = reader.ReadInodeId();
        ulong revision = reader.ReadVarUInt();
        long length = reader.ReadVarInt();
        bool created = reader.ReadBool();
        CommitToken token = reader.ReadCommitToken();
        SnapshotInfoData? snapshot = reader.ReadOptional<SnapshotInfoData>();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new WriteAllResultMessage(inode, revision, length, created, token, snapshot) { Extensions = extensions };
    }
}

/// <summary>
/// Atomically appends one complete suffix under one operation id (client → volume leader; committed <c>Append</c>
/// entry). Change offsets are relative to the end of file at apply time, so the client needs no prior read; a retry
/// returns the stored outcome and never appends twice (OUT-006). Optionally through a writable handle (then the
/// generation is checked). Errors as for <c>WriteAll</c>, plus FileNotFound when absent and not
/// <c>CreateIfMissing</c>.
/// </summary>
/// <param name="Header">Request header; <see cref="RequestHeader.Operation"/> identifies the mutation (retries reuse it).</param>
/// <param name="Path">File path (ignored when <c>Handle</c> is set).</param>
/// <param name="Handle">Writable handle to append through; empty = by path with an implicit writer reservation.</param>
/// <param name="Generation">Writer generation of <c>Handle</c> (0 when appending by path).</param>
/// <param name="AppendLength">Number of bytes appended (the suffix is <c>[oldEnd, oldEnd + AppendLength)</c>).</param>
/// <param name="Changes">Suffix ranges with offsets relative to the old end of file.</param>
/// <param name="Receipts">Durability receipts for every referenced object.</param>
/// <param name="Staging">Staging session of the uploads (empty when all content is inline).</param>
/// <param name="CreateIfMissing">True to create the file when absent (File.AppendAllBytes semantics).</param>
/// <param name="StoragePolicy">Named storage policy when the file is created; null = volume default.</param>
/// <param name="SnapshotName">Commit-and-snapshot: when set, a snapshot with this name is created in the same metadata decision as the mutation (FSD §11.2); null for none.</param>
public sealed record AppendMessage(
    RequestHeader Header,
    string Path,
    HandleId Handle,
    ulong Generation,
    long AppendLength,
    IReadOnlyList<RangeChange> Changes,
    IReadOnlyList<ObjectReceipt> Receipts,
    StagingId Staging,
    bool CreateIfMissing,
    string? StoragePolicy,
    string? SnapshotName) : IWireMessage<AppendMessage>, IVolumeRequest
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.Append;

    /// <inheritdoc />
    public IVolumeRequest WithHeader(RequestHeader header) => this with { Header = header };

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Header);
        writer.WriteString(Path);
        writer.WriteHandleId(Handle);
        writer.WriteVarUInt(Generation);
        writer.WriteVarInt(AppendLength);
        writer.WriteList(Changes);
        writer.WriteList(Receipts);
        writer.WriteStagingId(Staging);
        writer.WriteBool(CreateIfMissing);
        writer.WriteOptionalString(StoragePolicy);
        writer.WriteOptionalString(SnapshotName);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static AppendMessage Read(ref WireReader reader)
    {
        RequestHeader header = reader.ReadRecord<RequestHeader>();
        string path = reader.ReadString();
        HandleId handle = reader.ReadHandleId();
        ulong generation = reader.ReadVarUInt();
        long appendLength = reader.ReadVarInt();
        IReadOnlyList<RangeChange> changes = reader.ReadList<RangeChange>();
        IReadOnlyList<ObjectReceipt> receipts = reader.ReadList<ObjectReceipt>();
        StagingId staging = reader.ReadStagingId();
        bool createIfMissing = reader.ReadBool();
        string? storagePolicy = reader.ReadOptionalString();
        string? snapshotName = reader.ReadOptionalString();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new AppendMessage(header, path, handle, generation, appendLength, changes, receipts, staging, createIfMissing, storagePolicy, snapshotName) { Extensions = extensions };
    }
}

/// <summary>
/// Reply to <see cref="AppendMessage"/>.
/// </summary>
/// <param name="Inode">Inode appended to.</param>
/// <param name="Revision">New revision (0 when transaction-bound).</param>
/// <param name="OldLength">Length before the append (where the suffix starts).</param>
/// <param name="NewLength">Length after the append.</param>
/// <param name="Created">True when the file was created.</param>
/// <param name="Token">Commit token of the decision (<see cref="CommitToken.None"/> when transaction-bound: nothing is committed before <c>TxCommit</c>).</param>
/// <param name="Snapshot">The snapshot created by commit-and-snapshot, or null.</param>
public sealed record AppendResultMessage(
    InodeId Inode,
    ulong Revision,
    long OldLength,
    long NewLength,
    bool Created,
    CommitToken Token,
    SnapshotInfoData? Snapshot) : IWireMessage<AppendResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.AppendResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteInodeId(Inode);
        writer.WriteVarUInt(Revision);
        writer.WriteVarInt(OldLength);
        writer.WriteVarInt(NewLength);
        writer.WriteBool(Created);
        writer.WriteCommitToken(Token);
        writer.WriteOptional(Snapshot);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static AppendResultMessage Read(ref WireReader reader)
    {
        InodeId inode = reader.ReadInodeId();
        ulong revision = reader.ReadVarUInt();
        long oldLength = reader.ReadVarInt();
        long newLength = reader.ReadVarInt();
        bool created = reader.ReadBool();
        CommitToken token = reader.ReadCommitToken();
        SnapshotInfoData? snapshot = reader.ReadOptional<SnapshotInfoData>();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new AppendResultMessage(inode, revision, oldLength, newLength, created, token, snapshot) { Extensions = extensions };
    }
}

/// <summary>
/// Copies a file inside a volume as metadata only (client → volume leader; one committed <c>Copy</c> entry): the
/// destination receives the source revision's extent map (objects are shared) or inline bytes. The source revision is
/// pinned for the whole operation; an existing destination keeps its inode identity. Errors: FileNotFound (source),
/// AlreadyExists (destination exists and not <c>Overwrite</c>), InvalidArgument (copy onto itself), SharingViolation,
/// IsADirectory, CrossVolume, AccessDenied.
/// </summary>
/// <param name="Header">Request header; <see cref="RequestHeader.Operation"/> identifies the mutation (retries reuse it).</param>
/// <param name="Source">Source file path.</param>
/// <param name="Destination">Destination file path.</param>
/// <param name="Overwrite">True to replace an existing destination file.</param>
/// <param name="ExpectedSourceRevision">Copy only if the source is at this revision (0 = current); mismatch → WriterConflict.</param>
/// <param name="StoragePolicy">Named storage policy for a newly created destination; null = the source policy.</param>
/// <param name="SnapshotName">Commit-and-snapshot: when set, a snapshot with this name is created in the same metadata decision as the mutation (FSD §11.2); null for none.</param>
public sealed record CopyMessage(
    RequestHeader Header,
    string Source,
    string Destination,
    bool Overwrite,
    ulong ExpectedSourceRevision,
    string? StoragePolicy,
    string? SnapshotName) : IWireMessage<CopyMessage>, IVolumeRequest
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.Copy;

    /// <inheritdoc />
    public IVolumeRequest WithHeader(RequestHeader header) => this with { Header = header };

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Header);
        writer.WriteString(Source);
        writer.WriteString(Destination);
        writer.WriteBool(Overwrite);
        writer.WriteVarUInt(ExpectedSourceRevision);
        writer.WriteOptionalString(StoragePolicy);
        writer.WriteOptionalString(SnapshotName);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static CopyMessage Read(ref WireReader reader)
    {
        RequestHeader header = reader.ReadRecord<RequestHeader>();
        string source = reader.ReadString();
        string destination = reader.ReadString();
        bool overwrite = reader.ReadBool();
        ulong expectedSourceRevision = reader.ReadVarUInt();
        string? storagePolicy = reader.ReadOptionalString();
        string? snapshotName = reader.ReadOptionalString();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new CopyMessage(header, source, destination, overwrite, expectedSourceRevision, storagePolicy, snapshotName) { Extensions = extensions };
    }
}

/// <summary>
/// Reply to <see cref="CopyMessage"/>.
/// </summary>
/// <param name="Inode">Destination inode.</param>
/// <param name="Revision">Destination revision (0 when transaction-bound).</param>
/// <param name="SourceRevision">Source revision that was copied.</param>
/// <param name="Created">True when the destination was created.</param>
/// <param name="Token">Commit token of the decision (<see cref="CommitToken.None"/> when transaction-bound: nothing is committed before <c>TxCommit</c>).</param>
/// <param name="Snapshot">The snapshot created by commit-and-snapshot, or null.</param>
public sealed record CopyResultMessage(
    InodeId Inode,
    ulong Revision,
    ulong SourceRevision,
    bool Created,
    CommitToken Token,
    SnapshotInfoData? Snapshot) : IWireMessage<CopyResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.CopyResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteInodeId(Inode);
        writer.WriteVarUInt(Revision);
        writer.WriteVarUInt(SourceRevision);
        writer.WriteBool(Created);
        writer.WriteCommitToken(Token);
        writer.WriteOptional(Snapshot);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static CopyResultMessage Read(ref WireReader reader)
    {
        InodeId inode = reader.ReadInodeId();
        ulong revision = reader.ReadVarUInt();
        ulong sourceRevision = reader.ReadVarUInt();
        bool created = reader.ReadBool();
        CommitToken token = reader.ReadCommitToken();
        SnapshotInfoData? snapshot = reader.ReadOptional<SnapshotInfoData>();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new CopyResultMessage(inode, revision, sourceRevision, created, token, snapshot) { Extensions = extensions };
    }
}

/// <summary>
/// Atomically renames a file or directory inside a volume (client → volume leader; committed <c>Move</c> entry).
/// Handles keep addressing the inode (NS-06). Replacing an existing destination with open handles requires all of
/// them to grant FileShare.Delete; the replaced inode is detached. Errors: FileNotFound/DirectoryNotFound (source
/// kind mismatch or absent), AlreadyExists, SharingViolation, InvalidArgument (root, move into own subtree),
/// CrossVolume, AccessDenied.
/// </summary>
/// <param name="Header">Request header; <see cref="RequestHeader.Operation"/> identifies the mutation (retries reuse it).</param>
/// <param name="Source">Source path.</param>
/// <param name="Destination">Destination path.</param>
/// <param name="Overwrite">True to replace an existing destination file (directories are never replaced).</param>
/// <param name="ExpectDirectory">True when the source must be a directory (DfsDirectoryClient); false when it must be a file.</param>
/// <param name="SnapshotName">Commit-and-snapshot: when set, a snapshot with this name is created in the same metadata decision as the mutation (FSD §11.2); null for none.</param>
public sealed record MoveMessage(
    RequestHeader Header,
    string Source,
    string Destination,
    bool Overwrite,
    bool ExpectDirectory,
    string? SnapshotName) : IWireMessage<MoveMessage>, IVolumeRequest
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.Move;

    /// <inheritdoc />
    public IVolumeRequest WithHeader(RequestHeader header) => this with { Header = header };

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Header);
        writer.WriteString(Source);
        writer.WriteString(Destination);
        writer.WriteBool(Overwrite);
        writer.WriteBool(ExpectDirectory);
        writer.WriteOptionalString(SnapshotName);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static MoveMessage Read(ref WireReader reader)
    {
        RequestHeader header = reader.ReadRecord<RequestHeader>();
        string source = reader.ReadString();
        string destination = reader.ReadString();
        bool overwrite = reader.ReadBool();
        bool expectDirectory = reader.ReadBool();
        string? snapshotName = reader.ReadOptionalString();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new MoveMessage(header, source, destination, overwrite, expectDirectory, snapshotName) { Extensions = extensions };
    }
}

/// <summary>
/// Reply to <see cref="MoveMessage"/>.
/// </summary>
/// <param name="Inode">Inode moved.</param>
/// <param name="Revision">Revision of the moved inode (0 when transaction-bound).</param>
/// <param name="Token">Commit token of the decision (<see cref="CommitToken.None"/> when transaction-bound: nothing is committed before <c>TxCommit</c>).</param>
/// <param name="Snapshot">The snapshot created by commit-and-snapshot, or null.</param>
public sealed record MoveResultMessage(
    InodeId Inode,
    ulong Revision,
    CommitToken Token,
    SnapshotInfoData? Snapshot) : IWireMessage<MoveResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.MoveResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteInodeId(Inode);
        writer.WriteVarUInt(Revision);
        writer.WriteCommitToken(Token);
        writer.WriteOptional(Snapshot);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static MoveResultMessage Read(ref WireReader reader)
    {
        InodeId inode = reader.ReadInodeId();
        ulong revision = reader.ReadVarUInt();
        CommitToken token = reader.ReadCommitToken();
        SnapshotInfoData? snapshot = reader.ReadOptional<SnapshotInfoData>();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new MoveResultMessage(inode, revision, token, snapshot) { Extensions = extensions };
    }
}

/// <summary>
/// Deletes a file or directory (client → volume leader; committed <c>Delete</c> entry). Deleting an absent file
/// succeeds (<c>Existed = false</c>); an absent directory fails with DirectoryNotFound. A non-empty directory
/// requires <c>Recursive</c>, which atomically detaches the whole subtree. Files with open handles require
/// FileShare.Delete on all of them; the inode stays readable through those handles until they close (OPEN-017).
/// Errors: SharingViolation, DirectoryNotEmpty, InvalidArgument (root), AccessDenied.
/// </summary>
/// <param name="Header">Request header; <see cref="RequestHeader.Operation"/> identifies the mutation (retries reuse it).</param>
/// <param name="Path">Path to delete.</param>
/// <param name="Recursive">True to delete a non-empty directory with its subtree.</param>
/// <param name="ExpectDirectory">True when the entry must be a directory; false when it must be a file.</param>
/// <param name="SnapshotName">Commit-and-snapshot: when set, a snapshot with this name is created in the same metadata decision as the mutation (FSD §11.2); null for none.</param>
public sealed record DeleteMessage(
    RequestHeader Header,
    string Path,
    bool Recursive,
    bool ExpectDirectory,
    string? SnapshotName) : IWireMessage<DeleteMessage>, IVolumeRequest
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.Delete;

    /// <inheritdoc />
    public IVolumeRequest WithHeader(RequestHeader header) => this with { Header = header };

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Header);
        writer.WriteString(Path);
        writer.WriteBool(Recursive);
        writer.WriteBool(ExpectDirectory);
        writer.WriteOptionalString(SnapshotName);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static DeleteMessage Read(ref WireReader reader)
    {
        RequestHeader header = reader.ReadRecord<RequestHeader>();
        string path = reader.ReadString();
        bool recursive = reader.ReadBool();
        bool expectDirectory = reader.ReadBool();
        string? snapshotName = reader.ReadOptionalString();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new DeleteMessage(header, path, recursive, expectDirectory, snapshotName) { Extensions = extensions };
    }
}

/// <summary>
/// Reply to <see cref="DeleteMessage"/>.
/// </summary>
/// <param name="Existed">True when an entry was deleted.</param>
/// <param name="Inode">Inode deleted (empty when nothing existed).</param>
/// <param name="Token">Commit of the delete (<see cref="CommitToken.None"/> when nothing existed or when transaction-bound).</param>
/// <param name="Snapshot">The snapshot created by commit-and-snapshot, or null.</param>
public sealed record DeleteResultMessage(
    bool Existed,
    InodeId Inode,
    CommitToken Token,
    SnapshotInfoData? Snapshot) : IWireMessage<DeleteResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.DeleteResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteBool(Existed);
        writer.WriteInodeId(Inode);
        writer.WriteCommitToken(Token);
        writer.WriteOptional(Snapshot);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static DeleteResultMessage Read(ref WireReader reader)
    {
        bool existed = reader.ReadBool();
        InodeId inode = reader.ReadInodeId();
        CommitToken token = reader.ReadCommitToken();
        SnapshotInfoData? snapshot = reader.ReadOptional<SnapshotInfoData>();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new DeleteResultMessage(existed, inode, token, snapshot) { Extensions = extensions };
    }
}

/// <summary>
/// Creates a directory (client → volume leader; committed <c>CreateDirectory</c> entry). Idempotent: an existing
/// directory succeeds with <c>Created = false</c>. Errors: AlreadyExists (a file is in the way), DirectoryNotFound
/// (parent missing and not <c>Recursive</c>), NotADirectory, AccessDenied.
/// </summary>
/// <param name="Header">Request header; <see cref="RequestHeader.Operation"/> identifies the mutation (retries reuse it).</param>
/// <param name="Path">Directory path.</param>
/// <param name="Recursive">True to create missing ancestors.</param>
/// <param name="SnapshotName">Commit-and-snapshot: when set, a snapshot with this name is created in the same metadata decision as the mutation (FSD §11.2); null for none.</param>
public sealed record CreateDirectoryMessage(
    RequestHeader Header,
    string Path,
    bool Recursive,
    string? SnapshotName) : IWireMessage<CreateDirectoryMessage>, IVolumeRequest
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.CreateDirectory;

    /// <inheritdoc />
    public IVolumeRequest WithHeader(RequestHeader header) => this with { Header = header };

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Header);
        writer.WriteString(Path);
        writer.WriteBool(Recursive);
        writer.WriteOptionalString(SnapshotName);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static CreateDirectoryMessage Read(ref WireReader reader)
    {
        RequestHeader header = reader.ReadRecord<RequestHeader>();
        string path = reader.ReadString();
        bool recursive = reader.ReadBool();
        string? snapshotName = reader.ReadOptionalString();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new CreateDirectoryMessage(header, path, recursive, snapshotName) { Extensions = extensions };
    }
}

/// <summary>
/// Reply to <see cref="CreateDirectoryMessage"/>.
/// </summary>
/// <param name="Info">Metadata of the (new or existing) directory.</param>
/// <param name="Created">True when the directory (or an ancestor) was created.</param>
/// <param name="Token">Commit token of the decision (<see cref="CommitToken.None"/> when transaction-bound: nothing is committed before <c>TxCommit</c>).</param>
/// <param name="Snapshot">The snapshot created by commit-and-snapshot, or null.</param>
public sealed record CreateDirectoryResultMessage(
    FileInfoData Info,
    bool Created,
    CommitToken Token,
    SnapshotInfoData? Snapshot) : IWireMessage<CreateDirectoryResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.CreateDirectoryResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Info);
        writer.WriteBool(Created);
        writer.WriteCommitToken(Token);
        writer.WriteOptional(Snapshot);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static CreateDirectoryResultMessage Read(ref WireReader reader)
    {
        FileInfoData info = reader.ReadRecord<FileInfoData>();
        bool created = reader.ReadBool();
        CommitToken token = reader.ReadCommitToken();
        SnapshotInfoData? snapshot = reader.ReadOptional<SnapshotInfoData>();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new CreateDirectoryResultMessage(info, created, token, snapshot) { Extensions = extensions };
    }
}

/// <summary>
/// Selects a named storage policy for an inode (client → volume leader; committed <c>SetPolicy</c> entry). Policies
/// attach to inode identity. The change is not reported effective before migration of existing objects completes (FSD
/// §9.2). Errors: InvalidPolicy (unknown name), FileNotFound, AccessDenied.
/// </summary>
/// <param name="Header">Request header; <see cref="RequestHeader.Operation"/> identifies the mutation (retries reuse it).</param>
/// <param name="Path">File or directory path.</param>
/// <param name="PolicyName">Named policy of the volume (empty = the volume default).</param>
public sealed record SetPolicyMessage(
    RequestHeader Header,
    string Path,
    string PolicyName) : IWireMessage<SetPolicyMessage>, IVolumeRequest
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.SetPolicy;

    /// <inheritdoc />
    public IVolumeRequest WithHeader(RequestHeader header) => this with { Header = header };

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Header);
        writer.WriteString(Path);
        writer.WriteString(PolicyName);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static SetPolicyMessage Read(ref WireReader reader)
    {
        RequestHeader header = reader.ReadRecord<RequestHeader>();
        string path = reader.ReadString();
        string policyName = reader.ReadString();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new SetPolicyMessage(header, path, policyName) { Extensions = extensions };
    }
}

/// <summary>
/// Reply to <see cref="SetPolicyMessage"/>.
/// </summary>
/// <param name="Inode">Inode changed.</param>
/// <param name="Revision">New revision (0 when transaction-bound).</param>
/// <param name="Token">Commit token of the decision (<see cref="CommitToken.None"/> when transaction-bound: nothing is committed before <c>TxCommit</c>).</param>
public sealed record SetPolicyResultMessage(
    InodeId Inode,
    ulong Revision,
    CommitToken Token) : IWireMessage<SetPolicyResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.SetPolicyResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteInodeId(Inode);
        writer.WriteVarUInt(Revision);
        writer.WriteCommitToken(Token);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static SetPolicyResultMessage Read(ref WireReader reader)
    {
        InodeId inode = reader.ReadInodeId();
        ulong revision = reader.ReadVarUInt();
        CommitToken token = reader.ReadCommitToken();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new SetPolicyResultMessage(inode, revision, token) { Extensions = extensions };
    }
}

/// <summary>
/// Changes an inode's access flags (client → volume leader; committed <c>SetAcl</c> entry). Only the owner or a
/// volume administrator may change them. Errors: FileNotFound, AccessDenied.
/// </summary>
/// <param name="Header">Request header; <see cref="RequestHeader.Operation"/> identifies the mutation (retries reuse it).</param>
/// <param name="Path">File or directory path.</param>
/// <param name="OwnerOnly">Only the owner may access the inode.</param>
/// <param name="ReadOnlyForOthers">Principals other than the owner may only read.</param>
public sealed record SetAccessMessage(
    RequestHeader Header,
    string Path,
    bool OwnerOnly,
    bool ReadOnlyForOthers) : IWireMessage<SetAccessMessage>, IVolumeRequest
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.SetAccess;

    /// <inheritdoc />
    public IVolumeRequest WithHeader(RequestHeader header) => this with { Header = header };

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Header);
        writer.WriteString(Path);
        writer.WriteBool(OwnerOnly);
        writer.WriteBool(ReadOnlyForOthers);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static SetAccessMessage Read(ref WireReader reader)
    {
        RequestHeader header = reader.ReadRecord<RequestHeader>();
        string path = reader.ReadString();
        bool ownerOnly = reader.ReadBool();
        bool readOnlyForOthers = reader.ReadBool();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new SetAccessMessage(header, path, ownerOnly, readOnlyForOthers) { Extensions = extensions };
    }
}

/// <summary>
/// Reply to <see cref="SetAccessMessage"/>.
/// </summary>
/// <param name="Inode">Inode changed.</param>
/// <param name="Revision">New revision (0 when transaction-bound).</param>
/// <param name="Token">Commit token of the decision (<see cref="CommitToken.None"/> when transaction-bound: nothing is committed before <c>TxCommit</c>).</param>
public sealed record SetAccessResultMessage(
    InodeId Inode,
    ulong Revision,
    CommitToken Token) : IWireMessage<SetAccessResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.SetAccessResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteInodeId(Inode);
        writer.WriteVarUInt(Revision);
        writer.WriteCommitToken(Token);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static SetAccessResultMessage Read(ref WireReader reader)
    {
        InodeId inode = reader.ReadInodeId();
        ulong revision = reader.ReadVarUInt();
        CommitToken token = reader.ReadCommitToken();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new SetAccessResultMessage(inode, revision, token) { Extensions = extensions };
    }
}
