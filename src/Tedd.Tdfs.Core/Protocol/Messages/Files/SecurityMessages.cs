namespace Tedd.Tdfs.Protocol.Messages;

/// <summary>
/// Reads the security descriptor of a file, directory or stream path (client → volume leader or backup node; ReadIndex
/// read; Addendum A, ACL-02/03). The reply carries the owner, the protection flag, the explicit entries in canonical order
/// and the inherited entries generation by generation (<see cref="DfsAccessRule.IsInherited"/> = true, with
/// <see cref="DfsAccessRule.InheritedFrom"/>), exactly as <see cref="DfsAccessEvaluator.EffectiveRules(IReadOnlyList{DfsSecurityDescriptor}, bool, DfsIdentity, string?)"/>
/// computes them. A stream path reads its file's descriptor. Requires the volume Read grant and ReadPermissions.
/// Errors: FileNotFound, DirectoryNotFound, AccessDenied, InvalidPath, PinExpired (snapshot view).
/// </summary>
/// <param name="Header">Request header.</param>
/// <param name="Path">Path whose descriptor is read (re-validated and NFC-normalized by the server).</param>
/// <param name="SnapshotPin">Read the descriptor inside the snapshot held by this reader pin; 0 = live (or transaction) view.</param>
public sealed record GetAccessControlMessage(
    RequestHeader Header,
    string Path,
    ulong SnapshotPin) : IWireMessage<GetAccessControlMessage>, IVolumeRequest
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.GetAccessControl;

    /// <inheritdoc />
    public IVolumeRequest WithHeader(RequestHeader header) => this with { Header = header };

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Header);
        writer.WriteString(Path);
        writer.WriteVarUInt(SnapshotPin);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static GetAccessControlMessage Read(ref WireReader reader)
    {
        RequestHeader header = reader.ReadRecord<RequestHeader>();
        string path = reader.ReadString();
        ulong pin = reader.ReadVarUInt();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new GetAccessControlMessage(header, path, pin) { Extensions = extensions };
    }
}

/// <summary>Reply to <see cref="GetAccessControlMessage"/>.</summary>
/// <param name="Descriptor">The full descriptor: owner, protection flag, explicit then inherited entries in evaluation order.</param>
/// <param name="Inode">Inode the descriptor belongs to.</param>
/// <param name="Revision">Revision of the inode at the observed position.</param>
/// <param name="ObservedAt">Commit position the answer is based on.</param>
public sealed record GetAccessControlResultMessage(
    DfsSecurityDescriptor Descriptor,
    InodeId Inode,
    ulong Revision,
    CommitToken ObservedAt) : IWireMessage<GetAccessControlResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.GetAccessControlResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteSecurityDescriptor(Descriptor);
        writer.WriteInodeId(Inode);
        writer.WriteVarUInt(Revision);
        writer.WriteCommitToken(ObservedAt);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static GetAccessControlResultMessage Read(ref WireReader reader)
    {
        DfsSecurityDescriptor descriptor = reader.ReadSecurityDescriptor();
        InodeId inode = reader.ReadInodeId();
        ulong revision = reader.ReadVarUInt();
        CommitToken observed = reader.ReadCommitToken();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new GetAccessControlResultMessage(descriptor, inode, revision, observed) { Extensions = extensions };
    }
}

/// <summary>
/// Atomically replaces the explicit entries and the protection flag of an inode (client → volume leader; committed
/// <c>SetAcl</c> entry; Addendum A, ACL-07). Inherited entries and the owner in <see cref="Descriptor"/> are ignored (the
/// owner changes only through <see cref="SetOwnerMessage"/>). Requires the volume Write grant and ChangePermissions; the
/// change bumps the inode revision. A stream path changes its file's descriptor. Retries reuse the operation id and
/// receive the stored outcome. Errors: FileNotFound, DirectoryNotFound, AccessDenied, InvalidArgument (invalid explicit
/// entries), InvalidPath.
/// </summary>
/// <param name="Header">Request header; <see cref="RequestHeader.Operation"/> identifies the mutation (retries reuse it).</param>
/// <param name="Path">File, directory or stream path.</param>
/// <param name="Descriptor">New explicit entries and protection flag (validated by the server).</param>
public sealed record SetAccessControlMessage(
    RequestHeader Header,
    string Path,
    DfsSecurityDescriptor Descriptor) : IWireMessage<SetAccessControlMessage>, IVolumeRequest
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.SetAccessControl;

    /// <inheritdoc />
    public IVolumeRequest WithHeader(RequestHeader header) => this with { Header = header };

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Header);
        writer.WriteString(Path);
        writer.WriteSecurityDescriptor(Descriptor);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static SetAccessControlMessage Read(ref WireReader reader)
    {
        RequestHeader header = reader.ReadRecord<RequestHeader>();
        string path = reader.ReadString();
        DfsSecurityDescriptor descriptor = reader.ReadSecurityDescriptor();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new SetAccessControlMessage(header, path, descriptor) { Extensions = extensions };
    }
}

/// <summary>Reply to <see cref="SetAccessControlMessage"/>.</summary>
/// <param name="Inode">Inode changed.</param>
/// <param name="Revision">New revision of the inode (0 when transaction-bound).</param>
/// <param name="Token">Commit token of the decision (<see cref="CommitToken.None"/> when transaction-bound).</param>
public sealed record SetAccessControlResultMessage(
    InodeId Inode,
    ulong Revision,
    CommitToken Token) : IWireMessage<SetAccessControlResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.SetAccessControlResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteInodeId(Inode);
        writer.WriteVarUInt(Revision);
        writer.WriteCommitToken(Token);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static SetAccessControlResultMessage Read(ref WireReader reader)
    {
        InodeId inode = reader.ReadInodeId();
        ulong revision = reader.ReadVarUInt();
        CommitToken token = reader.ReadCommitToken();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new SetAccessControlResultMessage(inode, revision, token) { Extensions = extensions };
    }
}

/// <summary>
/// Changes the owner of an inode (client → volume leader; committed <c>SetOwner</c> entry; Addendum A, ACL-05/014): a caller
/// holding TakeOwnership may make itself the owner; members of Administrators may set any owner. CreatorOwner is rejected
/// (InvalidArgument). Requires the volume Write grant; the change bumps the inode revision. A stream path changes its
/// file's owner. Errors: FileNotFound, DirectoryNotFound, AccessDenied, InvalidArgument, InvalidPath.
/// </summary>
/// <param name="Header">Request header; <see cref="RequestHeader.Operation"/> identifies the mutation (retries reuse it).</param>
/// <param name="Path">File, directory or stream path.</param>
/// <param name="Owner">New owner identity.</param>
public sealed record SetOwnerMessage(
    RequestHeader Header,
    string Path,
    DfsIdentity Owner) : IWireMessage<SetOwnerMessage>, IVolumeRequest
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.SetOwner;

    /// <inheritdoc />
    public IVolumeRequest WithHeader(RequestHeader header) => this with { Header = header };

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Header);
        writer.WriteString(Path);
        writer.WriteIdentity(Owner);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static SetOwnerMessage Read(ref WireReader reader)
    {
        RequestHeader header = reader.ReadRecord<RequestHeader>();
        string path = reader.ReadString();
        DfsIdentity owner = reader.ReadIdentity();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new SetOwnerMessage(header, path, owner) { Extensions = extensions };
    }
}

/// <summary>Reply to <see cref="SetOwnerMessage"/>.</summary>
/// <param name="Inode">Inode changed.</param>
/// <param name="Revision">New revision of the inode (0 when transaction-bound).</param>
/// <param name="Token">Commit token of the decision (<see cref="CommitToken.None"/> when transaction-bound).</param>
public sealed record SetOwnerResultMessage(
    InodeId Inode,
    ulong Revision,
    CommitToken Token) : IWireMessage<SetOwnerResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.SetOwnerResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteInodeId(Inode);
        writer.WriteVarUInt(Revision);
        writer.WriteCommitToken(Token);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static SetOwnerResultMessage Read(ref WireReader reader)
    {
        InodeId inode = reader.ReadInodeId();
        ulong revision = reader.ReadVarUInt();
        CommitToken token = reader.ReadCommitToken();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new SetOwnerResultMessage(inode, revision, token) { Extensions = extensions };
    }
}

/// <summary>
/// The rights an identity would be granted on a path (client → volume leader or backup node; ReadIndex read; Addendum A,
/// ACL-034): <see cref="DfsAccessEvaluator.GrantedRights"/> over the effective entries, with the identity's groups and
/// administrator flag resolved from the identity database (volume grants not folded in). Querying another identity
/// requires ReadPermissions on the path; querying oneself (<see cref="Identity"/> null) only the volume Read grant.
/// Errors: FileNotFound, DirectoryNotFound, AccessDenied, InvalidPath.
/// </summary>
/// <param name="Header">Request header.</param>
/// <param name="Path">File, directory or stream path (a stream path evaluates its file).</param>
/// <param name="Identity">Identity to evaluate; null = the caller.</param>
/// <param name="SnapshotPin">Evaluate inside the snapshot held by this reader pin; 0 = live view.</param>
public sealed record GetEffectiveRightsMessage(
    RequestHeader Header,
    string Path,
    DfsIdentity? Identity,
    ulong SnapshotPin) : IWireMessage<GetEffectiveRightsMessage>, IVolumeRequest
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.GetEffectiveRights;

    /// <inheritdoc />
    public IVolumeRequest WithHeader(RequestHeader header) => this with { Header = header };

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Header);
        writer.WriteString(Path);
        writer.WriteBool(Identity.HasValue);
        if (Identity is { } identity)
            writer.WriteIdentity(identity);
        writer.WriteVarUInt(SnapshotPin);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static GetEffectiveRightsMessage Read(ref WireReader reader)
    {
        RequestHeader header = reader.ReadRecord<RequestHeader>();
        string path = reader.ReadString();
        DfsIdentity? identity = reader.ReadBool() ? reader.ReadIdentity() : null;
        ulong pin = reader.ReadVarUInt();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new GetEffectiveRightsMessage(header, path, identity, pin) { Extensions = extensions };
    }
}

/// <summary>Reply to <see cref="GetEffectiveRightsMessage"/>.</summary>
/// <param name="Rights">Granted rights (raw <see cref="DfsFileSystemRights"/> bits).</param>
/// <param name="ObservedAt">Commit position the answer is based on.</param>
public sealed record GetEffectiveRightsResultMessage(
    DfsFileSystemRights Rights,
    CommitToken ObservedAt) : IWireMessage<GetEffectiveRightsResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.GetEffectiveRightsResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteVarUInt(unchecked((uint)Rights));
        writer.WriteCommitToken(ObservedAt);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static GetEffectiveRightsResultMessage Read(ref WireReader reader)
    {
        ulong rights = reader.ReadVarUInt();
        if (rights > uint.MaxValue)
            throw WireReader.Malformed("Access rights do not fit 32 bits.");
        CommitToken observed = reader.ReadCommitToken();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new GetEffectiveRightsResultMessage(unchecked((DfsFileSystemRights)(uint)rights), observed) { Extensions = extensions };
    }
}

/// <summary>
/// Lists the named streams of a file or directory (client → volume leader or backup node; ReadIndex read; Addendum A,
/// ADS-03/005): name, length and revision of every named stream (not the default stream), ordinal by name. Requires the
/// volume Read grant and ReadAttributes on the inode (or ListDirectory on its parent). Errors: FileNotFound (absent
/// path), InvalidPath (a stream path), AccessDenied.
/// </summary>
/// <param name="Header">Request header.</param>
/// <param name="Path">File or directory path (a stream path is rejected).</param>
/// <param name="SnapshotPin">List inside the snapshot held by this reader pin; 0 = live view.</param>
public sealed record ListStreamsMessage(
    RequestHeader Header,
    string Path,
    ulong SnapshotPin) : IWireMessage<ListStreamsMessage>, IVolumeRequest
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.ListStreams;

    /// <inheritdoc />
    public IVolumeRequest WithHeader(RequestHeader header) => this with { Header = header };

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Header);
        writer.WriteString(Path);
        writer.WriteVarUInt(SnapshotPin);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static ListStreamsMessage Read(ref WireReader reader)
    {
        RequestHeader header = reader.ReadRecord<RequestHeader>();
        string path = reader.ReadString();
        ulong pin = reader.ReadVarUInt();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new ListStreamsMessage(header, path, pin) { Extensions = extensions };
    }
}

/// <summary>Reply to <see cref="ListStreamsMessage"/>.</summary>
/// <param name="Streams">Named streams, ordinal by name.</param>
/// <param name="ObservedAt">Commit position the answer is based on.</param>
public sealed record ListStreamsResultMessage(
    IReadOnlyList<StreamInfoData> Streams,
    CommitToken ObservedAt) : IWireMessage<ListStreamsResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.ListStreamsResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteList(Streams);
        writer.WriteCommitToken(ObservedAt);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static ListStreamsResultMessage Read(ref WireReader reader)
    {
        IReadOnlyList<StreamInfoData> streams = reader.ReadList<StreamInfoData>();
        CommitToken observed = reader.ReadCommitToken();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new ListStreamsResultMessage(streams, observed) { Extensions = extensions };
    }
}

/// <summary>One named stream of a file or directory (mirrors the SDK's <see cref="DfsStreamInfo"/>).</summary>
/// <param name="Name">Stream name (NFC).</param>
/// <param name="Length">Stream length.</param>
/// <param name="Revision">Revision (log index) of the stream's last modification.</param>
public sealed record StreamInfoData(
    string Name,
    long Length,
    ulong Revision) : IWireRecord<StreamInfoData>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 4;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteString(Name);
        writer.WriteVarInt(Length);
        writer.WriteVarUInt(Revision);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static StreamInfoData Read(ref WireReader reader)
    {
        string name = reader.ReadString();
        long length = reader.ReadVarInt();
        ulong revision = reader.ReadVarUInt();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new StreamInfoData(name, length, revision) { Extensions = extensions };
    }
}
