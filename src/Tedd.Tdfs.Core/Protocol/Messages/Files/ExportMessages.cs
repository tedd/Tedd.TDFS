namespace Tedd.Tdfs.Protocol.Messages;

/// <summary>Export reservation with a volume-qualified directory identity.</summary>
public sealed record ExportOpenMessage(OpenHandleMessage Open, DfsExportRoot Root, DfsOpenAccess DesiredAccess, bool Directory, bool DeleteOnClose = false)
    : IWireMessage<ExportOpenMessage>, IVolumeRequest
{
    public RequestHeader Header => Open.Header;
    public static MessageType WireType => MessageType.ExportOpen;
    public MessageType Type => WireType;
    public IVolumeRequest WithHeader(RequestHeader header) => this with { Open = Open with { Header = header } };
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];
    public void Write(ref WireWriter w)
    {
        Open.Write(ref w);
        w.WriteCommitToken(Root.Volume);
        w.WriteInodeId(Root.Directory);
        w.WriteEnum(DesiredAccess);
        w.WriteBool(Directory);
        w.WriteBool(DeleteOnClose);
        w.WriteExtensionBlock(Extensions);
    }

    public static ExportOpenMessage Read(ref WireReader r) => new ExportOpenMessage(OpenHandleMessage.Read(ref r),
        new DfsExportRoot(r.ReadCommitToken(), r.ReadInodeId()), r.ReadEnumRaw<DfsOpenAccess>(), r.ReadBool(), r.ReadBool()) { Extensions = r.ReadExtensionBlock() };
}

public sealed record ExportOpenResultMessage(OpenHandleResultMessage Open) : IWireMessage<ExportOpenResultMessage>
{
    public static MessageType WireType => MessageType.ExportOpenResult;
    public MessageType Type => WireType;
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];
    public void Write(ref WireWriter w)
    {
        Open.Write(ref w);
        w.WriteExtensionBlock(Extensions);
    }
    public static ExportOpenResultMessage Read(ref WireReader r) => new ExportOpenResultMessage(OpenHandleResultMessage.Read(ref r))
        { Extensions = r.ReadExtensionBlock() };
}

public sealed record ExportControlResultMessage(CommitToken Token, InodeId Directory = default) : IWireMessage<ExportControlResultMessage>
{
    public static MessageType WireType => MessageType.ExportControlResult;
    public MessageType Type => WireType;
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];
    public void Write(ref WireWriter w)
    {
        w.WriteCommitToken(Token);
        w.WriteInodeId(Directory);
        w.WriteExtensionBlock(Extensions);
    }
    public static ExportControlResultMessage Read(ref WireReader r) => new ExportControlResultMessage(r.ReadCommitToken(), r.ReadInodeId())
        { Extensions = r.ReadExtensionBlock() };
}

public enum ExportControlKind { Configure, Disposition, Rename, SetBasicInformation, Bind }

/// <summary>A replicated profile change or an operation on a session-owned export handle.</summary>
public sealed record ExportControlMessage(RequestHeader Header, ExportControlKind Action, HandleId Handle = default,
    DfsExportProfile Profile = default, string Destination = "", bool Flag = false, DfsBasicInformation? Information = null)
    : IWireMessage<ExportControlMessage>, IVolumeRequest
{
    public static MessageType WireType => MessageType.ExportControl;
    public MessageType Type => WireType;
    public IVolumeRequest WithHeader(RequestHeader header) => this with { Header = header };
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    public void Write(ref WireWriter w)
    {
        w.WriteRecord(Header);
        w.WriteDefinedEnum(Action);
        w.WriteHandleId(Handle);
        w.WriteDefinedEnum(Profile);
        w.WriteString(Destination);
        w.WriteBool(Flag);
        w.WriteBool(Information is not null);
        if (Information is { } info)
            BasicInformationCodec.Write(ref w, info);
        w.WriteExtensionBlock(Extensions);
    }

    public static ExportControlMessage Read(ref WireReader r) => new ExportControlMessage(r.ReadRecord<RequestHeader>(),
        r.ReadEnum<ExportControlKind>(), r.ReadHandleId(), r.ReadEnum<DfsExportProfile>(), r.ReadString(), r.ReadBool(),
        r.ReadBool() ? BasicInformationCodec.Read(ref r) : null) { Extensions = r.ReadExtensionBlock() };
}

internal static class BasicInformationCodec
{
    private const uint FieldId = 1;
    internal static WireExtension Extension(DfsBasicInformation info)
    {
        var bytes = new System.Buffers.ArrayBufferWriter<byte>();
        var w = new WireWriter(bytes);
        Write(ref w, info);
        return new WireExtension(FieldId, false, bytes.WrittenMemory);
    }

    internal static DfsBasicInformation? FromExtensions(IReadOnlyList<WireExtension> extensions)
    {
        foreach (var extension in extensions)
        {
            if (extension.FieldId != FieldId)
                continue;
            var r = new WireReader(extension.Value.Span, WireLimits.Default);
            return Read(ref r);
        }
        return null;
    }

    internal static void Write(ref WireWriter w, DfsBasicInformation info)
    {
        w.WriteOptionalDateTimeOffset(info.CreatedUtc);
        w.WriteOptionalDateTimeOffset(info.ModifiedUtc);
        w.WriteOptionalDateTimeOffset(info.AccessedUtc);
        w.WriteOptionalDateTimeOffset(info.ChangedUtc);
        w.WriteVarInt(info.Attributes is { } attributes ? (int)attributes : -1);
    }

    internal static DfsBasicInformation Read(ref WireReader r)
    {
        var created = r.ReadOptionalDateTimeOffset();
        var modified = r.ReadOptionalDateTimeOffset();
        var accessed = r.ReadOptionalDateTimeOffset();
        var changed = r.ReadOptionalDateTimeOffset();
        int attributes = r.ReadVarInt32();
        return new(created, modified, accessed, changed, attributes == -1 ? null : (FileAttributes)attributes);
    }
}
