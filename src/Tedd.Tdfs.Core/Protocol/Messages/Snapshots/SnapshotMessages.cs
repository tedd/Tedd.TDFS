namespace Tedd.Tdfs.Protocol.Messages;

/// <summary>
/// Creates a named, immutable snapshot of the whole volume (client → volume leader; committed <c>CreateSnapshot</c>
/// entry; O(1) capture of the current root). With <c>MinimumCommit</c> the leader waits until it has applied at least
/// that token (same cluster/volume/epoch, else EpochMismatch) before proposing. Adapters run client-side; the label
/// is <see cref="DfsConsistencyLabel.ApplicationConsistent"/> only if every declared adapter prepared successfully.
/// Errors: AlreadyExists (name), EpochMismatch, AccessDenied (needs Snapshot permission), ReadOnly (backup host).
/// </summary>
/// <param name="Header">Request header; <see cref="RequestHeader.Operation"/> identifies the creation.</param>
/// <param name="Name">Snapshot name (unique per volume).</param>
/// <param name="MinimumCommit">Boundary must be at or after this commit; null = current state.</param>
/// <param name="Label">Requested consistency label.</param>
/// <param name="Adapters">Adapters that prepared successfully (recorded with the snapshot).</param>
/// <param name="Retention">Initial retention; null = none.</param>
public sealed record CreateSnapshotMessage(
    RequestHeader Header,
    string Name,
    CommitToken? MinimumCommit,
    DfsConsistencyLabel Label,
    IReadOnlyList<AdapterRecordData> Adapters,
    DfsRetention? Retention) : IWireMessage<CreateSnapshotMessage>, IVolumeRequest
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.CreateSnapshot;

    /// <inheritdoc />
    public IVolumeRequest WithHeader(RequestHeader header) => this with { Header = header };

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Header);
        writer.WriteString(Name);
        writer.WriteOptionalCommitToken(MinimumCommit);
        writer.WriteDefinedEnum(Label);
        writer.WriteList(Adapters);
        writer.WriteOptionalRetention(Retention);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static CreateSnapshotMessage Read(ref WireReader reader)
    {
        RequestHeader header = reader.ReadRecord<RequestHeader>();
        string name = reader.ReadString();
        CommitToken? minimumCommit = reader.ReadOptionalCommitToken();
        DfsConsistencyLabel label = reader.ReadEnum<DfsConsistencyLabel>();
        IReadOnlyList<AdapterRecordData> adapters = reader.ReadList<AdapterRecordData>();
        DfsRetention? retention = reader.ReadOptionalRetention();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new CreateSnapshotMessage(header, name, minimumCommit, label, adapters, retention) { Extensions = extensions };
    }
}

/// <summary>
/// Reply to <see cref="CreateSnapshotMessage"/>.
/// </summary>
/// <param name="Snapshot">The created snapshot (its boundary is the creating entry).</param>
public sealed record CreateSnapshotResultMessage(
    SnapshotInfoData Snapshot) : IWireMessage<CreateSnapshotResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.CreateSnapshotResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Snapshot);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static CreateSnapshotResultMessage Read(ref WireReader reader)
    {
        SnapshotInfoData snapshot = reader.ReadRecord<SnapshotInfoData>();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new CreateSnapshotResultMessage(snapshot) { Extensions = extensions };
    }
}

/// <summary>
/// Lists snapshots of a volume (client → volume leader or backup node; ReadIndex read). On a backup node the list
/// contains the published snapshots it retains.
/// </summary>
/// <param name="Header">Request header.</param>
/// <param name="Name">Only the snapshot with this name; null = all.</param>
public sealed record ListSnapshotsMessage(
    RequestHeader Header,
    string? Name) : IWireMessage<ListSnapshotsMessage>, IVolumeRequest
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.ListSnapshots;

    /// <inheritdoc />
    public IVolumeRequest WithHeader(RequestHeader header) => this with { Header = header };

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Header);
        writer.WriteOptionalString(Name);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static ListSnapshotsMessage Read(ref WireReader reader)
    {
        RequestHeader header = reader.ReadRecord<RequestHeader>();
        string? name = reader.ReadOptionalString();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new ListSnapshotsMessage(header, name) { Extensions = extensions };
    }
}

/// <summary>
/// Reply to <see cref="ListSnapshotsMessage"/>.
/// </summary>
/// <param name="Snapshots">Snapshots in ascending boundary order.</param>
/// <param name="ObservedAt">Commit position the list is based on (<see cref="CommitToken.None"/> on backup nodes).</param>
public sealed record ListSnapshotsResultMessage(
    IReadOnlyList<SnapshotInfoData> Snapshots,
    CommitToken ObservedAt) : IWireMessage<ListSnapshotsResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.ListSnapshotsResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteList(Snapshots);
        writer.WriteCommitToken(ObservedAt);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static ListSnapshotsResultMessage Read(ref WireReader reader)
    {
        IReadOnlyList<SnapshotInfoData> snapshots = reader.ReadList<SnapshotInfoData>();
        CommitToken observedAt = reader.ReadCommitToken();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new ListSnapshotsResultMessage(snapshots, observedAt) { Extensions = extensions };
    }
}

/// <summary>
/// Deletes a snapshot's retention reference (client → volume leader or backup node; committed <c>DeleteSnapshot</c>
/// entry). Data still needed by other snapshots, the current state, readers or transfers is kept; GC decides
/// reclamation. Errors: SnapshotNotFound, RetentionProtected (expiry in the future or hold), AccessDenied.
/// </summary>
/// <param name="Header">Request header; <see cref="RequestHeader.Operation"/> identifies the deletion.</param>
/// <param name="Name">Snapshot name.</param>
public sealed record DeleteSnapshotMessage(
    RequestHeader Header,
    string Name) : IWireMessage<DeleteSnapshotMessage>, IVolumeRequest
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.DeleteSnapshot;

    /// <inheritdoc />
    public IVolumeRequest WithHeader(RequestHeader header) => this with { Header = header };

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Header);
        writer.WriteString(Name);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static DeleteSnapshotMessage Read(ref WireReader reader)
    {
        RequestHeader header = reader.ReadRecord<RequestHeader>();
        string name = reader.ReadString();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new DeleteSnapshotMessage(header, name) { Extensions = extensions };
    }
}

/// <summary>
/// Reply to <see cref="DeleteSnapshotMessage"/>.
/// </summary>
/// <param name="Existed">True when the snapshot existed and was deleted.</param>
/// <param name="Token">Commit of the deletion.</param>
public sealed record DeleteSnapshotResultMessage(
    bool Existed,
    CommitToken Token) : IWireMessage<DeleteSnapshotResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.DeleteSnapshotResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteBool(Existed);
        writer.WriteCommitToken(Token);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static DeleteSnapshotResultMessage Read(ref WireReader reader)
    {
        bool existed = reader.ReadBool();
        CommitToken token = reader.ReadCommitToken();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new DeleteSnapshotResultMessage(existed, token) { Extensions = extensions };
    }
}

/// <summary>
/// Opens a read-only snapshot view (client → volume leader or backup node; committed <c>AcquirePin</c> entry on live
/// volumes, a local pin on backup nodes). Returns a reader pin owned by the caller's session and renewed by
/// <c>KeepAlive</c>; reads address the snapshot with it. Errors: SnapshotNotFound, AccessDenied.
/// </summary>
/// <param name="Header">Request header (<see cref="RequestHeader.Session"/> owns the pin).</param>
/// <param name="Name">Snapshot name.</param>
public sealed record OpenSnapshotMessage(
    RequestHeader Header,
    string Name) : IWireMessage<OpenSnapshotMessage>, IVolumeRequest
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.OpenSnapshot;

    /// <inheritdoc />
    public IVolumeRequest WithHeader(RequestHeader header) => this with { Header = header };

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Header);
        writer.WriteString(Name);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static OpenSnapshotMessage Read(ref WireReader reader)
    {
        RequestHeader header = reader.ReadRecord<RequestHeader>();
        string name = reader.ReadString();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new OpenSnapshotMessage(header, name) { Extensions = extensions };
    }
}

/// <summary>
/// Reply to <see cref="OpenSnapshotMessage"/>.
/// </summary>
/// <param name="SnapshotPin">Reader pin id (non-zero).</param>
/// <param name="Snapshot">The opened snapshot.</param>
/// <param name="PinDuration">Pin lifetime without renewal.</param>
public sealed record OpenSnapshotResultMessage(
    ulong SnapshotPin,
    SnapshotInfoData Snapshot,
    TimeSpan PinDuration) : IWireMessage<OpenSnapshotResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.OpenSnapshotResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteVarUInt(SnapshotPin);
        writer.WriteRecord(Snapshot);
        writer.WriteTimeSpan(PinDuration);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static OpenSnapshotResultMessage Read(ref WireReader reader)
    {
        ulong snapshotPin = reader.ReadVarUInt();
        SnapshotInfoData snapshot = reader.ReadRecord<SnapshotInfoData>();
        TimeSpan pinDuration = reader.ReadTimeSpan();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new OpenSnapshotResultMessage(snapshotPin, snapshot, pinDuration) { Extensions = extensions };
    }
}

/// <summary>
/// Releases a reader pin owned by the caller's session (client → volume leader or backup node): a snapshot reader pin
/// from <c>OpenSnapshot</c>, or an enumeration pin from <c>ListDirectory</c> when an enumeration is abandoned early.
/// Idempotent.
/// </summary>
/// <param name="Header">Request header.</param>
/// <param name="SnapshotPin">Reader pin to release.</param>
public sealed record CloseSnapshotMessage(
    RequestHeader Header,
    ulong SnapshotPin) : IWireMessage<CloseSnapshotMessage>, IVolumeRequest
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.CloseSnapshot;

    /// <inheritdoc />
    public IVolumeRequest WithHeader(RequestHeader header) => this with { Header = header };

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Header);
        writer.WriteVarUInt(SnapshotPin);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static CloseSnapshotMessage Read(ref WireReader reader)
    {
        RequestHeader header = reader.ReadRecord<RequestHeader>();
        ulong snapshotPin = reader.ReadVarUInt();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new CloseSnapshotMessage(header, snapshotPin) { Extensions = extensions };
    }
}

/// <summary>
/// Reply to <see cref="CloseSnapshotMessage"/>.
/// </summary>
/// <param name="Released">True when the pin was held and is now released.</param>
public sealed record CloseSnapshotResultMessage(
    bool Released) : IWireMessage<CloseSnapshotResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.CloseSnapshotResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteBool(Released);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static CloseSnapshotResultMessage Read(ref WireReader reader)
    {
        bool released = reader.ReadBool();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new CloseSnapshotResultMessage(released) { Extensions = extensions };
    }
}

/// <summary>
/// Changes a snapshot's retention (client → volume leader or backup node; committed <c>SetSnapshotRetention</c>
/// entry). Shortening an expiry or removing a hold requires RetentionAdmin; on backup nodes export credentials can
/// never shorten protected retention (BAK-022). Errors: SnapshotNotFound, RetentionProtected, AccessDenied.
/// </summary>
/// <param name="Header">Request header; <see cref="RequestHeader.Operation"/> identifies the change.</param>
/// <param name="Name">Snapshot name.</param>
/// <param name="Retention">New retention.</param>
public sealed record SetRetentionMessage(
    RequestHeader Header,
    string Name,
    DfsRetention Retention) : IWireMessage<SetRetentionMessage>, IVolumeRequest
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.SetRetention;

    /// <inheritdoc />
    public IVolumeRequest WithHeader(RequestHeader header) => this with { Header = header };

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Header);
        writer.WriteString(Name);
        writer.WriteRetention(Retention);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static SetRetentionMessage Read(ref WireReader reader)
    {
        RequestHeader header = reader.ReadRecord<RequestHeader>();
        string name = reader.ReadString();
        DfsRetention retention = reader.ReadRetention();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new SetRetentionMessage(header, name, retention) { Extensions = extensions };
    }
}

/// <summary>
/// Reply to <see cref="SetRetentionMessage"/>.
/// </summary>
/// <param name="Snapshot">The snapshot with its new retention.</param>
/// <param name="Token">Commit of the change.</param>
public sealed record SetRetentionResultMessage(
    SnapshotInfoData Snapshot,
    CommitToken Token) : IWireMessage<SetRetentionResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.SetRetentionResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Snapshot);
        writer.WriteCommitToken(Token);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static SetRetentionResultMessage Read(ref WireReader reader)
    {
        SnapshotInfoData snapshot = reader.ReadRecord<SnapshotInfoData>();
        CommitToken token = reader.ReadCommitToken();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new SetRetentionResultMessage(snapshot, token) { Extensions = extensions };
    }
}

/// <summary>
/// Restores a snapshot into a new volume (client → volume leader, which coordinates with the control leader): new
/// VolumeId, epoch 1, initial state = the snapshot root, objects copied server-side to new ids owned by the new
/// volume (<c>CopyObjects</c>, or read and re-placed when the new volume lives in another storage pool). Never rolls a
/// live volume back in place. The new volume is not served before the restore is complete. Errors: SnapshotNotFound,
/// AlreadyExists (volume name), PoolNotFound, InvalidPolicy (the pool cannot satisfy the restored policies),
/// PlacementUnavailable, AccessDenied (needs VolumeAdmin and ClusterAdmin).
/// </summary>
/// <param name="Header">Request header; <see cref="RequestHeader.Operation"/> identifies the restore.</param>
/// <param name="Name">Snapshot name.</param>
/// <param name="NewVolumeName">Name of the new volume.</param>
public sealed record RestoreSnapshotMessage(
    RequestHeader Header,
    string Name,
    string NewVolumeName) : IWireMessage<RestoreSnapshotMessage>, IVolumeRequest
{
    /// <summary>Storage pool of the new volume (POOL-016); null = the source volume's pool.</summary>
    public string? StoragePool { get; init; }

    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.RestoreSnapshot;

    /// <inheritdoc />
    public IVolumeRequest WithHeader(RequestHeader header) => this with { Header = header };

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Header);
        writer.WriteString(Name);
        writer.WriteString(NewVolumeName);
        writer.WriteOptionalString(StoragePool);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static RestoreSnapshotMessage Read(ref WireReader reader)
    {
        RequestHeader header = reader.ReadRecord<RequestHeader>();
        string name = reader.ReadString();
        string newVolumeName = reader.ReadString();
        string? pool = reader.ReadOptionalString();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new RestoreSnapshotMessage(header, name, newVolumeName) { StoragePool = pool, Extensions = extensions };
    }
}

/// <summary>
/// Reply to <see cref="RestoreSnapshotMessage"/>.
/// </summary>
/// <param name="Volume">The new volume.</param>
/// <param name="LastCommit">Commit of the new volume's initial state.</param>
public sealed record RestoreSnapshotResultMessage(
    VolumeEntry Volume,
    CommitToken LastCommit) : IWireMessage<RestoreSnapshotResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.RestoreSnapshotResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Volume);
        writer.WriteCommitToken(LastCommit);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static RestoreSnapshotResultMessage Read(ref WireReader reader)
    {
        VolumeEntry volume = reader.ReadRecord<VolumeEntry>();
        CommitToken lastCommit = reader.ReadCommitToken();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new RestoreSnapshotResultMessage(volume, lastCommit) { Extensions = extensions };
    }
}
