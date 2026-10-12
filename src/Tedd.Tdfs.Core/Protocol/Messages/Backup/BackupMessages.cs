namespace Tedd.Tdfs.Protocol.Messages;

/// <summary>
/// Manages backup target registrations of a volume (client → volume leader; committed entries for mutating actions).
/// Registration stores endpoints, schedule, budgets and the export credentials; removal releases transfer pins.
/// Requires BackupAdmin. Backup targets never join any Raft group (BAK-001).
/// </summary>
/// <param name="Header">Request header (volume scoping; <see cref="RequestHeader.Operation"/> identifies mutations).</param>
/// <param name="Action">What to do.</param>
/// <param name="Target">Target name (ignored for List).</param>
/// <param name="Endpoints">Target endpoints (Register).</param>
/// <param name="ExportCredentials">Credentials the exporter authenticates with on the target (Register).</param>
/// <param name="Schedule">Schedule (Register, Configure).</param>
/// <param name="Budgets">Budgets (Register, Configure).</param>
public sealed record BackupRegisterMessage(
    RequestHeader Header,
    BackupTargetAction Action,
    string Target,
    IReadOnlyList<string> Endpoints,
    DfsCredentials? ExportCredentials,
    BackupScheduleData? Schedule,
    BackupBudgetsData? Budgets) : IWireMessage<BackupRegisterMessage>, IVolumeRequest
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.BackupRegister;

    /// <inheritdoc />
    public IVolumeRequest WithHeader(RequestHeader header) => this with { Header = header };

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Header);
        writer.WriteDefinedEnum(Action);
        writer.WriteString(Target);
        writer.WriteStringList(Endpoints);
        writer.WriteOptionalCredentials(ExportCredentials);
        writer.WriteOptional(Schedule);
        writer.WriteOptional(Budgets);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static BackupRegisterMessage Read(ref WireReader reader)
    {
        RequestHeader header = reader.ReadRecord<RequestHeader>();
        BackupTargetAction action = reader.ReadEnum<BackupTargetAction>();
        string target = reader.ReadString();
        IReadOnlyList<string> endpoints = reader.ReadStringList();
        DfsCredentials? exportCredentials = reader.ReadOptionalCredentials();
        BackupScheduleData? schedule = reader.ReadOptional<BackupScheduleData>();
        BackupBudgetsData? budgets = reader.ReadOptional<BackupBudgetsData>();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new BackupRegisterMessage(header, action, target, endpoints, exportCredentials, schedule, budgets) { Extensions = extensions };
    }
}

/// <summary>
/// Reply to <see cref="BackupRegisterMessage"/>.
/// </summary>
/// <param name="Targets">Registered target names after the action.</param>
/// <param name="Token">Commit of the change (<see cref="CommitToken.None"/> for List).</param>
public sealed record BackupRegisterResultMessage(
    IReadOnlyList<string> Targets,
    CommitToken Token) : IWireMessage<BackupRegisterResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.BackupRegisterResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteStringList(Targets);
        writer.WriteCommitToken(Token);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static BackupRegisterResultMessage Read(ref WireReader reader)
    {
        IReadOnlyList<string> targets = reader.ReadStringList();
        CommitToken token = reader.ReadCommitToken();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new BackupRegisterResultMessage(targets, token) { Extensions = extensions };
    }
}

/// <summary>
/// Reads the status of one backup target (client → volume leader). Requires BackupAdmin or Read.
/// </summary>
/// <param name="Header">Request header (volume scoping; <see cref="RequestHeader.Operation"/> identifies mutations).</param>
/// <param name="Target">Target name.</param>
public sealed record BackupStatusMessage(
    RequestHeader Header,
    string Target) : IWireMessage<BackupStatusMessage>, IVolumeRequest
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.BackupStatus;

    /// <inheritdoc />
    public IVolumeRequest WithHeader(RequestHeader header) => this with { Header = header };

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Header);
        writer.WriteString(Target);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static BackupStatusMessage Read(ref WireReader reader)
    {
        RequestHeader header = reader.ReadRecord<RequestHeader>();
        string target = reader.ReadString();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new BackupStatusMessage(header, target) { Extensions = extensions };
    }
}

/// <summary>
/// Reply to <see cref="BackupStatusMessage"/>.
/// </summary>
/// <param name="Status">Target status (received, durable and published boundaries reported separately, BAK-016).</param>
public sealed record BackupStatusResultMessage(
    BackupStatusData Status) : IWireMessage<BackupStatusResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.BackupStatusResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Status);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static BackupStatusResultMessage Read(ref WireReader reader)
    {
        BackupStatusData status = reader.ReadRecord<BackupStatusData>();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new BackupStatusResultMessage(status) { Extensions = extensions };
    }
}

/// <summary>
/// Asks the exporter to synchronize a target now (client → volume leader).
/// </summary>
/// <param name="Header">Request header (volume scoping; <see cref="RequestHeader.Operation"/> identifies mutations).</param>
/// <param name="Target">Target name.</param>
/// <param name="SnapshotName">Snapshot to export; null = create/pick the export snapshot.</param>
public sealed record BackupRequestSyncMessage(
    RequestHeader Header,
    string Target,
    string? SnapshotName) : IWireMessage<BackupRequestSyncMessage>, IVolumeRequest
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.BackupRequestSync;

    /// <inheritdoc />
    public IVolumeRequest WithHeader(RequestHeader header) => this with { Header = header };

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Header);
        writer.WriteString(Target);
        writer.WriteOptionalString(SnapshotName);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static BackupRequestSyncMessage Read(ref WireReader reader)
    {
        RequestHeader header = reader.ReadRecord<RequestHeader>();
        string target = reader.ReadString();
        string? snapshotName = reader.ReadOptionalString();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new BackupRequestSyncMessage(header, target, snapshotName) { Extensions = extensions };
    }
}

/// <summary>
/// Reply to <see cref="BackupRequestSyncMessage"/>.
/// </summary>
/// <param name="Accepted">True when a transfer was scheduled.</param>
/// <param name="Token">Boundary of the snapshot scheduled for transfer.</param>
public sealed record BackupRequestSyncResultMessage(
    bool Accepted,
    CommitToken Token) : IWireMessage<BackupRequestSyncResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.BackupRequestSyncResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteBool(Accepted);
        writer.WriteCommitToken(Token);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static BackupRequestSyncResultMessage Read(ref WireReader reader)
    {
        bool accepted = reader.ReadBool();
        CommitToken token = reader.ReadCommitToken();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new BackupRequestSyncResultMessage(accepted, token) { Extensions = extensions };
    }
}

/// <summary>
/// Waits for a backup recovery boundary (client → volume leader). With <c>Commit</c>: the first published checkpoint
/// of the same volume epoch covering at least that commit; with <c>SnapshotName</c>: that exact snapshot durably
/// retained. The leader waits at most <c>Timeout</c> and then answers <c>Reached = false</c>; a timeout never rolls
/// back the live commit (BAK-019).
/// </summary>
/// <param name="Header">Request header (volume scoping; <see cref="RequestHeader.Operation"/> identifies mutations).</param>
/// <param name="Target">Target name.</param>
/// <param name="Commit">Commit boundary to await (exclusive with <c>SnapshotName</c>).</param>
/// <param name="SnapshotName">Exact snapshot to await (exclusive with <c>Commit</c>).</param>
/// <param name="Timeout">Maximum time the leader waits before answering.</param>
public sealed record BackupAwaitMessage(
    RequestHeader Header,
    string Target,
    CommitToken? Commit,
    string? SnapshotName,
    TimeSpan Timeout) : IWireMessage<BackupAwaitMessage>, IVolumeRequest
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.BackupAwait;

    /// <inheritdoc />
    public IVolumeRequest WithHeader(RequestHeader header) => this with { Header = header };

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Header);
        writer.WriteString(Target);
        writer.WriteOptionalCommitToken(Commit);
        writer.WriteOptionalString(SnapshotName);
        writer.WriteTimeSpan(Timeout);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static BackupAwaitMessage Read(ref WireReader reader)
    {
        RequestHeader header = reader.ReadRecord<RequestHeader>();
        string target = reader.ReadString();
        CommitToken? commit = reader.ReadOptionalCommitToken();
        string? snapshotName = reader.ReadOptionalString();
        TimeSpan timeout = reader.ReadTimeSpan();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new BackupAwaitMessage(header, target, commit, snapshotName, timeout) { Extensions = extensions };
    }
}

/// <summary>
/// Reply to <see cref="BackupAwaitMessage"/>.
/// </summary>
/// <param name="Reached">True when the requested boundary is published.</param>
/// <param name="Checkpoint">The satisfying checkpoint (actual snapshot, covered commit, retention).</param>
/// <param name="LastPublished">Latest published checkpoint (reported when not reached).</param>
public sealed record BackupAwaitResultMessage(
    bool Reached,
    DfsBackupCheckpoint? Checkpoint,
    DfsBackupCheckpoint? LastPublished) : IWireMessage<BackupAwaitResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.BackupAwaitResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteBool(Reached);
        writer.WriteOptionalBackupCheckpoint(Checkpoint);
        writer.WriteOptionalBackupCheckpoint(LastPublished);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static BackupAwaitResultMessage Read(ref WireReader reader)
    {
        bool reached = reader.ReadBool();
        DfsBackupCheckpoint? checkpoint = reader.ReadOptionalBackupCheckpoint();
        DfsBackupCheckpoint? lastPublished = reader.ReadOptionalBackupCheckpoint();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new BackupAwaitResultMessage(reached, checkpoint, lastPublished) { Extensions = extensions };
    }
}

/// <summary>
/// Starts (or resumes) the transfer of one snapshot (exporter on the volume leader → backup node, authenticated with
/// the export credentials). The target answers whether it already published the snapshot and whether the lineage must
/// be reseeded. Transfers are resumable and idempotent per transfer key (BAK-009).
/// </summary>
/// <param name="Key">Transfer identity.</param>
/// <param name="SourceClusterName">Source cluster name.</param>
/// <param name="VolumeName">Source volume name.</param>
/// <param name="SnapshotName">Snapshot name.</param>
/// <param name="Boundary">Commit boundary of the snapshot.</param>
/// <param name="CreatedUtc">Snapshot creation time.</param>
/// <param name="Label">Consistency label.</param>
/// <param name="Adapters">Adapter records of the snapshot.</param>
/// <param name="Retention">Retention requested by the source (the target applies its own policy on top).</param>
/// <param name="BaseSnapshot">Previously published snapshot of the same lineage this transfer is incremental to; null = full.</param>
/// <param name="FullReseed">True when the source starts a new lineage after abandoning the old one.</param>
public sealed record BackupBeginMessage(
    BackupTransferKey Key,
    string SourceClusterName,
    string VolumeName,
    string SnapshotName,
    CommitToken Boundary,
    DateTimeOffset CreatedUtc,
    DfsConsistencyLabel Label,
    IReadOnlyList<AdapterRecordData> Adapters,
    DfsRetention Retention,
    SnapshotId? BaseSnapshot,
    bool FullReseed) : IWireMessage<BackupBeginMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.BackupBegin;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Key);
        writer.WriteString(SourceClusterName);
        writer.WriteString(VolumeName);
        writer.WriteString(SnapshotName);
        writer.WriteCommitToken(Boundary);
        writer.WriteDateTimeOffset(CreatedUtc);
        writer.WriteDefinedEnum(Label);
        writer.WriteList(Adapters);
        writer.WriteRetention(Retention);
        writer.WriteOptionalSnapshotId(BaseSnapshot);
        writer.WriteBool(FullReseed);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static BackupBeginMessage Read(ref WireReader reader)
    {
        BackupTransferKey key = reader.ReadRecord<BackupTransferKey>();
        string sourceClusterName = reader.ReadString();
        string volumeName = reader.ReadString();
        string snapshotName = reader.ReadString();
        CommitToken boundary = reader.ReadCommitToken();
        DateTimeOffset createdUtc = reader.ReadDateTimeOffset();
        DfsConsistencyLabel label = reader.ReadEnum<DfsConsistencyLabel>();
        IReadOnlyList<AdapterRecordData> adapters = reader.ReadList<AdapterRecordData>();
        DfsRetention retention = reader.ReadRetention();
        SnapshotId? baseSnapshot = reader.ReadOptionalSnapshotId();
        bool fullReseed = reader.ReadBool();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new BackupBeginMessage(key, sourceClusterName, volumeName, snapshotName, boundary, createdUtc, label, adapters, retention, baseSnapshot, fullReseed) { Extensions = extensions };
    }
}

/// <summary>
/// Reply to <see cref="BackupBeginMessage"/>.
/// </summary>
/// <param name="Accepted">True when the target accepts the transfer.</param>
/// <param name="AlreadyPublished">True when the snapshot is already published (nothing to send).</param>
/// <param name="ReseedRequired">True when the target cannot continue the lineage (send a full reseed).</param>
/// <param name="CurrentLineage">Lineage the target currently follows.</param>
/// <param name="LastPublished">Latest snapshot the target published for the volume.</param>
/// <param name="KnownObjects">Objects the target already holds for the volume (hint only).</param>
/// <param name="ReceivedBytes">Bytes of this transfer already received (resume point).</param>
public sealed record BackupBeginResultMessage(
    bool Accepted,
    bool AlreadyPublished,
    bool ReseedRequired,
    Guid CurrentLineage,
    SnapshotId? LastPublished,
    long KnownObjects,
    long ReceivedBytes) : IWireMessage<BackupBeginResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.BackupBeginResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteBool(Accepted);
        writer.WriteBool(AlreadyPublished);
        writer.WriteBool(ReseedRequired);
        writer.WriteGuid(CurrentLineage);
        writer.WriteOptionalSnapshotId(LastPublished);
        writer.WriteVarInt(KnownObjects);
        writer.WriteVarInt(ReceivedBytes);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static BackupBeginResultMessage Read(ref WireReader reader)
    {
        bool accepted = reader.ReadBool();
        bool alreadyPublished = reader.ReadBool();
        bool reseedRequired = reader.ReadBool();
        Guid currentLineage = reader.ReadGuid();
        SnapshotId? lastPublished = reader.ReadOptionalSnapshotId();
        long knownObjects = reader.ReadVarInt();
        long receivedBytes = reader.ReadVarInt();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new BackupBeginResultMessage(accepted, alreadyPublished, reseedRequired, currentLineage, lastPublished, knownObjects, receivedBytes) { Extensions = extensions };
    }
}

/// <summary>
/// Asks the target which of the listed objects it still needs (exporter → backup node). The answer is only a hint:
/// the target verifies the complete dependency closure itself before publishing (BAK-007).
/// </summary>
/// <param name="Key">Transfer identity.</param>
/// <param name="Objects">Objects referenced by the snapshot.</param>
public sealed record BackupInventoryMessage(
    BackupTransferKey Key,
    IReadOnlyList<ObjectId> Objects) : IWireMessage<BackupInventoryMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.BackupInventory;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Key);
        writer.WriteObjectIdList(Objects);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static BackupInventoryMessage Read(ref WireReader reader)
    {
        BackupTransferKey key = reader.ReadRecord<BackupTransferKey>();
        IReadOnlyList<ObjectId> objects = reader.ReadObjectIdList();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new BackupInventoryMessage(key, objects) { Extensions = extensions };
    }
}

/// <summary>
/// Reply to <see cref="BackupInventoryMessage"/>.
/// </summary>
/// <param name="Missing">Listed objects the target does not hold verified.</param>
public sealed record BackupInventoryResultMessage(
    IReadOnlyList<ObjectId> Missing) : IWireMessage<BackupInventoryResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.BackupInventoryResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteObjectIdList(Missing);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static BackupInventoryResultMessage Read(ref WireReader reader)
    {
        IReadOnlyList<ObjectId> missing = reader.ReadObjectIdList();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new BackupInventoryResultMessage(missing) { Extensions = extensions };
    }
}

/// <summary>
/// Transfers one object (exporter → backup node). The target verifies length and checksum, persists it durably and
/// only then reports it durable. Idempotent.
/// </summary>
/// <param name="Key">Transfer identity.</param>
/// <param name="Object">Object id.</param>
/// <param name="Checksum">XxHash128 of the plaintext.</param>
/// <param name="Length">Plaintext length.</param>
/// <param name="Encoding">Object encoding.</param>
/// <param name="Data">Object plaintext (the target encrypts at rest with its own keys).</param>
public sealed record BackupPutObjectMessage(
    BackupTransferKey Key,
    ObjectId Object,
    UInt128 Checksum,
    long Length,
    byte Encoding,
    ReadOnlyMemory<byte> Data) : IWireMessage<BackupPutObjectMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.BackupPutObject;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Key);
        writer.WriteObjectId(Object);
        writer.WriteUInt128(Checksum);
        writer.WriteVarInt(Length);
        writer.WriteByte(Encoding);
        writer.WriteBytes(Data.Span);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static BackupPutObjectMessage Read(ref WireReader reader)
    {
        BackupTransferKey key = reader.ReadRecord<BackupTransferKey>();
        ObjectId @object = reader.ReadObjectId();
        UInt128 checksum = reader.ReadUInt128();
        long length = reader.ReadVarInt();
        byte encoding = reader.ReadByte();
        ReadOnlyMemory<byte> data = reader.ReadBytesCopy();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new BackupPutObjectMessage(key, @object, checksum, length, encoding, data) { Extensions = extensions };
    }
}

/// <summary>
/// Reply to <see cref="BackupPutObjectMessage"/>.
/// </summary>
/// <param name="Durable">True when the object is durably stored.</param>
/// <param name="AlreadyPresent">True when a verified copy already existed.</param>
/// <param name="ReceivedBytes">Bytes received in this transfer so far.</param>
public sealed record BackupPutObjectResultMessage(
    bool Durable,
    bool AlreadyPresent,
    long ReceivedBytes) : IWireMessage<BackupPutObjectResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.BackupPutObjectResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteBool(Durable);
        writer.WriteBool(AlreadyPresent);
        writer.WriteVarInt(ReceivedBytes);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static BackupPutObjectResultMessage Read(ref WireReader reader)
    {
        bool durable = reader.ReadBool();
        bool alreadyPresent = reader.ReadBool();
        long receivedBytes = reader.ReadVarInt();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new BackupPutObjectResultMessage(durable, alreadyPresent, receivedBytes) { Extensions = extensions };
    }
}

/// <summary>
/// Transfers a chunk of the snapshot manifest (exporter → backup node), in offset order. The manifest (namespace,
/// extent maps, policies, adapter metadata, format version) is opaque to the protocol; its format is owned by the
/// backup module and is checksummed as a whole.
/// </summary>
/// <param name="Key">Transfer identity.</param>
/// <param name="Offset">Offset of <c>Data</c> in the manifest.</param>
/// <param name="TotalLength">Total manifest length.</param>
/// <param name="ManifestChecksum">XxHash128 of the whole manifest.</param>
/// <param name="Data">Chunk bytes.</param>
/// <param name="Last">True for the final chunk.</param>
public sealed record BackupPutManifestMessage(
    BackupTransferKey Key,
    long Offset,
    long TotalLength,
    UInt128 ManifestChecksum,
    ReadOnlyMemory<byte> Data,
    bool Last) : IWireMessage<BackupPutManifestMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.BackupPutManifest;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Key);
        writer.WriteVarInt(Offset);
        writer.WriteVarInt(TotalLength);
        writer.WriteUInt128(ManifestChecksum);
        writer.WriteBytes(Data.Span);
        writer.WriteBool(Last);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static BackupPutManifestMessage Read(ref WireReader reader)
    {
        BackupTransferKey key = reader.ReadRecord<BackupTransferKey>();
        long offset = reader.ReadVarInt();
        long totalLength = reader.ReadVarInt();
        UInt128 manifestChecksum = reader.ReadUInt128();
        ReadOnlyMemory<byte> data = reader.ReadBytesCopy();
        bool last = reader.ReadBool();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new BackupPutManifestMessage(key, offset, totalLength, manifestChecksum, data, last) { Extensions = extensions };
    }
}

/// <summary>
/// Reply to <see cref="BackupPutManifestMessage"/>.
/// </summary>
/// <param name="NextOffset">Next manifest offset the target expects.</param>
/// <param name="Complete">True when the whole manifest is durable and its checksum verified.</param>
public sealed record BackupPutManifestResultMessage(
    long NextOffset,
    bool Complete) : IWireMessage<BackupPutManifestResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.BackupPutManifestResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteVarInt(NextOffset);
        writer.WriteBool(Complete);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static BackupPutManifestResultMessage Read(ref WireReader reader)
    {
        long nextOffset = reader.ReadVarInt();
        bool complete = reader.ReadBool();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new BackupPutManifestResultMessage(nextOffset, complete) { Extensions = extensions };
    }
}

/// <summary>
/// Asks the target to publish the snapshot (exporter → backup node). The target verifies the full dependency closure
/// itself, persists manifest and objects durably, atomically replaces its published pointer, and only then
/// acknowledges. If anything is missing it does not publish and lists what is missing.
/// </summary>
/// <param name="Key">Transfer identity.</param>
/// <param name="SnapshotName">Snapshot name.</param>
/// <param name="Boundary">Commit boundary covered.</param>
/// <param name="ManifestChecksum">Expected manifest checksum.</param>
/// <param name="ManifestLength">Expected manifest length.</param>
/// <param name="RequestedRetention">Retention requested by the source.</param>
public sealed record BackupPublishMessage(
    BackupTransferKey Key,
    string SnapshotName,
    CommitToken Boundary,
    UInt128 ManifestChecksum,
    long ManifestLength,
    DfsRetention RequestedRetention) : IWireMessage<BackupPublishMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.BackupPublish;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Key);
        writer.WriteString(SnapshotName);
        writer.WriteCommitToken(Boundary);
        writer.WriteUInt128(ManifestChecksum);
        writer.WriteVarInt(ManifestLength);
        writer.WriteRetention(RequestedRetention);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static BackupPublishMessage Read(ref WireReader reader)
    {
        BackupTransferKey key = reader.ReadRecord<BackupTransferKey>();
        string snapshotName = reader.ReadString();
        CommitToken boundary = reader.ReadCommitToken();
        UInt128 manifestChecksum = reader.ReadUInt128();
        long manifestLength = reader.ReadVarInt();
        DfsRetention requestedRetention = reader.ReadRetention();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new BackupPublishMessage(key, snapshotName, boundary, manifestChecksum, manifestLength, requestedRetention) { Extensions = extensions };
    }
}

/// <summary>
/// Reply to <see cref="BackupPublishMessage"/>.
/// </summary>
/// <param name="Published">True when the snapshot is published and durable.</param>
/// <param name="MissingObjects">Objects missing from the closure (when not published).</param>
/// <param name="ManifestIncomplete">True when the manifest is missing or fails verification.</param>
/// <param name="CoveredCommit">Commit covered by the published snapshot.</param>
/// <param name="RetentionExpiryUtc">Retention expiry enforced by the target.</param>
/// <param name="Held">True when the target holds the snapshot under a retention hold.</param>
public sealed record BackupPublishResultMessage(
    bool Published,
    IReadOnlyList<ObjectId> MissingObjects,
    bool ManifestIncomplete,
    CommitToken CoveredCommit,
    DateTimeOffset? RetentionExpiryUtc,
    bool Held) : IWireMessage<BackupPublishResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.BackupPublishResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteBool(Published);
        writer.WriteObjectIdList(MissingObjects);
        writer.WriteBool(ManifestIncomplete);
        writer.WriteCommitToken(CoveredCommit);
        writer.WriteOptionalDateTimeOffset(RetentionExpiryUtc);
        writer.WriteBool(Held);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static BackupPublishResultMessage Read(ref WireReader reader)
    {
        bool published = reader.ReadBool();
        IReadOnlyList<ObjectId> missingObjects = reader.ReadObjectIdList();
        bool manifestIncomplete = reader.ReadBool();
        CommitToken coveredCommit = reader.ReadCommitToken();
        DateTimeOffset? retentionExpiryUtc = reader.ReadOptionalDateTimeOffset();
        bool held = reader.ReadBool();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new BackupPublishResultMessage(published, missingObjects, manifestIncomplete, coveredCommit, retentionExpiryUtc, held) { Extensions = extensions };
    }
}

/// <summary>
/// Queries (and optionally extends) retention on a backup node (exporter or retention administrator → backup node).
/// The backup node enforces retention with its own authorization: export credentials can extend but never shorten
/// protected retention or delete protected snapshots (BAK-022).
/// </summary>
/// <param name="SourceClusterId">Source cluster.</param>
/// <param name="VolumeName">Source volume name.</param>
/// <param name="SnapshotName">One snapshot; null = all retained snapshots of the volume.</param>
/// <param name="Extend">Retention to extend to (never shortens); null = query only.</param>
public sealed record BackupRetentionMessage(
    Guid SourceClusterId,
    string VolumeName,
    string? SnapshotName,
    DfsRetention? Extend) : IWireMessage<BackupRetentionMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.BackupRetention;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteGuid(SourceClusterId);
        writer.WriteString(VolumeName);
        writer.WriteOptionalString(SnapshotName);
        writer.WriteOptionalRetention(Extend);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static BackupRetentionMessage Read(ref WireReader reader)
    {
        Guid sourceClusterId = reader.ReadGuid();
        string volumeName = reader.ReadString();
        string? snapshotName = reader.ReadOptionalString();
        DfsRetention? extend = reader.ReadOptionalRetention();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new BackupRetentionMessage(sourceClusterId, volumeName, snapshotName, extend) { Extensions = extensions };
    }
}

/// <summary>
/// Reply to <see cref="BackupRetentionMessage"/>.
/// </summary>
/// <param name="Snapshots">Retained snapshots in ascending boundary order.</param>
public sealed record BackupRetentionResultMessage(
    IReadOnlyList<BackupRetainedSnapshotData> Snapshots) : IWireMessage<BackupRetentionResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.BackupRetentionResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteList(Snapshots);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static BackupRetentionResultMessage Read(ref WireReader reader)
    {
        IReadOnlyList<BackupRetainedSnapshotData> snapshots = reader.ReadList<BackupRetainedSnapshotData>();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new BackupRetentionResultMessage(snapshots) { Extensions = extensions };
    }
}

/// <summary>
/// Pulls a published snapshot from a backup node for restore (restoring cluster → backup node, authenticated as a
/// reader principal). <c>Describe</c> pins the snapshot and returns its descriptor; <c>Manifest</c> and <c>Object</c>
/// return chunks under that pin; <c>Release</c> drops the pin. Missing key material fails with <see
/// cref="DfsErrorCode.RecoveryKeyMissing"/>, never with undecryptable bytes (RST-005).
/// </summary>
/// <param name="SourceVolume">Source volume name.</param>
/// <param name="SnapshotName">Published snapshot.</param>
/// <param name="Part">Requested part.</param>
/// <param name="ReaderPin">Pin from <c>Describe</c> (0 for <c>Describe</c>).</param>
/// <param name="Object">Object to read (<c>Object</c> part).</param>
/// <param name="Offset">Offset within the manifest or object.</param>
/// <param name="MaxBytes">Maximum bytes to return.</param>
/// <param name="RecoveryKey">Recovery key material when the backup node lacks its own (empty = none).</param>
public sealed record BackupExportRestoreMessage(
    string SourceVolume,
    string SnapshotName,
    BackupRestorePart Part,
    ulong ReaderPin,
    ObjectId Object,
    long Offset,
    int MaxBytes,
    ReadOnlyMemory<byte> RecoveryKey) : IWireMessage<BackupExportRestoreMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.BackupExportRestore;

    /// <summary>Returns a description without key material (SEC-016).</summary>
    public override string ToString()
        => $"BackupExportRestoreMessage {{ SourceVolume = {SourceVolume}, SnapshotName = {SnapshotName}, Part = {Part}, ReaderPin = {ReaderPin}, Object = {Object}, Offset = {Offset}, MaxBytes = {MaxBytes}, RecoveryKey = <{RecoveryKey.Length} bytes> }}";

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteString(SourceVolume);
        writer.WriteString(SnapshotName);
        writer.WriteDefinedEnum(Part);
        writer.WriteVarUInt(ReaderPin);
        writer.WriteObjectId(Object);
        writer.WriteVarInt(Offset);
        writer.WriteVarInt32(MaxBytes);
        writer.WriteBytes(RecoveryKey.Span);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static BackupExportRestoreMessage Read(ref WireReader reader)
    {
        string sourceVolume = reader.ReadString();
        string snapshotName = reader.ReadString();
        BackupRestorePart part = reader.ReadEnum<BackupRestorePart>();
        ulong readerPin = reader.ReadVarUInt();
        ObjectId @object = reader.ReadObjectId();
        long offset = reader.ReadVarInt();
        int maxBytes = reader.ReadVarInt32();
        ReadOnlyMemory<byte> recoveryKey = reader.ReadBytesCopy();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new BackupExportRestoreMessage(sourceVolume, snapshotName, part, readerPin, @object, offset, maxBytes, recoveryKey) { Extensions = extensions };
    }
}

/// <summary>
/// Reply to <see cref="BackupExportRestoreMessage"/>.
/// </summary>
/// <param name="Part">Part answered.</param>
/// <param name="ReaderPin">Reader pin (from <c>Describe</c>).</param>
/// <param name="Descriptor">Snapshot descriptor (<c>Describe</c>).</param>
/// <param name="Offset">Offset of <c>Data</c>.</param>
/// <param name="TotalLength">Total length of the manifest or object.</param>
/// <param name="Checksum">XxHash128 of the whole manifest or object.</param>
/// <param name="Data">Chunk bytes.</param>
/// <param name="Last">True when <c>Data</c> reaches the end.</param>
public sealed record BackupExportRestoreResultMessage(
    BackupRestorePart Part,
    ulong ReaderPin,
    BackupRestoreDescriptor? Descriptor,
    long Offset,
    long TotalLength,
    UInt128 Checksum,
    ReadOnlyMemory<byte> Data,
    bool Last) : IWireMessage<BackupExportRestoreResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.BackupExportRestoreResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteDefinedEnum(Part);
        writer.WriteVarUInt(ReaderPin);
        writer.WriteOptional(Descriptor);
        writer.WriteVarInt(Offset);
        writer.WriteVarInt(TotalLength);
        writer.WriteUInt128(Checksum);
        writer.WriteBytes(Data.Span);
        writer.WriteBool(Last);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static BackupExportRestoreResultMessage Read(ref WireReader reader)
    {
        BackupRestorePart part = reader.ReadEnum<BackupRestorePart>();
        ulong readerPin = reader.ReadVarUInt();
        BackupRestoreDescriptor? descriptor = reader.ReadOptional<BackupRestoreDescriptor>();
        long offset = reader.ReadVarInt();
        long totalLength = reader.ReadVarInt();
        UInt128 checksum = reader.ReadUInt128();
        ReadOnlyMemory<byte> data = reader.ReadBytesCopy();
        bool last = reader.ReadBool();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new BackupExportRestoreResultMessage(part, readerPin, descriptor, offset, totalLength, checksum, data, last) { Extensions = extensions };
    }
}
