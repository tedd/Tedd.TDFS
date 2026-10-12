namespace Tedd.Tdfs.Protocol.Messages;

/// <summary>
/// Begins a transaction (client → volume leader; committed <c>TxBegin</c> entry). The header operation id becomes the
/// durable transaction id. The entry records the read index R, a pin and a staging session; every replica retains the
/// state root at R while the transaction is active. Leader change, restart, expiry or limit violations abort the
/// transaction (<see cref="DfsErrorCode.TransactionAborted"/>, retryable by the application).
/// </summary>
/// <param name="Header">Request header; <see cref="RequestHeader.Operation"/> is the transaction's begin id (retries reuse it).</param>
/// <param name="Timeout">Requested maximum duration (clamped to the volume limit); null = the volume limit.</param>
public sealed record TxBeginMessage(
    RequestHeader Header,
    TimeSpan? Timeout) : IWireMessage<TxBeginMessage>, IVolumeRequest
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.TxBegin;

    /// <inheritdoc />
    public IVolumeRequest WithHeader(RequestHeader header) => this with { Header = header };

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Header);
        writer.WriteOptionalTimeSpan(Timeout);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static TxBeginMessage Read(ref WireReader reader)
    {
        RequestHeader header = reader.ReadRecord<RequestHeader>();
        TimeSpan? timeout = reader.ReadOptionalTimeSpan();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new TxBeginMessage(header, timeout) { Extensions = extensions };
    }
}

/// <summary>
/// Reply to <see cref="TxBeginMessage"/>.
/// </summary>
/// <param name="Transaction">Durable transaction id.</param>
/// <param name="ReadVersion">Commit position R the transaction reads from.</param>
/// <param name="Staging">Staging session of the transaction (closed when it ends).</param>
/// <param name="Capability">Encoded staging <see cref="DataCapability"/> for the transaction's uploads.</param>
/// <param name="CapabilityExpiry">Capability expiry (renew with <c>CreateStaging</c> and <c>Renew</c>).</param>
/// <param name="Deadline">Leader wall-clock time after which the transaction is aborted.</param>
/// <param name="PlacementEpoch">Placement epoch for uploads.</param>
/// <param name="Limits">Transaction limits in force.</param>
public sealed record TxBeginResultMessage(
    DfsTransactionId Transaction,
    CommitToken ReadVersion,
    StagingId Staging,
    ReadOnlyMemory<byte> Capability,
    DateTimeOffset CapabilityExpiry,
    DateTimeOffset Deadline,
    ulong PlacementEpoch,
    DfsVolumeLimits Limits) : IWireMessage<TxBeginResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.TxBeginResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteTransactionId(Transaction);
        writer.WriteCommitToken(ReadVersion);
        writer.WriteStagingId(Staging);
        writer.WriteBytes(Capability.Span);
        writer.WriteDateTimeOffset(CapabilityExpiry);
        writer.WriteDateTimeOffset(Deadline);
        writer.WriteVarUInt(PlacementEpoch);
        writer.WriteVolumeLimits(Limits);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static TxBeginResultMessage Read(ref WireReader reader)
    {
        DfsTransactionId transaction = reader.ReadTransactionId();
        CommitToken readVersion = reader.ReadCommitToken();
        StagingId staging = reader.ReadStagingId();
        ReadOnlyMemory<byte> capability = reader.ReadBytesCopy();
        DateTimeOffset capabilityExpiry = reader.ReadDateTimeOffset();
        DateTimeOffset deadline = reader.ReadDateTimeOffset();
        ulong placementEpoch = reader.ReadVarUInt();
        DfsVolumeLimits limits = reader.ReadVolumeLimits();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new TxBeginResultMessage(transaction, readVersion, staging, capability, capabilityExpiry, deadline, placementEpoch, limits) { Extensions = extensions };
    }
}

/// <summary>
/// Optional batching envelope: executes several transaction-bound requests in order against the transaction's working
/// root (client → volume leader). Each element is a complete request frame whose header carries the same transaction;
/// execution stops at the first failing request, whose error reply is the last element of the result. Ordinary
/// transaction-bound requests do not need this envelope; a server that does not implement it answers <see
/// cref="DfsErrorCode.Unsupported"/>.
/// </summary>
/// <param name="Header">Request header; <see cref="RequestHeader.Transaction"/> identifies the transaction.</param>
/// <param name="Requests">Encoded request frames.</param>
public sealed record TxOpMessage(
    RequestHeader Header,
    IReadOnlyList<ReadOnlyMemory<byte>> Requests) : IWireMessage<TxOpMessage>, IVolumeRequest
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.TxOp;

    /// <inheritdoc />
    public IVolumeRequest WithHeader(RequestHeader header) => this with { Header = header };

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Header);
        writer.WriteBytesList(Requests);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static TxOpMessage Read(ref WireReader reader)
    {
        RequestHeader header = reader.ReadRecord<RequestHeader>();
        IReadOnlyList<ReadOnlyMemory<byte>> requests = reader.ReadBytesList();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new TxOpMessage(header, requests) { Extensions = extensions };
    }
}

/// <summary>
/// Reply to <see cref="TxOpMessage"/>.
/// </summary>
/// <param name="Replies">Encoded reply frames, one per executed request (the last one may be an error reply).</param>
public sealed record TxOpResultMessage(
    IReadOnlyList<ReadOnlyMemory<byte>> Replies) : IWireMessage<TxOpResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.TxOpResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteBytesList(Replies);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static TxOpResultMessage Read(ref WireReader reader)
    {
        IReadOnlyList<ReadOnlyMemory<byte>> replies = reader.ReadBytesList();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new TxOpResultMessage(replies) { Extensions = extensions };
    }
}

/// <summary>
/// Commits a transaction (client → volume leader; committed <c>TxCommit</c> entry with the read set and ordered
/// writes). Apply validates every read item against the current state; any change → <see
/// cref="DfsErrorCode.TransactionConflict"/> with no effect, otherwise all writes apply atomically at the commit
/// index (strict serializability). Retries reuse the header operation id; an unknown outcome is resolved with
/// <c>QueryOperation(IsTransaction: true)</c>.
/// </summary>
/// <param name="Header">Request header; <see cref="RequestHeader.Transaction"/> is the transaction and <see cref="RequestHeader.Operation"/> the commit attempt id.</param>
/// <param name="SnapshotName">Commit-and-snapshot: create a snapshot with this name in the same decision; null for none.</param>
public sealed record TxCommitMessage(
    RequestHeader Header,
    string? SnapshotName) : IWireMessage<TxCommitMessage>, IVolumeRequest
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.TxCommit;

    /// <inheritdoc />
    public IVolumeRequest WithHeader(RequestHeader header) => this with { Header = header };

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Header);
        writer.WriteOptionalString(SnapshotName);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static TxCommitMessage Read(ref WireReader reader)
    {
        RequestHeader header = reader.ReadRecord<RequestHeader>();
        string? snapshotName = reader.ReadOptionalString();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new TxCommitMessage(header, snapshotName) { Extensions = extensions };
    }
}

/// <summary>
/// Reply to <see cref="TxCommitMessage"/>.
/// </summary>
/// <param name="Token">Commit token of the transaction.</param>
/// <param name="Snapshot">Snapshot created by commit-and-snapshot, or null.</param>
public sealed record TxCommitResultMessage(
    CommitToken Token,
    SnapshotInfoData? Snapshot) : IWireMessage<TxCommitResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.TxCommitResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteCommitToken(Token);
        writer.WriteOptional(Snapshot);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static TxCommitResultMessage Read(ref WireReader reader)
    {
        CommitToken token = reader.ReadCommitToken();
        SnapshotInfoData? snapshot = reader.ReadOptional<SnapshotInfoData>();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new TxCommitResultMessage(token, snapshot) { Extensions = extensions };
    }
}

/// <summary>
/// Aborts a transaction (client → volume leader; committed <c>TxAbort</c> entry): releases reservations, pins and the
/// staging session. Never reverses a committed decision: aborting a committed transaction reports its committed
/// state. Idempotent.
/// </summary>
/// <param name="Header">Request header; <see cref="RequestHeader.Transaction"/> identifies the transaction.</param>
public sealed record TxAbortMessage(
    RequestHeader Header) : IWireMessage<TxAbortMessage>, IVolumeRequest
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.TxAbort;

    /// <inheritdoc />
    public IVolumeRequest WithHeader(RequestHeader header) => this with { Header = header };

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Header);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static TxAbortMessage Read(ref WireReader reader)
    {
        RequestHeader header = reader.ReadRecord<RequestHeader>();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new TxAbortMessage(header) { Extensions = extensions };
    }
}

/// <summary>
/// Reply to <see cref="TxAbortMessage"/>.
/// </summary>
/// <param name="FinalStatus">Final state: Aborted, or Committed when the transaction had already committed.</param>
/// <param name="Token">Commit token when committed; otherwise the abort entry's commit (or <see cref="CommitToken.None"/>).</param>
public sealed record TxAbortResultMessage(
    WireOutcomeStatus FinalStatus,
    CommitToken Token) : IWireMessage<TxAbortResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.TxAbortResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteDefinedEnum(FinalStatus);
        writer.WriteCommitToken(Token);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static TxAbortResultMessage Read(ref WireReader reader)
    {
        WireOutcomeStatus finalStatus = reader.ReadEnum<WireOutcomeStatus>();
        CommitToken token = reader.ReadCommitToken();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new TxAbortResultMessage(finalStatus, token) { Extensions = extensions };
    }
}
