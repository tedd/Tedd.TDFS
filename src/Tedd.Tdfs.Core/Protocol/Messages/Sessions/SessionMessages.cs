namespace Tedd.Tdfs.Protocol.Messages;

/// <summary>
/// Opens a client session on a volume (client → volume leader; committed <c>OpenSession</c> entry). Sessions own
/// handles, pins and staging sessions and scope operation-id sequences. <see cref="RequestHeader.Session"/> and
/// <see cref="RequestHeader.Operation"/> are empty. Retries reuse <paramref name="Nonce"/>: while the session created
/// for (principal, nonce) is retained the leader returns it instead of creating another one.
/// </summary>
/// <param name="Header">Request header (volume addressing only).</param>
/// <param name="ClientName">Informational client name (diagnostics).</param>
/// <param name="Nonce">Random 128-bit value chosen once per logical open and reused by its retries.</param>
public sealed record OpenSessionMessage(RequestHeader Header, string ClientName, UInt128 Nonce) : IWireMessage<OpenSessionMessage>, IVolumeRequest
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.OpenSession;

    /// <inheritdoc />
    public IVolumeRequest WithHeader(RequestHeader header) => this with { Header = header };

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Header);
        writer.WriteString(ClientName);
        writer.WriteUInt128(Nonce);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static OpenSessionMessage Read(ref WireReader reader)
    {
        RequestHeader header = reader.ReadRecord<RequestHeader>();
        string clientName = reader.ReadString();
        UInt128 nonce = reader.ReadUInt128();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new OpenSessionMessage(header, clientName, nonce) { Extensions = extensions };
    }
}

/// <summary>Reply to <see cref="OpenSessionMessage"/>.</summary>
/// <param name="Session">Server-issued session number (never reused within the volume epoch).</param>
/// <param name="FirstSequence">First operation sequence the client may allocate (sequences are monotonic per session).</param>
/// <param name="LeaseDuration">Handle lease duration; renew with <c>KeepAlive</c> every third of it.</param>
/// <param name="SessionTimeout">Session expires after this long without a <c>KeepAlive</c>.</param>
/// <param name="ServerWallClock">Leader's wall clock at apply time (informational; client clocks are never authoritative).</param>
/// <param name="MaxOutcomesPerSession">Number of operation outcomes retained per session before the oldest are retired.</param>
/// <param name="OutcomeRetention">Time an outcome is retained after its session expires.</param>
/// <param name="Token">Commit of the <c>OpenSession</c> entry.</param>
public sealed record OpenSessionResultMessage(
    ulong Session,
    ulong FirstSequence,
    TimeSpan LeaseDuration,
    TimeSpan SessionTimeout,
    DateTimeOffset ServerWallClock,
    int MaxOutcomesPerSession,
    TimeSpan OutcomeRetention,
    CommitToken Token) : IWireMessage<OpenSessionResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.OpenSessionResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteVarUInt(Session);
        writer.WriteVarUInt(FirstSequence);
        writer.WriteTimeSpan(LeaseDuration);
        writer.WriteTimeSpan(SessionTimeout);
        writer.WriteDateTimeOffset(ServerWallClock);
        writer.WriteVarInt32(MaxOutcomesPerSession);
        writer.WriteTimeSpan(OutcomeRetention);
        writer.WriteCommitToken(Token);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static OpenSessionResultMessage Read(ref WireReader reader)
    {
        ulong session = reader.ReadVarUInt();
        ulong first = reader.ReadVarUInt();
        TimeSpan lease = reader.ReadTimeSpan();
        TimeSpan timeout = reader.ReadTimeSpan();
        DateTimeOffset wall = reader.ReadDateTimeOffset();
        int maxOutcomes = reader.ReadVarInt32();
        TimeSpan retention = reader.ReadTimeSpan();
        CommitToken token = reader.ReadCommitToken();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new OpenSessionResultMessage(session, first, lease, timeout, wall, maxOutcomes, retention, token) { Extensions = extensions };
    }
}

/// <summary>Advances the GC-protection index of one live-read handle (part of <see cref="KeepAliveMessage"/>).</summary>
/// <param name="Handle">Handle whose pin moves.</param>
/// <param name="PinIndex">New pin index: the client no longer needs objects of revisions older than this index.</param>
public sealed record HandlePinUpdate(HandleId Handle, ulong PinIndex) : IWireRecord<HandlePinUpdate>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 3;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteHandleId(Handle);
        writer.WriteVarUInt(PinIndex);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static HandlePinUpdate Read(ref WireReader reader)
    {
        HandleId handle = reader.ReadHandleId();
        ulong pinIndex = reader.ReadVarUInt();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new HandlePinUpdate(handle, pinIndex) { Extensions = extensions };
    }
}

/// <summary>
/// Renews the session in <see cref="RequestHeader.Session"/>, the leases of its handles and its reader pins (client →
/// volume leader; committed <c>KeepAlive</c> entry). The leader tracks renewals with its monotonic clock; client clocks
/// are never authoritative (LOCK-014). Naturally idempotent.
/// </summary>
/// <param name="Header">Request header; <see cref="RequestHeader.Session"/> is the session being renewed.</param>
/// <param name="Handles">Handles whose leases are renewed (handles not listed are not renewed).</param>
/// <param name="PinUpdates">Pin index advances of live-read handles.</param>
/// <param name="ReaderPins">Snapshot-reader and enumeration pins whose lifetime is renewed.</param>
public sealed record KeepAliveMessage(
    RequestHeader Header,
    IReadOnlyList<HandleId> Handles,
    IReadOnlyList<HandlePinUpdate> PinUpdates,
    IReadOnlyList<ulong> ReaderPins) : IWireMessage<KeepAliveMessage>, IVolumeRequest
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.KeepAlive;

    /// <inheritdoc />
    public IVolumeRequest WithHeader(RequestHeader header) => this with { Header = header };

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Header);
        writer.WriteHandleIdList(Handles);
        writer.WriteList(PinUpdates);
        writer.WriteUInt64List(ReaderPins);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static KeepAliveMessage Read(ref WireReader reader)
    {
        RequestHeader header = reader.ReadRecord<RequestHeader>();
        IReadOnlyList<HandleId> handles = reader.ReadHandleIdList();
        IReadOnlyList<HandlePinUpdate> pins = reader.ReadList<HandlePinUpdate>();
        IReadOnlyList<ulong> readerPins = reader.ReadUInt64List();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new KeepAliveMessage(header, handles, pins, readerPins) { Extensions = extensions };
    }
}

/// <summary>Reply to <see cref="KeepAliveMessage"/>.</summary>
/// <param name="ExpiredHandles">Listed handles that are no longer active (expired or closed): their streams must fault with <see cref="DfsErrorCode.LeaseLost"/>.</param>
/// <param name="ExpiredPins">Listed reader pins that are no longer held (<see cref="DfsErrorCode.PinExpired"/>).</param>
/// <param name="SessionExpired">True when the session itself has expired: every handle, pin and staging session is gone and the client must open a new session.</param>
/// <param name="LeaseDuration">Current lease duration.</param>
/// <param name="ServerWallClock">Leader's wall clock (informational).</param>
public sealed record KeepAliveResultMessage(
    IReadOnlyList<HandleId> ExpiredHandles,
    IReadOnlyList<ulong> ExpiredPins,
    bool SessionExpired,
    TimeSpan LeaseDuration,
    DateTimeOffset ServerWallClock) : IWireMessage<KeepAliveResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.KeepAliveResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteHandleIdList(ExpiredHandles);
        writer.WriteUInt64List(ExpiredPins);
        writer.WriteBool(SessionExpired);
        writer.WriteTimeSpan(LeaseDuration);
        writer.WriteDateTimeOffset(ServerWallClock);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static KeepAliveResultMessage Read(ref WireReader reader)
    {
        IReadOnlyList<HandleId> handles = reader.ReadHandleIdList();
        IReadOnlyList<ulong> pins = reader.ReadUInt64List();
        bool expired = reader.ReadBool();
        TimeSpan lease = reader.ReadTimeSpan();
        DateTimeOffset wall = reader.ReadDateTimeOffset();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new KeepAliveResultMessage(handles, pins, expired, lease, wall) { Extensions = extensions };
    }
}

/// <summary>
/// Queries the durable outcome of an operation id or transaction (client → volume leader; ReadIndex read, or a
/// committed <c>FenceOperation</c> entry when <paramref name="Resolve"/> is set). Only the principal that owns the
/// operation may query it (OUT-004).
/// </summary>
/// <param name="Header">Request header (<see cref="RequestHeader.Operation"/> empty).</param>
/// <param name="Target">Operation id to query; for a transaction, the operation id wrapped by its <see cref="DfsTransactionId"/>.</param>
/// <param name="IsTransaction">True to query the final state of the transaction <paramref name="Target"/> began.</param>
/// <param name="Resolve">True to make the answer definite: an id with no outcome is fenced (recorded as aborted) so a
/// delayed copy can never commit later (OUT-013). Never returns <see cref="WireOutcomeStatus.NotFound"/> or <see cref="WireOutcomeStatus.Pending"/>.</param>
public sealed record QueryOperationMessage(
    RequestHeader Header,
    DfsOperationId Target,
    bool IsTransaction,
    bool Resolve) : IWireMessage<QueryOperationMessage>, IVolumeRequest
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.QueryOperation;

    /// <inheritdoc />
    public IVolumeRequest WithHeader(RequestHeader header) => this with { Header = header };

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Header);
        writer.WriteOperationId(Target);
        writer.WriteBool(IsTransaction);
        writer.WriteBool(Resolve);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static QueryOperationMessage Read(ref WireReader reader)
    {
        RequestHeader header = reader.ReadRecord<RequestHeader>();
        DfsOperationId target = reader.ReadOperationId();
        bool isTx = reader.ReadBool();
        bool resolve = reader.ReadBool();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new QueryOperationMessage(header, target, isTx, resolve) { Extensions = extensions };
    }
}

/// <summary>Reply to <see cref="QueryOperationMessage"/>.</summary>
/// <param name="Target">The queried operation id.</param>
/// <param name="Status">Outcome status.</param>
/// <param name="Token">Commit token of a committed operation (<see cref="CommitToken.None"/> otherwise).</param>
/// <param name="Error">For a completed-with-failure or aborted operation, its error code (<see cref="DfsErrorCode.None"/> for success).</param>
/// <param name="Detail">Human-readable detail.</param>
/// <param name="Operation">Kind of the original operation, when known.</param>
/// <param name="ResultFrame">Encoded success reply frame of the original request (for example an <c>OpenHandleResult</c>),
/// when the outcome record retains it; empty otherwise.</param>
public sealed record QueryOperationResultMessage(
    DfsOperationId Target,
    WireOutcomeStatus Status,
    CommitToken Token,
    DfsErrorCode Error,
    string? Detail,
    DfsOperationKind Operation,
    ReadOnlyMemory<byte> ResultFrame) : IWireMessage<QueryOperationResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.QueryOperationResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteOperationId(Target);
        writer.WriteDefinedEnum(Status);
        writer.WriteCommitToken(Token);
        writer.WriteEnum(Error);
        writer.WriteOptionalString(Detail);
        writer.WriteEnum(Operation);
        writer.WriteBytes(ResultFrame.Span);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static QueryOperationResultMessage Read(ref WireReader reader)
    {
        DfsOperationId target = reader.ReadOperationId();
        WireOutcomeStatus status = reader.ReadEnum<WireOutcomeStatus>();
        CommitToken token = reader.ReadCommitToken();
        DfsErrorCode error = reader.ReadEnumRaw<DfsErrorCode>();
        string? detail = reader.ReadOptionalString();
        DfsOperationKind operation = reader.ReadEnumRaw<DfsOperationKind>();
        ReadOnlyMemory<byte> result = reader.ReadBytesCopy();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new QueryOperationResultMessage(target, status, token, error, detail, operation, result) { Extensions = extensions };
    }
}

/// <summary>
/// Reserves a block of operation sequences in the caller's session (client → volume leader) so that ids handed to the
/// application (<c>DfsVolume.NewOperationIdAsync</c>) are known to the leader and not retired before use.
/// </summary>
/// <param name="Header">Request header; <see cref="RequestHeader.Session"/> is the session to reserve in.</param>
/// <param name="Count">Number of sequences to reserve (1..65536).</param>
public sealed record ReserveOperationIdsMessage(RequestHeader Header, int Count) : IWireMessage<ReserveOperationIdsMessage>, IVolumeRequest
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.ReserveOperationIds;

    /// <inheritdoc />
    public IVolumeRequest WithHeader(RequestHeader header) => this with { Header = header };

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Header);
        writer.WriteVarInt32(Count);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static ReserveOperationIdsMessage Read(ref WireReader reader)
    {
        RequestHeader header = reader.ReadRecord<RequestHeader>();
        int count = reader.ReadVarInt32();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new ReserveOperationIdsMessage(header, count) { Extensions = extensions };
    }
}

/// <summary>Reply to <see cref="ReserveOperationIdsMessage"/>: sequences <c>[FirstSequence, FirstSequence + Count)</c> of <paramref name="Session"/>.</summary>
/// <param name="Session">Session the sequences belong to.</param>
/// <param name="FirstSequence">First reserved sequence.</param>
/// <param name="Count">Number of reserved sequences.</param>
public sealed record ReserveOperationIdsResultMessage(ulong Session, ulong FirstSequence, int Count) : IWireMessage<ReserveOperationIdsResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.ReserveOperationIdsResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteVarUInt(Session);
        writer.WriteVarUInt(FirstSequence);
        writer.WriteVarInt32(Count);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static ReserveOperationIdsResultMessage Read(ref WireReader reader)
    {
        ulong session = reader.ReadVarUInt();
        ulong first = reader.ReadVarUInt();
        int count = reader.ReadVarInt32();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new ReserveOperationIdsResultMessage(session, first, count) { Extensions = extensions };
    }
}
