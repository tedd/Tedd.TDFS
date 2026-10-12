namespace Tedd.Tdfs.Protocol.Messages;

/// <summary>
/// Administrative request (admin client → any live node; control-group operations are forwarded to or must be sent to
/// the control leader, which answers NotLeader with a hint otherwise). The body is the encoded body record of
/// <c>Op</c> (see <see cref="AdminOp"/>). Mutating operations are deduplicated by <c>RequestId</c> while the control
/// leader retains it. Requires ClusterAdmin (volume operations accept VolumeAdmin on that volume). Unknown operations
/// are answered with <see cref="DfsErrorCode.Unsupported"/>; failures use the error reply.
/// </summary>
/// <param name="Op">Operation.</param>
/// <param name="RequestId">Random id chosen once per logical request and reused by retries.</param>
/// <param name="Body">Encoded request body record.</param>
public sealed record AdminRequestMessage(
    AdminOp Op,
    UInt128 RequestId,
    ReadOnlyMemory<byte> Body) : IWireMessage<AdminRequestMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.AdminRequest;

    /// <summary>Creates a request carrying <paramref name="body"/>.</summary>
    public static AdminRequestMessage Create<TBody>(AdminOp op, UInt128 requestId, TBody body) where TBody : IWireRecord<TBody>
        => new(op, requestId, WireRecordCodec.Encode(body));

    /// <summary>Decodes the body as <typeparamref name="TBody"/>.</summary>
    /// <exception cref="DfsProtocolException">The body is malformed.</exception>
    public TBody ReadBody<TBody>(WireLimits? limits = null) where TBody : IWireRecord<TBody> => WireRecordCodec.Decode<TBody>(Body.Span, limits);

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteEnum(Op);
        writer.WriteUInt128(RequestId);
        writer.WriteBytes(Body.Span);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static AdminRequestMessage Read(ref WireReader reader)
    {
        AdminOp op = reader.ReadEnumRaw<AdminOp>();
        UInt128 requestId = reader.ReadUInt128();
        ReadOnlyMemory<byte> body = reader.ReadBytesCopy();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new AdminRequestMessage(op, requestId, body) { Extensions = extensions };
    }
}

/// <summary>
/// Successful reply to <see cref="AdminRequestMessage"/>.
/// </summary>
/// <param name="Op">Operation answered.</param>
/// <param name="Body">Encoded result body record.</param>
public sealed record AdminResultMessage(
    AdminOp Op,
    ReadOnlyMemory<byte> Body) : IWireMessage<AdminResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.AdminResult;

    /// <summary>Creates a result carrying <paramref name="body"/>.</summary>
    public static AdminResultMessage Create<TBody>(AdminOp op, TBody body) where TBody : IWireRecord<TBody>
        => new(op, WireRecordCodec.Encode(body));

    /// <summary>Decodes the body as <typeparamref name="TBody"/>.</summary>
    /// <exception cref="DfsProtocolException">The body is malformed.</exception>
    public TBody ReadBody<TBody>(WireLimits? limits = null) where TBody : IWireRecord<TBody> => WireRecordCodec.Decode<TBody>(Body.Span, limits);

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteEnum(Op);
        writer.WriteBytes(Body.Span);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static AdminResultMessage Read(ref WireReader reader)
    {
        AdminOp op = reader.ReadEnumRaw<AdminOp>();
        ReadOnlyMemory<byte> body = reader.ReadBytesCopy();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new AdminResultMessage(op, body) { Extensions = extensions };
    }
}
