namespace Tedd.Tdfs.Protocol.Messages;

/// <summary>
/// Common header of every volume-scoped request (namespace, files, sessions, transactions, snapshots, backup
/// administration). It addresses the volume, binds the request to an authority (volume id and epoch), and carries the
/// operation identity used for idempotency (FSD §13).
/// </summary>
/// <param name="Volume">Volume name (the server resolves it through the control state).</param>
/// <param name="ExpectedVolumeId">Volume identity the caller believes it addresses; <see cref="VolumeId"/> empty = any.
/// A mismatch (for example after the name was deleted and re-created) is rejected with
/// <see cref="DfsErrorCode.EpochMismatch"/> so handles, sessions and tokens of another volume are never applied (PATH-014, RST-008).</param>
/// <param name="ExpectedVolumeEpoch">Volume epoch the caller believes is current; 0 = any. Mismatch →
/// <see cref="DfsErrorCode.EpochMismatch"/>.</param>
/// <param name="Session">The caller's own session on this volume (0 = none). Handles, pins and staging sessions created
/// by the request are owned by this session and renewed by its <c>KeepAlive</c>.</param>
/// <param name="Operation">Stable operation id of a mutating request, assigned before the first transmission and reused
/// by every retry (OUT-001); empty for reads. It may belong to another session of the same principal (an id persisted
/// by a previous process).</param>
/// <param name="AckWatermark">Sequence watermark of <see cref="Session"/>: the client will never retry or query any of
/// its operations with a lower sequence, so their outcomes may be retired (ARCHITECTURE §5.2 step 4). 0 = none.</param>
/// <param name="Transaction">Transaction the request belongs to; null outside transactions. Transaction-bound requests are
/// applied to the transaction's private working root and are not visible to anyone else before commit.</param>
public sealed record RequestHeader(
    string Volume,
    VolumeId ExpectedVolumeId = default,
    ulong ExpectedVolumeEpoch = 0,
    ulong Session = 0,
    DfsOperationId Operation = default,
    ulong AckWatermark = 0,
    DfsTransactionId? Transaction = null) : IWireRecord<RequestHeader>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 1 + 16 + 1 + 1 + 2 + 1 + 1 + 1;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteString(Volume);
        writer.WriteVolumeId(ExpectedVolumeId);
        writer.WriteVarUInt(ExpectedVolumeEpoch);
        writer.WriteVarUInt(Session);
        writer.WriteOperationId(Operation);
        writer.WriteVarUInt(AckWatermark);
        writer.WriteOptionalTransactionId(Transaction);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static RequestHeader Read(ref WireReader reader)
    {
        string volume = reader.ReadString();
        VolumeId volumeId = reader.ReadVolumeId();
        ulong epoch = reader.ReadVarUInt();
        ulong session = reader.ReadVarUInt();
        DfsOperationId operation = reader.ReadOperationId();
        ulong ack = reader.ReadVarUInt();
        DfsTransactionId? tx = reader.ReadOptionalTransactionId();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new RequestHeader(volume, volumeId, epoch, session, operation, ack, tx) { Extensions = extensions };
    }
}
