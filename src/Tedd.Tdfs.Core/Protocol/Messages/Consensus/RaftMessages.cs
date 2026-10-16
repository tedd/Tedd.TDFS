namespace Tedd.Tdfs.Protocol.Messages;

/// <summary>
/// Raft messages from one node to another, batched per destination (node → node). Normally sent one-way; when sent as
/// a request it is answered by <see cref="RaftBatchResultMessage"/>. Receivers drop messages from unknown or removed
/// members, from an incarnation older than the registered one, from a process whose incarnation equals the registered
/// one but whose boot id differs (a cloned disk, RSY-021), and for groups the receiver does not host. A newer, not yet
/// registered incarnation is accepted: a restarted voter must take part in the elections and commits that register it
/// (after a full-cluster restart every voter is in that state). The sender must persist hard state and entries before
/// sending messages that depend on them (CON-004).
/// </summary>
/// <param name="From">Sending node (must equal the authenticated connection identity).</param>
/// <param name="Incarnation">Sending node incarnation.</param>
/// <param name="Messages">Messages in send order.</param>
public sealed record RaftBatchMessage(
    NodeId From,
    ulong Incarnation,
    IReadOnlyList<RaftEnvelope> Messages) : IWireMessage<RaftBatchMessage>
{
    /// <summary>
    /// Extension field id of <see cref="CapabilityClock"/> (optional; value: the sender's data-capability clock as UTC ticks,
    /// 8 bytes little-endian). An extension because the message shipped without it (PROTOCOL.md §2.5): a receiver that does
    /// not know it ignores it (FMT-003), and a batch without it simply carries no clock sample.
    /// </summary>
    public const uint CapabilityClockExtension = 1;

    /// <summary>Optional extensions preserved from the wire (re-emitted on encode), including <see cref="CapabilityClockExtension"/>.</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <summary>
    /// The sender's data-capability clock when it sent the batch (PROTOCOL.md §7.3), from <see cref="CapabilityClockExtension"/>;
    /// null when absent or malformed.
    /// </summary>
    public DateTimeOffset? CapabilityClock
    {
        get
        {
            foreach (WireExtension extension in Extensions)
            {
                if (extension.FieldId != CapabilityClockExtension || extension.Value.Length != sizeof(long))
                    continue;
                long ticks = System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(extension.Value.Span);
                return ticks >= DateTimeOffset.MinValue.UtcTicks && ticks <= DateTimeOffset.MaxValue.UtcTicks
                    ? new DateTimeOffset(ticks, TimeSpan.Zero)
                    : null;
            }

            return null;
        }
    }

    /// <summary>A copy that carries <paramref name="clock"/> as its <see cref="CapabilityClockExtension"/>.</summary>
    public RaftBatchMessage WithCapabilityClock(DateTimeOffset clock)
    {
        byte[] value = new byte[sizeof(long)];
        System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(value, clock.UtcTicks);
        WireExtension[] others = [.. Extensions.Where(e => e.FieldId != CapabilityClockExtension)];
        return this with { Extensions = [new WireExtension(CapabilityClockExtension, Required: false, value), .. others] };
    }

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.RaftBatch;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteNodeId(From);
        writer.WriteVarUInt(Incarnation);
        writer.WriteList(Messages);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static RaftBatchMessage Read(ref WireReader reader)
    {
        NodeId from = reader.ReadNodeId();
        ulong incarnation = reader.ReadVarUInt();
        IReadOnlyList<RaftEnvelope> messages = reader.ReadList<RaftEnvelope>();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new RaftBatchMessage(from, incarnation, messages) { Extensions = extensions };
    }
}

/// <summary>
/// Reply to a <see cref="RaftBatchMessage"/> sent as a request.
/// </summary>
/// <param name="Receiver">Receiving node.</param>
/// <param name="ReceiverIncarnation">Receiving node incarnation.</param>
/// <param name="Accepted">Number of messages delivered to a hosted group.</param>
/// <param name="UnknownGroups">Groups named in the batch that the receiver does not host.</param>
public sealed record RaftBatchResultMessage(
    NodeId Receiver,
    ulong ReceiverIncarnation,
    int Accepted,
    IReadOnlyList<string> UnknownGroups) : IWireMessage<RaftBatchResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.RaftBatchResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteNodeId(Receiver);
        writer.WriteVarUInt(ReceiverIncarnation);
        writer.WriteVarInt32(Accepted);
        writer.WriteStringList(UnknownGroups);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static RaftBatchResultMessage Read(ref WireReader reader)
    {
        NodeId receiver = reader.ReadNodeId();
        ulong receiverIncarnation = reader.ReadVarUInt();
        int accepted = reader.ReadVarInt32();
        IReadOnlyList<string> unknownGroups = reader.ReadStringList();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new RaftBatchResultMessage(receiver, receiverIncarnation, accepted, unknownGroups) { Extensions = extensions };
    }
}

/// <summary>
/// One chunk of a Raft snapshot transfer as a request/response pair (leader → lagging follower), giving the sender
/// flow control for large checkpoints; equivalent to an InstallSnapshot message plus its response. Chunks are sent in
/// offset order; the follower installs the snapshot atomically after the final chunk and reports the next expected
/// offset (equal to the snapshot length once installed).
/// </summary>
/// <param name="From">Leader node.</param>
/// <param name="Incarnation">Leader node incarnation.</param>
/// <param name="Group">Group name.</param>
/// <param name="To">Follower node.</param>
/// <param name="Term">Leader term.</param>
/// <param name="Meta">Snapshot metadata.</param>
/// <param name="Offset">Byte offset of <c>Data</c> in the snapshot payload.</param>
/// <param name="Data">Chunk bytes.</param>
/// <param name="Done">True for the final chunk.</param>
public sealed record RaftSnapshotChunkMessage(
    NodeId From,
    ulong Incarnation,
    string Group,
    NodeId To,
    ulong Term,
    RaftWireSnapshotMeta Meta,
    long Offset,
    ReadOnlyMemory<byte> Data,
    bool Done) : IWireMessage<RaftSnapshotChunkMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.RaftSnapshotChunk;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteNodeId(From);
        writer.WriteVarUInt(Incarnation);
        writer.WriteString(Group);
        writer.WriteNodeId(To);
        writer.WriteVarUInt(Term);
        writer.WriteRecord(Meta);
        writer.WriteVarInt(Offset);
        writer.WriteBytes(Data.Span);
        writer.WriteBool(Done);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static RaftSnapshotChunkMessage Read(ref WireReader reader)
    {
        NodeId from = reader.ReadNodeId();
        ulong incarnation = reader.ReadVarUInt();
        string group = reader.ReadString();
        NodeId to = reader.ReadNodeId();
        ulong term = reader.ReadVarUInt();
        RaftWireSnapshotMeta meta = reader.ReadRecord<RaftWireSnapshotMeta>();
        long offset = reader.ReadVarInt();
        ReadOnlyMemory<byte> data = reader.ReadBytesCopy();
        bool done = reader.ReadBool();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new RaftSnapshotChunkMessage(from, incarnation, group, to, term, meta, offset, data, done) { Extensions = extensions };
    }
}

/// <summary>
/// Reply to <see cref="RaftSnapshotChunkMessage"/>.
/// </summary>
/// <param name="Term">Follower term.</param>
/// <param name="SnapshotIndex">Index of the snapshot being transferred.</param>
/// <param name="NextOffset">Next byte offset the follower expects.</param>
/// <param name="Reject">True when the chunk was refused (stale snapshot, out-of-order offset, install failure).</param>
public sealed record RaftSnapshotChunkResultMessage(
    ulong Term,
    ulong SnapshotIndex,
    long NextOffset,
    bool Reject) : IWireMessage<RaftSnapshotChunkResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.RaftSnapshotChunkResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteVarUInt(Term);
        writer.WriteVarUInt(SnapshotIndex);
        writer.WriteVarInt(NextOffset);
        writer.WriteBool(Reject);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static RaftSnapshotChunkResultMessage Read(ref WireReader reader)
    {
        ulong term = reader.ReadVarUInt();
        ulong snapshotIndex = reader.ReadVarUInt();
        long nextOffset = reader.ReadVarInt();
        bool reject = reader.ReadBool();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new RaftSnapshotChunkResultMessage(term, snapshotIndex, nextOffset, reject) { Extensions = extensions };
    }
}
