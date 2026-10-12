namespace Tedd.Tdfs.Protocol.Messages;

/// <summary>
/// One Raft log entry on the wire (mirrors <c>Tedd.Tdfs.Consensus.RaftEntry</c>).
/// </summary>
/// <param name="Index">Log index.</param>
/// <param name="Term">Term of the entry.</param>
/// <param name="Kind">Entry kind as the Consensus <c>RaftEntryKind</c> byte (0 Normal, 1 ConfChange, 2 Noop); validated by the receiver.</param>
/// <param name="Data">Entry payload.</param>
public sealed record RaftWireEntry(
    ulong Index,
    ulong Term,
    byte Kind,
    ReadOnlyMemory<byte> Data) : IWireRecord<RaftWireEntry>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 5;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteVarUInt(Index);
        writer.WriteVarUInt(Term);
        writer.WriteByte(Kind);
        writer.WriteBytes(Data.Span);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static RaftWireEntry Read(ref WireReader reader)
    {
        ulong index = reader.ReadVarUInt();
        ulong term = reader.ReadVarUInt();
        byte kind = reader.ReadByte();
        ReadOnlyMemory<byte> data = reader.ReadBytesCopy();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new RaftWireEntry(index, term, kind, data) { Extensions = extensions };
    }
}

/// <summary>
/// Snapshot metadata on the wire (mirrors <c>RaftSnapshotMeta</c>).
/// </summary>
/// <param name="Index">Last log index covered by the snapshot.</param>
/// <param name="Term">Term of that index.</param>
/// <param name="Voters">Voters of the configuration at the snapshot.</param>
/// <param name="Learners">Learners of the configuration at the snapshot.</param>
public sealed record RaftWireSnapshotMeta(
    ulong Index,
    ulong Term,
    IReadOnlyList<NodeId> Voters,
    IReadOnlyList<NodeId> Learners) : IWireRecord<RaftWireSnapshotMeta>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 5;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteVarUInt(Index);
        writer.WriteVarUInt(Term);
        writer.WriteNodeIdList(Voters);
        writer.WriteNodeIdList(Learners);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static RaftWireSnapshotMeta Read(ref WireReader reader)
    {
        ulong index = reader.ReadVarUInt();
        ulong term = reader.ReadVarUInt();
        IReadOnlyList<NodeId> voters = reader.ReadNodeIdList();
        IReadOnlyList<NodeId> learners = reader.ReadNodeIdList();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new RaftWireSnapshotMeta(index, term, voters, learners) { Extensions = extensions };
    }
}

/// <summary>
/// A Raft message in a neutral, flat layout (in the style of etcd raftpb.Message) so Core does not depend on the
/// Consensus project; the node runtime maps each Consensus message record to and from it. All fields are always
/// encoded; fields not used by <c>Kind</c> are zero/empty. Field use per kind: VoteRequest — LogIndex/LogTerm = last
/// log index/term, PreVote, LeadershipTransfer; VoteResponse — PreVote, Reject; AppendEntries — LogIndex/LogTerm =
/// previous index/term, Entries, Commit, ReadContext; AppendResponse — Reject, LogIndex = match index, RejectHint,
/// RejectHintTerm, ReadContext; InstallSnapshot — Snapshot, Offset, Data, Done; InstallSnapshotResponse — LogIndex =
/// snapshot index, Offset = next offset, Reject; TimeoutNow — none; Heartbeat — Commit, ReadContext;
/// HeartbeatResponse — ReadContext.
/// </summary>
/// <param name="Kind">Message kind.</param>
/// <param name="From">Sender.</param>
/// <param name="To">Recipient.</param>
/// <param name="Term">Sender term (for PreVote requests: the term the sender would campaign in).</param>
/// <param name="LogIndex">Log index field (meaning depends on the kind).</param>
/// <param name="LogTerm">Log term field (meaning depends on the kind).</param>
/// <param name="Commit">Leader commit index.</param>
/// <param name="ReadContext">ReadIndex context being confirmed/echoed (0 = none).</param>
/// <param name="RejectHint">Fast-backtrack probe index (AppendResponse rejections).</param>
/// <param name="RejectHintTerm">Follower term at <c>RejectHint</c> (0 when unknown).</param>
/// <param name="PreVote">PreVote probe/response.</param>
/// <param name="LeadershipTransfer">Campaign triggered by TimeoutNow.</param>
/// <param name="Reject">Rejection flag.</param>
/// <param name="Entries">Entries to append.</param>
/// <param name="Snapshot">Snapshot metadata (InstallSnapshot).</param>
/// <param name="Offset">Snapshot byte offset (chunk offset or next expected offset).</param>
/// <param name="Data">Snapshot chunk bytes.</param>
/// <param name="Done">Final snapshot chunk.</param>
public sealed record RaftWireMessage(
    RaftWireKind Kind,
    NodeId From,
    NodeId To,
    ulong Term,
    ulong LogIndex,
    ulong LogTerm,
    ulong Commit,
    ulong ReadContext,
    ulong RejectHint,
    ulong RejectHintTerm,
    bool PreVote,
    bool LeadershipTransfer,
    bool Reject,
    IReadOnlyList<RaftWireEntry> Entries,
    RaftWireSnapshotMeta? Snapshot,
    long Offset,
    ReadOnlyMemory<byte> Data,
    bool Done) : IWireRecord<RaftWireMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 49;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteDefinedEnum(Kind);
        writer.WriteNodeId(From);
        writer.WriteNodeId(To);
        writer.WriteVarUInt(Term);
        writer.WriteVarUInt(LogIndex);
        writer.WriteVarUInt(LogTerm);
        writer.WriteVarUInt(Commit);
        writer.WriteVarUInt(ReadContext);
        writer.WriteVarUInt(RejectHint);
        writer.WriteVarUInt(RejectHintTerm);
        writer.WriteBool(PreVote);
        writer.WriteBool(LeadershipTransfer);
        writer.WriteBool(Reject);
        writer.WriteList(Entries);
        writer.WriteOptional(Snapshot);
        writer.WriteVarInt(Offset);
        writer.WriteBytes(Data.Span);
        writer.WriteBool(Done);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static RaftWireMessage Read(ref WireReader reader)
    {
        RaftWireKind kind = reader.ReadEnum<RaftWireKind>();
        NodeId from = reader.ReadNodeId();
        NodeId to = reader.ReadNodeId();
        ulong term = reader.ReadVarUInt();
        ulong logIndex = reader.ReadVarUInt();
        ulong logTerm = reader.ReadVarUInt();
        ulong commit = reader.ReadVarUInt();
        ulong readContext = reader.ReadVarUInt();
        ulong rejectHint = reader.ReadVarUInt();
        ulong rejectHintTerm = reader.ReadVarUInt();
        bool preVote = reader.ReadBool();
        bool leadershipTransfer = reader.ReadBool();
        bool reject = reader.ReadBool();
        IReadOnlyList<RaftWireEntry> entries = reader.ReadList<RaftWireEntry>();
        RaftWireSnapshotMeta? snapshot = reader.ReadOptional<RaftWireSnapshotMeta>();
        long offset = reader.ReadVarInt();
        ReadOnlyMemory<byte> data = reader.ReadBytesCopy();
        bool done = reader.ReadBool();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new RaftWireMessage(kind, from, to, term, logIndex, logTerm, commit, readContext, rejectHint, rejectHintTerm, preVote, leadershipTransfer, reject, entries, snapshot, offset, data, done) { Extensions = extensions };
    }
}

/// <summary>
/// One Raft message addressed to a group (element of <see cref="RaftBatchMessage"/>).
/// </summary>
/// <param name="Group">Group name (<see cref="ProtocolInfo.ControlGroup"/> or <see cref="ProtocolInfo.VolumeGroup"/>).</param>
/// <param name="Message">The message.</param>
public sealed record RaftEnvelope(
    string Group,
    RaftWireMessage Message) : IWireRecord<RaftEnvelope>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 51;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteString(Group);
        writer.WriteRecord(Message);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static RaftEnvelope Read(ref WireReader reader)
    {
        string group = reader.ReadString();
        RaftWireMessage message = reader.ReadRecord<RaftWireMessage>();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new RaftEnvelope(group, message) { Extensions = extensions };
    }
}
