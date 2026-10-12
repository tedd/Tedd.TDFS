namespace Tedd.Tdfs.Consensus;

/// <summary>One chunk of a snapshot sent to a lagging follower.</summary>
/// <param name="From">Leader.</param>
/// <param name="To">Follower or learner.</param>
/// <param name="Term">Leader's term.</param>
/// <param name="Meta">Snapshot metadata.</param>
/// <param name="Offset">Byte offset of <paramref name="Data"/> within the snapshot payload.</param>
/// <param name="Data">Chunk bytes.</param>
/// <param name="Done">True for the final chunk.</param>
public sealed record InstallSnapshot(NodeId From, NodeId To, ulong Term, RaftSnapshotMeta Meta, long Offset, ReadOnlyMemory<byte> Data, bool Done)
    : RaftMessage(From, To, Term);
