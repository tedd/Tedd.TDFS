namespace Tedd.Tdfs.Consensus;

/// <summary>Answer to an <see cref="InstallSnapshot"/> chunk.</summary>
/// <param name="From">Follower.</param>
/// <param name="To">Leader.</param>
/// <param name="Term">Follower's term.</param>
/// <param name="SnapshotIndex">Index of the snapshot being transferred.</param>
/// <param name="NextOffset">Next byte offset the follower expects (equals the payload length once installed).</param>
/// <param name="Reject">True when the chunk was refused (stale snapshot, out-of-order offset or install failure).</param>
public sealed record InstallSnapshotResponse(NodeId From, NodeId To, ulong Term, ulong SnapshotIndex, long NextOffset, bool Reject)
    : RaftMessage(From, To, Term);
