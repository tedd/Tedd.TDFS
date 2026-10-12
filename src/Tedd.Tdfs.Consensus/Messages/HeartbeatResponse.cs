namespace Tedd.Tdfs.Consensus;

/// <summary>Answer to a <see cref="Heartbeat"/>.</summary>
/// <param name="From">Follower.</param>
/// <param name="To">Leader.</param>
/// <param name="Term">Follower's term.</param>
/// <param name="ReadContext">Echo of the heartbeat's ReadIndex context.</param>
public sealed record HeartbeatResponse(NodeId From, NodeId To, ulong Term, ulong ReadContext = 0) : RaftMessage(From, To, Term);
