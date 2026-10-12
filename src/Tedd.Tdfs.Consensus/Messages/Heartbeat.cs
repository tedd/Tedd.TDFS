namespace Tedd.Tdfs.Consensus;

/// <summary>Leader heartbeat (also used to confirm leadership for ReadIndex).</summary>
/// <param name="From">Leader.</param>
/// <param name="To">Follower or learner.</param>
/// <param name="Term">Leader's term.</param>
/// <param name="Commit">Commit index the follower may adopt (bounded by its matched index).</param>
/// <param name="ReadContext">ReadIndex context being confirmed (0 = none).</param>
public sealed record Heartbeat(NodeId From, NodeId To, ulong Term, ulong Commit, ulong ReadContext = 0) : RaftMessage(From, To, Term);
