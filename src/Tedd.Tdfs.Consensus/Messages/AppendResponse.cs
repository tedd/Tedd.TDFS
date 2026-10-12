namespace Tedd.Tdfs.Consensus;

/// <summary>Answer to an <see cref="AppendEntries"/>.</summary>
/// <param name="From">Follower.</param>
/// <param name="To">Leader.</param>
/// <param name="Term">Follower's term.</param>
/// <param name="Reject">True when the log did not match at the previous index.</param>
/// <param name="MatchIndex">On success: highest index known to match the leader's log.</param>
/// <param name="RejectHint">On rejection: the index the leader should probe next (fast backtrack).</param>
/// <param name="ReadContext">Echo of the request's ReadIndex context.</param>
/// <param name="RejectHintTerm">On rejection: the follower's term at <paramref name="RejectHint"/> (0 when unknown).</param>
public sealed record AppendResponse(NodeId From, NodeId To, ulong Term, bool Reject, ulong MatchIndex, ulong RejectHint, ulong ReadContext = 0,
    ulong RejectHintTerm = 0) : RaftMessage(From, To, Term);
