namespace Tedd.Tdfs.Consensus;

/// <summary>Log replication from the leader.</summary>
/// <param name="From">Leader.</param>
/// <param name="To">Follower or learner.</param>
/// <param name="Term">Leader's term.</param>
/// <param name="PrevLogIndex">Index of the entry immediately preceding <paramref name="Entries"/>.</param>
/// <param name="PrevLogTerm">Term of the entry at <paramref name="PrevLogIndex"/>.</param>
/// <param name="Entries">Entries to append (may be empty).</param>
/// <param name="Commit">Leader's commit index.</param>
/// <param name="ReadContext">ReadIndex context being confirmed (0 = none).</param>
public sealed record AppendEntries(NodeId From, NodeId To, ulong Term, ulong PrevLogIndex, ulong PrevLogTerm, IReadOnlyList<RaftEntry> Entries,
    ulong Commit, ulong ReadContext = 0) : RaftMessage(From, To, Term);
