namespace Tedd.Tdfs.Consensus;

/// <summary>Snapshot of a <see cref="RaftNode"/>'s internal state for invariant checkers.</summary>
/// <param name="Self">Node id.</param>
/// <param name="Role">Role.</param>
/// <param name="Term">Current term.</param>
/// <param name="Vote">Vote in the current term.</param>
/// <param name="Leader">Known leader.</param>
/// <param name="Commit">Commit index.</param>
/// <param name="Applied">Applied index.</param>
/// <param name="LastIndex">Last log index.</param>
/// <param name="LastTerm">Term of the last log entry.</param>
/// <param name="Conf">Current membership.</param>
public sealed record RaftNodeDebugState(NodeId Self, RaftRole Role, ulong Term, NodeId? Vote, NodeId? Leader, ulong Commit, ulong Applied,
    ulong LastIndex, ulong LastTerm, RaftConfState Conf);
