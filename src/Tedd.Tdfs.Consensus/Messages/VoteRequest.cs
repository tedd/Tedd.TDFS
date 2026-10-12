namespace Tedd.Tdfs.Consensus;

/// <summary>Request for a (pre-)vote.</summary>
/// <param name="From">Candidate.</param>
/// <param name="To">Voter.</param>
/// <param name="Term">Election term (for PreVote: the term the candidate would use).</param>
/// <param name="PreVote">True for a PreVote probe that does not change any term.</param>
/// <param name="LastLogIndex">Candidate's last log index.</param>
/// <param name="LastLogTerm">Term of the candidate's last log entry.</param>
/// <param name="LeadershipTransfer">True when the campaign was triggered by <see cref="TimeoutNow"/> (voters ignore their leader lease).</param>
public sealed record VoteRequest(NodeId From, NodeId To, ulong Term, bool PreVote, ulong LastLogIndex, ulong LastLogTerm, bool LeadershipTransfer = false)
    : RaftMessage(From, To, Term);
