namespace Tedd.Tdfs.Consensus;

/// <summary>Answer to a <see cref="VoteRequest"/>.</summary>
/// <param name="From">Voter.</param>
/// <param name="To">Candidate.</param>
/// <param name="Term">Voter's term (for a granted PreVote: the request's term).</param>
/// <param name="PreVote">True when answering a PreVote probe.</param>
/// <param name="Reject">True when the vote was not granted.</param>
public sealed record VoteResponse(NodeId From, NodeId To, ulong Term, bool PreVote, bool Reject) : RaftMessage(From, To, Term);
