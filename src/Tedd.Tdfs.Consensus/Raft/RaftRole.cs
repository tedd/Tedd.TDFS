namespace Tedd.Tdfs.Consensus;

/// <summary>Role of a Raft node in its group.</summary>
public enum RaftRole
{
    /// <summary>Follows a leader; votes in elections.</summary>
    Follower,

    /// <summary>Probing whether an election could succeed (PreVote) without disturbing the term.</summary>
    PreCandidate,

    /// <summary>Campaigning for leadership in a new term.</summary>
    Candidate,

    /// <summary>Accepts proposals and replicates the log.</summary>
    Leader,

    /// <summary>Non-voting member that receives the log.</summary>
    Learner,
}
