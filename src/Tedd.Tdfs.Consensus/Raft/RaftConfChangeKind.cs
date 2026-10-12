namespace Tedd.Tdfs.Consensus;

/// <summary>Single-server membership change.</summary>
public enum RaftConfChangeKind
{
    /// <summary>Add a non-voting learner.</summary>
    AddLearner,

    /// <summary>Promote a caught-up learner to voter.</summary>
    PromoteLearner,

    /// <summary>Remove a voter or learner.</summary>
    RemoveNode,
}
