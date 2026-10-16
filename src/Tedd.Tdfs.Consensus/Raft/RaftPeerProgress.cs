namespace Tedd.Tdfs.Consensus;

/// <summary>Replication state of a follower as tracked by the leader.</summary>
public enum RaftProgressState
{
    /// <summary>The leader is searching for the last matching index; at most one append in flight.</summary>
    Probe,

    /// <summary>Optimistic pipelined replication (bounded by <see cref="RaftConfig.MaxInflightAppends"/>).</summary>
    Replicate,

    /// <summary>A snapshot is being transferred; appends are paused.</summary>
    Snapshot,
}

/// <summary>Leader-side view of one peer (for hosts deciding learner promotion or diagnostics).</summary>
/// <param name="Peer">Peer id.</param>
/// <param name="Match">Highest index known to be replicated on the peer.</param>
/// <param name="Next">Next index the leader will send.</param>
/// <param name="State">Replication state.</param>
/// <param name="RecentActive">True when the peer responded during the current or the previous CheckQuorum interval.</param>
/// <param name="IsLearner">True for a non-voting member.</param>
public sealed record RaftPeerProgress(NodeId Peer, ulong Match, ulong Next, RaftProgressState State, bool RecentActive, bool IsLearner);
