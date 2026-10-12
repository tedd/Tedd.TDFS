namespace Tedd.Tdfs.Consensus;

/// <summary>
/// Pure Raft state machine (etcd/raft style): no I/O and no clock. Driven by <see cref="Tick"/>, <see cref="Step"/> and
/// proposals; produces <see cref="RaftReady"/> work items that the host persists, sends and applies before calling
/// <see cref="Advance"/>. Supports PreVote, CheckQuorum, ReadIndex, InstallSnapshot, learners, single-server membership
/// changes and leadership transfer.
/// </summary>
public sealed class RaftNode
{
    /// <summary>Creates a node from persisted state.</summary>
    /// <param name="config">Static configuration.</param>
    /// <param name="hardState">Persisted term/vote/commit.</param>
    /// <param name="confState">Current membership.</param>
    /// <param name="log">Durable log view (entries persisted so far and the latest snapshot).</param>
    /// <param name="random">Source for randomized election timeouts.</param>
    public RaftNode(RaftConfig config, RaftHardState hardState, RaftConfState confState, IRaftLogView log, IRandomSource random)
    {
        ArgumentNullException.ThrowIfNull(config);
        ArgumentNullException.ThrowIfNull(confState);
        ArgumentNullException.ThrowIfNull(log);
        ArgumentNullException.ThrowIfNull(random);
        Config = config;
        _ = hardState;
    }

    /// <summary>The node's configuration.</summary>
    public RaftConfig Config { get; }

    /// <summary>Current role.</summary>
    public RaftRole Role => throw new NotImplementedException("RaftNode.Role: implemented in a later phase");

    /// <summary>Current term.</summary>
    public ulong Term => throw new NotImplementedException("RaftNode.Term: implemented in a later phase");

    /// <summary>Known leader, if any.</summary>
    public NodeId? Leader => throw new NotImplementedException("RaftNode.Leader: implemented in a later phase");

    /// <summary>Highest committed index.</summary>
    public ulong CommitIndex => throw new NotImplementedException("RaftNode.CommitIndex: implemented in a later phase");

    /// <summary>Highest index handed out for application.</summary>
    public ulong AppliedIndex => throw new NotImplementedException("RaftNode.AppliedIndex: implemented in a later phase");

    /// <summary>Current membership.</summary>
    public RaftConfState ConfState => throw new NotImplementedException("RaftNode.ConfState: implemented in a later phase");

    /// <summary>Internal state for invariant checkers.</summary>
    public RaftNodeDebugState DebugState => throw new NotImplementedException("RaftNode.DebugState: implemented in a later phase");

    /// <summary>Advances the logical clock by one tick (elections and heartbeats).</summary>
    public void Tick() => throw new NotImplementedException("RaftNode.Tick: implemented in a later phase");

    /// <summary>Processes an inbound message.</summary>
    public void Step(RaftMessage message) => throw new NotImplementedException("RaftNode.Step: implemented in a later phase");

    /// <summary>Proposes a command; false when not leader or a leadership transfer is in progress.</summary>
    public bool TryPropose(ReadOnlyMemory<byte> data, out ulong index) => throw new NotImplementedException("RaftNode.TryPropose: implemented in a later phase");

    /// <summary>Proposes a membership change; false when not leader or another change is pending.</summary>
    public bool TryProposeConfChange(RaftConfChange change, out ulong index) => throw new NotImplementedException("RaftNode.TryProposeConfChange: implemented in a later phase");

    /// <summary>Requests a linearizable read point; the result appears in <see cref="RaftReady.ReadStates"/> (dropped when not leader).</summary>
    public void ReadIndex(ulong requestContext) => throw new NotImplementedException("RaftNode.ReadIndex: implemented in a later phase");

    /// <summary>Transfers leadership to <paramref name="target"/> (TimeoutNow once it is caught up).</summary>
    public void TransferLeadership(NodeId target) => throw new NotImplementedException("RaftNode.TransferLeadership: implemented in a later phase");

    /// <summary>True when <see cref="GetReady"/> would return work.</summary>
    public bool HasReady() => throw new NotImplementedException("RaftNode.HasReady: implemented in a later phase");

    /// <summary>Returns pending work; call <see cref="Advance"/> after handling it.</summary>
    public RaftReady GetReady() => throw new NotImplementedException("RaftNode.GetReady: implemented in a later phase");

    /// <summary>Acknowledges that a ready was persisted, sent and applied.</summary>
    public void Advance(RaftReady ready) => throw new NotImplementedException("RaftNode.Advance: implemented in a later phase");

    /// <summary>Reports the result of sending a snapshot to <paramref name="peer"/>.</summary>
    public void ReportSnapshotStatus(NodeId peer, bool succeeded) => throw new NotImplementedException("RaftNode.ReportSnapshotStatus: implemented in a later phase");

    /// <summary>Reports that <paramref name="peer"/> could not be reached (pauses optimistic replication).</summary>
    public void ReportUnreachable(NodeId peer) => throw new NotImplementedException("RaftNode.ReportUnreachable: implemented in a later phase");
}
