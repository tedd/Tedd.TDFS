using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Control;
using Tedd.Tdfs.Server.Raft;
using Tedd.Tdfs.Server.Runtime;

namespace Tedd.Tdfs.Server.Volumes;

/// <summary>What a volume state machine is created from (INTERNALS.md §1.2).</summary>
internal sealed class VolumeHostContext
{
    /// <summary>Services of the hosting node (not for use inside deterministic apply).</summary>
    public required NodeServices Services { get; init; }

    /// <summary>The control record of the volume when the group was opened.</summary>
    public required VolumeRecord Record { get; init; }

    /// <summary>Wire group name (<c>vol:{id}</c>).</summary>
    public required string GroupName { get; init; }

    /// <summary>Immutable creation data: the only input of the deterministic initial state.</summary>
    public VolumeCreation Creation => Record.Creation;

    /// <summary>Cluster id (for commit tokens).</summary>
    public Guid ClusterId => Services.ClusterId;
}

/// <summary>
/// Creates the replicated state machine of a volume group. The volume engineers provide the production implementation
/// (the real <c>VolumeStateMachine</c>) by implementing the partial method
/// <c>VolumeHost.CreateDefaultStateMachineFactory</c>; tests may inject one through <c>NodeEnvironment.VolumeStateMachines</c>.
/// </summary>
internal interface IVolumeStateMachineFactory
{
    /// <summary>
    /// Creates the state machine in its initial state — the state at log index 0 — derived only from
    /// <see cref="VolumeHostContext.Creation"/> and <see cref="VolumeHostContext.ClusterId"/>, so every replica (including
    /// members added long after creation) starts identical. A restored checkpoint replaces this state.
    /// </summary>
    IReplicatedStateMachine Create(VolumeHostContext context);
}

/// <summary>Optional inspection surface of a volume state machine (NodeInspector).</summary>
internal interface IVolumeStateInspection
{
    /// <summary>This replica's applied state view.</summary>
    VolumeStateInspection Inspect();

    /// <summary>Bytes held under open staging sessions (as far as the state knows).</summary>
    long StagedBytes { get; }

    /// <summary>Bytes retained only by pins.</summary>
    long PinnedBytes { get; }
}

/// <summary>
/// Per-volume composition on one node (INTERNALS.md §1): the volume's Raft group, its state machine and the leader-only
/// jobs. The runtime part reconciles the group's membership with the control record when this node leads it; volume
/// features attach through the partial hooks (<c>OnOpened</c>, <c>OnClosing</c>, <c>OnRecordChanged</c>,
/// <c>OnLeadershipChanged</c>) in their own partial files.
/// </summary>
internal sealed partial class VolumeHost
{
    private readonly NodeServices _s;

    public VolumeHost(NodeServices services, VolumeRecord record, GroupRuntime group, IReplicatedStateMachine stateMachine)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(record);
        ArgumentNullException.ThrowIfNull(group);
        ArgumentNullException.ThrowIfNull(stateMachine);
        _s = services;
        Record = record;
        Group = group;
        StateMachine = stateMachine;
    }

    /// <summary>The volume's current control record.</summary>
    public VolumeRecord Record { get; private set; }

    public VolumeId Id => Record.Id;

    public string Name => Record.Name;

    /// <summary>The volume's Raft group on this node.</summary>
    public GroupRuntime Group { get; }

    /// <summary>The volume state machine.</summary>
    public IReplicatedStateMachine StateMachine { get; }

    /// <summary>True when this node leads the volume group.</summary>
    public bool IsLeader => Group.IsLeader;

    /// <summary>
    /// Production factory hook: the volume state machine implementation assigns its factory here (implement this partial
    /// method in a partial of <see cref="VolumeHost"/>). Left unimplemented, volume groups are hosted only when a factory is
    /// injected through <c>NodeEnvironment.VolumeStateMachines</c>.
    /// </summary>
    static partial void CreateDefaultStateMachineFactory(ref IVolumeStateMachineFactory? factory);

    /// <summary>The production factory, or null when this build has none.</summary>
    public static IVolumeStateMachineFactory? ResolveDefaultFactory()
    {
        IVolumeStateMachineFactory? factory = null;
        CreateDefaultStateMachineFactory(ref factory);
        return factory;
    }

    /// <summary>Called once after the group was opened.</summary>
    partial void OnOpened();

    /// <summary>Called before the group is closed.</summary>
    partial void OnClosing();

    /// <summary>Called when the control record of the volume changed.</summary>
    partial void OnRecordChanged(VolumeRecord previous);

    /// <summary>Called when the group's role, leader or term changed.</summary>
    partial void OnLeadershipChanged();

    internal void NotifyOpened() => OnOpened();

    internal void NotifyClosing() => OnClosing();

    internal void NotifyLeadershipChanged() => OnLeadershipChanged();

    internal void UpdateRecord(VolumeRecord record)
    {
        if (ReferenceEquals(record, Record))
            return;
        VolumeRecord previous = Record;
        Record = record;
        OnRecordChanged(previous);
    }

    /// <summary>
    /// Leader-only: moves the group's membership one single-server change towards the record's desired voters/learners
    /// (add learner → caught up → promote → remove; removed cluster members leave first). False when nothing was done.
    /// </summary>
    public async Task<bool> ReconcileMembershipAsync()
    {
        if (!Group.IsLeader || Group.Node.HasPendingConfChange)
            return false;
        ControlState state = _s.Control.State;
        RaftConfState conf = Group.Conf;
        IReadOnlyList<NodeId> desiredVoters = Record.Voters;
        IReadOnlyList<NodeId> desiredLearners = Record.Learners;
        bool Desired(NodeId id) => desiredVoters.Contains(id) || desiredLearners.Contains(id);

        foreach (NodeId id in conf.Voters.Concat(conf.Learners))
        {
            if (!Desired(id) && state.FindMember(id) is not { IsLive: true } && !(conf.IsVoter(id) && conf.Voters.Count == 1))
                return await ChangeAsync(RaftConfChangeKind.RemoveNode, id, "removed member");
        }

        foreach (NodeId id in desiredVoters.Concat(desiredLearners))
        {
            if (!conf.Contains(id))
                return await ChangeAsync(RaftConfChangeKind.AddLearner, id, "desired member");
        }

        ulong commit = Group.CommitIndex;
        foreach (RaftPeerProgress progress in Group.GetProgress())
        {
            if (desiredVoters.Contains(progress.Peer) && _s.Membership.IsCaughtUp(progress, commit))
                return await ChangeAsync(RaftConfChangeKind.PromoteLearner, progress.Peer, "caught-up desired voter");
        }

        int desiredPresent = conf.Voters.Count(desiredVoters.Contains);
        foreach (NodeId id in conf.Voters.Concat(conf.Learners))
        {
            if (Desired(id))
                continue;
            if (conf.IsVoter(id) && desiredPresent < desiredVoters.Count)
                continue; // keep the old voter until every desired voter votes
            if (conf.IsVoter(id) && conf.Voters.Count == 1)
                continue;
            return await ChangeAsync(RaftConfChangeKind.RemoveNode, id, "no longer desired");
        }

        return false;
    }

    private async Task<bool> ChangeAsync(RaftConfChangeKind kind, NodeId node, string reason)
    {
        _s.Info("volume", NodeServices.Inv($"Volume {Name}: {kind} {_s.Control.NameOf(node)} ({reason})."));

        // A deadline: the fate of a change appended just before losing leadership may stay open in an idle group, and the
        // node-wide reconciliation job awaiting it would stop serving every other volume.
        using CancellationTokenSource deadline = _s.CreateDeadline();
        return await Group.ProposeConfChangeAsync(new RaftConfChange(kind, node), deadline.Token);
    }
}
