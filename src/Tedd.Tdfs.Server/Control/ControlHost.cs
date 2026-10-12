using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Raft;
using Tedd.Tdfs.Server.Runtime;

namespace Tedd.Tdfs.Server.Control;

/// <summary>
/// The control group on this node (every live node is a voter or learner of it, so every node has the cluster map):
/// owns the <see cref="ControlStateMachine"/>, proposes control commands, and answers the control-state queries other
/// components use (members, endpoints, principals, volumes, capabilities, cluster map).
/// </summary>
internal sealed class ControlHost
{
    /// <summary>Directory name of the control group's data.</summary>
    public const string Directory = "control";

    private const int MaxPendingAudits = 64;

    private readonly NodeServices _s;
    private readonly Queue<AppendAuditCommand> _pendingAudits = new();
    private bool _auditing;

    public ControlHost(NodeServices services)
    {
        ArgumentNullException.ThrowIfNull(services);
        _s = services;
    }

    /// <summary>The state machine.</summary>
    public ControlStateMachine StateMachine { get; } = new();

    /// <summary>The replicated state (local applied view).</summary>
    public ControlState State => StateMachine.State;

    /// <summary>The control group (null before it is opened).</summary>
    public GroupRuntime? Group { get; private set; }

    /// <summary>True when this node leads the control group.</summary>
    public bool IsLeader => Group?.IsLeader == true;

    /// <summary>The control leader as known locally.</summary>
    public NodeId? Leader => Group?.Leader;

    /// <summary>This node's member record, when the local view contains it.</summary>
    public MemberRecord? Self => State.FindMember(_s.NodeId);

    /// <summary>Opens the control group (bootstrap entries are written when the log is empty on an initial member).</summary>
    public async Task OpenAsync(IReadOnlyList<RaftEntry>? bootstrapEntries, CancellationToken ct)
    {
        var spec = new GroupOpenSpec(ProtocolInfo.ControlGroup, ProtocolInfo.ControlGroup, null,
            _s.DataPath(StoragePathOf(Directory)), StateMachine, bootstrapEntries);
        Group = await _s.Raft.OpenAsync(spec, ct);
    }

    /// <summary>Path (relative to the data directory) of a group directory.</summary>
    public static string StoragePathOf(string groupDirectory) => NodeServices.GroupsDirectory + "/" + groupDirectory;

    /// <summary>A command header stamped with this (leader) node's wall clock.</summary>
    public CommandHeader NewHeader(UInt128 requestId = default, string principal = "") => new(requestId, principal, _s.Time.GetUtcNow());

    /// <summary>Proposes a control command (this node must lead the control group) and returns its apply outcome.</summary>
    public Task<ApplyOutcome> ProposeAsync(ControlCommand command, CancellationToken ct = default)
    {
        GroupRuntime group = Group ?? throw new DfsUnavailableException(DfsErrorCode.Unavailable, "The control group is not open.");
        return group.ProposeAsync(ControlCommandCodec.Encode(command), new ProposalContext(command.FaultOperation), ct);
    }

    /// <summary>A retryable NotLeader error pointing at the control leader.</summary>
    public DfsUnavailableException NotLeader()
        => Group?.NotLeader() ?? new DfsUnavailableException(DfsErrorCode.NotLeader, "The control group is not open on this node.");

    /// <summary>First endpoint of a member (null when unknown or removed).</summary>
    public string? ResolveEndpoint(NodeId node)
        => State.FindMember(node) is { IsLive: true, Endpoints.Count: > 0 } member ? member.Endpoints[0] : null;

    /// <summary>Name of a member, or its id text.</summary>
    public string NameOf(NodeId node) => State.FindMember(node)?.Name ?? node.ToString();

    /// <summary>Endpoint of the known control leader, or null.</summary>
    public string? LeaderEndpoint => Leader is { } id ? ResolveEndpoint(id) : null;

    /// <summary>True when this node's (incarnation, boot id) is registered in the applied control state (RCV-02).</summary>
    public bool IsSelfRegistered => Self is { } self && self.IsLive && self.Incarnation == _s.Incarnation && self.BootId == _s.BootId;

    /// <summary>Capabilities and limits advertised to clients (DfsServerCapabilities).</summary>
    public DfsServerCapabilities BuildCapabilities()
    {
        NodeTimings t = _s.Timings;
        return new DfsServerCapabilities
        {
            ProtocolVersion = ProtocolInfo.CurrentVersion,
            Features = State.Features.ToHashSet(StringComparer.Ordinal),
            ChunkSize = t.ChunkSize,
            MaxObjectSize = t.MaxObjectSize,
            LeaseDuration = t.LeaseDuration,
        };
    }

    /// <summary>The cluster map built from the local applied control state (ClusterMapData).</summary>
    public ClusterMapData BuildClusterMap()
    {
        ControlState state = State;
        RaftConfState conf = Group?.AppliedConf ?? RaftConfState.Empty;
        var nodes = state.Members.Values
            .OrderBy(m => m.Name, StringComparer.Ordinal)
            .ThenBy(m => m.Id, NodeIdComparer.Instance)
            .Select(m => ToNodeEntry(m, conf))
            .ToArray();
        var volumes = state.Volumes.Values
            .OrderBy(v => v.Name, StringComparer.Ordinal)
            .Select(ToVolumeEntry)
            .ToArray();
        return new ClusterMapData(state.ClusterId, state.ClusterName, state.AppliedIndex, state.PlacementEpoch, nodes, volumes, Leader, BuildCapabilities());
    }

    /// <summary>Map entry of a member as seen from this node.</summary>
    public NodeEntry ToNodeEntry(MemberRecord m, RaftConfState conf)
        => new(m.Id, m.Name, m.Labels, m.Endpoints.ToArray(), m.Status, m.Incarnation, false, conf.IsVoter(m.Id), IsReachable(m.Id),
            m.IsPlacementEligible && m.CapacityBytes - m.UsedBytes > 0, m.CapacityBytes, m.UsedBytes);

    /// <summary>Map entry of a volume (leader hint from this node's view when it hosts the group).</summary>
    public VolumeEntry ToVolumeEntry(VolumeRecord v)
        => new(v.Id, v.Name, v.Epoch, v.State, v.Voters.ToArray(), v.Learners.ToArray(), _s.Raft.LeaderOf(v.GroupName), v.Creation.Metadata,
            v.DefaultPolicy, v.NamedPolicies, v.Creation.InlineThreshold, v.Creation.Limits)
        {
            StoragePool = v.StoragePool,
            EnforceTraverseChecking = v.Creation.EnforceTraverseChecking,
        };

    /// <summary>
    /// Advisory reachability: the control leader's replication view (recently active followers), otherwise an open
    /// authenticated connection from this node.
    /// </summary>
    public bool IsReachable(NodeId node)
    {
        if (node == _s.NodeId)
            return true;
        if (Group is { IsLeader: true } group)
        {
            foreach (RaftPeerProgress progress in group.GetProgress())
            {
                if (progress.Peer == node)
                    return progress.RecentActive;
            }

            return false;
        }

        return _s.Peers.IsConnected(node);
    }

    /// <summary>
    /// Records a security event (failed authentication, denied access) in the replicated audit log when this node leads the
    /// control group; best effort and bounded (floods are dropped).
    /// </summary>
    public void ReportSecurityEvent(string operation, string outcome, string scope, string principal = "-")
    {
        if (!IsLeader || _pendingAudits.Count >= MaxPendingAudits)
            return;
        _pendingAudits.Enqueue(new AppendAuditCommand(NewHeader(default, string.IsNullOrEmpty(principal) ? "-" : principal), operation, outcome, scope));
        if (!_auditing)
            _ = DrainAuditsAsync();
    }

    private async Task DrainAuditsAsync()
    {
        _auditing = true;
        try
        {
            while (_pendingAudits.Count > 0 && IsLeader && !_s.IsStopping)
            {
                AppendAuditCommand command = _pendingAudits.Dequeue();
                try
                {
                    await ProposeAsync(command);
                }
                catch (Exception ex) when (ex is DfsException or OperationCanceledException)
                {
                    _pendingAudits.Clear();
                }
            }
        }
        finally
        {
            _auditing = false;
        }
    }
}
