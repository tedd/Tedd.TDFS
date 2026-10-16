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

    private const int MaxPendingAudits = 128;

    /// <summary>
    /// Security events a node may audit in a burst (then <see cref="AuditRefillPerSecond"/>): an administrator's session of
    /// denied attempts is recorded completely (SEC-018), while a flood (failed authentications, malformed requests) stays
    /// bounded and cannot swamp the control log.
    /// </summary>
    private const int AuditBurst = 100;

    private const double AuditRefillPerSecond = 20;

    private readonly NodeServices _s;
    private readonly Queue<AppendAuditCommand> _pendingAudits = new();
    private readonly Queue<AuditEventMessage> _forwardedAudits = new();
    private bool _auditing;
    private bool _forwarding;
    private double _auditTokens = AuditBurst;
    private long _auditRefilled;

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
        NodeRelease release = _s.Release;

        // The cluster speaks the newest version every live member speaks (mixed releases negotiate the older one during a
        // rolling upgrade), and only active features this node's release understands are advertised.
        int version = release.ProtocolMax;
        foreach (MemberRecord member in State.Members.Values)
        {
            if (member.IsLive && !member.PendingAdmission && member.ProtocolMax > 0)
                version = Math.Min(version, member.ProtocolMax);
        }

        return new DfsServerCapabilities
        {
            ProtocolVersion = Math.Max(version, release.ProtocolMin),
            Features = State.Features.Where(release.Supports).ToHashSet(StringComparer.Ordinal),
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

    /// <summary>Map entry of a member as seen from this node (including the named pools it belongs to, Addendum A).</summary>
    public NodeEntry ToNodeEntry(MemberRecord m, RaftConfState conf)
        => new(m.Id, m.Name, m.Labels, m.Endpoints.ToArray(), m.Status, m.Incarnation, false, conf.IsVoter(m.Id), IsReachable(m.Id),
            m.IsPlacementEligible && m.CapacityBytes - m.UsedBytes > 0, m.CapacityBytes, m.UsedBytes)
        {
            Pools = PoolsOf(m.Id),
        };

    /// <summary>Named pools (ordinal) whose member list contains <paramref name="node"/>; <c>default</c> is implicit.</summary>
    private System.Collections.Immutable.ImmutableSortedSet<string> PoolsOf(NodeId node)
    {
        var pools = System.Collections.Immutable.ImmutableSortedSet.CreateBuilder<string>(StringComparer.Ordinal);
        foreach (PoolRecord pool in State.Pools.Values)
        {
            if (pool.Nodes.Contains(node))
                pools.Add(pool.Name);
        }

        return pools.ToImmutable();
    }

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
    /// Records a security event (failed authentication, denied access) in the replicated audit log: the control leader
    /// proposes it, any other node forwards it to the control leader (<see cref="AuditEventMessage"/>), so an event is audited
    /// whichever node observed it (SEC-016). Best effort and bounded on every node (floods are dropped).
    /// </summary>
    public void ReportSecurityEvent(string operation, string outcome, string scope, string principal = "-")
    {
        string who = string.IsNullOrEmpty(principal) ? "-" : principal;
        if (IsLeader)
        {
            if (_pendingAudits.Count >= MaxPendingAudits || !TakeAuditToken())
                return;
            _pendingAudits.Enqueue(new AppendAuditCommand(NewHeader(default, who), operation, outcome, scope));
            if (!_auditing)
                _ = DrainAuditsAsync();
            return;
        }

        if (Leader is null || _forwardedAudits.Count >= MaxPendingAudits || !TakeAuditToken())
            return;
        _forwardedAudits.Enqueue(new AuditEventMessage(operation, outcome, scope, who));
        if (!_forwarding)
            _ = ForwardAuditsAsync();
    }

    /// <summary>
    /// Like <see cref="ReportSecurityEvent"/>, but returns once the audit record is applied (or dropped): an administrative
    /// denial is answered only after it is visible in the audit log (read-your-writes for the denied caller's auditor). On a
    /// node that does not lead the control group it returns once the control leader applied the forwarded record.
    /// Same flood bound; never throws.
    /// </summary>
    public async Task ReportSecurityEventAndWaitAsync(string operation, string outcome, string scope, string principal = "-")
    {
        string who = string.IsNullOrEmpty(principal) ? "-" : principal;
        if (!IsLeader)
        {
            if (Leader is { } leader && leader != _s.NodeId && TakeAuditToken())
                await ForwardAsync(leader, new AuditEventMessage(operation, outcome, scope, who), AuditWait);
            return;
        }

        if (_pendingAudits.Count >= MaxPendingAudits || !TakeAuditToken())
            return;
        try
        {
            using CancellationTokenSource deadline = _s.CreateDeadline(AuditWait);
            await ProposeAsync(new AppendAuditCommand(NewHeader(default, who), operation, outcome, scope), deadline.Token);
        }
        catch (Exception ex) when (ex is DfsException or OperationCanceledException)
        {
        }
    }

    /// <summary>
    /// Records an event another node forwarded (<see cref="AuditEventMessage"/>, control leader only), under this node's flood
    /// bound, and completes once it is applied (the forwarding node answers its own caller after that, read-your-writes as for
    /// local events). False when it was dropped or could not be committed; never throws.
    /// </summary>
    public async Task<bool> RecordForwardedSecurityEventAsync(AuditEventMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);
        if (!IsLeader || _pendingAudits.Count >= MaxPendingAudits || !TakeAuditToken())
            return false;
        try
        {
            using CancellationTokenSource deadline = _s.CreateDeadline();
            ApplyOutcome outcome = await ProposeAsync(new AppendAuditCommand(NewHeader(default, string.IsNullOrEmpty(message.Principal) ? "-" : message.Principal),
                message.Operation, message.Outcome, message.Scope), deadline.Token);
            return outcome.IsSuccess;
        }
        catch (Exception ex) when (ex is DfsException or OperationCanceledException)
        {
            return false;
        }
    }

    /// <summary>Sends forwarded security events to the control leader, one at a time (best effort: a failure drops the backlog).</summary>
    private async Task ForwardAuditsAsync()
    {
        _forwarding = true;
        try
        {
            while (_forwardedAudits.Count > 0 && !_s.IsStopping)
            {
                if (Leader is not { } leader || leader == _s.NodeId)
                {
                    // No other control leader to forward to (unknown, or this node took over meanwhile and audits locally).
                    _forwardedAudits.Clear();
                    break;
                }

                if (!await ForwardAsync(leader, _forwardedAudits.Dequeue(), null))
                    _forwardedAudits.Clear();
            }
        }
        finally
        {
            _forwarding = false;
        }
    }

    /// <summary>
    /// Longest wait of a caller for its audit record (<see cref="ReportSecurityEventAndWaitAsync"/>): a few election timeouts
    /// (at least a second), so an unavailable control group delays the audited reply only briefly.
    /// </summary>
    private TimeSpan AuditWait => TimeSpan.FromTicks(Math.Max(TimeSpan.FromSeconds(1).Ticks, _s.Timings.Tick.Ticks * _s.Timings.ElectionTicksMax * 4));

    /// <summary>Forwards one security event to the control leader (within <paramref name="timeout"/>, default the peer call timeout); false when it could not be delivered.</summary>
    private async Task<bool> ForwardAsync(NodeId leader, AuditEventMessage message, TimeSpan? timeout)
    {
        try
        {
            using CancellationTokenSource deadline = _s.CreateDeadline(timeout);
            await _s.Peers.CallAsync(leader, message, timeout, deadline.Token);
            return true;
        }
        catch (Exception ex) when (ex is DfsException or IOException or TimeoutException or UnauthorizedAccessException or OperationCanceledException
                                       or ObjectDisposedException)
        {
            _s.Debug("control", NodeServices.Inv($"Forwarding the audit record '{message.Operation}' to the control leader failed: {ex.Message}"));
            return false;
        }
    }

    /// <summary>Token bucket (20 events burst, 10 per second) so floods of failed handshakes cannot flood the log.</summary>
    private bool TakeAuditToken()
    {
        long now = _s.Time.GetTimestamp();
        if (_auditRefilled != 0)
            _auditTokens = Math.Min(AuditBurst, _auditTokens + (_s.Time.GetElapsedTime(_auditRefilled, now).TotalSeconds * AuditRefillPerSecond));
        _auditRefilled = now;
        if (_auditTokens < 1)
            return false;
        _auditTokens--;
        return true;
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
                    using CancellationTokenSource deadline = _s.CreateDeadline();
                    await ProposeAsync(command, deadline.Token);
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
