using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Runtime;

namespace Tedd.Tdfs.Server.Raft;

/// <summary>
/// Owns every Raft group of the node (ARCHITECTURE §4, INTERNALS §1.1): opens/closes groups, drives ticks from the node's
/// <see cref="TimeProvider"/> at <see cref="NodeTimings.Tick"/>, dispatches inbound Raft traffic by group, batches outbound
/// traffic per destination (<see cref="RaftTransport"/>), and exposes proposals, ReadIndex barriers and leadership.
/// </summary>
internal sealed class RaftGroupHost
{
    private readonly NodeServices _s;
    private readonly SortedDictionary<string, GroupRuntime> _groups = new(StringComparer.Ordinal);
    private bool _ticking;
    private bool _stopped;

    public RaftGroupHost(NodeServices services)
    {
        ArgumentNullException.ThrowIfNull(services);
        _s = services;
        Transport = new RaftTransport(services, this);
    }

    /// <summary>Outbound Raft traffic.</summary>
    public RaftTransport Transport { get; }

    /// <summary>Raised after entries were applied: (wire group name, applied index). Runs inside Ready processing.</summary>
    public event Action<string, ulong>? Applied;

    /// <summary>Raised when a group's role, leader or term changed (or the group was reset/stopped).</summary>
    public event Action<GroupRuntime>? LeadershipChanged;

    /// <summary>Raised when a configuration-change entry was applied (or a snapshot installed a membership).</summary>
    public event Action<GroupRuntime, RaftConfState>? ConfApplied;

    /// <summary>Hosted groups in name order.</summary>
    public IReadOnlyList<GroupRuntime> Groups => [.. _groups.Values];

    /// <summary>The group, or null when not hosted.</summary>
    public GroupRuntime? TryGet(string group) => _groups.GetValueOrDefault(group);

    /// <summary>Opens a group (recovering or bootstrapping it) and starts processing it.</summary>
    public async Task<GroupRuntime> OpenAsync(GroupOpenSpec spec, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(spec);
        if (_groups.ContainsKey(spec.Group))
            throw new InvalidOperationException($"Group '{spec.Group}' is already open.");
        GroupRuntime group = await GroupRuntime.OpenAsync(_s, this, spec, ct);
        if (_stopped || _groups.ContainsKey(spec.Group))
        {
            await group.CloseAsync(deleteData: false);
            throw new InvalidOperationException($"Group '{spec.Group}' could not be opened (host stopping or opened concurrently).");
        }

        _groups[spec.Group] = group;
        _s.Info("raft", NodeServices.Inv($"Opened group {spec.Group} ({spec.DisplayName}): applied {group.AppliedIndex}, last {group.LastIndex}, conf {group.Conf}."));
        group.Kick();
        return group;
    }

    /// <summary>Closes a group; optionally deletes its local data (this node no longer hosts it).</summary>
    public async Task CloseAsync(string group, bool deleteData)
    {
        if (!_groups.Remove(group, out GroupRuntime? runtime))
            return;
        Transport.ForgetGroup(group);
        await runtime.CloseAsync(deleteData);
        _s.Info("raft", NodeServices.Inv($"Closed group {group}{(deleteData ? " and deleted its data" : string.Empty)}."));
    }

    /// <summary>Closes every group (graceful stop).</summary>
    public async Task CloseAllAsync()
    {
        _stopped = true;
        foreach (string name in _groups.Keys.ToArray())
            await CloseAsync(name, deleteData: false);
    }

    /// <summary>Starts the tick loop.</summary>
    public void StartTicking()
    {
        if (_ticking)
            return;
        _ticking = true;
        _ = TickLoopAsync();
    }

    /// <summary>Stops ticking (the loop exits at its next tick).</summary>
    public void Stop() => _stopped = true;

    /// <summary>Proposes a command to a hosted group (see <see cref="GroupRuntime.ProposeAsync(ReadOnlyMemory{byte}, ProposalContext, CancellationToken)"/>).</summary>
    public Task<ApplyOutcome> ProposeAsync(string group, ReadOnlyMemory<byte> command, ProposalContext context, CancellationToken ct = default)
        => Require(group).ProposeAsync(command, context, ct);

    /// <summary>Linearizable read barrier on a hosted group (see <see cref="GroupRuntime.ReadIndexAsync"/>).</summary>
    public Task<ulong> ReadIndexAsync(string group, CancellationToken ct = default) => Require(group).ReadIndexAsync(ct);

    /// <summary>True when this node leads the hosted group.</summary>
    public bool IsLeader(string group) => TryGet(group)?.IsLeader == true;

    /// <summary>Known leader of the hosted group.</summary>
    public NodeId? LeaderOf(string group) => TryGet(group)?.Leader;

    /// <summary>Endpoint of the hosted group's known leader, or null.</summary>
    public string? LeaderHint(string group) => LeaderOf(group) is { } id ? ResolveEndpoint(id) : null;

    /// <summary>Starts a leadership transfer (production path: TimeoutNow once the target is caught up).</summary>
    public void TransferLeadership(string group, NodeId target) => Require(group).TransferLeadership(target);

    /// <summary>
    /// Transfers leadership of a group this node leads to <paramref name="target"/> and waits until this node observes the
    /// target as leader. False on timeout or when this node is not the leader.
    /// </summary>
    public async Task<bool> TransferLeadershipAsync(string group, NodeId target, TimeSpan timeout)
    {
        GroupRuntime g = Require(group);
        if (g.Leader == target)
            return true;
        if (!g.IsLeader)
            return false;
        long start = _s.Time.GetTimestamp();
        g.TransferLeadership(target);
        while (_s.Time.GetElapsedTime(start) < timeout)
        {
            await Task.Delay(_s.Timings.Tick, _s.Time);
            if (g.IsClosed)
                return false;
            if (g.Leader == target && g.Role != RaftRole.Leader)
                return true;
            if (g.IsLeader && g.Node.LeadTransferee is null && _s.Time.GetElapsedTime(start) > _s.Timings.Tick * _s.Timings.ElectionTicksMax)
                g.TransferLeadership(target);
        }

        return false;
    }

    /// <summary>Endpoint of a member from the control state (null when unknown).</summary>
    public string? ResolveEndpoint(NodeId node) => _s.Control?.ResolveEndpoint(node);

    /// <summary>Handles an inbound <see cref="RaftBatchMessage"/> from the authenticated node <paramref name="sender"/>.</summary>
    public RaftBatchResultMessage HandleBatch(RaftBatchMessage batch, NodeId sender)
    {
        int accepted = 0;
        List<string>? unknown = null;
        foreach (RaftEnvelope envelope in batch.Messages)
        {
            RaftWireMessage wire = envelope.Message;
            if (wire.From != sender || wire.To != _s.NodeId)
                continue;
            if (!_groups.TryGetValue(envelope.Group, out GroupRuntime? group))
            {
                unknown ??= [];
                if (!unknown.Contains(envelope.Group, StringComparer.Ordinal))
                    unknown.Add(envelope.Group);
                continue;
            }

            RaftMessage? message = RaftWireMapper.FromWire(wire);
            if (message is null)
                continue;
            group.Step(message);
            accepted++;
        }

        unknown?.Sort(StringComparer.Ordinal);
        return new RaftBatchResultMessage(_s.NodeId, _s.Incarnation, accepted, unknown ?? []);
    }

    /// <summary>Handles an inbound <see cref="RaftSnapshotChunkMessage"/> (follower side).</summary>
    public async Task<RaftSnapshotChunkResultMessage> HandleSnapshotChunkAsync(RaftSnapshotChunkMessage chunk, NodeId sender, CancellationToken ct)
    {
        if (chunk.From != sender || chunk.To != _s.NodeId)
            throw new UnauthorizedAccessException("The snapshot chunk's sender or recipient does not match the connection.");
        if (!_groups.TryGetValue(chunk.Group, out GroupRuntime? group))
            throw new DfsUnavailableException(DfsErrorCode.NodeUnavailable, $"Group '{chunk.Group}' is not hosted on {_s.NodeName}.");
        var message = new InstallSnapshot(chunk.From, chunk.To, chunk.Term, RaftWireMapper.FromWire(chunk.Meta), chunk.Offset, chunk.Data, chunk.Done);
        InstallSnapshotResponse? response = await group.StepSnapshotChunkAsync(message, ct);
        return response is not null
            ? new RaftSnapshotChunkResultMessage(response.Term, response.SnapshotIndex, response.NextOffset, response.Reject)
            : new RaftSnapshotChunkResultMessage(group.Term, chunk.Meta.Index, chunk.Offset + chunk.Data.Length, Reject: false);
    }

    internal void OnApplied(GroupRuntime group, ulong index) => Raise(() => Applied?.Invoke(group.Name, index), "Applied");

    internal void OnLeadershipChanged(GroupRuntime group) => Raise(() => LeadershipChanged?.Invoke(group), "LeadershipChanged");

    internal void OnConfApplied(GroupRuntime group, RaftConfState conf) => Raise(() => ConfApplied?.Invoke(group, conf), "ConfApplied");

    private void Raise(Action action, string name)
    {
        try
        {
            action();
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            _s.Error("raft", NodeServices.Inv($"A {name} handler threw {ex.GetType().Name}: {ex.Message}"));
        }
    }

    private GroupRuntime Require(string group)
        => TryGet(group) ?? throw new DfsUnavailableException(DfsErrorCode.NodeUnavailable, $"Group '{group}' is not hosted on {_s.NodeName}.");

    private async Task TickLoopAsync()
    {
        TimeSpan tick = _s.Timings.Tick;
        while (!_stopped)
        {
            await Task.Delay(tick, _s.Time);
            if (_stopped)
                return;
            foreach (GroupRuntime group in _groups.Values.ToArray())
                group.Tick();
        }
    }
}
