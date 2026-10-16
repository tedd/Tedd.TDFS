using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Data;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Raft;
using Tedd.Tdfs.Server.Runtime;

namespace Tedd.Tdfs.Server.Control;

/// <summary>
/// Control-leader side of the maintenance plane: keeps the latest <c>ReportUsage</c> of every node in memory (never
/// replicated; a new control leader rebuilds the picture from the next reports) and derives from it
/// <list type="bullet">
/// <item>node usage and per-pool stored bytes and replica health (<c>ListNodes</c>, <c>ListPools</c>, <c>PoolStatus</c>,
/// POOL-013);</item>
/// <item>the Draining → Drained transition: a draining node that votes in no group and holds no copy any volume keeps
/// (its volume leaders' fresh summaries list it as up with zero holdings) becomes Drained;</item>
/// <item>the safety checks of a non-forced <c>RemoveNode</c> (OPS-002): policy satisfiability without the node
/// (<c>InvalidPolicy</c>), a reachable majority in every group it votes in (<c>QuorumUnavailable</c>) and no object whose
/// acknowledged durability depends on it (<c>PlacementUnavailable</c>);</item>
/// <item>completion of default-policy migrations reported by volume leaders (REP-014/015, CHA-002);</item>
/// <item>scrub requests (<c>AdminOp.StartScrub</c>) delivered with the report replies.</item>
/// </list>
/// Runs on the node loop.
/// </summary>
internal sealed class MaintenanceControl
{
    private readonly NodeServices _s;
    private readonly Dictionary<NodeId, (ReportUsageMessage Report, long At)> _nodes = [];
    private readonly Dictionary<VolumeId, (VolumeUsageReport Report, NodeId From, long At)> _volumes = [];

    /// <summary>Sequence number of the latest accepted summary per volume (orders summaries for migration completion).</summary>
    private readonly Dictionary<VolumeId, ulong> _volumeSeq = [];
    private ulong _reportSeq;
    private readonly Dictionary<NodeId, ulong> _scrubRequests = [];
    private readonly HashSet<VolumeId> _completing = [];
    private readonly HashSet<NodeId> _draining = [];
    private ulong _scrubCounter;
    private ulong _term;

    public MaintenanceControl(NodeServices services)
    {
        ArgumentNullException.ThrowIfNull(services);
        _s = services;
    }

    /// <summary>How long a report counts as current.</summary>
    public TimeSpan Freshness => _s.Timings.InventoryInterval * 8;

    private ControlState State => _s.Control.State;

    /// <summary>Registers the <c>ReportUsage</c> handler (node peers only).</summary>
    public void Register(RpcDispatcher rpc) => rpc.Register<ReportUsageMessage>(HandleAsync, RpcAccess.Node);

    private ValueTask<IWireMessage> HandleAsync(RpcContext context, IWireMessage request)
    {
        var message = (ReportUsageMessage)request;
        if (context.PeerNode != message.Node)
            throw new UnauthorizedAccessException("A node can only report its own usage.");
        return new ValueTask<IWireMessage>(Accept(message));
    }

    /// <summary>Records a report (this node must lead the control group) and answers with the node's scrub request.</summary>
    public ReportUsageResultMessage Accept(ReportUsageMessage report)
    {
        ArgumentNullException.ThrowIfNull(report);
        if (!_s.Control.IsLeader)
            throw _s.Control.NotLeader();
        ResetIfNewTerm();
        if (State.FindMember(report.Node) is not { IsLive: true } member || member.Incarnation != report.Incarnation)
            throw new DfsException(DfsErrorCode.StaleIncarnation, $"Usage report of node {report.Node} incarnation {report.Incarnation} is not current.");
        long now = _s.Time.GetTimestamp();
        _nodes[report.Node] = (report, now);
        foreach (VolumeUsageReport volume in report.Volumes)
        {
            if (State.FindVolume(volume.Volume) is not { } record || volume.VolumeEpoch < record.Epoch)
                continue;

            // A deposed volume leader that has not noticed yet may still report: its summary (possibly missing copies its
            // successor made or deleted) never replaces a current one of a newer term, so neither a removal check nor a
            // drain or migration decision can rest on it.
            if (_volumes.TryGetValue(volume.Volume, out (VolumeUsageReport Report, NodeId From, long At) known)
                && known.Report.VolumeEpoch == volume.VolumeEpoch && known.Report.Term > volume.Term && _s.Time.GetElapsedTime(known.At, now) <= Freshness)
            {
                continue;
            }

            _volumes[volume.Volume] = (volume, report.Node, now);
            _volumeSeq[volume.Volume] = ++_reportSeq;
            if (volume.CompletedMigration is { } completed && record.MigrationTarget is { } target && completed == target)
                CompleteMigration(record, target);
        }

        return new ReportUsageResultMessage(_scrubRequests.GetValueOrDefault(report.Node));
    }

    /// <summary>Requests a full scrub on <paramref name="node"/> (or on every live member) with the next report replies.</summary>
    public void RequestScrub(NodeId? node)
    {
        if (!_s.Control.IsLeader)
            throw _s.Control.NotLeader();
        ResetIfNewTerm();
        ulong request = ++_scrubCounter;
        foreach (MemberRecord m in State.Members.Values)
        {
            if (m.IsLive && (node is null || m.Id == node.Value))
                _scrubRequests[m.Id] = request;
        }
    }

    /// <summary>Device bytes in use on <paramref name="node"/> as last reported (null when unknown here).</summary>
    public long? UsedBytesOf(NodeId node) => Fresh(_nodes, node) is { } report ? report.UsedBytes : null;

    /// <summary>Plaintext bytes of the copies stored through <paramref name="pool"/> (sum of the volume leaders' summaries).</summary>
    public long? PoolUsedBytes(string pool)
    {
        if (!_s.Control.IsLeader)
            return null;
        long total = 0;
        foreach (VolumeUsageReport report in FreshVolumes())
        {
            foreach (PoolUsageReport p in report.Pools)
            {
                if (string.Equals(p.Pool, pool, StringComparison.Ordinal))
                    total += p.StoredBytes;
            }
        }

        return total;
    }

    /// <summary>Replica health of the objects placed through <paramref name="pool"/> (sum over volumes).</summary>
    public ReplicaHealthData PoolHealth(string pool)
    {
        long objects = 0, under = 0, zones = 0, missing = 0, violations = 0, backlog = 0, gc = 0, staged = 0, pinned = 0;
        foreach (VolumeUsageReport report in FreshVolumes())
        {
            foreach (PoolUsageReport p in report.Pools)
            {
                if (!string.Equals(p.Pool, pool, StringComparison.Ordinal))
                    continue;
                ReplicaHealthData h = p.Health;
                objects += h.Objects;
                under += h.UnderReplicatedObjects;
                zones += h.ZoneDeficitObjects;
                missing += h.MissingObjects;
                violations += h.PlacementViolations;
                backlog += h.RepairBacklogBytes;
                gc += h.PendingGcObjects;
                staged += h.StagedBytes;
                pinned += h.PinnedBytes;
            }
        }

        return new ReplicaHealthData(objects, under, zones, missing, violations, backlog, gc, staged, pinned);
    }

    /// <summary>
    /// Non-members of <paramref name="pool"/> that still hold copies placed through it (they are being drained of the
    /// pool's objects), by name.
    /// </summary>
    public IReadOnlyList<string> StrayHolders(string pool)
    {
        var members = new HashSet<NodeId>(State.PoolMembers(pool).Select(m => m.Id));
        var stray = new SortedSet<string>(StringComparer.Ordinal);
        foreach ((VolumeUsageReport report, _, _) in _volumes.Values)
        {
            if (State.FindVolume(report.Volume) is not { } volume || !UsesPool(volume, pool))
                continue;
            foreach (NodeHoldingReport holding in report.Nodes)
            {
                if (holding.Objects > 0 && !members.Contains(holding.Node) && State.FindMember(holding.Node) is { IsLive: true } m)
                    stray.Add(m.Name);
            }
        }

        return [.. stray];
    }

    /// <summary>
    /// <see cref="CheckRemoval"/>, after waiting a bounded time (at most <see cref="Freshness"/>, and never more than ten
    /// seconds) for the summaries of the volumes involved when this control leader has none yet (a new leader, or volume
    /// leaders that only just took over): an unknown replica state is a reason to wait, then to refuse, never to allow.
    /// </summary>
    /// <exception cref="DfsException">InvalidPolicy, QuorumUnavailable or PlacementUnavailable.</exception>
    public async Task CheckRemovalAsync(MemberRecord member, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(member);
        TimeSpan budget = Freshness < TimeSpan.FromSeconds(10) ? Freshness : TimeSpan.FromSeconds(10);
        long started = _s.Time.GetTimestamp();
        while (_s.Control.IsLeader && UnknownVolume(member) is not null && _s.Time.GetElapsedTime(started) < budget)
        {
            // Timer-completed waits only (a delay completed by cancelling its token may resume off the node loop).
            ct.ThrowIfCancellationRequested();
            await Task.Delay(_s.Timings.InventoryInterval, _s.Time);
        }

        CheckRemoval(State.FindMember(member.Id) ?? member);
    }

    /// <summary>A volume the removal of <paramref name="member"/> concerns whose current replica summary is missing (null: none).</summary>
    private string? UnknownVolume(MemberRecord member)
    {
        ResetIfNewTerm();
        foreach (VolumeRecord volume in State.Volumes.Values)
        {
            if (volume.State == WireVolumeState.Restoring || FreshVolume(volume.Id) is not null)
                continue;
            if (PoolsOf(volume).Any(p => State.PoolMembers(p).Any(m => m.Id == member.Id)))
                return volume.Name;
        }

        return null;
    }

    /// <summary>
    /// Refuses a non-forced removal that would make a policy unsatisfiable, leave a group it votes in without a reachable
    /// majority, or lose acknowledged durability of any object (PROTOCOL.md §8.8, OPS-002).
    /// </summary>
    /// <exception cref="DfsException">InvalidPolicy, QuorumUnavailable or PlacementUnavailable.</exception>
    public void CheckRemoval(MemberRecord member)
    {
        ArgumentNullException.ThrowIfNull(member);
        ControlState state = State;
        ResetIfNewTerm();

        // 1. Every policy that the cluster can satisfy now (counting the node) must stay satisfiable without it.
        foreach (VolumeRecord volume in state.Volumes.Values)
        {
            foreach (DfsStoragePolicy policy in PoliciesOf(volume))
            {
                string pool = policy.Pool ?? volume.StoragePool;
                PlacementNode[] now = [.. state.PoolMembers(pool).Where(m => !m.PendingAdmission)
                    .Select(m => new PlacementNode(m.Id, m.Name, m.Labels, true, 1))];
                PlacementNode[] after = [.. now.Where(n => n.Id != member.Id)];
                if (Placement.IsSatisfiable(policy, now, out _) && !Placement.IsSatisfiable(policy, after, out string? reason))
                {
                    throw new DfsException(DfsErrorCode.InvalidPolicy,
                        $"Removing {member.Name} would leave volume '{volume.Name}' unable to satisfy its storage policy: {reason} Drain more capacity first or remove with force.");
                }
            }
        }

        // 2. Every group the node votes in keeps a reachable majority.
        if (_s.Control.Group is { } control && control.Conf.IsVoter(member.Id))
            RequireMajorityWithout(member, "the control group", [.. control.Conf.Voters]);
        foreach (VolumeRecord volume in state.Volumes.Values)
        {
            if (volume.Voters.Contains(member.Id))
                RequireMajorityWithout(member, $"volume '{volume.Name}'", volume.Voters);
        }

        // 3. No referenced object may lose acknowledged durability with the node gone.
        bool reachable = _s.Control.IsReachable(member.Id);
        foreach (VolumeRecord volume in state.Volumes.Values)
        {
            if (volume.State == WireVolumeState.Restoring)
                continue;
            VolumeUsageReport? report = FreshVolume(volume.Id);
            NodeHoldingReport? holding = report?.Nodes.FirstOrDefault(n => n.Node == member.Id);
            bool involved = holding is not null || PoolsOf(volume).Any(p => state.PoolMembers(p).Any(m => m.Id == member.Id));
            if (!involved)
                continue;
            if (report is null)
            {
                throw new DfsPlacementUnavailableException(
                    $"The replica state of volume '{volume.Name}' is not known yet; {member.Name} cannot be removed safely now (retry, or remove with force).", 1, 0, 1, 0);
            }

            if (holding is { NeededForAck: > 0 })
            {
                throw new DfsPlacementUnavailableException(
                    $"{holding.NeededForAck} object(s) of volume '{volume.Name}' would fall below their acknowledged copies or zones without {member.Name}; drain it first or remove with force.",
                    1, 0, 1, 0);
            }

            long missing = report.Pools.Sum(p => p.Health.MissingObjects);
            if (!reachable && missing > 0)
            {
                throw new DfsPlacementUnavailableException(
                    $"Volume '{volume.Name}' has {missing} object(s) without a reachable copy and {member.Name} is down: it may hold the only copy. Bring it back, or remove with force.",
                    1, 0, 1, 0);
            }
        }
    }

    /// <summary>One round of the Draining → Drained transition (control leader only).</summary>
    public async Task RunAsync()
    {
        if (!_s.Control.IsLeader || _s.Control.Group is not { } group || !State.IsBootstrapped)
            return;
        ResetIfNewTerm();
        foreach (MemberRecord member in State.Members.Values.Where(m => m.Status == WireNodeStatus.Draining).ToArray())
        {
            if (_draining.Contains(member.Id) || !IsDrained(member, group.Conf))
                continue;
            _draining.Add(member.Id);
            try
            {
                using CancellationTokenSource deadline = _s.CreateDeadline();
                ApplyOutcome outcome = await _s.Control.ProposeAsync(
                    new SetMemberStatusCommand(_s.Control.NewHeader(), member.Id, WireNodeStatus.Drained, false), deadline.Token);
                if (outcome.IsSuccess)
                    _s.Info("maintenance", NodeServices.Inv($"{member.Name} is drained: it votes nowhere and holds no data any volume keeps."));
            }
            finally
            {
                _draining.Remove(member.Id);
            }
        }
    }

    private bool IsDrained(MemberRecord member, RaftConfState conf)
    {
        if (conf.IsVoter(member.Id) || !_s.Control.IsReachable(member.Id))
            return false;
        foreach (VolumeRecord volume in State.Volumes.Values)
        {
            if (volume.Voters.Contains(member.Id))
                return false;
            if (volume.State == WireVolumeState.Restoring)
                continue;
            bool inPool = PoolsOf(volume).Any(p => State.PoolMembers(p).Any(m => m.Id == member.Id));
            VolumeUsageReport? report = FreshVolume(volume.Id);
            NodeHoldingReport? holding = report?.Nodes.FirstOrDefault(n => n.Node == member.Id);
            if (holding is null)
            {
                if (inPool)
                    return false; // the volume's leader has no current inventory of the node yet
                continue;
            }

            if (!holding.Up || holding.Objects > 0)
                return false;
        }

        return true;
    }

    private void RequireMajorityWithout(MemberRecord member, string group, IReadOnlyCollection<NodeId> voters)
    {
        NodeId[] remaining = [.. voters.Where(v => v != member.Id)];
        if (remaining.Length == 0)
            throw new DfsException(DfsErrorCode.QuorumUnavailable, $"{member.Name} is the last voter of {group} and cannot be removed.");
        int reachable = remaining.Count(_s.Control.IsReachable);
        if (reachable <= remaining.Length / 2)
        {
            throw new DfsException(DfsErrorCode.QuorumUnavailable,
                $"Removing {member.Name} would leave {group} with {reachable} reachable of {remaining.Length} voters (no majority). Restore the unreachable voters first, or remove a dead voter with force.");
        }
    }

    private void CompleteMigration(VolumeRecord record, DfsStoragePolicy target)
    {
        if (!_completing.Add(record.Id))
            return;
        _s.Jobs.Run("complete-policy-migration", async () =>
        {
            try
            {
                // The policy transition is a reconfiguration of the volume (fault site F21 on the control leader).
                await _s.HitAsync(FaultStage.F21, FaultSites.Reconfiguration, record.Name, DfsOperationKind.PolicyChange, detail: "complete-policy-migration");
                if (!await CompletionStillHoldsAsync(record.Id, target))
                    return;
                using CancellationTokenSource deadline = _s.CreateDeadline();
                ApplyOutcome outcome = await _s.Control.ProposeAsync(new CompletePolicyMigrationCommand(_s.Control.NewHeader(), record.Id, target), deadline.Token);
                if (outcome.IsSuccess)
                    _s.Info("maintenance", NodeServices.Inv($"Volume {record.Name}: the default-policy migration is complete; the new policy is effective."));
            }
            finally
            {
                _completing.Remove(record.Id);
            }
        });
    }

    /// <summary>
    /// Re-validates a migration completion at the transition point (FSD §10.2: never declared effective before the migration
    /// finished; CHA-002): the decision rests only on a summary the volume leader built after this point, never on one taken
    /// before a holder (or a whole zone) was lost meanwhile. Summaries of one node are sequential (the next one is built only
    /// after the reply to the previous one), so the second summary of the same sender accepted from here on was built after
    /// this point; it must still report the migration complete, and the target must still be pending in the control state.
    /// </summary>
    private async Task<bool> CompletionStillHoldsAsync(VolumeId volume, DfsStoragePolicy target)
    {
        ulong from = _volumeSeq.GetValueOrDefault(volume);
        long started = _s.Time.GetTimestamp();
        NodeId? sender = null;
        ulong first = 0;
        TimeSpan budget = Freshness * 2;
        while (_s.Control.IsLeader && _s.Time.GetElapsedTime(started) < budget)
        {
            ResetIfNewTerm();
            if (State.FindVolume(volume) is not { } current || current.MigrationTarget != target)
                return false;
            ulong seq = _volumeSeq.GetValueOrDefault(volume);
            if (seq > from && _volumes.TryGetValue(volume, out (VolumeUsageReport Report, NodeId From, long At) latest))
            {
                if (sender is null || sender != latest.From)
                {
                    sender = latest.From;
                    first = seq;
                }
                else if (seq > first)
                {
                    return latest.Report.CompletedMigration == target;
                }
            }

            // Timer-completed waits only (a delay completed by cancelling its token may resume off the node loop).
            await Task.Delay(_s.Timings.InventoryInterval / 4, _s.Time);
        }

        return false;
    }

    private static IEnumerable<DfsStoragePolicy> PoliciesOf(VolumeRecord volume)
    {
        yield return volume.DefaultPolicy;
        if (volume.MigrationTarget is { } target)
            yield return target;
        foreach (DfsStoragePolicy named in volume.NamedPolicies.Values)
            yield return named;
    }

    private static IEnumerable<string> PoolsOf(VolumeRecord volume)
        => PoliciesOf(volume).Select(p => p.Pool ?? volume.StoragePool).Distinct(StringComparer.Ordinal);

    private static bool UsesPool(VolumeRecord volume, string pool) => PoolsOf(volume).Contains(pool, StringComparer.Ordinal);

    private IEnumerable<VolumeUsageReport> FreshVolumes()
    {
        long now = _s.Time.GetTimestamp();
        foreach ((VolumeId id, (VolumeUsageReport report, _, long at)) in _volumes)
        {
            if (_s.Time.GetElapsedTime(at, now) <= Freshness && State.FindVolume(id) is not null)
                yield return report;
        }
    }

    private VolumeUsageReport? FreshVolume(VolumeId volume)
    {
        if (!_volumes.TryGetValue(volume, out (VolumeUsageReport Report, NodeId From, long At) entry))
            return null;
        return _s.Time.GetElapsedTime(entry.At) <= Freshness ? entry.Report : null;
    }

    private ReportUsageMessage? Fresh(Dictionary<NodeId, (ReportUsageMessage Report, long At)> reports, NodeId node)
    {
        if (!_s.Control.IsLeader || !reports.TryGetValue(node, out (ReportUsageMessage Report, long At) entry))
            return null;
        return _s.Time.GetElapsedTime(entry.At) <= Freshness ? entry.Report : null;
    }

    /// <summary>Forgets everything learned under an earlier term (reports are leader-local).</summary>
    private void ResetIfNewTerm()
    {
        ulong term = _s.Control.Group?.Term ?? 0;
        if (term == _term)
            return;
        _term = term;
        _nodes.Clear();
        _volumes.Clear();
        _volumeSeq.Clear();
        _scrubRequests.Clear();
        _scrubCounter = 0;
    }
}
