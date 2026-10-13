using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing.Simulation;

namespace Tedd.Tdfs.Testing;

// Leaders, stability, replication, repair, generic waits and the replicated-state digest. Every wait polls on the harness
// clock (virtual in deterministic mode) and reads the nodes' applied state through their in-process inspectors.
public sealed partial class TestCluster
{
    private readonly Dictionary<VolumeId, string> _volumeNames = [];

    /// <summary>Default bound of the leader/replication waits (harness time).</summary>
    internal TimeSpan DefaultWaitTimeout =>
        Max(TimeSpan.FromSeconds(60), Options.Timings.Tick * Options.Timings.ElectionTicksMax * 100);

    /// <summary>Waits for and returns the leader of a volume group.</summary>
    /// <exception cref="TimeoutException">No leader within the default wait bound (message names the volume and what was observed).</exception>
    public async Task<TestNode> GetLeaderAsync(string volume, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(volume);
        return await WaitForGroupLeaderAsync(volume, DefaultWaitTimeout, ct);
    }

    /// <summary>Waits for and returns the control-group leader.</summary>
    public Task<TestNode> GetControlLeaderAsync(CancellationToken ct = default) =>
        WaitForGroupLeaderAsync(ProtocolInfo.ControlGroup, DefaultWaitTimeout, ct);

    /// <summary>Waits until control and all volume leaders are elected and all reachable nodes caught up.</summary>
    /// <remarks>
    /// Stable means: every running node serves and has registered its incarnation; the control group has a leader whose term
    /// every reachable running node follows, every reachable running member applied the leader's commit index, every live
    /// member is a voter or learner and the voter target is met; and every volume that is not being deleted has a leader
    /// whose desired running voters are configured voters that follow it and applied its commit index. Paused, crashed and
    /// stopped nodes, and nodes the network separates from the leader, are not waited for.
    /// </remarks>
    /// <exception cref="TimeoutException">Not stable within <paramref name="timeout"/> (default 60 s); the message says why.</exception>
    public Task WaitForStableAsync(TimeSpan? timeout = null, CancellationToken ct = default) =>
        WaitForStableCoreAsync(timeout ?? DefaultWaitTimeout, includeVolumes: true, ct);

    /// <summary>Waits until all reachable voters of a volume applied the leader commit index (as sampled when the wait starts).</summary>
    public async Task WaitForReplicationAsync(string volume, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(volume);
        TestNode leader = await GetLeaderAsync(volume, ct);
        GroupInspection? sample = await TryInspectGroupAsync(leader, volume);
        ulong commit = sample?.CommitIndex ?? 0;
        await WaitUntilCoreAsync(async () =>
        {
            IReadOnlyList<NodeView> views = await SnapshotAsync();
            NodeView? leaderView = views.FirstOrDefault(v => ReferenceEquals(v.Node, leader));
            if (leaderView?.Group(volume) is not { } group)
                return false;
            foreach (string voter in group.Voters)
            {
                if (voter == leader.Name || FindNode(voter) is not { State: TestNodeState.Running } node || !Reachable(leader.Name, voter))
                    continue;
                if (views.FirstOrDefault(v => ReferenceEquals(v.Node, node))?.Group(volume) is not { } replica || replica.AppliedIndex < commit)
                    return false;
            }

            return group.AppliedIndex >= commit;
        }, DefaultWaitTimeout, $"every reachable voter of volume {volume} applied index {commit.ToString(CultureInfo.InvariantCulture)}", ct);
    }

    /// <summary>Waits until a volume has no under-replicated objects (and no zone deficits), per <see cref="ClusterInspector.GetReplicaHealthAsync"/>.</summary>
    public async Task WaitForRepairAsync(string volume, TimeSpan? timeout = null, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(volume);
        DfsReplicaHealth? last = null;
        try
        {
            await WaitUntilCoreAsync(async () =>
            {
                last = await Inspect.GetReplicaHealthAsync(volume);
                return last.UnderReplicatedObjects == 0 && last.ZoneDeficitObjects == 0;
            }, timeout ?? TimeSpan.FromSeconds(120), $"volume {volume} has no under-replicated objects", ct);
        }
        catch (TimeoutException ex)
        {
            // The last observation is only known once the wait gave up.
            throw new TimeoutException($"{ex.Message} (last health: {last})", ex);
        }
    }

    /// <summary>Polls a condition; throws <see cref="TimeoutException"/> naming <paramref name="because"/>.</summary>
    public Task WaitUntilAsync(Func<Task<bool>> condition, TimeSpan timeout, string because, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(condition);
        ArgumentOutOfRangeException.ThrowIfLessThan(timeout, TimeSpan.Zero);
        return WaitUntilCoreAsync(condition, timeout, because ?? "the condition holds", ct);
    }

    /// <summary>Delays on the harness clock (virtual in deterministic mode).</summary>
    public Task DelayAsync(TimeSpan delay, CancellationToken ct = default) => Runtime.DelayAsync(delay, ct);

    /// <summary>Cluster-wide replicated-state digest.</summary>
    /// <remarks>
    /// SHA-256 (hex) over, for the control group and then every hosted volume group in name order, the group's digest line
    /// (<c>name@applied:sha256(checkpoint)</c>) taken from its leader, or from the most advanced replica when it has none.
    /// Identical for two runs exactly when their replicated states (and applied positions) are identical.
    /// </remarks>
    public async Task<string> ComputeDigestAsync(CancellationToken ct = default)
    {
        IReadOnlyList<NodeView> views = await SnapshotAsync();
        var groups = new SortedSet<string>(StringComparer.Ordinal);
        foreach (NodeView view in views)
        {
            foreach (GroupInspection g in view.Groups)
                groups.Add(g.Group);
        }

        var lines = new StringBuilder();
        foreach (string group in groups.OrderBy(g => g == ProtocolInfo.ControlGroup ? 0 : 1).ThenBy(g => g, StringComparer.Ordinal))
        {
            ct.ThrowIfCancellationRequested();
            NodeView? source = views.Where(v => v.Group(group) is not null)
                .OrderByDescending(v => v.Group(group)!.Role == RaftRole.Leader)
                .ThenByDescending(v => v.Group(group)!.Term)
                .ThenByDescending(v => v.Group(group)!.AppliedIndex)
                .ThenBy(v => v.Node.Name, StringComparer.Ordinal)
                .FirstOrDefault();
            if (source is null)
                continue;
            string? line;
            try
            {
                line = await source.Node.Inspect.ComputeGroupDigestAsync(group);
            }
            catch (Exception ex) when (IsNodeGone(ex))
            {
                line = null;
            }

            lines.Append(line ?? group + "@?;");
        }

        return Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(lines.ToString())));
    }

    /// <summary>Transfers leadership of a Raft group to a chosen node through the production transfer (see <c>Additions/con-net</c>).</summary>
    private async Task<TestNode> TransferLeadershipCoreAsync(string group, string targetNode, TimeSpan? timeout, bool waitUntilServing, CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(group);
        ArgumentException.ThrowIfNullOrWhiteSpace(targetNode);
        TestNode target = FindNode(targetNode) ?? throw new ArgumentException($"The cluster has no live node named '{targetNode}'.", nameof(targetNode));
        bool isControl = string.Equals(group, ProtocolInfo.ControlGroup, StringComparison.Ordinal);
        if (!isControl && !Options.Volumes.Any(v => v.Name == group) && !(await KnownVolumeNamesAsync()).Contains(group))
            throw new ArgumentException($"'{group}' is neither \"control\" nor a volume of this cluster.", nameof(group));
        if (target.State != TestNodeState.Running)
            throw new InvalidOperationException($"Node {targetNode} is {target.State}; only a running voter can take over leadership.");

        TimeSpan bound = timeout ?? Max(TimeSpan.FromSeconds(10), Options.Timings.Tick * Options.Timings.ElectionTicksMax * 10);
        long start = Time.GetTimestamp();
        TestNode leader = await WaitForGroupLeaderAsync(group, bound, ct);
        GroupInspection before = await TryInspectGroupAsync(leader, group)
            ?? throw new TimeoutException($"The leader {leader.Name} of group {group} stopped answering.");
        if (!before.Voters.Contains(targetNode, StringComparer.Ordinal))
            throw new InvalidOperationException($"Node {targetNode} is not a voter of group {group} (voters: {string.Join(", ", before.Voters)}).");

        Record($"transfer leadership of {group} from {leader.Name} (term {before.Term.ToString(CultureInfo.InvariantCulture)}) to {targetNode}");
        ulong termBefore = before.Term;
        bool alreadyLeader = ReferenceEquals(leader, target);
        ulong electedLastIndex = 0;
        long lastRequest = long.MinValue;
        TimeSpan retryEvery = Options.Timings.Tick * Options.Timings.ElectionTicksMax;
        string observed = $"leader {leader.Name}, term {termBefore}";
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            if (Time.GetElapsedTime(start) > bound)
            {
                throw new TimeoutException(string.Create(CultureInfo.InvariantCulture,
                    $"Leadership of group {group} did not move to {targetNode} within {bound} (observed {observed})."));
            }

            if (target.State != TestNodeState.Running)
                throw new InvalidOperationException($"Node {targetNode} stopped running during the leadership transfer of {group}.");
            GroupInspection? mine = await TryInspectGroupAsync(target, group);
            if (mine is { Role: RaftRole.Leader } && (alreadyLeader || mine.Term > termBefore))
            {
                if (electedLastIndex == 0)
                    electedLastIndex = mine.LastIndex;
                if (!waitUntilServing)
                    break;
                if (mine.CommitIndex >= electedLastIndex && mine.AppliedIndex >= mine.CommitIndex && await FollowersAgreeAsync(target, group, mine))
                    break;
            }
            else
            {
                observed = mine is null ? $"{targetNode} does not host {group}" : $"{targetNode} is {mine.Role} in term {mine.Term}, leader {mine.Leader ?? "unknown"}";
                if (Time.GetElapsedTime(start).Ticks - lastRequest >= retryEvery.Ticks || lastRequest == long.MinValue)
                {
                    lastRequest = Time.GetElapsedTime(start).Ticks;
                    TestNode? current = await TryFindGroupLeaderAsync(group);
                    if (current is not null && !ReferenceEquals(current, target))
                    {
                        try
                        {
                            await current.Inspect.TransferLeadershipAsync(group, target.Id);
                        }
                        catch (Exception ex) when (IsNodeGone(ex))
                        {
                            // The leader went away; a later round asks the next one.
                        }
                    }
                }
            }

            await Runtime.DelayAsync(Options.Timings.Tick * 2);
        }

        Record($"leadership of {group} is now with {targetNode}");
        return target;
    }

    // -------------------------------------------------------------------------------------------- harness internals

    /// <summary>Waits until <paramref name="node"/>'s current incarnation is registered with the control group.</summary>
    internal async Task WaitForRegistrationAsync(TestNode node, TimeSpan timeout)
    {
        if (!await TryWaitForRegistrationAsync(node, timeout))
            throw new TimeoutException($"Node {node.Name} did not register incarnation {node.Incarnation} with the control group within {timeout}.");
    }

    /// <summary>As <see cref="WaitForRegistrationAsync"/>; false on timeout (and true when the node is no longer running).</summary>
    internal async Task<bool> TryWaitForRegistrationAsync(TestNode node, TimeSpan timeout)
    {
        if (Options.OutOfProcess)
            return true;
        try
        {
            await WaitUntilCoreAsync(async () =>
            {
                if (node.State != TestNodeState.Running)
                    return true;
                try
                {
                    // A node that stopped serving by itself (its release cannot interpret an active feature) never registers.
                    HarnessNodeStatus status = await node.Inspect.GetNodeStatusAsync();
                    return status.IsSelfRegistered || status.IsStopping;
                }
                catch (Exception ex) when (IsNodeGone(ex))
                {
                    return node.State != TestNodeState.Running;
                }
            }, timeout, $"node {node.Name} registered its incarnation", CancellationToken.None);
            return true;
        }
        catch (TimeoutException)
        {
            return false;
        }
    }

    /// <summary>The control leader's endpoint as far as the inspectors tell, else a running node (round robin by attempt).</summary>
    internal async Task<string?> FindControlLeaderEndpointAsync(int attempt)
    {
        IReadOnlyList<TestNode> running = RunningNodes();
        if (running.Count == 0)
            return null;
        if (!Options.OutOfProcess && await TryFindGroupLeaderAsync(ProtocolInfo.ControlGroup) is { } leader)
            return leader.Endpoint;
        return running[attempt % running.Count].Endpoint;
    }

    /// <summary>
    /// Refreshes the volume id → name map the interposition layer resolves <c>GetObject</c>/<c>BackupInventory</c> volumes
    /// with (volumes created after the last stability check are otherwise unknown to it).
    /// </summary>
    internal async Task RefreshVolumeNamesAsync()
    {
        if (!Options.OutOfProcess)
            await SnapshotAsync();
    }

    /// <summary>The name of a volume id as last seen in a control state snapshot, or null.</summary>
    internal string? VolumeNameOf(VolumeId id)
    {
        lock (_volumeNames)
            return _volumeNames.TryGetValue(id, out string? name) ? name : null;
    }

    /// <summary>Polls <paramref name="condition"/> on the harness clock until it holds.</summary>
    internal async Task WaitUntilCoreAsync(Func<Task<bool>> condition, TimeSpan timeout, string because, CancellationToken ct)
    {
        long start = Time.GetTimestamp();
        TimeSpan poll = PollInterval;
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            if (await condition())
                return;
            if (Time.GetElapsedTime(start) >= timeout)
                throw new TimeoutException($"Timed out after {timeout} waiting until {because}.");
            await Runtime.DelayAsync(poll);
        }
    }

    /// <summary>Polling interval of harness waits: two node ticks (at least 1 ms).</summary>
    private TimeSpan PollInterval => Max(TimeSpan.FromMilliseconds(1), Options.Timings.Tick * 2);

    /// <summary>Waits until the cluster is stable (see <see cref="WaitForStableAsync"/>), optionally ignoring volumes.</summary>
    internal async Task WaitForStableCoreAsync(TimeSpan timeout, bool includeVolumes, CancellationToken ct)
    {
        string? reason = "not checked yet";
        try
        {
            await WaitUntilCoreAsync(async () =>
            {
                reason = Options.OutOfProcess ? await EvaluateStabilityRemoteAsync(includeVolumes) : EvaluateStability(await SnapshotAsync(), includeVolumes);
                return reason is null;
            }, timeout, "the cluster is stable", ct);
        }
        catch (TimeoutException ex)
        {
            string hint = includeVolumes && reason is not null && reason.Contains("volume", StringComparison.Ordinal)
                && StartOptions.VolumeStateMachines is null && Server.Volumes.VolumeHost.ResolveDefaultFactory() is null
                ? " This build has no volume state machine (VolumeHost.CreateDefaultStateMachineFactory is not implemented), so volume groups are never hosted."
                : string.Empty;
            throw new TimeoutException($"The cluster did not become stable within {timeout}: {reason}.{hint}", ex);
        }
    }

    /// <summary>Null when stable; otherwise why not.</summary>
    private string? EvaluateStability(IReadOnlyList<NodeView> views, bool includeVolumes)
    {
        IReadOnlyList<TestNode> running = RunningNodes();
        if (running.Count == 0)
            return "no node is running";
        foreach (TestNode node in running)
        {
            NodeView? view = views.FirstOrDefault(v => ReferenceEquals(v.Node, node));
            if (view is null)
                return $"node {node.Name} does not answer";
            if (!view.Status.IsServing)
                return $"node {node.Name} is not serving yet";
        }

        NodeView? leader = LeaderView(views, ProtocolInfo.ControlGroup);
        if (leader is null)
            return "the control group has no leader";
        GroupInspection control = leader.Group(ProtocolInfo.ControlGroup)!;
        if (control.AppliedIndex < control.CommitIndex)
            return $"control leader {leader.Node.Name} applied {control.AppliedIndex} < commit {control.CommitIndex}";
        HarnessControlSummary state = leader.Control;
        RememberVolumeNames(state);

        foreach (NodeView view in views)
        {
            if (ReferenceEquals(view, leader) || !Reachable(leader.Node.Name, view.Node.Name))
                continue;
            if (!view.Status.IsSelfRegistered)
                return $"node {view.Node.Name} has not registered its incarnation";
            if (state.FindMember(view.Node.Name) is not { IsLive: true })
                return $"node {view.Node.Name} is not a live member of the cluster";
            GroupInspection? replica = view.Group(ProtocolInfo.ControlGroup);
            if (replica is null)
                return $"node {view.Node.Name} does not host the control group";
            if (replica.Term != control.Term || replica.Leader != leader.Node.Name)
                return $"node {view.Node.Name} follows {replica.Leader ?? "nobody"} in control term {replica.Term} (leader {leader.Node.Name}, term {control.Term})";
            if (replica.AppliedIndex < control.CommitIndex)
                return $"node {view.Node.Name} applied control index {replica.AppliedIndex} < commit {control.CommitIndex}";
        }

        if (!leader.Status.IsSelfRegistered)
            return $"control leader {leader.Node.Name} has not registered its incarnation";

        int eligible = state.Members.Count(m => m.IsLive && m.Status == "Active");
        int activeVoters = control.Voters.Count(v => state.FindMember(v) is { IsLive: true, Status: "Active" });
        if (activeVoters < Math.Min(state.ControlVoterTarget, eligible))
            return $"the control group has {activeVoters} active voters, target {Math.Min(state.ControlVoterTarget, eligible)}";
        foreach (HarnessMemberSummary member in state.Members)
        {
            if (member.IsLive && FindNode(member.Name) is { State: TestNodeState.Running }
                && !control.Voters.Contains(member.Name, StringComparer.Ordinal) && !control.Learners.Contains(member.Name, StringComparer.Ordinal))
                return $"live member {member.Name} is not yet in the control group configuration";
        }

        if (!includeVolumes)
            return null;
        foreach (HarnessVolumeSummary volume in state.Volumes)
        {
            if (volume.State == "Deleting")
                continue;
            NodeView? volumeLeader = LeaderView(views, volume.Name);
            if (volumeLeader is null)
                return $"volume {volume.Name} has no leader";
            GroupInspection group = volumeLeader.Group(volume.Name)!;
            if (group.AppliedIndex < group.CommitIndex)
                return $"volume {volume.Name} leader {volumeLeader.Node.Name} applied {group.AppliedIndex} < commit {group.CommitIndex}";
            foreach (string voter in volume.Voters)
            {
                if (FindNode(voter) is not { State: TestNodeState.Running } || !Reachable(volumeLeader.Node.Name, voter))
                    continue;
                if (!group.Voters.Contains(voter, StringComparer.Ordinal))
                    return $"volume {volume.Name}: desired voter {voter} is not a configured voter yet";
                if (voter == volumeLeader.Node.Name)
                    continue;
                GroupInspection? replica = views.FirstOrDefault(v => v.Node.Name == voter)?.Group(volume.Name);
                if (replica is null)
                    return $"volume {volume.Name}: voter {voter} does not host the group";
                if (replica.Term != group.Term || replica.Leader != volumeLeader.Node.Name)
                    return $"volume {volume.Name}: voter {voter} follows {replica.Leader ?? "nobody"} in term {replica.Term}";
                if (replica.AppliedIndex < group.CommitIndex)
                    return $"volume {volume.Name}: voter {voter} applied {replica.AppliedIndex} < commit {group.CommitIndex}";
            }
        }

        return null;
    }

    /// <summary>Stability as seen through the admin API (out-of-process clusters: no inspectors).</summary>
    private async Task<string?> EvaluateStabilityRemoteAsync(bool includeVolumes)
    {
        AdminClusterStatusBody status;
        try
        {
            status = await HarnessAdmin.CallAsync<AdminEmptyBody, AdminClusterStatusBody>(AdminOp.ClusterStatus, new AdminEmptyBody(),
                timeout: TimeSpan.FromSeconds(5));
        }
        catch (Exception ex) when (ex is TimeoutException or DfsException)
        {
            return "the control group does not answer: " + ex.Message;
        }

        if (status.Control.Leader is null || !status.Control.HasQuorum)
            return "the control group has no leader with a quorum";
        foreach (TestNode node in RunningNodes())
        {
            if (!status.Nodes.Any(n => n.Name == node.Name))
                return $"node {node.Name} is not a member yet";
        }

        if (!includeVolumes)
            return null;
        foreach (string volume in status.Volumes)
        {
            try
            {
                AdminVolumeStatusBody v = await HarnessAdmin.CallAsync<AdminVolumeNameBody, AdminVolumeStatusBody>(AdminOp.VolumeStatus,
                    new AdminVolumeNameBody(volume), timeout: TimeSpan.FromSeconds(5));
                if (v.Group.Leader is null || !v.Group.HasQuorum)
                    return $"volume {volume} has no leader with a quorum";
            }
            catch (Exception ex) when (ex is TimeoutException or DfsException or NotSupportedException)
            {
                return $"volume {volume} status unavailable: {ex.Message}";
            }
        }

        return null;
    }

    /// <summary>Waits until a running node leads <paramref name="group"/> in the highest term any running node knows.</summary>
    private async Task<TestNode> WaitForGroupLeaderAsync(string group, TimeSpan timeout, CancellationToken ct)
    {
        TestNode? found = null;
        await WaitUntilCoreAsync(async () => (found = await TryFindGroupLeaderAsync(group)) is not null, timeout, $"group {group} has a leader", ct);
        return found!;
    }

    /// <summary>The running node that leads <paramref name="group"/> in the highest observed term, or null.</summary>
    private async Task<TestNode?> TryFindGroupLeaderAsync(string group)
    {
        if (Options.OutOfProcess)
            return await TryFindGroupLeaderRemoteAsync(group);
        IReadOnlyList<NodeView> views = await SnapshotAsync(groupsOnly: true);
        return LeaderView(views, group)?.Node;
    }

    private async Task<TestNode?> TryFindGroupLeaderRemoteAsync(string group)
    {
        try
        {
            TimeSpan quick = TimeSpan.FromSeconds(5);
            GroupStatusData data = string.Equals(group, ProtocolInfo.ControlGroup, StringComparison.Ordinal)
                ? (await HarnessAdmin.CallAsync<AdminEmptyBody, AdminClusterStatusBody>(AdminOp.ClusterStatus, new AdminEmptyBody(), timeout: quick)).Control
                : (await HarnessAdmin.CallAsync<AdminVolumeNameBody, AdminVolumeStatusBody>(AdminOp.VolumeStatus, new AdminVolumeNameBody(group),
                    timeout: quick)).Group;
            return data.Leader is { } name ? FindNode(name) : null;
        }
        catch (Exception ex) when (ex is TimeoutException or DfsException or NotSupportedException)
        {
            return null;
        }
    }

    /// <summary>The view of the running node leading <paramref name="group"/> in the highest term seen by any running node.</summary>
    private static NodeView? LeaderView(IReadOnlyList<NodeView> views, string group)
    {
        ulong maxTerm = 0;
        foreach (NodeView view in views)
        {
            if (view.Group(group) is { } g && g.Term > maxTerm)
                maxTerm = g.Term;
        }

        NodeView? leader = null;
        foreach (NodeView view in views)
        {
            if (view.Group(group) is { Role: RaftRole.Leader } g && g.Term == maxTerm)
            {
                if (leader is not null)
                    return null; // two leaders in one term never is a stable leadership (split brain is reported separately)
                leader = view;
            }
        }

        return leader;
    }

    /// <summary>True when every reachable voter of <paramref name="group"/> follows <paramref name="leader"/> in its current term.</summary>
    private async Task<bool> FollowersAgreeAsync(TestNode leader, string group, GroupInspection leaderGroup)
    {
        IReadOnlyList<NodeView> views = await SnapshotAsync(groupsOnly: true);
        foreach (string voter in leaderGroup.Voters)
        {
            if (voter == leader.Name || FindNode(voter) is not { State: TestNodeState.Running } || !Reachable(leader.Name, voter))
                continue;
            GroupInspection? replica = views.FirstOrDefault(v => v.Node.Name == voter)?.Group(group);
            if (replica is null || replica.Leader != leader.Name || replica.Term != leaderGroup.Term)
                return false;
        }

        return true;
    }

    /// <summary>True when messages flow both ways between two network identities.</summary>
    private bool Reachable(string a, string b) => Fabric.Policy.IsLinkUp(a, b) && Fabric.Policy.IsLinkUp(b, a);

    private async Task<IReadOnlySet<string>> KnownVolumeNamesAsync()
    {
        IReadOnlyList<NodeView> views = await SnapshotAsync();
        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (NodeView view in views)
        {
            foreach (HarnessVolumeSummary v in view.Control.Volumes)
                names.Add(v.Name);
        }

        return names;
    }

    private void RememberVolumeNames(HarnessControlSummary state)
    {
        lock (_volumeNames)
        {
            foreach (HarnessVolumeSummary v in state.Volumes)
                _volumeNames[v.Id] = v.Name;
        }
    }

    private static async Task<GroupInspection?> TryInspectGroupAsync(TestNode node, string group)
    {
        try
        {
            return node.State == TestNodeState.Running ? await node.Inspect.GetGroupAsync(group) : null;
        }
        catch (Exception ex) when (IsNodeGone(ex))
        {
            return null;
        }
    }

    /// <summary>The applied state of every running node that answers (in node order).</summary>
    internal async Task<IReadOnlyList<NodeView>> SnapshotAsync(bool groupsOnly = false)
    {
        var views = new List<NodeView>();
        foreach (TestNode node in RunningNodes())
        {
            try
            {
                NodeInspector inspector = node.Inspect;
                if (await IsForeignAsync(inspector))
                    continue; // runs another cluster's identity (SEC-001): never a member, leader or replica of this one
                IReadOnlyList<GroupInspection> groups = await inspector.GetGroupsAsync();
                HarnessNodeStatus status = groupsOnly ? EmptyStatus : await inspector.GetNodeStatusAsync();
                HarnessControlSummary control = groupsOnly ? EmptyControl : await inspector.GetControlSummaryAsync();
                if (!groupsOnly)
                    RememberVolumeNames(control);
                views.Add(new NodeView(node, status, groups, control));
            }
            catch (Exception ex) when (IsNodeGone(ex))
            {
                // Crashed, stopped or paused meanwhile: not part of this snapshot.
            }
        }

        return views;
    }

    /// <summary>
    /// True when the node runs under another cluster's identity (for example a member's endpoint re-bootstrapped as a foreign
    /// cluster): its groups ("control" included) belong to that cluster, so leader, stability and split-brain views of this
    /// cluster must not count them. A node that does not know its cluster yet (joining) is not foreign.
    /// </summary>
    internal async Task<bool> IsForeignAsync(NodeInspector inspector)
    {
        Guid id = await inspector.GetClusterIdAsync();
        return id != Guid.Empty && id != ClusterId;
    }

    /// <summary>True for the exceptions an inspector call raises when the node went away (crash, stop, pause, kill).</summary>
    internal static bool IsNodeGone(Exception ex) =>
        ex is InvalidOperationException or NodeLoopStoppedException or ObjectDisposedException or SimProcessKilledException or OperationCanceledException;

    private static readonly HarnessNodeStatus EmptyStatus = new(default, 0, false, false, false);

    private static readonly HarnessControlSummary EmptyControl = new(Guid.Empty, 0, 0, 0, [], [], [], []);

    /// <summary>One running node's applied state.</summary>
    internal sealed record NodeView(TestNode Node, HarnessNodeStatus Status, IReadOnlyList<GroupInspection> Groups, HarnessControlSummary Control)
    {
        /// <summary>This node's view of a group ("control" or a volume name), or null when it does not host it.</summary>
        public GroupInspection? Group(string name)
        {
            foreach (GroupInspection g in Groups)
            {
                if (string.Equals(g.Group, name, StringComparison.Ordinal))
                    return g;
            }

            return null;
        }
    }
}
