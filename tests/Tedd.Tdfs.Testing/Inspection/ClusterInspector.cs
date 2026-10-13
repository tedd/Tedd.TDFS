using System.Globalization;
using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Storage;

namespace Tedd.Tdfs.Testing;

/// <summary>Cluster-wide inspection over all running nodes (reads durable/applied state, not client views).</summary>
/// <remarks>
/// Every member reads the nodes' own applied and stored state through their in-process inspectors (node-loop
/// invocations), never through the product's client-facing APIs, so it serves as harness ground truth: "holders" are
/// running nodes holding a copy that re-reads and verifies, replica health is computed by the harness from the leader's
/// reachable object set and those holders, and convergence compares the replicated state digests of replicas at the same
/// applied index. Nodes that are not <see cref="TestNodeState.Running"/> are not consulted. Out-of-process clusters have no
/// in-process inspectors: every member throws <see cref="NotSupportedException"/> there.
/// </remarks>
public sealed partial class ClusterInspector
{
    private readonly TestCluster _cluster;

    internal ClusterInspector(TestCluster cluster)
    {
        _cluster = cluster;
    }

    /// <summary>The control group as seen by its leader.</summary>
    public async Task<GroupInspection> GetControlGroupAsync()
    {
        RequireInProcess();
        TestNode leader = await _cluster.GetControlLeaderAsync();
        return await leader.Inspect.GetGroupAsync(ProtocolInfo.ControlGroup)
            ?? throw new InvalidOperationException($"The control leader {leader.Name} stopped hosting the control group.");
    }

    /// <summary>A volume group as seen by its leader.</summary>
    public async Task<GroupInspection> GetVolumeGroupAsync(string volume)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(volume);
        RequireInProcess();
        TestNode leader = await _cluster.GetLeaderAsync(volume);
        return await leader.Inspect.GetGroupAsync(volume)
            ?? throw new InvalidOperationException($"The leader {leader.Name} of volume {volume} stopped hosting it.");
    }

    /// <summary>A replica's applied volume state (default: the leader).</summary>
    /// <exception cref="InvalidOperationException">The node does not run or does not host the volume.</exception>
    public async Task<VolumeStateInspection> GetVolumeStateAsync(string volume, string? node = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(volume);
        RequireInProcess();
        TestNode target = node is null ? await _cluster.GetLeaderAsync(volume) : _cluster.Node(node);
        return await target.Inspect.GetVolumeStateAsync(volume)
            ?? throw new InvalidOperationException($"Node {target.Name} does not host volume {volume}.");
    }

    /// <summary>A file's inode/extent view on the leader; null when absent.</summary>
    public async Task<FileInspection?> GetFileAsync(string volume, string path)
    {
        ArgumentNullException.ThrowIfNull(path);
        VolumeStateInspection state = await GetVolumeStateAsync(volume);
        return state.GetFile(path);
    }

    /// <summary>Running nodes durably holding a verified copy of an object (node order).</summary>
    public async Task<IReadOnlyList<string>> GetObjectHoldersAsync(ObjectId id)
    {
        RequireInProcess();
        IReadOnlyDictionary<ObjectId, List<TestNode>> holders = await VerifiedHoldersAsync([id]);
        return holders.TryGetValue(id, out List<TestNode>? nodes) ? nodes.Select(n => n.Name).ToArray() : [];
    }

    /// <summary>Distinct hosts and zones durably holding an object (verified copies on running nodes).</summary>
    public async Task<(int Copies, int Zones)> CountDurableDomainsAsync(ObjectId id)
    {
        RequireInProcess();
        IReadOnlyDictionary<ObjectId, List<TestNode>> holders = await VerifiedHoldersAsync([id]);
        return holders.TryGetValue(id, out List<TestNode>? nodes) ? Domains(nodes) : (0, 0);
    }

    /// <summary>Replica health of a volume, computed by the harness (not read from the product).</summary>
    /// <remarks>
    /// Objects: those reachable from the leader's applied state (current root, snapshots, transactions, pins). Each is
    /// measured against the volume's default policy: under-replicated when fewer distinct hosts than
    /// <see cref="DfsStoragePolicy.TargetCopies"/> hold a verified copy, zone deficit when those copies span fewer zones than
    /// <see cref="DfsStoragePolicy.TargetDistinctZones"/>, missing when no verified copy exists, a placement violation when a
    /// copy lies on a node outside the volume's storage pool. The backlog is the plaintext bytes of under-replicated objects
    /// with at least one copy; pending GC counts stored records of the volume that are no longer reachable; staged and
    /// pinned bytes are the leader's.
    /// </remarks>
    public async Task<DfsReplicaHealth> GetReplicaHealthAsync(string volume)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(volume);
        RequireInProcess();
        TestNode leader = await _cluster.GetLeaderAsync(volume);
        NodeInspector leaderInspector = leader.Inspect;
        VolumeStateInspection state = await leaderInspector.GetVolumeStateAsync(volume)
            ?? throw new InvalidOperationException($"The leader {leader.Name} of volume {volume} does not host it.");
        HarnessControlSummary control = await leaderInspector.GetControlSummaryAsync();
        HarnessVolumeSummary summary = control.FindVolume(volume) ?? throw new ArgumentException($"Volume '{volume}' does not exist.", nameof(volume));
        IReadOnlySet<ObjectId> reachable = state.ReachableObjects();
        DfsStoragePolicy policy = summary.DefaultPolicy;
        IReadOnlySet<string> poolNodes = PoolMembers(control, summary.StoragePool);

        var lengths = new Dictionary<ObjectId, long>();
        var stored = new HashSet<ObjectId>();
        foreach (TestNode node in _cluster.RunningNodes())
        {
            IReadOnlyList<ObjectRecordInfo> records;
            try
            {
                records = await node.Inspect.ListObjectsAsync(volume);
            }
            catch (Exception ex) when (TestCluster.IsNodeGone(ex))
            {
                continue;
            }

            foreach (ObjectRecordInfo record in records)
            {
                stored.Add(record.Id);
                lengths.TryAdd(record.Id, record.Length);
            }
        }

        IReadOnlyDictionary<ObjectId, List<TestNode>> holders = await VerifiedHoldersAsync(reachable);
        long under = 0, zoneDeficit = 0, missing = 0, violations = 0, backlog = 0;
        foreach (ObjectId id in reachable)
        {
            List<TestNode> nodes = holders.TryGetValue(id, out List<TestNode>? h) ? h : [];
            (int copies, int zones) = Domains(nodes);
            if (copies == 0)
                missing++;
            if (copies < policy.TargetCopies)
            {
                under++;
                if (copies > 0 && lengths.TryGetValue(id, out long length))
                    backlog += length;
            }

            if (zones < policy.TargetDistinctZones)
                zoneDeficit++;
            if (nodes.Any(n => !poolNodes.Contains(n.Name)))
                violations++;
        }

        long pendingGc = stored.Count(id => !reachable.Contains(id));
        long staged = await leaderInspector.GetStagedBytesAsync();
        long pinned = await leaderInspector.GetPinnedBytesAsync();
        return new DfsReplicaHealth(reachable.Count, under, zoneDeficit, missing, violations, backlog, pendingGc, staged, pinned);
    }

    /// <summary>All objects stored on any running node (optionally of one volume), staged records included.</summary>
    public async Task<IReadOnlySet<ObjectId>> GetAllStoredObjectsAsync(string? volume = null)
    {
        RequireInProcess();
        var all = new HashSet<ObjectId>();
        foreach (TestNode node in _cluster.RunningNodes())
        {
            try
            {
                foreach (ObjectRecordInfo record in await node.Inspect.ListObjectsAsync(volume))
                    all.Add(record.Id);
            }
            catch (Exception ex) when (TestCluster.IsNodeGone(ex))
            {
                // Not running any more: not part of the answer.
            }
        }

        return all;
    }

    /// <summary>Sum of all running nodes' counters.</summary>
    public async Task<NodeCounters> GetTotalCountersAsync()
    {
        RequireInProcess();
        NodeCounters total = NodeCounters.Zero;
        foreach (TestNode node in _cluster.RunningNodes())
        {
            try
            {
                total = total.Add(await node.Inspect.GetCountersAsync());
            }
            catch (Exception ex) when (TestCluster.IsNodeGone(ex))
            {
                // Not running any more.
            }
        }

        return total;
    }

    /// <summary>Asserts at most one leader per term per group.</summary>
    /// <remarks>
    /// Checks every leadership observed during the whole run (each node reports every group it starts leading, with its
    /// term) and the current roles of the running nodes.
    /// </remarks>
    /// <exception cref="Xunit.Sdk.XunitException">Some group had two leaders in one term.</exception>
    public async Task AssertNoSplitBrainAsync()
    {
        _cluster.CheckNoSplitBrain();
        if (_cluster.Options.OutOfProcess)
            return;
        IReadOnlyList<TestCluster.NodeView> views = await _cluster.SnapshotAsync(groupsOnly: true);
        var leaders = new Dictionary<(string Group, ulong Term), string>();
        foreach (TestCluster.NodeView view in views)
        {
            foreach (GroupInspection group in view.Groups)
            {
                if (group.Role != RaftRole.Leader)
                    continue;
                if (leaders.TryGetValue((group.Group, group.Term), out string? other) && other != view.Node.Name)
                {
                    throw new Xunit.Sdk.XunitException(string.Create(CultureInfo.InvariantCulture,
                        $"Split brain: group {group.Group} has leaders {other} and {view.Node.Name} in term {group.Term}."));
                }

                leaders[(group.Group, group.Term)] = view.Node.Name;
            }
        }
    }

    /// <summary>Asserts identical digests on all caught-up replicas of a volume.</summary>
    /// <remarks>
    /// Replicas are compared by applied index: any two running replicas at the same applied index must report the same
    /// replicated-state digest (replicated state is a deterministic function of the applied log prefix), which includes every
    /// replica that caught up with the leader.
    /// </remarks>
    /// <exception cref="Xunit.Sdk.XunitException">Two replicas at the same applied index have different states.</exception>
    public async Task AssertReplicasConvergedAsync(string volume)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(volume);
        RequireInProcess();
        IReadOnlyList<string> divergence = await FindDivergenceAsync(volume);
        if (divergence.Count > 0)
            throw new Xunit.Sdk.XunitException($"Replicas of {volume} diverged: " + string.Join("; ", divergence));
    }

    // -------------------------------------------------------------------------------------------- harness internals

    /// <summary>Descriptions of replicas of <paramref name="group"/> whose state differs from another replica at the same applied index.</summary>
    internal async Task<IReadOnlyList<string>> FindDivergenceAsync(string group)
    {
        var byApplied = new SortedDictionary<ulong, List<(string Node, string Digest)>>();
        foreach (TestNode node in _cluster.RunningNodes())
        {
            string? line;
            try
            {
                line = await node.Inspect.ComputeGroupDigestAsync(group);
            }
            catch (Exception ex) when (TestCluster.IsNodeGone(ex))
            {
                continue;
            }

            if (line is null || !TryParseDigestLine(line, out ulong applied, out string digest))
                continue;
            if (!byApplied.TryGetValue(applied, out List<(string Node, string Digest)>? list))
                byApplied[applied] = list = [];
            list.Add((node.Name, digest));
        }

        var problems = new List<string>();
        foreach ((ulong applied, List<(string Node, string Digest)> replicas) in byApplied)
        {
            if (replicas.Select(r => r.Digest).Distinct(StringComparer.Ordinal).Count() > 1)
            {
                problems.Add(string.Create(CultureInfo.InvariantCulture,
                    $"group {group} at applied index {applied}: {string.Join(", ", replicas.Select(r => r.Node + "=" + r.Digest[..Math.Min(12, r.Digest.Length)]))}"));
            }
        }

        return problems;
    }

    /// <summary>Pool member names as applied by the control leader.</summary>
    internal async Task<IReadOnlyList<string>> PoolNodesAsync(string pool)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(pool);
        RequireInProcess();
        TestNode leader = await _cluster.GetControlLeaderAsync();
        HarnessControlSummary control = await leader.Inspect.GetControlSummaryAsync();
        return PoolMembers(control, pool).Order(StringComparer.Ordinal).ToArray();
    }

    /// <summary>Object ids held by one node's store (optionally of one volume).</summary>
    internal async Task<IReadOnlySet<ObjectId>> StoredObjectsOnNodeAsync(string node, string? volume)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(node);
        RequireInProcess();
        TestNode target = _cluster.Node(node);
        IReadOnlyList<ObjectRecordInfo> records = await target.Inspect.ListObjectsAsync(volume);
        return records.Select(r => r.Id).ToHashSet();
    }

    private static IReadOnlySet<string> PoolMembers(HarnessControlSummary control, string pool)
    {
        if (string.Equals(pool, DfsPoolNames.Default, StringComparison.Ordinal))
            return control.Members.Where(m => m.IsLive).Select(m => m.Name).ToHashSet(StringComparer.Ordinal);
        HarnessPoolSummary? summary = control.Pools.FirstOrDefault(p => string.Equals(p.Name, pool, StringComparison.Ordinal));
        return summary is null ? new HashSet<string>(StringComparer.Ordinal) : summary.Nodes.ToHashSet(StringComparer.Ordinal);
    }

    private async Task<IReadOnlyDictionary<ObjectId, List<TestNode>>> VerifiedHoldersAsync(IReadOnlyCollection<ObjectId> ids)
    {
        var holders = new Dictionary<ObjectId, List<TestNode>>();
        if (ids.Count == 0)
            return holders;
        foreach (TestNode node in _cluster.RunningNodes())
        {
            IReadOnlySet<ObjectId> verified;
            try
            {
                verified = await node.Inspect.VerifyObjectsAsync(ids);
            }
            catch (Exception ex) when (TestCluster.IsNodeGone(ex))
            {
                continue;
            }

            foreach (ObjectId id in verified)
            {
                if (!holders.TryGetValue(id, out List<TestNode>? list))
                    holders[id] = list = [];
                list.Add(node);
            }
        }

        return holders;
    }

    private static (int Copies, int Zones) Domains(IReadOnlyCollection<TestNode> nodes) =>
        (nodes.Select(n => n.Labels.Host).Distinct(StringComparer.Ordinal).Count(),
            nodes.Select(n => n.Labels.Region + "\0" + n.Labels.Zone).Distinct(StringComparer.Ordinal).Count());

    private static bool TryParseDigestLine(string line, out ulong applied, out string digest)
    {
        // "name@applied:sha256;"
        applied = 0;
        digest = string.Empty;
        int at = line.LastIndexOf('@');
        if (at < 0)
            return false;
        int colon = line.IndexOf(':', at + 1);
        if (colon < 0)
            return false;
        if (!ulong.TryParse(line.AsSpan(at + 1, colon - at - 1), NumberStyles.None, CultureInfo.InvariantCulture, out applied))
            return false;
        digest = line[(colon + 1)..].TrimEnd(';');
        return true;
    }

    private void RequireInProcess()
    {
        if (_cluster.Options.OutOfProcess)
            throw new NotSupportedException("Out-of-process clusters have no in-process inspectors; read state through the admin API.");
    }
}
