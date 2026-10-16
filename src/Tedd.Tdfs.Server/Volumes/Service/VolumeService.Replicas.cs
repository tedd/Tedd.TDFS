using System.Collections.Immutable;
using Tedd.Tdfs.Data;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Control;
using Tedd.Tdfs.Server.Volumes.Apply;
using Tedd.Tdfs.Server.Volumes.State;

namespace Tedd.Tdfs.Server.Volumes.Service;

// Replica tracking of the volume leader (ARCHITECTURE §6): where the volume's objects are, which holders are up, and the
// policy every referenced object must meet. Everything here is leader-local and rebuilt after a leadership change from
// the storage nodes' inventories (a new leader acts on it only after one liveness window, once every node had a chance to
// report).
internal sealed partial class VolumeService
{
    /// <summary>Known copies per object: holder → what its inventory (or a receipt) said, and when.</summary>
    private readonly Dictionary<ObjectId, Dictionary<NodeId, Holding>> _holdings = [];

    /// <summary>Objects per holder (index of <see cref="_holdings"/>).</summary>
    private readonly Dictionary<NodeId, HashSet<ObjectId>> _heldBy = [];

    /// <summary>Inventory state per reporting node.</summary>
    private readonly Dictionary<NodeId, NodeInventory> _inventoryNodes = [];

    /// <summary>Objects whose publication receipt holders were already taken as initial holdings in this term.</summary>
    private readonly HashSet<ObjectId> _seeded = [];

    private ReplicaPlans? _plans;

    /// <summary>One known copy.</summary>
    /// <param name="Incarnation">Holder incarnation the copy was reported (or receipted) under.</param>
    /// <param name="Staging">Staging session the copy is bound to.</param>
    /// <param name="Length">Plaintext length.</param>
    /// <param name="Checksum">XxHash128 of the plaintext.</param>
    /// <param name="ObservedAt">Leader timestamp of the observation.</param>
    /// <param name="Seeded">
    /// True for a copy assumed from the receipt holders recorded at publication (<see cref="SeedFromReceipts"/>), which no
    /// report of the holder's current incarnation confirmed: a location hint and a candidate source, never a counted copy
    /// in a safety decision (policy changes, migration completion, node removal), since the holder may have restarted with
    /// an empty disk since the publication (RCV-07).
    /// </param>
    private readonly record struct Holding(ulong Incarnation, StagingId Staging, long Length, UInt128 Checksum, long ObservedAt, bool Seeded = false);

    /// <summary>Inventory state of one node.</summary>
    private sealed class NodeInventory
    {
        /// <summary>Incarnation the knowledge below belongs to.</summary>
        public ulong Incarnation { get; set; }

        /// <summary>Last proof of life under <see cref="Incarnation"/>: a complete inventory or a receipt.</summary>
        public long LastReportAt { get; set; }
    }

    /// <summary>What a referenced object must meet.</summary>
    /// <param name="Object">Object id.</param>
    /// <param name="Policy">Strongest policy of the files referencing it.</param>
    /// <param name="Pool">Storage pool of that policy.</param>
    /// <param name="UsesDefault">True when a referencing file uses the volume's default policy.</param>
    /// <param name="Length">Plaintext length.</param>
    /// <param name="Checksum">XxHash128 of the plaintext.</param>
    /// <param name="Staging">Staging session of the publication.</param>
    private sealed record ObjectPlan(ObjectId Object, DfsStoragePolicy Policy, string Pool, bool UsesDefault, long Length, UInt128 Checksum, StagingId Staging);

    /// <summary>Plans of every referenced object, computed at an applied volume index and control index.</summary>
    private sealed record ReplicaPlans(ulong Applied, ulong ControlApplied, SortedDictionary<UInt128, ObjectPlan> Objects);

    /// <summary>How long a node's last complete inventory keeps it "up".</summary>
    private TimeSpan LivenessWindow => _s.Timings.InventoryInterval * 4;

    /// <summary>True once this leader had one liveness window to hear from every node.</summary>
    private bool ReplicaKnowledgeReady => _s.Time.GetElapsedTime(_leaderSince) >= LivenessWindow;

    /// <summary>Forgets every replica observation (leadership change).</summary>
    private void ClearReplicaTracking()
    {
        _holdings.Clear();
        _heldBy.Clear();
        _inventoryNodes.Clear();
        _seeded.Clear();
        _plans = null;
        ClearRepairState();
        ClearGcPushState();
    }

    /// <summary>
    /// Applies a complete inventory of <paramref name="node"/> taken no earlier than <paramref name="startedAt"/>: copies
    /// it no longer lists (observed before the report started) are forgotten, listed ones are refreshed. A new incarnation
    /// replaces everything known about the node (an empty disk is never credited with its former copies).
    /// </summary>
    private void RecordInventory(NodeId node, ulong incarnation, IReadOnlyCollection<InventoryItem> items, long startedAt)
    {
        long now = _s.Time.GetTimestamp();
        if (!_inventoryNodes.TryGetValue(node, out NodeInventory? state))
            _inventoryNodes[node] = state = new NodeInventory();
        if (state.Incarnation != incarnation)
        {
            ForgetNode(node);
            ForgetPendingDeletes(node);
        }

        state.Incarnation = incarnation;
        state.LastReportAt = now;
        var listed = new HashSet<ObjectId>(items.Select(i => i.Object));
        if (_heldBy.TryGetValue(node, out HashSet<ObjectId>? held))
        {
            foreach (ObjectId obj in held.ToArray())
            {
                if (!listed.Contains(obj) && _holdings.TryGetValue(obj, out Dictionary<NodeId, Holding>? holders)
                    && holders.TryGetValue(node, out Holding h) && h.ObservedAt < startedAt)
                {
                    RemoveHolding(obj, node);
                }
            }
        }

        foreach (InventoryItem item in items)
        {
            // A copy whose deletion the holder already confirmed is listed only by an inventory taken before the deletion:
            // it is not a copy (the holder may never report this volume again once it holds nothing of it).
            if (_deleted.Contains((item.Object, node)))
                continue;
            AddHolding(item.Object, node, new Holding(incarnation, item.Staging, item.Length, item.Checksum, now));
        }

        ResolveDeletesByInventory(node, listed);
    }

    /// <summary>
    /// Records a copy proven by a receipt (repair result). The receipt is also a proof of life of its node's registered
    /// incarnation: a node that restarted counts again from its first receipt, with only what it proved since.
    /// </summary>
    private void RecordReceipt(ObjectReceipt receipt)
    {
        if (_s.Control.State.FindMember(receipt.Node) is not { IsLive: true } member || member.Incarnation != receipt.Incarnation)
            return;
        long now = _s.Time.GetTimestamp();
        if (!_inventoryNodes.TryGetValue(receipt.Node, out NodeInventory? state))
            _inventoryNodes[receipt.Node] = state = new NodeInventory { Incarnation = receipt.Incarnation };
        if (state.Incarnation != receipt.Incarnation)
        {
            ForgetNode(receipt.Node);
            ForgetPendingDeletes(receipt.Node);
            state.Incarnation = receipt.Incarnation;
        }

        state.LastReportAt = now;
        SettleConfirmedDelete(receipt.Object, receipt.Node);
        AddHolding(receipt.Object, receipt.Node, new Holding(receipt.Incarnation, receipt.Staging, receipt.Length, receipt.Checksum, now));
    }

    private void AddHolding(ObjectId obj, NodeId node, Holding holding)
    {
        if (!_holdings.TryGetValue(obj, out Dictionary<NodeId, Holding>? holders))
            _holdings[obj] = holders = [];
        holders[node] = holding;
        if (!_heldBy.TryGetValue(node, out HashSet<ObjectId>? held))
            _heldBy[node] = held = [];
        held.Add(obj);
    }

    private void RemoveHolding(ObjectId obj, NodeId node)
    {
        if (_holdings.TryGetValue(obj, out Dictionary<NodeId, Holding>? holders))
        {
            holders.Remove(node);
            if (holders.Count == 0)
                _holdings.Remove(obj);
        }

        if (_heldBy.TryGetValue(node, out HashSet<ObjectId>? held))
        {
            held.Remove(obj);
            if (held.Count == 0)
                _heldBy.Remove(node);
        }
    }

    private void ForgetNode(NodeId node)
    {
        if (!_heldBy.TryGetValue(node, out HashSet<ObjectId>? held))
            return;
        foreach (ObjectId obj in held.ToArray())
            RemoveHolding(obj, node);
    }

    /// <summary>
    /// True when <paramref name="node"/> is a live member whose latest complete inventory (or receipt), under its
    /// registered incarnation, is recent: its known copies count. A node this leader has not heard from yet counts as up
    /// during the leader's first liveness window (its publication receipts stand until its first inventory).
    /// </summary>
    private bool IsUp(NodeId node, ControlState control, long now)
    {
        if (control.FindMember(node) is not { IsLive: true } member)
            return false;
        if (!_inventoryNodes.TryGetValue(node, out NodeInventory? state))
            return !ReplicaKnowledgeReady;
        return state.Incarnation == member.Incarnation && _s.Time.GetElapsedTime(state.LastReportAt, now) <= LivenessWindow;
    }

    /// <summary>
    /// True when <paramref name="node"/> is not a live member, or this leader heard from its registered incarnation but not
    /// within the liveness window: known down. A node not heard from under its registered incarnation is not known down.
    /// </summary>
    private bool IsKnownDown(NodeId node, ControlState control, long now)
    {
        if (control.FindMember(node) is not { IsLive: true } member)
            return true;
        return _inventoryNodes.TryGetValue(node, out NodeInventory? state) && state.Incarnation == member.Incarnation
            && _s.Time.GetElapsedTime(state.LastReportAt, now) > LivenessWindow;
    }

    /// <summary>
    /// Holders whose copies count now: up, the copy reported under the current incarnation, not being deleted (unless
    /// <paramref name="includeDeleting"/>), and confirmed by the holder rather than assumed from publication receipts (unless
    /// <paramref name="includeSeeded"/>).
    /// </summary>
    private List<NodeId> UpHolders(ObjectId obj, ControlState control, long now, bool includeDeleting = false, bool includeSeeded = true)
    {
        var result = new List<NodeId>();
        if (!_holdings.TryGetValue(obj, out Dictionary<NodeId, Holding>? holders))
            return result;
        foreach ((NodeId node, Holding h) in holders)
        {
            if (control.FindMember(node) is not { IsLive: true } member || h.Incarnation != member.Incarnation || !IsUp(node, control, now))
                continue;
            if (!includeSeeded && h.Seeded)
                continue;
            if (!includeDeleting && IsDeleting(obj, node))
                continue;
            result.Add(node);
        }

        result.Sort(static (a, b) => a.Value.CompareTo(b.Value));
        return result;
    }

    /// <summary>Location hints of an extent: nodes known to hold the object (up ones first), then the receipt holders at publication.</summary>
    internal IReadOnlyList<NodeId> LocationsOf(ExtentRef extent)
    {
        var result = new List<NodeId>();
        if (_holdings.TryGetValue(extent.Object, out Dictionary<NodeId, Holding>? holders))
        {
            ControlState control = _s.Control.State;
            long now = _s.Time.GetTimestamp();
            result.AddRange(holders.Keys.Where(n => IsUp(n, control, now)).OrderBy(n => n.Value));
            result.AddRange(holders.Keys.Where(n => !result.Contains(n)).OrderBy(n => n.Value));
        }

        if (!extent.Locations.IsDefault)
        {
            foreach (NodeId n in extent.Locations)
            {
                if (!result.Contains(n))
                    result.Add(n);
            }
        }

        return result;
    }

    /// <summary>
    /// The plan of every object referenced from an authoritative root (current namespace, snapshots, pins, version-pinned
    /// handles, restores): the strongest policy referencing it (copies first, then zones) and that policy's pool. Cached
    /// per applied volume index and control index.
    /// </summary>
    private ReplicaPlans Plans()
    {
        VolumeState s = _sm.State;
        ulong controlApplied = _s.Control.State.AppliedIndex;
        if (_plans is { } cached && cached.Applied == s.AppliedIndex && cached.ControlApplied == controlApplied)
            return cached;

        var objects = new SortedDictionary<UInt128, ObjectPlan>();
        var locations = new Dictionary<ObjectId, ImmutableArray<NodeId>>();
        var seen = new HashSet<(ulong, ulong)>();
        VolumeRecord record = Record;

        void AddRoot(ImmutableSortedDictionary<ulong, InodeRecord> root)
        {
            foreach (InodeRecord inode in root.Values)
                AddInode(inode);
        }

        void AddInode(InodeRecord inode)
        {
            if (!seen.Add((inode.Id, inode.Revision)))
                return;
            bool usesDefault = inode.PolicyName.Length == 0 || !record.NamedPolicies.ContainsKey(inode.PolicyName);
            (DfsStoragePolicy policy, string pool) = PolicyOf(inode.PolicyName);
            foreach (StreamRecord stream in inode.Streams.Values)
            {
                if (stream.IsInline)
                    continue;
                foreach (ExtentRef e in stream.Extents)
                {
                    if (!objects.TryGetValue(e.Object.Value, out ObjectPlan? plan))
                    {
                        objects[e.Object.Value] = new ObjectPlan(e.Object, policy, pool, usesDefault, e.ObjectLength, e.Checksum, e.Staging);
                        if (!e.Locations.IsDefaultOrEmpty)
                            locations[e.Object] = e.Locations;
                        continue;
                    }

                    bool stronger = policy.TargetCopies > plan.Policy.TargetCopies
                        || (policy.TargetCopies == plan.Policy.TargetCopies && policy.TargetDistinctZones > plan.Policy.TargetDistinctZones);
                    objects[e.Object.Value] = stronger
                        ? plan with { Policy = policy, Pool = pool, UsesDefault = plan.UsesDefault || usesDefault }
                        : plan with { UsesDefault = plan.UsesDefault || usesDefault };
                }
            }
        }

        AddRoot(s.Inodes);
        foreach (SnapshotRecord snapshot in s.Snapshots.Values)
            AddRoot(snapshot.Root);
        foreach (PinRecord pin in s.Pins.Values)
        {
            if (pin.Root is not null)
                AddRoot(pin.Root);
        }

        foreach (HandleRecord handle in s.Handles.Values)
        {
            if (handle.PinnedInode is not null)
                AddInode(handle.PinnedInode);
        }

        if (s.Restore is { } restore)
            AddRoot(restore.Staged);
        foreach (RestoreIntentRecord intent in s.Restores.Values)
            AddRoot(intent.Root);

        SeedFromReceipts(objects.Values, locations);
        _plans = new ReplicaPlans(s.AppliedIndex, controlApplied, objects);
        return _plans;
    }

    /// <summary>
    /// Takes the receipt holders recorded at publication as the initial holdings of objects this leader knows nothing about
    /// yet (once per object and term): a just-published object is not reported missing before the next inventories.
    /// Inventories that start later correct them.
    /// </summary>
    private void SeedFromReceipts(IEnumerable<ObjectPlan> plans, Dictionary<ObjectId, ImmutableArray<NodeId>> locations)
    {
        ControlState control = _s.Control.State;
        long now = _s.Time.GetTimestamp();
        var planned = new HashSet<ObjectId>();
        foreach (ObjectPlan plan in plans)
        {
            planned.Add(plan.Object);
            if (!_seeded.Add(plan.Object) || _holdings.ContainsKey(plan.Object) || !locations.TryGetValue(plan.Object, out ImmutableArray<NodeId> holders))
                continue;
            foreach (NodeId node in holders)
            {
                // A complete inventory of the holder's current incarnation is authoritative: it did not list the copy.
                if (control.FindMember(node) is { IsLive: true } member
                    && !(_inventoryNodes.TryGetValue(node, out NodeInventory? reported) && reported.Incarnation == member.Incarnation))
                {
                    AddHolding(plan.Object, node, new Holding(member.Incarnation, plan.Staging, plan.Length, plan.Checksum, now, Seeded: true));
                }
            }
        }

        // Objects no longer referenced are forgotten (bounded by the referenced set).
        _seeded.RemoveWhere(o => !planned.Contains(o));
    }

    /// <summary>Live members of a pool, as a set.</summary>
    private static HashSet<NodeId> PoolMemberSet(ControlState control, string pool) => [.. control.PoolMembers(pool).Select(m => m.Id)];

    /// <summary>Copies and zones of a set of holders (distinct hosts, distinct region/zone pairs).</summary>
    private static (int Copies, int Zones) DomainsOf(ControlState control, IEnumerable<NodeId> holders)
        => Placement.CountDomains(holders.Select(h => control.FindMember(h)?.Labels).OfType<DfsTopologyLabels>());

    /// <summary>
    /// Replica health of the referenced objects attributed to <paramref name="pool"/> (null: every pool), judged against the
    /// up holders that are members of the object's pool; a copy on a node outside the pool is a placement violation.
    /// </summary>
    private ReplicaHealthData ComputeHealth(string? pool)
    {
        ControlState control = _s.Control.State;
        long now = _s.Time.GetTimestamp();
        var members = new Dictionary<string, HashSet<NodeId>>(StringComparer.Ordinal);
        long objects = 0, under = 0, zoneDeficit = 0, missing = 0, violations = 0, backlog = 0;
        foreach (ObjectPlan plan in Plans().Objects.Values)
        {
            if (pool is not null && !string.Equals(plan.Pool, pool, StringComparison.Ordinal))
                continue;
            if (!members.TryGetValue(plan.Pool, out HashSet<NodeId>? poolMembers))
                members[plan.Pool] = poolMembers = PoolMemberSet(control, plan.Pool);
            // A copy whose excess-replica deletion is in flight no longer counts (it may be gone any moment), but it is
            // still a placement violation while it exists outside the pool.
            List<NodeId> up = UpHolders(plan.Object, control, now, includeDeleting: true);
            (int copies, int zones) = DomainsOf(control, up.Where(n => poolMembers.Contains(n) && !IsDeleting(plan.Object, n)));
            objects++;
            if (copies == 0)
                missing++;
            if (copies < plan.Policy.TargetCopies)
            {
                under++;
                if (copies > 0)
                    backlog += plan.Length;
            }

            if (zones < plan.Policy.TargetDistinctZones)
                zoneDeficit++;
            if (up.Any(h => !poolMembers.Contains(h)))
                violations++;
        }

        bool whole = pool is null || string.Equals(pool, Record.StoragePool, StringComparison.Ordinal);
        long pendingGc = whole ? _pendingGc.Values.Sum() : 0;
        long staged = whole ? StagedBytes(State) : 0;
        long pinned = whole ? _sm.PinnedBytes : 0;
        return new ReplicaHealthData(objects, under, zoneDeficit, missing, violations, backlog, pendingGc, staged, pinned);
    }

    /// <summary>Replica health of the volume as the leader knows it (inventories, receipts and the referenced objects).</summary>
    public ReplicaHealthData ReplicaHealth() => ComputeHealth(null);

    /// <summary>Most objects one health query probes.</summary>
    private const int MaxProbedObjects = 4096;

    /// <summary>
    /// Brings the replica knowledge of under-replicated objects up to date before a health answer: asks the pool members
    /// that are not known to hold them (<c>LocateObject</c>, one call per node) and records the copies they confirm, so an
    /// answer never lags behind copies that already exist (a late upload, a copy made since the last inventory).
    /// </summary>
    private async Task ProbeDeficitsAsync(CancellationToken ct)
    {
        ControlState control = _s.Control.State;
        long now = _s.Time.GetTimestamp();
        var perNode = new Dictionary<NodeId, List<ObjectId>>();
        var members = new Dictionary<string, IReadOnlyList<MemberRecord>>(StringComparer.Ordinal);
        int probed = 0;
        foreach (ObjectPlan plan in Plans().Objects.Values)
        {
            if (probed >= MaxProbedObjects)
                break;
            if (!members.TryGetValue(plan.Pool, out IReadOnlyList<MemberRecord>? poolMembers))
                members[plan.Pool] = poolMembers = control.PoolMembers(plan.Pool);
            List<NodeId> up = UpHolders(plan.Object, control, now, includeDeleting: true);
            (int copies, int zones) = DomainsOf(control, up.Where(n => !IsDeleting(plan.Object, n) && poolMembers.Any(m => m.Id == n)));
            if (copies >= plan.Policy.TargetCopies && zones >= plan.Policy.TargetDistinctZones)
                continue;
            probed++;
            foreach (MemberRecord member in poolMembers)
            {
                if (up.Contains(member.Id) || !IsUp(member.Id, control, now) && _inventoryNodes.ContainsKey(member.Id))
                    continue;
                if (!perNode.TryGetValue(member.Id, out List<ObjectId>? list))
                    perNode[member.Id] = list = [];
                list.Add(plan.Object);
            }
        }

        if (perNode.Count == 0)
            return;
        ulong term = Group.Term;

        // Best effort and bounded as a whole: a node that cannot be reached (down, or without a link to this leader on a
        // partial mesh, where even connecting waits for the connect timeout) must not hold up the status request that asked.
        // One election window: well inside any caller's attempt timeout (which is derived from the same timings). What
        // answered by then counts; the rest finishes (and is ignored) later.
        TimeSpan window = _s.Timings.Tick * _s.Timings.ElectionTicksMax;
        using CancellationTokenSource probeDeadline = _s.CreateDeadline(window);
        using var probe = CancellationTokenSource.CreateLinkedTokenSource(ct, probeDeadline.Token);
        Task<(NodeId Node, IReadOnlyList<ObjectLocation>? Locations)>[] calls = [.. perNode.Select(p => LocateOnAsync(p.Key, p.Value, probe.Token))];
        Task all = Task.WhenAll(calls);
        if (!all.IsCompleted)
        {
            await Task.WhenAny(all, Task.Delay(window, _s.Time));
            probe.Cancel(); // the calls still running end now (before their token source is disposed)
        }

        ct.ThrowIfCancellationRequested();
        foreach ((NodeId node, IReadOnlyList<ObjectLocation>? locations) in calls.Where(c => c.IsCompletedSuccessfully).Select(c => c.Result))
        {
            if (locations is null || !IsLeader || Group.Term != term || control.FindMember(node) is not { IsLive: true } member)
                continue;
            foreach (ObjectLocation location in locations)
            {
                // A reply may have been computed before a deletion this leader saw confirmed meanwhile.
                if (location.Present && !_deleted.Contains((location.Object, node)))
                {
                    AddHolding(location.Object, node, new Holding(member.Incarnation, location.Staging, location.Length, location.Checksum, _s.Time.GetTimestamp()));
                    if (_inventoryNodes.TryGetValue(node, out NodeInventory? state) && state.Incarnation == member.Incarnation)
                        state.LastReportAt = Math.Max(state.LastReportAt, _s.Time.GetTimestamp());
                }
            }
        }
    }

    /// <summary>One <c>LocateObject</c> (or a local lookup); null when the node did not answer (or <paramref name="ct"/> ended the wait).</summary>
    private async Task<(NodeId Node, IReadOnlyList<ObjectLocation>? Locations)> LocateOnAsync(NodeId node, List<ObjectId> objects, CancellationToken ct)
    {
        if (node == _s.NodeId)
        {
            var local = new List<ObjectLocation>(objects.Count);
            foreach (ObjectId obj in objects)
            {
                local.Add(_s.ObjectStore.TryGetInfo(obj) is { } info && info.Volume == Record.Id
                    ? new ObjectLocation(obj, true, info.Length, info.Checksum, info.Staging)
                    : new ObjectLocation(obj, false, 0, UInt128.Zero, default));
            }

            return (node, local);
        }

        try
        {
            var reply = (LocateObjectResultMessage)await _s.Peers.CallAsync(node, new LocateObjectMessage(Record.Id, objects), _s.Peers.DefaultCallTimeout / 2, ct);
            return (node, reply.Locations);
        }
        catch (Exception ex) when (ex is DfsException or IOException or TimeoutException or OperationCanceledException or InvalidCastException)
        {
            return (node, null);
        }
    }

    /// <summary>
    /// Referenced objects that use the default policy and do not meet <paramref name="target"/> yet (copies and zones among
    /// the up holders in their pool): the pending work of a default-policy migration.
    /// </summary>
    private long PendingMigrationObjects(DfsStoragePolicy target)
    {
        ControlState control = _s.Control.State;
        long now = _s.Time.GetTimestamp();
        var members = new Dictionary<string, HashSet<NodeId>>(StringComparer.Ordinal);
        long pending = 0;
        foreach (ObjectPlan plan in Plans().Objects.Values)
        {
            if (!plan.UsesDefault)
                continue;
            if (!members.TryGetValue(plan.Pool, out HashSet<NodeId>? poolMembers))
                members[plan.Pool] = poolMembers = PoolMemberSet(control, plan.Pool);
            // Copies being deleted as excess replicas, or only assumed from publication receipts, do not count: a migration is
            // never declared complete on a copy that is about to disappear or may not exist (REP-014, CHA-002).
            (int copies, int zones) = DomainsOf(control, UpHolders(plan.Object, control, now, includeSeeded: false).Where(poolMembers.Contains));
            if (copies < target.TargetCopies || zones < target.TargetDistinctZones)
                pending++;
        }

        return pending;
    }

    /// <summary>
    /// The replica-state summary this leader sends with its usage report (null while it is not ready or has not heard from
    /// every node yet): per-pool stored bytes and health, per-node holdings of kept copies with the objects whose
    /// acknowledged durability depends on each node, and a completed default-policy migration.
    /// </summary>
    public VolumeUsageReport? BuildUsageReport()
    {
        if (!IsLeader || _readyTerm != Group.Term || _leaseTerm != Group.Term || !ReplicaKnowledgeReady)
            return null;
        ControlState control = _s.Control.State;
        VolumeRecord record = Record;
        VolumeState s = _sm.State;
        long now = _s.Time.GetTimestamp();
        ReplicaPlans plans = Plans();

        // Stored bytes per pool: every copy (garbage included) held by a member of the object's pool.
        var stored = new SortedDictionary<string, long>(StringComparer.Ordinal);
        var members = new Dictionary<string, HashSet<NodeId>>(StringComparer.Ordinal);
        HashSet<NodeId> MembersOf(string pool)
        {
            if (!members.TryGetValue(pool, out HashSet<NodeId>? set))
                members[pool] = set = PoolMemberSet(control, pool);
            return set;
        }

        foreach (string pool in PoolsOf(record))
            stored[pool] = 0;
        foreach ((ObjectId obj, Dictionary<NodeId, Holding> holders) in _holdings)
        {
            string pool = plans.Objects.TryGetValue(obj.Value, out ObjectPlan? plan) ? plan.Pool : record.StoragePool;
            HashSet<NodeId> poolMembers = MembersOf(pool);
            foreach ((NodeId node, Holding h) in holders)
            {
                if (poolMembers.Contains(node) && control.FindMember(node) is { } m && m.Incarnation == h.Incarnation)
                    stored[pool] = stored.GetValueOrDefault(pool) + h.Length;
            }
        }

        foreach (ObjectPlan plan in plans.Objects.Values)
            stored.TryAdd(plan.Pool, 0);
        var pools = new List<PoolUsageReport>(stored.Count);
        foreach ((string pool, long bytes) in stored)
            pools.Add(new PoolUsageReport(pool, bytes, ComputeHealth(pool)));

        // Holdings per node: copies the volume keeps, and objects whose acknowledged durability depends on the node.
        HashSet<ObjectId> protectedObjects = VolumeStateMachine.Protected(s, VolumeStateMachine.Watermark(s, _s.Mutations.IgnoreSnapshotPins));
        var reachable = new HashSet<ObjectId>(plans.Objects.Values.Select(p => p.Object));
        var nodes = new List<NodeHoldingReport>();
        foreach (NodeId node in _inventoryNodes.Keys.Concat(_heldBy.Keys).Distinct().OrderBy(n => n.Value))
        {
            long objects = 0, bytes = 0, neededForAck = 0;
            if (_heldBy.TryGetValue(node, out HashSet<ObjectId>? held))
            {
                foreach (ObjectId obj in held)
                {
                    Holding h = _holdings[obj][node];
                    if (VolumeStateMachine.Classify(s, obj, h.Staging.Value, reachable, protectedObjects) == ObjectClass.Deletable)
                        continue;
                    objects++;
                    bytes += h.Length;
                    if (!plans.Objects.TryGetValue(obj.Value, out ObjectPlan? plan))
                        continue;
                    HashSet<NodeId> poolMembers = MembersOf(plan.Pool);
                    // Other holders whose excess-replica deletion is in flight do not count (OPS-002: a removal must
                    // never rely on a copy that is being deleted).
                    (int copies, int zones) = DomainsOf(control, UpHolders(obj, control, now, includeSeeded: false).Where(n => n != node && poolMembers.Contains(n)));
                    if (copies < plan.Policy.DurableCopiesBeforeAck || zones < plan.Policy.DurableZonesBeforeAck)
                        neededForAck++;
                }
            }

            nodes.Add(new NodeHoldingReport(node, IsUp(node, control, now), objects, bytes, neededForAck));
        }

        DfsStoragePolicy? completed = record.MigrationTarget is { } target && PendingMigrationObjects(target) == 0 ? target : null;
        return new VolumeUsageReport(record.Id, record.Epoch, Group.Term, control.PlacementEpoch, completed, pools, nodes);
    }

    /// <summary>Every pool the volume's policies place objects in.</summary>
    private static IEnumerable<string> PoolsOf(VolumeRecord record)
    {
        var pools = new SortedSet<string>(StringComparer.Ordinal) { record.DefaultPolicy.Pool ?? record.StoragePool };
        if (record.MigrationTarget is { } target)
            pools.Add(target.Pool ?? record.StoragePool);
        foreach (DfsStoragePolicy named in record.NamedPolicies.Values)
            pools.Add(named.Pool ?? record.StoragePool);
        return pools;
    }
}
