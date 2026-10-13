using System.Collections.Immutable;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Control;
using Tedd.Tdfs.Server.Runtime;
using Tedd.Tdfs.Server.Volumes.Apply;
using Tedd.Tdfs.Server.Volumes.State;

namespace Tedd.Tdfs.Server.Volumes.Service;

internal sealed partial class VolumeService
{
    /// <summary>Leader-local object-location map from inventories (holders, most recent knowledge first).</summary>
    private readonly Dictionary<ObjectId, List<NodeId>> _locations = [];

    /// <summary>Inventory reports being received (paged), per (node, report id).</summary>
    private readonly Dictionary<(NodeId Node, ulong Report), InventoryAccumulator> _inventories = [];

    /// <summary>Copies classified deletable by the last completed inventory of each node (replica health backlog).</summary>
    private readonly Dictionary<NodeId, int> _pendingGc = [];

    /// <summary>
    /// Unpublished copies (unreachable, under an open staging session) in the last completed inventory of each node: the
    /// staged-bytes accounting of replica health (GC-015).
    /// </summary>
    private readonly Dictionary<NodeId, Dictionary<ObjectId, (ulong Staging, long Length)>> _staged = [];

    /// <summary>Location hints of an extent: receipt holders at publication, then nodes that reported a copy.</summary>
    internal IReadOnlyList<NodeId> LocationsOf(ExtentRef extent)
    {
        IEnumerable<NodeId> known = _locations.TryGetValue(extent.Object, out List<NodeId>? holders) ? holders : [];
        return known.Concat(extent.Locations.IsDefault ? [] : extent.Locations).Distinct().ToArray();
    }

    /// <summary>
    /// <c>InventoryReport</c> (ARCHITECTURE §6, §8): a storage node reports its copies of this volume (paged). The leader
    /// updates its location map and, on the last page, classifies every reported copy at its applied index: deletable iff
    /// unreachable from every root, not protected by the dereference log at the GC watermark, and its staging session closed
    /// (open sessions protect their objects regardless of age, GC-005); copies of staging sessions newer than the applied
    /// state are reported unknown.
    /// </summary>
    public async Task<IWireMessage> InventoryReportAsync(RpcContext context, InventoryReportMessage request)
    {
        if (context.PeerNode != request.Node)
            throw new UnauthorizedAccessException("A node can only report its own inventory.");
        return await InventoryReportCoreAsync(request, context.Cancellation);
    }

    /// <summary>The inventory of the leader's own node (no RPC; same rules).</summary>
    public Task<InventoryReportResultMessage> InventoryReportLocalAsync(InventoryReportMessage request, CancellationToken ct)
    {
        if (request.Node != _s.NodeId)
            throw new UnauthorizedAccessException("A node can only report its own inventory.");
        return InventoryReportCoreAsync(request, ct);
    }

    private async Task<InventoryReportResultMessage> InventoryReportCoreAsync(InventoryReportMessage request, CancellationToken ct)
    {
        if (!IsLeader)
            throw Group.NotLeader();
        if (_s.Control.State.FindMember(request.Node) is not { IsLive: true } member || member.Incarnation != request.Incarnation)
            throw new DfsException(DfsErrorCode.StaleIncarnation, $"Inventory of node {request.Node} incarnation {request.Incarnation} is not current.");
        await EnsureLeaderReadyAsync(ct);
        var key = (request.Node, request.ReportId);
        if (!_inventories.TryGetValue(key, out InventoryAccumulator? accumulator) || request.Page == 0)
        {
            accumulator = new InventoryAccumulator();
            _inventories[key] = accumulator;
        }

        if (request.Page != accumulator.NextPage)
            throw new ArgumentException($"Inventory page {request.Page} arrived out of order (expected {accumulator.NextPage}).");
        accumulator.NextPage++;
        foreach (InventoryItem item in request.Items)
            accumulator.Items.Add(item);
        if (!request.LastPage)
            return new InventoryReportResultMessage([], [], 0, Record.Epoch);

        _inventories.Remove(key);
        VolumeState s = State;
        RecordHoldings(request.Node, accumulator.Items);
        HashSet<ObjectId> reachable = VolumeStateMachine.Reachable(s);
        HashSet<ObjectId> protectedObjects = VolumeStateMachine.Protected(s, VolumeStateMachine.Watermark(s, _s.Mutations.IgnoreSnapshotPins));
        var delete = new List<ObjectStagingRef>();
        var unknown = new List<ObjectId>();
        var staged = new Dictionary<ObjectId, (ulong Staging, long Length)>();
        foreach (InventoryItem item in accumulator.Items.OrderBy(i => i.Object.Value))
        {
            switch (VolumeStateMachine.Classify(s, item.Object, item.Staging.Value, reachable, protectedObjects))
            {
                case ObjectClass.Deletable:
                    delete.Add(new ObjectStagingRef(item.Object, item.Staging));
                    break;
                case ObjectClass.Unknown:
                    unknown.Add(item.Object);
                    break;
                case ObjectClass.Keep when !reachable.Contains(item.Object) && s.Staging.TryGetValue(item.Staging.Value, out StagingRecord? open) && open.IsOpen:
                    staged[item.Object] = (item.Staging.Value, item.Length);
                    break;
            }
        }

        _staged[request.Node] = staged;
        _pendingGc[request.Node] = delete.Count;
        if (delete.Count > 0)
            await _s.HitAsync(FaultStage.F20, FaultSites.GcSweep, Name, DfsOperationKind.Gc, detail: _s.Control.NameOf(request.Node), ct: ct);
        return new InventoryReportResultMessage(delete, unknown, delete.Count > 0 ? s.AppliedIndex : 0, Record.Epoch);
    }

    private void RecordHoldings(NodeId node, IReadOnlyCollection<InventoryItem> items)
    {
        var held = new HashSet<ObjectId>(items.Select(i => i.Object));
        foreach ((ObjectId obj, List<NodeId> holders) in _locations.ToArray())
        {
            if (!held.Contains(obj))
            {
                holders.Remove(node);
                if (holders.Count == 0)
                    _locations.Remove(obj);
            }
        }

        foreach (ObjectId obj in held)
        {
            if (!_locations.TryGetValue(obj, out List<NodeId>? holders))
                _locations[obj] = holders = [];
            holders.Remove(node);
            holders.Insert(0, node);
        }
    }

    /// <summary>Replica health of the volume as the leader knows it (receipts at publication plus inventories).</summary>
    public ReplicaHealthData ReplicaHealth()
    {
        VolumeState s = State;
        ControlState control = _s.Control.State;
        var policies = new Dictionary<ObjectId, (DfsStoragePolicy Policy, string Pool, long Length, IReadOnlyList<NodeId> Holders)>();
        foreach (InodeRecord inode in s.Inodes.Values)
        {
            (DfsStoragePolicy policy, string pool) = PolicyOf(inode.PolicyName);
            foreach (StreamRecord stream in inode.Streams.Values)
            {
                if (stream.IsInline)
                    continue;
                foreach (ExtentRef e in stream.Extents)
                    policies.TryAdd(e.Object, (policy, pool, e.ObjectLength, LocationsOf(e)));
            }
        }

        long under = 0, zones = 0, missing = 0, violations = 0, backlog = 0;
        foreach ((ObjectId _, (DfsStoragePolicy policy, string pool, long length, IReadOnlyList<NodeId> holders)) in policies)
        {
            var poolMembers = control.PoolMembers(pool).Select(m => m.Id).ToHashSet();
            var labels = holders.Select(h => control.FindMember(h)).Where(m => m is { IsLive: true }).Select(m => m!.Labels).ToArray();
            (int copies, int distinctZones) = Tedd.Tdfs.Data.Placement.CountDomains(labels);
            if (copies == 0)
                missing++;
            if (copies < policy.TargetCopies)
            {
                under++;
                backlog += length;
            }

            if (distinctZones < policy.TargetDistinctZones)
                zones++;
            violations += holders.Count(h => !poolMembers.Contains(h));
        }

        return new ReplicaHealthData(policies.Count, under, zones, missing, violations, backlog, _pendingGc.Values.Sum(), StagedBytes(s), _sm.PinnedBytes);
    }

    /// <summary>
    /// Bytes of distinct objects stored under open staging sessions and not (yet) referenced by any root, as the nodes'
    /// latest inventories report them.
    /// </summary>
    private long StagedBytes(VolumeState s)
    {
        if (_staged.Count == 0)
            return 0;
        HashSet<ObjectId> reachable = VolumeStateMachine.Reachable(s);
        var objects = new Dictionary<ObjectId, long>();
        foreach (Dictionary<ObjectId, (ulong Staging, long Length)> perNode in _staged.Values)
        {
            foreach ((ObjectId obj, (ulong staging, long length)) in perNode)
            {
                if (reachable.Contains(obj) || !s.Staging.TryGetValue(staging, out StagingRecord? record) || !record.IsOpen)
                    continue;
                objects[obj] = Math.Max(objects.GetValueOrDefault(obj), length);
            }
        }

        return objects.Values.Sum();
    }

    // ------------------------------------------------------------------------------------------------- admin operations

    /// <summary><c>AdminOp.VolumeStatus</c> (VolumeAdmin): group status, last commit and replica health.</summary>
    public async Task<AdminResultMessage> VolumeStatusAsync(RpcContext context, AdminRequestMessage request)
    {
        RequireVolumeAdmin(context.Principal ?? string.Empty);
        ulong applied = await ReadBarrierAsync(context.Cancellation);
        var body = new AdminVolumeStatusBody(_s.Control.ToVolumeEntry(Record), Token(applied), _s.Admin.GroupStatus(Group, Name), ReplicaHealth(), []);
        return AdminResultMessage.Create(request.Op, body);
    }

    /// <summary><c>AdminOp.ReplicaHealth</c> (VolumeAdmin).</summary>
    public async Task<AdminResultMessage> ReplicaHealthAsync(RpcContext context, AdminRequestMessage request)
    {
        RequireVolumeAdmin(context.Principal ?? string.Empty);
        await ReadBarrierAsync(context.Cancellation);
        return AdminResultMessage.Create(request.Op, ReplicaHealth());
    }

    /// <summary>
    /// <c>AdminOp.PolicyMigrationStatus</c> (VolumeAdmin): the migration target and the referenced objects whose known copies
    /// do not meet it yet; effective once the control record no longer lists a migration target.
    /// </summary>
    public async Task<AdminResultMessage> PolicyMigrationStatusAsync(RpcContext context, AdminRequestMessage request)
    {
        RequireVolumeAdmin(context.Principal ?? string.Empty);
        await ReadBarrierAsync(context.Cancellation);
        VolumeRecord record = Record;
        DfsStoragePolicy target = record.MigrationTarget ?? record.DefaultPolicy;
        ControlState control = _s.Control.State;
        long pending = 0;
        if (record.MigrationTarget is not null)
        {
            var seen = new HashSet<ObjectId>();
            foreach (InodeRecord inode in State.Inodes.Values)
            {
                if (inode.PolicyName.Length > 0 && record.NamedPolicies.ContainsKey(inode.PolicyName))
                    continue;
                foreach (StreamRecord stream in inode.Streams.Values)
                {
                    if (stream.IsInline)
                        continue;
                    foreach (ExtentRef e in stream.Extents)
                    {
                        if (!seen.Add(e.Object))
                            continue;
                        var labels = LocationsOf(e).Select(h => control.FindMember(h)).Where(m => m is { IsLive: true }).Select(m => m!.Labels).ToArray();
                        (int copies, int zones) = Tedd.Tdfs.Data.Placement.CountDomains(labels);
                        if (copies < target.TargetCopies || zones < target.TargetDistinctZones)
                            pending++;
                    }
                }
            }
        }

        return AdminResultMessage.Create(request.Op, new AdminPolicyMigrationBody(Name, target, record.MigrationTarget is null, pending));
    }

    private void RequireVolumeAdmin(string principal)
    {
        ControlState control = _s.Control.State;
        if (!control.IsClusterAdmin(principal) && (control.GetPermissions(principal, Record.Id) & DfsPermissions.VolumeAdmin) == 0)
        {
            _s.Control.ReportSecurityEvent("volume:admin", "denied", "volume=" + Name, principal);
            throw new UnauthorizedAccessException($"Principal '{principal}' lacks VolumeAdmin on volume '{Name}'.");
        }
    }

    private sealed class InventoryAccumulator
    {
        public int NextPage { get; set; }

        public List<InventoryItem> Items { get; } = [];
    }
}
