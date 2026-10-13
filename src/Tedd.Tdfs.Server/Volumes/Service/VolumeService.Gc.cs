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
    /// <summary>Inventory reports being received (paged), per (node, report id).</summary>
    private readonly Dictionary<(NodeId Node, ulong Report), InventoryAccumulator> _inventories = [];

    /// <summary>Copies classified deletable by the last completed inventory of each node (replica health backlog).</summary>
    private readonly Dictionary<NodeId, int> _pendingGc = [];

    /// <summary>
    /// Unpublished copies (unreachable, under an open staging session) in the last completed inventory of each node: the
    /// staged-bytes accounting of replica health (GC-015).
    /// </summary>
    private readonly Dictionary<NodeId, Dictionary<ObjectId, (ulong Staging, long Length)>> _staged = [];

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
            accumulator = new InventoryAccumulator(_s.Time.GetTimestamp());
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
        RecordInventory(request.Node, request.Incarnation, accumulator.Items, accumulator.StartedAt);
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
        return new InventoryReportResultMessage(delete, unknown, delete.Count > 0 ? s.AppliedIndex : 0, Record.Epoch);
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
        await ProbeDeficitsAsync(context.Cancellation);
        var body = new AdminVolumeStatusBody(_s.Control.ToVolumeEntry(Record), Token(applied), _s.Admin.GroupStatus(Group, Name), ReplicaHealth(), []);
        return AdminResultMessage.Create(request.Op, body);
    }

    /// <summary><c>AdminOp.ReplicaHealth</c> (VolumeAdmin).</summary>
    public async Task<AdminResultMessage> ReplicaHealthAsync(RpcContext context, AdminRequestMessage request)
    {
        RequireVolumeAdmin(context.Principal ?? string.Empty);
        await ReadBarrierAsync(context.Cancellation);
        await ProbeDeficitsAsync(context.Cancellation);
        return AdminResultMessage.Create(request.Op, ReplicaHealth());
    }

    /// <summary>
    /// <c>AdminOp.PolicyMigrationStatus</c> (VolumeAdmin): the migration target and the referenced default-policy objects
    /// whose counted copies do not meet it yet; effective once the control record no longer lists a migration target (the
    /// control leader clears it when this leader reports every such object migrated).
    /// </summary>
    public async Task<AdminResultMessage> PolicyMigrationStatusAsync(RpcContext context, AdminRequestMessage request)
    {
        RequireVolumeAdmin(context.Principal ?? string.Empty);
        await ReadBarrierAsync(context.Cancellation);
        VolumeRecord record = Record;
        DfsStoragePolicy target = record.MigrationTarget ?? record.DefaultPolicy;
        long pending = 0;
        if (record.MigrationTarget is { } migrating)
        {
            await ProbeDeficitsAsync(context.Cancellation);
            pending = Math.Max(PendingMigrationObjects(migrating), ReplicaKnowledgeReady ? 0 : 1);
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

    private sealed class InventoryAccumulator(long startedAt)
    {
        /// <summary>Leader timestamp of the first page (the node listed its copies no later than this).</summary>
        public long StartedAt { get; } = startedAt;

        public int NextPage { get; set; }

        public List<InventoryItem> Items { get; } = [];
    }
}
