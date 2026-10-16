using Tedd.Tdfs.Data;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Control;
using Tedd.Tdfs.Server.Data;
using Tedd.Tdfs.Server.Runtime;

namespace Tedd.Tdfs.Server.Volumes.Service;

// Repair coordinator of the volume leader (ARCHITECTURE §6, RCV-07): converges every referenced object to the
// deterministic HRW targets of its policy within its pool at the current placement epoch.
//
// Per object and pass:
//  * Targets = SelectTargets over the pool's active, admitted members (fullness ignored, so placement is stable).
//  * A target without a counted copy that can receive one gets a ReplicateObject (pulled from up holders, verified,
//    stored under repair admission). A target that is down, refused for capacity or failing is substituted by the next
//    node of the object's HRW order that already holds a copy, else that can receive one.
//  * Copies on other nodes (non-targets, draining, outside the pool, stale substitutes) are excess. They are deleted only
//    once every target holds a counted copy and the targets meet the policy's copies and zones, after each target
//    re-verified its copy (ReplicateObject answers with a fresh receipt only for an intact copy, and repairs a damaged
//    one) — copy before delete, never below the target (REP-016, GC-011). Default-policy objects keep every copy while a
//    default-policy migration is pending (REP-015).
//  * Work is rate-limited (RepairBytesPerSecond) and bounded in concurrency, overall and per receiving node (a slow or
//    congested node is not handed more transfers than it can take); one action per object at a time; a delete whose
//    outcome is unknown, or that was just confirmed, keeps its holder out of every count until an inventory without the copy
//    (or a new incarnation of the holder) settles it.
internal sealed partial class VolumeService
{
    private const int MaxRepairsInFlight = 16;

    /// <summary>Most copies in flight onto one node.</summary>
    private const int MaxCopiesPerTarget = 1;

    /// <summary>Copies in flight per receiving node.</summary>
    private readonly Dictionary<NodeId, int> _copiesTo = [];

    /// <summary>Objects with a copy or verify/delete job in flight.</summary>
    private readonly HashSet<ObjectId> _repairing = [];

    /// <summary>
    /// Excess-copy deletions not settled by an inventory yet: object → holders being (or just) deleted. A holder stays here
    /// after its deletion was confirmed until an inventory without the copy arrives, so an inventory listed before the
    /// deletion (still in flight when the deletion was answered) cannot count the deleted copy again.
    /// </summary>
    private readonly Dictionary<ObjectId, HashSet<NodeId>> _deleting = [];

    /// <summary>(object, holder) pairs of <see cref="_deleting"/> whose deletion the holder confirmed.</summary>
    private readonly HashSet<(ObjectId Object, NodeId Node)> _deleted = [];

    /// <summary>Nodes that refused a repair copy for capacity, until when (leader timestamp).</summary>
    private readonly Dictionary<NodeId, long> _refusedUntil = [];

    /// <summary>(object, node) copies that failed recently, until when.</summary>
    private readonly Dictionary<(ObjectId, NodeId), long> _copyBackoff = [];

    private ByteRateLimiter? _repairRate;
    private int _repairsInFlight;
    private bool _repairPassRunning;
    private bool _repairRequested;
    private bool _repairBacklog;
    private ulong _repairTerm;
    private ulong _usageReportedTerm;

    /// <summary>
    /// This leader's replica summary of <paramref name="term"/> reached the control leader (its <c>ReportUsage</c> was answered).
    /// Excess copies are trimmed only from then on, so the control plane's view of where copies are (draining holders,
    /// removal checks, migration progress) covers a holder before its copies start to disappear.
    /// </summary>
    public void NoteUsageReported(ulong term)
    {
        if (IsLeader && term == Group.Term)
            _usageReportedTerm = term;
    }

    /// <summary>How long a node that refused a repair copy for capacity is not used as a destination.</summary>
    private TimeSpan RefusalBackoff => _s.Timings.InventoryInterval * 8;

    private void ClearRepairState()
    {
        _repairing.Clear();
        _copiesTo.Clear();
        _deleting.Clear();
        _deleted.Clear();
        _refusedUntil.Clear();
        _copyBackoff.Clear();
        _repairsInFlight = 0;
        _repairRequested = false;
        _repairBacklog = false;
        _repairTerm = 0;
    }

    /// <summary>Called when a repair job finished: a pass that stopped at its limits continues right away.</summary>
    private void OnRepairJobDone()
    {
        if (!_repairBacklog || _repairPassRunning || !IsLeader)
            return;
        _repairBacklog = false;
        _s.Jobs.Run("repair-continue", RepairPassAsync);
    }

    private bool IsDeleting(ObjectId obj, NodeId node) => _deleting.TryGetValue(obj, out HashSet<NodeId>? nodes) && nodes.Contains(node);

    private void ForgetPendingDeletes(NodeId node)
    {
        foreach ((ObjectId obj, HashSet<NodeId> nodes) in _deleting.ToArray())
            SettleDelete(obj, node);
    }

    /// <summary>A complete inventory without the object proves a delete on <paramref name="node"/> happened.</summary>
    private void ResolveDeletesByInventory(NodeId node, HashSet<ObjectId> listed)
    {
        foreach ((ObjectId obj, HashSet<NodeId> nodes) in _deleting.ToArray())
        {
            if (!listed.Contains(obj) && nodes.Contains(node))
                SettleDelete(obj, node);
        }
    }

    /// <summary>
    /// A copy proven by a receipt issued after its deletion was confirmed (a later repair copied the object back): the
    /// confirmed deletion no longer describes the holder. A deletion whose outcome is still unknown stays: it may yet
    /// remove the new copy.
    /// </summary>
    private void SettleConfirmedDelete(ObjectId obj, NodeId node)
    {
        if (_deleted.Contains((obj, node)))
            SettleDelete(obj, node);
    }

    private void SettleDelete(ObjectId obj, NodeId node)
    {
        _deleted.Remove((obj, node));
        if (_deleting.TryGetValue(obj, out HashSet<NodeId>? nodes) && nodes.Remove(node) && nodes.Count == 0)
            _deleting.Remove(obj);
    }

    /// <summary>
    /// <c>AdminOp.StartRepair</c> (VolumeAdmin): starts a repair pass now (forgetting capacity refusals and failure
    /// backoffs). Returns once the pass is scheduled.
    /// </summary>
    public async Task<AdminResultMessage> StartRepairAsync(RpcContext context, AdminRequestMessage request)
    {
        RequireVolumeAdmin(context.Principal ?? string.Empty);
        await ReadBarrierAsync(context.Cancellation);
        _refusedUntil.Clear();
        _copyBackoff.Clear();
        _repairRequested = true;
        _s.Jobs.Run("repair-now", RepairPassAsync);
        return AdminResultMessage.Create(request.Op, new AdminEmptyBody());
    }

    /// <summary>One repair pass (periodic job of the leader; schedules work and returns).</summary>
    public async Task RepairPassAsync()
    {
        if (_repairPassRunning || !IsLeader || _leaseTerm != Group.Term)
            return;
        if (_readyTerm != Group.Term)
        {
            using CancellationTokenSource deadline = _s.CreateDeadline();
            try
            {
                await EnsureLeaderReadyAsync(deadline.Token);
            }
            catch (Exception ex) when (ex is DfsException or OperationCanceledException or IOException)
            {
                return;
            }
        }

        if (!IsLeader || _readyTerm != Group.Term || (!ReplicaKnowledgeReady && !_repairRequested) || Record.State == WireVolumeState.Restoring)
            return;
        _repairPassRunning = true;
        try
        {
            _repairRequested = false;
            if (_repairTerm != Group.Term)
            {
                _repairTerm = Group.Term;
                _repairRate = new ByteRateLimiter(_s, _s.Timings.RepairBytesPerSecond);
            }

            PlanRepairs();
        }
        finally
        {
            _repairPassRunning = false;
        }
    }

    private void PlanRepairs()
    {
        ControlState control = _s.Control.State;
        long now = _s.Time.GetTimestamp();
        bool migrating = Record.MigrationTarget is not null;
        ByteRateLimiter rate = _repairRate!;
        var idealNodes = new Dictionary<string, IReadOnlyList<PlacementNode>>(StringComparer.Ordinal);
        PruneBackoffs(now);

        foreach (ObjectPlan plan in Plans().Objects.Values)
        {
            if (_repairsInFlight >= MaxRepairsInFlight || rate.Available <= 0)
            {
                _repairBacklog = true;
                return;
            }

            ObjectId obj = plan.Object;
            if (_repairing.Contains(obj))
                continue;
            List<NodeId> up = UpHolders(obj, control, now);
            if (up.Count == 0)
                continue; // nothing to copy from (missing, or every holder down)

            if (!idealNodes.TryGetValue(plan.Pool, out IReadOnlyList<PlacementNode>? nodes))
                idealNodes[plan.Pool] = nodes = IdealNodes(control, plan.Pool);
            ulong epoch = control.PoolEpoch(plan.Pool);
            IReadOnlyList<PlacementNode> targets = Placement.SelectTargets(obj, epoch, plan.Policy, nodes);
            var desired = new List<NodeId>(targets.Count);
            var copyTo = new List<NodeId>();
            IReadOnlyList<PlacementNode>? order = null;
            foreach (PlacementNode target in targets)
            {
                if (up.Contains(target.Id))
                {
                    desired.Add(target.Id);
                    continue;
                }

                if (CanReceive(obj, target.Id, control, now))
                {
                    desired.Add(target.Id);
                    copyTo.Add(target.Id);
                    continue;
                }

                // Substitute: the next node of the object's order that already holds a copy, else one that can take it.
                order ??= Placement.FallbackOrder(obj, epoch, nodes);
                PlacementNode? substitute = order.FirstOrDefault(n => !targets.Contains(n) && !desired.Contains(n.Id) && up.Contains(n.Id))
                    ?? order.FirstOrDefault(n => !targets.Contains(n) && !desired.Contains(n.Id) && CanReceive(obj, n.Id, control, now));
                if (substitute is null)
                    continue;
                desired.Add(substitute.Id);
                if (!up.Contains(substitute.Id))
                    copyTo.Add(substitute.Id);
            }

            if (copyTo.Count > 0)
            {
                NodeId[] sources = [.. desired.Where(up.Contains), .. up.Where(n => !desired.Contains(n))];
                foreach (NodeId target in copyTo)
                {
                    if (_repairsInFlight >= MaxRepairsInFlight)
                    {
                        _repairBacklog = true;
                        return;
                    }

                    if (_copiesTo.GetValueOrDefault(target) >= MaxCopiesPerTarget)
                    {
                        _repairBacklog = true; // the target keeps its place; the copy starts once one of its transfers ends
                        continue;
                    }

                    rate.Charge(plan.Length);
                    StartCopy(plan, target, sources, epoch);
                }

                continue;
            }

            List<NodeId> excess = [.. up.Where(n => !desired.Contains(n))];
            if (excess.Count == 0 || (migrating && plan.UsesDefault) || targets.Count == 0 || !targets.All(t => up.Contains(t.Id)))
                continue;
            (int copies, int zones) = DomainsOf(control, targets.Select(t => t.Id));
            if (copies < plan.Policy.TargetCopies || zones < plan.Policy.TargetDistinctZones)
                continue;
            if (_usageReportedTerm != Group.Term)
                continue; // the control leader does not know this term's holders yet (see NoteUsageReported)
            rate.Charge(plan.Length * targets.Count);
            StartExcessDeletion(plan, [.. targets.Select(t => t.Id)], excess, epoch);
        }
    }

    /// <summary>
    /// Placement epoch for client uploads under this volume's staging leases: the epoch of the pool of the volume's default
    /// policy (pool.md: targets are computed with the epoch of the pool's last membership change).
    /// </summary>
    private ulong PlacementEpochForUploads()
    {
        VolumeRecord record = Record;
        return _s.Control.State.PoolEpoch(record.DefaultPolicy.Pool ?? record.StoragePool);
    }

    /// <summary>The pool's members new copies may be placed on: active and admitted (fullness ignored: placement is stable).</summary>
    private static IReadOnlyList<PlacementNode> IdealNodes(ControlState control, string pool)
        => [.. control.PoolMembers(pool).Where(m => m.IsPlacementEligible).Select(m => new PlacementNode(m.Id, m.Name, m.Labels, true, 1))];

    /// <summary>True when <paramref name="node"/> is up and neither refused capacity nor failed this object recently.</summary>
    private bool CanReceive(ObjectId obj, NodeId node, ControlState control, long now)
    {
        if (!IsUp(node, control, now) || IsDeleting(obj, node))
            return false;
        if (_refusedUntil.TryGetValue(node, out long until) && now < until)
            return false;
        return !(_copyBackoff.TryGetValue((obj, node), out long retry) && now < retry);
    }

    private void PruneBackoffs(long now)
    {
        foreach ((NodeId node, long until) in _refusedUntil.ToArray())
        {
            if (now >= until)
                _refusedUntil.Remove(node);
        }

        foreach (((ObjectId, NodeId) key, long until) in _copyBackoff.ToArray())
        {
            if (now >= until)
                _copyBackoff.Remove(key);
        }
    }

    private long After(TimeSpan delay) => _s.Time.GetTimestamp() + (long)(delay.TotalSeconds * _s.Time.TimestampFrequency);

    private void StartCopy(ObjectPlan plan, NodeId target, NodeId[] sources, ulong epoch)
    {
        _repairing.Add(plan.Object);
        _repairsInFlight++;
        _copiesTo[target] = _copiesTo.GetValueOrDefault(target) + 1;
        ulong term = Group.Term;
        _s.Jobs.Run("repair-copy", async () =>
        {
            try
            {
                var message = new ReplicateObjectMessage(Record.Id, plan.Object, plan.Staging, plan.Checksum, plan.Length, ObjectEncodings.Plain, epoch, sources);
                ReplicateObjectResultMessage result = await ReplicateOnAsync(target, message);
                if (IsLeader && Group.Term == term)
                    RecordReceipt(result.Receipt);
            }
            catch (Exception ex) when (ex is DfsException or IOException or TimeoutException or OperationCanceledException or InvalidCastException
                                           or UnauthorizedAccessException or NotSupportedException or ArgumentException)
            {
                if (IsLeader && Group.Term == term)
                    NoteCopyFailure(plan.Object, target, ex);
            }
            finally
            {
                if (Group.Term == term && _repairTerm == term)
                {
                    _repairing.Remove(plan.Object);
                    _repairsInFlight = Math.Max(0, _repairsInFlight - 1);
                    if (_copiesTo.GetValueOrDefault(target) <= 1)
                        _copiesTo.Remove(target);
                    else
                        _copiesTo[target]--;
                    OnRepairJobDone();
                }
            }
        });
    }

    private void NoteCopyFailure(ObjectId obj, NodeId target, Exception ex)
    {
        if (ex is DfsException { ErrorCode: DfsErrorCode.CapacityExhausted } || ex is DfsCapacityException)
        {
            _refusedUntil[target] = After(RefusalBackoff);
            _s.Debug("repair", NodeServices.Inv($"Volume {Name}: {_s.Control.NameOf(target)} refused a repair copy for capacity; using other nodes for {RefusalBackoff}."));
            return;
        }

        _copyBackoff[(obj, target)] = After(LivenessWindow);
        _s.Debug("repair", NodeServices.Inv($"Volume {Name}: repairing {obj} onto {_s.Control.NameOf(target)} failed: {ex.GetType().Name}: {ex.Message}"));
    }

    /// <summary>Asks <paramref name="node"/> to hold a verified copy (locally without an RPC when it is this node).</summary>
    private async Task<ReplicateObjectResultMessage> ReplicateOnAsync(NodeId node, ReplicateObjectMessage message)
    {
        TimeSpan timeout = _s.Peers.DefaultCallTimeout * 4;
        using CancellationTokenSource deadline = _s.CreateDeadline(timeout);
        if (node == _s.NodeId)
        {
            RepairWorker worker = _s.Maintenance?.Repair ?? throw new DfsException(DfsErrorCode.Unsupported, "Repair is not available on this node.");
            return await worker.ReplicateAsync(message, deadline.Token);
        }

        return (ReplicateObjectResultMessage)await _s.Peers.CallAsync(node, message, timeout, deadline.Token);
    }

    /// <summary>
    /// Re-verifies every target's copy, then, while the decision still holds (<see cref="StillExcess"/>), deletes the excess
    /// copies one holder at a time. A holder stays out of every count from the moment its deletion is sent until an
    /// inventory without the copy (or the holder's next incarnation) settles it: a delete held at its fault point may
    /// complete late, and an inventory listed before a confirmed delete must not count the copy again.
    /// </summary>
    private void StartExcessDeletion(ObjectPlan plan, NodeId[] keep, List<NodeId> excess, ulong epoch)
    {
        _repairing.Add(plan.Object);
        _repairsInFlight++;
        ulong term = Group.Term;
        _s.Jobs.Run("repair-trim", async () =>
        {
            try
            {
                NodeId[] sources = [.. keep, .. excess];
                foreach (NodeId target in keep)
                {
                    var message = new ReplicateObjectMessage(Record.Id, plan.Object, plan.Staging, plan.Checksum, plan.Length, ObjectEncodings.Plain, epoch,
                        [.. sources.Where(s => s != target)]);
                    ReplicateObjectResultMessage result = await ReplicateOnAsync(target, message);
                    if (!IsLeader || Group.Term != term)
                        return;
                    RecordReceipt(result.Receipt);
                }

                foreach (NodeId holder in excess)
                {
                    // The decision was taken before the targets were re-verified (which may have taken a while): it still
                    // has to hold now, for this holder (copy-before-delete, REP-015/016).
                    if (!IsLeader || Group.Term != term || !StillExcess(plan, keep, holder, epoch))
                        return;
                    if (!_holdings.TryGetValue(plan.Object, out Dictionary<NodeId, Holding>? holders) || !holders.TryGetValue(holder, out Holding h))
                        continue;
                    if (!_deleting.TryGetValue(plan.Object, out HashSet<NodeId>? pending))
                        _deleting[plan.Object] = pending = [];
                    pending.Add(holder);
                    var delete = new DeleteObjectsMessage(Record.Id, Record.Epoch, _sm.State.AppliedIndex, [new ObjectStagingRef(plan.Object, h.Staging)], true);
                    DeleteObjectsResultMessage result;
                    try
                    {
                        result = await DeleteOnAsync(holder, delete);
                    }
                    catch (DfsException ex) when (ex.ErrorCode is DfsErrorCode.EpochMismatch or DfsErrorCode.StaleIncarnation or DfsErrorCode.AccessDenied)
                    {
                        // Refused before anything was deleted.
                        if (Group.Term == term)
                            SettleDelete(plan.Object, holder);
                        continue;
                    }

                    if (result.Deleted == 0)
                    {
                        // Nothing matched (already gone, or held under another staging session): the next inventory tells.
                        if (Group.Term == term)
                            SettleDelete(plan.Object, holder);
                        continue;
                    }

                    // Deleted: the holder stays out of every count until an inventory without the copy settles it.
                    if (IsLeader && Group.Term == term)
                    {
                        _deleted.Add((plan.Object, holder));
                        RemoveHolding(plan.Object, holder);
                    }
                }
            }
            catch (Exception ex) when (ex is DfsException or IOException or TimeoutException or OperationCanceledException or InvalidCastException
                                           or UnauthorizedAccessException or NotSupportedException or ArgumentException)
            {
                _s.Debug("repair", NodeServices.Inv($"Volume {Name}: trimming excess copies of {plan.Object} stopped: {ex.GetType().Name}: {ex.Message}"));
            }
            finally
            {
                if (Group.Term == term && _repairTerm == term)
                {
                    _repairing.Remove(plan.Object);
                    _repairsInFlight = Math.Max(0, _repairsInFlight - 1);
                    OnRepairJobDone();
                }
            }
        });
    }

    /// <summary>
    /// True when deleting <paramref name="holder"/>'s copy is still justified now: the object is still referenced under
    /// the same policy and pool, its placement epoch is unchanged, no default-policy migration that covers it has started,
    /// the holder is not one of the kept targets, and every kept target still holds a counted copy.
    /// </summary>
    private bool StillExcess(ObjectPlan decided, NodeId[] keep, NodeId holder, ulong epoch)
    {
        ControlState control = _s.Control.State;
        if (control.PoolEpoch(decided.Pool) != epoch || keep.Contains(holder))
            return false;
        if (!Plans().Objects.TryGetValue(decided.Object.Value, out ObjectPlan? current)
            || current.Policy != decided.Policy || !string.Equals(current.Pool, decided.Pool, StringComparison.Ordinal))
        {
            return false;
        }

        if (Record.MigrationTarget is not null && current.UsesDefault)
            return false;
        List<NodeId> counted = UpHolders(decided.Object, control, _s.Time.GetTimestamp());
        return keep.All(counted.Contains);
    }

    /// <summary>
    /// Per-file policy change (REP-014): before a file may advertise <paramref name="policy"/>, every object it references
    /// must meet that policy's acknowledgement copies and zones inside <paramref name="pool"/>. Missing copies are made now on
    /// the policy's deterministic targets (verified pulls from the current holders); when that is impossible (a zone is
    /// down) the change is refused with a retryable placement error and the file keeps its current policy.
    /// </summary>
    /// <exception cref="DfsPlacementUnavailableException">An object cannot meet the policy now.</exception>
    private async Task EnsurePolicyMetAsync(State.InodeRecord inode, DfsStoragePolicy policy, string pool, CancellationToken ct)
    {
        var objects = new List<State.ExtentRef>();
        var seen = new HashSet<ObjectId>();
        foreach (State.StreamRecord stream in inode.Streams.Values)
        {
            if (stream.IsInline)
                continue;
            foreach (State.ExtentRef e in stream.Extents)
            {
                if (seen.Add(e.Object))
                    objects.Add(e);
            }
        }

        foreach (State.ExtentRef extent in objects)
        {
            ControlState control = _s.Control.State;
            (int copies, int zones) = CountedIn(extent.Object, control, pool);
            if (copies >= policy.DurableCopiesBeforeAck && zones >= policy.DurableZonesBeforeAck)
                continue;

            ulong epoch = control.PoolEpoch(pool);
            long now = _s.Time.GetTimestamp();
            List<NodeId> up = UpHolders(extent.Object, control, now, includeDeleting: true);
            if (up.Count == 0)
                up.AddRange(LocationsOf(extent).Where(n => control.FindMember(n) is { IsLive: true }));
            IReadOnlyList<PlacementNode> targets = Placement.SelectTargets(extent.Object, epoch, policy, IdealNodes(control, pool));
            foreach (PlacementNode target in targets)
            {
                // A target this leader has not heard from under its registered incarnation (it just restarted) is tried:
                // the copy's receipt is what counts.
                if (up.Contains(target.Id) || IsKnownDown(target.Id, control, now))
                    continue;
                var message = new ReplicateObjectMessage(Record.Id, extent.Object, extent.Staging, extent.Checksum, extent.ObjectLength, ObjectEncodings.Plain,
                    epoch, [.. up]);
                try
                {
                    RecordReceipt((await ReplicateOnAsync(target.Id, message)).Receipt);
                }
                catch (Exception ex) when (ex is DfsException or IOException or TimeoutException or OperationCanceledException or InvalidCastException)
                {
                    ct.ThrowIfCancellationRequested();
                    _s.Debug("repair", NodeServices.Inv($"Volume {Name}: copying {extent.Object} to {target.Name} for a policy change failed: {ex.Message}"));
                }
            }

            (copies, zones) = CountedIn(extent.Object, _s.Control.State, pool);
            if (copies < policy.DurableCopiesBeforeAck || zones < policy.DurableZonesBeforeAck)
            {
                throw new DfsPlacementUnavailableException(
                    $"Object {extent.Object} has {copies} copies in {zones} zones in pool '{pool}'; the new policy requires {policy.DurableCopiesBeforeAck} in {policy.DurableZonesBeforeAck} before it can take effect.",
                    policy.DurableCopiesBeforeAck, copies, policy.DurableZonesBeforeAck, zones);
            }
        }
    }

    /// <summary>
    /// Copies and zones of an object's counted holders that are members of <paramref name="pool"/> (a copy whose excess-replica
    /// deletion is in flight, or that is only assumed from publication receipts, does not count).
    /// </summary>
    private (int Copies, int Zones) CountedIn(ObjectId obj, ControlState control, string pool)
    {
        HashSet<NodeId> members = PoolMemberSet(control, pool);
        return DomainsOf(control, UpHolders(obj, control, _s.Time.GetTimestamp(), includeSeeded: false).Where(members.Contains));
    }

    /// <summary>
    /// Deletes copies on <paramref name="node"/> within the replication deadline. A call that does not answer in time
    /// (a black-holed node, a request held at its fault point) throws: its outcome stays unknown, so an excess-replica
    /// holder stays out of every count until an inventory or a new incarnation resolves it, while the repair slot and the
    /// object are released (one unresponsive holder never stalls repair, RCV-08).
    /// </summary>
    private async Task<DeleteObjectsResultMessage> DeleteOnAsync(NodeId node, DeleteObjectsMessage message)
    {
        TimeSpan timeout = _s.Peers.DefaultCallTimeout * 4;
        using CancellationTokenSource deadline = _s.CreateDeadline(timeout);
        if (node == _s.NodeId)
        {
            // A local deletion that does not finish in time (held at its fault point) is treated like an unanswered remote
            // one: it may still complete later, its outcome stays unknown.
            return await _s.Objects.DeleteCopiesAsync(message, CancellationToken.None).WaitAsync(deadline.Token);
        }

        return (DeleteObjectsResultMessage)await _s.Peers.CallAsync(node, message, timeout, deadline.Token);
    }
}
