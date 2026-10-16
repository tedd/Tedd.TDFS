using System.Collections.Immutable;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Control;
using Tedd.Tdfs.Server.Runtime;
using Tedd.Tdfs.Server.Volumes.Apply;
using Tedd.Tdfs.Server.Volumes.State;

namespace Tedd.Tdfs.Server.Volumes.Service;

// Leader-pushed garbage collection (ARCHITECTURE §8, PROTOCOL.md DeleteObjects). Objects the current root stopped
// referencing (dereference log) are classified soon after the leader applied their dereference, and the copies this leader
// knows of (validated publication receipts, inventories, repair receipts) are deleted with DeleteObjects instead of waiting
// for each holder's next inventory. The rules are the inventory sweep's: a copy is deletable iff its object is unreachable
// from every root and not protected at the GC watermark, and its staging session is closed, decided at an applied index of
// a ready leader; a node deletes a copy only while its record still carries the decided staging session. The inventory
// sweep stays the complete mechanism (copies this leader does not know of, staging sessions closed without publication,
// objects protected when first considered); pushes only make reclamation prompt. Every push is one GC sweep on the
// receiving node (fault site gc.sweep, hit before anything is deleted).
internal sealed partial class VolumeService
{
    /// <summary>Most copies per <c>DeleteObjects</c> push.</summary>
    private const int GcPushBatch = 512;

    /// <summary>Copies queued for a push, per holder: object → (staging session, decision index).</summary>
    private readonly Dictionary<NodeId, SortedDictionary<ObjectId, (StagingId Staging, ulong DecidedAt)>> _gcPushQueue = [];

    /// <summary>Holders with a push in flight.</summary>
    private readonly HashSet<NodeId> _gcPushSending = [];

    /// <summary>Applied index through which dereferenced objects were considered in this term.</summary>
    private ulong _gcPushedThrough;

    /// <summary>Term of the running push loop (0: none).</summary>
    private ulong _gcPushLoopTerm;

    /// <summary>Leader timestamp of the last push pass (null: none yet).</summary>
    private long? _lastGcPush;

    /// <summary>Minimum spacing of push passes: dereferences applied meanwhile are classified together.</summary>
    private TimeSpan GcPushPace => _s.Timings.GcInterval / 8;

    private void ClearGcPushState()
    {
        _gcPushQueue.Clear();
        _gcPushSending.Clear();
        _gcPushedThrough = 0;
        _gcPushLoopTerm = 0;
    }

    /// <summary>
    /// Called after the group applied entries: on a ready leader whose dereference log grew, starts the push loop (one per
    /// term; it paces itself and stops when nothing new is dereferenced).
    /// </summary>
    internal void OnStateApplied()
    {
        if (!IsLeader)
            return;
        ulong term = Group.Term;
        if (_gcPushLoopTerm == term || !CanPushGarbage(term))
            return;
        _gcPushLoopTerm = term;

        // Posted: the loop never runs inside the group's apply notification.
        _s.Loop.Post(() => _s.Jobs.Run("gc-push", () => GcPushLoopAsync(term)));
    }

    /// <summary>True when this ready leader of <paramref name="term"/> has dereferences it has not considered yet.</summary>
    private bool CanPushGarbage(ulong term)
    {
        if (!IsLeader || Group.Term != term || _readyTerm != term || _s.IsStopping)
            return false;
        ImmutableList<DerefEntry> log = _sm.State.DerefLog;
        return log.Count > 0 && log[^1].Index > _gcPushedThrough;
    }

    private async Task GcPushLoopAsync(ulong term)
    {
        try
        {
            while (CanPushGarbage(term))
            {
                // Waits out the rest of the pace once, then pushes (a timer may fire a hair early; no busy re-check).
                if (_lastGcPush is { } last && GcPushPace - _s.Time.GetElapsedTime(last) is { } wait && wait > TimeSpan.Zero)
                {
                    await _s.Jobs.DelayAsync(wait);
                    if (!CanPushGarbage(term))
                        break;
                }

                _lastGcPush = _s.Time.GetTimestamp();
                PushGarbage(term);
            }
        }
        finally
        {
            // No await separates the last check from the reset: a dereference applied later starts a new loop.
            if (_gcPushLoopTerm == term)
                _gcPushLoopTerm = 0;
        }
    }

    /// <summary>
    /// Classifies the known copies of the objects dereferenced since the last pass at the applied index and queues the
    /// deletable ones per holder.
    /// </summary>
    private void PushGarbage(ulong term)
    {
        VolumeState s = _sm.State;
        ulong from = _gcPushedThrough;
        _gcPushedThrough = s.AppliedIndex;
        var candidates = new SortedSet<ObjectId>(ObjectIdComparer.Instance);
        for (int i = s.DerefLog.Count - 1; i >= 0 && s.DerefLog[i].Index > from; i--)
        {
            if (_holdings.ContainsKey(s.DerefLog[i].Object))
                candidates.Add(s.DerefLog[i].Object);
        }

        if (candidates.Count == 0)
            return;
        HashSet<ObjectId> reachable = VolumeStateMachine.Reachable(s, _s.Mutations.IgnoreSnapshotPins);
        HashSet<ObjectId> protectedObjects = VolumeStateMachine.Protected(s, VolumeStateMachine.Watermark(s, _s.Mutations.IgnoreSnapshotPins));
        ControlState control = _s.Control.State;
        var holders = new SortedSet<NodeId>(Comparer<NodeId>.Create(static (a, b) => a.Value.CompareTo(b.Value)));
        foreach (ObjectId obj in candidates)
        {
            if (!_holdings.TryGetValue(obj, out Dictionary<NodeId, Holding>? copies))
                continue;
            foreach ((NodeId node, Holding h) in copies)
            {
                if (control.FindMember(node) is not { IsLive: true }
                    || VolumeStateMachine.Classify(s, obj, h.Staging.Value, reachable, protectedObjects) != ObjectClass.Deletable)
                {
                    continue;
                }

                if (!_gcPushQueue.TryGetValue(node, out SortedDictionary<ObjectId, (StagingId, ulong)>? queue))
                    _gcPushQueue[node] = queue = new SortedDictionary<ObjectId, (StagingId, ulong)>(ObjectIdComparer.Instance);
                queue[obj] = (h.Staging, s.AppliedIndex);
                holders.Add(node);
            }
        }

        foreach (NodeId node in holders)
            StartGcPush(node, term);
    }

    /// <summary>Sends the queued deletions of <paramref name="node"/> (one push in flight per holder).</summary>
    private void StartGcPush(NodeId node, ulong term)
    {
        if (!_gcPushSending.Add(node))
            return;
        _s.Jobs.Run("gc-push-send", async () =>
        {
            try
            {
                while (IsLeader && Group.Term == term && _gcPushQueue.TryGetValue(node, out SortedDictionary<ObjectId, (StagingId Staging, ulong DecidedAt)>? queue))
                {
                    var batch = new List<ObjectStagingRef>(Math.Min(queue.Count, GcPushBatch));
                    ulong decidedAt = 0;
                    foreach ((ObjectId obj, (StagingId staging, ulong at)) in queue)
                    {
                        if (batch.Count == GcPushBatch)
                            break;
                        batch.Add(new ObjectStagingRef(obj, staging));
                        decidedAt = Math.Max(decidedAt, at);
                    }

                    foreach (ObjectStagingRef r in batch)
                        queue.Remove(r.Object);
                    if (queue.Count == 0)
                        _gcPushQueue.Remove(node);
                    await DeleteOnAsync(node, new DeleteObjectsMessage(Record.Id, Record.Epoch, decidedAt, batch, ExcessReplicas: false));
                    if (!IsLeader || Group.Term != term)
                        return;
                    foreach (ObjectStagingRef r in batch)
                    {
                        if (_holdings.TryGetValue(r.Object, out Dictionary<NodeId, Holding>? copies) && copies.TryGetValue(node, out Holding h) && h.Staging == r.Staging)
                            RemoveHolding(r.Object, node);
                    }
                }
            }
            catch (Exception ex) when (ex is DfsException or IOException or TimeoutException or OperationCanceledException or InvalidCastException
                                           or UnauthorizedAccessException or NotSupportedException or ArgumentException)
            {
                // The holder's next inventory sweep collects what this push did not.
                if (Group.Term == term)
                    _gcPushQueue.Remove(node);
                _s.Debug("gc", NodeServices.Inv($"Volume {Name}: pushing deletions to {node} stopped: {ex.GetType().Name}: {ex.Message}"));
            }
            finally
            {
                if (Group.Term == term)
                    _gcPushSending.Remove(node);
            }
        });
    }

    /// <summary>
    /// Records the holders of an object's validated publication receipts as known copies: each receipt proves a durable
    /// copy under its holder's registered incarnation, so the copy can be collected promptly once dereferenced.
    /// </summary>
    private void RecordPublishedCopies(ObjectId obj, StagingId staging, long length, UInt128 checksum, IEnumerable<NodeId> holders)
    {
        if (!IsLeader)
            return;
        ControlState control = _s.Control.State;
        long now = _s.Time.GetTimestamp();
        foreach (NodeId node in holders)
        {
            if (control.FindMember(node) is { IsLive: true } member)
                AddHolding(obj, node, new Holding(member.Incarnation, staging, length, checksum, now));
        }
    }
}
