using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Runtime;

namespace Tedd.Tdfs.Server.Raft;

/// <summary>
/// Outbound Raft traffic (PROTOCOL.md §8.7): messages are batched per destination node into one-way
/// <see cref="RaftBatchMessage"/>s over authenticated peer connections; snapshot chunks travel as request/response
/// <see cref="RaftSnapshotChunkMessage"/>s (flow control: the leader sends the next chunk only after the answer). Lost
/// sends are reported to the group as unreachable peers; Raft retransmits.
/// </summary>
internal sealed class RaftTransport
{
    private const int MaxBatchMessages = 1024;
    private const long MaxBatchBytes = 8L << 20;
    private const int MaxQueuedMessages = 16 * 1024;

    private readonly NodeServices _s;
    private readonly RaftGroupHost _host;
    private readonly SortedDictionary<NodeId, Outbox> _outboxes = new(Control.NodeIdComparer.Instance);
    private readonly HashSet<(string Group, NodeId Peer)> _snapshotsInFlight = [];

    public RaftTransport(NodeServices services, RaftGroupHost host)
    {
        _s = services;
        _host = host;
    }

    /// <summary>Messages dropped because a destination queue was full.</summary>
    public long Dropped { get; private set; }

    /// <summary>Queues messages produced by <paramref name="group"/> (called after their dependencies were persisted).</summary>
    public void Send(GroupRuntime group, IReadOnlyList<RaftMessage> messages)
    {
        foreach (RaftMessage message in messages)
        {
            if (message.To == _s.NodeId || message.To.IsEmpty)
                continue;
            if (message is InstallSnapshot snapshot)
            {
                _ = SendSnapshotChunkAsync(group, snapshot);
                continue;
            }

            if (!_outboxes.TryGetValue(message.To, out Outbox? box))
            {
                box = new Outbox();
                _outboxes[message.To] = box;
            }

            if (box.Queue.Count >= MaxQueuedMessages)
            {
                Dropped++;
                continue;
            }

            box.Queue.Add(new Pending(group, new RaftEnvelope(group.Name, RaftWireMapper.ToWire(message)), EstimateSize(message)));
            if (!box.Sending)
                _ = FlushAsync(message.To, box);
        }
    }

    /// <summary>Drops queued traffic of a closed group.</summary>
    public void ForgetGroup(string group)
    {
        foreach (Outbox box in _outboxes.Values)
            box.Queue.RemoveAll(p => string.Equals(p.Group.Name, group, StringComparison.Ordinal));
    }

    private async Task FlushAsync(NodeId peer, Outbox box)
    {
        box.Sending = true;
        try
        {
            while (box.Queue.Count > 0 && !_s.IsStopping)
            {
                int take = 0;
                long bytes = 0;
                while (take < box.Queue.Count && take < MaxBatchMessages && (take == 0 || bytes + box.Queue[take].Size <= MaxBatchBytes))
                {
                    bytes += box.Queue[take].Size;
                    take++;
                }

                Pending[] batch = box.Queue.GetRange(0, take).ToArray();
                box.Queue.RemoveRange(0, take);
                var message = new RaftBatchMessage(_s.NodeId, _s.Incarnation, batch.Select(p => p.Envelope).ToArray());

                // Every batch carries this node's data-capability clock so group peers converge on one capability time
                // (Capabilities.ObservePeerClock, PROTOCOL.md §7.3).
                if (_s.Objects is { } objects)
                    message = message.WithCapabilityClock(objects.Capabilities.Clock);
                bool sent = await _s.Peers.TrySendAsync(peer, message);
                if (!sent)
                {
                    foreach (GroupRuntime group in batch.Select(p => p.Group).Distinct())
                        group.ReportUnreachable(peer);
                }
            }
        }
        finally
        {
            box.Sending = false;
        }
    }

    private async Task SendSnapshotChunkAsync(GroupRuntime group, InstallSnapshot snapshot)
    {
        (string, NodeId) key = (group.Name, snapshot.To);
        if (!_snapshotsInFlight.Add(key))
            return; // one chunk in flight per follower; the leader retransmits on the next heartbeat response
        try
        {
            var request = new RaftSnapshotChunkMessage(_s.NodeId, _s.Incarnation, group.Name, snapshot.To, snapshot.Term,
                RaftWireMapper.ToWire(snapshot.Meta), snapshot.Offset, snapshot.Data, snapshot.Done);
            IWireMessage reply = await _s.Peers.CallAsync(snapshot.To, request, _s.Peers.SnapshotCallTimeout);
            if (reply is RaftSnapshotChunkResultMessage result)
            {
                group.Step(new InstallSnapshotResponse(snapshot.To, _s.NodeId, result.Term, result.SnapshotIndex, result.NextOffset, result.Reject));
            }
            else
            {
                group.ReportUnreachable(snapshot.To);
            }
        }
        catch (Exception ex) when (ex is DfsException or IOException or TimeoutException or UnauthorizedAccessException or OperationCanceledException)
        {
            _s.Debug("raft", NodeServices.Inv($"{group.Name}: snapshot chunk at {snapshot.Offset} to {snapshot.To} failed: {ex.Message}"));
            group.ReportUnreachable(snapshot.To);
        }
        finally
        {
            _snapshotsInFlight.Remove(key);
        }
    }

    private static long EstimateSize(RaftMessage message)
    {
        if (message is AppendEntries append)
        {
            long size = 64;
            foreach (RaftEntry e in append.Entries)
                size += 24 + e.Data.Length;
            return size;
        }

        return 64;
    }

    private sealed class Outbox
    {
        public List<Pending> Queue { get; } = [];

        public bool Sending { get; set; }
    }

    private readonly record struct Pending(GroupRuntime Group, RaftEnvelope Envelope, long Size);
}
