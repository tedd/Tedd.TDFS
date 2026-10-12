using Tedd.Tdfs.Consensus;

namespace Tedd.Tdfs.Tests.Unit.Internal.Raft;

/// <summary>
/// Raft nodes with hand-routed messages: every outbound message is queued and only delivered when a test asks for it,
/// so tests can inspect, drop or reorder individual messages.
/// </summary>
internal sealed class ManualCluster
{
    public ManualCluster(int voters, int learners = 0, Func<NodeId, RaftConfig>? config = null, ulong seed = 1, RaftNodeOptions? options = null)
    {
        Ids = RaftTestHelpers.Ids(voters + learners, seed);
        Conf = new RaftConfState(Ids[..voters], Ids[voters..]);
        Nodes = new RaftNode[Ids.Length];
        Logs = new InMemoryRaftLog[Ids.Length];
        Applied = new List<RaftEntry>[Ids.Length];
        for (int i = 0; i < Ids.Length; i++)
        {
            Logs[i] = new InMemoryRaftLog();
            Applied[i] = [];
            Nodes[i] = new RaftNode(config?.Invoke(Ids[i]) ?? new RaftConfig(Ids[i], 10, 20, 2), default, Conf, Logs[i],
                new SeededRandom(seed + (ulong)i), options);
        }
    }

    public NodeId[] Ids { get; }
    public RaftConfState Conf { get; }
    public RaftNode[] Nodes { get; }
    public InMemoryRaftLog[] Logs { get; }
    public List<RaftEntry>[] Applied { get; }
    public List<RaftMessage> Queue { get; } = [];
    public List<RaftReadState>[] Reads { get; } = Enumerable.Range(0, 16).Select(_ => new List<RaftReadState>()).ToArray();

    public int IndexOf(NodeId id) => Array.IndexOf(Ids, id);

    /// <summary>Processes every ready of node <paramref name="i"/> (persist, queue messages, apply).</summary>
    public void Drain(int i)
    {
        while (Nodes[i].HasReady())
        {
            RaftReady rd = Nodes[i].GetReady();
            if (rd.SnapshotToInstall is { } meta)
                Logs[i].ApplySnapshot(meta);
            if (rd.EntriesToPersist.Count > 0)
                Logs[i].Append(rd.EntriesToPersist);
            Queue.AddRange(rd.Messages);
            Applied[i].AddRange(rd.CommittedEntries);
            Reads[i].AddRange(rd.ReadStates);
            Nodes[i].Advance(rd);
        }
    }

    public void DrainAll()
    {
        for (int i = 0; i < Nodes.Length; i++)
            Drain(i);
    }

    /// <summary>
    /// Delivers queued messages accepted by <paramref name="filter"/> (others stay queued) until none are left; messages
    /// rejected by <paramref name="drop"/> are discarded. Returns the number delivered.
    /// </summary>
    public int Deliver(Func<RaftMessage, bool>? filter = null, Func<RaftMessage, bool>? drop = null, int maxRounds = 10_000)
    {
        int delivered = 0;
        for (int round = 0; round < maxRounds; round++)
        {
            DrainAll();
            List<RaftMessage> batch = [.. Queue.Where(m => filter is null || filter(m))];
            if (batch.Count == 0)
                return delivered;
            foreach (RaftMessage m in batch)
                Queue.Remove(m);
            foreach (RaftMessage m in batch)
            {
                if (drop is not null && drop(m))
                    continue;
                int to = IndexOf(m.To);
                if (to < 0)
                    continue;
                Nodes[to].Step(m);
                delivered++;
                Drain(to);
            }
        }

        return delivered;
    }

    /// <summary>Makes node <paramref name="i"/> campaign immediately (as on TimeoutNow) and delivers until settled.</summary>
    public void Elect(int i, Func<RaftMessage, bool>? filter = null)
    {
        if (Ids.Length == 1)
        {
            for (int t = 0; t < 100 && Nodes[i].Role != RaftRole.Leader; t++)
                Nodes[i].Tick();
        }
        else
        {
            NodeId other = Ids[(i + 1) % Ids.Length];
            Nodes[i].Step(new TimeoutNow(other, Ids[i], Math.Max(Nodes[i].Term, 1)));
        }

        Drain(i);
        Deliver(filter);
        Assert.Equal(RaftRole.Leader, Nodes[i].Role);
    }

    /// <summary>Messages currently queued for <paramref name="to"/> of type <typeparamref name="T"/>.</summary>
    public List<T> Pending<T>(int to) where T : RaftMessage
        => Queue.OfType<T>().Where(m => m.To == Ids[to]).ToList();
}
