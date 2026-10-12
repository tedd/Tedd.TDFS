using System.Text;
using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Unit.Internal.Raft;

/// <summary>Shared helpers for the Raft core and simulation tests.</summary>
internal static class RaftTestHelpers
{
    public const int ElectionBudget = 400;

    public static byte[] Payload(string text) => Encoding.UTF8.GetBytes(text);

    public static string AsText(byte[] data) => Encoding.UTF8.GetString(data);

    /// <summary>Runs until a unique leader exists and returns its index.</summary>
    public static int WaitForLeader(RaftSimulation sim, int maxTicks = ElectionBudget)
    {
        Assert.True(sim.TryRunUntil(() => sim.LeaderIndex is not null, maxTicks),
            $"no leader within {maxTicks} ticks (seed {sim.Seed}).\n{Tail(sim)}");
        return sim.LeaderIndex!.Value;
    }

    /// <summary>Runs until the leader has committed an entry of its term (so proposals and reads are served).</summary>
    public static int WaitForStableLeader(RaftSimulation sim, int maxTicks = ElectionBudget)
    {
        bool ok = sim.TryRunUntil(() =>
        {
            if (sim.LeaderIndex is not int l)
                return false;
            RaftNode n = sim.Node(l);
            return n.CommitIndex >= n.LastIndex && n.CommitIndex > 0 && sim.AppliedIndex(l) == n.CommitIndex;
        }, maxTicks);
        Assert.True(ok, $"no stable leader within {maxTicks} ticks (seed {sim.Seed}).\n{Tail(sim)}");
        return sim.LeaderIndex!.Value;
    }

    /// <summary>Runs until every listed node applied at least <paramref name="count"/> normal entries.</summary>
    public static void WaitForApplied(RaftSimulation sim, IEnumerable<int> nodes, int count, int maxTicks = 2000)
    {
        int[] list = [.. nodes];
        bool ok = sim.TryRunUntil(() => list.All(n => sim.IsUp(n) && sim.AppliedLog(n).Count >= count), maxTicks);
        Assert.True(ok, $"nodes [{string.Join(",", list)}] did not apply {count} entries within {maxTicks} ticks: " +
            $"[{string.Join(",", list.Select(n => sim.AppliedLog(n).Count))}] (seed {sim.Seed}).\n{Tail(sim)}");
    }

    /// <summary>Proposes at the current leader (waiting for one when necessary).</summary>
    public static int ProposeAtLeader(RaftSimulation sim, byte[] data, int maxTicks = ElectionBudget)
    {
        int leader = -1;
        bool ok = sim.TryRunUntil(() =>
        {
            if (sim.LeaderIndex is not int l)
                return false;
            if (!sim.Propose(l, data))
                return false;
            leader = l;
            return true;
        }, maxTicks);
        Assert.True(ok, $"could not propose within {maxTicks} ticks (seed {sim.Seed}).\n{Tail(sim)}");
        return leader;
    }

    public static void AssertSafe(RaftSimulation sim)
    {
        string? violation = sim.CheckSafety();
        Assert.True(violation is null, $"safety violation (seed {sim.Seed}): {violation}\n{Tail(sim)}");
    }

    public static string Tail(RaftSimulation sim, int lines = 60)
        => string.Join("\n", sim.Trace.Skip(Math.Max(0, sim.Trace.Count - lines)));

    public static IEnumerable<int> All(RaftSimulation sim) => Enumerable.Range(0, sim.NodeCount);

    /// <summary>A bare node over an in-memory log for direct state-machine tests.</summary>
    public static (RaftNode Node, InMemoryRaftLog Log) CreateNode(NodeId self, RaftConfState conf, RaftConfig? config = null,
        RaftHardState hardState = default, InMemoryRaftLog? log = null, RaftNodeOptions? options = null, ulong seed = 1)
    {
        log ??= new InMemoryRaftLog();
        var node = new RaftNode(config ?? new RaftConfig(self, 10, 20, 2), hardState, conf, log, new SeededRandom(seed), options);
        return (node, log);
    }

    /// <summary>Handles all readies of a bare node like a host (persist into <paramref name="log"/>), returning them.</summary>
    public static List<RaftReady> Drain(RaftNode node, InMemoryRaftLog log, List<RaftEntry>? applied = null)
    {
        var readies = new List<RaftReady>();
        while (node.HasReady())
        {
            RaftReady rd = node.GetReady();
            if (rd.SnapshotToInstall is { } meta)
                log.ApplySnapshot(meta);
            if (rd.EntriesToPersist.Count > 0)
                log.Append(rd.EntriesToPersist);
            applied?.AddRange(rd.CommittedEntries);
            node.Advance(rd);
            readies.Add(rd);
        }

        return readies;
    }

    public static NodeId[] Ids(int count, ulong seed = 42)
    {
        var random = new SeededRandom(seed);
        return Enumerable.Range(0, count).Select(_ => NodeId.New(random)).ToArray();
    }
}
