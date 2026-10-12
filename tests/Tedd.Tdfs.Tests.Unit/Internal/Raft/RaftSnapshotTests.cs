using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Testing;
using static Tedd.Tdfs.Tests.Unit.Internal.Raft.RaftTestHelpers;

namespace Tedd.Tdfs.Tests.Unit.Internal.Raft;

/// <summary>Log compaction, chunked InstallSnapshot and restart from snapshots.</summary>
public sealed class RaftSnapshotTests
{
    private static RaftSimulation Compacting(ulong seed, int voters = 3, int learners = 0, int chunk = 32)
        => new(new RaftSimulationOptions { Voters = voters, Learners = learners, Seed = seed, SnapshotThreshold = 10, SnapshotChunkBytes = chunk });

    [Fact]
    public void LaggingFollower_IsRepairedWithChunkedSnapshot()
    {
        var sim = Compacting(401);
        int leader = WaitForStableLeader(sim);
        int lagging = (leader + 1) % 3;
        sim.Crash(lagging);
        for (int i = 0; i < 60; i++)
            ProposeAtLeader(sim, Payload($"entry-{i:D3}"));
        WaitForApplied(sim, [.. All(sim).Where(i => i != lagging)], 60);
        sim.RunTicks(2);
        Assert.True(sim.PersistedLog(leader).FirstIndex > sim.PersistedLog(lagging).LastIndex + 1, "the leader should have compacted past the lagging node");

        sim.Restart(lagging);
        WaitForApplied(sim, All(sim), 60);
        Assert.Contains(sim.Trace, t => t.Contains($"n{lagging} installed snapshot", StringComparison.Ordinal));
        Assert.Equal(sim.AppliedLog(leader).Select(AsText), sim.AppliedLog(lagging).Select(AsText));
        Assert.True(sim.PersistedLog(lagging).Snapshot.Index > 0);

        // It keeps replicating normally after the snapshot.
        ProposeAtLeader(sim, Payload("after"));
        WaitForApplied(sim, All(sim), 61);
        AssertSafe(sim);
    }

    [Fact]
    public void SnapshotTransfer_SurvivesDropsDuplicatesAndReordering()
    {
        var sim = Compacting(403, voters: 5, chunk: 16);
        int leader = WaitForStableLeader(sim);
        int lagging = (leader + 2) % 5;
        sim.Isolate(lagging);
        for (int i = 0; i < 80; i++)
            ProposeAtLeader(sim, Payload($"payload-{i:D3}-xxxxxxxx"));
        WaitForApplied(sim, [.. All(sim).Where(i => i != lagging)], 80);
        sim.RunTicks(2);
        Assert.True(sim.PersistedLog(sim.LeaderIndex!.Value).FirstIndex > 2, "the leader should have compacted");
        sim.DropProbability(0.2);
        sim.DuplicateProbability(0.2);
        sim.ReorderMessages(true);
        sim.Heal();
        WaitForApplied(sim, All(sim), 80, maxTicks: 5000);
        Assert.True(sim.Trace.Any(t => t.Contains($"n{lagging} installed snapshot", StringComparison.Ordinal)),
            $"lagging n{lagging}{Environment.NewLine}{string.Join(Environment.NewLine, sim.Trace.Where(t => !t.Contains("propose", StringComparison.Ordinal)))}");
        Assert.All(All(sim), i => Assert.Equal(sim.AppliedLog(leader).Take(80).Select(AsText), sim.AppliedLog(i).Take(80).Select(AsText)));
        AssertSafe(sim);
    }

    [Fact]
    public void Restart_RebuildsStateFromSnapshotAndLogSuffix()
    {
        var sim = Compacting(405);
        int leader = WaitForStableLeader(sim);
        for (int i = 0; i < 25; i++)
            ProposeAtLeader(sim, Payload($"r{i}"));
        WaitForApplied(sim, All(sim), 25);
        sim.RunTicks(2);
        int node = (leader + 1) % 3;
        Assert.True(sim.PersistedLog(node).Snapshot.Index > 0);
        IReadOnlyList<string> before = [.. sim.AppliedLog(node).Select(AsText)];

        sim.Restart(node);
        Assert.True(sim.Node(node).FirstIndex > 1);
        WaitForApplied(sim, [node], 25);
        Assert.Equal(before, sim.AppliedLog(node).Select(AsText));

        // Every node restarted from compacted logs still converges.
        foreach (int i in All(sim))
            sim.Restart(i);
        ProposeAtLeader(sim, Payload("final"));
        WaitForApplied(sim, All(sim), 26);
        AssertSafe(sim);
    }

    [Fact]
    public void NewLearner_JoinsThroughSnapshot()
    {
        var sim = Compacting(407);
        int leader = WaitForStableLeader(sim);
        for (int i = 0; i < 30; i++)
            ProposeAtLeader(sim, Payload($"j{i}"));
        WaitForApplied(sim, All(sim), 30);
        sim.RunTicks(2);

        int joiner = sim.AddNode();
        leader = sim.LeaderIndex!.Value;
        Assert.True(sim.ProposeConfChange(leader, new RaftConfChange(RaftConfChangeKind.AddLearner, sim.IdOf(joiner))));
        WaitForApplied(sim, [joiner], 30);
        Assert.Contains(sim.Trace, t => t.Contains($"n{joiner} installed snapshot", StringComparison.Ordinal));
        Assert.Equal(RaftRole.Learner, sim.Node(joiner).Role);
        Assert.Equal(sim.Node(leader).ConfState, sim.Node(joiner).ConfState);
        AssertSafe(sim);
    }

    [Fact]
    public void Follower_AssemblesChunks_RejectsGapsAndOversizedSnapshots()
    {
        NodeId[] ids = Ids(3);
        var conf = new RaftConfState(ids, []);
        (RaftNode node, InMemoryRaftLog log) = CreateNode(ids[1], conf, options: new RaftNodeOptions { MaxSnapshotBytes = 10 });
        var meta = new RaftSnapshotMeta(50, 3, conf);
        byte[] data = [1, 2, 3, 4, 5, 6, 7, 8];

        node.Step(new InstallSnapshot(ids[0], ids[1], 3, meta, 0, data.AsMemory(0, 4), Done: false));
        InstallSnapshotResponse ack = Assert.IsType<InstallSnapshotResponse>(Assert.Single(Drain(node, log).SelectMany(r => r.Messages)));
        Assert.False(ack.Reject);
        Assert.Equal(4, ack.NextOffset);

        // A gap is refused with the expected offset; a duplicate is acknowledged idempotently.
        node.Step(new InstallSnapshot(ids[0], ids[1], 3, meta, 6, data.AsMemory(6, 2), Done: true));
        InstallSnapshotResponse gap = Assert.IsType<InstallSnapshotResponse>(Assert.Single(Drain(node, log).SelectMany(r => r.Messages)));
        Assert.True(gap.Reject);
        Assert.Equal(4, gap.NextOffset);
        node.Step(new InstallSnapshot(ids[0], ids[1], 3, meta, 0, data.AsMemory(0, 4), Done: false));
        Assert.Equal(4, Assert.IsType<InstallSnapshotResponse>(Assert.Single(Drain(node, log).SelectMany(r => r.Messages))).NextOffset);

        node.Step(new InstallSnapshot(ids[0], ids[1], 3, meta, 4, data.AsMemory(4, 4), Done: true));
        List<RaftReady> readies = Drain(node, log);
        RaftReady install = Assert.Single(readies, r => r.SnapshotToInstall is not null);
        Assert.Equal(meta, install.SnapshotToInstall);
        Assert.Equal(data, install.SnapshotData.ToArray());
        AppendResponse done = Assert.IsType<AppendResponse>(Assert.Single(readies.SelectMany(r => r.Messages)));
        Assert.Equal(50UL, done.MatchIndex);
        Assert.Equal(50UL, node.CommitIndex);
        Assert.Equal(50UL, node.AppliedIndex);
        Assert.Equal(50UL, log.Snapshot.Index);

        // An oversized transfer is refused outright (NextOffset -1).
        node.Step(new InstallSnapshot(ids[0], ids[1], 3, new RaftSnapshotMeta(90, 3, conf), 0, new byte[11], Done: true));
        InstallSnapshotResponse tooBig = Assert.IsType<InstallSnapshotResponse>(Assert.Single(Drain(node, log).SelectMany(r => r.Messages)));
        Assert.True(tooBig.Reject);
        Assert.Equal(-1, tooBig.NextOffset);
        Assert.Equal(50UL, node.CommitIndex);
    }

    [Fact]
    public void Leader_WithoutSnapshotSource_KeepsProbingAndDoesNotFabricateData()
    {
        // A leader that compacted its log but has no snapshot source cannot repair a lagging follower; it must not send
        // anything else in its place.
        NodeId[] ids = Ids(2);
        var conf = new RaftConfState(ids, []);
        var log = new InMemoryRaftLog(new RaftSnapshotMeta(20, 1, conf));
        (RaftNode leader, _) = CreateNode(ids[0], conf, config: new RaftConfig(ids[0], 10, 20, 2, PreVote: false),
            hardState: new RaftHardState(1, null, 20), log: log);
        for (int t = 0; t < 25 && leader.Role != RaftRole.Candidate; t++)
            leader.Tick();
        Drain(leader, log);
        leader.Step(new VoteResponse(ids[1], ids[0], leader.Term, false, Reject: false));
        List<RaftMessage> sent = [.. Drain(leader, log).SelectMany(r => r.Messages)];
        Assert.Equal(RaftRole.Leader, leader.Role);
        // Follower reports it has nothing: the leader needs entry 1..20 which are compacted.
        leader.Step(new AppendResponse(ids[1], ids[0], leader.Term, Reject: true, MatchIndex: 20, RejectHint: 0));
        sent = [.. Drain(leader, log).SelectMany(r => r.Messages)];
        Assert.DoesNotContain(sent, m => m is InstallSnapshot);
        Assert.DoesNotContain(sent, m => m is AppendEntries ae && ae.PrevLogIndex < 20);
        Assert.Equal(RaftProgressState.Probe, Assert.Single(leader.GetProgress()).State);
    }
}
