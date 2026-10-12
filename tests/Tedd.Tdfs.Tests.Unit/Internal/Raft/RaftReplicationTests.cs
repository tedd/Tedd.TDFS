using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Testing;
using static Tedd.Tdfs.Tests.Unit.Internal.Raft.RaftTestHelpers;

namespace Tedd.Tdfs.Tests.Unit.Internal.Raft;

/// <summary>Log replication, commit rules, log repair and message-fault tolerance.</summary>
public sealed class RaftReplicationTests
{
    [Fact]
    public void ThreeVoters_TwoConnectedCommit_OneAloneCannot()
    {
        var sim = new RaftSimulation(3, seed: 101);
        int leader = WaitForStableLeader(sim);
        int f1 = (leader + 1) % 3;
        int f2 = (leader + 2) % 3;

        sim.Partition([leader, f1], [f2]);
        Assert.True(sim.Propose(leader, Payload("with-majority")));
        WaitForApplied(sim, [leader, f1], 1);

        // Now the leader alone: it may accept the proposal locally but never commits it.
        sim.Partition([leader], [f1], [f2]);
        ulong commitBefore = sim.Node(leader).CommitIndex;
        bool accepted = sim.Propose(leader, Payload("alone"));
        Assert.True(accepted);
        sim.RunTicks(200);
        Assert.Equal(commitBefore, sim.Node(leader).CommitIndex);
        Assert.Equal(["with-majority"], sim.AppliedLog(leader).Select(AsText));
        Assert.DoesNotContain(All(sim), i => sim.AppliedLog(i).Select(AsText).Contains("alone"));
        AssertSafe(sim);
    }

    [Fact]
    public void FiveVoters_TwoSurvivorsNeverCommit_ThreeDo()
    {
        var sim = new RaftSimulation(5, seed: 103);
        int leader = WaitForStableLeader(sim);
        int other = (leader + 1) % 5;
        int[] rest = [.. All(sim).Where(i => i != leader && i != other)];
        sim.Partition([leader, other], rest);
        Assert.True(sim.Propose(leader, Payload("minority")));
        sim.RunTicks(300);
        Assert.All(All(sim), i => Assert.DoesNotContain("minority", sim.AppliedLog(i).Select(AsText)));

        // The three-node side elects a leader and commits.
        int majorityLeader = ProposeAtLeader(sim, Payload("majority"));
        Assert.Contains(majorityLeader, rest);
        WaitForApplied(sim, rest, 1);
        Assert.All(rest, i => Assert.Equal(["majority"], sim.AppliedLog(i).Select(AsText)));

        sim.Heal();
        WaitForApplied(sim, All(sim), 1);
        sim.RunTicks(50);
        Assert.All(All(sim), i => Assert.Equal(["majority"], sim.AppliedLog(i).Select(AsText)));
        AssertSafe(sim);
    }

    [Fact]
    public void Follower_RejectsProposals()
    {
        var sim = new RaftSimulation(3, seed: 105);
        int leader = WaitForStableLeader(sim);
        Assert.False(sim.Propose((leader + 1) % 3, Payload("no")));
        Assert.False(sim.Node((leader + 1) % 3).TryPropose(Payload("no"), out ulong index));
        Assert.Equal(0UL, index);
    }

    [Fact]
    public void LogRepair_DivergentSuffixIsReplacedAfterPartitionHeals()
    {
        var sim = new RaftSimulation(5, seed: 107);
        int oldLeader = WaitForStableLeader(sim);
        for (int i = 0; i < 3; i++)
            sim.Propose(oldLeader, Payload($"base{i}"));
        WaitForApplied(sim, All(sim), 3);

        int buddy = (oldLeader + 1) % 5;
        int[] majority = [.. All(sim).Where(i => i != oldLeader && i != buddy)];
        sim.Partition([oldLeader, buddy], majority);
        for (int i = 0; i < 10; i++)
            Assert.True(sim.Propose(oldLeader, Payload($"lost{i}")));
        sim.RunTicks(5);
        Assert.True(sim.PersistedLog(buddy).LastIndex > sim.PersistedLog(majority[0]).LastIndex, "the buddy should hold the divergent suffix");

        // The old leader still believes it leads term 1; wait for the majority side's own leader.
        Assert.True(sim.TryRunUntil(() => majority.Any(i => sim.Node(i).Role == RaftRole.Leader), ElectionBudget), Tail(sim));
        int newLeader = majority.Single(i => sim.Node(i).Role == RaftRole.Leader);
        for (int i = 0; i < 4; i++)
            Assert.True(sim.Propose(newLeader, Payload($"new{i}")));
        WaitForApplied(sim, majority, 7);

        sim.Heal();
        WaitForApplied(sim, All(sim), 7);
        sim.RunTicks(50);
        string[] expected = ["base0", "base1", "base2", "new0", "new1", "new2", "new3"];
        Assert.All(All(sim), i => Assert.Equal(expected, sim.AppliedLog(i).Select(AsText)));
        int leader = sim.LeaderIndex!.Value;
        Assert.All(All(sim), i => Assert.Equal(sim.PersistedLog(leader).LastIndex, sim.PersistedLog(i).LastIndex));
        AssertSafe(sim);
    }

    [Fact]
    public void DuplicatedDroppedReorderedMessages_AppliedExactlyOnceInOrder()
    {
        var sim = new RaftSimulation(3, seed: 109);
        sim.DropProbability(0.1);
        sim.DuplicateProbability(0.3);
        sim.ReorderMessages(true);
        WaitForLeader(sim, 2000);
        var proposed = new List<string>();
        for (int i = 0; i < 60; i++)
        {
            string text = $"op{i}";
            ProposeAtLeader(sim, Payload(text), 2000);
            proposed.Add(text);
            sim.RunTicks(1);
        }

        sim.DropProbability(0);
        sim.DuplicateProbability(0);
        sim.ReorderMessages(false);
        bool ok = sim.TryRunUntil(() => All(sim).All(i => sim.AppliedLog(i).Count == sim.AppliedLog(sim.LeaderIndex ?? 0).Count)
            && sim.LeaderIndex is int l && sim.Node(l).CommitIndex == sim.Node(l).LastIndex, 2000);
        Assert.True(ok, Tail(sim));
        Assert.True(sim.MessagesDuplicated > 0 && sim.MessagesDropped > 0);

        IReadOnlyList<string> applied = sim.AppliedLog(0).Select(AsText).ToArray();
        Assert.Equal(applied.Count, applied.Distinct().Count());
        Assert.All(All(sim), i => Assert.Equal(applied, sim.AppliedLog(i).Select(AsText)));
        // Every committed payload was proposed; proposals lost with a deposed leader may be missing, but never duplicated.
        Assert.All(applied, a => Assert.Contains(a, proposed));
        AssertSafe(sim);
    }

    [Fact]
    public void Leader_CountsReplicasOnlyForCurrentTermEntries()
    {
        // Figure 8 of the Raft paper: an entry from an earlier term that is on a majority is not committed by counting;
        // it becomes committed only together with an entry of the leader's term.
        var c = new ManualCluster(3);
        c.Elect(0);
        Assert.True(c.Nodes[0].TryPropose(Payload("t-old"), out ulong oldIndex));
        c.Drain(0);
        // Replicate to node 1 only and lose the acknowledgement, so the entry stays uncommitted.
        c.Deliver(m => m.To == c.Ids[1] && m is AppendEntries);
        c.Queue.Clear();
        Assert.True(c.Nodes[0].CommitIndex < oldIndex);
        Assert.True(c.Logs[1].LastIndex >= oldIndex);

        // Node 1 becomes leader of a newer term.
        c.Elect(1, m => m is VoteRequest or VoteResponse);
        RaftNode leader = c.Nodes[1];
        ulong noop = leader.LastIndex;
        Assert.True(noop > oldIndex);

        // Replicate to node 2 everything up to the old-term entry but never the new no-op; node 0 stays cut off.
        c.Queue.RemoveAll(m => m.To == c.Ids[0] || m.From == c.Ids[0]);
        for (int round = 0; round < 50; round++)
        {
            c.DrainAll();
            List<RaftMessage> batch = [.. c.Queue];
            c.Queue.Clear();
            if (batch.Count == 0)
                break;
            foreach (RaftMessage m in batch)
            {
                if (m.To == c.Ids[0] || m.From == c.Ids[0])
                    continue;
                RaftMessage msg = m is AppendEntries ae && ae.To == c.Ids[2]
                    ? ae with { Entries = ae.Entries.Where(e => e.Index <= oldIndex).ToArray() }
                    : m;
                c.Nodes[c.IndexOf(msg.To)].Step(msg);
            }
        }

        Assert.Equal(oldIndex, c.Logs[2].LastIndex);
        Assert.True(leader.CommitIndex < oldIndex, "an earlier-term entry must not be committed by counting replicas");

        // Once the current-term no-op is replicated (the leader retransmits after a heartbeat round), both commit.
        for (int t = 0; t < leader.Config.HeartbeatTicks; t++)
            leader.Tick();
        c.Deliver();
        Assert.True(leader.CommitIndex >= noop);
        Assert.Contains(c.Applied[1], e => e.Index == oldIndex && AsText(e.Data.ToArray()) == "t-old");
    }

    [Fact]
    public void ConflictHints_BacktrackLongDivergentLogInFewRoundTrips()
    {
        var c = new ManualCluster(3);
        c.Elect(0);
        c.Deliver();
        // Node 0 appends 200 entries that only it has (its messages are lost).
        for (int i = 0; i < 200; i++)
            c.Nodes[0].TryPropose(Payload($"stale{i}"), out _);
        c.Drain(0);
        c.Queue.Clear();

        // Nodes 1 and 2 move on without node 0 and build a long log of newer terms.
        bool NotNode0(RaftMessage m) => m.To != c.Ids[0] && m.From != c.Ids[0];
        c.Elect(1, NotNode0);
        for (int i = 0; i < 200; i++)
            c.Nodes[1].TryPropose(Payload($"fresh{i}"), out _);
        c.Deliver(NotNode0);
        c.Queue.RemoveAll(m => !NotNode0(m));
        c.Elect(2, NotNode0);
        c.Deliver(NotNode0);
        c.Queue.RemoveAll(m => !NotNode0(m));
        Assert.Equal(c.Nodes[2].LastIndex, c.Nodes[2].CommitIndex);

        // Reconnect node 0 and count the rejections needed to repair it.
        int rejections = 0;
        for (int t = 0; t < c.Nodes[2].Config.HeartbeatTicks; t++)
            c.Nodes[2].Tick();
        c.Drain(2);
        c.Deliver(drop: m =>
        {
            if (m is AppendResponse { Reject: true })
                rejections++;
            return false;
        });
        Assert.InRange(rejections, 1, 3);
        Assert.Equal(c.Logs[2].LastIndex, c.Logs[0].LastIndex);
        Assert.Equal(c.Logs[2].LastTerm, c.Logs[0].LastTerm);
        Assert.Equal(c.Nodes[2].CommitIndex, c.Nodes[0].CommitIndex);
        Assert.DoesNotContain(c.Applied[0], e => AsText(e.Data.ToArray()).StartsWith("stale", StringComparison.Ordinal));
        Assert.Equal(200, c.Applied[0].Count(e => AsText(e.Data.ToArray()).StartsWith("fresh", StringComparison.Ordinal)));
    }

    [Fact]
    public void FlowControl_LimitsInflightAppendsAndEntriesPerMessage()
    {
        var c = new ManualCluster(3, config: id => new RaftConfig(id, 10, 20, 2, MaxEntriesPerAppend: 3, MaxInflightAppends: 2));
        c.Elect(0);
        c.Deliver(); // followers are now in Replicate state
        for (int i = 0; i < 30; i++)
            Assert.True(c.Nodes[0].TryPropose(Payload($"p{i}"), out _));
        c.Drain(0);

        List<AppendEntries> toNode1 = c.Pending<AppendEntries>(1);
        Assert.Equal(2, toNode1.Count);
        Assert.All(toNode1, ae => Assert.InRange(ae.Entries.Count, 1, 3));

        // Acknowledgements open the window again until everything is replicated.
        c.Deliver();
        Assert.Equal(c.Nodes[0].LastIndex, c.Logs[1].LastIndex);
        Assert.Equal(c.Nodes[0].LastIndex, c.Nodes[0].CommitIndex);
        Assert.Equal(30, c.Applied[0].Count(e => e.Kind == RaftEntryKind.Normal));
    }

    [Fact]
    public void LostAppends_AreRetransmittedAfterHeartbeat()
    {
        var c = new ManualCluster(3, config: id => new RaftConfig(id, 10, 20, 2, MaxInflightAppends: 2));
        c.Elect(0);
        c.Deliver();
        for (int i = 0; i < 10; i++)
            c.Nodes[0].TryPropose(Payload($"p{i}"), out _);
        c.Drain(0);
        c.Queue.Clear(); // every append lost

        for (int t = 0; t < 10; t++)
        {
            c.Nodes[0].Tick();
            c.Drain(0);
            c.Deliver();
        }

        Assert.Equal(c.Nodes[0].LastIndex, c.Logs[1].LastIndex);
        Assert.Equal(c.Nodes[0].LastIndex, c.Logs[2].LastIndex);
        Assert.Equal(c.Nodes[0].LastIndex, c.Nodes[0].CommitIndex);
    }

    [Fact]
    public void LeaderCrashWithUncommittedSuffix_SuffixNeverApplied()
    {
        // CON-006/CON-007: a leader's locally appended entries are not success until a quorum commits them.
        var sim = new RaftSimulation(3, seed: 111);
        int leader = WaitForStableLeader(sim);
        sim.Isolate(leader);
        Assert.True(sim.Propose(leader, Payload("local-only")));
        sim.RunTicks(3);
        Assert.Contains(sim.PersistedLog(leader).Entries(sim.PersistedLog(leader).FirstIndex, sim.PersistedLog(leader).LastIndex + 1, 0),
            e => e.Kind == RaftEntryKind.Normal && AsText(e.Data.ToArray()) == "local-only");
        sim.Crash(leader);
        ProposeAtLeader(sim, Payload("committed"));
        sim.Heal();
        sim.Restart(leader);
        WaitForApplied(sim, All(sim), 1);
        sim.RunTicks(50);
        Assert.All(All(sim), i => Assert.Equal(["committed"], sim.AppliedLog(i).Select(AsText)));
        AssertSafe(sim);
    }

    [Fact]
    public void CrashPoints_DuringReadyProcessing_PreserveSafety()
    {
        foreach (RaftCrashPoint point in Enum.GetValues<RaftCrashPoint>())
        {
            var sim = new RaftSimulation(3, seed: 113 + (ulong)point);
            int leader = WaitForStableLeader(sim);
            int follower = (leader + 1) % 3;
            sim.ArmCrash(follower, point);
            ProposeAtLeader(sim, Payload("a"));
            sim.RunTicks(5);
            Assert.False(sim.IsUp(follower));
            ProposeAtLeader(sim, Payload("b"));
            sim.Restart(follower);
            WaitForApplied(sim, All(sim), 2);
            Assert.All(All(sim), i => Assert.Equal(["a", "b"], sim.AppliedLog(i).Select(AsText)));
            AssertSafe(sim);

            // Leader crashes after persisting a proposal but before sending it.
            leader = sim.LeaderIndex!.Value;
            sim.ArmCrash(leader, point);
            sim.Propose(leader, Payload("c"));
            Assert.False(sim.IsUp(leader));
            sim.RunTicks(100);
            sim.Restart(leader);
            ProposeAtLeader(sim, Payload("d"));
            WaitForApplied(sim, All(sim), 3);
            sim.RunTicks(50);
            IReadOnlyList<string> log0 = sim.AppliedLog(0).Select(AsText).ToArray();
            Assert.All(All(sim), i => Assert.Equal(log0, sim.AppliedLog(i).Select(AsText)));
            Assert.Equal(["a", "b"], log0.Take(2));
            Assert.Equal("d", log0[^1]);
            AssertSafe(sim);
        }
    }
}
