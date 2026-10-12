using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Testing;
using static Tedd.Tdfs.Tests.Unit.Internal.Raft.RaftTestHelpers;

namespace Tedd.Tdfs.Tests.Unit.Internal.Raft;

/// <summary>ReadIndex: quorum confirmation, current-term commit requirement, stale leaders (CON-008/009).</summary>
public sealed class RaftReadIndexTests
{
    [Fact]
    public void Leader_ConfirmsReadAtCommitIndex()
    {
        var sim = new RaftSimulation(3, seed: 201);
        int leader = WaitForStableLeader(sim);
        for (int i = 0; i < 3; i++)
            sim.Propose(leader, Payload($"v{i}"));
        WaitForApplied(sim, All(sim), 3);
        ulong commit = sim.Node(leader).CommitIndex;

        sim.ReadIndex(leader, 42);
        sim.RunTicks(1);
        SimReadState read = Assert.Single(sim.ReadStates(leader));
        Assert.Equal(42UL, read.Context);
        Assert.Equal(commit, read.Index);
        Assert.True(sim.AppliedIndex(leader) >= read.Index);
        AssertSafe(sim);
    }

    [Fact]
    public void Follower_DropsReadIndex()
    {
        var sim = new RaftSimulation(3, seed: 203);
        int leader = WaitForStableLeader(sim);
        int follower = (leader + 1) % 3;
        sim.ReadIndex(follower, 7);
        sim.RunTicks(50);
        Assert.Empty(sim.ReadStates(follower));
        Assert.Empty(sim.ReadStates(leader));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void StaleLeader_CannotConfirmRead(bool checkQuorum)
    {
        // CON-008: an isolated old leader cannot label its stale state as a linearizable read.
        var sim = new RaftSimulation(new RaftSimulationOptions { Voters = 5, Seed = 205, CheckQuorum = checkQuorum });
        int old = WaitForStableLeader(sim);
        sim.Propose(old, Payload("before"));
        WaitForApplied(sim, All(sim), 1);

        sim.Isolate(old);
        int[] others = [.. All(sim).Where(i => i != old)];
        Assert.True(sim.TryRunUntil(() => others.Any(i => sim.Node(i).Role == RaftRole.Leader), ElectionBudget));
        int fresh = others.Single(i => sim.Node(i).Role == RaftRole.Leader);
        sim.Propose(fresh, Payload("after"));
        WaitForApplied(sim, others, 2);

        sim.ReadIndex(old, 99);
        sim.RunTicks(200);
        Assert.Empty(sim.ReadStates(old));

        // After healing the old leader steps down; the pending read is dropped, never confirmed late.
        sim.Heal();
        sim.RunTicks(50);
        Assert.NotEqual(RaftRole.Leader, sim.Node(old).Role);
        Assert.Empty(sim.ReadStates(old));
        Assert.Equal(0, sim.Node(old).PendingReadCount);

        // The new leader serves reads that include the new data.
        sim.ReadIndex(fresh, 100);
        sim.RunTicks(2);
        SimReadState read = Assert.Single(sim.ReadStates(fresh));
        Assert.True(read.Index >= sim.AppliedEntries(fresh).Single(e => AsText(e.Data) == "after").Index);
        AssertSafe(sim);
    }

    [Fact]
    public void NewLeader_DefersReadsUntilItCommitsInItsTerm()
    {
        var c = new ManualCluster(3);
        c.Elect(0);
        c.Deliver();
        Assert.True(c.Nodes[0].TryPropose(Payload("x"), out ulong xIndex));
        c.Deliver();
        Assert.Equal(xIndex, c.Nodes[0].CommitIndex);

        // Node 1 is elected but its no-op is not replicated yet.
        c.Elect(1, m => m is VoteRequest or VoteResponse);
        RaftNode leader = c.Nodes[1];
        Assert.True(leader.CommitIndex < leader.LastIndex);
        leader.ReadIndex(5);
        c.Drain(1);
        Assert.Equal(1, leader.PendingReadCount);
        Assert.DoesNotContain(c.Queue, m => m is Heartbeat { ReadContext: not 0 });
        Assert.Empty(c.Reads[1]);

        c.Deliver();
        RaftReadState read = Assert.Single(c.Reads[1]);
        Assert.Equal(5UL, read.RequestContext);
        Assert.Equal(leader.LastIndex, read.Index); // includes the no-op of the new term (and thus the earlier entry x)
        Assert.True(read.Index > xIndex);
    }

    [Fact]
    public void SingleVoter_ConfirmsReadImmediately()
    {
        var sim = new RaftSimulation(1, seed: 207);
        WaitForStableLeader(sim);
        sim.Propose(0, Payload("a"));
        sim.ReadIndex(0, 1);
        SimReadState read = Assert.Single(sim.ReadStates(0));
        Assert.Equal(sim.Node(0).CommitIndex, read.Index);
    }

    [Fact]
    public void Read_RequiresQuorumOfHeartbeatAcks()
    {
        var c = new ManualCluster(5);
        c.Elect(0);
        c.Deliver();
        c.Nodes[0].ReadIndex(11);
        c.Drain(0);
        List<Heartbeat> beats = [.. c.Queue.OfType<Heartbeat>().Where(h => h.ReadContext != 0)];
        Assert.Equal(4, beats.Count);
        c.Queue.Clear();

        // One acknowledgement (plus the leader) is not a quorum of five.
        c.Nodes[1].Step(beats.Single(b => b.To == c.Ids[1]));
        c.Drain(1);
        c.Deliver(m => m.To == c.Ids[0]);
        Assert.Empty(c.Reads[0]);

        c.Nodes[2].Step(beats.Single(b => b.To == c.Ids[2]));
        c.Drain(2);
        c.Deliver(m => m.To == c.Ids[0]);
        RaftReadState read = Assert.Single(c.Reads[0]);
        Assert.Equal(11UL, read.RequestContext);
        Assert.Equal(c.Nodes[0].CommitIndex, read.Index);
    }

    [Fact]
    public void LaterRoundConfirmation_ReleasesEarlierReadsInOrder()
    {
        var c = new ManualCluster(3);
        c.Elect(0);
        c.Deliver();
        c.Nodes[0].ReadIndex(1);
        c.Drain(0);
        c.Queue.Clear(); // the first round's heartbeats are lost
        c.Nodes[0].ReadIndex(2);
        c.Deliver();
        Assert.Equal([1UL, 2UL], c.Reads[0].Select(r => r.RequestContext));
        Assert.Equal(0, c.Nodes[0].PendingReadCount);
    }

    [Fact]
    public void ReadIndex_NeverBelowCommittedIndex_UnderFaults()
    {
        // The simulation records a violation when a confirmed read index is below the highest index known committed when
        // the read was requested; run reads continuously under message faults and leader changes.
        var sim = new RaftSimulation(new RaftSimulationOptions { Voters = 5, Seed = 209 });
        sim.DropProbability(0.1);
        sim.DuplicateProbability(0.1);
        sim.ReorderMessages(true);
        ulong ctx = 0;
        for (int round = 0; round < 300; round++)
        {
            if (sim.LeaderIndex is int l)
            {
                sim.Propose(l, Payload($"w{round}"));
                sim.ReadIndex(l, ++ctx);
                if (round % 50 == 25)
                    sim.Isolate(l);
            }

            if (round % 50 == 40)
                sim.Heal();
            sim.RunTicks(2);
        }

        Assert.True(All(sim).Sum(i => sim.ReadStates(i).Count) > 50);
        AssertSafe(sim);
    }
}
