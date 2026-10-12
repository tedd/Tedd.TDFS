using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Testing;
using static Tedd.Tdfs.Tests.Unit.Internal.Raft.RaftTestHelpers;

namespace Tedd.Tdfs.Tests.Unit.Internal.Raft;

/// <summary>Single-server membership changes with learners (CON-014/015/016, MOD-009).</summary>
public sealed class RaftMembershipTests
{
    private static RaftConfChange Add(RaftSimulation sim, int node) => new(RaftConfChangeKind.AddLearner, sim.IdOf(node));
    private static RaftConfChange Promote(RaftSimulation sim, int node) => new(RaftConfChangeKind.PromoteLearner, sim.IdOf(node));
    private static RaftConfChange Remove(RaftSimulation sim, int node) => new(RaftConfChangeKind.RemoveNode, sim.IdOf(node));

    private static void WaitCommitted(RaftSimulation sim, int leader)
        => Assert.True(sim.TryRunUntil(() => !sim.Node(leader).HasPendingConfChange, 500), Tail(sim));

    [Fact]
    public void AddLearner_CatchesUp_ButIsNotCountedInQuorum()
    {
        var sim = new RaftSimulation(3, seed: 301);
        int leader = WaitForStableLeader(sim);
        sim.Propose(leader, Payload("a"));
        int learner = sim.AddNode();
        Assert.Equal(RaftRole.Follower, sim.Node(learner).Role);
        Assert.True(sim.ProposeConfChange(leader, Add(sim, learner)));
        WaitCommitted(sim, leader);
        sim.Propose(leader, Payload("b"));
        WaitForApplied(sim, All(sim), 2);
        Assert.Equal(RaftRole.Learner, sim.Node(learner).Role);
        Assert.Equal(["a", "b"], sim.AppliedLog(learner).Select(AsText));
        Assert.Contains(sim.IdOf(learner), sim.Node(leader).ConfState.Learners);
        Assert.Contains(sim.Node(leader).GetProgress(), p => p.Peer == sim.IdOf(learner) && p.IsLearner && p.Match == sim.Node(leader).LastIndex);

        // Leader + learner are not a majority of three voters.
        int[] voters = [.. Enumerable.Range(0, 3).Where(i => i != leader)];
        sim.Partition([leader, learner], voters);
        sim.Propose(leader, Payload("minority"));
        sim.RunTicks(100);
        Assert.DoesNotContain("minority", sim.AppliedLog(leader).Select(AsText));
        AssertSafe(sim);
    }

    [Fact]
    public void PromotedLearner_CountsTowardsQuorum()
    {
        var sim = new RaftSimulation(3, seed: 303, learners: 1);
        int leader = WaitForStableLeader(sim);
        Assert.True(sim.ProposeConfChange(leader, Promote(sim, 3)));
        WaitCommitted(sim, leader);
        Assert.Equal(4, sim.Node(leader).ConfState.Voters.Count);
        Assert.Empty(sim.Node(leader).ConfState.Learners);
        sim.RunTicks(5);
        Assert.Equal(RaftRole.Follower, sim.Node(3).Role);

        // With four voters a quorum is three: leader + one voter is not enough, leader + two (one being the promoted node) is.
        int[] others = [.. Enumerable.Range(0, 3).Where(i => i != leader)];
        sim.Partition([leader, others[0]], [others[1], 3]);
        sim.Propose(leader, Payload("two-of-four"));
        sim.RunTicks(60);
        Assert.All(All(sim), i => Assert.DoesNotContain("two-of-four", sim.AppliedLog(i).Select(AsText)));

        sim.Heal();
        int outsider = others[1];
        int[] majority = [.. All(sim).Where(i => i != outsider)];
        sim.Partition(majority, [outsider]);
        Assert.True(sim.TryRunUntil(() => majority.Any(i => sim.Node(i).Role == RaftRole.Leader), ElectionBudget), Tail(sim));
        int majorityLeader = majority.Single(i => sim.Node(i).Role == RaftRole.Leader);
        Assert.True(sim.TryRunUntil(() => sim.Propose(majorityLeader, Payload("three-of-four")), 50));
        Assert.True(sim.TryRunUntil(() => sim.AppliedLog(3).Select(AsText).Contains("three-of-four"), 300), Tail(sim));
        AssertSafe(sim);
    }

    [Fact]
    public void PromotedLearner_CanBeElected()
    {
        var sim = new RaftSimulation(3, seed: 305, learners: 1);
        int leader = WaitForStableLeader(sim);
        Assert.True(sim.ProposeConfChange(leader, Promote(sim, 3)));
        WaitCommitted(sim, leader);
        sim.RunTicks(5);
        sim.TransferLeadership(leader, 3);
        Assert.True(sim.TryRunUntil(() => sim.LeaderIndex == 3, 200), Tail(sim));
        ProposeAtLeader(sim, Payload("from-promoted"));
        WaitForApplied(sim, All(sim), 1);
        AssertSafe(sim);
    }

    [Fact]
    public void RemoveFollower_ShrinksQuorum_AndRemovedNodeStopsReceiving()
    {
        var sim = new RaftSimulation(3, seed: 307);
        int leader = WaitForStableLeader(sim);
        int removed = (leader + 1) % 3;
        int kept = (leader + 2) % 3;
        Assert.True(sim.ProposeConfChange(leader, Remove(sim, removed)));
        WaitCommitted(sim, leader);
        Assert.Equal(2, sim.Node(leader).ConfState.Voters.Count);
        Assert.DoesNotContain(sim.Node(leader).GetProgress(), p => p.Peer == sim.IdOf(removed));

        ulong removedLast = sim.Node(removed).LastIndex;
        sim.Propose(leader, Payload("after-removal"));
        WaitForApplied(sim, [leader, kept], 1);
        sim.RunTicks(50);
        Assert.Equal(removedLast, sim.Node(removed).LastIndex);

        // Two voters: both are needed.
        sim.Isolate(kept);
        sim.Propose(leader, Payload("alone"));
        sim.RunTicks(100);
        Assert.DoesNotContain("alone", sim.AppliedLog(leader).Select(AsText));
        AssertSafe(sim);
    }

    [Fact]
    public void RemoveLeader_StepsDownAfterCommit_AndNeverCampaignsAgain()
    {
        var sim = new RaftSimulation(3, seed: 309);
        int leader = WaitForStableLeader(sim);
        Assert.True(sim.ProposeConfChange(leader, Remove(sim, leader)));
        Assert.True(sim.TryRunUntil(() => sim.Node(leader).Role != RaftRole.Leader, 200), Tail(sim));
        int next = WaitForLeader(sim);
        Assert.NotEqual(leader, next);
        Assert.DoesNotContain(sim.IdOf(leader), sim.Node(next).ConfState.Voters);
        ulong term = sim.Node(next).Term;

        sim.RunTicks(300);
        Assert.Equal(next, sim.LeaderIndex);
        Assert.Equal(term, sim.Node(next).Term);
        Assert.NotEqual(RaftRole.Candidate, sim.Node(leader).Role);
        Assert.NotEqual(RaftRole.PreCandidate, sim.Node(leader).Role);
        ProposeAtLeader(sim, Payload("post"));
        WaitForApplied(sim, [.. All(sim).Where(i => i != leader)], 1);
        AssertSafe(sim);
    }

    [Fact]
    public void RemovedNode_CannotDisruptHealthyGroup()
    {
        // CON-016: a removed node that never learned of its removal campaigns with ever higher terms (no PreVote); the
        // group ignores it while it has a leader.
        var sim = new RaftSimulation(new RaftSimulationOptions { Voters = 3, Seed = 311, PreVote = false, CheckQuorum = false });
        int leader = WaitForStableLeader(sim);
        int victim = (leader + 1) % 3;
        sim.Isolate(victim);
        Assert.True(sim.ProposeConfChange(leader, Remove(sim, victim)));
        WaitCommitted(sim, leader);
        sim.RunTicks(200);
        Assert.True(sim.Node(victim).Term > sim.Node(leader).Term + 3);
        ulong term = sim.Node(leader).Term;

        sim.Heal();
        sim.RunTicks(300);
        Assert.Equal(leader, sim.LeaderIndex);
        Assert.Equal(term, sim.Node(leader).Term);
        ProposeAtLeader(sim, Payload("still-fine"));
        WaitForApplied(sim, [.. All(sim).Where(i => i != victim)], 1);
        AssertSafe(sim);
    }

    [Fact]
    public void OnlyOneUncommittedConfChangeAtATime()
    {
        var c = new ManualCluster(3, learners: 2);
        c.Elect(0);
        c.Deliver();
        RaftNode leader = c.Nodes[0];
        Assert.True(leader.TryProposeConfChange(new RaftConfChange(RaftConfChangeKind.PromoteLearner, c.Ids[3]), out ulong first));
        Assert.True(leader.HasPendingConfChange);
        Assert.False(leader.TryProposeConfChange(new RaftConfChange(RaftConfChangeKind.PromoteLearner, c.Ids[4]), out ulong second));
        Assert.Equal(0UL, second);
        // Effective on append: the leader already counts the promoted node as a voter.
        Assert.Contains(c.Ids[3], leader.ConfState.Voters);

        c.Deliver();
        Assert.True(leader.CommitIndex >= first);
        Assert.False(leader.HasPendingConfChange);
        Assert.True(leader.TryProposeConfChange(new RaftConfChange(RaftConfChangeKind.PromoteLearner, c.Ids[4]), out _));
        c.Deliver();
        Assert.Equal(5, leader.ConfState.Voters.Count);
        Assert.All(c.Nodes, n => Assert.Equal(leader.ConfState, n.ConfState));
    }

    [Fact]
    public void InvalidConfChanges_AreRefused()
    {
        var c = new ManualCluster(2, learners: 1);
        c.Elect(0);
        c.Deliver();
        RaftNode leader = c.Nodes[0];
        NodeId stranger = Ids(1, seed: 999)[0];
        Assert.False(leader.TryProposeConfChange(new RaftConfChange(RaftConfChangeKind.AddLearner, c.Ids[1]), out _));
        Assert.False(leader.TryProposeConfChange(new RaftConfChange(RaftConfChangeKind.PromoteLearner, c.Ids[1]), out _));
        Assert.False(leader.TryProposeConfChange(new RaftConfChange(RaftConfChangeKind.PromoteLearner, stranger), out _));
        Assert.False(leader.TryProposeConfChange(new RaftConfChange(RaftConfChangeKind.RemoveNode, stranger), out _));
        Assert.False(leader.TryProposeConfChange(new RaftConfChange(RaftConfChangeKind.AddLearner, default), out _));
        Assert.False(c.Nodes[1].TryProposeConfChange(new RaftConfChange(RaftConfChangeKind.AddLearner, stranger), out _));

        var single = new ManualCluster(1);
        single.Elect(0);
        single.Deliver();
        Assert.False(single.Nodes[0].TryProposeConfChange(new RaftConfChange(RaftConfChangeKind.RemoveNode, single.Ids[0]), out _));
    }

    [Fact]
    public void ConfChange_IsRevertedWhenItsEntryIsTruncated()
    {
        NodeId[] ids = Ids(4);
        var conf = new RaftConfState(ids[..3], []);
        (RaftNode follower, InMemoryRaftLog log) = CreateNode(ids[2], conf);
        var addLearner = new RaftConfChange(RaftConfChangeKind.AddLearner, ids[3]);

        // Leader ids[0] (term 1) appends a no-op and an uncommitted AddLearner.
        follower.Step(new AppendEntries(ids[0], ids[2], 1, 0, 0,
            [new RaftEntry(1, 1, RaftEntryKind.Noop, default), new RaftEntry(2, 1, RaftEntryKind.ConfChange, addLearner.Encode())], Commit: 1));
        Drain(follower, log);
        Assert.Contains(ids[3], follower.ConfState.Learners);

        // Leader ids[1] (term 2) never had entry 2 and overwrites it.
        follower.Step(new AppendEntries(ids[1], ids[2], 2, 1, 1, [new RaftEntry(2, 2, RaftEntryKind.Noop, default)], Commit: 1));
        Drain(follower, log);
        Assert.Equal(conf, follower.ConfState);
        Assert.Equal(2UL, log.TermAt(2));

        // Restart from the durable log replays configuration entries.
        follower.Step(new AppendEntries(ids[1], ids[2], 2, 2, 2, [new RaftEntry(3, 2, RaftEntryKind.ConfChange, addLearner.Encode())], Commit: 3));
        Drain(follower, log);
        (RaftNode restarted, _) = CreateNode(ids[2], conf, hardState: follower.HardState, log: log);
        Assert.Equal(follower.ConfState, restarted.ConfState);
        Assert.Contains(ids[3], restarted.ConfState.Learners);
    }

    [Fact]
    public void LearnerVotes_AreNotCounted()
    {
        // Two voters and a learner: with the other voter down, the learner's vote cannot elect anyone.
        var sim = new RaftSimulation(2, seed: 313, learners: 1);
        int leader = WaitForStableLeader(sim);
        int other = 1 - leader;
        sim.Crash(other);
        sim.RunTicks(3 * sim.Options.ElectionTicksMax);
        sim.Restart(leader);
        sim.RunTicks(300);
        Assert.Null(sim.LeaderIndex);
        Assert.NotEqual(RaftRole.Leader, sim.Node(leader).Role);
        sim.Restart(other);
        WaitForLeader(sim);
        AssertSafe(sim);
    }
}
