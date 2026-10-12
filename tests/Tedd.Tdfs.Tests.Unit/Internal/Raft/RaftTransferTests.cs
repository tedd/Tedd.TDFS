using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Testing;
using static Tedd.Tdfs.Tests.Unit.Internal.Raft.RaftTestHelpers;

namespace Tedd.Tdfs.Tests.Unit.Internal.Raft;

/// <summary>Leadership transfer via TimeoutNow.</summary>
public sealed class RaftTransferTests
{
    [Fact]
    public void Transfer_ToUpToDateFollower_ElectsIt()
    {
        var sim = new RaftSimulation(3, seed: 501);
        int leader = WaitForStableLeader(sim);
        int target = (leader + 1) % 3;
        ulong term = sim.Node(leader).Term;
        sim.TransferLeadership(leader, target);
        Assert.True(sim.TryRunUntil(() => sim.LeaderIndex == target, 5), Tail(sim));
        Assert.Equal(term + 1, sim.Node(target).Term);
        ProposeAtLeader(sim, Payload("after-transfer"));
        WaitForApplied(sim, All(sim), 1);
        AssertSafe(sim);
    }

    [Fact]
    public void Transfer_ToLaggingFollower_CatchesItUpFirst()
    {
        var sim = new RaftSimulation(3, seed: 503);
        int leader = WaitForStableLeader(sim);
        int target = (leader + 1) % 3;
        sim.Isolate(target);
        for (int i = 0; i < 20; i++)
            sim.Propose(leader, Payload($"l{i}"));
        sim.RunTicks(3);
        Assert.True(sim.Node(target).LastIndex < sim.Node(leader).LastIndex);

        sim.Heal();
        sim.TransferLeadership(leader, target);
        Assert.Equal(sim.IdOf(target), sim.Node(leader).LeadTransferee);
        Assert.False(sim.Propose(leader, Payload("refused")), "proposals are refused while a transfer is in progress");
        Assert.True(sim.TryRunUntil(() => sim.LeaderIndex == target, 50), Tail(sim));
        WaitForApplied(sim, All(sim), 20);
        Assert.Equal(Enumerable.Range(0, 20).Select(i => $"l{i}"), sim.AppliedLog(target).Select(AsText));
        AssertSafe(sim);
    }

    [Fact]
    public void Transfer_ToUnreachableTarget_IsAbandonedAfterElectionTimeout()
    {
        var sim = new RaftSimulation(3, seed: 505);
        int leader = WaitForStableLeader(sim);
        int target = (leader + 1) % 3;
        sim.Cut(leader, target);
        sim.Propose(leader, Payload("x"));
        sim.TransferLeadership(leader, target);
        Assert.NotNull(sim.Node(leader).LeadTransferee);
        sim.RunTicks(sim.Options.ElectionTicksMin + 1);
        Assert.Null(sim.Node(leader).LeadTransferee);
        Assert.Equal(leader, sim.LeaderIndex);
        Assert.True(sim.Propose(leader, Payload("accepted-again")));
        AssertSafe(sim);
    }

    [Fact]
    public void Transfer_ToLearnerSelfOrNonMember_IsIgnored()
    {
        var sim = new RaftSimulation(3, seed: 507, learners: 1);
        int leader = WaitForStableLeader(sim);
        sim.TransferLeadership(leader, 3);
        Assert.Null(sim.Node(leader).LeadTransferee);
        sim.TransferLeadership(leader, leader);
        Assert.Null(sim.Node(leader).LeadTransferee);
        int stranger = sim.AddNode();
        sim.TransferLeadership(leader, stranger);
        Assert.Null(sim.Node(leader).LeadTransferee);
        sim.RunTicks(50);
        Assert.Equal(leader, sim.LeaderIndex);

        // A follower ignores the request entirely.
        sim.TransferLeadership((leader + 1) % 3, leader);
        Assert.Null(sim.Node((leader + 1) % 3).LeadTransferee);
    }

    [Fact]
    public void TimeoutNow_BypassesPreVoteAndLease()
    {
        var c = new ManualCluster(3);
        c.Elect(0);
        c.Deliver();
        ulong term = c.Nodes[0].Term;
        c.Nodes[0].TransferLeadership(c.Ids[2]);
        c.Drain(0);
        TimeoutNow tn = Assert.Single(c.Pending<TimeoutNow>(2));
        Assert.Equal(term, tn.Term);
        c.Deliver();
        Assert.Equal(RaftRole.Leader, c.Nodes[2].Role);
        Assert.Equal(term + 1, c.Nodes[2].Term);
        Assert.Equal(RaftRole.Follower, c.Nodes[0].Role);
    }
}
