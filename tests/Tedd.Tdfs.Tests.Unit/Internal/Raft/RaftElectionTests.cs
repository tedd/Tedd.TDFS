using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Testing;
using static Tedd.Tdfs.Tests.Unit.Internal.Raft.RaftTestHelpers;

namespace Tedd.Tdfs.Tests.Unit.Internal.Raft;

/// <summary>Leader election: basics, failover, PreVote, CheckQuorum, vote rules.</summary>
public sealed class RaftElectionTests
{
    [Fact]
    public void SingleVoter_ElectsItselfAndCommits()
    {
        var sim = new RaftSimulation(1, seed: 7);
        int leader = WaitForLeader(sim);
        Assert.Equal(0, leader);
        Assert.True(sim.Propose(0, Payload("a")));
        sim.RunTicks(1);
        Assert.Equal(["a"], sim.AppliedLog(0).Select(AsText));
        AssertSafe(sim);
    }

    [Theory]
    [InlineData(3)]
    [InlineData(5)]
    public void Cluster_ElectsExactlyOneLeader_AndReplicates(int voters)
    {
        var sim = new RaftSimulation(voters, seed: 11);
        int leader = WaitForLeader(sim);
        Assert.Equal(RaftRole.Leader, sim.Node(leader).Role);
        Assert.Single(All(sim), i => sim.Node(i).Role == RaftRole.Leader);
        sim.RunTicks(5);
        ulong term = sim.Node(leader).Term;
        Assert.All(All(sim), i =>
        {
            Assert.Equal(term, sim.Node(i).Term);
            Assert.Equal(sim.IdOf(leader), sim.Node(i).Leader);
        });

        Assert.True(sim.Propose(leader, Payload("x")));
        WaitForApplied(sim, All(sim), 1);
        AssertSafe(sim);
    }

    [Fact]
    public void ElectionTimeout_IsWithinConfiguredRange()
    {
        // A lone voter with PreVote off campaigns exactly when its randomized timeout expires.
        for (ulong seed = 1; seed <= 50; seed++)
        {
            NodeId self = Ids(1, seed)[0];
            var conf = new RaftConfState([self, Ids(2, seed + 1000)[1]], []);
            (RaftNode node, InMemoryRaftLog log) = CreateNode(self, conf, new RaftConfig(self, 10, 20, 2, PreVote: false), seed: seed);
            int ticks = 0;
            while (node.Role != RaftRole.Candidate)
            {
                node.Tick();
                ticks++;
                Assert.True(ticks < 100);
            }

            Assert.InRange(ticks, 10, 19);
            Drain(node, log);
        }
    }

    [Fact]
    public void LeaderCrash_NewLeaderElected_CommittedEntriesSurvive()
    {
        var sim = new RaftSimulation(3, seed: 21);
        int leader = WaitForStableLeader(sim);
        for (int i = 0; i < 5; i++)
            Assert.True(sim.Propose(leader, Payload($"e{i}")));
        WaitForApplied(sim, All(sim), 5);
        ulong oldTerm = sim.Node(leader).Term;

        sim.Crash(leader);
        int next = WaitForLeader(sim);
        Assert.NotEqual(leader, next);
        Assert.True(sim.Node(next).Term > oldTerm);
        Assert.True(sim.Propose(next, Payload("after")));
        int[] survivors = [.. All(sim).Where(i => i != leader)];
        WaitForApplied(sim, survivors, 6);
        Assert.All(survivors, i => Assert.Equal(["e0", "e1", "e2", "e3", "e4", "after"], sim.AppliedLog(i).Select(AsText)));

        sim.Restart(leader);
        WaitForApplied(sim, All(sim), 6);
        Assert.Equal(["e0", "e1", "e2", "e3", "e4", "after"], sim.AppliedLog(leader).Select(AsText));
        AssertSafe(sim);
    }

    [Fact]
    public void PreVote_RejoiningPartitionedFollower_DoesNotDisruptLeader()
    {
        var sim = new RaftSimulation(3, seed: 31);
        int leader = WaitForStableLeader(sim);
        int follower = (leader + 1) % 3;
        ulong term = sim.Node(leader).Term;

        sim.Isolate(follower);
        sim.RunTicks(300); // many election timeouts: without PreVote its term would climb
        Assert.Equal(term, sim.Node(follower).Term);
        Assert.Equal(RaftRole.PreCandidate, sim.Node(follower).Role);

        sim.Heal();
        sim.RunTicks(50);
        Assert.Equal(leader, sim.LeaderIndex);
        Assert.Equal(term, sim.Node(leader).Term);
        Assert.Equal(RaftRole.Follower, sim.Node(follower).Role);
        AssertSafe(sim);
    }

    [Fact]
    public void WithoutPreVote_RejoiningFollowerDisruptsTerm()
    {
        // Control for the PreVote test: the same schedule without PreVote and CheckQuorum bumps the term.
        var sim = new RaftSimulation(new RaftSimulationOptions { Voters = 3, Seed = 31, PreVote = false, CheckQuorum = false });
        int leader = WaitForStableLeader(sim);
        int follower = (leader + 1) % 3;
        ulong term = sim.Node(leader).Term;
        sim.Isolate(follower);
        sim.RunTicks(300);
        Assert.True(sim.Node(follower).Term > term + 5);
        sim.Heal();
        // The rejoining node's higher term deposes the healthy leader.
        Assert.True(sim.TryRunUntil(() => sim.LeaderIndex is int l && sim.Node(l).Term > term, ElectionBudget));
        AssertSafe(sim);
    }

    [Fact]
    public void CheckQuorum_IsolatedLeaderStepsDown()
    {
        var sim = new RaftSimulation(3, seed: 41);
        int leader = WaitForStableLeader(sim);
        sim.Isolate(leader);
        Assert.True(sim.TryRunUntil(() => sim.Node(leader).Role != RaftRole.Leader, 3 * sim.Options.ElectionTicksMin),
            "isolated leader did not step down");
        Assert.Equal(RaftRole.Follower, sim.Node(leader).Role);

        // The majority side elects a new leader in a higher term.
        bool ok = sim.TryRunUntil(() => All(sim).Any(i => i != leader && sim.Node(i).Role == RaftRole.Leader), ElectionBudget);
        Assert.True(ok);
        AssertSafe(sim);
    }

    [Fact]
    public void WithoutCheckQuorum_IsolatedLeaderKeepsRole()
    {
        var sim = new RaftSimulation(new RaftSimulationOptions { Voters = 3, Seed = 41, CheckQuorum = false });
        int leader = WaitForStableLeader(sim);
        sim.Isolate(leader);
        sim.RunTicks(200);
        Assert.Equal(RaftRole.Leader, sim.Node(leader).Role);
        AssertSafe(sim);
    }

    [Fact]
    public void Voter_NeverVotesTwiceInATerm()
    {
        NodeId[] ids = Ids(3);
        var conf = new RaftConfState(ids, []);
        (RaftNode node, InMemoryRaftLog log) = CreateNode(ids[0], conf);

        node.Step(new VoteRequest(ids[1], ids[0], 5, PreVote: false, LastLogIndex: 0, LastLogTerm: 0));
        List<RaftReady> first = Drain(node, log);
        VoteResponse granted = Assert.IsType<VoteResponse>(Assert.Single(first.SelectMany(r => r.Messages)));
        Assert.False(granted.Reject);
        Assert.Equal(ids[1], node.Vote);

        // The vote is in the ready's hard state, so the host persists it before sending the response.
        RaftReady withVote = Assert.Single(first, r => r.Messages.Count > 0);
        Assert.Equal(new RaftHardState(5, ids[1], 0), withVote.HardState);

        node.Step(new VoteRequest(ids[2], ids[0], 5, PreVote: false, LastLogIndex: 10, LastLogTerm: 4));
        VoteResponse rejected = Assert.IsType<VoteResponse>(Assert.Single(Drain(node, log).SelectMany(r => r.Messages)));
        Assert.True(rejected.Reject);
        Assert.Equal(ids[1], node.Vote);

        // Re-delivery of the same request is answered consistently (idempotent).
        node.Step(new VoteRequest(ids[1], ids[0], 5, PreVote: false, LastLogIndex: 0, LastLogTerm: 0));
        Assert.False(Assert.IsType<VoteResponse>(Assert.Single(Drain(node, log).SelectMany(r => r.Messages))).Reject);
    }

    [Fact]
    public void Voter_RejectsCandidateWithStaleLog()
    {
        NodeId[] ids = Ids(3);
        var conf = new RaftConfState(ids, []);
        var log = new InMemoryRaftLog();
        log.Append([new RaftEntry(1, 1, RaftEntryKind.Noop, default), new RaftEntry(2, 2, RaftEntryKind.Normal, Payload("x"))]);
        (RaftNode node, _) = CreateNode(ids[0], conf, hardState: new RaftHardState(2, null, 1), log: log);

        // Lower last term.
        node.Step(new VoteRequest(ids[1], ids[0], 3, false, LastLogIndex: 5, LastLogTerm: 1));
        Assert.True(Assert.IsType<VoteResponse>(Assert.Single(Drain(node, log).SelectMany(r => r.Messages))).Reject);
        // Same last term, shorter log.
        node.Step(new VoteRequest(ids[1], ids[0], 4, false, LastLogIndex: 1, LastLogTerm: 2));
        Assert.True(Assert.IsType<VoteResponse>(Assert.Single(Drain(node, log).SelectMany(r => r.Messages))).Reject);
        // Up to date.
        node.Step(new VoteRequest(ids[2], ids[0], 5, false, LastLogIndex: 2, LastLogTerm: 2));
        Assert.False(Assert.IsType<VoteResponse>(Assert.Single(Drain(node, log).SelectMany(r => r.Messages))).Reject);
        Assert.Equal(5UL, node.Term);
    }

    [Fact]
    public void PreVote_DoesNotChangeTermOrVote()
    {
        NodeId[] ids = Ids(3);
        (RaftNode node, InMemoryRaftLog log) = CreateNode(ids[0], new RaftConfState(ids, []), hardState: new RaftHardState(3, null, 0));
        node.Step(new VoteRequest(ids[1], ids[0], 4, PreVote: true, 0, 0));
        VoteResponse resp = Assert.IsType<VoteResponse>(Assert.Single(Drain(node, log).SelectMany(r => r.Messages)));
        Assert.False(resp.Reject);
        Assert.True(resp.PreVote);
        Assert.Equal(4UL, resp.Term);
        Assert.Equal(3UL, node.Term);
        Assert.Null(node.Vote);
    }

    [Fact]
    public void FollowerInLease_IgnoresVoteRequests_ButHonoursLeadershipTransfer()
    {
        NodeId[] ids = Ids(3);
        (RaftNode node, InMemoryRaftLog log) = CreateNode(ids[0], new RaftConfState(ids, []), hardState: new RaftHardState(2, null, 0));
        node.Step(new Heartbeat(ids[1], ids[0], 2, 0));
        Drain(node, log);
        Assert.Equal(ids[1], node.Leader);

        node.Step(new VoteRequest(ids[2], ids[0], 3, PreVote: false, 0, 0));
        Assert.Empty(Drain(node, log).SelectMany(r => r.Messages));
        Assert.Equal(2UL, node.Term);

        node.Step(new VoteRequest(ids[2], ids[0], 3, PreVote: false, 0, 0, LeadershipTransfer: true));
        Assert.False(Assert.IsType<VoteResponse>(Assert.Single(Drain(node, log).SelectMany(r => r.Messages))).Reject);
        Assert.Equal(3UL, node.Term);
    }

    [Fact]
    public void StaleTermLeaderMessage_GetsHigherTermResponse()
    {
        NodeId[] ids = Ids(3);
        (RaftNode node, InMemoryRaftLog log) = CreateNode(ids[0], new RaftConfState(ids, []), hardState: new RaftHardState(5, null, 0));
        node.Step(new AppendEntries(ids[1], ids[0], 3, 0, 0, [], 0));
        AppendResponse resp = Assert.IsType<AppendResponse>(Assert.Single(Drain(node, log).SelectMany(r => r.Messages)));
        Assert.Equal(5UL, resp.Term);
        Assert.Equal(5UL, node.Term);
    }

    [Fact]
    public void Learner_NeverCampaigns()
    {
        var sim = new RaftSimulation(3, seed: 51, learners: 1);
        WaitForStableLeader(sim);
        Assert.Equal(RaftRole.Learner, sim.Node(3).Role);
        for (int v = 0; v < 3; v++)
            sim.Crash(v);
        sim.RunTicks(500);
        Assert.Equal(RaftRole.Learner, sim.Node(3).Role);
        Assert.Equal(sim.PersistedHardState(3).Term, sim.Node(3).Term);
        Assert.Null(sim.PersistedHardState(3).Vote);
        AssertSafe(sim);
    }

    [Fact]
    public void Restart_PreservesTermAndVote()
    {
        var sim = new RaftSimulation(3, seed: 61);
        int leader = WaitForStableLeader(sim);
        int follower = (leader + 1) % 3;
        RaftHardState before = sim.PersistedHardState(follower);
        sim.Crash(follower);
        sim.Restart(follower);
        Assert.Equal(before.Term, sim.Node(follower).Term);
        Assert.Equal(before.Vote, sim.Node(follower).Vote);
        AssertSafe(sim);
    }

    [Fact]
    public void CrashAfterVotePersistedBeforeSend_CannotVoteAgainInThatTerm()
    {
        // CON-004: a vote persisted but never sent still binds the voter after restart.
        NodeId[] ids = Ids(3);
        var conf = new RaftConfState(ids, []);
        var log = new InMemoryRaftLog();
        (RaftNode node, _) = CreateNode(ids[0], conf, log: log);
        node.Step(new VoteRequest(ids[1], ids[0], 7, false, 0, 0));
        RaftReady rd = node.GetReady();
        RaftHardState persisted = rd.HardState!.Value;   // host persists, then "crashes" before sending

        (RaftNode restarted, InMemoryRaftLog log2) = CreateNode(ids[0], conf, hardState: persisted, log: log);
        restarted.Step(new VoteRequest(ids[2], ids[0], 7, false, 0, 0));
        Assert.True(Assert.IsType<VoteResponse>(Assert.Single(Drain(restarted, log2).SelectMany(r => r.Messages))).Reject);
        restarted.Step(new VoteRequest(ids[1], ids[0], 7, false, 0, 0));
        Assert.False(Assert.IsType<VoteResponse>(Assert.Single(Drain(restarted, log2).SelectMany(r => r.Messages))).Reject);
    }
}
