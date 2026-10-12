using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Testing;
using static Tedd.Tdfs.Tests.Unit.Internal.Raft.RaftTestHelpers;

namespace Tedd.Tdfs.Tests.Unit.Internal.Raft;

/// <summary>
/// Hand-scheduled adversarial scenarios found in review: membership changes racing leader changes, hosts that retain
/// durable entries the core no longer had in view, and hosts that step several messages per ready.
/// </summary>
public sealed class RaftAdversarialTests
{
    private static bool Between(ManualCluster c, RaftMessage m, params int[] group)
        => group.Contains(c.IndexOf(m.From)) && group.Contains(c.IndexOf(m.To));

    /// <summary>Every index applied by two nodes holds the same entry on both (state-machine safety).</summary>
    private static void AssertAppliedConsistent(ManualCluster c)
    {
        var byIndex = new Dictionary<ulong, (int Node, RaftEntry Entry)>();
        for (int i = 0; i < c.Nodes.Length; i++)
        {
            foreach (RaftEntry e in c.Applied[i])
            {
                if (byIndex.TryGetValue(e.Index, out (int Node, RaftEntry Entry) first))
                {
                    Assert.True(first.Entry.Term == e.Term && first.Entry.Kind == e.Kind && first.Entry.Data.Span.SequenceEqual(e.Data.Span),
                        $"n{first.Node} applied {first.Entry.Kind}@t{first.Entry.Term} at index {e.Index} but n{i} applied {e.Kind}@t{e.Term}");
                }
                else
                {
                    byIndex[e.Index] = (i, e);
                }
            }
        }
    }

    [Fact]
    public void NewLeader_RefusesConfChange_UntilAnEntryOfItsTermIsCommitted()
    {
        // Single-server changes take effect on append, so a new leader must commit an entry of its own term (under the
        // configuration it inherited) before appending a configuration change (Ongaro, raft-dev 2015).
        var c = new ManualCluster(3, learners: 1);
        c.Elect(0);
        c.Deliver();
        Assert.Equal(c.Nodes[0].LastIndex, c.Nodes[1].CommitIndex);

        // Node 1 wins an election but its no-op cannot commit yet (node 2 is unreachable, node 0 only votes).
        c.Nodes[1].Step(new TimeoutNow(c.Ids[0], c.Ids[1], c.Nodes[1].Term));
        c.Deliver(drop: m => !(Between(c, m, 0, 1) && m is VoteRequest or VoteResponse));
        RaftNode leader = c.Nodes[1];
        Assert.Equal(RaftRole.Leader, leader.Role);
        Assert.True(leader.CommitIndex < leader.LastIndex);
        Assert.True(leader.HasPendingConfChange);
        Assert.False(leader.TryProposeConfChange(new RaftConfChange(RaftConfChangeKind.PromoteLearner, c.Ids[3]), out _));
        Assert.False(leader.TryProposeConfChange(new RaftConfChange(RaftConfChangeKind.RemoveNode, c.Ids[0]), out _));

        // Once the no-op commits, changes are accepted again.
        c.Queue.Clear();
        c.Nodes[1].Tick();
        c.Nodes[1].Tick();
        c.Deliver();
        Assert.Equal(leader.LastIndex, leader.CommitIndex);
        Assert.False(leader.HasPendingConfChange);
        Assert.True(leader.TryProposeConfChange(new RaftConfChange(RaftConfChangeKind.PromoteLearner, c.Ids[3]), out _));
    }

    [Fact]
    public void SingleServerChange_RacingLeaderChange_CannotCommitConflictingEntries()
    {
        // Four voters (0..3) and a learner (4). The schedule is the classic single-server-change split brain:
        //  1. leader 0 promotes 4 (C1 = 5 voters) and replicates it to 4 only;
        //  2. node 1 is elected under C0 by 1,2,3 and immediately removes 0 (C2 = {1,2,3}); it commits its no-op and C2
        //     with node 2 only (a majority of C2) - unless it is required to commit its no-op under C0 first;
        //  3. node 0 is elected under C1 by 0,3,4 (3 has not seen any term-3 entry) and commits its own no-op.
        var c = new ManualCluster(4, learners: 1);
        c.Elect(0);
        c.Deliver();

        Assert.True(c.Nodes[0].TryProposeConfChange(new RaftConfChange(RaftConfChangeKind.PromoteLearner, c.Ids[4]), out ulong promoteIndex));
        c.Deliver(drop: m => !Between(c, m, 0, 4));
        Assert.Equal(promoteIndex, c.Nodes[4].LastIndex);
        Assert.True(c.Nodes[0].CommitIndex < promoteIndex);

        // Node 1 campaigns; node 3 votes but receives no entries from it.
        c.Nodes[1].Step(new TimeoutNow(c.Ids[2], c.Ids[1], c.Nodes[1].Term));
        c.Deliver(drop: m => !(Between(c, m, 1, 2) || (Between(c, m, 1, 3) && m is VoteRequest or VoteResponse)));
        Assert.Equal(RaftRole.Leader, c.Nodes[1].Role);
        c.Nodes[1].TryProposeConfChange(new RaftConfChange(RaftConfChangeKind.RemoveNode, c.Ids[0]), out _);
        c.Deliver(drop: m => !Between(c, m, 1, 2));

        // Node 0 learns the newer term from node 3 (as it would from any answer to its heartbeats) and campaigns.
        c.Queue.Clear();
        c.Nodes[0].Step(new HeartbeatResponse(c.Ids[3], c.Ids[0], c.Nodes[3].Term));
        c.Drain(0);
        c.Nodes[0].Step(new TimeoutNow(c.Ids[3], c.Ids[0], c.Nodes[0].Term));
        c.Deliver(drop: m => !Between(c, m, 0, 3, 4));
        Assert.Equal(RaftRole.Leader, c.Nodes[0].Role);
        AssertAppliedConsistent(c);

        // After healing everything converges without contradicting what was applied.
        c.Queue.Clear();
        for (int t = 0; t < 4; t++)
        {
            foreach (RaftNode n in c.Nodes)
                n.Tick();
            c.Deliver();
        }

        AssertAppliedConsistent(c);
    }

    [Fact]
    public void SnapshotInstall_RetainingMatchingDurableSuffix_KeepsMembershipInSync()
    {
        // The IRaftLogView contract lets a host keep durable entries after an installed snapshot whose last entry it holds
        // with the same term. Entries the core had dropped from its view (an unpersisted truncation) come back that way;
        // configuration changes among them must take effect again, exactly as a restart from that log would replay them.
        NodeId[] ids = Ids(4);
        var conf = new RaftConfState(ids[..3], []);
        NodeId follower = ids[2];
        (RaftNode node, InMemoryRaftLog log) = CreateNode(follower, conf);
        var addLearner = new RaftConfChange(RaftConfChangeKind.AddLearner, ids[3]);
        node.Step(new AppendEntries(ids[0], follower, 1, 0, 0,
        [
            new RaftEntry(1, 1, RaftEntryKind.Noop, default),
            new RaftEntry(2, 1, RaftEntryKind.Normal, Payload("a")),
            new RaftEntry(3, 1, RaftEntryKind.Normal, Payload("b")),
            new RaftEntry(4, 1, RaftEntryKind.ConfChange, addLearner.Encode()),
            new RaftEntry(5, 1, RaftEntryKind.Normal, Payload("c")),
        ], Commit: 1));
        Drain(node, log);
        Assert.Contains(ids[3], node.ConfState.Learners);

        // Two messages are stepped before the host handles the next ready: a term-2 leader overwrites index 3 (dropping
        // 4..5 from the core's view, so the learner is reverted), then a term-3 leader sends a snapshot at (3, term 1).
        node.Step(new AppendEntries(ids[1], follower, 2, 2, 1, [new RaftEntry(3, 2, RaftEntryKind.Normal, Payload("x"))], Commit: 1));
        Assert.DoesNotContain(ids[3], node.ConfState.Learners);
        node.Step(new InstallSnapshot(ids[0], follower, 3, new RaftSnapshotMeta(3, 1, conf), 0, ReadOnlyMemory<byte>.Empty, Done: true));
        Drain(node, log);

        Assert.Equal(5UL, log.LastIndex); // the host kept 4..5 (allowed: it holds (3, term 1))
        Assert.Equal(log.LastIndex, node.LastIndex);
        Assert.Equal(log.TermAt(log.LastIndex), node.LastTerm);
        (RaftNode restarted, _) = CreateNode(follower, log.Snapshot.ConfState, hardState: node.HardState, log: log);
        Assert.Equal(restarted.ConfState, node.ConfState);
        Assert.Contains(ids[3], node.ConfState.Learners);
    }

    [Fact]
    public void HostThatSkipsMatchingEntries_KeepsTheCoreViewConsistent()
    {
        // InMemoryRaftLog.Append skips entries it already holds with the same term and keeps a longer durable suffix.
        // When several messages are stepped per ready, the core can hand it a batch that ends before that suffix does:
        // the core must then see the durable log as it is (consistent last index/term, configuration replayed).
        NodeId[] ids = Ids(4);
        var conf = new RaftConfState(ids[..3], []);
        NodeId follower = ids[2];
        (RaftNode node, InMemoryRaftLog log) = CreateNode(follower, conf);
        var addLearner = new RaftConfChange(RaftConfChangeKind.AddLearner, ids[3]);
        node.Step(new AppendEntries(ids[0], follower, 1, 0, 0,
        [
            new RaftEntry(1, 1, RaftEntryKind.Noop, default),
            new RaftEntry(2, 1, RaftEntryKind.Normal, Payload("a")),
            new RaftEntry(3, 1, RaftEntryKind.Normal, Payload("b")),
            new RaftEntry(4, 1, RaftEntryKind.ConfChange, addLearner.Encode()),
            new RaftEntry(5, 1, RaftEntryKind.Normal, Payload("c")),
        ], Commit: 1));
        Drain(node, log);

        // Term 2 overwrites index 3 in the core's view only; term 3 (whose log still has (3, term 1)) restores it.
        node.Step(new AppendEntries(ids[1], follower, 2, 2, 1, [new RaftEntry(3, 2, RaftEntryKind.Normal, Payload("x"))], Commit: 1));
        node.Step(new AppendEntries(ids[0], follower, 3, 2, 1, [new RaftEntry(3, 1, RaftEntryKind.Normal, Payload("b"))], Commit: 1));
        Drain(node, log);

        Assert.Equal(log.LastIndex, node.LastIndex);
        Assert.Equal(log.TermAt(log.LastIndex), node.LastTerm);
        (RaftNode restarted, _) = CreateNode(follower, conf, hardState: node.HardState, log: log);
        Assert.Equal(restarted.ConfState, node.ConfState);

        // The core keeps working on that log: the next append continues after it.
        node.Step(new AppendEntries(ids[0], follower, 3, 5, 1, [new RaftEntry(6, 3, RaftEntryKind.Noop, default)], Commit: 6));
        Drain(node, log);
        Assert.Equal(6UL, node.CommitIndex);
        Assert.Equal(6UL, log.LastIndex);
    }

    [Theory]
    [InlineData(3)]
    [InlineData(2)]
    public void LeaderRemovingItself_HandsOverWithTimeoutNow_WithoutAnElectionTimeout(int voters)
    {
        // Once its removal commits the leader sends TimeoutNow to an up-to-date voter. That voter already applied the
        // removal (effective on append), so the old leader is no longer a member in its view: the hand-over must still be
        // honoured, otherwise the group sits leaderless for a whole election timeout. No ticks are used here, so only the
        // hand-over can produce the next leader.
        var c = new ManualCluster(voters);
        c.Elect(0);
        c.Deliver();
        RaftNode old = c.Nodes[0];
        ulong term = old.Term;
        Assert.True(old.TryProposeConfChange(new RaftConfChange(RaftConfChangeKind.RemoveNode, c.Ids[0]), out ulong removal));
        c.Deliver();

        Assert.NotEqual(RaftRole.Leader, old.Role);
        int next = Array.FindIndex(c.Nodes, n => n.Role == RaftRole.Leader);
        Assert.True(next > 0, "no voter took over after the leader's removal committed");
        RaftNode leader = c.Nodes[next];
        Assert.True(leader.Term > term);
        Assert.True(leader.CommitIndex > removal); // its own no-op committed on top of the removal
        Assert.DoesNotContain(c.Ids[0], leader.ConfState.Voters);
        AssertAppliedConsistent(c);

        // The removed node never campaigns again.
        for (int t = 0; t < 100; t++)
            old.Tick();
        c.Drain(0);
        Assert.DoesNotContain(c.Queue, m => m.From == c.Ids[0] && m is VoteRequest);
    }

    [Fact]
    public void LostReadConfirmationRound_IsRetriedByPeriodicHeartbeats()
    {
        // The heartbeat round confirming a ReadIndex (or its answers) can be lost. The leader must retry it on its regular
        // heartbeats rather than leave the read pending until some later ReadIndex call happens to start a new round.
        var c = new ManualCluster(3);
        c.Elect(0);
        c.Deliver();
        RaftNode leader = c.Nodes[0];
        leader.ReadIndex(7);
        c.Drain(0);
        Assert.Contains(c.Queue, m => m is Heartbeat { ReadContext: not 0 });
        c.Queue.Clear(); // the confirmation round is lost
        Assert.Equal(1, leader.PendingReadCount);

        for (int t = 0; t < leader.Config.HeartbeatTicks; t++)
            leader.Tick();
        c.Deliver();
        RaftReadState read = Assert.Single(c.Reads[0]);
        Assert.Equal(7UL, read.RequestContext);
        Assert.Equal(leader.CommitIndex, read.Index);
        Assert.Equal(0, leader.PendingReadCount);

        // Answers to the retried round are also lost; the next round still confirms.
        leader.ReadIndex(8);
        c.Deliver(drop: m => m is HeartbeatResponse);
        Assert.Equal(1, leader.PendingReadCount);
        for (int t = 0; t < leader.Config.HeartbeatTicks; t++)
            leader.Tick();
        c.Deliver(drop: m => m is HeartbeatResponse);
        for (int t = 0; t < leader.Config.HeartbeatTicks; t++)
            leader.Tick();
        c.Deliver();
        Assert.Equal([7UL, 8UL], c.Reads[0].Select(r => r.RequestContext));
        Assert.Equal(0, leader.PendingReadCount);
    }

    [Fact]
    public void MalformedMessagesWithAHigherTerm_DoNotChangeTermVoteOrLeader()
    {
        // Structural garbage must be ignored outright. Validating only after adopting the sender's term would let a
        // corrupt or buggy peer reset this node's vote and leader (and force a hard-state write) with a message that is
        // then discarded anyway.
        NodeId[] ids = Ids(3);
        var conf = new RaftConfState(ids, []);
        (RaftNode node, InMemoryRaftLog log) = CreateNode(ids[1], conf, hardState: new RaftHardState(2, ids[2], 0));
        RaftMessage[] bad =
        [
            new AppendEntries(ids[0], ids[1], 5, 0, 0, [new RaftEntry(2, 5, RaftEntryKind.Normal, default)], 0),  // gap
            new AppendEntries(ids[0], ids[1], 5, 0, 6, [], 0),                                                   // previous term above the leader's
            new AppendEntries(ids[0], ids[1], 5, 0, 0, [new RaftEntry(1, 6, RaftEntryKind.Normal, default)], 0),  // entry term above the leader's
            new AppendEntries(ids[0], ids[1], 5, 0, 0, [new RaftEntry(1, 5, (RaftEntryKind)9, default)], 0),      // unknown kind
            new InstallSnapshot(ids[0], ids[1], 5, null!, 0, default, true),                                      // no metadata
            new InstallSnapshot(ids[0], ids[1], 5, new RaftSnapshotMeta(9, 4, null!), 0, default, true),          // no membership
            new InstallSnapshot(ids[0], ids[1], 5, new RaftSnapshotMeta(9, 6, conf), 0, default, true),           // snapshot term above the leader's
            new VoteRequest(ids[0], ids[1], 5, false, 10, 7),                                                    // last log term above the campaign term
        ];
        foreach (RaftMessage m in bad)
            node.Step(m);

        Assert.Equal(2UL, node.Term);
        Assert.Equal(ids[2], node.Vote);
        Assert.Null(node.Leader);
        Assert.False(node.HasReady());
        Assert.Equal(0UL, log.LastIndex);

        // A well-formed message at that term is still processed normally.
        node.Step(new Heartbeat(ids[0], ids[1], 5, 0));
        Assert.Equal(5UL, node.Term);
        Assert.Equal(ids[0], node.Leader);
    }

    [Fact]
    public void DelayedChunkOfAnOlderSnapshot_DoesNotAbortTheTransferOfANewerOne()
    {
        NodeId[] ids = Ids(3);
        var conf = new RaftConfState(ids, []);
        (RaftNode node, InMemoryRaftLog log) = CreateNode(ids[1], conf);
        var older = new RaftSnapshotMeta(50, 3, conf);
        var newer = new RaftSnapshotMeta(90, 3, conf);
        byte[] data = [1, 2, 3, 4, 5, 6, 7, 8];

        node.Step(new InstallSnapshot(ids[0], ids[1], 3, older, 0, data, Done: true));
        Drain(node, log);
        Assert.Equal(50UL, node.CommitIndex);

        node.Step(new InstallSnapshot(ids[0], ids[1], 3, newer, 0, data.AsMemory(0, 4), Done: false));
        Drain(node, log);

        // A duplicate of the older snapshot's chunk arrives late (it is answered, but must not reset the assembly).
        node.Step(new InstallSnapshot(ids[0], ids[1], 3, older, 0, data, Done: true));
        AppendResponse stale = Assert.IsType<AppendResponse>(Assert.Single(Drain(node, log).SelectMany(r => r.Messages)));
        Assert.Equal(50UL, stale.MatchIndex);

        node.Step(new InstallSnapshot(ids[0], ids[1], 3, newer, 4, data.AsMemory(4, 4), Done: true));
        List<RaftReady> readies = Drain(node, log);
        Assert.DoesNotContain(readies.SelectMany(r => r.Messages), m => m is InstallSnapshotResponse { Reject: true });
        Assert.Equal(90UL, Assert.Single(readies, r => r.SnapshotToInstall is not null).SnapshotToInstall!.Index);
        Assert.Equal(90UL, node.CommitIndex);
        Assert.Equal(90UL, log.Snapshot.Index);
    }

    [Fact]
    public void ElectionTimeout_CoversTheInclusiveConfiguredRange()
    {
        // RaftConfig documents ElectionTicksMax as the inclusive upper bound (architecture: "30-60 ticks").
        var seen = new SortedSet<int>();
        for (ulong seed = 1; seed <= 300; seed++)
        {
            NodeId self = Ids(1, seed)[0];
            var conf = new RaftConfState([self, Ids(2, seed + 1000)[1]], []);
            (RaftNode node, _) = CreateNode(self, conf, new RaftConfig(self, 10, 14, 2, PreVote: false), seed: seed);
            int ticks = 0;
            while (node.Role != RaftRole.Candidate)
            {
                node.Tick();
                ticks++;
                Assert.True(ticks <= 14, $"seed {seed}: no campaign within the maximum timeout");
            }

            seen.Add(ticks);
        }

        Assert.Equal(new[] { 10, 11, 12, 13, 14 }, seen.ToArray());

        // Equal bounds give exactly that timeout.
        NodeId fixedSelf = Ids(1, 7)[0];
        (RaftNode exact, _) = CreateNode(fixedSelf, new RaftConfState([fixedSelf, Ids(2, 8)[1]], []),
            new RaftConfig(fixedSelf, 12, 12, 2, PreVote: false));
        for (int t = 0; t < 11; t++)
            exact.Tick();
        Assert.Equal(RaftRole.Follower, exact.Role);
        exact.Tick();
        Assert.Equal(RaftRole.Candidate, exact.Role);
    }

    [Fact]
    public void Simulation_RecordsALeaderThatCrashesRightAfterSending()
    {
        // The election-safety oracle must see every leader whose messages reached the network, including one that
        // crashes before it advances its ready.
        var sim = new RaftSimulation(1, seed: 5);
        sim.ArmCrash(0, RaftCrashPoint.AfterSendBeforeApply);
        Assert.True(sim.TryRunUntil(() => !sim.IsUp(0), ElectionBudget));
        Assert.Equal(RaftRole.Leader, sim.Node(0).Role);
        ulong term = sim.Node(0).Term;
        Assert.Equal(term, sim.PersistedHardState(0).Term);
        Assert.True(sim.LeadersByTerm.TryGetValue(term, out int recorded), $"leader of term {term} was not recorded");
        Assert.Equal(0, recorded);

        sim.Restart(0);
        WaitForStableLeader(sim);
        AssertSafe(sim);
    }

    [Fact]
    public void LaggingNode_RememberingADepartedLeader_StillVotesForCandidatesItDoesNotKnow()
    {
        // Found by the membership-churn campaign: leader A adds X as a learner and promotes it while X is cut off, then
        // loses leadership. Voters are {A, X}, but X still holds its bootstrap membership (which lists neither A nor any
        // later voter) and still remembers A as its leader. X is a learner in its own view, so it never campaigns and
        // nothing ever clears that memory. If X ignored non-members' vote requests for as long as it remembered a leader,
        // no candidate could ever collect X's vote and the group would stay leaderless forever.
        NodeId[] ids = Ids(5);
        NodeId a = ids[0], c = ids[1], x = ids[2];
        var stale = new RaftConfState([ids[3], ids[4]], [x]);
        (RaftNode node, InMemoryRaftLog log) = CreateNode(x, stale);
        node.Step(new Heartbeat(a, x, 5, 0));
        Drain(node, log);
        Assert.Equal(a, node.Leader);

        // While the leader is live, non-members cannot disturb this node (removed servers are ignored).
        node.Step(new VoteRequest(c, x, 6, PreVote: true, 12, 5));
        Assert.Empty(Drain(node, log).SelectMany(r => r.Messages));

        // Once the leader has been silent for an election timeout, candidates it does not know are answered: both the old
        // leader itself and any other candidate.
        for (int t = 0; t < node.Config.ElectionTicksMin; t++)
            node.Tick();
        Assert.Equal(RaftRole.Learner, node.Role); // still never campaigns
        node.Step(new VoteRequest(a, x, 6, PreVote: true, 12, 5));
        VoteResponse toA = Assert.IsType<VoteResponse>(Assert.Single(Drain(node, log).SelectMany(r => r.Messages)));
        Assert.False(toA.Reject);
        Assert.Equal(5UL, node.Term); // a pre-vote changes nothing
        node.Step(new VoteRequest(c, x, 6, PreVote: false, 12, 5));
        VoteResponse toC = Assert.IsType<VoteResponse>(Assert.Single(Drain(node, log).SelectMany(r => r.Messages)));
        Assert.False(toC.Reject);
        Assert.Equal(6UL, node.Term);
        Assert.Equal(c, node.Vote);
    }

    [Fact]
    public void Simulation_WithDeferredReadies_IsDeterministic_SafeAndConverges()
    {
        // Hosts that keep readies outstanding while the core is stepped (as asynchronous hosts do) must still replay
        // exactly per seed, satisfy every oracle (including core/durable log agreement) and replicate everything.
        static (RaftSimulation Sim, IReadOnlyList<string> Run) Run(ulong seed)
        {
            var sim = new RaftSimulation(new RaftSimulationOptions { Voters = 5, Seed = seed, SnapshotThreshold = 8, DeferReadyProbability = 0.5 });
            sim.DropProbability(0.05);
            sim.ReorderMessages(true);
            for (int i = 0; i < 60; i++)
            {
                if (sim.LeaderIndex is int l)
                    sim.Propose(l, Payload($"d{i}"));
                if (i == 20)
                    sim.Partition([0, 1], [2, 3, 4]);
                if (i == 35)
                    sim.Heal();
                sim.RunTicks(4);
            }

            return (sim, [.. sim.Trace, .. All(sim).Select(n => string.Join(",", sim.AppliedLog(n).Select(AsText)))]);
        }

        (RaftSimulation sim, IReadOnlyList<string> first) = Run(17);
        Assert.Equal(first, Run(17).Run);
        Assert.True(sim.ReadiesDeferred > 100, $"only {sim.ReadiesDeferred} readies were deferred");
        AssertSafe(sim);

        sim.DropProbability(0);
        sim.ReorderMessages(false);
        int leader = ProposeAtLeader(sim, Payload("final"));
        WaitForApplied(sim, All(sim), sim.AppliedLog(leader).Count + 1);
        sim.RunTicks(20);
        IReadOnlyList<string> reference = sim.AppliedLog(0).Select(AsText).ToArray();
        Assert.Equal("final", reference[^1]);
        Assert.All(All(sim), i => Assert.Equal(reference, sim.AppliedLog(i).Select(AsText)));
        AssertSafe(sim);
    }
}
