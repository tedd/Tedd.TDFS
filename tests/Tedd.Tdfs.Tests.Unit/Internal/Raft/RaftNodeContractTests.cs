using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Testing;
using static Tedd.Tdfs.Tests.Unit.Internal.Raft.RaftTestHelpers;

namespace Tedd.Tdfs.Tests.Unit.Internal.Raft;

/// <summary>Host contract (Ready/Advance), construction validation, malformed input, codecs and determinism.</summary>
public sealed class RaftNodeContractTests
{
    [Fact]
    public void Ready_AtMostOneOutstanding()
    {
        NodeId[] ids = Ids(1);
        (RaftNode node, InMemoryRaftLog log) = CreateNode(ids[0], new RaftConfState(ids, []));
        for (int t = 0; t < 30 && !node.HasReady(); t++)
            node.Tick();
        Assert.True(node.HasReady());
        RaftReady rd = node.GetReady();
        Assert.False(node.HasReady());
        Assert.Throws<InvalidOperationException>(() => node.GetReady());

        // Work arriving meanwhile goes into the next ready.
        Assert.Equal(RaftRole.Leader, node.Role);
        Assert.True(node.TryPropose(Payload("p"), out ulong index));
        Assert.Throws<InvalidOperationException>(() => node.Advance(new RaftReady(null, [], [], [], null, default, [], RaftRole.Leader, null)));

        log.Append(rd.EntriesToPersist);
        node.Advance(rd);
        Assert.True(node.HasReady());
        RaftReady next = node.GetReady();
        Assert.Contains(next.EntriesToPersist, e => e.Index == index);
        log.Append(next.EntriesToPersist);
        node.Advance(next);
        Assert.Throws<InvalidOperationException>(() => node.Advance(next));
    }

    [Fact]
    public void Advance_DetectsHostThatDidNotPersist()
    {
        NodeId[] ids = Ids(1);
        (RaftNode node, _) = CreateNode(ids[0], new RaftConfState(ids, []));
        for (int t = 0; t < 30 && !node.HasReady(); t++)
            node.Tick();
        RaftReady rd = node.GetReady();
        Assert.NotEmpty(rd.EntriesToPersist);
        Assert.Throws<InvalidOperationException>(() => node.Advance(rd)); // entries were never written to the log view
    }

    [Fact]
    public void Ready_PersistsBeforeDependentMessages_AndAppliesOnce()
    {
        var c = new ManualCluster(3);
        c.Elect(0);
        c.Deliver();
        Assert.True(c.Nodes[0].TryPropose(Payload("once"), out ulong index));
        c.Drain(0);
        AppendEntries ae = Assert.Single(c.Pending<AppendEntries>(1));
        c.Queue.Clear();

        RaftNode follower = c.Nodes[1];
        follower.Step(ae);
        RaftReady rd = follower.GetReady();
        // The acknowledgement travels in the same ready as the entries it acknowledges (host persists them first).
        Assert.Contains(rd.EntriesToPersist, e => e.Index == index);
        Assert.Contains(rd.Messages, m => m is AppendResponse { Reject: false } r && r.MatchIndex == index);
        c.Logs[1].Append(rd.EntriesToPersist);
        c.Queue.AddRange(rd.Messages);
        c.Applied[1].AddRange(rd.CommittedEntries);
        follower.Advance(rd);

        // Duplicate delivery: nothing new to persist, same acknowledgement, never re-applied.
        follower.Step(ae);
        RaftReady dup = follower.GetReady();
        Assert.Empty(dup.EntriesToPersist);
        Assert.Contains(dup.Messages, m => m is AppendResponse { Reject: false } r && r.MatchIndex == index);
        c.Queue.AddRange(dup.Messages);
        c.Applied[1].AddRange(dup.CommittedEntries);
        follower.Advance(dup);
        c.Deliver();
        c.Nodes[0].Tick();
        c.Nodes[0].Tick();
        c.Deliver();
        Assert.Single(c.Applied[1], e => e.Index == index);
    }

    [Fact]
    public void Constructor_ValidatesPersistedState()
    {
        NodeId[] ids = Ids(3);
        var conf = new RaftConfState(ids, []);
        var log = new InMemoryRaftLog();
        log.Append([new RaftEntry(1, 2, RaftEntryKind.Noop, default)]);
        var random = new SeededRandom(1);
        var cfg = new RaftConfig(ids[0], 10, 20, 2);

        Assert.Throws<ArgumentException>(() => new RaftNode(cfg, new RaftHardState(1, null, 0), conf, log, random));      // term below last log term
        Assert.Throws<ArgumentException>(() => new RaftNode(cfg, new RaftHardState(2, null, 5), conf, log, random));      // commit beyond the log
        Assert.Throws<ArgumentException>(() => new RaftNode(cfg, new RaftHardState(2, null, 0), conf, log, random, new RaftNodeOptions { Applied = 1 }));
        Assert.Throws<ArgumentException>(() => new RaftNode(cfg, default, new RaftConfState([ids[0], ids[0]], []), new InMemoryRaftLog(), random));
        Assert.Throws<ArgumentException>(() => new RaftNode(cfg, default, new RaftConfState([ids[0]], [ids[0]]), new InMemoryRaftLog(), random));
        Assert.Throws<ArgumentException>(() => new RaftNode(cfg with { HeartbeatTicks = 10 }, default, conf, new InMemoryRaftLog(), random));
        Assert.Throws<ArgumentException>(() => new RaftNode(cfg with { Self = default }, default, conf, new InMemoryRaftLog(), random));
        Assert.Throws<ArgumentException>(() => new RaftNode(cfg, default, conf, new InMemoryRaftLog(), random, new RaftNodeOptions { SnapshotChunkBytes = 0 }));
        Assert.Throws<ArgumentNullException>(() => new RaftNode(cfg, default, conf, null!, random));

        var ok = new RaftNode(cfg, new RaftHardState(2, ids[1], 1), conf, log, random);
        Assert.Equal(2UL, ok.Term);
        Assert.Equal(ids[1], ok.Vote);
        Assert.Equal(1UL, ok.CommitIndex);
        Assert.Equal(0UL, ok.AppliedIndex);
        Assert.Equal(RaftRole.Follower, ok.Role);
        Assert.Null(ok.Leader);
        RaftReady rd = ok.GetReady(); // committed entry 1 is handed out again after restart
        Assert.Equal([1UL], rd.CommittedEntries.Select(e => e.Index));
    }

    [Fact]
    public void MalformedOrMisaddressedMessages_AreIgnored()
    {
        NodeId[] ids = Ids(3);
        var conf = new RaftConfState(ids, []);
        (RaftNode node, InMemoryRaftLog log) = CreateNode(ids[1], conf, hardState: new RaftHardState(2, null, 0));
        NodeId stranger = Ids(1, seed: 77)[0];
        RaftMessage[] bad =
        [
            new AppendEntries(ids[0], ids[2], 3, 0, 0, [], 0),                                                     // not for us
            new AppendEntries(ids[1], ids[1], 3, 0, 0, [], 0),                                                     // from ourselves
            new Heartbeat(ids[0], ids[1], 0, 0),                                                                   // term 0
            new AppendEntries(ids[0], ids[1], 3, 0, 0, [new RaftEntry(2, 3, RaftEntryKind.Normal, default)], 0),   // gap
            new AppendEntries(ids[0], ids[1], 3, 0, 0, [new RaftEntry(1, 4, RaftEntryKind.Normal, default)], 0),   // entry term above leader term
            new AppendEntries(ids[0], ids[1], 3, 0, 0, [new RaftEntry(1, 3, (RaftEntryKind)9, default)], 0),       // unknown kind
            new AppendEntries(ids[0], ids[1], 3, 0, 0,
                [new RaftEntry(1, 3, RaftEntryKind.Normal, default), new RaftEntry(2, 2, RaftEntryKind.Normal, default)], 0), // decreasing terms
            new AppendEntries(ids[0], ids[1], 3, 0, 0, [null!], 0),                                               // null entry
            new InstallSnapshot(ids[0], ids[1], 3, null!, 0, default, true),                                       // no metadata
            new TimeoutNow(stranger, ids[1], 2),                                                                   // non-member
            new HeartbeatResponse(stranger, ids[1], 9),                                                            // non-member response
        ];
        foreach (RaftMessage m in bad)
            node.Step(m);
        Drain(node, log);
        Assert.Equal(0UL, log.LastIndex);
        Assert.Equal(RaftRole.Follower, node.Role);
        Assert.True(node.Term <= 3);
        Assert.Throws<ArgumentNullException>(() => node.Step(null!));
    }

    [Fact]
    public void ConfChangeCodec_RoundTripsAndRejectsMalformedPayloads()
    {
        NodeId id = Ids(1)[0];
        foreach (RaftConfChangeKind kind in Enum.GetValues<RaftConfChangeKind>())
        {
            var change = new RaftConfChange(kind, id);
            Assert.Equal(change, RaftConfChange.Decode(change.Encode()));
        }

        byte[] good = new RaftConfChange(RaftConfChangeKind.AddLearner, id).Encode();
        Assert.False(RaftConfChange.TryDecode(good.AsSpan(0, good.Length - 1), out _));
        Assert.False(RaftConfChange.TryDecode([], out _));
        byte[] badVersion = (byte[])good.Clone();
        badVersion[0] = 2;
        Assert.False(RaftConfChange.TryDecode(badVersion, out _));
        byte[] badKind = (byte[])good.Clone();
        badKind[1] = 7;
        Assert.False(RaftConfChange.TryDecode(badKind, out _));
        Assert.Throws<DfsProtocolException>(() => RaftConfChange.Decode(badKind));
    }

    [Fact]
    public void ConfState_ApplyAndCanApply()
    {
        NodeId[] ids = Ids(4);
        var conf = new RaftConfState(ids[..2], [ids[2]]);
        RaftConfState added = conf.Apply(new RaftConfChange(RaftConfChangeKind.AddLearner, ids[3]));
        Assert.Equal([ids[2], ids[3]], added.Learners);
        RaftConfState promoted = added.Apply(new RaftConfChange(RaftConfChangeKind.PromoteLearner, ids[2]));
        Assert.Equal([ids[0], ids[1], ids[2]], promoted.Voters);
        Assert.Equal([ids[3]], promoted.Learners);
        RaftConfState removed = promoted.Apply(new RaftConfChange(RaftConfChangeKind.RemoveNode, ids[0]));
        Assert.Equal([ids[1], ids[2]], removed.Voters);
        Assert.Equal(2, removed.Quorum);
        // Inapplicable changes are no-ops (deterministic on every replica).
        Assert.Same(conf, conf.Apply(new RaftConfChange(RaftConfChangeKind.AddLearner, ids[0])));
        Assert.Same(conf, conf.Apply(new RaftConfChange(RaftConfChangeKind.PromoteLearner, ids[3])));
        Assert.Same(conf, conf.Apply(new RaftConfChange(RaftConfChangeKind.RemoveNode, ids[3])));
        Assert.False(conf.CanApply(new RaftConfChange(RaftConfChangeKind.AddLearner, ids[0]), out string? reason));
        Assert.NotNull(reason);
        Assert.False(new RaftConfState([ids[0]], []).CanApply(new RaftConfChange(RaftConfChangeKind.RemoveNode, ids[0]), out _));
        Assert.True(conf.CanApply(new RaftConfChange(RaftConfChangeKind.PromoteLearner, ids[2]), out _));
    }

    [Fact]
    public void Simulation_IsDeterministicForASeed()
    {
        static IReadOnlyList<string> Run(ulong seed)
        {
            var sim = new RaftSimulation(new RaftSimulationOptions { Voters = 5, Seed = seed, SnapshotThreshold = 8 });
            sim.DropProbability(0.1);
            sim.DuplicateProbability(0.1);
            sim.ReorderMessages(true);
            for (int i = 0; i < 40; i++)
            {
                if (sim.LeaderIndex is int l)
                    sim.Propose(l, Payload($"d{i}"));
                if (i == 15)
                    sim.Partition([0, 1], [2, 3, 4]);
                if (i == 25)
                    sim.Heal();
                sim.RunTicks(5);
            }

            return [.. sim.Trace, .. All(sim).Select(n => string.Join(",", sim.AppliedLog(n).Select(AsText)))];
        }

        Assert.Equal(Run(9), Run(9));
        Assert.NotEqual(Run(9), Run(10));
    }

    [Fact]
    public void Follower_BehindCommit_AcknowledgesItsCommitIndex()
    {
        var c = new ManualCluster(3);
        c.Elect(0);
        for (int i = 0; i < 5; i++)
            c.Nodes[0].TryPropose(Payload($"c{i}"), out _);
        c.Deliver();
        ulong commit = c.Nodes[1].CommitIndex;
        Assert.True(commit >= 6);
        c.Nodes[1].Step(new AppendEntries(c.Ids[0], c.Ids[1], c.Nodes[0].Term, 1, c.Logs[0].TermAt(1), [], 1));
        c.Drain(1);
        AppendResponse resp = Assert.Single(c.Pending<AppendResponse>(0));
        Assert.False(resp.Reject);
        Assert.Equal(commit, resp.MatchIndex);
    }

    [Fact]
    public void DebugState_ReflectsNode()
    {
        var c = new ManualCluster(3);
        c.Elect(0);
        c.Deliver();
        RaftNodeDebugState d = c.Nodes[0].DebugState;
        Assert.Equal(c.Ids[0], d.Self);
        Assert.Equal(RaftRole.Leader, d.Role);
        Assert.Equal(c.Nodes[0].Term, d.Term);
        Assert.Equal(c.Ids[0], d.Vote);
        Assert.Equal(c.Ids[0], d.Leader);
        Assert.Equal(c.Nodes[0].LastIndex, d.LastIndex);
        Assert.Equal(c.Nodes[0].CommitIndex, d.Commit);
        Assert.Equal(c.Conf, d.Conf);
    }
}
