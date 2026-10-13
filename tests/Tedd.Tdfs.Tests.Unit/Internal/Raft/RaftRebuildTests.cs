using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Testing;
using static Tedd.Tdfs.Tests.Unit.Internal.Raft.RaftTestHelpers;

namespace Tedd.Tdfs.Tests.Unit.Internal.Raft;

/// <summary>
/// Members that lost their durable state and rebuild it from a leader (OFF-020 empty-disk replacement,
/// <see cref="RaftNodeOptions.Rebuilding"/>): they must not vote before they hold what they may have acknowledged before
/// the loss, and they must vote again once a live leader brought them up to date.
/// </summary>
public sealed class RaftRebuildTests
{
    /// <summary>Replaces node <paramref name="i"/> by an empty, rebuilding member with the same identity.</summary>
    private static RaftNode ReplaceWithEmptyDisk(ManualCluster c, int i)
    {
        c.Logs[i] = new InMemoryRaftLog();
        c.Nodes[i] = new RaftNode(new RaftConfig(c.Ids[i], 10, 20, 2), default, c.Conf, c.Logs[i], new SeededRandom(99),
            new RaftNodeOptions { Rebuilding = true });
        c.Queue.RemoveAll(m => m.To == c.Ids[i] || m.From == c.Ids[i]);
        return c.Nodes[i];
    }

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

    /// <summary>
    /// Entries 4..5 are committed by nodes 0 and 2 only. Node 2 then loses its disk and node 0 is gone for good. A delayed
    /// copy of an early append of node 0 (commit index 3) reaches the empty node 2 first. That append alone must not end
    /// the rebuild: otherwise node 2 votes again with entries 4..5 missing, and nodes 1 and 2 elect a leader that commits
    /// a different entry at index 4 (a committed entry is lost).
    /// </summary>
    [Fact]
    public void StaleAppendAfterDiskLoss_DoesNotEndTheRebuild()
    {
        var c = new ManualCluster(3);
        c.Elect(0);
        c.Deliver();
        Assert.True(c.Nodes[0].TryPropose(Payload("a"), out _));
        Assert.True(c.Nodes[0].TryPropose(Payload("b"), out _));
        c.Deliver();
        ulong staleTerm = c.Nodes[0].Term;
        ulong staleCommit = c.Nodes[0].CommitIndex;
        RaftEntry[] early = [.. c.Logs[0].Entries(1, staleCommit + 1, int.MaxValue)];

        // Node 1 is cut off; nodes 0 and 2 commit two more entries.
        Assert.True(c.Nodes[0].TryPropose(Payload("c"), out _));
        Assert.True(c.Nodes[0].TryPropose(Payload("d"), out ulong last));
        c.Deliver(drop: m => m.To == c.Ids[1] || m.From == c.Ids[1]);
        Assert.Equal(last, c.Nodes[0].CommitIndex);
        Assert.True(c.Nodes[1].LastIndex < last);

        // Node 2 restarts with an empty disk; node 0 is gone.
        RaftNode rebuilt = ReplaceWithEmptyDisk(c, 2);
        c.Queue.RemoveAll(m => m.To == c.Ids[0] || m.From == c.Ids[0]);

        // A delayed append of node 0 from before the loss arrives first.
        rebuilt.Step(new AppendEntries(c.Ids[0], c.Ids[2], staleTerm, 0, 0, early, staleCommit));
        c.Drain(2);
        c.Queue.RemoveAll(m => m.To == c.Ids[0]);
        Assert.Equal(staleCommit, rebuilt.CommitIndex);
        Assert.True(rebuilt.IsRebuilding, "a single (possibly stale) append must not end the rebuild");

        // Nodes 1 and 2 keep running: no leader may emerge without entries 4..5.
        for (int t = 0; t < 400; t++)
        {
            c.Nodes[1].Tick();
            rebuilt.Tick();
            c.Deliver(drop: m => m.To == c.Ids[0] || m.From == c.Ids[0]);
        }

        Assert.NotEqual(RaftRole.Leader, c.Nodes[1].Role);
        Assert.NotEqual(RaftRole.Leader, rebuilt.Role);
        Assert.True(rebuilt.IsRebuilding);
        AssertAppliedConsistent(c);
    }

    /// <summary>
    /// A member that lost its disk while the leader is alive: the leader notices the member no longer holds what it
    /// acknowledged (heartbeat commit beyond its log), probes it again from what it has, and the member votes again once it
    /// caught up with the leader's commit index and followed the leader for a full election timeout.
    /// </summary>
    [Fact]
    public void RebuiltMember_CatchesUpFromTheLiveLeader_AndVotesAgain()
    {
        var c = new ManualCluster(3);
        c.Elect(0);
        c.Deliver();
        for (int i = 0; i < 5; i++)
            Assert.True(c.Nodes[0].TryPropose(Payload("e" + i), out _));
        c.Deliver();
        ulong commit = c.Nodes[0].CommitIndex;

        RaftNode rebuilt = ReplaceWithEmptyDisk(c, 2);
        Assert.True(rebuilt.IsRebuilding);
        int ticks = 0;
        for (; ticks < 400 && rebuilt.IsRebuilding; ticks++)
        {
            foreach (RaftNode n in c.Nodes)
                n.Tick();
            c.Deliver();
        }

        Assert.False(rebuilt.IsRebuilding, "a member caught up with a live leader must vote again");
        Assert.True(ticks >= 20, $"the rebuild ended after {ticks} ticks, before a full election timeout of contact");
        Assert.True(rebuilt.CommitIndex >= commit);
        Assert.Equal(c.Nodes[0].LastIndex, rebuilt.LastIndex);
        Assert.Equal(RaftRole.Leader, c.Nodes[0].Role);
        AssertAppliedConsistent(c);
    }

    /// <summary>A rebuilding member refuses votes, including leadership-transfer votes that bypass the leader lease.</summary>
    [Fact]
    public void RebuildingMember_RefusesEveryVote()
    {
        var c = new ManualCluster(3);
        c.Elect(0);
        c.Deliver();
        RaftNode rebuilt = ReplaceWithEmptyDisk(c, 2);
        rebuilt.Step(new VoteRequest(c.Ids[1], c.Ids[2], 7, PreVote: false, LastLogIndex: 0, LastLogTerm: 0, LeadershipTransfer: true));
        c.Drain(2);
        VoteResponse reply = Assert.Single(c.Queue.OfType<VoteResponse>(), m => m.From == c.Ids[2]);
        Assert.True(reply.Reject);
        Assert.True(rebuilt.IsRebuilding);
    }
}
