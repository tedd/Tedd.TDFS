using System.Text;
using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Testing;
using static Tedd.Tdfs.Tests.Unit.Internal.Raft.RaftTestHelpers;

namespace Tedd.Tdfs.Tests.Unit.Internal.Raft;

/// <summary>
/// Randomized fault campaigns over <see cref="RaftSimulation"/>: partitions, isolation, crashes/restarts (including
/// crashes in the middle of ready processing), drops, duplicates, reordering, compaction with snapshot transfer,
/// leadership transfers and reads. Safety must hold on every seed; after healing every fault the cluster must make
/// progress (liveness under stable conditions).
/// </summary>
public sealed class RaftSafetyCampaignTests
{
    /// <summary>Seeds per cluster size: 300 by default; override with TDFS_RAFT_CAMPAIGN_SEEDS for longer exploratory runs.</summary>
    private static readonly int Seeds =
        int.TryParse(Environment.GetEnvironmentVariable("TDFS_RAFT_CAMPAIGN_SEEDS"), out int n) && n > 0 ? n : 300;

    /// <summary>First seed (TDFS_RAFT_CAMPAIGN_FIRST_SEED), so exploratory runs can cover fresh ranges.</summary>
    private static readonly ulong FirstSeed =
        ulong.TryParse(Environment.GetEnvironmentVariable("TDFS_RAFT_CAMPAIGN_FIRST_SEED"), out ulong f) && f > 0 ? f : 1;

    [Theory]
    [InlineData(3)]
    [InlineData(5)]
    public void RandomFaults_SafetyHolds_AndClusterRecoversAfterHeal(int voters)
    {
        var failures = new List<string>();
        int totalCommitted = 0;
        for (ulong seed = FirstSeed; seed < FirstSeed + (ulong)Seeds; seed++)
        {
            string? failure = RunCampaign(voters, seed, SafetyMutations.None, requireLiveness: true, out int committed);
            totalCommitted += committed;
            if (failure is not null)
            {
                failures.Add(failure);
                if (failures.Count >= 3)
                    break;
            }
        }

        Assert.True(failures.Count == 0, string.Join("\n\n", failures));
        Assert.True(totalCommitted > Seeds * 5, $"the campaign committed too little to be meaningful ({totalCommitted})");
    }

    [Fact]
    public void NegativeControl_QuorumFromReachableNodes_ProducesSafetyViolation()
    {
        // MUT-004: computing the majority from reachable voters lets both sides of a partition commit.
        var mutation = new SafetyMutations { QuorumFromReachableNodes = true };
        RaftSimulation mutated = RunSplitBrainSchedule(mutation);
        Assert.NotNull(mutated.CheckSafety());

        // The same schedule without the mutation is safe and the minority never commits.
        RaftSimulation control = RunSplitBrainSchedule(SafetyMutations.None);
        AssertSafe(control);
        Assert.All(All(control), i => Assert.DoesNotContain(control.AppliedLog(i).Select(AsText), t => t.StartsWith("minority", StringComparison.Ordinal)));
    }

    /// <summary>Partitions a 5-voter cluster 2/3 and keeps proposing on whichever side has a leader, then heals.</summary>
    private static RaftSimulation RunSplitBrainSchedule(SafetyMutations mutations)
    {
        var sim = new RaftSimulation(new RaftSimulationOptions { Voters = 5, Seed = 3, Mutations = mutations });
        int leader = WaitForStableLeader(sim);
        int buddy = (leader + 1) % 5;
        int[] minority = [leader, buddy];
        int[] majority = [.. All(sim).Where(i => i != leader && i != buddy)];
        sim.Partition(minority, majority);
        for (int t = 0; t < 400; t++)
        {
            foreach (int i in All(sim))
            {
                if (sim.Node(i).Role == RaftRole.Leader)
                    sim.Propose(i, Payload($"{(minority.Contains(i) ? "minority" : "majority")}-{t}"));
            }

            sim.RunTicks(1);
        }

        sim.Heal();
        sim.RunTicks(300);
        return sim;
    }

    [Fact]
    public void NegativeControl_RandomCampaignDetectsReachableQuorumMutation()
    {
        var mutation = new SafetyMutations { QuorumFromReachableNodes = true };
        int detected = 0;
        for (ulong seed = 1; seed <= 100 && detected == 0; seed++)
        {
            if (RunCampaign(5, seed, mutation, requireLiveness: false, out _) is not null)
                detected++;
        }

        Assert.True(detected > 0, "no seed exposed the reachable-quorum mutation");
    }

    /// <summary>Adds a learner (a fresh node), promotes a learner or removes a member at the leader.</summary>
    private static void ChangeMembership(RaftSimulation sim, int leader, SeededRandom rnd, int voters)
    {
        RaftConfState conf = sim.Node(leader).ConfState;
        switch (rnd.Next(3))
        {
            case 0 when sim.NodeCount < voters + 4:
            {
                int added = sim.AddNode();
                sim.ProposeConfChange(leader, new RaftConfChange(RaftConfChangeKind.AddLearner, sim.IdOf(added)));
                break;
            }
            case 1 when conf.Learners.Count > 0:
                sim.ProposeConfChange(leader, new RaftConfChange(RaftConfChangeKind.PromoteLearner, conf.Learners[rnd.Next(conf.Learners.Count)]));
                break;
            case 2 when conf.Voters.Count + conf.Learners.Count > 2:
            {
                NodeId[] members = [.. conf.Voters, .. conf.Learners];
                sim.ProposeConfChange(leader, new RaftConfChange(RaftConfChangeKind.RemoveNode, members[rnd.Next(members.Length)]));
                break;
            }
        }
    }

    /// <summary>Runs one randomized campaign; returns a failure description or null.</summary>
    private static string? RunCampaign(int voters, ulong seed, SafetyMutations mutations, bool requireLiveness, out int committed)
    {
        var rnd = new SeededRandom(seed * 7919 + (ulong)voters);
        var options = new RaftSimulationOptions
        {
            Voters = voters,
            Learners = rnd.Next(2),
            Seed = seed,
            Mutations = mutations,
            PreVote = seed % 3 != 0,
            CheckQuorum = seed % 4 != 0,
            SnapshotThreshold = 10 + rnd.Next(30),
            SnapshotChunkBytes = 16 + rnd.Next(200),
            MaxEntriesPerAppend = 1 + rnd.Next(16),
            MaxInflightAppends = 1 + rnd.Next(8),
            TraceMessages = Environment.GetEnvironmentVariable("TDFS_RAFT_TRACE_MESSAGES") == "1",
        };
        var sim = new RaftSimulation(options);
        sim.DropProbability(rnd.Next(4) * 0.05);
        sim.DuplicateProbability(rnd.Next(3) * 0.05);
        sim.ReorderMessages(rnd.Next(2) == 1);
        int proposals = 0;
        committed = 0;

        string Fail(string what)
        {
            // Set TDFS_RAFT_TRACE_DIR to keep the complete trace of failing campaigns for replay analysis.
            string? dir = Environment.GetEnvironmentVariable("TDFS_RAFT_TRACE_DIR");
            if (!string.IsNullOrEmpty(dir))
            {
                Directory.CreateDirectory(dir);
                File.WriteAllLines(Path.Combine(dir, $"raft-v{voters}-s{seed}-m{(mutations.Any ? 1 : 0)}.log"), [what, .. sim.Trace]);
            }

            return $"voters={voters} seed={seed} mutations={mutations.Any}: {what}\n{string.Join("\n", sim.Trace.Skip(Math.Max(0, sim.Trace.Count - 80)))}";
        }

        try
        {
            for (int step = 0; step < 60; step++)
            {
                switch (rnd.Next(16))
                {
                    case 14:
                    case 15:
                        if (sim.LeaderIndex is int cl)
                            ChangeMembership(sim, cl, rnd, voters);
                        break;
                    case 0:
                    {
                        int[] order = [.. Enumerable.Range(0, sim.NodeCount)];
                        rnd.Shuffle(order);
                        int split = 1 + rnd.Next(order.Length - 1);
                        sim.Partition(order[..split], order[split..]);
                        break;
                    }
                    case 1:
                    case 2:
                        sim.Heal();
                        break;
                    case 3:
                        sim.Crash(rnd.Next(sim.NodeCount));
                        break;
                    case 4:
                    case 5:
                    {
                        int[] down = [.. Enumerable.Range(0, sim.NodeCount).Where(i => !sim.IsUp(i))];
                        if (down.Length > 0)
                            sim.Restart(down[rnd.Next(down.Length)]);
                        break;
                    }
                    case 6:
                        sim.Isolate(rnd.Next(sim.NodeCount));
                        break;
                    case 7:
                        sim.ArmCrash(rnd.Next(sim.NodeCount), (RaftCrashPoint)rnd.Next(3));
                        break;
                    case 8:
                        if (sim.LeaderIndex is int l)
                            sim.TransferLeadership(l, rnd.Next(sim.NodeCount));
                        break;
                    case 9:
                        sim.ReadIndex(sim.LeaderIndex ?? rnd.Next(sim.NodeCount), (ulong)step + 1);
                        break;
                    default:
                    {
                        int target = sim.LeaderIndex ?? rnd.Next(sim.NodeCount);
                        int count = 1 + rnd.Next(4);
                        for (int k = 0; k < count; k++)
                        {
                            if (sim.Propose(target, Encoding.UTF8.GetBytes($"s{seed}-p{proposals}")))
                                proposals++;
                        }

                        break;
                    }
                }

                sim.RunTicks(1 + rnd.Next(15));
                string? violation = sim.CheckSafety();
                if (violation is not null)
                    return Fail(violation);
            }

            if (!requireLiveness)
                return null;

            // Stable conditions: heal everything and require progress.
            sim.Heal();
            sim.DropProbability(0);
            sim.DuplicateProbability(0);
            sim.ReorderMessages(false);
            sim.DisarmCrashes();
            for (int i = 0; i < sim.NodeCount; i++)
            {
                if (!sim.IsUp(i))
                    sim.Restart(i);
            }

            // Only current members (per the current leader) must converge; removed nodes stop receiving the log.
            int[] Members()
            {
                RaftConfState conf = sim.LeaderIndex is int ml ? sim.Node(ml).ConfState : sim.BootstrapConf;
                return [.. conf.Voters.Concat(conf.Learners).Select(sim.IndexOf).Where(i => i >= 0).Order()];
            }

            // A proposal accepted by a leader that is deposed before committing it is legitimately lost: retry.
            bool converged = false;
            for (int attempt = 0; attempt < 10 && !converged; attempt++)
            {
                byte[] marker = Encoding.UTF8.GetBytes($"s{seed}-final{attempt}");
                if (!sim.TryRunUntil(() => sim.LeaderIndex is int l && sim.Propose(l, marker), 3000))
                    return Fail("no leader accepted a proposal after healing");
                converged = sim.TryRunUntil(() =>
                    Members().All(i => sim.AppliedLog(i).Any(e => e.AsSpan().SequenceEqual(marker))), 500);
            }

            if (!converged)
            {
                return Fail("nodes did not converge after healing: " +
                    string.Join(", ", Enumerable.Range(0, sim.NodeCount).Select(i => $"n{i}:{sim.Node(i).Role}/t{sim.Node(i).Term}/c{sim.Node(i).CommitIndex}/a{sim.AppliedIndex(i)}")) +
                    $" members [{string.Join(",", Members())}]");
            }

            sim.RunTicks(20);
            int[] members = Members();
            IReadOnlyList<byte[]> reference = sim.AppliedLog(members[0]);
            foreach (int i in members)
            {
                IReadOnlyList<byte[]> other = sim.AppliedLog(i);
                if (other.Count != reference.Count || !other.Zip(reference).All(p => p.First.AsSpan().SequenceEqual(p.Second)))
                    return Fail($"n{i} applied a different sequence than n{members[0]}");
            }

            committed = reference.Count;
            string? final = sim.CheckSafety();
            return final is null ? null : Fail(final);
        }
        catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
        {
            return Fail($"exception: {ex}");
        }
    }
}
