using System.Globalization;
using System.Text;
using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Models.Con;

/// <summary>
/// CON — consensus, authority and metadata recovery: bounded formal-model checks (level M).
/// FSD §§3.2, 7, 8, 13; invariants INV-01, INV-03, INV-04, INV-05, INV-11, INV-18.
/// </summary>
public sealed class ConModelsTests
{
    /// <summary>
    /// The bounded configurations explored exhaustively for CON-001. Each row is a published finite domain
    /// (test plan §1: "exhaustive" means exhaustive over the stated bounds): number of voters and learners,
    /// PreVote/CheckQuorum, and budgets for elections, heartbeat rounds, proposals, drops, duplications, crashes and
    /// single-server membership changes. All interleavings of message deliveries within those budgets are explored, so
    /// the configuration rows cover elections racing an uncommitted configuration change ("no term/configuration has
    /// conflicting authoritative leaders").
    /// </summary>
    public static TheoryData<string, int, bool, bool, int, int, int, int, int, int, int, int> ElectionModels => new()
    {
        // name, voters, preVote, checkQuorum, timeouts, heartbeats, proposals, drops, duplicates, crashes, learners, confChanges
        { "3v-concurrent-candidates", 3, false, false, 2, 0, 0, 0, 0, 0, 0, 0 },
        { "3v-prevote-concurrent-candidates", 3, true, false, 2, 0, 0, 0, 0, 0, 0, 0 },
        { "3v-three-elections", 3, false, false, 3, 0, 0, 0, 0, 0, 0, 0 },
        { "3v-election-with-proposal-and-loss", 3, false, false, 2, 1, 1, 1, 0, 0, 0, 0 },
        { "3v-duplicate-vote-messages", 3, false, false, 2, 0, 0, 0, 2, 0, 0, 0 },
        { "3v-crash-restart-voter", 3, false, false, 2, 0, 0, 0, 1, 1, 0, 0 },
        { "3v-prevote-checkquorum-lease", 3, true, true, 3, 1, 0, 0, 0, 0, 0, 0 },
        { "5v-split-vote", 5, false, false, 2, 0, 0, 0, 0, 0, 0, 0 },
        { "3v1l-promote-learner-racing-elections", 3, false, false, 2, 0, 0, 0, 0, 0, 1, 1 },
        { "3v-remove-voter-racing-elections", 3, true, false, 2, 0, 0, 0, 0, 0, 0, 1 },
    };

    [Theory, Catalog("CON-001", "M", "INV-01", "INV-03", "INV-04", "INV-05", "INV-11", "INV-18")]
    [MemberData(nameof(ElectionModels))]
    public void CON_001_ElectionSafety(string model, int voters, bool preVote, bool checkQuorum, int timeouts, int heartbeats, int proposals,
        int drops, int duplicates, int crashes, int learners, int confChanges)
    {
        var bounds = new RaftModelBounds(voters, preVote, checkQuorum, timeouts, heartbeats, proposals, drops, duplicates, crashes, learners, confChanges);
        ulong seed = SeededRandom.DeriveSeed(0xC0_0001, model);

        RaftModelState initial = RaftModelCluster.Initial(bounds, seed);
        Assert.Null(initial.Violation);

        // The budgets make the reachable state space finite; the depth bound is far above the longest possible schedule
        // so that only the budgets limit the exploration and the result can report completion.
        const int maxDepth = 400;
        const long maxStates = 3_000_000;
        ModelCheckResult result = ModelChecker.Explore(
            initial,
            state => RaftModelCluster.Successors(bounds, seed, state),
            state => state.Violation,
            state => state.Key,
            maxDepth,
            maxStates);

        Report(model, bounds, result);

        // Safety: no reachable state violates election safety, single vote per term, log matching, committed-entry
        // stability, leader completeness or state-machine safety.
        result.AssertNoViolation();

        // Evidence integrity: a cut-off exploration is never a pass (HAR-017, VER-04).
        result.AssertCompleted();
        Assert.True(result.StatesExplored > 1, "The exploration must leave the initial state.");

        // The explored domain must actually contain elections: a schedule exists in which a leader is elected.
        Assert.True(ContainsElection(bounds, seed), $"Model {model}: no explored schedule elected a leader; the bounds are vacuous.");

        // Configuration rows must actually reach a configuration change: an elected leader accepts the proposal.
        if (confChanges > 0)
            Assert.True(ContainsConfigurationChange(bounds, seed), $"Model {model}: no explored schedule proposed a configuration change; the bounds are vacuous.");
    }

    public static TheoryData<int, ulong> RandomizedSeeds()
    {
        var data = new TheoryData<int, ulong>();
        foreach (int voters in new[] { 3, 5 })
        {
            for (ulong seed = 1; seed <= 12; seed++)
                data.Add(voters, seed);
        }

        return data;
    }

    /// <summary>
    /// Larger randomized campaign (test plan §8.4) over the same safety model using <see cref="RaftSimulation"/>:
    /// partitions, crashes/restarts from persisted state, drops, duplicates and reordering for many seeds. Complements the
    /// exhaustive bounded rows above; it is not presented as exhaustive.
    /// </summary>
    [Theory, Catalog("CON-001", "M", "INV-01", "INV-03", "INV-04", "INV-05", "INV-11", "INV-18")]
    [MemberData(nameof(RandomizedSeeds))]
    public void CON_001_ElectionSafety_RandomizedSchedules(int voters, ulong seed)
    {
        var sim = new RaftSimulation(voters, seed);
        var random = new SeededRandom(SeededRandom.DeriveSeed(seed, "con-001-schedule"));
        sim.DropProbability(0.05);
        sim.DuplicateProbability(0.05);
        sim.ReorderMessages(true);

        var crashed = new SortedSet<int>();
        var leaderOfTerm = new Dictionary<ulong, int>();
        int proposals = 0;

        for (int step = 0; step < 300; step++)
        {
            switch (random.Next(12))
            {
                case 0:
                {
                    // Random two-way partition (every shape, including a lone node and minority/majority splits).
                    var left = new List<int>();
                    var right = new List<int>();
                    for (int i = 0; i < voters; i++)
                        (random.Next(2) == 0 ? left : right).Add(i);
                    sim.Partition([.. left], [.. right]);
                    break;
                }

                case 1:
                    sim.Heal();
                    break;

                case 2:
                {
                    int node = random.Next(voters);
                    if (!crashed.Contains(node) && crashed.Count < voters - 1)
                    {
                        sim.Crash(node);
                        crashed.Add(node);
                    }

                    break;
                }

                case 3:
                    if (crashed.Count > 0)
                    {
                        int node = crashed.ElementAt(random.Next(crashed.Count));
                        sim.Restart(node);
                        crashed.Remove(node);
                    }

                    break;

                case 4:
                    if (sim.LeaderIndex is int leader && !crashed.Contains(leader))
                        sim.Propose(leader, Encoding.ASCII.GetBytes(string.Create(CultureInfo.InvariantCulture, $"cmd-{seed}-{proposals++}")));
                    break;

                default:
                    sim.RunTicks(1 + random.Next(25));
                    break;
            }

            // Safety is checked during the failure schedule, not only after convergence (VER-05).
            string? violation = sim.CheckSafety();
            Assert.True(violation is null, $"seed {seed}, step {step}: {violation}{Environment.NewLine}{string.Join(Environment.NewLine, sim.Trace.TakeLast(40))}");

            for (int i = 0; i < voters; i++)
            {
                if (crashed.Contains(i))
                    continue;
                RaftNode core = sim.Nodes[i];
                if (core.Role != RaftRole.Leader)
                    continue;
                if (leaderOfTerm.TryGetValue(core.Term, out int other) && other != i)
                    Assert.Fail($"seed {seed}, step {step}: term {core.Term} has two leaders n{other} and n{i}");
                leaderOfTerm[core.Term] = i;
            }
        }

        // Liveness under restored prerequisites (INV-17 assumptions stated): heal, restart everything, and a leader is
        // elected and replicates a final command to every voter within a bounded number of ticks.
        sim.Heal();
        foreach (int node in crashed)
            sim.Restart(node);
        sim.DropProbability(0);
        sim.DuplicateProbability(0);
        sim.RunUntil(() => sim.LeaderIndex is not null, 5_000);
        Assert.True(sim.LeaderIndex is not null, $"seed {seed}: no leader within 5000 ticks after healing");
        byte[] final = Encoding.ASCII.GetBytes("final-" + seed.ToString(CultureInfo.InvariantCulture));
        Assert.True(sim.Propose(sim.LeaderIndex!.Value, final), "the elected leader must accept a proposal");
        sim.RunUntil(() => Enumerable.Range(0, voters).All(i => sim.AppliedLog(i).Count > 0 && sim.AppliedLog(i)[^1].AsSpan().SequenceEqual(final)), 5_000);
        for (int i = 0; i < voters; i++)
        {
            IReadOnlyList<byte[]> applied = sim.AppliedLog(i);
            Assert.True(applied.Count > 0 && applied[^1].AsSpan().SequenceEqual(final), $"seed {seed}: n{i} did not apply the final command");
            Assert.Equal(sim.AppliedLog(0).Count, applied.Count);
            for (int k = 0; k < applied.Count; k++)
                AssertEx.BytesEqual(sim.AppliedLog(0)[k], applied[k], $"seed {seed}: applied command {k} differs on n{i}");
        }

        Assert.Null(sim.CheckSafety());
    }

    private static bool ContainsElection(RaftModelBounds bounds, ulong seed)
    {
        // A short canonical schedule: n0 times out and every message is delivered in FIFO order until quiescent.
        var cluster = new RaftModelCluster(bounds, seed);
        cluster.Apply("timeout n0");
        for (int guard = 0; guard < 200 && !cluster.AnyLeader; guard++)
        {
            string? deliver = cluster.EnabledActions().FirstOrDefault(a => a.StartsWith("deliver ", StringComparison.Ordinal));
            if (deliver is null)
                break;
            cluster.Apply(deliver);
        }

        return cluster.Violation is null && cluster.AnyLeader;
    }

    private static bool ContainsConfigurationChange(RaftModelBounds bounds, ulong seed)
    {
        // Elect n0 as in ContainsElection, then let it propose the first enabled membership change and deliver everything.
        var cluster = new RaftModelCluster(bounds, seed);
        cluster.Apply("timeout n0");
        for (int guard = 0; guard < 200 && !cluster.AnyLeader; guard++)
        {
            string? deliver = cluster.EnabledActions().FirstOrDefault(a => a.StartsWith("deliver ", StringComparison.Ordinal));
            if (deliver is null)
                break;
            cluster.Apply(deliver);
        }

        string? change = cluster.EnabledActions().FirstOrDefault(a => a.StartsWith("promote ", StringComparison.Ordinal) || a.StartsWith("remove ", StringComparison.Ordinal));
        if (change is null)
            return false;
        cluster.Apply(change);
        for (int guard = 0; guard < 200; guard++)
        {
            string? deliver = cluster.EnabledActions().FirstOrDefault(a => a.StartsWith("deliver ", StringComparison.Ordinal));
            if (deliver is null)
                break;
            cluster.Apply(deliver);
        }

        return cluster.AcceptedConfChanges > 0 && cluster.Violation is null && cluster.CheckInvariants() is null;
    }

    private static void Report(string model, RaftModelBounds bounds, ModelCheckResult result)
    {
        ITestOutputHelper? output = TestContext.Current.TestOutputHelper;
        output?.WriteLine($"CON-001 model '{model}': {bounds}");
        output?.WriteLine($"  completed={result.Completed} states={result.StatesExplored} maxDepth={result.MaxDepth}");
        foreach (string bound in result.Bounds)
            output?.WriteLine("  bound: " + bound);
        if (result.Violation is not null)
            output?.WriteLine("  violation: " + result.Violation);
    }
}
