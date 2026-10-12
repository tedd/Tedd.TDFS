using System.Text;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Models.Mod;

/// <summary>
/// MOD — bounded formal models and protocol obligations (test plan §7.29). Each test explores an abstract transition
/// system of one protocol under explicit bounds with <see cref="ModelChecker"/>, requires a completed exploration without
/// violation, and runs deliberately broken variants (negative controls) whose exploration must find a violation.
/// MOD-001 additionally drives the real <c>RaftNode</c> through <see cref="RaftSimulation"/>; MOD-012 writes the
/// refinement/bounds report.
/// </summary>
public sealed class ModModelsTests
{
    /// <summary>Bounds of the real-RaftNode exploration in MOD-001 (also listed in the MOD-012 report).</summary>
    internal const int RealRaftSeedsPerBlock = 25;
    internal const int RealRaftSteps = 60;
    internal const int RealRaftMaxTicksPerStep = 40;

    internal static readonly string[] RealRaftExplorationBounds =
    [
        $"RaftSimulation over the production RaftNode: voters in {{3,5}}, 4 seed blocks x {RealRaftSeedsPerBlock} seeds per voter count (200 seeds), " +
        $"{RealRaftSteps} randomized steps per seed (propose, partition into random groups including leader isolation and 1+1+1 splits, heal, crash, restart), " +
        $"1..{RealRaftMaxTicksPerStep} ticks per step, 2% drop, 2% duplicate, reordering enabled; CheckSafety() after every step; liveness only after heal/restart with no drops.",
        "Negative control: SafetyMutations.QuorumFromReachableNodes with an isolated leader must be detected by CheckSafety() for at least one of 10 seeds.",
    ];

    // ------------------------------------------------------------------ MOD-001

    [Fact, Catalog("MOD-001", "M", "INV-01", "INV-18")]
    public void MOD_001_ConsensusSafetyModel()
    {
        var spec = new RaftSafetySpec();
        ModHelpers.AssertRefinementMapWellFormed(spec);
        SpecRun run = ModHelpers.AssertCorrectVariantHolds(spec, minimumStates: 1_000);
        Assert.Contains("Elect", run.ActionKinds);
        Assert.Contains("AdvanceCommit", run.ActionKinds);

        IReadOnlyList<SpecRun> broken = ModHelpers.AssertNegativeControlsDetected(spec);
        SpecRun noUpToDate = broken.Single(b => b.Variant == RaftSafetySpec.VoteWithoutUpToDateCheck);
        Assert.Contains("leader completeness", noUpToDate.Result.Violation!, StringComparison.Ordinal);
        Assert.Contains(noUpToDate.Result.CounterexampleTrace!, a => a.StartsWith("AdvanceCommit", StringComparison.Ordinal));
        SpecRun doubleVote = broken.Single(b => b.Variant == RaftSafetySpec.DoubleVoteInTerm);
        Assert.Contains("election safety", doubleVote.Result.Violation!, StringComparison.Ordinal);
    }

    public static TheoryData<int, int> RealRaftSeedBlocks => new()
    {
        { 3, 0 }, { 3, 1 }, { 3, 2 }, { 3, 3 },
        { 5, 0 }, { 5, 1 }, { 5, 2 }, { 5, 3 },
    };

    /// <summary>
    /// MOD-001 against the implementation: the production <c>RaftNode</c> core is driven through many seeded schedules
    /// with partitions (including leader isolation and 1+1+1 splits), crashes, restarts, drops, duplicates and reordering.
    /// Election safety, log matching, leader completeness and state-machine safety (RaftSimulation.CheckSafety) and an
    /// independent applied-log prefix check must hold after every step; once healed, all nodes converge.
    /// </summary>
    [Theory, MemberData(nameof(RealRaftSeedBlocks)), Catalog("MOD-001", "M", "INV-01", "INV-18")]
    public void MOD_001_ConsensusSafetyModel_RealRaftNode(int voters, int block)
    {
        for (int i = 0; i < RealRaftSeedsPerBlock; i++)
        {
            ulong seed = (ulong)(voters * 10_000 + block * RealRaftSeedsPerBlock + i + 1);
            ExploreRealRaft(voters, seed);
        }
    }

    /// <summary>MOD-001 negative control on the implementation: a quorum counted from reachable nodes must be caught.</summary>
    [Fact, Catalog("MOD-001", "M", "INV-01", "INV-18")]
    public void MOD_001_ConsensusSafetyModel_RealRaftNodeMutantDetected()
    {
        int detected = 0;
        var reports = new List<string>();
        for (ulong seed = 1; seed <= 10; seed++)
        {
            var sim = new RaftSimulation(3, seed, mutations: new SafetyMutations { QuorumFromReachableNodes = true });
            sim.RunUntil(() => sim.LeaderIndex is not null, 3_000);
            Assert.True(sim.LeaderIndex is not null, $"seed {seed}: no initial leader");
            int oldLeader = sim.LeaderIndex!.Value;
            int[] others = Enumerable.Range(0, 3).Where(n => n != oldLeader).ToArray();
            sim.Partition([oldLeader], others);
            sim.Propose(oldLeader, Encoding.UTF8.GetBytes($"minority-{seed}"));
            sim.RunTicks(400);
            if (sim.LeaderIndex is int majorityLeader && majorityLeader != oldLeader)
                sim.Propose(majorityLeader, Encoding.UTF8.GetBytes($"majority-{seed}"));
            for (int k = 0; k < 400 && sim.CheckSafety() is null; k += 20)
                sim.RunTicks(20);
            sim.Heal();
            string? violation = sim.CheckSafety();
            for (int k = 0; k < 400 && violation is null; k += 20)
            {
                sim.RunTicks(20);
                violation = sim.CheckSafety();
            }

            if (violation is not null)
            {
                detected++;
                reports.Add($"seed {seed}: {violation}");
            }
        }

        foreach (string line in reports)
            ModHelpers.Log(line);
        Assert.True(detected >= 1, "SafetyMutations.QuorumFromReachableNodes survived: RaftSimulation.CheckSafety() reported no violation for any of 10 seeds with an isolated leader.");
    }

    private static void ExploreRealRaft(int voters, ulong seed)
    {
        var sim = new RaftSimulation(voters, seed);
        var random = new SeededRandom(seed * 0x9E37_79B9_7F4A_7C15UL + 7);
        var crashed = new HashSet<int>();
        int proposed = 0;
        string context = $"voters={voters} seed={seed}";
        try
        {
            sim.DropProbability(0.02);
            sim.DuplicateProbability(0.02);
            sim.ReorderMessages(true);
            sim.RunUntil(() => sim.LeaderIndex is not null, 3_000);
            Assert.True(sim.LeaderIndex is not null, $"{context}: no leader within 3000 ticks on a connected network");
            AssertRaftSafe(sim, voters, crashed, context + " after initial election");

            for (int step = 0; step < RealRaftSteps; step++)
            {
                switch (random.Next(8))
                {
                    case 0:
                    case 1:
                    case 2:
                    {
                        int target = sim.LeaderIndex is int l && random.Next(4) != 0 ? l : random.Next(voters);
                        if (!crashed.Contains(target))
                            sim.Propose(target, Encoding.UTF8.GetBytes($"{seed}:{proposed++}"));
                        break;
                    }

                    case 3:
                        sim.Partition(RandomGroups(random, voters, sim.LeaderIndex));
                        break;
                    case 4:
                        sim.Heal();
                        break;
                    case 5:
                    {
                        int victim = random.Next(voters);
                        if (crashed.Count < voters - 1 && crashed.Add(victim))
                            sim.Crash(victim);
                        break;
                    }

                    case 6:
                        if (crashed.Count > 0)
                        {
                            int back = crashed.Order().ElementAt(random.Next(crashed.Count));
                            crashed.Remove(back);
                            sim.Restart(back);
                        }

                        break;
                    default:
                        sim.RunTicks(50 + random.Next(150));
                        break;
                }

                sim.RunTicks(1 + random.Next(RealRaftMaxTicksPerStep));
                AssertRaftSafe(sim, voters, crashed, $"{context} step {step}");
            }

            // Liveness only under restored assumptions: full connectivity, every node running, no message loss.
            sim.Heal();
            foreach (int node in crashed.Order().ToArray())
                sim.Restart(node);
            crashed.Clear();
            sim.DropProbability(0);
            sim.DuplicateProbability(0);
            sim.RunUntil(() => sim.LeaderIndex is not null, 5_000);
            Assert.True(sim.LeaderIndex is not null, $"{context}: no leader within 5000 ticks after heal/restart");
            byte[] marker = Encoding.UTF8.GetBytes($"{seed}:final-marker");
            bool accepted = false;
            for (int attempt = 0; attempt < 50 && !accepted; attempt++)
            {
                if (sim.LeaderIndex is int leader)
                    accepted = sim.Propose(leader, marker);
                if (!accepted)
                    sim.RunTicks(50);
            }

            Assert.True(accepted, $"{context}: the healed cluster's leader never accepted a proposal");
            sim.RunUntil(() => Enumerable.Range(0, voters).All(n => sim.AppliedLog(n).Any(e => e.AsSpan().SequenceEqual(marker))), 5_000);
            AssertRaftSafe(sim, voters, crashed, context + " after convergence");
            IReadOnlyList<byte[]> reference = sim.AppliedLog(0);
            Assert.Contains(reference, e => e.AsSpan().SequenceEqual(marker));
            for (int n = 1; n < voters; n++)
            {
                IReadOnlyList<byte[]> log = sim.AppliedLog(n);
                Assert.True(log.Count == reference.Count && log.Zip(reference).All(p => p.First.AsSpan().SequenceEqual(p.Second)),
                    $"{context}: node {n} applied log differs from node 0 after convergence ({log.Count} vs {reference.Count} entries)");
            }

            // Every proposal is nonempty and unique; empty payloads (leader no-op entries, if exposed) are not proposals.
            string[] proposals = reference.Where(e => e.Length > 0).Select(Convert.ToHexString).ToArray();
            Assert.True(proposals.Distinct(StringComparer.Ordinal).Count() == proposals.Length, $"{context}: a proposal was applied more than once");
            Assert.True(proposals.Length <= proposed + 1, $"{context}: {proposals.Length} applied proposals but only {proposed + 1} were made");
        }
        catch (Exception ex) when (ex is not NotImplementedException)
        {
            string trace = string.Join(Environment.NewLine, sim.Trace.TakeLast(60));
            throw new Xunit.Sdk.XunitException($"MOD-001 RaftNode exploration failed ({context}); replay with this seed. Last trace lines:{Environment.NewLine}{trace}{Environment.NewLine}{ex}");
        }
    }

    private static int[][] RandomGroups(SeededRandom random, int voters, int? leader)
    {
        int[] order = Enumerable.Range(0, voters).OrderBy(_ => random.NextUInt64()).ToArray();
        switch (random.Next(3))
        {
            case 0 when leader is int l:
                return [[l], Enumerable.Range(0, voters).Where(n => n != l).ToArray()]; // isolate the leader
            case 1:
                return order.Select(n => new[] { n }).ToArray();                         // 1+1+...+1
            default:
            {
                int cut = 1 + random.Next(voters - 1);
                return [order[..cut], order[cut..]];                                     // minority/majority split
            }
        }
    }

    private static void AssertRaftSafe(RaftSimulation sim, int voters, IReadOnlySet<int> crashed, string context)
    {
        string? violation = sim.CheckSafety();
        Assert.True(violation is null, $"{context}: {violation}");
        // Independent oracle: applied logs of running nodes are prefixes of one another (state-machine safety).
        var running = Enumerable.Range(0, voters).Where(n => !crashed.Contains(n)).ToArray();
        for (int i = 0; i < running.Length; i++)
        {
            for (int j = i + 1; j < running.Length; j++)
            {
                IReadOnlyList<byte[]> a = sim.AppliedLog(running[i]);
                IReadOnlyList<byte[]> b = sim.AppliedLog(running[j]);
                int common = Math.Min(a.Count, b.Count);
                for (int k = 0; k < common; k++)
                {
                    Assert.True(a[k].AsSpan().SequenceEqual(b[k]),
                        $"{context}: nodes {running[i]} and {running[j]} applied different entries at position {k}");
                }
            }
        }
    }

    // ------------------------------------------------------------------ MOD-002..MOD-011

    [Fact, Catalog("MOD-002", "M", "INV-05")]
    public void MOD_002_FencingAcrossEpochsModel()
    {
        var spec = new FencingSpec();
        ModHelpers.AssertRefinementMapWellFormed(spec);
        ModHelpers.AssertCorrectVariantHolds(spec, minimumStates: 1_000);
        IReadOnlyList<SpecRun> broken = ModHelpers.AssertNegativeControlsDetected(spec);

        // The counterexample must show a replacement (a newer generation granted after the old writer sent its
        // publication) before the stale publication is accepted.
        IReadOnlyList<string> skip = broken.Single(b => b.Variant == FencingSpec.SkipFencingCheck).Result.CounterexampleTrace!;
        int firstGrant = IndexOf(skip, "Open(");
        int staleSend = IndexOf(skip, "SendPublish(", after: firstGrant);
        int regrant = IndexOf(skip, "Open(", after: staleSend);
        int stalePublish = IndexOf(skip, "DeliverPublish(", after: regrant);
        Assert.True(firstGrant >= 0 && staleSend > firstGrant && regrant > staleSend && stalePublish > regrant,
            "SkipFencingCheck counterexample must grant g, send a publication with g, grant a newer generation and then accept the stale publication: " + string.Join(" → ", skip));
        Assert.Contains("obsolete generation", broken.Single(b => b.Variant == FencingSpec.SkipFencingCheck).Result.Violation!, StringComparison.Ordinal);
        Assert.Contains(broken.Single(b => b.Variant == FencingSpec.RenewalResurrectsExpiredGeneration).Result.CounterexampleTrace!,
            a => a.StartsWith("DeliverRenew(", StringComparison.Ordinal));
    }

    [Fact, Catalog("MOD-003", "M", "INV-01", "INV-02", "INV-09")]
    public void MOD_003_DataThenMetadataModel()
    {
        var spec = new DataThenMetadataSpec();
        ModHelpers.AssertRefinementMapWellFormed(spec);
        ModHelpers.AssertCorrectVariantHolds(spec, minimumStates: 1_000);
        IReadOnlyList<SpecRun> broken = ModHelpers.AssertNegativeControlsDetected(spec);

        IReadOnlyList<string> early = broken.Single(b => b.Variant == DataThenMetadataSpec.AcknowledgeBeforeFlush).Result.CounterexampleTrace!;
        // Receipts for unflushed copies let the commit publish data that is not durable.
        int receipt = IndexOf(early, "Receipt(");
        Assert.True(receipt >= 0 && IndexOf(early, "Commit(", after: receipt) > receipt,
            "AcknowledgeBeforeFlush counterexample must commit on early receipts: " + string.Join(" → ", early));
        Assert.Contains("durable copies", broken.Single(b => b.Variant == DataThenMetadataSpec.AcknowledgeBeforeFlush).Result.Violation!, StringComparison.Ordinal);
        IReadOnlyList<string> gc = broken.Single(b => b.Variant == DataThenMetadataSpec.GcIgnoresOpenStaging).Result.CounterexampleTrace!;
        int gcIndex = IndexOf(gc, "Gc(");
        Assert.True(gcIndex >= 0 && IndexOf(gc, "Commit(", after: gcIndex) > gcIndex,
            "GcIgnoresOpenStaging counterexample must delete a staged copy before the commit that publishes it: " + string.Join(" → ", gc));
    }

    [Fact, Catalog("MOD-004", "M", "INV-06")]
    public void MOD_004_IdempotencyAndRetirementModel()
    {
        var spec = new IdempotencySpec();
        ModHelpers.AssertRefinementMapWellFormed(spec);
        ModHelpers.AssertCorrectVariantHolds(spec, minimumStates: 1_000);
        IReadOnlyList<SpecRun> broken = ModHelpers.AssertNegativeControlsDetected(spec);

        foreach (SpecRun b in broken)
            Assert.Contains("operation id seq", b.Result.Violation!, StringComparison.Ordinal);
        IReadOnlyList<string> dedup = broken.Single(b => b.Variant == IdempotencySpec.DisableOperationDedup).Result.CounterexampleTrace!;
        Assert.Contains(dedup, a => a.StartsWith("Deliver(", StringComparison.Ordinal));
        IReadOnlyList<string> forget = broken.Single(b => b.Variant == IdempotencySpec.ForgetExpiredIdWatermark).Result.CounterexampleTrace!;
        Assert.True(forget.Any(a => a.StartsWith("RetireAcked(", StringComparison.Ordinal)) || forget.Count(a => a.StartsWith("Deliver(", StringComparison.Ordinal)) >= 2,
            "ForgetExpiredIdWatermark counterexample must retire an outcome before the id is reused: " + string.Join(" → ", forget));
    }

    [Fact, Catalog("MOD-005", "M", "INV-04")]
    public void MOD_005_TransactionValidationModel()
    {
        var spec = new TransactionValidationSpec();
        ModHelpers.AssertRefinementMapWellFormed(spec);
        SpecRun run = ModHelpers.AssertCorrectVariantHolds(spec, minimumStates: 100);
        Assert.Contains("Commit", run.ActionKinds);
        IReadOnlyList<SpecRun> broken = ModHelpers.AssertNegativeControlsDetected(spec);

        Assert.Contains("phantom", broken.Single(b => b.Variant == TransactionValidationSpec.SkipPredicateValidation).Result.Violation!, StringComparison.Ordinal);
        Assert.Contains(broken.Single(b => b.Variant == TransactionValidationSpec.SkipAbsentNameValidation).Result.CounterexampleTrace!,
            a => a == "OrdinaryCreateC");
        IReadOnlyList<string> skew = broken.Single(b => b.Variant == TransactionValidationSpec.NoReadVersionValidation).Result.CounterexampleTrace!;
        Assert.True(skew.Count(a => a.StartsWith("Commit(", StringComparison.Ordinal) && a.EndsWith(",ok)", StringComparison.Ordinal)) >= 2,
            "NoReadVersionValidation counterexample must commit both conflicting transactions (write skew): " + string.Join(" → ", skew));
    }

    [Fact, Catalog("MOD-006", "M", "INV-02", "INV-04")]
    public void MOD_006_SnapshotConsistentBoundaryModel()
    {
        var spec = new SnapshotBoundarySpec();
        ModHelpers.AssertRefinementMapWellFormed(spec);
        SpecRun run = ModHelpers.AssertCorrectVariantHolds(spec, minimumStates: 100);
        Assert.Contains("CommitWithSnapshot", run.ActionKinds);
        Assert.Contains("WriteZ", run.ActionKinds);
        IReadOnlyList<SpecRun> broken = ModHelpers.AssertNegativeControlsDetected(spec);
        foreach (string cut in new[] { SnapshotBoundarySpec.IndependentFileCopy, SnapshotBoundarySpec.TwoEntryTransactionCommit })
            Assert.Contains("cuts through a transaction", broken.Single(b => b.Variant == cut).Result.Violation!, StringComparison.Ordinal);
        // A capture racing the apply of the latest decision misses a decision ordered before the snapshot.
        SpecRun racing = broken.Single(b => b.Variant == SnapshotBoundarySpec.CaptureRacesApply);
        Assert.Contains("does not match its log position", racing.Result.Violation!, StringComparison.Ordinal);
        Assert.Contains(racing.Result.CounterexampleTrace!, a => a.StartsWith("Snapshot(", StringComparison.Ordinal));
    }

    [Fact, Catalog("MOD-007", "M", "INV-10")]
    public void MOD_007_BackupPublishModel()
    {
        var spec = new BackupPublishSpec();
        ModHelpers.AssertRefinementMapWellFormed(spec);
        SpecRun run = ModHelpers.AssertCorrectVariantHolds(spec, minimumStates: 200);
        Assert.Contains("Prune", run.ActionKinds);
        Assert.Contains("Crash", run.ActionKinds);
        IReadOnlyList<SpecRun> broken = ModHelpers.AssertNegativeControlsDetected(spec);

        Assert.Contains(broken.Single(b => b.Variant == BackupPublishSpec.PublishTrustingInventoryHints).Result.CounterexampleTrace!,
            a => a.StartsWith("FalseInventoryHint(", StringComparison.Ordinal));
        Assert.Contains("incomplete", broken.Single(b => b.Variant == BackupPublishSpec.GcConsidersLatestSnapshotOnly).Result.Violation!, StringComparison.Ordinal);
    }

    [Fact, Catalog("MOD-008", "M", "INV-09")]
    public void MOD_008_GcReachabilityModel()
    {
        var spec = new GcReachabilitySpec();
        ModHelpers.AssertRefinementMapWellFormed(spec);
        ModHelpers.AssertCorrectVariantHolds(spec, minimumStates: 1_000);
        IReadOnlyList<SpecRun> broken = ModHelpers.AssertNegativeControlsDetected(spec);

        IReadOnlyList<string> time = broken.Single(b => b.Variant == GcReachabilitySpec.TimeBasedStagingExpiry).Result.CounterexampleTrace!;
        Assert.True(IndexOf(time, "GcMark(") >= 0, "TimeBasedStagingExpiry counterexample must mark a staged object: " + string.Join(" → ", time));
        IReadOnlyList<string> pins = broken.Single(b => b.Variant == GcReachabilitySpec.IgnoreSnapshotsAndPins).Result.CounterexampleTrace!;
        Assert.Contains(pins, a => a.StartsWith("Pin(", StringComparison.Ordinal) || a.StartsWith("CreateSnapshot(", StringComparison.Ordinal));
    }

    [Fact, Catalog("MOD-009", "M", "INV-18")]
    public void MOD_009_MembershipTransitionModel()
    {
        var spec = new MembershipSpec();
        ModHelpers.AssertRefinementMapWellFormed(spec);
        SpecRun run = ModHelpers.AssertCorrectVariantHolds(spec, minimumStates: 1_000);
        Assert.Contains("ProposeConfig", run.ActionKinds);
        IReadOnlyList<SpecRun> broken = ModHelpers.AssertNegativeControlsDetected(spec);
        Assert.Contains(broken.Single(b => b.Variant == MembershipSpec.ReplaceTwoVotersAtOnce).Result.CounterexampleTrace!,
            a => a.StartsWith("ProposeConfig(", StringComparison.Ordinal));
    }

    [Fact, Catalog("MOD-010", "M", "INV-18")]
    public void MOD_010_RecoveryEpochModel()
    {
        var spec = new RecoveryEpochSpec();
        ModHelpers.AssertRefinementMapWellFormed(spec);
        SpecRun run = ModHelpers.AssertCorrectVariantHolds(spec, minimumStates: 100);
        Assert.Contains("Promote", run.ActionKinds);
        Assert.Contains("Renew", run.ActionKinds);
        IReadOnlyList<SpecRun> broken = ModHelpers.AssertNegativeControlsDetected(spec);
        foreach (SpecRun b in broken)
            Assert.Contains("split authority", b.Result.Violation!, StringComparison.Ordinal);
        IReadOnlyList<string> automatic = broken.Single(b => b.Variant == RecoveryEpochSpec.AutomaticPromotionOnPartition).Result.CounterexampleTrace!;
        Assert.Contains("Promote(automatic)", automatic);
        Assert.DoesNotContain("BeginPromotion", automatic); // no explicit administration happened
        Assert.Contains(broken.Single(b => b.Variant == RecoveryEpochSpec.OldSourceRenewsAfterReturn).Result.CounterexampleTrace!,
            a => a == "Return(old)");
    }

    [Fact, Catalog("MOD-011", "M", "INV-01", "INV-17", "INV-18")]
    public void MOD_011_ConditionalLivenessModel()
    {
        var spec = new ConditionalLivenessSpec();
        ModHelpers.AssertRefinementMapWellFormed(spec);
        SpecRun run = ModHelpers.AssertCorrectVariantHolds(spec, minimumStates: 50);

        // Progress was checked in assumption-satisfying states, and the model also reached states where the assumptions
        // fail and progress is (correctly) not possible — so liveness is claimed only conditionally.
        Assert.True(run.Statistics.GetValueOrDefault("statesWhereAssumptionsHold") > 0, "no state satisfied the liveness assumptions; the progress check was vacuous");
        Assert.True(run.Statistics.GetValueOrDefault("statesWithoutProgressBecauseAssumptionsFail") > 0,
            "progress was possible in every state, so the model does not distinguish conditional from unconditional liveness");
        Assert.Contains(spec.Assumptions, a => a.Contains("fairness", StringComparison.OrdinalIgnoreCase));
        Assert.Contains(spec.Assumptions, a => a.Contains("quorum", StringComparison.OrdinalIgnoreCase));
        Assert.Contains(spec.Assumptions, a => a.Contains("communication", StringComparison.OrdinalIgnoreCase));
        Assert.Contains(spec.Assumptions, a => a.Contains("capacity", StringComparison.OrdinalIgnoreCase));

        IReadOnlyList<SpecRun> broken = ModHelpers.AssertNegativeControlsDetected(spec);
        Assert.Contains("conditional liveness violated", broken.Single(b => b.Variant == ConditionalLivenessSpec.DropPendingAndWaitForever).Result.Violation!, StringComparison.Ordinal);
        Assert.Contains("truthful reporting", broken.Single(b => b.Variant == ConditionalLivenessSpec.FabricateSuccessWithoutQuorum).Result.Violation!, StringComparison.Ordinal);
        Assert.Contains("truthful reporting", broken.Single(b => b.Variant == ConditionalLivenessSpec.ReportTimeoutAsDefiniteFailure).Result.Violation!, StringComparison.Ordinal);
    }

    // ------------------------------------------------------------------ MOD-012

    [Fact, Catalog("MOD-012", "M", "INV-01", "INV-02", "INV-04", "INV-05", "INV-06", "INV-09", "INV-10", "INV-18")]
    public void MOD_012_RefinementAndBoundsReport()
    {
        IReadOnlyList<IProtocolSpec> specs = ModHelpers.AllSpecs();

        // Every MOD catalog row except this report has exactly one model with the catalog's id and name.
        CatalogEntry[] rows = TestCatalog.All.Where(e => e.Suite == "MOD").OrderBy(e => e.Id, StringComparer.Ordinal).ToArray();
        Assert.Equal(12, rows.Length);
        foreach (CatalogEntry row in rows.Where(r => r.Id != "MOD-012"))
            Assert.Single(specs, s => s.TestId == row.Id && s.Name == row.Name);
        Assert.Equal(specs.Count, specs.Select(s => s.TestId).Distinct(StringComparer.Ordinal).Count());

        var results = new List<(IProtocolSpec Spec, SpecRun Correct, IReadOnlyList<SpecRun> Broken)>();
        foreach (IProtocolSpec spec in specs)
        {
            ModHelpers.AssertRefinementMapWellFormed(spec);
            SpecRun correct = ModHelpers.Run(spec, ProtocolSpecNames.Correct);
            var broken = spec.BrokenVariants.Select(v => ModHelpers.Run(spec, v.Name)).ToList();
            results.Add((spec, correct, broken));
        }

        string report = ModHelpers.BuildReport(results, RealRaftExplorationBounds);
        string directory = Path.Combine(new TestClusterOptions().ResolveDataRoot(), "mod-012");
        Directory.CreateDirectory(directory);
        string path = Path.Combine(directory, "refinement-and-bounds-report.md");
        File.WriteAllText(path, report, new UTF8Encoding(false));
        ModHelpers.Log("MOD-012 report written to " + path);
        ModHelpers.Log(report);
        Assert.Equal(report, File.ReadAllText(path, Encoding.UTF8));

        // The report never labels bounded exploration as an implementation proof.
        Assert.Contains(ModHelpers.Disclaimer, report, StringComparison.Ordinal);
        foreach (string forbidden in new[] { "proves the implementation", "implementation is proven", "implementation is verified", "100% guarantee", "fully verified" })
            Assert.DoesNotContain(forbidden, report, StringComparison.OrdinalIgnoreCase);

        foreach ((IProtocolSpec spec, SpecRun correct, IReadOnlyList<SpecRun> broken) in results)
        {
            string section = Section(report, $"## {spec.TestId} {spec.Name}");

            // Completed, violation-free bounded exploration with the declared bounds (an incomplete exploration is
            // unresolved, never a completed model).
            correct.Result.AssertNoViolation();
            correct.Result.AssertCompleted();
            Assert.Contains($"- maxDepth={spec.Bounds.MaxDepth}", section, StringComparison.Ordinal);
            Assert.Contains($"- maxStates={spec.Bounds.MaxStates}", section, StringComparison.Ordinal);
            Assert.Contains($"explored: {correct.Result.StatesExplored} states", section, StringComparison.Ordinal);
            Assert.Contains("completed=yes", section, StringComparison.Ordinal);
            Assert.NotEmpty(spec.Bounds.Parameters);
            foreach (string parameter in spec.Bounds.Parameters)
                Assert.Contains("- parameter: " + parameter, section, StringComparison.Ordinal);

            // Properties, assumptions, abstractions and refinement gaps are all recorded.
            Assert.NotEmpty(spec.Properties);
            Assert.NotEmpty(spec.Assumptions);
            Assert.NotEmpty(spec.Abstractions);
            Assert.NotEmpty(spec.RefinementGaps);
            foreach (string item in spec.Properties.Concat(spec.Assumptions).Concat(spec.Abstractions).Concat(spec.RefinementGaps))
                Assert.Contains("- " + item, section, StringComparison.Ordinal);

            // Every model action is mapped to implementation fault points or explicitly declared a gap.
            foreach (string action in correct.ActionKinds)
            {
                Assert.True(spec.ActionFaultSites.ContainsKey(action), $"{spec.TestId}: action '{action}' is not mapped to a fault point");
                Assert.Contains($"- {action}: ", section, StringComparison.Ordinal);
            }

            Assert.Contains(spec.ActionFaultSites.Values, sites => sites.Count > 0);

            // Every negative control is listed and was detected.
            Assert.NotEmpty(broken);
            foreach (SpecRun b in broken)
            {
                Assert.NotNull(b.Result.Violation);
                Assert.Contains($"- {b.Variant}", section, StringComparison.Ordinal);
            }

            Assert.DoesNotContain("NOT DETECTED", section, StringComparison.Ordinal);
        }

        foreach (string line in RealRaftExplorationBounds)
            Assert.Contains(line, report, StringComparison.Ordinal);
    }

    private static string Section(string report, string heading)
    {
        int start = report.IndexOf(heading + Environment.NewLine, StringComparison.Ordinal);
        Assert.True(start >= 0, $"report has no section '{heading}'");
        int end = report.IndexOf(Environment.NewLine + "## ", start + heading.Length, StringComparison.Ordinal);
        return end < 0 ? report[start..] : report[start..end];
    }

    private static int IndexOf(IReadOnlyList<string> trace, string prefix, int after = -1)
    {
        for (int i = Math.Max(0, after + 1); i < trace.Count; i++)
        {
            if (trace[i].StartsWith(prefix, StringComparison.Ordinal))
                return i;
        }

        return -1;
    }
}
