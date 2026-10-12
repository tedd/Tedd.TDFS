using System.Reflection;
using System.Text;
using Tedd.Tdfs.Testing;
using static Tedd.Tdfs.Tests.Simulation.Cha.ChaHelpers;

namespace Tedd.Tdfs.Tests.Simulation.Mut;

/// <summary>
/// MUT — negative controls (test plan §7.31, FSD §18 VER-06). Each test runs its regression scenario twice: without the
/// safety mutation (the scenario's invariants must hold and every intended fault point must be reached) and with exactly
/// that mutation enabled (the scenario's oracle must detect the specific violation). A scenario that cannot tell the two
/// apart provides no evidence for the safety check it is meant to cover.
/// </summary>
public sealed class MutSimulationTests
{
    private static async Task AssertNegativeControlAsync(Func<SafetyMutations, Task<ScenarioVerdict>> scenario, SafetyMutations mutant, ViolationKind expected)
    {
        Assert.True(mutant.Any, "the mutant run must enable a safety mutation");
        ScenarioVerdict baseline = await scenario(SafetyMutations.None);
        Assert.True(baseline.InvariantHeld, $"without the mutation the regression scenario must pass and reach its fault points:{Environment.NewLine}{baseline}");
        ScenarioVerdict detected = await scenario(mutant);
        Assert.True(detected.Detected(expected), $"with the mutation the scenario must detect {expected}:{Environment.NewLine}{detected}");
    }

    /// <summary>Bypassing server-side writer fencing must make the stale-writer regression fail (stale publication accepted).</summary>
    [Fact]
    [Catalog("MUT-001", "S", "INV-05", "INV-20")]
    public Task MUT_001_RemoveFencingCheckIsDetected() =>
        AssertNegativeControlAsync(MutHelpers.StaleWriterPublicationAsync, new SafetyMutations { SkipFencingCheck = true }, ViolationKind.StalePublicationAccepted);

    /// <summary>Issuing receipts before the flush barrier must be exposed by power loss right after acknowledgement (lost acknowledged bytes).</summary>
    [Fact]
    [Catalog("MUT-002", "S", "INV-01", "INV-20")]
    public Task MUT_002_AcknowledgeBeforeDiskFlushIsDetected() =>
        AssertNegativeControlAsync(MutHelpers.PowerLossAfterAcknowledgementAsync, new SafetyMutations { AcknowledgeBeforeFlush = true }, ViolationKind.LostAcknowledgedData);

    /// <summary>Publishing metadata before its data receipts are validated must fail the dependency-closure and recovery checks.</summary>
    [Fact]
    [Catalog("MUT-003", "S", "INV-01", "INV-02", "INV-20")]
    public Task MUT_003_CommitMetadataBeforeDataIsDetected() =>
        AssertNegativeControlAsync(MutHelpers.StaleReceiptPublicationAsync, new SafetyMutations { CommitMetadataBeforeData = true }, ViolationKind.PublishedMissingDependency);

    /// <summary>Majority computed from reachable nodes must make the partition history non-linearizable.</summary>
    [Fact]
    [Catalog("MUT-004", "S", "INV-03", "INV-08", "INV-20")]
    public Task MUT_004_UseReachableNodeCountForQuorumIsDetected() =>
        AssertNegativeControlAsync(MutHelpers.MinorityPartitionAsync, new SafetyMutations { QuorumFromReachableNodes = true }, ViolationKind.NonLinearizableHistory);

    /// <summary>Disabling accepted-id deduplication must be caught by the duplicate-append and name-reuse retries.</summary>
    [Fact]
    [Catalog("MUT-005", "S", "INV-06", "INV-20")]
    public Task MUT_005_DropOperationDedupIsDetected() =>
        AssertNegativeControlAsync(MutHelpers.DuplicateRetryAsync, new SafetyMutations { DisableOperationDedup = true }, ViolationKind.DuplicateEffect);

    /// <summary>An SDK that reports indeterminate commits as aborted must be caught by a later committed effect.</summary>
    [Fact]
    [Catalog("MUT-006", "S", "INV-06", "INV-20")]
    public Task MUT_006_TreatUnknownAsAbortedIsDetected() =>
        AssertNegativeControlAsync(MutHelpers.LostReplyOutcomeAsync, new SafetyMutations { TreatUnknownAsAborted = true }, ViolationKind.CommittedAfterReportedAbort);

    /// <summary>Ignoring absent-name/enumeration dependencies must fail the phantom and write-skew serializability check.</summary>
    [Fact]
    [Catalog("MUT-007", "S", "INV-04", "INV-20")]
    public Task MUT_007_SkipTransactionPredicateValidationIsDetected() =>
        AssertNegativeControlAsync(MutHelpers.PhantomWriteSkewAsync, new SafetyMutations { SkipPredicateValidation = true }, ViolationKind.NonSerializableTransactions);

    /// <summary>Serving a deposed leader's cached metadata as live must be rejected by the real-time consistency check.</summary>
    [Fact]
    [Catalog("MUT-008", "S", "INV-03", "INV-20")]
    public Task MUT_008_AllowStaleLiveReadIsDetected() =>
        AssertNegativeControlAsync(MutHelpers.StaleLeaderReadAsync, new SafetyMutations { AllowStaleLiveReads = true }, ViolationKind.StaleLiveRead);

    /// <summary>Removing snapshot/reader roots from GC marking must fail the historical-read check.</summary>
    [Fact]
    [Catalog("MUT-009", "S", "INV-09", "INV-20")]
    public Task MUT_009_IgnoreSnapshotPinIsDetected() =>
        AssertNegativeControlAsync(MutHelpers.PinnedHistoryGcAsync, new SafetyMutations { IgnoreSnapshotPins = true }, ViolationKind.ReclaimedProtectedData);

    /// <summary>Advancing the backup pointer before the dependency closure is durable and verified must fail the interrupted-transfer check.</summary>
    [Fact]
    [Catalog("MUT-010", "S", "INV-10", "INV-20")]
    public Task MUT_010_PublishBackupBeforeClosureIsDetected() =>
        AssertNegativeControlAsync(MutHelpers.InterruptedBackupPublicationAsync, new SafetyMutations { PublishBackupBeforeClosure = true }, ViolationKind.IncompleteBackupPublished);

    /// <summary>Returning replica bytes without verification must make the integrity test see corrupted bytes delivered.</summary>
    [Fact]
    [Catalog("MUT-011", "S", "INV-14", "INV-20")]
    public Task MUT_011_SkipChecksumVerificationIsDetected() =>
        AssertNegativeControlAsync(MutHelpers.CorruptReplicaReadAsync, new SafetyMutations { SkipChecksumVerification = true }, ViolationKind.CorruptBytesDelivered);

    /// <summary>Counting one zone twice must fail the zone acknowledgement-policy check.</summary>
    [Fact]
    [Catalog("MUT-012", "S", "INV-08", "INV-11", "INV-20")]
    public Task MUT_012_CountSameZoneTwiceIsDetected() =>
        AssertNegativeControlAsync(MutHelpers.SameZoneAcknowledgementAsync, new SafetyMutations { CountSameZoneTwice = true }, ViolationKind.PolicyUnderAcknowledged);

    /// <summary>Compacting outcomes without the replay-rejection watermark must let a delayed expired-id replay through (caught).</summary>
    [Fact]
    [Catalog("MUT-013", "S", "INV-06", "INV-20")]
    public Task MUT_013_ForgetExpiredIdWatermarkIsDetected() =>
        AssertNegativeControlAsync(MutHelpers.ExpiredIdReplayAsync, new SafetyMutations { ForgetExpiredIdWatermark = true }, ViolationKind.ExpiredIdReplayed);

    /// <summary>Lowering the ack/domain requirement on timeout must fail the policy/durability oracle.</summary>
    [Fact]
    [Catalog("MUT-014", "S", "INV-08", "INV-11", "INV-20")]
    public Task MUT_014_SilentlyRelaxPolicyIsDetected() =>
        AssertNegativeControlAsync(MutHelpers.StrongPolicyTimeoutAsync, new SafetyMutations { RelaxPolicyOnTimeout = true }, ViolationKind.PolicyUnderAcknowledged);

    /// <summary>
    /// Runs every specified safety mutant against its regression scenario (and the unmutated baseline). Every mutant must be
    /// killed: the baseline holds and the mutant's specific violation is detected. Any survivor blocks the coverage claim.
    /// </summary>
    [Fact]
    [Catalog("MUT-015", "S", "INV-20")]
    public async Task MUT_015_AllCriticalNegativeControlsAreKilled()
    {
        AssertEveryMutationHasOneControl();
        var survivors = new List<string>();
        var report = new StringBuilder();
        foreach (NegativeControl control in MutHelpers.Controls)
        {
            ScenarioVerdict baseline = await control.Run(SafetyMutations.None);
            ScenarioVerdict mutant = await control.Run(control.Mutant);
            bool baselineHeld = baseline.InvariantHeld;
            bool detected = mutant.Detected(control.Detects);
            report.AppendLine($"{control.CatalogId} {control.Mutation}: baseline {(baselineHeld ? "held" : "FAILED")}, mutant {(detected ? "killed" : "SURVIVED")} (expects {control.Detects})");
            if (!baselineHeld || !detected)
            {
                survivors.Add($"{control.CatalogId} {control.Mutation}:{Environment.NewLine}{(baselineHeld ? string.Empty : baseline.ToString())}{(detected ? string.Empty : mutant.ToString())}");
            }
        }

        Log(report.ToString());
        Assert.True(survivors.Count == 0,
            $"{survivors.Count} of {MutHelpers.Controls.Count} critical negative controls were not killed:{Environment.NewLine}{string.Join(Environment.NewLine, survivors)}");
    }

    /// <summary>
    /// MUT-015 structural part: every <see cref="SafetyMutations"/> flag has exactly one negative control, each control
    /// enables exactly its own flag, and the controls map one-to-one onto catalog rows MUT-001..MUT-014.
    /// </summary>
    [Fact]
    [Catalog("MUT-015", "S", "INV-20")]
    public void MUT_015_AllCriticalNegativeControlsAreKilled_EveryMutationHasOneControl() => AssertEveryMutationHasOneControl();

    private static void AssertEveryMutationHasOneControl()
    {
        PropertyInfo[] flags = typeof(SafetyMutations).GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.PropertyType == typeof(bool) && p.CanWrite)
            .ToArray();
        string[] flagNames = flags.Select(p => p.Name).Order(StringComparer.Ordinal).ToArray();
        string[] controlled = MutHelpers.Controls.Select(c => c.Mutation).Order(StringComparer.Ordinal).ToArray();
        Assert.Equal(flagNames, controlled);

        foreach (NegativeControl control in MutHelpers.Controls)
        {
            SafetyMutations mutant = control.Mutant;
            string[] enabled = flags.Where(p => (bool)p.GetValue(mutant)!).Select(p => p.Name).ToArray();
            Assert.Equal([control.Mutation], enabled);
            Assert.True(mutant.Any);
            CatalogEntry? row = TestCatalog.Find(control.CatalogId);
            Assert.True(row is not null, $"{control.CatalogId} is not a catalog row");
            Assert.Equal("MUT", row.Suite);
        }

        string[] expectedIds = Enumerable.Range(1, 14).Select(i => $"MUT-{i:D3}").ToArray();
        Assert.Equal(expectedIds, MutHelpers.Controls.Select(c => c.CatalogId).Order(StringComparer.Ordinal).ToArray());
        Assert.False(SafetyMutations.None.Any, "the production configuration enables no mutation");
    }
}
