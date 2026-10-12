using System.Text;
using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;
using static Tedd.Tdfs.Tests.Simulation.Cha.ChaHelpers;

namespace Tedd.Tdfs.Tests.Simulation.Mut;

/// <summary>The safety violation a regression scenario's oracle can detect.</summary>
internal enum ViolationKind
{
    /// <summary>Acknowledged (durably committed) content is gone or different.</summary>
    LostAcknowledgedData,

    /// <summary>A publication by an expired/superseded writer generation was accepted.</summary>
    StalePublicationAccepted,

    /// <summary>A published revision references an object without a durable verified copy (incomplete dependency closure).</summary>
    PublishedMissingDependency,

    /// <summary>Two leaders for one group in one term.</summary>
    SplitBrain,

    /// <summary>A minority acknowledged a mutation it had no authority to commit.</summary>
    MinorityAuthority,

    /// <summary>The recorded history has no linearization.</summary>
    NonLinearizableHistory,

    /// <summary>One operation identity produced more than one logical effect.</summary>
    DuplicateEffect,

    /// <summary>The client reported a definite failure for an operation that later turned out committed.</summary>
    CommittedAfterReportedAbort,

    /// <summary>The committed transactions admit no strictly serializable order.</summary>
    NonSerializableTransactions,

    /// <summary>A live read returned state older than a write that completed before the read began.</summary>
    StaleLiveRead,

    /// <summary>Data protected by a snapshot or reader pin was reclaimed or became unreadable.</summary>
    ReclaimedProtectedData,

    /// <summary>A backup snapshot was published/acknowledged without its complete durable dependency closure.</summary>
    IncompleteBackupPublished,

    /// <summary>Bytes that do not match the written content were returned as a successful read.</summary>
    CorruptBytesDelivered,

    /// <summary>A write was acknowledged without meeting its copy/zone acknowledgement policy.</summary>
    PolicyUnderAcknowledged,

    /// <summary>A retired/expired operation id was executed again instead of being rejected.</summary>
    ExpiredIdReplayed,
}

/// <summary>One detected violation.</summary>
internal sealed record Violation(ViolationKind Kind, string Detail);

/// <summary>
/// The result of one run of a regression scenario: what its oracles detected, which intended fault points were not
/// reached, and errors that prevented a conclusion. Scenarios never assert; tests decide from the verdict.
/// </summary>
internal sealed record ScenarioVerdict(string Scenario, SafetyMutations Mutations, ulong Seed, bool Completed,
    IReadOnlyList<Violation> Violations, IReadOnlyList<string> UnreachedFaultPoints, IReadOnlyList<string> Errors, IReadOnlyList<string> Evidence)
{
    /// <summary>True when the scenario ran to completion, reached every intended fault point and its oracles found nothing.</summary>
    public bool InvariantHeld => Completed && Violations.Count == 0 && UnreachedFaultPoints.Count == 0 && Errors.Count == 0;

    /// <summary>True when an oracle detected a violation of <paramref name="kind"/>.</summary>
    public bool Detected(ViolationKind kind) => Violations.Any(v => v.Kind == kind);

    /// <inheritdoc />
    public override string ToString()
    {
        var sb = new StringBuilder();
        sb.Append(Scenario).Append(" [mutations: ").Append(Describe(Mutations)).Append("] seed=").Append(Seed)
            .Append(" completed=").Append(Completed).AppendLine();
        foreach (Violation v in Violations)
            sb.Append("  VIOLATION ").Append(v.Kind).Append(": ").AppendLine(v.Detail);
        foreach (string u in UnreachedFaultPoints)
            sb.Append("  UNREACHED ").AppendLine(u);
        foreach (string e in Errors)
            sb.Append("  ERROR ").AppendLine(e);
        foreach (string e in Evidence)
            sb.Append("  evidence: ").AppendLine(e);
        return sb.ToString();
    }

    private static string Describe(SafetyMutations m)
    {
        string[] enabled = typeof(SafetyMutations).GetProperties()
            .Where(p => p.PropertyType == typeof(bool) && p.CanWrite && (bool)p.GetValue(m)!)
            .Select(p => p.Name)
            .ToArray();
        return enabled.Length == 0 ? "none" : string.Join(",", enabled);
    }
}

/// <summary>Collects a scenario's observations into a <see cref="ScenarioVerdict"/>.</summary>
internal sealed class VerdictBuilder
{
    private readonly List<Violation> _violations = [];
    private readonly List<string> _unreached = [];
    private readonly List<string> _errors = [];
    private readonly List<string> _evidence = [];

    public VerdictBuilder(string scenario, SafetyMutations mutations)
    {
        Scenario = scenario;
        Mutations = mutations;
    }

    public string Scenario { get; }

    public SafetyMutations Mutations { get; }

    public ulong Seed { get; set; }

    public bool Completed { get; set; }

    public void Violation(ViolationKind kind, string detail) => _violations.Add(new Violation(kind, detail));

    public void Check(bool holds, ViolationKind kind, string detail)
    {
        if (!holds)
            Violation(kind, detail);
    }

    public void Reached(bool reached, string point)
    {
        if (reached)
            _evidence.Add("reached " + point);
        else
            _unreached.Add(point);
    }

    public void Error(string message) => _errors.Add(message);

    public void Evidence(string message) => _evidence.Add(message);

    /// <summary>Records a history-checker result: invalid → violation; inconclusive → error (never a pass, HAR-017).</summary>
    public void History(CheckResult result, ViolationKind kind, string what)
    {
        if (result.IsInconclusive)
            Error($"{what}: checker inconclusive after {result.StatesExplored} states: {result.Explanation}");
        else if (!result.IsValid)
            Violation(kind, $"{what}: {result.Explanation}");
        else
            Evidence($"{what}: valid ({result.StatesExplored} states)");
    }

    public ScenarioVerdict Build() =>
        new(Scenario, Mutations, Seed, Completed, _violations.ToArray(), _unreached.ToArray(), _errors.ToArray(), _evidence.ToArray());
}

/// <summary>A safety mutation paired with the regression scenario that must detect it (FSD VER-06).</summary>
internal sealed record NegativeControl(string Mutation, string CatalogId, ViolationKind Detects, Func<SafetyMutations, Task<ScenarioVerdict>> Run)
{
    /// <summary>The mutant configuration: exactly <see cref="Mutation"/> enabled.</summary>
    public SafetyMutations Mutant => MutHelpers.Enable(Mutation);
}

/// <summary>Regression scenarios used as negative-control oracles for every <see cref="SafetyMutations"/> flag.</summary>
internal static class MutHelpers
{
    private const string MutDir = "/mut";

    /// <summary>Every negative control, one per safety mutation (MUT-001..MUT-014).</summary>
    public static IReadOnlyList<NegativeControl> Controls { get; } =
    [
        new(nameof(SafetyMutations.SkipFencingCheck), "MUT-001", ViolationKind.StalePublicationAccepted, StaleWriterPublicationAsync),
        new(nameof(SafetyMutations.AcknowledgeBeforeFlush), "MUT-002", ViolationKind.LostAcknowledgedData, PowerLossAfterAcknowledgementAsync),
        new(nameof(SafetyMutations.CommitMetadataBeforeData), "MUT-003", ViolationKind.PublishedMissingDependency, StaleReceiptPublicationAsync),
        new(nameof(SafetyMutations.QuorumFromReachableNodes), "MUT-004", ViolationKind.NonLinearizableHistory, MinorityPartitionAsync),
        new(nameof(SafetyMutations.DisableOperationDedup), "MUT-005", ViolationKind.DuplicateEffect, DuplicateRetryAsync),
        new(nameof(SafetyMutations.TreatUnknownAsAborted), "MUT-006", ViolationKind.CommittedAfterReportedAbort, LostReplyOutcomeAsync),
        new(nameof(SafetyMutations.SkipPredicateValidation), "MUT-007", ViolationKind.NonSerializableTransactions, PhantomWriteSkewAsync),
        new(nameof(SafetyMutations.AllowStaleLiveReads), "MUT-008", ViolationKind.StaleLiveRead, StaleLeaderReadAsync),
        new(nameof(SafetyMutations.IgnoreSnapshotPins), "MUT-009", ViolationKind.ReclaimedProtectedData, PinnedHistoryGcAsync),
        new(nameof(SafetyMutations.PublishBackupBeforeClosure), "MUT-010", ViolationKind.IncompleteBackupPublished, InterruptedBackupPublicationAsync),
        new(nameof(SafetyMutations.SkipChecksumVerification), "MUT-011", ViolationKind.CorruptBytesDelivered, CorruptReplicaReadAsync),
        new(nameof(SafetyMutations.CountSameZoneTwice), "MUT-012", ViolationKind.PolicyUnderAcknowledged, SameZoneAcknowledgementAsync),
        new(nameof(SafetyMutations.ForgetExpiredIdWatermark), "MUT-013", ViolationKind.ExpiredIdReplayed, ExpiredIdReplayAsync),
        new(nameof(SafetyMutations.RelaxPolicyOnTimeout), "MUT-014", ViolationKind.PolicyUnderAcknowledged, StrongPolicyTimeoutAsync),
    ];

    /// <summary>A <see cref="SafetyMutations"/> with exactly the named flag enabled.</summary>
    public static SafetyMutations Enable(string flag) => flag switch
    {
        nameof(SafetyMutations.SkipFencingCheck) => new SafetyMutations { SkipFencingCheck = true },
        nameof(SafetyMutations.AcknowledgeBeforeFlush) => new SafetyMutations { AcknowledgeBeforeFlush = true },
        nameof(SafetyMutations.CommitMetadataBeforeData) => new SafetyMutations { CommitMetadataBeforeData = true },
        nameof(SafetyMutations.QuorumFromReachableNodes) => new SafetyMutations { QuorumFromReachableNodes = true },
        nameof(SafetyMutations.DisableOperationDedup) => new SafetyMutations { DisableOperationDedup = true },
        nameof(SafetyMutations.TreatUnknownAsAborted) => new SafetyMutations { TreatUnknownAsAborted = true },
        nameof(SafetyMutations.SkipPredicateValidation) => new SafetyMutations { SkipPredicateValidation = true },
        nameof(SafetyMutations.AllowStaleLiveReads) => new SafetyMutations { AllowStaleLiveReads = true },
        nameof(SafetyMutations.IgnoreSnapshotPins) => new SafetyMutations { IgnoreSnapshotPins = true },
        nameof(SafetyMutations.PublishBackupBeforeClosure) => new SafetyMutations { PublishBackupBeforeClosure = true },
        nameof(SafetyMutations.SkipChecksumVerification) => new SafetyMutations { SkipChecksumVerification = true },
        nameof(SafetyMutations.CountSameZoneTwice) => new SafetyMutations { CountSameZoneTwice = true },
        nameof(SafetyMutations.ForgetExpiredIdWatermark) => new SafetyMutations { ForgetExpiredIdWatermark = true },
        nameof(SafetyMutations.RelaxPolicyOnTimeout) => new SafetyMutations { RelaxPolicyOnTimeout = true },
        _ => throw new ArgumentOutOfRangeException(nameof(flag), flag, "unknown safety mutation"),
    };

    /// <summary>
    /// Runs <paramref name="body"/> in a deterministic cluster with <paramref name="mutations"/> applied to every node
    /// (and, through <see cref="Client"/>, to every SDK client). Exceptions never escape: they become verdict errors.
    /// </summary>
    public static async Task<ScenarioVerdict> RunScenarioAsync(string scenario, SafetyMutations mutations, TestClusterOptions options,
        Func<TestCluster, VerdictBuilder, Task> body)
    {
        var verdict = new VerdictBuilder(scenario, mutations);
        TestClusterOptions effective = Sim(options, 15) with { Mutations = mutations };
        try
        {
            await ClusterTest.RunAsync(effective, async cluster =>
            {
                verdict.Seed = cluster.Seed;
                try
                {
                    await body(cluster, verdict);
                    verdict.Completed = true;
                }
                catch (Exception ex)
                {
                    verdict.Error($"scenario aborted: {ex}");
                }
            }, Ct);
        }
        catch (Exception ex) when (verdict.Completed)
        {
            verdict.Error($"harness post-scenario invariant check failed: {ex.GetType().Name}: {ex.Message}");
        }
        catch (Exception ex)
        {
            verdict.Error($"cluster run failed: {ex}");
        }

        ScenarioVerdict result = verdict.Build();
        Log(result.ToString());
        return result;
    }

    /// <summary>Client options carrying the scenario's mutations (SDK-side flags take effect, server-only flags are ignored).</summary>
    public static TestClientOptions Client(string name, SafetyMutations mutations, IReadOnlyList<string>? seeds = null, TimeSpan? operationTimeout = null) =>
        new() { Name = name, Mutations = mutations, SeedNodes = seeds, OperationTimeout = operationTimeout };

    private static async Task<Exception?> CaptureAsync(Func<Task> action)
    {
        try
        {
            await action();
            return null;
        }
        catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
        {
            return ex;
        }
    }

    private static async Task<bool> WaitHeldAsync(MessageHold hold, int count, TimeSpan timeout)
    {
        try
        {
            await hold.WaitForAsync(count, timeout);
            return true;
        }
        catch (Exception ex) when (ex is TimeoutException or Xunit.Sdk.XunitException or InvalidOperationException)
        {
            return false;
        }
    }

    private static async Task<bool> WaitTriggeredAsync(FaultArm arm, TimeSpan timeout)
    {
        try
        {
            await arm.WaitTriggeredAsync(timeout);
            return arm.Triggered;
        }
        catch (Exception ex) when (ex is TimeoutException or Xunit.Sdk.XunitException or InvalidOperationException)
        {
            return false;
        }
    }

    private static async Task<bool> TryWaitUntilAsync(TestCluster cluster, Func<Task<bool>> condition, TimeSpan timeout, string because)
    {
        try
        {
            await cluster.WaitUntilAsync(condition, timeout, because, Ct);
            return true;
        }
        catch (TimeoutException)
        {
            return false;
        }
    }

    /// <summary>Reads a whole file as an oracle: any failure is recorded as a violation of <paramref name="kind"/>.</summary>
    private static async Task<byte[]?> ReadOracleAsync(VerdictBuilder verdict, DfsFileClient files, string path, ViolationKind kind)
    {
        try
        {
            return await files.ReadAllBytesAsync(path, Ct);
        }
        catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
        {
            verdict.Violation(kind, $"reading {path} failed: {ex.GetType().Name}: {ex.Message}");
            return null;
        }
    }

    private static async Task RecordAsync(TestCluster cluster, string process, string kind, string key, byte[]? input, Func<Task> action)
    {
        try
        {
            await cluster.History.RecordAsync<bool>(process, kind, key, input, async () =>
            {
                await action();
                return true;
            });
        }
        catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
        {
            // Failed/unknown outcomes are recorded in the history; the checker decides.
        }
    }

    private static async Task<byte[]?> RecordReadAsync(TestCluster cluster, string process, string key, DfsFileClient files)
    {
        try
        {
            return await cluster.History.RecordAsync(process, "read", key, null, () => TryReadAllAsync(files, key));
        }
        catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
        {
            return null;
        }
    }

    private static async Task CheckDurableCopiesAsync(TestCluster cluster, VerdictBuilder verdict, string path, int copies, int zones, ViolationKind kind, string what)
    {
        FileInspection? file = await cluster.Inspect.GetFileAsync(Vol, path);
        if (file is null)
        {
            verdict.Violation(kind, $"{what}: {path} is missing from the authoritative state");
            return;
        }

        foreach (ObjectId obj in file.Extents.Select(e => e.Object).Distinct())
        {
            (int actualCopies, int actualZones) = await cluster.Inspect.CountDurableDomainsAsync(obj);
            verdict.Check(actualCopies >= copies && actualZones >= zones, kind,
                $"{what}: {obj} of {path} has {actualCopies} durable copies in {actualZones} zones (required {copies}/{zones})");
        }
    }

    /// <summary>
    /// Policy oracle independent of what the client was told: whatever revision of <paramref name="path"/> is committed
    /// must meet the ack copy/zone policy, and a write reported as definitely failed must not be committed.
    /// </summary>
    private static async Task CheckCommittedMeetsPolicyAsync(TestCluster cluster, VerdictBuilder verdict, string path, Exception? clientFailure,
        int copies, int zones, string when)
    {
        FileInspection? committed = await cluster.Inspect.GetFileAsync(Vol, path);
        if (committed is null)
        {
            verdict.Check(clientFailure is not null, ViolationKind.LostAcknowledgedData, $"an acknowledged write {when} is not in the authoritative state");
            return;
        }

        await CheckDurableCopiesAsync(cluster, verdict, path, copies, zones, ViolationKind.PolicyUnderAcknowledged,
            clientFailure is null ? $"acknowledged {when}" : $"committed {when} although the client saw {clientFailure.GetType().Name}");
        if (clientFailure is not null && HistoryRecorder.Classify(clientFailure) == OpOutcome.Failed)
            verdict.Violation(ViolationKind.PolicyUnderAcknowledged, $"a write reported as definitely failed {when} is committed");
    }

    // ------------------------------------------------------------------------------------------------------------------
    // MUT-001 SkipFencingCheck: the stale-writer regression (§9.3).
    // ------------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Writer A's publication (generation g) is delayed in the network together with its renewals while its lease expires
    /// and writer B (generation &gt; g) commits; the delayed publication and a duplicate of it are then delivered. Detects
    /// any acceptance of the stale publication.
    /// </summary>
    /// <remarks>
    /// A's stale revision is below the inline threshold, so its publication needs no staging session or receipts: the
    /// writer-fencing check (handle validity + generation) is the only server-side barrier, and a closed staging session
    /// cannot mask the mutant. The delayed commit is delivered well inside A's session timeout (lease 2 s, session 10 s),
    /// so session expiry cannot mask it either.
    /// </remarks>
    public static Task<ScenarioVerdict> StaleWriterPublicationAsync(SafetyMutations m) =>
        RunScenarioAsync("stale-writer-publication", m, TestClusterOptions.ThreeZones, async (cluster, v) =>
        {
            const string path = MutDir + "/fenced";
            byte[] baseBytes = TestData.Bytes(20_001, MultiExtentLength);
            byte[] aBytes = TestData.Bytes(20_002, 1_500);   // inline-sized stale revision
            byte[] bBytes = TestData.Bytes(20_003, MultiExtentLength - 20_000);
            TestClientOptions aOptions = Client("wa", m, operationTimeout: TimeSpan.FromSeconds(60));
            await using DfsClient a = await cluster.ConnectAsync(aOptions, Ct);
            await using DfsClient b = await cluster.ConnectAsync(Client("wb", m), Ct);
            DfsVolume av = a.GetVolume(Vol);
            DfsVolume bv = b.GetVolume(Vol);
            await bv.Directories.CreateDirectoryAsync(MutDir, Ct);
            await bv.Files.WriteAllBytesAsync(path, baseBytes, Ct);

            DfsStream writerA = await av.Files.OpenAsync(path, new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.Write, Share = FileShare.Read }, Ct);
            ulong generationA = writerA.WriterGeneration;
            await writerA.WriteAsync(aBytes, Ct);
            await writerA.SetLengthAsync(aBytes.Length, Ct);
            MessageHold renewals = cluster.Network.Hold(new MessageFilter(From: aOptions.NetworkName, Type: MessageType.KeepAlive));
            MessageHold publication = cluster.Network.Hold(new MessageFilter(From: aOptions.NetworkName, Type: MessageType.CommitFile));
            Task<CommitToken> staleCommit = writerA.CommitAsync(Ct);
            bool captured = await WaitHeldAsync(publication, 1, TimeSpan.FromSeconds(30));
            v.Reached(captured, "the stale writer's CommitFile is in flight (captured)");
            if (!captured)
            {
                publication.Dispose();
                renewals.Dispose();
                return;
            }

            // Lease (2 s) expires server-side; OpenWriterEventuallyAsync waits for the expiry if it has not happened yet.
            await cluster.DelayAsync(Lease * 2, Ct);
            DfsStream writerB = await OpenWriterEventuallyAsync(cluster, bv, path, TimeSpan.FromSeconds(30));
            ulong generationB = writerB.WriterGeneration;
            v.Check(generationB > generationA, ViolationKind.StalePublicationAccepted, $"replacement generation {generationB} does not exceed expired {generationA}");
            await writerB.WriteAsync(bBytes, Ct);
            await writerB.SetLengthAsync(bBytes.Length, Ct);
            await writerB.CommitAsync(Ct);
            await writerB.DisposeAsync();   // nothing pending; frees the inode for the oracle's whole-file read

            CapturedMessage[] frames = publication.Captured.ToArray();
            publication.ReleaseAll();
            renewals.ReleaseAll();
            publication.Dispose();
            renewals.Dispose();
            Task finished = await Task.WhenAny(staleCommit, cluster.DelayAsync(TimeSpan.FromSeconds(90), Ct));
            if (!ReferenceEquals(finished, staleCommit))
                v.Error("the stale writer's commit never terminated");
            else if (staleCommit.IsCompletedSuccessfully)
                v.Violation(ViolationKind.StalePublicationAccepted, $"the expired writer's commit (generation {generationA}) succeeded after generation {generationB} committed");
            else
                v.Evidence($"stale commit rejected with {staleCommit.Exception?.InnerException?.GetType().Name}");

            foreach (CapturedMessage frame in frames)
            {
                DfsErrorCode code = await cluster.Network.ReplayAsync(frame);
                v.Check(code != DfsErrorCode.None, ViolationKind.StalePublicationAccepted, "a delayed duplicate of the stale CommitFile was accepted");
            }

            await using DfsClient check = await cluster.ConnectAsync(Client("check", m), Ct);
            byte[]? content = await ReadOracleAsync(v, check.GetVolume(Vol).Files, path, ViolationKind.LostAcknowledgedData);
            if (content is not null && content.AsSpan().SequenceEqual(aBytes))
                v.Violation(ViolationKind.StalePublicationAccepted, "the file holds the expired writer's bytes");
            else if (content is not null && !content.AsSpan().SequenceEqual(bBytes))
                v.Violation(ViolationKind.LostAcknowledgedData, "the replacement writer's acknowledged commit is not the current content");
            FileInspection? file = await cluster.Inspect.GetFileAsync(Vol, path);
            v.Check(file is not null && file.WriterGeneration == generationB, ViolationKind.StalePublicationAccepted,
                $"inode writer generation {file?.WriterGeneration} is not the replacement's {generationB}");

            await CaptureAsync(() => writerA.DisposeAsync().AsTask());
        });

    // ------------------------------------------------------------------------------------------------------------------
    // MUT-002 AcknowledgeBeforeFlush: power loss right after an acknowledgement (DSK-001 / §9.1).
    // ------------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Object flushes are slowed; a multi-extent write is acknowledged and every node immediately loses power
    /// (unflushed data lost). Detects acknowledged bytes that did not survive.
    /// </summary>
    public static Task<ScenarioVerdict> PowerLossAfterAcknowledgementAsync(SafetyMutations m) =>
        RunScenarioAsync("power-loss-after-ack", m, TestClusterOptions.ThreeZones, async (cluster, v) =>
        {
            const string path = MutDir + "/durable";
            byte[] bytes = TestData.Bytes(20_101, 2 * MultiExtentLength);
            await using (DfsClient c = await cluster.ConnectAsync(Client("c0", m, operationTimeout: TimeSpan.FromSeconds(60)), Ct))
            {
                DfsVolume vol = c.GetVolume(Vol);
                await vol.Directories.CreateDirectoryAsync(MutDir, Ct);
                FaultArm slowFlush = cluster.Faults.Arm(new FaultMatch(FaultSites.ObjectBeforeFlush, Count: 100_000), FaultAction.Delay(TimeSpan.FromSeconds(3)));
                long started = cluster.Time.GetTimestamp();
                await vol.Files.WriteAllBytesAsync(path, bytes, Ct);
                v.Evidence($"write acknowledged after {cluster.Time.GetElapsedTime(started)}");
                v.Reached(slowFlush.Triggered, "the object flush barrier (delayed)");

                foreach (TestNode node in cluster.Nodes)
                    await node.CrashAsync(CrashMode.LoseUnflushed);
                cluster.Faults.DisarmAll();
                foreach (TestNode node in cluster.Nodes)
                    await node.RestartAsync();
                await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), Ct);
            }

            await using DfsClient check = await cluster.ConnectAsync(Client("check", m), Ct);
            byte[]? content = await ReadOracleAsync(v, check.GetVolume(Vol).Files, path, ViolationKind.LostAcknowledgedData);
            if (content is not null)
                v.Check(content.AsSpan().SequenceEqual(bytes), ViolationKind.LostAcknowledgedData, "acknowledged bytes differ after power loss");
            await CheckDurableCopiesAsync(cluster, v, path, 2, 2, ViolationKind.LostAcknowledgedData, "after power loss");
        });

    // ------------------------------------------------------------------------------------------------------------------
    // MUT-003 CommitMetadataBeforeData: publication with a missing new object.
    // ------------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// With exactly two copies per object, a publication is held in flight after all receipts were issued; every holder of
    /// the new objects then loses them and restarts (new incarnation), so the in-flight receipts no longer describe durable
    /// data. Detects a published revision whose dependency closure is incomplete (also after a full restart).
    /// </summary>
    public static Task<ScenarioVerdict> StaleReceiptPublicationAsync(SafetyMutations m)
    {
        var twoCopies = new DfsStoragePolicy { TargetCopies = 2, TargetDistinctZones = 2, DurableCopiesBeforeAck = 2, DurableZonesBeforeAck = 2 };
        TestClusterOptions options = TestClusterOptions.ThreeZones with { Volumes = [new TestVolumeSpec(Vol) { DefaultPolicy = twoCopies }] };
        return RunScenarioAsync("missing-object-publication", m, options, async (cluster, v) =>
        {
            const string path = MutDir + "/closure";
            byte[] oldBytes = TestData.Bytes(20_201, MultiExtentLength);
            byte[] newBytes = TestData.Bytes(20_202, 2 * MultiExtentLength);
            TestClientOptions clientOptions = Client("c0", m, operationTimeout: TimeSpan.FromSeconds(120));
            await using DfsClient c = await cluster.ConnectAsync(clientOptions, Ct);
            DfsFileClient files = c.GetVolume(Vol).Files;
            await c.GetVolume(Vol).Directories.CreateDirectoryAsync(MutDir, Ct);
            await files.WriteAllBytesAsync(path, oldBytes, Ct);
            IReadOnlySet<ObjectId> before = await cluster.Inspect.GetAllStoredObjectsAsync(Vol);

            DfsStream stream = await files.OpenAsync(path, new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.Write, Share = FileShare.Read }, Ct);
            await stream.WriteAsync(newBytes, Ct);
            await stream.SetLengthAsync(newBytes.Length, Ct);
            using MessageHold commit = cluster.Network.Hold(new MessageFilter(From: clientOptions.NetworkName, Type: MessageType.CommitFile));
            Task<CommitToken> publish = stream.CommitAsync(Ct);
            bool held = await WaitHeldAsync(commit, 1, TimeSpan.FromSeconds(60));
            v.Reached(held, "CommitFile in flight after every receipt was issued");
            if (!held)
                return;

            ObjectId[] newObjects = (await cluster.Inspect.GetAllStoredObjectsAsync(Vol)).Where(o => !before.Contains(o)).ToArray();
            v.Evidence($"{newObjects.Length} new objects staged");
            var byHolder = new Dictionary<string, List<ObjectId>>(StringComparer.Ordinal);
            foreach (ObjectId obj in newObjects)
            {
                foreach (string holder in await cluster.Inspect.GetObjectHoldersAsync(obj))
                {
                    if (!byHolder.TryGetValue(holder, out List<ObjectId>? list))
                        byHolder[holder] = list = [];
                    list.Add(obj);
                }
            }

            foreach ((string holder, List<ObjectId> objects) in byHolder)
            {
                TestNode node = cluster.Node(holder);
                await node.CrashAsync();
                foreach (ObjectId obj in objects)
                    await node.Disk.DeleteObjectAsync(obj);
                await node.RestartAsync();
                await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), Ct);
            }

            v.Reached(byHolder.Count > 0, "holders of the new objects lost them and restarted with a new incarnation");
            commit.ReleaseAll();
            Task finished = await Task.WhenAny(publish, cluster.DelayAsync(TimeSpan.FromSeconds(120), Ct));
            bool reportedCommitted = ReferenceEquals(finished, publish) && publish.IsCompletedSuccessfully;
            v.Evidence(reportedCommitted ? "publication reported success" : $"publication did not succeed ({publish.Status})");

            async Task CheckPublishedClosureAsync(string when)
            {
                FileInspection? file = await cluster.Inspect.GetFileAsync(Vol, path);
                if (file is null)
                {
                    v.Violation(ViolationKind.PublishedMissingDependency, $"{when}: {path} vanished");
                    return;
                }

                foreach (ObjectId obj in file.Extents.Select(e => e.Object).Distinct())
                {
                    int copies = (await cluster.Inspect.GetObjectHoldersAsync(obj)).Count;
                    v.Check(copies >= twoCopies.DurableCopiesBeforeAck, ViolationKind.PublishedMissingDependency,
                        $"{when}: published revision {file.Revision} references {obj} with {copies} verified copies");
                }

                await using DfsClient check = await cluster.ConnectAsync(Client($"check-{when.Replace(' ', '-')}", m), Ct);
                byte[]? content = await ReadOracleAsync(v, check.GetVolume(Vol).Files, path, ViolationKind.PublishedMissingDependency);
                if (content is null)
                    return;
                bool isOld = content.AsSpan().SequenceEqual(oldBytes);
                bool isNew = content.AsSpan().SequenceEqual(newBytes);
                v.Check(isOld || isNew, ViolationKind.PublishedMissingDependency, $"{when}: content is neither the old nor the new complete revision");
                v.Check(!reportedCommitted || isNew, ViolationKind.LostAcknowledgedData, $"{when}: a reported commit is not visible");
            }

            await CheckPublishedClosureAsync("after publication");
            foreach (TestNode node in cluster.Nodes)
                await node.CrashAsync();
            foreach (TestNode node in cluster.Nodes)
                await node.RestartAsync();
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), Ct);
            await CheckPublishedClosureAsync("after full restart");
            await CaptureAsync(() => stream.DisposeAsync().AsTask());
        });
    }

    // ------------------------------------------------------------------------------------------------------------------
    // MUT-004 QuorumFromReachableNodes: minority partition safety.
    // ------------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Five voters are split 2 (old leader + one follower, with client A) / 3 (with client B); A and B alternate strictly
    /// sequential writes and reads of one register. Detects a non-linearizable register history (the minority acknowledged
    /// writes or served reads it had no authority for) and split brain.
    /// </summary>
    public static Task<ScenarioVerdict> MinorityPartitionAsync(SafetyMutations m) =>
        RunScenarioAsync("minority-partition", m, TestClusterOptions.FiveNodes, async (cluster, v) =>
        {
            const string key = "/mut-register";
            TestNode oldLeader = await cluster.GetLeaderAsync(Vol, Ct);
            TestNode[] others = cluster.Nodes.Where(n => n.Name != oldLeader.Name).ToArray();
            TestNode buddy = others[0];
            TestNode[] majority = others.Skip(1).ToArray();
            TestClientOptions aOptions = Client("pa", m, [oldLeader.Name, buddy.Name], TimeSpan.FromSeconds(3));
            TestClientOptions bOptions = Client("pb", m, Names(majority), TimeSpan.FromSeconds(5));
            await using DfsClient a = await cluster.ConnectAsync(aOptions, Ct);
            await using DfsClient b = await cluster.ConnectAsync(bOptions, Ct);
            DfsFileClient af = a.GetVolume(Vol).Files;
            DfsFileClient bf = b.GetVolume(Vol).Files;
            byte[] Value(int i) => TestData.Bytes(20_400 + (ulong)i, 64);
            await RecordAsync(cluster, "pb", "write", key, Value(0), () => bf.WriteAllBytesAsync(key, Value(0), Ct));
            await RecordReadAsync(cluster, "pa", key, af);

            var leaders = new Dictionary<(string Group, ulong Term), string>();
            cluster.Network.Partition([oldLeader.Name, buddy.Name, aOptions.NetworkName], [.. Names(majority), bOptions.NetworkName]);
            bool elected = await TryWaitUntilAsync(cluster, async () =>
            {
                foreach (TestNode node in majority)
                {
                    GroupInspection? g = await node.Inspect.GetGroupAsync(Vol);
                    if (g is not null && g.Role == RaftRole.Leader)
                        return true;
                }

                return false;
            }, TimeSpan.FromSeconds(30), "the majority elects a leader");
            v.Reached(elected, "majority-side leader elected during the partition");

            int minorityAcks = 0;
            async Task SampleAsync()
            {
                string? split = await SampleLeadersAsync(cluster, leaders);
                if (split is not null)
                    v.Violation(ViolationKind.SplitBrain, split);
            }

            await RecordAsync(cluster, "pa", "write", key, Value(1), async () => { await af.WriteAllBytesAsync(key, Value(1), Ct); minorityAcks++; });
            await SampleAsync();
            await RecordAsync(cluster, "pb", "write", key, Value(2), () => bf.WriteAllBytesAsync(key, Value(2), Ct));
            await RecordReadAsync(cluster, "pa", key, af);
            await RecordReadAsync(cluster, "pb", key, bf);
            await SampleAsync();
            await RecordAsync(cluster, "pa", "write", key, Value(3), async () => { await af.WriteAllBytesAsync(key, Value(3), Ct); minorityAcks++; });
            await RecordReadAsync(cluster, "pb", key, bf);
            await RecordReadAsync(cluster, "pa", key, af);
            await SampleAsync();
            v.Check(minorityAcks == 0, ViolationKind.MinorityAuthority, $"the 2-of-5 minority acknowledged {minorityAcks} writes");

            cluster.Network.Heal();
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), Ct);
            await RecordReadAsync(cluster, "pa", key, af);
            await RecordReadAsync(cluster, "pb", key, bf);
            v.History(LinearizabilityChecker.Check(cluster.History.Operations.Where(o => o.Key == key).ToArray(), new RegisterModel()),
                ViolationKind.NonLinearizableHistory, "register history across the 2/3 partition");
        });

    // ------------------------------------------------------------------------------------------------------------------
    // MUT-005 DisableOperationDedup: duplicate append and name reuse.
    // ------------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// An append's reply is lost and the application retries with the same id; a move's reply is lost, the source name is
    /// reused by a new inode and the move is retried with the same id. Detects any second effect of one identity.
    /// </summary>
    public static Task<ScenarioVerdict> DuplicateRetryAsync(SafetyMutations m) =>
        RunScenarioAsync("duplicate-retry", m, TestClusterOptions.ThreeZones, async (cluster, v) =>
        {
            TestClientOptions options = Client("c0", m, operationTimeout: TimeSpan.FromSeconds(3));
            await using DfsClient c = await cluster.ConnectAsync(options, Ct);
            DfsVolume vol = c.GetVolume(Vol);
            DfsFileClient files = vol.Files;
            await vol.Directories.CreateDirectoryAsync(MutDir, Ct);
            byte[] prefix = TestData.Bytes(20_501, 10_000);
            byte[] suffix = TestData.Bytes(20_502, 3_000);
            await files.WriteAllBytesAsync(MutDir + "/log", prefix, Ct);

            DfsOperationId appendId = await vol.NewOperationIdAsync(Ct);
            MessageRule lostAppend = cluster.Network.DropReplies(new MessageFilter(From: options.NetworkName, Type: MessageType.Append), count: 1);
            Exception? first = await CaptureAsync(() => files.AppendAllBytesAsync(MutDir + "/log", suffix, new DfsMutationOptions { OperationId = appendId }, Ct));
            v.Reached(lostAppend.Matched >= 1, "append executed with its reply lost");
            Exception? retry = await CaptureAsync(() => files.AppendAllBytesAsync(MutDir + "/log", suffix, new DfsMutationOptions { OperationId = appendId }, Ct));
            v.Evidence($"append: first {first?.GetType().Name ?? "ok"}, retry {retry?.GetType().Name ?? "ok"}");
            byte[]? log = await ReadOracleAsync(v, files, MutDir + "/log", ViolationKind.LostAcknowledgedData);
            if (log is not null)
            {
                v.Check(log.Length <= prefix.Length + suffix.Length, ViolationKind.DuplicateEffect,
                    $"append id {appendId} took effect {(log.Length - prefix.Length) / suffix.Length} times");
                v.Check(log.AsSpan().SequenceEqual([.. prefix, .. suffix]), ViolationKind.DuplicateEffect, "log is not exactly prefix + one suffix");
            }

            byte[] original = TestData.Bytes(20_503, 4_000);
            byte[] reused = TestData.Bytes(20_504, 4_000);
            await files.WriteAllBytesAsync(MutDir + "/src", original, Ct);
            DfsOperationId moveId = await vol.NewOperationIdAsync(Ct);
            MessageRule lostMove = cluster.Network.DropReplies(new MessageFilter(From: options.NetworkName, Type: MessageType.Move), count: 1);
            Exception? moveFirst = await CaptureAsync(() => files.MoveAsync(MutDir + "/src", MutDir + "/dst", overwrite: true, new DfsMutationOptions { OperationId = moveId }, Ct));
            v.Reached(lostMove.Matched >= 1, "move executed with its reply lost");
            await files.WriteAllBytesAsync(MutDir + "/src", reused, Ct);   // the source name is reused by a new inode
            Exception? moveRetry = await CaptureAsync(() => files.MoveAsync(MutDir + "/src", MutDir + "/dst", overwrite: true, new DfsMutationOptions { OperationId = moveId }, Ct));
            v.Evidence($"move: first {moveFirst?.GetType().Name ?? "ok"}, retry {moveRetry?.GetType().Name ?? "ok"}");
            byte[]? dst = await ReadOracleAsync(v, files, MutDir + "/dst", ViolationKind.LostAcknowledgedData);
            byte[]? src = await TryReadAllAsync(files, MutDir + "/src");
            v.Check(dst is not null && dst.AsSpan().SequenceEqual(original), ViolationKind.DuplicateEffect, "the retried move re-executed against the reused name (destination replaced)");
            v.Check(src is not null && src.AsSpan().SequenceEqual(reused), ViolationKind.DuplicateEffect, "the retried move consumed the reused source name");
        });

    // ------------------------------------------------------------------------------------------------------------------
    // MUT-006 TreatUnknownAsAborted (SDK): later committed effect after a claimed abort.
    // ------------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// A write and a transaction commit execute but every reply and outcome query reply to the client is lost. The
    /// history holds the SDK to its claims (a definite failure, including cancellation, is taken as "did not happen").
    /// Detects a reported abort whose operation is later found committed (outcome query and history check).
    /// </summary>
    public static Task<ScenarioVerdict> LostReplyOutcomeAsync(SafetyMutations m) =>
        RunScenarioAsync("outcome-after-lost-reply", m, TestClusterOptions.ThreeZones, async (cluster, v) =>
        {
            const string key = "/mut-outcome";
            HistoryRecorder history = cluster.History;
            history.TrustCancellationAsAbort = true;
            TestClientOptions options = Client("c0", m, operationTimeout: TimeSpan.FromSeconds(2));
            await using DfsClient c = await cluster.ConnectAsync(options, Ct);
            DfsVolume vol = c.GetVolume(Vol);
            byte[] v0 = TestData.Bytes(20_601, 64);
            byte[] v1 = TestData.Bytes(20_602, 64);
            await RecordAsync(cluster, "c0", "write", key, v0, () => vol.Files.WriteAllBytesAsync(key, v0, Ct));

            DfsOperationId writeId = await vol.NewOperationIdAsync(Ct);
            MessageRule lostWrite = cluster.Network.DropReplies(new MessageFilter(From: options.NetworkName, Type: MessageType.WriteAll), 10_000);
            MessageRule lostQuery = cluster.Network.DropReplies(new MessageFilter(From: options.NetworkName, Type: MessageType.QueryOperation), 10_000);
            long id = history.Invoke("c0", "write", key, v1, writeId);
            OpOutcome writeReported;
            try
            {
                await vol.Files.WriteAllBytesAsync(key, v1, new DfsMutationOptions { OperationId = writeId }, Ct);
                writeReported = OpOutcome.Ok;
                history.Complete(id, OpOutcome.Ok);
            }
            catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
            {
                writeReported = HistoryRecorder.Classify(ex, requestMayHaveBeenSent: true, trustCancellation: true);
                history.Complete(id, writeReported, ex);
                v.Evidence($"write reported {writeReported} via {ex.GetType().Name}");
            }

            v.Reached(lostWrite.Matched >= 1, "write executed with its reply lost");

            DfsTransaction tx = await vol.Transactions.BeginAsync(ct: Ct);
            await tx.Files.WriteAllTextAsync("/mut-outcome-tx", "committed", ct: Ct);
            MessageRule lostCommit = cluster.Network.DropReplies(new MessageFilter(From: options.NetworkName, Type: MessageType.TxCommit), 10_000);
            OpOutcome txReported;
            try
            {
                await tx.CommitAsync(Ct);
                txReported = OpOutcome.Ok;
            }
            catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
            {
                txReported = HistoryRecorder.Classify(ex, requestMayHaveBeenSent: true, trustCancellation: true);
                v.Evidence($"transaction commit reported {txReported} via {ex.GetType().Name}");
            }

            v.Reached(lostCommit.Matched >= 1, "transaction commit executed with its reply lost");
            lostWrite.Cancel();
            lostQuery.Cancel();
            lostCommit.Cancel();

            await using DfsClient check = await cluster.ConnectAsync(Client("check", m), Ct);
            DfsVolume cv = check.GetVolume(Vol);
            await RecordReadAsync(cluster, "check", key, cv.Files);
            DfsOperationOutcome writeOutcome = await cv.QueryOperationAsync(writeId, resolve: true, Ct);
            DfsOperationOutcome txOutcome = await cv.Transactions.QueryOutcomeAsync(tx.Id, resolve: true, Ct);
            v.Check(!(writeReported == OpOutcome.Failed && writeOutcome.Status == DfsOutcomeStatus.Committed), ViolationKind.CommittedAfterReportedAbort,
                $"the write was reported as definitely failed but is committed at {writeOutcome.Token}");
            v.Check(!(txReported == OpOutcome.Failed && txOutcome.Status == DfsOutcomeStatus.Committed), ViolationKind.CommittedAfterReportedAbort,
                $"the transaction was reported aborted but is committed at {txOutcome.Token}");
            v.History(LinearizabilityChecker.Check(history.Operations.Where(o => o.Key == key).ToArray(), new RegisterModel()),
                ViolationKind.CommittedAfterReportedAbort, "register history with the client's reported outcomes");
            await CaptureAsync(() => tx.DisposeAsync().AsTask());
        });

    // ------------------------------------------------------------------------------------------------------------------
    // MUT-007 SkipPredicateValidation: phantom / write-skew through absent names and enumeration.
    // ------------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Two transaction pairs. Absent names: T1 checks that /mut-d/x is absent and creates /mut-f/y; T2 checks that
    /// /mut-f/y is absent and creates /mut-d/x. Enumeration: T3 enumerates /mut-e and inserts into /mut-g; T4 enumerates
    /// /mut-g and inserts into /mut-e. Within each pair the write sets (files and parent directories) are disjoint and no
    /// existing item is read, so the absent-name / enumeration predicate is the only conflict: with it, the second commit
    /// of each pair must abort; without it, both commit and the history has no strictly serializable order.
    /// </summary>
    public static Task<ScenarioVerdict> PhantomWriteSkewAsync(SafetyMutations m) =>
        RunScenarioAsync("phantom-write-skew", m, TestClusterOptions.ThreeZones, async (cluster, v) =>
        {
            await using DfsClient c = await cluster.ConnectAsync(Client("c0", m), Ct);
            DfsVolume vol = c.GetVolume(Vol);
            foreach (string dir in new[] { "/mut-d", "/mut-e", "/mut-f", "/mut-g" })
                await vol.Directories.CreateDirectoryAsync(dir, Ct);
            await vol.Files.WriteAllTextAsync("/mut-d/a", "1", ct: Ct);
            await vol.Files.WriteAllTextAsync("/mut-e/a", "1", ct: Ct);
            await vol.Files.WriteAllTextAsync("/mut-g/b", "1", ct: Ct);
            var clock = new Cha.LogicalClock();
            var records = new List<TxRecord>();
            var none = new Dictionary<string, string?>(StringComparer.Ordinal);

            static async Task<OpOutcome> CommitAsync(DfsTransaction tx)
            {
                try
                {
                    await tx.CommitAsync(Ct);
                    return OpOutcome.Ok;
                }
                catch (DfsTransactionConflictException)
                {
                    return OpOutcome.Failed;
                }
                catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
                {
                    return HistoryRecorder.Classify(ex);
                }
                finally
                {
                    await tx.DisposeAsync();
                }
            }

            // Absent-name dependencies (write skew through phantoms).
            long b1 = clock.Tick();
            DfsTransaction t1 = await vol.Transactions.BeginAsync(ct: Ct);
            long b2 = clock.Tick();
            DfsTransaction t2 = await vol.Transactions.BeginAsync(ct: Ct);
            bool t1SawX = await t1.Files.ExistsAsync("/mut-d/x", Ct);
            bool t2SawY = await t2.Files.ExistsAsync("/mut-f/y", Ct);
            if (t1SawX || t2SawY)
                v.Error($"setup: the names checked for absence exist (x={t1SawX}, y={t2SawY})");
            await t1.Files.WriteAllTextAsync("/mut-f/y", "t1", ct: Ct);
            await t2.Files.WriteAllTextAsync("/mut-d/x", "t2", ct: Ct);
            OpOutcome o1 = await CommitAsync(t1);
            long e1 = clock.Tick();
            OpOutcome o2 = await CommitAsync(t2);
            long e2 = clock.Tick();
            v.Evidence($"absent-name pair: outcomes {o1}/{o2}");
            records.Add(new TxRecord("absent-1", b1, e1, o1, new Dictionary<string, string?>(StringComparer.Ordinal) { ["/mut-d/x"] = null },
                new Dictionary<string, string?>(StringComparer.Ordinal) { ["/mut-f/y"] = "t1" }, []));
            records.Add(new TxRecord("absent-2", b2, e2, o2, new Dictionary<string, string?>(StringComparer.Ordinal) { ["/mut-f/y"] = null },
                new Dictionary<string, string?>(StringComparer.Ordinal) { ["/mut-d/x"] = "t2" }, []));
            v.Check(!(o1 == OpOutcome.Ok && o2 == OpOutcome.Ok), ViolationKind.NonSerializableTransactions,
                "both absent-name transactions committed (write skew through phantoms)");
            if (o1 != OpOutcome.Ok)
                v.Error($"the first absent-name transaction did not commit ({o1}); the pair is not a conflict test");

            // Enumeration predicates (phantom inserts).
            long b3 = clock.Tick();
            DfsTransaction t3 = await vol.Transactions.BeginAsync(ct: Ct);
            long b4 = clock.Tick();
            DfsTransaction t4 = await vol.Transactions.BeginAsync(ct: Ct);
            var options = new DfsEnumerationOptions { Kinds = DfsEntryKinds.Files };
            HashSet<string> seen3 = [];
            await foreach (DfsEntryInfo entry in t3.Directories.EnumerateAsync("/mut-e", options, Ct))
                seen3.Add(entry.Name);
            HashSet<string> seen4 = [];
            await foreach (DfsEntryInfo entry in t4.Directories.EnumerateAsync("/mut-g", options, Ct))
                seen4.Add(entry.Name);
            if (!seen3.SetEquals(["a"]) || !seen4.SetEquals(["b"]))
                v.Error($"setup: unexpected enumerations /mut-e={{{string.Join(",", seen3)}}} /mut-g={{{string.Join(",", seen4)}}}");
            await t3.Files.WriteAllTextAsync("/mut-g/p", "t3", ct: Ct);
            await t4.Files.WriteAllTextAsync("/mut-e/q", "t4", ct: Ct);
            OpOutcome o3 = await CommitAsync(t3);
            long e3 = clock.Tick();
            OpOutcome o4 = await CommitAsync(t4);
            long e4 = clock.Tick();
            v.Evidence($"enumeration pair: outcomes {o3}/{o4}");
            records.Add(new TxRecord("enum-3", b3, e3, o3, none, new Dictionary<string, string?>(StringComparer.Ordinal) { ["/mut-g/p"] = "t3" }, [("/mut-e", seen3)]));
            records.Add(new TxRecord("enum-4", b4, e4, o4, none, new Dictionary<string, string?>(StringComparer.Ordinal) { ["/mut-e/q"] = "t4" }, [("/mut-g", seen4)]));
            v.Check(!(o3 == OpOutcome.Ok && o4 == OpOutcome.Ok), ViolationKind.NonSerializableTransactions,
                "both enumerating transactions committed (phantom insert)");
            if (o3 != OpOutcome.Ok)
                v.Error($"the first enumerating transaction did not commit ({o3}); the pair is not a conflict test");

            var initial = new Dictionary<string, string?>(StringComparer.Ordinal) { ["/mut-d/a"] = "1", ["/mut-e/a"] = "1", ["/mut-g/b"] = "1" };
            v.History(SerializabilityChecker.CheckStrict(records, initial), ViolationKind.NonSerializableTransactions, "strict serializability of the transaction history");
        });

    // ------------------------------------------------------------------------------------------------------------------
    // MUT-008 AllowStaleLiveReads: a deposed leader serving live reads.
    // ------------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// The old leader keeps believing it leads (its clock is frozen, so it never steps down) while partitioned with client
    /// A; the majority elects a new leader and B's write completes; then A reads. Detects a read returning the
    /// pre-partition value after B's write completed (real-time consistency).
    /// </summary>
    public static Task<ScenarioVerdict> StaleLeaderReadAsync(SafetyMutations m) =>
        RunScenarioAsync("stale-leader-read", m, TestClusterOptions.ThreeZones, async (cluster, v) =>
        {
            const string key = "/mut-live";
            TestNode oldLeader = await cluster.GetLeaderAsync(Vol, Ct);
            TestNode[] rest = cluster.Nodes.Where(n => n.Name != oldLeader.Name).ToArray();
            TestClientOptions aOptions = Client("ra", m, [oldLeader.Name], TimeSpan.FromSeconds(3));
            TestClientOptions bOptions = Client("rb", m, Names(rest), TimeSpan.FromSeconds(5));
            await using DfsClient a = await cluster.ConnectAsync(aOptions, Ct);
            await using DfsClient b = await cluster.ConnectAsync(bOptions, Ct);
            DfsFileClient af = a.GetVolume(Vol).Files;
            DfsFileClient bf = b.GetVolume(Vol).Files;
            byte[] v1 = TestData.Bytes(20_801, 64);
            byte[] v2 = TestData.Bytes(20_802, 64);
            await RecordAsync(cluster, "rb", "write", key, v1, () => bf.WriteAllBytesAsync(key, v1, Ct));
            await RecordReadAsync(cluster, "ra", key, af);

            oldLeader.Clock.Freeze();
            cluster.Network.Partition([oldLeader.Name, aOptions.NetworkName], [.. Names(rest), bOptions.NetworkName]);
            bool elected = await TryWaitUntilAsync(cluster, async () =>
            {
                foreach (TestNode node in rest)
                {
                    GroupInspection? g = await node.Inspect.GetGroupAsync(Vol);
                    if (g is not null && g.Role == RaftRole.Leader)
                        return true;
                }

                return false;
            }, TimeSpan.FromSeconds(30), "the majority elects a new leader");
            v.Reached(elected, "new leader elected while the old one still believes it leads");
            await RecordAsync(cluster, "rb", "write", key, v2, () => bf.WriteAllBytesAsync(key, v2, Ct));
            byte[]? seen = await RecordReadAsync(cluster, "ra", key, af);
            if (seen is not null && seen.AsSpan().SequenceEqual(v1))
                v.Violation(ViolationKind.StaleLiveRead, "the deposed leader served the pre-partition value as a live read after a newer write completed");

            cluster.Network.Heal();
            oldLeader.Clock.Unfreeze();
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), Ct);
            await RecordReadAsync(cluster, "ra", key, af);
            await RecordReadAsync(cluster, "rb", key, bf);
            v.History(LinearizabilityChecker.Check(cluster.History.Operations.Where(o => o.Key == key).ToArray(), new RegisterModel()),
                ViolationKind.StaleLiveRead, "live-read history across the partition");
        });

    // ------------------------------------------------------------------------------------------------------------------
    // MUT-009 IgnoreSnapshotPins: GC versus snapshot roots and reader pins.
    // ------------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Two overwritten revisions stay protected while GC demonstrably runs (it reclaims unrelated garbage): /h's old revision
    /// only by a snapshot root, /p's old revision only by a version-pinned reader (it was written after the snapshot).
    /// Detects pinned history that was reclaimed or became unreadable, for either kind of root.
    /// </summary>
    public static Task<ScenarioVerdict> PinnedHistoryGcAsync(SafetyMutations m) =>
        RunScenarioAsync("pinned-history-gc", m, TestClusterOptions.ThreeZones, async (cluster, v) =>
        {
            const string dir = "/mut-gc";
            byte[] h1 = TestData.Bytes(20_901, MultiExtentLength);
            byte[] p1 = TestData.Bytes(20_903, MultiExtentLength);
            await using DfsClient c = await cluster.ConnectAsync(Client("c0", m), Ct);
            DfsVolume vol = c.GetVolume(Vol);
            await vol.Directories.CreateDirectoryAsync(dir, Ct);
            await vol.Files.WriteAllBytesAsync(dir + "/h", h1, Ct);
            await vol.Snapshots.CreateAsync("mut-gc-s1", ct: Ct);   // snapshot root holds h1 only (/p does not exist yet)
            await vol.Files.WriteAllBytesAsync(dir + "/p", p1, Ct);
            var snapshotOnly = new HashSet<ObjectId>(await ObjectsOfAsync(cluster, dir + "/h"));
            var protectedObjects = new HashSet<ObjectId>(snapshotOnly);
            protectedObjects.UnionWith(await ObjectsOfAsync(cluster, dir + "/p"));   // p1: protected only by the reader pin below
            DfsStream pinned = await vol.Files.OpenAsync(dir + "/p", new DfsOpenOptions
            {
                Mode = FileMode.Open,
                Access = FileAccess.Read,
                Share = FileShare.ReadWrite | FileShare.Delete,
                ReadView = DfsReadView.VersionPinned,
            }, Ct);
            await vol.Files.WriteAllBytesAsync(dir + "/h", TestData.Bytes(20_902, MultiExtentLength), Ct);
            await vol.Files.WriteAllBytesAsync(dir + "/p", TestData.Bytes(20_904, MultiExtentLength), Ct);

            var garbage = new HashSet<ObjectId>();
            for (int i = 0; i < 4; i++)
            {
                if (i > 0)
                    garbage.UnionWith(await ObjectsOfAsync(cluster, dir + "/g"));
                await vol.Files.WriteAllBytesAsync(dir + "/g", TestData.Bytes(20_910 + (ulong)i, MultiExtentLength), Ct);
            }

            bool gcRan = await TryWaitUntilAsync(cluster, async () =>
            {
                IReadOnlySet<ObjectId> stored = await cluster.Inspect.GetAllStoredObjectsAsync(Vol);
                return !garbage.Any(stored.Contains);
            }, TimeSpan.FromSeconds(60), "GC reclaims unreferenced garbage");
            v.Reached(gcRan, "GC sweep (unreferenced garbage reclaimed)");
            await cluster.DelayAsync(TimeSpan.FromSeconds(3), Ct);   // more GC rounds with the pinned objects in scope

            foreach (ObjectId obj in protectedObjects)
            {
                int copies = (await cluster.Inspect.GetObjectHoldersAsync(obj)).Count;
                string root = snapshotOnly.Contains(obj) ? "snapshot-root" : "reader-pin";
                v.Check(copies > 0, ViolationKind.ReclaimedProtectedData, $"{root}-protected object {obj} has no copy left");
            }

            await using (DfsSnapshotView view = await vol.Snapshots.OpenAsync("mut-gc-s1", Ct))
            {
                byte[]? fromSnapshot = await ReadOracleAsync(v, view.Files, dir + "/h", ViolationKind.ReclaimedProtectedData);
                if (fromSnapshot is not null)
                    v.Check(fromSnapshot.AsSpan().SequenceEqual(h1), ViolationKind.ReclaimedProtectedData, "snapshot read returned bytes other than the snapshot revision");
            }

            try
            {
                pinned.Seek(0, SeekOrigin.Begin);
                byte[] fromPin = await ReadToEndAsync(pinned);
                v.Check(fromPin.AsSpan().SequenceEqual(p1), ViolationKind.ReclaimedProtectedData, "version-pinned read returned bytes other than the pinned revision");
            }
            catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
            {
                v.Violation(ViolationKind.ReclaimedProtectedData, $"version-pinned read failed: {ex.GetType().Name}: {ex.Message}");
            }

            await pinned.DisposeAsync();
        });

    // ------------------------------------------------------------------------------------------------------------------
    // MUT-010 PublishBackupBeforeClosure: interrupted transfer around publication (§9.5).
    // ------------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// The backup holds S1; a dependency S2 shares with S1 silently disappears at the destination (its inventory may still
    /// claim it); S2 is transferred and the backup loses power immediately after persisting its published pointer. Detects
    /// a published snapshot whose content or dependency closure is incomplete after the cold restart.
    /// </summary>
    public static Task<ScenarioVerdict> InterruptedBackupPublicationAsync(SafetyMutations m) =>
        RunScenarioAsync("interrupted-backup-publication", m, TestClusterOptions.ThreeZones with { BackupNodeCount = 1 }, async (cluster, v) =>
        {
            TestBackupNode backup = cluster.BackupNodes[0];
            string target = backup.TargetName;
            await backup.RegisterAsync(Vol, new DfsBackupSchedule(), new DfsBackupBudgets(TimeSpan.FromHours(1), 1L << 30));
            await using DfsClient c = await cluster.ConnectAsync(Client("c0", m), Ct);
            DfsVolume vol = c.GetVolume(Vol);
            await vol.Directories.CreateDirectoryAsync("/mut-bk", Ct);
            var s1 = new Dictionary<string, byte[]>(StringComparer.Ordinal)
            {
                ["/mut-bk/f1"] = TestData.Bytes(21_001, MultiExtentLength),
                ["/mut-bk/f2"] = TestData.Bytes(21_002, MultiExtentLength),
            };
            foreach ((string path, byte[] bytes) in s1)
                await vol.Files.WriteAllBytesAsync(path, bytes, Ct);
            await vol.Snapshots.CreateAsync("mut-bk-s1", ct: Ct);
            await vol.Backups.RequestSyncAsync(target, "mut-bk-s1", Ct);
            await vol.Backups.AwaitBackupSnapshotAsync(target, "mut-bk-s1", TimeSpan.FromSeconds(60), Ct);
            string? pointer1 = await backup.Inspect.GetPublishedPointerAsync(Vol);

            var s2 = new Dictionary<string, byte[]>(s1, StringComparer.Ordinal)
            {
                ["/mut-bk/f2"] = TestData.Bytes(21_012, MultiExtentLength),
                ["/mut-bk/f3"] = TestData.Bytes(21_013, 2 * MultiExtentLength),
            };
            await vol.Files.WriteAllBytesAsync("/mut-bk/f2", s2["/mut-bk/f2"], Ct);
            await vol.Files.WriteAllBytesAsync("/mut-bk/f3", s2["/mut-bk/f3"], Ct);
            await vol.Snapshots.CreateAsync("mut-bk-s2", ct: Ct);
            var s2Objects = new HashSet<ObjectId>();
            foreach (string path in s2.Keys)
                s2Objects.UnionWith(await ObjectsOfAsync(cluster, path));

            ObjectId shared = (await ObjectsOfAsync(cluster, "/mut-bk/f1"))[0];
            bool heldShared = (await backup.Inspect.ListObjectsAsync()).Any(o => o.Id == shared);
            v.Reached(heldShared, "the backup held the dependency S2 shares with S1");
            await backup.Disk.DeleteObjectAsync(shared);

            FaultArm afterPointer = cluster.Faults.Arm(new FaultMatch(FaultSites.BackupAfterPointer, Node: backup.Name), FaultAction.CrashNode(CrashMode.LoseUnflushed));
            await vol.Backups.RequestSyncAsync(target, "mut-bk-s2", Ct);
            bool published = await WaitTriggeredAsync(afterPointer, TimeSpan.FromSeconds(90));
            v.Reached(published, "S2 publication point (pointer persisted, then power loss)");
            if (backup.State != TestNodeState.Running)
                await backup.RestartAsync();

            IReadOnlyList<string> list = await backup.Inspect.GetPublishedSnapshotsAsync(Vol);
            if (list.Contains("mut-bk-s2"))
            {
                await using DfsClient reader = await cluster.ConnectBackupAsync(0, Client("b-reader", m) with { Credentials = backup.ReaderCredentials }, Ct);
                await using DfsSnapshotView view = await reader.GetVolume(Vol).Snapshots.OpenAsync("mut-bk-s2", Ct);
                foreach ((string path, byte[] bytes) in s2)
                {
                    byte[]? content = await ReadOracleAsync(v, view.Files, path, ViolationKind.IncompleteBackupPublished);
                    if (content is not null)
                        v.Check(content.AsSpan().SequenceEqual(bytes), ViolationKind.IncompleteBackupPublished, $"published S2 {path} differs from the source revision");
                }

                HashSet<ObjectId> atBackup = (await backup.Inspect.ListObjectsAsync()).Select(o => o.Id).ToHashSet();
                foreach (ObjectId obj in s2Objects)
                    v.Check(atBackup.Contains(obj), ViolationKind.IncompleteBackupPublished, $"published S2 lacks dependency {obj} at the backup");
            }
            else
            {
                v.Check(string.Equals(pointer1, await backup.Inspect.GetPublishedPointerAsync(Vol), StringComparison.Ordinal),
                    ViolationKind.IncompleteBackupPublished, "the published pointer moved although S2 is not published");
            }
        });

    // ------------------------------------------------------------------------------------------------------------------
    // MUT-011 SkipChecksumVerification: corrupted replicas.
    // ------------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Every replica of a file's objects is corrupted (including a byte inside the range read); a cold client reads it.
    /// Detects corrupted bytes delivered as a successful read. Read-path checksum verification is made the only check
    /// standing between the corrupt replicas and the caller: background scrubbing is pushed out of the scenario window (a
    /// scrub quarantining them first would mask the mutant) and encryption at rest is off (an AES-GCM tag failure would
    /// reject the flipped ciphertext independently of the checksum).
    /// </summary>
    public static Task<ScenarioVerdict> CorruptReplicaReadAsync(SafetyMutations m) =>
        RunScenarioAsync("corrupt-replica-read", m,
            TestClusterOptions.ThreeZones with
            {
                EncryptAtRest = false,
                Timings = NodeTimings.Fast with { ScrubInterval = TimeSpan.FromHours(1) },
            },
            async (cluster, v) =>
        {
            const string path = "/mut-cr/f";
            byte[] bytes = TestData.Bytes(21_101, MultiExtentLength);
            await using (DfsClient setup = await cluster.ConnectAsync(Client("setup", m), Ct))
            {
                await setup.GetVolume(Vol).Directories.CreateDirectoryAsync("/mut-cr", Ct);
                await setup.GetVolume(Vol).Files.WriteAllBytesAsync(path, bytes, Ct);
            }

            await cluster.WaitForRepairAsync(Vol, TimeSpan.FromSeconds(30), Ct);
            (ObjectId rangeObject, long rangeOffset) = await LocateAsync(cluster, path, 70_033);   // inside the range read below
            int corrupted = 0;
            foreach (ObjectId obj in await ObjectsOfAsync(cluster, path))
            {
                foreach (TestNode node in cluster.Nodes)
                {
                    if (await node.Inspect.HasObjectAsync(obj))
                    {
                        await node.Disk.CorruptObjectAsync(obj, offset: 33);
                        if (obj == rangeObject)
                            await node.Disk.CorruptObjectAsync(obj, rangeOffset);
                        corrupted++;
                    }
                }
            }

            v.Reached(corrupted > 0, $"corrupted {corrupted} stored replicas");
            await using DfsClient reader = await cluster.ConnectAsync(Client("reader", m), Ct);
            DfsFileClient files = reader.GetVolume(Vol).Files;

            // With verification, every read must fail with an explicit integrity (or availability) error; any other failure
            // (false absence, fabricated EOF, ...) means the baseline itself is wrong, so it is an error, not evidence.
            void Explicit(Exception ex, string what)
            {
                if (IsExplicitReadFailure(ex))
                    v.Evidence($"{what} failed explicitly: {ex.GetType().Name}");
                else
                    v.Error($"{what} failed without an explicit integrity/availability error: {ex.GetType().Name}: {ex.Message}");
            }

            try
            {
                byte[] seen = await files.ReadAllBytesAsync(path, Ct);
                v.Check(seen.AsSpan().SequenceEqual(bytes), ViolationKind.CorruptBytesDelivered,
                    $"a whole-file read returned {seen.Length} bytes that differ from the written content without an integrity error");
                v.Error("a whole-file read succeeded although every replica is corrupt");
            }
            catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
            {
                Explicit(ex, "whole-file read");
            }

            try
            {
                await using DfsStream stream = await files.OpenReadAsync(path, Ct);
                stream.Seek(70_000, SeekOrigin.Begin);
                byte[] window = new byte[1_000];
                await stream.ReadExactlyAsync(window, Ct);
                v.Check(window.AsSpan().SequenceEqual(bytes.AsSpan(70_000, 1_000)), ViolationKind.CorruptBytesDelivered,
                    "a range read returned corrupted bytes without an integrity error");
                v.Error("a range read covering a corrupted byte of every replica succeeded");
            }
            catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
            {
                Explicit(ex, "range read");
            }
        });

    // ------------------------------------------------------------------------------------------------------------------
    // MUT-012 CountSameZoneTwice: zone uniqueness in acknowledgement.
    // ------------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Two nodes share zone z1, one is in z2; the ack policy needs two zones. With z2 down a write can only reach z1.
    /// Detects an acknowledged write whose durable copies span fewer zones than required (or a failed write that became visible).
    /// </summary>
    public static Task<ScenarioVerdict> SameZoneAcknowledgementAsync(SafetyMutations m)
    {
        var options = new TestClusterOptions
        {
            Nodes = [new TestNodeSpec("n0", "z1"), new TestNodeSpec("n1", "z1"), new TestNodeSpec("n2", "z2")],
            ControlVoters = 3,
            Volumes = [new TestVolumeSpec(Vol) { Metadata = new DfsMetadataPolicy { Voters = 3, Placement = DfsVoterPlacement.Any } }],
            DefaultPolicy = new DfsStoragePolicy { TargetCopies = 2, TargetDistinctZones = 2, DurableCopiesBeforeAck = 2, DurableZonesBeforeAck = 2 },
        };
        return RunScenarioAsync("same-zone-ack", m, options, async (cluster, v) =>
        {
            const string path = "/mut-z/f";
            await using DfsClient c = await cluster.ConnectAsync(Client("c0", m, operationTimeout: TimeSpan.FromSeconds(5)), Ct);
            DfsFileClient files = c.GetVolume(Vol).Files;
            await c.GetVolume(Vol).Directories.CreateDirectoryAsync("/mut-z", Ct);
            await files.WriteAllBytesAsync("/mut-z/warm", TestData.Bytes(21_201, MultiExtentLength), Ct);
            await CheckDurableCopiesAsync(cluster, v, "/mut-z/warm", 2, 2, ViolationKind.PolicyUnderAcknowledged, "with both zones up");

            TestNode zoneTwo = cluster.Node("n2");
            await zoneTwo.CrashAsync();
            v.Reached(zoneTwo.State == TestNodeState.Crashed, "zone z2 unavailable");
            Exception? failure = await CaptureAsync(() => files.WriteAllBytesAsync(path, TestData.Bytes(21_202, MultiExtentLength), Ct));
            v.Evidence(failure switch
            {
                null => "write acknowledged",
                DfsPlacementUnavailableException p => $"placement unavailable: zones {p.AchievedZones}/{p.RequiredZones}, copies {p.AchievedCopies}/{p.RequiredCopies}",
                _ => $"write failed: {failure.GetType().Name}",
            });
            await CheckCommittedMeetsPolicyAsync(cluster, v, path, failure, 2, 2, "while only z1 was reachable");

            await zoneTwo.RestartAsync();
        });
    }

    // ------------------------------------------------------------------------------------------------------------------
    // MUT-013 ForgetExpiredIdWatermark: delayed replay of a retired operation id.
    // ------------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// An append's id is retired (outcome window exceeded, retention elapsed, metadata compacted and every node restarted
    /// from its checkpoint); the application then retries the id and a captured copy of the original request is replayed.
    /// Detects a retired id that is accepted or executed again.
    /// </summary>
    public static Task<ScenarioVerdict> ExpiredIdReplayAsync(SafetyMutations m) =>
        RunScenarioAsync("expired-id-replay", m, TestClusterOptions.ThreeZones, async (cluster, v) =>
        {
            const string path = "/mut-x/log";
            TestClientOptions options = Client("c0", m, operationTimeout: TimeSpan.FromSeconds(10));
            await using DfsClient c = await cluster.ConnectAsync(options, Ct);
            DfsVolume vol = c.GetVolume(Vol);
            await vol.Directories.CreateDirectoryAsync("/mut-x", Ct);
            byte[] prefix = TestData.Bytes(21_301, 5_000);
            byte[] suffix = TestData.Bytes(21_302, 1_000);
            await vol.Files.WriteAllBytesAsync(path, prefix, Ct);

            DfsOperationId id = await vol.NewOperationIdAsync(Ct);
            MessageHold capture = cluster.Network.Hold(new MessageFilter(From: options.NetworkName, Type: MessageType.Append));
            Task<CommitToken> append = vol.Files.AppendAllBytesAsync(path, suffix, new DfsMutationOptions { OperationId = id }, Ct);
            bool captured = await WaitHeldAsync(capture, 1, TimeSpan.FromSeconds(30));
            v.Reached(captured, "original append request captured for a delayed replay");
            CapturedMessage[] frames = capture.Captured.ToArray();
            capture.ReleaseAll();
            capture.Dispose();
            await append;

            for (int i = 0; i < NodeTimings.Fast.MaxOutcomesPerSession + 40; i++)
                await vol.Files.WriteAllBytesAsync($"/mut-x/fill{i % 4}", TestData.Bytes(21_400 + (ulong)i, 64), Ct);
            await cluster.DelayAsync(NodeTimings.Fast.OutcomeRetention + TimeSpan.FromSeconds(5), Ct);
            foreach (TestNode node in cluster.Nodes)
                await node.CrashAsync();
            foreach (TestNode node in cluster.Nodes)
                await node.RestartAsync();
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), Ct);
            SessionInspection? session = (await cluster.Inspect.GetVolumeStateAsync(Vol)).Sessions.FirstOrDefault(s => s.Id == id.Session);
            v.Evidence($"session {id.Session}: retired watermark {session?.RetiredWatermark}, retained outcomes {session?.RetainedOutcomes}");

            Exception? retry = await CaptureAsync(() => vol.Files.AppendAllBytesAsync(path, suffix, new DfsMutationOptions { OperationId = id }, Ct));
            if (retry is null)
                v.Violation(ViolationKind.ExpiredIdReplayed, $"retired operation id {id} was accepted instead of being rejected as expired");
            else if (retry is DfsOperationExpiredException)
                v.Evidence("retry of the retired id rejected as expired");
            else
                v.Error($"retry of the retired id must be rejected with the explicit expired-outcome error (FSD §13), got {retry.GetType().Name}: {retry.Message}");

            foreach (CapturedMessage frame in frames)
            {
                DfsErrorCode code = await cluster.Network.ReplayAsync(frame);
                v.Check(code != DfsErrorCode.None, ViolationKind.ExpiredIdReplayed, "a delayed duplicate of the retired request was executed");
                if (code is not DfsErrorCode.None and not DfsErrorCode.OperationExpired)
                    v.Error($"a delayed duplicate of the retired request must be rejected as expired, got {code}");
            }

            byte[]? content = await ReadOracleAsync(v, vol.Files, path, ViolationKind.LostAcknowledgedData);
            if (content is not null)
            {
                v.Check(content.AsSpan().SequenceEqual([.. prefix, .. suffix]), ViolationKind.ExpiredIdReplayed,
                    $"the log holds {(content.Length - prefix.Length) / suffix.Length} copies of the suffix");
            }
        });

    // ------------------------------------------------------------------------------------------------------------------
    // MUT-014 RelaxPolicyOnTimeout: no silent weakening of the acknowledgement policy.
    // ------------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// The volume requires three copies in three zones before acknowledgement and one zone is down, so acknowledgements time
    /// out. Detects a write acknowledged with fewer copies/zones than the policy requires, or a weakened commit that lands
    /// after the client gave up (or a failed write that became visible).
    /// </summary>
    /// <remarks>
    /// The client deadline is long (60 s) so a server-side "relax on timeout" fires before the caller abandons the request,
    /// and the committed state is inspected after a further grace period, independent of what the client was told.
    /// </remarks>
    public static Task<ScenarioVerdict> StrongPolicyTimeoutAsync(SafetyMutations m)
    {
        var strong = new DfsStoragePolicy { TargetCopies = 3, TargetDistinctZones = 3, DurableCopiesBeforeAck = 3, DurableZonesBeforeAck = 3 };
        TestClusterOptions options = TestClusterOptions.ThreeZones with { Volumes = [new TestVolumeSpec(Vol) { DefaultPolicy = strong }] };
        return RunScenarioAsync("strong-policy-timeout", m, options, async (cluster, v) =>
        {
            const string path = "/mut-p/f";
            await using DfsClient c = await cluster.ConnectAsync(Client("c0", m, operationTimeout: TimeSpan.FromSeconds(60)), Ct);
            DfsFileClient files = c.GetVolume(Vol).Files;
            await c.GetVolume(Vol).Directories.CreateDirectoryAsync("/mut-p", Ct);
            await files.WriteAllBytesAsync("/mut-p/warm", TestData.Bytes(21_501, MultiExtentLength), Ct);
            await CheckDurableCopiesAsync(cluster, v, "/mut-p/warm", 3, 3, ViolationKind.PolicyUnderAcknowledged, "with all zones up");

            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            TestNode down = cluster.Nodes.First(n => n.Name != leader.Name);
            await down.CrashAsync();
            v.Reached(down.State == TestNodeState.Crashed, $"zone of {down.Name} unavailable");
            Exception? failure = await CaptureAsync(() => files.WriteAllBytesAsync(path, TestData.Bytes(21_502, MultiExtentLength), Ct));
            v.Evidence(failure is null ? "write acknowledged" : $"write failed: {failure.GetType().Name}");
            await cluster.DelayAsync(TimeSpan.FromSeconds(10), Ct);   // a weakened commit may land after the caller gave up
            await CheckCommittedMeetsPolicyAsync(cluster, v, path, failure, 3, 3, "with one of three zones down");

            await down.RestartAsync();
        });
    }
}
