using System.Reflection;
using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Transport;
using Xunit.Sdk;
using static Tedd.Tdfs.Tests.Unit.Har.HarHelpers;

namespace Tedd.Tdfs.Tests.Unit.Har;

/// <summary>
/// HAR — test harness and independent oracles (test plan §7.1; FSD §§16, 18; invariants INV-01..INV-20). These tests
/// verify the evidence machinery itself: deterministic replay, controlled nondeterminism, independent clocks, crash-faithful
/// storage, network fault fidelity, ambiguity-preserving histories, independent oracles, sound and complete checkers,
/// fault-point accounting, trace minimization, honest liveness preconditions, isolation and assumption labelling.
/// </summary>
public sealed class HarUnitTests
{
    // =====================================================================================================================
    // HAR-001 SeedReplayIsExact
    // =====================================================================================================================

    [Theory]
    [InlineData(0x4841_5230_3031_0001UL, 3)]
    [InlineData(0x4841_5230_3031_0002UL, 3)]
    [InlineData(0x4841_5230_3031_0003UL, 5)]
    [Catalog("HAR-001", "U", "INV-20", "INV-01", "INV-06")]
    public async Task HAR_001_SeedReplayIsExact(ulong seed, int nodeCount)
    {
        TestClusterOptions options = Deterministic(nodeCount, seed);

        ReplayFingerprint first = await ClusterTest.RunAsync<ReplayFingerprint>(options, RunReplayScenarioAsync, Ct);
        ReplayFingerprint second = await ClusterTest.RunAsync<ReplayFingerprint>(options, RunReplayScenarioAsync, Ct);

        // The schedule really exercised faults, retries and operations; an empty replay would be trivially identical.
        Assert.Equal(seed, first.Seed);
        Assert.Equal(nodeCount, first.Nodes.Count);
        Assert.Contains(first.ArmEvents, e => e.StartsWith("crash@", StringComparison.Ordinal));
        Assert.Contains(first.ArmEvents, e => e.StartsWith("delay@", StringComparison.Ordinal));
        Assert.Contains("drop-reply matched=1 remaining=0", first.ArmEvents);
        Assert.NotEmpty(first.CoverageHits);
        Assert.NotEmpty(first.Trace);
        Assert.NotEmpty(first.History);
        Assert.True(first.MessagesDelivered > 0);

        // Same build, seed, starting images and schedule: outcomes, fault events, traces and digests are identical.
        AssertSameReplay(first, second);

        // A different seed must drive a different run; otherwise the equality above proves nothing about the seed.
        ReplayFingerprint other = await ClusterTest.RunAsync<ReplayFingerprint>(options with { Seed = seed ^ 0x00FF_00FF_00FF_00FFUL }, RunReplayScenarioAsync, Ct);
        Assert.True(other.ClusterId != first.ClusterId || !other.Nodes.SequenceEqual(first.Nodes, StringComparer.Ordinal)
            || !other.Trace.SequenceEqual(first.Trace, StringComparer.Ordinal),
            "cluster identity, node identities and trace are identical for a different seed: the seed does not control the run");
    }

    [Theory]
    [InlineData(CrashMode.RandomSubset)]
    [InlineData(CrashMode.TearLastWrite)]
    [Catalog("HAR-001", "U", "INV-20", "INV-01")]
    public async Task HAR_001_SeedReplayIsExact_SimulatedStorageCrashImages(CrashMode mode)
    {
        var distinctImages = new HashSet<string>(StringComparer.Ordinal);
        for (ulong crashSeed = 1; crashSeed <= 12; crashSeed++)
        {
            CrashScriptResult first = await RunCrashScriptAsync(mode, crashSeed);
            CrashScriptResult replay = await RunCrashScriptAsync(mode, crashSeed);
            AssertEx.BytesEqual(first.DurableImage, replay.DurableImage, $"crash seed {crashSeed}: replayed durable image");
            AssertEx.BytesEqual(first.RebootedView, replay.RebootedView, $"crash seed {crashSeed}: replayed rebooted view");
            Assert.Equal(first.UnflushedEntryExists, replay.UnflushedEntryExists);
            Assert.Equal(first.AllFilesAfterCrash, replay.AllFilesAfterCrash);
            distinctImages.Add(Hex(first.DurableImage));
        }

        // The crash outcome really depends on the seed (same data, different crash seeds).
        Assert.True(distinctImages.Count >= 2, $"{mode}: all crash seeds produced the same image; the seeded choice is not exercised");

        // The seeded sources the harness hands to components replay exactly and fork independently.
        var a = new SeededRandom(99);
        var b = new SeededRandom(99);
        Assert.Equal(Enumerable.Range(0, 32).Select(_ => a.NextUInt64()).ToArray(), Enumerable.Range(0, 32).Select(_ => b.NextUInt64()).ToArray());
        Assert.Equal(new SeededRandom(5).Fork("node:n1").NextUInt64(), new SeededRandom(5).Fork("node:n1").NextUInt64());
        Assert.NotEqual(new SeededRandom(5).Fork("node:n1").NextUInt64(), new SeededRandom(5).Fork("node:n2").NextUInt64());
        AssertEx.BytesEqual(TestData.Bytes(7, 1000), TestData.Bytes(7, 1000), "test data is deterministic");
    }

    // =====================================================================================================================
    // HAR-002 NondeterminismIsIntercepted
    // =====================================================================================================================

    [Theory]
    [InlineData(HarEscape.TaskRun)]
    [InlineData(HarEscape.ThreadPoolWorkItem)]
    [InlineData(HarEscape.SystemTimer)]
    [InlineData(HarEscape.TaskDelayWithoutTimeProvider)]
    [InlineData(HarEscape.SocketReceive)]
    [Catalog("HAR-002", "U", "INV-20")]
    public async Task HAR_002_NondeterminismIsIntercepted(HarEscape escape)
    {
        var seenDuringRun = new List<UncontrolledDependency>();
        byte[] payload = TestData.Bytes(2002, 1500);
        byte[]? readBack = null;

        UncontrolledDependencyException reported = await Assert.ThrowsAsync<UncontrolledDependencyException>(() =>
            ClusterTest.RunAsync(Deterministic(), async cluster =>
            {
                Assert.Empty(cluster.UncontrolledDependencies); // reaching a stable cluster uses controlled facilities only
                await EscapeAsync(escape);
                seenDuringRun.AddRange(cluster.UncontrolledDependencies);

                // Detection must not wedge the simulation: the cluster keeps serving.
                await using DfsClient client = await cluster.ConnectAsync(null, Ct);
                DfsVolume vol = client.GetVolume(Vol);
                await vol.Files.WriteAllBytesAsync("/har002", payload, Ct);
                readBack = await vol.Files.ReadAllBytesAsync("/har002", Ct);
            }, Ct).WaitAsync(TimeSpan.FromMinutes(2), Ct));

        Assert.NotEmpty(seenDuringRun);
        Assert.All(seenDuringRun, d =>
        {
            Assert.Equal(UncontrolledDependencyKind.ForeignThreadPost, d.Kind);
            Assert.False(string.IsNullOrWhiteSpace(d.Component));
            Assert.False(string.IsNullOrWhiteSpace(d.Detail));
        });
        Assert.All(seenDuringRun, d => Assert.Contains(d, reported.Dependencies));
        Assert.Null(reported.InnerException); // the scenario itself succeeded; only the evidence is invalid
        Assert.NotNull(readBack);
        AssertEx.BytesEqual(payload, readBack, "the cluster kept working after the escape was detected");
    }

    [Fact]
    [Catalog("HAR-002", "U", "INV-20")]
    public Task HAR_002_NondeterminismIsIntercepted_ControlledFacilitiesAreNotReported() =>
        ClusterTest.RunAsync(Deterministic(), async cluster =>
        {
            long start = cluster.Time.GetTimestamp();
            await cluster.DelayAsync(TimeSpan.FromSeconds(1), Ct);
            await Task.Delay(TimeSpan.FromMilliseconds(250), cluster.Time, Ct);
            var fired = new TaskCompletionSource<bool>();
            using (ITimer timer = cluster.Time.CreateTimer(_ => fired.TrySetResult(true), null, TimeSpan.FromMilliseconds(100), Timeout.InfiniteTimeSpan))
                Assert.True(await fired.Task);
            await using DfsClient client = await cluster.ConnectAsync(null, Ct);
            await client.GetVolume(Vol).Files.WriteAllBytesAsync("/har002-ok", TestData.Bytes(1, 100), Ct);
            Assert.True(cluster.Time.GetElapsedTime(start) >= TimeSpan.FromMilliseconds(1350));
            Assert.Empty(cluster.UncontrolledDependencies);
        }, Ct);

    [Theory]
    [InlineData(nameof(RogueSimulatedComponent.ReadsWallClock), UncontrolledDependencyKind.WallClock)]
    [InlineData(nameof(RogueSimulatedComponent.ReadsWallClockOffset), UncontrolledDependencyKind.WallClock)]
    [InlineData(nameof(RogueSimulatedComponent.ReadsTickCount), UncontrolledDependencyKind.WallClock)]
    [InlineData(nameof(RogueSimulatedComponent.ReadsStopwatch), UncontrolledDependencyKind.WallClock)]
    [InlineData(nameof(RogueSimulatedComponent.UsesSharedRandom), UncontrolledDependencyKind.Random)]
    [InlineData(nameof(RogueSimulatedComponent.UsesNewRandom), UncontrolledDependencyKind.Random)]
    [InlineData(nameof(RogueSimulatedComponent.UsesNewGuid), UncontrolledDependencyKind.Random)]
    [InlineData(nameof(RogueSimulatedComponent.UsesCryptoRandom), UncontrolledDependencyKind.Random)]
    [InlineData(nameof(RogueSimulatedComponent.StartsTimer), UncontrolledDependencyKind.Timer)]
    [InlineData(nameof(RogueSimulatedComponent.DelaysWithoutTimeProvider), UncontrolledDependencyKind.Timer)]
    [InlineData(nameof(RogueSimulatedComponent.TimesOutWithoutTimeProvider), UncontrolledDependencyKind.Timer)]
    [InlineData(nameof(RogueSimulatedComponent.RunsOnThreadPool), UncontrolledDependencyKind.ThreadPool)]
    [InlineData(nameof(RogueSimulatedComponent.QueuesWorkItem), UncontrolledDependencyKind.ThreadPool)]
    [InlineData(nameof(RogueSimulatedComponent.StartsThread), UncontrolledDependencyKind.Thread)]
    [InlineData(nameof(RogueSimulatedComponent.SleepsThread), UncontrolledDependencyKind.Thread)]
    [InlineData(nameof(RogueSimulatedComponent.OpensSocket), UncontrolledDependencyKind.Socket)]
    [InlineData(nameof(RogueSimulatedComponent.OpensTcpClient), UncontrolledDependencyKind.Socket)]
    [InlineData(nameof(RogueSimulatedComponent.ReadsDisk), UncontrolledDependencyKind.Disk)]
    [InlineData(nameof(RogueSimulatedComponent.OpensFileStream), UncontrolledDependencyKind.Disk)]
    [InlineData(nameof(RogueSimulatedComponent.EscapesContextAsync), UncontrolledDependencyKind.ContextEscape)]
    [InlineData(nameof(RogueSimulatedComponent.YieldsAsync), UncontrolledDependencyKind.ContextEscape)]
    [InlineData(nameof(RogueSimulatedComponent.CreatesSemaphore), UncontrolledDependencyKind.ContextEscape)]
    [InlineData(nameof(RogueSimulatedComponent.RunsContinuationsAsynchronously), UncontrolledDependencyKind.ContextEscape)]
    [Catalog("HAR-002", "U", "INV-20")]
    public void HAR_002_NondeterminismIsIntercepted_StaticAuditReportsDirectDependency(string method, UncontrolledDependencyKind kind)
    {
        AuditReport report = IlAudit.ScanTypes([typeof(RogueSimulatedComponent)], AuditRules.All);
        Assert.Empty(report.Unresolved);
        Assert.Contains(report.Findings, f => f.Kind == kind
            && (f.Location.EndsWith("::" + method, StringComparison.Ordinal) || f.Location.Contains("<" + method + ">", StringComparison.Ordinal)));
    }

    [Fact]
    [Catalog("HAR-002", "U", "INV-20")]
    public void HAR_002_NondeterminismIsIntercepted_StaticAuditIsDiscriminating()
    {
        AuditReport clean = IlAudit.ScanTypes([typeof(CleanSimulatedComponent)], AuditRules.All);
        Assert.Empty(clean.Unresolved);
        Assert.True(clean.MethodsScanned >= 8, $"only {clean.MethodsScanned} methods scanned");
        Assert.True(clean.Findings.Count == 0, "injected time/random/storage use was reported: " + string.Join("; ", clean.Findings));

        AuditReport rogue = IlAudit.ScanTypes([typeof(RogueSimulatedComponent)], AuditRules.All);
        HashSet<UncontrolledDependencyKind> kinds = rogue.Findings.Select(f => f.Kind).ToHashSet();
        foreach (UncontrolledDependencyKind kind in Enum.GetValues<UncontrolledDependencyKind>().Where(k => k != UncontrolledDependencyKind.ForeignThreadPost))
            Assert.Contains(kind, kinds);
        foreach (MethodInfo method in typeof(RogueSimulatedComponent).GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly))
        {
            Assert.Contains(rogue.Findings, f => f.Location.EndsWith("::" + method.Name, StringComparison.Ordinal)
                || f.Location.Contains("<" + method.Name + ">", StringComparison.Ordinal));
        }
    }

    [Theory]
    [InlineData("Consensus")]
    [InlineData("Storage")]
    [InlineData("Server")]
    [InlineData("Client")]
    [Catalog("HAR-002", "U", "INV-20")]
    public void HAR_002_NondeterminismIsIntercepted_ProductionComponentsUseInjectedDependenciesOnly(string component)
    {
        (Assembly Assembly, AuditRules Rules, Func<Type, bool>? Exclude) target = component switch
        {
            // Pure Raft core: no clock, no I/O, no threads (ARCHITECTURE §4).
            "Consensus" => (typeof(RaftNode).Assembly, AuditRules.Server, null),
            // The real-disk medium is the one sanctioned file-system boundary of the storage engine.
            "Storage" => (typeof(SimulatedMedium).Assembly, AuditRules.Server,
                t => t.Name.StartsWith("FileSystem", StringComparison.Ordinal) || t.Name == "DirectorySync"),
            // The production node loop is the one sanctioned thread owner (ARCHITECTURE §3.3).
            "Server" => (typeof(NodeHost).Assembly, AuditRules.Server, t => t.Name.StartsWith("ThreadNodeLoop", StringComparison.Ordinal)),
            // SDK request path: simulation-compatible (ARCHITECTURE §9.6); ConfigureAwait(false) is allowed there.
            "Client" => (typeof(DfsClient).Assembly, AuditRules.Sdk, null),
            _ => throw new ArgumentOutOfRangeException(nameof(component), component, null),
        };

        AuditReport report = IlAudit.ScanAssembly(target.Assembly, target.Rules, target.Exclude);
        Assert.True(report.MethodsScanned > 0, "nothing was scanned");
        Assert.True(report.Unresolved.Count == 0, "unresolved IL references (missing evidence is not a pass): " + string.Join("; ", report.Unresolved.Take(10)));
        Assert.True(report.Findings.Count == 0, $"{component} uses uncontrolled dependencies: " + string.Join("; ", report.Findings.Take(25)));
    }

    // =====================================================================================================================
    // HAR-003 IndependentNodeClocks
    // =====================================================================================================================

    public static TheoryData<ClockStep, int, int> ClockStepCases()
    {
        var data = new TheoryData<ClockStep, int, int>();
        foreach (ClockStep step in Enum.GetValues<ClockStep>())
        {
            data.Add(step, 3, 1);
            data.Add(step, 5, 4);
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(ClockStepCases))]
    [Catalog("HAR-003", "U", "INV-05", "INV-20")]
    public void HAR_003_IndependentNodeClocks(ClockStep step, int clockCount, int target)
    {
        var manualBase = new TestClock();
        manualBase.Freeze(); // a manual base: it only moves when advanced explicitly
        TestClock[] clocks = Enumerable.Range(0, clockCount).Select(_ => new TestClock(manualBase)).ToArray();
        long[] mono0 = clocks.Select(c => c.GetTimestamp()).ToArray();
        DateTimeOffset[] wall0 = clocks.Select(c => c.GetUtcNow()).ToArray();
        Assert.All(mono0, m => Assert.Equal(manualBase.GetTimestamp(), m));
        Assert.All(wall0, w => Assert.Equal(manualBase.GetUtcNow(), w));
        bool[] fired = new bool[clockCount];
        ITimer[] timers = clocks.Select((c, i) => c.CreateTimer(_ => fired[i] = true, null, TimeSpan.FromSeconds(12), Timeout.InfiniteTimeSpan)).ToArray();

        TimeSpan targetMono;
        TimeSpan targetWall;
        switch (step)
        {
            case ClockStep.Advance:
                clocks[target].Advance(TimeSpan.FromSeconds(5));
                (targetMono, targetWall) = (TimeSpan.FromSeconds(15), TimeSpan.FromSeconds(15));
                break;
            case ClockStep.JumpWallForward:
                clocks[target].JumpWall(TimeSpan.FromHours(1));
                (targetMono, targetWall) = (TimeSpan.FromSeconds(10), TimeSpan.FromHours(1) + TimeSpan.FromSeconds(10));
                break;
            case ClockStep.JumpWallBackward:
                clocks[target].JumpWall(TimeSpan.FromHours(-1));
                (targetMono, targetWall) = (TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(10) - TimeSpan.FromHours(1));
                break;
            case ClockStep.Freeze:
                clocks[target].Freeze();
                (targetMono, targetWall) = (TimeSpan.Zero, TimeSpan.Zero);
                break;
            case ClockStep.SetRate:
                clocks[target].SetRate(2.0);
                (targetMono, targetWall) = (TimeSpan.FromSeconds(20), TimeSpan.FromSeconds(20));
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(step), step, null);
        }

        // Manipulating one clock moves no other clock.
        for (int i = 0; i < clockCount; i++)
        {
            if (i == target)
                continue;
            Assert.Equal(mono0[i], clocks[i].GetTimestamp());
            Assert.Equal(wall0[i], clocks[i].GetUtcNow());
            Assert.False(fired[i]);
        }

        manualBase.Advance(TimeSpan.FromSeconds(10));
        for (int i = 0; i < clockCount; i++)
        {
            TimeSpan mono = i == target ? targetMono : TimeSpan.FromSeconds(10);
            TimeSpan wall = i == target ? targetWall : TimeSpan.FromSeconds(10);
            Assert.Equal(mono.Ticks, clocks[i].GetTimestamp() - mono0[i]);
            Assert.Equal(wall, clocks[i].GetUtcNow() - wall0[i]);
            Assert.Equal(mono >= TimeSpan.FromSeconds(12), fired[i]); // timers follow each clock's own monotonic time
        }

        Assert.True(clocks[target].GetTimestamp() >= mono0[target], "monotonic time never runs backwards, even after a wall step");
        Assert.Equal(step == ClockStep.SetRate ? 2.0 : 1.0, clocks[target].Rate);
        Assert.Equal(step == ClockStep.Freeze, clocks[target].IsFrozen);
        Assert.All(clocks.Where((_, i) => i != target), c => Assert.False(c.IsFrozen));

        if (step == ClockStep.Freeze)
        {
            clocks[target].Unfreeze(); // resumes from the frozen reading without a jump
            manualBase.Advance(TimeSpan.FromSeconds(3));
            Assert.Equal(TimeSpan.FromSeconds(3).Ticks, clocks[target].GetTimestamp() - mono0[target]);
            Assert.False(fired[target]);
            manualBase.Advance(TimeSpan.FromSeconds(10));
            Assert.True(fired[target]);
        }

        foreach (ITimer timer in timers)
            timer.Dispose();
    }

    [Fact]
    [Catalog("HAR-003", "U", "INV-05", "INV-20")]
    public Task HAR_003_IndependentNodeClocks_ClusterNodesAndClient() =>
        ClusterTest.RunAsync(Deterministic(), async cluster =>
        {
            CancellationToken ct = Ct;
            TestClock[] nodeClocks = cluster.Nodes.Select(n => n.Clock).ToArray();
            Assert.Equal(nodeClocks.Length, nodeClocks.Distinct(ReferenceEqualityComparer.Instance).Count());
            Assert.DoesNotContain(nodeClocks, c => ReferenceEquals(c, cluster.Time));
            var clientClock = new TestClock(cluster.Time);
            Assert.DoesNotContain(nodeClocks, c => ReferenceEquals(c, clientClock));
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Clock = clientClock }, ct);
            DfsVolume vol = client.GetVolume(Vol);

            TestNode volumeLeader = await cluster.GetLeaderAsync(Vol, ct);
            TestNode controlLeader = await cluster.GetControlLeaderAsync(ct);
            TestNode stepped = cluster.Nodes.First(n => n.Name != volumeLeader.Name && n.Name != controlLeader.Name);

            long[] Mono() => cluster.Nodes.Select(n => n.Clock.GetTimestamp()).ToArray();
            DateTimeOffset[] Wall() => cluster.Nodes.Select(n => n.Clock.GetUtcNow()).ToArray();

            long[] mono0 = Mono();
            DateTimeOffset[] wall0 = Wall();
            long harness0 = cluster.Time.GetTimestamp();
            long client0 = clientClock.GetTimestamp();

            // Step one follower's clock and the leader's wall clock; nothing else moves (no virtual time passes here).
            stepped.Clock.Advance(TimeSpan.FromMinutes(10));
            volumeLeader.Clock.JumpWall(TimeSpan.FromHours(-1));
            long[] mono1 = Mono();
            DateTimeOffset[] wall1 = Wall();
            for (int i = 0; i < cluster.Nodes.Count; i++)
            {
                TestNode node = cluster.Nodes[i];
                bool isStepped = node.Name == stepped.Name;
                bool isLeader = node.Name == volumeLeader.Name;
                Assert.Equal(isStepped ? TimeSpan.FromMinutes(10) : TimeSpan.Zero, node.Clock.GetElapsedTime(mono0[i], mono1[i]));
                TimeSpan expectedWall = (isStepped ? TimeSpan.FromMinutes(10) : TimeSpan.Zero) + (isLeader ? TimeSpan.FromHours(-1) : TimeSpan.Zero);
                Assert.Equal(expectedWall, wall1[i] - wall0[i]);
            }

            Assert.Equal(harness0, cluster.Time.GetTimestamp());
            Assert.Equal(client0, clientClock.GetTimestamp());

            // Writer authority is decided on the leader's monotonic clock: a follower jump and a leader wall step do not
            // expire or extend the lease.
            byte[] payload = TestData.Bytes(3003, 10_000);
            await using (DfsStream writer = await vol.Files.OpenAsync("/har003", FileMode.Create, FileAccess.Write, FileShare.Read, ct))
            {
                await writer.WriteAsync(payload, ct);
                await cluster.DelayAsync(TimeSpan.FromSeconds(1), ct);
                await writer.FlushAsync(ct);
                Assert.False(writer.IsFaulted);
            }

            await BytesEqualAsync(payload, vol.Files.ReadAllBytesAsync("/har003", ct));

            // Passage of shared virtual time moves every running clock by its own rate.
            stepped.Clock.Freeze();
            long[] beforeFrozen = Mono();
            long harnessBefore = cluster.Time.GetTimestamp();
            await cluster.DelayAsync(TimeSpan.FromSeconds(1), ct);
            TimeSpan harnessDelta = cluster.Time.GetElapsedTime(harnessBefore);
            long[] afterFrozen = Mono();
            for (int i = 0; i < cluster.Nodes.Count; i++)
            {
                TimeSpan delta = cluster.Nodes[i].Clock.GetElapsedTime(beforeFrozen[i], afterFrozen[i]);
                Assert.Equal(cluster.Nodes[i].Name == stepped.Name ? TimeSpan.Zero : harnessDelta, delta);
            }

            stepped.Clock.Unfreeze();
            stepped.Clock.SetRate(2.0);
            long steppedBefore = stepped.Clock.GetTimestamp();
            harnessBefore = cluster.Time.GetTimestamp();
            await cluster.DelayAsync(TimeSpan.FromSeconds(1), ct);
            harnessDelta = cluster.Time.GetElapsedTime(harnessBefore);
            Assert.Equal(harnessDelta.Ticks * 2, stepped.Clock.GetElapsedTime(steppedBefore, stepped.Clock.GetTimestamp()).Ticks);
            stepped.Clock.SetRate(1.0);

            await vol.Files.WriteAllBytesAsync("/har003-after", payload, ct);
            await cluster.Inspect.AssertNoSplitBrainAsync();
        }, Ct);

    // =====================================================================================================================
    // HAR-004 CrashLosesVolatileState
    // =====================================================================================================================

    [Theory]
    [InlineData(CrashMode.LoseUnflushed, 1UL)]
    [InlineData(CrashMode.KeepUnflushed, 1UL)]
    [InlineData(CrashMode.RandomSubset, 1UL)]
    [InlineData(CrashMode.RandomSubset, 2UL)]
    [InlineData(CrashMode.RandomSubset, 3UL)]
    [InlineData(CrashMode.RandomSubset, 4UL)]
    [InlineData(CrashMode.TearLastWrite, 1UL)]
    [InlineData(CrashMode.TearLastWrite, 2UL)]
    [InlineData(CrashMode.TearLastWrite, 3UL)]
    [Catalog("HAR-004", "U", "INV-01", "INV-20")]
    public async Task HAR_004_CrashLosesVolatileState(CrashMode mode, ulong crashSeed)
    {
        CrashScriptResult r = await RunCrashScriptAsync(mode, crashSeed);

        Assert.True(r.OldHandleRejected, "handles opened before the crash must not survive it");
        AssertEx.BytesEqual(r.DurableImage, r.RebootedView, "after reboot the visible file is exactly its durable image (no heap or page cache)");
        Assert.Contains(Hex(r.RebootedView), AllowedCrashImages(r.Baseline, r.Pending, mode));

        switch (mode)
        {
            case CrashMode.LoseUnflushed:
                AssertEx.BytesEqual(r.Baseline, r.RebootedView, "only the last durability barrier's image survives");
                Assert.False(r.UnflushedEntryExists, "a directory entry without a directory flush is lost even though its data was flushed");
                Assert.DoesNotContain("d/g", r.AllFilesAfterCrash);
                break;
            case CrashMode.KeepUnflushed:
                AssertEx.BytesEqual(r.PreCrashView, r.RebootedView);
                Assert.True(r.UnflushedEntryExists);
                AssertEx.BytesEqual(r.UnflushedEntryData, r.UnflushedEntryContent);
                break;
            case CrashMode.TearLastWrite:
                Assert.False(r.RebootedView.AsSpan().SequenceEqual(r.PreCrashView), "the most recent unflushed write must be torn, not kept whole");
                Assert.True(r.UnflushedEntryExists);
                AssertEx.BytesEqual(r.UnflushedEntryData, r.UnflushedEntryContent);
                break;
            case CrashMode.RandomSubset:
                if (r.UnflushedEntryExists)
                    AssertEx.BytesEqual(r.UnflushedEntryData, r.UnflushedEntryContent, "a surviving entry carries its flushed data");
                break;
        }
    }

    [Theory]
    [InlineData(CrashMode.LoseUnflushed)]
    [InlineData(CrashMode.KeepUnflushed)]
    [InlineData(CrashMode.RandomSubset)]
    [InlineData(CrashMode.TearLastWrite)]
    [Catalog("HAR-004", "U", "INV-01", "INV-14", "INV-20")]
    public Task HAR_004_CrashLosesVolatileState_NodeRebootsFromDurableImage(CrashMode mode) =>
        ClusterTest.RunAsync(Deterministic(), async cluster =>
        {
            CancellationToken ct = Ct;
            const string path = "/har004";
            await using DfsClient client = await cluster.ConnectAsync(null, ct);
            DfsVolume vol = client.GetVolume(Vol);
            TestNode leader = await cluster.GetLeaderAsync(Vol, ct);
            TestNode victim = cluster.Nodes.First(n => n.Name != leader.Name);
            ulong incarnation = victim.Incarnation;

            // Crash the victim after its object write but before the flush barrier.
            FaultArm arm = cluster.Faults.Arm(new FaultMatch(FaultSites.ObjectBeforeFlush, Node: victim.Name, Volume: Vol), FaultAction.CrashNode(mode));
            byte[] data = TestData.Bytes(400, (192 * 1024) + 5);
            CommitToken token = await vol.Files.WriteAllBytesAsync(path, data, ct); // acknowledged by the two healthy nodes
            Assert.False(token.IsNone);
            await arm.WaitTriggeredAsync(TimeSpan.FromSeconds(30));
            cluster.Faults.AssertReached(arm);
            FaultContext crashPoint = Assert.Single(arm.Contexts);
            Assert.NotNull(crashPoint.Object);
            ObjectId obj = crashPoint.Object ?? throw new InvalidOperationException("crash point without object id");
            Assert.Equal(TestNodeState.Crashed, victim.State);

            // Reboot in isolation so that repair cannot add anything: the node sees only its own durable image.
            cluster.Network.Isolate(victim.Name);
            await victim.RestartAsync();
            Assert.Equal(incarnation + 1, victim.Incarnation);
            bool holds = await victim.Inspect.HasObjectAsync(obj);
            if (mode == CrashMode.LoseUnflushed)
                Assert.False(holds, "an object whose flush never completed must not survive a crash that loses unflushed writes");
            if (holds)
                Assert.Contains(victim.Name, await cluster.Inspect.GetObjectHoldersAsync(obj)); // complete and verified, never partial
            foreach (ObjectRecordInfo record in await victim.Inspect.ListObjectsAsync(Vol))
                Assert.Contains(victim.Name, await cluster.Inspect.GetObjectHoldersAsync(record.Id));

            // Liveness only after the prerequisite (connectivity) is restored.
            cluster.Network.Heal();
            await cluster.WaitForRepairAsync(Vol, TimeSpan.FromSeconds(60), ct);
            await BytesEqualAsync(data, vol.Files.ReadAllBytesAsync(path, ct));
            Assert.Contains(victim.Name, await cluster.Inspect.GetObjectHoldersAsync(obj)); // restored by verified repair
        }, Ct);

    // =====================================================================================================================
    // HAR-005 FlushFailureNeverPersistsByAssumption
    // =====================================================================================================================

    [Theory]
    [InlineData(FlushFault.FailFileFlush)]
    [InlineData(FlushFault.FailFileFlushThenRetry)]
    [InlineData(FlushFault.FailDirectoryFlush)]
    [InlineData(FlushFault.ReadOnlyDevice)]
    [InlineData(FlushFault.DelayedFlushInterruptedByCrash)]
    [InlineData(FlushFault.ShortWrite)]
    [InlineData(FlushFault.PathFilteredFailure)]
    [Catalog("HAR-005", "U", "INV-01", "INV-08", "INV-20")]
    public async Task HAR_005_FlushFailureNeverPersistsByAssumption(FlushFault fault)
    {
        CancellationToken ct = Ct;
        var medium = new SimulatedMedium("har005", 1 << 20, new SeededRandom(5));
        byte[] a = TestData.Bytes(51, 4096);
        byte[] x = TestData.Bytes(52, 4096);
        await medium.CreateDirectoryAsync("d", ct);
        await medium.FlushDirectoryAsync("", ct);
        IStorageFile f = await medium.OpenAsync("d/f", StorageOpenMode.CreateNew, ct);
        await f.WriteFullyAsync(0, a, ct);
        await f.FlushAsync(ct);
        await medium.FlushDirectoryAsync("d", ct);
        long flushes = medium.FlushCount;

        async Task<byte[]> PowerCutAndReadAsync(string path)
        {
            medium.Crash(CrashMode.LoseUnflushed);
            await using IStorageFile rebooted = await medium.OpenAsync(path, StorageOpenMode.OpenExisting, ct);
            return await ReadAllAsync(rebooted);
        }

        switch (fault)
        {
            case FlushFault.FailFileFlush:
                await f.WriteFullyAsync(0, x, ct);
                medium.Faults.FailNextFlushes = 1;
                await Assert.ThrowsAsync<IOException>(() => f.FlushAsync(ct).AsTask());
                Assert.Equal(flushes, medium.FlushCount);
                AssertEx.BytesEqual(a, medium.ReadDurable("d/f"), "a failed flush makes nothing durable");
                await BytesEqualAsync(a, PowerCutAndReadAsync("d/f"), "after power loss only the last successful barrier's image remains");
                break;

            case FlushFault.FailFileFlushThenRetry:
                await f.WriteFullyAsync(0, x, ct);
                medium.Faults.FailNextFlushes = 1;
                await Assert.ThrowsAsync<IOException>(() => f.FlushAsync(ct).AsTask());
                AssertEx.BytesEqual(a, medium.ReadDurable("d/f"));
                await f.FlushAsync(ct); // only the successful retry is a barrier
                Assert.Equal(flushes + 1, medium.FlushCount);
                AssertEx.BytesEqual(x, medium.ReadDurable("d/f"));
                await BytesEqualAsync(x, PowerCutAndReadAsync("d/f"));
                break;

            case FlushFault.FailDirectoryFlush:
            {
                IStorageFile n = await medium.OpenAsync("d/n", StorageOpenMode.CreateNew, ct);
                await n.WriteFullyAsync(0, TestData.Bytes(53, 500), ct);
                await n.FlushAsync(ct); // the data barrier succeeded
                medium.Faults.FailNextFlushes = 1;
                await Assert.ThrowsAsync<IOException>(() => medium.FlushDirectoryAsync("d", ct).AsTask());
                Assert.Throws<FileNotFoundException>(() => medium.ReadDurable("d/n")); // the entry itself is not durable
                Assert.Contains("d/n", medium.AllFiles()); // although it is visible before the crash
                medium.Crash(CrashMode.LoseUnflushed);
                Assert.False(await medium.ExistsAsync("d/n", ct));
                Assert.DoesNotContain("d/n", medium.AllFiles());
                break;
            }

            case FlushFault.ReadOnlyDevice:
                await f.WriteFullyAsync(0, x, ct);
                medium.Faults.ReadOnly = true;
                await Assert.ThrowsAsync<IOException>(() => f.FlushAsync(ct).AsTask());
                AssertEx.BytesEqual(a, medium.ReadDurable("d/f"));
                medium.Faults.ReadOnly = false;
                await BytesEqualAsync(a, PowerCutAndReadAsync("d/f"));
                break;

            case FlushFault.DelayedFlushInterruptedByCrash:
            {
                await f.WriteFullyAsync(0, x, ct);
                medium.Faults.IoDelay = TimeSpan.FromMilliseconds(300);
                Task pending = f.FlushAsync(ct).AsTask();
                Assert.False(pending.IsCompleted);
                AssertEx.BytesEqual(a, medium.ReadDurable("d/f"), "an in-flight flush is not a completed barrier");
                medium.Crash(CrashMode.LoseUnflushed);
                await Assert.ThrowsAsync<ObjectDisposedException>(() => pending); // the interrupted barrier never reports success
                medium.Faults.IoDelay = null;
                AssertEx.BytesEqual(a, medium.ReadDurable("d/f"));
                Assert.Equal(flushes, medium.FlushCount);
                break;
            }

            case FlushFault.ShortWrite:
            {
                medium.Faults.ShortNextWrites = 1;
                int written = await f.WriteAsync(0, x, ct);
                Assert.Equal((x.Length + 1) / 2, written);
                await f.FlushAsync(ct);
                byte[] expected = a.ToArray();
                x.AsSpan(0, written).CopyTo(expected);
                AssertEx.BytesEqual(expected, medium.ReadDurable("d/f"), "only the reported bytes were written and made durable");
                medium.Faults.ShortNextWrites = 3;
                await f.WriteFullyAsync(0, x, ct); // callers must loop over short writes
                await f.FlushAsync(ct);
                AssertEx.BytesEqual(x, medium.ReadDurable("d/f"));
                break;
            }

            case FlushFault.PathFilteredFailure:
            {
                medium.Faults.PathFilter = p => p.StartsWith("objects/", StringComparison.Ordinal);
                medium.Faults.FailNextFlushes = 1;
                await medium.CreateDirectoryAsync("objects", ct);
                await medium.FlushDirectoryAsync("", ct);
                IStorageFile o = await medium.OpenAsync("objects/o1", StorageOpenMode.CreateNew, ct);
                await medium.FlushDirectoryAsync("objects", ct);
                await f.WriteFullyAsync(0, x, ct);
                await f.FlushAsync(ct);
                Assert.Equal(1, medium.Faults.FailNextFlushes); // non-matching barriers neither fail nor consume the fault
                AssertEx.BytesEqual(x, medium.ReadDurable("d/f"));
                await o.WriteFullyAsync(0, TestData.Bytes(54, 700), ct);
                await Assert.ThrowsAsync<IOException>(() => o.FlushAsync(ct).AsTask());
                Assert.Equal(0, medium.Faults.FailNextFlushes);
                Assert.Empty(medium.ReadDurable("objects/o1"));
                Assert.Empty(await PowerCutAndReadAsync("objects/o1"));
                AssertEx.BytesEqual(x, medium.ReadDurable("d/f"));
                break;
            }

            default:
                throw new ArgumentOutOfRangeException(nameof(fault), fault, null);
        }
    }

    [Theory]
    [InlineData(ClusterFlushFault.IoError)]
    [InlineData(ClusterFlushFault.DelayBeyondDeadline)]
    [Catalog("HAR-005", "U", "INV-01", "INV-08", "INV-11", "INV-20")]
    public Task HAR_005_FlushFailureNeverPersistsByAssumption_NoReceiptWithoutBarrier(ClusterFlushFault fault) =>
        ClusterTest.RunAsync(Deterministic(), async cluster =>
        {
            CancellationToken ct = Ct;
            const string path = "/har005";
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { OperationTimeout = TimeSpan.FromSeconds(3) }, ct);
            DfsVolume vol = client.GetVolume(Vol);
            NodeCounters before = await cluster.Inspect.GetTotalCountersAsync();

            FaultArm arm = cluster.Faults.Arm(new FaultMatch(FaultSites.ObjectBeforeFlush, Volume: Vol, Count: int.MaxValue),
                fault == ClusterFlushFault.IoError ? FaultAction.IoError() : FaultAction.Delay(TimeSpan.FromMinutes(10)));
            DfsOperationId id = await vol.NewOperationIdAsync(ct);
            byte[] data = TestData.Bytes(500, (256 * 1024) + 3);
            Exception? failure = await Record.ExceptionAsync(() => vol.Files.WriteAllBytesAsync(path, data, new DfsMutationOptions { OperationId = id }, ct));

            Assert.NotNull(failure); // no copy ever passed its durability barrier: success would be fabricated
            Assert.True(IsAvailabilityFailure(failure), $"expected an honest unavailability/unknown-outcome error, got {failure}");
            cluster.Faults.AssertReached(arm);
            NodeCounters after = await cluster.Inspect.GetTotalCountersAsync();
            Assert.Equal(before.ReceiptsIssued, after.ReceiptsIssued); // no receipt for bytes that are only pending

            // Power-cut every node while the failed or pending flushes are outstanding.
            foreach (TestNode node in cluster.Nodes)
                await node.CrashAsync(CrashMode.LoseUnflushed);
            arm.Disarm();
            foreach (TestNode node in cluster.Nodes)
                await node.RestartAsync();
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), ct);

            await using DfsClient observer = await cluster.ConnectAsync(new TestClientOptions { Name = "c1" }, ct);
            DfsVolume v = observer.GetVolume(Vol);
            Assert.False(await v.Files.ExistsAsync(path, ct), "a write that never reached a durable copy cannot exist after a power cut");
            DfsOperationOutcome outcome = await v.QueryOperationAsync(id, resolve: true, ct);
            Assert.Contains(outcome.Status, new[] { DfsOutcomeStatus.Aborted, DfsOutcomeStatus.Expired });
            IReadOnlySet<ObjectId> stored = await cluster.Inspect.GetAllStoredObjectsAsync(Vol);
            foreach (FaultContext hit in arm.Contexts)
            {
                if (hit.Object is ObjectId o)
                    Assert.DoesNotContain(o, stored);
            }
        }, Ct);

    // =====================================================================================================================
    // HAR-006 NetworkDirectionality
    // =====================================================================================================================

    public static TheoryData<LinkFault, bool, int> LinkCases()
    {
        var data = new TheoryData<LinkFault, bool, int>();
        foreach (LinkFault fault in Enum.GetValues<LinkFault>())
        {
            foreach (bool leaderToFollower in new[] { true, false })
            {
                data.Add(fault, leaderToFollower, 3);
                data.Add(fault, leaderToFollower, 5);
            }
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(LinkCases))]
    [Catalog("HAR-006", "U", "INV-20")]
    public Task HAR_006_NetworkDirectionality(LinkFault fault, bool cutLeaderToFollower, int nodes) =>
        ClusterTest.RunAsync(Deterministic(nodes), async cluster =>
        {
            CancellationToken ct = Ct;
            TestNode leader = await cluster.GetLeaderAsync(Vol, ct);
            TestNode[] others = cluster.Nodes.Where(n => n.Name != leader.Name).ToArray();
            TestNode follower = others[0];
            TestNode bystander = others[1];
            string from = cutLeaderToFollower ? leader.Name : follower.Name;
            string to = cutLeaderToFollower ? follower.Name : leader.Name;
            MessageLog log = cluster.Network.Log;

            if (fault == LinkFault.Cut)
                cluster.Network.Cut(from, to);
            else
                cluster.Network.BlackHole(from, to);
            log.Reset();
            await cluster.DelayAsync(TimeSpan.FromSeconds(2), ct);

            Assert.Equal(0L, log.CountMessages(new MessageFilter(From: from, To: to)));
            Assert.True(log.CountMessages(new MessageFilter(From: to, To: from)) > 0, $"{to}->{from} must keep delivering while only {from}->{to} is cut");
            Assert.True(log.CountMessages(new MessageFilter(From: leader.Name, To: bystander.Name)) > 0, "unrelated links are unaffected");
            Assert.True(log.CountMessages(new MessageFilter(From: bystander.Name, To: leader.Name)) > 0, "unrelated links are unaffected");
            await cluster.Inspect.AssertNoSplitBrainAsync();

            cluster.Network.Restore(from, to);
            log.Reset();
            await cluster.DelayAsync(TimeSpan.FromSeconds(2), ct);
            Assert.True(log.CountMessages(new MessageFilter(From: from, To: to)) > 0, "the restored direction delivers again");
            Assert.True(log.CountMessages(new MessageFilter(From: to, To: from)) > 0);
            await cluster.Inspect.AssertNoSplitBrainAsync();
        }, Ct);

    [Theory]
    [InlineData(ClientCut.ClientToCluster)]
    [InlineData(ClientCut.ClusterToClient)]
    [Catalog("HAR-006", "U", "INV-06", "INV-08", "INV-20")]
    public Task HAR_006_NetworkDirectionality_RequestReplyDistinction(ClientCut cut) =>
        ClusterTest.RunAsync(Deterministic(), async cluster =>
        {
            CancellationToken ct = Ct;
            const string client0 = "client:c0";
            const string path = "/har006";
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0", OperationTimeout = TimeSpan.FromSeconds(2) }, ct);
            await using DfsClient observer = await cluster.ConnectAsync(new TestClientOptions { Name = "c1" }, ct);
            DfsVolume vol = client.GetVolume(Vol);
            DfsVolume observed = observer.GetVolume(Vol);
            await vol.Files.WriteAllBytesAsync("/har006-warm", TestData.Bytes(60, 100), ct); // connections and session established
            DfsOperationId id = await vol.NewOperationIdAsync(ct);
            byte[] payload = TestData.Bytes(61, 2000);

            foreach (TestNode node in cluster.Nodes)
            {
                if (cut == ClientCut.ClientToCluster)
                    cluster.Network.Cut(client0, node.Name);
                else
                    cluster.Network.Cut(node.Name, client0);
            }

            MessageLog log = cluster.Network.Log;
            log.Reset();
            Exception? failure = await Record.ExceptionAsync(() => vol.Files.WriteAllBytesAsync(path, payload, new DfsMutationOptions { OperationId = id }, ct));
            long requestsDelivered = log.CountMessages(new MessageFilter(From: client0, Direction: LinkDirection.Request));
            long deliveredToClient = log.CountMessages(new MessageFilter(To: client0));

            Assert.NotNull(failure); // no reply can reach the client: success would be fabricated
            Assert.True(IsAvailabilityFailure(failure), $"expected an honest unavailability/unknown-outcome error, got {failure}");
            Assert.Equal(0L, deliveredToClient);
            if (cut == ClientCut.ClientToCluster)
            {
                Assert.Equal(0L, requestsDelivered);
                Assert.False(await observed.Files.ExistsAsync(path, ct), "a request that was never delivered cannot have executed");
            }
            else
            {
                Assert.True(requestsDelivered > 0, "requests still travel client-to-cluster");
                Assert.Equal(OpOutcome.Unknown, HistoryRecorder.Classify(failure)); // executed but unacknowledged: never reported as rolled back
                await BytesEqualAsync(payload, observed.Files.ReadAllBytesAsync(path, ct), "the delivered request executed as sent");
            }

            cluster.Network.Heal();
            DfsOperationOutcome outcome = await vol.QueryOperationAsync(id, resolve: true, ct);
            Assert.Equal(id, outcome.Id);
            if (cut == ClientCut.ClientToCluster)
            {
                Assert.Contains(outcome.Status, new[] { DfsOutcomeStatus.Aborted, DfsOutcomeStatus.Expired });
                Assert.False(await observed.Files.ExistsAsync(path, ct));
            }
            else
            {
                Assert.Equal(DfsOutcomeStatus.Committed, outcome.Status);
                Assert.False(outcome.Token.IsNone);
            }
        }, Ct);

    // =====================================================================================================================
    // HAR-007 TransportFidelity
    // =====================================================================================================================

    [Theory]
    [InlineData(TransportKind.InMemory)]
    [InlineData(TransportKind.Quic)]
    [Catalog("HAR-007", "U", "INV-07", "INV-14", "INV-20")]
    public Task HAR_007_TransportFidelity(TransportKind transport)
    {
        TestClusterOptions options = transport == TransportKind.InMemory
            ? Deterministic()
            : TestClusterOptions.ThreeZones with { Transport = TransportKind.Quic };
        return ClusterTest.RunAsync(options, async cluster =>
        {
            CancellationToken ct = Ct;
            const int writers = 4;
            var clients = new List<DfsClient>();
            try
            {
                for (int i = 0; i < writers; i++)
                {
                    clients.Add(await cluster.ConnectAsync(new TestClientOptions { Name = "c" + i }, ct));
                    // Different per-link latencies make independent streams interleave and complete out of order.
                    for (int n = 0; n < cluster.Nodes.Count; n++)
                    {
                        cluster.Network.SetLatency("client:c" + i, cluster.Nodes[n].Name, TimeSpan.FromMilliseconds(((i * 3) + (n * 7)) % 11));
                        cluster.Network.SetLatency(cluster.Nodes[n].Name, "client:c" + i, TimeSpan.FromMilliseconds(((i * 5) + n) % 7));
                    }
                }

                byte[][] payloads = Enumerable.Range(0, writers).Select(i => TestData.Bytes(700 + (ulong)i, (300 * 1024) + 17 + i)).ToArray();
                await Task.WhenAll(Enumerable.Range(0, writers).Select(i =>
                    clients[i].GetVolume(Vol).Files.WriteAllBytesAsync($"/har007-{i}", payloads[i], ct)));

                // Every multi-frame message arrived with its bytes in order inside its stream.
                for (int reader = 0; reader < writers; reader++)
                {
                    for (int i = 0; i < writers; i++)
                        await BytesEqualAsync(payloads[i], clients[reader].GetVolume(Vol).Files.ReadAllBytesAsync($"/har007-{i}", ct), $"reader c{reader}, file {i}");
                }

                Assert.True(cluster.Network.Log.CountMessages(new MessageFilter(Type: MessageType.PutObject)) >= writers);
            }
            finally
            {
                foreach (DfsClient c in clients)
                    await c.DisposeAsync();
            }
        }, Ct);
    }

    [Fact]
    [Catalog("HAR-007", "U", "INV-06", "INV-07", "INV-20")]
    public Task HAR_007_TransportFidelity_ReorderedRetryNeverRegresses() =>
        ClusterTest.RunAsync(Deterministic(), async cluster =>
        {
            CancellationToken ct = Ct;
            const string path = "/har007-retry";
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { OperationTimeout = TimeSpan.FromSeconds(2) }, ct);
            DfsVolume vol = client.GetVolume(Vol);
            byte[] v1 = TestData.Bytes(7101, 1500);
            byte[] v2 = TestData.Bytes(7102, 1500);
            DfsOperationId k1 = await vol.NewOperationIdAsync(ct);
            var sameIdentity = new DfsMutationOptions { OperationId = k1 };

            // The first transmission of k1 is delayed far beyond the caller's deadline (an old correlated message).
            MessageRule delayed = cluster.Network.DelayNext(
                new MessageFilter(From: "client:c0", Operation: DfsOperationKind.WriteAll, Direction: LinkDirection.Request), TimeSpan.FromSeconds(20), count: 1);
            CommitToken? firstToken = null;
            try
            {
                firstToken = await vol.Files.WriteAllBytesAsync(path, v1, sameIdentity, ct);
            }
            catch (Exception ex) when (HistoryRecorder.Classify(ex) == OpOutcome.Unknown)
            {
                // Unknown is the honest answer while the only transmission is still in flight.
            }

            Assert.Equal(1, delayed.Matched);
            CommitToken t1 = await vol.Files.WriteAllBytesAsync(path, v1, sameIdentity, ct); // retry with the same identity
            if (firstToken is CommitToken f)
                Assert.Equal(f, t1);
            CommitToken t2 = await vol.Files.WriteAllBytesAsync(path, v2, ct);
            Assert.True(t2.CompareTo(t1) > 0);
            DfsFileInfo before = await vol.Files.GetInfoAsync(path, ct);

            await cluster.DelayAsync(TimeSpan.FromSeconds(25), ct); // the delayed original is now delivered (or discarded)

            await BytesEqualAsync(v2, vol.Files.ReadAllBytesAsync(path, ct), "a reordered duplicate of an applied request must not roll the file back");
            DfsFileInfo after = await vol.Files.GetInfoAsync(path, ct);
            Assert.Equal(before.InodeId, after.InodeId);
            Assert.Equal(before.Revision, after.Revision);
            DfsOperationOutcome outcome = await vol.QueryOperationAsync(k1, resolve: false, ct);
            Assert.Contains(outcome.Status, new[] { DfsOutcomeStatus.Committed, DfsOutcomeStatus.Expired });
            if (outcome.Status == DfsOutcomeStatus.Committed)
                Assert.Equal(t1, outcome.Token);
        }, Ct);

    [Theory]
    [InlineData(100)]
    [InlineData(150 * 1024)]
    [Catalog("HAR-007", "U", "INV-06", "INV-07", "INV-20")]
    public Task HAR_007_TransportFidelity_ApplicationDuplicationIsDistinct(int suffixLength) =>
        ClusterTest.RunAsync(Deterministic(), async cluster =>
        {
            CancellationToken ct = Ct;
            const string path = "/har007-dup";
            await using DfsClient client = await cluster.ConnectAsync(null, ct);
            DfsVolume vol = client.GetVolume(Vol);
            byte[] prefix = TestData.Bytes(7201, 3000);
            byte[] suffix = TestData.Bytes(7202, suffixLength);
            await vol.Files.WriteAllBytesAsync(path, prefix, ct);

            MessageLog log = cluster.Network.Log;
            var appendRequests = new MessageFilter(From: "client:c0", Operation: DfsOperationKind.Append, Direction: LinkDirection.Request);
            var objectUploads = new MessageFilter(From: "client:c0", Type: MessageType.PutObject, Direction: LinkDirection.Request);
            log.Reset();
            MessageRule duplicateAppend = cluster.Network.DuplicateNext(appendRequests, 1);
            MessageRule duplicateUpload = cluster.Network.DuplicateNext(objectUploads, 1);

            CommitToken token = await vol.Files.AppendAllBytesAsync(path, suffix, ct);
            Assert.False(token.IsNone);
            Assert.Equal(1, duplicateAppend.Matched);
            long appends = log.CountMessages(appendRequests);
            Assert.True(appends >= 2, "the duplicated application message is delivered as a second whole message");
            Assert.Equal(0L, log.CountBytes(appendRequests) % appends); // whole-message copies, never partial duplication inside a stream
            if (suffixLength > 4096)
            {
                Assert.Equal(1, duplicateUpload.Matched);
                Assert.True(log.CountMessages(objectUploads) >= 2);
            }

            duplicateUpload.Cancel();
            await BytesEqualAsync(prefix.Concat(suffix).ToArray(), vol.Files.ReadAllBytesAsync(path, ct), "the duplicate executes at most once");
        }, Ct);

    // =====================================================================================================================
    // HAR-008 CrashAndPauseDiffer
    // =====================================================================================================================

    [Theory]
    [InlineData(NodeFailureMode.Pause, true)]
    [InlineData(NodeFailureMode.Crash, true)]
    [InlineData(NodeFailureMode.Pause, false)]
    [InlineData(NodeFailureMode.Crash, false)]
    [Catalog("HAR-008", "U", "INV-01", "INV-05", "INV-20")]
    public Task HAR_008_CrashAndPauseDiffer(NodeFailureMode mode, bool viaFaultPoint) =>
        ClusterTest.RunAsync(Deterministic(), async cluster =>
        {
            CancellationToken ct = Ct;
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { OperationTimeout = TimeSpan.FromSeconds(5) }, ct);
            DfsVolume vol = client.GetVolume(Vol);
            await vol.Files.WriteAllBytesAsync("/har008-base", TestData.Bytes(80, 2000), ct);
            await cluster.WaitForReplicationAsync(Vol, ct);
            TestNode leader = await cluster.GetLeaderAsync(Vol, ct);
            TestNode victim = cluster.Nodes.First(n => n.Name != leader.Name);
            ulong incarnation = victim.Incarnation;
            NodeHost? hostBefore = victim.Host;
            Assert.NotNull(hostBefore);
            NodeCounters countersBefore = await victim.GetCountersAsync();
            int recoveryBefore = RecoveryHits(cluster, victim.Name);

            FaultArm? arm = null;
            if (viaFaultPoint)
            {
                arm = cluster.Faults.Arm(new FaultMatch(FaultSites.ObjectBeforeFlush, Node: victim.Name, Volume: Vol),
                    mode == NodeFailureMode.Pause ? FaultAction.PauseNode() : FaultAction.CrashNode(CrashMode.LoseUnflushed));
            }
            else if (mode == NodeFailureMode.Pause)
            {
                await victim.PauseAsync();
            }
            else
            {
                await victim.CrashAsync(CrashMode.LoseUnflushed);
            }

            byte[] data = TestData.Bytes(81, (200 * 1024) + 3);
            CommitToken token = await vol.Files.WriteAllBytesAsync("/har008", data, ct); // the two other nodes satisfy ack-2
            Assert.False(token.IsNone);
            ObjectId? interrupted = null;
            if (arm is not null)
            {
                await arm.WaitTriggeredAsync(TimeSpan.FromSeconds(30));
                cluster.Faults.AssertReached(arm);
                interrupted = Assert.Single(arm.Contexts).Object;
                Assert.NotNull(interrupted);
            }

            // Both modes are independently observable.
            Assert.Equal(mode == NodeFailureMode.Pause ? TestNodeState.Paused : TestNodeState.Crashed, victim.State);
            Assert.Throws<InvalidOperationException>(() => victim.Inspect);
            Assert.Contains(cluster.Trace, line => line.Contains(victim.Name, StringComparison.Ordinal)
                && line.Contains(mode == NodeFailureMode.Pause ? "pause" : "crash", StringComparison.OrdinalIgnoreCase));
            if (interrupted is ObjectId stopped)
            {
                Assert.DoesNotContain(cluster.Faults.Coverage.Hits, h => h.Site == FaultSites.ObjectAfterFlushBeforeReceipt
                    && h.Node == victim.Name && h.Object == stopped);
            }

            if (mode == NodeFailureMode.Pause)
            {
                Assert.Same(hostBefore, victim.Host); // process memory retained
                if (arm is not null)
                    arm.Release();
                else
                    await victim.ResumeAsync();
                await cluster.WaitUntilAsync(() => Task.FromResult(victim.State == TestNodeState.Running), TimeSpan.FromSeconds(30),
                    "a released pause resumes the same process", ct);
                Assert.Equal(incarnation, victim.Incarnation);
                Assert.Same(hostBefore, victim.Host);
                Assert.Equal(recoveryBefore, RecoveryHits(cluster, victim.Name)); // no recovery ran
                if (interrupted is ObjectId resumed)
                {
                    // The old task continues where it stopped: its flush completes on the same incarnation.
                    await cluster.WaitUntilAsync(() => Task.FromResult(cluster.Faults.Coverage.Hits.Any(h => h.Site == FaultSites.ObjectAfterFlushBeforeReceipt
                        && h.Node == victim.Name && h.Object == resumed)), TimeSpan.FromSeconds(30), "the paused put resumes after release", ct);
                    Assert.True(await victim.Inspect.HasObjectAsync(resumed));
                }

                NodeCounters countersAfter = await victim.GetCountersAsync();
                Assert.True(countersAfter.RpcRequestsHandled >= countersBefore.RpcRequestsHandled, "in-memory counters survive a pause");
                Assert.True(countersAfter.RaftEntriesApplied >= countersBefore.RaftEntriesApplied, "in-memory counters survive a pause");
            }
            else
            {
                Assert.Null(victim.Host); // process memory discarded
                await victim.RestartAsync();
                Assert.Equal(TestNodeState.Running, victim.State);
                Assert.Equal(incarnation + 1, victim.Incarnation);
                Assert.NotNull(victim.Host);
                Assert.NotSame(hostBefore, victim.Host);
                Assert.True(RecoveryHits(cluster, victim.Name) > recoveryBefore, "a crash restart runs recovery (journal replay / checkpoint install)");
            }

            await cluster.WaitForReplicationAsync(Vol, ct);
            await BytesEqualAsync(data, vol.Files.ReadAllBytesAsync("/har008", ct));
            await cluster.Inspect.AssertReplicasConvergedAsync(Vol);
        }, Ct);

    private static int RecoveryHits(TestCluster cluster, string node) =>
        cluster.Faults.Coverage.Hits.Count(h => h.Node == node
            && (h.Site == FaultSites.RecoveryJournalReplay || h.Site == FaultSites.RecoveryCheckpointInstall));

    // =====================================================================================================================
    // HAR-009 HistoryCapturesAmbiguity
    // =====================================================================================================================

    public static TheoryData<string, OpOutcome> ClassificationCases => new()
    {
        { "ok", OpOutcome.Ok },
        { "outcome-unknown", OpOutcome.Unknown },
        { "outcome-unknown-code", OpOutcome.Unknown },
        { "aggregate-outcome-unknown", OpOutcome.Unknown },
        { "timeout", OpOutcome.Unknown },
        { "dfs-timeout", OpOutcome.Unknown },
        { "cancelled-after-send", OpOutcome.Unknown },
        { "cancelled-after-send-trusted", OpOutcome.Failed },
        { "cancelled-before-send", OpOutcome.Failed },
        { "transaction-conflict", OpOutcome.Failed },
        { "transaction-aborted", OpOutcome.Failed },
        { "file-not-found", OpOutcome.Failed },
    };

    [Theory]
    [MemberData(nameof(ClassificationCases))]
    [Catalog("HAR-009", "U", "INV-06", "INV-20")]
    public async Task HAR_009_HistoryCapturesAmbiguity(string scenario, OpOutcome expected)
    {
        var recorder = new HistoryRecorder { TrustCancellationAsAbort = scenario == "cancelled-after-send-trusted" };
        long inFlight = recorder.Invoke("other", "write", "k", V("x")); // an unrelated in-flight operation
        var gate = new TaskCompletionSource<int>();
        var opId = new DfsOperationId(3, 9);
        Exception? injected = scenario switch
        {
            "outcome-unknown" => new DfsCommitOutcomeUnknownException(opId, "reply lost after commit"),
            "outcome-unknown-code" => new DfsException(DfsErrorCode.OutcomeUnknown, "outcome unknown"),
            "aggregate-outcome-unknown" => new AggregateException(new IOException("stream reset"), new DfsCommitOutcomeUnknownException(opId, "reply lost")),
            "timeout" => new TimeoutException("deadline"),
            "dfs-timeout" => new DfsException(DfsErrorCode.Timeout, "deadline"),
            "transaction-conflict" => new DfsTransactionConflictException("conflict"),
            "transaction-aborted" => new DfsTransactionAbortedException("aborted by leader change"),
            "file-not-found" => new FileNotFoundException("missing"),
            _ => null,
        };
        Func<Task<int>> action = scenario switch
        {
            "ok" => () => Task.FromResult(5),
            "cancelled-before-send" => () => throw new OperationCanceledException("cancelled before anything was sent"),
            _ => () => gate.Task,
        };

        Task<int> recording = recorder.RecordAsync("c0", "write", "k", V("v"), action);
        if (scenario.StartsWith("cancelled-after-send", StringComparison.Ordinal))
            gate.SetCanceled();
        else if (injected is not null)
            gate.SetException(injected);
        Exception? thrown = await Record.ExceptionAsync(() => recording);

        Assert.Equal(2, recorder.Count); // nothing is dropped from the history
        HistoryOp op = recorder.Get(2);
        Assert.Equal(expected, op.Outcome);
        Assert.NotNull(op.CompleteTime);
        Assert.True(op.CompleteTime > op.InvokeTime);
        Assert.False(op.WasResolved);
        if (expected == OpOutcome.Ok)
        {
            Assert.Null(thrown);
            Assert.Equal(5, op.Output);
            Assert.Equal(op.CompleteTime, op.EffectDeadline);
        }
        else
        {
            Assert.NotNull(thrown);
            Assert.Same(thrown, op.Output); // the caller sees the same exception the history recorded
            if (injected is not null)
                Assert.Same(injected, thrown);
            Assert.Null(op.EffectDeadline);
        }

        HistoryOp other = recorder.Get(inFlight);
        Assert.Equal(OpOutcome.Unknown, other.Outcome); // in flight: unknown, not failed and not omitted
        Assert.Null(other.CompleteTime);
        Assert.Contains(recorder.Operations, o => o.Id == inFlight);
    }

    [Fact]
    [Catalog("HAR-009", "U", "INV-06", "INV-20")]
    public void HAR_009_HistoryCapturesAmbiguity_ResolutionAndRetryIdentity()
    {
        var recorder = new HistoryRecorder();
        var retryId = new DfsOperationId(7, 1);
        long a = recorder.Invoke("c0", "write", "/f", V("a"), retryId);
        long b = recorder.Invoke("c1", "write", "/f", V("b"));
        recorder.Complete(a, OpOutcome.Unknown, new DfsCommitOutcomeUnknownException(retryId, "reply lost"));
        recorder.AttachOperationId(b, new DfsOperationId(9, 4));

        HistoryOp unknown = recorder.Get(a);
        Assert.Equal(OpOutcome.Unknown, unknown.Outcome);
        Assert.Equal(retryId, unknown.OperationId);
        Assert.Null(unknown.EffectDeadline);
        Assert.False(unknown.WasResolved);
        Assert.Equal(new DfsOperationId(9, 4), recorder.Get(b).OperationId);
        Assert.Equal(OpOutcome.Unknown, recorder.Get(b).Outcome);
        Assert.Null(recorder.Get(b).CompleteTime);

        // Authoritative resolution narrows a to committed; its effect window ends at the resolution, not the completion.
        recorder.Resolve(a, OpOutcome.Ok);
        HistoryOp resolved = recorder.Get(a);
        Assert.Equal(OpOutcome.Ok, resolved.Outcome);
        Assert.True(resolved.WasResolved);
        Assert.Equal(resolved.ResolveTime, resolved.EffectDeadline);
        Assert.True(resolved.ResolveTime > resolved.CompleteTime);

        // Narrowing is one-way and never contradicts.
        Assert.Throws<InvalidOperationException>(() => recorder.Resolve(a, OpOutcome.Failed));
        Assert.Throws<ArgumentException>(() => recorder.Resolve(b, OpOutcome.Unknown));
        recorder.Resolve(b, OpOutcome.Failed); // resolved while still in flight (fenced abort)
        recorder.Complete(b, OpOutcome.Unknown); // the client's later "unknown" is consistent and ignored
        Assert.Throws<InvalidOperationException>(() => recorder.Complete(b, OpOutcome.Ok)); // a contradiction is an error
        Assert.Equal(OpOutcome.Failed, recorder.Get(b).Outcome);

        long c = recorder.Invoke("c2", "read", "/f", null);
        Assert.True(recorder.Get(c).InvokeTime > recorder.Get(b).CompleteTime);
        Assert.Equal(3, recorder.Count);
    }

    [Fact]
    [Catalog("HAR-009", "U", "INV-06", "INV-20")]
    public void HAR_009_HistoryCapturesAmbiguity_UnknownIsNotTreatedAsFailed()
    {
        var model = new RegisterModel(V("v0"));
        LinearizabilityChecker.Check(
            [Op(1, "c0", "write", "k", V("v1"), 1, 2, OpOutcome.Unknown), Op(2, "c1", "read", "k", null, 3, 4, OpOutcome.Ok, V("v1"))], model)
            .AssertValid(); // an unknown write may have taken effect
        LinearizabilityChecker.Check(
            [Op(1, "c0", "write", "k", V("v1"), 1, 2, OpOutcome.Unknown), Op(2, "c1", "read", "k", null, 3, 4, OpOutcome.Ok, V("v0"))], model)
            .AssertValid(); // ... or not
        LinearizabilityChecker.Check(
            [Op(1, "c0", "write", "k", V("v1"), 1, null, OpOutcome.Unknown), Op(2, "c1", "read", "k", null, 3, 4, OpOutcome.Ok, V("v0")),
                Op(3, "c2", "read", "k", null, 5, 6, OpOutcome.Ok, V("v1"))], model)
            .AssertValid(); // an in-flight write can take effect at any later point

        CheckResult failedObserved = LinearizabilityChecker.Check(
            [Op(1, "c0", "write", "k", V("v1"), 1, 2, OpOutcome.Failed), Op(2, "c1", "read", "k", null, 3, 4, OpOutcome.Ok, V("v1"))], model);
        Assert.False(failedObserved.IsValid); // a definite abort can never be observed later
        Assert.False(failedObserved.IsInconclusive);
    }

    [Theory]
    [InlineData(MessageLoss.DropReplies)]
    [InlineData(MessageLoss.DropRequests)]
    [Catalog("HAR-009", "U", "INV-06", "INV-20")]
    public Task HAR_009_HistoryCapturesAmbiguity_LostMessages(MessageLoss loss) =>
        ClusterTest.RunAsync(Deterministic(), async cluster =>
        {
            CancellationToken ct = Ct;
            const string path = "/har009";
            HistoryRecorder history = cluster.History;
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { OperationTimeout = TimeSpan.FromSeconds(2) }, ct);
            DfsVolume vol = client.GetVolume(Vol);
            byte[] payload = TestData.Bytes(900, 2048);
            DfsOperationId id = await vol.NewOperationIdAsync(ct);
            var writes = new MessageFilter(From: "client:c0", Operation: DfsOperationKind.WriteAll, Direction: LinkDirection.Request);
            MessageRule rule = loss == MessageLoss.DropReplies
                ? cluster.Network.DropReplies(writes, count: 1000)
                : cluster.Network.DropNext(writes, count: 1000);

            long hid = history.Invoke("c0", "write", path, payload, id);
            Exception? failure = await Record.ExceptionAsync(() => vol.Files.WriteAllBytesAsync(path, payload, new DfsMutationOptions { OperationId = id }, ct));
            Assert.NotNull(failure);
            OpOutcome observedOutcome = HistoryRecorder.Classify(failure);
            history.Complete(hid, observedOutcome, failure);
            Assert.Equal(OpOutcome.Unknown, observedOutcome); // the client cannot know whether the request executed
            Assert.True(rule.Matched >= 1);
            rule.Cancel();

            HistoryOp recorded = history.Get(hid);
            Assert.Equal(OpOutcome.Unknown, recorded.Outcome);
            Assert.Equal(id, recorded.OperationId);
            Assert.Contains(history.Operations, o => o.Id == hid);

            DfsOperationOutcome outcome = await vol.QueryOperationAsync(id, resolve: true, ct);
            if (loss == MessageLoss.DropReplies)
            {
                Assert.Equal(DfsOutcomeStatus.Committed, outcome.Status);
                Assert.False(outcome.Token.IsNone);
                history.Resolve(hid, OpOutcome.Ok);
            }
            else
            {
                Assert.Contains(outcome.Status, new[] { DfsOutcomeStatus.Aborted, DfsOutcomeStatus.Expired });
                history.Resolve(hid, OpOutcome.Failed);
            }

            Assert.True(history.Get(hid).WasResolved);
            byte[]? content = await history.RecordAsync("c0", "read", path, null,
                async () => await vol.Files.ExistsAsync(path, ct) ? await vol.Files.ReadAllBytesAsync(path, ct) : (byte[]?)null);
            if (loss == MessageLoss.DropReplies)
                AssertEx.BytesEqual(payload, content, "the request executed although its reply was lost");
            else
                Assert.Null(content);
            LinearizabilityChecker.Check(history.Operations, new RegisterModel()).AssertValid();
        }, Ct);

    // =====================================================================================================================
    // HAR-010 OracleIsIndependent
    // =====================================================================================================================

    [Theory]
    [InlineData(ByteCorruption.BitFlip)]
    [InlineData(ByteCorruption.TruncatedTail)]
    [InlineData(ByteCorruption.ZeroFilledTail)]
    [InlineData(ByteCorruption.ExtraTrailingBytes)]
    [InlineData(ByteCorruption.StaleCommittedImage)]
    [InlineData(ByteCorruption.ResurrectedTruncatedBytes)]
    [InlineData(ByteCorruption.TornMix)]
    [Catalog("HAR-010", "U", "INV-07", "INV-14", "INV-20")]
    public void HAR_010_OracleIsIndependent(ByteCorruption corruption)
    {
        byte[] initial = TestData.Bytes(1010, 5000);
        byte[] x = TestData.Bytes(1011, 3000);
        byte[] y = TestData.Bytes(1012, 100);
        byte[] z = TestData.Bytes(1013, 50);
        var model = new ReferenceFileModel(initial);
        model.Write(4096, x); // extends to 7096
        model.SetLength(6000); // truncates
        model.SetLength(9000); // extension exposes zeros, never the truncated bytes
        model.Write(8500, y);
        model.Write(100, z);

        // Independent hand computation of the same semantics.
        byte[] expected = new byte[9000];
        initial.CopyTo(expected, 0);
        x.AsSpan(0, 6000 - 4096).CopyTo(expected.AsSpan(4096));
        y.CopyTo(expected, 8500);
        z.CopyTo(expected, 100);
        AssertEx.BytesEqual(expected, model.Staged, "oracle staged image");
        AssertEx.BytesEqual(initial, model.Committed, "nothing is committed before Flush");
        Assert.True(model.HasPendingChanges);
        AssertEx.BytesEqual(expected.AsSpan(8990).ToArray(), model.Read(8990, 100, staged: true), "short read at end of file");
        Assert.Empty(model.Read(9000, 10, staged: true));

        byte[] correct = model.Read(0, 9000, staged: true);
        byte[] corrupted = corruption switch
        {
            ByteCorruption.BitFlip => Flip(correct, 4321),
            ByteCorruption.TruncatedTail => correct[..^1],
            ByteCorruption.ZeroFilledTail => Zero(correct, 8500, 100), // missing data presented as zeros
            ByteCorruption.ExtraTrailingBytes => [.. correct, 0],
            ByteCorruption.StaleCommittedImage => model.Read(0, 9000, staged: false),
            ByteCorruption.ResurrectedTruncatedBytes => Splice(correct, 6000, x.AsSpan(6000 - 4096).ToArray()),
            ByteCorruption.TornMix => [.. correct.AsSpan(0, 4500), .. Pad(model.Committed, 9000).AsSpan(4500)],
            _ => throw new ArgumentOutOfRangeException(nameof(corruption), corruption, null),
        };
        Assert.False(corrupted.AsSpan().SequenceEqual(correct), $"{corruption}: the corrupted implementation output must disagree with the oracle");
        AssertEx.BytesEqual(expected, correct);

        model.Flush();
        AssertEx.BytesEqual(expected, model.Committed);
        Assert.False(model.HasPendingChanges);
        model.Write(0, y);
        model.Abort();
        AssertEx.BytesEqual(expected, model.Staged, "abort discards staged changes");

        static byte[] Flip(byte[] data, int offset)
        {
            byte[] copy = data.ToArray();
            copy[offset] ^= 0x01;
            return copy;
        }

        static byte[] Zero(byte[] data, int offset, int length)
        {
            byte[] copy = data.ToArray();
            Array.Clear(copy, offset, length);
            return copy;
        }

        static byte[] Splice(byte[] data, int offset, byte[] insert)
        {
            byte[] copy = data.ToArray();
            insert.CopyTo(copy, offset);
            return copy;
        }

        static byte[] Pad(byte[] data, int length)
        {
            byte[] copy = new byte[length];
            data.AsSpan(0, Math.Min(length, data.Length)).CopyTo(copy);
            return copy;
        }
    }

    [Theory]
    [InlineData(NamespaceCorruption.ResurrectedDeletedName)]
    [InlineData(NamespaceCorruption.MissingEntry)]
    [InlineData(NamespaceCorruption.InodeChangedByMove)]
    [InlineData(NamespaceCorruption.StaleEntryAfterDirectoryMove)]
    [InlineData(NamespaceCorruption.CopyOverwriteAllocatedNewInode)]
    [InlineData(NamespaceCorruption.ReusedInodeForNewFile)]
    [Catalog("HAR-010", "U", "INV-07", "INV-20")]
    public void HAR_010_OracleIsIndependent_NamespaceModel(NamespaceCorruption corruption)
    {
        var model = new ReferenceNamespaceModel();
        Assert.True(model.CreateDirectory("/a", out _));
        Assert.True(model.CreateFile("/a/x", out _));
        Assert.True(model.CreateFile("/a/y", out _));
        Assert.True(model.CreateDirectory("/b", out _));
        Assert.True(model.CreateDirectory("/a/sub", out _));
        Assert.True(model.CreateFile("/a/sub/z", out _));
        long x = model.InodeOf("/a/x");
        long y = model.InodeOf("/a/y");
        long z = model.InodeOf("/a/sub/z");
        long a = model.InodeOf("/a");
        Assert.True(model.Move("/a/x", "/b/x", overwrite: false, out _));
        Assert.True(model.Delete("/a/y", recursive: false, out _));
        Assert.True(model.Move("/a", "/c", overwrite: false, out _));
        Assert.True(model.CopyFile("/b/x", "/c/sub/z", overwrite: true, out _));

        // The oracle's own semantics, checked independently of any implementation.
        Assert.Equal(x, model.InodeOf("/b/x"));
        Assert.Equal(a, model.InodeOf("/c"));
        Assert.Equal(z, model.InodeOf("/c/sub/z"));
        Assert.False(model.Exists("/a"));
        Assert.False(model.Exists("/a/y"));
        Assert.False(model.Exists("/a/sub/z"));
        Assert.False(model.Delete("/c", recursive: false, out string? notEmpty));
        Assert.Equal(nameof(DfsErrorCode.DirectoryNotEmpty), notEmpty);
        Assert.False(model.Move("/c", "/c/sub/q", overwrite: false, out string? cycle));
        Assert.Equal(nameof(DfsErrorCode.InvalidArgument), cycle);
        Assert.False(model.CreateFile("/b/x", out string? exists));
        Assert.Equal(nameof(DfsErrorCode.AlreadyExists), exists);
        Assert.Equal(new[] { "sub" }, model.List("/c"));

        Dictionary<string, long> correct = model.AllPaths().ToDictionary(p => p, model.InodeOf, StringComparer.Ordinal);
        var observed = new Dictionary<string, long>(correct, StringComparer.Ordinal);
        string corrupted;
        switch (corruption)
        {
            case NamespaceCorruption.ResurrectedDeletedName:
                corrupted = "/c/y";
                observed[corrupted] = y;
                break;
            case NamespaceCorruption.MissingEntry:
                corrupted = "/b/x";
                observed.Remove(corrupted);
                break;
            case NamespaceCorruption.InodeChangedByMove:
                corrupted = "/b/x";
                observed[corrupted] = model.LastAllocatedInode + 100;
                break;
            case NamespaceCorruption.StaleEntryAfterDirectoryMove:
                corrupted = "/a/sub/z";
                observed[corrupted] = z;
                break;
            case NamespaceCorruption.CopyOverwriteAllocatedNewInode:
                corrupted = "/c/sub/z";
                observed[corrupted] = model.LastAllocatedInode + 1;
                break;
            case NamespaceCorruption.ReusedInodeForNewFile:
                corrupted = "/b/new";
                observed[corrupted] = y;
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(corruption), corruption, null);
        }

        IReadOnlyList<string> diff = NamespaceDiff(correct, observed);
        Assert.NotEmpty(diff);
        Assert.Contains(diff, d => d.Contains(corrupted, StringComparison.Ordinal));
        Assert.Empty(NamespaceDiff(correct, new Dictionary<string, long>(correct, StringComparer.Ordinal)));

        static IReadOnlyList<string> NamespaceDiff(IReadOnlyDictionary<string, long> oracle, IReadOnlyDictionary<string, long> output)
        {
            var problems = new List<string>();
            foreach ((string path, long inode) in oracle)
            {
                if (!output.TryGetValue(path, out long actual))
                    problems.Add($"missing {path}");
                else if (actual != inode)
                    problems.Add($"inode of {path}: expected {inode}, got {actual}");
            }

            foreach (string path in output.Keys.Where(p => !oracle.ContainsKey(p)))
                problems.Add($"unexpected {path}");
            return problems;
        }
    }

    [Fact]
    [Catalog("HAR-010", "U", "INV-03", "INV-04", "INV-20")]
    public void HAR_010_OracleIsIndependent_MvccModelRejectsStaleOrMixedVersions()
    {
        Dictionary<string, string?> initial = State(("/x", "0"), ("/y", "0"));
        TxRecord t1 = Tx("T1", 1, 2, OpOutcome.Ok, [], [("/x", "1"), ("/y", "1")]);
        TxRecord t2 = Tx("T2", 3, 6, OpOutcome.Ok, [], [("/x", "2"), ("/y", "2")]);

        // A reader starting after T2 completed must see version 2.
        SerializabilityChecker.CheckStrict([t1, t2, Tx("R", 7, 8, OpOutcome.Ok, [("/x", "2"), ("/y", "2")], [])], initial).AssertValid();
        CheckResult stale = SerializabilityChecker.CheckStrict([t1, t2, Tx("R", 7, 8, OpOutcome.Ok, [("/x", "1"), ("/y", "1")], [])], initial);
        Assert.False(stale.IsValid);
        Assert.False(stale.IsInconclusive);

        // A reader overlapping T2 may see version 1 or 2 but never a mix of both.
        SerializabilityChecker.CheckStrict([t1, t2, Tx("R", 4, 5, OpOutcome.Ok, [("/x", "1"), ("/y", "1")], [])], initial).AssertValid();
        SerializabilityChecker.CheckStrict([t1, t2, Tx("R", 4, 5, OpOutcome.Ok, [("/x", "2"), ("/y", "2")], [])], initial).AssertValid();
        CheckResult mixed = SerializabilityChecker.CheckStrict([t1, t2, Tx("R", 4, 5, OpOutcome.Ok, [("/x", "2"), ("/y", "1")], [])], initial);
        Assert.False(mixed.IsValid);
        Assert.False(mixed.IsInconclusive);
    }

    [Fact]
    [Catalog("HAR-010", "U", "INV-20")]
    public void HAR_010_OracleIsIndependent_OraclesShareNoImplementationCode()
    {
        Type[] oracles =
        [
            typeof(ReferenceFileModel), typeof(ReferenceNamespaceModel), typeof(RegisterModel), typeof(AppendLogModel),
            typeof(LinearizabilityChecker), typeof(SerializabilityChecker), typeof(ModelChecker),
        ];
        Assembly harness = typeof(RegisterModel).Assembly;
        (IReadOnlySet<Type> referenced, int scanned, IReadOnlyList<string> unresolved) = IlAudit.ReferencedTypes(oracles, t => t.Assembly == harness);
        Assert.True(unresolved.Count == 0, "unresolved references (missing evidence is not a pass): " + string.Join("; ", unresolved.Take(10)));
        Assert.True(scanned >= oracles.Length);

        Assembly[] implementation =
        [
            typeof(SimulatedMedium).Assembly, typeof(NodeHost).Assembly, typeof(RaftNode).Assembly, typeof(DfsClient).Assembly,
            typeof(InMemoryNetwork).Assembly,
        ];
        List<string> sharedImplementation = referenced.Where(t => implementation.Contains(t.Assembly)).Select(t => t.FullName ?? t.Name).ToList();
        Assert.True(sharedImplementation.Count == 0, "oracles reference implementation code: " + string.Join(", ", sharedImplementation));

        // From the shared contract assembly only plain identifiers/codes are allowed (no DfsPath, Placement, codecs, ...).
        Assembly core = typeof(DfsErrorCode).Assembly;
        Type[] allowed = [typeof(DfsErrorCode), typeof(DfsOperationId), typeof(DfsTransactionId), typeof(CommitToken)];
        List<string> coreLogic = referenced.Where(t => t.Assembly == core && !allowed.Contains(t)).Select(t => t.FullName ?? t.Name).ToList();
        Assert.True(coreLogic.Count == 0, "oracles reuse contract-assembly logic: " + string.Join(", ", coreLogic));

        // Nor do they reach the implementation indirectly through harness types that drive it.
        Assert.DoesNotContain(typeof(TestCluster), referenced);
        Assert.DoesNotContain(typeof(HistoryRecorder), referenced);
        Assert.DoesNotContain(typeof(ClusterInspector), referenced);
    }

    // =====================================================================================================================
    // HAR-011 CheckerRejectsBadHistories
    // =====================================================================================================================

    public static TheoryData<string> BadHistories => new()
    {
        "lost-update-transaction",
        "lost-update-append",
        "stale-live-read",
        "stale-read-after-observed-unknown-write",
        "stale-transaction-read",
        "fractured-transaction",
        "fractured-concurrent-transaction",
        "fractured-predicate-read",
        "duplicate-append",
        "duplicate-append-after-retry",
        "write-skew",
        "predicate-write-skew",
    };

    [Theory]
    [MemberData(nameof(BadHistories))]
    [Catalog("HAR-011", "U", "INV-02", "INV-03", "INV-04", "INV-06", "INV-20")]
    public void HAR_011_CheckerRejectsBadHistories(string anomaly)
    {
        (CheckResult bad, CheckResult repaired) = EvaluateAnomaly(anomaly);
        Assert.False(bad.IsInconclusive, $"{anomaly}: the checker must reach a definite verdict: {bad}");
        Assert.False(bad.IsValid, $"{anomaly}: the anomaly was accepted: {bad}");
        Assert.False(string.IsNullOrWhiteSpace(bad.Explanation));
        Assert.ThrowsAny<XunitException>(() => bad.AssertValid());
        repaired.AssertValid(); // the minimally repaired history is accepted: the rejection is caused by the anomaly itself
    }

    private static (CheckResult Bad, CheckResult Repaired) EvaluateAnomaly(string anomaly)
    {
        var register = new RegisterModel(V("v0"));
        var log = new AppendLogModel();
        byte[] e1 = V("entry-1");
        byte[] e2 = V("entry-2");
        Dictionary<string, string?> xy = State(("/x", "0"), ("/y", "0"));
        Dictionary<string, string?> ab = State(("/a", "1"), ("/b", "1"));
        Dictionary<string, string?> oncall = State(("/oncall/alice", "on"), ("/oncall/bob", "on"));
        const OpOutcome ok = OpOutcome.Ok;

        static CheckResult Lin<TState>(IReadOnlyList<HistoryOp> ops, ISequentialModel<TState> model) => LinearizabilityChecker.Check(ops, model);
        static CheckResult Ser(IReadOnlyList<TxRecord> txs, Dictionary<string, string?> initial) => SerializabilityChecker.CheckStrict(txs, initial);

        switch (anomaly)
        {
            case "lost-update-transaction":
                // Two read-modify-write increments both commit from the same read (0 -> 1 twice).
                return (
                    Ser([Tx("T1", 1, 4, ok, [("/x", "0")], [("/x", "1")]), Tx("T2", 2, 5, ok, [("/x", "0")], [("/x", "1")]),
                        Tx("R", 6, 7, ok, [("/x", "1")], [])], xy),
                    Ser([Tx("T1", 1, 4, ok, [("/x", "0")], [("/x", "1")]), Tx("T2", 2, 5, ok, [("/x", "1")], [("/x", "2")]),
                        Tx("R", 6, 7, ok, [("/x", "2")], [])], xy));
            case "lost-update-append":
                return (
                    Lin([Op(1, "c0", "append", "/log", e1, 1, 2, ok), Op(2, "c1", "append", "/log", e2, 3, 4, ok),
                        Op(3, "c2", "read", "/log", null, 5, 6, ok, e2)], log),
                    Lin([Op(1, "c0", "append", "/log", e1, 1, 2, ok), Op(2, "c1", "append", "/log", e2, 3, 4, ok),
                        Op(3, "c2", "read", "/log", null, 5, 6, ok, e1.Concat(e2).ToArray())], log));
            case "stale-live-read":
                return (
                    Lin([Op(1, "c0", "write", "/f", V("v1"), 1, 2, ok), Op(2, "c1", "read", "/f", null, 3, 4, ok, V("v0"))], register),
                    Lin([Op(1, "c0", "write", "/f", V("v1"), 1, 2, ok), Op(2, "c1", "read", "/f", null, 3, 4, ok, V("v1"))], register));
            case "stale-read-after-observed-unknown-write":
                return (
                    Lin([Op(1, "c0", "write", "/f", V("v1"), 1, null, OpOutcome.Unknown), Op(2, "c1", "read", "/f", null, 2, 3, ok, V("v1")),
                        Op(3, "c2", "read", "/f", null, 4, 5, ok, V("v0"))], register),
                    Lin([Op(1, "c0", "write", "/f", V("v1"), 1, null, OpOutcome.Unknown), Op(2, "c1", "read", "/f", null, 2, 3, ok, V("v1")),
                        Op(3, "c2", "read", "/f", null, 4, 5, ok, V("v1"))], register));
            case "stale-transaction-read":
                return (
                    Ser([Tx("T1", 1, 2, ok, [], [("/x", "1")]), Tx("T2", 3, 4, ok, [("/x", "0")], [])], xy),
                    Ser([Tx("T1", 1, 3, ok, [], [("/x", "1")]), Tx("T2", 2, 4, ok, [("/x", "0")], [])], xy));
            case "fractured-transaction":
                return (
                    Ser([Tx("T1", 1, 2, ok, [], [("/x", "1"), ("/y", "1")]), Tx("T2", 3, 4, ok, [("/x", "1"), ("/y", "0")], [])], xy),
                    Ser([Tx("T1", 1, 2, ok, [], [("/x", "1"), ("/y", "1")]), Tx("T2", 3, 4, ok, [("/x", "1"), ("/y", "1")], [])], xy));
            case "fractured-concurrent-transaction":
                return (
                    Ser([Tx("T1", 1, 4, ok, [], [("/x", "1"), ("/y", "1")]), Tx("T2", 2, 3, ok, [("/x", "1"), ("/y", "0")], [])], xy),
                    Ser([Tx("T1", 1, 4, ok, [], [("/x", "1"), ("/y", "1")]), Tx("T2", 2, 3, ok, [("/x", "0"), ("/y", "0")], [])], xy));
            case "fractured-predicate-read":
                return (
                    Ser([Tx("T1", 1, 2, ok, [], [("/d/a", "1"), ("/d/b", "1")]), Tx("T2", 3, 4, ok, [], [], ("/d", ["a"]))], State()),
                    Ser([Tx("T1", 1, 2, ok, [], [("/d/a", "1"), ("/d/b", "1")]), Tx("T2", 3, 4, ok, [], [], ("/d", ["a", "b"]))], State()));
            case "duplicate-append":
                return (
                    Lin([Op(1, "c0", "append", "/log", e1, 1, 2, ok), Op(2, "c1", "read", "/log", null, 3, 4, ok, e1.Concat(e1).ToArray())], log),
                    Lin([Op(1, "c0", "append", "/log", e1, 1, 2, ok), Op(2, "c1", "read", "/log", null, 3, 4, ok, e1)], log));
            case "duplicate-append-after-retry":
                // One logical append (one operation identity) whose reply was lost: retries must not add a second copy.
                return (
                    Lin([Op(1, "c0", "append", "/log", e1, 1, null, OpOutcome.Unknown), Op(2, "c1", "append", "/log", e2, 2, 3, ok),
                        Op(3, "c2", "read", "/log", null, 4, 5, ok, e1.Concat(e2).Concat(e1).ToArray())], log),
                    Lin([Op(1, "c0", "append", "/log", e1, 1, null, OpOutcome.Unknown), Op(2, "c1", "append", "/log", e2, 2, 3, ok),
                        Op(3, "c2", "read", "/log", null, 4, 5, ok, e1.Concat(e2).ToArray())], log));
            case "write-skew":
                return (
                    Ser([Tx("T1", 1, 4, ok, [("/a", "1"), ("/b", "1")], [("/a", "0")]), Tx("T2", 2, 5, ok, [("/a", "1"), ("/b", "1")], [("/b", "0")])], ab),
                    Ser([Tx("T1", 1, 4, ok, [("/a", "1"), ("/b", "1")], [("/a", "0")]),
                        Tx("T2", 2, 5, OpOutcome.Failed, [("/a", "1"), ("/b", "1")], [("/b", "0")])], ab));
            case "predicate-write-skew":
                return (
                    Ser([Tx("T1", 1, 4, ok, [], [("/oncall/alice", null)], ("/oncall", ["alice", "bob"])),
                        Tx("T2", 2, 5, ok, [], [("/oncall/bob", null)], ("/oncall", ["alice", "bob"]))], oncall),
                    Ser([Tx("T1", 1, 4, ok, [], [("/oncall/alice", null)], ("/oncall", ["alice", "bob"])),
                        Tx("T2", 6, 7, ok, [], [("/oncall/bob", null)], ("/oncall", ["bob"]))], oncall));
            default:
                throw new ArgumentOutOfRangeException(nameof(anomaly), anomaly, null);
        }
    }

    // =====================================================================================================================
    // HAR-012 CheckerAcceptsLegalConcurrency
    // =====================================================================================================================

    public static TheoryData<string> LegalConcurrentHistories => new()
    {
        "overlapping-writes-either-order",
        "read-overlapping-write-sees-old-or-new",
        "overlapping-appends-either-order",
        "transactions-serialize-against-invocation-order",
        "unknown-write-may-or-may-not-apply",
        "overlapping-transactions-either-order",
    };

    [Theory]
    [MemberData(nameof(LegalConcurrentHistories))]
    [Catalog("HAR-012", "U", "INV-03", "INV-04", "INV-20")]
    public void HAR_012_CheckerAcceptsLegalConcurrency(string scenario)
    {
        IReadOnlyList<CheckResult> alternatives = EvaluateLegal(scenario);
        Assert.True(alternatives.Count >= 2, "every scenario has more than one legal serialization");
        foreach (CheckResult result in alternatives)
            result.AssertValid();
    }

    [Theory]
    [InlineData(1UL)]
    [InlineData(2UL)]
    [InlineData(3UL)]
    [InlineData(4UL)]
    [InlineData(5UL)]
    [InlineData(6UL)]
    [Catalog("HAR-012", "U", "INV-03", "INV-20")]
    public void HAR_012_CheckerAcceptsLegalConcurrency_GeneratedHistories(ulong seed)
    {
        IReadOnlyList<HistoryOp> history = GenerateLegalRegisterHistory(seed, processes: 6, opsPerProcess: 8, keys: 2);
        Assert.Equal(48, history.Count);
        Assert.Contains(history, a => history.Any(b => b.Id != a.Id && b.InvokeTime < (a.CompleteTime ?? long.MaxValue)
            && a.InvokeTime < (b.CompleteTime ?? long.MaxValue))); // the history really is concurrent
        LinearizabilityChecker.Check(history, new RegisterModel()).AssertValid();
    }

    private static IReadOnlyList<CheckResult> EvaluateLegal(string scenario)
    {
        const OpOutcome ok = OpOutcome.Ok;
        var register = new RegisterModel(V("v0"));
        var log = new AppendLogModel();
        byte[] a1 = V("a1");
        byte[] a2 = V("a2");
        Dictionary<string, string?> xy = State(("/x", "0"), ("/y", "0"));
        switch (scenario)
        {
            case "overlapping-writes-either-order":
                // w2 is invoked after w1 but may take effect first; the final read may see either.
                return
                [
                    LinearizabilityChecker.Check([Op(1, "c0", "write", "k", V("a"), 1, 4, ok), Op(2, "c1", "write", "k", V("b"), 2, 3, ok),
                        Op(3, "c2", "read", "k", null, 5, 6, ok, V("a"))], register),
                    LinearizabilityChecker.Check([Op(1, "c0", "write", "k", V("a"), 1, 4, ok), Op(2, "c1", "write", "k", V("b"), 2, 3, ok),
                        Op(3, "c2", "read", "k", null, 5, 6, ok, V("b"))], register),
                ];
            case "read-overlapping-write-sees-old-or-new":
                return
                [
                    LinearizabilityChecker.Check([Op(1, "c0", "write", "k", V("v1"), 2, 5, ok), Op(2, "c1", "read", "k", null, 3, 4, ok, V("v0"))], register),
                    LinearizabilityChecker.Check([Op(1, "c0", "write", "k", V("v1"), 2, 5, ok), Op(2, "c1", "read", "k", null, 3, 4, ok, V("v1"))], register),
                ];
            case "overlapping-appends-either-order":
                return
                [
                    LinearizabilityChecker.Check([Op(1, "c0", "append", "/log", a1, 1, 4, ok), Op(2, "c1", "append", "/log", a2, 2, 3, ok),
                        Op(3, "c2", "read", "/log", null, 5, 6, ok, a2.Concat(a1).ToArray())], log),
                    LinearizabilityChecker.Check([Op(1, "c0", "append", "/log", a1, 1, 4, ok), Op(2, "c1", "append", "/log", a2, 2, 3, ok),
                        Op(3, "c2", "read", "/log", null, 5, 6, ok, a1.Concat(a2).ToArray())], log),
                ];
            case "transactions-serialize-against-invocation-order":
                return
                [
                    SerializabilityChecker.CheckStrict([Tx("T1", 1, 4, ok, [], [("/x", "1")]), Tx("T2", 2, 3, ok, [("/x", "0")], [("/y", "1")]),
                        Tx("R", 5, 6, ok, [("/x", "1"), ("/y", "1")], [])], xy),
                    SerializabilityChecker.CheckStrict([Tx("T1", 1, 4, ok, [], [("/x", "1")]), Tx("T2", 2, 3, ok, [("/x", "1")], [("/y", "1")]),
                        Tx("R", 5, 6, ok, [("/x", "1"), ("/y", "1")], [])], xy),
                ];
            case "unknown-write-may-or-may-not-apply":
                return
                [
                    LinearizabilityChecker.Check([Op(1, "c0", "write", "k", V("v1"), 1, null, OpOutcome.Unknown), Op(2, "c1", "read", "k", null, 2, 3, ok, V("v1"))], register),
                    LinearizabilityChecker.Check([Op(1, "c0", "write", "k", V("v1"), 1, null, OpOutcome.Unknown), Op(2, "c1", "read", "k", null, 2, 3, ok, V("v0"))], register),
                ];
            case "overlapping-transactions-either-order":
                return
                [
                    SerializabilityChecker.CheckStrict([Tx("T1", 1, 4, ok, [], [("/x", "1")]), Tx("T2", 2, 5, ok, [], [("/x", "2")]),
                        Tx("R", 6, 7, ok, [("/x", "1")], [])], xy),
                    SerializabilityChecker.CheckStrict([Tx("T1", 1, 4, ok, [], [("/x", "1")]), Tx("T2", 2, 5, ok, [], [("/x", "2")]),
                        Tx("R", 6, 7, ok, [("/x", "2")], [])], xy),
                ];
            default:
                throw new ArgumentOutOfRangeException(nameof(scenario), scenario, null);
        }
    }

    // =====================================================================================================================
    // HAR-013 FaultPointActuallyReached
    // =====================================================================================================================

    public static TheoryData<string, int> ReachableFaultPoints => new()
    {
        { FaultSites.ClientOperationStart, 1 },
        { FaultSites.ClientValidated, 1 },
        { FaultSites.ClientRequestSend, 1 },
        { FaultSites.ClientUploadChunk, 1 },
        { FaultSites.ClientUploadChunk, 3 },
        { FaultSites.ClientUploadChunk, 5 },
        { FaultSites.ClientReplyReceived, 1 },
        { FaultSites.RpcRequestAccepted, 1 },
        { FaultSites.CommitReceiptValidated, 1 },
        { FaultSites.ApplyAfterBeforeReply, 1 },
        { FaultSites.RpcReplySend, 1 },
        { FaultSites.ObjectWrite, 1 },
        { FaultSites.ObjectBeforeFlush, 1 },
        { FaultSites.ObjectAfterFlushBeforeReceipt, 1 },
        { FaultSites.RaftBeforePropose, 1 },
        { FaultSites.RaftAfterLocalAppend, 1 },
        { FaultSites.RaftAfterCommitBeforeApply, 1 },
    };

    [Theory]
    [MemberData(nameof(ReachableFaultPoints))]
    [Catalog("HAR-013", "U", "INV-20")]
    public Task HAR_013_FaultPointActuallyReached(string site, int occurrence) =>
        ClusterTest.RunAsync(Deterministic(), async cluster =>
        {
            CancellationToken ct = Ct;
            string path = $"/har013/{site.Replace('.', '-')}-{occurrence}";
            await using DfsClient client = await cluster.ConnectAsync(null, ct);
            DfsVolume vol = client.GetVolume(Vol);
            await vol.Directories.CreateDirectoryAsync("/har013", ct);
            TestNode leader = await cluster.GetLeaderAsync(Vol, ct);

            FaultStage stage = FaultSites.StageOf(site);
            bool clientSite = stage is FaultStage.F00 or FaultStage.F01 or FaultStage.F02 or FaultStage.F03 or FaultStage.F14;
            bool requestSite = site == FaultSites.RpcRequestAccepted || site == FaultSites.CommitReceiptValidated
                || site == FaultSites.ApplyAfterBeforeReply || site == FaultSites.RpcReplySend;
            FaultMatch match = clientSite
                ? new FaultMatch(site, PathPrefix: path, Occurrence: occurrence)
                : requestSite
                    ? new FaultMatch(site, Volume: Vol, Operation: DfsOperationKind.WriteAll, Occurrence: occurrence)
                    : new FaultMatch(site, Node: leader.Name, Volume: Vol, Occurrence: occurrence);
            FaultArm arm = cluster.Faults.Arm(match, FaultAction.Delay(TimeSpan.Zero));

            byte[] data = TestData.Bytes(1300, (4 * cluster.Options.Timings.ChunkSize) + 1); // five upload chunks
            await vol.Files.WriteAllBytesAsync(path, data, ct);
            await arm.WaitTriggeredAsync(TimeSpan.FromSeconds(30));

            cluster.Faults.AssertReached(arm);
            Assert.True(arm.Triggered);
            Assert.True(arm.Hits >= occurrence, $"{site}: {arm.Hits} matching hits but occurrence {occurrence} triggered");
            FaultContext hit = Assert.Single(arm.Contexts);
            Assert.Equal(site, hit.Site);
            Assert.Equal(stage, hit.Stage);
            if (clientSite)
            {
                Assert.StartsWith(path, hit.Path);
            }
            else if (requestSite)
            {
                Assert.Equal(Vol, hit.Volume);
                Assert.Equal(DfsOperationKind.WriteAll, hit.Operation);
            }
            else
            {
                Assert.Equal(leader.Name, hit.Node);
                Assert.Equal(Vol, hit.Volume);
            }

            Assert.True(cluster.Faults.Coverage.WasReached(site));
            Assert.True(cluster.Faults.Coverage.CountsBySite().TryGetValue(site, out int count) && count >= occurrence);
            await BytesEqualAsync(data, vol.Files.ReadAllBytesAsync(path, ct));
        }, Ct);

    [Fact]
    [Catalog("HAR-013", "U", "INV-20")]
    public Task HAR_013_FaultPointActuallyReached_UnreachedInjectionIsNeverCountedAsExercised() =>
        ClusterTest.RunAsync(Deterministic(), async cluster =>
        {
            CancellationToken ct = Ct;
            const string path = "/har013-unreached";
            await using DfsClient client = await cluster.ConnectAsync(null, ct);
            DfsVolume vol = client.GetVolume(Vol);

            FaultArm beyondLastChunk = cluster.Faults.Arm(new FaultMatch(FaultSites.ClientUploadChunk, PathPrefix: path, Occurrence: 99),
                FaultAction.Throw(() => new IOException("must never run")));
            FaultArm unusedStage = cluster.Faults.Arm(new FaultMatch(FaultSites.BackupBeforePointer, Volume: Vol), FaultAction.CrashNode());
            FaultArm? unknownSite = null;
            try
            {
                unknownSite = cluster.Faults.Arm(new FaultMatch("no.such.site"), FaultAction.Delay(TimeSpan.Zero));
            }
            catch (ArgumentException)
            {
                // Explicitly classified as invalid at arming time: acceptable.
            }

            byte[] data = TestData.Bytes(1301, (4 * cluster.Options.Timings.ChunkSize) + 1);
            await vol.Files.WriteAllBytesAsync(path, data, ct);
            await BytesEqualAsync(data, vol.Files.ReadAllBytesAsync(path, ct), "the unreached Throw action never ran");

            // The site was reached, but not at the configured occurrence: the injection was not exercised.
            Assert.True(cluster.Faults.Coverage.WasReached(FaultSites.ClientUploadChunk));
            Assert.InRange(beyondLastChunk.Hits, 1, 98);
            Assert.False(beyondLastChunk.Triggered);
            Assert.Empty(beyondLastChunk.Contexts);
            Assert.ThrowsAny<XunitException>(() => cluster.Faults.AssertReached(beyondLastChunk));

            // A stage the scenario never enters.
            Assert.False(unusedStage.Triggered);
            Assert.Equal(0, unusedStage.Hits);
            Assert.False(cluster.Faults.Coverage.WasReached(FaultSites.BackupBeforePointer));
            Assert.ThrowsAny<XunitException>(() => cluster.Faults.AssertReached(unusedStage));
            Exception? waited = await Record.ExceptionAsync(() => unusedStage.WaitTriggeredAsync(TimeSpan.FromSeconds(2)));
            Assert.True(waited is TimeoutException or XunitException, $"waiting for an unreached fault must fail, got {waited?.GetType().Name ?? "success"}");

            if (unknownSite is not null)
            {
                Assert.False(unknownSite.Triggered);
                Assert.ThrowsAny<XunitException>(() => cluster.Faults.AssertReached(unknownSite));
            }

            int applicable = unknownSite is null ? 2 : 3;
            CampaignReport report = cluster.Faults.Coverage.CreateReport("HAR-013 unreached", generated: 3, applicable: applicable, excluded: 3 - applicable);
            Assert.Equal(3, report.Generated);
            Assert.Equal(applicable, report.Applicable);
            Assert.Equal(3 - applicable, report.Excluded);
            Assert.True(report.Reached < report.Applicable, "unreached injections must not be counted as reached");
            Assert.False(report.IsComplete);
        }, Ct);

    [Theory]
    [InlineData(1, 1, new[] { 1 })]
    [InlineData(3, 1, new[] { 3 })]
    [InlineData(2, 3, new[] { 2, 3, 4 })]
    [InlineData(5, 2, new[] { 5, 6 })]
    [Catalog("HAR-013", "U", "INV-20")]
    public void HAR_013_FaultPointActuallyReached_HitCountWindow(int occurrence, int count, int[] triggering)
    {
        var match = new FaultMatch(FaultSites.ObjectWrite, Occurrence: occurrence, Count: count);
        match.Validate();
        for (int hit = 1; hit <= 10; hit++)
            Assert.Equal(triggering.Contains(hit), match.TriggersOn(hit));
    }

    [Fact]
    [Catalog("HAR-013", "U", "INV-20")]
    public void HAR_013_FaultPointActuallyReached_EventIdentityMatching()
    {
        var context = new FaultContext(FaultStage.F06, FaultSites.ObjectWrite, "n1", Volume: "vol", Operation: DfsOperationKind.PutObject,
            Path: "/dir/file", Occurrence: 2);
        Assert.True(new FaultMatch(FaultSites.ObjectWrite).Matches(context));
        Assert.True(new FaultMatch("object.*").Matches(context));
        Assert.True(new FaultMatch(FaultSites.ObjectWrite, Node: "n1", Volume: "vol", Operation: DfsOperationKind.PutObject, PathPrefix: "/dir/").Matches(context));
        Assert.False(new FaultMatch(FaultSites.ObjectBeforeFlush).Matches(context));
        Assert.False(new FaultMatch("raft.*").Matches(context));
        Assert.False(new FaultMatch(FaultSites.ObjectWrite, Node: "n2").Matches(context));
        Assert.False(new FaultMatch(FaultSites.ObjectWrite, Volume: "other").Matches(context));
        Assert.False(new FaultMatch(FaultSites.ObjectWrite, Operation: DfsOperationKind.Append).Matches(context));
        Assert.False(new FaultMatch(FaultSites.ObjectWrite, PathPrefix: "/other").Matches(context));
        Assert.False(new FaultMatch(FaultSites.ObjectWrite, PathPrefix: "/").Matches(context with { Path = null }));

        Assert.Throws<ArgumentOutOfRangeException>(() => new FaultMatch(FaultSites.ObjectWrite, Occurrence: 0).Validate());
        Assert.Throws<ArgumentOutOfRangeException>(() => new FaultMatch(FaultSites.ObjectWrite, Count: 0).Validate());
        Assert.Throws<ArgumentException>(() => new FaultMatch(string.Empty).Validate());
        Assert.Throws<ArgumentException>(() => new FaultMatch("object.*.write").Validate());
    }

    public static TheoryData<string, FaultStage> DeclaredSiteStages => new()
    {
        { FaultSites.ClientOperationStart, FaultStage.F00 },
        { FaultSites.ClientValidated, FaultStage.F01 },
        { FaultSites.ClientConnect, FaultStage.F02 },
        { FaultSites.ClientRequestSend, FaultStage.F03 },
        { FaultSites.ClientUploadChunk, FaultStage.F03 },
        { FaultSites.RpcRequestAccepted, FaultStage.F04 },
        { FaultSites.StagingRegistered, FaultStage.F05 },
        { FaultSites.ObjectWrite, FaultStage.F06 },
        { FaultSites.ObjectBeforeFlush, FaultStage.F07 },
        { FaultSites.ObjectAfterFlushBeforeReceipt, FaultStage.F08 },
        { FaultSites.CommitReceiptValidated, FaultStage.F09 },
        { FaultSites.RaftBeforePropose, FaultStage.F10 },
        { FaultSites.RaftAfterLocalAppend, FaultStage.F10 },
        { FaultSites.RaftAfterCommitBeforeApply, FaultStage.F11 },
        { FaultSites.ApplyAfterBeforeReply, FaultStage.F12 },
        { FaultSites.RpcReplySend, FaultStage.F13 },
        { FaultSites.ClientReplyReceived, FaultStage.F14 },
        { FaultSites.CleanupRelease, FaultStage.F15 },
        { FaultSites.RecoveryJournalReplay, FaultStage.F16 },
        { FaultSites.RecoveryCheckpointInstall, FaultStage.F16 },
        { FaultSites.RaftSnapshotInstall, FaultStage.F16 },
        { FaultSites.SnapshotPin, FaultStage.F17 },
        { FaultSites.BackupInventory, FaultStage.F17 },
        { FaultSites.BackupObjectReceived, FaultStage.F18 },
        { FaultSites.BackupBeforeClosureVerify, FaultStage.F18 },
        { FaultSites.BackupBeforePointer, FaultStage.F19 },
        { FaultSites.BackupAfterPointer, FaultStage.F19 },
        { FaultSites.BackupAck, FaultStage.F19 },
        { FaultSites.GcSweep, FaultStage.F20 },
        { FaultSites.CompactionCopy, FaultStage.F20 },
        { FaultSites.CompactionPublish, FaultStage.F20 },
        { FaultSites.CompactionDelete, FaultStage.F20 },
        { FaultSites.ReplicaDelete, FaultStage.F20 },
        { FaultSites.Reconfiguration, FaultStage.F21 },
        { FaultSites.KeyRotation, FaultStage.F21 },
    };

    [Theory]
    [MemberData(nameof(DeclaredSiteStages))]
    [Catalog("HAR-013", "U", "INV-20")]
    public void HAR_013_FaultPointActuallyReached_SiteIdentityAndStage(string site, FaultStage stage)
    {
        Assert.Equal(stage, FaultSites.StageOf(site));
        Assert.Contains(site, FaultSites.All);
        Assert.Single(FaultSites.All, s => s == site);
        Assert.True(new FaultMatch(site).MatchesSite(site));
        Assert.All(FaultSites.All.Where(s => s != site), other => Assert.False(new FaultMatch(site).MatchesSite(other)));
        Assert.Throws<ArgumentException>(() => FaultSites.StageOf("no.such.site"));
        Assert.All(FaultSites.All, s => Assert.True(FaultSites.TryGetStage(s, out _), $"{s} has no declared stage"));
    }

    [Fact]
    [Catalog("HAR-013", "U", "INV-20")]
    public void HAR_013_FaultPointActuallyReached_CampaignAccounting()
    {
        var complete = new CampaignReport("c", Generated: 10, Applicable: 8, Reached: 8, Completed: 8, Passed: 8, Failed: 0, Excluded: 2,
            Notes: ["2 excluded: read-only operation has no metadata publication"]);
        Assert.True(complete.IsComplete);
        Assert.Equal(0, complete.Unreached);

        CampaignReport unreached = complete with { Reached = 7, Completed = 7, Passed = 7 };
        Assert.False(unreached.IsComplete);
        Assert.Equal(1, unreached.Unreached);
        Assert.False((complete with { Passed = 7, Failed = 1 }).IsComplete);
        Assert.False((complete with { Completed = 7, Passed = 7 }).IsComplete);
    }

    // =====================================================================================================================
    // HAR-014 TraceMinimizationPreservesFailure
    // =====================================================================================================================

    [Fact]
    [Catalog("HAR-014", "U", "INV-05", "INV-20")]
    public void HAR_014_TraceMinimizationPreservesFailure_ModelCounterexample()
    {
        ModelCheckResult result = ModelChecker.Explore(FenceState.Initial, s => s.Next(), s => s.Check(), s => s.Key, maxDepth: 12, maxStates: 100_000);
        Assert.NotNull(result.Violation);
        Assert.StartsWith(FenceState.Invariant, result.Violation);
        Assert.NotNull(result.CounterexampleTrace);
        IReadOnlyList<string> trace = result.CounterexampleTrace;

        // Save and reload the minimal trace (the replay artifact), then replay it: the same invariant fails.
        string saved = string.Join('\n', trace);
        string[] reloaded = saved.Split('\n');
        Assert.Equal(FenceState.Invariant, FenceState.Replay(reloaded, out IReadOnlyList<FenceState> states));
        Assert.All(states.Take(states.Count - 1), s => Assert.Null(s.Check())); // every strict prefix is still safe

        // Minimal: no counterexample exists within one step less, and no single step can be dropped.
        Assert.Null(ModelChecker.Explore(FenceState.Initial, s => s.Next(), s => s.Check(), s => s.Key, maxDepth: trace.Count - 1, maxStates: 100_000).Violation);
        for (int i = 0; i < trace.Count; i++)
            Assert.NotEqual(FenceState.Invariant, FenceState.Replay(trace.Where((_, j) => j != i).ToList(), out _));

        // A longer noisy failing schedule fails the same invariant; the minimal trace is strictly shorter.
        string[] noisy = ["A.noop", "A.acquire", "B.noop", "A.write", "expire", "A.noop", "B.acquire", "B.write", "A.write"];
        Assert.Equal(FenceState.Invariant, FenceState.Replay(noisy, out _));
        Assert.Equal(4, trace.Count);
        Assert.True(trace.Count < noisy.Length);
    }

    [Fact]
    [Catalog("HAR-014", "U", "INV-05", "INV-20")]
    public async Task HAR_014_TraceMinimizationPreservesFailure_Minimizer()
    {
        CancellationToken ct = Ct;
        string[] noisy = ["A.noop", "A.acquire", "B.noop", "A.write", "expire", "A.noop", "B.acquire", "B.write", "A.write"];
        int runs = 0;
        Task<string?> Replay(IReadOnlyList<string> candidate)
        {
            Interlocked.Increment(ref runs);
            return Task.FromResult(FenceState.Replay(candidate, out _));
        }

        TraceMinimizationResult<string> result = await TraceMinimizer.MinimizeAsync<string>(noisy, Replay, maxRuns: 500, ct);
        Assert.Equal(FenceState.Invariant, result.Invariant);
        Assert.Equal(noisy, result.Original);
        Assert.Equal(new[] { "A.acquire", "expire", "B.acquire", "A.write" }, result.Minimal);
        Assert.True(result.IsOneMinimal);
        Assert.False(result.BudgetExhausted);
        Assert.Equal(runs, result.Runs);
        Assert.True(IsSubsequence(result.Minimal, noisy));
        Assert.Equal(FenceState.Invariant, FenceState.Replay(result.Minimal, out _)); // replaying the saved minimal trace fails the same way

        // The same invariant is preserved even when a smaller trace would fail differently.
        static Task<string?> TwoInvariants(IReadOnlyList<string> t) =>
            Task.FromResult<string?>(t.Contains("x") && t.Contains("y") ? "I1" : t.Contains("x") ? "I2" : null);
        TraceMinimizationResult<string> same = await TraceMinimizer.MinimizeAsync<string>(["a", "x", "b", "y", "c"], TwoInvariants, 200, ct);
        Assert.Equal("I1", same.Invariant);
        Assert.Equal(new[] { "x", "y" }, same.Minimal);
        Assert.True(same.IsOneMinimal);

        // A passing trace cannot be "minimized", and an exhausted budget is reported, never presented as minimal.
        await Assert.ThrowsAsync<ArgumentException>(() => TraceMinimizer.MinimizeAsync<string>(["a"], TwoInvariants, 10, ct));
        TraceMinimizationResult<string> cut = await TraceMinimizer.MinimizeAsync<string>(noisy, Replay, maxRuns: 2, ct);
        Assert.True(cut.BudgetExhausted);
        Assert.False(cut.IsOneMinimal);
        Assert.True(cut.Runs <= 2);
        Assert.Equal(FenceState.Invariant, FenceState.Replay(cut.Minimal, out _));
    }

    [Fact]
    [Catalog("HAR-014", "U", "INV-06", "INV-20")]
    public async Task HAR_014_TraceMinimizationPreservesFailure()
    {
        const ulong seed = 0x1400_0000_0000_0014UL;
        string[] original = ["write:a:1", "append:0", "pause-follower", "append-lost-reply:1", "resume-follower", "delay:300"];

        // Negative control: with the dedup safety check removed the schedule fails INV-06; the correct system passes.
        Assert.Equal("INV-06", await RunDedupScheduleAsync(original, seed, dedupDisabled: true));
        Assert.Null(await RunDedupScheduleAsync(original, seed, dedupDisabled: false));

        TraceMinimizationResult<string> result = await TraceMinimizer.MinimizeAsync<string>(original,
            t => RunDedupScheduleAsync(t, seed, dedupDisabled: true), maxRuns: 24, Ct);
        Assert.Equal("INV-06", result.Invariant);
        Assert.False(result.BudgetExhausted);
        Assert.True(result.IsOneMinimal);
        Assert.True(result.Minimal.Count < original.Length);
        Assert.Contains("append-lost-reply:1", result.Minimal);
        Assert.True(IsSubsequence(result.Minimal, original));

        // Save the minimal trace as the replay artifact, reload it and replay from the same seed.
        string directory = Path.Combine(TestClusterOptions.ThreeZones.ResolveDataRoot(), "har014-" + seed.ToString("x16", System.Globalization.CultureInfo.InvariantCulture));
        Directory.CreateDirectory(directory);
        string file = Path.Combine(directory, "minimal-trace.txt");
        try
        {
            await File.WriteAllLinesAsync(file, result.Minimal, Ct);
            string[] reloaded = await File.ReadAllLinesAsync(file, Ct);
            Assert.Equal(result.Minimal, reloaded);
            Assert.Equal("INV-06", await RunDedupScheduleAsync(reloaded, seed, dedupDisabled: true));
            Assert.Equal("INV-06", await RunDedupScheduleAsync(reloaded, seed, dedupDisabled: true)); // deterministic replay
            Assert.Null(await RunDedupScheduleAsync(reloaded, seed, dedupDisabled: false));
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    private static bool IsSubsequence(IReadOnlyList<string> candidate, IReadOnlyList<string> sequence)
    {
        int i = 0;
        foreach (string item in sequence)
        {
            if (i < candidate.Count && string.Equals(candidate[i], item, StringComparison.Ordinal))
                i++;
        }

        return i == candidate.Count;
    }

    // =====================================================================================================================
    // HAR-015 RecoveryRequiresStableConditions
    // =====================================================================================================================

    [Theory]
    [InlineData(WithheldPrerequisite.QuorumCrashedMajority)]
    [InlineData(WithheldPrerequisite.QuorumPartitioned)]
    [InlineData(WithheldPrerequisite.Capacity)]
    [Catalog("HAR-015", "U", "INV-08", "INV-17", "INV-20")]
    public Task HAR_015_RecoveryRequiresStableConditions(WithheldPrerequisite withheld) =>
        ClusterTest.RunAsync(Deterministic(), async cluster =>
        {
            CancellationToken ct = Ct;
            const string path = "/har015";
            HistoryRecorder history = cluster.History;
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { OperationTimeout = TimeSpan.FromSeconds(3) }, ct);
            DfsVolume vol = client.GetVolume(Vol);
            byte[] v0 = TestData.Bytes(1500, 70 * 1024);
            await history.RecordAsync("c0", "write", path, v0, () => vol.Files.WriteAllBytesAsync(path, v0, ct));
            await cluster.WaitForReplicationAsync(Vol, ct);
            TestNode leader = await cluster.GetLeaderAsync(Vol, ct);
            TestNode[] followers = cluster.Nodes.Where(n => n.Name != leader.Name).ToArray();

            // Withhold the prerequisite far longer than leases, sessions and retry budgets.
            switch (withheld)
            {
                case WithheldPrerequisite.QuorumCrashedMajority:
                    foreach (TestNode follower in followers)
                        await follower.CrashAsync(CrashMode.LoseUnflushed);
                    break;
                case WithheldPrerequisite.QuorumPartitioned:
                    cluster.Network.Partition([leader.Name, "client:c0"], [followers[0].Name], [followers[1].Name]);
                    break;
                case WithheldPrerequisite.Capacity:
                    foreach (TestNode node in cluster.Nodes)
                        node.Disk.Faults.CapacityOverride = node.Disk.UsedBytes;
                    break;
            }

            int outageStart = history.Count;
            for (int i = 1; i <= 4; i++)
            {
                byte[] value = TestData.Bytes(1500 + (ulong)i, 70 * 1024);
                await RecordAllowingFailureAsync(history, "write", path, value, () => vol.Files.WriteAllBytesAsync(path, value, ct));
                await RecordAllowingFailureAsync(history, "read", path, null, async () => (byte[]?)await vol.Files.ReadAllBytesAsync(path, ct));
                await cluster.DelayAsync(TimeSpan.FromSeconds(30), ct);
            }

            List<HistoryOp> outage = history.Operations.Skip(outageStart).ToList();
            Assert.Equal(8, outage.Count);
            Assert.DoesNotContain(outage, o => o.Kind == "write" && o.Outcome == OpOutcome.Ok); // unavailability, never fabricated success
            if (withheld != WithheldPrerequisite.Capacity)
                Assert.DoesNotContain(outage, o => o.Outcome == OpOutcome.Ok); // no authoritative live read without a quorum
            Assert.All(outage.Where(o => o.Outcome != OpOutcome.Ok), o =>
                Assert.True(o.Output is Exception e && IsAvailabilityFailure(e), $"op #{o.Id} ({o.Kind}) failed dishonestly: {o.Output}"));
            LinearizabilityChecker.Check(history.Operations, new RegisterModel()).AssertValid(); // the oracle permits unavailability

            // Restore the stated prerequisites; only then assert liveness, with a bounded wait.
            switch (withheld)
            {
                case WithheldPrerequisite.QuorumCrashedMajority:
                    foreach (TestNode follower in followers)
                        await follower.RestartAsync();
                    break;
                case WithheldPrerequisite.QuorumPartitioned:
                    cluster.Network.Heal();
                    break;
                case WithheldPrerequisite.Capacity:
                    foreach (TestNode node in cluster.Nodes)
                        node.Disk.Faults.CapacityOverride = null;
                    break;
            }

            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), ct);
            byte[] final = TestData.Bytes(1599, 70 * 1024);
            await cluster.WaitUntilAsync(async () =>
            {
                try
                {
                    await history.RecordAsync("c0", "write", path, final, () => vol.Files.WriteAllBytesAsync(path, final, ct));
                    return true;
                }
                catch (Exception e) when (IsAvailabilityFailure(e))
                {
                    return false;
                }
            }, TimeSpan.FromSeconds(120), "all three voters run, are connected and have free capacity, so a write must eventually commit", ct);

            byte[] back = await history.RecordAsync("c0", "read", path, null, () => vol.Files.ReadAllBytesAsync(path, ct));
            AssertEx.BytesEqual(final, back);
            LinearizabilityChecker.Check(history.Operations, new RegisterModel()).AssertValid();
        }, Ct);

    private static async Task RecordAllowingFailureAsync<T>(HistoryRecorder history, string kind, string key, object? input, Func<Task<T>> action)
    {
        try
        {
            await history.RecordAsync("c0", kind, key, input, action);
        }
        catch (Exception)
        {
            // The outcome (failed or unknown) is in the history; it is asserted by the caller.
        }
    }

    // =====================================================================================================================
    // HAR-016 TestIsolation
    // =====================================================================================================================

    [Fact]
    [Catalog("HAR-016", "U", "INV-20")]
    public async Task HAR_016_TestIsolation_HarnessComponentsShareNoState()
    {
        CancellationToken ct = Ct;
        var first = new SimulatedMedium("n0", 1 << 20, new SeededRandom(16));
        var second = new SimulatedMedium("n0", 1 << 20, new SeededRandom(16)); // same name and seed, still independent
        await using (IStorageFile f = await first.OpenAsync("state", StorageOpenMode.CreateNew, ct))
        {
            await f.WriteFullyAsync(0, TestData.Bytes(1, 100), ct);
            await f.FlushAsync(ct);
        }

        await first.FlushDirectoryAsync("", ct);
        Assert.Equal(new[] { "state" }, first.AllFiles());
        Assert.Empty(second.AllFiles());
        Assert.False(await second.ExistsAsync("state", ct));

        SimulatedMedium clone = first.CloneDurable("n0-clone");
        await using (IStorageFile c = await clone.OpenAsync("clone-only", StorageOpenMode.CreateNew, ct))
            await c.FlushAsync(ct);
        await clone.FlushDirectoryAsync("", ct);
        Assert.DoesNotContain("clone-only", first.AllFiles());
        first.Crash(CrashMode.LoseUnflushed);
        AssertEx.BytesEqual(TestData.Bytes(1, 100), clone.ReadDurable("state"));
        Assert.Contains("clone-only", clone.AllFiles());

        var h1 = new HistoryRecorder();
        var h2 = new HistoryRecorder();
        Assert.Equal(1, h1.Invoke("p", "write", "k", null));
        Assert.Equal(0, h2.Count);
        Assert.Equal(1, h2.Invoke("p", "write", "k", null));

        TestClusterOptions options = TestClusterOptions.ThreeZones;
        Assert.Equal(options.ResolveSeed("Suite.TestA"), options.ResolveSeed("Suite.TestA"));
        Assert.NotEqual(options.ResolveSeed("Suite.TestA"), options.ResolveSeed("Suite.TestB"));
        Assert.Equal(42UL, (options with { Seed = 42 }).ResolveSeed("Suite.TestA"));
        string explicitRoot = Path.Combine(options.ResolveDataRoot(), "har016-explicit");
        Assert.Equal(explicitRoot, (options with { DataRoot = explicitRoot }).ResolveDataRoot());
        Assert.NotSame(TestClusterOptions.ThreeZones, TestClusterOptions.ThreeZones);
        _ = TestClusterOptions.ThreeZones with { NodeCount = 7 };
        Assert.Equal(3, TestClusterOptions.ThreeZones.NodeCount);
    }

    [Theory]
    [InlineData(ClusterMode.Deterministic, TransportKind.InMemory)]
    [InlineData(ClusterMode.Realtime, TransportKind.InMemory)]
    [InlineData(ClusterMode.Realtime, TransportKind.Quic)]
    [Catalog("HAR-016", "U", "INV-12", "INV-20")]
    public async Task HAR_016_TestIsolation(ClusterMode mode, TransportKind transport)
    {
        TestClusterOptions options = TestClusterOptions.ThreeZones with { Mode = mode, Transport = transport };
        const ulong seedA = 0x1600_0000_0000_00A1UL;
        const ulong seedB = 0x1600_0000_0000_00B2UL;

        IsolationProbe[] concurrent = await Task.WhenAll(
            ClusterTest.RunAsync<IsolationProbe>(options with { Seed = seedA }, ProbeIsolationAsync, Ct),
            ClusterTest.RunAsync<IsolationProbe>(options with { Seed = seedB }, ProbeIsolationAsync, Ct));
        IsolationProbe sequential = await ClusterTest.RunAsync<IsolationProbe>(options with { Seed = seedA }, ProbeIsolationAsync, Ct);

        foreach (IsolationProbe probe in concurrent.Append(sequential))
        {
            Assert.Empty(probe.RootEntriesAtStart); // nothing leaked in from another run, concurrent or earlier
            Assert.Equal(new[] { probe.Marker }, probe.RootEntriesAfterWrite);
            Assert.True(probe.MarkerReadBackExact);
        }

        IsolationProbe a = concurrent[0];
        IsolationProbe b = concurrent[1];
        Assert.NotEqual(a.ClusterId, b.ClusterId);
        Assert.Empty(a.NodeIds.Intersect(b.NodeIds, StringComparer.Ordinal));
        Assert.False(IsSameOrNestedDirectory(a.DataRoot, b.DataRoot), "concurrent clusters must not share or nest data directories");
        if (transport == TransportKind.Quic)
            Assert.Empty(a.Endpoints.Intersect(b.Endpoints, StringComparer.OrdinalIgnoreCase)); // no shared ports
        if (mode == ClusterMode.Deterministic)
            Assert.Equal(a.Digest, sequential.Digest); // same seed: no retained state and no cross-talk from the concurrent run
    }

    [Theory]
    [InlineData(TransportKind.InMemory)]
    [InlineData(TransportKind.Quic)]
    [Catalog("HAR-016", "U", "INV-12", "INV-20")]
    public async Task HAR_016_TestIsolation_IdentitiesDoNotCrossClusters(TransportKind transport)
    {
        CancellationToken ct = Ct;
        TestClusterOptions options = TestClusterOptions.ThreeZones with { Transport = transport };
        await using TestCluster a = await TestCluster.StartAsync(options with { Seed = 0x16A }, ct);
        await using TestCluster b = await TestCluster.StartAsync(options with { Seed = 0x16B }, ct);
        Assert.NotEqual(a.ClusterId, b.ClusterId);
        Assert.False(IsSameOrNestedDirectory(a.DataRoot, b.DataRoot));

        await AssertEx.ThrowsDfsAsync<DfsClusterIdentityException>(
            () => a.ConnectAsync(new TestClientOptions { ExpectedClusterId = b.ClusterId }, ct), DfsErrorCode.ClusterMismatch);
        await using (DfsClient own = await a.ConnectAsync(null, ct))
            Assert.Equal(a.ClusterId, own.Cluster.ClusterId);

        if (transport == TransportKind.Quic)
        {
            Assert.Empty(a.Nodes.Select(n => n.Endpoint).Intersect(b.Nodes.Select(n => n.Endpoint), StringComparer.OrdinalIgnoreCase));
            DfsClientOptions crossed = a.CreateClientOptions() with
            {
                SeedEndpoints = b.Nodes.Select(n => n.Endpoint).ToList(),
                ConnectTimeout = TimeSpan.FromSeconds(5),
            };
            Exception? failure = await Record.ExceptionAsync(() => DfsClient.ConnectAsync(crossed, ct));
            Assert.NotNull(failure); // a client of cluster A must never attach to cluster B
            Assert.True(failure is DfsClusterIdentityException or DfsAuthenticationException or DfsUnavailableException or DfsProtocolException,
                $"unexpected failure kind: {failure}");
        }
    }

    // =====================================================================================================================
    // HAR-017 UnfinishedChecksAreNotPasses
    // =====================================================================================================================

    [Fact]
    [Catalog("HAR-017", "U", "INV-20")]
    public void HAR_017_UnfinishedChecksAreNotPasses()
    {
        static IEnumerable<(string, (int A, int B))> Counters((int A, int B) s, int limit)
        {
            if (s.A < limit)
                yield return ("a++", (s.A + 1, s.B));
            if (s.B < limit)
                yield return ("b++", (s.A, s.B + 1));
        }

        static string Key((int A, int B) s) => $"{s.A},{s.B}";

        // State bound reached: no violation found, but not a pass.
        ModelCheckResult stateBound = ModelChecker.Explore((0, 0), s => Counters(s, 1000), _ => null, Key, maxDepth: 5000, maxStates: 100);
        Assert.False(stateBound.Completed);
        Assert.Null(stateBound.Violation);
        Assert.Contains(stateBound.Bounds, b => b.Contains("state bound", StringComparison.Ordinal));
        stateBound.AssertNoViolation();
        XunitException incomplete = Assert.ThrowsAny<XunitException>(() => stateBound.AssertCompleted());
        Assert.Contains("incomplete", incomplete.Message, StringComparison.OrdinalIgnoreCase);

        // Depth bound reached.
        ModelCheckResult depthBound = ModelChecker.Explore((0, 0), s => Counters(s, 1000), _ => null, Key, maxDepth: 5, maxStates: 1_000_000);
        Assert.False(depthBound.Completed);
        Assert.Equal(5, depthBound.MaxDepth);
        Assert.Contains(depthBound.Bounds, b => b.Contains("depth bound", StringComparison.Ordinal));
        Assert.ThrowsAny<XunitException>(() => depthBound.AssertCompleted());

        // The same system with a state space inside the bounds completes.
        ModelCheckResult complete = ModelChecker.Explore((0, 0), s => Counters(s, 3), _ => null, Key, maxDepth: 10, maxStates: 1000);
        Assert.True(complete.Completed);
        Assert.Equal(16, complete.StatesExplored);
        complete.AssertCompleted();
        complete.AssertNoViolation();
    }

    [Fact]
    [Catalog("HAR-017", "U", "INV-03", "INV-04", "INV-20")]
    public void HAR_017_UnfinishedChecksAreNotPasses_HistoryCheckerBudget()
    {
        var model = new RegisterModel();
        List<HistoryOp> legal = Enumerable.Range(0, 10)
            .Select(i => Op(i + 1, "p" + i, "write", "k", V("w" + i), 1 + i, 30 + i, OpOutcome.Ok)).ToList();
        legal.Add(Op(11, "r", "read", "k", null, 50, 51, OpOutcome.Ok, V("w3")));
        List<HistoryOp> illegal = [.. legal.Take(10), Op(11, "r", "read", "k", null, 50, 51, OpOutcome.Ok, V("never-written"))];

        // A checker cut off by its budget is inconclusive for legal and illegal histories alike, and never a pass.
        CheckResult cutLegal = LinearizabilityChecker.Check(legal, model, maxStates: 5);
        Assert.True(cutLegal.IsInconclusive);
        Assert.False(cutLegal.IsValid);
        XunitException inconclusive = Assert.ThrowsAny<XunitException>(() => cutLegal.AssertValid());
        Assert.Contains("inconclusive", inconclusive.Message, StringComparison.OrdinalIgnoreCase);
        CheckResult cutIllegal = LinearizabilityChecker.Check(illegal, model, maxStates: 5);
        Assert.True(cutIllegal.IsInconclusive);
        Assert.False(cutIllegal.IsValid);

        // With an adequate budget both get definite answers: the bound, not the history, made them inconclusive.
        LinearizabilityChecker.Check(legal, model).AssertValid();
        CheckResult refuted = LinearizabilityChecker.Check(illegal, model);
        Assert.False(refuted.IsValid);
        Assert.False(refuted.IsInconclusive);

        List<TxRecord> transactions = Enumerable.Range(0, 8)
            .Select(i => Tx("T" + i, 1 + i, 20 + i, OpOutcome.Ok, [], [("/x", "v" + i)])).ToList();
        transactions.Add(Tx("R", 40, 41, OpOutcome.Ok, [("/x", "v3")], []));
        CheckResult cutTx = SerializabilityChecker.CheckStrict(transactions, State(("/x", null)), maxStates: 5);
        Assert.True(cutTx.IsInconclusive);
        Assert.False(cutTx.IsValid);
        Assert.ThrowsAny<XunitException>(() => cutTx.AssertValid());
        SerializabilityChecker.CheckStrict(transactions, State(("/x", null))).AssertValid();

        // A campaign with an interrupted check is not complete.
        var report = new CampaignReport("HAR-017", Generated: 4, Applicable: 4, Reached: 4, Completed: 3, Passed: 3, Failed: 0, Excluded: 0,
            Notes: ["case 4: history check inconclusive (state budget)"]);
        Assert.False(report.IsComplete);
    }

    // =====================================================================================================================
    // HAR-018 AssumptionViolationIsLabelled
    // =====================================================================================================================

    [Theory]
    [InlineData(DishonestDevices.None)]
    [InlineData(DishonestDevices.One)]
    [InlineData(DishonestDevices.All)]
    [Catalog("HAR-018", "U", "INV-01", "INV-08", "INV-20")]
    public async Task HAR_018_AssumptionViolationIsLabelled(DishonestDevices dishonest)
    {
        const string path = "/har018";
        byte[] v0 = TestData.Bytes(1800, (64 * 1024) + 9);
        byte[] v1 = TestData.Bytes(1801, (64 * 1024) + 9);
        var liars = new List<string>();
        var recordedAtCrash = new List<AssumptionViolation>();
        byte[]? readBack = null;
        Exception? readError = null;
        CheckResult? historyCheck = null;

        Task run = ClusterTest.RunAsync(Deterministic(), async cluster =>
        {
            CancellationToken ct = Ct;
            await using (DfsClient writer = await cluster.ConnectAsync(null, ct))
            {
                DfsVolume vol = writer.GetVolume(Vol);
                await cluster.History.RecordAsync("c0", "write", path, v0, () => vol.Files.WriteAllBytesAsync(path, v0, ct));
                await cluster.WaitForReplicationAsync(Vol, ct);
                Assert.Empty(cluster.AssumptionViolations);

                IEnumerable<TestNode> devices = dishonest switch
                {
                    DishonestDevices.None => [],
                    DishonestDevices.One => [cluster[0]],
                    _ => cluster.Nodes,
                };
                foreach (TestNode node in devices)
                {
                    node.Disk.SetDishonestFlush(true);
                    Assert.True(node.Disk.IsDishonestFlush);
                    liars.Add(node.Name);
                }

                await cluster.History.RecordAsync("c0", "write", path, v1, () => vol.Files.WriteAllBytesAsync(path, v1, ct)); // acknowledged
                await cluster.WaitForReplicationAsync(Vol, ct);
            }

            foreach (TestNode node in cluster.Nodes)
                await node.CrashAsync(CrashMode.LoseUnflushed);
            foreach (TestNode node in cluster.Nodes)
                node.Disk.SetDishonestFlush(false);
            recordedAtCrash.AddRange(cluster.AssumptionViolations);
            foreach (TestNode node in cluster.Nodes)
                await node.RestartAsync();

            try
            {
                await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), ct);
                await using DfsClient reader = await cluster.ConnectAsync(new TestClientOptions { Name = "c1" }, ct);
                readBack = await cluster.History.RecordAsync("c1", "read", path, null, () => reader.GetVolume(Vol).Files.ReadAllBytesAsync(path, ct));
            }
            catch (Exception ex)
            {
                readError = ex;
            }

            historyCheck = LinearizabilityChecker.Check(cluster.History.Operations, new RegisterModel());
        }, Ct);

        if (dishonest == DishonestDevices.None)
        {
            await run; // honest devices: an ordinary, labelled-in-model pass
            Assert.Null(readError);
            Assert.NotNull(readBack);
            AssertEx.BytesEqual(v1, readBack, "the acknowledged revision survives a full power cut on honest devices");
            Assert.Empty(recordedAtCrash);
            Assert.NotNull(historyCheck);
            historyCheck.AssertValid();
            return;
        }

        AssumptionViolatedException labelled = await Assert.ThrowsAsync<AssumptionViolatedException>(() => run);
        Assert.NotEmpty(labelled.Violations);
        Assert.All(labelled.Violations, v => Assert.Equal(AssumptionViolation.DurableFlush, v.Assumption));
        Assert.Equal(liars.Order(StringComparer.Ordinal), labelled.Violations.Select(v => v.Node).Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal));
        Assert.NotEmpty(recordedAtCrash); // recorded when the bytes were lost, not inferred afterwards
        Assert.All(recordedAtCrash, v => Assert.Contains(v, labelled.Violations));

        // Even out of model nothing is invented: a previously written revision or an explicit error.
        if (readError is null)
        {
            Assert.NotNull(readBack);
            Assert.True(readBack.AsSpan().SequenceEqual(v0) || readBack.AsSpan().SequenceEqual(v1), "content that was never written was returned");
        }
        else
        {
            Assert.True(readError is FileNotFoundException or DfsIntegrityException || IsAvailabilityFailure(readError), $"dishonest failure: {readError}");
        }

        // The history checker may well flag the lost acknowledgement; the label is what classifies the run as out-of-model.
        if (dishonest == DishonestDevices.All && readBack is not null && readBack.AsSpan().SequenceEqual(v0))
        {
            Assert.NotNull(historyCheck);
            Assert.False(historyCheck.IsValid);
        }
    }
}
