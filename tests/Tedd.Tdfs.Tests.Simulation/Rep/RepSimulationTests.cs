using Tedd.Tdfs.Data;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;
using static Tedd.Tdfs.Tests.Simulation.Rep.RepHelpers;

namespace Tedd.Tdfs.Tests.Simulation.Rep;

/// <summary>
/// REP — replication policy, zones, and placement; deterministic-simulation rows (test plan §7.12; FSD §9, §10.3).
/// Invariants: INV-01, INV-08, INV-11, INV-18.
/// </summary>
public sealed class RepSimulationTests
{
    private static readonly DfsStoragePolicy AllThree = Policy(3, 3, 3, 3);

    // ------------------------------------------------------------------------------------------------------------
    // REP-003 AllCopiesAckBlocksOnMissingNode
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<string, string> AllCopiesCases()
    {
        var data = new TheoryData<string, string>();
        foreach (string offline in new[] { "Crash", "Isolate", "Pause" })
        {
            foreach (string operation in new[] { "WriteAll", "StreamFlush", "Append" })
                data.Add(offline, operation);
        }

        return data;
    }

    /// <summary>
    /// Policy acknowledge-three with exactly three eligible hosts: with one target crashed, isolated or paused, writes
    /// through every publication path fail with placement unavailability reporting honest counts (or, for a silent target,
    /// a timeout-class failure) — never succeed with two copies — leave the previous revision current, and succeed with
    /// three copies in three zones once the target returns.
    /// </summary>
    [Theory]
    [MemberData(nameof(AllCopiesCases))]
    [Catalog("REP-003", "S", "INV-01", "INV-08", "INV-11", "INV-18")]
    public async Task REP_003_AllCopiesAckBlocksOnMissingNode(string offline, string operation)
    {
        await ClusterTest.RunAsync(Deterministic(3, AllThree), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { SeedNodes = ["n0", "n1"], OperationTimeout = TimeSpan.FromSeconds(5) }, Ct);
            DfsVolume vol = client.GetVolume(Vol);
            byte[] v1 = TestData.Bytes(3_000, 2 * Chunk);
            await vol.Files.WriteAllBytesAsync("/a", v1, Ct);
            await AssertDurableAsync(cluster, "/a", 3, 3, "initial write with every target up");
            DfsFileInfo before = await vol.Files.GetInfoAsync("/a", Ct);
            IReadOnlyList<ObjectId> objectsBefore = await ObjectsOfAsync(cluster, "/a");

            TestNode n2 = cluster.Node("n2");
            switch (offline)
            {
                case "Crash": await n2.CrashAsync(); break;
                case "Isolate": cluster.Network.Isolate("n2"); break;
                case "Pause": await n2.PauseAsync(); break;
            }

            // A crashed target refuses connections; a paused or isolated one may simply stay silent until the SDK gives up.
            await AssertPlacementBlockedAsync(() => RunOperationAsync(vol, operation, 3_001), requiredCopies: 3, maxAchievedCopies: 2,
                requiredZones: 3, maxAchievedZones: 2, silentTarget: offline != "Crash", $"{offline}/{operation}");

            DfsFileInfo after = await vol.Files.GetInfoAsync("/a", Ct);
            Assert.Equal(before.Revision, after.Revision);
            Assert.Equal(before.Length, after.Length);
            Same(v1, await vol.Files.ReadAllBytesAsync("/a", Ct), $"{offline}/{operation}: previous revision");
            Assert.Equal(objectsBefore, await ObjectsOfAsync(cluster, "/a"));

            switch (offline)
            {
                case "Crash": await n2.RestartAsync(); break;
                case "Isolate": cluster.Network.Heal(); break;
                case "Pause": await n2.ResumeAsync(); break;
            }

            await cluster.WaitForStableAsync(ct: Ct);
            await RunOperationAsync(vol, operation, 3_002);
            await AssertDurableAsync(cluster, "/a", 3, 3, $"{offline}/{operation}: after the target returned");
        }, Ct);

        static async Task RunOperationAsync(DfsVolume vol, string operation, ulong seed)
        {
            byte[] data = TestData.Bytes(seed, 2 * Chunk);
            switch (operation)
            {
                case "WriteAll":
                    await vol.Files.WriteAllBytesAsync("/a", data, Ct);
                    break;
                case "Append":
                    await vol.Files.AppendAllBytesAsync("/a", data, Ct);
                    break;
                case "StreamFlush":
                {
                    DfsStream s = await vol.Files.OpenAsync("/a", FileMode.Open, FileAccess.Write, FileShare.Read, Ct);
                    try
                    {
                        await s.WriteAsync(data.AsMemory(), Ct);
                        await s.FlushAsync(Ct);
                    }
                    catch
                    {
                        // Always release the writer reservation, or the later reopen of "/a" would conflict with it.
                        await DiscardAsync(s);
                        throw;
                    }

                    await s.DisposeAsync();
                    break;
                }
                default:
                    throw new ArgumentOutOfRangeException(nameof(operation));
            }
        }
    }

    // ------------------------------------------------------------------------------------------------------------
    // REP-004 ZoneAcknowledgementCountEnforced
    // ------------------------------------------------------------------------------------------------------------

    private static TestClusterOptions TwoZoneLayout(bool fourNodes) => new()
    {
        Mode = ClusterMode.Deterministic,
        Topology = TestTopology.Custom,
        Nodes = fourNodes
            ? [new("n0", "z1", "hA"), new("n1", "z1", "hB"), new("n2", "z1", "hC"), new("n3", "z2", "hD")]
            : [new("n0", "z1", "hA"), new("n1", "z1", "hB"), new("n2", "z2", "hC")],
        Volumes = [VolumeWithVoters("n0", "n1", "n2")],
        DefaultPolicy = Policy(3, 2, 2, 2),
    };

    /// <summary>
    /// Policy acknowledge 2 copies in 2 zones: two (or three) durable acknowledgements from distinct hosts of one zone are
    /// not two failure domains. With the second zone offline the write fails with placement unavailability reporting one
    /// zone; with the second zone's flush held, success is withheld until that zone is durable.
    /// </summary>
    [Theory]
    [InlineData("SecondZoneOffline")]
    [InlineData("ThreeAcksOneZone")]
    [InlineData("SecondZoneFlushHeld")]
    [Catalog("REP-004", "S", "INV-01", "INV-08", "INV-11", "INV-18")]
    public async Task REP_004_ZoneAcknowledgementCountEnforced(string variant)
    {
        bool four = variant == "ThreeAcksOneZone";
        string zone2 = four ? "n3" : "n2";
        await ClusterTest.RunAsync(TwoZoneLayout(four), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { SeedNodes = ["n0", "n1"], OperationTimeout = TimeSpan.FromSeconds(5) }, Ct);
            DfsVolume vol = client.GetVolume(Vol);
            byte[] data = TestData.Bytes(4_000, 2 * Chunk);

            if (variant != "SecondZoneFlushHeld")
            {
                await cluster.Node(zone2).CrashAsync();
                DfsPlacementUnavailableException ex = await AssertEx.ThrowsDfsAsync<DfsPlacementUnavailableException>(
                    () => vol.Files.WriteAllBytesAsync("/z", data, Ct), DfsErrorCode.PlacementUnavailable);
                AssertPlacementCounts(ex, requiredCopies: 2, maxAchievedCopies: four ? 3 : 2, requiredZones: 2, maxAchievedZones: 1);
                Assert.False(await vol.Files.ExistsAsync("/z", Ct), "a write acknowledged by one zone only must not be published");
                await cluster.Node(zone2).RestartAsync();
                await cluster.WaitForStableAsync(ct: Ct);
                await vol.Files.WriteAllBytesAsync("/z", data, Ct);
                await AssertDurableAsync(cluster, "/z", 2, 2, "after the second zone returned");
                return;
            }

            FaultArm held = cluster.Faults.Arm(new FaultMatch(FaultSites.ObjectBeforeFlush, Node: zone2, Count: int.MaxValue), FaultAction.Block());
            Task<CommitToken> write = vol.Files.WriteAllBytesAsync("/z", data, Ct);
            await held.WaitTriggeredAsync(TimeSpan.FromSeconds(10));
            cluster.Faults.AssertReached(held);
            for (int i = 0; i < 10; i++)
            {
                await cluster.DelayAsync(TimeSpan.FromMilliseconds(200), Ct);
                Assert.False(write.IsCompletedSuccessfully, "success reported with durable copies in only one zone");
            }

            held.Disarm();
            held.Release();
            try
            {
                await write;
            }
            catch (Exception ex) when (ex is DfsPlacementUnavailableException or TimeoutException
                                       or DfsException { ErrorCode: DfsErrorCode.Timeout or DfsErrorCode.NodeUnavailable })
            {
                // The SDK may give up on the silent second zone before the hold is released; that is a definite failure.
                if (ex is DfsPlacementUnavailableException placement)
                    AssertPlacementCounts(placement, requiredCopies: 2, maxAchievedCopies: 2, requiredZones: 2, maxAchievedZones: 1);
                Assert.False(await vol.Files.ExistsAsync("/z", Ct) && (await vol.Files.GetInfoAsync("/z", Ct)).Length > 0,
                    "a write that failed with one zone durable must not have published");
                await vol.Files.WriteAllBytesAsync("/z", data, Ct);
            }

            await AssertDurableAsync(cluster, "/z", 2, 2, "after the second zone became durable");
            Same(data, await vol.Files.ReadAllBytesAsync("/z", Ct));
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // REP-010 TwoPlusOneZoneQuorumAsymmetry
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<int, string> AsymmetryCases() => new() { { 3, "z1" }, { 3, "z2" }, { 5, "z1" }, { 5, "z2" } };

    /// <summary>
    /// Metadata voters 2+1 (or 4+1) across two zones with a data policy satisfiable inside one zone. Losing the minority
    /// zone leaves a majority that keeps committing and serving live reads. Losing the majority zone leaves no authority:
    /// mutations and live reads fail with (non-placement) unavailability or a resolvable unknown outcome, no split brain
    /// appears, and after recovery only resolved-committed operations are visible.
    /// </summary>
    [Theory]
    [MemberData(nameof(AsymmetryCases))]
    [Catalog("REP-010", "S", "INV-01", "INV-08", "INV-11", "INV-18")]
    public async Task REP_010_TwoPlusOneZoneQuorumAsymmetry(int nodeCount, string lostZone)
    {
        var options = new TestClusterOptions
        {
            Mode = ClusterMode.Deterministic,
            NodeCount = nodeCount,
            Topology = TestTopology.TwoPlusOne,
            ControlVoters = nodeCount,
            Volumes = [VolumeWithVoters(Enumerable.Range(0, nodeCount).Select(i => $"n{i}").ToArray())],
            DefaultPolicy = Policy(3, 2, 2, 1),
        };
        IReadOnlyList<TestNodeSpec> specs = options.GetNodeSpecs();
        string[] lost = specs.Where(s => s.Zone == lostZone).Select(s => s.Name).ToArray();
        string[] survivors = specs.Where(s => s.Zone != lostZone).Select(s => s.Name).ToArray();
        bool majoritySurvives = survivors.Length * 2 > nodeCount;
        Assert.Equal(lostZone == "z2", majoritySurvives);

        await ClusterTest.RunAsync(options, async cluster =>
        {
            byte[] baseline = TestData.Bytes(10_000, 2 * Chunk);
            await using (DfsClient setup = await cluster.ConnectAsync(ct: Ct))
                await setup.GetVolume(Vol).Files.WriteAllBytesAsync("/base", baseline, Ct);
            await cluster.WaitForReplicationAsync(Vol, Ct);

            // Connect while the cluster is whole (discovery is not what REP-010 tests); the client then only knows the
            // survivors as seeds and must find whatever authority remains.
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "survivor", SeedNodes = survivors, OperationTimeout = TimeSpan.FromSeconds(3) }, Ct);
            DfsVolume vol = client.GetVolume(Vol);

            foreach (string n in lost)
                await cluster.Node(n).CrashAsync();
            byte[] during = TestData.Bytes(10_001, 2 * Chunk);
            var unknowns = new List<(DfsCommitOutcomeUnknownException Ex, string What)>();

            if (majoritySurvives)
            {
                await vol.Files.WriteAllBytesAsync("/during", during, Ct);
                await vol.Directories.CreateDirectoryAsync("/during-dir", Ct);
                Same(baseline, await vol.Files.ReadAllBytesAsync("/base", Ct), "majority side serves live reads");
            }
            else
            {
                DfsCommitOutcomeUnknownException? u1 = await AssertNoAuthorityAsync(() => vol.Files.WriteAllBytesAsync("/during", during, Ct), "minority write");
                DfsCommitOutcomeUnknownException? u2 = await AssertNoAuthorityAsync(() => vol.Directories.CreateDirectoryAsync("/during-dir", Ct), "minority mkdir");
                await AssertNoAuthorityAsync(() => vol.Files.ReadAllBytesAsync("/base", Ct), "minority live read");
                if (u1 is not null)
                    unknowns.Add((u1, "/during"));
                if (u2 is not null)
                    unknowns.Add((u2, "/during-dir"));
                for (int i = 0; i < 5; i++)
                {
                    await cluster.DelayAsync(TimeSpan.FromMilliseconds(400), Ct);
                    await cluster.Inspect.AssertNoSplitBrainAsync();
                }
            }

            foreach (string n in lost)
                await cluster.Node(n).RestartAsync();
            await cluster.WaitForStableAsync(ct: Ct);
            await cluster.Inspect.AssertNoSplitBrainAsync();

            await using DfsClient checker = await cluster.ConnectAsync(new TestClientOptions { Name = "checker" }, Ct);
            DfsVolume cv = checker.GetVolume(Vol);
            Same(baseline, await cv.Files.ReadAllBytesAsync("/base", Ct), "baseline survives");
            if (majoritySurvives)
            {
                Same(during, await cv.Files.ReadAllBytesAsync("/during", Ct), "majority-side write survives recovery");
                Assert.True(await cv.Directories.ExistsAsync("/during-dir", Ct));
                return;
            }

            bool writeCommitted = false, dirCommitted = false;
            foreach ((DfsCommitOutcomeUnknownException ex, string what) in unknowns)
            {
                DfsOperationOutcome outcome = await cv.QueryOperationAsync(ex.OperationId, resolve: true, Ct);
                Assert.True(outcome.Status is DfsOutcomeStatus.Committed or DfsOutcomeStatus.Aborted, $"{what}: {outcome.Status}");
                if (what == "/during")
                    writeCommitted = outcome.Status == DfsOutcomeStatus.Committed;
                else
                    dirCommitted = outcome.Status == DfsOutcomeStatus.Committed;
            }

            Assert.Equal(writeCommitted, await cv.Files.ExistsAsync("/during", Ct));
            if (writeCommitted)
                Same(during, await cv.Files.ReadAllBytesAsync("/during", Ct), "a resolved-committed minority write is complete");
            Assert.Equal(dirCommitted, await cv.Directories.ExistsAsync("/during-dir", Ct));
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // REP-011 ThreeZoneSingleZoneLoss
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<string, string> ZoneLossCases()
    {
        var data = new TheoryData<string, string>();
        foreach (string zone in new[] { "z1", "z2", "z3" })
        {
            data.Add(zone, "Crash");
            data.Add(zone, "Isolate");
        }

        return data;
    }

    /// <summary>
    /// Three voters one per zone, acknowledge-two data. Losing any single zone (crash or isolation) keeps every
    /// acknowledged file readable — including one written immediately before the loss — and keeps ack-two writes and
    /// metadata working, while an acknowledge-three write fails with placement unavailability. Losing a second zone
    /// removes authority (no writes, no live reads). After restoration repair reaches three copies in three zones.
    /// </summary>
    [Theory]
    [MemberData(nameof(ZoneLossCases))]
    [Catalog("REP-011", "S", "INV-01", "INV-08", "INV-11", "INV-18")]
    public async Task REP_011_ThreeZoneSingleZoneLoss(string zone, string mode)
    {
        var options = Deterministic(3, Policy(3, 3, 2, 2)) with
        {
            Volumes = [new TestVolumeSpec(Vol) { NamedPolicies = new Dictionary<string, DfsStoragePolicy> { ["all3"] = AllThree } }],
        };
        string victim = "n" + (int.Parse(zone[1..], System.Globalization.CultureInfo.InvariantCulture) - 1);
        string[] survivors = new[] { "n0", "n1", "n2" }.Where(n => n != victim).ToArray();
        await ClusterTest.RunAsync(options, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { SeedNodes = survivors, OperationTimeout = TimeSpan.FromSeconds(3) }, Ct);
            DfsVolume vol = client.GetVolume(Vol);
            byte[] repaired = TestData.Bytes(11_000, 2 * Chunk);
            await vol.Files.WriteAllBytesAsync("/repaired", repaired, Ct);
            await cluster.WaitForRepairAsync(Vol, ct: Ct);
            byte[] fresh = TestData.Bytes(11_001, 2 * Chunk);
            await vol.Files.WriteAllBytesAsync("/fresh", fresh, Ct);
            await AssertDurableAsync(cluster, "/fresh", 2, 2, "acknowledged copies at success");

            if (mode == "Crash")
                await cluster.Node(victim).CrashAsync();
            else
                cluster.Network.Isolate(victim);

            Same(repaired, await vol.Files.ReadAllBytesAsync("/repaired", Ct), $"{zone} lost: fully repaired file");
            Same(fresh, await vol.Files.ReadAllBytesAsync("/fresh", Ct), $"{zone} lost: file acknowledged just before the loss");
            byte[] during = TestData.Bytes(11_002, 2 * Chunk);
            await vol.Files.WriteAllBytesAsync("/during", during, Ct);
            await AssertDurableAsync(cluster, "/during", 2, 2, $"{zone} lost: ack-two write");
            await vol.Directories.CreateDirectoryAsync("/meta-" + zone, Ct);
            await AssertPlacementBlockedAsync(() => vol.Files.WriteAllBytesAsync("/strong", during, new DfsMutationOptions { StoragePolicy = "all3" }, Ct),
                requiredCopies: 3, maxAchievedCopies: 2, requiredZones: 3, maxAchievedZones: 2, silentTarget: mode == "Isolate", $"{zone} {mode}: ack-three write");
            Assert.False(await vol.Files.ExistsAsync("/strong", Ct), "a rejected ack-three write must not publish");

            if (mode == "Crash")
            {
                string second = survivors[0];
                await cluster.Node(second).CrashAsync();
                await AssertNoAuthorityAsync(() => vol.Files.WriteAllBytesAsync("/two-zones-lost", during, Ct), "write with two zones lost");
                await AssertNoAuthorityAsync(() => vol.Files.ReadAllBytesAsync("/repaired", Ct), "live read with two zones lost");
                await cluster.Node(second).RestartAsync();
                await cluster.Node(victim).RestartAsync();
            }
            else
            {
                cluster.Network.Heal();
            }

            await cluster.WaitForStableAsync(ct: Ct);
            await cluster.WaitForRepairAsync(Vol, TimeSpan.FromSeconds(30), Ct);
            foreach (string path in new[] { "/repaired", "/fresh", "/during" })
                await AssertDurableAsync(cluster, path, 3, 3, $"{path} after zone {zone} returned");
            await vol.Files.WriteAllBytesAsync("/strong", during, new DfsMutationOptions { StoragePolicy = "all3" }, Ct);
            await AssertDurableAsync(cluster, "/strong", 3, 3, "ack-three write after restoration");
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // REP-012 ReceiptBindsObjectAndPayload
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<string, string> ReceiptBindingCases() => new()
    {
        { "Valid", "None" },
        { "ReceiptsForOtherObject", "PlacementUnavailable" },
        { "ChecksumMismatch", "PlacementUnavailable|Integrity" },
        { "LengthMismatch", "PlacementUnavailable|InvalidArgument|Integrity" },
        { "EncodingMismatch", "PlacementUnavailable|InvalidArgument|Unsupported|Integrity" },
        { "TamperedMac", "PlacementUnavailable|Integrity|AuthenticationFailed" },
        { "OneValidOneForeign", "PlacementUnavailable" },
        { "SameReceiptTwice", "PlacementUnavailable" },
    };

    /// <summary>
    /// Receipts are bound to object id, checksum, length, encoding and their MAC: a commit for object X offered receipts
    /// for Y, receipts whose bound checksum/length/encoding differs from the declared extent, tampered receipts, or one
    /// host's receipt twice, is rejected before proposal and nothing is published.
    /// </summary>
    [Theory]
    [MemberData(nameof(ReceiptBindingCases))]
    [Catalog("REP-012", "S", "INV-01", "INV-08", "INV-11", "INV-18")]
    public async Task REP_012_ReceiptBindsObjectAndPayload(string variant, string allowed)
    {
        await ClusterTest.RunAsync(Deterministic(3, Policy(3, 3, 2, 2)), async cluster =>
        {
            byte[] v1 = TestData.Bytes(12_000, 2 * Chunk);
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            DfsVolume vol = client.GetVolume(Vol);
            await vol.Files.WriteAllBytesAsync("/target", v1, Ct);
            DfsFileInfo before = await vol.Files.GetInfoAsync("/target", Ct);

            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            await using RawProtocolClient raw = await cluster.ConnectRawAsync(leader.Name, Ct);
            Assert.Equal(DfsErrorCode.None, await raw.AuthenticateAsync(cluster.AdminCredentials));
            await using RawStagedWriter writer = await raw.OpenStagedWriterAsync(Vol, "/target", FileMode.Open);

            var random = new SeededRandom(12_100);
            ObjectId x = ObjectId.New(random), y = ObjectId.New(random);
            byte[] a = TestData.Bytes(12_101, 5000), b = TestData.Bytes(12_102, 5000);
            RawReceipt rx0 = await PutAsync(writer, "n0", x, a), rx1 = await PutAsync(writer, "n1", x, a);
            RawReceipt ry0 = await PutAsync(writer, "n0", y, b), ry1 = await PutAsync(writer, "n1", y, b);
            Assert.Equal(ObjectChecksum.Compute(a), rx0.Checksum);

            RawExtentSpec ex = RawExtentSpec.FromReceipt(rx0, 0, a.Length);
            (RawExtentSpec extent, RawReceipt[] receipts) = variant switch
            {
                "Valid" => (ex, new[] { rx0, rx1 }),
                "ReceiptsForOtherObject" => (ex, new[] { ry0, ry1 }),
                "ChecksumMismatch" => (ex with { Checksum = ry0.Checksum }, new[] { rx0, rx1 }),
                "LengthMismatch" => (ex with { ObjectLength = a.Length + 1 }, new[] { rx0, rx1 }),
                "EncodingMismatch" => (ex with { Encoding = (byte)(rx0.Encoding ^ 1) }, new[] { rx0, rx1 }),
                "TamperedMac" => (ex, new[] { rx0.WithTamperedMac(), rx1.WithTamperedMac() }),
                "OneValidOneForeign" => (ex, new[] { rx0, ry1 }),
                "SameReceiptTwice" => (ex, new[] { rx0, rx0 }),
                _ => throw new ArgumentOutOfRangeException(nameof(variant)),
            };

            DfsErrorCode code = await writer.CommitExtentsAsync(a.Length, [extent], receipts);
            var set = allowed.Split('|').Select(c => Enum.Parse<DfsErrorCode>(c)).ToHashSet();
            Assert.True(set.Contains(code), $"{variant}: server answered {code}; allowed {allowed}");

            await cluster.WaitForReplicationAsync(Vol, Ct);
            DfsFileInfo after = await vol.Files.GetInfoAsync("/target", Ct);
            FileInspection? layout = await cluster.Inspect.GetFileAsync(Vol, "/target");
            Assert.NotNull(layout);
            if (code == DfsErrorCode.None)
            {
                Assert.True(after.Revision > before.Revision);
                Same(a, await vol.Files.ReadAllBytesAsync("/target", Ct), "valid receipts publish X");
                await AssertDurableAsync(cluster, "/target", 2, 2, "valid receipts");
            }
            else
            {
                Assert.Equal(before.Revision, after.Revision);
                Same(v1, await vol.Files.ReadAllBytesAsync("/target", Ct), $"{variant}: nothing published");
                Assert.DoesNotContain(layout.Extents, e => e.Object == x || e.Object == y);
            }
        }, Ct);

        static async Task<RawReceipt> PutAsync(RawStagedWriter writer, string node, ObjectId id, byte[] data)
        {
            RawPutResult put = await writer.PutObjectAsync(node, id, data);
            Assert.Equal(DfsErrorCode.None, put.Error);
            Assert.NotNull(put.Receipt);
            Assert.Equal(id, put.Receipt.Object);
            Assert.Equal(node, put.Receipt.NodeName);
            return put.Receipt;
        }
    }

    // ------------------------------------------------------------------------------------------------------------
    // REP-013 ReceiptBindsNodeAndEpoch
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Receipts are bound to the issuing node incarnation and placement epoch: a receipt from before an empty-disk
    /// restart, from a node relabelled into the other holder's zone, or from a removed node cannot satisfy the
    /// acknowledge-2-copies/2-zones policy; a receipt from before a same-disk restart counts only if the copy is verified
    /// to still exist. Fresh receipts under the current incarnations/epoch (re-issued by idempotent re-puts) then commit.
    /// </summary>
    [Theory]
    [InlineData("EmptyDiskNewIncarnation")]
    [InlineData("RestartSameDisk")]
    [InlineData("RelabeledIntoSameZone")]
    [InlineData("RemovedNode")]
    [Catalog("REP-013", "S", "INV-01", "INV-08", "INV-11", "INV-18")]
    public async Task REP_013_ReceiptBindsNodeAndEpoch(string variant)
    {
        await ClusterTest.RunAsync(Deterministic(4, Policy(3, 3, 2, 2)), async cluster =>
        {
            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            string[] others = cluster.Nodes.Where(n => n.Name != leader.Name).Select(n => n.Name).ToArray();
            string a = others[0], b = others[1], c = others[2];
            await using RawProtocolClient raw = await cluster.ConnectRawAsync(leader.Name, Ct);
            Assert.Equal(DfsErrorCode.None, await raw.AuthenticateAsync(cluster.AdminCredentials));
            await using RawStagedWriter writer = await raw.OpenStagedWriterAsync(Vol, "/epoch", FileMode.CreateNew);

            ObjectId x = ObjectId.New(new SeededRandom(13_000));
            byte[] payload = TestData.Bytes(13_001, 8000);
            RawReceipt ra = await PutAsync(writer, a, x, payload);
            RawReceipt rb = await PutAsync(writer, b, x, payload);
            RawExtentSpec extent = RawExtentSpec.FromReceipt(ra, 0, payload.Length);
            TestNode nb = cluster.Node(b);

            switch (variant)
            {
                case "EmptyDiskNewIncarnation":
                {
                    await nb.CrashAsync();
                    await nb.ReplaceDiskWithEmptyAsync();
                    await nb.RestartAsync();
                    await cluster.WaitForStableAsync(ct: Ct);
                    // The incarnation number of an empty replacement is not prescribed (its superblock is new); what matters
                    // is that b's pre-replacement receipt can no longer prove a copy b does not hold.
                    Assert.False(await nb.Inspect.HasObjectAsync(x), "the replaced disk no longer holds the object");
                    DfsErrorCode code = await writer.CommitExtentsAsync(payload.Length, [extent], [ra, rb]);
                    Assert.True(code is DfsErrorCode.PlacementUnavailable or DfsErrorCode.StaleIncarnation, $"receipt from a replaced disk: {code}");
                    break;
                }

                case "RestartSameDisk":
                {
                    await nb.CrashAsync(CrashMode.LoseUnflushed);
                    await nb.RestartAsync();
                    await cluster.WaitForStableAsync(ct: Ct);
                    Assert.True(nb.Incarnation > rb.Incarnation, "a same-disk restart must advance the incarnation (ARCHITECTURE §2.2)");
                    Assert.True(await nb.Inspect.HasObjectAsync(x), "a receipted object was flushed before the receipt and survives the crash");
                    DfsErrorCode code = await writer.CommitExtentsAsync(payload.Length, [extent], [ra, rb]);
                    Assert.True(code is DfsErrorCode.None or DfsErrorCode.PlacementUnavailable or DfsErrorCode.StaleIncarnation, $"stale-incarnation receipt: {code}");
                    if (code == DfsErrorCode.None)
                    {
                        IReadOnlyList<string> holders = await cluster.Inspect.GetObjectHoldersAsync(x);
                        Assert.Contains(a, holders);
                        Assert.Contains(b, holders);
                        await AssertDurableAsync(cluster, "/epoch", 2, 2, "accepted only with the copy re-verified");
                        return;
                    }

                    break;
                }
                case "RelabeledIntoSameZone":
                {
                    ulong epochBefore = (await cluster.Admin.GetClusterStatusAsync(Ct)).PlacementEpoch;
                    Assert.Equal(epochBefore, rb.PlacementEpoch);
                    IReadOnlyDictionary<string, DfsTopologyLabels> labels = await CurrentLabelsAsync(cluster.Admin);
                    await cluster.Admin.SetNodeLabelsAsync(b, labels[b] with { Zone = labels[a].Zone }, Ct);
                    await cluster.WaitUntilAsync(async () => (await cluster.Admin.GetClusterStatusAsync(Ct)).PlacementEpoch > epochBefore,
                        TimeSpan.FromSeconds(10), "relabelling must publish a new placement epoch", Ct);
                    Assert.Equal(DfsErrorCode.PlacementUnavailable, await writer.CommitExtentsAsync(payload.Length, [extent], [ra, rb]));
                    break;
                }
                case "RemovedNode":
                {
                    await nb.CrashAsync();
                    await cluster.Admin.RemoveNodeAsync(b, force: true, Ct);
                    await cluster.WaitUntilAsync(async () => (await cluster.Admin.ListNodesAsync(Ct)).All(n => n.Name != b || n.Status == DfsNodeStatus.Removed),
                        TimeSpan.FromSeconds(10), "the removal must be committed", Ct);
                    DfsErrorCode code = await writer.CommitExtentsAsync(payload.Length, [extent], [ra, rb]);
                    Assert.True(code is DfsErrorCode.PlacementUnavailable or DfsErrorCode.StaleIncarnation, $"receipt from a removed node: {code}");
                    break;
                }

                default:
                    throw new ArgumentOutOfRangeException(nameof(variant));
            }

            await using (DfsClient client = await cluster.ConnectAsync(ct: Ct))
            {
                DfsFileInfo rejected = await client.GetVolume(Vol).Files.GetInfoAsync("/epoch", Ct);
                Assert.Equal(0L, rejected.Length);
            }

            // Fresh receipts under the current incarnations and placement epoch (a's old receipt may itself be from an
            // older epoch now, so it is re-issued too): b's new incarnation after a same-disk restart, otherwise c.
            string second = variant == "RestartSameDisk" ? b : c;
            RawReceipt raFresh = await PutAsync(writer, a, x, payload);
            RawReceipt rf = await PutAsync(writer, second, x, payload);
            if (variant == "RestartSameDisk")
                Assert.True(rf.Incarnation > rb.Incarnation, "the re-issued receipt carries b's new incarnation");
            var receipts = new List<RawReceipt> { raFresh, rf };
            Assert.Equal(DfsErrorCode.None, await writer.CommitExtentsAsync(payload.Length, [RawExtentSpec.FromReceipt(raFresh, 0, payload.Length)], receipts));
            await AssertDurableAsync(cluster, "/epoch", 2, 2, $"{variant}: fresh receipts");
        }, Ct);

        static async Task<RawReceipt> PutAsync(RawStagedWriter writer, string node, ObjectId id, byte[] data)
        {
            RawPutResult put = await writer.PutObjectAsync(node, id, data);
            Assert.Equal(DfsErrorCode.None, put.Error);
            Assert.NotNull(put.Receipt);
            return put.Receipt;
        }
    }

    // ------------------------------------------------------------------------------------------------------------
    // REP-014 PolicyUpgradeWaitsForMigration
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Volume-wide upgrade from one copy to three copies in three zones while the third zone is offline: migration status
    /// never reports the new policy effective (pending objects remain) because the existing objects cannot meet it; after
    /// the zone returns migration completes, and once reported effective every object really has 3 copies in 3 zones.
    /// </summary>
    [Fact]
    [Catalog("REP-014", "S", "INV-01", "INV-08", "INV-11", "INV-18")]
    public async Task REP_014_PolicyUpgradeWaitsForMigration_VolumeDefault()
    {
        await ClusterTest.RunAsync(Deterministic(3, Policy(1, 1, 1, 1)), async cluster =>
        {
            await cluster.Node("n2").CrashAsync();
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { SeedNodes = ["n0", "n1"] }, Ct);
            DfsVolume vol = client.GetVolume(Vol);
            var files = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            for (int i = 0; i < 5; i++)
            {
                files[$"/u{i}"] = TestData.Bytes((ulong)(14_000 + i), 2 * Chunk);
                await vol.Files.WriteAllBytesAsync($"/u{i}", files[$"/u{i}"], Ct);
            }

            // The upgrade may be accepted as a pending migration, or refused/left unresolved while the third zone is down
            // (an admin call that waits for the migration may time out). Either way it must not become effective.
            bool accepted = true;
            try
            {
                await cluster.Admin.SetDefaultPolicyAsync(Vol, AllThree, migrateExisting: true, Ct);
            }
            catch (Exception ex) when (ex is DfsUnavailableException or TimeoutException or DfsCommitOutcomeUnknownException
                                       or DfsException { ErrorCode: DfsErrorCode.Timeout })
            {
                accepted = false;
                Log($"upgrade while a zone is offline reported {ex.GetType().Name}");
            }

            for (int i = 0; i < 12; i++)
            {
                DfsPolicyMigrationStatus status = await cluster.Admin.GetPolicyMigrationStatusAsync(Vol, Ct);
                // When the upgrade was refused the status describes the old (one-copy) policy, which is legitimately effective.
                Assert.False(status.Effective && status.Target == AllThree, "the stronger policy was advertised effective while no object can have three copies");
                if (accepted)
                {
                    Assert.Equal(AllThree, status.Target);
                    Assert.False(status.Effective, "a pending upgrade must not be reported effective");
                    Assert.True(status.PendingObjects > 0, "pending migration work must be reported");
                }

                await cluster.DelayAsync(TimeSpan.FromMilliseconds(250), Ct);
            }

            foreach ((string path, byte[] data) in files)
                Same(data, await vol.Files.ReadAllBytesAsync(path, Ct), $"{path} readable during migration");

            await cluster.Node("n2").RestartAsync();
            await cluster.WaitForStableAsync(ct: Ct);
            if (!accepted)
                await cluster.Admin.SetDefaultPolicyAsync(Vol, AllThree, migrateExisting: true, Ct);

            await cluster.WaitUntilAsync(async () =>
            {
                DfsPolicyMigrationStatus status = await cluster.Admin.GetPolicyMigrationStatusAsync(Vol, Ct);
                if (!status.Effective || status.Target != AllThree)
                    return false;
                foreach (string path in files.Keys)
                    await AssertDurableAsync(cluster, path, 3, 3, "policy reported effective");
                Assert.Equal(0, status.PendingObjects);
                return true;
            }, TimeSpan.FromSeconds(30), "migration must complete once the third zone is back", Ct);
        }, Ct);
    }

    /// <summary>
    /// Per-file upgrade from a one-copy policy to a three-copy/three-zone policy while one zone is offline: the file never
    /// reports the stronger policy before its objects meet it; after the zone returns it does, with three copies in three zones.
    /// </summary>
    [Fact]
    [Catalog("REP-014", "S", "INV-01", "INV-08", "INV-11", "INV-18")]
    public async Task REP_014_PolicyUpgradeWaitsForMigration_PerFile()
    {
        var options = Deterministic(3, Policy(3, 3, 2, 2)) with
        {
            Volumes = [new TestVolumeSpec(Vol) { NamedPolicies = new Dictionary<string, DfsStoragePolicy> { ["weak"] = Policy(1, 1, 1, 1), ["strong"] = AllThree } }],
        };
        await ClusterTest.RunAsync(options, async cluster =>
        {
            await cluster.Node("n2").CrashAsync();
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { SeedNodes = ["n0", "n1"], OperationTimeout = TimeSpan.FromSeconds(5) }, Ct);
            DfsVolume vol = client.GetVolume(Vol);
            byte[] data = TestData.Bytes(14_100, 3 * Chunk);
            await vol.Files.WriteAllBytesAsync("/f", data, new DfsMutationOptions { StoragePolicy = "weak" }, Ct);
            Assert.Equal("weak", (await vol.Files.GetInfoAsync("/f", Ct)).StoragePolicy);

            // Accepted as a pending per-file migration, or refused/unresolved while the zone is down; never effective early.
            bool rejected = false;
            try
            {
                await vol.Files.SetStoragePolicyAsync("/f", "strong", Ct);
            }
            catch (Exception ex) when (ex is DfsUnavailableException or TimeoutException or DfsCommitOutcomeUnknownException
                                       or DfsException { ErrorCode: DfsErrorCode.Timeout })
            {
                rejected = true;
                Log($"per-file upgrade while a zone is offline reported {ex.GetType().Name}");
            }

            for (int i = 0; i < 10; i++)
            {
                Assert.NotEqual("strong", (await vol.Files.GetInfoAsync("/f", Ct)).StoragePolicy);
                await cluster.DelayAsync(TimeSpan.FromMilliseconds(200), Ct);
            }

            Same(data, await vol.Files.ReadAllBytesAsync("/f", Ct), "contents during the pending upgrade");
            await cluster.Node("n2").RestartAsync();
            await cluster.WaitForStableAsync(ct: Ct);
            if (rejected)
                await vol.Files.SetStoragePolicyAsync("/f", "strong", Ct);

            await cluster.WaitUntilAsync(async () => (await vol.Files.GetInfoAsync("/f", Ct)).StoragePolicy == "strong",
                TimeSpan.FromSeconds(30), "the upgrade must become effective once the zone is back", Ct);
            await AssertDurableAsync(cluster, "/f", 3, 3, "advertised strong policy");
            Same(data, await vol.Files.ReadAllBytesAsync("/f", Ct));
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // REP-015 PolicyDowngradeRetainsSafeCopies
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Downgrading from 3 copies (ack 2/2) to 1 copy: until the downgrade is effective every object keeps at least the old
    /// acknowledged durability (2 copies in 2 zones); afterwards at least one verified copy always remains; excess copies
    /// are eventually deleted; a corrupt replica is never the one retained; a crash during replica deletion does not lose data.
    /// </summary>
    [Theory]
    [InlineData("Plain")]
    [InlineData("CorruptReplica")]
    [InlineData("CrashDuringDelete")]
    [Catalog("REP-015", "S", "INV-01", "INV-08", "INV-11", "INV-18")]
    public async Task REP_015_PolicyDowngradeRetainsSafeCopies(string variant)
    {
        await ClusterTest.RunAsync(Deterministic(3, Policy(3, 3, 2, 2)), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            DfsVolume vol = client.GetVolume(Vol);
            var files = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            for (int i = 0; i < 4; i++)
            {
                files[$"/d{i}"] = TestData.Bytes((ulong)(15_000 + i), 2 * Chunk);
                await vol.Files.WriteAllBytesAsync($"/d{i}", files[$"/d{i}"], Ct);
            }

            await cluster.WaitForRepairAsync(Vol, ct: Ct);
            var objects = new List<ObjectId>();
            foreach (string path in files.Keys)
            {
                await AssertDurableAsync(cluster, path, 3, 3, "before the downgrade");
                objects.AddRange(await ObjectsOfAsync(cluster, path));
            }

            if (variant == "CorruptReplica")
            {
                foreach (ObjectId o in objects)
                    await cluster.Node((await cluster.Inspect.GetObjectHoldersAsync(o))[0]).Disk.CorruptObjectAsync(o, offset: 17);
            }

            FaultArm? crash = variant == "CrashDuringDelete"
                ? cluster.Faults.Arm(new FaultMatch(FaultSites.ReplicaDelete), FaultAction.CrashNode())
                : null;

            await cluster.Admin.SetDefaultPolicyAsync(Vol, Policy(1, 1, 1, 1), migrateExisting: true, Ct);
            IReadOnlyDictionary<string, DfsTopologyLabels> labels = await CurrentLabelsAsync(cluster.Admin);
            bool converged = false;
            for (int step = 0; step < 300 && !converged; step++)
            {
                foreach (TestNode n in cluster.Nodes.Where(n => n.State == TestNodeState.Crashed).ToList())
                    await n.RestartAsync();

                // Read the holders first and the migration status afterwards: "effective" is monotonic, so a status that is
                // still not effective after the holder reads proves the holders were observed before the transition.
                // Holder counts only include running nodes, so copy-count checks apply only when no node was down meanwhile.
                bool allRunning = cluster.Nodes.All(n => n.State == TestNodeState.Running);
                var observed = new List<(ObjectId Object, IReadOnlyList<string> Holders)>();
                foreach (ObjectId o in objects)
                    observed.Add((o, await cluster.Inspect.GetObjectHoldersAsync(o)));
                DfsPolicyMigrationStatus status = await cluster.Admin.GetPolicyMigrationStatusAsync(Vol, Ct);
                allRunning &= cluster.Nodes.All(n => n.State == TestNodeState.Running);

                bool allSingle = true;
                foreach ((ObjectId o, IReadOnlyList<string> holders) in observed)
                {
                    if (!status.Effective && allRunning)
                    {
                        (int c, int z) = Domains(holders, labels);
                        Assert.True(c >= 2 && z >= 2, $"step {step}: object {o} dropped to {c} copies/{z} zones before the downgrade was effective");
                    }
                    else if (allRunning)
                    {
                        Assert.True(holders.Count >= 1, $"step {step}: object {o} has no verified copy left");
                    }

                    allSingle &= holders.Count == 1;
                }

                converged = status.Effective && allSingle && allRunning;
                if (!converged)
                    await cluster.DelayAsync(TimeSpan.FromMilliseconds(100), Ct);
            }

            Assert.True(converged, "the downgrade must become effective and excess copies must be deleted");
            if (crash is not null)
                cluster.Faults.AssertReached(crash);
            await using DfsClient reader = await cluster.ConnectAsync(new TestClientOptions { Name = "reader" }, Ct);
            foreach ((string path, byte[] data) in files)
                Same(data, await reader.GetVolume(Vol).Files.ReadAllBytesAsync(path, Ct), $"{variant}: {path} keeps a verified copy");
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // REP-016 RebalanceCopiesBeforeDelete
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Rebalancing (drain a node, or add a node that changes deterministic targets) under foreground reads and writes:
    /// every replica deletion is held at its fault point, and for each live object the copies that remain once all its
    /// pending deletions execute already meet the target (3 hosts in 3 zones); foreground operations all succeed with
    /// exact bytes; placement converges to the new targets.
    /// </summary>
    [Theory]
    [InlineData("Drain")]
    [InlineData("AddNode")]
    [Catalog("REP-016", "S", "INV-01", "INV-08", "INV-11", "INV-18")]
    public async Task REP_016_RebalanceCopiesBeforeDelete(string trigger)
    {
        await ClusterTest.RunAsync(Deterministic(4, Policy(3, 3, 2, 2)), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { SeedNodes = ["n1", "n2", "n3"] }, Ct);
            DfsVolume vol = client.GetVolume(Vol);
            var files = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            for (int i = 0; i < 6; i++)
            {
                files[$"/r{i}"] = TestData.Bytes((ulong)(16_000 + i), 2 * Chunk);
                await vol.Files.WriteAllBytesAsync($"/r{i}", files[$"/r{i}"], Ct);
            }

            await cluster.WaitForRepairAsync(Vol, ct: Ct);

            using var stop = new CancellationTokenSource();
            var errors = new List<string>();
            int foregroundOps = 0;
            Task foreground = ForegroundAsync();

            // Hold every replica deletion (not just the first: unblocked ones would race past the checks), let the rebalance
            // queue up its deletions, then check them jointly — after all pending deletions of an object execute, the copies
            // left must still meet the target. Then release them all.
            FaultArm arm = cluster.Faults.Arm(new FaultMatch(FaultSites.ReplicaDelete, Count: int.MaxValue), FaultAction.Block());
            if (trigger == "Drain")
                await cluster.Admin.DrainNodeAsync("n0", Ct);
            else
                await cluster.AddNodeAsync(new TestNodeSpec("n4", "z5"), ct: Ct);

            await arm.WaitTriggeredAsync(TimeSpan.FromSeconds(30));
            cluster.Faults.AssertReached(arm);
            await cluster.DelayAsync(TimeSpan.FromSeconds(2), Ct);

            IReadOnlyList<FaultContext> pending = arm.Contexts.ToList();
            Log($"{trigger}: {pending.Count} replica deletions held");
            VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Vol);
            IReadOnlySet<ObjectId> reachable = state.ReachableObjects();
            IReadOnlyDictionary<string, DfsTopologyLabels> labels = await CurrentLabelsAsync(cluster.Admin);
            Assert.All(pending, c => Assert.NotNull(c.Object));
            int checkedDeletes = 0;
            foreach (IGrouping<ObjectId, FaultContext> perObject in pending.GroupBy(c => c.Object!.Value))
            {
                if (!reachable.Contains(perObject.Key))
                    continue;
                var deleting = perObject.Select(c => c.Node).ToHashSet(StringComparer.Ordinal);
                var remaining = (await cluster.Inspect.GetObjectHoldersAsync(perObject.Key)).Where(h => !deleting.Contains(h)).ToList();
                (int copies, int zones) = Domains(remaining, labels);
                Assert.True(copies >= 3 && zones >= 3,
                    $"replica(s) of {perObject.Key} on [{string.Join(",", deleting)}] deleted while only [{string.Join(",", remaining)}] ({copies}/{zones}) hold verified copies");
                checkedDeletes++;
            }

            Assert.True(checkedDeletes > 0, "the rebalance must delete at least one superseded replica of a live object");
            arm.Disarm();
            arm.Release();

            await stop.CancelAsync();
            await foreground;
            Assert.Empty(errors);
            Assert.True(foregroundOps > 0, "foreground traffic must have run during the rebalance");

            await cluster.WaitForRepairAsync(Vol, TimeSpan.FromSeconds(30), Ct);
            if (trigger == "Drain")
            {
                await cluster.WaitUntilAsync(async () =>
                {
                    foreach (ObjectId o in await cluster.Inspect.GetAllStoredObjectsAsync(Vol))
                    {
                        if ((await cluster.Inspect.GetObjectHoldersAsync(o)).Contains("n0"))
                            return false;
                    }

                    return true;
                }, TimeSpan.FromSeconds(30), "the drained node must end up holding no replicas", Ct);
            }

            foreach ((string path, byte[] data) in files)
            {
                Same(data, await vol.Files.ReadAllBytesAsync(path, Ct), path);
                await AssertDurableAsync(cluster, path, 3, 3, $"{trigger}: after rebalance");
            }

            async Task ForegroundAsync()
            {
                int i = 0;
                while (!stop.IsCancellationRequested)
                {
                    try
                    {
                        byte[] data = TestData.Bytes((ulong)(16_100 + i), Chunk + i);
                        await vol.Files.WriteAllBytesAsync($"/fg{i}", data, Ct);
                        byte[] back = await vol.Files.ReadAllBytesAsync($"/fg{i}", Ct);
                        if (!back.AsSpan().SequenceEqual(data))
                            errors.Add($"/fg{i} read back different bytes");
                        string existing = $"/r{i % 6}";
                        if (!(await vol.Files.ReadAllBytesAsync(existing, Ct)).AsSpan().SequenceEqual(files[existing]))
                            errors.Add($"{existing} changed during rebalance");
                        foregroundOps++;
                    }
                    catch (Exception ex)
                    {
                        errors.Add($"foreground op {i}: {ex.GetType().Name}: {ex.Message}");
                    }

                    i++;
                    await cluster.DelayAsync(TimeSpan.FromMilliseconds(50), Ct);
                }
            }
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // REP-017 TopologyChangeDuringWrite
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Staged chunks were acknowledged by n0 and n1 while both were in zone z1; then both are relabelled into z2 before the
    /// flush (third zone's node offline). Neither the map pinned at staging (both z1) nor the current map (both z2) gives
    /// two zones; only a mixture (n0 judged by its new label, n1 by its old one) would. Both a raw commit offering such
    /// receipts and the SDK flush must fail with placement unavailability and publish nothing.
    /// </summary>
    [Fact]
    [Catalog("REP-017", "S", "INV-01", "INV-08", "INV-11", "INV-18")]
    public async Task REP_017_TopologyChangeDuringWrite_MixtureNeverSatisfies()
    {
        var options = new TestClusterOptions
        {
            Mode = ClusterMode.Deterministic,
            Topology = TestTopology.Custom,
            Nodes = [new("n0", "z1", "hA"), new("n1", "z1", "hB"), new("n2", "z3", "hC")],
            Volumes = [VolumeWithVoters("n0", "n1", "n2")],
            DefaultPolicy = Policy(3, 2, 2, 2),
        };
        await ClusterTest.RunAsync(options, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { SeedNodes = ["n0", "n1"], OperationTimeout = TimeSpan.FromSeconds(5) }, Ct);
            DfsVolume vol = client.GetVolume(Vol);
            byte[] v1 = TestData.Bytes(17_000, 2 * Chunk);
            await vol.Files.WriteAllBytesAsync("/t", v1, Ct);
            DfsFileInfo before = await vol.Files.GetInfoAsync("/t", Ct);
            await cluster.Node("n2").StopAsync();

            // Raw path: receipts for one object from n0 and n1 issued while both are in z1, offered after both moved to z2.
            // This exercises the leader's receipt validation directly, independent of when the SDK checks placement.
            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            await using RawProtocolClient raw = await cluster.ConnectRawAsync(leader.Name, Ct);
            Assert.Equal(DfsErrorCode.None, await raw.AuthenticateAsync(cluster.AdminCredentials));
            await using RawStagedWriter writer = await raw.OpenStagedWriterAsync(Vol, "/raw", FileMode.CreateNew);
            ObjectId x = ObjectId.New(new SeededRandom(17_050));
            byte[] payload = TestData.Bytes(17_051, 6000);
            var staged = new List<RawReceipt>();
            foreach (string node in new[] { "n0", "n1" })
            {
                RawPutResult put = await writer.PutObjectAsync(node, x, payload);
                Assert.Equal(DfsErrorCode.None, put.Error);
                Assert.NotNull(put.Receipt);
                staged.Add(put.Receipt);
            }

            // SDK path: chunks staged through a stream before the relabel. An SDK may already refuse them at upload time
            // (both holders are in z1 under the map current then); that is a legal, earlier form of the same rejection.
            cluster.Network.Log.Reset();
            DfsStream s = await vol.Files.OpenAsync("/t", FileMode.Open, FileAccess.Write, FileShare.Read, Ct);
            DfsPlacementUnavailableException? sdkFailure = null;
            try
            {
                await s.WriteAsync(TestData.Bytes(17_001, 3 * Chunk).AsMemory(), Ct);
                await cluster.WaitUntilAsync(() => Task.FromResult(cluster.Network.Log.CountMessages(new MessageFilter(Type: MessageType.PutObject)) > 0),
                    TimeSpan.FromSeconds(10), "staged chunks must be uploaded before the topology change", Ct);
            }
            catch (DfsPlacementUnavailableException early)
            {
                sdkFailure = early;
            }

            IReadOnlyDictionary<string, DfsTopologyLabels> labels = await CurrentLabelsAsync(cluster.Admin);
            await cluster.Admin.SetNodeLabelsAsync("n0", labels["n0"] with { Zone = "z2" }, Ct);
            await cluster.Admin.SetNodeLabelsAsync("n1", labels["n1"] with { Zone = "z2" }, Ct);

            Assert.Equal(DfsErrorCode.PlacementUnavailable,
                await writer.CommitExtentsAsync(payload.Length, [RawExtentSpec.FromReceipt(staged[0], 0, payload.Length)], staged));
            Assert.Equal(0L, (await vol.Files.GetInfoAsync("/raw", Ct)).Length);

            sdkFailure ??= await AssertEx.ThrowsDfsAsync<DfsPlacementUnavailableException>(() => s.FlushAsync(Ct), DfsErrorCode.PlacementUnavailable);
            Assert.Equal(2, sdkFailure.RequiredZones);
            Assert.True(sdkFailure.AchievedZones <= 1, $"reported {sdkFailure.AchievedZones} zones; neither the pinned nor the current map gives more than one");
            await DiscardAsync(s);

            DfsFileInfo after = await vol.Files.GetInfoAsync("/t", Ct);
            Assert.Equal(before.Revision, after.Revision);
            Same(v1, await vol.Files.ReadAllBytesAsync("/t", Ct), "nothing published");

            await cluster.Node("n2").RestartAsync();
            await cluster.WaitForStableAsync(ct: Ct);
            byte[] v2 = TestData.Bytes(17_002, 2 * Chunk);
            await vol.Files.WriteAllBytesAsync("/t", v2, Ct);
            await AssertDurableAsync(cluster, "/t", 2, 2, "after the third zone returned (current labels)");
        }, Ct);
    }

    /// <summary>
    /// A zone relabel during staging on a four-zone cluster: the flush may fail with placement unavailability or succeed;
    /// if it succeeds, the acknowledged holders satisfy acknowledge-2/2 under one consistent map (the pinned old map or the
    /// current one), and after repair the file meets its full target under the current map with no placement violations.
    /// </summary>
    [Fact]
    [Catalog("REP-017", "S", "INV-01", "INV-08", "INV-11", "INV-18")]
    public async Task REP_017_TopologyChangeDuringWrite_PinnedOrMigratedPolicy()
    {
        await ClusterTest.RunAsync(Deterministic(4, Policy(3, 3, 2, 2)), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            DfsVolume vol = client.GetVolume(Vol);
            IReadOnlyDictionary<string, DfsTopologyLabels> oldLabels = await CurrentLabelsAsync(cluster.Admin);
            byte[] data = TestData.Bytes(17_100, 3 * Chunk);

            cluster.Network.Log.Reset();
            DfsStream s = await vol.Files.OpenAsync("/p", FileMode.CreateNew, FileAccess.Write, FileShare.Read, Ct);
            await s.WriteAsync(data.AsMemory(), Ct);
            await cluster.WaitUntilAsync(() => Task.FromResult(cluster.Network.Log.CountMessages(new MessageFilter(Type: MessageType.PutObject)) > 0),
                TimeSpan.FromSeconds(10), "staged chunks must be uploaded before the topology change", Ct);
            await cluster.Admin.SetNodeLabelsAsync("n1", oldLabels["n1"] with { Zone = oldLabels["n0"].Zone }, Ct);
            try
            {
                await s.FlushAsync(Ct);
                await s.DisposeAsync();
            }
            catch (DfsPlacementUnavailableException placement)
            {
                // Rejecting the staged receipts under the migrated map is a legal outcome; the counts must be honest.
                Assert.True(placement.AchievedCopies < placement.RequiredCopies || placement.AchievedZones < placement.RequiredZones,
                    $"placement failure without a deficit ({placement.AchievedCopies}/{placement.AchievedZones})");
                await DiscardAsync(s);
            }

            if (!await vol.Files.ExistsAsync("/p", Ct) || (await vol.Files.GetInfoAsync("/p", Ct)).Length != data.Length)
                await vol.Files.WriteAllBytesAsync("/p", data, Ct);

            IReadOnlyDictionary<string, DfsTopologyLabels> newLabels = await CurrentLabelsAsync(cluster.Admin);
            foreach (ObjectId o in await ObjectsOfAsync(cluster, "/p"))
            {
                IReadOnlyList<string> holders = await cluster.Inspect.GetObjectHoldersAsync(o);
                (int cOld, int zOld) = Domains(holders, oldLabels);
                (int cNew, int zNew) = Domains(holders, newLabels);
                Assert.True((cNew >= 2 && zNew >= 2) || (cOld >= 2 && zOld >= 2),
                    $"object {o} on [{string.Join(",", holders)}] satisfies ack 2/2 under neither the old ({cOld}/{zOld}) nor the current ({cNew}/{zNew}) map");
            }

            await cluster.WaitForRepairAsync(Vol, TimeSpan.FromSeconds(30), Ct);
            await AssertDurableAsync(cluster, "/p", 3, 3, "full target under the current map");
            Assert.Equal(0, (await cluster.Admin.GetReplicaHealthAsync(Vol, Ct)).PlacementViolations);
            Same(data, await vol.Files.ReadAllBytesAsync("/p", Ct));
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // REP-018 FixedTargetBarrierDoesNotShrink
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// A barrier on a named backup target is fixed to that target: with b0 disconnected, awaiting b0 times out naming b0
    /// even though b1 has published a covering checkpoint; removing b0 while a waiter is pending fails the waiter instead
    /// of satisfying it from the remaining target set.
    /// </summary>
    [Fact]
    [Catalog("REP-018", "S", "INV-01", "INV-08", "INV-11", "INV-18")]
    public async Task REP_018_FixedTargetBarrierDoesNotShrink_NamedBackupTarget()
    {
        await ClusterTest.RunAsync(Deterministic() with { BackupNodeCount = 2 }, async cluster =>
        {
            TestBackupNode b0 = cluster.BackupNodes[0], b1 = cluster.BackupNodes[1];
            await b0.RegisterAsync(Vol);
            await b1.RegisterAsync(Vol);
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            DfsVolume vol = client.GetVolume(Vol);

            // b0 is disconnected before the commit exists, so no schedule can have shipped a covering checkpoint to it.
            cluster.Network.Isolate(b0.Name);
            CommitToken token = await vol.Files.WriteAllBytesAsync("/barrier", TestData.Bytes(18_000, 2 * Chunk), Ct);
            await vol.Backups.RequestSyncAsync(b1.TargetName, ct: Ct);
            DfsBackupCheckpoint fromB1 = await vol.Backups.AwaitBackupAsync(b1.TargetName, token, TimeSpan.FromSeconds(30), Ct);
            Assert.Equal(b1.TargetName, fromB1.Target);
            Assert.True(fromB1.CoveredCommit.IsAtOrAfter(token));

            DfsBackupNotReachedException notReached = await AssertEx.ThrowsDfsAsync<DfsBackupNotReachedException>(
                () => vol.Backups.AwaitBackupAsync(b0.TargetName, token, TimeSpan.FromSeconds(3), Ct));
            Assert.Equal(b0.TargetName, notReached.Target);
            Assert.True(notReached.LastPublished is null || (notReached.LastPublished.Target == b0.TargetName && !notReached.LastPublished.CoveredCommit.IsAtOrAfter(token)),
                "the timeout must describe b0's own progress, never b1's");

            Task<DfsBackupCheckpoint> pending = vol.Backups.AwaitBackupAsync(b0.TargetName, token, TimeSpan.FromSeconds(20), Ct);
            await cluster.DelayAsync(TimeSpan.FromMilliseconds(500), Ct);
            await vol.Backups.RemoveTargetAsync(b0.TargetName, Ct);
            Exception? failure = null;
            try
            {
                await pending;
            }
            catch (Exception ex)
            {
                failure = ex;
            }

            Assert.NotNull(failure);
            Assert.True(failure is DfsBackupNotReachedException || failure is DfsException { ErrorCode: DfsErrorCode.BackupTargetNotFound },
                $"a barrier on a removed target must fail, got {failure.GetType().Name}");
            Assert.DoesNotContain(b0.TargetName, await vol.Backups.ListTargetsAsync(Ct));
        }, Ct);
    }

    /// <summary>
    /// An acknowledge-all-three policy is a fixed contract. The publication is waiting for its third target (n2's object
    /// flush is held, so n2 has issued no receipt), then n2 is disconnected or removed from membership: the wait fails
    /// with placement unavailability (or, for a silent disconnected target, a timeout-class failure) and nothing is
    /// published — it never completes with the two remaining copies. If the removal itself is refused, n2 stays a
    /// required target and the publication completes with three copies once its flush is released.
    /// </summary>
    [Theory]
    [InlineData("Disconnect")]
    [InlineData("Remove")]
    [Catalog("REP-018", "S", "INV-01", "INV-08", "INV-11", "INV-18")]
    public async Task REP_018_FixedTargetBarrierDoesNotShrink_AllCopiesPolicy(string how)
    {
        await ClusterTest.RunAsync(Deterministic(3, AllThree), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { SeedNodes = ["n0", "n1"], OperationTimeout = TimeSpan.FromSeconds(5) }, Ct);
            DfsVolume vol = client.GetVolume(Vol);
            byte[] v1 = TestData.Bytes(18_100, 2 * Chunk);
            await vol.Files.WriteAllBytesAsync("/all", v1, Ct);
            DfsFileInfo before = await vol.Files.GetInfoAsync("/all", Ct);

            // Hold n2's object flushes so the barrier is genuinely waiting on n2 (no n2 receipt exists) when n2 goes away;
            // a receipt n2 had already issued would legitimately keep counting after a later disconnect.
            FaultArm held = cluster.Faults.Arm(new FaultMatch(FaultSites.ObjectBeforeFlush, Node: "n2", Count: int.MaxValue), FaultAction.Block());
            DfsStream s = await vol.Files.OpenAsync("/all", FileMode.Open, FileAccess.Write, FileShare.Read, Ct);
            byte[] v2 = TestData.Bytes(18_101, 2 * Chunk);
            // Write and flush as one pending task: an SDK may already wait for staged-chunk receipts inside WriteAsync.
            Task flush = WriteAndFlushAsync(s, v2);
            await held.WaitTriggeredAsync(TimeSpan.FromSeconds(10));
            cluster.Faults.AssertReached(held);
            Assert.False(flush.IsCompletedSuccessfully, "the all-three publication succeeded while n2's copy was not durable");

            bool removalRefused = false;
            if (how == "Disconnect")
            {
                cluster.Network.Isolate("n2");
            }
            else
            {
                try
                {
                    await cluster.Admin.RemoveNodeAsync("n2", force: true, Ct);
                }
                catch (DfsException ex)
                {
                    removalRefused = true;
                    Log($"removal of a required target refused: {ex.ErrorCode}");
                }
            }

            // Let n2 finish its (now irrelevant or undeliverable) flush; for a removed node its receipt must not count.
            held.Disarm();
            held.Release();

            if (removalRefused)
            {
                await flush;
                await s.DisposeAsync();
                await AssertDurableAsync(cluster, "/all", 3, 3, "removal refused: the original target set still acknowledged");
                Same(v2, await vol.Files.ReadAllBytesAsync("/all", Ct));
                return;
            }

            await AssertPlacementBlockedAsync(() => flush, requiredCopies: 3, maxAchievedCopies: 2, requiredZones: 3, maxAchievedZones: 2,
                silentTarget: how == "Disconnect", $"{how}: all-three barrier");
            await DiscardAsync(s);
            Assert.Equal(before.Revision, (await vol.Files.GetInfoAsync("/all", Ct)).Revision);
            Same(v1, await vol.Files.ReadAllBytesAsync("/all", Ct), "the barrier did not shrink to the remaining nodes");
        }, Ct);

        static async Task WriteAndFlushAsync(DfsStream stream, byte[] bytes)
        {
            await stream.WriteAsync(bytes.AsMemory(), Ct);
            await stream.FlushAsync(Ct);
        }
    }

    /// <summary>
    /// The acknowledge-three contract does not expand either: a fourth node added (and kept disconnected) does not become a
    /// required acknowledgement; the write succeeds once three verified copies in three zones exist.
    /// </summary>
    [Fact]
    [Catalog("REP-018", "S", "INV-01", "INV-08", "INV-11", "INV-18")]
    public async Task REP_018_FixedTargetBarrierDoesNotShrink_DoesNotExpand()
    {
        await ClusterTest.RunAsync(Deterministic(3, AllThree), async cluster =>
        {
            await cluster.AddNodeAsync(new TestNodeSpec("n3", "z4"), ct: Ct);
            cluster.Network.Isolate("n3");
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { SeedNodes = ["n0", "n1", "n2"] }, Ct);
            DfsVolume vol = client.GetVolume(Vol);
            byte[] data = TestData.Bytes(18_200, 2 * Chunk);
            await vol.Files.WriteAllBytesAsync("/grow", data, Ct);
            foreach (ObjectId o in await ObjectsOfAsync(cluster, "/grow"))
            {
                IReadOnlyList<string> holders = await cluster.Inspect.GetObjectHoldersAsync(o);
                Assert.DoesNotContain("n3", holders);
                Assert.Equal(3, holders.Count);
            }

            await AssertDurableAsync(cluster, "/grow", 3, 3, "ack-three satisfied without the new node");
            Same(data, await vol.Files.ReadAllBytesAsync("/grow", Ct));
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // REP-019 UnrelatedNodeDoesNotBlockWrite
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<string, int> UnrelatedCases() => new() { { "Crash", 1 }, { "Crash", -1 }, { "Isolate", -1 } };

    /// <summary>
    /// Nine nodes; the volume's metadata voters are n0..n2. Nodes that are neither control voters nor this volume's voters
    /// (and host another volume's group) go offline: this volume keeps committing new files promptly with copies on
    /// healthy nodes, reads every file that still has a healthy copy, and reports (never fabricates) files whose copies
    /// are all offline; the other volume is genuinely unavailable until its nodes return.
    /// </summary>
    [Theory]
    [MemberData(nameof(UnrelatedCases))]
    [Catalog("REP-019", "S", "INV-01", "INV-08", "INV-11", "INV-18")]
    public async Task REP_019_UnrelatedNodeDoesNotBlockWrite(string mode, int count)
    {
        var options = new TestClusterOptions
        {
            Mode = ClusterMode.Deterministic,
            NodeCount = 9,
            ControlVoters = 3,
            Volumes = [VolumeWithVoters("n0", "n1", "n2")],
            DefaultPolicy = Policy(3, 3, 2, 2),
        };
        await ClusterTest.RunAsync(options, async cluster =>
        {
            var control = (await cluster.Inspect.GetControlGroupAsync()).Voters.ToHashSet(StringComparer.Ordinal);
            List<string> unrelated = cluster.Nodes.Select(n => n.Name).Where(n => !control.Contains(n) && n is not ("n0" or "n1" or "n2")).ToList();
            Assert.True(unrelated.Count >= 3, $"need at least three unrelated nodes, have {unrelated.Count}");
            string[] vol2Voters = unrelated.Take(3).ToArray();
            await cluster.Admin.CreateVolumeAsync(new DfsVolumeSpec
            {
                Name = "vol2",
                Metadata = new DfsMetadataPolicy { Voters = 3, Placement = DfsVoterPlacement.Explicit, ExplicitVoterNodes = vol2Voters },
                DefaultPolicy = Policy(3, 3, 2, 2),
            }, Ct);

            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { SeedNodes = ["n0", "n1", "n2"], OperationTimeout = TimeSpan.FromSeconds(5) }, Ct);
            DfsVolume vol = client.GetVolume(Vol), vol2 = client.GetVolume("vol2");
            var files = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            for (int i = 0; i < 8; i++)
            {
                files[$"/e{i}"] = TestData.Bytes((ulong)(19_000 + i), 2 * Chunk);
                await vol.Files.WriteAllBytesAsync($"/e{i}", files[$"/e{i}"], Ct);
            }

            byte[] other = TestData.Bytes(19_100, Chunk);
            await vol2.Files.WriteAllBytesAsync("/o", other, Ct);
            await cluster.WaitForRepairAsync(Vol, ct: Ct);

            List<string> offline = count < 0 ? unrelated : unrelated.Take(count).ToList();
            foreach (string n in offline)
            {
                if (mode == "Crash")
                    await cluster.Node(n).CrashAsync();
                else
                    cluster.Network.Isolate(n);
            }

            await vol.Directories.CreateDirectoryAsync("/meta", Ct);
            for (int i = 0; i < 4; i++)
            {
                byte[] data = TestData.Bytes((ulong)(19_200 + i), 2 * Chunk);
                long started = cluster.Time.GetTimestamp();
                await vol.Files.WriteAllBytesAsync($"/new{i}", data, Ct);
                TimeSpan took = cluster.Time.GetElapsedTime(started);
                Assert.True(took < TimeSpan.FromSeconds(5), $"write {i} took {took} with only unrelated nodes offline");
                await AssertDurableAsync(cluster, $"/new{i}", 2, 2, "new write with unrelated nodes offline");
                foreach (ObjectId o in await ObjectsOfAsync(cluster, $"/new{i}"))
                    Assert.Empty((await cluster.Inspect.GetObjectHoldersAsync(o)).Intersect(offline));
                Same(data, await vol.Files.ReadAllBytesAsync($"/new{i}", Ct));
            }

            foreach ((string path, byte[] data) in files)
            {
                bool everyObjectHealthy = true;
                foreach (ObjectId o in await ObjectsOfAsync(cluster, path))
                    everyObjectHealthy &= (await cluster.Inspect.GetObjectHoldersAsync(o)).Any(h => !offline.Contains(h));
                if (everyObjectHealthy)
                {
                    Same(data, await vol.Files.ReadAllBytesAsync(path, Ct), $"{path}: every object has a healthy copy");
                    continue;
                }

                try
                {
                    Same(data, await vol.Files.ReadAllBytesAsync(path, Ct), $"{path}: a successful read must be exact");
                }
                catch (Exception ex) when (ex is DfsUnavailableException or DfsIntegrityException or TimeoutException
                                           or DfsException { ErrorCode: DfsErrorCode.Timeout or DfsErrorCode.NodeUnavailable })
                {
                    Log($"{path}: all copies of some object are offline; read reported {ex.GetType().Name}");
                }
            }

            if (vol2Voters.All(offline.Contains))
                await AssertNoAuthorityAsync(() => vol2.Files.WriteAllBytesAsync("/o2", other, Ct), "vol2 with all its voters offline");

            foreach (string n in offline)
            {
                if (mode == "Crash")
                    await cluster.Node(n).RestartAsync();
            }

            cluster.Network.Heal();
            await cluster.WaitForStableAsync(ct: Ct);
            Same(other, await vol2.Files.ReadAllBytesAsync("/o", Ct), "vol2 recovers when its nodes return");
            await cluster.WaitForRepairAsync(Vol, TimeSpan.FromSeconds(30), Ct);
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // REP-020 RepairRestoresActualTarget
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// After an empty-disk replacement, a corrupted replica, or a permanently removed node with a spare available,
    /// repair restores the full target (3 verified copies on 3 hosts in 3 zones) — counted from verified contents, never
    /// from a reconnected endpoint. Without a spare (or with only a full spare) the deficit stays reported and is not
    /// papered over, while the unchanged acknowledgement policy still governs new writes.
    /// </summary>
    [Theory]
    [InlineData("EmptyDisk")]
    [InlineData("CorruptReplica")]
    [InlineData("NodeRemovedWithSpare")]
    [InlineData("NodeLostNoSpare")]
    [InlineData("SpareFull")]
    [Catalog("REP-020", "S", "INV-01", "INV-08", "INV-11", "INV-18")]
    public async Task REP_020_RepairRestoresActualTarget(string variant)
    {
        int nodeCount = variant == "NodeLostNoSpare" ? 3 : 4;
        await ClusterTest.RunAsync(Deterministic(nodeCount, Policy(3, 3, 2, 2)), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            DfsVolume vol = client.GetVolume(Vol);
            var files = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            for (int i = 0; i < 4; i++)
            {
                files[$"/x{i}"] = TestData.Bytes((ulong)(20_000 + i), 2 * Chunk);
                await vol.Files.WriteAllBytesAsync($"/x{i}", files[$"/x{i}"], Ct);
            }

            await cluster.WaitForRepairAsync(Vol, ct: Ct);
            var original = new Dictionary<ObjectId, IReadOnlyList<string>>();
            foreach (string path in files.Keys)
            {
                foreach (ObjectId o in await ObjectsOfAsync(cluster, path))
                    original[o] = await cluster.Inspect.GetObjectHoldersAsync(o);
            }

            ObjectId o0 = (await ObjectsOfAsync(cluster, "/x0"))[0];
            IReadOnlyList<string> holders0 = original[o0];
            Assert.Equal(3, holders0.Count);
            string victim = holders0[0];
            string? spare = cluster.Nodes.Select(n => n.Name).FirstOrDefault(n => !holders0.Contains(n));
            TestNode v = cluster.Node(victim);

            switch (variant)
            {
                case "EmptyDisk":
                    await v.CrashAsync();
                    await v.ReplaceDiskWithEmptyAsync();
                    await v.RestartAsync();
                    await cluster.WaitForStableAsync(ct: Ct);
                    foreach (ObjectId o in original.Keys)
                    {
                        if ((await cluster.Inspect.GetObjectHoldersAsync(o)).Contains(victim))
                            Assert.True(await v.Inspect.HasObjectAsync(o), $"the reconnected empty node is counted for {o} without holding it");
                    }

                    break;
                case "CorruptReplica":
                    await v.Disk.CorruptObjectAsync(o0, offset: 100);
                    await cluster.Admin.StartScrubAsync(victim, Ct);
                    await cluster.WaitUntilAsync(async () => (await v.GetCountersAsync()).ChecksumFailures > 0,
                        TimeSpan.FromSeconds(20), "scrub must detect the corrupted replica", Ct);
                    break;
                case "NodeRemovedWithSpare":
                case "NodeLostNoSpare":
                case "SpareFull":
                    if (variant == "SpareFull")
                        cluster.Node(spare!).Disk.Fill(leaveFreeBytes: 4L << 20);
                    await v.CrashAsync();
                    if (variant == "NodeRemovedWithSpare")
                        await cluster.Admin.RemoveNodeAsync(victim, force: true, Ct);
                    break;
            }

            if (variant is "NodeLostNoSpare" or "SpareFull")
            {
                await cluster.WaitUntilAsync(async () => (await cluster.Admin.GetReplicaHealthAsync(Vol, Ct)).UnderReplicatedObjects > 0,
                    TimeSpan.FromSeconds(20), "losing a replica holder must be reported as a deficit", Ct);
                // Give repair ample opportunity (Fast inventory/repair interval: 250 ms) to paper over the deficit.
                await cluster.DelayAsync(TimeSpan.FromSeconds(5), Ct);
                DfsReplicaHealth health = await cluster.Admin.GetReplicaHealthAsync(Vol, Ct);
                Assert.True(health.UnderReplicatedObjects > 0, "the deficit must stay visible while no legal capacity exists");
                foreach ((ObjectId o, IReadOnlyList<string> before) in original)
                {
                    IReadOnlyList<string> holders = await cluster.Inspect.GetObjectHoldersAsync(o);
                    Assert.DoesNotContain(victim, holders);
                    if (spare is null)
                        Assert.All(holders, h => Assert.Contains(h, before));
                    else if (!before.Contains(spare))
                        Assert.DoesNotContain(spare, holders);
                    (int c, _) = await cluster.Inspect.CountDurableDomainsAsync(o);
                    Assert.Equal(holders.Count, c);
                }

                Assert.Equal(2, (await cluster.Inspect.CountDurableDomainsAsync(o0)).Copies);
                byte[] data = TestData.Bytes(20_100, Chunk);
                await vol.Files.WriteAllBytesAsync("/during-deficit", data, Ct);
                await AssertDurableAsync(cluster, "/during-deficit", 2, 2, "ack policy unchanged during the deficit");
                foreach ((string path, byte[] content) in files)
                    Same(content, await vol.Files.ReadAllBytesAsync(path, Ct), path);
                return;
            }

            await cluster.WaitForRepairAsync(Vol, TimeSpan.FromSeconds(30), Ct);
            Assert.Equal(0, (await cluster.Admin.GetReplicaHealthAsync(Vol, Ct)).UnderReplicatedObjects);
            foreach ((string path, byte[] content) in files)
            {
                await AssertDurableAsync(cluster, path, 3, 3, $"{variant}: repaired");
                foreach (ObjectId o in await ObjectsOfAsync(cluster, path))
                {
                    var checksums = new HashSet<UInt128>();
                    foreach (string holder in await cluster.Inspect.GetObjectHoldersAsync(o))
                    {
                        Assert.True(await cluster.Node(holder).Inspect.HasObjectAsync(o), $"{holder} is counted for {o} but does not hold it");
                        checksums.Add((await cluster.Node(holder).Inspect.ListObjectsAsync(Vol)).Single(r => r.Id == o).Checksum);
                    }

                    Assert.Single(checksums);
                }

                await using DfsClient reader = await cluster.ConnectAsync(new TestClientOptions { Name = "reader-" + path.TrimStart('/') }, Ct);
                Same(content, await reader.GetVolume(Vol).Files.ReadAllBytesAsync(path, Ct), $"{variant}: {path}");
            }

            if (variant == "NodeRemovedWithSpare")
                Assert.Contains(spare!, await cluster.Inspect.GetObjectHoldersAsync(o0));
        }, Ct);
    }
}
