using Tedd.Tdfs.Data;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;
using static Tedd.Tdfs.Tests.Simulation.Pool.PoolHelpers;

namespace Tedd.Tdfs.Tests.Simulation.Pool;

/// <summary>
/// POOL — namespaces and storage pools; deterministic-simulation rows (FSD Addendum A §A2 POOL-02..POOL-05, catalog §A5.2).
/// Invariants: INV-08, INV-11, INV-12, INV-16, INV-18. Placement is judged from node inventories and verified holders,
/// during faults and migrations as well as after convergence.
/// </summary>
public sealed class PoolSimulationTests
{
    // ------------------------------------------------------------------------------------------------------------
    // POOL-005 ObjectsStayInPool
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Two volumes on disjoint pools under a concurrent mixed write load (WriteAll, flushed streams, appends, partial
    /// overwrites, copies, deletes): after every load round — and, in the second variant, while a pool node is crashed and
    /// after it returns and repair runs — every object (staged or committed) of each volume is stored only on its own
    /// pool's nodes, reachable objects meet the acknowledgement policy inside the pool, and after repair the full target
    /// inside the pool. The bytes match an independent model.
    /// </summary>
    [Theory]
    [InlineData("Steady")]
    [InlineData("CrashAndRepair")]
    [Catalog("POOL-005", "S", "INV-08", "INV-11")]
    public async Task POOL_005_ObjectsStayInPool(string variant)
    {
        IReadOnlyList<TestNodeSpec> nodes = TwoPools();
        string[] poolA = Members(nodes, PoolA);
        string[] poolB = Members(nodes, PoolB);
        await ClusterTest.RunAsync(Deterministic(nodes, 3, Volume(VolA, PoolA), Volume(VolB, PoolB)), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { OperationTimeout = TimeSpan.FromSeconds(10) }, Ct);
            VolumeLoad[] loads = [new(client.GetVolume(VolA), 5_000), new(client.GetVolume(VolB), 5_500)];

            for (int round = 0; round < 4; round++)
            {
                if (variant == "CrashAndRepair" && round == 1)
                {
                    // Round 0's objects first reach their full target (an object acknowledged on n0+n1 alone would
                    // otherwise legitimately drop below the acknowledgement policy when n1 crashes).
                    await cluster.WaitForRepairAsync(VolA, Converge, Ct);
                    await cluster.WaitForRepairAsync(VolB, Converge, Ct);
                    await cluster.Node("n1").CrashAsync();
                }

                if (variant == "CrashAndRepair" && round == 3)
                {
                    await cluster.Node("n1").RestartAsync();
                    await cluster.WaitForStableAsync(ct: Ct);
                }

                await Task.WhenAll(loads.Select(l => l.RunRoundAsync(round)));

                string when = $"{variant}, after load round {round}";
                await AssertVolumeConfinedAsync(cluster, VolA, poolA, when);
                await AssertVolumeConfinedAsync(cluster, VolB, poolB, when);
                await AssertPlacedWithinAsync(cluster, await ReachableAsync(cluster, VolA), poolA, 2, 2, when);
                await AssertPlacedWithinAsync(cluster, await ReachableAsync(cluster, VolB), poolB, 2, 2, when);
            }

            await cluster.WaitForRepairAsync(VolA, Converge, Ct);
            await cluster.WaitForRepairAsync(VolB, Converge, Ct);
            IReadOnlySet<ObjectId> reachableA = await ReachableAsync(cluster, VolA);
            IReadOnlySet<ObjectId> reachableB = await ReachableAsync(cluster, VolB);
            Assert.NotEmpty(reachableA);
            Assert.NotEmpty(reachableB);
            Assert.False(reachableA.Overlaps(reachableB), "independently written volumes never share objects");

            foreach ((string volume, string[] pool, IReadOnlySet<ObjectId> reachable) in new[] { (VolA, poolA, reachableA), (VolB, poolB, reachableB) })
            {
                await AssertVolumeConfinedAsync(cluster, volume, pool, $"{variant}, after repair");
                await AssertPlacedWithinAsync(cluster, reachable, pool, 3, 3, $"{variant}, after repair");

                // The pool members' inventories account for every reachable object of the volume.
                var inPool = new HashSet<ObjectId>();
                foreach (string n in pool)
                    inPool.UnionWith(await InventoryAsync(cluster, n, volume));
                ObjectId[] unaccounted = reachable.Where(o => !inPool.Contains(o)).ToArray();
                Assert.True(unaccounted.Length == 0, $"{volume}: {unaccounted.Length} reachable object(s) missing from the inventories of {Show(pool)}");
            }

            foreach (VolumeLoad load in loads)
                await load.VerifyAsync(variant);
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // POOL-006 NoFallbackOutsidePool
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<string> Outages => new() { "TwoCrashed", "AllCutFromClient", "AllRejectWrites" };

    /// <summary>
    /// Volume <c>va</c> is bound to pool <c>a</c> (n3..n5) while its metadata voters (explicit) and three healthy nodes
    /// (n0..n2) live outside the pool. When the pool cannot provide the acknowledgement copies — two members crashed, every
    /// member unreachable from the client, or every member rejecting object writes — WriteAll, Append and a flushed stream
    /// fail with placement unavailability (honest counts) instead of using the healthy outside nodes; nothing is published
    /// and the previous revision stays; repair does not move the pool's deficit outside either (and reports it); the
    /// outside nodes keep serving a volume that may use them. After recovery the same write succeeds inside the pool.
    /// </summary>
    [Theory]
    [MemberData(nameof(Outages))]
    [Catalog("POOL-006", "S", "INV-08", "INV-11")]
    public async Task POOL_006_NoFallbackOutsidePool(string outage)
    {
        IReadOnlyList<TestNodeSpec> nodes =
        [
            Node("n0", "z1"), Node("n1", "z2"), Node("n2", "z3"),
            Node("n3", "z1", PoolA), Node("n4", "z2", PoolA), Node("n5", "z3", PoolA),
        ];
        string[] poolA = Members(nodes, PoolA);
        string[] outside = ["n0", "n1", "n2"];
        // Metadata voters are explicit and outside the pool, and five of six nodes vote in the control group: losing the
        // pool's members removes placement capacity only, never metadata authority, so the failure must be placement.
        TestClusterOptions options = Deterministic(nodes, 5,
            Volume(VolA, PoolA, ThreeZones, outside), Volume("vd", DfsPoolNames.Default, ThreeZones, outside));
        await ClusterTest.RunAsync(options, async cluster =>
        {
            var clientOptions = new TestClientOptions { OperationTimeout = TimeSpan.FromSeconds(5) };
            await using DfsClient client = await cluster.ConnectAsync(clientOptions, Ct);
            DfsVolume va = client.GetVolume(VolA);
            DfsVolume vd = client.GetVolume("vd");

            byte[] baseline = TestData.Bytes(6_000, 2 * Chunk + 3);
            await va.Files.WriteAllBytesAsync("/base", baseline, Ct);
            await cluster.WaitForRepairAsync(VolA, Converge, Ct);
            IReadOnlyList<ObjectId> baseObjects = await ObjectsOfAsync(cluster, VolA, "/base");
            await AssertPlacedWithinAsync(cluster, baseObjects, poolA, 3, 3, "baseline");
            DfsFileInfo baseInfo = await va.Files.GetInfoAsync("/base", Ct);

            var arms = new List<FaultArm>();
            int maxAchieved;
            bool silent = false;
            bool readable = true;
            switch (outage)
            {
                case "TwoCrashed":
                    await cluster.Node("n4").CrashAsync();
                    await cluster.Node("n5").CrashAsync();
                    maxAchieved = 1;
                    break;
                case "AllCutFromClient":
                    foreach (string n in poolA)
                        cluster.Network.CutBoth(clientOptions.NetworkName, n);
                    maxAchieved = 0;
                    silent = true;
                    readable = false;
                    break;
                case "AllRejectWrites":
                    foreach (string n in poolA)
                        arms.Add(cluster.Faults.Arm(new FaultMatch(FaultSites.ObjectWrite, Node: n, Count: int.MaxValue), FaultAction.IoError()));
                    maxAchieved = 0;
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(outage));
            }

            byte[] attempt = TestData.Bytes(6_001, 2 * Chunk + 9);
            await AssertNoPlacementAsync(() => va.Files.WriteAllBytesAsync("/new", attempt, Ct), maxAchieved, silent, $"{outage}: WriteAll");
            await AssertNoPlacementAsync(() => va.Files.AppendAllBytesAsync("/base", attempt, Ct), maxAchieved, silent, $"{outage}: Append");
            await AssertNoPlacementAsync(() => WriteStreamAsync(va, "/base", attempt, FileMode.Open), maxAchieved, silent, $"{outage}: stream flush");
            if (arms.Count > 0)
            {
                // Two copies are needed and any two members could have provided them, so the failed writes must have asked at
                // least two different members (an uploader may stop once the remaining members cannot reach the ack count).
                FaultArm[] reached = arms.Where(a => a.Triggered).ToArray();
                Assert.True(reached.Length >= 2, $"{outage}: only {reached.Length} pool member(s) were asked to store an object");
                foreach (FaultArm arm in reached)
                    cluster.Faults.AssertReached(arm);
            }

            Assert.False(await va.Files.ExistsAsync("/new", Ct), $"{outage}: a failed WriteAll must not publish");
            DfsFileInfo afterInfo = await va.Files.GetInfoAsync("/base", Ct);
            Assert.Equal(baseInfo.Revision, afterInfo.Revision);
            Assert.Equal(baseInfo.Length, afterInfo.Length);
            if (readable)
                Same(baseline, await va.Files.ReadAllBytesAsync("/base", Ct), $"{outage}: previous revision");

            // The outside nodes are healthy: a volume allowed to use them keeps working.
            byte[] other = TestData.Bytes(6_002, 2 * Chunk);
            await vd.Files.WriteAllBytesAsync("/ok", other, Ct);
            Same(other, await vd.Files.ReadAllBytesAsync("/ok", Ct), $"{outage}: default-pool volume");

            // Several repair/inventory periods later, still nothing of va outside its pool (no repair fallback either).
            await cluster.DelayAsync(TimeSpan.FromSeconds(3), Ct);
            await AssertVolumeConfinedAsync(cluster, VolA, poolA, $"{outage}: during the outage");
            if (outage == "TwoCrashed")
            {
                await cluster.WaitUntilAsync(async () => (await cluster.Admin.GetReplicaHealthAsync(VolA, Ct)).UnderReplicatedObjects >= baseObjects.Count,
                    Converge, "the pool's replica deficit is reported instead of being hidden by copies outside the pool", Ct);
                await AssertPlacedWithinAsync(cluster, baseObjects, poolA, 1, 1, "TwoCrashed: the surviving pool copy");
            }

            switch (outage)
            {
                case "TwoCrashed":
                    await cluster.Node("n4").RestartAsync();
                    await cluster.Node("n5").RestartAsync();
                    break;
                case "AllCutFromClient":
                    cluster.Network.Heal();
                    break;
                default:
                    foreach (FaultArm arm in arms)
                        arm.Disarm();
                    break;
            }

            await cluster.WaitForStableAsync(ct: Ct);
            await va.Files.WriteAllBytesAsync("/new", attempt, Ct);
            await AssertPlacedWithinAsync(cluster, await ObjectsOfAsync(cluster, VolA, "/new"), poolA, 2, 2, $"{outage}: write after recovery");
            await cluster.WaitForRepairAsync(VolA, Converge, Ct);
            await AssertPlacedWithinAsync(cluster, await ReachableAsync(cluster, VolA), poolA, 3, 3, $"{outage}: after recovery");
            await AssertVolumeConfinedAsync(cluster, VolA, poolA, $"{outage}: after recovery");
            Same(attempt, await va.Files.ReadAllBytesAsync("/new", Ct), $"{outage}: /new");
            Same(baseline, await va.Files.ReadAllBytesAsync("/base", Ct), $"{outage}: /base");
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // POOL-008 PolicyNamedPoolTier
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// A named policy <c>hot</c> bound to pool <c>fast</c>: files created with it (WriteAll with options, creating stream
    /// open) record the policy and place every object only on <c>fast</c> nodes — at acknowledgement, after repair, and
    /// with a <c>fast</c> node down (no fallback to the volume's pool even when, in the second layout, the volume's pool is
    /// <c>default</c> and contains healthy non-fast nodes) — while files without a policy use the volume pool. Changing a
    /// file to <c>hot</c> migrates it onto <c>fast</c> while it stays readable.
    /// </summary>
    [Theory]
    [InlineData("DisjointVolumePool")]
    [InlineData("DefaultVolumePool")]
    [Catalog("POOL-008", "S", "INV-08", "INV-11")]
    public async Task POOL_008_PolicyNamedPoolTier(string layout)
    {
        const string Bulk = "bulk", Fast = "fast", Hot = "hot", Vol = "vol";
        bool disjoint = layout == "DisjointVolumePool";
        IReadOnlyList<TestNodeSpec> nodes = disjoint
            ? [Node("n0", "z1", Bulk), Node("n1", "z2", Bulk), Node("n2", "z3", Bulk), Node("n3", "z1", Fast), Node("n4", "z2", Fast), Node("n5", "z3", Fast)]
            : [Node("n0", "z1"), Node("n1", "z2"), Node("n2", "z3"), Node("n3", "z1", Fast), Node("n4", "z2", Fast), Node("n5", "z3", Fast)];
        string volumePoolName = disjoint ? Bulk : DfsPoolNames.Default;
        string[] fast = Members(nodes, Fast);
        string[] volumePool = Members(nodes, volumePoolName);
        string[] notFast = nodes.Select(n => n.Name).Where(n => !fast.Contains(n)).ToArray();
        TestVolumeSpec spec = Volume(Vol, volumePoolName) with
        {
            NamedPolicies = new Dictionary<string, DfsStoragePolicy>(StringComparer.Ordinal) { [Hot] = ThreeZones with { Pool = Fast } },
        };

        await ClusterTest.RunAsync(Deterministic(nodes, 3, spec), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { OperationTimeout = TimeSpan.FromSeconds(5) }, Ct);
            DfsVolume vol = client.GetVolume(Vol);
            DfsVolumeInfo info = await vol.GetInfoAsync(Ct);
            Assert.Equal(volumePoolName, info.StoragePool);
            Assert.Equal(Fast, info.NamedPolicies[Hot].Pool);

            var model = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            string[] coldFiles = ["/cold0", "/cold1", "/cold2"];
            string[] hotFiles = ["/hot0", "/hot1", "/hot-stream"];
            for (int i = 0; i < coldFiles.Length; i++)
            {
                byte[] data = TestData.Bytes(8_000 + (ulong)i, 2 * Chunk + i);
                await vol.Files.WriteAllBytesAsync(coldFiles[i], data, Ct);
                model[coldFiles[i]] = data;
            }

            var hot = new DfsMutationOptions { StoragePolicy = Hot };
            for (int i = 0; i < 2; i++)
            {
                byte[] data = TestData.Bytes(8_010 + (ulong)i, 2 * Chunk + i);
                await vol.Files.WriteAllBytesAsync(hotFiles[i], data, hot, Ct);
                model[hotFiles[i]] = data;
            }

            byte[] streamed = TestData.Bytes(8_020, 3 * Chunk);
            await WriteStreamAsync(vol, "/hot-stream", streamed, FileMode.CreateNew, Hot);
            model["/hot-stream"] = streamed;

            foreach (string f in hotFiles)
            {
                Assert.Equal(Hot, (await vol.Files.GetInfoAsync(f, Ct)).StoragePolicy);
                FileInspection? inspected = await cluster.Inspect.GetFileAsync(Vol, f);
                Assert.NotNull(inspected);
                Assert.Equal(Hot, inspected.Policy);
            }

            foreach (string f in coldFiles)
                Assert.NotEqual(Hot, (await vol.Files.GetInfoAsync(f, Ct)).StoragePolicy);

            List<ObjectId> hotObjects = await ObjectsOfAllAsync(cluster, Vol, hotFiles);
            List<ObjectId> coldObjects = await ObjectsOfAllAsync(cluster, Vol, coldFiles);
            await AssertPlacedWithinAsync(cluster, hotObjects, fast, 2, 2, $"{layout}: hot files at acknowledgement");
            await AssertPlacedWithinAsync(cluster, coldObjects, volumePool, 2, 2, $"{layout}: cold files at acknowledgement");
            await cluster.WaitForRepairAsync(Vol, Converge, Ct);
            await AssertPlacedWithinAsync(cluster, hotObjects, fast, 3, 3, $"{layout}: hot files after repair");
            await AssertPlacedWithinAsync(cluster, coldObjects, volumePool, 3, 3, $"{layout}: cold files after repair");
            await AssertTierInventoriesAsync("after repair");

            // One fast node down: the hot write is acknowledged by the two remaining fast nodes, never by the volume pool.
            await cluster.Node("n5").CrashAsync();
            byte[] degraded = TestData.Bytes(8_030, 2 * Chunk + 5);
            await vol.Files.WriteAllBytesAsync("/hot-degraded", degraded, hot, Ct);
            model["/hot-degraded"] = degraded;
            IReadOnlyList<ObjectId> degradedObjects = await ObjectsOfAsync(cluster, Vol, "/hot-degraded");
            await AssertPlacedWithinAsync(cluster, degradedObjects, ["n3", "n4"], 2, 2, $"{layout}: hot write with n5 down");
            await cluster.DelayAsync(TimeSpan.FromSeconds(2), Ct);
            hotObjects.AddRange(degradedObjects);
            await AssertTierInventoriesAsync("with n5 down, after several repair periods");
            await cluster.Node("n5").RestartAsync();
            await cluster.WaitForStableAsync(ct: Ct);

            // Moving a file onto the tier migrates its objects onto fast nodes; the file stays readable throughout.
            await vol.Files.SetStoragePolicyAsync("/cold0", Hot, Ct);
            await cluster.WaitUntilAsync(async () =>
            {
                Same(model["/cold0"], await vol.Files.ReadAllBytesAsync("/cold0", Ct), $"{layout}: /cold0 during migration");
                IReadOnlyDictionary<string, DfsTopologyLabels> labels = await LabelsAsync(cluster);
                foreach (ObjectId o in await ObjectsOfAsync(cluster, Vol, "/cold0"))
                {
                    HashSet<string> holders = await HoldersAsync(cluster, o);
                    (int c, int z) = Domains(holders, labels);
                    if (holders.Any(h => !fast.Contains(h)) || c < 3 || z < 3)
                        return false;
                }

                return true;
            }, Converge, "/cold0 migrates onto the fast pool", Ct);
            Assert.Equal(Hot, (await vol.Files.GetInfoAsync("/cold0", Ct)).StoragePolicy);

            await cluster.WaitForRepairAsync(Vol, Converge, Ct);
            hotObjects = await ObjectsOfAllAsync(cluster, Vol, [.. hotFiles, "/hot-degraded", "/cold0"]);
            coldObjects = await ObjectsOfAllAsync(cluster, Vol, ["/cold1", "/cold2"]);
            await AssertPlacedWithinAsync(cluster, hotObjects, fast, 3, 3, $"{layout}: hot files at the end");
            await AssertPlacedWithinAsync(cluster, coldObjects, volumePool, 3, 3, $"{layout}: cold files at the end");
            await AssertTierInventoriesAsync("at the end");
            foreach ((string path, byte[] expected) in model)
                Same(expected, await vol.Files.ReadAllBytesAsync(path, Ct), $"{layout}: {path}");

            // Inventory view of the tiers: no non-fast node stores a hot object; in the disjoint layout no fast node
            // stores a cold one.
            async Task AssertTierInventoriesAsync(string when)
            {
                foreach (string n in notFast)
                {
                    if (cluster.Node(n).State != TestNodeState.Running)
                        continue;
                    HashSet<ObjectId> stored = await InventoryAsync(cluster, n, Vol);
                    Assert.False(stored.Overlaps(hotObjects), $"{layout} {when}: non-fast node {n} stores a hot object");
                }

                if (!disjoint)
                    return;
                foreach (string n in fast)
                {
                    if (cluster.Node(n).State != TestNodeState.Running)
                        continue;
                    HashSet<ObjectId> stored = await InventoryAsync(cluster, n, Vol);
                    Assert.False(stored.Overlaps(coldObjects), $"{layout} {when}: fast node {n} stores a cold object");
                }
            }
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // POOL-009 VotersFromPool
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// A volume on pool <c>a</c> (n3..n5) without an explicit voter list gets its three metadata voters from the pool
    /// (and a leader inside it), while a volume on the same pool with an explicit list uses exactly that list. Crashing one
    /// or two nodes outside the pool never costs <c>va</c> its quorum: on every poll during the outage the group reports
    /// quorum with unchanged pool voters and a pool leader, and writes and live reads keep succeeding with their objects
    /// inside the pool.
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [Catalog("POOL-009", "S", "INV-11", "INV-18")]
    public async Task POOL_009_VotersFromPool(int outsideNodesLost)
    {
        IReadOnlyList<TestNodeSpec> nodes =
        [
            Node("n0", "z1"), Node("n1", "z2"), Node("n2", "z3"),
            Node("n3", "z1", PoolA), Node("n4", "z2", PoolA), Node("n5", "z3", PoolA),
        ];
        string[] poolA = Members(nodes, PoolA);
        string[] outside = ["n0", "n1", "n2"];
        // Five of six control voters: losing two outside nodes keeps cluster administration (and status reads) available,
        // so the observation is about the volume group's own quorum.
        TestClusterOptions options = Deterministic(nodes, 5, Volume(VolA, PoolA), Volume("vx", PoolA, ThreeZones, outside));
        await ClusterTest.RunAsync(options, async cluster =>
        {
            GroupInspection group = await cluster.Inspect.GetVolumeGroupAsync(VolA);
            Assert.Equal(poolA, Sorted(group.Voters));
            Assert.NotNull(group.Leader);
            Assert.Contains(group.Leader!, poolA);
            DfsVolumeStatus status = await cluster.Admin.GetVolumeStatusAsync(VolA, Ct);
            Assert.Equal(PoolA, status.Info.StoragePool);
            Assert.Equal(poolA, Sorted(status.Group.Voters));
            Assert.True(status.Group.HasQuorum);
            Assert.Equal(outside, Sorted((await cluster.Inspect.GetVolumeGroupAsync("vx")).Voters));

            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { OperationTimeout = TimeSpan.FromSeconds(5) }, Ct);
            DfsVolume va = client.GetVolume(VolA);
            byte[] before = TestData.Bytes(9_000, 2 * Chunk);
            await va.Files.WriteAllBytesAsync("/before", before, Ct);
            await cluster.WaitForReplicationAsync(VolA, Ct);

            string[] lost = outside.Take(outsideNodesLost).ToArray();
            foreach (string n in lost)
                await cluster.Node(n).CrashAsync();

            for (int i = 0; i < 8; i++)
            {
                DfsVolumeStatus during = await cluster.Admin.GetVolumeStatusAsync(VolA, Ct);
                Assert.True(during.Group.HasQuorum, $"va lost quorum after {Show(lost)} (outside its pool) crashed; poll {i}");
                Assert.Equal(poolA, Sorted(during.Group.Voters));
                Assert.NotNull(during.Group.Leader);
                Assert.Contains(during.Group.Leader!, poolA);

                byte[] data = TestData.Bytes(9_100 + (ulong)i, 2 * Chunk + i);
                await va.Files.WriteAllBytesAsync($"/during{i}", data, Ct);
                Same(data, await va.Files.ReadAllBytesAsync($"/during{i}", Ct), $"poll {i}: write during the outage");
                Same(before, await va.Files.ReadAllBytesAsync("/before", Ct), $"poll {i}: live read during the outage");
                await cluster.DelayAsync(TimeSpan.FromMilliseconds(250), Ct);
            }

            GroupInspection after = await cluster.Inspect.GetVolumeGroupAsync(VolA);
            Assert.Equal(poolA, Sorted(after.Voters));
            Assert.True(after.CommitIndex > group.CommitIndex, "va kept committing while the outside nodes were down");
            await AssertPlacedWithinAsync(cluster, await ReachableAsync(cluster, VolA), poolA, 2, 2, "objects written during the outage");

            foreach (string n in lost)
                await cluster.Node(n).RestartAsync();
            await cluster.WaitForStableAsync(ct: Ct);
            Assert.Equal(poolA, Sorted((await cluster.Inspect.GetVolumeGroupAsync(VolA)).Voters));
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // POOL-010 RemoveNodeFromPoolMigrates
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Pool <c>a</c> = n0 (z1), n1 (z2), n2 (z3), n3 (z3); n4, n5 are outside. Removing n2 from the pool advances the
    /// placement epoch and reports n2 as draining for the pool while its objects migrate. The first replica deletion on
    /// n2 is held: at that moment the object already has its full target on the remaining pool nodes (copy-before-delete).
    /// On every sample during the migration, no object leaves the pool (n4/n5 never receive any) or drops below the
    /// acknowledgement policy, and objects committed after the change are acknowledged by remaining pool nodes alone.
    /// Afterwards n2 holds nothing of the volume, every object sits on its deterministic targets among the remaining
    /// nodes, the pool reports no draining node and all bytes are intact. Second variant: writes continue throughout.
    /// </summary>
    [Theory]
    [InlineData("Idle")]
    [InlineData("WithConcurrentWrites")]
    [Catalog("POOL-010", "S", "INV-11", "INV-18")]
    public async Task POOL_010_RemoveNodeFromPoolMigrates(string variant)
    {
        bool concurrent = variant == "WithConcurrentWrites";
        IReadOnlyList<TestNodeSpec> nodes =
        [
            Node("n0", "z1", PoolA), Node("n1", "z2", PoolA), Node("n2", "z3", PoolA), Node("n3", "z3", PoolA),
            Node("n4", "z1"), Node("n5", "z2"),
        ];
        string[] remaining = ["n0", "n1", "n3"];
        string[] withLeaving = ["n0", "n1", "n2", "n3"];
        // Explicit voters on nodes that stay in the pool: only data placement changes.
        await ClusterTest.RunAsync(Deterministic(nodes, 3, Volume(VolA, PoolA, ThreeZones, remaining)), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { OperationTimeout = TimeSpan.FromSeconds(10) }, Ct);
            DfsVolume va = client.GetVolume(VolA);
            var model = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            for (int i = 0; i < 10; i++)
            {
                byte[] data = TestData.Bytes(10_000 + (ulong)i, 2 * Chunk + i);
                await va.Files.WriteAllBytesAsync($"/f{i}", data, Ct);
                model[$"/f{i}"] = data;
            }

            await cluster.WaitForRepairAsync(VolA, Converge, Ct);
            Assert.NotEmpty(await InventoryAsync(cluster, "n2", VolA));
            DfsStoragePolicy policy = (await va.GetInfoAsync(Ct)).DefaultPolicy;
            IReadOnlyDictionary<string, DfsTopologyLabels> labels = await LabelsAsync(cluster);
            ulong epoch0 = await PlacementEpochAsync(cluster);

            FaultArm firstDelete = cluster.Faults.Arm(new FaultMatch(FaultSites.ReplicaDelete, Node: "n2"), FaultAction.Block());

            var changeCommitted = new TaskCompletionSource();
            var stopWriting = new TaskCompletionSource();
            var written = new List<(string Path, byte[] Data, bool AfterChange)>();
            Task writer = concurrent ? WriteLoopAsync() : Task.CompletedTask;
            if (concurrent)
                await cluster.DelayAsync(TimeSpan.FromMilliseconds(300), Ct);

            await cluster.Admin.SetPoolMembersAsync(PoolA, remaining, Ct);
            changeCommitted.TrySetResult();
            Assert.True(await PlacementEpochAsync(cluster) > epoch0, "a pool membership change advances the placement epoch");
            Assert.Equal(remaining, Sorted(await cluster.Inspect.GetPoolNodesAsync(PoolA)));
            Assert.Equal(remaining, Sorted((await PoolAsync(cluster, PoolA)).Nodes));
            DfsNodeInfo n2Info = (await cluster.Admin.ListNodesAsync(Ct)).Single(n => n.Name == "n2");
            Assert.Equal(DfsNodeStatus.Active, n2Info.Status);

            // Copy-before-delete: when n2 is about to drop its first replica, the remaining pool nodes hold the full target.
            await firstDelete.WaitTriggeredAsync(Converge);
            cluster.Faults.AssertReached(firstDelete);
            ObjectId deleting = firstDelete.Contexts[0].Object
                ?? throw new InvalidOperationException("the replica-delete fault context must name the object being deleted");
            Assert.Contains(deleting, await InventoryAsync(cluster, "n2", VolA));
            var replacements = new List<string>();
            foreach (string n in remaining)
            {
                if ((await InventoryAsync(cluster, n, VolA)).Contains(deleting))
                    replacements.Add(n);
            }

            (int copies, int zones) = Domains(replacements, labels);
            Assert.True(copies >= policy.TargetCopies && zones >= policy.TargetDistinctZones,
                $"n2 started deleting {deleting} while the remaining pool nodes held only {copies} copies in {zones} zones on {Show(replacements)}");
            Assert.Contains("n2", (await cluster.Admin.GetPoolStatusAsync(PoolA, Ct)).DrainingNodes);
            firstDelete.Disarm();
            firstDelete.Release();

            await cluster.WaitUntilAsync(async () =>
            {
                await AssertMigrationSafeAsync();
                return (await InventoryAsync(cluster, "n2", VolA)).Count == 0;
            }, Converge, "n2 drains every object of pool a", Ct);

            stopWriting.TrySetResult();
            await writer;
            await cluster.WaitForRepairAsync(VolA, Converge, Ct);
            await cluster.WaitUntilAsync(async () => (await cluster.Admin.GetPoolStatusAsync(PoolA, Ct)).DrainingNodes.Count == 0,
                Converge, "the pool stops reporting n2 as draining once its objects moved", Ct);

            ulong epoch = await PlacementEpochAsync(cluster);
            IReadOnlyList<PlacementNode> map = await PlacementNodesAsync(cluster, remaining);
            foreach (ObjectId o in await ReachableAsync(cluster, VolA))
            {
                HashSet<string> expected = Targets(o, epoch, policy, map);
                HashSet<string> holders = await HoldersAsync(cluster, o);
                Assert.True(expected.SetEquals(holders), $"object {o}: holders {Show(holders)} differ from the deterministic targets {Show(expected)} at epoch {epoch}");
            }

            await AssertVolumeConfinedAsync(cluster, VolA, remaining, "after the migration");
            DfsPoolStatus final = await cluster.Admin.GetPoolStatusAsync(PoolA, Ct);
            Assert.Equal(0, final.Replicas.UnderReplicatedObjects);
            Assert.Equal(0, final.Replicas.MissingObjects);
            Assert.Equal(0, final.Replicas.PlacementViolations);
            foreach ((string path, byte[] expected) in model)
                Same(expected, await va.Files.ReadAllBytesAsync(path, Ct), path);
            foreach ((string path, byte[] data, _) in written)
                Same(data, await va.Files.ReadAllBytesAsync(path, Ct), path);
            if (concurrent)
                Assert.Contains(written, w => w.AfterChange);

            async Task WriteLoopAsync()
            {
                for (int i = 0; !stopWriting.Task.IsCompleted && i < 400; i++)
                {
                    bool after = changeCommitted.Task.IsCompleted;
                    byte[] data = TestData.Bytes(11_000 + (ulong)i, 2 * Chunk + 7);
                    await va.Files.WriteAllBytesAsync($"/live{i}", data, Ct);
                    written.Add(($"/live{i}", data, after));
                    await cluster.DelayAsync(TimeSpan.FromMilliseconds(50), Ct);
                }
            }

            async Task AssertMigrationSafeAsync()
            {
                foreach (ObjectId o in await ReachableAsync(cluster, VolA))
                {
                    HashSet<string> holders = await HoldersAsync(cluster, o);
                    string[] outsidePool = holders.Where(h => !withLeaving.Contains(h)).ToArray();
                    Assert.True(outsidePool.Length == 0, $"object {o} was copied outside the pool to {Show(outsidePool)}");
                    (int c, int z) = Domains(holders, labels);
                    Assert.True(c >= policy.DurableCopiesBeforeAck && z >= policy.DurableZonesBeforeAck,
                        $"object {o} dropped to {c} copies in {z} zones on {Show(holders)} during the migration");
                }

                foreach ((string path, _, bool after) in written.ToArray())
                {
                    if (!after)
                        continue;
                    foreach (ObjectId o in await ObjectsOfAsync(cluster, VolA, path))
                    {
                        HashSet<string> inPool = await HoldersAsync(cluster, o);
                        inPool.Remove("n2");
                        (int c, int z) = Domains(inPool, labels);
                        Assert.True(c >= policy.DurableCopiesBeforeAck && z >= policy.DurableZonesBeforeAck,
                            $"{path} (committed after the change) is acknowledged by {Show(inPool)} inside the shrunk pool only");
                    }
                }
            }
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // POOL-011 AddNodeToPoolRebalances
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Adding a node to pool <c>a</c> (an existing cluster member, or one that just joined and is only in <c>default</c>)
    /// advances the placement epoch; repair converges every object of the pool's volume to the deterministic HRW targets
    /// of the grown pool at that epoch. On every sample until convergence no object leaves the pool, falls below the
    /// acknowledgement policy or goes missing, and files stay readable; afterwards the added node holds exactly the
    /// objects the oracle assigns to it and every byte is intact.
    /// </summary>
    [Theory]
    [InlineData("ExistingNode")]
    [InlineData("NewlyJoinedNode")]
    [Catalog("POOL-011", "S", "INV-11", "INV-18")]
    public async Task POOL_011_AddNodeToPoolRebalances(string variant)
    {
        IReadOnlyList<TestNodeSpec> nodes =
        [
            Node("n0", "z1", PoolA), Node("n1", "z2", PoolA), Node("n2", "z3", PoolA),
            Node("n3", "z1"), Node("n4", "z2"),
        ];
        string[] initial = Members(nodes, PoolA);
        await ClusterTest.RunAsync(Deterministic(nodes, 3, Volume(VolA, PoolA)), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { OperationTimeout = TimeSpan.FromSeconds(10) }, Ct);
            DfsVolume va = client.GetVolume(VolA);
            var model = new List<(string Path, byte[] Data)>();
            for (int i = 0; i < 12; i++)
            {
                byte[] data = TestData.Bytes(11_500 + (ulong)i, (2 + i % 2) * Chunk + i);
                await va.Files.WriteAllBytesAsync($"/f{i}", data, Ct);
                model.Add(($"/f{i}", data));
            }

            await cluster.WaitForRepairAsync(VolA, Converge, Ct);
            await AssertPlacedWithinAsync(cluster, await ReachableAsync(cluster, VolA), initial, 3, 3, "before the change");

            string added = "n3";
            if (variant == "NewlyJoinedNode")
            {
                added = (await cluster.AddNodeAsync(new TestNodeSpec("n5", "z2"), true, Ct)).Name;
                await cluster.WaitForStableAsync(ct: Ct);
                Assert.Contains(added, (await PoolAsync(cluster, DfsPoolNames.Default)).Nodes);
                Assert.DoesNotContain(added, (await PoolAsync(cluster, PoolA)).Nodes);
            }

            string[] grown = initial.Append(added).Order(StringComparer.Ordinal).ToArray();
            ulong epoch0 = await PlacementEpochAsync(cluster);
            await cluster.Admin.SetPoolMembersAsync(PoolA, grown, Ct);
            ulong epoch1 = await PlacementEpochAsync(cluster);
            Assert.True(epoch1 > epoch0, $"adding {added} must advance the placement epoch ({epoch0} -> {epoch1})");
            Assert.Equal(grown, Sorted(await cluster.Inspect.GetPoolNodesAsync(PoolA)));

            DfsStoragePolicy policy = (await va.GetInfoAsync(Ct)).DefaultPolicy;
            IReadOnlyDictionary<string, DfsTopologyLabels> labels = await LabelsAsync(cluster);
            IReadOnlyList<PlacementNode> map = await PlacementNodesAsync(cluster, grown);
            // The targets are those of the epoch the membership change created (epoch1). Nothing else changes placement in
            // this scenario, so the current epoch normally equals it; should the cluster have advanced it further, convergence
            // to the current epoch's targets is accepted as well (pool.md, "Placement is stable ...").
            int sample = 0;
            ulong convergedEpoch = 0;
            await cluster.WaitUntilAsync(async () =>
            {
                ulong epochNow = await PlacementEpochAsync(cluster);
                bool atChange = true, atNow = true;
                foreach (ObjectId o in await ReachableAsync(cluster, VolA))
                {
                    HashSet<string> holders = await HoldersAsync(cluster, o);
                    string[] stray = holders.Where(h => !grown.Contains(h)).ToArray();
                    Assert.True(stray.Length == 0, $"object {o} is held outside the pool on {Show(stray)}");
                    (int c, int z) = Domains(holders, labels);
                    Assert.True(c >= policy.DurableCopiesBeforeAck && z >= policy.DurableZonesBeforeAck,
                        $"object {o} dropped to {c} copies in {z} zones on {Show(holders)} while rebalancing");
                    atChange &= Targets(o, epoch1, policy, map).SetEquals(holders);
                    atNow &= Targets(o, epochNow, policy, map).SetEquals(holders);
                }

                Assert.Equal(0, (await cluster.Admin.GetReplicaHealthAsync(VolA, Ct)).MissingObjects);
                (string path, byte[] data) = model[sample++ % model.Count];
                Same(data, await va.Files.ReadAllBytesAsync(path, Ct), $"{path} while rebalancing");
                convergedEpoch = atChange ? epoch1 : epochNow;
                return atChange || atNow;
            }, Converge, "repair converges to the deterministic targets of the grown pool", Ct);

            IReadOnlySet<ObjectId> reachable = await ReachableAsync(cluster, VolA);
            Assert.True(await PlacementEpochAsync(cluster) >= epoch1);
            Log($"POOL-011 ({variant}): converged to the targets of placement epoch {convergedEpoch} (change at {epoch1})");
            int expectedOnAdded = reachable.Count(o => Targets(o, convergedEpoch, policy, map).Contains(added));
            Assert.True(expectedOnAdded > 0, $"the oracle assigns objects to {added} (otherwise the rebalance is not exercised)");
            HashSet<ObjectId> onAdded = await InventoryAsync(cluster, added, VolA);
            Assert.Equal(expectedOnAdded, reachable.Count(onAdded.Contains));

            DfsReplicaHealth health = await cluster.Admin.GetReplicaHealthAsync(VolA, Ct);
            Assert.Equal(0, health.UnderReplicatedObjects);
            Assert.Equal(0, health.MissingObjects);
            Assert.Equal(0, health.PlacementViolations);
            DfsPoolStatus status = await cluster.Admin.GetPoolStatusAsync(PoolA, Ct);
            Assert.Equal(grown, Sorted(status.Info.Nodes));
            Assert.Empty(status.DrainingNodes);
            await AssertVolumeConfinedAsync(cluster, VolA, grown, "after rebalancing");
            foreach ((string path, byte[] data) in model)
                Same(data, await va.Files.ReadAllBytesAsync(path, Ct), path);
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // POOL-012 PoolCapacityIsolation
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<string> CapacityWritePaths => new() { "WriteAll", "StreamFlush", "Append" };

    /// <summary>
    /// Every node of pool <c>a</c> is filled below its capacity reserve. Writes to <c>va</c> (through WriteAll, a flushed
    /// stream or Append) fail with <see cref="DfsCapacityException"/> (<c>CapacityExhausted</c>), publish nothing and do
    /// not spill onto pool <c>b</c>, while reads of <c>va</c> keep working. At the same time — and afterwards — writes of
    /// every kind to <c>vb</c> succeed promptly, durable inside pool <c>b</c>.
    /// </summary>
    [Theory]
    [MemberData(nameof(CapacityWritePaths))]
    [Catalog("POOL-012", "S", "INV-08", "INV-16")]
    public async Task POOL_012_PoolCapacityIsolation(string writePath)
    {
        IReadOnlyList<TestNodeSpec> nodes = TwoPools();
        string[] poolA = Members(nodes, PoolA);
        string[] poolB = Members(nodes, PoolB);
        await ClusterTest.RunAsync(Deterministic(nodes, 3, Volume(VolA, PoolA), Volume(VolB, PoolB)), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { OperationTimeout = TimeSpan.FromSeconds(10) }, Ct);
            DfsVolume va = client.GetVolume(VolA);
            DfsVolume vb = client.GetVolume(VolB);
            byte[] baseA = TestData.Bytes(12_000, 2 * Chunk + 1);
            byte[] baseB = TestData.Bytes(12_001, 2 * Chunk + 1);
            await va.Files.WriteAllBytesAsync("/base", baseA, Ct);
            await vb.Files.WriteAllBytesAsync("/base", baseB, Ct);
            await cluster.WaitForRepairAsync(VolA, Converge, Ct);
            await cluster.WaitForRepairAsync(VolB, Converge, Ct);
            DfsFileInfo baseInfo = await va.Files.GetInfoAsync("/base", Ct);

            // Less than the reserve stays free on every member of pool a: admission must reject new objects there.
            long leave = cluster.Options.ReservedBytes / 2;
            foreach (string n in poolA)
                cluster.Node(n).Disk.Fill(leave);

            byte[] attempt = TestData.Bytes(12_002, 3 * Chunk);
            Task<Exception?> rejected = Record.ExceptionAsync(async () =>
            {
                switch (writePath)
                {
                    case "WriteAll":
                        await va.Files.WriteAllBytesAsync("/new", attempt, Ct);
                        break;
                    case "Append":
                        await va.Files.AppendAllBytesAsync("/base", attempt, Ct);
                        break;
                    case "StreamFlush":
                        await WriteStreamAsync(va, "/base", attempt, FileMode.Open, position: baseA.Length);
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(writePath));
                }
            }).AsTask();

            var vbModel = new Dictionary<string, byte[]>(StringComparer.Ordinal) { ["/base"] = baseB };
            await Task.WhenAll(rejected, LoadPoolBAsync(0));

            Exception? ex = await rejected;
            Assert.NotNull(ex);
            DfsCapacityException capacity = Assert.IsAssignableFrom<DfsCapacityException>(ex);
            Assert.Equal(DfsErrorCode.CapacityExhausted, capacity.ErrorCode);
            if (writePath == "WriteAll")
                Assert.False(await va.Files.ExistsAsync("/new", Ct), "a rejected WriteAll must not publish");
            DfsFileInfo after = await va.Files.GetInfoAsync("/base", Ct);
            Assert.Equal(baseInfo.Revision, after.Revision);
            Assert.Equal(baseInfo.Length, after.Length);
            Same(baseA, await va.Files.ReadAllBytesAsync("/base", Ct), "reads keep working in the exhausted pool");
            await AssertVolumeConfinedAsync(cluster, VolA, poolA, "pool a exhausted: no spill-over");

            // Pool b keeps admitting work after pool a's rejections, too.
            await LoadPoolBAsync(10);
            await AssertEx.ThrowsDfsAsync<DfsCapacityException>(() => va.Files.WriteAllBytesAsync("/new2", attempt, Ct), DfsErrorCode.CapacityExhausted);

            await AssertVolumeConfinedAsync(cluster, VolB, poolB, "pool b");
            await cluster.WaitForRepairAsync(VolB, Converge, Ct);
            await AssertPlacedWithinAsync(cluster, await ReachableAsync(cluster, VolB), poolB, 3, 3, "pool b after repair");
            foreach ((string path, byte[] expected) in vbModel)
                Same(expected, await vb.Files.ReadAllBytesAsync(path, Ct), $"vb:{path}");

            async Task LoadPoolBAsync(int first)
            {
                for (int i = first; i < first + 4; i++)
                {
                    byte[] data = TestData.Bytes(12_100 + (ulong)i, 2 * Chunk + i);
                    long started = cluster.Time.GetTimestamp();
                    await vb.Files.WriteAllBytesAsync($"/f{i}", data, Ct);
                    TimeSpan took = cluster.Time.GetElapsedTime(started);
                    Assert.True(took < TimeSpan.FromSeconds(5), $"vb write {i} took {took} while pool a was full");
                    vbModel[$"/f{i}"] = data;
                    await AssertPlacedWithinAsync(cluster, await ObjectsOfAsync(cluster, VolB, $"/f{i}"), poolB, 2, 2, $"vb:/f{i} at acknowledgement");
                }

                byte[] tail = TestData.Bytes(12_200 + (ulong)first, Chunk + 3);
                await vb.Files.AppendAllBytesAsync("/base", tail, Ct);
                vbModel["/base"] = vbModel["/base"].Concat(tail).ToArray();
                byte[] streamed = TestData.Bytes(12_300 + (ulong)first, 2 * Chunk);
                await WriteStreamAsync(vb, $"/s{first}", streamed, FileMode.CreateNew);
                vbModel[$"/s{first}"] = streamed;
            }
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // POOL-014 NodeInSeveralPools
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// n6 belongs to pools <c>a</c> and <c>b</c> and stores objects of both pools' volumes. Removing it from one pool
    /// migrates only that pool's objects off n6 (never below the acknowledgement policy, never outside that pool), while the
    /// other pool's copies on n6 — and that pool's whole placement — stay exactly as they were, and n6 is never reported as
    /// draining for the other pool.
    /// </summary>
    [Theory]
    [InlineData(PoolA)]
    [InlineData(PoolB)]
    [Catalog("POOL-014", "S", "INV-11", "INV-18")]
    public async Task POOL_014_NodeInSeveralPools(string removeFrom)
    {
        const string Shared = "n6";
        IReadOnlyList<TestNodeSpec> nodes =
        [
            Node("n0", "z1", PoolA), Node("n1", "z2", PoolA), Node("n2", "z3", PoolA),
            Node("n3", "z1", PoolB), Node("n4", "z2", PoolB), Node("n5", "z3", PoolB),
            Node(Shared, "z3", PoolA, PoolB),
        ];
        string keep = removeFrom == PoolA ? PoolB : PoolA;
        string migratingVolume = removeFrom == PoolA ? VolA : VolB;
        string stayingVolume = removeFrom == PoolA ? VolB : VolA;
        string[] remaining = Members(nodes, removeFrom).Where(n => n != Shared).ToArray();
        string[] withShared = Members(nodes, removeFrom);
        // Explicit voters keep the shared node out of both metadata groups: only data placement is under test.
        TestClusterOptions options = Deterministic(nodes, 3,
            Volume(VolA, PoolA, ThreeZones, "n0", "n1", "n2"), Volume(VolB, PoolB, ThreeZones, "n3", "n4", "n5"));
        await ClusterTest.RunAsync(options, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { OperationTimeout = TimeSpan.FromSeconds(10) }, Ct);
            var model = new List<(string Volume, string Path, byte[] Data)>();
            foreach (string volume in new[] { VolA, VolB })
            {
                for (int i = 0; i < 12; i++)
                {
                    byte[] data = TestData.Bytes((volume == VolA ? 14_000UL : 14_500UL) + (ulong)i, 2 * Chunk + i);
                    await client.GetVolume(volume).Files.WriteAllBytesAsync($"/f{i}", data, Ct);
                    model.Add((volume, $"/f{i}", data));
                }

                await cluster.WaitForRepairAsync(volume, Converge, Ct);
            }

            IReadOnlySet<ObjectId> reachableStaying = await ReachableAsync(cluster, stayingVolume);
            HashSet<ObjectId> sharedA = await InventoryAsync(cluster, Shared, VolA);
            HashSet<ObjectId> sharedB = await InventoryAsync(cluster, Shared, VolB);
            Assert.NotEmpty(sharedA);
            Assert.NotEmpty(sharedB);
            HashSet<ObjectId> stayingOnShared = [.. (removeFrom == PoolA ? sharedB : sharedA).Where(reachableStaying.Contains)];
            Assert.NotEmpty(stayingOnShared);
            var stayingHolders = new Dictionary<ObjectId, HashSet<string>>();
            foreach (ObjectId o in reachableStaying)
                stayingHolders[o] = await HoldersAsync(cluster, o);
            DfsStoragePolicy policy = (await client.GetVolume(migratingVolume).GetInfoAsync(Ct)).DefaultPolicy;
            IReadOnlyDictionary<string, DfsTopologyLabels> labels = await LabelsAsync(cluster);

            await cluster.Admin.SetPoolMembersAsync(removeFrom, remaining, Ct);
            Assert.Equal(remaining, Sorted(await cluster.Inspect.GetPoolNodesAsync(removeFrom)));
            Assert.Contains(Shared, await cluster.Inspect.GetPoolNodesAsync(keep));

            await cluster.WaitUntilAsync(async () =>
            {
                HashSet<ObjectId> nowStaying = await InventoryAsync(cluster, Shared, stayingVolume);
                ObjectId[] lost = stayingOnShared.Where(o => !nowStaying.Contains(o)).ToArray();
                Assert.True(lost.Length == 0, $"{Shared} dropped {lost.Length} object(s) of {stayingVolume} although it stays in pool {keep}");
                Assert.DoesNotContain(Shared, (await cluster.Admin.GetPoolStatusAsync(keep, Ct)).DrainingNodes);
                foreach (ObjectId o in await ReachableAsync(cluster, migratingVolume))
                {
                    HashSet<string> holders = await HoldersAsync(cluster, o);
                    string[] stray = holders.Where(h => !withShared.Contains(h)).ToArray();
                    Assert.True(stray.Length == 0, $"object {o} of {migratingVolume} copied outside pool {removeFrom} to {Show(stray)}");
                    (int c, int z) = Domains(holders, labels);
                    Assert.True(c >= policy.DurableCopiesBeforeAck && z >= policy.DurableZonesBeforeAck,
                        $"object {o} of {migratingVolume} dropped to {c} copies in {z} zones during the migration");
                }

                return (await InventoryAsync(cluster, Shared, migratingVolume)).Count == 0;
            }, Converge, $"{Shared} drains the objects of pool {removeFrom}", Ct);

            await cluster.WaitForRepairAsync(migratingVolume, Converge, Ct);
            await AssertPlacedWithinAsync(cluster, await ReachableAsync(cluster, migratingVolume), remaining, 3, 3, $"{migratingVolume} after the migration");
            await AssertVolumeConfinedAsync(cluster, migratingVolume, remaining, $"{migratingVolume} after the migration");
            foreach ((ObjectId o, HashSet<string> before) in stayingHolders)
            {
                HashSet<string> now = await HoldersAsync(cluster, o);
                Assert.True(before.SetEquals(now), $"object {o} of {stayingVolume} moved from {Show(before)} to {Show(now)} although its pool did not change");
            }

            Assert.True(stayingOnShared.SetEquals((await InventoryAsync(cluster, Shared, stayingVolume)).Where(reachableStaying.Contains)));
            await cluster.WaitUntilAsync(async () => (await cluster.Admin.GetPoolStatusAsync(removeFrom, Ct)).DrainingNodes.Count == 0,
                Converge, $"pool {removeFrom} stops reporting {Shared} as draining", Ct);
            Assert.Contains(Shared, (await cluster.Admin.GetPoolStatusAsync(keep, Ct)).Info.Nodes);
            foreach ((string volume, string path, byte[] data) in model)
                Same(data, await client.GetVolume(volume).Files.ReadAllBytesAsync(path, Ct), $"{volume}:{path}");
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // POOL-015 PoolChangeDuringWrites
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// A writer stages a full overwrite of <c>/f</c> while only n0 (z1, stays in the pool) and n3 (z3, about to leave)
    /// can take copies, so its receipts satisfy two copies in two zones only by counting n3. The commit request is held at
    /// the leader; n3 is removed from the pool (new placement epoch); the commit is released. It must not count n3's
    /// receipts: with the other pool nodes still unreachable it fails with a placement/epoch error and <c>/f</c> keeps its
    /// previous revision; with them back it either fails the same way or succeeds only after the pool at the commit's epoch
    /// (n0..n2) holds the acknowledgement policy by itself. Afterwards n3 ends up holding nothing of the volume.
    /// </summary>
    [Theory]
    [InlineData("TargetsStillDown")]
    [InlineData("TargetsBack")]
    [Catalog("POOL-015", "S", "INV-08", "INV-11", "INV-18")]
    public async Task POOL_015_PoolChangeDuringWrites(string variant)
    {
        IReadOnlyList<TestNodeSpec> nodes =
        [
            Node("n0", "z1", PoolA), Node("n1", "z2", PoolA), Node("n2", "z3", PoolA), Node("n3", "z3", PoolA),
            Node("n4", "z1"), Node("n5", "z2"),
        ];
        string[] shrunk = ["n0", "n1", "n2"];
        string[] voters = ["n0", "n4", "n5"];
        // Volume voters (explicit) and four of five control voters survive isolating n1 and n2.
        await ClusterTest.RunAsync(Deterministic(nodes, 5, Volume(VolA, PoolA, ThreeZones, voters)), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { OperationTimeout = TimeSpan.FromSeconds(30), SeedNodes = voters }, Ct);
            DfsVolume va = client.GetVolume(VolA);
            byte[] v0 = TestData.Bytes(15_000, 3 * Chunk);
            await va.Files.WriteAllBytesAsync("/f", v0, Ct);
            await cluster.WaitForRepairAsync(VolA, Converge, Ct);
            DfsFileInfo v0Info = await va.Files.GetInfoAsync("/f", Ct);
            HashSet<ObjectId> n0Before = await InventoryAsync(cluster, "n0", VolA);
            HashSet<ObjectId> n3Before = await InventoryAsync(cluster, "n3", VolA);

            cluster.Network.Isolate("n1");
            cluster.Network.Isolate("n2");
            FaultArm commitGate = cluster.Faults.Arm(
                new FaultMatch(FaultSites.RpcRequestAccepted, Volume: VolA, Operation: DfsOperationKind.CommitFile, Count: int.MaxValue), FaultAction.Block());

            byte[] v1 = TestData.Bytes(15_001, 3 * Chunk);
            DfsStream writer = await va.Files.OpenAsync("/f", new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.Write, Share = FileShare.Read }, Ct);
            await writer.WriteAsync(v1, Ct);
            Task flush = writer.FlushAsync(Ct);
            await commitGate.WaitTriggeredAsync(Converge);
            cluster.Faults.AssertReached(commitGate);
            Assert.False(flush.IsCompleted, "the commit is held before the leader validates its receipts");

            HashSet<ObjectId> stagedOnN3 = await InventoryAsync(cluster, "n3", VolA);
            stagedOnN3.ExceptWith(n3Before);
            HashSet<ObjectId> stagedOnN0 = await InventoryAsync(cluster, "n0", VolA);
            stagedOnN0.ExceptWith(n0Before);
            Assert.NotEmpty(stagedOnN3);
            Assert.NotEmpty(stagedOnN0);

            ulong epochBefore = await PlacementEpochAsync(cluster);
            await cluster.Admin.SetPoolMembersAsync(PoolA, shrunk, Ct);
            Assert.True(await PlacementEpochAsync(cluster) > epochBefore, "removing n3 advances the placement epoch");
            Assert.Equal(shrunk, Sorted(await cluster.Inspect.GetPoolNodesAsync(PoolA)));

            if (variant == "TargetsBack")
                cluster.Network.Heal();
            commitGate.Disarm();
            commitGate.Release();
            Exception? outcome = await Record.ExceptionAsync(() => flush);
            IReadOnlyDictionary<string, DfsTopologyLabels> labels = await LabelsAsync(cluster);

            if (outcome is null)
            {
                Assert.True(variant == "TargetsBack", "with only n0 left reachable in the pool, nothing can acknowledge 2 copies in 2 zones");
                foreach (ObjectId o in await ObjectsOfAsync(cluster, VolA, "/f"))
                {
                    string[] inPool = (await HoldersAsync(cluster, o)).Where(h => shrunk.Contains(h)).ToArray();
                    (int c, int z) = Domains(inPool, labels);
                    Assert.True(c >= 2 && z >= 2,
                        $"the commit was acknowledged although the pool at its epoch holds object {o} only on {Show(inPool)} ({c} copies, {z} zones)");
                }

                await writer.DisposeAsync();
                Same(v1, await va.Files.ReadAllBytesAsync("/f", Ct), "committed overwrite");
            }
            else
            {
                Assert.True(outcome is DfsPlacementUnavailableException or DfsEpochMismatchException
                                || outcome is DfsException { ErrorCode: DfsErrorCode.PlacementUnavailable or DfsErrorCode.EpochMismatch },
                    $"{variant}: the rejected commit must fail with a placement/epoch error, got {outcome}");
                await DiscardAsync(writer);
                cluster.Network.Heal();
                DfsFileInfo now = await va.Files.GetInfoAsync("/f", Ct);
                Assert.Equal(v0Info.Revision, now.Revision);
                Same(v0, await va.Files.ReadAllBytesAsync("/f", Ct), "previous revision after the rejected commit");
            }

            cluster.Network.Heal();
            await cluster.WaitForStableAsync(ct: Ct);
            byte[] v2 = TestData.Bytes(15_002, 3 * Chunk);
            await va.Files.WriteAllBytesAsync("/f", v2, Ct);
            await AssertPlacedWithinAsync(cluster, await ObjectsOfAsync(cluster, VolA, "/f"), shrunk, 2, 2, "a write after the change");
            await cluster.WaitForRepairAsync(VolA, Converge, Ct);
            await cluster.WaitUntilAsync(async () => (await InventoryAsync(cluster, "n3", VolA)).Count == 0, Converge,
                "n3 (removed from the pool) ends up holding nothing of va: committed copies migrate, abandoned staging is collected", Ct);
            await AssertPlacedWithinAsync(cluster, await ReachableAsync(cluster, VolA), shrunk, 3, 3, "after convergence");
            Same(v2, await va.Files.ReadAllBytesAsync("/f", Ct), "final content");
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // POOL-016 SnapshotRestoreKeepsPool
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<string> RestoreTargets => new() { "OtherPool", "SamePool", "OtherPoolNodeDown", "UnsatisfiablePool", "UnknownPool" };

    /// <summary>
    /// Restores snapshot <c>s1</c> of <c>va</c> (pool <c>a</c>) into a new volume bound to a chosen pool. The new volume
    /// reports that pool, is listed under it, takes its voters from it and holds exactly the snapshot's content (not later
    /// live changes); every object it references is stored on at least the policy's copies/zones among the chosen pool's
    /// nodes, and no node outside that pool gains any object — also with a member of the chosen pool down. A pool that
    /// cannot satisfy the restored policy (<c>InvalidPolicy</c>) or does not exist (<c>PoolNotFound</c>) is rejected
    /// without creating a volume or copying anything.
    /// </summary>
    [Theory]
    [MemberData(nameof(RestoreTargets))]
    [Catalog("POOL-016", "S", "INV-08", "INV-11", "INV-18")]
    public async Task POOL_016_SnapshotRestoreKeepsPool(string target)
    {
        const string PoolC = "c", Restored = "vr";
        IReadOnlyList<TestNodeSpec> nodes = [.. TwoPools(), Node("n6", "z1", PoolC)];
        string pool = target switch
        {
            "SamePool" => PoolA,
            "UnsatisfiablePool" => PoolC,
            "UnknownPool" => "nope",
            _ => PoolB,
        };
        string[] poolNodes = Members(nodes, pool);
        await ClusterTest.RunAsync(Deterministic(nodes, 3, Volume(VolA, PoolA)), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { OperationTimeout = TimeSpan.FromSeconds(10) }, Ct);
            DfsVolume va = client.GetVolume(VolA);
            var snapshotModel = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            await va.Directories.CreateDirectoryAsync("/d", Ct);
            for (int i = 0; i < 5; i++)
            {
                byte[] data = TestData.Bytes(16_000 + (ulong)i, 2 * Chunk + i);
                await va.Files.WriteAllBytesAsync($"/d/f{i}", data, Ct);
                snapshotModel[$"/d/f{i}"] = data;
            }

            byte[] small = TestData.Bytes(16_010, 100);
            await va.Files.WriteAllBytesAsync("/small", small, Ct);
            snapshotModel["/small"] = small;
            await va.Directories.CreateDirectoryAsync("/d/empty", Ct);
            await va.Snapshots.CreateAsync("s1", null, Ct);

            // Live changes after the boundary must not leak into the restore.
            byte[] liveF0 = TestData.Bytes(16_100, 2 * Chunk);
            byte[] late = TestData.Bytes(16_101, 2 * Chunk);
            await va.Files.WriteAllBytesAsync("/d/f0", liveF0, Ct);
            await va.Files.DeleteAsync("/d/f1", Ct);
            await va.Files.WriteAllBytesAsync("/late", late, Ct);
            await cluster.WaitForRepairAsync(VolA, Converge, Ct);

            var before = new Dictionary<string, HashSet<ObjectId>>(StringComparer.Ordinal);
            foreach (TestNode n in cluster.Nodes)
                before[n.Name] = await InventoryAsync(cluster, n.Name);
            if (target == "OtherPoolNodeDown")
                await cluster.Node("n5").CrashAsync();

            if (target is "UnsatisfiablePool" or "UnknownPool")
            {
                await AssertEx.ThrowsDfsAsync<DfsException>(() => cluster.RestoreSnapshotToPoolAsync(VolA, "s1", Restored, pool, Ct),
                    target == "UnknownPool" ? DfsErrorCode.PoolNotFound : DfsErrorCode.InvalidPolicy);
                Assert.DoesNotContain(Restored, await cluster.Admin.ListVolumesAsync(Ct));
                if (target == "UnsatisfiablePool")
                    Assert.DoesNotContain(Restored, (await PoolAsync(cluster, PoolC)).Volumes);
                await AssertNothingNewOutsideAsync([], "rejected restore");
                return;
            }

            DfsVolumeInfo restored = await cluster.RestoreSnapshotToPoolAsync(VolA, "s1", Restored, pool, Ct);
            Assert.Equal(Restored, restored.Name);
            Assert.Equal(pool, restored.StoragePool);
            Assert.NotEqual((await va.GetInfoAsync(Ct)).Id, restored.Id);
            Assert.Contains(Restored, (await PoolAsync(cluster, pool)).Volumes);
            if (pool != PoolA)
                Assert.DoesNotContain(Restored, (await PoolAsync(cluster, PoolA)).Volumes);
            Assert.Equal(pool, (await cluster.Admin.GetVolumeStatusAsync(Restored, Ct)).Info.StoragePool);
            GroupInspection group = await cluster.Inspect.GetVolumeGroupAsync(Restored);
            Assert.All(group.Voters, v => Assert.Contains(v, poolNodes));

            DfsVolume vr = client.GetVolume(Restored);
            foreach ((string path, byte[] expected) in snapshotModel)
                Same(expected, await vr.Files.ReadAllBytesAsync(path, Ct), $"{Restored}:{path} equals the snapshot");
            Assert.False(await vr.Files.ExistsAsync("/late", Ct), "a file created after the snapshot boundary must not be restored");
            Assert.True(await vr.Directories.ExistsAsync("/d/empty", Ct));

            await AssertCopiesInPoolAsync(2, 2, "at restore completion");
            await AssertNothingNewOutsideAsync(poolNodes, "at restore completion");

            // New data of the restored volume goes to its pool as well.
            byte[] fresh = TestData.Bytes(16_200, 2 * Chunk);
            await vr.Files.WriteAllBytesAsync("/after", fresh, Ct);
            await AssertPlacedWithinAsync(cluster, await ObjectsOfAsync(cluster, Restored, "/after"), poolNodes, 2, 2, "a write to the restored volume");

            if (target == "OtherPoolNodeDown")
            {
                await cluster.DelayAsync(TimeSpan.FromSeconds(2), Ct);
                await AssertNothingNewOutsideAsync(poolNodes, "with n5 down, after several repair periods");
                await cluster.Node("n5").RestartAsync();
                await cluster.WaitForStableAsync(ct: Ct);
            }

            await cluster.WaitForRepairAsync(Restored, Converge, Ct);
            await AssertCopiesInPoolAsync(3, 3, "after repair");
            await AssertNothingNewOutsideAsync(poolNodes, "after repair");
            Same(fresh, await vr.Files.ReadAllBytesAsync("/after", Ct), "restored volume write");

            // The source volume is untouched.
            Same(liveF0, await va.Files.ReadAllBytesAsync("/d/f0", Ct), "live /d/f0");
            Same(late, await va.Files.ReadAllBytesAsync("/late", Ct), "live /late");
            Assert.False(await va.Files.ExistsAsync("/d/f1", Ct));

            // Every object the restored volume references is stored, by inventory, on enough distinct hosts/zones of the
            // chosen pool (object ids may or may not be shared with the source; only the chosen pool's nodes count).
            async Task AssertCopiesInPoolAsync(int copies, int zones, string when)
            {
                IReadOnlyDictionary<string, DfsTopologyLabels> labels = await LabelsAsync(cluster);
                var inventories = new Dictionary<string, HashSet<ObjectId>>(StringComparer.Ordinal);
                foreach (string n in poolNodes)
                {
                    if (cluster.Node(n).State == TestNodeState.Running)
                        inventories[n] = await InventoryAsync(cluster, n);
                }

                IReadOnlySet<ObjectId> reachable = await ReachableAsync(cluster, Restored);
                Assert.NotEmpty(reachable);
                foreach (ObjectId o in reachable)
                {
                    string[] holders = inventories.Where(kv => kv.Value.Contains(o)).Select(kv => kv.Key).ToArray();
                    (int c, int z) = Domains(holders, labels);
                    Assert.True(c >= copies && z >= zones, $"{target} {when}: object {o} of {Restored} is stored on {Show(holders)} in pool {pool}; required {copies}/{zones}");
                }
            }

            // Nodes outside the chosen pool gain no object (their inventories only shrink or stay).
            async Task AssertNothingNewOutsideAsync(string[] allowed, string when)
            {
                foreach (TestNode n in cluster.Nodes)
                {
                    if (allowed.Contains(n.Name) || n.State != TestNodeState.Running)
                        continue;
                    HashSet<ObjectId> now = await InventoryAsync(cluster, n.Name);
                    ObjectId[] gained = now.Where(o => !before[n.Name].Contains(o)).ToArray();
                    Assert.True(gained.Length == 0, $"{target} {when}: node {n.Name} outside pool {pool} gained {gained.Length} object(s)");
                }
            }
        }, Ct);
    }

}
