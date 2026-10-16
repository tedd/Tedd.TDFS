using Tedd.Tdfs.Server;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Tests.Simulation.Ops;

/// <summary>
/// OPS suite, level S: administration, observability and bounded liveness under deterministic simulation (FSD §§9, 12,
/// 13, 14, 15; test plan §7.26). Safety properties are checked during faults and again after recovery; every liveness
/// wait states its restored prerequisites and has an explicit virtual-time bound.
/// </summary>
public sealed class OpsSimulationTests
{
    private const string Vol = OpsHelpers.Volume;

    private static CancellationToken Ct => OpsHelpers.Ct;

    private static readonly DfsErrorCode[] RemovalRefusalCodes =
        [DfsErrorCode.PlacementUnavailable, DfsErrorCode.QuorumUnavailable, DfsErrorCode.InvalidArgument, DfsErrorCode.InvalidPolicy];

    // ============================================================================================================
    // OPS-001 DrainNodeSafely
    // ============================================================================================================

    /// <summary>
    /// Drain the volume leader of a 4- or 5-node cluster (target 3 copies in 3 zones) while writers, an appender and a reader
    /// run. The node passes through a visible Draining state to Drained (never silently Removed); before Drained, its
    /// leadership and control/volume votes move away and every referenced object gains the full copy/zone target on the
    /// remaining nodes; the foreground history stays linearizable with no unexpected errors; afterwards removal is accepted
    /// and the cluster keeps serving the exact bytes with no deficit and no split brain.
    /// </summary>
    [Theory, Catalog("OPS-001", "S", "INV-08", "INV-11", "INV-13", "INV-16", "INV-17", "INV-18")]
    [InlineData(4)]
    [InlineData(5)]
    public async Task OPS_001_DrainNodeSafely(int nodes)
    {
        await ClusterTest.RunAsync(OpsHelpers.Nodes(nodes, OpsHelpers.Policy(3, 3, 2, 2)), async cluster =>
        {
            DfsAdminClient admin = cluster.Admin;
            await using DfsClient c0 = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            await using DfsClient c1 = await cluster.ConnectAsync(new TestClientOptions { Name = "c1" }, Ct);
            await using DfsClient reader = await cluster.ConnectAsync(new TestClientOptions { Name = "r0" }, Ct);
            DfsVolume vol = c0.GetVolume(Vol);
            Dictionary<string, byte[]> seeded = await OpsHelpers.SeedFilesAsync(vol, "/ops001/seed", 12, 96 * 1024, 100);
            await cluster.WaitForRepairAsync(Vol, TimeSpan.FromSeconds(60), Ct);

            TestNode target = await cluster.GetLeaderAsync(Vol, Ct);
            Assert.NotEmpty(await target.Inspect.ListObjectsAsync(Vol));

            await vol.Directories.CreateDirectoryAsync("/ops001/fg", Ct);
            var workload = new HistoryWorkload(cluster, "/ops001/fg");
            workload.Start([(c0, "k0"), (c1, "k1"), (c0, "log0")], reader, valueBytes: 8 * 1024, pause: TimeSpan.FromMilliseconds(50));

            await admin.DrainNodeAsync(target.Name, Ct);
            var seen = new List<DfsNodeStatus>();
            await cluster.WaitUntilAsync(async () =>
            {
                DfsNodeInfo info = (await admin.ListNodesAsync(Ct)).Single(n => n.Name == target.Name);
                if (seen.Count == 0 || seen[^1] != info.Status)
                    seen.Add(info.Status);
                Assert.NotEqual(DfsNodeStatus.Removed, info.Status);
                OpsHelpers.BytesEqual(seeded["/ops001/seed/f0"], await reader.GetVolume(Vol).Files.ReadAllBytesAsync("/ops001/seed/f0", Ct), "read during drain");
                return info.Status == DfsNodeStatus.Drained;
            }, TimeSpan.FromSeconds(180), $"draining {target.Name} must complete while foreground traffic runs", Ct);
            OpsHelpers.Log($"{target.Name} status sequence: {string.Join(" -> ", seen)}");
            Assert.Contains(DfsNodeStatus.Draining, seen);
            Assert.Equal(DfsNodeStatus.Drained, seen[^1]);

            DfsVolumeStatus volumeStatus = await admin.GetVolumeStatusAsync(Vol, Ct);
            Assert.DoesNotContain(target.Name, volumeStatus.Group.Voters);
            Assert.NotEqual(target.Name, volumeStatus.Group.Leader);
            Assert.Equal(3, volumeStatus.Group.Voters.Count);
            Assert.True(volumeStatus.Group.HasQuorum);
            DfsClusterStatus clusterStatus = await admin.GetClusterStatusAsync(Ct);
            Assert.DoesNotContain(target.Name, clusterStatus.Control.Voters);

            VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Vol);
            foreach (ObjectId id in state.ReachableObjects())
            {
                List<string> holders = (await cluster.Inspect.GetObjectHoldersAsync(id)).Where(h => h != target.Name).ToList();
                int zones = holders.Select(h => cluster.Node(h).Labels.Zone).Distinct(StringComparer.Ordinal).Count();
                Assert.True(holders.Count >= 3 && zones >= 3,
                    $"object {id} has only {holders.Count} copies in {zones} zones outside drained node {target.Name}");
            }

            await workload.StopAsync();
            await OpsHelpers.VerifyFilesAsync(reader.GetVolume(Vol), seeded, "after drain");

            await admin.RemoveNodeAsync(target.Name, force: false, Ct);
            await cluster.WaitUntilAsync(async () => (await admin.ListNodesAsync(Ct)).All(n => n.Name != target.Name || n.Status == DfsNodeStatus.Removed),
                TimeSpan.FromSeconds(60), "a drained node must be removable", Ct);
            await target.CrashAsync();

            await workload.WriteAsync(c0, "k0", TestData.Bytes(199, 8 * 1024));
            await workload.ResolveUnknownsAsync(reader);
            await workload.FinalReadsAsync(reader, ["k0", "k1", "log0"]);
            workload.AssertLinearizable();
            Assert.True(workload.OkWrites > 0, "foreground writes must have made progress");
            await OpsHelpers.VerifyFilesAsync(reader.GetVolume(Vol), seeded, "after removal of the drained node");
            DfsReplicaHealth health = await admin.GetReplicaHealthAsync(Vol, Ct);
            Assert.Equal(0, health.MissingObjects);
            Assert.Equal(0, health.UnderReplicatedObjects);
            await cluster.Inspect.AssertNoSplitBrainAsync();
        }, Ct);
    }

    /// <summary>
    /// A drain that cannot complete (three nodes, target three zones — no legal destination for the replicas) is exposed
    /// as a failure/progress state: the request is refused explicitly or the node stays Draining, never reaching Drained or
    /// Removed; its replicas are retained (every object keeps three verified copies); and a non-destructive removal is
    /// refused while data and voters stay intact.
    /// </summary>
    [Fact, Catalog("OPS-001", "S", "INV-08", "INV-11", "INV-13", "INV-16", "INV-17", "INV-18")]
    public async Task OPS_001_DrainNodeSafely_ImpossibleDrainReportsFailureState()
    {
        await ClusterTest.RunAsync(OpsHelpers.Nodes(3, OpsHelpers.Policy(3, 3, 2, 2)), async cluster =>
        {
            DfsAdminClient admin = cluster.Admin;
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            DfsVolume vol = client.GetVolume(Vol);
            Dictionary<string, byte[]> seeded = await OpsHelpers.SeedFilesAsync(vol, "/ops001b", 6, 96 * 1024, 110);
            await cluster.WaitForRepairAsync(Vol, TimeSpan.FromSeconds(60), Ct);
            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            TestNode target = cluster.Nodes.First(n => n.Name != leader.Name);

            Exception? refused = await Record.ExceptionAsync(() => admin.DrainNodeAsync(target.Name, Ct));
            if (refused is not null)
                OpsHelpers.Log($"drain refused explicitly: {Assert.IsAssignableFrom<DfsException>(refused).ErrorCode}");

            for (int i = 0; i < 30; i++)
            {
                DfsNodeInfo info = (await admin.ListNodesAsync(Ct)).Single(n => n.Name == target.Name);
                Assert.True(info.Status is DfsNodeStatus.Active or DfsNodeStatus.Draining,
                    $"an impossible drain must not report {info.Status}");
                if (refused is null)
                    Assert.Equal(DfsNodeStatus.Draining, info.Status);
                await cluster.DelayAsync(TimeSpan.FromSeconds(1), Ct);
            }

            foreach (ObjectId id in (await cluster.Inspect.GetVolumeStateAsync(Vol)).ReachableObjects())
                Assert.Equal((3, 3), await cluster.Inspect.CountDurableDomainsAsync(id));

            DfsException removal = await AssertEx.ThrowsDfsAsync<DfsException>(() => admin.RemoveNodeAsync(target.Name, force: false, Ct));
            Assert.Contains(removal.ErrorCode, RemovalRefusalCodes);
            DfsNodeInfo after = (await admin.ListNodesAsync(Ct)).Single(n => n.Name == target.Name);
            Assert.NotEqual(DfsNodeStatus.Removed, after.Status);
            Assert.Contains(target.Name, (await admin.GetVolumeStatusAsync(Vol, Ct)).Group.Voters.Concat(
                (await admin.GetClusterStatusAsync(Ct)).Control.Voters));
            await OpsHelpers.VerifyFilesAsync(vol, seeded, "after an impossible drain");
            await vol.Files.WriteAllBytesAsync("/ops001b/after", TestData.Bytes(111, 5000), Ct);
        }, Ct);
    }

    // ============================================================================================================
    // OPS-002 RemoveNodeRequiresSafeTransition
    // ============================================================================================================

    /// <summary>
    /// Removing the only holder of a single-copy file is refused (alive or crashed) and changes nothing; only the explicit
    /// destructive procedure (<c>force: true</c>) removes it, after which the loss is reported honestly — reads fail with an
    /// explicit integrity/availability error (never fabricated bytes) and replica health counts the missing object.
    /// </summary>
    [Fact, Catalog("OPS-002", "S", "INV-08", "INV-11", "INV-13", "INV-16", "INV-17", "INV-18")]
    public async Task OPS_002_RemoveNodeRequiresSafeTransition()
    {
        TestClusterOptions options = OpsHelpers.ThreeZones() with
        {
            Volumes = [new TestVolumeSpec(Vol) { NamedPolicies = new Dictionary<string, DfsStoragePolicy> { ["single"] = OpsHelpers.Policy(1, 1, 1, 1) } }],
        };
        await ClusterTest.RunAsync(options, async cluster =>
        {
            DfsAdminClient admin = cluster.Admin;
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0", OperationTimeout = TimeSpan.FromSeconds(5) }, Ct);
            DfsVolume vol = client.GetVolume(Vol);
            await vol.Directories.CreateDirectoryAsync("/ops002", Ct);
            byte[] content = TestData.Bytes(200, 40 * 1024);
            await vol.Files.WriteAllBytesAsync("/ops002/single", content, new DfsMutationOptions { StoragePolicy = "single" }, Ct);
            IReadOnlyList<ObjectId> objects = await OpsHelpers.ObjectsOfAsync(cluster, ["/ops002/single"]);
            ObjectId only = Assert.Single(objects);
            string holderName = Assert.Single(await cluster.Inspect.GetObjectHoldersAsync(only));
            TestNode holder = cluster.Node(holderName);

            DfsException alive = await AssertEx.ThrowsDfsAsync<DfsException>(() => admin.RemoveNodeAsync(holderName, force: false, Ct));
            Assert.Contains(alive.ErrorCode, RemovalRefusalCodes);
            Assert.Equal(DfsNodeStatus.Active, (await admin.ListNodesAsync(Ct)).Single(n => n.Name == holderName).Status);
            Assert.Contains(holderName, await cluster.Inspect.GetObjectHoldersAsync(only));
            OpsHelpers.BytesEqual(content, await vol.Files.ReadAllBytesAsync("/ops002/single", Ct), "after a refused removal");

            await holder.CrashAsync();
            DfsException crashed = await AssertEx.ThrowsDfsAsync<DfsException>(() => admin.RemoveNodeAsync(holderName, force: false, Ct));
            Assert.Contains(crashed.ErrorCode, RemovalRefusalCodes);
            Assert.NotEqual(DfsNodeStatus.Removed, (await admin.ListNodesAsync(Ct)).Single(n => n.Name == holderName).Status);

            await admin.RemoveNodeAsync(holderName, force: true, Ct);
            await cluster.WaitUntilAsync(async () => (await admin.ListNodesAsync(Ct)).All(n => n.Name != holderName || n.Status == DfsNodeStatus.Removed),
                TimeSpan.FromSeconds(30), "the explicit destructive removal must take effect", Ct);

            Exception? read = await Record.ExceptionAsync(async () =>
            {
                byte[] bytes = await vol.Files.ReadAllBytesAsync("/ops002/single", Ct);
                OpsHelpers.BytesEqual(content, bytes, "a read after losing the only copy may only return the original bytes");
            });
            Assert.True(read is DfsIntegrityException or DfsUnavailableException,
                $"losing the only copy must surface as an explicit integrity/availability error, got {read?.GetType().Name ?? "success"}");
            await cluster.WaitUntilAsync(async () => (await admin.GetReplicaHealthAsync(Vol, Ct)).MissingObjects >= 1,
                TimeSpan.FromSeconds(30), "the lost object must be reported as missing", Ct);
        }, Ct);
    }

    /// <summary>
    /// With one of three voters down, removing another live voter (or the leader) would leave a configuration without a
    /// reachable majority and is refused; voters, quorum and writes are unaffected. The documented destructive procedure
    /// (force-removing the dead voter) succeeds and leaves a working two-voter configuration; the removed node later
    /// returns but never regains a vote or leadership.
    /// </summary>
    [Fact, Catalog("OPS-002", "S", "INV-08", "INV-11", "INV-13", "INV-16", "INV-17", "INV-18")]
    public async Task OPS_002_RemoveNodeRequiresSafeTransition_UnsafeVoterCombination()
    {
        await ClusterTest.RunAsync(OpsHelpers.ThreeZones(), async cluster =>
        {
            DfsAdminClient admin = cluster.Admin;
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            DfsVolume vol = client.GetVolume(Vol);
            Dictionary<string, byte[]> seeded = await OpsHelpers.SeedFilesAsync(vol, "/ops002v", 4, 20_000, 210);
            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            TestNode down = cluster.Nodes.First(n => n.Name != leader.Name);
            TestNode other = cluster.Nodes.First(n => n.Name != leader.Name && n.Name != down.Name);
            await down.CrashAsync();

            foreach (TestNode victim in new[] { other, leader })
            {
                DfsException refused = await AssertEx.ThrowsDfsAsync<DfsException>(() => admin.RemoveNodeAsync(victim.Name, force: false, Ct));
                Assert.Contains(refused.ErrorCode, RemovalRefusalCodes);
                DfsVolumeStatus status = await admin.GetVolumeStatusAsync(Vol, Ct);
                Assert.Equal(3, status.Group.Voters.Count);
                Assert.Contains(victim.Name, status.Group.Voters);
                Assert.True(status.Group.HasQuorum);
                await vol.Files.WriteAllBytesAsync($"/ops002v/after-refusal-{victim.Name}", TestData.Bytes(211, 3000), Ct);
            }

            await admin.RemoveNodeAsync(down.Name, force: true, Ct);
            await cluster.WaitUntilAsync(async () =>
            {
                DfsVolumeStatus status = await admin.GetVolumeStatusAsync(Vol, Ct);
                return !status.Group.Voters.Contains(down.Name) && status.Group.HasQuorum;
            }, TimeSpan.FromSeconds(30), "the force-removed voter must leave the volume configuration", Ct);
            await vol.Files.WriteAllBytesAsync("/ops002v/after-force", TestData.Bytes(212, 3000), Ct);
            await OpsHelpers.VerifyFilesAsync(vol, seeded, "after the destructive voter removal");

            Exception? rejoin = await Record.ExceptionAsync(() => down.RestartAsync());
            if (rejoin is not null)
                OpsHelpers.Log($"removed node refused to restart: {rejoin.GetType().Name}");
            for (int i = 0; i < 10; i++)
            {
                await cluster.DelayAsync(TimeSpan.FromSeconds(1), Ct);
                DfsVolumeStatus status = await admin.GetVolumeStatusAsync(Vol, Ct);
                Assert.DoesNotContain(down.Name, status.Group.Voters);
                Assert.NotEqual(down.Name, status.Group.Leader);
                Assert.DoesNotContain(down.Name, (await admin.GetClusterStatusAsync(Ct)).Control.Voters);
                Assert.Equal(DfsNodeStatus.Removed, (await admin.ListNodesAsync(Ct)).Where(n => n.Name == down.Name).Select(n => n.Status).DefaultIfEmpty(DfsNodeStatus.Removed).Single());
                await cluster.Inspect.AssertNoSplitBrainAsync();
            }

            await OpsHelpers.VerifyFilesAsync(vol, seeded, "after the removed node returned");
        }, Ct);
    }

    // ============================================================================================================
    // OPS-003 MetricsReflectDurableReality
    // ============================================================================================================

    /// <summary>
    /// Reported replica health, quorum and backup checkpoints are compared with durable reality. A node that stays
    /// connected but holds a corrupted copy and has silently lost another (and whose repair writes are held) must be reported as under-replication exactly matching the independently counted verified copies; voter
    /// lists and quorum match the configuration (an unreachable voter is not dropped from the denominator); backup status
    /// reports exactly the snapshot the backup node durably published, and does not advance while the backup is paused.
    /// </summary>
    [Fact, Catalog("OPS-003", "S", "INV-08", "INV-11", "INV-13", "INV-16", "INV-17", "INV-18")]
    public async Task OPS_003_MetricsReflectDurableReality()
    {
        await ClusterTest.RunAsync(OpsHelpers.ThreeZones(backupNodes: 1), async cluster =>
        {
            DfsAdminClient admin = cluster.Admin;
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            DfsVolume vol = client.GetVolume(Vol);
            Dictionary<string, byte[]> seeded = await OpsHelpers.SeedFilesAsync(vol, "/ops003", 8, 128 * 1024, 300);
            await cluster.WaitForRepairAsync(Vol, TimeSpan.FromSeconds(60), Ct);
            DfsStoragePolicy policy = (await vol.GetInfoAsync(Ct)).DefaultPolicy;

            (DfsReplicaHealth healthy, IndependentHealth actual) = await OpsHelpers.AssertHealthMatchesRealityAsync(cluster, admin, policy, "baseline");
            Assert.Equal(0, actual.UnderReplicated);
            Assert.Equal(0, healthy.MissingObjects);

            // Quorum and voter reporting match the configuration.
            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            DfsVolumeStatus volumeStatus = await admin.GetVolumeStatusAsync(Vol, Ct);
            Assert.Equal(cluster.Nodes.Select(n => n.Name).Order(StringComparer.Ordinal), volumeStatus.Group.Voters.Order(StringComparer.Ordinal));
            Assert.Equal(leader.Name, volumeStatus.Group.Leader);
            Assert.True(volumeStatus.Group.HasQuorum);

            // Backup checkpoint reporting matches what the backup node durably published.
            TestBackupNode b0 = cluster.BackupNodes[0];
            await b0.RegisterAsync(Vol);
            DfsSnapshotInfo s1 = await vol.Snapshots.CreateAsync("ops003-s1", null, Ct);
            await vol.Backups.RequestSyncAsync(b0.TargetName, "ops003-s1", Ct);   // registered without an automatic schedule
            DfsBackupCheckpoint cp1 = await vol.Backups.AwaitBackupSnapshotAsync(b0.TargetName, "ops003-s1", TimeSpan.FromSeconds(60), Ct);
            Assert.Equal(s1.Boundary, cp1.CoveredCommit);
            Assert.True(cp1.IsExactSnapshot);
            DfsBackupStatus backupStatus = await vol.Backups.GetStatusAsync(b0.TargetName, Ct);
            Assert.Equal("ops003-s1", backupStatus.LatestPublished?.SnapshotName);
            Assert.Equal(s1.Boundary, backupStatus.PublishedBoundary);
            Assert.Contains("ops003-s1", await b0.Inspect.GetPublishedSnapshotsAsync(Vol));

            // Silent damage on a node that stays connected and cannot be repaired.
            IReadOnlyList<ObjectId> objects = await OpsHelpers.ObjectsOfAsync(cluster, ["/ops003/f0", "/ops003/f1"]);
            ObjectId corrupted = objects[0];
            ObjectId lost = objects[^1];
            TestNode damaged = cluster.Nodes.First(n => n.Name != leader.Name);
            Assert.Contains(damaged.Name, await cluster.Inspect.GetObjectHoldersAsync(corrupted));
            Assert.Contains(damaged.Name, await cluster.Inspect.GetObjectHoldersAsync(lost));
            // Repair writes to the damaged node are held so the deficit persists while the node stays connected and healthy-looking.
            FaultArm repairBlock = cluster.Faults.Arm(
                new FaultMatch(FaultSites.ObjectWrite, Node: damaged.Name, Operation: DfsOperationKind.Repair, Count: 1_000_000), FaultAction.Block());
            await damaged.Disk.CorruptObjectAsync(corrupted);
            await damaged.Disk.DeleteObjectAsync(lost);
            await admin.StartScrubAsync(damaged.Name, Ct);

            Assert.All(await admin.ListNodesAsync(Ct), n => Assert.True(n.IsReachable, $"{n.Name} stays connected"));
            await cluster.WaitUntilAsync(async () => (await admin.GetReplicaHealthAsync(Vol, Ct)).UnderReplicatedObjects >= 2,
                TimeSpan.FromSeconds(60), "corrupted and silently lost replicas on a connected node must be reported", Ct);
            (DfsReplicaHealth degraded, IndependentHealth reality) = await OpsHelpers.AssertHealthMatchesRealityAsync(cluster, admin, policy, "damaged replicas");
            Assert.True(reality.UnderReplicated >= 2);
            Assert.Equal((2, 2), await cluster.Inspect.CountDurableDomainsAsync(corrupted));
            Assert.Equal((2, 2), await cluster.Inspect.CountDurableDomainsAsync(lost));
            Assert.True(degraded.UnderReplicatedObjects >= 2);

            // An unreachable voter is still counted in the configuration.
            TestNode follower = cluster.Nodes.First(n => n.Name != leader.Name && n.Name != damaged.Name);
            await follower.CrashAsync();
            await cluster.WaitUntilAsync(async () => !(await admin.ListNodesAsync(Ct)).Single(n => n.Name == follower.Name).IsReachable,
                TimeSpan.FromSeconds(30), "a crashed node must be reported unreachable", Ct);
            DfsVolumeStatus withCrash = await admin.GetVolumeStatusAsync(Vol, Ct);
            Assert.Equal(3, withCrash.Group.Voters.Count);
            Assert.Contains(follower.Name, withCrash.Group.Voters);
            Assert.True(withCrash.Group.HasQuorum);
            await follower.RestartAsync();

            // Backup status does not advance while the backup host is paused, whatever connection state it shows.
            await b0.PauseAsync();
            await vol.Files.WriteAllBytesAsync("/ops003/later", TestData.Bytes(301, 64 * 1024), Ct);
            await vol.Snapshots.CreateAsync("ops003-s2", null, Ct);
            await vol.Backups.RequestSyncAsync(b0.TargetName, "ops003-s2", Ct);
            await cluster.DelayAsync(TimeSpan.FromSeconds(10), Ct);
            DfsBackupStatus paused = await vol.Backups.GetStatusAsync(b0.TargetName, Ct);
            Assert.Equal("ops003-s1", paused.LatestPublished?.SnapshotName);
            Assert.Equal(s1.Boundary, paused.PublishedBoundary);
            await b0.ResumeAsync();
            DfsBackupCheckpoint cp2 = await vol.Backups.AwaitBackupSnapshotAsync(b0.TargetName, "ops003-s2", TimeSpan.FromSeconds(60), Ct);
            Assert.Contains("ops003-s2", await b0.Inspect.GetPublishedSnapshotsAsync(Vol));
            Assert.Equal(cp2.CoveredCommit, (await vol.Backups.GetStatusAsync(b0.TargetName, Ct)).PublishedBoundary);

            // Once repair may write again the deficit is repaired and the report returns to healthy.
            cluster.Faults.AssertReached(repairBlock);
            repairBlock.Disarm();
            repairBlock.Release();
            await cluster.WaitForRepairAsync(Vol, TimeSpan.FromSeconds(60), Ct);
            (_, IndependentHealth repaired) = await OpsHelpers.AssertHealthMatchesRealityAsync(cluster, admin, policy, "after repair");
            Assert.Equal(0, repaired.UnderReplicated);
            await OpsHelpers.VerifyFilesAsync(vol, seeded, "after repair");
        }, Ct);
    }

    // ============================================================================================================
    // OPS-004 DegradedStateVisible
    // ============================================================================================================

    /// <summary>
    /// With one of three zones crashed, isolated or paused, writes still succeed under the ack-2 policy, but every new
    /// object is reported as under-replicated and zone-deficient — at every observation while degraded, never as full
    /// durability — and the absent node is flagged. After the node returns, repair restores three copies and the report
    /// returns to zero deficits.
    /// </summary>
    [Theory, Catalog("OPS-004", "S", "INV-08", "INV-11", "INV-13", "INV-16", "INV-17", "INV-18")]
    [InlineData("crash")]
    [InlineData("isolate")]
    [InlineData("pause")]
    public async Task OPS_004_DegradedStateVisible(string mode)
    {
        await ClusterTest.RunAsync(OpsHelpers.ThreeZones(), async cluster =>
        {
            DfsAdminClient admin = cluster.Admin;
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            DfsVolume vol = client.GetVolume(Vol);
            await vol.Directories.CreateDirectoryAsync("/ops004", Ct);
            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            TestNode victim = cluster.Nodes.First(n => n.Name != leader.Name);

            switch (mode)
            {
                case "crash":
                    await victim.CrashAsync();
                    break;
                case "isolate":
                    cluster.Network.Isolate(victim.Name);
                    break;
                default:
                    await victim.PauseAsync();
                    break;
            }

            var written = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            for (int i = 0; i < 5; i++)
            {
                string path = $"/ops004/f{i}";
                byte[] content = TestData.Bytes(400 + (ulong)i, 128 * 1024);
                await vol.Files.WriteAllBytesAsync(path, content, Ct);
                written[path] = content;
            }

            IReadOnlyList<ObjectId> newObjects = await OpsHelpers.ObjectsOfAsync(cluster, written.Keys);
            foreach (ObjectId id in newObjects)
            {
                (int copies, int zones) = await cluster.Inspect.CountDurableDomainsAsync(id);
                Assert.True(copies >= 2 && zones >= 2, $"acknowledged object {id} must meet the ack policy (has {copies}/{zones})");
                Assert.DoesNotContain(victim.Name, await cluster.Inspect.GetObjectHoldersAsync(id));
            }

            await cluster.WaitUntilAsync(async () => !(await admin.ListNodesAsync(Ct)).Single(n => n.Name == victim.Name).IsReachable,
                TimeSpan.FromSeconds(30), $"the {mode}d node must be reported unreachable", Ct);
            for (int observation = 0; observation < 10; observation++)
            {
                DfsReplicaHealth health = await admin.GetReplicaHealthAsync(Vol, Ct);
                Assert.True(health.UnderReplicatedObjects >= newObjects.Count,
                    $"observation {observation}: {health.UnderReplicatedObjects} under-replicated reported, {newObjects.Count} objects have only two copies");
                Assert.True(health.ZoneDeficitObjects >= newObjects.Count,
                    $"observation {observation}: {health.ZoneDeficitObjects} zone-deficit reported, {newObjects.Count} objects span two zones");
                DfsVolumeStatus status = await admin.GetVolumeStatusAsync(Vol, Ct);
                Assert.True(status.Replicas.UnderReplicatedObjects >= newObjects.Count);
                await cluster.DelayAsync(TimeSpan.FromMilliseconds(500), Ct);
            }

            switch (mode)
            {
                case "crash":
                    await victim.RestartAsync();
                    break;
                case "isolate":
                    cluster.Network.Heal();
                    break;
                default:
                    await victim.ResumeAsync();
                    break;
            }

            await cluster.WaitForRepairAsync(Vol, TimeSpan.FromSeconds(60), Ct);
            foreach (ObjectId id in newObjects)
                Assert.Equal((3, 3), await cluster.Inspect.CountDurableDomainsAsync(id));
            DfsReplicaHealth repaired = await admin.GetReplicaHealthAsync(Vol, Ct);
            Assert.Equal(0, repaired.UnderReplicatedObjects);
            Assert.Equal(0, repaired.ZoneDeficitObjects);
            await OpsHelpers.VerifyFilesAsync(vol, written, "after repair");
        }, Ct);
    }

    // ============================================================================================================
    // OPS-005 RepairAndGcBacklogVisible
    // ============================================================================================================

    /// <summary>
    /// Maintenance is delayed on every axis at once — GC sweeps blocked, a node down, a backup paused mid-transfer, a
    /// writer holding uploaded-but-unpublished chunks and a pinned reader holding an overwritten revision. Staged bytes,
    /// pinned bytes, pending-GC objects, under-replication/repair backlog and backup pins all grow to at least their
    /// independently computed values; when the delays are lifted every one of them drains to zero and the deleted objects
    /// are physically reclaimed.
    /// </summary>
    [Fact, Catalog("OPS-005", "S", "INV-08", "INV-11", "INV-13", "INV-16", "INV-17", "INV-18")]
    public async Task OPS_005_RepairAndGcBacklogVisible()
    {
        await ClusterTest.RunAsync(OpsHelpers.ThreeZones(backupNodes: 1), async cluster =>
        {
            DfsAdminClient admin = cluster.Admin;
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            DfsVolume vol = client.GetVolume(Vol);
            await vol.Directories.CreateDirectoryAsync("/ops005", Ct);
            TestBackupNode b0 = cluster.BackupNodes[0];
            await b0.RegisterAsync(Vol);

            FaultArm gcBlock = cluster.Faults.Arm(new FaultMatch(FaultSites.GcSweep, Count: 1_000_000), FaultAction.Block());

            // Pending GC: deleted files whose objects cannot be swept yet.
            var doomed = new List<string>();
            for (int i = 0; i < 6; i++)
            {
                string path = $"/ops005/doomed{i}";
                await vol.Files.WriteAllBytesAsync(path, TestData.Bytes(500 + (ulong)i, 64 * 1024 + 1), Ct);
                doomed.Add(path);
            }

            IReadOnlyList<ObjectId> doomedObjects = await OpsHelpers.ObjectsOfAsync(cluster, doomed);
            foreach (string path in doomed)
                await vol.Files.DeleteAsync(path, Ct);

            // Pinned bytes: a version-pinned reader keeps the overwritten revision alive.
            await vol.Files.WriteAllBytesAsync("/ops005/pinned", TestData.Bytes(510, 256 * 1024), Ct);
            DfsStream pinnedReader = await vol.Files.OpenAsync("/ops005/pinned",
                new DfsOpenOptions { Mode = FileMode.Open, Access = FileAccess.Read, Share = FileShare.ReadWrite | FileShare.Delete, ReadView = DfsReadView.VersionPinned }, Ct);
            await vol.Files.WriteAllBytesAsync("/ops005/pinned", TestData.Bytes(511, 256 * 1024), Ct);

            // Staged bytes: uploaded chunks of an unflushed writer.
            DfsStream staged = await vol.Files.OpenAsync("/ops005/staged", FileMode.Create, FileAccess.Write, FileShare.Read, Ct);
            byte[] stagedContent = TestData.Bytes(512, 320 * 1024);
            for (int offset = 0; offset < stagedContent.Length; offset += 64 * 1024)
                await staged.WriteAsync(stagedContent.AsMemory(offset, 64 * 1024), Ct);
            Assert.Equal(stagedContent.Length, staged.PendingBytes);

            // Missing replicas: writes while a node is down.
            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            TestNode down = cluster.Nodes.First(n => n.Name != leader.Name);
            await down.CrashAsync();
            var degradedFiles = new List<string>();
            for (int i = 0; i < 4; i++)
            {
                string path = $"/ops005/degraded{i}";
                await vol.Files.WriteAllBytesAsync(path, TestData.Bytes(520 + (ulong)i, 128 * 1024), Ct);
                degradedFiles.Add(path);
            }

            IReadOnlyList<ObjectId> degradedObjects = await OpsHelpers.ObjectsOfAsync(cluster, degradedFiles);
            long degradedBytes = 0;
            foreach (string path in degradedFiles)
                degradedBytes += (await cluster.Inspect.GetFileAsync(Vol, path))!.Extents.Sum(e => e.ObjectLength);

            // Transfer backlog: a snapshot export to a paused backup host.
            await b0.PauseAsync();
            await vol.Snapshots.CreateAsync("ops005-s1", null, Ct);
            await vol.Backups.RequestSyncAsync(b0.TargetName, "ops005-s1", Ct);

            await cluster.DelayAsync(TimeSpan.FromSeconds(3), Ct);
            DfsReplicaHealth grown = await admin.GetReplicaHealthAsync(Vol, Ct);
            OpsHelpers.Log($"backlog: staged={grown.StagedBytes} pinned={grown.PinnedBytes} pendingGc={grown.PendingGcObjects} under={grown.UnderReplicatedObjects} repairBacklog={grown.RepairBacklogBytes}");
            cluster.Faults.AssertReached(gcBlock);

            // Staged objects were uploaded before `down` crashed, so their copies on it still exist durably: the reported
            // value lies between the distinct staged bytes and those bytes times the copy target (every replica counted).
            long stagedDistinct = await StagedObjectBytesAsync(cluster);
            DfsStoragePolicy stagedPolicy = (await vol.GetInfoAsync(Ct)).DefaultPolicy;
            Assert.True(stagedDistinct >= 192 * 1024, $"at least 192 KiB of the 320 KiB written must have been uploaded as staged objects (found {stagedDistinct})");
            Assert.InRange(grown.StagedBytes, stagedDistinct, stagedDistinct * stagedPolicy.TargetCopies);
            Assert.True(grown.PinnedBytes >= 256 * 1024, $"the pinned revision (256 KiB) must be counted, reported {grown.PinnedBytes}");
            Assert.True(grown.PendingGcObjects >= doomedObjects.Count, $"{doomedObjects.Count} deleted objects await GC, reported {grown.PendingGcObjects}");
            Assert.True(grown.UnderReplicatedObjects >= degradedObjects.Count, $"{degradedObjects.Count} objects lack a copy, reported {grown.UnderReplicatedObjects}");
            Assert.True(grown.RepairBacklogBytes >= degradedBytes, $"{degradedBytes} bytes need repair, reported {grown.RepairBacklogBytes}");
            DfsBackupStatus backlog = await vol.Backups.GetStatusAsync(b0.TargetName, Ct);
            Assert.True(backlog.PinnedSourceBytes > 0, "the export to the paused backup must hold a reported transfer pin");
            Assert.NotEqual("ops005-s1", backlog.LatestPublished?.SnapshotName);
            foreach (ObjectId id in doomedObjects)
                Assert.Contains(id, await cluster.Inspect.GetAllStoredObjectsAsync(Vol));

            // Lift every delay.
            await staged.FlushAsync(Ct);
            await staged.DisposeAsync();
            await pinnedReader.DisposeAsync();
            gcBlock.Disarm();
            gcBlock.Release();
            await down.RestartAsync();
            await b0.ResumeAsync();
            await vol.Backups.AwaitBackupSnapshotAsync(b0.TargetName, "ops005-s1", TimeSpan.FromSeconds(60), Ct);
            await cluster.WaitForRepairAsync(Vol, TimeSpan.FromSeconds(60), Ct);

            await cluster.WaitUntilAsync(async () =>
            {
                DfsReplicaHealth h = await admin.GetReplicaHealthAsync(Vol, Ct);
                DfsBackupStatus b = await vol.Backups.GetStatusAsync(b0.TargetName, Ct);
                return h.StagedBytes == 0 && h.PinnedBytes == 0 && h.PendingGcObjects == 0 && h.UnderReplicatedObjects == 0
                    && h.MissingObjects == 0 && h.RepairBacklogBytes == 0 && b.PinnedSourceBytes == 0;
            }, TimeSpan.FromSeconds(60), "every backlog metric must clear once maintenance can run", Ct);

            IReadOnlySet<ObjectId> stored = await cluster.Inspect.GetAllStoredObjectsAsync(Vol);
            foreach (ObjectId id in doomedObjects)
                Assert.DoesNotContain(id, stored);
            OpsHelpers.BytesEqual(stagedContent, await vol.Files.ReadAllBytesAsync("/ops005/staged", Ct), "flushed staged file");
        }, Ct);
    }

    // ============================================================================================================
    // OPS-006 BackupRecoveryAgeNotTransferAge
    // ============================================================================================================

    /// <summary>
    /// After S1 is published, megabytes of S2 are transferred but publication is blocked just before the pointer update.
    /// Throughout, the latest recoverable boundary stays S1 and its age keeps growing with time (≥ time since S1), even
    /// though received bytes grow; a commit-based barrier for S2 times out reporting S1 as the last published checkpoint.
    /// Once publication proceeds, the boundary becomes S2 and the age resets relative to S2.
    /// </summary>
    [Fact, Catalog("OPS-006", "S", "INV-08", "INV-11", "INV-13", "INV-16", "INV-17", "INV-18")]
    public async Task OPS_006_BackupRecoveryAgeNotTransferAge()
    {
        await ClusterTest.RunAsync(OpsHelpers.ThreeZones(backupNodes: 1), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            DfsVolume vol = client.GetVolume(Vol);
            TestBackupNode b0 = cluster.BackupNodes[0];
            await b0.RegisterAsync(Vol);
            await OpsHelpers.SeedFilesAsync(vol, "/ops006/a", 4, 64 * 1024, 600);
            DfsSnapshotInfo s1 = await vol.Snapshots.CreateAsync("ops006-s1", null, Ct);
            await vol.Backups.RequestSyncAsync(b0.TargetName, "ops006-s1", Ct);   // registered without an automatic schedule
            DfsBackupCheckpoint cp1 = await vol.Backups.AwaitBackupSnapshotAsync(b0.TargetName, "ops006-s1", TimeSpan.FromSeconds(60), Ct);

            await cluster.DelayAsync(TimeSpan.FromSeconds(30), Ct);
            DfsBackupStatus aged = await vol.Backups.GetStatusAsync(b0.TargetName, Ct);
            Assert.Equal("ops006-s1", aged.LatestPublished?.SnapshotName);
            Assert.NotNull(aged.RecoveryPointAge);
            Assert.True(aged.RecoveryPointAge >= TimeSpan.FromSeconds(29), $"age after 30 s must be at least ~30 s, was {aged.RecoveryPointAge}");

            FaultArm publish = cluster.Faults.Arm(new FaultMatch(FaultSites.BackupBeforePointer, Node: b0.Name), FaultAction.Block());
            await OpsHelpers.SeedFilesAsync(vol, "/ops006/b", 16, 128 * 1024, 610);
            DfsSnapshotInfo s2 = await vol.Snapshots.CreateAsync("ops006-s2", null, Ct);
            await vol.Backups.RequestSyncAsync(b0.TargetName, "ops006-s2", Ct);
            await publish.WaitTriggeredAsync(TimeSpan.FromSeconds(120));
            cluster.Faults.AssertReached(publish);

            DfsBackupStatus transferring = await vol.Backups.GetStatusAsync(b0.TargetName, Ct);
            // Publication is blocked after every object of S2 was received and verified, so the 2 MiB of new data are counted
            // (whether ReceivedBytes is cumulative or counts the current transfer only).
            Assert.True(transferring.ReceivedBytes > aged.ReceivedBytes && transferring.ReceivedBytes >= 2L * 1024 * 1024,
                $"the 2 MiB of S2 must be reported as received ({aged.ReceivedBytes} -> {transferring.ReceivedBytes})");
            Assert.Equal("ops006-s1", transferring.LatestPublished?.SnapshotName);
            Assert.Equal(cp1.CoveredCommit, transferring.PublishedBoundary);
            TimeSpan sinceS1 = cluster.Time.GetUtcNow() - s1.CreatedUtc;
            Assert.True(transferring.RecoveryPointAge >= sinceS1 - TimeSpan.FromSeconds(2),
                $"recovery-point age {transferring.RecoveryPointAge} must not reset on transfer activity (S1 is {sinceS1} old)");

            await cluster.DelayAsync(TimeSpan.FromSeconds(20), Ct);
            DfsBackupStatus stillOld = await vol.Backups.GetStatusAsync(b0.TargetName, Ct);
            Assert.Equal("ops006-s1", stillOld.LatestPublished?.SnapshotName);
            Assert.True(stillOld.RecoveryPointAge >= transferring.RecoveryPointAge + TimeSpan.FromSeconds(19),
                $"age must keep growing while S2 is unpublished ({transferring.RecoveryPointAge} -> {stillOld.RecoveryPointAge})");

            DfsBackupNotReachedException notReached = await AssertEx.ThrowsDfsAsync<DfsBackupNotReachedException>(
                () => vol.Backups.AwaitBackupAsync(b0.TargetName, s2.Boundary, TimeSpan.FromSeconds(1), Ct));
            Assert.Equal("ops006-s1", notReached.LastPublished?.SnapshotName);

            publish.Release();
            publish.Disarm();
            DfsBackupCheckpoint cp2 = await vol.Backups.AwaitBackupSnapshotAsync(b0.TargetName, "ops006-s2", TimeSpan.FromSeconds(60), Ct);
            Assert.Equal(s2.Boundary, cp2.CoveredCommit);
            DfsBackupStatus fresh = await vol.Backups.GetStatusAsync(b0.TargetName, Ct);
            Assert.Equal("ops006-s2", fresh.LatestPublished?.SnapshotName);
            TimeSpan sinceS2 = cluster.Time.GetUtcNow() - s2.CreatedUtc;
            Assert.True(fresh.RecoveryPointAge <= sinceS2 + TimeSpan.FromSeconds(2),
                $"after S2 is published the age ({fresh.RecoveryPointAge}) must be measured from S2 ({sinceS2})");
            Assert.True(fresh.RecoveryPointAge < stillOld.RecoveryPointAge);
        }, Ct);
    }

    // ============================================================================================================
    // OPS-008 GracefulShutdownDrainsSafely / OPS-009 AbruptShutdownStillSafe
    // ============================================================================================================

    /// <summary>
    /// Three clients run register writes (8 KiB, extent-based) and appends with persisted operation ids; the leader or a
    /// follower is stopped gracefully mid-flight. Every acknowledged write is durable (meets the ack policy and survives the
    /// restart), every unfinished operation resolves to a definite outcome consistent with the data, the full history is
    /// linearizable, and replicas converge after the node returns.
    /// </summary>
    [Theory, Catalog("OPS-008", "S", "INV-08", "INV-11", "INV-13", "INV-16", "INV-17", "INV-18")]
    [InlineData("leader")]
    [InlineData("follower")]
    public async Task OPS_008_GracefulShutdownDrainsSafely(string role)
    {
        await ClusterTest.RunAsync(OpsHelpers.ThreeZones(), async cluster =>
        {
            await RunShutdownScenarioAsync(cluster, role, async node => await node.StopAsync(), restartAll: false);
        }, Ct);
    }

    /// <summary>
    /// As OPS-008 but with no drain or cleanup at all: the leader, a follower, or every node crashes abruptly with each
    /// crash mode for unflushed writes. Reads during partial recovery are part of the checked history; after recovery every
    /// acknowledged write survives, unknown outcomes resolve definitely, the history is linearizable and replicas converge.
    /// </summary>
    [Theory, Catalog("OPS-009", "S", "INV-08", "INV-11", "INV-13", "INV-16", "INV-17", "INV-18")]
    [InlineData("leader", CrashMode.LoseUnflushed)]
    [InlineData("leader", CrashMode.TearLastWrite)]
    [InlineData("follower", CrashMode.RandomSubset)]
    [InlineData("follower", CrashMode.KeepUnflushed)]
    [InlineData("all", CrashMode.LoseUnflushed)]
    [InlineData("all", CrashMode.RandomSubset)]
    [InlineData("all", CrashMode.TearLastWrite)]
    [InlineData("all", CrashMode.KeepUnflushed)]
    public async Task OPS_009_AbruptShutdownStillSafe(string role, CrashMode mode)
    {
        await ClusterTest.RunAsync(OpsHelpers.ThreeZones(), async cluster =>
        {
            await RunShutdownScenarioAsync(cluster, role, async node => await node.CrashAsync(mode), restartAll: role == "all");
        }, Ct);
    }

    private static async Task RunShutdownScenarioAsync(TestCluster cluster, string role, Func<TestNode, Task> shutdown, bool restartAll)
    {
        var clients = new List<DfsClient>();
        for (int i = 0; i < 3; i++)
            clients.Add(await cluster.ConnectAsync(new TestClientOptions { Name = $"c{i}", OperationTimeout = TimeSpan.FromSeconds(5) }, Ct));
        DfsClient reader = await cluster.ConnectAsync(new TestClientOptions { Name = "r0", OperationTimeout = TimeSpan.FromSeconds(5) }, Ct);
        try
        {
            await clients[0].GetVolume(Vol).Directories.CreateDirectoryAsync("/shutdown", Ct);
            var workload = new HistoryWorkload(cluster, "/shutdown");
            string[] keys = ["k0", "k1", "k2", "log0"];
            workload.Start([(clients[0], "k0"), (clients[1], "k1"), (clients[2], "k2"), (clients[0], "log0")], reader,
                valueBytes: 8 * 1024, pause: TimeSpan.FromMilliseconds(20), maxOpsPerWriter: 30);

            await cluster.DelayAsync(TimeSpan.FromMilliseconds(300), Ct);
            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            List<TestNode> victims = role switch
            {
                "leader" => [leader],
                "follower" => [cluster.Nodes.First(n => n.Name != leader.Name)],
                _ => cluster.Nodes.ToList(),
            };
            foreach (TestNode victim in victims)
                await shutdown(victim);

            if (restartAll)
            {
                // Safety during recovery: restart a majority first and read through it before the last node returns.
                await victims[0].RestartAsync();
                await victims[1].RestartAsync();
                await cluster.WaitUntilAsync(async () =>
                {
                    try
                    {
                        await reader.GetVolume(Vol).Directories.ExistsAsync("/shutdown", Ct);
                        return true;
                    }
                    catch (Exception ex) when (OpsHelpers.IsAvailabilityOrUnknown(ex))
                    {
                        return false;
                    }
                }, TimeSpan.FromSeconds(60), "a restarted majority must serve again", Ct);
                foreach (string key in keys)
                    await workload.ReadAsync(reader, key, "recovering");
                await victims[2].RestartAsync();
            }
            else
            {
                await cluster.DelayAsync(TimeSpan.FromSeconds(2), Ct);
                await victims[0].RestartAsync();
            }

            await workload.CompletionAsync();
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), Ct);
            await workload.ResolveUnknownsAsync(reader);
            await workload.FinalReadsAsync(reader, keys);
            workload.AssertLinearizable();
            Assert.True(workload.OkWrites > 0, "some writes must have been acknowledged");

            // Acknowledged data meets its durability policy on durable images.
            foreach (string key in keys.Where(k => !k.StartsWith("log", StringComparison.Ordinal)))
            {
                FileInspection? file = await cluster.Inspect.GetFileAsync(Vol, workload.PathOf(key));
                if (file is null)
                    continue;
                foreach (ExtentInspection extent in file.Extents)
                {
                    (int copies, int zones) = await cluster.Inspect.CountDurableDomainsAsync(extent.Object);
                    Assert.True(copies >= 2 && zones >= 2, $"{key}: committed object {extent.Object} has {copies} copies in {zones} zones");
                }
            }

            await cluster.WaitForReplicationAsync(Vol, Ct);
            await cluster.Inspect.AssertReplicasConvergedAsync(Vol);
            await cluster.Inspect.AssertNoSplitBrainAsync();
        }
        finally
        {
            foreach (DfsClient c in clients)
                await c.DisposeAsync();
            await reader.DisposeAsync();
        }
    }

    // ============================================================================================================
    // OPS-010 LoadSheddingBeforeReserveExhaustion
    // ============================================================================================================

    /// <summary>
    /// On 48 MiB nodes with an 8 MiB reserve, sequential and then concurrent 1 MiB writes are shed with
    /// <see cref="DfsCapacityException"/> (retryable) before the reserve is consumed: at least half the reserve remains free
    /// on every node, every acknowledged file is byte-exact and meets its ack policy, metadata operations and crash recovery
    /// (journal replay) still work, and after deletions and GC new writes are admitted again.
    /// </summary>
    [Fact, Catalog("OPS-010", "S", "INV-08", "INV-11", "INV-13", "INV-16", "INV-17", "INV-18")]
    public async Task OPS_010_LoadSheddingBeforeReserveExhaustion()
    {
        const long capacity = 48L << 20;
        const long reserve = 8L << 20;
        TestClusterOptions options = OpsHelpers.ThreeZones() with { NodeCapacityBytes = capacity, ReservedBytes = reserve };
        await ClusterTest.RunAsync(options, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0", OperationTimeout = TimeSpan.FromSeconds(10) }, Ct);
            DfsVolume vol = client.GetVolume(Vol);
            await vol.Directories.CreateDirectoryAsync("/ops010", Ct);

            var acked = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            DfsUnavailableException? shed = null;
            for (int i = 0; i < 64 && shed is null; i++)
            {
                string path = $"/ops010/f{i}";
                byte[] content = TestData.Bytes(1000 + (ulong)i, 1 << 20);
                try
                {
                    await vol.Files.WriteAllBytesAsync(path, content, Ct);
                    acked[path] = content;
                }
                catch (DfsUnavailableException ex) when (ex is DfsCapacityException or DfsPlacementUnavailableException)
                {
                    // Admission control: an explicit capacity rejection (or no eligible placement target left outside the reserve).
                    shed = ex;
                }
            }

            Assert.NotNull(shed);
            Assert.Contains(shed.ErrorCode, new[] { DfsErrorCode.CapacityExhausted, DfsErrorCode.PlacementUnavailable });
            Assert.True(shed.IsRetryable);
            Assert.True(acked.Count <= (capacity - reserve) >> 20, $"{acked.Count} MiB admitted per node exceeds the {(capacity - reserve) >> 20} MiB outside the reserve");
            OpsHelpers.Log($"admitted {acked.Count} x 1 MiB before shedding");
            foreach (TestNode node in cluster.Nodes)
                Assert.True(node.Disk.UsedBytes <= capacity - reserve / 2, $"{node.Name} used {node.Disk.UsedBytes} bytes, eating into the reserve");

            // Concurrent saturation: each request either succeeds durably or is shed with a retryable availability error.
            var attempts = new List<Task<(string Path, byte[] Content, Exception? Error)>>();
            for (int i = 0; i < 16; i++)
            {
                string path = $"/ops010/c{i}";
                byte[] content = TestData.Bytes(2000 + (ulong)i, 1 << 20);
                attempts.Add(AttemptAsync(vol, path, content));
            }

            foreach ((string path, byte[] content, Exception? error) in await Task.WhenAll(attempts))
            {
                if (error is null)
                    acked[path] = content;
                else
                    Assert.True(error is DfsCapacityException or DfsUnavailableException, $"{path}: unexpected {error.GetType().Name}: {error.Message}");
            }

            foreach (TestNode node in cluster.Nodes)
                Assert.True(node.Disk.UsedBytes <= capacity - reserve / 2, $"{node.Name} used {node.Disk.UsedBytes} bytes under concurrent saturation");
            foreach ((string path, byte[] content) in acked)
            {
                foreach (ObjectId id in await OpsHelpers.ObjectsOfAsync(cluster, [path]))
                {
                    (int copies, int zones) = await cluster.Inspect.CountDurableDomainsAsync(id);
                    Assert.True(copies >= 2 && zones >= 2, $"{path}: acknowledged object {id} has {copies}/{zones}");
                }
            }

            await OpsHelpers.VerifyFilesAsync(vol, acked, "acknowledged before/while shedding");

            // The reserve keeps metadata progress and crash recovery working.
            await vol.Directories.CreateDirectoryAsync("/ops010/still-works", Ct);
            TestNode currentLeader = await cluster.GetLeaderAsync(Vol, Ct);
            TestNode follower = cluster.Nodes.First(n => n.Name != currentLeader.Name);
            await follower.CrashAsync();
            await follower.RestartAsync();
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), Ct);

            string[] toDelete = acked.Keys.Where((_, i) => i % 2 == 0).ToArray();
            foreach (string path in toDelete)
            {
                await vol.Files.DeleteAsync(path, Ct);
                acked.Remove(path);
            }

            await cluster.WaitUntilAsync(async () =>
            {
                try
                {
                    await vol.Files.WriteAllBytesAsync("/ops010/after-gc", TestData.Bytes(3000, 1 << 20), Ct);
                    return true;
                }
                catch (DfsUnavailableException ex) when (ex is DfsCapacityException or DfsPlacementUnavailableException)
                {
                    return false;
                }
            }, TimeSpan.FromSeconds(60), "after deletions and GC, admission must accept new writes", Ct);
            await OpsHelpers.VerifyFilesAsync(vol, acked, "after recovery and GC");
        }, Ct);

        static async Task<(string, byte[], Exception?)> AttemptAsync(DfsVolume vol, string path, byte[] content)
        {
            try
            {
                await vol.Files.WriteAllBytesAsync(path, content, Ct);
                return (path, content, null);
            }
            catch (Exception ex)
            {
                return (path, content, ex);
            }
        }
    }

    // ============================================================================================================
    // OPS-011 RetryStormIsBounded
    // ============================================================================================================

    /// <summary>
    /// Sixteen clients hammer a volume whose leader is black-holed for all of them (connections stay open, requests vanish).
    /// Each operation finishes within its deadline (timeout + 1 s) with success or an availability/unknown outcome, never an
    /// unbounded hang; the requests sent per logical operation never exceed the SDK's retry budget (MaxRetryAttempts + 1
    /// for each of the operation and its outcome query at the failed endpoint, doubled for reroutes); and after healing
    /// every client completes new work promptly.
    /// </summary>
    [Fact, Catalog("OPS-011", "S", "INV-08", "INV-11", "INV-13", "INV-16", "INV-17", "INV-18")]
    public async Task OPS_011_RetryStormIsBounded()
    {
        await ClusterTest.RunAsync(OpsHelpers.ThreeZones(), async cluster =>
        {
            const int clientCount = 16;
            const int opsPerClient = 5;
            TimeSpan timeout = TimeSpan.FromSeconds(3);
            int maxRetries = cluster.CreateClientOptions(new TestClientOptions { Name = "probe" }).MaxRetryAttempts;
            var clients = new List<DfsClient>();
            for (int i = 0; i < clientCount; i++)
                clients.Add(await cluster.ConnectAsync(new TestClientOptions { Name = $"s{i}", OperationTimeout = timeout }, Ct));
            try
            {
                await clients[0].GetVolume(Vol).Directories.CreateDirectoryAsync("/ops011", Ct);
                TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
                for (int i = 0; i < clientCount; i++)
                    cluster.Network.BlackHole($"client:s{i}", leader.Name);
                cluster.Network.Log.Reset();

                var latencies = new List<TimeSpan>();
                var outcomes = new List<string>();
                var storms = clients.Select((c, i) => StormAsync(c, i)).ToArray();
                await Task.WhenAll(storms);

                OpsHelpers.Log($"max latency {latencies.Max()}, outcomes: {string.Join(", ", outcomes.GroupBy(o => o).Select(g => $"{g.Key}={g.Count()}"))}");
                Assert.All(latencies, l => Assert.True(l <= timeout + TimeSpan.FromSeconds(1), $"an operation took {l}, beyond its {timeout} deadline"));

                long toLeader = 0;
                long total = 0;
                for (int i = 0; i < clientCount; i++)
                {
                    toLeader += cluster.Network.Log.CountMessages(new MessageFilter(From: $"client:s{i}", To: leader.Name, Direction: LinkDirection.Request));
                    total += cluster.Network.Log.CountMessages(new MessageFilter(From: $"client:s{i}", Direction: LinkDirection.Request));
                }

                // Each logical operation may need two request kinds at the leader (its session/operation request and the
                // outcome query that resolves it), each within the SDK's retry budget; reroutes via followers at most double
                // the total. An unbounded storm (no backoff within a 3 s deadline) exceeds these bounds by orders of magnitude.
                long logicalOps = clientCount * opsPerClient;
                long perOpBudget = 2L * (maxRetries + 1);
                OpsHelpers.Log($"requests to the black-holed leader: {toLeader}, all requests: {total}, logical operations: {logicalOps}, retry budget {maxRetries}");
                Assert.True(toLeader <= logicalOps * perOpBudget, $"{toLeader} requests to the failed endpoint exceed {logicalOps} x {perOpBudget}");
                Assert.True(total <= 2 * logicalOps * perOpBudget, $"{total} requests in total exceed the bounded retry amplification");

                cluster.Network.Heal();
                for (int i = 0; i < clients.Count; i++)
                {
                    DfsClient c = clients[i];
                    TimeSpan elapsed = await OpsHelpers.TimeAsync(cluster, () => c.GetVolume(Vol).Files.WriteAllBytesAsync($"/ops011/healed-{i}", TestData.Bytes(1100, 1000), Ct));
                    Assert.True(elapsed <= TimeSpan.FromSeconds(10), $"after healing, client s{i} needed {elapsed} for a write");
                }

                async Task StormAsync(DfsClient c, int index)
                {
                    for (int n = 0; n < opsPerClient; n++)
                    {
                        long start = cluster.Time.GetTimestamp();
                        string outcome;
                        try
                        {
                            await c.GetVolume(Vol).Files.WriteAllBytesAsync($"/ops011/s{index}-{n}", TestData.Bytes((ulong)(index * 100 + n), 2000), Ct);
                            outcome = "ok";
                        }
                        catch (Exception ex) when (ex is DfsUnavailableException or DfsCommitOutcomeUnknownException or TimeoutException)
                        {
                            outcome = ex.GetType().Name;
                        }

                        TimeSpan elapsed = cluster.Time.GetElapsedTime(start);
                        lock (latencies)
                        {
                            latencies.Add(elapsed);
                            outcomes.Add(outcome);
                        }
                    }
                }
            }
            finally
            {
                foreach (DfsClient c in clients)
                    await c.DisposeAsync();
            }
        }, Ct);
    }

    // ============================================================================================================
    // OPS-012 RecoveredQuorumEventuallyProgresses
    // ============================================================================================================

    /// <summary>
    /// Quorum is lost (two of three crashed, three of five crashed, or the client side partitioned with a two-node
    /// minority) or the leader is isolated. While a majority is unavailable nothing is acknowledged and there is never more
    /// than one leader per term. Once a stable majority, the data targets and communication return, new writes complete
    /// within 60 s of virtual time, pending/unknown operations resolve definitely, and the history is linearizable.
    /// </summary>
    [Theory, Catalog("OPS-012", "S", "INV-08", "INV-11", "INV-13", "INV-16", "INV-17", "INV-18")]
    [InlineData("crash-two-of-three")]
    [InlineData("isolate-leader-of-three")]
    [InlineData("crash-three-of-five")]
    [InlineData("partition-client-with-minority-of-five")]
    public async Task OPS_012_RecoveredQuorumEventuallyProgresses(string scenario)
    {
        bool five = scenario.EndsWith("five", StringComparison.Ordinal);
        TestClusterOptions options = five ? OpsHelpers.Nodes(5, null, volumeVoters: 5) : OpsHelpers.ThreeZones();
        await ClusterTest.RunAsync(options, async cluster =>
        {
            await using DfsClient c0 = await cluster.ConnectAsync(new TestClientOptions { Name = "c0", OperationTimeout = TimeSpan.FromSeconds(3) }, Ct);
            await using DfsClient r0 = await cluster.ConnectAsync(new TestClientOptions { Name = "r0", OperationTimeout = TimeSpan.FromSeconds(3) }, Ct);
            await c0.GetVolume(Vol).Directories.CreateDirectoryAsync("/ops012", Ct);
            var workload = new HistoryWorkload(cluster, "/ops012");
            for (int i = 0; i < 3; i++)
                await workload.WriteAsync(c0, "k0", TestData.Bytes(1200 + (ulong)i, 6000));

            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            List<TestNode> crashed = [];
            bool quorumLost = true;
            switch (scenario)
            {
                case "crash-two-of-three":
                    crashed = cluster.Nodes.Where(n => n.Name != "n0").ToList();
                    break;
                case "crash-three-of-five":
                    crashed = cluster.Nodes.Where(n => n.Name is "n2" or "n3" or "n4").ToList();
                    break;
                case "isolate-leader-of-three":
                    cluster.Network.Isolate(leader.Name);
                    quorumLost = false;
                    break;
                default:
                    cluster.Network.Partition(["n0", "n1", "client:c0", "client:r0"], ["n2", "n3", "n4"]);
                    break;
            }

            foreach (TestNode node in crashed)
                await node.CrashAsync();

            int okBefore = workload.OkWrites;
            for (int i = 0; i < 4; i++)
            {
                await workload.WriteAsync(c0, "k0", TestData.Bytes(1210 + (ulong)i, 6000));
                await workload.ReadAsync(r0, "k0");
                await cluster.Inspect.AssertNoSplitBrainAsync();
            }

            if (quorumLost)
                Assert.Equal(okBefore, workload.OkWrites);

            foreach (TestNode node in crashed)
                await node.RestartAsync();
            cluster.Network.Heal();

            long recoveredAt = cluster.Time.GetTimestamp();
            await cluster.WaitUntilAsync(async () =>
            {
                int before = workload.OkWrites;
                await workload.WriteAsync(c0, "k0", TestData.Bytes(1300 + (ulong)before, 6000));
                return workload.OkWrites > before;
            }, TimeSpan.FromSeconds(60), "with a stable majority, healthy data targets and communication restored, new writes must complete", Ct);
            OpsHelpers.Log($"{scenario}: progress resumed {cluster.Time.GetElapsedTime(recoveredAt)} after recovery");

            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), Ct);
            await workload.ResolveUnknownsAsync(r0);
            await workload.FinalReadsAsync(r0, ["k0"]);
            workload.AssertLinearizable();
            await cluster.WaitForReplicationAsync(Vol, Ct);
            await cluster.Inspect.AssertReplicasConvergedAsync(Vol);
            await cluster.Inspect.AssertNoSplitBrainAsync();
        }, Ct);
    }

    // ============================================================================================================
    // OPS-013 PermanentlyUnavailableIsNotLivenessBug
    // ============================================================================================================

    /// <summary>
    /// Two of three voters are permanently gone. For a full minute of virtual time every write and every live read fails
    /// explicitly with a typed availability error (or, for a write, an unknown outcome) within its deadline; nothing is
    /// acknowledged, no stale data is served as live, and the surviving replica's durable state never contains the attempted
    /// files.
    /// </summary>
    [Fact, Catalog("OPS-013", "S", "INV-08", "INV-11", "INV-13", "INV-16", "INV-17", "INV-18")]
    public async Task OPS_013_PermanentlyUnavailableIsNotLivenessBug()
    {
        await ClusterTest.RunAsync(OpsHelpers.ThreeZones(), async cluster =>
        {
            TimeSpan timeout = TimeSpan.FromSeconds(3);
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0", OperationTimeout = timeout }, Ct);
            DfsVolume vol = client.GetVolume(Vol);
            await vol.Directories.CreateDirectoryAsync("/ops013", Ct);
            await vol.Files.WriteAllBytesAsync("/ops013/existing", TestData.Bytes(1300, 5000), Ct);

            TestNode survivor = cluster.Nodes[0];
            foreach (TestNode node in cluster.Nodes.Skip(1))
                await node.CrashAsync();
            await cluster.DelayAsync(TimeSpan.FromSeconds(2), Ct);

            for (int i = 0; i < 12; i++)
            {
                string path = $"/ops013/attempt{i}";
                Exception? write = null;
                TimeSpan elapsed = await OpsHelpers.TimeAsync(cluster, async () =>
                    write = await Record.ExceptionAsync(() => vol.Files.WriteAllBytesAsync(path, TestData.Bytes(1310 + (ulong)i, 5000), Ct)));
                Assert.True(write is DfsUnavailableException or DfsCommitOutcomeUnknownException,
                    $"attempt {i}: without quorum a write must fail explicitly, got {write?.GetType().Name ?? "success"}");
                Assert.True(elapsed <= timeout + TimeSpan.FromSeconds(2), $"attempt {i} took {elapsed}");

                Exception? read = await Record.ExceptionAsync(() => vol.Files.ReadAllBytesAsync("/ops013/existing", Ct));
                Assert.True(read is DfsUnavailableException, $"attempt {i}: a live read without quorum must fail as unavailable, got {read?.GetType().Name ?? "success"}");

                VolumeStateInspection survivorState = await cluster.Inspect.GetVolumeStateAsync(Vol, survivor.Name);
                Assert.Null(survivorState.GetFile(path));
                await cluster.DelayAsync(TimeSpan.FromSeconds(5), Ct);
            }
        }, Ct);
    }

    /// <summary>
    /// Policy ack 3 copies in 3 zones with one zone permanently gone: every write — large or small enough to inline — fails
    /// with <see cref="DfsPlacementUnavailableException"/> reporting required 3/3 and achieved at most 2/2, for a full
    /// minute; nothing is published, and metadata-only operations still succeed (placement unavailability is distinct from
    /// quorum unavailability).
    /// </summary>
    [Fact, Catalog("OPS-013", "S", "INV-08", "INV-11", "INV-13", "INV-16", "INV-17", "INV-18")]
    public async Task OPS_013_PermanentlyUnavailableIsNotLivenessBug_RequiredDomainAbsent()
    {
        TestClusterOptions options = OpsHelpers.ThreeZones() with { DefaultPolicy = OpsHelpers.Policy(3, 3, 3, 3) };
        await ClusterTest.RunAsync(options, async cluster =>
        {
            TimeSpan timeout = TimeSpan.FromSeconds(3);
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0", OperationTimeout = timeout }, Ct);
            DfsVolume vol = client.GetVolume(Vol);
            await vol.Directories.CreateDirectoryAsync("/ops013p", Ct);
            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            await cluster.Nodes.First(n => n.Name != leader.Name).CrashAsync();
            await cluster.DelayAsync(TimeSpan.FromSeconds(2), Ct);

            for (int i = 0; i < 12; i++)
            {
                foreach (int length in new[] { 100, 128 * 1024 })
                {
                    string path = $"/ops013p/f{i}-{length}";
                    Exception? error = null;
                    TimeSpan elapsed = await OpsHelpers.TimeAsync(cluster, async () =>
                        error = await Record.ExceptionAsync(() => vol.Files.WriteAllBytesAsync(path, TestData.Bytes(1320 + (ulong)i, length), Ct)));
                    DfsPlacementUnavailableException placement = Assert.IsType<DfsPlacementUnavailableException>(error);
                    Assert.Equal(DfsErrorCode.PlacementUnavailable, placement.ErrorCode);
                    Assert.Equal(3, placement.RequiredCopies);
                    Assert.Equal(3, placement.RequiredZones);
                    Assert.InRange(placement.AchievedCopies, 0, 2);
                    Assert.InRange(placement.AchievedZones, 0, 2);
                    Assert.True(placement.IsRetryable);
                    Assert.True(elapsed <= timeout + TimeSpan.FromSeconds(2), $"{path}: failing took {elapsed}");
                    Assert.False(await vol.Files.ExistsAsync(path, Ct), $"{path} must not be published under a weaker policy");
                }

                await vol.Directories.CreateDirectoryAsync($"/ops013p/dir{i}", Ct);
                await cluster.DelayAsync(TimeSpan.FromSeconds(5), Ct);
            }
        }, Ct);
    }

    // ============================================================================================================
    // OPS-014 MaintenanceCannotStarveForeground / OPS-015 ForegroundCannotStarveRepairForever
    // ============================================================================================================

    /// <summary>
    /// Repair of a wiped node (4 MiB at a 2 MiB/s budget), scrubbing, a backup export and GC/compaction of deleted files run
    /// together. The admitted foreground workload (extent-sized writes and reads that share the data path with repair) keeps
    /// completing: its p99 latency stays within max(4 × baseline p99, baseline p99 + 500 ms) and all operations finish,
    /// while repair bandwidth stays within its configured budget.
    /// </summary>
    [Fact, Catalog("OPS-014", "S", "INV-08", "INV-11", "INV-13", "INV-16", "INV-17", "INV-18")]
    public async Task OPS_014_MaintenanceCannotStarveForeground()
    {
        const long repairBudget = 2L << 20;
        TestClusterOptions options = OpsHelpers.ThreeZones(backupNodes: 1) with { Timings = NodeTimings.Fast with { RepairBytesPerSecond = repairBudget } };
        await ClusterTest.RunAsync(options, async cluster =>
        {
            DfsAdminClient admin = cluster.Admin;
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            DfsVolume vol = client.GetVolume(Vol);
            Dictionary<string, byte[]> seeded = await OpsHelpers.SeedFilesAsync(vol, "/ops014/seed", 64, 64 * 1024, 1400);
            await vol.Directories.CreateDirectoryAsync("/ops014/fg", Ct);
            await cluster.WaitForRepairAsync(Vol, TimeSpan.FromSeconds(60), Ct);

            List<TimeSpan> baseline = await ForegroundAsync(cluster, vol, "/ops014/fg", 40);
            TimeSpan baselineP99 = OpsHelpers.Percentile(baseline, 99);

            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            TestNode wiped = cluster.Nodes.First(n => n.Name != leader.Name);
            await wiped.CrashAsync();
            await wiped.ReplaceDiskWithEmptyAsync();
            long repairBefore = (await cluster.Inspect.GetTotalCountersAsync()).RepairBytes;
            long maintenanceStart = cluster.Time.GetTimestamp();
            await wiped.RestartAsync();
            await admin.StartScrubAsync(null, Ct);
            TestBackupNode b0 = cluster.BackupNodes[0];
            await b0.RegisterAsync(Vol);
            await vol.Snapshots.CreateAsync("ops014-s1", null, Ct);
            await vol.Backups.RequestSyncAsync(b0.TargetName, "ops014-s1", Ct);
            var deleted = new HashSet<string>(StringComparer.Ordinal);
            for (int i = 0; i < 32; i++)
            {
                await vol.Files.DeleteAsync($"/ops014/seed/f{i}", Ct);
                deleted.Add($"/ops014/seed/f{i}");
            }

            List<TimeSpan> during = await ForegroundAsync(cluster, vol, "/ops014/fg", 40);
            TimeSpan duringP99 = OpsHelpers.Percentile(during, 99);
            TimeSpan bound = TimeSpan.FromTicks(Math.Max(baselineP99.Ticks * 4, (baselineP99 + TimeSpan.FromMilliseconds(500)).Ticks));
            OpsHelpers.Log($"foreground p99 baseline {baselineP99}, during maintenance {duringP99}, bound {bound}");
            Assert.Equal(40, during.Count);
            Assert.True(duringP99 <= bound, $"foreground p99 {duringP99} exceeded {bound} while maintenance ran");

            TimeSpan window = cluster.Time.GetElapsedTime(maintenanceStart);
            long repaired = (await cluster.Inspect.GetTotalCountersAsync()).RepairBytes - repairBefore;
            double rate = repaired / Math.Max(window.TotalSeconds, 0.001);
            OpsHelpers.Log($"repair moved {repaired} bytes in {window} ({rate:F0} B/s, budget {repairBudget} B/s per node)");
            Assert.True(repaired <= (window.TotalSeconds * repairBudget * 1.25 + (1 << 20)) * cluster.Nodes.Count,
                $"repair moved {repaired} bytes in {window}, more than the configured budget allows");

            await cluster.WaitForRepairAsync(Vol, TimeSpan.FromSeconds(120), Ct);
            await OpsHelpers.VerifyFilesAsync(vol, seeded.Where(kv => !deleted.Contains(kv.Key)).ToDictionary(kv => kv.Key, kv => kv.Value), "after maintenance");
        }, Ct);
    }

    /// <summary>
    /// A saturating but admitted foreground workload runs while a wiped node must be repaired and deleted files reclaimed.
    /// With the reserved maintenance budget, repair completes within 3 × (data / budget) + 30 s of virtual time, the repair
    /// counter covers the lost data, GC physically reclaims every deleted object while the load continues (and the pending
    /// GC count drains to zero once it stops), foreground work keeps completing throughout, and its history is linearizable.
    /// </summary>
    [Fact, Catalog("OPS-015", "S", "INV-08", "INV-11", "INV-13", "INV-16", "INV-17", "INV-18")]
    public async Task OPS_015_ForegroundCannotStarveRepairForever()
    {
        const long repairBudget = 2L << 20;
        TestClusterOptions options = OpsHelpers.ThreeZones() with { Timings = NodeTimings.Fast with { RepairBytesPerSecond = repairBudget } };
        await ClusterTest.RunAsync(options, async cluster =>
        {
            DfsAdminClient admin = cluster.Admin;
            await using DfsClient c0 = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            await using DfsClient c1 = await cluster.ConnectAsync(new TestClientOptions { Name = "c1" }, Ct);
            DfsVolume vol = c0.GetVolume(Vol);
            Dictionary<string, byte[]> seeded = await OpsHelpers.SeedFilesAsync(vol, "/ops015/seed", 64, 64 * 1024, 1500);
            long seededBytes = 64L * 64 * 1024;
            await cluster.WaitForRepairAsync(Vol, TimeSpan.FromSeconds(60), Ct);
            await vol.Directories.CreateDirectoryAsync("/ops015/fg", Ct);

            var workload = new HistoryWorkload(cluster, "/ops015/fg");
            workload.Start([(c0, "k0"), (c1, "k1"), (c0, "k2"), (c1, "k3")], null, valueBytes: 16 * 1024, pause: TimeSpan.FromMilliseconds(1));

            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            TestNode wiped = cluster.Nodes.First(n => n.Name != leader.Name);
            await wiped.CrashAsync();
            await wiped.ReplaceDiskWithEmptyAsync();
            long repairBefore = (await cluster.Inspect.GetTotalCountersAsync()).RepairBytes;
            await wiped.RestartAsync();

            TimeSpan repairBound = TimeSpan.FromSeconds(3.0 * seededBytes / repairBudget + 30);
            int okAtStart = workload.OkWrites;
            await cluster.WaitForRepairAsync(Vol, repairBound, Ct);
            long repaired = (await cluster.Inspect.GetTotalCountersAsync()).RepairBytes - repairBefore;
            Assert.True(repaired >= seededBytes, $"repair moved {repaired} bytes, less than the {seededBytes} lost with the wiped disk");
            Assert.True(workload.OkWrites > okAtStart, "foreground work must have continued during repair");

            long gcBefore = (await cluster.Inspect.GetTotalCountersAsync()).GcDeletedObjects;
            IReadOnlyList<ObjectId> doomed = await OpsHelpers.ObjectsOfAsync(cluster, Enumerable.Range(0, 16).Select(i => $"/ops015/seed/f{i}"));
            for (int i = 0; i < 16; i++)
                await vol.Files.DeleteAsync($"/ops015/seed/f{i}", Ct);
            // The foreground load keeps overwriting its registers (creating new garbage all the time), so progress means the
            // deleted objects are physically reclaimed while the load runs, not that the pending count is momentarily zero.
            int okBeforeGc = workload.OkWrites;
            await cluster.WaitUntilAsync(async () => !(await cluster.Inspect.GetAllStoredObjectsAsync(Vol)).Overlaps(doomed),
                TimeSpan.FromSeconds(60), "GC must reclaim deleted objects while admitted foreground load continues", Ct);
            Assert.True((await cluster.Inspect.GetTotalCountersAsync()).GcDeletedObjects - gcBefore >= doomed.Count);
            Assert.True(workload.OkWrites > okBeforeGc, "foreground work must have continued while GC reclaimed the deleted objects");

            await workload.StopAsync();
            await cluster.WaitUntilAsync(async () => (await admin.GetReplicaHealthAsync(Vol, Ct)).PendingGcObjects == 0,
                TimeSpan.FromSeconds(60), "once the overwriting load stops, the GC backlog must drain completely", Ct);
            await workload.ResolveUnknownsAsync(c1);
            await workload.FinalReadsAsync(c1, ["k0", "k1", "k2", "k3"]);
            workload.AssertLinearizable();
            await OpsHelpers.VerifyFilesAsync(vol, seeded.Skip(16).ToDictionary(kv => kv.Key, kv => kv.Value), "after repair and GC");
        }, Ct);
    }

    private static async Task<List<TimeSpan>> ForegroundAsync(TestCluster cluster, DfsVolume vol, string directory, int operations)
    {
        var latencies = new List<TimeSpan>();
        for (int i = 0; i < operations; i++)
        {
            string path = $"{directory}/k{i % 4}";
            if (i % 2 == 0)
                // 16 KiB: above the inline threshold, so foreground writes use the same data path (uploads, receipts) as repair.
                latencies.Add(await OpsHelpers.TimeAsync(cluster, () => vol.Files.WriteAllBytesAsync(path, TestData.Bytes(1600 + (ulong)i, 16 * 1024), Ct)));
            else
                latencies.Add(await OpsHelpers.TimeAsync(cluster, () => vol.Files.ReadAllBytesAsync($"{directory}/k{(i - 1) % 4}", Ct)));
        }

        return latencies;
    }

    /// <summary>Distinct bytes of unreferenced objects of open staging sessions, as held by the running nodes.</summary>
    private static async Task<long> StagedObjectBytesAsync(TestCluster cluster)
    {
        VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Vol);
        var open = state.Staging.Where(s => s.Open).Select(s => s.Id).ToHashSet();
        IReadOnlySet<ObjectId> reachable = state.ReachableObjects();
        var distinct = new Dictionary<ObjectId, long>();
        foreach (TestNode node in cluster.Nodes.Where(n => n.State == TestNodeState.Running))
        {
            foreach (ObjectRecordInfo record in await node.Inspect.ListObjectsAsync(Vol))
            {
                if (open.Contains(record.Staging) && !reachable.Contains(record.Id))
                    distinct[record.Id] = record.Length;
            }
        }

        return distinct.Values.Sum();
    }
}
