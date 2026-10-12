using Tedd.Tdfs.Data;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;
using static Tedd.Tdfs.Tests.Unit.Rep.RepHelpers;

namespace Tedd.Tdfs.Tests.Unit.Rep;

/// <summary>
/// REP — replication policy, zones, and placement (test plan §7.12; FSD §9, §10.3).
/// Invariants: INV-01, INV-08, INV-11, INV-18.
/// </summary>
public sealed class RepUnitTests
{
    // ------------------------------------------------------------------------------------------------------------
    // REP-001 InvalidReplicaPolicyRejected
    // ------------------------------------------------------------------------------------------------------------

    private static readonly (int Copies, int Zones, int Ack, int AckZones, string Why)[] InvalidShapes =
    [
        (0, 1, 1, 1, "zero target copies"),
        (-1, 1, 1, 1, "negative target copies"),
        (3, 0, 1, 1, "zero target zones"),
        (3, -3, 1, 1, "negative target zones"),
        (3, 3, 0, 1, "zero acknowledgement copies"),
        (3, 3, -2, 1, "negative acknowledgement copies"),
        (3, 3, 2, 0, "zero acknowledgement zones"),
        (3, 3, 2, -1, "negative acknowledgement zones"),
        (2, 2, 3, 2, "acknowledgement copies above target"),
        (2, 3, 2, 2, "distinct zones above copies"),
        (3, 3, 1, 2, "acknowledgement zones above acknowledgement copies"),
        (3, 2, 3, 3, "acknowledgement zones above target zones"),
    ];

    /// <summary>Policies that are well-formed but impossible on three hosts in three zones.</summary>
    private static readonly (int Copies, int Zones, int Ack, int AckZones, string Why)[] ImpossibleOnThreeHosts =
    [
        (4, 3, 2, 2, "four copies on three hosts"),
        (4, 4, 2, 2, "four zones in a three-zone cluster"),
        (5, 1, 1, 1, "five copies on three hosts"),
    ];

    public static TheoryData<int, int, int, int, string> InvalidPolicyShapes()
    {
        var data = new TheoryData<int, int, int, int, string>();
        foreach (var p in InvalidShapes)
            data.Add(p.Copies, p.Zones, p.Ack, p.AckZones, p.Why);
        return data;
    }

    /// <summary>
    /// Zero/negative counts, acknowledgement above target and impossible zone/copy relations are rejected by policy
    /// validation with <see cref="DfsErrorCode.InvalidPolicy"/>, and placement refuses to plan with them.
    /// </summary>
    [Theory]
    [MemberData(nameof(InvalidPolicyShapes))]
    [Catalog("REP-001", "U", "INV-01", "INV-08", "INV-11", "INV-18")]
    public void REP_001_InvalidReplicaPolicyRejected_Validation(int copies, int zones, int ack, int ackZones, string why)
    {
        DfsStoragePolicy policy = Policy(copies, zones, ack, ackZones);
        DfsException ex = Assert.Throws<DfsException>(policy.Validate);
        Assert.Equal(DfsErrorCode.InvalidPolicy, ex.ErrorCode);

        var random = new SeededRandom(1_001);
        var nodes = Enumerable.Range(0, 5)
            .Select(i => new PlacementNode(NodeId.New(random), $"n{i}", new DfsTopologyLabels("r1", $"z{i}", "rk1", $"h{i}", "d1"), true, 1L << 30))
            .ToList();
        Assert.False(Placement.IsSatisfiable(policy, nodes, out string? reason), why);
        Assert.False(string.IsNullOrWhiteSpace(reason));
        DfsException placementEx = Assert.Throws<DfsException>(() => Placement.SelectTargets(ObjectId.New(random), 1, policy, nodes));
        Assert.Equal(DfsErrorCode.InvalidPolicy, placementEx.ErrorCode);
    }

    /// <summary>Boundary-valid policies are accepted (guards against over-rejection).</summary>
    [Theory]
    [InlineData(1, 1, 1, 1)]
    [InlineData(3, 3, 2, 2)]
    [InlineData(3, 3, 3, 3)]
    [InlineData(3, 2, 2, 1)]
    [InlineData(5, 3, 3, 2)]
    [Catalog("REP-001", "U", "INV-01", "INV-08", "INV-11", "INV-18")]
    public void REP_001_InvalidReplicaPolicyRejected_ValidBoundariesAccepted(int copies, int zones, int ack, int ackZones)
    {
        Policy(copies, zones, ack, ackZones).Validate();
    }

    /// <summary>
    /// Against a live three-zone cluster: invalid shapes and policies that are impossible for the actual topology are
    /// rejected by volume creation, named-policy definition and default-policy changes with
    /// <see cref="DfsErrorCode.InvalidPolicy"/> and leave no trace; unknown policy names in opens, whole-file writes,
    /// copies and policy assignment are rejected without creating or changing anything.
    /// </summary>
    [Fact]
    [Catalog("REP-001", "U", "INV-01", "INV-08", "INV-11", "INV-18")]
    public async Task REP_001_InvalidReplicaPolicyRejected_Cluster()
    {
        await ClusterTest.RunAsync(TestClusterOptions.ThreeZones, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            DfsVolume vol = client.GetVolume(Vol);
            DfsAdminClient admin = cluster.Admin;
            DfsVolumeInfo initial = await vol.GetInfoAsync(Ct);

            int i = 0;
            foreach (var p in InvalidShapes.Concat(ImpossibleOnThreeHosts))
            {
                DfsStoragePolicy policy = Policy(p.Copies, p.Zones, p.Ack, p.AckZones);
                string volumeName = $"bad{i}", policyName = $"bad{i}";
                i++;
                await AssertEx.ThrowsDfsAsync<DfsException>(
                    () => admin.CreateVolumeAsync(new DfsVolumeSpec { Name = volumeName, DefaultPolicy = policy }, Ct), DfsErrorCode.InvalidPolicy);
                Assert.DoesNotContain(volumeName, await admin.ListVolumesAsync(Ct));

                await AssertEx.ThrowsDfsAsync<DfsException>(() => admin.DefinePolicyAsync(Vol, policyName, policy, Ct), DfsErrorCode.InvalidPolicy);
                await AssertEx.ThrowsDfsAsync<DfsException>(() => admin.SetDefaultPolicyAsync(Vol, policy, migrateExisting: false, Ct), DfsErrorCode.InvalidPolicy);
                DfsVolumeInfo now = await vol.GetInfoAsync(Ct);
                Assert.False(now.NamedPolicies.ContainsKey(policyName), $"{p.Why}: rejected policy was stored");
                Assert.Equal(initial.DefaultPolicy, now.DefaultPolicy);
            }

            await vol.Files.WriteAllBytesAsync("/existing", TestData.Bytes(1_002, 10_000), Ct);
            DfsFileInfo existing = await vol.Files.GetInfoAsync("/existing", Ct);

            await AssertEx.ThrowsDfsAsync<DfsException>(() => vol.Files.OpenAsync("/new-open",
                new DfsOpenOptions { Mode = FileMode.CreateNew, Access = FileAccess.Write, StoragePolicy = "no-such-policy" }, Ct), DfsErrorCode.InvalidPolicy);
            Assert.False(await vol.Files.ExistsAsync("/new-open", Ct));

            await AssertEx.ThrowsDfsAsync<DfsException>(() => vol.Files.WriteAllBytesAsync("/new-write", TestData.Bytes(1_003, 100),
                new DfsMutationOptions { StoragePolicy = "no-such-policy" }, Ct), DfsErrorCode.InvalidPolicy);
            Assert.False(await vol.Files.ExistsAsync("/new-write", Ct));

            await AssertEx.ThrowsDfsAsync<DfsException>(() => vol.Files.CopyAsync("/existing", "/new-copy", false,
                new DfsMutationOptions { StoragePolicy = "no-such-policy" }, Ct), DfsErrorCode.InvalidPolicy);
            Assert.False(await vol.Files.ExistsAsync("/new-copy", Ct));

            await AssertEx.ThrowsDfsAsync<DfsException>(() => vol.Files.SetStoragePolicyAsync("/existing", "no-such-policy", Ct), DfsErrorCode.InvalidPolicy);
            DfsFileInfo after = await vol.Files.GetInfoAsync("/existing", Ct);
            Assert.Equal(existing.StoragePolicy, after.StoragePolicy);
            Assert.Equal(existing.Revision, after.Revision);
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // REP-002 TargetAndAcknowledgedCopiesDiffer
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Target three / acknowledge two with the third target offline or with its flush held: the write succeeds once two
    /// copies in two zones are durable, the third copy is reported pending (under-replicated, not missing), and repair
    /// later reaches the full target of three hosts in three zones.
    /// </summary>
    [Theory]
    [InlineData("ThirdNodeOffline")]
    [InlineData("ThirdNodeFlushHeld")]
    [Catalog("REP-002", "U", "INV-01", "INV-08", "INV-11", "INV-18")]
    public async Task REP_002_TargetAndAcknowledgedCopiesDiffer(string variant)
    {
        await ClusterTest.RunAsync(TestClusterOptions.ThreeZones with { DefaultPolicy = Policy(3, 3, 2, 2) }, async cluster =>
        {
            TestNode third = cluster.Node("n2");
            FaultArm? held = null;
            if (variant == "ThirdNodeOffline")
                await third.StopAsync();
            else
                held = cluster.Faults.Arm(new FaultMatch(FaultSites.ObjectBeforeFlush, Node: "n2", Volume: Vol, Count: int.MaxValue), FaultAction.Block());

            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { SeedNodes = ["n0", "n1"] }, Ct);
            DfsVolume vol = client.GetVolume(Vol);
            byte[] data = TestData.Bytes(2_000, 3 * 64 * 1024 + 11);
            CommitToken token = await vol.Files.WriteAllBytesAsync("/p", data, Ct);
            Assert.False(token.IsNone);
            if (held is not null)
                cluster.Faults.AssertReached(held);

            IReadOnlyList<ObjectId> objects = await ObjectsOfAsync(cluster, "/p");
            foreach (ObjectId o in objects)
            {
                IReadOnlyList<string> holders = await cluster.Inspect.GetObjectHoldersAsync(o);
                Assert.DoesNotContain("n2", holders);
                Assert.Equal(2, holders.Count);
                Assert.Equal((2, 2), await cluster.Inspect.CountDurableDomainsAsync(o));
            }

            DfsReplicaHealth health = await cluster.Admin.GetReplicaHealthAsync(Vol, Ct);
            Assert.True(health.UnderReplicatedObjects >= objects.Count, $"the pending third copies must be reported ({health.UnderReplicatedObjects} < {objects.Count})");
            Assert.Equal(0, health.MissingObjects);
            DfsVolumeStatus status = await cluster.Admin.GetVolumeStatusAsync(Vol, Ct);
            Assert.True(status.Replicas.UnderReplicatedObjects >= objects.Count);
            Same(data, await vol.Files.ReadAllBytesAsync("/p", Ct), "acknowledged contents");

            if (held is not null)
            {
                held.Disarm();
                held.Release();
            }
            else
            {
                await third.RestartAsync();
            }

            await cluster.WaitForRepairAsync(Vol, TimeSpan.FromSeconds(30), Ct);
            await AssertDurableAsync(cluster, "/p", 3, 3, "after repair");
            Assert.Equal(0, (await cluster.Admin.GetReplicaHealthAsync(Vol, Ct)).UnderReplicatedObjects);
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // REP-005 HostAndDiskIdentityNotDoubleCounted
    // ------------------------------------------------------------------------------------------------------------

    private static TestClusterOptions CoLocated() => new()
    {
        Nodes =
        [
            new TestNodeSpec("n0", "z1", Host: "hostA", Device: "d1"),
            new TestNodeSpec("n1", "z1", Host: "hostA", Device: "d2"),
            new TestNodeSpec("n2", "z2", Host: "hostB"),
            new TestNodeSpec("n3", "z3", Host: "hostC"),
        ],
        Topology = TestTopology.Custom,
        Volumes = [new TestVolumeSpec(Vol) { Metadata = new DfsMetadataPolicy { Voters = 3, Placement = DfsVoterPlacement.Explicit, ExplicitVoterNodes = ["n0", "n1", "n2"] } }],
        DefaultPolicy = Policy(3, 3, 2, 2),
    };

    /// <summary>Pure accounting: two devices/endpoints on one host are one copy; one host never spans two zones' worth of copies.</summary>
    [Fact]
    [Catalog("REP-005", "U", "INV-01", "INV-08", "INV-11", "INV-18")]
    public void REP_005_HostAndDiskIdentityNotDoubleCounted_Accounting()
    {
        var a1 = new DfsTopologyLabels("r1", "z1", "rk1", "hostA", "d1");
        var a2 = new DfsTopologyLabels("r1", "z1", "rk2", "hostA", "d2");
        var b = new DfsTopologyLabels("r1", "z2", "rk1", "hostB", "d1");
        var otherRegionSameZoneName = new DfsTopologyLabels("r2", "z1", "rk1", "hostD", "d1");
        Assert.Equal((1, 1), Placement.CountDomains([a1, a2]));
        Assert.Equal((2, 2), Placement.CountDomains([a1, a2, b]));
        Assert.Equal((2, 2), Placement.CountDomains([a1, otherRegionSameZoneName]));

        var random = new SeededRandom(5_000);
        var nodes = new List<PlacementNode>
        {
            new(NodeId.New(random), "n0", a1, true, 1L << 30),
            new(NodeId.New(random), "n1", a2, true, 1L << 30),
            new(NodeId.New(random), "n2", b, true, 1L << 30),
        };
        Assert.False(Placement.IsSatisfiable(Policy(3, 2, 2, 2), nodes, out string? reason));
        Assert.Contains("host", reason, StringComparison.OrdinalIgnoreCase);
        Assert.True(Placement.IsSatisfiable(Policy(2, 2, 2, 2), nodes, out _));

        nodes.Add(new PlacementNode(NodeId.New(random), "n3", new DfsTopologyLabels("r1", "z3", "rk1", "hostC", "d1"), true, 1L << 30));
        for (int i = 0; i < 500; i++)
        {
            IReadOnlyList<PlacementNode> targets = Placement.SelectTargets(ObjectId.New(random), 7, Policy(3, 3, 2, 2), nodes);
            Assert.Equal(3, targets.Select(t => t.Labels.Host).Distinct(StringComparer.Ordinal).Count());
            Assert.False(targets.Any(t => t.Name == "n0") && targets.Any(t => t.Name == "n1"), "both devices of hostA chosen while a distinct host was available");
        }
    }

    /// <summary>
    /// Live cluster with two node processes (devices) on one host: replicas land on distinct hosts, the inspector counts
    /// copies by host, a policy needing four distinct hosts is rejected despite four endpoints, and when only hostA's two
    /// endpoints can accept data the write fails with placement unavailability reporting one achieved copy.
    /// </summary>
    [Fact]
    [Catalog("REP-005", "U", "INV-01", "INV-08", "INV-11", "INV-18")]
    public async Task REP_005_HostAndDiskIdentityNotDoubleCounted_Cluster()
    {
        await ClusterTest.RunAsync(CoLocated(), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            DfsVolume vol = client.GetVolume(Vol);
            for (int i = 0; i < 6; i++)
                await vol.Files.WriteAllBytesAsync($"/f{i}", TestData.Bytes((ulong)(5_100 + i), 2 * 64 * 1024), Ct);
            await cluster.WaitForRepairAsync(Vol, TimeSpan.FromSeconds(30), Ct);
            for (int i = 0; i < 6; i++)
            {
                await AssertDurableAsync(cluster, $"/f{i}", 3, 3, "healthy co-located topology");
                foreach (ObjectId o in await ObjectsOfAsync(cluster, $"/f{i}"))
                {
                    IReadOnlyList<string> holders = await cluster.Inspect.GetObjectHoldersAsync(o);
                    Assert.False(holders.Contains("n0") && holders.Contains("n1"), $"object {o} placed twice on hostA");
                }
            }

            await AssertEx.ThrowsDfsAsync<DfsException>(() => cluster.Admin.DefinePolicyAsync(Vol, "four-hosts", Policy(4, 3, 2, 2), Ct), DfsErrorCode.InvalidPolicy);

            byte[] before = await vol.Files.ReadAllBytesAsync("/f0", Ct);
            cluster.Node("n2").Disk.Fill(leaveFreeBytes: 4L << 20);
            cluster.Node("n3").Disk.Fill(leaveFreeBytes: 4L << 20);
            DfsPlacementUnavailableException ex = await AssertEx.ThrowsDfsAsync<DfsPlacementUnavailableException>(
                () => vol.Files.WriteAllBytesAsync("/f0", TestData.Bytes(5_200, 2 * 64 * 1024), Ct), DfsErrorCode.PlacementUnavailable);
            AssertPlacementCounts(ex, requiredCopies: 2, maxAchievedCopies: 1, requiredZones: 2, maxAchievedZones: 1);
            Same(before, await vol.Files.ReadAllBytesAsync("/f0", Ct), "rejected write must leave the previous revision");
            await vol.Directories.CreateDirectoryAsync("/metadata-still-works", Ct);
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // REP-006 WitnessNotADataReplica
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// A metadata voter without data capacity (witness) never holds payload and never counts toward data
    /// acknowledgement: with one data node down, metadata still commits (witness + data node majority) but a 2-copy
    /// write fails with placement unavailability reporting one achieved copy — not quorum unavailability, not success.
    /// </summary>
    [Fact]
    [Catalog("REP-006", "U", "INV-01", "INV-08", "INV-11", "INV-18")]
    public async Task REP_006_WitnessNotADataReplica()
    {
        var options = new TestClusterOptions
        {
            NodeCount = 3,
            ControlVoters = 3,
            Volumes = [new TestVolumeSpec(Vol) { Metadata = new DfsMetadataPolicy { Voters = 3, Placement = DfsVoterPlacement.Explicit, ExplicitVoterNodes = ["n0", "n1", "n2"] } }],
            DefaultPolicy = Policy(2, 2, 2, 2),
            ConfigureNodeOptions = (name, o) => name == "n2" ? o with { CapacityBytes = o.ReservedBytes } : o,
        };
        await ClusterTest.RunAsync(options, async cluster =>
        {
            GroupInspection group = await cluster.Inspect.GetVolumeGroupAsync(Vol);
            Assert.Contains("n2", group.Voters);

            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            DfsVolume vol = client.GetVolume(Vol);
            byte[] data = TestData.Bytes(6_000, 2 * 64 * 1024);
            await vol.Files.WriteAllBytesAsync("/w", data, Ct);
            await cluster.WaitForRepairAsync(Vol, TimeSpan.FromSeconds(20), Ct);
            foreach (ObjectId o in await ObjectsOfAsync(cluster, "/w"))
            {
                IReadOnlyList<string> holders = await cluster.Inspect.GetObjectHoldersAsync(o);
                Assert.DoesNotContain("n2", holders);
                Assert.Equal((2, 2), await cluster.Inspect.CountDurableDomainsAsync(o));
                Assert.False(await cluster.Node("n2").Inspect.HasObjectAsync(o), "the witness must not store payload");
            }

            await cluster.Node("n1").StopAsync();
            await vol.Directories.CreateDirectoryAsync("/quorum-ok", Ct);
            DfsPlacementUnavailableException ex = await AssertEx.ThrowsDfsAsync<DfsPlacementUnavailableException>(
                () => vol.Files.WriteAllBytesAsync("/w2", TestData.Bytes(6_001, 64 * 1024), Ct), DfsErrorCode.PlacementUnavailable);
            AssertPlacementCounts(ex, requiredCopies: 2, maxAchievedCopies: 1, requiredZones: 2, maxAchievedZones: 1);
            Assert.False(await vol.Files.ExistsAsync("/w2", Ct) && (await vol.Files.GetInfoAsync("/w2", Ct)).Length > 0,
                "a write acknowledged only by a data node and a witness must not publish data");
            Same(data, await vol.Files.ReadAllBytesAsync("/w", Ct), "existing data readable from the surviving data copy");
        }, Ct);
    }

    /// <summary>Pure placement: a node that cannot receive payload (no capacity / not data-eligible) is never a target.</summary>
    [Fact]
    [Catalog("REP-006", "U", "INV-01", "INV-08", "INV-11", "INV-18")]
    public void REP_006_WitnessNotADataReplica_Placement()
    {
        var random = new SeededRandom(6_100);
        var witnessFull = new PlacementNode(NodeId.New(random), "w-full", new DfsTopologyLabels("r1", "z3", "rk1", "hw1", "d1"), true, 0);
        var witnessIneligible = new PlacementNode(NodeId.New(random), "w-vote", new DfsTopologyLabels("r1", "z4", "rk1", "hw2", "d1"), false, 1L << 30);
        var nodes = new List<PlacementNode>
        {
            new(NodeId.New(random), "n0", new DfsTopologyLabels("r1", "z1", "rk1", "h0", "d1"), true, 1L << 30),
            new(NodeId.New(random), "n1", new DfsTopologyLabels("r1", "z2", "rk1", "h1", "d1"), true, 1L << 30),
            witnessFull,
            witnessIneligible,
        };
        for (int i = 0; i < 200; i++)
        {
            ObjectId o = ObjectId.New(random);
            IReadOnlyList<PlacementNode> targets = Placement.SelectTargets(o, 3, Policy(3, 3, 2, 2), nodes);
            Assert.Equal(2, targets.Count);
            Assert.DoesNotContain(targets, t => t.Name.StartsWith("w-", StringComparison.Ordinal));
            Assert.DoesNotContain(Placement.FallbackOrder(o, 3, nodes), t => t.Name.StartsWith("w-", StringComparison.Ordinal));
        }

        Assert.False(Placement.IsSatisfiable(Policy(3, 3, 2, 2), nodes, out _));
    }

    // ------------------------------------------------------------------------------------------------------------
    // REP-007 BackupNotALiveReplica
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// A fully synchronized late-sync backup holding every object is never a live holder, never a Raft member, does not
    /// cure under-replication when a live node is lost, and does not satisfy a 3-copy acknowledgement for new writes.
    /// </summary>
    [Fact]
    [Catalog("REP-007", "U", "INV-01", "INV-08", "INV-11", "INV-18")]
    public async Task REP_007_BackupNotALiveReplica()
    {
        var options = TestClusterOptions.ThreeZones with
        {
            BackupNodeCount = 1,
            Volumes = [new TestVolumeSpec(Vol) { DefaultPolicy = Policy(3, 3, 3, 3) }],
        };
        await ClusterTest.RunAsync(options, async cluster =>
        {
            TestBackupNode backup = cluster.BackupNodes[0];
            await backup.RegisterAsync(Vol);
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { SeedNodes = ["n0", "n1"] }, Ct);
            DfsVolume vol = client.GetVolume(Vol);
            byte[] data = TestData.Bytes(7_000, 3 * 64 * 1024);
            CommitToken token = await vol.Files.WriteAllBytesAsync("/b", data, Ct);
            await vol.Backups.RequestSyncAsync(backup.TargetName, ct: Ct);
            DfsBackupCheckpoint checkpoint = await vol.Backups.AwaitBackupAsync(backup.TargetName, token, TimeSpan.FromSeconds(30), Ct);
            Assert.True(checkpoint.CoveredCommit.IsAtOrAfter(token));

            IReadOnlyList<ObjectId> objects = await ObjectsOfAsync(cluster, "/b");
            foreach (ObjectId o in objects)
                Assert.DoesNotContain(backup.Name, await cluster.Inspect.GetObjectHoldersAsync(o));
            GroupInspection group = await cluster.Inspect.GetVolumeGroupAsync(Vol);
            Assert.DoesNotContain(backup.Name, group.Voters);
            Assert.DoesNotContain(backup.Name, group.Learners);

            await cluster.Node("n2").StopAsync();
            await cluster.WaitUntilAsync(async () => (await cluster.Admin.GetReplicaHealthAsync(Vol, Ct)).UnderReplicatedObjects >= objects.Count,
                TimeSpan.FromSeconds(20), "losing a live replica must be reported although the backup holds every object", Ct);
            foreach (ObjectId o in objects)
                Assert.Equal((2, 2), await cluster.Inspect.CountDurableDomainsAsync(o));

            await vol.Directories.CreateDirectoryAsync("/quorum-ok", Ct);
            DfsPlacementUnavailableException ex = await AssertEx.ThrowsDfsAsync<DfsPlacementUnavailableException>(
                () => vol.Files.WriteAllBytesAsync("/b", TestData.Bytes(7_001, 64 * 1024), Ct), DfsErrorCode.PlacementUnavailable);
            AssertPlacementCounts(ex, requiredCopies: 3, maxAchievedCopies: 2, requiredZones: 3, maxAchievedZones: 2);
            Same(data, await vol.Files.ReadAllBytesAsync("/b", Ct), "the previous revision stays current");
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // REP-008 PlacementIsDeterministic
    // ------------------------------------------------------------------------------------------------------------

    private static List<PlacementNode> MakeNodes(int count, string layout, ulong seed)
    {
        var random = new SeededRandom(seed);
        int firstZone = (2 * count + 2) / 3;
        var nodes = new List<PlacementNode>(count);
        for (int i = 0; i < count; i++)
        {
            (string zone, string host) = layout switch
            {
                "OnePerZone" => ($"z{i}", $"h{i}"),
                "TwoPlusOne" => (i < firstZone ? "z1" : "z2", $"h{i}"),
                "SingleZone" => ("z1", $"h{i}"),
                "SharedHosts" => ($"z{i / 2 % 3}", $"h{i / 2}"),
                _ => throw new ArgumentOutOfRangeException(nameof(layout)),
            };
            nodes.Add(new PlacementNode(NodeId.New(random), $"n{i}", new DfsTopologyLabels("r1", zone, "rk1", host, "d1"), true, 1L << 30));
        }

        return nodes;
    }

    public static TheoryData<int, string> PlacementLayouts() => new()
    {
        { 3, "OnePerZone" }, { 5, "OnePerZone" }, { 10, "OnePerZone" }, { 7, "TwoPlusOne" }, { 6, "SingleZone" }, { 10, "SharedHosts" },
    };

    /// <summary>
    /// For hundreds of object ids, several policies and epochs, every permutation of the node list yields the identical
    /// ordered target list and fallback order; results contain only distinct receivable nodes and are maximally diverse
    /// (zones and hosts covered up to the policy's targets).
    /// </summary>
    [Theory]
    [MemberData(nameof(PlacementLayouts))]
    [Catalog("REP-008", "U", "INV-01", "INV-08", "INV-11", "INV-18")]
    public void REP_008_PlacementIsDeterministic(int count, string layout)
    {
        List<PlacementNode> nodes = MakeNodes(count, layout, (ulong)(8_000 + count));
        var shuffler = new SeededRandom(8_100);
        var permutations = new List<List<PlacementNode>> { nodes, Enumerable.Reverse(nodes).ToList() };
        for (int p = 0; p < 4; p++)
        {
            var copy = nodes.ToList();
            shuffler.Shuffle(copy);
            permutations.Add(copy);
        }

        DfsStoragePolicy[] policies = [Policy(3, 3, 2, 2), Policy(2, 2, 1, 1), Policy(5, 3, 2, 2), Policy(1, 1, 1, 1)];
        var ids = new SeededRandom(8_200);
        for (int i = 0; i < 300; i++)
        {
            ObjectId obj = ObjectId.New(ids);
            ulong epoch = (ulong)(1 + i % 3);
            foreach (DfsStoragePolicy policy in policies)
            {
                IReadOnlyList<PlacementNode> reference = Placement.SelectTargets(obj, epoch, policy, nodes);
                IReadOnlyList<PlacementNode> fallback = Placement.FallbackOrder(obj, epoch, nodes);
                foreach (List<PlacementNode> permutation in permutations)
                {
                    Assert.Equal(reference.Select(n => n.Id), Placement.SelectTargets(obj, epoch, policy, permutation).Select(n => n.Id));
                    Assert.Equal(fallback.Select(n => n.Id), Placement.FallbackOrder(obj, epoch, permutation).Select(n => n.Id));
                }

                Assert.Equal(reference.Select(n => n.Id), Placement.SelectTargets(obj, epoch, policy, nodes).Select(n => n.Id));
                Assert.Equal(reference.Count, reference.Select(n => n.Id).Distinct().Count());
                Assert.Equal(Math.Min(policy.TargetCopies, nodes.Count), reference.Count);
                Assert.Equal(nodes.Count, fallback.Count);
                Assert.All(reference, n => Assert.Contains(n, fallback));

                int availableZones = nodes.Select(n => n.Labels.ZoneKey).Distinct().Count();
                int availableHosts = nodes.Select(n => n.Labels.Host).Distinct(StringComparer.Ordinal).Count();
                (int hosts, int zones) = Placement.CountDomains(reference.Select(n => n.Labels));
                Assert.Equal(Math.Min(Math.Min(policy.TargetDistinctZones, policy.TargetCopies), availableZones), Math.Min(zones, policy.TargetDistinctZones));
                Assert.Equal(Math.Min(policy.TargetCopies, availableHosts), hosts);
            }
        }
    }

    /// <summary>
    /// Cluster-level: after repair converges, the nodes holding each object are exactly the deterministic targets computed
    /// from (object id, policy, placement epoch, cluster map) — independent of the order the map lists the nodes.
    /// </summary>
    [Theory]
    [InlineData(4)]
    [InlineData(5)]
    [InlineData(7)]
    [Catalog("REP-008", "U", "INV-01", "INV-08", "INV-11", "INV-18")]
    public async Task REP_008_PlacementIsDeterministic_Cluster(int nodeCount)
    {
        await ClusterTest.RunAsync(TestClusterOptions.ThreeZones with { NodeCount = nodeCount }, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            DfsVolume vol = client.GetVolume(Vol);
            for (int i = 0; i < 4; i++)
                await vol.Files.WriteAllBytesAsync($"/d{i}", TestData.Bytes((ulong)(8_300 + i), 2 * 64 * 1024 + i), Ct);
            await cluster.WaitForRepairAsync(Vol, TimeSpan.FromSeconds(30), Ct);

            DfsStoragePolicy policy = (await vol.GetInfoAsync(Ct)).DefaultPolicy;
            ulong epoch = (await cluster.Admin.GetClusterStatusAsync(Ct)).PlacementEpoch;
            IReadOnlyList<PlacementNode> map = await PlacementNodesAsync(cluster.Admin);
            var reversed = map.Reverse().ToList();
            for (int i = 0; i < 4; i++)
            {
                foreach (ObjectId o in await ObjectsOfAsync(cluster, $"/d{i}"))
                {
                    var expected = Placement.SelectTargets(o, epoch, policy, map).Select(n => n.Name).ToHashSet(StringComparer.Ordinal);
                    Assert.Equal(expected, Placement.SelectTargets(o, epoch, policy, reversed).Select(n => n.Name).ToHashSet(StringComparer.Ordinal));
                    var holders = (await cluster.Inspect.GetObjectHoldersAsync(o)).ToHashSet(StringComparer.Ordinal);
                    Assert.True(expected.SetEquals(holders),
                        $"object {o}: holders [{string.Join(",", holders.Order())}] differ from deterministic targets [{string.Join(",", expected.Order())}] at epoch {epoch}");
                }
            }
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // REP-009 PlacementUsesEligibleCapacity
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Pure placement: full, draining/wrong-tier/disallowed (ineligible) nodes are never selected, not even as fallback;
    /// when the remaining eligible nodes cannot meet the target, the result is shorter and the policy is reported
    /// unsatisfiable instead of silently using forbidden nodes.
    /// </summary>
    [Fact]
    [Catalog("REP-009", "U", "INV-01", "INV-08", "INV-11", "INV-18")]
    public void REP_009_PlacementUsesEligibleCapacity_Placement()
    {
        var random = new SeededRandom(9_000);
        PlacementNode Node(string name, string zone, bool eligible, long free) =>
            new(NodeId.New(random), name, new DfsTopologyLabels("r1", zone, "rk1", "h-" + name, "d1"), eligible, free);
        var nodes = new List<PlacementNode>
        {
            Node("ok0", "z1", true, 1L << 30),
            Node("ok1", "z2", true, 1),
            Node("full", "z3", true, 0),
            Node("negative", "z3", true, -5),
            Node("draining", "z4", false, 1L << 30),
            Node("wrong-tier", "z5", false, 1L << 30),
        };
        var forbidden = new HashSet<string>(["full", "negative", "draining", "wrong-tier"], StringComparer.Ordinal);
        for (int i = 0; i < 300; i++)
        {
            ObjectId o = ObjectId.New(random);
            IReadOnlyList<PlacementNode> targets = Placement.SelectTargets(o, 2, Policy(3, 3, 2, 2), nodes);
            Assert.Equal(2, targets.Count);
            Assert.DoesNotContain(targets, t => forbidden.Contains(t.Name));
            IReadOnlyList<PlacementNode> fallback = Placement.FallbackOrder(o, 2, nodes);
            Assert.Equal(2, fallback.Count);
            Assert.DoesNotContain(fallback, t => forbidden.Contains(t.Name));
        }

        Assert.False(Placement.IsSatisfiable(Policy(3, 3, 2, 2), nodes, out string? reason));
        Assert.False(string.IsNullOrWhiteSpace(reason));
        Assert.True(Placement.IsSatisfiable(Policy(2, 2, 2, 2), nodes, out _));
        Assert.False(Placement.IsSatisfiable(Policy(1, 1, 1, 1), nodes.Where(n => forbidden.Contains(n.Name)).ToList(), out _));
    }

    /// <summary>
    /// Live five-zone cluster: a draining node and a node whose disk is full (below the admission reserve) receive no new
    /// copies; the drained node's existing copies move off; with only two eligible nodes a 3/2-ack write still succeeds on
    /// exactly those two (deficit reported); with one eligible node it fails with placement unavailability rather than
    /// falling back to full or draining nodes.
    /// </summary>
    [Fact]
    [Catalog("REP-009", "U", "INV-01", "INV-08", "INV-11", "INV-18")]
    public async Task REP_009_PlacementUsesEligibleCapacity_Cluster()
    {
        await ClusterTest.RunAsync(TestClusterOptions.ThreeZones with { NodeCount = 5, DefaultPolicy = Policy(3, 3, 2, 2) }, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            DfsVolume vol = client.GetVolume(Vol);
            for (int i = 0; i < 4; i++)
                await vol.Files.WriteAllBytesAsync($"/old{i}", TestData.Bytes((ulong)(9_100 + i), 2 * 64 * 1024), Ct);
            await cluster.WaitForRepairAsync(Vol, TimeSpan.FromSeconds(30), Ct);

            cluster.Node("n4").Disk.Fill(leaveFreeBytes: 4L << 20);
            await cluster.Admin.DrainNodeAsync("n3", Ct);
            await cluster.WaitUntilAsync(async () =>
            {
                for (int i = 0; i < 4; i++)
                {
                    foreach (ObjectId o in await ObjectsOfAsync(cluster, $"/old{i}"))
                    {
                        if ((await cluster.Inspect.GetObjectHoldersAsync(o)).Contains("n3"))
                            return false;
                    }
                }

                return true;
            }, TimeSpan.FromSeconds(30), "draining n3 must move its copies to eligible nodes", Ct);
            DfsNodeInfo n3 = (await cluster.Admin.ListNodesAsync(Ct)).Single(n => n.Name == "n3");
            Assert.True(n3.Status is DfsNodeStatus.Draining or DfsNodeStatus.Drained, $"n3 status {n3.Status}");

            for (int i = 0; i < 6; i++)
            {
                await vol.Files.WriteAllBytesAsync($"/new{i}", TestData.Bytes((ulong)(9_200 + i), 2 * 64 * 1024), Ct);
                foreach (ObjectId o in await ObjectsOfAsync(cluster, $"/new{i}"))
                {
                    IReadOnlyList<string> holders = await cluster.Inspect.GetObjectHoldersAsync(o);
                    Assert.DoesNotContain("n3", holders);
                    Assert.DoesNotContain("n4", holders);
                }
            }

            cluster.Node("n2").Disk.Fill(leaveFreeBytes: 4L << 20);
            await vol.Files.WriteAllBytesAsync("/two-eligible", TestData.Bytes(9_300, 2 * 64 * 1024), Ct);
            foreach (ObjectId o in await ObjectsOfAsync(cluster, "/two-eligible"))
            {
                var holders = (await cluster.Inspect.GetObjectHoldersAsync(o)).ToHashSet(StringComparer.Ordinal);
                Assert.True(holders.SetEquals(["n0", "n1"]), $"object {o} placed on [{string.Join(",", holders)}]; only n0/n1 are eligible");
            }

            Assert.True((await cluster.Admin.GetReplicaHealthAsync(Vol, Ct)).UnderReplicatedObjects > 0, "the unreachable third target must be reported as a deficit");

            cluster.Node("n1").Disk.Fill(leaveFreeBytes: 4L << 20);
            DfsPlacementUnavailableException ex = await AssertEx.ThrowsDfsAsync<DfsPlacementUnavailableException>(
                () => vol.Files.WriteAllBytesAsync("/one-eligible", TestData.Bytes(9_301, 2 * 64 * 1024), Ct), DfsErrorCode.PlacementUnavailable);
            AssertPlacementCounts(ex, requiredCopies: 2, maxAchievedCopies: 1, requiredZones: 2, maxAchievedZones: 1);
        }, Ct);
    }
}
