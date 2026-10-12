using System.Globalization;
using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Testing.Mesh;

namespace Tedd.Tdfs.Tests.Simulation.Mesh;

/// <summary>
/// MESH — clusters of 2..10 live nodes connected in different meshes (bootstrap/seed graphs, link meshes, zone
/// topologies, voter counts and data policies) under every failure kind, in deterministic whole-cluster simulation.
/// Every scenario runs a concurrent multi-client workload (histories checked for linearizability and strict
/// serializability), a namespace script checked against the reference model, and availability probes checked against
/// the independent quorum/placement oracle (<see cref="MeshOracle"/>): progress exactly when a majority of the
/// <b>configured</b> voters and the data acknowledgement policy are reachable, typed
/// <see cref="DfsUnavailableException"/> subclasses otherwise, never fabricated success. Safety (no split brain, no lost
/// acknowledged write, no stale or invented bytes) is checked during the fault and again after recovery, followed by
/// convergence of every replica. Matrices: critical dimensions exhaustive, non-critical pairwise (docs/test-notes/mesh.md).
/// </summary>
public sealed class MeshSimulationTests
{
    private const string Vol = MeshHelpers.Vol;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    // ============================================================================================================
    // MESH-001 — formation under every bootstrap mode and seed graph
    // ============================================================================================================

    public static TheoryData<int, string> FormationMatrix() => MeshHelpers.Matrix("MESH-001",
        () => MeshHelpers.NodeCounts.SelectMany(n => Enum.GetNames<MeshBoot>().Select(b => MeshHelpers.Row(("n", MeshHelpers.N(n)), ("boot", b)))),
        MeshConfig.DimensionsExcept("boot", "crash"));

    /// <summary>
    /// MESH-001: n nodes bootstrapped statically or by seed join over every seed graph form one cluster: every node is
    /// listed Active and reachable with its labels and a distinct id, the control group has exactly the configured
    /// (clipped) voters and every other node as learner, every node's control replica converges, the volume group has
    /// its configured voters with the declared placement, exactly one leader per term, every node works as the sole entry
    /// point of a client (full mesh) and the healthy workload is linearizable.
    /// </summary>
    [Theory]
    [MemberData(nameof(FormationMatrix))]
    [Catalog("MESH-001", "S", "INV-03", "INV-07", "INV-11", "INV-17", "INV-18")]
    public async Task MESH_001_MeshFormsUnderEveryBootstrapAndSeedGraph(int nodes, string config)
    {
        MeshConfig cfg = MeshHelpers.Config(nodes, config);
        await ClusterTest.RunAsync(MeshHelpers.Sim(cfg), async cluster =>
        {
            MeshScenario s = await MeshScenario.StartAsync(cluster, cfg, MeshHelpers.Options(namespaceScript: false), Ct);
            DfsAdminClient admin = s.Client("p0").Admin;

            IReadOnlyList<DfsNodeInfo> listed = await admin.ListNodesAsync(Ct);
            Assert.Equal(nodes, listed.Count);
            Assert.Equal(nodes, listed.Select(i => i.Id).Distinct().Count());
            foreach (TestNodeSpec spec in cfg.NodeSpecs())
            {
                DfsNodeInfo info = Assert.Single(listed, i => i.Name == spec.Name);
                Assert.Equal(DfsNodeStatus.Active, info.Status);
                Assert.Equal(spec.ToLabels(), info.Labels);
                Assert.Equal(cluster.Node(spec.Name).Id, info.Id);
                Assert.True(info.IsControlVoter == s.ControlVoters.Contains(spec.Name), $"{spec.Name} control-voter flag disagrees with the committed configuration");
                Assert.NotEmpty(info.Endpoints);
                if (cfg.Mesh == LinkMesh.Full)
                {
                    // On a partial mesh a node not adjacent to the control leader may legitimately be reported unreachable.
                    Assert.True(info.IsReachable, $"{spec.Name} must be reachable after formation");
                    Assert.True(info.Incarnation >= 1, $"{spec.Name} registered incarnation {info.Incarnation}");
                }
            }

            DfsClusterStatus status = await admin.GetClusterStatusAsync(Ct);
            Assert.Equal(cluster.ClusterId, status.Cluster.ClusterId);
            Assert.True(status.Control.HasQuorum, "control group must report a quorum after formation");
            Assert.Equal(s.ControlVoters, status.Control.Voters.Order(StringComparer.Ordinal));
            Assert.Contains(Vol, status.Volumes);
            DfsVolumeStatus volume = await admin.GetVolumeStatusAsync(Vol, Ct);
            Assert.True(volume.Group.HasQuorum);
            Assert.Equal(s.VolumeVoters, volume.Group.Voters.Order(StringComparer.Ordinal));

            if (cfg.Mesh == LinkMesh.Full)
            {
                // Partial meshes converge only after healing (checked by FinishAsync).
                await s.AssertControlConvergedAsync();
                foreach (TestNode node in cluster.Nodes)
                {
                    GroupInspection? control = await node.Inspect.GetGroupAsync("control");
                    Assert.NotNull(control);
                    Assert.Equal(s.ControlVoters, control.Voters.Order(StringComparer.Ordinal));
                }
            }

            await cluster.Inspect.AssertNoSplitBrainAsync();
            await s.ProbeAsync("formed");

            // Every node is a working entry point: a client that knows only n_i writes, the client that knows only n_{i+1} reads.
            // On a partial mesh a node that is not adjacent to the leaders may hold a stale cluster map (no relaying), so
            // connecting or entering through it may fail — but only typed (availability, or an unknown outcome carrying
            // its id for the write), never with wrong bytes, and never as a false absence of the acknowledged file.
            bool partial = cfg.Mesh != LinkMesh.Full;
            var unavailable = new HashSet<MeshCause> { MeshCause.MetadataQuorum, MeshCause.Unreachable, MeshCause.NoHealthyReplica };
            var entries = new string?[nodes];
            for (int i = 0; i < nodes; i++)
            {
                string name = "e" + i.ToString(CultureInfo.InvariantCulture);
                try
                {
                    await s.ConnectClientAsync(name, [cluster[i].Name]);
                    entries[i] = name;
                }
                catch (Exception ex) when (partial && ex is not Xunit.Sdk.XunitException)
                {
                    Assert.True(MeshOracle.Unwrap(ex) is DfsUnavailableException,
                        $"connecting through {cluster[i].Name} of a {cfg.Mesh} mesh failed untyped: {MeshOracle.Describe(ex)}");
                }
            }

            for (int i = 0; i < nodes; i++)
            {
                byte[] data = TestData.Bytes(1000UL + (ulong)i, 6000);
                string path = "/mesh/entry-" + cluster[i].Name;
                string? writer = entries[i];
                string? reader = entries[(i + 1) % nodes];
                string route = $"written through entry {cluster[i].Name}, read through entry {cluster[(i + 1) % nodes].Name}";
                if (!partial)
                {
                    await s.Client(writer!).GetVolume(Vol).Files.WriteAllBytesAsync(path, data, Ct);
                    byte[] back = await s.Client(reader!).GetVolume(Vol).Files.ReadAllBytesAsync(path, Ct);
                    MeshHelpers.BytesEqual(data, back, route);
                    continue;
                }

                if (writer is null || reader is null)
                    continue;
                bool written = false;
                try
                {
                    await s.Client(writer).GetVolume(Vol).Files.WriteAllBytesAsync(path, data, Ct);
                    written = true;
                }
                catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
                {
                    string? why = MeshOracle.CheckFailure(ex, unavailable, mutation: true, s.Policy);
                    Assert.True(why is null, $"partial-mesh write {route}: {why}");
                }

                if (!written)
                    continue;
                try
                {
                    byte[] back = await s.Client(reader).GetVolume(Vol).Files.ReadAllBytesAsync(path, Ct);
                    MeshHelpers.BytesEqual(data, back, "partial-mesh entry round trip " + route);
                }
                catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
                {
                    Assert.False(MeshOracle.Unwrap(ex) is FileNotFoundException,
                        $"acknowledged write reported absent ({route}): a stale entry node must not answer a live read (INV-03, INV-08)");
                    string? why = MeshOracle.CheckFailure(ex, unavailable, mutation: false, s.Policy);
                    Assert.True(why is null, $"partial-mesh read {route}: {why}");
                }
            }

            await s.FinishAsync();
        }, Ct);
    }

    // ============================================================================================================
    // MESH-002 — healthy mesh: concurrent workload histories
    // ============================================================================================================

    public static TheoryData<int, string> HealthyMatrix() => MeshHelpers.Matrix("MESH-002",
        () => MeshHelpers.ByNodes(MeshHelpers.NodeCounts), MeshHelpers.NoCrash);

    /// <summary>
    /// MESH-002: a healthy mesh of n nodes serves four concurrent clients that each enter through a different node
    /// (full mesh) doing writes, reads, deletes, appends, renames, acknowledged unique writes and transactions: every
    /// key's history is linearizable, transactions are strictly serializable (including predicate reads), the namespace
    /// script matches the reference model, every acknowledged write survives with exact bytes, and all replicas converge.
    /// </summary>
    [Theory]
    [MemberData(nameof(HealthyMatrix))]
    [Catalog("MESH-002", "S", "INV-01", "INV-02", "INV-03", "INV-04", "INV-06", "INV-07")]
    public async Task MESH_002_HealthyMeshWorkloadIsLinearizableAndSerializable(int nodes, string config)
    {
        MeshConfig cfg = MeshHelpers.Config(nodes, config);
        await ClusterTest.RunAsync(MeshHelpers.Sim(cfg), async cluster =>
        {
            Func<int, IReadOnlyList<string>>? seeds = cfg.Mesh == LinkMesh.Full ? i => [cluster[(i * nodes / 4) % nodes].Name] : null;
            MeshScenarioOptions options = MeshHelpers.Options() with
            {
                WorkloadOptions = new MeshWorkloadOptions { Clients = 4, RegisterKeys = 4, Logs = 2, RenameDirectories = 2, SeedsForClient = seeds },
                NamespaceSteps = 24,
            };
            MeshScenario s = await MeshScenario.StartAsync(cluster, cfg, options, Ct);
            await s.ProbeAsync("healthy");
            MeshWorkload workload = s.Workload!;
            long mark = workload.Mark();
            await cluster.DelayAsync(TimeSpan.FromSeconds(4), Ct);
            foreach (string process in workload.Processes)
                Assert.True(workload.OkOperationsSince(process, mark) > 0, $"{process} made no progress in a healthy mesh");
            await s.AssertSafetyAsync("healthy");
            await s.FinishAsync();
        }, Ct);
    }

    // ============================================================================================================
    // MESH-003 — partial link meshes
    // ============================================================================================================

    public static TheoryData<int, string> PartialMeshMatrix() => MeshHelpers.Matrix("MESH-003",
        () => MeshHelpers.NodeCounts.SelectMany(n => new[] { "Ring", "Star", "Line" }.Select(m => MeshHelpers.Row(("n", MeshHelpers.N(n)), ("mesh", m)))),
        MeshConfig.DimensionsExcept("mesh"));

    /// <summary>
    /// MESH-003: with only Ring/Star/Line node-to-node links (voters placed where the mesh can elect them), the cluster
    /// forms, metadata-only operations succeed (a leader exists and clients reach every node), every other operation
    /// either succeeds with verified bytes or fails typed, and there is never more than one leader per term. Crashing the
    /// volume leader is never fabricated into success when no connected majority remains. After the mesh restriction is
    /// healed every node — including those never adjacent to a leader — catches up and all histories are valid.
    /// </summary>
    [Theory]
    [MemberData(nameof(PartialMeshMatrix))]
    [Catalog("MESH-003", "S", "INV-03", "INV-08", "INV-11", "INV-17", "INV-18")]
    public async Task MESH_003_PartialLinkMeshesKeepSafetyAndBoundedAvailability(int nodes, string config)
    {
        MeshConfig cfg = MeshHelpers.Config(nodes, config);
        await ClusterTest.RunAsync(MeshHelpers.Sim(cfg), async cluster =>
        {
            MeshScenario s = await MeshScenario.StartAsync(cluster, cfg, MeshHelpers.Options(), Ct);
            MeshExpectation formed = MeshExpectation.Must("the cluster formed under this mesh (a volume leader exists) and clients reach every node");
            await s.ProbeOneAsync("mesh", "p0", MeshProbeKind.ReadSmall, s.Expect(MeshProbeKind.ReadSmall, "p0", formed));
            await s.ProbeOneAsync("mesh", "p0", MeshProbeKind.WriteSmall, s.Expect(MeshProbeKind.WriteSmall, "p0", formed));
            await s.ProbeAsync("mesh");
            await s.AssertSafetyAsync("mesh");

            string leader = await s.VolumeLeaderAsync();
            await s.CrashAsync(leader);
            await s.ProbeAsync("mesh-leader-down");
            await s.AssertSafetyAsync("mesh-leader-down");
            await s.RestartAsync(leader);
            await s.AssertSafetyAsync("mesh-leader-back");

            await s.FinishAsync(); // heals the mesh restriction, so every node must catch up
        }, Ct);
    }

    // ============================================================================================================
    // MESH-004 — volume leader crash and restart
    // ============================================================================================================

    public static TheoryData<int, string> VolumeLeaderCrashMatrix() => MeshHelpers.Matrix("MESH-004",
        () => MeshHelpers.ByNodes(MeshHelpers.NodeCounts), MeshConfig.AllDimensions);

    /// <summary>
    /// MESH-004: crash the volume leader under load (every crash mode). With three or more configured voters a surviving
    /// voter is elected in a higher term within the bound and every operation the oracle permits succeeds; with one voter
    /// the volume is unavailable (typed) until the restart. The restarted node advances its incarnation, rejoins as a
    /// follower of at least the new term and catches up; no acknowledged write is lost.
    /// </summary>
    [Theory]
    [MemberData(nameof(VolumeLeaderCrashMatrix))]
    [Catalog("MESH-004", "S", "INV-01", "INV-03", "INV-05", "INV-08", "INV-17", "INV-18")]
    public async Task MESH_004_VolumeLeaderCrashAndRestart(int nodes, string config)
    {
        MeshConfig cfg = MeshHelpers.Config(nodes, config);
        await ClusterTest.RunAsync(MeshHelpers.Sim(cfg), async cluster =>
        {
            MeshScenario s = await MeshScenario.StartAsync(cluster, cfg, MeshHelpers.Options(), Ct);
            string leader = await s.VolumeLeaderAsync();
            ulong term = (await s.VolumeGroupAsync()).Term;
            await cluster.DelayAsync(TimeSpan.FromSeconds(1), Ct);
            await s.CrashAsync(leader);

            MeshExpectation quorum = MeshOracle.Quorum(s.Net, s.VolumeVoters, "client:p0", MeshCause.MetadataQuorum);
            if (quorum.Outcome == MeshTri.Must)
            {
                string successor = await s.WaitForVolumeLeaderAmongAsync(s.VolumeVoters.Where(v => v != leader).ToArray(),
                    $"a surviving voter of [{string.Join(",", s.VolumeVoters)}] must take over from crashed {leader}");
                Assert.True((await s.VolumeGroupAsync()).Term > term, $"successor {successor} must lead in a term above {term}");
            }

            await s.ProbeAsync("leader-crashed");
            await s.AssertSafetyAsync("leader-crashed");
            ulong termWhileDown = quorum.Outcome == MeshTri.Must ? (await s.VolumeGroupAsync()).Term : term;

            await s.RestartAsync(leader);
            await s.WaitSettledAsync();
            // The restarted node learns the current term from the leader's heartbeats, so this is asserted (bounded) when it
            // is directly linked to the current leader; on a partial mesh it may be adjacent only to other followers
            // (no relaying) and is checked after healing by FinishAsync's convergence checks.
            string current = await s.VolumeLeaderAsync();
            if (current == leader || s.Net.Linked(leader, current))
            {
                await cluster.WaitUntilAsync(async () => (await cluster.Node(leader).Inspect.GetGroupAsync(Vol))?.Term >= termWhileDown,
                    s.Options.MustWithin, $"restarted {leader} must adopt at least the term {termWhileDown} of the leader that served while it was down", Ct);
            }

            await s.FinishAsync();
        }, Ct);
    }

    // ============================================================================================================
    // MESH-005 — control leader crash and restart
    // ============================================================================================================

    public static TheoryData<int, string> ControlLeaderCrashMatrix() => MeshHelpers.Matrix("MESH-005",
        () => MeshHelpers.ByNodes(MeshHelpers.NodeCounts), MeshConfig.AllDimensions);

    /// <summary>
    /// MESH-005: crash the control-group leader under load. Membership/principal changes continue exactly when a majority
    /// of the configured control voters survives (new control leader in a higher term) and fail typed otherwise;
    /// established volume operations follow the volume group's own quorum and data policy only (CON-017: no
    /// cluster-wide majority is invented). Restart, catch-up and convergence follow.
    /// </summary>
    [Theory]
    [MemberData(nameof(ControlLeaderCrashMatrix))]
    [Catalog("MESH-005", "S", "INV-08", "INV-11", "INV-17", "INV-18")]
    public async Task MESH_005_ControlLeaderCrashAndRestart(int nodes, string config)
    {
        MeshConfig cfg = MeshHelpers.Config(nodes, config);
        await ClusterTest.RunAsync(MeshHelpers.Sim(cfg), async cluster =>
        {
            MeshScenario s = await MeshScenario.StartAsync(cluster, cfg, MeshHelpers.Options(), Ct);
            string leader = await s.ControlLeaderAsync();
            ulong term = (await cluster.Inspect.GetControlGroupAsync()).Term;
            await s.CrashAsync(leader);

            MeshExpectation control = s.Expect(MeshProbeKind.ControlChange, "p0");
            if (control.Outcome == MeshTri.Must)
            {
                await cluster.WaitUntilAsync(async () =>
                {
                    GroupInspection g = await cluster.Inspect.GetControlGroupAsync();
                    return g.Leader is not null && g.Leader != leader && g.Term > term;
                }, s.Options.MustWithin, $"a surviving control voter of [{string.Join(",", s.ControlVoters)}] must take over from {leader} in a higher term", Ct);
            }
            else
            {
                Assert.True(s.ControlVoters.Count <= 2 || s.Net.IsFullyConnected == false,
                    $"control loss expected only with at most two configured control voters in a full mesh: {control}");
            }

            await s.ProbeAsync("control-leader-crashed");
            await s.AssertSafetyAsync("control-leader-crashed");
            await s.RestartAsync(leader);
            await s.FinishAsync();
        }, Ct);
    }

    // ============================================================================================================
    // MESH-006 / MESH-007 — crashing voters within and beyond tolerance
    // ============================================================================================================

    public static TheoryData<int, string> WithinToleranceMatrix() => MeshHelpers.Matrix("MESH-006",
        () => MeshHelpers.ByNodes(MeshHelpers.NodeCounts, "WithLeader", "FollowersOnly"), MeshConfig.AllDimensions,
        c => c.Variant != "WithLeader" || c.VolumeVoters >= 3);

    /// <summary>
    /// MESH-006: crash exactly as many configured volume voters as the group tolerates ((V-1)/2, with or without the
    /// leader; a non-voter when the tolerance is zero) at once under load. Metadata stays available (a majority of the
    /// configured voters survives; full mesh exact), data operations follow the acknowledgement policy over the surviving
    /// hosts/zones, reads succeed when a verified holder survives, and every acknowledged write remains readable during the
    /// fault. Restart converges.
    /// </summary>
    [Theory]
    [MemberData(nameof(WithinToleranceMatrix))]
    [Catalog("MESH-006", "S", "INV-01", "INV-03", "INV-08", "INV-11", "INV-17")]
    public async Task MESH_006_CrashVotersWithinTolerance(int nodes, string config)
    {
        MeshConfig cfg = MeshHelpers.Config(nodes, config);
        await ClusterTest.RunAsync(MeshHelpers.Sim(cfg), async cluster =>
        {
            MeshScenario s = await MeshScenario.StartAsync(cluster, cfg, MeshHelpers.Options(), Ct);
            string leader = await s.VolumeLeaderAsync();
            int tolerance = MeshOracle.Tolerance(s.VolumeVoters.Count);
            string[] followers = s.VolumeVoters.Where(v => v != leader).ToArray();
            string[] victims = cfg.Variant == "WithLeader"
                ? followers.Take(tolerance - 1).Prepend(leader).ToArray()
                : tolerance > 0 ? followers.Take(tolerance).ToArray() : MeshHelpers.Names(s).Where(n => !s.VolumeVoters.Contains(n)).Take(1).ToArray();
            Assert.NotEmpty(victims);
            foreach (string victim in victims)
                await s.CrashAsync(victim);

            // Full mesh: a majority of the configured voters survives and forms a clique, so metadata must stay available.
            // Partial mesh: the survivors may no longer be connected to each other (e.g. the middle voter of a line), in
            // which case the oracle legitimately answers Never; only the probes below are asserted there.
            MeshExpectation quorum = MeshOracle.Quorum(s.Net, s.VolumeVoters, "client:p0", MeshCause.MetadataQuorum);
            Assert.True(s.VolumeVoters.Count - victims.Count(s.VolumeVoters.Contains) >= MeshOracle.Majority(s.VolumeVoters.Count),
                $"setup must crash at most {tolerance} of {s.VolumeVoters.Count} configured voters, crashed [{string.Join(",", victims)}]");
            if (s.Net.Mesh == LinkMesh.Full)
                Assert.True(quorum.Outcome == MeshTri.Must, $"crashing {victims.Length} of {s.VolumeVoters.Count} voters is within tolerance on a full mesh: {quorum}");
            await s.ProbeAsync("within-tolerance");
            await s.AssertSafetyAsync("within-tolerance");
            foreach (string victim in victims.Reverse())
                await s.RestartAsync(victim);
            await s.FinishAsync();
        }, Ct);
    }

    public static TheoryData<int, string> BeyondToleranceMatrix() => MeshHelpers.Matrix("MESH-007",
        () => MeshHelpers.ByNodes(MeshHelpers.NodeCounts, "Minimal", "AllVoters"), MeshConfig.AllDimensions);

    /// <summary>
    /// MESH-007: crash one voter more than the group tolerates (followers first, so the old leader survives in a minority)
    /// or every voter. Every new metadata operation fails with <see cref="DfsQuorumUnavailableException"/> (mutations may
    /// report an unknown outcome carrying their id), reads never return stale data, and the surviving voters' commit
    /// index does not advance (no success without the configured majority, CON-007). After restart every unknown outcome
    /// resolves definitely and the histories remain valid.
    /// </summary>
    [Theory]
    [MemberData(nameof(BeyondToleranceMatrix))]
    [Catalog("MESH-007", "S", "INV-01", "INV-06", "INV-08", "INV-11", "INV-17")]
    public async Task MESH_007_CrashVotersBeyondTolerance(int nodes, string config)
    {
        MeshConfig cfg = MeshHelpers.Config(nodes, config);
        await ClusterTest.RunAsync(MeshHelpers.Sim(cfg), async cluster =>
        {
            MeshScenario s = await MeshScenario.StartAsync(cluster, cfg, MeshHelpers.Options(), Ct);
            string leader = await s.VolumeLeaderAsync();
            int count = cfg.Variant == "Minimal" ? MeshOracle.Tolerance(s.VolumeVoters.Count) + 1 : s.VolumeVoters.Count;
            string[] victims = s.VolumeVoters.Where(v => v != leader).Append(leader).Take(count).ToArray();
            foreach (string victim in victims)
                await s.CrashAsync(victim);
            Assert.Equal(MeshTri.Never, MeshOracle.Quorum(s.Net, s.VolumeVoters, "client:p0", MeshCause.MetadataQuorum).Outcome);

            // Acknowledgements the victims sent before crashing are legitimate evidence and may still be delivered, and
            // followers learn an earlier commit index from heartbeats: let that settle, then nothing new may commit.
            string[] survivors = s.VolumeVoters.Except(victims).ToArray();
            await cluster.DelayAsync(s.ElectionWindow * 2, Ct);
            ulong settled = 0;
            foreach (string survivor in survivors)
                settled = Math.Max(settled, (await cluster.Node(survivor).Inspect.GetGroupAsync(Vol))!.CommitIndex);

            await s.ProbeAsync("beyond-tolerance");
            await s.AssertSafetyAsync("beyond-tolerance");
            foreach (string survivor in survivors)
            {
                GroupInspection g = (await cluster.Node(survivor).Inspect.GetGroupAsync(Vol))!;
                Assert.True(g.CommitIndex <= settled,
                    $"surviving voter {survivor} reached commit index {g.CommitIndex} > {settled} (the highest index committed before the majority was lost) " +
                    $"with only [{string.Join(",", survivors)}] of the configured voters [{string.Join(",", s.VolumeVoters)}] alive (CON-007, MUT-004)");
            }

            foreach (string victim in victims.Reverse())
                await s.RestartAsync(victim);
            await s.FinishAsync();
        }, Ct);
    }

    // ============================================================================================================
    // MESH-008 — storage-node loss against the acknowledgement policy
    // ============================================================================================================

    public static TheoryData<int, string> AckPolicyMatrix() => MeshHelpers.Matrix("MESH-008",
        () => MeshHelpers.NodeCounts.SelectMany(n => new[] { "1", "2", "3" }.SelectMany(a => new[] { "At", "Below" }
            .Select(side => MeshHelpers.Row(("n", MeshHelpers.N(n)), ("ack", a), ("variant", side))))),
        MeshHelpers.FullMesh("ack"),
        c =>
        {
            int survivors = c.Variant == "At" ? c.Ack : c.Ack - 1;
            return survivors >= 1 && survivors >= MeshOracle.Majority(c.VolumeVoters) && survivors <= c.Nodes;
        });

    /// <summary>
    /// MESH-008: keep a majority of the volume voters but crash storage hosts until exactly the acknowledged number of
    /// hosts survives ("At", chosen to span as many zones as possible) or one fewer ("Below"). At the threshold extent
    /// writes succeed iff the surviving zones also satisfy the acknowledgement zones; below it every extent write fails
    /// with <see cref="DfsPlacementUnavailableException"/> naming the unchanged required copies/zones and fewer achieved,
    /// although consensus is healthy (OFF-008). Inline writes depend only on the metadata quorum when the voters'
    /// guaranteed durability satisfies the policy. Restart and repair restore the target.
    /// </summary>
    [Theory]
    [MemberData(nameof(AckPolicyMatrix))]
    [Catalog("MESH-008", "S", "INV-01", "INV-08", "INV-11", "INV-17")]
    public async Task MESH_008_StorageNodeLossAgainstAckPolicy(int nodes, string config)
    {
        MeshConfig cfg = MeshHelpers.Config(nodes, config);
        await ClusterTest.RunAsync(MeshHelpers.Sim(cfg), async cluster =>
        {
            MeshScenario s = await MeshScenario.StartAsync(cluster, cfg, MeshHelpers.Options(), Ct);
            string leader = await s.VolumeLeaderAsync();
            int keep = cfg.Variant == "At" ? cfg.Ack : cfg.Ack - 1;

            // Survivors: the leader and enough voters for a majority, then nodes from new zones, then the rest.
            var survivors = new List<string> { leader };
            survivors.AddRange(s.VolumeVoters.Where(v => v != leader).Take(MeshOracle.Majority(s.VolumeVoters.Count) - 1));
            foreach (TestNodeSpec spec in s.Net.Nodes.OrderBy(n => n.Name, StringComparer.Ordinal))
            {
                if (survivors.Count >= keep)
                    break;
                if (!survivors.Contains(spec.Name) && survivors.All(x => s.Net.Spec(x).Zone != spec.Zone))
                    survivors.Add(spec.Name);
            }

            foreach (string name in MeshHelpers.Names(s))
            {
                if (survivors.Count >= keep)
                    break;
                if (!survivors.Contains(name))
                    survivors.Add(name);
            }

            Assert.Equal(keep, survivors.Count);
            foreach (string victim in MeshHelpers.Names(s).Except(survivors))
                await s.CrashAsync(victim);

            Assert.Equal(MeshTri.Must, MeshOracle.Quorum(s.Net, s.VolumeVoters, "client:p0", MeshCause.MetadataQuorum).Outcome);
            MeshExpectation large = s.Expect(MeshProbeKind.WriteLarge, "p0");
            (int hosts, int zones) = MeshOracle.CountDomains(s.Net, survivors);
            bool satisfiable = hosts >= s.Policy.DurableCopiesBeforeAck && zones >= s.Policy.DurableZonesBeforeAck;
            Assert.Equal(satisfiable ? MeshTri.Must : MeshTri.Never, large.Outcome);
            if (cfg.Variant == "Below")
                Assert.True(!satisfiable && large.Causes.SetEquals([MeshCause.Placement]), $"one host below the threshold must be a pure placement failure: {large}");

            await s.ProbeAsync("ack-" + cfg.Variant);
            await s.AssertSafetyAsync("ack-" + cfg.Variant);
            foreach (string victim in MeshHelpers.Names(s).Except(survivors))
                await s.RestartAsync(victim);
            await s.FinishAsync();
        }, Ct);
    }

    // ============================================================================================================
    // MESH-009 — majority/minority partitions
    // ============================================================================================================

    public static TheoryData<int, string> PartitionMatrix() => MeshHelpers.Matrix("MESH-009",
        () => MeshHelpers.ByNodes(MeshHelpers.NodeCounts, "LeaderInMajority", "LeaderInMinority"), MeshHelpers.NoCrash,
        c => c.Variant != "LeaderInMinority" || c.VolumeVoters >= 3);

    /// <summary>
    /// MESH-009: split the nodes into a side holding a majority of the volume voters and the rest, with the old leader on
    /// either side and one probe client (and half the workload) on each side. The majority side elects (if needed) and
    /// serves exactly what the oracle permits; the minority-side client gets typed failures only — no lease, no live
    /// read, no acknowledged write (AT-06) — and the old leader never commits again in its old term. After healing, the
    /// minority's unknown outcomes resolve definitely and everything converges.
    /// </summary>
    [Theory]
    [MemberData(nameof(PartitionMatrix))]
    [Catalog("MESH-009", "S", "INV-01", "INV-03", "INV-05", "INV-06", "INV-08", "INV-18")]
    public async Task MESH_009_MajorityMinorityPartition(int nodes, string config)
    {
        MeshConfig cfg = MeshHelpers.Config(nodes, config);
        await ClusterTest.RunAsync(MeshHelpers.Sim(cfg), async cluster =>
        {
            MeshScenario s = await MeshScenario.StartAsync(cluster, cfg, MeshHelpers.Options(probeClients: ["p0", "p1"]), Ct);
            string leader = await s.VolumeLeaderAsync();
            ulong term = (await s.VolumeGroupAsync()).Term;
            int majority = MeshOracle.Majority(s.VolumeVoters.Count);
            string[] others = s.VolumeVoters.Where(v => v != leader).ToArray();
            string[] majorityVoters = cfg.Variant == "LeaderInMajority"
                ? others.Take(majority - 1).Prepend(leader).ToArray()
                : others.Take(majority).ToArray();
            string[] nonVoters = MeshHelpers.Names(s).Where(n => !s.VolumeVoters.Contains(n)).ToArray();
            string[] sideA = majorityVoters.Concat(nonVoters.Where((_, i) => i % 2 == 1)).ToArray();
            string[] sideB = MeshHelpers.Names(s).Except(sideA).ToArray();
            int index = 0;
            Assert.NotEmpty(sideB);
            s.PartitionNodes([sideA, sideB], client => client == "client:p1" ? 1 : client == "client:p0" || client == "client:ns" ? 0 : index++ % 2);

            if (cfg.Variant == "LeaderInMinority" && s.Net.Mesh == LinkMesh.Full)
            {
                await s.WaitForVolumeLeaderAmongAsync(majorityVoters, "the majority side must elect a new leader");
                Assert.True((await s.VolumeGroupAsync()).Term > term, "the majority side's leader must lead in a higher term");
            }

            await s.ProbeAsync("partitioned");
            await s.AssertSafetyAsync("partitioned");
            if (cfg.Variant == "LeaderInMinority")
            {
                GroupInspection old = (await cluster.Node(leader).Inspect.GetGroupAsync(Vol))!;
                await cluster.DelayAsync(s.ElectionWindow * 3, Ct);
                GroupInspection later = (await cluster.Node(leader).Inspect.GetGroupAsync(Vol))!;
                Assert.True(later.CommitIndex == old.CommitIndex,
                    $"isolated old leader {leader} advanced its commit index {old.CommitIndex} -> {later.CommitIndex} without a majority (term {later.Term})");
                Assert.True(later.Role != RaftRole.Leader || later.Term == term,
                    $"isolated old leader {leader} cannot win a new term without a majority (role {later.Role}, term {later.Term})");
            }

            await s.FinishAsync();
        }, Ct);
    }

    // ============================================================================================================
    // MESH-010 — asymmetric and half-open links
    // ============================================================================================================

    public static TheoryData<int, string> AsymmetricMatrix() => MeshHelpers.Matrix("MESH-010",
        () => MeshHelpers.ByNodes(MeshHelpers.NodeCounts, "LeaderOutboundCut", "LeaderInboundCut", "LeaderBlackHole", "FollowerOneWayCut"),
        MeshHelpers.FullMesh("crash"),
        c => c.Variant != "FollowerOneWayCut" || c.VolumeVoters >= 3);

    /// <summary>
    /// MESH-010: cut only the leader's outbound or inbound node links, black-hole its outbound traffic (half-open), or cut
    /// one direction between the leader and one follower. Consensus counts completed exchanges, never one-way
    /// reachability (NET-011): a leader that cannot complete exchanges with a majority is replaced in a higher term and
    /// the rest progress; a single one-way cut to one follower leaves the leader with a working majority, and PreVote /
    /// CheckQuorum keep the cut follower from disrupting it (the term does not change).
    /// </summary>
    [Theory]
    [MemberData(nameof(AsymmetricMatrix))]
    [Catalog("MESH-010", "S", "INV-03", "INV-05", "INV-08", "INV-17", "INV-18")]
    public async Task MESH_010_AsymmetricAndHalfOpenLinks(int nodes, string config)
    {
        MeshConfig cfg = MeshHelpers.Config(nodes, config);
        await ClusterTest.RunAsync(MeshHelpers.Sim(cfg), async cluster =>
        {
            MeshScenario s = await MeshScenario.StartAsync(cluster, cfg, MeshHelpers.Options(), Ct);
            string leader = await s.VolumeLeaderAsync();
            ulong term = (await s.VolumeGroupAsync()).Term;
            string[] others = MeshHelpers.Names(s).Where(n => n != leader).ToArray();
            Func<string, MeshExpectation?>? metadata = null;
            switch (cfg.Variant)
            {
                case "LeaderOutboundCut":
                    foreach (string o in others)
                        s.Cut(leader, o);
                    break;
                case "LeaderInboundCut":
                    foreach (string o in others)
                        s.Cut(o, leader);
                    break;
                case "LeaderBlackHole":
                    foreach (string o in others)
                        s.BlackHole(leader, o);
                    break;
                case "FollowerOneWayCut":
                    string follower = s.VolumeVoters.First(v => v != leader);
                    s.Cut(leader, follower);
                    // The leader still completes exchanges with V-2 >= majority-1 other voters; PreVote with leader stickiness
                    // and CheckQuorum (ARCHITECTURE §4) stop the cut follower from forcing an election.
                    metadata = _ => MeshExpectation.Must($"leader {leader} keeps a working majority; only {leader}>{follower} is cut");
                    break;
            }

            if (cfg.Variant != "FollowerOneWayCut" && s.VolumeVoters.Count >= 3)
            {
                await s.WaitForVolumeLeaderAmongAsync(s.VolumeVoters.Where(v => v != leader).ToArray(),
                    $"{cfg.Variant}: {leader} cannot complete exchanges with any voter and must be replaced");
                Assert.True((await s.VolumeGroupAsync()).Term > term);
            }

            await s.ProbeAsync("asymmetric", metadataFor: metadata);
            await s.AssertSafetyAsync("asymmetric");
            if (cfg.Variant == "FollowerOneWayCut")
            {
                GroupInspection g = await s.VolumeGroupAsync();
                Assert.True(g.Leader == leader && g.Term == term, $"a one-way cut to one follower must not disrupt leader {leader} in term {term} (now {g.Leader} term {g.Term})");
            }

            await s.FinishAsync();
        }, Ct);
    }

    // ============================================================================================================
    // MESH-011 — client isolation variants
    // ============================================================================================================

    public static TheoryData<int, string> ClientIsolationMatrix() => MeshHelpers.Matrix("MESH-011",
        () => MeshHelpers.ByNodes(MeshHelpers.NodeCounts, "FullyIsolated", "OnlyLeader", "AllButLeader", "OnlyNonVoter"),
        MeshHelpers.FullMesh("crash"),
        c => c.Variant != "OnlyNonVoter" || c.Nodes > c.VolumeVoters);

    /// <summary>
    /// MESH-011: partitions that separate one client differently from the servers (servers stay fully connected). A fully
    /// isolated client gets typed failures only (mutations may be unknown with their id) while every other client keeps
    /// progressing; a client that reaches only the leader keeps metadata access but extent writes need the acknowledged
    /// number of reachable hosts (placement failure otherwise); a client that cannot reach the leader may only fail typed.
    /// Healing restores service and resolves every unknown outcome.
    /// </summary>
    [Theory]
    [MemberData(nameof(ClientIsolationMatrix))]
    [Catalog("MESH-011", "S", "INV-06", "INV-08", "INV-12", "INV-17")]
    public async Task MESH_011_ClientIsolationVariants(int nodes, string config)
    {
        MeshConfig cfg = MeshHelpers.Config(nodes, config);
        await ClusterTest.RunAsync(MeshHelpers.Sim(cfg), async cluster =>
        {
            MeshScenario s = await MeshScenario.StartAsync(cluster, cfg, MeshHelpers.Options(), Ct);
            string leader = await s.VolumeLeaderAsync();
            const string client = "client:p0";
            string[] reach = cfg.Variant switch
            {
                "FullyIsolated" => [],
                "OnlyLeader" => [leader],
                "AllButLeader" => MeshHelpers.Names(s).Where(n => n != leader).ToArray(),
                _ => [MeshHelpers.Names(s).First(n => !s.VolumeVoters.Contains(n))],
            };
            if (reach.Length == 0)
            {
                s.Isolate(client);
            }
            else
            {
                foreach (string node in MeshHelpers.Names(s).Except(reach))
                    s.CutBoth(client, node);
            }

            // OnlyLeader: the healthy leader does not change, so metadata is certainly reachable.
            Func<string, MeshExpectation?>? metadata = cfg.Variant == "OnlyLeader"
                ? _ => MeshExpectation.Must($"{client} reaches the stable leader {leader}")
                : null;
            MeshWorkload workload = s.Workload!;
            long mark = workload.Mark();
            await s.ProbeAsync("client-" + cfg.Variant, metadataFor: metadata);
            await cluster.WaitUntilAsync(() => Task.FromResult(workload.Processes.All(p => workload.OkOperationsSince(p, mark) > 0)),
                s.Options.MustWithin, "isolating one client must not stop other clients", Ct);
            await s.AssertSafetyAsync("client-" + cfg.Variant, "p0");
            await s.FinishAsync();
        }, Ct);
    }

    // ============================================================================================================
    // MESH-012 — pauses beyond the election and lease windows
    // ============================================================================================================

    public static TheoryData<int, string> PauseMatrix() => MeshHelpers.Matrix("MESH-012",
        () => MeshHelpers.ByNodes(MeshHelpers.NodeCounts, "VolumeLeader", "ControlLeader", "OtherNode"), MeshHelpers.NoCrash);

    /// <summary>
    /// MESH-012: pause a node (volume leader, control leader or another node) for longer than both the election window
    /// and the lease duration while clients keep writing, then resume it with its queued work. The rest of the cluster
    /// behaves as if the node were down (oracle); a value committed by the new authority during the pause is still the
    /// value read after the resumed node's stale tasks run (no regained authority, OFF-021); the resumed node adopts the
    /// newer term.
    /// </summary>
    [Theory]
    [MemberData(nameof(PauseMatrix))]
    [Catalog("MESH-012", "S", "INV-01", "INV-03", "INV-05", "INV-08", "INV-18")]
    public async Task MESH_012_PausedNodeCannotRegainAuthority(int nodes, string config)
    {
        MeshConfig cfg = MeshHelpers.Config(nodes, config);
        await ClusterTest.RunAsync(MeshHelpers.Sim(cfg), async cluster =>
        {
            MeshScenario s = await MeshScenario.StartAsync(cluster, cfg, MeshHelpers.Options(), Ct);
            string volumeLeader = await s.VolumeLeaderAsync();
            string target = cfg.Variant switch
            {
                "VolumeLeader" => volumeLeader,
                "ControlLeader" => await s.ControlLeaderAsync(),
                _ => MeshHelpers.Names(s).Where(n => n != volumeLeader).OrderByDescending(n => n, StringComparer.Ordinal).First(),
            };
            // OFF-021 needs stale work queued at the paused node: a client whose route points at the volume leader (it
            // entered through it and wrote the key there) sends a write of the same key right after the pause, with an
            // operation id reserved beforehand. When the volume leader is the paused node that request waits in its queue.
            const string key = "/mesh/pause-key";
            byte[] before = TestData.Bytes(4241, 300);
            byte[] stale = TestData.Bytes(4243, 300);
            byte[] during = TestData.Bytes(4242, 300);
            DfsVolume staleVolume = (await s.ConnectClientAsync("stale", [volumeLeader])).GetVolume(Vol);
            await MeshHelpers.MustAsync(s, () => staleVolume.Files.WriteAllBytesAsync(key, before, Ct), "initial value through the client routed to the leader");
            DfsOperationId staleId = await staleVolume.NewOperationIdAsync(Ct);

            ulong term = (await s.VolumeGroupAsync()).Term;
            await s.PauseAsync(target);
            Task<CommitToken> staleWrite = staleVolume.Files.WriteAllBytesAsync(key, stale, new DfsMutationOptions { OperationId = staleId }, Ct);
            await s.ProbeAsync("paused");

            MeshExpectation quorum = MeshOracle.Quorum(s.Net, s.VolumeVoters, "client:p0", MeshCause.MetadataQuorum);
            bool wrote = false;
            if (quorum.Outcome == MeshTri.Must)
            {
                // Settle the stale write before the new authority's write: wait for the call, then fence its id through
                // the new authority (resolve: true), so it can never take effect after `during`.
                try
                {
                    await staleWrite;
                }
                catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
                {
                    string? why = MeshOracle.CheckFailure(ex, new HashSet<MeshCause> { MeshCause.MetadataQuorum, MeshCause.Unreachable }, mutation: true, s.Policy);
                    Assert.True(why is null, "write routed to the paused node: " + why);
                }

                DfsOperationOutcome fenced = await MeshHelpers.MustAsync(s, () => staleVolume.QueryOperationAsync(staleId, resolve: true, Ct),
                    "the new authority must resolve the stale write's id");
                Assert.True(fenced.Status is DfsOutcomeStatus.Committed or DfsOutcomeStatus.Aborted,
                    $"resolve: true for {staleId} must be definite, got {fenced.Status} (OUT-013)");
                await MeshHelpers.MustAsync(s, () => s.Client("p0").GetVolume(Vol).Files.WriteAllBytesAsync(key, during, Ct),
                    "the new authority must accept writes while the old one is paused");
                wrote = true;
            }

            await cluster.DelayAsync(s.ElectionWindow * 2 + s.Timings.LeaseDuration * 2, Ct);
            await s.AssertSafetyAsync("paused");
            ulong termDuring = (await cluster.Inspect.GetVolumeGroupAsync(Vol)).Term;
            await s.ResumeAsync(target);
            await cluster.DelayAsync(s.ElectionWindow * 3, Ct);
            await cluster.Inspect.AssertNoSplitBrainAsync();
            if (!wrote)
            {
                // No other authority existed (for example a single paused voter): the queued write runs on resume and may
                // legitimately commit; it only has to end typed.
                try
                {
                    await staleWrite;
                }
                catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
                {
                    string? why = MeshOracle.CheckFailure(ex, new HashSet<MeshCause> { MeshCause.MetadataQuorum, MeshCause.Unreachable }, mutation: true, s.Policy);
                    Assert.True(why is null, "write queued at the paused node: " + why);
                }
            }
            if (target == volumeLeader && quorum.Outcome == MeshTri.Must)
            {
                // The remaining voters had a connected majority, so they elected a successor in a higher term while the
                // old leader was frozen; on its return it must follow that term, never resume its own.
                GroupInspection resumed = (await cluster.Node(target).Inspect.GetGroupAsync(Vol))!;
                Assert.True(resumed.Term >= termDuring && termDuring > term,
                    $"resumed former leader {target} must adopt the newer term (its term {resumed.Term}, cluster term {termDuring}, old {term})");
            }

            await s.RecoverAllAsync();
            if (wrote)
            {
                byte[] after = await s.Client("p0").GetVolume(Vol).Files.ReadAllBytesAsync(key, Ct);
                MeshHelpers.BytesEqual(during, after, "value committed by the new authority after the paused node's stale work resumed");
            }

            await s.FinishAsync();
        }, Ct);
    }

    // ============================================================================================================
    // MESH-013 — rolling restart of every node
    // ============================================================================================================

    public static TheoryData<int, string> RollingRestartMatrix() => MeshHelpers.Matrix("MESH-013",
        () => MeshHelpers.ByNodes(MeshHelpers.NodeCounts, "Graceful", "Crash"), MeshConfig.AllDimensions);

    /// <summary>
    /// MESH-013: stop (gracefully or by crash) and restart every node one at a time under load. While each node is down
    /// the oracle's expectation holds for every probe; after each restart the node's incarnation advanced by exactly one,
    /// the cluster is stable, every probe succeeds and every earlier acknowledged write is still intact.
    /// </summary>
    [Theory]
    [MemberData(nameof(RollingRestartMatrix))]
    [Catalog("MESH-013", "S", "INV-01", "INV-03", "INV-08", "INV-17", "INV-18")]
    public async Task MESH_013_RollingRestartOfEveryNode(int nodes, string config)
    {
        MeshConfig cfg = MeshHelpers.Config(nodes, config);
        await ClusterTest.RunAsync(MeshHelpers.Sim(cfg), async cluster =>
        {
            MeshScenarioOptions options = MeshHelpers.Options() with
            {
                ProbeKinds = [MeshProbeKind.WriteSmall, MeshProbeKind.WriteLarge, MeshProbeKind.ReadLarge, MeshProbeKind.ControlChange],
            };
            MeshScenario s = await MeshScenario.StartAsync(cluster, cfg, options, Ct);
            foreach (string node in MeshHelpers.Names(s))
            {
                if (cfg.Variant == "Graceful")
                    await s.StopNodeAsync(node);
                else
                    await s.CrashAsync(node);
                await s.ProbeAsync("down-" + node);
                await s.AssertSafetyAsync("down-" + node);
                await s.RestartAsync(node);
                await s.WaitSettledAsync();
                if (s.Net.Mesh == LinkMesh.Full)
                    await s.ProbeAsync("up-" + node);
            }

            await s.FinishAsync();
        }, Ct);
    }

    // ============================================================================================================
    // MESH-014 — link flapping
    // ============================================================================================================

    public static TheoryData<int, string> FlappingMatrix() => MeshHelpers.Matrix("MESH-014",
        () => MeshHelpers.ByNodes(MeshHelpers.NodeCounts, "NodeLinks", "LeaderLinks", "ClientLinks"), MeshHelpers.NoCrash);

    /// <summary>
    /// MESH-014: repeatedly cut and restore random node links, the leader's links or client links (ten seeded cycles of
    /// 100–800 ms) under load. During flapping there is never more than one leader per term; after the links are stable
    /// every client progresses within the bound, appends are never duplicated (append-log linearizability), and all
    /// replicas converge.
    /// </summary>
    [Theory]
    [MemberData(nameof(FlappingMatrix))]
    [Catalog("MESH-014", "S", "INV-02", "INV-06", "INV-17", "INV-18")]
    public async Task MESH_014_LinkFlappingConverges(int nodes, string config)
    {
        MeshConfig cfg = MeshHelpers.Config(nodes, config);
        await ClusterTest.RunAsync(MeshHelpers.Sim(cfg), async cluster =>
        {
            MeshScenario s = await MeshScenario.StartAsync(cluster, cfg, MeshHelpers.Options(), Ct);
            IRandomSource rng = new SeededRandom(cluster.Seed).Fork("flap");
            string[] names = MeshHelpers.Names(s);
            MeshWorkload workload = s.Workload!;
            string[] clients = workload.NetworkNames.Take(workload.Processes.Count).ToArray();
            // Only links the configured mesh actually has are flapped, so each restore returns to the formed mesh.
            string[] Neighbours(string node) => names.Where(n => n != node && s.Net.MeshAllows(node, n)).ToArray();
            for (int cycle = 0; cycle < 10; cycle++)
            {
                string a;
                string b;
                switch (cfg.Variant)
                {
                    case "LeaderLinks":
                        a = (await cluster.Inspect.GetVolumeGroupAsync(Vol)).Leader ?? names[0];
                        b = Neighbours(a) is { Length: > 0 } rest ? rest[rng.Next(rest.Length)] : clients[0];
                        break;
                    case "ClientLinks":
                        a = clients[rng.Next(clients.Length)];
                        b = names[rng.Next(names.Length)];
                        break;
                    default:
                        a = names[rng.Next(names.Length)];
                        b = Neighbours(a) is { Length: > 0 } adjacent ? adjacent[rng.Next(adjacent.Length)] : clients[0];
                        break;
                }

                s.CutBoth(a, b);
                await cluster.DelayAsync(TimeSpan.FromMilliseconds(100 + rng.Next(700)), Ct);
                s.Restore(a, b);
                s.Restore(b, a);
                if (cycle % 3 == 2)
                    await cluster.Inspect.AssertNoSplitBrainAsync();
                await cluster.DelayAsync(TimeSpan.FromMilliseconds(rng.Next(300)), Ct);
            }

            await s.AssertSafetyAsync("after-flapping");
            await s.FinishAsync();
        }, Ct);
    }

    // ============================================================================================================
    // MESH-015 — adding nodes under load
    // ============================================================================================================

    public static TheoryData<int, string> AddNodesMatrix() => MeshHelpers.Matrix("MESH-015",
        () => MeshHelpers.NodeCounts.Where(n => n < 10).SelectMany(n => (n == 9
                ? new[] { "One", "DuringControlFailover" }
                : new[] { "One", "Fill", "Concurrent", "DuringControlFailover" })
            .Select(v => MeshHelpers.Row(("n", MeshHelpers.N(n)), ("variant", v)))),
        MeshHelpers.FullMesh("crash"),
        c => c.Variant != "DuringControlFailover" || c.EffectiveControlVoters >= 3);

    /// <summary>
    /// MESH-015: add one node, as many as fit up to ten (one after another, or up to three joining concurrently), or one
    /// node while the control leader has just crashed, all while the workload runs. Each new node joins exactly once
    /// (concurrent joins are serialized into single membership changes), is listed Active and reachable, becomes a control
    /// member (voter or learner, never exceeding the configured voter count), catches up with the control log, serves as
    /// the sole entry point of a new client, and the workload's histories stay valid; availability never drops below the
    /// oracle's expectation while nodes join, and a join during a control failover completes through the new control
    /// leader.
    /// </summary>
    [Theory]
    [MemberData(nameof(AddNodesMatrix))]
    [Catalog("MESH-015", "S", "INV-11", "INV-17", "INV-18")]
    public async Task MESH_015_AddNodesDuringLoad(int nodes, string config)
    {
        MeshConfig cfg = MeshHelpers.Config(nodes, config);
        await ClusterTest.RunAsync(MeshHelpers.Sim(cfg), async cluster =>
        {
            MeshScenario s = await MeshScenario.StartAsync(cluster, cfg, MeshHelpers.Options(), Ct);
            int add = cfg.Variant switch
            {
                "Fill" => TestClusterOptions.MaxNodes - nodes,
                "Concurrent" => Math.Min(3, TestClusterOptions.MaxNodes - nodes),
                _ => 1,
            };
            int controlVotersBefore = s.ControlVoters.Count;
            TestNodeSpec Spec(int index) => new("n" + index.ToString(CultureInfo.InvariantCulture), cfg.Topology switch
            {
                TestTopology.OnePerZone => "z" + (index + 1).ToString(CultureInfo.InvariantCulture),
                TestTopology.TwoPlusOne => "z2",
                _ => "z1",
            });

            string? crashedControlLeader = null;
            if (cfg.Variant == "DuringControlFailover")
            {
                crashedControlLeader = await s.ControlLeaderAsync();
                await s.CrashAsync(crashedControlLeader);
                Assert.Equal(MeshTri.Must, s.Expect(MeshProbeKind.ControlChange, "p0").Outcome);
            }

            var added = new List<string>();
            if (cfg.Variant == "Concurrent")
            {
                TestNode[] joined = await Task.WhenAll(Enumerable.Range(nodes, add).Select(i => s.AddNodeAsync(Spec(i))));
                added.AddRange(joined.Select(j => j.Name));
                await s.ProbeAsync("added-concurrently", kinds: [MeshProbeKind.WriteSmall, MeshProbeKind.ReadLarge]);
            }
            else
            {
                for (int i = 0; i < add; i++)
                {
                    TestNode node = await s.AddNodeAsync(Spec(nodes + i));
                    added.Add(node.Name);
                    await s.ProbeAsync("added-" + node.Name, kinds: [MeshProbeKind.WriteSmall, MeshProbeKind.ReadLarge, MeshProbeKind.ControlChange]);
                }
            }

            if (crashedControlLeader is not null)
            {
                GroupInspection during = await cluster.Inspect.GetControlGroupAsync();
                Assert.True(during.Leader is not null && during.Leader != crashedControlLeader,
                    $"the join completed while {crashedControlLeader} was down, so a surviving control voter must lead (now {during.Leader})");
                Assert.Contains(added[0], during.Voters.Concat(during.Learners));
                await s.RestartAsync(crashedControlLeader);
            }

            await s.AssertControlConvergedAsync();
            IReadOnlyList<DfsNodeInfo> listed = await s.Client("p0").Admin.ListNodesAsync(Ct);
            Assert.Equal(nodes + add, listed.Count);
            foreach (string name in added)
            {
                DfsNodeInfo info = Assert.Single(listed, i => i.Name == name);
                Assert.Equal(DfsNodeStatus.Active, info.Status);
                Assert.True(info.IsReachable);
                Assert.True(info.CapacityBytes > 0, $"{name} must report storage capacity");
                DfsClient entry = await s.ConnectClientAsync("entry-" + name, [name]);
                byte[] data = TestData.Bytes(9000UL + (ulong)name.Length, 5000);
                await entry.GetVolume(Vol).Files.WriteAllBytesAsync("/mesh/added-" + name, data, Ct);
                MeshHelpers.BytesEqual(data, await s.Client("p0").GetVolume(Vol).Files.ReadAllBytesAsync("/mesh/added-" + name, Ct), "write entering through new node " + name);
            }

            GroupInspection control = await cluster.Inspect.GetControlGroupAsync();
            Assert.True(control.Voters.Count >= controlVotersBefore && control.Voters.Count <= Math.Max(cfg.ControlVoters, controlVotersBefore),
                $"control voters {control.Voters.Count} must stay within [{controlVotersBefore}, {cfg.ControlVoters}] after adding nodes");
            Assert.True(control.Voters.Concat(control.Learners).Distinct().Count() == nodes + add, "every node, old and new, is a control member exactly once");
            s.SetConfiguredVoters(controlVoters: control.Voters);
            await s.FinishAsync();
        }, Ct);
    }

    // ============================================================================================================
    // MESH-016 — draining and removing nodes under load
    // ============================================================================================================

    public static TheoryData<int, string> DrainMatrix() => MeshHelpers.Matrix("MESH-016",
        () => MeshHelpers.ByNodes(MeshHelpers.NodeCounts, "NonVoter", "VolumeVoter", "VolumeLeader"), MeshHelpers.FullMesh("crash"),
        c => c.Variant != "NonVoter" || c.Nodes > c.VolumeVoters);

    /// <summary>
    /// MESH-016: drain a non-voter, a volume voter or the volume leader under load. When the remaining hosts/zones can
    /// hold the full target and a replacement voter exists, the drain completes (Draining → Drained), the node holds no
    /// required copy and no voter seat, and removal marks it Removed; restarting it with its old disk does not regain any
    /// authority. When the remaining capacity cannot hold the policy, the drain does not complete and removal without
    /// force is rejected with a typed error while all data stays readable (OPS-001/002).
    /// </summary>
    [Theory]
    [MemberData(nameof(DrainMatrix))]
    [Catalog("MESH-016", "S", "INV-08", "INV-09", "INV-11", "INV-17", "INV-18")]
    public async Task MESH_016_DrainAndRemoveNodesDuringLoad(int nodes, string config)
    {
        MeshConfig cfg = MeshHelpers.Config(nodes, config);
        await ClusterTest.RunAsync(MeshHelpers.Sim(cfg), async cluster =>
        {
            MeshScenario s = await MeshScenario.StartAsync(cluster, cfg, MeshHelpers.Options(), Ct);
            string leader = await s.VolumeLeaderAsync();
            string target = cfg.Variant switch
            {
                "VolumeLeader" => leader,
                "VolumeVoter" => s.VolumeVoters.FirstOrDefault(v => v != leader) ?? leader,
                _ => MeshHelpers.Prefer(MeshHelpers.Names(s).Where(n => !s.VolumeVoters.Contains(n)), 1, s.ControlVoters)[0],
            };
            string[] remaining = MeshHelpers.Names(s).Where(n => n != target).ToArray();
            (int hosts, int zones) = MeshOracle.CountDomains(s.Net, remaining);
            bool feasible = hosts >= s.Policy.TargetCopies && zones >= s.Policy.TargetDistinctZones
                && (!s.VolumeVoters.Contains(target) || remaining.Length >= s.VolumeVoters.Count);
            DfsAdminClient admin = s.Client("p0").Admin;
            await s.DrainAsync(target);
            await s.ProbeAsync("draining");

            if (feasible)
            {
                await cluster.WaitUntilAsync(async () => (await admin.ListNodesAsync(Ct)).Single(i => i.Name == target).Status == DfsNodeStatus.Drained,
                    s.Options.RecoveryTimeout * 2, $"draining {target} must complete: {hosts} hosts/{zones} zones remain for target {s.Policy.TargetCopies}/{s.Policy.TargetDistinctZones}", Ct);
                GroupInspection volume = await s.VolumeGroupAsync();
                Assert.DoesNotContain(target, volume.Voters);
                Assert.NotEqual(target, volume.Leader);
                Assert.Equal(s.VolumeVoters.Count, volume.Voters.Count);

                // Every object referenced when the drain was reported complete has its full target without the drained
                // node. The workload keeps committing, so objects committed just before that moment may still be at the
                // acknowledged count: let repair finish first. An object dereferenced meanwhile can never become
                // reachable again, so it is skipped (holders are read before reachability).
                IReadOnlySet<ObjectId> referenced = (await cluster.Inspect.GetVolumeStateAsync(Vol)).ReachableObjects();
                await cluster.WaitForRepairAsync(Vol, s.Options.RecoveryTimeout, Ct);
                foreach (ObjectId obj in referenced.OrderBy(o => o.ToString(), StringComparer.Ordinal))
                {
                    IReadOnlyList<string> holders = await cluster.Inspect.GetObjectHoldersAsync(obj);
                    string[] elsewhere = holders.Where(h => h != target).ToArray();
                    (int copies, int objectZones) = MeshOracle.CountDomains(s.Net, elsewhere);
                    Assert.True(copies >= s.Policy.TargetCopies && objectZones >= s.Policy.TargetDistinctZones || !(await IsReferencedAsync(cluster, obj)),
                        $"referenced object {obj} must have its full target {s.Policy.TargetCopies}/{s.Policy.TargetDistinctZones} off drained {target}, " +
                        $"holders [{string.Join(",", holders)}] (OPS-001, RCV-06)");
                }

                s.SetConfiguredVoters(volumeVoters: volume.Voters, controlVoters: (await cluster.Inspect.GetControlGroupAsync()).Voters);
                await admin.RemoveNodeAsync(target, force: false, Ct);
                await cluster.WaitUntilAsync(async () =>
                {
                    DfsNodeInfo? info = (await admin.ListNodesAsync(Ct)).SingleOrDefault(i => i.Name == target);
                    return info is null || info.Status == DfsNodeStatus.Removed;
                }, s.Options.RecoveryTimeout, $"{target} must be marked removed", Ct);
                s.MarkRemoved(target);
                s.SetConfiguredVoters(controlVoters: (await cluster.Inspect.GetControlGroupAsync()).Voters); // removal may change the control seats
                await s.CrashAsync(target);

                // The removed node returns with its old disk: it must not vote, lead or rejoin membership (CON-016, RSY-020).
                await s.RestartAsync(target);
                await cluster.DelayAsync(s.ElectionWindow * 4, Ct);
                GroupInspection control = await cluster.Inspect.GetControlGroupAsync();
                Assert.DoesNotContain(target, control.Voters);
                Assert.DoesNotContain(target, (await s.VolumeGroupAsync()).Voters);
                DfsNodeInfo? returned = (await admin.ListNodesAsync(Ct)).SingleOrDefault(i => i.Name == target);
                Assert.True(returned is null || returned.Status == DfsNodeStatus.Removed, $"removed {target} regained membership status {returned?.Status}");
                await cluster.Inspect.AssertNoSplitBrainAsync();
                await s.CrashAsync(target);
            }
            else
            {
                await cluster.DelayAsync(s.Options.RecoveryTimeout / 3, Ct);
                DfsNodeInfo info = (await admin.ListNodesAsync(Ct)).Single(i => i.Name == target);
                Assert.True(info.Status == DfsNodeStatus.Draining,
                    $"drain of {target} cannot complete with {hosts} hosts/{zones} zones for target {s.Policy.TargetCopies}/{s.Policy.TargetDistinctZones}, but status is {info.Status}");
                // OPS-002 names no error code for the rejection; it must be a typed refusal, not an internal error, a
                // timeout or an unknown outcome (the removal must definitely not have happened).
                DfsException rejected = await AssertEx.ThrowsDfsAsync<DfsException>(() => admin.RemoveNodeAsync(target, force: false, Ct));
                Assert.True(rejected.ErrorCode is not (DfsErrorCode.None or DfsErrorCode.Internal or DfsErrorCode.OutcomeUnknown or DfsErrorCode.Timeout
                    or DfsErrorCode.ProtocolError or DfsErrorCode.Cancelled), $"unsafe removal must be rejected with a typed reason, got {MeshOracle.Describe(rejected)}");
                Assert.Contains(target, (await admin.ListNodesAsync(Ct)).Select(i => i.Name));
                await s.ProbeAsync("drain-blocked", kinds: [MeshProbeKind.ReadSmall, MeshProbeKind.ReadLarge]);
                await s.FinishAsync(requireFullRepair: false);
                return;
            }

            await s.FinishAsync();
        }, Ct);
    }

    private static async Task<bool> IsReferencedAsync(TestCluster cluster, ObjectId obj) =>
        (await cluster.Inspect.GetVolumeStateAsync(Vol)).ReachableObjects().Contains(obj);

    // ============================================================================================================
    // MESH-017 — whole-zone outage per topology
    // ============================================================================================================

    public static TheoryData<int, string> ZoneOutageMatrix() => MeshHelpers.Matrix("MESH-017",
        () => MeshHelpers.NodeCounts.SelectMany(n => new[]
        {
            ("OnePerZone", "LeaderZone"), ("OnePerZone", "OtherZone"), ("TwoPlusOne", "z1"), ("TwoPlusOne", "z2"), ("SingleZone", "z1"),
        }.Where(t => !(n == 2 && t.Item1 == "TwoPlusOne" && t.Item2 == "z2"))
            .Select(t => MeshHelpers.Row(("n", MeshHelpers.N(n)), ("topo", t.Item1), ("variant", t.Item2)))),
        MeshConfig.DimensionsExcept("topo"));

    /// <summary>
    /// MESH-017: stop every node of one zone (the leader's zone, another zone, either zone of a 2+1 layout, the single
    /// zone). Progress depends on the surviving majority of configured voters and the acknowledgement zones/copies of the
    /// survivors, not on the number of live servers (OFF-006, REP-010/011); losing the zone holding two of three voters,
    /// or the only zone, makes metadata unavailable (typed) while the other zone of 2+1 continues.
    /// </summary>
    [Theory]
    [MemberData(nameof(ZoneOutageMatrix))]
    [Catalog("MESH-017", "S", "INV-01", "INV-08", "INV-11", "INV-17")]
    public async Task MESH_017_WholeZoneOutage(int nodes, string config)
    {
        MeshConfig cfg = MeshHelpers.Config(nodes, config);
        await ClusterTest.RunAsync(MeshHelpers.Sim(cfg), async cluster =>
        {
            MeshScenario s = await MeshScenario.StartAsync(cluster, cfg, MeshHelpers.Options(), Ct);
            string leader = await s.VolumeLeaderAsync();
            string leaderZone = s.Net.Spec(leader).Zone;
            string zone = cfg.Variant switch
            {
                "LeaderZone" => leaderZone,
                "OtherZone" => s.Net.Nodes.Select(n => n.Zone).First(z => z != leaderZone),
                _ => cfg.Variant,
            };
            string[] victims = s.Net.Nodes.Where(n => n.Zone == zone).Select(n => n.Name).ToArray();
            Assert.NotEmpty(victims);
            foreach (string victim in victims)
                await s.CrashAsync(victim);

            int aliveVoters = s.VolumeVoters.Count(v => !victims.Contains(v));
            MeshExpectation quorum = MeshOracle.Quorum(s.Net, s.VolumeVoters, "client:p0", MeshCause.MetadataQuorum);
            if (s.Net.Mesh == LinkMesh.Full)
                Assert.Equal(aliveVoters >= MeshOracle.Majority(s.VolumeVoters.Count) ? MeshTri.Must : MeshTri.Never, quorum.Outcome);
            if (quorum.Outcome == MeshTri.Must && victims.Contains(leader))
                await s.WaitForVolumeLeaderAmongAsync(s.VolumeVoters.Except(victims).ToArray(), $"zone {zone} outage: a voter in another zone must lead");
            await s.ProbeAsync("zone-" + zone + "-down");
            await s.AssertSafetyAsync("zone-" + zone + "-down");
            foreach (string victim in victims)
                await s.RestartAsync(victim);
            await s.FinishAsync();
        }, Ct);
    }

    // ============================================================================================================
    // MESH-018 — total outage and cold restart in different orders
    // ============================================================================================================

    public static TheoryData<int, string> TotalOutageMatrix() => MeshHelpers.Matrix("MESH-018",
        () => MeshHelpers.ByNodes(MeshHelpers.NodeCounts, "Ascending", "Descending", "VotersLast", "VotersFirst"), MeshConfig.AllDimensions);

    /// <summary>
    /// MESH-018: crash every node, then cold-restart them one by one in ascending, descending, voters-last or voters-first
    /// order. With everything down every operation fails typed; after each restart availability is the oracle's (a
    /// majority of configured voters plus the data policy over restarted hosts) — a node restarted before the control group
    /// has a quorum again cannot re-register its incarnation yet (RCV-02), so until then it only bounds the expectation
    /// (<see cref="MeshScenario.PendingRegistration"/>: required success only without it, required failure only with it);
    /// no bootstrap deadlock occurs; every acknowledged write survives from the durable images only (CON-020, OFF-018).
    /// </summary>
    [Theory]
    [MemberData(nameof(TotalOutageMatrix))]
    [Catalog("MESH-018", "S", "INV-01", "INV-06", "INV-08", "INV-17", "INV-18")]
    public async Task MESH_018_TotalOutageColdRestartInEveryOrder(int nodes, string config)
    {
        MeshConfig cfg = MeshHelpers.Config(nodes, config);
        await ClusterTest.RunAsync(MeshHelpers.Sim(cfg), async cluster =>
        {
            MeshScenarioOptions options = MeshHelpers.Options() with
            {
                ProbeKinds = [MeshProbeKind.WriteSmall, MeshProbeKind.ReadSmall, MeshProbeKind.WriteLarge, MeshProbeKind.ReadLarge, MeshProbeKind.ControlChange],
            };
            MeshScenario s = await MeshScenario.StartAsync(cluster, cfg, options, Ct);
            await cluster.DelayAsync(TimeSpan.FromSeconds(1), Ct);
            string[] all = MeshHelpers.Names(s);
            foreach (string node in all)
                await s.CrashAsync(node);
            await s.ProbeAsync("all-down");

            string[] voters = s.VolumeVoters.Union(s.ControlVoters).ToArray();
            string[] order = cfg.Variant switch
            {
                "Descending" => all.Reverse().ToArray(),
                "VotersLast" => all.Where(n => !voters.Contains(n)).Concat(all.Where(voters.Contains)).ToArray(),
                "VotersFirst" => all.Where(voters.Contains).Concat(all.Where(n => !voters.Contains(n))).ToArray(),
                _ => all,
            };
            foreach (string node in order)
            {
                await s.RestartAsync(node);
                await s.ProbeAsync("restarted-" + node);
            }

            if (s.Net.IsFullyConnected)
                Assert.Empty(s.PendingRegistration); // every node is back, so the control group can register every incarnation
            await s.FinishAsync();
        }, Ct);
    }

    // ============================================================================================================
    // MESH-019 — every offline subset for 2..5 nodes
    // ============================================================================================================

    public static TheoryData<int, int> OfflineSubsets()
    {
        var data = new TheoryData<int, int>();
        foreach (int n in new[] { 2, 3, 4, 5 })
        {
            for (int mask = 0; mask < 1 << n; mask++)
                data.Add(n, mask);
        }

        return data;
    }

    private static MeshConfig CompactConfig(int n) => new()
    {
        Nodes = n,
        Boot = MeshBoot.Static,
        Mesh = LinkMesh.Full,
        Topology = TestTopology.OnePerZone,
        ControlVoters = 3,
        VolumeVoters = n == 2 ? 1 : 3,
        Ack = n == 2 ? 1 : 2,
    };

    /// <summary>
    /// MESH-019: for compact meshes of 2, 3, 4 and 5 nodes enumerate every subset of offline nodes, including none and
    /// all (test plan §5). With the subset crashed at once, inline and extent writes, reads of files written before,
    /// appends and control changes match the oracle exactly (Must or Never, never fabricated success); after restarting
    /// the subset everything succeeds and converges.
    /// </summary>
    [Theory]
    [MemberData(nameof(OfflineSubsets))]
    [Catalog("MESH-019", "S", "INV-01", "INV-08", "INV-11", "INV-17")]
    public async Task MESH_019_EveryOfflineSubsetForCompactMeshes(int nodes, int offlineMask)
    {
        MeshConfig cfg = CompactConfig(nodes);
        await ClusterTest.RunAsync(MeshHelpers.Sim(cfg), async cluster =>
        {
            MeshScenario s = await MeshScenario.StartAsync(cluster, cfg, MeshHelpers.Options(workload: false, namespaceScript: false), Ct);
            string[] victims = Enumerable.Range(0, nodes).Where(i => (offlineMask & (1 << i)) != 0).Select(i => "n" + i.ToString(CultureInfo.InvariantCulture)).ToArray();
            foreach (string victim in victims)
                await s.CrashAsync(victim);
            foreach (MeshProbeKind kind in s.Options.ProbeKinds)
                Assert.NotEqual(MeshTri.Either, s.Expect(kind, "p0").Outcome);
            await s.ProbeAsync("offline-" + offlineMask.ToString(CultureInfo.InvariantCulture));
            if (victims.Length < nodes)
                await s.AssertSafetyAsync("offline");
            foreach (string victim in victims)
                await s.RestartAsync(victim);
            await s.FinishAsync();
        }, Ct);
    }

    // ============================================================================================================
    // MESH-020 — every partition shape of 3 and 5 nodes, with the client in every group
    // ============================================================================================================

    public static TheoryData<int, string, int> PartitionShapes()
    {
        var data = new TheoryData<int, string, int>();
        foreach (int n in new[] { 3, 5 })
        {
            foreach (int[][] groups in MeshHelpers.SetPartitions(n))
            {
                for (int clientGroup = 0; clientGroup < groups.Length; clientGroup++)
                    data.Add(n, MeshHelpers.EncodePartition(groups), clientGroup);
            }
        }

        return data;
    }

    /// <summary>
    /// MESH-020: every set partition of 3 and 5 nodes (3, 2+1, 1+1+1, … 1+1+1+1+1) with the probe client placed in each
    /// group in turn (partitions that separate clients differently from servers). Exactly the group holding a majority of
    /// the configured voters serves, and only to a client inside it; every other placement fails typed. No split brain;
    /// healing converges.
    /// </summary>
    [Theory]
    [MemberData(nameof(PartitionShapes))]
    [Catalog("MESH-020", "S", "INV-03", "INV-05", "INV-08", "INV-18")]
    public async Task MESH_020_EveryPartitionShapeForThreeAndFiveNodes(int nodes, string partition, int clientGroup)
    {
        MeshConfig cfg = CompactConfig(nodes) with { ControlVoters = 3, VolumeVoters = 3, Ack = 2 };
        await ClusterTest.RunAsync(MeshHelpers.Sim(cfg), async cluster =>
        {
            MeshScenario s = await MeshScenario.StartAsync(cluster, cfg, MeshHelpers.Options(workload: false, namespaceScript: false), Ct);
            string[][] groups = MeshHelpers.DecodePartition(partition);
            s.PartitionNodes(groups, client => client == "client:p0" ? clientGroup : 0);
            foreach (MeshProbeKind kind in s.Options.ProbeKinds)
                Assert.NotEqual(MeshTri.Either, s.Expect(kind, "p0").Outcome);
            await s.ProbeAsync("partition-" + partition.Replace('|', '_') + "-c" + clientGroup.ToString(CultureInfo.InvariantCulture));
            await s.AssertSafetyAsync("partition");
            await s.FinishAsync();
        }, Ct);
    }

    // ============================================================================================================
    // MESH-021 — even and single voter groups
    // ============================================================================================================

    public static TheoryData<int, string> EvenVoterMatrix()
    {
        var data = new TheoryData<int, string>();
        foreach (int n in MeshHelpers.NodeCounts)
        {
            foreach (int v in new[] { 1, 2, 4 }.Where(v => v <= n))
            {
                foreach (string side in new[] { "AtTolerance", "BeyondTolerance" })
                    data.Add(n, new MeshConfig { Nodes = n, VolumeVoters = v, ControlVoters = 3, Ack = 1, Variant = "volume-" + side }.Format());
            }

            foreach (int cv in new[] { 2, 4 }.Where(cv => cv <= n))
            {
                foreach (string side in new[] { "AtTolerance", "BeyondTolerance" })
                    data.Add(n, new MeshConfig { Nodes = n, VolumeVoters = 1, ControlVoters = cv, Ack = 1, Variant = "control-" + side }.Format());
            }
        }

        return data;
    }

    /// <summary>
    /// MESH-021: groups of 1, 2 and 4 configured voters (volume) and 2 and 4 (control) tolerate exactly (V-1)/2 failures:
    /// an even group gains no tolerance over the odd group below it. Crashing the tolerated number keeps the group
    /// available; one more makes it unavailable with a typed error, even though half of the voters are alive.
    /// </summary>
    [Theory]
    [MemberData(nameof(EvenVoterMatrix))]
    [Catalog("MESH-021", "S", "INV-08", "INV-11", "INV-17")]
    public async Task MESH_021_EvenAndSingleVoterGroupsTolerance(int nodes, string config)
    {
        MeshConfig cfg = MeshHelpers.Config(nodes, config);
        bool control = cfg.Variant.StartsWith("control", StringComparison.Ordinal);
        if (control)
            cfg = cfg with { VolumeVoterNodes = ["n" + (nodes - 1).ToString(CultureInfo.InvariantCulture)] };
        await ClusterTest.RunAsync(MeshHelpers.Sim(cfg), async cluster =>
        {
            MeshScenario s = await MeshScenario.StartAsync(cluster, cfg, MeshHelpers.Options(), Ct);
            IReadOnlyList<string> group = control ? s.ControlVoters : s.VolumeVoters;
            Assert.Equal(control ? cfg.EffectiveControlVoters : cfg.VolumeVoters, group.Count);
            int tolerance = MeshOracle.Tolerance(group.Count);
            Assert.Equal((group.Count - 1) / 2, tolerance);
            int crash = cfg.Variant.EndsWith("AtTolerance", StringComparison.Ordinal) ? tolerance : tolerance + 1;
            string[] avoid = control ? s.VolumeVoters.ToArray() : [];
            string[] victims = MeshHelpers.Prefer(group, crash, avoid);
            foreach (string victim in victims)
                await s.CrashAsync(victim);

            MeshExpectation e = control
                ? MeshOracle.Quorum(s.Net, s.ControlVoters, "client:p0", MeshCause.ControlQuorum)
                : MeshOracle.Quorum(s.Net, s.VolumeVoters, "client:p0", MeshCause.MetadataQuorum);
            Assert.Equal(crash <= tolerance ? MeshTri.Must : MeshTri.Never, e.Outcome);
            await s.ProbeAsync("voters-" + cfg.Variant);
            if (victims.Length > 0)
                await s.AssertSafetyAsync("voters-" + cfg.Variant);
            foreach (string victim in victims)
                await s.RestartAsync(victim);
            await s.FinishAsync();
        }, Ct);
    }

    // ============================================================================================================
    // MESH-022 — control quorum loss keeps established volumes serving
    // ============================================================================================================

    public static TheoryData<int, string> ControlLossMatrix() => MeshHelpers.Matrix("MESH-022",
        () => MeshHelpers.ByNodes(MeshHelpers.NodeCounts),
        [MeshConfig.Restrict("boot", "Static"), .. MeshHelpers.FullMesh("boot", "crash")],
        c =>
        {
            int cv = c.EffectiveControlVoters;
            int overlap = Math.Max(0, cv + c.VolumeVoters - c.Nodes);
            int toLose = cv - MeshOracle.Majority(cv) + 1;
            return toLose <= cv - overlap + MeshOracle.Tolerance(c.VolumeVoters);
        });

    /// <summary>
    /// MESH-022: with control voters n0.. and volume voters at the other end of the node list, crash enough control voters
    /// to lose the control quorum while the volume group keeps a majority. Established volume reads/writes keep working
    /// (CON-017: no cluster-wide majority is invented) while control changes fail typed; after restart control changes
    /// work again and memberships converge.
    /// </summary>
    [Theory]
    [MemberData(nameof(ControlLossMatrix))]
    [Catalog("MESH-022", "S", "INV-08", "INV-11", "INV-17")]
    public async Task MESH_022_ControlQuorumLossKeepsEstablishedVolumesServing(int nodes, string config)
    {
        MeshConfig cfg = MeshHelpers.Config(nodes, config);
        cfg = cfg with
        {
            ControlVoterNodes = Enumerable.Range(0, cfg.EffectiveControlVoters).Select(i => "n" + i.ToString(CultureInfo.InvariantCulture)).ToArray(),
            VolumeVoterNodes = Enumerable.Range(nodes - cfg.VolumeVoters, cfg.VolumeVoters).Select(i => "n" + i.ToString(CultureInfo.InvariantCulture)).ToArray(),
        };
        await ClusterTest.RunAsync(MeshHelpers.Sim(cfg), async cluster =>
        {
            MeshScenario s = await MeshScenario.StartAsync(cluster, cfg, MeshHelpers.Options(), Ct);
            int toLose = s.ControlVoters.Count - MeshOracle.Majority(s.ControlVoters.Count) + 1;
            string[] exclusive = s.ControlVoters.Where(c => !s.VolumeVoters.Contains(c)).ToArray();
            string[] shared = s.ControlVoters.Where(s.VolumeVoters.Contains).ToArray();
            string[] victims = exclusive.Concat(shared).Take(toLose).ToArray();
            Assert.True(victims.Count(s.VolumeVoters.Contains) <= MeshOracle.Tolerance(s.VolumeVoters.Count), "setup keeps the volume majority");
            foreach (string victim in victims)
                await s.CrashAsync(victim);

            Assert.Equal(MeshTri.Never, s.Expect(MeshProbeKind.ControlChange, "p0").Outcome);
            Assert.Equal(MeshTri.Must, MeshOracle.Quorum(s.Net, s.VolumeVoters, "client:p0", MeshCause.MetadataQuorum).Outcome);
            await s.ProbeAsync("control-lost");
            await s.AssertSafetyAsync("control-lost");
            foreach (string victim in victims)
                await s.RestartAsync(victim);
            await s.ProbeAsync("control-back", kinds: [MeshProbeKind.ControlChange]);
            await s.FinishAsync();
        }, Ct);
    }

    // ============================================================================================================
    // MESH-023 — several volumes: leadership spread and independent failure
    // ============================================================================================================

    public static TheoryData<int, string> MultiVolumeMatrix() => MeshHelpers.Matrix("MESH-023",
        () => MeshHelpers.NodeCounts.SelectMany(n => new[] { "Shared2", "Shared3", "Disjoint2", "Disjoint3" }
            .Where(v => v.StartsWith("Shared", StringComparison.Ordinal) || n >= v[^1] - '0')
            .Select(v => MeshHelpers.Row(("n", MeshHelpers.N(n)), ("variant", v), ("vv", MeshHelpers.N(VotersPerVolume(n, v)))))),
        MeshHelpers.FullMesh("vv"));

    private static int VotersPerVolume(int nodes, string variant) =>
        variant.StartsWith("Shared", StringComparison.Ordinal) ? Math.Min(3, nodes) : nodes >= 3 * (variant[^1] - '0') ? 3 : 1;

    /// <summary>
    /// MESH-023: two or three volumes either share the same voters or use disjoint voter blocks. Volumes sharing voters
    /// get their leadership spread over more than one node (FSD §3.2); losing the quorum of one volume's own voters makes
    /// only that volume unavailable (typed) while the other volumes keep serving (CON-018). Every volume converges.
    /// </summary>
    [Theory]
    [MemberData(nameof(MultiVolumeMatrix))]
    [Catalog("MESH-023", "S", "INV-08", "INV-11", "INV-17")]
    public async Task MESH_023_VolumesSpreadLeadershipAndFailIndependently(int nodes, string config)
    {
        MeshConfig cfg = MeshHelpers.Config(nodes, config);
        bool shared = cfg.Variant.StartsWith("Shared", StringComparison.Ordinal);
        int count = cfg.Variant[^1] - '0';
        int perVolume = VotersPerVolume(nodes, cfg.Variant);
        string[] Block(int v) => Enumerable.Range(0, perVolume).Select(i => "n" + ((v * perVolume + i) % nodes).ToString(CultureInfo.InvariantCulture)).ToArray();
        // Shared: every volume has exactly the same explicit voters (so spreading leadership is the implementation's job,
        // FSD §3.2, not an accident of placement); Disjoint: consecutive non-overlapping blocks.
        cfg = cfg with { VolumeVoters = perVolume, VolumeVoterNodes = Block(0) };
        TestClusterOptions options = MeshHelpers.Sim(cfg);
        string[] extra = Enumerable.Range(1, count - 1).Select(i => "vol" + (i + 1).ToString(CultureInfo.InvariantCulture)).ToArray();
        options = options with
        {
            Volumes =
            [
                .. options.Volumes,
                .. extra.Select((name, i) => new TestVolumeSpec(name)
                {
                    Metadata = new DfsMetadataPolicy { Voters = perVolume, Placement = DfsVoterPlacement.Explicit, ExplicitVoterNodes = shared ? Block(0) : Block(i + 1) },
                    DefaultPolicy = cfg.Policy(),
                }),
            ],
        };
        await ClusterTest.RunAsync(options, async cluster =>
        {
            MeshScenario s = await MeshScenario.StartAsync(cluster, cfg, MeshHelpers.Options(), Ct);
            var voters = new Dictionary<string, string[]>(StringComparer.Ordinal) { [Vol] = s.VolumeVoters.ToArray() };
            foreach (string name in extra)
            {
                voters[name] = (await cluster.Inspect.GetVolumeGroupAsync(name)).Voters.Order(StringComparer.Ordinal).ToArray();
                string[] declared = (shared ? Block(0) : Block(Array.IndexOf(extra, name) + 1)).Order(StringComparer.Ordinal).ToArray();
                Assert.True(voters[name].SequenceEqual(declared), $"{name} must have exactly its explicit voters [{string.Join(",", declared)}], has [{string.Join(",", voters[name])}] (INV-11)");
            }

            if (shared && perVolume >= 2)
            {
                await cluster.WaitUntilAsync(async () =>
                {
                    var leaders = new HashSet<string?>();
                    foreach (string name in voters.Keys)
                        leaders.Add((await cluster.Inspect.GetVolumeGroupAsync(name)).Leader);
                    return leaders.Count(l => l is not null) >= 2;
                }, s.Options.RecoveryTimeout, $"leadership of {voters.Count} volumes sharing {perVolume} voters must be spread over more than one node (FSD §3.2)", Ct);
            }

            if (shared)
            {
                // Same voters: losing one tolerated voter (the leader of "vol") re-elects every volume; all keep serving.
                string leader = await s.VolumeLeaderAsync();
                if (MeshOracle.Tolerance(perVolume) >= 1)
                {
                    await s.CrashAsync(leader);
                    foreach (string name in voters.Keys)
                    {
                        await cluster.WaitUntilAsync(async () => (await cluster.Inspect.GetVolumeGroupAsync(name)).Leader is { } l && l != leader,
                            s.Options.MustWithin, $"{name} must re-elect after its shared voter {leader} crashed", Ct);
                    }

                    await s.ProbeAsync("shared-voter-lost");
                    await s.RestartAsync(leader);
                }
            }
            else
            {
                // Disjoint blocks: lose the last volume's own voters; "vol" (other voters) keeps serving (CON-018).
                string lost = extra[^1];
                string[] victims = voters[lost];
                Assert.Empty(victims.Intersect(voters[Vol]));
                foreach (string victim in victims)
                    await s.CrashAsync(victim);
                DfsVolume lostVolume = s.Client("p0").GetVolume(lost);
                MeshExpectation lostExpectation = MeshOracle.Quorum(s.Net, victims, "client:p0", MeshCause.MetadataQuorum);
                Assert.Equal(MeshTri.Never, lostExpectation.Outcome);
                for (int attempt = 0; attempt < 2; attempt++)
                {
                    string path = "/independent-" + attempt.ToString(CultureInfo.InvariantCulture);
                    Exception ex = await Assert.ThrowsAnyAsync<Exception>(() => lostVolume.Files.WriteAllBytesAsync(path, new byte[64], Ct));
                    string? why = MeshOracle.CheckFailure(ex, lostExpectation.Causes, mutation: true, s.Policy);
                    Assert.True(why is null, $"{lost} lost every configured voter [{string.Join(",", victims)}] ({lostExpectation}): {why}");
                    // A live read needs a ReadIndex round with a majority: without any live voter neither the bytes nor
                    // the absence of a never-written name may be reported (INV-03, INV-08).
                    bool answered = false;
                    try
                    {
                        await lostVolume.Files.ReadAllBytesAsync("/never-written", Ct);
                        answered = true;
                    }
                    catch (Exception readFailure) when (readFailure is not Xunit.Sdk.XunitException)
                    {
                        Assert.False(MeshOracle.Unwrap(readFailure) is FileNotFoundException,
                            $"{lost} reported a false absence without any live voter (INV-08): {MeshOracle.Describe(readFailure)}");
                        string? readWhy = MeshOracle.CheckFailure(readFailure, lostExpectation.Causes, mutation: false, s.Policy);
                        Assert.True(readWhy is null, $"live read on {lost} without a quorum: {readWhy}");
                    }

                    Assert.False(answered, $"{lost} answered a live read without any live voter (INV-03)");
                    await cluster.DelayAsync(s.ElectionWindow * 2, Ct);
                }

                Assert.Equal(MeshTri.Must, MeshOracle.Quorum(s.Net, s.VolumeVoters, "client:p0", MeshCause.MetadataQuorum).Outcome);
                await s.ProbeAsync("other-volume-lost");
                foreach (string victim in victims)
                    await s.RestartAsync(victim);
            }

            await s.FinishAsync();
            foreach (string name in extra)
            {
                byte[] data = TestData.Bytes(77, 3000);
                await s.Client("p0").GetVolume(name).Files.WriteAllBytesAsync("/after", data, Ct);
                MeshHelpers.BytesEqual(data, await s.Client("p0").GetVolume(name).Files.ReadAllBytesAsync("/after", Ct), name + " after recovery");
                await cluster.Inspect.AssertReplicasConvergedAsync(name);
            }
        }, Ct);
    }

    // ============================================================================================================
    // MESH-024 — every node as a client entry point, surviving loss of the entry node
    // ============================================================================================================

    public static TheoryData<int, string> EntryMatrix() => MeshHelpers.Matrix("MESH-024",
        () => MeshHelpers.ByNodes(MeshHelpers.NodeCounts), MeshHelpers.FullMesh());

    /// <summary>
    /// MESH-024: one client per node, each knowing only that node as its seed, writes and reads data written through
    /// another entry. When an entry node crashes, its client keeps operating through the cluster map it discovered
    /// (exactly as the oracle permits for the remaining nodes), and returns to full service after restart.
    /// </summary>
    [Theory]
    [MemberData(nameof(EntryMatrix))]
    [Catalog("MESH-024", "S", "INV-03", "INV-08", "INV-17")]
    public async Task MESH_024_ClientsThroughEveryEntryNodeSurviveEntryLoss(int nodes, string config)
    {
        MeshConfig cfg = MeshHelpers.Config(nodes, config);
        await ClusterTest.RunAsync(MeshHelpers.Sim(cfg), async cluster =>
        {
            string[] clients = Enumerable.Range(0, nodes).Select(i => "e" + i.ToString(CultureInfo.InvariantCulture)).ToArray();
            MeshScenarioOptions options = MeshHelpers.Options() with
            {
                ProbeClients = ["p0", .. clients],
                ProbeKinds = [MeshProbeKind.WriteSmall, MeshProbeKind.ReadLarge],
            };
            MeshScenario s = await MeshScenario.StartAsync(cluster, cfg, options with { ProbeClients = ["p0"] }, Ct);
            for (int i = 0; i < nodes; i++)
                await s.ConnectClientAsync(clients[i], [cluster[i].Name]);
            for (int i = 0; i < nodes; i++)
            {
                byte[] data = TestData.Bytes(500UL + (ulong)i, 5000);
                await s.Client(clients[i]).GetVolume(Vol).Files.WriteAllBytesAsync("/mesh/entry-" + clients[i], data, Ct);
                MeshHelpers.BytesEqual(data, await s.Client(clients[(i + 1) % nodes]).GetVolume(Vol).Files.ReadAllBytesAsync("/mesh/entry-" + clients[i], Ct),
                    $"entry {cluster[i].Name} -> entry {cluster[(i + 1) % nodes].Name}");
            }

            string entryNode = cluster[0].Name;
            await s.CrashAsync(entryNode);
            await s.ProbeAsync("entry-lost", clients, [MeshProbeKind.WriteSmall, MeshProbeKind.ReadLarge]);
            await s.RestartAsync(entryNode);
            await s.FinishAsync();
            await s.ProbeAsync("entry-back", clients, [MeshProbeKind.WriteSmall, MeshProbeKind.ReadLarge]);
        }, Ct);
    }

    // ============================================================================================================
    // MESH-025 — lagging node catches up through a checkpoint
    // ============================================================================================================

    public static TheoryData<int, string> LaggingMatrix() => MeshHelpers.Matrix("MESH-025",
        () => MeshHelpers.ByNodes(MeshHelpers.NodeCounts, "VolumeVoter", "ControlLearner"), MeshHelpers.FullMesh(),
        c => c.Variant == "VolumeVoter" ? c.VolumeVoters >= 3 : c.Nodes > c.EffectiveControlVoters || MeshOracle.Tolerance(c.EffectiveControlVoters) >= 1);

    /// <summary>
    /// MESH-025: keep a volume voter (or a control learner) down while more entries than the Raft snapshot threshold
    /// commit — overwrites, deletions and renames for the volume (directory creations, deletions and renames instead of
    /// overwrites when the data policy cannot acknowledge writes without that voter); principal creations for the control
    /// group — so the log it misses is compacted. On return it installs a checkpoint (the snapshot-install point is
    /// reached on it and its snapshot index exceeds what it had applied), catches up to the leader's commit index as of its
    /// return, deleted/renamed names do not resurrect on it, principals created meanwhile authenticate through it alone,
    /// and its replicated state equals the others.
    /// </summary>
    [Theory]
    [MemberData(nameof(LaggingMatrix))]
    [Catalog("MESH-025", "S", "INV-06", "INV-07", "INV-18")]
    public async Task MESH_025_LaggingNodeCatchesUpThroughCheckpoint(int nodes, string config)
    {
        MeshConfig cfg = MeshHelpers.Config(nodes, config);
        await ClusterTest.RunAsync(MeshHelpers.Sim(cfg), async cluster =>
        {
            MeshScenario s = await MeshScenario.StartAsync(cluster, cfg, MeshHelpers.Options(namespaceScript: false), Ct);
            bool volume = cfg.Variant == "VolumeVoter";
            string group = volume ? Vol : "control";
            string leader = volume ? await s.VolumeLeaderAsync() : await s.ControlLeaderAsync();
            // Volume variant: prefer a follower whose loss still lets the data policy acknowledge writes (overwrites are then
            // possible). Control variant: prefer a pure learner (neither control nor volume voter), then a
            // non-control-voter, then a control voter (the matrix constraint guarantees the control group tolerates it).
            string lagger = volume
                ? s.VolumeVoters.Where(n => n != leader)
                    .OrderBy(n => s.ExpectIf(net => net.SetDown(n, true), MeshProbeKind.WriteSmall, "p0").Outcome == MeshTri.Must ? 0 : 1)
                    .ThenBy(n => n, StringComparer.Ordinal)
                    .First()
                : MeshHelpers.Names(s).Where(n => n != leader)
                    .OrderBy(n => (s.ControlVoters.Contains(n) ? 2 : 0) + (s.VolumeVoters.Contains(n) ? 1 : 0))
                    .ThenBy(n => n, StringComparer.Ordinal)
                    .First();
            ulong appliedBefore = (await cluster.Node(lagger).Inspect.GetGroupAsync(group))!.AppliedIndex;
            await s.CrashAsync(lagger);

            DfsVolume v = s.Client("p0").GetVolume(Vol);
            int entries = s.Timings.RaftSnapshotThreshold + 40;
            bool overwrites = volume && s.Expect(MeshProbeKind.WriteSmall, "p0").Outcome == MeshTri.Must;
            if (volume)
            {
                await MeshHelpers.MustAsync(s, () => v.Directories.CreateDirectoryAsync("/mesh/lag", Ct), "create /mesh/lag");
                for (int i = 0; i < entries; i++)
                {
                    int k = i;
                    if (overwrites)
                        await MeshHelpers.MustAsync(s, () => v.Files.WriteAllBytesAsync($"/mesh/lag/x{k % 16}", TestData.Bytes((ulong)k, 200), Ct), "overwrite while a voter is down");
                    else
                        await MeshHelpers.MustAsync(s, () => v.Directories.CreateDirectoryAsync($"/mesh/lag/x{k % 16}/s{k}", Ct), "metadata entry while a voter is down");
                }

                // Reserved ids: a retry after an unknown outcome returns the stored outcome instead of failing on the
                // already-moved source.
                var deleteId = new DfsMutationOptions { OperationId = await MeshHelpers.MustAsync(s, () => v.NewOperationIdAsync(Ct), "reserve an id") };
                var moveId = new DfsMutationOptions { OperationId = await MeshHelpers.MustAsync(s, () => v.NewOperationIdAsync(Ct), "reserve an id") };
                if (overwrites)
                {
                    await MeshHelpers.MustAsync(s, () => v.Files.DeleteAsync("/mesh/lag/x0", deleteId, Ct), "delete while a voter is down");
                    await MeshHelpers.MustAsync(s, () => v.Files.MoveAsync("/mesh/lag/x1", "/mesh/lag/renamed", false, moveId, Ct), "rename while a voter is down");
                }
                else
                {
                    await MeshHelpers.MustAsync(s, () => v.Directories.DeleteAsync("/mesh/lag/x0", true, deleteId, Ct), "delete while a voter is down");
                    await MeshHelpers.MustAsync(s, () => v.Directories.MoveAsync("/mesh/lag/x1", "/mesh/lag/renamed", moveId, Ct), "rename while a voter is down");
                }
            }
            else
            {
                for (int i = 0; i < entries; i++)
                {
                    string principal = "lag" + i.ToString(CultureInfo.InvariantCulture);
                    await MeshHelpers.MustAsync(s, async () =>
                    {
                        await s.Client("p0").Admin.CreatePrincipalAsync(principal, "secret", clusterAdmin: false, Ct);
                        return principal;
                    }, "principal creation while a control member is down");
                }
            }

            await cluster.WaitUntilAsync(async () =>
            {
                GroupInspection g = volume ? await s.VolumeGroupAsync() : await cluster.Inspect.GetControlGroupAsync();
                return g.SnapshotIndex > appliedBefore;
            }, s.Options.RecoveryTimeout, $"the {group} leader must compact past index {appliedBefore} after {entries} entries (threshold {s.Timings.RaftSnapshotThreshold})", Ct);

            // Observe the checkpoint-install path on the returning node (a no-op action; only reachability is asserted).
            FaultArm install = cluster.Faults.Arm(new FaultMatch(FaultSites.RaftSnapshotInstall, Node: lagger, Count: 1_000_000), FaultAction.Delay(TimeSpan.Zero));
            await s.RestartAsync(lagger);
            // The workload keeps committing, so "caught up" is measured against the leader's commit index at the return.
            ulong target = (volume ? await s.VolumeGroupAsync() : await cluster.Inspect.GetControlGroupAsync()).CommitIndex;
            await cluster.WaitUntilAsync(async () =>
            {
                GroupInspection? mine = await cluster.Node(lagger).Inspect.GetGroupAsync(group);
                return mine is not null && mine.AppliedIndex >= target;
            }, s.Options.RecoveryTimeout, $"{lagger} must catch up with the {group} leader's commit index {target}", Ct);
            GroupInspection caught = (await cluster.Node(lagger).Inspect.GetGroupAsync(group))!;
            cluster.Faults.AssertReached(install);
            install.Disarm();
            Assert.True(caught.SnapshotIndex > appliedBefore,
                $"{lagger} applied {appliedBefore} before going down; the missing log was compacted so it must have installed a checkpoint (snapshot index {caught.SnapshotIndex}, RCV-03)");
            if (volume)
            {
                if (overwrites)
                {
                    VolumeStateInspection state = (await cluster.Node(lagger).Inspect.GetVolumeStateAsync(Vol))!;
                    Assert.Null(state.GetFile("/mesh/lag/x0"));
                    Assert.Null(state.GetFile("/mesh/lag/x1"));
                    Assert.NotNull(state.GetFile("/mesh/lag/renamed"));
                }
                else
                {
                    Assert.False(await v.Directories.ExistsAsync("/mesh/lag/x0", Ct), "/mesh/lag/x0 was deleted while the voter was down");
                    Assert.False(await v.Directories.ExistsAsync("/mesh/lag/x1", Ct), "/mesh/lag/x1 was renamed while the voter was down");
                    Assert.True(await v.Directories.ExistsAsync("/mesh/lag/renamed", Ct), "the renamed directory exists");
                }

                // Identical replicated-state digests on every caught-up replica, the returned node included.
                await cluster.Inspect.AssertReplicasConvergedAsync(Vol);
            }
            else
            {
                // The returned member serves the principals created while it was down (its control state caught up).
                string last = "lag" + (entries - 1).ToString(CultureInfo.InvariantCulture);
                await using DfsClient check = await cluster.ConnectAsync(new TestClientOptions
                {
                    Name = "lag-check",
                    Credentials = new DfsCredentials(last, "secret"),
                    SeedNodes = [lagger],
                    OperationTimeout = s.Options.ClientOperationTimeout,
                }, Ct);
                Assert.Equal(last, check.Principal);
            }

            await s.FinishAsync();
        }, Ct);
    }

    // ============================================================================================================
    // MESH-026 — a second failure during recovery
    // ============================================================================================================

    public static TheoryData<int, string> SecondFailureMatrix() => MeshHelpers.Matrix("MESH-026",
        () => MeshHelpers.NodeCounts.Where(n => n >= 3).SelectMany(n => new[] { "3", "5" }.Where(v => int.Parse(v, CultureInfo.InvariantCulture) <= n)
            .Select(v => MeshHelpers.Row(("n", MeshHelpers.N(n)), ("vv", v)))),
        MeshHelpers.FullMesh("vv"));

    /// <summary>
    /// MESH-026: crash the leader L1, let L2 take over and commit W, crash L2 too (three voters: no majority, typed
    /// failures; five voters: L3 takes over), then restart L1 while L2 is still down and finally L2. Availability follows
    /// the oracle at every step, W is never lost although L1 never saw it (CON-005), and histories stay valid.
    /// </summary>
    [Theory]
    [MemberData(nameof(SecondFailureMatrix))]
    [Catalog("MESH-026", "S", "INV-01", "INV-03", "INV-08", "INV-18")]
    public async Task MESH_026_SecondFailureDuringRecovery(int nodes, string config)
    {
        MeshConfig cfg = MeshHelpers.Config(nodes, config);
        await ClusterTest.RunAsync(MeshHelpers.Sim(cfg), async cluster =>
        {
            MeshScenario s = await MeshScenario.StartAsync(cluster, cfg, MeshHelpers.Options(), Ct);
            string l1 = await s.VolumeLeaderAsync();
            await s.CrashAsync(l1);
            string l2 = await s.WaitForVolumeLeaderAmongAsync(s.VolumeVoters.Where(v => v != l1).ToArray(), "a second voter must take over");

            // W: a file when the data policy can acknowledge it without L1 (else a directory: a metadata-only commit, so a
            // policy that needs L1's host, e.g. ack 3 on three nodes, does not make "commit W" impossible).
            const string wPath = "/mesh/w-after-l1";
            byte[] w = TestData.Bytes(2626, 400);
            DfsVolume v = s.Client("p0").GetVolume(Vol);
            bool wIsFile = s.Expect(MeshProbeKind.WriteSmall, "p0").Outcome == MeshTri.Must;
            if (wIsFile)
                await MeshHelpers.MustAsync(s, () => v.Files.WriteAllBytesAsync(wPath, w, Ct), "W must commit under L2");
            else
                await MeshHelpers.MustAsync(s, () => v.Directories.CreateDirectoryAsync(wPath, Ct), "W must commit under L2");
            IReadOnlyList<IReadOnlyList<string>> wHolders = wIsFile ? await s.RecordHoldersAsync(wPath) : [];

            async Task<bool> VerifyWAsync(string because)
            {
                if (!wIsFile)
                {
                    Assert.True(await v.Directories.ExistsAsync(wPath, Ct), $"{because}: directory W reported absent");
                    return true;
                }

                MeshHelpers.BytesEqual(w, await v.Files.ReadAllBytesAsync(wPath, Ct), because);
                return true;
            }

            await s.CrashAsync(l2);
            await s.ProbeAsync("l1-l2-down");
            await s.AssertSafetyAsync("l1-l2-down");
            await s.RestartAsync(l1);
            await s.ProbeAsync("l1-back-l2-down");
            if (s.ExpectRead(wHolders, "p0").Outcome == MeshTri.Must)
                await MeshHelpers.MustAsync(s, () => VerifyWAsync("W committed under L2, read after L1 returned without it (CON-005)"), "W must be readable");
            await s.RestartAsync(l2);
            await s.FinishAsync();
            await VerifyWAsync("W after full recovery");
        }, Ct);
    }

    // ============================================================================================================
    // MESH-027 — membership change during a partition
    // ============================================================================================================

    public static TheoryData<int, string> MembershipPartitionMatrix() => MeshHelpers.Matrix("MESH-027",
        () => MeshHelpers.ByNodes(MeshHelpers.NodeCounts.Where(n => n is >= 3 and <= 9)),
        MeshHelpers.FullMesh("crash", "cv").Append(MeshConfig.Restrict("cv", "3", "5")).ToArray(),
        c => c.EffectiveControlVoters >= 3);

    /// <summary>
    /// MESH-027: partition the nodes into a side with a majority of the control voters and a side with the rest, with a
    /// client on each side. Adding a node through the minority side fails typed; adding it through the majority side
    /// succeeds exactly once. After healing every node agrees on one membership containing the new node once, and no
    /// phantom member from the rejected attempt exists (CON-014).
    /// </summary>
    [Theory]
    [MemberData(nameof(MembershipPartitionMatrix))]
    [Catalog("MESH-027", "S", "INV-08", "INV-11", "INV-18")]
    public async Task MESH_027_MembershipChangeDuringPartitionKeepsOneAuthority(int nodes, string config)
    {
        MeshConfig cfg = MeshHelpers.Config(nodes, config);
        await ClusterTest.RunAsync(MeshHelpers.Sim(cfg), async cluster =>
        {
            MeshScenario s = await MeshScenario.StartAsync(cluster, cfg, MeshHelpers.Options(probeClients: ["p0", "p1"]), Ct);
            int majority = MeshOracle.Majority(s.ControlVoters.Count);
            string[] sideA = s.ControlVoters.Take(majority).Concat(MeshHelpers.Names(s).Where(n => !s.ControlVoters.Contains(n))).ToArray();
            string newName = "n" + nodes.ToString(CultureInfo.InvariantCulture);
            string[] sideB = MeshHelpers.Names(s).Except(sideA).ToArray();
            s.PartitionNodes([[.. sideA, newName], sideB], client => client == "client:p1" ? 1 : 0);

            MeshExpectation minorityControl = s.Expect(MeshProbeKind.ControlChange, "p1");
            Assert.Equal(MeshTri.Never, minorityControl.Outcome);
            // A well-formed request (so argument validation cannot be what rejects it). Its endpoint does not exist, so an
            // implementation that contacts the new member before proposing may report it unavailable instead of the missing
            // quorum: any typed availability failure (or an unknown outcome with its id) is legal, success never is.
            var causes = new HashSet<MeshCause>(minorityControl.Causes) { MeshCause.Unreachable };
            Exception minority = await Assert.ThrowsAnyAsync<Exception>(() =>
                s.Client("p1").Admin.AddNodeAsync("phantom", new DfsTopologyLabels("r1", "z1", "rk1", "phantom", "d1"), ["mem://phantom"], Ct));
            string? why = MeshOracle.CheckFailure(minority, causes, mutation: true, s.Policy);
            Assert.True(why is null, "adding a member through the minority side: " + why);

            string zone = cfg.Topology == TestTopology.OnePerZone ? "z" + (nodes + 1).ToString(CultureInfo.InvariantCulture) : "z1";
            await s.AddNodeAsync(new TestNodeSpec(newName, zone));
            await s.ProbeAsync("membership-partitioned");
            await s.AssertSafetyAsync("membership-partitioned");
            await s.FinishAsync();

            IReadOnlyList<DfsNodeInfo> listed = await s.Client("p1").Admin.ListNodesAsync(Ct);
            Assert.Single(listed, i => i.Name == newName);
            Assert.DoesNotContain(listed, i => i.Name == "phantom" && i.Status != DfsNodeStatus.Removed);
            Assert.Equal(nodes + 1, listed.Count(i => i.Status == DfsNodeStatus.Active));
        }, Ct);
    }

    // ============================================================================================================
    // MESH-036 — clock skew and drift across the mesh
    // ============================================================================================================

    public static TheoryData<int, string> ClockSkewMatrix() => MeshHelpers.Matrix("MESH-036",
        () => MeshHelpers.ByNodes(MeshHelpers.NodeCounts, "LeaderFast", "LeaderSlow", "LeaderWallBackwards", "FollowersWallForward", "AllDrift"),
        MeshHelpers.FullMesh("crash"));

    /// <summary>
    /// MESH-036: skew the node clocks under load — the volume leader's clock runs at 1.5× or 0.6×, its wall clock jumps
    /// back an hour, every follower's wall clock jumps forward an hour, or every node drifts (seeded rates 0.80–1.25 and
    /// wall offsets of ±60 min). Wall-clock time never orders commits (NS-04, ARCHITECTURE §3.1.5): every write that
    /// succeeds gets a strictly greater commit token and revision than the writes acknowledged before it and is read back
    /// exactly, also after authority moves to a node with a skewed clock (elections run on monotonic ticks, so a surviving
    /// voter takes over). Server clock changes may reduce availability but never safety (LOCK-015, SEC-017): while skewed
    /// every operation either succeeds correctly or fails typed (probes: the oracle's expectation with required successes
    /// relaxed), and a writer whose lease is lost cannot publish (its commit fails, its bytes never appear, INV-05). Once
    /// the clocks agree again the oracle's availability is required, the ordering holds for a new write, histories stay
    /// linearizable/serializable and replicas converge.
    /// </summary>
    [Theory]
    [MemberData(nameof(ClockSkewMatrix))]
    [Catalog("MESH-036", "S", "INV-03", "INV-04", "INV-05", "INV-08", "INV-17")]
    public async Task MESH_036_ClockSkewAndDriftNeverOrderCommits(int nodes, string config)
    {
        MeshConfig cfg = MeshHelpers.Config(nodes, config);
        await ClusterTest.RunAsync(MeshHelpers.Sim(cfg), async cluster =>
        {
            MeshScenario s = await MeshScenario.StartAsync(cluster, cfg, MeshHelpers.Options(), Ct);
            string leader = await s.VolumeLeaderAsync();
            DfsFileClient files = s.Client("p0").GetVolume(Vol).Files;

            // Each step writes its own file once (retried with the same bytes), so a step that ends unknown can never be
            // confused with another step's value; tokens and revisions (Raft indexes, ARCHITECTURE §2.2) order steps.
            CommitToken last = CommitToken.None;
            ulong lastRevision = 0;
            async Task<bool> StepAsync(string name, bool required, string because)
            {
                string path = "/mesh/clock-" + name;
                byte[] data = TestData.Bytes(3600UL + name[0], 300);
                (bool ok, CommitToken t) = required
                    ? (true, await MeshHelpers.MustAsync(s, () => files.WriteAllBytesAsync(path, data, Ct), because))
                    : await MeshHelpers.TryWithinAsync(s, () => files.WriteAllBytesAsync(path, data, Ct), because);
                if (!ok)
                    return false;
                if (!last.IsNone)
                {
                    Assert.True(t.IsComparableWith(last), $"tokens {last} and {t} of one volume and epoch must be comparable");
                    Assert.True(t.CompareTo(last) > 0, $"{because}: write {t} must be ordered after the earlier acknowledged {last}, whatever the wall clocks say (NS-04)");
                }

                last = t;
                (bool infoOk, DfsFileInfo? info) = required
                    ? (true, await MeshHelpers.MustAsync(s, () => files.GetInfoAsync(path, Ct), because))
                    : await MeshHelpers.TryWithinAsync(s, () => files.GetInfoAsync(path, Ct), because, mutation: false);
                if (infoOk && info is not null)
                {
                    Assert.True(info.Revision > lastRevision, $"{because}: revision {info.Revision} must exceed the earlier {lastRevision}; wall-clock time never orders commits");
                    lastRevision = info.Revision;
                }

                (bool readOk, byte[]? back) = required
                    ? (true, await MeshHelpers.MustAsync(s, () => files.ReadAllBytesAsync(path, Ct), because))
                    : await MeshHelpers.TryWithinAsync(s, () => files.ReadAllBytesAsync(path, Ct), because, mutation: false);
                if (readOk && back is not null)
                    MeshHelpers.BytesEqual(data, back, because);
                return true;
            }

            await StepAsync("1", required: true, "write before the clock change");

            IRandomSource rng = new SeededRandom(cluster.Seed).Fork("clock");
            switch (cfg.Variant)
            {
                case "LeaderFast":
                    cluster.Node(leader).Clock.SetRate(1.5);
                    break;
                case "LeaderSlow":
                    cluster.Node(leader).Clock.SetRate(0.6);
                    break;
                case "LeaderWallBackwards":
                    cluster.Node(leader).Clock.JumpWall(TimeSpan.FromHours(-1));
                    break;
                case "FollowersWallForward":
                    foreach (TestNode node in cluster.Nodes.Where(n => n.Name != leader))
                        node.Clock.JumpWall(TimeSpan.FromHours(1));
                    break;
                default:
                    foreach (TestNode node in cluster.Nodes)
                    {
                        node.Clock.SetRate(0.80 + (rng.Next(46) / 100.0));
                        node.Clock.JumpWall(TimeSpan.FromMinutes(rng.Next(121) - 60));
                    }

                    break;
            }

            s.Note("clocks skewed: " + string.Join(", ", cluster.Nodes.Select(n => $"{n.Name} rate {n.Clock.Rate:0.00} wall {n.Clock.GetUtcNow():HH:mm:ss}")));
            await StepAsync("2", required: false, "sequential write after the clock change");

            // A writer holding a lease across three lease durations of skewed time: renewals normally keep it valid (leases
            // run on the leader's monotonic clock and are renewed every third of the lease), but a server clock change may
            // cost availability (LOCK-015) — never a publication by a writer whose lease was lost.
            const string leased = "/mesh/clock-leased";
            byte[] leasedData = TestData.Bytes(3603, 5000);
            OpOutcome leasedOutcome = OpOutcome.Failed;
            CommitToken leasedToken = CommitToken.None;
            (bool opened, DfsStream? writer) = await MeshHelpers.TryWithinAsync(s, () => files.OpenAsync(leased, FileMode.Create, FileAccess.Write, FileShare.Read, Ct), "open a writer under skew");
            if (opened && writer is not null)
            {
                try
                {
                    await writer.WriteAsync(leasedData, Ct);
                    await cluster.DelayAsync(s.Timings.LeaseDuration * 3, Ct);
                    leasedToken = await writer.CommitAsync(Ct);
                    leasedOutcome = OpOutcome.Ok;
                }
                catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
                {
                    Exception e = MeshOracle.Unwrap(ex);
                    Assert.True(e is DfsLeaseLostException or DfsUnavailableException or DfsCommitOutcomeUnknownException or DfsOperationExpiredException,
                        $"committing a leased writer under skew may only fail typed: {MeshOracle.Describe(e)}");
                    if (e is DfsLeaseLostException or DfsCommitOutcomeUnknownException)
                        Assert.True(writer.IsFaulted, $"lease loss or an unknown publication must fault the stream (FSD §6.4): {MeshOracle.Describe(e)}");
                    leasedOutcome = MeshOracle.ClassifyOutcome(e);
                    try
                    {
                        await writer.AbortAsync(Ct); // never let disposal publish the staged bytes later
                    }
                    catch (Exception abort) when (abort is not Xunit.Sdk.XunitException)
                    {
                        s.Note("abort after the failed commit: " + MeshOracle.Describe(abort));
                    }
                }

                try
                {
                    await writer.DisposeAsync();
                }
                catch (Exception ex) when (leasedOutcome != OpOutcome.Ok && ex is not Xunit.Sdk.XunitException)
                {
                    s.Note("dispose of the failed writer: " + MeshOracle.Describe(ex));
                }

                if (leasedOutcome == OpOutcome.Ok && !last.IsNone)
                    Assert.True(leasedToken.CompareTo(last) > 0, $"commit {leasedToken} must be ordered after the earlier acknowledged {last}");
                if (leasedOutcome == OpOutcome.Ok)
                    last = leasedToken;
            }

            await ProbeRelaxedAsync(s, "skewed", "node clocks are skewed (LOCK-015: availability may be reduced, safety may not)");
            await s.AssertSafetyAsync("skewed");

            if (MeshOracle.Tolerance(s.VolumeVoters.Count) >= 1)
            {
                // Hand authority to a node whose clock differs from the old leader's. Elections run on monotonic ticks
                // (heartbeat 5 ticks vs election timeout 30-60 ticks), so drift of 0.6-1.5x cannot prevent one.
                await s.CrashAsync(leader);
                string successor = await s.WaitForVolumeLeaderAmongAsync(s.VolumeVoters.Where(v => v != leader).ToArray(), "a skewed-clock voter must take over");
                await StepAsync("3", required: false, "write under the skewed successor " + successor);
                await ProbeRelaxedAsync(s, "skewed-failover", "node clocks are skewed (LOCK-015)");
                await s.AssertSafetyAsync("skewed-failover");
                await s.RestartAsync(leader);
            }

            // Clocks agree again (rate 1, wall time aligned with the harness clock): full availability and ordering return.
            foreach (TestNode node in cluster.Nodes)
            {
                node.Clock.SetRate(1.0);
                TimeSpan behind = cluster.Time.GetUtcNow() - node.Clock.GetUtcNow();
                if (behind != TimeSpan.Zero)
                    node.Clock.JumpWall(behind);
            }

            s.Note("clocks restored");
            await StepAsync("4", required: true, "write after the clocks agree again");
            if (opened)
            {
                // The creating open committed, so the file exists; its content shows whether the writer published.
                byte[] leasedNow = await MeshHelpers.MustAsync(s, () => files.ReadAllBytesAsync(leased, Ct), "read the leased file after the clocks agree");
                if (leasedOutcome == OpOutcome.Ok)
                    MeshHelpers.BytesEqual(leasedData, leasedNow, "writer committed across three lease durations under skew");
                else if (leasedOutcome == OpOutcome.Failed)
                    Assert.False(leasedNow.AsSpan().SequenceEqual(leasedData), "a writer whose commit failed definitely (lease lost or unavailable) published its bytes (INV-05)");
            }

            await s.FinishAsync();
        }, Ct);
    }

    /// <summary>Probes every kind against the oracle with required successes relaxed to "success or typed failure".</summary>
    private static Task ProbeRelaxedAsync(MeshScenario s, string phase, string why) =>
        Task.WhenAll(s.Options.ProbeKinds.Select(kind => s.ProbeOneAsync(phase, "p0", kind, MeshHelpers.NotPromised(s.Expect(kind, "p0"), kind, why))));

    // ============================================================================================================
    // MESH-037 — slow node links (below and above the election timeout)
    // ============================================================================================================

    public static TheoryData<int, string> LatencyMatrix() => MeshHelpers.Matrix("MESH-037",
        () => MeshHelpers.ByNodes(MeshHelpers.NodeCounts, "AllBelowElection", "AllAboveElection", "LeaderZoneSlow"),
        MeshHelpers.FullMesh("crash"));

    /// <summary>
    /// MESH-037: delay node-to-node links (clients unaffected): every link by a quarter of the minimum election timeout
    /// (the oracle's availability must hold), every link by twice the maximum election timeout (no progress is promised:
    /// success with verified bytes or a typed failure, never more than one leader per term, no lost acknowledged write),
    /// or every link touching the volume leader's zone by twice the election timeout (when the nodes with fast links hold a
    /// majority of the configured voters service must continue within the bound — through them or through the slow leader;
    /// otherwise no progress is promised). At most one leader per term throughout, also after the delayed vote/append
    /// traffic arrives. Removing the delay restores full service within the bound and everything converges.
    /// </summary>
    [Theory]
    [MemberData(nameof(LatencyMatrix))]
    [Catalog("MESH-037", "S", "INV-01", "INV-03", "INV-05", "INV-08", "INV-17")]
    public async Task MESH_037_SlowLinksKeepSafetyAndConditionalProgress(int nodes, string config)
    {
        MeshConfig cfg = MeshHelpers.Config(nodes, config);
        await ClusterTest.RunAsync(MeshHelpers.Sim(cfg), async cluster =>
        {
            MeshScenario s = await MeshScenario.StartAsync(cluster, cfg, MeshHelpers.Options(), Ct);
            string leader = await s.VolumeLeaderAsync();
            string[] names = MeshHelpers.Names(s);
            string leaderZone = s.Net.Spec(leader).Zone;
            string[] slow = cfg.Variant == "LeaderZoneSlow" ? names.Where(n => s.Net.Spec(n).Zone == leaderZone).ToArray() : names;
            TimeSpan latency = cfg.Variant == "AllBelowElection" ? s.Timings.Tick * s.Timings.ElectionTicksMin / 4 : s.ElectionWindow * 2;
            var delayed = new List<(string From, string To)>();
            foreach (string a in names)
            {
                foreach (string b in names.Where(b => b != a && (slow.Contains(a) || slow.Contains(b))))
                {
                    cluster.Network.SetLatency(a, b, latency);
                    delayed.Add((a, b));
                }
            }

            s.Note($"latency {latency.TotalMilliseconds} ms on {delayed.Count} directed node links touching [{string.Join(",", slow)}]");

            // The fast side: nodes none of whose links are delayed. Its quorum (over configured voters) decides Must.
            MeshNetworkModel fast = s.Net.Clone();
            foreach (string node in slow)
                fast.SetDown(node, true);
            var expectations = new Dictionary<MeshProbeKind, MeshExpectation>();
            foreach (MeshProbeKind kind in s.Options.ProbeKinds)
            {
                MeshExpectation real = s.Expect(kind, "p0");
                IReadOnlyList<string> group = kind == MeshProbeKind.ControlChange ? s.ControlVoters : s.VolumeVoters;
                bool fastSideServes = MeshOracle.Quorum(fast, group, "client:p0", MeshCause.MetadataQuorum).Outcome == MeshTri.Must;
                expectations[kind] = cfg.Variant switch
                {
                    "AllBelowElection" => real,
                    _ when cfg.Variant == "LeaderZoneSlow" && fastSideServes => real,
                    _ => MeshHelpers.NotPromised(real, kind, $"node links touching [{string.Join(",", slow)}] are slower ({latency.TotalMilliseconds} ms) than the election timeout"),
                };
            }

            await Task.WhenAll(expectations.Select(e => s.ProbeOneAsync("slow-links", "p0", e.Key, e.Value)));
            await s.AssertSafetyAsync("slow-links");
            await cluster.DelayAsync(s.ElectionWindow * 4, Ct); // let delayed vote/append traffic arrive in later terms
            await cluster.Inspect.AssertNoSplitBrainAsync();

            foreach ((string from, string to) in delayed)
                cluster.Network.SetLatency(from, to, TimeSpan.Zero);
            s.Note("latency removed");
            await s.ProbeAsync("links-fast-again");
            await s.FinishAsync();
        }, Ct);
    }
}
