using System.Globalization;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Testing.Mesh;

namespace Tedd.Tdfs.Tests.Integration.Mesh;

/// <summary>
/// MESH — real-boundary repetitions of the critical mesh scenarios (test plan §2: critical simulation scenarios are
/// repeated against real transport/persistence): 2, 3, 5, 7 and 10 nodes over real QUIC (TLS 1.3, real sockets on
/// 127.0.0.1) with on-disk storage, real abrupt crashes (and, for one row, real host processes killed), smaller
/// workloads, and the same oracle, history checks and convergence checks as the simulation suite.
/// </summary>
public sealed class MeshIntegrationTests
{
    private const string Vol = MeshHelpers.Vol;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    // ============================================================================================================
    // MESH-028
    // ============================================================================================================

    public static TheoryData<int, string> FormationMatrix() => MeshHelpers.ByNodes(n =>
        new[] { MeshBoot.Static, MeshBoot.SeedStar, MeshBoot.SeedRandom }.Select(b => MeshHelpers.Base(n, b)));

    /// <summary>
    /// MESH-028: n real nodes over QUIC with disk storage form one cluster statically or by seed join (star and random
    /// seed graphs): every node Active and reachable, control and volume voters as configured, every node usable as the
    /// sole entry point of a client, the smaller concurrent workload linearizable/serializable, and all replicas converge.
    /// </summary>
    [Theory]
    [MemberData(nameof(FormationMatrix))]
    [Catalog("MESH-028", "I", "INV-03", "INV-07", "INV-11", "INV-17")]
    public async Task MESH_028_QuicDiskMeshFormsAndServes(int nodes, string config)
    {
        MeshConfig cfg = MeshConfig.Parse(nodes, config);
        await ClusterTest.RunAsync(MeshHelpers.Real(cfg), async cluster =>
        {
            Assert.Equal(TransportKind.Quic, cluster.Options.Transport);
            Assert.Equal(StorageKind.Disk, cluster.Options.Storage);
            MeshScenario s = await MeshScenario.StartAsync(cluster, cfg, MeshHelpers.Options(), Ct);
            foreach (TestNode node in cluster.Nodes)
            {
                Assert.StartsWith("quic://", node.Endpoint, StringComparison.Ordinal);
                Assert.NotNull(node.Disk.RootDirectory);
                Assert.True(Directory.Exists(node.Disk.RootDirectory), $"{node.Name} must persist to a real directory");
            }

            IReadOnlyList<DfsNodeInfo> listed = await s.Client("p0").Admin.ListNodesAsync(Ct);
            Assert.Equal(nodes, listed.Count);
            Assert.All(listed, i => Assert.Equal(DfsNodeStatus.Active, i.Status));
            await s.ProbeAsync("formed");

            for (int i = 0; i < nodes; i++)
            {
                string writer = "e" + i.ToString(CultureInfo.InvariantCulture);
                await s.ConnectClientAsync(writer, [cluster[i].Name]);
            }

            for (int i = 0; i < nodes; i++)
            {
                byte[] data = TestData.Bytes(2800UL + (ulong)i, 6000);
                string path = "/mesh/entry-" + cluster[i].Name;
                await s.Client("e" + i.ToString(CultureInfo.InvariantCulture)).GetVolume(Vol).Files.WriteAllBytesAsync(path, data, Ct);
                byte[] back = await s.Client("e" + ((i + 1) % nodes).ToString(CultureInfo.InvariantCulture)).GetVolume(Vol).Files.ReadAllBytesAsync(path, Ct);
                MeshHelpers.BytesEqual(data, back, $"written through {cluster[i].Name}, read through {cluster[(i + 1) % nodes].Name} over QUIC");
            }

            await s.FinishAsync();
        }, Ct);
    }

    // ============================================================================================================
    // MESH-029
    // ============================================================================================================

    public static TheoryData<int, string> LeaderCrashMatrix() => MeshHelpers.ByNodes(n =>
        new[] { CrashMode.LoseUnflushed, CrashMode.TearLastWrite }.Select(m => MeshHelpers.Base(n, crash: m)));

    /// <summary>
    /// MESH-029: abruptly crash the volume leader of a real QUIC/disk cluster under load (lose-unflushed and torn-last-
    /// write disk images). Availability while it is down matches the oracle (a surviving voter takes over when a majority
    /// of the configured voters remains); the node restarts from its on-disk image with incarnation + 1 and catches up;
    /// every acknowledged write survives and all histories are valid.
    /// </summary>
    [Theory]
    [MemberData(nameof(LeaderCrashMatrix))]
    [Catalog("MESH-029", "I", "INV-01", "INV-03", "INV-08", "INV-17", "INV-18")]
    public async Task MESH_029_QuicDiskLeaderCrashAndRestartFromDisk(int nodes, string config)
    {
        MeshConfig cfg = MeshConfig.Parse(nodes, config);
        await ClusterTest.RunAsync(MeshHelpers.Real(cfg), async cluster =>
        {
            MeshScenario s = await MeshScenario.StartAsync(cluster, cfg, MeshHelpers.Options(), Ct);
            string leader = await s.VolumeLeaderAsync();
            ulong term = (await s.VolumeGroupAsync()).Term;
            await cluster.DelayAsync(TimeSpan.FromSeconds(1), Ct);
            await s.CrashAsync(leader);
            if (MeshOracle.Quorum(s.Net, s.VolumeVoters, "client:p0", MeshCause.MetadataQuorum).Outcome == MeshTri.Must)
            {
                await s.WaitForVolumeLeaderAmongAsync(s.VolumeVoters.Where(v => v != leader).ToArray(), $"a surviving voter must replace crashed {leader}");
                Assert.True((await s.VolumeGroupAsync()).Term > term);
            }

            await s.ProbeAsync("leader-crashed");
            await s.AssertSafetyAsync("leader-crashed");
            await s.RestartAsync(leader);
            await s.FinishAsync();
        }, Ct);
    }

    // ============================================================================================================
    // MESH-030
    // ============================================================================================================

    public static TheoryData<int, string> PartitionMatrix() => MeshHelpers.ByNodes(n => [MeshHelpers.Base(n, variant: "LeaderInMinority")]);

    /// <summary>
    /// MESH-030: partition a real QUIC cluster into a side with a majority of the configured volume voters (without the
    /// old leader when there are three or more voters) and the rest, one probe client per side. The majority side serves
    /// exactly as the oracle says, the minority side's client gets typed failures only, there is at most one leader per
    /// term, and after healing all unknown outcomes resolve and replicas converge.
    /// </summary>
    [Theory]
    [MemberData(nameof(PartitionMatrix))]
    [Catalog("MESH-030", "I", "INV-03", "INV-05", "INV-06", "INV-08", "INV-18")]
    public async Task MESH_030_QuicDiskMajorityMinorityPartition(int nodes, string config)
    {
        MeshConfig cfg = MeshConfig.Parse(nodes, config);
        await ClusterTest.RunAsync(MeshHelpers.Real(cfg), async cluster =>
        {
            MeshScenario s = await MeshScenario.StartAsync(cluster, cfg, MeshHelpers.Options(probeClients: ["p0", "p1"]), Ct);
            string leader = await s.VolumeLeaderAsync();
            int majority = MeshOracle.Majority(s.VolumeVoters.Count);
            string[] others = s.VolumeVoters.Where(v => v != leader).ToArray();
            string[] majorityVoters = others.Length >= majority ? others.Take(majority).ToArray() : others.Take(majority - 1).Prepend(leader).ToArray();
            string[] nonVoters = MeshHelpers.Names(s).Where(n => !s.VolumeVoters.Contains(n)).ToArray();
            string[] sideA = majorityVoters.Concat(nonVoters.Where((_, i) => i % 2 == 1)).ToArray();
            string[] sideB = MeshHelpers.Names(s).Except(sideA).ToArray();
            Assert.NotEmpty(sideB);
            int index = 0;
            s.PartitionNodes([sideA, sideB], client => client == "client:p1" ? 1 : client == "client:p0" || client == "client:ns" ? 0 : index++ % 2);
            if (!majorityVoters.Contains(leader))
                await s.WaitForVolumeLeaderAmongAsync(majorityVoters, "the majority side must elect a leader over QUIC");
            await s.ProbeAsync("partitioned");
            await s.AssertSafetyAsync("partitioned");
            await s.FinishAsync();
        }, Ct);
    }

    // ============================================================================================================
    // MESH-031
    // ============================================================================================================

    public static TheoryData<int, string> RollingRestartMatrix() => MeshHelpers.ByNodes(n =>
        new[] { "Graceful", "Crash" }.Select(v => MeshHelpers.Base(n, variant: v)));

    /// <summary>
    /// MESH-031: restart every node of a real QUIC/disk cluster one at a time (graceful stop or abrupt crash) under load.
    /// While each node is down availability matches the oracle; each restart reads its on-disk image, advances the
    /// incarnation by one and rejoins; every acknowledged write survives; histories and replicas check out at the end.
    /// </summary>
    [Theory]
    [MemberData(nameof(RollingRestartMatrix))]
    [Catalog("MESH-031", "I", "INV-01", "INV-08", "INV-17", "INV-18")]
    public async Task MESH_031_QuicDiskRollingRestart(int nodes, string config)
    {
        MeshConfig cfg = MeshConfig.Parse(nodes, config);
        await ClusterTest.RunAsync(MeshHelpers.Real(cfg), async cluster =>
        {
            MeshScenarioOptions options = MeshHelpers.Options() with { ProbeKinds = [MeshProbeKind.WriteSmall, MeshProbeKind.ReadLarge] };
            MeshScenario s = await MeshScenario.StartAsync(cluster, cfg, options, Ct);
            foreach (string node in MeshHelpers.Names(s))
            {
                if (cfg.Variant == "Graceful")
                    await s.StopNodeAsync(node);
                else
                    await s.CrashAsync(node);
                await s.ProbeAsync("down-" + node);
                await s.RestartAsync(node);
                await cluster.WaitForStableAsync(s.Options.RecoveryTimeout, Ct);
                await cluster.WaitForReplicationAsync(Vol, Ct);
            }

            await s.FinishAsync();
        }, Ct);
    }

    // ============================================================================================================
    // MESH-032
    // ============================================================================================================

    public static TheoryData<int, string> QuorumLossMatrix() => MeshHelpers.ByNodes(n => [MeshHelpers.Base(n)]);

    /// <summary>
    /// MESH-032: on a real QUIC/disk cluster crash one voter more than the volume group tolerates — every operation fails
    /// typed (quorum unavailable, or unknown with an id for mutations), never fabricated success — then crash every node
    /// and cold-restart all of them from disk. Every acknowledged write and every committed unknown outcome is present
    /// afterwards; definitely failed writes left no trace.
    /// </summary>
    [Theory]
    [MemberData(nameof(QuorumLossMatrix))]
    [Catalog("MESH-032", "I", "INV-01", "INV-06", "INV-08", "INV-17")]
    public async Task MESH_032_QuicDiskQuorumLossThenColdRestart(int nodes, string config)
    {
        MeshConfig cfg = MeshConfig.Parse(nodes, config);
        await ClusterTest.RunAsync(MeshHelpers.Real(cfg), async cluster =>
        {
            MeshScenario s = await MeshScenario.StartAsync(cluster, cfg, MeshHelpers.Options(), Ct);
            string leader = await s.VolumeLeaderAsync();
            string[] victims = s.VolumeVoters.Where(v => v != leader).Append(leader).Take(MeshOracle.Tolerance(s.VolumeVoters.Count) + 1).ToArray();
            foreach (string victim in victims)
                await s.CrashAsync(victim);
            Assert.Equal(MeshTri.Never, MeshOracle.Quorum(s.Net, s.VolumeVoters, "client:p0", MeshCause.MetadataQuorum).Outcome);
            await s.ProbeAsync("quorum-lost");
            await s.AssertSafetyAsync("quorum-lost");

            foreach (string node in MeshHelpers.Names(s).Where(n => !s.Net.IsDown(n)))
                await s.CrashAsync(node);
            foreach (string node in MeshHelpers.Names(s))
                await s.RestartAsync(node);
            await s.FinishAsync();
        }, Ct);
    }

    // ============================================================================================================
    // MESH-033
    // ============================================================================================================

    public static TheoryData<int, string> OutOfProcessMatrix()
    {
        var data = new TheoryData<int, string>();
        foreach (int n in new[] { 3, 5 })
            data.Add(n, MeshHelpers.Base(n).Format());
        return data;
    }

    /// <summary>
    /// MESH-033: run 3 and 5 real <c>Tedd.Tdfs.Host</c> processes over QUIC with disk storage, kill the volume leader's
    /// process (real crash: no flush, no cleanup), keep serving through the survivors as the oracle requires, restart the
    /// process from its directory and check that every acknowledged write survived, unknown outcomes resolve and all
    /// replicas converge.
    /// </summary>
    [Theory]
    [MemberData(nameof(OutOfProcessMatrix))]
    [Catalog("MESH-033", "I", "INV-01", "INV-06", "INV-17", "INV-18")]
    public async Task MESH_033_OutOfProcessHostsKilledAndRestarted(int nodes, string config)
    {
        MeshConfig cfg = MeshConfig.Parse(nodes, config);
        await ClusterTest.RunAsync(MeshHelpers.Real(cfg, outOfProcess: true), async cluster =>
        {
            Assert.True(cluster.Options.OutOfProcess);
            MeshScenario s = await MeshScenario.StartAsync(cluster, cfg, MeshHelpers.Options(), Ct);
            foreach (TestNode node in cluster.Nodes)
                Assert.Null(node.Host); // real processes, not in-process hosts

            string leader = await s.VolumeLeaderAsync();
            await cluster.DelayAsync(TimeSpan.FromSeconds(1), Ct);
            await s.CrashAsync(leader, CrashMode.LoseUnflushed);
            await s.WaitForVolumeLeaderAmongAsync(s.VolumeVoters.Where(v => v != leader).ToArray(), "a surviving host process must take over");
            await s.ProbeAsync("process-killed");
            await s.AssertSafetyAsync("process-killed");
            await s.RestartAsync(leader);
            await s.FinishAsync();
        }, Ct);
    }

    // ============================================================================================================
    // MESH-038
    // ============================================================================================================

    public static TheoryData<int, string> PowerCutMatrix()
    {
        var data = new TheoryData<int, string>();
        foreach (int n in new[] { 3, 5 })
            data.Add(n, MeshHelpers.Base(n).Format());
        return data;
    }

    /// <summary>
    /// MESH-038 (hardware): a 3- and a 5-host mesh of real <c>Tedd.Tdfs.Host</c> processes on supported devices with the
    /// documented persistence settings; while the concurrent workload runs, cut power to every host at once (and, in a
    /// second run, to a majority of the voters only), cold-boot in arbitrary order. Required: every acknowledged write and
    /// committed unknown outcome survives from the devices' durable state alone, definitely failed writes left no trace,
    /// the histories are linearizable/serializable, and replicas converge. Needs a power-cut rig, so it is reported
    /// <c>not run</c> (test plan §2.1: not-run is never a pass); the configuration it would use is validated first.
    /// </summary>
    [Theory]
    [MemberData(nameof(PowerCutMatrix))]
    [Catalog("MESH-038", "H", "INV-01", "INV-06", "INV-17", "INV-18")]
    public void MESH_038_PowerCutOfEveryMeshHostOnSupportedHardware(int nodes, string config)
    {
        MeshConfig cfg = MeshConfig.Parse(nodes, config);
        TestClusterOptions options = MeshHelpers.Real(cfg, outOfProcess: true);
        options.Validate();
        Assert.Equal(TransportKind.Quic, options.Transport);
        Assert.Equal(StorageKind.Disk, options.Storage);
        Assert.Equal(nodes, options.EffectiveNodeCount);
        Assert.Skip($"requires a power-cut rig with {nodes} supported hosts (simultaneous cut of every host, then of a voter majority; cold boot); status: not run");
    }
}
