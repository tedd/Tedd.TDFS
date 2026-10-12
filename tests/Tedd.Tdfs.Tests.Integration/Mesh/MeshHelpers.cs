using System.Globalization;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Testing.Mesh;

namespace Tedd.Tdfs.Tests.Integration.Mesh;

/// <summary>
/// Configurations of the MESH integration suite: real QUIC transport and real on-disk storage with 2, 3, 5, 7 and 10
/// nodes. Non-critical dimensions are fixed per node count (documented in docs/test-notes/mesh.md) so the real-boundary
/// runs cover several voter counts, topologies and policies without re-running the whole simulation matrix.
/// </summary>
internal static class MeshHelpers
{
    public const string Vol = MeshScenario.Volume;

    public static readonly int[] NodeCounts = [2, 3, 5, 7, 10];

    /// <summary>Per-node-count layout: (topology, control voters, volume voters, ack).</summary>
    public static MeshConfig Base(int nodes, MeshBoot boot = MeshBoot.Static, CrashMode crash = CrashMode.LoseUnflushed, string variant = "") => nodes switch
    {
        2 => new MeshConfig { Nodes = 2, Boot = boot, Topology = TestTopology.OnePerZone, ControlVoters = 3, VolumeVoters = 1, Ack = 1, Crash = crash, Variant = variant },
        3 => new MeshConfig { Nodes = 3, Boot = boot, Topology = TestTopology.OnePerZone, ControlVoters = 3, VolumeVoters = 3, Ack = 2, Crash = crash, Variant = variant },
        5 => new MeshConfig { Nodes = 5, Boot = boot, Topology = TestTopology.OnePerZone, ControlVoters = 5, VolumeVoters = 5, Ack = 3, Crash = crash, Variant = variant },
        7 => new MeshConfig { Nodes = 7, Boot = boot, Topology = TestTopology.TwoPlusOne, ControlVoters = 5, VolumeVoters = 3, Ack = 2, Crash = crash, Variant = variant },
        10 => new MeshConfig { Nodes = 10, Boot = boot, Topology = TestTopology.OnePerZone, ControlVoters = 3, VolumeVoters = 5, Ack = 2, Crash = crash, Variant = variant },
        _ => throw new ArgumentOutOfRangeException(nameof(nodes), nodes, "MESH integration runs use 2, 3, 5, 7 or 10 nodes."),
    };

    /// <summary>Real QUIC + disk options (optionally real host processes).</summary>
    public static TestClusterOptions Real(MeshConfig config, bool outOfProcess = false) =>
        config.ToClusterOptions(ClusterMode.Realtime, TransportKind.Quic, StorageKind.Disk, TimeSpan.FromMinutes(6)) with { OutOfProcess = outOfProcess };

    /// <summary>Smaller workload and probe set for real-time runs.</summary>
    public static MeshScenarioOptions Options(bool workload = true, bool namespaceScript = true, IReadOnlyList<string>? probeClients = null) => new()
    {
        Workload = workload,
        NamespaceScript = namespaceScript,
        NamespaceSteps = 8,
        ProbeClients = probeClients ?? ["p0"],
        ProbeKinds = [MeshProbeKind.WriteSmall, MeshProbeKind.WriteLarge, MeshProbeKind.ReadLarge, MeshProbeKind.Append, MeshProbeKind.ControlChange],
        WorkloadOptions = new MeshWorkloadOptions
        {
            Clients = 2,
            RegisterKeys = 2,
            Logs = 1,
            RenameDirectories = 1,
            TransactionKeys = 2,
            MinOperationsPerClient = 4,
            MaxOperationsPerClient = 80,
            ThinkTime = TimeSpan.FromMilliseconds(60),
        },
        RecoveryTimeout = TimeSpan.FromSeconds(120),
        MustWithin = TimeSpan.FromSeconds(45),
    };

    public static TheoryData<int, string> ByNodes(Func<int, IEnumerable<MeshConfig>> variants)
    {
        var data = new TheoryData<int, string>();
        foreach (int n in NodeCounts)
        {
            foreach (MeshConfig config in variants(n))
                data.Add(n, config.Format());
        }

        return data;
    }

    public static string N(int n) => n.ToString(CultureInfo.InvariantCulture);

    public static string[] Names(MeshScenario s) => s.Net.NodeNames.ToArray();

    /// <summary>Array overload of <see cref="AssertEx.BytesEqual"/> (spans cannot cross an awaited argument).</summary>
    public static void BytesEqual(byte[] expected, byte[] actual, string because) => AssertEx.BytesEqual(expected, actual, because);
}
