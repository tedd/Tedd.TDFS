using Tedd.Tdfs.Server;

namespace Tedd.Tdfs.Testing;

/// <summary>Configuration of a test cluster (topology, transport, storage, timing, faults and seeds).</summary>
public sealed record TestClusterOptions
{
    /// <summary>Largest supported number of live nodes.</summary>
    public const int MaxNodes = 10;

    /// <summary>Number of live nodes, 1..10 (ignored when <see cref="Nodes"/> is set).</summary>
    public int NodeCount { get; init; } = 3;

    /// <summary>Explicit node specifications; overrides <see cref="NodeCount"/> and <see cref="Topology"/>.</summary>
    public IReadOnlyList<TestNodeSpec>? Nodes { get; init; }

    /// <summary>Zone layout used when <see cref="Nodes"/> is null.</summary>
    public TestTopology Topology { get; init; } = TestTopology.OnePerZone;

    /// <summary>Control-group voters (clipped to the node count).</summary>
    public int ControlVoters { get; init; } = 3;

    /// <summary>Volumes created at startup.</summary>
    public IReadOnlyList<TestVolumeSpec> Volumes { get; init; } = [new("vol")];

    /// <summary>Cluster default data policy; null derives one from the topology (see <see cref="GetEffectiveDefaultPolicy()"/>).</summary>
    public DfsStoragePolicy? DefaultPolicy { get; init; }

    /// <summary>Number of late-sync backup nodes (b0, b1, ...).</summary>
    public int BackupNodeCount { get; init; } = 0;

    /// <summary>Execution mode.</summary>
    public ClusterMode Mode { get; init; } = ClusterMode.Realtime;

    /// <summary>Transport.</summary>
    public TransportKind Transport { get; init; } = TransportKind.InMemory;

    /// <summary>Storage medium.</summary>
    public StorageKind Storage { get; init; } = StorageKind.Simulated;

    /// <summary>Bootstrap mode.</summary>
    public BootstrapMode Bootstrap { get; init; } = BootstrapMode.Static;

    /// <summary>Seed graph for <see cref="BootstrapMode.SeedJoin"/>.</summary>
    public SeedGraph Seeds { get; init; } = SeedGraph.Full;

    /// <summary>Which node-to-node links exist.</summary>
    public LinkMesh Mesh { get; init; } = LinkMesh.Full;

    /// <summary>Master seed; 0 derives one from the test name (see <see cref="ResolveSeed"/>). Always reported on failure.</summary>
    public ulong Seed { get; init; } = 0;

    /// <summary>Node timings.</summary>
    public NodeTimings Timings { get; init; } = NodeTimings.Fast;

    /// <summary>Encrypt object payloads and journals at rest.</summary>
    public bool EncryptAtRest { get; init; } = true;

    /// <summary>Server-side safety mutations (negative controls only).</summary>
    public SafetyMutations Mutations { get; init; } = SafetyMutations.None;

    /// <summary>Capacity of each node's medium.</summary>
    public long NodeCapacityBytes { get; init; } = 1L << 30;

    /// <summary>Reserved bytes per node (journals, checkpoints, compaction, repair).</summary>
    public long ReservedBytes { get; init; } = 8L << 20;

    /// <summary>Scenario timeout (wall clock in realtime mode, virtual time in deterministic mode).</summary>
    public TimeSpan ScenarioTimeout { get; init; } = TimeSpan.FromSeconds(120);

    /// <summary>Run real Tedd.Tdfs.Host processes (requires <see cref="TransportKind.Quic"/> and <see cref="StorageKind.Disk"/>).</summary>
    public bool OutOfProcess { get; init; } = false;

    /// <summary>
    /// The software release every live node of the cluster is created and bootstrapped by (null: the build under test);
    /// set by <see cref="VersionedClusterTest.RunAsync"/> (FMT suite).
    /// </summary>
    internal SoftwareVersionProfile? InitialSoftwareVersion { get; init; }

    /// <summary>Data root; null means <c>$TDFS_TEST_ROOT</c> or <c>%TEMP%/tdfs-tests</c> (per-test subfolder below it).</summary>
    public string? DataRoot { get; init; }

    /// <summary>Last-chance per-node hook (name, options); prefer <see cref="ConfigureNodeOptions"/>.</summary>
    public Action<string, NodeOptions>? ConfigureNode { get; init; }

    /// <summary>Per-node options transform (name, options) → options; use <c>with</c> expressions.</summary>
    public Func<string, NodeOptions, NodeOptions>? ConfigureNodeOptions { get; init; }

    /// <summary>One node, one control voter, one volume voter, policy 1/1/1/1.</summary>
    public static TestClusterOptions SingleNode => new()
    {
        NodeCount = 1,
        Topology = TestTopology.OnePerZone,
        ControlVoters = 1,
        Volumes = [new TestVolumeSpec("vol") { Metadata = new DfsMetadataPolicy { Voters = 1 } }],
        DefaultPolicy = new DfsStoragePolicy
        {
            TargetCopies = 1,
            TargetDistinctZones = 1,
            DurableCopiesBeforeAck = 1,
            DurableZonesBeforeAck = 1,
        },
    };

    /// <summary>The default configuration: three nodes, one per zone, three voters, target 3 / ack 2 in 2 zones.</summary>
    public static TestClusterOptions ThreeZones => new();

    /// <summary>Five nodes in zones z1..z5, five control voters and five volume voters.</summary>
    public static TestClusterOptions FiveNodes => new()
    {
        NodeCount = 5,
        Topology = TestTopology.OnePerZone,
        ControlVoters = 5,
        Volumes = [new TestVolumeSpec("vol") { Metadata = new DfsMetadataPolicy { Voters = 5 } }],
    };

    /// <summary>Number of live nodes after resolving <see cref="Nodes"/> / <see cref="NodeCount"/>.</summary>
    public int EffectiveNodeCount => Nodes?.Count ?? NodeCount;

    /// <summary>Control voters clipped to the node count.</summary>
    public int EffectiveControlVoters => Math.Max(1, Math.Min(ControlVoters, EffectiveNodeCount));

    /// <summary>
    /// The node specifications: <see cref="Nodes"/> when set, otherwise <c>n0..n{N-1}</c> laid out per <see cref="Topology"/>
    /// (each node is its own host).
    /// </summary>
    /// <exception cref="InvalidOperationException"><see cref="TestTopology.Custom"/> without <see cref="Nodes"/>.</exception>
    public IReadOnlyList<TestNodeSpec> GetNodeSpecs()
    {
        if (Nodes is not null)
            return Nodes;
        if (Topology == TestTopology.Custom)
            throw new InvalidOperationException("TestTopology.Custom requires TestClusterOptions.Nodes.");

        int n = NodeCount;
        var specs = new TestNodeSpec[Math.Max(0, n)];
        int firstZoneCount = (2 * n + 2) / 3; // ceil(2n/3)
        for (int i = 0; i < specs.Length; i++)
        {
            string zone = Topology switch
            {
                TestTopology.OnePerZone => "z" + (i + 1),
                TestTopology.SingleZone => "z1",
                TestTopology.TwoPlusOne => i < firstZoneCount ? "z1" : "z2",
                _ => throw new InvalidOperationException($"Unsupported topology {Topology}."),
            };
            specs[i] = new TestNodeSpec("n" + i, zone);
        }

        return specs;
    }

    /// <summary>
    /// The cluster default data policy: <see cref="DefaultPolicy"/> when set, otherwise
    /// target = min(3, hosts), zones = min(3, zones), ack = min(2, hosts), ackZones = min(2, zones), where hosts and zones
    /// are the distinct host labels and distinct (region, zone) pairs of the node layout (clamped so the policy validates).
    /// </summary>
    public DfsStoragePolicy GetEffectiveDefaultPolicy()
    {
        if (DefaultPolicy is not null)
            return DefaultPolicy;
        return DerivePolicy(GetNodeSpecs());
    }

    /// <summary>
    /// The default data policy for volumes bound to <paramref name="pool"/> (Addendum A, POOL-03): <see cref="DefaultPolicy"/>
    /// when set, otherwise derived as in <see cref="GetEffectiveDefaultPolicy()"/> from the pool's nodes only
    /// (<c>"default"</c> = every node).
    /// </summary>
    /// <exception cref="ArgumentException">No node of the layout belongs to <paramref name="pool"/>.</exception>
    public DfsStoragePolicy GetEffectiveDefaultPolicy(string pool)
    {
        ArgumentNullException.ThrowIfNull(pool);
        if (DefaultPolicy is not null)
            return DefaultPolicy;
        TestNodeSpec[] members = GetNodeSpecs().Where(s => s.IsInPool(pool)).ToArray();
        if (members.Length == 0)
            throw new ArgumentException($"No node belongs to pool '{pool}'.", nameof(pool));
        return DerivePolicy(members);
    }

    /// <summary>Distinct pool names listed by the node layout (ordinal order, excluding <c>"default"</c>).</summary>
    public IReadOnlyList<string> GetPoolNames() =>
        GetNodeSpecs().SelectMany(s => s.EffectivePools).Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal).ToArray();

    private static DfsStoragePolicy DerivePolicy(IReadOnlyList<TestNodeSpec> specs)
    {
        int hosts = Math.Max(1, specs.Select(s => s.EffectiveHost).Distinct(StringComparer.Ordinal).Count());
        int zones = Math.Max(1, specs.Select(s => s.Region + "\0" + s.Zone).Distinct(StringComparer.Ordinal).Count());

        int target = Math.Min(3, hosts);
        int targetZones = Math.Min(Math.Min(3, zones), target);
        int ack = Math.Min(2, hosts);
        int ackZones = Math.Min(Math.Min(2, zones), Math.Min(ack, targetZones));
        return new DfsStoragePolicy
        {
            TargetCopies = target,
            TargetDistinctZones = targetZones,
            DurableCopiesBeforeAck = ack,
            DurableZonesBeforeAck = ackZones,
        };
    }

    /// <summary>The seed to use: <see cref="Seed"/> when nonzero, otherwise a stable nonzero hash of <paramref name="testName"/>.</summary>
    public ulong ResolveSeed(string testName)
    {
        ArgumentNullException.ThrowIfNull(testName);
        if (Seed != 0)
            return Seed;
        ulong derived = SeededRandom.DeriveSeed(0x7D_F5_7E_57_0000_0001UL, testName);
        return derived == 0 ? 1 : derived;
    }

    /// <summary>The data root: <see cref="DataRoot"/>, else <c>$TDFS_TEST_ROOT</c>, else <c>%TEMP%/tdfs-tests</c>.</summary>
    public string ResolveDataRoot()
    {
        if (!string.IsNullOrWhiteSpace(DataRoot))
            return DataRoot;
        string? env = Environment.GetEnvironmentVariable("TDFS_TEST_ROOT");
        if (!string.IsNullOrWhiteSpace(env))
            return env;
        return Path.Combine(Path.GetTempPath(), "tdfs-tests");
    }

    /// <summary>Validates ranges and combinations.</summary>
    /// <exception cref="ArgumentException">An option is out of range or inconsistent.</exception>
    public void Validate()
    {
        if (Nodes is null)
        {
            if (NodeCount is < 1 or > MaxNodes)
                throw new ArgumentException($"NodeCount must be 1..{MaxNodes} (was {NodeCount}).", nameof(NodeCount));
            if (Topology == TestTopology.Custom)
                throw new ArgumentException("TestTopology.Custom requires Nodes.", nameof(Nodes));
        }
        else
        {
            if (Nodes.Count is < 1 or > MaxNodes)
                throw new ArgumentException($"Nodes must contain 1..{MaxNodes} entries (was {Nodes.Count}).", nameof(Nodes));
            var names = new HashSet<string>(StringComparer.Ordinal);
            foreach (TestNodeSpec spec in Nodes)
            {
                if (spec is null)
                    throw new ArgumentException("Nodes contains null.", nameof(Nodes));
                if (string.IsNullOrWhiteSpace(spec.Name) || string.IsNullOrWhiteSpace(spec.Zone))
                    throw new ArgumentException("Every node needs a name and a zone.", nameof(Nodes));
                if (!names.Add(spec.Name))
                    throw new ArgumentException($"Duplicate node name '{spec.Name}'.", nameof(Nodes));
                foreach (string pool in spec.EffectivePools)
                {
                    if (!DfsPoolNames.IsValid(pool))
                        throw new ArgumentException($"Node '{spec.Name}' lists invalid pool name '{pool}'.", nameof(Nodes));
                    if (string.Equals(pool, DfsPoolNames.Default, StringComparison.Ordinal))
                        throw new ArgumentException($"Node '{spec.Name}' lists '{DfsPoolNames.Default}', which is implicit.", nameof(Nodes));
                }
            }
        }

        if (ControlVoters < 1)
            throw new ArgumentException($"ControlVoters must be at least 1 (was {ControlVoters}).", nameof(ControlVoters));
        if (Volumes is null)
            throw new ArgumentException("Volumes must not be null.", nameof(Volumes));
        var volumeNames = new HashSet<string>(StringComparer.Ordinal);
        foreach (TestVolumeSpec volume in Volumes)
        {
            if (volume is null || string.IsNullOrWhiteSpace(volume.Name))
                throw new ArgumentException("Every volume needs a name.", nameof(Volumes));
            if (!volumeNames.Add(volume.Name))
                throw new ArgumentException($"Duplicate volume name '{volume.Name}'.", nameof(Volumes));
            if (volume.Metadata.Voters < 1)
                throw new ArgumentException($"Volume '{volume.Name}' needs at least one voter.", nameof(Volumes));
            if (volume.InlineThreshold < 0)
                throw new ArgumentException($"Volume '{volume.Name}' has a negative inline threshold.", nameof(Volumes));
            if (volume.StoragePool is null || !DfsPoolNames.IsValid(volume.StoragePool))
                throw new ArgumentException($"Volume '{volume.Name}' has an invalid storage pool name.", nameof(Volumes));
            if (!GetNodeSpecs().Any(s => s.IsInPool(volume.StoragePool)))
                throw new ArgumentException($"Volume '{volume.Name}' uses pool '{volume.StoragePool}', which no node belongs to.", nameof(Volumes));
            volume.RootSecurity?.Validate();
        }

        if (BackupNodeCount < 0)
            throw new ArgumentException($"BackupNodeCount must not be negative (was {BackupNodeCount}).", nameof(BackupNodeCount));
        if (!Enum.IsDefined(Mode) || !Enum.IsDefined(Transport) || !Enum.IsDefined(Storage) || !Enum.IsDefined(Bootstrap)
            || !Enum.IsDefined(Seeds) || !Enum.IsDefined(Mesh) || !Enum.IsDefined(Topology))
            throw new ArgumentException("An enum option has an undefined value.");
        if (NodeCapacityBytes <= 0)
            throw new ArgumentException("NodeCapacityBytes must be positive.", nameof(NodeCapacityBytes));
        if (ReservedBytes < 0 || ReservedBytes >= NodeCapacityBytes)
            throw new ArgumentException("ReservedBytes must be nonnegative and below NodeCapacityBytes.", nameof(ReservedBytes));
        if (ScenarioTimeout <= TimeSpan.Zero)
            throw new ArgumentException("ScenarioTimeout must be positive.", nameof(ScenarioTimeout));
        if (Timings is null)
            throw new ArgumentException("Timings must not be null.", nameof(Timings));
        if (Mutations is null)
            throw new ArgumentException("Mutations must not be null.", nameof(Mutations));
        if (OutOfProcess && (Transport != TransportKind.Quic || Storage != StorageKind.Disk))
            throw new ArgumentException("OutOfProcess requires TransportKind.Quic and StorageKind.Disk.", nameof(OutOfProcess));
        if (OutOfProcess && Mode == ClusterMode.Deterministic)
            throw new ArgumentException("OutOfProcess clusters cannot run in deterministic mode.", nameof(OutOfProcess));
        if (Mode == ClusterMode.Deterministic && (Transport == TransportKind.Quic || Storage == StorageKind.Disk))
            throw new ArgumentException("Deterministic mode requires the in-memory transport and simulated storage.", nameof(Mode));

        GetEffectiveDefaultPolicy().Validate();
    }
}
