using System.Globalization;
using System.Text;
using Tedd.Tdfs.Storage;

namespace Tedd.Tdfs.Testing.Mesh;

/// <summary>How a mesh cluster is bootstrapped (static description, or seed join over a seed graph).</summary>
public enum MeshBoot
{
    /// <summary><see cref="BootstrapMode.Static"/>.</summary>
    Static,

    /// <summary><see cref="BootstrapMode.SeedJoin"/> with <see cref="SeedGraph.Full"/>.</summary>
    SeedFull,

    /// <summary><see cref="BootstrapMode.SeedJoin"/> with <see cref="SeedGraph.Ring"/>.</summary>
    SeedRing,

    /// <summary><see cref="BootstrapMode.SeedJoin"/> with <see cref="SeedGraph.Star"/>.</summary>
    SeedStar,

    /// <summary><see cref="BootstrapMode.SeedJoin"/> with <see cref="SeedGraph.Line"/>.</summary>
    SeedLine,

    /// <summary><see cref="BootstrapMode.SeedJoin"/> with <see cref="SeedGraph.RandomConnected"/>.</summary>
    SeedRandom,
}

/// <summary>
/// One configuration of the MESH matrix: node count plus the non-critical dimensions (bootstrap/seed graph, link mesh,
/// zone topology, control voters, volume voters, data acknowledgement count, crash mode) and a family-specific
/// critical variant. Encoded as a compact <c>key=value;...</c> string for theory data.
/// </summary>
public sealed record MeshConfig
{
    /// <summary>Volume name used by every mesh scenario.</summary>
    public const string VolumeName = "vol";

    /// <summary>Live node count, 2..10.</summary>
    public int Nodes { get; init; } = 3;

    /// <summary>Bootstrap mode and seed graph.</summary>
    public MeshBoot Boot { get; init; } = MeshBoot.Static;

    /// <summary>Node-to-node link mesh.</summary>
    public LinkMesh Mesh { get; init; } = LinkMesh.Full;

    /// <summary>Zone layout.</summary>
    public TestTopology Topology { get; init; } = TestTopology.OnePerZone;

    /// <summary>Configured control voters (1, 3, 5; the harness clips to the node count).</summary>
    public int ControlVoters { get; init; } = 3;

    /// <summary>Configured volume voters (1, 3, 5; even counts in dedicated tests).</summary>
    public int VolumeVoters { get; init; } = 3;

    /// <summary>Durable copies before acknowledgement (1..3).</summary>
    public int Ack { get; init; } = 2;

    /// <summary>Crash mode used whenever the scenario crashes a node.</summary>
    public CrashMode Crash { get; init; } = CrashMode.LoseUnflushed;

    /// <summary>Family-specific critical variant (for example the failure sub-kind).</summary>
    public string Variant { get; init; } = string.Empty;

    /// <summary>Fixed master seed (0 = derive from the test name).</summary>
    public ulong Seed { get; init; }

    /// <summary>Explicit control voters (static bootstrap only); overrides the central/default choice.</summary>
    public IReadOnlyList<string>? ControlVoterNodes { get; init; }

    /// <summary>Explicit volume voters; overrides the central/zone-based placement.</summary>
    public IReadOnlyList<string>? VolumeVoterNodes { get; init; }

    /// <summary>Control voters after clipping to the node count.</summary>
    public int EffectiveControlVoters => Math.Max(1, Math.Min(ControlVoters, Nodes));

    /// <summary>Harness bootstrap mode.</summary>
    public BootstrapMode Bootstrap => Boot == MeshBoot.Static ? BootstrapMode.Static : BootstrapMode.SeedJoin;

    /// <summary>Harness seed graph.</summary>
    public SeedGraph Seeds => Boot switch
    {
        MeshBoot.SeedRing => SeedGraph.Ring,
        MeshBoot.SeedStar => SeedGraph.Star,
        MeshBoot.SeedLine => SeedGraph.Line,
        MeshBoot.SeedRandom => SeedGraph.RandomConnected,
        _ => SeedGraph.Full,
    };

    /// <summary>The node layout (n0..n{N-1}) per <see cref="Topology"/>.</summary>
    public IReadOnlyList<TestNodeSpec> NodeSpecs() => new TestClusterOptions { NodeCount = Nodes, Topology = Topology }.GetNodeSpecs();

    /// <summary>Distinct zones of the layout.</summary>
    public int Zones => NodeSpecs().Select(s => s.Region + "\0" + s.Zone).Distinct(StringComparer.Ordinal).Count();

    /// <summary>
    /// The volume's data policy: ack = <see cref="Ack"/>; target = max(ack, min(3, hosts)); target zones = min(target,
    /// zones); ack zones = min(ack, target zones).
    /// </summary>
    public DfsStoragePolicy Policy()
    {
        int hosts = Nodes;
        int target = Math.Max(Ack, Math.Min(3, hosts));
        int targetZones = Math.Min(target, Zones);
        return new DfsStoragePolicy
        {
            TargetCopies = target,
            TargetDistinctZones = targetZones,
            DurableCopiesBeforeAck = Ack,
            DurableZonesBeforeAck = Math.Min(Ack, targetZones),
        };
    }

    /// <summary>
    /// Voters placed where a partial link mesh can elect them: the hub and its first leaves for <see cref="LinkMesh.Star"/>,
    /// a centred contiguous block for <see cref="LinkMesh.Ring"/>/<see cref="LinkMesh.Line"/> (control and volume blocks are
    /// concentric so a majority of the volume voters is adjacent to any electable control leader). Null for a full mesh.
    /// </summary>
    public IReadOnlyList<string>? CentralVoters(int count)
    {
        if (Mesh == LinkMesh.Full)
            return null;
        int start = Mesh == LinkMesh.Star ? 0 : (Nodes - count) / 2;
        return Enumerable.Range(start, count).Select(i => "n" + i.ToString(CultureInfo.InvariantCulture)).ToArray();
    }

    /// <summary>Volume metadata policy: explicit central voters on partial meshes, one-per-zone when zones allow, else any.</summary>
    public DfsMetadataPolicy VolumeMetadata()
    {
        IReadOnlyList<string>? central = VolumeVoterNodes ?? CentralVoters(VolumeVoters);
        if (central is not null)
            return new DfsMetadataPolicy { Voters = VolumeVoters, Placement = DfsVoterPlacement.Explicit, ExplicitVoterNodes = central };
        return new DfsMetadataPolicy
        {
            Voters = VolumeVoters,
            Placement = Zones >= VolumeVoters ? DfsVoterPlacement.OnePerZone : DfsVoterPlacement.Any,
        };
    }

    /// <summary>Control voters forced through the static bootstrap description (partial meshes only), else null.</summary>
    public IReadOnlyList<string>? ExplicitControlVoters() => Boot == MeshBoot.Static ? ControlVoterNodes ?? CentralVoters(EffectiveControlVoters) : null;

    /// <summary>Why the configuration is invalid, or null when it is valid.</summary>
    public string? Invalid()
    {
        if (Nodes is < 1 or > TestClusterOptions.MaxNodes)
            return "node count outside 1..10";
        if (VolumeVoters < 1 || VolumeVoters > Nodes)
            return "volume voters exceed the node count";
        if (Ack < 1 || Ack > Nodes)
            return "acknowledgement count exceeds the number of hosts (impossible policy)";
        if (Boot != MeshBoot.Static && Mesh is LinkMesh.Ring or LinkMesh.Line)
            return "seed join needs every joiner to reach the control leader (n0); ring/line meshes cut that link";
        if (Boot != MeshBoot.Static && Mesh == LinkMesh.Star && Boot is not (MeshBoot.SeedFull or MeshBoot.SeedStar))
            return "on a star mesh a joiner can only contact the hub, so its seeds must include n0";
        if (ControlVoterNodes is not null && (Boot != MeshBoot.Static || ControlVoterNodes.Count != EffectiveControlVoters))
            return "explicit control voters need a static bootstrap and exactly the configured count";
        if (VolumeVoterNodes is not null && VolumeVoterNodes.Count != VolumeVoters)
            return "explicit volume voters must match the configured count";
        if (Mesh == LinkMesh.Star && Nodes > 2 && VolumeVoters == 5 && EffectiveControlVoters == 3)
            return "a star with 3 control voters may elect a leaf control leader whose log reaches only the hub; 5 volume voters could not form";
        return null;
    }

    /// <summary>Builds the harness options (one volume <see cref="VolumeName"/> with <see cref="Policy"/>).</summary>
    public TestClusterOptions ToClusterOptions(ClusterMode mode, TransportKind transport = TransportKind.InMemory, StorageKind storage = StorageKind.Simulated,
        TimeSpan? scenarioTimeout = null)
    {
        string? invalid = Invalid();
        if (invalid is not null)
            throw new InvalidOperationException($"Invalid mesh configuration {Format()}: {invalid}");
        DfsStoragePolicy policy = Policy();
        IReadOnlyList<string>? controlVoters = ExplicitControlVoters();
        return new TestClusterOptions
        {
            NodeCount = Nodes,
            Topology = Topology,
            ControlVoters = ControlVoters,
            Volumes = [new TestVolumeSpec(VolumeName) { Metadata = VolumeMetadata(), DefaultPolicy = policy }],
            DefaultPolicy = policy,
            Mode = mode,
            Transport = transport,
            Storage = storage,
            Bootstrap = Bootstrap,
            Seeds = Seeds,
            Mesh = Mesh,
            Seed = Seed,
            ScenarioTimeout = scenarioTimeout ?? (mode == ClusterMode.Deterministic ? TimeSpan.FromMinutes(20) : TimeSpan.FromMinutes(5)),
            ConfigureNodeOptions = controlVoters is null
                ? null
                : (_, options) => options.Bootstrap is null ? options : options with { Bootstrap = options.Bootstrap with { ControlVoters = controlVoters } },
        };
    }

    private static readonly (MeshBoot Value, string Code)[] BootCodes =
    [
        (MeshBoot.Static, "St"), (MeshBoot.SeedFull, "SjFull"), (MeshBoot.SeedRing, "SjRing"), (MeshBoot.SeedStar, "SjStar"),
        (MeshBoot.SeedLine, "SjLine"), (MeshBoot.SeedRandom, "SjRand"),
    ];

    private static readonly (TestTopology Value, string Code)[] TopologyCodes =
        [(TestTopology.OnePerZone, "1pz"), (TestTopology.TwoPlusOne, "2+1"), (TestTopology.SingleZone, "1z"), (TestTopology.Custom, "custom")];

    private static readonly (CrashMode Value, string Code)[] CrashCodes =
        [(CrashMode.LoseUnflushed, "LU"), (CrashMode.KeepUnflushed, "KU"), (CrashMode.RandomSubset, "RS"), (CrashMode.TearLastWrite, "TL")];

    /// <summary>
    /// Encodes the configuration compactly (without the node count, which theories pass separately), for example
    /// <c>"St Full 1pz c3 v3 a2 LU WithLeader"</c>: bootstrap (St = static, Sj* = seed join over Full/Ring/Star/Line/Rand
    /// seed graph), link mesh, topology (1pz = one per zone, 2+1, 1z = single zone), control voters, volume voters,
    /// acknowledged copies, crash mode (LU/KU/RS/TL), then the optional variant and explicit voter lists.
    /// </summary>
    public string Format()
    {
        var sb = new StringBuilder();
        sb.Append(BootCodes.Single(c => c.Value == Boot).Code)
            .Append(' ').Append(Mesh)
            .Append(' ').Append(TopologyCodes.Single(c => c.Value == Topology).Code)
            .Append(" c").Append(ControlVoters.ToString(CultureInfo.InvariantCulture))
            .Append(" v").Append(VolumeVoters.ToString(CultureInfo.InvariantCulture))
            .Append(" a").Append(Ack.ToString(CultureInfo.InvariantCulture))
            .Append(' ').Append(CrashCodes.Single(c => c.Value == Crash).Code);
        if (!string.IsNullOrEmpty(Variant))
            sb.Append(' ').Append(Variant);
        if (ControlVoterNodes is not null)
            sb.Append(" cvn=").Append(string.Join(",", ControlVoterNodes));
        if (VolumeVoterNodes is not null)
            sb.Append(" vvn=").Append(string.Join(",", VolumeVoterNodes));
        return sb.ToString();
    }

    /// <summary>Decodes <see cref="Format"/> output for <paramref name="nodes"/> nodes.</summary>
    public static MeshConfig Parse(int nodes, string text)
    {
        ArgumentNullException.ThrowIfNull(text);
        string[] tokens = text.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (tokens.Length < 7)
            throw new FormatException($"Malformed mesh configuration '{text}'.");
        static int Number(string token, char prefix) =>
            token.Length > 1 && token[0] == prefix ? int.Parse(token.AsSpan(1), CultureInfo.InvariantCulture) : throw new FormatException($"Expected {prefix}<n>, got '{token}'.");
        var config = new MeshConfig
        {
            Nodes = nodes,
            Boot = BootCodes.Single(c => c.Code == tokens[0]).Value,
            Mesh = Enum.Parse<LinkMesh>(tokens[1]),
            Topology = TopologyCodes.Single(c => c.Code == tokens[2]).Value,
            ControlVoters = Number(tokens[3], 'c'),
            VolumeVoters = Number(tokens[4], 'v'),
            Ack = Number(tokens[5], 'a'),
            Crash = CrashCodes.Single(c => c.Code == tokens[6]).Value,
        };
        foreach (string token in tokens.Skip(7))
        {
            int eq = token.IndexOf('=', StringComparison.Ordinal);
            config = eq < 0 ? config with { Variant = token } : config.With(token[..eq], token[(eq + 1)..]);
        }

        return config;
    }

    /// <summary>Builds a configuration from a generated matrix row (keys: n, boot, mesh, topo, cv, vv, ack, crash, variant).</summary>
    public static MeshConfig FromRow(IReadOnlyDictionary<string, string> row)
    {
        ArgumentNullException.ThrowIfNull(row);
        var config = new MeshConfig();
        foreach (KeyValuePair<string, string> kv in row)
            config = config.With(kv.Key, kv.Value);
        return config;
    }

    /// <summary>Returns a copy with one dimension set from its encoded value.</summary>
    public MeshConfig With(string key, string value) => key switch
    {
        "n" => this with { Nodes = int.Parse(value, CultureInfo.InvariantCulture) },
        "boot" => this with { Boot = Enum.Parse<MeshBoot>(value) },
        "mesh" => this with { Mesh = Enum.Parse<LinkMesh>(value) },
        "topo" => this with { Topology = Enum.Parse<TestTopology>(value) },
        "cv" => this with { ControlVoters = int.Parse(value, CultureInfo.InvariantCulture) },
        "vv" => this with { VolumeVoters = int.Parse(value, CultureInfo.InvariantCulture) },
        "ack" => this with { Ack = int.Parse(value, CultureInfo.InvariantCulture) },
        "crash" => this with { Crash = Enum.Parse<CrashMode>(value) },
        "variant" => this with { Variant = value },
        "cvn" => this with { ControlVoterNodes = value.Split(',', StringSplitOptions.RemoveEmptyEntries) },
        "vvn" => this with { VolumeVoterNodes = value.Split(',', StringSplitOptions.RemoveEmptyEntries) },
        _ => throw new FormatException($"Unknown mesh configuration key '{key}'."),
    };

    /// <summary>The non-critical dimensions of the MESH matrix, defaults first.</summary>
    public static IReadOnlyList<MeshDimension> AllDimensions { get; } =
    [
        new("boot", Enum.GetNames<MeshBoot>()),
        new("mesh", ["Full", "Ring", "Star", "Line"]),
        new("topo", ["OnePerZone", "TwoPlusOne", "SingleZone"]),
        new("cv", ["3", "1", "5"]),
        new("vv", ["3", "1", "5"]),
        new("ack", ["2", "1", "3"]),
        new("crash", ["LoseUnflushed", "KeepUnflushed", "RandomSubset", "TearLastWrite"]),
    ];

    /// <summary>The non-critical dimensions except the named ones (those become critical or fixed in a family).</summary>
    public static IReadOnlyList<MeshDimension> DimensionsExcept(params string[] names) =>
        AllDimensions.Where(d => !names.Contains(d.Name, StringComparer.Ordinal)).ToArray();

    /// <summary>The dimension restricted to the given values (order kept as given).</summary>
    public static MeshDimension Restrict(string name, params string[] values)
    {
        MeshDimension dim = AllDimensions.Single(d => d.Name == name);
        return dim with { Values = values.Where(dim.Values.Contains).ToArray() };
    }
}
