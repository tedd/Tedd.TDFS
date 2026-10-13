using System.Globalization;
using System.Security.Cryptography.X509Certificates;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Testing;

// Bootstrap, node creation, per-node environments and disposal.
public sealed partial class TestCluster
{
    /// <summary>The runtime of the deterministic run whose scenario is executing (null outside one).</summary>
    internal static readonly AsyncLocal<ClusterRuntime?> AmbientRuntime = new();

    private const string DataDirectory = "data";
    private static long s_instances;

    private readonly List<TestCluster> _recoveries = [];
    private readonly List<ForeignNode> _foreignNodes = [];
    private HarnessAdmin? _harnessAdmin;
    private QuicCertificate? _liveCertificate;
    private QuicCertificate? _backupCertificate;
    private IReadOnlyList<byte[]> _trustedPins = [];
    private OutOfProcessLauncher? _outOfProcess;
    private Task? _disposal;
    private int _recoveryCount;
    private int _foreignCount;

    /// <summary>Harness-internal start settings.</summary>
    internal HarnessStartOptions StartOptions { get; private set; } = HarnessStartOptions.Default;

    /// <summary>True once disposal started.</summary>
    internal bool IsDisposed => Volatile.Read(ref _disposal) is not null;

    /// <summary>Keep the data root on disposal (set for failed runs so the evidence survives).</summary>
    internal bool KeepData { get; set; }

    /// <summary>QUIC pins every transport of this cluster trusts (its live and backup certificates, plus a parent's backup certificate).</summary>
    internal IReadOnlyList<byte[]> TrustedPins => _trustedPins;

    /// <summary>The out-of-process launcher (<see cref="TestClusterOptions.OutOfProcess"/> clusters).</summary>
    internal OutOfProcessLauncher OutOfProcess =>
        _outOfProcess ?? throw new InvalidOperationException("The cluster does not run out-of-process nodes.");

    /// <summary>The harness admin channel (control-leader routed administrative requests as "admin").</summary>
    internal HarnessAdmin HarnessAdmin => _harnessAdmin ?? throw new InvalidOperationException("The cluster is not started.");

    /// <summary>The certificate of live nodes (QUIC), else null.</summary>
    internal QuicCertificate? LiveCertificate => _liveCertificate;

    /// <summary>The certificate of backup nodes (QUIC), else null.</summary>
    internal QuicCertificate? BackupCertificate => _backupCertificate;

    /// <summary>Forks a deterministic random source of this cluster.</summary>
    internal IRandomSource ForkRandom(string purpose) => _random.Fork(purpose);

    /// <summary>Starts a cluster in <paramref name="runtime"/>: bootstrap, pools, volumes, backup nodes, admin client, stability.</summary>
    internal static async Task<TestCluster> StartCoreAsync(TestClusterOptions options, ClusterRuntime runtime, string testName, CancellationToken ct,
        HarnessStartOptions? start = null)
    {
        options.Validate();
        if (options.Mode != runtime.Mode)
            throw new ArgumentException($"The options ask for {options.Mode} mode but the runtime is {runtime.Mode}.", nameof(options));
        ulong seed = options.ResolveSeed(testName);
        long instance = Interlocked.Increment(ref s_instances);
        string root = Path.GetFullPath(options.ResolveDataRoot());
        string dataRoot = Path.Combine(root, SafeFolderName(testName),
            string.Create(CultureInfo.InvariantCulture, $"{seed:x16}-p{Environment.ProcessId}-{instance}"));
        var random = new SeededRandom(seed);
        var fabric = new NetworkFabric(runtime, options.Transport, random.Fork("fabric"));
        var cluster = new TestCluster(options, runtime, fabric, seed, random, dataRoot, parent: null, namePrefix: "n", backupPrefix: "b")
        {
            StartOptions = start ?? HarnessStartOptions.Default,
        };
        await cluster.StartNewClusterAsync(ct);
        return cluster;
    }

    /// <summary>Bootstraps this (new) cluster and waits until it is stable; disposes it on failure.</summary>
    private async Task StartNewClusterAsync(CancellationToken ct)
    {
        try
        {
            await BootstrapAsync(ct);
        }
        catch (Exception ex)
        {
            Record($"cluster: start failed: {ex.GetType().Name}: {ex.Message}");
            KeepData = true;
            await DisposeAsync();
            throw;
        }
    }

    private async Task BootstrapAsync(CancellationToken ct)
    {
        TestClusterOptions o = Options;
        Record(string.Create(CultureInfo.InvariantCulture,
            $"cluster {ClusterId}: start (seed 0x{Seed:x16}, {o.Mode}, {o.Transport}, {o.Storage}, {o.Bootstrap}, mesh {o.Mesh})"));
        if (o.Storage == StorageKind.Disk || o.OutOfProcess)
        {
            if (Directory.Exists(DataRoot) && Directory.EnumerateFileSystemEntries(DataRoot).Any())
                throw new InvalidOperationException($"The data root {DataRoot} already exists and is not empty.");
            Directory.CreateDirectory(DataRoot);
        }

        if (o.Transport == TransportKind.Quic)
            CreateCertificates();
        if (o.OutOfProcess)
            _outOfProcess = new OutOfProcessLauncher(this);
        _harnessAdmin = new HarnessAdmin(this, HarnessName("admin"), ClusterId, AdminCredentials, FindControlLeaderEndpointAsync);

        IReadOnlyList<TestNodeSpec> specs = ResolveNodeSpecs(o);
        var planned = new List<PlannedNode>(specs.Count);
        foreach (TestNodeSpec spec in specs)
            planned.Add(new PlannedNode(spec, NodeId.New(_random.Fork("node-id:" + spec.Name)), PlanEndpoint(spec.Name)));

        if (o.Bootstrap == BootstrapMode.Static)
        {
            ClusterBootstrap bootstrap = BuildStaticBootstrap(planned, o.EffectiveControlVoters);
            foreach (PlannedNode p in planned)
                AddLiveNode(p, bootstrap, []);
        }
        else
        {
            ClusterBootstrap single = BuildStaticBootstrap(planned.Take(1).ToList(), 1);
            IReadOnlyList<IReadOnlyList<int>> seeds = BuildSeedGraph(o.Seeds, planned.Count);
            for (int i = 0; i < planned.Count; i++)
                AddLiveNode(planned[i], i == 0 ? single : null, seeds[i].Select(j => planned[j].Endpoint).ToArray());
        }

        if (o.Mesh != LinkMesh.Full)
            Fabric.Policy.SetMesh(o.Mesh, planned.Select(p => p.Spec.Name).ToArray());

        IReadOnlyList<TestNode> nodes = Nodes;
        if (o.Bootstrap == BootstrapMode.Static)
        {
            await Task.WhenAll(nodes.Select(n => n.StartAsync(ct)));
        }
        else
        {
            foreach (TestNode node in nodes)
                await node.StartAsync(ct);
        }

        TimeSpan budget = StartOptions.StartTimeout;
        await WaitForStableCoreAsync(budget, includeVolumes: false, ct);
        Record("cluster: control group stable");

        foreach (string pool in o.GetPoolNames())
        {
            string[] members = specs.Where(s => s.IsInPool(pool)).Select(s => s.Name).Order(StringComparer.Ordinal).ToArray();
            await HarnessAdmin.CallAsync(AdminOp.CreatePool, new AdminPoolMembersBody(pool, members), ct);
            Record($"cluster: pool {pool} created ({string.Join(",", members)})");
        }

        if (o.Volumes.Count > 0 && !o.OutOfProcess && StartOptions.VolumeStateMachines is null
            && Server.Volumes.VolumeHost.ResolveDefaultFactory() is null)
        {
            // No node of this build can ever host a volume group: fail now instead of after the stability budget.
            throw new NotImplementedException(
                "Volume groups cannot be hosted: the volume state machine (VolumeHost.CreateDefaultStateMachineFactory) is implemented in a later phase.");
        }

        foreach (TestVolumeSpec volume in o.Volumes)
            await CreateVolumeAsync(volume, ct);

        for (int i = 0; i < o.BackupNodeCount; i++)
            await AddBackupNodeAsync(null, ct);

        if (StartOptions.ConnectAdminClient)
            await ConnectAdminClientAsync(ct);

        await WaitForStableCoreAsync(budget, includeVolumes: true, ct);
        Record("cluster: stable");
    }

    /// <summary>Creates one configured volume through the admin path.</summary>
    private async Task CreateVolumeAsync(TestVolumeSpec volume, CancellationToken ct)
    {
        DfsStoragePolicy clusterDefault = Options.DefaultPolicy ?? Options.GetEffectiveDefaultPolicy(volume.StoragePool);
        DfsVolumeSpec spec = volume.ToVolumeSpec(clusterDefault);
        var body = new AdminVolumeSpecBody(spec.Name, spec.Metadata, spec.DefaultPolicy, spec.NamedPolicies, spec.InlineThreshold, spec.Limits)
        {
            StoragePool = spec.StoragePool,
            RootSecurity = spec.RootSecurity,
            EnforceTraverseChecking = spec.EnforceTraverseChecking,
        };
        AdminVolumeInfoBody info = await HarnessAdmin.CallAsync<AdminVolumeSpecBody, AdminVolumeInfoBody>(AdminOp.CreateVolume, body, ct);
        Record($"cluster: volume {volume.Name} created (id {info.Volume.Id}, pool {spec.StoragePool})");
    }

    /// <summary>The node specifications of this cluster (recovery clusters rename the default <c>n{i}</c> nodes).</summary>
    private IReadOnlyList<TestNodeSpec> ResolveNodeSpecs(TestClusterOptions options)
    {
        IReadOnlyList<TestNodeSpec> specs = options.GetNodeSpecs();
        if (options.Nodes is null && !string.Equals(_namePrefix, "n", StringComparison.Ordinal))
            specs = specs.Select((s, i) => s with { Name = _namePrefix + i.ToString(CultureInfo.InvariantCulture) }).ToArray();
        foreach (TestNodeSpec spec in specs)
            RequireFreeName(spec.Name);
        return specs;
    }

    /// <summary>Throws when <paramref name="name"/> is already an identity on the fabric (any cluster).</summary>
    private void RequireFreeName(string name)
    {
        if (FindNode(name) is not null || FindBackup(name) is not null || Fabric.IsKnownName(name)
            || Fabric.EndpointOfName(name) is not null)
            throw new ArgumentException($"The name '{name}' is already used by a node, backup node or client on this cluster's network.");
    }

    /// <summary>A network identity for harness-owned channels of this cluster.</summary>
    private string HarnessName(string purpose) =>
        _parent is null ? "harness:" + purpose : "harness:" + _namePrefix + "-" + purpose;

    /// <summary>The listen endpoint of a node (fixed for its lifetime, so restarts and static bootstrap agree).</summary>
    private string PlanEndpoint(string name) => Options.Transport switch
    {
        TransportKind.Quic => "quic://127.0.0.1:" + PortAllocator.Allocate().ToString(CultureInfo.InvariantCulture),
        _ => RpcAddress.Memory(name).ToString(),
    };

    /// <summary>
    /// The static bootstrap description: every planned member, the control voters (one per zone first, then in node order,
    /// like the server's own voter promotion) and the "admin" principal.
    /// </summary>
    private ClusterBootstrap BuildStaticBootstrap(IReadOnlyList<PlannedNode> planned, int voters)
    {
        var chosen = new List<string>();
        var zones = new HashSet<string>(StringComparer.Ordinal);
        foreach (PlannedNode p in planned)
        {
            if (chosen.Count < voters && zones.Add(p.Spec.Region + "\0" + p.Spec.Zone))
                chosen.Add(p.Spec.Name);
        }

        foreach (PlannedNode p in planned)
        {
            if (chosen.Count < voters && !chosen.Contains(p.Spec.Name, StringComparer.Ordinal))
                chosen.Add(p.Spec.Name);
        }

        string[] ordered = planned.Select(p => p.Spec.Name).Where(n => chosen.Contains(n, StringComparer.Ordinal)).ToArray();
        return new ClusterBootstrap(ClusterId, ClusterName,
            planned.Select(p => new BootstrapMember(p.Id, p.Spec.Name, p.Spec.ToLabels(), [p.Endpoint])).ToArray(),
            ordered, [new BootstrapPrincipal(AdminCredentials.Principal, AdminCredentials.Secret, ClusterAdmin: true)]);
    }

    /// <summary>For each node (by index), the indexes of the nodes it knows at startup in seed-join mode.</summary>
    private IReadOnlyList<IReadOnlyList<int>> BuildSeedGraph(SeedGraph graph, int count)
    {
        var result = new List<IReadOnlyList<int>>(count);
        IRandomSource random = _random.Fork("seed-graph");
        for (int i = 0; i < count; i++)
        {
            int[] seeds = graph switch
            {
                SeedGraph.Full => Enumerable.Range(0, count).Where(j => j != i).ToArray(),
                SeedGraph.Ring => count > 1 ? [(i - 1 + count) % count] : [],
                SeedGraph.Star => i == 0 ? (count > 1 ? Enumerable.Range(1, count - 1).ToArray() : []) : [0],
                SeedGraph.Line => i == 0 ? [] : [i - 1],
                SeedGraph.RandomConnected => i == 0 ? [] : RandomEarlier(random, i),
                _ => throw new ArgumentOutOfRangeException(nameof(graph), graph, null),
            };
            result.Add(seeds);
        }

        return result;

        static int[] RandomEarlier(IRandomSource random, int i)
        {
            // A random non-empty subset of the already started nodes keeps the graph connected; the first entry is a
            // uniformly chosen earlier node, so every shape of spanning tree occurs.
            var picked = new List<int> { random.Next(i) };
            int extra = random.Next(Math.Min(3, i));
            for (int k = 0; k < extra; k++)
            {
                int j = random.Next(i);
                if (!picked.Contains(j))
                    picked.Add(j);
            }

            return [.. picked];
        }
    }

    /// <summary>Creates a live node object (not started) and adds it to <see cref="Nodes"/>.</summary>
    private TestNode AddLiveNode(PlannedNode planned, ClusterBootstrap? bootstrap, IReadOnlyList<string> seeds,
        Func<NodeOptions, NodeOptions>? configure = null)
    {
        TestNodeSpec spec = planned.Spec;
        TestClock clock = Runtime.CreateProcessClock(spec.Name);
        TestDisk disk = CreateDisk(spec.Name, clock);
        IRpcTransportFactory factory = Fabric.CreateFactory(new TransportIdentity
        {
            Owner = this,
            Clock = clock,
            ClusterId = ClusterId,
            Certificate = _liveCertificate?.Certificate,
            TrustedPins = _trustedPins,
        });
        var options = new NodeOptions
        {
            Name = spec.Name,
            Labels = spec.ToLabels(),
            ListenEndpoint = planned.Endpoint,
            DataDirectory = DataDirectory,
            ClusterSecret = ClusterSecret,
            Bootstrap = bootstrap,
            JoinSeeds = seeds,
            Timings = Options.Timings,
            CapacityBytes = Options.NodeCapacityBytes,
            ReservedBytes = Options.ReservedBytes,
            Mutations = Options.Mutations,
            EncryptAtRest = Options.EncryptAtRest,
        };
        if (Options.ConfigureNodeOptions is { } transform)
            options = transform(spec.Name, options) ?? throw new InvalidOperationException($"ConfigureNodeOptions returned null for node {spec.Name}.");
        Options.ConfigureNode?.Invoke(spec.Name, options);
        if (configure is not null)
            options = configure(options) ?? throw new InvalidOperationException($"The node configuration returned null for node {spec.Name}.");

        TestNode node;
        lock (_gate)
        {
            node = new TestNode(this, spec, _nodes.Count, planned.Id, options, clock, disk, factory);
            _nodes.Add(node);
        }

        Fabric.RegisterEndpoint(options.ListenEndpoint, spec.Name);
        Record($"node {spec.Name}: created ({spec.ToLabels()}, {options.ListenEndpoint})");
        return node;
    }

    /// <summary>The disk of a node: a simulated medium or a faulting real directory under the data root.</summary>
    private TestDisk CreateDisk(string name, TimeProvider clock)
    {
        long capacity = Options.NodeCapacityBytes;
        if (Options.Storage == StorageKind.Simulated)
        {
            return new TestDisk(name, generation => new SimulatedMedium(name, capacity,
                _random.Fork(string.Create(CultureInfo.InvariantCulture, $"disk:{name}:{generation}")), clock));
        }

        return new TestDisk(name, generation => new FaultingFileSystemMedium(Path.Combine(DataRoot, name), Path.Combine(DataRoot, ".trash", name),
            capacity, _random.Fork(string.Create(CultureInfo.InvariantCulture, $"disk:{name}:{generation}")), clock));
    }

    /// <summary>Creates the per-cluster development certificates and the trusted pin list (QUIC).</summary>
    private void CreateCertificates()
    {
        _liveCertificate = QuicCertificate.Create("tdfs-live-" + ClusterId.ToString("N")[..8]);
        _backupCertificate = QuicCertificate.Create("tdfs-backup-" + ClusterId.ToString("N")[..8]);
        var pins = new List<byte[]> { _liveCertificate.Pin, _backupCertificate.Pin };
        if (_parent?._backupCertificate is { } parentBackup)
            pins.Add(parentBackup.Pin); // a recovery cluster trusts the backup hosts it restores from, nothing else of the source
        _trustedPins = pins;
    }

    // -------------------------------------------------------------------------------------------- environments

    /// <summary>The environment of one start of a live node.</summary>
    internal NodeEnvironment CreateNodeEnvironment(TestNode node, IStorageMedium medium, int start)
    {
        INodeLog log = NodeLog;
        return new NodeEnvironment
        {
            Medium = medium,
            TransportFactory = node.TransportFactory ?? throw new InvalidOperationException($"Node {node.Name} has no transport factory."),
            Time = node.Clock,
            Random = _random.Fork(string.Create(CultureInfo.InvariantCulture, $"node:{node.Name}/start:{start}")),
            Faults = Faults.Injector,
            Log = log,
            LoopFactory = Runtime.CreateLoopFactory(log),
            VolumeStateMachines = StartOptions.VolumeStateMachines,
        };
    }

    /// <summary>The environment of one start of a backup node.</summary>
    internal NodeEnvironment CreateBackupEnvironment(TestBackupNode node, IStorageMedium medium, int start)
    {
        INodeLog log = NodeLog;
        return new NodeEnvironment
        {
            Medium = medium,
            TransportFactory = node.TransportFactory ?? throw new InvalidOperationException($"Backup node {node.Name} has no transport factory."),
            Time = node.Clock,
            Random = _random.Fork(string.Create(CultureInfo.InvariantCulture, $"backup:{node.Name}/start:{start}")),
            Faults = Faults.Injector,
            Log = log,
            LoopFactory = Runtime.CreateLoopFactory(log),
        };
    }

    /// <summary>Node log sink: warnings and errors of every node are kept (bounded) for failure reports.</summary>
    internal HarnessNodeLog NodeLog { get; } = new();

    // -------------------------------------------------------------------------------------------- adding nodes

    /// <summary>Adds a live node (joining the cluster when <paramref name="join"/> is true).</summary>
    /// <remarks>
    /// With <paramref name="join"/> = true the node starts with every current live node as a join seed and the task completes
    /// once it was admitted and registered its incarnation; a refused join throws. With <paramref name="join"/> = false the
    /// node is only created (<see cref="TestNodeState.Stopped"/>, with the same seeds configured): it joins when the test
    /// starts it with <see cref="TestNode.RestartAsync"/>.
    /// </remarks>
    /// <exception cref="ArgumentException">The name is taken or the spec is invalid.</exception>
    public async Task<TestNode> AddNodeAsync(TestNodeSpec spec, bool join = true, CancellationToken ct = default)
    {
        TestNode node = CreateJoiningNode(spec, configure: null);
        if (!join)
            return node;
        await node.StartAsync(ct);
        if (node.State == TestNodeState.Running)
            await WaitForRegistrationAsync(node, RegistrationTimeout);
        return node;
    }

    /// <summary>
    /// Adds a live node whose <see cref="NodeOptions"/> are transformed by <paramref name="configure"/> before the node
    /// starts, for peer-identity tests (SEC-001). Never throws for a rejected join; see the definition in <c>Additions/rst-sec</c>.
    /// </summary>
    private async Task<TestNode> AddConfiguredNodeAsync(TestNodeSpec spec, Func<NodeOptions, NodeOptions> configure, bool join, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(configure);
        TestNode node = CreateJoiningNode(spec, configure, join);
        try
        {
            await node.StartAsync(ct);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
        {
            Record($"node {node.Name}: join rejected or failed: {ex.GetType().Name} ({DfsErrors.ToCode(ex)}): {ex.Message}");
            return node;
        }

        if (node.State == TestNodeState.Running)
        {
            TimeSpan joinTimeout = Max(TimeSpan.FromSeconds(30), Options.Timings.Tick * Options.Timings.ElectionTicksMax * 100);
            if (!await TryWaitForRegistrationAsync(node, joinTimeout))
                Record($"node {node.Name}: not admitted within {joinTimeout}");
        }

        return node;
    }

    private TestNode CreateJoiningNode(TestNodeSpec spec, Func<NodeOptions, NodeOptions>? configure, bool withSeeds = true)
    {
        ArgumentNullException.ThrowIfNull(spec);
        ObjectDisposedException.ThrowIf(IsDisposed, this);
        if (string.IsNullOrWhiteSpace(spec.Name) || string.IsNullOrWhiteSpace(spec.Zone))
            throw new ArgumentException("A node needs a name and a zone.", nameof(spec));
        if (Nodes.Count >= TestClusterOptions.MaxNodes)
            throw new ArgumentException($"A test cluster has at most {TestClusterOptions.MaxNodes} live nodes.", nameof(spec));
        foreach (string pool in spec.EffectivePools)
        {
            if (!DfsPoolNames.IsValid(pool) || string.Equals(pool, DfsPoolNames.Default, StringComparison.Ordinal))
                throw new ArgumentException($"Node '{spec.Name}' lists invalid pool name '{pool}'.", nameof(spec));
        }

        RequireFreeName(spec.Name);
        string[] seeds = withSeeds ? Nodes.Select(n => n.Endpoint).ToArray() : [];
        var planned = new PlannedNode(spec, NodeId.New(_random.Fork("node-id:" + spec.Name)), PlanEndpoint(spec.Name));
        return AddLiveNode(planned, null, seeds, configure);
    }

    /// <summary>Adds a backup node.</summary>
    public async Task<TestBackupNode> AddBackupNodeAsync(string? name = null, CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(IsDisposed, this);
        TestBackupNode node;
        lock (_gate)
        {
            name ??= _backupPrefix + _backups.Count.ToString(CultureInfo.InvariantCulture);
        }

        RequireFreeName(name);
        TestClock clock = Runtime.CreateProcessClock(name);
        TestDisk disk = CreateDisk(name, clock);
        IRpcTransportFactory factory = Fabric.CreateFactory(new TransportIdentity
        {
            Owner = this,
            Clock = clock,
            ClusterId = ClusterId,
            Certificate = _backupCertificate?.Certificate,
            TrustedPins = _trustedPins,
        });
        IRandomSource random = _random.Fork("backup-node:" + name);
        var export = new DfsCredentials(name + "-export", NewSecret(random.Fork("export")));
        var reader = new DfsCredentials(name + "-reader", NewSecret(random.Fork("reader")));
        var retention = new DfsCredentials(name + "-retention", NewSecret(random.Fork("retention")));
        var options = new BackupNodeOptions
        {
            Name = name,
            ListenEndpoint = PlanEndpoint(name),
            DataDirectory = DataDirectory,
            SourceClusterId = ClusterId,
            BackupSecret = NextBytes(random.Fork("backup-secret"), 32),
            Principals =
            [
                new BootstrapPrincipal(export.Principal, export.Secret, ClusterAdmin: false),
                new BootstrapPrincipal(reader.Principal, reader.Secret, ClusterAdmin: false),
                new BootstrapPrincipal(retention.Principal, retention.Secret, ClusterAdmin: false),
            ],
            Grants = new Dictionary<string, DfsPermissions>(StringComparer.Ordinal)
            {
                [export.Principal] = DfsPermissions.BackupExport,
                [reader.Principal] = DfsPermissions.Read,
                [retention.Principal] = DfsPermissions.Read | DfsPermissions.RetentionAdmin | DfsPermissions.BackupAdmin,
            },
            CapacityBytes = Options.NodeCapacityBytes,
            Timings = Options.Timings,
        };

        lock (_gate)
        {
            node = new TestBackupNode(this, name, _backups.Count, options, clock, disk, factory, export, reader, retention);
            _backups.Add(node);
        }

        lock (_credentials)
        {
            _credentials[export.Principal] = export;
            _credentials[reader.Principal] = reader;
            _credentials[retention.Principal] = retention;
        }

        Fabric.RegisterEndpoint(options.ListenEndpoint, name);
        Record($"backup {name}: created ({options.ListenEndpoint})");
        await node.StartAsync(ct);
        return node;
    }

    // -------------------------------------------------------------------------------------------- recovery and foreign clusters

    /// <summary>Starts an independent recovery cluster on this cluster's fabric (see the definition in <c>Additions/rst-sec</c>).</summary>
    private async Task<TestCluster> StartRecoveryClusterCoreAsync(TestClusterOptions options, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(options);
        ObjectDisposedException.ThrowIf(IsDisposed, this);
        options.Validate();
        if (options.Mode != Mode)
            throw new ArgumentException($"A recovery cluster must run in this cluster's mode ({Mode}).", nameof(options));
        if (options.Transport != Options.Transport)
            throw new ArgumentException($"A recovery cluster must use this cluster's transport ({Options.Transport}).", nameof(options));
        if (!options.OutOfProcess && options.Storage != Options.Storage)
            throw new ArgumentException($"An in-process recovery cluster must use this cluster's storage ({Options.Storage}).", nameof(options));

        int n = Interlocked.Increment(ref _recoveryCount);
        string tag = n == 1 ? "r" : "r" + n.ToString(CultureInfo.InvariantCulture) + "-";
        string dataRoot = Path.Combine(DataRoot, "recovery-" + n.ToString(CultureInfo.InvariantCulture));
        if (Directory.Exists(dataRoot))
            throw new InvalidOperationException($"The recovery data root {dataRoot} already exists.");
        ulong seed = SeededRandom.DeriveSeed(Seed, "recovery/" + n.ToString(CultureInfo.InvariantCulture));
        if (seed == 0)
            seed = 1;
        var recovery = new TestCluster(options with { DataRoot = dataRoot }, Runtime, Fabric, seed, new SeededRandom(seed), dataRoot, this, tag, tag + "b")
        {
            StartOptions = StartOptions,
        };
        lock (_gate)
            _recoveries.Add(recovery);
        Record($"cluster: starting recovery cluster {recovery.ClusterId} ({tag}*)");
        await recovery.StartNewClusterAsync(ct);
        return recovery;
    }

    /// <summary>Starts a single-node foreign cluster on <paramref name="listenEndpoint"/> (see the definition in <c>Additions/con-net</c>).</summary>
    private async Task<NodeHost> StartForeignClusterNodeCoreAsync(string listenEndpoint, CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(listenEndpoint);
        ObjectDisposedException.ThrowIf(IsDisposed, this);
        if (Options.OutOfProcess)
            throw new NotSupportedException("Foreign cluster nodes run in process; this cluster runs out-of-process nodes.");
        string endpoint = NetworkFabric.NormalizeEndpoint(listenEndpoint);
        TestNode? displaced = Nodes.FirstOrDefault(node => string.Equals(NetworkFabric.NormalizeEndpoint(node.Endpoint), endpoint, StringComparison.OrdinalIgnoreCase));
        if (displaced is { State: TestNodeState.Running or TestNodeState.Paused })
            throw new InvalidOperationException($"Node {displaced.Name} of this cluster still listens on {listenEndpoint}.");
        string name = Fabric.NameOfEndpoint(endpoint) ?? displaced?.Name ?? "foreign-" + Interlocked.Increment(ref _foreignCount).ToString(CultureInfo.InvariantCulture);
        int number = Interlocked.Increment(ref _foreignCount);
        IRandomSource random = _random.Fork("foreign/" + number.ToString(CultureInfo.InvariantCulture));
        Guid clusterId = NewGuid(random.Fork("cluster-id"));
        TestClock clock = Runtime.CreateProcessClock(name);
        string diskName = name + "-foreign" + number.ToString(CultureInfo.InvariantCulture);
        IStorageMedium medium = Options.Storage == StorageKind.Simulated
            ? new SimulatedMedium(diskName, Options.NodeCapacityBytes, random.Fork("disk"), clock)
            : new FaultingFileSystemMedium(Path.Combine(DataRoot, diskName), Path.Combine(DataRoot, ".trash", diskName), Options.NodeCapacityBytes,
                random.Fork("disk"), clock);
        IRpcTransportFactory factory = Fabric.CreateFactory(new TransportIdentity
        {
            Owner = this,
            Clock = clock,
            ClusterId = clusterId,
            Certificate = _liveCertificate?.Certificate,
            TrustedPins = _trustedPins,
        });
        TestNodeSpec spec = displaced?.Spec ?? new TestNodeSpec(name, "z1");
        DfsCredentials[] principals;
        lock (_credentials)
            principals = [.. _credentials.Values.OrderBy(c => c.Principal, StringComparer.Ordinal)];
        NodeId id = NodeId.New(random.Fork("node-id"));
        var bootstrap = new ClusterBootstrap(clusterId, "tdfs-foreign", [new BootstrapMember(id, name, spec.ToLabels(), [listenEndpoint])], [name],
            principals.Select(p => new BootstrapPrincipal(p.Principal, p.Secret, string.Equals(p.Principal, AdminCredentials.Principal, StringComparison.Ordinal)))
                .ToArray());
        var options = new NodeOptions
        {
            Name = name,
            Labels = spec.ToLabels(),
            ListenEndpoint = listenEndpoint,
            DataDirectory = DataDirectory,
            ClusterSecret = NextBytes(random.Fork("secret"), 32),
            Bootstrap = bootstrap,
            Timings = Options.Timings,
            CapacityBytes = Options.NodeCapacityBytes,
            ReservedBytes = Options.ReservedBytes,
            EncryptAtRest = Options.EncryptAtRest,
        };
        INodeLog log = NodeLog;
        var environment = new NodeEnvironment
        {
            Medium = medium,
            TransportFactory = factory,
            Time = clock,
            Random = random.Fork("node"),
            Faults = Faults.Injector,
            Log = log,
            LoopFactory = Runtime.CreateLoopFactory(log),
            VolumeStateMachines = StartOptions.VolumeStateMachines,
        };

        Record($"foreign node {name}: start on {listenEndpoint} (cluster {clusterId})");
        await PortAllocator.WaitUntilFreeAsync(listenEndpoint, TimeSpan.FromSeconds(10), Runtime); // the displaced node's listener
        NodeHost host = await NodeHost.StartAsync(options, environment, ct);
        var foreign = new ForeignNode(name, host, clusterId);
        lock (_gate)
            _foreignNodes.Add(foreign);

        // The foreign cluster gets a volume named like this cluster's first volume, created through its own admin path.
        if (Options.Volumes.Count > 0)
        {
            TestVolumeSpec first = Options.Volumes[0];
            await using var admin = new HarnessAdmin(this, HarnessName("foreign-admin-" + number.ToString(CultureInfo.InvariantCulture)), clusterId,
                AdminCredentials, _ => Task.FromResult<string?>(host.Endpoint));
            DfsStoragePolicy single = new() { TargetCopies = 1, TargetDistinctZones = 1, DurableCopiesBeforeAck = 1, DurableZonesBeforeAck = 1 };
            var body = new AdminVolumeSpecBody(first.Name, new DfsMetadataPolicy { Voters = 1 }, single, new Dictionary<string, DfsStoragePolicy>(),
                first.InlineThreshold, first.Limits ?? new DfsVolumeLimits());
            await WaitUntilCoreAsync(async () => await IsLeaderOfAsync(host, Protocol.ProtocolInfo.ControlGroup), StartOptions.StartTimeout,
                $"the foreign node {name} to lead its control group", ct);
            await admin.CallAsync(AdminOp.CreateVolume, body, ct);
            await WaitUntilCoreAsync(async () => await IsLeaderOfAsync(host, first.Name), StartOptions.StartTimeout,
                $"the foreign node {name} to lead its volume {first.Name}", ct);
        }

        Record($"foreign node {name}: running (cluster {clusterId})");
        return host;
    }

    private static async Task<bool> IsLeaderOfAsync(NodeHost host, string group)
    {
        GroupInspection? inspection = await host.Inspect.GetGroupAsync(group);
        return inspection is { Role: Consensus.RaftRole.Leader };
    }

    // -------------------------------------------------------------------------------------------- disposal

    /// <summary>Stops all nodes and releases resources.</summary>
    /// <remarks>
    /// Recovery clusters started by this cluster are disposed first. Blocked and paused fault points are released, every node
    /// process is terminated (the end-of-run invariants were checked before), clients still connected are abandoned, the
    /// transports and certificates are released, and the data root is deleted unless the run failed (<see cref="KeepData"/>).
    /// </remarks>
    public ValueTask DisposeAsync()
    {
        Task disposal;
        lock (_gate)
        {
            disposal = _disposal ??= DisposeCoreAsync();
        }

        return new ValueTask(disposal);
    }

    private async Task DisposeCoreAsync()
    {
        Record("cluster: dispose");
        TestCluster[] recoveries;
        ForeignNode[] foreign;
        lock (_gate)
        {
            recoveries = [.. _recoveries];
            foreign = [.. _foreignNodes];
        }

        foreach (TestCluster recovery in recoveries)
            await recovery.DisposeAsync();

        Faults.ReleaseAll();
        Faults.DisarmAll();
        if (_parent is null)
        {
            Fabric.CancelReadLimits();
            Fabric.Policy.Heal();
        }

        await DisposeClientsAsync();
        if (_harnessAdmin is { } admin)
            await admin.DisposeAsync();

        foreach (ForeignNode f in foreign)
            f.Host.Kill();
        foreach (TestNode node in Nodes)
            node.Terminate();
        foreach (TestBackupNode node in BackupNodes)
            node.Terminate();
        if (_outOfProcess is { } launcher)
            await launcher.DisposeAsync();
        await Fabric.DisposeTransportsAsync(this);

        _liveCertificate?.Certificate.Dispose();
        _backupCertificate?.Certificate.Dispose();

        if (!KeepData && Directory.Exists(DataRoot))
            DeleteDataRoot();
    }

    private void DeleteDataRoot()
    {
        for (int attempt = 0; attempt < 5; attempt++)
        {
            try
            {
                Directory.Delete(DataRoot, recursive: true);
                DeleteEmptyTestFolder();
                return;
            }
            catch (IOException) when (attempt < 4)
            {
                // Handles of a just-terminated node may still be closing (Windows); try again shortly.
                Thread.Sleep(50 * (attempt + 1));
            }
            catch (UnauthorizedAccessException) when (attempt < 4)
            {
                Thread.Sleep(50 * (attempt + 1));
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                Record($"cluster: data root {DataRoot} could not be deleted: {ex.Message}");
                return;
            }
        }
    }

    /// <summary>Removes the per-test folder above the data root when no other run of the test left anything in it.</summary>
    private void DeleteEmptyTestFolder()
    {
        if (_parent is not null || Path.GetDirectoryName(DataRoot) is not { } folder)
            return;
        try
        {
            if (Directory.Exists(folder) && !Directory.EnumerateFileSystemEntries(folder).Any())
                Directory.Delete(folder);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            // A concurrent run of the same test created its data root meanwhile.
        }
    }

    // -------------------------------------------------------------------------------------------- types

    /// <summary>A node about to be created: specification, planned id and fixed endpoint.</summary>
    private sealed record PlannedNode(TestNodeSpec Spec, NodeId Id, string Endpoint);

    /// <summary>A foreign single-node cluster started on this cluster's network.</summary>
    private sealed record ForeignNode(string Name, NodeHost Host, Guid ClusterId);

    /// <summary>A development certificate (loaded, and as PKCS#12 bytes for host processes) and its SPKI pin (QUIC).</summary>
    internal sealed record QuicCertificate(X509Certificate2 Certificate, byte[] Pin, byte[] Pfx)
    {
        public static QuicCertificate Create(string subject)
        {
            (byte[] pfx, byte[] pin) = Host.DevelopmentCertificates.CreatePfx("CN=" + subject, TimeSpan.FromDays(7));
            return new QuicCertificate(Host.DevelopmentCertificates.Load(pfx, null), pin, pfx);
        }
    }
}
