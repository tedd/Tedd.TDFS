using System.Globalization;
using Tedd.Tdfs.Testing.Simulation;

namespace Tedd.Tdfs.Testing;

/// <summary>A running test cluster: live nodes, backup nodes, network/fault control, inspection and history.</summary>
/// <remarks>
/// <para>The cluster owns one execution context (real threads, or one deterministic <see cref="SimScheduler"/>), one
/// transport fabric (shared with the recovery clusters it starts), one fault injector, and one seeded random root from
/// which every identity, secret and per-process random source is forked, so a deterministic run replays exactly from
/// <see cref="Seed"/>.</para>
/// <para>Partials: <c>TestCluster.Lifecycle.cs</c> (bootstrap, nodes, environments, disposal), <c>TestCluster.Clients.cs</c>
/// (SDK, raw and admin clients, principals), <c>TestCluster.Waits.cs</c> (leaders, stability, replication, repair, digest)
/// and <c>TestCluster.Invariants.cs</c> (leadership history, assumption violations, uncontrolled dependencies).</para>
/// </remarks>
public sealed partial class TestCluster : IAsyncDisposable
{
    private readonly object _gate = new();
    private readonly List<TestNode> _nodes = [];
    private readonly List<TestBackupNode> _backups = [];
    private readonly List<string> _trace = [];
    private readonly SeededRandom _random;
    private readonly TestNetwork _network;
    private readonly TestFaults _faults;
    private readonly ClusterInspector _inspector;
    private readonly HistoryRecorder _history = new();
    private readonly TestCluster? _parent;
    private readonly string _namePrefix;
    private readonly string _backupPrefix;

    private TestCluster(TestClusterOptions options, ClusterRuntime runtime, NetworkFabric fabric, ulong seed, SeededRandom random, string dataRoot,
        TestCluster? parent, string namePrefix, string backupPrefix)
    {
        Options = options;
        Runtime = runtime;
        Fabric = fabric;
        Seed = seed;
        _random = random;
        DataRoot = dataRoot;
        _parent = parent;
        _namePrefix = namePrefix;
        _backupPrefix = backupPrefix;
        ClusterId = NewGuid(random.Fork("cluster/id"));
        ClusterSecret = NextBytes(random.Fork("cluster/secret"), 32);
        ClusterName = parent is null ? "tdfs-test" : "tdfs-recovery";
        AdminCredentials = new DfsCredentials("admin", NewSecret(random.Fork("principal/admin")));
        _credentials["admin"] = AdminCredentials;

        Action<string> trace = Record;
        _network = new TestNetwork(fabric, trace);
        _faults = new TestFaults(runtime, trace) { Targets = new TargetDirectory(this) };
        _faults.ReadLimitRegistrar = fabric.AddReadLimit;
        _inspector = new ClusterInspector(this);
        fabric.AttachTrace(trace);
        fabric.AttachVolumeNames(VolumeNameOf);
    }

    /// <summary>Starts a cluster (prefer <see cref="ClusterTest.RunAsync(TestClusterOptions, Func{TestCluster, Task}, CancellationToken)"/>).</summary>
    /// <remarks>
    /// Realtime clusters may be started anywhere. A <see cref="ClusterMode.Deterministic"/> cluster runs inside a simulation:
    /// started from a scenario of a deterministic <see cref="ClusterTest"/> run it joins that run's scheduler (with its own
    /// fabric, clocks and identities); started anywhere else it is refused with <see cref="NotSupportedException"/>, because
    /// nothing would drive its scheduler. The caller owns the returned cluster and must dispose it.
    /// </remarks>
    /// <exception cref="ArgumentException">The options are invalid.</exception>
    /// <exception cref="NotSupportedException">A deterministic cluster outside a deterministic run.</exception>
    public static Task<TestCluster> StartAsync(TestClusterOptions options, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        ClusterRuntime runtime;
        if (options.Mode == ClusterMode.Deterministic)
        {
            runtime = AmbientRuntime.Value ?? throw new NotSupportedException(
                "A deterministic cluster runs inside a simulation: start it with ClusterTest.RunAsync (or from a deterministic scenario).");
        }
        else
        {
            runtime = ClusterRuntime.CreateRealtime();
        }

        return StartCoreAsync(options, runtime, CurrentTestName(), ct);
    }

    /// <summary>The options the cluster was started with.</summary>
    public TestClusterOptions Options { get; }

    /// <summary>Execution mode.</summary>
    public ClusterMode Mode => Runtime.Mode;

    /// <summary>The resolved master seed (reported on failure).</summary>
    public ulong Seed { get; }

    /// <summary>Cluster id.</summary>
    public Guid ClusterId { get; }

    /// <summary>Per-test data root.</summary>
    public string DataRoot { get; }

    /// <summary>Live nodes in creation order (n0, n1, ...).</summary>
    public IReadOnlyList<TestNode> Nodes
    {
        get
        {
            lock (_gate)
                return _nodes.ToArray();
        }
    }

    /// <summary>Backup nodes (b0, b1, ...).</summary>
    public IReadOnlyList<TestBackupNode> BackupNodes
    {
        get
        {
            lock (_gate)
                return _backups.ToArray();
        }
    }

    /// <summary>Live node by index.</summary>
    public TestNode this[int index]
    {
        get
        {
            lock (_gate)
            {
                if ((uint)index >= (uint)_nodes.Count)
                    throw new ArgumentOutOfRangeException(nameof(index), index, $"The cluster has {_nodes.Count} live nodes.");
                return _nodes[index];
            }
        }
    }

    /// <summary>Network control.</summary>
    public TestNetwork Network => _network;

    /// <summary>Fault injection control.</summary>
    public TestFaults Faults => _faults;

    /// <summary>Cluster-wide inspection.</summary>
    public ClusterInspector Inspect => _inspector;

    /// <summary>Operation history of this scenario.</summary>
    public HistoryRecorder History => _history;

    /// <summary>Harness/test clock (virtual in deterministic mode).</summary>
    public TimeProvider Time => Runtime.Time;

    /// <summary>Admin client connected as "admin".</summary>
    /// <exception cref="InvalidOperationException">The admin client could not be connected when the cluster started.</exception>
    public DfsAdminClient Admin => AdminClientForHarness.Admin;

    /// <summary>Credentials of principal "admin" (ClusterAdmin, all grants).</summary>
    public DfsCredentials AdminCredentials { get; }

    /// <summary>Fault/network/node events (replay artifact).</summary>
    /// <remarks>Each line is prefixed with the harness time (virtual in deterministic mode) at which the event happened.</remarks>
    public IReadOnlyList<string> Trace
    {
        get
        {
            lock (_trace)
                return _trace.ToArray();
        }
    }

    /// <summary>Live node by name.</summary>
    /// <exception cref="ArgumentException">No live node has that name.</exception>
    public TestNode Node(string name)
    {
        ArgumentNullException.ThrowIfNull(name);
        return FindNode(name) ?? throw new ArgumentException($"The cluster has no live node named '{name}'.", nameof(name));
    }

    /// <inheritdoc />
    public override string ToString() =>
        string.Create(CultureInfo.InvariantCulture, $"TestCluster({Mode}, seed 0x{Seed:x16}, {Nodes.Count} nodes, cluster {ClusterId})");

    // -------------------------------------------------------------------------------------------- harness internals

    /// <summary>The execution context.</summary>
    internal ClusterRuntime Runtime { get; }

    /// <summary>The transport fabric (shared with recovery clusters).</summary>
    internal NetworkFabric Fabric { get; }

    /// <summary>The cluster secret (root of node authentication and storage keys).</summary>
    internal ReadOnlyMemory<byte> ClusterSecret { get; }

    /// <summary>The cluster name used at bootstrap.</summary>
    internal string ClusterName { get; }

    /// <summary>Bound on a graceful node stop before the harness kills the process (harness time).</summary>
    internal TimeSpan StopTimeout => TimeSpan.FromSeconds(30);

    /// <summary>Bound on a (re)started node registering its incarnation (harness time).</summary>
    internal TimeSpan RegistrationTimeout =>
        Max(TimeSpan.FromSeconds(30), Options.Timings.Tick * Options.Timings.ElectionTicksMax * 50);

    /// <summary>Records one event in <see cref="Trace"/>.</summary>
    internal void Record(string message)
    {
        ArgumentNullException.ThrowIfNull(message);
        string line = Runtime.FormatNow() + " " + message;
        lock (_trace)
            _trace.Add(line);
    }

    /// <summary>The live node named <paramref name="name"/>, or null.</summary>
    internal TestNode? FindNode(string name)
    {
        lock (_gate)
            return _nodes.FirstOrDefault(n => string.Equals(n.Name, name, StringComparison.Ordinal));
    }

    /// <summary>The backup node named <paramref name="name"/>, or null.</summary>
    internal TestBackupNode? FindBackup(string name)
    {
        lock (_gate)
            return _backups.FirstOrDefault(n => string.Equals(n.Name, name, StringComparison.Ordinal));
    }

    /// <summary>Running live nodes in creation order.</summary>
    internal IReadOnlyList<TestNode> RunningNodes()
    {
        lock (_gate)
            return _nodes.Where(n => n.State == TestNodeState.Running).ToArray();
    }

    internal static TimeSpan Max(TimeSpan a, TimeSpan b) => a >= b ? a : b;

    internal static Guid NewGuid(IRandomSource random)
    {
        Span<byte> bytes = stackalloc byte[16];
        random.Fill(bytes);
        bytes[7] = (byte)((bytes[7] & 0x0F) | 0x40); // version 4 (Guid byte order)
        bytes[8] = (byte)((bytes[8] & 0x3F) | 0x80);
        return new Guid(bytes);
    }

    internal static byte[] NextBytes(IRandomSource random, int count)
    {
        byte[] bytes = new byte[count];
        random.Fill(bytes);
        return bytes;
    }

    /// <summary>A principal secret drawn from <paramref name="random"/> (printable).</summary>
    internal static string NewSecret(IRandomSource random) => "s-" + Convert.ToHexStringLower(NextBytes(random, 16));

    /// <summary>The name of the running test (seed derivation and data folder), or a fixed name outside xunit.</summary>
    internal static string CurrentTestName()
    {
        ITestContext? context = TestContext.Current;
        string? name = context?.Test?.TestDisplayName;
        if (string.IsNullOrWhiteSpace(name))
            name = context?.TestMethod?.MethodName;
        return string.IsNullOrWhiteSpace(name) ? "adhoc" : name;
    }

    /// <summary>A file-system-safe, bounded folder name for <paramref name="name"/>.</summary>
    internal static string SafeFolderName(string name)
    {
        var sb = new System.Text.StringBuilder(name.Length);
        foreach (char c in name)
            sb.Append(char.IsAsciiLetterOrDigit(c) || c is '-' or '_' or '.' ? c : '_');
        string text = sb.ToString().Trim('.', '_');
        if (text.Length == 0)
            text = "test";
        if (text.Length > 80)
        {
            // Keep the most specific end (method and arguments) and disambiguate with a hash of the full name.
            string hash = SeededRandom.DeriveSeed(0, name).ToString("x16", CultureInfo.InvariantCulture);
            text = text[^63..].TrimStart('.', '_') + "-" + hash;
        }

        return text;
    }

    /// <summary>The live/backup nodes of this cluster as fault targets.</summary>
    private sealed class TargetDirectory(TestCluster cluster) : IFaultTargets
    {
        public IFaultTarget? FindTarget(string name) => (IFaultTarget?)cluster.FindNode(name) ?? cluster.FindBackup(name);
    }
}
