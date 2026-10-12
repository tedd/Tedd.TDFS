using System.Buffers.Binary;
using System.Globalization;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Storage;

namespace Tedd.Tdfs.Testing.Mesh;

/// <summary>What a <see cref="MeshScenario"/> sets up besides the cluster.</summary>
public sealed record MeshScenarioOptions
{
    /// <summary>Run the concurrent multi-client <see cref="MeshWorkload"/> throughout the scenario.</summary>
    public bool Workload { get; init; } = true;

    /// <summary>Workload shape.</summary>
    public MeshWorkloadOptions WorkloadOptions { get; init; } = new();

    /// <summary>Run the single-process <see cref="MeshNamespaceScript"/> throughout the scenario.</summary>
    public bool NamespaceScript { get; init; } = true;

    /// <summary>Steps of the namespace script.</summary>
    public int NamespaceSteps { get; init; } = 14;

    /// <summary>Probe clients (network names <c>client:{name}</c>).</summary>
    public IReadOnlyList<string> ProbeClients { get; init; } = ["p0"];

    /// <summary>Operations probed by <see cref="MeshScenario.ProbeAsync"/> by default.</summary>
    public IReadOnlyList<MeshProbeKind> ProbeKinds { get; init; } = Enum.GetValues<MeshProbeKind>();

    /// <summary>SDK operation timeout of every scenario client (the per-operation deadline).</summary>
    public TimeSpan ClientOperationTimeout { get; init; } = TimeSpan.FromSeconds(3);

    /// <summary>Bound within which a <see cref="MeshTri.Must"/> probe has to succeed (covers election, lease and re-registration).</summary>
    public TimeSpan MustWithin { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>Attempts (spaced by an election window) a <see cref="MeshTri.Never"/> probe makes; each must fail typed.</summary>
    public int NeverAttempts { get; init; } = 2;

    /// <summary>Bound for recovery waits (stability, replication, repair, catch-up).</summary>
    public TimeSpan RecoveryTimeout { get; init; } = TimeSpan.FromSeconds(90);

    /// <summary>Bound for the namespace script to wait through an outage.</summary>
    public TimeSpan NamespaceMaxWait { get; init; } = TimeSpan.FromMinutes(6);

    /// <summary>Length of the small probe file.</summary>
    public int SmallLength { get; init; } = 512;
}

/// <summary>
/// Orchestrates one mesh scenario on a running <see cref="TestCluster"/>: verifies the formed topology and roles,
/// prepares probe files (recording the verified holders of every object), runs the workload and namespace script,
/// mirrors every fault into a <see cref="MeshNetworkModel"/>, probes availability against <see cref="MeshOracle"/>
/// (Must = bounded eventual success with verified bytes; Never = typed failure on every attempt, never fabricated
/// success; Either = success with verified bytes or a typed failure), checks safety during faults and, after recovery,
/// checks convergence, durability of every acknowledged write and all histories.
/// </summary>
public sealed class MeshScenario
{
    /// <summary>Volume used by mesh scenarios.</summary>
    public const string Volume = MeshConfig.VolumeName;

    /// <summary>Probe directory.</summary>
    public const string ProbeRoot = "/mesh/probe";

    private const string SmallPath = ProbeRoot + "/small";
    private const string LargePath = ProbeRoot + "/large";
    private const string PrincipalSecret = "mesh-probe-secret";

    private readonly object _lock = new();
    private readonly Dictionary<string, DfsClient> _clients = new(StringComparer.Ordinal);
    private readonly SortedSet<string> _clientNetworkNames = new(StringComparer.Ordinal);
    private readonly List<ProbeWrite> _probeWrites = [];
    private readonly List<ProbeAppend> _probeAppends = [];
    private readonly List<string> _ackedPrincipals = [];
    private readonly HashSet<string> _paused = new(StringComparer.Ordinal);
    private readonly HashSet<string> _removed = new(StringComparer.Ordinal);
    private readonly SortedSet<string> _pendingRegistration = new(StringComparer.Ordinal);
    private readonly List<string> _trace = [];
    private int _counter;
    private byte[] _small = [];
    private byte[] _large = [];
    private IReadOnlyList<IReadOnlyList<string>> _smallHolders = [];
    private IReadOnlyList<IReadOnlyList<string>> _largeHolders = [];
    private bool _smallInline;

    private MeshScenario(TestCluster cluster, MeshConfig config, MeshScenarioOptions options, CancellationToken ct)
    {
        Cluster = cluster;
        Config = config;
        Options = options;
        Ct = ct;
        Net = new MeshNetworkModel(cluster.Options.GetNodeSpecs(), cluster.Options.Mesh);
    }

    /// <summary>The cluster.</summary>
    public TestCluster Cluster { get; }

    /// <summary>The matrix configuration.</summary>
    public MeshConfig Config { get; }

    /// <summary>Scenario options.</summary>
    public MeshScenarioOptions Options { get; }

    /// <summary>Test-side liveness/link mirror (oracle input).</summary>
    public MeshNetworkModel Net { get; }

    /// <summary>Cancellation token of the test.</summary>
    public CancellationToken Ct { get; }

    /// <summary>The volume's effective default data policy as reported by the cluster.</summary>
    public DfsStoragePolicy Policy { get; private set; } = DfsStoragePolicy.Default;

    /// <summary>The volume's inline threshold.</summary>
    public int InlineThreshold { get; private set; }

    /// <summary>Configured volume voters (committed configuration read while healthy).</summary>
    public IReadOnlyList<string> VolumeVoters { get; private set; } = [];

    /// <summary>Configured control voters (committed configuration read while healthy).</summary>
    public IReadOnlyList<string> ControlVoters { get; private set; } = [];

    /// <summary>The workload, when enabled.</summary>
    public MeshWorkload? Workload { get; private set; }

    /// <summary>The namespace script, when enabled.</summary>
    public MeshNamespaceScript? Namespace { get; private set; }

    /// <summary>Timings of the nodes.</summary>
    public NodeTimings Timings => Cluster.Options.Timings;

    /// <summary>Longest election timeout.</summary>
    public TimeSpan ElectionWindow => Timings.Tick * Timings.ElectionTicksMax;

    /// <summary>Scenario events (faults, phases) for diagnostics.</summary>
    public IReadOnlyList<string> Trace => _trace;

    /// <summary>Every client network name known to the scenario (workload, script, probes, ad-hoc).</summary>
    public IReadOnlyCollection<string> ClientNetworkNames => _clientNetworkNames;

    /// <summary>
    /// Nodes restarted while the control group could not re-register their new incarnation (no connected control-voter
    /// majority they can reach). RCV-02 lets a returning member be distrusted until it re-established its incarnation, so
    /// whether such a node already counts for volume quorums, receipts and reads is not fixed by the specification; the
    /// expectations treat it as "maybe down" (<see cref="MeshOracle.Bound"/>) until the control group can register it.
    /// </summary>
    public IReadOnlyCollection<string> PendingRegistration
    {
        get
        {
            lock (_lock)
                return _pendingRegistration.ToArray();
        }
    }

    /// <summary>True when the small probe file is stored inline (metadata durability satisfies the acknowledgement policy).</summary>
    public bool SmallIsInline => _smallInline;

    /// <summary>Nodes that are neither volume nor control voters.</summary>
    public IReadOnlyList<string> NonVoters => Net.NodeNames.Where(n => !VolumeVoters.Contains(n) && !ControlVoters.Contains(n)).ToArray();

    /// <summary>Network name of a client.</summary>
    public static string ClientNet(string client) => "client:" + client;

    /// <summary>A connected scenario client by name.</summary>
    public DfsClient Client(string name) => _clients[name];

    /// <summary>Sets up the scenario on a stable cluster.</summary>
    public static async Task<MeshScenario> StartAsync(TestCluster cluster, MeshConfig config, MeshScenarioOptions? options = null, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(cluster);
        ArgumentNullException.ThrowIfNull(config);
        var s = new MeshScenario(cluster, config, options ?? new MeshScenarioOptions(), ct);
        s.Note($"start {config.Nodes} nodes {config.Format()} seed {cluster.Seed}");
        s.VerifyLayout();
        await s.ReadRolesAsync();
        string[] seeds = await s.LeaderFirstSeedsAsync();
        foreach (string name in s.Options.ProbeClients)
            s._clients[name] = await s.ConnectClientAsync(name, seeds);
        await s.PrepareProbeFilesAsync();
        if (s.Options.Workload)
        {
            s.Workload = await MeshWorkload.StartAsync(cluster, Volume, s.Options.WorkloadOptions with { OperationTimeout = s.Options.ClientOperationTimeout }, ct);
            foreach (string name in s.Workload.NetworkNames)
                s._clientNetworkNames.Add(name);
        }

        if (s.Options.NamespaceScript)
        {
            DfsClient ns = await s.ConnectClientAsync("ns", seeds);
            s.Namespace = await MeshNamespaceScript.StartAsync(cluster, ns, Volume, "/mesh/ns", s.Options.NamespaceSteps, s.Options.NamespaceMaxWait, ct);
        }

        return s;
    }

    /// <summary>Connects (and registers) a scenario client.</summary>
    public async Task<DfsClient> ConnectClientAsync(string name, IReadOnlyList<string>? seeds)
    {
        _clientNetworkNames.Add(ClientNet(name));
        DfsClient client = await Cluster.ConnectAsync(new TestClientOptions
        {
            Name = name,
            SeedNodes = seeds,
            OperationTimeout = Options.ClientOperationTimeout,
        }, Ct);
        Assert.Equal(Cluster.ClusterId, client.Cluster.ClusterId);
        _clients[name] = client;
        return client;
    }

    /// <summary>Records a diagnostic event.</summary>
    public void Note(string text)
    {
        lock (_lock)
            _trace.Add($"[{Cluster.Time.GetUtcNow():HH:mm:ss.fff}] {text}");
    }

    // ------------------------------------------------------------------------------------------------------------
    // Roles
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>The current volume leader (waits for one).</summary>
    public async Task<string> VolumeLeaderAsync() => (await Cluster.GetLeaderAsync(Volume, Ct)).Name;

    /// <summary>The current control leader (waits for one).</summary>
    public async Task<string> ControlLeaderAsync() => (await Cluster.GetControlLeaderAsync(Ct)).Name;

    /// <summary>The volume group as seen by the harness.</summary>
    public Task<GroupInspection> VolumeGroupAsync() => Cluster.Inspect.GetVolumeGroupAsync(Volume);

    /// <summary>Specifications of the configured volume voters.</summary>
    public IReadOnlyList<TestNodeSpec> VolumeVoterSpecs => VolumeVoters.Select(Net.Spec).ToArray();

    /// <summary>Overrides the configured voter sets after an explicit membership change (drain/remove).</summary>
    public void SetConfiguredVoters(IReadOnlyList<string>? volumeVoters = null, IReadOnlyList<string>? controlVoters = null)
    {
        if (volumeVoters is not null)
            VolumeVoters = volumeVoters.Order(StringComparer.Ordinal).ToArray();
        if (controlVoters is not null)
            ControlVoters = controlVoters.Order(StringComparer.Ordinal).ToArray();
        Note($"configured voters now volume [{string.Join(",", VolumeVoters)}] control [{string.Join(",", ControlVoters)}]");
    }

    /// <summary>Waits until the volume leader is one of <paramref name="candidates"/> (bounded) and returns it.</summary>
    public async Task<string> WaitForVolumeLeaderAmongAsync(IReadOnlyCollection<string> candidates, string because)
    {
        string? leader = null;
        await Cluster.WaitUntilAsync(async () =>
        {
            GroupInspection g = await VolumeGroupAsync();
            leader = g.Leader;
            return leader is not null && candidates.Contains(leader);
        }, Options.MustWithin, because, Ct);
        return leader!;
    }

    // ------------------------------------------------------------------------------------------------------------
    // Faults (mirrored into Net)
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>Crashes a node with the configured crash mode (or <paramref name="mode"/>).</summary>
    public async Task CrashAsync(string node, CrashMode? mode = null)
    {
        Note($"crash {node} ({mode ?? Config.Crash})");
        await Cluster.Node(node).CrashAsync(mode ?? Config.Crash);
        Net.SetDown(node, true);
        Assert.Equal(TestNodeState.Crashed, Cluster.Node(node).State);
    }

    /// <summary>Stops a node gracefully.</summary>
    public async Task StopNodeAsync(string node)
    {
        Note($"stop {node}");
        await Cluster.Node(node).StopAsync();
        Net.SetDown(node, true);
        Assert.Equal(TestNodeState.Stopped, Cluster.Node(node).State);
    }

    /// <summary>Restarts a crashed/stopped node from its durable image and checks the incarnation advanced by one.</summary>
    public async Task RestartAsync(string node)
    {
        TestNode n = Cluster.Node(node);
        ulong before = n.Incarnation;
        Note($"restart {node} (incarnation {before})");
        await n.RestartAsync();
        Net.SetDown(node, false);
        Assert.Equal(TestNodeState.Running, n.State);
        Assert.True(n.Incarnation == before + 1, $"restart of {node} must advance its incarnation durably by one ({before} -> {n.Incarnation}, RCV-02)");
        lock (_lock)
        {
            if (CanRegister(node))
            {
                _pendingRegistration.Remove(node);
            }
            else
            {
                _pendingRegistration.Add(node);
                _trace.Add($"[{Cluster.Time.GetUtcNow():HH:mm:ss.fff}] {node} restarted while the control group cannot register its incarnation (RCV-02): maybe-down until it can");
            }
        }
    }

    /// <summary>
    /// True when a running node can re-register its incarnation: a connected, fully linked majority of the configured
    /// control voters exists and the node reaches every control voter of it (so it reaches the control leader).
    /// </summary>
    private bool CanRegister(string node)
    {
        if (Net.IsDown(node) || ControlVoters.Count == 0)
            return false;
        IReadOnlySet<string>? side = MeshOracle.QuorumSide(Net, ControlVoters);
        if (side is null || !MeshOracle.IsClique(Net, side))
            return false;
        return ControlVoters.Where(side.Contains).All(v => v == node || Net.Linked(node, v));
    }

    private void RefreshRegistrations()
    {
        lock (_lock)
        {
            foreach (string node in _pendingRegistration.Where(CanRegister).ToArray())
            {
                _pendingRegistration.Remove(node);
                _trace.Add($"[{Cluster.Time.GetUtcNow():HH:mm:ss.fff}] {node} can now register its incarnation with the control group");
            }
        }
    }

    /// <summary>Pauses a node (memory retained).</summary>
    public async Task PauseAsync(string node)
    {
        Note($"pause {node}");
        await Cluster.Node(node).PauseAsync();
        _paused.Add(node);
        Net.SetDown(node, true);
    }

    /// <summary>Resumes a paused node.</summary>
    public async Task ResumeAsync(string node)
    {
        Note($"resume {node}");
        await Cluster.Node(node).ResumeAsync();
        _paused.Remove(node);
        Net.SetDown(node, false);
    }

    /// <summary>
    /// Partitions nodes into groups; every known client is placed in group <paramref name="clientGroup"/>(client network
    /// name) (default: group 0).
    /// </summary>
    public void PartitionNodes(IReadOnlyList<IReadOnlyList<string>> nodeGroups, Func<string, int>? clientGroup = null)
    {
        List<List<string>> groups = nodeGroups.Select(g => g.ToList()).ToList();
        foreach (string client in _clientNetworkNames)
            groups[clientGroup?.Invoke(client) ?? 0].Add(client);
        string[][] array = groups.Select(g => g.ToArray()).ToArray();
        Note("partition " + string.Join(" | ", array.Select(g => string.Join(",", g))));
        Cluster.Network.Partition(array);
        Net.Partition(array);
    }

    /// <summary>Cuts every link of a name.</summary>
    public void Isolate(string name)
    {
        Note("isolate " + name);
        Cluster.Network.Isolate(name);
        Net.Isolate(name);
    }

    /// <summary>Cuts one direction.</summary>
    public void Cut(string from, string to)
    {
        Note($"cut {from}>{to}");
        Cluster.Network.Cut(from, to);
        Net.Cut(from, to);
    }

    /// <summary>Cuts both directions.</summary>
    public void CutBoth(string a, string b)
    {
        Note($"cut {a}<>{b}");
        Cluster.Network.CutBoth(a, b);
        Net.CutBoth(a, b);
    }

    /// <summary>Half-open link from <paramref name="from"/> to <paramref name="to"/>.</summary>
    public void BlackHole(string from, string to)
    {
        Note($"black-hole {from}>{to}");
        Cluster.Network.BlackHole(from, to);
        Net.BlackHole(from, to);
    }

    /// <summary>Restores one direction.</summary>
    public void Restore(string from, string to)
    {
        Note($"restore {from}>{to}");
        Cluster.Network.Restore(from, to);
        Net.Restore(from, to);
    }

    /// <summary>Removes every network fault and the mesh restriction.</summary>
    public void Heal()
    {
        Note("heal");
        Cluster.Network.Heal();
        Net.Heal();
    }

    /// <summary>Adds and joins a live node.</summary>
    public async Task<TestNode> AddNodeAsync(TestNodeSpec spec)
    {
        Note("add " + spec.Name + " in " + spec.Zone);
        TestNode node = await Cluster.AddNodeAsync(spec, join: true, Ct);
        lock (_lock)
            Net.AddNode(spec); // several joins may be in flight at once (MESH-015 "Concurrent")
        return node;
    }

    /// <summary>Starts draining a node (it stops being a placement target at once).</summary>
    public async Task DrainAsync(string node, string adminClient = "p0")
    {
        Note("drain " + node);
        Net.SetEligible(node, false);
        await Client(adminClient).Admin.DrainNodeAsync(node, Ct);
    }

    /// <summary>True when the node was removed from the cluster by the scenario.</summary>
    public bool IsRemoved(string node) => _removed.Contains(node);

    /// <summary>Marks a node removed in the model (after a successful RemoveNode) so recovery leaves it down.</summary>
    public void MarkRemoved(string node)
    {
        Note("removed " + node);
        _removed.Add(node);
        Net.SetEligible(node, false);
    }

    // ------------------------------------------------------------------------------------------------------------
    // Expectations and probes
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// The oracle's expectation for <paramref name="kind"/> issued by scenario client <paramref name="client"/> now.
    /// <paramref name="metadata"/> replaces the volume-quorum component when the test knows more than the generic oracle
    /// (for example which node is leader); the reasoning must be documented at the call site.
    /// </summary>
    public MeshExpectation Expect(MeshProbeKind kind, string client, MeshExpectation? metadata = null) => ExpectWith(Net, kind, client, metadata);

    /// <summary>
    /// The oracle's expectation for <paramref name="kind"/> if <paramref name="change"/> were applied to the current network
    /// (evaluated on a copy; nothing changes). Used to choose fault targets that keep a precondition.
    /// </summary>
    public MeshExpectation ExpectIf(Action<MeshNetworkModel> change, MeshProbeKind kind, string client)
    {
        ArgumentNullException.ThrowIfNull(change);
        MeshNetworkModel what = Net.Clone();
        change(what);
        return ExpectWith(what, kind, client, null);
    }

    /// <summary>
    /// Expectation for a live read of a file whose verified holders were recorded with <see cref="RecordHoldersAsync"/>
    /// (no holders = inline: the metadata quorum alone decides).
    /// </summary>
    public MeshExpectation ExpectRead(IReadOnlyList<IReadOnlyList<string>> holders, string client)
    {
        ArgumentNullException.ThrowIfNull(holders);
        string net = ClientNet(client);
        return Bounded(Net, n =>
        {
            MeshExpectation quorum = MeshOracle.Quorum(n, VolumeVoters, net, MeshCause.MetadataQuorum);
            return holders.Count == 0 ? quorum : MeshExpectation.Combine(quorum, MeshOracle.Holders(n, holders, net));
        });
    }

    /// <summary>Records the verified holders of every object of an acknowledged file (empty when it is stored inline).</summary>
    public async Task<IReadOnlyList<IReadOnlyList<string>>> RecordHoldersAsync(string path)
    {
        FileInspection file = await Cluster.Inspect.GetFileAsync(Volume, path)
            ?? throw new InvalidOperationException(path + " missing from the leader's applied state");
        return file.IsInline ? [] : await HoldersAsync(file, requireTarget: false);
    }

    private MeshExpectation ExpectWith(MeshNetworkModel network, MeshProbeKind kind, string client, MeshExpectation? metadata)
    {
        string net = ClientNet(client);
        if (kind == MeshProbeKind.ControlChange)
        {
            // Re-registration is itself a control-group operation, so restarted control voters take part without it.
            return MeshOracle.Quorum(network, ControlVoters, net, MeshCause.ControlQuorum);
        }

        return Bounded(network, n =>
        {
            MeshExpectation quorum = metadata ?? MeshOracle.Quorum(n, VolumeVoters, net, MeshCause.MetadataQuorum);
            IReadOnlySet<string>? side = MeshOracle.QuorumSide(n, VolumeVoters);
            MeshExpectation Placement() => MeshOracle.Placement(n, Policy, net, side);
            return kind switch
            {
                MeshProbeKind.WriteSmall => _smallInline ? quorum : MeshExpectation.Combine(quorum, Placement()),
                MeshProbeKind.WriteLarge => MeshExpectation.Combine(quorum, Placement()),
                MeshProbeKind.ReadSmall => _smallInline ? quorum : MeshExpectation.Combine(quorum, MeshOracle.Holders(n, _smallHolders, net)),
                MeshProbeKind.ReadLarge => MeshExpectation.Combine(quorum, MeshOracle.Holders(n, _largeHolders, net)),
                MeshProbeKind.Append => AppendStaysInline(client) ? quorum : MeshExpectation.Combine(quorum, Placement()),
                _ => throw new ArgumentOutOfRangeException(nameof(kind)),
            };
        });
    }

    /// <summary>Applies the RCV-02 uncertainty of <see cref="PendingRegistration"/> to an expectation computed on <paramref name="network"/>.</summary>
    private MeshExpectation Bounded(MeshNetworkModel network, Func<MeshNetworkModel, MeshExpectation> compute)
    {
        RefreshRegistrations();
        string[] pending = PendingRegistration.ToArray();
        if (pending.Length == 0)
            return compute(network);
        MeshNetworkModel without = network.Clone();
        foreach (string node in pending)
            without.SetDown(node, true);
        return MeshOracle.Bound(compute(without), compute(network),
            $"[{string.Join(",", pending)}] restarted while the control group could not re-register their incarnation (RCV-02)");
    }

    private bool AppendStaysInline(string client)
    {
        long length;
        lock (_lock)
            length = 16L * (_probeAppends.Count(a => a.Client == client && a.Status != OpOutcome.Failed) + 1);
        return MeshOracle.StoresInline(length, InlineThreshold, Policy, VolumeVoterSpecs);
    }

    /// <summary>
    /// Probes every (client, kind) concurrently against the oracle's current expectation and asserts it. Returns the
    /// expectations that were checked.
    /// </summary>
    public async Task<IReadOnlyDictionary<(string Client, MeshProbeKind Kind), MeshExpectation>> ProbeAsync(string phase,
        IReadOnlyList<string>? clients = null, IReadOnlyList<MeshProbeKind>? kinds = null, Func<string, MeshExpectation?>? metadataFor = null)
    {
        clients ??= Options.ProbeClients;
        kinds ??= Options.ProbeKinds;
        var expectations = new Dictionary<(string, MeshProbeKind), MeshExpectation>();
        foreach (string client in clients)
        {
            foreach (MeshProbeKind kind in kinds)
                expectations[(client, kind)] = Expect(kind, client, metadataFor?.Invoke(client));
        }

        Note($"probe {phase}: " + string.Join("; ", expectations.Select(e => $"{e.Key.Item1}/{e.Key.Item2}={e.Value.Outcome}")));
        await Task.WhenAll(expectations.Select(e => ProbeOneAsync(phase, e.Key.Item1, e.Key.Item2, e.Value)));
        return expectations;
    }

    /// <summary>Asserts one probe against an explicit expectation (for scenario-specific reasoning documented in the test).</summary>
    public async Task ProbeOneAsync(string phase, string client, MeshProbeKind kind, MeshExpectation expectation)
    {
        bool mutation = kind is MeshProbeKind.WriteSmall or MeshProbeKind.WriteLarge or MeshProbeKind.Append or MeshProbeKind.ControlChange;
        string what = $"[{phase}] {kind} via {client}";
        switch (expectation.Outcome)
        {
            case MeshTri.Must:
            {
                DateTimeOffset deadline = Cluster.Time.GetUtcNow() + Options.MustWithin;
                Exception? last = null;
                int attempt = 0;
                while (true)
                {
                    attempt++;
                    try
                    {
                        await ExecuteAsync(phase, client, kind, attempt);
                        return;
                    }
                    catch (Exception ex) when (ex is not Xunit.Sdk.XunitException && IsTransient(ex))
                    {
                        last = MeshOracle.Unwrap(ex);
                    }
                    catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
                    {
                        Assert.Fail($"{what} must succeed ({expectation}) but failed permanently: {MeshOracle.Describe(MeshOracle.Unwrap(ex))}; net: {Net.Describe()}{Environment.NewLine}{ex}");
                    }

                    if (Cluster.Time.GetUtcNow() >= deadline)
                        Assert.Fail($"{what} must succeed within {Options.MustWithin} ({expectation}); {attempt} attempts, last: {MeshOracle.Describe(last)}; net: {Net.Describe()}");
                    await Cluster.DelayAsync(TimeSpan.FromMilliseconds(250), Ct);
                }
            }

            case MeshTri.Never:
                for (int attempt = 1; attempt <= Options.NeverAttempts; attempt++)
                {
                    bool succeeded = false;
                    try
                    {
                        await ExecuteAsync(phase, client, kind, attempt);
                        succeeded = true;
                    }
                    catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
                    {
                        string? why = MeshOracle.CheckFailure(ex, expectation.Causes, mutation, Policy);
                        Assert.True(why is null, $"{what} failed illegally ({expectation}): {why}; net: {Net.Describe()}");
                    }

                    Assert.False(succeeded, $"{what} succeeded but no correct implementation can ({expectation}); fabricated success (INV-08); net: {Net.Describe()}");
                    if (attempt < Options.NeverAttempts)
                        await Cluster.DelayAsync(ElectionWindow * 2, Ct);
                }

                return;
            default:
                try
                {
                    await ExecuteAsync(phase, client, kind, 1);
                }
                catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
                {
                    string? why = MeshOracle.CheckFailure(ex, expectation.Causes, mutation, Policy);
                    Assert.True(why is null, $"{what} may fail but only typed ({expectation}): {why}; net: {Net.Describe()}");
                }

                return;
        }
    }

    private static bool IsTransient(Exception ex) => MeshOracle.Unwrap(ex) is DfsUnavailableException or DfsCommitOutcomeUnknownException or TimeoutException
        or DfsLeaseLostException or DfsPinExpiredException or DfsOperationExpiredException or DfsException { ErrorCode: DfsErrorCode.Timeout or DfsErrorCode.NotLeader };

    private async Task ExecuteAsync(string phase, string client, MeshProbeKind kind, int attempt)
    {
        DfsClient c = Client(client);
        DfsVolume v = c.GetVolume(Volume);
        int n = Interlocked.Increment(ref _counter);
        switch (kind)
        {
            case MeshProbeKind.WriteSmall:
            case MeshProbeKind.WriteLarge:
            {
                int length = kind == MeshProbeKind.WriteSmall ? Options.SmallLength : _large.Length;
                var write = new ProbeWrite($"{ProbeRoot}/{phase}-{client}-{kind}-{attempt}-{n}", TestData.Bytes(Cluster.Seed ^ (ulong)n, length));
                lock (_lock)
                    _probeWrites.Add(write);
                try
                {
                    await v.Files.WriteAllBytesAsync(write.Path, write.Data, Ct);
                    write.Status = OpOutcome.Ok;
                }
                catch (Exception ex)
                {
                    Exception e = MeshOracle.Unwrap(ex);
                    write.Status = MeshOracle.ClassifyOutcome(e);
                    if (e is DfsCommitOutcomeUnknownException u)
                        write.OperationId = u.OperationId;
                    throw;
                }

                byte[] back = await v.Files.ReadAllBytesAsync(write.Path, Ct);
                AssertEx.BytesEqual(write.Data, back, $"read-back of acknowledged probe write {write.Path}");
                break;
            }

            case MeshProbeKind.ReadSmall:
            case MeshProbeKind.ReadLarge:
            {
                string path = kind == MeshProbeKind.ReadSmall ? SmallPath : LargePath;
                byte[] expected = kind == MeshProbeKind.ReadSmall ? _small : _large;
                byte[] actual;
                try
                {
                    actual = await v.Files.ReadAllBytesAsync(path, Ct);
                }
                catch (FileNotFoundException ex)
                {
                    Assert.Fail($"[{phase}] {path} written before the fault was reported absent via {client} (false absence, INV-08): {ex.Message}");
                    throw;
                }

                AssertEx.BytesEqual(expected, actual, $"[{phase}] {path} via {client} (INV-14: never invented or stale bytes)");
                break;
            }

            case MeshProbeKind.Append:
            {
                var entry = new ProbeAppend(client, new byte[16]);
                BinaryPrimitives.WriteUInt32LittleEndian(entry.Data, 0x50524F42u);
                BinaryPrimitives.WriteInt32LittleEndian(entry.Data.AsSpan(4), n);
                BinaryPrimitives.WriteUInt64LittleEndian(entry.Data.AsSpan(8), Cluster.Seed ^ (ulong)n);
                lock (_lock)
                    _probeAppends.Add(entry);
                try
                {
                    await v.Files.AppendAllBytesAsync(LogPath(client), entry.Data, Ct);
                    entry.Status = OpOutcome.Ok;
                    entry.Order = Interlocked.Increment(ref _counter);
                }
                catch (Exception ex)
                {
                    Exception e = MeshOracle.Unwrap(ex);
                    entry.Status = MeshOracle.ClassifyOutcome(e);
                    throw;
                }

                break;
            }

            case MeshProbeKind.ControlChange:
            {
                string principal = "mp" + n.ToString(CultureInfo.InvariantCulture);
                await c.Admin.CreatePrincipalAsync(principal, PrincipalSecret, clusterAdmin: false, Ct);
                lock (_lock)
                    _ackedPrincipals.Add(principal);
                break;
            }

            default:
                throw new ArgumentOutOfRangeException(nameof(kind));
        }
    }

    private static string LogPath(string client) => $"{ProbeRoot}/log-{client}";

    // ------------------------------------------------------------------------------------------------------------
    // Safety, recovery, final checks
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Safety during a fault: at most one leader per term in every group, and acknowledged writes read through
    /// <paramref name="client"/> are never reported absent nor returned with other bytes (a typed failure is legal).
    /// </summary>
    public async Task AssertSafetyAsync(string phase, string client = "p0")
    {
        await Cluster.Inspect.AssertNoSplitBrainAsync();
        ProbeWrite[] sample;
        lock (_lock)
            sample = _probeWrites.Where(w => w.Status == OpOutcome.Ok).TakeLast(3).ToArray();
        DfsVolume v = Client(client).GetVolume(Volume);
        await Task.WhenAll(sample.Select(async w =>
        {
            try
            {
                byte[] data = await v.Files.ReadAllBytesAsync(w.Path, Ct);
                AssertEx.BytesEqual(w.Data, data, $"[{phase}] acknowledged write {w.Path} read during the fault");
            }
            catch (FileNotFoundException)
            {
                Assert.Fail($"[{phase}] acknowledged write {w.Path} reported absent during the fault (lost acknowledged write or false absence)");
            }
            catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
            {
                string? why = MeshOracle.CheckFailure(ex, new HashSet<MeshCause> { MeshCause.MetadataQuorum, MeshCause.Unreachable, MeshCause.NoHealthyReplica }, false, Policy);
                Assert.True(why is null, $"[{phase}] reading acknowledged {w.Path} during the fault: {why}");
            }
        }));
    }

    /// <summary>
    /// Waits until the cluster settled after a node returned. On a fully connected mesh this is the harness's stability
    /// and replication wait. On a partial link mesh a node without a direct link to a group leader cannot receive that
    /// leader's log (the FSD requires no relaying), so only the re-election of a control and a volume leader is awaited
    /// (bounded), exactly as at formation.
    /// </summary>
    public async Task WaitSettledAsync()
    {
        if (Net.IsFullyConnected)
        {
            await Cluster.WaitForStableAsync(Options.RecoveryTimeout, Ct);
            await Cluster.WaitForReplicationAsync(Volume, Ct);
            return;
        }

        await Cluster.WaitUntilAsync(
            async () => (await Cluster.Inspect.GetControlGroupAsync()).Leader is not null && (await VolumeGroupAsync()).Leader is not null,
            Options.RecoveryTimeout, $"control and volume leaders must be elected again on the {Net.Mesh} link mesh, as at formation", Ct);
    }

    /// <summary>Resumes paused nodes, restarts down nodes (except removed ones), heals the network and waits for stability and replication.</summary>
    public async Task RecoverAllAsync()
    {
        foreach (string node in _paused.Order(StringComparer.Ordinal).ToArray())
            await ResumeAsync(node);
        foreach (string node in Net.DownNodes.Where(n => !_removed.Contains(n)).Order(StringComparer.Ordinal).ToArray())
            await RestartAsync(node);
        Heal();
        await Cluster.WaitForStableAsync(Options.RecoveryTimeout, Ct);
        await Cluster.WaitForReplicationAsync(Volume, Ct);
        Note("recovered");
    }

    /// <summary>
    /// After the scenario: full recovery, every probe Must again, workload progress after recovery, histories, namespace,
    /// acknowledged probe writes/appends/principals, repair to target, control catch-up, converged replicas and no split brain.
    /// </summary>
    /// <param name="requireFullRepair">
    /// False only when the scenario deliberately leaves the cluster unable to hold the full replica target (for example a
    /// blocked drain); the deficit must then stay visible rather than be waited away.
    /// </param>
    public async Task FinishAsync(bool requireFullRepair = true)
    {
        await RecoverAllAsync();
        bool allEligible = Net.NodeNames.Where(n => !_removed.Contains(n)).All(Net.IsEligible);
        foreach (string client in Options.ProbeClients.Where(_ => allEligible))
        {
            foreach (MeshProbeKind kind in Options.ProbeKinds)
            {
                MeshExpectation e = Expect(kind, client);
                Assert.True(e.Outcome == MeshTri.Must, $"after full recovery {kind} via {client} must be available, oracle says {e}");
            }
        }

        await ProbeAsync("final");
        if (Workload is { } workload)
        {
            long mark = workload.Mark();
            await Cluster.WaitUntilAsync(() => Task.FromResult(workload.Processes.All(p => !workload.IsRunning(p) || workload.OkOperationsSince(p, mark) > 0)),
                Options.MustWithin, "every workload client makes progress once quorum, data targets and connectivity are restored (INV-17)", Ct);
            await workload.VerifyAsync();
        }

        if (Namespace is not null)
            await Namespace.VerifyAsync();
        await VerifyProbeLedgerAsync();
        if (requireFullRepair)
            await Cluster.WaitForRepairAsync(Volume, Options.RecoveryTimeout, Ct);
        await AssertControlConvergedAsync();
        await Cluster.Inspect.AssertReplicasConvergedAsync(Volume);
        await Cluster.Inspect.AssertNoSplitBrainAsync();
        Note("finished");
    }

    /// <summary>Every running, non-removed node's control replica catches up with the control leader (cluster map convergence).</summary>
    public async Task AssertControlConvergedAsync()
    {
        await Cluster.WaitUntilAsync(async () =>
        {
            GroupInspection control = await Cluster.Inspect.GetControlGroupAsync();
            foreach (string name in Net.AliveNodes.Where(n => !_removed.Contains(n)))
            {
                GroupInspection? g = await Cluster.Node(name).Inspect.GetGroupAsync("control");
                if (g is null || g.AppliedIndex < control.CommitIndex)
                    return false;
            }

            return true;
        }, Options.RecoveryTimeout, "every running node's control replica must catch up with the control leader (membership and cluster map converge)", Ct);
    }

    private async Task VerifyProbeLedgerAsync()
    {
        DfsVolume v = Client(Options.ProbeClients[0]).GetVolume(Volume);
        ProbeWrite[] writes;
        ProbeAppend[] appends;
        string[] principals;
        lock (_lock)
        {
            writes = _probeWrites.ToArray();
            appends = _probeAppends.ToArray();
            principals = _ackedPrincipals.ToArray();
        }

        foreach (ProbeWrite w in writes)
        {
            OpOutcome status = w.Status;
            if (status == OpOutcome.Unknown && w.OperationId is DfsOperationId id)
            {
                DfsOperationOutcome outcome = await v.QueryOperationAsync(id, resolve: true, Ct);
                Assert.True(outcome.Status is DfsOutcomeStatus.Committed or DfsOutcomeStatus.Aborted or DfsOutcomeStatus.Expired,
                    $"resolving {id} of {w.Path} after recovery must not stay {outcome.Status} (OUT-014)");
                status = outcome.Status switch
                {
                    DfsOutcomeStatus.Committed => OpOutcome.Ok,
                    DfsOutcomeStatus.Aborted => OpOutcome.Failed,
                    _ => OpOutcome.Unknown,
                };
            }

            byte[]? actual = await ReadOrNullAsync(v, w.Path);
            if (status == OpOutcome.Ok)
            {
                Assert.True(actual is not null, $"acknowledged probe write {w.Path} lost (INV-01)");
                AssertEx.BytesEqual(w.Data, actual, $"acknowledged probe write {w.Path}");
            }
            else if (status == OpOutcome.Failed)
            {
                Assert.True(actual is null, $"probe write {w.Path} failed definitely but took effect (INV-06)");
            }
            else if (actual is not null)
            {
                AssertEx.BytesEqual(w.Data, actual, $"unresolved probe write {w.Path} took effect with different bytes");
            }
        }

        foreach (IGrouping<string, ProbeAppend> perClient in appends.GroupBy(a => a.Client))
        {
            byte[] log = await v.Files.ReadAllBytesAsync(LogPath(perClient.Key), Ct);
            Assert.True(log.Length % 16 == 0, $"probe log of {perClient.Key} has length {log.Length}, not a whole number of 16-byte entries (partial append exposed, INV-02)");
            var entries = new List<string>();
            for (int i = 0; i < log.Length; i += 16)
                entries.Add(Convert.ToHexString(log, i, 16));
            var known = perClient.ToDictionary(a => Convert.ToHexString(a.Data), StringComparer.Ordinal);
            foreach (string entry in entries)
                Assert.True(known.ContainsKey(entry), $"probe log of {perClient.Key} contains an entry nobody appended (invented bytes): {entry}");
            foreach (ProbeAppend a in perClient)
            {
                string hex = Convert.ToHexString(a.Data);
                int count = entries.Count(e => e == hex);
                if (a.Status == OpOutcome.Ok)
                    Assert.True(count == 1, $"acknowledged append {hex} of {perClient.Key} appears {count} times (INV-01/INV-06)");
                else if (a.Status == OpOutcome.Failed)
                    Assert.True(count == 0, $"definitely failed append {hex} of {perClient.Key} appears {count} times (INV-06)");
                else
                    Assert.True(count <= 1, $"unknown-outcome append {hex} of {perClient.Key} appears {count} times (at most once, INV-06)");
            }

            string[] ackedInOrder = perClient.Where(a => a.Status == OpOutcome.Ok).OrderBy(a => a.Order).Select(a => Convert.ToHexString(a.Data)).ToArray();
            string[] observedAcked = entries.Where(ackedInOrder.Contains).ToArray();
            Assert.True(ackedInOrder.SequenceEqual(observedAcked), $"acknowledged appends of {perClient.Key} are not in completion order (real-time order violated)");
        }

        foreach (string principal in principals.Take(3))
        {
            await using DfsClient c = await Cluster.ConnectAsync(new TestClientOptions
            {
                Name = "verify-" + principal,
                Credentials = new DfsCredentials(principal, PrincipalSecret),
                OperationTimeout = Options.ClientOperationTimeout,
            }, Ct);
            Assert.Equal(principal, c.Principal);
        }
    }

    private async Task<byte[]?> ReadOrNullAsync(DfsVolume v, string path)
    {
        try
        {
            return await v.Files.ReadAllBytesAsync(path, Ct);
        }
        catch (FileNotFoundException)
        {
            return null;
        }
    }

    // ------------------------------------------------------------------------------------------------------------
    // Setup
    // ------------------------------------------------------------------------------------------------------------

    private void VerifyLayout()
    {
        IReadOnlyList<TestNodeSpec> specs = Cluster.Options.GetNodeSpecs();
        Assert.Equal(Config.Nodes, specs.Count);
        Assert.Equal(Config.Nodes, Cluster.Nodes.Count);
        foreach (TestNodeSpec spec in specs)
        {
            TestNode node = Cluster.Node(spec.Name);
            Assert.Equal(TestNodeState.Running, node.State);
            Assert.Equal(spec.ToLabels(), node.Labels);
        }

        Assert.True(Cluster.Nodes.Select(n => n.Id).Distinct().Count() == Config.Nodes, "every node has a distinct persistent NodeId");
    }

    private async Task ReadRolesAsync()
    {
        int expectedControl = Config.EffectiveControlVoters;
        await Cluster.WaitUntilAsync(async () => (await Cluster.Inspect.GetControlGroupAsync()).Voters.Count == expectedControl, Options.RecoveryTimeout,
            $"the control group must reach its configured {expectedControl} voters (seed join promotes learners one at a time)", Ct);
        GroupInspection control = await Cluster.Inspect.GetControlGroupAsync();
        ControlVoters = control.Voters.Order(StringComparer.Ordinal).ToArray();
        string[] members = control.Voters.Concat(control.Learners).Order(StringComparer.Ordinal).ToArray();
        Assert.True(members.SequenceEqual(Net.NodeNames.Order(StringComparer.Ordinal)),
            $"every live node is a control voter or learner (ARCHITECTURE §2.4): voters [{string.Join(",", control.Voters)}] learners [{string.Join(",", control.Learners)}]");
        if (Config.ExplicitControlVoters() is { } explicitControl)
            Assert.True(ControlVoters.SequenceEqual(explicitControl.Order(StringComparer.Ordinal)), $"control voters [{string.Join(",", ControlVoters)}] differ from the bootstrap description [{string.Join(",", explicitControl)}]");

        GroupInspection volume = await VolumeGroupAsync();
        VolumeVoters = volume.Voters.Order(StringComparer.Ordinal).ToArray();
        Assert.True(VolumeVoters.Count == Config.VolumeVoters, $"volume group has {VolumeVoters.Count} voters, configured {Config.VolumeVoters} (INV-11)");
        DfsMetadataPolicy metadata = Config.VolumeMetadata();
        if (metadata.Placement == DfsVoterPlacement.Explicit)
            Assert.True(VolumeVoters.SequenceEqual(metadata.ExplicitVoterNodes!.Order(StringComparer.Ordinal)), $"explicit volume voters not honoured: [{string.Join(",", VolumeVoters)}]");
        if (metadata.Placement == DfsVoterPlacement.OnePerZone)
        {
            int zones = VolumeVoters.Select(v => Net.Spec(v).Zone).Distinct(StringComparer.Ordinal).Count();
            Assert.True(zones == VolumeVoters.Count, $"one-per-zone volume voters [{string.Join(",", VolumeVoters)}] span only {zones} zones");
        }

        Note($"control voters [{string.Join(",", ControlVoters)}], volume voters [{string.Join(",", VolumeVoters)}], volume leader {volume.Leader} term {volume.Term}");
    }

    private async Task<string[]> LeaderFirstSeedsAsync()
    {
        string volumeLeader = await VolumeLeaderAsync();
        string controlLeader = await ControlLeaderAsync();
        return new[] { volumeLeader, controlLeader }.Concat(Net.NodeNames).Distinct(StringComparer.Ordinal).ToArray();
    }

    private async Task PrepareProbeFilesAsync()
    {
        DfsVolume v = Client(Options.ProbeClients[0]).GetVolume(Volume);
        DfsVolumeInfo info = await v.GetInfoAsync(Ct);
        Assert.Equal(Config.Policy(), info.DefaultPolicy);
        Assert.Equal(Config.VolumeVoters, info.Metadata.Voters);
        Policy = info.DefaultPolicy;
        InlineThreshold = info.InlineThreshold;

        await v.Directories.CreateDirectoryAsync(ProbeRoot, Ct);
        _small = TestData.Bytes(Cluster.Seed ^ 0x5341UL, Options.SmallLength);
        _large = TestData.Bytes(Cluster.Seed ^ 0x4C41UL, (2 * Timings.ChunkSize) + 1234);
        await v.Files.WriteAllBytesAsync(SmallPath, _small, Ct);
        await v.Files.WriteAllBytesAsync(LargePath, _large, Ct);
        foreach (string client in Options.ProbeClients)
            await v.Files.WriteAllBytesAsync(LogPath(client), ReadOnlyMemory<byte>.Empty, Ct);

        // On a partial link mesh, repair between non-adjacent nodes is not specified, so the replica target is only
        // required (and waited for) on a fully connected mesh; holders are then recorded as found.
        bool full = Net.IsFullyConnected;
        if (full)
            await Cluster.WaitForRepairAsync(Volume, Options.RecoveryTimeout, Ct);

        _smallInline = MeshOracle.StoresInline(_small.Length, InlineThreshold, Policy, VolumeVoterSpecs);
        FileInspection small = await Cluster.Inspect.GetFileAsync(Volume, SmallPath)
            ?? throw new InvalidOperationException(SmallPath + " missing from the leader's applied state");
        Assert.True(small.IsInline == _smallInline,
            $"{SmallPath} ({_small.Length} bytes, threshold {InlineThreshold}, policy ack {Policy.DurableCopiesBeforeAck}/{Policy.DurableZonesBeforeAck}, " +
            $"metadata guarantees {MeshOracle.GuaranteedMetadataDurability(VolumeVoterSpecs)}) must {(_smallInline ? string.Empty : "not ")}be inline (ARCHITECTURE §5.4, LAY-002)");
        _smallHolders = small.IsInline ? [] : await HoldersAsync(small, full);

        FileInspection large = await Cluster.Inspect.GetFileAsync(Volume, LargePath)
            ?? throw new InvalidOperationException(LargePath + " missing from the leader's applied state");
        Assert.False(large.IsInline, $"{LargePath} ({_large.Length} bytes) exceeds the inline threshold and must be extent-backed");
        _largeHolders = await HoldersAsync(large, full);
        Note($"probe files: small inline={small.IsInline}, large objects={_largeHolders.Count} holders [{string.Join(" | ", _largeHolders.Select(h => string.Join(",", h)))}]");
    }

    private async Task<IReadOnlyList<IReadOnlyList<string>>> HoldersAsync(FileInspection file, bool requireTarget)
    {
        var result = new List<IReadOnlyList<string>>();
        foreach (ObjectId obj in file.Extents.Select(e => e.Object).Distinct())
        {
            IReadOnlyList<string> holders = await Cluster.Inspect.GetObjectHoldersAsync(obj);
            (int copies, int zones) = await Cluster.Inspect.CountDurableDomainsAsync(obj);
            Assert.True(copies >= Policy.DurableCopiesBeforeAck && zones >= Policy.DurableZonesBeforeAck,
                $"object {obj} of {file.Path} has {copies} copies in {zones} zones right after an acknowledged write, ack policy {Policy.DurableCopiesBeforeAck}/{Policy.DurableZonesBeforeAck} (INV-01, INV-11)");
            if (requireTarget)
            {
                Assert.True(copies >= Policy.TargetCopies && zones >= Policy.TargetDistinctZones,
                    $"object {obj} of {file.Path} has {copies} copies in {zones} zones after repair, target {Policy.TargetCopies}/{Policy.TargetDistinctZones} (INV-11, RCV-07)");
            }
            result.Add(holders.Order(StringComparer.Ordinal).ToArray());
        }

        Assert.True(result.Count > 0, $"{file.Path} is extent-backed but references no objects");
        return result;
    }

    private sealed class ProbeWrite(string path, byte[] data)
    {
        public string Path { get; } = path;
        public byte[] Data { get; } = data;
        public OpOutcome Status { get; set; } = OpOutcome.Unknown;
        public DfsOperationId? OperationId { get; set; }
    }

    private sealed class ProbeAppend(string client, byte[] data)
    {
        public string Client { get; } = client;
        public byte[] Data { get; } = data;
        public OpOutcome Status { get; set; } = OpOutcome.Unknown;
        public int Order { get; set; } = int.MaxValue;
    }
}
