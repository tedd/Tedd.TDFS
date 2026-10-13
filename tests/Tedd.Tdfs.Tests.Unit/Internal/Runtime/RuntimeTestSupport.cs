using System.Buffers;
using System.Buffers.Binary;
using System.Globalization;
using System.Security.Cryptography;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Server.Control;
using Tedd.Tdfs.Server.Raft;
using Tedd.Tdfs.Server.Runtime;
using Tedd.Tdfs.Server.Volumes;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Tests.Unit.Internal.Runtime;

/// <summary>
/// Trivial deterministic volume state machine used only by the runtime tests (the real VolumeStateMachine lives with the
/// volume service): counts applied commands and chains their SHA-256.
/// </summary>
internal sealed class TestVolumeStateMachine : IReplicatedStateMachine
{
    private byte[] _hash;

    public TestVolumeStateMachine(VolumeCreation creation)
    {
        Creation = creation;
        _hash = SHA256.HashData(creation.Id.Value.ToByteArray());
    }

    public VolumeCreation Creation { get; }

    public string GroupName => Creation.Name;

    public ulong AppliedIndex { get; private set; }

    public long Count { get; private set; }

    public string Hash => Convert.ToHexStringLower(_hash);

    public List<string> Applied { get; } = [];

    public ApplyOutcome Apply(ulong index, ulong term, ReadOnlyMemory<byte> command)
    {
        Count++;
        _hash = SHA256.HashData([.. _hash, .. command.Span]);
        AppliedIndex = index;
        Applied.Add(System.Text.Encoding.UTF8.GetString(command.Span));
        return ApplyOutcome.Success(Count);
    }

    public void WriteCheckpoint(IBufferWriter<byte> output)
    {
        Span<byte> header = output.GetSpan(8 + 32);
        BinaryPrimitives.WriteInt64LittleEndian(header, Count);
        _hash.CopyTo(header[8..]);
        output.Advance(8 + 32);
    }

    public void RestoreCheckpoint(ReadOnlySequence<byte> data, ulong index, ulong term)
    {
        byte[] bytes = data.ToArray();
        Count = BinaryPrimitives.ReadInt64LittleEndian(bytes);
        _hash = bytes[8..40];
        AppliedIndex = index;
        Applied.Clear();
    }
}

/// <summary>Factory of <see cref="TestVolumeStateMachine"/> (injected through <c>NodeEnvironment.VolumeStateMachines</c>).</summary>
internal sealed class TestVolumeStateMachineFactory : IVolumeStateMachineFactory
{
    public static readonly TestVolumeStateMachineFactory Instance = new();

    public IReplicatedStateMachine Create(VolumeHostContext context) => new TestVolumeStateMachine(context.Creation);
}

/// <summary>Collects node log lines (dumped by failing tests).</summary>
internal sealed class CollectingNodeLog : INodeLog
{
    private readonly System.Collections.Concurrent.ConcurrentQueue<string> _lines = new();

    public NodeLogLevel MinimumLevel { get; set; } = NodeLogLevel.Debug;

    public IReadOnlyList<string> Lines => [.. _lines];

    public void Write(NodeLogLevel level, string node, string category, string message)
    {
        if (level >= MinimumLevel)
            _lines.Enqueue($"{DateTime.UtcNow:HH:mm:ss.fff} {level} {node} [{category}] {message}");
    }

    public string Dump(int max = 400) => string.Join(System.Environment.NewLine, Lines.TakeLast(max));
}

/// <summary>Records every fault-site hit; an optional callback may delay, throw or drop replies.</summary>
internal sealed class RecordingFaultInjector : IFaultInjector
{
    private readonly System.Collections.Concurrent.ConcurrentQueue<FaultContext> _hits = new();

    public bool IsEnabled => true;

    public Func<FaultContext, ValueTask>? OnHit { get; set; }

    public IReadOnlyList<FaultContext> Hits => [.. _hits];

    public ValueTask HitAsync(FaultContext context, CancellationToken ct = default)
    {
        _hits.Enqueue(context);
        return OnHit?.Invoke(context) ?? ValueTask.CompletedTask;
    }

    public void Clear() => _hits.Clear();
}

/// <summary>Isolates network identities: every message to or from an isolated name is dropped and no connection is opened.</summary>
internal sealed class TestLinkPolicy : ILinkPolicy
{
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, bool> _isolated = new(StringComparer.Ordinal);

    public void Isolate(string name) => _isolated[name] = true;

    public void Heal(string name) => _isolated.TryRemove(name, out _);

    public LinkDecision Decide(string fromName, string toName, LinkDirection direction, ReadOnlySpan<byte> payloadPrefix)
        => IsCut(fromName, toName) ? LinkDecision.Drop : LinkDecision.Deliver;

    public bool CanConnect(string fromName, string toName) => !IsCut(fromName, toName);

    private bool IsCut(string from, string to) => _isolated.ContainsKey(from) || _isolated.ContainsKey(to);
}

/// <summary>One node of a <see cref="RuntimeCluster"/>.</summary>
internal sealed class RuntimeNode
{
    public required string Name { get; init; }

    public required int Index { get; init; }

    public required NodeId Id { get; init; }

    public required DfsTopologyLabels Labels { get; init; }

    public required SimulatedMedium Medium { get; set; }

    public required NodeOptions Options { get; set; }

    public NodeHost? Host { get; set; }

    /// <summary>Number of process starts (seeds each start's randomness differently).</summary>
    public int Starts { get; set; }

    public string Endpoint => "mem://" + Name;
}

/// <summary>
/// Multi-node runtime harness for unit tests: real <see cref="ThreadNodeLoop"/>s, an <see cref="InMemoryNetwork"/>, one
/// <see cref="SimulatedMedium"/> per node, a static bootstrap (or seed join), and raw protocol clients.
/// </summary>
internal sealed class RuntimeCluster : IAsyncDisposable
{
    public const string AdminPrincipal = "admin";
    public const string AdminSecret = "admin-secret-1";
    public static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(30);

    private readonly List<RuntimeNode> _nodes = [];
    private readonly List<RawClient> _clients = [];
    private readonly ulong _seed;
    private int _clientCounter;

    public RuntimeCluster(int nodeCount, int controlVoters = 3, NodeTimings? timings = null, ulong seed = 1, IReadOnlyList<string>? zones = null)
    {
        Current = this;
        _seed = seed;
        Timings = timings ?? NodeTimings.Fast;
        var random = new SeededRandom(seed);
        ClusterId = random.NextGuid();
        Secret = random.NextBytes(32);
        Network = new InMemoryNetwork(Links, TimeProvider.System);
        for (int i = 0; i < nodeCount; i++)
            _nodes.Add(CreateNode(i, zones is null ? "z" + (i + 1).ToString(CultureInfo.InvariantCulture) : zones[i], random));
        int voters = Math.Min(controlVoters, nodeCount);
        Bootstrap = new ClusterBootstrap(ClusterId, "runtime-test",
            _nodes.Select(n => new BootstrapMember(n.Id, n.Name, n.Labels, [n.Endpoint])).ToArray(),
            _nodes.Take(voters).Select(n => n.Name).ToArray(),
            [new BootstrapPrincipal(AdminPrincipal, AdminSecret, true), new BootstrapPrincipal("reader", "reader-secret-1", false)]);
        foreach (RuntimeNode node in _nodes)
            node.Options = node.Options with { Bootstrap = Bootstrap };
    }

    /// <summary>The cluster of the running test (log dumps on timeouts).</summary>
    public static RuntimeCluster? Current { get => s_current.Value; private set => s_current.Value = value; }

    private static readonly AsyncLocal<RuntimeCluster?> s_current = new();

    public Guid ClusterId { get; }

    public byte[] Secret { get; }

    public NodeTimings Timings { get; }

    public InMemoryNetwork Network { get; }

    /// <summary>Link rules of <see cref="Network"/> (isolate and heal nodes).</summary>
    public TestLinkPolicy Links { get; } = new();

    public CollectingNodeLog Log { get; } = new();

    /// <summary>Fault injector handed to every node started afterwards.</summary>
    public IFaultInjector Faults { get; set; } = NoFaults.Instance;

    /// <summary>
    /// Volume state machine factory handed to every node started afterwards: the trivial test machine by default, null for the
    /// production volume state machine and service.
    /// </summary>
    public IVolumeStateMachineFactory? VolumeStateMachines { get; set; } = TestVolumeStateMachineFactory.Instance;

    public ClusterBootstrap Bootstrap { get; }

    public IReadOnlyList<RuntimeNode> Nodes => _nodes;

    public RuntimeNode this[int index] => _nodes[index];

    public static DfsCredentials AdminCredentials => new(AdminPrincipal, AdminSecret);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Adds a node that is not part of the bootstrap (seed join).</summary>
    public RuntimeNode AddJoiningNode(string zone, IReadOnlyList<string> seeds)
    {
        RuntimeNode node = CreateNode(_nodes.Count, zone, new SeededRandom(_seed + 1000 + (ulong)_nodes.Count));
        node.Options = node.Options with { Bootstrap = null, JoinSeeds = seeds };
        _nodes.Add(node);
        return node;
    }

    public async Task StartAllAsync()
    {
        await Task.WhenAll(_nodes.Select(StartAsync));
    }

    public async Task StartAsync(RuntimeNode node)
    {
        node.Starts++;
        node.Host = await NodeHost.StartAsync(node.Options, Environment(node), Ct);
    }

    public NodeEnvironment Environment(RuntimeNode node) => new()
    {
        Medium = node.Medium,
        TransportFactory = Network.Factory,
        Time = TimeProvider.System,
        Random = new SeededRandom(_seed * 7919 + (ulong)node.Index * 104729 + (ulong)node.Starts * 31),
        VolumeStateMachines = VolumeStateMachines,
        Log = Log,
        Faults = Faults,
    };

    /// <summary>Crashes a node: the loop stops, the transport is torn down and unflushed disk state is lost.</summary>
    public void Kill(RuntimeNode node)
    {
        node.Host?.Kill();
        node.Host = null;
        node.Medium.Crash(CrashMode.LoseUnflushed);
    }

    public async Task StopAsync(RuntimeNode node)
    {
        if (node.Host is { } host)
            await host.StopAsync(Ct);
        node.Host = null;
    }

    public NodeHost Host(int index) => _nodes[index].Host ?? throw new InvalidOperationException($"{_nodes[index].Name} is not running.");

    public IEnumerable<RuntimeNode> Running => _nodes.Where(n => n.Host is not null);

    /// <summary>Polls <paramref name="condition"/> until true.</summary>
    public static async Task WaitUntilAsync(Func<Task<bool>> condition, string because, TimeSpan? timeout = null)
    {
        DateTime deadline = DateTime.UtcNow + (timeout ?? DefaultTimeout);
        while (true)
        {
            Exception? last = null;
            try
            {
                if (await condition())
                    return;
            }
            catch (Exception ex) when (ex is DfsException or TimeoutException or IOException or UnauthorizedAccessException or NodeLoopStoppedException)
            {
                last = ex;
            }

            if (DateTime.UtcNow > deadline)
            {
                throw new TimeoutException($"Timed out waiting until {because}." + (last is null ? string.Empty : " Last error: " + last.Message)
                    + (Current?.Log is { } log ? System.Environment.NewLine + log.Dump() : string.Empty));
            }
            await Task.Delay(20, Ct);
        }
    }

    /// <summary>The running node that leads <paramref name="group"/> (waits for one).</summary>
    public async Task<RuntimeNode> WaitForLeaderAsync(string group, IEnumerable<RuntimeNode>? among = null, TimeSpan? timeout = null)
    {
        RuntimeNode? leader = null;
        await WaitUntilAsync(async () =>
        {
            foreach (RuntimeNode node in (among ?? Running).Where(n => n.Host is not null).ToArray())
            {
                GroupInspection? g = await node.Host!.Inspect.GetGroupAsync(group);
                if (g?.Role == Consensus.RaftRole.Leader)
                {
                    leader = node;
                    return true;
                }
            }

            return false;
        }, $"group '{group}' has a leader", timeout);
        return leader!;
    }

    /// <summary>Waits until every running node is registered with its current incarnation.</summary>
    public Task WaitForRegistrationAsync() => WaitUntilAsync(async () =>
    {
        foreach (RuntimeNode node in Running.ToArray())
        {
            if (!await node.Host!.InvokeAsync(s => s.Control.IsSelfRegistered))
                return false;
        }

        return true;
    }, "every running node registered its incarnation");

    /// <summary>Connects a raw client authenticated as <paramref name="credentials"/> to <paramref name="node"/>.</summary>
    public async Task<RawClient> ConnectAsync(RuntimeNode node, DfsCredentials? credentials = null, Guid? expectedCluster = null)
    {
        string name = "client:c" + Interlocked.Increment(ref _clientCounter).ToString(CultureInfo.InvariantCulture);
        RawClient client = await RawClient.ConnectAsync(Network, name, node.Endpoint, expectedCluster ?? ClusterId, credentials ?? AdminCredentials);
        _clients.Add(client);
        return client;
    }

    /// <summary>Creates a volume through the control leader and waits until its group has a leader.</summary>
    public async Task<VolumeEntry> CreateVolumeAsync(string name, int voters = 3)
    {
        RuntimeNode leader = await WaitForLeaderAsync("control");
        await using RawClient admin = await RawClient.ConnectAsync(Network, "client:admin-create-" + name, leader.Endpoint, ClusterId, AdminCredentials);
        var policy = new DfsStoragePolicy
        {
            TargetCopies = Math.Min(3, _nodes.Count),
            TargetDistinctZones = 1,
            DurableCopiesBeforeAck = 1,
            DurableZonesBeforeAck = 1,
        };
        var spec = new AdminVolumeSpecBody(name, new DfsMetadataPolicy { Voters = voters }, policy, new Dictionary<string, DfsStoragePolicy>(), 4096,
            new DfsVolumeLimits());
        AdminVolumeInfoBody info = await admin.AdminAsync<AdminVolumeSpecBody, AdminVolumeInfoBody>(AdminOp.CreateVolume, spec);
        await WaitForLeaderAsync(name);
        return info.Volume;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (RawClient client in _clients)
            await client.DisposeAsync();
        foreach (RuntimeNode node in _nodes)
        {
            if (node.Host is { } host)
            {
                try
                {
                    await host.StopAsync().WaitAsync(TimeSpan.FromSeconds(20));
                }
                catch (Exception ex) when (ex is TimeoutException or NodeLoopStoppedException or OperationCanceledException)
                {
                    host.Kill();
                }

                node.Host = null;
            }
        }
    }

    private RuntimeNode CreateNode(int index, string zone, IRandomSource random)
    {
        string name = "n" + index.ToString(CultureInfo.InvariantCulture);
        var labels = new DfsTopologyLabels("r1", zone, "rk1", "h" + index.ToString(CultureInfo.InvariantCulture), "d1");
        var medium = new SimulatedMedium(name, 1L << 30, new SeededRandom(_seed + (ulong)index + 17));
        return new RuntimeNode
        {
            Name = name,
            Index = index,
            Id = NodeId.New(random),
            Labels = labels,
            Medium = medium,
            Options = new NodeOptions
            {
                Name = name,
                Labels = labels,
                ListenEndpoint = "mem://" + name,
                DataDirectory = "node",
                ClusterSecret = Secret,
                Timings = Timings,
                CapacityBytes = 1L << 30,
                ReservedBytes = 1 << 20,
            },
        };
    }
}

/// <summary>A raw protocol client: in-memory connection, principal handshake, typed calls.</summary>
internal sealed class RawClient : IAsyncDisposable
{
    private static readonly TimeSpan CallTimeout = TimeSpan.FromSeconds(20);

    private readonly IRpcTransport _transport;
    private int _requests;

    private RawClient(IRpcTransport transport, IRpcConnection connection, HandshakeOutcome outcome)
    {
        _transport = transport;
        Connection = connection;
        Outcome = outcome;
    }

    public IRpcConnection Connection { get; }

    public HandshakeOutcome Outcome { get; }

    /// <summary>Connects and authenticates; failures dispose the transport and propagate.</summary>
    public static async Task<RawClient> ConnectAsync(InMemoryNetwork network, string clientName, string endpoint, Guid expectedCluster, DfsCredentials credentials)
    {
        IRpcTransport transport = network.Factory.Create(clientName, "mem://" + clientName.Replace(':', '-'));
        try
        {
            IRpcConnection connection = await transport.ConnectAsync(endpoint);
            HandshakeOutcome outcome = await HandshakeClient.AuthenticatePrincipalAsync(connection, expectedCluster, clientName, credentials,
                new SeededRandom((ulong)clientName.GetHashCode(StringComparison.Ordinal)), CallTimeout);
            return new RawClient(transport, connection, outcome);
        }
        catch
        {
            await transport.DisposeAsync();
            throw;
        }
    }

    /// <summary>Sends a request and returns the decoded reply (an ErrorReply is returned as a message).</summary>
    public async Task<IWireMessage> CallAsync(IWireMessage request)
    {
        ReadOnlyMemory<byte> reply = await Connection.CallAsync(MessageCodec.Encode(request), CallTimeout);
        return MessageCodec.Decode(reply.Span);
    }

    /// <summary>Sends a request; an error reply is thrown as its exception.</summary>
    public async Task<TReply> CallOrThrowAsync<TReply>(IWireMessage request) where TReply : IWireMessage
    {
        IWireMessage reply = await CallAsync(request);
        if (reply is ErrorReplyMessage error)
            throw error.ToException();
        return (TReply)reply;
    }

    /// <summary>An administrative request with a fresh request id.</summary>
    public async Task<TResult> AdminAsync<TBody, TResult>(AdminOp op, TBody body)
        where TBody : IWireRecord<TBody>
        where TResult : IWireRecord<TResult>
    {
        var requestId = new UInt128(0xC0FFEE, (ulong)Interlocked.Increment(ref _requests) + (ulong)Environment.TickCount64);
        AdminResultMessage result = await CallOrThrowAsync<AdminResultMessage>(AdminRequestMessage.Create(op, requestId, body));
        return result.ReadBody<TResult>();
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            await Connection.DisposeAsync();
        }
        catch (Exception ex) when (ex is RpcConnectionException or ObjectDisposedException)
        {
            // Already closed.
        }

        await _transport.DisposeAsync();
    }
}
