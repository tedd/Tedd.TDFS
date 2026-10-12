using System.Globalization;
using System.Text;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Testing.Simulation;

/// <summary>Configuration of <see cref="SimPingWorkload"/>.</summary>
public sealed record SimPingWorkloadOptions
{
    /// <summary>Number of nodes (at least 2).</summary>
    public int Nodes { get; init; } = 3;

    /// <summary>Pings each node sends (to random peers).</summary>
    public int RoundsPerNode { get; init; } = 12;

    /// <summary>Echo calls made by the SDK-style client.</summary>
    public int ClientCalls { get; init; } = 6;

    /// <summary>Scheduler mode.</summary>
    public SimScheduleMode Mode { get; init; } = SimScheduleMode.Random;

    /// <summary>Base interval between pings (each wait adds up to one more interval of seeded jitter).</summary>
    public TimeSpan PingInterval { get; init; } = TimeSpan.FromMilliseconds(4);

    /// <summary>Timeout of every call.</summary>
    public TimeSpan CallTimeout { get; init; } = TimeSpan.FromMilliseconds(250);

    /// <summary>Record one trace line per executed work item.</summary>
    public bool RecordSteps { get; init; } = true;

    /// <summary>Network options.</summary>
    public SimNetworkOptions Network { get; init; } = new();

    /// <summary>
    /// Optional per-run link policy factory (for example one that drops messages). It receives the run's scheduler so it
    /// can draw from <see cref="SimScheduler.ForkRandom"/> and stay deterministic.
    /// </summary>
    public Func<SimScheduler, ILinkPolicy>? LinkPolicy { get; init; }

    /// <summary>Virtual-time budget of the run.</summary>
    public TimeSpan VirtualTimeout { get; init; } = TimeSpan.FromMinutes(5);

    /// <summary>Validates the options.</summary>
    public void Validate()
    {
        if (Nodes < 2)
            throw new ArgumentException("At least two nodes are required.", nameof(Nodes));
        if (RoundsPerNode < 0 || ClientCalls < 0)
            throw new ArgumentException("Counts must not be negative.");
        if (PingInterval <= TimeSpan.Zero || CallTimeout <= TimeSpan.Zero || VirtualTimeout <= TimeSpan.Zero)
            throw new ArgumentException("Intervals and timeouts must be positive.");
        ArgumentNullException.ThrowIfNull(Network);
        Network.Validate();
    }
}

/// <summary>
/// A toy multi-node workload for determinism self-tests (HAR-001): nodes on <see cref="SimNodeLoop"/>s with drifting,
/// offset clocks ping random peers over a <see cref="SimNetwork"/> (requests, replies, one-way gossip, timeouts) while an
/// SDK-style client (<c>ConfigureAwait(false)</c>) makes echo calls from <see cref="SimScheduler.RunAsync(Func{Task}, TimeSpan?)"/>.
/// Node code follows the server rules (plain awaits that resume on the loop) and asserts it never runs off its loop.
/// </summary>
public static class SimPingWorkload
{
    /// <summary>Runs the workload with <paramref name="seed"/> and returns everything observable about the run.</summary>
    public static SimRunRecord Run(ulong seed, SimPingWorkloadOptions? options = null)
    {
        options ??= new SimPingWorkloadOptions();
        options.Validate();
        var scheduler = new SimScheduler(seed, new SimSchedulerOptions { Mode = options.Mode, RecordSteps = options.RecordSteps });
        var network = new SimNetwork(scheduler, options.Network, options.LinkPolicy?.Invoke(scheduler));
        IRandomSource setup = scheduler.ForkRandom("ping-workload/setup");
        string[] names = Enumerable.Range(0, options.Nodes).Select(i => "n" + i.ToString(CultureInfo.InvariantCulture)).ToArray();
        var nodes = new List<PingNode>(names.Length);
        foreach (string name in names)
        {
            SimTimeProvider clock = scheduler.CreateClock(name, new SimClockOptions
            {
                WallOffset = TimeSpan.FromMilliseconds(setup.Next(-500, 501)),
                Rate = 1.0 + (setup.Next(0, 21) - 10) / 1000.0,
            });
            SimNodeLoop loop = scheduler.LoopFactory.Create(name);
            IRpcTransport transport = network.Create(name, RpcAddress.MemoryScheme + "://" + name);
            string[] peers = names.Where(n => !string.Equals(n, name, StringComparison.Ordinal)).ToArray();
            nodes.Add(new PingNode(scheduler, name, loop, transport, clock, scheduler.ForkRandom("ping-workload/" + name), peers, options));
        }

        foreach (PingNode node in nodes)
            node.Start();

        string client = scheduler.Run(() => RunClientAsync(scheduler, network, names, options), options.VirtualTimeout);
        scheduler.RunUntil(() => nodes.TrueForAll(n => n.IsFinished), options.VirtualTimeout, "every ping node to finish");
        foreach (PingNode node in nodes)
            node.ThrowIfFailed();

        var digest = new StringBuilder();
        foreach (PingNode node in nodes)
            digest.Append(node.Summary).Append('|');
        digest.Append(client);
        digest.Append(CultureInfo.InvariantCulture, $"|messages={network.MessagesSent},dropped={network.MessagesDropped}");
        return SimRunRecord.Capture(scheduler, digest.ToString());
    }

    private static byte[] Utf8(string text) => Encoding.UTF8.GetBytes(text);

    private static string Text(ReadOnlyMemory<byte> bytes) => Encoding.UTF8.GetString(bytes.Span);

    // SDK-style client: ConfigureAwait(false) everywhere; continuations run inline on the simulation thread.
    private static async Task<string> RunClientAsync(SimScheduler scheduler, SimNetwork network, string[] nodes, SimPingWorkloadOptions options)
    {
        IRandomSource random = scheduler.ForkRandom("ping-workload/client");
        IRpcTransport transport = network.Create(scheduler.ClientProcess.Name, string.Empty);
        int ok = 0;
        int failed = 0;
        for (int i = 1; i <= options.ClientCalls; i++)
        {
            string node = nodes[random.Next(nodes.Length)];
            try
            {
                IRpcConnection connection = await transport.ConnectAsync(RpcAddress.MemoryScheme + "://" + node).ConfigureAwait(false);
                ReadOnlyMemory<byte> reply = await connection.CallAsync(Utf8($"echo client {i}"), options.CallTimeout).ConfigureAwait(false);
                scheduler.AddTrace($"client echo {i} via {node}: '{Text(reply)}'");
                await connection.DisposeAsync().ConfigureAwait(false);
                ok++;
            }
            catch (Exception ex) when (ex is TimeoutException or RpcConnectionException)
            {
                failed++;
                scheduler.AddTrace($"client echo {i} via {node} failed: {ex.GetType().Name}");
            }
        }

        await transport.DisposeAsync().ConfigureAwait(false);
        return string.Create(CultureInfo.InvariantCulture, $"client:ok={ok},failed={failed}");
    }

    private sealed class PingNode : IRpcHandler
    {
        private readonly SimScheduler _scheduler;
        private readonly INodeLoop _loop;
        private readonly IRpcTransport _transport;
        private readonly SimTimeProvider _clock;
        private readonly IRandomSource _random;
        private readonly string[] _peers;
        private readonly SimPingWorkloadOptions _options;
        private readonly Dictionary<string, IRpcConnection> _connections = new(StringComparer.Ordinal);
        private Task? _main;
        private int _pongs;
        private int _timeouts;
        private int _failures;
        private int _served;
        private int _gossipSent;
        private int _gossipHeard;
        private bool _done;

        public PingNode(SimScheduler scheduler, string name, INodeLoop loop, IRpcTransport transport, SimTimeProvider clock,
            IRandomSource random, string[] peers, SimPingWorkloadOptions options)
        {
            _scheduler = scheduler;
            Name = name;
            _loop = loop;
            _transport = transport;
            _clock = clock;
            _random = random;
            _peers = peers;
            _options = options;
        }

        public string Name { get; }

        public bool IsFinished => _done || _main is { IsFaulted: true } or { IsCanceled: true };

        public string Summary =>
            string.Create(CultureInfo.InvariantCulture,
                $"{Name}:pongs={_pongs},timeouts={_timeouts},failures={_failures},served={_served},gossip={_gossipSent}/{_gossipHeard},wall={_clock.GetUtcNow().UtcTicks},mono={_clock.GetTimestamp()}");

        public void Start() => _loop.Post(() => _main = RunAsync());

        public void ThrowIfFailed()
        {
            if (_main is { IsFaulted: true } faulted)
                throw new SimulationException($"Ping node {Name} failed.", faulted.Exception!.GetBaseException());
            if (_main is { IsCanceled: true })
                throw new SimulationException($"Ping node {Name} was cancelled.");
        }

        public ValueTask<ReadOnlyMemory<byte>?> HandleAsync(IRpcConnection connection, ReadOnlyMemory<byte> request, bool expectsReply, CancellationToken ct)
        {
            // Called by the transport off the loop: hop onto the loop like NodeHost does.
            byte[] copy = request.ToArray();
            return new ValueTask<ReadOnlyMemory<byte>?>(_loop.InvokeAsync(() => OnMessage(copy, expectsReply)));
        }

        private ReadOnlyMemory<byte>? OnMessage(byte[] request, bool expectsReply)
        {
            EnsureOnLoop();
            string text = Text(request);
            if (!expectsReply)
            {
                _gossipHeard++;
                Trace($"heard '{text}'");
                return null;
            }

            _served++;
            Trace($"served '{text}'");
            return Utf8(string.Create(CultureInfo.InvariantCulture, $"pong {Name} {_served} re:{text}"));
        }

        private async Task RunAsync()
        {
            EnsureOnLoop();
            await _transport.StartAsync(this);
            long intervalTicks = _options.PingInterval.Ticks;
            for (int round = 1; round <= _options.RoundsPerNode; round++)
            {
                long jitter = (long)_random.NextUInt64((ulong)intervalTicks + 1);
                await Task.Delay(TimeSpan.FromTicks(intervalTicks + jitter), _clock);
                EnsureOnLoop();
                string peer = _peers[_random.Next(_peers.Length)];
                IRpcConnection connection;
                try
                {
                    connection = await GetConnectionAsync(peer);
                }
                catch (RpcConnectionException ex)
                {
                    _failures++;
                    Trace($"connect to {peer} failed: {ex.Message}");
                    continue;
                }

                EnsureOnLoop();
                long started = _clock.GetTimestamp();
                try
                {
                    ReadOnlyMemory<byte> reply = await connection.CallAsync(Utf8(string.Create(CultureInfo.InvariantCulture, $"ping {Name} {round}")), _options.CallTimeout);
                    EnsureOnLoop();
                    _pongs++;
                    TimeSpan rtt = _clock.GetElapsedTime(started);
                    Trace($"round {round}: '{Text(reply)}' from {peer} rtt={SimScheduler.FormatTime(rtt)}");
                }
                catch (TimeoutException)
                {
                    EnsureOnLoop();
                    _timeouts++;
                    Trace($"round {round}: ping to {peer} timed out");
                }
                catch (RpcConnectionException ex)
                {
                    EnsureOnLoop();
                    _failures++;
                    _connections.Remove(peer);
                    Trace($"round {round}: ping to {peer} failed: {ex.Message}");
                }

                if (round % 3 == 0 && connection.IsOpen)
                {
                    await connection.SendAsync(Utf8(string.Create(CultureInfo.InvariantCulture, $"gossip {Name} {round}")));
                    _gossipSent++;
                }
            }

            _done = true;
            Trace("done");
        }

        private async Task<IRpcConnection> GetConnectionAsync(string peer)
        {
            if (_connections.TryGetValue(peer, out IRpcConnection? existing) && existing.IsOpen)
                return existing;
            IRpcConnection connection = await _transport.ConnectAsync(RpcAddress.MemoryScheme + "://" + peer);
            EnsureOnLoop();
            _connections[peer] = connection;
            return connection;
        }

        private void EnsureOnLoop()
        {
            if (!_loop.IsOnLoop)
                throw new InvalidOperationException($"Ping node {Name}: node code ran off its loop.");
        }

        private void Trace(string message) => _scheduler.AddTrace(Name + " " + message);
    }
}
