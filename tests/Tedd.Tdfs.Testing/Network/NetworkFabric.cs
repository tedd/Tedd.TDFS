using System.Net;
using System.Runtime.CompilerServices;
using System.Security.Cryptography.X509Certificates;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Testing.Simulation;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Testing;

/// <summary>How a transport created by the fabric identifies and authenticates itself (for replays and attempt accounting).</summary>
internal sealed record TransportIdentity
{
    /// <summary>Cluster that created the transport (its traffic towards that cluster is harness-owned when <see cref="IsHarness"/>).</summary>
    public required TestCluster? Owner { get; init; }

    /// <summary>The process clock: call timeouts in the simulation, QUIC timers and silent-death timeouts.</summary>
    public TimeProvider? Clock { get; init; }

    /// <summary>True for the harness's own traffic (admin client, harness admin channel): excluded from attempt counts.</summary>
    public bool IsHarness { get; init; }

    /// <summary>Principal credentials the transport's connections authenticate with (clients), for replays on fresh connections.</summary>
    public DfsCredentials? Credentials { get; init; }

    /// <summary>Cluster the credentials belong to (expected cluster id of replays).</summary>
    public Guid ClusterId { get; init; }

    /// <summary>QUIC: listener certificate (null for connect-only transports).</summary>
    public X509Certificate2? Certificate { get; init; }

    /// <summary>QUIC: trusted SPKI pins of outbound connections.</summary>
    public IReadOnlyList<byte[]> TrustedPins { get; init; } = [];
}

/// <summary>
/// A transport fabric shared by a test cluster and the recovery clusters it starts: the inner network (simulated, in-memory
/// or QUIC), the harness link policy, and the interposition layer every transport of the fabric is wrapped in (holds, reply
/// drops, truncation, tampering, silent connection death, read-result limits, connection and attempt accounting, replay).
/// </summary>
internal sealed class NetworkFabric
{
    private readonly object _gate = new();
    private readonly IRandomSource _random;
    private readonly Dictionary<string, string> _endpointNames = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, string> _nameEndpoints = new(StringComparer.Ordinal);
    private readonly HashSet<string> _names = new(StringComparer.Ordinal);
    private readonly Dictionary<string, long> _attempts = new(StringComparer.Ordinal);
    private readonly List<HarnessTransport> _transports = [];
    private readonly Dictionary<string, HarnessTransportFactory> _factories = new(StringComparer.Ordinal);
    private readonly List<HarnessConnection> _connections = [];
    private readonly Dictionary<IRpcConnection, HarnessConnection> _serverConnections = new(ReferenceEqualityComparer.Instance);
    private readonly Dictionary<IRpcConnection, HarnessConnection> _clientConnections = new(ReferenceEqualityComparer.Instance);
    private readonly List<MessageHold> _holds = [];
    private readonly List<ReadResultLimitRule> _readLimits = [];
    private readonly List<Action<string>> _traces = [];
    private readonly List<Func<VolumeId, string?>> _volumeNames = [];
    private readonly ConditionalWeakTable<CapturedMessage, HarnessConnection> _origins = [];
    private readonly Dictionary<(string Node, string Volume), IReadOnlySet<ObjectId>> _inventoryClaims = [];
    private readonly Dictionary<int, DatagramRelay> _relays = [];
    private int _relaySeed;
    private int _activeHolds;
    private int _activeReadLimits;
    private int _activeClaims;

    public NetworkFabric(ClusterRuntime runtime, TransportKind kind, IRandomSource random)
    {
        Runtime = runtime;
        Kind = kind;
        _random = random.Fork("fabric/replay");
        Policy = new HarnessLinkPolicy(runtime);
        if (runtime.Scheduler is { } scheduler)
        {
            if (kind != TransportKind.InMemory)
                throw new ArgumentException("Deterministic mode requires the in-memory (simulated) transport.", nameof(kind));
            Sim = new SimNetwork(scheduler, null, Policy);
        }
        else if (kind == TransportKind.InMemory)
        {
            Memory = new InMemoryNetwork(Policy, TimeProvider.System);
            Policy.ApplyLatency = true;
        }
        else
        {
            Policy.ApplyLatency = true;
        }

        Log = new MessageLog(Policy);
    }

    /// <summary>The execution context.</summary>
    public ClusterRuntime Runtime { get; }

    /// <summary>The transport kind.</summary>
    public TransportKind Kind { get; }

    /// <summary>The link policy every inner transport consults.</summary>
    public HarnessLinkPolicy Policy { get; }

    /// <summary>The simulated network (deterministic mode).</summary>
    public SimNetwork? Sim { get; }

    /// <summary>The in-memory network (realtime in-memory transport).</summary>
    public InMemoryNetwork? Memory { get; }

    /// <summary>The fabric-wide message log.</summary>
    public MessageLog Log { get; }

    /// <summary>
    /// True when every QUIC connection of the fabric runs through its own <see cref="DatagramRelay"/>
    /// (<see cref="TestClusterOptions.QuicDatagramInterposer"/>), so tampering happens on encrypted datagrams (SEC-011).
    /// </summary>
    public bool UsesDatagramRelays { get; set; }

    // -------------------------------------------------------------------------------------------- datagram relays

    /// <summary>A relay in front of <paramref name="endpoint"/> for one new connection (registered by its back port).</summary>
    internal async Task<DatagramRelay> CreateRelayAsync(string endpoint, CancellationToken ct)
    {
        RpcAddress address = RpcAddress.Parse(endpoint);
        IPAddress ip = IPAddress.TryParse(address.Host, out IPAddress? literal)
            ? literal
            : (await Dns.GetHostAddressesAsync(address.Host, ct).ConfigureAwait(false)).First();
        var relay = new DatagramRelay(new IPEndPoint(ip, address.Port), Trace, Interlocked.Increment(ref _relaySeed));
        lock (_gate)
            _relays[relay.BackPort] = relay;
        return relay;
    }

    /// <summary>The relay whose back socket is <paramref name="remote"/> (the peer address a relayed connection's target sees).</summary>
    internal DatagramRelay? RelayOfPeer(RpcAddress remote)
    {
        if (!UsesDatagramRelays || !remote.IsQuic)
            return null;
        lock (_gate)
            return _relays.TryGetValue(remote.Port, out DatagramRelay? relay) ? relay : null;
    }

    /// <summary>Stops and forgets a relay (its connection closed).</summary>
    internal void ReleaseRelay(DatagramRelay relay)
    {
        lock (_gate)
        {
            if (_relays.TryGetValue(relay.BackPort, out DatagramRelay? known) && ReferenceEquals(known, relay))
                _relays.Remove(relay.BackPort);
        }

        _ = relay.DisposeAsync().AsTask();
    }

    /// <summary>Stops every relay (fabric disposal).</summary>
    internal async Task DisposeRelaysAsync()
    {
        DatagramRelay[] relays;
        lock (_gate)
        {
            relays = [.. _relays.Values];
            _relays.Clear();
        }

        foreach (DatagramRelay relay in relays)
            await relay.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Arms the relay carrying a transmission of <paramref name="connection"/>'s local side for <paramref name="rule"/>: the
    /// connection's own relay (datagrams towards its target) for a connection this side opened, else the relay of the peer
    /// that opened it (datagrams back towards that peer). False when the connection is not relayed.
    /// </summary>
    internal bool ArmRelay(HarnessConnection connection, MessageRule rule, string description)
    {
        if (connection.Relay is { } own)
        {
            own.Arm(RelayDirection.Forward, rule, description);
            return true;
        }

        if (!connection.IsClientSide && RelayOfPeer(connection.Inner.Remote) is { } peer)
        {
            peer.Arm(RelayDirection.Backward, rule, description);
            return true;
        }

        return false;
    }

    /// <summary>True while a hold, a reply drop, a reply truncation/tamper or a read limit is active (reply interposition needed).</summary>
    public bool NeedsReplyInterposition =>
        Volatile.Read(ref _activeHolds) > 0 || Volatile.Read(ref _activeReadLimits) > 0 || Volatile.Read(ref _activeClaims) > 0
        || Policy.HasActiveRule(MessageRuleKind.DropReply) || Policy.HasActiveRule(MessageRuleKind.Truncate) || Policy.HasActiveRule(MessageRuleKind.Tamper);

    /// <summary>Sets (or, for an empty collection, clears) the false inventory claims of a backup node for a volume.</summary>
    public void SetInventoryClaims(string node, string volume, IReadOnlyCollection<ObjectId> ids)
    {
        lock (_gate)
        {
            if (ids.Count == 0)
                _inventoryClaims.Remove((node, volume));
            else
                _inventoryClaims[(node, volume)] = new HashSet<ObjectId>(ids);
            Volatile.Write(ref _activeClaims, _inventoryClaims.Count);
        }
    }

    /// <summary>Clears every false inventory claim of a backup node (restart).</summary>
    public void ClearInventoryClaims(string node)
    {
        lock (_gate)
        {
            foreach ((string Node, string Volume) key in _inventoryClaims.Keys.Where(k => string.Equals(k.Node, node, StringComparison.Ordinal)).ToList())
                _inventoryClaims.Remove(key);
            Volatile.Write(ref _activeClaims, _inventoryClaims.Count);
        }
    }

    /// <summary>The false inventory claims of a backup node for a volume, or null.</summary>
    internal IReadOnlySet<ObjectId>? InventoryClaimsFor(string node, string? volume)
    {
        if (volume is null || Volatile.Read(ref _activeClaims) == 0)
            return null;
        lock (_gate)
            return _inventoryClaims.TryGetValue((node, volume), out IReadOnlySet<ObjectId>? ids) ? ids : null;
    }

    /// <summary>Attaches a cluster's trace sink (fabric events are recorded in every attached cluster's trace).</summary>
    public void AttachTrace(Action<string> trace)
    {
        lock (_gate)
            _traces.Add(trace);
    }

    /// <summary>Attaches a cluster's volume id → name resolver (read-result limits on <c>GetObject</c>).</summary>
    public void AttachVolumeNames(Func<VolumeId, string?> resolver)
    {
        lock (_gate)
            _volumeNames.Add(resolver);
    }

    /// <summary>Records a fabric event in every attached cluster trace.</summary>
    public void Trace(string message)
    {
        Action<string>[] traces;
        lock (_gate)
            traces = [.. _traces];
        foreach (Action<string> trace in traces)
            trace(message);
    }

    // -------------------------------------------------------------------------------------------- factories

    /// <summary>Creates the (wrapped) transport factory for one identity.</summary>
    public IRpcTransportFactory CreateFactory(TransportIdentity identity)
    {
        ArgumentNullException.ThrowIfNull(identity);
        IRpcTransportFactory inner;
        if (Sim is not null)
        {
            inner = Sim;
        }
        else if (Memory is not null)
        {
            inner = Memory.Factory;
        }
        else
        {
            var options = new QuiclyTransportOptions(identity.Certificate!, identity.TrustedPins, Policy)
            {
                TimeProvider = identity.Clock ?? TimeProvider.System,
                ConnectTimeout = TimeSpan.FromSeconds(10),
            };
            inner = new QuiclyTransportFactory(options);
        }

        return new HarnessTransportFactory(this, inner, identity);
    }

    // -------------------------------------------------------------------------------------------- registries

    /// <summary>Registers the network name behind an endpoint (listening transports, static bootstrap endpoints).</summary>
    public void RegisterEndpoint(string endpoint, string name)
    {
        string key = NormalizeEndpoint(endpoint);
        lock (_gate)
        {
            _endpointNames[key] = name;
            _nameEndpoints[name] = key;
            _names.Add(name);
        }
    }

    /// <summary>The network name registered for an endpoint, or null.</summary>
    public string? NameOfEndpoint(string endpoint)
    {
        string key = NormalizeEndpoint(endpoint);
        lock (_gate)
            return _endpointNames.TryGetValue(key, out string? name) ? name : null;
    }

    /// <summary>The endpoint registered for a network name, or null.</summary>
    public string? EndpointOfName(string name)
    {
        lock (_gate)
            return _nameEndpoints.TryGetValue(name, out string? endpoint) ? endpoint : null;
    }

    /// <summary>True when a transport or endpoint with the name has existed on the fabric.</summary>
    public bool IsKnownName(string name)
    {
        lock (_gate)
            return _names.Contains(name);
    }

    /// <summary>Counts one contact attempt addressed to <paramref name="name"/>.</summary>
    public void CountAttempt(string name)
    {
        lock (_gate)
            _attempts[name] = _attempts.TryGetValue(name, out long count) ? count + 1 : 1;
    }

    /// <summary>Contact attempts addressed to <paramref name="name"/> since the fabric was created.</summary>
    public long AttemptsTo(string name)
    {
        lock (_gate)
            return _attempts.TryGetValue(name, out long count) ? count : 0;
    }

    internal void AddTransport(HarnessTransport transport)
    {
        lock (_gate)
        {
            _transports.Add(transport);
            _names.Add(transport.LocalName);
            _factories[transport.LocalName] = transport.Factory; // replays of the identity outlive its transports
        }
    }

    /// <summary>
    /// Disposes every transport of the fabric created for <paramref name="owner"/> that is still open (clients the test
    /// abandoned, transports of killed processes), so no socket or simulated endpoint outlives its cluster.
    /// </summary>
    internal async Task DisposeTransportsAsync(TestCluster owner)
    {
        HarnessTransport[] owned;
        lock (_gate)
            owned = _transports.Where(t => ReferenceEquals(t.Identity.Owner, owner)).ToArray();
        foreach (HarnessTransport transport in owned)
        {
            try
            {
                await transport.DisposeAsync();
            }
            catch (Exception ex) when (ex is ObjectDisposedException or RpcConnectionException or IOException or InvalidOperationException)
            {
                RemoveTransport(transport); // already torn down by its owner
            }
        }
    }

    internal void RemoveTransport(HarnessTransport transport)
    {
        lock (_gate)
            _transports.Remove(transport);
    }

    internal void AddConnection(HarnessConnection connection)
    {
        lock (_gate)
        {
            _connections.Add(connection);
            if (connection.IsClientSide)
                _clientConnections[connection.Inner] = connection;
        }
    }

    internal void RemoveConnection(HarnessConnection connection)
    {
        lock (_gate)
        {
            _connections.Remove(connection);
            if (connection.IsClientSide)
                _clientConnections.Remove(connection.Inner);
            else
                _serverConnections.Remove(connection.Inner);
        }
    }

    /// <summary>
    /// The wrapper a transport's handler sees for an inner connection a frame arrived on: the connection the transport opened
    /// itself when the frame came back on it (connections carry messages both ways: a node reaches a peer over the
    /// connection that peer opened, so the receiver must see the very connection it authenticated and registered),
    /// otherwise the server-side wrapper of an accepted connection (created on first sight).
    /// </summary>
    internal HarnessConnection GetServerConnection(HarnessTransport owner, IRpcConnection inner)
    {
        lock (_gate)
        {
            if (_clientConnections.TryGetValue(inner, out HarnessConnection? opened))
                return opened;
            if (_serverConnections.TryGetValue(inner, out HarnessConnection? existing))
                return existing;
        }

        var created = new HarnessConnection(owner, inner, RemoteNameOf(inner) ?? inner.Remote.ToString(), isClientSide: false, endpoint: null);
        lock (_gate)
        {
            if (_serverConnections.TryGetValue(inner, out HarnessConnection? raced))
                return raced;
            _serverConnections[inner] = created;
            _connections.Add(created);
        }

        created.AttachClosed();
        return created;
    }

    /// <summary>Open connections (counted once, from the side that opened them) with <paramref name="name"/> at either end.</summary>
    public int CountOpenConnections(string name)
    {
        HarnessConnection[] connections = SnapshotConnections();
        int count = 0;
        foreach (HarnessConnection connection in connections)
        {
            if (connection.IsClientSide && connection.IsOpen
                && (string.Equals(connection.LocalName, name, StringComparison.Ordinal) || string.Equals(connection.RemoteName, name, StringComparison.Ordinal)))
                count++;
        }

        return count;
    }

    /// <summary>Makes every open connection with <paramref name="name"/> at either end silently dead; returns how many.</summary>
    public int KillConnectionsSilently(string name)
    {
        int killed = 0;
        foreach (HarnessConnection connection in SnapshotConnections())
        {
            if (!connection.IsOpen || connection.SilentlyDead)
                continue;
            if (string.Equals(connection.LocalName, name, StringComparison.Ordinal) || string.Equals(connection.RemoteName, name, StringComparison.Ordinal))
            {
                connection.SilentlyDead = true;
                if (connection.IsClientSide)
                    killed++;
            }
        }

        return killed;
    }

    /// <summary>Abortively closes every connection with <paramref name="name"/> at either end (both ends see a reset).</summary>
    public void ResetConnections(string name)
    {
        if (Sim is not null)
        {
            Sim.ResetConnections(name);
            return;
        }

        if (Memory is not null)
        {
            Memory.ResetConnections(name);
            return;
        }

        HarnessTransport[] transports;
        lock (_gate)
            transports = [.. _transports];
        foreach (HarnessTransport transport in transports)
        {
            if (transport.InnerControl is not { } control)
                continue;
            control.ResetConnections(string.Equals(transport.LocalName, name, StringComparison.Ordinal) ? null : name);
        }
    }

    // -------------------------------------------------------------------------------------------- holds and limits

    public void AddHold(MessageHold hold)
    {
        lock (_gate)
            _holds.Add(hold);
        Interlocked.Increment(ref _activeHolds);
    }

    /// <summary>
    /// Offers a message to the active holds whose filters match: each captures it, and the first one still holding holds it
    /// (returned); null when no hold holds it.
    /// </summary>
    internal HeldMessage? TryHold(string from, string to, LinkDirection direction, ReadOnlySpan<byte> frame, HarnessConnection origin)
    {
        if (Volatile.Read(ref _activeHolds) == 0)
            return null;
        MessageHold[] holds;
        lock (_gate)
        {
            _holds.RemoveAll(static h => !h.IsActive);
            Volatile.Write(ref _activeHolds, _holds.Count);
            holds = [.. _holds];
        }

        foreach (MessageHold hold in holds)
        {
            HeldMessage? held = hold.TryCapture(from, to, direction, frame, static () => new TaskCompletionSource<bool>(), origin, out CapturedMessage? captured);
            if (captured is not null)
            {
                lock (_gate)
                    _origins.AddOrUpdate(captured, origin);
            }

            if (held is not null)
                return held;
        }

        return null;
    }

    public void AddReadLimit(ReadResultLimitRule rule)
    {
        lock (_gate)
            _readLimits.Add(rule);
        Interlocked.Increment(ref _activeReadLimits);
    }

    /// <summary>The active read-result limits applying to a result served by <paramref name="node"/>.</summary>
    internal ReadResultLimitRule? FindReadLimit(string node, string? volume)
    {
        if (Volatile.Read(ref _activeReadLimits) == 0)
            return null;
        lock (_gate)
        {
            _readLimits.RemoveAll(static r => !r.IsActive);
            Volatile.Write(ref _activeReadLimits, _readLimits.Count);
            foreach (ReadResultLimitRule rule in _readLimits)
            {
                if (rule.AppliesTo(node, volume))
                    return rule;
            }
        }

        return null;
    }

    /// <summary>Cancels every read-result limit (cluster disposal).</summary>
    public void CancelReadLimits()
    {
        lock (_gate)
        {
            foreach (ReadResultLimitRule rule in _readLimits)
                rule.Cancel();
            _readLimits.Clear();
            Volatile.Write(ref _activeReadLimits, 0);
        }
    }

    internal string? VolumeNameOf(VolumeId id)
    {
        Func<VolumeId, string?>[] resolvers;
        lock (_gate)
            resolvers = [.. _volumeNames];
        foreach (Func<VolumeId, string?> resolver in resolvers)
        {
            if (resolver(id) is { } name)
                return name;
        }

        return null;
    }

    // -------------------------------------------------------------------------------------------- replay

    /// <summary>
    /// Re-delivers a captured frame from its original sender identity: on an open connection of that sender to the receiver
    /// (preferably the one it was captured on), else on a fresh connection authenticated like the original one. A node
    /// sender cannot be impersonated on a fresh connection: when none of its connections to the receiver is usable (the
    /// receiver restarted a moment ago and the sender has not reconnected yet, or the chosen connection turns out dead),
    /// the replay waits, within <paramref name="timeout"/>, for the sender's own peer connection to the receiver.
    /// </summary>
    public async Task<DfsErrorCode> ReplayAsync(CapturedMessage message, TimeSpan timeout)
    {
        ArgumentNullException.ThrowIfNull(message);
        HarnessConnection? origin;
        lock (_gate)
            origin = _origins.TryGetValue(message, out HarnessConnection? o) ? o : null;

        if (message.Direction == LinkDirection.Reply)
        {
            // A late reply carries the correlation of an exchange that is over: the receiving transport discards it. The
            // transmission is still observable (log, attempts); nothing else can happen.
            CountAttempt(message.To);
            Policy.Decide(message.From, message.To, LinkDirection.Reply, message.Frame.Span);
            Trace($"replayed reply {message.Type} {message.From}->{message.To} (discarded by the receiver as a late reply)");
            return DfsErrorCode.None;
        }

        // Holds capture at the receiving end, so the origin is the receiver's side of the connection; the sender's side (its
        // client end, which observed the handshake) and the sender's authenticated identity are resolved from it.
        HarnessConnection? sender = origin is null ? null : origin.IsClientSide ? origin : CounterpartOf(origin);
        ReplayIdentity? identity = origin is null ? null : IdentityOf(sender, origin);
        long started = Runtime.Time.GetTimestamp();
        var failed = new HashSet<HarnessConnection>();
        while (true)
        {
            HarnessConnection? connection = FindReplayConnection(message.From, message.To, sender, identity, failed);
            HarnessTransport? fresh = null;
            if (connection is null && identity is not null)
                (connection, fresh) = await OpenReplayConnectionAsync(message.From, message.To, sender, identity, timeout);
            if (connection is null)
            {
                if (identity is { Role: PeerRole.Node } && Runtime.Time.GetElapsedTime(started) < timeout)
                {
                    await Runtime.DelayAsync(ReplayRetryDelay);
                    continue;
                }

                Trace($"replay of {message.Type} {message.From}->{message.To} impossible: no authenticated connection of the sender remains");
                return DfsErrorCode.Unavailable;
            }

            DfsErrorCode? code = await ReplayOnAsync(connection, fresh, message, timeout);
            if (code is { } done)
                return done;

            // The connection was dead: try another one (a node sender's peer connection reappears when it reconnects).
            failed.Add(connection);
            if (Runtime.Time.GetElapsedTime(started) >= timeout)
                return DfsErrorCode.Unavailable;
        }
    }

    private static readonly TimeSpan ReplayRetryDelay = TimeSpan.FromMilliseconds(10);

    /// <summary>Replays on <paramref name="connection"/>; null when it turned out dead before an answer.</summary>
    private async Task<DfsErrorCode?> ReplayOnAsync(HarnessConnection connection, HarnessTransport? fresh, CapturedMessage message, TimeSpan timeout)
    {
        Trace($"replayed {message.Direction} {message.Type} {message.From}->{message.To}");
        try
        {
            if (message.Direction == LinkDirection.OneWay)
            {
                await connection.SendAsync(message.Frame);
                return DfsErrorCode.None;
            }

            ReadOnlyMemory<byte> reply = await connection.CallAsync(message.Frame, timeout);
            return CodeOf(reply.Span);
        }
        catch (TimeoutException)
        {
            return DfsErrorCode.Timeout;
        }
        catch (RpcConnectionException)
        {
            return null;
        }
        catch (ObjectDisposedException)
        {
            return null;
        }
        finally
        {
            if (fresh is not null)
                await fresh.DisposeAsync();
        }
    }

    /// <summary>The error code of a reply frame (<see cref="DfsErrorCode.None"/> for a success reply).</summary>
    internal static DfsErrorCode CodeOf(ReadOnlySpan<byte> reply)
    {
        try
        {
            IWireMessage decoded = MessageCodec.Decode(reply);
            return decoded is ErrorReplyMessage error ? error.Code : DfsErrorCode.None;
        }
        catch (DfsProtocolException)
        {
            return DfsErrorCode.ProtocolError;
        }
    }

    /// <summary>The authenticated identity a replay must present: the sender's role, principal and handshake name.</summary>
    private sealed record ReplayIdentity(PeerRole Role, string? Principal, string? Name);

    /// <summary>The sender's (client-side) end of the connection whose receiving end is <paramref name="receiving"/>, when known.</summary>
    private HarnessConnection? CounterpartOf(HarnessConnection receiving)
    {
        object? peer = receiving.Inner is SimConnectionEnd sim ? sim.Peer : null;
        if (peer is null)
            return null;
        foreach (HarnessConnection connection in SnapshotConnections())
        {
            if (connection.IsClientSide && ReferenceEquals(connection.Inner, peer))
                return connection;
        }

        return null;
    }

    /// <summary>
    /// The identity a replay presents: the handshake the sender's end observed, else the identity the receiving end
    /// authenticated (<see cref="RpcPeerIdentity"/>), else none (an unauthenticated sender cannot be impersonated).
    /// </summary>
    private static ReplayIdentity? IdentityOf(HarnessConnection? sender, HarnessConnection origin)
    {
        if (sender is { HelloRole: { } role })
            return new ReplayIdentity(role, sender.HelloPrincipal, sender.HelloName);
        if (origin.IsClientSide)
            return origin.HelloRole is { } own ? new ReplayIdentity(own, origin.HelloPrincipal, origin.HelloName) : null;
        RpcPeerIdentity? authenticated = origin.Identity;
        if (authenticated is null)
            return null;
        PeerRole peerRole = authenticated.IsNode ? PeerRole.Node : authenticated.IsBackup ? PeerRole.Backup : PeerRole.Client;
        return new ReplayIdentity(peerRole, authenticated.Principal, authenticated.NodeName);
    }

    /// <summary>An open, authenticated connection of the sender to <paramref name="to"/> presenting <paramref name="identity"/>.</summary>
    private HarnessConnection? FindReplayConnection(string from, string to, HarnessConnection? sender, ReplayIdentity? identity,
        HashSet<HarnessConnection> failed)
    {
        if (sender is { IsOpen: true, SilentlyDead: false, IsClientSide: true } && !failed.Contains(sender)
            && string.Equals(sender.LocalName, from, StringComparison.Ordinal) && string.Equals(sender.RemoteName, to, StringComparison.Ordinal))
            return sender;
        foreach (HarnessConnection connection in SnapshotConnections())
        {
            if (connection.IsClientSide && connection.IsOpen && !connection.SilentlyDead && connection.IsAuthenticated && !failed.Contains(connection)
                && string.Equals(connection.LocalName, from, StringComparison.Ordinal) && string.Equals(connection.RemoteName, to, StringComparison.Ordinal)
                && (identity is null || Presents(connection, identity)))
                return connection;
        }

        return null;
    }

    private static bool Presents(HarnessConnection connection, ReplayIdentity identity) =>
        connection.HelloRole == identity.Role
        && (identity.Role == PeerRole.Node || string.Equals(connection.HelloPrincipal, identity.Principal, StringComparison.Ordinal));

    /// <summary>
    /// A fresh connection of the sender's identity to <paramref name="to"/>, authenticated with the principal's credentials
    /// (clients and backup principals only: the harness never impersonates a node); null when impossible.
    /// </summary>
    private async Task<(HarnessConnection?, HarnessTransport?)> OpenReplayConnectionAsync(string from, string to, HarnessConnection? sender,
        ReplayIdentity identity, TimeSpan timeout)
    {
        if (identity.Role == PeerRole.Node || identity.Principal is not { } principal)
            return (null, null);
        HarnessTransportFactory? factory = sender?.Owner.Factory;
        if (factory is null)
        {
            lock (_gate)
                factory = _factories.TryGetValue(from, out HarnessTransportFactory? known) ? known : null;
        }

        if (factory is null)
            return (null, null);
        DfsCredentials? credentials = factory.Identity.Credentials is { } own && string.Equals(own.Principal, principal, StringComparison.Ordinal)
            ? own
            : factory.Identity.Owner?.FindCredentials(principal);
        string? endpoint = sender is not null && string.Equals(sender.RemoteName, to, StringComparison.Ordinal)
            ? sender.Endpoint ?? EndpointOfName(to)
            : EndpointOfName(to);
        if (credentials is null || endpoint is null)
            return (null, null);

        var transport = (HarnessTransport)factory.Create(from, string.Empty);
        try
        {
            var connection = (HarnessConnection)await transport.ConnectAsync(endpoint);
            await Server.Runtime.HandshakeClient.AuthenticatePrincipalAsync(connection, Guid.Empty, identity.Name ?? from, credentials,
                _random.Fork("replay/" + from), timeout, identity.Role);
            return (connection, transport);
        }
        catch (Exception ex) when (ex is DfsException or UnauthorizedAccessException or IOException or TimeoutException or OperationCanceledException
            or ObjectDisposedException or ArgumentException)
        {
            // RpcConnectionException (refused/cut/crashed endpoint) is an IOException: the sender has no usable connection.
            try
            {
                await transport.DisposeAsync();
            }
            catch (Exception dispose) when (dispose is ObjectDisposedException or IOException or InvalidOperationException)
            {
                // Already torn down.
            }

            return (null, null);
        }
    }

    private HarnessConnection[] SnapshotConnections()
    {
        lock (_gate)
            return [.. _connections];
    }

    internal static string? RemoteNameOf(IRpcConnection inner) => inner switch
    {
        SimConnectionEnd sim => sim.Peer.Transport.LocalName,
        Transport.Internal.RpcConnectionBase rpc => rpc.RemoteName,
        _ => inner.Remote.Name,
    };

    internal static string NormalizeEndpoint(string endpoint) =>
        RpcAddress.TryParse(endpoint, out RpcAddress address, out _) ? address.ToString() : endpoint;
}
