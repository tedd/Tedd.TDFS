using System.Globalization;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Testing.Simulation;

/// <summary>
/// Deterministic in-simulation network implementing <see cref="IRpcTransportFactory"/> (ARCHITECTURE §10.3) on a
/// <see cref="SimScheduler"/>: seeded per-message latency, optional per-link latency/bandwidth, <see cref="ILinkPolicy"/>
/// fault interposition (drop, black hole, delay, duplicate, blocked connects), FIFO delivery per connection direction,
/// call timeouts measured on the caller's clock, and explicit connection close/reset semantics.
/// </summary>
/// <remarks>
/// <para>
/// <b>Identities.</b> A transport's <c>localName</c> is both its network identity for the link policy and the name of the
/// <see cref="SimProcess"/> that owns it (the live process of that name, created on demand). Deliveries to a transport run
/// as background work of its process: held while it is paused, discarded when it is killed. When the process is killed
/// its endpoint is released (a restarted incarnation can listen again) and its connections die.
/// </para>
/// <para>
/// <b>Ordering.</b> Messages of one connection direction arrive in send order (an intact ordered stream, HAR-007); a
/// <see cref="LinkDecisionKind.Delay"/> decision models an application-level delayed message and is delivered
/// independently (later messages may overtake it). <see cref="LinkDecisionKind.Duplicate"/> delivers the message twice.
/// <see cref="LinkDecisionKind.Drop"/> and <see cref="LinkDecisionKind.BlackHole"/> discard it while the connection stays
/// open. Requests and replies are decided separately, so a reply can be lost after the request executed (HAR-006).
/// </para>
/// <para>
/// <b>Timeouts.</b> <see cref="IRpcConnection.CallAsync"/> measures its timeout on the clock registered for the calling
/// transport (<see cref="SetClock"/>), else the scheduler clock with the same name, else the global clock.
/// </para>
/// <para>
/// <b>Completion.</b> Call and connect results complete their tasks inline on the simulation thread (no
/// <c>RunContinuationsAsynchronously</c>) and with no synchronization context installed
/// (<see cref="SimScheduler.CompleteWithoutContext"/>), so SDK code using <c>ConfigureAwait(false)</c> continues
/// deterministically inside the completing work item.
/// </para>
/// </remarks>
public sealed class SimNetwork : IRpcTransportFactory
{
    private readonly object _gate = new();
    private readonly SimScheduler _scheduler;
    private readonly IRandomSource _random;
    private readonly Dictionary<string, SimTransport> _listeners = new(StringComparer.Ordinal);
    private readonly List<SimTransport> _transports = [];
    private readonly Dictionary<string, TimeProvider> _clocks = new(StringComparer.Ordinal);
    private readonly Dictionary<(string From, string To), SimLinkSettings> _links = [];
    private readonly Dictionary<(string From, string To), long> _busyUntil = [];
    private ILinkPolicy _policy;
    private long _nextConnectionId;
    private int _nextPort = 40000;
    private long _messagesSent;
    private long _messagesDelivered;
    private long _messagesDropped;
    private long _messagesDuplicated;
    private long _messagesDelayed;
    private long _lateReplies;
    private long _bytesSent;
    private long _connectionsOpened;
    private long _connectionsFailed;

    /// <summary>Creates a network on <paramref name="scheduler"/>.</summary>
    /// <param name="scheduler">The simulation.</param>
    /// <param name="options">Latency and behaviour options (null = defaults).</param>
    /// <param name="policy">Link policy (null = allow everything).</param>
    public SimNetwork(SimScheduler scheduler, SimNetworkOptions? options = null, ILinkPolicy? policy = null)
    {
        ArgumentNullException.ThrowIfNull(scheduler);
        Options = options ?? new SimNetworkOptions();
        Options.Validate();
        _scheduler = scheduler;
        _random = scheduler.ForkRandom("sim/network/" + scheduler.NextNetworkIndex().ToString(CultureInfo.InvariantCulture));
        _policy = policy ?? AllowAllLinkPolicy.Instance;
        scheduler.ProcessKilled += OnProcessKilled;
    }

    /// <summary>The simulation.</summary>
    public SimScheduler Scheduler => _scheduler;

    /// <summary>The options.</summary>
    public SimNetworkOptions Options { get; }

    /// <summary>This network as a transport factory (for <c>NodeEnvironment.TransportFactory</c> and the SDK).</summary>
    public IRpcTransportFactory Factory => this;

    /// <summary>The link policy consulted for every connection attempt and message; null restores allow-all.</summary>
    public ILinkPolicy Policy
    {
        get => Volatile.Read(ref _policy);
        set => Volatile.Write(ref _policy, value ?? AllowAllLinkPolicy.Instance);
    }

    /// <summary>Endpoints currently accepting connections, sorted.</summary>
    public IReadOnlyList<string> ListeningEndpoints
    {
        get
        {
            lock (_gate)
            {
                var list = new List<string>();
                foreach ((string endpoint, SimTransport transport) in _listeners)
                {
                    if (transport.IsAccepting)
                        list.Add(endpoint);
                }

                list.Sort(StringComparer.Ordinal);
                return list;
            }
        }
    }

    /// <summary>Number of open connection ends over all live transports.</summary>
    public int OpenConnectionCount
    {
        get
        {
            SimTransport[] transports;
            lock (_gate)
                transports = [.. _transports];
            int count = 0;
            foreach (SimTransport transport in transports)
            {
                foreach (SimConnectionEnd end in transport.SnapshotConnections())
                {
                    if (end.IsOpen)
                        count++;
                }
            }

            return count;
        }
    }

    /// <summary>Application messages handed to the network (requests, replies, one-way).</summary>
    public long MessagesSent => Interlocked.Read(ref _messagesSent);

    /// <summary>Messages delivered to an open receiving end.</summary>
    public long MessagesDelivered => Interlocked.Read(ref _messagesDelivered);

    /// <summary>Messages discarded by the policy, or because the receiving end was closed (or its process dead) on arrival.</summary>
    public long MessagesDropped => Interlocked.Read(ref _messagesDropped);

    /// <summary>Messages the policy duplicated.</summary>
    public long MessagesDuplicated => Interlocked.Read(ref _messagesDuplicated);

    /// <summary>Messages the policy delayed.</summary>
    public long MessagesDelayed => Interlocked.Read(ref _messagesDelayed);

    /// <summary>Replies that arrived after their call completed (timed out, cancelled) or for an unknown call.</summary>
    public long LateReplies => Interlocked.Read(ref _lateReplies);

    /// <summary>Payload bytes handed to the network.</summary>
    public long BytesSent => Interlocked.Read(ref _bytesSent);

    /// <summary>Successfully established connections.</summary>
    public long ConnectionsOpened => Interlocked.Read(ref _connectionsOpened);

    /// <summary>Connection attempts that failed (refused or blocked).</summary>
    public long ConnectionsFailed => Interlocked.Read(ref _connectionsFailed);

    /// <summary>Raised on the sending thread for every application message, after the policy decided.</summary>
    public event Action<SimMessageEvent>? MessageSent;

    /// <summary>Creates a transport for <paramref name="localName"/>.</summary>
    /// <param name="localName">Network identity and owning process name (for example "n0" or "client:c0").</param>
    /// <param name="listenEndpoint">
    /// Endpoint to listen on after <see cref="IRpcTransport.StartAsync"/>, for example <c>mem://n0</c>; a <c>quic</c>
    /// endpoint with port 0 gets a deterministic port. Null or empty means <c>mem://{localName}</c> (client-only transports
    /// never need to start).
    /// </param>
    /// <exception cref="ArgumentException">A name or endpoint is invalid.</exception>
    public IRpcTransport Create(string localName, string listenEndpoint)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(localName);
        _scheduler.CheckThread("create transport");
        string endpoint;
        if (string.IsNullOrEmpty(listenEndpoint))
        {
            if (!RpcAddress.TryParse(RpcAddress.MemoryScheme + "://" + localName, out RpcAddress implicitAddress, out string? implicitError))
                throw new ArgumentException($"No listen endpoint was given and '{localName}' is not a valid in-memory endpoint name: {implicitError}", nameof(listenEndpoint));
            endpoint = implicitAddress.ToString();
        }
        else
        {
            if (!RpcAddress.TryParse(listenEndpoint, out RpcAddress address, out string? error))
                throw new ArgumentException(error, nameof(listenEndpoint));
            if (address.IsQuic && address.Port == 0)
            {
                lock (_gate)
                    address = address with { Port = _nextPort++ };
            }

            endpoint = address.ToString();
        }

        SimProcess owner = _scheduler.GetOrCreateProcess(localName);
        var transport = new SimTransport(this, localName, endpoint, owner);
        lock (_gate)
            _transports.Add(transport);
        return transport;
    }

    /// <summary>
    /// Registers the clock used for call timeouts of transports named <paramref name="localName"/> (for example a node's
    /// <see cref="TestClock"/> layered over the simulation clock).
    /// </summary>
    public void SetClock(string localName, TimeProvider clock)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(localName);
        ArgumentNullException.ThrowIfNull(clock);
        lock (_gate)
            _clocks[localName] = clock;
    }

    /// <summary>The clock used for call timeouts of <paramref name="localName"/>.</summary>
    public TimeProvider GetClock(string localName)
    {
        ArgumentNullException.ThrowIfNull(localName);
        lock (_gate)
        {
            if (_clocks.TryGetValue(localName, out TimeProvider? clock))
                return clock;
        }

        return (TimeProvider?)_scheduler.FindClock(localName) ?? _scheduler.Clock;
    }

    /// <summary>Overrides latency/bandwidth of the direction <paramref name="from"/> → <paramref name="to"/>.</summary>
    public void SetLink(string from, string to, SimLinkSettings settings)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(from);
        ArgumentException.ThrowIfNullOrWhiteSpace(to);
        ArgumentNullException.ThrowIfNull(settings);
        settings.Validate();
        lock (_gate)
            _links[(from, to)] = settings;
    }

    /// <summary>Sets a fixed one-way latency for one direction (keeping its bandwidth setting).</summary>
    public void SetLatency(string from, string to, TimeSpan latency)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(latency, TimeSpan.Zero);
        SimLinkSettings current = GetLink(from, to);
        SetLink(from, to, current with { MinLatency = latency, MaxLatency = latency });
    }

    /// <summary>Limits the bandwidth of one direction (0 = unlimited), keeping its latency setting.</summary>
    public void SetBandwidth(string from, string to, long bytesPerSecond)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(bytesPerSecond);
        SimLinkSettings current = GetLink(from, to);
        SetLink(from, to, current with { BytesPerSecond = bytesPerSecond });
    }

    /// <summary>The effective settings of one direction.</summary>
    public SimLinkSettings GetLink(string from, string to)
    {
        ArgumentNullException.ThrowIfNull(from);
        ArgumentNullException.ThrowIfNull(to);
        lock (_gate)
        {
            return _links.TryGetValue((from, to), out SimLinkSettings? settings)
                ? settings
                : new SimLinkSettings(Options.MinLatency, Options.MaxLatency);
        }
    }

    /// <summary>Removes the override of one direction.</summary>
    public void ClearLink(string from, string to)
    {
        lock (_gate)
        {
            _links.Remove((from, to));
            _busyUntil.Remove((from, to));
        }
    }

    /// <summary>Removes every link override.</summary>
    public void ClearLinks()
    {
        lock (_gate)
        {
            _links.Clear();
            _busyUntil.Clear();
        }
    }

    /// <summary>
    /// Resets every open connection of the transports named <paramref name="name"/>: both ends close with
    /// <see cref="RpcConnectionException"/> (pending calls fail; peers may reconnect).
    /// </summary>
    public void ResetConnections(string name)
    {
        ArgumentNullException.ThrowIfNull(name);
        _scheduler.CheckThread("reset connections");
        foreach (SimTransport transport in SnapshotTransports())
        {
            if (!string.Equals(transport.LocalName, name, StringComparison.Ordinal) || transport.IsDead)
                continue;
            foreach (SimConnectionEnd end in transport.SnapshotConnections())
            {
                if (end.IsOpen)
                    ResetPair(end, $"Connection {end.ConnectionId} was reset by the network (ResetConnections({name})).");
            }
        }

        _scheduler.AddTrace($"net: connections of {name} reset");
    }

    /// <inheritdoc />
    public override string ToString() => $"SimNetwork({_scheduler})";

    internal void CheckThread(string operation) => _scheduler.CheckThread(operation);

    internal void ReportHandlerFailure(SimConnectionEnd end, Exception exception)
    {
        _scheduler.ReportUnhandledException(exception, $"RPC handler of {end.Transport.LocalName} (connection {end.ConnectionId})");
        ResetPair(end, $"Connection {end.ConnectionId} was reset because the handler of {end.Transport.LocalName} failed: {exception.GetType().Name}.");
    }

    internal void ReportCallbackFailure(SimConnectionEnd end, string what, Exception exception) =>
        _scheduler.ReportUnhandledException(exception, $"{what} of {end.Transport.LocalName} (connection {end.ConnectionId})");

    internal void CountDroppedAtClosedEnd() => Interlocked.Increment(ref _messagesDropped);

    internal void CountDelivered() => Interlocked.Increment(ref _messagesDelivered);

    internal void CountLateReply() => Interlocked.Increment(ref _lateReplies);

    internal Task Start(SimTransport transport, IRpcHandler handler, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(handler);
        _scheduler.CheckThread("start transport");
        if (ct.IsCancellationRequested)
            return Task.FromCanceled(ct);
        if (transport.IsDisposed)
            return Task.FromException(new ObjectDisposedException(transport.ToString()));
        if (transport.IsDead)
            return Task.FromException(new RpcConnectionException($"Cannot start {transport}: its process {transport.Owner.Id} is dead."));
        if (transport.IsStarted)
            return Task.FromException(new InvalidOperationException($"{transport} is already started."));
        lock (_gate)
        {
            if (_listeners.TryGetValue(transport.LocalEndpoint, out SimTransport? existing) && existing.IsAccepting)
                return Task.FromException(new RpcConnectionException($"Address {transport.LocalEndpoint} is already in use by '{existing.LocalName}'."));
            transport.Handler = handler;
            transport.IsStarted = true;
            _listeners[transport.LocalEndpoint] = transport;
        }

        _scheduler.AddTrace($"net: {transport.Owner.Id} listening on {transport.LocalEndpoint}");
        return Task.CompletedTask;
    }

    internal Task<IRpcConnection> Connect(SimTransport from, string endpoint, CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrEmpty(endpoint);
        _scheduler.CheckThread("connect");
        if (!RpcAddress.TryParse(endpoint, out RpcAddress address, out string? error))
            throw new ArgumentException(error, nameof(endpoint));
        if (ct.IsCancellationRequested)
            return Task.FromCanceled<IRpcConnection>(ct);
        if (from.IsDisposed)
            return Task.FromException<IRpcConnection>(new ObjectDisposedException(from.ToString()));
        if (from.IsDead)
            return Task.FromException<IRpcConnection>(new RpcConnectionException($"Cannot connect from {from}: its process {from.Owner.Id} is dead."));

        string key = address.ToString();
        string targetName;
        lock (_gate)
            targetName = _listeners.TryGetValue(key, out SimTransport? listener) ? listener.LocalName : key;
        long now = _scheduler.NowTicks;
        var attempt = new ConnectAttempt(this, from, address, now, ct);
        long arrival = SaturatingAdd(now, SampleLatency(from.LocalName, targetName));
        from.AddConnectAbort(attempt.Abort);
        _scheduler.ScheduleOnNewLane(null, "net/connect", arrival, new SimWorkItem(attempt.OnArrived, $"connect {from.LocalName} -> {key}"), "connect");
        if (ct.CanBeCanceled)
            attempt.Registration = ct.Register(static s => ((ConnectAttempt)s!).Cancel(), attempt);
        return attempt.Task;
    }

    internal void DisposeTransport(SimTransport transport)
    {
        _scheduler.CheckThread("dispose transport");
        lock (_gate)
        {
            if (transport.IsDisposed)
                return;
            transport.IsDisposed = true;
            if (_listeners.TryGetValue(transport.LocalEndpoint, out SimTransport? registered) && ReferenceEquals(registered, transport))
                _listeners.Remove(transport.LocalEndpoint);
            _transports.Remove(transport);
        }

        foreach (SimConnectionEnd end in transport.SnapshotConnections())
            end.CloseLocal(null, notifyPeer: true);

        // Outstanding connects fail now (ObjectDisposedException); a handshake still in flight is torn down when it lands.
        foreach (Action abort in transport.TakeConnectAborts())
            abort();
    }

    /// <summary>Hands one envelope from <paramref name="from"/> to its peer, applying the link policy.</summary>
    internal void Transmit(SimConnectionEnd from, SimEnvelope envelope)
    {
        SimConnectionEnd to = from.Peer;
        string fromName = from.Transport.LocalName;
        string toName = to.Transport.LocalName;
        ILinkPolicy policy = Policy;
        if (envelope.Kind == SimEnvelopeKind.Close)
        {
            // Transport-level close/reset: travels in order behind data already sent, unless the link is cut.
            if (policy.CanConnect(fromName, toName))
                ScheduleDelivery(from, to, envelope, fifo: true, extraTicks: 0);
            return;
        }

        LinkDirection direction = envelope.Kind switch
        {
            SimEnvelopeKind.Request or SimEnvelopeKind.RequestFragment => LinkDirection.Request,
            SimEnvelopeKind.Reply => LinkDirection.Reply,
            _ => LinkDirection.OneWay,
        };
        LinkDecision decision = policy.Decide(fromName, toName, direction, envelope.Payload);
        Interlocked.Increment(ref _messagesSent);
        Interlocked.Add(ref _bytesSent, envelope.Payload.Length);
        RaiseMessageSent(new SimMessageEvent(from.ConnectionId, fromName, toName, direction, envelope.Payload, decision, _scheduler.Now));
        switch (decision.Kind)
        {
            case LinkDecisionKind.Deliver:
                ScheduleDelivery(from, to, envelope, fifo: true, extraTicks: 0);
                break;
            case LinkDecisionKind.Duplicate:
                Interlocked.Increment(ref _messagesDuplicated);
                ScheduleDelivery(from, to, envelope, fifo: true, extraTicks: 0);
                ScheduleDelivery(from, to, envelope, fifo: true, extraTicks: 0);
                break;
            case LinkDecisionKind.Delay:
                Interlocked.Increment(ref _messagesDelayed);
                ScheduleDelivery(from, to, envelope, fifo: false, extraTicks: Math.Max(0, decision.Delay.Ticks));
                break;
            case LinkDecisionKind.Drop:
            case LinkDecisionKind.BlackHole:
                Interlocked.Increment(ref _messagesDropped);
                break;
            default:
                throw new InvalidOperationException($"The link policy returned an unknown decision '{decision.Kind}'.");
        }
    }

    /// <summary>Closes both ends of a connection with a reset error, each as work of its own process.</summary>
    internal void ResetPair(SimConnectionEnd end, string message)
    {
        ScheduleClose(end, new RpcConnectionException(message));
        ScheduleClose(end.Peer, new RpcConnectionException(message));
    }

    private static long SaturatingAdd(long a, long b)
    {
        long r = unchecked(a + b);
        return b > 0 && r < a ? long.MaxValue - 1 : r;
    }

    private void ScheduleClose(SimConnectionEnd end, Exception error)
    {
        if (!end.IsOpen || end.Transport.IsDead)
            return;
        SimProcess owner = end.Transport.Owner;
        _scheduler.ScheduleOnNewLane(owner, owner.Id + "/net-reset", _scheduler.NowTicks,
            new SimWorkItem(() => end.CloseLocal(error, notifyPeer: false), $"reset connection {end.ConnectionId}"), "reset connection");
    }

    private void RaiseMessageSent(SimMessageEvent message)
    {
        Action<SimMessageEvent>? handlers = MessageSent;
        if (handlers is null)
            return;
        try
        {
            handlers(message);
        }
        catch (Exception ex)
        {
            _scheduler.ReportUnhandledException(ex, "SimNetwork.MessageSent handler");
        }
    }

    private void ScheduleDelivery(SimConnectionEnd from, SimConnectionEnd to, SimEnvelope envelope, bool fifo, long extraTicks)
    {
        string fromName = from.Transport.LocalName;
        string toName = to.Transport.LocalName;
        long now = _scheduler.NowTicks;
        long sent = TransmissionEnd(fromName, toName, envelope.Payload.Length, now);
        long arrival = SaturatingAdd(SaturatingAdd(sent, SampleLatency(fromName, toName)), extraTicks);

        // A message discarded because the receiving process is dead (killed before or after it arrived) was dropped at a
        // closed end; transport-level close frames are not application messages and are not counted.
        Action? onDiscard = envelope.Kind == SimEnvelopeKind.Close ? null : CountDroppedAtClosedEnd;
        var item = new SimWorkItem(() => to.Receive(envelope), envelope.Describe(from.ConnectionId, fromName, toName), onDiscard);
        if (fifo)
        {
            arrival = to.ReserveArrival(arrival);
            _scheduler.ScheduleOnLane(to.InboundLane, arrival, item, "send message");
        }
        else
        {
            SimProcess owner = to.Transport.Owner;
            _scheduler.ScheduleOnNewLane(owner, owner.Id + "/net-delayed", arrival, item, "send message");
        }
    }

    private long TransmissionEnd(string from, string to, int bytes, long now)
    {
        lock (_gate)
        {
            if (!_links.TryGetValue((from, to), out SimLinkSettings? settings) || settings.BytesPerSecond <= 0)
                return now;
            double ticks = Math.Ceiling(bytes * (double)TimeSpan.TicksPerSecond / settings.BytesPerSecond);
            long transfer = ticks >= long.MaxValue / 4 ? long.MaxValue / 4 : (long)ticks;
            long start = _busyUntil.TryGetValue((from, to), out long busy) && busy > now ? busy : now;
            long end = SaturatingAdd(start, transfer);
            _busyUntil[(from, to)] = end;
            return end;
        }
    }

    private long SampleLatency(string from, string to)
    {
        lock (_gate)
        {
            TimeSpan min = Options.MinLatency;
            TimeSpan max = Options.MaxLatency;
            if (_links.TryGetValue((from, to), out SimLinkSettings? settings))
            {
                min = settings.MinLatency;
                max = settings.MaxLatency;
            }

            long range = max.Ticks - min.Ticks;
            return range <= 0 ? min.Ticks : min.Ticks + (long)_random.NextUInt64((ulong)range + 1);
        }
    }

    private SimTransport[] SnapshotTransports()
    {
        lock (_gate)
            return [.. _transports];
    }

    private SimConnectionPair CreatePair(SimTransport client, SimTransport server, RpcAddress serverAddress)
    {
        long id;
        lock (_gate)
            id = ++_nextConnectionId;
        RpcAddress clientAddress = RpcAddress.TryParse(client.LocalEndpoint, out RpcAddress parsed)
            ? parsed
            : new RpcAddress(RpcAddress.MemoryScheme, client.LocalName, 0, client.LocalName);
        string idText = id.ToString(CultureInfo.InvariantCulture);
        SimLane serverLane = _scheduler.CreateLane(server.Owner, $"{server.Owner.Id}/net#{idText}", context: null, transient: false);
        SimLane clientLane = _scheduler.CreateLane(client.Owner, $"{client.Owner.Id}/net#{idText}", context: null, transient: false);
        var serverEnd = new SimConnectionEnd(this, server, id, clientAddress, serverLane);
        var clientEnd = new SimConnectionEnd(this, client, id, serverAddress, clientLane);
        serverEnd.Peer = clientEnd;
        clientEnd.Peer = serverEnd;
        server.AddConnection(serverEnd);
        client.AddConnection(clientEnd);
        Interlocked.Increment(ref _connectionsOpened);
        _scheduler.AddTrace($"net: connection {idText} {client.LocalName} -> {server.LocalName} ({server.LocalEndpoint})");
        return new SimConnectionPair(clientEnd, serverEnd);
    }

    private void OnProcessKilled(SimProcess process)
    {
        List<SimTransport> dead = [];
        lock (_gate)
        {
            foreach (SimTransport transport in _transports)
            {
                if (ReferenceEquals(transport.Owner, process) && !transport.IsDead)
                {
                    transport.IsDead = true;
                    dead.Add(transport);
                    if (_listeners.TryGetValue(transport.LocalEndpoint, out SimTransport? registered) && ReferenceEquals(registered, transport))
                        _listeners.Remove(transport.LocalEndpoint);
                }
            }

            _transports.RemoveAll(static t => t.IsDead);
        }

        foreach (SimTransport transport in dead)
        {
            foreach (SimConnectionEnd end in transport.SnapshotConnections())
            {
                // The dead side observes nothing (its code no longer runs); its pending calls are abandoned with it.
                bool wasOpen = end.MarkDead();
                SimConnectionEnd peer = end.Peer;
                if (wasOpen && Options.ResetPeersOnProcessDeath && peer.IsOpen && !peer.Transport.IsDead)
                {
                    Transmit(end, new SimEnvelope(SimEnvelopeKind.Close, 0, [],
                        new RpcConnectionException($"Connection {end.ConnectionId} was reset: process {process.Id} died.")));
                }
            }
        }
    }

    private sealed record SimConnectionPair(SimConnectionEnd Client, SimConnectionEnd Server);

    /// <summary>One outstanding <see cref="IRpcTransport.ConnectAsync"/>.</summary>
    private sealed class ConnectAttempt
    {
        private readonly SimNetwork _network;
        private readonly SimTransport _from;
        private readonly RpcAddress _address;
        private readonly long _startTicks;
        private readonly CancellationToken _ct;
        private readonly TaskCompletionSource<IRpcConnection> _source = new();

        public ConnectAttempt(SimNetwork network, SimTransport from, RpcAddress address, long startTicks, CancellationToken ct)
        {
            _network = network;
            _from = from;
            _address = address;
            _startTicks = startTicks;
            _ct = ct;
            Abort = OnTransportDisposed;
        }

        public CancellationTokenRegistration Registration { get; set; }

        public Task<IRpcConnection> Task => _source.Task;

        /// <summary>Fails the attempt because its transport was disposed (registered with the transport).</summary>
        public Action Abort { get; }

        public void Cancel()
        {
            Finish();
            SimScheduler.CompleteWithoutContext(() => _source.TrySetCanceled(_ct));
        }

        /// <summary>The connection request reached the destination address (network-level work, no owner).</summary>
        public void OnArrived()
        {
            if (_source.Task.IsCompleted)
                return;
            if (_from.IsDead || _from.Owner.IsKilled)
            {
                // The connecting process died while its request was in flight: nobody completes the handshake, so the
                // destination must not end up with a half-open connection. (The dead process's code never observes this.)
                Finish();
                return;
            }

            string key = _address.ToString();
            SimTransport? target;
            lock (_network._gate)
                target = _network._listeners.TryGetValue(key, out SimTransport? listener) && listener.IsAccepting ? listener : null;
            SimScheduler scheduler = _network._scheduler;
            SimProcess owner = _from.Owner;
            if (target is null)
            {
                long back = SaturatingAdd(scheduler.NowTicks, _network.SampleLatency(key, _from.LocalName));
                Reply(back, () => Fail(new RpcConnectionException($"Connection from {_from.LocalName} to {key} was refused: nothing is listening there.")));
                return;
            }

            if (!_network.Policy.CanConnect(_from.LocalName, target.LocalName))
            {
                long giveUp = SaturatingAdd(_startTicks, _network.Options.BlockedConnectTimeout.Ticks);
                Reply(giveUp, () => Fail(new RpcConnectionException(
                    $"Connection from {_from.LocalName} to {key} timed out after {_network.Options.BlockedConnectTimeout}: the link to {target.LocalName} is blocked.")));
                return;
            }

            SimConnectionPair pair = _network.CreatePair(_from, target, _address);
            long ack = SaturatingAdd(scheduler.NowTicks, _network.SampleLatency(target.LocalName, _from.LocalName));
            Reply(ack, () => Complete(pair));

            void Reply(long due, Action action) =>
                scheduler.ScheduleOnNewLane(owner, owner.Id + "/net-connect", due, new SimWorkItem(action, $"connect result {_from.LocalName} -> {key}"), "connect");
        }

        private void Fail(Exception error)
        {
            Finish();
            if (_source.Task.IsCompleted)
                return;
            Interlocked.Increment(ref _network._connectionsFailed);
            SimScheduler.CompleteWithoutContext(() => _source.TrySetException(error));
        }

        private void Complete(SimConnectionPair pair)
        {
            Finish();
            if (_from.IsDisposed || _source.Task.IsCompleted)
            {
                // Cancelled (or the transport went away) while the handshake was in flight: close the server side.
                pair.Client.CloseLocal(null, notifyPeer: true);
                return;
            }

            SimScheduler.CompleteWithoutContext(() => _source.TrySetResult(pair.Client));
        }

        private void OnTransportDisposed()
        {
            Registration.Dispose();
            SimScheduler.CompleteWithoutContext(() => _source.TrySetException(new ObjectDisposedException(_from.ToString())));
        }

        private void Finish()
        {
            Registration.Dispose();
            _from.RemoveConnectAbort(Abort);
        }
    }
}
