using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Server.Runtime;

/// <summary>
/// Pool of authenticated outbound node-to-node connections, keyed by <see cref="NodeId"/> (endpoints resolved from the
/// control state) or by endpoint (join seeds, leader hints). Connections are established on demand with the node
/// handshake; failures back off exponentially (with jitter) on the node's <see cref="TimeProvider"/>.
/// </summary>
internal sealed class PeerConnections
{
    private readonly NodeServices _s;
    private readonly SortedDictionary<NodeId, Slot> _byNode = new(Control.NodeIdComparer.Instance);
    private readonly SortedDictionary<string, Slot> _byEndpoint = new(StringComparer.Ordinal);
    private readonly SortedDictionary<NodeId, string> _endpointHints = new(Control.NodeIdComparer.Instance);
    private IRpcTransport? _transport;
    private bool _closed;

    public PeerConnections(NodeServices services)
    {
        ArgumentNullException.ThrowIfNull(services);
        _s = services;
        TimeSpan election = services.Timings.Tick * services.Timings.ElectionTicksMin;
        MaxBackoff = Clamp(election * 2, TimeSpan.FromMilliseconds(50), TimeSpan.FromSeconds(2));
        MinBackoff = Clamp(services.Timings.Tick * 2, TimeSpan.FromMilliseconds(5), MaxBackoff);
        DefaultCallTimeout = Clamp(election * 20, TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(30));
        SnapshotCallTimeout = DefaultCallTimeout * 2;
    }

    /// <summary>Timeout of node-to-node requests and handshakes.</summary>
    public TimeSpan DefaultCallTimeout { get; }

    /// <summary>Timeout of one snapshot chunk request.</summary>
    public TimeSpan SnapshotCallTimeout { get; }

    /// <summary>Smallest reconnect backoff.</summary>
    public TimeSpan MinBackoff { get; }

    /// <summary>Largest reconnect backoff.</summary>
    public TimeSpan MaxBackoff { get; }

    /// <summary>Binds the transport used for outbound connections.</summary>
    public void Attach(IRpcTransport transport)
    {
        ArgumentNullException.ThrowIfNull(transport);
        _transport = transport;
    }

    /// <summary>
    /// Remembers endpoints learned outside the local control state (a cluster map received while joining): used when the
    /// local view does not know a member yet.
    /// </summary>
    public void LearnEndpoints(ClusterMapData map)
    {
        ArgumentNullException.ThrowIfNull(map);
        foreach (NodeEntry node in map.Nodes)
        {
            if (node.Endpoints.Count > 0 && node.Status != WireNodeStatus.Removed && node.Id != _s.NodeId)
                _endpointHints[node.Id] = node.Endpoints[0];
        }
    }

    /// <summary>True when an authenticated connection to <paramref name="peer"/> is open.</summary>
    public bool IsConnected(NodeId peer) => _byNode.TryGetValue(peer, out Slot? slot) && slot.Connection is { IsOpen: true };

    /// <summary>Sends a one-way message; false when no connection could be used (the message is lost).</summary>
    public async Task<bool> TrySendAsync(NodeId peer, IWireMessage message)
    {
        IRpcConnection connection;
        try
        {
            connection = await GetAsync(peer);
        }
        catch (Exception ex) when (IsTransient(ex))
        {
            return false;
        }

        try
        {
            await connection.SendAsync(MessageCodec.Encode(message));
            return true;
        }
        catch (Exception ex) when (IsTransient(ex))
        {
            Drop(peer, connection);
            return false;
        }
    }

    /// <summary>Sends a request to a member and returns its decoded reply; an error reply is thrown as its exception.</summary>
    public async Task<IWireMessage> CallAsync(NodeId peer, IWireMessage request, TimeSpan? timeout = null, CancellationToken ct = default)
    {
        IRpcConnection connection = await GetAsync(peer, ct);
        return await CallOnAsync(connection, request, timeout ?? DefaultCallTimeout, () => Drop(peer, connection), ct);
    }

    /// <summary>Sends a request to an endpoint (seed or leader hint) and returns its decoded reply.</summary>
    public async Task<IWireMessage> CallEndpointAsync(string endpoint, IWireMessage request, TimeSpan? timeout = null, CancellationToken ct = default)
    {
        IRpcConnection connection = await GetEndpointAsync(endpoint, ct);
        return await CallOnAsync(connection, request, timeout ?? DefaultCallTimeout, () => DropEndpoint(endpoint, connection), ct);
    }

    /// <summary>
    /// Opens an authenticated connection to an endpoint without a pool slot and returns it with the handshake outcome
    /// (seed join learns the cluster identity this way). The caller owns the connection.
    /// </summary>
    public async Task<(IRpcConnection Connection, HandshakeOutcome Outcome)> ConnectUnpooledAsync(string endpoint, NodeCredentials credentials, CancellationToken ct = default)
    {
        IRpcTransport transport = _transport ?? throw new InvalidOperationException("The transport is not attached.");
        IRpcConnection connection = await transport.ConnectAsync(endpoint, ct);
        try
        {
            HandshakeOutcome outcome = await HandshakeClient.AuthenticateNodeAsync(connection, credentials, null, _s.Random, DefaultCallTimeout, ct);
            return (connection, outcome);
        }
        catch
        {
            await connection.DisposeAsync();
            throw;
        }
    }

    /// <summary>
    /// A peer authenticated a connection to this node as <paramref name="incarnation"/>: pooled outbound connections that were
    /// authenticated with an older incarnation of it lead to a dead process. Their close may never have arrived (it travels
    /// like any frame, for example across a link that was cut while the peer crashed), and one-way sends on them vanish
    /// silently, so they are dropped now and the next use connects to the running incarnation.
    /// </summary>
    public void OnPeerAuthenticated(NodeId peer, ulong incarnation)
    {
        if (_byNode.TryGetValue(peer, out Slot? slot))
            DropIfOlder(slot, incarnation);
        foreach (Slot endpointSlot in _byEndpoint.Values)
        {
            if (endpointSlot.PeerNode == peer)
                DropIfOlder(endpointSlot, incarnation);
        }
    }

    private static void DropIfOlder(Slot slot, ulong incarnation)
    {
        if (slot.Connection is not { } connection || slot.PeerIncarnation >= incarnation)
            return;
        slot.Connection = null;
        slot.Failures = 0;
        slot.RetryAfter = 0;
        _ = connection.DisposeAsync().AsTask();
    }

    /// <summary>Closes the pooled connection to <paramref name="peer"/> (it reconnects on next use).</summary>
    public void Reset(NodeId peer)
    {
        if (_byNode.Remove(peer, out Slot? slot) && slot.Connection is { } connection)
            _ = connection.DisposeAsync().AsTask();
    }

    /// <summary>Closes every pooled connection.</summary>
    public async Task CloseAllAsync()
    {
        _closed = true;
        var connections = _byNode.Values.Concat(_byEndpoint.Values).Select(s => s.Connection).OfType<IRpcConnection>().ToArray();
        _byNode.Clear();
        _byEndpoint.Clear();
        foreach (IRpcConnection connection in connections)
        {
            try
            {
                await connection.DisposeAsync();
            }
            catch (Exception ex) when (IsTransient(ex) || ex is ObjectDisposedException)
            {
                // Closing anyway.
            }
        }
    }

    /// <summary>The pooled authenticated connection to a member (connects on demand).</summary>
    /// <exception cref="DfsUnavailableException">No endpoint is known, the connection attempt is backing off, or it failed.</exception>
    public Task<IRpcConnection> GetAsync(NodeId peer, CancellationToken ct = default)
    {
        if (!_byNode.TryGetValue(peer, out Slot? slot))
        {
            slot = new Slot();
            _byNode[peer] = slot;
        }

        if (slot.Connection is not { IsOpen: true } && slot.Connecting is null && _s.Rpc.FindInbound(peer) is { } inbound
            && (slot.RetryAfter > _s.Time.GetTimestamp() || ResolveEndpoint(peer) is null))
        {
            // No endpoint is known locally (a learner whose control view is still empty), or connecting to the known one
            // keeps failing (a stale endpoint of a restarted peer): use the authenticated connection the peer opened.
            return Task.FromResult(inbound);
        }

        return GetAsync(slot, () => ResolveEndpoint(peer), peer, peer.ToString(), ct);
    }

    private string? ResolveEndpoint(NodeId peer)
        => _s.Control?.ResolveEndpoint(peer) ?? (_s.Control?.State.FindMember(peer) is { IsLive: false } ? null : _endpointHints.GetValueOrDefault(peer));

    private Task<IRpcConnection> GetEndpointAsync(string endpoint, CancellationToken ct)
    {
        if (!_byEndpoint.TryGetValue(endpoint, out Slot? slot))
        {
            slot = new Slot();
            _byEndpoint[endpoint] = slot;
        }

        return GetAsync(slot, () => endpoint, null, endpoint, ct);
    }

    private async Task<IRpcConnection> GetAsync(Slot slot, Func<string?> resolve, NodeId? expectedPeer, string label, CancellationToken ct)
    {
        if (_closed)
            throw new DfsUnavailableException(DfsErrorCode.NodeUnavailable, $"{_s.NodeName} is stopping.");
        if (slot.Connection is { IsOpen: true } open)
            return open;
        if (slot.Connecting is { } pending)
            return await pending;
        long now = _s.Time.GetTimestamp();
        if (slot.RetryAfter > now)
            throw new DfsUnavailableException(DfsErrorCode.NodeUnavailable, $"Connecting to {label} is backing off after {slot.Failures} failure(s).");
        string? endpoint = resolve();
        if (string.IsNullOrEmpty(endpoint))
            throw new DfsUnavailableException(DfsErrorCode.NodeUnavailable, $"No endpoint is known for {label}.");

        Task<IRpcConnection> connecting = ConnectAsync(slot, endpoint, expectedPeer, ct);
        slot.Connecting = connecting;
        try
        {
            return await connecting;
        }
        finally
        {
            if (ReferenceEquals(slot.Connecting, connecting))
                slot.Connecting = null;
        }
    }

    private async Task<IRpcConnection> ConnectAsync(Slot slot, string endpoint, NodeId? expectedPeer, CancellationToken ct)
    {
        IRpcTransport transport = _transport ?? throw new DfsUnavailableException(DfsErrorCode.NodeUnavailable, "The transport is not attached.");
        IRpcConnection? connection = null;
        try
        {
            connection = await transport.ConnectAsync(endpoint, ct);
            HandshakeOutcome outcome = await HandshakeClient.AuthenticateNodeAsync(connection, _s.CreateNodeCredentials(), expectedPeer, _s.Random,
                DefaultCallTimeout, ct);
            _s.Rpc.RegisterOutbound(connection, outcome.ServerIdentity);
            IRpcConnection opened = connection;
            connection.Closed += (c, _) => _s.Loop.Post(() =>
            {
                if (ReferenceEquals(slot.Connection, c))
                    slot.Connection = null;
            });
            if (_closed)
            {
                await opened.DisposeAsync();
                throw new DfsUnavailableException(DfsErrorCode.NodeUnavailable, $"{_s.NodeName} is stopping.");
            }

            slot.Connection = opened;
            slot.PeerNode = outcome.ServerIdentity.NodeId;
            slot.PeerIncarnation = outcome.ServerIdentity.Incarnation;
            slot.Failures = 0;
            slot.RetryAfter = 0;
            return opened;
        }
        catch (Exception ex)
        {
            if (connection is not null)
            {
                try
                {
                    await connection.DisposeAsync();
                }
                catch (Exception closeError) when (IsTransient(closeError) || closeError is ObjectDisposedException)
                {
                    // Already failed.
                }
            }

            slot.Failures++;
            TimeSpan backoff = Backoff(slot.Failures);
            slot.RetryAfter = _s.Time.GetTimestamp() + (long)(backoff.TotalSeconds * _s.Time.TimestampFrequency);
            if (ex is DfsException or UnauthorizedAccessException)
                throw;
            throw new DfsUnavailableException(DfsErrorCode.NodeUnavailable, $"Connecting to {endpoint} failed: {ex.Message}", ex);
        }
    }

    private static async Task<IWireMessage> CallOnAsync(IRpcConnection connection, IWireMessage request, TimeSpan timeout, Action drop, CancellationToken ct)
    {
        ReadOnlyMemory<byte> reply;
        try
        {
            reply = await connection.CallAsync(MessageCodec.Encode(request), timeout, ct);
        }
        catch (RpcConnectionException ex)
        {
            drop();
            throw new DfsUnavailableException(DfsErrorCode.NodeUnavailable, "The peer connection failed: " + ex.Message, ex);
        }

        IWireMessage message = MessageCodec.Decode(reply.Span);
        if (message is ErrorReplyMessage error)
            throw error.ToException();
        return message;
    }

    private void Drop(NodeId peer, IRpcConnection connection)
    {
        if (_byNode.TryGetValue(peer, out Slot? slot) && ReferenceEquals(slot.Connection, connection))
            slot.Connection = null;
        _ = connection.DisposeAsync().AsTask();
    }

    private void DropEndpoint(string endpoint, IRpcConnection connection)
    {
        if (_byEndpoint.TryGetValue(endpoint, out Slot? slot) && ReferenceEquals(slot.Connection, connection))
            slot.Connection = null;
        _ = connection.DisposeAsync().AsTask();
    }

    private TimeSpan Backoff(int failures)
    {
        double factor = Math.Pow(2, Math.Min(failures - 1, 10));
        TimeSpan delay = TimeSpan.FromTicks((long)Math.Min(MaxBackoff.Ticks, MinBackoff.Ticks * factor));
        long jitter = (long)_s.Random.NextUInt64((ulong)Math.Max(1, delay.Ticks / 2));
        return delay + TimeSpan.FromTicks(jitter);
    }

    private static TimeSpan Clamp(TimeSpan value, TimeSpan min, TimeSpan max) => value < min ? min : value > max ? max : value;

    private static bool IsTransient(Exception ex)
        => ex is DfsException or IOException or TimeoutException or UnauthorizedAccessException or OperationCanceledException or ObjectDisposedException;

    private sealed class Slot
    {
        public IRpcConnection? Connection { get; set; }

        public Task<IRpcConnection>? Connecting { get; set; }

        public long RetryAfter { get; set; }

        public int Failures { get; set; }

        /// <summary>Node identity the open connection authenticated (its server side).</summary>
        public NodeId? PeerNode { get; set; }

        /// <summary>Incarnation the open connection authenticated.</summary>
        public ulong PeerIncarnation { get; set; }
    }
}
