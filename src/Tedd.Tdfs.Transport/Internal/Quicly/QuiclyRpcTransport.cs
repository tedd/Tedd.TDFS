using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Client;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Time;
using Tedd.Quicly.Server;
using Tedd.Quicly.Transport.MsQuic;

namespace Tedd.Tdfs.Transport.Internal;

/// <summary>
/// An <see cref="IRpcTransport"/> over Tedd.Quicly (QUIC via MsQuic). One dedicated pump thread per transport owns the
/// Quicly server and every outbound Quicly client and is the only thread that polls, flushes or sends on them (the
/// Quicly "game thread" rule). Other threads enqueue frames and actions and wake the pump; MsQuic callbacks wake it
/// through wrapped transport sinks, and an idle pump waits at most 1 ms.
/// </summary>
internal sealed class QuiclyRpcTransport : IRpcTransport, IRpcTransportControl, IPumpSignal
{
    private const int StateCreated = 0;
    private const int StateDisposing = 1;
    private const int StateDisposed = 2;

    /// <summary>Most frames one connection may hand to Quicly in one pump pass (fairness between connections).</summary>
    private const int FramesPerConnectionPerPass = 64;

    /// <summary>Longest idle wait of the pump between passes.</summary>
    private static readonly TimeSpan IdleWait = TimeSpan.FromMilliseconds(1);

    /// <summary>How long a gracefully closing client or a shutting-down server may take before it is disposed.</summary>
    private static readonly TimeSpan CloseGrace = TimeSpan.FromSeconds(2);

    /// <summary>How long <see cref="DisposeAsync"/> waits for the pump thread.</summary>
    private static readonly TimeSpan PumpExitTimeout = TimeSpan.FromSeconds(15);

    /// <summary>
    /// Real-time clock for the pump's own grace periods. The pump is inherently real-time (Quicly and MsQuic keep their
    /// own monotonic clocks); the configurable <see cref="QuiclyTransportOptions.TimeProvider"/> drives call timeouts,
    /// connect timeouts and link delays only, so a frozen test clock cannot stall a shutdown.
    /// </summary>
    private static readonly TimeProvider PumpClock = TimeProvider.System;

    private readonly object _gate = new();
    private readonly object _inlineGate = new();
    private readonly ConcurrentQueue<Action> _actions = new();
    private readonly ManualResetEventSlim _wake = new(false, spinCount: 0);
    private readonly ConcurrentQueue<QuiclyRpcConnection> _sendReady = new();
    private readonly ConcurrentDictionary<long, QuiclyRpcConnection> _connections = new();
    private readonly CancellationTokenSource _disposeCts = new();
    private readonly TaskCompletionSource _pumpExited = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource _nativeReleased = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly RpcAddress _listen;
    private readonly byte[] _authToken;
    private readonly ChannelTable _channels;
    private Thread? _pump;
    private bool _pumpStopped;
    private volatile bool _stopRequested;
    private int _state;
    private long _nextConnectionId;
    private IRpcHandler? _handler;
    private bool _listenRequested;
    private string _localEndpoint;
    private ClientPool? _clientPool;
    private MsQuicTransportConnector? _connector;
    private MsQuicRegistration? _registration;

    // Pump-thread state.
    private QuiclyServer? _server;
    private readonly List<QuiclyRpcConnection> _clients = [];
    private readonly List<QuiclyRpcConnection> _closingClients = [];
    private readonly List<QuiclyRpcConnection> _drainingPeers = [];
    private readonly Dictionary<QuiclyPeer, QuiclyRpcConnection> _serverConnections = new(ReferenceEqualityComparer.Instance);
    private readonly ConditionalWeakTable<QuiclyPeer, string> _admissionNames = new();
    private readonly List<QuiclyRpcConnection> _sending = [];
    private readonly byte[] _scratch = new byte[QuiclyEnvelope.MaxFrameSize];
    private bool _moreToSend;

    public QuiclyRpcTransport(QuiclyTransportOptions options, string localName, RpcAddress listen)
    {
        Options = options;
        LocalName = localName;
        _listen = listen;
        _localEndpoint = listen.ToString();
        _authToken = QuiclyEnvelope.EncodeAuthToken(localName);
        Policy = options.LinkPolicy ?? AllowAllLinkPolicy.Instance;
        _channels = CreateChannelTable();
    }

    /// <summary>The options the transport was created with.</summary>
    public QuiclyTransportOptions Options { get; }

    /// <inheritdoc />
    public string LocalName { get; }

    /// <summary>The link policy consulted for every inbound message and admission.</summary>
    public ILinkPolicy Policy { get; }

    /// <summary>The handler for inbound requests, or null before <see cref="StartAsync"/>.</summary>
    public IRpcHandler? Handler => Volatile.Read(ref _handler);

    /// <inheritdoc />
    public string LocalEndpoint => Volatile.Read(ref _localEndpoint);

    /// <inheritdoc />
    public int OpenConnectionCount => _connections.Count;

    /// <summary>
    /// Completes once disposal has released every native MsQuic resource of this transport (the registration close,
    /// which waits for MsQuic to finish closing every connection, runs in the background after <see cref="DisposeAsync"/>).
    /// </summary>
    internal Task NativeResourcesReleased => _nativeReleased.Task;

    /// <summary>True when no client buffer pool exists or it has been disposed (every client peer returned its memory).</summary>
    internal bool ClientPoolReleased
    {
        get
        {
            lock (_gate)
            {
                return _clientPool is null || _clientPool.IsDisposed;
            }
        }
    }

    // ------------------------------------------------------------------ public API

    /// <inheritdoc />
    /// <exception cref="InvalidOperationException">Already started, or no certificate was configured.</exception>
    /// <exception cref="RpcConnectionException">The listener could not be started (for example the port is in use).</exception>
    /// <exception cref="ObjectDisposedException">The transport is disposed.</exception>
    public async Task StartAsync(IRpcHandler handler, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(handler);
        ct.ThrowIfCancellationRequested();
        X509Certificate2? certificate = Options.Certificate;
        if (certificate is null)
            throw new InvalidOperationException("A certificate is required to listen; set QuiclyTransportOptions.Certificate.");

        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_state != StateCreated, this);
            if (_listenRequested)
                throw new InvalidOperationException($"The transport '{LocalName}' has already been started.");
            _listenRequested = true;
        }

        IPEndPoint endPoint;
        try
        {
            endPoint = new IPEndPoint(await ResolveListenAddressAsync(_listen.Host, ct).ConfigureAwait(false), _listen.Port);
        }
        catch
        {
            lock (_gate)
            {
                _listenRequested = false;
            }

            throw;
        }

        Volatile.Write(ref _handler, handler);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        try
        {
            EnsurePump();
            Post(() =>
            {
                try
                {
                    if (_stopRequested)
                        throw new ObjectDisposedException(nameof(QuiclyRpcTransport));
                    StartServer(endPoint, certificate);
                    started.TrySetResult();
                }
                catch (Exception ex)
                {
                    started.TrySetException(ex);
                }
            });

            await started.Task.ConfigureAwait(false);
        }
        catch
        {
            Volatile.Write(ref _handler, null);
            lock (_gate)
            {
                _listenRequested = false;
            }

            throw;
        }
    }

    /// <inheritdoc />
    /// <exception cref="FormatException"><paramref name="endpoint"/> is malformed.</exception>
    /// <exception cref="ArgumentException"><paramref name="endpoint"/> is not a <c>quic://</c> endpoint.</exception>
    /// <exception cref="RpcConnectionException">
    /// The endpoint is unreachable, the TLS handshake failed (for example an untrusted certificate), the server refused
    /// the connection (link policy or limits), or the connect timed out.
    /// </exception>
    /// <exception cref="ObjectDisposedException">The transport is disposed.</exception>
    public async Task<IRpcConnection> ConnectAsync(string endpoint, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(endpoint);
        ct.ThrowIfCancellationRequested();
        RpcAddress address = RpcAddress.Parse(endpoint);
        if (!address.IsQuic)
            throw new ArgumentException($"The QUIC transport only supports 'quic://host:port' endpoints (got '{endpoint}').", nameof(endpoint));

        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_state != StateCreated, this);
        }

        EnsurePump();
        using var timeout = Options.ConnectTimeout == Timeout.InfiniteTimeSpan
            ? new CancellationTokenSource()
            : new CancellationTokenSource(Options.ConnectTimeout, Options.TimeProvider);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, timeout.Token, _disposeCts.Token);
        try
        {
            IPAddress[] candidates = await ResolveConnectAddressesAsync(address.Host, linked.Token).ConfigureAwait(false);
            RpcConnectionException? last = null;
            foreach (IPAddress candidate in candidates)
            {
                try
                {
                    return await ConnectOnceAsync(address, new IPEndPoint(candidate, address.Port), linked.Token).ConfigureAwait(false);
                }
                catch (RpcConnectionException ex)
                {
                    last = ex;
                }
            }

            throw last ?? new RpcConnectionException($"'{address.Host}' did not resolve to any address.");
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (OperationCanceledException) when (_disposeCts.IsCancellationRequested)
        {
            throw new ObjectDisposedException(nameof(QuiclyRpcTransport));
        }
        catch (OperationCanceledException ex) when (timeout.IsCancellationRequested)
        {
            throw new RpcConnectionException($"Connecting to {address} timed out after {Options.ConnectTimeout}.", ex);
        }
    }

    /// <inheritdoc />
    public void ResetConnections(string? remoteName = null)
    {
        foreach (QuiclyRpcConnection connection in _connections.Values)
        {
            if (remoteName is null || string.Equals(connection.RemoteName, remoteName, StringComparison.Ordinal))
                connection.TryClose(new RpcConnectionException($"The connection to '{connection.RemoteName}' was reset."), RpcCloseOrigin.Reset);
        }
    }

    /// <inheritdoc />
    /// <remarks>
    /// Closes every connection (remote ends observe the close), stops the listener and the pump thread, and releases
    /// MsQuic resources. Waits for the pump to finish its graceful shutdown (bounded).
    /// </remarks>
    public async ValueTask DisposeAsync()
    {
        bool first;
        bool pumpStarted;
        lock (_gate)
        {
            first = _state == StateCreated;
            if (first)
                _state = StateDisposing;
            pumpStarted = _pump is not null;
        }

        if (first)
        {
            _disposeCts.Cancel();
            foreach (QuiclyRpcConnection connection in _connections.Values)
                connection.TryClose(null, RpcCloseOrigin.TransportDisposed);

            if (pumpStarted)
            {
                _stopRequested = true;
                Wake();
            }
            else
            {
                ReleaseNativeResources();
                _pumpExited.TrySetResult();
            }
        }

        try
        {
            await _pumpExited.Task.WaitAsync(PumpExitTimeout).ConfigureAwait(false);
        }
        catch (TimeoutException ex)
        {
            Report("The QUIC pump thread did not stop in time; its resources are left to finalization.", ex);
        }

        lock (_gate)
        {
            _state = StateDisposed;
        }
    }

    // ------------------------------------------------------------------ internal API (any thread)

    /// <inheritdoc />
    public void Wake()
    {
        try
        {
            _wake.Set();
        }
        catch (ObjectDisposedException)
        {
            // Late callback after shutdown.
        }
    }

    /// <summary>
    /// Queues an action for the pump thread. Before the pump starts and after it stopped, nothing else touches the
    /// pump-owned state, so the action runs inline (serialised with other inline actions).
    /// </summary>
    public void Post(Action action)
    {
        lock (_gate)
        {
            if (!_pumpStopped && _pump is not null)
            {
                _actions.Enqueue(action);
                Wake();
                return;
            }
        }

        lock (_inlineGate)
        {
            RunAction(action);
        }
    }

    /// <summary>Hands a connection with queued frames to the pump.</summary>
    public void ScheduleSend(QuiclyRpcConnection connection)
    {
        if (connection.TryClaimSendSchedule())
        {
            _sendReady.Enqueue(connection);
            Wake();
        }
    }

    /// <summary>Called once when a connection closed (any thread): forgets it and closes its Quicly session on the pump.</summary>
    public void OnConnectionClosed(QuiclyRpcConnection connection, RpcCloseOrigin origin, Exception? error)
    {
        _connections.TryRemove(connection.Id, out _);
        Post(() => CloseSession(connection, origin, error));
    }

    /// <summary>Reports a diagnostic message (never throws).</summary>
    public void Report(string message, Exception? exception)
    {
        Action<string>? sink = Options.Diagnostic;
        if (sink is null)
            return;
        try
        {
            sink(exception is null ? $"[quic {LocalName}] {message}" : $"[quic {LocalName}] {message} {exception.GetType().Name}: {exception.Message}");
        }
        catch (Exception)
        {
            // Diagnostics must never affect the transport.
        }
    }

    // ------------------------------------------------------------------ connect

    private async Task<IRpcConnection> ConnectOnceAsync(RpcAddress address, IPEndPoint endPoint, CancellationToken ct)
    {
        ClientPool pool = GetClientPool();
        if (!pool.TryAcquire())
            throw new ObjectDisposedException(nameof(QuiclyRpcTransport));
        var lease = new ClientLease(pool);
        var connection = new QuiclyRpcConnection(this, NextConnectionId(), address, remoteName: null, isClientSide: true);
        QuiclyClient client;
        try
        {
            client = new QuiclyClient(new WakingConnector(GetConnector(), this, lease));
        }
        catch (Exception ex)
        {
            lease.TransportClosed();
            lease.ClientDisposed();
            throw new RpcConnectionException($"Could not create a QUIC client for {address}: {ex.Message}", ex);
        }

        var clientOptions = new ClientOptions
        {
            Channels = _channels,
            PeerOptions = CreatePeerOptions(pool.Allocator),
            AuthToken = _authToken,
            ServerName = IPAddress.TryParse(address.Host, out _) ? null : address.Host,
            Reconnect = null,
            PeerCreated = peer =>
            {
                peer.RegisterHandler(QuiclyEnvelope.ControlChannel, connection.OnMessage);
                peer.RegisterHandler(QuiclyEnvelope.DataChannel, connection.OnMessage);
            },
        };

        bool handedToPump = false;
        try
        {
            QuiclyPeer peer;
            try
            {
                peer = await client.ConnectAsync(endPoint, clientOptions, ct).ConfigureAwait(false);
            }
            catch (QuiclyConnectException ex)
            {
                throw new RpcConnectionException(ex.HandshakeStatus == HelloStatus.Rejected
                    ? $"The server at {address} refused the connection from '{LocalName}': {ex.Message}"
                    : $"Could not connect to {address}: {ex.Message}", ex);
            }
            catch (MsQuicException ex)
            {
                throw new RpcConnectionException($"Could not connect to {address}: {ex.Message}", ex);
            }
            catch (ObjectDisposedException ex) when (!_disposeCts.IsCancellationRequested)
            {
                throw new RpcConnectionException($"The connection to {address} was aborted.", ex);
            }

            var attached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            handedToPump = true;
            Post(() =>
            {
                if (_stopRequested || !connection.IsOpen)
                {
                    DisposeClient(client, lease);
                    connection.TryClose(new RpcConnectionException("The transport is shutting down."), RpcCloseOrigin.Fault);
                    attached.TrySetException(new RpcConnectionException($"The connection to {address} closed while it was being established."));
                    return;
                }

                connection.Client = client;
                connection.Peer = peer;
                connection.Lease = lease;
                _clients.Add(connection);
                _connections[connection.Id] = connection;
                if (connection.HasOutbound)
                    ScheduleSend(connection);
                attached.TrySetResult();
            });

            await attached.Task.ConfigureAwait(false);
            string serverName;
            try
            {
                serverName = await connection.Preamble!.Task.WaitAsync(ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                connection.TryClose(new RpcConnectionException("The connect was cancelled before the server's preamble arrived."), RpcCloseOrigin.Fault);
                throw;
            }

            bool allowed;
            try
            {
                allowed = Policy.CanConnect(LocalName, serverName);
            }
            catch (Exception ex)
            {
                connection.TryClose(new RpcConnectionException("The link policy failed."), RpcCloseOrigin.Fault);
                throw new RpcConnectionException($"The link policy failed while connecting '{LocalName}' to '{serverName}'.", ex);
            }

            if (!allowed)
            {
                connection.TryClose(new RpcConnectionException("Refused by the link policy."), RpcCloseOrigin.Fault);
                throw new RpcConnectionException($"The connection from '{LocalName}' to '{serverName}' at {address} was refused (link down).");
            }

            if (!connection.IsOpen)
                throw new RpcConnectionException($"The connection to {address} closed while it was being established.", connection.CloseError);
            return connection;
        }
        finally
        {
            if (!handedToPump)
            {
                // The peer's construction can fail before the connector is asked for a transport; then nothing will call back.
                if (!lease.ConnectorInvoked)
                    lease.TransportClosed();
                DisposeClient(client, lease);
                connection.TryClose(null, RpcCloseOrigin.Fault);
            }
        }
    }

    // ------------------------------------------------------------------ pump thread

    private void EnsurePump()
    {
        lock (_gate)
        {
            if (_pump is not null)
                return;
            ObjectDisposedException.ThrowIf(_state != StateCreated, this);
            _pump = new Thread(PumpMain)
            {
                IsBackground = true,
                Name = "tdfs-quic-" + LocalName,
            };
            _pump.Start();
        }
    }

    private void PumpMain()
    {
        TimerResolution.Acquire();
        try
        {
            while (!_stopRequested)
            {
                _wake.Reset();
                RunPass();
                if (_stopRequested)
                    break;
                if (_moreToSend || !_actions.IsEmpty || !_sendReady.IsEmpty)
                    continue;
                _wake.Wait(IdleWait);
            }

            Shutdown();
        }
        catch (Exception ex)
        {
            Report("The QUIC pump failed unexpectedly; the transport stops.", ex);

            // Nothing polls any more: actions that run inline from now on (attaching a client, starting a listener) must
            // refuse instead of creating Quicly objects nobody drives.
            _stopRequested = true;
            try
            {
                foreach (QuiclyRpcConnection connection in _connections.Values)
                    connection.TryClose(new RpcConnectionException("The transport failed.", ex), RpcCloseOrigin.Fault);
                Shutdown();
            }
            catch (Exception inner)
            {
                Report("The QUIC pump could not shut down cleanly.", inner);
            }
        }
        finally
        {
            lock (_gate)
            {
                _pumpStopped = true;
            }

            lock (_inlineGate)
            {
                RunActions();
                FailAllOutbound();
            }

            TimerResolution.Release();
            _pumpExited.TrySetResult();
        }
    }

    private void RunPass()
    {
        RunActions();
        if (_server is { } server)
        {
            try
            {
                server.PollAll();
            }
            catch (Exception ex)
            {
                Report("QuiclyServer.PollAll failed.", ex);
            }
        }

        PollClients();
        SendPass();
        CloseDrainedServerPeers(force: false);
        Flush();
    }

    private void RunActions()
    {
        while (_actions.TryDequeue(out Action? action))
            RunAction(action);
    }

    private void RunAction(Action action)
    {
        try
        {
            action();
        }
        catch (Exception ex)
        {
            Report("A queued transport action failed.", ex);
        }
    }

    private void PollClients()
    {
        long nowMicros = MonotonicClock.Instance.NowMicros; // the peers' clock (PeerOptions.Clock default)
        for (int i = _clients.Count - 1; i >= 0; i--)
        {
            QuiclyRpcConnection connection = _clients[i];
            QuiclyClient? client = connection.Client;
            if (client is null)
            {
                _clients.RemoveAt(i);
                continue;
            }

            // Skip an idle client: HasPendingWork is Quicly's authority on whether a Poll has anything to serve (it re-arms
            // the work edge when it answers false), and the poll deadline covers its timers (pings, heartbeat, linger).
            QuiclyPeer? peer = client.Peer;
            if (peer is not null && client.State == PeerState.Connected && !peer.HasPendingWork && nowMicros < peer.NextPollDeadlineMicros)
                continue;

            connection.NeedsFlush = true;
            try
            {
                client.Poll();
            }
            catch (Exception ex)
            {
                Report("QuiclyClient.Poll failed.", ex);
            }

            if (client.State != PeerState.Closed)
                continue;
            _clients.RemoveAt(i);
            CloseReason reason = client.Peer?.CloseReason ?? default;

            // Queued behind the messages this connection already received (replies, one-way messages), so they are still
            // delivered, exactly as the in-memory transport does; it also keeps handler code off the pump thread.
            connection.PostRemoteClose(CreateRemoteCloseError(connection, reason));
            ReleaseClient(connection);
        }

        long now = PumpClock.GetTimestamp();
        for (int i = _closingClients.Count - 1; i >= 0; i--)
        {
            QuiclyRpcConnection connection = _closingClients[i];
            QuiclyClient? client = connection.Client;
            if (client is not null)
            {
                try
                {
                    client.Poll();
                }
                catch (Exception ex)
                {
                    Report("QuiclyClient.Poll failed while closing.", ex);
                }

                if (connection.PendingClose is { } pending && client.State != PeerState.Closed)
                {
                    // Graceful close: Close only once what was handed to Quicly has been delivered (or the wait ran out).
                    if (client.State == PeerState.Connected && client.Peer is { } peer && HasUndeliveredSends(peer) && now < connection.CloseDeadline)
                        continue;
                    connection.PendingClose = null;
                    connection.CloseDeadline = now + ToTimestampTicks(CloseGrace);
                    if (client.State == PeerState.Connected)
                    {
                        try
                        {
                            client.Close(pending);
                        }
                        catch (Exception ex)
                        {
                            Report("QuiclyClient.Close failed.", ex);
                        }
                    }

                    continue;
                }

                if (client.State != PeerState.Closed && now < connection.CloseDeadline)
                    continue;
            }

            connection.PendingClose = null;
            _closingClients.RemoveAt(i);
            ReleaseClient(connection);
        }
    }

    /// <summary>
    /// Sends the Quicly close of gracefully closed server connections once everything they handed to Quicly was delivered,
    /// or when their delivery wait ran out (pump thread).
    /// </summary>
    private void CloseDrainedServerPeers(bool force)
    {
        if (_drainingPeers.Count == 0)
            return;
        long now = PumpClock.GetTimestamp();
        for (int i = _drainingPeers.Count - 1; i >= 0; i--)
        {
            QuiclyRpcConnection connection = _drainingPeers[i];
            QuiclyPeer? peer = connection.Peer; // cleared when the server reports the peer closed
            bool usable = peer is not null && !peer.IsDisposed && peer.State is not (PeerState.Closing or PeerState.Closed);
            if (usable && !force && HasUndeliveredSends(peer!) && now < connection.CloseDeadline)
                continue;

            _drainingPeers.RemoveAt(i);
            CloseReason? pending = connection.PendingClose;
            connection.PendingClose = null;
            if (usable && pending is { } reason)
                ClosePeer(peer!, reason);
        }
    }

    /// <summary>
    /// True while a message handed to Quicly on one of the RPC channels waits to be transmitted or acknowledged (MsQuic
    /// runs without send buffering, so a reliable send completes when the peer acknowledged it). Pump thread.
    /// </summary>
    private static bool HasUndeliveredSends(QuiclyPeer peer)
    {
        if (peer.IsDisposed)
            return false;
        return Pending(peer, QuiclyEnvelope.ControlChannel) || Pending(peer, QuiclyEnvelope.DataChannel);

        static bool Pending(QuiclyPeer peer, ushort channel)
            => peer.GetChannelStatistics(channel, out ChannelStatistics statistics) && (statistics.QueuedMessages > 0 || statistics.InFlightMessages > 0);
    }

    private void ClosePeer(QuiclyPeer peer, CloseReason reason)
    {
        try
        {
            peer.Close(reason);
        }
        catch (Exception ex)
        {
            Report("QuiclyPeer.Close failed.", ex);
        }
    }

    private void SendPass()
    {
        _moreToSend = false;
        while (_sendReady.TryDequeue(out QuiclyRpcConnection? ready))
            _sending.Add(ready);

        for (int i = _sending.Count - 1; i >= 0; i--)
        {
            QuiclyRpcConnection connection = _sending[i];
            SendOutcome outcome;
            try
            {
                outcome = connection.PumpSends(_scratch, FramesPerConnectionPerPass);
            }
            catch (Exception ex)
            {
                Report("Sending queued frames failed.", ex);
                connection.TryClose(new RpcConnectionException($"Sending to {connection.Remote} failed.", ex), RpcCloseOrigin.Fault);
                connection.FailOutbound();
                outcome = SendOutcome.Drained;
            }

            if (outcome == SendOutcome.More)
            {
                _moreToSend = true;
                continue;
            }

            if (outcome == SendOutcome.Blocked)
                continue;

            _sending[i] = _sending[^1];
            _sending.RemoveAt(_sending.Count - 1);
            if (connection.ReleaseSendSchedule())
                _sending.Add(connection);
        }
    }

    private void Flush()
    {
        if (_server is { } server)
        {
            try
            {
                server.FlushAll();
            }
            catch (Exception ex)
            {
                Report("QuiclyServer.FlushAll failed.", ex);
            }
        }

        FlushClients(_clients, always: false);
        FlushClients(_closingClients, always: true);
    }

    private void FlushClients(List<QuiclyRpcConnection> connections, bool always)
    {
        long nowMicros = MonotonicClock.Instance.NowMicros;
        foreach (QuiclyRpcConnection connection in connections)
        {
            QuiclyClient? client = connection.Client;
            if (client is null)
                continue;

            // A Flush has work only after a Poll or a send in this pass, or when the peer's engine deadline is due.
            bool due = always || connection.NeedsFlush || client.Peer is not { } peer || nowMicros >= peer.NextFlushDeadlineMicros;
            connection.NeedsFlush = false;
            if (!due)
                continue;
            try
            {
                client.Flush();
            }
            catch (Exception ex)
            {
                Report("QuiclyClient.Flush failed.", ex);
            }
        }
    }

    /// <summary>
    /// Closes the Quicly session of a connection that closed (pump thread). Frames still queued in the connection are
    /// failed. A graceful local close (<see cref="RpcCloseOrigin.Local"/>, <see cref="RpcCloseOrigin.TransportDisposed"/>)
    /// sends the Quicly close only once every message already handed to Quicly was delivered (bounded by
    /// <see cref="CloseGrace"/>): Quicly's close travels on its own control stream and would otherwise end the session
    /// while admitted data (up to the send budget) is still queued, losing messages whose <c>SendAsync</c> completed.
    /// </summary>
    private void CloseSession(QuiclyRpcConnection connection, RpcCloseOrigin origin, Exception? error)
    {
        connection.FailOutbound();
        _sending.Remove(connection);
        CloseReason reason = CreateCloseReason(origin, error);
        bool graceful = origin is RpcCloseOrigin.Local or RpcCloseOrigin.TransportDisposed;
        long deadline = PumpClock.GetTimestamp() + ToTimestampTicks(CloseGrace);
        if (connection.IsClientSide)
        {
            // A client that is not attached is still driven by its connecting thread (which disposes it), so its
            // game-thread state (the reassembler) is not touched here.
            if (connection.Client is not { } client)
                return;
            connection.Reassembler.Clear();
            _clients.Remove(connection);
            if (client.State == PeerState.Connected)
            {
                connection.CloseDeadline = deadline;
                if (!_closingClients.Contains(connection))
                    _closingClients.Add(connection);
                if (graceful && client.Peer is { } clientPeer && HasUndeliveredSends(clientPeer))
                {
                    connection.PendingClose = reason; // closed by PollClients once delivered
                    return;
                }

                try
                {
                    client.Close(reason);
                    return;
                }
                catch (Exception ex)
                {
                    _closingClients.Remove(connection);
                    Report("QuiclyClient.Close failed.", ex);
                }
            }

            ReleaseClient(connection);
            return;
        }

        connection.Reassembler.Clear(); // server peers are polled by this thread only
        if (connection.Peer is { } peer && !peer.IsDisposed && peer.State is not (PeerState.Closing or PeerState.Closed))
        {
            if (graceful && HasUndeliveredSends(peer))
            {
                connection.PendingClose = reason; // closed by CloseDrainedServerPeers once delivered
                connection.CloseDeadline = deadline;
                _drainingPeers.Add(connection);
                return;
            }

            ClosePeer(peer, reason);
        }
    }

    private void ReleaseClient(QuiclyRpcConnection connection)
    {
        QuiclyClient? client = connection.Client;
        ClientLease? lease = connection.Lease;
        connection.Client = null;
        connection.Peer = null;
        connection.Lease = null;
        if (client is not null && lease is not null)
            DisposeClient(client, lease);
    }

    private void DisposeClient(QuiclyClient client, ClientLease lease)
    {
        try
        {
            client.Dispose();
        }
        catch (Exception ex)
        {
            Report("QuiclyClient.Dispose failed.", ex);
        }

        lease.ClientDisposed();
    }

    private void Shutdown()
    {
        RunActions();

        // Close everything gracefully, then keep pumping until the sessions are closed or the grace period ends.
        foreach (QuiclyRpcConnection connection in _connections.Values)
            connection.TryClose(null, RpcCloseOrigin.TransportDisposed);
        RunActions();

        // First let every graceful close deliver what was already handed to Quicly (the server's shutdown below would
        // close its peers at once).
        QuiclyServer? server = _server;
        long drainDeadline = PumpClock.GetTimestamp() + ToTimestampTicks(CloseGrace);
        while ((_drainingPeers.Count > 0 || _closingClients.Exists(static c => c.PendingClose is not null))
               && PumpClock.GetTimestamp() < drainDeadline)
        {
            _wake.Reset();
            RunActions();
            if (server is not null)
            {
                try
                {
                    server.PollAll();
                }
                catch (Exception ex)
                {
                    Report("QuiclyServer.PollAll failed during shutdown.", ex);
                }
            }

            PollClients();
            CloseDrainedServerPeers(force: false);
            Flush();
            _wake.Wait(IdleWait);
        }

        CloseDrainedServerPeers(force: true);

        for (int i = _clients.Count - 1; i >= 0; i--)
        {
            QuiclyRpcConnection connection = _clients[i];
            _clients.RemoveAt(i);
            try
            {
                if (connection.Client is { State: PeerState.Connected } client)
                {
                    client.Close(new CloseReason(QuiclyErrorCode.NoError, "transport stopped"));
                    connection.CloseDeadline = PumpClock.GetTimestamp() + ToTimestampTicks(CloseGrace);
                    _closingClients.Add(connection);
                    continue;
                }
            }
            catch (Exception ex)
            {
                Report("QuiclyClient.Close failed during shutdown.", ex);
            }

            ReleaseClient(connection);
        }

        if (server is not null)
        {
            try
            {
                server.BeginShutdown(new CloseReason(QuiclyErrorCode.NoError, "transport stopped"));
            }
            catch (Exception ex)
            {
                Report("QuiclyServer.BeginShutdown failed.", ex);
            }
        }

        long deadline = PumpClock.GetTimestamp() + ToTimestampTicks(CloseGrace);
        while ((_closingClients.Count > 0 || (server is not null && !server.IsShutdownComplete))
               && PumpClock.GetTimestamp() < deadline)
        {
            _wake.Reset();
            RunActions();
            if (server is not null)
            {
                try
                {
                    server.PollAll();
                }
                catch (Exception ex)
                {
                    Report("QuiclyServer.PollAll failed during shutdown.", ex);
                }
            }

            PollClients();
            Flush();
            _wake.Wait(IdleWait);
        }

        foreach (QuiclyRpcConnection connection in _closingClients)
            ReleaseClient(connection);
        _closingClients.Clear();

        if (server is not null)
        {
            _server = null;
            try
            {
                server.DisposeAsync().AsTask().GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                Report("QuiclyServer.DisposeAsync failed.", ex);
            }
        }

        _serverConnections.Clear();
        ReleaseNativeResources();
    }

    private void FailAllOutbound()
    {
        foreach (QuiclyRpcConnection connection in _sending)
            connection.FailOutbound();
        _sending.Clear();
        while (_sendReady.TryDequeue(out QuiclyRpcConnection? connection))
            connection.FailOutbound();
    }

    /// <summary>
    /// Releases the connector, the client buffer pool and the MsQuic registration. Called once the server and every client
    /// were disposed (or never existed).
    /// </summary>
    private void ReleaseNativeResources()
    {
        MsQuicTransportConnector? connector;
        ClientPool? pool;
        MsQuicRegistration? registration;
        lock (_gate)
        {
            connector = _connector;
            pool = _clientPool;
            registration = _registration;
            _connector = null;
            _registration = null;
        }

        try
        {
            connector?.Dispose();
        }
        catch (Exception ex)
        {
            Report("Disposing the MsQuic connector failed.", ex);
        }

        pool?.ReleaseOwner();
        if (registration is null)
        {
            _nativeReleased.TrySetResult();
            return;
        }

        // RegistrationClose blocks until every connection, configuration and listener of the registration is closed, which
        // MsQuic and the Quicly transports finish asynchronously (handles are released on the thread pool after the
        // connection's shutdown completes). Wait for that on a background thread so disposal never blocks on the network.
        var closer = new Thread(static state =>
        {
            (QuiclyRpcTransport owner, MsQuicRegistration toClose) = ((QuiclyRpcTransport, MsQuicRegistration))state!;
            try
            {
                toClose.Close();
            }
            catch (Exception ex)
            {
                owner.Report("Closing the MsQuic registration failed.", ex);
            }
            finally
            {
                owner._nativeReleased.TrySetResult();
            }
        })
        {
            IsBackground = true,
            Name = "tdfs-quic-close-" + LocalName,
        };
        closer.Start((this, registration));
    }

    // ------------------------------------------------------------------ server (pump thread)

    private void StartServer(IPEndPoint endPoint, X509Certificate2 certificate)
    {
        if (_server is not null)
            throw new InvalidOperationException("The transport is already listening.");

        MsQuicTransportListener listener;
        try
        {
            listener = new MsQuicTransportListener(endPoint, certificate, CreateMsQuicOptions(client: false), GetRegistration());
        }
        catch (MsQuicException ex)
        {
            throw new RpcConnectionException($"Could not listen on {_listen}: {ex.Message}", ex);
        }

        var options = new ServerOptions
        {
            ListenEndPoint = endPoint,
            Channels = _channels,
            MaxPeers = Options.MaxInboundConnections,
            PeerOptions = CreatePeerOptions(allocator: null),
            ShutdownReason = new CloseReason(QuiclyErrorCode.NoError, "transport stopped"),
            ShutdownTimeout = CloseGrace,
        };
        options.Admission.MaxConnectionsPerAddress = Options.MaxConnectionsPerAddress;
        options.Admission.AuthTokenValidator = ValidateAuthToken;

        // Quicly's auth token only carries our network identity (application authentication runs on top), so the
        // per-address failure limiter must not lock out every localhost peer after a few link-policy refusals.
        options.Admission.AuthFailureBurst = 1_000_000;
        options.Admission.AuthFailureRefillInterval = TimeSpan.FromMilliseconds(1);
        options.Sessions.Grace = TimeSpan.Zero; // sessions are never resumed; a lost connection is a new connection

        QuiclyServer server;
        try
        {
            server = new QuiclyServer(options, new WakingListener(listener, this));
        }
        catch
        {
            listener.Dispose();
            throw;
        }

        server.PeerAdmitted += OnPeerAdmitted;
        server.PeerClosed += OnPeerClosed;
        server.AdmissionFailed += failure => Report($"Admission refused ({failure.Stage}, {failure.Reason}) for {failure.RemoteEndPoint}.", failure.Exception);
        try
        {
            server.StartAsync().GetAwaiter().GetResult();
        }
        catch (Exception ex)
        {
            try
            {
                server.DisposeAsync().AsTask().GetAwaiter().GetResult();
            }
            catch (Exception inner)
            {
                Report("Disposing a server that failed to start failed.", inner);
            }

            if (ex is MsQuicException)
                throw new RpcConnectionException($"Could not listen on {_listen}: {ex.Message}", ex);
            throw;
        }

        _server = server;
        IPEndPoint bound = server.LocalEndPoint;
        IPAddress boundAddress = bound.Address.IsIPv4MappedToIPv6 ? bound.Address.MapToIPv4() : bound.Address;
        Volatile.Write(ref _localEndpoint, RpcAddress.Quic(boundAddress.ToString(), bound.Port).ToString());
    }

    private AuthTokenDecision ValidateAuthToken(in AuthTokenContext context)
    {
        if (_stopRequested || !QuiclyEnvelope.TryDecodeAuthToken(context.AuthToken, out string? clientName))
            return AuthTokenDecision.Reject;
        try
        {
            if (!Policy.CanConnect(clientName!, LocalName))
                return AuthTokenDecision.Reject;
        }
        catch (Exception ex)
        {
            Report("The link policy failed during admission; the connection is refused.", ex);
            return AuthTokenDecision.Reject;
        }

        _admissionNames.AddOrUpdate(context.Peer, clientName!);
        return AuthTokenDecision.Accept;
    }

    private void OnPeerAdmitted(QuiclyPeer peer)
    {
        try
        {
            if (!_admissionNames.TryGetValue(peer, out string? remoteName) || _stopRequested)
            {
                peer.Close(new CloseReason(QuiclyErrorCode.AdmissionRejected, "not admitted"));
                return;
            }

            _admissionNames.Remove(peer);
            IPEndPoint? remoteEndPoint = peer.RemoteEndPoint;
            RpcAddress remote = remoteEndPoint is null
                ? RpcAddress.Quic("0.0.0.0", 0)
                : RpcAddress.Quic((remoteEndPoint.Address.IsIPv4MappedToIPv6 ? remoteEndPoint.Address.MapToIPv4() : remoteEndPoint.Address).ToString(), remoteEndPoint.Port);
            var connection = new QuiclyRpcConnection(this, NextConnectionId(), remote, remoteName, isClientSide: false)
            {
                Peer = peer,
            };
            peer.RegisterHandler(QuiclyEnvelope.ControlChannel, connection.OnMessage);
            peer.RegisterHandler(QuiclyEnvelope.DataChannel, connection.OnMessage);
            _serverConnections[peer] = connection;
            _connections[connection.Id] = connection;
            connection.EnqueuePreamble(LocalName);
        }
        catch (Exception ex)
        {
            Report("Accepting an admitted peer failed.", ex);
            try
            {
                peer.Close(new CloseReason(QuiclyErrorCode.InternalError, "accept failed"));
            }
            catch (Exception)
            {
                // The peer is closing anyway.
            }
        }
    }

    private void OnPeerClosed(QuiclyPeer peer, CloseReason reason)
    {
        if (!_serverConnections.Remove(peer, out QuiclyRpcConnection? connection))
            return;
        connection.Peer = null;

        // Behind every message already received from the peer (see PollClients).
        connection.PostRemoteClose(CreateRemoteCloseError(connection, reason));
    }

    // ------------------------------------------------------------------ configuration

    private static ChannelTable CreateChannelTable()
        => ChannelTable.Create()
            .Add(QuiclyEnvelope.ControlChannel, "tdfs.rpc", ChannelMode.ReliableOrdered, o =>
            {
                o.Compression = ChannelCompression.Lz4;
                o.MinCompressSize = 256;
                o.MaxMessageSize = 64 * 1024;
            })
            .Add(QuiclyEnvelope.DataChannel, "tdfs.data", ChannelMode.ReliableUnordered, o =>
            {
                o.MaxGroups = 8;
                o.GroupMaxBytes = 1 << 20;
                o.MaxMessageSize = 64 * 1024;
            })
            .Build();

    /// <summary>Peer options of server peers (shared pool built by the server from these options) and clients (shared client pool).</summary>
    private static PeerOptions CreatePeerOptions(SlabAllocator? allocator)
    {
        var options = new PeerOptions
        {
            ThreadSafeSend = true,
            SendBudgetBytes = 2 << 20,
            ReceiveBudgetBytes = 2 << 20,
            DecodedBytesPerSecond = int.MaxValue, // compressed reliable messages beyond the rate would be dropped
            GroupMinInterval = TimeSpan.Zero,     // the pump flushes continuously; never hold data back for stream churn
            CloseLinger = TimeSpan.FromMilliseconds(500),
        };
        if (allocator is not null)
            options.Allocator = allocator;
        else
            options.AllocatorOptions = CreateServerPoolOptions();
        return options;
    }

    private static SlabAllocatorOptions CreateServerPoolOptions() => new()
    {
        SizeClasses =
        [
            new(64, 8192),
            new(256, 2048),
            new(1536, 1024),
            new(4096, 512),
            new(16384, 128),
            new(65536, 128),
            new(262144, 4),
        ],
    };

    private static SlabAllocatorOptions CreateClientPoolOptions() => new()
    {
        SizeClasses =
        [
            new(64, 4096),
            new(256, 2048),
            new(1536, 512),
            new(4096, 256),
            new(16384, 64),
            new(65536, 96),
            new(262144, 4),
        ],
    };

    private MsQuicTransportOptions CreateMsQuicOptions(bool client)
    {
        var options = new MsQuicTransportOptions
        {
            AppName = "tdfs",
            Diagnostic = (level, message, exception) =>
            {
                if (level != TransportDiagnosticLevel.Information)
                    Report($"MsQuic {level}: {message}", exception);
            },
        };
        if (!client)
            return options;

        IReadOnlyList<byte[]> pins = Options.TrustedSpkiPins;
        Func<X509Certificate2, bool>? callback = Options.ValidateServerCertificate;
        bool allowUntrusted = Options.AllowUntrustedForDevelopment;
        if (callback is null && !allowUntrusted)
        {
            if (pins.Count > 0)
            {
                options.ServerCertificateValidation = ServerCertificateValidationMode.PinnedSpki;
                foreach (byte[] pin in pins)
                    options.PinnedSpkiSha256.Add((byte[])pin.Clone());
            }

            return options; // pins, or the platform trust store when none are configured
        }

        byte[][] pinCopies = [.. pins.Select(p => (byte[])p.Clone())];
        options.ServerCertificateValidation = ServerCertificateValidationMode.Callback;
        options.ServerCertificateValidator = (in ServerCertificateContext context) =>
        {
            try
            {
                if (pinCopies.Length > 0)
                {
                    byte[] spki = SpkiPin.Compute(context.LeafDer);
                    foreach (byte[] pin in pinCopies)
                    {
                        if (CryptographicOperations.FixedTimeEquals(pin, spki))
                            return ServerCertificateDecision.AcceptIgnoringPlatformValidation;
                    }
                }

                if (callback is not null)
                {
                    using X509Certificate2 certificate = X509CertificateLoader.LoadCertificate(context.LeafDer);
                    return callback(certificate) ? ServerCertificateDecision.AcceptIgnoringPlatformValidation : ServerCertificateDecision.Reject;
                }

                return allowUntrusted ? ServerCertificateDecision.AcceptIgnoringPlatformValidation : ServerCertificateDecision.Reject;
            }
            catch (Exception ex)
            {
                Report("Server certificate validation failed; the certificate is rejected.", ex);
                return ServerCertificateDecision.Reject;
            }
        };
        return options;
    }

    private ClientPool GetClientPool()
    {
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_state != StateCreated, this);
            return _clientPool ??= new ClientPool(CreateClientPoolOptions());
        }
    }

    private MsQuicTransportConnector GetConnector()
    {
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_state != StateCreated, this);
            return _connector ??= new MsQuicTransportConnector(CreateMsQuicOptions(client: true), GetRegistrationLocked());
        }
    }

    /// <summary>
    /// The MsQuic registration shared by this transport's listener and connector. The transport owns it: a listener that
    /// owns its registration in Tedd.Quicly 0.2.2 can close the registration before its own listener handle while
    /// disposing (RegistrationClose then waits for the listener forever), so the registration is always borrowed.
    /// </summary>
    private MsQuicRegistration GetRegistration()
    {
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_state != StateCreated, this);
            return GetRegistrationLocked();
        }
    }

    private MsQuicRegistration GetRegistrationLocked() => _registration ??= new MsQuicRegistration("tdfs");

    private long NextConnectionId() => Interlocked.Increment(ref _nextConnectionId);

    private static long ToTimestampTicks(TimeSpan span) => (long)(span.TotalSeconds * PumpClock.TimestampFrequency);

    private static CloseReason CreateCloseReason(RpcCloseOrigin origin, Exception? error)
    {
        return origin switch
        {
            RpcCloseOrigin.Reset => new CloseReason(QuiclyErrorCode.NoError, "connection reset"),
            RpcCloseOrigin.Fault => new CloseReason(QuiclyErrorCode.InternalError, Truncate(error?.Message ?? "transport fault")),
            RpcCloseOrigin.TransportDisposed => new CloseReason(QuiclyErrorCode.NoError, "transport stopped"),
            _ => new CloseReason(QuiclyErrorCode.NoError, "connection closed"),
        };
    }

    private static RpcConnectionException CreateRemoteCloseError(QuiclyRpcConnection connection, CloseReason reason)
    {
        string who = connection.RemoteName is { } name ? $"'{name}' ({connection.Remote})" : connection.Remote.ToString();
        return reason.Source switch
        {
            CloseSource.Peer => new RpcConnectionException($"The connection was closed by {who}: {reason.Reason ?? reason.Code.ToString()}."),
            CloseSource.Local => new RpcConnectionException($"The connection to {who} was closed locally ({reason.Code}: {reason.Reason})."),
            _ => new RpcConnectionException($"The connection to {who} was lost ({reason.Code}{(reason.Reason is null ? string.Empty : ": " + reason.Reason)}, transport status 0x{reason.TransportStatus:X8})."),
        };
    }

    private static string Truncate(string text) => text.Length <= 200 ? text : text[..200];

    private static async Task<IPAddress> ResolveListenAddressAsync(string host, CancellationToken ct)
    {
        if (IPAddress.TryParse(host, out IPAddress? address))
            return address;
        if (string.Equals(host, "localhost", StringComparison.OrdinalIgnoreCase))
            return IPAddress.Loopback;
        IPAddress[] addresses = await Dns.GetHostAddressesAsync(host, ct).ConfigureAwait(false);
        IPAddress? chosen = addresses.FirstOrDefault(a => a.AddressFamily == AddressFamily.InterNetwork) ?? addresses.FirstOrDefault();
        return chosen ?? throw new RpcConnectionException($"The listen host '{host}' did not resolve to any address.");
    }

    private static async Task<IPAddress[]> ResolveConnectAddressesAsync(string host, CancellationToken ct)
    {
        if (IPAddress.TryParse(host, out IPAddress? address))
            return [address];
        IPAddress[] addresses;
        try
        {
            addresses = await Dns.GetHostAddressesAsync(host, ct).ConfigureAwait(false);
        }
        catch (SocketException ex)
        {
            throw new RpcConnectionException($"The host '{host}' could not be resolved: {ex.Message}", ex);
        }

        // IPv4 first: listeners are typically bound to IPv4 addresses (loopback in tests).
        return [.. addresses.Where(a => a.AddressFamily == AddressFamily.InterNetwork), .. addresses.Where(a => a.AddressFamily != AddressFamily.InterNetwork)];
    }
}
