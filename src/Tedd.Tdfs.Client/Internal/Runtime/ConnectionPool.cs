using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Client.Internal.Runtime;

/// <summary>
/// Authenticated connections of one client, pooled per endpoint (ARCHITECTURE §9.1). Concurrent callers share one
/// connection per endpoint and one in-progress connect; dead pooled connections (closed by either side, or declared dead
/// after a timeout on a silently dead link) are detected and replaced, and a connect stuck on a lost handshake message is
/// abandoned once it outlived a patient caller's wait, or a wait of many round trips of the endpoint. Each endpoint keeps
/// one round-trip estimate across its connections (<see cref="RttEstimator"/>): waits for a connection never cut below
/// the handshake's round trips.
/// A caller whose wait outlives a failed connect it joined (started by another caller) makes one fresh attempt. Thread-safe.
/// </summary>
internal sealed class ConnectionPool
{
    /// <summary>
    /// Smoothed round trips of the endpoint a caller must have waited before it may abandon a pending connect:
    /// the handshake takes two, so a connect still pending after this many has lost a message rather than being slow.
    /// </summary>
    private const int StuckHandshakeRoundTrips = 8;

    private readonly ClientRuntime _runtime;
    private readonly Lock _sync = new();
    private readonly Dictionary<string, Slot> _slots = new(StringComparer.Ordinal);
    private bool _closed;

    public ConnectionPool(ClientRuntime runtime)
    {
        ArgumentNullException.ThrowIfNull(runtime);
        _runtime = runtime;
    }

    /// <summary>Number of usable pooled connections (diagnostics).</summary>
    public int OpenCount
    {
        get
        {
            lock (_sync)
                return _slots.Values.Count(s => s.Connection is { IsUsable: true });
        }
    }

    /// <summary>
    /// True when a pooled connection to <paramref name="endpoint"/> was lost (closed by the transport or the peer, not
    /// invalidated by this client) after <paramref name="since"/> (a client clock timestamp): the node at the endpoint may
    /// have restarted, so what was learned from it before (such as which node leads a group) may be stale.
    /// </summary>
    public bool LostSince(string endpoint, long since)
    {
        lock (_sync)
            return _slots.TryGetValue(endpoint, out Slot? slot) && slot.LostAt != 0 && slot.LostAt >= since;
    }

    /// <summary>Returns a pooled usable connection to <paramref name="endpoint"/> without connecting, if one exists.</summary>
    public bool TryGetOpen(string endpoint, out ClientConnection connection)
    {
        lock (_sync)
        {
            if (_slots.TryGetValue(endpoint, out Slot? slot) && slot.Connection is { IsUsable: true } open)
            {
                connection = open;
                return true;
            }
        }

        connection = null!;
        return false;
    }

    /// <summary>
    /// Returns an authenticated connection to <paramref name="endpoint"/>, connecting when none is pooled. A new connection
    /// and the reuse of a pooled one hit the <c>client.connect</c> fault site (F02) of the operation that triggers it. Waiting ends after
    /// <paramref name="waitTimeout"/> (<see cref="TimeoutException"/>) or when the operation is cancelled; the shared
    /// connect itself is bounded by <see cref="DfsClientOptions.ConnectTimeout"/>.
    /// </summary>
    public async Task<ClientConnection> GetAsync(string endpoint, OperationScope scope, TimeSpan waitTimeout)
    {
        ArgumentNullException.ThrowIfNull(endpoint);
        long waitStarted = _runtime.Time.GetTimestamp();
        waitTimeout = ConnectWait(endpoint, scope, waitTimeout);
        var attempt = new JoinedConnect();
        try
        {
            return await GetOnceAsync(endpoint, scope, waitTimeout, waitStarted, attempt).ConfigureAwait(false);
        }
        catch (Exception ex) when (attempt.Joined && !scope.Token.IsCancellationRequested && _runtime.Time.GetElapsedTime(waitStarted) < waitTimeout
                                   && (ex is RpcConnectionException or TimeoutException || (ex is IOException and not DfsException)))
        {
            // The shared connect this caller joined was started by another caller earlier, and it failed (it was abandoned as
            // stuck on a lost handshake message, or it began while the link was cut and the link has healed since). Its
            // outcome may predate the current state of the link, so within its own wait this caller makes one fresh attempt
            // instead of failing: a request that may wait long (an upload, PUB-004) must not fail because a short-waiting one
            // (a keep-alive) gave up on a stuck connect first, or because a connect begun during a partition failed after it.
        }

        return await GetOnceAsync(endpoint, scope, waitTimeout, waitStarted, new JoinedConnect()).ConfigureAwait(false);
    }

    /// <summary>
    /// The wait for a connection to <paramref name="endpoint"/>: <paramref name="waitTimeout"/>, but when no usable connection
    /// is pooled at least the handshake's two round trips at the endpoint's adaptive reply timeout (a link whose latency
    /// grew is reconnected over, not given up on below its round trip), never past what the operation has left.
    /// </summary>
    private TimeSpan ConnectWait(string endpoint, OperationScope scope, TimeSpan waitTimeout)
    {
        RttEstimator rtt;
        lock (_sync)
        {
            Slot slot = SlotOf(endpoint);
            if (slot.Connection is { IsUsable: true })
                return waitTimeout;
            rtt = slot.Rtt;
        }

        TimeSpan handshake = rtt.ReplyTimeout * 2;
        if (handshake <= waitTimeout)
            return waitTimeout;
        TimeSpan remaining = scope.Remaining;
        TimeSpan wait = handshake < remaining ? handshake : remaining;
        return wait > waitTimeout ? wait : waitTimeout;
    }

    /// <summary>The round-trip estimate of <paramref name="endpoint"/> (shared by its successive connections).</summary>
    public RttEstimator RttOf(string endpoint)
    {
        ArgumentNullException.ThrowIfNull(endpoint);
        lock (_sync)
            return SlotOf(endpoint).Rtt;
    }

    /// <summary>Whether a <see cref="GetOnceAsync"/> call waited for a connect another caller had started.</summary>
    private sealed class JoinedConnect
    {
        public bool Joined { get; set; }
    }

    private async Task<ClientConnection> GetOnceAsync(string endpoint, OperationScope scope, TimeSpan waitTimeout, long waitStarted, JoinedConnect attempt)
    {
        Task<ClientConnection>? pending = null;
        ClientConnection? pooled = null;
        bool mustStart = false;
        lock (_sync)
        {
            ObjectDisposedException.ThrowIf(_closed, typeof(DfsClient));
            Slot slot = SlotOf(endpoint);
            if (slot.Connection is { IsUsable: true } open)
                pooled = open;
            else if (slot.Connection is not null)
            {
                slot.Connection.Poison();
                Forget(slot, slot.Connection);
            }

            if (pooled is null)
            {
                if (slot.Pending is { IsCompleted: true })
                    slot.Pending = null; // a finished connect either pooled its connection (now unusable) or failed: start afresh
                if (slot.Pending is null)
                {
                    mustStart = true;
                }
                else
                {
                    pending = slot.Pending;
                    attempt.Joined = true;
                }
            }
        }

        if (pooled is not null)
        {
            // F02 also covers reusing a pooled connection (test plan §8.2): the operation can still stop here untransmitted.
            await scope.HitAsync(FaultStage.F02, FaultSites.ClientConnect, detail: endpoint).ConfigureAwait(false);
            scope.ThrowIfCancelled();
            return pooled;
        }

        if (mustStart)
        {
            await scope.HitAsync(FaultStage.F02, FaultSites.ClientConnect, detail: endpoint).ConfigureAwait(false);
            scope.ThrowIfCancelled();
            lock (_sync)
            {
                ObjectDisposedException.ThrowIf(_closed, typeof(DfsClient));
                Slot slot = SlotOf(endpoint);
                if (slot.Connection is { IsUsable: true } open)
                    return open;
                if (slot.Pending is { IsCompleted: true })
                    slot.Pending = null;
                if (slot.Pending is null)
                {
                    var abandon = new CancellationTokenSource();
                    slot.PendingAbandon = abandon;
                    slot.PendingStarted = _runtime.Time.GetTimestamp();
                    slot.Pending = ConnectAsync(endpoint, slot, abandon);
                }
                else
                {
                    attempt.Joined = true; // another caller started one meanwhile
                }

                pending = slot.Pending;
            }
        }

        if (pending!.IsCompleted)
            return await pending.ConfigureAwait(false);

        TimeSpan left = waitTimeout - _runtime.Time.GetElapsedTime(waitStarted);
        using var wait = new CancellationTokenSource(left > TimeSpan.Zero ? left : TimeSpan.FromMilliseconds(1), _runtime.Time);
        using CancellationTokenRegistration link = scope.Token.UnsafeRegister(static s => ContextFree.Cancel((CancellationTokenSource)s!), wait);
        try
        {
            return await Waits.WaitAsync(pending, wait.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!scope.Token.IsCancellationRequested && wait.IsCancellationRequested)
        {
            AbandonIfStuck(endpoint, pending, waitTimeout);
            throw new TimeoutException($"No authenticated connection to {endpoint} within {waitTimeout}.");
        }
    }

    /// <summary>
    /// A shared connect that outlived the whole wait of a patient caller without any handshake reply arriving during it (a
    /// reply is a round-trip sample of the endpoint, <see cref="RttEstimator.LastSampleAt"/>) is presumably stuck on a lost handshake message (a
    /// black-holed or partitioned link that may have healed since): it is abandoned so the next caller starts a fresh
    /// connect instead of waiting for the stuck one until the connect timeout. A caller is patient when it waited at least a
    /// tenth of the connect timeout, or at least <see cref="StuckHandshakeRoundTrips"/> smoothed round trips of the endpoint
    /// (a handshake takes two). Callers with shorter waits never abandon a connect, so a slow but progressing
    /// handshake over a high-latency link is not restarted over and over.
    /// </summary>
    private void AbandonIfStuck(string endpoint, Task<ClientConnection> pending, TimeSpan waited)
    {
        CancellationTokenSource? abandon = null;
        lock (_sync)
        {
            if (_slots.TryGetValue(endpoint, out Slot? slot) && ReferenceEquals(slot.Pending, pending)
                && _runtime.Time.GetElapsedTime(Math.Max(slot.PendingStarted, slot.Rtt.LastSampleAt)) >= waited
                && (waited >= _runtime.Options.ConnectTimeout / 10
                    || (slot.Rtt.Smoothed > TimeSpan.Zero && waited >= slot.Rtt.Smoothed * StuckHandshakeRoundTrips)))
            {
                abandon = slot.PendingAbandon;
                slot.Pending = null;
                slot.PendingAbandon = null;
            }
        }

        if (abandon is not null)
            ContextFree.Cancel(abandon);
    }

    /// <summary>Declares <paramref name="connection"/> dead and removes it from the pool (the next call reconnects).</summary>
    public void Invalidate(ClientConnection connection)
    {
        ArgumentNullException.ThrowIfNull(connection);
        lock (_sync)
        {
            if (_slots.TryGetValue(connection.Endpoint, out Slot? slot) && ReferenceEquals(slot.Connection, connection))
                Forget(slot, connection);
        }

        connection.Poison();
    }

    /// <summary>Removes the pooled <paramref name="connection"/> of <paramref name="slot"/> (its round trips stay in the slot's estimate).</summary>
    private static void Forget(Slot slot, ClientConnection connection)
    {
        if (ReferenceEquals(slot.Connection, connection))
            slot.Connection = null;
    }

    /// <summary>Closes every connection; later requests fail with <see cref="ObjectDisposedException"/>.</summary>
    public void CloseAll()
    {
        List<ClientConnection> connections = [];
        lock (_sync)
        {
            _closed = true;
            foreach (Slot slot in _slots.Values)
            {
                if (slot.Connection is not null)
                    connections.Add(slot.Connection);
                slot.Connection = null;
            }

            _slots.Clear();
        }

        foreach (ClientConnection connection in connections)
            connection.Poison();
    }

    private Slot SlotOf(string endpoint)
    {
        if (!_slots.TryGetValue(endpoint, out Slot? slot))
        {
            slot = new Slot();
            _slots[endpoint] = slot;
        }

        return slot;
    }

    private async Task<ClientConnection> ConnectAsync(string endpoint, Slot slot, CancellationTokenSource abandon)
    {
        TimeSpan timeout = _runtime.Options.ConnectTimeout;
        ClientConnection opened;
        using (abandon)
        using (var deadline = new CancellationTokenSource(timeout, _runtime.Time))
        using (_runtime.KillToken.UnsafeRegister(static s => ContextFree.Cancel((CancellationTokenSource)s!), deadline))
        using (abandon.Token.UnsafeRegister(static s => ContextFree.Cancel((CancellationTokenSource)s!), deadline))
        {
            try
            {
                opened = await ClientConnection.OpenAsync(_runtime, endpoint, _runtime.ExpectedClusterIdForConnections, timeout, slot.Rtt, deadline.Token)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (deadline.IsCancellationRequested)
            {
                if (_runtime.KillToken.IsCancellationRequested)
                    throw new ObjectDisposedException(nameof(DfsClient), "The client was disposed while connecting.");
                if (abandon.IsCancellationRequested)
                    throw new TimeoutException($"Connecting to {endpoint} was abandoned: it did not complete while callers waited for it.");
                throw new TimeoutException($"Connecting to {endpoint} did not complete within {timeout}.");
            }

            lock (_sync)
            {
                if (_closed)
                {
                    opened.Poison();
                    throw new ObjectDisposedException(nameof(DfsClient), "The client was disposed while connecting.");
                }

                if (abandon.IsCancellationRequested)
                {
                    // Abandoned (a newer connect may be running for this endpoint): this one is not pooled.
                    opened.Poison();
                    throw new TimeoutException($"Connecting to {endpoint} was abandoned: it did not complete while callers waited for it.");
                }

                slot.Connection = opened;
                slot.Pending = null;
                slot.PendingAbandon = null;
            }
        }

        opened.Raw.Closed += (_, _) =>
        {
            lock (_sync)
            {
                if (ReferenceEquals(slot.Connection, opened))
                {
                    Forget(slot, opened);
                    slot.LostAt = _runtime.Time.GetTimestamp();
                }
            }
        };
        return opened;
    }

    private sealed class Slot
    {
        public ClientConnection? Connection;
        public Task<ClientConnection>? Pending;
        public CancellationTokenSource? PendingAbandon;
        public long PendingStarted;
        public readonly RttEstimator Rtt = new(); // of the endpoint, across its connections (handshakes and replies)
        public long LostAt;            // when a pooled connection was last lost (0: never)
    }
}
