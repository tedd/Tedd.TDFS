using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Client.Internal.Runtime;

/// <summary>
/// Authenticated connections of one client, pooled per endpoint (ARCHITECTURE §9.1). Concurrent callers share one
/// connection per endpoint and one in-progress connect; dead pooled connections (closed by either side, or declared dead
/// after a timeout on a silently dead link) are detected and replaced, and a connect stuck on a lost handshake message is
/// abandoned once it outlived a patient caller's wait, or a wait of many round trips of the endpoint's previous connection.
/// Thread-safe.
/// </summary>
internal sealed class ConnectionPool
{
    /// <summary>
    /// Round trips of the endpoint's previous connection a caller must have waited before it may abandon a pending connect:
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
                    mustStart = true;
                else
                    pending = slot.Pending;
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

                pending = slot.Pending;
            }
        }

        if (pending!.IsCompleted)
            return await pending.ConfigureAwait(false);

        using var wait = new CancellationTokenSource(waitTimeout > TimeSpan.Zero ? waitTimeout : TimeSpan.FromMilliseconds(1), _runtime.Time);
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
    /// A shared connect that outlived the whole wait of a patient caller is presumably stuck on a lost handshake message (a
    /// black-holed or partitioned link that may have healed since): it is abandoned so the next caller starts a fresh
    /// connect instead of waiting for the stuck one until the connect timeout. A caller is patient when it waited at least a
    /// tenth of the connect timeout, or at least <see cref="StuckHandshakeRoundTrips"/> round trips of the endpoint's previous
    /// connection (a handshake takes two). Callers with shorter waits never abandon a connect, so a slow but progressing
    /// handshake over a high-latency link is not restarted over and over.
    /// </summary>
    private void AbandonIfStuck(string endpoint, Task<ClientConnection> pending, TimeSpan waited)
    {
        CancellationTokenSource? abandon = null;
        lock (_sync)
        {
            if (_slots.TryGetValue(endpoint, out Slot? slot) && ReferenceEquals(slot.Pending, pending)
                && _runtime.Time.GetElapsedTime(slot.PendingStarted) >= waited
                && (waited >= _runtime.Options.ConnectTimeout / 10
                    || (slot.LastRoundTrip > TimeSpan.Zero && waited >= slot.LastRoundTrip * StuckHandshakeRoundTrips)))
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

    /// <summary>Removes the pooled <paramref name="connection"/> of <paramref name="slot"/>, remembering its round trip.</summary>
    private static void Forget(Slot slot, ClientConnection connection)
    {
        if (connection.RoundTrip > TimeSpan.Zero)
            slot.LastRoundTrip = connection.RoundTrip;
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
                opened = await ClientConnection.OpenAsync(_runtime, endpoint, _runtime.ExpectedClusterIdForConnections, timeout, deadline.Token)
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
                    Forget(slot, opened);
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
        public TimeSpan LastRoundTrip; // of the endpoint's previous connection (zero when none was ever open)
    }
}
