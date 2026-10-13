using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Client.Internal.Runtime;

/// <summary>
/// Authenticated connections of one client, pooled per endpoint (ARCHITECTURE §9.1). Concurrent callers share one
/// connection per endpoint and one in-progress connect; dead pooled connections (closed by either side, or declared dead
/// after a timeout on a silently dead link) are detected and replaced, and a connect stuck on a lost handshake message is
/// abandoned once it outlived a caller's wait. Thread-safe.
/// </summary>
internal sealed class ConnectionPool
{
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
    /// hits the <c>client.connect</c> fault site (F02) of the operation that triggers it. Waiting ends after
    /// <paramref name="waitTimeout"/> (<see cref="TimeoutException"/>) or when the operation is cancelled; the shared
    /// connect itself is bounded by <see cref="DfsClientOptions.ConnectTimeout"/>.
    /// </summary>
    public async Task<ClientConnection> GetAsync(string endpoint, OperationScope scope, TimeSpan waitTimeout)
    {
        ArgumentNullException.ThrowIfNull(endpoint);
        Task<ClientConnection>? pending = null;
        bool mustStart = false;
        lock (_sync)
        {
            ObjectDisposedException.ThrowIf(_closed, typeof(DfsClient));
            Slot slot = SlotOf(endpoint);
            if (slot.Connection is { IsUsable: true } open)
                return open;
            if (slot.Connection is not null)
            {
                slot.Connection.Poison();
                slot.Connection = null;
            }

            if (slot.Pending is { IsCompleted: true })
                slot.Pending = null; // a finished connect either pooled its connection (now unusable) or failed: start afresh
            if (slot.Pending is null)
                mustStart = true;
            else
                pending = slot.Pending;
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
    /// A shared connect that outlived a caller's whole wait (and a tenth of the connect timeout) is presumably stuck on a
    /// lost handshake message (a black-holed or partitioned link that may have healed since): it is abandoned so the next
    /// caller starts a fresh connect instead of waiting for the stuck one until the connect timeout.
    /// </summary>
    private void AbandonIfStuck(string endpoint, Task<ClientConnection> pending, TimeSpan waited)
    {
        TimeSpan tenth = _runtime.Options.ConnectTimeout / 10;
        TimeSpan threshold = waited > tenth ? waited : tenth;
        CancellationTokenSource? abandon = null;
        lock (_sync)
        {
            if (_slots.TryGetValue(endpoint, out Slot? slot) && ReferenceEquals(slot.Pending, pending)
                && _runtime.Time.GetElapsedTime(slot.PendingStarted) >= threshold)
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
                slot.Connection = null;
        }

        connection.Poison();
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
                    slot.Connection = null;
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
    }
}
