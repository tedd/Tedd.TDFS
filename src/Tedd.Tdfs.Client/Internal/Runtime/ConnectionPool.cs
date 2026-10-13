using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Client.Internal.Runtime;

/// <summary>
/// Authenticated connections of one client, pooled per endpoint (ARCHITECTURE §9.1). Concurrent callers share one
/// connection per endpoint and one in-progress connect; dead pooled connections (closed by either side, or declared dead
/// after a timeout on a silently dead link) are detected and replaced. A connect still in progress after a caller's whole
/// wait is presumed stuck (for example its handshake was lost to a partition that has healed since): the next caller
/// starts a fresh one and takes whichever of the two completes first. Thread-safe.
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
        Task<ClientConnection>? previous = null;
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

            Prune(slot);
            if (slot.Pending is null || IsStale(slot, waitTimeout))
                mustStart = true;
            else
                (pending, previous) = (slot.Pending, slot.Previous);
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
                Prune(slot);
                if (slot.Pending is null || IsStale(slot, waitTimeout))
                {
                    slot.Previous = slot.Pending; // a superseded connect keeps running: whichever completes first is used
                    slot.Pending = ConnectAsync(endpoint, slot);
                    slot.PendingStarted = _runtime.Time.GetTimestamp();
                }

                (pending, previous) = (slot.Pending, slot.Previous);
            }
        }

        if (previous is { IsCompleted: false })
            pending = FirstSuccessfulAsync(pending!, previous);
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
            throw new TimeoutException($"No authenticated connection to {endpoint} within {waitTimeout}.");
        }
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

    /// <summary>Drops finished connects (each either pooled its connection, now unusable, or failed): the next caller starts afresh.</summary>
    private static void Prune(Slot slot)
    {
        if (slot.Previous is { IsCompleted: true })
            slot.Previous = null;
        if (slot.Pending is { IsCompleted: true })
        {
            slot.Pending = slot.Previous;
            slot.Previous = null;
        }
    }

    /// <summary>
    /// True when the slot's connect has run for a caller's whole wait without completing and no fresh one supersedes it yet
    /// (at most two connects per endpoint run at a time).
    /// </summary>
    private bool IsStale(Slot slot, TimeSpan waitTimeout)
        => slot.Pending is { IsCompleted: false } && slot.Previous is null && waitTimeout > TimeSpan.Zero
            && _runtime.Time.GetElapsedTime(slot.PendingStarted) >= waitTimeout;

    /// <summary>
    /// The first of two connects to succeed, or the failure of the last one to fail. Completed without a synchronization
    /// context (<see cref="ContextFree"/>), so awaiting it never escapes the simulation.
    /// </summary>
    private static Task<ClientConnection> FirstSuccessfulAsync(Task<ClientConnection> a, Task<ClientConnection> b)
    {
        var source = new TaskCompletionSource<ClientConnection>(TaskCreationOptions.None);
        int failed = 0;
        void OnCompleted(Task<ClientConnection> completed)
        {
            if (completed.IsCompletedSuccessfully)
            {
                ContextFree.Run(static q => q.Source.TrySetResult(q.Connection), (Source: source, Connection: completed.Result));
                return;
            }

            _ = completed.Exception; // observed
            if (Interlocked.Increment(ref failed) < 2)
                return;
            ContextFree.Run(static q =>
            {
                if (q.Last.IsCanceled)
                    q.Source.TrySetCanceled();
                else
                    q.Source.TrySetException(q.Last.Exception!.InnerExceptions);
            }, (Source: source, Last: completed));
        }

        a.ContinueWith(OnCompleted, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
        b.ContinueWith(OnCompleted, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
        return source.Task;
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

    private async Task<ClientConnection> ConnectAsync(string endpoint, Slot slot)
    {
        TimeSpan timeout = _runtime.Options.ConnectTimeout;
        ClientConnection opened;
        using (var deadline = new CancellationTokenSource(timeout, _runtime.Time))
        using (_runtime.KillToken.UnsafeRegister(static s => ContextFree.Cancel((CancellationTokenSource)s!), deadline))
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
                throw new TimeoutException($"Connecting to {endpoint} did not complete within {timeout}.");
            }
        }

        lock (_sync)
        {
            if (_closed)
            {
                opened.Poison();
                throw new ObjectDisposedException(nameof(DfsClient), "The client was disposed while connecting.");
            }

            if (slot.Connection is { IsUsable: true } pooled && !ReferenceEquals(pooled, opened))
            {
                // The other connect of this endpoint completed first: one connection per endpoint.
                opened.Poison();
                return pooled;
            }

            slot.Connection = opened;
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
        public long PendingStarted;
        public Task<ClientConnection>? Previous;
    }
}
