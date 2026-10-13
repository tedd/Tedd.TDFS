namespace Tedd.Tdfs.Server.Volumes.Service;

/// <summary>
/// FIFO mutual exclusion per key for node-loop code (the volume service serializes the planning and proposal of
/// publications per stream with it). Waiters are completed on the releasing caller's turn of the node loop; a waiter whose
/// token is cancelled leaves the queue without ever owning the key, and a release never hands the key to such a waiter.
/// Not thread-safe: every call happens on the node loop.
/// </summary>
/// <typeparam name="TKey">Key type.</typeparam>
/// <param name="comparer">Key comparer (null = the default equality comparer).</param>
internal sealed class KeyedAsyncLock<TKey>(IEqualityComparer<TKey>? comparer = null)
    where TKey : notnull
{
    private readonly Dictionary<TKey, Queue<TaskCompletionSource>> _held = new(comparer);

    /// <summary>Keys currently held (for tests and diagnostics).</summary>
    public int HeldCount => _held.Count;

    /// <summary>True when <paramref name="key"/> is held.</summary>
    public bool IsHeld(TKey key) => _held.ContainsKey(key);

    /// <summary>Acquires <paramref name="key"/>; dispose the result to release it (idempotent).</summary>
    /// <exception cref="OperationCanceledException"><paramref name="ct"/> was cancelled before the key was handed over.</exception>
    public async Task<IDisposable> AcquireAsync(TKey key, CancellationToken ct)
    {
        if (!_held.TryGetValue(key, out Queue<TaskCompletionSource>? queue))
        {
            _held[key] = new Queue<TaskCompletionSource>();
            return new Releaser(this, key);
        }

        ct.ThrowIfCancellationRequested();
        var waiter = new TaskCompletionSource();
        queue.Enqueue(waiter);
        try
        {
            await waiter.Task.WaitAsync(ct);
        }
        catch (OperationCanceledException)
        {
            // Withdraw: a waiter cancelled before the hand-over can never be selected by a later release. When the hand-over
            // already happened (the release completed this waiter first), this caller owns the key and must pass it on.
            if (!waiter.TrySetCanceled(ct))
                Release(key);
            throw;
        }

        return new Releaser(this, key);
    }

    private void Release(TKey key)
    {
        if (!_held.TryGetValue(key, out Queue<TaskCompletionSource>? queue))
            return;
        while (queue.Count > 0)
        {
            // Hand the key to the first waiter that has not withdrawn.
            if (queue.Dequeue().TrySetResult())
                return;
        }

        _held.Remove(key);
    }

    private sealed class Releaser(KeyedAsyncLock<TKey> owner, TKey key) : IDisposable
    {
        private bool _released;

        public void Dispose()
        {
            if (_released)
                return;
            _released = true;
            owner.Release(key);
        }
    }
}
