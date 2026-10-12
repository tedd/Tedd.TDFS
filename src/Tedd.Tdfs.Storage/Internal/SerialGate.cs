namespace Tedd.Tdfs.Storage;

/// <summary>
/// FIFO asynchronous mutual exclusion for storage operations that must not interleave (appends, flushes, truncation).
/// Never blocks a thread and never schedules work itself: waiters resume through the awaiting context (the node loop in
/// server code). Uncontended entry completes synchronously.
/// </summary>
internal sealed class SerialGate
{
    private Task _tail = Task.CompletedTask;

    /// <summary>Waits for every earlier holder to release, then holds the gate until the returned releaser is disposed.</summary>
    /// <remarks>Cancellation is observed after entry (the gate is released again before throwing), never while queued.</remarks>
    public async ValueTask<Releaser> EnterAsync(CancellationToken ct = default)
    {
        var mine = new TaskCompletionSource();
        Task previous = Interlocked.Exchange(ref _tail, mine.Task);
        if (!previous.IsCompleted)
            await previous;

        var releaser = new Releaser(mine);
        if (ct.IsCancellationRequested)
        {
            releaser.Dispose();
            ct.ThrowIfCancellationRequested();
        }

        return releaser;
    }

    /// <summary>Releases the gate when disposed (idempotent).</summary>
    internal readonly struct Releaser : IDisposable
    {
        private readonly TaskCompletionSource? _source;

        internal Releaser(TaskCompletionSource source) => _source = source;

        /// <inheritdoc />
        public void Dispose() => _source?.TrySetResult();
    }
}
