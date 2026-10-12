namespace Tedd.Tdfs.Client.Internal;

/// <summary>
/// Detects overlapping operations on an object that does not support concurrent use (for example
/// <see cref="DfsStream"/>, STR-019). Entering never waits: a caller that overlaps an operation in progress gets an
/// <see cref="InvalidOperationException"/> immediately, and a caller that arrives after <see cref="Close"/> gets an
/// <see cref="ObjectDisposedException"/> (which is also an <see cref="InvalidOperationException"/>).
/// </summary>
/// <remarks>
/// Lock-free and allocation-free (simulation-compatible: no waiting, no scheduling). Each successful
/// <see cref="Enter"/> is identified by a unique entry id carried by the returned <see cref="Scope"/>, so disposing a
/// scope twice, or disposing it after the gate was closed, can never release a later operation's entry.
/// </remarks>
internal sealed class AsyncGate
{
    private const long FreeState = 0;
    private const long ClosedState = -1;

    // FreeState, ClosedState, or the positive id of the entry currently holding the gate.
    private long _state;
    private long _lastEntryId;

    // Name of the most recent successful entry; only meaningful while the gate is busy (diagnostics only).
    private string? _operation;

    /// <summary>Creates an open, idle gate.</summary>
    /// <param name="objectName">Name of the guarded object, used in exception messages (e.g. <c>DfsStream</c>).</param>
    public AsyncGate(string objectName)
    {
        ArgumentException.ThrowIfNullOrEmpty(objectName);
        ObjectName = objectName;
    }

    /// <summary>Name of the guarded object.</summary>
    public string ObjectName { get; }

    /// <summary>True while an operation holds the gate.</summary>
    public bool IsBusy => Volatile.Read(ref _state) > 0;

    /// <summary>True once the gate has been closed; it never reopens.</summary>
    public bool IsClosed => Volatile.Read(ref _state) == ClosedState;

    /// <summary>Name of the operation holding the gate, or null when idle or closed (best effort, diagnostics only).</summary>
    public string? CurrentOperation => IsBusy ? Volatile.Read(ref _operation) : null;

    /// <summary>Tries to enter without throwing. Returns false when another operation holds the gate or it is closed.</summary>
    /// <remarks>Pair a successful call with <see cref="Exit()"/>.</remarks>
    public bool TryEnter(string operation) => TryEnterCore(operation, out _) == FreeState;

    /// <summary>Enters the gate for <paramref name="operation"/>; dispose the returned scope to exit.</summary>
    /// <exception cref="InvalidOperationException">Another operation is in progress (overlap).</exception>
    /// <exception cref="ObjectDisposedException">The gate is closed.</exception>
    public Scope Enter(string operation)
    {
        long prior = TryEnterCore(operation, out long entryId);
        if (prior == FreeState)
            return new Scope(this, entryId);
        if (prior == ClosedState)
            throw new ObjectDisposedException(ObjectName);
        throw CreateOverlapException(operation);
    }

    /// <summary>Throws <see cref="ObjectDisposedException"/> when the gate is closed.</summary>
    public void ThrowIfClosed()
    {
        if (IsClosed)
            throw new ObjectDisposedException(ObjectName);
    }

    /// <summary>
    /// Exits after a successful <see cref="TryEnter"/>. Exiting a gate that was closed meanwhile is a no-op (it stays
    /// closed).
    /// </summary>
    /// <exception cref="InvalidOperationException">The gate is not held (exit without a matching enter).</exception>
    public void Exit()
    {
        while (true)
        {
            long current = Volatile.Read(ref _state);
            if (current == ClosedState)
                return;
            if (current == FreeState)
                throw new InvalidOperationException($"{ObjectName}: exit was called without a matching enter.");
            if (Interlocked.CompareExchange(ref _state, FreeState, current) == current)
                return;
        }
    }

    /// <summary>Closes the gate when it is idle. Returns false when an operation holds it or it is already closed.</summary>
    public bool TryClose() => Interlocked.CompareExchange(ref _state, ClosedState, FreeState) == FreeState;

    /// <summary>
    /// Closes the gate unconditionally, even while an operation holds it (that operation's exit then leaves it closed).
    /// Returns true when this call closed it, false when it was already closed.
    /// </summary>
    public bool Close() => Interlocked.Exchange(ref _state, ClosedState) != ClosedState;

    private long TryEnterCore(string operation, out long entryId)
    {
        entryId = Interlocked.Increment(ref _lastEntryId);
        long prior = Interlocked.CompareExchange(ref _state, entryId, FreeState);
        if (prior == FreeState)
            Volatile.Write(ref _operation, operation);
        return prior;
    }

    private InvalidOperationException CreateOverlapException(string operation)
    {
        string current = CurrentOperation ?? "another operation";
        return new InvalidOperationException(
            $"{operation} cannot start while {current} is still in progress on this {ObjectName}. " +
            $"{ObjectName} does not support overlapping operations; complete each operation before starting the next.");
    }

    // Releases exactly the given entry; a no-op when that entry no longer holds the gate (idempotent).
    private bool ExitEntry(long entryId, long newState) =>
        Interlocked.CompareExchange(ref _state, newState, entryId) == entryId;

    /// <summary>One successful entry into an <see cref="AsyncGate"/>. Disposing it exits the gate (idempotent).</summary>
    public readonly struct Scope : IDisposable
    {
        private readonly AsyncGate? _gate;
        private readonly long _entryId;

        internal Scope(AsyncGate gate, long entryId)
        {
            _gate = gate;
            _entryId = entryId;
        }

        /// <summary>True while this entry still holds the gate.</summary>
        public bool IsHeld => _gate is not null && Volatile.Read(ref _gate._state) == _entryId;

        /// <summary>Exits the gate and closes it in one step (e.g. at the end of a disposing operation).</summary>
        /// <returns>True when this entry held the gate and closed it.</returns>
        public bool ExitAndClose() => _gate is not null && _gate.ExitEntry(_entryId, ClosedState);

        /// <summary>Exits the gate if this entry still holds it; otherwise does nothing.</summary>
        public void Dispose() => _gate?.ExitEntry(_entryId, FreeState);
    }
}
