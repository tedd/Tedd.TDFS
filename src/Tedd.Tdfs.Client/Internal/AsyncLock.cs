namespace Tedd.Tdfs.Client.Internal;

/// <summary>
/// Simulation-compatible asynchronous mutual exclusion (a replacement for <c>SemaphoreSlim(1, 1)</c>, which the SDK
/// must not use — ARCHITECTURE §9.6).
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item>Waiters are served strictly FIFO. Release hands ownership directly to the next waiter (no barging), so the
/// order of grants depends only on the order of requests.</item>
/// <item>A waiter is completed <b>synchronously on the releasing thread</b>, outside the internal lock: its
/// continuation runs inline unless it captured a synchronization context (in which case it is posted there, e.g. to
/// the simulation scheduler). No thread pool, timer or <c>RunContinuationsAsynchronously</c> is involved.</item>
/// <item>Cancellation removes a waiter that has not been granted yet and completes it as cancelled. Once granted,
/// ownership is not revoked by a later cancellation; the caller must release it.</item>
/// <item>Every grant has a unique id carried by its <see cref="Releaser"/>, so disposing a releaser twice is
/// harmless and can never release a later owner.</item>
/// </list>
/// </remarks>
internal sealed class AsyncLock
{
    private readonly Lock _sync = new();
    private readonly LinkedList<Waiter> _waiters = new();

    // 0 when free; otherwise the grant id of the current owner. Guarded by _sync.
    private ulong _holder;
    private ulong _lastGrant;

    /// <summary>True while the lock is owned.</summary>
    public bool IsHeld
    {
        get
        {
            lock (_sync)
                return _holder != 0;
        }
    }

    /// <summary>Number of queued (not yet granted, not cancelled) waiters.</summary>
    public int WaiterCount
    {
        get
        {
            lock (_sync)
                return _waiters.Count;
        }
    }

    /// <summary>Acquires the lock if it is free, without waiting.</summary>
    /// <param name="releaser">The releaser to dispose when done; default when the lock was not acquired.</param>
    /// <returns>True when acquired.</returns>
    public bool TryAcquire(out Releaser releaser)
    {
        lock (_sync)
        {
            if (_holder == 0)
            {
                releaser = new Releaser(this, GrantLocked());
                return true;
            }
        }

        releaser = default;
        return false;
    }

    /// <summary>
    /// Acquires the lock. Completes synchronously when the lock is free; otherwise queues behind earlier waiters.
    /// Dispose the returned <see cref="Releaser"/> to release it.
    /// </summary>
    /// <param name="ct">Cancels waiting (not an already granted ownership).</param>
    /// <exception cref="OperationCanceledException">Cancelled before the lock was granted.</exception>
    public ValueTask<Releaser> AcquireAsync(CancellationToken ct = default)
    {
        if (ct.IsCancellationRequested)
            return ValueTask.FromCanceled<Releaser>(ct);

        Waiter waiter;
        lock (_sync)
        {
            if (_holder == 0)
                return new ValueTask<Releaser>(new Releaser(this, GrantLocked()));

            waiter = new Waiter(this);
            waiter.Node = _waiters.AddLast(waiter);
        }

        if (ct.CanBeCanceled)
            RegisterCancellation(waiter, ct);

        return new ValueTask<Releaser>(waiter.Completion.Task);
    }

    private ulong GrantLocked()
    {
        _holder = ++_lastGrant;
        return _holder;
    }

    private void RegisterCancellation(Waiter waiter, CancellationToken ct)
    {
        // Registered outside _sync: if the token is already cancelled the callback runs synchronously here and takes
        // _sync itself.
        CancellationTokenRegistration registration = ct.UnsafeRegister(
            static (state, token) =>
            {
                var w = (Waiter)state!;
                w.Owner.CancelWaiter(w, token);
            },
            waiter);

        lock (_sync)
        {
            if (waiter.Node is not null)
            {
                // Still queued: whoever dequeues it (release or cancellation) disposes of the registration.
                waiter.Registration = registration;
                return;
            }
        }

        // Granted or cancelled while registering; the registration is no longer needed.
        registration.Unregister();
    }

    private void CancelWaiter(Waiter waiter, CancellationToken ct)
    {
        lock (_sync)
        {
            if (waiter.Node is null)
                return; // already granted (ownership stands) or already cancelled

            _waiters.Remove(waiter.Node);
            waiter.Node = null;
            waiter.Registration = default; // we are running inside this registration's callback
        }

        waiter.Completion.TrySetCanceled(ct);
    }

    private void Release(ulong grant)
    {
        Waiter next;
        ulong nextGrant;
        CancellationTokenRegistration registration;
        lock (_sync)
        {
            if (grant == 0 || grant != _holder)
                return; // stale releaser (double dispose): no effect

            LinkedListNode<Waiter>? first = _waiters.First;
            if (first is null)
            {
                _holder = 0;
                return;
            }

            _waiters.Remove(first);
            next = first.Value;
            next.Node = null;
            registration = next.Registration;
            next.Registration = default;
            nextGrant = GrantLocked(); // ownership transfers now; nobody can barge in before the waiter resumes
        }

        // Outside _sync: Unregister never blocks (a concurrently running cancellation callback finds Node == null and
        // does nothing), and the waiter's continuation may run inline and re-enter this lock.
        registration.Unregister();
        next.Completion.TrySetResult(new Releaser(this, nextGrant));
    }

    /// <summary>Ownership of an <see cref="AsyncLock"/>. Dispose to release (idempotent).</summary>
    public readonly struct Releaser : IDisposable
    {
        private readonly AsyncLock? _owner;
        private readonly ulong _grant;

        internal Releaser(AsyncLock owner, ulong grant)
        {
            _owner = owner;
            _grant = grant;
        }

        /// <summary>Releases the lock if this grant still owns it; otherwise does nothing.</summary>
        public void Dispose() => _owner?.Release(_grant);
    }

    private sealed class Waiter(AsyncLock owner)
    {
        public AsyncLock Owner { get; } = owner;

        // Deliberately without RunContinuationsAsynchronously: waiters are completed synchronously on release.
        public TaskCompletionSource<Releaser> Completion { get; } = new(TaskCreationOptions.None);

        // Guarded by AsyncLock._sync. Non-null exactly while the waiter is queued.
        public LinkedListNode<Waiter>? Node;

        // Guarded by AsyncLock._sync.
        public CancellationTokenRegistration Registration;
    }
}
