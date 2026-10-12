namespace Tedd.Tdfs.Client.Internal.Sessions;

/// <summary>
/// The client's session on one volume (ARCHITECTURE §7, PROTOCOL.md §5–6): the server-issued session id, the operation
/// sequences the leader reserved for this client, and the acknowledgement watermark.
/// </summary>
/// <remarks>
/// <para>
/// Sequence allocation: every sequence comes from a block the leader handed out with <c>ReserveOperationIds</c> (blocks are
/// strictly increasing and never overlap within a session). Internal operations draw from a block reserved in advance;
/// application-visible ids (<see cref="DfsVolume.NewOperationIdAsync"/>) are reserved one at a time.
/// </para>
/// <para>
/// Watermark (<c>RequestHeader.AckWatermark</c>): the lowest sequence that is still in flight, whose outcome is unknown, or
/// that was handed to the application (and may be retried or queried later, possibly by another process); when nothing
/// is protected it is the next unallocated sequence. Outcomes below it may be retired by the leader.
/// </para>
/// </remarks>
internal sealed class VolumeSession
{
    private readonly Lock _sync = new();
    private readonly SortedSet<ulong> _protected = [];
    private readonly HashSet<ulong> _applicationVisible = [];
    private readonly SortedSet<ulong> _free = [];
    private ulong _next;
    private ulong _end;
    private ulong _highWater;

    public VolumeSession(ulong id, ulong firstSequence, TimeSpan leaseDuration, TimeSpan sessionTimeout, VolumeId volume, ulong epoch)
    {
        Id = id;
        LeaseDuration = leaseDuration;
        SessionTimeout = sessionTimeout;
        Volume = volume;
        Epoch = epoch;
        _next = firstSequence;
        _end = firstSequence;
        _highWater = firstSequence;
    }

    /// <summary>Server-issued session number.</summary>
    public ulong Id { get; }

    /// <summary>Handle lease duration (renew every third).</summary>
    public TimeSpan LeaseDuration { get; }

    /// <summary>Session timeout without keep-alive.</summary>
    public TimeSpan SessionTimeout { get; }

    /// <summary>Volume identity the session belongs to.</summary>
    public VolumeId Volume { get; }

    /// <summary>Volume epoch the session belongs to.</summary>
    public ulong Epoch { get; }

    /// <summary>True once the leader reported the session expired (a new one is opened on demand).</summary>
    public bool IsExpired { get; private set; }

    /// <summary>The current acknowledgement watermark.</summary>
    public ulong Watermark
    {
        get
        {
            lock (_sync)
            {
                // Sequences of the current block that are not allocated yet will still be used: never past them.
                ulong next = _next < _end ? _next : _highWater;
                return _protected.Count > 0 ? Math.Min(_protected.Min, next) : next;
            }
        }
    }

    /// <summary>Takes the next sequence of the reserved block, protecting it until <see cref="Complete"/>; false when the block is used up.</summary>
    public bool TryAllocate(out DfsOperationId id)
    {
        lock (_sync)
        {
            if (_free.Count > 0)
            {
                ulong reused = _free.Min;
                _free.Remove(reused);
                id = new DfsOperationId(Id, reused);
                return true;
            }

            if (_next < _end)
            {
                ulong sequence = _next++;
                _protected.Add(sequence);
                _highWater = Math.Max(_highWater, _next);
                id = new DfsOperationId(Id, sequence);
                return true;
            }
        }

        id = default;
        return false;
    }

    /// <summary>Installs a block reserved by the leader for internal allocation.</summary>
    public void AddBlock(ulong first, int count)
    {
        lock (_sync)
        {
            if (first >= _end)
            {
                _next = first;
                _end = first + (ulong)count;
            }
        }
    }

    /// <summary>
    /// Registers a sequence the leader reserved for the application: protected for the session's lifetime (never acknowledged).
    /// </summary>
    public void AddApplicationVisible(ulong sequence)
    {
        lock (_sync)
        {
            _applicationVisible.Add(sequence);
            _protected.Add(sequence);
            _highWater = Math.Max(_highWater, sequence + 1);
        }
    }

    /// <summary>
    /// Marks an application-supplied id of this session as visible: it is never acknowledged (it may be retried or queried
    /// by another process later).
    /// </summary>
    public void Protect(DfsOperationId id)
    {
        if (id.Session != Id)
            return;
        lock (_sync)
        {
            _applicationVisible.Add(id.Sequence);
            _protected.Add(id.Sequence);
        }
    }

    /// <summary>The outcome of <paramref name="id"/> is known to the client: it may be acknowledged (unless application-visible).</summary>
    public void Complete(DfsOperationId id)
    {
        if (id.Session != Id)
            return;
        lock (_sync)
        {
            if (!_applicationVisible.Contains(id.Sequence))
                _protected.Remove(id.Sequence);
        }
    }

    /// <summary>
    /// Returns an SDK-allocated id that was never transmitted: it is reused by the next allocation (so that reserved
    /// blocks are used up and retired by the leader).
    /// </summary>
    public void Release(DfsOperationId id)
    {
        if (id.Session != Id)
            return;
        lock (_sync)
        {
            // Stays protected (the watermark must not pass an id that will still be used).
            if (!_applicationVisible.Contains(id.Sequence) && _protected.Contains(id.Sequence))
                _free.Add(id.Sequence);
        }
    }

    /// <summary>The leader reported the session expired.</summary>
    public void MarkExpired() => IsExpired = true;
}
