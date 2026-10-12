namespace Tedd.Tdfs.Client.Internal.Data;

/// <summary>
/// A staging session the client uploads objects under (PROTOCOL.md §7.1): its id, the staging capability presented to
/// storage nodes, the capability expiry and the placement epoch uploads are placed under. Thread-safe; the capability
/// is replaced on renewal.
/// </summary>
internal sealed class StagingLease
{
    private readonly Lock _sync = new();
    private ReadOnlyMemory<byte> _capability;
    private DateTimeOffset _expiry;
    private ulong _placementEpoch;

    public StagingLease(StagingId id, HandleId handle, DfsTransactionId? transaction, ulong sessionId, ReadOnlyMemory<byte> capability,
        DateTimeOffset expiry, ulong placementEpoch)
    {
        Id = id;
        Handle = handle;
        Transaction = transaction;
        SessionId = sessionId;
        _capability = capability;
        _expiry = expiry;
        _placementEpoch = placementEpoch;
    }

    /// <summary>Remaining validity below which the capability is renewed before an upload.</summary>
    public static readonly TimeSpan RenewalMargin = TimeSpan.FromSeconds(5);

    /// <summary>Staging session id.</summary>
    public StagingId Id { get; }

    /// <summary>Handle the session is bound to (empty when unbound).</summary>
    public HandleId Handle { get; }

    /// <summary>Transaction the session belongs to, if any.</summary>
    public DfsTransactionId? Transaction { get; }

    /// <summary>Client session that created the staging session.</summary>
    public ulong SessionId { get; }

    /// <summary>The current staging capability.</summary>
    public ReadOnlyMemory<byte> Capability
    {
        get
        {
            lock (_sync)
                return _capability;
        }
    }

    /// <summary>Placement epoch for uploads.</summary>
    public ulong PlacementEpoch
    {
        get
        {
            lock (_sync)
                return _placementEpoch;
        }
    }

    /// <summary>Capability expiry (server clock).</summary>
    public DateTimeOffset Expiry
    {
        get
        {
            lock (_sync)
                return _expiry;
        }
    }

    /// <summary>
    /// True when the capability should be renewed before use: less than <see cref="RenewalMargin"/> of its validity is
    /// left on the client's clock. Client clocks are never authoritative; a node rejecting an expired capability
    /// (<c>AccessDenied</c>) also triggers a renewal.
    /// </summary>
    public bool NeedsRenewal(DateTimeOffset now)
    {
        lock (_sync)
        {
            if (_capability.IsEmpty)
                return true;
            return _expiry - now < RenewalMargin;
        }
    }

    /// <summary>Installs a renewed capability.</summary>
    public void Update(ReadOnlyMemory<byte> capability, DateTimeOffset expiry, ulong placementEpoch)
    {
        lock (_sync)
        {
            _capability = capability;
            _expiry = expiry;
            if (placementEpoch > _placementEpoch)
                _placementEpoch = placementEpoch;
        }
    }
}
