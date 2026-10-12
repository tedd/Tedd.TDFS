namespace Tedd.Tdfs;

/// <summary>
/// Data durability policy. Copies are counted by distinct host and zones by distinct (region, zone).
/// <see cref="DurableCopiesBeforeAck"/>/<see cref="DurableZonesBeforeAck"/> must be durable before a commit is
/// acknowledged; the remaining copies up to the target are completed by repair.
/// </summary>
public sealed record DfsStoragePolicy
{
    /// <summary>Target number of copies (distinct hosts).</summary>
    public int TargetCopies { get; init; } = 3;

    /// <summary>Target number of distinct zones.</summary>
    public int TargetDistinctZones { get; init; } = 3;

    /// <summary>Durable copies (distinct hosts) required before acknowledging a commit.</summary>
    public int DurableCopiesBeforeAck { get; init; } = 2;

    /// <summary>Distinct durable zones required before acknowledging a commit.</summary>
    public int DurableZonesBeforeAck { get; init; } = 2;

    /// <summary>The default policy (3 copies in 3 zones, acknowledge after 2 copies in 2 zones).</summary>
    public static DfsStoragePolicy Default { get; } = new();

    /// <summary>Validates internal consistency.</summary>
    /// <exception cref="DfsException">With <see cref="DfsErrorCode.InvalidPolicy"/> when any count is below 1, the
    /// acknowledgement count exceeds the target, the zone target exceeds the copy target, the acknowledgement zone count
    /// exceeds the acknowledgement copy count, or the acknowledgement zone count exceeds the zone target.</exception>
    public void Validate()
    {
        string? error = GetValidationError();
        if (error is not null)
            throw new DfsException(DfsErrorCode.InvalidPolicy, error);
    }

    /// <summary>Returns the first violated rule, or null when the policy is valid.</summary>
    public string? GetValidationError()
    {
        if (TargetCopies < 1)
            return $"TargetCopies must be at least 1 (was {TargetCopies}).";
        if (TargetDistinctZones < 1)
            return $"TargetDistinctZones must be at least 1 (was {TargetDistinctZones}).";
        if (DurableCopiesBeforeAck < 1)
            return $"DurableCopiesBeforeAck must be at least 1 (was {DurableCopiesBeforeAck}).";
        if (DurableZonesBeforeAck < 1)
            return $"DurableZonesBeforeAck must be at least 1 (was {DurableZonesBeforeAck}).";
        if (DurableCopiesBeforeAck > TargetCopies)
            return $"DurableCopiesBeforeAck ({DurableCopiesBeforeAck}) must not exceed TargetCopies ({TargetCopies}).";
        if (TargetDistinctZones > TargetCopies)
            return $"TargetDistinctZones ({TargetDistinctZones}) must not exceed TargetCopies ({TargetCopies}).";
        if (DurableZonesBeforeAck > DurableCopiesBeforeAck)
            return $"DurableZonesBeforeAck ({DurableZonesBeforeAck}) must not exceed DurableCopiesBeforeAck ({DurableCopiesBeforeAck}).";
        if (DurableZonesBeforeAck > TargetDistinctZones)
            return $"DurableZonesBeforeAck ({DurableZonesBeforeAck}) must not exceed TargetDistinctZones ({TargetDistinctZones}).";
        return null;
    }
}
