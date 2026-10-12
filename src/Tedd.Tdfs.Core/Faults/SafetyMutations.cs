namespace Tedd.Tdfs;

/// <summary>
/// Negative-control switches that disable individual safety checks (MUT-* tests only). Every flag is false in
/// production; a test enabling one must observe the corresponding checker failing.
/// </summary>
public sealed record SafetyMutations
{
    /// <summary>Skip the writer-generation fencing check on publication.</summary>
    public bool SkipFencingCheck { get; init; }
    /// <summary>Issue object receipts before the flush barrier completes.</summary>
    public bool AcknowledgeBeforeFlush { get; init; }
    /// <summary>Commit metadata before data receipts are validated.</summary>
    public bool CommitMetadataBeforeData { get; init; }
    /// <summary>Compute quorum from reachable nodes instead of configured voters.</summary>
    public bool QuorumFromReachableNodes { get; init; }
    /// <summary>Disable operation-id deduplication.</summary>
    public bool DisableOperationDedup { get; init; }
    /// <summary>SDK-side: report unknown outcomes as aborts.</summary>
    public bool TreatUnknownAsAborted { get; init; }
    /// <summary>Skip transaction predicate (phantom) validation.</summary>
    public bool SkipPredicateValidation { get; init; }
    /// <summary>Serve live reads without a ReadIndex round.</summary>
    public bool AllowStaleLiveReads { get; init; }
    /// <summary>Ignore snapshot pins during GC.</summary>
    public bool IgnoreSnapshotPins { get; init; }
    /// <summary>Publish a backup before verifying its dependency closure.</summary>
    public bool PublishBackupBeforeClosure { get; init; }
    /// <summary>Skip checksum verification on reads.</summary>
    public bool SkipChecksumVerification { get; init; }
    /// <summary>Count two copies in the same zone as two zones.</summary>
    public bool CountSameZoneTwice { get; init; }
    /// <summary>Forget the retired operation-id watermark.</summary>
    public bool ForgetExpiredIdWatermark { get; init; }
    /// <summary>Relax the storage policy when acknowledgements time out.</summary>
    public bool RelaxPolicyOnTimeout { get; init; }

    /// <summary>All checks enabled (production).</summary>
    public static SafetyMutations None { get; } = new();

    /// <summary>True when at least one safety check is disabled.</summary>
    public bool Any =>
        SkipFencingCheck || AcknowledgeBeforeFlush || CommitMetadataBeforeData || QuorumFromReachableNodes ||
        DisableOperationDedup || TreatUnknownAsAborted || SkipPredicateValidation || AllowStaleLiveReads ||
        IgnoreSnapshotPins || PublishBackupBeforeClosure || SkipChecksumVerification || CountSameZoneTwice ||
        ForgetExpiredIdWatermark || RelaxPolicyOnTimeout;
}
