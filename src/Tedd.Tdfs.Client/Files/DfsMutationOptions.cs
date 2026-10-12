namespace Tedd.Tdfs;

/// <summary>Options for mutating helpers and stream commits.</summary>
public sealed record DfsMutationOptions
{
    /// <summary>Operation id to use (for example a persisted id being retried); null allocates one.</summary>
    public DfsOperationId? OperationId { get; init; }

    /// <summary>Named storage policy for a newly created file; null uses the volume default.</summary>
    public string? StoragePolicy { get; init; }

    /// <summary>When set, a snapshot with this name is created in the same metadata decision as the commit.</summary>
    public string? CommitSnapshotName { get; init; }
}
