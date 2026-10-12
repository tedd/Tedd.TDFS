namespace Tedd.Tdfs;

/// <summary>Options for <see cref="DfsSnapshotClient.CreateAsync"/>.</summary>
public sealed record DfsSnapshotOptions
{
    /// <summary>The snapshot must include at least this commit (same cluster, volume and epoch).</summary>
    public CommitToken? MinimumCommit { get; init; }

    /// <summary>Application adapters prepared before capture and resumed afterwards.</summary>
    public IReadOnlyList<IDfsSnapshotAdapter> Adapters { get; init; } = [];

    /// <summary>Deadline for each adapter phase.</summary>
    public TimeSpan AdapterTimeout { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>Initial retention; null uses no retention protection.</summary>
    public DfsRetention? Retention { get; init; }

    /// <summary>Operation id for the snapshot creation; null allocates one.</summary>
    public DfsOperationId? OperationId { get; init; }
}
