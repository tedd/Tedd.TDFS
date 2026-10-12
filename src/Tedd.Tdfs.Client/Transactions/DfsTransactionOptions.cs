namespace Tedd.Tdfs;

/// <summary>Options for <see cref="DfsTransactionClient.BeginAsync"/>.</summary>
public sealed record DfsTransactionOptions
{
    /// <summary>Maximum transaction duration; null uses the volume limit.</summary>
    public TimeSpan? Timeout { get; init; }

    /// <summary>Operation id for the transaction begin (becomes the transaction id); null allocates one.</summary>
    public DfsOperationId? OperationId { get; init; }
}
