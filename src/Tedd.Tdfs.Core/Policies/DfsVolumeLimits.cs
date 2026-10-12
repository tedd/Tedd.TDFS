namespace Tedd.Tdfs;

/// <summary>Per-volume resource limits enforced by the server (and pre-checked by the SDK).</summary>
public sealed record DfsVolumeLimits
{
    /// <summary>Maximum file length in bytes.</summary>
    public long MaxFileLength { get; init; } = long.MaxValue / 2;

    /// <summary>Maximum bytes published by one commit.</summary>
    public long MaxPublishBytes { get; init; } = 64L << 30;

    /// <summary>Maximum transaction lifetime.</summary>
    public TimeSpan MaxTransactionDuration { get; init; } = TimeSpan.FromMinutes(5);

    /// <summary>Maximum inodes touched by one transaction.</summary>
    public int MaxTransactionTouchedInodes { get; init; } = 10_000;

    /// <summary>Maximum encoded transaction descriptor size.</summary>
    public int MaxTransactionDescriptorBytes { get; init; } = 4 << 20;

    /// <summary>Maximum staged bytes per transaction.</summary>
    public long MaxTransactionStagedBytes { get; init; } = 16L << 30;

    /// <summary>Maximum read pins per session.</summary>
    public int MaxPinsPerSession { get; init; } = 4096;

    /// <summary>Maximum open handles per session.</summary>
    public int MaxHandlesPerSession { get; init; } = 4096;
}
