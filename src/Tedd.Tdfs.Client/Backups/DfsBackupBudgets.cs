namespace Tedd.Tdfs;

/// <summary>Source-side resource budgets for a backup target (exceeding them abandons the transfer lineage).</summary>
/// <param name="MaxPinAge">Maximum age of the source transfer pin.</param>
/// <param name="MaxPinnedBytes">Maximum source bytes kept pinned for the target.</param>
/// <param name="MaxBytesPerSecond">Transfer rate limit.</param>
/// <param name="MaxConcurrentObjects">Maximum objects in flight.</param>
public sealed record DfsBackupBudgets(TimeSpan MaxPinAge, long MaxPinnedBytes, long MaxBytesPerSecond = long.MaxValue, int MaxConcurrentObjects = 8);
