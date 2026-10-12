namespace Tedd.Tdfs.Testing;

/// <summary>
/// An active backend short-read rule created by <see cref="TestFaults.LimitReadResultBytes"/> (STR-004, STR-006).
/// </summary>
public sealed class ReadResultLimitRule
{
    private int _applied;
    private int _cancelled;

    internal ReadResultLimitRule(int maxBytesPerResult, string? node, string? volume)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxBytesPerResult, 1);
        MaxBytesPerResult = maxBytesPerResult;
        Node = node;
        Volume = volume;
    }

    /// <summary>Largest payload a matching read result may carry.</summary>
    public int MaxBytesPerResult { get; }

    /// <summary>Node restriction (null = every live node).</summary>
    public string? Node { get; }

    /// <summary>Volume restriction (null = every volume).</summary>
    public string? Volume { get; }

    /// <summary>Number of read results that were actually shortened by this rule so far (thread-safe snapshot).</summary>
    public int Applied => Volatile.Read(ref _applied);

    /// <summary>True until <see cref="Cancel"/> is called or the cluster is disposed.</summary>
    public bool IsActive => Volatile.Read(ref _cancelled) == 0;

    /// <summary>Deactivates the rule; replies produced afterwards are no longer shortened. Idempotent.</summary>
    public void Cancel() => Interlocked.Exchange(ref _cancelled, 1);

    /// <summary>True when the rule applies to a result served by <paramref name="node"/> for <paramref name="volume"/>.</summary>
    internal bool AppliesTo(string node, string? volume) =>
        IsActive
        && (Node is null || string.Equals(Node, node, StringComparison.Ordinal))
        && (Volume is null || string.Equals(Volume, volume, StringComparison.Ordinal));

    internal void CountApplied() => Interlocked.Increment(ref _applied);
}
