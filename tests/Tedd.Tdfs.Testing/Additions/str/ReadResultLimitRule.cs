namespace Tedd.Tdfs.Testing;

/// <summary>
/// An active backend short-read rule created by <see cref="TestFaults.LimitReadResultBytes"/> (STR-004, STR-006).
/// </summary>
public sealed class ReadResultLimitRule
{
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
    public int Applied => throw Stub.NotYet("ReadResultLimitRule.Applied");

    /// <summary>True until <see cref="Cancel"/> is called or the cluster is disposed.</summary>
    public bool IsActive => throw Stub.NotYet("ReadResultLimitRule.IsActive");

    /// <summary>Deactivates the rule; replies produced afterwards are no longer shortened. Idempotent.</summary>
    public void Cancel() => throw Stub.NotYet("ReadResultLimitRule.Cancel");
}
