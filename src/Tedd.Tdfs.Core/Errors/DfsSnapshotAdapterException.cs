namespace Tedd.Tdfs;

/// <summary>A snapshot adapter failed (<see cref="DfsErrorCode.AdapterFailed"/>).</summary>
public class DfsSnapshotAdapterException : DfsException
{
    /// <summary>Creates the exception without adapter details.</summary>
    public DfsSnapshotAdapterException(string message, Exception? inner = null)
        : base(DfsErrorCode.AdapterFailed, message, inner)
    {
    }

    /// <summary>Creates the exception for an adapter and phase (for example "prepare", "resume", "abort").</summary>
    public DfsSnapshotAdapterException(string adapterId, string phase, string message, Exception? inner = null)
        : base(DfsErrorCode.AdapterFailed, message, inner)
    {
        AdapterId = adapterId ?? string.Empty;
        Phase = phase ?? string.Empty;
    }

    /// <summary>Id of the failing adapter (empty when unknown).</summary>
    public string AdapterId { get; init; } = string.Empty;

    /// <summary>Adapter phase that failed (empty when unknown).</summary>
    public string Phase { get; init; } = string.Empty;
}
