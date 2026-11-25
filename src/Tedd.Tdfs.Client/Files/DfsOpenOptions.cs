namespace Tedd.Tdfs;

/// <summary>Options for <see cref="DfsFileClient.OpenAsync(string, DfsOpenOptions, CancellationToken)"/>.</summary>
public sealed record DfsOpenOptions
{
    /// <summary>Directory mapping for a live export reservation. Paths are absolute within this root.</summary>
    public DfsExportRoot? ExportRoot { get; init; }

    /// <summary>Desired access for an export open. Defaults to Access when omitted.</summary>
    public DfsOpenAccess? DesiredAccess { get; init; }

    /// <summary>Open or create a directory within the mapping.</summary>
    public bool Directory { get; init; }

    /// <summary>Set delete disposition in the open decision. Requires delete access on the base inode.</summary>
    public bool DeleteOnClose { get; init; }

    /// <summary>Open mode.</summary>
    public FileMode Mode { get; init; } = FileMode.Open;

    /// <summary>Requested access.</summary>
    public FileAccess Access { get; init; } = FileAccess.Read;

    /// <summary>Sharing granted to other handles.</summary>
    public FileShare Share { get; init; } = FileShare.Read;

    /// <summary>Publication mode for writes.</summary>
    public DfsWriteMode WriteMode { get; init; } = DfsWriteMode.OnFlush;

    /// <summary>Read view.</summary>
    public DfsReadView ReadView { get; init; } = DfsReadView.Live;

    /// <summary>Client buffer size in bytes (does not determine the replication unit).</summary>
    public int BufferSize { get; init; } = 64 * 1024;

    /// <summary>File options; anything but None, Asynchronous, SequentialScan or RandomAccess is rejected with <see cref="NotSupportedException"/>.</summary>
    public FileOptions Options { get; init; } = FileOptions.None;

    /// <summary>Named storage policy for a newly created file; null uses the volume default.</summary>
    public string? StoragePolicy { get; init; }

    /// <summary>Operation id for a creating or truncating open; null allocates one.</summary>
    public DfsOperationId? OperationId { get; init; }
}
