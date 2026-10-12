namespace Tedd.Tdfs.Storage;

/// <summary>File data of a <see cref="SimulatedMedium"/> file (an inode): durable image, current image and unflushed changes.</summary>
internal sealed class SimulatedNode
{
    /// <summary>Content that survives a crash.</summary>
    public PagedBuffer Durable { get; set; } = new();

    /// <summary>Content visible to readers (durable image plus every unflushed change).</summary>
    public PagedBuffer Current { get; set; } = new();

    /// <summary>Unflushed writes and length changes, in issue order.</summary>
    public List<PendingWrite> Pending { get; } = [];

    /// <summary>Number of open handles referencing this node.</summary>
    public int OpenHandles { get; set; }

    /// <summary>Applies a pending change to a buffer.</summary>
    public static void Apply(PagedBuffer target, PendingWrite write)
    {
        if (write.Data is null)
            target.SetLength(write.NewLength);
        else
            target.Write(write.Offset, write.Data);
    }
}

/// <summary>One unflushed change: a write (<see cref="Data"/> non-null) or a length change.</summary>
/// <param name="Sequence">Medium-wide issue order.</param>
/// <param name="Offset">Write offset.</param>
/// <param name="Data">Written bytes (copied), or null for a length change.</param>
/// <param name="NewLength">New length for a length change.</param>
internal readonly record struct PendingWrite(long Sequence, long Offset, byte[]? Data, long NewLength);
