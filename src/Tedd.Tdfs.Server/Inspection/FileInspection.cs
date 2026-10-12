namespace Tedd.Tdfs.Server;

/// <summary>Applied metadata of one file.</summary>
/// <param name="Path">Normalized path.</param>
/// <param name="Inode">Inode id.</param>
/// <param name="Revision">Raft index of the last modification.</param>
/// <param name="Length">File length (default stream).</param>
/// <param name="IsInline">True when the bytes are stored inline in the inode.</param>
/// <param name="WriterGeneration">Current writer fencing generation (default stream).</param>
/// <param name="Policy">Storage policy name.</param>
/// <param name="Extents">Extent map in offset order (empty for inline files).</param>
/// <param name="Streams">Named streams (Addendum A, ADS-02), ordinal by name; empty when the file has none.</param>
public sealed record FileInspection(string Path, InodeId Inode, ulong Revision, long Length, bool IsInline, ulong WriterGeneration,
    string Policy, IReadOnlyList<ExtentInspection> Extents, IReadOnlyList<StreamInspection> Streams = null!)
{
    /// <summary>Named streams (Addendum A, ADS-02), ordinal by name; never null (empty when the file has none).</summary>
    public IReadOnlyList<StreamInspection> Streams { get; init; } = Streams ?? [];
}
