namespace Tedd.Tdfs.Server;

/// <summary>Applied metadata of one file.</summary>
/// <param name="Path">Normalized path.</param>
/// <param name="Inode">Inode id.</param>
/// <param name="Revision">Raft index of the last modification.</param>
/// <param name="Length">File length.</param>
/// <param name="IsInline">True when the bytes are stored inline in the inode.</param>
/// <param name="WriterGeneration">Current writer fencing generation.</param>
/// <param name="Policy">Storage policy name.</param>
/// <param name="Extents">Extent map in offset order (empty for inline files).</param>
public sealed record FileInspection(string Path, InodeId Inode, ulong Revision, long Length, bool IsInline, ulong WriterGeneration,
    string Policy, IReadOnlyList<ExtentInspection> Extents);
