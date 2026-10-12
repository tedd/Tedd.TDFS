namespace Tedd.Tdfs;

/// <summary>One directory entry from an enumeration.</summary>
/// <param name="Name">Entry name (NFC).</param>
/// <param name="Path">Full normalised path.</param>
/// <param name="Kind">File or directory.</param>
/// <param name="InodeId">Inode id.</param>
/// <param name="Revision">Inode revision at the enumeration's pinned version.</param>
/// <param name="Length">File length (0 for directories).</param>
/// <param name="ModifiedUtc">Last modification time (leader clock).</param>
public sealed record DfsEntryInfo(string Name, string Path, DfsEntryKind Kind, InodeId InodeId, ulong Revision, long Length, DateTimeOffset ModifiedUtc);
