namespace Tedd.Tdfs;

/// <summary>
/// File metadata observed at <see cref="ObservedAt"/>. For a stream path (<c>/f:s</c>, Addendum A ADS-03) the length,
/// revision, inline/sparse flags and extent count describe the addressed stream; identity, owner, times and policy are
/// the file's.
/// </summary>
/// <param name="Path">Normalised path.</param>
/// <param name="InodeId">Inode id (stable across in-place updates and renames).</param>
/// <param name="Revision">Revision (log index of the last modification).</param>
/// <param name="Length">Length in bytes.</param>
/// <param name="CreatedUtc">Creation time (leader clock).</param>
/// <param name="ModifiedUtc">Last modification time (leader clock).</param>
/// <param name="Owner">Owning principal.</param>
/// <param name="Access">Inode access flags.</param>
/// <param name="StoragePolicy">Effective storage policy name.</param>
/// <param name="IsInline">True when the content is stored inline in metadata.</param>
/// <param name="IsSparse">True when the file has holes.</param>
/// <param name="ExtentCount">Number of extents.</param>
/// <param name="ObservedAt">Commit token at which this information was read.</param>
/// <param name="IsStream">True when the information describes a named stream (the path addressed <c>file:stream</c>).</param>
/// <param name="StreamName">The named stream's name, or null for the default stream.</param>
/// <param name="StreamCount">Number of named streams of the file (excluding the default stream).</param>
public sealed record DfsFileInfo(
    string Path,
    InodeId InodeId,
    ulong Revision,
    long Length,
    DateTimeOffset CreatedUtc,
    DateTimeOffset ModifiedUtc,
    string Owner,
    DfsInodeAccess Access,
    string StoragePolicy,
    bool IsInline,
    bool IsSparse,
    int ExtentCount,
    CommitToken ObservedAt,
    bool IsStream = false,
    string? StreamName = null,
    int StreamCount = 0);
