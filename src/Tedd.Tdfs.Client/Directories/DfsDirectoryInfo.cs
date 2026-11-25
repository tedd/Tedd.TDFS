namespace Tedd.Tdfs;

/// <summary>Directory metadata observed at <see cref="ObservedAt"/>.</summary>
/// <param name="Path">Normalised path.</param>
/// <param name="InodeId">Inode id.</param>
/// <param name="Revision">Revision (log index of the last modification).</param>
/// <param name="CreatedUtc">Creation time (leader clock).</param>
/// <param name="ModifiedUtc">Last modification time (leader clock).</param>
/// <param name="Owner">Owning principal.</param>
/// <param name="EntryCount">Number of direct children.</param>
/// <param name="ObservedAt">Commit token at which this information was read.</param>
public sealed record DfsDirectoryInfo(
    string Path,
    InodeId InodeId,
    ulong Revision,
    DateTimeOffset CreatedUtc,
    DateTimeOffset ModifiedUtc,
    string Owner,
    int EntryCount,
    CommitToken ObservedAt)
{
    public DfsBasicInformation? BasicInformation { get; init; }
}
