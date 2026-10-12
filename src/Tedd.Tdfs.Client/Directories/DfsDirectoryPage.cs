namespace Tedd.Tdfs;

/// <summary>One page of a version-pinned directory listing.</summary>
/// <param name="Entries">Entries of this page, in ordinal name order.</param>
/// <param name="ContinuationToken">Token for the next page; null when this is the last page.</param>
/// <param name="PinnedAt">The single version the whole listing observes.</param>
public sealed record DfsDirectoryPage(IReadOnlyList<DfsEntryInfo> Entries, string? ContinuationToken, CommitToken PinnedAt);
