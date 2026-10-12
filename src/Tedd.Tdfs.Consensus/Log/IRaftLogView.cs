namespace Tedd.Tdfs.Consensus;

/// <summary>
/// Read access to a durable Raft log: a snapshot covering indices up to <see cref="Snapshot"/>.Index followed by the
/// entries <see cref="FirstIndex"/>..<see cref="LastIndex"/> (empty when <see cref="LastIndex"/> &lt; <see cref="FirstIndex"/>).
/// </summary>
public interface IRaftLogView
{
    /// <summary>Index of the first retained entry (snapshot index + 1).</summary>
    ulong FirstIndex { get; }

    /// <summary>Index of the last entry (the snapshot index when no entries are retained).</summary>
    ulong LastIndex { get; }

    /// <summary>Term of the entry at <paramref name="index"/>; the snapshot index is answered from the snapshot metadata.</summary>
    /// <exception cref="ArgumentOutOfRangeException">The index is compacted (below the snapshot index) or beyond <see cref="LastIndex"/>.</exception>
    ulong TermAt(ulong index);

    /// <summary>Returns at most <paramref name="maxCount"/> entries of [<paramref name="from"/>, <paramref name="toExclusive"/>).</summary>
    /// <exception cref="ArgumentOutOfRangeException">The range starts below <see cref="FirstIndex"/> (compacted) or ends beyond <see cref="LastIndex"/> + 1.</exception>
    IReadOnlyList<RaftEntry> Entries(ulong from, ulong toExclusive, int maxCount);

    /// <summary>Metadata of the latest snapshot (<see cref="RaftSnapshotMeta.Empty"/> initially).</summary>
    RaftSnapshotMeta Snapshot { get; }
}
