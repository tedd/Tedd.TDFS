namespace Tedd.Tdfs.Consensus;

/// <summary>
/// Read access to a durable Raft log: a snapshot covering indices up to <see cref="Snapshot"/>.Index followed by the
/// entries <see cref="FirstIndex"/>..<see cref="LastIndex"/> (empty when <see cref="LastIndex"/> &lt; <see cref="FirstIndex"/>).
/// </summary>
/// <remarks>
/// A <see cref="RaftNode"/> reads only persisted state through this view and keeps not-yet-persisted entries itself, so a
/// durable store (for example a journal-backed log store) implements it directly. Obligations of the implementing host:
/// <list type="bullet">
/// <item>When handling a <see cref="RaftReady"/>, append <see cref="RaftReady.EntriesToPersist"/> by discarding every
/// entry at or after the first one's index, and install <see cref="RaftReady.SnapshotToInstall"/> by dropping the
/// entries it covers (all of them when the snapshot's last entry is not held with the same term); both must be visible
/// through this view before <see cref="RaftNode.Advance"/> is called.</item>
/// <item>The host may compact on its own (replace a prefix with a snapshot) only up to the node's applied index, with
/// the membership in effect at that index (the host tracks it by applying committed configuration entries), and must
/// be able to serve that snapshot's payload through <see cref="IRaftSnapshotSource"/>.</item>
/// <item>Reads happen on the node loop and must not block; entries returned must be the persisted ones, in index order.</item>
/// </list>
/// </remarks>
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
