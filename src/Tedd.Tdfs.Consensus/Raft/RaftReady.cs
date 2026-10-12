namespace Tedd.Tdfs.Consensus;

/// <summary>
/// Work produced by a <see cref="RaftNode"/>: the host persists <see cref="HardState"/> and
/// <see cref="EntriesToPersist"/> (and installs <see cref="SnapshotToInstall"/>) durably, then sends
/// <see cref="Messages"/>, applies <see cref="CommittedEntries"/>, and finally calls <see cref="RaftNode.Advance"/>.
/// </summary>
/// <param name="HardState">Hard state to persist, or null when unchanged.</param>
/// <param name="EntriesToPersist">Entries to append to the durable log (conflicting suffixes are truncated).</param>
/// <param name="CommittedEntries">Entries committed and ready to apply, in order.</param>
/// <param name="Messages">Outbound messages (send only after persisting).</param>
/// <param name="SnapshotToInstall">Snapshot received from the leader to install, if any.</param>
/// <param name="SnapshotData">Snapshot payload accompanying <paramref name="SnapshotToInstall"/>.</param>
/// <param name="ReadStates">Confirmed ReadIndex requests.</param>
/// <param name="Role">Current role.</param>
/// <param name="Leader">Currently known leader, if any.</param>
public sealed record RaftReady(RaftHardState? HardState, IReadOnlyList<RaftEntry> EntriesToPersist, IReadOnlyList<RaftEntry> CommittedEntries,
    IReadOnlyList<RaftMessage> Messages, RaftSnapshotMeta? SnapshotToInstall, ReadOnlyMemory<byte> SnapshotData,
    IReadOnlyList<RaftReadState> ReadStates, RaftRole Role, NodeId? Leader);
