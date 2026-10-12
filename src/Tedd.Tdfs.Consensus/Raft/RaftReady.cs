namespace Tedd.Tdfs.Consensus;

/// <summary>
/// Work produced by a <see cref="RaftNode"/>. The host handles it in this order, then calls
/// <see cref="RaftNode.Advance"/> with the same instance:
/// <list type="number">
/// <item>Durably install <see cref="SnapshotToInstall"/> (restore the state machine from <see cref="SnapshotData"/> and
/// make the durable log start after it), append <see cref="EntriesToPersist"/> (discarding every durable entry at or after
/// the first one's index) and persist <see cref="HardState"/> — entries before (or atomically with) the hard state, since
/// a restart rejects a persisted commit index beyond the durable log.</item>
/// <item>Send <see cref="Messages"/>: votes and append acknowledgements in them depend on step 1 (CON-004).</item>
/// <item>Apply <see cref="CommittedEntries"/> in order; serve each of <see cref="ReadStates"/> once the applied index
/// reaches its <see cref="RaftReadState.Index"/>.</item>
/// </list>
/// </summary>
/// <param name="HardState">Hard state to persist, or null when unchanged.</param>
/// <param name="EntriesToPersist">Entries to append to the durable log; any durable entry at or after the first one's index is replaced.</param>
/// <param name="CommittedEntries">Entries committed and ready to apply, in order (may include entries persisted by this same ready).</param>
/// <param name="Messages">Outbound messages (send only after persisting).</param>
/// <param name="SnapshotToInstall">Snapshot received from the leader to install, if any.</param>
/// <param name="SnapshotData">Snapshot payload accompanying <paramref name="SnapshotToInstall"/>.</param>
/// <param name="ReadStates">Confirmed ReadIndex requests.</param>
/// <param name="Role">Current role.</param>
/// <param name="Leader">Currently known leader, if any.</param>
public sealed record RaftReady(RaftHardState? HardState, IReadOnlyList<RaftEntry> EntriesToPersist, IReadOnlyList<RaftEntry> CommittedEntries,
    IReadOnlyList<RaftMessage> Messages, RaftSnapshotMeta? SnapshotToInstall, ReadOnlyMemory<byte> SnapshotData,
    IReadOnlyList<RaftReadState> ReadStates, RaftRole Role, NodeId? Leader);
