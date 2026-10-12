namespace Tedd.Tdfs.Server;

/// <summary>
/// Per-Raft-group replication and catch-up statistics of one node, captured atomically on the node loop (one consistent
/// snapshot of counters and indexes). All counters cover the current incarnation only (they start at zero when the node
/// process starts) and never decrease within an incarnation. Added for the RSY suite (docs/api-additions/rsy.md).
/// </summary>
/// <param name="Group">"control" or the volume name.</param>
/// <param name="StartIndex">
/// Applied index of the state this incarnation loaded at startup (the local durable checkpoint index, before any local
/// journal replay); 0 for a node that started from an empty disk.
/// </param>
/// <param name="AppliedIndex">Index of the last entry whose effect is part of this replica's state machine.</param>
/// <param name="SnapshotIndex">Index covered by this replica's newest durable checkpoint (local compaction or installed).</param>
/// <param name="EntriesAppended">Log entries durably appended to this replica's log (local journal replay excluded).</param>
/// <param name="EntriesApplied">
/// Committed log entries handed to the state machine individually (local journal replay at startup included). Entries
/// covered by an installed checkpoint are never counted here.
/// </param>
/// <param name="EntriesCoveredByInstall">
/// Sum over every activated checkpoint install of (installed checkpoint index − applied index immediately before the
/// install). Invariant: <c>EntriesApplied + EntriesCoveredByInstall == AppliedIndex − StartIndex</c> (each index reaches
/// the state machine exactly once, either individually or through exactly one installed checkpoint).
/// </param>
/// <param name="SnapshotsInstalled">Checkpoints received from a peer that were verified, durably installed and activated.</param>
/// <param name="SnapshotInstallsAborted">
/// Checkpoint transfers/installs started but abandoned before activation (source changed or failed, newer term, transfer
/// timeout, disk full, verification failure). An aborted install never changes the activated state.
/// </param>
/// <param name="SnapshotsRejectedStale">
/// Checkpoint offers/chunks rejected because they are from an older term or do not advance this replica's state (index at
/// or below the local applied/snapshot index).
/// </param>
/// <param name="SnapshotVerificationFailures">Checkpoints (or chunks) rejected because checksum/identity verification failed.</param>
public sealed record RaftGroupStats(
    string Group,
    ulong StartIndex,
    ulong AppliedIndex,
    ulong SnapshotIndex,
    long EntriesAppended,
    long EntriesApplied,
    long EntriesCoveredByInstall,
    long SnapshotsInstalled,
    long SnapshotInstallsAborted,
    long SnapshotsRejectedStale,
    long SnapshotVerificationFailures);
