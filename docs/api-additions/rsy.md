# API additions — RSY suite (RSY-001..RSY-026)

Additions made while writing the RSY tests (`tests/Tedd.Tdfs.Tests.Simulation/Rsy/`). All are stubs that throw
`NotImplementedException` until implemented; no existing signature was changed.

## 1. `TestCluster.TransferLeadershipAsync` (harness)

File: `tests/Tedd.Tdfs.Testing/Additions/rsy/TestCluster.rsy.cs`

```csharp
public Task<TestNode> TransferLeadershipAsync(string group, string nodeName, TimeSpan? timeout = null, CancellationToken ct = default);
```

Behaviour: moves leadership of Raft group `group` (`"control"` or a volume name) to `nodeName` through the protocol's
own leadership transfer (current leader waits until the target is caught up, sends `TimeoutNow`; the target wins an
election in a higher term). Never forces a role or bypasses vote/log checks. Completes when the target reports
`RaftRole.Leader`, every reachable voter reports it as leader, and it applied its own no-op of the new term (so it serves
linearizable reads). Transferring to the current leader returns immediately. `InvalidOperationException` when the target
is not Running or not a voter of the group; `TimeoutException` after `timeout` (cluster time, default 10 s).

Needed by: RSY-001, RSY-002, RSY-004..RSY-013, RSY-018, RSY-026 (`RsyHelpers.ServeFromAsync`: prove the returning replica
itself serves the authoritative state / rejects stale requests), RSY-026 (leader change before a delayed checkpoint).

## 2. `TestFaults.Arm(FaultMatch, Func<FaultContext,bool>, FaultAction)` (harness)

File: `tests/Tedd.Tdfs.Testing/Additions/rsy/TestFaults.rsy.cs`

```csharp
public FaultArm Arm(FaultMatch match, Func<FaultContext, bool> predicate, FaultAction action);
```

Behaviour: like `Arm(match, action)`, but a hit counts as matching only when `match.Matches(ctx) && predicate(ctx)`.
`Occurrence`/`Count`, `FaultArm.Contexts`, `Hits`, `Triggered`, `WaitTriggeredAsync` and `AssertReached` consider only
such hits. The predicate runs on the hitting node's loop, must be pure and non-blocking. Every hit is still recorded in
`Coverage`.

Needed by: RSY-012 and RSY-013 (select checkpoint catch-up sub-stages by `FaultContext.Detail`).

## 3. `ResyncStageDetails` (harness constants + server requirement)

File: `tests/Tedd.Tdfs.Testing/Additions/rsy/ResyncStageDetails.cs`

```csharp
public static class ResyncStageDetails
{
    public const string Receive = "receive", Verify = "verify", Install = "install", Suffix = "suffix", Activate = "activate";
    public static IReadOnlyList<string> All { get; }
}
```

Required server behaviour (product): on the **receiving** replica of a peer checkpoint (Raft InstallSnapshot path) the
server calls `IFaultInjector.HitAsync` with `Site = FaultSites.RaftSnapshotInstall`, `Node` = receiving node name,
`Volume` = the group's volume name (null for the control group), `Operation = DfsOperationKind.RaftSnapshot`, and
`Detail` =

| Detail | Point |
|---|---|
| `receive` | each checkpoint chunk received, before it is persisted (`Occurrence` = 1-based chunk number) |
| `verify` | all chunks durably received, before checksum/identity/lineage verification of the whole image |
| `install` | verified, before it durably replaces local state / log prefix |
| `suffix` | after install, before each post-checkpoint log entry of the same catch-up is appended/applied |
| `activate` | installed (+ suffix so far), before the new state-machine root is activated (served, counted as caught up) |

Each hit precedes the sub-stage's durable/visible effect, so a crash at the hit leaves only previously completed durable
progress. Every RSY checkpoint test also relies on `RaftSnapshotInstall` hits carrying `Node` and `Volume` as above
(RSY-002 interrupts "the first install hit on C for volume vol").

Needed by: RSY-002 (any detail), RSY-012 (every detail), RSY-013 (`receive`, `suffix`), and `RsyHelpers.AssertCatchupPathAsync`
(log path ⇒ no `RaftSnapshotInstall` hit on the returning node; checkpoint path ⇒ at least one).

## 4. `NodeInspector.GetRaftGroupStatsAsync` + `RaftGroupStats` (server inspection)

Files: `src/Tedd.Tdfs.Server/Additions/rsy/NodeInspector.rsy.cs`, `src/Tedd.Tdfs.Server/Additions/rsy/RaftGroupStats.cs`

```csharp
public Task<RaftGroupStats?> GetRaftGroupStatsAsync(string group);   // null when the node does not host the group

public sealed record RaftGroupStats(string Group, ulong StartIndex, ulong AppliedIndex, ulong SnapshotIndex,
    ulong LogFirstIndex, long EntriesAppended, long EntriesApplied, long EntriesCoveredByInstall, long SnapshotsInstalled,
    long SnapshotInstallsAborted, long SnapshotsRejectedStale, long SnapshotVerificationFailures);
```

Behaviour: one atomic snapshot (taken on the node loop) of a group's catch-up counters for the current incarnation
(counters start at 0 at process start and never decrease within an incarnation):

* `StartIndex` — applied index of the state loaded at startup (local checkpoint index before journal replay; 0 if empty).
* `AppliedIndex`, `SnapshotIndex` — as in `GroupInspection`, read at the same instant as the counters.
* `LogFirstIndex` — first index still held in the log (`SnapshotIndex + 1` with exact compaction, lower if a trailing
  window of checkpointed entries is retained, `LastIndex + 1` when empty). Used for exact catch-up preconditions: the
  checkpoint path is forced only when every source has `LogFirstIndex > C.LastIndex + 1`; the log path is guaranteed when
  every source has `LogFirstIndex <= C.AppliedIndex + 1`.
* `EntriesAppended` — entries durably appended to the log (local replay excluded).
* `EntriesApplied` — committed entries handed to the state machine individually (startup journal replay included);
  entries covered by an installed checkpoint are never counted.
* `EntriesCoveredByInstall` — Σ over activated peer-checkpoint installs of (installed index − applied index just before).
  Invariant tested everywhere: `EntriesApplied + EntriesCoveredByInstall == AppliedIndex − StartIndex`.
* `SnapshotsInstalled` — peer checkpoints verified, durably installed and activated.
* `SnapshotInstallsAborted` — transfers/installs abandoned before activation (source change, newer term, timeout, disk
  full, verification failure).
* `SnapshotsRejectedStale` — checkpoint offers/chunks rejected for an older term or for not advancing local state.
* `SnapshotVerificationFailures` — checkpoints/chunks rejected because checksum or identity verification failed.

Needed by: RSY-001 (each missing index applied exactly once, no install), RSY-002/RSY-004..RSY-011 (checkpoint vs log path),
RSY-012, RSY-013, RSY-014, RSY-015 (rejected corrupt chunk), RSY-022 (reported failed install), RSY-026 (stale checkpoint
rejected, no install, no rollback).

## Assumptions on existing harness members (no signature change)

* `MessageHold.Captured` lists every frame captured since the hold was created, including frames already released
  (RSY-026 captures checkpoint chunks while letting them through, then replays them later).
* `TestNetwork.ReplayAsync` delivers the `Frame` of the given `CapturedMessage` verbatim from its original sender, so a
  modified copy (`captured with { Frame = corrupted }`) injects corrupted/truncated bytes (RSY-015).
* `TestDisk.DeleteObjectAsync` / `CorruptObjectAsync` work on running nodes (silent loss/corruption the node must discover
  by inventory/scrub) and on stopped nodes (durable image).
* `TestDisk.Faults.CapacityOverride` can be set while the node is stopped and reset to `null` to restore space (RSY-022,
  RSY-023).
* `ClusterInspector.CountDurableDomainsAsync` / `GetObjectHoldersAsync` report harness ground truth (verified copies on
  running nodes); product-reported health is read through `DfsAdminClient.GetReplicaHealthAsync`.
* `FaultSites.RaftAfterCommitBeforeApply` is hit on **every** replica (followers included) before it applies a committed
  entry, with `Node` and `Volume` filled in (RSY-003 blocks a returning follower's apply with it).
* `TestNode.RestartAsync` completes once the node has recovered locally and runs; it does not wait for peers, so it also
  completes while the node is partitioned or its inbound Raft traffic is held (RSY-003, RSY-012). When a fault armed on the
  restarting node crashes it before the call returns, the returned task may fault; the RSY tests start the restart, wait
  for the armed fault, and tolerate that failure only when the node is `Crashed` (`RsyHelpers.AwaitInterruptedRestartAsync`).
* Disposing a `DfsClient` (or its session expiring) closes the staging sessions its whole-file helpers used, so their
  unreferenced objects become reclaimable (RSY-005 and RSY-018 dispose the writing client before waiting for reclamation).
