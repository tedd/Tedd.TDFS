# API / harness additions requested by the RST and SEC suites

Branch `tests/rst-sec`. All additions are new files; no existing signature was changed. Harness members are stubs
(`NotImplementedException` via `Stub.NotYet`) whose XML docs define the required behaviour. Pure test-logic helpers that
only use the public SDK are implemented.

## Harness stubs (tests/Tedd.Tdfs.Testing/Additions/rst-sec/)

### `TestCluster.StartRecoveryClusterAsync` — `TestCluster.rst-sec.cs`

```csharp
public Task<TestCluster> StartRecoveryClusterAsync(TestClusterOptions options, CancellationToken ct = default);
```

Starts an independent "clean host" recovery cluster: new `ClusterId`, new cluster secret/key hierarchy, own admin
principal, fresh empty disks in a new subfolder of this cluster's `DataRoot`, no volumes unless listed. Shares no
in-memory state, caches, connections, credentials, keys or disks with this cluster's live nodes, but runs on the same
transport fabric so it can reach this cluster's backup nodes (QUIC: trusts the backup hosts' certificates as the runbook
configures by pinning; never given source live-node certificates, endpoints or secrets). Deterministic mode: same
simulation (scheduler, virtual time, simulated network). Node names must not collide (default `r0..`, backup nodes
`rb0..`); both clusters' `Network` objects control the same fabric by name. Mode/transport/storage must match;
`OutOfProcess = true` starts real `Tedd.Tdfs.Host` processes with only runbook-documented configuration. Returns once
stable; disposing the source cluster disposes its recovery clusters.

Used by: RST-001, RST-002, RST-003, RST-004, RST-005, RST-006, RST-010 (new-identity case), RST-014 (out-of-process),
RST-015 (with a recovery backup node), RST-016, RST-008 and RST-011 (deterministic, S-level). Every restore covered by
test plan §9.8 (RST-001..005, RST-014..016) runs on a recovery cluster after the source was destroyed.

### `TestCluster.AddNodeAsync` overload with options transform — `TestCluster.rst-sec.cs`

```csharp
public Task<TestNode> AddNodeAsync(TestNodeSpec spec, Func<NodeOptions, NodeOptions> configure, bool join = true, CancellationToken ct = default);
```

Adds a live node whose `NodeOptions` are transformed before start (wrong `ClusterSecret`, foreign `Bootstrap`, explicit
`JoinSeeds`, ...). Does not throw when the cluster rejects the node: completes when the node process started and its join
was admitted, definitively rejected (rejection recorded in `Trace`) or timed out; the node is always added to `Nodes`.
Throws only for harness errors (duplicate name, invalid spec).

Used by: SEC-001 (`UnknownNodeWrongSecret`).

### `TestNetwork.CountAttemptsTo` — `TestNetwork.rst-sec.cs`

```csharp
public long CountAttemptsTo(string name);
```

Counts every connection attempt and every message (request, reply, one-way) addressed to the endpoint `name` since the
fabric was created, counted when the sender hands it to the transport — delivered, dropped, delayed, black-holed, refused
by cuts/partitions/isolation, or addressed to a crashed/stopped/destroyed endpoint — including senders of recovery
clusters sharing the fabric, backup nodes and every test-created client. Only the owning cluster's own harness traffic
(its `Admin` client, inspectors, stability/invariant checks) is excluded. Proves a recovery made no undeclared call to a
source service (test plan §9.8).

Used by: `RecoveryScenario.DestroySourceAsync`/`AssertNoSourceContact` → RST-001, RST-002, RST-003, RST-004, RST-005,
RST-006, RST-014 (also logged), RST-015, RST-016.

### `TestNetwork.TamperNext` and `enum TransportTamper` — `TestNetwork.rst-sec.cs`, `TransportTamper.cs`

```csharp
public MessageRule TamperNext(MessageFilter filter, TransportTamper mode, int count = 1);
public enum TransportTamper { FlipBit, Truncate, AppendGarbage, Replay, Scramble }
```

Tampers with the next `count` transmissions of matching messages *below* the transport's integrity protection. QUIC: a
datagram interposer mutates the encrypted QUIC packet(s) carrying the frame after encryption (AEAD must detect it).
InMemory/simulated: mutates the frame's protected envelope after the sender's MAC. The application frame is never
mutated before protection. `MessageRule.Matched` counts tampered transmissions (tamper point reached).

Used by: SEC-011.

## Implemented test-logic helpers (tests/Tedd.Tdfs.Testing/Additions/rst-sec/)

* `VolumeContentFixture.cs` — `ExpectedFile`, `ExpectedVolumeContent` (independent manifest), `ObservedEntry`,
  `ObservedVolumeContent`, `VolumeContentFixture` (`PopulateAsync` boundary-sized/sparse/multibyte/policy/access-tagged
  content; `CaptureAsync` for live volumes, snapshot views and backup views; `AssertMatches`, `AssertSameMetadata`,
  `Digest`; named policy `pair` = 2/2/2/2). Used by RST-001..016, SEC-005, SEC-008, SEC-009, SEC-015.
* `RecoveryScenario.cs` — `BackedUpSource`, `RecoveryScenario` (`CreateBackedUpSourceAsync`, `PublishAsync`,
  `DestroySourceAsync`, `AssertNoSourceContact`, `RestoreRequest`, `AssertNotComparable`, `AssertRestoredIdentity`,
  `AssertFileBytesAsync`, `CaptureAsync`, `CodeOf`, `AssertFailedWith`, `AwaitOutcomeAsync`, `ManualSchedule`). Used by
  every RST test and SEC-006/007/008/009/011/013/015/017.

## Behavioural expectations the tests encode (for implementers)

* Restore-from-backup and snapshot-restore work reports `DfsOperationKind.RestoreSnapshot` in its `FaultContext`
  (object import at `object.write`/`object.before-flush`/`object.after-flush`, metadata install at
  `raft.after-local-append`/`raft.after-commit`, activation at `reconfig.transition`/`apply.after`) — RST-007.
* A restore that is interrupted is resumable by re-issuing the same `DfsRestoreFromBackupRequest`; it either completes or
  reports `DfsAlreadyExistsException` when the first attempt completed (or is completing); an incomplete restored volume
  is never served (probes see `VolumeNotFound`/availability errors or the complete volume) — RST-007.
* `DfsRestoreFromBackupRequest.BackupCredentials` are the backup host's reader credentials; restored volumes keep the
  snapshot's data policies, inline threshold and limits, get a new `VolumeId`, and their tokens are not comparable with
  the source's — RST-001/002/008.
* Operation ids are bound to their volume/epoch (FSD §13): ids of a previous authority are rejected
  (`OperationExpired`/`OperationIdMismatch`/`EpochMismatch`/`CrossVolume`/`InvalidArgument`) by a restored authority even
  if session numbers coincide; querying one through the old client after same-name restore reports `Expired` (never
  `NotFound`/`Committed`) — RST-008/011.
* `DfsAdminClient.DeleteVolumeAsync` succeeds while handles are open and fences them (their later publications fail);
  it is the external fencing step before a same-name restore — RST-008/010/011.
* A retention requested at snapshot creation (`DfsSnapshotOptions.Retention`) is carried to the backup host and reported
  by `DfsBackupCheckpoint`/the host's snapshot listing — RST-012, SEC-008, SEC-018.
* Expiry ends a backup snapshot's protection; it does not oblige the host to delete it. After expiry the retention admin's
  delete may succeed, report `SnapshotNotFound` (already reclaimed) or be refused with `DfsReadOnlyException` (BAK-015),
  but never as retention-protected. A point still listed after its expiry discloses `Hold = false` and no future `ExpiresUtc`
  (past or cleared) and still restores exactly; a point that is gone (or was never held) fails precisely with `SnapshotNotFound`
  (restore, `GetAsync`) / `DfsBackupNotReachedException` whose `LastPublished` is the actual latest point (exact barrier),
  never substituted by a later point. Export, reader and source-admin credentials can neither delete nor shorten/release
  protected retention (`UnauthorizedAccessException`/`DfsRetentionProtectedException` or `DfsReadOnlyException`). The
  tests make no claim about whether the retention admin may delete an unexpired or held point directly — RST-012, SEC-008.
* After the single backup host lost its disk, the source's status for it may claim the old `S` under the old lineage only
  if `S` was genuinely re-transferred to the host (the host then serves it completely) — RST-013.
* With a manual schedule (`RecoveryScenario.ManualSchedule`) a target syncs only on `RequestSyncAsync`; awaiting a barrier
  is a wait, never a sync request — RST-004/012.
* `TestBackupNode.RemoveRecoveryKeyAsync` leaves the backup host running (restarting it without key material if
  needed); the host then reports `DfsRecoveryKeyMissingException` for restores and snapshot reads instead of refusing to
  start — RST-005.
* `TestCluster.ConnectRawAsync` accepts backup node names ("b0"); the backup host refuses every raw mutation — RST-009.
* Key rotation reports `FaultSites.KeyRotation` with `Volume` set; objects keep their key version — SEC-013, RST-006.
* `RawProtocolClient.SendFrameAsync` returns `DfsErrorCode.None` when the server answered with a non-error reply;
  storage nodes refuse direct reads/writes without a capability with `AccessDenied` regardless of object existence;
  requests before authentication are refused with `AuthenticationFailed`/`AccessDenied` or a closed connection
  (`ProtocolError`) — SEC-002/004/005/014/015.
* Volume-level authorization is checked before path resolution (a principal without `Read` gets `AccessDenied`, not
  `FileNotFound`, for absent paths) — SEC-003.
* A data capability presented by another principal is refused with `AccessDenied`; a same-principal replay of a still
  valid capability on a new connection may be served or refused (the pinned SDK handle must keep reading either way) —
  SEC-007.
* How idle unauthenticated connections are bounded (deadline, cap, refusal) is not asserted; SEC-014 only logs it and
  requires unrelated authenticated work, including a new authenticated connection, to proceed during the flood.
* Whether `ClusterAdmin` alone implies volume data access is not assumed: cluster-admin principals in SEC-003/004 also get
  explicit volume grants.
* Principal names are compared ordinally (a case variant of a name is a different, unknown principal) — SEC-002.
* A read-only transaction (begin, read, abort, `QueryOutcomeAsync` of it → `Aborted`) needs only `Read`; querying one's
  own committed operation (`QueryOperationAsync` → `Committed` with the commit token) works for a `Write` holder; whether
  live-snapshot retention needs `Snapshot` or `RetentionAdmin` is left open (a `Snapshot` holder may be allowed or denied
  without effect), but deleting a held live snapshot is refused for a `Snapshot` holder (`AccessDenied` or
  `RetentionProtected`) and audited — SEC-003, SEC-018.
* `BackupAdmin` implies no namespace write or snapshot rights — SEC-003.
* Leases are tracked on the leader's monotonic clock: a wall-clock jump on a client, the leader or a storage node neither
  revives expired writer authority (also not after the old client re-authenticates) nor expires a renewed lease — SEC-017.
* Audit entries name principal, operation, outcome (e.g. "denied"/"ok") and scope (volume/target names) and never contain
  payload bytes, secrets, passphrases or key material; principal/grant/revoke changes, failed authentication, denied
  access, key rotation, recovery-key export, volume creation/deletion and snapshot deletion are audited — SEC-016/018.
* `TestNetwork.CountAttemptsTo` excludes the owning cluster's harness traffic (its `Admin` client, inspectors and
  invariant checks), so a harness reconnecting to a destroyed source node does not fail RST-001..006/014/015.
