# Tedd.TDFS — Architecture and Engineering Decisions

Status: binding design for implementation and tests. Requirements live in
[spec/distributed-filesystem-fsd.md](spec/distributed-filesystem-fsd.md) (the **FSD**) and
[spec/distributed-filesystem-test-plan.md](spec/distributed-filesystem-test-plan.md) (the **test plan**).
Exact type and member signatures are in [API.md](API.md). Where this document and API.md disagree, API.md wins
for signatures and this document wins for behaviour. Neither may weaken an FSD `SHALL`.

Root namespace: `Tedd.Tdfs`. Target framework: `net10.0`. Test framework: xunit.v3 on Microsoft.Testing.Platform.

---

## 1. Solution layout

| Project | Kind | Contents |
|---|---|---|
| `src/Tedd.Tdfs.Core` | lib | Public value types shared by SDK and server (`CommitToken`, `DfsOperationId`, identifiers, policies, topology labels), **all public exceptions** and `DfsErrorCode`, `DfsPath` (validation/normalisation), wire codec (`WireWriter`/`WireReader`), protocol message definitions and registry, checksums, crypto helpers, `FaultStage`/`FaultSites`/`IFaultInjector`, `IRandomSource`, `SafetyMutations`. |
| `src/Tedd.Tdfs.Transport` | lib | `IRpcTransport` abstraction, `InMemoryNetwork` transport, `QuiclyRpcTransport` (Tedd.Quicly over MsQuic), fragmentation/correlation, `FaultInterposer` hooks (link rules are applied here). |
| `src/Tedd.Tdfs.Client` | lib | Public SDK: `DfsClient`, `DfsVolume`, facades, `DfsStream`, transactions, snapshots, backups, `DfsAdminClient`. Namespace `Tedd.Tdfs` (public), internals in `Tedd.Tdfs.Client.*`. |
| `src/Tedd.Tdfs.Storage` | lib | `IStorageMedium` (+ `FileSystemMedium`, `SimulatedMedium`), superblock, segmented journal (Raft log store), checkpoint store, packed object store, record cipher, backup store. |
| `src/Tedd.Tdfs.Consensus` | lib | Pure Raft core (`RaftNode`), `IRaftStorage`, `RaftGroupHost` (multi-group driver bound to a node runtime). |
| `src/Tedd.Tdfs.Server` | lib | `NodeHost` runtime (actor event loop), RPC dispatch, auth, control state machine, volume state machine, extent/data service, placement, leases, transactions, snapshots, GC, scrub, repair, backup exporter, `BackupNodeHost`, admin service, metrics. |
| `src/Tedd.Tdfs.Host` | exe | Command-line host: `tdfs node --config node.json`, `tdfs backup --config backup.json`, `tdfs bootstrap`, `tdfs recover`. |
| `tests/Tedd.Tdfs.Testing` | lib | Harness: `TestCluster`, `ClusterTest`, deterministic simulation (`SimScheduler`), `TestNetwork`, `TestFaults`, `TestDisk`, `TestClock`, inspectors, reference models, history checkers, model checker, fault-coverage registry, catalog attributes. |
| `tests/Tedd.Tdfs.Tests.Unit` | tests | Level `U` tests. |
| `tests/Tedd.Tdfs.Tests.Simulation` | tests | Level `S` tests. |
| `tests/Tedd.Tdfs.Tests.Integration` | tests | Level `I` and `H` tests (real QUIC, real disk, real processes). |
| `tests/Tedd.Tdfs.Tests.Models` | tests | Level `M` tests (bounded model checking). |
| `tests/Tedd.Tdfs.Tests.Performance` | tests | Level `P` tests (resource envelopes; opt-in heavy runs). |

Dependency direction: Core ← Transport ← Client; Core ← Storage ← Consensus; {Core, Transport, Storage, Consensus} ← Server;
Server + Client ← Testing ← Tests. **Client never references Server.** Server never references Client.

---

## 2. Cluster model

### 2.1 Roles

* **Live node** (`NodeHost`): every live node hosts the *extent store* (data role), is a member (voter or learner) of
  the *control group*, and may be a voter/learner of any number of *volume groups*.
* **Backup node** (`BackupNodeHost`): a late-sync target. Never a member of any Raft group, never stores live
  replicas, never grants leases. It receives snapshot exports, stores them, and serves read-only snapshot views.
* **Client**: `DfsClient` SDK instance. Talks to any live node for discovery, to volume leaders for metadata, and to
  storage nodes directly for bulk data. Bulk data never passes through a metadata leader merely to reach storage.

### 2.2 Identities

| Identity | Type | Notes |
|---|---|---|
| Cluster | `Guid ClusterId` + name | Fixed at bootstrap; every handshake checks it. |
| Node | `NodeId` (Guid) + `Name` (string, unique) | Persisted in the node superblock. |
| Node incarnation | `ulong` | Superblock counter incremented durably on every start before serving. Receipts and Raft messages carry `(NodeId, Incarnation)`; stale incarnations are rejected. Two running processes with the same `NodeId` (cloned disk) are detected via incarnation/registration conflict and fenced (RSY-021). |
| Volume | `VolumeId` (Guid) + `Name` + `VolumeEpoch` (ulong) | Epoch increments on restore/promotion (new authority). |
| Inode | `InodeId` (ulong) | Allocated monotonically by the volume state machine; never reused. Root = 1. |
| Revision | `ulong` | Raft log index of the entry that last modified the inode. |
| Commit token | `CommitToken(ClusterId, VolumeId, VolumeEpoch, Sequence)` | `Sequence` = Raft log index of the applied entry. Comparable only within the same cluster+volume+epoch. |
| Object | `ObjectId` (128-bit, random, generated by the uploader) | Immutable: same id must always mean same bytes; conflicting re-upload is rejected. |
| Operation | `DfsOperationId(Session, Sequence)` | `Session` is server-issued per volume (Raft-committed), `Sequence` is client-allocated, monotonic per session. |
| Transaction | `DfsTransactionId` | Wraps the `DfsOperationId` of the transaction's begin. Durable. |
| Snapshot | `SnapshotId` (Guid) + unique name per volume | |
| Handle | `HandleId` (ulong) | Allocated by the volume state machine. |
| Staging session | `StagingId` (ulong) | Allocated by the volume state machine. |
| Writer generation | `ulong` per inode | Monotonic fencing generation, persisted in the inode record. |

### 2.3 Topology

`DfsTopologyLabels(Region, Zone, Rack, Host, Device)`. Failure-domain counting:

* **Copies** are counted by distinct `Host` (two nodes sharing a host label count once — REP-005).
* **Zones** are counted by distinct `(Region, Zone)`.
* A witness or a backup never counts as a data copy (REP-006/007).

### 2.4 Groups

* **Control group**: Raft group over all live nodes. `ControlVoters` (default 3, or 5; never more than nodes) are
  voters placed one-per-zone when possible; all other live nodes are learners (they receive the log so every node
  has the cluster map). Holds: cluster identity, members (labels, endpoints, status, incarnation), placement
  epoch, volumes (id, name, epoch, voter set, state), named storage policies, principals/credentials/grants,
  wrapped key material, backup target registrations, feature flags.
* **Volume group** (one per volume): Raft group with `Metadata.Voters` voters (default 3) placed per policy
  (`OnePerZone` default, or explicit node list). Holds the volume state machine (§5).
* Consensus requires a majority of the group's **configured** voters, never of reachable nodes (RCV-01, MUT-004).
* Different volumes' groups and leaders are spread over nodes (leader balancing by `TimeoutNow` transfers).

### 2.5 Bootstrap, join, restart

* **Static bootstrap** (`BootstrapMode.Static`): all initial nodes start with an identical `ClusterBootstrap`
  description (cluster id/name, member list with ids/names/labels/endpoints, control voter ids, cluster secret,
  initial principals). Each node writes its superblock and initial control checkpoint at index 0 and starts.
* **Seed join** (`BootstrapMode.SeedJoin`): one node bootstraps a single-member cluster; other nodes start with
  `JoinSeeds` (any subset of endpoints — a "seed graph": full, ring, star, line, random). A joining node contacts a
  seed, follows leader hints to the control leader, which commits `AddMember`, then the node becomes a learner,
  catches up, and the leader promotes learners to voters until `ControlVoters` is reached (one change at a time).
* **Restart**: node reads its superblock, increments incarnation durably, recovers each Raft group from journal +
  checkpoint, and re-registers its incarnation with the control group before issuing receipts (RCV-02).
* Removed members are rejected by peers (their `NodeId` is marked removed in the control state); re-admission
  requires an explicit admin join with a new incarnation lineage (CON-016, RSY-020).

---

## 3. Execution model (mandatory for all server code)

### 3.1 Node event loop

Each `NodeHost` / `BackupNodeHost` owns a **single-threaded event loop** exposed as a `SynchronizationContext`
(`NodeLoop`). All node state is created, read and mutated only on that loop. Rules for code in
`Tedd.Tdfs.Server`, `Tedd.Tdfs.Consensus` and `Tedd.Tdfs.Storage`:

1. **Never** use `ConfigureAwait(false)`. Every `await` resumes on the node loop.
2. **Never** use `Task.Run`, `new Thread`, `ThreadPool`, `System.Threading.Timer`, `Task.Delay(TimeSpan)` without a
   provider, `DateTime.UtcNow`, `DateTimeOffset.UtcNow`, `Stopwatch`, `Environment.TickCount`, `Guid.NewGuid()`, or
   `Random.Shared`. Use the injected `NodeEnvironment`: `TimeProvider Time`, `IRandomSource Random`,
   `INodeScheduler Scheduler` (`OffloadAsync` for CPU-heavy work such as hashing/encryption of large buffers; it
   runs inline in deterministic simulation), `IStorageMedium Medium`, `IRpcTransport Transport`,
   `IFaultInjector Faults`, `INodeLog Log`.
3. No locks are needed for node state. Do not share mutable state between nodes.
4. **Determinism:** replicated state machines must produce identical results on every replica and every process:
   never iterate a hash-ordered collection where order affects output, state, or a message. Use
   `SortedDictionary`/`ImmutableSortedDictionary`/sorted arrays with ordinal comparers. Never use
   `string.GetHashCode()` for anything persisted or replicated.
5. Time used inside a replicated decision (e.g. lease expiry, wall-clock timestamps for file metadata) is chosen by
   the **leader** and carried **inside the log entry**; apply code never reads a clock.

### 3.2 Deterministic simulation

`Tedd.Tdfs.Testing` provides `SimScheduler`: one OS thread, a seeded priority queue of `(virtualTime, seq, work)`,
per-node `SynchronizationContext`s, a `SimTimeProvider` per node (global virtual time × per-node offset/rate, can be
frozen/jumped), `SimNetwork` (an `IRpcTransport` factory with seeded delay/drop/duplicate/reorder and link rules) and
`SimulatedMedium` (crash-faithful in-memory disk). Pausing a node holds its queue; crashing a node discards its
queue, its heap (the `NodeHost` object is abandoned) and its medium's volatile state. The SDK (`Tedd.Tdfs.Client`)
must also be **simulation-compatible** (§9.6) so whole-stack scenarios run deterministically and replay exactly
from a seed (HAR-001). Posting work to a sim context from a non-sim thread is reported as an uncontrolled dependency
(HAR-002).

### 3.3 Production loop

`NodeLoop` in production is a dedicated thread running a work queue (`BlockingCollection`-free: a lock-protected
queue + `ManualResetEventSlim`). Transport callbacks, disk I/O completions and timers post continuations to it.

---

## 4. Consensus (Raft)

`RaftNode` is a **pure state machine** in the style of etcd/raft: no I/O, no clock. It is driven by:

* `Tick()` — logical clock tick (the host calls it every `TickInterval`, e.g. 10 ms prod / configurable in tests).
* `Step(RaftMessage)` — inbound message.
* `Propose(ReadOnlyMemory<byte> command)` / `ProposeConfChange(RaftConfChange)` / `ReadIndex(ulong requestCtx)` /
  `TransferLeadership(NodeId)`.
* `GetReady()` → `RaftReady { HardState?, EntriesToPersist, CommittedEntries, MessagesToSend, SnapshotToInstall?,
  ReadStates, SoftState(lead, role) }`; the host persists `HardState` + entries (**durably, before sending messages
  that depend on them**: votes and append acknowledgements — CON-004), sends messages, applies committed entries,
  then calls `Advance(ready)`.

Features: leader election with **PreVote** and **CheckQuorum**; log replication with conflict-index fast backtrack;
commit only entries from the current term by counting (Raft §5.4.2); **ReadIndex** linearizable reads (leader confirms
leadership with a heartbeat quorum after the read arrives, then waits for apply ≥ read index — CON-008/009);
**InstallSnapshot** for lagging followers (log compaction beyond `SnapshotThreshold` entries); **single-server
membership changes** with **learners** (add learner → caught up within `CatchUpLag` → promote → remove), one pending
change at a time, effective on append; leadership transfer via `TimeoutNow`. Removed/unknown peers' messages are
ignored. Messages carry `(GroupId, Term, From NodeId+Incarnation)`.

`IRaftStorage` (implemented by `Storage.RaftLogStore`): durable `HardState(Term, Vote, Commit)`, entries
`[FirstIndex..LastIndex]`, `Term(index)`, `Snapshot` metadata `(Index, Term, ConfState)`. Persisting term/vote uses
write-temp + flush + rename + directory flush. Entries are appended to segment files with per-record CRC32C and a
flush barrier per batch. Torn tails are truncated at recovery; a corrupt **non-tail** record is an integrity failure
that stops the group (never silently skipped).

`RaftGroupHost` (Server) owns all groups of a node: drives ticks, batches outbound messages per destination node,
persists, applies to the group's `IReplicatedStateMachine` (`Apply(entry) → result`, `TakeCheckpoint()`,
`RestoreCheckpoint(stream)`), and correlates proposals with apply results so RPC handlers can `await` them
(`ProposeAsync(command) → ApplyResult` — completes only after the entry is **committed and applied**; leader
change before commit → `NotLeader`/outcome-unknown handling by the caller).

---

## 5. Volume state machine (VSM)

### 5.1 State shape

The VSM state is an **immutable persistent structure** (`VolumeState`, built from `ImmutableSortedDictionary` /
`ImmutableDictionary` with deterministic iteration where it matters). Each applied entry produces a new root, so old
roots can be retained cheaply for transactions and snapshots.

```
VolumeState
  Volume: id, name, epoch, policies (default + named), inline threshold, limits, key versions
  Inodes: InodeId -> InodeRecord (immutable)
  NextInodeId, NextHandleId, NextStagingId, NextSessionId
  Handles: HandleId -> HandleRecord {inode, session, access, share, mode, isWriter, writerGeneration,
                                      readView, pinnedInode (for VersionPinned), pinIndex, leaseExpiry(leader time), txId?}
  Sessions: SessionId -> SessionRecord {principal, state Active|Expired, lastActivity, retiredWatermark,
                                        outcomes: SortedMap<seq, OutcomeRecord>, expiredAt}
  RetiredSessionsBelow: SessionId watermark (all sessions below it are fully retired)
  Transactions: TxId -> TxRecord {state Active|Committed|Aborted, readIndex, reservations, stagingId}
  Staging: StagingId -> StagingRecord {owner session/handle/tx, state Open|Closed, createdIndex}
  Snapshots: SnapshotId -> SnapshotRecord {name, boundaryIndex, label, adapters, createdUtc, retention, root(inodes map)}
  Pins: PinId -> PinRecord {kind Handle|SnapshotReader|Transfer|Enumeration, createdIndex, expiry, snapshotId?}
  DerefLog: sorted (index, objectId) of objects removed from the current root (for GC watermarking)
  BackupProgress: target -> {lastAckedSnapshot, coveredIndex, retention, reseedRequired, lineage}
InodeRecord
  Id, Kind(File|Directory), Revision, CreatedUtc, ModifiedUtc, Owner, Acl, PolicyName,
  ParentId, Name (single link; no hard links), Detached flag,
  File: Length, Inline bytes (<= inline threshold) OR Extents: ImmutableSortedDictionary<long offset, ExtentRef>,
        WriterGeneration
  Directory: Children: ImmutableSortedDictionary<string(NFC, ordinal), InodeId>
ExtentRef { Offset, Length, ObjectId, ObjectOffset, ObjectLength, Checksum(xxHash128 of whole object plaintext),
            Encoding, PlacementEpoch, LocationsHint[] }
```

Sparse holes are simply ranges without extents (reads return zeroes). Truncation removes/clips extents beyond the new
length; extension never resurrects clipped data because clipped extents are removed from the map (STR-011).

### 5.2 Commands (log entries)

Every mutating command carries an `OperationHeader { DfsOperationId Id, Principal, OpKind, PayloadHash(SHA-256/16),
ClientAckWatermark }` and is applied through `OperationGate`:

1. Session unknown and below `RetiredSessionsBelow`, or session `Expired`, or `seq <= RetiredWatermark` and no
   outcome retained → **result: `OperationExpired`** (no effect).
2. Outcome retained for `(session, seq)`: if principal/opKind/payloadHash differ → `OperationIdMismatch`;
   else → return the **stored outcome** (no second effect).
3. Otherwise execute, then store the outcome record `{opKind, payloadHash, principal, status, result, index}`.
4. Retire outcomes `< ClientAckWatermark`, and retire the oldest (by deciding log index, whatever their sequences)
   beyond `MaxOutcomesPerSession` (advancing `RetiredWatermark` to the highest retired sequence, which is durable state
   — OUT-012/019; unused sequences below it, reserved ones included, are expired from then on).

`FenceOperation(id)` records a definite **Aborted(fenced)** outcome for an id with no outcome yet, so a delayed copy
can never commit later (OUT-013, used by `QueryOperationAsync(resolve: true)`).

Command catalogue (all deterministic):
`InitVolume`, `OpenSession`, `KeepAlive(session, handleIds, pinIndexUpdates)`, `ExpireSessions(list, leaderTime)`,
`ExpireHandles(list)`, `OpenHandle`, `CloseHandle`, `CreateStaging`, `CloseStaging`, `CommitFile`,
`WriteAll`, `Append`, `Copy`, `Move`, `Delete`, `CreateDirectory`, `SetPolicy`, `SetAcl`, `TxBegin`, `TxCommit`,
`TxAbort`, `TxAcquireReservation`, `CreateSnapshot`, `DeleteSnapshot`, `SetSnapshotRetention`, `AcquirePin`,
`ReleasePin`, `FenceOperation`, `ReclaimDetached`, `PruneDerefLog`, `RecordBackupProgress`, `AbandonTransferLineage`,
`RotateDataKey`, `MigratePolicy`.

### 5.3 Opens, sharing, leases, fencing (FSD §6.1, §7)

* `OpenHandle` performs, in one entry: path resolution, permission check, `FileMode` semantics (create / truncate),
  sharing check against **all** active handles on the inode in **both directions** (LOCK-002), writer exclusivity
  (one writable handle or tx reservation per inode — LOCK-01), and allocates the handle with a lease and a pin.
  Writable handles requesting `FileShare.Write` are rejected; `FileShare.Inheritable` and unknown bits are rejected.
* Writable opens increment the inode's `WriterGeneration` and bind it to the handle.
* Every publication (`CommitFile`, `Append` via handle, `SetLength`, tx commit touching the inode) carries
  `(HandleId, WriterGeneration)` and is rejected unless the handle is active and the generation equals the inode's
  current generation (LOCK-04). A rejected publication → `LeaseLost` (the SDK faults the stream).
* Leases: the client renews with `KeepAlive` every `LeaseDuration/3`. The **leader** tracks renewals with its
  monotonic clock and proposes `ExpireHandles` after `LeaseDuration` without renewal. A newly elected leader treats
  every lease as renewed at its election (conservative). Expiry releases sharing reservations and writer
  authority; an expired handle's further operations fail with `LeaseLost`/`PinExpired`. Client clocks are never
  authoritative (LOCK-014).
* Delete/rename-over of an inode with open handles requires all of them to have `FileShare.Delete`; the inode is
  then detached (no path) and remains readable/writable through existing handles; writes never recreate the path;
  the inode is reclaimed (`ReclaimDetached`) after the last handle closes or expires (NS-06, OPEN-017).

### 5.4 Publication (FSD §10.3)

Client-side `DfsStream` keeps a private overlay. Publishing a file revision:

1. Client ensures a staging session (`CreateStaging`, committed) and receives a **staging capability**
   (HMAC-SHA256 over `{cluster, volume, epoch, stagingId, principal, expiry}` with the cluster key).
2. Client uploads each new object to its placement targets (§6) with the capability; each storage node verifies the
   capability, writes the object durably (flush barrier) and returns a signed **receipt**.
3. Client sends `CommitFile { handle, generation, baseRevision, newLength, changes[], receipts[], stagingId,
   snapshotName? }`. The leader validates receipts (MAC, object id + checksum + length + encoding binding, node
   incarnation current, placement epoch valid, distinct hosts/zones meet `DurableCopiesBeforeAck` /
   `DurableZonesBeforeAck`) **before proposing**. Insufficient receipts → `PlacementUnavailable`; nothing proposed.
4. The entry is committed by majority and applied: extent map updated (only changed ranges; untouched extents keep
   their objects — AT-07), length set, revision = index, objects referenced. Optional commit-and-snapshot creates
   the snapshot in the same entry (FSD §11.2).
5. Reply with `CommitToken`. If the reply is lost, the outcome is recoverable via the operation id.

Inline data: if the full file length ≤ `InlineThreshold` **and** the metadata group's guaranteed durability
(majority size, minimum distinct zones of any majority) satisfies the file's ack policy, the bytes are stored inline
in the inode (no objects). Otherwise extents are used (LAY-002).

Staging sessions are closed when their handle closes/expires, their transaction ends, or their session expires.
Commits referencing a closed staging session are rejected — a closed session is the authoritative "no further
publication" decision that lets GC reclaim its unreferenced objects (§8).

### 5.5 Reads

* **Live read** (`ReadRange(handle, offset, length)`): served by the leader after a **ReadIndex** round; returns the
  current revision, length and the extent refs (or inline bytes) covering the range. The client then fetches
  object ranges from storage nodes and verifies checksums. One read call uses exactly one revision (STR-014).
* **Pinned read**: the handle's pinned inode record is used for its lifetime; no ReadIndex needed.
* **Transaction read**: served from the transaction's retained root (§5.6).
* **Snapshot read**: served from the snapshot root under a snapshot-reader pin.
* `Exists`/`GetInfo`/enumeration are ReadIndex reads. A minority/stale leader can never answer a live read
  (it cannot complete ReadIndex — OFF-014, CON-008).

### 5.6 Transactions (FSD §8)

* `TxBegin` (committed entry) records the transaction, its read index R, a pin, and a staging session. Every replica
  retains the `VolumeState` root at R in memory (derived state) while the tx is active. After a restart/checkpoint
  restore the root is unavailable → the leader aborts such transactions (`TransactionAborted`, retryable).
* Transaction operations (`TxOp`) are sent to the leader, which applies them to a **private working root** (copy of R
  plus the tx's own changes) kept in leader memory. Reads see own writes. The leader records the read set: items
  `(inode, revision)`, absent names `(dirInode, name)` and enumeration predicates `(dirInode, dirRevision)`.
  Writer reservations required by tx writes are taken with `TxAcquireReservation` entries (conflict → immediate
  `SharingViolation`, never waiting — LOCK-019). Leader change → tx aborted.
* `TxCommit` entry contains the read set and the ordered write ops. Apply validates each read item against the current
  state (revision unchanged since R, name still absent, directory revision unchanged); any change →
  `TransactionConflict` (no effect). Otherwise all write ops apply atomically at that index. This yields **strict
  serializability**: the serialization point is the commit index, the log is a total order, and replies follow apply.
* Ordinary (non-tx) mutations bump the same revisions, so they participate in conflict detection (TX-011).
* Limits (duration, touched inodes, descriptor bytes, staged bytes, pins) are checked before proposing and again at
  apply; exceeding → `LimitExceeded`, no partial effect.
* Dispose without commit → `TxAbort`. An unknown commit outcome is resolved by transaction id; abort never reverses a
  committed decision.

### 5.7 Snapshots (FSD §11)

`CreateSnapshot` retains the current root (structural sharing — O(1) capture, no payload copy) at its entry index.
Commit-and-snapshot variants put both in one entry. `MinimumCommit` → leader waits until applied ≥ token (same
cluster/volume/epoch, else rejected) before proposing. Default label `FilesystemConsistent`; `ApplicationConsistent`
only when every declared adapter's prepare succeeded (adapters run client-side: prepare → snapshot → resume; abort
path always resumes; resume failure is reported but the snapshot remains). Snapshot deletion removes the retention
reference only; GC decides reclamation. Restore creates a **new volume** (new VolumeId, epoch 1) whose initial state
is the snapshot root with objects copied (server-side object copy to new ids owned by the new volume).

Checkpoint serialization stores each distinct inode version `(InodeId, Revision)` once, then the current root and each
snapshot root as id→revision lists.

---

## 6. Data path, placement, receipts

* **Object store** (`Storage.ObjectStore`): packed append-only segment files. Record =
  `{magic, version, ObjectId, VolumeId, StagingId, plaintextLength, storedLength, encoding, keyVersion,
  checksum(xxHash128 of plaintext), headerCrc32C}` + payload (+ GCM tags per 64 KiB unit when encrypted). Index is
  in memory and rebuilt by scanning segments at startup (self-contained, DSK-018). Deletes write tombstones.
  Compaction copies live records to a new segment, flushes, then deletes the old segment (crash-safe either way).
* Object size: ≤ `MaxObjectSize` (default 4 MiB). The client chunks writes into logical update units of
  `ChunkSize` (default 1 MiB) — independent of the stream buffer size and of segment size (LAY-011).
* **Placement**: deterministic rendezvous hashing over eligible storage nodes (active, not draining, not full,
  allowed domains) using `(ObjectId, placementEpoch)`. Target set selection is greedy in HRW order maximising distinct
  zones up to `TargetDistinctZones`, then distinct hosts, up to `TargetCopies`. If a target is unavailable the
  uploader continues down the HRW order (fallback) and records actual holders as `LocationsHint`; repair later moves
  copies to the deterministic targets. Ties are broken by NodeId ordinal (REP-008).
* **Receipt**: `{ObjectId, Checksum, Length, Encoding, NodeId, Incarnation, PlacementEpoch, DurableAtNodeTime,
  Mac}` where `Mac = HMAC-SHA256(clusterReceiptKey, all fields)`. Receipts are only issued after the object's record
  and its segment flush completed (`SafetyMutations.AckBeforeFlush` exists only for negative tests).
* **Reads** of objects go to `LocationsHint` then HRW targets then a `LocateObject` broadcast. Every returned range is
  verified against the object checksum (per-unit checksums allow range verification); mismatch → try next replica →
  `DfsIntegrityException` if none. Never zero-fill missing data.
* **Inventory/anti-entropy**: each storage node periodically reports per-volume inventories to the volume leader,
  which keeps an object-location map, computes replica/domain deficits, and schedules repair: the target node pulls
  the object from a verified holder, verifies, persists, then reports. Excess copies are deleted only after targets
  are verified (REP-016, RCV-06). Repair is rate-limited (bytes/s, concurrency).
* **Scrub** re-reads stored objects at a bounded rate; corrupt records are quarantined and reported (DSK-016).

---

## 7. Sessions, idempotency, outcomes (FSD §13)

* The SDK opens a session per volume lazily (`OpenSession` entry) and allocates sequence numbers from it. Each
  mutating request carries its `DfsOperationId` **from the first transmission** (OUT-001). Applications can reserve
  ids (`DfsVolume.NewOperationId()`), persist them, and pass them via options; a different client process can retry
  or query them later under the same principal.
* Retries reuse the id. The server answers retries from the outcome record; an in-flight duplicate on the leader is
  coalesced with the original proposal (OUT-005).
* `QueryOperationAsync(id, resolve)`: `Committed(token, result)`, `Aborted`, `Pending` (known in flight),
  `NotFound` (not seen; only with `resolve:false`), `Expired`. `resolve:true` proposes `FenceOperation` so the answer
  becomes definite (Committed or Aborted).
* Unknown outcomes surface as `DfsCommitOutcomeUnknownException` carrying the id (and tx id). Cancellation surfaces as
  `OperationCanceledException` **only** when the SDK knows nothing was transmitted (or the server confirmed a fenced
  abort).
* Session expiry (`ExpireSessions`) releases handles, closes staging sessions, aborts active transactions, and makes
  every seq of that session expired after the outcome retention period.

---

## 8. Garbage collection (FSD §14)

Tracing GC with a dereference log:

* Reachable set `R(G)` at applied index G = objects referenced by the current root, all snapshot roots, retained
  transaction roots, and pinned inode records.
* Watermark `W = min(createdIndex/pinIndex over active pins, tx read indexes, transfer pins)`; `DerefLog` entries
  with index ≥ W are protected.
* A storage node's inventory item `(objectId, stagingId)` is **deletable** iff: not in `R(G)`, not protected by
  `DerefLog`, and its staging session is `Closed` (or fully retired) at G. Open staging sessions protect their
  objects regardless of age (GC-005: time alone never authorises deletion of possibly-committed data).
* Once unreachable and with a closed staging session, an object can never become reachable again, so a decision taken
  at any committed G is safe even if leadership changed afterwards.
* `PruneDerefLog` entries drop entries below W. `ReclaimDetached` removes detached inodes without handles.
* Capacity: the object store reserves space for journals/checkpoints/compaction/repair (`ReservedBytes`); admission
  rejects new uploads with `CapacityExhausted` before touching the reserve (GC-016/017, DSK-005).

---

## 9. Client SDK internals

### 9.1 Routing and connections
`DfsClient.ConnectAsync` contacts seeds in order (skipping unreachable ones), performs the handshake (cluster id
check against `ExpectedClusterId` if set; protocol version negotiation; authentication), fetches the cluster map, and
then routes: metadata to the volume leader (leader hints from `NotLeader` errors; refresh map on epoch change), data
to storage nodes. Connections are pooled per endpoint and reused; dead pooled connections are detected and replaced.

### 9.2 Retries
Only SDK-internal idempotent operations are retried automatically (same operation id). Retries use bounded
exponential backoff with jitter (seeded `IRandomSource`) and respect the operation deadline. Application
transaction bodies are never re-executed.

### 9.3 DfsStream
* Overlay = sorted set of staged ranges (each either buffered bytes or an already-uploaded staged object range) +
  staged length + truncation watermark. Reads on a writable handle merge overlay with the base revision.
* Memory bound: when buffered bytes exceed `BufferSize`, complete chunks are uploaded under the staging session (still
  unpublished — PUB-002), and the remaining buffered ranges too when they alone still exceed `BufferSize` (FSD §15:
  memory bounded by the configured buffer). Readable writers read uploaded ranges back with their staging capability.
* `Flush`/`FlushAsync` publish all pending changes as one revision (`CommitFile`). `WriteThrough` publishes per call.
* Overlap detection: each public operation enters an `OperationGate`; overlapping calls throw
  `InvalidOperationException` (STR-019).
* Write buffers are copied before `Write` returns (STR-027). A failed write faults the stream (PUB-022).
* Lease loss or unknown publication outcome faults the stream; subsequent writes throw the fault (FSD §6.4).
* Finalizer never commits (PUB-020). `Dispose` flushes and surfaces failures, then closes the handle.
* Append handles: `CanSeek == false`; writes go to the staged logical end.

### 9.4 Helpers
`WriteAll*` = server-side atomic replacement preserving inode identity (`WriteAll` command), via a short implicit
writer reservation; data uploaded first, then one commit. `Copy` = metadata-only copy of the pinned source revision's
extent map (or inline bytes) into the destination in one command (objects are shared). `Append*` = upload suffix, then
`Append` command (one operation id; atomic, idempotent). `ReadAll*`/`ReadLines` pin one revision for the helper
lifetime (open a `VersionPinned` handle internally) and enforce `MaxMaterializeBytes`.

### 9.5 Transactions
`DfsTransaction` owns a staging session and a tx-bound `DfsFileClient`/`DfsDirectoryClient`. Bound streams stage
data and send `TxOp`s on flush/dispose; their `CommitAsync` throws `InvalidOperationException`. `CommitAsync` waits for
no stream operation to be in progress (overlap → `InvalidOperationException`), flushes bound streams, then sends
`TxCommit`. After commit/abort, bound handles reject further I/O.

### 9.6 Simulation compatibility rules for the SDK
The SDK may use `ConfigureAwait(false)` but must: take all time from `DfsClientOptions.TimeProvider`, all randomness
from an injected `IRandomSource`, all I/O from the injected transport; never use `Task.Run`, `Task.Yield`,
`Thread`, `ThreadPool`, `SemaphoreSlim` (use `Tedd.Tdfs.Client.Internal.AsyncGate`/`AsyncLock`, which completes
waiters synchronously), `Task.Delay` without the injected `TimeProvider`, or `TaskCompletionSource` with
`RunContinuationsAsynchronously` on the request path. Synchronous wrappers (`Open`, `ReadAllBytes`, ...) run the async
implementation with `ConfigureAwait(false)` and block (`GetAwaiter().GetResult()`), never capturing a
synchronization context (SDK-010). When the caller's context asks for wait notifications
(`SynchronizationContext.IsWaitNotificationRequired`) they block through the task's wait handle instead, so the context
sees the wait: the simulation's contexts then keep running every other lane until the wait completes (a synchronous call
such as the `Stream.Length` read by the BCL's `Stream.CopyToAsync` stays deterministic instead of blocking the only
simulation thread).

---

## 10. Transport

### 10.1 Abstraction (`IRpcTransport`)
Message-oriented request/response + one-way messages between endpoints identified by `RpcAddress` (scheme
`quic://host:port` or `mem://name`). Payloads are arbitrary length (fragmented internally). Each connection carries
an authenticated `RpcPeerIdentity` after the application handshake. Inbound requests are delivered to an
`IRpcHandler` registered by the node; delivery is posted to the node loop by `NodeHost`.

### 10.2 Quicly implementation
* One `QuiclyNetworkLoop` thread per transport instance owns a `QuiclyServer` (listener on `quic://ip:port`, port 0
  allowed; bound port read after start) and all outbound `QuiclyClient`s. Other threads enqueue work to it
  (`ConcurrentQueue<Action>`), sends use `PeerOptions.ThreadSafeSend = true` and `SendCopy`.
* Channels: `2 = control` (ReliableOrdered; RPC envelope frames), `3 = data` (ReliableUnordered, `MaxGroups 8`; bulk
  object frames). Our own envelope: `{u8 kind (Request|Response|OneWay|Fragment), u64 correlationId,
  u32 fragmentIndex, u32 fragmentCount, payload}`; frames ≤ 60 KiB; reassembly per correlation id with a size cap.
  Timeouts are ours (TimeProvider), not Quicly's request feature.
* Raise `ServerAdmissionOptions.MaxConnectionsPerAddress` (all localhost peers share 127.0.0.1); share one
  `SlabAllocator` per transport; raise receive/send budgets accordingly.
* TLS: QUIC TLS 1.3 always. Nodes present the cluster certificate (PFX from config, or a generated self-signed one in
  tests); clients validate by SPKI pin(s) from options (`DfsTransportSecurity.PinnedSpkiSha256`) or a callback.
  Application-level authentication (§11) runs on top. LZ4 compression is enabled per channel for metadata frames
  (`MinCompressSize 256`) and configurable for data frames; `DecodedBytesPerSecond` raised accordingly.
* The pump loop polls/flushes continuously while work exists and waits on an event (signalled by sends and by wrapped
  transport sinks) with a ≤ 1 ms timeout otherwise.

### 10.3 In-memory implementation
`InMemoryNetwork` (Transport project): a hub of named endpoints; asynchronous delivery via the destination's
dispatcher; supports link rules (drop/delay/duplicate/cut per direction, black hole) through `ILinkPolicy`. The
deterministic `SimNetwork` (Testing) implements the same `IRpcTransport` contracts on the sim scheduler.

### 10.4 Fault interposition
Both transports consult an `ILinkPolicy` (provided by the harness; production = allow-all) per message:
`Decide(from, to, direction, messageType, correlation) → Deliver | Drop | Delay(t) | Duplicate | Hold`. This
implements partitions, asymmetric cuts, black holes, reply drops, duplication and delayed old replies (NET-*).

---

## 11. Security (FSD §14)

* **Transport**: QUIC TLS 1.3 (confidentiality + integrity). Tampered frames are rejected by AEAD (SEC-011).
* **Node authentication**: shared cluster secret (config) → HKDF-derived keys: `NodeAuthKey`, `ReceiptKey`,
  `CapabilityKey`, `StorageKek`. Handshake: `Hello{clusterId, protocol range, features, role, nodeId, incarnation,
  nonce}` → challenge-response HMAC over both nonces. Wrong cluster/secret/removed node → rejected (SEC-001).
* **Client authentication**: `DfsCredentials(principal, secret)`; principals are stored in control state with a
  salted PBKDF2 hash; handshake challenge-response proves knowledge. Anonymous access is impossible (SEC-002).
* **Authorization**: per-volume grants `DfsPermissions` flags: `Read`, `Write`, `Snapshot`, `BackupAdmin`,
  `BackupExport`, `RetentionAdmin`, `VolumeAdmin`; cluster-level `ClusterAdmin`. Enforced **server-side** on every
  namespace, data (capabilities bind principal+volume+staging), snapshot, backup and admin operation, including at
  commit apply time (permission revoked during staging → rejected, SEC-006). Inode ACLs: owner + optional
  `ReadOnly`/`Private` flags checked in addition to volume grants.
* **Data capabilities**: storage-node object reads require a short-lived read capability (HMAC over
  `{volume, objectId set or handle, principal, expiry}`) issued with read metadata; object ids alone grant nothing
  (SEC-004).
* **At rest**: object payloads encrypted with AES-256-GCM using per-volume data keys (DEK versions); Raft journals,
  checkpoints and backup manifests encrypted with node/backup storage keys derived from the KEK. DEKs are stored in
  control state wrapped by the cluster KEK. Key rotation adds a DEK version; old versions are retained while any
  object uses them. **Recovery key**: `DfsAdminClient.ExportRecoveryKeyAsync` returns the KEK escrow wrapped under an
  admin-supplied passphrase; backup nodes hold their own KEK (config) and escrow. Missing key material →
  `DfsRecoveryKeyMissingException`, never a "verified" backup (RST-005).
* Diagnostics/audit never contain payload bytes or secrets (SDK-016, SEC-016).

---

## 12. Late-sync backup (FSD §12)

* Registration (`DfsBackupClient.RegisterTargetAsync`) stores target endpoints, schedule, budgets and the export
  credential in volume/control state. Backup nodes are never Raft members (BAK-001) and reject lock/lease RPCs
  (BAK-002, LOCK-020).
* `BackupExporter` runs on the volume leader per target: pick/create the export snapshot, acquire a **transfer pin**
  (logged, with age/capacity budget), `BeginTransfer` (lineage id, epoch, snapshot id, boundary), receive the
  target's inventory hint, stream missing objects (verified on read and on receipt), send the manifest (complete
  namespace + extent maps + policies + adapter metadata, encrypted), then `Publish`. The target verifies the full
  dependency closure **itself** (never trusting inventory hints — BAK-007), persists manifest and objects durably,
  then atomically replaces its published pointer (temp + flush + rename + dir flush) and only then acknowledges
  (`BackupAck{snapshot, coveredIndex, retentionExpiry}`). Status separates *received bytes*, *durable boundary* and
  *published boundary* (BAK-016).
* Exporter records progress via `RecordBackupProgress`; `AwaitBackupAsync(target, token)` returns the first published
  checkpoint with same epoch and covered index ≥ token; `AwaitBackupSnapshotAsync(target, snapshot)` requires that
  exact snapshot to be durably retained. Timeouts never roll back live commits.
* Budgets exceeded while a target is offline → `AbandonTransferLineage` (release pins, mark reseed). The destination
  keeps its last complete snapshots; next sync is a full reseed under a new lineage.
* Backup retention is enforced by the backup node with its own authorization (export credentials cannot delete or
  shorten protected retention — BAK-022).
* Backup nodes serve `DfsClient` connections read-only: `GetVolume(name).Snapshots.ListAsync/OpenAsync` work; every
  mutation is rejected with `DfsReadOnlyException` (BAK-015). Readers pin one published snapshot (BAK-013).
* Restore (`DfsAdminClient.RestoreFromBackupAsync`) pulls a published snapshot from a backup node into a **new**
  volume with a new epoch; old-epoch tokens/sessions/handles are rejected (RST-008). Promotion of a backup is never
  automatic (RST-009).

---

## 13. Fault injection and coverage

* `IFaultInjector.HitAsync(in FaultContext ctx)` is called at every registered stage boundary in client and server
  code (`FaultStage.F00`–`F21`, site names in `FaultSites`). Production uses `NoFaults.Instance` (inlined no-op).
* The harness arms faults by `(stage, site pattern, node, volume, opKind, occurrence, count)` with actions: `Crash`,
  `Pause` (until released), `Throw(exception factory)`, `DropReply`, `Delay`, `DiskFull`, `Custom`.
* Every hit is recorded in the `FaultCoverageRegistry` (site, stage, node, op) so tests can assert the intended point
  was reached (HAR-013) and campaigns can report generated/applicable/reached/passed counts (§8.4 of the test plan).
* `SafetyMutations` (per node options) disable individual safety checks for negative-control tests (MUT-*). They are
  never enabled in production configuration.

---

## 14. Observability and administration

* `System.Diagnostics.Metrics` meter `Tedd.Tdfs` plus `NodeCounters` snapshot (objects/bytes stored and received,
  receipts issued, Raft entries appended/applied, elections, lease expiries, tx conflicts, checksum failures, GC
  deleted objects/bytes, repair bytes, staged/pinned bytes, backup recovery age).
* `DfsAdminClient` exposes cluster/volume/node status (quorum, leader, voters/learners, effective vs target replicas,
  placement violations, repair/GC backlog, backup progress with recovery boundary age), drain/remove, topology
  updates, policy migration, scrub/repair triggers, snapshot/backup management, key rotation, feature activation.
* Logs carry operation id, transaction id, volume epoch and commit/snapshot tokens; never payloads or secrets.

---

## 15. Formats and compatibility

* Wire: `WireWriter`/`WireReader` (little-endian fixed ints, LEB128 varints, length-prefixed UTF-8/bytes with
  limits). Every message body ends with an **extension block** `{count, (fieldId, flags{Required}, bytes)*}`; unknown
  required → `UnsupportedFeature`; unknown optional → ignored (FMT-002/003). Handshake negotiates protocol version
  range and feature bits; features activate cluster-wide only when all members support them (FMT-009).
* Disk: superblock, journal segments, checkpoints, object segments, backup manifests each carry a magic + format
  version; newer unknown versions are refused; irreversible format activations record a minimum version that blocks
  downgrade (FMT-006).

---

## 16. Defaults (production / `TestTimings.Fast`)

| Setting | Production | Tests (Fast) |
|---|---|---|
| Raft tick | 10 ms | 5 ms |
| Heartbeat | 5 ticks | 5 ticks |
| Election timeout | 30–60 ticks | 30–60 ticks |
| Handle lease | 30 s | 2 s |
| Session timeout | 5 min | 10 s |
| Outcome retention per session | 4096 ops / 1 h | 256 ops / 30 s |
| Inline threshold | 4 KiB | 4 KiB |
| Chunk (logical update unit) | 1 MiB | 64 KiB |
| Max object size | 4 MiB | 1 MiB |
| Stream buffer | 64 KiB | 64 KiB |
| GC interval | 60 s | 250 ms |
| Repair/inventory interval | 30 s | 250 ms |
| Snapshot (Raft) threshold | 10 000 entries | 200 entries |
| Transfer pin budget | 24 h / 10 GiB | configurable per test |
| Max materialize (ReadAll) | 256 MiB | 4 MiB |

---

## 17. Addendum A — permissions, pools, alternate data streams

Requirements: [spec/fsd-addendum-acl-pools-streams.md](spec/fsd-addendum-acl-pools-streams.md). Signatures: API.md §H.

* **Volumes are namespaces.** Nothing about cross-volume isolation changes; it is tested explicitly (suite POOL).
* **Pools** live in the control state (`Pools: name -> sorted node-id set`); `default` is implicit (all live nodes not
  excluded). Volume records and named policies carry a pool name. `Placement.SelectTargets/FallbackOrder` receive only
  the pool's eligible nodes (the caller filters), so fallback can never leave the pool. Pool membership changes bump the
  placement epoch; the repair coordinator treats out-of-pool holders like draining holders (copy-before-delete). The
  membership manager chooses volume voters from pool nodes.
* **Security descriptors** are part of `InodeRecord` (owner + explicit ACEs + protected flag). Effective ACEs are computed
  dynamically by `DfsAccessEvaluator.EffectiveRules` from the ancestor chain resolved during path lookup (nearest
  first). Groups and principals are in the control state; the deciding leader resolves the caller's identity set
  (`user:x`, its groups, `Everyone`, `AuthenticatedUsers`, `Administrators` if cluster admin) and places it in the
  command's `OperationHeader.Identities`; apply re-checks ACLs deterministically with that set (ACL-08). Volume grants
  stay as the coarse gate. Handles keep the access granted at open. Snapshot inode records keep their descriptors.
  An ACL change bumps the inode revision (transaction conflict detection, ACL-07).
* **Streams**: `InodeRecord.Streams: ImmutableSortedDictionary<string, StreamRecord>` where `""` is the default stream;
  `StreamRecord {Length, Inline | Extents, Revision, WriterGeneration}`. Handles, sharing, writer reservations and
  fencing are keyed by `(InodeId, StreamName)`; delete/rename sharing checks consider handles of all streams of the
  inode. Copy copies every stream's extent map (objects shared); delete drops all streams (their objects enter the
  dereference log); GC/backup/snapshots traverse all streams. Protocol requests carry the full normalized path including
  `:stream`; the server splits it with `DfsStreamPath.Split`.
