# Functional Specification: Distributed Filesystem with a .NET Client Library

**Version:** 0.2  
**Status:** Proposed specification; not an implementation or validated performance claim  
**Date:** 2026-10-12  
**Working namespace:** `Dfs` — provisional names throughout
**Companion:** [Verification specification and test catalog](distributed-filesystem-test-plan.md)

## 1. Purpose and scope

Provide a self-contained distributed filesystem accessed directly through a .NET client library. Applications use familiar file operations and ordinary `System.IO.Stream` consumers without mounting a filesystem.

The system SHALL provide configurable replicated storage across failure domains, strongly consistent committed state, file-level writer coordination, efficient small-file and partial-file operations, explicit ACID transactions, immutable snapshots, and independent asynchronous read-only backup hosts.

**Core release:** The .NET SDK and server, volume-scoped transactions and snapshots, zone-aware replication, repair, and late-sync backup replication.

**Deferred:** FUSE, SMB/NFS, kernel drivers, native OS file handles, memory mapping, hard links, symbolic links, concurrent range writers, erasure coding, and transparent metadata sharding within one volume. No external database, coordination service, or mount is required.

The SDK is conceptually similar to `System.IO.File`; it is not a binary replacement. Code accepting `Stream` can use it. Code requiring `FileStream`, `SafeFileHandle`, or an OS pathname cannot do so without an adapter. The standard `Stream` abstraction supplies the interoperability boundary.[^1]

### 1.1 Normative language and assumptions

SHALL denotes a required behavior. SHOULD denotes a recommendation permitting a documented exception. MAY denotes an optional feature.

Assume one administrative trust domain, authenticated clients, unreliable networks, crash/restart failures, and detectable storage corruption. Byzantine consensus and malicious storage-node tolerance are outside scope.

Proposed SDK target: `net10.0`, tested on Windows, Linux, and macOS. Initial server target: Linux. These are project choices, not claims that software already exists.

## 2. Principal decisions

| ID | Decision |
|---|---|
| DEC-01 | Use an instance-based SDK; no process-global filesystem or working directory. |
| DEC-02 | Return `DfsStream : Stream`, with genuine asynchronous I/O and synchronous compatibility methods. |
| DEC-03 | Store immutable data extents; publish changes through transactional metadata. |
| DEC-04 | Separate target copies, commit acknowledgements, metadata voters, and backup copies. |
| DEC-05 | Distribute authority across volume replication groups; do not require agreement from every node. |
| DEC-06 | Allow one active writer per inode initially, with fencing and compatible readers. |
| DEC-07 | Default stream writes remain private until explicit flush/commit or successful disposal. |
| DEC-08 | Provide strictly serializable transactions and atomic snapshots within one volume. |
| DEC-09 | Late-sync hosts publish complete historical snapshots and never participate in live locking or elections. |
| DEC-10 | Never silently weaken consistency, durability, or placement requirements during failure. |

## 3. Terminology and architecture

A **volume** is a namespace, authorization, transaction, and snapshot boundary. An **inode** is a stable file or directory identity independent of its path. An **extent** is immutable data covering a logical file range. A **revision** identifies a committed file version.

A **commit token** identifies an authoritative volume commit. A **snapshot** preserves a complete volume version. A **late-sync target** stores and serves independently retained historical snapshots.

### 3.1 Responsibility boundaries

| Component | Responsibilities |
|---|---|
| Client SDK | API, authentication, routing, buffering, stream state, immutable-data cache, retries. |
| Namespace/transaction service | Directories, inodes, permissions, share reservations, writer fencing, atomic commits. |
| File-layout service | Inline data, sparse ranges, extent trees, staged versions, snapshot references. |
| Distributed extent store | Durable placement, replication, integrity verification, reads and repairs. |
| Local storage engine | Journals, packed records, indexes, flushes, crash recovery, compaction. |
| Cluster control | Membership, topology, policy versions, placement transitions, protocol compatibility. |
| Maintenance/backup workers | Scrubbing, rebalancing, garbage collection, snapshot transfer and retention. |

These are logical modules. A server MAY host several or all roles. Bulk file data SHALL NOT pass through a metadata leader merely to reach storage nodes.

### 3.2 Decentralization and initial scalability boundary

No individual machine is permanently authoritative. Each volume's initial metadata implementation SHALL use one replicated consensus group with an elected leader. Different volumes SHALL distribute their groups and leadership across the cluster.

A small replicated control group SHALL maintain cluster configuration. Established operations SHOULD continue during control-group unavailability where their existing routing and policy epochs remain valid; membership and policy changes SHALL wait.

Consensus requires a majority of the relevant voters, not a majority of all storage nodes. Raft is the proposed protocol family.[^4]

**Initial limitation:** one volume's metadata throughput is bounded by one consensus group's processing capacity. Adding volumes distributes that load. Transparent sharding inside a volume is deferred because it requires cross-shard atomic commit and coherent snapshot protocols, not merely additional metadata servers.

All authoritative metadata, journals, policies, and transaction records SHALL use cluster-managed persistence. Bootstrap identities, local superblocks, and trust material SHALL be discoverable without calling the public filesystem API. Internal metadata SHALL NOT depend on opening files through its own public namespace.

## 4. Volume and pathname contract

**NS-01.** Paths SHALL be absolute and volume-relative, for example `/documents/report.json`. `/` is the only separator. Names are case-sensitive, compared ordinally, and normalized to Unicode NFC by the server and SDK.

**NS-02.** Reject NUL, backslash, empty components, `.` and `..` components, malformed Unicode, and attempts to address another volume through a path. `/` is valid; other trailing slashes are rejected. Different input names that normalize identically identify the same entry.

**NS-03.** Core-release limits SHALL be 255 UTF-8 bytes per component and 4,096 UTF-8 bytes per complete normalized path. File offsets and lengths use nonnegative signed 64-bit values; the server SHALL advertise its supported maximum file length and reject excess before publication.

**NS-04.** File metadata SHALL include inode ID, revision, length, creation/modification timestamps, authorization metadata, effective storage policy, and sparse/inline layout indicators. Wall-clock timestamps SHALL NOT determine commit order.

**NS-05.** Rename/move within a volume SHALL be atomic. Cross-volume move SHALL fail explicitly; callers may perform a separately identified copy-and-delete workflow with no atomicity claim.

**NS-06.** An open handle identifies an inode, not a pathname. Rename SHALL NOT retarget that handle. Delete/overwrite SHALL respect share reservations. Where deletion is allowed, existing handles remain attached to the old inode until closed or expired; writes through them SHALL NOT recreate its deleted pathname.

**NS-07.** Directory enumeration SHALL use a pinned namespace version and continuation token. Expired continuations SHALL fail rather than silently restart against a different version. Recursive deletion SHALL atomically detach the subtree; physical reclamation may continue afterward.

## 5. .NET client API

### 5.1 Object model

```text
DfsClient
  GetVolume(name) -> DfsVolume
    Files        -> DfsFileClient
    Directories  -> DfsDirectoryClient
    Transactions -> DfsTransactionClient
    Snapshots    -> DfsSnapshotClient
    Backups      -> DfsBackupClient
```

`DfsClient` SHALL be reusable and thread-safe, own connection pools, accept multiple seed endpoints, verify cluster identity, discover routing changes, and implement `IAsyncDisposable` and `IDisposable`.

Client disposal SHALL reject new work and complete or cancel in-flight work within the configured shutdown deadline while preserving uncertain outcome identities. It SHALL NOT implicitly commit transactions or substitute for disposing/flushing writable streams; applications SHALL close those streams before their client.

Volume facades SHALL be thread-safe. Individual streams SHALL not support overlapping operations; they SHALL reject overlap with `InvalidOperationException` rather than race their position or buffers.

### 5.2 Required operation families

| Surface | Required operations |
|---|---|
| Files | `Open`, `OpenRead`, `OpenWrite`, `Create`; read/write all bytes, text, and lines; streamed line reading; append; copy; move; delete; existence; metadata retrieval. |
| Directories | Create recursively, existence, metadata retrieval, move, delete, version-pinned enumeration. |
| Transactions | Begin, obtain transaction-bound file/directory facades, commit, abort, query outcome. |
| Snapshots | Create, list, open read-only view, delete subject to retention, restore into a new volume. |
| Backups | Register target, configure schedule/retention, request synchronization, inspect progress, await a recovery boundary. |

Network-bound operations SHALL provide `Async` variants accepting a final optional `CancellationToken`. Async enumeration SHALL use `IAsyncEnumerable<T>`. Synchronous file operations SHALL also be available, except administrative APIs may be async-only.

Whole-file read helpers and streamed line enumeration SHALL pin one selected revision for the helper/enumerator lifetime. Text helpers SHALL default to UTF-8 without emitting a BOM, permit an explicit `Encoding`, and recognize a UTF-8 BOM when reading. Convenience methods that materialize complete contents SHALL enforce a configurable allocation limit; larger files remain available through streams.

### 5.3 Illustrative usage

The following describes the proposed API, not an available NuGet package. `options` and `ct` are supplied by the application.

```csharp
await using var client = await DfsClient.ConnectAsync(options, ct);
var volume = client.GetVolume("application");

await volume.Directories.CreateDirectoryAsync("/documents", ct);
await volume.Files.WriteAllTextAsync(
    "/documents/settings.json", "{\"enabled\":true}", ct);

await using Stream input =
    await volume.Files.OpenReadAsync("/documents/settings.json", ct);

using var reader = new StreamReader(input);
string text = await reader.ReadToEndAsync(ct);
```

A configurable open SHALL accept the following shape:

```csharp
public sealed record DfsOpenOptions
{
    public FileMode Mode { get; init; } = FileMode.Open;
    public FileAccess Access { get; init; } = FileAccess.Read;
    public FileShare Share { get; init; } = FileShare.Read;
    public DfsWriteMode WriteMode { get; init; } = DfsWriteMode.OnFlush;
    public DfsReadView ReadView { get; init; } = DfsReadView.Live;
    public int BufferSize { get; init; } = 64 * 1024;
}

public enum DfsWriteMode { OnFlush, WriteThrough }
public enum DfsReadView { Live, VersionPinned }
```

The buffer default is provisional and SHALL be benchmarked. It does not determine the replication unit. `VersionPinned` SHALL be restricted to read-only opens outside transactions; transaction-bound reads use the transaction's view.

### 5.4 Deliberate differences from `System.IO.File`

`ExistsAsync` SHALL return false for an absent entry, but SHALL propagate authorization, connectivity, and integrity failures rather than presenting them as absence.

`WriteAll*` and `Copy` SHALL publish a complete destination update atomically and return only after durable commit. Updating an existing destination through these operations SHALL preserve its inode identity. Failure before commit SHALL leave its old contents intact. `Copy` SHALL pin one source revision for its entire transfer and SHALL reject copying an inode onto itself.

`OpenWrite` SHALL open or create at offset zero without implicitly truncating an existing file. `Create`/`Truncate` SHALL truncate when the open succeeds; a later failed write does not restore the old contents. Atomic replacement requires `WriteAll*`, a transaction, or an explicitly staged operation.

Standalone append convenience helpers SHALL append one complete suffix atomically and durably under one operation identity; transaction-bound helpers remain private until transaction commit.

`Delete` of an absent file SHALL succeed. Unsupported OS-specific options SHALL throw `NotSupportedException`, not be ignored.

## 6. Stream contract

`DfsStream` SHALL derive from `Stream`, not `FileStream`. It SHALL implement synchronous array/span and asynchronous array/memory reads and writes, seeking, length, position, flush, disposal, and standard copying behavior. Async methods SHALL perform actual asynchronous I/O rather than wrapping each synchronous operation in `Task.Run`.[^1]

### 6.1 Open behavior

Reuse `FileMode`, `FileAccess`, and `FileShare` where their concepts apply.[^2][^3]

| Mode | Required behavior |
|---|---|
| `CreateNew` | Atomically create; fail when the name already exists. Requires write access. |
| `Create` | Create or truncate to zero at successful open. Requires write access. |
| `Open` | Open an existing file; fail when absent. |
| `OpenOrCreate` | Open existing or atomically create; namespace permission is required when creating. |
| `Truncate` | Require an existing file and write-only access; truncate at successful open. |
| `Append` | Open/create for write-only append. Each write targets the handle's logical end. |

For append streams, seeking and `SetLength` SHALL be unsupported and `CanSeek` SHALL be false. This is an intentional simplification from `FileStream`'s append seeking rules. Concurrent append writers are not supported initially.

Outside a transaction, creating/truncating and acquiring the handle reservation SHALL form one committed open operation. Inside a transaction, creation/truncation remains private until transaction commit.

### 6.2 Read, seek, and length

A nonempty read SHALL return the number of bytes obtained, possibly fewer than requested; zero means EOF at the selected revision. A zero-length read returns zero. It SHALL NOT return zero merely because the cluster is temporarily unavailable.

Each live read SHALL select a linearizable committed inode revision and retain it for the entire read call. Different read calls may observe newer revisions. A pinned read SHALL use one immutable revision for the handle's lifetime. Only the owning writer/transaction sees its private staged changes.

Seek beyond EOF SHALL be allowed on ordinary seekable streams and SHALL NOT extend the file until a write or `SetLength`. Reading holes and newly extended unwritten ranges SHALL return zeroes. Truncating and later extending SHALL NOT reveal discarded data. `SetLength` SHALL NOT implicitly change `Position`, including when shrinking below it. A failed read that returns no successful count SHALL leave `Position` unchanged; caller-buffer contents after an exception are not a valid successful read result.

`Position` SHALL reflect the handle's local position. `Length` SHALL reflect its declared live, pinned, or staged view and may require network I/O. An asynchronous metadata method SHALL be available for callers avoiding synchronous property I/O.

### 6.3 Publication and durability boundaries

| Operation | Outside an explicit transaction |
|---|---|
| `WriteAsync`, `OnFlush` | Accept bytes into the private staged version; no durability or external visibility guarantee yet. |
| Internal staging | May upload data to bound memory, but SHALL NOT publish it. |
| `Flush` / `FlushAsync` | Atomically publish all pending changes for this file and meet its live durability policy. |
| `WriteAsync`, `WriteThrough` | Atomically publish that call's changes and meet live durability before returning. |
| `SetLength`, `OnFlush` | Change staged length; publish at the next flush. |
| `SetLength`, `WriteThrough` | Commit the length change before returning. |
| `CommitAsync` extension | Same live commit as flush, returning a `CommitToken`. |
| Successful `Dispose` / `DisposeAsync` | Flush pending changes, then release reservations and resources. |

A flush SHALL publish one complete file revision, even when its data spans many extents. Publication size limits SHALL be advertised; exceeding them SHALL fail without publishing a partial revision. Staged extent trees SHALL permit large transfers without holding the complete file or mapping in client memory.

`FlushAsync` is deliberately a **distributed durability boundary**, not merely a network-buffer drain. It does not wait for late-sync backups.

The writer SHALL read its own staged modifications. Other clients SHALL see the preceding committed revision until publication. Streams wrapped by `StreamWriter`, compression, or other buffering layers require those outer layers to be flushed first.

### 6.4 Disposal, abort, and faults

Disposal SHALL surface flush failures. It SHALL NOT silently discard acknowledged-to-buffer writes while reporting successful disposal. A finalizer SHALL NOT attempt a network commit.

After a successful write returns, the SDK SHALL own an independent accepted representation of the bytes; later caller-buffer reuse SHALL NOT change staged contents. A failed partially staged write SHALL either restore the preceding private state or fault the stream so a later flush/dispose cannot publish a partial failed call.

A writable stream SHALL expose `AbortAsync` to discard uncommitted staged changes and release its reservation. Abort does not undo prior flushes or a create/truncate already committed at open.

Lease loss or an indeterminate publication outcome SHALL fault the stream. Further writes SHALL fail until the application resolves the outcome and opens a new handle. The SDK SHALL NOT silently reacquire authority and publish a stale buffer.

Cancellation before publication leaves committed state unchanged. Cancellation or disconnection after a commit request may leave its outcome unknown; that is not proof of rollback.[^5]

`Stream.CopyTo` and `CopyToAsync` are compositions of stream operations, not implicit atomic file replacements; a failed copy does not undo earlier successful writes or flushes. Atomic copying is provided by the file helper or an explicit transaction.

Read-only flush is a no-op. Transaction-bound stream behavior is defined separately below. Operation deadlines come from SDK options; `CanTimeout` is false and `ReadTimeout`/`WriteTimeout` are unsupported.

## 7. Writer coordination and sharing

**LOCK-01.** Core release SHALL allow at most one active writable handle or transaction writer reservation per inode. Many compatible readers may coexist.

**LOCK-02.** Share compatibility SHALL be checked in both directions across all active handles. `FileShare.None`, `Read`, `Write`, `ReadWrite`, and `Delete` combinations SHALL be supported; `Inheritable` SHALL be rejected.

A read-only handle may grant `FileShare.Write` so that a writer can coexist. A writable handle SHALL reject options granting `FileShare.Write` in the core release, making unsupported multiple-writer sharing explicit. For simultaneous reads and writes, a typical pairing is a writer with `Share.Read` and a reader with `Share.ReadWrite`.

**LOCK-03.** Reservations SHALL be cluster-enforced renewable leases, persisted or recoverably fenced by the metadata group. Conflicting opens SHALL fail with a typed sharing/writer conflict; automatic indefinite waiting is not part of the core API.

**LOCK-04.** Every writer-authorized publication SHALL carry a monotonically advancing fencing generation. The authority committing the mutation SHALL reject older generations. Client clocks or client-side expiry checks SHALL NOT be the sole correctness mechanism.

**LOCK-05.** A lost session SHALL release reservations only through a safe server-authoritative expiry/recovery process. Read-handle pins also require bounded lifetimes. An expired handle SHALL fail rather than return data from reclaimed extents.

**LOCK-06.** All mutation paths, including convenience methods, transactions, restoration, truncation, and policy changes, SHALL obey the same authority rules. A lower-level storage endpoint SHALL NOT bypass namespace authorization.

Snapshot and backup reads SHALL take only local version pins, not writer locks against the live filesystem.

## 8. ACID transactions

### 8.1 Scope and isolation

Transactions SHALL atomically group file data, file creation/deletion, directory operations, renames, and metadata updates within one volume. Cross-volume transactions and automatic `System.Transactions` enlistment are outside core scope.

Isolation SHALL be **strict serializability**: committed transactions admit a serial order consistent with real-time completion. The implementation SHALL validate read versions, absent-name reads, and directory-enumeration predicates, or provide equivalent protections against lost updates, write skew, and phantoms.

All ordinary committed mutations SHALL participate in the same conflict system. ACID consistency covers filesystem invariants and constraints explicitly checked by the application; the filesystem cannot infer business rules from arbitrary bytes.

### 8.2 Transaction-bound streams

A transaction SHALL expose file/directory facades bound to its view. Reads see its own writes; other clients see none of them before commit.

Stream flush and stream disposal SHALL stage transaction data durably as required for commit, but SHALL NOT publish or commit the transaction. A transaction-bound stream's independent `CommitAsync` SHALL throw `InvalidOperationException`.

`DfsTransaction.CommitAsync` SHALL collect remaining staged data, validate conflicts, establish required data durability, and atomically publish all changes. No stream operation may overlap commit. After commit or abort, bound handles SHALL become unusable except for disposal.

Disposing an uncommitted transaction SHALL request abort, never commit. An unknown commit outcome SHALL be resolved from its durable transaction ID; disposal cannot reverse an already committed decision.

```csharp
await using var tx = await volume.Transactions.BeginAsync(ct);

await tx.Files.WriteAllTextAsync("/state/balance.json", balanceJson, ct);
await tx.Files.WriteAllTextAsync("/state/ledger.json", ledgerJson, ct);

CommitToken committed = await tx.CommitAsync(ct);
```

Within the transaction, the helper methods above stage complete file updates rather than individually committing them.

### 8.3 Limits and recovery

The server SHALL advertise transaction duration, metadata size, touched-inode, staged-byte, and pin limits. Exceeding a limit SHALL never partially publish a transaction. Payloads may be staged outside the consensus log; commit descriptors SHALL remain bounded.

Conflicts SHALL abort with a typed exception. Automatic retries SHALL be limited to SDK-internal idempotent operations; application transaction bodies SHALL NOT be re-executed without explicit application control.

A committed transaction SHALL appear wholly before or wholly after every volume snapshot. Copying changed blocks independently is not a substitute for transaction publication.

## 9. Replication, zones, and selective synchronization

### 9.1 Policy model

Each volume SHALL define default storage policies; an inode MAY select a named stronger or alternative allowed policy. Policies attach to inode identity, not current pathname. Directory policies apply at creation unless migration is explicitly requested.

```yaml
liveData:
  targetCopies: 3
  targetDistinctZones: 3
  durableCopiesBeforeAck: 2
  durableZonesBeforeAck: 2
metadata:
  voters: 3
  placement: one-per-zone
  commit: majority
backup:
  mode: asynchronous-snapshots
  targets: [archive-host]
```

This example promises at least two durable live data copies in two zones when a write succeeds, with a target of three. It does **not** promise that all three copies already exist at acknowledgement.

To require all three before success, set the acknowledgement counts to three. With only three eligible zones, one unavailable zone then prevents that write policy from succeeding.

Metadata voters, extent replicas, and asynchronous backups SHALL be counted separately. A witness without file contents SHALL NOT count as a data copy. Data and metadata policies together determine the actual recovery guarantee.

### 9.2 Placement and transitions

Topology SHALL support `region / zone / rack / host / device`. The server SHALL reject impossible policies and expose insufficient placement separately from quorum unavailability.

Replica placement SHALL be deterministic from object identity, policy, and a versioned placement map, or provide an equivalent stable directory. Distribution SHALL account for failure domains and eligible capacity, not just replica count.

Commit receipts SHALL identify the object, durable persistence, node/failure domain, and placement epoch. A policy change SHALL NOT be declared effective before its required migration finishes. Existing recoverable copies SHALL not be discarded prematurely during reconfiguration.

### 9.3 Synchronization scopes

| State | Required coordination |
|---|---|
| Membership/policy change | Relevant control consensus group. |
| File publication or transaction | Owning volume metadata group plus required data receipts. |
| Extent contents | Selected storage replicas. |
| Metrics and approximate usage | Asynchronous aggregation. |
| Historical backup | Independent asynchronous transfer. |

An explicit durability barrier MAY require acknowledgements from a named set of storage targets. The set and placement epoch SHALL be fixed for the barrier; “all” SHALL never silently expand with cluster membership or shrink on failure.

Acknowledging storage durability grants neither writer authority nor election rights. Strong reads SHALL validate a current authoritative revision; every cache need not receive every update proactively.

## 10. Data layout and commit invariants

### 10.1 Logical representation

```text
Directory entry -> inode ID
Inode -> attributes, length, revision, inline contents OR extent-tree root
Extent-tree entry -> logical offset, logical length, immutable object ID
Object record -> payload, encoding version, integrity information
```

Object identity SHALL remain independent of its current physical node. Content-addressed deduplication is optional, not a prerequisite.

Small contents SHOULD be inline when the metadata group's durability satisfies the file policy. Otherwise, small records SHOULD be packed into larger local storage segments. Inlining SHALL NOT bypass stricter data-placement requirements.

### 10.2 Partial-file synchronization

Writes SHALL replicate changed extents and changed mapping records rather than entire files. Logical update units, physical packing, compression units, network batches, and consensus groups SHALL be independently tunable.

A small overwrite SHALL NOT require rewriting an unrelated large streaming extent. Adjacent extents SHOULD coalesce where beneficial, and large sequential transfers SHOULD use batching, read-ahead, and pipelining. A file's extent count and metadata overhead SHALL be measured explicitly.

### 10.3 Required publication sequence

1. Validate the handle/transaction, authorization, expected revision, fencing generation, and policy epoch.
2. Stage immutable data and mapping records under a registered, bounded staging session.
3. Obtain durable receipts meeting the effective policy for every newly referenced object, including non-inline mapping objects.
4. Atomically commit the authoritative inode/transaction update and its durable staging-to-live reference transition.
5. Return the commit token; release superseded references only when all retention obligations permit.

A successful commit SHALL never reference data that failed its durability policy. Storage receipts do not by themselves publish state.

A crash before publication leaves the previous state authoritative. A crash after publication but before the reply produces a queryable committed result. Garbage collection SHALL distinguish unresolved staging from authoritatively aborted staging; elapsed time alone is insufficient proof of abandonment.

## 11. Snapshots and application consistency

### 11.1 Filesystem snapshots

The core release SHALL support named, immutable, read-only snapshots of an entire volume. Snapshot creation SHALL capture one authoritative commit boundary and pin every referenced metadata/data object without copying all payloads immediately.

Unflushed client buffers and uncommitted transactions SHALL be excluded. An ordinary snapshot SHALL not wait for every client to flush. A minimum commit token MAY be supplied to require a sufficiently recent boundary.

Deleting a snapshot SHALL remove its retention reference, not data still needed by another snapshot, current state, reader, or transfer. Snapshot restore SHALL initially create a new volume rather than roll a live volume backward in place.

### 11.2 Exact recovery points

A commit token is a position in history, not a promise that the exact version will be retained indefinitely.

The SDK SHALL support committing a transaction or standalone stream together with creating a named snapshot in the same metadata decision. This is the way to preserve an exact committed state for later backup confirmation.

Creating a snapshot *after* a commit with `MinimumCommit = token` may capture later changes. The API SHALL distinguish these two contracts.

### 11.3 Crash-consistent versus application-consistent

A default snapshot SHALL be labelled `FilesystemConsistent`: it contains complete filesystem transactions and can serve as a crash-style recovery image for applications respecting the documented flush order. It does not assert that arbitrary application data is logically consistent.

The snapshot API SHALL support optional application hooks: prepare/checkpoint, establish source snapshot, then resume. A bounded failure/abort path SHALL always release an application pause. Remote transfer SHALL occur after the application resumes.

An `ApplicationConsistent` label SHALL require successful completion of the declared adapters and include their identities and restore requirements. Failed adapters SHALL not silently downgrade the label.

This separation follows the writer/provider distinction in VSS, but implementing a Windows VSS provider is deferred.[^6] A database may recover a filesystem-consistent snapshot using its transaction log when its documented requirements are satisfied; PostgreSQL explicitly documents such a recovery path.[^7]

No database engine is assumed to work merely because it is “ACID.” Native-path databases require an appropriate storage adapter; a `Stream` API alone does not provide one.

## 12. Late-sync read-only backup hosts

### 12.1 Role and isolation

A late-sync host SHALL be a separate replication consumer, not a live consensus member. It SHALL NOT vote, acquire live writer locks, block normal commits, or become writable automatically.

It SHALL receive committed snapshots incrementally, serve read-only snapshot views through the same SDK abstractions, and remain readable without connectivity to the source. Asynchronous snapshot mirroring is an established pattern; CephFS provides a relevant precedent.[^8]

The backup process may write its local storage, but clients SHALL have no mutation access to published snapshots. Hosting it on the same physical server as a live role SHALL NOT make that copy an independent failure domain.

### 12.2 Transfer and publication

```text
Source:      snapshot S100 -> snapshot S101 -> snapshot S102
Destination: S100 readable | S101 receiving | S102 pending
```

The exporter SHALL pin a source snapshot, identify objects absent at the destination, and transfer only necessary metadata and extents. Transfers SHALL be resumable and integrity-checked. Probabilistic inventory shortcuts SHALL never replace final completeness verification.

The receiver SHALL stage data, verify the complete dependency closure, persist the snapshot manifest and required objects, then atomically advance its published pointer. It SHALL NOT expose a partially received directory tree.

Each reader SHALL pin one published snapshot. New synchronization SHALL NOT change that reader's contents. Destination status SHALL distinguish received bytes, durable recovery boundaries, and published readable boundaries.

### 12.3 Backup acknowledgements

The API SHALL provide both contracts:

```text
AwaitBackupAsync(target, commitToken)
    Wait for a complete retained checkpoint from the same volume epoch
    at or beyond the supplied commit boundary.

AwaitBackupSnapshotAsync(target, snapshotId)
    Wait for the specified exact snapshot to be durably retained.
```

The first contract does not preserve an overwritten/deleted earlier version merely because the checkpoint is later. The second does. A result SHALL report the actual snapshot, covered source commit, and retention expiry or hold.

Either barrier may be explicitly required by the application after a live commit. A timeout SHALL NOT roll back that commit. Ordinary flush SHALL not imply backup acknowledgement or zero remote recovery-point loss.

### 12.4 Retention and disconnected operation

Destination retention SHALL be independently enforced. Source credentials permitted to export data SHALL not automatically permit deletion of protected destination history.

Source-side transfer pins SHALL have capacity and age budgets. If an offline destination exceeds them, the system SHALL safely abandon that incremental lineage, preserve its last complete destination snapshot, and mark it for reseeding. Live writes SHALL not become dependent on indefinitely retaining backup history.

Transfers SHALL have bandwidth, I/O, and concurrency limits. A full or corrupt backup host SHALL report failure without forcing live commits to wait.

A recoverable backup SHALL include namespace metadata, mappings, object contents, format manifests, and a documented independently recoverable encryption-key path. It SHALL not depend on the lost source's metadata service to interpret its bytes.

### 12.5 Recovery and promotion

Restore into a new live volume/cluster SHALL be the default recovery procedure. Promotion SHALL require explicit administration, selection of a complete snapshot, a new authority epoch, and prevention of the old authority from accepting writes for the same recovered identity.

A network partition SHALL never trigger automatic backup promotion. Monitoring SHALL show the latest complete recovery boundary, not only connectivity or transfer speed. One backup host itself has no replicated-host availability guarantee.

## 13. Failures, retries, and observable outcomes

Every mutating operation SHALL have a stable operation ID assigned before transmission. Every transaction SHALL have a durable transaction ID. Retries of an accepted ID SHALL preserve its original outcome and SHALL NOT repeat an append or create. IDs SHALL be bound to authenticated principal, volume/epoch, operation type, and request payload identity; reuse with incompatible arguments SHALL be rejected.

Outcome records MAY have bounded retention. IDs outside the retained window SHALL be rejected as expired; they SHALL NOT be replayed as new operations. Applications requiring recovery across SDK-process loss SHALL be able to persist and supply an operation ID. The implementation SHALL retain durable replay-rejection state, for example server-issued sessions with retired sequence windows, rather than relying solely on a bounded cache of random IDs. An authoritative aborted/retired decision SHALL fence delayed requests from later committing. Pending or indeterminate status SHALL NOT be presented as definite abort or ordinary absence.

`CommitToken` SHALL identify cluster, volume, volume epoch, and commit sequence. Tokens from different epochs or volumes SHALL not be treated as globally ordered. Unrelated volume commits need not share a global sequence.

| Condition | Required surface |
|---|---|
| Missing file/directory | Standard `FileNotFoundException` / `DirectoryNotFoundException`. |
| Permission denied | `UnauthorizedAccessException`. |
| Invalid options/path | Appropriate argument exception. |
| Sharing/writer conflict | Typed `IOException` subtype with retryability information. |
| Transaction conflict | `DfsTransactionConflictException`. |
| Lost writer authority | `DfsLeaseLostException`; handle faulted. |
| Quorum/placement unavailable | Distinct typed availability exceptions. |
| Integrity failure | `DfsIntegrityException`; never substituted zeroes or fabricated EOF. |
| Commit may have happened | `DfsCommitOutcomeUnknownException` carrying operation/transaction ID. |
| Request-ID retention expired | Explicit expired-outcome error; no automatic replay. |

Normal cancellation SHALL use `OperationCanceledException` only when the SDK can establish that publication did not occur. Once publication is potentially committed, an unknown-outcome exception SHALL take precedence over presenting cancellation as definite failure.[^5]

Loss of a majority SHALL prevent affected authoritative mutations. A minority SHALL not grant leases or answer new live reads without valid authority. Explicitly pinned immutable snapshots may remain readable where all necessary local data and pins are available.

## 14. Integrity, garbage collection, and security

Checksums SHALL cover stored/transferred contents and relevant identity/encoding metadata. Reads SHALL verify integrity and retry another replica when available. Scrubbing SHALL detect latent corruption; repairs SHALL use verified copies, preserve placement rules, and be rate-limited.

Garbage collection SHALL account for current versions, retained snapshots, active read pins, staged or unresolved transactions, transfers, and reconfiguration. It SHALL use recoverable reference transitions or an equivalent safe tracing protocol. Clock expiry alone SHALL not authorize deletion of potentially committed objects.

The system SHALL reserve capacity for journals, metadata progress, compaction, and repair. Admission control SHALL reject new work before consuming those reserves. Disk-full conditions SHALL never produce false durability acknowledgements.

Transport SHALL authenticate peers and clients and protect confidentiality/integrity. Authorization SHALL be enforced server-side for namespace operations, direct object reads, writes, snapshots, and administration. At-rest encryption and key rotation SHALL be supported with an independent recovery procedure.

Backups SHALL have separate authorization/retention controls. A source administrator SHALL not automatically have permission to erase protected recovery points. Protection against a malicious storage administrator requires an independently enforced storage/credential boundary; software retention flags alone are insufficient.

### 14.1 Offline members and resynchronization

**RCV-01.** Unreachability SHALL NOT implicitly remove a voting member or reduce a quorum denominator. Membership changes require the configured safe consensus transition.

**RCV-02.** A returning member SHALL establish its cluster identity, node incarnation, membership status, and relevant epochs before its state is trusted. Removed members and cloned/replaced disk identities SHALL NOT silently regain authority.

**RCV-03.** A replica with available missing log history SHALL catch up incrementally. When required history has been compacted, it SHALL install a verified complete metadata checkpoint and valid log suffix. Checkpoint installation SHALL be crash-recoverable and SHALL preserve outcomes, request retirement, writer generations, namespace state, snapshots, and protection references.

**RCV-04.** Returning metadata replicas SHALL NOT serve new live reads from stale unapplied state. Existing voter eligibility follows the selected consensus protocol; newly added or empty replacement members SHALL NOT vote before its admission/catch-up rules permit. Data replicas SHALL be counted healthy only for verified objects they actually hold.

**RCV-05.** Old local contents SHALL never override committed namespace history through timestamp comparison or directory merging. Deleted, truncated, renamed, and recreated files SHALL recover according to authoritative inode identities and revisions, without resurrection.

**RCV-06.** Catch-up, repair, and rebalancing SHALL survive interruption and retries while ordinary traffic continues. They SHALL retain valid recoverable copies until their replacements and required transitions are established. Lack of a healthy source SHALL produce explicit unavailability/integrity status, not invented bytes.

**RCV-07.** Repair SHALL restore configured copy/domain targets when legal capacity and healthy sources exist. Until then the cluster SHALL expose the actual deficit and enforce its unchanged commit policy. Merely reconnecting an endpoint SHALL NOT count missing/corrupt contents as a restored replica.

**RCV-08.** Recovery work SHALL be bounded and observable. Eventual progress is required only once sufficient authority, healthy sources, legal capacity, resources, and communication are restored; a permanent loss of every required copy is outside the promised recovery envelope.

## 15. Performance and operational requirements

No throughput or latency guarantee is claimed before benchmarking. The implementation SHALL publish results for small-file create/open/stat/delete, small random reads/writes, sequential transfer, directory enumeration, snapshot creation, backup catch-up, and foreground operation during repair.

Tests SHALL report p50/p95/p99 latency, throughput, useful bytes versus replicated bytes, metadata bytes per file, write/read amplification, CPU, memory, and failure-domain topology. Results SHALL distinguish `OnFlush` buffer acceptance from durable commit latency.

Streaming memory usage SHALL remain bounded by configured buffers and in-flight limits, not total file size. Backpressure SHALL apply before those limits are exceeded. A single hot volume and hot inode SHALL be benchmarked separately from cluster-wide scaling.

Observability SHALL expose quorum state, effective versus target replicas, placement violations, lease failures, transaction conflicts, staged/pinned bytes, repair backlog, garbage-collection progress, checksum failures, and backup recovery age.

Logs/traces SHALL carry operation ID, transaction ID where applicable, volume epoch, and commit/snapshot tokens without recording file contents or secrets by default. Administration SHALL support drain/remove, topology updates, policy migration, scrubbing, snapshot/backup management, and rolling upgrades with version negotiation.

## 16. Acceptance criteria

| ID | Required verification |
|---|---|
| AT-01 | Use returned streams with `StreamReader`, `StreamWriter`, `CopyToAsync`, and a seek-requiring consumer. No mount or local pathname is required. |
| AT-02 | Verify every open mode, share combination, sparse extension/truncation, EOF, offset overflow, cancellation, and disposal contract. |
| AT-03 | With `OnFlush`, another client sees old contents before flush and one complete new revision afterward. Buffered data never leaks through auto-staging. |
| AT-04 | Crash around every persistence/publication boundary; every reported durable commit survives failures within its declared replica policy. |
| AT-05 | Pause a writer, expire/fence it, activate a replacement, then resume the old writer. Its publication is rejected. |
| AT-06 | Partition the cluster; minority replicas do not grant authoritative writes or pretend stale reads are live. |
| AT-07 | Randomly overwrite a small range in a large file; unaffected contents remain identical and replication does not resend the whole file. |
| AT-08 | Run conflicting multi-file transactions and predicate reads; verify strict serializability and all-or-nothing snapshots. |
| AT-09 | Drop a reply after commit and retry with the same ID. Exactly one mutation results; expired IDs cannot be replayed as new. |
| AT-10 | Interrupt backup transfer repeatedly. The last published snapshot remains complete, readable, and unchanged. |
| AT-11 | Confirm the late-sync target never appears in live voter, lease, or ordinary acknowledgement sets. |
| AT-12 | Differentiate a later checkpoint from an exact retained snapshot when a file is overwritten or deleted between them. |
| AT-13 | Keep the backup offline beyond the source pin budget. Live storage remains bounded; the target is explicitly reseeded. |
| AT-14 | Destroy access to all source nodes and restore solely from backup plus documented recovery credentials/keys. |
| AT-15 | Inject corruption, full disks, interrupted compaction, and placement transitions; no silent corruption or premature reclamation occurs. |
| AT-16 | Reject incompatible features/options and impossible durability policies instead of silently approximating them. |

A deterministic fault-injection harness and property-based state-machine tests SHALL accompany the implementation. Release approval SHALL include recovery drills and workload-specific benchmark evidence, not only API unit tests.

## 17. Delivery boundaries and remaining engineering decisions

**Stage A — SDK and semantic reference:** Implement the public contracts, a single-node development backend, stream behavior, transaction semantics, and a deterministic test harness. A single-node backend carries no distributed durability claim.

**Stage B — replicated live storage:** Add consensus-backed volume metadata, zone-aware extent durability, fencing, crash recovery, repair, and safe garbage collection. Exercise the same contract suite against multiple nodes.

**Stage C — complete core release:** Add retained snapshots, exact commit-and-snapshot, independent late-sync hosts, restore verification, security hardening, operations, and performance validation. These backup capabilities are required, not optional roadmap items.

**Later:** Transparent intra-volume metadata sharding, concurrent range writers and offset-oriented APIs, transport/storage optimizations, other-language SDKs, and FUSE/OS adapters. These additions SHALL preserve the core consistency and durability contracts.

Before production implementation, architecture decisions SHALL select the consensus library and membership protocol, durable local engine, wire format, authentication/key management, staging/garbage-collection protocol, capability limits, and measured inline/extent/batching thresholds. These choices must satisfy this specification; the FSD does not constitute a proof of the distributed protocol.

## 18. Verification and assurance requirements

The companion [test specification](distributed-filesystem-test-plan.md) defines named tests, invariant oracles, failure injection, coverage dimensions, and release evidence. It is a specification to implement, not a report that tests have already passed.

**VER-01.** No test count, line/branch coverage score, or finite simulation campaign SHALL be represented as an unconditional 100% correctness guarantee. Claims SHALL identify the requirements, configurations, fault model, and exploration bounds actually covered.

**VER-02.** Verification SHALL combine pure unit tests, deterministic multi-node simulation, real-process/network/storage integration, property-based history checking, bounded formal models of critical protocols, and supported-hardware recovery testing. Distributed failure tests SHALL not be labelled isolated unit tests merely because they run in one process.

**VER-03.** The test harness SHALL control scheduling, clocks, network delivery, persistence outcomes, random choices, and process failures; failing traces SHALL be reproducible. The production state-machine logic SHALL be reused where practical, with independent reference models/checkers rather than mocks that simply return expected results.

**VER-04.** Every public and internal operation SHALL register its reachable network/persistence/publication fault points. Every applicable required operation/stage/fault combination in the declared bounded campaign SHALL have an execution result or a documented exclusion; unvisited injections, silent skips, and incomplete checkers SHALL not count as passes.

**VER-05.** Safety SHALL be checked during failures and recovery, not only after convergence. Liveness assertions SHALL state their eventual-quorum, delivery, capacity, fairness, and healthy-source assumptions explicitly.

**VER-06.** Each FSD requirement SHALL map to test and/or formal-analysis evidence before release. Critical negative controls SHALL demonstrate that the suite detects removed fencing, premature acknowledgement, missing deduplication, stale live reads, unsafe GC, and incomplete backup publication.

**VER-07.** Release evidence SHALL include independently recoverable backup restoration with all source services unavailable, platform/format compatibility results, measured resource envelopes, unresolved defects, model assumptions, and any unverified refinement from model to implementation.

## References

References establish external API concepts and architectural precedents. All SHALL statements and provisional defaults above are requirements proposed for this project, not claims about those systems.

[^1]: Microsoft, *System.IO.Stream*. https://learn.microsoft.com/en-us/dotnet/api/system.io.stream?view=net-10.0
[^2]: Microsoft, *System.IO.FileMode*. https://learn.microsoft.com/en-us/dotnet/api/system.io.filemode?view=net-10.0
[^3]: Microsoft, *System.IO.FileShare*. https://learn.microsoft.com/en-us/dotnet/api/system.io.fileshare?view=net-10.0
[^4]: Ongaro and Ousterhout, *The Raft Consensus Algorithm*. https://raft.github.io/
[^5]: etcd, *API guarantees*. https://etcd.io/docs/v3.6/learning/api_guarantees/
[^6]: Microsoft, *Volume Shadow Copy Service*. https://learn.microsoft.com/en-us/windows-server/storage/file-server/volume-shadow-copy-service
[^7]: PostgreSQL, *File System Level Backup*. https://www.postgresql.org/docs/current/backup-file.html
[^8]: Ceph, *CephFS Snapshot Mirroring*. https://docs.ceph.com/en/latest/cephfs/cephfs-mirroring/
