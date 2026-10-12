# Verification Specification and Test Catalog: Distributed Filesystem

**Version:** 0.1  
**Date:** 2026-10-12  
**Applies to:** [Functional specification, version 0.2](distributed-filesystem-fsd.md)  
**Status:** Proposed tests and release requirements. No filesystem implementation was supplied; these tests have not been implemented or executed here.  
**Scope:** .NET client library and `DfsStream : System.IO.Stream`; replicated live storage; volume-scoped ACID transactions; snapshots; independent late-sync read-only backup. No FUSE dependency.

## 1. What this document can establish

No finite test suite provides an unconditional **100% guarantee that a distributed system works**. Neither 100% line/branch coverage nor a large number of passing random seeds covers every input, concurrent schedule, failure history, hardware behavior, or incorrect requirement. A formal proof also establishes only the stated properties of its model under its assumptions unless implementation refinement and the relevant environment are covered.

The target is **requirements-linked verification with explicit safety invariants, controlled fault coverage, reproducible histories, independent oracles, and bounded formal analysis**. “Exhaustive” in a test report SHALL mean exhaustive over a published finite configuration/input/fault/schedule domain—not over every possible execution.

This catalog contains **586 named test specifications across 31 suites**, plus parameterized matrices that expand into additional concrete test executions. A row defines a test family; it is not a claim that one example invocation exhausts that family. Reusing a row across multiple configurations does not create a new named requirement or increase the named-case count.

Connection loss, offline nodes, failover, and resynchronization are mostly **distributed simulation/integration tests**, not isolated unit tests. Deterministic whole-cluster simulation with replayable failures has a practical precedent in FoundationDB.[^fdb] Coyote is a candidate for controlling .NET concurrency interleavings; its documentation explicitly distinguishes systematic testing from theorem-proving guarantees.[^coyote] Selection of a particular harness/library remains an implementation decision, including verification of its compatibility with the chosen runtime and dependencies.

## 2. Test levels and execution requirements

| Level | Meaning | Required environment |
|---|---|---|
| `U` | Isolated unit/component or reference-backend contract test | Controlled dependencies, no real distributed services |
| `S` | Deterministic distributed simulation, property-based histories, or compound-failure test | Production state-machine logic with virtual network, storage, scheduling, and clocks |
| `I` | Integration, protocol, real-process, or supported-software compatibility test | Actual SDK/server processes, transport, and local storage engine; faultable adapters where required |
| `H` | Hardware/storage-stack recovery test | Supported real devices, persistence settings, OS/filesystem, and controlled cold/power restart |
| `P` | Performance/resource-envelope test | Reproducible benchmark topology and explicitly stated durability policy |
| `M` | Bounded formal-model check or proof obligation | Specified model, properties, bounds, fairness assumptions, and completion report |

The listed level is the primary execution level. Every externally observable SDK contract SHALL also have a representative real-process integration execution. Protocol tests SHALL not be considered verified solely because a mock returned a predefined successful result. Critical simulation scenarios SHALL be repeated against real transport/persistence boundaries where those boundaries materially affect correctness.

`System.IO.Stream` is the interoperability contract, not `FileStream` or an OS pathname. The catalog exercises required abstract members, asynchronous overrides, and standard consumers against the proposed semantics.[^stream]

### 2.1 Minimum record for each executable test

Each concrete execution SHALL record: test ID; FSD clause and invariant IDs; implementation/build and dependency versions; initial data/model state; cluster/node/volume identities; topology and placement epoch; voting and durability policy; client/role count; input data seed; operation IDs; fault type, target, and exact injection point; scheduler/clock seed; expected legal outcomes; history-checker result; restart/recovery sequence; final object/namespace audit; resource cleanup; replay artifact; and outcome (`pass`, `fail`, `not run`, `inapplicable with reason`, or `inconclusive`).

A test SHALL assert its intended fault point was reached. An interrupted checker, unvisited injected fault, unreported skip, or missing oracle result SHALL NOT be recorded as a pass. Named tests in this document currently have status **not run**.

## 3. Fault model and claims

### 3.1 In scope

Crash-stop and crash-restart processes; pauses and long scheduler delays; node/zone unavailability; lost, delayed, duplicated application messages and broken connections; asymmetric partitions; reordered independent requests; client crashes/cancellation; finite disk stalls, short writes, full disks, I/O errors, torn/unflushed writes; detectable corruption; legitimate membership/policy/key/retention transitions; and failures during maintenance and backup transfer.

The initial trust model assumes one administrative domain, authenticated participants, and no Byzantine consensus requirement. Storage may fail or return an error, but a successfully completed durability primitive is assumed to meet the declared supported storage-stack contract. Real hardware tests check that assumption for supported configurations; they cannot prove all future hardware obeys it.

### 3.2 Out-of-envelope losses are tested, not magically repaired

A system with two actual durable data copies cannot promise survival after permanent destruction of both and every independent backup containing the data. Losing quorum may make intact bytes unavailable. Tests beyond the durability envelope SHALL assert honest unavailability/loss reporting, no invented content, and restoration only from genuinely surviving recovery points.

Keep these independent in every test: target copies; actual durable copies at acknowledgement; distinct required domains; metadata voters and current committed configuration; physically surviving objects; independent retained backup checkpoints. An asynchronous backup is not an extra live vote or an ordinary write acknowledgement.

### 3.3 Safety versus liveness

**Safety:** invalid committed state must not be exposed even during indefinite partition or paused processes. For example, the minority must not manufacture authority and a backup must not publish half a snapshot.

**Liveness:** eventual completion is asserted only after adequate authority, required healthy data sources and placement domains, sufficient space, a fair scheduler, and usable communication remain available long enough. No write deadline can be promised through permanent loss of a required acknowledgement target.

### 3.4 Correct handling of ambiguity

Track operation outcome separately from what the client currently knows. After a request may have reached commit, a lost reply can leave the client uncertain even when the operation is committed. The checker SHALL include unresolved operations as potentially committed, then narrow their outcomes when authoritative resolution becomes available. It SHALL never simply discard timed-out writes from the history. This distinction is also documented in etcd's API failure guarantees.[^etcd]

Atomicity is operation-specific. `Files.Copy`, `WriteAll*`, a standalone flush, and an explicit transaction have their specified publication units. `Stream.CopyToAsync` is a composition of reads/writes, not an implicit multi-call transaction. An earlier successful creating/truncating open or previous flush is not undone by a later failed write. A successful `OnFlush` write means private buffer acceptance, not committed durability.

## 4. Invariants checked throughout execution

| ID | Invariant and oracle |
|---|---|
| INV-01 | **Acknowledged durability.** A reported durable commit survives failures within its actual data-and-metadata protection envelope. Subsequent valid overwrite/deletion can change current state; durability does not imply retaining every historical byte forever. Inspect durable images, not server RAM. |
| INV-02 | **Atomic publication.** Every published file update/namespace operation/transaction has one legal complete outcome at its specified boundary; no partial object set is exposed as a successful publication. |
| INV-03 | **Live-read consistency.** Each new live read selects an authoritative revision consistent with its invocation/completion interval; all chunks of that read use that revision. Historical reads are explicitly labelled/pinned. |
| INV-04 | **Strictly serializable committed transactions.** Validate data reads, absence, and enumeration predicates against a real-time-compatible serialization; preserve all-or-nothing transaction views. |
| INV-05 | **Fenced writer authority.** At most one valid writer generation per inode can publish at a time; obsolete generations cannot regain authority through delayed requests, restart, or renewed connectivity. |
| INV-06 | **Retry/outcome integrity.** One accepted operation identity has at most one logical effect; incompatible reuse and retired IDs are rejected; definite abort prevents later commit and uncertainty is not misreported as rollback. |
| INV-07 | **Namespace and byte identity.** Paths resolve to the correct inode incarnations; no dangling entries/cycles/resurrection; sparse/truncated ranges and all returned bytes match the independent reference view. |
| INV-08 | **No silent weakening.** Failed quorum, placement, authorization, format, or integrity requirements cannot turn into fabricated success, false absence, unexpected fallback, or a weaker undeclared policy. |
| INV-09 | **Protected reachability.** GC/rebalance cannot remove objects required by current roots, snapshots, valid readers, unresolved/staged work, transfers, or safe placement transitions. |
| INV-10 | **Complete historical publication.** Every exposed or backup-acknowledged snapshot has its entire verified durable metadata/data dependency closure and required recovery material. |
| INV-11 | **Actual replica/domain accounting.** Distinct configured voters, durable copies, eligible host/zone identities, and witnesses/backups are counted according to their real roles and contents. |
| INV-12 | **Security boundary.** Every namespace/direct-storage/admin path enforces authenticated scope; tokens, caches, deduplication, and diagnostics do not bypass confidentiality or authorization. |
| INV-13 | **Lineage and retention integrity.** Epochs and volume identities do not mix; exact-snapshot retention is distinguished from a later checkpoint; independently protected history cannot be silently erased. |
| INV-14 | **Verified contents.** Successfully returned data match the selected immutable object/revision; corrupt/missing data produce verified failover or explicit error, never invented bytes. |
| INV-15 | **Backup independence.** Late-sync roles never enter live consensus, writer-lock, or ordinary acknowledgement sets; ordinary progress has no unbounded backup-retention dependency. |
| INV-16 | **Resource bounds.** Memory, in-flight requests, staging, pins, maintenance bandwidth, and capacity reserves obey configured limits; resource pressure cannot cause false acknowledgement. |
| INV-17 | **Conditional progress.** Under declared restored prerequisites, useful work and maintenance eventually proceed; absence of prerequisites is reported rather than misclassified as a liveness promise. |
| INV-18 | **Safe reconfiguration and resync.** Old state cannot supersede committed authority; new members/copies become eligible only through valid verification/admission, and old recoverable state is not discarded early. |
| INV-19 | **Truthful consistency/recovery labels.** Filesystem-consistent, application-consistent, at-least-boundary, and exact-snapshot claims reflect the specific adapter/snapshot/retention contract actually achieved. |
| INV-20 | **Evidence integrity.** Independent checkers, controlled nondeterminism, reached injection points, replayability, and complete reports are required; missing evidence is never a pass. |

Use invocation/completion histories to check consistency, not only the final directory tree. For byte correctness, compare deterministic source bytes and independent logical mappings; matching one aggregate checksum alone does not establish namespace, atomicity, or authority correctness.

## 5. Baseline configurations and boundary inputs

| Configuration | Purpose |
|---|---|
| One live node, one development voter | SDK/reference semantics only; no distributed-failure-tolerance claim |
| Three live hosts, one per zone; three metadata voters; target three/ack two in two zones | Core availability, fencing, policy, and recovery tests |
| Same topology, target three/ack three in three zones | Verify that loss of a required host/zone blocks the stronger policy |
| Three voters distributed 2+1 across two zones | Expose asymmetric zone-loss availability; do not assume both zones are independently survivable |
| Five live hosts/voters with legal explicit placement; independent data-copy policies | Majority boundaries and wider reconfiguration/interleaving coverage |
| Metadata/control/data roles on different host sets | Ensure one role's availability does not falsely imply another's availability |
| Every applicable live configuration plus one or two independent late-sync targets | Backup independence, heterogeneous lag, retention, reseed, and restore |
| Co-located roles/devices and deliberately invalid topology | Verify host/domain deduplication and rejection of false independence |

For compact three- and five-host models, enumerate all host-offline subsets, including none/all, and each node's pause/crash/disconnect incarnation where applicable. For three-voter partition tests include `3`, `2+1`, `1+1+1`, both directional halves of asymmetric cuts, and partitions that separate clients differently from servers. Extend with role-specific and zone-correlated cuts; do not infer that sampled larger deployments were exhaustively enumerated.

Parameterize lengths/offsets around `0`, `1`, inline threshold ±1, logical update size ±1, extent boundary ±1, buffer and batch boundaries, maximum supported values, and overflow attempts. Include empty/nonempty directories; absent/existing/deleted/recreated names; ASCII/multibyte/normalization-equivalent names; sparse/compressible/incompressible files; no/one/many snapshots; no/one/multiple clients; and cold/warm caches.

Visibility tests with concurrent live readers/writers SHALL deliberately use compatible sharing options (for example, writer `Share.Read` and reader `Share.ReadWrite`); a setup-level sharing conflict is not evidence about publication consistency. Snapshot/backup readers use their separately specified historical pin contract.

Enumerate all declared `FileMode` × `FileAccess` × `FileShare` combinations, including invalid flags, for missing/existing paths and conflicting active handles. Reuse native .NET semantics only where the FSD declares compatibility; intentional differences are tested against the FSD, not blindly against `FileStream`.[^modes][^sharing]

## 6. Suite index

| Suite | Area | Named cases | FSD trace |
|---|---|---:|---|
| HAR | Test harness and independent oracles | 18 | FSD §§16,18 |
| SDK | Client lifecycle, discovery, and API surface | 16 | FSD §§1,5,13 |
| PATH | Path parsing and name identity | 14 | FSD §4, NS-01–NS-04 |
| OPEN | Open modes, access rights, and handles | 19 | FSD §§5.4,6.1,7 |
| STR | Stream reads, writes, seeking, and interoperability | 28 | FSD §§5,6 |
| PUB | Buffering, flush, commit, and disposal | 24 | FSD §§6.3–6.4,10.3,13 |
| FILE | Convenience file operations and encoding | 16 | FSD §§5.2–5.4,6,8 |
| NS | Directories, renames, and namespace concurrency | 18 | FSD §4,NS-05–NS-07,§§5,8 |
| LOCK | Writer leases, sharing, and fencing | 20 | FSD §7,LOCK-01–LOCK-06 |
| TX | ACID transactions and strict serializability | 28 | FSD §8,§11 |
| LAY | Extent mapping, small files, and partial updates | 20 | FSD §10,§15 |
| REP | Replication policy, zones, and placement | 20 | FSD §9,§10.3 |
| CON | Consensus, authority, and metadata recovery | 20 | FSD §§3.2,7,8,13 |
| NET | Connection faults and transport behavior | 20 | FSD §§5,6,10,12,13 |
| OUT | Idempotency, cancellation, and uncertain outcomes | 20 | FSD §13,§6.4 |
| OFF | Offline nodes, partitions, and restart scenarios | 22 | FSD §§9,13,14.1 |
| RSY | Resynchronization, repair, and stale-state exclusion | 26 | FSD §§9.2,13,14.1 |
| DSK | Local persistence, corruption, and crash consistency | 20 | FSD §§10.3,13,14 |
| SNP | Snapshots, exact recovery points, and shadow copies | 18 | FSD §11 |
| APP | Application-consistent snapshots and adapters | 12 | FSD §11.3 |
| BAK | Late-sync replica transfer, read-only service, and retention | 30 | FSD §12 |
| RST | Disaster recovery, recovery keys, and promotion | 16 | FSD §§11.1,12.4–12.5,14 |
| GC | Garbage collection, references, and capacity reserves | 20 | FSD §§10.3,11,12.4,14 |
| SEC | Authentication, authorization, and confidentiality | 18 | FSD §§4,7,12,14 |
| FMT | Wire formats, disk formats, upgrades, and compatibility | 12 | FSD §§3.2,10,12,15,17 |
| OPS | Administration, observability, and bounded liveness | 16 | FSD §§9,12,13,14,15 |
| PERF | Performance, amplification, and resource envelopes | 16 | FSD §§10,15 |
| PROP | Property-based and model-based operation histories | 16 | FSD §§4–16,18 |
| MOD | Bounded formal models and protocol obligations | 12 | FSD §§3.2,7–14,18 |
| CHA | Compound-failure regression scenarios | 16 | FSD §§7–16,18 |
| MUT | Negative controls and mutation testing of safety checks | 15 | FSD §18 |

## 7. Named test catalog

All assertions are normative targets for the proposed implementation. Each suite inherits its FSD references, invariant set, boundary-input matrix, and applicable connection/persistence fault matrix. Expand multi-variant assertions into individually identifiable parameterized executions.

### 7.1. HAR — Test harness and independent oracles

**Requirements:** FSD §§16,18. **Invariants:** INV-01–INV-20.

| Test ID | Level | Test name | What it tests / required assertion |
|---|---|---|---|
| HAR-001 | U | `SeedReplayIsExact` | Run the same build, seed, starting images, and schedule twice; operation outcomes, fault events, and state digests are identical. |
| HAR-002 | U | `NondeterminismIsIntercepted` | Attempt direct wall-clock, random, timer, socket, and disk access from simulated components; the harness reports uncontrolled dependencies instead of silently permitting them. |
| HAR-003 | U | `IndependentNodeClocks` | Advance, drift, freeze, and step individual wall clocks without changing other nodes; lease and ordering tests do not accidentally rely on one shared clock. |
| HAR-004 | U | `CrashLosesVolatileState` | Crash a node after writes without a successful durability barrier; reboot reconstructs only an allowed persisted image, not the pre-crash heap or page cache. |
| HAR-005 | U | `FlushFailureNeverPersistsByAssumption` | Fail or delay a flush; simulated receipts cannot treat pending bytes as necessarily durable. |
| HAR-006 | U | `NetworkDirectionality` | Cut A-to-B while retaining B-to-A; the harness delivers exactly the permitted directions and retains the intended request/reply distinction. |
| HAR-007 | U | `TransportFidelity` | Preserve byte order inside one intact ordered transport stream while independently reordering RPCs, retries, and messages across streams; application-message duplication is distinct from TCP packet duplication. |
| HAR-008 | U | `CrashAndPauseDiffer` | Pause retains process memory and resumes old tasks; crash discards memory and executes recovery. Both modes are independently observable. |
| HAR-009 | U | `HistoryCapturesAmbiguity` | Capture invocation, completion, definite abort, unknown outcome, retry ID, and commit resolution; unknown operations are not omitted or automatically classified as failed. |
| HAR-010 | U | `OracleIsIndependent` | Compare deliberately corrupted implementation outputs to a simple independent namespace/byte-array/MVCC model; sharing a storage implementation with the oracle is prohibited. |
| HAR-011 | U | `CheckerRejectsBadHistories` | Feed known lost-update, stale-live-read, fractured-transaction, duplicate-append, and write-skew histories; each is rejected. |
| HAR-012 | U | `CheckerAcceptsLegalConcurrency` | Feed known legal overlapping histories with more than one possible serialization; the checker does not require invocation order for overlapping operations. |
| HAR-013 | U | `FaultPointActuallyReached` | Configure each injection by event identity and hit count; missing or unreachable injections fail or are explicitly classified, never counted as exercised. |
| HAR-014 | U | `TraceMinimizationPreservesFailure` | Shrink a failing operation/fault sequence and replay it; the same invariant still fails with the saved minimal trace. |
| HAR-015 | U | `RecoveryRequiresStableConditions` | Withhold quorum or capacity indefinitely; the oracle permits unavailability but not fabricated success. Restore stated prerequisites before asserting liveness. |
| HAR-016 | U | `TestIsolation` | Run seeds concurrently and sequentially; ports, directories, caches, identities, and retained state cannot leak between tests. |
| HAR-017 | U | `UnfinishedChecksAreNotPasses` | Exhaust a model-checking bound or time out a history checker; report incomplete/inconclusive rather than success. |
| HAR-018 | U | `AssumptionViolationIsLabelled` | Make every supposedly durable device lose acknowledged bytes; classify an out-of-model storage violation rather than claim the replication protocol covers dishonest persistence. |

### 7.2. SDK — Client lifecycle, discovery, and API surface

**Requirements:** FSD §§1,5,13. **Invariants:** INV-06,INV-08,INV-12,INV-17.

| Test ID | Level | Test name | What it tests / required assertion |
|---|---|---|---|
| SDK-001 | U | `OptionsRejectInvalidValues` | Reject missing credentials, invalid endpoints, nonpositive buffers/timeouts, unknown enum values, and incompatible options before issuing a mutation. |
| SDK-002 | I | `NoMountRequired` | Use file helpers and returned Stream objects with all mounts and FUSE components absent; no hidden local-path dependency exists. |
| SDK-003 | U | `MultipleClientsAreIsolated` | Two clients with different clusters, volumes, credentials, and defaults never share routing or authorization state incorrectly. |
| SDK-004 | S | `SeedEndpointOffline` | Make the first seed unreachable; discovery uses another trusted seed and reaches the intended cluster. |
| SDK-005 | S | `AllSeedsOffline` | Fail all seed endpoints; connection terminates at its deadline with a typed failure and no leaked tasks or sockets. |
| SDK-006 | S | `ClusterIdentityMismatch` | Return a reachable endpoint belonging to another cluster; reject it rather than silently binding to its namespace. |
| SDK-007 | S | `RoutingChangesDuringCall` | Move volume leadership after discovery; reroute within the deadline while preserving the operation ID and outcome. |
| SDK-008 | U | `FacadesAreThreadSafe` | Concurrently use file/directory facades on unrelated handles; connection pools, request IDs, and results remain isolated. |
| SDK-009 | U | `AsyncMethodsAreActuallyAsync` | Block the injected transport without a worker-thread substitute; async calls yield and honor cancellation without performing synchronous network waits. |
| SDK-010 | I | `SyncContextDoesNotDeadlock` | Invoke synchronous wrappers from a single-threaded synchronization context; completion does not depend on continuations returning to that blocked context. |
| SDK-011 | U | `ClientDisposalIsIdempotent` | Dispose twice and through both disposal interfaces; resources are released once and new operations are rejected. |
| SDK-012 | S | `DisposalDuringRequests` | Dispose with active requests under the documented shutdown policy; each operation resolves or reports uncertainty correctly, and transactions are not implicitly committed. |
| SDK-013 | U | `CancellationTokenPropagation` | For every async public overload, cancellation reaches routing, connection acquisition, staging, and wait operations without losing commit-outcome semantics. |
| SDK-014 | U | `CapabilitiesAreAdvertised` | Read limits and supported features; SDK validation and server-side enforcement agree, including when a server has stricter limits. |
| SDK-015 | I | `PlatformContractParity` | Run the same supported SDK contract on Windows, Linux, and macOS; host path rules do not change the volume namespace. |
| SDK-016 | U | `SecretsAreNotLogged` | Trigger connection, authorization, and serialization failures; diagnostic events contain neither credentials nor file payloads. |

### 7.3. PATH — Path parsing and name identity

**Requirements:** FSD §4, NS-01–NS-04. **Invariants:** INV-07,INV-08,INV-12.

| Test ID | Level | Test name | What it tests / required assertion |
|---|---|---|---|
| PATH-001 | U | `AbsoluteVolumeRelativePaths` | Accept root and valid absolute volume-relative names; reject relative paths without sending a request. |
| PATH-002 | U | `RejectTraversalAndEmptyComponents` | Reject dot, dot-dot, repeated separators, forbidden trailing separators, and attempts to escape the selected volume. |
| PATH-003 | U | `RejectForbiddenCharacters` | Reject NUL, backslash, malformed surrogate sequences, and malformed wire UTF-8 without changing namespace state. |
| PATH-004 | U | `UnicodeNormalizationIdentity` | Precomposed and decomposed NFC-equivalent names resolve to the same entry, including create races between different clients. |
| PATH-005 | U | `CaseSensitiveOrdinalIdentity` | Names differing only by case remain distinct under all supported cultures and server locales. |
| PATH-006 | U | `Utf8ComponentLimit` | Test 254, 255, and 256 UTF-8 bytes after normalization with ASCII and multibyte names; enforce byte rather than character limits. |
| PATH-007 | U | `Utf8PathLimit` | Test normalized paths at 4,095, 4,096, and 4,097 bytes; reject excess consistently on client and server. |
| PATH-008 | U | `CultureCannotChangeLookup` | Switch culture, including special case-folding locales; ordinal lookup and normalization stay deterministic. |
| PATH-009 | U | `NoUrlDoubleDecoding` | Send literal percent sequences and encoded separators through the wire API; names are not decoded twice or treated as traversal. |
| PATH-010 | U | `RootProtection` | Reject root deletion, renaming root, and file creation over root; valid root enumeration and metadata remain available. |
| PATH-011 | S | `CanonicalNameCreateRace` | Concurrently create normalization-equivalent names; one committed entry exists and duplicate-create semantics are preserved. |
| PATH-012 | U | `ServerRevalidatesPaths` | Bypass SDK validation with a direct protocol client; the server rejects every invalid path variant independently. |
| PATH-013 | U | `OffsetsDoNotOverflow` | Test negative values, maximum supported length, long.MaxValue, and offset-plus-length overflow; no wrapped addressing or partial publication occurs. |
| PATH-014 | U | `TokenIdentityCannotCrossVolumes` | Supply handles, revisions, continuations, or commit tokens for a different volume; reject rather than accidentally address matching local numeric IDs. |

### 7.4. OPEN — Open modes, access rights, and handles

**Requirements:** FSD §§5.4,6.1,7. **Invariants:** INV-02,INV-05,INV-07,INV-12.

| Test ID | Level | Test name | What it tests / required assertion |
|---|---|---|---|
| OPEN-001 | U | `CreateNewAbsent` | Create a missing file; the handle and namespace entry appear together with length zero. |
| OPEN-002 | S | `CreateNewRace` | Multiple clients create the same absent name; exactly one logical create wins and losing requests do not leak visible inodes. |
| OPEN-003 | U | `CreateExistingTruncatesAtOpen` | Open an existing file with Create; a successful open durably truncates it even before subsequent writes. |
| OPEN-004 | U | `CreateMissingCreates` | Open a missing file with Create; creation obeys parent permissions, inherited policy, and durability requirements. |
| OPEN-005 | U | `OpenMissingFails` | Open an absent file with Open; receive the specified missing-file exception without creating anything. |
| OPEN-006 | U | `OpenExistingPreservesBytes` | Open existing contents with supported access options; merely opening does not change data or length. |
| OPEN-007 | S | `OpenOrCreateRace` | Race OpenOrCreate calls; creation is atomic and share/writer reservations admit only compatible handles. |
| OPEN-008 | U | `TruncateRequiresExistingWriteOnly` | Test absent files and each access mode; only the specified write-only existing-file case succeeds. |
| OPEN-009 | U | `OpenWriteDoesNotTruncate` | OpenWrite on a longer file and overwrite a short prefix; the old suffix survives unless SetLength is called. |
| OPEN-010 | U | `AppendTargetsLogicalEnd` | Seek is unavailable and every append uses the staged logical end; the original prefix remains intact. |
| OPEN-011 | U | `AppendRejectsReadAndSetLength` | Reject unsupported access, seeking, Position assignment, and SetLength on append handles without changing contents. |
| OPEN-012 | U | `ReadOnlyDeniesMutation` | Write, WriteByte, SetLength, and mutating extensions fail on read-only handles; reads still work. |
| OPEN-013 | U | `WriteOnlyDeniesRead` | Read, ReadByte, and reading helpers fail on write-only handles; writable operations preserve their normal semantics. |
| OPEN-014 | S | `ReservationAndCreateAreAtomic` | Interrupt open between creation and reservation processing; no visible unreserved intermediate state bypasses the sharing rules. |
| OPEN-015 | S | `OpenReplyLost` | Commit a creating/truncating open and drop its reply; retry the same ID without a second truncate or a second independent lease. |
| OPEN-016 | S | `RenameDoesNotRetargetHandle` | Rename an open inode and recreate its previous name; existing handles still refer to the original inode. |
| OPEN-017 | S | `UnlinkDoesNotResurrectName` | Delete with compatible FileShare.Delete, then write through the old handle; bytes remain on the old inode and do not recreate the path. |
| OPEN-018 | U | `OpenInsideTransactionIsPrivate` | Create/truncate through a transaction; other clients see no change until its commit. |
| OPEN-019 | U | `UnsupportedOsFeaturesFailExplicitly` | Request inherited OS handles, native handles, unsupported FileOptions, or other deferred features; receive an explicit unsupported result. |

### 7.5. STR — Stream reads, writes, seeking, and interoperability

**Requirements:** FSD §§5,6. **Invariants:** INV-02,INV-03,INV-07,INV-16.

| Test ID | Level | Test name | What it tests / required assertion |
|---|---|---|---|
| STR-001 | U | `AllReadOverloadsAgree` | Compare array, span, memory, asynchronous array, ReadByte, ReadExactly, and ReadAtLeast behavior against the declared byte/EOF contract. |
| STR-002 | U | `AllWriteOverloadsAgree` | Write the same sequence using array, span, memory, async array, and WriteByte overloads; staged and committed contents agree. |
| STR-003 | U | `BufferArgumentValidation` | Test null buffers, negative offset/count, excessive ranges, and zero-sized buffers; validate without unintended position or metadata changes. |
| STR-004 | U | `ShortReadIsValid` | Return verified short reads from the backend; callers receive actual counts and Position advances by those counts only. |
| STR-005 | U | `EofIsNotUnavailability` | At EOF return zero; with required data or authority unavailable throw/report availability failure, never fabricated EOF. |
| STR-006 | U | `ReadExactlyHandlesShortReads` | Force multiple short reads; ReadExactly assembles the requested data or reports genuine premature EOF. |
| STR-007 | U | `ZeroLengthReadAndWrite` | Zero-length calls have no data effect or artificial file extension; disposed/access validation follows the published overload contract. |
| STR-008 | U | `SeekOrigins` | Exercise Begin, Current, and End with positive/negative offsets; valid positions are exact and invalid positions are rejected. |
| STR-009 | U | `SeekBeyondEofDoesNotExtend` | Seek beyond current length; length stays unchanged until a write or SetLength. |
| STR-010 | U | `SparseWriteReturnsZeroGap` | Write beyond EOF, commit, reopen, and read the gap; every unwritten byte is zero. |
| STR-011 | U | `TruncateThenExtendErasesTail` | Truncate, extend, flush, restart, and read; discarded bytes never reappear through sparse mapping or allocator reuse. |
| STR-012 | U | `SetLengthAndPosition` | Shrink below current Position without silently moving Position; later reads/writes follow the specified offset semantics. |
| STR-013 | U | `WriterReadsOwnStaging` | Interleave writes, seeks, reads, and truncation on one writable/readable handle; reads combine staged ranges and the base revision correctly. |
| STR-014 | S | `LiveReadPinsOneRevisionPerCall` | Commit a replacement during a multi-extent read; that read returns one revision, not old/new mixed extents. |
| STR-015 | S | `LiveCallsMayObserveNewRevisions` | Read, commit from another permitted writer, then read again; a new live call observes a current revision, not a silently pinned opening view. |
| STR-016 | S | `PinnedStreamRemainsStable` | Overwrite, rename, or delete the current file while reading a pinned revision; the pinned view remains unchanged until its pin expires. |
| STR-017 | S | `ExpiredPinFailsExplicitly` | Let a pin expire and reclaim its objects; subsequent operations fail rather than read reused storage or silently switch revisions. |
| STR-018 | U | `LengthUsesDeclaredView` | Check Length and async metadata for live, pinned, writer-staged, and transaction views after grow/truncate operations. |
| STR-019 | U | `ConcurrentStreamCallsRejected` | Overlap read/read, write/write, read/write, seek/I/O, flush/I/O, and disposal/I/O; reject the unsupported overlap deterministically. |
| STR-020 | U | `CopyStartsAtCurrentPosition` | CopyTo and CopyToAsync transfer only bytes from the source's current position and advance both positions by transferred counts. |
| STR-021 | I | `StreamReaderWriterInterop` | Use UTF-8 text with split multibyte characters and small buffers; outer flush/disposal correctly hands data to DfsStream. |
| STR-022 | I | `BufferedStreamInterop` | Wrap read/write streams in BufferedStream; outer buffering does not imply underlying publication until properly flushed. |
| STR-023 | I | `CompressionStreamInterop` | Round-trip GZip or another standard compression stream, including finalization bytes; leaveOpen and disposal behavior are respected. |
| STR-024 | I | `SeekRequiringConsumerInterop` | Create/read a seek-requiring archive through the supported stream mode; backward seeks and final metadata updates work without local files. |
| STR-025 | U | `DisposedStreamFails` | After successful disposal, all unsupported further operations report the appropriate disposed behavior and capabilities reflect closure. |
| STR-026 | U | `ReadFailureDoesNotMovePosition` | Fail a read without a successful return; Position remains unchanged, and any caller-buffer modifications are not presented as valid returned data. |
| STR-027 | U | `CallerWriteBufferOwnership` | After a successful Write returns, mutate/reuse the caller buffer; later flush uses the originally accepted bytes, not the mutated array. |
| STR-028 | U | `CancellationDoesNotRetainMemoryUnsafely` | Cancel outstanding memory-based I/O and complete the operation; no later callback reads or writes application memory after its ownership returns. |

### 7.6. PUB — Buffering, flush, commit, and disposal

**Requirements:** FSD §§6.3–6.4,10.3,13. **Invariants:** INV-01,INV-02,INV-06,INV-09,INV-16.

| Test ID | Level | Test name | What it tests / required assertion |
|---|---|---|---|
| PUB-001 | U | `OnFlushKeepsWritesPrivate` | Write without flush; writer sees staging and other clients still see the last committed version. |
| PUB-002 | S | `AutoStagingDoesNotPublish` | Exceed client buffer size so data uploads early; remote readers and snapshots still exclude all uncommitted bytes. |
| PUB-003 | S | `FlushPublishesWholeFileRevision` | Modify multiple distant extents and length, then flush; readers see all changes together at one publication boundary. |
| PUB-004 | S | `FlushWaitsForRequiredDurability` | Hold a required disk acknowledgement; FlushAsync does not return success merely because network delivery completed. |
| PUB-005 | S | `FlushDoesNotWaitForBackup` | Disconnect the late-sync host while live durability remains possible; flush completes without adding the backup to its acknowledgement set. |
| PUB-006 | S | `WriteThroughCommitsEachCall` | After each successful WriteThrough call, independent live reads see its complete change and required durability has been met. |
| PUB-007 | U | `NoOpFlushDoesNotDuplicateData` | Flush repeatedly without new writes; no data mutation, duplicate append, or accidental truncation occurs. |
| PUB-008 | S | `SequentialFlushesHaveDistinctBoundaries` | Stage A, flush, stage B, flush; snapshot/token observations identify the appropriate complete file revisions. |
| PUB-009 | S | `SetLengthFollowsWriteMode` | Under OnFlush length changes stay private; under WriteThrough a successful SetLength is committed before return. |
| PUB-010 | S | `CommitReturnsResolvableToken` | CommitAsync returns cluster/volume/epoch/sequence identity matching the published decision and queryable outcome. |
| PUB-011 | S | `DataBeforeMetadataPublication` | Delay one newly referenced data or mapping object; metadata publication and successful flush remain prohibited. |
| PUB-012 | S | `CrashBeforePublicationKeepsOldState` | Crash at each staging step; recovery exposes the old committed file, not some subset of staged changes. |
| PUB-013 | S | `CrashAfterCommitKeepsNewState` | Crash after durable metadata decision, before reply; recovery preserves the full new revision and outcome. |
| PUB-014 | S | `CancelledBeforeRequestDoesNotPublish` | Cancel before any publish request is transmitted; committed contents and revision remain unchanged. |
| PUB-015 | S | `CancellationAfterPossibleCommitIsUnknown` | Cancel after transmission when outcome cannot be established; return the typed unknown-outcome error rather than definite rollback. |
| PUB-016 | U | `SuccessfulDisposeFlushes` | Dispose a dirty standalone stream; success means pending bytes met live durability and the reservation was released. |
| PUB-017 | S | `DisposeFlushFailureIsVisible` | Fail required persistence during Dispose/DisposeAsync; surface the failure, retain/query possible outcome, and never report successful commit. |
| PUB-018 | U | `AbortDiscardsOnlyUncommittedChanges` | Flush A, stage B, then abort; A remains committed and B is never published. |
| PUB-019 | U | `AbortDoesNotUndoCreatingOpen` | Create/truncate successfully, then abort later staging; the already committed namespace/open effect remains. |
| PUB-020 | U | `FinalizerDoesNotCommit` | Abandon a dirty stream and trigger collection; no finalizer-driven network publication occurs. |
| PUB-021 | S | `LostLeaseFaultsDirtyStream` | Expire and replace the writer before flush; old staged data cannot be published by silently reacquiring a new lease. |
| PUB-022 | U | `FailedStagedWriteCannotLeakPartialCall` | Fail acceptance of a buffered Write partway through; roll it back privately or fault the stream so disposal/flush cannot publish a partial failed call. |
| PUB-023 | S | `FlushDeadlineBoundsWaiting` | Withhold quorum or required replicas; the operation terminates by its deadline without relaxing the policy. |
| PUB-024 | U | `PublicationLimitIsAtomic` | Exceed advertised staged/publish limits by one; receive the typed limit failure with no partial committed file revision. |

### 7.7. FILE — Convenience file operations and encoding

**Requirements:** FSD §§5.2–5.4,6,8. **Invariants:** INV-02,INV-07,INV-08,INV-12,INV-16.

| Test ID | Level | Test name | What it tests / required assertion |
|---|---|---|---|
| FILE-001 | U | `WriteAllBytesAtomicReplacement` | Replace existing bytes; readers see old or complete new contents, and inode identity is preserved. |
| FILE-002 | S | `WriteAllFailureLeavesOldFile` | Fail upload or validation before publication; the old destination remains intact instead of being pre-truncated. |
| FILE-003 | U | `ReadAllAllocationLimit` | Read above the configured materialization limit; reject allocation while the same file remains readable as a bounded stream. |
| FILE-004 | U | `Utf8WithoutBomDefault` | Write default text and inspect bytes; no BOM is emitted and Unicode round-trips. |
| FILE-005 | U | `ExplicitEncodingAndBom` | Test supported explicit encodings and UTF-8 BOM input; documented encoding selection and BOM handling are consistent. |
| FILE-006 | U | `LineHelpersHandleBoundaries` | Use empty files, empty lines, CRLF/LF, final unterminated lines, and multibyte characters split between blocks. |
| FILE-007 | U | `LazyReadLinesStaysBounded` | Read a large line collection through async enumeration; memory and open-handle lifetimes stay within configured bounds. |
| FILE-008 | S | `AppendHelperRetryIsIdempotent` | Drop the response after a committed append helper; retry with the same ID produces one suffix only. |
| FILE-009 | S | `CopyPinsSourceVersion` | Change the source during Copy; the committed destination is a complete source revision, not a composite of several versions. |
| FILE-010 | S | `CopyFailureIsAtomic` | Interrupt copy before destination publication; old destination or absence remains, with no partially visible replacement. |
| FILE-011 | U | `CopySelfAndInvalidDestinations` | Reject copying a file onto itself or a directory according to the contract, without truncating the source. |
| FILE-012 | U | `CopyOverwriteOptions` | Test missing/existing destinations with overwrite on/off; respect permissions, share reservations, and atomic replacement. |
| FILE-013 | U | `DeleteMissingIsIdempotent` | Delete an absent file repeatedly; success does not affect its parent or similarly named entries. |
| FILE-014 | S | `ExistsDistinguishesAbsenceFromFailure` | Compare absent entry, denied permission, disconnected authority, and corrupt metadata; only genuine absence produces false. |
| FILE-015 | U | `HelpersInsideTransactionDoNotCommit` | Run WriteAll, Copy, Move, Delete, and append through a transaction facade; all remain private until transaction commit. |
| FILE-016 | U | `HelperCancellationCleansResources` | Cancel staged helpers; close internal streams, release safe reservations, and retain unresolved outcome IDs when necessary. |

### 7.8. NS — Directories, renames, and namespace concurrency

**Requirements:** FSD §4,NS-05–NS-07,§§5,8. **Invariants:** INV-02,INV-03,INV-04,INV-07,INV-09.

| Test ID | Level | Test name | What it tests / required assertion |
|---|---|---|---|
| NS-001 | U | `RecursiveCreateIsIdempotent` | Create nested directories twice; existing directories remain and intermediate file conflicts are rejected. |
| NS-002 | S | `ConcurrentDirectoryCreate` | Race recursive creation through common parents; produce one valid tree without duplicate entries or dangling inodes. |
| NS-003 | U | `NonrecursiveDeleteRejectsNonempty` | Attempt nonrecursive deletion of a nonempty directory; the entire tree remains unchanged. |
| NS-004 | S | `RecursiveDeleteDetachesAtomically` | Observe a large subtree while it is deleted; namespace detachment is atomic even when reclamation continues later. |
| NS-005 | S | `RenameFileAtomic` | Observe source and destination in one consistent namespace view while renaming; no committed view contains an invalid halfway state. |
| NS-006 | S | `RenameDirectoryAtomic` | Rename a populated subtree; all descendants remain attached through the new parent without copying or partial visibility. |
| NS-007 | U | `RejectDirectoryCycles` | Move a directory into itself or a descendant; reject without creating namespace cycles. |
| NS-008 | S | `RenameOverDestinationHonorsSharing` | Replace an existing destination only with compatible reservations; old open handles remain bound to their old inodes. |
| NS-009 | U | `CrossVolumeMoveRejected` | Attempt an atomic Move across volumes; return an explicit unsupported/cross-volume error, not a hidden copy-delete. |
| NS-010 | S | `DeleteRacesCreate` | Interleave deletion and recreation of one name; each committed operation refers to the correct inode incarnation. |
| NS-011 | S | `RenameRacesOpen` | Open while rename commits; each successful handle names exactly one valid inode and obeys authorization/share checks at its linearization point. |
| NS-012 | S | `EnumeratePinnedVersion` | Modify the directory between pages; one enumeration returns its original namespace snapshot without duplicates or missing original entries. |
| NS-013 | U | `ContinuationIsBoundToQuery` | Reuse continuation with a different volume, directory, filter, or principal; reject instead of reading another query's state. |
| NS-014 | S | `ExpiredContinuationFails` | Expire and reclaim the enumeration pin; next page fails explicitly rather than restarting against a newer view. |
| NS-015 | S | `DeletionRespectsOpenPins` | Delete an inode/subtree with permitted open readers; reads remain safe until close/expiry and reclamation happens only afterward. |
| NS-016 | U | `PolicyFollowsInodeAcrossRename` | Move a file between directories with different defaults; its effective existing policy does not silently change. |
| NS-017 | S | `DirectoryMutationHasRetryIdentity` | Lose replies for mkdir, rename, and recursive delete; retries resolve the original mutation without applying it to newly recreated names. |
| NS-018 | U | `CreationMetadataIsConsistent` | Check stable inode ID, revision, length, timestamps, authorization, and policy fields after each namespace mutation. |

### 7.9. LOCK — Writer leases, sharing, and fencing

**Requirements:** FSD §7,LOCK-01–LOCK-06. **Invariants:** INV-02,INV-05,INV-06,INV-12.

| Test ID | Level | Test name | What it tests / required assertion |
|---|---|---|---|
| LOCK-001 | U | `ShareCompatibilityBothDirections` | Enumerate access/share pairs for existing and proposed handles; both handles must permit the other's access. |
| LOCK-002 | U | `ShareNoneExcludesOtherHandles` | Open with FileShare.None; all conflicting opens fail until safe close or lease expiry. |
| LOCK-003 | S | `OneWriterPerInode` | Race two writable opens through different paths/clients; at most one writer reservation becomes authoritative. |
| LOCK-004 | U | `CompatibleReaderWriterPair` | Use writer Share.Read and reader Share.ReadWrite; readers coexist while only the writer can publish. |
| LOCK-005 | U | `UnsupportedMultipleWriterShareRejected` | Request FileShare.Write on a writable handle in the core release; reject explicitly rather than imply multiwriter support. |
| LOCK-006 | U | `DeleteShareIsIndependent` | Vary FileShare.Delete independently from read/write bits; delete/rename-overwrite obey its required checks. |
| LOCK-007 | U | `InheritableFlagRejected` | Request FileShare.Inheritable or unknown bits; reject before acquiring a reservation. |
| LOCK-008 | S | `LeaseRenewalReplyLost` | Commit renewal and drop its response; retries preserve the same reservation and do not create an extra writer. |
| LOCK-009 | S | `OldWriterResumesAfterReplacement` | Pause writer A, safely expire it, and grant writer B; resume A both before B changes the inode revision and after B commits. Every publication by A's old fencing generation is rejected, even when A's expected revision is still current. |
| LOCK-010 | S | `DelayedRenewalCannotReviveExpiredLease` | Deliver an old renewal after a new generation is granted; the new writer remains authoritative and old authority is not restored. |
| LOCK-011 | S | `LeaderChangePreservesFencing` | Elect a new metadata leader with leases in flight; persisted generations prevent duplicate grants and stale publication. |
| LOCK-012 | S | `FullRestartPreservesGeneration` | Cold-restart all durable metadata replicas; new writer generations cannot reuse an older accepted fencing token. |
| LOCK-013 | S | `PartitionedMinorityCannotGrantWriter` | Isolate a minority with an old leader; it cannot successfully grant a new live reservation or authorize a flush. |
| LOCK-014 | S | `ClientClockJumpCannotExtendAuthority` | Jump a client's wall clock backward/forward and suspend its timers; server-authoritative fencing remains decisive. |
| LOCK-015 | S | `ServerClockChangesDoNotCreateTwoWriters` | Exercise independent clock jumps around renewal/expiry; safety is maintained even if availability is reduced. |
| LOCK-016 | S | `ClosedHandleDelayedRequestRejected` | Release a reservation, acquire a successor, then deliver an old queued write; it cannot mutate the successor's inode state. |
| LOCK-017 | S | `AllMutationPathsUseFencing` | Attempt stale writes via helpers, transaction commit, truncate, restore, and direct publication RPCs; no alternate path bypasses authority. |
| LOCK-018 | S | `TransactionReservationCleanup` | Abort/conflict/expire a transaction holding several inode reservations; release them safely without affecting a newer holder. |
| LOCK-019 | U | `LockOrderCannotDeadlockForever` | Two transactions request files in opposing orders; conflict/abort/deadline policy resolves the cycle rather than indefinitely blocking both. |
| LOCK-020 | S | `BackupCannotAcquireLiveLocks` | Connect the backup role to lock endpoints; deny acquisition/renewal and verify its absence from writer reservation state. |

### 7.10. TX — ACID transactions and strict serializability

**Requirements:** FSD §8,§11. **Invariants:** INV-01,INV-02,INV-04,INV-06,INV-07,INV-09.

| Test ID | Level | Test name | What it tests / required assertion |
|---|---|---|---|
| TX-001 | U | `CommitMultipleFilesAtomically` | Update two files and commit; a transactional observer sees either both old versions or both new versions. |
| TX-002 | U | `AbortLeavesNoVisibleChanges` | Stage file and directory modifications then abort; no namespace or data change is visible. |
| TX-003 | U | `ReadOwnWritesAndDeletes` | Within one transaction, reads reflect its staged creates, overwrites, renames, and deletes. |
| TX-004 | S | `NoDirtyReads` | Read from another transaction/client while data is staged; uncommitted contents never leak. |
| TX-005 | S | `NoLostUpdate` | Two transactions read a version and try to overwrite it; conflicting commits cannot both silently discard one another's changes. |
| TX-006 | S | `WriteSkewRejected` | Use two files representing a joint invariant, with each transaction reading both and updating a different one; prevent an invalid jointly committed result. |
| TX-007 | S | `AbsentNameReadIsValidated` | Read a missing name, then concurrently create it; validate the absence dependency rather than ignore the phantom. |
| TX-008 | S | `DirectoryPredicateIsValidated` | Enumerate matching entries, then concurrently add/remove a matching entry; serialization checks include the predicate dependency. |
| TX-009 | S | `StrictRealTimeOrder` | Complete transaction A before invoking B; successful B's reads and commit cannot serialize before A when they depend on its state. |
| TX-010 | S | `ReadOnlyTransactionIsConsistent` | Read several files around a concurrent atomic commit; the read-only transaction never observes a fractured combination. |
| TX-011 | S | `OrdinaryWritesParticipateInConflicts` | Mix standalone flushes/helpers with transaction reads and writes; ordinary operations cannot bypass transaction isolation. |
| TX-012 | U | `BoundStreamFlushOnlyStages` | Flush a transaction-bound stream; data may be durable internally but no transaction changes become publicly visible. |
| TX-013 | U | `BoundStreamDisposeDoesNotCommit` | Dispose all transaction-bound streams; transaction remains uncommitted until its explicit CommitAsync. |
| TX-014 | U | `BoundStreamStandaloneCommitRejected` | Call the stream's independent CommitAsync while transaction-bound; reject without committing any part of the transaction. |
| TX-015 | U | `TransactionDisposeAborts` | Dispose an uncommitted transaction; request abort and never implicitly commit. |
| TX-016 | S | `CommitCollectsRemainingStaging` | Leave valid pending bound-stream changes and commit the transaction with no overlapping stream operations; include all staged data atomically. |
| TX-017 | U | `CommitRejectsOverlappingStreamIo` | Race transaction commit with a bound stream operation; reject the unsupported overlap without a partial transaction. |
| TX-018 | U | `HandlesInvalidAfterCompletion` | After commit or abort, bound streams cannot read/write as if the transaction were still active; disposal remains safe. |
| TX-019 | S | `LeaderCrashBeforeDecision` | Crash metadata leadership before transaction decision; resolve to a legal committed/aborted/pending outcome without partial visibility. |
| TX-020 | S | `LeaderCrashAfterDecision` | Crash after durable commit but before response; transaction recovery reports the same committed result and complete contents. |
| TX-021 | S | `RetryDoesNotRerunUserCode` | Force retryable transaction conflicts; SDK does not silently execute application callbacks or external side effects again. |
| TX-022 | U | `TransactionLimitsFailAtomically` | Exercise duration, touched-inode, descriptor, staging, and pin limits at boundary and boundary-plus-one; excess cannot partially commit. |
| TX-023 | U | `CrossVolumeTransactionRejected` | Use a handle or operation from another volume; reject without enlisting it implicitly. |
| TX-024 | S | `SnapshotNeverSplitsTransaction` | Race snapshot creation with transaction commit; snapshot includes all or none of that transaction. |
| TX-025 | S | `AtomicCommitAndSnapshot` | Commit plus named snapshot in one decision; snapshot preserves the exact committed boundary despite a concurrent later overwrite. |
| TX-026 | S | `UnknownCommitCannotBeUndoneByDispose` | Lose commit reply then dispose/abort; recovery retains the already committed decision rather than falsely reporting rollback. |
| TX-027 | S | `ReadValidationAfterStagingDelay` | Pause after reading/staging, perform a conflicting change, then resume commit; late validation detects the changed dependency. |
| TX-028 | U | `ApplicationInvariantIsNotInferred` | Store logically inconsistent arbitrary bytes without an application check; tests do not misrepresent filesystem ACID as automatic business-rule validation. |

### 7.11. LAY — Extent mapping, small files, and partial updates

**Requirements:** FSD §10,§15. **Invariants:** INV-01,INV-02,INV-09,INV-16.

| Test ID | Level | Test name | What it tests / required assertion |
|---|---|---|---|
| LAY-001 | U | `InlineThresholdBoundaries` | Exercise sizes just below, at, and above the configured inline threshold; contents and metadata remain correct across representation changes. |
| LAY-002 | S | `InlineHonorsStrongerPolicy` | Select a file policy stronger than metadata placement; inline storage is rejected or supplemented so acknowledged durability is not weakened. |
| LAY-003 | U | `SmallFilesArePackedIndependently` | Pack many tiny objects in one segment; updating or deleting one cannot alter any other object's bytes or identity. |
| LAY-004 | U | `ExtentBoundaryWrites` | Write immediately before, at, and across extent boundaries; unaffected prefixes/suffixes remain exact. |
| LAY-005 | U | `SparseMappingRoundTrip` | Generate alternating holes and data extents; serialized mappings reproduce the correct logical byte stream. |
| LAY-006 | U | `OverlappingStagedWrites` | Stage overlapping writes on one handle; later writes replace only their overlapping byte ranges. |
| LAY-007 | U | `TruncateDropsMappingsSafely` | Shrink through an extent, flush, extend again; discarded ranges are neither reachable nor accidentally exposed. |
| LAY-008 | U | `InlineToExtentMigration` | Grow an inline file while an old snapshot exists; live contents migrate and the snapshot still reads its original bytes. |
| LAY-009 | U | `ExtentToInlineMigration` | Shrink a large file into inline eligibility; mapping cleanup preserves snapshots and required policy. |
| LAY-010 | S | `SmallOverwriteDoesNotResendWholeFile` | Modify one small range of a large file; instrumentation shows only changed data/mapping records, apart from explicitly measured bounded metadata overhead. |
| LAY-011 | U | `LogicalUnitsIndependentFromPacking` | Vary update size, local segment size, and network batch size; logical outputs remain identical and packing does not force segment-sized replication. |
| LAY-012 | U | `ExtentCoalescingPreservesBytes` | Coalesce adjacent mappings and split them again; logical byte ranges, holes, and snapshots remain unchanged. |
| LAY-013 | U | `CompressionRangeAccess` | Read a small range in compressed data; decode only the necessary configured compression units and verify correct boundaries. |
| LAY-014 | U | `ObjectIdentityIndependentOfLocation` | Move replicas between nodes; inode mappings remain valid without changing object identity. |
| LAY-015 | U | `MetadataTreePathCopy` | Change one leaf mapping; old roots remain readable and only necessary new mapping records are published. |
| LAY-016 | S | `MappingObjectsNeedDurability` | Lose a newly referenced mapping node while payloads are durable; commit is blocked until the mapping's durability requirement is met. |
| LAY-017 | U | `ManyExtentsStayBounded` | Read/write an adversarially fragmented file; iterators and staging obey memory limits rather than materializing the whole mapping tree. |
| LAY-018 | U | `UntrustedEncodingLengthsRejected` | Deserialize negative, overflowing, inconsistent, or unsupported encoded ranges; fail safely without invalid allocations or reads. |
| LAY-019 | S | `ExtentRetryIsIdempotent` | Repeat interrupted object uploads; the same immutable identity cannot acquire different contents or conflicting checksums. |
| LAY-020 | U | `DedupScopeCannotLeakData` | When optional deduplication is enabled, scope identities and access checks so that another principal cannot infer/read unauthorized content; otherwise assert the feature is absent. |

### 7.12. REP — Replication policy, zones, and placement

**Requirements:** FSD §9,§10.3. **Invariants:** INV-01,INV-08,INV-11,INV-18.

| Test ID | Level | Test name | What it tests / required assertion |
|---|---|---|---|
| REP-001 | U | `InvalidReplicaPolicyRejected` | Reject zero/negative counts, acknowledgement count above target, impossible distinct-domain counts, and unknown policy references. |
| REP-002 | U | `TargetAndAcknowledgedCopiesDiffer` | Configure target three and ack two; success certifies two durable copies while the third is explicitly reported pending. |
| REP-003 | S | `AllCopiesAckBlocksOnMissingNode` | Configure ack three with only three eligible targets; one offline target prevents success instead of silently changing ack to two. |
| REP-004 | S | `ZoneAcknowledgementCountEnforced` | Receive two disk acknowledgements in one zone when two zones are required; do not count them as two failure domains. |
| REP-005 | U | `HostAndDiskIdentityNotDoubleCounted` | Register two devices/roles on one host; count copies/domains according to topology, not endpoint count. |
| REP-006 | U | `WitnessNotADataReplica` | Add a voting witness without payloads; it contributes no data durability acknowledgement. |
| REP-007 | U | `BackupNotALiveReplica` | Add a fully synchronized backup; it still does not satisfy a live replica/zone requirement unless explicitly reconfigured through recovery policy. |
| REP-008 | U | `PlacementIsDeterministic` | Identical object ID, policy, and map epoch select identical eligible placement regardless of node iteration order. |
| REP-009 | U | `PlacementUsesEligibleCapacity` | Exclude full, draining, wrong-tier, or disallowed-domain nodes; do not fall back to forbidden placements silently. |
| REP-010 | S | `TwoPlusOneZoneQuorumAsymmetry` | Place three metadata voters 2+1 across two zones; test loss of either zone separately and assert only the majority-surviving case can progress. |
| REP-011 | S | `ThreeZoneSingleZoneLoss` | Place three voters one per zone with ack-two data; lose each zone in turn and continue only where actual required data/metadata survive. |
| REP-012 | S | `ReceiptBindsObjectAndPayload` | Replay an acknowledgement for another object, checksum, length, or encoding; reject it as proof for this object. |
| REP-013 | S | `ReceiptBindsNodeAndEpoch` | Replay a receipt from an obsolete node incarnation or invalid placement epoch; it cannot satisfy the new commit policy without explicit validation. |
| REP-014 | S | `PolicyUpgradeWaitsForMigration` | Upgrade a file from weaker to stronger durability; do not advertise the stronger effective policy before its object closure meets it. |
| REP-015 | S | `PolicyDowngradeRetainsSafeCopies` | Lower target count through authorized transition; delete excess copies only after required metadata and remaining durability are safe. |
| REP-016 | S | `RebalanceCopiesBeforeDelete` | Move placement under foreground reads/writes; old recoverable copies remain until replacements are verified and transitions committed. |
| REP-017 | S | `TopologyChangeDuringWrite` | Change zone labels/map epoch while staging; commit uses a valid pinned/migrated policy, never a mixture that falsely satisfies distinct zones. |
| REP-018 | S | `FixedTargetBarrierDoesNotShrink` | Wait for a named target set, then remove/disconnect a target; barrier fails/waits according to its fixed contract, not an implicit smaller set. |
| REP-019 | S | `UnrelatedNodeDoesNotBlockWrite` | Take unrelated storage groups offline; operations whose required metadata/data targets remain healthy continue. |
| REP-020 | S | `RepairRestoresActualTarget` | After a failure, repair reconstructs the full configured replica/domain target where capacity permits, not merely a metadata counter. |

### 7.13. CON — Consensus, authority, and metadata recovery

**Requirements:** FSD §§3.2,7,8,13. **Invariants:** INV-01,INV-03,INV-04,INV-05,INV-11,INV-18.

| Test ID | Level | Test name | What it tests / required assertion |
|---|---|---|---|
| CON-001 | M | `ElectionSafety` | Explore allowed election interleavings; no term/configuration has conflicting authoritative leaders under the protocol's safety model. |
| CON-002 | S | `ThreeVoterMajority` | With three voters, two connected eligible voters can progress; one alone cannot commit new metadata. |
| CON-003 | S | `FiveVoterMajority` | With five voters, require three; two disconnected survivors never advertise successful authoritative changes. |
| CON-004 | S | `PersistTermAndVoteBeforeReply` | Crash around vote/term persistence and vote response; reboot cannot vote inconsistently due to an unpersisted reply. |
| CON-005 | S | `CommittedLogSurvivesLeaderChange` | Commit mutations, replace the leader, and read; committed entries and outcome records are preserved. |
| CON-006 | S | `UncommittedSuffixNotExposed` | Crash a leader with a locally appended but uncommitted suffix; it is not presented as committed state during recovery. |
| CON-007 | S | `LeaderLocalAppendNotSuccess` | Block replication after local journal append; no client success is returned without the configured metadata quorum decision. |
| CON-008 | S | `StaleLeaderReadRejected` | Isolate an old leader while a majority commits new data; it cannot label its stale revision as a new live linearizable read. |
| CON-009 | S | `ReadWaitsForAppliedIndex` | Commit a decision but delay state-machine apply on the serving replica; a read requiring that boundary waits or reroutes. |
| CON-010 | S | `DuplicateLogDeliveryAppliedOnce` | Duplicate/reorder allowed replication messages; logical mutations and request outcomes are not applied twice. |
| CON-011 | S | `LogConflictReconcilesSafely` | Reconnect a follower with divergent uncommitted entries; reconcile the suffix without removing committed history. |
| CON-012 | S | `ConsensusSnapshotIncludesSafetyState` | Install a metadata snapshot containing versions, epochs, dedup state, pins, and reservations; replay suffix operations preserves all safety invariants. |
| CON-013 | S | `SnapshotInstallInterrupted` | Crash at every metadata snapshot-install boundary; restart selects one complete recoverable state, not mixed old/new files. |
| CON-014 | S | `ConfigurationTransitionQuorums` | Exercise the selected safe membership-change protocol under partitions; neither an old-only nor new-only invalid quorum can split authority. |
| CON-015 | S | `NewReplicaCannotVotePrematurely` | Join an empty replacement; require protocol-defined catch-up/admission before it becomes an eligible voter. |
| CON-016 | S | `RemovedNodeCannotRejoinByOldDisk` | Restart a removed member with its old disk and configuration; reject stale authority and require explicit re-admission. |
| CON-017 | S | `ControlGroupUnavailable` | Lose control quorum but keep established volume groups healthy; permitted existing-epoch operations continue while configuration mutations fail explicitly. |
| CON-018 | S | `VolumeGroupsAreIndependent` | Lose one volume's quorum; another healthy volume continues and no cluster-wide majority requirement is invented. |
| CON-019 | S | `BootstrapWithoutFilesystemRecursion` | Start a cold cluster from local superblocks/trust data; metadata recovery never requires opening its own public filesystem paths. |
| CON-020 | S | `AllNodesRestartFromDurableImages` | Cold-restart all nodes in different orders without memory caches; recover acknowledged commits or explicitly report unavailable missing durability rather than fabricate state. |

### 7.14. NET — Connection faults and transport behavior

**Requirements:** FSD §§5,6,10,12,13. **Invariants:** INV-01,INV-02,INV-03,INV-06,INV-08,INV-17.

| Test ID | Level | Test name | What it tests / required assertion |
|---|---|---|---|
| NET-001 | S | `RequestDroppedBeforeDelivery` | Drop a request before the server sees it; retry can execute once and the client never treats non-delivery as a completed success. |
| NET-002 | S | `RequestDeliveredReplyDropped` | Deliver and commit a mutation but lose its reply; report/resolve uncertainty and preserve exactly one logical mutation. |
| NET-003 | S | `HalfOpenConnection` | Make an established socket stop delivering without closing; deadline and retry logic terminate or reroute without hanging indefinitely. |
| NET-004 | S | `ResetDuringObjectUpload` | Reset connections at header, payload-chunk, checksum, and finalization boundaries; no incomplete object receives a valid durable receipt. |
| NET-005 | S | `ResetDuringRead` | Interrupt a read after metadata resolution and between data chunks; retries stay on the selected revision and integrity is preserved. |
| NET-006 | S | `PartialProtocolFrame` | Deliver every split of small frames and sampled splits of large frames; parser either reconstructs exactly or rejects safely. |
| NET-007 | S | `TruncatedFrameAtEof` | Close after a partial header/payload; report transport/protocol failure, never a valid short file or EOF. |
| NET-008 | S | `DuplicateMutationRpc` | Duplicate a whole application request with the same ID over separate connections; its logical effect occurs once. |
| NET-009 | S | `DelayedOldReply` | Deliver an old response after retry/new request completion; correlation IDs prevent it from completing the wrong operation. |
| NET-010 | S | `OutOfOrderResponses` | Complete concurrent independent requests out of order; results, offsets, and tokens reach their correct callers. |
| NET-011 | S | `AsymmetricPartition` | Break one direction of selected node links; consensus and acknowledgements count completed protocol exchanges, not apparent one-way reachability. |
| NET-012 | S | `IntermittentFlapping` | Repeatedly connect/disconnect client and replica links; safe retries progress after stability without duplicate commits or unbounded retry queues. |
| NET-013 | S | `LatencyAndBandwidthAsymmetry` | Make one replica slow and another fast; meet policy without false acknowledgements and enforce backpressure on outstanding requests. |
| NET-014 | S | `DnsAndEndpointChange` | Change resolved endpoints during a connection failure; validate identity on reconnection and prevent routing to a different cluster. |
| NET-015 | S | `TlsFailureMidHandshake` | Fail certificate validation or handshake; no unauthenticated application request or retry bypass is allowed. |
| NET-016 | S | `IdlePoolConnectionDies` | Kill an idle pooled connection and reuse it; recover according to deadline without exposing a stale request outcome as the new one. |
| NET-017 | S | `CancellationRaceWithReply` | Schedule cancellation immediately before/after a commit response; select success, definite abort, or unknown consistently with actual evidence. |
| NET-018 | S | `ReconnectAfterSessionExpiry` | Resume an old client session after its reservations and request window expire; reject stale authority and expired operation IDs. |
| NET-019 | S | `BatchPartiallyAcknowledged` | Drop the connection after only some batched object receipts; count only valid durable receipts and do not publish a partial higher-level operation. |
| NET-020 | S | `EveryOperationEveryReachableStage` | Expand the operation/stage/fault matrix defined below; verify data, namespace, outcome, pin, reservation, and recovery invariants for each generated case. |

### 7.15. OUT — Idempotency, cancellation, and uncertain outcomes

**Requirements:** FSD §13,§6.4. **Invariants:** INV-01,INV-02,INV-06,INV-08.

| Test ID | Level | Test name | What it tests / required assertion |
|---|---|---|---|
| OUT-001 | U | `OperationIdAssignedBeforeSend` | Observe first transmission of every mutating API; it already carries a stable caller-recoverable operation ID. |
| OUT-002 | S | `SameIdSamePayloadSameOutcome` | Retry before, during, and after original completion; return or resolve one durable outcome, not execute twice. |
| OUT-003 | S | `SameIdDifferentPayloadRejected` | Reuse an accepted ID for different bytes, path, options, or transaction descriptor; reject the mismatch rather than return misleading success. |
| OUT-004 | S | `SameIdDifferentPrincipalRejected` | Reuse another principal's ID; authorization and namespace binding prevent outcome disclosure or cross-user replay. |
| OUT-005 | S | `ConcurrentRetriesCoalesce` | Send the same mutation concurrently through several endpoints; commit at most one logical effect and report compatible outcomes. |
| OUT-006 | S | `AppendReplyLossDoesNotDuplicate` | Commit append, lose response, retry/restart client with saved ID; append contents appear exactly once. |
| OUT-007 | S | `CreateRetryAfterNameReuse` | Commit create, later delete/recreate the name, then retry the original ID; do not modify the new incarnation. |
| OUT-008 | S | `DeleteRetryAfterNameReuse` | Commit delete, create a new file at the path, retry original delete; the new inode survives. |
| OUT-009 | S | `RenameRetryAfterFurtherMoves` | Commit a rename and subsequently move/recreate names; retry resolves the original event rather than executing against current occupants. |
| OUT-010 | S | `ClientCrashCanRecoverSavedId` | Crash client after sending; a new client using persisted ID can query the durable decision. |
| OUT-011 | S | `OutcomeSurvivesConsensusCompaction` | Compact/restart metadata after commit; retained ID outcomes still resolve correctly. |
| OUT-012 | S | `ExpiredIdCannotBecomeNewRequest` | After outcome retention expires, replay the ID; server rejects expired/retired sequence rather than accepting it as unseen. |
| OUT-013 | S | `DefiniteAbortPreventsLateCommit` | Obtain an authoritative abort/no-commit decision, then release delayed old requests; they cannot later commit that operation. |
| OUT-014 | S | `PendingIsNotNotFound` | Query an in-flight/indeterminate operation; distinguish unresolved/pending from an authoritative absent-or-aborted outcome. |
| OUT-015 | U | `UnknownExceptionCarriesIdentity` | Every unknown-outcome exception includes enough operation/transaction identity for later resolution and preserves the original cause. |
| OUT-016 | S | `CancellationBeforePublication` | Confirm an operation was cancelled before publication; no delayed path can later publish under the same cancelled decision. |
| OUT-017 | S | `CancellationCannotRollBackCommit` | Cancel after durable decision; successful resolution still reports committed and contents remain durable. |
| OUT-018 | U | `CommitTokensNotGloballyOrdered` | Compare tokens across clusters, volumes, or volume epochs; reject invalid comparisons rather than compare sequence integers alone. |
| OUT-019 | S | `OutcomeWindowRetirementIsDurable` | Crash while advancing session/request retirement watermarks; recovery never re-enables an expired replay. |
| OUT-020 | S | `BarrierTimeoutPreservesLiveCommit` | Timeout a replica/backup durability barrier after live success; the live commit remains committed and its weaker/stronger guarantees are reported accurately. |

### 7.16. OFF — Offline nodes, partitions, and restart scenarios

**Requirements:** FSD §§9,13,14.1. **Invariants:** INV-01,INV-03,INV-05,INV-08,INV-11,INV-18.

| Test ID | Level | Test name | What it tests / required assertion |
|---|---|---|---|
| OFF-001 | S | `FollowerOfflineBeforeWrite` | Stop one follower with its disk preserved, then issue writes under a feasible quorum/ack policy; commits succeed and the offline node is not counted as current. |
| OFF-002 | S | `LeaderOfflineBeforeWrite` | Stop the current metadata leader; an eligible majority elects a successor and operations resume without lost acknowledged state. |
| OFF-003 | S | `DataNodeOfflineBeforeRead` | Take the preferred data replica offline; read verified bytes from another replica without changing the selected file revision. |
| OFF-004 | S | `NodeOfflineDuringEveryMutationPhase` | Stop each relevant node immediately before/after each persistence/publication step; enforce policy and outcome rules through recovery. |
| OFF-005 | S | `NodeOfflineDuringOpenAndLease` | Disconnect a node during grant/renew/release; no duplicate writable authority or permanently leaked reservation remains after safe recovery. |
| OFF-006 | S | `OneWholeZoneOffline` | Stop every role in one zone; progress depends on surviving metadata majority and data placement requirements, not merely live server count. |
| OFF-007 | S | `QuorumLostNoFalseSuccess` | Stop enough voters to lose majority; affected new live mutations fail/wait and no buffered acceptance is reported as durable commit. |
| OFF-008 | S | `DataPolicyLostWithMetadataQuorum` | Keep metadata majority but lose required data-copy domains; block new publication even though consensus itself is healthy. |
| OFF-009 | S | `MetadataQuorumLostWithAllPayloads` | Keep all extent servers healthy but lose metadata quorum; do not authorize new live reads/writes from data availability alone. |
| OFF-010 | S | `AllLiveNodesTemporarilyOffline` | Stop all live nodes while preserving disks; clients report unavailability, backup reads may continue, and restart recovers durable state. |
| OFF-011 | S | `AllLiveNodesPermanentlyLost` | Destroy all live disks; report live data unavailable and restore only from actual independent retained backup state, without inventing missing updates. |
| OFF-012 | S | `ClientOfflineWithDirtyBuffer` | Disconnect a writer before flush; it may retain private staging but cannot claim committed durability or later bypass expired authority. |
| OFF-013 | S | `ClientOfflineAfterCommitBeforeReply` | Drop client connectivity after commit; new client/session resolution preserves the acknowledged-or-unknown distinction and single logical effect. |
| OFF-014 | S | `MinorityReadNotLabelledLive` | Query an isolated minority using live-read APIs; reject or reroute rather than silently return stale metadata. |
| OFF-015 | S | `PinnedReadDuringPartition` | Read a valid pinned immutable view whose data are available; allow it only within the pin contract, explicitly historical rather than live. |
| OFF-016 | S | `OfflineNodeNotImplicitlyRemoved` | Disconnect a voting member; quorum arithmetic still uses the committed membership configuration rather than the currently reachable set. |
| OFF-017 | S | `RepeatedOfflineOnlineCycles` | Cycle each role through disconnect, pause, crash, restart, and reconnect; leases, retries, repair state, and object references converge after stable recovery. |
| OFF-018 | S | `DifferentRestartOrders` | Restart data, metadata, and control roles in all bounded orders; no bootstrap deadlock or premature authority assumption occurs. |
| OFF-019 | S | `BackupOfflineDoesNotBlockLive` | Keep the backup absent through many commits/snapshots; live correctness is unchanged and bounded history policies are enforced. |
| OFF-020 | S | `NetworkHealsButDiskIsMissing` | Reconnect a node whose device was replaced with an empty disk; treat it as missing state, not a healthy replica merely because its endpoint responds. |
| OFF-021 | S | `PauseExceedsElectionAndLeaseWindows` | Pause an old leader/writer longer than both windows, then resume with queued messages; stale tasks cannot regain authority. |
| OFF-022 | S | `OfflinePastCredentialExpiry` | Return a node/client with expired credentials; reauthentication is required without weakening cluster identity or trusting its stale state. |

### 7.17. RSY — Resynchronization, repair, and stale-state exclusion

**Requirements:** FSD §§9.2,13,14.1. **Invariants:** INV-01,INV-05,INV-06,INV-07,INV-09,INV-11,INV-18.

| Test ID | Level | Test name | What it tests / required assertion |
|---|---|---|---|
| RSY-001 | S | `ShortGapIncrementalCatchup` | Return a follower before its missing log range is compacted; replay the range once and match current authoritative state. |
| RSY-002 | S | `LongGapRequiresCheckpoint` | Return after log compaction; install a complete checkpoint plus valid suffix rather than invent missing log entries. |
| RSY-003 | S | `OfflineReplicaNeverServesNewLiveReadsEarly` | Return with stale state and delay catch-up/apply; it cannot serve a new live read until it establishes an appropriate authoritative boundary. |
| RSY-004 | S | `CatchupIncludesOverwrites` | Overwrite multiple versions while one node is offline; resync installs the authoritative current mappings, not the largest local timestamp. |
| RSY-005 | S | `CatchupIncludesDeletion` | Delete a file while the node is offline; reconnecting old contents cannot recreate the namespace entry. |
| RSY-006 | S | `CatchupIncludesTruncation` | Shrink then re-extend while offline; repaired copies retain zeroed gaps and never restore the discarded tail. |
| RSY-007 | S | `CatchupIncludesRenameAndNameReuse` | Rename/delete/recreate paths with different inode identities; resync reproduces current bindings rather than matching by old path strings. |
| RSY-008 | S | `CatchupIncludesTransactionBoundary` | Commit multi-file changes during outage; recovered metadata and reads never expose a half-applied transaction. |
| RSY-009 | S | `CatchupIncludesSnapshotsAndPins` | Create/delete snapshots during outage; recovered reachability and retention records match the authoritative history. |
| RSY-010 | S | `CatchupPreservesRequestRetirement` | Sync after dedup compaction/session retirement; old IDs remain non-replayable on the returning node. |
| RSY-011 | S | `CatchupPreservesFenceGenerations` | Sync lease loss/replacement during outage; the returning node rejects old writers and does not reset generations. |
| RSY-012 | S | `ResyncInterruptedAtEveryStage` | Disconnect or crash during checkpoint receive, verification, installation, suffix replay, and activation; retry safely from complete durable progress. |
| RSY-013 | S | `SecondLeaderFailureDuringResync` | Fail the catch-up source/leader partway through; another source resumes against a valid boundary without combining inconsistent histories. |
| RSY-014 | S | `ConcurrentWritesDuringCatchup` | Continue writes while a node catches up; it applies a valid checkpoint-plus-suffix and eventually converges after a stable catch-up window. |
| RSY-015 | S | `ResyncChecksumMismatch` | Corrupt transferred metadata/data; reject bad records, retry verified sources, and never activate an incomplete/corrupt image. |
| RSY-016 | S | `RepairFromOneVerifiedSurvivor` | Lose all but one usable payload copy while metadata remains recoverable; reconstruct additional copies from the verified survivor without overstating write availability. |
| RSY-017 | S | `NoHealthySourceForObject` | Make every required copy missing/corrupt; report explicit unavailability/integrity failure and never fill missing content with zeroes. |
| RSY-018 | S | `StaleObjectsAreNotAuthoritative` | Return old immutable objects with matching paths but obsolete IDs; placement repair cannot change authoritative inode roots. |
| RSY-019 | S | `RepairWithPolicyChange` | Change required zones while repair runs; completion and old-copy deletion use a valid transition epoch and effective policy. |
| RSY-020 | S | `RemovedNodeReappears` | Reconnect a removed or replaced identity with old data; it cannot vote, publish receipts as the replacement, or influence current membership without re-admission. |
| RSY-021 | S | `DiskCloneNodeIdCollision` | Start two nodes from the same copied disk identity; detect/fence duplicate incarnations rather than count them as independent replicas. |
| RSY-022 | S | `ResyncDiskFull` | Fill destination during catch-up; retain the last valid recoverable state, report inability to complete, and avoid false health/replica counts. |
| RSY-023 | S | `RepairRateLimited` | Run repair against heavy foreground traffic; honor configured repair budgets and measure forward progress after capacity/bandwidth become sufficient. |
| RSY-024 | S | `NoInfinitePingPongRepair` | Alternate corruption/availability announcements in a bounded sequence; after faults stop, placement stabilizes without endlessly copying/deleting the same healthy replicas. |
| RSY-025 | S | `AntiEntropyFindsSilentDivergence` | Inject a missed background replication job or silent local corruption; inventory/scrubbing finds the missing/bad copy and repairs it from authoritative references. |
| RSY-026 | S | `CheckpointDoesNotRollbackCurrentState` | Deliver a delayed older checkpoint after newer state is installed; reject it or use it only in an explicitly safe recovery path, never silently roll back live state. |

### 7.18. DSK — Local persistence, corruption, and crash consistency

**Requirements:** FSD §§10.3,13,14. **Invariants:** INV-01,INV-02,INV-09,INV-14.

| Test ID | Level | Test name | What it tests / required assertion |
|---|---|---|---|
| DSK-001 | I | `JournalAppendCrashMatrix` | Crash before/after each journal write and durability barrier; recovery produces an allowed complete prefix and no false acknowledged commit. |
| DSK-002 | I | `TornRecordDetection` | Tear headers, payloads, trailers, and checksum fields; recovery detects incomplete/corrupt records rather than interpreting them as valid data. |
| DSK-003 | I | `ShortWriteHandling` | Return short local writes without errors; persistence loops complete them or fail and never acknowledge the intended full object prematurely. |
| DSK-004 | I | `FlushErrorPropagates` | Return I/O error from file/device flush; no durable receipt or successful dependent commit is emitted. |
| DSK-005 | I | `EnospcAtEveryLocalStep` | Exhaust space during data, journal, index, checkpoint, or compaction writes; preserve valid old state and reserved recovery capacity. |
| DSK-006 | I | `MetadataDiskFullSeparateFromData` | Fill metadata storage while payload devices remain free; new metadata publication fails correctly and cannot be bypassed by payload receipts. |
| DSK-007 | I | `ReadIoErrorFailsOver` | Return a read error from one replica; try another verified copy, or return an explicit error if none exists. |
| DSK-008 | I | `PayloadBitFlipDetected` | Flip bytes in a stored object; checksum validation rejects them before successful read delivery. |
| DSK-009 | I | `IdentityMetadataCorruptionDetected` | Alter object ID, length, encoding, or mapping metadata; integrity coverage prevents misdirected reads despite intact payload bytes. |
| DSK-010 | I | `AllReplicasCorruptIsNotSuccess` | Corrupt every copy of an extent; reads fail explicitly instead of returning whichever bytes happen to be available. |
| DSK-011 | I | `InterruptedCompactionPreservesReferences` | Crash while copying records, publishing the new index, and deleting the old segment; one complete valid layout remains recoverable. |
| DSK-012 | I | `CheckpointAtomicInstallation` | Crash around temporary checkpoint writes, flushes, pointer/rename update, and directory persistence; no partially installed checkpoint is selected. |
| DSK-013 | I | `UnflushedWritesMayPersistOrDisappear` | Explore both legal storage outcomes for unacknowledged writes; the protocol works without assuming all such bytes disappear or all survive. |
| DSK-014 | I | `DirectoryEntryDurability` | Exercise required local directory/superblock persistence after file creation/rename; process restart and cold restart do not lose acknowledged storage structures. |
| DSK-015 | I | `ReadOnlyDeviceTransition` | Switch a device to read-only while serving; fail new durability work safely and continue verified reads where possible. |
| DSK-016 | I | `LatentCorruptionScrubRepair` | Corrupt an unread object, run scrubbing, and verify detection/quarantine/repair before marking it healthy. |
| DSK-017 | I | `SegmentReuseDoesNotLeakOldData` | Delete/compact and reuse physical space across files/principals; reads, holes, and failed allocations do not expose previous tenants' bytes. |
| DSK-018 | I | `LocalEngineReopenIsSelfContained` | Remove process caches and reopen from durable files; indexes can be recovered/rebuilt from documented storage metadata. |
| DSK-019 | H | `ColdPowerLossDurability` | On supported real storage stacks, interrupt power after successful durable commits; surviving hardware within the declared contract recovers exact acknowledged data. |
| DSK-020 | H | `FilesystemAndDeviceMatrix` | Repeat crash/flush/rename/recovery tests on each supported local filesystem, kernel, device cache, and persistence configuration; record unsupported combinations explicitly. |

### 7.19. SNP — Snapshots, exact recovery points, and shadow copies

**Requirements:** FSD §11. **Invariants:** INV-02,INV-04,INV-09,INV-10,INV-13,INV-19.

| Test ID | Level | Test name | What it tests / required assertion |
|---|---|---|---|
| SNP-001 | S | `SnapshotCapturesOneCommitBoundary` | Race snapshot creation with many ordinary and transactional commits; every file resolves at one valid volume boundary. |
| SNP-002 | S | `SnapshotExcludesPrivateBuffers` | Create a snapshot while clients have unflushed writes; it contains only previously committed bytes. |
| SNP-003 | S | `SnapshotExcludesUncommittedTransaction` | Snapshot during transaction staging; no partial or uncommitted transaction state is included. |
| SNP-004 | U | `SnapshotIsImmutable` | Try every mutating file/directory/stream API through a snapshot view; reject before any visible mutation. |
| SNP-005 | S | `CopyOnWritePreservesOldVersions` | After snapshot creation, overwrite/truncate/delete live files; historical bytes and namespace stay readable. |
| SNP-006 | S | `SnapshotCreateReplyLost` | Commit snapshot creation and lose its reply; retry produces the same snapshot identity rather than duplicate retention references. |
| SNP-007 | S | `MinimumCommitBoundaryRespected` | Request a snapshot at least as new as token T; return a valid same-epoch boundary at or after T, never older. |
| SNP-008 | S | `MinimumCommitIsNotExactHistory` | Overwrite after T before snapshot capture; a later snapshot may contain the overwrite and must not claim to preserve the exact T version. |
| SNP-009 | S | `CommitAndSnapshotPreservesExactVersion` | Atomically commit plus snapshot, then immediately overwrite/delete; the named snapshot retains the exact commit's full state. |
| SNP-010 | U | `ForeignEpochTokenRejected` | Request a minimum boundary from another volume/authority epoch; reject invalid ordering or retention claims. |
| SNP-011 | S | `DeleteSnapshotRespectsOtherReferences` | Delete one snapshot sharing extents with live state, another snapshot, readers, and transfers; no remaining reference is reclaimed. |
| SNP-012 | S | `DeleteSnapshotWithActiveReader` | Delete its retention reference while a reader has a valid pin; honor the reader pin until explicit close/expiry. |
| SNP-013 | S | `SnapshotDeletionReplyLost` | Retry a committed delete; do not decrement references twice or delete an unrelated recreated snapshot name. |
| SNP-014 | S | `SnapshotManifestMissingDependency` | Remove a required mapping/data object; integrity/completeness validation fails and does not label the snapshot fully recoverable. |
| SNP-015 | S | `ManySnapshotsBoundedMetadataWork` | Create many snapshots and measure metadata/pinning overhead; no full payload copy is required by the capture operation. |
| SNP-016 | S | `SnapshotSurvivesColdRestart` | Restart all source nodes after snapshot acknowledgement; historical namespace, objects, labels, and retention state survive. |
| SNP-017 | S | `SnapshotRestoreCreatesNewVolume` | Restore to a new volume; source snapshot remains unchanged and new volume receives distinct valid authority identity. |
| SNP-018 | S | `SnapshotScopeIsOneVolume` | Attempt atomic snapshot across volumes in the core API; reject or require explicitly separate snapshots without a false global-atomicity claim. |

### 7.20. APP — Application-consistent snapshots and adapters

**Requirements:** FSD §11.3. **Invariants:** INV-04,INV-10,INV-13,INV-17,INV-19.

| Test ID | Level | Test name | What it tests / required assertion |
|---|---|---|---|
| APP-001 | U | `DefaultLabelIsFilesystemConsistent` | Create without adapters; report filesystem consistency only, not automatic application consistency. |
| APP-002 | I | `PrepareCaptureResumeOrder` | Instrument application adapter callbacks; successful prepare/checkpoint precedes snapshot boundary and resume occurs before remote transfer. |
| APP-003 | S | `NoPauseDuringBackupTransfer` | Delay the remote backup indefinitely after source capture; application writes resume and continue independently. |
| APP-004 | I | `PrepareFailureAbortsLabelledCapture` | Fail one adapter's prepare; no snapshot is labelled ApplicationConsistent under an incomplete adapter set. |
| APP-005 | I | `AdapterTimeoutReleasesPause` | Hang prepare or capture until the bounded timeout; cleanup attempts every required resume/abort callback and surfaces failures. |
| APP-006 | I | `ResumeFailureIsVisible` | Throw from resume after a source snapshot was created; preserve the recoverable snapshot/outcome and explicitly report the application cleanup problem. |
| APP-007 | I | `MultipleAdaptersAllMustSucceed` | Require several cooperating applications; publish the stronger label only when their declared preparation contract succeeds together. |
| APP-008 | I | `AdapterMetadataRetained` | Persist adapter IDs, versions, log/checkpoint requirements, and scope in source and backup manifests for restoration. |
| APP-009 | I | `FilesystemSnapshotMayNeedRecovery` | Use a controlled log-based sample application; restore a filesystem-consistent snapshot and replay its valid log to a recoverable state. |
| APP-010 | I | `NontransactionalApplicationIntermediateState` | Snapshot between two unrelated application writes; demonstrate it may capture an application-invalid intermediate state and is not labelled automatically consistent. |
| APP-011 | I | `ApplicationConsistencyWithExplicitTransaction` | Repeat the same multi-file update inside a filesystem transaction; snapshots contain both changes or neither. |
| APP-012 | I | `AdapterStorageInterfaceCompatibility` | Verify each adapter really uses the provided storage contract; do not claim a native-path database works merely because Stream is available. |

### 7.21. BAK — Late-sync replica transfer, read-only service, and retention

**Requirements:** FSD §12. **Invariants:** INV-01,INV-09,INV-10,INV-13,INV-15,INV-19.

| Test ID | Level | Test name | What it tests / required assertion |
|---|---|---|---|
| BAK-001 | S | `BackupNotInConsensusMembership` | Register a backup target; inspect every live voter/configuration state and confirm the target is not added. |
| BAK-002 | S | `BackupDoesNotGrantLocks` | Attempt lock/lease operations through the backup role; no live writer authority or reservation can be issued. |
| BAK-003 | S | `BackupNotInOrdinaryAckSet` | Block/kill the backup during live flush; normal live acknowledgements depend only on live policy. |
| BAK-004 | S | `InitialSyncHasNoPartialPublishedTree` | Begin first transfer and read before closure verification; expose no incomplete snapshot as a valid published version. |
| BAK-005 | S | `IncrementalTransferSendsMissingObjects` | Create a second snapshot with small changes; transfer new required objects and metadata while retaining unchanged verified objects. |
| BAK-006 | S | `ManifestBeforeDataNotPublished` | Deliver manifest/root first and delay a referenced object; snapshot remains unpublishable until the full dependency closure is durable. |
| BAK-007 | S | `FalseInventoryPositiveCannotOmitData` | Make an inventory/Bloom-filter hint claim a missing object exists; final completeness verification finds the gap and requests it. |
| BAK-008 | S | `InterruptedTransferKeepsPreviousSnapshot` | Interrupt every transfer phase; the prior published snapshot remains readable and unchanged. |
| BAK-009 | S | `TransferResumeIsIdempotent` | Reconnect after partial receive/verification; already verified immutable objects are reusable and duplicates do not corrupt reference counts. |
| BAK-010 | S | `CorruptTransferObjectRejected` | Alter bytes, object identity, encoding, or manifest references in transit; reject/repair and never publish a corrupt snapshot. |
| BAK-011 | S | `BackupDiskFullPreventsPublication` | Fill destination before all dependencies are durable; keep its last complete published snapshot and report backup failure. |
| BAK-012 | S | `PublishPointerCrashAtomicity` | Crash immediately before/after durable publication-pointer replacement; restart exposes a complete previous or complete new snapshot only. |
| BAK-013 | S | `PinnedBackupReaderStaysOnOldSnapshot` | Read S1 while S2 finishes and publishes; the existing reader stays on S1 while new readers may select S2. |
| BAK-014 | S | `BackupReadableWithoutSource` | Disconnect all source nodes after completing a snapshot; metadata and contents are fully readable using destination-local authority/pins. |
| BAK-015 | U | `AllMutationsOnBackupDenied` | Attempt creating/truncating opens, writes, transactions, rename, delete, and policy mutations through read-only client credentials; reject each. |
| BAK-016 | S | `ReceivedDurablePublishedProgressSeparated` | Delay persistence or publication independently; status counters/tokens reflect the actual three stages rather than reporting arrived bytes as recoverable state. |
| BAK-017 | S | `AwaitCommitBoundaryReturnsActualCheckpoint` | Await token T and publish a later complete same-epoch snapshot; return that snapshot's identity and actual retention guarantee. |
| BAK-018 | S | `AwaitExactSnapshotRequiresThatSnapshot` | Publish later S2 after S1 was requested; do not satisfy the exact-S1 barrier unless S1 itself is durably retained. |
| BAK-019 | S | `BackupBarrierTimeoutDoesNotUndoLive` | Lose backup connectivity after live commit; timeout preserves the committed live state and reports no remote guarantee. |
| BAK-020 | S | `BackupAcknowledgementIsDurable` | Crash destination after backup acknowledgement and before any graceful shutdown; promised snapshot and retention state are recoverable. |
| BAK-021 | S | `SourceDeletionDoesNotEraseProtectedHistory` | Delete source data/snapshots and run mirroring; independently protected destination snapshots remain retained. |
| BAK-022 | S | `ExporterCannotDeleteProtectedBackup` | Use the source's export credentials against deletion/retention APIs; authorization denies protected-history removal. |
| BAK-023 | S | `OfflinePastPinBudgetTriggersReseed` | Keep target offline beyond source age/capacity budget; bound source pins, preserve destination's last complete snapshot, and explicitly reseed. |
| BAK-024 | S | `ReseedInterruptedPreservesHistory` | Fail a full reseed repeatedly; old destination recovery points remain intact until independently eligible for expiry. |
| BAK-025 | S | `IndependentTargetsHaveIndependentLag` | Operate one fast and one offline backup; their queues, retention, barriers, and live-load budgets remain independent. |
| BAK-026 | S | `OldEpochBackupStreamRejected` | After source restore/promotion, deliver old-epoch exports; reject mixing their namespace/commit sequence into the new lineage. |
| BAK-027 | S | `BackupPinVersusRetentionRace` | Expire destination retention while opening/reading a snapshot; acquire a valid local pin or fail, never reclaim under a valid reader. |
| BAK-028 | S | `SlowBackupBoundedResourceUse` | Throttle backup to near-zero bandwidth; source staging, queue length, and pin budgets remain bounded without blocking unrelated live commits. |
| BAK-029 | S | `BackupCredentialExpiryIsNotPromotion` | Expire its source access or partition it; backup remains read-only and its historical reads use the documented local authorization policy. |
| BAK-030 | S | `ExportNotRawUncommittedBlocks` | Stage an aborted transaction on live storage; no published backup version references its uncommitted namespace or data changes. |

### 7.22. RST — Disaster recovery, recovery keys, and promotion

**Requirements:** FSD §§11.1,12.4–12.5,14. **Invariants:** INV-08,INV-10,INV-12,INV-13,INV-19.

| Test ID | Level | Test name | What it tests / required assertion |
|---|---|---|---|
| RST-001 | I | `RestoreWithAllSourceNodesAbsent` | Destroy access to source services and disks; rebuild a new live volume using only retained backup plus documented recovery material. |
| RST-002 | I | `RestoreExactNamespaceAndBytes` | Compare every restored file, directory, length, sparse range, metadata field, and applicable policy against the selected snapshot's independent manifest. |
| RST-003 | I | `RestoreTransactionsWhole` | Inspect related files from committed transactions; restored state includes complete transactions through the chosen boundary. |
| RST-004 | I | `RestoreNoLaterUnreplicatedGuarantee` | Commit additional source writes that never reached backup, lose source, restore; report the actual recovery boundary and do not claim those writes survived. |
| RST-005 | I | `MissingKeyFailsClearly` | Remove required recovery key material; fail restoration explicitly and do not describe encrypted bytes alone as a usable backup. |
| RST-006 | I | `KeyRotationHistoricalRestore` | Rotate keys, then restore old and new retained snapshots with supported key versions; retain every still-required decryption path. |
| RST-007 | S | `RestoreInterruptionIsResumable` | Interrupt object import, metadata install, and activation; restart keeps the source backup intact and does not expose an incomplete new live volume. |
| RST-008 | S | `RestoreUsesNewAuthorityEpoch` | Activate a new volume/cluster from historical data; old tokens, sessions, leases, and operation IDs cannot mutate the new authority incorrectly. |
| RST-009 | S | `BackupNeverAutoPromotesOnPartition` | Remove source connectivity for arbitrarily long bounded runs; no election or timeout makes the backup writable. |
| RST-010 | I | `PromotionRequiresOldAuthorityFencing` | Attempt same-identity recovery while the old cluster can still write; require the specified external fencing/new identity procedure before activation. |
| RST-011 | S | `OldSourceReturnsAfterPromotion` | Reconnect the old source after explicit recovery; reject old-epoch writes/replication rather than merge divergent live histories. |
| RST-012 | I | `RetentionExpiryDisclosed` | Restore/query near retention expiry; report which exact recovery points are still held and fail precisely when requested history is no longer available. |
| RST-013 | I | `BackupHostItselfFails` | Lose the single backup's only disk; report its loss accurately and do not imply the backup role is itself replicated. |
| RST-014 | I | `RecoveryRunbookWorksFromCleanHost` | Follow documented recovery on an otherwise empty supported host, including binaries, certificates, keys, manifests, and identity initialization; record any undeclared source dependency. |
| RST-015 | I | `RecoveredClusterCanOperateAndBackUp` | After restore, create/modify/snapshot/replicate new data under the new epoch; history and new commits remain distinguishable and coherent. |
| RST-016 | I | `ApplicationRestoreRequirementsEnforced` | For ApplicationConsistent snapshots, invoke required recovery adapters/log steps and verify their reported outcomes before declaring application recovery complete. |

### 7.23. GC — Garbage collection, references, and capacity reserves

**Requirements:** FSD §§10.3,11,12.4,14. **Invariants:** INV-01,INV-09,INV-10,INV-16.

| Test ID | Level | Test name | What it tests / required assertion |
|---|---|---|---|
| GC-001 | S | `CurrentRootObjectsNeverCollected` | Run collection concurrently with reads/writes; every object reachable from an authoritative current root remains available. |
| GC-002 | S | `SharedSnapshotReferencesSafe` | Share extents among many snapshots and live versions; removing one reference cannot reclaim an object still reachable elsewhere. |
| GC-003 | S | `ReaderPinProtectsHistoricalData` | Replace/delete a file and run collection while a valid old-reader pin exists; reads remain safe until pin release/expiry. |
| GC-004 | S | `StagingSessionProtectsUploads` | Pause between durable upload and metadata commit; collection does not remove data belonging to a live staging session. |
| GC-005 | S | `UnknownTransactionIsNotAbortedByAge` | Lose a commit response and delay outcome resolution; elapsed time alone cannot authorize collection of possibly committed objects. |
| GC-006 | S | `StagingToLiveTransitionAtomic` | Crash between reference promotion and metadata publication; recovery cannot create a committed dangling root or permanently lose staged-to-live ownership. |
| GC-007 | S | `AbortThenDelayedUploadSafe` | Abort a staging session then receive old uploads; data are rejected or remain unreachable reclaimable orphans, not published content. |
| GC-008 | S | `SnapshotCreationRacesCollection` | Create a snapshot during tracing/reference cleanup; the captured root's required objects cannot be reclaimed between capture and protection. |
| GC-009 | S | `BackupTransferPinProtectsClosure` | Keep a valid transfer in progress while live files change/delete; source objects remain accessible until completion or safe transfer abandonment. |
| GC-010 | S | `TransferAbandonmentIsFenced` | Expire the source transfer budget, release pins, then deliver old requests; the stale transfer cannot publish an incomplete destination snapshot. |
| GC-011 | S | `RebalancePinsOldAndNewCopies` | Collect while placement transitions; retain required old copies until verified replacement and transition completion. |
| GC-012 | S | `ReferenceUpdatesIdempotent` | Replay commit, snapshot-delete, abort, and retry events; reference accounting does not underflow or double-release. |
| GC-013 | S | `CrashDuringSweep` | Crash between mark/trace checkpoints and object deletion; recovered collection resumes without using invalid stale reachability information. |
| GC-014 | S | `CrashDuringCompactionAndGc` | Interleave compaction relocation with reachability deletion; indexes never reference reclaimed or wrong objects. |
| GC-015 | S | `QuotaCountsPinnedAndStagedBytes` | Exhaust space with snapshots, transfers, and uncommitted uploads; accounting includes these bytes rather than only visible live file sizes. |
| GC-016 | S | `ReserveProtectsMetadataProgress` | Approach capacity limit; admission stops new payload work before consuming journal/cleanup/recovery reserves. |
| GC-017 | S | `RepairReserveIsNotUserCapacity` | Use reserved repair space during failure; normal admission cannot pre-consume it while claiming the configured repair guarantee. |
| GC-018 | S | `DeletionEventuallyReclaims` | After all references expire/release and faults stop, reclaim unreachable objects within the test's configured maintenance budget. |
| GC-019 | S | `OrphanCleanupAfterClientCrash` | Crash clients with staged unpublished objects; authoritative session recovery eventually releases safe orphans without leaking storage indefinitely. |
| GC-020 | S | `LongDisconnectedReaderIsBounded` | Leave a client offline indefinitely; its bounded pin expires safely and later stale reads fail rather than force unbounded retention. |

### 7.24. SEC — Authentication, authorization, and confidentiality

**Requirements:** FSD §§4,7,12,14. **Invariants:** INV-08,INV-12,INV-13,INV-14.

| Test ID | Level | Test name | What it tests / required assertion |
|---|---|---|---|
| SEC-001 | I | `MutualPeerIdentity` | Unknown, wrong-cluster, expired, or revoked node identities cannot join or issue trusted storage receipts. |
| SEC-002 | I | `ClientAuthenticationRequired` | Anonymous/invalid clients cannot access protected namespace, data, snapshots, or administrative endpoints. |
| SEC-003 | I | `NamespaceAuthorizationEveryOperation` | Enumerate all file/directory/transaction/snapshot operations with allowed and denied principals; server checks each independently of SDK validation. |
| SEC-004 | I | `DirectObjectReadCannotBypassAcl` | Guess a valid object ID from another protected file/volume; direct storage reads require valid authorized scope. |
| SEC-005 | I | `DirectWriteCannotPublishNamespace` | Upload arbitrary extents without namespace authority; they cannot become reachable live content through bypass endpoints. |
| SEC-006 | S | `PermissionRevokedDuringStaging` | Revoke authorization before publication; revalidate at the authoritative decision and reject the previously staged unauthorized mutation. |
| SEC-007 | S | `CapabilityExpiryAndReplay` | Replay expired or wrong-scope data capabilities after reconnect/epoch change; deny misuse while preserving explicitly valid historical access rules. |
| SEC-008 | I | `BackupRetentionPermissionsSeparated` | Export credentials can transfer but cannot shorten independently protected retention or erase protected recovery points. |
| SEC-009 | I | `BackupOfflineAuthorizationIsExplicit` | Disconnect the source and change source ACLs; backup enforces its documented local policy and does not claim instantaneous offline revocation propagation. |
| SEC-010 | I | `CrossVolumeObjectIsolation` | Use identical object IDs, token integers, names, or dedup hints across security domains; authorization remains volume/principal scoped. |
| SEC-011 | I | `TransportTamperingDetected` | Alter authenticated/encrypted protocol frames; integrity failure closes/rejects the exchange without accepting data or metadata changes. |
| SEC-012 | I | `AtRestEncryptionCoversRequiredData` | Inspect offline data, metadata, journals, snapshots, and temporary files according to the encryption contract; protected plaintext/secrets do not leak into excluded temporary paths. |
| SEC-013 | S | `KeyRotationDuringIo` | Rotate active keys while reading/writing/repairing; objects remain decodable with recorded key versions and old required keys are retained. |
| SEC-014 | I | `UnauthenticatedResourceLimits` | Send oversized lengths, excessive unauthenticated connections, and malformed messages; enforce allocation/rate limits without taking down unrelated work. |
| SEC-015 | I | `ParserAndManifestFuzzing` | Fuzz every wire/disk manifest parser with valid/invalid framing and counts; no crash, unbounded allocation, unauthorized access, or silent acceptance of malformed state. |
| SEC-016 | I | `AuditContainsIdentityNotPayload` | Inspect audit logs for security and admin actions; record principal, operation, outcome, and scope while excluding file bytes and key material by default. |
| SEC-017 | S | `ClockChangeCertificateBehavior` | Move wall clocks across credential-validity boundaries; fail authentication safely without turning time changes into new write authority. |
| SEC-018 | I | `AdminCannotSilentlyWeakenPolicy` | Attempt unauthorized replication, topology, retention, or promotion changes; deny and audit them rather than apply an undocumented fallback. |

### 7.25. FMT — Wire formats, disk formats, upgrades, and compatibility

**Requirements:** FSD §§3.2,10,12,15,17. **Invariants:** INV-01,INV-06,INV-08,INV-10,INV-18.

| Test ID | Level | Test name | What it tests / required assertion |
|---|---|---|---|
| FMT-001 | U | `WireRoundTripGoldenVectors` | Round-trip fixed vectors for paths, IDs, revisions, extents, manifests, errors, and tokens across supported encoders/decoders. |
| FMT-002 | U | `UnknownRequiredFeatureRejected` | Read a message requiring unsupported semantics; fail explicitly rather than ignore fields essential to correctness. |
| FMT-003 | U | `OptionalFieldsRemainCompatible` | Add permitted optional fields; older supported readers preserve defined behavior without corrupting identity or checksums. |
| FMT-004 | U | `EndianAndIntegerBoundaries` | Test zero, maximum, overflow, truncated varints, and byte-order vectors; decode consistently on supported architectures. |
| FMT-005 | I | `OldDiskImageOpensSupportedVersion` | Start new software on every supported prior format image; migrate or read exactly according to the compatibility policy. |
| FMT-006 | I | `UnsupportedDowngradeBlocked` | Attempt rollback after an irreversible format/feature activation; refuse unsafe downgrade instead of misreading storage. |
| FMT-007 | S | `RollingUpgradeMixedVersions` | Upgrade nodes one at a time under load; negotiate protocol features and retain quorum/commit semantics across the supported version window. |
| FMT-008 | S | `UpgradeInterruptedByCrash` | Crash during migration/checkpoint installation; restart selects a documented recoverable old/new state. |
| FMT-009 | S | `FeatureActivationWaitsForEligibility` | Enable a feature only after relevant participants can interpret it; older members cannot vote/serve incompatible state silently. |
| FMT-010 | I | `BackupRestorableAcrossSupportedVersions` | Create snapshots with each supported source version and restore with supported destination versions, including encoding/key metadata. |
| FMT-011 | U | `ErrorSerializationPreservesOutcome` | Round-trip typed exceptions/status codes; unknown commit outcome, identity, and retryability cannot degrade into generic not-found or cancelled. |
| FMT-012 | U | `ManifestIdentityAndHashStable` | Equivalent canonical manifests produce stable identity; malformed/noncanonical representations cannot exploit inconsistent parsers. |

### 7.26. OPS — Administration, observability, and bounded liveness

**Requirements:** FSD §§9,12,13,14,15. **Invariants:** INV-08,INV-11,INV-13,INV-16,INV-17,INV-18.

| Test ID | Level | Test name | What it tests / required assertion |
|---|---|---|---|
| OPS-001 | S | `DrainNodeSafely` | Drain under foreground traffic; migrate required replicas/leadership before removal and expose progress/failure states. |
| OPS-002 | S | `RemoveNodeRequiresSafeTransition` | Attempt removing a required last copy or unsafe voter combination; reject unless an explicit documented destructive recovery procedure is invoked. |
| OPS-003 | S | `MetricsReflectDurableReality` | Compare reported replicas/quorum/backup checkpoints against actual durable storage and configuration; connection status alone does not imply health. |
| OPS-004 | S | `DegradedStateVisible` | Lose required target copies while acknowledgement policy still permits writes; show under-replication/domain deficit rather than green full durability. |
| OPS-005 | S | `RepairAndGcBacklogVisible` | Delay maintenance; staged/pinned bytes, missing replicas, pending GC, and transfer backlog metrics grow accurately and clear after completion. |
| OPS-006 | S | `BackupRecoveryAgeNotTransferAge` | Transfer many bytes without completing a new snapshot; latest recoverable-boundary age remains old rather than resetting on network activity. |
| OPS-007 | U | `OperationTraceCorrelatesRetries` | A traced logical mutation retains its stable ID through reroutes/retries, with attempt IDs distinct and no duplicate success accounting. |
| OPS-008 | S | `GracefulShutdownDrainsSafely` | Stop a node gracefully with in-flight work; accepted success obeys durability and unfinished operations preserve correct outcomes. |
| OPS-009 | S | `AbruptShutdownStillSafe` | Skip drain/cleanup entirely; subsequent crash recovery maintains safety even when graceful operations were not run. |
| OPS-010 | S | `LoadSheddingBeforeReserveExhaustion` | Saturate queues/storage; reject or backpressure new work before exhausting bounded resources needed for recovery. |
| OPS-011 | S | `RetryStormIsBounded` | Fail a popular endpoint for many clients; bounded retries, backoff, and queue limits prevent unbounded amplification while preserving deadlines. |
| OPS-012 | S | `RecoveredQuorumEventuallyProgresses` | After stable quorum, adequate data targets, capacity, fair scheduling, and bounded communication return, pending/new valid work eventually completes. |
| OPS-013 | S | `PermanentlyUnavailableIsNotLivenessBug` | Keep quorum or required domains absent; test expects explicit unavailability, not an impossible successful write deadline. |
| OPS-014 | S | `MaintenanceCannotStarveForeground` | Run repair/scrub/backup/compaction together; configured budgets prevent unbounded foreground starvation under the admitted workload. |
| OPS-015 | S | `ForegroundCannotStarveRepairForever` | Run admitted foreground load with reserved maintenance budget; repair/GC makes progress once its stated prerequisites hold. |
| OPS-016 | I | `HealthChecksDoNotMutateAuthority` | Poll health/readiness aggressively; endpoints do not renew writer leases, count as durability receipts, or hide stale state. |

### 7.27. PERF — Performance, amplification, and resource envelopes

**Requirements:** FSD §§10,15. **Invariants:** INV-16,INV-17.

| Test ID | Level | Test name | What it tests / required assertion |
|---|---|---|---|
| PERF-001 | P | `SmallFileCreateOpenStatDelete` | Benchmark size buckets around inline/packing thresholds with realistic directory populations; record latency, throughput, and metadata cost per file. |
| PERF-002 | P | `RandomReadWriteBoundaryMatrix` | Benchmark aligned/unaligned small I/O across extent boundaries at different file sizes; report useful bytes, network/storage amplification, and p50/p95/p99. |
| PERF-003 | P | `SequentialStreamingBoundedMemory` | Stream files much larger than RAM; memory follows configured buffers/in-flight limits rather than total file size. |
| PERF-004 | P | `DurableLatencyNotBufferLatency` | Report OnFlush Write acceptance, Flush durability, WriteThrough, and backup barrier separately; never present buffer throughput as durable replication throughput. |
| PERF-005 | P | `ReplicationFactorAndZoneCost` | Compare configured copy/ack/domain policies with documented topology and link delay; report the actual policy behind every number. |
| PERF-006 | P | `HotInodeSingleWriterLimit` | Stress one file separately from many files; identify the single-writer serialization limit without disguising it in cluster aggregate throughput. |
| PERF-007 | P | `HotVolumeMetadataLimit` | Compare one volume with many independent volumes; report the initial per-volume consensus bottleneck explicitly. |
| PERF-008 | P | `LargeDirectoryEnumeration` | Enumerate large pinned directory views through pagination; bound memory, pin time, and metadata RPC amplification. |
| PERF-009 | P | `FragmentedFileReadAmplification` | Create adversarial extent fragmentation; measure mapping depth, lookup cost, and compaction/coalescing impact. |
| PERF-010 | P | `RepairUnderForegroundLoad` | Lose a node/zone under load; measure recovery time, foreground tail latency, bandwidth, and reserved capacity usage together. |
| PERF-011 | P | `BackupCatchupAndReseedCost` | Measure incremental catch-up and full reseed independently while preserving live performance and pin budgets. |
| PERF-012 | P | `SnapshotCreationAndRetentionCost` | Measure capture latency separately from backup transfer, and quantify space growth from retained overwritten extents. |
| PERF-013 | P | `CancellationAndFailureLeakSoak` | Repeat cancelled I/O, unknown outcomes, reconnects, and disposal failures; memory, handles, tasks, and staging stabilize after safe cleanup. |
| PERF-014 | P | `BackpressureAtBandwidthLimit` | Throttle slow consumers/replicas; client/server memory and outstanding work remain bounded while throughput adapts. |
| PERF-015 | P | `CapacityNearReserveBoundary` | Run workloads near configured admission thresholds; tail latency and failure modes remain controlled, with no false durability. |
| PERF-016 | P | `RegressionBaselinesReproducible` | Record build, hardware, OS, storage settings, topology, workload seed, warm/cold-cache state, and policy; compare only compatible baselines. |

### 7.28. PROP — Property-based and model-based operation histories

**Requirements:** FSD §§4–16,18. **Invariants:** INV-01–INV-20.

| Test ID | Level | Test name | What it tests / required assertion |
|---|---|---|---|
| PROP-001 | S | `GeneratedFileByteModel` | Generate valid writes, seeks, truncations, flushes, aborts, and reads; compare committed/pinned/staged byte views with an independent reference model. |
| PROP-002 | S | `GeneratedNamespaceModel` | Generate create, mkdir, move, delete, enumeration, and name reuse; compare namespace identity and topology after every committed operation. |
| PROP-003 | S | `GeneratedTransactionHistories` | Generate conflicting read/write/predicate transactions; check strict serializability and complete transaction snapshots, not just final bytes. |
| PROP-004 | S | `GeneratedLiveReadHistories` | Record invocation/completion intervals for live reads and mutations; check a legal linearization with correct handling of unknown operations. |
| PROP-005 | S | `GeneratedSnapshotHistories` | Generate snapshots, exact commit-and-snapshot, overwrites, and deletions; each retained snapshot matches its model boundary forever until valid expiry. |
| PROP-006 | S | `GeneratedBackupHistories` | Generate transfer interruption, retention, reseed, and source loss; every published/acknowledged destination snapshot has a complete correct dependency closure. |
| PROP-007 | S | `FaultAndWorkloadSeedCrossProduct` | Combine operation generators with node/network/storage/scheduler seeds; save complete starting images and traces for every failure. |
| PROP-008 | S | `MetamorphicBatchingInvariant` | Execute equivalent byte updates with different batching, chunking, and transport fragmentation; compare results at equivalent publication boundaries. |
| PROP-009 | S | `MetamorphicPlacementInvariant` | Run equivalent histories on different legal replica placements; semantics agree despite different latency/availability profiles. |
| PROP-010 | S | `MetamorphicRestartInvariant` | Insert cold restarts after durable decisions into a successful history; preserved committed state agrees with the uninterrupted reference run. |
| PROP-011 | S | `MetamorphicDuplicateRequestInvariant` | Insert duplicates of accepted operation IDs; logical committed history is unchanged except for repeated result delivery. |
| PROP-012 | S | `MetamorphicSnapshotReadInvariant` | Read a retained snapshot before/after arbitrary later live operations; bytes and namespace remain identical. |
| PROP-013 | S | `GeneratorCoversBoundarySizes` | Bias generated lengths and offsets to zero, one, inline boundary, extent boundary, transaction limit, and overflow edges rather than mostly easy random values. |
| PROP-014 | S | `LongHistoryCompactionAndEpochs` | Generate enough history to trigger log compaction, ID retirement, key rotation, repair, and restoration; validate cross-epoch rejection and old-history retention. |
| PROP-015 | S | `FailedHistoriesAreShrunkAndRetained` | Minimize every discovered failure while preserving its oracle violation; add the resulting schedule as a named regression test. |
| PROP-016 | S | `CheckerDoesNotConfuseLiveAndPinned` | Label each read's declared view; allow deliberate historical results only for pinned/backup reads, not new live reads. |

### 7.29. MOD — Bounded formal models and protocol obligations

**Requirements:** FSD §§3.2,7–14,18. **Invariants:** INV-01,INV-02,INV-04,INV-05,INV-06,INV-09,INV-10,INV-18.

| Test ID | Level | Test name | What it tests / required assertion |
|---|---|---|---|
| MOD-001 | M | `ConsensusSafetyModel` | Check the selected consensus protocol/configuration model for election, log, and committed-state safety under declared bounded failures. |
| MOD-002 | M | `FencingAcrossEpochsModel` | Model lease grants, expiry, delayed renewal, restart, and writer publication; prove/check that obsolete generations cannot publish after replacement under the model assumptions. |
| MOD-003 | M | `DataThenMetadataModel` | Explore object staging, receipts, commit decision, crash, and GC; no published root references insufficiently protected data. |
| MOD-004 | M | `IdempotencyAndRetirementModel` | Explore retry, delayed delivery, outcome compaction, and session retirement; one accepted ID cannot later execute a second logical effect. |
| MOD-005 | M | `TransactionValidationModel` | Check read versions, absent-name dependencies, and directory predicates against concurrent ordinary/transactional mutations within a volume. |
| MOD-006 | M | `SnapshotConsistentBoundaryModel` | Explore snapshots racing transaction decisions; no snapshot cuts through a committed transaction. |
| MOD-007 | M | `BackupPublishModel` | Explore partial transfer, local crash, missing dependencies, retention, and publication; every exposed snapshot is complete and retained as promised. |
| MOD-008 | M | `GcReachabilityModel` | Model current roots, snapshots, reader pins, staging, unresolved transactions, transfers, and placement transitions; reachable/protected objects cannot be deleted. |
| MOD-009 | M | `MembershipTransitionModel` | Explore old/new voter configurations and delayed nodes; only the selected safe reconfiguration protocol may transfer authority. |
| MOD-010 | M | `RecoveryEpochModel` | Model source partition, explicit restore/promotion, and old source return; no two authorities are accepted under the same recovered identity. |
| MOD-011 | M | `ConditionalLivenessModel` | State fairness, eventual communication, quorum, and capacity assumptions explicitly; check progress only under those conditions. |
| MOD-012 | M | `RefinementAndBoundsReport` | Map model actions to implementation fault points and record every state-space bound, abstraction, assumption, and unproved refinement gap; completed bounded exploration is not labelled an implementation proof. |

### 7.30. CHA — Compound-failure regression scenarios

**Requirements:** FSD §§7–16,18. **Invariants:** INV-01–INV-20.

| Test ID | Level | Test name | What it tests / required assertion |
|---|---|---|---|
| CHA-001 | S | `LeaderFailoverPlusLostCommitReply` | Commit while leader crashes and client reply is dropped; retry via new leader resolves one complete durable mutation. |
| CHA-002 | S | `ZoneFailureDuringPolicyMigration` | Lose a zone while upgrading replication policy; no false strong-policy activation or unsafe old-copy deletion occurs. |
| CHA-003 | S | `OldWriterResumeDuringResync` | Resume an expired writer on a returning stale node while repair proceeds; stale publication is rejected throughout catch-up. |
| CHA-004 | S | `DeleteThenLongOutageThenCompaction` | Delete and reuse names during a node's long outage, compact history, return the node; checkpoint catch-up prevents resurrection and stale ID replay. |
| CHA-005 | S | `BackupOfflinePlusSourceGcPressure` | Keep backup offline while overwrites and snapshots fill source history; safely expire transfer pins/reseed without blocking live commits or deleting protected destination history. |
| CHA-006 | S | `SourceLossDuringPartialBackup` | Lose all source storage while next snapshot is incomplete; restore only the last complete destination snapshot and report the missing newer interval. |
| CHA-007 | S | `DiskFullDuringTransactionAndLeaderChange` | Fill required storage while leader changes during commit; all-or-nothing visibility and correct unknown/abort outcomes survive. |
| CHA-008 | S | `CorruptReplicaPlusNetworkPartition` | Corrupt one copy and partition another; return verified data only when a healthy reachable source exists, otherwise fail explicitly. |
| CHA-009 | S | `RestartDuringCompactionAndSnapshotDelete` | Crash nodes while compaction, snapshot deletion, and GC interleave; no still-pinned object is reclaimed or mapped to wrong bytes. |
| CHA-010 | S | `KeyRotationDuringBackupReseed` | Rotate keys while reseeding an interrupted backup; completed snapshots retain a self-contained decryptable dependency closure. |
| CHA-011 | S | `RollingUpgradeDuringFailover` | Upgrade a replica, fail the current leader, and reconnect an old-version node; feature negotiation and authority remain safe. |
| CHA-012 | S | `RetryStormDuringMetadataRecovery` | Reconnect many clients with saved IDs while metadata snapshot installs; deduplication and bounds prevent duplicate operations or memory exhaustion. |
| CHA-013 | S | `PartitionHealWithDelayedOldMessages` | Heal after new authority commits, then release old packets in adversarial order; old votes, leases, receipts, and outcomes cannot overwrite new state. |
| CHA-014 | S | `PinnedReaderAndRetentionExpiryDuringRepair` | Hold an allowed reader pin while snapshot retention expires and replica repair runs; data remain safe until the last valid obligation ends. |
| CHA-015 | S | `FullColdRestartWithUnknownTransactions` | Cold-start every node with staged and outcome-unknown work; recover durable decisions, fence old sessions, and reclaim only definitively abandoned data. |
| CHA-016 | S | `BackupPromotionAndOldSourceReturn` | Restore explicitly with new authority, then reconnect old source plus delayed exports; preserve lineage separation and deny conflicting writes. |

### 7.31. MUT — Negative controls and mutation testing of safety checks

**Requirements:** FSD §18. **Invariants:** INV-01–INV-20.

| Test ID | Level | Test name | What it tests / required assertion |
|---|---|---|---|
| MUT-001 | S | `RemoveFencingCheckIsDetected` | Deliberately bypass server-side fencing in a test build; the stale-writer regression must fail. |
| MUT-002 | S | `AcknowledgeBeforeDiskFlushIsDetected` | Move a data receipt before required persistence; a crash/power-loss simulation must expose lost acknowledged content. |
| MUT-003 | S | `CommitMetadataBeforeDataIsDetected` | Permit publication with one missing new object; dependency-closure and recovery checks must fail. |
| MUT-004 | S | `UseReachableNodeCountForQuorumIsDetected` | Compute majority from reachable nodes instead of configured voters; partition safety tests must reject the resulting history. |
| MUT-005 | S | `DropOperationDedupIsDetected` | Disable accepted-ID deduplication; duplicate-append/name-reuse tests must catch the extra mutation. |
| MUT-006 | S | `TreatUnknownAsAbortedIsDetected` | Map indeterminate commit to definite cancellation; history/outcome tests must find a later committed effect after claimed abort. |
| MUT-007 | S | `SkipTransactionPredicateValidationIsDetected` | Ignore absent-name/enumeration dependencies; generated phantom/write-skew tests must fail. |
| MUT-008 | S | `AllowStaleLiveReadIsDetected` | Serve a minority's cached metadata as live; real-time consistency checks must reject the stale result. |
| MUT-009 | S | `IgnoreSnapshotPinIsDetected` | Remove a snapshot/reader root from GC marking; historical-read and GC-race tests must fail. |
| MUT-010 | S | `PublishBackupBeforeClosureIsDetected` | Advance backup pointer before the last dependency is durable; interrupted-transfer/recovery tests must fail. |
| MUT-011 | S | `SkipChecksumVerificationIsDetected` | Return corrupted replica bytes without verification; integrity tests must fail before successful delivery is accepted. |
| MUT-012 | S | `CountSameZoneTwiceIsDetected` | Ignore failure-domain uniqueness; acknowledgement-policy tests must fail. |
| MUT-013 | S | `ForgetExpiredIdWatermarkIsDetected` | Compact outcomes without retaining replay rejection state; delayed expired-ID replay must be caught. |
| MUT-014 | S | `SilentlyRelaxPolicyIsDetected` | Lower ack/domain requirements on timeout; policy and durability-oracle tests must fail. |
| MUT-015 | S | `AllCriticalNegativeControlsAreKilled` | Run every specified safety mutant; any surviving non-equivalent critical mutant blocks approval of the affected test coverage claim. |


## 8. Systematic fault matrix: failures during every operation

Named scenarios are necessary but insufficient. An operation registry SHALL generate and track cases of the following form:

```text
TestCase = (
    operation family and concrete overload,
    valid/invalid input equivalence class,
    initial namespace/object/transaction state,
    durability policy and topology,
    participating role and node,
    reachable stage and occurrence,
    fault or concurrent transition,
    client count and schedule,
    retry/recovery path
)
```

Do not blindly execute impossible Cartesian products. Each operation publishes its real stage graph and applicability predicates. Exclusions require a specific reason such as “read-only operation has no metadata publication” or “no new extent is allocated in this inline path.” A fault point unexpectedly not reached is different from a reviewed inapplicable combination.

### 8.1 Operation families that must register

| Group | Required registry entries |
|---|---|
| Connection/session | Connect, discover volume, reroute, reconnect, authenticate/refresh, client shutdown |
| Open/handle | Each creating/noncreating/truncating/append open; acquire/renew/release reservation; acquire/renew/release reader pin |
| Stream | Each read/write overload; ReadByte/WriteByte; network-requiring Length/view resolution; seek/SetLength; flush; commit; abort; dispose; CopyTo/CopyToAsync composition |
| Helpers | Read/write all bytes/text/lines; lazy line enumeration; append; Copy; Move; Delete; Exists; metadata retrieval |
| Namespace | CreateDirectory, MoveDirectory, recursive/nonrecursive delete, enumeration first page/continuation, metadata/authorization changes |
| Transaction | Begin, bound reads/writes and directory operations, stage/flush bound stream, commit, abort, conflict/expiry, outcome query |
| Snapshot | Create, minimum-boundary create, exact commit-and-snapshot, list, open, delete, retention change, restore into new volume |
| Backup | Register/remove/configure target; capture/export; inventory; object transfer; verify closure; durably acknowledge; publish; pin/read; await at-least/exact barrier; retention expiry; reseed; restore/promotion |
| Data storage | Allocate/stage extent or mapping object; put/get/head; replicate; issue/validate receipt; seal/flush; verify/scrub; repair; quarantine |
| Metadata/consensus | Election, read authority validation, append/persist/apply, outcome persistence, checkpoint/compaction/install, leader transfer, member addition/removal |
| Maintenance/control | Placement-map update; policy migration; rebalance; GC mark/reference transition/sweep; compaction; drain; admission; key rotation; format upgrade |

A local-only seek has no network fault stage itself; test it before/after connection failures and with disposed/faulted/staged handles. Do not manufacture RPCs simply to satisfy a test label. Test both fast paths (inline, cached immutable data, reused connections, idempotent result) and slow paths (new objects, renewed authority, repair, reconnect).

SDK API reflection and protocol-operation registration SHOULD detect new public/internal operations lacking a test mapping. A new operation or publication path cannot inherit coverage merely because it calls a previously tested helper.

### 8.2 Required stage instrumentation

| Stage | Injection boundary |
|---|---|
| F00 | Before invocation, with the chosen node/link already unavailable |
| F01 | After local validation/operation-ID allocation, before routing or transmission |
| F02 | During DNS/discovery/connect/TLS/session establishment or pooled-connection reuse |
| F03 | After request header, during each payload chunk, and before complete frame receipt |
| F04 | After the server accepts a complete request, before durable acceptance/reservation state |
| F05 | After operation/session/staging registration, before new object writes |
| F06 | During each data and mapping-object write, including short/torn writes |
| F07 | After local writes, before the required durability primitive completes |
| F08 | After durable object persistence, before its receipt reaches the coordinator |
| F09 | After each receipt, including immediately below and at the policy threshold |
| F10 | Before metadata proposal and immediately after local log append but before commit |
| F11 | After quorum commit, before state-machine application/publication |
| F12 | After authoritative application and outcome persistence, before response serialization |
| F13 | During response transmission, after server completion but before client receipt |
| F14 | After client receipt but racing cancellation/completion and caller resumption |
| F15 | During postdecision cleanup, reservation release, pin release, and reference retirement |
| F16 | During restart, local journal replay, metadata checkpoint install, and suffix catch-up |
| F17 | During snapshot/source-transfer pin establishment and manifest/object inventory |
| F18 | After backup object receipt but before destination flush and closure verification |
| F19 | After complete destination durability, before/after published-pointer update and backup acknowledgement |
| F20 | During retention expiry, GC tracing/sweep, compaction, and old-replica deletion |
| F21 | During membership, placement, policy, protocol, key, or authority-epoch transition |

Repeat injection immediately before and after each relevant boundary, and at each occurrence for the bounded trace. A multi-extent write must not test only its first chunk: cover first/interior/last chunks and every chunk of small bounded cases. The exact physical mapping of “flush” and “commit” must be documented for the selected storage engine and consensus implementation.

### 8.3 Fault and transition dimensions

| Family | Required variants |
|---|---|
| Application/network delivery | Drop request; drop reply; duplicate accepted RPC; delay beyond caller deadline; out-of-order independent RPCs/replies; replay old correlated messages |
| Connection | Reset; graceful close at wrong point; half-open black hole; refused connection; stale pooled socket; partial/truncated frame; changed endpoint; handshake/certificate failure |
| Partition | Client-only isolation; leader isolation; follower isolation; data-only isolation; complete zone cut; minority/majority cuts; asymmetric links; repeated flapping |
| Process | Pause/resume; abrupt crash; graceful stop; restart same durable disk; restart stale image; empty replacement disk; old removed member returns; cloned identity detection |
| Storage | Delayed I/O; short write; failed flush; legal reordering of unflushed writes; torn record; ENOSPC; EIO; read-only device; missing object; corrupted payload/metadata/index |
| Client control | Cancellation before/after possible publication; deadline; process crash with/without persisted operation ID; retry with same ID; invalid changed-payload replay; stream/client disposal |
| Concurrent valid transition | Leader/member/placement change; policy migration; rename/delete/name reuse; transaction conflict; snapshot create/delete; pin expiry; log/outcome compaction; key rotation; upgrade |
| Recovery/maintenance | Repair interrupted by second fault; destination full; source lost during backup; reseed interrupted; checkpoint installation interrupted; GC during unresolved commit or transfer |

Transport fidelity matters: intact TCP/TLS streams do not deliver arbitrarily reordered bytes to the application. Test segmentation and connection truncation there, and reorder independent requests/streams where the actual protocol permits it. Deliberately malformed frames belong to parser/security tests rather than a fictitious normal-network guarantee.

### 8.4 Coverage levels

**Required finite single-fault campaign:** For each declared compact configuration and bounded operation path, enumerate every applicable single fault at every registered occurrence and both adjacent sides of each durable/publication boundary. Record the exact number of generated, applicable, reached, completed, passed, failed, and excluded cases. Do not publish “100% fault coverage” without that denominator.

**Required compound campaign:** Execute all named CHA cases, and explicitly enumerate bounded two-fault schedules around writer fencing, metadata/data publication, outcome retirement, snapshot/backup publication, GC, and membership transition. Include a fault during recovery from the first fault. Bound message counts, operation counts, and failure opportunities in the report; do not imply that this enumerates every long-running interleaving.

**Larger randomized campaign:** Run deterministic state-machine generators across larger histories, data sets, role placements, fault sequences, and schedules. Publish seeds and limits. Pairwise/covering-array sampling can reduce noncritical configuration combinations, but it is not a substitute for the critical exhaustive bounded stage schedules.

**Real-environment campaign:** Repeat selected severe scenarios using actual processes, network disruption, storage failures, and cold restart. Simulation cannot validate every property of an unmodeled OS, driver, runtime, library, or device.

## 9. Fully specified critical scenarios

These examples show the minimum rigor expected when implementing the named families. Every named test should have equivalent explicit setup, schedule, assertions, and cleanup—not merely a method with the correct name.

### 9.1 Durable flush with a temporarily offline replica

**Covers:** REP-002, OFF-001, DSK-001; INV-01, INV-11.  
**Setup:** Three live hosts A/B/C in three zones, three metadata voters, target three data copies, durable acknowledgement from two copies in two zones. C is offline with preserved disk. A and B are available and valid metadata voters. Independent backup is disconnected.

**Schedule:** Create an initial committed file. Stage a multi-extent replacement and length change. Hold B's data flush before completion; verify client FlushAsync has not succeeded. Release B's flush, allow valid receipts and metadata majority commit, and receive success. Hard-stop A and B without graceful shutdown. Restart from durable images only, then return C and complete catch-up.

**Assert:** The full new revision and its resolvable outcome survive. Success did not depend on C or backup and did not occur before A/B met the policy. C is reported stale until its relevant state is repaired. No intermediate revision was ever exposed. Run a separate variant with ack-three, where C's absence prevents new success. Do not assert survival after permanently destroying both actual acknowledged copies unless another verified copy exists.

### 9.2 Append committed but response lost

**Covers:** OUT-006, NET-002, CHA-001; INV-02, INV-06.  
**Setup:** Healthy core cluster; file contains prefix P; caller persists operation identity K and intended suffix S.

**Schedule:** Submit append K. Let durable metadata commit finish. Drop every response to the caller and crash its process. Elect a new leader if needed. Start a new client, query K, and retry K with the same payload. Then attempt K with a different suffix.

**Assert:** Contents are exactly `P + S`, not `P + S + S`. Query/retry reports the original committed result. Changed-payload reuse is rejected. The first client's uncertainty did not establish rollback. Repeat after metadata compaction within the retained outcome window, and beyond the window where explicit expired-ID rejection is required.

### 9.3 Expired writer resumes after replacement

**Covers:** LOCK-009, LOCK-010, OFF-021, CHA-003; INV-05.  
**Setup:** Writer A owns generation g and has private staged data. Configure compatible readers as required by the share contract.

**Schedule:** Pause A's process, retaining memory and queued messages. Advance only the required simulated server/session time; safely expire A. Grant writer B generation greater than g. Run one variant before B changes the inode revision, and another after B commits its replacement. Resume A, deliver its delayed renewal, data receipts, flush, and disposal requests in every bounded order. Include a metadata leader change between expiry and A's return.

**Assert:** B remains the only valid writer; any committed contents from B remain authoritative. Every obsolete publication path from A is rejected and its stream becomes faulted. A's immutable orphan uploads may exist temporarily but are not published. No client-clock decision or reconnect silently renews g. Old orphan state is eventually reclaimed only when safely abandoned.

### 9.4 Long-offline node returns after deletion and compaction

**Covers:** RSY-002, RSY-005–RSY-011, CHA-004; INV-06, INV-07, INV-18.  
**Setup:** A/B/C core cluster, C has old `/x` inode I and old file mappings, plus an old request/session window.

**Schedule:** Stop C. On A/B, overwrite and truncate `/x`, rename it, delete the renamed inode, recreate `/x` as a new inode J, commit a multi-file transaction, create/delete snapshots, and retire selected old operation IDs. Compact missing metadata history. Return C and force checkpoint rather than log-only catch-up. Interrupt installation once, then finish. Stop A after C is safely caught up and verify service through B/C.

**Assert:** `/x` maps to J, not I. Deleted names and truncated bytes do not resurrect. Checkpoint recovery preserves complete transactions, snapshot references, fencing generations, and replay rejection. C does not serve stale live reads while catching up. Once eligible and complete, B/C can serve the correct authoritative state.

### 9.5 Backup interruption immediately before publication

**Covers:** BAK-006–BAK-013; INV-10, INV-15.  
**Setup:** Backup has fully retained/readable S100. S101 contains a multi-file commit and several new mapping/data objects. A reader pins S100.

**Schedule:** Transfer the S101 manifest first. Make inventory claim one required object exists when it does not. Send other objects, interrupt before the last dependency is durable, and cold-restart the backup. Restore connectivity, transfer the missing object, verify closure, and crash immediately before/after publication-pointer persistence in separate executions.

**Assert:** Before complete S101 durability, S100 remains the only newly selectable valid published boundary. The pinned S100 reader never changes versions. False inventory hints cannot skip completeness checks. After successful publication and cold restart, S101 is complete without contacting source metadata. At no point does backup status/acknowledgement certify a partially received S101.

### 9.6 Strict-serializability write-skew test

**Covers:** TX-006–TX-011; INV-04.  
**Setup:** Files `/a` and `/b` each contain `1`. The application requires at least one to remain `1`. Two transactions can read both and take different writer reservations.

**Schedule:** T1 reads both, plans `/a = 0`; T2 reads both, plans `/b = 0`. Stage both before either commits, then explore both commit orders and concurrent commit processing. Repeat with one predicate involving a missing name or directory enumeration.

**Assert:** Both conflicting transactions cannot commit a final `{a:0,b:0}` based on their original joint reads. A transaction must abort/conflict or observe a valid updated state; the committed history is strictly serializable. Check real-time order, not only final invariant satisfaction. Repeat with an ordinary standalone mutation replacing one transaction to ensure isolation is not limited to explicit transaction callers.

### 9.7 Garbage collection during an unknown commit

**Covers:** GC-004–GC-006, OUT-014, CHA-009; INV-01, INV-06, INV-09.  
**Setup:** Multi-extent file replacement is staged and its commit may have happened; client response and coordinator outcome query are partitioned.

**Schedule:** Advance wall clocks and staging-age timers, run GC/compaction repeatedly, and crash/restart the coordinator. Explore both legal underlying outcomes: committed and authoritatively aborted. Heal the partition and resolve the operation before final orphan cleanup.

**Assert:** Time alone does not delete objects that may already be committed or otherwise protected. If committed, the full new revision remains recoverable. If definitively aborted, old state remains and safe orphans are eventually reclaimed. A pending query is not treated as proof of abort. Reference promotion, outcome recovery, and collection are checked together.

### 9.8 Restore with no source services or cached metadata

**Covers:** RST-001–RST-005, RST-014–RST-016; INV-10, INV-13, INV-19.  
**Setup:** Backup durably retains exact snapshot S with metadata, mappings, encrypted payloads, encoding manifests, adapter metadata where applicable, and an independently recoverable key path. Source later commits unreplicated T.

**Schedule:** Remove source disks, network endpoints, process memory, and client caches from the recovery environment. Restore S on a clean host using the documented runbook. Instrument outbound connections so any undeclared call to a source service fails the test. Start a new authority epoch, then exercise new writes/snapshots/backups.

**Assert:** Every file and namespace record matches S, and required application recovery steps succeed where declared. No claim is made that unreplicated T survived. Missing recovery keys cause a precise failure rather than a false “backup verified” result. Old source tokens cannot mutate the restored authority.

## 10. Requirement traceability and release evidence

### 10.1 FSD acceptance-criterion map

| FSD ID | Primary suites | Evidence focus |
|---|---|---|
| AT-01 | SDK, STR | No mount; .NET Stream interoperability and supported consumers |
| AT-02 | PATH, OPEN, STR, PUB, LOCK, OUT | Mode/share/offset/EOF/sparse/cancellation/disposal contracts |
| AT-03 | PUB, SNP, BAK | Private staging versus atomic committed visibility |
| AT-04 | PUB, REP, CON, OFF, DSK, CHA | Durable crash boundaries and honest survival envelope |
| AT-05 | LOCK, OFF, RSY, MOD, MUT | Old-writer fencing across pause, failover, and recovery |
| AT-06 | CON, NET, OFF, MOD, MUT | Partition authority and live-read correctness |
| AT-07 | LAY, PERF, PROP | Changed-range synchronization and measured amplification |
| AT-08 | TX, NS, SNP, PROP, MOD | Strict transactions, predicate conflicts, coherent snapshots |
| AT-09 | OUT, NET, CON, RSY, MOD, MUT | Idempotency, unknown outcomes, durable replay rejection |
| AT-10 | BAK, SNP, DSK, CHA, MOD | Interrupted transfer and complete historical publication |
| AT-11 | BAK, REP, LOCK, OFF | Backup excluded from live votes, locks, and ordinary acknowledgements |
| AT-12 | SNP, BAK, RST | At-least checkpoint versus exact retained recovery version |
| AT-13 | BAK, GC, OPS, CHA | Bounded source retention and safe long-offline reseed |
| AT-14 | RST, APP, SEC | Self-contained source-independent recovery and key/adapter requirements |
| AT-15 | DSK, GC, REP, RSY, CHA | Corruption, full disks, compaction, reconfiguration, and reclamation |
| AT-16 | SDK, PATH, OPEN, REP, FMT, SEC | Explicit option, format, feature, and policy rejection |
| RCV-01–RCV-08 | CON, OFF, RSY, REP, CHA | Offline membership, checkpoints, no resurrection, verified repair |
| VER-01–VER-07 | HAR, PROP, MOD, MUT; §§8–11 of this document | Evidence integrity, fault accounting, analysis bounds, release gates |

Suite headers give section-level traceability for the complete FSD, including the added RCV and VER requirements. Before implementation approval, expand every normative clause into a stable requirement record linked to individual concrete tests or formal obligations. Do not claim 100% clause coverage from this section-level map alone.

### 10.2 Release gates

| Gate | Required evidence |
|---|---|
| Specification | Public API/error/durability semantics and selected wire/disk/consensus protocols are fixed for the release; each requirement has traceability. Deferred functionality is explicitly rejected or absent. |
| Named catalog | Every applicable required case is implemented and passes on its declared supported configurations. Conditional optional-feature cases clearly state applicability. Not-run/inconclusive is not pass. |
| Fault boundaries | The declared finite single-fault matrix is fully accounted for; required compound scenarios and critical bounded two-fault schedules pass. Every claimed injection is observed. |
| Concurrency/history | Live histories pass independent linearizability checks; committed transaction histories pass strict-serializability/predicate checks. Unknown operations are modeled correctly. |
| Critical protocol analysis | Models for fencing, publication, replay retirement, GC, backup publication, and reconfiguration have completed reports with assumptions/bounds/refinement gaps. Unfinished exploration is explicitly unresolved. |
| Negative controls | Every specified non-equivalent critical mutant is detected by the associated suite; surviving safety mutants block the affected assurance claim. |
| Real persistence/recovery | Real-process restart and supported hardware/storage-stack tests pass; backup restoration is demonstrated with source completely absent. |
| Security/compatibility | Authentication/authorization, format/upgrade, key recovery, and supported SDK platform tests pass; no unresolved release-blocking vulnerability or unsafe format transition. |
| Operational envelope | Measured memory, staging/pin budgets, repair capacity, and workload-specific performance thresholds are recorded and approved; simulated timing is not published as production performance. |
| Defect/evidence record | Every discovered safety failure has a deterministic regression, root-cause resolution, and replay artifact. Remaining risks and untested/out-of-model scenarios are disclosed. |

Performance thresholds must be selected from actual deployment requirements and measured baselines before a performance pass/fail claim; this document invents no universal latency or throughput guarantee. Randomized-campaign budgets and supported hardware/version matrices must likewise be published before the release, not retroactively selected to hide failures.

Code coverage is supplementary evidence. Report line/branch coverage, mutation results, requirements covered, reachable protocol stages exercised, fault combinations completed, schedule bounds, and recovery results separately. None is interchangeable with the others.

## 11. Suggested implementation organization

```text
Dfs.Tests.Unit/
Dfs.Tests.Contract/          # same semantic tests against reference and real backends
Dfs.Tests.Simulation/        # virtual network, persistence, scheduler, independent clocks
Dfs.Tests.Integration/       # real processes, network, and storage-engine boundaries
Dfs.Tests.Recovery/          # cold restart, checkpoint, backup, clean-host restoration
Dfs.Tests.Security/
Dfs.Tests.Performance/
Dfs.Models/                 # bounded protocol specifications and checker configurations
Dfs.Testing.ReferenceModel/
Dfs.Testing.HistoryChecker/
Dfs.Testing.FaultRegistry/
Dfs.Testing.ReplayCorpus/
```

Keep the reference model simpler than the implementation and independent of its serialization, placement, and storage algorithms. Tests should invoke the actual state-machine/SDK code wherever practicable, replacing physical dependencies rather than reimplementing the system solely for the simulator. A model of a correct algorithm is not evidence that a different production implementation follows it.

Raft provides an established consensus reference with a formal specification, but selecting Raft does not verify this filesystem's staging, garbage collection, lease, API, or backup protocols.[^raft] TLA+ is one suitable language for specifying and checking those state transitions; the chosen model and its relation to code still require explicit review.[^tla]

## References

External references ground API semantics and verification techniques. All named cases, proposed invariants, and release gates are requirements for this proposed system, not claims that the referenced projects implement or pass this catalog.

[^stream]: Microsoft, *System.IO.Stream*. https://learn.microsoft.com/en-us/dotnet/api/system.io.stream?view=net-10.0
[^modes]: Microsoft, *System.IO.FileMode*. https://learn.microsoft.com/en-us/dotnet/api/system.io.filemode?view=net-10.0
[^sharing]: Microsoft, *System.IO.FileShare*. https://learn.microsoft.com/en-us/dotnet/api/system.io.fileshare?view=net-10.0
[^fdb]: FoundationDB, *Simulation and Testing*. https://apple.github.io/foundationdb/testing.html
[^coyote]: Microsoft, *Concurrency Unit Testing with Coyote*. https://microsoft.github.io/coyote/
[^etcd]: etcd, *API guarantees*. https://etcd.io/docs/v3.6/learning/api_guarantees/
[^raft]: Ongaro and Ousterhout, *The Raft Consensus Algorithm*. https://raft.github.io/
[^tla]: Leslie Lamport, *Specifying Systems: The TLA+ Language and Tools for Hardware and Software Engineers*. Microsoft Research publication page. https://www.microsoft.com/en-us/research/publication/specifying-systems-the-tla-language-and-tools-for-hardware-and-software-engineers/
