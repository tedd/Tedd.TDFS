# FSD Addendum A: NTFS-like permissions, storage pools, alternate data streams

**Version:** 0.1 · **Date:** 2026-10-12 · **Extends:** [FSD 0.2](distributed-filesystem-fsd.md) and its
[test plan](distributed-filesystem-test-plan.md). Same normative language. Nothing here weakens an FSD requirement.

## A1. Namespaces (clarification)

**NSP-01.** A *volume* is the namespace boundary required by the product owner ("namespaces"): directory listings,
path resolution, snapshots, transactions, handles, continuation tokens, operation ids and object references of one
volume SHALL never expose or address entries of another volume. Identical paths in two volumes are unrelated
entries. (FSD §3, NS-05, PATH-014 already require this; ADD tests in suite POOL.)

## A2. Storage pools

**POOL-01.** The cluster SHALL support named **storage pools**: a pool is a named, non-empty set of live nodes. A
node MAY belong to several pools. The pool `default` always exists and contains every live node not explicitly
excluded from it.

**POOL-02.** A volume SHALL be bound to one pool (`DfsVolumeSpec.StoragePool`, default `default`). All data objects of
that volume SHALL be placed only on nodes of that pool, and the volume's metadata voters SHALL be chosen from that pool
unless an explicit voter list is given. A named storage policy MAY name a different pool for inodes using that policy
(e.g. a fast tier); such objects SHALL be placed only in that pool.

**POOL-03.** Policy satisfiability (copies, distinct zones/hosts) SHALL be evaluated against the pool's eligible nodes
only. Creating a volume or policy whose pool cannot satisfy it SHALL fail with `InvalidPolicy`. Nodes outside the pool
SHALL never be used as fallback targets, even when pool nodes are unavailable (no silent weakening).

**POOL-04.** Pool membership changes are control-group decisions that advance the placement epoch. Removing a node from a
pool SHALL migrate that pool's objects off the node through the normal repair path (copy-before-delete); the node is
reported as draining for that pool until done. A pool cannot be deleted while a volume or policy references it.

**POOL-05.** Capacity, usage and replica health SHALL be reported per pool. Exhausting one pool's capacity SHALL NOT
affect admission for volumes in other pools.

## A3. NTFS-like permissions

**ACL-01. Identities.** Principals are users. The control group SHALL maintain named **groups** with user members.
Well-known identities: `Everyone`, `AuthenticatedUsers` (all authenticated principals), `Administrators` (principals
with ClusterAdmin), `CreatorOwner` (inheritance template only). An identity is written `user:<name>`, `group:<name>`
or the well-known name.

**ACL-02. Security descriptor.** Every inode SHALL have a security descriptor: an owner identity and a discretionary
ACL of access control entries (ACEs) `{identity, rights, Allow|Deny, inheritance flags {ContainerInherit,
ObjectInherit}, propagation flags {NoPropagateInherit, InheritOnly}}`, plus a *protected* flag that blocks inheritance
from the parent. Rights use the NTFS bit values (`DfsFileSystemRights`): ReadData/ListDirectory, WriteData/CreateFiles,
AppendData/CreateDirectories, ReadExtendedAttributes, WriteExtendedAttributes, ExecuteFile/Traverse,
DeleteSubdirectoriesAndFiles, ReadAttributes, WriteAttributes, Delete, ReadPermissions, ChangePermissions,
TakeOwnership, plus composites Read, Write, ReadAndExecute, Modify, FullControl.

**ACL-03. Inheritance.** Inherited ACEs are derived from ancestors' inheritable ACEs following NTFS rules:
ObjectInherit applies to files, ContainerInherit to directories; InheritOnly ACEs do not apply to the object they are
set on; NoPropagateInherit limits inheritance to immediate children; a protected descriptor ignores ancestors;
`CreatorOwner` in an inherited ACE applies to the inheriting object's owner. Inheritance SHALL be evaluated
**dynamically** from the current ancestor chain, so changing a directory's inheritable ACEs or moving an inode takes
effect atomically for the whole subtree with the committing operation (deliberate difference from NTFS's eager
propagation; observable results equal a completed NTFS propagation).

**ACL-04. Evaluation.** Access checks SHALL use NTFS canonical order: explicit Deny, explicit Allow, inherited Deny
(nearest ancestor first), inherited Allow (nearest first). A request for rights R is granted iff every bit of R is
allowed before any matching Deny for that bit; anything not allowed is denied. The owner is implicitly granted
ReadPermissions and ChangePermissions. Members of `Administrators` may always TakeOwnership and ChangePermissions but
are otherwise subject to the ACL.

**ACL-05. Operation rights** (checked at the authoritative decision, i.e. the leader immediately before proposing and
again deterministically at apply using the identity set carried in the command):

| Operation | Required |
|---|---|
| Open for read / ReadAll / live read | ReadData on the inode |
| Open for write, truncate, WriteAll over existing, SetLength | WriteData on the inode |
| Append mode / Append helper | AppendData (or WriteData) on the inode |
| Create file / directory | CreateFiles / CreateDirectories on the parent |
| Delete file or directory | Delete on the inode OR DeleteSubdirectoriesAndFiles on the parent |
| Rename/move | delete rights on the source (as above) + create right on the destination parent (+ delete rights on an overwritten destination) |
| Enumerate | ListDirectory on the directory |
| GetInfo / Exists | ReadAttributes on the inode OR ListDirectory on its parent |
| GetAccessControl | ReadPermissions |
| SetAccessControl | ChangePermissions |
| SetOwner | TakeOwnership (owner := caller) or Administrators (any owner) |
| Copy | ReadData on the source + create right on the destination parent; the copy's descriptor is inherited from the destination parent with the caller as owner |
| Named stream operations | the same rights on the owning inode |

Traverse checking SHALL be bypassed by default (NTFS "bypass traverse checking"); a volume option
`EnforceTraverseChecking` requires Traverse on every ancestor directory.

**ACL-06. Layering.** Volume grants (`DfsPermissions`, FSD/ARCHITECTURE §11) remain the coarse gate (like share
permissions); an operation requires BOTH the volume grant and the ACL. The default root descriptor of a new volume is
owner = creating principal, ACL = `Everyone: FullControl (ContainerInherit|ObjectInherit)` unless the volume spec
supplies one.

**ACL-07. Consistency.** ACL changes, ownership changes and group membership changes SHALL be atomic, durable,
idempotent under the operation-id rules, included in snapshots (snapshot reads evaluate the snapshot's descriptors
with the *current* identity/group database), included in backups, participate in transaction conflict detection
(an ACL change is a revision of the inode), and SHALL NOT be bypassable through direct object reads, transactions,
copies, restores, streams, or stale handles. Rights are evaluated at open; an open handle keeps its granted access
(NTFS semantics) until closed or expired.

**ACL-08. Group changes** take effect for operations authorised after the change commits in the control group and is
applied by the deciding leader; each command carries the identity set the leader resolved, so apply is deterministic.

## A4. Alternate data streams

**ADS-01. Syntax.** A path's final component MAY address a named stream: `name:stream`. `name::$DATA` addresses the
default (unnamed) stream and normalises to `name`; `name:stream:$DATA` normalises to `name:stream`. Any other stream
type, a colon in a non-final component, an empty file name, an empty stream name (other than `::$DATA`), or more than
one stream separator SHALL be rejected as an invalid path. Stream names follow component rules (NFC, ≤ 255 UTF-8
bytes, no `/`, `\`, NUL or `:`). The root has no streams.

**ADS-02. Model.** Every file and directory inode has a default stream (files only carry default-stream data) and up to
`MaxStreamsPerInode` (default 1024) named streams. Each named stream has its own data (inline or extents), length,
revision, sparse semantics and writer reservation. Streams share the inode's identity, owner, security descriptor,
storage policy and timestamps (modification of any stream updates the inode's modification time).

**ADS-03. Operations.** Every file operation that accepts a path (open in every mode, read/write all bytes/text/lines,
append, exists, get info, delete, SetLength via stream) SHALL accept a stream path. Opening a stream with a creating
mode on an absent file SHALL create the file (empty default stream) and the stream atomically. `Exists("/f:s")` is
true only when the stream exists. Deleting `"/f:s"` removes only that stream; deleting the file removes all of its
streams atomically; a stream SHALL NOT survive the deletion of its file. `ListStreamsAsync(path)` returns the named
streams with lengths and revisions. Directory enumeration SHALL NOT list streams.

**ADS-04. Copy/move/snapshot/backup.** Move/rename keeps all streams (same inode). Copy copies the default stream and
all named streams atomically. Snapshots, backups and restores include streams. WriteAll/Copy over an existing file
replaces only the addressed stream (default stream for a plain path) and keeps other streams.

**ADS-05. Sharing and fencing.** Share-mode compatibility and writer exclusivity SHALL be evaluated per stream, except
that deleting or renaming the file requires `FileShare.Delete` from handles on any of its streams. Fencing generations
are per stream. Transactions record stream revisions in their read sets.

**ADS-06. Limits and errors.** Exceeding `MaxStreamsPerInode` → `LimitExceeded`. Opening an absent stream with
`FileMode.Open` → `FileNotFoundException`. Stream syntax on a directory path in a directory operation (e.g.
`CreateDirectory("/d:s")`) → invalid path.

## A5. Test catalog additions

Same conventions as the test plan (§2, §7, API.md §G). Suites: **ACL** (34), **POOL** (18), **ADS** (26) — 78 named
cases. Their IDs are in `TestCatalog.Addendum` (not in `TestCatalog.All`, which remains the 586 cases of the plan).

### A5.1 ACL — NTFS-like permissions

**Invariants:** INV-08, INV-12, INV-13.

| Test ID | Level | Test name | What it tests / required assertion |
|---|---|---|---|
| ACL-001 | U | `DefaultRootDescriptor` | A new volume root has owner = creator and `Everyone: FullControl (CI|OI)`; a new file shows that ACE as inherited from `/`. |
| ACL-002 | U | `RightsBitValuesMatchNtfs` | `DfsFileSystemRights` numeric values and composites equal the NTFS `FileSystemRights` values. |
| ACL-003 | U | `IdentityParsingAndWellKnowns` | Parse/format `user:`, `group:` and well-known identities; reject malformed or unknown kinds. |
| ACL-004 | U | `ExplicitDenyBeatsAllow` | An explicit Deny of ReadData for a user overrides an explicit Allow FullControl for a group containing the user. |
| ACL-005 | U | `CanonicalOrderInheritedVsExplicit` | Explicit Allow on a child overrides an inherited Deny from the parent; explicit Deny overrides inherited Allow. |
| ACL-006 | U | `NearestAncestorWins` | Inherited ACEs from the nearest ancestor are evaluated before those of farther ancestors. |
| ACL-007 | U | `PartialRightsAreNotGranted` | Requesting ReadData|WriteData when only ReadData is allowed is denied; no partial handle is created. |
| ACL-008 | U | `ObjectVsContainerInherit` | ObjectInherit ACEs apply to files only, ContainerInherit to directories only, both to both. |
| ACL-009 | U | `InheritOnlyNotApplied` | An InheritOnly ACE does not grant access on the directory it is set on but applies to children. |
| ACL-010 | U | `NoPropagateInherit` | A NoPropagateInherit ACE applies to immediate children but not grandchildren. |
| ACL-011 | U | `ProtectedDescriptorBlocksInheritance` | Setting AreAccessRulesProtected removes all inherited ACEs from evaluation for the subtree root. |
| ACL-012 | U | `CreatorOwnerSubstitution` | An inheritable CreatorOwner ACE grants its rights to the owner of each inheriting object, not to other principals. |
| ACL-013 | U | `OwnerImplicitRights` | The owner can always read and change permissions even when the ACL denies ReadPermissions/ChangePermissions to the owner identity via groups. |
| ACL-014 | U | `TakeOwnershipRules` | A user with TakeOwnership can set itself as owner but not another user; Administrators can set any owner; others are denied. |
| ACL-015 | U | `OperationRightMatrix` | For every operation in ACL-05's table, a principal holding exactly the required right succeeds and one lacking it gets UnauthorizedAccessException, with namespace and data unchanged. |
| ACL-016 | U | `DeleteViaParentDeleteChild` | Deleting is allowed with DeleteSubdirectoriesAndFiles on the parent even when Delete on the file is not granted, and denied when neither is granted. |
| ACL-017 | U | `RenameNeedsBothSides` | Move succeeds only with delete rights at the source and create rights at the destination; each missing side is denied atomically. |
| ACL-018 | U | `CopyInheritsDestinationAcl` | A copied file's descriptor is inherited from the destination parent with the caller as owner; the source's explicit ACEs are not copied. |
| ACL-019 | U | `MoveRecomputesInheritance` | Moving a file from a directory granting Read to a directory denying Read changes effective access immediately; explicit ACEs travel with the inode. |
| ACL-020 | S | `InheritanceChangeIsAtomicForSubtree` | Changing a directory's inheritable ACE is one committed decision: concurrent readers in a deep subtree observe either old or new effective rights, never a mix within one revision. |
| ACL-021 | U | `GroupMembershipEffect` | Adding/removing a user to/from a group changes access for subsequent operations; existing open handles keep their granted access until closed. |
| ACL-022 | U | `VolumeGrantAndAclBothRequired` | Volume Write grant without ACL WriteData is denied, and ACL FullControl without the volume Write grant is denied. |
| ACL-023 | U | `ExistsAndInfoNeedAttributeRights` | Exists/GetInfo require ReadAttributes or parent ListDirectory; lacking both throws UnauthorizedAccessException, never false. |
| ACL-024 | U | `EnumerationNeedsListDirectory` | Enumerating without ListDirectory is denied; with it, entries are listed even if their own ACLs deny reading data. |
| ACL-025 | U | `EnforceTraverseOption` | With EnforceTraverseChecking off, access to `/a/b/f` ignores Traverse on `/a`; with it on, a missing Traverse on `/a` denies the access. |
| ACL-026 | S | `AclIsCheckedAtDecision` | Revoke ReadData between a writer's staging and a reader's open; the open is denied; revoke WriteData before a staged commit and the commit is rejected. |
| ACL-027 | S | `AclChangeIdempotentAndDurable` | Lose the reply of SetAccessControl and retry with the same operation id; one change results and survives leader failover and cold restart. |
| ACL-028 | S | `AclInTransactionsConflicts` | A transaction that read a file conflicts with a concurrent ACL change on that file; a transaction can change an ACL and data atomically. |
| ACL-029 | S | `SnapshotPreservesDescriptors` | A snapshot keeps the descriptors at its boundary; later ACL changes on live files do not change snapshot access decisions. |
| ACL-030 | S | `BackupAndRestorePreserveDescriptors` | Backup and restore carry owners, explicit ACEs and protection flags exactly; the restored volume enforces them. |
| ACL-031 | I | `DirectObjectReadStillNeedsAcl` | A principal denied ReadData cannot obtain read capabilities or object bytes through raw protocol requests. |
| ACL-032 | U | `StreamsShareFileAcl` | Named streams are governed by their file's descriptor; denying ReadData on the file denies every stream. |
| ACL-033 | S | `ReplicatedApplyIsDeterministic` | Group membership changing during commits never makes replicas disagree: all replicas apply identical decisions from the identity set carried in each command. |
| ACL-034 | U | `GetEffectiveRights` | GetEffectiveRights returns exactly the rights the evaluator would grant for a given identity, including inherited and group-derived bits. |

### A5.2 POOL — namespaces and storage pools

**Invariants:** INV-08, INV-11, INV-12, INV-16, INV-18.

| Test ID | Level | Test name | What it tests / required assertion |
|---|---|---|---|
| POOL-001 | U | `VolumesAreSeparateNamespaces` | Identical paths in two volumes are independent; listing, Exists, GetInfo and enumeration of one volume never show the other's entries. |
| POOL-002 | U | `HandlesAndTokensDoNotCrossVolumes` | Handles, continuation tokens, operation ids and snapshot names from one volume are rejected in another. |
| POOL-003 | U | `DefaultPoolContainsAllNodes` | The `default` pool lists every live node; a volume without a pool uses it. |
| POOL-004 | U | `CreatePoolValidation` | Creating an empty pool, an unknown node member or a duplicate name fails with a typed error; deleting an in-use pool fails. |
| POOL-005 | S | `ObjectsStayInPool` | Two volumes on disjoint pools store every object only on their own pool's nodes (verified through inventories), under load. |
| POOL-006 | S | `NoFallbackOutsidePool` | With the pool's nodes unavailable but other nodes healthy, writes fail with PlacementUnavailable instead of using outside nodes. |
| POOL-007 | U | `PolicySatisfiabilityPerPool` | A policy requiring three zones is rejected for a pool spanning two zones even when the cluster has three. |
| POOL-008 | S | `PolicyNamedPoolTier` | Files using a named policy bound to pool `fast` place objects only on `fast` nodes while other files of the volume use the volume pool. |
| POOL-009 | S | `VotersFromPool` | A volume's metadata voters are chosen from its pool's nodes; losing a node outside the pool does not affect its quorum. |
| POOL-010 | S | `RemoveNodeFromPoolMigrates` | Removing a node from a pool migrates its objects to remaining pool nodes copy-before-delete; replica counts never drop below the ack policy. |
| POOL-011 | S | `AddNodeToPoolRebalances` | Adding a node advances the placement epoch and repair converges to the deterministic targets without data loss. |
| POOL-012 | S | `PoolCapacityIsolation` | Filling pool A's capacity rejects writes to volumes in A with CapacityExhausted while volumes in pool B continue normally. |
| POOL-013 | U | `PerPoolStatusReporting` | Pool status reports nodes, volumes, capacity, usage and replica deficits matching inventories. |
| POOL-014 | S | `NodeInSeveralPools` | A node in two pools stores objects for both; removing it from one pool migrates only that pool's objects. |
| POOL-015 | S | `PoolChangeDuringWrites` | Pool membership changes during staging never let a commit count receipts from nodes outside the pool at the commit's placement epoch. |
| POOL-016 | S | `SnapshotRestoreKeepsPool` | Restoring a snapshot into a new volume binds it to a chosen pool and copies objects only onto that pool. |
| POOL-017 | U | `PoolAdminRequiresClusterAdmin` | Pool and group administration is denied to non-admin principals and audited. |
| POOL-018 | I | `PoolsOverRealTransport` | Two pools on a 6-node QUIC/disk cluster keep objects disjoint across a node crash and restart. |

### A5.3 ADS — alternate data streams

**Invariants:** INV-02, INV-07, INV-09, INV-12.

| Test ID | Level | Test name | What it tests / required assertion |
|---|---|---|---|
| ADS-001 | U | `StreamPathSyntax` | Accept `f:s`, `f:s:$DATA`, `f::$DATA` (normalised); reject colon in a non-final component, empty names, two separators, other stream types, root streams. |
| ADS-002 | U | `WriteReadNamedStream` | WriteAllBytes to `/f:s` and read it back exactly; the default stream of `/f` is unaffected. |
| ADS-003 | U | `CreatingStreamCreatesFile` | Writing `/f:s` when `/f` is absent creates `/f` with an empty default stream and the stream atomically. |
| ADS-004 | U | `ExistsDistinguishesStreams` | Exists(`/f:s`) is false for an absent stream on an existing file and true after creation; Exists(`/f`) stays true. |
| ADS-005 | U | `ListStreams` | ListStreamsAsync returns exactly the named streams with correct lengths and revisions, ordinal-sorted. |
| ADS-006 | U | `EnumerationHidesStreams` | Directory enumeration lists `f` once and never lists stream names. |
| ADS-007 | U | `DeleteStreamOnly` | Deleting `/f:s` removes only that stream; the file and other streams remain byte-identical. |
| ADS-008 | U | `DeleteFileRemovesStreams` | Deleting `/f` removes all streams; recreating `/f` does not resurrect any old stream. |
| ADS-009 | U | `MoveKeepsStreams` | Renaming `/f` to `/g` keeps every stream under `/g:*` with the same inode id and bytes. |
| ADS-010 | U | `CopyCopiesAllStreams` | Copy copies the default and all named streams atomically; later changes to the source streams do not affect the copy. |
| ADS-011 | U | `WriteAllReplacesOnlyAddressedStream` | WriteAll on `/f` keeps named streams; WriteAll on `/f:s` keeps the default stream and other streams. |
| ADS-012 | U | `StreamOpenModes` | Every FileMode on a stream path follows FSD §6.1 semantics per stream (CreateNew on existing stream fails, Truncate, Append, OpenOrCreate). |
| ADS-013 | U | `StreamSparseAndTruncate` | Sparse writes, SetLength and truncation on a stream follow the default-stream rules (zero holes, no resurrected tail). |
| ADS-014 | U | `PerStreamSharing` | A writer on `/f:a` does not conflict with a writer on `/f:b` or on `/f`; two writers on `/f:a` conflict. |
| ADS-015 | U | `DeleteNeedsShareDeleteOnAllStreams` | Deleting or renaming `/f` while a handle on `/f:s` lacks FileShare.Delete fails with a sharing violation. |
| ADS-016 | S | `PerStreamFencing` | An expired writer on `/f:s` cannot publish after a replacement writer is granted on the same stream. |
| ADS-017 | U | `StreamLimit` | Creating stream number MaxStreamsPerInode+1 fails with LimitExceeded and changes nothing. |
| ADS-018 | U | `StreamNamesNormalised` | NFC-equivalent stream names address the same stream; case-different names are distinct. |
| ADS-019 | S | `StreamsInTransactions` | A transaction writing `/f` and `/f:s` commits both atomically; a concurrent change to `/f:s` read by a transaction causes a conflict. |
| ADS-020 | S | `SnapshotIncludesStreams` | A snapshot preserves all streams at its boundary after later stream writes and deletes. |
| ADS-021 | S | `BackupRestoreIncludesStreams` | Backup and restore reproduce every stream byte-exactly. |
| ADS-022 | S | `StreamGcAfterFileDelete` | After deleting a file with large streams and releasing pins, the streams' objects are eventually reclaimed; while a reader pins a stream, they are not. |
| ADS-023 | U | `StreamPartialUpdate` | A small overwrite in a large stream uploads only the changed range (extent-level update), like the default stream. |
| ADS-024 | U | `StreamInfo` | GetInfo on `/f:s` reports the stream's length and revision; GetInfo on `/f` reports the default stream and StreamCount. |
| ADS-025 | U | `DirectoryStreams` | Directories can carry named streams (`/d:s`) which are deleted with the directory; `CreateDirectory("/d:s")` is rejected. |
| ADS-026 | I | `StreamsOverRealTransport` | Large named streams round-trip over QUIC/disk across a node crash/restart byte-exactly. |
