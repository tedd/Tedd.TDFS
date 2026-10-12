# API / harness additions requested by the ACL suite

Branch `tests/acl` (from `impl/addendum-api`). Suite: FSD Addendum A §A5.1, ACL-001..ACL-034. All additions are new
files; no existing signature was changed. Harness members are stubs (`NotImplementedException` via `Stub.NotYet`) whose
XML docs define the required behaviour.

## Harness stubs (tests/Tedd.Tdfs.Testing/Additions/acl/)

### `RawProtocolClient.TryReadRangeAsync` and `RawProtocolClient.GetObjectWithCapabilityAsync` — `RawProtocolClient.acl.cs`

```csharp
public Task<RawReadAttempt> TryReadRangeAsync(string volume, string path, long offset, int length);
public Task<RawObjectRead> GetObjectWithCapabilityAsync(string volume, ObjectId id, ReadOnlyMemory<byte> capability);

public sealed record RawReadAttempt(DfsErrorCode Code, byte[]? InlineBytes, IReadOnlyList<ObjectId> Objects,
    IReadOnlyList<ReadOnlyMemory<byte>> Capabilities) { public bool DisclosedAnything { get; } }
public sealed record RawObjectRead(DfsErrorCode Code, byte[]? Data);
```

`TryReadRangeAsync` sends, verbatim and without SDK checks, the read-path requests an SDK reader uses (`OpenHandle` with
FileMode.Open / FileAccess.Read / FileShare.ReadWrite|Delete / live view, `ReadRange`, `CloseHandle`) to the connected
node (the volume leader); a stream path (`/f:s`) is sent as is. It stops at the first error reply and returns that code
(`None` when all steps succeeded) plus everything the server returned that could disclose or fetch data: inline bytes,
the object ids of the returned extents and the read capability for each (`Capabilities[i]` authorizes `Objects[i]`).
`GetObjectWithCapabilityAsync` sends `GetObject` for a whole object to the connected storage node presenting a capability
verbatim (possibly one issued to another principal or on another connection) and returns the code and the verified
plaintext when served.

Why: the existing raw members open only for creation/writing (`SendPathOperationAsync`) and read objects only without a
capability (`GetObjectDirectAsync`), so "a principal denied ReadData cannot obtain read capabilities or object bytes
through raw protocol requests" cannot be expressed. Message bodies for `OpenHandle`/`ReadRange` do not exist yet on this
branch, so `CallAsync` cannot be used either.

Used by: ACL-031 (`ACL_031_DirectObjectReadStillNeedsAcl`).

## Test logic that is not a harness addition

* `tests/Tedd.Tdfs.Tests.Unit/Acl/AclReference.cs` — `NtfsAclReference`, the independent oracle. It keeps its own
  namespace tree and identity database, materializes each object's ACL by NTFS-style **eager** top-down propagation
  (CreatorOwner resolved at every object as NTFS stores it), and decides access with an AccessCheck loop over
  allowed/denied masks; `Allows(AclTableRow, ...)` encodes the ACL-05 table (including traverse enforcement). It shares no
  code with `DfsAccessEvaluator`; `ACL_034_GetEffectiveRights_ReferenceModelSelfCheck` checks it against hand-computed
  NTFS outcomes and differentially against the product evaluator on 400 random chains (passes today).
* `tests/Tedd.Tdfs.Tests.Unit/Acl/AclHelpers.cs` — `AclWorld` keeps a cluster volume and the reference in lockstep and
  cross-checks `GetAccessControl` (owner, protection, every entry in order with flags and `InheritedFrom`) and
  `GetEffectiveRights(path, user:x)` against the reference.
* `tests/Tedd.Tdfs.Tests.Unit/Acl/AclOperationMatrix.cs` — ACL-015: 46 SDK operations covering every row of the ACL-05
  table (verified against the embedded addendum by `ACL_015_OperationRightMatrix_CoversEveryTableRow`), each with grant
  sets holding exactly the required right (every OR alternative separately) and grant sets holding every right except it.

## Behavioural expectations the tests encode (for implementers)

Where the addendum or API.md leaves room, the tests assume the following:

* **Descriptor reporting.** `GetAccessControl` returns explicit entries in canonical order (Deny before Allow, otherwise
  stored order) followed by the inherited entries generation by generation (API.md §H.1), each inherited entry with
  `InheritedFrom` set to the ancestor's normalized path for live paths. Inheritance flags of inherited entries follow
  §H.1 exactly (file: none; directory: CI kept with IO cleared, NP → none, OI-only → OI|IO). An applicable inherited
  CreatorOwner entry is reported as an entry for the current owner with **no** inheritance/propagation flags, followed
  (if still inheritable) by a CreatorOwner template with the same inheritance flags and InheritOnly. A stream path
  returns its file's descriptor; `GetEffectiveRights` on a stream path equals the file's. ACL-001, 008..012, 032, 034.
* **CreatorOwner substitution is dynamic**: it follows the inode's *current* owner (API.md §H.1 "current"), so changing
  the owner moves the substituted rights. Explicit CreatorOwner entries match nobody. ACL-012.
* **GetEffectiveRights(path, user:x)** resolves x's groups and ClusterAdmin flag from the identity database and returns
  `DfsAccessEvaluator.GrantedRights` semantics (Synchronize reported as entries decide; volume grants not folded in).
  The tests call it as the harness admin (who always holds ReadPermissions in the test trees); `identity: null` returns
  the caller's own rights. ACL-001, 004, 007, 012, 013, 022, 034.
* **Owner implicit rights** apply to every member of an owning group. ACL-013.
* **SetOwner(CreatorOwner)** is rejected with `ArgumentException` (client or server side). ACL-014.
* **Create rights.** Creating a file (CreateNew open, WriteAll to a new path) needs only CreateFiles on the parent and
  creating a directory only CreateDirectories; the new inode is owned by the creator. Moving a **directory** needs
  CreateDirectories on the destination parent, a file CreateFiles; a rename within one directory needs the create right
  on that directory too. Copy copies all named streams; the copy has no explicit entries, is not protected and is owned
  by the caller. ACL-015, 017, 018.
* **Exists/GetInfo** without ReadAttributes on the entry and without ListDirectory on the parent throw
  `UnauthorizedAccessException` (never `false`, never not-found) — also for stream paths and directories. ACL-023.
* **Enumeration** needs only ListDirectory on the directory; entries (names, kinds, inode ids, lengths) are listed as the
  admin sees them regardless of the entries' own ACLs. ACL-024.
* **Traverse enforcement** (`EnforceTraverseChecking`, reported in `DfsVolumeInfo`) requires Traverse on every proper
  ancestor directory of every path an operation uses (root and the immediate parent included). ACL-025.
* **Volume grants** missing for an authenticated principal surface as `AccessDenied` (`UnauthorizedAccessException`) even
  when the ACL allows the operation. ACL-022.
* **Handles vs. authoritative commits.** A handle keeps the access granted at open: reads and writes/flushes through a
  handle opened before a group removal still succeed (ACL-021). Operations whose authoritative decision comes later —
  opens themselves, WriteAll/Append helper commits and transaction commits — are re-checked at the leader immediately
  before proposing and again at apply, so a revocation that lands while the request is held at F04 or F10 rejects it
  (ACL-026). A transaction commit rejected by such a revocation may report `AccessDenied` or `TransactionConflict` (the
  ACL change is a revision of the inode); a later attempt is `AccessDenied`.
* **Fault contexts.** At `rpc.request.accepted` and `raft.before-propose`, opens report `DfsOperationKind.Open`, WriteAll
  `WriteAll`, Append `Append`, transaction commits `TxCommit`, with `Volume` and (for path operations) the normalized
  `Path`. At `rpc.reply.send`, a SetAccessControl reply carries `Volume`, `Path` and its `OperationId`. ACL-026, 027.
* **Idempotent ACL changes.** The SDK retries a SetAccessControl whose reply was lost with the same operation id and
  receives the stored outcome; `QueryOperationAsync(id)` reports `Committed` with the same token; no second application
  (a later change by another client is not overwritten). ACL-027.
* **Transactions.** Transaction-bound `Files.SetAccessControlAsync` exists and applies at commit atomically with the
  transaction's data writes; an aborted transaction changes neither. A transaction that read a file conflicts with a
  concurrent SetAccessControl **or SetOwner** on it. ACL-028.
* **Snapshots and restores.** Snapshot views return the boundary-time descriptors (including inherited entries from the
  snapshot's ancestors) and evaluate them with the current group database. Snapshot restore and backup restore
  reproduce the root descriptor, owners, explicit entries (order and flags), protection and named streams; users need a
  fresh volume grant on the restored volume (granted by the test). ACL-029, 030.
* **Group changes** take effect once applied by the deciding volume leader's control-group replica; ACL-033 waits for that
  (via `NodeInspector.GetGroupAsync("control")`) before treating a membership as definite. ACL-021 (single node) expects
  the first operation after `AddGroupMemberAsync`/`RemoveGroupMemberAsync` returns to see the change.
* **Volume creator.** `DfsAdminClient.CreateVolumeAsync` without `RootSecurity` gives the root `DefaultRoot(user:<caller>)`;
  a `RootSecurity` in the spec (including via `TestVolumeSpec`) is stored exactly. ACL-001.
* **Raw read path** (ACL-031): a denied principal's `OpenHandle`/`ReadRange` is refused with `AccessDenied` before any
  inline bytes, extent object ids or capabilities are returned; storage nodes refuse `GetObject` without a capability and
  with a capability issued to another principal (`AccessDenied`, no data), also after restarting from disk.
