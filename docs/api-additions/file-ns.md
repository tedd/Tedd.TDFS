# API additions: FILE and NS suites

No new members were added to the harness, SDK or server for FILE-001..FILE-016 and NS-001..NS-018. The tests use only
members listed in `docs/API.md`.

The tests do depend on the following behaviour of existing members. Harness and product implementers should keep it.

## Harness behaviour

| Member | Required behaviour | Used by |
|---|---|---|
| `FaultContext.Operation` at client sites (`client.op.validated`, `client.request.send`, `client.reply.received`) | Carries the public operation kind: `WriteAll` (WriteAll*), `Append` (Append*), `Copy`, `Read` (read helpers and stream reads). | FILE-002, FILE-010, FILE-016 |
| `FaultContext.Operation` at `rpc.request.accepted` / `rpc.reply.send` | Equals `MessageTypes.OperationOf(type)` of the request being handled or replied to. | FILE-002, FILE-008, FILE-009, FILE-010, FILE-014, NS-017 |
| `FaultContext.Operation` at `raft.before-propose`, `raft.after-local-append`, `raft.after-commit`, `apply.after` | Equals the operation kind of the proposed command, for example `WriteAll`, `Append`, `Copy`, `CreateDirectory`, `Move` or `Delete`. | FILE-002, FILE-008, FILE-010, FILE-016 |
| `FaultSites.ClientUploadChunk` | Hit once per uploaded chunk, not once per replica. The 1-based matching hit therefore counts chunks. | FILE-002, FILE-016 |
| `FaultAction.Custom` | An exception from the action, thrown synchronously or from the returned task, surfaces at the hit site exactly like `FaultAction.Throw`. Awaiting inside the action suspends only the hitting operation, and this must work in deterministic mode. A `Custom` arm counts as `Triggered`. | FILE-002, FILE-009, FILE-010, FILE-014, FILE-016 |
| `FaultMatch.Count = int.MaxValue` | Supported. It means a persistent fault that fires on every matching hit. | FILE-002, FILE-008, FILE-010, FILE-014, NS-017 |
| `VolumeStateInspection.Pins[].Kind` | Directory-enumeration continuation pins report `"Enumeration"`, as listed in the PinRecord kinds of ARCHITECTURE §5.1. | NS-014 |
| `VolumeStateInspection.InodeCount` | Counts inodes in the applied state. It drops once deleted or detached inodes are reclaimed. | NS-001, NS-002, NS-003, NS-004, NS-005, NS-006, NS-007, NS-008, NS-015 |
| `MessageLog.CountBytes(new MessageFilter(To: "client:<name>"))` | Counts the bytes delivered to that client since the last `Reset()`. | FILE-007 |
| `ClusterInspector.GetFileAsync(...).Policy` | Holds the same policy name string as `DfsFileInfo.StoragePolicy`. | NS-016 |

## Product semantics the tests assert (from FSD and ARCHITECTURE)

- `DfsFileInfo.Revision` and `DfsDirectoryInfo.Revision` are the Raft index of the entry that last modified the inode,
  so they equal `CommitToken.Sequence` of that mutation (ARCHITECTURE §2.2). Covered by NS-018.
- The tests do NOT assume that taking a writer reservation without publishing leaves `Revision` unchanged (a writable
  open advances the inode's persisted writer generation, which an implementation may count as a modification). Where a
  failed or not-yet-committed helper may have held a reservation, "unchanged" is asserted through bytes, length, inode
  identity and the published extent map (`FileInspection.Extents`) instead. Affected tests: FILE-002, FILE-011 (self-copy),
  FILE-015.
- Inode ids are allocated monotonically and never reused (ARCHITECTURE §2.2). Covered by NS-010 and NS-018.
- Overwriting through `WriteAll*` or `Copy(overwrite: true)` keeps the destination inode. A move with overwrite replaces
  the destination inode (FSD §5.4, NS-06). Covered by FILE-001, FILE-010, FILE-012 and NS-008.
- Visibility observations made while a writer is active use explicit handles opened with
  `FileShare.ReadWrite | FileShare.Delete` and `DfsReadView.VersionPinned`, as required by test plan §5. The pinned view
  matters: through a live handle every read call may select a newer revision (FSD §6.2), so a multi-call live read that
  straddles a publication is a legal composite and not evidence of a bug. Where a helper's implicit reservation (or a
  read helper's internal handle) may legitimately refuse such a reader, the tests treat `DfsSharingViolationException` as
  a retryable refusal and fall back to metadata/extent checks. Affected tests: FILE-001, FILE-002, FILE-007, FILE-010,
  FILE-015, NS-004, NS-010 and NS-011. Writers that are legitimately refused retry with bounded loops that outlast the
  bounded readers, and readers leave reservation-free gaps, so a correct implementation cannot be starved.
- "No authoritative answer" failures (FILE-010 observers, FILE-014) may surface as typed availability exceptions,
  `TimeoutException`, or a transport `RpcConnectionException`; the FSD requires connectivity failures to propagate (never
  as `false`/absence), not a specific type.
- After the winning move of two concurrent cross moves commits, the loser's destination parent path no longer exists, so
  `DirectoryNotFoundException` is an accepted explicit rejection in NS-007's concurrent variant.
- Continuation tokens: reuse for another volume, directory, filter or principal must be rejected (NS-013). A corrupted
  token for the same query is not required to be rejected (the FSD does not mandate integrity-protected tokens), but if
  accepted it may only continue that query's own pinned snapshot.
- The API has no per-directory default storage policies. NS-016 therefore models "directories with different defaults"
  as directories whose resident files use different named policies.
