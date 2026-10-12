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
- Inode ids are allocated monotonically and never reused (ARCHITECTURE §2.2). Covered by NS-010 and NS-018.
- Overwriting through `WriteAll*` or `Copy(overwrite: true)` keeps the destination inode. A move with overwrite replaces
  the destination inode (FSD §5.4, NS-06). Covered by FILE-001, FILE-012 and NS-008.
- Visibility observations made while a writer is active use explicit handles opened with
  `FileShare.ReadWrite | FileShare.Delete`, as required by test plan §5. Where a read helper (`ReadAll*`) may legitimately
  hold a reservation, the tests treat `DfsSharingViolationException` as a retryable refusal and not as a failure.
  Affected tests: FILE-001, FILE-007 and NS-004.
- The API has no per-directory default storage policies. NS-016 therefore models "directories with different defaults"
  as directories whose resident files use different named policies.
