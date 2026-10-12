# API additions for suite PUB (PUB-001..PUB-024)

**No harness or SDK members were added.** Every PUB test is expressed with members already listed in `docs/API.md`.

## Behaviour of existing members the PUB tests rely on

These are not new signatures. They are the documented behaviours the tests assume:

| Member / behaviour | Needed by |
|---|---|
| Clients connected through `TestCluster.ConnectAsync` use the cluster's `TestFaults` injector at the client sites `client.op.start`, `client.op.validated`, `client.request.send` and `client.upload.chunk` (`DfsClientOptionsInternal.Faults`). | PUB-011, 012, 014, 022 |
| `client.upload.chunk` is hit once per staged chunk upload (a chunk is `ChunkSize`, 64 KiB with `NodeTimings.Fast`), not once per replica, in upload order. `FaultMatch.Occurrence` counts these hits. | PUB-011, 012, 014, 022 |
| `FaultContext.Operation` is `PutObject` for the storage sites `rpc.request.accepted`, `object.write`, `object.before-flush` and `object.after-flush` when they handle a client upload. It is `CommitFile`, `WriteAll` or `Append` for the leader sites (`commit.receipt-validated`, `raft.before-propose`, `raft.after-commit`, `apply.after`, `rpc.reply.send`) and for the client sites `client.op.start`, `client.op.validated` and `client.request.send` when they handle a publish request. A stream flush, a stream commit and a WriteThrough write all count as `CommitFile`. | PUB-004, 011, 012, 013, 014, 015, 017, 022, 023 |
| `FaultContext.Object` carries the uploaded object's id at `object.before-flush` (and the other object sites). `FaultAction.Custom` may await a `TaskCompletionSource` completed by the scenario; this suspends only the hitting operation and works in deterministic mode. | PUB-011 (storage-durability variants) |
| `FaultAction.DiskFull()` armed at `object.write` with `Operation = PutObject` makes that node reject the object upload with ENOSPC (capacity exhausted) without affecting its Raft journal, and stops when disarmed. | PUB-017, 022, 023 |
| `FaultContext.Node` is the live node's name ("n0"..) at server sites. At client sites it is not the name of a live node. | PUB-012 |
| `TestNetwork.Log` counts client to node messages by `MessageType`: `PutObject` for uploads and `CommitFile`/`WriteAll`/`Append` for publish requests, with sender `client:{Name}`. | PUB-002, 004, 007, 011, 014, 020 |
| Independent observations use an explicit read handle `{ Open, Read, Share = ReadWrite \| Delete, VersionPinned }` rather than `ReadAll*` (whose sharing mode is unspecified): it coexists with a writer that granted `Share.Read` (test plan §5), and its `OpenRevision` is the pinned revision, equal to `GetInfoAsync(path).Revision` when nothing is published concurrently. | all PUB rows (`AssertCommittedAsync`) |
| `DfsFileInfo.Revision` changes only when committed file content/attributes change: a non-creating open, a read-only open, closing a handle and `AbortAsync` do not create a revision. A creating/truncating open commits its effect in the open, and the handle's `OpenRevision` is the revision after that effect. | PUB-001, 016, 018, 019, 020, 022 |
| `DfsLimitExceededException.LimitName` is the name of the exceeded `DfsVolumeLimits` member (`MaxPublishBytes`, `MaxFileLength`, `MaxTransactionStagedBytes`), as in the TX suite. | PUB-024 |
| A client created with `TestClientOptions.Clock = new TestClock(cluster.Time)` stops renewing leases while that clock is frozen. | PUB-021 |
| `VolumeStateInspection.ReachableObjects()` includes objects referenced by snapshot roots. | PUB-002 |

## Points where the tests deliberately do not over-specify

- **PUB-007:** a flush or commit with nothing pending must not change bytes, length, inode, extent map or upload
  objects, and must not re-publish the old buffer (duplicate append). Whether a *writable* handle's empty flush records
  an identical new revision is not specified by the FSD, so it is allowed. A read-only flush is a documented no-op: it
  sends no publication request and creates no revision.
- **PUB-020:** the FSD forbids finalizer commits but does not require abandoned streams to be collectable. If the
  abandoned `DfsStream` is still reachable after forced full collections, the finalizer path was not exercised and the
  test reports **inconclusive** (skipped, test plan §2.1) after all other checks (no publication, release only by
  server-side close/expiry) have run. It is never reported as a pass.
