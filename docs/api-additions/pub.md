# API additions for suite PUB (PUB-001..PUB-024)

**No harness or SDK members were added.** Every PUB test is expressed with members already listed in `docs/API.md`.

## Behaviour of existing members the PUB tests rely on

These are not new signatures. They are the documented behaviours the tests assume:

| Member / behaviour | Needed by |
|---|---|
| Clients connected through `TestCluster.ConnectAsync` use the cluster's `TestFaults` injector at the client sites `client.op.start`, `client.op.validated`, `client.request.send` and `client.upload.chunk` (`DfsClientOptionsInternal.Faults`). | PUB-011, 012, 014, 022 |
| `client.upload.chunk` is hit once per staged chunk upload (a chunk is `ChunkSize`, 64 KiB with `NodeTimings.Fast`), in upload order. `FaultMatch.Occurrence` counts these hits. | PUB-011, 012, 014, 022 |
| `FaultContext.Operation` is `PutObject` for the storage sites `rpc.request.accepted`, `object.write`, `object.before-flush` and `object.after-flush` when they handle a client upload. It is `CommitFile`, `WriteAll` or `Append` for the leader sites (`commit.receipt-validated`, `raft.before-propose`, `raft.after-commit`, `apply.after`, `rpc.reply.send`) and for the client sites `client.op.start`, `client.op.validated` and `client.request.send` when they handle a publish request. A stream flush, a stream commit and a WriteThrough write all count as `CommitFile`. | PUB-004, 011, 012, 013, 014, 015, 017, 022, 023 |
| `FaultContext.Node` is the live node's name ("n0"..) at server sites. At client sites it is not the name of a live node. | PUB-012 |
| `TestNetwork.Log` counts client to node messages by `MessageType`: `PutObject` for uploads and `CommitFile`/`WriteAll`/`Append` for publish requests, with sender `client:{Name}`. | PUB-002, 004, 011, 014, 020 |
| A `DfsStream` abandoned without disposal is collectable: the SDK holds no strong reference from its lease or keepalive bookkeeping. | PUB-020 |
| A client created with `TestClientOptions.Clock = new TestClock(cluster.Time)` stops renewing leases while that clock is frozen. | PUB-021 |
| `VolumeStateInspection.ReachableObjects()` includes objects referenced by snapshot roots. | PUB-002 |
