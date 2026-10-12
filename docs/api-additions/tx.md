# API / harness additions: TX suite

No additions. Every TX test (TX-001..TX-028) is expressed with the members listed in `docs/API.md`:

- Transaction histories are recorded with `HistoryRecorder.Invoke/Complete/Resolve/Get` (logical real-time stamps) and
  converted to `TxRecord`s for `SerializabilityChecker.CheckStrict` by suite-local helpers
  (`tests/Tedd.Tdfs.Tests.Simulation/Tx/TxHelpers.cs`: `TxHistory`, `TxLog`, `ContentLabels`).
- Server-side transaction state is observed through `ClusterInspector.GetVolumeStateAsync(...).ActiveTransactions`,
  `ClusterInspector.GetTotalCountersAsync().TransactionConflicts` and `NodeInspector.GetGroupAsync`.
- "Abort was requested / no commit was sent / no hidden re-begin" is observed with `TestNetwork.Log.CountMessages`
  filtered by `MessageFilter(From: "client:<name>", Type: MessageType.TxBegin | TxCommit | TxAbort | CommitFile)`.
- Faults use existing sites (`client.request.send`, `rpc.request.accepted`, `commit.receipt-validated`,
  `raft.before-propose`, `raft.after-local-append`, `raft.after-commit`, `apply.after`, `rpc.reply.send`) with
  `FaultMatch(..., Operation: DfsOperationKind.TxCommit | TxOp)` and `Block`, `CrashNode`, `DropReply`, `Custom`.
