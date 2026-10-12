# Tedd.TDFS — Internal module layout (implementation guide)

This complements [ARCHITECTURE.md](ARCHITECTURE.md) (behaviour) and [API.md](API.md) (public/test-visible
signatures). It fixes *where* code lives and *how* components plug together so that several engineers can work on
different features concurrently with minimal merge conflicts. Feature code goes into **feature-named partial files**;
shared registries are extended by adding one line in a `*.Registrations.cs` file.

## 1. Server (`src/Tedd.Tdfs.Server`)

```
Runtime/        NodeHost, NodeServices (service container), NodeLoop glue, RpcDispatcher, PeerConnections,
                Authentication (handshake), BackgroundTasks (periodic jobs on the node loop), Metrics/NodeCounters
Raft/           RaftGroupHost, GroupRuntime, RaftStorageAdapter (Storage.RaftLogStore -> Consensus.IRaftLogView),
                IReplicatedStateMachine, CheckpointCoordinator, RaftTransport (RaftBatch send/receive)
Control/        ControlState (deterministic, SortedDictionary-based), ControlCommands, ControlStateMachine,
                ControlService (cluster map, join, incarnation, admin RPCs), MembershipManager (voter promotion,
                volume voter placement), ControlStateMachine.<Feature>.cs partials
Volumes/
  State/        VolumeState (immutable root, partial record), InodeRecord, ExtentRef, HandleRecord, SessionRecord,
                OutcomeRecord, StagingRecord, SnapshotRecord, PinRecord, TxRecord, DerefLog, BackupProgress
  Commands/     VolumeCommand base + one file per command family; VolumeCommandCodec + VolumeCommandCodec.Registrations.cs
  Apply/        VolumeStateMachine (dispatch, OperationGate, checkpoint) + partials:
                VolumeStateMachine.Namespace.cs, .Handles.cs, .Sessions.cs, .Publication.cs, .Transactions.cs,
                .Snapshots.cs, .Gc.cs, .Backup.cs, .Policy.cs, .Security.cs
  Service/      VolumeService (leader-side RPC handlers) + partials per feature: VolumeService.Files.cs,
                .Namespace.cs, .Handles.cs (lease tracking/expiry proposals), .Transactions.cs (working roots),
                .Snapshots.cs, .Enumeration.cs (continuation cache), .Gc.cs, .Repair.cs, .Backup.cs, .Outcomes.cs
  VolumeHost    per-volume composition: group runtime + state machine + service + leader-only jobs
Data/           ObjectService (PutObject/GetObject/Locate/Replicate/Delete/Copy handlers), Receipts (MAC),
                Capabilities (staging + per-object read capabilities), InventoryReporter, Scrubber, RepairWorker
Backup/         BackupExporter (leader side, per target), BackupNodeHost runtime, BackupReceiver, BackupCatalog
Security/       KeyHierarchy (HKDF from cluster secret: NodeAuth, Receipt, Capability, StorageKek), PrincipalStore
                helpers (PBKDF2 verifiers), Authorization (permission checks), AuditLog
Admin/          AdminService (AdminRequest dispatch) + partials per admin area
Inspection/     NodeInspector, BackupInspector, inspection record builders
Additions/      test-requested inspector additions (partial NodeInspector/BackupInspector), one folder per suite group
```

### 1.1 Key contracts

```csharp
// Runtime
internal sealed class NodeServices          // created once per NodeHost on the node loop
{
    NodeOptions Options; NodeEnvironment Env; INodeLoop Loop; TimeProvider Time; IRandomSource Random;
    IFaultInjector Faults; KeyHierarchy Keys; Superblock Superblock; RpcDispatcher Rpc; PeerConnections Peers;
    RaftGroupHost Raft; ControlHost Control; VolumeRegistry Volumes; ObjectService Objects; NodeMetrics Metrics;
    BackgroundTasks Jobs; INodeLog Log;
}
internal delegate ValueTask<IWireMessage> RpcHandler(RpcContext context, IWireMessage request);
internal sealed class RpcContext { IRpcConnection Connection; RpcPeerIdentity Identity; CancellationToken Cancellation; MessageType Type; }
internal sealed class RpcDispatcher
{
    void Register<TRequest>(RpcHandler handler, RpcAccess access) where TRequest : IWireMessage<TRequest>; // access: Anonymous (handshake only), Node, Client, NodeOrClient, Backup
    // errors thrown by handlers are mapped with DfsErrors.ToCode -> ErrorReplyMessage (leader hint for NotLeader)
}
// Raft
internal interface IReplicatedStateMachine
{
    string GroupName { get; }
    ApplyOutcome Apply(ulong index, ulong term, ReadOnlyMemory<byte> command);   // deterministic, synchronous
    void WriteCheckpoint(IBufferWriter<byte> output);                             // state at AppliedIndex
    void RestoreCheckpoint(ReadOnlySequence<byte> data, ulong index, ulong term);
    ulong AppliedIndex { get; }
}
internal sealed class RaftGroupHost
{
    GroupRuntime Open(string group, RaftConfState initialConf, IReplicatedStateMachine sm, ...);
    Task<ApplyOutcome> ProposeAsync(string group, ReadOnlyMemory<byte> command, CancellationToken ct); // completes after apply; NotLeader -> DfsUnavailableException(NotLeader, LeaderHint)
    Task<ulong> ReadIndexAsync(string group, CancellationToken ct);   // linearizable barrier; returns applied index >= read index
    bool IsLeader(string group); string? LeaderHint(string group);
    event Action<string, ulong> Applied;                               // group, index (for waiters)
}
```

### 1.2 Volume state machine pattern

* `VolumeState` is an immutable `partial record`; features add properties in their own partial file with
  `ImmutableSortedDictionary`/immutable types and **initialise defaults in `VolumeState.Empty(...)` via a
  partial method** (`static partial void InitFeature(ref VolumeStateBuilder b)` style) or with property initialisers.
* Commands: `abstract record VolumeCommand(OperationHeader? Op)`; each concrete command has a `VolumeCommandKind`
  value (ushort; ranges reserved per feature: 1-99 core/sessions, 100-199 namespace, 200-299 handles/publication,
  300-399 transactions, 400-499 snapshots/pins, 500-599 GC/repair, 600-699 backup, 700-799 policy/security/keys).
  Encoding via `WireWriter`; registered in `VolumeCommandCodec.Registrations.cs`.
* `VolumeStateMachine.Apply` → decode → `OperationGate` (ARCHITECTURE §5.2) → feature handler
  `ApplyResult Apply<Feature>(VolumeState s, Cmd c, ApplyContext ctx, out VolumeState next)` → outcome stored.
* Leader-only, non-replicated state (lease timers, tx working roots, enumeration cache, in-flight op coalescing,
  inventory/location maps, repair queues, exporter state) lives in `VolumeService` partials and is rebuilt after
  leadership changes.

## 2. Client (`src/Tedd.Tdfs.Client`)

```
Internal/Runtime/   ClientRuntime (options, transport, time, random, faults, mutations), ConnectionPool,
                    ClientConnection (handshake, reconnect), ClusterMapCache
Internal/Rpc/       RpcInvoker (leader routing, NotLeader reroute, retries with same op id, deadline, unknown-outcome
                    classification), RequestFactory
Internal/Sessions/  VolumeSession (session id, sequence allocator, ack watermark, keepalive timer, handle registry)
Internal/Data/      DataPlane (object upload to placement targets with fallback and receipts; verified range reads with
                    replica failover; read capability handling), ChunkPlanner
Internal/Overlay/   StagedOverlay (sorted ranges of buffered bytes / staged object ranges, truncation watermark)
Files/              DfsFileClient (+ partials per helper family), Directories/, Streams/DfsStream (+ partials: .Read.cs,
                    .Write.cs, .Publish.cs, .Lifecycle.cs), Transactions/, Snapshots/, Backups/, Admin/
```

## 3. Harness (`tests/Tedd.Tdfs.Testing`)

```
Cluster/       TestCluster (+ partials: .Lifecycle.cs, .Clients.cs, .Waits.cs), ClusterTest, ClusterBuilder (bootstrap,
               per-node environments), TestClusterOptions
Nodes/         TestNode, TestBackupNode, TestDisk (SimulatedMedium or FaultingFileSystemMedium), node environments
Network/       TestNetwork -> HarnessLinkPolicy (implements ILinkPolicy; partitions, cuts, black holes, rules, holds, log)
Faults/        TestFaults -> HarnessFaultInjector (implements IFaultInjector; arms, coverage registry)
Simulation/    SimScheduler, SimNodeLoop, SimTimeProvider, SimNetwork (deterministic mode)
Inspection/    ClusterInspector
Protocol/      RawProtocolClient
Raft/          RaftSimulation
History/, Models/, Catalog/, Data/   (implemented)
Additions/     suite-requested additions (partials), one folder per suite group
```

## 4. Conventions for feature work

* Each feature owns its partial files; touching another feature's partial requires a short note in the commit message.
* New wire fields: add as **optional extensions** if the message already shipped in `main`; otherwise add fields
  directly and update `docs/PROTOCOL.md` in the same commit.
* Every new RPC handler: register in `<Area>.Registrations.cs`, document access level, add fault-site hits where the
  test plan's stage table (§8.2) applies.
* Every replicated decision that depends on time carries the leader's timestamp in the command.
