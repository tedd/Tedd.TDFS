# Tedd.TDFS — Internal module layout (implementation guide)

This complements [ARCHITECTURE.md](ARCHITECTURE.md) (behaviour) and [API.md](API.md) (public/test-visible
signatures). It fixes *where* code lives and *how* components plug together so that several engineers can work on
different features concurrently with minimal merge conflicts. Feature code goes into **feature-named partial files**;
shared registries are extended by adding one line in a `*.Registrations.cs` file.

## 1. Server (`src/Tedd.Tdfs.Server`)

```
Runtime/        NodeRuntime (start/join/stop; feature hooks in NodeRuntime.Registrations.cs), NodeServices (service
                container), RpcDispatcher (+ .Handshake.cs), HandshakeClient, PeerConnections, BackgroundTasks (periodic
                jobs on the node loop), NodeMetrics (NodeCounters), StorageCleanup
Raft/           RaftGroupHost, GroupRuntime (+ .Ready.cs Ready loop, .Snapshots.cs checkpoints/install),
                RaftStorageAdapter (Storage.RaftLogStore -> Consensus.IRaftLogView), IReplicatedStateMachine,
                SnapshotEnvelope, RaftWireMapper, RaftTransport (RaftBatch batching, RaftSnapshotChunk)
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
                .Snapshots.cs, .Enumeration.cs (continuation cache), .Gc.cs, .Repair.cs, .Backup.cs, .Outcomes.cs,
                .Restore.cs (snapshot restore coordinator + install target); RestoreControl (control-leader restore steps)
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

Signatures as implemented (namespace `Tedd.Tdfs.Server.*`, all `internal`). Everything runs on the node loop: no
`ConfigureAwait(false)`, no `Task.Run`, timers via `Task.Delay(delay, services.Time)` or `NodeServices.Jobs`.

```csharp
// Runtime/NodeServices.cs — one per NodeHost, created on the node loop
internal sealed class NodeServices
{
    NodeOptions Options; NodeEnvironment Env; INodeLoop Loop; TimeProvider Time; IRandomSource Random; IFaultInjector Faults;
    INodeLog Log; NodeTimings Timings; SafetyMutations Mutations; string NodeName; NodeMetrics Metrics;
    NodeId NodeId; ulong Incarnation; UInt128 BootId; Guid ClusterId; Superblock? Superblock; KeyHierarchy Keys; string Endpoint;
    RpcDispatcher Rpc; PeerConnections Peers; RaftGroupHost Raft; ControlHost Control; VolumeRegistry Volumes;
    ObjectStore ObjectStore; ObjectService Objects; AdminService Admin; MembershipManager Membership; BackgroundTasks Jobs;
    IVolumeStateMachineFactory? VolumeStateMachines; bool IsServing; bool IsStopping;
    ValueTask HitAsync(FaultStage stage, string site, string? volume = null, DfsOperationKind operation = DfsOperationKind.Unknown,
        DfsOperationId? operationId = null, string? path = null, ObjectId? obj = null, int occurrence = 0, string? detail = null,
        CancellationToken ct = default);
    string DataPath(string relative);                       // under Options.DataDirectory
    CancellationTokenSource CreateDeadline(TimeSpan? timeout = null); // node-clock deadline (default: peer call timeout)
    void Info/Debug/Warn/Error(string category, string message);
}

// Runtime/RpcDispatcher.cs
[Flags] internal enum RpcAccess { Anonymous = 0, Client = 1, Node = 2, Backup = 4, JoiningNode = 8, NodeOrClient = Client | Node, Any = Client | Node | Backup }
internal delegate ValueTask<IWireMessage> RpcHandler(RpcContext context, IWireMessage request);
internal sealed class RpcContext
{
    IRpcConnection Connection; RpcPeerIdentity Identity; ConnectionState State; CancellationToken Cancellation; MessageType Type;
    ReadOnlyMemory<byte> Frame; bool ExpectsReply; string? Principal; bool IsNode; NodeId? PeerNode;
}
internal sealed partial class RpcDispatcher
{
    void Register<TRequest>(RpcHandler handler, RpcAccess access) where TRequest : IWireMessage<TRequest>; // once per type
    bool IsRegistered(MessageType type);
}
```

Dispatcher rules (handlers only see authenticated, authorized requests):

* Requests are refused with retryable `Unavailable` until `IsServing` and while stopping; unknown types → `Unsupported`.
* `Client`: the principal must still exist. `Node`: a live member with its registered incarnation and boot id (removed
  → `AccessDenied`, older incarnation/other boot → `StaleIncarnation`), on connections it opened and on connections this
  node opened to it alike (the server's boot id travels in a `HelloChallenge` extension; a server without it is checked by
  incarnation only). `JoiningNode`: also nodes that proved the
  cluster secret but are not (yet) in this node's applied view (seed join, Raft traffic only). `Backup`: backup principals.
* The dispatcher hits `F04 rpc.request.accepted` before and `F13 rpc.reply.send` after the handler (not for Raft traffic).
  An injector may throw `FaultReplyDroppedException` (Core, `Tedd.Tdfs.Faults`) at F13: the request was executed and no
  reply is sent. Handlers that send replies themselves must honour the same exception.
* Handler exceptions become `ErrorReplyMessage.FromException(ex, operationId)` (`DfsErrors.ToCode`); throw
  `GroupRuntime.NotLeader()` / `ControlHost.NotLeader()` (a `DfsUnavailableException` with the leader endpoint hint) to
  redirect. `Internal` errors are logged with the stack trace.

```csharp
// Raft/IReplicatedStateMachine.cs
internal interface IReplicatedStateMachine
{
    string GroupName { get; }
    ApplyOutcome Apply(ulong index, ulong term, ReadOnlyMemory<byte> command); // deterministic, synchronous; no clocks/random/node state
    void WriteCheckpoint(IBufferWriter<byte> output);                         // canonical bytes of the state at AppliedIndex
    void RestoreCheckpoint(ReadOnlySequence<byte> data, ulong index, ulong term);
    ulong AppliedIndex { get; }
}
internal interface ICommandDescriber { ProposalContext Describe(ReadOnlyMemory<byte> command); } // optional: follower fault contexts
internal readonly record struct ProposalContext(DfsOperationKind Operation, DfsOperationId? OperationId = null, string? Path = null);
internal sealed record ApplyOutcome(DfsErrorCode Code, string Message, object? Value)
{
    ulong Index { get; init; } ulong Term { get; init; } bool IsSuccess;
    static ApplyOutcome Success(object? value = null); static ApplyOutcome Failure(DfsErrorCode code, string message, object? value = null);
    Exception ToException(); T GetValueOrThrow<T>();
}

// Raft/GroupRuntime.cs
internal sealed record GroupOpenSpec(string Group, string DisplayName, string? VolumeName, string Directory,
    IReplicatedStateMachine StateMachine, IReadOnlyList<RaftEntry>? BootstrapEntries); // entries only for initial members

// Raft/RaftGroupHost.cs — every Raft group of the node (control + volumes); one tick timer, batched transport
internal sealed class RaftGroupHost
{
    event Action<string, ulong>? Applied;                         // group, applied index (after each apply batch / install)
    event Action<GroupRuntime>? LeadershipChanged;                // role, leader or term changed
    event Action<GroupRuntime, RaftConfState>? ConfApplied;
    IReadOnlyList<GroupRuntime> Groups; GroupRuntime? TryGet(string group);
    Task<GroupRuntime> OpenAsync(GroupOpenSpec spec, CancellationToken ct = default);
    Task CloseAsync(string group, bool deleteData); Task CloseAllAsync();
    Task<ApplyOutcome> ProposeAsync(string group, ReadOnlyMemory<byte> command, ProposalContext context, CancellationToken ct = default);
    Task<ulong> ReadIndexAsync(string group, CancellationToken ct = default);
    bool IsLeader(string group); NodeId? LeaderOf(string group); string? LeaderHint(string group); // hint = leader endpoint
    void TransferLeadership(string group, NodeId target);
    Task<bool> TransferLeadershipAsync(string group, NodeId target, TimeSpan timeout); // true when target leads in time
}
internal sealed partial class GroupRuntime
{
    string Name; string DisplayName; string? VolumeName; IReplicatedStateMachine StateMachine; GroupStats Stats; RaftNode Node;
    RaftRole Role; ulong Term; NodeId? Leader; bool IsLeader; ulong CommitIndex, AppliedIndex, AppliedTerm, LastIndex,
    SnapshotIndex, LogFirstIndex; RaftConfState AppliedConf, Conf; bool IsClosed;
    Task<ApplyOutcome> ProposeAsync(ReadOnlyMemory<byte> command, ProposalContext context, CancellationToken ct = default);
    Task<bool> ProposeConfChangeAsync(RaftConfChange change, CancellationToken ct = default);
    Task<ulong> ReadIndexAsync(CancellationToken ct = default);
    Task WaitForAppliedAsync(ulong index, CancellationToken ct = default);
    IReadOnlyList<RaftPeerProgress> GetProgress(); void TransferLeadership(NodeId target);
    Task WriteCheckpointAsync(); DfsUnavailableException NotLeader();
}
```

Proposal semantics: `ProposeAsync` completes with the `ApplyOutcome` after the entry was applied **on this node**
(a failed outcome is a value, not an exception). Not leader → `DfsUnavailableException(NotLeader)` with the leader hint
and nothing proposed. After proposing, a lost leadership keeps waiting for the entry's fate; a different term at the
index means the proposal lost (retryable `NotLeader`); cancellation (deadline), shutdown, a local reset or an installed
snapshot covering the index before the fate is known → `DfsCommitOutcomeUnknownException` (retry with the same
operation id). Pending proposals are keyed by (index, term): a re-elected leader may append a new entry at the index of
one of its own undecided proposals, and the entry committed there decides both. The fate of an entry appended just
before leadership was lost is only decided when a later leader commits something at its index, which may never happen
in an idle group: background proposals (membership, registration, audit) therefore always pass a deadline
(`NodeServices.CreateDeadline`). `ReadIndexAsync` (leader only) confirms leadership with a heartbeat quorum and returns
once the applied index reached the read index. A failed durable write resets the group: proposals become
outcome-unknown, the journal files are flushed and the log store is reopened from the medium (a failed journal write
faults the journal, and appended-but-unflushed entries must never be acknowledged), then the core is rebuilt; retried
while the medium keeps failing. Opening a group raises a persisted term that is behind the last durable entry's term
(crash between the entry flush and the hard-state write; no vote of that term was ever sent).

Fault sites: F10 `RaftBeforePropose` and `RaftAfterLocalAppend`, F11
`RaftAfterCommitBeforeApply` (per entry, also on followers; `ICommandDescriber` supplies the operation), F12
`ApplyAfterBeforeReply`, F16 `RaftSnapshotInstall` (detail receive/verify/install/activate), F21 `Reconfiguration`;
`RecoveryJournalReplay` is hit by the log store while the group opens. Checkpoints are written when
`AppliedIndex - SnapshotIndex > NodeTimings.RaftSnapshotThreshold` (checkpoint first, then log compaction); lagging
followers receive the latest checkpoint through `RaftSnapshotChunk` (one chunk in flight per follower).

```csharp
// Control/ControlState.cs — deterministic applied control state (read on the loop; never mutate outside apply)
internal sealed class ControlState
{
    Guid ClusterId; string ClusterName; int ControlVoterTarget; ulong PlacementEpoch; ulong AppliedIndex; bool IsBootstrapped;
    SortedDictionary<NodeId, MemberRecord> Members; SortedDictionary<VolumeId, VolumeRecord> Volumes;
    SortedDictionary<string, PrincipalRecord> Principals; SortedDictionary<string, GroupRecord> Groups;
    SortedDictionary<string, PoolRecord> Pools; SortedSet<string> Features; List<AuditEntry> Audit;
    MemberRecord? FindMember(NodeId id); MemberRecord? FindMemberByName(string name);
    VolumeRecord? FindVolume(VolumeId id); VolumeRecord? FindVolumeByName(string name);
    PrincipalRecord? FindPrincipal(string name); DfsPermissions GetPermissions(string principal, VolumeId volume);
    bool IsClusterAdmin(string principal); IReadOnlyList<string> GroupsOf(string principal);
    bool PoolExists(string pool); IReadOnlyList<MemberRecord> PoolMembers(string pool);
}
// VolumeRecord(VolumeCreation Creation, WireVolumeState State, IReadOnlyList<NodeId> Voters, IReadOnlyList<NodeId> Learners,
//   DfsStoragePolicy DefaultPolicy, IReadOnlyDictionary<string, DfsStoragePolicy> NamedPolicies, DfsStoragePolicy? MigrationTarget,
//   IReadOnlyList<DataKeyVersion> DataKeys, IReadOnlyDictionary<string, BackupTargetRecord> BackupTargets)
//   { VolumeId Id; string Name; ulong Epoch; string StoragePool; string GroupName; uint CurrentKeyVersion; bool Hosts(NodeId) }

// Control/ControlHost.cs
internal sealed class ControlHost
{
    ControlStateMachine StateMachine; ControlState State; GroupRuntime? Group; bool IsLeader; NodeId? Leader; MemberRecord? Self;
    bool IsSelfRegistered;                       // live member with this process's incarnation and boot id (receipts allowed)
    CommandHeader NewHeader(UInt128 requestId = default, string principal = "");   // carries the leader's clock
    Task<ApplyOutcome> ProposeAsync(ControlCommand command, CancellationToken ct = default);
    DfsUnavailableException NotLeader(); string? ResolveEndpoint(NodeId node); string NameOf(NodeId node);
    ClusterMapData BuildClusterMap(); DfsServerCapabilities BuildCapabilities(); bool IsReachable(NodeId node);
    void ReportSecurityEvent(string operation, string outcome, string scope, string principal = "-"); // rate limited audit (forwarded to the control leader, AuditEvent)
}
// ControlStateMachine.Changed (event Action<ulong>, the applied index) fires after each applied command or restored checkpoint.

// Data/ObjectService.cs, Data/Capabilities.cs, Data/Receipts.cs
internal sealed class ObjectService
{
    Capabilities Capabilities;
    void Register(RpcDispatcher rpc);   // PutObject/GetObject/LocateObject (NodeOrClient), DeleteObjects/CopyObjects (Node)
    Task<(ObjectReceipt Receipt, bool AlreadyPresent)> StoreAsync(ObjectId obj, VolumeRecord volume, StagingId staging,
        UInt128 checksum, long length, byte encoding, ulong placementEpoch, ReadOnlyMemory<byte> data, DfsOperationKind operation,
        CancellationToken ct);          // local durable store + receipt; NodeUnavailable before the incarnation is registered
}
internal sealed class Capabilities
{
    byte[] IssueStaging(VolumeRecord volume, string principal, StagingId staging, TimeSpan lifetime, out DateTimeOffset expires);
    byte[] IssueRead(VolumeRecord volume, string principal, IEnumerable<ObjectId> objects, TimeSpan lifetime, out DateTimeOffset expires);
    DataCapability Verify(ReadOnlySpan<byte> token, DataCapabilityKind kind, VolumeRecord volume, string principal,
        StagingId? staging = null, ObjectId? obj = null);          // throws UnauthorizedAccessException
}
internal static class Receipts
{
    static ObjectReceipt Issue(byte[] receiptKey, ObjectId obj, VolumeId volume, StagingId staging, UInt128 checksum, long length,
        byte encoding, NodeId node, ulong incarnation, ulong placementEpoch, DateTimeOffset durableAt);
    static ReceiptEvaluation Evaluate(IEnumerable<ObjectReceipt> receipts, byte[] receiptKey, ControlState state, ObjectId obj,
        UInt128 checksum, long length, byte encoding, VolumeId volume, StagingId staging, string? pool = null);
}
internal sealed record ReceiptEvaluation(int Copies, int Zones, IReadOnlyList<NodeId> Holders);   // copies per distinct host
// Receipt key: services.Keys.ReceiptKey; capability key: services.Keys.CapabilityKey (HKDF purposes, Security/KeyHierarchy.cs).

// Admin/AdminService.cs
internal delegate ValueTask<AdminResultMessage> AdminOperationHandler(RpcContext context, AdminRequestMessage request);
internal sealed partial class AdminService
{
    static IReadOnlyList<AdminOp> VolumeOperations;   // VolumeStatus, PolicyMigrationStatus, StartScrub, StartRepair, ReplicaHealth, RestoreFromBackup
    void RegisterVolumeOperation(AdminOp op, AdminOperationHandler handler);   // only those ops; else Unsupported
    GroupStatusData GroupStatus(GroupRuntime group, string displayName);
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
  leadership changes. A lease the leader has not seen yet counts as renewed when first seen (never measured from the
  election: a handle granted by an applied entry whose reply path is still held must not expire).
* Revisions: `InodeRecord.Revision` advances for every change of the record (it is the checkpoint version key
  `(Id, Revision)`, so every change must advance it); `MetadataRevision` only for the inode's own metadata (creation,
  owner, ACL, policy, name/parent). The revision reported for a file (`FileInfoData.Revision`, listings, inspection,
  conditional writes) is `ReportedRevision` = max(default-stream revision, metadata revision): named-stream writes do not
  change it (ADS-011). `CommitFile.BaseRevision` conflicts only when the stream's content changed after the base.
* The leader pre-checks a command by running apply without committing (`DryRun`). A definitely (non-retryably) refused
  request **with** an operation id is still proposed, so apply records the refusal as the id's outcome (an answered id
  never executes later); retryable refusals and id-less refusals are answered from the pre-check. Apply catches every
  exception (except OOM) as a deterministic `Internal` failure of the entry.

### 1.3 Runtime plug-in points (how feature areas attach)

**Volume state machine.** The runtime hosts volume groups without knowing the volume implementation; it depends only
on this interface (`Volumes/VolumeHost.cs`):

```csharp
internal sealed class VolumeHostContext
{
    required NodeServices Services { get; init; }   // node services; never used inside deterministic apply
    required VolumeRecord Record { get; init; }     // control record when the group was opened
    required string GroupName { get; init; }        // "vol:{id}"
    VolumeCreation Creation { get; }                // immutable creation data: the only input of the initial state
    Guid ClusterId { get; }
}
internal interface IVolumeStateMachineFactory
{
    IReplicatedStateMachine Create(VolumeHostContext context);   // state at log index 0, identical on every replica
}
internal interface IVolumeStateInspection        // optional, implemented by the state machine (NodeInspector)
{
    VolumeStateInspection Inspect(); long StagedBytes { get; } long PinnedBytes { get; }
}
internal sealed partial class VolumeHost
{
    VolumeRecord Record; VolumeId Id; string Name; GroupRuntime Group; IReplicatedStateMachine StateMachine; bool IsLeader;
    static partial void CreateDefaultStateMachineFactory(ref IVolumeStateMachineFactory? factory); // production factory
    partial void OnOpened(); partial void OnClosing(); partial void OnRecordChanged(VolumeRecord previous); partial void OnLeadershipChanged();
}
```

* **Production wiring**: the volume area implements `CreateDefaultStateMachineFactory` once, in its own partial file
  (for example `Volumes/Apply/VolumeHost.Factory.cs`: `factory = new VolumeStateMachineFactory();`). Tests may inject a
  factory through `NodeEnvironment.VolumeStateMachines` (internal init property), which takes precedence. Without either,
  volume groups are not hosted and the node logs one error.
* **Determinism**: `Create` must derive the initial state only from `Creation` and `ClusterId`. Members added later
  start from the same initial state with an empty log and receive the log or a checkpoint; initial voters
  (`Creation.InitialVoters`) bootstrap the group with identical configuration entries at term 1, so the first volume
  command is at index > 1 — never assume command indexes.
* **Lifecycle** (`Volumes/VolumeRegistry.cs`, on the node loop): the registry reconciles after every control-state change
  and every 40 ticks. It opens a group for each volume whose record lists this node (`VolumeRecord.Hosts`) in
  `groups/vol-{id:N}`, then calls `OnOpened`; record changes call `OnRecordChanged(previous)`; role/leader/term changes
  call `OnLeadershipChanged`; before closing (volume deleted, node no longer listed and removed from the applied
  configuration, or node stop) it calls `OnClosing`. Deleted volumes, and unlisted ones whose applied configuration no
  longer contains this node, delete their group data; stop keeps it. A node that is unlisted but still a voter of the
  applied configuration keeps hosting the group (its replacement may still be catching up; leaving would reduce the
  group's failure tolerance, RCV-01); one that is only a learner there leaves after a grace period but keeps its log: a
  member the configuration may still count must never lose its log.
  Leader-only jobs (lease expiry, GC, repair, exporter) start in `OnLeadershipChanged` when `IsLeader` and must stop
  themselves when leadership is lost or `Group.IsClosed`.
* **Membership** is owned by the runtime: the control leader's `MembershipManager` chooses volume voters/learners
  (eligible members of the volume's pool per the metadata policy, rendezvous hashing on the volume id) and writes them to the record; the volume
  leader's `VolumeHost.ReconcileMembershipAsync` moves the group one single-server change at a time towards the record.
  A learner is promoted only when `MembershipManager.IsCaughtUp` (it acknowledged entries — replicate state, positive
  match — and lags the commit index by at most `CatchUpLag`; a just-added member's progress starts "recently active" with
  match 0 and does not qualify).
* **Proposing**: `host.Group.ProposeAsync(command, new ProposalContext(kind, opId, path), ct)`; implement
  `ICommandDescriber` on the state machine so follower-side F11 contexts carry the operation.

**RPC handlers and node-wide jobs** (`Runtime/NodeRuntime.Registrations.cs`): each area implements its partial hooks
in a partial file of `NodeRuntime` it owns; they run on the node loop after every service exists (object store open,
before the control group opens; requests are refused until the node serves) and after serving starts respectively:

```csharp
partial void RegisterVolumeHandlers(NodeServices services);   // services.Rpc.Register<CreateFileMessage>(handler, RpcAccess.Client) ...
partial void RegisterDataHandlers(NodeServices services);     // replication, inventory, scrub, repair
partial void RegisterBackupHandlers(NodeServices services);
partial void StartVolumeJobs(NodeServices services);          // services.Jobs.StartPeriodic(name, interval, job, initialDelay)
partial void StartDataJobs(NodeServices services);
partial void StartBackupJobs(NodeServices services);
```

The backup exporter reconciliation is started on demand rather than from `StartBackupJobs`: the volume service's
leadership and control-record hooks call `BackupExporters.OnVolumeChanged`, which starts the periodic job the first time
the node hosts a volume with a registered target or leftover backup state, so nodes of clusters without backups run no
backup job (and their deterministic schedules are unchanged).

Handlers resolve the volume with `services.Volumes.TryGet(VolumeId)` / `TryGet(string name)` / `TryGetByGroup(group)`,
throw `host.Group.NotLeader()` when not leading, and check permissions with
`services.Control.State.GetPermissions(context.Principal!, volumeId)`. `BackgroundTasks.StartPeriodic` jobs stop with
the node; a failed run is logged and the job continues at the next interval.

**Volume-scoped admin operations**: `services.Admin.RegisterVolumeOperation(op, handler)` for `VolumeStatus`,
`PolicyMigrationStatus`, `StartScrub`, `StartRepair`, `ReplicaHealth` and `RestoreFromBackup` (call it from
`RegisterVolumeHandlers`). The handler receives the authenticated `RpcContext` and the raw `AdminRequestMessage`; it does
its own authorization (cluster admin or volume grant) and returns `AdminResultMessage.Create(op, body)`. Unregistered operations answer `Unsupported`. `AdminService.PoolHealthProvider`
(`Func<string, ReplicaHealthData>?`) supplies replica health for `PoolStatus` (zeros otherwise).

**Data plane**: publication receipts are evaluated with `Receipts.Evaluate(receipts, services.Keys.ReceiptKey,
services.Control.State, obj, checksum, length, encoding, volumeId, staging, pool)`; staging and read capabilities are
issued and verified with `services.Objects.Capabilities`. `ObjectService.StoreAsync` is the local store path for repair
and restore (it refuses with `NodeUnavailable` until this incarnation is registered, RCV-02).

**Inspector additions**: `NodeInspector` exposes group digests and stats through `ComputeGroupDigestAsync(group)` and
`GetGroupStatsAsync(group)` (`GroupStats`: start index, entries appended/applied/covered by install, snapshots
installed/aborted/rejected, verification failures, checkpoints written) for suite-specific additions.

## 2. Client (`src/Tedd.Tdfs.Client`)

```
Internal/Runtime/   ClientRuntime (options, transport, time, random, faults, mutations, lifecycle: in-flight tracking,
                    shutdown, Abandon), OperationScope (deadline, caller/kill tokens, F00/F01 hits, classification of
                    never-transmitted failures), ConnectionPool, ClientConnection (handshake, cluster identity, protocol
                    version, poisoning of stale connections), ClusterMapCache (map, pools, discovery order), Waits
                    (TimeProvider delays/waits without sync context), LockedRandom
Internal/Rpc/       RpcInvoker (routing, NotLeader redirects, retries with the same op id, per-attempt and per-operation
                    deadlines, unknown-outcome resolution via QueryOperation(resolve)), RpcRequest, Routers (control
                    leader, any node)
Internal/Sessions/  VolumeContext (volume binding id/epoch, leader routing via GetVolumeRoute, session open, sequence
                    blocks, Read/Mutate helpers, keep-alive timer, weak handle/pin registries, helper staging session),
                    VolumeSession (session id, reserved sequence blocks, ack watermark, the session's volume binding),
                    VolumeBinding (expected volume id/epoch carried by every request about something a session owns,
                    and by every transmission of one operation id)
Internal/Data/      DataPlane (HRW upload with fallback, receipts until the ack policy is met, grace for extra copies;
                    verified range reads with replica failover and LocateObject; never zero-fill), ContentLayout (chunking
                    at ChunkSize boundaries, inline-permission from the voters' failure domains), StagingLease
Internal/Overlay/   StagedOverlay (sorted buffered / uploaded ranges, staged length, truncation point, snapshot/restore)
Internal/Text/      TextCodec (UTF-8 without BOM by default, BOM detection, incremental line splitting)
Internal/Facades/   WireConvert (wire records <-> SDK records), SnapshotAdapters (prepare/abort/resume bounded by timeouts)
Internal/           Validation (paths and options, before any request), AsyncGate, AsyncLock, SyncBridge, ContextFree
Files/              DfsFileClient (+ .Security.cs), Directories/, Streams/DfsStream, Transactions/, Snapshots/, Backups/, Admin/
```

## 3. Harness (`tests/Tedd.Tdfs.Testing`)

```
Cluster/       TestCluster (+ partials: .Lifecycle.cs = bootstrap/static+seed-join, nodes, per-node NodeEnvironment,
               recovery/foreign clusters, disposal; .Clients.cs = SDK/raw/admin clients, principals; .Waits.cs = leaders,
               stability, replication, repair, digest, leadership transfer; .Invariants.cs = leadership history/split
               brain, assumption violations, uncontrolled dependencies), ClusterTest (realtime runs; deterministic runs on a
               driver thread in four SimScheduler phases), HarnessAdmin (raw admin channel to the control leader),
               HarnessStartOptions (internal start settings), TestClusterOptions
Nodes/         TestNode, TestBackupNode, TestDisk (SimulatedMedium or FaultingFileSystemMedium), FencedMedium (per-incarnation
               view fenced at crash/stop)
Network/       TestNetwork -> NetworkFabric (sim/in-memory/QUIC transports wrapped by HarnessTransport: holds, reply drops,
               truncation, tampering, silent death, read limits, replay, attempt/connection accounting) and HarnessLinkPolicy
               (implements ILinkPolicy; partitions, cuts, black holes, mesh, latency, rules, message log)
Faults/        TestFaults -> HarnessFaultInjector (implements IFaultInjector; arms, coverage registry)
Simulation/    SimScheduler, SimNodeLoop, SimTimeProvider, SimNetwork (deterministic mode)
Inspection/    ClusterInspector (harness ground truth from node inspectors)
Protocol/      RawProtocolClient
Internal/      ClusterRuntime (realtime vs deterministic execution context), out-of-process launcher (HostProcess runs
               src/Tedd.Tdfs.Host), PortAllocator, HarnessNodeLog
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
