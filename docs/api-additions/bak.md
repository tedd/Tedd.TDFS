# API additions requested by the BAK suite (BAK-001..BAK-030)

All additions are stubs (`NotImplementedException`) in partial classes or new files; no existing signature was changed.
Tests: `tests/Tedd.Tdfs.Tests.Simulation/BAK/BAKSimulationTests.cs` (S rows) and
`tests/Tedd.Tdfs.Tests.Unit/BAK/BAKUnitTests.cs` (BAK-015).

## 1. New members

### 1.1 `Tedd.Tdfs.Server.BackupInspector` — `src/Tedd.Tdfs.Server/Additions/bak/BackupInspector.bak.cs`

Read-only diagnostics executed on the backup node loop; none may change retained or published state.

| Member | Required behaviour | Used by |
|---|---|---|
| `Task<IReadOnlyList<BackupSnapshotInspection>> GetSnapshotsAsync(string volume)` | Every snapshot the node currently retains for the source volume name, across lineages/epochs (including snapshots kept only by a reader pin after retention expiry), ordered by (lineage start, covered commit). Only completely verified, durably persisted snapshots that were published at least once; in-progress transfers are not listed. `Closure` = complete object dependency closure from the persisted manifest (sorted, distinct); `Retention` = durable destination retention; `ReaderPins` = currently valid local reader pins; `IsPublishedPointer` marks the current pointer. Empty list for an unknown volume. | BAK-004, 005, 009, 013, 017, 020, 023, 024, 026, 027, 030 |
| `Task<BackupReceiveInspection?> GetReceiveStateAsync(string volume)` | The staged, not yet published incoming transfer (or `null`). `ReceivedObjects` = bytes arrived; `DurableObjects` = flushed **and** verified; `MissingObjects` = closure objects not yet durable, computed by the node itself from the received manifest (never from inventory hints; empty before the manifest arrives); `ReceivedBytes` = payload bytes received for this transfer. Survives restarts when the transfer state is resumable. | BAK-006, 007, 009, 016, 028 |
| `Task<IReadOnlyDictionary<ObjectId,int>> GetObjectReferenceCountsAsync()` | For every stored object, the number of retained snapshots (all volumes/lineages, published or staged manifest) referencing it, as the node's retention/GC logic accounts it (tracing implementations report the traced count; unreferenced stored objects report 0). Duplicate deliveries never change counts. | BAK-009, 010, 024, 026, 027 (via `AssertBackupStoreConsistentAsync`) |
| `Task<IReadOnlyList<ObjectId>> VerifyClosureAsync(string volume, string snapshotName)` | Independently re-reads and verifies (identity, length, encoding, checksum) every closure object of the retained snapshot from the durable store; returns missing/corrupt ids (empty = complete). `DfsException(SnapshotNotFound)` when not retained. | every `AssertPublishedStateAsync` call (BAK-004..014, 018, 020..027) |
| `record BackupSnapshotInspection(string Name, SnapshotId Id, CommitToken CoveredCommit, Guid Lineage, bool IsPublishedPointer, DfsRetention Retention, IReadOnlyList<ObjectId> Closure, int ReaderPins)` | Value record (Server namespace). | as above |
| `record BackupReceiveInspection(string SnapshotName, Guid Lineage, ulong VolumeEpoch, bool ManifestReceived, IReadOnlySet<ObjectId> ReceivedObjects, IReadOnlySet<ObjectId> DurableObjects, IReadOnlySet<ObjectId> MissingObjects, long ReceivedBytes)` | Value record (Server namespace). | as above |

### 1.2 `Tedd.Tdfs.Testing.TestCluster` — `tests/Tedd.Tdfs.Testing/Additions/bak/TestCluster.bak.cs`

| Member | Required behaviour | Used by |
|---|---|---|
| `Task<ManualBackupExport> BeginManualBackupExportAsync(string volume, string snapshotName, int backupIndex = 0, CancellationToken ct = default)` | Creates a step-by-step driver of the real export protocol for an existing live snapshot towards backup node `backupIndex` (target must be registered). Acquires a logged source transfer pin and suspends the automatic exporter for that (volume, target) until the driver is disposed. Continues the target's current lineage (new lineage when the target is marked for reseed). A successful publish records backup progress on the source exactly like the automatic exporter. | BAK-006, 007, 009, 010 |
| `Task<RawProtocolClient> ConnectRawBackupAsync(int backupIndex = 0, CancellationToken ct = default)` | A `RawProtocolClient` connected to backup node `backupIndex` (identity `client:raw-b{index}`), unauthenticated until `AuthenticateAsync`. All members behave as for a live node, so mutations exercise the backup's own server-side read-only enforcement (expected `DfsErrorCode.ReadOnly`). | BAK-002, 015, 029 |

### 1.3 `Tedd.Tdfs.Testing.TestBackupNode` — `tests/Tedd.Tdfs.Testing/Additions/bak/TestBackupNode.bak.cs`

| Member | Required behaviour | Used by |
|---|---|---|
| `Task SetFalseInventoryClaimsAsync(string volume, IReadOnlyCollection<ObjectId> objectIds)` | Every `BackupInventory` reply of this node for `volume` additionally claims to hold the given ids (simulated Bloom-filter false positives). Nothing is stored; closure verification, receive state and published state are unaffected. Empty collection clears; cleared by restart. | BAK-007 |
| `Task RestartAsync(Func<BackupNodeOptions, BackupNodeOptions> reconfigure)` | Graceful stop when running (no-op when crashed/stopped), then restart from the durable image with transformed options (e.g. `KeepLastSnapshots = 1`); the reconfiguration persists for later `RestartAsync()` calls. Completes when serving. | BAK-018, 027 |

### 1.4 New harness types — `tests/Tedd.Tdfs.Testing/Additions/bak/ManualBackupExport.cs`

```csharp
public sealed class ManualBackupExport : IAsyncDisposable
{
    public string Volume { get; } public string SnapshotName { get; } public SnapshotId SnapshotId { get; }
    public CommitToken Boundary { get; } public string TargetName { get; } public Guid LineageId { get; }
    public IReadOnlyList<ObjectId> Closure { get; }                       // full closure of the pinned source snapshot
    public Task<DfsErrorCode> BeginAsync();                               // BackupBegin; repeatable after a destination restart
    public Task<IReadOnlySet<ObjectId>> RequestInventoryAsync();          // raw destination hint (false positives included)
    public Task<DfsErrorCode> SendManifestAsync(BackupManifestTamper tamper = BackupManifestTamper.None);
    public Task<DfsErrorCode> SendObjectAsync(ObjectId id, BackupObjectTamper tamper = BackupObjectTamper.None); // None only after durable+verified
    public Task<BackupPublishAttempt> PublishAsync();                     // destination verifies closure itself
    public ValueTask DisposeAsync();                                      // abandon if unpublished, release pin, resume exporter
}
public enum BackupObjectTamper { None, FlipPayloadByte, WrongObjectId, WrongEncoding, TruncatedPayload, WrongChecksum }
public enum BackupManifestTamper { None, DanglingObjectReference, AlteredExtentChecksum, FlipCiphertextByte, DroppedFileRecord }
public sealed record BackupPublishAttempt(DfsErrorCode Error, bool Published, IReadOnlyList<ObjectId> MissingObjects, DfsBackupCheckpoint? Checkpoint);
```

Steps send exactly the wire messages the automatic exporter sends (`BackupBegin`, `BackupInventory`,
`BackupPutManifest`, `BackupPutObject`, `BackupPublish`) in the order the test chooses; tampering is applied in transit
with valid framing so only content verification can detect it. `SendObjectAsync` with an id outside `Closure` throws
`ArgumentException`. A refused publish returns `Published = false`, a non-`None` error and the closure objects the
destination found missing or invalid. Used by BAK-006 (manifest first, §9.5), BAK-007 (hint-trusting exporter),
BAK-009 (duplicate deliveries), BAK-010 (object/manifest tampering).

## 2. Assumed semantics of existing members (please implement accordingly)

* `BackupInspector.GetPublishedPointerAsync(volume)` returns the **snapshot name** the durable published pointer
  designates (`null` before the first publication); `GetPublishedSnapshotsAsync(volume)` returns the names of all
  retained published snapshots.
* Fault sites on the backup node carry `Node` = backup name and `Volume` = source volume name:
  `BackupObjectReceived` once per received object (`FaultContext.Object` set) after receipt, before flush;
  `BackupBeforeClosureVerify` once per transfer; `BackupBeforePointer` after complete destination durability and
  before the pointer replacement; `BackupAfterPointer` right after the durable pointer replacement; `BackupAck` on the
  backup node after the pointer is durable and before the acknowledgement is sent. `SnapshotPin` is hit on the volume
  leader when a transfer pin is established (also at snapshot creation). `FaultCoverageRegistry.Hits` records every
  hit, including hits that did not trigger an arm. `FaultAction.CrashNode` on a backup site crashes the backup node.
* `DfsBackupStatus`: `ReceivedBytes` is cumulative and never decreases; `DurableBoundary` is the covered commit of the
  newest snapshot whose closure is durably verified on the destination (None before the first one);
  `PublishedBoundary`/`LatestPublished` reflect the latest acknowledged publication; `RecoveryPointAge` grows with time
  from the published boundary; `ReseedRequired`/`LineageId` reflect `AbandonTransferLineage` and the next lineage.
* `AwaitBackupAsync`/`AwaitBackupSnapshotAsync` report the destination's **current** retention (expiry/hold) of the
  returned snapshot; `IsExactSnapshot` is true only for `AwaitBackupSnapshotAsync` results. Tokens that are not
  comparable with the volume's tokens (other volume/epoch, `CommitToken.None`) are rejected with `ArgumentException`
  (or `DfsException` `InvalidArgument`/`EpochMismatch`), never compared.
* Backup-host clients (`TestCluster.ConnectBackupAsync`) authenticate against the backup node's own principals; source
  principals are unknown there. Every SDK mutation on a backup host throws `DfsReadOnlyException` (`ReadOnly`);
  configuration/admin paths may instead throw `UnauthorizedAccessException`. The export principal may connect as a
  client but cannot delete or shorten retention (`DfsRetentionProtectedException` or other
  `UnauthorizedAccessException`); the retention-admin principal can extend/hold retention via
  `DfsSnapshotClient.SetRetentionAsync` on the backup host (the tests never require it to shorten an expiry). All
  three backup principals (reader, export, retention admin) hold `Read` on the backup host (snapshot list/open/read).
  Deleting an unprotected snapshot with the retention-admin principal is not required to fail (BAK-015 skips it).
* Destination retention: a snapshot is prunable once it is outside `KeepLastSnapshots`, its retention expiry has
  passed, it is not held and no valid reader pin exists; pruning removes it as a unit. `ExpiresUtc = null` (the
  `BackupNodeOptions.DefaultRetention`) means "no time-based protection", so setting a finite expiry on such a
  snapshot is an extension.
* Source transfer pins and budgets: the exporter acquires the transfer pin before contacting the target (so a pin
  exists while the target is unreachable, visible as `PinInspection.Kind == "Transfer"`). Budgets are evaluated while
  the target is offline; `DfsBackupStatus.PinnedSourceBytes`/`MaxPinnedBytes` count source bytes retained only
  because of that target's transfer pins. Deleting a user snapshot that a pending transfer still needs keeps its data
  for the transfer (FSD §11.1). `ConfigureAsync` changes budgets without clearing `ReseedRequired`.
* `FaultAction.Custom` callbacks may change the network and may crash any node, including the node that hit the
  site (`TestNode`/`TestBackupNode.CrashAsync` must not wait on the crashed node's own loop). A node crashed while one
  of its operations is held by `FaultAction.Block` discards that operation; a later `FaultArm.Release()` is a no-op
  for it.
* A crash at `BackupBeforePointer` may restart with either the previous pointer or (roll-forward of the complete,
  durable staged snapshot) the new one; a crash at `BackupAfterPointer`/`BackupAck` must restart with the new one.
* `MessageHold` keeps capturing after `ReleaseAll()` until disposed and `Captured` is cumulative;
  `TestNetwork.ReplayAsync` re-delivers a captured request and returns the receiver's error code.
* `PinInspection.Kind` is `"Transfer"` for backup transfer pins. Setting `TestDisk.Faults.CapacityOverride = null`
  restores the disk's capacity after a `CapacityOverride` fault.
* `DfsAdminClient.RestoreFromBackupAsync` may reuse the name of a deleted volume; the restored authority has a new
  volume id and/or epoch whose tokens are not comparable with the old ones.
