# API additions requested by the DSK and GC suites

Branch `tests/dsk-gc`. Every addition is a stub (`NotImplementedException`) in a separate partial-class file; no
existing signature was changed.

## Harness (`Tedd.Tdfs.Testing`)

### `TestNode.CompactStorageAsync()`

File: `tests/Tedd.Tdfs.Testing/Additions/dsk-gc/TestNode.dsk-gc.cs`

```csharp
public Task CompactStorageAsync();   // TestNode (partial)
```

Behaviour:

* Runs one complete object-store compaction pass on this node now, in addition to background maintenance, and
  completes when the pass has finished: new segments are durable and published, and old segments are deleted.
* The pass is posted to the node loop. It runs on the node's own scheduler, so it interleaves with garbage
  collection, repair and client traffic in the same way as a background pass, including in deterministic mode.
* Every compaction fault site (`FaultSites.CompactionCopy`, `CompactionPublish`, `CompactionDelete`) is reported to
  the node's fault injector, so faults armed with `TestFaults.Arm` fire inside the pass.
* If an armed fault crashes the node during the pass, the returned task faults (with any exception) instead of
  hanging.
* Compacting a store that has no dead records is a no-op that still completes.
* Throws `InvalidOperationException` unless the node is `Running`.

Needed by: `GC_014_CrashDuringCompactionAndGc`. Without it there is no deterministic way to make compaction overlap
collection. Background compaction depends on thresholds the API does not expose, so a fault armed at a compaction
site might never be reached.

## Test-local helpers (no harness changes)

These are implemented in the test projects and need no product or harness work:

* `tests/Tedd.Tdfs.Tests.Integration/Dsk/DskHelpers.cs`:
  * `InterposedMedium` / `InterposedFile` wrap an `IStorageMedium`. Every write and durability barrier (open-create,
    write, set-length, file flush, directory flush, rename, delete, create-directory) is logged as a crash
    boundary. The wrapper can lose power right before boundary *k*, inject an exception at any boundary, and
    emulate short writes, zero-progress writes and read errors on `FileSystemMedium`.
  * `DskMedium` is a per-case `SimulatedMedium` (power loss through `SimulatedMedium.Crash(CrashMode)`) or a fresh
    directory under `$TDFS_TEST_ROOT/dsk/...` (process-kill crashes).
  * `RecordingFaultInjector` is an `IFaultInjector` for `ObjectStore.OpenAsync` that records site hits and loses
    power at a chosen site occurrence.
  * `JournalModel`, `ObjectStoreModel` and `AtomicModel` (in the test class) are reference models of the legal
    post-recovery states.
  * `TestAesGcmCipher` is an `IRecordCipher` used for the encrypted-store variant.
  * `RunCrashMatrixAsync` is the crash-matrix driver. It does a dry run to count boundaries, then crashes at every
    boundary and after the last one, applies the crash mode, and verifies.
* `tests/Tedd.Tdfs.Tests.Simulation/Gc/GcHelpers.cs`: reachability, holder and reclaim assertions built on
  `ClusterInspector`.

## Members relied on that exist but are worth noting for implementers

* The `FaultContext` reported at `FaultSites.ReplicaDelete` must carry `Object`. GC-011 uses it to check that a
  draining node's old copy is deleted only after the replacements are verified.
* Raft-stage and apply-stage fault sites (`raft.*`, `apply.after`, `commit.receipt-validated`) must report
  `Operation` (for example `WriteAll` or `CommitFile`) and `Volume`. DSK-001 and GC-006 filter on these to crash
  the leader while it handles the test's own write.
* `MessageHold.Captured` must keep every frame captured during the hold's lifetime, including frames already
  released, so they can be replayed with `TestNetwork.ReplayAsync`. GC-007, GC-010 and GC-012 depend on this.
* Holding `MessageType.ReplicateObject` must block repair or re-replication copies (DSK-016, GC-011).
* `TestDisk.Faults` on `StorageKind.Disk` nodes must honour `ShortNextWrites`, `FailNextFlushes`, `FailNextReads` and
  `CapacityOverride` (resettable to `null` to "unfill" a disk). DSK-003, DSK-004, DSK-005, DSK-006, DSK-007 and
  DSK-017 use these switches.
