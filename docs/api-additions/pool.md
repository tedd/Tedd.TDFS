# API additions — POOL test suite

Harness additions made while writing the POOL suite (POOL-001..POOL-018, FSD Addendum A §A1–A2, catalog §A5.2). All
members are stubs (`Stub.NotYet`) until the harness phase implements them; no existing harness, SDK or server signature
was changed. Files live in `tests/Tedd.Tdfs.Testing/Additions/pool/`.

## `RawProtocolClient` (partial) — `RawProtocolClient.pool.cs`

```csharp
public Task<(DfsErrorCode Code, HandleId Handle)> OpenHandleInVolumeAsync(string volume, string rawPath, FileAccess access, FileShare share);
public Task<(DfsErrorCode Code, byte[] Data)> ReadThroughHandleAsync(string volume, HandleId handle, long offset, int length);
public Task<DfsErrorCode> CloseHandleInVolumeAsync(string volume, HandleId handle);
```

Behaviour:
- `OpenHandleInVolumeAsync` opens a file handle (`FileMode.Open`) in `volume` through the raw connection's own
  authenticated session in that volume, sending `rawPath` verbatim, and returns the server's error code (`None` on
  success) plus the server-issued handle id exactly as it appeared on the wire (`default` on failure). The handle is kept
  alive by the raw client's session keep-alive until closed or the connection is disposed.
- `ReadThroughHandleAsync` sends one `ReadRange` for `handle` (number sent verbatim, never translated through the raw
  client's own handle table) and returns the reply's error code and, only when it is `None`, the bytes read (inline data
  directly; extent data fetched with the read capability the reply carries, verified); otherwise an empty array.
- `CloseHandleInVolumeAsync` sends one `CloseHandle` for `handle` (verbatim) and returns the reply's error code.
- One raw client may hold handles in several volumes at once (one session per volume, same principal and connection
  identity); all three methods route to the named volume's current leader regardless of the node the raw client
  connected to, following `NotLeader` hints like the SDK.

Needed by: POOL-002 `HandlesAndTokensDoNotCrossVolumes` (`Handle` case): handle numbers issued by one volume and presented
to another must never read the other volume's bytes. Handle ids are allocated per volume (ARCHITECTURE §2.2), so a
foreign number may coincide with a handle the target volume issued itself; the test accepts that such a number addresses
only the target volume's own handle, and requires every foreign number the target volume never issued (read through
`VolumeStateInspection.Handles`) to be rejected for reads and closes. The test drives a numeric collision when per-volume
numbering allows one and then lets `vb` run ahead, so rejection of never-issued numbers is always exercised.

Overlap note: branch `tests/sdk-path` adds `RawProtocolClient.OpenHandleAsync` / `SendHandleOperationAsync` for
PATH-014 with similar routing. The POOL members use distinct names so both branches merge without conflicts; an
implementation may share one code path (the POOL read additionally returns the bytes so a test can prove that no foreign
data was served).

## `TestCluster.RestoreSnapshotToPoolAsync` (partial) — `TestCluster.pool.cs`

```csharp
public Task<DfsVolumeInfo> RestoreSnapshotToPoolAsync(string volume, string snapshotName, string newVolumeName, string storagePool, CancellationToken ct = default);
```

Behaviour: restores snapshot `snapshotName` of `volume` into a new volume `newVolumeName` bound to `storagePool`, as the
admin principal, and completes at the same point as `DfsSnapshotClient.RestoreAsync` (new group has a leader, restore
complete). The new volume keeps the snapshot's default and named policies; its data objects are copied only onto the
pool's nodes and its metadata voters are chosen from them (POOL-02). Errors are the SDK's typed exceptions:
`PoolNotFound`, `InvalidPolicy` (the pool cannot satisfy the restored policies, POOL-03), `SnapshotNotFound`,
`DfsAlreadyExistsException`, `DfsPlacementUnavailableException`. A failed restore leaves no volume behind and copies
nothing.

Why a harness member: POOL-016 requires "restoring a snapshot into a new volume binds it to a chosen pool", but the public
SDK (`DfsSnapshotClient.RestoreAsync(name, newVolumeName)`, API.md §B.7) has no pool parameter. Suggested SDK follow-up
(not added here, API.md is binding): an overload `RestoreAsync(string name, string newVolumeName, string storagePool, ct)`
(or a restore-options record with `StoragePool`); the harness member can then delegate to it.

Needed by: POOL-016 `SnapshotRestoreKeepsPool` (all five cases).

## Conventions the tests rely on (no new members)

- Operation-id sessions are issued per volume (ARCHITECTURE §2.2), so `DfsOperationId.Session` values of two volumes
  usually coincide. POOL-002 (`OperationId`, `TransactionId`) therefore takes the foreign id from a fresh client whose
  session number the target volume never issued (checked through `VolumeStateInspection.Sessions`), and requires such an
  id to be rejected (`ArgumentException`, `OperationIdMismatch`, `CrossVolume`, `OperationExpired`) or, for queries, to
  resolve to anything but the other volume's committed outcome.
- `DfsPoolInfo.Nodes`, `DfsPoolInfo.Volumes`, `ListPoolsAsync` and `ClusterInspector.GetPoolNodesAsync` have no specified
  order; the tests compare them as sets. `DfsPoolInfo.Volumes` lists the volumes bound to the pool and MAY also list
  volumes that reference the pool only through a named policy (POOL-013 accepts both).
- `ClusterInspector.GetStoredObjectsOnNodeAsync(node, volume)` reports staged as well as committed records (it reads the
  node's object store), so POOL-005/006/012 can assert that not even staged uploads leave a pool. With `volume == null`
  it reports every record id on the node (POOL-016 compares whole-node inventories because a restore may or may not
  reuse object ids).
- `ClusterInspector.GetObjectHoldersAsync` only reports running nodes holding a verified copy (a crashed node is not a
  holder); POOL-013 computes expected per-pool deficits from it.
- `FaultContext.Object` is populated at `FaultSites.ReplicaDelete` and `FaultContext.Node` names the node dropping its
  replica (POOL-010's copy-before-delete check holds the first deletion on the node leaving the pool).
- `FaultContext.Volume` and `FaultContext.Operation` are populated at `FaultSites.RpcRequestAccepted` (POOL-015 holds the
  `CommitFile` request of one volume at the leader while the pool membership changes).
- `FaultAction.Block()` suspends only the hitting operation; the node keeps serving other requests and inspection while
  it is held (POOL-010, POOL-015).
- `TestDisk.Fill(leaveFreeBytes)` with `leaveFreeBytes < ReservedBytes` makes the node refuse new objects with
  `CapacityExhausted` while its journals keep working (POOL-012).
- `DfsPoolInfo.UsedBytes` counts stored bytes of objects placed through that pool only (a node in several pools, or in a
  named pool and `default`, attributes each object to the pool it was placed through); POOL-013 bounds it by the member
  inventories' record lengths plus encoding overhead and checks other pools are unaffected.
- `DfsPoolStatus.Replicas` counts the objects placed through the pool (`Objects`) and their deficits against the target
  policy, counting only verified copies on the pool's members (POOL-013).
- Placement is stable for pools whose membership did not change: a membership change of one pool never migrates another
  pool's objects (POOL-014), and converged holders equal `Placement.SelectTargets(object, epoch, policy, poolMembers)`
  where `epoch` is the placement epoch created by the pool's last membership change (POOL-010, POOL-011). POOL-011 also
  accepts the cluster's current placement epoch, which is the same value unless something else advanced it.
- Pool and group administration is audited like other security-relevant actions (SEC suite vocabulary): a denial entry
  names the principal, the object kind (`pool`/`group`), the target name when there is one, and a denial outcome
  (`denied`, `unauthorized`, `refused`, `rejected`, `forbidden` or `fail…`); successful changes name the acting principal,
  the kind and the target. Successful reads (`ListPools`, `GetPoolStatus`, `ListGroups`) need not be audited (POOL-017).
