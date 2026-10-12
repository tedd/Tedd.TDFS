# API additions — LAY / REP test suites

Harness additions made while writing the LAY (LAY-001..LAY-020) and REP (REP-001..REP-020) suites. All members are
stubs (`Stub.NotYet`) until the harness phase implements them; no existing harness/SDK signature was changed.
Files live in `tests/Tedd.Tdfs.Testing/Additions/lay-rep/`.

## `RawProtocolClient` (partial) — `RawProtocolClient.lay-rep.cs`

```csharp
public Task<DfsErrorCode> SendReadRangeAsync(string volume, string path, long offset, long length);
```
Opens a read handle (`FileShare.ReadWrite`) on `path` as the authenticated principal, sends one `ReadRange` whose
`offset`/`length` are encoded verbatim (negative and overflowing values included, no SDK validation), closes the handle
and returns the server's error code (`None` when a range result came back). Never allocates by `length`; reply data is
discarded. **Used by:** LAY-018 (`LAY_018_UntrustedEncodingLengthsRejected_ReadRange`).

```csharp
public Task<RawStagedWriter> OpenStagedWriterAsync(string volume, string path, FileMode mode = FileMode.OpenOrCreate);
```
Opens a writable handle (`FileAccess.Write`, `FileShare.Read`) through the volume leader, creates a staging session bound
to it and returns a `RawStagedWriter`. Open/staging failures throw the mapped SDK exception (`DfsErrors.ToException`).
**Used by:** LAY-018, LAY-019, REP-012, REP-013.

## `RawStagedWriter` (new) — `RawStagedWriter.cs`

```csharp
public sealed class RawStagedWriter : IAsyncDisposable
{
    public HandleId Handle { get; }
    public InodeId Inode { get; }
    public ulong WriterGeneration { get; }
    public ulong BaseRevision { get; }          // advances after a successful CommitExtentsAsync
    public StagingId Staging { get; }
    public Task<RawPutResult> PutObjectAsync(string nodeName, ObjectId id, ReadOnlyMemory<byte> data);
    public Task<DfsErrorCode> CommitExtentsAsync(long newLength, IReadOnlyList<RawExtentSpec> extents, IReadOnlyList<RawReceipt> receipts);
    public ValueTask DisposeAsync();            // closes the handle (and so the staging session); idempotent
}
```
Behaviour:
* Keeps its session and handle lease alive (KeepAlive every LeaseDuration/3, following leader changes) until disposed, so
  tests can crash/restart nodes or relabel topology between uploads and the commit.
* `PutObjectAsync` sends `PutObject` for the given id/payload (plaintext, default encoding) to the named storage node with
  this session's staging capability and returns the error code plus the decoded receipt (if issued). Payload sent
  verbatim, so re-sending different bytes under an existing id exercises object immutability.
* `CommitExtentsAsync` sends `CommitFile` for this handle (handle id, writer generation, base revision, staging id) with
  `newLength`, the extent refs exactly as given (no helper-side validation) and each receipt's `Wire` bytes verbatim;
  returns the server's error code.

**Used by:** LAY-018 (`_CommitExtents`), LAY-019 (`_ConflictingReupload`), REP-012, REP-013.

## `RawReceipt`, `RawPutResult`, `RawExtentSpec` (new records) — `RawReceipt.cs`

```csharp
public sealed record RawReceipt(ObjectId Object, UInt128 Checksum, long Length, byte Encoding, NodeId Node, string NodeName,
    ulong Incarnation, ulong PlacementEpoch, ReadOnlyMemory<byte> Wire)
{
    public RawReceipt WithTamperedMac();   // copy with the last wire byte (inside the MAC) XOR 0xFF — implemented (pure)
}
public sealed record RawPutResult(DfsErrorCode Error, RawReceipt? Receipt);
public sealed record RawExtentSpec(long Offset, long Length, ObjectId Object, long ObjectOffset, long ObjectLength, UInt128 Checksum, byte Encoding)
{
    public static RawExtentSpec FromReceipt(RawReceipt receipt, long offset, long length, long objectOffset = 0); // implemented (pure)
}
```
`RawReceipt` carries the decoded receipt fields (ARCHITECTURE §6: object, checksum, length, encoding, node id,
incarnation, placement epoch) for assertions and the exact encoded receipt (`Wire`, fields + MAC) for replay/tampering.
`RawExtentSpec` mirrors the `ExtentRef` of a `CommitFile` change; every field is sent verbatim.
**Used by:** LAY-018, LAY-019, REP-012, REP-013.

## Conventions the tests rely on (no new members)

* `FaultContext.Object` is populated at `FaultSites.ReplicaDelete` (REP-016) and `FaultContext.Node` names the node
  deleting its replica.
* `NodeCounters.ObjectBytesRead` counts bytes read/decoded from local object stores (LAY-013);
  `NodeCounters.ObjectBytesReceived` counts object payload bytes stored by a node from uploads and repair (LAY-010,
  LAY-011, LAY-020).
* `DfsPlacementUnavailableException.RequiredCopies/RequiredZones` report the acknowledgement requirement
  (`DurableCopiesBeforeAck`/`DurableZonesBeforeAck`) and `AchievedCopies/AchievedZones` the distinct hosts/zones of valid
  durable receipts.
* A node whose `NodeOptions.CapacityBytes` equals its `ReservedBytes`, or whose disk was filled below the reserve
  (`TestDisk.Fill(leaveFreeBytes < ReservedBytes)`), accepts no new objects but keeps its journals working (REP-005,
  REP-006, REP-009, REP-020, LAY-002).
