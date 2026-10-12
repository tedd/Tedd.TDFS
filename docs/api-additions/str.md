# API additions requested by the STR suite

Suite: STR (STR-001..STR-028). All additions are harness-only stubs (`Tedd.Tdfs.Testing`) that throw
`NotImplementedException` until implemented; no existing signature was changed.

## `TestFaults.LimitReadResultBytes` (+ `ReadResultLimitRule`)

File: `tests/Tedd.Tdfs.Testing/Additions/str/TestFaults.str.cs`, `tests/Tedd.Tdfs.Testing/Additions/str/ReadResultLimitRule.cs`

```csharp
// TestFaults (partial)
public ReadResultLimitRule LimitReadResultBytes(int maxBytesPerResult, string? node = null, string? volume = null);

public sealed class ReadResultLimitRule
{
    public int MaxBytesPerResult { get; }
    public string? Node { get; }
    public string? Volume { get; }
    public int Applied { get; }      // read results actually shortened so far
    public bool IsActive { get; }
    public void Cancel();            // idempotent
}
```

**Behaviour.** While the rule is active, every read result that returns file/object payload to a client — a volume
leader's `ReadRange` result carrying inline bytes and a storage node's `GetObject` range result — carries at most
`maxBytesPerResult` payload bytes, even when more were requested. The shortened payload is a correct prefix of the
requested range of the selected revision/object, its integrity information covers exactly the bytes returned (a
verifiable short result, not corruption), and the reply does not claim end-of-file/end-of-object for the shortened range.
The SDK must treat it as an ordinary short backend result (FSD §6.2): return a short count to the caller or issue further
requests, but never pad, zero-fill, report EOF, fault the stream or fail the call because of it. `node`/`volume` restrict
the rule (null = all). It applies to requests arriving after the call, from any client, until `Cancel()` or cluster
disposal, and never affects node-to-node replication, repair or backup traffic. `Applied` counts only replies that were
actually shortened. `maxBytesPerResult < 1` throws `ArgumentOutOfRangeException`.

**Why.** The test plan requires *forcing* verified short reads from the backend (STR-004 "Return verified short reads
from the backend", STR-006 "Force multiple short reads"). No existing harness member can shape reply payloads
(`TestNetwork` rules only deliver/drop/delay/duplicate/hold whole messages, fault actions cannot change a reply).

**Used by.** `StrUnitTests.STR_004_ShortReadIsValid`, `StrUnitTests.STR_006_ReadExactlyHandlesShortReads`,
`StrUnitTests.STR_006_ReadExactlyHandlesShortReads_UnavailabilityIsNotPrematureEof`.

## Test-side assumptions (no new members)

These are not API additions but conventions the STR rows rely on; listed so implementers can check them:

- Client-side fault sites carry the logical operation in `FaultContext.Operation` (`Read` for `ReadRange`, `ReadObject`
  for `GetObject`, `CommitFile` for publication), both at `client.request.send` (F03) and `client.reply.received` (F14),
  and `FaultAction.Block()`/`Throw(...)` armed there apply to the SDK of clients created by `TestCluster.ConnectAsync`.
- `MessageFilter.From` for SDK traffic is the client's network name `client:{TestClientOptions.Name}`.
- `TestDisk.CorruptObjectAsync(id, offset)` takes `offset` relative to the start of the object's plaintext payload
  (tests pass `ExtentInspection.ObjectOffset + (fileOffset - ExtentInspection.Offset)` so the corruption lies inside the
  range being read).
- `ClusterInspector.GetAllStoredObjectsAsync` covers the stores of all running nodes (STR-016/STR-017 use it to check
  that pinned objects are retained and expired-pin objects are reclaimed).
- `SeedOneObjectPerChunkAsync` (STR-014, STR-026) writes a file through one stream and flushes after every 64 KiB chunk;
  it relies on each such flush publishing the newly written chunk as its own object(s) while untouched extents keep
  their objects (AT-07), so the file is multi-object regardless of how `WriteAll*` would pack objects (up to 1 MiB).
- `FaultAction.Custom` armed at `client.reply.received` runs on the client's request path before the reply is used and
  may call harness network controls (STR-006 isolates the client after the first object reply to fail mid-assembly).
- STR-019 holds a read either at its live `ReadRange` request (`Operation = Read`) or, when a writable handle serves its
  exclusively held base revision without a revision round trip, at its first `GetObject` (`Operation = ReadObject`); it
  uses a fresh client per iteration so no object data can already be cached.
