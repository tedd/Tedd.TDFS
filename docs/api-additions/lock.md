# API additions — LOCK suite

> **Integration note (integrate/tests).** `RawProtocolClient.OpenHandleAsync` also serves PATH-014 (SDK/PATH request merged here) and gained `bool autoRenew = false`; with the default the renewal rule below is unchanged. See docs/API.md §I for the consolidated list.

Harness members added (as stubs throwing `NotImplementedException`) for the LOCK tests
(`tests/Tedd.Tdfs.Tests.Unit/Lock`, `tests/Tedd.Tdfs.Tests.Simulation/Lock`). No existing signature was changed.
Source: `tests/Tedd.Tdfs.Testing/Additions/lock/`.

## `TestCluster` (partial, `TestCluster.lock.cs`)

```csharp
public Task<RawProtocolClient> ConnectRawAsBackupAsync(int backupIndex, string nodeName, CancellationToken ct = default);
```

Connects a `RawProtocolClient` to live node `nodeName` presenting the node-level identity of backup node
`b{backupIndex}`: the handshake carries the backup's name, role = backup and its backup credentials exactly as the
backup's own connections do, so the live side sees `RpcPeerIdentity { IsBackup = true }`. The connection's network
identity is the backup's name (`"b0"`), so `TestNetwork` rules naming the backup apply. If the live node refuses the
backup-role handshake, the method still returns a client whose `IsConnected` is false and whose calls return the
refusal error code instead of throwing. No further `AuthenticateAsync` is needed.
Throws `ArgumentOutOfRangeException` for an unknown backup index and `ArgumentException` for an unknown node name.

Used by: LOCK-020 (`LOCK_020_BackupCannotAcquireLiveLocks`).

## `RawProtocolClient` (partial, `RawProtocolClient.lock.cs`)

```csharp
public Task<RawOpenHandleResult> OpenHandleAsync(string volume, string path, FileMode mode, FileAccess access, FileShare share);
public Task<DfsErrorCode> KeepAliveAsync(string volume, IReadOnlyList<HandleId> handles);
public Task<DfsErrorCode> CloseHandleAsync(string volume, HandleId handle);
```

* `OpenHandleAsync` — sends `OpenHandle` for `path` directly to the volume leader (following `NotLeader` hints),
  bypassing all SDK checks. Lazily opens one volume session (`OpenSession`) per volume under the client's
  authenticated identity and allocates a fresh operation id per call. `mode`/`access`/`share` are encoded verbatim —
  undefined values and bits such as `FileShare.Inheritable` are transmitted unchanged so server-side validation is
  observable; the path is sent as wire UTF-8, unnormalised. Never throws for server refusals: the reply's error code
  (or the failing handshake/session step's code) is returned in `RawOpenHandleResult.Code`; transport failures map
  to `DfsErrorCode.Unavailable`. A granted handle is a real lease of this raw session, renewed only by
  `KeepAliveAsync` and released by `CloseHandleAsync` or expiry.
* `KeepAliveAsync` — sends one `KeepAlive` for the raw client's session listing exactly `handles` (which may belong
  to other sessions). Returns `None` only when the server reports every listed handle renewed by this request;
  otherwise the first per-handle or request-level error code (`InvalidHandle`, `LeaseLost`, `AccessDenied`, the
  handshake refusal code, ...).
* `CloseHandleAsync` — sends `CloseHandle` for `handle` in the raw client's session; returns the reply's error code.

Used by: LOCK-005 (`LOCK_005_UnsupportedMultipleWriterShareRejected_ServerSide`), LOCK-007
(`LOCK_007_InheritableFlagRejected_ServerSide`), LOCK-020 (`LOCK_020_BackupCannotAcquireLiveLocks`).

## `RawOpenHandleResult` (new record, `RawOpenHandleResult.cs`)

```csharp
public sealed record RawOpenHandleResult(DfsErrorCode Code, HandleId? Handle, ulong WriterGeneration)
{
    public bool Granted { get; }   // Code == None && Handle is not null
}
```

`Handle` is set only when the server granted the handle; `WriterGeneration` is the generation bound to a granted
writable handle (0 for read-only grants and failures).

Used by: the same tests as `RawProtocolClient.OpenHandleAsync`.
