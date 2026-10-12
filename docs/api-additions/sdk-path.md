# API additions requested by the SDK and PATH test suites

> **Integration note (integrate/tests).** `RawProtocolClient.OpenHandleAsync` was merged with the LOCK request: it is defined once in `Additions/lock/RawProtocolClient.lock.cs` and returns `RawOpenHandleResult(Code, HandleId? Handle, WriterGeneration)`; the session keep-alive renewal described below is requested with the new `autoRenew: true` parameter, which PATH-014 passes. See docs/API.md §I for the consolidated list.

Branch `tests/sdk-path`. All additions are harness members (`tests/Tedd.Tdfs.Testing`), added as stubs that throw
`NotImplementedException` in new partial-class files under `tests/Tedd.Tdfs.Testing/Additions/sdk-path/`. No existing
harness, SDK or server signature was changed.

## `TestNetwork.CountOpenConnections`

File: `tests/Tedd.Tdfs.Testing/Additions/sdk-path/TestNetwork.sdk-path.cs`

```csharp
public int CountOpenConnections(string name);
```

Behaviour: returns the number of transport connections that are currently open and have `name` ("n0", "b0",
"client:c0") as either endpoint. A connection counts from the moment the transport reports it established until either
side closes or disposes it (graceful close, `ResetConnections`, node crash/stop, client disposal). Connections made
unusable by a partition or black hole but not closed by either side still count (half-open). Connection attempts that
never completed do not count. Unknown names return 0. Must work for both `TransportKind.InMemory` and
`TransportKind.Quic` (the harness wraps every transport factory it hands to nodes and clients).

Needed by:
- SDK-005 `AllSeedsOffline` (a failed connect leaks no sockets),
- SDK-008 `FacadesAreThreadSafe` (pooled connections are reused, not opened per call),
- SDK-009 `AsyncMethodsAreActuallyAsync` (a cancelled connect leaves no connection),
- SDK-011 `ClientDisposalIsIdempotent` (resources released on disposal),
- SDK-006 `ClusterIdentityMismatch` (a rejected cluster leaves no connection),
- SDK-012 `DisposalDuringRequests` (nothing remains after shutdown).

## `TestCluster.ConnectAsync(TestClientOptions?, Func<DfsClientOptions, DfsClientOptions>, CancellationToken)`

File: `tests/Tedd.Tdfs.Testing/Additions/sdk-path/TestCluster.sdk-path.cs`

```csharp
public Task<DfsClient> ConnectAsync(TestClientOptions? options, Func<DfsClientOptions, DfsClientOptions> configure, CancellationToken ct = default);
```

Behaviour: identical to `ConnectAsync(TestClientOptions?, CancellationToken)` (network identity `client:{Name}`,
harness transport, randomness, client clock, fault injector and mutations), except that the public `DfsClientOptions`
the harness built (as `CreateClientOptions` returns them) are first passed through `configure`, so a test can set public
options that `TestClientOptions` does not expose (`ShutdownTimeout`, `MaxRetryAttempts`, `RetryBaseDelay`, ...). The
SDK validates the result exactly as on the public connect path; the harness must not override values the callback
changed.

Needed by: SDK-012 `DisposalDuringRequests` (the documented shutdown policy is tested against a known
`ShutdownTimeout`).

## `RawProtocolClient.OpenHandleAsync` and `RawProtocolClient.SendHandleOperationAsync`

File: `tests/Tedd.Tdfs.Testing/Additions/sdk-path/RawProtocolClient.sdk-path.cs`

```csharp
public Task<(DfsErrorCode Code, HandleId Handle)> OpenHandleAsync(string volume, string rawPath, FileMode mode, FileAccess access, FileShare share);
public Task<DfsErrorCode> SendHandleOperationAsync(string volume, DfsOperationKind kind, HandleId handle);
```

Behaviour:
- `OpenHandleAsync` opens a file handle in `volume` through the raw connection's own authenticated session in that
  volume (path sent verbatim, like `SendPathOperationAsync`) and returns the server's error code (`None` on success)
  and the server-issued handle id exactly as it appears on the wire (`default` on failure). The handle is kept alive by
  the raw client's session keep-alive until closed or the connection is disposed.
- `SendHandleOperationAsync` sends one handle-addressed request for `handle` to the leader of `volume`, bypassing all
  SDK checks, and returns the reply's error code. Kinds: `Read` (ReadRange offset 0 length 1), `KeepAlive` (renew only
  that handle), `CloseHandle`, `CommitFile` (an empty commit carrying the handle's writer generation as last reported
  by the server, or 0). The handle number is sent verbatim, never translated through the raw client's own handle
  table. Other kinds throw `ArgumentOutOfRangeException`.
- One raw client may hold handles in several volumes at once (one session per volume, same principal and connection
  identity): both methods route to the named volume's current leader regardless of the node the raw client was
  connected to (following `NotLeader` hints like the SDK).

Needed by: PATH-014 `TokenIdentityCannotCrossVolumes` (a handle issued by one volume must be rejected by another
volume, e.g. `InvalidHandle`, even when the same raw client holds a handle with the same number in that volume).

## Test-side helpers (not harness additions)

- `tests/Tedd.Tdfs.Tests.Integration/Sdk/SdkHelpers.cs`: `SingleThreadedSynchronizationContext` (one pumping thread,
  counts posts; SDK-010).
- `tests/Tedd.Tdfs.Tests.Unit/Sdk/SdkHelpers.cs`: `RecordingDiagnostics` (`IDfsDiagnostics` sink; SDK-016) and
  request-counting helpers over `TestNetwork.Log`.
- `tests/Tedd.Tdfs.Tests.Unit/Path/PathHelpers.cs`: the "rejected locally, no request sent, namespace unchanged" sweep
  over every path-taking SDK entry point, and exact-byte-length path/component builders.

## Notes for the implementation

- PATH tests use the namespaces `Tedd.Tdfs.Tests.Unit.Paths` / `Tedd.Tdfs.Tests.Simulation.Paths` (folder `Path/` and
  class `Path<Level>Tests` as in API.md §G). This is a deliberate deviation from the §G namespace
  `Tedd.Tdfs.Tests.<Level>.Path`: a namespace named `Path` directly under `Tedd.Tdfs.Tests.<Level>` would shadow
  `System.IO.Path` for every other suite in the same project (an unqualified `Path.Combine` there would bind to the
  namespace and stop compiling after merge).
- PATH-014 (operation ids, transaction ids and handle ids) and FSD §13 require that ids issued by one volume can never
  be accepted or resolved by another volume, even when per-volume counters produce equal numbers: session/handle ids
  must be volume-bound (for example allocated from a volume-derived space) or carried with their volume on the wire.
