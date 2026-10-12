# Tedd.TDFS — working notes for agents

Distributed filesystem with a .NET client SDK. Requirements: `docs/spec/distributed-filesystem-fsd.md` (FSD) and
`docs/spec/distributed-filesystem-test-plan.md` (test plan). Design: `docs/ARCHITECTURE.md`. Exact signatures:
`docs/API.md` (binding — do not rename or remove listed members; adding members is fine).

## Build and test
- SDK pinned by `global.json` (10.0.401, net10.0). Solution: `Tedd.Tdfs.slnx`.
- Build: `dotnet build Tedd.Tdfs.slnx -v q` (or a single project with `--no-dependencies` when others are being edited concurrently).
- Tests use xunit.v3 on Microsoft.Testing.Platform. Run a project: `dotnet test --project tests/Tedd.Tdfs.Tests.Unit`
  (filter: `-- --filter-trait "TestId=OPEN-001"` or `--filter-class`/`--filter-method`).
- Test data root: env `TDFS_TEST_ROOT` (use `D:\Workspaces\AI\tdfs-tests`). Scratch files and temporary git worktrees
  go under `D:\Workspaces\AI\` — never in the repo, user profile, or %TEMP%.

## Non-negotiable code rules
- Server-side code (`Tedd.Tdfs.Server`, `.Consensus`, `.Storage`) runs on a single-threaded node loop: never
  `ConfigureAwait(false)`, `Task.Run`, raw threads/timers, `DateTime.UtcNow`, `Stopwatch`, `Guid.NewGuid()`,
  `Random.Shared`; use `NodeEnvironment` (TimeProvider, IRandomSource, IStorageMedium, transport, faults).
- Replicated state machines must be deterministic: no hash-order iteration affecting results; ordinal comparers.
- SDK (`Tedd.Tdfs.Client`) must be simulation-compatible: injected TimeProvider/random/transport, no `Task.Run`,
  `Task.Yield`, `SemaphoreSlim`, or `RunContinuationsAsynchronously` on the request path (ARCHITECTURE §9.6).
- Never weaken an FSD SHALL to make a test pass. Never fabricate success, zero-fill missing data, or report
  unknown outcomes as aborts.
- Match the surrounding code style (file-scoped namespaces, nullable enabled, XML docs on public members).

## Gotchas discovered so far
- Product projects use `InvariantGlobalization=true`, so `string.Normalize` is a no-op there: product code must use
  `Tedd.Tdfs.DfsUnicode.ToNfc/IsNfc` (pinned Unicode 16 tables). Test projects run with ICU
  (`InvariantGlobalization=false`), so tests may use `string.Normalize` and real cultures (e.g. tr-TR) as oracles.
- Harness classes (`TestCluster`, `TestNode`, `TestBackupNode`, `TestNetwork`, `TestFaults`, `ClusterInspector`,
  `TestDisk`, `RawProtocolClient`, `RaftSimulation`, `HistoryRecorder`) and server `NodeInspector`/`BackupInspector`
  are `partial` so additions can live in separate files.
- `.gitignore` has VS rules like `Backup*/` and `[Ll]og/`; `src/**` and `tests/**` are re-included, but check
  `git status` shows new folders before committing.
- Shared public types needed by both Client and Server (e.g. `DfsRetention`, `DfsConsistencyLabel`,
  `DfsBackupCheckpoint`) live in Core (namespace `Tedd.Tdfs`).
- `MessageCodec` registrations go in `src/Tedd.Tdfs.Core/Protocol/MessageCodec.Registrations.cs`; message records
  implement `IWireMessage<TSelf>`.
- Tedd.Quicly 0.2.2: an `MsQuicTransportListener` that owns its registration can deadlock in `Dispose` (it closes the
  registration before its listener handle). `QuiclyRpcTransport` therefore always passes its own `MsQuicRegistration`
  and closes it on a background thread. Use `QuiclyTransportFactory.CreateDevelopmentCertificate()`/`ComputeSpkiPin` for
  test certificates (non-exportable, non-persisted key: nothing is left in the user's key store).
- Tedd.Quicly 0.2.2: `QuiclyPeer.Close`/`QuiclyClient.Close` travel on Quicly's own control stream and end the session
  without waiting for messages already admitted on channel streams (up to `SendBudgetBytes` is silently lost). Wait until
  `GetChannelStatistics` shows `QueuedMessages + InFlightMessages == 0` first (MsQuic runs without send buffering, so a
  reliable send completes on the peer's ack); `QuiclyRpcTransport.CloseSession` does this for graceful closes.
- Transport threads (QUIC pump, inbound executors) must not run handler code: cancel handler tokens with
  `CancellationTokenSource.CancelAsync()`, since `Cancel()` runs `ct.Register` callbacks, and the handler code they resume,
  inline on the cancelling thread.
- Simulation (`Tedd.Tdfs.Testing.Simulation`): the runtime only inlines `ConfigureAwait(false)` continuations when the
  completing thread has **no custom `SynchronizationContext`**; completing a TCS (or cancelling a CTS) inside a node-loop
  or client-context work item queues SDK-style continuations to the thread pool, which `SimScheduler` reports as a
  violation. Complete such tasks via `SimScheduler.CompleteWithoutContext(...)` (the sim's own loops/network already do).
  Inside the sim never block on simulated work (`.Result`, `Wait()`, SDK sync wrappers): the single sim thread deadlocks.
- A `Task.Delay(..., ct)` completed by **cancelling its token** runs `ConfigureAwait(false)` continuations on the thread
  pool on .NET 10 even when the cancelling thread has no synchronization context (timer completion inlines fine). SDK code
  that must stay simulation-compatible should implement cancellable waits as a TCS completed by the injected
  `TimeProvider`'s timer plus `ct.UnsafeRegister(...)`, not as a cancellable `Task.Delay` awaited with
  `ConfigureAwait(false)` (the sim reports the escape as a HAR-002 violation; see `SimReviewTests`).
- `TimeProvider` subclasses: `System.X` resolves to the inherited `TimeProvider.System` property — add `using` directives
  instead of fully qualified `System.` names.
- Addendum A: ':' is no longer a name character — `DfsPath.Normalize` treats it as the stream separator in the final
  component (`/a/f:s`); split with `DfsStreamPath`. `DfsFileClient`, `DfsDirectoryClient` and `DfsAdminClient` are
  `partial` (Addendum members live in `*.Security.cs` / `*.Addendum.cs`). ACL decisions go only through
  `DfsAccessEvaluator`. `DfsFileSystemRights.ToString()` may print directory aliases (`CreateFiles` for `WriteData`);
  use `DfsAccessRule.FormatRights` in messages.
