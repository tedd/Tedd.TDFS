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
