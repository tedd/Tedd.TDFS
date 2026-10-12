# API additions for the HAR suite (HAR-001..HAR-018)

All additions are new files under `tests/Tedd.Tdfs.Testing/Additions/har/`. No existing signature was changed.
Members marked *stub* throw `NotImplementedException` (via `Stub.NotYet`) until the harness implements them; plain data
types (records, enum, exception classes) are complete.

## 1. Uncontrolled dependencies (HAR-002) — `UncontrolledDependency.har.cs`, `TestCluster.har.cs`

```csharp
namespace Tedd.Tdfs.Testing;
public enum UncontrolledDependencyKind { ForeignThreadPost, WallClock, Random, Timer, ThreadPool, Thread, Socket, Disk, ContextEscape }
public sealed record UncontrolledDependency(UncontrolledDependencyKind Kind, string Component, string Detail);
public sealed class UncontrolledDependencyException : Exception
{
    public UncontrolledDependencyException(IReadOnlyList<UncontrolledDependency> dependencies, Exception? inner = null);
    public IReadOnlyList<UncontrolledDependency> Dependencies { get; }
}
public sealed partial class TestCluster
{
    public IReadOnlyList<UncontrolledDependency> UncontrolledDependencies { get; }   // stub
}
```

Required behaviour:
* In `ClusterMode.Deterministic`, any work posted to a simulation `SynchronizationContext` (node loops and the
  scenario/client context) from a thread other than the simulation scheduler thread (thread-pool continuations of
  `Task.Run`/`ThreadPool`, OS timers incl. `Task.Delay` without a `TimeProvider`, socket/file I/O completions) is recorded
  as `UncontrolledDependency(ForeignThreadPost, component, detail)`: `Component` = target context ("n0", ..., or
  "scenario"), `Detail` = posting thread + stack trace. The posted work is still executed (no deadlock).
* A cluster start that reaches stability through the harness's own facilities records nothing; using `cluster.Time`,
  `cluster.DelayAsync`, `Task.Delay(…, cluster.Time)` and `cluster.Time.CreateTimer` records nothing.
* **Behaviour of the existing `ClusterTest.RunAsync` (both overloads):** when the list is non-empty at the end of the run
  it throws `UncontrolledDependencyException` with every recorded dependency (inner exception = the scenario's own
  failure, if any), even when the scenario succeeded. In `Realtime` mode the list is always empty.

Used by: `HAR_002_NondeterminismIsIntercepted` (5 escapes), `HAR_002_NondeterminismIsIntercepted_ControlledFacilitiesAreNotReported`.
`UncontrolledDependencyKind` is also used by the suite's static IL audit (`Har/HarHelpers.cs`, test-local, no stub).

## 2. Environment-assumption violations (HAR-018) — `AssumptionViolation.har.cs`, `TestCluster.har.cs`, `TestDisk.har.cs`

```csharp
public sealed record AssumptionViolation(string Assumption, string Node, string Detail)
{
    public const string DurableFlush = "storage.flush-durability";
}
public sealed class AssumptionViolatedException : Exception
{
    public AssumptionViolatedException(IReadOnlyList<AssumptionViolation> violations, Exception? inner = null);
    public IReadOnlyList<AssumptionViolation> Violations { get; }
}
public sealed partial class TestCluster
{
    public IReadOnlyList<AssumptionViolation> AssumptionViolations { get; }   // stub
}
public sealed partial class TestDisk
{
    public bool IsDishonestFlush { get; }                                    // stub
    public void SetDishonestFlush(bool dishonest);                           // stub
}
```

Required behaviour:
* `SetDishonestFlush(true)`: every file flush and directory flush on that disk reports success immediately but leaves
  the writes/namespace changes volatile, so a later crash (any `CrashMode`, `TestNode.CrashAsync`, `FaultAction.CrashNode`,
  disk replacement) loses them as if never flushed. Turning it off does not retroactively persist anything. Works for
  simulated and wrapped real disks, node running or not.
* When a crash discards bytes or namespace changes whose flush reported success, the harness records one
  `AssumptionViolation(DurableFlush, node, detail)` per node and crash in `TestCluster.AssumptionViolations`
  (visible immediately after the crash call returns).
* **Behaviour of the existing `ClusterTest.RunAsync` (both overloads):** when `AssumptionViolations` is non-empty at the
  end of the run it throws `AssumptionViolatedException` (inner = scenario failure, if any) — the run is labelled
  out-of-model, never a pass and never a protocol failure. This takes precedence over the end-of-run harness invariant
  checks (split brain, replica convergence, history checks): their failures are also wrapped as the inner exception,
  because evidence gathered under a violated assumption cannot refute the protocol either.

Used by: `HAR_018_AssumptionViolationIsLabelled` (None / One / All dishonest devices).

## 3. Trace minimization (HAR-014) — `TraceMinimizer.har.cs`

```csharp
public sealed record TraceMinimizationResult<TStep>(IReadOnlyList<TStep> Original, IReadOnlyList<TStep> Minimal, string Invariant,
    int Runs, bool IsOneMinimal, bool BudgetExhausted);
public static class TraceMinimizer
{
    public static Task<TraceMinimizationResult<TStep>> MinimizeAsync<TStep>(IReadOnlyList<TStep> failingTrace,
        Func<IReadOnlyList<TStep>, Task<string?>> run, int maxRuns, CancellationToken ct = default);   // stub
}
```

Required behaviour (ddmin): run the original once (a passing original → `ArgumentException`); accept only
order-preserving subsequences whose run returns the **same** invariant id (ordinal) — passing, differently failing or
throwing candidates are rejected; complements first, then subsets, refining granularity; deterministic candidate order;
no candidate executed twice; verify 1-minimality by running every single-step removal of the result; stop at `maxRuns`
executions (counting the initial one) returning the smallest failing trace so far with `BudgetExhausted = true`,
`IsOneMinimal = false`. `Runs` = number of delegate executions.

Used by: `HAR_014_TraceMinimizationPreservesFailure_Minimizer` (pure), `HAR_014_TraceMinimizationPreservesFailure`
(deterministic cluster schedule with the `DisableOperationDedup` negative control).

## 4. Behaviour the HAR tests rely on in existing members (no signature change)

* `TestNode.RestartAsync()` completes once the node process has recovered from its durable image and is running, even
  when the node is network-isolated (`TestNetwork.Isolate`) at that moment (HAR-004 reboots a node in isolation so repair
  cannot mask the rebooted image).
* `FaultContext` produced at `object.*` sites carries `Object`, `Node` and `Volume`; client sites (`client.*`) carry the
  user path in `Path`; request sites (`rpc.request.accepted`, `commit.receipt-validated`, `apply.after`,
  `rpc.reply.send`) carry `Volume` and the user operation kind (`DfsOperationKind.WriteAll` for a whole-file write);
  node-local metadata sites (`staging.registered`, `raft.*`) carry the hitting `Node` and, for volume groups, `Volume`.
* `TestCluster.Trace` contains node lifecycle events that name the node and the action ("pause"/"crash", any case).
* `FaultAction.PauseNode()` puts the hitting node into `TestNodeState.Paused` (same `Host` instance) until
  `FaultArm.Release()`; `FaultAction.CrashNode()` puts it into `TestNodeState.Crashed` (`Host == null`).
* `TestNode.Clock` instances are distinct per node, distinct from `TestCluster.Time`, and (in deterministic mode) advance
  at the same rate as `TestCluster.Time` unless frozen/re-rated.
* `FaultArm.WaitTriggeredAsync` on a never-reached arm throws (`TimeoutException` or an xunit assertion exception);
  `TestFaults.AssertReached` throws an xunit assertion exception (`XunitException`).
* Concurrently running clusters get distinct, non-nested `DataRoot` directories and distinct cluster/node identities;
  in deterministic mode `ClusterId`, node ids and every replayed observable derive from the seed.
* `TestNetwork.BlackHole(from, to)` and `TestNetwork.Cut(from, to)` affect only the `from -> to` direction;
  `TestNetwork.Restore(from, to)` removes either kind of rule on that direction (HAR-006).
* `FaultCoverageRegistry.Hits` lists every hit in the order the arms observed them (deterministic mode), so the
  `Occurrence`-th hit matching an arm's `FaultMatch` is the one its action ran on (HAR-013).
* `FaultCoverageRegistry.CreateReport(campaign, generated, applicable, excluded)` counts as `Reached` only arms of this
  run whose trigger window was actually hit (`FaultArm.Triggered`), capped at `applicable`; an armed-but-never-triggered
  arm, or a site hit outside every arm's occurrence window, is not reached, so `IsComplete` is false (HAR-013).
* `MessageHold` holds every matching transmission (including SDK retries with the same operation id) until
  `ReleaseAll`; two holds with disjoint filters are independent, so releasing one delivers only its messages (HAR-007).
* `TestDisk.Faults.CapacityOverride` on a simulated disk caps the medium's capacity (ENOSPC beyond it); HAR-015 sets it
  to `UsedBytes + TestClusterOptions.ReservedBytes`, which must leave metadata progress (journal, checkpoints) possible
  while object-store admission rejects new payloads (ARCHITECTURE §8).
