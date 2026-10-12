# API additions requested by the CHA and MUT suites

> **Integration note (integrate/tests).** `TestCluster.StartRecoveryClusterAsync` is now defined once in `Additions/rst-sec/TestCluster.rst-sec.cs` (docs merged). `TestNode.CompactObjectStoreAsync` was merged into `TestNode.CompactStorageAsync(CancellationToken ct = default)` (`Additions/dsk-gc`; an interrupted pass **faults**, so CHA-009 tolerates the exception only for the node its armed crash took down). `TestSoftwareVersion`, `TestNode.SoftwareVersion` and `TestNode.RestartWithSoftwareAsync` were replaced by `SoftwareVersionProfile` (which gained `Next` and `NextFeature`), `TestNode.SoftwareVersion` and `TestNode.RestartAsSoftwareVersionAsync` (`Additions/fmt-ops-perf`). The original requests are kept below for reference. See docs/API.md §I for the consolidated list.

Harness-only additions (stubs throwing `NotImplementedException` until implemented). No existing signature was changed.
Files: `tests/Tedd.Tdfs.Testing/Additions/cha-mut/`.

## `TestCluster.StartRecoveryClusterAsync`

```csharp
public Task<TestCluster> StartRecoveryClusterAsync(TestClusterOptions options, CancellationToken ct = default);
```

Starts an independent "recovery" cluster for disaster-recovery scenarios that restore from this cluster's backup nodes
onto clean hosts.

- Bootstrapped from scratch: new `ClusterId`, new cluster secret/key hierarchy, fresh empty disks, its own control group,
  its own `admin` principal, and the volumes in `options.Volumes` (may be empty). Shares no Raft group, key, session,
  cache or storage with the parent.
- Same execution context: in `ClusterMode.Deterministic` the same simulation scheduler and virtual clock; in realtime
  the same process. `options.Mode`, `Transport` and `Storage` must equal the parent's (`ArgumentException` otherwise).
- Same network fabric: its nodes/clients can reach the parent's backup nodes (and, subject to network rules, its live
  nodes). `Network` of either cluster controls the shared fabric; partitions/cuts/holds may name endpoints of both.
- Node names from `options.Nodes`, default `r0..r{N-1}` (backup nodes `rb0..`); collisions with parent endpoints →
  `ArgumentException`.
- Returns once stable. Own `Faults`, `Inspect`, `History`. Disposed with the parent unless disposed earlier; disposing it
  never affects the parent.

Needed by: CHA-006 (restore the last complete snapshot after total source loss), CHA-010 (restore every completed
snapshot with all source nodes unreachable after key rotation), CHA-016 (promotion onto a new authority while the old
source is isolated, then reconnect it).

## `TestNode.CompactObjectStoreAsync`

```csharp
public Task CompactObjectStoreAsync(CancellationToken ct = default);
```

Forces one object-store compaction pass on the node (`ObjectStore.CompactAsync` for every store, on the node loop):
every segment containing at least one dead record — including the active segment, which is sealed first — has its live
records copied to a new segment, flushed, published, and the old segment deleted, passing `FaultSites.CompactionCopy`,
`CompactionPublish` and `CompactionDelete` for each compacted segment. Completes when the pass finishes; if the node
crashes/stops during the pass the task completes without throwing (the node's `State` reports the crash). A pass with
nothing to compact completes immediately without hitting compaction fault points. `InvalidOperationException` when the
node is not `Running` at the call.

Needed by: CHA-009 (deterministically interleave compaction with snapshot deletion, GC and node crashes).

## `TestSoftwareVersion` (new type) and `TestNode.SoftwareVersion` / `TestNode.RestartWithSoftwareAsync`

```csharp
public sealed record TestSoftwareVersion(string Name, int MinProtocolVersion, int MaxProtocolVersion, IReadOnlySet<string> Features)
{
    public const string NextFeature = "test.next-format";
    public static TestSoftwareVersion Current { get; }   // the build under test; every node starts as this version
    public static TestSoftwareVersion Next { get; }      // Min = Current.Min, Max = Current.Max + 1, Features = Current ∪ {NextFeature}
}

// TestNode
public TestSoftwareVersion SoftwareVersion { get; }
public Task RestartWithSoftwareAsync(TestSoftwareVersion version, Func<NodeOptions, NodeOptions>? reconfigure = null);
```

- `Current` / `Next` return one cached instance each; `SoftwareVersion` returns the instance the node was (re)started
  with.
- `RestartWithSoftwareAsync` restarts from the durable image (incarnation + 1, like `RestartAsync`) emulating that build:
  it advertises exactly the version's protocol window and feature set in every node/client handshake, reports them to
  the control group (feature eligibility / cluster status), and refuses to interpret messages, log entries, checkpoints
  or disk formats requiring a feature outside its set (`DfsErrorCode.Unsupported` / stops serving the affected group)
  instead of misreading them.
- Activating `NextFeature` through `DfsAdminClient.SetFeatureAsync` changes replicated/on-disk format so `Current` builds
  can no longer interpret new state; activation must be refused (`Unsupported`/`InvalidArgument`) until every member
  runs a build that supports it.
- Restarting into a build that cannot open the durable image (e.g. after activation) either throws `DfsException` with
  the refusal, or leaves the node running without voting for/leading/serving incompatible state.

Needed by: CHA-011 (rolling upgrade during failover, reconnecting an old-version node, feature negotiation safety).
