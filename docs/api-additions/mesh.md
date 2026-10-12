# Harness/API additions for the MESH suite

Branch `tests/mesh`. **No stub, product member or server inspector was added, and no existing harness, SDK or server file
or signature was changed.** The suite needs only members already listed in `docs/API.md` (sections C, D and F).

What was added is test-side helper code shared by the simulation and integration projects, so it lives in the harness
library as **new files** under `tests/Tedd.Tdfs.Testing/Additions/mesh/` in the new namespace `Tedd.Tdfs.Testing.Mesh`.
All of it is ordinary implemented code built only on the public SDK and the harness API (it compiles today; the parts
that need a cluster fail at the stubbed harness members until those are implemented). It shares no code with the
product, so it can serve as an independent oracle.

## Added types (`tests/Tedd.Tdfs.Testing/Additions/mesh/`)

| File | Types | Purpose |
|---|---|---|
| `MeshConfig.cs` | `MeshBoot`, `MeshConfig` | One matrix configuration (node count, bootstrap + seed graph, link mesh, topology, control/volume voters, ack copies, crash mode, variant, optional explicit voters). Validity rules (`Invalid()` with reasons), data policy derivation, voter placement for partial meshes, `ToClusterOptions(mode, transport, storage, timeout)`, compact `Format()`/`Parse()` theory encoding, the non-critical dimension list. |
| `MeshPairwise.cs` | `MeshDimension`, `MeshPairwiseResult`, `MeshPairwise` | Deterministic constraint-aware covering-array generator (critical rows exhaustive, non-critical pairs covered) with exact counts (critical, excluded, extra rows, coverable/covered pairs). |
| `MeshNetworkModel.cs` | `MeshNetworkModel` | Test-side mirror of node liveness, placement eligibility and link state (mesh restriction, partitions, isolation, one-way cuts, black holes, heal). |
| `MeshExpectation.cs` | `MeshTri`, `MeshCause`, `MeshProbeKind`, `MeshExpectation` | Three-valued availability expectation (`Must`/`Either`/`Never`) with the failure causes that are legal. |
| `MeshOracle.cs` | `MeshOracle` | Independent availability oracle: quorum over configured voters, data acknowledgement policy over usable hosts/zones, replica holders, guaranteed metadata durability and the inline rule, legal-failure classification. |
| `MeshRenameModel.cs` | `MeshRenameModel` | `ISequentialModel<string>` for a token file renamed between two names (torn renames are impossible states). |
| `MeshWorkload.cs` | `MeshWorkloadOptions`, `MeshWorkload` | Concurrent multi-client workload recorded in `TestCluster.History` and a transaction list; resolves unknown outcomes by id; checks linearizability, strict serializability, acknowledged-write durability and typed failures. |
| `MeshNamespaceScript.cs` | `MeshNamespaceScript` | Single-process namespace script through faults (ids reserved before transmission, retries with the same id, unknown outcomes resolved), compared with `ReferenceNamespaceModel` including inode identity. |
| `MeshScenario.cs` | `MeshScenarioOptions`, `MeshScenario` | Scenario driver: formation checks, probe files and holders, fault methods mirrored into the model, oracle probes, safety during faults, `WaitSettledAsync`, recovery and the final checks. |

Suite-local helpers (internal, per G conventions): `tests/Tedd.Tdfs.Tests.Simulation/Mesh/MeshHelpers.cs` (matrices,
`MustAsync`, `NotPromised`, set partitions, generator reports) and `tests/Tedd.Tdfs.Tests.Integration/Mesh/MeshHelpers.cs`
(real QUIC/disk layouts per node count, smaller workload).

## Harness semantics relied upon

These are behaviours of existing (currently stubbed) harness members that the MESH rows assume. None needs a new member
or a signature change; the harness implementation should satisfy them (or the affected rows must be revisited).

1. **Link meshes are in force from cluster start.** `TestClusterOptions.Mesh` restricts node-to-node links during
   formation and afterwards, by creation index exactly as `LinkMesh` documents (Star hub = `n0`; Ring wraps for n > 2;
   Line = i±1). Clients reach every node unless cut. Nodes added later are linked to everyone.
2. **Stability on a partial mesh.** `ClusterTest.RunAsync`'s initial wait, `WaitForStableAsync` and
   `WaitForReplicationAsync` cannot require a node without a direct link to a group leader to catch up (Raft without
   relaying never delivers the log to it): on a partial mesh "stable" means control and volume leaders are elected and
   every node directly linked (both directions) to the respective leader has caught up. Crashed/stopped nodes are
   excluded from every "caught up" condition. Mid-scenario the suite uses `MeshScenario.WaitSettledAsync`, which on a
   partial mesh waits only for leaders.
3. **`TestNetwork.Heal`** removes cuts, black holes, partitions, isolation **and** the mesh restriction (as documented).
   The suite never relies on `Restore(from, to)` lifting a mesh restriction: MESH-014 flaps only links the mesh has.
4. **`TestNetwork.Partition`**: a name listed in no group is cut from everyone (the suite always lists every known
   client). Names of nodes that do not exist yet are accepted and take effect when the node is created (MESH-027 places
   the node it is about to add on the majority side). Traffic the harness generates for itself (`TestCluster.Admin`,
   joins started by `AddNodeAsync`, inspectors) must not depend on the partitioned scenario clients: a joining node
   uses its own `JoinSeeds` and is subject to the partition like any node.
5. **`TestNetwork.SetLatency(from, to, TimeSpan.Zero)`** removes a latency set earlier; latency applies per direction
   and only to traffic sent after the call.
6. **`TestNode.Clock`** is the node's `TimeProvider` (Raft ticks, lease tracking on the leader, file timestamps), so
   `SetRate`/`JumpWall` skew exactly that node (MESH-036). Client clocks stay independent (`TestClientOptions.Clock`).
7. **`TestClusterOptions.ConfigureNodeOptions`** receives options whose `Bootstrap` is the shared static
   `ClusterBootstrap`; overriding `Bootstrap.ControlVoters` there (identically for every node) places the control
   voters explicitly (partial meshes, MESH-022).
8. **Voter counts.** `TestClusterOptions.ControlVoters` is clipped to the node count and, under seed join, learners are
   promoted until that count is reached; `TestVolumeSpec.Metadata` with `DfsVoterPlacement.Explicit` places exactly
   the listed voters; even voter counts (2, 4) are accepted (MESH-021).
9. **Restart identity.** `TestNode.RestartAsync` restarts from the durable image with incarnation + 1 (asserted by the
   suite); for `OutOfProcess` clusters `CrashAsync` kills the host process and `TestNode.Host` is `null`.
10. **Availability errors.** Operations that cannot reach the required quorum/placement within the SDK operation
    timeout surface as `DfsUnavailableException` subclasses (or, after possible publication, as
    `DfsCommitOutcomeUnknownException` with the operation id) — never as a bare `TimeoutException` (FSD §13; the
    workload records a bare timeout as a violation).
