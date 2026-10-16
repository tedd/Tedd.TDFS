# MESH — multi-node meshes under every failure kind

**Origin:** new suite requested by the product owner ("spin up 2-10 instances locally that connect to each other in
different meshes; test combinations of nodes, failure cases, reads/writes, every thinkable scenario and edge case").
The IDs `MESH-001..MESH-038` are defined here; they are **not** rows of the test plan's §7 catalog, so
`TestCatalog.ExpectedCount` (586) is unchanged. Traceability: FSD §§3.2, 7–9, 10.3, 13, 14.1 (RCV-01..RCV-08), 15, 16
(AT-04, AT-06, AT-09, AT-15); test plan §§3, 4, 5, 8.3, 8.4.

**Status:** every row is **not run** — the product and most harness members are stubs (`NotImplementedException`).
The pure-logic rows (MESH-034, MESH-035) run and pass today; MESH-038 is a hardware row reported `not run` via
`Assert.Skip`.

Files:

| Level | File | Methods | Concrete cases |
|---|---|---:|---:|
| S | `tests/Tedd.Tdfs.Tests.Simulation/Mesh/MeshSimulationTests.cs` (+ `MeshHelpers.cs`) | 29 | 1101 |
| I + H | `tests/Tedd.Tdfs.Tests.Integration/Mesh/MeshIntegrationTests.cs` (+ `MeshHelpers.cs`) | 7 | 49 |
| U | `tests/Tedd.Tdfs.Tests.Unit/Mesh/MeshUnitTests.cs` | 5 | 39 |
| shared | `tests/Tedd.Tdfs.Testing/Additions/mesh/*.cs` (scenario driver, oracle, workload, pairwise generator) | — | — |

## 1. How every scenario is checked

Every cluster row runs through `ClusterTest.RunAsync` (S: `ClusterMode.Deterministic`, in-memory transport, simulated
disks, `NodeTimings.Fast`; I: realtime, QUIC, real disk) and is driven by `MeshScenario`:

1. **Formation.** The node layout and labels match the configuration; every node has a distinct `NodeId`; the control
   group reaches its configured (clipped) voter count and every other node is a learner; the volume group has exactly
   the configured voters with the declared placement (explicit / one-per-zone); the volume reports the configured data
   policy and voter count.
2. **Probe files.** A small file (inline exactly when length ≤ inline threshold **and** every majority of the volume
   voters spans at least the acknowledged hosts/zones — ARCHITECTURE §5.4, LAY-002) and a multi-chunk file are written;
   right after acknowledgement every object has at least the acknowledged copies/zones, and after repair (full mesh)
   the target copies/zones. Verified holders of every object are recorded.
3. **Load.** A concurrent multi-client workload (`MeshWorkload`: whole-file register writes/reads/deletes, appends,
   renames of a token file between two names, acknowledged unique writes, and transactions with reads, predicate reads
   and writes/deletes) and a namespace script (`MeshNamespaceScript`: mkdir/create/move file/move directory/delete,
   including invalid steps, each with a reserved operation id) run throughout. Clients enter through different nodes.
4. **Fault**, mirrored into an independent link/liveness model (`MeshNetworkModel`).
5. **Availability probes** (small/large write, small/large read, append, control-group change) are compared with the
   independent oracle `MeshOracle` (section 2): `Must` → succeeds within a bounded window with verified bytes;
   `Never` → fails typed on every attempt (never fabricated success); `Either` → success with verified bytes or a typed
   failure.
6. **Safety during the fault.** `AssertNoSplitBrainAsync`; acknowledged writes are never reported absent and never
   returned with other bytes (a typed failure is legal).
7. **Recovery** (resume, restart from durable images, heal), then: every probe `Must`; every workload client makes
   progress again; unknown outcomes are resolved by operation/transaction id (`resolve: true` must be definite); per-key
   linearizability (`RegisterModel`, `AppendLogModel`, `MeshRenameModel`) and strict serializability
   (`SerializabilityChecker`, including predicate reads) are `AssertValid`; the namespace subtree equals
   `ReferenceNamespaceModel` (paths, kinds, inode identity across moves, no two paths on one inode); every acknowledged
   write survives with exact bytes, every definitely failed write left no effect, every unknown write is absent or
   exact; acknowledged appends occur exactly once and in completion order, failed ones never, unknown ones at most once,
   no invented entries; acknowledged principals authenticate; repair restores the target; every control replica catches
   up; `AssertReplicasConvergedAsync`; `AssertNoSplitBrainAsync`.

Untyped failures anywhere in the workload are violations: only `DfsUnavailableException` subclasses (placement failures
must report achieved < required), `DfsCommitOutcomeUnknownException` with an operation id (mutations only),
sharing/transaction conflicts, lease loss, and expired operation ids/pins are legal. The last two arise when a client is
cut off from the authority for longer than the session timeout (10 s with `NodeTimings.Fast`; partitions, isolation):
FSD §13 requires ids outside the retained window to be rejected explicitly, never replayed. An expired id says nothing
about whether the operation took effect, so it is recorded as **unknown** (never as a definite failure); the namespace
script then resolves the step by id or, when the outcome is no longer retained, by observing its own subtree. A bare
`TimeoutException` is a violation — a deadline must surface as a typed availability error or, once publication may have
happened, as an unknown outcome (FSD §13). The rename model accepts "destination exists" as well as "source absent" for a
move whose token already sits at the destination (the specification does not fix which of the two errors is reported).

## 2. Availability oracle (`MeshOracle`)

Independent of the implementation; checked by MESH-034.

* **Quorum** of a group (volume or control) is computed over its **configured** voters (committed configuration read
  while healthy), never over reachable nodes (RCV-01, MUT-004): majority = ⌊V/2⌋+1, tolerance = ⌊(V-1)/2⌋.
  The quorum side is the connected component of live nodes (links usable in both directions) holding a majority.
  * no such component → `Never` (cause `MetadataQuorum`/`ControlQuorum` when the client reaches a live voter, else
    `Unreachable`);
  * the client reaches no node of it → `Never(Unreachable)`;
  * the component is a clique and the client reaches all its voters → `Must`;
  * otherwise (partial link meshes, single-link cuts — Raft without relaying may or may not elect) → `Either`.
* **Data acknowledgement policy** for a new object: upper bound = live, placement-eligible nodes the client reaches;
  fewer distinct hosts/zones than `DurableCopiesBeforeAck/DurableZonesBeforeAck` → `Never(Placement)` (decided before
  proposing, so never an unknown outcome, and the exception must name the unchanged required counts and smaller achieved
  counts); enough on the (clique) quorum side → `Must`; otherwise `Either`.
* **Reads** of extent-backed files need a live, reachable verified holder of every object, else
  `Never(NoHealthyReplica)` (`DfsUnavailableException` or `DfsIntegrityException`, never invented bytes or false absence).
* Inline files depend on the metadata quorum only.
* An operation's expectation combines its components: any `Never` → `Never`; all `Must` → `Must`; else `Either`.
* Mutations failing for a quorum/reachability cause may report `DfsCommitOutcomeUnknownException` (with a non-empty
  operation id); reads may not. Any operation failing for a quorum/reachability cause may be rejected with
  `DfsOperationExpiredException` (session expired while the client was cut off).
* **Re-registration after restart (RCV-02).** A node restarted while the control group has no connected, fully linked
  voter majority it reaches cannot re-register its new incarnation, and RCV-02 lets the cluster distrust it until then
  (receipts, votes, reads). Such nodes are kept in `MeshScenario.PendingRegistration` until the control group can register
  them; every volume/data expectation is then bounded by `MeshOracle.Bound`: `Must` only if it holds with those nodes
  down, `Never` only if it holds with them up, otherwise `Either`. Control-group expectations are unaffected
  (registration is itself a control operation).
* **Partial meshes at formation.** On a Ring/Line/Star link mesh, nodes without a direct link to the control leader
  that formed the cluster never received its log, so they could neither register nor learn which volume groups they host;
  they start in `MeshScenario.PendingRegistration` (bounded expectations as above) until a healed, fully linked mesh lets
  the control group register them.

## 3. Catalog

All cluster rows also carry the section 1 checks; the "Required assertion" column states what each row adds.

| ID | Level | Name | Required assertion | Invariants | Cases |
|---|---|---|---|---|---:|
| MESH-001 | S | MeshFormsUnderEveryBootstrapAndSeedGraph | For n = 2..10 under static bootstrap and seed join over Full/Ring/Star/Line/RandomConnected seed graphs: `ListNodes` returns n distinct Active nodes with their labels/ids/endpoints and the control-voter flag of the committed configuration (full mesh: reachable, incarnation ≥ 1); cluster/volume status report quorum and exactly the configured voters; every control replica converges on the same voter set (full mesh); every node works as the sole seed of a client (written through node i, read through node i+1 with exact bytes; on a partial mesh connecting/writing/reading through a stale node may fail, but only typed — an acknowledged file reported absent is a violation). | INV-03, INV-07, INV-11, INV-17, INV-18 | 54 |
| MESH-002 | S | HealthyMeshWorkloadIsLinearizableAndSerializable | Healthy mesh, four clients each entering through a different node: every client makes progress, all probes `Must`, histories linearizable per key, transactions strictly serializable, namespace equals the model, acknowledged writes exact, replicas converge. | INV-01, INV-02, INV-03, INV-04, INV-06, INV-07 | 24 |
| MESH-003 | S | PartialLinkMeshesKeepSafetyAndBoundedAvailability | Ring/Star/Line node links (voters placed where the mesh can elect them): after formation small reads/writes `Must` (a leader with a linked majority exists, clients reach every node), everything else per oracle; after crashing the volume leader no success is fabricated when no connected majority remains; at most one leader per term; after healing the mesh restriction every node catches up and converges. | INV-03, INV-08, INV-11, INV-17, INV-18 | 27 |
| MESH-004 | S | VolumeLeaderCrashAndRestart | Crash the volume leader under load (every crash mode): with V ≥ 3 a surviving voter leads in a higher term within the bound and probes match the oracle; with V = 1 the volume is unavailable (typed) until restart; the restarted node's incarnation is +1, it adopts at least the current term (bounded wait, when directly linked to the current leader) and catches up; no acknowledged write lost. | INV-01, INV-03, INV-05, INV-08, INV-17, INV-18 | 30 |
| MESH-005 | S | ControlLeaderCrashAndRestart | Crash the control leader: control changes continue (new control leader, higher term) exactly when a majority of the configured control voters survives, else fail typed (full mesh: only with ≤ 2 control voters); volume operations follow the volume group's quorum and data policy only (CON-017). | INV-08, INV-11, INV-17, INV-18 | 30 |
| MESH-006 | S | CrashVotersWithinTolerance | Crash exactly ⌊(V-1)/2⌋ configured volume voters (with or without the leader; a non-voter when the tolerance is 0): on a full mesh metadata is exactly `Must`; data per acknowledgement policy over surviving hosts/zones; reads succeed while a verified holder survives; acknowledged writes readable during the fault. | INV-01, INV-03, INV-08, INV-11, INV-17 | 30 |
| MESH-007 | S | CrashVotersBeyondTolerance | Crash one voter more than tolerated (followers first, old leader survives in a minority) or all voters: every metadata operation fails with `DfsQuorumUnavailableException` (mutations may be unknown with their id); after in-flight acknowledgements settle no surviving voter's commit index exceeds the highest index committed before the majority was lost (CON-007); unknown outcomes resolve definitely after restart. | INV-01, INV-06, INV-08, INV-11, INV-17 | 30 |
| MESH-008 | S | StorageNodeLossAgainstAckPolicy | Keep a volume-voter majority but crash storage hosts until exactly `ack` hosts survive (spread over as many zones as possible) or one fewer: at the threshold extent writes are `Must` iff the survivors also meet the acknowledged zones; below it every extent write fails with `DfsPlacementUnavailableException` reporting the unchanged required copies/zones and fewer achieved, although consensus is healthy (OFF-008); inline writes depend on the quorum only. | INV-01, INV-08, INV-11, INV-17 | 43 |
| MESH-009 | S | MajorityMinorityPartition | Partition into a side with a majority of the configured volume voters and the rest, old leader on either side, one probe client and half the workload per side: the majority side elects (higher term) and serves exactly per oracle; the minority-side client only fails typed (no lease, no live read, no acknowledged write — AT-06); an isolated old leader neither advances its commit index nor wins a new term. | INV-01, INV-03, INV-05, INV-06, INV-08, INV-18 | 24 |
| MESH-010 | S | AsymmetricAndHalfOpenLinks | Cut only the leader's outbound or inbound node links, black-hole its outbound traffic, or cut leader→one follower: a leader without complete exchanges with a majority is replaced in a higher term (NET-011: consensus counts completed exchanges, not one-way reachability); a single one-way cut leaves leader and term unchanged (PreVote/CheckQuorum) with metadata `Must`. | INV-03, INV-05, INV-08, INV-17, INV-18 | 35 |
| MESH-011 | S | ClientIsolationVariants | Servers fully connected; one client isolated completely, reaching only the leader, all but the leader, or only a non-voter: that client gets exactly the oracle's outcome (reaching only the leader: metadata `Must`, extent writes need `ack` reachable hosts else `Placement`); every other client keeps progressing. | INV-06, INV-08, INV-12, INV-17 | 36 |
| MESH-012 | S | PausedNodeCannotRegainAuthority | Pause the volume leader, the control leader or another node beyond the election window and twice the lease, with a write of the probed key (reserved id) queued at the paused leader by a client routed to it: the rest behaves as if it were down (oracle); the new authority fences the queued write's id (`resolve: true` definite) and commits a new value, which is still the value read after the paused node's queued work ran (OFF-021); a resumed former leader follows the newer term. | INV-01, INV-03, INV-05, INV-08, INV-18 | 28 |
| MESH-013 | S | RollingRestartOfEveryNode | Stop (graceful) or crash every node in turn under load: while each is down probes match the oracle and acknowledged writes stay readable; after each restart incarnation +1, the cluster settles and (full mesh) every probe succeeds. | INV-01, INV-03, INV-08, INV-17, INV-18 | 30 |
| MESH-014 | S | LinkFlappingConverges | Ten seeded cycles of cutting/restoring node links, the leader's links, or client links (100–800 ms): never two leaders in one term; afterwards every client progresses, appends are never duplicated, replicas converge. | INV-02, INV-06, INV-17, INV-18 | 28 |
| MESH-015 | S | AddNodesDuringLoad | Add one node, fill up to ten nodes sequentially, add up to three concurrently, or add one while the control leader is crashed: each new node is listed exactly once Active and reachable with capacity, is a control member exactly once (voters stay within [before, configured]), catches up, works as the sole seed of a client (exact bytes); a join during a control failover completes through a surviving control leader; probes match the oracle throughout. | INV-11, INV-17, INV-18 | 29 |
| MESH-016 | S | DrainAndRemoveNodesDuringLoad | Drain a non-voter, a volume voter or the volume leader: if the rest can hold the target and a replacement voter, the drain completes (Drained), the node holds no voter seat and every object referenced at completion has its full target elsewhere after repair; removal marks it Removed, and restarting it with its old disk regains no vote, leadership or membership (CON-016, RSY-020). Otherwise the drain stays Draining, removal without force is rejected with a typed error (any `DfsException` code except Internal/OutcomeUnknown/Timeout/ProtocolError/Cancelled — OPS-002 names none), and all data stays readable (OPS-001/002). | INV-08, INV-09, INV-11, INV-17, INV-18 | 27 |
| MESH-017 | S | WholeZoneOutage | Crash every node of the leader's zone / another zone (one per zone), either zone of 2+1, or the single zone: on a full mesh metadata is `Must` iff a majority of configured voters survives; a surviving-zone voter leads; data per acknowledged copies/zones of survivors (OFF-006, REP-010/011). | INV-01, INV-08, INV-11, INV-17 | 44 |
| MESH-018 | S | TotalOutageColdRestartInEveryOrder | Crash every node, then cold-restart one at a time ascending, descending, voters last or voters first: all-down probes fail typed; after each restart availability is the oracle's, where a node restarted before the control group regained a quorum only bounds it (RCV-02 re-registration, section 2); no bootstrap deadlock; acknowledged writes survive from durable images only (CON-020, OFF-018). | INV-01, INV-06, INV-08, INV-17, INV-18 | 36 |
| MESH-019 | S | EveryOfflineSubsetForCompactMeshes | For 2, 3, 4 and 5 nodes every subset of offline nodes including none and all (test plan §5): the oracle is exact (never `Either`) and every probe matches it; after restart everything converges. | INV-01, INV-08, INV-11, INV-17 | 60 |
| MESH-020 | S | EveryPartitionShapeForThreeAndFiveNodes | Every set partition of 3 and 5 nodes with the probe client in each group: exactly the group holding a majority of the configured voters serves, and only to a client inside it (oracle exact); no split brain; healing converges. | INV-03, INV-05, INV-08, INV-18 | 161 |
| MESH-021 | S | EvenAndSingleVoterGroupsTolerance | Volume groups of 1, 2, 4 voters and control groups of 2, 4: crashing ⌊(V-1)/2⌋ keeps the group available, one more makes it unavailable with a typed error although half the voters live (even groups gain no tolerance). | INV-08, INV-11, INV-17 | 82 |
| MESH-022 | S | ControlQuorumLossKeepsEstablishedVolumesServing | Control voters n0.., volume voters at the other end; lose the control quorum while the volume keeps its majority: control changes fail typed, established volume reads/writes match the oracle's `Must` (CON-017); after restart control changes work again. | INV-08, INV-11, INV-17 | 10 |
| MESH-023 | S | VolumesSpreadLeadershipAndFailIndependently | Two or three volumes with the same explicit voters or disjoint explicit voter blocks (voter sets asserted): shared voters get leadership on more than one node (FSD §3.2) and re-elect every volume after the shared leader crashes; with disjoint blocks, losing one volume's voters makes only it unavailable (writes and live reads fail typed per oracle, no false absence) while the others serve (CON-018); every volume converges. | INV-08, INV-11, INV-17 | 39 |
| MESH-024 | S | ClientsThroughEveryEntryNodeSurviveEntryLoss | One client per node with that node as its only seed: cross-entry write/read with exact bytes; after the entry node crashes its client keeps working through the discovered cluster map exactly as the oracle permits, and fully after restart. | INV-03, INV-08, INV-17 | 24 |
| MESH-025 | S | LaggingNodeCatchesUpThroughCheckpoint | Keep a volume voter (or a control learner) down while more than the Raft snapshot threshold of entries commit (overwrites, delete, rename — directory creations instead of overwrites when the data policy cannot acknowledge without that voter / principal creations) and the leader compacts past its applied index: on return it installs a checkpoint (`raft.snapshot-install` reached on it, snapshot index > applied before), catches up to the leader's commit index at its return, deleted/renamed names do not resurrect on it, principals created meanwhile authenticate through it alone, replicas converge (RCV-03). | INV-06, INV-07, INV-18 | 25 |
| MESH-026 | S | SecondFailureDuringRecovery | Crash leader L1, commit W under L2 (a file when the data policy can acknowledge it without L1, else a directory), crash L2 (V = 3: no majority, typed failures; V = 5: L3), restart L1 while L2 is down, then L2: availability per oracle at every step and W never lost although L1 never saw it (read exactly whenever the oracle requires it, and after recovery; CON-005). | INV-01, INV-03, INV-08, INV-18 | 25 |
| MESH-027 | S | MembershipChangeDuringPartitionKeepsOneAuthority | Partition into a side with a control-voter majority and the rest, a client on each: adding a well-formed node through the minority fails typed (quorum, or availability of the unknown endpoint; never success); adding through the majority succeeds once; after healing every node agrees on one membership with the new node exactly once and no phantom member (CON-014). | INV-08, INV-11, INV-18 | 18 |
| MESH-028 | I | QuicDiskMeshFormsAndServes | 2, 3, 5, 7, 10 real nodes over QUIC with disk storage, static or seed join (star, random): quic:// endpoints and real data directories, every node Active, every node as sole client entry point (exact bytes over QUIC), smaller workload valid, replicas converge. | INV-03, INV-07, INV-11, INV-17 | 15 |
| MESH-029 | I | QuicDiskLeaderCrashAndRestartFromDisk | Abruptly crash the volume leader (lose-unflushed, torn-last-write) under load: successor in a higher term when a majority survives, probes per oracle, restart from the on-disk image with incarnation +1, nothing acknowledged lost. | INV-01, INV-03, INV-08, INV-17, INV-18 | 10 |
| MESH-030 | I | QuicDiskMajorityMinorityPartition | Real QUIC partition into a voter majority (without the old leader when V ≥ 3) and the rest, a client per side: majority serves per oracle, minority client fails typed only, no split brain, unknown outcomes resolve after healing. | INV-03, INV-05, INV-06, INV-08, INV-18 | 5 |
| MESH-031 | I | QuicDiskRollingRestart | Graceful or abrupt restart of every node in turn: probes per oracle while down, on-disk restart with incarnation +1, settled cluster, nothing acknowledged lost. | INV-01, INV-08, INV-17, INV-18 | 10 |
| MESH-032 | I | QuicDiskQuorumLossThenColdRestart | Crash one voter beyond tolerance (all operations typed failures), then crash all and cold-restart from disk: every acknowledged write and committed unknown outcome present, definitely failed writes absent. | INV-01, INV-06, INV-08, INV-17 | 5 |
| MESH-033 | I | OutOfProcessHostsKilledAndRestarted | 3 and 5 real `Tedd.Tdfs.Host` processes: kill the volume leader's process, survivors take over and serve per oracle, restart the process from its directory, nothing acknowledged lost, unknown outcomes resolve, replicas converge. | INV-01, INV-06, INV-17, INV-18 | 2 |
| MESH-034 | U | AvailabilityOracleQuorumArithmetic | The oracle against hand-computed values: V = 1..7 configured voters × k = 0..V crashed → `Must` iff V-k ≥ ⌊V/2⌋+1 (cause `MetadataQuorum`, or `Unreachable` when no voter lives), tolerance ⌊(V-1)/2⌋; plus variant methods `AvailabilityOracleTopologyCases` (partitions, Ring/Star/Line links, one-way cuts, partial and isolated clients, paused nodes), `AvailabilityOraclePlacementAndClassification` (ack copies/zones, shared hosts, ineligible nodes, holders, guaranteed metadata durability and the inline rule, legal/illegal failure types incl. weakened policy counts, unknown outcomes on reads and expired ids; expired ids classified unknown; `Bound` for nodes that may or may not count), `RenameModelRejectsTornRenames` (incl. "destination exists" for a token already at the destination). | INV-02, INV-08, INV-11, INV-14, INV-20 | 38 |
| MESH-035 | U | PairwiseMatrixCoversEveryValidPair | The covering-array generator keeps every valid critical row exactly once, covers every coverable pair, emits only valid rows, reports critical rows without a valid completion, is deterministic; configuration encoding round-trips, stays ≤ 50 characters, and every generated policy validates. | INV-20 | 1 |
| MESH-036 | S | ClockSkewAndDriftNeverOrderCommits | Leader clock at 1.5× / 0.6×, leader wall clock −1 h, followers +1 h, or all nodes drifting (0.80–1.25×, ±60 min). Server clock changes may reduce availability but never safety (LOCK-015, SEC-017): while skewed every write that succeeds gets a strictly greater comparable commit token and revision than the earlier acknowledged ones and reads back exactly (one file per step), also after authority moves to a skewed node (elections run on monotonic ticks, so a successor is required); a leased writer held across three lease durations either commits (bytes read back) or fails typed with its stream faulted and its bytes never published (INV-05); probes are the oracle's with required successes relaxed. After the clocks are realigned the oracle's availability and the ordering are required again. | INV-03, INV-04, INV-05, INV-08, INV-17 | 45 |
| MESH-037 | S | SlowLinksKeepSafetyAndConditionalProgress | Node links delayed by ¼ of the minimum election timeout (oracle unchanged), by 2× the maximum election timeout on every link (no progress promised: success with exact bytes or typed failure), or on every link touching the leader's zone (service must continue iff the fast nodes hold a configured-voter majority): no split brain, also after delayed traffic arrives; removing the delay restores full service within the bound. | INV-01, INV-03, INV-05, INV-08, INV-17 | 27 |
| MESH-038 | H | PowerCutOfEveryMeshHostOnSupportedHardware | 3- and 5-host meshes of real host processes on supported devices: power cut of every host (and of a voter majority), cold boot in arbitrary order; acknowledged writes and committed unknown outcomes survive from device state alone, histories valid, replicas converge. Validates the configuration, then `Assert.Skip` — needs a power-cut rig (**not run**). | INV-01, INV-06, INV-17, INV-18 | 2 |

Totals: 38 IDs; 41 test methods (S 29, I 6, H 1, U 5); 1189 concrete cases (S 1101, I 47, H 2, U 39).

## 4. Matrix dimensions and the pairwise reduction

### 4.1 Dimensions

| Dimension | Key | Values (default first) | Code in the theory string |
|---|---|---|---|
| Node count | `n` | 2..10 | separate theory argument |
| Bootstrap + seed graph | `boot` | Static; SeedJoin × Full/Ring/Star/Line/RandomConnected | `St`, `SjFull`, `SjRing`, `SjStar`, `SjLine`, `SjRand` |
| Link mesh | `mesh` | Full, Ring, Star, Line | name |
| Zone topology | `topo` | OnePerZone, TwoPlusOne, SingleZone | `1pz`, `2+1`, `1z` |
| Control voters | `cv` | 3, 1, 5 (clipped to n) | `c3` |
| Volume voters | `vv` | 3, 1, 5 | `v3` |
| Data ack copies | `ack` | 2, 1, 3 | `a2` |
| Crash mode | `crash` | LoseUnflushed, KeepUnflushed, RandomSubset, TearLastWrite | `LU`, `KU`, `RS`, `TL` |
| Family variant | `variant` | per family (failure sub-kind) | trailing word |

Data policy for a configuration: ack copies = `ack`; target = max(ack, min(3, n)); target zones = min(target, zones);
ack zones = min(ack, target zones). Volume voters: explicit central voters on partial meshes (Star: hub and first
leaves; Ring/Line: a centred contiguous block, control and volume blocks concentric), else one-per-zone when there are
enough zones, else any. Control voters on partial meshes are forced through the static bootstrap description.
On Ring/Line link meshes the second zone of `2+1` (the same ⌊n/3⌋ nodes) is the centred block of the line rather than its
end: groups replicate without relaying, so only nodes linked to the control leader register (RCV-02) and can acknowledge
data, and with centred voter blocks a second zone at the far end would make the acknowledged zones unreachable by
construction (the probe files of section 1 could never be written).

Excluded (invalid) configurations, each with its reason (`MeshConfig.Invalid`): volume voters or ack copies above the
node count (impossible); seed join on Ring/Line link meshes (a joiner cannot reach the control leader n0); seed join on a
Star mesh unless the seed graph includes n0 (Full or Star seeds); explicit control voters other than with static
bootstrap and the configured count; Star mesh with 3 control voters and 5 volume voters (a leaf control leader's log
reaches only the hub, so 5 volume voters could not form). Family constraints are listed in 4.3.

### 4.2 Method

Critical dimensions — node count × failure kind/variant (plus the dimension a family is about: seed graph in MESH-001,
link mesh in MESH-003, ack in MESH-008, topology in MESH-017, voters in MESH-023/MESH-026, control voters in
MESH-027) — are enumerated **exhaustively**. The other dimensions are reduced with `MeshPairwise.Generate`, a
deterministic constraint-aware covering-array generator: each valid critical row is kept exactly once and completed
with the valid non-critical assignment covering the most still-uncovered pairs (ties prefer defaults); extra rows are
then added until every coverable pair is covered. Required pairs: every pair of two non-critical values and every
(critical variant/dimension, non-critical value) pair; pairs with the node count are not required because node count ×
variant is already exhaustive and MESH-001 covers node count × bootstrap exhaustively. A pair is coverable when it
occurs in at least one valid row. Pairwise sampling reduces only non-critical configuration combinations; it does not
replace the exhaustive bounded campaigns (test plan §8.4) — MESH-019/020/021 and the integration rows are explicit,
not sampled. Seeds: `TestClusterOptions.Seed = 0` derives the seed from the test name and is reported on failure.

### 4.3 Reduction per family (exact generator output)

| Family | Critical (exhaustive) | Non-critical (pairwise) | Constraint | Critical rows | Excluded critical rows (reason) | Extra rows | Generated | Pairs covered | Unconstrained product |
|---|---|---|---|---:|---|---:|---:|---:|---:|
| MESH-001 | n × boot (6) | mesh, topo, cv, vv, ack | — | 54 | 0 | 0 | 54 | 185/185 | 17,496 |
| MESH-002 | n | all but crash | — | 9 | 0 | 15 | 24 | 185/185 | 17,496 |
| MESH-003 | n × mesh {Ring, Star, Line} | boot, topo, cv, vv, ack, crash | — | 27 | 0 | 0 | 27 | 203/203 | 52,488 |
| MESH-004 | n | all | — | 9 | 0 | 21 | 30 | 273/273 | 69,984 |
| MESH-005 | n | all | — | 9 | 0 | 21 | 30 | 273/273 | 69,984 |
| MESH-006 | n × {WithLeader, FollowersOnly} | all | WithLeader needs V ≥ 3 | 18 | 1 (n=2 WithLeader: two nodes cannot hold three voters) | 13 | 30 | 324/324 | 139,968 |
| MESH-007 | n × {Minimal, AllVoters} | all | — | 18 | 0 | 12 | 30 | 325/325 | 139,968 |
| MESH-008 | n × ack {1,2,3} × {At, Below} | boot, topo, cv, vv, crash (full mesh) | survivors ≥ 1, ≥ voter majority, ≤ n | 54 | 11 (ack 3 on 2 nodes; "Below" with ack 1 leaves no survivor) | 0 | 43 | 232/232 | 34,992 |
| MESH-009 | n × {LeaderInMajority, LeaderInMinority} | all but crash | LeaderInMinority needs V ≥ 3 | 18 | 1 (n=2 LeaderInMinority) | 7 | 24 | 228/228 | 34,992 |
| MESH-010 | n × 4 link variants | boot, topo, cv, vv, ack (full mesh) | FollowerOneWayCut needs V ≥ 3 | 36 | 1 (n=2 FollowerOneWayCut) | 0 | 35 | 197/197 | 17,496 |
| MESH-011 | n × 4 client variants | boot, topo, cv, vv, ack (full mesh) | OnlyNonVoter needs n > V | 36 | 0 | 0 | 36 | 198/198 | 17,496 |
| MESH-012 | n × {VolumeLeader, ControlLeader, OtherNode} | all but crash | — | 27 | 0 | 1 | 28 | 251/251 | 52,488 |
| MESH-013 | n × {Graceful, Crash} | all | — | 18 | 0 | 12 | 30 | 325/325 | 139,968 |
| MESH-014 | n × {NodeLinks, LeaderLinks, ClientLinks} | all but crash | — | 27 | 0 | 1 | 28 | 251/251 | 52,488 |
| MESH-015 | n (2..9) × {One, Fill, Concurrent, DuringControlFailover} (n=9: One, DuringControlFailover) | boot, topo, cv, vv, ack (full mesh) | DuringControlFailover needs ≥ 3 control voters | 30 | 1 (n=2 DuringControlFailover) | 0 | 29 | 197/197 | 14,580 |
| MESH-016 | n × {NonVoter, VolumeVoter, VolumeLeader} | boot, topo, cv, vv, ack (full mesh) | NonVoter needs n > V | 27 | 0 | 0 | 27 | 180/180 | 13,122 |
| MESH-017 | n × (topology, zone) {1pz leader zone, 1pz other zone, 2+1 z1, 2+1 z2, 1z z1} | boot, mesh, cv, vv, ack, crash | n=2 has no z2 in 2+1 | 44 | 0 | 0 | 44 | 365/365 | 114,048 |
| MESH-018 | n × {Ascending, Descending, VotersLast, VotersFirst} | all | — | 36 | 0 | 0 | 36 | 377/377 | 279,936 |
| MESH-019 | n ∈ {2..5} × every offline subset (2ⁿ) | fixed compact layout (static, full, one per zone, c3, V=1/3, ack 1/2) | — | 60 | 0 | — | 60 | exhaustive | 60 |
| MESH-020 | every set partition of 3 and 5 nodes × client group | fixed (static, full, one per zone, c3, v3, a2) | — | 161 | 0 | — | 161 | exhaustive | 161 |
| MESH-021 | n × voters {V 1,2,4 / CV 2,4} (≤ n) × {AtTolerance, BeyondTolerance} | fixed (static, full, one per zone, ack 1) | — | 82 | 0 | — | 82 | exhaustive | 82 |
| MESH-022 | n | topo, cv, vv, ack (static, full mesh) | control loss must leave the volume majority | 9 | 0 | 1 | 10 | 66/66 | 729 |
| MESH-023 | n × {Shared2, Shared3, Disjoint2, Disjoint3} (vv derived) | boot, topo, cv, ack, crash (full mesh) | disjoint blocks need n ≥ volumes | 35 | 0 | 4 | 39 | 273/273 | 22,680 |
| MESH-024 | n | boot, topo, cv, vv, ack, crash (full mesh) | — | 9 | 0 | 15 | 24 | 198/198 | 17,496 |
| MESH-025 | n × {VolumeVoter, ControlLearner} | boot, topo, cv, vv, ack, crash (full mesh) | VolumeVoter needs V ≥ 3; a control learner exists or the control group tolerates a loss | 18 | 1 (n=2 VolumeVoter) | 8 | 25 | 241/241 | 34,992 |
| MESH-026 | n (3..10) × vv {3, 5} | boot, topo, cv, ack, crash (full mesh) | V ≤ n | 14 | 0 | 11 | 25 | 179/179 | 9,072 |
| MESH-027 | n (3..9) | boot, topo, vv, ack, cv {3,5} (full mesh) | ≥ 3 effective control voters | 7 | 0 | 11 | 18 | 111/111 | 2,268 |
| MESH-036 | n × 5 clock variants | boot, topo, cv, vv, ack (full mesh) | — | 45 | 0 | 0 | 45 | 216/216 | 21,870 |
| MESH-037 | n × 3 latency variants | boot, topo, cv, vv, ack (full mesh) | — | 27 | 0 | 0 | 27 | 180/180 | 13,122 |

"Unconstrained product" is the full Cartesian product of the critical rows with every non-critical value before
validity filtering — what exhaustive enumeration of these families would have cost (1,401,522 cluster runs in total
versus the 1,101 generated).

## 5. Integration layout (real QUIC + disk)

Non-critical dimensions are fixed per node count so the real-boundary runs still cover different voter counts,
topologies and policies (`MeshHelpers.Base` in the integration project). Smaller workload: 2 clients, 2 register keys,
1 log, 1 rename token, 2 transaction keys, 4..80 operations per client, 8 namespace steps; bounds: 45 s for `Must`,
120 s for recovery, 6 min per scenario.

| Nodes | Topology | Control voters | Volume voters | Ack |
|---:|---|---:|---:|---:|
| 2 | one per zone | 3 (clipped to 2) | 1 | 1 |
| 3 | one per zone | 3 | 3 | 2 |
| 5 | one per zone | 5 | 5 | 3 |
| 7 | 2+1 | 5 | 3 | 2 |
| 10 | one per zone | 3 | 5 | 2 |

## 6. Assumptions on harness behaviour

The suite relies on documented-but-stubbed harness behaviour (no signature changes); the precise list is in
`docs/api-additions/mesh.md` ("Harness semantics relied upon").
