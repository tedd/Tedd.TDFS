# Tedd.TDFS — Release evidence (VER-07)

**Build:** `main` @ 65ec067 (plus the fixes listed in §6 once merged) · **Date:** 2026-10-17 ·
**Environment:** Windows 11 Pro 10.0.26200, .NET SDK 10.0.401 (`net10.0`), 32 logical cores, 128 GB RAM, MsQuic
(Schannel) via Tedd.Quicly 0.2.2.

This report states what the test campaign established and what it did not. Per FSD VER-01 and test plan §1, no number
below is a claim of unconditional correctness: results hold for the requirements, configurations, fault model and
exploration bounds actually exercised.

## 1. Summary

| Project | Level | Run | Passed | Failed | Skipped |
|---|---|---|---|---|---|
| `Tedd.Tdfs.Tests.Unit` | U (+ internal component tests) | 5,128 | 5,128 | 0 | 0 |
| `Tedd.Tdfs.Tests.Simulation` | S (deterministic and in-process multi-node) | 3,435 | 3,434 | 1 | 0 |
| `Tedd.Tdfs.Tests.Integration` | I / H (real QUIC, real disks, real processes) | 472 | 458 | 2 | 12 |
| `Tedd.Tdfs.Tests.Performance` | P | 16 | 16 | 0 | 0 |
| `Tedd.Tdfs.Tests.Models` | M (bounded model checking) | 57 | 57 | 0 | 0 |
| **Total** | | **9,108** | **9,093** | **3** | **12** |

The three failures and their status are in §6. Wall time of a full run: Unit 7 min, Simulation 27 min, Integration
10 min, Performance 2 min, Models 18 min.

## 2. Requirement and catalog coverage

* **Named catalog:** every one of the 586 named cases of the [test plan](spec/distributed-filesystem-test-plan.md) and the
  78 cases of [Addendum A](spec/fsd-addendum-acl-pools-streams.md) (ACL 34, POOL 18, ADS 26) has at least one test
  carrying its `[Catalog]` id; `CatalogCoverageTests` enforces this mechanically from the embedded catalogs. Rows are
  test *families*: most expand into theory matrices (e.g. every `FileMode × FileAccess × FileShare` combination, every
  fault stage, every offline subset of 3- and 5-node models).
* **Mesh matrix (product owner request):** 38 `MESH-*` cases ([test-notes/mesh.md](test-notes/mesh.md)) spin up 2–10
  nodes with static and seed-join bootstrap, full/ring/star/line/random seed graphs and link meshes, 1/2+1/one-per-zone
  topologies, 1/3/5 control and volume voters and ack 1–3 policies, under crashes within and beyond tolerance,
  partitions, pauses, rolling restarts, flapping, joins and drains. Histories are checked by the linearizability and
  strict-serializability checkers; availability is compared with quorum and placement arithmetic over *configured*
  voters. 1,101 simulation and 49 real-QUIC/disk executions.
* **Test counts by suite** (all levels): ACL 175, ADS 231, APP 54, BAK 188, CHA 77, CON 119, DSK 217, FILE 147,
  FMT 297, GC 67, HAR 224, LAY 151, LOCK 212, MESH 1,189, MOD 21, MUT 16, NET 183, NS 110, OFF 484, OPEN 256, OPS 36,
  OUT 348, PATH 216, PERF 16, POOL 69, PROP 65, PUB 178, REP 92, RST 41, RSY 91, SDK 117, SEC 102, SNP 157, STR 293,
  TX 194; internal component tests 2,659.

## 3. Fault coverage

* **Stage registry:** every server and SDK operation reports its reachable stages (FSD VER-04, test plan §8.2 F00–F21)
  through `IFaultInjector`; tests arm faults by site, node, volume, operation, path and occurrence and assert that the
  intended point was reached (`TestFaults.AssertReached`). Unreached injections fail the test.
* **Operation × stage × fault matrix (NET-020):** generated from `FaultSites.All × DfsOperationKind` with explicit
  applicability predicates; every generated case is accounted as applicable-and-reached or excluded with a reason. One
  documented exclusion: Raft append batches are one-way background traffic with no reply stage; they are covered by
  network faults and the `raft.*`/`recovery.*` sites instead.
* **Compound scenarios:** all 16 `CHA-*` scenarios pass.
* **Real environment:** Integration runs selected scenarios over real QUIC (MsQuic), real files with flush barriers,
  and out-of-process `Tedd.Tdfs.Host` nodes killed with process termination.

## 4. Checkers, models and negative controls

* **Independent oracles:** the linearizability (Wing–Gong/Lowe with memoisation, per-key partitioning, unknown-outcome
  handling) and strict-serializability checkers were cross-validated against brute-force checkers on 30,000 random
  small histories each (0 disagreements) and reject the canonical bad histories of HAR-011.
* **Bounded model checking:** CON-001 explores the *real* `RaftNode` core exhaustively within its stated bounds (3 and 5
  voters, bounded message/crash budgets) — all 36 rows complete. The state-space reductions used (abstract keys,
  discarding provably ineffective messages, subsumption) are each argued against the Raft code; an unsound rule found in
  review was removed, and deliberately injected bugs (commit without quorum, vote without log check) are caught on every
  row. MOD-001…MOD-012 check abstract models of fencing, data-then-metadata publication, idempotency/retirement,
  transaction validation, snapshot boundaries, backup publication, GC reachability, membership changes, recovery epochs
  and conditional liveness; each completes within its bounds and has a broken variant that the checker must reject.
  MOD-012 records bounds, assumptions and the model-to-implementation refinement gap (the abstract models are not a
  proof that the production code refines them).
* **Randomized campaigns:** the Raft safety campaign passed 18,000+ seeded runs (3 and 5 voters, partitions, crashes,
  drops, duplicates, reordering, snapshots, membership changes); deterministic whole-cluster simulations replay exactly
  from their seed (HAR-001).
* **Negative controls (MUT-001…015):** each safety mutant (removed fencing, ack before flush, metadata before data,
  quorum from reachable nodes, disabled dedup, unknown-as-aborted, skipped predicate validation, stale live reads,
  ignored snapshot pins, early backup publication, skipped checksums, same-zone double counting, forgotten expired-id
  watermark, relaxed policy on timeout) is detected by its regression scenario, and the scenario passes without the
  mutant. MUT-015 confirms every critical mutant is killed.

## 5. Not run / out of envelope

* **12 skipped executions:** DSK-019 (cold power loss) and DSK-020 (filesystem/device matrix) require a supported
  hardware power-cut rig and multiple filesystems; two MESH integration rows likewise need real hardware. Status: *not
  run* — never counted as passes.
* **Platforms:** the campaign ran on Windows 11 only. Linux/macOS (SDK-015 parity, `libmsquic`) are untested here.
* **Performance:** PERF-001…016 measure resource envelopes (bounded memory, amplification, backpressure, latency
  separation of buffer acceptance vs durable commit) on one developer machine with loopback networking. They are
  regression baselines, not production performance claims (FSD §15).
* **Beyond the durability envelope** (losing every copy and every backup) is tested only for honest failure reporting.

## 6. Open defects and limitations

| Item | Status |
|---|---|
| NET-013 slow-leader-node: reply/connect timeouts do not adapt to a sudden 400 ms one-way latency, so lease-granting calls end as outcome-unknown | Fix in progress (RTT-adaptive timeouts) |
| SEC-018: under a flood of denied admin attempts the audit rate limiter drops some denial entries | Fix in progress (no silent audit loss) |
| Old data-key versions are never retired; node-local key files keep keys of deleted volumes | Known limitation |
| Object encoding 2 (per-volume keys) has no minimum-reader marker, so an older build would refuse rather than be blocked up front (FMT-006 intent) | Known limitation |
| A node restarted on an empty replacement disk is fenced as a stale incarnation; it must be removed and re-added under a new node id | By design for now; documented in OPERATIONS §6 |
| A removed volume voter that never learns of its removal keeps an idle Raft group | Known limitation |
| Partial link meshes: nodes with no link to the control leader cannot register (no message relaying) | By design; tested as such |
| One flaky internal test under heavy CPU load (`RuntimeSimulationTests …WithoutThreadPoolEscapes`) | Timing-dependent trace line; passes in isolation |

## 7. How to reproduce

```bash
dotnet build Tedd.Tdfs.slnx
```

```bash
dotnet test --project tests/Tedd.Tdfs.Tests.Simulation -- --output Detailed
```

Set `TDFS_TEST_ROOT` to a scratch directory. Failing deterministic scenarios print their seed
(`TestClusterOptions.Seed = 0x…`) and data root for exact replay.
