# Tedd.TDFS

Tedd's Distributed File System: a self-contained, replicated, strongly consistent filesystem for .NET, used directly
from a client library. There is no mount, FUSE driver or external coordination service; applications use familiar file
operations and ordinary `System.IO.Stream` consumers.

* **Strong consistency.** Every volume's metadata is a Raft group; reads are linearizable, writes are durable when they
  return, and explicit transactions are strictly serializable.
* **Replicated, zone-aware storage.** Immutable data extents placed by deterministic rendezvous hashing across failure
  domains, with separate *target* and *acknowledged* copy counts, repair, rebalance, scrubbing and safe garbage
  collection.
* **Familiar API.** `DfsStream : Stream`, `ReadAllText`/`WriteAllBytes`/`Copy`/`Move`/`Delete`, version-pinned
  directory enumeration, `FileMode`/`FileAccess`/`FileShare` semantics with cluster-enforced sharing and writer fencing.
* **NTFS-style permissions.** Owners, allow/deny ACEs for users and groups, inheritance flags, canonical evaluation order.
* **Alternate data streams.** `"/dir/file.txt:stream"`, removed together with their file.
* **Volumes and storage pools.** Volumes are separate namespaces; pools pin a volume's (or a policy's) data to a set of
  nodes.
* **Snapshots and late-sync backups.** O(1) immutable snapshots, commit-and-snapshot, application-consistent snapshot
  adapters, and independent read-only backup hosts that receive complete snapshots incrementally.
* **Secure by default.** QUIC (TLS 1.3) transport via [Tedd.Quicly](https://www.nuget.org/packages/Tedd.Quicly.Core),
  challenge-response authentication, server-side authorization on every path, encryption at rest.

Requirements and design: [FSD](docs/spec/distributed-filesystem-fsd.md), [addendum](docs/spec/fsd-addendum-acl-pools-streams.md),
[test plan](docs/spec/distributed-filesystem-test-plan.md), [architecture](docs/ARCHITECTURE.md),
[protocol](docs/PROTOCOL.md), [API contract](docs/API.md), [internals](docs/INTERNALS.md),
[operations guide](docs/OPERATIONS.md).

## Quick start (client)

```csharp
using Tedd.Tdfs;

await using var client = await DfsClient.ConnectAsync(new DfsClientOptions
{
    SeedEndpoints = ["quic://10.0.0.11:7100", "quic://10.0.0.12:7100"],
    Credentials = new DfsCredentials("app", secret),
    Security = new DfsTransportSecurity { PinnedSpkiSha256 = [clusterCertificatePin] },
}, ct);

DfsVolume volume = client.GetVolume("application");
await volume.Directories.CreateDirectoryAsync("/documents", ct);
await volume.Files.WriteAllTextAsync("/documents/settings.json", "{\"enabled\":true}", ct);

await using (Stream input = await volume.Files.OpenReadAsync("/documents/settings.json", ct))
using (var reader = new StreamReader(input))
    Console.WriteLine(await reader.ReadToEndAsync(ct));

// Atomic multi-file update
await using (DfsTransaction tx = await volume.Transactions.BeginAsync(ct: ct))
{
    await tx.Files.WriteAllTextAsync("/state/balance.json", balanceJson, ct);
    await tx.Files.WriteAllTextAsync("/state/ledger.json", ledgerJson, ct);
    CommitToken committed = await tx.CommitAsync(ct);
}

// Snapshot, then read it
await volume.Snapshots.CreateAsync("nightly-2026-10-16", ct: ct);
await using DfsSnapshotView view = await volume.Snapshots.OpenAsync("nightly-2026-10-16", ct);
byte[] old = await view.Files.ReadAllBytesAsync("/documents/settings.json", ct);
```

Write semantics worth knowing (FSD §5.4, §6.3):

* Stream writes are private until `Flush`/`FlushAsync`, `CommitAsync` or a successful `Dispose`; a flush publishes one
  complete revision and returns only after the volume's durability policy is met. `DfsWriteMode.WriteThrough` publishes
  every write call.
* `WriteAll*`, `Copy` and the `Append*` helpers are atomic and durable on return and preserve the destination's inode.
* `ExistsAsync` returns `false` only for genuine absence; permission, connectivity and integrity failures throw.
* A possibly-committed operation whose reply was lost surfaces as `DfsCommitOutcomeUnknownException` carrying its
  operation id; resolve it with `volume.QueryOperationAsync(id, resolve: true)`. It is never reported as a failure.

## Running a cluster

`src/Tedd.Tdfs.Host` builds the server executable (`Tedd.Tdfs.Host`). A three-node cluster in one minute:

```bash
dotnet build src/Tedd.Tdfs.Host -c Release
```

```bash
Tedd.Tdfs.Host bootstrap --name n0 --listen quic://10.0.0.11:7100 --storage /var/lib/tdfs/n0 --out /etc/tdfs/n0.json --zone z1
```

```bash
Tedd.Tdfs.Host node --config /etc/tdfs/n0.json
```

`bootstrap` prints the new cluster id, the certificate pin and the generated `admin` secret. Add more nodes with
`joinSeeds`, create volumes and principals with `DfsAdminClient`, and register backup hosts — see
[docs/OPERATIONS.md](docs/OPERATIONS.md).

## Repository layout

| Path | Contents |
|---|---|
| `src/Tedd.Tdfs.Core` | Identifiers, paths, errors, policies, ACL evaluator, wire codec, protocol messages |
| `src/Tedd.Tdfs.Transport` | RPC transport: Tedd.Quicly (QUIC) and in-memory |
| `src/Tedd.Tdfs.Client` | The SDK (`DfsClient`, `DfsStream`, transactions, snapshots, backups, admin) |
| `src/Tedd.Tdfs.Storage` | Local storage engine: journals, packed object segments, checkpoints, encryption |
| `src/Tedd.Tdfs.Consensus` | Pure Raft core |
| `src/Tedd.Tdfs.Server` | Node runtime, control and volume state machines, data plane, maintenance, backup |
| `src/Tedd.Tdfs.Host` | Server executable |
| `tests/Tedd.Tdfs.Testing` | Test harness: in-process and deterministic clusters, fault injection, checkers |
| `tests/Tedd.Tdfs.Tests.*` | Unit, Simulation, Integration, Models and Performance suites |

## Building and testing

```bash
dotnet build Tedd.Tdfs.slnx
```

```bash
dotnet test --project tests/Tedd.Tdfs.Tests.Unit
```

The acceptance suites implement every named case of the [test plan](docs/spec/distributed-filesystem-test-plan.md)
(586) and the addendum (78), plus a 2–10 node mesh matrix (`MESH-*`). Tests run in-process clusters over an in-memory
transport, a deterministic simulator (seeded, replayable), or real QUIC and disks. Set `TDFS_TEST_ROOT` to choose where
test clusters keep their data. The `Simulation` and `Models` projects take tens of minutes; `Performance` tests print
their measurements.

## License

See [LICENSE](LICENSE).
