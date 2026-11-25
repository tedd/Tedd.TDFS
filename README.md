# Tedd.TDFS

Tedd.TDFS is a replicated filesystem for .NET applications. Connect to a cluster over QUIC, open a named volume, and read or write paths through a C# client. Files open as ordinary `System.IO.Stream` instances. Each volume has its own namespace, permissions, transactions, and snapshots.

[Project website](https://tedd.no/Tedd.TDFS/) · [API reference](docs/API.md) · [Operations guide](docs/OPERATIONS.md)

## How a write becomes durable

For a file stored outside metadata, the client first stages immutable data on storage nodes. The volume's policy sets how many distinct hosts and zones must acknowledge durable copies. The volume's Raft group then commits the metadata revision that makes those bytes visible. Small files can be stored inline in the metadata revision.

The default storage policy targets three copies in three zones and requires two copies in two zones before acknowledgement. Background repair fills the remaining target. A metadata quorum is also required. Stream writes stay private until a flush, explicit commit, or successful disposal. Use a transaction to publish changes to several files together.

## Run a local node

Use a .NET SDK compatible with [global.json](global.json) (10.0.401 or a later .NET 10 feature band or patch) and a host with MsQuic support. The example uses an available UDP port 7100 on loopback. Start from a fresh checkout in a directory where you can create a private sibling directory named `tdfs-demo`.

```sh
git clone https://github.com/tedd/Tedd.TDFS.git
cd Tedd.TDFS
dotnet build src/Tedd.Tdfs.Host -c Release
mkdir ../tdfs-demo
dotnet src/Tedd.Tdfs.Host/bin/Release/net10.0/Tedd.Tdfs.Host.dll bootstrap --name n0 --listen quic://127.0.0.1:7100 --storage ../tdfs-demo/data --out ../tdfs-demo/node.json > ../tdfs-demo/bootstrap.json
dotnet src/Tedd.Tdfs.Host/bin/Release/net10.0/Tedd.Tdfs.Host.dll node --config ../tdfs-demo/node.json
```

Wait for `TDFS-READY` and leave the host running. Bootstrap creates the node configuration, development certificate, and a summary containing the generated admin secret and certificate pin. Keep the demo directory private. To restart this node, repeat only the last command.

## Write and read a file

In a second terminal, start in the repository root. Create a console application and reference the client SDK from the checkout.

```sh
dotnet new console -f net10.0 -o ../tdfs-demo/client
dotnet add ../tdfs-demo/client/client.csproj reference src/Tedd.Tdfs.Client/Tedd.Tdfs.Client.csproj
```

Replace the demo client's `Program.cs` with the following code. It uses the bootstrap identity to provision a volume with one voter and one data copy, suitable for a single development node.

```csharp
using System.Text.Json;
using Tedd.Tdfs;

using var bootstrap = JsonDocument.Parse(File.ReadAllText("../bootstrap.json"));
var settings = bootstrap.RootElement;
await using var client = await DfsClient.ConnectAsync(new DfsClientOptions
{
    SeedEndpoints = ["quic://127.0.0.1:7100"],
    ExpectedClusterId = settings.GetProperty("clusterId").GetGuid(),
    Credentials = new DfsCredentials("admin", settings.GetProperty("adminSecret").GetString()!),
    Security = new DfsTransportSecurity
    {
        PinnedSpkiSha256 = [Convert.FromHexString(settings.GetProperty("certificatePin").GetString()!)],
    },
});

if (!(await client.Admin.ListVolumesAsync()).Contains("demo"))
{
    await client.Admin.CreateVolumeAsync(new DfsVolumeSpec
    {
        Name = "demo",
        Metadata = new DfsMetadataPolicy { Voters = 1 },
        DefaultPolicy = new DfsStoragePolicy
        {
            TargetCopies = 1, TargetDistinctZones = 1,
            DurableCopiesBeforeAck = 1, DurableZonesBeforeAck = 1,
        },
    });
}

var volume = client.GetVolume("demo");
await volume.Files.WriteAllTextAsync("/hello.txt", "Hello from TDFS");
Console.WriteLine(await volume.Files.ReadAllTextAsync("/hello.txt"));
```

```sh
cd ../tdfs-demo/client
dotnet run
```

The client prints `Hello from TDFS`. Stop the host with Ctrl+C. For a replicated deployment, join nodes in distinct failure domains and give each application a principal with the volume permissions it needs. The [operations guide](docs/OPERATIONS.md) covers membership, grants, storage pools, and recovery.

## Working with volumes

- **Files and directories.** Open files with .NET access and sharing modes, enumerate a version-pinned directory, or address an alternate data stream as `/path/file:stream`. Volume grants and NTFS-style access control lists govern access.
- **Transactions.** Use `volume.Transactions.BeginAsync()` to group reads and writes within a volume. A successful commit publishes all changes at one strictly serializable boundary.
- **Snapshots and backups.** Open a snapshot to read an immutable version of the volume. Use `CommitWithSnapshotAsync` to capture a transaction's exact commit. An independent backup host can receive snapshots incrementally and serve read-only recovery points.
- **Lost replies.** A `DfsCommitOutcomeUnknownException` carries the operation ID. Call `volume.QueryOperationAsync(id, resolve: true)` to determine the outcome before retrying a mutation.

## Repository layout

| Project under `src/` | Responsibility |
| --- | --- |
| `Tedd.Tdfs.Client` | Application and administration APIs |
| `Tedd.Tdfs.Host` | Live node and backup host executable |
| `Tedd.Tdfs.Server` | Cluster membership, volume state, placement, repair, and backups |
| `Tedd.Tdfs.Consensus` | Raft elections, replication, and snapshots |
| `Tedd.Tdfs.Storage` | Journals, object storage, checkpoints, and encryption |
| `Tedd.Tdfs.Transport` | QUIC and in-memory RPC transports |
| `Tedd.Tdfs.Core` | Shared types, access rules, wire codec, and protocol messages |

## Build and test

From the repository root, build the solution and run the unit suite.

```sh
dotnet build Tedd.Tdfs.slnx
dotnet test --project tests/Tedd.Tdfs.Tests.Unit
```

The `tests/` directory also contains integration, deterministic simulation, model, and performance suites. Set `TDFS_TEST_ROOT` to select the directory for test cluster data.

## Reference

- [Architecture](docs/ARCHITECTURE.md), [wire protocol](docs/PROTOCOL.md), and [implementation internals](docs/INTERNALS.md)
- [Filesystem specification](docs/spec/distributed-filesystem-fsd.md) and [ACL, pool, and stream semantics](docs/spec/fsd-addendum-acl-pools-streams.md)
- [Test plan](docs/spec/distributed-filesystem-test-plan.md)
- [Apache License 2.0](LICENSE)
