# Tedd.TDFS — Operations guide

How to deploy, administer, back up and recover a Tedd.TDFS cluster. Behaviour referenced here is specified in the
[FSD](spec/distributed-filesystem-fsd.md) and [addendum](spec/fsd-addendum-acl-pools-streams.md) and designed in
[ARCHITECTURE.md](ARCHITECTURE.md).

## Configuring a live export

After upgrading the cluster and its backup readers, activate the export format once with
`await client.Admin.SetFeatureAsync(DfsExportRoot.ClusterFeature, true)`. Activation checks member capabilities
and permanently requires servers that understand the format. Activate it on a recovery cluster before restoring a snapshot containing export metadata. The per-volume profile remains independently configurable.

The default volume profile is `Disabled`. An administrator can enable `CaseSensitive` for scoped handles and
shared writers, or `Windows` for those contracts with ordinal case-insensitive, case-preserving names.
Close live handles and finish active
transactions before changing the profile. The change traverses the live inode tree in one replicated decision.
If directory entries or named streams collide under the selected comparison, activation fails atomically.
Rename the colliding entries in the current profile and retry. Existing snapshots retain their captured namespace.

Register each mapping as a cluster administrator. A nested registered directory starts a separate mapping. Switching an active profile to Disabled clears its registrations.
Store the cluster, volume incarnation and directory identity returned by `BindExportAsync` with the export.
Volume grants and inode ACLs authorize each request. The gateway selects the registered root for its export.
Reuse this mapping after gateway restarts. Rebind explicitly when exporting a replacement directory or a restored
volume. Gateways and pods using TDFS must submit opens and namespace changes to the volume service so they
participate in the same reservation table.

## 1. Components

| Process | Command | Role |
|---|---|---|
| Live node | `Tedd.Tdfs.Host node --config node.json` | Stores data, is a member (voter or learner) of the control group, hosts volume Raft groups. |
| Backup node | `Tedd.Tdfs.Host backup --config backup.json` | Late-sync, read-only target. Never votes, never holds live replicas or locks. |
| Clients | `DfsClient` (SDK) | Applications and administration (`DfsAdminClient`). |

A host prints one `TDFS-READY {json}` line on standard output when it serves (or `TDFS-FAILED {json}`), logs to
standard error (`--log trace|debug|info|warn|error`), accepts `stop`, `pause` and `resume` on standard input, and stops
gracefully on Ctrl+C / SIGTERM. Exit codes: `0` ok, `1` usage, `2` configuration error, `3` start failed,
`4` recovery key missing.

Requirements: .NET 10 runtime, 64-bit, MsQuic (bundled on Windows 11 / Server 2022+; install `libmsquic` ≥ 2.4 on
Linux/macOS). Every node needs one UDP port.

## 2. Planning

* **Zones.** Give every node accurate topology labels (`region`, `zone`, `rack`, `host`, `device`). Copies are counted
  by distinct `host`, zones by distinct `(region, zone)`; two nodes on one physical machine must share a `host` label so
  they are not counted twice.
* **Voters.** The control group and each volume use 3 voters by default (5 for wider fault tolerance), placed one per
  zone. A majority of the *configured* voters must be up for that group to accept writes; with 3 voters in 2 zones
  (2+1), losing the 2-voter zone stops writes.
* **Durability policy.** `TargetCopies`/`TargetDistinctZones` are the goal; `DurableCopiesBeforeAck`/
  `DurableZonesBeforeAck` must be met before a write returns. Requiring all copies means one unavailable target blocks
  writes; the cluster never silently weakens a policy.
* **Capacity.** `capacityBytes` caps object storage per node; `reservedBytes` (default 64 MiB) is kept for journals,
  compaction and repair. New uploads are refused with `CapacityExhausted` before the reserve is touched.

## 3. Bootstrapping a cluster

On the first node:

```bash
Tedd.Tdfs.Host bootstrap --name n0 --listen quic://10.0.0.11:7100 --storage /var/lib/tdfs/n0 --out /etc/tdfs/n0.json --zone z1 --cluster-name prod
```

This writes `n0.json` and a certificate `n0.pfx`, and prints the cluster id, the certificate pin (SHA-256 of the
public key) and the generated `admin` secret (pass `--admin-secret` to choose it). **Store the admin secret and the
cluster secret (inside `n0.json`) securely.** Start the node:

```bash
Tedd.Tdfs.Host node --config /etc/tdfs/n0.json
```

### 3.1 Adding nodes

Every node of a cluster shares the cluster secret. The simplest certificate setup is to copy the same `.pfx` to every
node so a single pin is trusted; alternatively give each node its own certificate and list all pins in `trustedPins`.
A joining node's configuration (`n1.json`):

```json
{
  "name": "n1",
  "labels": { "region": "r1", "zone": "z2", "rack": "rk1", "host": "n1", "device": "d1" },
  "listenEndpoint": "quic://10.0.0.12:7100",
  "storageRoot": "/var/lib/tdfs/n1",
  "clusterSecret": "<same base64 secret as n0.json>",
  "joinSeeds": [ "quic://10.0.0.11:7100" ],
  "certificatePath": "/etc/tdfs/cluster.pfx",
  "trustedPins": [ "<pin printed by bootstrap>" ],
  "timingsPreset": "Production"
}
```

The node contacts a seed, is added to the control group as a learner, catches up, and the control leader promotes
learners to voters (one change at a time) until the configured voter count is reached. Any subset of members works as
seeds. A node restarted later needs no seeds: it reads its identity from its superblock (incarnation is bumped on every
start) and reconnects to known members.

Static bootstrap (all initial members known up front) is also supported: give every node an identical `bootstrap`
section listing all members and the control voters.

## 4. Administration

All administration goes through `DfsAdminClient` (`client.Admin`) and requires the `ClusterAdmin` permission unless
noted.

```csharp
await using var client = await DfsClient.ConnectAsync(new DfsClientOptions
{
    SeedEndpoints = ["quic://10.0.0.11:7100"],
    Credentials = new DfsCredentials("admin", adminSecret),
    Security = new DfsTransportSecurity { PinnedSpkiSha256 = [Convert.FromHexString(pin)] },
});
DfsAdminClient admin = client.Admin;

await admin.CreatePoolAsync("ssd", ["n0", "n1", "n2"]);
await admin.CreateVolumeAsync(new DfsVolumeSpec
{
    Name = "application",
    StoragePool = "ssd",
    DefaultPolicy = new DfsStoragePolicy { TargetCopies = 3, TargetDistinctZones = 3, DurableCopiesBeforeAck = 2, DurableZonesBeforeAck = 2 },
});
await admin.CreatePrincipalAsync("app", appSecret);
await admin.CreateGroupAsync("engineering");
await admin.AddGroupMemberAsync("engineering", "app");
await admin.GrantAsync("application", "app", DfsPermissions.Read | DfsPermissions.Write | DfsPermissions.Snapshot);
```

* **Volumes are separate namespaces** with their own Raft group, authorization, transactions and snapshots.
* **Permissions** are two layers: the volume grant (`DfsPermissions`) and the inode's NTFS-style ACL
  (`GetAccessControlAsync`/`SetAccessControlAsync`/`SetOwnerAsync`). Both must allow an operation. New volumes default
  to `Everyone: FullControl` inherited from `/`, so the volume grant decides until you set restrictive ACLs.
* **Policies**: `DefinePolicyAsync(volume, name, policy)` adds a named policy (optionally bound to another pool);
  `SetDefaultPolicyAsync(volume, policy, migrateExisting: true)` upgrades existing data — the new policy becomes
  effective only after migration completes (`GetPolicyMigrationStatusAsync`).
* **Audit**: `GetAuditLogAsync` lists security and administrative events (principals, outcomes, scope — never data or
  secrets).

## 5. Monitoring

| Call | Shows |
|---|---|
| `GetClusterStatusAsync` | control quorum and leader, nodes, placement epoch, active features |
| `GetVolumeStatusAsync(volume)` | volume group (voters, leader, commit/applied index, quorum), replica health, backup status |
| `GetReplicaHealthAsync(volume)` | under-replicated objects, zone deficits, missing objects, placement violations, repair backlog, pending GC, staged/pinned bytes |
| `GetPoolStatusAsync(pool)` | pool nodes, volumes, capacity, usage, replica deficits, draining nodes |
| `volume.Backups.GetStatusAsync(target)` | received bytes vs durable vs published boundary, recovery-point age, reseed flag |

Nodes also publish `System.Diagnostics.Metrics` under the meter `Tedd.Tdfs` (objects/bytes stored, receipts, Raft
entries, elections, lease expiries, transaction conflicts, checksum failures, GC and repair bytes). Alert on lost quorum,
a growing repair backlog, non-zero missing objects, checksum failures and backup recovery-point age — not merely on
connectivity.

## 6. Maintenance

* **Drain** a node before removing it: `DrainNodeAsync(name)` migrates its replicas and voter seats (copy-before-delete),
  then `RemoveNodeAsync(name)`. A removal that would drop the last copy or an unsafe voter combination is refused unless
  `force: true` (destructive recovery only).
* **Replace a failed disk**: a node restarted with an empty disk is fenced as a stale incarnation; remove it and re-add
  it under a new node id (it is then repopulated by repair).
* **Repair and scrub** run continuously and rate-limited; trigger them with `StartRepairAsync(volume)` and
  `StartScrubAsync(node)`. Corrupt replicas are quarantined and stop counting as copies.
* **Pools**: `SetPoolMembersAsync` changes membership; objects leave removed nodes through repair. A pool in use cannot
  be deleted.
* **Keys**: `RotateDataKeyAsync(volume)` starts a new data-key version for new writes; old versions are retained while
  referenced. `ExportRecoveryKeyAsync(passphrase)` returns the KEK escrow — store it offline; it is required to recover
  encrypted data if every node's configuration is lost.
* **Upgrades**: nodes negotiate protocol features; new features activate only when every member supports them. Upgrade
  one node at a time and wait for `GetClusterStatusAsync` to show the control and volume groups healthy before the next.

## 7. Snapshots

```csharp
DfsVolume volume = client.GetVolume("application");
await volume.Snapshots.CreateAsync("daily-2026-10-16", new DfsSnapshotOptions { Retention = new DfsRetention(DateTimeOffset.UtcNow.AddDays(30)) });
CommitToken t = await tx.CommitAsync();                                      // or: exact commit + snapshot in one decision:
DfsSnapshotInfo exact = await tx2.CommitWithSnapshotAsync("before-migration");
await volume.Snapshots.RestoreAsync("daily-2026-10-16", "application-restored");   // always into a NEW volume
```

Snapshots are immutable, capture one commit boundary, and cost no payload copy. Use `DfsSnapshotOptions.Adapters` for
application-consistent snapshots (prepare → capture → resume); without adapters a snapshot is labelled
`FilesystemConsistent`.

## 8. Backup hosts

A backup host's configuration (`backup.json`):

```json
{
  "name": "b0",
  "listenEndpoint": "quic://10.0.1.20:7200",
  "storageRoot": "/var/lib/tdfs-backup/b0",
  "sourceClusterId": "<cluster id>",
  "backupSecret": "<base64, 32 random bytes — the backup's own key root; keep a copy offline>",
  "principals": [
    { "name": "exporter", "secret": "<secret>", "clusterAdmin": false },
    { "name": "reader", "secret": "<secret>", "clusterAdmin": false },
    { "name": "retention-admin", "secret": "<secret>", "clusterAdmin": false }
  ],
  "grants": { "exporter": "BackupExport", "reader": "Read", "retention-admin": "RetentionAdmin" },
  "keepLastSnapshots": 30,
  "certificatePath": "/etc/tdfs/backup.pfx",
  "trustedPins": [ "<cluster pin>" ]
}
```

Register it on a live volume and wait for recovery points:

```csharp
await volume.Backups.RegisterTargetAsync(new DfsBackupTargetRegistration("b0", ["quic://10.0.1.20:7200"],
    new DfsCredentials("exporter", exporterSecret), new DfsBackupSchedule(Interval: TimeSpan.FromHours(1)),
    new DfsBackupBudgets(MaxPinAge: TimeSpan.FromDays(2), MaxPinnedBytes: 500L << 30)));

CommitToken token = await volume.Files.WriteAllTextAsync("/important.json", json);
DfsBackupCheckpoint atLeast = await volume.Backups.AwaitBackupAsync("b0", token, TimeSpan.FromMinutes(30));          // a later checkpoint covering token
DfsBackupCheckpoint exactly = await volume.Backups.AwaitBackupSnapshotAsync("b0", "before-migration", TimeSpan.FromHours(1)); // that exact snapshot retained
```

* The export credential can transfer data but cannot delete or shorten retained history.
* Normal writes never wait for backups; an offline target that exceeds its pin budget is abandoned safely and reseeded
  later while it keeps its last complete snapshots.
* Read a backup directly by connecting a `DfsClient` to the backup endpoint with the reader credential:
  `GetVolume(name).Snapshots.ListAsync()/OpenAsync(...)`. Every mutation is rejected (`DfsReadOnlyException`).

## 9. Disaster recovery runbook (source cluster lost)

1. **Prepare a clean host**: install the runtime and `Tedd.Tdfs.Host`; have the backup host's configuration (or its
   `backupSecret`) and the backup's reader credentials available. The backup host must be running and reachable.
2. **Initialise a new cluster** on the clean host:
   `Tedd.Tdfs.Host bootstrap --name r0 --listen quic://<ip>:7100 --storage <dir> --out r0.json`, start it, and add
   further nodes as in §3.1.
3. **Restore** from the backup into a new volume (new identity and epoch):

   ```csharp
   await recoveredClient.Admin.RestoreFromBackupAsync(new DfsRestoreFromBackupRequest(
       BackupEndpoints: ["quic://10.0.1.20:7200"], BackupCredentials: new DfsCredentials("reader", readerSecret),
       SourceVolume: "application", SnapshotName: "<latest published snapshot>", NewVolumeName: "application",
       RecoveryKey: recoveryKeyOrNull));
   ```

   Missing key material fails with `DfsRecoveryKeyMissingException`; it never produces a partially readable volume.
4. **Verify** the recovery point: `GetStatusAsync`/`ListAsync` on the backup show the snapshot's covered commit; writes
   after that boundary that never reached the backup are not recoverable and must be reported as lost.
5. **Re-establish backups** for the new cluster (register a backup target; the new lineage starts with a full transfer).
6. **Fence the old cluster**: tokens, sessions and handles of the old authority are rejected by the restored volume; if
   the old source comes back it must not be allowed to write to the same clients (decommission it or isolate it).

A backup is never promoted automatically; restoring is always an explicit administrative act.

## 10. Troubleshooting

| Symptom | Likely cause |
|---|---|
| `DfsQuorumUnavailableException` | fewer than a majority of the group's configured voters reachable |
| `DfsPlacementUnavailableException` (required vs achieved copies/zones) | not enough healthy nodes/zones in the volume's pool for the ack policy |
| `DfsCapacityException` | pool nodes at capacity minus reserve; add capacity or delete data (GC reclaims after pins release) |
| `DfsLeaseLostException` | a writer's lease expired (paused or partitioned client); reopen the file |
| `DfsCommitOutcomeUnknownException` | reply lost after a possible commit; resolve with `QueryOperationAsync(id, resolve: true)` |
| `StaleIncarnation` in logs | cloned disk or replaced node identity; see §6 |
| Node exits with code 4 | recovery/backup key material missing from the configuration |
