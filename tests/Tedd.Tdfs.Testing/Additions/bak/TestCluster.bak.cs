namespace Tedd.Tdfs.Testing;

// Harness additions requested by the BAK suite (docs/api-additions/bak.md).
public sealed partial class TestCluster
{
    /// <summary>
    /// Creates a step-by-step driver of the late-sync export protocol for the existing live snapshot
    /// <paramref name="snapshotName"/> of <paramref name="volume"/> towards backup node <paramref name="backupIndex"/>,
    /// acting exactly like the volume leader's exporter (same wire messages, credentials, encryption and lineage) but
    /// letting the test choose the order of manifest, objects and publication and inject transit tampering.
    /// <para>
    /// Preconditions: the backup node is registered as a target of the volume
    /// (<see cref="TestBackupNode.RegisterAsync"/>) and the snapshot exists on the source. On creation the driver
    /// acquires a source transfer pin for the snapshot (logged like an exporter pin) and suspends the automatic
    /// exporter for this (volume, target) pair so the two cannot interleave; <see cref="ManualBackupExport.DisposeAsync"/>
    /// releases both. The driver continues the target's current lineage (incremental) unless the volume's backup
    /// progress marks the target for reseed, in which case it starts a new lineage. A successful
    /// <see cref="ManualBackupExport.PublishAsync"/> records backup progress on the source exactly like the automatic
    /// exporter (so later automatic syncs are incremental from it).
    /// </para>
    /// Used by BAK-006, BAK-007, BAK-009, BAK-010.
    /// </summary>
    public async Task<ManualBackupExport> BeginManualBackupExportAsync(string volume, string snapshotName, int backupIndex = 0, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(volume);
        ArgumentException.ThrowIfNullOrWhiteSpace(snapshotName);
        TestBackupNode backup = BackupAt(backupIndex);
        TestNode leader = await GetLeaderAsync(volume, ct);
        Server.NodeHost host = leader.Host
            ?? throw new InvalidOperationException($"The leader {leader.Name} of volume {volume} runs out of process; manual exports need an in-process leader.");
        Server.Backup.ManualBackupTransfer transfer = await host.InvokeAsync(async services =>
        {
            Server.Volumes.Service.VolumeService service = services.Volumes.TryGet(volume)?.Service is { IsLeader: true } led
                ? led
                : throw new DfsUnavailableException(DfsErrorCode.NotLeader, $"{leader.Name} no longer leads volume {volume}.");
            return await Server.Backup.ManualBackupTransfer.BeginAsync(services, service, backup.TargetName, snapshotName);
        });
        Record($"manual export {volume}/{snapshotName} -> {backup.TargetName}: pinned on {leader.Name} (lineage {transfer.Lineage})");
        return new ManualBackupExport(this, host, transfer, ForkRandom("manual-export:" + volume + ":" + snapshotName));
    }

    /// <summary>
    /// Connects a <see cref="RawProtocolClient"/> to backup node <paramref name="backupIndex"/> (network identity
    /// "client:raw-b{index}"). The client is not authenticated until <see cref="RawProtocolClient.AuthenticateAsync"/>
    /// is called with credentials known to the backup node. All <see cref="RawProtocolClient"/> members behave as for a
    /// live node, so mutating path operations sent this way exercise the backup node's own server-side read-only
    /// enforcement without any SDK-side guard. Used by BAK-002, BAK-015.
    /// </summary>
    public Task<RawProtocolClient> ConnectRawBackupAsync(int backupIndex = 0, CancellationToken ct = default)
    {
        TestBackupNode backup = BackupAt(backupIndex);
        return RawProtocolClient.ConnectAsync(this, "client:raw-b" + backupIndex.ToString(System.Globalization.CultureInfo.InvariantCulture), backup.Name,
            backup.Endpoint, expectedClusterId: Guid.Empty, ct);
    }
}
