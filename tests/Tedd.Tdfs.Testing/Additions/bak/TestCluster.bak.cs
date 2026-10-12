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
    public Task<ManualBackupExport> BeginManualBackupExportAsync(string volume, string snapshotName, int backupIndex = 0, CancellationToken ct = default) =>
        throw Stub.NotYet("TestCluster.BeginManualBackupExportAsync");

    /// <summary>
    /// Connects a <see cref="RawProtocolClient"/> to backup node <paramref name="backupIndex"/> (network identity
    /// "client:raw-b{index}"). The client is not authenticated until <see cref="RawProtocolClient.AuthenticateAsync"/>
    /// is called with credentials known to the backup node. All <see cref="RawProtocolClient"/> members behave as for a
    /// live node, so mutating path operations sent this way exercise the backup node's own server-side read-only
    /// enforcement without any SDK-side guard. Used by BAK-002, BAK-015.
    /// </summary>
    public Task<RawProtocolClient> ConnectRawBackupAsync(int backupIndex = 0, CancellationToken ct = default) =>
        throw Stub.NotYet("TestCluster.ConnectRawBackupAsync");
}
