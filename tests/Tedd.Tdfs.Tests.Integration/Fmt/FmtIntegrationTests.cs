using System.Text;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Integration.Fmt;

/// <summary>
/// FMT suite, level I: disk images, downgrade protection and backup restore across the supported release window, with real
/// QUIC transport and on-disk storage (FSD §§12, 15, 17; test plan §7.25).
/// </summary>
public sealed class FmtIntegrationTests
{
    private const string Vol = FmtHelpers.Volume;

    private static CancellationToken Ct => FmtHelpers.Ct;

    // ============================================================================================================
    // FMT-005 OldDiskImageOpensSupportedVersion
    // ============================================================================================================

    /// <summary>
    /// For every release of the supported window (oldest to current), an on-disk image written by that release
    /// (directories, inline/multi-chunk/sparse/overwritten/appended/renamed/deleted files and a snapshot followed by further
    /// changes), shut down gracefully or by crash, is opened by the current release: each node's format is either read in
    /// place (unchanged) or migrated to the current format — never lower, never unreadable — and the volume serves exactly
    /// the same bytes, inode ids, revisions, absences and snapshot contents, and accepts new writes.
    /// </summary>
    [Theory, Catalog("FMT-005", "I", "INV-01", "INV-06", "INV-08", "INV-10", "INV-18")]
    [InlineData("graceful")]
    [InlineData("crash")]
    public async Task FMT_005_OldDiskImageOpensSupportedVersion(string shutdown)
    {
        SoftwareVersionProfile current = SoftwareVersionProfile.Current;
        IReadOnlyList<SoftwareVersionProfile> window = SoftwareVersionProfile.SupportedWindow;
        Assert.Equal(current.Name, window[^1].Name);
        foreach (SoftwareVersionProfile writer in window)
        {
            FmtHelpers.Log($"image written by {writer} ({shutdown} shutdown), opened by {current}");
            await VersionedClusterTest.RunAsync(FmtHelpers.QuicDisk(), writer, async cluster =>
            {
                Assert.All(cluster.Nodes, n => Assert.Equal(writer.Name, n.SoftwareVersion.Name));

                ReferenceImage image;
                await using (DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "writer" }, Ct))
                    image = await FmtHelpers.WriteReferenceImageAsync(client.GetVolume(Vol), "/fmt005");
                await cluster.WaitForReplicationAsync(Vol, Ct);

                var before = new Dictionary<string, DiskFormatInspection>(StringComparer.Ordinal);
                foreach (TestNode node in cluster.Nodes)
                    before[node.Name] = await node.Inspect.GetDiskFormatAsync();
                Assert.All(before.Values, f => Assert.Equal(writer.DiskFormatVersion, f.SuperblockVersion));

                foreach (TestNode node in cluster.Nodes)
                {
                    if (shutdown == "graceful")
                        await node.StopAsync();
                    else
                        await node.CrashAsync();
                }

                foreach (TestNode node in cluster.Nodes)
                    await node.RestartAsSoftwareVersionAsync(current);
                await cluster.WaitForStableAsync(TimeSpan.FromSeconds(90), Ct);

                foreach (TestNode node in cluster.Nodes)
                {
                    DiskFormatInspection after = await node.Inspect.GetDiskFormatAsync();
                    FmtHelpers.Log($"{node.Name}: {before[node.Name]} -> {after}");
                    bool readInPlace = after.SuperblockVersion == before[node.Name].SuperblockVersion;
                    bool migrated = after.SuperblockVersion == current.DiskFormatVersion;
                    Assert.True(readInPlace || migrated, $"{node.Name}: format {after.SuperblockVersion} is neither {writer.Name}'s nor the current format");
                    Assert.True(after.SuperblockVersion >= before[node.Name].SuperblockVersion, $"{node.Name}: format went backwards");
                    Assert.True(after.HighestComponentVersion <= current.MaxReadableDiskFormatVersion);
                    Assert.True(after.MinimumReaderVersion <= current.MaxReadableDiskFormatVersion);
                }

                await using DfsClient reader = await cluster.ConnectAsync(new TestClientOptions { Name = "reader" }, Ct);
                DfsVolume vol = reader.GetVolume(Vol);
                await FmtHelpers.VerifyReferenceImageAsync(vol, image, $"image written by {writer.Name} ({shutdown}) opened by {current.Name}");
                Assert.Single(await vol.Snapshots.ListAsync(Ct), s => s.Name == image.SnapshotName);
                byte[] fresh = TestData.Bytes(5099, 90_000);
                await vol.Files.WriteAllBytesAsync("/fmt005/written-after-upgrade", fresh, Ct);
                FmtHelpers.BytesEqual(fresh, await vol.Files.ReadAllBytesAsync("/fmt005/written-after-upgrade", Ct));
                await cluster.Inspect.AssertReplicasConvergedAsync(Vol);
            }, Ct);
        }
    }

    // ============================================================================================================
    // FMT-006 UnsupportedDowngradeBlocked
    // ============================================================================================================

    /// <summary>
    /// A node whose image was written by the current release is restarted as a release below the supported window: the
    /// start is refused with <see cref="DfsErrorCode.Unsupported"/>, the node stays stopped, and not a single byte of its
    /// image changes; the remaining majority keeps serving, and the node returns normally on the current release.
    /// </summary>
    [Fact, Catalog("FMT-006", "I", "INV-01", "INV-06", "INV-08", "INV-10", "INV-18")]
    public async Task FMT_006_UnsupportedDowngradeBlocked_ReleaseBelowWindow()
    {
        await ClusterTest.RunAsync(FmtHelpers.QuicDisk(), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            DfsVolume vol = client.GetVolume(Vol);
            ReferenceImage image = await FmtHelpers.WriteReferenceImageAsync(vol, "/fmt006a");
            await cluster.WaitForReplicationAsync(Vol, Ct);

            TestNode target = cluster.Nodes[2];
            await target.StopAsync();
            SortedDictionary<string, string> fingerprint = FmtHelpers.Fingerprint(target.Disk);
            Assert.NotEmpty(fingerprint);

            DfsException refusal = await AssertEx.ThrowsDfsAsync<DfsException>(
                () => target.RestartAsSoftwareVersionAsync(SoftwareVersionProfile.BelowSupportedWindow), DfsErrorCode.Unsupported);
            Assert.Equal(DfsErrorCode.Unsupported, refusal.ErrorCode);
            Assert.Equal(TestNodeState.Stopped, target.State);
            Assert.Equal(fingerprint, FmtHelpers.Fingerprint(target.Disk));

            byte[] during = TestData.Bytes(6001, 50_000);
            await vol.Files.WriteAllBytesAsync("/fmt006a/during", during, Ct);
            FmtHelpers.BytesEqual(during, await vol.Files.ReadAllBytesAsync("/fmt006a/during", Ct));

            await target.RestartAsSoftwareVersionAsync(SoftwareVersionProfile.Current);
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), Ct);
            await cluster.WaitForReplicationAsync(Vol, Ct);
            await cluster.Inspect.AssertReplicasConvergedAsync(Vol);
            await FmtHelpers.VerifyReferenceImageAsync(vol, image, "after the refused downgrade");
        }, Ct);
    }

    /// <summary>
    /// After an irreversible format feature is activated cluster-wide, every node durably records it and raises its
    /// minimum-reader marker above what the previous release can read; deactivation is refused; and restarting any node as
    /// the previous release is refused with <see cref="DfsErrorCode.Unsupported"/> without touching its image, after which it
    /// returns on the current release with all data intact.
    /// </summary>
    [Fact, Catalog("FMT-006", "I", "INV-01", "INV-06", "INV-08", "INV-10", "INV-18")]
    public async Task FMT_006_UnsupportedDowngradeBlocked()
    {
        await ClusterTest.RunAsync(FmtHelpers.QuicDisk(), async cluster =>
        {
            string feature = SoftwareVersionProfile.IrreversibleProbeFeature;
            SoftwareVersionProfile previous = SoftwareVersionProfile.Previous;
            Assert.Contains(feature, SoftwareVersionProfile.Current.IrreversibleFeatures);
            Assert.False(previous.Supports(feature));
            DfsAdminClient admin = cluster.Admin;

            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            DfsVolume vol = client.GetVolume(Vol);
            ReferenceImage image = await FmtHelpers.WriteReferenceImageAsync(vol, "/fmt006b");

            await admin.SetFeatureAsync(feature, true, Ct);
            await cluster.WaitUntilAsync(async () => (await admin.GetClusterStatusAsync(Ct)).ActiveFeatures.Contains(feature),
                TimeSpan.FromSeconds(30), "the irreversible feature must activate", Ct);
            await cluster.WaitUntilAsync(async () =>
            {
                foreach (TestNode node in cluster.Nodes)
                {
                    DiskFormatInspection format = await node.Inspect.GetDiskFormatAsync();
                    if (!format.ActivatedIrreversibleFeatures.Contains(feature) || format.MinimumReaderVersion <= previous.MaxReadableDiskFormatVersion)
                        return false;
                }

                return true;
            }, TimeSpan.FromSeconds(30), "every node must durably record the activation and raise its minimum-reader marker", Ct);

            DfsException deactivate = await AssertEx.ThrowsDfsAsync<DfsException>(() => admin.SetFeatureAsync(feature, false, Ct));
            Assert.Contains(deactivate.ErrorCode, new[] { DfsErrorCode.Unsupported, DfsErrorCode.InvalidArgument });
            Assert.Contains(feature, (await admin.GetClusterStatusAsync(Ct)).ActiveFeatures);

            foreach (TestNode node in cluster.Nodes.ToArray())
            {
                await node.StopAsync();
                SortedDictionary<string, string> fingerprint = FmtHelpers.Fingerprint(node.Disk);
                await AssertEx.ThrowsDfsAsync<DfsException>(() => node.RestartAsSoftwareVersionAsync(previous), DfsErrorCode.Unsupported);
                Assert.Equal(TestNodeState.Stopped, node.State);
                Assert.Equal(fingerprint, FmtHelpers.Fingerprint(node.Disk));
                await node.RestartAsSoftwareVersionAsync(SoftwareVersionProfile.Current);
                await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), Ct);
            }

            await FmtHelpers.VerifyReferenceImageAsync(vol, image, "after refused downgrades");
            Assert.Contains(feature, (await admin.GetClusterStatusAsync(Ct)).ActiveFeatures);
        }, Ct);
    }

    // ============================================================================================================
    // FMT-010 BackupRestorableAcrossSupportedVersions
    // ============================================================================================================

    /// <summary>
    /// For each (source release, destination release) pair of the supported window: a snapshot taken and exported to the
    /// backup host on the source release is restored — after every live node and the backup host moved to the destination
    /// release, using the escrowed recovery key — into a new volume that holds exactly the snapshot (not later overwrites),
    /// with a new identity and working writes; the restored data is encrypted at rest on every live node and on the backup
    /// host (no plaintext on any raw disk). A destination older than the source may instead refuse explicitly with
    /// <see cref="DfsErrorCode.Unsupported"/> (no volume created), but must never restore misread data.
    /// </summary>
    [Theory, Catalog("FMT-010", "I", "INV-01", "INV-06", "INV-08", "INV-10", "INV-18")]
    [InlineData("Previous", "Previous")]
    [InlineData("Previous", "Current")]
    [InlineData("Current", "Previous")]
    [InlineData("Current", "Current")]
    public async Task FMT_010_BackupRestorableAcrossSupportedVersions(string sourceProfile, string destinationProfile)
    {
        // The rows cover every pair only while the window is {Previous, Current}; a wider window must extend them.
        Assert.All(SoftwareVersionProfile.SupportedWindow, p => Assert.Contains(p.Name, new[] { "Previous", "Current" }));
        SoftwareVersionProfile source = SoftwareVersionProfile.Get(sourceProfile);
        SoftwareVersionProfile destination = SoftwareVersionProfile.Get(destinationProfile);
        bool downgrade = SoftwareVersionProfile.SupportedWindow.ToList().FindIndex(p => p.Name == destination.Name)
            < SoftwareVersionProfile.SupportedWindow.ToList().FindIndex(p => p.Name == source.Name);
        await VersionedClusterTest.RunAsync(FmtHelpers.QuicDisk(backupNodes: 1), source, async cluster =>
        {
            TestBackupNode b0 = cluster.BackupNodes[0];
            Assert.Equal(source.Name, b0.SoftwareVersion.Name);
            await b0.RegisterAsync(Vol);

            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            DfsVolume vol = client.GetVolume(Vol);
            VolumeId sourceId = (await vol.GetInfoAsync(Ct)).Id;
            byte[] marker = Encoding.ASCII.GetBytes("FMT010-PLAINTEXT-MARKER-0123456789");
            var snapshotFiles = new Dictionary<string, byte[]>(StringComparer.Ordinal)
            {
                ["/fmt010/inline"] = TestData.Bytes(7001, 300),
                ["/fmt010/extents"] = TestData.Bytes(7002, 150 * 1024),
                ["/fmt010/marker"] = Enumerable.Repeat(marker, 4096).SelectMany(m => m).ToArray(),
                ["/fmt010/dir/nested"] = TestData.Bytes(7003, 5000),
            };
            await vol.Directories.CreateDirectoryAsync("/fmt010/dir", Ct);
            foreach ((string path, byte[] bytes) in snapshotFiles)
                await vol.Files.WriteAllBytesAsync(path, bytes, Ct);

            DfsSnapshotInfo snapshot = await vol.Snapshots.CreateAsync("fmt010-src", null, Ct);
            DfsBackupCheckpoint checkpoint = await vol.Backups.AwaitBackupSnapshotAsync(b0.TargetName, "fmt010-src", TimeSpan.FromSeconds(120), Ct);
            Assert.True(checkpoint.IsExactSnapshot);
            Assert.Equal(snapshot.Boundary, checkpoint.CoveredCommit);
            byte[] recoveryKey = await cluster.Admin.ExportRecoveryKeyAsync("fmt010-passphrase", Ct);
            await vol.Files.WriteAllBytesAsync("/fmt010/extents", TestData.Bytes(7004, 10_000), Ct);

            // Moving to an older destination release may be refused explicitly (by a live node, the backup host or the restore
            // itself); moving to the same or a newer release must work.
            Exception? liveRestart = await Record.ExceptionAsync(() => FmtHelpers.RestartAllAsAsync(cluster, destination));
            Exception? backupRestart = liveRestart is null ? await Record.ExceptionAsync(() => b0.RestartAsSoftwareVersionAsync(destination)) : null;
            Exception? restoreFailure = null;
            DfsVolumeInfo? restoredInfo = null;
            if (liveRestart is null && backupRestart is null)
            {
                restoreFailure = await Record.ExceptionAsync(async () => restoredInfo = await cluster.Admin.RestoreFromBackupAsync(
                    new DfsRestoreFromBackupRequest([b0.Endpoint], b0.ReaderCredentials, Vol, "fmt010-src", "fmt010-restored", recoveryKey), Ct));
            }

            Exception? refusal = liveRestart ?? backupRestart ?? restoreFailure;
            if (refusal is not null)
            {
                Assert.True(downgrade, $"restoring a {source.Name} backup with {destination.Name} must succeed, but failed: {refusal}");
                Assert.Equal(DfsErrorCode.Unsupported, Assert.IsAssignableFrom<DfsException>(refusal).ErrorCode);
                if (restoreFailure is not null)
                    Assert.DoesNotContain("fmt010-restored", await cluster.Admin.ListVolumesAsync(Ct));
                FmtHelpers.Log($"{destination.Name} refused the {source.Name} image/backup explicitly: {refusal.Message}");
                return;
            }

            DfsVolumeInfo restored = restoredInfo!;
            Assert.Equal("fmt010-restored", restored.Name);
            Assert.NotEqual(sourceId, restored.Id);

            DfsVolume restoredVol = client.GetVolume("fmt010-restored");
            foreach ((string path, byte[] bytes) in snapshotFiles)
                FmtHelpers.BytesEqual(bytes, await restoredVol.Files.ReadAllBytesAsync(path, Ct), $"{path} restored from {source.Name} with {destination.Name}");
            var restoredNames = new List<string>();
            await foreach (DfsEntryInfo entry in restoredVol.Directories.EnumerateAsync("/fmt010", null, Ct))
                restoredNames.Add(entry.Name);
            Assert.Equal(new[] { "dir", "extents", "inline", "marker" }, restoredNames.Order(StringComparer.Ordinal));
            await restoredVol.Files.WriteAllBytesAsync("/fmt010/written-after-restore", TestData.Bytes(7005, 1000), Ct);

            await cluster.WaitForReplicationAsync("fmt010-restored", Ct);
            foreach (TestNode node in cluster.Nodes)
                AssertNoPlaintext(node.Disk, marker, node.Name);
            AssertNoPlaintext(b0.Disk, marker, b0.Name);
        }, Ct);

        static void AssertNoPlaintext(TestDisk disk, byte[] marker, string owner)
        {
            foreach (string file in disk.Files())
                Assert.True(disk.ReadRaw(file).AsSpan().IndexOf(marker) < 0, $"{owner}:{file} contains plaintext file content");
        }
    }
}
