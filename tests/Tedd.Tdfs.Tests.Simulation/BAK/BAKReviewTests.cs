using Tedd.Tdfs.Testing;
using static Tedd.Tdfs.Tests.Simulation.BAK.BAKHelpers;

namespace Tedd.Tdfs.Tests.Simulation.BAK;

/// <summary>
/// Adversarial BAK cases found in review (FSD §12.2, §12.4): a backup node must never lose the data or metadata of a
/// published snapshot through its own housekeeping — neither when garbage collection runs while a transfer publishes, nor
/// when a deleted snapshot that a reader still pins is published again.
/// </summary>
public sealed class BAKReviewTests
{
    private static int s_clientCounter;

    private static string NextClientName(string prefix) => prefix + "r" + Interlocked.Increment(ref s_clientCounter);

    private static async Task SetBackupRetentionAsync(TestCluster cluster, TestBackupNode backup, string snapshot, DfsRetention retention)
    {
        await using DfsClient admin = await ConnectBackupAsync(cluster, backup.RetentionAdminCredentials, backup.Index, NextClientName("ret"));
        await admin.GetVolume(Volume).Snapshots.SetRetentionAsync(snapshot, retention, Ct);
    }

    private static async Task AssertClosureVerifiesAsync(TestBackupNode backup, string snapshot, string because)
    {
        IReadOnlyList<ObjectId> bad = await backup.Inspect.VerifyClosureAsync(Volume, snapshot);
        Assert.True(bad.Count == 0, $"{because}: published {snapshot} on {backup.Name} lacks closure objects [{string.Join(", ", bad)}]");
    }

    /// <summary>
    /// Retention prunes a snapshot and garbage collection deletes its objects slowly; meanwhile the source exports a snapshot
    /// referencing the same objects (an explicit sync of an older snapshot). The publication must not be verified against
    /// objects the running collection is about to delete: after the collection, the published snapshot is still complete.
    /// </summary>
    [Fact]
    [Catalog("BAK-027", "S", "INV-09", "INV-10", "INV-13")]
    public Task GarbageCollectionNeverDeletesTheClosureOfAConcurrentPublication() =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            TestBackupNode b0 = cluster.BackupNodes[0];
            await using DfsClient live = await ConnectLiveAsync(cluster);
            DfsVolume vol = live.GetVolume(Volume);
            await RegisterAsync(b0);
            await b0.RestartAsync(o => o with { KeepLastSnapshots = 1 });
            var model = new TreeModel();
            model.Put("/old/a.bin", TestData.Bytes(910, (4 * 64 * 1024) + 5));
            await WriteTreeAsync(vol, model);
            (DfsSnapshotInfo s1, _) = await SnapshotAndSyncAsync(vol, b0.TargetName, "s1");
            TreeModel model1 = model.Clone();
            HashSet<ObjectId> s1Objects = await FileObjectsAsync(cluster, ["/old/a.bin"]);
            Assert.True(s1Objects.Count >= 3, $"the scenario needs several objects in S1 (got {s1Objects.Count})");

            // S2 shares no object with S1: once S2 is published, retention (KeepLastSnapshots = 1) prunes S1 and garbage
            // collection deletes S1's objects one by one — slowly, every object-store write and flush taking a second.
            await DeleteFileAsync(vol, model, "/old/a.bin");
            await PutAsync(vol, model, "/new.txt", "inline"u8.ToArray());
            b0.Disk.Faults.PathFilter = p => p.Contains("objects", StringComparison.Ordinal);
            b0.Disk.Faults.IoDelay = TimeSpan.FromSeconds(1);
            await SnapshotAndSyncAsync(vol, b0.TargetName, "s2");
            await cluster.WaitUntilAsync(async () => !(await b0.Inspect.GetPublishedSnapshotsAsync(Volume)).Contains("s1"),
                FaultTimeout, "retention prunes S1 once S2 is published", Ct);

            // While the collection runs, the source exports S1 again (it still holds the snapshot; its hold travels with the
            // export, so destination retention keeps the older snapshot next to the newer S2).
            await vol.Snapshots.SetRetentionAsync("s1", new DfsRetention(null, Hold: true), Ct);
            await vol.Backups.RequestSyncAsync(b0.TargetName, "s1", Ct);
            DfsBackupCheckpoint checkpoint = await vol.Backups.AwaitBackupSnapshotAsync(b0.TargetName, "s1", TimeSpan.FromMinutes(3), Ct);
            AssertExactCheckpoint(checkpoint, b0.TargetName, s1);
            b0.Disk.Faults.Reset();

            // Let every pending deletion finish: the published S1 must still verify and read back completely.
            await cluster.DelayAsync(TimeSpan.FromSeconds(30), Ct);
            Assert.Contains("s1", await b0.Inspect.GetPublishedSnapshotsAsync(Volume));
            await AssertClosureVerifiesAsync(b0, "s1", "S1 published while garbage collection ran");
            await using DfsClient reader = await ConnectBackupReaderAsync(cluster, 0, NextClientName("bk"));
            await AssertBackupSnapshotAsync(reader, "s1", model1, "S1 published while garbage collection ran");
        });

    /// <summary>
    /// An explicit sync of an older snapshot (pruned on the backup, held on the source) publishes it again. It must not become
    /// the newest recovery point: the published pointer stays on the newest snapshot, destination retention
    /// (KeepLastSnapshots) never prunes a newer snapshot in its favour, and the source's status agrees with the backup.
    /// </summary>
    [Fact]
    [Catalog("BAK-027", "S", "INV-09", "INV-10", "INV-13")]
    public Task ReexportedOlderSnapshotNeverDisplacesTheNewestRecoveryPoint() =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            TestBackupNode b0 = cluster.BackupNodes[0];
            await using DfsClient live = await ConnectLiveAsync(cluster);
            DfsVolume vol = live.GetVolume(Volume);
            await RegisterAsync(b0);
            await b0.RestartAsync(o => o with { KeepLastSnapshots = 2 });
            TreeModel model = InitialTree(930);
            await WriteTreeAsync(vol, model);
            (DfsSnapshotInfo s1, _) = await SnapshotAndSyncAsync(vol, b0.TargetName, "s1");
            TreeModel model1 = model.Clone();
            await PutAsync(vol, model, "/b.bin", TestData.Bytes(931, 70 * 1024));
            (DfsSnapshotInfo s2, _) = await SnapshotAndSyncAsync(vol, b0.TargetName, "s2");
            TreeModel model2 = model.Clone();
            await PutAsync(vol, model, "/c.bin", TestData.Bytes(932, 70 * 1024));
            (DfsSnapshotInfo s3, _) = await SnapshotAndSyncAsync(vol, b0.TargetName, "s3");
            TreeModel model3 = model.Clone();
            await cluster.WaitUntilAsync(async () => !(await b0.Inspect.GetPublishedSnapshotsAsync(Volume)).Contains("s1"),
                FaultTimeout, "KeepLastSnapshots = 2 prunes S1", Ct);

            // The source holds S1 (the hold travels with the export) and exports it again.
            await vol.Snapshots.SetRetentionAsync("s1", new DfsRetention(null, Hold: true), Ct);
            await SyncExactAsync(vol, b0.TargetName, s1);
            await cluster.DelayAsync(TimeSpan.FromSeconds(5), Ct);

            await AssertPublishedStateAsync(b0, "s3", ["s1", "s2", "s3"], [], "after S1 was exported again");
            DfsBackupStatus status = await vol.Backups.GetStatusAsync(b0.TargetName, Ct);
            Assert.Equal(s3.Boundary, status.PublishedBoundary);
            Assert.Equal("s3", status.LatestPublished?.SnapshotName);
            await using DfsClient reader = await ConnectBackupReaderAsync(cluster, 0, NextClientName("bk"));
            IReadOnlyList<DfsSnapshotInfo> listed = await reader.GetVolume(Volume).Snapshots.ListAsync(Ct);
            Assert.Equal(["s1", "s2", "s3"], listed.Select(s => s.Name).Order(StringComparer.Ordinal));
            await AssertBackupSnapshotAsync(reader, "s1", model1, "re-exported S1");
            await AssertBackupSnapshotAsync(reader, "s2", model2, "S2 kept");
            await AssertBackupSnapshotAsync(reader, "s3", model3, "S3 stays the newest recovery point");
            Assert.Equal(s2.Boundary, (await reader.GetVolume(Volume).Snapshots.GetAsync("s2", Ct)).Boundary);
        });

    /// <summary>
    /// A sync request without a snapshot name exports the current commit under a generated export-snapshot name. Target names
    /// may contain characters a snapshot name may not ('/', ':') and may be as long as a snapshot name: the generated name must
    /// still be valid, or the target could never be synced by request or interval schedule.
    /// </summary>
    [Fact]
    [Catalog("BAK-017", "S", "INV-13", "INV-19")]
    public Task NamelessSyncWorksForEveryValidTargetName() =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            TestBackupNode b0 = cluster.BackupNodes[0];
            await using DfsClient live = await ConnectLiveAsync(cluster);
            DfsVolume vol = live.GetVolume(Volume);
            string target = "site/" + b0.Name + ":" + new string('x', 240);
            await vol.Backups.RegisterTargetAsync(
                new DfsBackupTargetRegistration(target, [b0.Endpoint], b0.ExportCredentials, ManualSchedule, GenerousBudgets), Ct);
            TreeModel model = InitialTree(940);
            await WriteTreeAsync(vol, model);
            CommitToken last = await PutAsync(vol, model, "/z.bin", TestData.Bytes(941, 10 * 1024));

            await vol.Backups.RequestSyncAsync(target, null, Ct);
            DfsBackupCheckpoint checkpoint = await vol.Backups.AwaitBackupAsync(target, last, SyncTimeout, Ct);
            Assert.True(checkpoint.CoveredCommit.IsAtOrAfter(last), $"checkpoint {checkpoint.CoveredCommit} must cover {last}");
            await using DfsClient reader = await ConnectBackupReaderAsync(cluster, 0, NextClientName("bk"));
            await AssertBackupSnapshotAsync(reader, checkpoint.SnapshotName, model, "export snapshot of a target with an unusual name");
        });

    /// <summary>
    /// The age budget of a transfer pin runs from the time the pin was taken, whichever node leads the volume: leadership that
    /// moves faster than the budget while the target stays unreachable must not keep source history pinned indefinitely
    /// (FSD §12.4).
    /// </summary>
    [Fact]
    [Catalog("BAK-023", "S", "INV-09", "INV-13", "INV-15", "INV-16")]
    public Task PinAgeBudgetSurvivesLeadershipChanges() =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            TestBackupNode b0 = cluster.BackupNodes[0];
            await using DfsClient live = await ConnectLiveAsync(cluster);
            DfsVolume vol = live.GetVolume(Volume);
            await RegisterAsync(b0);
            TreeModel model = InitialTree(950);
            await WriteTreeAsync(vol, model);
            await SnapshotAndSyncAsync(vol, b0.TargetName, "s1");
            var budgets = new DfsBackupBudgets(MaxPinAge: TimeSpan.FromSeconds(6), MaxPinnedBytes: 1L << 30);
            await vol.Backups.ConfigureAsync(b0.TargetName, ManualSchedule, budgets, Ct);

            CutFromSources(cluster, b0.Name);
            await PutAsync(vol, model, "/off/a.bin", TestData.Bytes(951, 256 * 1024));
            await SnapshotAsync(vol, "s2");
            await vol.Backups.RequestSyncAsync(b0.TargetName, "s2", Ct);

            // Leadership moves every 4 s: an age measured from each leader's first sight of the pin never reaches 6 s.
            bool reseeded = false;
            for (int round = 0; round < 10 && !reseeded; round++)
            {
                await cluster.DelayAsync(TimeSpan.FromSeconds(4), Ct);
                reseeded = (await vol.Backups.GetStatusAsync(b0.TargetName, Ct)).ReseedRequired;
                if (reseeded)
                    break;
                TestNode leader = await cluster.GetLeaderAsync(Volume, Ct);
                TestNode next = cluster.Nodes.First(n => n.State == TestNodeState.Running && !ReferenceEquals(n, leader));
                await cluster.TransferLeadershipAsync(Volume, next.Name, ct: Ct);
            }

            Assert.True(reseeded, "a target unreachable beyond its pin age budget must be marked for reseed despite leadership changes");
            await cluster.WaitUntilAsync(async () => !(await cluster.Inspect.GetVolumeStateAsync(Volume)).Pins.Any(p => p.Kind == "Transfer"),
                TimeSpan.FromSeconds(10), "abandoning the lineage releases every transfer pin", Ct);
        });

    /// <summary>
    /// A backup node that lost its key material starts locked; a restore that supplies the recovery key unlocks it, and the node
    /// then opens its object store and decrypts its manifests one by one. Garbage collection triggered meanwhile (the retention
    /// administrator deletes an old snapshot) must not treat the snapshots whose manifests are not decrypted yet as
    /// referencing nothing: every retained snapshot stays complete.
    /// </summary>
    [Fact]
    [Catalog("BAK-027", "S", "INV-09", "INV-10", "INV-13")]
    public Task GarbageCollectionWaitsForEveryManifestAfterAnUnlock() =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            TestBackupNode b0 = cluster.BackupNodes[0];
            await using DfsClient live = await ConnectLiveAsync(cluster);
            DfsVolume vol = live.GetVolume(Volume);
            await RegisterAsync(b0);
            TreeModel model = InitialTree(960);
            await WriteTreeAsync(vol, model);
            await SnapshotAndSyncAsync(vol, b0.TargetName, "s1");
            await PutAsync(vol, model, "/two.bin", TestData.Bytes(961, 150 * 1024));
            await SnapshotAndSyncAsync(vol, b0.TargetName, "s2");
            TreeModel model2 = model.Clone();
            await PutAsync(vol, model, "/three.bin", TestData.Bytes(962, 150 * 1024));
            await SnapshotAndSyncAsync(vol, b0.TargetName, "s3");
            TreeModel model3 = model.Clone();

            byte[] secret = b0.Options.BackupSecret.ToArray();
            await b0.RemoveRecoveryKeyAsync();
            Assert.Equal(TestNodeState.Running, b0.State);

            // Decrypting a manifest takes seconds; the restore's recovery key unlocks the node.
            b0.Disk.Faults.PathFilter = p => p.EndsWith(".manifest", StringComparison.Ordinal);
            b0.Disk.Faults.IoDelay = TimeSpan.FromSeconds(2);
            Task<DfsVolumeInfo> restore = live.Admin.RestoreFromBackupAsync(
                new DfsRestoreFromBackupRequest([b0.Endpoint], b0.ReaderCredentials, Volume, "s3", "restored", secret), Ct);
            await cluster.DelayAsync(TimeSpan.FromSeconds(1), Ct);

            // Meanwhile the backup's retention administrator deletes S1: its objects become garbage, nothing else does.
            await using (DfsClient admin = await ConnectBackupAsync(cluster, b0.RetentionAdminCredentials, 0, NextClientName("adm")))
                await admin.GetVolume(Volume).Snapshots.DeleteAsync("s1", Ct);

            Exception? restoreFailure = null;
            try
            {
                await restore;
            }
            catch (Exception ex) when (ex is DfsException or IOException or TimeoutException)
            {
                restoreFailure = ex;
            }

            b0.Disk.Faults.Reset();
            await cluster.DelayAsync(TimeSpan.FromSeconds(2), Ct);
            await b0.RestartAsync(o => o with { BackupSecret = secret });
            IReadOnlyList<string> published = await b0.Inspect.GetPublishedSnapshotsAsync(Volume);
            Assert.Equal(["s2", "s3"], published.Order(StringComparer.Ordinal));
            await AssertClosureVerifiesAsync(b0, "s2", "S2 after garbage collection during the unlock");
            await AssertClosureVerifiesAsync(b0, "s3", "S3 after garbage collection during the unlock");
            await using DfsClient reader = await ConnectBackupReaderAsync(cluster, 0, NextClientName("bk"));
            await AssertBackupSnapshotAsync(reader, "s2", model2, "S2 after garbage collection during the unlock");
            await AssertBackupSnapshotAsync(reader, "s3", model3, "S3 after garbage collection during the unlock");
            Assert.True(restoreFailure is null, $"the restore with the recovery key must succeed: {restoreFailure}");
        });

    /// <summary>
    /// As above with two source volumes on one backup node: while the unlock decrypts the manifests of the first volume, the
    /// retention administrator deletes a snapshot of the second. The collection that follows must not run while any retained
    /// snapshot of any volume lacks its manifest.
    /// </summary>
    [Fact]
    [Catalog("BAK-027", "S", "INV-09", "INV-10", "INV-13")]
    public Task GarbageCollectionOfOneVolumeWaitsForTheManifestsOfAnother() =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            const string other = "vol2";
            TestBackupNode b0 = cluster.BackupNodes[0];
            await using DfsClient live = await ConnectLiveAsync(cluster);
            await live.Admin.CreateVolumeAsync(new DfsVolumeSpec { Name = other }, Ct);
            DfsVolume vol = live.GetVolume(Volume);
            DfsVolume vol2 = live.GetVolume(other);
            await RegisterAsync(b0);
            await b0.RegisterAsync(other, ManualSchedule, GenerousBudgets);
            TreeModel model = InitialTree(970);
            await WriteTreeAsync(vol, model);
            await SnapshotAndSyncAsync(vol, b0.TargetName, "s1");
            await PutAsync(vol, model, "/two.bin", TestData.Bytes(971, 150 * 1024));
            await SnapshotAndSyncAsync(vol, b0.TargetName, "s2");
            TreeModel model2 = model.Clone();
            TreeModel otherModel = InitialTree(972);
            await WriteTreeAsync(vol2, otherModel);
            await SnapshotAndSyncAsync(vol2, b0.TargetName, "t1");
            await PutAsync(vol2, otherModel, "/later.bin", TestData.Bytes(973, 150 * 1024));
            await SnapshotAndSyncAsync(vol2, b0.TargetName, "t2");

            byte[] secret = b0.Options.BackupSecret.ToArray();
            await b0.RemoveRecoveryKeyAsync();
            b0.Disk.Faults.PathFilter = p => p.EndsWith(".manifest", StringComparison.Ordinal);
            b0.Disk.Faults.IoDelay = TimeSpan.FromSeconds(2);
            Task<DfsVolumeInfo> restore = live.Admin.RestoreFromBackupAsync(
                new DfsRestoreFromBackupRequest([b0.Endpoint], b0.ReaderCredentials, Volume, "s2", "restored", secret), Ct);
            await cluster.DelayAsync(TimeSpan.FromSeconds(1), Ct);
            await using (DfsClient admin = await ConnectBackupAsync(cluster, b0.RetentionAdminCredentials, 0, NextClientName("adm")))
                await admin.GetVolume(other).Snapshots.DeleteAsync("t1", Ct);
            await restore;

            b0.Disk.Faults.Reset();
            await cluster.DelayAsync(TimeSpan.FromSeconds(2), Ct);
            await b0.RestartAsync(o => o with { BackupSecret = secret });
            await AssertClosureVerifiesAsync(b0, "s1", "S1 of the first volume");
            await AssertClosureVerifiesAsync(b0, "s2", "S2 of the first volume");
            IReadOnlyList<ObjectId> bad = await b0.Inspect.VerifyClosureAsync(other, "t2");
            Assert.True(bad.Count == 0, $"T2 of the second volume lacks closure objects [{string.Join(", ", bad)}]");
            await using DfsClient reader = await ConnectBackupReaderAsync(cluster, 0, NextClientName("bk"));
            await AssertBackupSnapshotAsync(reader, "s2", model2, "S2 of the first volume");
            await using DfsSnapshotView view = await reader.GetVolume(other).Snapshots.OpenAsync("t2", Ct);
            await AssertViewMatchesAsync(view, otherModel, "T2 of the second volume");
        });

    /// <summary>
    /// The retention administrator deletes a snapshot a reader still pins (it stays readable, condemned); the source then
    /// publishes the same snapshot again. Releasing the old reader pin removes the condemned predecessor only: the new
    /// publication keeps its manifest and its retention hold, also across a restart of the backup node.
    /// </summary>
    [Fact]
    [Catalog("BAK-013", "S", "INV-10", "INV-19", "INV-03")]
    public Task RepublishedSnapshotSurvivesTheReleaseOfItsCondemnedPredecessor() =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            TestBackupNode b0 = cluster.BackupNodes[0];
            await using DfsClient live = await ConnectLiveAsync(cluster);
            DfsVolume vol = live.GetVolume(Volume);
            await RegisterAsync(b0);
            TreeModel model = InitialTree(920);
            await WriteTreeAsync(vol, model);
            (DfsSnapshotInfo s1, _) = await SnapshotAndSyncAsync(vol, b0.TargetName, "s1");
            TreeModel model1 = model.Clone();
            await PutAsync(vol, model, "/later.bin", TestData.Bytes(921, 80 * 1024));
            await SnapshotAndSyncAsync(vol, b0.TargetName, "s2");
            TreeModel model2 = model.Clone();

            // A reader pins S1; the backup's retention administrator deletes it: S1 is condemned but stays readable.
            await using DfsClient reader = await ConnectBackupReaderAsync(cluster, 0, NextClientName("rd"));
            DfsSnapshotView view = await reader.GetVolume(Volume).Snapshots.OpenAsync("s1", Ct);
            await using (DfsClient admin = await ConnectBackupAsync(cluster, b0.RetentionAdminCredentials, 0, NextClientName("adm")))
                await admin.GetVolume(Volume).Snapshots.DeleteAsync("s1", Ct);
            Assert.DoesNotContain("s1", await b0.Inspect.GetPublishedSnapshotsAsync(Volume));
            await AssertViewMatchesAsync(view, model1, "the pinned reader of the deleted S1");

            // The source exports S1 again: a new publication of the same snapshot, protected by a hold.
            await SyncExactAsync(vol, b0.TargetName, s1);
            await SetBackupRetentionAsync(cluster, b0, "s1", new DfsRetention(null, Hold: true));

            // Releasing the old pin removes the condemned predecessor, never the files of the new publication.
            await view.DisposeAsync();
            await cluster.DelayAsync(TimeSpan.FromSeconds(2), Ct);
            Assert.Contains("s1", await b0.Inspect.GetPublishedSnapshotsAsync(Volume));
            await AssertClosureVerifiesAsync(b0, "s1", "republished S1 after the old pin was released");

            await b0.RestartAsync();
            IReadOnlyList<string> published = await b0.Inspect.GetPublishedSnapshotsAsync(Volume);
            Assert.Contains("s1", published);
            Assert.Contains("s2", published);
            await AssertClosureVerifiesAsync(b0, "s1", "republished S1 after a restart");
            await using DfsClient after = await ConnectBackupReaderAsync(cluster, 0, NextClientName("bk"));
            DfsSnapshotInfo info = await after.GetVolume(Volume).Snapshots.GetAsync("s1", Ct);
            Assert.True(info.Retention.Hold, "the retention hold of the republished S1 must survive a restart");
            await AssertBackupSnapshotAsync(after, "s1", model1, "republished S1 after a restart");
            await AssertBackupSnapshotAsync(after, "s2", model2, "S2 after a restart");
        });
}
