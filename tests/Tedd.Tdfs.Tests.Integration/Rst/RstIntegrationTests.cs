using System.Globalization;
using System.Text;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Integration.Rst;

/// <summary>
/// RST — disaster recovery, recovery keys and promotion (FSD §§11.1, 12.4–12.5, 14; test plan §9.8). Integration level:
/// real QUIC transport and on-disk storage; "clean host" recoveries run in an independent recovery cluster that can only
/// reach the backup host.
/// </summary>
public sealed class RstIntegrationTests
{
    private const string Vol = RecoveryScenario.Volume;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    // ---------------------------------------------------------------------------------------------------------------
    // RST-001
    // ---------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Destroys every source node (process, network endpoint and disk) and the source client, then rebuilds a live
    /// volume in a new cluster using only the backup host and its documented credentials; any contact with a source node
    /// fails the test. Varies source and recovery cluster sizes.
    /// </summary>
    [Theory]
    [InlineData(3, 3)]
    [InlineData(5, 3)]
    [InlineData(3, 5)]
    [Catalog("RST-001", "I", "INV-08", "INV-10", "INV-12", "INV-13", "INV-19")]
    public Task RST_001_RestoreWithAllSourceNodesAbsent(int sourceNodes, int recoveryNodes) =>
        ClusterTest.RunAsync(RstHelpers.SourceOptions(sourceNodes), async cluster =>
        {
            BackedUpSource source = await RecoveryScenario.CreateBackedUpSourceAsync(cluster, "S", 1001, Ct);
            IReadOnlyDictionary<string, long> baseline = await RecoveryScenario.DestroySourceAsync(cluster, source.Client);
            Assert.All(cluster.Nodes, n => Assert.Equal(TestNodeState.Crashed, n.State));

            await using TestCluster recovery = await cluster.StartRecoveryClusterAsync(RstHelpers.RecoveryOptions(recoveryNodes), Ct);
            Assert.NotEqual(cluster.ClusterId, recovery.ClusterId);
            Assert.DoesNotContain(Vol, await recovery.Admin.ListVolumesAsync(Ct));

            DfsVolumeInfo restored = await recovery.Admin.RestoreFromBackupAsync(RecoveryScenario.RestoreRequest(source.Backup, "S", Vol), Ct);
            RecoveryScenario.AssertRestoredIdentity(restored, source.Info, recovery.ClusterId, Vol);
            Assert.Contains(Vol, await recovery.Admin.ListVolumesAsync(Ct));

            await using DfsClient fresh = await recovery.ConnectAsync(new TestClientOptions { Name = "verifier" }, Ct);
            DfsVolume volume = fresh.GetVolume(Vol);
            VolumeContentFixture.AssertMatches(source.Expected, await VolumeContentFixture.CaptureAsync(volume, Ct),
                "volume rebuilt with every source node, disk and client cache absent");

            // The rebuilt volume is a live, writable authority of the recovery cluster.
            byte[] probe = TestData.Bytes(1002, 5000);
            CommitToken written = await volume.Files.WriteAllBytesAsync("/after-restore.bin", probe, Ct);
            Assert.True(written.IsComparableWith(restored.LastCommit), "new commits belong to the restored authority");
            Assert.True(written.IsAtOrAfter(restored.LastCommit));
            RecoveryScenario.AssertNotComparable(written, source.Info.LastCommit, "the source authority is gone; its tokens do not order the new history");
            await RecoveryScenario.AssertFileBytesAsync(volume.Files, "/after-restore.bin", probe, "written after restore", Ct);

            await recovery.WaitForReplicationAsync(Vol, Ct);
            await recovery.Inspect.AssertReplicasConvergedAsync(Vol);
            RecoveryScenario.AssertNoSourceContact(cluster, baseline, "while restoring and operating the recovered volume");
            Assert.All(cluster.Nodes, n => Assert.Equal(TestNodeState.Crashed, n.State));
        }, Ct);

    // ---------------------------------------------------------------------------------------------------------------
    // RST-002
    // ---------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Compares every restored file, directory, length, byte, sparse range, metadata field (owner, access, timestamps,
    /// policy, inline/sparse flags) and the volume's policies against the independent manifest and the source's own view
    /// of the snapshot; repeated for inline thresholds 0 (no inline data), default and large.
    /// </summary>
    [Theory]
    [InlineData(4096)]
    [InlineData(0)]
    [InlineData(16384)]
    [Catalog("RST-002", "I", "INV-08", "INV-10", "INV-12", "INV-13", "INV-19")]
    public Task RST_002_RestoreExactNamespaceAndBytes(int inlineThreshold) =>
        ClusterTest.RunAsync(RstHelpers.SourceOptions(inlineThreshold: inlineThreshold), async cluster =>
        {
            BackedUpSource source = await RecoveryScenario.CreateBackedUpSourceAsync(cluster, "S", 2000 + (ulong)inlineThreshold, Ct);
            Assert.Equal(inlineThreshold, source.Info.InlineThreshold);

            // Changes after S must not leak into a restore of S.
            await source.Volume.Files.WriteAllBytesAsync("/docs/one.bin", TestData.Bytes(2999, 17), Ct);
            await source.Volume.Files.DeleteAsync(VolumeContentFixture.TextPath, Ct);
            await source.Volume.Directories.CreateDirectoryAsync("/created-after-snapshot", Ct);

            await using (DfsClient reader = await RstHelpers.ConnectBackupReaderAsync(cluster, source.Backup, "bk-reader", Ct))
            await using (DfsSnapshotView view = await reader.GetVolume(Vol).Snapshots.OpenAsync("S", Ct))
            {
                Assert.True(view.Files.IsReadOnly);
                ObservedVolumeContent backupView = await VolumeContentFixture.CaptureAsync(view, Ct);
                VolumeContentFixture.AssertMatches(source.Expected, backupView, "backup host view of S");
                VolumeContentFixture.AssertSameMetadata(source.SnapshotCapture, backupView, "backup host view vs source view of S");
            }

            IReadOnlyDictionary<string, long> baseline = await RecoveryScenario.DestroySourceAsync(cluster, source.Client);
            await using TestCluster recovery = await cluster.StartRecoveryClusterAsync(RstHelpers.RecoveryOptions(), Ct);
            DfsVolumeInfo restored = await recovery.Admin.RestoreFromBackupAsync(RecoveryScenario.RestoreRequest(source.Backup, "S", Vol), Ct);
            RecoveryScenario.AssertRestoredIdentity(restored, source.Info, recovery.ClusterId, Vol);
            Assert.Equal(source.Info.Metadata.Voters, restored.Metadata.Voters);

            ObservedVolumeContent restoredView = await RstHelpers.CaptureFreshAsync(recovery, Vol, "verifier", Ct);
            VolumeContentFixture.AssertMatches(source.Expected, restoredView, "restored volume vs independent manifest");
            VolumeContentFixture.AssertSameMetadata(source.SnapshotCapture, restoredView, "restored volume vs source view of S (every metadata field)");

            await RstHelpers.AssertHolesUnmappedAsync(recovery, Vol, source.Expected.Files[VolumeContentFixture.SparsePath]);
            foreach (ExpectedFile file in source.Expected.Files.Values)
            {
                FileInspection? inspection = await recovery.Inspect.GetFileAsync(Vol, file.Path);
                Assert.NotNull(inspection);
                Assert.Equal(file.Content.LongLength, inspection.Length);
                Assert.Equal(source.SnapshotCapture.Entries[file.Path].IsInline, inspection.IsInline);
                if (inspection.IsInline)
                {
                    Assert.Empty(inspection.Extents);
                    Assert.True(file.Content.LongLength <= inlineThreshold, $"{file.Path} is inline but longer than the threshold {inlineThreshold}");
                }
                else if (!file.IsSparse && file.Content.Length > 0)
                {
                    Assert.Equal(file.Content.LongLength, inspection.Extents.Sum(e => e.Length));
                }

                if (file.PolicyName is not null)
                {
                    Assert.Equal(file.PolicyName, inspection.Policy);
                    foreach (ExtentInspection extent in inspection.Extents)
                    {
                        (int copies, int zones) = await recovery.Inspect.CountDurableDomainsAsync(extent.Object);
                        Assert.True(copies >= VolumeContentFixture.PairPolicyDefinition.DurableCopiesBeforeAck && zones >= VolumeContentFixture.PairPolicyDefinition.DurableZonesBeforeAck,
                            $"{file.Path}: restored object {extent.Object} has {copies} copies in {zones} zones, below its '{file.PolicyName}' policy");
                    }
                }
            }

            RecoveryScenario.AssertNoSourceContact(cluster, baseline, "during restore");
        }, Ct);

    // ---------------------------------------------------------------------------------------------------------------
    // RST-003
    // ---------------------------------------------------------------------------------------------------------------

    /// <summary>How the snapshot boundary relates to the committed transactions in RST-003.</summary>
    public enum TxBoundaryKind
    {
        /// <summary>The boundary is created by the transaction's own commit-and-snapshot decision.</summary>
        CommitWithSnapshot,

        /// <summary>A snapshot with MinimumCommit = the last commit token, taken after the commit.</summary>
        SnapshotAfterCommit,

        /// <summary>A snapshot taken while further transactions keep committing.</summary>
        SnapshotDuringConcurrentTransactions,
    }

    /// <summary>
    /// Multi-file transactions (account rewrites, counter, rename, directory creation) are restored whole: the restored
    /// volume shows exactly the last generation at or before the snapshot boundary, never a mix, and nothing from aborted,
    /// uncommitted or later transactions. Restore follows test plan §9.8: the source is destroyed and S is restored on a
    /// clean recovery cluster that never contacts a source node.
    /// </summary>
    [Theory]
    [InlineData(TxBoundaryKind.CommitWithSnapshot)]
    [InlineData(TxBoundaryKind.SnapshotAfterCommit)]
    [InlineData(TxBoundaryKind.SnapshotDuringConcurrentTransactions)]
    [Catalog("RST-003", "I", "INV-08", "INV-10", "INV-12", "INV-13", "INV-19")]
    public Task RST_003_RestoreTransactionsWhole(TxBoundaryKind kind) =>
        ClusterTest.RunAsync(RstHelpers.SourceOptions(), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "app" }, Ct);
            DfsVolume volume = client.GetVolume(Vol);
            await RstHelpers.InitLedgerAsync(volume, Ct);
            TestBackupNode backup = cluster.BackupNodes[0];
            await backup.RegisterAsync(Vol, RecoveryScenario.ManualSchedule);

            await using (DfsTransaction aborted = await volume.Transactions.BeginAsync(null, Ct))
            {
                await RstHelpers.StageGenerationAsync(aborted, 900, Ct);
                await aborted.AbortAsync(Ct);
                Assert.Equal(DfsTransactionState.Aborted, aborted.State);
            }

            var committed = new List<(int Generation, CommitToken Token)>();
            const int lastGeneration = 6;
            DfsSnapshotInfo snapshot;
            DfsTransaction pending = await volume.Transactions.BeginAsync(null, Ct);
            await pending.Files.WriteAllBytesAsync(RstHelpers.PendingFile, TestData.Bytes(3001, RstHelpers.PendingFileLength), Ct);
            await pending.Directories.CreateDirectoryAsync(RstHelpers.PendingSubdirectory, Ct);
            switch (kind)
            {
                case TxBoundaryKind.CommitWithSnapshot:
                    for (int g = 1; g <= 2; g++)
                        committed.Add((g, await RstHelpers.CommitGenerationAsync(volume, g, Ct)));
                    await using (DfsTransaction t3 = await volume.Transactions.BeginAsync(null, Ct))
                    {
                        await RstHelpers.StageGenerationAsync(t3, 3, Ct);
                        snapshot = await t3.CommitWithSnapshotAsync("S", Ct);
                        committed.Add((3, snapshot.Boundary));
                    }

                    for (int g = 4; g <= lastGeneration; g++)
                        committed.Add((g, await RstHelpers.CommitGenerationAsync(volume, g, Ct)));
                    await pending.AbortAsync(Ct);
                    break;

                case TxBoundaryKind.SnapshotAfterCommit:
                    for (int g = 1; g <= 3; g++)
                        committed.Add((g, await RstHelpers.CommitGenerationAsync(volume, g, Ct)));
                    snapshot = await volume.Snapshots.CreateAsync("S", new DfsSnapshotOptions { MinimumCommit = committed[^1].Token }, Ct);
                    Assert.True(snapshot.Boundary.IsAtOrAfter(committed[^1].Token), "MinimumCommit must bound the snapshot from below");
                    for (int g = 4; g <= lastGeneration; g++)
                        committed.Add((g, await RstHelpers.CommitGenerationAsync(volume, g, Ct)));
                    await pending.AbortAsync(Ct);
                    break;

                default:
                    var gate = new object();
                    async Task CommitLoopAsync()
                    {
                        for (int g = 1; g <= lastGeneration; g++)
                        {
                            CommitToken token = await RstHelpers.CommitGenerationAsync(volume, g, Ct);
                            lock (gate)
                                committed.Add((g, token));
                        }
                    }

                    Task loop = CommitLoopAsync();
                    await cluster.WaitUntilAsync(() =>
                    {
                        lock (gate)
                            return Task.FromResult(committed.Count >= 2 || loop.IsCompleted);
                    }, TimeSpan.FromSeconds(30), "two transactions committed before the concurrent snapshot");
                    snapshot = await volume.Snapshots.CreateAsync("S", null, Ct);
                    await loop;
                    CommitToken pendingToken = await pending.CommitAsync(Ct);
                    Assert.True(pendingToken.CompareTo(snapshot.Boundary) > 0, "the pending transaction committed after the boundary");
                    break;
            }

            await pending.DisposeAsync();
            Assert.Equal(lastGeneration, committed.Count);
            Assert.Equal(DfsConsistencyLabel.FilesystemConsistent, snapshot.Label);
            int boundaryGeneration = committed.Where(c => c.Token.CompareTo(snapshot.Boundary) <= 0).Select(c => c.Generation).DefaultIfEmpty(0).Max();
            if (kind != TxBoundaryKind.SnapshotDuringConcurrentTransactions)
                Assert.Equal(3, boundaryGeneration);
            else
                Assert.True(boundaryGeneration >= 2, $"the snapshot was taken after two commits completed, boundary generation {boundaryGeneration}");

            await RecoveryScenario.PublishAsync(volume, backup, snapshot, Ct);
            // In the concurrent case the pending transaction committed after the boundary: whole on the live volume, absent from S.
            bool pendingCommittedLive = kind == TxBoundaryKind.SnapshotDuringConcurrentTransactions;
            await RstHelpers.AssertLedgerAsync(volume.Files, volume.Directories, lastGeneration, lastGeneration, "live volume after all commits", Ct,
                pendingCommitted: pendingCommittedLive);

            // Test plan §9.8: no source service, disk or client cache survives into the recovery environment.
            IReadOnlyDictionary<string, long> baseline = await RecoveryScenario.DestroySourceAsync(cluster, client);
            await using TestCluster recovery = await cluster.StartRecoveryClusterAsync(RstHelpers.RecoveryOptions(), Ct);
            DfsVolumeInfo restored = await recovery.Admin.RestoreFromBackupAsync(RecoveryScenario.RestoreRequest(backup, "S", Vol), Ct);
            Assert.Equal(Vol, restored.Name);
            Assert.Equal(recovery.ClusterId, restored.LastCommit.ClusterId);
            RecoveryScenario.AssertNotComparable(restored.LastCommit, committed[^1].Token, "the restored ledger is a new authority");
            await using DfsClient verifier = await recovery.ConnectAsync(new TestClientOptions { Name = "verifier" }, Ct);
            DfsVolume restoredVolume = verifier.GetVolume(Vol);
            await RstHelpers.AssertLedgerAsync(restoredVolume.Files, restoredVolume.Directories, boundaryGeneration, lastGeneration,
                $"restored S ({kind}) contains whole transactions through generation {boundaryGeneration}", Ct);

            await using DfsClient reader = await RstHelpers.ConnectBackupReaderAsync(cluster, backup, "bk-reader", Ct);
            await using DfsSnapshotView view = await reader.GetVolume(Vol).Snapshots.OpenAsync("S", Ct);
            await RstHelpers.AssertLedgerAsync(view.Files, view.Directories, boundaryGeneration, lastGeneration, "backup host view of S", Ct);
            RecoveryScenario.AssertNoSourceContact(cluster, baseline, "while restoring the transaction ledger");
        }, Ct);

    // ---------------------------------------------------------------------------------------------------------------
    // RST-004
    // ---------------------------------------------------------------------------------------------------------------

    /// <summary>Why the later source commits never reached the backup in RST-004.</summary>
    public enum BackupLagCause
    {
        /// <summary>The backup host was stopped before the later commits.</summary>
        BackupTargetOffline,

        /// <summary>The backup host kept running but every link between it and the live nodes was cut before the later commits.</summary>
        BackupUnreachable,

        /// <summary>
        /// The backup host was online and reachable but no synchronisation was requested after S
        /// (<see cref="RecoveryScenario.ManualSchedule"/>: the target only syncs on an explicit request; a barrier is a wait,
        /// not a sync request).
        /// </summary>
        NoSyncRequested,
    }

    /// <summary>
    /// Commits overwrite/create/delete T after the backed-up snapshot S, never replicates T, loses the source and restores:
    /// the source and backup report S's boundary as the recovery point, barriers for T time out with the actual last
    /// checkpoint, the restore contains S exactly, and nothing claims T survived.
    /// </summary>
    [Theory]
    [InlineData(BackupLagCause.BackupTargetOffline)]
    [InlineData(BackupLagCause.BackupUnreachable)]
    [InlineData(BackupLagCause.NoSyncRequested)]
    [Catalog("RST-004", "I", "INV-08", "INV-10", "INV-12", "INV-13", "INV-19")]
    public Task RST_004_RestoreNoLaterUnreplicatedGuarantee(BackupLagCause cause) =>
        ClusterTest.RunAsync(RstHelpers.SourceOptions(), async cluster =>
        {
            BackedUpSource source = await RecoveryScenario.CreateBackedUpSourceAsync(cluster, "S", 4000, Ct);
            TestBackupNode backup = source.Backup;
            DfsVolume volume = source.Volume;
            if (cause == BackupLagCause.BackupTargetOffline)
                await backup.StopAsync();
            if (cause == BackupLagCause.BackupUnreachable)
            {
                foreach (TestNode node in cluster.Nodes)
                    cluster.Network.CutBoth(node.Name, backup.Name);
            }

            DfsOperationId overwriteId = await volume.NewOperationIdAsync(Ct);
            byte[] overwritten = TestData.Bytes(4001, 3333);
            CommitToken t1 = await volume.Files.WriteAllBytesAsync("/docs/one.bin", overwritten, new DfsMutationOptions { OperationId = overwriteId }, Ct);
            CommitToken t2 = await volume.Files.WriteAllBytesAsync("/later.bin", TestData.Bytes(4002, 70_000), Ct);
            CommitToken t3 = await volume.Files.DeleteAsync("/docs/empty.txt", Ct);
            DfsSnapshotInfo s2 = await volume.Snapshots.CreateAsync("S2", null, Ct);
            Assert.True(t1.CompareTo(source.Snapshot.Boundary) > 0, "T is after S");
            Assert.True(t3.IsAtOrAfter(t2) && t2.IsAtOrAfter(t1) && s2.Boundary.IsAtOrAfter(t3));

            DfsBackupStatus status = await volume.Backups.GetStatusAsync(backup.TargetName, Ct);
            Assert.Equal("S", status.LatestPublished?.SnapshotName);
            Assert.Equal(source.Snapshot.Boundary, status.PublishedBoundary);
            Assert.True(status.PublishedBoundary.CompareTo(t1) < 0, "the published recovery boundary must not be reported beyond what reached the backup");

            // T never reaches the target (offline, unreachable, or never requested), so both barriers must time out reporting
            // the actual last published checkpoint. The later assertions (the host holds only S) depend on this as well.
            DfsBackupNotReachedException notReached = await Assert.ThrowsAsync<DfsBackupNotReachedException>(
                () => volume.Backups.AwaitBackupAsync(backup.TargetName, t3, TimeSpan.FromSeconds(2), Ct));
            Assert.Equal(backup.TargetName, notReached.Target);
            Assert.NotNull(notReached.LastPublished);
            Assert.Equal("S", notReached.LastPublished.SnapshotName);
            Assert.Equal(source.Snapshot.Boundary, notReached.LastPublished.CoveredCommit);
            DfsBackupNotReachedException exactNotReached = await Assert.ThrowsAsync<DfsBackupNotReachedException>(
                () => volume.Backups.AwaitBackupSnapshotAsync(backup.TargetName, "S2", TimeSpan.FromSeconds(2), Ct));
            Assert.Equal("S", exactNotReached.LastPublished?.SnapshotName);

            await RecoveryScenario.AssertFileBytesAsync(volume.Files, "/docs/one.bin", overwritten, "a barrier timeout must not undo live commits", Ct);

            IReadOnlyDictionary<string, long> baseline = await RecoveryScenario.DestroySourceAsync(cluster, source.Client);
            if (cause == BackupLagCause.BackupTargetOffline)
                await backup.RestartAsync();

            await using (DfsClient reader = await RstHelpers.ConnectBackupReaderAsync(cluster, backup, "bk-reader", Ct))
            {
                IReadOnlyList<DfsSnapshotInfo> held = await reader.GetVolume(Vol).Snapshots.ListAsync(Ct);
                DfsSnapshotInfo onlyS = Assert.Single(held);
                Assert.Equal("S", onlyS.Name);
                Assert.Equal(source.Snapshot.Boundary, onlyS.Boundary);
                Assert.True(onlyS.Boundary.CompareTo(t1) < 0, "the backup host discloses S's boundary, before T");
            }

            await using TestCluster recovery = await cluster.StartRecoveryClusterAsync(RstHelpers.RecoveryOptions(), Ct);
            DfsVolumeInfo restored = await recovery.Admin.RestoreFromBackupAsync(RecoveryScenario.RestoreRequest(backup, "S", Vol), Ct);
            RecoveryScenario.AssertRestoredIdentity(restored, source.Info, recovery.ClusterId, Vol);
            RecoveryScenario.AssertNotComparable(restored.LastCommit, t3, "the restored authority makes no ordering claim about T");

            await using DfsClient client = await recovery.ConnectAsync(new TestClientOptions { Name = "verifier" }, Ct);
            DfsVolume rv = client.GetVolume(Vol);
            VolumeContentFixture.AssertMatches(source.Expected, await VolumeContentFixture.CaptureAsync(rv, Ct), "restore of S excludes every unreplicated write of T");
            await Assert.ThrowsAsync<FileNotFoundException>(() => rv.Files.ReadAllBytesAsync("/later.bin", Ct));

            DfsOperationOutcome? outcome = null;
            Exception? queryFailure = await RecoveryScenario.CaptureAsync(async () => outcome = await rv.QueryOperationAsync(overwriteId, false, Ct));
            if (queryFailure is null)
            {
                Assert.NotNull(outcome);
                Assert.True(outcome.Status is DfsOutcomeStatus.NotFound or DfsOutcomeStatus.Expired,
                    $"the restored authority must not report the unreplicated operation {overwriteId} as {outcome.Status}");
            }
            else
            {
                RecoveryScenario.AssertFailedWith(queryFailure, "querying an operation of the lost authority",
                    DfsErrorCode.OperationExpired, DfsErrorCode.EpochMismatch, DfsErrorCode.ClusterMismatch, DfsErrorCode.OperationIdMismatch, DfsErrorCode.InvalidArgument);
            }

            Exception? s2Restore = await RecoveryScenario.CaptureAsync(() => recovery.Admin.RestoreFromBackupAsync(RecoveryScenario.RestoreRequest(backup, "S2", "vol-s2"), Ct));
            RecoveryScenario.AssertFailedWith(s2Restore, "S2 never reached the backup", DfsErrorCode.SnapshotNotFound);
            Assert.DoesNotContain("vol-s2", await recovery.Admin.ListVolumesAsync(Ct));
            RecoveryScenario.AssertNoSourceContact(cluster, baseline, "during restore");
        }, Ct);

    // ---------------------------------------------------------------------------------------------------------------
    // RST-005
    // ---------------------------------------------------------------------------------------------------------------

    /// <summary>Which recovery key material is missing in RST-005.</summary>
    public enum KeyLoss
    {
        /// <summary>The backup host's key material is removed and no recovery key is supplied.</summary>
        BackupKeyRemovedNoRecoveryKey,

        /// <summary>The backup host's key material is removed and a wrong recovery key is supplied.</summary>
        BackupKeyRemovedWrongRecoveryKey,
    }

    /// <summary>
    /// Removing the required key material makes restoration fail with an explicit recovery-key error, creates no volume,
    /// and the backup host refuses to present the undecryptable bytes as a readable snapshot — although the encrypted
    /// bytes are still physically on its disk.
    /// </summary>
    [Theory]
    [InlineData(KeyLoss.BackupKeyRemovedNoRecoveryKey)]
    [InlineData(KeyLoss.BackupKeyRemovedWrongRecoveryKey)]
    [Catalog("RST-005", "I", "INV-08", "INV-10", "INV-12", "INV-13", "INV-19")]
    public Task RST_005_MissingKeyFailsClearly(KeyLoss loss) =>
        ClusterTest.RunAsync(RstHelpers.SourceOptions(), async cluster =>
        {
            BackedUpSource source = await RecoveryScenario.CreateBackedUpSourceAsync(cluster, "S", 5000, Ct);
            IReadOnlyDictionary<string, long> baseline = await RecoveryScenario.DestroySourceAsync(cluster, source.Client);
            await source.Backup.RemoveRecoveryKeyAsync();
            Assert.NotEmpty(source.Backup.Disk.Files());

            byte[]? suppliedKey = loss == KeyLoss.BackupKeyRemovedWrongRecoveryKey ? TestData.Bytes(5001, 64) : null;
            await using TestCluster recovery = await cluster.StartRecoveryClusterAsync(RstHelpers.RecoveryOptions(), Ct);
            Exception? failure = await RecoveryScenario.CaptureAsync(
                () => recovery.Admin.RestoreFromBackupAsync(RecoveryScenario.RestoreRequest(source.Backup, "S", Vol, suppliedKey), Ct));
            if (loss == KeyLoss.BackupKeyRemovedNoRecoveryKey)
            {
                DfsRecoveryKeyMissingException missing = Assert.IsType<DfsRecoveryKeyMissingException>(failure);
                Assert.Equal(DfsErrorCode.RecoveryKeyMissing, missing.ErrorCode);
            }
            else
            {
                Assert.IsAssignableFrom<DfsException>(failure);
                RecoveryScenario.AssertFailedWith(failure, "a wrong recovery key cannot decrypt the backup", DfsErrorCode.RecoveryKeyMissing, DfsErrorCode.Integrity);
            }

            Assert.DoesNotContain(Vol, await recovery.Admin.ListVolumesAsync(Ct));

            Exception? readFailure = await RecoveryScenario.CaptureAsync(async () =>
            {
                await using DfsClient reader = await RstHelpers.ConnectBackupReaderAsync(cluster, source.Backup, "bk-reader", Ct);
                await using DfsSnapshotView view = await reader.GetVolume(Vol).Snapshots.OpenAsync("S", Ct);
                _ = await view.Files.ReadAllBytesAsync(VolumeContentFixture.SparsePath, Ct);
            });
            Assert.IsType<DfsRecoveryKeyMissingException>(readFailure);
            RecoveryScenario.AssertNoSourceContact(cluster, baseline, "during the failed restore");
        }, Ct);

    // ---------------------------------------------------------------------------------------------------------------
    // RST-006
    // ---------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Rotates the volume data key between snapshots, publishes every snapshot, verifies objects of every key version
    /// coexist and stay readable live, then destroys the source and restores each historical snapshot exactly.
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [Catalog("RST-006", "I", "INV-08", "INV-10", "INV-12", "INV-13", "INV-19")]
    public Task RST_006_KeyRotationHistoricalRestore(int rotations) =>
        ClusterTest.RunAsync(RstHelpers.SourceOptions(), async cluster =>
        {
            BackedUpSource source = await RecoveryScenario.CreateBackedUpSourceAsync(cluster, "S0", 6000, Ct);
            int chunk = source.Client.Capabilities.ChunkSize;
            var history = new List<(string Snapshot, ExpectedVolumeContent Expected)> { ("S0", source.Expected) };
            ExpectedVolumeContent current = source.Expected.Clone();
            await source.Volume.Directories.CreateDirectoryAsync("/rotated", Ct);
            current.AddDirectory("/rotated");
            for (int r = 1; r <= rotations; r++)
            {
                await cluster.Admin.RotateDataKeyAsync(Vol, Ct);
                string added = "/rotated/r" + r.ToString(CultureInfo.InvariantCulture) + ".bin";
                byte[] addedBytes = TestData.Bytes(6000 + (ulong)r, 3 * chunk + 11);
                await source.Volume.Files.WriteAllBytesAsync(added, addedBytes, Ct);
                current.SetFile(new ExpectedFile(added, addedBytes, null, new DfsInodeAccess(), []));
                byte[] replaced = TestData.Bytes(6100 + (ulong)r, chunk + 1500);
                await source.Volume.Files.WriteAllBytesAsync("/docs/one.bin", replaced, Ct);
                current.SetFile(current.Files["/docs/one.bin"] with { Content = replaced });
                DfsSnapshotInfo snapshot = await source.Volume.Snapshots.CreateAsync("S" + r.ToString(CultureInfo.InvariantCulture), null, Ct);
                await RecoveryScenario.PublishAsync(source.Volume, source.Backup, snapshot, Ct);
                history.Add((snapshot.Name, current.Clone()));
            }

            var keyVersions = new SortedSet<uint>();
            foreach (TestNode node in cluster.Nodes)
            {
                foreach (ObjectRecordInfo record in await node.Inspect.ListObjectsAsync(Vol))
                    keyVersions.Add(record.KeyVersion);
            }

            Assert.True(keyVersions.Count >= rotations + 1,
                $"objects of every key version must be retained while referenced; found versions [{string.Join(", ", keyVersions)}] after {rotations} rotation(s)");
            VolumeContentFixture.AssertMatches(current, await VolumeContentFixture.CaptureAsync(source.Volume, Ct),
                "live reads after rotation decrypt objects of every retained key version");

            IReadOnlyDictionary<string, long> baseline = await RecoveryScenario.DestroySourceAsync(cluster, source.Client);
            await using TestCluster recovery = await cluster.StartRecoveryClusterAsync(RstHelpers.RecoveryOptions(), Ct);
            foreach ((string snapshotName, ExpectedVolumeContent expected) in history)
            {
                string target = "v-" + snapshotName.ToLowerInvariant();
                DfsVolumeInfo restored = await recovery.Admin.RestoreFromBackupAsync(RecoveryScenario.RestoreRequest(source.Backup, snapshotName, target), Ct);
                Assert.Equal(target, restored.Name);
                ObservedVolumeContent observed = await RstHelpers.CaptureFreshAsync(recovery, target, "verify-" + target, Ct);
                VolumeContentFixture.AssertMatches(expected, observed, $"historical restore of {snapshotName} after {rotations} key rotation(s)");
            }

            RecoveryScenario.AssertNoSourceContact(cluster, baseline, "during historical restores");
        }, Ct);

    // ---------------------------------------------------------------------------------------------------------------
    // RST-010
    // ---------------------------------------------------------------------------------------------------------------

    /// <summary>The same-identity recovery attempts of RST-010.</summary>
    public enum PromotionCase
    {
        /// <summary>Restore into the same cluster and volume name while the old authority is live: refused.</summary>
        SameClusterSameNameWhileOldLive,

        /// <summary>Fence the old authority (delete the volume), then restore into the same name: a new identity, old writer fenced.</summary>
        SameClusterSameNameAfterFencing,

        /// <summary>Restore into a new cluster while the old authority can still write: a distinct identity, histories never merge.</summary>
        NewClusterIdentityWhileOldLive,
    }

    /// <summary>
    /// Same-identity recovery is refused while the old authority can still write; it is permitted only after explicit
    /// fencing (and then the old writer is rejected) or under a new identity whose history never merges with the old one.
    /// </summary>
    [Theory]
    [InlineData(PromotionCase.SameClusterSameNameWhileOldLive)]
    [InlineData(PromotionCase.SameClusterSameNameAfterFencing)]
    [InlineData(PromotionCase.NewClusterIdentityWhileOldLive)]
    [Catalog("RST-010", "I", "INV-08", "INV-10", "INV-12", "INV-13", "INV-19")]
    public Task RST_010_PromotionRequiresOldAuthorityFencing(PromotionCase promotion) =>
        ClusterTest.RunAsync(RstHelpers.SourceOptions(), async cluster =>
        {
            BackedUpSource source = await RecoveryScenario.CreateBackedUpSourceAsync(cluster, "S", 10_000, Ct);
            DfsVolume oldVolume = source.Volume;
            byte[] staged = TestData.Bytes(10_001, 2048);
            DfsStream oldWriter = await oldVolume.Files.OpenAsync("/docs/one.bin", FileMode.Open, FileAccess.ReadWrite, FileShare.Read, Ct);
            await oldWriter.WriteAsync(staged, Ct);
            CommitToken oldToken = (await oldVolume.GetInfoAsync(Ct)).LastCommit;

            switch (promotion)
            {
                case PromotionCase.SameClusterSameNameWhileOldLive:
                {
                    Exception? viaBackup = await RecoveryScenario.CaptureAsync(() => cluster.Admin.RestoreFromBackupAsync(RecoveryScenario.RestoreRequest(source.Backup, "S", Vol), Ct));
                    Assert.IsType<DfsAlreadyExistsException>(viaBackup);
                    Exception? viaSnapshot = await RecoveryScenario.CaptureAsync(() => oldVolume.Snapshots.RestoreAsync("S", Vol, Ct));
                    Assert.IsType<DfsAlreadyExistsException>(viaSnapshot);

                    DfsVolumeInfo after = await oldVolume.GetInfoAsync(Ct);
                    Assert.Equal(source.Info.Id, after.Id);
                    Assert.Equal(source.Info.Epoch, after.Epoch);
                    Assert.Single(await cluster.Admin.ListVolumesAsync(Ct), v => v == Vol);
                    CommitToken flushed = await oldWriter.CommitAsync(Ct);
                    Assert.True(flushed.IsComparableWith(oldToken) && flushed.IsAtOrAfter(oldToken), "the old authority keeps its own history");
                    await RecoveryScenario.AssertFileBytesAsync(oldVolume.Files, "/docs/one.bin", staged, "the old authority committed its own writer", Ct);
                    break;
                }

                case PromotionCase.SameClusterSameNameAfterFencing:
                {
                    await cluster.Admin.DeleteVolumeAsync(Vol, Ct);
                    Exception? fenced = await RecoveryScenario.CaptureAsync(() => oldWriter.CommitAsync(Ct));
                    RecoveryScenario.AssertFailedWith(fenced, "a fenced authority must not accept the old writer's publication",
                        DfsErrorCode.VolumeNotFound, DfsErrorCode.LeaseLost, DfsErrorCode.EpochMismatch, DfsErrorCode.InvalidHandle);
                    Exception? oldWrite = await RecoveryScenario.CaptureAsync(() => oldVolume.Files.WriteAllBytesAsync("/fenced.bin", new byte[] { 1, 2, 3 }, Ct));
                    RecoveryScenario.AssertFailedWith(oldWrite, "writes to the fenced identity", DfsErrorCode.VolumeNotFound, DfsErrorCode.EpochMismatch);

                    DfsVolumeInfo restored = await cluster.Admin.RestoreFromBackupAsync(RecoveryScenario.RestoreRequest(source.Backup, "S", Vol), Ct);
                    RecoveryScenario.AssertRestoredIdentity(restored, source.Info, cluster.ClusterId, Vol);
                    Exception? stale = await RecoveryScenario.CaptureAsync(() => oldWriter.CommitAsync(Ct));
                    Assert.NotNull(stale);
                    if (fenced is DfsLeaseLostException)
                        Assert.True(oldWriter.IsFaulted, "lease loss must fault the old writer's stream (FSD §6.4)");
                    RecoveryScenario.AssertNotComparable(restored.LastCommit, oldToken, "old tokens cannot order the recovered identity");
                    VolumeContentFixture.AssertMatches(source.Expected, await RstHelpers.CaptureFreshAsync(cluster, Vol, "verifier", Ct),
                        "the recovered identity holds exactly S; nothing from the fenced writer");
                    break;
                }

                default:
                {
                    await using TestCluster recovery = await cluster.StartRecoveryClusterAsync(RstHelpers.RecoveryOptions(), Ct);
                    DfsVolumeInfo restored = await recovery.Admin.RestoreFromBackupAsync(RecoveryScenario.RestoreRequest(source.Backup, "S", Vol), Ct);
                    RecoveryScenario.AssertRestoredIdentity(restored, source.Info, recovery.ClusterId, Vol);
                    Assert.NotEqual(cluster.ClusterId, recovery.ClusterId);

                    CommitToken oldCommit = await oldWriter.CommitAsync(Ct);
                    await using DfsClient newClient = await recovery.ConnectAsync(new TestClientOptions { Name = "new-identity" }, Ct);
                    DfsVolume newVolume = newClient.GetVolume(Vol);
                    byte[] newBytes = TestData.Bytes(10_002, 4444);
                    CommitToken newCommit = await newVolume.Files.WriteAllBytesAsync("/new-identity.bin", newBytes, Ct);
                    RecoveryScenario.AssertNotComparable(oldCommit, newCommit, "two identities never share an order");

                    await Assert.ThrowsAsync<FileNotFoundException>(() => oldVolume.Files.ReadAllBytesAsync("/new-identity.bin", Ct));
                    await RecoveryScenario.AssertFileBytesAsync(newVolume.Files, "/docs/one.bin", source.Expected.Files["/docs/one.bin"].Content,
                        "the old authority's later commit must not merge into the new identity", Ct);
                    await RecoveryScenario.AssertFileBytesAsync(oldVolume.Files, "/docs/one.bin", staged, "the old authority committed its own writer", Ct);

                    Exception? pinned = await RecoveryScenario.CaptureAsync(() => recovery.ConnectAsync(new TestClientOptions { Name = "pinned-old", ExpectedClusterId = cluster.ClusterId }, Ct));
                    DfsClusterIdentityException mismatch = Assert.IsType<DfsClusterIdentityException>(pinned);
                    Assert.Equal(DfsErrorCode.ClusterMismatch, mismatch.ErrorCode);
                    break;
                }
            }

            await RecoveryScenario.CaptureAsync(async () => await oldWriter.DisposeAsync());
        }, Ct);

    // ---------------------------------------------------------------------------------------------------------------
    // RST-012
    // ---------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Snapshots with an expiry, a hold and default retention are published; the backup host and checkpoints disclose the
    /// exact retention of each; before expiry S1 is protected and restorable; history the host never held fails precisely
    /// with SnapshotNotFound (no substitution); after the destination clock passes the expiry, S1 is either gone (requests
    /// fail precisely) or disclosed as no longer protected and still restored exactly, while held/later points stay restorable.
    /// </summary>
    [Fact]
    [Catalog("RST-012", "I", "INV-08", "INV-10", "INV-12", "INV-13", "INV-19")]
    public Task RST_012_RetentionExpiryDisclosed() =>
        ClusterTest.RunAsync(RstHelpers.SourceOptions(), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "app" }, Ct);
            DfsVolume volume = client.GetVolume(Vol);
            ExpectedVolumeContent e1 = await VolumeContentFixture.PopulateAsync(volume, 12_000, Ct);
            TestBackupNode backup = cluster.BackupNodes[0];
            await backup.RegisterAsync(Vol, RecoveryScenario.ManualSchedule);

            // Generous margin: everything up to the early restore must happen before the expiry; expiry is then crossed by
            // advancing the destination clock, never by waiting.
            DateTimeOffset expiry = backup.Clock.GetUtcNow() + TimeSpan.FromMinutes(10);
            DfsSnapshotInfo s1 = await volume.Snapshots.CreateAsync("S1", new DfsSnapshotOptions { Retention = new DfsRetention(expiry) }, Ct);
            DfsBackupCheckpoint c1 = await RecoveryScenario.PublishAsync(volume, backup, s1, Ct);
            Assert.False(c1.Held);
            Assert.True(c1.RetentionExpiryUtc is { } reportedExpiry && (reportedExpiry - expiry).Duration() <= TimeSpan.FromSeconds(1),
                $"the checkpoint must report the actual retention expiry {expiry:O}, reported {c1.RetentionExpiryUtc:O}");

            ExpectedVolumeContent e2 = e1.Clone();
            byte[] changed = TestData.Bytes(12_001, 999);
            await volume.Files.WriteAllBytesAsync("/docs/one.bin", changed, Ct);
            e2.SetFile(e2.Files["/docs/one.bin"] with { Content = changed });
            DfsSnapshotInfo s2 = await volume.Snapshots.CreateAsync("S2", new DfsSnapshotOptions { Retention = new DfsRetention(null, Hold: true) }, Ct);
            DfsBackupCheckpoint c2 = await RecoveryScenario.PublishAsync(volume, backup, s2, Ct);
            Assert.True(c2.Held, "a held snapshot's checkpoint discloses the hold");

            ExpectedVolumeContent e3 = e2.Clone();
            await volume.Files.DeleteAsync(VolumeContentFixture.TextPath, Ct);
            e3.RemoveFile(VolumeContentFixture.TextPath);
            DfsSnapshotInfo s3 = await volume.Snapshots.CreateAsync("S3", null, Ct);
            await RecoveryScenario.PublishAsync(volume, backup, s3, Ct);

            await using DfsClient retentionAdmin = await cluster.ConnectBackupAsync(backup.Index,
                new TestClientOptions { Name = "bk-retention", Credentials = backup.RetentionAdminCredentials }, Ct);
            DfsSnapshotClient held = retentionAdmin.GetVolume(Vol).Snapshots;
            IReadOnlyList<DfsSnapshotInfo> listed = await held.ListAsync(Ct);
            DfsSnapshotInfo l1 = Assert.Single(listed, s => s.Name == "S1");
            Assert.True(l1.Retention.ExpiresUtc is { } listedExpiry && (listedExpiry - expiry).Duration() <= TimeSpan.FromSeconds(1),
                $"the backup host must disclose S1's expiry {expiry:O}, listed {l1.Retention.ExpiresUtc:O}");
            Assert.False(l1.Retention.Hold);
            Assert.True(Assert.Single(listed, s => s.Name == "S2").Retention.Hold);
            Assert.Single(listed, s => s.Name == "S3");

            // Before expiry S1 (and the held S2) are protected against the source's export credentials (FSD 12.4): the
            // client session is refused, or the deletion is denied/read-only.
            foreach (string protectedName in new[] { "S1", "S2" })
            {
                Exception? exportDelete = await RecoveryScenario.CaptureAsync(async () =>
                {
                    await using DfsClient exporter = await cluster.ConnectBackupAsync(backup.Index,
                        new TestClientOptions { Name = "bk-export", Credentials = backup.ExportCredentials }, Ct);
                    await exporter.GetVolume(Vol).Snapshots.DeleteAsync(protectedName, Ct);
                });
                Assert.True(exportDelete is UnauthorizedAccessException or DfsReadOnlyException,
                    $"export credentials must not delete protected {protectedName}, got {(exportDelete is null ? "success" : exportDelete.GetType().Name)}");
            }

            Assert.Equal(new[] { "S1", "S2", "S3" }, (await held.ListAsync(Ct)).Select(s => s.Name).Order(StringComparer.Ordinal));
            DfsVolumeInfo early = await cluster.Admin.RestoreFromBackupAsync(RecoveryScenario.RestoreRequest(backup, "S1", "r1-early"), Ct);
            Assert.Equal("r1-early", early.Name);
            VolumeContentFixture.AssertMatches(e1, await RstHelpers.CaptureFreshAsync(cluster, "r1-early", "v-r1", Ct), "S1 restored before its expiry");

            // Requested history the host never held fails precisely, with no substitution by the latest point (S3).
            DfsSnapshotInfo unsynced = await volume.Snapshots.CreateAsync("S4-unsynced", null, Ct);
            DfsBackupNotReachedException notHeld = await Assert.ThrowsAsync<DfsBackupNotReachedException>(
                () => volume.Backups.AwaitBackupSnapshotAsync(backup.TargetName, unsynced.Name, TimeSpan.FromSeconds(2), Ct));
            Assert.Equal("S3", notHeld.LastPublished?.SnapshotName);
            Exception? neverHeld = await RecoveryScenario.CaptureAsync(() => cluster.Admin.RestoreFromBackupAsync(RecoveryScenario.RestoreRequest(backup, unsynced.Name, "r4"), Ct));
            RecoveryScenario.AssertFailedWith(neverHeld, "history the backup never held; no substitution by a later point", DfsErrorCode.SnapshotNotFound);
            Assert.DoesNotContain("r4", await cluster.Admin.ListVolumesAsync(Ct));

            // Cross the expiry on the destination clock. Expiry ends protection; it does not oblige the host to delete. The
            // retention admin's delete may succeed, find S1 already reclaimed, or be refused by a host whose SDK surface is
            // read-only (BAK-015) — but never as "protected" any more.
            backup.Clock.Advance(TimeSpan.FromMinutes(15));
            Exception? deleteExpired = await RecoveryScenario.CaptureAsync(() => held.DeleteAsync("S1", Ct));
            if (deleteExpired is not null && deleteExpired is not DfsReadOnlyException)
                RecoveryScenario.AssertFailedWith(deleteExpired, "an expired snapshot is deletable by the retention admin or already reclaimed", DfsErrorCode.SnapshotNotFound);
            bool removed = deleteExpired is null || RecoveryScenario.CodeOf(deleteExpired) == DfsErrorCode.SnapshotNotFound;

            DfsSnapshotInfo? stillListed = (await held.ListAsync(Ct)).SingleOrDefault(s => s.Name == "S1");
            if (removed)
                Assert.True(stillListed is null, "a deleted/reclaimed recovery point must not be reported as held");
            Exception? late = await RecoveryScenario.CaptureAsync(() => cluster.Admin.RestoreFromBackupAsync(RecoveryScenario.RestoreRequest(backup, "S1", "r1-late"), Ct));
            if (stillListed is not null)
            {
                // Retained beyond its expiry by host policy: it must disclose that it is no longer protected, and while it is
                // still available a restore must yield exactly S1 (or fail precisely if reclaimed in the meantime).
                Assert.False(stillListed.Retention.Hold, "an expired, never-held point must not be reported as held");
                Assert.True(stillListed.Retention.ExpiresUtc is null || stillListed.Retention.ExpiresUtc <= backup.Clock.GetUtcNow(),
                    $"S1 is listed after its expiry {expiry:O} but still discloses protection until {stillListed.Retention.ExpiresUtc:O}");
                if (late is null)
                    VolumeContentFixture.AssertMatches(e1, await RstHelpers.CaptureFreshAsync(cluster, "r1-late", "v-r1-late", Ct), "S1 restored after expiry while still held");
                else
                    RecoveryScenario.AssertFailedWith(late, "S1 reclaimed between listing and restore", DfsErrorCode.SnapshotNotFound);
            }
            else
            {
                RecoveryScenario.AssertFailedWith(await RecoveryScenario.CaptureAsync(() => held.GetAsync("S1", Ct)), "S1 is no longer held", DfsErrorCode.SnapshotNotFound);
                RecoveryScenario.AssertFailedWith(late, "requested history is no longer available; no substitution by a later point", DfsErrorCode.SnapshotNotFound);
            }

            if (late is not null)
                Assert.DoesNotContain("r1-late", await cluster.Admin.ListVolumesAsync(Ct));
            TestContext.Current.TestOutputHelper?.WriteLine($"S1 after expiry: {(stillListed is null ? "removed" : "retained unprotected")}, delete outcome {RecoveryScenario.CodeOf(deleteExpired)}");

            IReadOnlyList<DfsSnapshotInfo> remaining = await held.ListAsync(Ct);
            Assert.True(Assert.Single(remaining, s => s.Name == "S2").Retention.Hold, "the held S2 survives S1's expiry with its hold disclosed");
            Assert.Single(remaining, s => s.Name == "S3");
            await cluster.Admin.RestoreFromBackupAsync(RecoveryScenario.RestoreRequest(backup, "S2", "r2"), Ct);
            VolumeContentFixture.AssertMatches(e2, await RstHelpers.CaptureFreshAsync(cluster, "r2", "v-r2", Ct), "held S2 after S1 expired");
            await cluster.Admin.RestoreFromBackupAsync(RecoveryScenario.RestoreRequest(backup, "S3", "r3"), Ct);
            VolumeContentFixture.AssertMatches(e3, await RstHelpers.CaptureFreshAsync(cluster, "r3", "v-r3", Ct), "S3 after S1 expired");
        }, Ct);

    // ---------------------------------------------------------------------------------------------------------------
    // RST-013
    // ---------------------------------------------------------------------------------------------------------------

    /// <summary>How the single backup host is lost in RST-013.</summary>
    public enum BackupHostLoss
    {
        /// <summary>The host crashes and its only disk is destroyed; it stays down.</summary>
        DiskDestroyedHostDown,

        /// <summary>The host's only disk is destroyed and the host restarts empty.</summary>
        DiskDestroyedHostRestartedEmpty,
    }

    /// <summary>
    /// Losing the single backup host's only disk is reported as exactly that: one target, now offline or reseed-required,
    /// no published recovery point claimed for the lost data, restore from it fails explicitly, and live service continues.
    /// </summary>
    [Theory]
    [InlineData(BackupHostLoss.DiskDestroyedHostDown)]
    [InlineData(BackupHostLoss.DiskDestroyedHostRestartedEmpty)]
    [Catalog("RST-013", "I", "INV-08", "INV-10", "INV-12", "INV-13", "INV-19")]
    public Task RST_013_BackupHostItselfFails(BackupHostLoss loss) =>
        ClusterTest.RunAsync(RstHelpers.SourceOptions(), async cluster =>
        {
            BackedUpSource source = await RecoveryScenario.CreateBackedUpSourceAsync(cluster, "S", 13_000, Ct);
            DfsVolume volume = source.Volume;
            TestBackupNode backup = source.Backup;
            DfsBackupStatus before = Assert.Single((await cluster.Admin.GetVolumeStatusAsync(Vol, Ct)).Backups);
            Assert.Equal(backup.TargetName, before.Target);
            Assert.Equal("S", before.LatestPublished?.SnapshotName);

            await backup.CrashAsync();
            await backup.DestroyDiskAsync();
            if (loss == BackupHostLoss.DiskDestroyedHostRestartedEmpty)
            {
                // The host itself discloses the loss: nothing published, nothing listed, nothing restorable.
                await backup.RestartAsync();
                Assert.Empty(await backup.Inspect.GetPublishedSnapshotsAsync(Vol));
                await using (DfsClient reader = await RstHelpers.ConnectBackupReaderAsync(cluster, backup, "bk-reader", Ct))
                {
                    Exception? list = await RecoveryScenario.CaptureAsync(async () =>
                        Assert.DoesNotContain(await reader.GetVolume(Vol).Snapshots.ListAsync(Ct), s => s.Name == "S"));
                    if (list is not null && list is not DfsException)
                        throw list;
                }

                Exception? emptyRestore = await RecoveryScenario.CaptureAsync(() => cluster.Admin.RestoreFromBackupAsync(RecoveryScenario.RestoreRequest(backup, "S", "from-lost-host"), Ct));
                RecoveryScenario.AssertFailedWith(emptyRestore, "restore of a snapshot the emptied host no longer holds",
                    DfsErrorCode.SnapshotNotFound, DfsErrorCode.BackupTargetNotFound, DfsErrorCode.VolumeNotFound);
            }
            else
            {
                Exception? downRestore = await RecoveryScenario.CaptureAsync(() => cluster.Admin.RestoreFromBackupAsync(RecoveryScenario.RestoreRequest(backup, "S", "from-lost-host"), Ct));
                RecoveryScenario.AssertFailedWith(downRestore, "restore from a destroyed, offline backup host",
                    DfsErrorCode.NodeUnavailable, DfsErrorCode.Unavailable, DfsErrorCode.BackupTargetNotFound, DfsErrorCode.Timeout);
            }

            Assert.DoesNotContain("from-lost-host", await cluster.Admin.ListVolumesAsync(Ct));

            // Live service is independent of the lost host.
            ExpectedVolumeContent live = source.Expected.Clone();
            byte[] afterLoss = TestData.Bytes(13_001, 9000);
            await volume.Files.WriteAllBytesAsync("/after-loss.bin", afterLoss, Ct);
            live.SetFile(new ExpectedFile("/after-loss.bin", afterLoss, null, new DfsInodeAccess(), []));

            // The source learns about the loss on its next contact with the target (manual schedule: provoke one). It must
            // then stop claiming the lost S under the old lineage: offline/failed when the host is down; reseed-required,
            // failed, nothing published or a fresh lineage when the host came back empty — unless the contact itself
            // re-transferred S, in which case the claim is true again and the host must genuinely hold S.
            Exception? sync = await RecoveryScenario.CaptureAsync(() => volume.Backups.RequestSyncAsync(backup.TargetName, null, Ct));
            Assert.True(sync is null or DfsException or TimeoutException, $"requesting a sync to a lost target must succeed or fail with a typed error, got {sync}");
            DfsBackupStatus? reported = null;
            await cluster.WaitUntilAsync(async () =>
            {
                reported = await volume.Backups.GetStatusAsync(backup.TargetName, Ct);
                if (loss == BackupHostLoss.DiskDestroyedHostDown)
                    return reported.State is DfsBackupState.Offline or DfsBackupState.Failed;
                bool disclosesLoss = reported.ReseedRequired || reported.State is DfsBackupState.ReseedRequired or DfsBackupState.Failed
                    || reported.LatestPublished is null || reported.LineageId != before.LineageId;
                bool republished = reported.LatestPublished is { SnapshotName: "S" } && (await backup.Inspect.GetPublishedSnapshotsAsync(Vol)).Contains("S");
                return disclosesLoss || republished;
            }, TimeSpan.FromSeconds(30), "the source must report the single backup host's loss");
            Assert.NotNull(reported);
            if (loss == BackupHostLoss.DiskDestroyedHostRestartedEmpty)
            {
                bool claimsS = reported.LatestPublished is { SnapshotName: "S" } && !reported.ReseedRequired && reported.LineageId == before.LineageId;
                if (claimsS)
                {
                    // Only truthful if the host really holds a complete S again.
                    Assert.Contains("S", await backup.Inspect.GetPublishedSnapshotsAsync(Vol));
                    await using DfsClient reader = await RstHelpers.ConnectBackupReaderAsync(cluster, backup, "bk-reader-2", Ct);
                    await using DfsSnapshotView view = await reader.GetVolume(Vol).Snapshots.OpenAsync("S", Ct);
                    VolumeContentFixture.AssertMatches(source.Expected, await VolumeContentFixture.CaptureAsync(view, Ct),
                        "a status that still claims S after the disk loss must be backed by a complete re-transferred S");
                }
            }

            // Exactly one target is reported: the backup role is not itself replicated.
            DfsVolumeStatus after = await cluster.Admin.GetVolumeStatusAsync(Vol, Ct);
            DfsBackupStatus only = Assert.Single(after.Backups);
            Assert.Equal(backup.TargetName, only.Target);
            Assert.Equal(new[] { backup.TargetName }, await volume.Backups.ListTargetsAsync(Ct));

            VolumeContentFixture.AssertMatches(live, await VolumeContentFixture.CaptureAsync(volume, Ct), "live service is independent of the lost backup host");
        }, Ct);

    // ---------------------------------------------------------------------------------------------------------------
    // RST-014
    // ---------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Follows the recovery runbook on clean hosts: real host processes started from the binaries with generated
    /// certificates, new cluster secret and identity, then restore using only the backup endpoint and backup credentials;
    /// records (and fails on) any contact with source services.
    /// </summary>
    [Fact]
    [Catalog("RST-014", "I", "INV-08", "INV-10", "INV-12", "INV-13", "INV-19")]
    public Task RST_014_RecoveryRunbookWorksFromCleanHost() =>
        ClusterTest.RunAsync(RstHelpers.SourceOptions(), async cluster =>
        {
            BackedUpSource source = await RecoveryScenario.CreateBackedUpSourceAsync(cluster, "S", 14_000, Ct);
            IReadOnlyDictionary<string, long> baseline = await RecoveryScenario.DestroySourceAsync(cluster, source.Client);

            await using TestCluster recovery = await cluster.StartRecoveryClusterAsync(RstHelpers.RecoveryOptions(outOfProcess: true), Ct);
            Assert.True(recovery.Options.OutOfProcess);
            Assert.All(recovery.Nodes, n =>
            {
                Assert.Equal(TestNodeState.Running, n.State);
                Assert.Null(n.Host);
                Assert.StartsWith("quic://", n.Endpoint, StringComparison.Ordinal);
            });
            Assert.NotEqual(cluster.ClusterId, recovery.ClusterId);
            Assert.False(string.Equals(cluster.DataRoot, recovery.DataRoot, StringComparison.OrdinalIgnoreCase));

            DfsVolumeInfo restored = await recovery.Admin.RestoreFromBackupAsync(RecoveryScenario.RestoreRequest(source.Backup, "S", Vol), Ct);
            RecoveryScenario.AssertRestoredIdentity(restored, source.Info, recovery.ClusterId, Vol);

            await using DfsClient client = await recovery.ConnectAsync(new TestClientOptions { Name = "operator" }, Ct);
            DfsVolume volume = client.GetVolume(Vol);
            VolumeContentFixture.AssertMatches(source.Expected, await VolumeContentFixture.CaptureAsync(volume, Ct), "runbook restore on clean hosts");
            CommitToken w = await volume.Files.WriteAllBytesAsync("/runbook-check.bin", TestData.Bytes(14_001, 1234), Ct);
            DfsSnapshotInfo snap = await volume.Snapshots.CreateAsync("post-recovery", null, Ct);
            Assert.True(snap.Boundary.IsAtOrAfter(w));

            ITestOutputHelper? log = TestContext.Current.TestOutputHelper;
            foreach ((string node, long before) in baseline)
                log?.WriteLine($"source node {node}: {cluster.Network.CountAttemptsTo(node) - before} contact attempt(s) during recovery");
            RecoveryScenario.AssertNoSourceContact(cluster, baseline, "while following the runbook");
        }, Ct);

    // ---------------------------------------------------------------------------------------------------------------
    // RST-015
    // ---------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// After a restore the recovered cluster creates, modifies, deletes, snapshots, transacts and backs up new data under
    /// its new epoch: new tokens order after the restore point, the new backup holds the new history exactly, and the old
    /// backup still holds S unchanged and distinguishable.
    /// </summary>
    [Fact]
    [Catalog("RST-015", "I", "INV-08", "INV-10", "INV-12", "INV-13", "INV-19")]
    public Task RST_015_RecoveredClusterCanOperateAndBackUp() =>
        ClusterTest.RunAsync(RstHelpers.SourceOptions(), async cluster =>
        {
            BackedUpSource source = await RecoveryScenario.CreateBackedUpSourceAsync(cluster, "S", 15_000, Ct);
            CommitToken oldToken = source.Info.LastCommit;
            IReadOnlyDictionary<string, long> baseline = await RecoveryScenario.DestroySourceAsync(cluster, source.Client);

            await using TestCluster recovery = await cluster.StartRecoveryClusterAsync(RstHelpers.RecoveryOptions(backups: 1), Ct);
            DfsVolumeInfo restored = await recovery.Admin.RestoreFromBackupAsync(RecoveryScenario.RestoreRequest(source.Backup, "S", Vol), Ct);
            RecoveryScenario.AssertRestoredIdentity(restored, source.Info, recovery.ClusterId, Vol);

            await using DfsClient client = await recovery.ConnectAsync(new TestClientOptions { Name = "app" }, Ct);
            DfsVolume volume = client.GetVolume(Vol);
            ExpectedVolumeContent next = source.Expected.Clone();
            var tokens = new List<CommitToken>();
            await volume.Directories.CreateDirectoryAsync("/new", Ct);
            next.AddDirectory("/new");
            byte[] created = TestData.Bytes(15_001, 200_000);
            tokens.Add(await volume.Files.WriteAllBytesAsync("/new/created.bin", created, Ct));
            next.SetFile(new ExpectedFile("/new/created.bin", created, null, new DfsInodeAccess(), []));
            byte[] appended = TestData.Bytes(15_002, 777);
            tokens.Add(await volume.Files.AppendAllBytesAsync("/docs/one.bin", appended, Ct));
            next.SetFile(next.Files["/docs/one.bin"] with { Content = [.. next.Files["/docs/one.bin"].Content, .. appended] });
            tokens.Add(await volume.Files.DeleteAsync(VolumeContentFixture.TextPath, Ct));
            next.RemoveFile(VolumeContentFixture.TextPath);
            await using (DfsTransaction tx = await volume.Transactions.BeginAsync(null, Ct))
            {
                await tx.Files.MoveAsync("/new/created.bin", "/new/moved.bin", false, Ct);
                await tx.Directories.CreateDirectoryAsync("/new/tx-dir", Ct);
                tokens.Add(await tx.CommitAsync(Ct));
            }

            next.SetFile(next.Files["/new/created.bin"] with { Path = "/new/moved.bin" });
            next.RemoveFile("/new/created.bin");
            next.AddDirectory("/new/tx-dir");
            foreach (CommitToken t in tokens)
            {
                Assert.True(t.IsComparableWith(restored.LastCommit) && t.IsAtOrAfter(restored.LastCommit), "new commits order after the restore point");
                RecoveryScenario.AssertNotComparable(t, oldToken, "new commits are distinguishable from the old history");
            }

            for (int i = 1; i < tokens.Count; i++)
                Assert.True(tokens[i].CompareTo(tokens[i - 1]) > 0);

            TestBackupNode newBackup = recovery.BackupNodes[0];
            await newBackup.RegisterAsync(Vol, RecoveryScenario.ManualSchedule);
            DfsSnapshotInfo n1 = await volume.Snapshots.CreateAsync("N1", null, Ct);
            Assert.True(n1.Boundary.IsAtOrAfter(tokens[^1]));
            DfsBackupCheckpoint checkpoint = await RecoveryScenario.PublishAsync(volume, newBackup, n1, Ct);
            Assert.True(checkpoint.CoveredCommit.IsComparableWith(restored.LastCommit));
            RecoveryScenario.AssertNotComparable(checkpoint.CoveredCommit, source.Checkpoint.CoveredCommit, "the two backup lineages stay distinguishable");

            VolumeContentFixture.AssertMatches(next, await VolumeContentFixture.CaptureAsync(volume, Ct), "recovered cluster after new work");
            await recovery.WaitForReplicationAsync(Vol, Ct);
            await recovery.Inspect.AssertReplicasConvergedAsync(Vol);

            await using (DfsClient newReader = await recovery.ConnectBackupAsync(0, new TestClientOptions { Name = "new-bk", Credentials = newBackup.ReaderCredentials }, Ct))
            await using (DfsSnapshotView view = await newReader.GetVolume(Vol).Snapshots.OpenAsync("N1", Ct))
                VolumeContentFixture.AssertMatches(next, await VolumeContentFixture.CaptureAsync(view, Ct), "new backup lineage holds the new history");

            await using (DfsClient oldReader = await RstHelpers.ConnectBackupReaderAsync(cluster, source.Backup, "old-bk", Ct))
            await using (DfsSnapshotView view = await oldReader.GetVolume(Vol).Snapshots.OpenAsync("S", Ct))
            {
                Assert.Equal(source.Snapshot.Boundary, view.Info.Boundary);
                VolumeContentFixture.AssertMatches(source.Expected, await VolumeContentFixture.CaptureAsync(view, Ct), "old backup still holds S unchanged");
            }

            RecoveryScenario.AssertNoSourceContact(cluster, baseline, "while operating the recovered cluster");
        }, Ct);

    // ---------------------------------------------------------------------------------------------------------------
    // RST-016
    // ---------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// An ApplicationConsistent snapshot carries its adapter identity and restore requirements through backup and restore;
    /// application recovery executes each declared requirement (version check, checkpoint load, WAL replay) against the
    /// volume restored on a clean recovery cluster after the source was destroyed (test plan §9.8) and is declared complete
    /// only when every step reports success and yields the prepared state. A failing adapter never yields a (downgraded)
    /// snapshot.
    /// </summary>
    [Fact]
    [Catalog("RST-016", "I", "INV-08", "INV-10", "INV-12", "INV-13", "INV-19")]
    public Task RST_016_ApplicationRestoreRequirementsEnforced() =>
        ClusterTest.RunAsync(RstHelpers.SourceOptions(), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "app" }, Ct);
            DfsVolume volume = client.GetVolume(Vol);
            TestBackupNode backup = cluster.BackupNodes[0];
            await backup.RegisterAsync(Vol, RecoveryScenario.ManualSchedule);

            var app = new MiniWalApplication(volume);
            await app.InitializeAsync(Ct);
            for (int i = 0; i < 40; i++)
                await app.SetAsync("k" + (i % 7).ToString(CultureInfo.InvariantCulture), "v" + i.ToString(CultureInfo.InvariantCulture), Ct);
            await app.CheckpointDatabaseAsync(Ct);
            for (int i = 40; i < 55; i++)
                await app.SetAsync("k" + (i % 9).ToString(CultureInfo.InvariantCulture), "v" + i.ToString(CultureInfo.InvariantCulture), Ct);

            var adapter = new WalCheckpointAdapter(app, failPrepare: false);
            DfsSnapshotInfo snapshot = await volume.Snapshots.CreateAsync("S-app", new DfsSnapshotOptions { Adapters = [adapter] }, Ct);
            IReadOnlyDictionary<string, string> preparedState = adapter.PreparedState ?? throw new InvalidOperationException("prepare did not run");
            Assert.Equal(DfsConsistencyLabel.ApplicationConsistent, snapshot.Label);
            DfsAdapterRecord record = Assert.Single(snapshot.Adapters);
            AssertAdapterRecord(adapter, record);
            Assert.Null(snapshot.AdapterResumeFailure);
            Assert.Equal((1, 1, 0), (adapter.Prepared, adapter.Resumed, adapter.Aborted));

            // After resume the application keeps writing; those writes are not part of S-app.
            for (int i = 55; i < 70; i++)
                await app.SetAsync("k" + (i % 5).ToString(CultureInfo.InvariantCulture), "late" + i.ToString(CultureInfo.InvariantCulture), Ct);

            var failing = new WalCheckpointAdapter(app, failPrepare: true);
            Exception? failed = await RecoveryScenario.CaptureAsync(() => volume.Snapshots.CreateAsync("S-fail", new DfsSnapshotOptions { Adapters = [failing] }, Ct));
            DfsSnapshotAdapterException adapterFailure = Assert.IsType<DfsSnapshotAdapterException>(failed);
            Assert.Equal(failing.Id, adapterFailure.AdapterId);
            Assert.Contains("prepare", adapterFailure.Phase, StringComparison.OrdinalIgnoreCase);
            Assert.True(failing.Aborted + failing.Resumed >= 1, "a failed capture must release the application pause");
            Assert.DoesNotContain(await volume.Snapshots.ListAsync(Ct), s => s.Name == "S-fail");

            await RecoveryScenario.PublishAsync(volume, backup, snapshot, Ct);
            DfsAdapterRecord backupRecord;
            await using (DfsClient reader = await RstHelpers.ConnectBackupReaderAsync(cluster, backup, "bk-reader", Ct))
            {
                DfsSnapshotInfo onBackup = await reader.GetVolume(Vol).Snapshots.GetAsync("S-app", Ct);
                Assert.Equal(DfsConsistencyLabel.ApplicationConsistent, onBackup.Label);
                backupRecord = Assert.Single(onBackup.Adapters);
                AssertAdapterRecord(adapter, backupRecord);
                Assert.DoesNotContain(await reader.GetVolume(Vol).Snapshots.ListAsync(Ct), s => s.Name == "S-fail");
            }

            // Test plan §9.8: the source is gone; restore on a clean recovery cluster that never contacts a source node.
            IReadOnlyDictionary<string, long> baseline = await RecoveryScenario.DestroySourceAsync(cluster, client);
            await using TestCluster recovery = await cluster.StartRecoveryClusterAsync(RstHelpers.RecoveryOptions(), Ct);
            DfsVolumeInfo restored = await recovery.Admin.RestoreFromBackupAsync(RecoveryScenario.RestoreRequest(backup, "S-app", Vol), Ct);
            Assert.Equal(Vol, restored.Name);
            Assert.Equal(recovery.ClusterId, restored.LastCommit.ClusterId);
            await using DfsClient verifier = await recovery.ConnectAsync(new TestClientOptions { Name = "app-recovery" }, Ct);
            DfsVolume restoredVolume = verifier.GetVolume(Vol);

            // Application recovery is driven only by what the backup host disclosed (the source is gone).
            IReadOnlyDictionary<string, bool> steps = await MiniWalApplication.RecoverAsync(restoredVolume, backupRecord, Ct);
            Assert.Equal(backupRecord.RestoreRequirements.Keys.Order(StringComparer.Ordinal), steps.Keys.Order(StringComparer.Ordinal));
            Assert.All(steps, s => Assert.True(s.Value, $"application recovery step '{s.Key}' did not succeed"));
            IReadOnlyDictionary<string, string> recovered = await MiniWalApplication.ReadRecoveredStateAsync(restoredVolume, Ct);
            Assert.Equal(preparedState.OrderBy(p => p.Key, StringComparer.Ordinal), recovered.OrderBy(p => p.Key, StringComparer.Ordinal));
            Assert.DoesNotContain(recovered.Values, v => v.StartsWith("late", StringComparison.Ordinal));

            var stricter = new DfsAdapterRecord(backupRecord.Id, backupRecord.Version,
                new Dictionary<string, string>(backupRecord.RestoreRequirements, StringComparer.Ordinal) { [MiniWalApplication.MinVersionRequirement] = "99" });
            IReadOnlyDictionary<string, bool> refused = await MiniWalApplication.RecoverAsync(restoredVolume, stricter, Ct);
            Assert.False(refused[MiniWalApplication.MinVersionRequirement], "an unsatisfied restore requirement must be reported, not skipped");
            RecoveryScenario.AssertNoSourceContact(cluster, baseline, "during application-consistent restore and recovery");
        }, Ct);

    private static void AssertAdapterRecord(WalCheckpointAdapter adapter, DfsAdapterRecord record)
    {
        Assert.Equal(adapter.Id, record.Id);
        Assert.Equal(adapter.Version, record.Version);
        Assert.Equal(adapter.RestoreRequirements.OrderBy(p => p.Key, StringComparer.Ordinal), record.RestoreRequirements.OrderBy(p => p.Key, StringComparer.Ordinal));
    }

    /// <summary>A tiny write-ahead-logged key/value application stored in the volume (RST-016 oracle).</summary>
    private sealed class MiniWalApplication(DfsVolume volume)
    {
        public const string Database = "/app/db.txt";
        public const string Wal = "/app/wal.log";
        public const string CheckpointFile = "/app/checkpoint.txt";
        public const string RecoveredFile = "/app/recovered.txt";
        public const string MinVersionRequirement = "min-app-version";
        public const int AppVersion = 2;

        private readonly SortedDictionary<string, string> _state = new(StringComparer.Ordinal);

        public IReadOnlyDictionary<string, string> State => _state;

        public async Task InitializeAsync(CancellationToken ct)
        {
            await volume.Directories.CreateDirectoryAsync("/app", ct);
            await volume.Files.WriteAllTextAsync(Database, string.Empty, null, ct);
            await volume.Files.WriteAllTextAsync(Wal, string.Empty, null, ct);
        }

        public async Task SetAsync(string key, string value, CancellationToken ct)
        {
            await volume.Files.AppendAllTextAsync(Wal, $"set {key} {value}\n", null, ct);
            _state[key] = value;
        }

        public async Task CheckpointDatabaseAsync(CancellationToken ct)
        {
            await volume.Files.WriteAllTextAsync(Database, Serialize(_state), null, ct);
            long walLength = (await volume.Files.GetInfoAsync(Wal, ct)).Length;
            await volume.Files.WriteAllTextAsync(CheckpointFile, "db-wal-offset=" + walLength.ToString(CultureInfo.InvariantCulture) + "\n", null, ct);
        }

        /// <summary>Writes the prepare-time marker: the WAL length that the recovered state must replay up to.</summary>
        public async Task<long> MarkPreparedAsync(CancellationToken ct)
        {
            long walLength = (await volume.Files.GetInfoAsync(Wal, ct)).Length;
            string dbMarker = await volume.Files.ReadAllTextAsync(CheckpointFile, null, ct);
            await volume.Files.WriteAllTextAsync(CheckpointFile, dbMarker.TrimEnd('\n') + "\nprepared-wal-length=" + walLength.ToString(CultureInfo.InvariantCulture) + "\n", null, ct);
            return walLength;
        }

        public static async Task<IReadOnlyDictionary<string, bool>> RecoverAsync(DfsVolume restored, DfsAdapterRecord record, CancellationToken ct)
        {
            var steps = new SortedDictionary<string, bool>(StringComparer.Ordinal);
            SortedDictionary<string, string>? state = null;
            Dictionary<string, long>? checkpoint = null;
            foreach ((string requirement, string argument) in record.RestoreRequirements.OrderBy(p => p.Key, StringComparer.Ordinal))
            {
                switch (requirement)
                {
                    case MinVersionRequirement:
                        steps[requirement] = int.Parse(argument, CultureInfo.InvariantCulture) <= AppVersion;
                        break;
                    case "checkpoint":
                        checkpoint = (await restored.Files.ReadAllTextAsync(argument, null, ct))
                            .Split('\n', StringSplitOptions.RemoveEmptyEntries)
                            .Select(l => l.Split('='))
                            .ToDictionary(p => p[0], p => long.Parse(p[1], CultureInfo.InvariantCulture), StringComparer.Ordinal);
                        steps[requirement] = checkpoint.ContainsKey("db-wal-offset") && checkpoint.ContainsKey("prepared-wal-length");
                        break;
                    case "replay-wal":
                        steps[requirement] = false;
                        break;
                    default:
                        steps[requirement] = false;
                        break;
                }
            }

            if (record.RestoreRequirements.TryGetValue("replay-wal", out string? walPath) && checkpoint is not null && steps.GetValueOrDefault("checkpoint"))
            {
                state = Deserialize(await restored.Files.ReadAllTextAsync(Database, null, ct));
                byte[] wal = await restored.Files.ReadAllBytesAsync(walPath, ct);
                long from = checkpoint["db-wal-offset"];
                long to = checkpoint["prepared-wal-length"];
                bool ok = from <= to && to <= wal.LongLength;
                if (ok)
                {
                    foreach (string line in Encoding.UTF8.GetString(wal, (int)from, (int)(to - from)).Split('\n', StringSplitOptions.RemoveEmptyEntries))
                    {
                        string[] parts = line.Split(' ');
                        ok &= parts.Length == 3 && parts[0] == "set";
                        if (ok)
                            state[parts[1]] = parts[2];
                    }
                }

                steps["replay-wal"] = ok;
                if (ok && steps.Values.All(v => v))
                    await restored.Files.WriteAllTextAsync(RecoveredFile, Serialize(state), null, ct);
            }

            return steps;
        }

        public static async Task<IReadOnlyDictionary<string, string>> ReadRecoveredStateAsync(DfsVolume restored, CancellationToken ct) =>
            Deserialize(await restored.Files.ReadAllTextAsync(RecoveredFile, null, ct));

        private static string Serialize(IReadOnlyDictionary<string, string> state) =>
            string.Concat(state.OrderBy(p => p.Key, StringComparer.Ordinal).Select(p => p.Key + "=" + p.Value + "\n"));

        private static SortedDictionary<string, string> Deserialize(string text)
        {
            var result = new SortedDictionary<string, string>(StringComparer.Ordinal);
            foreach (string line in text.Split('\n', StringSplitOptions.RemoveEmptyEntries))
            {
                int eq = line.IndexOf('=', StringComparison.Ordinal);
                result[line[..eq]] = line[(eq + 1)..];
            }

            return result;
        }
    }

    /// <summary>Snapshot adapter for <see cref="MiniWalApplication"/>: prepare = flush WAL marker and pause writers.</summary>
    private sealed class WalCheckpointAdapter(MiniWalApplication app, bool failPrepare) : IDfsSnapshotAdapter
    {
        public string Id => "test.mini-wal-db";

        public string Version => "2.1";

        public IReadOnlyDictionary<string, string> RestoreRequirements { get; } = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [MiniWalApplication.MinVersionRequirement] = MiniWalApplication.AppVersion.ToString(CultureInfo.InvariantCulture),
            ["checkpoint"] = MiniWalApplication.CheckpointFile,
            ["replay-wal"] = MiniWalApplication.Wal,
        };

        public int Prepared { get; private set; }

        public int Resumed { get; private set; }

        public int Aborted { get; private set; }

        public IReadOnlyDictionary<string, string>? PreparedState { get; private set; }

        public async Task PrepareAsync(DfsSnapshotAdapterContext context, CancellationToken ct)
        {
            Prepared++;
            if (failPrepare)
                throw new InvalidOperationException("simulated application checkpoint failure");
            await app.MarkPreparedAsync(ct);
            PreparedState = new SortedDictionary<string, string>(app.State.ToDictionary(p => p.Key, p => p.Value), StringComparer.Ordinal);
        }

        public Task ResumeAsync(DfsSnapshotAdapterContext context, CancellationToken ct)
        {
            Resumed++;
            return Task.CompletedTask;
        }

        public Task AbortAsync(DfsSnapshotAdapterContext context, CancellationToken ct)
        {
            Aborted++;
            return Task.CompletedTask;
        }
    }
}
