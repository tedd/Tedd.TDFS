using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Simulation.Rst;

/// <summary>
/// RST — disaster recovery, recovery keys and promotion (FSD §§11.1, 12.4–12.5, 14). Deterministic simulation:
/// restore interruption, new authority epochs, no automatic promotion and returning old sources.
/// </summary>
public sealed class RstSimulationTests
{
    private const string Vol = RecoveryScenario.Volume;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    // ---------------------------------------------------------------------------------------------------------------
    // RST-007
    // ---------------------------------------------------------------------------------------------------------------

    /// <summary>The restore phase interrupted in RST-007 (restore work is tagged <see cref="DfsOperationKind.RestoreSnapshot"/>).</summary>
    public enum RestorePhase
    {
        /// <summary>F06: during an interior imported object write on a live storage node.</summary>
        ObjectImportWrite,

        /// <summary>F07: an imported object written locally, before its durability barrier.</summary>
        ObjectImportBeforeFlush,

        /// <summary>F08: an imported object durable, before its receipt reaches the restore coordinator.</summary>
        ObjectImportAfterFlush,

        /// <summary>F10: the metadata install entry appended locally, before quorum commit.</summary>
        MetadataInstallAfterLocalAppend,

        /// <summary>F11: the metadata install entry committed, before it is applied.</summary>
        MetadataInstallAfterCommit,

        /// <summary>F21: during the activation (new authority epoch) transition.</summary>
        Activation,

        /// <summary>F12: activation applied, before the reply is sent.</summary>
        ActivationAppliedBeforeReply,
    }

    private static FaultMatch RestorePhaseMatch(RestorePhase phase) => phase switch
    {
        RestorePhase.ObjectImportWrite => new FaultMatch(FaultSites.ObjectWrite, Operation: DfsOperationKind.RestoreSnapshot, Occurrence: 2),
        RestorePhase.ObjectImportBeforeFlush => new FaultMatch(FaultSites.ObjectBeforeFlush, Operation: DfsOperationKind.RestoreSnapshot, Occurrence: 2),
        RestorePhase.ObjectImportAfterFlush => new FaultMatch(FaultSites.ObjectAfterFlushBeforeReceipt, Operation: DfsOperationKind.RestoreSnapshot, Occurrence: 2),
        RestorePhase.MetadataInstallAfterLocalAppend => new FaultMatch(FaultSites.RaftAfterLocalAppend, Operation: DfsOperationKind.RestoreSnapshot),
        RestorePhase.MetadataInstallAfterCommit => new FaultMatch(FaultSites.RaftAfterCommitBeforeApply, Operation: DfsOperationKind.RestoreSnapshot),
        RestorePhase.Activation => new FaultMatch(FaultSites.Reconfiguration, Operation: DfsOperationKind.RestoreSnapshot),
        RestorePhase.ActivationAppliedBeforeReply => new FaultMatch(FaultSites.ApplyAfterBeforeReply, Operation: DfsOperationKind.RestoreSnapshot),
        _ => throw new ArgumentOutOfRangeException(nameof(phase)),
    };

    /// <summary>
    /// Blocks the restore at each phase, probes that no incomplete volume is served, crashes the node executing the
    /// phase, probes again before and after its restart, verifies the backup's published snapshots/pointer and content
    /// are untouched, then resumes the restore with the same request and verifies the complete new volume.
    /// </summary>
    [Theory]
    [InlineData(RestorePhase.ObjectImportWrite)]
    [InlineData(RestorePhase.ObjectImportBeforeFlush)]
    [InlineData(RestorePhase.ObjectImportAfterFlush)]
    [InlineData(RestorePhase.MetadataInstallAfterLocalAppend)]
    [InlineData(RestorePhase.MetadataInstallAfterCommit)]
    [InlineData(RestorePhase.Activation)]
    [InlineData(RestorePhase.ActivationAppliedBeforeReply)]
    [Catalog("RST-007", "S", "INV-08", "INV-10", "INV-12", "INV-13", "INV-19")]
    public Task RST_007_RestoreInterruptionIsResumable(RestorePhase phase) =>
        ClusterTest.RunAsync(RstHelpers.SourceOptions(), async cluster =>
        {
            BackedUpSource source = await RecoveryScenario.CreateBackedUpSourceAsync(cluster, "S", 7000 + (ulong)phase, Ct);
            TestBackupNode backup = source.Backup;
            IReadOnlyList<string> publishedBefore = await backup.Inspect.GetPublishedSnapshotsAsync(Vol);
            string? pointerBefore = await backup.Inspect.GetPublishedPointerAsync(Vol);
            Assert.Contains("S", publishedBefore);
            Assert.NotNull(pointerBefore);

            await using DfsClient admin = await cluster.ConnectAsync(new TestClientOptions { Name = "restore-admin" }, Ct);
            await using DfsClient probe = await cluster.ConnectAsync(new TestClientOptions { Name = "probe" }, Ct);
            DfsRestoreFromBackupRequest request = RecoveryScenario.RestoreRequest(backup, "S", "vol-r");
            FaultArm arm = cluster.Faults.Arm(RestorePhaseMatch(phase), FaultAction.Block());
            Task<DfsVolumeInfo> first = admin.Admin.RestoreFromBackupAsync(request, Ct);
            await arm.WaitTriggeredAsync(TimeSpan.FromSeconds(60));
            cluster.Faults.AssertReached(arm);

            await RstHelpers.AssertNotExposedIncompleteAsync(probe, "vol-r", source.Expected, $"while {phase} is blocked", Ct);

            TestNode interrupted = cluster.Node(arm.Contexts[0].Node);
            await interrupted.CrashAsync();
            arm.Release();
            Exception? firstOutcome = await RecoveryScenario.AwaitOutcomeAsync(cluster, first, TimeSpan.FromSeconds(60),
                "the interrupted restore call must complete or fail, not hang", Ct);
            await RstHelpers.AssertNotExposedIncompleteAsync(probe, "vol-r", source.Expected, $"after crashing {interrupted.Name} during {phase}", Ct);

            await interrupted.RestartAsync();
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), Ct);
            await RstHelpers.AssertNotExposedIncompleteAsync(probe, "vol-r", source.Expected, "after restart, before resuming", Ct);

            Assert.Equal(publishedBefore, await backup.Inspect.GetPublishedSnapshotsAsync(Vol));
            Assert.Equal(pointerBefore, await backup.Inspect.GetPublishedPointerAsync(Vol));
            await using (DfsClient reader = await RstHelpers.ConnectBackupAsync(cluster, backup, "bk-reader", backup.ReaderCredentials, Ct))
            await using (DfsSnapshotView view = await reader.GetVolume(Vol).Snapshots.OpenAsync("S", Ct))
                VolumeContentFixture.AssertMatches(source.Expected, await VolumeContentFixture.CaptureAsync(view, Ct), "the source backup stays intact after the interruption");

            DfsVolumeInfo? restored = null;
            if (firstOutcome is null)
                restored = await first;
            for (int attempt = 1; restored is null; attempt++)
            {
                Exception? retry = await RecoveryScenario.CaptureAsync(async () => restored = await admin.Admin.RestoreFromBackupAsync(request, Ct));
                if (retry is null)
                    break;
                if (retry is DfsAlreadyExistsException)
                {
                    restored = (await admin.Admin.GetVolumeStatusAsync("vol-r", Ct)).Info;
                    break;
                }

                Assert.True(retry is DfsUnavailableException && attempt < 5, $"resuming the restore failed on attempt {attempt}: {retry}");
                await cluster.DelayAsync(TimeSpan.FromSeconds(1), Ct);
            }

            Assert.NotNull(restored);
            Assert.Equal("vol-r", restored.Name);
            Assert.NotEqual(source.Info.Id, restored.Id);
            RecoveryScenario.AssertNotComparable(restored.LastCommit, source.Info.LastCommit, "the restored volume is a new authority");
            Assert.Single(await admin.Admin.ListVolumesAsync(Ct), v => v == "vol-r");
            VolumeContentFixture.AssertMatches(source.Expected, await VolumeContentFixture.CaptureAsync(probe.GetVolume("vol-r"), Ct), $"restore resumed after {phase}");
            await cluster.WaitForReplicationAsync("vol-r", Ct);
            await cluster.Inspect.AssertReplicasConvergedAsync("vol-r");
            Assert.Equal(publishedBefore, await backup.Inspect.GetPublishedSnapshotsAsync(Vol));
        }, Ct);

    // ---------------------------------------------------------------------------------------------------------------
    // RST-008
    // ---------------------------------------------------------------------------------------------------------------

    /// <summary>How the new authority is activated in RST-008.</summary>
    public enum NewAuthorityKind
    {
        /// <summary>The old volume is deleted (fenced) and the backup is restored under the same name.</summary>
        BackupRestoreIntoFencedName,

        /// <summary>A snapshot is restored into a new volume next to the still-live old one.</summary>
        SnapshotRestoreIntoNewVolume,

        /// <summary>The backup is restored into an independent recovery cluster.</summary>
        BackupRestoreIntoRecoveryCluster,
    }

    /// <summary>
    /// Activates a new authority from historical data and replays every old-authority artifact against it — commit
    /// tokens (as ordering constraints), committed and reserved operation ids, a writer lease with staged bytes and the old
    /// session — asserting none mutates the new authority, while fresh work on it proceeds under its own epoch.
    /// </summary>
    [Theory]
    [InlineData(NewAuthorityKind.BackupRestoreIntoFencedName)]
    [InlineData(NewAuthorityKind.SnapshotRestoreIntoNewVolume)]
    [InlineData(NewAuthorityKind.BackupRestoreIntoRecoveryCluster)]
    [Catalog("RST-008", "S", "INV-08", "INV-10", "INV-12", "INV-13", "INV-19")]
    public Task RST_008_RestoreUsesNewAuthorityEpoch(NewAuthorityKind kind) =>
        ClusterTest.RunAsync(RstHelpers.SourceOptions(), async cluster =>
        {
            BackedUpSource source = await RecoveryScenario.CreateBackedUpSourceAsync(cluster, "S", 8000 + (ulong)kind, Ct);
            DfsVolume oldVolume = source.Volume;
            DfsOperationId committedId = await oldVolume.NewOperationIdAsync(Ct);
            CommitToken oldToken = await oldVolume.Files.WriteAllBytesAsync("/old-op.bin", TestData.Bytes(8001, 100), new DfsMutationOptions { OperationId = committedId }, Ct);
            DfsOperationId reservedId = await oldVolume.NewOperationIdAsync(Ct);
            byte[] staged = TestData.Bytes(8002, 3000);
            DfsStream oldWriter = await oldVolume.Files.OpenAsync("/docs/one.bin", FileMode.Open, FileAccess.ReadWrite, FileShare.None, Ct);
            await oldWriter.WriteAsync(staged, Ct);

            TestCluster target = cluster;
            TestCluster? recovery = null;
            string name;
            DfsVolumeInfo restored;
            switch (kind)
            {
                case NewAuthorityKind.BackupRestoreIntoFencedName:
                    await cluster.Admin.DeleteVolumeAsync(Vol, Ct);
                    restored = await cluster.Admin.RestoreFromBackupAsync(RecoveryScenario.RestoreRequest(source.Backup, "S", Vol), Ct);
                    name = Vol;
                    break;
                case NewAuthorityKind.SnapshotRestoreIntoNewVolume:
                    restored = await oldVolume.Snapshots.RestoreAsync("S", "vol-new", Ct);
                    name = "vol-new";
                    break;
                default:
                    recovery = await cluster.StartRecoveryClusterAsync(RstHelpers.RecoveryOptions(), Ct);
                    target = recovery;
                    restored = await recovery.Admin.RestoreFromBackupAsync(RecoveryScenario.RestoreRequest(source.Backup, "S", Vol), Ct);
                    name = Vol;
                    break;
            }

            try
            {
                Assert.Equal(name, restored.Name);
                Assert.NotEqual(source.Info.Id, restored.Id);
                Assert.Equal(restored.Id, restored.LastCommit.VolumeId);
                Assert.Equal(restored.Epoch, restored.LastCommit.VolumeEpoch);
                RecoveryScenario.AssertNotComparable(oldToken, restored.LastCommit, "old tokens cannot order the new authority");

                await using DfsClient fresh = await target.ConnectAsync(new TestClientOptions { Name = "fresh" }, Ct);
                DfsVolume newVolume = fresh.GetVolume(name);
                VolumeContentFixture.AssertMatches(source.Expected, await VolumeContentFixture.CaptureAsync(newVolume, Ct), "the new authority starts exactly from S");

                Exception? minimum = await RecoveryScenario.CaptureAsync(() => newVolume.Snapshots.CreateAsync("min-old", new DfsSnapshotOptions { MinimumCommit = oldToken }, Ct));
                RecoveryScenario.AssertFailedWith(minimum, "a MinimumCommit from the old authority", DfsErrorCode.EpochMismatch, DfsErrorCode.InvalidArgument, DfsErrorCode.ClusterMismatch);
                Assert.DoesNotContain(await newVolume.Snapshots.ListAsync(Ct), s => s.Name == "min-old");

                foreach ((string label, DfsOperationId id) in new[] { ("committed", committedId), ("reserved", reservedId) })
                {
                    Exception? replay = await RecoveryScenario.CaptureAsync(() => newVolume.Files.WriteAllBytesAsync("/replayed-" + label + ".bin",
                        TestData.Bytes(8003, 10), new DfsMutationOptions { OperationId = id }, Ct));
                    RecoveryScenario.AssertFailedWith(replay, $"replaying the old {label} operation id {id} against the new authority",
                        DfsErrorCode.OperationExpired, DfsErrorCode.OperationIdMismatch, DfsErrorCode.EpochMismatch, DfsErrorCode.InvalidArgument, DfsErrorCode.ClusterMismatch);
                    await Assert.ThrowsAsync<FileNotFoundException>(() => newVolume.Files.ReadAllBytesAsync("/replayed-" + label + ".bin", Ct));
                }

                await Assert.ThrowsAsync<FileNotFoundException>(() => newVolume.Files.ReadAllBytesAsync("/old-op.bin", Ct));

                if (kind == NewAuthorityKind.BackupRestoreIntoFencedName)
                {
                    Exception? stale = await RecoveryScenario.CaptureAsync(() => oldWriter.CommitAsync(Ct));
                    RecoveryScenario.AssertFailedWith(stale, "the old writer lease confers no authority on the restored identity",
                        DfsErrorCode.VolumeNotFound, DfsErrorCode.LeaseLost, DfsErrorCode.EpochMismatch, DfsErrorCode.InvalidHandle);
                    Assert.True(oldWriter.IsFaulted);

                    DfsOperationOutcome? outcome = null;
                    Exception? query = await RecoveryScenario.CaptureAsync(async () => outcome = await oldVolume.QueryOperationAsync(committedId, false, Ct));
                    if (query is null)
                    {
                        Assert.NotNull(outcome);
                        Assert.True(outcome.Status == DfsOutcomeStatus.Expired,
                            $"an operation of the previous epoch must be reported Expired by the new authority, not {outcome.Status}");
                    }
                    else
                    {
                        RecoveryScenario.AssertFailedWith(query, "querying an old-epoch operation", DfsErrorCode.EpochMismatch, DfsErrorCode.OperationExpired, DfsErrorCode.VolumeNotFound);
                    }

                    CommitToken? viaOldSession = null;
                    Exception? oldSessionWrite = await RecoveryScenario.CaptureAsync(async () =>
                        viaOldSession = await oldVolume.Files.WriteAllBytesAsync("/old-session.bin", TestData.Bytes(8004, 50), Ct));
                    if (oldSessionWrite is null)
                    {
                        CommitToken t = viaOldSession ?? throw new InvalidOperationException("a successful write must return a commit token");
                        Assert.Equal(restored.Id, t.VolumeId);
                        Assert.Equal(restored.Epoch, t.VolumeEpoch);
                        Assert.True(t.IsAtOrAfter(restored.LastCommit), "a write that succeeds after the epoch change is a new-epoch commit");
                    }
                    else
                    {
                        RecoveryScenario.AssertFailedWith(oldSessionWrite, "a write on the old session", DfsErrorCode.EpochMismatch, DfsErrorCode.OperationExpired, DfsErrorCode.VolumeNotFound);
                    }
                }
                else
                {
                    DfsOperationOutcome? outcome = null;
                    Exception? query = await RecoveryScenario.CaptureAsync(async () => outcome = await newVolume.QueryOperationAsync(committedId, false, Ct));
                    if (query is null)
                    {
                        Assert.NotNull(outcome);
                        Assert.True(outcome.Status is DfsOutcomeStatus.NotFound or DfsOutcomeStatus.Expired,
                            $"the new authority must not report another authority's operation as {outcome.Status}");
                    }
                    else
                    {
                        RecoveryScenario.AssertFailedWith(query, "querying another authority's operation",
                            DfsErrorCode.OperationExpired, DfsErrorCode.EpochMismatch, DfsErrorCode.OperationIdMismatch, DfsErrorCode.ClusterMismatch, DfsErrorCode.InvalidArgument);
                    }
                }

                // The old lease does not exist in the new authority: an exclusive writer opens and commits there.
                byte[] fresher = TestData.Bytes(8005, 1234);
                CommitToken newCommit;
                await using (DfsStream writer = await newVolume.Files.OpenAsync("/docs/one.bin", FileMode.Open, FileAccess.ReadWrite, FileShare.None, Ct))
                {
                    writer.SetLength(0);
                    await writer.WriteAsync(fresher, Ct);
                    newCommit = await writer.CommitAsync(Ct);
                }

                Assert.True(newCommit.IsComparableWith(restored.LastCommit) && newCommit.IsAtOrAfter(restored.LastCommit));
                RecoveryScenario.AssertNotComparable(newCommit, oldToken, "new-epoch commits are not ordered against the old authority");
                await RecoveryScenario.AssertFileBytesAsync(newVolume.Files, "/docs/one.bin", fresher, "the new authority's writer", Ct);
                if (kind == NewAuthorityKind.SnapshotRestoreIntoNewVolume)
                {
                    CommitToken oldCommit = await oldWriter.CommitAsync(Ct);
                    Assert.True(oldCommit.IsComparableWith(oldToken) && oldCommit.IsAtOrAfter(oldToken), "the old volume remains its own authority");
                    await RecoveryScenario.AssertFileBytesAsync(oldVolume.Files, "/docs/one.bin", staged, "the old volume keeps its own writer", Ct);
                    await RecoveryScenario.AssertFileBytesAsync(newVolume.Files, "/docs/one.bin", fresher, "the old writer's commit does not reach the new volume", Ct);
                }
            }
            finally
            {
                await RecoveryScenario.CaptureAsync(async () => await oldWriter.DisposeAsync());
                if (recovery is not null)
                    await recovery.DisposeAsync();
            }
        }, Ct);

    // ---------------------------------------------------------------------------------------------------------------
    // RST-009
    // ---------------------------------------------------------------------------------------------------------------

    /// <summary>How the backup loses its source in RST-009.</summary>
    public enum SourceLoss
    {
        /// <summary>Every link between the backup host and the live nodes is cut.</summary>
        BackupIsolatedFromSources,

        /// <summary>Every live node crashes.</summary>
        AllSourcesCrashed,

        /// <summary>The backup sits in a partition with one live node (a minority) and its clients.</summary>
        BackupWithSourceMinority,
    }

    /// <summary>
    /// Keeps the backup separated from its source for bounded long runs (many election, lease and session timeouts); at
    /// every checkpoint and after reconnection the backup is in no live group, every mutation through it (with reader,
    /// export and retention-admin credentials) is rejected as read-only, and it keeps serving its published snapshot.
    /// </summary>
    [Theory]
    [InlineData(SourceLoss.BackupIsolatedFromSources, 1)]
    [InlineData(SourceLoss.BackupIsolatedFromSources, 5)]
    [InlineData(SourceLoss.AllSourcesCrashed, 5)]
    [InlineData(SourceLoss.BackupWithSourceMinority, 5)]
    [Catalog("RST-009", "S", "INV-08", "INV-10", "INV-12", "INV-13", "INV-19")]
    public Task RST_009_BackupNeverAutoPromotesOnPartition(SourceLoss loss, int minutes) =>
        ClusterTest.RunAsync(RstHelpers.SourceOptions(scenarioTimeout: TimeSpan.FromMinutes(minutes * 2 + 10)), async cluster =>
        {
            BackedUpSource source = await RecoveryScenario.CreateBackedUpSourceAsync(cluster, "S", 9000 + (ulong)loss, Ct);
            TestBackupNode backup = source.Backup;
            IReadOnlyList<string> published = await backup.Inspect.GetPublishedSnapshotsAsync(Vol);
            await RstHelpers.AssertBackupNotLiveMemberAsync(cluster, backup.Name, "before the outage");

            await using DfsClient reader = await RstHelpers.ConnectBackupAsync(cluster, backup, "bk-reader", backup.ReaderCredentials, Ct);
            await using DfsClient exporter = await RstHelpers.ConnectBackupAsync(cluster, backup, "bk-export", backup.ExportCredentials, Ct);
            await using DfsClient retention = await RstHelpers.ConnectBackupAsync(cluster, backup, "bk-retention", backup.RetentionAdminCredentials, Ct);
            DfsClient[] backupClients = [reader, exporter, retention];
            await RstHelpers.AssertBackupReadOnlyAsync(reader, "before the outage", Ct);

            switch (loss)
            {
                case SourceLoss.BackupIsolatedFromSources:
                    foreach (TestNode node in cluster.Nodes)
                        cluster.Network.CutBoth(backup.Name, node.Name);
                    break;
                case SourceLoss.AllSourcesCrashed:
                    foreach (TestNode node in cluster.Nodes)
                        await node.CrashAsync();
                    break;
                default:
                    cluster.Network.Partition(
                        [backup.Name, cluster[0].Name, "client:bk-reader", "client:bk-export", "client:bk-retention"],
                        [.. cluster.Nodes.Skip(1).Select(n => n.Name), "client:source"]);
                    break;
            }

            TimeSpan total = TimeSpan.FromMinutes(minutes);
            const int checkpoints = 5;
            for (int i = 1; i <= checkpoints; i++)
            {
                await cluster.DelayAsync(total / checkpoints, Ct);
                string when = $"{(total / checkpoints * i).TotalSeconds:0}s into {loss}";
                foreach (DfsClient c in backupClients)
                    await RstHelpers.AssertBackupReadOnlyAsync(c, when, Ct);
                await using (DfsSnapshotView view = await reader.GetVolume(Vol).Snapshots.OpenAsync("S", Ct))
                    await RecoveryScenario.AssertFileBytesAsync(view.Files, VolumeContentFixture.SparsePath,
                        source.Expected.Files[VolumeContentFixture.SparsePath].Content, $"backup stays readable from local authority {when}", Ct);
                if (loss != SourceLoss.AllSourcesCrashed)
                    await RstHelpers.AssertBackupNotLiveMemberAsync(cluster, backup.Name, when);
                Assert.Equal(published, await backup.Inspect.GetPublishedSnapshotsAsync(Vol));
            }

            await using (DfsSnapshotView view = await reader.GetVolume(Vol).Snapshots.OpenAsync("S", Ct))
                VolumeContentFixture.AssertMatches(source.Expected, await VolumeContentFixture.CaptureAsync(view, Ct), "the backup serves S unchanged without its source");

            cluster.Network.Heal();
            if (loss == SourceLoss.AllSourcesCrashed)
            {
                foreach (TestNode node in cluster.Nodes)
                    await node.RestartAsync();
            }

            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), Ct);
            await RstHelpers.AssertBackupNotLiveMemberAsync(cluster, backup.Name, "after reconnection");
            DfsClusterStatus status = await cluster.Admin.GetClusterStatusAsync(Ct);
            Assert.DoesNotContain(status.Nodes, n => n.Name == backup.Name);
            Assert.DoesNotContain(backup.Name, status.Control.Voters);
            foreach (DfsClient c in backupClients)
                await RstHelpers.AssertBackupReadOnlyAsync(c, "after reconnection", Ct);

            // The live volume kept a single authority and the backup's attempts left no trace in it.
            await using DfsClient live = await cluster.ConnectAsync(new TestClientOptions { Name = "live-check" }, Ct);
            VolumeContentFixture.AssertMatches(source.Expected, await VolumeContentFixture.CaptureAsync(live.GetVolume(Vol), Ct), "no mutation through the backup reached the live volume");
            await cluster.Inspect.AssertNoSplitBrainAsync();
        }, Ct);

    // ---------------------------------------------------------------------------------------------------------------
    // RST-011
    // ---------------------------------------------------------------------------------------------------------------

    /// <summary>Which old source returns in RST-011.</summary>
    public enum ReturningSource
    {
        /// <summary>An old replica, offline during deletion and same-name restore in the same cluster, restarts with its old-epoch state.</summary>
        OldReplicaRejoinsSameCluster,

        /// <summary>The whole old cluster (which kept writing while cut off) reconnects after a restore into a recovery cluster.</summary>
        OldClusterReconnectsAfterRecoveryCluster,
    }

    /// <summary>
    /// After explicit recovery the old source returns with divergent, later history; the recovered authority must reject
    /// its writes and replication instead of merging: the recovered volume holds exactly S plus its own new writes on every
    /// replica, old tokens/ids/handles/identities are rejected, and no old member joins the recovered groups.
    /// </summary>
    [Theory]
    [InlineData(ReturningSource.OldReplicaRejoinsSameCluster)]
    [InlineData(ReturningSource.OldClusterReconnectsAfterRecoveryCluster)]
    [Catalog("RST-011", "S", "INV-08", "INV-10", "INV-12", "INV-13", "INV-19")]
    public Task RST_011_OldSourceReturnsAfterPromotion(ReturningSource returning) =>
        ClusterTest.RunAsync(RstHelpers.SourceOptions(), async cluster =>
        {
            BackedUpSource source = await RecoveryScenario.CreateBackedUpSourceAsync(cluster, "S", 11_000 + (ulong)returning, Ct);
            DfsVolume oldVolume = source.Volume;
            DfsOperationId divergentId = await oldVolume.NewOperationIdAsync(Ct);
            ExpectedVolumeContent recoveredExpected = source.Expected.Clone();
            byte[] newBytes = TestData.Bytes(11_002, 66_000);
            recoveredExpected.SetFile(new ExpectedFile("/recovered/new.bin", newBytes, null, new DfsInodeAccess(), []));

            if (returning == ReturningSource.OldReplicaRejoinsSameCluster)
            {
                TestNode oldReplica = cluster[2];
                await oldReplica.CrashAsync();
                CommitToken divergent = await oldVolume.Files.WriteAllBytesAsync("/divergent.bin", TestData.Bytes(11_001, 70_000),
                    new DfsMutationOptions { OperationId = divergentId }, Ct);
                DfsStream oldWriter = await oldVolume.Files.OpenAsync("/docs/one.bin", FileMode.Open, FileAccess.ReadWrite, FileShare.None, Ct);
                await oldWriter.WriteAsync(TestData.Bytes(11_003, 999), Ct);

                await cluster.Admin.DeleteVolumeAsync(Vol, Ct);
                DfsVolumeInfo restored = await cluster.Admin.RestoreFromBackupAsync(RecoveryScenario.RestoreRequest(source.Backup, "S", Vol), Ct);
                RecoveryScenario.AssertRestoredIdentity(restored, source.Info, cluster.ClusterId, Vol);
                await using DfsClient fresh = await cluster.ConnectAsync(new TestClientOptions { Name = "fresh" }, Ct);
                DfsVolume newVolume = fresh.GetVolume(Vol);
                CommitToken newCommit = await newVolume.Files.WriteAllBytesAsync("/recovered/new.bin", newBytes, Ct);
                RecoveryScenario.AssertNotComparable(newCommit, divergent, "the recovered history is not ordered after the divergent one");

                await oldReplica.RestartAsync();
                await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), Ct);
                await cluster.WaitForReplicationAsync(Vol, Ct);

                foreach (TestNode node in cluster.Nodes)
                {
                    VolumeStateInspection? state = await node.Inspect.GetVolumeStateAsync(Vol);
                    if (state is null)
                        continue;
                    Assert.Equal(restored.Id, state.Id);
                    Assert.Equal(restored.Epoch, state.Epoch);
                    Assert.Null(state.GetFile("/divergent.bin"));
                }

                await cluster.Inspect.AssertReplicasConvergedAsync(Vol);
                await cluster.Inspect.AssertNoSplitBrainAsync();

                Exception? staleWriter = await RecoveryScenario.CaptureAsync(() => oldWriter.CommitAsync(Ct));
                RecoveryScenario.AssertFailedWith(staleWriter, "the old writer's handle after recovery",
                    DfsErrorCode.VolumeNotFound, DfsErrorCode.LeaseLost, DfsErrorCode.EpochMismatch, DfsErrorCode.InvalidHandle);
                Exception? replay = await RecoveryScenario.CaptureAsync(() => oldVolume.Files.WriteAllBytesAsync("/divergent.bin", TestData.Bytes(11_001, 70_000),
                    new DfsMutationOptions { OperationId = divergentId }, Ct));
                RecoveryScenario.AssertFailedWith(replay, "replaying the divergent operation into the recovered authority",
                    DfsErrorCode.OperationExpired, DfsErrorCode.EpochMismatch, DfsErrorCode.OperationIdMismatch, DfsErrorCode.VolumeNotFound);
                await RecoveryScenario.CaptureAsync(async () => await oldWriter.DisposeAsync());

                await using DfsClient verifier = await cluster.ConnectAsync(new TestClientOptions { Name = "verifier" }, Ct);
                VolumeContentFixture.AssertMatches(recoveredExpected, await VolumeContentFixture.CaptureAsync(verifier.GetVolume(Vol), Ct),
                    "recovered volume = S + its own new writes; the old replica's history is not merged");
            }
            else
            {
                await using TestCluster recovery = await cluster.StartRecoveryClusterAsync(RstHelpers.RecoveryOptions(), Ct);
                foreach (TestNode node in cluster.Nodes)
                {
                    cluster.Network.CutBoth(node.Name, source.Backup.Name);
                    foreach (TestNode r in recovery.Nodes)
                        cluster.Network.CutBoth(node.Name, r.Name);
                }

                byte[] divergentBytes = TestData.Bytes(11_001, 70_000);
                CommitToken divergent = await oldVolume.Files.WriteAllBytesAsync("/divergent.bin", divergentBytes, new DfsMutationOptions { OperationId = divergentId }, Ct);

                DfsVolumeInfo restored = await recovery.Admin.RestoreFromBackupAsync(RecoveryScenario.RestoreRequest(source.Backup, "S", Vol), Ct);
                RecoveryScenario.AssertRestoredIdentity(restored, source.Info, recovery.ClusterId, Vol);
                await using DfsClient fresh = await recovery.ConnectAsync(new TestClientOptions { Name = "fresh" }, Ct);
                DfsVolume newVolume = fresh.GetVolume(Vol);
                CommitToken newCommit = await newVolume.Files.WriteAllBytesAsync("/recovered/new.bin", newBytes, Ct);

                cluster.Network.Heal();
                await cluster.DelayAsync(TimeSpan.FromSeconds(10), Ct);

                // The old source continues its own lineage (a new snapshot exported to the old backup target) ...
                DfsSnapshotInfo oldNext = await oldVolume.Snapshots.CreateAsync("S-old-next", null, Ct);
                await RecoveryScenario.PublishAsync(oldVolume, source.Backup, oldNext, Ct);

                // ... and an old node tries to join/replicate into the recovered cluster: rejected, never a member.
                TestNode oldNode = cluster[2];
                await oldNode.StopAsync();
                await oldNode.RestartAsync(o => o with { JoinSeeds = [recovery[0].Endpoint] });
                await cluster.DelayAsync(TimeSpan.FromSeconds(10), Ct);
                GroupInspection recoveryControl = await recovery.Inspect.GetControlGroupAsync();
                Assert.DoesNotContain(oldNode.Name, recoveryControl.Voters);
                Assert.DoesNotContain(oldNode.Name, recoveryControl.Learners);
                GroupInspection recoveryVolume = await recovery.Inspect.GetVolumeGroupAsync(Vol);
                Assert.DoesNotContain(oldNode.Name, recoveryVolume.Voters);
                Assert.DoesNotContain(oldNode.Name, recoveryVolume.Learners);
                Assert.DoesNotContain(await recovery.Admin.ListNodesAsync(Ct), n => n.Name == oldNode.Name || n.Id == oldNode.Id);

                Exception? pinned = await RecoveryScenario.CaptureAsync(() => recovery.ConnectAsync(new TestClientOptions { Name = "old-identity", ExpectedClusterId = cluster.ClusterId }, Ct));
                Assert.IsType<DfsClusterIdentityException>(pinned);
                RecoveryScenario.AssertNotComparable(divergent, newCommit, "divergent histories are never ordered together");
                Exception? replay = await RecoveryScenario.CaptureAsync(() => newVolume.Files.WriteAllBytesAsync("/divergent.bin", divergentBytes,
                    new DfsMutationOptions { OperationId = divergentId }, Ct));
                RecoveryScenario.AssertFailedWith(replay, "replaying the old source's operation into the recovered authority",
                    DfsErrorCode.OperationExpired, DfsErrorCode.EpochMismatch, DfsErrorCode.OperationIdMismatch, DfsErrorCode.InvalidArgument, DfsErrorCode.ClusterMismatch);

                await recovery.WaitForReplicationAsync(Vol, Ct);
                await recovery.Inspect.AssertReplicasConvergedAsync(Vol);
                await recovery.Inspect.AssertNoSplitBrainAsync();
                await using DfsClient verifier = await recovery.ConnectAsync(new TestClientOptions { Name = "verifier" }, Ct);
                VolumeContentFixture.AssertMatches(recoveredExpected, await VolumeContentFixture.CaptureAsync(verifier.GetVolume(Vol), Ct),
                    "recovered volume = S + its own writes; nothing from the returning old cluster");

                // The old source's own history is still its own (not merged either way).
                ExpectedVolumeContent oldExpected = source.Expected.Clone();
                oldExpected.SetFile(new ExpectedFile("/divergent.bin", divergentBytes, null, new DfsInodeAccess(), []));
                VolumeContentFixture.AssertMatches(oldExpected, await VolumeContentFixture.CaptureAsync(oldVolume, Ct), "old source keeps only its own lineage");
            }
        }, Ct);
}
