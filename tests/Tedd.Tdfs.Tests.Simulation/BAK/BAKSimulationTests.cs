using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;
using static Tedd.Tdfs.Tests.Simulation.BAK.BAKHelpers;

namespace Tedd.Tdfs.Tests.Simulation.BAK;

/// <summary>
/// BAK — late-sync replica transfer, read-only service and retention (FSD §12; test plan §7.21 and the critical
/// scenario §9.5). Every scenario runs in deterministic simulation with at least one backup node; the test builds an
/// independent <see cref="TreeModel"/> of every snapshot it creates and compares backup contents byte-for-byte.
/// </summary>
public sealed class BAKSimulationTests
{
    private static int s_clientCounter;

    private static string NextClientName(string prefix) => prefix + Interlocked.Increment(ref s_clientCounter);

    // ------------------------------------------------------------------------------------------------------------
    // BAK-001
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<int, int> MembershipTopologies => new()
    {
        { 2, 1 },
        { 3, 1 },
        { 3, 2 },
        { 5, 2 },
    };

    [Theory]
    [MemberData(nameof(MembershipTopologies))]
    [Catalog("BAK-001", "S", "INV-15", "INV-11")]
    public Task BAK_001_BackupNotInConsensusMembership(int nodeCount, int backupCount) =>
        ClusterTest.RunAsync(Options(nodeCount, backupCount), async cluster =>
        {
            await using DfsClient client = await ConnectLiveAsync(cluster);
            DfsVolume vol = client.GetVolume(Volume);
            foreach (TestBackupNode backup in cluster.BackupNodes)
                await RegisterAsync(backup);

            IReadOnlyList<string> targets = await vol.Backups.ListTargetsAsync(Ct);
            Assert.Equal(cluster.BackupNodes.Select(b => b.TargetName).Order(StringComparer.Ordinal), targets.Order(StringComparer.Ordinal));

            TreeModel model = InitialTree(1);
            await WriteTreeAsync(vol, model);
            DfsSnapshotInfo s1 = await SnapshotAsync(vol, "s1");
            foreach (TestBackupNode backup in cluster.BackupNodes)
                await SyncExactAsync(vol, backup.TargetName, s1);

            await AssertOutsideConsensusAsync(cluster, client, "after registration and a completed transfer");

            // Leader churn must not admit a backup either (only meaningful while a live majority survives).
            if (nodeCount >= 3)
            {
                TestNode leader = await cluster.GetLeaderAsync(Volume, Ct);
                await leader.CrashAsync();
                TestNode newLeader = await cluster.GetLeaderAsync(Volume, Ct);
                Assert.NotEqual(leader.Name, newLeader.Name);
                Assert.DoesNotContain(newLeader.Name, cluster.BackupNodes.Select(b => b.Name));
                await AssertOutsideConsensusAsync(cluster, client, "after a volume leader crash and re-election");
                await leader.RestartAsync();
                await cluster.WaitForStableAsync(TimeSpan.FromSeconds(30), Ct);
            }

            // Quorum denominator: crash enough configured live voters to lose the majority. Healthy, registered,
            // fully caught-up backups must not substitute for the missing votes.
            DfsOperationId deniedOp = await vol.NewOperationIdAsync(Ct);
            GroupInspection group = await cluster.Inspect.GetVolumeGroupAsync(Volume);
            int voters = group.Voters.Count;
            int toCrash = voters - (voters / 2);
            var crashed = new List<TestNode>();
            foreach (string name in group.Voters.Order(StringComparer.Ordinal).Take(toCrash))
            {
                TestNode node = cluster.Node(name);
                await node.CrashAsync();
                crashed.Add(node);
            }

            var survivors = group.Voters.Except(crashed.Select(c => c.Name), StringComparer.Ordinal).ToList();
            await cluster.WaitUntilAsync(async () =>
            {
                foreach (string name in survivors)
                {
                    GroupInspection? g = await cluster.Node(name).Inspect.GetGroupAsync(Volume);
                    if (g is not null && g.Role == RaftRole.Leader)
                        return false;
                }

                return true;
            }, TimeSpan.FromSeconds(10), "a minority of live voters must not keep or gain volume leadership while every backup is healthy", Ct);
            Assert.All(cluster.BackupNodes, b => Assert.Equal(TestNodeState.Running, b.State));

            byte[] denied = TestData.Bytes(99, 10_000);
            Exception refusal = await AssertRefusedAsync(
                () => vol.Files.WriteAllBytesAsync("/denied.bin", denied, new DfsMutationOptions { OperationId = deniedOp }, Ct),
                ex => ex is DfsUnavailableException or DfsCommitOutcomeUnknownException || ex is DfsException { ErrorCode: DfsErrorCode.Timeout },
                "a live write without a live majority must not succeed although every backup node is healthy and current");

            foreach (TestNode node in crashed)
                await node.RestartAsync();
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(30), Ct);

            DfsOperationOutcome outcome = await vol.QueryOperationAsync(deniedOp, resolve: true, Ct);
            Assert.True(outcome.Status is DfsOutcomeStatus.Committed or DfsOutcomeStatus.Aborted,
                $"resolve:true must return a definite outcome, got {outcome.Status}");
            if (refusal is DfsCommitOutcomeUnknownException unknown)
            {
                Assert.Equal(deniedOp, unknown.OperationId);
            }
            else
            {
                Assert.True(outcome.Status == DfsOutcomeStatus.Aborted,
                    $"a definite availability failure ({refusal.GetType().Name}) must not have committed, outcome {outcome.Status}");
            }

            if (outcome.Status == DfsOutcomeStatus.Committed)
                await AssertFileBytesAsync(vol.Files, "/denied.bin", denied, "committed unknown write");
            else
                Assert.False(await vol.Files.ExistsAsync("/denied.bin", Ct), "an aborted write must leave no file");

            await AssertOutsideConsensusAsync(cluster, client, "after majority loss and recovery");
        });

    private static async Task AssertOutsideConsensusAsync(TestCluster cluster, DfsClient client, string because)
    {
        var backupNames = cluster.BackupNodes.Select(b => b.Name).ToHashSet(StringComparer.Ordinal);
        var liveNames = cluster.Nodes.Select(n => n.Name).ToHashSet(StringComparer.Ordinal);
        foreach (TestNode node in cluster.Nodes.Where(n => n.State == TestNodeState.Running))
        {
            foreach (GroupInspection g in await node.Inspect.GetGroupsAsync())
            {
                foreach (string member in g.Voters.Concat(g.Learners))
                {
                    Assert.False(backupNames.Contains(member), $"{because}: backup {member} is a member of group {g.Group} as seen by {node.Name}");
                    Assert.True(liveNames.Contains(member), $"{because}: group {g.Group} on {node.Name} lists unknown member {member}");
                }

                if (g.Leader is not null)
                    Assert.False(backupNames.Contains(g.Leader), $"{because}: backup {g.Leader} leads group {g.Group}");
            }
        }

        GroupInspection control = await cluster.Inspect.GetControlGroupAsync();
        Assert.Equal(cluster.Options.EffectiveControlVoters, control.Voters.Count);
        GroupInspection volume = await cluster.Inspect.GetVolumeGroupAsync(Volume);
        Assert.Equal(Math.Min(3, cluster.Options.EffectiveNodeCount), volume.Voters.Count);

        DfsClusterStatus status = await client.Admin.GetClusterStatusAsync(Ct);
        Assert.All(status.Nodes, n => Assert.False(backupNames.Contains(n.Name), $"{because}: backup {n.Name} listed as a cluster member"));
        Assert.Equal(cluster.Options.EffectiveNodeCount, status.Nodes.Count);
        Assert.All(status.Control.Voters.Concat(status.Control.Learners), m => Assert.False(backupNames.Contains(m), $"{because}: control member {m}"));
        DfsVolumeStatus volumeStatus = await client.Admin.GetVolumeStatusAsync(Volume, Ct);
        Assert.All(volumeStatus.Group.Voters.Concat(volumeStatus.Group.Learners),
            m => Assert.False(backupNames.Contains(m), $"{because}: volume group member {m}"));
        IReadOnlyList<string> registered = await client.GetVolume(Volume).Backups.ListTargetsAsync(Ct);
        Assert.Equal(registered.Order(StringComparer.Ordinal), volumeStatus.Backups.Select(b => b.Target).Order(StringComparer.Ordinal));
    }

    // ------------------------------------------------------------------------------------------------------------
    // BAK-002
    // ------------------------------------------------------------------------------------------------------------

    public enum BackupPrincipal
    {
        Reader,
        Export,
        RetentionAdmin,
    }

    private static DfsCredentials CredentialsOf(TestBackupNode backup, BackupPrincipal principal) => principal switch
    {
        BackupPrincipal.Reader => backup.ReaderCredentials,
        BackupPrincipal.Export => backup.ExportCredentials,
        _ => backup.RetentionAdminCredentials,
    };

    private static readonly (FileMode Mode, FileAccess Access)[] s_writableOpens =
    [
        (FileMode.Open, FileAccess.Write),
        (FileMode.Open, FileAccess.ReadWrite),
        (FileMode.OpenOrCreate, FileAccess.Write),
        (FileMode.OpenOrCreate, FileAccess.ReadWrite),
        (FileMode.Create, FileAccess.Write),
        (FileMode.Create, FileAccess.ReadWrite),
        (FileMode.Truncate, FileAccess.Write),
        (FileMode.Truncate, FileAccess.ReadWrite),
        (FileMode.Append, FileAccess.Write),
        (FileMode.CreateNew, FileAccess.Write),
        (FileMode.CreateNew, FileAccess.ReadWrite),
    ];

    [Theory]
    [InlineData(BackupPrincipal.Reader)]
    [InlineData(BackupPrincipal.Export)]
    [InlineData(BackupPrincipal.RetentionAdmin)]
    [Catalog("BAK-002", "S", "INV-15", "INV-05")]
    public Task BAK_002_BackupDoesNotGrantLocks(BackupPrincipal principal) =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            TestBackupNode b0 = cluster.BackupNodes[0];
            await using DfsClient live = await ConnectLiveAsync(cluster);
            DfsVolume vol = live.GetVolume(Volume);
            await RegisterAsync(b0);
            TreeModel model = InitialTree(2);
            await WriteTreeAsync(vol, model);
            await SnapshotAndSyncAsync(vol, b0.TargetName, "s1");

            const string target = "/docs/a.bin";
            FileInspection? before = await cluster.Inspect.GetFileAsync(Volume, target);
            Assert.NotNull(before);

            DfsCredentials credentials = CredentialsOf(b0, principal);
            await using DfsClient backupClient = await ConnectBackupAsync(cluster, credentials);
            DfsVolume bvol = backupClient.GetVolume(Volume);
            Assert.True(bvol.Files.IsReadOnly, "the backup-host file facade must report read-only");
            Assert.True(bvol.Directories.IsReadOnly, "the backup-host directory facade must report read-only");

            // Writer/lease-taking opens and transactions through the backup role are refused.
            foreach ((FileMode mode, FileAccess access) in s_writableOpens)
            {
                string path = mode == FileMode.CreateNew ? "/new-on-backup.bin" : target;
                foreach (FileShare share in new[] { FileShare.None, FileShare.Read })
                {
                    await AssertEx.ThrowsDfsAsync<DfsReadOnlyException>(async () =>
                    {
                        await using DfsStream s = await bvol.Files.OpenAsync(path, mode, access, share, Ct);
                    }, DfsErrorCode.ReadOnly);
                }
            }

            await AssertEx.ThrowsDfsAsync<DfsReadOnlyException>(async () =>
            {
                await using DfsStream s = await bvol.Files.OpenWriteAsync(target, Ct);
            }, DfsErrorCode.ReadOnly);
            await AssertEx.ThrowsDfsAsync<DfsReadOnlyException>(async () =>
            {
                await using DfsTransaction tx = await bvol.Transactions.BeginAsync(null, Ct);
            }, DfsErrorCode.ReadOnly);

            await using (DfsSnapshotView view = await bvol.Snapshots.OpenAsync("s1", Ct))
            {
                Assert.True(view.Files.IsReadOnly);
                foreach ((FileMode mode, FileAccess access) in s_writableOpens)
                {
                    string path = mode == FileMode.CreateNew ? "/new-on-backup.bin" : target;
                    await AssertEx.ThrowsDfsAsync<DfsReadOnlyException>(async () =>
                    {
                        await using DfsStream s = await view.Files.OpenAsync(path, mode, access, FileShare.None, Ct);
                    }, DfsErrorCode.ReadOnly);
                }

                // An exclusive *read* open of a snapshot view is either refused or granted without any exclusive
                // reservation: a second reader with FileShare.None must then succeed concurrently.
                DfsStream? exclusive = null;
                try
                {
                    exclusive = await view.Files.OpenAsync(target, FileMode.Open, FileAccess.Read, FileShare.None, Ct);
                }
                catch (Exception ex) when (ex is DfsReadOnlyException or NotSupportedException)
                {
                }

                if (exclusive is not null)
                {
                    await using (exclusive)
                    {
                        await using DfsClient other = await ConnectBackupReaderAsync(cluster, 0, NextClientName("bk"));
                        await using DfsSnapshotView view2 = await other.GetVolume(Volume).Snapshots.OpenAsync("s1", Ct);
                        await using DfsStream second = await view2.Files.OpenAsync(target, FileMode.Open, FileAccess.Read, FileShare.None, Ct);
                        Assert.Equal(model.Files[target].Length, await second.GetLengthAsync(Ct));
                        Assert.Equal(0UL, exclusive.WriterGeneration);
                        Assert.Equal(0UL, second.WriterGeneration);
                    }
                }
            }

            // Server-side enforcement without the SDK guard.
            await using (RawProtocolClient raw = await cluster.ConnectRawBackupAsync(0, Ct))
            {
                Assert.Equal(DfsErrorCode.None, await raw.AuthenticateAsync(credentials));
                Assert.Equal(DfsErrorCode.ReadOnly, await raw.SendPathOperationAsync(Volume, DfsOperationKind.Open, "/raw-new.bin"));
                Assert.Equal(DfsErrorCode.ReadOnly, await raw.SendPathOperationAsync(Volume, DfsOperationKind.WriteAll, target));
            }

            // Live state: no session, handle or writer generation originates from the backup role.
            string[] backupPrincipals = [b0.ReaderCredentials.Principal, b0.ExportCredentials.Principal, b0.RetentionAdminCredentials.Principal];
            VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Volume);
            Assert.DoesNotContain(state.Sessions, s => backupPrincipals.Contains(s.Principal, StringComparer.Ordinal));
            Assert.DoesNotContain(state.Handles, h => h.IsWriter);
            FileInspection? afterAttempts = await cluster.Inspect.GetFileAsync(Volume, target);
            Assert.NotNull(afterAttempts);
            Assert.Equal(before.WriterGeneration, afterAttempts.WriterGeneration);
            Assert.Equal(before.Revision, afterAttempts.Revision);

            // A live writer obtains exclusive authority immediately, even while an export is in flight.
            await PutAsync(vol, model, "/export/payload.bin", TestData.Bytes(3, 300 * 1024));
            DfsSnapshotInfo s2 = await SnapshotAsync(vol, "s2");
            TreeModel model2 = model.Clone();
            FaultArm hold = cluster.Faults.Arm(new FaultMatch(FaultSites.BackupObjectReceived, Node: b0.Name, Volume: Volume), FaultAction.Block());
            await vol.Backups.RequestSyncAsync(b0.TargetName, "s2", Ct);
            await hold.WaitTriggeredAsync(FaultTimeout);
            cluster.Faults.AssertReached(hold);

            byte[] replacement = TestData.Bytes(4, 150 * 1024);
            TimeSpan exclusiveWrite = await TimeAsync(cluster, async () =>
            {
                await using DfsStream writer = await vol.Files.OpenAsync(target, FileMode.Open, FileAccess.ReadWrite, FileShare.None, Ct);
                Assert.True(writer.WriterGeneration > afterAttempts.WriterGeneration, "the live writer must receive a fresh writer generation");
                await writer.SetLengthAsync(0, Ct);
                await writer.WriteAsync(replacement, Ct);
                CommitToken t = await writer.CommitAsync(Ct);
                Assert.False(t.IsNone);
            });
            Assert.True(exclusiveWrite <= LiveCommitBound, $"exclusive live write during an export took {exclusiveWrite}");
            model.Put(target, replacement);
            VolumeStateInspection during = await cluster.Inspect.GetVolumeStateAsync(Volume);
            Assert.DoesNotContain(during.Sessions, s => backupPrincipals.Contains(s.Principal, StringComparer.Ordinal));
            Assert.DoesNotContain(during.Handles, h => h.IsWriter);

            hold.Release();
            DfsBackupCheckpoint cp2 = await vol.Backups.AwaitBackupSnapshotAsync(b0.TargetName, "s2", SyncTimeout, Ct);
            AssertExactCheckpoint(cp2, b0.TargetName, s2);
            await using DfsClient reader = await ConnectBackupReaderAsync(cluster, 0, NextClientName("bk"));
            await AssertBackupSnapshotAsync(reader, "s2", model2, "s2 exported while a live writer held exclusive authority");
            await AssertFileBytesAsync(vol.Files, target, replacement, "live content after the exclusive write");
        });

    // ------------------------------------------------------------------------------------------------------------
    // BAK-003
    // ------------------------------------------------------------------------------------------------------------

    public enum BackupDisruption
    {
        Paused,
        Crashed,
        Isolated,
        BlackHoled,
        BlockedMidTransfer,
    }

    [Theory]
    [InlineData(BackupDisruption.Paused)]
    [InlineData(BackupDisruption.Crashed)]
    [InlineData(BackupDisruption.Isolated)]
    [InlineData(BackupDisruption.BlackHoled)]
    [InlineData(BackupDisruption.BlockedMidTransfer)]
    [Catalog("BAK-003", "S", "INV-15", "INV-01", "INV-11")]
    public Task BAK_003_BackupNotInOrdinaryAckSet(BackupDisruption disruption) =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            TestBackupNode b0 = cluster.BackupNodes[0];
            await using DfsClient live = await ConnectLiveAsync(cluster);
            DfsVolume vol = live.GetVolume(Volume);
            await RegisterAsync(b0);
            TreeModel model = InitialTree(3);
            await WriteTreeAsync(vol, model);
            (DfsSnapshotInfo s1, _) = await SnapshotAndSyncAsync(vol, b0.TargetName, "s1");

            // An outstanding transfer exists while the backup is disrupted.
            await PutAsync(vol, model, "/pending/p.bin", TestData.Bytes(5, 256 * 1024));
            DfsSnapshotInfo s2 = await SnapshotAsync(vol, "s2");
            TreeModel model2 = model.Clone();
            FaultArm? blocked = null;
            switch (disruption)
            {
                case BackupDisruption.Paused:
                    await b0.PauseAsync();
                    break;
                case BackupDisruption.Crashed:
                    await b0.CrashAsync();
                    break;
                case BackupDisruption.Isolated:
                    CutFromSources(cluster, b0.Name);
                    break;
                case BackupDisruption.BlackHoled:
                    BlackHoleFromSources(cluster, b0.Name);
                    break;
                case BackupDisruption.BlockedMidTransfer:
                    blocked = cluster.Faults.Arm(new FaultMatch(FaultSites.BackupObjectReceived, Node: b0.Name, Volume: Volume, Count: int.MaxValue),
                        FaultAction.Block());
                    break;
            }

            await vol.Backups.RequestSyncAsync(b0.TargetName, "s2", Ct);
            if (blocked is not null)
            {
                await blocked.WaitTriggeredAsync(FaultTimeout);
                cluster.Faults.AssertReached(blocked);
            }

            // Ordinary stream flush/commit and helpers complete within the live bound.
            const string path = "/live/stream.bin";
            byte[] part1 = TestData.Bytes(6, 150 * 1024);
            byte[] part2 = TestData.Bytes(7, 90 * 1024);
            await vol.Directories.CreateDirectoryAsync("/live", Ct);
            CommitToken streamToken = CommitToken.None;
            await using (DfsStream stream = await vol.Files.CreateAsync(path, Ct))
            {
                await stream.WriteAsync(part1, Ct);
                TimeSpan flush = await TimeAsync(cluster, () => stream.FlushAsync(Ct));
                Assert.True(flush <= LiveCommitBound, $"FlushAsync with the backup {disruption} took {flush}");
                await stream.WriteAsync(part2, Ct);
                TimeSpan commit = await TimeAsync(cluster, async () => streamToken = await stream.CommitAsync(Ct));
                Assert.True(commit <= LiveCommitBound, $"CommitAsync with the backup {disruption} took {commit}");
            }

            Assert.False(streamToken.IsNone);
            byte[] expected = [.. part1, .. part2];
            model.Put(path, expected);
            await AssertLiveCommitUnblockedAsync(cluster, vol, model, "/live/helper.bin", TestData.Bytes(8, 70 * 1024), $"WriteAllBytes with the backup {disruption}");

            await using DfsClient other = await ConnectLiveAsync(cluster, NextClientName("c"));
            await AssertFileBytesAsync(other.GetVolume(Volume).Files, path, expected, "stream publication seen by another client");

            // Durability was established by live nodes only and meets the live policy.
            DfsStoragePolicy policy = cluster.Options.GetEffectiveDefaultPolicy();
            var liveNames = cluster.Nodes.Select(n => n.Name).ToHashSet(StringComparer.Ordinal);
            foreach (ObjectId id in await FileObjectsAsync(cluster, [path, "/live/helper.bin"]))
            {
                IReadOnlyList<string> holders = await cluster.Inspect.GetObjectHoldersAsync(id);
                Assert.All(holders, h => Assert.True(liveNames.Contains(h), $"object {id} is held by non-live node {h}"));
                (int copies, int zones) = await cluster.Inspect.CountDurableDomainsAsync(id);
                Assert.True(copies >= policy.DurableCopiesBeforeAck && zones >= policy.DurableZonesBeforeAck,
                    $"object {id} has {copies} copies in {zones} zones; the live ack policy needs {policy.DurableCopiesBeforeAck}/{policy.DurableZonesBeforeAck}");
            }

            // The acknowledgement gave no backup guarantee.
            DfsBackupStatus status = await vol.Backups.GetStatusAsync(b0.TargetName, Ct);
            Assert.Equal(s1.Boundary, status.PublishedBoundary);
            Assert.False(status.PublishedBoundary.IsAtOrAfter(streamToken), "a live flush must not be reported as backed up");

            // Undo the disruption; the backup catches up on its own schedule.
            switch (disruption)
            {
                case BackupDisruption.Paused:
                    await b0.ResumeAsync();
                    break;
                case BackupDisruption.Crashed:
                    await b0.RestartAsync();
                    break;
                case BackupDisruption.Isolated:
                case BackupDisruption.BlackHoled:
                    cluster.Network.Heal();
                    break;
                case BackupDisruption.BlockedMidTransfer:
                    blocked!.Release();
                    blocked.Disarm();
                    break;
            }

            DfsBackupCheckpoint cp2 = await SyncExactAsync(vol, b0.TargetName, s2);
            Assert.Equal(s2.Boundary, cp2.CoveredCommit);
            await using DfsClient reader = await ConnectBackupReaderAsync(cluster, 0, NextClientName("bk"));
            await AssertBackupSnapshotAsync(reader, "s2", model2, $"s2 after the backup recovered from {disruption}");
        });

    [Fact]
    [Catalog("BAK-003", "S", "INV-15", "INV-08", "INV-11")]
    public Task BAK_003_BackupNotInOrdinaryAckSet_StrictPolicyNeverCountsBackup() =>
        ClusterTest.RunAsync(Options() with
        {
            Volumes =
            [
                new TestVolumeSpec(Volume)
                {
                    NamedPolicies = new Dictionary<string, DfsStoragePolicy>(StringComparer.Ordinal)
                    {
                        ["strict"] = new DfsStoragePolicy { TargetCopies = 3, TargetDistinctZones = 3, DurableCopiesBeforeAck = 3, DurableZonesBeforeAck = 3 },
                    },
                },
            ],
        }, async cluster =>
        {
            TestBackupNode b0 = cluster.BackupNodes[0];
            await using DfsClient live = await ConnectLiveAsync(cluster);
            DfsVolume vol = live.GetVolume(Volume);
            await RegisterAsync(b0);
            TreeModel model = InitialTree(9);
            await WriteTreeAsync(vol, model);
            await SnapshotAndSyncAsync(vol, b0.TargetName, "s1");

            // One live host down: the metadata majority survives, the strict data policy cannot be met by live nodes.
            TestNode leader = await cluster.GetLeaderAsync(Volume, Ct);
            TestNode victim = cluster.Nodes.First(n => !string.Equals(n.Name, leader.Name, StringComparison.Ordinal));
            await victim.CrashAsync();

            byte[] data = TestData.Bytes(10, 128 * 1024);
            var strict = new DfsMutationOptions { StoragePolicy = "strict" };
            DfsPlacementUnavailableException refused = await AssertEx.ThrowsDfsAsync<DfsPlacementUnavailableException>(
                () => vol.Files.WriteAllBytesAsync("/strict.bin", data, strict, Ct), DfsErrorCode.PlacementUnavailable);
            Assert.Equal(3, refused.RequiredCopies);
            Assert.True(refused.AchievedCopies <= 2, $"a healthy backup must never count as a live copy (achieved {refused.AchievedCopies})");
            Assert.Equal(3, refused.RequiredZones);
            Assert.True(refused.AchievedZones <= 2, $"a backup must never count as a live zone (achieved {refused.AchievedZones})");
            Assert.False(await vol.Files.ExistsAsync("/strict.bin", Ct), "a refused publication must not create the file");
            Assert.Equal(TestNodeState.Running, b0.State);

            // The default (ack two) policy still commits with two live hosts.
            await AssertLiveCommitUnblockedAsync(cluster, vol, model, "/default.bin", TestData.Bytes(11, 100 * 1024), "default policy with one live host down");

            await victim.RestartAsync();
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(30), Ct);
            CommitToken ok = await vol.Files.WriteAllBytesAsync("/strict.bin", data, strict, Ct);
            Assert.False(ok.IsNone);
            var liveNames = cluster.Nodes.Select(n => n.Name).ToHashSet(StringComparer.Ordinal);
            foreach (ObjectId id in await FileObjectsAsync(cluster, ["/strict.bin"]))
            {
                IReadOnlyList<string> holders = await cluster.Inspect.GetObjectHoldersAsync(id);
                Assert.All(holders, h => Assert.True(liveNames.Contains(h), $"strict object {id} held by non-live {h}"));
                (int copies, int zones) = await cluster.Inspect.CountDurableDomainsAsync(id);
                Assert.True(copies >= 3 && zones >= 3, $"strict object {id}: {copies} copies / {zones} zones");
            }
        });

    // ------------------------------------------------------------------------------------------------------------
    // BAK-004
    // ------------------------------------------------------------------------------------------------------------

    public enum PrePublishPoint
    {
        FirstObjectReceived,
        LastObjectReceived,
        BeforeClosureVerify,
        BeforePointer,
    }

    [Theory]
    [InlineData(PrePublishPoint.FirstObjectReceived)]
    [InlineData(PrePublishPoint.LastObjectReceived)]
    [InlineData(PrePublishPoint.BeforeClosureVerify)]
    [InlineData(PrePublishPoint.BeforePointer)]
    [Catalog("BAK-004", "S", "INV-10", "INV-19")]
    public Task BAK_004_InitialSyncHasNoPartialPublishedTree(PrePublishPoint point) =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            TestBackupNode b0 = cluster.BackupNodes[0];
            await using DfsClient live = await ConnectLiveAsync(cluster);
            DfsVolume vol = live.GetVolume(Volume);
            await RegisterAsync(b0);
            TreeModel model = InitialTree(4);
            await WriteTreeAsync(vol, model);
            DfsSnapshotInfo s1 = await SnapshotAsync(vol, "s1");
            int objectCount = (await FileObjectsAsync(cluster, model.Files.Keys)).Count;
            Assert.True(objectCount >= 3, $"the initial tree must need several data objects (has {objectCount})");

            FaultMatch match = point switch
            {
                PrePublishPoint.FirstObjectReceived => new FaultMatch(FaultSites.BackupObjectReceived, Node: b0.Name, Volume: Volume, Occurrence: 1),
                PrePublishPoint.LastObjectReceived => new FaultMatch(FaultSites.BackupObjectReceived, Node: b0.Name, Volume: Volume, Occurrence: objectCount),
                PrePublishPoint.BeforeClosureVerify => new FaultMatch(FaultSites.BackupBeforeClosureVerify, Node: b0.Name, Volume: Volume),
                _ => new FaultMatch(FaultSites.BackupBeforePointer, Node: b0.Name, Volume: Volume),
            };
            FaultArm arm = cluster.Faults.Arm(match, FaultAction.Block());
            await vol.Backups.RequestSyncAsync(b0.TargetName, "s1", Ct);
            await arm.WaitTriggeredAsync(FaultTimeout);
            cluster.Faults.AssertReached(arm);

            string because = $"first transfer held at {point}";
            await using DfsClient reader = await ConnectBackupReaderAsync(cluster);
            Assert.True(reader.GetVolume(Volume).Snapshots.IsReadOnly);
            IReadOnlyList<DfsSnapshotInfo> listed = await reader.GetVolume(Volume).Snapshots.ListAsync(Ct);
            Assert.True(listed.Count == 0, $"{because}: nothing may be listed, got [{string.Join(", ", listed.Select(s => s.Name))}]");
            await AssertNotSelectableAsync(reader, "s1", because);
            await AssertPublishedStateAsync(b0, null, [], ["s1"], because);
            Assert.Empty(await b0.Inspect.GetSnapshotsAsync(Volume));

            DfsBackupStatus status = await vol.Backups.GetStatusAsync(b0.TargetName, Ct);
            Assert.True(status.PublishedBoundary.IsNone, $"{because}: published boundary must be none, got {status.PublishedBoundary}");
            Assert.Null(status.LatestPublished);
            if (point != PrePublishPoint.BeforePointer)
                Assert.True(status.DurableBoundary.IsNone, $"{because}: no durable recovery boundary before closure verification, got {status.DurableBoundary}");

            DfsBackupNotReachedException notReached = await Assert.ThrowsAsync<DfsBackupNotReachedException>(
                () => vol.Backups.AwaitBackupSnapshotAsync(b0.TargetName, "s1", ShortAwait, Ct));
            Assert.Equal(b0.TargetName, notReached.Target);
            Assert.Null(notReached.LastPublished);
            Assert.Equal(DfsErrorCode.BackupNotReached, notReached.ErrorCode);

            arm.Release();
            DfsBackupCheckpoint cp = await vol.Backups.AwaitBackupSnapshotAsync(b0.TargetName, "s1", SyncTimeout, Ct);
            AssertExactCheckpoint(cp, b0.TargetName, s1);
            await AssertPublishedStateAsync(b0, "s1", ["s1"], [], "after release");
            await AssertBackupSnapshotAsync(reader, "s1", model, "first snapshot after publication");
        });

    // ------------------------------------------------------------------------------------------------------------
    // BAK-005
    // ------------------------------------------------------------------------------------------------------------

    [Fact]
    [Catalog("BAK-005", "S", "INV-10", "INV-09")]
    public Task BAK_005_IncrementalTransferSendsMissingObjects() =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            TestBackupNode b0 = cluster.BackupNodes[0];
            await using DfsClient live = await ConnectLiveAsync(cluster);
            DfsVolume vol = live.GetVolume(Volume);
            await RegisterAsync(b0);

            var model = new TreeModel();
            for (int i = 0; i < 4; i++)
                model.Put($"/inc/f{i}.bin", TestData.Bytes(50 + (ulong)i, 256 * 1024));
            model.Put("/inc/small.txt", "inline"u8.ToArray());
            await WriteTreeAsync(vol, model);
            await SnapshotAndSyncAsync(vol, b0.TargetName, "s1");
            TreeModel model1 = model.Clone();
            HashSet<ObjectId> objects1 = await FileObjectsAsync(cluster, model1.Files.Keys);
            IReadOnlyList<ObjectRecordInfo> records1 = await b0.Inspect.ListObjectsAsync();
            Assert.Equal(records1.Count, records1.Select(r => r.Id).Distinct().Count());
            Assert.Subset(records1.Select(r => r.Id).ToHashSet(), objects1);

            // Small changes: an in-place 100-byte patch inside one chunk, an append, a new file, a deletion.
            byte[] f1 = model.Files["/inc/f1.bin"].ToArray();
            byte[] patch = TestData.Bytes(60, 100);
            await using (DfsStream s = await vol.Files.OpenAsync("/inc/f1.bin", FileMode.Open, FileAccess.ReadWrite, FileShare.Read, Ct))
            {
                s.Seek(70_000, SeekOrigin.Begin);
                await s.WriteAsync(patch, Ct);
                Assert.False((await s.CommitAsync(Ct)).IsNone);
            }

            patch.CopyTo(f1, 70_000);
            model.Put("/inc/f1.bin", f1);
            byte[] suffix = TestData.Bytes(61, 10);
            Assert.False((await vol.Files.AppendAllBytesAsync("/inc/f2.bin", suffix, Ct)).IsNone);
            model.Put("/inc/f2.bin", [.. model.Files["/inc/f2.bin"], .. suffix]);
            await PutAsync(vol, model, "/inc/new.bin", TestData.Bytes(62, 70 * 1024));
            await DeleteFileAsync(vol, model, "/inc/f3.bin");

            HashSet<ObjectId> objects2 = await FileObjectsAsync(cluster, model.Files.Keys);
            var fresh = objects2.Except(objects1).ToHashSet();
            var unchanged = objects2.Intersect(objects1).ToHashSet();
            Assert.NotEmpty(fresh);
            Assert.NotEmpty(unchanged);
            Dictionary<ObjectId, ObjectRecordInfo> liveRecords = await LiveObjectRecordsAsync(cluster);
            long freshBytes = fresh.Sum(id => liveRecords[id].Length);

            cluster.Network.Log.Reset();
            int hitsBefore = Hits(cluster, FaultSites.BackupObjectReceived, b0.Name).Count;
            (DfsSnapshotInfo s2, _) = await SnapshotAndSyncAsync(vol, b0.TargetName, "s2");

            var received = Hits(cluster, FaultSites.BackupObjectReceived, b0.Name).Skip(hitsBefore)
                .Where(h => h.Object.HasValue).Select(h => h.Object.GetValueOrDefault()).ToList();
            Assert.Subset(received.ToHashSet(), fresh);
            Assert.True(received.All(id => !objects1.Contains(id)),
                $"objects already verified on the destination must not be re-sent: [{string.Join(", ", received.Where(objects1.Contains))}]");
            long putMessages = cluster.Network.Log.CountMessages(new MessageFilter(To: b0.Name, Type: MessageType.BackupPutObject));
            Assert.InRange(putMessages, 1, fresh.Count);
            long putBytes = cluster.Network.Log.CountBytes(new MessageFilter(To: b0.Name, Type: MessageType.BackupPutObject));
            const long perMessageOverhead = 4096;
            Assert.True(putBytes <= freshBytes + (putMessages * perMessageOverhead),
                $"incremental object traffic {putBytes} B exceeds new objects {freshBytes} B plus framing");
            Assert.True(putBytes < model.TotalFileBytes / 2, $"incremental traffic {putBytes} B must be far below the snapshot size {model.TotalFileBytes} B");

            // Retained objects are reused unchanged and stored once.
            IReadOnlyList<ObjectRecordInfo> records2 = await b0.Inspect.ListObjectsAsync();
            Assert.Equal(records2.Count, records2.Select(r => r.Id).Distinct().Count());
            Dictionary<ObjectId, ObjectRecordInfo> byId2 = records2.ToDictionary(r => r.Id);
            foreach (ObjectRecordInfo r1 in records1)
            {
                Assert.True(byId2.TryGetValue(r1.Id, out ObjectRecordInfo? r2), $"object {r1.Id} retained for s1 disappeared");
                Assert.Equal(r1.Checksum, r2.Checksum);
                Assert.Equal(r1.Length, r2.Length);
            }

            Assert.Subset(byId2.Keys.ToHashSet(), objects2);

            IReadOnlyList<BackupSnapshotInspection> snaps = await b0.Inspect.GetSnapshotsAsync(Volume);
            BackupSnapshotInspection bs1 = snaps.Single(s => s.Name == "s1");
            BackupSnapshotInspection bs2 = snaps.Single(s => s.Name == "s2");
            Assert.True(objects1.SetEquals(bs1.Closure), "s1 closure on the backup must equal the source dependency set");
            Assert.True(objects2.SetEquals(bs2.Closure), "s2 closure on the backup must equal the source dependency set");
            Assert.Equal(s2.Boundary, bs2.CoveredCommit);
            Assert.Equal(bs1.Lineage, bs2.Lineage);

            await using DfsClient reader = await ConnectBackupReaderAsync(cluster);
            await AssertBackupSnapshotAsync(reader, "s1", model1, "s1 after incremental s2");
            await AssertBackupSnapshotAsync(reader, "s2", model, "incremental s2");
        });

    // ------------------------------------------------------------------------------------------------------------
    // BAK-006 (test plan §9.5)
    // ------------------------------------------------------------------------------------------------------------

    [Fact]
    [Catalog("BAK-006", "S", "INV-10", "INV-15", "INV-19")]
    public Task BAK_006_ManifestBeforeDataNotPublished() =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            TestBackupNode b0 = cluster.BackupNodes[0];
            await using DfsClient live = await ConnectLiveAsync(cluster);
            DfsVolume vol = live.GetVolume(Volume);
            await RegisterAsync(b0);
            TreeModel model = InitialTree(6);
            await WriteTreeAsync(vol, model);
            (DfsSnapshotInfo s1, _) = await SnapshotAndSyncAsync(vol, b0.TargetName, "s1");
            TreeModel model1 = model.Clone();

            // A reader pins S1 and holds an open stream.
            await using DfsClient pinnedClient = await ConnectBackupReaderAsync(cluster, 0, NextClientName("pinned"));
            DfsSnapshotView pinned = await pinnedClient.GetVolume(Volume).Snapshots.OpenAsync("s1", Ct);
            DfsStream pinnedStream = await pinned.Files.OpenReadAsync("/docs/a.bin", Ct);
            byte[] head = new byte[4096];
            await pinnedStream.ReadExactlyAsync(head, Ct);
            AssertEx.BytesEqual(model1.Files["/docs/a.bin"].AsSpan(0, head.Length), head, "pinned S1 reader before S2");

            // S2: a multi-file transaction and several new data objects.
            byte[] ta = TestData.Bytes(70, 130 * 1024);
            byte[] tb = TestData.Bytes(71, 130 * 1024);
            byte[] tc = TestData.Bytes(72, 200 * 1024);
            await using (DfsTransaction tx = await vol.Transactions.BeginAsync(null, Ct))
            {
                await tx.Directories.CreateDirectoryAsync("/tx", Ct);
                await tx.Files.WriteAllBytesAsync("/tx/a.bin", ta, Ct);
                await tx.Files.WriteAllBytesAsync("/tx/b.bin", tb, Ct);
                await tx.Files.WriteAllBytesAsync("/docs/a.bin", tc, Ct);
                Assert.False((await tx.CommitAsync(Ct)).IsNone);
            }

            model.Put("/tx/a.bin", ta);
            model.Put("/tx/b.bin", tb);
            model.Put("/docs/a.bin", tc);
            DfsSnapshotInfo s2 = await SnapshotAsync(vol, "s2");

            HashSet<ObjectId> held = await BackupObjectsAsync(b0);
            HashSet<ObjectId> closure2 = await FileObjectsAsync(cluster, model.Files.Keys);
            var needed = closure2.Except(held).ToList();
            Assert.True(needed.Count >= 3, $"S2 must need several new objects (needs {needed.Count})");
            ObjectId withheld = Highest(needed);

            async Task AssertOnlyS1Async(string because)
            {
                await AssertPublishedStateAsync(b0, "s1", ["s1"], ["s2"], because);
                await using DfsClient probe = await ConnectBackupReaderAsync(cluster, 0, NextClientName("probe"));
                await AssertNotSelectableAsync(probe, "s2", because);
                await AssertBackupSnapshotAsync(probe, "s1", model1, because);
                DfsBackupStatus st = await vol.Backups.GetStatusAsync(b0.TargetName, Ct);
                Assert.Equal(s1.Boundary, st.PublishedBoundary);
                Assert.Equal("s1", st.LatestPublished?.SnapshotName);
                Assert.False(st.DurableBoundary.IsComparableWith(s2.Boundary) && st.DurableBoundary.IsAtOrAfter(s2.Boundary),
                    $"{because}: the durable recovery boundary must not cover S2 yet ({st.DurableBoundary})");
            }

            async Task AssertPinnedS1Async(bool mayHaveFailed, string because)
            {
                try
                {
                    pinnedStream.Position = 0;
                    byte[] all = new byte[model1.Files["/docs/a.bin"].Length];
                    await pinnedStream.ReadExactlyAsync(all, Ct);
                    AssertEx.BytesEqual(model1.Files["/docs/a.bin"], all, because);
                    Assert.Equal("s1", pinned.Info.Name);
                    Assert.Equal(s1.Boundary, pinned.Info.Boundary);
                }
                catch (Exception ex) when (mayHaveFailed && ex is IOException or ObjectDisposedException or TimeoutException)
                {
                    // The pin lived in the crashed process: an explicit failure is legal, other contents never are.
                }
            }

            await using (ManualBackupExport export = await cluster.BeginManualBackupExportAsync(Volume, "s2", 0, Ct))
            {
                Assert.Equal(s2.Id, export.SnapshotId);
                Assert.Equal(s2.Boundary, export.Boundary);
                Assert.True(closure2.SetEquals(export.Closure), "the exported closure must equal the snapshot's extent references");

                // Manifest/root first, while the inventory hint falsely claims the withheld dependency exists (§9.5).
                Assert.Equal(DfsErrorCode.None, await export.BeginAsync());
                await b0.SetFalseInventoryClaimsAsync(Volume, [withheld]);
                IReadOnlySet<ObjectId> hint = await export.RequestInventoryAsync();
                Assert.Contains(withheld, hint);
                Assert.Equal(DfsErrorCode.None, await export.SendManifestAsync());
                BackupReceiveInspection? recv = await b0.Inspect.GetReceiveStateAsync(Volume);
                Assert.NotNull(recv);
                Assert.True(recv.ManifestReceived);
                Assert.True(recv.MissingObjects.SetEquals(needed),
                    "the destination must compute the missing closure itself, ignoring its own (false) inventory hint");
                await AssertOnlyS1Async("manifest delivered before any new data");
                await AssertPinnedS1Async(false, "pinned S1 after the S2 manifest arrived");

                // Every dependency but one.
                foreach (ObjectId id in needed.Where(i => i != withheld))
                    Assert.Equal(DfsErrorCode.None, await export.SendObjectAsync(id));
                recv = await b0.Inspect.GetReceiveStateAsync(Volume);
                Assert.NotNull(recv);
                Assert.True(recv.MissingObjects.SetEquals([withheld]), $"only {withheld} may be missing");
                Assert.False(recv.DurableObjects.Contains(withheld));

                BackupPublishAttempt early = await export.PublishAsync();
                Assert.False(early.Published, "publication with a missing dependency must be refused");
                Assert.NotEqual(DfsErrorCode.None, early.Error);
                Assert.Equal([withheld], early.MissingObjects);
                Assert.Null(early.Checkpoint);
                await AssertOnlyS1Async("publish refused with one dependency missing");
                await AssertPinnedS1Async(false, "pinned S1 after the refused publication");

                // Cold restart of the backup before the last dependency is durable.
                await b0.CrashAsync(CrashMode.LoseUnflushed);
                await b0.RestartAsync();
                await AssertOnlyS1Async("cold restart with S2 incomplete");
                await AssertPinnedS1Async(true, "pinned S1 reader across the backup cold restart");

                // Resume: durably verified dependencies are reused; only the withheld object is still missing.
                Assert.Equal(DfsErrorCode.None, await export.BeginAsync());
                Assert.Equal(DfsErrorCode.None, await export.SendManifestAsync());
                BackupPublishAttempt resumed = await export.PublishAsync();
                Assert.False(resumed.Published);
                Assert.Equal([withheld], resumed.MissingObjects);

                Assert.Equal(DfsErrorCode.None, await export.SendObjectAsync(withheld));
                BackupPublishAttempt done = await export.PublishAsync();
                Assert.True(done.Published, $"publication with the full closure durable must succeed ({done.Error})");
                Assert.Equal(DfsErrorCode.None, done.Error);
                Assert.Empty(done.MissingObjects);
                Assert.NotNull(done.Checkpoint);
                AssertExactCheckpoint(done.Checkpoint, b0.TargetName, s2);
            }

            await AssertPublishedStateAsync(b0, "s2", ["s1", "s2"], [], "after the full closure became durable");
            await AssertPinnedS1Async(true, "pinned S1 reader after S2 was published");
            await using (DfsClient reader = await ConnectBackupReaderAsync(cluster, 0, NextClientName("bk")))
            {
                await AssertBackupSnapshotAsync(reader, "s2", model, "S2 published after its closure became durable");
                await AssertBackupSnapshotAsync(reader, "s1", model1, "S1 after S2 publication");
            }

            // After publication, a cold restart with every source unreachable still serves the complete S2.
            CutFromSources(cluster, b0.Name);
            await b0.CrashAsync(CrashMode.LoseUnflushed);
            await b0.RestartAsync();
            await AssertPublishedStateAsync(b0, "s2", ["s1", "s2"], [], "cold restart after publication, sources unreachable");
            await using DfsClient after = await ConnectBackupReaderAsync(cluster, 0, NextClientName("bk"));
            await AssertBackupSnapshotAsync(after, "s2", model, "S2 after cold restart without source metadata");
            await AssertBackupSnapshotAsync(after, "s1", model1, "S1 after cold restart without source metadata");

            try
            {
                await pinnedStream.DisposeAsync();
                await pinned.DisposeAsync();
            }
            catch (Exception ex) when (ex is IOException or ObjectDisposedException)
            {
            }
        });

    [Fact]
    [Catalog("BAK-006", "S", "INV-10", "INV-19")]
    public Task BAK_006_ManifestBeforeDataNotPublished_ExporterDependencyDelayed() =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            TestBackupNode b0 = cluster.BackupNodes[0];
            await using DfsClient live = await ConnectLiveAsync(cluster);
            DfsVolume vol = live.GetVolume(Volume);
            await RegisterAsync(b0, new DfsBackupBudgets(TimeSpan.FromHours(1), 1L << 30, MaxConcurrentObjects: 32));
            TreeModel model = InitialTree(12);
            await WriteTreeAsync(vol, model);
            (DfsSnapshotInfo s1, _) = await SnapshotAndSyncAsync(vol, b0.TargetName, "s1");
            TreeModel model1 = model.Clone();

            for (int i = 0; i < 4; i++)
                await PutAsync(vol, model, $"/delay/n{i}.bin", TestData.Bytes(120 + (ulong)i, 130 * 1024));
            DfsSnapshotInfo s2 = await SnapshotAsync(vol, "s2");
            var needed = (await FileObjectsAsync(cluster, model.Files.Keys)).Except(await BackupObjectsAsync(b0)).ToList();
            Assert.True(needed.Count >= 4);
            ObjectId withheld = Lowest(needed);

            var gate = new TaskCompletionSource();
            int delayedHits = 0;
            FaultArm delay = cluster.Faults.Arm(
                new FaultMatch(FaultSites.BackupObjectReceived, Node: b0.Name, Volume: Volume, Count: int.MaxValue),
                FaultAction.Custom(async ctx =>
                {
                    if (ctx.Object == withheld)
                    {
                        delayedHits++;
                        await gate.Task;
                    }
                }));

            await vol.Backups.RequestSyncAsync(b0.TargetName, "s2", Ct);
            await cluster.WaitUntilAsync(async () =>
            {
                BackupReceiveInspection? r = await b0.Inspect.GetReceiveStateAsync(Volume);
                return r is not null && r.ReceivedObjects.Contains(withheld) && needed.Where(i => i != withheld).All(r.DurableObjects.Contains);
            }, FaultTimeout, "every other S2 dependency durable while one referenced object is held before its flush", Ct);
            cluster.Faults.AssertReached(delay);
            Assert.True(delayedHits >= 1);

            // Hold the dependency for a long (virtual) time; S2 must stay unpublishable throughout.
            for (int round = 0; round < 3; round++)
            {
                string because = $"one S2 dependency not durable (round {round})";
                BackupReceiveInspection? state = await b0.Inspect.GetReceiveStateAsync(Volume);
                Assert.NotNull(state);
                Assert.False(state.DurableObjects.Contains(withheld), because);
                if (state.ManifestReceived)
                    Assert.Contains(withheld, state.MissingObjects);
                await AssertPublishedStateAsync(b0, "s1", ["s1"], ["s2"], because);
                await using (DfsClient probe = await ConnectBackupReaderAsync(cluster, 0, NextClientName("probe")))
                {
                    await AssertNotSelectableAsync(probe, "s2", because);
                    await AssertBackupSnapshotAsync(probe, "s1", model1, because);
                }

                DfsBackupStatus st = await vol.Backups.GetStatusAsync(b0.TargetName, Ct);
                Assert.Equal(s1.Boundary, st.PublishedBoundary);
                Assert.NotEqual(s2.Boundary, st.DurableBoundary);
                await cluster.DelayAsync(TimeSpan.FromSeconds(5), Ct);
            }

            DfsBackupNotReachedException notReached = await Assert.ThrowsAsync<DfsBackupNotReachedException>(
                () => vol.Backups.AwaitBackupSnapshotAsync(b0.TargetName, "s2", ShortAwait, Ct));
            Assert.Equal("s1", notReached.LastPublished?.SnapshotName);

            gate.SetResult();
            DfsBackupCheckpoint cp = await vol.Backups.AwaitBackupSnapshotAsync(b0.TargetName, "s2", SyncTimeout, Ct);
            AssertExactCheckpoint(cp, b0.TargetName, s2);
            delay.Disarm();
            await AssertPublishedStateAsync(b0, "s2", ["s1", "s2"], [], "after the held dependency became durable");
            await using DfsClient reader = await ConnectBackupReaderAsync(cluster, 0, NextClientName("bk"));
            await AssertBackupSnapshotAsync(reader, "s2", model, "S2 after the delayed dependency");
            await AssertBackupSnapshotAsync(reader, "s1", model1, "S1 unchanged");
        });

    // ------------------------------------------------------------------------------------------------------------
    // BAK-007
    // ------------------------------------------------------------------------------------------------------------

    [Fact]
    [Catalog("BAK-007", "S", "INV-10", "INV-14")]
    public Task BAK_007_FalseInventoryPositiveCannotOmitData() =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            TestBackupNode b0 = cluster.BackupNodes[0];
            await using DfsClient live = await ConnectLiveAsync(cluster);
            DfsVolume vol = live.GetVolume(Volume);
            await RegisterAsync(b0);
            TreeModel model = InitialTree(7);
            await WriteTreeAsync(vol, model);
            await SnapshotAndSyncAsync(vol, b0.TargetName, "s1");
            TreeModel model1 = model.Clone();

            await PutAsync(vol, model, "/inv/n1.bin", TestData.Bytes(80, 200 * 1024));
            await PutAsync(vol, model, "/inv/n2.bin", TestData.Bytes(81, 130 * 1024));
            DfsSnapshotInfo s2 = await SnapshotAsync(vol, "s2");
            var needed = (await FileObjectsAsync(cluster, model.Files.Keys)).Except(await BackupObjectsAsync(b0)).ToList();
            Assert.True(needed.Count >= 2);
            ObjectId falseClaim = Lowest(needed);
            await b0.SetFalseInventoryClaimsAsync(Volume, [falseClaim]);

            FaultArm beforePointer = cluster.Faults.Arm(new FaultMatch(FaultSites.BackupBeforePointer, Node: b0.Name, Volume: Volume), FaultAction.Block());
            int hitsBefore = Hits(cluster, FaultSites.BackupObjectReceived, b0.Name).Count(h => h.Object == falseClaim);
            await vol.Backups.RequestSyncAsync(b0.TargetName, "s2", Ct);
            await beforePointer.WaitTriggeredAsync(SyncTimeout);
            cluster.Faults.AssertReached(beforePointer);

            // The pointer step is reached only after the destination itself verified the complete closure.
            Assert.Contains(falseClaim, await BackupObjectsAsync(b0));
            BackupReceiveInspection? recv = await b0.Inspect.GetReceiveStateAsync(Volume);
            Assert.NotNull(recv);
            Assert.Empty(recv.MissingObjects);
            Assert.Contains(falseClaim, recv.DurableObjects);
            int hitsAfter = Hits(cluster, FaultSites.BackupObjectReceived, b0.Name).Count(h => h.Object == falseClaim);
            Assert.True(hitsAfter > hitsBefore, "the falsely claimed object must be requested and transferred after final verification found the gap");
            await AssertPublishedStateAsync(b0, "s1", ["s1"], ["s2"], "before the pointer is replaced");

            beforePointer.Release();
            DfsBackupCheckpoint cp = await vol.Backups.AwaitBackupSnapshotAsync(b0.TargetName, "s2", SyncTimeout, Ct);
            AssertExactCheckpoint(cp, b0.TargetName, s2);
            await AssertPublishedStateAsync(b0, "s2", ["s1", "s2"], [], "after publication");
            await using DfsClient reader = await ConnectBackupReaderAsync(cluster);
            await AssertBackupSnapshotAsync(reader, "s2", model, "S2 despite a false inventory claim");
            await AssertBackupSnapshotAsync(reader, "s1", model1, "S1 unchanged");
            await b0.SetFalseInventoryClaimsAsync(Volume, []);
        });

    [Fact]
    [Catalog("BAK-007", "S", "INV-10", "INV-14")]
    public Task BAK_007_FalseInventoryPositiveCannotOmitData_HintTrustingExporter() =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            TestBackupNode b0 = cluster.BackupNodes[0];
            await using DfsClient live = await ConnectLiveAsync(cluster);
            DfsVolume vol = live.GetVolume(Volume);
            await RegisterAsync(b0);
            TreeModel model = InitialTree(13);
            await WriteTreeAsync(vol, model);
            await SnapshotAndSyncAsync(vol, b0.TargetName, "s1");
            TreeModel model1 = model.Clone();

            await PutAsync(vol, model, "/inv/n1.bin", TestData.Bytes(82, 200 * 1024));
            DfsSnapshotInfo s2 = await SnapshotAsync(vol, "s2");
            var needed = (await FileObjectsAsync(cluster, model.Files.Keys)).Except(await BackupObjectsAsync(b0)).ToList();
            ObjectId falseClaim = Highest(needed);
            await b0.SetFalseInventoryClaimsAsync(Volume, [falseClaim]);

            await using ManualBackupExport export = await cluster.BeginManualBackupExportAsync(Volume, "s2", 0, Ct);
            Assert.Equal(DfsErrorCode.None, await export.BeginAsync());
            IReadOnlySet<ObjectId> hint = await export.RequestInventoryAsync();
            Assert.Contains(falseClaim, hint);
            Assert.Equal(DfsErrorCode.None, await export.SendManifestAsync());

            // A naive exporter trusts the hint and skips everything it claims.
            foreach (ObjectId id in export.Closure.Where(i => !hint.Contains(i)))
                Assert.Equal(DfsErrorCode.None, await export.SendObjectAsync(id));
            BackupPublishAttempt refused = await export.PublishAsync();
            Assert.False(refused.Published, "final completeness verification must refuse a closure with a falsely claimed object");
            Assert.NotEqual(DfsErrorCode.None, refused.Error);
            Assert.Equal([falseClaim], refused.MissingObjects);
            await AssertPublishedStateAsync(b0, "s1", ["s1"], ["s2"], "hint-trusting transfer refused");
            await using (DfsClient probe = await ConnectBackupReaderAsync(cluster, 0, NextClientName("probe")))
                await AssertNotSelectableAsync(probe, "s2", "hint-trusting transfer refused");

            Assert.Equal(DfsErrorCode.None, await export.SendObjectAsync(falseClaim));
            BackupPublishAttempt ok = await export.PublishAsync();
            Assert.True(ok.Published, $"publication after the gap was filled must succeed ({ok.Error})");
            Assert.NotNull(ok.Checkpoint);
            AssertExactCheckpoint(ok.Checkpoint, b0.TargetName, s2);
            await AssertPublishedStateAsync(b0, "s2", ["s1", "s2"], [], "after the gap was filled");
            await using DfsClient reader = await ConnectBackupReaderAsync(cluster, 0, NextClientName("bk"));
            await AssertBackupSnapshotAsync(reader, "s2", model, "S2 after the gap was filled");
            await AssertBackupSnapshotAsync(reader, "s1", model1, "S1 unchanged");
        });

    // ------------------------------------------------------------------------------------------------------------
    // BAK-008
    // ------------------------------------------------------------------------------------------------------------

    public enum TransferPhase
    {
        SourcePin,
        Inventory,
        FirstObject,
        MiddleObject,
        LastObject,
        BeforeClosureVerify,
        BeforePointer,
    }

    public enum Interruption
    {
        BackupCrash,
        BackupIoError,
        NetworkCut,
        SourceLeaderCrash,
    }

    public static TheoryData<TransferPhase, Interruption> InterruptionMatrix
    {
        get
        {
            var data = new TheoryData<TransferPhase, Interruption>();
            foreach (TransferPhase phase in Enum.GetValues<TransferPhase>())
            {
                foreach (Interruption interruption in Enum.GetValues<Interruption>())
                    data.Add(phase, interruption);
            }

            return data;
        }
    }

    [Theory]
    [MemberData(nameof(InterruptionMatrix))]
    [Catalog("BAK-008", "S", "INV-10", "INV-13", "INV-19")]
    public Task BAK_008_InterruptedTransferKeepsPreviousSnapshot(TransferPhase phase, Interruption interruption) =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            TestBackupNode b0 = cluster.BackupNodes[0];
            await using DfsClient live = await ConnectLiveAsync(cluster);
            DfsVolume vol = live.GetVolume(Volume);
            await RegisterAsync(b0);
            TreeModel model = InitialTree(8);
            await WriteTreeAsync(vol, model);
            await SnapshotAndSyncAsync(vol, b0.TargetName, "s1");
            TreeModel model1 = model.Clone();

            await using DfsClient pinnedClient = await ConnectBackupReaderAsync(cluster, 0, NextClientName("pinned"));
            DfsSnapshotView pinned = await pinnedClient.GetVolume(Volume).Snapshots.OpenAsync("s1", Ct);
            await AssertViewMatchesAsync(pinned, model1, "pinned S1 before the interrupted transfer");

            for (int i = 0; i < 3; i++)
                await PutAsync(vol, model, $"/p/n{i}.bin", TestData.Bytes(90 + (ulong)i, 130 * 1024));
            await PutAsync(vol, model, "/docs/a.bin", TestData.Bytes(95, 64 * 1024));
            DfsSnapshotInfo s2 = await SnapshotAsync(vol, "s2");
            int newObjects = (await FileObjectsAsync(cluster, model.Files.Keys)).Except(await BackupObjectsAsync(b0)).Count();
            Assert.True(newObjects >= 3, $"S2 must need several new objects (needs {newObjects})");

            TestNode leader = await cluster.GetLeaderAsync(Volume, Ct);
            (string site, string? node, int occurrence) = phase switch
            {
                TransferPhase.SourcePin => (FaultSites.SnapshotPin, leader.Name, 1),
                TransferPhase.Inventory => (FaultSites.BackupInventory, (string?)null, 1),
                TransferPhase.FirstObject => (FaultSites.BackupObjectReceived, b0.Name, 1),
                TransferPhase.MiddleObject => (FaultSites.BackupObjectReceived, b0.Name, Math.Max(1, newObjects / 2)),
                TransferPhase.LastObject => (FaultSites.BackupObjectReceived, b0.Name, newObjects),
                TransferPhase.BeforeClosureVerify => (FaultSites.BackupBeforeClosureVerify, b0.Name, 1),
                _ => (FaultSites.BackupBeforePointer, b0.Name, 1),
            };
            bool onBackup = string.Equals(node, b0.Name, StringComparison.Ordinal);
            bool onLeader = string.Equals(node, leader.Name, StringComparison.Ordinal);
            FaultAction action = interruption switch
            {
                Interruption.BackupCrash when onBackup => FaultAction.CrashNode(CrashMode.LoseUnflushed),
                Interruption.BackupCrash => FaultAction.Custom(_ => b0.CrashAsync(CrashMode.LoseUnflushed)),
                Interruption.BackupIoError => FaultAction.IoError(),
                Interruption.NetworkCut => FaultAction.Custom(_ =>
                {
                    CutFromSources(cluster, b0.Name);
                    return Task.CompletedTask;
                }),
                _ when onLeader => FaultAction.CrashNode(CrashMode.LoseUnflushed),
                _ => FaultAction.Custom(_ => leader.CrashAsync(CrashMode.LoseUnflushed)),
            };

            FaultArm arm = cluster.Faults.Arm(new FaultMatch(site, Node: node, Volume: Volume, Occurrence: occurrence), action);
            await vol.Backups.RequestSyncAsync(b0.TargetName, "s2", Ct);
            await arm.WaitTriggeredAsync(FaultTimeout);
            cluster.Faults.AssertReached(arm);
            string because = $"transfer of S2 interrupted by {interruption} at {phase}";

            if (interruption == Interruption.BackupCrash)
            {
                await cluster.WaitUntilAsync(() => Task.FromResult(b0.State == TestNodeState.Crashed), FaultTimeout, "the backup crash took effect", Ct);
                CutFromSources(cluster, b0.Name); // keep the transfer from resuming until the safety checks ran
                await b0.RestartAsync();
            }
            else if (interruption == Interruption.SourceLeaderCrash)
            {
                await cluster.WaitUntilAsync(() => Task.FromResult(leader.State == TestNodeState.Crashed), FaultTimeout, "the source leader crash took effect", Ct);
            }

            // Safety while interrupted: S1 stays published, complete and unchanged; S2 is either absent or complete.
            await AssertPreviousIntactAsync(cluster, b0, "s1", model1, "s2", model, because);
            await AssertPinnedViewAsync(pinned, model1, interruption == Interruption.BackupCrash, because + " (pinned reader)");

            // Recovery: the transfer completes and S1 is still intact.
            cluster.Faults.DisarmAll();
            cluster.Network.Heal();
            if (interruption == Interruption.SourceLeaderCrash)
                await leader.RestartAsync();
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(30), Ct);
            await vol.Backups.RequestSyncAsync(b0.TargetName, "s2", Ct);
            DfsBackupCheckpoint cp = await vol.Backups.AwaitBackupSnapshotAsync(b0.TargetName, "s2", SyncTimeout, Ct);
            AssertExactCheckpoint(cp, b0.TargetName, s2);
            await AssertPublishedStateAsync(b0, "s2", ["s1", "s2"], [], because + ", after recovery");
            await using DfsClient reader = await ConnectBackupReaderAsync(cluster, 0, NextClientName("bk"));
            await AssertBackupSnapshotAsync(reader, "s1", model1, because + ": S1 after recovery");
            await AssertBackupSnapshotAsync(reader, "s2", model, because + ": S2 after recovery");
            await AssertPinnedViewAsync(pinned, model1, interruption == Interruption.BackupCrash, because + " (pinned reader after recovery)");
            try
            {
                await pinned.DisposeAsync();
            }
            catch (Exception ex) when (ex is IOException or ObjectDisposedException)
            {
            }
        });

    /// <summary>
    /// The previous snapshot is still published, verifies and reads exactly; the next one is either not selectable
    /// at all or (when the interrupted transfer already resumed and finished) published completely and exactly.
    /// </summary>
    private static async Task AssertPreviousIntactAsync(TestCluster cluster, TestBackupNode backup, string previous, TreeModel previousModel,
        string next, TreeModel nextModel, string because)
    {
        IReadOnlyList<string> published = await backup.Inspect.GetPublishedSnapshotsAsync(Volume);
        string? pointer = await backup.Inspect.GetPublishedPointerAsync(Volume);
        Assert.True(published.Contains(previous, StringComparer.Ordinal), $"{because}: {previous} must remain published");
        bool nextPublished = published.Contains(next, StringComparer.Ordinal);
        string expectedPointer = nextPublished ? next : previous;
        Assert.True(string.Equals(pointer, expectedPointer, StringComparison.Ordinal),
            $"{because}: pointer '{pointer}' must be the latest complete snapshot '{expectedPointer}'");
        Assert.Empty(await backup.Inspect.VerifyClosureAsync(Volume, previous));

        await using DfsClient probe = await ConnectBackupReaderAsync(cluster, backup.Index, NextClientName("probe"));
        await AssertBackupSnapshotAsync(probe, previous, previousModel, $"{because}: {previous} unchanged");
        if (nextPublished)
        {
            Assert.Empty(await backup.Inspect.VerifyClosureAsync(Volume, next));
            await AssertBackupSnapshotAsync(probe, next, nextModel, $"{because}: {next} may only be published complete");
        }
        else
        {
            Xunit.Sdk.XunitException? selectable = null;
            try
            {
                await AssertNotSelectableAsync(probe, next, because);
            }
            catch (Xunit.Sdk.XunitException ex)
            {
                selectable = ex;
            }

            if (selectable is not null)
            {
                // The interrupted transfer may resume and finish while these checks run (a new source leader resumes it on its
                // own): the next snapshot must then be published, and only complete.
                IReadOnlyList<string> now = await backup.Inspect.GetPublishedSnapshotsAsync(Volume);
                Assert.True(now.Contains(next, StringComparer.Ordinal), $"{because}: {selectable.Message}");
                Assert.Empty(await backup.Inspect.VerifyClosureAsync(Volume, next));
                await AssertBackupSnapshotAsync(probe, next, nextModel, $"{because}: {next} may only be published complete");
            }
        }
    }

    /// <summary>A pinned view returns its own snapshot's contents or fails explicitly (only when its backup process died).</summary>
    private static async Task AssertPinnedViewAsync(DfsSnapshotView view, TreeModel expected, bool mayHaveFailed, string because)
    {
        try
        {
            await AssertViewMatchesAsync(view, expected, because);
        }
        catch (Exception ex) when (mayHaveFailed && ex is IOException or ObjectDisposedException or TimeoutException)
        {
            // The reader's pin lived in a crashed process: an explicit failure is legal, other contents never are.
        }
    }

    // ------------------------------------------------------------------------------------------------------------
    // BAK-009
    // ------------------------------------------------------------------------------------------------------------

    public enum ResumePoint
    {
        AfterPartialReceive,
        DuringClosureVerification,
    }

    [Theory]
    [InlineData(ResumePoint.AfterPartialReceive)]
    [InlineData(ResumePoint.DuringClosureVerification)]
    [Catalog("BAK-009", "S", "INV-10", "INV-09")]
    public Task BAK_009_TransferResumeIsIdempotent(ResumePoint point) =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            TestBackupNode b0 = cluster.BackupNodes[0];
            await using DfsClient live = await ConnectLiveAsync(cluster);
            DfsVolume vol = live.GetVolume(Volume);
            const int maxConcurrent = 1;
            await RegisterAsync(b0, new DfsBackupBudgets(TimeSpan.FromHours(1), 1L << 30, MaxConcurrentObjects: maxConcurrent));
            TreeModel model = InitialTree(14);
            await WriteTreeAsync(vol, model);
            await SnapshotAndSyncAsync(vol, b0.TargetName, "s1");
            TreeModel model1 = model.Clone();

            for (int i = 0; i < 6; i++)
                await PutAsync(vol, model, $"/r/n{i}.bin", TestData.Bytes(100 + (ulong)i, 100 * 1024));
            DfsSnapshotInfo s2 = await SnapshotAsync(vol, "s2");
            HashSet<ObjectId> fresh = (await FileObjectsAsync(cluster, model.Files.Keys)).Except(await BackupObjectsAsync(b0)).ToHashSet();
            Assert.True(fresh.Count >= 6, $"S2 must need several new objects (needs {fresh.Count})");
            int hitsBefore = Hits(cluster, FaultSites.BackupObjectReceived, b0.Name).Count;

            FaultArm arm = point == ResumePoint.AfterPartialReceive
                ? cluster.Faults.Arm(new FaultMatch(FaultSites.BackupObjectReceived, Node: b0.Name, Volume: Volume, Occurrence: 3),
                    FaultAction.Custom(_ =>
                    {
                        CutFromSources(cluster, b0.Name);
                        cluster.Network.ResetConnections(b0.Name);
                        return Task.CompletedTask;
                    }))
                : cluster.Faults.Arm(new FaultMatch(FaultSites.BackupBeforeClosureVerify, Node: b0.Name, Volume: Volume), FaultAction.Block());
            await vol.Backups.RequestSyncAsync(b0.TargetName, "s2", Ct);
            await arm.WaitTriggeredAsync(FaultTimeout);
            cluster.Faults.AssertReached(arm);

            // The objects the destination itself reports as durably persisted and verified at the interruption. Only
            // these are promised to be reusable: F18 lies before the destination flush, so others may be lost.
            IReadOnlySet<ObjectId> durableAtInterruption;
            if (point == ResumePoint.AfterPartialReceive)
            {
                BackupReceiveInspection? partial = await b0.Inspect.GetReceiveStateAsync(Volume);
                Assert.NotNull(partial);
                Assert.True(partial.DurableObjects.Count >= 2, $"two objects were durably received before the cut (got {partial.DurableObjects.Count})");
                durableAtInterruption = partial.DurableObjects.ToHashSet();
                await AssertPublishedStateAsync(b0, "s1", ["s1"], ["s2"], "partial receive, connection lost");
                await cluster.DelayAsync(TimeSpan.FromSeconds(5), Ct);
                cluster.Network.Heal();
            }
            else
            {
                // Held at closure verification: capture the durable set, then cold-crash the backup before it verifies.
                BackupReceiveInspection? verifying = await b0.Inspect.GetReceiveStateAsync(Volume);
                Assert.NotNull(verifying);
                Assert.True(verifying.ManifestReceived, "closure verification runs on a received manifest");
                durableAtInterruption = verifying.DurableObjects.ToHashSet();
                await AssertPublishedStateAsync(b0, "s1", ["s1"], ["s2"], "held before closure verification");
                await b0.CrashAsync(CrashMode.LoseUnflushed);
                arm.Release();
                await b0.RestartAsync();
                HashSet<ObjectId> survived = await BackupObjectsAsync(b0);
                Assert.True(durableAtInterruption.All(survived.Contains),
                    $"objects reported durable before the crash were lost: [{string.Join(", ", durableAtInterruption.Where(id => !survived.Contains(id)))}]");
                await AssertPublishedStateAsync(b0, "s1", ["s1"], ["s2"], "crash before closure verification");
            }

            cluster.Faults.DisarmAll();
            await vol.Backups.RequestSyncAsync(b0.TargetName, "s2", Ct);
            DfsBackupCheckpoint cp = await vol.Backups.AwaitBackupSnapshotAsync(b0.TargetName, "s2", SyncTimeout, Ct);
            AssertExactCheckpoint(cp, b0.TargetName, s2);

            var receipts = Hits(cluster, FaultSites.BackupObjectReceived, b0.Name).Skip(hitsBefore)
                .Where(h => h.Object.HasValue)
                .GroupBy(h => h.Object.GetValueOrDefault())
                .ToDictionary(g => g.Key, g => g.Count());
            Assert.Subset(receipts.Keys.ToHashSet(), fresh);
            Assert.True(receipts.Keys.All(fresh.Contains), "objects retained from S1 must not be re-sent on resume");
            var resentDurable = durableAtInterruption.Where(id => receipts.TryGetValue(id, out int n) && n > 1).ToList();
            Assert.True(resentDurable.Count == 0,
                $"objects already durable and verified on the destination must be reused, not re-sent: [{string.Join(", ", resentDurable)}]");
            int resent = receipts.Values.Sum() - receipts.Count;
            if (point == ResumePoint.AfterPartialReceive)
                Assert.True(resent <= maxConcurrent + 1, $"resumption may resend at most the in-flight window, {resent} were resent");

            await AssertBackupStoreConsistentAsync(b0, $"after resume ({point})");
            await AssertPublishedStateAsync(b0, "s2", ["s1", "s2"], [], $"after resume ({point})");
            await using DfsClient reader = await ConnectBackupReaderAsync(cluster, 0, NextClientName("bk"));
            await AssertBackupSnapshotAsync(reader, "s2", model, $"S2 after resume ({point})");
            await AssertBackupSnapshotAsync(reader, "s1", model1, "S1 unchanged");
        });

    [Fact]
    [Catalog("BAK-009", "S", "INV-10", "INV-09")]
    public Task BAK_009_TransferResumeIsIdempotent_DuplicateDelivery() =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            TestBackupNode b0 = cluster.BackupNodes[0];
            await using DfsClient live = await ConnectLiveAsync(cluster);
            DfsVolume vol = live.GetVolume(Volume);
            await RegisterAsync(b0);
            TreeModel model = InitialTree(15);
            await WriteTreeAsync(vol, model);
            await SnapshotAndSyncAsync(vol, b0.TargetName, "s1");
            TreeModel model1 = model.Clone();

            for (int i = 0; i < 3; i++)
                await PutAsync(vol, model, $"/dup/n{i}.bin", TestData.Bytes(110 + (ulong)i, 130 * 1024));
            DfsSnapshotInfo s2 = await SnapshotAsync(vol, "s2");
            TreeModel model2 = model.Clone();

            MessageRule dupObjects = cluster.Network.DuplicateNext(new MessageFilter(To: b0.Name, Type: MessageType.BackupPutObject), count: 1000);
            MessageRule dupManifest = cluster.Network.DuplicateNext(new MessageFilter(To: b0.Name, Type: MessageType.BackupPutManifest), count: 10);
            MessageRule dupPublish = cluster.Network.DuplicateNext(new MessageFilter(To: b0.Name, Type: MessageType.BackupPublish), count: 10);
            await SyncExactAsync(vol, b0.TargetName, s2);
            Assert.True(dupObjects.Matched >= 1, "object deliveries must actually have been duplicated");
            Assert.True(dupManifest.Matched >= 1, "the manifest delivery must actually have been duplicated");
            Assert.True(dupPublish.Matched >= 1, "the publish request must actually have been duplicated");
            dupObjects.Cancel();
            dupManifest.Cancel();
            dupPublish.Cancel();

            await AssertBackupStoreConsistentAsync(b0, "after duplicated deliveries");
            IReadOnlyList<string> published = await b0.Inspect.GetPublishedSnapshotsAsync(Volume);
            Assert.Equal(1, published.Count(n => n == "s2"));
            await AssertPublishedStateAsync(b0, "s2", ["s1", "s2"], [], "after duplicated deliveries");

            // Re-delivering already verified immutable objects through the protocol is idempotent.
            await PutAsync(vol, model, "/dup/extra.bin", TestData.Bytes(119, 70 * 1024));
            DfsSnapshotInfo s3 = await SnapshotAsync(vol, "s3");
            IReadOnlyList<ObjectRecordInfo> recordsBefore = await b0.Inspect.ListObjectsAsync();
            await using (ManualBackupExport export = await cluster.BeginManualBackupExportAsync(Volume, "s3", 0, Ct))
            {
                Assert.Equal(DfsErrorCode.None, await export.BeginAsync());
                Assert.Equal(DfsErrorCode.None, await export.SendManifestAsync());
                Assert.Equal(DfsErrorCode.None, await export.SendManifestAsync());
                foreach (ObjectId id in export.Closure)
                {
                    Assert.Equal(DfsErrorCode.None, await export.SendObjectAsync(id));
                    Assert.Equal(DfsErrorCode.None, await export.SendObjectAsync(id));
                }

                BackupPublishAttempt ok = await export.PublishAsync();
                Assert.True(ok.Published, $"publication after duplicate deliveries must succeed ({ok.Error})");
                Assert.NotNull(ok.Checkpoint);
                AssertExactCheckpoint(ok.Checkpoint, b0.TargetName, s3);
            }

            IReadOnlyList<ObjectRecordInfo> recordsAfter = await b0.Inspect.ListObjectsAsync();
            Dictionary<ObjectId, ObjectRecordInfo> afterById = recordsAfter.ToDictionary(r => r.Id);
            foreach (ObjectRecordInfo r in recordsBefore)
            {
                Assert.True(afterById.TryGetValue(r.Id, out ObjectRecordInfo? a), $"object {r.Id} vanished after re-delivery");
                Assert.Equal(r.Checksum, a.Checksum);
                Assert.Equal(r.Length, a.Length);
            }

            await AssertBackupStoreConsistentAsync(b0, "after manual duplicate deliveries");
            await using DfsClient reader = await ConnectBackupReaderAsync(cluster, 0, NextClientName("bk"));
            await AssertBackupSnapshotAsync(reader, "s1", model1, "S1 after duplicates");
            await AssertBackupSnapshotAsync(reader, "s2", model2, "S2 after duplicates");
            await AssertBackupSnapshotAsync(reader, "s3", model, "S3 after duplicates");
        });

    /// <summary>
    /// Every object is stored once, every retained snapshot's closure is stored, and the node's reference accounting
    /// equals the number of retained snapshots referencing each object.
    /// </summary>
    private static async Task AssertBackupStoreConsistentAsync(TestBackupNode backup, string because)
    {
        IReadOnlyList<ObjectRecordInfo> records = await backup.Inspect.ListObjectsAsync();
        var duplicates = records.GroupBy(r => r.Id).Where(g => g.Count() > 1).Select(g => g.Key).ToList();
        Assert.True(duplicates.Count == 0, $"{because}: duplicate object records [{string.Join(", ", duplicates)}]");
        var stored = records.Select(r => r.Id).ToHashSet();
        IReadOnlyList<BackupSnapshotInspection> snapshots = await backup.Inspect.GetSnapshotsAsync(Volume);
        IReadOnlyDictionary<ObjectId, int> counts = await backup.Inspect.GetObjectReferenceCountsAsync();
        foreach (BackupSnapshotInspection snapshot in snapshots)
        {
            var missing = snapshot.Closure.Where(id => !stored.Contains(id)).ToList();
            Assert.True(missing.Count == 0, $"{because}: retained {snapshot.Name} references unstored objects [{string.Join(", ", missing)}]");
        }

        foreach (ObjectId id in snapshots.SelectMany(s => s.Closure).Distinct())
        {
            int expected = snapshots.Count(s => s.Closure.Contains(id));
            int actual = counts.TryGetValue(id, out int c) ? c : 0;
            Assert.True(actual == expected, $"{because}: reference count of {id} is {actual}, expected {expected}");
        }
    }

    // ------------------------------------------------------------------------------------------------------------
    // BAK-010
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<BackupObjectTamper, bool> ObjectTampers
    {
        get
        {
            var data = new TheoryData<BackupObjectTamper, bool>();
            foreach (BackupObjectTamper tamper in Enum.GetValues<BackupObjectTamper>().Where(t => t != BackupObjectTamper.None))
            {
                data.Add(tamper, true);
                data.Add(tamper, false);
            }

            return data;
        }
    }

    [Theory]
    [MemberData(nameof(ObjectTampers))]
    [Catalog("BAK-010", "S", "INV-10", "INV-14")]
    public Task BAK_010_CorruptTransferObjectRejected(BackupObjectTamper tamper, bool manifestFirst) =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            TestBackupNode b0 = cluster.BackupNodes[0];
            await using DfsClient live = await ConnectLiveAsync(cluster);
            DfsVolume vol = live.GetVolume(Volume);
            await RegisterAsync(b0);
            TreeModel model = InitialTree(16);
            await WriteTreeAsync(vol, model);
            await SnapshotAndSyncAsync(vol, b0.TargetName, "s1");
            TreeModel model1 = model.Clone();

            for (int i = 0; i < 3; i++)
                await PutAsync(vol, model, $"/c/n{i}.bin", TestData.Bytes(130 + (ulong)i, 130 * 1024));
            DfsSnapshotInfo s2 = await SnapshotAsync(vol, "s2");
            Dictionary<ObjectId, ObjectRecordInfo> liveRecords = await LiveObjectRecordsAsync(cluster);
            string because = $"S2 with one object tampered ({tamper}, manifest {(manifestFirst ? "first" : "last")})";

            await using (ManualBackupExport export = await cluster.BeginManualBackupExportAsync(Volume, "s2", 0, Ct))
            {
                HashSet<ObjectId> held = await BackupObjectsAsync(b0);
                var needed = export.Closure.Where(i => !held.Contains(i)).ToList();
                Assert.True(needed.Count >= 2);
                ObjectId victim = Lowest(needed);

                Assert.Equal(DfsErrorCode.None, await export.BeginAsync());
                if (manifestFirst)
                    Assert.Equal(DfsErrorCode.None, await export.SendManifestAsync());
                foreach (ObjectId id in needed.Where(i => i != victim))
                    Assert.Equal(DfsErrorCode.None, await export.SendObjectAsync(id));
                DfsErrorCode tampered = await export.SendObjectAsync(victim, tamper);
                if (!manifestFirst)
                    Assert.Equal(DfsErrorCode.None, await export.SendManifestAsync());

                bool selfEvident = tamper is BackupObjectTamper.FlipPayloadByte or BackupObjectTamper.TruncatedPayload or BackupObjectTamper.WrongChecksum;
                if (selfEvident || manifestFirst)
                    Assert.True(tampered == DfsErrorCode.Integrity, $"{because}: a corruption detectable on receipt must be rejected with Integrity, got {tampered}");
                else
                    Assert.True(tampered is DfsErrorCode.None or DfsErrorCode.Integrity, $"{because}: unexpected receipt result {tampered}");

                BackupPublishAttempt attempt = await export.PublishAsync();
                Assert.False(attempt.Published, $"{because}: a closure containing a corrupt object must never be published");
                Assert.NotEqual(DfsErrorCode.None, attempt.Error);
                Assert.Contains(victim, attempt.MissingObjects);
                await AssertPublishedStateAsync(b0, "s1", ["s1"], ["s2"], because);
                await using (DfsClient probe = await ConnectBackupReaderAsync(cluster, 0, NextClientName("probe")))
                {
                    await AssertNotSelectableAsync(probe, "s2", because);
                    await AssertBackupSnapshotAsync(probe, "s1", model1, because);
                }

                // Repair: the genuine object replaces the rejected bytes and the snapshot publishes.
                Assert.Equal(DfsErrorCode.None, await export.SendObjectAsync(victim));
                BackupPublishAttempt ok = await export.PublishAsync();
                Assert.True(ok.Published, $"{because}: publication after the genuine object arrived must succeed ({ok.Error})");
                Assert.NotNull(ok.Checkpoint);
                AssertExactCheckpoint(ok.Checkpoint, b0.TargetName, s2);
            }

            await AssertPublishedStateAsync(b0, "s2", ["s1", "s2"], [], because + ", repaired");
            foreach (ObjectRecordInfo record in await b0.Inspect.ListObjectsAsync())
            {
                if (liveRecords.TryGetValue(record.Id, out ObjectRecordInfo? genuine))
                {
                    Assert.True(genuine.Checksum == record.Checksum && genuine.Length == record.Length,
                        $"{because}: backup record {record.Id} does not match the genuine object");
                }
            }

            await AssertBackupStoreConsistentAsync(b0, because);
            await using DfsClient reader = await ConnectBackupReaderAsync(cluster, 0, NextClientName("bk"));
            await AssertBackupSnapshotAsync(reader, "s2", model, because + ", repaired");
            await AssertBackupSnapshotAsync(reader, "s1", model1, "S1 unchanged");
        });

    [Theory]
    [InlineData(BackupManifestTamper.DanglingObjectReference)]
    [InlineData(BackupManifestTamper.AlteredExtentChecksum)]
    [InlineData(BackupManifestTamper.FlipCiphertextByte)]
    [InlineData(BackupManifestTamper.DroppedFileRecord)]
    [Catalog("BAK-010", "S", "INV-10", "INV-14")]
    public Task BAK_010_CorruptTransferObjectRejected_Manifest(BackupManifestTamper tamper) =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            TestBackupNode b0 = cluster.BackupNodes[0];
            await using DfsClient live = await ConnectLiveAsync(cluster);
            DfsVolume vol = live.GetVolume(Volume);
            await RegisterAsync(b0);
            TreeModel model = InitialTree(17);
            await WriteTreeAsync(vol, model);
            await SnapshotAndSyncAsync(vol, b0.TargetName, "s1");
            TreeModel model1 = model.Clone();

            for (int i = 0; i < 3; i++)
                await PutAsync(vol, model, $"/m/n{i}.bin", TestData.Bytes(140 + (ulong)i, 130 * 1024));
            DfsSnapshotInfo s2 = await SnapshotAsync(vol, "s2");
            string because = $"S2 with a tampered manifest ({tamper})";

            await using (ManualBackupExport export = await cluster.BeginManualBackupExportAsync(Volume, "s2", 0, Ct))
            {
                HashSet<ObjectId> held = await BackupObjectsAsync(b0);
                Assert.Equal(DfsErrorCode.None, await export.BeginAsync());
                foreach (ObjectId id in export.Closure.Where(i => !held.Contains(i)))
                    Assert.Equal(DfsErrorCode.None, await export.SendObjectAsync(id));
                DfsErrorCode code = await export.SendManifestAsync(tamper);
                if (tamper == BackupManifestTamper.FlipCiphertextByte)
                    Assert.True(code == DfsErrorCode.Integrity, $"{because}: authenticated manifest encryption must reject the change on receipt, got {code}");
                else
                    Assert.True(code is DfsErrorCode.None or DfsErrorCode.Integrity or DfsErrorCode.ProtocolError, $"{because}: unexpected result {code}");

                BackupPublishAttempt attempt = await export.PublishAsync();
                Assert.False(attempt.Published, $"{because}: must never be published");
                Assert.NotEqual(DfsErrorCode.None, attempt.Error);
                Assert.Null(attempt.Checkpoint);
                await AssertPublishedStateAsync(b0, "s1", ["s1"], ["s2"], because);
                await using DfsClient probe = await ConnectBackupReaderAsync(cluster, 0, NextClientName("probe"));
                await AssertNotSelectableAsync(probe, "s2", because);
                await AssertBackupSnapshotAsync(probe, "s1", model1, because);
            }

            // The automatic exporter then publishes the genuine snapshot.
            await SyncExactAsync(vol, b0.TargetName, s2);
            await AssertPublishedStateAsync(b0, "s2", ["s1", "s2"], [], because + ", genuine resend");
            await AssertBackupStoreConsistentAsync(b0, because);
            await using DfsClient reader = await ConnectBackupReaderAsync(cluster, 0, NextClientName("bk"));
            await AssertBackupSnapshotAsync(reader, "s2", model, because + ", genuine resend");
            await AssertBackupSnapshotAsync(reader, "s1", model1, "S1 unchanged");
        });

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    [Catalog("BAK-010", "S", "INV-10", "INV-14", "INV-08")]
    public Task BAK_010_CorruptTransferObjectRejected_SourceReplicaCorrupt(bool everyReplica) =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            TestBackupNode b0 = cluster.BackupNodes[0];
            await using DfsClient live = await ConnectLiveAsync(cluster);
            DfsVolume vol = live.GetVolume(Volume);
            await RegisterAsync(b0);
            TreeModel model = InitialTree(18);
            await WriteTreeAsync(vol, model);
            (DfsSnapshotInfo s1, _) = await SnapshotAndSyncAsync(vol, b0.TargetName, "s1");
            TreeModel model1 = model.Clone();

            await PutAsync(vol, model, "/src/x.bin", TestData.Bytes(150, 200 * 1024));
            DfsSnapshotInfo s2 = await SnapshotAsync(vol, "s2");
            var needed = (await FileObjectsAsync(cluster, ["/src/x.bin"])).Except(await BackupObjectsAsync(b0)).ToList();
            ObjectId victim = Lowest(needed);
            IReadOnlyList<string> holders = await cluster.Inspect.GetObjectHoldersAsync(victim);
            Assert.True(holders.Count >= 2, $"the victim object must have several live replicas (has {holders.Count})");
            foreach (string holder in everyReplica ? holders : holders.Take(1))
                await cluster.Node(holder).Disk.CorruptObjectAsync(victim, offset: 100);

            await vol.Backups.RequestSyncAsync(b0.TargetName, "s2", Ct);
            if (!everyReplica)
            {
                // The exporter verifies on read and fails over to a healthy replica.
                DfsBackupCheckpoint cp = await vol.Backups.AwaitBackupSnapshotAsync(b0.TargetName, "s2", SyncTimeout, Ct);
                AssertExactCheckpoint(cp, b0.TargetName, s2);
                await AssertPublishedStateAsync(b0, "s2", ["s1", "s2"], [], "one corrupt source replica");
                await using DfsClient reader = await ConnectBackupReaderAsync(cluster, 0, NextClientName("bk"));
                await AssertBackupSnapshotAsync(reader, "s2", model, "S2 exported from a verified replica");
                return;
            }

            // No verified source copy exists: the backup must report failure and never publish invented bytes.
            await cluster.WaitUntilAsync(async () =>
            {
                DfsBackupStatus st = await vol.Backups.GetStatusAsync(b0.TargetName, Ct);
                return st.LastError is not null || st.State == DfsBackupState.Failed;
            }, SyncTimeout, "the exporter must report that no verified copy of a dependency can be read", Ct);
            for (int round = 0; round < 3; round++)
            {
                string because = $"every source replica of a dependency is corrupt (round {round})";
                DfsBackupStatus st = await vol.Backups.GetStatusAsync(b0.TargetName, Ct);
                Assert.Equal(s1.Boundary, st.PublishedBoundary);
                Assert.Equal("s1", st.LatestPublished?.SnapshotName);
                await AssertPublishedStateAsync(b0, "s1", ["s1"], ["s2"], because);
                await using (DfsClient probe = await ConnectBackupReaderAsync(cluster, 0, NextClientName("probe")))
                {
                    await AssertNotSelectableAsync(probe, "s2", because);
                    await AssertBackupSnapshotAsync(probe, "s1", model1, because);
                }

                await cluster.DelayAsync(TimeSpan.FromSeconds(10), Ct);
            }

            DfsBackupNotReachedException notReached = await Assert.ThrowsAsync<DfsBackupNotReachedException>(
                () => vol.Backups.AwaitBackupSnapshotAsync(b0.TargetName, "s2", ShortAwait, Ct));
            Assert.Equal("s1", notReached.LastPublished?.SnapshotName);
        });

    // ------------------------------------------------------------------------------------------------------------
    // BAK-011
    // ------------------------------------------------------------------------------------------------------------

    public enum DiskFullKind
    {
        CapacityOverride,
        FillDisk,
        DiskFullOnObject,
        DiskFullBeforePointer,
    }

    [Theory]
    [InlineData(DiskFullKind.CapacityOverride)]
    [InlineData(DiskFullKind.FillDisk)]
    [InlineData(DiskFullKind.DiskFullOnObject)]
    [InlineData(DiskFullKind.DiskFullBeforePointer)]
    [Catalog("BAK-011", "S", "INV-10", "INV-15", "INV-16")]
    public Task BAK_011_BackupDiskFullPreventsPublication(DiskFullKind kind) =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            TestBackupNode b0 = cluster.BackupNodes[0];
            await using DfsClient live = await ConnectLiveAsync(cluster);
            DfsVolume vol = live.GetVolume(Volume);
            await RegisterAsync(b0);
            TreeModel model = InitialTree(19);
            await WriteTreeAsync(vol, model);
            (DfsSnapshotInfo s1, _) = await SnapshotAndSyncAsync(vol, b0.TargetName, "s1");
            TreeModel model1 = model.Clone();

            await PutAsync(vol, model, "/big/b0.bin", TestData.Bytes(160, 1024 * 1024));
            await PutAsync(vol, model, "/big/b1.bin", TestData.Bytes(161, 512 * 1024));
            DfsSnapshotInfo s2 = await SnapshotAsync(vol, "s2");
            TreeModel model2 = model.Clone();

            FaultArm? arm = null;
            switch (kind)
            {
                case DiskFullKind.CapacityOverride:
                    b0.Disk.Faults.CapacityOverride = b0.Disk.UsedBytes + (32 * 1024);
                    break;
                case DiskFullKind.FillDisk:
                    b0.Disk.Fill(leaveFreeBytes: 32 * 1024);
                    break;
                case DiskFullKind.DiskFullOnObject:
                    arm = cluster.Faults.Arm(new FaultMatch(FaultSites.BackupObjectReceived, Node: b0.Name, Volume: Volume, Occurrence: 2, Count: 10_000),
                        FaultAction.DiskFull());
                    break;
                case DiskFullKind.DiskFullBeforePointer:
                    arm = cluster.Faults.Arm(new FaultMatch(FaultSites.BackupBeforePointer, Node: b0.Name, Volume: Volume, Count: 10_000),
                        FaultAction.DiskFull());
                    break;
            }

            await vol.Backups.RequestSyncAsync(b0.TargetName, "s2", Ct);
            await cluster.WaitUntilAsync(async () =>
            {
                DfsBackupStatus st = await vol.Backups.GetStatusAsync(b0.TargetName, Ct);
                return st.State == DfsBackupState.Failed || st.LastError is not null;
            }, SyncTimeout, "a full backup destination must report backup failure", Ct);
            if (arm is not null)
                cluster.Faults.AssertReached(arm);

            string because = $"backup destination full ({kind})";
            DfsBackupStatus failed = await vol.Backups.GetStatusAsync(b0.TargetName, Ct);
            Assert.Equal(s1.Boundary, failed.PublishedBoundary);
            Assert.Equal("s1", failed.LatestPublished?.SnapshotName);
            if (kind != DiskFullKind.DiskFullBeforePointer)
                Assert.NotEqual(s2.Boundary, failed.DurableBoundary);
            await AssertPublishedStateAsync(b0, "s1", ["s1"], ["s2"], because);
            await using (DfsClient probe = await ConnectBackupReaderAsync(cluster, 0, NextClientName("probe")))
            {
                await AssertNotSelectableAsync(probe, "s2", because);
                await AssertBackupSnapshotAsync(probe, "s1", model1, because);
            }

            DfsBackupNotReachedException notReached = await Assert.ThrowsAsync<DfsBackupNotReachedException>(
                () => vol.Backups.AwaitBackupSnapshotAsync(b0.TargetName, "s2", ShortAwait, Ct));
            Assert.Equal(b0.TargetName, notReached.Target);
            Assert.Equal("s1", notReached.LastPublished?.SnapshotName);

            // Live commits and snapshots never wait for the full backup.
            await AssertLiveCommitUnblockedAsync(cluster, vol, model, "/after-full.bin", TestData.Bytes(162, 100 * 1024), because);
            DfsSnapshotInfo s3 = await SnapshotAsync(vol, "s3");
            Assert.True(s3.Boundary.CompareTo(s2.Boundary) > 0);

            if (kind == DiskFullKind.FillDisk)
                return; // TestDisk.Fill is not reversible; this variant establishes the safety half only.

            // Free space: the same snapshot then transfers completely.
            b0.Disk.Faults.CapacityOverride = null;
            cluster.Faults.DisarmAll();
            await SyncExactAsync(vol, b0.TargetName, s2);
            await AssertPublishedStateAsync(b0, "s2", ["s1", "s2"], [], because + ", space freed");
            await using DfsClient reader = await ConnectBackupReaderAsync(cluster, 0, NextClientName("bk"));
            await AssertBackupSnapshotAsync(reader, "s2", model2, because + ", space freed");
            await AssertBackupSnapshotAsync(reader, "s1", model1, "S1 unchanged");
        });

    // ------------------------------------------------------------------------------------------------------------
    // BAK-012
    // ------------------------------------------------------------------------------------------------------------

    public enum PointerCrashPoint
    {
        BeforePointer,
        AfterPointer,
        AtAcknowledgement,
    }

    public static TheoryData<PointerCrashPoint, CrashMode> PointerCrashMatrix
    {
        get
        {
            var data = new TheoryData<PointerCrashPoint, CrashMode>();
            foreach (PointerCrashPoint point in Enum.GetValues<PointerCrashPoint>())
            {
                foreach (CrashMode mode in Enum.GetValues<CrashMode>())
                    data.Add(point, mode);
            }

            return data;
        }
    }

    [Theory]
    [MemberData(nameof(PointerCrashMatrix))]
    [Catalog("BAK-012", "S", "INV-10", "INV-01", "INV-19")]
    public Task BAK_012_PublishPointerCrashAtomicity(PointerCrashPoint point, CrashMode mode) =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            TestBackupNode b0 = cluster.BackupNodes[0];
            await using DfsClient live = await ConnectLiveAsync(cluster);
            DfsVolume vol = live.GetVolume(Volume);
            await RegisterAsync(b0);
            TreeModel model = InitialTree(20);
            await WriteTreeAsync(vol, model);
            await SnapshotAndSyncAsync(vol, b0.TargetName, "s1");
            TreeModel model1 = model.Clone();

            await PutAsync(vol, model, "/docs/a.bin", TestData.Bytes(170, 180 * 1024));
            await PutAsync(vol, model, "/n.bin", TestData.Bytes(171, 150 * 1024));
            await DeleteFileAsync(vol, model, "/docs/b.bin");
            DfsSnapshotInfo s2 = await SnapshotAsync(vol, "s2");
            TreeModel model2 = model.Clone();

            string site = point switch
            {
                PointerCrashPoint.BeforePointer => FaultSites.BackupBeforePointer,
                PointerCrashPoint.AfterPointer => FaultSites.BackupAfterPointer,
                _ => FaultSites.BackupAck,
            };
            FaultArm arm = cluster.Faults.Arm(new FaultMatch(site, Node: b0.Name, Volume: Volume), FaultAction.CrashNode(mode));
            await vol.Backups.RequestSyncAsync(b0.TargetName, "s2", Ct);
            await arm.WaitTriggeredAsync(FaultTimeout);
            cluster.Faults.AssertReached(arm);
            await cluster.WaitUntilAsync(() => Task.FromResult(b0.State == TestNodeState.Crashed), FaultTimeout, "the backup crashed at the pointer boundary", Ct);

            // Restart with the source unreachable so only the durable image decides what is exposed.
            CutFromSources(cluster, b0.Name);
            await b0.RestartAsync();
            string because = $"cold restart after a crash {point} ({mode})";

            // After the durable pointer replacement the new snapshot must be exposed. Before it, the previous snapshot
            // or (if recovery rolls the fully verified, durable S2 forward) the complete new one - never anything else.
            bool newExposed = (await b0.Inspect.GetPublishedSnapshotsAsync(Volume)).Contains("s2", StringComparer.Ordinal);
            if (point != PointerCrashPoint.BeforePointer)
                Assert.True(newExposed, $"{because}: the durably replaced pointer must survive the crash");
            string expected = newExposed ? "s2" : "s1";
            await AssertPublishedStateAsync(b0, expected, newExposed ? ["s1", "s2"] : ["s1"], newExposed ? [] : ["s2"], because);
            await using (DfsClient probe = await ConnectBackupReaderAsync(cluster, 0, NextClientName("probe")))
            {
                IReadOnlyList<DfsSnapshotInfo> listed = await probe.GetVolume(Volume).Snapshots.ListAsync(Ct);
                IReadOnlyList<string> published = await b0.Inspect.GetPublishedSnapshotsAsync(Volume);
                Assert.Equal(published.Order(StringComparer.Ordinal), listed.Select(s => s.Name).Order(StringComparer.Ordinal));
                await AssertBackupSnapshotAsync(probe, "s1", model1, because);
                if (newExposed)
                    await AssertBackupSnapshotAsync(probe, "s2", model2, because);
                else
                    await AssertNotSelectableAsync(probe, "s2", because);
            }

            // A second cold restart never regresses an exposed S2 and only ever exposes complete snapshots.
            await b0.CrashAsync(mode);
            await b0.RestartAsync();
            bool newExposedAgain = (await b0.Inspect.GetPublishedSnapshotsAsync(Volume)).Contains("s2", StringComparer.Ordinal);
            Assert.True(newExposedAgain || !newExposed, $"{because}: an exposed S2 disappeared after a second restart");
            await AssertPublishedStateAsync(b0, newExposedAgain ? "s2" : "s1", newExposedAgain ? ["s1", "s2"] : ["s1"],
                newExposedAgain ? [] : ["s2"], because + ", second restart");

            // Reconnected: the transfer converges and the source learns the actual published boundary.
            cluster.Network.Heal();
            await SyncExactAsync(vol, b0.TargetName, s2);
            DfsBackupStatus status = await vol.Backups.GetStatusAsync(b0.TargetName, Ct);
            Assert.Equal(s2.Boundary, status.PublishedBoundary);
            Assert.Equal("s2", status.LatestPublished?.SnapshotName);
            await AssertPublishedStateAsync(b0, "s2", ["s1", "s2"], [], because + ", reconnected");
            await using DfsClient reader = await ConnectBackupReaderAsync(cluster, 0, NextClientName("bk"));
            await AssertBackupSnapshotAsync(reader, "s1", model1, because + ", reconnected");
            await AssertBackupSnapshotAsync(reader, "s2", model2, because + ", reconnected");
        });

    // ------------------------------------------------------------------------------------------------------------
    // BAK-013
    // ------------------------------------------------------------------------------------------------------------

    [Fact]
    [Catalog("BAK-013", "S", "INV-10", "INV-19", "INV-03")]
    public Task BAK_013_PinnedBackupReaderStaysOnOldSnapshot() =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            TestBackupNode b0 = cluster.BackupNodes[0];
            await using DfsClient live = await ConnectLiveAsync(cluster);
            DfsVolume vol = live.GetVolume(Volume);
            await RegisterAsync(b0);
            TreeModel model = InitialTree(21);
            await WriteTreeAsync(vol, model);
            (DfsSnapshotInfo s1, _) = await SnapshotAndSyncAsync(vol, b0.TargetName, "s1");
            TreeModel model1 = model.Clone();

            // R1 selects the latest published snapshot (S1) and starts streaming a file.
            await using DfsClient r1Client = await ConnectBackupReaderAsync(cluster, 0, NextClientName("r"));
            DfsSnapshotClient r1Snapshots = r1Client.GetVolume(Volume).Snapshots;
            DfsSnapshotInfo latest = (await r1Snapshots.ListAsync(Ct)).MaxBy(s => s.Boundary.Sequence)!;
            Assert.Equal("s1", latest.Name);
            DfsSnapshotView r1 = await r1Snapshots.OpenAsync(latest.Name, Ct);
            byte[] a1 = model1.Files["/docs/a.bin"];
            DfsStream r1Stream = await r1.Files.OpenReadAsync("/docs/a.bin", Ct);
            byte[] chunk = new byte[64 * 1024];
            await r1Stream.ReadExactlyAsync(chunk, Ct);
            AssertEx.BytesEqual(a1.AsSpan(0, chunk.Length), chunk, "R1 first chunk");

            // S2 rewrites, deletes and renames exactly what R1 is reading.
            await PutAsync(vol, model, "/docs/a.bin", TestData.Bytes(180, 220 * 1024));
            await DeleteFileAsync(vol, model, "/docs/b.bin");
            await vol.Files.MoveAsync("/small.txt", "/moved.txt", false, Ct);
            model.Put("/moved.txt", model.Files["/small.txt"]);
            model.Remove("/small.txt");
            await PutAsync(vol, model, "/new.bin", TestData.Bytes(181, 90 * 1024));
            DfsSnapshotInfo s2 = await SnapshotAsync(vol, "s2");

            FaultArm beforePointer = cluster.Faults.Arm(new FaultMatch(FaultSites.BackupBeforePointer, Node: b0.Name, Volume: Volume), FaultAction.Block());
            await vol.Backups.RequestSyncAsync(b0.TargetName, "s2", Ct);
            await beforePointer.WaitTriggeredAsync(SyncTimeout);
            cluster.Faults.AssertReached(beforePointer);
            await r1Stream.ReadExactlyAsync(chunk, Ct);
            AssertEx.BytesEqual(a1.AsSpan(chunk.Length, chunk.Length), chunk, "R1 second chunk while S2 is being published");
            beforePointer.Release();
            DfsBackupCheckpoint cp2 = await vol.Backups.AwaitBackupSnapshotAsync(b0.TargetName, "s2", SyncTimeout, Ct);
            AssertExactCheckpoint(cp2, b0.TargetName, s2);

            // R1 keeps reading S1 after S2 published.
            byte[] rest = new byte[a1.Length - (2 * chunk.Length)];
            await r1Stream.ReadExactlyAsync(rest, Ct);
            AssertEx.BytesEqual(a1.AsSpan(2 * chunk.Length), rest, "R1 remainder after S2 publication");
            Assert.Equal(0, await r1Stream.ReadAsync(new byte[1], Ct));
            Assert.Equal("s1", r1.Info.Name);
            Assert.Equal(s1.Boundary, r1.Info.Boundary);
            await AssertViewMatchesAsync(r1, model1, "R1's whole view after S2 publication");

            // New readers may select S2.
            await using DfsClient r2Client = await ConnectBackupReaderAsync(cluster, 0, NextClientName("r"));
            IReadOnlyList<DfsSnapshotInfo> listed = await r2Client.GetVolume(Volume).Snapshots.ListAsync(Ct);
            Assert.Contains(listed, s => s.Name == "s1");
            DfsSnapshotInfo latest2 = listed.MaxBy(s => s.Boundary.Sequence)!;
            Assert.Equal("s2", latest2.Name);
            Assert.Equal(s2.Boundary, latest2.Boundary);
            await AssertBackupSnapshotAsync(r2Client, "s2", model, "R2 selects S2");
            await AssertViewMatchesAsync(r1, model1, "R1 still on S1 after R2 opened S2");

            // The reader's pin is accounted on S1 and released on dispose.
            BackupSnapshotInspection pinnedS1 = (await b0.Inspect.GetSnapshotsAsync(Volume)).Single(s => s.Name == "s1");
            Assert.True(pinnedS1.ReaderPins >= 1, "R1's local pin must be recorded on S1");
            await r1Stream.DisposeAsync();
            await r1.DisposeAsync();
            await cluster.WaitUntilAsync(async () => (await b0.Inspect.GetSnapshotsAsync(Volume)).Single(s => s.Name == "s1").ReaderPins == 0,
                TimeSpan.FromSeconds(10), "R1's pin is released when its view is disposed", Ct);
        });

    // ------------------------------------------------------------------------------------------------------------
    // BAK-014
    // ------------------------------------------------------------------------------------------------------------

    public enum SourceLoss
    {
        AllStopped,
        AllCrashed,
        Partitioned,
    }

    [Theory]
    [InlineData(SourceLoss.AllStopped)]
    [InlineData(SourceLoss.AllCrashed)]
    [InlineData(SourceLoss.Partitioned)]
    [Catalog("BAK-014", "S", "INV-10", "INV-15", "INV-13")]
    public Task BAK_014_BackupReadableWithoutSource(SourceLoss loss) =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            TestBackupNode b0 = cluster.BackupNodes[0];
            await using DfsClient live = await ConnectLiveAsync(cluster);
            DfsVolume vol = live.GetVolume(Volume);
            await RegisterAsync(b0);
            TreeModel model = InitialTree(22);
            await WriteTreeAsync(vol, model);
            (DfsSnapshotInfo s1, _) = await SnapshotAndSyncAsync(vol, b0.TargetName, "s1");
            TreeModel model1 = model.Clone();
            await PutAsync(vol, model, "/docs/a.bin", TestData.Bytes(190, 210 * 1024));
            await PutAsync(vol, model, "/later/l.bin", TestData.Bytes(191, 100 * 1024));
            await DeleteFileAsync(vol, model, "/empty.bin");
            (DfsSnapshotInfo s2, _) = await SnapshotAndSyncAsync(vol, b0.TargetName, "s2");
            TreeModel model2 = model.Clone();

            switch (loss)
            {
                case SourceLoss.AllStopped:
                    foreach (TestNode node in cluster.Nodes)
                        await node.StopAsync();
                    break;
                case SourceLoss.AllCrashed:
                    foreach (TestNode node in cluster.Nodes)
                        await node.CrashAsync();
                    break;
                case SourceLoss.Partitioned:
                    CutFromSources(cluster, b0.Name);
                    break;
            }

            // Cold restart of the backup with no source reachable.
            await b0.CrashAsync(CrashMode.LoseUnflushed);
            await b0.RestartAsync();
            string because = $"backup readable with the source {loss}";

            await using DfsClient reader = await ConnectBackupReaderAsync(cluster, 0, NextClientName("offline"));
            DfsSnapshotClient snapshots = reader.GetVolume(Volume).Snapshots;
            IReadOnlyList<DfsSnapshotInfo> listed = await snapshots.ListAsync(Ct);
            Assert.Equal(["s1", "s2"], listed.Select(s => s.Name).Order(StringComparer.Ordinal));
            foreach (DfsSnapshotInfo source in new[] { s1, s2 })
            {
                DfsSnapshotInfo onBackup = listed.Single(s => s.Name == source.Name);
                Assert.Equal(source.Id, onBackup.Id);
                Assert.Equal(source.Boundary, onBackup.Boundary);
                Assert.Equal(source.Label, onBackup.Label);
                Assert.Equal(DfsConsistencyLabel.FilesystemConsistent, onBackup.Label);
                DfsSnapshotInfo got = await snapshots.GetAsync(source.Name, Ct);
                Assert.Equal(source.Boundary, got.Boundary);
            }

            await AssertBackupSnapshotAsync(reader, "s1", model1, because);
            await AssertBackupSnapshotAsync(reader, "s2", model2, because);

            // Ranged and text reads through the same SDK abstractions.
            await using (DfsSnapshotView view = await snapshots.OpenAsync("s2", Ct))
            {
                await using DfsStream stream = await view.Files.OpenReadAsync("/docs/a.bin", Ct);
                Assert.True(stream.CanSeek);
                Assert.False(stream.CanWrite);
                stream.Seek(100_000, SeekOrigin.Begin);
                byte[] range = new byte[5000];
                await stream.ReadExactlyAsync(range, Ct);
                AssertEx.BytesEqual(model2.Files["/docs/a.bin"].AsSpan(100_000, 5000), range, because + ": ranged read");
                string text = await view.Files.ReadAllTextAsync("/small.txt", null, Ct);
                Assert.Equal(System.Text.Encoding.UTF8.GetString(model2.Files["/small.txt"]), text);
            }

            // Still readable after a long disconnected period.
            await cluster.DelayAsync(TimeSpan.FromMinutes(2), Ct);
            await AssertBackupSnapshotAsync(reader, "s2", model2, because + ", two minutes later");
            await AssertPublishedStateAsync(b0, "s2", ["s1", "s2"], [], because);

            if (loss != SourceLoss.Partitioned)
            {
                foreach (TestNode node in cluster.Nodes)
                    await node.RestartAsync();
            }

            cluster.Network.Heal();
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), Ct);
        });

    // ------------------------------------------------------------------------------------------------------------
    // BAK-016
    // ------------------------------------------------------------------------------------------------------------

    [Fact]
    [Catalog("BAK-016", "S", "INV-19", "INV-10")]
    public Task BAK_016_ReceivedDurablePublishedProgressSeparated() =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            TestBackupNode b0 = cluster.BackupNodes[0];
            await using DfsClient live = await ConnectLiveAsync(cluster);
            DfsVolume vol = live.GetVolume(Volume);
            await RegisterAsync(b0, new DfsBackupBudgets(TimeSpan.FromHours(1), 1L << 30, MaxConcurrentObjects: 16));
            TreeModel model = InitialTree(23);
            await WriteTreeAsync(vol, model);
            (DfsSnapshotInfo s1, _) = await SnapshotAndSyncAsync(vol, b0.TargetName, "s1");

            Task<DfsBackupStatus> StatusAsync() => vol.Backups.GetStatusAsync(b0.TargetName, Ct);

            DfsBackupStatus st0 = await StatusAsync();
            Assert.Equal(s1.Boundary, st0.PublishedBoundary);
            Assert.Equal(s1.Boundary, st0.DurableBoundary);
            Assert.Equal("s1", st0.LatestPublished?.SnapshotName);
            Assert.NotNull(st0.RecoveryPointAge);

            for (int i = 0; i < 4; i++)
                await PutAsync(vol, model, $"/prog/n{i}.bin", TestData.Bytes(200 + (ulong)i, 130 * 1024));
            DfsSnapshotInfo s2 = await SnapshotAsync(vol, "s2");

            // Stage 1: bytes arrive, persistence of one dependency is held.
            FaultArm received = cluster.Faults.Arm(new FaultMatch(FaultSites.BackupObjectReceived, Node: b0.Name, Volume: Volume, Occurrence: 1), FaultAction.Block());
            await vol.Backups.RequestSyncAsync(b0.TargetName, "s2", Ct);
            await received.WaitTriggeredAsync(FaultTimeout);
            cluster.Faults.AssertReached(received);
            await cluster.WaitUntilAsync(async () => (await StatusAsync()).ReceivedBytes > st0.ReceivedBytes,
                FaultTimeout, "bytes that arrived are reported as received", Ct);
            DfsBackupStatus st1 = await StatusAsync();
            Assert.Equal(s1.Boundary, st1.DurableBoundary);
            Assert.Equal(s1.Boundary, st1.PublishedBoundary);
            Assert.Equal("s1", st1.LatestPublished?.SnapshotName);
            Assert.True(st1.State is DfsBackupState.Transferring or DfsBackupState.Verifying, $"state while receiving: {st1.State}");
            BackupReceiveInspection? recv1 = await b0.Inspect.GetReceiveStateAsync(Volume);
            Assert.NotNull(recv1);
            Assert.True(recv1.ReceivedBytes > 0);
            Assert.True(!recv1.ManifestReceived || recv1.MissingObjects.Count > 0, "a dependency is not yet durable");
            await Assert.ThrowsAsync<DfsBackupNotReachedException>(() => vol.Backups.AwaitBackupAsync(b0.TargetName, s2.Boundary, ShortAwait, Ct));
            await Assert.ThrowsAsync<DfsBackupNotReachedException>(() => vol.Backups.AwaitBackupSnapshotAsync(b0.TargetName, "s2", ShortAwait, Ct));

            // Stage 2: the whole closure durable, publication held.
            FaultArm pointer = cluster.Faults.Arm(new FaultMatch(FaultSites.BackupBeforePointer, Node: b0.Name, Volume: Volume), FaultAction.Block());
            received.Release();
            await pointer.WaitTriggeredAsync(SyncTimeout);
            cluster.Faults.AssertReached(pointer);
            await cluster.WaitUntilAsync(async () => (await StatusAsync()).DurableBoundary == s2.Boundary,
                FaultTimeout, "the durable recovery boundary advances once the closure is durable", Ct);
            DfsBackupStatus st2 = await StatusAsync();
            Assert.Equal(s1.Boundary, st2.PublishedBoundary);
            Assert.Equal("s1", st2.LatestPublished?.SnapshotName);
            Assert.True(st2.ReceivedBytes >= st1.ReceivedBytes);
            Assert.True(st2.State is DfsBackupState.Verifying or DfsBackupState.Publishing, $"state before the pointer replacement: {st2.State}");
            await Assert.ThrowsAsync<DfsBackupNotReachedException>(() => vol.Backups.AwaitBackupSnapshotAsync(b0.TargetName, "s2", ShortAwait, Ct));
            await using (DfsClient probe = await ConnectBackupReaderAsync(cluster, 0, NextClientName("probe")))
                await AssertNotSelectableAsync(probe, "s2", "durable but not yet published");

            // Stage 3: published.
            pointer.Release();
            DfsBackupCheckpoint cp = await vol.Backups.AwaitBackupSnapshotAsync(b0.TargetName, "s2", SyncTimeout, Ct);
            AssertExactCheckpoint(cp, b0.TargetName, s2);
            DfsBackupStatus st3 = await StatusAsync();
            Assert.Equal(s2.Boundary, st3.PublishedBoundary);
            Assert.Equal(s2.Boundary, st3.DurableBoundary);
            Assert.Equal("s2", st3.LatestPublished?.SnapshotName);
            Assert.True(st3.ReceivedBytes >= st2.ReceivedBytes);
            await cluster.WaitUntilAsync(async () => (await StatusAsync()).State == DfsBackupState.Idle,
                TimeSpan.FromSeconds(10), "the target returns to idle after publication", Ct);

            // Recovery-point age grows from the published boundary while nothing new publishes.
            TimeSpan age0 = (await StatusAsync()).RecoveryPointAge ?? TimeSpan.MinValue;
            Assert.True(age0 >= TimeSpan.Zero, "a published target reports a recovery point age");
            await cluster.DelayAsync(TimeSpan.FromSeconds(5), Ct);
            TimeSpan age1 = (await StatusAsync()).RecoveryPointAge ?? TimeSpan.MinValue;
            Assert.True(age1 - age0 >= TimeSpan.FromSeconds(4.5), $"recovery point age must grow with time ({age0} -> {age1})");

            // Slow persistence: every sampled status keeps received >= durable >= published ordering.
            await PutAsync(vol, model, "/prog/late.bin", TestData.Bytes(210, 300 * 1024));
            DfsSnapshotInfo s3 = await SnapshotAsync(vol, "s3");
            b0.Disk.Faults.IoDelay = TimeSpan.FromMilliseconds(300);
            await vol.Backups.RequestSyncAsync(b0.TargetName, "s3", Ct);
            long lastReceived = st3.ReceivedBytes;
            long start = cluster.Time.GetTimestamp();
            while (true)
            {
                Assert.True(cluster.Time.GetElapsedTime(start) < SyncTimeout, "S3 must publish under slow persistence within the sync bound");
                DfsBackupStatus st = await StatusAsync();
                Assert.True(st.ReceivedBytes >= lastReceived, $"received bytes never decrease ({lastReceived} -> {st.ReceivedBytes})");
                lastReceived = st.ReceivedBytes;
                Assert.True(st.PublishedBoundary == s2.Boundary || st.PublishedBoundary == s3.Boundary, $"published boundary {st.PublishedBoundary}");
                Assert.True(st.DurableBoundary == s2.Boundary || st.DurableBoundary == s3.Boundary, $"durable boundary {st.DurableBoundary}");
                Assert.True(st.DurableBoundary.IsAtOrAfter(st.PublishedBoundary), "the published boundary never runs ahead of the durable boundary");
                if (st.PublishedBoundary == s3.Boundary)
                {
                    Assert.Contains("s3", await b0.Inspect.GetPublishedSnapshotsAsync(Volume));
                    break;
                }

                await cluster.DelayAsync(TimeSpan.FromMilliseconds(250), Ct);
            }

            b0.Disk.Faults.IoDelay = null;
        });

    // ------------------------------------------------------------------------------------------------------------
    // BAK-017
    // ------------------------------------------------------------------------------------------------------------

    [Fact]
    [Catalog("BAK-017", "S", "INV-13", "INV-19")]
    public Task BAK_017_AwaitCommitBoundaryReturnsActualCheckpoint() =>
        ClusterTest.RunAsync(Options() with { Volumes = [new TestVolumeSpec(Volume), new TestVolumeSpec("other")] }, async cluster =>
        {
            TestBackupNode b0 = cluster.BackupNodes[0];
            await using DfsClient live = await ConnectLiveAsync(cluster);
            DfsVolume vol = live.GetVolume(Volume);
            await RegisterAsync(b0);
            TreeModel model = InitialTree(24);
            await WriteTreeAsync(vol, model);
            (DfsSnapshotInfo s1, _) = await SnapshotAndSyncAsync(vol, b0.TargetName, "s1");

            byte[] v1 = TestData.Bytes(220, 90 * 1024);
            byte[] v2 = TestData.Bytes(221, 90 * 1024);
            CommitToken t = await PutAsync(vol, model, "/k.bin", v1);
            CommitToken t2 = await PutAsync(vol, model, "/k.bin", v2);
            Assert.True(t2.CompareTo(t) > 0);
            Assert.True(s1.Boundary.CompareTo(t) < 0);

            // No published checkpoint covers T yet: the barrier times out and reports the actual last checkpoint.
            DfsBackupNotReachedException early = await Assert.ThrowsAsync<DfsBackupNotReachedException>(
                () => vol.Backups.AwaitBackupAsync(b0.TargetName, t, ShortAwait, Ct));
            Assert.Equal(b0.TargetName, early.Target);
            Assert.Equal("s1", early.LastPublished?.SnapshotName);
            Assert.Equal(s1.Boundary, early.LastPublished?.CoveredCommit);

            // A later snapshot S2 (no snapshot exists exactly at T) satisfies the at-least barrier.
            DfsSnapshotInfo s2 = await SnapshotAsync(vol, "s2");
            Assert.True(s2.Boundary.CompareTo(t2) >= 0);
            await vol.Backups.RequestSyncAsync(b0.TargetName, "s2", Ct);
            DfsBackupCheckpoint cp = await vol.Backups.AwaitBackupAsync(b0.TargetName, t, SyncTimeout, Ct);
            Assert.Equal(b0.TargetName, cp.Target);
            Assert.Equal("s2", cp.SnapshotName);
            Assert.Equal(s2.Id, cp.SnapshotId);
            Assert.Equal(s2.Boundary, cp.CoveredCommit);
            Assert.True(cp.CoveredCommit.IsComparableWith(t), "the checkpoint must be from the same cluster, volume and epoch as T");
            Assert.True(cp.CoveredCommit.IsAtOrAfter(t));
            Assert.True(cp.CoveredCommit.CompareTo(t) > 0, "the checkpoint is strictly later than T");
            Assert.False(cp.IsExactSnapshot, "an at-least-boundary checkpoint must not claim to retain the exact state at T");

            // The reported retention is the destination's actual retention of that snapshot.
            await using DfsClient reader = await ConnectBackupReaderAsync(cluster);
            DfsSnapshotInfo onBackup = await reader.GetVolume(Volume).Snapshots.GetAsync("s2", Ct);
            Assert.Equal(onBackup.Retention.ExpiresUtc, cp.RetentionExpiryUtc);
            Assert.Equal(onBackup.Retention.Hold, cp.Held);
            BackupSnapshotInspection inspected = (await b0.Inspect.GetSnapshotsAsync(Volume)).Single(s => s.Name == "s2");
            Assert.Equal(inspected.Retention.ExpiresUtc, cp.RetentionExpiryUtc);
            Assert.Equal(inspected.Retention.Hold, cp.Held);

            // The later checkpoint does not preserve the overwritten version that existed at T.
            await AssertBackupSnapshotAsync(reader, "s2", model, "S2 holds the later version of /k.bin");
            Assert.False(v1.AsSpan().SequenceEqual(model.Files["/k.bin"]));

            // Tokens of other volumes or epochs are not ordered against this volume's checkpoints.
            CommitToken foreign = await live.GetVolume("other").Files.WriteAllBytesAsync("/f.bin", TestData.Bytes(222, 1000), Ct);
            Assert.False(foreign.IsComparableWith(t));
            static bool IsTokenRejection(Exception ex) =>
                ex is ArgumentException || ex is DfsException { ErrorCode: DfsErrorCode.InvalidArgument or DfsErrorCode.EpochMismatch };
            await AssertRefusedAsync(() => vol.Backups.AwaitBackupAsync(b0.TargetName, foreign, ShortAwait, Ct), IsTokenRejection,
                "a token of another volume must be rejected, not compared");
            CommitToken otherEpoch = t with { VolumeEpoch = t.VolumeEpoch + 1 };
            await AssertRefusedAsync(() => vol.Backups.AwaitBackupAsync(b0.TargetName, otherEpoch, ShortAwait, Ct), IsTokenRejection,
                "a token of another epoch must be rejected, not compared");
            await AssertRefusedAsync(() => vol.Backups.AwaitBackupAsync(b0.TargetName, CommitToken.None, ShortAwait, Ct), IsTokenRejection,
                "CommitToken.None is not a boundary");
        });

    // ------------------------------------------------------------------------------------------------------------
    // BAK-018
    // ------------------------------------------------------------------------------------------------------------

    public enum ExactBarrierCase
    {
        ExactSnapshotRetained,
        LaterSnapshotOnly,
        ExactSnapshotExpired,
        CommitAndSnapshot,
    }

    [Theory]
    [InlineData(ExactBarrierCase.ExactSnapshotRetained)]
    [InlineData(ExactBarrierCase.LaterSnapshotOnly)]
    [InlineData(ExactBarrierCase.ExactSnapshotExpired)]
    [InlineData(ExactBarrierCase.CommitAndSnapshot)]
    [Catalog("BAK-018", "S", "INV-13", "INV-19")]
    public Task BAK_018_AwaitExactSnapshotRequiresThatSnapshot(ExactBarrierCase barrierCase) =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            TestBackupNode b0 = cluster.BackupNodes[0];
            await using DfsClient live = await ConnectLiveAsync(cluster);
            DfsVolume vol = live.GetVolume(Volume);
            await RegisterAsync(b0);
            TreeModel model = InitialTree(25);
            await WriteTreeAsync(vol, model);
            await SnapshotAndSyncAsync(vol, b0.TargetName, "s0");
            byte[] v1 = TestData.Bytes(230, 80 * 1024);
            byte[] v2 = TestData.Bytes(231, 80 * 1024);
            string because = $"exact barrier ({barrierCase})";

            switch (barrierCase)
            {
                case ExactBarrierCase.ExactSnapshotRetained:
                {
                    await PutAsync(vol, model, "/e.bin", v1);
                    DfsSnapshotInfo s1 = await SnapshotAsync(vol, "s1");
                    TreeModel model1 = model.Clone();
                    await PutAsync(vol, model, "/e.bin", v2);
                    DfsSnapshotInfo s2 = await SnapshotAsync(vol, "s2");
                    await vol.Backups.RequestSyncAsync(b0.TargetName, "s1", Ct);
                    await vol.Backups.RequestSyncAsync(b0.TargetName, "s2", Ct);
                    DfsBackupCheckpoint cp1 = await vol.Backups.AwaitBackupSnapshotAsync(b0.TargetName, "s1", SyncTimeout, Ct);
                    AssertExactCheckpoint(cp1, b0.TargetName, s1);
                    DfsBackupCheckpoint cp2 = await vol.Backups.AwaitBackupSnapshotAsync(b0.TargetName, "s2", SyncTimeout, Ct);
                    AssertExactCheckpoint(cp2, b0.TargetName, s2);
                    Assert.NotEqual(cp1.SnapshotId, cp2.SnapshotId);
                    await using DfsClient reader = await ConnectBackupReaderAsync(cluster);
                    DfsSnapshotInfo retained = await reader.GetVolume(Volume).Snapshots.GetAsync("s1", Ct);
                    Assert.Equal(retained.Retention.ExpiresUtc, cp1.RetentionExpiryUtc);
                    Assert.Equal(retained.Retention.Hold, cp1.Held);
                    await AssertBackupSnapshotAsync(reader, "s1", model1, because + ": S1 retains the exact earlier version");
                    break;
                }

                case ExactBarrierCase.LaterSnapshotOnly:
                {
                    CutFromSources(cluster, b0.Name);
                    await PutAsync(vol, model, "/e.bin", v1);
                    DfsSnapshotInfo s1 = await SnapshotAsync(vol, "s1");
                    TreeModel model1 = model.Clone();
                    await PutAsync(vol, model, "/e.bin", v2);
                    DfsSnapshotInfo s2 = await SnapshotAsync(vol, "s2");
                    cluster.Network.Heal();
                    await SyncExactAsync(vol, b0.TargetName, s2);

                    DfsBackupCheckpoint? cp1 = null;
                    DfsBackupNotReachedException? notReached = null;
                    try
                    {
                        cp1 = await vol.Backups.AwaitBackupSnapshotAsync(b0.TargetName, "s1", TimeSpan.FromSeconds(5), Ct);
                    }
                    catch (DfsBackupNotReachedException ex)
                    {
                        notReached = ex;
                    }

                    await using DfsClient reader = await ConnectBackupReaderAsync(cluster);
                    if (cp1 is not null)
                    {
                        // Legal only because S1 itself was transferred and is durably retained.
                        AssertExactCheckpoint(cp1, b0.TargetName, s1);
                        await AssertPublishedStateAsync(b0, "s2", ["s1", "s2"], [], because + ": S1 itself retained");
                        await AssertBackupSnapshotAsync(reader, "s1", model1, because + ": S1 itself retained");
                    }
                    else
                    {
                        Assert.NotNull(notReached);
                        Assert.Equal(b0.TargetName, notReached.Target);
                        Assert.Equal("s2", notReached.LastPublished?.SnapshotName);
                        Assert.DoesNotContain("s1", await b0.Inspect.GetPublishedSnapshotsAsync(Volume));
                        await AssertNotSelectableAsync(reader, "s1", because + ": S2 is not a substitute");
                    }

                    break;
                }

                case ExactBarrierCase.ExactSnapshotExpired:
                {
                    await b0.RestartAsync(o => o with { KeepLastSnapshots = 1 });
                    await PutAsync(vol, model, "/e.bin", v1);
                    DfsSnapshotInfo s1 = await SnapshotAsync(vol, "s1");
                    await SyncExactAsync(vol, b0.TargetName, s1);
                    await SetBackupRetentionAsync(cluster, b0, "s1", new DfsRetention(b0.Clock.GetUtcNow().AddSeconds(1), Hold: false));
                    await PutAsync(vol, model, "/e.bin", v2);
                    DfsSnapshotInfo s2 = await SnapshotAsync(vol, "s2");
                    await SyncExactAsync(vol, b0.TargetName, s2);
                    await vol.Snapshots.DeleteAsync("s1", Ct); // the source can no longer re-send S1
                    await cluster.DelayAsync(TimeSpan.FromSeconds(5), Ct);
                    await cluster.WaitUntilAsync(async () => !(await b0.Inspect.GetPublishedSnapshotsAsync(Volume)).Contains("s1"),
                        TimeSpan.FromSeconds(30), "an expired, unpinned snapshot beyond KeepLastSnapshots is pruned by destination retention", Ct);

                    Exception refused = await AssertRefusedAsync(
                        () => vol.Backups.AwaitBackupSnapshotAsync(b0.TargetName, "s1", ShortAwait, Ct),
                        ex => ex is DfsBackupNotReachedException || ex is DfsException { ErrorCode: DfsErrorCode.SnapshotNotFound },
                        because + ": the exact barrier for an expired snapshot must not be satisfied by S2");
                    if (refused is DfsBackupNotReachedException nr)
                        Assert.Equal("s2", nr.LastPublished?.SnapshotName);
                    await using DfsClient reader = await ConnectBackupReaderAsync(cluster);
                    await AssertNotSelectableAsync(reader, "s1", because);
                    await AssertBackupSnapshotAsync(reader, "s2", model, because + ": S2 retained");
                    break;
                }

                case ExactBarrierCase.CommitAndSnapshot:
                {
                    CommitToken t1 = await PutAsync(vol, model, "/exact.bin", v1, new DfsMutationOptions { CommitSnapshotName = "x1" });
                    TreeModel modelX1 = model.Clone();
                    DfsSnapshotInfo x1 = await vol.Snapshots.GetAsync("x1", Ct);
                    Assert.Equal(t1, x1.Boundary);
                    await PutAsync(vol, model, "/exact.bin", v2);
                    DfsSnapshotInfo s2 = await SnapshotAsync(vol, "s2");
                    await vol.Backups.RequestSyncAsync(b0.TargetName, "x1", Ct);
                    await vol.Backups.RequestSyncAsync(b0.TargetName, "s2", Ct);
                    DfsBackupCheckpoint cpx = await vol.Backups.AwaitBackupSnapshotAsync(b0.TargetName, "x1", SyncTimeout, Ct);
                    AssertExactCheckpoint(cpx, b0.TargetName, x1);
                    Assert.Equal(t1, cpx.CoveredCommit);
                    await using DfsClient reader = await ConnectBackupReaderAsync(cluster);
                    await AssertBackupSnapshotAsync(reader, "x1", modelX1, because + ": exact committed state");

                    // The at-least contract may return either checkpoint; whichever it returns describes itself truthfully.
                    DfsBackupCheckpoint atLeast = await vol.Backups.AwaitBackupAsync(b0.TargetName, t1, SyncTimeout, Ct);
                    Assert.True(atLeast.CoveredCommit.IsAtOrAfter(t1));
                    Assert.Contains(atLeast.SnapshotName, new[] { "x1", "s2" });
                    Assert.Equal(atLeast.SnapshotName == "x1" ? x1.Boundary : s2.Boundary, atLeast.CoveredCommit);
                    await AssertBackupSnapshotAsync(reader, atLeast.SnapshotName, atLeast.SnapshotName == "x1" ? modelX1 : model,
                        because + ": at-least checkpoint contents");
                    break;
                }
            }
        });

    private static async Task SetBackupRetentionAsync(TestCluster cluster, TestBackupNode backup, string snapshot, DfsRetention retention)
    {
        await using DfsClient admin = await ConnectBackupAsync(cluster, backup.RetentionAdminCredentials, backup.Index, NextClientName("ret"));
        await admin.GetVolume(Volume).Snapshots.SetRetentionAsync(snapshot, retention, Ct);
    }

    private static DateTimeOffset WholeSeconds(DateTimeOffset time) => DateTimeOffset.FromUnixTimeSeconds(time.ToUnixTimeSeconds());

    // ------------------------------------------------------------------------------------------------------------
    // BAK-019
    // ------------------------------------------------------------------------------------------------------------

    [Theory]
    [InlineData(BackupDisruption.Isolated)]
    [InlineData(BackupDisruption.Crashed)]
    [InlineData(BackupDisruption.Paused)]
    [InlineData(BackupDisruption.BlackHoled)]
    [Catalog("BAK-019", "S", "INV-01", "INV-15", "INV-19")]
    public Task BAK_019_BackupBarrierTimeoutDoesNotUndoLive(BackupDisruption disruption) =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            TestBackupNode b0 = cluster.BackupNodes[0];
            await using DfsClient live = await ConnectLiveAsync(cluster);
            DfsVolume vol = live.GetVolume(Volume);
            await RegisterAsync(b0);
            TreeModel model = InitialTree(26);
            await WriteTreeAsync(vol, model);
            (DfsSnapshotInfo s1, _) = await SnapshotAndSyncAsync(vol, b0.TargetName, "s1");

            // Live commit first, then the backup connectivity is lost.
            DfsOperationId op = await vol.NewOperationIdAsync(Ct);
            byte[] x = TestData.Bytes(240, 120 * 1024);
            CommitToken t = await PutAsync(vol, model, "/c.bin", x, new DfsMutationOptions { OperationId = op });
            FileInspection? committed = await cluster.Inspect.GetFileAsync(Volume, "/c.bin");
            Assert.NotNull(committed);
            switch (disruption)
            {
                case BackupDisruption.Isolated:
                    CutFromSources(cluster, b0.Name);
                    break;
                case BackupDisruption.Crashed:
                    await b0.CrashAsync();
                    break;
                case BackupDisruption.Paused:
                    await b0.PauseAsync();
                    break;
                default:
                    BlackHoleFromSources(cluster, b0.Name);
                    break;
            }

            TimeSpan timeout = TimeSpan.FromSeconds(3);
            DfsBackupNotReachedException? notReached = null;
            TimeSpan waited = await TimeAsync(cluster, async () =>
                notReached = await Assert.ThrowsAsync<DfsBackupNotReachedException>(() => vol.Backups.AwaitBackupAsync(b0.TargetName, t, timeout, Ct)));
            Assert.NotNull(notReached);
            Assert.InRange(waited, timeout, timeout + TimeSpan.FromSeconds(5));
            Assert.Equal(b0.TargetName, notReached.Target);
            Assert.Equal(DfsErrorCode.BackupNotReached, notReached.ErrorCode);
            Assert.NotNull(notReached.LastPublished);
            Assert.Equal("s1", notReached.LastPublished.SnapshotName);
            Assert.Equal(s1.Boundary, notReached.LastPublished.CoveredCommit);
            Assert.False(notReached.LastPublished.CoveredCommit.IsAtOrAfter(t), "no remote guarantee may be reported for T");

            // The exact barrier on a commit-and-snapshot also times out without undoing either.
            byte[] y = TestData.Bytes(241, 60 * 1024);
            CommitToken tx2 = await PutAsync(vol, model, "/d.bin", y, new DfsMutationOptions { CommitSnapshotName = "x2" });
            await Assert.ThrowsAsync<DfsBackupNotReachedException>(() => vol.Backups.AwaitBackupSnapshotAsync(b0.TargetName, "x2", timeout, Ct));

            // Live state is untouched by the timeouts.
            await using (DfsClient fresh = await ConnectLiveAsync(cluster, NextClientName("verify")))
            {
                DfsVolume freshVol = fresh.GetVolume(Volume);
                await AssertFileBytesAsync(freshVol.Files, "/c.bin", x, "committed live data after a backup barrier timeout");
                await AssertFileBytesAsync(freshVol.Files, "/d.bin", y, "commit-and-snapshot data after a backup barrier timeout");
                DfsOperationOutcome outcome = await freshVol.QueryOperationAsync(op, false, Ct);
                Assert.Equal(DfsOutcomeStatus.Committed, outcome.Status);
                Assert.Equal(t, outcome.Token);
                DfsSnapshotInfo x2 = await freshVol.Snapshots.GetAsync("x2", Ct);
                Assert.Equal(tx2, x2.Boundary);
            }

            FileInspection? after = await cluster.Inspect.GetFileAsync(Volume, "/c.bin");
            Assert.NotNull(after);
            Assert.Equal(committed.Revision, after.Revision);
            Assert.Equal(committed.Inode, after.Inode);
            DfsBackupStatus status = await vol.Backups.GetStatusAsync(b0.TargetName, Ct);
            Assert.False(status.PublishedBoundary.IsAtOrAfter(t));

            // Connectivity restored: a later complete checkpoint eventually satisfies T.
            switch (disruption)
            {
                case BackupDisruption.Crashed:
                    await b0.RestartAsync();
                    break;
                case BackupDisruption.Paused:
                    await b0.ResumeAsync();
                    break;
                default:
                    cluster.Network.Heal();
                    break;
            }

            DfsSnapshotInfo s3 = await SnapshotAsync(vol, "s3");
            await vol.Backups.RequestSyncAsync(b0.TargetName, "s3", Ct);
            DfsBackupCheckpoint cp = await vol.Backups.AwaitBackupAsync(b0.TargetName, t, SyncTimeout, Ct);
            Assert.True(cp.CoveredCommit.IsAtOrAfter(t));
            await using DfsClient reader = await ConnectBackupReaderAsync(cluster, 0, NextClientName("bk"));
            await using DfsSnapshotView view = await reader.GetVolume(Volume).Snapshots.OpenAsync(cp.SnapshotName, Ct);
            await AssertFileBytesAsync(view.Files, "/c.bin", x, "the live commit is contained in the checkpoint that satisfied T");
        });

    // ------------------------------------------------------------------------------------------------------------
    // BAK-020
    // ------------------------------------------------------------------------------------------------------------

    [Theory]
    [InlineData(CrashMode.LoseUnflushed)]
    [InlineData(CrashMode.KeepUnflushed)]
    [InlineData(CrashMode.RandomSubset)]
    [InlineData(CrashMode.TearLastWrite)]
    [Catalog("BAK-020", "S", "INV-01", "INV-10", "INV-13")]
    public Task BAK_020_BackupAcknowledgementIsDurable(CrashMode mode) =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            TestBackupNode b0 = cluster.BackupNodes[0];
            await using DfsClient live = await ConnectLiveAsync(cluster);
            DfsVolume vol = live.GetVolume(Volume);
            await RegisterAsync(b0);
            TreeModel model = InitialTree(27);
            await WriteTreeAsync(vol, model);
            (DfsSnapshotInfo s1, DfsBackupCheckpoint cp1) = await SnapshotAndSyncAsync(vol, b0.TargetName, "s1");

            // Crash immediately after the acknowledgement, with no graceful shutdown; no source can re-send.
            await b0.CrashAsync(mode);
            CutFromSources(cluster, b0.Name);
            await b0.RestartAsync();
            string because = $"restart after an acknowledged publication ({mode})";
            await AssertPublishedStateAsync(b0, "s1", ["s1"], [], because);
            await using (DfsClient reader = await ConnectBackupReaderAsync(cluster, 0, NextClientName("bk")))
            {
                await AssertBackupSnapshotAsync(reader, "s1", model, because);
                DfsSnapshotInfo info = await reader.GetVolume(Volume).Snapshots.GetAsync("s1", Ct);
                Assert.Equal(s1.Id, info.Id);
                Assert.Equal(s1.Boundary, info.Boundary);
                Assert.Equal(cp1.RetentionExpiryUtc, info.Retention.ExpiresUtc);
                Assert.Equal(cp1.Held, info.Retention.Hold);
            }

            BackupSnapshotInspection inspected = (await b0.Inspect.GetSnapshotsAsync(Volume)).Single(s => s.Name == "s1");
            Assert.Equal(s1.Boundary, inspected.CoveredCommit);
            Assert.Equal(cp1.RetentionExpiryUtc, inspected.Retention.ExpiresUtc);
            Assert.Equal(cp1.Held, inspected.Retention.Hold);

            // An acknowledged retention change is equally durable.
            var promised = new DfsRetention(WholeSeconds(b0.Clock.GetUtcNow().AddDays(30)), Hold: true);
            await SetBackupRetentionAsync(cluster, b0, "s1", promised);
            await b0.CrashAsync(mode);
            await b0.RestartAsync();
            await using (DfsClient reader = await ConnectBackupReaderAsync(cluster, 0, NextClientName("bk")))
            {
                DfsSnapshotInfo info = await reader.GetVolume(Volume).Snapshots.GetAsync("s1", Ct);
                Assert.Equal(promised, info.Retention);
                await AssertBackupSnapshotAsync(reader, "s1", model, because + ", after the retention change");
            }

            // Reconnected, the exact barrier is still satisfied from the durable state.
            cluster.Network.Heal();
            DfsBackupCheckpoint again = await vol.Backups.AwaitBackupSnapshotAsync(b0.TargetName, "s1", SyncTimeout, Ct);
            AssertExactCheckpoint(again, b0.TargetName, s1);
            Assert.True(again.Held, "the reported guarantee must reflect the durable hold");
        });

    // ------------------------------------------------------------------------------------------------------------
    // BAK-021
    // ------------------------------------------------------------------------------------------------------------

    [Fact]
    [Catalog("BAK-021", "S", "INV-13", "INV-09", "INV-15")]
    public Task BAK_021_SourceDeletionDoesNotEraseProtectedHistory() =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            TestBackupNode b0 = cluster.BackupNodes[0];
            await using DfsClient live = await ConnectLiveAsync(cluster);
            DfsVolume vol = live.GetVolume(Volume);
            await RegisterAsync(b0);
            var model = new TreeModel();
            for (int i = 0; i < 3; i++)
                model.Put($"/hist/h{i}.bin", TestData.Bytes(250 + (ulong)i, 150 * 1024));
            model.Put("/keep.txt", "kept"u8.ToArray());
            await WriteTreeAsync(vol, model);
            await SnapshotAndSyncAsync(vol, b0.TargetName, "s1");
            TreeModel model1 = model.Clone();
            HashSet<ObjectId> histObjects = await FileObjectsAsync(cluster, model1.Files.Keys.Where(p => p.StartsWith("/hist/", StringComparison.Ordinal)));
            Assert.NotEmpty(histObjects);
            await SetBackupRetentionAsync(cluster, b0, "s1", new DfsRetention(null, Hold: true));

            // The source deletes the data and its own snapshot, then mirrors.
            for (int i = 0; i < 3; i++)
                await DeleteFileAsync(vol, model, $"/hist/h{i}.bin");
            await vol.Directories.DeleteAsync("/hist", false, Ct);
            model.Directories.Remove("/hist");
            await vol.Snapshots.DeleteAsync("s1", Ct);
            await AssertEx.ThrowsDfsAsync<DfsException>(() => vol.Snapshots.GetAsync("s1", Ct), DfsErrorCode.SnapshotNotFound);
            await SnapshotAndSyncAsync(vol, b0.TargetName, "s2");
            await AssertPublishedStateAsync(b0, "s2", ["s1", "s2"], [], "mirroring after source deletion");
            await using DfsClient reader = await ConnectBackupReaderAsync(cluster);
            await AssertBackupSnapshotAsync(reader, "s1", model1, "protected S1 after source deletion");
            await AssertBackupSnapshotAsync(reader, "s2", model, "S2 mirrors the deletion");

            // The source reclaims its copies; the destination's history does not depend on them.
            await cluster.WaitUntilAsync(async () => !(await cluster.Inspect.GetAllStoredObjectsAsync(Volume)).Overlaps(histObjects),
                TimeSpan.FromSeconds(60), "source GC reclaims objects referenced by no source root", Ct);
            Assert.Subset(await BackupObjectsAsync(b0), histObjects);
            await AssertBackupSnapshotAsync(reader, "s1", model1, "protected S1 after the source reclaimed its copies");

            // Removing the target registration at the source does not erase destination history either.
            await vol.Backups.RemoveTargetAsync(b0.TargetName, Ct);
            Assert.DoesNotContain(b0.TargetName, await vol.Backups.ListTargetsAsync(Ct));
            await cluster.DelayAsync(TimeSpan.FromSeconds(10), Ct);
            await AssertPublishedStateAsync(b0, "s2", ["s1", "s2"], [], "after the source removed the target");
            await AssertBackupSnapshotAsync(reader, "s1", model1, "protected S1 after target removal");
            DfsSnapshotInfo held = await reader.GetVolume(Volume).Snapshots.GetAsync("s1", Ct);
            Assert.True(held.Retention.Hold);
        });

    // ------------------------------------------------------------------------------------------------------------
    // BAK-022
    // ------------------------------------------------------------------------------------------------------------

    public enum ExporterAttack
    {
        DeleteSnapshot,
        DeleteSnapshotSync,
        ShortenRetention,
        ReleaseHold,
        ExpireNow,
    }

    [Theory]
    [InlineData(ExporterAttack.DeleteSnapshot)]
    [InlineData(ExporterAttack.DeleteSnapshotSync)]
    [InlineData(ExporterAttack.ShortenRetention)]
    [InlineData(ExporterAttack.ReleaseHold)]
    [InlineData(ExporterAttack.ExpireNow)]
    [Catalog("BAK-022", "S", "INV-13", "INV-12")]
    public Task BAK_022_ExporterCannotDeleteProtectedBackup(ExporterAttack attack) =>
        // A synchronous SDK wrapper blocks its calling thread (ARCHITECTURE §9.6); inside the single-threaded
        // deterministic scheduler that would starve the simulated reply, so the sync variant runs in real time.
        ClusterTest.RunAsync(Options() with { Mode = attack == ExporterAttack.DeleteSnapshotSync ? ClusterMode.Realtime : ClusterMode.Deterministic }, async cluster =>
        {
            TestBackupNode b0 = cluster.BackupNodes[0];
            await using DfsClient live = await ConnectLiveAsync(cluster);
            DfsVolume vol = live.GetVolume(Volume);
            await RegisterAsync(b0);
            TreeModel model = InitialTree(28);
            await WriteTreeAsync(vol, model);
            await SnapshotAndSyncAsync(vol, b0.TargetName, "s1");
            var protectedRetention = new DfsRetention(WholeSeconds(b0.Clock.GetUtcNow().AddDays(30)), Hold: true);
            await SetBackupRetentionAsync(cluster, b0, "s1", protectedRetention);

            await using DfsClient exporter = await ConnectBackupAsync(cluster, b0.ExportCredentials, 0, NextClientName("exporter"));
            DfsSnapshotClient snapshots = exporter.GetVolume(Volume).Snapshots;
            Func<Task> act = attack switch
            {
                ExporterAttack.DeleteSnapshot => () => snapshots.DeleteAsync("s1", Ct),
                ExporterAttack.DeleteSnapshotSync => () =>
                {
                    snapshots.Delete("s1");
                    return Task.CompletedTask;
                },
                ExporterAttack.ShortenRetention => () => snapshots.SetRetentionAsync("s1", new DfsRetention(b0.Clock.GetUtcNow().AddMinutes(1), true), Ct),
                ExporterAttack.ReleaseHold => () => snapshots.SetRetentionAsync("s1", protectedRetention with { Hold = false }, Ct),
                _ => () => snapshots.SetRetentionAsync("s1", new DfsRetention(b0.Clock.GetUtcNow().AddDays(-1), false), Ct),
            };
            Exception refused = await AssertRefusedAsync(act, IsAccessDenial, $"export credentials attempting {attack}");
            DfsErrorCode code = DfsErrors.ToCode(refused);
            Assert.True(code is DfsErrorCode.AccessDenied or DfsErrorCode.RetentionProtected or DfsErrorCode.ReadOnly,
                $"{attack} must be denied as an authorization/retention failure, got {code}");

            // Nothing changed, also after retention enforcement had time to run.
            await cluster.DelayAsync(TimeSpan.FromSeconds(10), Ct);
            await using DfsClient reader = await ConnectBackupReaderAsync(cluster);
            DfsSnapshotInfo info = await reader.GetVolume(Volume).Snapshots.GetAsync("s1", Ct);
            Assert.Equal(protectedRetention, info.Retention);
            await AssertPublishedStateAsync(b0, "s1", ["s1"], [], $"after {attack} by the exporter");
            await AssertBackupSnapshotAsync(reader, "s1", model, $"S1 after {attack} by the exporter");

            // The source's own administrator has no standing on the backup either.
            await AssertRefusedAsync(async () =>
            {
                await using DfsClient sourceAdmin = await ConnectBackupAsync(cluster, cluster.AdminCredentials, 0, NextClientName("srcadmin"));
                await sourceAdmin.GetVolume(Volume).Snapshots.DeleteAsync("s1", Ct);
            }, IsAccessDenial, "source cluster administrator credentials against protected backup history");
            await AssertBackupSnapshotAsync(reader, "s1", model, "S1 after the source administrator attempt");

            // Positive control: the backup's own retention administrator may extend protection.
            DfsRetention extended = protectedRetention with { ExpiresUtc = protectedRetention.ExpiresUtc!.Value.AddDays(30) };
            await SetBackupRetentionAsync(cluster, b0, "s1", extended);
            Assert.Equal(extended, (await reader.GetVolume(Volume).Snapshots.GetAsync("s1", Ct)).Retention);
            await AssertRefusedAsync(() => snapshots.SetRetentionAsync("s1", protectedRetention, Ct), IsAccessDenial,
                "export credentials cannot shorten the extended retention either");
            Assert.Equal(extended, (await reader.GetVolume(Volume).Snapshots.GetAsync("s1", Ct)).Retention);
        });

    // ------------------------------------------------------------------------------------------------------------
    // BAK-023
    // ------------------------------------------------------------------------------------------------------------

    public enum PinBudget
    {
        Age,
        Bytes,
    }

    private static bool IsTransferPin(PinInspection pin) => string.Equals(pin.Kind, "Transfer", StringComparison.OrdinalIgnoreCase);

    [Theory]
    [InlineData(PinBudget.Age)]
    [InlineData(PinBudget.Bytes)]
    [Catalog("BAK-023", "S", "INV-09", "INV-13", "INV-15", "INV-16")]
    public Task BAK_023_OfflinePastPinBudgetTriggersReseed(PinBudget budget) =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            TestBackupNode b0 = cluster.BackupNodes[0];
            await using DfsClient live = await ConnectLiveAsync(cluster);
            DfsVolume vol = live.GetVolume(Volume);
            DfsBackupBudgets budgets = budget == PinBudget.Age
                ? new DfsBackupBudgets(MaxPinAge: TimeSpan.FromSeconds(5), MaxPinnedBytes: 1L << 30)
                : new DfsBackupBudgets(MaxPinAge: TimeSpan.FromHours(1), MaxPinnedBytes: 256 * 1024);

            // The initial full sync runs online under generous budgets; the tight budget applies from then on.
            await RegisterAsync(b0);
            TreeModel model = InitialTree(29);
            await WriteTreeAsync(vol, model);
            (DfsSnapshotInfo s1, _) = await SnapshotAndSyncAsync(vol, b0.TargetName, "s1");
            TreeModel model1 = model.Clone();
            Guid oldLineage = (await vol.Backups.GetStatusAsync(b0.TargetName, Ct)).LineageId;
            Assert.NotEqual(Guid.Empty, oldLineage);
            await vol.Backups.ConfigureAsync(b0.TargetName, ManualSchedule, budgets, Ct);

            // The target goes offline while unpublished history accumulates behind a source transfer pin.
            CutFromSources(cluster, b0.Name);
            await PutAsync(vol, model, "/off/a.bin", TestData.Bytes(260, 1024 * 1024));
            HashSet<ObjectId> pinnedObjects = await FileObjectsAsync(cluster, ["/off/a.bin"]);
            await SnapshotAsync(vol, "s2");
            await vol.Backups.RequestSyncAsync(b0.TargetName, "s2", Ct);
            await cluster.WaitUntilAsync(async () => (await cluster.Inspect.GetVolumeStateAsync(Volume)).Pins.Any(IsTransferPin),
                FaultTimeout, "the exporter pins the requested snapshot on the source although the target is unreachable", Ct);
            bool userSnapshotDeleted = false;
            if (budget == PinBudget.Age)
            {
                await cluster.DelayAsync(TimeSpan.FromSeconds(10), Ct);
            }
            else
            {
                // Make well over 256 KiB retained *only* for the offline target, whatever the pin accounting: the live
                // file moves on twice (the intermediate version is protected only by the pin's GC watermark) and the
                // user snapshot is deleted (FSD §11.1 keeps data still needed by the transfer).
                await PutAsync(vol, model, "/off/a.bin", TestData.Bytes(261, 1024 * 1024));
                pinnedObjects.UnionWith(await FileObjectsAsync(cluster, ["/off/a.bin"]));
                await PutAsync(vol, model, "/off/a.bin", TestData.Bytes(263, 1024 * 1024));
                await vol.Snapshots.DeleteAsync("s2", Ct);
                userSnapshotDeleted = true;
            }

            await cluster.WaitUntilAsync(async () => (await vol.Backups.GetStatusAsync(b0.TargetName, Ct)).ReseedRequired,
                SyncTimeout, $"a target offline beyond its {budget} budget must be marked for reseed", Ct);
            DfsBackupStatus st = await vol.Backups.GetStatusAsync(b0.TargetName, Ct);
            Assert.True(st.PinnedSourceBytes <= budgets.MaxPinnedBytes, $"source pinned bytes {st.PinnedSourceBytes} exceed the budget {budgets.MaxPinnedBytes}");
            Assert.Equal(s1.Boundary, st.PublishedBoundary);
            Assert.Equal("s1", st.LatestPublished?.SnapshotName);
            VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Volume);
            Assert.True(state.Backups.Single(b => b.Target == b0.TargetName).ReseedRequired, "the replicated backup progress records the reseed");
            await cluster.WaitUntilAsync(async () => !(await cluster.Inspect.GetVolumeStateAsync(Volume)).Pins.Any(IsTransferPin),
                TimeSpan.FromSeconds(10), "abandoning the lineage releases every transfer pin", Ct);

            // Live work never depends on the offline target's history: commits proceed and source GC reclaims what
            // only the abandoned lineage still pinned.
            await AssertLiveCommitUnblockedAsync(cluster, vol, model, "/off/live.bin", TestData.Bytes(262, 50 * 1024), "live commit with the target abandoned");
            if (budget == PinBudget.Bytes)
            {
                await cluster.WaitUntilAsync(async () => !(await cluster.Inspect.GetAllStoredObjectsAsync(Volume)).Overlaps(pinnedObjects),
                    SyncTimeout, "source GC reclaims the data that only the abandoned transfer pin retained", Ct);
            }

            HashSet<ObjectId> abandonedObjects = [.. pinnedObjects, .. await FileObjectsAsync(cluster, ["/off/a.bin"])];
            if (!userSnapshotDeleted)
                await vol.Snapshots.DeleteAsync("s2", Ct);
            await DeleteFileAsync(vol, model, "/off/a.bin");
            await cluster.WaitUntilAsync(async () => !(await cluster.Inspect.GetAllStoredObjectsAsync(Volume)).Overlaps(abandonedObjects),
                SyncTimeout, "source GC is not blocked by the abandoned backup lineage", Ct);

            // The destination keeps its last complete snapshot and nothing of the abandoned lineage.
            string[] abandoned = ["s2"];
            await AssertPublishedStateAsync(b0, "s1", ["s1"], abandoned, "target abandoned while offline");
            Assert.Equal(["s1"], (await b0.Inspect.GetSnapshotsAsync(Volume)).Select(s => s.Name));
            await using (DfsClient probe = await ConnectBackupReaderAsync(cluster, 0, NextClientName("probe")))
                await AssertBackupSnapshotAsync(probe, "s1", model1, "S1 kept while the lineage is abandoned");

            // Reconnect: an explicit full reseed under a new lineage.
            cluster.Network.Heal();
            (DfsSnapshotInfo s4, _) = await SnapshotAndSyncAsync(vol, b0.TargetName, "s4");
            DfsBackupStatus after = await vol.Backups.GetStatusAsync(b0.TargetName, Ct);
            Assert.False(after.ReseedRequired);
            Assert.NotEqual(oldLineage, after.LineageId);
            Assert.Equal(s4.Boundary, after.PublishedBoundary);
            IReadOnlyList<BackupSnapshotInspection> snaps = await b0.Inspect.GetSnapshotsAsync(Volume);
            Assert.Equal(oldLineage, snaps.Single(s => s.Name == "s1").Lineage);
            Assert.Equal(after.LineageId, snaps.Single(s => s.Name == "s4").Lineage);
            await AssertPublishedStateAsync(b0, "s4", ["s1", "s4"], abandoned, "after the reseed");
            await using DfsClient reader = await ConnectBackupReaderAsync(cluster, 0, NextClientName("bk"));
            await AssertBackupSnapshotAsync(reader, "s1", model1, "old-lineage S1 after the reseed");
            await AssertBackupSnapshotAsync(reader, "s4", model, "reseeded S4");
        });

    // ------------------------------------------------------------------------------------------------------------
    // BAK-024
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Reseed failures in execution order. Object-receipt failures come first (a resumable transfer keeps durable
    /// objects, so each attempt consumes at most one more object with <c>MaxConcurrentObjects = 1</c>); every failure
    /// before the last one strikes before the destination holds the publish request, so S3 cannot legally publish yet.
    /// A network cut at or after the publish request would let the destination finish publishing on its own, which is
    /// not a failed reseed, so connectivity failures are injected while objects are still flowing.
    /// </summary>
    public enum ReseedFailure
    {
        DiskFullOnObject,
        CrashOnObject,
        NetworkCutOnObject,
        IoErrorBeforeVerify,
        TornCrashBeforePointer,
    }

    [Fact]
    [Catalog("BAK-024", "S", "INV-13", "INV-10", "INV-19")]
    public Task BAK_024_ReseedInterruptedPreservesHistory() =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            TestBackupNode b0 = cluster.BackupNodes[0];
            await using DfsClient live = await ConnectLiveAsync(cluster);
            DfsVolume vol = live.GetVolume(Volume);
            DfsBackupBudgets serial = GenerousBudgets with { MaxConcurrentObjects = 1 };
            await RegisterAsync(b0, serial);
            TreeModel model = InitialTree(30);
            await WriteTreeAsync(vol, model);
            (DfsSnapshotInfo s1, _) = await SnapshotAndSyncAsync(vol, b0.TargetName, "s1");
            TreeModel model1 = model.Clone();
            Guid oldLineage = (await vol.Backups.GetStatusAsync(b0.TargetName, Ct)).LineageId;

            // Force the reseed: offline beyond a (temporarily) tight age budget.
            await vol.Backups.ConfigureAsync(b0.TargetName, ManualSchedule, serial with { MaxPinAge = TimeSpan.FromSeconds(5) }, Ct);
            CutFromSources(cluster, b0.Name);
            await PutAsync(vol, model, "/reseed/a.bin", TestData.Bytes(270, 512 * 1024));
            await SnapshotAsync(vol, "s2");
            await vol.Backups.RequestSyncAsync(b0.TargetName, "s2", Ct);
            await cluster.DelayAsync(TimeSpan.FromSeconds(10), Ct);
            await cluster.WaitUntilAsync(async () => (await vol.Backups.GetStatusAsync(b0.TargetName, Ct)).ReseedRequired,
                SyncTimeout, "the target is marked for reseed", Ct);

            // From here on only the injected failures interrupt the reseed (no further budget abandonment).
            await vol.Backups.ConfigureAsync(b0.TargetName, ManualSchedule, serial, Ct);
            Assert.True((await vol.Backups.GetStatusAsync(b0.TargetName, Ct)).ReseedRequired, "reconfiguring budgets must not clear the reseed mark");
            cluster.Network.Heal();
            await PutAsync(vol, model, "/reseed/b.bin", TestData.Bytes(271, 300 * 1024));
            DfsSnapshotInfo s3 = await SnapshotAsync(vol, "s3");
            int closureObjects = (await FileObjectsAsync(cluster, model.Files.Keys)).Count;
            Assert.True(closureObjects >= 5, $"the reseed must need enough objects for every receipt-phase failure (has {closureObjects})");

            // Fail the reseed repeatedly, in different phases and ways.
            foreach (ReseedFailure failure in Enum.GetValues<ReseedFailure>())
            {
                FaultAction CutThenThrow(Func<Exception> error) => FaultAction.Custom(_ =>
                {
                    CutFromSources(cluster, b0.Name);
                    throw error();
                });

                FaultArm arm = failure switch
                {
                    ReseedFailure.DiskFullOnObject => cluster.Faults.Arm(
                        new FaultMatch(FaultSites.BackupObjectReceived, Node: b0.Name, Volume: Volume, Occurrence: 1),
                        CutThenThrow(() => new IOException("injected ENOSPC", unchecked((int)0x80070070)))),
                    ReseedFailure.CrashOnObject => cluster.Faults.Arm(
                        new FaultMatch(FaultSites.BackupObjectReceived, Node: b0.Name, Volume: Volume, Occurrence: 2), FaultAction.CrashNode(CrashMode.LoseUnflushed)),
                    ReseedFailure.NetworkCutOnObject => cluster.Faults.Arm(
                        new FaultMatch(FaultSites.BackupObjectReceived, Node: b0.Name, Volume: Volume, Occurrence: 1), FaultAction.Custom(_ =>
                        {
                            CutFromSources(cluster, b0.Name);
                            cluster.Network.ResetConnections(b0.Name);
                            return Task.CompletedTask;
                        })),
                    ReseedFailure.IoErrorBeforeVerify => cluster.Faults.Arm(
                        new FaultMatch(FaultSites.BackupBeforeClosureVerify, Node: b0.Name, Volume: Volume), CutThenThrow(() => new IOException("injected EIO before closure verification"))),
                    _ => cluster.Faults.Arm(
                        new FaultMatch(FaultSites.BackupBeforePointer, Node: b0.Name, Volume: Volume), FaultAction.CrashNode(CrashMode.TearLastWrite)),
                };
                await vol.Backups.RequestSyncAsync(b0.TargetName, "s3", Ct);
                await arm.WaitTriggeredAsync(SyncTimeout);
                cluster.Faults.AssertReached(arm);
                arm.Disarm();
                CutFromSources(cluster, b0.Name);
                if (failure is ReseedFailure.CrashOnObject or ReseedFailure.TornCrashBeforePointer)
                {
                    await cluster.WaitUntilAsync(() => Task.FromResult(b0.State == TestNodeState.Crashed), FaultTimeout, "the backup crashed during the reseed", Ct);
                    await b0.RestartAsync();
                }

                // The old recovery point is intact. S3 cannot be complete before the last failure; after a crash at the
                // pointer step a restart may expose it only complete (BAK-012).
                string because = $"reseed interrupted by {failure}";
                bool s3Exposed = failure == ReseedFailure.TornCrashBeforePointer
                    && (await b0.Inspect.GetPublishedSnapshotsAsync(Volume)).Contains("s3", StringComparer.Ordinal);
                await AssertPublishedStateAsync(b0, s3Exposed ? "s3" : "s1", s3Exposed ? ["s1", "s3"] : ["s1"], s3Exposed ? ["s2"] : ["s2", "s3"], because);
                IReadOnlyList<BackupSnapshotInspection> snaps = await b0.Inspect.GetSnapshotsAsync(Volume);
                Assert.Equal(s3Exposed ? ["s1", "s3"] : ["s1"], snaps.Select(s => s.Name));
                BackupSnapshotInspection kept = snaps.Single(s => s.Name == "s1");
                Assert.Equal(oldLineage, kept.Lineage);
                Assert.Equal(s1.Boundary, kept.CoveredCommit);
                if (s3Exposed)
                    Assert.NotEqual(oldLineage, snaps.Single(s => s.Name == "s3").Lineage);
                await using (DfsClient probe = await ConnectBackupReaderAsync(cluster, 0, NextClientName("probe")))
                {
                    await AssertBackupSnapshotAsync(probe, "s1", model1, because);
                    await AssertNotSelectableAsync(probe, "s2", because);
                    if (s3Exposed)
                        await AssertBackupSnapshotAsync(probe, "s3", model, because + ": S3 exposed only complete");
                    else
                        await AssertNotSelectableAsync(probe, "s3", because);
                }

                DfsBackupStatus st = await vol.Backups.GetStatusAsync(b0.TargetName, Ct);
                Assert.Equal(s1.Boundary, st.PublishedBoundary);
                cluster.Network.Heal();
            }

            // Finally the reseed completes and the old recovery point is still retained.
            await SyncExactAsync(vol, b0.TargetName, s3);
            DfsBackupStatus done = await vol.Backups.GetStatusAsync(b0.TargetName, Ct);
            Assert.False(done.ReseedRequired);
            Assert.NotEqual(oldLineage, done.LineageId);
            await AssertPublishedStateAsync(b0, "s3", ["s1", "s3"], ["s2"], "after the reseed finally completed");
            await AssertBackupStoreConsistentAsync(b0, "after repeated reseed failures");
            await using DfsClient reader = await ConnectBackupReaderAsync(cluster, 0, NextClientName("bk"));
            await AssertBackupSnapshotAsync(reader, "s1", model1, "old recovery point after the reseed");
            await AssertBackupSnapshotAsync(reader, "s3", model, "reseeded S3");
        });

    // ------------------------------------------------------------------------------------------------------------
    // BAK-025
    // ------------------------------------------------------------------------------------------------------------

    [Fact]
    [Catalog("BAK-025", "S", "INV-15", "INV-13", "INV-16")]
    public Task BAK_025_IndependentTargetsHaveIndependentLag() =>
        ClusterTest.RunAsync(Options(backupCount: 2), async cluster =>
        {
            TestBackupNode fast = cluster.BackupNodes[0];
            TestBackupNode slow = cluster.BackupNodes[1];
            await using DfsClient live = await ConnectLiveAsync(cluster);
            DfsVolume vol = live.GetVolume(Volume);
            var slowBudgets = new DfsBackupBudgets(MaxPinAge: TimeSpan.FromSeconds(5), MaxPinnedBytes: 256 * 1024, MaxConcurrentObjects: 2);
            await RegisterAsync(fast);
            await RegisterAsync(slow);
            TreeModel model = InitialTree(31);
            await WriteTreeAsync(vol, model);
            DfsSnapshotInfo s1 = await SnapshotAsync(vol, "s1");
            await SyncExactAsync(vol, fast.TargetName, s1);
            await SyncExactAsync(vol, slow.TargetName, s1);
            await vol.Backups.ConfigureAsync(slow.TargetName, ManualSchedule, slowBudgets, Ct);
            var models = new Dictionary<string, TreeModel>(StringComparer.Ordinal) { ["s1"] = model.Clone() };
            Guid fastLineage = (await vol.Backups.GetStatusAsync(fast.TargetName, Ct)).LineageId;
            Guid slowLineage = (await vol.Backups.GetStatusAsync(slow.TargetName, Ct)).LineageId;

            CutFromSources(cluster, slow.Name);
            DfsSnapshotInfo last = s1;
            for (int i = 2; i <= 4; i++)
            {
                await AssertLiveCommitUnblockedAsync(cluster, vol, model, $"/lag/f{i}.bin", TestData.Bytes(280 + (ulong)i, 300 * 1024),
                    $"live commit {i} with one backup target offline");
                last = await SnapshotAsync(vol, $"s{i}");
                models[last.Name] = model.Clone();
                await vol.Backups.RequestSyncAsync(slow.TargetName, last.Name, Ct);
                DfsSnapshotInfo current = last;
                TimeSpan fastSync = await TimeAsync(cluster, () => SyncExactAsync(vol, fast.TargetName, current));
                Assert.True(fastSync <= TimeSpan.FromSeconds(20), $"the fast target's barrier took {fastSync} while the other target is offline");
                DfsBackupNotReachedException nr = await Assert.ThrowsAsync<DfsBackupNotReachedException>(
                    () => vol.Backups.AwaitBackupSnapshotAsync(slow.TargetName, current.Name, TimeSpan.FromSeconds(1), Ct));
                Assert.Equal(slow.TargetName, nr.Target);
                Assert.Equal("s1", nr.LastPublished?.SnapshotName);
            }

            await cluster.DelayAsync(TimeSpan.FromSeconds(10), Ct);
            await cluster.WaitUntilAsync(async () => (await vol.Backups.GetStatusAsync(slow.TargetName, Ct)).ReseedRequired,
                SyncTimeout, "the offline target exceeds its own budget", Ct);
            DfsBackupStatus fastSt = await vol.Backups.GetStatusAsync(fast.TargetName, Ct);
            DfsBackupStatus slowSt = await vol.Backups.GetStatusAsync(slow.TargetName, Ct);
            Assert.False(fastSt.ReseedRequired, "the healthy target's lineage must be unaffected");
            Assert.Equal(fastLineage, fastSt.LineageId);
            Assert.Equal(last.Boundary, fastSt.PublishedBoundary);
            Assert.True(slowSt.ReseedRequired);
            Assert.Equal(s1.Boundary, slowSt.PublishedBoundary);
            Assert.True(slowSt.PinnedSourceBytes <= slowBudgets.MaxPinnedBytes, $"slow target pins {slowSt.PinnedSourceBytes} exceed its budget");
            await AssertPublishedStateAsync(fast, "s4", ["s1", "s2", "s3", "s4"], [], "fast target");
            await AssertPublishedStateAsync(slow, "s1", ["s1"], ["s2", "s3", "s4"], "slow target");

            // Retention is per destination.
            await SetBackupRetentionAsync(cluster, fast, "s2", new DfsRetention(null, Hold: true));
            await using (DfsClient fastReader = await ConnectBackupReaderAsync(cluster, 0, NextClientName("fast")))
                Assert.True((await fastReader.GetVolume(Volume).Snapshots.GetAsync("s2", Ct)).Retention.Hold);
            await using (DfsClient slowReader = await ConnectBackupReaderAsync(cluster, 1, NextClientName("slow")))
            {
                await AssertNotSelectableAsync(slowReader, "s2", "the slow target never received S2");
                Assert.False((await slowReader.GetVolume(Volume).Snapshots.GetAsync("s1", Ct)).Retention.Hold,
                    "a hold placed on one destination must not change another destination's retention");
            }

            // The slow target reconnects and reseeds on its own; the fast target is unaffected.
            cluster.Network.Heal();
            await SyncExactAsync(vol, slow.TargetName, last);
            DfsBackupStatus slowAfter = await vol.Backups.GetStatusAsync(slow.TargetName, Ct);
            Assert.False(slowAfter.ReseedRequired);
            Assert.NotEqual(slowLineage, slowAfter.LineageId);
            DfsBackupStatus fastAfter = await vol.Backups.GetStatusAsync(fast.TargetName, Ct);
            Assert.Equal(fastLineage, fastAfter.LineageId);
            Assert.False(fastAfter.ReseedRequired);

            await using DfsClient fastCheck = await ConnectBackupReaderAsync(cluster, 0, NextClientName("fast"));
            foreach (KeyValuePair<string, TreeModel> snapshot in models)
                await AssertBackupSnapshotAsync(fastCheck, snapshot.Key, snapshot.Value, $"fast target {snapshot.Key}");
            await using DfsClient slowCheck = await ConnectBackupReaderAsync(cluster, 1, NextClientName("slow"));
            await AssertBackupSnapshotAsync(slowCheck, "s1", models["s1"], "slow target S1");
            await AssertBackupSnapshotAsync(slowCheck, last.Name, models[last.Name], "slow target after reseed");
        });

    // ------------------------------------------------------------------------------------------------------------
    // BAK-026
    // ------------------------------------------------------------------------------------------------------------

    [Fact]
    [Catalog("BAK-026", "S", "INV-13", "INV-10")]
    public Task BAK_026_OldEpochBackupStreamRejected() =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            TestBackupNode b0 = cluster.BackupNodes[0];
            await using DfsClient live = await ConnectLiveAsync(cluster);
            DfsVolume vol = live.GetVolume(Volume);
            await RegisterAsync(b0);
            TreeModel model = InitialTree(32);
            await WriteTreeAsync(vol, model);
            (DfsSnapshotInfo s1, _) = await SnapshotAndSyncAsync(vol, b0.TargetName, "s1");
            TreeModel model1 = model.Clone();
            DfsVolumeInfo oldInfo = await vol.GetInfoAsync(Ct);
            Guid oldLineage = (await vol.Backups.GetStatusAsync(b0.TargetName, Ct)).LineageId;

            // Capture a complete old-epoch export of S2 whose publication never reaches the backup.
            await PutAsync(vol, model, "/old/x.bin", TestData.Bytes(290, 150 * 1024));
            await SnapshotAsync(vol, "s2");
            HashSet<ObjectId> oldOnlyObjects = await FileObjectsAsync(cluster, ["/old/x.bin"]);
            List<CapturedMessage> oldFrames = [];
            var transferFilter = new MessageFilter(To: b0.Name, Operation: DfsOperationKind.BackupTransfer, Direction: Transport.LinkDirection.Request);
            MessageHold transfer = cluster.Network.Hold(transferFilter);
            try
            {
                using MessageHold publish = cluster.Network.Hold(new MessageFilter(To: b0.Name, Operation: DfsOperationKind.BackupPublish, Direction: Transport.LinkDirection.Request));
                await vol.Backups.RequestSyncAsync(b0.TargetName, "s2", Ct);
                await cluster.WaitUntilAsync(() =>
                {
                    // ReleaseAll ends a hold: keep capturing the transfer stream with a fresh hold after each release.
                    if (transfer.Count > 0)
                    {
                        oldFrames.AddRange(transfer.Captured);
                        transfer.ReleaseAll();
                        transfer = cluster.Network.Hold(transferFilter);
                    }

                    return Task.FromResult(publish.Count > 0);
                }, SyncTimeout, "the old-epoch export reaches its publish request", Ct);
                CutFromSources(cluster, b0.Name);
                publish.DropAll();
                transfer.DropAll();
                oldFrames = [.. oldFrames, .. transfer.Captured, .. publish.Captured];
            }
            finally
            {
                transfer.Dispose();
            }

            Assert.Contains(oldFrames, f => f.Type == MessageType.BackupBegin);
            Assert.Contains(oldFrames, f => f.Type == MessageType.BackupPutManifest);
            Assert.Contains(oldFrames, f => f.Type == MessageType.BackupPublish);
            Assert.All(oldFrames, f => Assert.Equal(b0.Name, f.To));
            await AssertPublishedStateAsync(b0, "s1", ["s1"], ["s2"], "old-epoch S2 captured, publication withheld");

            // Promotion: the old authority is retired and S1 is restored from the backup as a new authority, same name.
            await vol.Backups.RemoveTargetAsync(b0.TargetName, Ct);
            await live.Admin.DeleteVolumeAsync(Volume, Ct);
            cluster.Network.Heal();
            DfsVolumeInfo restored = await live.Admin.RestoreFromBackupAsync(
                new DfsRestoreFromBackupRequest([b0.Endpoint], b0.ReaderCredentials, Volume, "s1", Volume), Ct);
            Assert.Equal(Volume, restored.Name);
            Assert.True(restored.Id != oldInfo.Id || restored.Epoch > oldInfo.Epoch, "a promoted authority needs a new volume identity or epoch");
            Assert.False(restored.LastCommit.IsComparableWith(s1.Boundary), "old-authority tokens must not be ordered against the new lineage");

            await using DfsClient fresh = await ConnectLiveAsync(cluster, NextClientName("c"));
            DfsVolume newVol = fresh.GetVolume(Volume);
            TreeModel newModel = model1.Clone();
            await AssertFileBytesAsync(newVol.Files, "/docs/a.bin", model1.Files["/docs/a.bin"], "restored authority content");
            await b0.RegisterAsync(Volume, ManualSchedule, GenerousBudgets);
            await PutAsync(newVol, newModel, "/new/y.bin", TestData.Bytes(291, 140 * 1024));
            (DfsSnapshotInfo r1, _) = await SnapshotAndSyncAsync(newVol, b0.TargetName, "r1");
            Assert.Equal(restored.Id, r1.Boundary.VolumeId);
            Guid newLineage = (await newVol.Backups.GetStatusAsync(b0.TargetName, Ct)).LineageId;
            Assert.NotEqual(oldLineage, newLineage);
            IReadOnlyList<string> publishedBefore = await b0.Inspect.GetPublishedSnapshotsAsync(Volume);
            Assert.Equal("r1", await b0.Inspect.GetPublishedPointerAsync(Volume));

            // Deliver the captured old-epoch export stream again, from its original sender.
            foreach (CapturedMessage frame in oldFrames)
            {
                DfsErrorCode code = await cluster.Network.ReplayAsync(frame);
                if (frame.Type is MessageType.BackupBegin or MessageType.BackupPutManifest or MessageType.BackupPublish)
                    Assert.True(code != DfsErrorCode.None, $"old-epoch {frame.Type} must be rejected, got {code}");
            }

            Assert.Equal(publishedBefore.Order(StringComparer.Ordinal), (await b0.Inspect.GetPublishedSnapshotsAsync(Volume)).Order(StringComparer.Ordinal));
            Assert.Equal("r1", await b0.Inspect.GetPublishedPointerAsync(Volume));
            IReadOnlyList<BackupSnapshotInspection> snaps = await b0.Inspect.GetSnapshotsAsync(Volume);
            Assert.DoesNotContain(snaps, s => s.Name == "s2");
            BackupSnapshotInspection r1b = snaps.Single(s => s.Name == "r1");
            BackupSnapshotInspection s1b = snaps.Single(s => s.Name == "s1");
            Assert.Equal(newLineage, r1b.Lineage);
            Assert.Equal(r1.Boundary, r1b.CoveredCommit);
            Assert.Equal(oldLineage, s1b.Lineage);
            Assert.False(s1b.CoveredCommit.IsComparableWith(r1b.CoveredCommit), "old- and new-epoch boundaries must not be comparable");
            Assert.False(r1b.Closure.Any(oldOnlyObjects.Contains), "the new lineage must not reference data from the rejected old-epoch export");
            await AssertBackupStoreConsistentAsync(b0, "after the old-epoch replay");

            await using (DfsClient reader = await ConnectBackupReaderAsync(cluster, 0, NextClientName("bk")))
            {
                await AssertBackupSnapshotAsync(reader, "r1", newModel, "new-lineage R1 after the old-epoch replay");
                await AssertBackupSnapshotAsync(reader, "s1", model1, "old-lineage S1 after the old-epoch replay");
                await AssertNotSelectableAsync(reader, "s2", "old-epoch S2 must never be published");
            }

            // The new authority keeps operating and backing up.
            await PutAsync(newVol, newModel, "/new/z.bin", TestData.Bytes(292, 70 * 1024));
            await SnapshotAndSyncAsync(newVol, b0.TargetName, "r2");
            await using DfsClient finalReader = await ConnectBackupReaderAsync(cluster, 0, NextClientName("bk"));
            await AssertBackupSnapshotAsync(finalReader, "r2", newModel, "new-lineage R2");
        });

    // ------------------------------------------------------------------------------------------------------------
    // BAK-027
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<int> ExpiryOffsetsMs => new() { -1000, -100, 0, 100, 1000 };

    [Theory]
    [MemberData(nameof(ExpiryOffsetsMs))]
    [Catalog("BAK-027", "S", "INV-09", "INV-10", "INV-13")]
    public Task BAK_027_BackupPinVersusRetentionRace(int openOffsetMs) =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            TestBackupNode b0 = cluster.BackupNodes[0];
            await using DfsClient live = await ConnectLiveAsync(cluster);
            DfsVolume vol = live.GetVolume(Volume);
            await RegisterAsync(b0);
            await b0.RestartAsync(o => o with { KeepLastSnapshots = 1 });
            var model = new TreeModel();
            model.Put("/r/u.bin", TestData.Bytes(300, 200 * 1024));
            model.Put("/r/shared.bin", TestData.Bytes(301, 100 * 1024));
            await WriteTreeAsync(vol, model);
            await SnapshotAndSyncAsync(vol, b0.TargetName, "s1");
            TreeModel model1 = model.Clone();
            HashSet<ObjectId> s1Unique = await FileObjectsAsync(cluster, ["/r/u.bin"]);

            // Protect S1 with a finite retention before S2 publishes (KeepLastSnapshots = 1 would otherwise not retain
            // it). The expiry is set once, extending the default; the race below happens at exactly this instant.
            DateTimeOffset expiry = b0.Clock.GetUtcNow() + TimeSpan.FromSeconds(30);
            await SetBackupRetentionAsync(cluster, b0, "s1", new DfsRetention(expiry, Hold: false));
            await PutAsync(vol, model, "/r/u.bin", TestData.Bytes(302, 200 * 1024));
            await SnapshotAndSyncAsync(vol, b0.TargetName, "s2");
            TreeModel model2 = model.Clone();
            Assert.Contains("s1", await b0.Inspect.GetPublishedSnapshotsAsync(Volume));

            // S1 is now protected only by a retention that is about to expire.
            await using DfsClient racer = await ConnectBackupReaderAsync(cluster, 0, NextClientName("racer"));
            TimeSpan remaining = expiry - b0.Clock.GetUtcNow();
            Assert.True(remaining >= TimeSpan.FromSeconds(2), $"setup must finish well before S1's retention expires (remaining {remaining})");
            TimeSpan wait = remaining + TimeSpan.FromMilliseconds(openOffsetMs);
            if (wait > TimeSpan.Zero)
                await cluster.DelayAsync(wait, Ct);
            string because = $"open {openOffsetMs} ms relative to S1 retention expiry (backup clock)";

            DfsSnapshotView? view = null;
            DfsException? refused = null;
            try
            {
                view = await racer.GetVolume(Volume).Snapshots.OpenAsync("s1", Ct);
            }
            catch (DfsException ex) when (ex.ErrorCode is DfsErrorCode.SnapshotNotFound or DfsErrorCode.PinExpired)
            {
                // A clean refusal is legal once the retention may have expired (asserted below).
                refused = ex;
            }

            if (view is null)
            {
                // Lost the race cleanly: no pin was granted and nothing partial was served.
                Assert.True(openOffsetMs >= 0, $"{because}: a snapshot whose retention has not yet expired must be openable (got {refused?.ErrorCode})");
                if (refused?.ErrorCode == DfsErrorCode.SnapshotNotFound)
                {
                    await AssertNotSelectableAsync(racer, "s1", because);
                    Assert.DoesNotContain("s1", await b0.Inspect.GetPublishedSnapshotsAsync(Volume));
                }
            }
            else
            {
                // Won the race: a valid local pin; nothing it needs may be reclaimed while it is held.
                await using (view)
                {
                    Assert.Equal("s1", view.Info.Name);
                    for (int round = 0; round < 4; round++)
                    {
                        await AssertViewMatchesAsync(view, model1, $"{because}: pinned reader, round {round}");
                        Assert.Subset(await BackupObjectsAsync(b0), s1Unique);
                        BackupSnapshotInspection? pinned = (await b0.Inspect.GetSnapshotsAsync(Volume)).SingleOrDefault(s => s.Name == "s1");
                        Assert.True(pinned is not null && pinned.ReaderPins >= 1, $"{because}: S1 must stay retained and pinned while read (round {round})");
                        Assert.Empty(await b0.Inspect.VerifyClosureAsync(Volume, "s1"));
                        await cluster.DelayAsync(TimeSpan.FromSeconds(3), Ct);
                    }
                }
            }

            // After the reader released its pin, S1 is either still complete or gone as a unit; S2 is untouched.
            await cluster.DelayAsync(TimeSpan.FromSeconds(5), Ct);
            await using DfsClient probe = await ConnectBackupReaderAsync(cluster, 0, NextClientName("probe"));
            if ((await b0.Inspect.GetPublishedSnapshotsAsync(Volume)).Contains("s1"))
            {
                Assert.Empty(await b0.Inspect.VerifyClosureAsync(Volume, "s1"));
                await AssertBackupSnapshotAsync(probe, "s1", model1, because + ", still retained");
            }
            else
            {
                await AssertNotSelectableAsync(probe, "s1", because + ", reclaimed");
            }

            await AssertPublishedStateAsync(b0, "s2", ["s2"], [], because);
            await AssertBackupSnapshotAsync(probe, "s2", model2, because + ": S2 untouched");
            await AssertBackupStoreConsistentAsync(b0, because);
        });

    // ------------------------------------------------------------------------------------------------------------
    // BAK-028
    // ------------------------------------------------------------------------------------------------------------

    public enum Throttle
    {
        NetworkBandwidth,
        BudgetRate,
    }

    [Theory]
    [InlineData(Throttle.NetworkBandwidth)]
    [InlineData(Throttle.BudgetRate)]
    [Catalog("BAK-028", "S", "INV-15", "INV-16", "INV-17")]
    public Task BAK_028_SlowBackupBoundedResourceUse(Throttle throttle) =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            TestBackupNode b0 = cluster.BackupNodes[0];
            await using DfsClient live = await ConnectLiveAsync(cluster);
            DfsVolume vol = live.GetVolume(Volume);
            const int maxConcurrent = 2;
            const long rate = 32 * 1024;
            const long maxObjectSize = 1L << 20; // NodeTimings.Fast
            var budgets = new DfsBackupBudgets(TimeSpan.FromHours(1), 64L << 20,
                throttle == Throttle.BudgetRate ? rate : long.MaxValue, maxConcurrent);
            await RegisterAsync(b0, budgets);
            if (throttle == Throttle.NetworkBandwidth)
            {
                foreach (TestNode node in cluster.Nodes)
                    cluster.Network.SetBandwidth(node.Name, b0.Name, rate);
            }

            var model = new TreeModel();
            for (int i = 0; i < 4; i++)
                model.Put($"/bulk/f{i}.bin", TestData.Bytes(310 + (ulong)i, 512 * 1024));
            await WriteTreeAsync(vol, model);
            await vol.Directories.CreateDirectoryAsync("/unrelated", Ct);
            model.AddDirectory("/unrelated");
            DfsSnapshotInfo s1 = await SnapshotAsync(vol, "s1");
            TestNode leader = await cluster.GetLeaderAsync(Volume, Ct);
            long leaderStagedBaseline = await leader.Inspect.GetStagedBytesAsync();
            NodeCounters before = await cluster.Inspect.GetTotalCountersAsync();
            await vol.Backups.RequestSyncAsync(b0.TargetName, "s1", Ct);

            TimeSpan window = TimeSpan.FromSeconds(20);
            long start = cluster.Time.GetTimestamp();
            int liveCommits = 0;
            while (cluster.Time.GetElapsedTime(start) < window)
            {
                DfsBackupStatus st = await vol.Backups.GetStatusAsync(b0.TargetName, Ct);
                Assert.True(st.PinnedSourceBytes <= budgets.MaxPinnedBytes, $"pinned source bytes {st.PinnedSourceBytes} exceed the budget");
                Assert.True(st.PublishedBoundary.IsNone, "2 MiB cannot have been published at 32 KiB/s within the window");
                long backupStaged = await b0.Inspect.GetStagedBytesAsync();
                Assert.True(backupStaged <= (maxConcurrent + 1) * maxObjectSize, $"backup staging {backupStaged} B exceeds the in-flight window");
                BackupReceiveInspection? recv = await b0.Inspect.GetReceiveStateAsync(Volume);
                if (recv is not null)
                {
                    int inFlight = recv.ReceivedObjects.Count - recv.DurableObjects.Count;
                    Assert.True(inFlight <= maxConcurrent, $"{inFlight} objects in flight exceed MaxConcurrentObjects {maxConcurrent}");
                }

                long leaderStaged = await leader.Inspect.GetStagedBytesAsync();
                Assert.True(leaderStaged <= leaderStagedBaseline + maxObjectSize, $"source staging grew to {leaderStaged} B because of the backup");
                await AssertLiveCommitUnblockedAsync(cluster, vol, null, $"/unrelated/u{liveCommits}.bin", TestData.Bytes(400 + (ulong)liveCommits, 16 * 1024),
                    "unrelated live commit during a throttled backup");
                liveCommits++;
                await cluster.DelayAsync(TimeSpan.FromSeconds(1), Ct);
            }

            Assert.True(liveCommits >= 10, $"only {liveCommits} live commits completed during the throttled transfer");
            NodeCounters after = await cluster.Inspect.GetTotalCountersAsync();
            long sent = after.BackupBytesSent - before.BackupBytesSent;
            double seconds = cluster.Time.GetElapsedTime(start).TotalSeconds;
            TestContext.Current.TestOutputHelper?.WriteLine($"{throttle}: sent {sent} B in {seconds:F1} s, {liveCommits} live commits");
            Assert.True(sent > 0, "the throttled transfer must still make progress");
            Assert.True(sent <= (long)(rate * seconds * 1.25) + (maxConcurrent * maxObjectSize),
                $"backup traffic {sent} B in {seconds:F1} s exceeds the {rate} B/s limit");
            Assert.True((await vol.Backups.GetStatusAsync(b0.TargetName, Ct)).ReceivedBytes > 0);

            // Lift the throttle: the transfer completes.
            if (throttle == Throttle.NetworkBandwidth)
            {
                foreach (TestNode node in cluster.Nodes)
                    cluster.Network.SetBandwidth(node.Name, b0.Name, long.MaxValue);
            }
            else
            {
                await vol.Backups.ConfigureAsync(b0.TargetName, ManualSchedule, budgets with { MaxBytesPerSecond = long.MaxValue }, Ct);
            }

            DfsBackupCheckpoint cp = await vol.Backups.AwaitBackupSnapshotAsync(b0.TargetName, "s1", TimeSpan.FromMinutes(3), Ct);
            AssertExactCheckpoint(cp, b0.TargetName, s1);
            await using DfsClient reader = await ConnectBackupReaderAsync(cluster);
            await AssertBackupSnapshotAsync(reader, "s1", model, $"S1 after the {throttle} throttle was lifted");
        });

    // ------------------------------------------------------------------------------------------------------------
    // BAK-029
    // ------------------------------------------------------------------------------------------------------------

    public enum AccessLoss
    {
        PartitionedLong,
        TargetRemoved,
        SourceGone,
    }

    [Theory]
    [InlineData(AccessLoss.PartitionedLong)]
    [InlineData(AccessLoss.TargetRemoved)]
    [InlineData(AccessLoss.SourceGone)]
    [Catalog("BAK-029", "S", "INV-15", "INV-12", "INV-13")]
    public Task BAK_029_BackupCredentialExpiryIsNotPromotion(AccessLoss loss) =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            TestBackupNode b0 = cluster.BackupNodes[0];
            await using DfsClient live = await ConnectLiveAsync(cluster);
            DfsVolume vol = live.GetVolume(Volume);
            await RegisterAsync(b0);
            TreeModel model = InitialTree(33);
            await WriteTreeAsync(vol, model);
            await SnapshotAndSyncAsync(vol, b0.TargetName, "s1");

            switch (loss)
            {
                case AccessLoss.PartitionedLong:
                    CutFromSources(cluster, b0.Name);
                    break;
                case AccessLoss.TargetRemoved:
                    await vol.Backups.RemoveTargetAsync(b0.TargetName, Ct);
                    break;
                case AccessLoss.SourceGone:
                    foreach (TestNode node in cluster.Nodes)
                        await node.StopAsync();
                    break;
            }

            // Far beyond every lease, session, election, pin budget and credential lifetime.
            await cluster.DelayAsync(TimeSpan.FromMinutes(5), Ct);
            string because = $"backup after {loss}";

            await using DfsClient reader = await ConnectBackupReaderAsync(cluster, 0, NextClientName("bk"));
            await AssertBackupSnapshotAsync(reader, "s1", model, because + ": historical read under local authorization");
            DfsVolume bvol = reader.GetVolume(Volume);
            Assert.True(bvol.Files.IsReadOnly && bvol.Directories.IsReadOnly && bvol.Snapshots.IsReadOnly, because + ": facades stay read-only");
            await AssertEx.ThrowsDfsAsync<DfsReadOnlyException>(() => bvol.Files.WriteAllBytesAsync("/promoted.bin", TestData.Bytes(320, 100), Ct), DfsErrorCode.ReadOnly);
            await AssertEx.ThrowsDfsAsync<DfsReadOnlyException>(async () =>
            {
                await using DfsStream s = await bvol.Files.OpenAsync("/docs/a.bin", FileMode.Open, FileAccess.ReadWrite, FileShare.None, Ct);
            }, DfsErrorCode.ReadOnly);
            await AssertEx.ThrowsDfsAsync<DfsReadOnlyException>(async () =>
            {
                await using DfsTransaction tx = await bvol.Transactions.BeginAsync(null, Ct);
            }, DfsErrorCode.ReadOnly);
            await AssertEx.ThrowsDfsAsync<DfsReadOnlyException>(() => bvol.Snapshots.CreateAsync("p1", null, Ct), DfsErrorCode.ReadOnly);
            await AssertEx.ThrowsDfsAsync<DfsReadOnlyException>(() => bvol.Directories.CreateDirectoryAsync("/p", Ct), DfsErrorCode.ReadOnly);
            await using (RawProtocolClient raw = await cluster.ConnectRawBackupAsync(0, Ct))
            {
                Assert.Equal(DfsErrorCode.None, await raw.AuthenticateAsync(b0.ReaderCredentials));
                Assert.Equal(DfsErrorCode.ReadOnly, await raw.SendPathOperationAsync(Volume, DfsOperationKind.CreateDirectory, "/raw-promoted"));
            }

            // Local authorization: unknown principals and wrong secrets are rejected without consulting the source.
            await AssertRefusedAsync(async () =>
            {
                await using DfsClient c = await ConnectBackupAsync(cluster, cluster.AdminCredentials, 0, NextClientName("src"));
                await c.GetVolume(Volume).Snapshots.ListAsync(Ct);
            }, ex => ex is UnauthorizedAccessException, because + ": source principals are not backup principals");
            await AssertRefusedAsync(async () =>
            {
                await using DfsClient c = await ConnectBackupAsync(cluster, new DfsCredentials(b0.ReaderCredentials.Principal, "wrong-secret"), 0, NextClientName("bad"));
                await c.GetVolume(Volume).Snapshots.ListAsync(Ct);
            }, ex => ex is DfsAuthenticationException, because + ": a wrong secret is rejected");

            await AssertPublishedStateAsync(b0, "s1", ["s1"], [], because);
            if (loss == AccessLoss.SourceGone)
            {
                foreach (TestNode node in cluster.Nodes)
                    await node.RestartAsync();
                await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), Ct);
            }

            // Not promoted: still outside every live group, and the live cluster keeps its own authority.
            cluster.Network.Heal();
            await AssertOutsideConsensusAsync(cluster, live, because);
            await AssertLiveCommitUnblockedAsync(cluster, vol, null, "/live-after.bin", TestData.Bytes(321, 30 * 1024), because);
            await AssertBackupSnapshotAsync(reader, "s1", model, because + ": unchanged after the source returned");
        });

    // ------------------------------------------------------------------------------------------------------------
    // BAK-030
    // ------------------------------------------------------------------------------------------------------------

    [Fact]
    [Catalog("BAK-030", "S", "INV-10", "INV-09", "INV-04")]
    public Task BAK_030_ExportNotRawUncommittedBlocks() =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            TestBackupNode b0 = cluster.BackupNodes[0];
            await using DfsClient live = await ConnectLiveAsync(cluster);
            DfsVolume vol = live.GetVolume(Volume);
            await RegisterAsync(b0);
            TreeModel model = InitialTree(34);
            await WriteTreeAsync(vol, model);
            await SnapshotAndSyncAsync(vol, b0.TargetName, "s1");
            HashSet<ObjectId> before = (await cluster.Inspect.GetAllStoredObjectsAsync(Volume)).ToHashSet();

            // An active transaction stages data and namespace changes on live storage.
            DfsTransaction tx = await vol.Transactions.BeginAsync(null, Ct);
            Assert.True((await tx.Files.WriteAllBytesAsync("/docs/a.bin", TestData.Bytes(330, 200 * 1024), Ct)).IsNone,
                "transaction-bound helpers return CommitToken.None");
            await tx.Directories.CreateDirectoryAsync("/txdir", Ct);
            await tx.Files.WriteAllBytesAsync("/txdir/new.bin", TestData.Bytes(331, 150 * 1024), Ct);
            await using (DfsStream txStream = await tx.Files.CreateAsync("/txdir/stream.bin", Ct))
            {
                await txStream.WriteAsync(TestData.Bytes(332, 130 * 1024), Ct);
                await txStream.FlushAsync(Ct);
            }

            // An uncommitted standalone writer auto-stages chunks beyond its buffer.
            DfsStream pending = await vol.Files.OpenAsync("/docs/b.bin", FileMode.Open, FileAccess.ReadWrite, FileShare.Read, Ct);
            await pending.WriteAsync(TestData.Bytes(333, 300 * 1024), Ct);
            Assert.True(pending.PendingBytes > 0);

            var staged = (await cluster.Inspect.GetAllStoredObjectsAsync(Volume)).Except(before).ToHashSet();
            Assert.True(staged.Count > 0, "the uncommitted work must have uploaded objects to live storage");
            VolumeStateInspection active = await cluster.Inspect.GetVolumeStateAsync(Volume);
            Assert.True(active.ActiveTransactions >= 1);
            Assert.Contains(active.Staging, s => s.Open);

            // Snapshot and export while the work is staged, then abort everything and export again.
            await SnapshotAndSyncAsync(vol, b0.TargetName, "s2");
            await tx.AbortAsync(Ct);
            Assert.Equal(DfsTransactionState.Aborted, tx.State);
            await tx.DisposeAsync();
            await pending.AbortAsync(Ct);
            await pending.DisposeAsync();
            await SnapshotAndSyncAsync(vol, b0.TargetName, "s3");

            await using DfsClient reader = await ConnectBackupReaderAsync(cluster);
            foreach (string name in new[] { "s1", "s2", "s3" })
                await AssertBackupSnapshotAsync(reader, name, model, $"{name} contains no uncommitted namespace or data change");
            HashSet<ObjectId> backupObjects = await BackupObjectsAsync(b0);
            var leaked = backupObjects.Intersect(staged).ToList();
            Assert.True(leaked.Count == 0, $"staged, never-committed objects were exported: [{string.Join(", ", leaked)}]");
            foreach (BackupSnapshotInspection snapshot in await b0.Inspect.GetSnapshotsAsync(Volume))
                Assert.False(snapshot.Closure.Any(staged.Contains), $"{snapshot.Name} references uncommitted staged objects");

            // The live view agrees: the aborted changes never became visible.
            Assert.False(await vol.Directories.ExistsAsync("/txdir", Ct));
            await AssertFileBytesAsync(vol.Files, "/docs/a.bin", model.Files["/docs/a.bin"], "live file after the aborted transaction");
            await AssertFileBytesAsync(vol.Files, "/docs/b.bin", model.Files["/docs/b.bin"], "live file after the aborted stream");
        });
}
