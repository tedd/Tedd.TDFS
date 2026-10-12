using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Testing.SnpApp;

namespace Tedd.Tdfs.Tests.Simulation.App;

/// <summary>APP simulation tests (test plan §7.20, FSD §11.3, §12.3): application pauses are independent of backup transfer.</summary>
public sealed class AppSimulationTests
{
    private const string Volume = "vol";

    /// <summary>
    /// APP-003: with the late-sync target unreachable (remote backup delayed indefinitely) an application-consistent
    /// snapshot completes and the application is resumed right after source capture; its writes then continue with
    /// bounded latency while the backup barrier times out without rolling anything back; remote transfer starts only
    /// after the resume, and once connectivity returns the backup publishes the application-consistent snapshot.
    /// </summary>
    [Fact]
    [Catalog("APP-003", "S", "INV-04", "INV-10", "INV-13", "INV-17", "INV-19")]
    public async Task APP_003_NoPauseDuringBackupTransfer()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        TestClusterOptions options = TestClusterOptions.ThreeZones with { Mode = ClusterMode.Deterministic, BackupNodeCount = 1 };
        await ClusterTest.RunAsync(options, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(null, ct);
            DfsVolume vol = client.GetVolume(Volume);
            TestBackupNode backup = cluster.BackupNodes[0];
            await backup.RegisterAsync(Volume, new DfsBackupSchedule(OnEverySnapshot: true));

            var app = new LogSampleApp(vol);
            await app.InitializeAsync(ct);
            for (int i = 0; i < 3; i++)
                await app.UpdateAsync($"k{i}", TestData.Bytes(0xA003_0000UL + (ulong)i, 200), ct);

            // Delay the remote backup indefinitely.
            cluster.Network.Isolate(backup.Name);

            var timeline = new AdapterTimeline(cluster.Time);
            FaultArm transferProbe = cluster.Faults.Arm(new FaultMatch("backup.*"), FaultAction.Custom(context =>
            {
                timeline.Record("server", "transfer.first", context.Site);
                return Task.CompletedTask;
            }));

            var latencies = new List<TimeSpan>();
            bool resumed = false;
            bool stop = false;
            async Task WriterAsync()
            {
                int n = 100;
                while (!stop)
                {
                    long start = cluster.Time.GetTimestamp();
                    bool afterResume = resumed;
                    await app.UpdateAsync($"k{n % 7}", TestData.Bytes(0xA003_1000UL + (ulong)n, 150), CancellationToken.None);
                    if (afterResume)
                        latencies.Add(cluster.Time.GetElapsedTime(start));
                    n++;
                    await cluster.DelayAsync(TimeSpan.FromMilliseconds(5), CancellationToken.None);
                }
            }

            Task writer = WriterAsync();
            await cluster.DelayAsync(TimeSpan.FromMilliseconds(50), ct);

            DfsSnapshotInfo info = await vol.Snapshots.CreateAsync("app-snap", new DfsSnapshotOptions
            {
                Adapters = [app.CreateAdapter("log-app", timeline)],
                AdapterTimeout = TimeSpan.FromSeconds(5),
            }, ct);
            resumed = true;
            Assert.Equal(DfsConsistencyLabel.ApplicationConsistent, info.Label);
            Assert.Null(info.AdapterResumeFailure);
            Assert.False(app.IsPaused, "the application must be resumed when CreateAsync returns, without waiting for the backup");
            TimelineEvent resumeDone = timeline.Get("log-app", "resume.done");

            long seqAtResume = app.LastSequence;
            await cluster.WaitUntilAsync(() => Task.FromResult(app.LastSequence >= seqAtResume + 10), TimeSpan.FromSeconds(20),
                "application writes must resume and continue while the remote backup is delayed", ct);

            DfsBackupStatus status = await vol.Backups.GetStatusAsync(backup.TargetName, ct);
            Assert.True(status.LatestPublished is null || status.LatestPublished.SnapshotName != "app-snap",
                "the unreachable target cannot have published the snapshot");
            DfsBackupNotReachedException notReached = await Assert.ThrowsAsync<DfsBackupNotReachedException>(
                () => vol.Backups.AwaitBackupSnapshotAsync(backup.TargetName, "app-snap", TimeSpan.FromSeconds(2), ct));
            Assert.Equal(backup.TargetName, notReached.Target);

            stop = true;
            await writer;
            Assert.True(latencies.Count >= 10, $"writes after resume were measured ({latencies.Count})");
            TimeSpan worst = latencies.Max();
            Assert.True(worst < TimeSpan.FromSeconds(1), $"post-resume application writes must not wait for the backup (worst {worst})");

            // The barrier timeout rolled nothing back: the live state is every committed update.
            LogRecoveryResult live = await LogSampleApp.RecoverAsync(vol.Files, app.Root, false, ct);
            LogSampleApp.AssertStatesEqual(app.CommittedState, live.State, "live application state after the backup barrier timed out");

            // Remote transfer can only start after the application resumed; once connectivity returns it completes.
            if (timeline.Find("server", "transfer.first") is { } early)
                timeline.AssertBefore(resumeDone, early, "remote transfer starts only after the application resumed");
            cluster.Network.Heal();
            DfsBackupCheckpoint checkpoint = await vol.Backups.AwaitBackupSnapshotAsync(backup.TargetName, "app-snap", TimeSpan.FromSeconds(60), ct);
            Assert.Equal(info.Id, checkpoint.SnapshotId);
            Assert.True(checkpoint.IsExactSnapshot);
            cluster.Faults.AssertReached(transferProbe);
            timeline.AssertBefore(resumeDone, timeline.Get("server", "transfer.first"), "remote transfer occurs after the application resumes");

            await using DfsClient backupClient = await cluster.ConnectBackupAsync(0, new TestClientOptions { Name = "breader", Credentials = backup.ReaderCredentials }, ct);
            await using DfsSnapshotView view = await backupClient.GetVolume(Volume).Snapshots.OpenAsync("app-snap", ct);
            Assert.Equal(DfsConsistencyLabel.ApplicationConsistent, view.Info.Label);
            LogRecoveryResult image = await LogSampleApp.RecoverAsync(view.Files, app.Root, false, ct);
            Assert.Equal(0, image.ReplayedRecords);
            Assert.False(image.TornTail);
            LogSampleApp.AssertStatesEqual(LogSampleApp.ExpectedStateAt(app.Records, info.Boundary), image.State,
                "the backed-up application-consistent snapshot needs no log replay");
        }, ct);
    }
}
