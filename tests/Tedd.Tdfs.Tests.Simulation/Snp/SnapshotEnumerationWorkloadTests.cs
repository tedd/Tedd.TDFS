using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Simulation.Snp;

public sealed class SnapshotEnumerationWorkloadTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public Task SnapshotRemainsReadableAfterDisposingConcurrentEnumerators(bool backup, bool stopEarly)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        return ClusterTest.RunAsync(TestClusterOptions.ThreeZones with
        {
            Mode = ClusterMode.Deterministic,
            BackupNodeCount = backup ? 1 : 0,
        }, async cluster =>
        {
            await using DfsClient source = await cluster.ConnectAsync(new TestClientOptions { Name = "source" }, ct);
            DfsVolume volume = source.GetVolume("vol");
            if (backup)
                await cluster.BackupNodes[0].RegisterAsync("vol", new DfsBackupSchedule(null, false), new DfsBackupBudgets(TimeSpan.FromHours(1), 1L << 30));

            string[] names = ["a", "b", "c", "d"];
            byte[] original = TestData.Bytes(1701, 65537);
            foreach (string name in names)
                await volume.Files.WriteAllBytesAsync("/" + name, original, ct);
            DfsSnapshotInfo snapshot = await volume.Snapshots.CreateAsync("before", ct: ct);
            await volume.Files.DeleteAsync("/d", ct);
            if (backup)
            {
                string target = cluster.BackupNodes[0].TargetName;
                await volume.Backups.RequestSyncAsync(target, snapshot.Name, ct);
                DfsBackupCheckpoint checkpoint = await volume.Backups.AwaitBackupSnapshotAsync(target, snapshot.Name, TimeSpan.FromSeconds(60), ct);
                Assert.Equal(snapshot.Boundary, checkpoint.CoveredCommit);
            }
            await using DfsClient reader = backup
                ? await cluster.ConnectBackupAsync(0, new TestClientOptions { Name = "reader", Credentials = cluster.BackupNodes[0].ReaderCredentials }, ct)
                : await cluster.ConnectAsync(new TestClientOptions { Name = "reader" }, ct);
            await using DfsSnapshotView view = await reader.GetVolume("vol").Snapshots.OpenAsync(snapshot.Name, ct);
            var options = new DfsEnumerationOptions { PageSize = 1 };
            await using var outer = view.Directories.EnumerateAsync("/", options, ct).GetAsyncEnumerator(ct);
            Assert.True(await outer.MoveNextAsync());
            Assert.Equal(names[0], outer.Current.Name);

            var inner = new List<string>();
            await foreach (DfsEntryInfo entry in view.Directories.EnumerateAsync("/", options, ct))
            {
                inner.Add(entry.Name);
                if (stopEarly)
                    break;
            }
            Assert.Equal(stopEarly ? names[..1] : names, inner);
            var remaining = new List<string>();
            while (await outer.MoveNextAsync())
                remaining.Add(outer.Current.Name);
            Assert.Equal(names[1..], remaining);
            byte[] retained = await view.Files.ReadAllBytesAsync("/d", ct);
            AssertEx.BytesEqual(original, retained, "file removed from the live directory");
        }, ct);
    }
}
