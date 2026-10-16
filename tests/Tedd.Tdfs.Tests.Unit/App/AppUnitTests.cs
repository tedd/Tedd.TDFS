using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Unit.App;

/// <summary>APP unit-level contract tests (test plan §7.20, FSD §11.3).</summary>
public sealed class AppUnitTests
{
    private const string Volume = "vol";

    public enum CreationPath
    {
        CreateAsyncNullOptions,
        CreateAsyncDefaultOptions,
        CreateAsyncEmptyAdapterList,
        CreateAsyncWithMinimumCommit,
        CreateAsyncWithRetention,
        CreateSync,
        WriteAllBytesCommitSnapshot,
        StreamCommitSnapshot,
        TransactionCommitWithSnapshot,
    }

    /// <summary>
    /// APP-001: every snapshot created without adapters — through every creation path, including commit-and-snapshot —
    /// is labelled FilesystemConsistent with no adapter records and no resume failure, on the client view, in list/get
    /// results, in an opened view and in every replica's applied state; it is never automatically labelled
    /// ApplicationConsistent.
    /// </summary>
    [Theory]
    [InlineData(CreationPath.CreateAsyncNullOptions)]
    [InlineData(CreationPath.CreateAsyncDefaultOptions)]
    [InlineData(CreationPath.CreateAsyncEmptyAdapterList)]
    [InlineData(CreationPath.CreateAsyncWithMinimumCommit)]
    [InlineData(CreationPath.CreateAsyncWithRetention)]
    [InlineData(CreationPath.CreateSync)]
    [InlineData(CreationPath.WriteAllBytesCommitSnapshot)]
    [InlineData(CreationPath.StreamCommitSnapshot)]
    [InlineData(CreationPath.TransactionCommitWithSnapshot)]
    [Catalog("APP-001", "U", "INV-04", "INV-10", "INV-13", "INV-17", "INV-19")]
    public async Task APP_001_DefaultLabelIsFilesystemConsistent(CreationPath path)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(TestClusterOptions.ThreeZones, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(null, ct);
            DfsVolume vol = client.GetVolume(Volume);
            byte[] content = TestData.Bytes(0xA_001_001, 5000);
            await vol.Directories.CreateDirectoryAsync("/app", ct);
            CommitToken before = await vol.Files.WriteAllBytesAsync("/app/state", content, ct);
            const string name = "fs-default";

            DfsSnapshotInfo created;
            switch (path)
            {
                case CreationPath.CreateAsyncNullOptions:
                    created = await vol.Snapshots.CreateAsync(name, null, ct);
                    break;
                case CreationPath.CreateAsyncDefaultOptions:
                    created = await vol.Snapshots.CreateAsync(name, new DfsSnapshotOptions(), ct);
                    break;
                case CreationPath.CreateAsyncEmptyAdapterList:
                    created = await vol.Snapshots.CreateAsync(name, new DfsSnapshotOptions { Adapters = Array.Empty<IDfsSnapshotAdapter>() }, ct);
                    break;
                case CreationPath.CreateAsyncWithMinimumCommit:
                    created = await vol.Snapshots.CreateAsync(name, new DfsSnapshotOptions { MinimumCommit = before }, ct);
                    Assert.True(created.Boundary.IsAtOrAfter(before));
                    break;
                case CreationPath.CreateAsyncWithRetention:
                    {
                        var retention = new DfsRetention(cluster.Time.GetUtcNow().AddDays(1), Hold: true);
                        created = await vol.Snapshots.CreateAsync(name, new DfsSnapshotOptions { Retention = retention }, ct);
                        Assert.Equal(retention.Hold, created.Retention.Hold);
                        break;
                    }

                case CreationPath.CreateSync:
                    created = await Task.Run(() => vol.Snapshots.Create(name), ct);
                    break;
                case CreationPath.WriteAllBytesCommitSnapshot:
                    {
                        CommitToken token = await vol.Files.WriteAllBytesAsync("/app/state", TestData.Bytes(0xA_001_002, 6000),
                            new DfsMutationOptions { CommitSnapshotName = name }, ct);
                        created = await vol.Snapshots.GetAsync(name, ct);
                        Assert.Equal(token, created.Boundary);
                        break;
                    }

                case CreationPath.StreamCommitSnapshot:
                    {
                        await using DfsStream stream = await vol.Files.OpenAsync("/app/state", FileMode.Open, FileAccess.ReadWrite, FileShare.Read, ct);
                        await stream.WriteAsync(TestData.Bytes(0xA_001_003, 300), ct);
                        CommitToken token = await stream.CommitAsync(new DfsMutationOptions { CommitSnapshotName = name }, ct);
                        created = await vol.Snapshots.GetAsync(name, ct);
                        Assert.Equal(token, created.Boundary);
                        break;
                    }

                case CreationPath.TransactionCommitWithSnapshot:
                    {
                        await using DfsTransaction tx = await vol.Transactions.BeginAsync(null, ct);
                        await tx.Files.WriteAllBytesAsync("/app/state", TestData.Bytes(0xA_001_004, 4000), ct);
                        await tx.Files.WriteAllBytesAsync("/app/other", TestData.Bytes(0xA_001_005, 4000), ct);
                        created = await tx.CommitWithSnapshotAsync(name, ct);
                        Assert.Equal(DfsTransactionState.Committed, tx.State);
                        break;
                    }

                default:
                    throw new ArgumentOutOfRangeException(nameof(path), path, null);
            }

            AssertFilesystemConsistent(created, name, "created");
            AssertFilesystemConsistent(await vol.Snapshots.GetAsync(name, ct), name, "GetAsync");
            DfsSnapshotInfo listed = Assert.Single(await vol.Snapshots.ListAsync(ct), s => s.Name == name);
            AssertFilesystemConsistent(listed, name, "ListAsync");
            Assert.Equal(created.Id, listed.Id);
            Assert.Equal(created.Boundary, listed.Boundary);
            await using (DfsSnapshotView view = await vol.Snapshots.OpenAsync(name, ct))
                AssertFilesystemConsistent(view.Info, name, "OpenAsync view");

            // Every replica's applied state carries the same label (it is decided in the replicated entry).
            await cluster.WaitForReplicationAsync(Volume, ct);
            foreach (TestNode node in cluster.Nodes)
            {
                VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Volume, node.Name);
                SnapshotInspection replica = Assert.Single(state.Snapshots, s => s.Name == name);
                Assert.Equal(DfsConsistencyLabel.FilesystemConsistent, replica.Label);
                Assert.Equal(created.Id, replica.Id);
                Assert.Equal(created.Boundary.Sequence, replica.BoundaryIndex);
            }
        }, ct);
    }

    private static void AssertFilesystemConsistent(DfsSnapshotInfo info, string name, string source)
    {
        Assert.Equal(name, info.Name);
        Assert.Equal(Volume, info.Volume);
        Assert.Equal(DfsConsistencyLabel.FilesystemConsistent, info.Label);
        Assert.NotEqual(DfsConsistencyLabel.ApplicationConsistent, info.Label);
        Assert.Empty(info.Adapters);
        Assert.Null(info.AdapterResumeFailure);
        Assert.False(info.Boundary.IsNone, $"{source}: the snapshot must report its commit boundary");
    }
}
