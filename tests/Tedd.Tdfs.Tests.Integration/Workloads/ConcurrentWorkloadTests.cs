using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Integration.Workloads;

public sealed class ConcurrentWorkloadTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private sealed record FileImage(string Path, byte[] Bytes, byte[]? Metadata);

    [Theory]
    [InlineData(1024, 0, false)]
    [InlineData(128, 4, true)]
    [InlineData(0, 4, false)]
    public Task PagedTreeAndSnapshotPreserveEveryWorkersFiles(int smallFiles, int largeFiles, bool shareClient) =>
        ClusterTest.RunAsync(TestClusterOptions.ThreeZones with
        {
            Storage = StorageKind.Disk,
            ScenarioTimeout = TimeSpan.FromMinutes(5),
        }, async cluster =>
        {
            await using DfsClient first = await cluster.ConnectAsync(new TestClientOptions { Name = "first" }, Ct);
            await using DfsClient second = await cluster.ConnectAsync(new TestClientOptions { Name = "second" }, Ct);
            await using DfsClient observer = await cluster.ConnectAsync(new TestClientOptions { Name = "observer" }, Ct);
            DfsVolume volume = observer.GetVolume("vol");
            await volume.Directories.CreateDirectoryAsync("/batch", Ct);
            int[] smallSizes = [0, 1, 7, 255, 4095, 4096, 4097];
            var original = Enumerable.Range(0, smallFiles + largeFiles).Select(i =>
            {
                int size = i < largeFiles ? (4 << 20) + i - 1 : smallSizes[(i - largeFiles) % smallSizes.Length];
                return new FileImage($"/batch/f{i:D4}", TestData.Bytes((ulong)i + 1, size),
                    i % 7 == 0 ? TestData.Bytes((ulong)i + 503, 4101) : null);
            }).ToArray();

            DfsFileClient Writer(int i) => (shareClient || i % 2 == 0 ? first : second).GetVolume("vol").Files;
            await RunWorkersAsync(original.Length, async i =>
            {
                FileImage image = original[i];
                DfsFileClient files = Writer(i);
                await files.WriteAllBytesAsync(image.Path, image.Bytes, Ct);
                if (image.Metadata is not null)
                    await files.WriteAllBytesAsync(image.Path + ":meta", image.Metadata, Ct);
            });
            await volume.Snapshots.CreateAsync("baseline", ct: Ct);

            var live = new FileImage?[original.Length];
            await RunWorkersAsync(original.Length, async i =>
            {
                FileImage image = original[i];
                DfsFileClient files = Writer(i);
                switch (i % 4)
                {
                    case 0:
                        await files.DeleteAsync(image.Path, Ct);
                        break;
                    case 1:
                        string renamed = image.Path + "-moved";
                        await files.MoveAsync(image.Path, renamed, ct: Ct);
                        live[i] = image with { Path = renamed };
                        break;
                    case 2:
                        byte[] replacement = TestData.Bytes((ulong)i + 1009, image.Bytes.Length == 0 ? 8193 : 1);
                        await files.WriteAllBytesAsync(image.Path, replacement, Ct);
                        live[i] = image with { Bytes = replacement };
                        break;
                    default:
                        byte[] suffix = TestData.Bytes((ulong)i + 2039, 33);
                        await files.AppendAllBytesAsync(image.Path, suffix, Ct);
                        live[i] = image with { Bytes = [.. image.Bytes, .. suffix] };
                        break;
                }
            });
            await VerifyTreeAsync(volume.Directories, volume.Files, live.OfType<FileImage>());
            await using DfsSnapshotView snapshot = await volume.Snapshots.OpenAsync("baseline", Ct);
            await VerifyTreeAsync(snapshot.Directories, snapshot.Files, original);
        }, Ct);

    private static async Task VerifyTreeAsync(DfsDirectoryClient directories, DfsFileClient files, IEnumerable<FileImage> images)
    {
        FileImage[] expected = images.ToArray();
        Dictionary<string, FileImage> remaining = expected.ToDictionary(image => image.Path, StringComparer.Ordinal);
        await foreach (DfsEntryInfo entry in directories.EnumerateAsync("/batch", new DfsEnumerationOptions { PageSize = 7 }, Ct))
        {
            Assert.True(remaining.Remove(entry.Path, out FileImage? image), $"Unexpected or duplicate entry {entry.Path}");
            Assert.Equal(image.Bytes.LongLength, entry.Length);
        }
        Assert.Empty(remaining);
        await RunWorkersAsync(expected.Length, async index =>
        {
            FileImage image = expected[index];
            byte[] data = await files.ReadAllBytesAsync(image.Path, Ct);
            AssertEx.BytesEqual(image.Bytes, data, image.Path);
            var streams = await files.ListStreamsAsync(image.Path, Ct);
            if (image.Metadata is not null)
            {
                DfsStreamInfo stream = Assert.Single(streams);
                Assert.Equal(("meta", image.Metadata.LongLength), (stream.Name, stream.Length));
                byte[] metadata = await files.ReadAllBytesAsync(image.Path + ":meta", Ct);
                AssertEx.BytesEqual(image.Metadata, metadata, image.Path + ":meta");
            }
            else
            {
                Assert.Empty(streams);
            }
        });
    }

    private static Task RunWorkersAsync(int count, Func<int, Task> operation) =>
        Parallel.ForEachAsync(Enumerable.Range(0, count), new ParallelOptions
        {
            MaxDegreeOfParallelism = 8,
            CancellationToken = Ct,
        }, async (index, _) => await operation(index));
}
