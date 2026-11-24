using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Integration.Workloads;

public sealed class LargeFileFailoverTests
{
    [Theory]
    [InlineData("/payload", 32)]
    [InlineData("/payload:archive", 64)]
    public Task FailoverPreservesOverwrittenBlocksAndTheirSnapshot(string path, int mebibytes)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        return ClusterTest.RunAsync(TestClusterOptions.ThreeZones with
        {
            Storage = StorageKind.Disk,
            ScenarioTimeout = TimeSpan.FromMinutes(5),
        }, async cluster =>
        {
            const int blockSize = 1 << 20;
            const int tailLength = 37;
            long length = (long)mebibytes * blockSize + tailLength;
            byte[] patch = TestData.Bytes(7721, 131);
            long[] offsets = [0, blockSize - 1, length / 2 - 31, length - patch.Length];
            string sibling = path == "/payload" ? "/payload:marker" : "/payload";
            byte[] marker = TestData.Bytes(7723, 4097);

            await using (DfsClient writer = await cluster.ConnectAsync(new TestClientOptions { Name = "writer" }, ct))
            {
                DfsVolume volume = writer.GetVolume("vol");
                await volume.Files.WriteAllBytesAsync(sibling, marker, ct);
                await using (DfsStream file = await volume.Files.CreateAsync(path, ct))
                {
                    for (int i = 0; i <= mebibytes; i++)
                        await file.WriteAsync(Block(i), ct);
                }
                await volume.Snapshots.CreateAsync("unpatched", ct: ct);
                await using DfsStream edit = await volume.Files.OpenWriteAsync(path, ct);
                foreach (long offset in offsets)
                {
                    edit.Position = offset;
                    await edit.WriteAsync(patch, ct);
                }
                await edit.CommitAsync(ct);
            }

            TestNode leader = await cluster.GetLeaderAsync("vol", ct);
            await leader.CrashAsync();
            try
            {
                Assert.NotEqual(leader.Name, (await cluster.GetLeaderAsync("vol", ct)).Name);
                await using DfsClient observer = await cluster.ConnectAsync(new TestClientOptions { Name = "observer" }, ct);
                DfsVolume volume = observer.GetVolume("vol");
                await VerifyAsync(volume.Files, patched: true);
                await using DfsSnapshotView snapshot = await volume.Snapshots.OpenAsync("unpatched", ct);
                await VerifyAsync(snapshot.Files, patched: false);
            }
            finally
            {
                await leader.RestartAsync();
                await cluster.WaitForStableAsync(ct: ct);
            }

            byte[] Block(int i) => TestData.Bytes((ulong)(5000 + i), i == mebibytes ? tailLength : blockSize);

            async Task VerifyAsync(DfsFileClient files, bool patched)
            {
                byte[] siblingBytes = await files.ReadAllBytesAsync(sibling, ct);
                AssertEx.BytesEqual(marker, siblingBytes, sibling);
                await using DfsStream file = await files.OpenReadAsync(path, ct);
                Assert.Equal(length, await file.GetLengthAsync(ct));
                byte[] actual = new byte[blockSize];
                for (int i = 0; i <= mebibytes; i++)
                {
                    byte[] expected = Block(i);
                    long start = (long)i * blockSize;
                    if (patched)
                    {
                        foreach (long offset in offsets)
                        {
                            long from = Math.Max(start, offset);
                            long to = Math.Min(start + expected.Length, offset + patch.Length);
                            if (from < to)
                                patch.AsSpan((int)(from - offset), (int)(to - from)).CopyTo(expected.AsSpan((int)(from - start)));
                        }
                    }
                    await file.ReadExactlyAsync(actual.AsMemory(0, expected.Length), ct);
                    AssertEx.BytesEqual(expected, actual.AsSpan(0, expected.Length), $"{path} block {i}, patched={patched}");
                }
                Assert.Equal(0, await file.ReadAsync(actual.AsMemory(0, 1), ct));
            }
        }, ct);
    }
}
