using Tedd.Tdfs.Testing;
using static Tedd.Tdfs.Tests.Simulation.Prop.PropHelpers;

namespace Tedd.Tdfs.Tests.Simulation.Prop;

public sealed class SparseAddressWorkloadTests
{
    [Theory]
    [InlineData(31, 19001UL)]
    [InlineData(32, 19009UL)]
    [InlineData(40, 19013UL)]
    public Task HighAddressOverwritesAndTruncationPreserveLowOffsets(int addressBits, ulong seed) =>
        RunPropertyAsync(nameof(HighAddressOverwritesAndTruncationPreserveLowOffsets), seed, trace =>
            ClusterTest.RunAsync(Sim(seed), async cluster =>
            {
                const string path = "/sparse";
                const int width = 8192;
                long boundary = 1L << addressBits;
                long[] starts = [0, boundary - width / 2, boundary * 2 - width / 2];
                long length = starts[^1] + width;
                byte[][] windows = starts.Select(_ => new byte[width]).ToArray();
                var random = new SeededRandom(seed);
                await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "writer" }, Ct);
                await using DfsClient observer = await cluster.ConnectAsync(new TestClientOptions { Name = "observer" }, Ct);
                DfsVolume volume = client.GetVolume(Vol);
                DfsVolume other = observer.GetVolume(Vol);
                await using (DfsStream writer = await volume.Files.CreateAsync(path, Ct))
                {
                    await writer.SetLengthAsync(length, Ct);
                    for (int step = 0; step < 45; step++)
                    {
                        int window = step % starts.Length;
                        int offset = step < 3 ? 0 : random.Next(width);
                        int count = step < 3 ? width : random.Next(width - offset + 1);
                        byte[] data = TestData.Bytes(seed + (ulong)step, count);
                        trace.Step($"write at {starts[window] + offset} length {count}");
                        writer.Seek(starts[window] + offset, SeekOrigin.Begin);
                        await writer.WriteAsync(data, Ct);
                        data.CopyTo(windows[window], offset);
                    }
                    await writer.CommitAsync(Ct);
                }
                await other.Snapshots.CreateAsync("original", ct: Ct);
                await VerifyAsync(other.Files, path, starts, windows, length);

                // Truncate inside the middle window, then extend back over previously allocated extents.
                long cut = boundary + 1;
                await using (DfsStream aborted = await volume.Files.OpenWriteAsync(path, Ct))
                {
                    await aborted.SetLengthAsync(cut, Ct);
                    await aborted.SetLengthAsync(length, Ct);
                    await aborted.AbortAsync(Ct);
                }
                await VerifyAsync(other.Files, path, starts, windows, length);
                await using (DfsStream committed = await volume.Files.OpenWriteAsync(path, Ct))
                {
                    await committed.SetLengthAsync(cut, Ct);
                    await committed.CommitAsync(Ct);
                    await committed.SetLengthAsync(length, Ct);
                    await committed.CommitAsync(Ct);
                }

                byte[][] truncated = windows.Select(bytes => bytes.ToArray()).ToArray();
                Array.Clear(truncated[1], (int)(cut - starts[1]), width - (int)(cut - starts[1]));
                Array.Clear(truncated[2]);
                await VerifyAsync(other.Files, path, starts, truncated, length);
                await using DfsSnapshotView snapshot = await other.Snapshots.OpenAsync("original", Ct);
                await VerifyAsync(snapshot.Files, path, starts, windows, length);
            }, Ct));

    private static async Task VerifyAsync(DfsFileClient files, string path, long[] starts, byte[][] windows, long length)
    {
        await using DfsStream reader = await files.OpenReadAsync(path, Ct);
        Assert.Equal(length, await reader.GetLengthAsync(Ct));
        for (int i = 0; i < starts.Length; i++)
        {
            byte[] bytes = await ReadRangeAsync(reader, starts[i], windows[i].Length);
            AssertEx.BytesEqual(windows[i], bytes, $"window at {starts[i]}");
            if (i > 0)
            {
                long middleOfHole = (starts[i - 1] + windows[i - 1].Length + starts[i]) / 2;
                byte[] hole = await ReadRangeAsync(reader, middleOfHole, 8193);
                AssertEx.BytesEqual(new byte[8193], hole, $"hole at {middleOfHole}");
            }
        }
        Assert.Empty(await ReadRangeAsync(reader, length, 1));
    }
}
