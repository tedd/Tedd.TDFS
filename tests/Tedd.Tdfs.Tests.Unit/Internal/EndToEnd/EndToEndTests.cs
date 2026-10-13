using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Unit.Internal.EndToEnd;

/// <summary>
/// Whole-stack smoke path (integration of harness, SDK and volume service): a client creates a directory, writes a file
/// larger than the inline threshold and the chunk size through a stream, flushes, a second client reads it back byte for
/// byte, then the file is renamed and deleted. Runs realtime in memory, deterministic, and over QUIC on real disks.
/// </summary>
public sealed class EndToEndTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public static TheoryData<string> Variants => new() { "Realtime", "Deterministic", "QuicDisk" };

    private static TestClusterOptions OptionsFor(string variant) => variant switch
    {
        "Realtime" => TestClusterOptions.ThreeZones with { Mode = ClusterMode.Realtime },
        "Deterministic" => TestClusterOptions.ThreeZones with { Mode = ClusterMode.Deterministic },
        "QuicDisk" => TestClusterOptions.ThreeZones with { Mode = ClusterMode.Realtime, Transport = TransportKind.Quic, Storage = StorageKind.Disk },
        _ => throw new ArgumentOutOfRangeException(nameof(variant)),
    };

    [Theory]
    [MemberData(nameof(Variants))]
    public async Task CreateWriteReadRenameDelete(string variant)
    {
        await ClusterTest.RunAsync(OptionsFor(variant), async cluster =>
        {
            await using DfsClient writer = await cluster.ConnectAsync(new TestClientOptions { Name = "w" }, Ct);
            await using DfsClient reader = await cluster.ConnectAsync(new TestClientOptions { Name = "r" }, Ct);
            DfsVolume wv = writer.GetVolume("vol");
            DfsVolume rv = reader.GetVolume("vol");

            DfsVolumeInfo info = await wv.GetInfoAsync(Ct);
            int chunk = cluster.Options.Timings.ChunkSize;
            Assert.True(chunk > info.InlineThreshold);

            DfsDirectoryInfo dir = await wv.Directories.CreateDirectoryAsync("/e2e/sub", Ct);
            Assert.Equal("/e2e/sub", dir.Path);
            Assert.True(await rv.Directories.ExistsAsync("/e2e/sub", Ct));

            // Larger than the chunk size (several chunks plus a ragged tail) and far above the inline threshold.
            int size = (3 * chunk) + 12_345;
            byte[] payload = TestData.Bytes(4242, size);
            await using (DfsStream stream = await wv.Files.CreateAsync("/e2e/sub/data.bin", Ct))
            {
                int offset = 0;
                int step = 7_001;
                while (offset < size)
                {
                    int n = Math.Min(step, size - offset);
                    await stream.WriteAsync(payload.AsMemory(offset, n), Ct);
                    offset += n;
                    step = (step * 3 % 50_000) + 1;
                }

                await stream.FlushAsync(Ct);
                Assert.Equal(0, stream.PendingBytes);
            }

            DfsFileInfo fileInfo = await rv.Files.GetInfoAsync("/e2e/sub/data.bin", Ct);
            Assert.Equal(size, fileInfo.Length);
            Assert.False(fileInfo.IsInline);
            Assert.True(fileInfo.ExtentCount >= 1);

            await using (DfsStream read = await rv.Files.OpenReadAsync("/e2e/sub/data.bin", Ct))
            {
                var copy = new MemoryStream();
                await read.CopyToAsync(copy, 10_000, Ct);
                AssertEx.BytesEqual(payload, copy.ToArray(), "stream read from the second client");
            }

            byte[] all = await rv.Files.ReadAllBytesAsync("/e2e/sub/data.bin", Ct);
            AssertEx.BytesEqual(payload, all, "ReadAllBytes from the second client");

            // Rename.
            await wv.Files.MoveAsync("/e2e/sub/data.bin", "/e2e/renamed.bin", false, Ct);
            Assert.False(await rv.Files.ExistsAsync("/e2e/sub/data.bin", Ct));
            Assert.True(await rv.Files.ExistsAsync("/e2e/renamed.bin", Ct));
            Assert.Equal(fileInfo.InodeId, (await rv.Files.GetInfoAsync("/e2e/renamed.bin", Ct)).InodeId);
            byte[] renamed = await rv.Files.ReadAllBytesAsync("/e2e/renamed.bin", Ct);
            AssertEx.BytesEqual(payload, renamed, "content after rename");

            var names = new List<string>();
            await foreach (DfsEntryInfo e in rv.Directories.EnumerateAsync("/e2e", null, Ct))
                names.Add(e.Name);
            Assert.Equal(["renamed.bin", "sub"], names);

            // Delete.
            await wv.Files.DeleteAsync("/e2e/renamed.bin", Ct);
            Assert.False(await rv.Files.ExistsAsync("/e2e/renamed.bin", Ct));
            await Assert.ThrowsAnyAsync<FileNotFoundException>(() => rv.Files.ReadAllBytesAsync("/e2e/renamed.bin", Ct));
            await wv.Directories.DeleteAsync("/e2e", recursive: true, Ct);
            Assert.False(await rv.Directories.ExistsAsync("/e2e", Ct));
        }, Ct);
    }

    /// <summary>
    /// Transactions end to end: private until commit (helpers and streams), atomic visibility at commit, abort discards,
    /// and a conflicting concurrent change of something the transaction read makes its commit fail without effect.
    /// </summary>
    [Theory]
    [MemberData(nameof(Variants))]
    public async Task TransactionsArePrivateAtomicAndValidated(string variant)
    {
        await ClusterTest.RunAsync(OptionsFor(variant), async cluster =>
        {
            await using DfsClient writer = await cluster.ConnectAsync(new TestClientOptions { Name = "w" }, Ct);
            await using DfsClient other = await cluster.ConnectAsync(new TestClientOptions { Name = "o" }, Ct);
            DfsVolume wv = writer.GetVolume("vol");
            DfsVolume ov = other.GetVolume("vol");
            int chunk = cluster.Options.Timings.ChunkSize;
            await wv.Directories.CreateDirectoryAsync("/tx", Ct);
            await wv.Files.WriteAllBytesAsync("/tx/existing", TestData.Bytes(1, 100), Ct);

            // Commit: helpers, a stream and a directory become visible together.
            byte[] big = TestData.Bytes(2, (2 * chunk) + 77);
            await using (DfsTransaction tx = await wv.Transactions.BeginAsync(null, Ct))
            {
                await tx.Directories.CreateDirectoryAsync("/tx/d", Ct);
                await tx.Files.WriteAllBytesAsync("/tx/d/small", TestData.Bytes(3, 10), Ct);
                await using (DfsStream s = await tx.Files.CreateAsync("/tx/d/big", Ct))
                    await s.WriteAsync(big, Ct);
                await tx.Files.AppendAllBytesAsync("/tx/existing", TestData.Bytes(4, 50), Ct);
                Assert.True(await tx.Files.ExistsAsync("/tx/d/small", Ct), "a transaction sees its own writes");
                Assert.False(await ov.Files.ExistsAsync("/tx/d/small", Ct), "others do not see uncommitted writes");
                Assert.Equal(100L, (await ov.Files.GetInfoAsync("/tx/existing", Ct)).Length);
                CommitToken token = await tx.CommitAsync(Ct);
                Assert.False(token.IsNone);
            }

            byte[] small = await ov.Files.ReadAllBytesAsync("/tx/d/small", Ct);
            AssertEx.BytesEqual(TestData.Bytes(3, 10), small, "committed small file");
            byte[] bigRead = await ov.Files.ReadAllBytesAsync("/tx/d/big", Ct);
            AssertEx.BytesEqual(big, bigRead, "committed stream");
            Assert.Equal(150L, (await ov.Files.GetInfoAsync("/tx/existing", Ct)).Length);

            // Abort: nothing becomes visible.
            await using (DfsTransaction tx = await wv.Transactions.BeginAsync(null, Ct))
            {
                await tx.Files.WriteAllBytesAsync("/tx/aborted", TestData.Bytes(5, 10), Ct);
                await tx.AbortAsync(Ct);
            }

            Assert.False(await ov.Files.ExistsAsync("/tx/aborted", Ct));

            // Conflict: the transaction read /tx/existing, another client changed it before the commit.
            await using (DfsTransaction tx = await wv.Transactions.BeginAsync(null, Ct))
            {
                _ = await tx.Files.ReadAllBytesAsync("/tx/existing", Ct);
                await tx.Files.WriteAllBytesAsync("/tx/derived", TestData.Bytes(6, 10), Ct);
                await ov.Files.WriteAllBytesAsync("/tx/existing", TestData.Bytes(7, 10), Ct);
                await Assert.ThrowsAsync<DfsTransactionConflictException>(() => tx.CommitAsync(Ct));
            }

            Assert.False(await ov.Files.ExistsAsync("/tx/derived", Ct), "a conflicting commit has no effect");

            // A disjoint concurrent change does not conflict.
            await using (DfsTransaction tx = await wv.Transactions.BeginAsync(null, Ct))
            {
                await tx.Files.WriteAllBytesAsync("/tx/mine", TestData.Bytes(8, 10), Ct);
                await ov.Files.WriteAllBytesAsync("/tx/theirs", TestData.Bytes(9, 10), Ct);
                await tx.CommitAsync(Ct);
            }

            Assert.True(await ov.Files.ExistsAsync("/tx/mine", Ct));
            Assert.True(await ov.Files.ExistsAsync("/tx/theirs", Ct));
        }, Ct);
    }
}
