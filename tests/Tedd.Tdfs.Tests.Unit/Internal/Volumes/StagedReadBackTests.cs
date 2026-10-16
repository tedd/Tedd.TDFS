using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Unit.Internal.Volumes;

/// <summary>
/// A readable writer keeps client memory bounded by uploading staged bytes early and reads them back with its staging
/// capability, which authorizes reading the objects stored under its own staging session (FSD §6.3/§15, PROTOCOL.md §7.3,
/// §8.1).
/// </summary>
public sealed class StagedReadBackTests
{
    private const string Vol = "vol";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static TestClusterOptions Options() => TestClusterOptions.ThreeZones with { Volumes = [new TestVolumeSpec(Vol)] };

    [Fact]
    public Task ReadableWriter_SpillsStagedBytesAndReadsThemBack() =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "rw" }, Ct);
            DfsFileClient files = client.GetVolume(Vol).Files;
            int chunk = client.Capabilities.ChunkSize;
            byte[] data = TestData.Bytes(0x57A6_0010, (6 * chunk) + 1234);
            await using DfsStream stream = await files.CreateAsync("/spill.bin", Ct);
            var log = cluster.Network.Log;
            long putsBefore = log.CountMessages(new MessageFilter(From: "client:rw", Type: MessageType.PutObject));
            for (int offset = 0; offset < data.Length; offset += 4096)
                await stream.WriteAsync(data.AsMemory(offset, Math.Min(4096, data.Length - offset)), Ct);

            // Staged, unpublished, and (beyond the buffer bound) already uploaded rather than held in memory.
            Assert.Equal(data.Length, stream.PendingBytes);
            Assert.True(log.CountMessages(new MessageFilter(From: "client:rw", Type: MessageType.PutObject)) > putsBefore,
                "a readable writer must upload staged bytes beyond its buffer before the flush");
            await using (DfsClient observer = await cluster.ConnectAsync(new TestClientOptions { Name = "observer" }, Ct))
                Assert.Empty(await observer.GetVolume(Vol).Files.ReadAllBytesAsync("/spill.bin", Ct)); // uploads are not published

            // The writer reads its staged view back exactly, uploaded ranges included, and an overwrite of an uploaded range wins.
            stream.Position = 0;
            byte[] back = new byte[data.Length];
            await stream.ReadExactlyAsync(back, Ct);
            Assert.Equal(data, back);
            byte[] patch = TestData.Bytes(0x57A6_0011, 1000);
            stream.Position = chunk + 17;
            await stream.WriteAsync(patch, Ct);
            patch.CopyTo(data, chunk + 17);
            stream.Position = 0;
            await stream.ReadExactlyAsync(back, Ct);
            Assert.Equal(data, back);

            await stream.FlushAsync(Ct);
            Assert.Equal(0, stream.PendingBytes);
            Assert.Equal(data, await files.ReadAllBytesAsync("/spill.bin", Ct));
        }, Ct);
}
