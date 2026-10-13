using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Unit.Internal.Volumes;

/// <summary>
/// A readable writer keeps client memory bounded by uploading staged bytes early and reads them back through a read
/// capability the volume leader grants on <c>ReadRange</c> with the <c>StagedObjects</c> extension (FSD §6.3/§15,
/// PROTOCOL.md §8.1). Staging capabilities never authorize <c>GetObject</c>; the grant is refused for anything but
/// authentic receipts of uploads to the open staging session bound to the readable handle that presents them.
/// </summary>
public sealed class StagedReadBackTests
{
    private const string Vol = "vol";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static TestClusterOptions Options() => TestClusterOptions.ThreeZones with { Volumes = [new TestVolumeSpec(Vol)] };

    [Fact]
    public Task OwnUploads_AreReadableOnlyThroughTheWritersReadableHandle() =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            await using RawProtocolClient raw = await cluster.ConnectRawAsync(leader.Name, Ct);
            Assert.Equal(DfsErrorCode.None, await raw.AuthenticateAsync(cluster.AdminCredentials));
            await using RawStagedWriter writer = await raw.OpenStagedReadWriterAsync(Vol, "/rw", FileMode.CreateNew);
            await using RawStagedWriter other = await raw.OpenStagedReadWriterAsync(Vol, "/other", FileMode.CreateNew);
            await using RawStagedWriter writeOnly = await raw.OpenStagedWriterAsync(Vol, "/wo", FileMode.CreateNew);

            byte[] data = TestData.Bytes(0x57A6_0001, 70_000);
            var id = new ObjectId(new UInt128(0x57A6_0002, 0x57A6_0003));
            RawPutResult put = await writer.PutObjectAsync(leader.Name, id, data);
            Assert.Equal(DfsErrorCode.None, put.Error);
            RawReceipt receipt = Assert.IsType<RawReceipt>(put.Receipt);

            // The writer's own readable handle is granted read access to its upload, and the object reads back exactly.
            (DfsErrorCode granted, ReadOnlyMemory<byte> capability) = await raw.RequestStagedReadAsync(Vol, writer.Handle, [receipt]);
            Assert.Equal(DfsErrorCode.None, granted);
            Assert.False(capability.IsEmpty);
            RawObjectRead read = await raw.GetObjectWithCapabilityAsync(Vol, id, capability);
            Assert.Equal(DfsErrorCode.None, read.Code);
            Assert.Equal(data, read.Data);

            // Another readable handle (its own staging session) and a write-only handle are refused, and so is a receipt whose
            // MAC no storage node issued.
            Assert.Equal(DfsErrorCode.AccessDenied, (await raw.RequestStagedReadAsync(Vol, other.Handle, [receipt])).Code);
            Assert.Equal(DfsErrorCode.AccessDenied, (await raw.RequestStagedReadAsync(Vol, writeOnly.Handle, [receipt])).Code);
            ObjectReceipt decoded = WireRecordCodec.Decode<ObjectReceipt>(receipt.Wire.Span);
            RawReceipt forged = receipt with { Wire = WireRecordCodec.Encode(decoded with { Mac = new byte[decoded.Mac.Length] }) };
            Assert.Equal(DfsErrorCode.AccessDenied, (await raw.RequestStagedReadAsync(Vol, writer.Handle, [forged])).Code);

            // Once the handle closes (and its staging session with it) the upload can no longer be granted.
            await writer.DisposeAsync();
            Assert.NotEqual(DfsErrorCode.None, (await raw.RequestStagedReadAsync(Vol, writer.Handle, [receipt])).Code);
        }, Ct);

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
