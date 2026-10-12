using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Tests.Unit.Internal.Storage.Infrastructure;

namespace Tedd.Tdfs.Tests.Unit.Internal.Storage;

public abstract class AtomicFileContractTests : StorageContractTests
{
    [Fact]
    public async Task ReadAbsent_ReturnsNull()
    {
        Assert.Null(await AtomicFile.ReadAsync(Medium, "dir/none"));
    }

    [Fact]
    public async Task WriteThenRead_RoundTrips_AndCreatesParentDirectories()
    {
        byte[] content = TestData.Bytes(1, 1000);
        await AtomicFile.WriteAsync(Medium, "a/b/c/file", content);
        Assert.Equal(content, await AtomicFile.ReadAsync(Medium, "a/b/c/file"));
        Assert.Equal(content, await AtomicFile.ReadAsync(Host.Restart(), "a/b/c/file"));
    }

    [Fact]
    public async Task Overwrite_ReplacesContent_AndLeavesNoTempFile()
    {
        await AtomicFile.WriteAsync(Medium, "f", TestData.Bytes(1, 50));
        await AtomicFile.WriteAsync(Medium, "f", TestData.Bytes(2, 70));
        Assert.Equal(TestData.Bytes(2, 70), await AtomicFile.ReadAsync(Medium, "f"));
        Assert.False(await Medium.ExistsAsync("f" + AtomicFile.TempSuffix));
    }

    [Fact]
    public async Task EmptyContent_RoundTrips()
    {
        await AtomicFile.WriteAsync(Medium, "empty", ReadOnlyMemory<byte>.Empty);
        byte[]? read = await AtomicFile.ReadAsync(Medium, "empty");
        Assert.NotNull(read);
        Assert.Empty(read);
    }

    [Fact]
    public async Task StaleTempFile_IsIgnoredAndOverwritten()
    {
        await AtomicFile.WriteAsync(Medium, "f", TestData.Bytes(1, 10));
        await using (IStorageFile stale = await Medium.OpenAsync("f" + AtomicFile.TempSuffix, StorageOpenMode.Truncate))
            await stale.WriteFullyAsync(0, TestData.Bytes(9, 3000));
        Assert.Equal(TestData.Bytes(1, 10), await AtomicFile.ReadAsync(Medium, "f"));
        await AtomicFile.WriteAsync(Medium, "f", TestData.Bytes(2, 20));
        Assert.Equal(TestData.Bytes(2, 20), await AtomicFile.ReadAsync(Medium, "f"));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(5)]
    [InlineData(10)]
    [InlineData(17)]
    [InlineData(40)]
    public async Task CorruptByte_ThrowsIntegrity(int offset)
    {
        await AtomicFile.WriteAsync(Medium, "f", TestData.Bytes(3, 30));
        Host.Corrupt("f", offset);
        await Assert.ThrowsAsync<DfsIntegrityException>(async () => await AtomicFile.ReadAsync(Medium, "f"));
    }

    [Fact]
    public async Task TruncatedFile_ThrowsIntegrity()
    {
        await AtomicFile.WriteAsync(Medium, "f", TestData.Bytes(3, 30));
        await using (IStorageFile file = await Medium.OpenAsync("f", StorageOpenMode.OpenExisting))
            await file.SetLengthAsync(25);
        await Assert.ThrowsAsync<DfsIntegrityException>(async () => await AtomicFile.ReadAsync(Medium, "f"));
    }

    [Fact]
    public async Task Delete_RemovesFileAndTemp()
    {
        await AtomicFile.WriteAsync(Medium, "d/f", TestData.Bytes(1, 5));
        await AtomicFile.DeleteAsync(Medium, "d/f");
        Assert.Null(await AtomicFile.ReadAsync(Medium, "d/f"));
        await AtomicFile.DeleteAsync(Medium, "d/f"); // idempotent
    }
}

public sealed class AtomicFileSimulatedTests : AtomicFileContractTests
{
    protected override MediumHost CreateHost() => new SimulatedHost();

    [Fact]
    public async Task NewerFrameVersion_IsRefusedAsUnsupported()
    {
        byte[] frame = AtomicFile.Encode(TestData.Bytes(1, 8));
        frame[4] = 2; // version
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(frame.AsSpan(frame.Length - 4), Tedd.Tdfs.Data.ObjectChecksum.Crc32C(frame.AsSpan(0, frame.Length - 4)));
        await using (IStorageFile file = await Medium.OpenAsync("f", StorageOpenMode.Truncate))
            await file.WriteFullyAsync(0, frame);
        DfsProtocolException ex = await Assert.ThrowsAsync<DfsProtocolException>(async () => await AtomicFile.ReadAsync(Medium, "f"));
        Assert.Equal(DfsErrorCode.Unsupported, ex.ErrorCode);
    }

    [Fact]
    public async Task FailedFlush_LeavesPreviousContent()
    {
        await AtomicFile.WriteAsync(Medium, "f", TestData.Bytes(1, 10));
        Host.Simulated!.Faults.FailNextFlushes = 1;
        await Assert.ThrowsAnyAsync<IOException>(async () => await AtomicFile.WriteAsync(Medium, "f", TestData.Bytes(2, 10)));
        Host.Simulated.Crash(CrashMode.KeepUnflushed);
        Assert.Equal(TestData.Bytes(1, 10), await AtomicFile.ReadAsync(Medium, "f"));
    }

    [Fact]
    public async Task CrashMatrix_WriteYieldsOldOrNewContent()
    {
        byte[] oldContent = TestData.Bytes(1, 300);
        byte[] newContent = TestData.Bytes(2, 500);
        int cases = await CrashMatrix.RunAsync(
            async medium => await AtomicFile.WriteAsync(medium, "dir/state", oldContent),
            async medium => await AtomicFile.WriteAsync(medium, "dir/state", newContent),
            async (medium, crash) =>
            {
                byte[]? read = await AtomicFile.ReadAsync(medium, "dir/state");
                Assert.NotNull(read);
                if (crash.Completed)
                    Assert.Equal(newContent, read);
                else
                    Assert.True(read.AsSpan().SequenceEqual(oldContent) || read.AsSpan().SequenceEqual(newContent), "content is neither old nor new");
                return Convert.ToHexString(read);
            });
        Assert.True(cases > 20);
    }

    [Fact]
    public async Task CrashMatrix_FirstWriteYieldsAbsentOrNew()
    {
        byte[] content = TestData.Bytes(3, 64);
        await CrashMatrix.RunAsync(
            _ => Task.CompletedTask,
            async medium => await AtomicFile.WriteAsync(medium, "a/b/state", content),
            async (medium, crash) =>
            {
                byte[]? read = await AtomicFile.ReadAsync(medium, "a/b/state");
                if (crash.Completed)
                    Assert.Equal(content, read);
                else
                    Assert.True(read is null || read.AsSpan().SequenceEqual(content));
                return read is null ? "absent" : "new";
            });
    }
}

public sealed class AtomicFileFileSystemTests : AtomicFileContractTests
{
    protected override MediumHost CreateHost() => new FileSystemHost();
}
