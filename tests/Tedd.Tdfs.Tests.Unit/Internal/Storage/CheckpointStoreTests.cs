using System.Globalization;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Tests.Unit.Internal.Storage.Infrastructure;

namespace Tedd.Tdfs.Tests.Unit.Internal.Storage;

internal static class CheckpointTestData
{
    public const string Dir = "raft/control/checkpoints";

    public static byte[][] Chunks(int seed, int count, int size = 3000)
        => [.. Enumerable.Range(0, count).Select(i => TestData.Bytes((ulong)((seed * 1000) + i), size + i))];

    public static byte[] Metadata(int seed) => TestData.Bytes((ulong)(90_000 + seed), 40);

    public static async Task<CheckpointInfo> WriteAsync(CheckpointStore store, ulong index, ulong term, int seed, int count = 4)
    {
        await using CheckpointWriter writer = await store.BeginWriteAsync(index, term, Metadata(seed));
        foreach (byte[] chunk in Chunks(seed, count))
            await writer.WriteAsync(chunk);
        return await writer.CompleteAsync();
    }

    public static async Task AssertContentAsync(CheckpointReader reader, ulong index, ulong term, int seed, int count = 4)
    {
        Assert.Equal(index, reader.Index);
        Assert.Equal(term, reader.Term);
        Assert.Equal(Metadata(seed), reader.Metadata.ToArray());
        var chunks = new List<byte[]>();
        await foreach (ReadOnlyMemory<byte> chunk in reader.ReadChunksAsync())
            chunks.Add(chunk.ToArray());
        Assert.Equal(Chunks(seed, count), chunks);
    }
}

public abstract class CheckpointStoreContractTests : StorageContractTests
{
    private const string Dir = CheckpointTestData.Dir;

    protected virtual IRecordCipher? Cipher => null;

    protected ValueTask<CheckpointStore> OpenAsync(IStorageMedium? medium = null, IFaultInjector? faults = null)
        => CheckpointStore.OpenAsync(medium ?? Medium, Dir, new CheckpointStoreOptions { Cipher = Cipher, Faults = faults ?? NoFaults.Instance });

    [Fact]
    public async Task NoCheckpoint_ReturnsNull()
    {
        CheckpointStore store = await OpenAsync();
        Assert.Null(store.Current);
        Assert.Null(await store.OpenLatestAsync());
    }

    [Fact]
    public async Task WriteComplete_BecomesCurrent_AndSurvivesRestart()
    {
        CheckpointStore store = await OpenAsync();
        CheckpointInfo info = await CheckpointTestData.WriteAsync(store, 100, 3, seed: 1);
        Assert.Equal(info, store.Current);
        await using (CheckpointReader reader = (await store.OpenLatestAsync())!)
            await CheckpointTestData.AssertContentAsync(reader, 100, 3, 1);

        CheckpointStore reopened = await OpenAsync(Host.Restart());
        Assert.Equal(info, reopened.Current);
        await using CheckpointReader again = (await reopened.OpenLatestAsync())!;
        await CheckpointTestData.AssertContentAsync(again, 100, 3, 1);
    }

    [Fact]
    public async Task EmptyCheckpoint_IsValid()
    {
        CheckpointStore store = await OpenAsync();
        await using (CheckpointWriter writer = await store.BeginWriteAsync(5, 1, ReadOnlyMemory<byte>.Empty))
        {
            await writer.WriteAsync(ReadOnlyMemory<byte>.Empty);
            await writer.CompleteAsync();
        }

        await using CheckpointReader reader = (await store.OpenLatestAsync())!;
        Assert.Empty((await reader.ReadAllAsync()));
        Assert.True(reader.Metadata.IsEmpty);
    }

    [Fact]
    public async Task NewerCheckpoint_ReplacesOlder_AndOldFileIsDeleted()
    {
        CheckpointStore store = await OpenAsync();
        CheckpointInfo first = await CheckpointTestData.WriteAsync(store, 10, 1, seed: 1);
        CheckpointInfo second = await CheckpointTestData.WriteAsync(store, 20, 2, seed: 2);
        Assert.True(second.Sequence > first.Sequence);
        Assert.Equal(second, store.Current);
        IReadOnlyList<string> files = await Medium.ListAsync(Dir);
        Assert.Single(files, f => f.EndsWith(CheckpointStore.FileExtension, StringComparison.Ordinal));
        await using CheckpointReader reader = (await store.OpenLatestAsync())!;
        await CheckpointTestData.AssertContentAsync(reader, 20, 2, 2);
    }

    [Fact]
    public async Task AbortOrDispose_KeepsPreviousCurrent()
    {
        CheckpointStore store = await OpenAsync();
        CheckpointInfo first = await CheckpointTestData.WriteAsync(store, 10, 1, seed: 1);
        await using (CheckpointWriter writer = await store.BeginWriteAsync(30, 2, new byte[] { 1 }))
            await writer.WriteAsync(new byte[100]);
        CheckpointWriter aborted = await store.BeginWriteAsync(31, 2, new byte[] { 1 });
        await aborted.WriteAsync(new byte[100]);
        await aborted.AbortAsync();
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await aborted.WriteAsync(new byte[1]));

        Assert.Equal(first, store.Current);
        Assert.Single(await Medium.ListAsync(Dir), f => f.EndsWith(CheckpointStore.FileExtension, StringComparison.Ordinal));
        await using CheckpointReader reader = (await store.OpenLatestAsync())!;
        await CheckpointTestData.AssertContentAsync(reader, 10, 1, 1);
    }

    [Fact]
    public async Task OlderCheckpoint_IsRejected()
    {
        CheckpointStore store = await OpenAsync();
        await CheckpointTestData.WriteAsync(store, 50, 2, seed: 1);
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.BeginWriteAsync(49, 2, ReadOnlyMemory<byte>.Empty));

        // A writer started before a newer checkpoint completed cannot roll the state back.
        CheckpointWriter slow = await store.BeginWriteAsync(60, 2, ReadOnlyMemory<byte>.Empty);
        await CheckpointTestData.WriteAsync(store, 70, 3, seed: 2);
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await slow.CompleteAsync());
        await slow.DisposeAsync();
        Assert.Equal(70ul, store.Current!.Index);
        await using CheckpointReader reader = (await store.OpenLatestAsync())!;
        await CheckpointTestData.AssertContentAsync(reader, 70, 3, 2);
    }

    [Fact]
    public async Task CorruptChunk_IsDetected()
    {
        CheckpointStore store = await OpenAsync();
        CheckpointInfo info = await CheckpointTestData.WriteAsync(store, 10, 1, seed: 1);
        string path = Dir + "/" + StorageFormatName(info.Sequence);
        Host.Corrupt(path, info.FileLength / 2);
        await Assert.ThrowsAsync<DfsIntegrityException>(async () => await store.OpenLatestAsync());

        await using CheckpointReader unverified = (await store.OpenLatestAsync(verifyContent: false))!;
        await Assert.ThrowsAsync<DfsIntegrityException>(async () => await unverified.ReadAllAsync());
    }

    [Fact]
    public async Task TruncatedOrMissingFile_IsDetected()
    {
        CheckpointStore store = await OpenAsync();
        CheckpointInfo info = await CheckpointTestData.WriteAsync(store, 10, 1, seed: 1);
        string path = Dir + "/" + StorageFormatName(info.Sequence);
        await using (IStorageFile file = await Medium.OpenAsync(path, StorageOpenMode.OpenExisting))
            await file.SetLengthAsync(info.FileLength - 1);
        await Assert.ThrowsAsync<DfsIntegrityException>(async () => await store.OpenLatestAsync());
        await Medium.DeleteAsync(path);
        await Assert.ThrowsAsync<DfsIntegrityException>(async () => await store.OpenLatestAsync());
    }

    [Fact]
    public async Task ChunkLimit_IsEnforced()
    {
        CheckpointStore store = await CheckpointStore.OpenAsync(Medium, Dir, new CheckpointStoreOptions { MaxChunkSize = 10, Cipher = Cipher });
        await using CheckpointWriter writer = await store.BeginWriteAsync(1, 1, ReadOnlyMemory<byte>.Empty);
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await writer.WriteAsync(new byte[11]));
    }

    protected static string StorageFormatName(ulong sequence) => sequence.ToString("x16", CultureInfo.InvariantCulture) + CheckpointStore.FileExtension;
}

public sealed class CheckpointStoreSimulatedTests : CheckpointStoreContractTests
{
    private const string Dir = CheckpointTestData.Dir;

    protected override MediumHost CreateHost() => new SimulatedHost();

    [Fact]
    public async Task InstallFaultSite_IsHit_AndAThrowKeepsPreviousCurrent()
    {
        var faults = new RecordingFaults();
        CheckpointStore store = await OpenAsync(faults: faults);
        CheckpointInfo first = await CheckpointTestData.WriteAsync(store, 10, 1, seed: 1);
        FaultContext hit = Assert.Single(faults.Hits);
        Assert.Equal(FaultSites.RecoveryCheckpointInstall, hit.Site);
        Assert.Equal(FaultStage.F16, hit.Stage);

        faults.ThrowAtSite = FaultSites.RecoveryCheckpointInstall;
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await CheckpointTestData.WriteAsync(store, 20, 1, seed: 2));
        Assert.Equal(first, store.Current);
        Assert.Single(await Medium.ListAsync(Dir), f => f.EndsWith(CheckpointStore.FileExtension, StringComparison.Ordinal));
    }

    [Fact]
    public async Task StaleFilesFromCrashedWriters_AreRemovedAtOpen()
    {
        CheckpointStore store = await OpenAsync();
        await CheckpointTestData.WriteAsync(store, 10, 1, seed: 1);
        CheckpointWriter writer = await store.BeginWriteAsync(20, 1, ReadOnlyMemory<byte>.Empty);
        await writer.WriteAsync(new byte[1000]);
        Host.Simulated!.Crash(CrashMode.KeepUnflushed); // writer abandoned

        CheckpointStore reopened = await OpenAsync();
        Assert.Equal(10ul, reopened.Current!.Index);
        Assert.Single(await Medium.ListAsync(Dir), f => f.EndsWith(CheckpointStore.FileExtension, StringComparison.Ordinal));
        CheckpointInfo next = await CheckpointTestData.WriteAsync(reopened, 30, 2, seed: 3);
        Assert.True(next.Sequence > writer.Sequence);
    }

    [Fact]
    public async Task CrashMatrix_CompleteYieldsPreviousOrNewCheckpoint()
    {
        bool acknowledged = false;
        int cases = await CrashMatrix.RunAsync(
            async medium =>
            {
                acknowledged = false;
                CheckpointStore store = await CheckpointStore.OpenAsync(medium, Dir, new CheckpointStoreOptions());
                await CheckpointTestData.WriteAsync(store, 10, 1, seed: 1, count: 2);
            },
            async medium =>
            {
                CheckpointStore store = await CheckpointStore.OpenAsync(medium, Dir, new CheckpointStoreOptions());
                await CheckpointTestData.WriteAsync(store, 20, 2, seed: 2, count: 3);
                acknowledged = true;
            },
            async (medium, crash) =>
            {
                CheckpointStore store = await CheckpointStore.OpenAsync(medium, Dir, new CheckpointStoreOptions());
                Assert.NotNull(store.Current);
                await using CheckpointReader reader = (await store.OpenLatestAsync())!;
                if (reader.Index == 10)
                {
                    Assert.False(acknowledged, "an acknowledged checkpoint was lost");
                    await CheckpointTestData.AssertContentAsync(reader, 10, 1, 1, 2);
                }
                else
                {
                    await CheckpointTestData.AssertContentAsync(reader, 20, 2, 2, 3);
                }

                Assert.Single(await medium.ListAsync(Dir), f => f.EndsWith(CheckpointStore.FileExtension, StringComparison.Ordinal));
                return reader.Index.ToString(CultureInfo.InvariantCulture);
            });
        Assert.True(cases > 30);
    }

    [Fact]
    public async Task CrashMatrix_FirstCheckpointYieldsNoneOrComplete()
    {
        await CrashMatrix.RunAsync(
            _ => Task.CompletedTask,
            async medium =>
            {
                CheckpointStore store = await CheckpointStore.OpenAsync(medium, Dir, new CheckpointStoreOptions());
                await CheckpointTestData.WriteAsync(store, 7, 1, seed: 5, count: 2);
            },
            async (medium, crash) =>
            {
                CheckpointStore store = await CheckpointStore.OpenAsync(medium, Dir, new CheckpointStoreOptions());
                if (crash.Completed)
                    Assert.NotNull(store.Current);
                await using CheckpointReader? reader = await store.OpenLatestAsync();
                if (reader is not null)
                    await CheckpointTestData.AssertContentAsync(reader, 7, 1, 5, 2);
                return reader is null ? "none" : "complete";
            });
    }
}

public sealed class CheckpointStoreSimulatedEncryptedTests : CheckpointStoreContractTests
{
    private readonly RecordCipher _cipher = new(KeyRing.Derive(TestData.Bytes(12, 32), StorageKeys.CheckpointPurpose), new SeededRandom(8));

    protected override MediumHost CreateHost() => new SimulatedHost();

    protected override IRecordCipher? Cipher => _cipher;

    [Fact]
    public async Task StoredBytes_DoNotContainPlaintext_AndKeyIsRequired()
    {
        byte[] marker = System.Text.Encoding.ASCII.GetBytes("CHECKPOINT-PLAINTEXT-MARKER-42");
        CheckpointStore store = await OpenAsync();
        CheckpointInfo info;
        await using (CheckpointWriter writer = await store.BeginWriteAsync(1, 1, marker))
        {
            await writer.WriteAsync(marker);
            info = await writer.CompleteAsync();
        }

        byte[] raw = Host.ReadRaw(CheckpointTestData.Dir + "/" + StorageFormatName(info.Sequence));
        Assert.Equal(-1, raw.AsSpan().IndexOf(marker));
        CheckpointStore plain = await CheckpointStore.OpenAsync(Medium, CheckpointTestData.Dir, new CheckpointStoreOptions());
        await Assert.ThrowsAsync<DfsRecoveryKeyMissingException>(async () => await plain.OpenLatestAsync());
    }
}

public sealed class CheckpointStoreFileSystemTests : CheckpointStoreContractTests
{
    protected override MediumHost CreateHost() => new FileSystemHost();
}
