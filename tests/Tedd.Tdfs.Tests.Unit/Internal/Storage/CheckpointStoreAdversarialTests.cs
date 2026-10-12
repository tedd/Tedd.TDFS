using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Tests.Unit.Internal.Storage.Infrastructure;

namespace Tedd.Tdfs.Tests.Unit.Internal.Storage;

/// <summary>Adversarial checkpoint-store tests: failed barriers that are retried, writes that fail midway, and races
/// between readers and installs.</summary>
public sealed class CheckpointStoreAdversarialTests
{
    private const string Dir = CheckpointTestData.Dir;

    private static SimulatedMedium NewMedium() => new("sim", 1L << 34, new SeededRandom(21));

    /// <summary>
    /// The checkpoint's flush barrier fails and the device drops the dirty pages (reporting the error once). Retrying
    /// <see cref="CheckpointWriter.CompleteAsync"/> must not succeed: the retried flush would only persist the trailer and
    /// install a checkpoint whose chunks never reached the device, which no later restore could read.
    /// </summary>
    [Fact]
    public async Task CompleteRetriedAfterAFailedBarrier_NeverInstallsALostCheckpoint()
    {
        SimulatedMedium sim = NewMedium();
        var medium = new LossyFlushMedium(sim);
        CheckpointStore store = await CheckpointStore.OpenAsync(medium, Dir, new CheckpointStoreOptions());
        CheckpointInfo first = await CheckpointTestData.WriteAsync(store, 10, 1, seed: 1);

        CheckpointWriter writer = await store.BeginWriteAsync(20, 2, CheckpointTestData.Metadata(2));
        foreach (byte[] chunk in CheckpointTestData.Chunks(2, 4))
            await writer.WriteAsync(chunk);
        medium.FailNextFlushes = 1;
        await Assert.ThrowsAnyAsync<IOException>(async () => await writer.CompleteAsync());

        bool installed;
        try
        {
            await writer.CompleteAsync();
            installed = true;
        }
        catch (InvalidOperationException)
        {
            installed = false;
        }

        await writer.DisposeAsync();
        if (installed)
        {
            await using CheckpointReader? reader = await store.OpenLatestAsync();
        }

        Assert.False(installed, "a checkpoint whose barrier failed was installed by a retry");
        Assert.Equal(first, store.Current);
        await using (CheckpointReader reader = (await store.OpenLatestAsync())!)
            await CheckpointTestData.AssertContentAsync(reader, 10, 1, 1);

        sim.Crash(CrashMode.LoseUnflushed);
        CheckpointStore reopened = await CheckpointStore.OpenAsync(sim, Dir, new CheckpointStoreOptions());
        await using CheckpointReader again = (await reopened.OpenLatestAsync())!;
        await CheckpointTestData.AssertContentAsync(again, 10, 1, 1);
    }

    /// <summary>
    /// A chunk write fails partway (the device is full). The writer must not go on to complete a checkpoint around the
    /// failed chunk (silently missing content, or with the failed bytes left behind its trailer).
    /// </summary>
    [Fact]
    public async Task FailedChunkWrite_FaultsTheWriter()
    {
        SimulatedMedium medium = NewMedium();
        CheckpointStore store = await CheckpointStore.OpenAsync(medium, Dir, new CheckpointStoreOptions());
        CheckpointInfo first = await CheckpointTestData.WriteAsync(store, 10, 1, seed: 1);

        await using (CheckpointWriter writer = await store.BeginWriteAsync(20, 2, CheckpointTestData.Metadata(2)))
        {
            await writer.WriteAsync(new byte[1000]);
            medium.Faults.ShortNextWrites = 1;
            medium.Faults.FailNextWrites = 0;
            medium.Faults.CapacityOverride = medium.UsedBytes + 600; // the next chunk only half fits
            await Assert.ThrowsAsync<DfsCapacityException>(async () => await writer.WriteAsync(new byte[1000]));
            medium.Faults.CapacityOverride = null;
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await writer.WriteAsync(new byte[10]));
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await writer.CompleteAsync());
        }

        Assert.Equal(first, store.Current);
        Assert.Single(await medium.ListAsync(Dir), f => f.EndsWith(CheckpointStore.FileExtension, StringComparison.Ordinal));
        await using CheckpointReader reader = (await store.OpenLatestAsync())!;
        await CheckpointTestData.AssertContentAsync(reader, 10, 1, 1);
    }

    /// <summary>
    /// A newer checkpoint is installed (and the superseded file deleted) while a reader is opening the current one.
    /// The reader must get a valid checkpoint, never a false "checkpoint file is missing" integrity failure.
    /// </summary>
    [Fact]
    public async Task OpenLatestRacingWithAnInstall_ReturnsAValidCheckpoint()
    {
        SimulatedMedium sim = NewMedium();
        var medium = new OpenHookMedium(sim);
        CheckpointStore store = await CheckpointStore.OpenAsync(medium, Dir, new CheckpointStoreOptions());
        CheckpointInfo first = await CheckpointTestData.WriteAsync(store, 10, 1, seed: 1);
        string firstPath = Dir + "/" + first.Sequence.ToString("x16", System.Globalization.CultureInfo.InvariantCulture) + CheckpointStore.FileExtension;

        medium.BeforeOpen = (firstPath, async () => await CheckpointTestData.WriteAsync(store, 20, 2, seed: 2));
        await using CheckpointReader? reader = await store.OpenLatestAsync();
        Assert.Null(medium.BeforeOpen);
        Assert.NotNull(reader);
        await CheckpointTestData.AssertContentAsync(reader, 20, 2, 2);
    }

    /// <summary>Runs a hook (once) before a matching file is opened, so tests can interleave work at that point.</summary>
    private sealed class OpenHookMedium(SimulatedMedium inner) : IStorageMedium
    {
        public (string Path, Func<Task> Action)? BeforeOpen { get; set; }

        public string Name => inner.Name;

        public long CapacityBytes => inner.CapacityBytes;

        public long UsedBytes => inner.UsedBytes;

        public async ValueTask<IStorageFile> OpenAsync(string path, StorageOpenMode mode, CancellationToken ct = default)
        {
            if (BeforeOpen is { } hook && hook.Path == StoragePath.Normalize(path))
            {
                BeforeOpen = null;
                await hook.Action();
            }

            return await inner.OpenAsync(path, mode, ct);
        }

        public ValueTask<bool> ExistsAsync(string path, CancellationToken ct = default) => inner.ExistsAsync(path, ct);

        public ValueTask DeleteAsync(string path, CancellationToken ct = default) => inner.DeleteAsync(path, ct);

        public ValueTask RenameAsync(string from, string to, bool overwrite, CancellationToken ct = default) => inner.RenameAsync(from, to, overwrite, ct);

        public ValueTask<IReadOnlyList<string>> ListAsync(string directory, CancellationToken ct = default) => inner.ListAsync(directory, ct);

        public ValueTask CreateDirectoryAsync(string directory, CancellationToken ct = default) => inner.CreateDirectoryAsync(directory, ct);

        public ValueTask FlushDirectoryAsync(string directory, CancellationToken ct = default) => inner.FlushDirectoryAsync(directory, ct);
    }
}
