using System.Globalization;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Tests.Unit.Internal.Storage.Infrastructure;

namespace Tedd.Tdfs.Tests.Unit.Internal.Storage;

/// <summary>
/// Crash matrices over the recovery paths themselves: the durable state left by an earlier crash needs repair at open
/// (torn tails, torn segment creation, interrupted drops, committed compactions, pending snapshot installs) and the
/// medium crashes again before every step of that repair and of the work that follows it. Recovery must be
/// idempotent and must never lose what the earlier run acknowledged.
/// </summary>
public sealed class RecoveryCrashMatrixTests
{
    private static string Fingerprint(Journal journal)
        => string.Join(",", journal.Records.Select(r => r.Index.ToString(CultureInfo.InvariantCulture) + ":" + Convert.ToHexString(r.Payload.Span)))
            + "|first=" + journal.FirstIndex.ToString(CultureInfo.InvariantCulture)
            + "|next=" + journal.NextIndex.ToString(CultureInfo.InvariantCulture);

    private static async Task AppendRawAsync(SimulatedMedium medium, string path, byte[] bytes)
    {
        await using (IStorageFile file = await medium.OpenAsync(path, StorageOpenMode.OpenExisting))
        {
            await file.WriteFullyAsync(file.Length, bytes);
            await file.FlushAsync();
        }
    }

    /// <summary>Last segment ends with a torn record and a stale temporary file exists; open repairs, then appends.</summary>
    [Fact]
    public async Task Journal_TornTailRepairThenAppend()
    {
        const string Dir = JournalTestData.Dir;
        var options = new JournalOptions(250);
        long acknowledged = 0;
        await CrashMatrix.RunAsync(
            async medium =>
            {
                acknowledged = 20;
                await using (Journal journal = await Journal.OpenAsync(medium, Dir, options))
                {
                    await JournalTestData.AppendRangeAsync(journal, 1, 20);
                    await journal.FlushAsync();
                }

                IReadOnlyList<string> names = await medium.ListAsync(Dir);
                string last = names.Where(n => n.EndsWith(Journal.SegmentExtension, StringComparison.Ordinal)).Order(StringComparer.Ordinal).Last();

                // A record header (copied from the start of a real record) followed by a few payload bytes: torn.
                byte[] first = medium.ReadDurable(JournalTestData.SegmentPath(1));
                byte[] torn = first.AsSpan(Journal.SegmentHeaderSize, Journal.RecordHeaderSize + 3).ToArray();
                await AppendRawAsync(medium, Dir + "/" + last, torn);
                await using (IStorageFile tmp = await medium.OpenAsync(Dir + "/" + last + ".tmp", StorageOpenMode.CreateNew))
                    await tmp.WriteFullyAsync(0, new byte[] { 1, 2, 3 });
                await medium.FlushDirectoryAsync(Dir);
                medium.Crash(CrashMode.LoseUnflushed);
            },
            async medium =>
            {
                Journal journal = await Journal.OpenAsync(medium, Dir, options);
                await JournalTestData.AppendRangeAsync(journal, 21, 25);
                await journal.FlushAsync();
                acknowledged = 25;
                await journal.DisposeAsync();
            },
            async (medium, crash) =>
            {
                await using Journal journal = await Journal.OpenAsync(medium, Dir, options);
                long last = journal.Records.Count == 0 ? 0 : journal.Records[^1].Index;
                Assert.InRange(last, acknowledged, 25);
                JournalTestData.AssertRecords(journal.Records, 1, last);
                Assert.DoesNotContain(await medium.ListAsync(Dir), n => n.EndsWith(".tmp", StringComparison.Ordinal));
                return Fingerprint(journal);
            },
            seedsPerRandomMode: 2);
    }

    /// <summary>
    /// A drop past the end was interrupted after its reset segment became durable but before every old segment was
    /// deleted; the next open discards the old segments and continues at the reset index.
    /// </summary>
    [Fact]
    public async Task Journal_InterruptedResetRepairThenAppend()
    {
        const string Dir = JournalTestData.Dir;
        var options = new JournalOptions(250);
        const long Reset = 700;
        long acknowledged = Reset - 1;
        await CrashMatrix.RunAsync(
            async medium =>
            {
                acknowledged = Reset - 1;
                await using (Journal journal = await Journal.OpenAsync(medium, Dir, options))
                {
                    await JournalTestData.AppendRangeAsync(journal, 1, 30);
                    await journal.FlushAsync();
                }

                IReadOnlyList<string> old = await medium.ListAsync(Dir);
                Dictionary<string, byte[]> saved = old.ToDictionary(n => n, n => medium.ReadDurable(Dir + "/" + n));
                await using (Journal journal = await Journal.OpenAsync(medium, Dir, options))
                    await journal.DropBeforeAsync(Reset);

                // Two of the old segments "survive" their deletion.
                foreach ((string name, byte[] bytes) in saved.OrderBy(kv => kv.Key, StringComparer.Ordinal).Skip(1).Take(2))
                {
                    await using IStorageFile file = await medium.OpenAsync(Dir + "/" + name, StorageOpenMode.CreateNew);
                    await file.WriteFullyAsync(0, bytes);
                    await file.FlushAsync();
                }

                await medium.FlushDirectoryAsync(Dir);
                medium.Crash(CrashMode.LoseUnflushed);
            },
            async medium =>
            {
                Journal journal = await Journal.OpenAsync(medium, Dir, options);
                Assert.Equal(Reset, journal.NextIndex);
                await JournalTestData.AppendRangeAsync(journal, Reset, Reset + 5);
                await journal.FlushAsync();
                acknowledged = Reset + 5;
                await journal.DisposeAsync();
            },
            async (medium, crash) =>
            {
                await using Journal journal = await Journal.OpenAsync(medium, Dir, options);
                Assert.Equal(Reset, journal.FirstIndex);
                long last = journal.Records.Count == 0 ? Reset - 1 : journal.Records[^1].Index;
                Assert.InRange(last, acknowledged, Reset + 5);
                JournalTestData.AssertRecords(journal.Records, Reset, last);
                return Fingerprint(journal);
            },
            seedsPerRandomMode: 2);
    }

    /// <summary>The newest segment's header never reached the device (torn creation); open rewrites it, then appends.</summary>
    [Fact]
    public async Task Journal_TornSegmentCreationRepairThenAppend()
    {
        const string Dir = JournalTestData.Dir;
        var options = new JournalOptions(250);
        long acknowledged = 0;
        long lastFirst = 0;
        await CrashMatrix.RunAsync(
            async medium =>
            {
                await using (Journal journal = await Journal.OpenAsync(medium, Dir, options))
                {
                    await JournalTestData.AppendRangeAsync(journal, 1, 12);
                    await journal.FlushAsync();
                }

                string last = (await medium.ListAsync(Dir)).Where(n => n.EndsWith(Journal.SegmentExtension, StringComparison.Ordinal)).Order(StringComparer.Ordinal).Last();
                Assert.True(Journal.TryParseSegmentFileName(last, out lastFirst));
                acknowledged = lastFirst - 1;

                // Everything in the newest segment was unacknowledged and its header is gone.
                byte[] image = medium.ReadDurable(Dir + "/" + last);
                Array.Clear(image, 0, Journal.SegmentHeaderSize);
                image = image[..^5];
                await using (IStorageFile file = await medium.OpenAsync(Dir + "/" + last, StorageOpenMode.Truncate))
                {
                    await file.WriteFullyAsync(0, image);
                    await file.FlushAsync();
                }

                medium.Crash(CrashMode.LoseUnflushed);
            },
            async medium =>
            {
                Journal journal = await Journal.OpenAsync(medium, Dir, options);
                Assert.Equal(lastFirst, journal.NextIndex);
                await JournalTestData.AppendRangeAsync(journal, lastFirst, lastFirst + 4);
                await journal.FlushAsync();
                acknowledged = lastFirst + 4;
                await journal.DisposeAsync();
            },
            async (medium, crash) =>
            {
                await using Journal journal = await Journal.OpenAsync(medium, Dir, options);
                long last = journal.Records.Count == 0 ? 0 : journal.Records[^1].Index;
                Assert.InRange(last, acknowledged, lastFirst + 4);
                JournalTestData.AssertRecords(journal.Records, 1, last);
                return Fingerprint(journal);
            },
            seedsPerRandomMode: 2);
    }

    /// <summary>
    /// A compaction reached its commit point (manifest durable) but its outputs were not renamed yet and no input was
    /// deleted; the next open rolls it forward and then stores more objects.
    /// </summary>
    [Fact]
    public async Task ObjectStore_CommittedCompactionRollForwardThenPut()
    {
        const string Dir = ObjectTestData.Dir;
        const long SegmentSize = 3000;
        bool put9 = false;
        await CrashMatrix.RunAsync(
            async medium =>
            {
                put9 = false;
                var faults = new RecordingFaults { ThrowAtSite = FaultSites.CompactionDelete };
                await using (ObjectStore store = await ObjectStore.OpenAsync(medium, Dir, new ObjectStoreOptions(SegmentSize, 0), faults))
                {
                    for (int n = 1; n <= 8; n++)
                        await store.PutAsync(ObjectTestData.Id(n), ObjectTestData.VolumeA, ObjectTestData.Staging1, ObjectTestData.Content(n, 1000));
                    await store.DeleteAsync(ObjectTestData.Id(2));
                    await store.DeleteAsync(ObjectTestData.Id(5));
                    await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.CompactAsync());
                }

                // Turn the published outputs back into unpublished ones: the manifest is the commit point.
                foreach (string name in await medium.ListAsync(Dir))
                {
                    if (ObjectStore.TryParseSegmentFileName(name, out ObjectStore.SegmentKey key, out bool compacting) && !compacting && key.Minor > 0)
                        await medium.RenameAsync(Dir + "/" + name, Dir + "/" + ObjectStore.SegmentFileName(key, compacting: true), overwrite: false);
                }

                await medium.FlushDirectoryAsync(Dir);
                Assert.Contains(ObjectStore.ManifestFileName, await medium.ListAsync(Dir));
                medium.Crash(CrashMode.LoseUnflushed);
            },
            async medium =>
            {
                ObjectStore store = await ObjectStore.OpenAsync(medium, Dir, new ObjectStoreOptions(SegmentSize, 0), NoFaults.Instance);
                await store.PutAsync(ObjectTestData.Id(9), ObjectTestData.VolumeA, ObjectTestData.Staging1, ObjectTestData.Content(9, 1000));
                put9 = true;
                await store.DisposeAsync();
            },
            async (medium, crash) =>
            {
                await using ObjectStore store = await ObjectStore.OpenAsync(medium, Dir, new ObjectStoreOptions(SegmentSize, 0), NoFaults.Instance);
                IReadOnlyList<string> names = await medium.ListAsync(Dir);
                Assert.DoesNotContain(ObjectStore.ManifestFileName, names);
                Assert.DoesNotContain(names, n => n.EndsWith(ObjectStore.CompactingExtension, StringComparison.Ordinal));
                foreach (int n in new[] { 1, 3, 4, 6, 7, 8 })
                    Assert.Equal(ObjectTestData.Content(n, 1000), await ObjectTestData.ReadAllAsync(store, ObjectTestData.Id(n)));
                Assert.False(await store.ContainsAsync(ObjectTestData.Id(2)), "deleted object resurrected");
                Assert.False(await store.ContainsAsync(ObjectTestData.Id(5)), "deleted object resurrected");
                if (put9)
                    Assert.True(await store.ContainsAsync(ObjectTestData.Id(9)), "acknowledged object lost");
                if (await store.ContainsAsync(ObjectTestData.Id(9)))
                    Assert.Equal(ObjectTestData.Content(9, 1000), await ObjectTestData.ReadAllAsync(store, ObjectTestData.Id(9)));
                Assert.Empty(store.Quarantined);
                return string.Join(",", store.List().Select(i => i.Id.ToString()));
            },
            seedsPerRandomMode: 2);
    }

    /// <summary>
    /// A snapshot install recorded its metadata (reset pending) and crashed before resetting the log; the next open
    /// finishes the reset, then appends after the snapshot.
    /// </summary>
    [Fact]
    public async Task RaftLog_PendingSnapshotInstallRepairThenAppend()
    {
        const string Dir = RaftTestData.Dir;
        var options = new RaftLogStoreOptions { SegmentSize = 300 };
        byte[] conf = [1, 2, 3];
        bool appended = false;
        await CrashMatrix.RunAsync(
            async medium =>
            {
                appended = false;
                await using (RaftLogStore store = await RaftLogStore.OpenAsync(medium, Dir, options))
                {
                    await store.AppendAsync(RaftTestData.Range(1, 40, 1));
                    await store.FlushAsync();
                    await store.SaveHardStateAsync(new RaftLogHardState(2, RaftTestData.Voter, 10));
                }

                await AtomicFile.WriteAsync(medium, Dir + "/" + RaftLogStore.SnapshotFileName, new RaftLogSnapshotMeta(60, 3, conf).Encode(resetPending: true));
                medium.Crash(CrashMode.LoseUnflushed);
            },
            async medium =>
            {
                RaftLogStore store = await RaftLogStore.OpenAsync(medium, Dir, options);
                Assert.Equal(60ul, store.LastIndex);
                await store.AppendAsync([RaftTestData.Entry(61, 3)]);
                await store.FlushAsync();
                appended = true;
                await store.DisposeAsync();
            },
            async (medium, crash) =>
            {
                await using RaftLogStore store = await RaftLogStore.OpenAsync(medium, Dir, options);
                Assert.Equal(new RaftLogSnapshotMeta(60, 3, conf), store.Snapshot);
                Assert.Equal(61ul, store.FirstIndex);
                Assert.Equal(3ul, store.TermAt(60));
                Assert.InRange(store.LastIndex, appended ? 61ul : 60ul, 61ul);
                if (store.LastIndex == 61)
                    RaftTestData.AssertEntry(RaftTestData.Entry(61, 3), store.Entries(61, 62, 0)[0]);
                Assert.Equal(new RaftLogHardState(2, RaftTestData.Voter, 10), store.HardState);
                return RaftTestData.Fingerprint(store);
            },
            seedsPerRandomMode: 2);
    }
}
