using System.Buffers.Binary;
using System.Globalization;
using Tedd.Tdfs.Data;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Tests.Unit.Internal.Storage.Infrastructure;

namespace Tedd.Tdfs.Tests.Unit.Internal.Storage;

/// <summary>Adversarial journal tests: crash windows inside multi-step operations and malformed segment sets.</summary>
public sealed class JournalAdversarialTests
{
    private const string Dir = JournalTestData.Dir;

    private static string Fingerprint(Journal journal)
        => string.Join(",", journal.Records.Select(r => r.Index.ToString(CultureInfo.InvariantCulture) + ":" + Convert.ToHexString(r.Payload.Span)))
            + "|first=" + journal.FirstIndex.ToString(CultureInfo.InvariantCulture)
            + "|next=" + journal.NextIndex.ToString(CultureInfo.InvariantCulture);

    /// <summary>
    /// Dropping past the last record must be crash-atomic on its own: a crash may leave the old records or the new
    /// empty journal continuing at the requested index, but never an empty journal that restarts at
    /// <see cref="JournalOptions.FirstIndex"/> (which would reuse indexes that were already handed out).
    /// </summary>
    [Theory]
    [InlineData(250)]
    [InlineData(1 << 20)]
    public async Task DropBeforeBeyondLast_CrashNeverForgetsTheNewStart(long segmentSize)
    {
        bool dropped = false;
        const long Reset = 1000;
        var options = new JournalOptions(segmentSize);
        await CrashMatrix.RunAsync(
            async medium =>
            {
                dropped = false;
                await using Journal journal = await Journal.OpenAsync(medium, Dir, options);
                await JournalTestData.AppendRangeAsync(journal, 1, 30);
                await journal.FlushAsync();
            },
            async medium =>
            {
                Journal journal = await Journal.OpenAsync(medium, Dir, options);
                await journal.DropBeforeAsync(Reset);
                dropped = true;
                Assert.Equal(Reset, await journal.AppendAsync(new byte[] { 42 }));
                await journal.FlushAsync();
            },
            async (medium, crash) =>
            {
                // Default options: nothing outside the journal remembers the new start.
                await using Journal journal = await Journal.OpenAsync(medium, Dir, options);
                if (journal.Records.Count > 0 && journal.Records[0].Index < Reset)
                {
                    Assert.False(dropped, "dropped records reappeared");
                    JournalTestData.AssertRecords(journal.Records, journal.FirstIndex, 30);
                }
                else
                {
                    Assert.Equal(Reset, journal.FirstIndex);
                    Assert.True(journal.Records.Count <= 1);
                    if (crash.Completed)
                        Assert.Single(journal.Records);
                }

                return Fingerprint(journal);
            });
    }

    [Fact]
    public async Task DropBeforeBeyondLast_ThenRollOverAndReopen_KeepsOnlyNewRecords()
    {
        var medium = new SimulatedMedium("sim", 1L << 34, new SeededRandom(3));
        var options = new JournalOptions(250);
        await using (Journal journal = await Journal.OpenAsync(medium, Dir, options))
        {
            await JournalTestData.AppendRangeAsync(journal, 1, 30);
            await journal.FlushAsync();
            await journal.DropBeforeAsync(500);
            await JournalTestData.AppendRangeAsync(journal, 500, 530);
            await journal.FlushAsync();
        }

        medium.Crash(CrashMode.LoseUnflushed);
        await using Journal reopened = await Journal.OpenAsync(medium, Dir, options);
        Assert.Equal(500, reopened.FirstIndex);
        JournalTestData.AssertRecords(reopened.Records, 500, 530);
    }

    [Fact]
    public async Task ResetSegmentFollowedByLeftoverOlderSegments_OlderAreDiscardedDurably()
    {
        // Simulates the crash window of a drop past the end: the new (reset) segment exists and some of the old
        // segments were not deleted yet.
        var medium = new SimulatedMedium("sim", 1L << 34, new SeededRandom(3));
        var options = new JournalOptions(250);
        await using (Journal journal = await Journal.OpenAsync(medium, Dir, options))
        {
            await JournalTestData.AppendRangeAsync(journal, 1, 30);
            await journal.FlushAsync();
        }

        IReadOnlyList<string> before = await medium.ListAsync(Dir);
        var saved = before.ToDictionary(n => n, n => medium.ReadDurable(Dir + "/" + n));
        await using (Journal journal = await Journal.OpenAsync(medium, Dir, options))
            await journal.DropBeforeAsync(700);

        // Put the old segments back (as if their deletion never reached the disk).
        foreach ((string name, byte[] bytes) in saved)
        {
            await using IStorageFile file = await medium.OpenAsync(Dir + "/" + name, StorageOpenMode.CreateNew);
            await file.WriteFullyAsync(0, bytes);
            await file.FlushAsync();
        }

        await medium.FlushDirectoryAsync(Dir);
        await using (Journal reopened = await Journal.OpenAsync(medium, Dir, options))
        {
            Assert.Equal(700, reopened.FirstIndex);
            Assert.Equal(700, reopened.NextIndex);
            Assert.Empty(reopened.Records);
        }

        medium.Crash(CrashMode.LoseUnflushed);
        Assert.All(saved.Keys, name => Assert.DoesNotContain(name, medium.AllFiles().Select(StoragePathName)));
        await using Journal again = await Journal.OpenAsync(medium, Dir, options);
        Assert.Equal(700, again.FirstIndex);
    }

    /// <summary>A well-formed plaintext marker record (the on-disk journal format) certifying <paramref name="index"/>.</summary>
    private static byte[] ForgedMarker(long index)
    {
        var record = new byte[Journal.RecordHeaderSize + Journal.TrailerSize];
        BinaryPrimitives.WriteUInt32LittleEndian(record, 0x4C524A54); // "TJRL"
        record[4] = Journal.KindMarker;
        BinaryPrimitives.WriteInt64LittleEndian(record.AsSpan(16), index);
        BinaryPrimitives.WriteInt64LittleEndian(record.AsSpan(24), index);
        BinaryPrimitives.WriteUInt32LittleEndian(record.AsSpan(36), ObjectChecksum.Crc32C(record.AsSpan(0, 36)));
        BinaryPrimitives.WriteUInt32LittleEndian(record.AsSpan(40), ObjectChecksum.Crc32C(record.AsSpan(0, 40)));
        return record;
    }

    /// <summary>Payload controlled by a client (for example inline file data in a Raft entry) that embeds a forged marker.</summary>
    private static byte[] HostilePayload() => [.. new byte[13], .. ForgedMarker(1_000_000), .. new byte[29]];

    private static async Task ReplaceFileAsync(SimulatedMedium medium, string path, byte[] content)
    {
        await using (IStorageFile file = await medium.OpenAsync(path, StorageOpenMode.Truncate))
        {
            await file.WriteFullyAsync(0, content);
            await file.FlushAsync();
        }

        medium.Crash(CrashMode.LoseUnflushed);
    }

    /// <summary>
    /// The last record was torn (power loss before its barrier) and its payload, which a client controls, embeds a
    /// well-formed marker record. Bytes inside a damaged record are not records: recovery must cut the torn tail
    /// instead of treating the embedded marker as proof that the torn record was durable (which stops the group).
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task TornRecordWhosePayloadEmbedsAMarker_IsATornTail(bool cutInsideTrailer)
    {
        var medium = new SimulatedMedium("sim", 1L << 34, new SeededRandom(3));
        byte[] hostile = HostilePayload();
        await using (Journal journal = await Journal.OpenAsync(medium, Dir, new JournalOptions()))
        {
            await JournalTestData.AppendRangeAsync(journal, 1, 3);
            await journal.FlushAsync();
            Assert.Equal(4, await journal.AppendAsync(hostile));
            await journal.FlushAsync();
        }

        string path = JournalTestData.SegmentPath(1);
        byte[] image = medium.ReadDurable(path);
        int payloadAt = image.AsSpan().IndexOf(hostile);
        Assert.True(payloadAt > 0);
        int recordEnd = payloadAt + hostile.Length + Journal.TrailerSize;
        byte[] torn = cutInsideTrailer
            ? image[..(recordEnd - 2)] // the trailer never reached the device
            : image[..recordEnd]; // complete length, but the trailer does not match (the marker after it was lost)
        if (!cutInsideTrailer)
            torn[^1] ^= 0x5A;
        await ReplaceFileAsync(medium, path, torn);

        await using (Journal reopened = await Journal.OpenAsync(medium, Dir, new JournalOptions()))
        {
            JournalTestData.AssertRecords(reopened.Records, 1, 3);
            Assert.Equal(4, reopened.NextIndex);
        }
    }

    /// <summary>
    /// A new segment whose header write was lost, holding only a torn record whose payload embeds a well-formed marker:
    /// nothing in it was acknowledged, so it is a torn creation (rewritten empty), not proven-durable damage.
    /// </summary>
    [Fact]
    public async Task TornNewSegmentWhoseTornRecordEmbedsAMarker_IsATornCreation()
    {
        var medium = new SimulatedMedium("sim", 1L << 34, new SeededRandom(3));
        var options = new JournalOptions(400);
        byte[] hostile = HostilePayload();
        await using (Journal journal = await Journal.OpenAsync(medium, Dir, options))
        {
            await JournalTestData.AppendRangeAsync(journal, 1, 3);
            await journal.FlushAsync();
            Assert.Equal(4, await journal.AppendAsync(new byte[400])); // fills a segment of its own
            await journal.FlushAsync();
            Assert.Equal(5, await journal.AppendAsync(hostile)); // starts the next segment
            await journal.FlushAsync();
        }

        string last = JournalTestData.SegmentPath(5);
        byte[] image = medium.ReadDurable(last);
        int payloadAt = image.AsSpan().IndexOf(hostile);
        Assert.Equal(Journal.SegmentHeaderSize + Journal.RecordHeaderSize, payloadAt);
        byte[] torn = image[..(payloadAt + hostile.Length + 1)];
        Array.Clear(torn, 0, Journal.SegmentHeaderSize); // the header write never reached the device
        await ReplaceFileAsync(medium, last, torn);

        await using Journal reopened = await Journal.OpenAsync(medium, Dir, options);
        JournalTestData.AssertRecords([.. reopened.Records.Where(r => r.Index <= 3)], 1, 3);
        Assert.Equal(5, reopened.NextIndex);
    }

    /// <summary>
    /// Dropping a prefix and truncating a suffix delete several segments. On a file system that may persist unflushed
    /// unlinks in any order, a crash at any step must still leave a contiguous run of segments: a gap would make every
    /// later open fail although nothing acknowledged was lost.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task SegmentDeletions_SurviveFileSystemsThatReorderUnlinks(bool dropPrefix)
    {
        var options = new JournalOptions(250);
        for (int step = 0; ; step++)
        {
            Assert.True(step < 1000, "the operation did not complete");
            var sim = new SimulatedMedium("sim", 1L << 34, new SeededRandom(5));
            await using (Journal journal = await Journal.OpenAsync(sim, Dir, options))
            {
                await JournalTestData.AppendRangeAsync(journal, 1, 40);
                await journal.FlushAsync();
            }

            Assert.True((await sim.ListAsync(Dir)).Count(n => n.EndsWith(Journal.SegmentExtension, StringComparison.Ordinal)) >= 5);
            var medium = new UnorderedUnlinkMedium(sim, step);
            bool done = false;
            try
            {
                Journal journal = await Journal.OpenAsync(medium, Dir, options);
                if (dropPrefix)
                    await journal.DropBeforeAsync(30);
                else
                    await journal.TruncateAfterAsync(8);
                done = true;
                await journal.DisposeAsync();
            }
            catch (Exception) when (medium.Crashed)
            {
            }

            if (!medium.Crashed)
                sim.Crash(CrashMode.LoseUnflushed);

            Journal reopened;
            try
            {
                reopened = await Journal.OpenAsync(sim, Dir, options);
            }
            catch (DfsIntegrityException ex)
            {
                throw new Xunit.Sdk.XunitException($"crash before step {step}: the journal no longer opens: {ex.Message}");
            }

            await using (reopened)
            {
                IReadOnlyList<JournalRecord> records = reopened.Records;
                if (dropPrefix)
                {
                    Assert.Equal(40, records[^1].Index);
                    Assert.InRange(records[0].Index, 1, 30);
                    JournalTestData.AssertRecords(records, records[0].Index, 40);
                    if (done)
                        Assert.True(records[0].Index > 1);
                }
                else
                {
                    Assert.Equal(1, records[0].Index);
                    Assert.InRange(records[^1].Index, 8, 40);
                    JournalTestData.AssertRecords(records, 1, records[^1].Index);
                    if (done)
                        Assert.Equal(8, records[^1].Index);
                }
            }

            if (!medium.Crashed)
                break;
        }
    }

    private static string StoragePathName(string path) => path[(path.LastIndexOf('/') + 1)..];
}
