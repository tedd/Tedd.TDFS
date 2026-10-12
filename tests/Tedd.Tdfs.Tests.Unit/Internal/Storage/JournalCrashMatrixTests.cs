using System.Globalization;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Tests.Unit.Internal.Storage.Infrastructure;

namespace Tedd.Tdfs.Tests.Unit.Internal.Storage;

/// <summary>
/// Crash matrices for the journal (DSK-001/002/013): the medium is crashed before every write, flush, length change
/// and namespace step of each operation, with every crash mode; recovery must yield an allowed complete prefix and
/// never lose an acknowledged record.
/// </summary>
public sealed class JournalCrashMatrixTests
{
    private const string Dir = JournalTestData.Dir;

    private static string Fingerprint(Journal journal)
        => string.Join(",", journal.Records.Select(r => r.Index.ToString(CultureInfo.InvariantCulture) + ":" + Convert.ToHexString(r.Payload.Span)))
            + "|next=" + journal.NextIndex.ToString(CultureInfo.InvariantCulture);

    [Theory]
    [InlineData(1 << 20)]
    [InlineData(250)]
    public async Task AppendFlush_RecoversDurablePrefixPlusPrefixOfBatch(long segmentSize)
    {
        var options = new JournalOptions(segmentSize);
        long acknowledged = 0;
        int cases = await CrashMatrix.RunAsync(
            async medium =>
            {
                acknowledged = 10;
                await using Journal journal = await Journal.OpenAsync(medium, Dir, options);
                await JournalTestData.AppendRangeAsync(journal, 1, 10);
                await journal.FlushAsync();
            },
            async medium =>
            {
                Journal journal = await Journal.OpenAsync(medium, Dir, options);
                await JournalTestData.AppendRangeAsync(journal, 11, 16);
                await journal.FlushAsync();
                acknowledged = 16;
                await JournalTestData.AppendRangeAsync(journal, 17, 20);
                await journal.FlushAsync();
                acknowledged = 20;
            },
            async (medium, crash) =>
            {
                await using Journal journal = await Journal.OpenAsync(medium, Dir, options);
                long last = journal.Records.Count == 0 ? 0 : journal.Records[^1].Index;
                Assert.InRange(last, acknowledged, 20);
                JournalTestData.AssertRecords(journal.Records, 1, last);
                Assert.Equal(last + 1, journal.NextIndex);
                return Fingerprint(journal);
            });
        Assert.True(cases > 30, $"only {cases} cases");
    }

    [Fact]
    public async Task AppendFlushIntoFreshJournal_RecoversPrefix()
    {
        long acknowledged = 0;
        await CrashMatrix.RunAsync(
            _ =>
            {
                acknowledged = 0;
                return Task.CompletedTask;
            },
            async medium =>
            {
                Journal journal = await Journal.OpenAsync(medium, Dir, new JournalOptions(200));
                await JournalTestData.AppendRangeAsync(journal, 1, 6);
                await journal.FlushAsync();
                acknowledged = 6;
            },
            async (medium, crash) =>
            {
                await using Journal journal = await Journal.OpenAsync(medium, Dir, new JournalOptions(200));
                long last = journal.Records.Count;
                Assert.InRange(last, acknowledged, 6);
                JournalTestData.AssertRecords(journal.Records, 1, last);
                return Fingerprint(journal);
            });
    }

    [Theory]
    [InlineData(1 << 20, 12)]
    [InlineData(250, 12)]
    [InlineData(250, 3)]
    public async Task TruncateAfterThenAppend_NeverMixesOldAndNewSuffix(long segmentSize, long keep)
    {
        var options = new JournalOptions(segmentSize);
        bool truncated = false;
        long acknowledgedNew = keep;
        await CrashMatrix.RunAsync(
            async medium =>
            {
                truncated = false;
                acknowledgedNew = keep;
                await using Journal journal = await Journal.OpenAsync(medium, Dir, options);
                await JournalTestData.AppendRangeAsync(journal, 1, 20);
                await journal.FlushAsync();
            },
            async medium =>
            {
                Journal journal = await Journal.OpenAsync(medium, Dir, options);
                await journal.TruncateAfterAsync(keep);
                truncated = true;
                await JournalTestData.AppendRangeAsync(journal, keep + 1, keep + 4, salt: 99);
                await journal.FlushAsync();
                acknowledgedNew = keep + 4;
            },
            async (medium, crash) =>
            {
                await using Journal journal = await Journal.OpenAsync(medium, Dir, options);
                IReadOnlyList<JournalRecord> records = journal.Records;
                Assert.True(records.Count >= keep, "records at or below the truncation point were lost");
                JournalTestData.AssertRecords(records.Take((int)keep).ToList(), 1, keep);
                var suffix = records.Skip((int)keep).ToList();
                bool isOld = suffix.Count > 0 && suffix[0].Payload.Span.SequenceEqual(JournalTestData.Payload(keep + 1));
                if (isOld)
                {
                    Assert.False(truncated, "old records reappeared after an acknowledged truncation");
                    JournalTestData.AssertRecords(suffix, keep + 1, keep + suffix.Count);
                }
                else
                {
                    Assert.InRange(keep + suffix.Count, acknowledgedNew, keep + 4);
                    JournalTestData.AssertRecords(suffix, keep + 1, keep + suffix.Count, salt: 99);
                }

                return Fingerprint(journal);
            });
    }

    [Fact]
    public async Task DropBefore_LeavesContiguousSuffix()
    {
        var options = new JournalOptions(250);
        bool dropped = false;
        await CrashMatrix.RunAsync(
            async medium =>
            {
                dropped = false;
                await using Journal journal = await Journal.OpenAsync(medium, Dir, options);
                await JournalTestData.AppendRangeAsync(journal, 1, 40);
                await journal.FlushAsync();
            },
            async medium =>
            {
                Journal journal = await Journal.OpenAsync(medium, Dir, options);
                await journal.DropBeforeAsync(25);
                dropped = true;
            },
            async (medium, crash) =>
            {
                await using Journal journal = await Journal.OpenAsync(medium, Dir, options);
                long first = journal.FirstIndex;
                Assert.InRange(first, 1, 25);
                if (dropped)
                    Assert.True(first > 1);
                JournalTestData.AssertRecords(journal.Records, first, 40);
                return Fingerprint(journal);
            });
    }

    [Fact]
    public async Task DropBeforeBeyondLast_ResetsOrKeepsOldSuffix()
    {
        bool dropped = false;
        const long Reset = 1000;
        await CrashMatrix.RunAsync(
            async medium =>
            {
                dropped = false;
                await using Journal journal = await Journal.OpenAsync(medium, Dir, new JournalOptions(250));
                await JournalTestData.AppendRangeAsync(journal, 1, 30);
                await journal.FlushAsync();
            },
            async medium =>
            {
                Journal journal = await Journal.OpenAsync(medium, Dir, new JournalOptions(250));
                await journal.DropBeforeAsync(Reset);
                dropped = true;
                Assert.Equal(Reset, await journal.AppendAsync(new byte[] { 42 }));
                await journal.FlushAsync();
            },
            async (medium, crash) =>
            {
                // The caller (for example the Raft log store) durably recorded the new start before dropping.
                await using Journal journal = await Journal.OpenAsync(medium, Dir, new JournalOptions(250) { FirstIndex = Reset });
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
    public async Task CleanClose_MarkerWriteIsCrashSafe()
    {
        long acknowledged = 0;
        await CrashMatrix.RunAsync(
            _ =>
            {
                acknowledged = 0;
                return Task.CompletedTask;
            },
            async medium =>
            {
                Journal journal = await Journal.OpenAsync(medium, Dir, new JournalOptions());
                await JournalTestData.AppendRangeAsync(journal, 1, 4);
                await journal.FlushAsync();
                acknowledged = 4;
                await journal.DisposeAsync();
            },
            async (medium, crash) =>
            {
                await using Journal journal = await Journal.OpenAsync(medium, Dir, new JournalOptions());
                Assert.InRange(journal.Records.Count, acknowledged, 4);
                JournalTestData.AssertRecords(journal.Records, 1, journal.Records.Count);
                return Fingerprint(journal);
            });
    }

    [Fact]
    public async Task EncryptedAppendFlush_RecoversPrefix()
    {
        byte[] secret = TestData.Bytes(5, 32);
        long acknowledged = 0;
        await CrashMatrix.RunAsync(
            async medium =>
            {
                acknowledged = 3;
                using var cipher = new RecordCipher(KeyRing.Derive(secret, StorageKeys.JournalPurpose), new SeededRandom(1));
                await using Journal journal = await Journal.OpenAsync(medium, Dir, new JournalOptions(Cipher: cipher));
                await JournalTestData.AppendRangeAsync(journal, 1, 3);
            },
            async medium =>
            {
                var cipher = new RecordCipher(KeyRing.Derive(secret, StorageKeys.JournalPurpose), new SeededRandom(2));
                Journal journal = await Journal.OpenAsync(medium, Dir, new JournalOptions(Cipher: cipher));
                await JournalTestData.AppendRangeAsync(journal, 4, 8);
                await journal.FlushAsync();
                acknowledged = 8;
            },
            async (medium, crash) =>
            {
                using var cipher = new RecordCipher(KeyRing.Derive(secret, StorageKeys.JournalPurpose), new SeededRandom(3));
                await using Journal journal = await Journal.OpenAsync(medium, Dir, new JournalOptions(Cipher: cipher));
                long last = journal.Records.Count;
                Assert.InRange(last, acknowledged, 8);
                JournalTestData.AssertRecords(journal.Records, 1, last);
                return Fingerprint(journal);
            },
            seedsPerRandomMode: 2);
    }
}
