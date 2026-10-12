using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Tests.Unit.Internal.Storage.Infrastructure;

namespace Tedd.Tdfs.Tests.Unit.Internal.Storage;

/// <summary>Helpers shared by the journal tests.</summary>
internal static class JournalTestData
{
    public const string Dir = "groups/g1/log";

    public static byte[] Payload(long index, int salt = 0) => TestData.Bytes((ulong)(index * 7919) + (ulong)salt, 8 + (int)(index % 40));

    public static async Task AppendRangeAsync(Journal journal, long from, long toInclusive, int salt = 0)
    {
        for (long i = from; i <= toInclusive; i++)
            Assert.Equal(i, await journal.AppendAsync(Payload(i, salt)));
    }

    public static void AssertRecords(IReadOnlyList<JournalRecord> records, long from, long toInclusive, int salt = 0)
    {
        Assert.Equal(toInclusive - from + 1, records.Count);
        for (int k = 0; k < records.Count; k++)
        {
            Assert.Equal(from + k, records[k].Index);
            Assert.Equal(Payload(from + k, salt), records[k].Payload.ToArray());
        }
    }

    /// <summary>Offset of entry <paramref name="index"/> in a plaintext segment that starts at <paramref name="segmentFirst"/>.</summary>
    public static long OffsetOf(long segmentFirst, long index)
    {
        long offset = Journal.SegmentHeaderSize;
        for (long i = segmentFirst; i < index; i++)
            offset += Journal.RecordHeaderSize + Payload(i).Length + Journal.TrailerSize;
        return offset;
    }

    public static string SegmentPath(long first) => Dir + "/" + Journal.SegmentFileName(first);
}

public abstract class JournalContractTests : StorageContractTests
{
    private const string Dir = JournalTestData.Dir;

    [Fact]
    public async Task OpenEmpty_StartsAtConfiguredFirstIndex()
    {
        await using (Journal journal = await Journal.OpenAsync(Medium, Dir, new JournalOptions()))
        {
            Assert.Empty(journal.Records);
            Assert.Equal(1, journal.FirstIndex);
            Assert.Equal(0, journal.LastIndex);
            Assert.Equal(1, await journal.AppendAsync(new byte[] { 1 }));
        }

        await using (Journal journal = await Journal.OpenAsync(Host.Restart(), "other", new JournalOptions { FirstIndex = 100 }))
        {
            Assert.Equal(100, journal.FirstIndex);
            Assert.Equal(100, await journal.AppendAsync(new byte[] { 1 }));
            Assert.Equal(101, await journal.AppendAsync(ReadOnlyMemory<byte>.Empty));
        }
    }

    [Fact]
    public async Task AppendFlushReopen_ReplaysRecords()
    {
        await using (Journal journal = await Journal.OpenAsync(Medium, Dir, new JournalOptions()))
        {
            await JournalTestData.AppendRangeAsync(journal, 1, 50);
            Assert.Equal(0, journal.DurableIndex);
            await journal.FlushAsync();
            Assert.Equal(50, journal.DurableIndex);
        }

        await using (Journal journal = await Journal.OpenAsync(Host.Restart(), Dir, new JournalOptions()))
        {
            JournalTestData.AssertRecords(journal.Records, 1, 50);
            Assert.Equal(51, journal.NextIndex);
            await JournalTestData.AppendRangeAsync(journal, 51, 60);
            await journal.FlushAsync();
        }

        await using (Journal journal = await Journal.OpenAsync(Host.Restart(), Dir, new JournalOptions()))
            JournalTestData.AssertRecords(journal.Records, 1, 60);
    }

    [Fact]
    public async Task FirstIndexOption_IsIgnoredWhenSegmentsExist()
    {
        await using (Journal journal = await Journal.OpenAsync(Medium, Dir, new JournalOptions { FirstIndex = 5 }))
            await JournalTestData.AppendRangeAsync(journal, 5, 9);
        await using Journal reopened = await Journal.OpenAsync(Host.Restart(), Dir, new JournalOptions { FirstIndex = 1 });
        Assert.Equal(5, reopened.FirstIndex);
        JournalTestData.AssertRecords(reopened.Records, 5, 9);
        Assert.Equal(10, reopened.NextIndex);
    }

    [Fact]
    public async Task Dispose_FlushesPendingRecords()
    {
        await using (Journal journal = await Journal.OpenAsync(Medium, Dir, new JournalOptions()))
            await JournalTestData.AppendRangeAsync(journal, 1, 5);
        await using Journal reopened = await Journal.OpenAsync(Host.Restart(), Dir, new JournalOptions());
        JournalTestData.AssertRecords(reopened.Records, 1, 5);
    }

    [Fact]
    public async Task SmallSegments_RollOverAndReplay()
    {
        var options = new JournalOptions(SegmentSize: 300);
        await using (Journal journal = await Journal.OpenAsync(Medium, Dir, options))
        {
            for (long i = 1; i <= 100; i += 7)
            {
                await JournalTestData.AppendRangeAsync(journal, i, Math.Min(100, i + 6));
                await journal.FlushAsync();
            }
        }

        IReadOnlyList<string> files = await Medium.ListAsync(Dir);
        Assert.True(files.Count > 5, $"expected several segments, got {files.Count}");
        await using Journal reopened = await Journal.OpenAsync(Host.Restart(), Dir, options);
        JournalTestData.AssertRecords(reopened.Records, 1, 100);
    }

    [Fact]
    public async Task RecordLargerThanSegment_IsAccepted()
    {
        var options = new JournalOptions(SegmentSize: 100);
        byte[] big = TestData.Bytes(5, 5000);
        await using (Journal journal = await Journal.OpenAsync(Medium, Dir, options))
        {
            await journal.AppendAsync(big);
            await journal.AppendAsync(big);
            await journal.FlushAsync();
        }

        await using Journal reopened = await Journal.OpenAsync(Host.Restart(), Dir, options);
        Assert.Equal(2, reopened.Records.Count);
        Assert.Equal(big, reopened.Records[1].Payload.ToArray());
    }

    [Fact]
    public async Task OversizedRecord_IsRejected()
    {
        await using Journal journal = await Journal.OpenAsync(Medium, Dir, new JournalOptions { MaxRecordSize = 10 });
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await journal.AppendAsync(new byte[11]));
        Assert.Equal(1, await journal.AppendAsync(new byte[10]));
    }

    [Theory]
    [InlineData(25)]
    [InlineData(1)]
    [InlineData(0)]
    [InlineData(39)]
    public async Task TruncateAfter_RemovesSuffixDurably(long keep)
    {
        var options = new JournalOptions(SegmentSize: 400);
        await using (Journal journal = await Journal.OpenAsync(Medium, Dir, options))
        {
            await JournalTestData.AppendRangeAsync(journal, 1, 40);
            await journal.FlushAsync();
            await journal.TruncateAfterAsync(keep);
            Assert.Equal(keep, journal.LastIndex);
            await JournalTestData.AppendRangeAsync(journal, keep + 1, keep + 3, salt: 1);
            await journal.FlushAsync();
        }

        await using Journal reopened = await Journal.OpenAsync(Host.Restart(), Dir, options);
        Assert.Equal(keep + 3, reopened.Records.Count);
        JournalTestData.AssertRecords(reopened.Records.Take((int)keep).ToList(), 1, keep);
        JournalTestData.AssertRecords(reopened.Records.Skip((int)keep).ToList(), keep + 1, keep + 3, salt: 1);
    }

    [Fact]
    public async Task TruncateAfter_PendingOnly_DropsBufferedRecords()
    {
        await using (Journal journal = await Journal.OpenAsync(Medium, Dir, new JournalOptions()))
        {
            await JournalTestData.AppendRangeAsync(journal, 1, 5);
            await journal.FlushAsync();
            await JournalTestData.AppendRangeAsync(journal, 6, 10);
            await journal.TruncateAfterAsync(7);
            Assert.Equal(7, journal.LastIndex);
            Assert.Equal(8, await journal.AppendAsync(JournalTestData.Payload(8, 3)));
        }

        await using Journal reopened = await Journal.OpenAsync(Host.Restart(), Dir, new JournalOptions());
        Assert.Equal(8, reopened.Records.Count);
        Assert.Equal(JournalTestData.Payload(8, 3), reopened.Records[7].Payload.ToArray());
    }

    [Fact]
    public async Task TruncateAfter_BelowFirstIndex_Throws()
    {
        await using Journal journal = await Journal.OpenAsync(Medium, Dir, new JournalOptions { FirstIndex = 10 });
        await JournalTestData.AppendRangeAsync(journal, 10, 12);
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await journal.TruncateAfterAsync(8));
        await journal.TruncateAfterAsync(9);
        Assert.Equal(9, journal.LastIndex);
        Assert.Equal(10, await journal.AppendAsync(new byte[] { 7 }));
    }

    [Fact]
    public async Task DropBefore_DeletesWholeSegmentsOnly()
    {
        var options = new JournalOptions(SegmentSize: 300);
        long first;
        await using (Journal journal = await Journal.OpenAsync(Medium, Dir, options))
        {
            await JournalTestData.AppendRangeAsync(journal, 1, 60);
            await journal.FlushAsync();
            int before = (await Medium.ListAsync(Dir)).Count;
            await journal.DropBeforeAsync(30);
            first = journal.FirstIndex;
            Assert.InRange(first, 2, 30);
            Assert.True((await Medium.ListAsync(Dir)).Count < before);
            Assert.Equal(60, journal.LastIndex);
        }

        await using Journal reopened = await Journal.OpenAsync(Host.Restart(), Dir, options);
        Assert.Equal(first, reopened.FirstIndex);
        JournalTestData.AssertRecords(reopened.Records, first, 60);
    }

    [Fact]
    public async Task DropBefore_BeyondLast_EmptiesAndContinuesThere()
    {
        await using (Journal journal = await Journal.OpenAsync(Medium, Dir, new JournalOptions(SegmentSize: 300)))
        {
            await JournalTestData.AppendRangeAsync(journal, 1, 20);
            await journal.DropBeforeAsync(500);
            Assert.Equal(500, journal.FirstIndex);
            Assert.Equal(499, journal.LastIndex);
        }

        await using (Journal journal = await Journal.OpenAsync(Host.Restart(), Dir, new JournalOptions(SegmentSize: 300)))
        {
            Assert.Empty(journal.Records);
            Assert.Equal(500, journal.NextIndex);
            Assert.Equal(500, await journal.AppendAsync(new byte[] { 1 }));
            await journal.FlushAsync();
        }

        await using Journal last = await Journal.OpenAsync(Host.Restart(), Dir, new JournalOptions());
        Assert.Single(last.Records);
        Assert.Equal(500, last.Records[0].Index);
    }

    [Fact]
    public async Task DropBefore_AtOrBelowFirst_IsNoOp()
    {
        await using Journal journal = await Journal.OpenAsync(Medium, Dir, new JournalOptions());
        await JournalTestData.AppendRangeAsync(journal, 1, 3);
        await journal.DropBeforeAsync(1);
        await journal.DropBeforeAsync(-5);
        Assert.Equal(1, journal.FirstIndex);
    }

    [Fact]
    public async Task Encrypted_RoundTrips_AndHidesPlaintext()
    {
        using var cipher = new RecordCipher(KeyRing.Derive(TestData.Bytes(1, 32), StorageKeys.JournalPurpose), new SeededRandom(9));
        var options = new JournalOptions(Cipher: cipher);
        byte[] marker = System.Text.Encoding.ASCII.GetBytes("PLAINTEXT-MARKER-0123456789");
        await using (Journal journal = await Journal.OpenAsync(Medium, Dir, options))
        {
            await journal.AppendAsync(marker);
            await JournalTestData.AppendRangeAsync(journal, 2, 10);
        }

        byte[] raw = Host.ReadRaw(JournalTestData.SegmentPath(1));
        Assert.Equal(-1, raw.AsSpan().IndexOf(marker));

        await using (Journal journal = await Journal.OpenAsync(Host.Restart(), Dir, options))
        {
            Assert.Equal(marker, journal.Records[0].Payload.ToArray());
            JournalTestData.AssertRecords(journal.Records.Skip(1).ToList(), 2, 10);
        }

        await Assert.ThrowsAsync<DfsRecoveryKeyMissingException>(async () => await Journal.OpenAsync(Host.Restart(), Dir, new JournalOptions()));
        using var wrong = new RecordCipher(KeyRing.Derive(TestData.Bytes(2, 32), StorageKeys.JournalPurpose), new SeededRandom(9));
        await Assert.ThrowsAsync<DfsIntegrityException>(async () => await Journal.OpenAsync(Host.Restart(), Dir, new JournalOptions(Cipher: wrong)));
    }

    [Fact]
    public async Task CorruptRecordInSealedSegment_ThrowsIntegrity()
    {
        var options = new JournalOptions(SegmentSize: 300);
        await using (Journal journal = await Journal.OpenAsync(Medium, Dir, options))
        {
            await JournalTestData.AppendRangeAsync(journal, 1, 40);
            await journal.FlushAsync();
        }

        Host.Corrupt(JournalTestData.SegmentPath(1), JournalTestData.OffsetOf(1, 2) + Journal.RecordHeaderSize + 1);
        await Assert.ThrowsAsync<DfsIntegrityException>(async () => await Journal.OpenAsync(Host.Restart(), Dir, options));
    }

    [Fact]
    public async Task CorruptRecordFollowedByLaterBatch_ThrowsIntegrity()
    {
        await using (Journal journal = await Journal.OpenAsync(Medium, Dir, new JournalOptions()))
        {
            await JournalTestData.AppendRangeAsync(journal, 1, 5);
            await journal.FlushAsync();
            await JournalTestData.AppendRangeAsync(journal, 6, 10);
            await journal.FlushAsync();
        }

        Host.Corrupt(JournalTestData.SegmentPath(1), JournalTestData.OffsetOf(1, 3) + 20);
        await Assert.ThrowsAsync<DfsIntegrityException>(async () => await Journal.OpenAsync(Host.Restart(), Dir, new JournalOptions()));
    }

    [Fact]
    public async Task CorruptLastRecordAfterCleanClose_ThrowsIntegrity()
    {
        await using (Journal journal = await Journal.OpenAsync(Medium, Dir, new JournalOptions()))
        {
            await JournalTestData.AppendRangeAsync(journal, 1, 5);
            await journal.FlushAsync();
        }

        // The clean-close marker proves record 5 was durable, so damage to it is corruption, not a torn write.
        Host.Corrupt(JournalTestData.SegmentPath(1), JournalTestData.OffsetOf(1, 5) + Journal.RecordHeaderSize);
        await Assert.ThrowsAsync<DfsIntegrityException>(async () => await Journal.OpenAsync(Host.Restart(), Dir, new JournalOptions()));
    }

    [Fact]
    public async Task MissingMiddleSegment_ThrowsIntegrity()
    {
        var options = new JournalOptions(SegmentSize: 300);
        await using (Journal journal = await Journal.OpenAsync(Medium, Dir, options))
        {
            await JournalTestData.AppendRangeAsync(journal, 1, 60);
            await journal.FlushAsync();
        }

        IReadOnlyList<string> files = await Medium.ListAsync(Dir);
        Assert.True(files.Count >= 3);
        await Medium.DeleteAsync(Dir + "/" + files[1]);
        await Assert.ThrowsAsync<DfsIntegrityException>(async () => await Journal.OpenAsync(Host.Restart(), Dir, options));
    }

    [Fact]
    public async Task TornTail_IsTruncated()
    {
        await using (Journal journal = await Journal.OpenAsync(Medium, Dir, new JournalOptions()))
        {
            await JournalTestData.AppendRangeAsync(journal, 1, 8);
            await journal.FlushAsync();
        }

        string path = JournalTestData.SegmentPath(1);
        long cut = JournalTestData.OffsetOf(1, 8) + 10;
        await using (IStorageFile file = await Medium.OpenAsync(path, StorageOpenMode.OpenExisting))
            await file.SetLengthAsync(cut);
        await using (Journal journal = await Journal.OpenAsync(Host.Restart(), Dir, new JournalOptions()))
        {
            JournalTestData.AssertRecords(journal.Records, 1, 7);
            Assert.Equal(8, await journal.AppendAsync(JournalTestData.Payload(8, 5)));
            await journal.FlushAsync();
        }

        await using Journal reopened = await Journal.OpenAsync(Host.Restart(), Dir, new JournalOptions());
        Assert.Equal(8, reopened.Records.Count);
        Assert.Equal(JournalTestData.Payload(8, 5), reopened.Records[7].Payload.ToArray());
    }

    [Fact]
    public async Task GarbageAppendedAfterLastRecord_IsTruncated()
    {
        await using (Journal journal = await Journal.OpenAsync(Medium, Dir, new JournalOptions()))
        {
            await JournalTestData.AppendRangeAsync(journal, 1, 3);
            await journal.FlushAsync();
        }

        string path = JournalTestData.SegmentPath(1);
        await using (IStorageFile file = await Medium.OpenAsync(path, StorageOpenMode.OpenExisting))
            await file.WriteFullyAsync(file.Length, TestData.Bytes(3, 100));
        await using Journal reopened = await Journal.OpenAsync(Host.Restart(), Dir, new JournalOptions());
        JournalTestData.AssertRecords(reopened.Records, 1, 3);
    }
}

public sealed class JournalSimulatedTests : JournalContractTests
{
    private const string Dir = JournalTestData.Dir;

    protected override MediumHost CreateHost() => new SimulatedHost();

    private SimulatedMedium Sim => Host.Simulated!;

    [Fact]
    public async Task UnflushedRecords_AreLostOnCrash_FlushedSurvive()
    {
        Journal journal = await Journal.OpenAsync(Medium, Dir, new JournalOptions());
        await JournalTestData.AppendRangeAsync(journal, 1, 5);
        await journal.FlushAsync();
        await JournalTestData.AppendRangeAsync(journal, 6, 9);
        Sim.Crash(CrashMode.KeepUnflushed); // the journal object is abandoned (process died)

        await using Journal reopened = await Journal.OpenAsync(Medium, Dir, new JournalOptions());
        JournalTestData.AssertRecords(reopened.Records, 1, 5);
    }

    [Fact]
    public async Task LostWriteHoleFollowedByUnacknowledgedRecords_IsTreatedAsTornTail()
    {
        Journal journal = await Journal.OpenAsync(Medium, Dir, new JournalOptions());
        await JournalTestData.AppendRangeAsync(journal, 1, 3);
        await journal.FlushAsync();
        await JournalTestData.AppendRangeAsync(journal, 4, 6);
        Sim.Faults.FailNextFlushes = 1; // records 4-6 are written but never acknowledged
        await Assert.ThrowsAnyAsync<IOException>(async () => await journal.FlushAsync());
        Sim.Crash(CrashMode.KeepUnflushed);

        // Simulate a reordered crash where the write of record 4 was lost but records 5-6 reached the disk: records
        // 5 and 6 only certify durability through 3, so 4.. was never acknowledged and is cut.
        string path = JournalTestData.SegmentPath(1);
        long start = JournalTestData.OffsetOf(1, 4);
        long end = JournalTestData.OffsetOf(1, 5);
        await using (IStorageFile file = await Medium.OpenAsync(path, StorageOpenMode.OpenExisting))
        {
            await file.WriteFullyAsync(start, new byte[end - start]);
            await file.FlushAsync();
        }

        await using Journal reopened = await Journal.OpenAsync(Medium, Dir, new JournalOptions());
        JournalTestData.AssertRecords(reopened.Records, 1, 3);
    }

    [Fact]
    public async Task LostWriteHoleInAcknowledgedBatch_IsIntegrityFailure()
    {
        Journal journal = await Journal.OpenAsync(Medium, Dir, new JournalOptions());
        await JournalTestData.AppendRangeAsync(journal, 1, 3);
        await journal.FlushAsync();
        await JournalTestData.AppendRangeAsync(journal, 4, 6);
        await journal.FlushAsync();
        await JournalTestData.AppendRangeAsync(journal, 7, 8);
        await journal.FlushAsync();
        Sim.Crash(CrashMode.LoseUnflushed);

        // Records 7-8 certify 4-6 durable: zeroes where record 4 was can only be damage, never a torn write.
        long start = JournalTestData.OffsetOf(1, 4);
        await using (IStorageFile file = await Medium.OpenAsync(JournalTestData.SegmentPath(1), StorageOpenMode.OpenExisting))
            await file.WriteFullyAsync(start, new byte[Journal.RecordHeaderSize]);
        await Assert.ThrowsAsync<DfsIntegrityException>(async () => await Journal.OpenAsync(Medium, Dir, new JournalOptions()));
    }

    [Fact]
    public async Task CorruptLastBatchAfterProcessCrash_ThrowsIntegrity_WhenItsMarkerSurvived()
    {
        Journal journal = await Journal.OpenAsync(Medium, Dir, new JournalOptions());
        await JournalTestData.AppendRangeAsync(journal, 1, 6);
        await journal.FlushAsync();
        Sim.Crash(CrashMode.KeepUnflushed); // process killed; the post-flush marker reached the device

        Sim.CorruptByte(JournalTestData.SegmentPath(1), JournalTestData.OffsetOf(1, 4) + Journal.RecordHeaderSize);
        await Assert.ThrowsAsync<DfsIntegrityException>(async () => await Journal.OpenAsync(Medium, Dir, new JournalOptions()));
    }

    [Fact]
    public async Task CorruptLastBatchAfterPowerLoss_IsIndistinguishableFromTornTail()
    {
        Journal journal = await Journal.OpenAsync(Medium, Dir, new JournalOptions());
        await JournalTestData.AppendRangeAsync(journal, 1, 6);
        await journal.FlushAsync();
        Sim.Crash(CrashMode.LoseUnflushed); // the post-flush marker was lost with the power

        Sim.CorruptByte(JournalTestData.SegmentPath(1), JournalTestData.OffsetOf(1, 4) + Journal.RecordHeaderSize);
        await using Journal reopened = await Journal.OpenAsync(Medium, Dir, new JournalOptions());
        JournalTestData.AssertRecords(reopened.Records, 1, 3);
    }

    [Fact]
    public async Task FailedFlush_FaultsJournal_UntilReopened()
    {
        Journal journal = await Journal.OpenAsync(Medium, Dir, new JournalOptions());
        await JournalTestData.AppendRangeAsync(journal, 1, 3);
        await journal.FlushAsync();
        await JournalTestData.AppendRangeAsync(journal, 4, 5);
        Sim.Faults.FailNextFlushes = 1;
        await Assert.ThrowsAnyAsync<IOException>(async () => await journal.FlushAsync());
        Assert.True(journal.IsFaulted);
        await Assert.ThrowsAnyAsync<IOException>(async () => await journal.AppendAsync(new byte[] { 1 }));
        await Assert.ThrowsAnyAsync<IOException>(async () => await journal.FlushAsync());
        await journal.DisposeAsync();

        Sim.Crash(CrashMode.LoseUnflushed);
        await using Journal reopened = await Journal.OpenAsync(Medium, Dir, new JournalOptions());
        JournalTestData.AssertRecords(reopened.Records, 1, 3);
    }

    [Fact]
    public async Task ShortWrites_AreCompleted()
    {
        await using (Journal journal = await Journal.OpenAsync(Medium, Dir, new JournalOptions()))
        {
            await JournalTestData.AppendRangeAsync(journal, 1, 10);
            Sim.Faults.ShortNextWrites = 5;
            await journal.FlushAsync();
        }

        Sim.Crash(CrashMode.LoseUnflushed);
        await using Journal reopened = await Journal.OpenAsync(Medium, Dir, new JournalOptions());
        JournalTestData.AssertRecords(reopened.Records, 1, 10);
    }

    [Fact]
    public async Task DiskFull_FailsFlush_AndRecoveryKeepsDurablePrefix()
    {
        Journal journal = await Journal.OpenAsync(Medium, Dir, new JournalOptions());
        await JournalTestData.AppendRangeAsync(journal, 1, 5);
        await journal.FlushAsync();
        Sim.Faults.CapacityOverride = Sim.UsedBytes + 30;
        await journal.AppendAsync(TestData.Bytes(1, 500));
        IOException ex = await Assert.ThrowsAnyAsync<IOException>(async () => await journal.FlushAsync());
        Assert.True(StorageErrors.IsDiskFull(ex));
        Sim.Faults.CapacityOverride = null;
        Sim.Crash(CrashMode.KeepUnflushed);

        await using Journal reopened = await Journal.OpenAsync(Medium, Dir, new JournalOptions());
        JournalTestData.AssertRecords(reopened.Records, 1, 5);
    }

    [Fact]
    public async Task NewerSegmentVersion_IsRefused()
    {
        await using (Journal journal = await Journal.OpenAsync(Medium, Dir, new JournalOptions()))
            await JournalTestData.AppendRangeAsync(journal, 1, 2);
        string path = JournalTestData.SegmentPath(1);
        byte[] raw = Sim.ReadCurrent(path);
        raw[4] = 2;
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(raw.AsSpan(20), Tedd.Tdfs.Data.ObjectChecksum.Crc32C(raw.AsSpan(0, 20)));
        await using (IStorageFile file = await Medium.OpenAsync(path, StorageOpenMode.OpenExisting))
            await file.WriteFullyAsync(0, raw.AsMemory(0, 24));
        DfsProtocolException ex = await Assert.ThrowsAsync<DfsProtocolException>(async () => await Journal.OpenAsync(Medium, Dir, new JournalOptions()));
        Assert.Equal(DfsErrorCode.Unsupported, ex.ErrorCode);
    }

    [Fact]
    public async Task TornSegmentHeaderOfNewestSegment_IsRewritten()
    {
        var options = new JournalOptions(SegmentSize: 300);
        long lastFirst;
        await using (Journal journal = await Journal.OpenAsync(Medium, Dir, options))
        {
            await JournalTestData.AppendRangeAsync(journal, 1, 30);
            await journal.FlushAsync();
            lastFirst = journal.FirstIndex;
            IReadOnlyList<string> names = await Medium.ListAsync(Dir);
            Assert.True(Journal.TryParseSegmentFileName(names[^1], out lastFirst));
        }

        // Pretend the newest segment's creation was torn: header gone, records unacknowledged.
        string path = JournalTestData.SegmentPath(lastFirst);
        await using (IStorageFile file = await Medium.OpenAsync(path, StorageOpenMode.OpenExisting))
            await file.SetLengthAsync(10);
        await using Journal reopened = await Journal.OpenAsync(Medium, Dir, options);
        JournalTestData.AssertRecords(reopened.Records, 1, lastFirst - 1);
        Assert.Equal(lastFirst, reopened.NextIndex);
    }

    [Fact]
    public async Task OperationsAfterDispose_Throw()
    {
        Journal journal = await Journal.OpenAsync(Medium, Dir, new JournalOptions());
        await journal.DisposeAsync();
        await journal.DisposeAsync();
        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await journal.AppendAsync(new byte[] { 1 }));
    }

    [Fact]
    public async Task ConcurrentAppendsAndFlushes_AreSerialized()
    {
        await using (Journal journal = await Journal.OpenAsync(Medium, Dir, new JournalOptions(SegmentSize: 1000)))
        {
            var tasks = new List<Task>();
            for (int i = 0; i < 200; i++)
            {
                tasks.Add(journal.AppendAsync(TestData.Bytes((ulong)i, 30)).AsTask());
                if (i % 10 == 0)
                    tasks.Add(journal.FlushAsync().AsTask());
            }

            await Task.WhenAll(tasks);
            await journal.FlushAsync();
            Assert.Equal(200, journal.LastIndex);
        }

        await using Journal reopened = await Journal.OpenAsync(Medium, Dir, new JournalOptions(SegmentSize: 1000));
        Assert.Equal(200, reopened.Records.Count);
        Assert.Equal(Enumerable.Range(1, 200).Select(i => (long)i), reopened.Records.Select(r => r.Index));
    }
}

public sealed class JournalFileSystemTests : JournalContractTests
{
    protected override MediumHost CreateHost() => new FileSystemHost();
}
