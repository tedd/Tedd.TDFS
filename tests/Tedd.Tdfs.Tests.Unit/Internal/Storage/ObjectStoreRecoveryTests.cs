using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Tests.Unit.Internal.Storage.Infrastructure;

namespace Tedd.Tdfs.Tests.Unit.Internal.Storage;

/// <summary>Object store behaviour that needs the simulated medium's fault and crash controls.</summary>
public sealed class ObjectStoreRecoveryTests
{
    private const string Dir = ObjectTestData.Dir;
    private readonly SimulatedMedium _medium = new("sim", 1L << 34, new SeededRandom(7));

    private ValueTask<ObjectStore> OpenAsync(ObjectStoreOptions? options = null, IFaultInjector? faults = null)
        => ObjectStore.OpenAsync(_medium, Dir, options ?? new ObjectStoreOptions(1 << 20, 0), faults ?? NoFaults.Instance);

    private static Task<ObjectRecordInfo> PutAsync(ObjectStore store, int n, int length = 1000)
        => store.PutAsync(ObjectTestData.Id(n), ObjectTestData.VolumeA, ObjectTestData.Staging1, ObjectTestData.Content(n, length)).AsTask();

    [Fact]
    public async Task Put_IsDurableOnReturn()
    {
        ObjectStore store = await OpenAsync();
        await PutAsync(store, 1);
        long flushes = _medium.FlushCount;
        await PutAsync(store, 2);
        Assert.True(_medium.FlushCount > flushes);
        _medium.Crash(CrashMode.LoseUnflushed);

        await using ObjectStore reopened = await OpenAsync();
        Assert.Equal(ObjectTestData.Content(2, 1000), await ObjectTestData.ReadAllAsync(reopened, ObjectTestData.Id(2)));
    }

    [Fact]
    public async Task AcknowledgeBeforeFlushMutation_SkipsTheBarrier()
    {
        ObjectStore store = await OpenAsync(new ObjectStoreOptions(1 << 20, 0) { Mutations = new SafetyMutations { AcknowledgeBeforeFlush = true } });
        await PutAsync(store, 1);
        long flushes = _medium.FlushCount;
        await PutAsync(store, 2);
        Assert.Equal(flushes, _medium.FlushCount);
        Assert.True(await store.ContainsAsync(ObjectTestData.Id(2)));
        _medium.Crash(CrashMode.LoseUnflushed);

        await using ObjectStore reopened = await OpenAsync();
        Assert.False(await reopened.ContainsAsync(ObjectTestData.Id(2)), "the negative control must expose the lost acknowledged write");
    }

    [Fact]
    public async Task SkipChecksumVerificationMutation_ReturnsCorruptBytes()
    {
        await using ObjectStore store = await OpenAsync(new ObjectStoreOptions(1 << 20, 0) { Mutations = new SafetyMutations { SkipChecksumVerification = true } });
        await PutAsync(store, 1);
        ObjectPhysicalLocation location = store.GetPhysicalLocation(ObjectTestData.Id(1))!;
        _medium.CorruptByte(location.SegmentPath, location.PayloadOffset + 10);
        byte[] read = await ObjectTestData.ReadAllAsync(store, ObjectTestData.Id(1));
        Assert.NotEqual(ObjectTestData.Content(1, 1000), read);
    }

    [Fact]
    public async Task FaultSites_AreHitWithContext()
    {
        var faults = new RecordingFaults();
        await using ObjectStore store = await OpenAsync(new ObjectStoreOptions(1 << 20, 0) { NodeName = "n7" }, faults);
        await PutAsync(store, 1);
        await store.DeleteAsync(ObjectTestData.Id(1));
        await store.CompactAsync();
        Assert.Equal([FaultSites.ObjectWrite, FaultSites.ObjectBeforeFlush, FaultSites.CompactionCopy, FaultSites.CompactionPublish, FaultSites.CompactionDelete], faults.Sites);
        FaultContext write = faults.Hits[0];
        Assert.Equal(FaultStage.F06, write.Stage);
        Assert.Equal("n7", write.Node);
        Assert.Equal(DfsOperationKind.PutObject, write.Operation);
        Assert.Equal(ObjectTestData.Id(1), write.Object);
        Assert.Equal(FaultStage.F07, faults.Hits[1].Stage);
        Assert.All(faults.Hits.Skip(2), h => Assert.Equal(FaultStage.F20, h.Stage));
    }

    [Fact]
    public async Task FaultBeforeWrite_StoresNothing()
    {
        var faults = new RecordingFaults { ThrowAtSite = FaultSites.ObjectWrite };
        await using ObjectStore store = await OpenAsync(faults: faults);
        long total = store.TotalBytes;
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await PutAsync(store, 1));
        Assert.False(await store.ContainsAsync(ObjectTestData.Id(1)));
        Assert.Equal(total, store.TotalBytes);
        await PutAsync(store, 1);
        Assert.True(await store.ContainsAsync(ObjectTestData.Id(1)));
    }

    [Fact]
    public async Task FaultBeforeFlush_DoesNotAcknowledge()
    {
        var faults = new RecordingFaults { ThrowAtSite = FaultSites.ObjectBeforeFlush };
        ObjectStore store = await OpenAsync(faults: faults);
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await PutAsync(store, 1));
        Assert.False(await store.ContainsAsync(ObjectTestData.Id(1)));
        await PutAsync(store, 2);
        Assert.True(await store.ContainsAsync(ObjectTestData.Id(2)));
        _medium.Crash(CrashMode.KeepUnflushed);

        // The unacknowledged record may survive (DSK-013); if it does it is complete and verified.
        await using ObjectStore reopened = await OpenAsync();
        if (await reopened.ContainsAsync(ObjectTestData.Id(1)))
            Assert.Equal(ObjectTestData.Content(1, 1000), await ObjectTestData.ReadAllAsync(reopened, ObjectTestData.Id(1)));
        Assert.True(await reopened.ContainsAsync(ObjectTestData.Id(2)));
    }

    [Fact]
    public async Task FailedFlush_IsNotAcknowledged_AndStoreKeepsWorking()
    {
        await using ObjectStore store = await OpenAsync();
        _medium.Faults.FailNextFlushes = 1;
        await Assert.ThrowsAnyAsync<IOException>(async () => await PutAsync(store, 1));
        Assert.False(await store.ContainsAsync(ObjectTestData.Id(1)));
        await PutAsync(store, 1);
        Assert.Equal(ObjectTestData.Content(1, 1000), await ObjectTestData.ReadAllAsync(store, ObjectTestData.Id(1)));
    }

    [Fact]
    public async Task ShortWrites_AreCompleted()
    {
        ObjectStore store = await OpenAsync();
        _medium.Faults.ShortNextWrites = 10;
        await PutAsync(store, 1, 3 * ObjectStore.UnitSize);
        _medium.Crash(CrashMode.LoseUnflushed);
        await using ObjectStore reopened = await OpenAsync();
        Assert.Equal(ObjectTestData.Content(1, 3 * ObjectStore.UnitSize), await ObjectTestData.ReadAllAsync(reopened, ObjectTestData.Id(1)));
    }

    [Fact]
    public async Task WriteError_RollsBackAndReportsFailure()
    {
        await using ObjectStore store = await OpenAsync();
        await PutAsync(store, 1);
        long total = store.TotalBytes;
        _medium.Faults.FailNextWrites = 1;
        await Assert.ThrowsAnyAsync<IOException>(async () => await PutAsync(store, 2));
        Assert.Equal(total, store.TotalBytes);
        await PutAsync(store, 3);
        Assert.False(await store.ContainsAsync(ObjectTestData.Id(2)));
        Assert.True(await store.ContainsAsync(ObjectTestData.Id(3)));
    }

    [Fact]
    public async Task ReadIoError_Propagates()
    {
        await using ObjectStore store = await OpenAsync();
        await PutAsync(store, 1);
        _medium.Faults.FailNextReads = 1;
        IOException ex = await Assert.ThrowsAnyAsync<IOException>(async () => await store.ReadAsync(ObjectTestData.Id(1), 0, new byte[10]));
        Assert.IsNotType<DfsIntegrityException>(ex);
        Assert.Equal(10, await store.ReadAsync(ObjectTestData.Id(1), 0, new byte[10]));
    }

    [Fact]
    public async Task Capacity_RejectsBeforeConsumingReserve_DeletesAndCompactionStillWork()
    {
        const long Reserve = 200_000;
        var options = new ObjectStoreOptions(1 << 20, Reserve);
        await using ObjectStore store = await OpenAsync(options);
        _medium.Faults.CapacityOverride = 300_000 + Reserve;
        int stored = 0;
        DfsCapacityException? rejection = null;
        for (int n = 1; n <= 20 && rejection is null; n++)
        {
            try
            {
                await PutAsync(store, n, 50_000);
                stored++;
            }
            catch (DfsCapacityException ex)
            {
                rejection = ex;
            }
        }

        Assert.NotNull(rejection);
        Assert.Equal(DfsErrorCode.CapacityExhausted, rejection.ErrorCode);
        Assert.True(rejection.IsRetryable);
        Assert.InRange(stored, 4, 6);
        Assert.True(_medium.UsedBytes <= _medium.CapacityBytes - Reserve, "admission consumed the reserve");

        for (int n = 1; n <= stored; n++)
            await store.DeleteAsync(ObjectTestData.Id(n));
        await store.CompactAsync();
        await PutAsync(store, 100, 50_000);
        Assert.True(await store.ContainsAsync(ObjectTestData.Id(100)));
    }

    [Fact]
    public async Task DeviceFull_DuringWrite_MapsToCapacityException()
    {
        await using ObjectStore store = await OpenAsync(new ObjectStoreOptions(1 << 20, 0));
        await PutAsync(store, 1);
        _medium.Faults.CapacityOverride = _medium.UsedBytes + 100;
        await Assert.ThrowsAsync<DfsCapacityException>(async () => await PutAsync(store, 2, 5000));
        _medium.Faults.CapacityOverride = null;
        await PutAsync(store, 2, 5000);
        Assert.True(await store.ContainsAsync(ObjectTestData.Id(2)));
    }

    [Fact]
    public async Task ReadOnlyDevice_FailsPutsButServesVerifiedReads()
    {
        await using ObjectStore store = await OpenAsync();
        await PutAsync(store, 1);
        _medium.Faults.ReadOnly = true;
        await Assert.ThrowsAnyAsync<IOException>(async () => await PutAsync(store, 2));
        Assert.Equal(ObjectTestData.Content(1, 1000), await ObjectTestData.ReadAllAsync(store, ObjectTestData.Id(1)));
        _medium.Faults.ReadOnly = false;
        await PutAsync(store, 2);
    }

    [Fact]
    public async Task TornTailOfActiveSegment_IsTruncated()
    {
        string segmentPath;
        await using (ObjectStore store = await OpenAsync())
        {
            await PutAsync(store, 1);
            await PutAsync(store, 2);
            segmentPath = store.GetPhysicalLocation(ObjectTestData.Id(2))!.SegmentPath;
        }

        byte[] raw = _medium.ReadCurrent(segmentPath);
        await using (IStorageFile file = await _medium.OpenAsync(segmentPath, StorageOpenMode.OpenExisting))
        {
            // Append the first half of a copy of record 2: a torn, unacknowledged write.
            ObjectPhysicalLocation location;
            await using (ObjectStore probe = await OpenAsync())
                location = probe.GetPhysicalLocation(ObjectTestData.Id(2))!;
            int recordLength = (int)(location.HeaderLength + location.StoredLength);
            await file.WriteFullyAsync(raw.Length, raw.AsMemory((int)location.RecordOffset, recordLength / 2));
            await file.FlushAsync();
        }

        await using ObjectStore reopened = await OpenAsync();
        Assert.Equal(raw.Length, _medium.ReadCurrent(segmentPath).Length);
        Assert.Equal(2, reopened.ObjectCount);
        Assert.Empty(reopened.Quarantined);
        await PutAsync(reopened, 3);
        Assert.Equal(ObjectTestData.Content(3, 1000), await ObjectTestData.ReadAllAsync(reopened, ObjectTestData.Id(3)));
    }

    [Fact]
    public async Task ZeroHoleFollowedByValidRecord_IsSkippedSilently()
    {
        string segmentPath;
        long holeStart;
        long holeEnd;
        await using (ObjectStore store = await OpenAsync())
        {
            await PutAsync(store, 1);
            await PutAsync(store, 2);
            await PutAsync(store, 3);
            ObjectPhysicalLocation two = store.GetPhysicalLocation(ObjectTestData.Id(2))!;
            segmentPath = two.SegmentPath;
            holeStart = two.RecordOffset;
            holeEnd = two.RecordOffset + two.HeaderLength + two.StoredLength;
        }

        // A lost write (zeroes) in the middle, as left by a crash that reordered unflushed writes.
        await using (IStorageFile file = await _medium.OpenAsync(segmentPath, StorageOpenMode.OpenExisting))
            await file.WriteFullyAsync(holeStart, new byte[holeEnd - holeStart]);
        await using ObjectStore reopened = await OpenAsync();
        Assert.False(await reopened.ContainsAsync(ObjectTestData.Id(2)));
        Assert.True(await reopened.ContainsAsync(ObjectTestData.Id(1)));
        Assert.Equal(ObjectTestData.Content(3, 1000), await ObjectTestData.ReadAllAsync(reopened, ObjectTestData.Id(3)));
        Assert.Empty(reopened.Quarantined);
    }

    [Fact]
    public async Task GarbageRegionInSealedSegment_IsReportedAndSkipped()
    {
        var options = new ObjectStoreOptions(3000, 0);
        string segmentPath;
        long garbageAt;
        await using (ObjectStore store = await OpenAsync(options))
        {
            for (int n = 1; n <= 8; n++)
                await PutAsync(store, n, 700);
            ObjectPhysicalLocation two = store.GetPhysicalLocation(ObjectTestData.Id(2))!;
            Assert.NotEqual(two.SegmentPath, store.GetPhysicalLocation(ObjectTestData.Id(8))!.SegmentPath);
            segmentPath = two.SegmentPath;
            garbageAt = two.RecordOffset;
        }

        _medium.CorruptByte(segmentPath, garbageAt + 1);
        await using ObjectStore reopened = await OpenAsync(options);
        Assert.False(await reopened.ContainsAsync(ObjectTestData.Id(2)));
        Assert.Contains(reopened.Quarantined, q => q.Id is null && q.SegmentPath == segmentPath && q.Offset == garbageAt);
        foreach (int n in new[] { 1, 3, 4, 5, 6, 7, 8 })
            Assert.Equal(ObjectTestData.Content(n, 700), await ObjectTestData.ReadAllAsync(reopened, ObjectTestData.Id(n)));
    }

    [Fact]
    public async Task CorruptPayloadInActiveSegment_IsQuarantinedDurablyAtOpen()
    {
        await using (ObjectStore store = await OpenAsync())
        {
            await PutAsync(store, 1);
            await PutAsync(store, 2);
            ObjectPhysicalLocation one = store.GetPhysicalLocation(ObjectTestData.Id(1))!;
            _medium.CorruptByte(one.SegmentPath, one.PayloadOffset + 3);
        }

        await using (ObjectStore reopened = await OpenAsync())
        {
            Assert.False(await reopened.ContainsAsync(ObjectTestData.Id(1)));
            Assert.Single(reopened.Quarantined);
            await PutAsync(reopened, 1);
        }

        await using ObjectStore again = await OpenAsync();
        Assert.Equal(ObjectTestData.Content(1, 1000), await ObjectTestData.ReadAllAsync(again, ObjectTestData.Id(1)));
        Assert.Empty(again.Quarantined); // reported once (first open); the durable quarantine record retires it for good
    }

    [Fact]
    public async Task IdempotentPutOverCorruptCopy_ReplacesIt()
    {
        await using ObjectStore store = await OpenAsync();
        await PutAsync(store, 1, 3000);
        ObjectPhysicalLocation location = store.GetPhysicalLocation(ObjectTestData.Id(1))!;
        _medium.CorruptByte(location.SegmentPath, location.PayloadOffset + 5);
        await PutAsync(store, 1, 3000);
        Assert.NotEqual(location, store.GetPhysicalLocation(ObjectTestData.Id(1)));
        Assert.Equal(ObjectTestData.Content(1, 3000), await ObjectTestData.ReadAllAsync(store, ObjectTestData.Id(1)));
        Assert.Contains(store.Quarantined, q => q.Id == ObjectTestData.Id(1));
    }

    [Fact]
    public async Task NewerSegmentFormat_IsRefused()
    {
        string path;
        await using (ObjectStore store = await OpenAsync())
        {
            await PutAsync(store, 1);
            path = store.GetPhysicalLocation(ObjectTestData.Id(1))!.SegmentPath;
        }

        byte[] header = _medium.ReadCurrent(path).AsSpan(0, ObjectStore.SegmentHeaderSize).ToArray();
        header[4] = 9;
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(header.AsSpan(28), Tedd.Tdfs.Data.ObjectChecksum.Crc32C(header.AsSpan(0, 28)));
        await using (IStorageFile file = await _medium.OpenAsync(path, StorageOpenMode.OpenExisting))
            await file.WriteFullyAsync(0, header);
        DfsProtocolException ex = await Assert.ThrowsAsync<DfsProtocolException>(async () => await OpenAsync());
        Assert.Equal(DfsErrorCode.Unsupported, ex.ErrorCode);
    }

    [Fact]
    public async Task CompactionFaultBeforePublish_KeepsInputsAndDiscardsOutputs()
    {
        var faults = new RecordingFaults { ThrowAtSite = FaultSites.CompactionPublish };
        await using (ObjectStore store = await OpenAsync(faults: faults))
        {
            for (int n = 1; n <= 6; n++)
                await PutAsync(store, n);
            await store.DeleteAsync(ObjectTestData.Id(1));
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.CompactAsync());
            for (int n = 2; n <= 6; n++)
                Assert.Equal(ObjectTestData.Content(n, 1000), await ObjectTestData.ReadAllAsync(store, ObjectTestData.Id(n)));
            Assert.DoesNotContain(await _medium.ListAsync(Dir), name => name.EndsWith(ObjectStore.CompactingExtension, StringComparison.Ordinal));
            await store.CompactAsync();
            for (int n = 2; n <= 6; n++)
                Assert.Equal(ObjectTestData.Content(n, 1000), await ObjectTestData.ReadAllAsync(store, ObjectTestData.Id(n)));
        }

        await using ObjectStore reopened = await OpenAsync();
        Assert.Equal(5, reopened.ObjectCount);
    }

    [Fact]
    public async Task CompactionFaultBeforeDelete_IsCompletedByNextCompactionOrOpen()
    {
        var faults = new RecordingFaults { ThrowAtSite = FaultSites.CompactionDelete };
        ObjectStore store = await OpenAsync(faults: faults);
        for (int n = 1; n <= 6; n++)
            await PutAsync(store, n);
        await store.DeleteAsync(ObjectTestData.Id(1));
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.CompactAsync());
        Assert.Contains(await _medium.ListAsync(Dir), name => name == ObjectStore.ManifestFileName);
        for (int n = 2; n <= 6; n++)
            Assert.Equal(ObjectTestData.Content(n, 1000), await ObjectTestData.ReadAllAsync(store, ObjectTestData.Id(n)));
        _medium.Crash(CrashMode.LoseUnflushed);

        await using ObjectStore reopened = await OpenAsync();
        Assert.DoesNotContain(await _medium.ListAsync(Dir), name => name == ObjectStore.ManifestFileName);
        Assert.Equal(5, reopened.ObjectCount);
        for (int n = 2; n <= 6; n++)
            Assert.Equal(ObjectTestData.Content(n, 1000), await ObjectTestData.ReadAllAsync(reopened, ObjectTestData.Id(n)));
    }

    [Fact]
    public async Task ReadsInFlightSurviveCompactionRetiringTheirSegment()
    {
        await using ObjectStore store = await OpenAsync();
        for (int n = 1; n <= 4; n++)
            await PutAsync(store, n, 2 * ObjectStore.UnitSize);
        await store.DeleteAsync(ObjectTestData.Id(1));
        _medium.Faults.IoDelay = TimeSpan.FromMilliseconds(20);
        _medium.Faults.PathFilter = p => p.EndsWith(ObjectStore.SegmentExtension, StringComparison.Ordinal);
        Task<byte[]> read = ObjectTestData.ReadAllAsync(store, ObjectTestData.Id(2));
        Task compaction = store.CompactAsync().AsTask();
        await Task.WhenAll(read, compaction);
        _medium.Faults.Reset();
        Assert.Equal(ObjectTestData.Content(2, 2 * ObjectStore.UnitSize), await read);
        Assert.Equal(ObjectTestData.Content(2, 2 * ObjectStore.UnitSize), await ObjectTestData.ReadAllAsync(store, ObjectTestData.Id(2)));
    }

    [Fact]
    public async Task StaleCompactionOutputsWithoutManifest_AreDiscardedAtOpen()
    {
        await using (ObjectStore store = await OpenAsync())
            await PutAsync(store, 1);
        string stale = Dir + "/" + ObjectStore.SegmentFileName(new ObjectStore.SegmentKey(1, 1), compacting: true);
        await using (IStorageFile file = await _medium.OpenAsync(stale, StorageOpenMode.CreateNew))
            await file.WriteFullyAsync(0, TestData.Bytes(1, 100));
        await using ObjectStore reopened = await OpenAsync();
        Assert.False(await _medium.ExistsAsync(stale));
        Assert.Equal(1, reopened.ObjectCount);
    }
}
