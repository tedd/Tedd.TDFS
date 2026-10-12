using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Tests.Unit.Internal.Storage.Infrastructure;

namespace Tedd.Tdfs.Tests.Unit.Internal.Storage;

/// <summary>
/// Adversarial object-store tests: interleavings at awaits (the node loop runs many uploads at once), crashes while
/// other puts are still waiting for their barrier, and malformed on-disk state.
/// </summary>
public sealed class ObjectStoreAdversarialTests
{
    private const string Dir = ObjectTestData.Dir;

    private static ValueTask<ObjectStore> OpenAsync(IStorageMedium medium, IFaultInjector? faults = null, long segmentSize = 1 << 20)
        => ObjectStore.OpenAsync(medium, Dir, new ObjectStoreOptions(segmentSize, 0), faults ?? NoFaults.Instance);

    private static Task<ObjectRecordInfo> PutAsync(ObjectStore store, int n, int length = 1000, int contentOf = -1, VolumeId? volume = null, StagingId? staging = null)
        => store.PutAsync(ObjectTestData.Id(n), volume ?? ObjectTestData.VolumeA, staging ?? ObjectTestData.Staging1, ObjectTestData.Content(contentOf < 0 ? n : contentOf, length)).AsTask();

    private static SimulatedMedium NewMedium(ulong seed = 7) => new("sim", 1L << 34, new SeededRandom(seed));

    [Fact]
    public async Task ConcurrentConflictingPut_WhileFirstAwaitsItsBarrier_IsRejected()
    {
        SimulatedMedium medium = NewMedium();
        var faults = new PausingFaults();
        ObjectId id = ObjectTestData.Id(1);
        await using (ObjectStore store = await OpenAsync(medium, faults))
        {
            PausingFaults.Pause first = faults.PauseAt(FaultSites.ObjectBeforeFlush, id);
            Task<ObjectRecordInfo> a = PutAsync(store, 1);
            await first.Reached;

            // Same id, different bytes, while the first upload is still waiting for its flush barrier: objects are
            // immutable, so the second upload must be refused rather than both callers receiving success.
            await Assert.ThrowsAsync<DfsIntegrityException>(async () => await PutAsync(store, 1, contentOf: 2));
            await Assert.ThrowsAsync<DfsIntegrityException>(async () => await PutAsync(store, 1, volume: ObjectTestData.VolumeB));

            first.Release();
            ObjectRecordInfo info = await a;
            Assert.Equal(ObjectTestData.Content(1, 1000), await ObjectTestData.ReadAllAsync(store, id));
            Assert.Equal(info, store.TryGetInfo(id));
        }

        medium.Crash(CrashMode.KeepUnflushed);
        await using ObjectStore reopened = await OpenAsync(medium);
        Assert.Equal(ObjectTestData.Content(1, 1000), await ObjectTestData.ReadAllAsync(reopened, id));
    }

    [Fact]
    public async Task ConcurrentIdenticalPuts_BothSucceed()
    {
        SimulatedMedium medium = NewMedium();
        var faults = new PausingFaults();
        ObjectId id = ObjectTestData.Id(1);
        await using ObjectStore store = await OpenAsync(medium, faults);
        PausingFaults.Pause first = faults.PauseAt(FaultSites.ObjectBeforeFlush, id);
        Task<ObjectRecordInfo> a = PutAsync(store, 1);
        await first.Reached;
        ObjectRecordInfo second = await PutAsync(store, 1);
        first.Release();
        Assert.Equal(second, await a);
        Assert.Equal(ObjectTestData.Content(1, 1000), await ObjectTestData.ReadAllAsync(store, id));
    }

    [Fact]
    public async Task ConflictingPutAfterFailedFirstUpload_IsAccepted()
    {
        SimulatedMedium medium = NewMedium();
        var faults = new RecordingFaults { ThrowAtSite = FaultSites.ObjectBeforeFlush };
        await using ObjectStore store = await OpenAsync(medium, faults);
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await PutAsync(store, 1));

        // The failed upload was never acknowledged nor indexed; a later upload of other bytes under the same id is
        // not a conflict with anything this store acknowledged.
        await PutAsync(store, 1, contentOf: 2);
        Assert.Equal(ObjectTestData.Content(2, 1000), await ObjectTestData.ReadAllAsync(store, ObjectTestData.Id(1)));
    }

    /// <summary>
    /// Crash matrix over interleaved puts: A and C wait for their barrier while B rolls the segment over; then A and C
    /// finish. Every crash point and mode must recover every acknowledged object intact and never index a damaged one.
    /// </summary>
    [Theory]
    [InlineData(2600)]
    [InlineData(1 << 20)]
    public async Task CrashMatrix_InterleavedPutsAcrossRollover(long segmentSize)
    {
        var acknowledged = new HashSet<int>();
        bool deleted9 = false;
        await CrashMatrix.RunAsync(
            async medium =>
            {
                acknowledged.Clear();
                deleted9 = false;
                await using ObjectStore store = await OpenAsync(medium, segmentSize: segmentSize);
                await PutAsync(store, 9, 100);
                acknowledged.Add(9);
            },
            async medium =>
            {
                var faults = new PausingFaults();
                ObjectStore store = await OpenAsync(medium, faults, segmentSize);
                PausingFaults.Pause pauseA = faults.PauseAt(FaultSites.ObjectBeforeFlush, ObjectTestData.Id(1));
                PausingFaults.Pause pauseC = faults.PauseAt(FaultSites.ObjectBeforeFlush, ObjectTestData.Id(3));
                async Task TrackAsync(int n)
                {
                    await PutAsync(store, n);
                    acknowledged.Add(n);
                }

                Task a = TrackAsync(1);
                Task c = TrackAsync(3);
                try
                {
                    await Task.WhenAny(pauseA.Reached, a);
                    await Task.WhenAny(pauseC.Reached, c);
                    await PutAsync(store, 2);
                    acknowledged.Add(2);
                }
                finally
                {
                    pauseA.Release();
                    pauseC.Release();
                }

                await Task.WhenAll(a, c);
                acknowledged.Remove(9); // a delete in flight may or may not take effect
                await store.DeleteAsync(ObjectTestData.Id(9));
                deleted9 = true;
                await store.DisposeAsync();
            },
            async (medium, crash) =>
            {
                await using ObjectStore store = await OpenAsync(medium, segmentSize: segmentSize);
                var present = new List<int>();
                foreach (int n in new[] { 1, 2, 3, 9 })
                {
                    bool contains = await store.ContainsAsync(ObjectTestData.Id(n));
                    if (acknowledged.Contains(n))
                        Assert.True(contains, $"acknowledged object {n} was lost");
                    if (n == 9 && deleted9)
                        Assert.False(contains, "acknowledged delete was lost");
                    if (!contains)
                        continue;
                    Assert.Equal(ObjectTestData.Content(n, n == 9 ? 100 : 1000), await ObjectTestData.ReadAllAsync(store, ObjectTestData.Id(n)));
                    present.Add(n);
                }

                Assert.DoesNotContain(store.Quarantined, q => q.Id is not null);
                return string.Join(",", present);
            },
            seedsPerRandomMode: 4);
    }

    /// <summary>
    /// Put A is written and waits for its barrier; put B's flush of the same segment fails, and the device drops every
    /// dirty byte of the file (reporting the error to B only). A's own flush then succeeds although A's bytes are gone:
    /// A must not be acknowledged (DSK-004).
    /// </summary>
    [Fact]
    public async Task FlushFailureOfAConcurrentPut_FailsPutsWrittenBeforeIt()
    {
        SimulatedMedium sim = NewMedium();
        var lossy = new LossyFlushMedium(sim);
        var faults = new PausingFaults();
        ObjectStore store = await OpenAsync(lossy, faults);
        await PutAsync(store, 9);

        PausingFaults.Pause pauseA = faults.PauseAt(FaultSites.ObjectBeforeFlush, ObjectTestData.Id(1));
        Task<ObjectRecordInfo> a = PutAsync(store, 1);
        await pauseA.Reached;
        lossy.FailNextFlushes = 1;
        await Assert.ThrowsAnyAsync<IOException>(async () => await PutAsync(store, 2));
        pauseA.Release();

        bool acknowledgedA;
        try
        {
            await a;
            acknowledgedA = true;
        }
        catch (IOException)
        {
            acknowledgedA = false;
        }

        if (acknowledgedA)
            Assert.Equal(ObjectTestData.Content(1, 1000), await ObjectTestData.ReadAllAsync(store, ObjectTestData.Id(1)));
        Assert.False(acknowledgedA, "a put whose bytes were dropped by a failed flush was acknowledged");
        Assert.False(await store.ContainsAsync(ObjectTestData.Id(1)));
        Assert.Equal(ObjectTestData.Content(9, 1000), await ObjectTestData.ReadAllAsync(store, ObjectTestData.Id(9)));

        await PutAsync(store, 3);
        await store.DisposeAsync();
        sim.Crash(CrashMode.LoseUnflushed);

        await using ObjectStore reopened = await OpenAsync(sim);
        foreach (int n in new[] { 3, 9 })
            Assert.Equal(ObjectTestData.Content(n, 1000), await ObjectTestData.ReadAllAsync(reopened, ObjectTestData.Id(n)));
        Assert.False(await reopened.ContainsAsync(ObjectTestData.Id(1)));
        Assert.False(await reopened.ContainsAsync(ObjectTestData.Id(2)));
        Assert.DoesNotContain(reopened.Quarantined, q => q.Id is not null);
    }

    /// <summary>A delete whose tombstone was dropped by a concurrent put's failed flush must not be acknowledged.</summary>
    [Fact]
    public async Task FlushFailureOfAConcurrentPut_FailsADeleteWhoseTombstoneItDropped()
    {
        SimulatedMedium sim = NewMedium();
        var lossy = new LossyFlushMedium(sim);
        var faults = new PausingFaults();
        ObjectStore store = await OpenAsync(lossy, faults);
        await PutAsync(store, 9);

        PausingFaults.Pause pauseA = faults.PauseAt(FaultSites.ObjectBeforeFlush, ObjectTestData.Id(1));
        Task<ObjectRecordInfo> a = PutAsync(store, 1);
        await pauseA.Reached;

        // While the delete waits for its barrier, put A's flush runs first and fails, dropping the tombstone too.
        lossy.BeforeFlush = (p => p.EndsWith(ObjectStore.SegmentExtension, StringComparison.Ordinal), async () =>
        {
            lossy.FailNextFlushes = 1;
            pauseA.Release();
            for (int i = 0; i < 2000 && lossy.FailNextFlushes > 0; i++)
                await Task.Delay(1);
            Assert.Equal(0, lossy.FailNextFlushes);
        });

        bool deleted;
        try
        {
            await store.DeleteAsync(ObjectTestData.Id(9));
            deleted = true;
        }
        catch (IOException)
        {
            deleted = false;
        }

        await Assert.ThrowsAnyAsync<IOException>(async () => await a);
        await store.DisposeAsync();
        sim.Crash(CrashMode.LoseUnflushed);

        await using ObjectStore reopened = await OpenAsync(sim);
        if (deleted)
            Assert.False(await reopened.ContainsAsync(ObjectTestData.Id(9)), "an acknowledged delete was lost: its tombstone was dropped by a failed flush");
        Assert.False(deleted);
        Assert.Equal(ObjectTestData.Content(9, 1000), await ObjectTestData.ReadAllAsync(reopened, ObjectTestData.Id(9)));
    }

    /// <summary>
    /// Two uploads are written to the active segment and are still waiting for their flush when a third upload rolls
    /// over to a new segment and is acknowledged. A power loss then persists an arbitrary subset of the unflushed
    /// writes. The rolled-over segment is no longer the newest one at the next open, so it must not contain a record
    /// whose payload was only partly persisted (sealed segments are not payload-verified at open).
    /// </summary>
    [Fact]
    public async Task PutsStillAwaitingTheirBarrier_WhenAnotherPutRollsTheSegment_AreNeverIndexedPartially()
    {
        for (ulong seed = 1; seed <= 48; seed++)
        {
            SimulatedMedium medium = NewMedium(seed);
            var faults = new PausingFaults();
            ObjectStore store = await OpenAsync(medium, faults, segmentSize: 2600);
            await PutAsync(store, 9, 100);

            PausingFaults.Pause pauseA = faults.PauseAt(FaultSites.ObjectBeforeFlush, ObjectTestData.Id(1));
            PausingFaults.Pause pauseC = faults.PauseAt(FaultSites.ObjectBeforeFlush, ObjectTestData.Id(3));
            medium.Faults.ShortNextWrites = 1; // the first upload reaches the device as two separate writes
            Task a = PutAsync(store, 1);
            await pauseA.Reached;
            Task c = PutAsync(store, 3);
            await pauseC.Reached;
            await PutAsync(store, 2); // does not fit: rolls over and is acknowledged
            Assert.False(a.IsCompleted);
            Assert.False(c.IsCompleted);

            medium.Crash(CrashMode.RandomSubset);

            await using ObjectStore reopened = await OpenAsync(medium, segmentSize: 2600);
            Assert.True(await reopened.ContainsAsync(ObjectTestData.Id(2)), $"seed {seed}: acknowledged object lost");
            Assert.True(await reopened.ContainsAsync(ObjectTestData.Id(9)), $"seed {seed}: acknowledged object lost");
            foreach (int n in new[] { 1, 2, 3, 9 })
            {
                if (!await reopened.ContainsAsync(ObjectTestData.Id(n)))
                    continue;
                int length = n == 9 ? 100 : 1000;
                byte[] read;
                try
                {
                    read = await ObjectTestData.ReadAllAsync(reopened, ObjectTestData.Id(n));
                }
                catch (DfsIntegrityException ex)
                {
                    throw new Xunit.Sdk.XunitException($"seed {seed}: object {n} was indexed although its record is damaged: {ex.Message}");
                }

                Assert.Equal(ObjectTestData.Content(n, length), read);
            }
        }
    }

    /// <summary>
    /// An acknowledged object is uploaded again under a new staging session and the second upload fails before its
    /// barrier. The store voids the unacknowledged record durably; after a restart the void record must retire only that
    /// record and never also hide the acknowledged copy written before it.
    /// </summary>
    [Theory]
    [InlineData(CrashMode.LoseUnflushed)]
    [InlineData(CrashMode.KeepUnflushed)]
    public async Task FailedRePutUnderNewStaging_NeverHidesTheAcknowledgedCopyAfterRestart(CrashMode mode)
    {
        SimulatedMedium medium = NewMedium();
        var faults = new RecordingFaults();
        ObjectId id = ObjectTestData.Id(1);
        ObjectStore store = await OpenAsync(medium, faults);
        ObjectRecordInfo acknowledged = await PutAsync(store, 1);
        faults.ThrowAtSite = FaultSites.ObjectBeforeFlush;
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await PutAsync(store, 1, staging: ObjectTestData.Staging2));
        Assert.Equal(acknowledged, store.TryGetInfo(id));
        Assert.Equal(ObjectTestData.Content(1, 1000), await ObjectTestData.ReadAllAsync(store, id));

        medium.Crash(mode);

        await using ObjectStore reopened = await OpenAsync(medium);
        Assert.True(await reopened.ContainsAsync(id), "the acknowledged object was lost after a failed re-upload");
        Assert.Equal(acknowledged, reopened.TryGetInfo(id));
        Assert.Equal(ObjectTestData.Content(1, 1000), await ObjectTestData.ReadAllAsync(reopened, id));
    }

    /// <summary>
    /// Crash matrix of a failed re-upload under a new staging session followed by more work: whatever survives, the
    /// acknowledged copy stays readable and the store never indexes a damaged record.
    /// </summary>
    [Fact]
    public async Task CrashMatrix_FailedRePutUnderNewStaging_KeepsTheAcknowledgedCopy()
    {
        bool put3 = false;
        await CrashMatrix.RunAsync(
            async medium =>
            {
                put3 = false;
                await using ObjectStore store = await OpenAsync(medium, segmentSize: 3000);
                await PutAsync(store, 1);
                await PutAsync(store, 2);
            },
            async medium =>
            {
                var faults = new RecordingFaults { ThrowAtSite = FaultSites.ObjectBeforeFlush };
                ObjectStore store = await OpenAsync(medium, faults, 3000);
                await Assert.ThrowsAsync<InvalidOperationException>(async () => await PutAsync(store, 1, staging: ObjectTestData.Staging2));
                await PutAsync(store, 3);
                put3 = true;
                await store.DisposeAsync();
            },
            async (medium, crash) =>
            {
                await using ObjectStore store = await OpenAsync(medium, segmentSize: 3000);
                foreach (int n in new[] { 1, 2 })
                {
                    Assert.True(await store.ContainsAsync(ObjectTestData.Id(n)), $"acknowledged object {n} was lost");
                    Assert.Equal(ObjectTestData.Content(n, 1000), await ObjectTestData.ReadAllAsync(store, ObjectTestData.Id(n)));
                }

                if (put3)
                    Assert.True(await store.ContainsAsync(ObjectTestData.Id(3)), "acknowledged object 3 was lost");
                if (await store.ContainsAsync(ObjectTestData.Id(3)))
                    Assert.Equal(ObjectTestData.Content(3, 1000), await ObjectTestData.ReadAllAsync(store, ObjectTestData.Id(3)));
                Assert.DoesNotContain(store.Quarantined, q => q.Id is not null);
                return string.Join(",", store.List().Select(i => $"{i.Id}:{i.Staging}"));
            },
            seedsPerRandomMode: 2);
    }

    /// <summary>
    /// A re-upload under a new staging session is damaged on the device; the open that finds it (in the newest segment)
    /// quarantines it durably and falls back to the earlier intact copy. Once that segment is no longer the newest
    /// (its payloads are then not re-verified at open) the quarantine record must still hide only the damaged record,
    /// not the intact copy that precedes it.
    /// </summary>
    [Fact]
    public async Task QuarantinedRecordInASealedSegment_DoesNotHideTheEarlierIntactCopy()
    {
        SimulatedMedium medium = NewMedium();
        ObjectId id = ObjectTestData.Id(1);
        ObjectRecordInfo first;
        await using (ObjectStore store = await OpenAsync(medium, segmentSize: 4000))
        {
            first = await PutAsync(store, 1);
            await PutAsync(store, 1, staging: ObjectTestData.Staging2);
            ObjectPhysicalLocation location = store.GetPhysicalLocation(id)!;
            medium.CorruptByte(location.SegmentPath, location.PayloadOffset + 100);
        }

        await using (ObjectStore store = await OpenAsync(medium, segmentSize: 4000))
        {
            Assert.Single(store.Quarantined, q => q.Id == id);
            Assert.Equal(first, store.TryGetInfo(id));
            Assert.Equal(ObjectTestData.Content(1, 1000), await ObjectTestData.ReadAllAsync(store, id));
            await PutAsync(store, 2, 3000); // does not fit: the segment holding both copies is sealed
            Assert.Equal(2, (await medium.ListAsync(Dir)).Count(n => n.EndsWith(ObjectStore.SegmentExtension, StringComparison.Ordinal)));
        }

        medium.Crash(CrashMode.LoseUnflushed);
        await using ObjectStore reopened = await OpenAsync(medium, segmentSize: 4000);
        Assert.True(await reopened.ContainsAsync(id), "the intact copy was hidden by the quarantine record of a later damaged copy");
        Assert.Equal(first, reopened.TryGetInfo(id));
        Assert.Equal(ObjectTestData.Content(1, 1000), await ObjectTestData.ReadAllAsync(reopened, id));
        Assert.Equal(ObjectTestData.Content(2, 3000), await ObjectTestData.ReadAllAsync(reopened, ObjectTestData.Id(2)));
        Assert.Empty(reopened.Quarantined);
    }

    /// <summary>
    /// A newer copy of an object (re-upload under another staging session) is damaged in a segment that is no longer the
    /// newest, and no quarantine record exists yet. The open must not let the damaged copy replace the intact earlier
    /// copy (it would make the object unreadable although intact bytes are on disk); it retires the damaged one instead.
    /// </summary>
    [Fact]
    public async Task DamagedNewerCopyInASealedSegment_DoesNotDisplaceTheIntactCopy()
    {
        SimulatedMedium medium = NewMedium();
        ObjectId id = ObjectTestData.Id(1);
        ObjectRecordInfo first;
        await using (ObjectStore store = await OpenAsync(medium, segmentSize: 4000))
        {
            first = await PutAsync(store, 1);
            await PutAsync(store, 1, staging: ObjectTestData.Staging2);
            ObjectPhysicalLocation location = store.GetPhysicalLocation(id)!;
            medium.CorruptByte(location.SegmentPath, location.PayloadOffset + 100);
            await PutAsync(store, 2, 3000); // does not fit: the segment holding both copies is sealed
        }

        medium.Crash(CrashMode.LoseUnflushed);
        await using (ObjectStore reopened = await OpenAsync(medium, segmentSize: 4000))
        {
            Assert.Equal(first, reopened.TryGetInfo(id));
            Assert.Equal(ObjectTestData.Content(1, 1000), await ObjectTestData.ReadAllAsync(reopened, id));
            Assert.Single(reopened.Quarantined, q => q.Id == id);
        }

        medium.Crash(CrashMode.LoseUnflushed);
        await using ObjectStore again = await OpenAsync(medium, segmentSize: 4000);
        Assert.Equal(first, again.TryGetInfo(id));
        Assert.Empty(again.Quarantined); // retired durably by the previous open
    }

    /// <summary>
    /// The open that finds a damaged record in the newest segment writes its quarantine record. When that segment is
    /// full, the quarantine record must still go into it: rolling over first would seal the damaged record into a
    /// segment that is no longer payload-verified at open, and a crash before the quarantine record is durable would
    /// then let the next open index the damaged record.
    /// </summary>
    [Fact]
    public async Task CrashMatrix_OpenTimeQuarantineOfAFullSegment_NeverIndexesTheDamagedRecord()
    {
        const long SegmentSize = 2300; // header + two 1000-byte objects fit, a quarantine record after them does not
        bool put3 = false;
        await CrashMatrix.RunAsync(
            async medium =>
            {
                put3 = false;
                ObjectPhysicalLocation location;
                await using (ObjectStore store = await OpenAsync(medium, segmentSize: SegmentSize))
                {
                    await PutAsync(store, 1);
                    await PutAsync(store, 2);
                    location = store.GetPhysicalLocation(ObjectTestData.Id(2))!;
                }

                medium.CorruptByte(location.SegmentPath, location.PayloadOffset + 500);
                medium.Crash(CrashMode.LoseUnflushed);
            },
            async medium =>
            {
                ObjectStore store = await OpenAsync(medium, segmentSize: SegmentSize);
                await PutAsync(store, 3);
                put3 = true;
                await store.DisposeAsync();
            },
            async (medium, crash) =>
            {
                await using ObjectStore store = await OpenAsync(medium, segmentSize: SegmentSize);
                Assert.Equal(ObjectTestData.Content(1, 1000), await ObjectTestData.ReadAllAsync(store, ObjectTestData.Id(1)));
                Assert.False(await store.ContainsAsync(ObjectTestData.Id(2)), "a damaged record was indexed");
                if (put3)
                    Assert.True(await store.ContainsAsync(ObjectTestData.Id(3)), "acknowledged object 3 was lost");
                if (await store.ContainsAsync(ObjectTestData.Id(3)))
                    Assert.Equal(ObjectTestData.Content(3, 1000), await ObjectTestData.ReadAllAsync(store, ObjectTestData.Id(3)));
                return string.Join(",", store.List().Select(i => i.Id.ToString()));
            },
            seedsPerRandomMode: 2);
    }

    /// <summary>
    /// An upload fails before its barrier after its bytes were damaged on the way to the device (a write-back error
    /// can leave a record whose header persisted but whose payload did not). The record is voided; when the active
    /// segment is full, the void record must still go into that segment, so that a crash before it is durable leaves
    /// the damaged record in the newest (payload-verified) segment rather than sealed behind a new one.
    /// </summary>
    [Fact]
    public async Task CrashMatrix_VoidOfADamagedUnacknowledgedRecordInAFullSegment_NeverIndexesIt()
    {
        const long SegmentSize = 2300;
        SimulatedMedium? simulated = null;
        bool put3 = false;
        await CrashMatrix.RunAsync(
            async medium =>
            {
                simulated = medium;
                put3 = false;
                await using ObjectStore store = await OpenAsync(medium, segmentSize: SegmentSize);
                await PutAsync(store, 1);
            },
            async medium =>
            {
                string segment = Dir + "/" + ObjectStore.SegmentFileName(new ObjectStore.SegmentKey(1, 0));
                long payloadOfSecond = ObjectStore.SegmentHeaderSize + 1108 + 108;
                var faults = new RecordingFaults
                {
                    ThrowAtSite = FaultSites.ObjectBeforeFlush,
                    ThrowFactory = () =>
                    {
                        simulated!.CorruptByte(segment, payloadOfSecond + 10); // the record's pending bytes are damaged
                        return new IOException("write-back failed");
                    },
                };
                ObjectStore store = await OpenAsync(medium, faults, SegmentSize);
                await Assert.ThrowsAsync<IOException>(async () => await PutAsync(store, 2));
                await PutAsync(store, 3);
                put3 = true;
                await store.DisposeAsync();
            },
            async (medium, crash) =>
            {
                await using ObjectStore store = await OpenAsync(medium, segmentSize: SegmentSize);
                Assert.Equal(ObjectTestData.Content(1, 1000), await ObjectTestData.ReadAllAsync(store, ObjectTestData.Id(1)));
                Assert.False(await store.ContainsAsync(ObjectTestData.Id(2)), "a damaged unacknowledged record was indexed");
                if (put3)
                    Assert.True(await store.ContainsAsync(ObjectTestData.Id(3)), "acknowledged object 3 was lost");
                if (await store.ContainsAsync(ObjectTestData.Id(3)))
                    Assert.Equal(ObjectTestData.Content(3, 1000), await ObjectTestData.ReadAllAsync(store, ObjectTestData.Id(3)));
                return string.Join(",", store.List().Select(i => i.Id.ToString()));
            },
            seedsPerRandomMode: 2);
    }

    /// <summary>
    /// A client uploads an object whose (plaintext-stored) bytes embed a complete, well-formed record of another object
    /// in another volume. The upload is torn by a crash after the embedded record reached the device. Bytes inside a
    /// record whose header is intact are payload, never records: the open must cut the torn tail and must not index
    /// the embedded record (it would claim an id this store never accepted, and block the genuine upload of that id).
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task TornUploadWhosePayloadEmbedsAForgedRecord_NeverIndexesIt(bool sealedSegment)
    {
        ObjectId forgedId = ObjectTestData.Id(66);
        byte[] forged;
        var scratch = NewMedium(9);
        await using (ObjectStore other = await OpenAsync(scratch))
        {
            await other.PutAsync(forgedId, ObjectTestData.VolumeB, new StagingId(77), ObjectTestData.Content(66, 300));
            ObjectPhysicalLocation at = other.GetPhysicalLocation(forgedId)!;
            forged = scratch.ReadCurrent(at.SegmentPath).AsSpan((int)at.RecordOffset, (int)(at.HeaderLength + at.StoredLength)).ToArray();
        }

        SimulatedMedium medium = NewMedium();
        byte[] hostile = [.. ObjectTestData.Content(5, 200), .. forged, .. ObjectTestData.Content(6, 400)];
        string segmentPath;
        long cutAt;
        await using (ObjectStore store = await OpenAsync(medium, segmentSize: 6000))
        {
            await PutAsync(store, 1);
            await store.PutAsync(ObjectTestData.Id(2), ObjectTestData.VolumeA, ObjectTestData.Staging1, hostile);
            ObjectPhysicalLocation location = store.GetPhysicalLocation(ObjectTestData.Id(2))!;
            segmentPath = location.SegmentPath;
            cutAt = location.PayloadOffset + hostile.Length - 100; // the embedded record is complete, the upload is not
        }

        byte[] image = medium.ReadDurable(segmentPath)[..(int)cutAt];
        await using (IStorageFile file = await medium.OpenAsync(segmentPath, StorageOpenMode.Truncate))
        {
            await file.WriteFullyAsync(0, image);
            await file.FlushAsync();
        }

        if (sealedSegment)
        {
            // A newer segment exists, so the damaged one is not the newest (not payload-verified, tail not cut).
            string newer = Dir + "/" + ObjectStore.SegmentFileName(new ObjectStore.SegmentKey(9, 0));
            await using IStorageFile file = await medium.OpenAsync(newer, StorageOpenMode.CreateNew);
            await file.WriteFullyAsync(0, ObjectStore.EncodeSegmentHeader(new ObjectStore.SegmentKey(9, 0)));
            await file.FlushAsync();
        }

        await medium.FlushDirectoryAsync(Dir);
        medium.Crash(CrashMode.LoseUnflushed);

        await using ObjectStore reopened = await OpenAsync(medium, segmentSize: 6000);
        Assert.False(await reopened.ContainsAsync(forgedId), "a record embedded in an upload's bytes was indexed");
        Assert.False(await reopened.ContainsAsync(ObjectTestData.Id(2)));
        Assert.Equal(ObjectTestData.Content(1, 1000), await ObjectTestData.ReadAllAsync(reopened, ObjectTestData.Id(1)));

        // The genuine object with that id can still be stored here.
        await reopened.PutAsync(forgedId, ObjectTestData.VolumeA, ObjectTestData.Staging1, ObjectTestData.Content(1, 50));
        Assert.Equal(ObjectTestData.Content(1, 50), await ObjectTestData.ReadAllAsync(reopened, forgedId));
    }

    /// <summary>
    /// The store is disposed (node shutdown) while a compaction is between sealing its inputs and copying them. The
    /// compaction must not mistake "store closed" for "record no longer live": it must never commit a layout without
    /// the live records and delete their only segments.
    /// </summary>
    [Fact]
    public async Task DisposeDuringCompaction_NeverDropsLiveRecords()
    {
        SimulatedMedium medium = NewMedium();
        var faults = new PausingFaults();
        ObjectStore store = await OpenAsync(medium, faults, segmentSize: 2600);
        for (int n = 1; n <= 6; n++)
            await PutAsync(store, n);
        await store.DeleteAsync(ObjectTestData.Id(2));

        PausingFaults.Pause pause = faults.PauseAt(FaultSites.CompactionCopy);
        Task compaction = store.CompactAsync().AsTask();
        await pause.Reached;
        await store.DisposeAsync();
        pause.Release();
        try
        {
            await compaction;
        }
        catch (ObjectDisposedException)
        {
        }

        medium.Crash(CrashMode.LoseUnflushed);
        await using ObjectStore reopened = await OpenAsync(medium, segmentSize: 2600);
        foreach (int n in new[] { 1, 3, 4, 5, 6 })
            Assert.Equal(ObjectTestData.Content(n, 1000), await ObjectTestData.ReadAllAsync(reopened, ObjectTestData.Id(n)));
        Assert.False(await reopened.ContainsAsync(ObjectTestData.Id(2)));
    }

    /// <summary>
    /// The store is disposed while compaction writes its manifest (its commit point). The committed compaction must be
    /// left to the next open (which rolls it forward) rather than half-published by a closed store.
    /// </summary>
    [Fact]
    public async Task DisposeWhileCompactionCommits_IsRolledForwardAtTheNextOpen()
    {
        SimulatedMedium sim = NewMedium();
        var medium = new LossyFlushMedium(sim);
        ObjectStore store = await OpenAsync(medium, segmentSize: 2600);
        for (int n = 1; n <= 6; n++)
            await PutAsync(store, n);
        await store.DeleteAsync(ObjectTestData.Id(2));

        medium.BeforeFlush = (p => p.EndsWith(ObjectStore.ManifestFileName + AtomicFile.TempSuffix, StringComparison.Ordinal), async () => await store.DisposeAsync());
        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await store.CompactAsync());
        Assert.Null(medium.BeforeFlush);

        sim.Crash(CrashMode.LoseUnflushed);
        await using ObjectStore reopened = await OpenAsync(sim, segmentSize: 2600);
        foreach (int n in new[] { 1, 3, 4, 5, 6 })
            Assert.Equal(ObjectTestData.Content(n, 1000), await ObjectTestData.ReadAllAsync(reopened, ObjectTestData.Id(n)));
        Assert.False(await reopened.ContainsAsync(ObjectTestData.Id(2)));
        IReadOnlyList<string> names = await sim.ListAsync(Dir);
        Assert.DoesNotContain(ObjectStore.ManifestFileName, names);
        Assert.DoesNotContain(names, n => n.EndsWith(ObjectStore.CompactingExtension, StringComparison.Ordinal));
        Assert.True(reopened.TotalBytes - reopened.LiveBytes <= 3 * ObjectStore.SegmentHeaderSize, "the compaction was not rolled forward");
    }

    /// <summary>
    /// A read error (not a verification failure) while compaction copies a live record must not make compaction
    /// discard the record: the bytes may be intact, and compaction deletes the only local copy once it commits.
    /// </summary>
    [Fact]
    public async Task ReadErrorDuringCompactionCopy_NeverDiscardsTheRecord()
    {
        SimulatedMedium medium = NewMedium();
        var faults = new PausingFaults();
        await using (ObjectStore store = await OpenAsync(medium, faults, segmentSize: 2600))
        {
            for (int n = 1; n <= 6; n++)
                await PutAsync(store, n);
            await store.DeleteAsync(ObjectTestData.Id(2));

            PausingFaults.Pause pause = faults.PauseAt(FaultSites.CompactionCopy);
            Task compaction = store.CompactAsync().AsTask();
            await pause.Reached;
            medium.Faults.FailNextReads = 1; // a transient device error on the first copy read
            pause.Release();
            try
            {
                await compaction;
            }
            catch (IOException)
            {
            }

            foreach (int n in new[] { 1, 3, 4, 5, 6 })
                Assert.Equal(ObjectTestData.Content(n, 1000), await ObjectTestData.ReadAllAsync(store, ObjectTestData.Id(n)));
            await store.CompactAsync(); // a later compaction succeeds
        }

        medium.Crash(CrashMode.LoseUnflushed);
        await using ObjectStore reopened = await OpenAsync(medium, segmentSize: 2600);
        foreach (int n in new[] { 1, 3, 4, 5, 6 })
            Assert.Equal(ObjectTestData.Content(n, 1000), await ObjectTestData.ReadAllAsync(reopened, ObjectTestData.Id(n)));
        Assert.False(await reopened.ContainsAsync(ObjectTestData.Id(2)));
    }

    /// <summary>Same as above, but the segment is sealed by a compaction (which retires it) instead of a rollover.</summary>
    [Fact]
    public async Task PutsStillAwaitingTheirBarrier_WhenCompactionSealsTheSegment_AreNeverIndexedPartially()
    {
        for (ulong seed = 1; seed <= 48; seed++)
        {
            SimulatedMedium medium = NewMedium(seed);
            var faults = new PausingFaults();
            ObjectStore store = await OpenAsync(medium, faults);
            await PutAsync(store, 9, 100);
            await PutAsync(store, 8, 100);
            await store.DeleteAsync(ObjectTestData.Id(8)); // dead bytes, so compaction has work to do

            PausingFaults.Pause pauseA = faults.PauseAt(FaultSites.ObjectBeforeFlush, ObjectTestData.Id(1));
            PausingFaults.Pause pauseC = faults.PauseAt(FaultSites.ObjectBeforeFlush, ObjectTestData.Id(3));
            PausingFaults.Pause pauseCopy = faults.PauseAt(FaultSites.CompactionCopy);
            medium.Faults.ShortNextWrites = 1;
            Task a = PutAsync(store, 1);
            await pauseA.Reached;
            Task c = PutAsync(store, 3);
            await pauseC.Reached;
            Task compaction = store.CompactAsync().AsTask(); // seals the segment, then waits for the paused puts
            await PutAsync(store, 2); // goes to a new, newer segment and is acknowledged
            Assert.False(a.IsCompleted);
            Assert.False(c.IsCompleted);
            Assert.False(compaction.IsCompleted);
            Assert.False(pauseCopy.Reached.IsCompleted);

            medium.Crash(CrashMode.RandomSubset);

            await using ObjectStore reopened = await OpenAsync(medium);
            foreach (int n in new[] { 2, 9 })
                Assert.True(await reopened.ContainsAsync(ObjectTestData.Id(n)), $"seed {seed}: acknowledged object {n} lost");
            Assert.False(await reopened.ContainsAsync(ObjectTestData.Id(8)), $"seed {seed}: deleted object resurrected");
            foreach (int n in new[] { 1, 2, 3, 9 })
            {
                if (!await reopened.ContainsAsync(ObjectTestData.Id(n)))
                    continue;
                int length = n == 9 ? 100 : 1000;
                byte[] read;
                try
                {
                    read = await ObjectTestData.ReadAllAsync(reopened, ObjectTestData.Id(n));
                }
                catch (DfsIntegrityException ex)
                {
                    throw new Xunit.Sdk.XunitException($"seed {seed}: object {n} was indexed although its record is damaged: {ex.Message}");
                }

                Assert.Equal(ObjectTestData.Content(n, length), read);
            }
        }
    }
}
