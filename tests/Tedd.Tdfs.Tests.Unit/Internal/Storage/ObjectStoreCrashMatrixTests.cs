using System.Globalization;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Tests.Unit.Internal.Storage.Infrastructure;

namespace Tedd.Tdfs.Tests.Unit.Internal.Storage;

/// <summary>
/// Crash matrices for the object store (DSK-001/011/013/018, GC-014): crash before every I/O step of put, delete,
/// compaction and quarantine with every crash mode; recovered state must never lose an acknowledged object, never
/// resurrect an acknowledged delete, never index a partial record and never return wrong bytes.
/// </summary>
public sealed class ObjectStoreCrashMatrixTests
{
    private const string Dir = ObjectTestData.Dir;

    private static ValueTask<ObjectStore> OpenAsync(IStorageMedium medium, long segmentSize, IRecordCipher? cipher = null)
        => ObjectStore.OpenAsync(medium, Dir, new ObjectStoreOptions(segmentSize, 0, Cipher: cipher), NoFaults.Instance);

    private static Task PutAsync(ObjectStore store, int n, int length)
        => store.PutAsync(ObjectTestData.Id(n), ObjectTestData.VolumeA, ObjectTestData.Staging1, ObjectTestData.Content(n, length)).AsTask();

    private static int LengthOf(int n) => 700 + (n * 997 % 3000);

    /// <summary>Checks every listed object reads back exactly and returns a fingerprint of the index.</summary>
    private static async Task<string> VerifyAllAsync(ObjectStore store, Func<int, bool> mustExist, Func<int, bool> mustNotExist, int maxId)
    {
        var present = new List<int>();
        for (int n = 1; n <= maxId; n++)
        {
            bool contains = await store.ContainsAsync(ObjectTestData.Id(n));
            if (mustExist(n))
                Assert.True(contains, $"acknowledged object {n} was lost");
            if (mustNotExist(n))
                Assert.False(contains, $"deleted object {n} was resurrected");
            if (contains)
            {
                Assert.Equal(ObjectTestData.Content(n, LengthOf(n)), await ObjectTestData.ReadAllAsync(store, ObjectTestData.Id(n)));
                present.Add(n);
            }
        }

        Assert.Equal(present.Count, store.ObjectCount);
        Assert.DoesNotContain(store.Quarantined, q => q.Id is not null);
        return string.Join(",", present.Select(p => p.ToString(CultureInfo.InvariantCulture)));
    }

    [Theory]
    [InlineData(1 << 20)]
    [InlineData(6000)]
    public async Task Put_NeverLosesAcknowledgedObjects_NorIndexesPartialOnes(long segmentSize)
    {
        int acknowledged = 0;
        int cases = await CrashMatrix.RunAsync(
            async medium =>
            {
                acknowledged = 3;
                await using ObjectStore store = await OpenAsync(medium, segmentSize);
                for (int n = 1; n <= 3; n++)
                    await PutAsync(store, n, LengthOf(n));
            },
            async medium =>
            {
                ObjectStore store = await OpenAsync(medium, segmentSize);
                for (int n = 4; n <= 6; n++)
                {
                    await PutAsync(store, n, LengthOf(n));
                    acknowledged = n;
                }
            },
            async (medium, crash) =>
            {
                await using ObjectStore store = await OpenAsync(medium, segmentSize);
                return await VerifyAllAsync(store, n => n <= acknowledged, _ => false, 6);
            });
        Assert.True(cases > 30, $"only {cases} cases");
    }

    [Fact]
    public async Task LargePutWithShortUnits_NeverIndexesPartialRecord()
    {
        int length = (3 * ObjectStore.UnitSize) + 11;
        bool acknowledged = false;
        await CrashMatrix.RunAsync(
            _ =>
            {
                acknowledged = false;
                return Task.CompletedTask;
            },
            async medium =>
            {
                ObjectStore store = await OpenAsync(medium, 1 << 20);
                await store.PutAsync(ObjectTestData.Id(1), ObjectTestData.VolumeA, ObjectTestData.Staging1, ObjectTestData.Content(1, length));
                acknowledged = true;
            },
            async (medium, crash) =>
            {
                await using ObjectStore store = await OpenAsync(medium, 1 << 20);
                bool contains = await store.ContainsAsync(ObjectTestData.Id(1));
                Assert.True(contains || !acknowledged);
                if (contains)
                    Assert.Equal(ObjectTestData.Content(1, length), await ObjectTestData.ReadAllAsync(store, ObjectTestData.Id(1)));
                return contains ? "present" : "absent";
            });
    }

    [Fact]
    public async Task Delete_NeverResurrectsAcknowledgedDeletes()
    {
        int deletedThrough = 0;
        await CrashMatrix.RunAsync(
            async medium =>
            {
                deletedThrough = 0;
                await using ObjectStore store = await OpenAsync(medium, 6000);
                for (int n = 1; n <= 6; n++)
                    await PutAsync(store, n, LengthOf(n));
            },
            async medium =>
            {
                ObjectStore store = await OpenAsync(medium, 6000);
                for (int n = 1; n <= 3; n++)
                {
                    await store.DeleteAsync(ObjectTestData.Id(n));
                    deletedThrough = n;
                }
            },
            async (medium, crash) =>
            {
                await using ObjectStore store = await OpenAsync(medium, 6000);
                return await VerifyAllAsync(store, n => n > 3, n => n <= deletedThrough, 6);
            });
    }

    [Theory]
    [InlineData(1 << 20)]
    [InlineData(5000)]
    public async Task Compaction_LeavesOneCompleteLayout(long segmentSize)
    {
        bool compacted = false;
        int cases = await CrashMatrix.RunAsync(
            async medium =>
            {
                compacted = false;
                await using ObjectStore store = await OpenAsync(medium, segmentSize);
                for (int n = 1; n <= 10; n++)
                    await PutAsync(store, n, LengthOf(n));
                for (int n = 1; n <= 10; n += 3)
                    await store.DeleteAsync(ObjectTestData.Id(n));
            },
            async medium =>
            {
                ObjectStore store = await OpenAsync(medium, segmentSize);
                await store.CompactAsync();
                compacted = true;
                await PutAsync(store, 11, LengthOf(11));
            },
            async (medium, crash) =>
            {
                ObjectStore store = await OpenAsync(medium, segmentSize);
                IReadOnlyList<string> names = await medium.ListAsync(Dir);
                Assert.DoesNotContain(names, n => n.EndsWith(ObjectStore.CompactingExtension, StringComparison.Ordinal));
                Assert.DoesNotContain(names, n => n == ObjectStore.ManifestFileName);
                string fingerprint = await VerifyAllAsync(store, n => n <= 10 && n % 3 != 1, n => n % 3 == 1 && n <= 10, 11);
                if (compacted)
                {
                    long dead = store.TotalBytes - store.LiveBytes;
                    int segments = names.Count(n => n.EndsWith(ObjectStore.SegmentExtension, StringComparison.Ordinal));
                    Assert.True(dead <= segments * ObjectStore.SegmentHeaderSize + 6000, $"compaction left {dead} dead bytes");
                }

                await store.DisposeAsync();
                return fingerprint;
            });
        Assert.True(cases > 50, $"only {cases} cases");
    }

    [Fact]
    public async Task CompactionWithConcurrentDelete_DoesNotResurrect()
    {
        bool deleted = false;
        await CrashMatrix.RunAsync(
            async medium =>
            {
                deleted = false;
                await using ObjectStore store = await OpenAsync(medium, 4000);
                for (int n = 1; n <= 6; n++)
                    await PutAsync(store, n, LengthOf(n));
                await store.DeleteAsync(ObjectTestData.Id(1));
            },
            async medium =>
            {
                ObjectStore store = await OpenAsync(medium, 4000);
                Task compaction = store.CompactAsync().AsTask();
                await store.DeleteAsync(ObjectTestData.Id(2));
                deleted = true;
                await compaction;
            },
            async (medium, crash) =>
            {
                await using ObjectStore store = await OpenAsync(medium, 4000);
                return await VerifyAllAsync(store, n => n >= 3, n => n == 1 || (n == 2 && deleted), 6);
            },
            seedsPerRandomMode: 2);
    }

    [Fact]
    public async Task ScrubQuarantine_NeverServesCorruptBytes()
    {
        bool scrubbed = false;
        await CrashMatrix.RunAsync(
            async medium =>
            {
                scrubbed = false;
                ObjectPhysicalLocation location;
                await using (ObjectStore store = await OpenAsync(medium, 5000))
                {
                    for (int n = 1; n <= 5; n++)
                        await PutAsync(store, n, LengthOf(n));
                    location = store.GetPhysicalLocation(ObjectTestData.Id(2))!;
                }

                ((SimulatedMedium)medium).CorruptByte(location.SegmentPath, location.PayloadOffset + 9);
            },
            async medium =>
            {
                ObjectStore store = await OpenAsync(medium, 5000);
                ScrubResult result = await store.ScrubAsync(10);
                Assert.Equal([ObjectTestData.Id(2)], result.Corrupt);
                scrubbed = true;
            },
            async (medium, crash) =>
            {
                await using ObjectStore store = await OpenAsync(medium, 5000);
                if (scrubbed)
                    Assert.False(await store.ContainsAsync(ObjectTestData.Id(2)), "a durably quarantined record was re-indexed");
                if (await store.ContainsAsync(ObjectTestData.Id(2)))
                    await Assert.ThrowsAsync<DfsIntegrityException>(async () => await store.ReadAsync(ObjectTestData.Id(2), 0, new byte[LengthOf(2)]));
                foreach (int n in new[] { 1, 3, 4, 5 })
                    Assert.Equal(ObjectTestData.Content(n, LengthOf(n)), await ObjectTestData.ReadAllAsync(store, ObjectTestData.Id(n)));
                return (await store.ContainsAsync(ObjectTestData.Id(2))).ToString();
            },
            seedsPerRandomMode: 2);
    }

    [Fact]
    public async Task EncryptedPut_RecoversCompleteObjectsOnly()
    {
        int acknowledged = 0;
        await CrashMatrix.RunAsync(
            async medium =>
            {
                acknowledged = 1;
                using RecordCipher cipher = ObjectTestData.NewCipher(1);
                await using ObjectStore store = await OpenAsync(medium, 6000, cipher);
                await PutAsync(store, 1, LengthOf(1));
            },
            async medium =>
            {
                RecordCipher cipher = ObjectTestData.NewCipher(2);
                ObjectStore store = await OpenAsync(medium, 6000, cipher);
                for (int n = 2; n <= 4; n++)
                {
                    await PutAsync(store, n, LengthOf(n));
                    acknowledged = n;
                }
            },
            async (medium, crash) =>
            {
                using RecordCipher cipher = ObjectTestData.NewCipher(3);
                await using ObjectStore store = await OpenAsync(medium, 6000, cipher);
                return await VerifyAllAsync(store, n => n <= acknowledged, _ => false, 4);
            },
            seedsPerRandomMode: 2);
    }
}
