using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Tests.Unit.Internal.Storage.Infrastructure;

namespace Tedd.Tdfs.Tests.Unit.Internal.Storage;

/// <summary>
/// Crash matrix over a mixed sequence of object-store operations (uploads, re-uploads under new staging sessions,
/// uploads that fail before their barrier, deletes, compactions, scrubs), checked against a model of what was
/// acknowledged: every acknowledged object survives intact, every acknowledged delete stays deleted, an object whose
/// last operation was not acknowledged is either state, and nothing indexed ever reads back wrong or damaged.
/// </summary>
public sealed class ObjectStoreMixedCrashMatrixTests
{
    private const string Dir = ObjectTestData.Dir;

    private enum Expect
    {
        Absent,
        Present,
        Either,
    }

    private static int LengthOf(int n) => 300 + (n * 389 % 1700);

    private static ObjectStoreOptions Options(IRecordCipher? cipher) => new(2600, 0, Cipher: cipher);

    /// <summary>Throws at the barrier of selected uploads (by object id and occurrence).</summary>
    private sealed class FailSelectedPuts(params (int Object, int Occurrence)[] failures) : IFaultInjector
    {
        private readonly Dictionary<ObjectId, int> _seen = [];

        public bool IsEnabled => true;

        public ValueTask HitAsync(FaultContext context, CancellationToken ct = default)
        {
            if (context.Site != FaultSites.ObjectBeforeFlush || context.Object is not { } id)
                return default;
            int occurrence = _seen.TryGetValue(id, out int n) ? n + 1 : 1;
            _seen[id] = occurrence;
            if (failures.Any(f => ObjectTestData.Id(f.Object) == id && f.Occurrence == occurrence))
                throw new InvalidOperationException($"injected barrier failure for {id} #{occurrence}");
            return default;
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task MixedOperations_MatchTheAcknowledgedModel(bool encrypted)
    {
        var model = new Dictionary<int, Expect>();
        void Set(int n, Expect e) => model[n] = e;

        await CrashMatrix.RunAsync(
            async medium =>
            {
                model.Clear();
                using RecordCipher? cipher = encrypted ? ObjectTestData.NewCipher(11) : null;
                await using ObjectStore store = await ObjectStore.OpenAsync(medium, Dir, Options(cipher), NoFaults.Instance);
                await PutAsync(store, 9, ObjectTestData.Staging1);
                Set(9, Expect.Present);
                await PutAsync(store, 8, ObjectTestData.Staging1);
                await store.DeleteAsync(ObjectTestData.Id(8));
                Set(8, Expect.Absent);
            },
            async medium =>
            {
                RecordCipher? cipher = encrypted ? ObjectTestData.NewCipher(12) : null;
                var faults = new FailSelectedPuts((3, 1), (1, 4), (5, 1));
                ObjectStore store = await ObjectStore.OpenAsync(medium, Dir, Options(cipher), faults);

                async Task AckPutAsync(int n, StagingId staging)
                {
                    Expect before = model.GetValueOrDefault(n, Expect.Absent);
                    Set(n, before == Expect.Present ? Expect.Present : Expect.Either);
                    await PutAsync(store, n, staging);
                    Set(n, Expect.Present);
                }

                async Task FailedPutAsync(int n, StagingId staging)
                {
                    Expect before = model.GetValueOrDefault(n, Expect.Absent);
                    Set(n, before == Expect.Present ? Expect.Present : Expect.Either);
                    await Assert.ThrowsAsync<InvalidOperationException>(async () => await PutAsync(store, n, staging));
                }

                async Task AckDeleteAsync(int n)
                {
                    Expect before = model.GetValueOrDefault(n, Expect.Absent);
                    Set(n, before == Expect.Absent ? Expect.Absent : Expect.Either);
                    await store.DeleteAsync(ObjectTestData.Id(n));
                    Set(n, Expect.Absent);
                }

                await AckPutAsync(1, ObjectTestData.Staging1);
                await AckPutAsync(2, ObjectTestData.Staging1);
                await AckPutAsync(1, ObjectTestData.Staging2);          // re-upload under a new staging session
                await FailedPutAsync(3, ObjectTestData.Staging1);       // never acknowledged
                await AckDeleteAsync(2);
                await AckPutAsync(1, new StagingId(3));                // second re-upload
                await FailedPutAsync(1, new StagingId(4));             // failed re-upload of an acknowledged object
                await store.CompactAsync();
                await AckPutAsync(4, ObjectTestData.Staging1);
                await FailedPutAsync(5, ObjectTestData.Staging2);
                await AckDeleteAsync(4);
                await AckPutAsync(2, ObjectTestData.Staging2);          // stored again after its delete
                await AckDeleteAsync(9);
                ScrubResult scrub = await store.ScrubAsync(20);
                Assert.Empty(scrub.Corrupt);
                await AckPutAsync(6, ObjectTestData.Staging1);
                await store.CompactAsync();
                await AckPutAsync(3, ObjectTestData.Staging2);          // the id of the failed upload, now acknowledged
                await store.DisposeAsync();
                cipher?.Dispose();
            },
            async (medium, crash) =>
            {
                using RecordCipher? cipher = encrypted ? ObjectTestData.NewCipher(13) : null;
                await using ObjectStore store = await ObjectStore.OpenAsync(medium, Dir, Options(cipher), NoFaults.Instance);
                var present = new List<string>();
                for (int n = 1; n <= 9; n++)
                {
                    Expect expect = model.GetValueOrDefault(n, Expect.Absent);
                    bool contains = await store.ContainsAsync(ObjectTestData.Id(n));
                    if (expect == Expect.Present)
                        Assert.True(contains, $"acknowledged object {n} was lost");
                    if (expect == Expect.Absent)
                        Assert.False(contains, $"object {n} is present although its absence was acknowledged");
                    if (!contains)
                        continue;
                    Assert.Equal(ObjectTestData.Content(n, LengthOf(n)), await ObjectTestData.ReadAllAsync(store, ObjectTestData.Id(n)));
                    present.Add($"{n}:{store.TryGetInfo(ObjectTestData.Id(n))!.Staging}");
                }

                Assert.Equal(present.Count, store.ObjectCount);
                Assert.Empty((await store.ScrubAsync(100)).Corrupt);
                return string.Join(",", present);
            },
            seedsPerRandomMode: 2);
    }

    private static Task<ObjectRecordInfo> PutAsync(ObjectStore store, int n, StagingId staging)
        => store.PutAsync(ObjectTestData.Id(n), ObjectTestData.VolumeA, staging, ObjectTestData.Content(n, LengthOf(n))).AsTask();
}
