using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Tests.Unit.Internal.Storage.Infrastructure;

namespace Tedd.Tdfs.Tests.Unit.Internal.Storage;

internal static class ObjectTestData
{
    public const string Dir = "objects";

    public static readonly VolumeId VolumeA = new(new Guid("0a0a0a0a-0000-4000-8000-000000000001"));
    public static readonly VolumeId VolumeB = new(new Guid("0b0b0b0b-0000-4000-8000-000000000002"));
    public static readonly StagingId Staging1 = new(1);
    public static readonly StagingId Staging2 = new(2);

    public static ObjectId Id(int n) => new(new UInt128(0x5EED_0000_0000_0000UL, (ulong)n));

    public static byte[] Content(int n, int length) => TestData.Bytes(1000UL + (ulong)n, length);

    public static async Task<byte[]> ReadAllAsync(ObjectStore store, ObjectId id)
    {
        ObjectRecordInfo info = store.TryGetInfo(id) ?? throw new DfsObjectMissingException(id);
        var buffer = new byte[info.Length];
        int read = await store.ReadAsync(id, 0, buffer);
        Assert.Equal(info.Length, read);
        return buffer;
    }

    public static RecordCipher NewCipher(ulong seed = 3)
        => new(KeyRing.Derive(TestData.Bytes(55, 32), StorageKeys.ObjectPurpose), new SeededRandom(seed));
}

public abstract class ObjectStoreContractTests : StorageContractTests
{
    private const string Dir = ObjectTestData.Dir;

    protected virtual IRecordCipher? Cipher => null;

    protected ObjectStoreOptions Options(long segmentSize = 1 << 20, long reserved = 0)
        => new(segmentSize, reserved, 4 << 20, Cipher);

    protected ValueTask<ObjectStore> OpenAsync(ObjectStoreOptions? options = null, IStorageMedium? medium = null)
        => ObjectStore.OpenAsync(medium ?? Medium, Dir, options ?? Options(), NoFaults.Instance);

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(1000)]
    [InlineData(ObjectStore.UnitSize - 1)]
    [InlineData(ObjectStore.UnitSize)]
    [InlineData(ObjectStore.UnitSize + 1)]
    [InlineData((3 * ObjectStore.UnitSize) + 17)]
    public async Task PutRead_RoundTripsAllSizes(int length)
    {
        byte[] content = ObjectTestData.Content(length, length);
        await using (ObjectStore store = await OpenAsync())
        {
            ObjectRecordInfo info = await store.PutAsync(ObjectTestData.Id(1), ObjectTestData.VolumeA, ObjectTestData.Staging1, content);
            Assert.Equal(length, info.Length);
            Assert.Equal(Tedd.Tdfs.Data.ObjectChecksum.Compute(content), info.Checksum);
            Assert.Equal(ObjectTestData.VolumeA, info.Volume);
            Assert.Equal(ObjectTestData.Staging1, info.Staging);
            Assert.Equal(Cipher is null ? ObjectEncoding.Plain : ObjectEncoding.SealedUnits, info.Encoding);
            Assert.Equal(Cipher is null ? 0u : 1u, info.KeyVersion);
            Assert.Equal(content, await ObjectTestData.ReadAllAsync(store, ObjectTestData.Id(1)));
        }

        await using ObjectStore reopened = await OpenAsync(medium: Host.Restart());
        Assert.Equal(content, await ObjectTestData.ReadAllAsync(reopened, ObjectTestData.Id(1)));
        Assert.True(await reopened.ContainsAsync(ObjectTestData.Id(1)));
    }

    [Fact]
    public async Task RangeReads_ReturnExactBytes()
    {
        int length = (3 * ObjectStore.UnitSize) + 500;
        byte[] content = ObjectTestData.Content(2, length);
        await using ObjectStore store = await OpenAsync();
        await store.PutAsync(ObjectTestData.Id(2), ObjectTestData.VolumeA, ObjectTestData.Staging1, content);
        foreach ((long offset, int count) in new (long, int)[] { (0, 10), (ObjectStore.UnitSize - 5, 10), (ObjectStore.UnitSize, ObjectStore.UnitSize),
                     (100, 2 * ObjectStore.UnitSize), (length - 7, 7), (length - 7, 100), (12345, 1) })
        {
            var buffer = new byte[count];
            int read = await store.ReadAsync(ObjectTestData.Id(2), offset, buffer);
            int expected = (int)Math.Min(count, length - offset);
            Assert.Equal(expected, read);
            Assert.Equal(content.AsSpan((int)offset, expected).ToArray(), buffer.AsSpan(0, expected).ToArray());
        }

        Assert.Equal(0, await store.ReadAsync(ObjectTestData.Id(2), length, new byte[10]));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await store.ReadAsync(ObjectTestData.Id(2), length + 1, new byte[1]));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await store.ReadAsync(ObjectTestData.Id(2), -1, new byte[1]));
    }

    [Fact]
    public async Task MissingObject_ThrowsIntegrityWithMissingDetail()
    {
        await using ObjectStore store = await OpenAsync();
        DfsObjectMissingException ex = await Assert.ThrowsAsync<DfsObjectMissingException>(async () => await store.ReadAsync(ObjectTestData.Id(9), 0, new byte[1]));
        Assert.True(ex.IsMissing);
        Assert.Equal(DfsErrorCode.Integrity, ex.ErrorCode);
        Assert.Equal(ObjectTestData.Id(9), ex.ObjectId);
        Assert.IsAssignableFrom<DfsIntegrityException>(ex);
        Assert.Contains("missing", ex.Message, StringComparison.Ordinal);
        Assert.False(await store.ContainsAsync(ObjectTestData.Id(9)));
    }

    [Fact]
    public async Task IdempotentPut_ReturnsExistingWithoutWriting()
    {
        byte[] content = ObjectTestData.Content(3, 5000);
        await using ObjectStore store = await OpenAsync();
        ObjectRecordInfo first = await store.PutAsync(ObjectTestData.Id(3), ObjectTestData.VolumeA, ObjectTestData.Staging1, content);
        long total = store.TotalBytes;
        ObjectRecordInfo second = await store.PutAsync(ObjectTestData.Id(3), ObjectTestData.VolumeA, ObjectTestData.Staging1, content);
        Assert.Equal(first, second);
        Assert.Equal(total, store.TotalBytes);
    }

    [Fact]
    public async Task ConflictingPut_ThrowsIntegrity()
    {
        await using ObjectStore store = await OpenAsync();
        await store.PutAsync(ObjectTestData.Id(4), ObjectTestData.VolumeA, ObjectTestData.Staging1, ObjectTestData.Content(4, 100));
        DfsIntegrityException ex = await Assert.ThrowsAsync<DfsIntegrityException>(async () =>
            await store.PutAsync(ObjectTestData.Id(4), ObjectTestData.VolumeA, ObjectTestData.Staging1, ObjectTestData.Content(5, 100)));
        Assert.Equal(ObjectTestData.Id(4), ex.ObjectId);
        await Assert.ThrowsAsync<DfsIntegrityException>(async () =>
            await store.PutAsync(ObjectTestData.Id(4), ObjectTestData.VolumeB, ObjectTestData.Staging1, ObjectTestData.Content(4, 100)));
        Assert.Equal(ObjectTestData.Content(4, 100), await ObjectTestData.ReadAllAsync(store, ObjectTestData.Id(4)));
    }

    [Fact]
    public async Task RePutUnderNewStaging_RebindsOwner()
    {
        byte[] content = ObjectTestData.Content(6, 300);
        await using (ObjectStore store = await OpenAsync())
        {
            await store.PutAsync(ObjectTestData.Id(6), ObjectTestData.VolumeA, ObjectTestData.Staging1, content);
            ObjectRecordInfo rebound = await store.PutAsync(ObjectTestData.Id(6), ObjectTestData.VolumeA, ObjectTestData.Staging2, content);
            Assert.Equal(ObjectTestData.Staging2, rebound.Staging);
        }

        await using ObjectStore reopened = await OpenAsync(medium: Host.Restart());
        Assert.Equal(ObjectTestData.Staging2, reopened.TryGetInfo(ObjectTestData.Id(6))!.Staging);
        Assert.Equal(content, await ObjectTestData.ReadAllAsync(reopened, ObjectTestData.Id(6)));
    }

    [Fact]
    public async Task Delete_IsDurable_AndIdempotent()
    {
        await using (ObjectStore store = await OpenAsync())
        {
            await store.PutAsync(ObjectTestData.Id(7), ObjectTestData.VolumeA, ObjectTestData.Staging1, ObjectTestData.Content(7, 100));
            await store.PutAsync(ObjectTestData.Id(8), ObjectTestData.VolumeA, ObjectTestData.Staging1, ObjectTestData.Content(8, 100));
            await store.DeleteAsync(ObjectTestData.Id(7));
            await store.DeleteAsync(ObjectTestData.Id(7));
            await store.DeleteAsync(ObjectTestData.Id(99));
            Assert.False(await store.ContainsAsync(ObjectTestData.Id(7)));
        }

        await using ObjectStore reopened = await OpenAsync(medium: Host.Restart());
        Assert.False(await reopened.ContainsAsync(ObjectTestData.Id(7)));
        Assert.True(await reopened.ContainsAsync(ObjectTestData.Id(8)));
        await Assert.ThrowsAsync<DfsObjectMissingException>(async () => await reopened.ReadAsync(ObjectTestData.Id(7), 0, new byte[1]));
    }

    [Fact]
    public async Task PutAfterDelete_StoresAgain()
    {
        await using (ObjectStore store = await OpenAsync())
        {
            await store.PutAsync(ObjectTestData.Id(10), ObjectTestData.VolumeA, ObjectTestData.Staging1, ObjectTestData.Content(10, 100));
            await store.DeleteAsync(ObjectTestData.Id(10));
            await store.PutAsync(ObjectTestData.Id(10), ObjectTestData.VolumeA, ObjectTestData.Staging2, ObjectTestData.Content(11, 100));
        }

        await using ObjectStore reopened = await OpenAsync(medium: Host.Restart());
        Assert.Equal(ObjectTestData.Content(11, 100), await ObjectTestData.ReadAllAsync(reopened, ObjectTestData.Id(10)));
    }

    [Fact]
    public async Task List_FiltersByVolume_InIdOrder()
    {
        await using ObjectStore store = await OpenAsync();
        foreach (int n in new[] { 5, 1, 3, 2, 4 })
            await store.PutAsync(ObjectTestData.Id(n), n % 2 == 0 ? ObjectTestData.VolumeB : ObjectTestData.VolumeA, ObjectTestData.Staging1, ObjectTestData.Content(n, 10));
        Assert.Equal([1, 2, 3, 4, 5], store.List().Select(i => (int)(ulong)i.Id.Value));
        Assert.Equal([1, 3, 5], store.List(ObjectTestData.VolumeA).Select(i => (int)(ulong)i.Id.Value));
        Assert.Equal([2, 4], store.List(ObjectTestData.VolumeB).Select(i => (int)(ulong)i.Id.Value));
        Assert.Equal(5, store.ObjectCount);
    }

    [Fact]
    public async Task ReopenRebuildsIndex_AcrossManySegments()
    {
        var options = Options(segmentSize: 20_000);
        await using (ObjectStore store = await OpenAsync(options))
        {
            for (int n = 1; n <= 40; n++)
                await store.PutAsync(ObjectTestData.Id(n), ObjectTestData.VolumeA, new StagingId((ulong)n), ObjectTestData.Content(n, 3000 + (n * 10)));
            for (int n = 1; n <= 40; n += 3)
                await store.DeleteAsync(ObjectTestData.Id(n));
        }

        await using ObjectStore reopened = await OpenAsync(options, Host.Restart());
        for (int n = 1; n <= 40; n++)
        {
            if (n % 3 == 1)
                Assert.False(await reopened.ContainsAsync(ObjectTestData.Id(n)));
            else
                Assert.Equal(ObjectTestData.Content(n, 3000 + (n * 10)), await ObjectTestData.ReadAllAsync(reopened, ObjectTestData.Id(n)));
        }

        Assert.Empty(reopened.Quarantined);
    }

    [Fact]
    public async Task Compaction_ReclaimsDeadBytes_AndPreservesLiveObjects()
    {
        var options = Options(segmentSize: 30_000);
        long before;
        long after;
        await using (ObjectStore store = await OpenAsync(options))
        {
            for (int n = 1; n <= 30; n++)
                await store.PutAsync(ObjectTestData.Id(n), ObjectTestData.VolumeA, ObjectTestData.Staging1, ObjectTestData.Content(n, 4000));
            for (int n = 1; n <= 30; n += 2)
                await store.DeleteAsync(ObjectTestData.Id(n));
            before = store.TotalBytes;
            await store.CompactAsync();
            after = store.TotalBytes;
            Assert.True(after < before, $"compaction did not shrink the store ({before} -> {after})");
            int segments = (await Medium.ListAsync(Dir)).Count(n => n.EndsWith(ObjectStore.SegmentExtension, StringComparison.Ordinal));
            Assert.Equal(store.LiveBytes + (segments * ObjectStore.SegmentHeaderSize), after);
            for (int n = 2; n <= 30; n += 2)
                Assert.Equal(ObjectTestData.Content(n, 4000), await ObjectTestData.ReadAllAsync(store, ObjectTestData.Id(n)));
            await store.PutAsync(ObjectTestData.Id(100), ObjectTestData.VolumeA, ObjectTestData.Staging1, ObjectTestData.Content(100, 10));
            await store.CompactAsync(); // nothing dead -> no-op is allowed, must stay consistent
        }

        Assert.DoesNotContain(await Medium.ListAsync(Dir), n => n.EndsWith(ObjectStore.CompactingExtension, StringComparison.Ordinal) || n.StartsWith(ObjectStore.ManifestFileName, StringComparison.Ordinal));
        await using ObjectStore reopened = await OpenAsync(options, Host.Restart());
        for (int n = 1; n <= 30; n++)
        {
            if (n % 2 == 1)
                Assert.False(await reopened.ContainsAsync(ObjectTestData.Id(n)));
            else
                Assert.Equal(ObjectTestData.Content(n, 4000), await ObjectTestData.ReadAllAsync(reopened, ObjectTestData.Id(n)));
        }

        Assert.True(await reopened.ContainsAsync(ObjectTestData.Id(100)));
        Assert.Empty(reopened.Quarantined);
    }

    [Fact]
    public async Task CompactionOfEverythingDeleted_LeavesEmptyStore()
    {
        await using (ObjectStore store = await OpenAsync())
        {
            for (int n = 1; n <= 5; n++)
                await store.PutAsync(ObjectTestData.Id(n), ObjectTestData.VolumeA, ObjectTestData.Staging1, ObjectTestData.Content(n, 1000));
            for (int n = 1; n <= 5; n++)
                await store.DeleteAsync(ObjectTestData.Id(n));
            await store.CompactAsync();
            Assert.Equal(0, store.TotalBytes);
            Assert.Equal(0, store.LiveBytes);
            await store.PutAsync(ObjectTestData.Id(6), ObjectTestData.VolumeA, ObjectTestData.Staging1, ObjectTestData.Content(6, 10));
        }

        await using ObjectStore reopened = await OpenAsync(medium: Host.Restart());
        Assert.Equal([ObjectTestData.Id(6)], reopened.List().Select(i => i.Id));
    }

    [Fact]
    public async Task Scrub_FindsAndQuarantinesCorruptPayload()
    {
        await using (ObjectStore store = await OpenAsync())
        {
            for (int n = 1; n <= 6; n++)
                await store.PutAsync(ObjectTestData.Id(n), ObjectTestData.VolumeA, ObjectTestData.Staging1, ObjectTestData.Content(n, 2 * ObjectStore.UnitSize));
            ObjectPhysicalLocation location = store.GetPhysicalLocation(ObjectTestData.Id(4))!;
            Host.Corrupt(location.SegmentPath, location.PayloadOffset + location.StoredLength - 3);

            ScrubResult first = await store.ScrubAsync(4);
            ScrubResult second = await store.ScrubAsync(4);
            Assert.Equal(4, first.Checked);
            Assert.Equal(new[] { ObjectTestData.Id(4) }, first.Corrupt.Concat(second.Corrupt).ToArray());
            Assert.False(await store.ContainsAsync(ObjectTestData.Id(4)));
            Assert.Contains(store.Quarantined, q => q.Id == ObjectTestData.Id(4));
            Assert.True(store.ChecksumFailures >= 1);
        }

        await using ObjectStore reopened = await OpenAsync(medium: Host.Restart());
        Assert.False(await reopened.ContainsAsync(ObjectTestData.Id(4)));
        Assert.Equal(5, reopened.ObjectCount);
        ScrubResult clean = await reopened.ScrubAsync(100);
        Assert.Equal(5, clean.Checked);
        Assert.Empty(clean.Corrupt);
    }

    [Fact]
    public async Task CorruptUnit_FailsOnlyReadsTouchingIt()
    {
        byte[] content = ObjectTestData.Content(12, 3 * ObjectStore.UnitSize);
        await using ObjectStore store = await OpenAsync();
        await store.PutAsync(ObjectTestData.Id(12), ObjectTestData.VolumeA, ObjectTestData.Staging1, content);
        ObjectPhysicalLocation location = store.GetPhysicalLocation(ObjectTestData.Id(12))!;
        long unitStride = location.StoredLength / 3;
        Host.Corrupt(location.SegmentPath, location.PayloadOffset + (2 * unitStride) + 40);

        var head = new byte[ObjectStore.UnitSize];
        Assert.Equal(head.Length, await store.ReadAsync(ObjectTestData.Id(12), 0, head));
        Assert.Equal(content.AsSpan(0, head.Length).ToArray(), head);
        await Assert.ThrowsAsync<DfsIntegrityException>(async () => await store.ReadAsync(ObjectTestData.Id(12), (2 * ObjectStore.UnitSize) + 10, new byte[5]));
        await Assert.ThrowsAsync<DfsIntegrityException>(async () => await store.ReadAsync(ObjectTestData.Id(12), 0, new byte[content.Length]));
        Assert.True(store.ChecksumFailures >= 2);
    }

    [Fact]
    public async Task CorruptHeaderIdentity_IsNeverIndexed()
    {
        await using (ObjectStore store = await OpenAsync())
        {
            await store.PutAsync(ObjectTestData.Id(13), ObjectTestData.VolumeA, ObjectTestData.Staging1, ObjectTestData.Content(13, 500));
            await store.PutAsync(ObjectTestData.Id(14), ObjectTestData.VolumeA, ObjectTestData.Staging1, ObjectTestData.Content(14, 500));
            await store.PutAsync(ObjectTestData.Id(15), ObjectTestData.VolumeA, ObjectTestData.Staging1, ObjectTestData.Content(15, 500));
            ObjectPhysicalLocation location = store.GetPhysicalLocation(ObjectTestData.Id(14))!;
            Host.Corrupt(location.SegmentPath, location.RecordOffset + 8); // first byte of the object id
        }

        await using ObjectStore reopened = await OpenAsync(medium: Host.Restart());
        Assert.False(await reopened.ContainsAsync(ObjectTestData.Id(14)));
        Assert.Equal(2, reopened.ObjectCount);
        Assert.NotEmpty(reopened.Quarantined);
        Assert.Equal(ObjectTestData.Content(15, 500), await ObjectTestData.ReadAllAsync(reopened, ObjectTestData.Id(15)));
    }

    [Fact]
    public async Task MaxObjectSize_IsEnforced()
    {
        await using ObjectStore store = await ObjectStore.OpenAsync(Medium, Dir, new ObjectStoreOptions(MaxObjectSize: 100, Cipher: Cipher), NoFaults.Instance);
        DfsLimitExceededException ex = await Assert.ThrowsAsync<DfsLimitExceededException>(async () =>
            await store.PutAsync(ObjectTestData.Id(1), ObjectTestData.VolumeA, ObjectTestData.Staging1, new byte[101]));
        Assert.Equal(100, ex.Limit);
        Assert.Equal(101, ex.Requested);
        await store.PutAsync(ObjectTestData.Id(1), ObjectTestData.VolumeA, ObjectTestData.Staging1, new byte[100]);
        await Assert.ThrowsAsync<ArgumentException>(async () => await store.PutAsync(default, ObjectTestData.VolumeA, ObjectTestData.Staging1, new byte[1]));
    }

    [Fact]
    public async Task ConcurrentPutsReadsAndDeletes_StayConsistent()
    {
        var options = Options(segmentSize: 50_000);
        await using (ObjectStore store = await OpenAsync(options))
        {
            var tasks = new List<Task>();
            for (int n = 1; n <= 60; n++)
            {
                int k = n;
                tasks.Add(Task.Run(async () =>
                {
                    await store.PutAsync(ObjectTestData.Id(k), ObjectTestData.VolumeA, ObjectTestData.Staging1, ObjectTestData.Content(k, 2000 + k));
                    Assert.Equal(ObjectTestData.Content(k, 2000 + k), await ObjectTestData.ReadAllAsync(store, ObjectTestData.Id(k)));
                    if (k % 4 == 0)
                        await store.DeleteAsync(ObjectTestData.Id(k));
                    if (k % 15 == 0)
                        await store.CompactAsync();
                }));
            }

            await Task.WhenAll(tasks);
            await store.CompactAsync();
        }

        await using ObjectStore reopened = await OpenAsync(options, Host.Restart());
        for (int n = 1; n <= 60; n++)
        {
            if (n % 4 == 0)
                Assert.False(await reopened.ContainsAsync(ObjectTestData.Id(n)));
            else
                Assert.Equal(ObjectTestData.Content(n, 2000 + n), await ObjectTestData.ReadAllAsync(reopened, ObjectTestData.Id(n)));
        }
    }
}

public sealed class ObjectStoreSimulatedTests : ObjectStoreContractTests
{
    protected override MediumHost CreateHost() => new SimulatedHost();
}

public sealed class ObjectStoreSimulatedEncryptedTests : ObjectStoreContractTests
{
    private readonly RecordCipher _cipher = ObjectTestData.NewCipher();

    protected override MediumHost CreateHost() => new SimulatedHost();

    protected override IRecordCipher? Cipher => _cipher;

    [Fact]
    public async Task StoredBytes_DoNotContainPlaintext()
    {
        byte[] marker = System.Text.Encoding.ASCII.GetBytes("SECRET-PAYLOAD-MARKER-0123456789");
        byte[] content = [.. marker, .. marker, .. marker];
        await using ObjectStore store = await OpenAsync();
        await store.PutAsync(ObjectTestData.Id(1), ObjectTestData.VolumeA, ObjectTestData.Staging1, content);
        ObjectPhysicalLocation location = store.GetPhysicalLocation(ObjectTestData.Id(1))!;
        Assert.Equal(-1, Host.ReadRaw(location.SegmentPath).AsSpan().IndexOf(marker));
    }

    [Fact]
    public async Task KeyRotation_NewObjectsUseNewVersion_OldStayReadable()
    {
        var ring = KeyRing.Derive(TestData.Bytes(60, 32), StorageKeys.ObjectPurpose, 1);
        using var cipher = new RecordCipher(ring, new SeededRandom(4));
        var options = new ObjectStoreOptions(1 << 20, 0, Cipher: cipher);
        await using (ObjectStore store = await ObjectStore.OpenAsync(Medium, ObjectTestData.Dir, options, NoFaults.Instance))
        {
            Assert.Equal(1u, (await store.PutAsync(ObjectTestData.Id(1), ObjectTestData.VolumeA, ObjectTestData.Staging1, ObjectTestData.Content(1, 70_000))).KeyVersion);
            ring.Add(2, StorageKeys.DeriveKey(TestData.Bytes(60, 32), StorageKeys.ObjectPurpose, 2));
            Assert.Equal(2u, (await store.PutAsync(ObjectTestData.Id(2), ObjectTestData.VolumeA, ObjectTestData.Staging1, ObjectTestData.Content(2, 70_000))).KeyVersion);
        }

        await using ObjectStore reopened = await ObjectStore.OpenAsync(Medium, ObjectTestData.Dir, options, NoFaults.Instance);
        Assert.Equal(ObjectTestData.Content(1, 70_000), await ObjectTestData.ReadAllAsync(reopened, ObjectTestData.Id(1)));
        Assert.Equal(ObjectTestData.Content(2, 70_000), await ObjectTestData.ReadAllAsync(reopened, ObjectTestData.Id(2)));
        Assert.Equal(1u, reopened.TryGetInfo(ObjectTestData.Id(1))!.KeyVersion);

        ring.SetCurrent(2);
        ring.Remove(1);
        await Assert.ThrowsAsync<DfsRecoveryKeyMissingException>(async () => await reopened.ReadAsync(ObjectTestData.Id(1), 0, new byte[1]));
    }

    [Fact]
    public async Task OpenWithoutKey_ThrowsRecoveryKeyMissing()
    {
        await using (ObjectStore store = await OpenAsync())
            await store.PutAsync(ObjectTestData.Id(1), ObjectTestData.VolumeA, ObjectTestData.Staging1, ObjectTestData.Content(1, 100));
        await Assert.ThrowsAsync<DfsRecoveryKeyMissingException>(async () =>
            await ObjectStore.OpenAsync(Medium, ObjectTestData.Dir, new ObjectStoreOptions(), NoFaults.Instance));
    }

    [Fact]
    public async Task WrongKey_FailsVerification()
    {
        await using (ObjectStore store = await OpenAsync(Options(segmentSize: 300)))
        {
            await store.PutAsync(ObjectTestData.Id(1), ObjectTestData.VolumeA, ObjectTestData.Staging1, ObjectTestData.Content(1, 100));
            await store.PutAsync(ObjectTestData.Id(2), ObjectTestData.VolumeA, ObjectTestData.Staging1, ObjectTestData.Content(2, 100));
        }

        using var wrong = new RecordCipher(KeyRing.Derive(TestData.Bytes(56, 32), StorageKeys.ObjectPurpose), new SeededRandom(1));
        await using ObjectStore reopened = await ObjectStore.OpenAsync(Medium, ObjectTestData.Dir, new ObjectStoreOptions(300, 0, Cipher: wrong), NoFaults.Instance);
        // Object 1 lives in a sealed segment (not re-verified at open); object 2 in the active one (quarantined at open).
        Assert.False(await reopened.ContainsAsync(ObjectTestData.Id(2)));
        await Assert.ThrowsAsync<DfsIntegrityException>(async () => await reopened.ReadAsync(ObjectTestData.Id(1), 0, new byte[10]));
    }
}

public sealed class ObjectStoreFileSystemTests : ObjectStoreContractTests
{
    protected override MediumHost CreateHost() => new FileSystemHost();
}
