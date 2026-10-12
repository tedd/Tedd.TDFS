using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Unit.Lock;

/// <summary>Namespace operation that requires every open handle on the target inode to grant <see cref="FileShare.Delete"/>.</summary>
public enum DeleteShareOperation
{
    /// <summary><c>Files.DeleteAsync(path)</c> of the open inode.</summary>
    Delete,

    /// <summary><c>Files.MoveAsync(other, path, overwrite: true)</c> replacing the open inode.</summary>
    RenameOverwrite,
}

/// <summary>Lock-order cycle shapes for LOCK-019.</summary>
public enum LockCycleScenario
{
    /// <summary>T1: a then b, T2: b then a, acquired through transaction-bound WriteAllBytes helpers.</summary>
    TwoCycleHelpers,

    /// <summary>As <see cref="TwoCycleHelpers"/> but through transaction-bound writable streams kept open.</summary>
    TwoCycleStreams,

    /// <summary>Both closing acquisitions of the cycle are issued concurrently.</summary>
    TwoCycleSimultaneous,

    /// <summary>T1: a→b, T2: b→c, T3: c→a.</summary>
    ThreeCycle,
}

/// <summary>
/// LOCK — writer leases, sharing and fencing (FSD §7, LOCK-01..LOCK-06; test plan §7.9): unit-level SDK contract tests
/// against an in-memory three-zone cluster. Sharing expectations come from the independent oracle in
/// <see cref="LockHelpers.Compatible"/>, never from the implementation.
/// </summary>
public sealed class LockUnitTests
{
    private const string Vol = LockHelpers.Volume;

    // ------------------------------------------------------------------------------------------------------------
    // LOCK-001
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>Every access × share request that is valid in the core release (16 of 24).</summary>
    public static TheoryData<FileAccess, FileShare> ValidHandleRequests()
    {
        var data = new TheoryData<FileAccess, FileShare>();
        foreach (HandleSpec spec in LockHelpers.AllRequests())
        {
            if (LockHelpers.IsValidCoreRequest(spec))
                data.Add(spec.Access, spec.Share);
        }

        return data;
    }

    /// <summary>
    /// LOCK-001 (pairwise family): for one valid existing handle, enumerate all 24 proposed access/share requests and
    /// open them against it in BOTH orders (existing first, proposed first), alternately from a second client and from
    /// the same client. A pair may coexist iff each handle grants the other's access; otherwise the second open fails
    /// with a typed retryable sharing conflict and leaves no reservation behind. Invalid proposals (writable with
    /// FileShare.Write) are rejected explicitly, never as a contended share.
    /// </summary>
    [Theory]
    [MemberData(nameof(ValidHandleRequests))]
    [Catalog("LOCK-001", "U", "INV-02", "INV-05", "INV-06", "INV-12")]
    public async Task LOCK_001_ShareCompatibilityBothDirections(FileAccess existingAccess, FileShare existingShare)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var existing = new HandleSpec(existingAccess, existingShare);
        Assert.True(LockHelpers.IsValidCoreRequest(existing), "test data: existing handle must be valid");

        await ClusterTest.RunAsync(LockHelpers.UnitCluster(), async cluster =>
        {
            await using DfsClient c0 = await LockHelpers.ConnectAsync(cluster, "c0", ct);
            await using DfsClient c1 = await LockHelpers.ConnectAsync(cluster, "c1", ct);
            DfsVolume v0 = c0.GetVolume(Vol);
            DfsVolume v1 = c1.GetVolume(Vol);
            await v0.Directories.CreateDirectoryAsync("/lock001", ct);

            int index = 0;
            int compatiblePairs = 0;
            int conflictingPairs = 0;
            foreach (HandleSpec proposed in LockHelpers.AllRequests())
            {
                if (!LockHelpers.IsValidCoreRequest(proposed))
                {
                    string invalidPath = $"/lock001/f{index:D3}";
                    await CheckInvalidProposalAsync(cluster, v0, v1, invalidPath, existing, proposed, (ulong)(1000 + index), ct);
                    index++;
                    continue;
                }

                foreach (bool existingFirst in new[] { true, false })
                {
                    HandleSpec first = existingFirst ? existing : proposed;
                    HandleSpec second = existingFirst ? proposed : existing;
                    DfsVolume secondVolume = index % 2 == 0 ? v1 : v0; // sharing is per handle, not per client
                    bool compatible = await CheckPairAsync(cluster, v0, secondVolume, $"/lock001/f{index:D3}", first, second, (ulong)(1000 + index), ct);
                    if (compatible)
                        compatiblePairs++;
                    else
                        conflictingPairs++;
                    index++;
                }
            }

            // The matrix exercised both outcomes for this existing handle (sanity check of the enumeration itself).
            Assert.True(compatiblePairs + conflictingPairs == 32, $"expected 16 valid proposals × 2 orders, got {compatiblePairs + conflictingPairs}");
            Assert.True(conflictingPairs > 0, $"existing {existing}: every row of the matrix contains at least one conflicting proposal");
        }, ct);
    }

    private static readonly Dictionary<string, HandleSpec[]> ExistingHandleSets = new(StringComparer.Ordinal)
    {
        ["two-readers-sharing-everything"] = [new(FileAccess.Read, FileShare.ReadWrite | FileShare.Delete), new(FileAccess.Read, FileShare.ReadWrite | FileShare.Delete)],
        ["readers-one-denies-write"] = [new(FileAccess.Read, FileShare.ReadWrite), new(FileAccess.Read, FileShare.Read)],
        ["writer-and-sharing-reader"] = [new(FileAccess.Write, FileShare.Read), new(FileAccess.Read, FileShare.ReadWrite)],
        ["readwriter-and-reader-with-delete"] = [new(FileAccess.ReadWrite, FileShare.Read | FileShare.Delete), new(FileAccess.Read, FileShare.ReadWrite | FileShare.Delete)],
        ["three-readers-mixed"] = [new(FileAccess.Read, FileShare.ReadWrite | FileShare.Delete), new(FileAccess.Read, FileShare.Read | FileShare.Delete), new(FileAccess.Read, FileShare.ReadWrite)],
        ["two-readers-and-writer"] = [new(FileAccess.Read, FileShare.ReadWrite), new(FileAccess.Read, FileShare.ReadWrite | FileShare.Delete), new(FileAccess.Write, FileShare.Read)],
    };

    public static TheoryData<string> ExistingHandleSetNames()
    {
        var data = new TheoryData<string>();
        foreach (string name in ExistingHandleSets.Keys)
            data.Add(name);
        return data;
    }

    /// <summary>
    /// LOCK-001 (all active handles): with several mutually compatible handles already open, every proposed request is
    /// admitted iff it is compatible in both directions with EACH active handle — one denying handle is enough to
    /// reject it, and a rejection never disturbs the existing handles.
    /// </summary>
    [Theory]
    [MemberData(nameof(ExistingHandleSetNames))]
    [Catalog("LOCK-001", "U", "INV-02", "INV-05", "INV-06", "INV-12")]
    public async Task LOCK_001_ShareCompatibilityBothDirections_AcrossAllActiveHandles(string setName)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        HandleSpec[] set = ExistingHandleSets[setName];
        for (int i = 0; i < set.Length; i++)
        {
            for (int j = i + 1; j < set.Length; j++)
                Assert.True(LockHelpers.Compatible(set[i], set[j]), $"test data: {set[i]} and {set[j]} must be mutually compatible");
        }

        await ClusterTest.RunAsync(LockHelpers.UnitCluster(), async cluster =>
        {
            await using DfsClient c0 = await LockHelpers.ConnectAsync(cluster, "c0", ct);
            await using DfsClient c1 = await LockHelpers.ConnectAsync(cluster, "c1", ct);
            await using DfsClient c2 = await LockHelpers.ConnectAsync(cluster, "c2", ct);
            DfsVolume[] owners = [c0.GetVolume(Vol), c1.GetVolume(Vol)];
            DfsVolume proposer = c2.GetVolume(Vol);
            await owners[0].Directories.CreateDirectoryAsync("/lock001m", ct);

            int index = 0;
            foreach (HandleSpec proposed in LockHelpers.AllRequests())
            {
                string path = $"/lock001m/f{index:D3}";
                byte[] content = TestData.Bytes((ulong)(2000 + index), 300);
                index++;
                await owners[0].Files.WriteAllBytesAsync(path, content, ct);
                InodeId inode = (await owners[0].Files.GetInfoAsync(path, ct)).InodeId;

                var held = new List<DfsStream>();
                try
                {
                    for (int i = 0; i < set.Length; i++)
                        held.Add(await owners[i % 2].Files.OpenAsync(path, LockHelpers.Options(FileMode.Open, set[i]), ct));

                    (DfsStream? opened, Exception? error) = await LockHelpers.TryOpenAsync(proposer, path, LockHelpers.Options(FileMode.Open, proposed), ct);
                    string what = $"set '{setName}' [{string.Join(", ", set)}] + proposed {proposed}";
                    if (!LockHelpers.IsValidCoreRequest(proposed))
                    {
                        if (opened is not null)
                        {
                            await opened.DisposeAsync();
                            Assert.Fail($"{what}: a writable handle granting FileShare.Write must be rejected");
                        }

                        LockHelpers.AssertExplicitOptionRejection(error, what);
                    }
                    else if (LockHelpers.CompatibleWithAll(set, proposed))
                    {
                        if (opened is null)
                        {
                            Assert.Fail($"{what}: compatible with every active handle in both directions, so it must open; got {LockHelpers.Describe(error)}");
                            return;
                        }

                        await using (opened)
                        {
                            HandleInspection[] active = await LockHelpers.ActiveHandlesAsync(cluster, inode);
                            Assert.True(active.Length == set.Length + 1, $"{what}: expected {set.Length + 1} active handles, found {active.Length}");
                            Assert.True(active.Count(h => h.IsWriter) <= 1, $"{what}: at most one writer handle per inode (LOCK-01)");
                        }
                    }
                    else
                    {
                        if (opened is not null)
                        {
                            await opened.DisposeAsync();
                            Assert.Fail($"{what}: at least one active handle does not grant the proposed access (or vice versa); the open must be rejected");
                        }

                        LockHelpers.AssertSharingConflict(error, what);
                    }

                    HandleInspection[] remaining = await LockHelpers.ActiveHandlesAsync(cluster, inode);
                    Assert.True(remaining.Length == set.Length, $"{what}: the existing handles must be unaffected ({remaining.Length} active, expected {set.Length})");
                }
                finally
                {
                    foreach (DfsStream stream in held)
                        await stream.DisposeAsync();
                }

                Assert.Empty(await LockHelpers.ActiveHandlesAsync(cluster, inode));
                LockHelpers.BytesEqual(content, await LockHelpers.ReadCommittedAsync(owners[0], path, ct), $"{path}: opening handles must not change the data");
            }
        }, ct);
    }

    private static async Task<bool> CheckPairAsync(TestCluster cluster, DfsVolume firstVolume, DfsVolume secondVolume, string path,
        HandleSpec first, HandleSpec second, ulong seed, CancellationToken ct)
    {
        byte[] content = TestData.Bytes(seed, 257);
        await firstVolume.Files.WriteAllBytesAsync(path, content, ct);
        DfsFileInfo before = await firstVolume.Files.GetInfoAsync(path, ct);
        string pair = $"{path}: open {first} then {second}";
        bool expected = LockHelpers.Compatible(first, second);

        DfsStream h1 = await firstVolume.Files.OpenAsync(path, LockHelpers.Options(FileMode.Open, first), ct);
        try
        {
            Assert.Equal(first.Access, h1.Access);
            Assert.Equal(first.Share, h1.Share);
            (DfsStream? h2, Exception? error) = await LockHelpers.TryOpenAsync(secondVolume, path, LockHelpers.Options(FileMode.Open, second), ct);
            if (expected)
            {
                if (h2 is null)
                {
                    Assert.Fail($"{pair}: each handle grants the other's access, so both must coexist; got {LockHelpers.Describe(error)}");
                    return expected;
                }

                await using (h2)
                {
                    Assert.Equal(second.Access, h2.Access);
                    Assert.Equal(second.Share, h2.Share);
                    HandleInspection[] active = await LockHelpers.ActiveHandlesAsync(cluster, before.InodeId);
                    Assert.True(active.Length == 2, $"{pair}: expected both handles active, found {active.Length}");
                    Assert.Contains(active, h => h.Access == first.Access && h.Share == first.Share);
                    Assert.Contains(active, h => h.Access == second.Access && h.Share == second.Share);
                    Assert.True(active.Count(h => h.IsWriter) <= 1, $"{pair}: at most one writer per inode");

                    // Each handle can exercise the access it was granted while the other is open.
                    if ((first.Access & FileAccess.Read) != 0)
                        LockHelpers.BytesEqual(content, await LockHelpers.ReadToEndAsync(h1, ct), $"{pair}: first handle reads");
                    if ((second.Access & FileAccess.Read) != 0)
                        LockHelpers.BytesEqual(content, await LockHelpers.ReadToEndAsync(h2, ct), $"{pair}: second handle reads");
                }
            }
            else
            {
                if (h2 is not null)
                {
                    await h2.DisposeAsync();
                    Assert.Fail($"{pair}: one side does not grant the other's access (checked in both directions); the second open must fail");
                }

                LockHelpers.AssertSharingConflict(error, pair);
                HandleInspection[] active = await LockHelpers.ActiveHandlesAsync(cluster, before.InodeId);
                Assert.True(active.Length == 1, $"{pair}: a rejected open must not leave a reservation ({active.Length} active handles)");
                Assert.Equal(first.Access, active[0].Access);
                Assert.Equal(first.Share, active[0].Share);
            }
        }
        finally
        {
            await h1.DisposeAsync();
        }

        Assert.Empty(await LockHelpers.ActiveHandlesAsync(cluster, before.InodeId));
        DfsFileInfo after = await firstVolume.Files.GetInfoAsync(path, ct);
        Assert.Equal(before.InodeId, after.InodeId);
        Assert.Equal(content.Length, after.Length);
        if (!LockHelpers.IsWritable(first.Access) && !LockHelpers.IsWritable(second.Access))
            Assert.Equal(before.Revision, after.Revision);
        LockHelpers.BytesEqual(content, await LockHelpers.ReadCommittedAsync(firstVolume, path, ct), $"{pair}: opening/closing without writes must not change the data");
        return expected;
    }

    private static async Task CheckInvalidProposalAsync(TestCluster cluster, DfsVolume ownerVolume, DfsVolume proposerVolume, string path,
        HandleSpec existing, HandleSpec proposed, ulong seed, CancellationToken ct)
    {
        byte[] content = TestData.Bytes(seed, 257);
        await ownerVolume.Files.WriteAllBytesAsync(path, content, ct);
        InodeId inode = (await ownerVolume.Files.GetInfoAsync(path, ct)).InodeId;
        await using DfsStream held = await ownerVolume.Files.OpenAsync(path, LockHelpers.Options(FileMode.Open, existing), ct);
        (DfsStream? opened, Exception? error) = await LockHelpers.TryOpenAsync(proposerVolume, path, LockHelpers.Options(FileMode.Open, proposed), ct);
        if (opened is not null)
        {
            await opened.DisposeAsync();
            Assert.Fail($"{path}: {proposed} is a writable handle granting FileShare.Write and must be rejected in the core release");
        }

        LockHelpers.AssertExplicitOptionRejection(error, $"{path}: {existing} open, proposed {proposed}");
        HandleInspection[] active = await LockHelpers.ActiveHandlesAsync(cluster, inode);
        Assert.True(active.Length == 1, $"{path}: the rejected request must not reserve anything ({active.Length} active handles)");
    }

    // ------------------------------------------------------------------------------------------------------------
    // LOCK-002
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<FileAccess, bool> ShareNoneHolders()
    {
        var data = new TheoryData<FileAccess, bool>();
        foreach (FileAccess access in LockHelpers.Accesses)
        {
            data.Add(access, false);
            data.Add(access, true);
        }

        return data;
    }

    /// <summary>
    /// LOCK-002: a handle opened with FileShare.None excludes every other valid open (from another client and from the
    /// same client) and every helper that needs an open or a reservation on the inode, without changing it. Exclusion
    /// ends only by safe close or by server-authoritative lease expiry after the holder's client dies — never earlier.
    /// </summary>
    [Theory]
    [MemberData(nameof(ShareNoneHolders))]
    [Catalog("LOCK-002", "U", "INV-02", "INV-05", "INV-06", "INV-12")]
    public async Task LOCK_002_ShareNoneExcludesOtherHandles(FileAccess holderAccess, bool releaseByLeaseExpiry)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(LockHelpers.UnitCluster(ClusterMode.Deterministic), async cluster =>
        {
            TimeSpan lease = LockHelpers.Lease(cluster);
            DfsClient holderClient = await LockHelpers.ConnectAsync(cluster, "holder", ct);
            await using DfsClient other = await LockHelpers.ConnectAsync(cluster, "c1", ct);
            DfsVolume hv = holderClient.GetVolume(Vol);
            DfsVolume ov = other.GetVolume(Vol);

            const string path = "/lock002/f";
            const string src = "/lock002/src";
            byte[] content = TestData.Bytes(2001, 6000);
            byte[] replacement = TestData.Bytes(2002, 1500);
            await hv.Directories.CreateDirectoryAsync("/lock002", ct);
            await hv.Files.WriteAllBytesAsync(path, content, ct);
            await hv.Files.WriteAllBytesAsync(src, replacement, ct);
            DfsFileInfo before = await hv.Files.GetInfoAsync(path, ct);

            DfsStream holder = await hv.Files.OpenAsync(path, LockHelpers.Options(FileMode.Open, new HandleSpec(holderAccess, FileShare.None)), ct);
            // Metadata reads take no handle, so they work next to a FileShare.None holder. Taken after the holder's open
            // because a writable open may legitimately touch the inode record (writer generation).
            DfsFileInfo held = await ov.Files.GetInfoAsync(path, ct);

            int index = 0;
            foreach (HandleSpec request in LockHelpers.AllRequests().Where(LockHelpers.IsValidCoreRequest))
            {
                DfsVolume requester = index++ % 2 == 0 ? ov : hv; // other client and the holder's own client
                (DfsStream? opened, Exception? error) = await LockHelpers.TryOpenAsync(requester, path, LockHelpers.Options(FileMode.Open, request), ct);
                if (opened is not null)
                {
                    await opened.DisposeAsync();
                    Assert.Fail($"open {request} succeeded while a {holderAccess} handle with FileShare.None is active");
                }

                LockHelpers.AssertSharingConflict(error, $"open {request} against FileShare.None holder");
            }

            var helpers = new (string Name, Func<Task> Action)[]
            {
                ("ReadAllBytes", () => ov.Files.ReadAllBytesAsync(path, ct)),
                ("OpenRead", async () => { await using DfsStream s = await ov.Files.OpenReadAsync(path, ct); }),
                ("OpenWrite", async () => { await using DfsStream s = await ov.Files.OpenWriteAsync(path, ct); }),
                ("Create", async () => { await using DfsStream s = await ov.Files.CreateAsync(path, ct); }),
                ("Truncate", async () => { await using DfsStream s = await ov.Files.OpenAsync(path, FileMode.Truncate, FileAccess.Write, FileShare.Read, ct); }),
                ("WriteAllBytes", () => ov.Files.WriteAllBytesAsync(path, replacement, ct)),
                ("AppendAllBytes", () => ov.Files.AppendAllBytesAsync(path, replacement, ct)),
                ("CopyOverwrite", () => ov.Files.CopyAsync(src, path, true, ct)),
                ("MoveOverwrite", () => ov.Files.MoveAsync(src, path, true, ct)),
                ("Delete", () => ov.Files.DeleteAsync(path, ct)),
                ("TransactionWrite", async () =>
                {
                    await using DfsTransaction tx = await ov.Transactions.BeginAsync(null, ct);
                    await tx.Files.WriteAllBytesAsync(path, replacement, ct);
                    await tx.CommitAsync(ct);
                }),
            };
            foreach ((string name, Func<Task> action) in helpers)
                LockHelpers.AssertSharingConflict(await LockHelpers.CatchAsync(action), $"{name} against FileShare.None holder");

            // Nothing changed, nothing leaked.
            DfsFileInfo during = await ov.Files.GetInfoAsync(path, ct);
            Assert.Equal(before.InodeId, during.InodeId);
            Assert.Equal(content.Length, during.Length);
            Assert.True(held.Revision == during.Revision,
                $"no refused open/helper may publish anything: revision moved from {held.Revision} to {during.Revision} while the FileShare.None holder was active");
            Assert.True(await ov.Files.ExistsAsync(src, ct), "the failed move must not consume its source");
            HandleInspection[] active = await LockHelpers.ActiveHandlesAsync(cluster, before.InodeId);
            Assert.True(active.Length == 1, $"only the FileShare.None holder may be active, found {active.Length}");
            Assert.Equal(FileShare.None, active[0].Share);

            if (!releaseByLeaseExpiry)
            {
                await holder.DisposeAsync();
                Assert.Empty(await LockHelpers.ActiveHandlesAsync(cluster, before.InodeId));
            }
            else
            {
                long killedAt = cluster.Time.GetTimestamp();
                cluster.KillClient(holderClient);

                // Immediately after the client dies its lease is still valid: the exclusion must persist.
                (DfsStream? early, Exception? earlyError) = await LockHelpers.TryOpenAsync(ov, path,
                    LockHelpers.Options(FileMode.Open, new HandleSpec(FileAccess.Read, FileShare.ReadWrite | FileShare.Delete)), ct);
                if (early is not null)
                {
                    await early.DisposeAsync();
                    Assert.Fail("the reservation was released immediately when the client died; release must wait for lease expiry");
                }

                LockHelpers.AssertSharingConflict(earlyError, "open right after the holder's client died");

                await cluster.WaitUntilAsync(async () =>
                {
                    (DfsStream? s, Exception? e) = await LockHelpers.TryOpenAsync(ov, path,
                        LockHelpers.Options(FileMode.Open, new HandleSpec(FileAccess.Write, FileShare.None)), ct);
                    if (s is null)
                    {
                        LockHelpers.AssertSharingConflict(e, "open while waiting for the dead holder's lease to expire");
                        return false;
                    }

                    await s.DisposeAsync();
                    return true;
                }, lease * 5, "the dead holder's FileShare.None reservation must be released by server-side lease expiry", ct);

                TimeSpan elapsed = cluster.Time.GetElapsedTime(killedAt);
                Assert.True(elapsed >= lease / 2,
                    $"the reservation was released {elapsed} after the client died; with a {lease} lease renewed every {lease / 3} it cannot have expired before {lease / 2}");
            }

            // After a safe release every valid open (one at a time) and the mutating helpers work again.
            foreach (HandleSpec request in LockHelpers.AllRequests().Where(LockHelpers.IsValidCoreRequest))
            {
                (DfsStream? opened, Exception? error) = await LockHelpers.TryOpenAsync(ov, path, LockHelpers.Options(FileMode.Open, request), ct);
                if (opened is null)
                {
                    Assert.Fail($"open {request} must succeed after the FileShare.None holder was released; got {LockHelpers.Describe(error)}");
                    return;
                }

                await opened.DisposeAsync();
            }

            LockHelpers.BytesEqual(content, await LockHelpers.ReadCommittedAsync(ov, path, ct), "every conflicting open/helper left the bytes untouched");
            await ov.Files.WriteAllBytesAsync(path, replacement, ct);
            LockHelpers.BytesEqual(replacement, await LockHelpers.ReadCommittedAsync(ov, path, ct), "a writer can publish once the exclusive holder is gone");
            Assert.Equal(before.InodeId, (await ov.Files.GetInfoAsync(path, ct)).InodeId);

            if (!releaseByLeaseExpiry)
                await holderClient.DisposeAsync();
        }, ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // LOCK-004
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<FileAccess, int, DfsReadView> ReaderWriterPairs()
    {
        var data = new TheoryData<FileAccess, int, DfsReadView>();
        foreach (FileAccess writer in new[] { FileAccess.Write, FileAccess.ReadWrite })
        {
            foreach (int readers in new[] { 1, 3 })
            {
                data.Add(writer, readers, DfsReadView.Live);
                data.Add(writer, readers, DfsReadView.VersionPinned);
            }
        }

        return data;
    }

    /// <summary>
    /// LOCK-004: a writer with Share.Read and readers with Share.ReadWrite coexist. Readers see the last committed
    /// revision until the writer publishes (live readers then observe the new revision, pinned readers keep theirs),
    /// and only the writer can publish: every reader-side mutation is refused and never changes the revision.
    /// </summary>
    [Theory]
    [MemberData(nameof(ReaderWriterPairs))]
    [Catalog("LOCK-004", "U", "INV-02", "INV-05", "INV-06", "INV-12")]
    public async Task LOCK_004_CompatibleReaderWriterPair(FileAccess writerAccess, int readerCount, DfsReadView readView)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(LockHelpers.UnitCluster(), async cluster =>
        {
            const string path = "/lock004/f";
            byte[] original = TestData.Bytes(4001, 10_000);
            byte[] update = TestData.Bytes(4002, 12_345);
            byte[] update2 = TestData.Bytes(4003, 777);

            await using DfsClient writerClient = await LockHelpers.ConnectAsync(cluster, "w", ct);
            DfsVolume wv = writerClient.GetVolume(Vol);
            await wv.Directories.CreateDirectoryAsync("/lock004", ct);
            await wv.Files.WriteAllBytesAsync(path, original, ct);
            DfsFileInfo before = await wv.Files.GetInfoAsync(path, ct);

            await using DfsStream writer = await wv.Files.OpenAsync(path, LockHelpers.Options(FileMode.Open, new HandleSpec(writerAccess, FileShare.Read)), ct);
            Assert.True(writer.WriterGeneration > 0, "a writable handle is bound to a nonzero writer generation");

            var readerClients = new List<DfsClient>();
            var readers = new List<DfsStream>();
            try
            {
                for (int i = 0; i < readerCount; i++)
                {
                    DfsClient rc = await LockHelpers.ConnectAsync(cluster, "r" + i, ct);
                    readerClients.Add(rc);
                    (DfsStream? reader, Exception? error) = await LockHelpers.TryOpenAsync(rc.GetVolume(Vol), path,
                        LockHelpers.Options(FileMode.Open, new HandleSpec(FileAccess.Read, FileShare.ReadWrite), readView), ct);
                    if (reader is null)
                    {
                        Assert.Fail($"reader {i} (Read, Share.ReadWrite) must coexist with a writer sharing Read; got {LockHelpers.Describe(error)}");
                        return;
                    }

                    readers.Add(reader);
                    Assert.False(reader.CanWrite);
                    Assert.Equal(0UL, reader.WriterGeneration);
                    Assert.Equal(readView, reader.ReadView);
                }

                HandleInspection[] active = await LockHelpers.ActiveHandlesAsync(cluster, before.InodeId);
                Assert.Equal(readerCount + 1, active.Length);
                HandleInspection writerHandle = Assert.Single(active, h => h.IsWriter);
                Assert.Equal(writer.WriterGeneration, writerHandle.Generation);
                Assert.All(active.Where(h => !h.IsWriter), h => Assert.Equal(FileAccess.Read, h.Access));

                // Staged writes are private to the writer.
                writer.Seek(0, SeekOrigin.Begin);
                await writer.WriteAsync(update, ct);
                await writer.SetLengthAsync(update.Length, ct);
                foreach (DfsStream reader in readers)
                {
                    reader.Seek(0, SeekOrigin.Begin);
                    LockHelpers.BytesEqual(original, await LockHelpers.ReadToEndAsync(reader, ct), "readers see the last committed revision before the writer publishes");
                }

                CommitToken published = await writer.CommitAsync(ct);
                Assert.False(published.IsNone);
                foreach (DfsStream reader in readers)
                {
                    reader.Seek(0, SeekOrigin.Begin);
                    byte[] seen = await LockHelpers.ReadToEndAsync(reader, ct);
                    LockHelpers.BytesEqual(readView == DfsReadView.Live ? update : original, seen,
                        readView == DfsReadView.Live ? "a new live read after a completed publication observes it" : "a pinned reader keeps its revision");
                }

                // Readers cannot publish through any path.
                foreach (DfsStream reader in readers)
                {
                    Exception? write = await LockHelpers.CatchAsync(() => reader.WriteAsync(update2, ct).AsTask());
                    Assert.True(write is NotSupportedException, $"reader Write: expected NotSupportedException, got {LockHelpers.Describe(write)}");
                    Exception? setLength = await LockHelpers.CatchAsync(() => reader.SetLengthAsync(1, ct));
                    Assert.True(setLength is NotSupportedException, $"reader SetLength: expected NotSupportedException, got {LockHelpers.Describe(setLength)}");
                    Exception? commit = await LockHelpers.CatchAsync(() => reader.CommitAsync(ct));
                    Assert.True(commit is NotSupportedException or InvalidOperationException, $"reader CommitAsync must be refused, got {LockHelpers.Describe(commit)}");
                    await reader.FlushAsync(ct); // read-only flush is a no-op
                }

                foreach (DfsClient rc in readerClients)
                {
                    DfsVolume rv = rc.GetVolume(Vol);
                    LockHelpers.AssertSharingConflict(await LockHelpers.CatchAsync(() => rv.Files.WriteAllBytesAsync(path, update2, ct)),
                        "WriteAllBytes from a reader's client while the writer holds the reservation");
                    (DfsStream? second, Exception? secondError) = await LockHelpers.TryOpenAsync(rv, path,
                        LockHelpers.Options(FileMode.Open, new HandleSpec(FileAccess.Write, FileShare.Read)), ct);
                    if (second is not null)
                    {
                        await second.DisposeAsync();
                        Assert.Fail("a second writer was admitted next to the writer");
                    }

                    LockHelpers.AssertSharingConflict(secondError, "second writable open");
                }

                DfsFileInfo afterReaders = await wv.Files.GetInfoAsync(path, ct);
                Assert.Equal(published.Sequence, afterReaders.Revision);
                Assert.Equal(update.Length, afterReaders.Length);

                // The writer remains the only publisher.
                writer.Seek(0, SeekOrigin.Begin);
                await writer.WriteAsync(update2, ct);
                await writer.SetLengthAsync(update2.Length, ct);
                CommitToken published2 = await writer.CommitAsync(ct);
                Assert.True(published2.CompareTo(published) > 0, "each publication has a later commit token");
                LockHelpers.BytesEqual(update2, await LockHelpers.ReadCommittedAsync(wv, path, ct), "the writer's second publication is visible");
                FileInspection? file = await cluster.Inspect.GetFileAsync(Vol, path);
                Assert.NotNull(file);
                Assert.Equal(writer.WriterGeneration, file.WriterGeneration);
                Assert.Equal(published2.Sequence, file.Revision);
            }
            finally
            {
                foreach (DfsStream reader in readers)
                    await reader.DisposeAsync();
                foreach (DfsClient rc in readerClients)
                    await rc.DisposeAsync();
            }
        }, ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // LOCK-005
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<FileAccess, FileShare> WritableHandlesGrantingWrite()
    {
        var data = new TheoryData<FileAccess, FileShare>();
        foreach (FileAccess access in new[] { FileAccess.Write, FileAccess.ReadWrite })
        {
            foreach (FileShare share in LockHelpers.Shares.Where(s => (s & FileShare.Write) != 0))
                data.Add(access, share);
        }

        return data;
    }

    private static IEnumerable<(FileMode Mode, bool Exists)> ModeCases(FileAccess access)
    {
        yield return (FileMode.Open, true);
        yield return (FileMode.OpenOrCreate, true);
        yield return (FileMode.OpenOrCreate, false);
        yield return (FileMode.Create, true);
        yield return (FileMode.Create, false);
        yield return (FileMode.CreateNew, false);
        if (access == FileAccess.Write)
        {
            yield return (FileMode.Truncate, true);
            yield return (FileMode.Append, true);
            yield return (FileMode.Append, false);
        }
    }

    /// <summary>
    /// LOCK-005: a writable handle requesting FileShare.Write (any combination containing it) is rejected explicitly in
    /// every open mode and overload, also inside a transaction — not reported as a contended share, and without
    /// creating, truncating or reserving anything.
    /// </summary>
    [Theory]
    [MemberData(nameof(WritableHandlesGrantingWrite))]
    [Catalog("LOCK-005", "U", "INV-02", "INV-05", "INV-06", "INV-12")]
    public async Task LOCK_005_UnsupportedMultipleWriterShareRejected(FileAccess access, FileShare share)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(LockHelpers.UnitCluster(), async cluster =>
        {
            await using DfsClient c0 = await LockHelpers.ConnectAsync(cluster, "c0", ct);
            await using DfsClient c1 = await LockHelpers.ConnectAsync(cluster, "c1", ct);
            DfsVolume v = c0.GetVolume(Vol);
            await v.Directories.CreateDirectoryAsync("/lock005", ct);

            int index = 0;
            foreach ((FileMode mode, bool exists) in ModeCases(access))
            {
                foreach (bool optionsOverload in new[] { true, false })
                {
                    string path = $"/lock005/{mode}-{(exists ? "existing" : "absent")}-{index++}";
                    byte[] content = TestData.Bytes((ulong)(5000 + index), 900);
                    InodeId inode = default;
                    if (exists)
                    {
                        await v.Files.WriteAllBytesAsync(path, content, ct);
                        inode = (await v.Files.GetInfoAsync(path, ct)).InodeId;
                    }

                    Exception? error = await LockHelpers.CatchAsync(async () =>
                    {
                        DfsStream s = optionsOverload
                            ? await v.Files.OpenAsync(path, new DfsOpenOptions { Mode = mode, Access = access, Share = share }, ct)
                            : await v.Files.OpenAsync(path, mode, access, share, ct);
                        await s.DisposeAsync();
                    });
                    string what = $"{mode}/{access}/{share} on {(exists ? "existing" : "absent")} file ({(optionsOverload ? "DfsOpenOptions" : "enum")} overload)";
                    LockHelpers.AssertExplicitOptionRejection(error, what);

                    if (exists)
                    {
                        Assert.Empty(await LockHelpers.ActiveHandlesAsync(cluster, inode));
                        DfsFileInfo info = await v.Files.GetInfoAsync(path, ct);
                        Assert.Equal(inode, info.InodeId);
                        LockHelpers.BytesEqual(content, await LockHelpers.ReadCommittedAsync(v, path, ct), $"{what}: the rejected open must not truncate or modify");
                    }
                    else
                    {
                        Assert.False(await v.Files.ExistsAsync(path, ct), $"{what}: the rejected open must not create the file");
                    }
                }
            }

            // Transaction-bound opens obey the same rule.
            const string txPath = "/lock005/tx";
            byte[] txContent = TestData.Bytes(5999, 400);
            await v.Files.WriteAllBytesAsync(txPath, txContent, ct);
            await using (DfsTransaction tx = await v.Transactions.BeginAsync(null, ct))
            {
                Exception? txError = await LockHelpers.CatchAsync(async () =>
                {
                    DfsStream s = await tx.Files.OpenAsync(txPath, FileMode.Open, access, share, ct);
                    await s.DisposeAsync();
                });
                LockHelpers.AssertExplicitOptionRejection(txError, $"transaction-bound open {access}/{share}");
            }

            // Nothing changed and nothing was reserved: the bytes are intact and the file is fully available to a valid
            // writer afterwards. (Read first: the valid writer may grant no Read share, e.g. Share.None or Share.Delete.)
            LockHelpers.BytesEqual(txContent, await LockHelpers.ReadCommittedAsync(v, txPath, ct), "transaction-bound rejection changed nothing");
            InodeId txInode = (await v.Files.GetInfoAsync(txPath, ct)).InodeId;
            Assert.Empty(await LockHelpers.ActiveHandlesAsync(cluster, txInode));
            await using DfsStream valid = await c1.GetVolume(Vol).Files.OpenAsync(txPath, FileMode.Open, access, share & ~FileShare.Write, ct);
            Assert.True(valid.WriterGeneration > 0);
            Assert.Equal(share & ~FileShare.Write, valid.Share);
        }, ct);
    }

    public static TheoryData<FileShare> ReaderSharesGrantingWrite()
    {
        var data = new TheoryData<FileShare>();
        foreach (FileShare share in LockHelpers.Shares.Where(s => (s & FileShare.Write) != 0))
            data.Add(share);
        return data;
    }

    /// <summary>
    /// LOCK-005 (positive control): a read-only handle MAY grant FileShare.Write so that one writer can coexist; the
    /// rule targets writable handles only, and one writer per inode still holds.
    /// </summary>
    [Theory]
    [MemberData(nameof(ReaderSharesGrantingWrite))]
    [Catalog("LOCK-005", "U", "INV-02", "INV-05", "INV-06", "INV-12")]
    public async Task LOCK_005_UnsupportedMultipleWriterShareRejected_ReaderMayGrantWrite(FileShare readerShare)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(LockHelpers.UnitCluster(), async cluster =>
        {
            await using DfsClient c0 = await LockHelpers.ConnectAsync(cluster, "c0", ct);
            await using DfsClient c1 = await LockHelpers.ConnectAsync(cluster, "c1", ct);
            await using DfsClient c2 = await LockHelpers.ConnectAsync(cluster, "c2", ct);
            DfsVolume v0 = c0.GetVolume(Vol);
            const string path = "/lock005r/f";
            byte[] original = TestData.Bytes(5101, 5000);
            byte[] update = TestData.Bytes(5102, 4321);
            await v0.Directories.CreateDirectoryAsync("/lock005r", ct);
            await v0.Files.WriteAllBytesAsync(path, original, ct);

            await using DfsStream reader = await v0.Files.OpenAsync(path, FileMode.Open, FileAccess.Read, readerShare, ct);
            Assert.Equal(readerShare, reader.Share);
            await using DfsStream writer = await c1.GetVolume(Vol).Files.OpenAsync(path, FileMode.Open, FileAccess.Write, FileShare.Read, ct);

            (DfsStream? second, Exception? error) = await LockHelpers.TryOpenAsync(c2.GetVolume(Vol), path,
                LockHelpers.Options(FileMode.Open, new HandleSpec(FileAccess.Write, FileShare.Read)), ct);
            if (second is not null)
            {
                await second.DisposeAsync();
                Assert.Fail("a reader granting FileShare.Write must not enable a second writer");
            }

            LockHelpers.AssertSharingConflict(error, "second writer next to a writer and a Write-sharing reader");

            writer.Seek(0, SeekOrigin.Begin);
            await writer.WriteAsync(update, ct);
            await writer.SetLengthAsync(update.Length, ct);
            await writer.CommitAsync(ct);
            reader.Seek(0, SeekOrigin.Begin);
            LockHelpers.BytesEqual(update, await LockHelpers.ReadToEndAsync(reader, ct), "the live reader observes the coexisting writer's publication");

            HandleInspection[] active = await LockHelpers.ActiveHandlesAsync(cluster, reader.InodeId);
            Assert.Equal(2, active.Length);
            Assert.Single(active, h => h.IsWriter);
        }, ct);
    }

    /// <summary>
    /// LOCK-005 (server side): the same requests sent through the raw protocol, bypassing SDK validation, are refused
    /// by the metadata authority with InvalidArgument/Unsupported and grant no handle; a valid raw writer request is
    /// granted, proving the refusals are about the share option.
    /// </summary>
    [Fact]
    [Catalog("LOCK-005", "U", "INV-02", "INV-05", "INV-06", "INV-12")]
    public async Task LOCK_005_UnsupportedMultipleWriterShareRejected_ServerSide()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(LockHelpers.UnitCluster(), async cluster =>
        {
            await using DfsClient c0 = await LockHelpers.ConnectAsync(cluster, "c0", ct);
            DfsVolume v = c0.GetVolume(Vol);
            const string path = "/lock005s/f";
            await v.Directories.CreateDirectoryAsync("/lock005s", ct);
            await v.Files.WriteAllBytesAsync(path, TestData.Bytes(5201, 100), ct);
            InodeId inode = (await v.Files.GetInfoAsync(path, ct)).InodeId;

            TestNode leader = await cluster.GetLeaderAsync(Vol, ct);
            await using RawProtocolClient raw = await cluster.ConnectRawAsync(leader.Name, ct);
            Assert.Equal(DfsErrorCode.None, await raw.AuthenticateAsync(cluster.AdminCredentials));

            foreach (FileAccess access in new[] { FileAccess.Write, FileAccess.ReadWrite })
            {
                foreach (FileShare share in LockHelpers.Shares.Where(s => (s & FileShare.Write) != 0))
                {
                    RawOpenHandleResult existing = await raw.OpenHandleAsync(Vol, path, FileMode.Open, access, share);
                    Assert.False(existing.Granted, $"server granted {access}/{share}");
                    Assert.True(existing.Code is DfsErrorCode.InvalidArgument or DfsErrorCode.Unsupported,
                        $"server must reject {access}/{share} as an invalid/unsupported option, got {existing.Code}");

                    string absent = $"/lock005s/absent-{access}-{(int)share}";
                    RawOpenHandleResult create = await raw.OpenHandleAsync(Vol, absent, FileMode.OpenOrCreate, access, share);
                    Assert.False(create.Granted);
                    Assert.True(create.Code is DfsErrorCode.InvalidArgument or DfsErrorCode.Unsupported, $"OpenOrCreate {access}/{share}: got {create.Code}");
                    Assert.False(await v.Files.ExistsAsync(absent, ct), $"rejected OpenOrCreate {access}/{share} must not create {absent}");
                }
            }

            Assert.Empty(await LockHelpers.ActiveHandlesAsync(cluster, inode));

            RawOpenHandleResult valid = await raw.OpenHandleAsync(Vol, path, FileMode.Open, FileAccess.Write, FileShare.Read);
            Assert.True(valid.Granted, $"control: a valid writer request must be granted, got {valid.Code}");
            Assert.True(valid.WriterGeneration > 0);
            Assert.Equal(DfsErrorCode.None, await raw.CloseHandleAsync(Vol, valid.Handle!.Value));
            Assert.Empty(await LockHelpers.ActiveHandlesAsync(cluster, inode));
        }, ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // LOCK-006
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<FileAccess, FileShare, bool, DeleteShareOperation> DeleteShareMatrix()
    {
        var data = new TheoryData<FileAccess, FileShare, bool, DeleteShareOperation>();
        foreach (FileAccess access in LockHelpers.Accesses)
        {
            foreach (FileShare baseShare in new[] { FileShare.None, FileShare.Read, FileShare.Write, FileShare.ReadWrite })
            {
                if (!LockHelpers.IsValidCoreRequest(new HandleSpec(access, baseShare)))
                    continue;
                foreach (bool grantDelete in new[] { false, true })
                {
                    data.Add(access, baseShare, grantDelete, DeleteShareOperation.Delete);
                    data.Add(access, baseShare, grantDelete, DeleteShareOperation.RenameOverwrite);
                }
            }
        }

        return data;
    }

    /// <summary>
    /// LOCK-006: FileShare.Delete is independent of the read/write bits. Read/write admission ignores it, while delete
    /// and rename-over of an open inode succeed iff the open handle grants Delete. After a permitted delete/replace the
    /// handle stays bound to the detached inode (reads its bytes, may write it) and the path is never resurrected.
    /// </summary>
    [Theory]
    [MemberData(nameof(DeleteShareMatrix))]
    [Catalog("LOCK-006", "U", "INV-02", "INV-05", "INV-06", "INV-12")]
    public async Task LOCK_006_DeleteShareIsIndependent(FileAccess holderAccess, FileShare baseShare, bool grantDelete, DeleteShareOperation operation)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        FileShare holderShare = grantDelete ? baseShare | FileShare.Delete : baseShare;
        var holderSpec = new HandleSpec(holderAccess, holderShare);
        await ClusterTest.RunAsync(LockHelpers.UnitCluster(), async cluster =>
        {
            await using DfsClient c0 = await LockHelpers.ConnectAsync(cluster, "c0", ct);
            await using DfsClient c1 = await LockHelpers.ConnectAsync(cluster, "c1", ct);
            DfsVolume v0 = c0.GetVolume(Vol);
            DfsVolume v1 = c1.GetVolume(Vol);
            const string path = "/lock006/f";
            const string src = "/lock006/src";
            byte[] fileBytes = TestData.Bytes(6001, 5000);
            byte[] srcBytes = TestData.Bytes(6002, 3000);
            byte[] written = TestData.Bytes(6003, 2000);
            await v0.Directories.CreateDirectoryAsync("/lock006", ct);
            await v0.Files.WriteAllBytesAsync(path, fileBytes, ct);
            await v0.Files.WriteAllBytesAsync(src, srcBytes, ct);
            InodeId fileInode = (await v0.Files.GetInfoAsync(path, ct)).InodeId;
            InodeId srcInode = (await v0.Files.GetInfoAsync(src, ct)).InodeId;

            await using DfsStream holder = await v0.Files.OpenAsync(path, LockHelpers.Options(FileMode.Open, holderSpec), ct);

            // Independence: the Delete bit neither grants nor denies read/write admission.
            foreach (HandleSpec probe in new[]
            {
                new HandleSpec(FileAccess.Read, FileShare.ReadWrite | FileShare.Delete),
                new HandleSpec(FileAccess.Read, FileShare.ReadWrite),
            })
            {
                bool expected = LockHelpers.Compatible(holderSpec, probe);
                Assert.Equal(expected, LockHelpers.Compatible(new HandleSpec(holderAccess, baseShare), probe)); // oracle sanity: Delete-independent
                (DfsStream? opened, Exception? error) = await LockHelpers.TryOpenAsync(v1, path, LockHelpers.Options(FileMode.Open, probe), ct);
                if (expected)
                {
                    if (opened is null)
                    {
                        Assert.Fail($"probe {probe} vs holder {holderSpec} must open regardless of the Delete bit; got {LockHelpers.Describe(error)}");
                        return;
                    }

                    await opened.DisposeAsync();
                }
                else
                {
                    if (opened is not null)
                    {
                        await opened.DisposeAsync();
                        Assert.Fail($"probe {probe} vs holder {holderSpec} must be rejected regardless of the Delete bit");
                    }

                    LockHelpers.AssertSharingConflict(error, $"probe {probe} vs holder {holderSpec}");
                }
            }

            Exception? opError = operation == DeleteShareOperation.Delete
                ? await LockHelpers.CatchAsync(() => v1.Files.DeleteAsync(path, ct))
                : await LockHelpers.CatchAsync(() => v1.Files.MoveAsync(src, path, true, ct));
            string what = $"{operation} of an inode open as {holderSpec}";

            if (grantDelete)
            {
                Assert.True(opError is null, $"{what}: every open handle grants Delete, so it must succeed; got {LockHelpers.Describe(opError)}");
                if (operation == DeleteShareOperation.Delete)
                {
                    Assert.False(await v1.Files.ExistsAsync(path, ct), $"{what}: the name is gone");
                }
                else
                {
                    DfsFileInfo replaced = await v1.Files.GetInfoAsync(path, ct);
                    Assert.Equal(srcInode, replaced.InodeId);
                    Assert.False(await v1.Files.ExistsAsync(src, ct), $"{what}: the rename consumed its source name");
                    LockHelpers.BytesEqual(srcBytes, await LockHelpers.ReadCommittedAsync(v1, path, ct), $"{what}: the path now names the moved inode");
                }

                // The existing handle stays bound to the detached original inode.
                Assert.Equal(fileInode, holder.InodeId);
                if ((holderAccess & FileAccess.Read) != 0)
                {
                    holder.Seek(0, SeekOrigin.Begin);
                    LockHelpers.BytesEqual(fileBytes, await LockHelpers.ReadToEndAsync(holder, ct), $"{what}: the open handle still reads the original inode");
                }

                if (LockHelpers.IsWritable(holderAccess))
                {
                    holder.Seek(0, SeekOrigin.Begin);
                    await holder.WriteAsync(written, ct);
                    await holder.CommitAsync(ct);
                    if (operation == DeleteShareOperation.Delete)
                        Assert.False(await v1.Files.ExistsAsync(path, ct), $"{what}: writing through the old handle must not recreate the path");
                    else
                        LockHelpers.BytesEqual(srcBytes, await LockHelpers.ReadCommittedAsync(v1, path, ct), $"{what}: writes through the old handle stay on the detached inode");

                    if ((holderAccess & FileAccess.Read) != 0)
                    {
                        byte[] detachedExpected = [.. written, .. fileBytes.AsSpan(written.Length)];
                        holder.Seek(0, SeekOrigin.Begin);
                        LockHelpers.BytesEqual(detachedExpected, await LockHelpers.ReadToEndAsync(holder, ct),
                            $"{what}: the publication through the old handle changed the detached inode itself");
                    }
                }
            }
            else
            {
                LockHelpers.AssertSharingConflict(opError, $"{what}: the handle does not grant Delete");
                DfsFileInfo unchanged = await v1.Files.GetInfoAsync(path, ct);
                Assert.Equal(fileInode, unchanged.InodeId);
                Assert.Equal(fileBytes.Length, unchanged.Length);
                if (operation == DeleteShareOperation.RenameOverwrite)
                {
                    DfsFileInfo source = await v1.Files.GetInfoAsync(src, ct);
                    Assert.Equal(srcInode, source.InodeId);
                }

                await holder.DisposeAsync();
                LockHelpers.BytesEqual(fileBytes, await LockHelpers.ReadCommittedAsync(v1, path, ct), $"{what}: refused operation left the file intact");
            }
        }, ct);
    }

    /// <summary>
    /// LOCK-006 (all handles): delete/rename-over requires EVERY open handle to grant Delete; one handle without it
    /// blocks the operation until that handle closes.
    /// </summary>
    [Fact]
    [Catalog("LOCK-006", "U", "INV-02", "INV-05", "INV-06", "INV-12")]
    public async Task LOCK_006_DeleteShareIsIndependent_EveryHandleMustGrantDelete()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(LockHelpers.UnitCluster(), async cluster =>
        {
            await using DfsClient c0 = await LockHelpers.ConnectAsync(cluster, "c0", ct);
            await using DfsClient c1 = await LockHelpers.ConnectAsync(cluster, "c1", ct);
            await using DfsClient c2 = await LockHelpers.ConnectAsync(cluster, "c2", ct);
            DfsVolume v0 = c0.GetVolume(Vol);
            DfsVolume v2 = c2.GetVolume(Vol);
            const string path = "/lock006m/f";
            const string src = "/lock006m/src";
            byte[] fileBytes = TestData.Bytes(6101, 4500);
            await v0.Directories.CreateDirectoryAsync("/lock006m", ct);
            await v0.Files.WriteAllBytesAsync(path, fileBytes, ct);
            await v0.Files.WriteAllBytesAsync(src, TestData.Bytes(6102, 100), ct);

            await using DfsStream withDelete = await v0.Files.OpenAsync(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete, ct);
            DfsStream withoutDelete = await c1.GetVolume(Vol).Files.OpenAsync(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, ct);

            LockHelpers.AssertSharingConflict(await LockHelpers.CatchAsync(() => v2.Files.DeleteAsync(path, ct)), "delete while one handle lacks Delete");
            LockHelpers.AssertSharingConflict(await LockHelpers.CatchAsync(() => v2.Files.MoveAsync(src, path, true, ct)), "rename-over while one handle lacks Delete");
            Assert.True(await v2.Files.ExistsAsync(path, ct));
            Assert.True(await v2.Files.ExistsAsync(src, ct));

            await withoutDelete.DisposeAsync();
            await v2.Files.DeleteAsync(path, ct);
            Assert.False(await v2.Files.ExistsAsync(path, ct));
            withDelete.Seek(0, SeekOrigin.Begin);
            LockHelpers.BytesEqual(fileBytes, await LockHelpers.ReadToEndAsync(withDelete, ct), "the remaining Delete-sharing handle still reads the detached inode");
        }, ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // LOCK-007
    // ------------------------------------------------------------------------------------------------------------

    private static readonly FileShare[] UnsupportedShares =
    [
        FileShare.Inheritable,
        FileShare.Read | FileShare.Inheritable,
        FileShare.ReadWrite | FileShare.Delete | FileShare.Inheritable,
        (FileShare)8,
        (FileShare)32,
        (FileShare)256,
        (FileShare)(-1),
    ];

    public static TheoryData<FileShare> UnsupportedShareFlags()
    {
        var data = new TheoryData<FileShare>();
        foreach (FileShare share in UnsupportedShares)
            data.Add(share);
        return data;
    }

    private static IEnumerable<(FileAccess Access, FileMode Mode, bool Exists)> InheritableCases()
    {
        yield return (FileAccess.Read, FileMode.Open, true);
        yield return (FileAccess.Read, FileMode.OpenOrCreate, false);
        yield return (FileAccess.Write, FileMode.Open, true);
        yield return (FileAccess.Write, FileMode.OpenOrCreate, false);
        yield return (FileAccess.Write, FileMode.CreateNew, false);
        yield return (FileAccess.Write, FileMode.Create, true);
        yield return (FileAccess.Write, FileMode.Truncate, true);
        yield return (FileAccess.Write, FileMode.Append, false);
        yield return (FileAccess.ReadWrite, FileMode.Open, true);
    }

    /// <summary>
    /// LOCK-007: FileShare.Inheritable and undefined share bits are rejected explicitly for every access/mode and every
    /// open overload (async options, async enum, synchronous), inside and outside a transaction, BEFORE any
    /// reservation, creation or truncation takes effect.
    /// </summary>
    [Theory]
    [MemberData(nameof(UnsupportedShareFlags))]
    [Catalog("LOCK-007", "U", "INV-02", "INV-05", "INV-06", "INV-12")]
    public async Task LOCK_007_InheritableFlagRejected(FileShare share)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(LockHelpers.UnitCluster(), async cluster =>
        {
            await using DfsClient c0 = await LockHelpers.ConnectAsync(cluster, "c0", ct);
            await using DfsClient c1 = await LockHelpers.ConnectAsync(cluster, "c1", ct);
            DfsVolume v = c0.GetVolume(Vol);
            await v.Directories.CreateDirectoryAsync("/lock007", ct);

            int index = 0;
            foreach ((FileAccess access, FileMode mode, bool exists) in InheritableCases())
            {
                foreach (string overload in new[] { "async-options", "async-enum", "sync" })
                {
                    string path = $"/lock007/{index++:D2}-{mode}-{access}";
                    byte[] content = TestData.Bytes((ulong)(7000 + index), 1200);
                    InodeId inode = default;
                    if (exists)
                    {
                        await v.Files.WriteAllBytesAsync(path, content, ct);
                        inode = (await v.Files.GetInfoAsync(path, ct)).InodeId;
                    }

                    Exception? error = await LockHelpers.CatchAsync(async () =>
                    {
                        DfsStream s = overload switch
                        {
                            "async-options" => await v.Files.OpenAsync(path, new DfsOpenOptions { Mode = mode, Access = access, Share = share }, ct),
                            "async-enum" => await v.Files.OpenAsync(path, mode, access, share, ct),
                            _ => v.Files.Open(path, mode, access, share),
                        };
                        await s.DisposeAsync();
                    });
                    string what = $"{mode}/{access}/share 0x{(int)share:X} via {overload}";
                    LockHelpers.AssertExplicitOptionRejection(error, what);

                    if (exists)
                    {
                        Assert.Empty(await LockHelpers.ActiveHandlesAsync(cluster, inode));
                        LockHelpers.BytesEqual(content, await LockHelpers.ReadCommittedAsync(v, path, ct), $"{what}: must not truncate/modify");
                    }
                    else
                    {
                        Assert.False(await v.Files.ExistsAsync(path, ct), $"{what}: must not create");
                    }
                }
            }

            // Transaction-bound open.
            const string txPath = "/lock007/tx";
            byte[] txContent = TestData.Bytes(7999, 600);
            await v.Files.WriteAllBytesAsync(txPath, txContent, ct);
            InodeId txInode = (await v.Files.GetInfoAsync(txPath, ct)).InodeId;
            await using (DfsTransaction tx = await v.Transactions.BeginAsync(null, ct))
            {
                Exception? txError = await LockHelpers.CatchAsync(async () =>
                {
                    DfsStream s = await tx.Files.OpenAsync(txPath, FileMode.Open, FileAccess.Write, share, ct);
                    await s.DisposeAsync();
                });
                LockHelpers.AssertExplicitOptionRejection(txError, $"transaction-bound open with share 0x{(int)share:X}");
            }

            // No hidden reservation: another client can now take the file exclusively.
            Assert.Empty(await LockHelpers.ActiveHandlesAsync(cluster, txInode));
            await using DfsStream exclusive = await c1.GetVolume(Vol).Files.OpenAsync(txPath, FileMode.Open, FileAccess.Write, FileShare.None, ct);
            Assert.Equal(FileShare.None, exclusive.Share);
        }, ct);
    }

    /// <summary>
    /// LOCK-007 (server side): Inheritable/undefined share bits sent through the raw protocol (no SDK validation) are
    /// refused by the metadata authority before any reservation or creation.
    /// </summary>
    [Fact]
    [Catalog("LOCK-007", "U", "INV-02", "INV-05", "INV-06", "INV-12")]
    public async Task LOCK_007_InheritableFlagRejected_ServerSide()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(LockHelpers.UnitCluster(), async cluster =>
        {
            await using DfsClient c0 = await LockHelpers.ConnectAsync(cluster, "c0", ct);
            DfsVolume v = c0.GetVolume(Vol);
            const string path = "/lock007s/f";
            byte[] content = TestData.Bytes(7101, 800);
            await v.Directories.CreateDirectoryAsync("/lock007s", ct);
            await v.Files.WriteAllBytesAsync(path, content, ct);
            InodeId inode = (await v.Files.GetInfoAsync(path, ct)).InodeId;

            TestNode leader = await cluster.GetLeaderAsync(Vol, ct);
            await using RawProtocolClient raw = await cluster.ConnectRawAsync(leader.Name, ct);
            Assert.Equal(DfsErrorCode.None, await raw.AuthenticateAsync(cluster.AdminCredentials));

            int index = 0;
            foreach (FileShare share in UnsupportedShares)
            {
                foreach ((FileAccess access, FileMode mode, bool exists) in InheritableCases())
                {
                    string target = exists ? path : $"/lock007s/absent-{index++}";
                    RawOpenHandleResult result = await raw.OpenHandleAsync(Vol, target, mode, access, share);
                    string what = $"raw {mode}/{access}/share 0x{(int)share:X}";
                    Assert.False(result.Granted, $"{what}: granted a handle");
                    Assert.True(result.Code is DfsErrorCode.InvalidArgument or DfsErrorCode.Unsupported, $"{what}: expected InvalidArgument/Unsupported, got {result.Code}");
                    if (!exists)
                        Assert.False(await v.Files.ExistsAsync(target, ct), $"{what}: must not create {target}");
                }
            }

            Assert.Empty(await LockHelpers.ActiveHandlesAsync(cluster, inode));
            LockHelpers.BytesEqual(content, await LockHelpers.ReadCommittedAsync(v, path, ct), "raw rejected opens must not truncate");
            await using DfsStream exclusive = await v.Files.OpenAsync(path, FileMode.Open, FileAccess.Write, FileShare.None, ct);
            Assert.True(exclusive.WriterGeneration > 0);
        }, ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // LOCK-019
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<LockCycleScenario> LockCycles() => new()
    {
        LockCycleScenario.TwoCycleHelpers,
        LockCycleScenario.TwoCycleStreams,
        LockCycleScenario.TwoCycleSimultaneous,
        LockCycleScenario.ThreeCycle,
    };

    /// <summary>
    /// LOCK-019: transactions acquiring writer reservations in opposing orders never wait for each other. Every
    /// acquisition that closes the cycle completes promptly (well inside the operation timeout): when its target is
    /// still reserved by an ACTIVE transaction it fails with a typed sharing conflict (ARCHITECTURE §5.6: conflict →
    /// immediate SharingViolation, never waiting). The conflict policy may additionally abort the failing requester
    /// (FSD §8.3: conflicts abort with a typed exception), which releases its reservations — then a later closing
    /// acquisition on a released inode may legitimately be granted, but only if the previous holder can no longer
    /// commit. Either way the cycle resolves: the survivors (or their explicit retries) commit, and nothing written by
    /// an aborted transaction ever becomes visible.
    /// </summary>
    [Theory]
    [MemberData(nameof(LockCycles))]
    [Catalog("LOCK-019", "U", "INV-02", "INV-05", "INV-06", "INV-12")]
    public async Task LOCK_019_LockOrderCannotDeadlockForever(LockCycleScenario scenario)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(LockHelpers.UnitCluster(), async cluster =>
        {
            TimeSpan bound = TimeSpan.FromSeconds(5); // half the 10 s operation timeout: "immediate", never a timeout
            await using DfsClient c0 = await LockHelpers.ConnectAsync(cluster, "c0", ct);
            await using DfsClient c1 = await LockHelpers.ConnectAsync(cluster, "c1", ct);
            await using DfsClient c2 = await LockHelpers.ConnectAsync(cluster, "c2", ct);
            DfsVolume v0 = c0.GetVolume(Vol);
            DfsVolume v1 = c1.GetVolume(Vol);
            DfsVolume v2 = c2.GetVolume(Vol);

            string[] paths = ["/lock019/a", "/lock019/b", "/lock019/c"];
            byte[][] baseline = [TestData.Bytes(19_001, 300), TestData.Bytes(19_002, 300), TestData.Bytes(19_003, 300)];
            await v0.Directories.CreateDirectoryAsync("/lock019", ct);
            for (int i = 0; i < paths.Length; i++)
                await v0.Files.WriteAllBytesAsync(paths[i], baseline[i], ct);

            bool streams = scenario == LockCycleScenario.TwoCycleStreams;
            var openStreams = new List<DfsStream>();

            async Task AcquireAsync(DfsTransaction tx, string path, byte[] data)
            {
                if (!streams)
                {
                    await tx.Files.WriteAllBytesAsync(path, data, ct);
                    return;
                }

                DfsStream s = await tx.Files.OpenAsync(path, FileMode.Open, FileAccess.Write, FileShare.Read, ct);
                openStreams.Add(s);
                await s.WriteAsync(data, ct);
                await s.SetLengthAsync(data.Length, ct);
            }

            async Task ReleaseStreamsAsync()
            {
                foreach (DfsStream s in openStreams)
                    await LockHelpers.CatchAsync(() => s.DisposeAsync().AsTask());
                openStreams.Clear();
            }

            Task<Exception?> CloseCycleAsync(DfsTransaction tx, string path, byte[] data, string because) =>
                LockHelpers.CatchWithinAsync(cluster, () => AcquireAsync(tx, path, data), bound, because, ct);

            async Task AssertNothingVisibleAsync(int count)
            {
                for (int i = 0; i < count; i++)
                {
                    DfsFileInfo info = await v2.Files.GetInfoAsync(paths[i], ct);
                    Assert.True(info.Length == baseline[i].Length, $"{paths[i]}: an uncommitted transaction's write became visible (length {info.Length})");
                }
            }

            byte[] a1 = TestData.Bytes(19_101, 333), b1 = TestData.Bytes(19_102, 333);
            byte[] b2 = TestData.Bytes(19_201, 444), c2Data = TestData.Bytes(19_202, 444), a2 = TestData.Bytes(19_203, 444);
            byte[] c3 = TestData.Bytes(19_301, 555), a3 = TestData.Bytes(19_302, 555);

            await using DfsTransaction t1 = await v0.Transactions.BeginAsync(null, ct);
            await using DfsTransaction t2 = await v1.Transactions.BeginAsync(null, ct);

            if (scenario != LockCycleScenario.ThreeCycle)
            {
                await AcquireAsync(t1, paths[0], a1);
                await AcquireAsync(t2, paths[1], b2);

                Exception? e1;
                Exception? e2;
                if (scenario == LockCycleScenario.TwoCycleSimultaneous)
                {
                    Task<Exception?> first = CloseCycleAsync(t1, paths[1], b1, "T1 acquiring b (held by T2)");
                    Task<Exception?> second = CloseCycleAsync(t2, paths[0], a2, "T2 acquiring a (held by T1)");
                    Exception?[] both = await Task.WhenAll(first, second);
                    e1 = both[0];
                    e2 = both[1];
                    AssertConflictOrGranted(e1, "T1 closing the lock cycle");
                    AssertConflictOrGranted(e2, "T2 closing the lock cycle");
                }
                else
                {
                    e1 = await CloseCycleAsync(t1, paths[1], b1, "T1 acquiring b (held by T2)");
                    LockHelpers.AssertSharingConflict(e1, "T1 closing the lock cycle: b is reserved by the active T2");
                    e2 = await CloseCycleAsync(t2, paths[0], a2, "T2 acquiring a (held by T1 unless the policy aborted T1)");
                    AssertConflictOrGranted(e2, "T2 closing the lock cycle");
                }

                Assert.False(e1 is null && e2 is null, "both closing acquisitions were granted: a and b would each have two writers");
                await AssertNothingVisibleAsync(2);

                byte[] expectA;
                byte[] expectB;
                if (e1 is not null && e2 is not null)
                {
                    // Neither got through. Resolution: T2 aborts (releasing b); T1 — or, if the policy already aborted
                    // it, its explicit retry — completes.
                    if (t2.State == DfsTransactionState.Active)
                        await t2.AbortAsync(ct);
                    Assert.Equal(DfsTransactionState.Aborted, t2.State);
                    CommitToken committed = await ContinueOrRetryAsync(v0, t1, ReleaseStreamsAsync, [(paths[1], b1)], [(paths[0], a1), (paths[1], b1)], ct);
                    Assert.False(committed.IsNone);
                    (expectA, expectB) = (a1, b1);
                }
                else
                {
                    // One closing acquisition was granted: the inode it took must have been released by an abort of
                    // its previous holder, which therefore can never commit.
                    bool t1Won = e1 is null;
                    DfsTransaction winner = t1Won ? t1 : t2;
                    DfsTransaction loser = t1Won ? t2 : t1;
                    await ReleaseStreamsAsync();
                    await AssertCannotCommitAsync(loser, $"{(t1Won ? "T2" : "T1")} lost its reservation to the other transaction");
                    CommitToken committed = await winner.CommitAsync(ct);
                    Assert.False(committed.IsNone);
                    Assert.Equal(DfsTransactionState.Committed, winner.State);
                    (expectA, expectB) = t1Won ? (a1, b1) : (a2, b2);
                }

                LockHelpers.BytesEqual(expectA, await LockHelpers.ReadCommittedAsync(v2, paths[0], ct), "a holds exactly the committed transaction's write");
                LockHelpers.BytesEqual(expectB, await LockHelpers.ReadCommittedAsync(v2, paths[1], ct), "b holds exactly the committed transaction's write (the aborted one's never published)");
            }
            else
            {
                await using DfsTransaction t3 = await v2.Transactions.BeginAsync(null, ct);
                await AcquireAsync(t1, paths[0], a1);
                await AcquireAsync(t2, paths[1], b2);
                await AcquireAsync(t3, paths[2], c3);

                LockHelpers.AssertSharingConflict(
                    await CloseCycleAsync(t1, paths[1], b1, "T1 acquiring b (held by T2)"), "T1 in the 3-cycle: b is reserved by the active T2");
                LockHelpers.AssertSharingConflict(
                    await CloseCycleAsync(t2, paths[2], c2Data, "T2 acquiring c (held by T3)"), "T2 in the 3-cycle: c is reserved by the active T3");
                Exception? e3 = await CloseCycleAsync(t3, paths[0], a3, "T3 acquiring a (held by T1 unless the policy aborted T1)");
                AssertConflictOrGranted(e3, "T3 closing the 3-cycle");
                await AssertNothingVisibleAsync(3);

                if (e3 is null)
                {
                    // T3 got a: T1 must have been aborted when its own acquisition failed.
                    await AssertCannotCommitAsync(t1, "T1 lost a to T3");
                    await t3.CommitAsync(ct);
                    Assert.Equal(DfsTransactionState.Committed, t3.State);
                    LockHelpers.BytesEqual(a3, await LockHelpers.ReadCommittedAsync(v2, paths[0], ct), "a = T3 after T3 committed");
                    LockHelpers.BytesEqual(c3, await LockHelpers.ReadCommittedAsync(v2, paths[2], ct), "c = T3 after T3 committed");
                }
                else
                {
                    if (t3.State == DfsTransactionState.Active)
                        await t3.AbortAsync(ct);
                    Assert.Equal(DfsTransactionState.Aborted, t3.State);
                }

                // c is free now (T3 aborted or committed): T2 completes, which frees b for T1.
                await ContinueOrRetryAsync(v1, t2, ReleaseStreamsAsync, [(paths[2], c2Data)], [(paths[1], b2), (paths[2], c2Data)], ct);
                await ContinueOrRetryAsync(v0, t1, ReleaseStreamsAsync, [(paths[1], b1)], [(paths[0], a1), (paths[1], b1)], ct);

                LockHelpers.BytesEqual(a1, await LockHelpers.ReadCommittedAsync(v2, paths[0], ct), "a = T1, the last committed writer of a");
                LockHelpers.BytesEqual(b1, await LockHelpers.ReadCommittedAsync(v2, paths[1], ct), "b = T1, which committed after T2 released b");
                LockHelpers.BytesEqual(c2Data, await LockHelpers.ReadCommittedAsync(v2, paths[2], ct), "c = T2, the last committed writer of c");
            }

            await ReleaseStreamsAsync();

            // No reservation of the resolved cycle survives: a fresh exclusive writer takes every file.
            foreach (string path in paths)
            {
                await using DfsStream exclusive = await v2.Files.OpenAsync(path, FileMode.Open, FileAccess.Write, FileShare.None, ct);
                Assert.True(exclusive.WriterGeneration > 0);
            }
        }, ct);
    }

    /// <summary>A closing acquisition either was granted (null) or failed with a typed, retryable sharing conflict.</summary>
    private static void AssertConflictOrGranted(Exception? error, string because)
    {
        if (error is not null)
            LockHelpers.AssertSharingConflict(error, because);
    }

    /// <summary>
    /// A transaction whose reservation was handed to another transaction must not be able to commit: the attempt fails
    /// with a typed transaction failure (or, if the SDK already knows it was aborted, as an invalid operation) and the
    /// transaction never reaches <see cref="DfsTransactionState.Committed"/>.
    /// </summary>
    private static async Task AssertCannotCommitAsync(DfsTransaction tx, string because)
    {
        Exception? error = await LockHelpers.CatchAsync(() => tx.CommitAsync(TestContext.Current.CancellationToken));
        Assert.True(error is DfsTransactionConflictException or InvalidOperationException,
            $"{because}: its commit must fail with a typed transaction failure, got {LockHelpers.Describe(error)}");
        Assert.NotEqual(DfsTransactionState.Committed, tx.State);
    }

    /// <summary>
    /// Continues <paramref name="tx"/> with <paramref name="remaining"/> writes and commits it. If the conflict policy
    /// aborted the transaction when its acquisition failed (a legal resolution), the abort must surface as a typed
    /// transaction failure (or an invalid operation on the already-aborted transaction), and an explicit retry with
    /// <paramref name="all"/> writes must commit.
    /// </summary>
    private static async Task<CommitToken> ContinueOrRetryAsync(DfsVolume volume, DfsTransaction tx, Func<Task> releaseStreams,
        IReadOnlyList<(string Path, byte[] Data)> remaining, IReadOnlyList<(string Path, byte[] Data)> all, CancellationToken ct)
    {
        CommitToken token = CommitToken.None;
        Exception? error = await LockHelpers.CatchAsync(async () =>
        {
            foreach ((string path, byte[] data) in remaining)
                await tx.Files.WriteAllBytesAsync(path, data, ct);
            await releaseStreams();
            token = await tx.CommitAsync(ct);
        });
        if (error is null)
        {
            Assert.Equal(DfsTransactionState.Committed, tx.State);
            return token;
        }

        Assert.True(error is DfsTransactionConflictException or InvalidOperationException,
            $"a transaction that cannot continue after a lock-cycle conflict must report a typed transaction failure, got {LockHelpers.Describe(error)}");
        Assert.Equal(DfsTransactionState.Aborted, tx.State);
        await releaseStreams();
        await using DfsTransaction retry = await volume.Transactions.BeginAsync(null, ct);
        foreach ((string path, byte[] data) in all)
            await retry.Files.WriteAllBytesAsync(path, data, ct);
        CommitToken retried = await retry.CommitAsync(ct);
        Assert.Equal(DfsTransactionState.Committed, retry.State);
        return retried;
    }
}
