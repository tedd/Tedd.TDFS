using Tedd.Tdfs.Server;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;
using static Tedd.Tdfs.Tests.Unit.Pool.PoolHelpers;

namespace Tedd.Tdfs.Tests.Unit.Pool;

/// <summary>
/// POOL — namespaces and storage pools; unit-level rows (FSD Addendum A §A1 NSP-01, §A2 POOL-01..POOL-05, catalog §A5.2).
/// Invariants: INV-08, INV-11, INV-12, INV-16, INV-18.
/// </summary>
public sealed class PoolUnitTests
{
    // ------------------------------------------------------------------------------------------------------------
    // POOL-001 VolumesAreSeparateNamespaces
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Two volumes (on the same pool, or on disjoint pools) hold entries at identical paths with different contents and
    /// even different kinds (<c>/clash</c> is a file in one volume and a directory in the other). Reads, Exists, GetInfo,
    /// enumeration, paged listing, directory entry counts, snapshots and transactions of one volume only ever show that
    /// volume's entries; deleting, moving and creating in one volume leaves the other's entries, inode identities and
    /// revisions untouched; commit tokens are bound to their volume.
    /// </summary>
    [Theory]
    [InlineData("SamePool")]
    [InlineData("DisjointPools")]
    [Catalog("POOL-001", "U", "INV-08", "INV-12")]
    public async Task POOL_001_VolumesAreSeparateNamespaces(string layout)
    {
        bool disjoint = layout == "DisjointPools";
        TestClusterOptions options = disjoint
            ? Realtime(TwoPools(), 3, Volume(VolA, PoolA), Volume(VolB, PoolB))
            : TestClusterOptions.ThreeZones with { Volumes = [new TestVolumeSpec(VolA), new TestVolumeSpec(VolB)] };
        await ClusterTest.RunAsync(options, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(null, Ct);
            DfsVolume va = client.GetVolume(VolA);
            DfsVolume vb = client.GetVolume(VolB);
            DfsVolumeInfo infoA = await va.GetInfoAsync(Ct);
            DfsVolumeInfo infoB = await vb.GetInfoAsync(Ct);
            Assert.NotEqual(infoA.Id, infoB.Id);

            byte[] smallA = TestData.Bytes(1_000, 100), smallB = TestData.Bytes(1_001, 150);
            byte[] bigA = TestData.Bytes(1_002, 2 * Chunk + 1), bigB = TestData.Bytes(1_003, 3 * Chunk);
            byte[] onlyA = TestData.Bytes(1_004, 10), clashA = TestData.Bytes(1_005, 20);
            CommitToken tokenA = await va.Files.WriteAllBytesAsync("/shared.bin", smallA, Ct);
            CommitToken tokenB = await vb.Files.WriteAllBytesAsync("/shared.bin", smallB, Ct);
            Assert.Equal(infoA.Id, tokenA.VolumeId);
            Assert.Equal(infoB.Id, tokenB.VolumeId);
            Assert.False(tokenA.IsComparableWith(tokenB), "commit tokens of different volumes are not comparable");
            await va.Files.WriteAllBytesAsync("/big.bin", bigA, Ct);
            await vb.Files.WriteAllBytesAsync("/big.bin", bigB, Ct);
            await va.Directories.CreateDirectoryAsync("/dir", Ct);
            await vb.Directories.CreateDirectoryAsync("/dir", Ct);
            await va.Files.WriteAllBytesAsync("/dir/x", new byte[] { 1 }, Ct);
            await vb.Files.WriteAllBytesAsync("/dir/y", new byte[] { 2 }, Ct);
            await va.Files.WriteAllBytesAsync("/onlyA", onlyA, Ct);
            await vb.Directories.CreateDirectoryAsync("/onlyB", Ct);
            await va.Files.WriteAllBytesAsync("/clash", clashA, Ct);
            await vb.Directories.CreateDirectoryAsync("/clash", Ct);
            await vb.Files.WriteAllBytesAsync("/clash/c", new byte[] { 3 }, Ct);

            // Contents and metadata at identical paths are per volume.
            Same(smallA, await va.Files.ReadAllBytesAsync("/shared.bin", Ct), "va:/shared.bin");
            Same(smallB, await vb.Files.ReadAllBytesAsync("/shared.bin", Ct), "vb:/shared.bin");
            Same(bigA, await va.Files.ReadAllBytesAsync("/big.bin", Ct), "va:/big.bin");
            Same(bigB, await vb.Files.ReadAllBytesAsync("/big.bin", Ct), "vb:/big.bin");
            Assert.Equal(smallA.Length, (await va.Files.GetInfoAsync("/shared.bin", Ct)).Length);
            Assert.Equal(smallB.Length, (await vb.Files.GetInfoAsync("/shared.bin", Ct)).Length);
            Assert.Equal(bigB.Length, (await vb.Files.GetInfoAsync("/big.bin", Ct)).Length);

            // Exists / GetInfo never see the other volume's entries.
            Assert.False(await va.Files.ExistsAsync("/dir/y", Ct));
            Assert.False(await vb.Files.ExistsAsync("/dir/x", Ct));
            Assert.False(await vb.Files.ExistsAsync("/onlyA", Ct));
            Assert.False(await vb.Directories.ExistsAsync("/onlyA", Ct));
            Assert.False(await va.Directories.ExistsAsync("/onlyB", Ct));
            Assert.False(await va.Files.ExistsAsync("/onlyB", Ct));
            await Assert.ThrowsAnyAsync<FileNotFoundException>(() => vb.Files.GetInfoAsync("/onlyA", Ct));
            await Assert.ThrowsAnyAsync<DirectoryNotFoundException>(() => va.Directories.GetInfoAsync("/onlyB", Ct));
            await Assert.ThrowsAnyAsync<FileNotFoundException>(() => va.Files.ReadAllBytesAsync("/dir/y", Ct));
            Assert.True(await va.Files.ExistsAsync("/clash", Ct));
            Assert.False(await va.Directories.ExistsAsync("/clash", Ct));
            Assert.True(await vb.Directories.ExistsAsync("/clash", Ct));
            Assert.False(await vb.Files.ExistsAsync("/clash", Ct));

            // Enumeration, paged listing and entry counts list exactly the volume's own entries.
            string[] rootA = ["big.bin", "clash", "dir", "onlyA", "shared.bin"];
            string[] rootB = ["big.bin", "clash", "dir", "onlyB", "shared.bin"];
            Assert.Equal(rootA, await NamesAsync(va.Directories, "/"));
            Assert.Equal(rootB, await NamesAsync(vb.Directories, "/"));
            Assert.Equal(rootA, await PagedNamesAsync(va.Directories, "/", 2));
            Assert.Equal(rootB, await PagedNamesAsync(vb.Directories, "/", 2));
            Assert.Equal(new[] { "x" }, await NamesAsync(va.Directories, "/dir"));
            Assert.Equal(new[] { "y" }, await NamesAsync(vb.Directories, "/dir"));
            Assert.Equal(5, (await va.Directories.GetInfoAsync("/", Ct)).EntryCount);
            Assert.Equal(5, (await vb.Directories.GetInfoAsync("/", Ct)).EntryCount);
            Assert.Equal(1, (await vb.Directories.GetInfoAsync("/dir", Ct)).EntryCount);
            await foreach (DfsEntryInfo e in va.Directories.EnumerateAsync("/", null, Ct))
            {
                if (e.Name == "clash")
                    Assert.Equal(DfsEntryKind.File, e.Kind);
            }

            await foreach (DfsEntryInfo e in vb.Directories.EnumerateAsync("/", null, Ct))
            {
                if (e.Name == "clash")
                    Assert.Equal(DfsEntryKind.Directory, e.Kind);
            }

            await vb.Snapshots.CreateAsync("before", null, Ct);
            DfsFileInfo bShared = await vb.Files.GetInfoAsync("/shared.bin", Ct);
            DfsFileInfo bBig = await vb.Files.GetInfoAsync("/big.bin", Ct);
            DfsDirectoryInfo bDir = await vb.Directories.GetInfoAsync("/dir", Ct);

            // Mutations in va leave vb untouched; vb can create a directory where va has a file of the same name.
            await va.Files.DeleteAsync("/shared.bin", Ct);
            await va.Files.MoveAsync("/big.bin", "/moved.bin", false, Ct);
            await va.Directories.DeleteAsync("/dir", true, Ct);
            await vb.Directories.CreateDirectoryAsync("/onlyA", Ct);

            DfsFileInfo bShared2 = await vb.Files.GetInfoAsync("/shared.bin", Ct);
            DfsFileInfo bBig2 = await vb.Files.GetInfoAsync("/big.bin", Ct);
            Assert.Equal((bShared.InodeId, bShared.Revision), (bShared2.InodeId, bShared2.Revision));
            Assert.Equal((bBig.InodeId, bBig.Revision), (bBig2.InodeId, bBig2.Revision));
            Assert.Equal(bDir.InodeId, (await vb.Directories.GetInfoAsync("/dir", Ct)).InodeId);
            Same(smallB, await vb.Files.ReadAllBytesAsync("/shared.bin", Ct), "vb:/shared.bin after va's delete");
            Same(bigB, await vb.Files.ReadAllBytesAsync("/big.bin", Ct), "vb:/big.bin after va's move");
            Assert.True(await vb.Files.ExistsAsync("/dir/y", Ct));
            Assert.False(await vb.Files.ExistsAsync("/moved.bin", Ct));
            Assert.True(await vb.Directories.ExistsAsync("/onlyA", Ct));
            Assert.False(await va.Files.ExistsAsync("/shared.bin", Ct));
            Same(bigA, await va.Files.ReadAllBytesAsync("/moved.bin", Ct), "va:/moved.bin");
            Same(onlyA, await va.Files.ReadAllBytesAsync("/onlyA", Ct), "va:/onlyA is still a file");
            Assert.False(await va.Directories.ExistsAsync("/onlyA", Ct));
            Assert.Equal(new[] { "clash", "moved.bin", "onlyA" }, await NamesAsync(va.Directories, "/"));
            Assert.Equal(new[] { "big.bin", "clash", "dir", "onlyA", "onlyB", "shared.bin" }, await NamesAsync(vb.Directories, "/"));

            // vb's snapshot holds vb's entries only; va's transaction view holds va's entries only.
            await using (DfsSnapshotView view = await vb.Snapshots.OpenAsync("before", Ct))
            {
                Assert.Equal(rootB, await NamesAsync(view.Directories, "/"));
                Assert.False(await view.Files.ExistsAsync("/onlyA", Ct));
                Same(smallB, await view.Files.ReadAllBytesAsync("/shared.bin", Ct), "vb snapshot");
            }

            await using (DfsTransaction tx = await va.Transactions.BeginAsync(null, Ct))
            {
                Assert.False(await tx.Files.ExistsAsync("/shared.bin", Ct), "va deleted /shared.bin; vb's entry must not show through");
                Assert.False(await tx.Directories.ExistsAsync("/onlyB", Ct));
                Same(onlyA, await tx.Files.ReadAllBytesAsync("/onlyA", Ct), "va transaction read");
                await tx.AbortAsync(Ct);
            }

            if (disjoint)
            {
                await AssertPlacedWithinAsync(cluster, await ObjectsOfAsync(cluster, VolA, "/moved.bin"), Members(TwoPools(), PoolA), 2, 2, "va data");
                await AssertPlacedWithinAsync(cluster, await ObjectsOfAsync(cluster, VolB, "/big.bin"), Members(TwoPools(), PoolB), 2, 2, "vb data");
            }
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // POOL-002 HandlesAndTokensDoNotCrossVolumes
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<string> CrossVolumeArtifacts => new() { "Handle", "ContinuationToken", "OperationId", "TransactionId", "SnapshotName" };

    /// <summary>
    /// Identifiers issued by volume <c>vb</c> — handle numbers (presented raw), directory continuation tokens, operation
    /// ids, transaction ids and snapshot names — are rejected (or resolve to nothing) in volume <c>va</c>, never expose or
    /// address anything of <c>vb</c> there, and keep working in <c>vb</c>. Handle numbers and operation-id sessions are
    /// allocated per volume (ARCHITECTURE §2.2), so a foreign number can coincide with one the target volume issued itself;
    /// the tests drive such collisions where numbering allows them, accept that a coinciding number addresses only the
    /// target volume's own state, and require rejection of every foreign identifier the target volume never issued.
    /// </summary>
    [Theory]
    [MemberData(nameof(CrossVolumeArtifacts))]
    [Catalog("POOL-002", "U", "INV-08", "INV-12")]
    public async Task POOL_002_HandlesAndTokensDoNotCrossVolumes(string artifact)
    {
        TestClusterOptions options = TestClusterOptions.ThreeZones with { Volumes = [new TestVolumeSpec(VolA), new TestVolumeSpec(VolB)] };
        await ClusterTest.RunAsync(options, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(null, Ct);
            DfsVolume va = client.GetVolume(VolA);
            DfsVolume vb = client.GetVolume(VolB);
            switch (artifact)
            {
                case "Handle":
                    await CrossVolumeHandlesAsync(cluster, va, vb);
                    break;
                case "ContinuationToken":
                    await CrossVolumeContinuationTokensAsync(va, vb);
                    break;
                case "OperationId":
                    await CrossVolumeOperationIdsAsync(cluster, va, vb);
                    break;
                case "TransactionId":
                    await CrossVolumeTransactionIdsAsync(cluster, va);
                    break;
                case "SnapshotName":
                    await CrossVolumeSnapshotNamesAsync(cluster, va, vb);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(artifact));
            }
        }, Ct);
    }

    private static async Task CrossVolumeHandlesAsync(TestCluster cluster, DfsVolume va, DfsVolume vb)
    {
        byte[] bytesA = TestData.Bytes(2_000, 32), bytesB = TestData.Bytes(2_001, 32);
        await va.Files.WriteAllBytesAsync("/h.bin", bytesA, Ct);
        await vb.Files.WriteAllBytesAsync("/h.bin", bytesB, Ct);
        await using DfsStream sdkHandle = await va.Files.OpenAsync("/h.bin", FileMode.Open, FileAccess.Read, FileShare.ReadWrite, Ct);

        await using RawProtocolClient raw = await cluster.ConnectRawAsync((await cluster.GetLeaderAsync(VolA, Ct)).Name, Ct);
        Assert.Equal(DfsErrorCode.None, await raw.AuthenticateAsync(cluster.AdminCredentials));
        var handlesA = new List<HandleId>();
        var handlesB = new List<HandleId>();
        await OpenRawAsync(VolA, handlesA);
        await OpenRawAsync(VolB, handlesB);
        // Open in whichever volume is numerically behind: per-volume counters collide; volume-bound ids never do.
        for (int i = 0; i < 128 && !Collides(); i++)
        {
            if (handlesB[^1].Value < handlesA[^1].Value)
                await OpenRawAsync(VolB, handlesB);
            else
                await OpenRawAsync(VolA, handlesA);
        }

        // Then vb runs ahead: with per-volume numbering these numbers were never issued by va, so va must reject them.
        for (int i = 0; i < 3; i++)
            await OpenRawAsync(VolB, handlesB);

        Log($"POOL-002: {handlesA.Count} raw handles in va, {handlesB.Count} in vb; numeric collision: {Collides()}");
        var bytesOf = new Dictionary<string, byte[]>(StringComparer.Ordinal) { [VolA] = bytesA, [VolB] = bytesB };
        foreach ((string volume, List<HandleId> handles) in new[] { (VolA, handlesA), (VolB, handlesB) })
        {
            foreach (HandleId own in handles)
            {
                (DfsErrorCode code, byte[] data) = await raw.ReadThroughHandleAsync(volume, own, 0, bytesOf[volume].Length);
                Assert.Equal(DfsErrorCode.None, code);
                Same(bytesOf[volume], data, $"own handle {own.Value} in {volume}");
            }
        }

        // Handle ids are allocated by each volume's state machine (ARCHITECTURE §2.2), so a number issued by the other
        // volume may coincide with one this volume issued itself; such a number can only ever address this volume's own
        // handle. Every other foreign number must be rejected, and no foreign number may ever yield the other volume's bytes.
        IReadOnlySet<ulong> issuedA = await HandleNumbersAsync(VolA);
        int activeA = (await cluster.Inspect.GetVolumeStateAsync(VolA)).Handles.Count(h => !h.Expired);
        foreach ((string volume, List<HandleId> foreignHandles) in new[] { (VolA, handlesB), (VolB, handlesA) })
        {
            IReadOnlySet<ulong> issued = await HandleNumbersAsync(volume);
            int unissued = 0;
            foreach (HandleId foreign in foreignHandles)
            {
                (DfsErrorCode code, byte[] data) = await raw.ReadThroughHandleAsync(volume, foreign, 0, 32);
                if (issued.Contains(foreign.Value))
                {
                    if (code == DfsErrorCode.None)
                        Same(bytesOf[volume], data, $"number {foreign.Value} (also issued by {volume}) may only address {volume}'s own handle");
                    else
                        Assert.Empty(data);
                    continue;
                }

                unissued++;
                Assert.True(code != DfsErrorCode.None, $"handle {foreign.Value} of the other volume was accepted by {volume}, which never issued it");
                Assert.Empty(data);
            }

            if (volume == VolA)
                Assert.True(unissued > 0, "vb's handle numbers include numbers va never issued (rejection is exercised)");
        }

        foreach (HandleId foreign in handlesB.Where(h => !issuedA.Contains(h.Value)))
            Assert.NotEqual(DfsErrorCode.None, await raw.CloseHandleInVolumeAsync(VolA, foreign));

        // No local handle was closed or touched by foreign numbers.
        Assert.Equal(activeA, (await cluster.Inspect.GetVolumeStateAsync(VolA)).Handles.Count(h => !h.Expired));
        foreach (HandleId own in handlesA)
        {
            (DfsErrorCode code, byte[] data) = await raw.ReadThroughHandleAsync(VolA, own, 0, bytesA.Length);
            Assert.Equal(DfsErrorCode.None, code);
            Same(bytesA, data, $"own handle {own.Value} after foreign closes");
        }

        byte[] buffer = new byte[bytesA.Length];
        await sdkHandle.ReadExactlyAsync(buffer, Ct);
        Same(bytesA, buffer, "SDK handle");

        bool Collides() => handlesA.Any(a => handlesB.Any(b => a.Value == b.Value));

        async Task<IReadOnlySet<ulong>> HandleNumbersAsync(string volume) =>
            (await cluster.Inspect.GetVolumeStateAsync(volume)).Handles.Select(h => h.Id.Value).ToHashSet();

        async Task OpenRawAsync(string volume, List<HandleId> into)
        {
            (DfsErrorCode code, HandleId handle) = await raw.OpenHandleInVolumeAsync(volume, "/h.bin", FileAccess.Read, FileShare.ReadWrite);
            Assert.Equal(DfsErrorCode.None, code);
            into.Add(handle);
        }
    }

    private static async Task CrossVolumeContinuationTokensAsync(DfsVolume va, DfsVolume vb)
    {
        await va.Directories.CreateDirectoryAsync("/dir", Ct);
        await vb.Directories.CreateDirectoryAsync("/dir", Ct);
        for (int i = 0; i < 6; i++)
        {
            await va.Files.WriteAllBytesAsync($"/dir/a{i}", new byte[] { 1 }, Ct);
            await vb.Files.WriteAllBytesAsync($"/dir/b{i}", new byte[] { 2 }, Ct);
        }

        var paging = new DfsEnumerationOptions { PageSize = 2 };
        DfsDirectoryPage firstB = await vb.Directories.ListPageAsync("/dir", paging, null, Ct);
        Assert.NotNull(firstB.ContinuationToken);
        DfsDirectoryPage? leaked = null;
        Exception? ex = await Record.ExceptionAsync(async () => leaked = await va.Directories.ListPageAsync("/dir", paging, firstB.ContinuationToken, Ct));
        Assert.True(leaked is null, $"vb's continuation token produced a page in va: [{string.Join(",", leaked?.Entries.Select(e => e.Name) ?? Enumerable.Empty<string>())}]");
        Assert.True(ex is ArgumentException || ex is DfsException { ErrorCode: DfsErrorCode.CrossVolume or DfsErrorCode.ContinuationExpired or DfsErrorCode.InvalidArgument },
            $"vb's continuation token presented to va must be rejected: {Describe(ex)}");

        // va's own paging is unaffected and lists only va's entries.
        string[] namesA = await PagedNamesAsync(va.Directories, "/dir", 2);
        Assert.Equal(Enumerable.Range(0, 6).Select(i => $"a{i}"), namesA);

        // vb's token still resumes vb's enumeration exactly where it stopped.
        var namesB = new List<string>(firstB.Entries.Select(e => e.Name));
        string? token = firstB.ContinuationToken;
        while (token is not null)
        {
            DfsDirectoryPage page = await vb.Directories.ListPageAsync("/dir", paging, token, Ct);
            namesB.AddRange(page.Entries.Select(e => e.Name));
            token = page.ContinuationToken;
        }

        Assert.Equal(Enumerable.Range(0, 6).Select(i => $"b{i}"), namesB.Order(StringComparer.Ordinal));
        Assert.Equal(namesB.Count, namesB.Distinct(StringComparer.Ordinal).Count());
    }

    private static async Task CrossVolumeOperationIdsAsync(TestCluster cluster, DfsVolume va, DfsVolume vb)
    {
        // The main client holds a session in both volumes before any foreign session is chosen (so neither volume issues
        // a new session to it later), and va has issued and used an id of its own.
        DfsOperationId usedA = await va.NewOperationIdAsync(Ct);
        await vb.NewOperationIdAsync(Ct);
        await va.Files.WriteAllBytesAsync("/own", new byte[] { 9 }, new DfsMutationOptions { OperationId = usedA }, Ct);

        // Operation-id sessions are issued per volume (ARCHITECTURE §2.2), so vb's id is taken from a session number va has
        // never issued: only then is it recognisably foreign to va (a coinciding number would name one of va's own sessions).
        await using DfsClient clientB = await ClientWithSessionUnknownToAsync(cluster, VolB, VolA, "opb");
        DfsVolume fb = clientB.GetVolume(VolB);
        DfsOperationId opB = await fb.NewOperationIdAsync(Ct);
        CommitToken tokenB = await fb.Files.AppendAllBytesAsync("/log", new byte[] { 1 }, new DfsMutationOptions { OperationId = opB }, Ct);
        Assert.False(await IsSessionOfAsync(cluster, VolA, opB.Session), $"{opB} must name a session va never issued");

        Exception? reuse = await Record.ExceptionAsync(() => va.Files.AppendAllBytesAsync("/log", new byte[] { 2 }, new DfsMutationOptions { OperationId = opB }, Ct));
        Assert.True(IsOperationIdRejection(reuse), $"vb's operation id used for a va mutation: {Describe(reuse)}");
        Assert.False(await va.Files.ExistsAsync("/log", Ct), "the foreign id must not have an effect in va");

        foreach (bool resolve in new[] { false, true })
        {
            DfsOperationOutcome? outcome = null;
            Exception? queryError = await Record.ExceptionAsync(async () => outcome = await va.QueryOperationAsync(opB, resolve, Ct));
            if (queryError is not null)
            {
                Assert.True(IsOperationIdRejection(queryError), $"querying vb's id in va (resolve={resolve}): {Describe(queryError)}");
                continue;
            }

            Assert.NotNull(outcome);
            Assert.True(outcome.Status != DfsOutcomeStatus.Committed, $"va reported vb's operation as {outcome.Status} at {outcome.Token} (resolve={resolve})");
            Assert.NotEqual(tokenB, outcome.Token);
        }

        // In vb the id still has exactly its one committed effect, and a retry is answered from the outcome record.
        DfsOperationOutcome own = await fb.QueryOperationAsync(opB, false, Ct);
        Assert.Equal(DfsOutcomeStatus.Committed, own.Status);
        Assert.Equal(tokenB, own.Token);
        Assert.Equal(tokenB, await fb.Files.AppendAllBytesAsync("/log", new byte[] { 1 }, new DfsMutationOptions { OperationId = opB }, Ct));
        Same(new byte[] { 1 }, await vb.Files.ReadAllBytesAsync("/log", Ct), "vb:/log has one append");

        // Symmetric: an unused id reserved in va (from a session vb never issued) is rejected by vb and stays usable in va.
        await using DfsClient clientA = await ClientWithSessionUnknownToAsync(cluster, VolA, VolB, "opa");
        DfsVolume fa = clientA.GetVolume(VolA);
        DfsOperationId unusedA = await fa.NewOperationIdAsync(Ct);
        Assert.False(await IsSessionOfAsync(cluster, VolB, unusedA.Session), $"{unusedA} must name a session vb never issued");
        Exception? reverse = await Record.ExceptionAsync(() => vb.Files.WriteAllBytesAsync("/other", new byte[] { 3 }, new DfsMutationOptions { OperationId = unusedA }, Ct));
        Assert.True(IsOperationIdRejection(reverse), $"va's operation id used for a vb mutation: {Describe(reverse)}");
        Assert.False(await vb.Files.ExistsAsync("/other", Ct));
        await fa.Files.WriteAllBytesAsync("/other", new byte[] { 4 }, new DfsMutationOptions { OperationId = unusedA }, Ct);
        Same(new byte[] { 4 }, await va.Files.ReadAllBytesAsync("/other", Ct), "va:/other");
    }

    private static async Task CrossVolumeTransactionIdsAsync(TestCluster cluster, DfsVolume va)
    {
        // As for operation ids: the main client's va session exists first, and vb's transaction runs in a vb session whose
        // number va never issued.
        await va.NewOperationIdAsync(Ct);
        await using DfsClient clientB = await ClientWithSessionUnknownToAsync(cluster, VolB, VolA, "txb");
        DfsVolume vb = clientB.GetVolume(VolB);
        DfsTransactionId txB;
        await using (DfsTransaction tx = await vb.Transactions.BeginAsync(null, Ct))
        {
            await tx.Files.WriteAllBytesAsync("/tx.bin", new byte[] { 7 }, Ct);
            await tx.CommitAsync(Ct);
            txB = tx.Id;
        }

        Assert.False(await IsSessionOfAsync(cluster, VolA, txB.Operation.Session), $"{txB} must belong to a session va never issued");
        await using (DfsTransaction txA = await va.Transactions.BeginAsync(null, Ct))
        {
            await txA.Files.WriteAllBytesAsync("/tx.bin", new byte[] { 8 }, Ct);
            await txA.CommitAsync(Ct);
        }

        foreach (bool resolve in new[] { false, true })
        {
            DfsOperationOutcome? outcome = null;
            Exception? error = await Record.ExceptionAsync(async () => outcome = await va.Transactions.QueryOutcomeAsync(txB, resolve, Ct));
            if (error is not null)
            {
                Assert.True(IsOperationIdRejection(error), $"querying vb's transaction in va (resolve={resolve}): {Describe(error)}");
                continue;
            }

            Assert.NotNull(outcome);
            Assert.True(outcome.Status != DfsOutcomeStatus.Committed, $"va reported vb's transaction as committed (resolve={resolve})");
        }

        Assert.Equal(DfsOutcomeStatus.Committed, (await vb.Transactions.QueryOutcomeAsync(txB, false, Ct)).Status);
        Same(new byte[] { 7 }, await vb.Files.ReadAllBytesAsync("/tx.bin", Ct), "vb:/tx.bin");
        Same(new byte[] { 8 }, await va.Files.ReadAllBytesAsync("/tx.bin", Ct), "va:/tx.bin");
    }

    /// <summary>
    /// A fresh client whose session in <paramref name="from"/> has a number <paramref name="other"/> never issued. Session
    /// numbers are allocated per volume, so the first sessions of two volumes usually coincide; each fresh client opens a
    /// new, higher-numbered session in <paramref name="from"/> only, until one falls outside <paramref name="other"/>'s.
    /// </summary>
    private static async Task<DfsClient> ClientWithSessionUnknownToAsync(TestCluster cluster, string from, string other, string namePrefix)
    {
        for (int attempt = 0; attempt < 64; attempt++)
        {
            DfsClient candidate = await cluster.ConnectAsync(new TestClientOptions { Name = $"{namePrefix}{attempt}" }, Ct);
            DfsOperationId probe = await candidate.GetVolume(from).NewOperationIdAsync(Ct);
            if (!await IsSessionOfAsync(cluster, other, probe.Session))
                return candidate;
            await candidate.DisposeAsync();
        }

        Assert.Fail($"64 fresh clients found no session of {from} outside the session numbers of {other}");
        return null!;
    }

    private static async Task<bool> IsSessionOfAsync(TestCluster cluster, string volume, ulong session) =>
        (await cluster.Inspect.GetVolumeStateAsync(volume)).Sessions.Any(s => s.Id == session);

    private static async Task CrossVolumeSnapshotNamesAsync(TestCluster cluster, DfsVolume va, DfsVolume vb)
    {
        byte[] inA = TestData.Bytes(2_100, 64), inB = TestData.Bytes(2_101, 64);
        await va.Files.WriteAllBytesAsync("/s.bin", inA, Ct);
        await vb.Files.WriteAllBytesAsync("/s.bin", inB, Ct);
        DfsSnapshotInfo snapB = await vb.Snapshots.CreateAsync("snapB", null, Ct);
        Assert.Equal(VolB, snapB.Volume);

        await AssertEx.ThrowsDfsAsync<DfsException>(() => va.Snapshots.GetAsync("snapB", Ct), DfsErrorCode.SnapshotNotFound);
        await AssertEx.ThrowsDfsAsync<DfsException>(async () =>
        {
            await using DfsSnapshotView view = await va.Snapshots.OpenAsync("snapB", Ct);
        }, DfsErrorCode.SnapshotNotFound);
        await AssertEx.ThrowsDfsAsync<DfsException>(() => va.Snapshots.SetRetentionAsync("snapB", new DfsRetention(null, true), Ct), DfsErrorCode.SnapshotNotFound);
        await AssertEx.ThrowsDfsAsync<DfsException>(() => va.Snapshots.RestoreAsync("snapB", "restored-from-va", Ct), DfsErrorCode.SnapshotNotFound);
        Assert.DoesNotContain("restored-from-va", await cluster.Admin.ListVolumesAsync(Ct));
        Exception? delete = await Record.ExceptionAsync(() => va.Snapshots.DeleteAsync("snapB", Ct));
        Assert.True(delete is null or DfsException { ErrorCode: DfsErrorCode.SnapshotNotFound }, $"deleting vb's snapshot name in va: {Describe(delete)}");
        Assert.DoesNotContain(await va.Snapshots.ListAsync(Ct), s => s.Name == "snapB");

        // vb's snapshot is intact: same identity, retention unchanged, vb's bytes.
        DfsSnapshotInfo stillB = await vb.Snapshots.GetAsync("snapB", Ct);
        Assert.Equal(snapB.Id, stillB.Id);
        Assert.Equal(snapB.Retention, stillB.Retention);
        await using (DfsSnapshotView viewB = await vb.Snapshots.OpenAsync("snapB", Ct))
            Same(inB, await viewB.Files.ReadAllBytesAsync("/s.bin", Ct), "vb snapshot");

        // Snapshot names are per volume: va takes its own "snapB" (also through commit-and-snapshot) with va's content.
        DfsSnapshotInfo snapA = await va.Snapshots.CreateAsync("snapB", null, Ct);
        Assert.Equal(VolA, snapA.Volume);
        Assert.NotEqual(snapB.Id, snapA.Id);
        await vb.Snapshots.CreateAsync("second", null, Ct);
        await va.Files.WriteAllBytesAsync("/c.bin", new byte[] { 5 }, new DfsMutationOptions { CommitSnapshotName = "second" }, Ct);
        await using (DfsSnapshotView viewA = await va.Snapshots.OpenAsync("snapB", Ct))
            Same(inA, await viewA.Files.ReadAllBytesAsync("/s.bin", Ct), "va snapshot");
        await using (DfsSnapshotView viewB = await vb.Snapshots.OpenAsync("snapB", Ct))
            Same(inB, await viewB.Files.ReadAllBytesAsync("/s.bin", Ct), "vb snapshot after va took the same name");
        await using (DfsSnapshotView secondA = await va.Snapshots.OpenAsync("second", Ct))
            Assert.True(await secondA.Files.ExistsAsync("/c.bin", Ct));
        await using (DfsSnapshotView secondB = await vb.Snapshots.OpenAsync("second", Ct))
            Assert.False(await secondB.Files.ExistsAsync("/c.bin", Ct));
        Assert.Equal(new[] { "second", "snapB" }, (await va.Snapshots.ListAsync(Ct)).Select(s => s.Name).Order(StringComparer.Ordinal));
        Assert.Equal(new[] { "second", "snapB" }, (await vb.Snapshots.ListAsync(Ct)).Select(s => s.Name).Order(StringComparer.Ordinal));
        Assert.Equal(snapB.Id, (await vb.Snapshots.GetAsync("snapB", Ct)).Id);
    }

    private static bool IsOperationIdRejection(Exception? ex) =>
        ex is ArgumentException
        || ex is DfsException { ErrorCode: DfsErrorCode.OperationIdMismatch or DfsErrorCode.CrossVolume or DfsErrorCode.InvalidArgument or DfsErrorCode.OperationExpired };

    // ------------------------------------------------------------------------------------------------------------
    // POOL-003 DefaultPoolContainsAllNodes
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// The <c>default</c> pool lists every live node (also one that belongs to a named pool) and follows the live node set
    /// when a node joins and when one is removed; named pools are unaffected. A test volume and an admin-created volume
    /// that name no pool are bound to <c>default</c>, listed under it, and place data on every default-pool node.
    /// </summary>
    [Fact]
    [Catalog("POOL-003", "U", "INV-11")]
    public async Task POOL_003_DefaultPoolContainsAllNodes()
    {
        Assert.Equal(DfsPoolNames.Default, new TestVolumeSpec("vol").StoragePool);
        Assert.Equal(DfsPoolNames.Default, new DfsVolumeSpec { Name = "x" }.StoragePool);
        IReadOnlyList<TestNodeSpec> nodes = [Node("n0", "z1"), Node("n1", "z2"), Node("n2", "z3"), Node("n3", "z1", "fast")];
        string[] all = ["n0", "n1", "n2", "n3"];
        await ClusterTest.RunAsync(Realtime(nodes, 3, new TestVolumeSpec("vol")), async cluster =>
        {
            DfsAdminClient admin = cluster.Admin;
            IReadOnlyList<DfsPoolInfo> pools = await admin.ListPoolsAsync(Ct);
            Assert.Equal(new[] { DfsPoolNames.Default, "fast" }, Sorted(pools.Select(p => p.Name)));
            DfsPoolInfo defaultPool = pools.Single(p => p.Name == DfsPoolNames.Default);
            DfsPoolInfo fastPool = pools.Single(p => p.Name == "fast");
            Assert.Equal(all, Sorted(defaultPool.Nodes));
            Assert.Equal(new[] { "n3" }, fastPool.Nodes);
            Assert.Equal(all, Sorted(await cluster.Inspect.GetPoolNodesAsync(DfsPoolNames.Default)));
            Assert.Contains("vol", defaultPool.Volumes);
            Assert.DoesNotContain("vol", fastPool.Volumes);
            Assert.Equal((await admin.ListNodesAsync(Ct)).Sum(n => n.CapacityBytes), defaultPool.CapacityBytes);

            await using DfsClient client = await cluster.ConnectAsync(null, Ct);
            DfsVolume vol = client.GetVolume("vol");
            Assert.Equal(DfsPoolNames.Default, (await vol.GetInfoAsync(Ct)).StoragePool);
            Assert.Equal(DfsPoolNames.Default, (await admin.GetVolumeStatusAsync("vol", Ct)).Info.StoragePool);

            DfsVolumeInfo created = await admin.CreateVolumeAsync(new DfsVolumeSpec { Name = "v2", DefaultPolicy = ThreeZones }, Ct);
            Assert.Equal(DfsPoolNames.Default, created.StoragePool);
            Assert.Contains("v2", (await PoolAsync(cluster, DfsPoolNames.Default)).Volumes);
            Assert.DoesNotContain("v2", (await PoolAsync(cluster, "fast")).Volumes);

            // Every default-pool node is a placement candidate, including the one that also belongs to "fast".
            for (int i = 0; i < 16; i++)
                await vol.Files.WriteAllBytesAsync($"/f{i}", TestData.Bytes(3_000 + (ulong)i, 2 * Chunk + i), Ct);
            await cluster.WaitForRepairAsync("vol", Converge, Ct);
            var used = new HashSet<string>(StringComparer.Ordinal);
            foreach (ObjectId o in await ReachableAsync(cluster, "vol"))
                used.UnionWith(await HoldersAsync(cluster, o));
            Assert.Equal(all, used.Order(StringComparer.Ordinal));

            // Membership follows the live node set: a joining node enters default (and no named pool)...
            await cluster.AddNodeAsync(new TestNodeSpec("n4", "z2"), true, Ct);
            await cluster.WaitUntilAsync(async () => (await PoolAsync(cluster, DfsPoolNames.Default)).Nodes.Contains("n4"), Converge,
                "a joined node is a member of the default pool", Ct);
            Assert.Equal(new[] { "n0", "n1", "n2", "n3", "n4" }, Sorted((await PoolAsync(cluster, DfsPoolNames.Default)).Nodes));
            Assert.Equal(new[] { "n3" }, (await PoolAsync(cluster, "fast")).Nodes);

            // ...and a removed node leaves it.
            await admin.DrainNodeAsync("n4", Ct);
            await cluster.WaitUntilAsync(async () => (await admin.ListNodesAsync(Ct)).Single(n => n.Name == "n4").Status == DfsNodeStatus.Drained,
                Converge, "n4 drains", Ct);
            await admin.RemoveNodeAsync("n4", false, Ct);
            await cluster.WaitUntilAsync(async () => !(await PoolAsync(cluster, DfsPoolNames.Default)).Nodes.Contains("n4"), Converge,
                "a removed node leaves the default pool", Ct);
            Assert.Equal(all, Sorted((await PoolAsync(cluster, DfsPoolNames.Default)).Nodes));
            Assert.Equal(all, Sorted(await cluster.Inspect.GetPoolNodesAsync(DfsPoolNames.Default)));
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // POOL-004 CreatePoolValidation
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<string> PoolValidationCases => new()
    {
        "CreateEmpty", "CreateUnknownNode", "CreateRemovedNode", "CreateDuplicateName", "CreateDefaultName", "CreateInvalidName",
        "SetMembersEmpty", "SetMembersUnknownNode", "SetMembersAbsentPool",
        "DeleteBoundToVolume", "DeleteReferencedByPolicy", "DeleteDefault", "DeleteAbsent",
        "VolumeOnAbsentPool", "PolicyOnAbsentPool",
    };

    /// <summary>
    /// Invalid pool administration fails with a typed, non-retryable error and changes nothing (pool table, placement
    /// epoch, volume list): an empty member list, an unknown or removed node and an invalid name are argument errors; a
    /// duplicate name (including <c>default</c>) is <c>AlreadyExists</c>; an absent pool is <c>PoolNotFound</c>; deleting a
    /// pool bound to a volume or named by a policy is <c>PoolInUse</c> (and succeeds once the reference is gone);
    /// <c>default</c> cannot be deleted; binding a volume or policy to an absent pool is rejected.
    /// </summary>
    [Theory]
    [MemberData(nameof(PoolValidationCases))]
    [Catalog("POOL-004", "U", "INV-08", "INV-18")]
    public async Task POOL_004_CreatePoolValidation(string scenario)
    {
        IReadOnlyList<TestNodeSpec> nodes = [Node("n0", "z1", "p1"), Node("n1", "z2", "p1"), Node("n2", "z3", "p1"), Node("n3", "z1")];
        await ClusterTest.RunAsync(Realtime(nodes, 3, new TestVolumeSpec("vol")), async cluster =>
        {
            DfsAdminClient admin = cluster.Admin;
            switch (scenario)
            {
                case "CreateRemovedNode":
                    await admin.DrainNodeAsync("n3", Ct);
                    await cluster.WaitUntilAsync(async () => (await admin.ListNodesAsync(Ct)).Single(n => n.Name == "n3").Status == DfsNodeStatus.Drained,
                        Converge, "n3 drains", Ct);
                    await admin.RemoveNodeAsync("n3", false, Ct);
                    await cluster.WaitUntilAsync(async () => !(await PoolAsync(cluster, DfsPoolNames.Default)).Nodes.Contains("n3"), Converge,
                        "n3 is no longer a live node", Ct);
                    break;
                case "DeleteBoundToVolume":
                    await admin.CreatePoolAsync("p2", ["n0", "n1", "n2"], Ct);
                    await admin.CreateVolumeAsync(new DfsVolumeSpec { Name = "v2", StoragePool = "p2", DefaultPolicy = ThreeZones }, Ct);
                    Assert.Contains("v2", (await PoolAsync(cluster, "p2")).Volumes);
                    break;
                case "DeleteReferencedByPolicy":
                    await admin.CreatePoolAsync("p2", ["n0", "n1", "n2"], Ct);
                    await admin.CreateVolumeAsync(new DfsVolumeSpec { Name = "v2", DefaultPolicy = ThreeZones }, Ct);
                    await admin.DefinePolicyAsync("v2", "tier", ThreeZones with { Pool = "p2" }, Ct);
                    // v2 is bound to default; p2 is referenced only through the named policy.
                    Assert.Equal("p2", (await admin.GetVolumeStatusAsync("v2", Ct)).Info.NamedPolicies["tier"].Pool);
                    Assert.Equal(DfsPoolNames.Default, (await admin.GetVolumeStatusAsync("v2", Ct)).Info.StoragePool);
                    break;
            }

            string poolsBefore = DescribePools(await admin.ListPoolsAsync(Ct));
            ulong epochBefore = await PlacementEpochAsync(cluster);
            string[] volumesBefore = Sorted(await admin.ListVolumesAsync(Ct));

            Exception? ex = await Record.ExceptionAsync(Act);
            bool expected = scenario switch
            {
                "CreateEmpty" or "CreateUnknownNode" or "CreateRemovedNode" or "CreateInvalidName" or "SetMembersEmpty" or "SetMembersUnknownNode"
                    => ex is ArgumentException,
                "CreateDuplicateName" or "CreateDefaultName" => ex is DfsAlreadyExistsException { ErrorCode: DfsErrorCode.AlreadyExists },
                "SetMembersAbsentPool" or "DeleteAbsent" => ex is DfsException { ErrorCode: DfsErrorCode.PoolNotFound },
                "DeleteBoundToVolume" or "DeleteReferencedByPolicy" => ex is DfsException { ErrorCode: DfsErrorCode.PoolInUse },
                "DeleteDefault" => ex is ArgumentException or DfsException { ErrorCode: DfsErrorCode.PoolInUse },
                "VolumeOnAbsentPool" or "PolicyOnAbsentPool" => ex is DfsException { ErrorCode: DfsErrorCode.PoolNotFound or DfsErrorCode.InvalidPolicy },
                _ => false,
            };
            Assert.True(expected, $"{scenario}: expected the documented typed error, got {Describe(ex)}");
            Assert.False(ex is DfsException { IsRetryable: true }, $"{scenario}: a validation failure is not retryable");

            // Nothing changed.
            Assert.Equal(poolsBefore, DescribePools(await admin.ListPoolsAsync(Ct)));
            Assert.Equal(epochBefore, await PlacementEpochAsync(cluster));
            Assert.Equal(volumesBefore, Sorted(await admin.ListVolumesAsync(Ct)));
            Assert.Equal(new[] { "n0", "n1", "n2" }, Sorted(await cluster.Inspect.GetPoolNodesAsync("p1")));
            if (scenario == "PolicyOnAbsentPool")
                Assert.False((await admin.GetVolumeStatusAsync("vol", Ct)).Info.NamedPolicies.ContainsKey("tier2"));

            // Positive controls: once the reference is gone the pool can be deleted; a valid create still works.
            switch (scenario)
            {
                case "DeleteBoundToVolume" or "DeleteReferencedByPolicy":
                    await admin.DeleteVolumeAsync("v2", Ct);
                    await admin.DeletePoolAsync("p2", Ct);
                    Assert.DoesNotContain("p2", (await admin.ListPoolsAsync(Ct)).Select(p => p.Name));
                    break;
                case "CreateEmpty" or "CreateUnknownNode" or "CreateInvalidName":
                    await admin.CreatePoolAsync("p2", ["n3"], Ct);
                    Assert.Equal(new[] { "n3" }, (await PoolAsync(cluster, "p2")).Nodes);
                    break;
            }

            Task Act() => scenario switch
            {
                "CreateEmpty" => admin.CreatePoolAsync("p2", [], Ct),
                "CreateUnknownNode" => admin.CreatePoolAsync("p2", ["n0", "nX"], Ct),
                "CreateRemovedNode" => admin.CreatePoolAsync("p2", ["n3"], Ct),
                "CreateDuplicateName" => admin.CreatePoolAsync("p1", ["n3"], Ct),
                "CreateDefaultName" => admin.CreatePoolAsync(DfsPoolNames.Default, ["n0"], Ct),
                "CreateInvalidName" => admin.CreatePoolAsync("bad name", ["n0"], Ct),
                "SetMembersEmpty" => admin.SetPoolMembersAsync("p1", [], Ct),
                "SetMembersUnknownNode" => admin.SetPoolMembersAsync("p1", ["n0", "nX"], Ct),
                "SetMembersAbsentPool" => admin.SetPoolMembersAsync("nope", ["n0"], Ct),
                "DeleteBoundToVolume" or "DeleteReferencedByPolicy" => admin.DeletePoolAsync("p2", Ct),
                "DeleteDefault" => admin.DeletePoolAsync(DfsPoolNames.Default, Ct),
                "DeleteAbsent" => admin.DeletePoolAsync("nope", Ct),
                "VolumeOnAbsentPool" => admin.CreateVolumeAsync(new DfsVolumeSpec { Name = "v3", StoragePool = "nope", DefaultPolicy = ThreeZones }, Ct),
                "PolicyOnAbsentPool" => admin.DefinePolicyAsync("vol", "tier2", ThreeZones with { Pool = "nope" }, Ct),
                _ => throw new ArgumentOutOfRangeException(nameof(scenario)),
            };
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // POOL-007 PolicySatisfiabilityPerPool
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>(pool, target copies, target zones, satisfiable). The cluster has three zones and four hosts.</summary>
    public static TheoryData<string, int, int, bool> SatisfiabilityCases => new()
    {
        { "ab", 3, 3, false },                   // three zones in a two-zone pool, although the cluster has three
        { "ab", 3, 2, true },
        { "ab", 4, 2, false },                   // the pool has three hosts
        { "co", 3, 2, false },                   // three nodes, but n3 and n4 share a host
        { "co", 2, 2, true },
        { DfsPoolNames.Default, 3, 3, true },
        { DfsPoolNames.Default, 4, 3, true },
        { DfsPoolNames.Default, 5, 3, false },
    };

    /// <summary>
    /// Satisfiability is evaluated against the pool's nodes only (distinct hosts and zones): creating a volume on the pool,
    /// defining a named policy bound to it, defining a pool-less named policy on the pool's volume and replacing that
    /// volume's default policy all fail with <c>InvalidPolicy</c> — leaving the configuration unchanged — exactly when the
    /// pool's hosts/zones cannot meet the targets, and succeed (and are recorded) otherwise.
    /// </summary>
    [Theory]
    [MemberData(nameof(SatisfiabilityCases))]
    [Catalog("POOL-007", "U", "INV-08", "INV-11")]
    public async Task POOL_007_PolicySatisfiabilityPerPool(string pool, int copies, int zones, bool satisfiable)
    {
        IReadOnlyList<TestNodeSpec> nodes =
        [
            NodeOnHost("n0", "z1", "h0", "ab"), NodeOnHost("n1", "z1", "h1", "ab"), NodeOnHost("n2", "z2", "h2", "ab", "co"),
            NodeOnHost("n3", "z3", "hx", "co"), NodeOnHost("n4", "z3", "hx", "co"),
        ];
        TestNodeSpec[] members = nodes.Where(n => n.IsInPool(pool)).ToArray();
        int poolHosts = members.Select(n => n.EffectiveHost).Distinct(StringComparer.Ordinal).Count();
        int poolZones = members.Select(n => n.Region + "/" + n.Zone).Distinct(StringComparer.Ordinal).Count();
        Assert.Equal(satisfiable, poolHosts >= copies && poolZones >= zones);
        Assert.Equal(3, nodes.Select(n => n.Zone).Distinct(StringComparer.Ordinal).Count());
        int ack = Math.Min(2, copies);
        DfsStoragePolicy policy = Policy(copies, zones, ack, Math.Min(ack, Math.Min(2, zones)));
        Assert.Null(policy.GetValidationError());   // internally consistent: any rejection below is about the pool
        string poolVolume = pool switch { "ab" => "v-ab", "co" => "v-co", _ => "v-def" };

        TestClusterOptions options = Realtime(nodes, 3,
            Volume("v-ab", "ab", Policy(3, 2, 2, 2)), Volume("v-co", "co", Policy(2, 2, 2, 2)), Volume("v-def", DfsPoolNames.Default, ThreeZones));
        await ClusterTest.RunAsync(options, async cluster =>
        {
            DfsAdminClient admin = cluster.Admin;

            await ExpectAsync(() => admin.CreateVolumeAsync(new DfsVolumeSpec { Name = "v-new", StoragePool = pool, DefaultPolicy = policy }, Ct),
                "CreateVolume on the pool");
            if (satisfiable)
            {
                DfsVolumeInfo created = (await admin.GetVolumeStatusAsync("v-new", Ct)).Info;
                Assert.Equal(pool, created.StoragePool);
                AssertSameCounts(policy, created.DefaultPolicy, "v-new default policy");
            }
            else
            {
                Assert.DoesNotContain("v-new", await admin.ListVolumesAsync(Ct));
            }

            await ExpectAsync(() => admin.DefinePolicyAsync("v-def", "tier", policy with { Pool = pool }, Ct), "DefinePolicy(Pool = pool) on v-def");
            IReadOnlyDictionary<string, DfsStoragePolicy> defPolicies = (await admin.GetVolumeStatusAsync("v-def", Ct)).Info.NamedPolicies;
            Assert.Equal(satisfiable, defPolicies.ContainsKey("tier"));
            if (satisfiable)
            {
                Assert.Equal(pool, defPolicies["tier"].Pool);
                AssertSameCounts(policy, defPolicies["tier"], "v-def tier");
            }

            await ExpectAsync(() => admin.DefinePolicyAsync(poolVolume, "local", policy, Ct), $"DefinePolicy(no pool) on {poolVolume}");
            Assert.Equal(satisfiable, (await admin.GetVolumeStatusAsync(poolVolume, Ct)).Info.NamedPolicies.ContainsKey("local"));

            DfsStoragePolicy oldDefault = (await admin.GetVolumeStatusAsync(poolVolume, Ct)).Info.DefaultPolicy;
            await ExpectAsync(() => admin.SetDefaultPolicyAsync(poolVolume, policy, false, Ct), $"SetDefaultPolicy on {poolVolume}");
            DfsStoragePolicy newDefault = (await admin.GetVolumeStatusAsync(poolVolume, Ct)).Info.DefaultPolicy;
            AssertSameCounts(satisfiable ? policy : oldDefault, newDefault, $"{poolVolume} default policy");
            Assert.Equal(pool, (await admin.GetVolumeStatusAsync(poolVolume, Ct)).Info.StoragePool);

            async Task ExpectAsync(Func<Task> action, string what)
            {
                Exception? ex = await Record.ExceptionAsync(action);
                if (satisfiable)
                    Assert.True(ex is null, $"{what}: {copies} copies / {zones} zones fit pool '{pool}' ({poolHosts} hosts, {poolZones} zones) but failed: {Describe(ex)}");
                else
                    Assert.True(ex is DfsException { ErrorCode: DfsErrorCode.InvalidPolicy },
                        $"{what}: {copies} copies / {zones} zones exceed pool '{pool}' ({poolHosts} hosts, {poolZones} zones); expected InvalidPolicy, got {Describe(ex)}");
            }
        }, Ct);

        static void AssertSameCounts(DfsStoragePolicy expected, DfsStoragePolicy actual, string what)
        {
            Assert.True(expected.TargetCopies == actual.TargetCopies && expected.TargetDistinctZones == actual.TargetDistinctZones
                && expected.DurableCopiesBeforeAck == actual.DurableCopiesBeforeAck && expected.DurableZonesBeforeAck == actual.DurableZonesBeforeAck,
                $"{what}: expected {expected}, got {actual}");
        }
    }

    // ------------------------------------------------------------------------------------------------------------
    // POOL-013 PerPoolStatusReporting
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Pool listing and status report each pool's members, bound volumes (a volume that only names the pool in a policy may
    /// be listed as well), total member capacity and no draining nodes. Usage grows by at least the replicated bytes written
    /// through a pool and stays within encoding overhead of the members' object inventories, while the other pools' usage
    /// is unchanged (usage attribution per pool.md). Replica deficits match the inventories: with one member of pool a
    /// crashed and one object deleted from the other two, pool a (and the volume's own health) reports every object
    /// under-replicated and zone-deficient and that object missing, pool b reports nothing, the default pool (which also
    /// contains the crashed node) reports exactly what its inventories show, and everything returns to zero after recovery
    /// and repair.
    /// </summary>
    [Fact]
    [Catalog("POOL-013", "U", "INV-11", "INV-16")]
    public async Task POOL_013_PerPoolStatusReporting()
    {
        IReadOnlyList<TestNodeSpec> nodes = TwoPools();
        string[] poolA = Members(nodes, PoolA), poolB = Members(nodes, PoolB), all = Members(nodes, DfsPoolNames.Default);
        TestVolumeSpec vd = new("vd")
        {
            DefaultPolicy = ThreeZones,
            NamedPolicies = new Dictionary<string, DfsStoragePolicy>(StringComparer.Ordinal) { ["onb"] = ThreeZones with { Pool = PoolB } },
        };
        await ClusterTest.RunAsync(Realtime(nodes, 3, Volume(VolA, PoolA), Volume(VolB, PoolB), vd), async cluster =>
        {
            DfsAdminClient admin = cluster.Admin;
            IReadOnlyList<DfsPoolInfo> pools = await admin.ListPoolsAsync(Ct);
            Assert.Equal(new[] { PoolA, PoolB, DfsPoolNames.Default }, Sorted(pools.Select(p => p.Name)));
            IReadOnlyDictionary<string, long> capacity = (await admin.ListNodesAsync(Ct)).ToDictionary(n => n.Name, n => n.CapacityBytes, StringComparer.Ordinal);
            // Volumes: the bound volumes are listed; a volume that names the pool only through a policy (vd for pool b) may
            // be listed too (the addendum leaves this open), never a volume without any reference to the pool.
            foreach ((string name, string[] members, string[] bound, string[] referencing) in new[]
                     {
                         (PoolA, poolA, new[] { VolA }, Array.Empty<string>()), (PoolB, poolB, new[] { VolB }, new[] { "vd" }),
                         (DfsPoolNames.Default, all, new[] { "vd" }, Array.Empty<string>()),
                     })
            {
                DfsPoolInfo info = pools.Single(p => p.Name == name);
                Assert.Equal(members, Sorted(info.Nodes));
                Assert.Equal(info.Volumes.Count, info.Volumes.Distinct(StringComparer.Ordinal).Count());
                Assert.True(bound.All(info.Volumes.Contains) && info.Volumes.All(v => bound.Contains(v) || referencing.Contains(v)),
                    $"pool {name} lists volumes {Show(info.Volumes)}; expected {Show(bound)} (plus possibly {Show(referencing)})");
                Assert.Equal(members.Sum(m => capacity[m]), info.CapacityBytes);
                DfsPoolStatus status = await admin.GetPoolStatusAsync(name, Ct);
                Assert.Equal(DescribePools([info]), DescribePools([status.Info]));
                Assert.Equal(info.CapacityBytes, status.Info.CapacityBytes);
                Assert.Empty(status.DrainingNodes);
            }

            await AssertEx.ThrowsDfsAsync<DfsException>(() => admin.GetPoolStatusAsync("nope", Ct), DfsErrorCode.PoolNotFound);

            // Usage: writes through pool a raise only pool a's usage, by the replicated bytes (bounded encoding overhead).
            await using DfsClient client = await cluster.ConnectAsync(null, Ct);
            DfsVolume va = client.GetVolume(VolA);
            DfsVolume vdVolume = client.GetVolume("vd");
            Dictionary<string, long> used0 = await UsedAsync();
            long written = 0;
            for (int i = 0; i < 6; i++)
            {
                byte[] data = TestData.Bytes(13_000 + (ulong)i, 3 * Chunk);
                await va.Files.WriteAllBytesAsync($"/f{i}", data, Ct);
                written += data.Length;
            }

            await cluster.WaitForRepairAsync(VolA, Converge, Ct);
            await cluster.WaitUntilAsync(async () => (await UsedAsync())[PoolA] >= used0[PoolA] + 3 * written, Converge,
                "pool a's usage reflects three copies of the bytes written to va", Ct);
            Dictionary<string, long> used1 = await UsedAsync();
            (long inventoryBytes, int records) = await InventoryBytesAsync(poolA, VolA);
            Assert.True(inventoryBytes >= 3 * written, $"pool a's inventories hold {inventoryBytes} bytes for {3 * written} replicated bytes");
            Assert.InRange(used1[PoolA], inventoryBytes, inventoryBytes + inventoryBytes / 16 + records * 4096L);
            Assert.Equal(used0[PoolB], used1[PoolB]);
            Assert.Equal(used0[DfsPoolNames.Default], used1[DfsPoolNames.Default]);

            // vd: a file on its own pool (default) and one through a policy naming pool b.
            byte[] cold = TestData.Bytes(13_100, 3 * Chunk), hot = TestData.Bytes(13_101, 3 * Chunk);
            await vdVolume.Files.WriteAllBytesAsync("/cold", cold, Ct);
            await vdVolume.Files.WriteAllBytesAsync("/hot", hot, new DfsMutationOptions { StoragePolicy = "onb" }, Ct);
            await cluster.WaitForRepairAsync("vd", Converge, Ct);
            await cluster.WaitUntilAsync(async () =>
            {
                Dictionary<string, long> u = await UsedAsync();
                return u[PoolB] >= used1[PoolB] + 3L * hot.Length && u[DfsPoolNames.Default] >= used1[DfsPoolNames.Default] + 3L * cold.Length;
            }, Converge, "usage of pool b and default reflects vd's objects", Ct);
            Dictionary<string, long> used2 = await UsedAsync();
            Assert.Equal(used1[PoolA], used2[PoolA]);
            Assert.InRange(used2[PoolB] - used1[PoolB], 3L * hot.Length, 3L * hot.Length * 17 / 16 + 3 * 4096L * 8);
            Assert.InRange(used2[DfsPoolNames.Default] - used1[DfsPoolNames.Default], 3L * cold.Length, 3L * cold.Length * 17 / 16 + 3 * 4096L * 8);

            // Replica deficits match the inventories.
            List<ObjectId> hotObjects = [.. await ObjectsOfAsync(cluster, "vd", "/hot")];
            List<ObjectId> coldObjects = [.. await ObjectsOfAsync(cluster, "vd", "/cold")];
            IReadOnlySet<ObjectId> reachableA = await ReachableAsync(cluster, VolA);
            ObjectId victim = (await ObjectsOfAsync(cluster, VolA, "/f0"))[0];
            await cluster.Node("n0").CrashAsync();
            await cluster.Node("n1").Disk.DeleteObjectAsync(victim);
            await cluster.Node("n2").Disk.DeleteObjectAsync(victim);

            DfsStoragePolicy policy = ThreeZones;
            (long Objects, long Under, long ZoneDeficit, long Missing) expectedA = default;
            await cluster.WaitUntilAsync(async () =>
            {
                expectedA = await ExpectedHealthAsync(reachableA, poolA, policy);
                DfsReplicaHealth reported = (await admin.GetPoolStatusAsync(PoolA, Ct)).Replicas;
                return (reported.Objects, reported.UnderReplicatedObjects, reported.ZoneDeficitObjects, reported.MissingObjects) == expectedA;
            }, Converge, "pool a's reported replica deficits match the inventories", Ct);
            Assert.Equal(((long)reachableA.Count, (long)reachableA.Count, (long)reachableA.Count, 1L), expectedA);
            await cluster.WaitUntilAsync(async () =>
            {
                DfsReplicaHealth v = await admin.GetReplicaHealthAsync(VolA, Ct);
                return (v.Objects, v.UnderReplicatedObjects, v.ZoneDeficitObjects, v.MissingObjects) == expectedA;
            }, Converge, "va's volume replica health agrees with its pool's", Ct);

            // Pool b (no member down) reports nothing.
            IReadOnlySet<ObjectId> reachableB = await ReachableAsync(cluster, VolB);
            DfsReplicaHealth healthB = (await admin.GetPoolStatusAsync(PoolB, Ct)).Replicas;
            Assert.Equal((long)(reachableB.Count + hotObjects.Count), healthB.Objects);
            Assert.Equal((0L, 0L, 0L, 0L), (healthB.UnderReplicatedObjects, healthB.ZoneDeficitObjects, healthB.MissingObjects, healthB.PlacementViolations));

            // n0 is also a member of default: vd's /cold objects with a copy on n0 are short there until repair re-creates
            // the copy on n3 (the other z1 node), so default's report is judged against its own inventories, not as zero.
            (long Objects, long Under, long ZoneDeficit, long Missing) expectedDefault = default;
            await cluster.WaitUntilAsync(async () =>
            {
                expectedDefault = await ExpectedHealthAsync(coldObjects.ToHashSet(), all, policy);
                DfsReplicaHealth reported = (await admin.GetPoolStatusAsync(DfsPoolNames.Default, Ct)).Replicas;
                return (reported.Objects, reported.UnderReplicatedObjects, reported.ZoneDeficitObjects, reported.MissingObjects) == expectedDefault;
            }, Converge, "default's reported replica deficits match the inventories", Ct);
            Assert.Equal((long)coldObjects.Count, expectedDefault.Objects);
            Assert.Equal(0L, expectedDefault.Missing);

            // Recovery: n0 still holds the victim; repair restores every copy and the deficits return to zero.
            await cluster.Node("n0").RestartAsync();
            await cluster.WaitForStableAsync(ct: Ct);
            await cluster.WaitForRepairAsync(VolA, Converge, Ct);
            await cluster.WaitUntilAsync(async () =>
            {
                DfsReplicaHealth r = (await admin.GetPoolStatusAsync(PoolA, Ct)).Replicas;
                return r.UnderReplicatedObjects == 0 && r.ZoneDeficitObjects == 0 && r.MissingObjects == 0;
            }, Converge, "pool a reports no deficit after repair", Ct);
            Assert.Equal(((long)reachableA.Count, 0L, 0L, 0L), await ExpectedHealthAsync(reachableA, poolA, policy));
            Assert.Equal(poolA, Sorted(await HoldersAsync(cluster, victim)));
            Assert.Empty((await admin.GetPoolStatusAsync(PoolA, Ct)).DrainingNodes);
            await cluster.WaitForRepairAsync("vd", Converge, Ct);
            await cluster.WaitUntilAsync(async () =>
            {
                DfsReplicaHealth r = (await admin.GetPoolStatusAsync(DfsPoolNames.Default, Ct)).Replicas;
                return (r.Objects, r.UnderReplicatedObjects, r.ZoneDeficitObjects, r.MissingObjects) == ((long)coldObjects.Count, 0L, 0L, 0L);
            }, Converge, "the default pool reports no deficit after repair", Ct);

            async Task<Dictionary<string, long>> UsedAsync() =>
                (await admin.ListPoolsAsync(Ct)).ToDictionary(p => p.Name, p => p.UsedBytes, StringComparer.Ordinal);

            async Task<(long Bytes, int Records)> InventoryBytesAsync(IEnumerable<string> members, string volume)
            {
                long bytes = 0;
                int count = 0;
                foreach (string n in members)
                {
                    foreach (ObjectRecordInfo record in await cluster.Node(n).Inspect.ListObjectsAsync(volume))
                    {
                        bytes += record.Length;
                        count++;
                    }
                }

                return (bytes, count);
            }

            async Task<(long Objects, long Under, long ZoneDeficit, long Missing)> ExpectedHealthAsync(IReadOnlySet<ObjectId> objects, string[] members, DfsStoragePolicy p)
            {
                IReadOnlyDictionary<string, DfsTopologyLabels> labels = await LabelsAsync(cluster);
                long under = 0, zoneDeficit = 0, missing = 0;
                foreach (ObjectId o in objects)
                {
                    string[] holders = (await HoldersAsync(cluster, o)).Where(h => members.Contains(h)).ToArray();
                    (int c, int z) = Domains(holders, labels);
                    under += c < p.TargetCopies ? 1 : 0;
                    zoneDeficit += z < p.TargetDistinctZones ? 1 : 0;
                    missing += holders.Length == 0 ? 1 : 0;
                }

                return (objects.Count, under, zoneDeficit, missing);
            }
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // POOL-017 PoolAdminRequiresClusterAdmin
    // ------------------------------------------------------------------------------------------------------------

    private static readonly string[] DeniedWords = ["denied", "unauthorized", "refused", "rejected", "forbidden", "fail"];

    public static TheoryData<string> AdminOperations => new()
    {
        "CreatePool", "SetPoolMembers", "DeletePool", "ListPools", "GetPoolStatus",
        "CreateGroup", "DeleteGroup", "AddGroupMember", "RemoveGroupMember", "ListGroups",
    };

    /// <summary>
    /// A principal holding every volume right except ClusterAdmin is denied each pool and group administration call
    /// (<see cref="UnauthorizedAccessException"/>, <c>AccessDenied</c>, no data returned); pools, groups and the placement
    /// epoch are unchanged; the audit log records the denial with the principal, the object kind, the target and the
    /// outcome, never the principal's secret. A second ClusterAdmin principal performs the same call successfully, and
    /// successful changes are audited too.
    /// </summary>
    [Theory]
    [MemberData(nameof(AdminOperations))]
    [Catalog("POOL-017", "U", "INV-12")]
    public async Task POOL_017_PoolAdminRequiresClusterAdmin(string operation)
    {
        IReadOnlyList<TestNodeSpec> nodes = [Node("n0", "z1", "p1"), Node("n1", "z2", "p1"), Node("n2", "z3", "p1"), Node("n3", "z1")];
        string? target = operation switch
        {
            "CreatePool" => "p-new",
            "SetPoolMembers" or "DeletePool" or "GetPoolStatus" => "p1",
            "CreateGroup" => "g-new",
            "DeleteGroup" or "AddGroupMember" or "RemoveGroupMember" => "g1",
            _ => null,
        };
        string kind = operation.Contains("Group", StringComparison.Ordinal) ? "group" : "pool";
        bool mutates = operation is not ("ListPools" or "GetPoolStatus" or "ListGroups");
        await ClusterTest.RunAsync(Realtime(nodes, 3, new TestVolumeSpec("vol")), async cluster =>
        {
            DfsAdminClient admin = cluster.Admin;
            var grants = new Dictionary<string, DfsPermissions>(StringComparer.Ordinal) { ["vol"] = DfsPermissions.All & ~DfsPermissions.ClusterAdmin };
            await cluster.CreatePrincipalAsync("carol", grants);
            await cluster.CreateGroupAsync("g1", "carol");
            TestPrincipal bob = await cluster.CreatePrincipalAsync("bob", grants);
            TestPrincipal root2 = await cluster.CreatePrincipalAsync("root2", grants, clusterAdmin: true);

            string poolsBefore = DescribePools(await admin.ListPoolsAsync(Ct));
            string groupsBefore = DescribeGroups(await admin.ListGroupsAsync(Ct));
            ulong epochBefore = await PlacementEpochAsync(cluster);

            object? result = null;
            await using (DfsClient bobClient = await cluster.ConnectAsync(new TestClientOptions { Name = "bob", Credentials = bob.Credentials }, Ct))
            {
                Exception? denied = await Record.ExceptionAsync(() => RunAsync(bobClient.Admin));
                Assert.True(denied is UnauthorizedAccessException, $"{operation} by a non-admin principal: expected UnauthorizedAccessException, got {Describe(denied)}");
                Assert.Equal(DfsErrorCode.AccessDenied, DfsErrors.ToCode(denied!));
                Assert.Null(result);
            }

            Assert.Equal(poolsBefore, DescribePools(await admin.ListPoolsAsync(Ct)));
            Assert.Equal(groupsBefore, DescribeGroups(await admin.ListGroupsAsync(Ct)));
            Assert.Equal(epochBefore, await PlacementEpochAsync(cluster));

            // The denial is audited with the principal, the kind of object (pool/group), the target and a denial outcome
            // (same outcome vocabulary as the SEC suite's audit checks).
            IReadOnlyList<string> audit = await admin.GetAuditLogAsync(10_000, Ct);
            Assert.True(audit.Any(line => line.Contains("bob", StringComparison.Ordinal)
                                          && line.Contains(kind, StringComparison.OrdinalIgnoreCase)
                                          && (target is null || line.Contains(target, StringComparison.Ordinal))
                                          && DeniedWords.Any(w => line.Contains(w, StringComparison.OrdinalIgnoreCase))),
                $"no audit entry records {operation} denied to bob (target {target ?? "-"}); last entries:{Environment.NewLine}{string.Join(Environment.NewLine, audit.TakeLast(20))}");
            Assert.DoesNotContain(audit, line => line.Contains(bob.Credentials.Secret, StringComparison.Ordinal));

            // Control: a ClusterAdmin principal may perform the same call.
            await using (DfsClient rootClient = await cluster.ConnectAsync(new TestClientOptions { Name = "root2", Credentials = root2.Credentials }, Ct))
                await RunAsync(rootClient.Admin);
            switch (operation)
            {
                case "CreatePool":
                    Assert.Equal(new[] { "n3" }, (await PoolAsync(cluster, "p-new")).Nodes);
                    break;
                case "SetPoolMembers":
                    Assert.Equal(new[] { "n0", "n1", "n2", "n3" }, Sorted((await PoolAsync(cluster, "p1")).Nodes));
                    break;
                case "DeletePool":
                    Assert.DoesNotContain("p1", (await admin.ListPoolsAsync(Ct)).Select(p => p.Name));
                    break;
                case "ListPools":
                    Assert.Contains(Assert.IsAssignableFrom<IReadOnlyList<DfsPoolInfo>>(result), p => p.Name == "p1");
                    break;
                case "GetPoolStatus":
                    Assert.Equal(new[] { "n0", "n1", "n2" }, Sorted(Assert.IsType<DfsPoolStatus>(result).Info.Nodes));
                    break;
                case "CreateGroup":
                    Assert.Contains(await admin.ListGroupsAsync(Ct), g => g.Name == "g-new");
                    break;
                case "DeleteGroup":
                    Assert.DoesNotContain(await admin.ListGroupsAsync(Ct), g => g.Name == "g1");
                    break;
                case "AddGroupMember":
                    Assert.Contains("bob", (await admin.ListGroupsAsync(Ct)).Single(g => g.Name == "g1").Members);
                    break;
                case "RemoveGroupMember":
                    Assert.DoesNotContain("carol", (await admin.ListGroupsAsync(Ct)).Single(g => g.Name == "g1").Members);
                    break;
                case "ListGroups":
                    Assert.Contains(Assert.IsAssignableFrom<IReadOnlyList<DfsGroupInfo>>(result), g => g.Name == "g1");
                    break;
            }

            // Successful administration changes are audited too (successful reads need not be).
            if (mutates)
            {
                IReadOnlyList<string> audit2 = await admin.GetAuditLogAsync(10_000, Ct);
                Assert.True(audit2.Any(line => line.Contains("root2", StringComparison.Ordinal)
                                               && line.Contains(kind, StringComparison.OrdinalIgnoreCase)
                                               && line.Contains(target!, StringComparison.Ordinal)),
                    $"no audit entry records root2's {operation} of {target}; last entries:{Environment.NewLine}{string.Join(Environment.NewLine, audit2.TakeLast(20))}");
            }

            async Task RunAsync(DfsAdminClient a)
            {
                switch (operation)
                {
                    case "CreatePool":
                        await a.CreatePoolAsync("p-new", ["n3"], Ct);
                        break;
                    case "SetPoolMembers":
                        await a.SetPoolMembersAsync("p1", ["n0", "n1", "n2", "n3"], Ct);
                        break;
                    case "DeletePool":
                        await a.DeletePoolAsync("p1", Ct);
                        break;
                    case "ListPools":
                        result = await a.ListPoolsAsync(Ct);
                        break;
                    case "GetPoolStatus":
                        result = await a.GetPoolStatusAsync("p1", Ct);
                        break;
                    case "CreateGroup":
                        await a.CreateGroupAsync("g-new", Ct);
                        break;
                    case "DeleteGroup":
                        await a.DeleteGroupAsync("g1", Ct);
                        break;
                    case "AddGroupMember":
                        await a.AddGroupMemberAsync("g1", "bob", Ct);
                        break;
                    case "RemoveGroupMember":
                        await a.RemoveGroupMemberAsync("g1", "carol", Ct);
                        break;
                    case "ListGroups":
                        result = await a.ListGroupsAsync(Ct);
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(operation));
                }
            }
        }, Ct);
    }
}
