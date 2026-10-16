using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;
using static Tedd.Tdfs.Tests.Simulation.Ns.NsHelpers;

namespace Tedd.Tdfs.Tests.Simulation.Ns;

/// <summary>
/// NS suite, deterministic simulation level: namespace atomicity, rename/delete/open races, pinned enumeration,
/// deletion with open pins and directory-mutation retry identity (test plan §7.8; FSD §4 NS-05–NS-07, §§5, 8, 13).
/// </summary>
public sealed class NsSimulationTests
{
    // ------------------------------------------------------------------------------------------------------------
    // NS-002 ConcurrentDirectoryCreate
    // ------------------------------------------------------------------------------------------------------------

    [Theory]
    [InlineData(3, 201UL)]
    [InlineData(3, 202UL)]
    [InlineData(5, 203UL)]
    [InlineData(5, 204UL)]
    [InlineData(7, 205UL)]
    [Catalog("NS-002", "S", "INV-02", "INV-07")]
    public async Task NS_002_ConcurrentDirectoryCreate(int nodeCount, ulong seed)
    {
        const int racers = 4;
        await ClusterTest.RunAsync(Sim(seed, nodeCount), async cluster =>
        {
            var clients = new List<DfsClient>();
            for (int i = 0; i < racers; i++)
                clients.Add(await cluster.ConnectAsync(new TestClientOptions { Name = $"c{i}", SeedNodes = [cluster[i % nodeCount].Name] }, Ct));
            try
            {
                int inodesBefore = await InodeCountAsync(cluster);
                var commonIds = new List<InodeId>();
                var tIds = new List<InodeId>();

                async Task RaceAsync(int i)
                {
                    DfsVolume v = clients[i].GetVolume(Vol);
                    string[] plan = ["/r/s/t", $"/r/s/t/common/v{i}", $"/r/s/t/c{i}/u", "/r/s/t/common"];
                    for (int step = 0; step < plan.Length; step++)
                    {
                        string p = plan[(step + i) % plan.Length];
                        DfsDirectoryInfo info = await v.Directories.CreateDirectoryAsync(p, Ct);
                        Assert.Equal(p, info.Path);
                        if (p == "/r/s/t/common")
                            commonIds.Add(info.InodeId);
                        if (p == "/r/s/t")
                            tIds.Add(info.InodeId);
                    }
                }

                await Task.WhenAll(Enumerable.Range(0, racers).Select(RaceAsync));
                Assert.Single(commonIds.Distinct());
                Assert.Single(tIds.Distinct());

                var expected = new SortedSet<string>(StringComparer.Ordinal) { "/r", "/r/s", "/r/s/t", "/r/s/t/common" };
                for (int i = 0; i < racers; i++)
                {
                    expected.Add($"/r/s/t/common/v{i}");
                    expected.Add($"/r/s/t/c{i}");
                    expected.Add($"/r/s/t/c{i}/u");
                }

                // One valid tree: every directory exactly once, no duplicate names, unique inodes, nothing dangling.
                DfsVolume admin = clients[0].GetVolume(Vol);
                var found = new SortedSet<string>(StringComparer.Ordinal) { "/r" };
                var inodes = new HashSet<InodeId> { (await admin.Directories.GetInfoAsync("/r", Ct)).InodeId };
                async Task WalkAsync(string dir)
                {
                    List<DfsEntryInfo> entries = await ToListAsync(admin.Directories.EnumerateAsync(dir, null, Ct));
                    Assert.Equal(entries.Count, entries.Select(e => e.Name).Distinct(StringComparer.Ordinal).Count());
                    foreach (DfsEntryInfo e in entries)
                    {
                        Assert.Equal(DfsEntryKind.Directory, e.Kind);
                        Assert.True(found.Add(e.Path), $"{e.Path} listed twice");
                        Assert.True(inodes.Add(e.InodeId), $"inode {e.InodeId} reachable through two names");
                        Assert.Equal(e.InodeId, (await admin.Directories.GetInfoAsync(e.Path, Ct)).InodeId);
                        await WalkAsync(e.Path);
                    }
                }

                await WalkAsync("/r");
                Assert.Equal(expected, found);
                Assert.Equal(inodesBefore + expected.Count, await InodeCountAsync(cluster));
                await cluster.WaitForReplicationAsync(Vol, Ct);
                await cluster.Inspect.AssertReplicasConvergedAsync(Vol);

                // A file and a directory racing for the same name: exactly one wins, consistently.
                Task<Exception?> fileRace = Record.ExceptionAsync(() => clients[0].GetVolume(Vol).Files.WriteAllBytesAsync("/r/s/t/x", TestData.Bytes(seed, 10), Ct)).AsTask();
                Task<Exception?> dirRace = Record.ExceptionAsync(() => clients[1].GetVolume(Vol).Directories.CreateDirectoryAsync("/r/s/t/x/y", Ct)).AsTask();
                Exception?[] race = await Task.WhenAll(fileRace, dirRace);
                Assert.True(race[0] is null ^ race[1] is null, $"exactly one of file/directory creation must win: {race[0]} / {race[1]}");
                if (race[0] is null)
                {
                    Assert.True(race[1] is DfsException { ErrorCode: DfsErrorCode.AlreadyExists or DfsErrorCode.NotADirectory }, $"directory loser: {race[1]}");
                    Assert.True(await admin.Files.ExistsAsync("/r/s/t/x", Ct));
                    Assert.False(await admin.Directories.ExistsAsync("/r/s/t/x", Ct));
                }
                else
                {
                    Assert.True(race[0] is DfsException { ErrorCode: DfsErrorCode.IsADirectory or DfsErrorCode.AlreadyExists }, $"file loser: {race[0]}");
                    Assert.True(await admin.Directories.ExistsAsync("/r/s/t/x/y", Ct));
                    Assert.False(await admin.Files.ExistsAsync("/r/s/t/x", Ct));
                }
            }
            finally
            {
                foreach (DfsClient c in clients)
                    await c.DisposeAsync();
            }
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // NS-004 RecursiveDeleteDetachesAtomically
    // ------------------------------------------------------------------------------------------------------------

    [Theory]
    [InlineData(401UL)]
    [InlineData(402UL)]
    [InlineData(403UL)]
    [Catalog("NS-004", "S", "INV-02", "INV-03", "INV-07", "INV-09")]
    public async Task NS_004_RecursiveDeleteDetachesAtomically(ulong seed)
    {
        await ClusterTest.RunAsync(Sim(seed), async cluster =>
        {
            await using DfsClient deleterClient = await cluster.ConnectAsync(Named("deleter"), Ct);
            DfsVolume deleter = deleterClient.GetVolume(Vol);
            var dirs = new List<string> { "/big" };
            var files = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            for (int d = 0; d < 4; d++)
            {
                string dir = $"/big/d{d}";
                await deleter.Directories.CreateDirectoryAsync(dir, Ct);
                dirs.Add(dir);
                for (int f = 0; f < 6; f++)
                {
                    byte[] bytes = TestData.Bytes(seed + (ulong)(d * 10 + f), 5000);
                    await deleter.Files.WriteAllBytesAsync($"{dir}/f{f}.bin", bytes, Ct);
                    files[$"{dir}/f{f}.bin"] = bytes;
                }
            }

            await deleter.Directories.CreateDirectoryAsync("/big/d0/deep/deeper", Ct);
            dirs.Add("/big/d0/deep");
            dirs.Add("/big/d0/deep/deeper");
            files["/big/d0/deep/deeper/leaf.bin"] = TestData.Bytes(seed + 999, 2 * Chunk);
            await deleter.Files.WriteAllBytesAsync("/big/d0/deep/deeper/leaf.bin", files["/big/d0/deep/deeper/leaf.bin"], Ct);
            await deleter.Files.WriteAllBytesAsync("/keep.bin", TestData.Bytes(seed + 5, 100), Ct);

            var listings = new Dictionary<string, string[]>(StringComparer.Ordinal);
            foreach (string dir in dirs)
                listings[dir] = (await ToListAsync(deleter.Directories.EnumerateAsync(dir, null, Ct))).Select(e => e.Name).Order(StringComparer.Ordinal).ToArray();
            var objects = new HashSet<ObjectId>();
            foreach (string f in files.Keys)
                objects.UnionWith(await ObjectsOfAsync(cluster, f));
            Assert.NotEmpty(objects);
            int inodesBefore = await InodeCountAsync(cluster);
            int subtreeInodes = dirs.Count + files.Count;

            var observerClients = new List<DfsClient>();
            for (int i = 0; i < 2; i++)
                observerClients.Add(await cluster.ConnectAsync(Named($"obs{i}"), Ct));
            var deleteDone = new TaskCompletionSource();
            string[] filePaths = files.Keys.Order(StringComparer.Ordinal).ToArray();

            async Task ObserveAsync(int index)
            {
                DfsVolume v = observerClients[index].GetVolume(Vol);
                var random = new SeededRandom(seed * 31 + (ulong)index);
                int afterDelete = 0;
                for (int i = 0; i < 300 && afterDelete < 4; i++)
                {
                    if (deleteDone.Task.IsCompleted)
                        afterDelete++;
                    string process = $"obs{index}";
                    switch (random.Next(4))
                    {
                        case 0:
                        {
                            string dir = dirs[random.Next(dirs.Count)];
                            await ProbeAsync(cluster, process, "subtree", Present, Absent, () => v.Directories.ExistsAsync(dir, Ct));
                            break;
                        }

                        case 1:
                        {
                            string file = filePaths[random.Next(filePaths.Length)];
                            await ProbeAsync(cluster, process, "subtree", Present, Absent, () => v.Files.ExistsAsync(file, Ct));
                            break;
                        }

                        case 2:
                        {
                            string dir = dirs[random.Next(dirs.Count)];
                            await ProbeValueAsync(cluster, process, "subtree", async () =>
                            {
                                try
                                {
                                    DfsDirectoryPage page = await v.Directories.ListPageAsync(dir, new DfsEnumerationOptions { PageSize = 10_000 }, null, Ct);
                                    Assert.Null(page.ContinuationToken);
                                    Assert.Equal(listings[dir], page.Entries.Select(e => e.Name).Order(StringComparer.Ordinal).ToArray());
                                    return Present;
                                }
                                catch (DirectoryNotFoundException)
                                {
                                    return Absent;
                                }
                            });
                            break;
                        }

                        default:
                        {
                            string file = filePaths[random.Next(filePaths.Length)];
                            await ProbeValueAsync(cluster, process, "subtree", async () =>
                            {
                                try
                                {
                                    AssertBytes(files[file], await v.Files.ReadAllBytesAsync(file, Ct), $"{file} while the subtree is being deleted");
                                    return Present;
                                }
                                catch (Exception ex) when (ex is FileNotFoundException or DirectoryNotFoundException)
                                {
                                    return Absent;
                                }
                            });
                            break;
                        }
                    }
                }
            }

            Task[] observers = Enumerable.Range(0, observerClients.Count).Select(ObserveAsync).ToArray();
            await cluster.DelayAsync(TimeSpan.FromMilliseconds(30), Ct);
            CommitToken deleted = await WriteAsync(cluster, "deleter", "subtree", Absent, async () =>
            {
                for (int attempt = 0; ; attempt++)
                {
                    try
                    {
                        return await deleter.Directories.DeleteAsync("/big", true, Ct);
                    }
                    catch (DfsSharingViolationException) when (attempt < 1_000) // outlasts the bounded observers
                    {
                        // An observer's read helper may briefly hold a handle without Delete sharing; a refused
                        // recursive delete detaches nothing and is retryable.
                        await cluster.DelayAsync(TimeSpan.FromMilliseconds(5), Ct);
                    }
                }
            });
            deleteDone.SetResult();
            await Task.WhenAll(observers);
            foreach (DfsClient c in observerClients)
                await c.DisposeAsync();

            // No observer ever saw a partially detached subtree.
            LinearizabilityChecker.Check(cluster.History.Operations, new RegisterModel(Present)).AssertValid();
            Assert.False(deleted.IsNone);
            Assert.False(await deleter.Directories.ExistsAsync("/big", Ct));
            Assert.DoesNotContain(await ToListAsync(deleter.Directories.EnumerateAsync("/", null, Ct)), e => e.Name == "big");
            Assert.True(await deleter.Files.ExistsAsync("/keep.bin", Ct));

            // Physical reclamation may continue afterwards but does complete.
            await cluster.WaitUntilAsync(async () => await InodeCountAsync(cluster) == inodesBefore - subtreeInodes, RecoveryWait,
                "detached subtree inodes are eventually reclaimed", Ct);
            await WaitForObjectsReclaimedAsync(cluster, objects, "objects of the deleted subtree are eventually garbage collected");

            // The name can be reused; it is a new incarnation.
            DfsDirectoryInfo recreated = await deleter.Directories.CreateDirectoryAsync("/big", Ct);
            Assert.Equal(0, recreated.EntryCount);
            Assert.False(await deleter.Files.ExistsAsync("/big/d0/f0.bin", Ct));
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // NS-005 RenameFileAtomic
    // ------------------------------------------------------------------------------------------------------------

    [Theory]
    [InlineData("same-directory", 501UL)]
    [InlineData("same-directory", 502UL)]
    [InlineData("cross-directory", 503UL)]
    [InlineData("cross-directory", 504UL)]
    [InlineData("overwrite-same-directory", 505UL)]
    [InlineData("overwrite-cross-directory", 506UL)]
    [Catalog("NS-005", "S", "INV-02", "INV-03", "INV-07")]
    public async Task NS_005_RenameFileAtomic(string variant, ulong seed)
    {
        bool overwrite = variant.StartsWith("overwrite", StringComparison.Ordinal);
        bool sameDir = variant.EndsWith("same-directory", StringComparison.Ordinal);
        await ClusterTest.RunAsync(Sim(seed), async cluster =>
        {
            await using DfsClient renamerClient = await cluster.ConnectAsync(Named("renamer"), Ct);
            DfsVolume renamer = renamerClient.GetVolume(Vol);
            const string srcDir = "/a";
            string dstDir = sameDir ? "/a" : "/b";
            string src = srcDir + "/file.bin";
            string dst = dstDir + "/renamed.bin";
            await renamer.Directories.CreateDirectoryAsync(srcDir, Ct);
            await renamer.Directories.CreateDirectoryAsync(dstDir, Ct);
            byte[] xBytes = TestData.Bytes(seed, Chunk + 3);
            byte[] yBytes = TestData.Bytes(seed + 1, 777);
            await renamer.Files.WriteAllBytesAsync(src, xBytes, Ct);
            InodeId x = (await renamer.Files.GetInfoAsync(src, Ct)).InodeId;
            InodeId? y = null;
            if (overwrite)
            {
                await renamer.Files.WriteAllBytesAsync(dst, yBytes, Ct);
                y = (await renamer.Files.GetInfoAsync(dst, Ct)).InodeId;
            }

            int inodesBefore = await InodeCountAsync(cluster);
            byte[] s = State("S");
            byte[] d = State("D");
            var done = new TaskCompletionSource();
            var observerClients = new List<DfsClient>();
            for (int i = 0; i < 3; i++)
                observerClients.Add(await cluster.ConnectAsync(Named($"obs{i}"), Ct));

            async Task<byte[]> DestinationStateAsync(DfsFileClient files)
            {
                if (!overwrite)
                    return await files.ExistsAsync(dst, Ct) ? d : s;
                DfsFileInfo info = await files.GetInfoAsync(dst, Ct); // the destination name always exists
                Assert.True(info.InodeId == x || info.InodeId == y, $"destination names an unexpected inode {info.InodeId}");
                return info.InodeId == x ? d : s;
            }

            async Task ObserveAsync(int index)
            {
                DfsVolume v = observerClients[index].GetVolume(Vol);
                string process = $"obs{index}";
                var random = new SeededRandom(seed * 7 + (ulong)index);
                int after = 0;
                for (int i = 0; i < 300 && after < 4; i++)
                {
                    if (done.Task.IsCompleted)
                        after++;
                    switch (random.Next(4))
                    {
                        case 0:
                            await ProbeAsync(cluster, process, "rename", s, d, () => v.Files.ExistsAsync(src, Ct));
                            break;
                        case 1:
                            await ProbeValueAsync(cluster, process, "rename", () => DestinationStateAsync(v.Files));
                            break;
                        case 2:
                            // One pinned listing of the source directory is one namespace view.
                            await ProbeValueAsync(cluster, process, "rename", async () =>
                            {
                                DfsDirectoryPage page = await v.Directories.ListPageAsync(srcDir, new DfsEnumerationOptions { PageSize = 10_000 }, null, Ct);
                                bool hasSrc = page.Entries.Any(e => e.Name == "file.bin");
                                if (sameDir)
                                {
                                    DfsEntryInfo? dstEntry = page.Entries.FirstOrDefault(e => e.Name == "renamed.bin");
                                    if (!overwrite)
                                        Assert.True(hasSrc ^ (dstEntry is not null), "one directory view showed both or neither of source and destination");
                                    else
                                        Assert.True(dstEntry is not null && (hasSrc ? dstEntry.InodeId == y : dstEntry.InodeId == x),
                                            "one directory view showed an invalid halfway state of an overwriting rename");
                                }

                                return hasSrc ? s : d;
                            });
                            break;
                        default:
                            // A read-only transaction gives one consistent snapshot of both names.
                            await ProbeValueAsync(cluster, process, "rename", async () =>
                            {
                                await using DfsTransaction tx = await v.Transactions.BeginAsync(null, Ct);
                                bool hasSrc = await tx.Files.ExistsAsync(src, Ct);
                                byte[] dstState = await DestinationStateAsync(tx.Files);
                                bool fromDst = dstState.AsSpan().SequenceEqual(d);
                                Assert.True(hasSrc != fromDst, $"transaction snapshot saw source={hasSrc} and destination state {(fromDst ? "D" : "S")}");
                                return hasSrc ? s : d;
                            });
                            break;
                    }
                }
            }

            Task[] observers = Enumerable.Range(0, observerClients.Count).Select(ObserveAsync).ToArray();
            await cluster.DelayAsync(TimeSpan.FromMilliseconds(20), Ct);
            int renames = overwrite ? 1 : 3;
            for (int r = 0; r < renames; r++)
            {
                bool forward = r % 2 == 0;
                await WriteAsync(cluster, "renamer", "rename", forward ? d : s,
                    () => forward ? renamer.Files.MoveAsync(src, dst, overwrite, Ct) : renamer.Files.MoveAsync(dst, src, false, Ct));
                await cluster.DelayAsync(TimeSpan.FromMilliseconds(15), Ct);
            }

            done.SetResult();
            await Task.WhenAll(observers);
            foreach (DfsClient c in observerClients)
                await c.DisposeAsync();

            LinearizabilityChecker.Check(cluster.History.Operations, new RegisterModel(s)).AssertValid();

            // The renamed inode keeps its identity and bytes; the replaced inode is detached and reclaimed.
            string final = renames % 2 == 1 ? dst : src;
            string gone = renames % 2 == 1 ? src : dst;
            DfsFileInfo info = await renamer.Files.GetInfoAsync(final, Ct);
            Assert.Equal(x, info.InodeId);
            AssertBytes(xBytes, await renamer.Files.ReadAllBytesAsync(final, Ct), "renamed file bytes");
            Assert.False(await renamer.Files.ExistsAsync(gone, Ct));
            if (overwrite)
                await cluster.WaitUntilAsync(async () => await InodeCountAsync(cluster) == inodesBefore - 1, RecoveryWait,
                    "the overwritten destination inode is reclaimed (no handles)", Ct);
            else
                Assert.Equal(inodesBefore, await InodeCountAsync(cluster));
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // NS-006 RenameDirectoryAtomic
    // ------------------------------------------------------------------------------------------------------------

    [Theory]
    [InlineData(601UL)]
    [InlineData(602UL)]
    [InlineData(603UL)]
    [Catalog("NS-006", "S", "INV-02", "INV-03", "INV-07")]
    public async Task NS_006_RenameDirectoryAtomic(ulong seed)
    {
        await ClusterTest.RunAsync(Sim(seed), async cluster =>
        {
            await using DfsClient renamerClient = await cluster.ConnectAsync(Named("renamer"), Ct);
            DfsVolume renamer = renamerClient.GetVolume(Vol);
            await renamer.Directories.CreateDirectoryAsync("/p/src", Ct);
            await renamer.Directories.CreateDirectoryAsync("/q", Ct);
            var relativeFiles = new List<string>();
            var relativeDirs = new List<string> { string.Empty };
            for (int a = 0; a < 3; a++)
            {
                for (int b = 0; b < 2; b++)
                {
                    string rel = $"/s{a}/t{b}";
                    await renamer.Directories.CreateDirectoryAsync("/p/src" + rel, Ct);
                    if (b == 0)
                        relativeDirs.Add($"/s{a}");
                    relativeDirs.Add(rel);
                    for (int f = 0; f < 4; f++)
                    {
                        string file = $"{rel}/f{f}.bin";
                        await renamer.Files.WriteAllBytesAsync("/p/src" + file, TestData.Bytes(seed + (ulong)(a * 100 + b * 10 + f), f == 0 ? Chunk + 1 : 200), Ct);
                        relativeFiles.Add(file);
                    }
                }
            }

            // Identity map before the move.
            var fileIdentity = new Dictionary<string, (InodeId Inode, ulong Revision, HashSet<ObjectId> Objects)>(StringComparer.Ordinal);
            foreach (string rel in relativeFiles)
            {
                DfsFileInfo info = await renamer.Files.GetInfoAsync("/p/src" + rel, Ct);
                fileIdentity[rel] = (info.InodeId, info.Revision, await ObjectsOfAsync(cluster, "/p/src" + rel));
            }

            var dirIdentity = new Dictionary<string, (InodeId Inode, ulong Revision)>(StringComparer.Ordinal);
            foreach (string rel in relativeDirs)
            {
                DfsDirectoryInfo info = await renamer.Directories.GetInfoAsync("/p/src" + rel, Ct);
                dirIdentity[rel] = (info.InodeId, info.Revision);
            }

            IReadOnlySet<ObjectId> storedBefore = await cluster.Inspect.GetAllStoredObjectsAsync(Vol);
            int inodesBefore = await InodeCountAsync(cluster);
            byte[] s = State("S");
            byte[] d = State("D");
            var done = new TaskCompletionSource();
            var observerClients = new List<DfsClient>();
            for (int i = 0; i < 3; i++)
                observerClients.Add(await cluster.ConnectAsync(Named($"obs{i}"), Ct));

            async Task ObserveAsync(int index)
            {
                DfsVolume v = observerClients[index].GetVolume(Vol);
                string process = $"obs{index}";
                var random = new SeededRandom(seed * 13 + (ulong)index);
                int after = 0;
                for (int i = 0; i < 300 && after < 4; i++)
                {
                    if (done.Task.IsCompleted)
                        after++;
                    bool useFile = random.Next(2) == 0;
                    string rel = useFile ? relativeFiles[random.Next(relativeFiles.Count)] : relativeDirs[random.Next(relativeDirs.Count)];
                    switch (random.Next(3))
                    {
                        case 0:
                            await ProbeAsync(cluster, process, "move", s, d, () => useFile ? v.Files.ExistsAsync("/p/src" + rel, Ct) : v.Directories.ExistsAsync("/p/src" + rel, Ct));
                            break;
                        case 1:
                            await ProbeAsync(cluster, process, "move", d, s, () => useFile ? v.Files.ExistsAsync("/q/dst" + rel, Ct) : v.Directories.ExistsAsync("/q/dst" + rel, Ct));
                            break;
                        default:
                            // A transaction sees the whole subtree under exactly one parent.
                            await ProbeValueAsync(cluster, process, "move", async () =>
                            {
                                await using DfsTransaction tx = await v.Transactions.BeginAsync(null, Ct);
                                bool old = useFile ? await tx.Files.ExistsAsync("/p/src" + rel, Ct) : await tx.Directories.ExistsAsync("/p/src" + rel, Ct);
                                bool now = useFile ? await tx.Files.ExistsAsync("/q/dst" + rel, Ct) : await tx.Directories.ExistsAsync("/q/dst" + rel, Ct);
                                Assert.True(old ^ now, $"one snapshot saw {rel} under {(old && now ? "both" : "neither")} parents");
                                return old ? s : d;
                            });
                            break;
                    }
                }
            }

            Task[] observers = Enumerable.Range(0, observerClients.Count).Select(ObserveAsync).ToArray();
            await cluster.DelayAsync(TimeSpan.FromMilliseconds(20), Ct);
            CommitToken moved = await WriteAsync(cluster, "renamer", "move", d, () => renamer.Directories.MoveAsync("/p/src", "/q/dst", Ct));
            done.SetResult();
            await Task.WhenAll(observers);
            foreach (DfsClient c in observerClients)
                await c.DisposeAsync();

            LinearizabilityChecker.Check(cluster.History.Operations, new RegisterModel(s)).AssertValid();
            Assert.False(moved.IsNone);
            Assert.False(await renamer.Directories.ExistsAsync("/p/src", Ct));

            // Every descendant is attached through the new parent with unchanged identity, revision and extents.
            foreach ((string rel, (InodeId inode, ulong revision, HashSet<ObjectId> objectIds)) in fileIdentity)
            {
                DfsFileInfo info = await renamer.Files.GetInfoAsync("/q/dst" + rel, Ct);
                Assert.Equal(inode, info.InodeId);
                Assert.Equal(revision, info.Revision);
                Assert.True(objectIds.SetEquals(await ObjectsOfAsync(cluster, "/q/dst" + rel)), $"{rel} must keep its objects (no copy)");
            }

            foreach ((string rel, (InodeId inode, ulong revision)) in dirIdentity)
            {
                DfsDirectoryInfo info = await renamer.Directories.GetInfoAsync("/q/dst" + rel, Ct);
                Assert.Equal(inode, info.InodeId);
                if (rel.Length > 0)
                    Assert.Equal(revision, info.Revision);
            }

            Assert.Equal(inodesBefore, await InodeCountAsync(cluster));
            Assert.True(storedBefore.SetEquals(await cluster.Inspect.GetAllStoredObjectsAsync(Vol)), "a directory rename must not copy data");
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // NS-008 RenameOverDestinationHonorsSharing
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>Handles held on the destination inode while it is renamed over.</summary>
    public enum DestinationHandles
    {
        ReaderShareRead,
        ReaderShareReadDelete,
        ReaderShareReadWriteDelete,
        ReaderShareNone,
        ReaderShareDelete,
        PinnedReaderShareReadDelete,
        WriterShareRead,
        WriterShareReadDelete,
        TwoReadersOneWithoutDelete,
    }

    private static (FileAccess Access, FileShare Share, DfsReadView View)[] HandleSpecs(DestinationHandles kind) => kind switch
    {
        DestinationHandles.ReaderShareRead => [(FileAccess.Read, FileShare.Read, DfsReadView.Live)],
        DestinationHandles.ReaderShareReadDelete => [(FileAccess.Read, FileShare.Read | FileShare.Delete, DfsReadView.Live)],
        DestinationHandles.ReaderShareReadWriteDelete => [(FileAccess.Read, FileShare.ReadWrite | FileShare.Delete, DfsReadView.Live)],
        DestinationHandles.ReaderShareNone => [(FileAccess.Read, FileShare.None, DfsReadView.Live)],
        DestinationHandles.ReaderShareDelete => [(FileAccess.Read, FileShare.Delete, DfsReadView.Live)],
        DestinationHandles.PinnedReaderShareReadDelete => [(FileAccess.Read, FileShare.Read | FileShare.Delete, DfsReadView.VersionPinned)],
        DestinationHandles.WriterShareRead => [(FileAccess.ReadWrite, FileShare.Read, DfsReadView.Live)],
        DestinationHandles.WriterShareReadDelete => [(FileAccess.ReadWrite, FileShare.Read | FileShare.Delete, DfsReadView.Live)],
        _ => [(FileAccess.Read, FileShare.ReadWrite | FileShare.Delete, DfsReadView.Live), (FileAccess.Read, FileShare.ReadWrite, DfsReadView.Live)],
    };

    [Theory]
    [InlineData(DestinationHandles.ReaderShareRead, false, 801UL)]
    [InlineData(DestinationHandles.ReaderShareReadDelete, true, 802UL)]
    [InlineData(DestinationHandles.ReaderShareReadWriteDelete, true, 803UL)]
    [InlineData(DestinationHandles.ReaderShareNone, false, 804UL)]
    [InlineData(DestinationHandles.ReaderShareDelete, true, 805UL)]
    [InlineData(DestinationHandles.PinnedReaderShareReadDelete, true, 806UL)]
    [InlineData(DestinationHandles.WriterShareRead, false, 807UL)]
    [InlineData(DestinationHandles.WriterShareReadDelete, true, 808UL)]
    [InlineData(DestinationHandles.TwoReadersOneWithoutDelete, false, 809UL)]
    [Catalog("NS-008", "S", "INV-02", "INV-07", "INV-09")]
    public async Task NS_008_RenameOverDestinationHonorsSharing(DestinationHandles kind, bool renameAllowed, ulong seed)
    {
        await ClusterTest.RunAsync(Sim(seed), async cluster =>
        {
            await using DfsClient holderClient = await cluster.ConnectAsync(Named("holder"), Ct);
            await using DfsClient moverClient = await cluster.ConnectAsync(Named("mover"), Ct);
            DfsVolume holder = holderClient.GetVolume(Vol);
            DfsVolume mover = moverClient.GetVolume(Vol);
            const string src = "/n8/src.bin";
            const string dst = "/n8/dst.bin";
            await mover.Directories.CreateDirectoryAsync("/n8", Ct);
            byte[] xBytes = TestData.Bytes(seed, Chunk + 10);
            byte[] yBytes = TestData.Bytes(seed + 1, 2 * Chunk + 20);
            await mover.Files.WriteAllBytesAsync(src, xBytes, Ct);
            await mover.Files.WriteAllBytesAsync(dst, yBytes, Ct);
            InodeId x = (await mover.Files.GetInfoAsync(src, Ct)).InodeId;
            InodeId y = (await mover.Files.GetInfoAsync(dst, Ct)).InodeId;
            HashSet<ObjectId> yObjects = await ObjectsOfAsync(cluster, dst);
            int inodesBefore = await InodeCountAsync(cluster);

            var handles = new List<DfsStream>();
            foreach ((FileAccess access, FileShare share, DfsReadView view) in HandleSpecs(kind))
            {
                handles.Add(await holder.Files.OpenAsync(dst, new DfsOpenOptions { Mode = FileMode.Open, Access = access, Share = share, ReadView = view }, Ct));
            }

            try
            {
                Assert.All(handles, h => Assert.Equal(y, h.InodeId));

                // Without overwrite an existing destination is always refused.
                await AssertEx.ThrowsDfsAsync<DfsAlreadyExistsException>(() => mover.Files.MoveAsync(src, dst, false, Ct), DfsErrorCode.AlreadyExists);

                Exception? failure = await Record.ExceptionAsync(() => mover.Files.MoveAsync(src, dst, true, Ct));
                if (!renameAllowed)
                {
                    Assert.True(failure is DfsSharingViolationException { ErrorCode: DfsErrorCode.SharingViolation or DfsErrorCode.WriterConflict },
                        $"rename-over must be refused while a handle lacks FileShare.Delete, got {failure?.ToString() ?? "success"}");
                    Assert.Equal(x, (await mover.Files.GetInfoAsync(src, Ct)).InodeId);
                    Assert.Equal(y, (await mover.Files.GetInfoAsync(dst, Ct)).InodeId);
                    foreach (DfsStream h in handles.Where(h => h.CanRead))
                    {
                        h.Position = 0;
                        AssertBytes(yBytes, await ReadToEndAsync(h), "existing handle after refused rename");
                    }

                    foreach (DfsStream h in handles)
                        await h.DisposeAsync();
                    handles.Clear();
                    AssertBytes(yBytes, await mover.Files.ReadAllBytesAsync(dst, Ct), "destination unchanged after refused rename");
                    AssertBytes(xBytes, await mover.Files.ReadAllBytesAsync(src, Ct), "source unchanged after refused rename");
                    // Retryable: once the reservations are gone the same rename succeeds.
                    await cluster.WaitUntilAsync(async () =>
                    {
                        Exception? retry = await Record.ExceptionAsync(() => mover.Files.MoveAsync(src, dst, true, Ct));
                        return retry is null;
                    }, RecoveryWait, "rename-over succeeds after the conflicting handles close", Ct);
                    Assert.Equal(x, (await mover.Files.GetInfoAsync(dst, Ct)).InodeId);
                    return;
                }

                Assert.True(failure is null, $"rename-over with compatible (Delete-sharing) handles must succeed, got {failure}");
                Assert.False(await mover.Files.ExistsAsync(src, Ct));
                Assert.Equal(x, (await mover.Files.GetInfoAsync(dst, Ct)).InodeId);
                AssertBytes(xBytes, await mover.Files.ReadAllBytesAsync(dst, Ct), "destination name now resolves to the renamed inode");

                // Old handles stay bound to the old (now detached) inode.
                foreach (DfsStream h in handles)
                {
                    Assert.Equal(y, h.InodeId);
                    if (h.CanRead)
                    {
                        h.Position = 0;
                        AssertBytes(yBytes, await ReadToEndAsync(h), "old handle reads its detached inode");
                    }
                }

                DfsStream? writer = handles.FirstOrDefault(h => h.CanWrite);
                if (writer is not null)
                {
                    byte[] patch = TestData.Bytes(seed + 2, 500);
                    writer.Position = 0;
                    await writer.WriteAsync(patch, Ct);
                    await writer.FlushAsync(Ct);
                    // Writes through the detached inode never recreate or retarget its old pathname.
                    Assert.Equal(x, (await mover.Files.GetInfoAsync(dst, Ct)).InodeId);
                    AssertBytes(xBytes, await mover.Files.ReadAllBytesAsync(dst, Ct), "destination after a write through the detached handle");
                    Assert.False(await mover.Files.ExistsAsync(src, Ct));
                    writer.Position = 0;
                    byte[] expectedY = (byte[])yBytes.Clone();
                    patch.CopyTo(expectedY, 0);
                    AssertBytes(expectedY, await ReadToEndAsync(writer), "the writer reads its own write on the detached inode");
                }

                // The detached inode's data is protected while handles are open, then reclaimed.
                await cluster.DelayAsync(TimeSpan.FromSeconds(2), Ct);
                if (writer is null)
                    await AssertObjectsStoredAsync(cluster, yObjects, "detached inode with open handles");
                foreach (DfsStream h in handles)
                    await h.DisposeAsync();
                handles.Clear();
                await cluster.WaitUntilAsync(async () => await InodeCountAsync(cluster) == inodesBefore - 1, RecoveryWait,
                    "the detached destination inode is reclaimed after its last handle closes", Ct);
                await WaitForObjectsReclaimedAsync(cluster, yObjects, "objects of the replaced inode are reclaimed after close");
            }
            finally
            {
                foreach (DfsStream h in handles)
                    await h.DisposeAsync();
            }
        }, Ct);
    }

    [Theory]
    [InlineData(811UL)]
    [InlineData(812UL)]
    [Catalog("NS-008", "S", "INV-02", "INV-07")]
    public async Task NS_008_RenameOverDestinationHonorsSharing_SourceHandleStaysBound(ulong seed)
    {
        await ClusterTest.RunAsync(Sim(seed), async cluster =>
        {
            await using DfsClient holderClient = await cluster.ConnectAsync(Named("holder"), Ct);
            await using DfsClient moverClient = await cluster.ConnectAsync(Named("mover"), Ct);
            DfsVolume mover = moverClient.GetVolume(Vol);
            byte[] xBytes = TestData.Bytes(seed, 3000);
            await mover.Files.WriteAllBytesAsync("/n8s-src.bin", xBytes, Ct);
            InodeId x = (await mover.Files.GetInfoAsync("/n8s-src.bin", Ct)).InodeId;
            await using DfsStream handle = await holderClient.GetVolume(Vol).Files.OpenAsync("/n8s-src.bin", FileMode.Open, FileAccess.Read, FileShare.Read, Ct);

            // Renaming the source itself is not delete/overwrite; whichever way the contract decides, the handle keeps
            // naming inode X and never follows a path.
            Exception? failure = await Record.ExceptionAsync(() => mover.Files.MoveAsync("/n8s-src.bin", "/n8s-dst.bin", false, Ct));
            Assert.True(failure is null or DfsSharingViolationException, $"unexpected {failure}");
            string current = failure is null ? "/n8s-dst.bin" : "/n8s-src.bin";
            Assert.Equal(x, (await mover.Files.GetInfoAsync(current, Ct)).InodeId);
            Assert.Equal(x, handle.InodeId);
            AssertBytes(xBytes, await ReadToEndAsync(handle), "handle after source rename");
            await mover.Files.WriteAllBytesAsync(failure is null ? "/n8s-src.bin" : "/n8s-other.bin", TestData.Bytes(seed + 1, 10), Ct);
            handle.Position = 0;
            AssertBytes(xBytes, await ReadToEndAsync(handle), "a new file at the old name is not seen through the old handle");
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // NS-010 DeleteRacesCreate
    // ------------------------------------------------------------------------------------------------------------

    [Theory]
    [InlineData(1001UL)]
    [InlineData(1002UL)]
    [InlineData(1003UL)]
    [InlineData(1004UL)]
    [Catalog("NS-010", "S", "INV-02", "INV-07")]
    public async Task NS_010_DeleteRacesCreate(ulong seed)
    {
        const string name = "/n10";
        static byte[] Tag(InodeId inode)
        {
            var bytes = new byte[5000];
            for (int i = 0; i + 8 <= bytes.Length; i += 8)
                BitConverter.TryWriteBytes(bytes.AsSpan(i, 8), inode.Value);
            return bytes;
        }

        await ClusterTest.RunAsync(Sim(seed), async cluster =>
        {
            await using DfsClient creatorClient = await cluster.ConnectAsync(Named("creator"), Ct);
            await using DfsClient deleterClient = await cluster.ConnectAsync(Named("deleter"), Ct);
            await using DfsClient readerClient = await cluster.ConnectAsync(Named("reader"), Ct);
            DfsVolume creator = creatorClient.GetVolume(Vol);
            DfsVolume deleter = deleterClient.GetVolume(Vol);
            DfsVolume reader = readerClient.GetVolume(Vol);
            var created = new List<InodeId>();
            var deleted = new HashSet<InodeId>();
            bool creatorDone = false;
            bool deleterDone = false;

            async Task CreatorAsync()
            {
                for (int i = 0; i < 12; i++)
                {
                    DfsStream stream;
                    try
                    {
                        stream = await creator.Files.OpenAsync(name, new DfsOpenOptions
                        {
                            Mode = FileMode.CreateNew,
                            Access = FileAccess.Write,
                            Share = FileShare.Read | FileShare.Delete,
                        }, Ct);
                    }
                    catch (DfsAlreadyExistsException)
                    {
                        await cluster.DelayAsync(TimeSpan.FromMilliseconds(7), Ct);
                        continue;
                    }

                    InodeId inode = stream.InodeId;
                    Assert.DoesNotContain(inode, created);
                    Assert.True(created.Count == 0 || inode.Value > created[^1].Value, "a recreated name must get a new, never-reused inode");
                    created.Add(inode);
                    await using (stream)
                    {
                        await stream.WriteAsync(Tag(inode), Ct);
                        await stream.FlushAsync(Ct);
                    }

                    // A write through an inode deleted meanwhile never resurrects its pathname.
                    if (deleted.Contains(inode))
                    {
                        Exception? probe = await Record.ExceptionAsync(async () =>
                        {
                            DfsFileInfo info = await reader.Files.GetInfoAsync(name, Ct);
                            Assert.NotEqual(inode, info.InodeId);
                        });
                        Assert.True(probe is null or FileNotFoundException, $"unexpected {probe}");
                    }

                    await cluster.DelayAsync(TimeSpan.FromMilliseconds(3), Ct);
                }

                creatorDone = true;
            }

            async Task DeleterAsync()
            {
                for (int i = 0; i < 12; i++)
                {
                    DfsFileInfo info;
                    try
                    {
                        info = await deleter.Files.GetInfoAsync(name, Ct);
                    }
                    catch (FileNotFoundException)
                    {
                        await cluster.DelayAsync(TimeSpan.FromMilliseconds(5), Ct);
                        continue;
                    }

                    // Only this process deletes and the creator uses CreateNew, so the inode deleted is info.InodeId.
                    CommitToken token = await deleter.Files.DeleteAsync(name, Ct);
                    Assert.False(token.IsNone);
                    deleted.Add(info.InodeId);
                    await cluster.DelayAsync(TimeSpan.FromMilliseconds(4), Ct);
                }

                deleterDone = true;
            }

            // Inodes the reader observed. Membership in `created` is checked after the race: a CreateNew open can commit
            // (and be observed) before the creator's continuation has recorded its inode.
            var observedInodes = new List<InodeId>();

            async Task ReaderAsync()
            {
                for (int i = 0; i < 400 && !(creatorDone && deleterDone); i++)
                {
                    var deletedBefore = new HashSet<InodeId>(deleted);
                    try
                    {
                        DfsFileInfo info = await reader.Files.GetInfoAsync(name, Ct);
                        observedInodes.Add(info.InodeId);
                        Assert.False(deletedBefore.Contains(info.InodeId), $"name resolved to inode {info.InodeId} whose deletion had already completed (resurrection)");
                    }
                    catch (FileNotFoundException)
                    {
                    }

                    try
                    {
                        await using DfsStream s = await reader.Files.OpenAsync(name, new DfsOpenOptions
                        {
                            Mode = FileMode.Open,
                            Access = FileAccess.Read,
                            Share = FileShare.ReadWrite | FileShare.Delete,
                            ReadView = DfsReadView.VersionPinned, // one incarnation revision for the whole read
                        }, Ct);
                        Assert.False(deletedBefore.Contains(s.InodeId), $"open resolved to inode {s.InodeId} whose deletion had already completed (resurrection)");
                        observedInodes.Add(s.InodeId);
                        byte[] content = await ReadToEndAsync(s);
                        Assert.True(content.Length == 0 || content.AsSpan().SequenceEqual(Tag(s.InodeId)),
                            $"handle on inode {s.InodeId} returned bytes of another incarnation");
                    }
                    catch (FileNotFoundException)
                    {
                    }

                    await cluster.DelayAsync(TimeSpan.FromMilliseconds(2), Ct);
                }
            }

            await Task.WhenAll(CreatorAsync(), DeleterAsync(), ReaderAsync());
            Assert.True(created.Count >= 3, "the race must have produced several incarnations");
            Assert.True(deleted.Count >= 2, "the race must have deleted several incarnations");
            Assert.True(deleted.IsSubsetOf(created));
            Assert.All(observedInodes, id => Assert.Contains(id, created)); // the name only ever named real incarnations

            // Final state names at most the newest surviving incarnation.
            try
            {
                DfsFileInfo final = await reader.Files.GetInfoAsync(name, Ct);
                Assert.Equal(created[^1], final.InodeId);
                Assert.DoesNotContain(final.InodeId, deleted);
                AssertBytes(Tag(final.InodeId), await reader.Files.ReadAllBytesAsync(name, Ct), "final incarnation contents");
            }
            catch (FileNotFoundException)
            {
                Assert.Contains(created[^1], deleted);
            }

            await cluster.WaitForReplicationAsync(Vol, Ct);
            await cluster.Inspect.AssertReplicasConvergedAsync(Vol);
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // NS-011 RenameRacesOpen
    // ------------------------------------------------------------------------------------------------------------

    [Theory]
    [InlineData(false, 1101UL)]
    [InlineData(false, 1102UL)]
    [InlineData(true, 1103UL)]
    [InlineData(true, 1104UL)]
    [Catalog("NS-011", "S", "INV-02", "INV-03", "INV-07", "INV-12")]
    public async Task NS_011_RenameRacesOpen(bool includeBlockingOpener, ulong seed)
    {
        await ClusterTest.RunAsync(Sim(seed), async cluster =>
        {
            TestPrincipal pat = await cluster.CreatePrincipalAsync("pat", Grant(DfsPermissions.Read));
            await using DfsClient adminClient = await cluster.ConnectAsync(Named("admin"), Ct);
            await using DfsClient openerClient = await cluster.ConnectAsync(Named("opener"), Ct);
            await using DfsClient patClient = await cluster.ConnectAsync(Named("pat", pat.Credentials), Ct);
            DfsVolume admin = adminClient.GetVolume(Vol);
            DfsVolume opener = openerClient.GetVolume(Vol);
            DfsVolume patVol = patClient.GetVolume(Vol);
            byte[] xValue = State("X");
            byte[] yValue = State("Y");
            await admin.Directories.CreateDirectoryAsync("/n11", Ct);

            for (int round = 0; round < 4; round++)
            {
                string x = $"/n11/x{round}.bin";
                string b = $"/n11/b{round}.bin";
                string key = b;
                byte[] xBytes = TestData.Bytes(seed + (ulong)round, 2000);
                byte[] yBytes = TestData.Bytes(seed + 100 + (ulong)round, 3000);
                await admin.Files.WriteAllBytesAsync(x, xBytes, Ct);
                await admin.Files.SetAccessAsync(x, new DfsInodeAccess(OwnerOnly: true), Ct); // pat may never open X
                await admin.Files.WriteAllBytesAsync(b, yBytes, Ct);
                InodeId xId = (await admin.Files.GetInfoAsync(x, Ct)).InodeId;
                InodeId yId = (await admin.Files.GetInfoAsync(b, Ct)).InodeId;
                var renameDone = new TaskCompletionSource();
                var kept = new List<(DfsStream Stream, byte[] Expected)>();

                async Task AdminOpensAsync(FileShare share, string process)
                {
                    bool mayKeep = share.HasFlag(FileShare.Delete); // a kept handle without Delete sharing would block the rename forever
                    int after = 0;
                    for (int i = 0; i < 100 && after < 2; i++)
                    {
                        if (renameDone.Task.IsCompleted)
                            after++;
                        long id = cluster.History.Invoke(process, "read", key, null);
                        DfsStream s;
                        try
                        {
                            s = await opener.Files.OpenAsync(b, FileMode.Open, FileAccess.Read, share, Ct);
                        }
                        catch (DfsSharingViolationException ex)
                        {
                            cluster.History.Complete(id, OpOutcome.Failed, ex);
                            continue;
                        }

                        Assert.True(s.InodeId == xId || s.InodeId == yId, $"handle names an unexpected inode {s.InodeId}");
                        bool isX = s.InodeId == xId;
                        cluster.History.Complete(id, OpOutcome.Ok, isX ? xValue : yValue);
                        byte[] expected = isX ? xBytes : yBytes;
                        AssertBytes(expected, await ReadToEndAsync(s), "a handle reads exactly the inode it names");
                        if (mayKeep && i % 5 == 0)
                        {
                            kept.Add((s, expected)); // keep some handles across the rename
                        }
                        else
                        {
                            await s.DisposeAsync();
                            if (!mayKeep)
                            {
                                // Leave reservation-free windows: a back-to-back stream of non-Delete-sharing handles
                                // would otherwise starve the (legitimately refused, retryable) rename.
                                await cluster.DelayAsync(TimeSpan.FromMilliseconds(3), Ct);
                            }
                        }
                    }
                }

                async Task PatOpensAsync()
                {
                    int after = 0;
                    for (int i = 0; i < 100 && after < 2; i++)
                    {
                        if (renameDone.Task.IsCompleted)
                            after++;
                        long id = cluster.History.Invoke("pat", "read", key, null);
                        try
                        {
                            await using DfsStream s = await patVol.Files.OpenAsync(b, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete, Ct);
                            Assert.Equal(yId, s.InodeId); // authorization checked at the open's linearization point
                            cluster.History.Complete(id, OpOutcome.Ok, yValue);
                            AssertBytes(yBytes, await ReadToEndAsync(s), "pat reads Y");
                        }
                        catch (UnauthorizedAccessException)
                        {
                            cluster.History.Complete(id, OpOutcome.Ok, xValue); // denial ⇔ the name resolved to X
                        }
                    }
                }

                Task[] openers =
                [
                    AdminOpensAsync(FileShare.ReadWrite | FileShare.Delete, "admin-compatible"),
                    PatOpensAsync(),
                    includeBlockingOpener ? AdminOpensAsync(FileShare.Read, "admin-blocking") : Task.CompletedTask,
                ];
                await cluster.DelayAsync(TimeSpan.FromMilliseconds(10), Ct);
                long renameOp = cluster.History.Invoke("renamer", "write", key, xValue);
                // Bounded retry that outlasts the blocking opener (at most 100 short-lived handles): a refusal while a
                // non-Delete-sharing handle exists is correct and retryable.
                Exception? renameFailure = null;
                for (int attempt = 0; attempt < 1_000; attempt++)
                {
                    renameFailure = await Record.ExceptionAsync(() => admin.Files.MoveAsync(x, b, true, Ct));
                    if (renameFailure is not DfsSharingViolationException)
                        break;
                    Assert.True(includeBlockingOpener, "only a handle without FileShare.Delete may block the rename");
                    await cluster.DelayAsync(TimeSpan.FromMilliseconds(5), Ct);
                }

                Assert.Null(renameFailure);
                cluster.History.Complete(renameOp, OpOutcome.Ok);
                renameDone.SetResult();
                await Task.WhenAll(openers);

                // Handles opened before the rename stay bound to their inode.
                foreach ((DfsStream s, byte[] expected) in kept)
                {
                    s.Position = 0;
                    AssertBytes(expected, await ReadToEndAsync(s), "kept handle after the rename");
                    await s.DisposeAsync();
                }

                Assert.Equal(xId, (await admin.Files.GetInfoAsync(b, Ct)).InodeId);
                await Assert.ThrowsAnyAsync<UnauthorizedAccessException>(() => patVol.Files.OpenReadAsync(b, Ct));
            }

            LinearizabilityChecker.Check(cluster.History.Operations, new RegisterModel(yValue)).AssertValid();
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // NS-012 EnumeratePinnedVersion
    // ------------------------------------------------------------------------------------------------------------

    [Theory]
    [InlineData("add", 1, 1201UL)]
    [InlineData("add", 7, 1202UL)]
    [InlineData("delete-unreturned", 7, 1203UL)]
    [InlineData("delete-returned", 7, 1204UL)]
    [InlineData("rename-within", 7, 1205UL)]
    [InlineData("recreate-same-name", 7, 1206UL)]
    [InlineData("mixed", 1, 1207UL)]
    [InlineData("mixed", 7, 1208UL)]
    [InlineData("mixed", 53, 1209UL)]
    [InlineData("mixed", 1000, 1210UL)]
    [Catalog("NS-012", "S", "INV-03", "INV-07")]
    public async Task NS_012_EnumeratePinnedVersion(string mutation, int pageSize, ulong seed)
    {
        await ClusterTest.RunAsync(Sim(seed), async cluster =>
        {
            await using DfsClient enumeratorClient = await cluster.ConnectAsync(Named("enum"), Ct);
            await using DfsClient mutatorClient = await cluster.ConnectAsync(Named("mutator"), Ct);
            DfsVolume v = enumeratorClient.GetVolume(Vol);
            DfsVolume m = mutatorClient.GetVolume(Vol);
            const string dir = "/e";
            await m.Directories.CreateDirectoryAsync(dir, Ct);
            for (int i = 0; i < 50; i++)
                await m.Files.WriteAllBytesAsync($"{dir}/f{i:D2}", TestData.Bytes(seed + (ulong)i, 10 + i), Ct);
            for (int i = 0; i < 3; i++)
                await m.Directories.CreateDirectoryAsync($"{dir}/s{i}", Ct);
            Dictionary<string, DfsEntryInfo> original = (await ToListAsync(v.Directories.EnumerateAsync(dir, null, Ct)))
                .ToDictionary(e => e.Name, StringComparer.Ordinal);
            Assert.Equal(53, original.Count);
            int counter = 0;

            async Task MutateAsync(IReadOnlyCollection<string> returned)
            {
                string[] unreturned = original.Keys.Where(n => n.StartsWith('f') && !returned.Contains(n)).Order(StringComparer.Ordinal).ToArray();
                string[] returnedFiles = returned.Where(n => n.StartsWith('f')).Order(StringComparer.Ordinal).ToArray();
                bool all = mutation == "mixed";
                counter++;
                if (mutation == "add" || all)
                    await m.Files.WriteAllBytesAsync($"{dir}/g{counter:D2}", TestData.Bytes(seed + 500 + (ulong)counter, 5), Ct);
                if ((mutation == "delete-unreturned" || all) && unreturned.Length > 0)
                    await m.Files.DeleteAsync($"{dir}/{unreturned[^1]}", Ct);
                if ((mutation == "delete-returned" || all) && returnedFiles.Length > 0)
                    await m.Files.DeleteAsync($"{dir}/{returnedFiles[0]}", Ct);
                if ((mutation == "rename-within" || all) && unreturned.Length > 1 && await m.Files.ExistsAsync($"{dir}/{unreturned[0]}", Ct))
                    await m.Files.MoveAsync($"{dir}/{unreturned[0]}", $"{dir}/zz{counter:D2}", false, Ct);
                if ((mutation == "recreate-same-name" || all) && unreturned.Length > 2 && await m.Files.ExistsAsync($"{dir}/{unreturned[1]}", Ct))
                {
                    await m.Files.DeleteAsync($"{dir}/{unreturned[1]}", Ct);
                    await m.Files.WriteAllBytesAsync($"{dir}/{unreturned[1]}", TestData.Bytes(seed + 900 + (ulong)counter, 99), Ct);
                }
            }

            void AssertOriginal(List<DfsEntryInfo> entries, string because)
            {
                Assert.Equal(entries.Count, entries.Select(e => e.Name).Distinct(StringComparer.Ordinal).Count());
                Assert.Equal(original.Keys.Order(StringComparer.Ordinal), entries.Select(e => e.Name).Order(StringComparer.Ordinal));
                foreach (DfsEntryInfo e in entries)
                {
                    DfsEntryInfo o = original[e.Name];
                    Assert.True(o.InodeId == e.InodeId && o.Revision == e.Revision && o.Length == e.Length && o.Kind == e.Kind,
                        $"{because}: entry {e.Name} is not the pinned version (inode {e.InodeId} rev {e.Revision}, expected {o.InodeId} rev {o.Revision})");
                }
            }

            // Paged API: mutate between every page.
            var options = new DfsEnumerationOptions { PageSize = pageSize };
            var paged = new List<DfsEntryInfo>();
            DfsDirectoryPage page = await v.Directories.ListPageAsync(dir, options, null, Ct);
            CommitToken pinnedAt = page.PinnedAt;
            Assert.False(pinnedAt.IsNone);
            paged.AddRange(page.Entries);
            int pages = 1;
            while (page.ContinuationToken is not null)
            {
                Assert.True(page.Entries.Count <= pageSize);
                if (pages <= 6)
                    await MutateAsync(paged.Select(e => e.Name).ToHashSet(StringComparer.Ordinal));
                page = await v.Directories.ListPageAsync(dir, options, page.ContinuationToken, Ct);
                Assert.Equal(pinnedAt, page.PinnedAt);
                paged.AddRange(page.Entries);
                pages++;
                Assert.True(pages < 1000, "enumeration did not terminate");
            }

            AssertOriginal(paged, "paged enumeration");

            // Async enumeration: snapshot taken at the first page, mutations while consuming.
            Dictionary<string, DfsEntryInfo> second = (await ToListAsync(v.Directories.EnumerateAsync(dir, null, Ct))).ToDictionary(e => e.Name, StringComparer.Ordinal);
            original = second;
            var streamed = new List<DfsEntryInfo>();
            await foreach (DfsEntryInfo e in v.Directories.EnumerateAsync(dir, options, Ct))
            {
                streamed.Add(e);
                if (streamed.Count % Math.Max(1, Math.Min(pageSize, 10)) == 0 && streamed.Count <= 30)
                    await MutateAsync(streamed.Select(x => x.Name).ToHashSet(StringComparer.Ordinal));
            }

            AssertOriginal(streamed, "async enumeration");

            // A new enumeration observes the newer namespace version.
            List<DfsEntryInfo> fresh = await ToListAsync(v.Directories.EnumerateAsync(dir, null, Ct));
            if (mutation is "add" or "mixed")
                Assert.Contains(fresh, e => e.Name.StartsWith('g'));
            if (mutation is "rename-within" or "mixed")
                Assert.Contains(fresh, e => e.Name.StartsWith("zz", StringComparison.Ordinal));
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // NS-014 ExpiredContinuationFails
    // ------------------------------------------------------------------------------------------------------------

    [Theory]
    [InlineData("time-expiry", 1401UL)]
    [InlineData("time-expiry-with-mutation-and-gc", 1402UL)]
    [InlineData("leader-change-before-expiry", 1403UL)]
    [InlineData("async-enumeration-slow-consumer", 1404UL)]
    [Catalog("NS-014", "S", "INV-03", "INV-07", "INV-09")]
    public async Task NS_014_ExpiredContinuationFails(string variant, ulong seed)
    {
        await ClusterTest.RunAsync(Sim(seed), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(Named("enum"), Ct);
            await using DfsClient mutatorClient = await cluster.ConnectAsync(Named("mutator"), Ct);
            DfsVolume v = client.GetVolume(Vol);
            DfsVolume m = mutatorClient.GetVolume(Vol);
            const string dir = "/c";
            await m.Directories.CreateDirectoryAsync(dir, Ct);
            for (int i = 0; i < 30; i++)
                await m.Files.WriteAllBytesAsync($"{dir}/f{i:D2}", TestData.Bytes(seed + (ulong)i, 20), Ct);
            string[] originalNames = Enumerable.Range(0, 30).Select(i => $"f{i:D2}").ToArray();
            TimeSpan pinDuration = cluster.Options.Timings.EnumerationPinDuration;
            var options = new DfsEnumerationOptions { PageSize = 5 };

            async Task MutateAsync()
            {
                for (int i = 0; i < 10; i++)
                {
                    await m.Files.DeleteAsync($"{dir}/f{29 - i:D2}", Ct);
                    await m.Files.WriteAllBytesAsync($"{dir}/n{i:D2}", TestData.Bytes(seed + 300 + (ulong)i, 20), Ct);
                }
            }

            static void AssertExpired(Exception? failure, string because) =>
                Assert.True(failure is DfsPinExpiredException { ErrorCode: DfsErrorCode.ContinuationExpired or DfsErrorCode.PinExpired },
                    $"{because}: an expired continuation must fail explicitly, got {failure?.ToString() ?? "a page"}");

            if (variant == "async-enumeration-slow-consumer")
            {
                var yielded = new List<string>();
                Exception? failure = await Record.ExceptionAsync(async () =>
                {
                    await foreach (DfsEntryInfo e in v.Directories.EnumerateAsync(dir, options, Ct))
                    {
                        yielded.Add(e.Name);
                        if (yielded.Count == 3)
                        {
                            await MutateAsync();
                            await cluster.DelayAsync(pinDuration + TimeSpan.FromSeconds(3), Ct);
                        }
                    }
                });
                // Either the SDK kept its pin alive (complete original listing) or the expiry surfaces; never a newer view.
                Assert.All(yielded, n => Assert.Contains(n, originalNames));
                Assert.Equal(yielded.Count, yielded.Distinct(StringComparer.Ordinal).Count());
                if (failure is null)
                    Assert.Equal(originalNames, yielded.Order(StringComparer.Ordinal));
                else
                    AssertExpired(failure, "slow async enumeration");
                return;
            }

            DfsDirectoryPage first = await v.Directories.ListPageAsync(dir, options, null, Ct);
            Assert.NotNull(first.ContinuationToken);
            string token = first.ContinuationToken!;

            if (variant == "leader-change-before-expiry")
            {
                TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
                await leader.CrashAsync();
                TestNode newLeader = await cluster.GetLeaderAsync(Vol, Ct);
                Assert.NotEqual(leader.Name, newLeader.Name);
                await MutateAsync();
                var names = first.Entries.Select(e => e.Name).ToList();
                Exception? failure = await Record.ExceptionAsync(async () =>
                {
                    string? next = token;
                    while (next is not null)
                    {
                        DfsDirectoryPage page = await v.Directories.ListPageAsync(dir, options, next, Ct);
                        Assert.Equal(first.PinnedAt, page.PinnedAt);
                        names.AddRange(page.Entries.Select(e => e.Name));
                        next = page.ContinuationToken;
                    }
                });
                // The pinned version either survives the leader change or fails explicitly; it never restarts.
                Assert.All(names, n => Assert.Contains(n, originalNames));
                if (failure is null)
                    Assert.Equal(originalNames, names.Order(StringComparer.Ordinal));
                else
                    AssertExpired(failure, "continuation after leader change");
                await leader.RestartAsync();
                return;
            }

            if (variant == "time-expiry-with-mutation-and-gc")
                await MutateAsync();
            await cluster.DelayAsync(pinDuration + TimeSpan.FromSeconds(2), Ct);
            await cluster.WaitUntilAsync(async () =>
            {
                VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Vol);
                return !state.Pins.Any(p => string.Equals(p.Kind, "Enumeration", StringComparison.Ordinal));
            }, RecoveryWait, "the expired enumeration pin is reclaimed", Ct);
            if (variant == "time-expiry-with-mutation-and-gc")
                await cluster.DelayAsync(cluster.Options.Timings.GcInterval * 4, Ct);

            for (int attempt = 0; attempt < 2; attempt++)
            {
                Exception? failure = await Record.ExceptionAsync(() => v.Directories.ListPageAsync(dir, options, token, Ct));
                AssertExpired(failure, $"continuation after expiry (attempt {attempt + 1})");
            }

            // Restarting is the application's explicit choice and yields the newer version.
            List<DfsEntryInfo> restarted = await ToListAsync(v.Directories.EnumerateAsync(dir, options, Ct));
            if (variant == "time-expiry-with-mutation-and-gc")
            {
                Assert.Contains(restarted, e => e.Name == "n00");
                Assert.DoesNotContain(restarted, e => e.Name == "f29");
            }
            else
            {
                Assert.Equal(originalNames, restarted.Select(e => e.Name).Order(StringComparer.Ordinal));
            }
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // NS-015 DeletionRespectsOpenPins
    // ------------------------------------------------------------------------------------------------------------

    [Theory]
    [InlineData("file", "close", DfsReadView.Live, 1501UL)]
    [InlineData("file", "close", DfsReadView.VersionPinned, 1502UL)]
    [InlineData("file", "client-death", DfsReadView.Live, 1503UL)]
    [InlineData("subtree", "close", DfsReadView.Live, 1504UL)]
    [InlineData("subtree", "close", DfsReadView.VersionPinned, 1505UL)]
    [InlineData("subtree", "client-death", DfsReadView.VersionPinned, 1506UL)]
    [Catalog("NS-015", "S", "INV-07", "INV-09", "INV-14")]
    public async Task NS_015_DeletionRespectsOpenPins(string deleteKind, string ending, DfsReadView view, ulong seed)
    {
        await ClusterTest.RunAsync(Sim(seed), async cluster =>
        {
            DfsClient readerClient = await cluster.ConnectAsync(Named("reader"), Ct);
            await using DfsClient deleterClient = await cluster.ConnectAsync(Named("deleter"), Ct);
            DfsVolume reader = readerClient.GetVolume(Vol);
            DfsVolume deleter = deleterClient.GetVolume(Vol);
            const string path = "/n15/dir/file.bin";
            byte[] content = TestData.Bytes(seed, 4 * Chunk + 17);
            await deleter.Directories.CreateDirectoryAsync("/n15/dir", Ct);
            await deleter.Files.WriteAllBytesAsync(path, content, Ct);
            await deleter.Files.WriteAllBytesAsync("/n15/dir/other.bin", TestData.Bytes(seed + 1, 2 * Chunk), Ct);
            HashSet<ObjectId> objects = await ObjectsOfAsync(cluster, path);
            if (deleteKind == "subtree")
                objects.UnionWith(await ObjectsOfAsync(cluster, "/n15/dir/other.bin"));
            HashSet<ObjectId> fileObjects = await ObjectsOfAsync(cluster, path);
            int inodesBefore = await InodeCountAsync(cluster);
            bool killed = false;

            DfsStream handle = await reader.Files.OpenAsync(path, new DfsOpenOptions
            {
                Mode = FileMode.Open,
                Access = FileAccess.Read,
                Share = FileShare.Read | FileShare.Delete,
                ReadView = view,
            }, Ct);
            try
            {
                InodeId inode = handle.InodeId;
                var half = new byte[content.Length / 2];
                await handle.ReadExactlyAsync(half, Ct);

                // Delete while the permitted reader is mid-read.
                CommitToken token = deleteKind == "file"
                    ? await deleter.Files.DeleteAsync(path, Ct)
                    : await deleter.Directories.DeleteAsync("/n15/dir", true, Ct);
                Assert.False(token.IsNone);
                Assert.False(await deleter.Files.ExistsAsync(path, Ct));

                // Reads remain safe: the rest of the same bytes, then a re-read from the start, even across GC passes.
                byte[] rest = await ReadToEndAsync(handle);
                AssertBytes(content, Concat(half, rest), "read through the open handle across the deletion");
                await cluster.DelayAsync(cluster.Options.Timings.GcInterval * 8, Ct);
                await AssertObjectsStoredAsync(cluster, fileObjects, "objects of a deleted inode with an open reader");
                handle.Position = 0;
                AssertBytes(content, await ReadToEndAsync(handle), "re-read after GC passes");
                Assert.Equal(inode, handle.InodeId);

                // The deleted pathname is not recreated by the handle and a new file there is a different inode.
                await deleter.Files.WriteAllBytesAsync("/n15-recreated.bin", TestData.Bytes(seed + 2, 10), Ct);
                Assert.False(await deleter.Files.ExistsAsync(path, Ct));

                if (ending == "close")
                {
                    await handle.DisposeAsync();
                }
                else
                {
                    cluster.KillClient(readerClient);
                    killed = true;
                }

                // Reclamation happens only after close/expiry.
                await WaitForObjectsReclaimedAsync(cluster, objects, "objects of the deleted inode(s) are reclaimed after the last pin goes away");
                int subtreeInodes = deleteKind == "file" ? 1 : 3;
                await cluster.WaitUntilAsync(async () => await InodeCountAsync(cluster) == inodesBefore - subtreeInodes + 1, RecoveryWait,
                    "detached inodes are reclaimed after the last handle closes or expires", Ct);
            }
            finally
            {
                if (!killed)
                {
                    await handle.DisposeAsync();
                    await readerClient.DisposeAsync();
                }
            }
        }, Ct);
    }

    [Theory]
    [InlineData("file", 1511UL)]
    [InlineData("subtree", 1512UL)]
    [Catalog("NS-015", "S", "INV-07", "INV-09")]
    public async Task NS_015_DeletionRespectsOpenPins_NonDeleteShareBlocksDeletion(string deleteKind, ulong seed)
    {
        await ClusterTest.RunAsync(Sim(seed), async cluster =>
        {
            await using DfsClient readerClient = await cluster.ConnectAsync(Named("reader"), Ct);
            await using DfsClient deleterClient = await cluster.ConnectAsync(Named("deleter"), Ct);
            DfsVolume deleter = deleterClient.GetVolume(Vol);
            const string path = "/n15b/dir/file.bin";
            byte[] content = TestData.Bytes(seed, 2 * Chunk);
            await deleter.Directories.CreateDirectoryAsync("/n15b/dir", Ct);
            await deleter.Files.WriteAllBytesAsync(path, content, Ct);
            await using DfsStream handle = await readerClient.GetVolume(Vol).Files.OpenAsync(path, FileMode.Open, FileAccess.Read, FileShare.Read, Ct);
            Exception? failure = await Record.ExceptionAsync(() => deleteKind == "file"
                ? deleter.Files.DeleteAsync(path, Ct)
                : deleter.Directories.DeleteAsync("/n15b/dir", true, Ct));
            Assert.True(failure is DfsSharingViolationException, $"deleting an inode whose handle lacks FileShare.Delete must be refused, got {failure?.ToString() ?? "success"}");
            Assert.True(await deleter.Files.ExistsAsync(path, Ct), "a refused (recursive) delete must not detach anything");
            Assert.True(await deleter.Directories.ExistsAsync("/n15b/dir", Ct));
            AssertBytes(content, await ReadToEndAsync(handle), "the handle is unaffected");
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // NS-017 DirectoryMutationHasRetryIdentity
    // ------------------------------------------------------------------------------------------------------------

    [Theory]
    [InlineData("mkdir", true, 1701UL)]
    [InlineData("mkdir", false, 1702UL)]
    [InlineData("rename-directory", true, 1703UL)]
    [InlineData("rename-file", true, 1704UL)]
    [InlineData("rename-directory", false, 1705UL)]
    [InlineData("recursive-delete", true, 1706UL)]
    [InlineData("recursive-delete", false, 1707UL)]
    [Catalog("NS-017", "S", "INV-02", "INV-06", "INV-07")]
    public async Task NS_017_DirectoryMutationHasRetryIdentity(string operation, bool dropEveryReply, ulong seed)
    {
        await ClusterTest.RunAsync(Sim(seed), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(Named("c1"), Ct);
            await using DfsClient other = await cluster.ConnectAsync(Named("c2"), Ct);
            DfsVolume v = client.GetVolume(Vol);
            DfsVolume o = other.GetVolume(Vol);
            DfsOperationId k = await v.NewOperationIdAsync(Ct);
            var withK = new DfsMutationOptions { OperationId = k };
            DfsOperationKind kind = operation switch
            {
                "mkdir" => DfsOperationKind.CreateDirectory,
                "recursive-delete" => DfsOperationKind.Delete,
                _ => DfsOperationKind.Move,
            };

            // Setup.
            InodeId? originalInode = null;
            switch (operation)
            {
                case "rename-directory":
                    await o.Directories.CreateDirectoryAsync("/r1/child", Ct);
                    originalInode = (await o.Directories.GetInfoAsync("/r1", Ct)).InodeId;
                    break;
                case "rename-file":
                    await o.Files.WriteAllBytesAsync("/r1", TestData.Bytes(seed, 300), Ct);
                    originalInode = (await o.Files.GetInfoAsync("/r1", Ct)).InodeId;
                    break;
                case "recursive-delete":
                    await o.Directories.CreateDirectoryAsync("/t/a", Ct);
                    await o.Files.WriteAllBytesAsync("/t/a/b.bin", TestData.Bytes(seed, 300), Ct);
                    originalInode = (await o.Directories.GetInfoAsync("/t", Ct)).InodeId;
                    break;
            }

            FaultArm arm = cluster.Faults.Arm(
                new FaultMatch(FaultSites.RpcReplySend, Operation: kind, Count: dropEveryReply ? Always : 1), FaultAction.DropReply());

            // "Every reply lost" includes the replies to the outcome queries the SDK sends to resolve the mutation itself
            // (PROTOCOL.md §5.1); otherwise it learns the committed outcome and correctly reports success.
            MessageRule? lostQueries = dropEveryReply
                ? cluster.Network.DropReplies(new MessageFilter(From: "client:c1", Type: MessageType.QueryOperation), count: 10_000)
                : null;

            Task Mutate() => operation switch
            {
                "mkdir" => v.Directories.CreateDirectoryAsync("/m", withK, Ct),
                "rename-directory" => v.Directories.MoveAsync("/r1", "/r2", withK, Ct),
                "rename-file" => v.Files.MoveAsync("/r1", "/r2", false, withK, Ct),
                _ => v.Directories.DeleteAsync("/t", true, withK, Ct),
            };

            Exception? first = await Record.ExceptionAsync(Mutate);
            cluster.Faults.AssertReached(arm);
            cluster.Faults.DisarmAll();
            lostQueries?.Cancel();
            if (dropEveryReply)
            {
                Assert.True(first is DfsCommitOutcomeUnknownException, $"with every reply lost the outcome is unknown, got {first?.ToString() ?? "success"}");
                Assert.Equal(k, ((DfsCommitOutcomeUnknownException)first!).OperationId);
            }
            else
            {
                Assert.True(first is null or DfsCommitOutcomeUnknownException, $"a single lost reply is retried with the same id, got {first}");
            }

            DfsOperationOutcome outcome = await o.QueryOperationAsync(k, resolve: false, Ct);
            Assert.Equal(DfsOutcomeStatus.Committed, outcome.Status);
            if (operation == "mkdir")
                originalInode = (await o.Directories.GetInfoAsync("/m", Ct)).InodeId;

            // Someone else reuses the names before the retry arrives.
            InodeId recreated;
            byte[] recreatedBytes = TestData.Bytes(seed + 9, 123);
            switch (operation)
            {
                case "mkdir":
                    await o.Directories.DeleteAsync("/m", false, Ct);
                    await o.Files.WriteAllBytesAsync("/m", recreatedBytes, Ct);
                    recreated = (await o.Files.GetInfoAsync("/m", Ct)).InodeId;
                    break;
                case "rename-directory":
                    recreated = (await o.Directories.CreateDirectoryAsync("/r1", Ct)).InodeId;
                    break;
                case "rename-file":
                    await o.Files.WriteAllBytesAsync("/r1", recreatedBytes, Ct);
                    recreated = (await o.Files.GetInfoAsync("/r1", Ct)).InodeId;
                    break;
                default:
                    Assert.False(await o.Directories.ExistsAsync("/t", Ct));
                    await o.Directories.CreateDirectoryAsync("/t", Ct);
                    await o.Files.WriteAllBytesAsync("/t/new.bin", recreatedBytes, Ct);
                    recreated = (await o.Directories.GetInfoAsync("/t", Ct)).InodeId;
                    break;
            }

            Assert.NotEqual(originalInode, recreated);

            // Retrying K reports the original outcome and does not touch the recreated names.
            for (int attempt = 0; attempt < 2; attempt++)
            {
                switch (operation)
                {
                    case "mkdir":
                    {
                        DfsDirectoryInfo info = await v.Directories.CreateDirectoryAsync("/m", withK, Ct);
                        Assert.Equal(originalInode, info.InodeId);
                        Assert.Equal(recreated, (await o.Files.GetInfoAsync("/m", Ct)).InodeId);
                        AssertBytes(recreatedBytes, await o.Files.ReadAllBytesAsync("/m", Ct), "the file recreated at /m is untouched");
                        break;
                    }

                    case "rename-directory":
                        Assert.Equal(outcome.Token, await v.Directories.MoveAsync("/r1", "/r2", withK, Ct));
                        Assert.Equal(recreated, (await o.Directories.GetInfoAsync("/r1", Ct)).InodeId);
                        Assert.Equal(originalInode, (await o.Directories.GetInfoAsync("/r2", Ct)).InodeId);
                        Assert.True(await o.Directories.ExistsAsync("/r2/child", Ct));
                        break;
                    case "rename-file":
                        Assert.Equal(outcome.Token, await v.Files.MoveAsync("/r1", "/r2", false, withK, Ct));
                        Assert.Equal(recreated, (await o.Files.GetInfoAsync("/r1", Ct)).InodeId);
                        Assert.Equal(originalInode, (await o.Files.GetInfoAsync("/r2", Ct)).InodeId);
                        break;
                    default:
                        Assert.Equal(outcome.Token, await v.Directories.DeleteAsync("/t", true, withK, Ct));
                        Assert.Equal(recreated, (await o.Directories.GetInfoAsync("/t", Ct)).InodeId);
                        AssertBytes(recreatedBytes, await o.Files.ReadAllBytesAsync("/t/new.bin", Ct), "the recreated subtree is untouched");
                        break;
                }
            }

            // The same id with different arguments is rejected.
            await AssertEx.ThrowsDfsAsync<DfsOperationIdMismatchException>(() => operation switch
            {
                "mkdir" => v.Directories.CreateDirectoryAsync("/m-other", withK, Ct),
                "rename-directory" => v.Directories.MoveAsync("/r1", "/r3", withK, Ct),
                "rename-file" => v.Files.MoveAsync("/r1", "/r3", false, withK, Ct),
                _ => v.Directories.DeleteAsync("/t-other", true, withK, Ct),
            }, DfsErrorCode.OperationIdMismatch);
            Assert.False(await o.Directories.ExistsAsync("/m-other", Ct));
            Assert.False(await o.Directories.ExistsAsync("/r3", Ct));
            Assert.False(await o.Files.ExistsAsync("/r3", Ct));
        }, Ct);
    }
}
