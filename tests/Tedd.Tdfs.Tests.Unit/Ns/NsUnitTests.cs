using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;
using static Tedd.Tdfs.Tests.Unit.Ns.NsHelpers;

namespace Tedd.Tdfs.Tests.Unit.Ns;

/// <summary>
/// NS suite, unit level: directories, renames and namespace metadata (test plan §7.8; FSD §4 NS-01–NS-07, §§5, 8).
/// </summary>
public sealed class NsUnitTests
{
    // ------------------------------------------------------------------------------------------------------------
    // NS-001 RecursiveCreateIsIdempotent
    // ------------------------------------------------------------------------------------------------------------

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(5)]
    [InlineData(12)]
    [Catalog("NS-001", "U", "INV-02", "INV-07")]
    public async Task NS_001_RecursiveCreateIsIdempotent(int depth)
    {
        await ClusterTest.RunAsync(TestClusterOptions.ThreeZones, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(Named("c0"), Ct);
            DfsVolume v = client.GetVolume(Vol);
            var prefixes = new List<string> { "/ns001" };
            for (int i = 0; i < depth; i++)
                prefixes.Add($"{prefixes[^1]}/d{i}");
            string leaf = prefixes[^1];
            int inodesBefore = await InodeCountAsync(cluster);

            DfsDirectoryInfo first = await v.Directories.CreateDirectoryAsync(leaf, Ct);
            Assert.Equal(leaf, first.Path);
            Assert.Equal(0, first.EntryCount);
            Assert.Equal(inodesBefore + prefixes.Count, await InodeCountAsync(cluster));
            var infos = new Dictionary<string, DfsDirectoryInfo>(StringComparer.Ordinal);
            foreach (string p in prefixes)
            {
                Assert.True(await v.Directories.ExistsAsync(p, Ct), $"recursive create must create ancestor {p}");
                Assert.False(await v.Files.ExistsAsync(p, Ct));
                infos[p] = await v.Directories.GetInfoAsync(p, Ct);
            }

            Assert.Equal(prefixes.Count, infos.Values.Select(i => i.InodeId).Distinct().Count());

            // Second creation: same inode, nothing modified, nothing allocated.
            DfsDirectoryInfo second = await v.Directories.CreateDirectoryAsync(leaf, Ct);
            Assert.Equal(first.InodeId, second.InodeId);
            foreach (string p in prefixes)
            {
                DfsDirectoryInfo again = await v.Directories.GetInfoAsync(p, Ct);
                Assert.Equal(infos[p].InodeId, again.InodeId);
                Assert.Equal(infos[p].Revision, again.Revision);
                Assert.Equal(infos[p].EntryCount, again.EntryCount);
                Assert.Equal(infos[p].CreatedUtc, again.CreatedUtc);
                Assert.Equal(infos[p].InodeId, (await v.Directories.CreateDirectoryAsync(p, Ct)).InodeId);
            }

            Assert.Equal(inodesBefore + prefixes.Count, await InodeCountAsync(cluster));

            // Root is an existing directory: idempotent.
            DfsDirectoryInfo root = await v.Directories.CreateDirectoryAsync("/", Ct);
            Assert.Equal(InodeId.Root, root.InodeId);

            // Normalization-equivalent spellings identify the same directory.
            DfsDirectoryInfo nfc = await v.Directories.CreateDirectoryAsync(leaf + "/café", Ct);
            DfsDirectoryInfo nfd = await v.Directories.CreateDirectoryAsync(leaf + "/café", Ct);
            Assert.Equal(nfc.InodeId, nfd.InodeId);
            Assert.Equal(leaf + "/café", nfd.Path);

            // Extending the path creates only the missing components; existing ancestors keep identity.
            DfsDirectoryInfo extended = await v.Directories.CreateDirectoryAsync(leaf + "/x/y", Ct);
            Assert.Equal(leaf + "/x/y", extended.Path);
            foreach (string p in prefixes)
                Assert.Equal(infos[p].InodeId, (await v.Directories.GetInfoAsync(p, Ct)).InodeId);
            List<DfsEntryInfo> leafEntries = await ToListAsync(v.Directories.EnumerateAsync(leaf, null, Ct));
            Assert.Equal(new[] { "café", "x" }, leafEntries.Select(e => e.Name).Order(StringComparer.Ordinal).ToArray());
            Assert.Equal(inodesBefore + prefixes.Count + 3, await InodeCountAsync(cluster));
        }, Ct);
    }

    [Theory]
    [InlineData("target-is-file")]
    [InlineData("parent-is-file")]
    [InlineData("grandparent-is-file")]
    [InlineData("top-level-file")]
    [Catalog("NS-001", "U", "INV-02", "INV-07")]
    public async Task NS_001_RecursiveCreateIsIdempotent_IntermediateFileConflict(string kind)
    {
        await ClusterTest.RunAsync(TestClusterOptions.ThreeZones, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(Named("c0"), Ct);
            DfsVolume v = client.GetVolume(Vol);
            await v.Directories.CreateDirectoryAsync("/ns001c/a", Ct);
            string filePath = kind == "top-level-file" ? "/ns001c-file" : "/ns001c/a/f";
            byte[] content = TestData.Bytes(0x0101, 777);
            await v.Files.WriteAllBytesAsync(filePath, content, Ct);
            DfsFileInfo fileBefore = await v.Files.GetInfoAsync(filePath, Ct);
            string target = kind switch
            {
                "target-is-file" => filePath,
                "parent-is-file" => filePath + "/g",
                "grandparent-is-file" => filePath + "/g/h",
                _ => filePath + "/x/y",
            };
            SortedDictionary<string, string> treeBefore = await SnapshotTreeAsync(v, "/");
            int inodesBefore = await InodeCountAsync(cluster);

            for (int attempt = 0; attempt < 2; attempt++)
            {
                Exception? failure = await Record.ExceptionAsync(() => v.Directories.CreateDirectoryAsync(target, Ct));
                Assert.True(failure is DfsException { ErrorCode: DfsErrorCode.AlreadyExists or DfsErrorCode.NotADirectory },
                    $"CreateDirectory({target}) through a file must be rejected as AlreadyExists/NotADirectory, got {failure?.ToString() ?? "success"}");
            }

            AssertTreeUnchanged(treeBefore, await SnapshotTreeAsync(v, "/"), "rejected recursive create");
            Assert.Equal(inodesBefore, await InodeCountAsync(cluster));
            DfsFileInfo fileAfter = await v.Files.GetInfoAsync(filePath, Ct);
            Assert.Equal(fileBefore.InodeId, fileAfter.InodeId);
            Assert.Equal(fileBefore.Revision, fileAfter.Revision);
            AssertBytes(content, await v.Files.ReadAllBytesAsync(filePath, Ct), "the conflicting file is untouched");
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // NS-003 NonrecursiveDeleteRejectsNonempty
    // ------------------------------------------------------------------------------------------------------------

    [Theory]
    [InlineData("one-file")]
    [InlineData("only-empty-subdirectory")]
    [InlineData("many-entries")]
    [InlineData("deep-tree")]
    [InlineData("root")]
    [Catalog("NS-003", "U", "INV-02", "INV-07")]
    public async Task NS_003_NonrecursiveDeleteRejectsNonempty(string shape)
    {
        await ClusterTest.RunAsync(TestClusterOptions.ThreeZones, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(Named("c0"), Ct);
            DfsVolume v = client.GetVolume(Vol);
            string target = shape == "root" ? "/" : "/ns003";
            await v.Directories.CreateDirectoryAsync("/ns003", Ct);
            switch (shape)
            {
                case "one-file":
                    await v.Files.WriteAllBytesAsync("/ns003/only.bin", TestData.Bytes(0x0301, 2 * Chunk + 1), Ct);
                    break;
                case "only-empty-subdirectory":
                    await v.Directories.CreateDirectoryAsync("/ns003/empty", Ct);
                    break;
                case "many-entries":
                    await BuildTreeAsync(v, "/ns003", directories: 4, filesPerDirectory: 6, seed: 0x0310);
                    for (int i = 0; i < 20; i++)
                        await v.Files.WriteAllBytesAsync($"/ns003/top{i}.bin", TestData.Bytes((ulong)(0x0330 + i), 64 + i), Ct);
                    break;
                case "deep-tree":
                    await v.Directories.CreateDirectoryAsync("/ns003/a/b/c/d/e/f", Ct);
                    await v.Files.WriteAllBytesAsync("/ns003/a/b/c/d/e/f/leaf.bin", TestData.Bytes(0x0340, 5000), Ct);
                    break;
                case "root":
                    await v.Files.WriteAllBytesAsync("/ns003/x.bin", TestData.Bytes(0x0350, 10), Ct);
                    break;
            }

            SortedDictionary<string, string> before = await SnapshotTreeAsync(v, "/");
            int inodesBefore = await InodeCountAsync(cluster);

            Exception? failure = await Record.ExceptionAsync(() => v.Directories.DeleteAsync(target, false, Ct));
            if (shape == "root")
                Assert.True(failure is DfsDirectoryNotEmptyException or ArgumentException or DfsException { ErrorCode: DfsErrorCode.InvalidPath or DfsErrorCode.InvalidArgument or DfsErrorCode.AccessDenied },
                    $"non-recursive delete of a nonempty root must be rejected, got {failure?.ToString() ?? "success"}");
            else
                Assert.True(failure is DfsDirectoryNotEmptyException { ErrorCode: DfsErrorCode.DirectoryNotEmpty },
                    $"non-recursive delete of a nonempty directory must throw DfsDirectoryNotEmptyException, got {failure?.ToString() ?? "success"}");

            AssertTreeUnchanged(before, await SnapshotTreeAsync(v, "/"), "rejected non-recursive delete");
            Assert.Equal(inodesBefore, await InodeCountAsync(cluster));

            // The same rejection inside a transaction leaves the tree unchanged whatever happens at commit.
            if (shape != "root")
            {
                await using (DfsTransaction tx = await v.Transactions.BeginAsync(null, Ct))
                {
                    Exception? inTx = await Record.ExceptionAsync(async () =>
                    {
                        await tx.Directories.DeleteAsync(target, false, Ct);
                        await tx.CommitAsync(Ct);
                    });
                    Assert.True(inTx is DfsDirectoryNotEmptyException, $"transactional non-recursive delete must be rejected, got {inTx?.ToString() ?? "success"}");
                }

                AssertTreeUnchanged(before, await SnapshotTreeAsync(v, "/"), "rejected transactional non-recursive delete");
            }

            // Controls: an empty directory is deletable non-recursively; a missing one is DirectoryNotFound.
            await v.Directories.CreateDirectoryAsync("/ns003-empty", Ct);
            CommitToken deleted = await v.Directories.DeleteAsync("/ns003-empty", false, Ct);
            Assert.False(deleted.IsNone);
            Assert.False(await v.Directories.ExistsAsync("/ns003-empty", Ct));
            await Assert.ThrowsAnyAsync<DirectoryNotFoundException>(() => v.Directories.DeleteAsync("/ns003-empty", false, Ct));
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // NS-007 RejectDirectoryCycles
    // ------------------------------------------------------------------------------------------------------------

    [Theory]
    [InlineData("/a", "/a/b", false)]
    [InlineData("/a", "/a/new", false)]
    [InlineData("/a", "/a/b/c/new", false)]
    [InlineData("/a/b", "/a/b/c/new", false)]
    [InlineData("/a", "/a/b/c", false)]
    [InlineData("/a", "/a", true)]
    [InlineData("/a/b", "/a/b", true)]
    [InlineData("/a", "/a/b/café/new", false)]
    [Catalog("NS-007", "U", "INV-02", "INV-07")]
    public async Task NS_007_RejectDirectoryCycles(string source, string destination, bool isSelf)
    {
        await ClusterTest.RunAsync(TestClusterOptions.ThreeZones, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(Named("c0"), Ct);
            DfsVolume v = client.GetVolume(Vol);
            await v.Directories.CreateDirectoryAsync("/a/b/c", Ct);
            await v.Directories.CreateDirectoryAsync("/a/b/café", Ct);
            await v.Files.WriteAllBytesAsync("/a/b/c/file.bin", TestData.Bytes(0x0701, 1234), Ct);
            await v.Files.WriteAllBytesAsync("/a/top.bin", TestData.Bytes(0x0702, 99), Ct);
            SortedDictionary<string, string> before = await SnapshotTreeAsync(v, "/");
            int inodesBefore = await InodeCountAsync(cluster);

            Exception? failure = await Record.ExceptionAsync(() => v.Directories.MoveAsync(source, destination, Ct));
            if (!isSelf || failure is not null)
            {
                Assert.True(failure is IOException or ArgumentException,
                    $"moving {source} into {destination} must be rejected, got {failure?.ToString() ?? "success"}");
                Assert.False(failure is FileNotFoundException or DirectoryNotFoundException, "a cycle is not an absence");
            }

            AssertTreeUnchanged(before, await SnapshotTreeAsync(v, "/"), $"Move({source} -> {destination})");
            Assert.Equal(inodesBefore, await InodeCountAsync(cluster));

            // Same attempt through a transaction: rejected at the operation or at commit, never applied.
            await using (DfsTransaction tx = await v.Transactions.BeginAsync(null, Ct))
            {
                Exception? inTx = await Record.ExceptionAsync(async () =>
                {
                    await tx.Directories.MoveAsync(source, destination, Ct);
                    await tx.CommitAsync(Ct);
                });
                if (!isSelf)
                    Assert.True(inTx is not null, "a transactional cyclic move must be rejected");
            }

            AssertTreeUnchanged(before, await SnapshotTreeAsync(v, "/"), $"transactional Move({source} -> {destination})");
        }, Ct);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [Catalog("NS-007", "U", "INV-02", "INV-07")]
    public async Task NS_007_RejectDirectoryCycles_ConcurrentCrossMoves(int round)
    {
        await ClusterTest.RunAsync(TestClusterOptions.ThreeZones with { Seed = (ulong)(700 + round) }, async cluster =>
        {
            await using DfsClient c1 = await cluster.ConnectAsync(Named("c1"), Ct);
            await using DfsClient c2 = await cluster.ConnectAsync(Named("c2"), Ct);
            DfsVolume v1 = c1.GetVolume(Vol);
            DfsVolume v2 = c2.GetVolume(Vol);
            for (int i = 0; i < 8; i++)
            {
                string a = $"/r{round}-{i}-A";
                string b = $"/r{round}-{i}-B";
                InodeId aId = (await v1.Directories.CreateDirectoryAsync(a + "/inner", Ct)).InodeId;
                aId = (await v1.Directories.GetInfoAsync(a, Ct)).InodeId;
                InodeId bId = (await v1.Directories.CreateDirectoryAsync(b, Ct)).InodeId;

                // Each move is individually valid; together they would form a cycle A -> B -> A.
                Task<Exception?> m1 = Record.ExceptionAsync(() => v1.Directories.MoveAsync(a, b + "/A", Ct)).AsTask();
                Task<Exception?> m2 = Record.ExceptionAsync(() => v2.Directories.MoveAsync(b, a + "/B", Ct)).AsTask();
                Exception?[] results = await Task.WhenAll(m1, m2);
                bool firstWon = results[0] is null;
                bool secondWon = results[1] is null;
                Assert.False(firstWon && secondWon, "both cross moves committed: the namespace now contains a cycle");
                Assert.True(firstWon || secondWon, $"the move ordered first is valid and must commit; got {results[0]} / {results[1]}");
                // Once the winner commits, the loser's destination parent no longer exists at the path it named (after
                // A -> B/A the path ".../A/B" is gone), so DirectoryNotFound is a legitimate explicit rejection here, as is
                // a cycle error from an implementation that resolved inodes up front. Silent success is never allowed.
                Exception? loser = firstWon ? results[1] : results[0];
                Assert.True(loser is IOException or ArgumentException, $"the move that would close the cycle must be rejected explicitly, got {loser}");

                // Both directories are still reachable from the root exactly once.
                var reachable = new List<InodeId>();
                async Task WalkAsync(string dir)
                {
                    foreach (DfsEntryInfo e in await ToListAsync(v1.Directories.EnumerateAsync(dir, null, Ct)))
                    {
                        if (e.Kind != DfsEntryKind.Directory)
                            continue;
                        reachable.Add(e.InodeId);
                        await WalkAsync(e.Path);
                    }
                }

                await WalkAsync("/");
                Assert.Equal(1, reachable.Count(id => id == aId));
                Assert.Equal(1, reachable.Count(id => id == bId));
                if (firstWon)
                    Assert.Equal(aId, (await v1.Directories.GetInfoAsync(b + "/A", Ct)).InodeId);
                if (secondWon)
                    Assert.Equal(bId, (await v1.Directories.GetInfoAsync(a + "/B", Ct)).InodeId);
            }
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // NS-009 CrossVolumeMoveRejected
    // ------------------------------------------------------------------------------------------------------------

    private static TestClusterOptions TwoVolumes => TestClusterOptions.ThreeZones with
    {
        Volumes = [new TestVolumeSpec("vol"), new TestVolumeSpec("vol2")],
    };

    [Theory]
    [InlineData("/../vol2/x.bin")]
    [InlineData("vol2:/x.bin")]
    [InlineData("//vol2/x.bin")]
    [InlineData("dfs://cluster/vol2/x.bin")]
    [InlineData("\\vol2\\x.bin")]
    [InlineData("/vol2/../../x.bin")]
    [Catalog("NS-009", "U", "INV-02", "INV-08", "INV-13")]
    public async Task NS_009_CrossVolumeMoveRejected(string destinationSpelling)
    {
        await ClusterTest.RunAsync(TwoVolumes, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(Named("c0"), Ct);
            DfsVolume v = client.GetVolume("vol");
            DfsVolume v2 = client.GetVolume("vol2");
            byte[] content = TestData.Bytes(0x0901, 3 * Chunk);
            await v.Directories.CreateDirectoryAsync("/ns009/sub", Ct);
            await v.Files.WriteAllBytesAsync("/ns009/src.bin", content, Ct);
            SortedDictionary<string, string> volBefore = await SnapshotTreeAsync(v, "/");
            SortedDictionary<string, string> vol2Before = await SnapshotTreeAsync(v2, "/");

            static void AssertExplicitRejection(Exception? failure, string what) =>
                Assert.True(failure is ArgumentException or DfsCrossVolumeException { ErrorCode: DfsErrorCode.CrossVolume },
                    $"{what} must fail explicitly (invalid path or cross-volume), got {failure?.ToString() ?? "success"}");

            AssertExplicitRejection(await Record.ExceptionAsync(() => v.Files.MoveAsync("/ns009/src.bin", destinationSpelling, false, Ct)), "file move");
            AssertExplicitRejection(await Record.ExceptionAsync(() => v.Files.MoveAsync("/ns009/src.bin", destinationSpelling, true, Ct)), "overwriting file move");
            AssertExplicitRejection(await Record.ExceptionAsync(() => v.Directories.MoveAsync("/ns009/sub", destinationSpelling, Ct)), "directory move");
            await using (DfsTransaction tx = await v.Transactions.BeginAsync(null, Ct))
            {
                AssertExplicitRejection(await Record.ExceptionAsync(async () =>
                {
                    await tx.Files.MoveAsync("/ns009/src.bin", destinationSpelling, false, Ct);
                    await tx.CommitAsync(Ct);
                }), "transactional move");
            }

            // No hidden copy-delete: the source is intact and the other volume is untouched.
            AssertTreeUnchanged(volBefore, await SnapshotTreeAsync(v, "/"), "source volume after rejected cross-volume move");
            AssertTreeUnchanged(vol2Before, await SnapshotTreeAsync(v2, "/"), "destination volume after rejected cross-volume move");
            AssertBytes(content, await v.Files.ReadAllBytesAsync("/ns009/src.bin", Ct));
        }, Ct);
    }

    [Fact]
    [Catalog("NS-009", "U", "INV-02", "INV-08", "INV-13")]
    public async Task NS_009_CrossVolumeMoveRejected_PathsNeverAddressAnotherVolume()
    {
        await ClusterTest.RunAsync(TwoVolumes, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(Named("c0"), Ct);
            DfsVolume v = client.GetVolume("vol");
            DfsVolume v2 = client.GetVolume("vol2");
            byte[] content = TestData.Bytes(0x0911, 999);
            await v.Files.WriteAllBytesAsync("/src.bin", content, Ct);
            InodeId inode = (await v.Files.GetInfoAsync("/src.bin", Ct)).InodeId;
            await v.Directories.CreateDirectoryAsync("/vol2", Ct);

            // "/vol2/x.bin" is an ordinary path inside "vol": the move is local and atomic.
            CommitToken token = await v.Files.MoveAsync("/src.bin", "/vol2/x.bin", false, Ct);
            Assert.False(token.IsNone);
            Assert.Equal(inode, (await v.Files.GetInfoAsync("/vol2/x.bin", Ct)).InodeId);
            Assert.False(await v2.Files.ExistsAsync("/x.bin", Ct));
            Assert.False(await v2.Files.ExistsAsync("/vol2/x.bin", Ct));
            Assert.Empty(await ToListAsync(v2.Directories.EnumerateAsync("/", null, Ct)));

            // A raw request that tries to escape the volume is rejected by the server, and a request on vol2 never
            // resolves vol's namespace.
            await using RawProtocolClient raw = await cluster.ConnectRawAsync(cluster[0].Name, Ct);
            Assert.Equal(DfsErrorCode.None, await raw.AuthenticateAsync(cluster.AdminCredentials));
            DfsErrorCode escape = await raw.SendPathOperationAsync("vol", DfsOperationKind.Move, "/vol2/x.bin", "/../vol2/x.bin");
            Assert.True(escape is DfsErrorCode.InvalidPath or DfsErrorCode.CrossVolume or DfsErrorCode.InvalidArgument, $"raw escape move returned {escape}");
            DfsErrorCode wrongVolume = await raw.SendPathOperationAsync("vol2", DfsOperationKind.Move, "/vol2/x.bin", "/x.bin");
            Assert.True(wrongVolume is DfsErrorCode.FileNotFound or DfsErrorCode.DirectoryNotFound,
                $"a request on vol2 must resolve only vol2's namespace, got {wrongVolume}");
            Assert.Equal(inode, (await v.Files.GetInfoAsync("/vol2/x.bin", Ct)).InodeId);
            Assert.Empty(await ToListAsync(v2.Directories.EnumerateAsync("/", null, Ct)));
        }, Ct);
    }

    [Fact]
    [Catalog("NS-009", "U", "INV-02", "INV-13")]
    public async Task NS_009_CrossVolumeMoveRejected_ExplicitCopyDeleteHasSeparateIdentities()
    {
        await ClusterTest.RunAsync(TwoVolumes, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(Named("c0"), Ct);
            DfsVolume v = client.GetVolume("vol");
            DfsVolume v2 = client.GetVolume("vol2");
            byte[] content = TestData.Bytes(0x0921, 2 * Chunk + 3);
            await v.Files.WriteAllBytesAsync("/move-me.bin", content, Ct);

            // The documented workaround: separately identified copy then delete, no atomicity claim.
            DfsOperationId writeId = await v2.NewOperationIdAsync(Ct);
            DfsOperationId deleteId = await v.NewOperationIdAsync(Ct);
            byte[] read = await v.Files.ReadAllBytesAsync("/move-me.bin", Ct);
            CommitToken written = await v2.Files.WriteAllBytesAsync("/moved.bin", read, new DfsMutationOptions { OperationId = writeId }, Ct);
            CommitToken deleted = await v.Files.DeleteAsync("/move-me.bin", new DfsMutationOptions { OperationId = deleteId }, Ct);
            AssertBytes(content, await v2.Files.ReadAllBytesAsync("/moved.bin", Ct));
            Assert.False(await v.Files.ExistsAsync("/move-me.bin", Ct));

            // Tokens of different volumes are not globally ordered.
            Assert.NotEqual(written.VolumeId, deleted.VolumeId);
            Assert.False(written.IsComparableWith(deleted));
            Assert.Throws<ArgumentException>(() => written.CompareTo(deleted));
            Assert.Equal(DfsOutcomeStatus.Committed, (await v2.QueryOperationAsync(writeId, false, Ct)).Status);
            Assert.Equal(DfsOutcomeStatus.Committed, (await v.QueryOperationAsync(deleteId, false, Ct)).Status);
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // NS-013 ContinuationIsBoundToQuery
    // ------------------------------------------------------------------------------------------------------------

    [Theory]
    [InlineData("different-directory")]
    [InlineData("different-pattern")]
    [InlineData("different-kinds")]
    [InlineData("different-volume")]
    [InlineData("different-principal")]
    [InlineData("tampered-token")]
    [InlineData("truncated-token")]
    [Catalog("NS-013", "U", "INV-03", "INV-07", "INV-12")]
    public async Task NS_013_ContinuationIsBoundToQuery(string reuse)
    {
        await ClusterTest.RunAsync(TwoVolumes, async cluster =>
        {
            await using DfsClient admin = await cluster.ConnectAsync(Named("admin"), Ct);
            DfsVolume v = admin.GetVolume("vol");
            DfsVolume v2 = admin.GetVolume("vol2");
            await v.Directories.CreateDirectoryAsync("/d1", Ct);
            await v.Directories.CreateDirectoryAsync("/d2", Ct);
            await v2.Directories.CreateDirectoryAsync("/d1", Ct);
            for (int i = 0; i < 10; i++)
            {
                await v.Files.WriteAllBytesAsync($"/d1/a{i:D2}.bin", TestData.Bytes((ulong)i, 10), Ct);
                await v.Files.WriteAllBytesAsync($"/d2/z{i:D2}.bin", TestData.Bytes((ulong)(100 + i), 10), Ct);
                await v2.Files.WriteAllBytesAsync($"/d1/q{i:D2}.bin", TestData.Bytes((ulong)(200 + i), 10), Ct);
            }

            var options = new DfsEnumerationOptions { PageSize = 3 };
            DfsDirectoryPage first = await v.Directories.ListPageAsync("/d1", options, null, Ct);
            Assert.Equal(3, first.Entries.Count);
            Assert.NotNull(first.ContinuationToken);
            string token = first.ContinuationToken!;

            Func<Task<DfsDirectoryPage>> misuse;
            bool principalCase = false;
            await using DfsClient? other = reuse == "different-principal"
                ? await cluster.ConnectAsync(Named("bob", (await cluster.CreatePrincipalAsync("bob", Grant(DfsPermissions.Read))).Credentials), Ct)
                : null;
            switch (reuse)
            {
                case "different-directory":
                    misuse = () => v.Directories.ListPageAsync("/d2", options, token, Ct);
                    break;
                case "different-pattern":
                    misuse = () => v.Directories.ListPageAsync("/d1", options with { Pattern = "a0*" }, token, Ct);
                    break;
                case "different-kinds":
                    misuse = () => v.Directories.ListPageAsync("/d1", options with { Kinds = DfsEntryKinds.Directories }, token, Ct);
                    break;
                case "different-volume":
                    misuse = () => v2.Directories.ListPageAsync("/d1", options, token, Ct);
                    break;
                case "different-principal":
                    principalCase = true;
                    misuse = () => other!.GetVolume("vol").Directories.ListPageAsync("/d1", options, token, Ct);
                    break;
                case "tampered-token":
                    char[] chars = token.ToCharArray();
                    int mid = chars.Length / 2;
                    chars[mid] = chars[mid] == 'A' ? 'B' : 'A';
                    string tampered = new(chars);
                    misuse = () => v.Directories.ListPageAsync("/d1", options, tampered, Ct);
                    break;
                default:
                    string truncated = token[..(token.Length / 2)];
                    misuse = () => v.Directories.ListPageAsync("/d1", options, truncated, Ct);
                    break;
            }

            DfsDirectoryPage? misusePage = null;
            Exception? failure = await Record.ExceptionAsync(async () => misusePage = await misuse());
            bool rejected = failure is ArgumentException
                or DfsPinExpiredException { ErrorCode: DfsErrorCode.ContinuationExpired }
                or DfsException { ErrorCode: DfsErrorCode.InvalidArgument }
                || (principalCase && failure is UnauthorizedAccessException);
            if (reuse is "tampered-token" or "truncated-token" && failure is null)
            {
                // A corrupted token presented for the SAME query (same volume, directory, filter and principal) is not
                // one of the catalog's reuse cases; the FSD does not require integrity-protected tokens. If it still
                // decodes, it may only continue this query's own pinned snapshot, never another query's state.
                Assert.NotNull(misusePage);
                Assert.Equal(first.PinnedAt, misusePage.PinnedAt);
                Assert.All(misusePage.Entries, e => Assert.True(
                    e.Path.StartsWith("/d1/", StringComparison.Ordinal) && e.Name.StartsWith('a'),
                    $"a corrupted continuation token returned an entry of another query: {e.Path}"));
            }
            else
            {
                Assert.True(rejected,
                    $"reusing a continuation token for another query ({reuse}) must be rejected, got {failure?.ToString() ?? "a page"}");
            }

            // The original query is unaffected and completes with exactly its own entries.
            var names = first.Entries.Select(e => e.Name).ToList();
            string? next = token;
            while (next is not null)
            {
                DfsDirectoryPage page = await v.Directories.ListPageAsync("/d1", options, next, Ct);
                Assert.Equal(first.PinnedAt, page.PinnedAt);
                names.AddRange(page.Entries.Select(e => e.Name));
                next = page.ContinuationToken;
            }

            Assert.Equal(Enumerable.Range(0, 10).Select(i => $"a{i:D2}.bin").ToArray(), names.Order(StringComparer.Ordinal).ToArray());
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // NS-016 PolicyFollowsInodeAcrossRename
    // ------------------------------------------------------------------------------------------------------------

    private static readonly DfsStoragePolicy Silver = new()
    {
        TargetCopies = 2,
        TargetDistinctZones = 2,
        DurableCopiesBeforeAck = 2,
        DurableZonesBeforeAck = 2,
    };

    private static TestClusterOptions PolicyCluster => TestClusterOptions.ThreeZones with
    {
        Volumes = [new TestVolumeSpec("vol") { NamedPolicies = new Dictionary<string, DfsStoragePolicy>(StringComparer.Ordinal) { ["silver"] = Silver } }],
    };

    [Theory]
    [InlineData("named-into-default-directory")]
    [InlineData("default-into-named-directory")]
    [InlineData("overwrite-default-destination")]
    [InlineData("directory-subtree")]
    [InlineData("rename-then-rewrite")]
    [Catalog("NS-016", "U", "INV-07", "INV-08", "INV-11")]
    public async Task NS_016_PolicyFollowsInodeAcrossRename(string kind)
    {
        // The API has no per-directory default policies; "directories with different defaults" is modelled as
        // directories whose resident files use different policies (named "silver" = 2 copies vs volume default = 3).
        await ClusterTest.RunAsync(PolicyCluster, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(Named("c0"), Ct);
            DfsVolume v = client.GetVolume(Vol);
            var silverOptions = new DfsMutationOptions { StoragePolicy = "silver" };
            await v.Directories.CreateDirectoryAsync("/silver-dir", Ct);
            await v.Directories.CreateDirectoryAsync("/default-dir", Ct);
            await v.Files.WriteAllBytesAsync("/silver-dir/resident.bin", TestData.Bytes(0x1601, 2 * Chunk), silverOptions, Ct);
            await v.Files.WriteAllBytesAsync("/default-dir/resident.bin", TestData.Bytes(0x1602, 2 * Chunk), Ct);
            string defaultName = (await v.Files.GetInfoAsync("/default-dir/resident.bin", Ct)).StoragePolicy;
            Assert.NotEqual("silver", defaultName);
            Assert.Equal("silver", (await v.Files.GetInfoAsync("/silver-dir/resident.bin", Ct)).StoragePolicy);

            var moved = new List<(string Path, string Policy, int Copies)>();
            switch (kind)
            {
                case "named-into-default-directory":
                    await v.Files.WriteAllBytesAsync("/silver-dir/m.bin", TestData.Bytes(0x1610, 3 * Chunk), silverOptions, Ct);
                    await v.Files.MoveAsync("/silver-dir/m.bin", "/default-dir/m.bin", false, Ct);
                    moved.Add(("/default-dir/m.bin", "silver", 2));
                    break;
                case "default-into-named-directory":
                    await v.Files.WriteAllBytesAsync("/default-dir/m.bin", TestData.Bytes(0x1611, 3 * Chunk), Ct);
                    await v.Files.MoveAsync("/default-dir/m.bin", "/silver-dir/m.bin", false, Ct);
                    moved.Add(("/silver-dir/m.bin", defaultName, 3));
                    break;
                case "overwrite-default-destination":
                    await v.Files.WriteAllBytesAsync("/silver-dir/m.bin", TestData.Bytes(0x1612, 3 * Chunk), silverOptions, Ct);
                    await v.Files.WriteAllBytesAsync("/default-dir/m.bin", TestData.Bytes(0x1613, 3 * Chunk), Ct);
                    await v.Files.MoveAsync("/silver-dir/m.bin", "/default-dir/m.bin", true, Ct);
                    moved.Add(("/default-dir/m.bin", "silver", 2));
                    break;
                case "directory-subtree":
                    await v.Directories.CreateDirectoryAsync("/silver-dir/sub", Ct);
                    await v.Files.WriteAllBytesAsync("/silver-dir/sub/s.bin", TestData.Bytes(0x1614, 2 * Chunk), silverOptions, Ct);
                    await v.Files.WriteAllBytesAsync("/silver-dir/sub/d.bin", TestData.Bytes(0x1615, 2 * Chunk), Ct);
                    await v.Directories.MoveAsync("/silver-dir/sub", "/default-dir/sub", Ct);
                    moved.Add(("/default-dir/sub/s.bin", "silver", 2));
                    moved.Add(("/default-dir/sub/d.bin", defaultName, 3));
                    break;
                case "rename-then-rewrite":
                    await v.Files.WriteAllBytesAsync("/silver-dir/m.bin", TestData.Bytes(0x1616, 2 * Chunk), silverOptions, Ct);
                    await v.Files.MoveAsync("/silver-dir/m.bin", "/default-dir/m.bin", false, Ct);
                    await v.Files.WriteAllBytesAsync("/default-dir/m.bin", TestData.Bytes(0x1617, 3 * Chunk), Ct);
                    await v.Files.AppendAllBytesAsync("/default-dir/m.bin", TestData.Bytes(0x1618, Chunk), Ct);
                    moved.Add(("/default-dir/m.bin", "silver", 2));
                    break;
            }

            await cluster.WaitForRepairAsync(Vol, TimeSpan.FromSeconds(15), Ct);
            foreach ((string path, string policy, int copies) in moved)
            {
                DfsFileInfo info = await v.Files.GetInfoAsync(path, Ct);
                Assert.Equal(policy, info.StoragePolicy);
                FileInspection? inspected = await cluster.Inspect.GetFileAsync(Vol, path);
                Assert.NotNull(inspected);
                Assert.Equal(policy, inspected.Policy);
                Assert.NotEmpty(inspected.Extents);
                foreach (ExtentInspection extent in inspected.Extents)
                {
                    (int actualCopies, int zones) = await cluster.Inspect.CountDurableDomainsAsync(extent.Object);
                    Assert.Equal(copies, actualCopies);
                    Assert.Equal(copies, zones);
                }
            }

            // Residents of both directories keep their own policies too.
            Assert.Equal("silver", (await v.Files.GetInfoAsync("/silver-dir/resident.bin", Ct)).StoragePolicy);
            Assert.Equal(defaultName, (await v.Files.GetInfoAsync("/default-dir/resident.bin", Ct)).StoragePolicy);
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // NS-018 CreationMetadataIsConsistent
    // ------------------------------------------------------------------------------------------------------------

    [Fact]
    [Catalog("NS-018", "U", "INV-07", "INV-12")]
    public async Task NS_018_CreationMetadataIsConsistent()
    {
        await ClusterTest.RunAsync(PolicyCluster, async cluster =>
        {
            TestPrincipal alice = await cluster.CreatePrincipalAsync("alice", Grant(DfsPermissions.Read | DfsPermissions.Write));
            await using DfsClient aliceClient = await cluster.ConnectAsync(Named("alice", alice.Credentials), Ct);
            await using DfsClient adminClient = await cluster.ConnectAsync(Named("admin"), Ct);
            DfsVolume v = aliceClient.GetVolume(Vol);
            DfsVolume admin = adminClient.GetVolume(Vol);
            ulong maxInode = InodeId.Root.Value;
            CommitToken lastToken = CommitToken.None;

            void NewInode(InodeId id, string what)
            {
                Assert.True(id.Value > maxInode, $"{what}: inode ids must be allocated monotonically and never reused ({id.Value} <= {maxInode})");
                maxInode = id.Value;
            }

            void Observed(CommitToken observedAt, CommitToken token, string what)
            {
                Assert.False(observedAt.IsNone, $"{what}: metadata must carry the observed commit");
                Assert.True(observedAt.IsAtOrAfter(token), $"{what}: metadata read after a mutation must observe it");
            }

            // 1. Create directory.
            DfsDirectoryInfo d0 = await v.Directories.CreateDirectoryAsync("/ns018", Ct);
            NewInode(d0.InodeId, "mkdir");
            Assert.Equal("/ns018", d0.Path);
            Assert.Equal("alice", d0.Owner);
            Assert.Equal(0, d0.EntryCount);
            Assert.True(d0.ModifiedUtc >= d0.CreatedUtc);

            // 2. Create small (inline) file.
            byte[] small = TestData.Bytes(0x1801, 100);
            CommitToken t1 = await v.Files.WriteAllBytesAsync("/ns018/f", small, Ct);
            DfsFileInfo f1 = await v.Files.GetInfoAsync("/ns018/f", Ct);
            NewInode(f1.InodeId, "create file");
            Assert.Equal("/ns018/f", f1.Path);
            Assert.Equal(t1.Sequence, f1.Revision);
            Assert.Equal(100, f1.Length);
            Assert.Equal("alice", f1.Owner);
            Assert.Equal(new DfsInodeAccess(), f1.Access);
            Assert.False(string.IsNullOrEmpty(f1.StoragePolicy));
            Assert.True(f1.IsInline);
            Assert.False(f1.IsSparse);
            Assert.Equal(0, f1.ExtentCount);
            Assert.True(f1.ModifiedUtc >= f1.CreatedUtc);
            Observed(f1.ObservedAt, t1, "create file");
            DfsDirectoryInfo d1 = await v.Directories.GetInfoAsync("/ns018", Ct);
            Assert.Equal(d0.InodeId, d1.InodeId);
            Assert.Equal(1, d1.EntryCount);
            Assert.True(d1.Revision > d0.Revision, "adding an entry modifies the directory");
            Assert.Equal(d0.CreatedUtc, d1.CreatedUtc);
            lastToken = t1;

            // 3. Replace with a multi-extent file: identity and creation time stable.
            byte[] large = TestData.Bytes(0x1802, 3 * Chunk + 1);
            CommitToken t2 = await v.Files.WriteAllBytesAsync("/ns018/f", large, Ct);
            Assert.True(t2.CompareTo(lastToken) > 0);
            DfsFileInfo f2 = await v.Files.GetInfoAsync("/ns018/f", Ct);
            Assert.Equal(f1.InodeId, f2.InodeId);
            Assert.Equal(t2.Sequence, f2.Revision);
            Assert.Equal(large.Length, f2.Length);
            Assert.False(f2.IsInline);
            Assert.True(f2.ExtentCount >= 1);
            Assert.Equal(f1.CreatedUtc, f2.CreatedUtc);
            Assert.True(f2.ModifiedUtc >= f1.ModifiedUtc);
            Assert.Equal(f1.Owner, f2.Owner);
            Observed(f2.ObservedAt, t2, "replace");
            Assert.Equal(d1.Revision, (await v.Directories.GetInfoAsync("/ns018", Ct)).Revision);
            lastToken = t2;

            // 4. Append.
            CommitToken t3 = await v.Files.AppendAllBytesAsync("/ns018/f", TestData.Bytes(0x1803, 10), Ct);
            DfsFileInfo f3 = await v.Files.GetInfoAsync("/ns018/f", Ct);
            Assert.Equal(f1.InodeId, f3.InodeId);
            Assert.Equal(t3.Sequence, f3.Revision);
            Assert.Equal(large.Length + 10, f3.Length);
            Assert.Equal(f1.CreatedUtc, f3.CreatedUtc);
            Observed(f3.ObservedAt, t3, "append");

            // 5. Sparse extension through a stream.
            CommitToken t4;
            await using (DfsStream s = await v.Files.OpenAsync("/ns018/f", FileMode.Open, FileAccess.Write, FileShare.Read, Ct))
            {
                await s.SetLengthAsync(f3.Length + 4 * Chunk, Ct);
                t4 = await s.CommitAsync(Ct);
            }

            DfsFileInfo f4 = await v.Files.GetInfoAsync("/ns018/f", Ct);
            Assert.Equal(f1.InodeId, f4.InodeId);
            Assert.Equal(t4.Sequence, f4.Revision);
            Assert.Equal(f3.Length + 4 * Chunk, f4.Length);
            Assert.True(f4.IsSparse, "extending with SetLength leaves a hole");

            // 6. Rename into a subdirectory.
            DfsDirectoryInfo sub = await v.Directories.CreateDirectoryAsync("/ns018/sub", Ct);
            NewInode(sub.InodeId, "mkdir sub");
            DfsDirectoryInfo parentBeforeMove = await v.Directories.GetInfoAsync("/ns018", Ct);
            CommitToken t5 = await v.Files.MoveAsync("/ns018/f", "/ns018/sub/g", false, Ct);
            DfsFileInfo g = await v.Files.GetInfoAsync("/ns018/sub/g", Ct);
            Assert.Equal(f1.InodeId, g.InodeId);
            Assert.Equal("/ns018/sub/g", g.Path);
            Assert.True(g.Revision >= f4.Revision);
            Assert.True(g.Revision <= t5.Sequence);
            Assert.Equal(f4.Length, g.Length);
            Assert.Equal(f1.CreatedUtc, g.CreatedUtc);
            Assert.Equal("alice", g.Owner);
            Assert.Equal(f4.StoragePolicy, g.StoragePolicy);
            Observed(g.ObservedAt, t5, "rename");
            await Assert.ThrowsAnyAsync<FileNotFoundException>(() => v.Files.GetInfoAsync("/ns018/f", Ct));
            DfsDirectoryInfo parentAfterMove = await v.Directories.GetInfoAsync("/ns018", Ct);
            DfsDirectoryInfo subAfterMove = await v.Directories.GetInfoAsync("/ns018/sub", Ct);
            Assert.Equal(parentBeforeMove.EntryCount - 1, parentAfterMove.EntryCount);
            Assert.True(parentAfterMove.Revision > parentBeforeMove.Revision);
            Assert.Equal(1, subAfterMove.EntryCount);
            Assert.True(subAfterMove.Revision > sub.Revision);

            // 7. Access flags.
            CommitToken t6 = await v.Files.SetAccessAsync("/ns018/sub/g", new DfsInodeAccess(ReadOnlyForOthers: true), Ct);
            DfsFileInfo g2 = await v.Files.GetInfoAsync("/ns018/sub/g", Ct);
            Assert.Equal(new DfsInodeAccess(ReadOnlyForOthers: true), g2.Access);
            Assert.Equal(t6.Sequence, g2.Revision);
            Assert.Equal(g.InodeId, g2.InodeId);
            Assert.Equal(g.Length, g2.Length);

            // 8. Storage policy.
            CommitToken t7 = await v.Files.SetStoragePolicyAsync("/ns018/sub/g", "silver", Ct);
            DfsFileInfo g3 = await v.Files.GetInfoAsync("/ns018/sub/g", Ct);
            Assert.Equal("silver", g3.StoragePolicy);
            Assert.Equal(t7.Sequence, g3.Revision);
            Assert.Equal(g.InodeId, g3.InodeId);
            Assert.Equal(g2.Access, g3.Access);

            // 9. Copy: a new inode owned by the copier, same bytes.
            CommitToken t8 = await admin.Files.CopyAsync("/ns018/sub/g", "/ns018/copy", false, Ct);
            DfsFileInfo copy = await admin.Files.GetInfoAsync("/ns018/copy", Ct);
            NewInode(copy.InodeId, "copy");
            Assert.Equal(t8.Sequence, copy.Revision);
            Assert.Equal(g3.Length, copy.Length);
            Assert.Equal("admin", copy.Owner);
            Assert.True(copy.CreatedUtc >= g3.CreatedUtc);
            AssertBytes(await admin.Files.ReadAllBytesAsync("/ns018/sub/g", Ct), await admin.Files.ReadAllBytesAsync("/ns018/copy", Ct), "copy");

            // 10. Directory rename keeps directory and descendant identities.
            CommitToken t9 = await v.Directories.MoveAsync("/ns018/sub", "/ns018/sub2", Ct);
            DfsDirectoryInfo sub2 = await v.Directories.GetInfoAsync("/ns018/sub2", Ct);
            Assert.Equal(sub.InodeId, sub2.InodeId);
            Assert.Equal(sub.CreatedUtc, sub2.CreatedUtc);
            Assert.Equal("alice", sub2.Owner);
            Observed(sub2.ObservedAt, t9, "directory rename");
            DfsFileInfo g4 = await v.Files.GetInfoAsync("/ns018/sub2/g", Ct);
            Assert.Equal(g.InodeId, g4.InodeId);
            Assert.Equal("/ns018/sub2/g", g4.Path);
            Assert.Equal(g3.Revision, g4.Revision);
            Assert.Equal("silver", g4.StoragePolicy);

            // 11. Delete a sibling: parent entry count and revision update; other identities unchanged.
            DfsDirectoryInfo beforeDelete = await v.Directories.GetInfoAsync("/ns018", Ct);
            CommitToken t10 = await admin.Files.DeleteAsync("/ns018/copy", Ct);
            DfsDirectoryInfo afterDelete = await v.Directories.GetInfoAsync("/ns018", Ct);
            Assert.Equal(beforeDelete.EntryCount - 1, afterDelete.EntryCount);
            Assert.Equal(t10.Sequence, afterDelete.Revision);
            Assert.Equal(g4.InodeId, (await v.Files.GetInfoAsync("/ns018/sub2/g", Ct)).InodeId);

            // 12. Listing metadata agrees with GetInfo.
            DfsEntryInfo listed = Assert.Single(await ToListAsync(v.Directories.EnumerateAsync("/ns018/sub2", null, Ct)));
            Assert.Equal(g4.InodeId, listed.InodeId);
            Assert.Equal(g4.Revision, listed.Revision);
            Assert.Equal(g4.Length, listed.Length);
            Assert.Equal(DfsEntryKind.File, listed.Kind);
            Assert.Equal("/ns018/sub2/g", listed.Path);
        }, Ct);
    }
}
