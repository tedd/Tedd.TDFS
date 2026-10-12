using System.Text;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Testing;
using static Tedd.Tdfs.Tests.Unit.Tx.TxHelpers;

namespace Tedd.Tdfs.Tests.Unit.Tx;

/// <summary>
/// TX suite, level U: SDK contract of volume-scoped ACID transactions (FSD §8, §11; ARCHITECTURE §5.6, §9.5) against the
/// default three-zone in-memory cluster. Each test proves the specific property with exact bytes, inode identity,
/// revisions, commit tokens, outcome statuses and typed errors; staged changes are always shown to exist inside the
/// transaction first so "nothing visible" assertions cannot pass vacuously.
/// </summary>
public sealed class TxUnitTests
{
    // ------------------------------------------------------------------------------------------------------------
    // TX-001 CommitMultipleFilesAtomically
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>(write kind, new length of /a, new length of /b): inline, inline-threshold ±1, multi-chunk, empty.</summary>
    public static TheoryData<string, int, int> MultiFileShapes => new()
    {
        { "helpers", 5, 7 },
        { "helpers", 4095, 4096 },
        { "helpers", 4097, 1 },
        { "helpers", 200_000, 3 },
        { "helpers", 0, 150_000 },
        { "streams", 5, 7 },
        { "streams", 70_000, 300_000 },
        { "mixed", 4096, 65_537 },
    };

    [Theory]
    [MemberData(nameof(MultiFileShapes))]
    [Catalog("TX-001", "U", "INV-02", "INV-04", "INV-07")]
    public Task TX_001_CommitMultipleFilesAtomically(string writeKind, int newLengthA, int newLengthB) =>
        ClusterTest.RunAsync(Cluster, async cluster =>
        {
            await using DfsClient writerClient = await ConnectAsync(cluster, "writer");
            await using DfsClient observerClient = await ConnectAsync(cluster, "observer");
            DfsVolume writer = writerClient.GetVolume(Vol);
            DfsVolume observer = observerClient.GetVolume(Vol);

            byte[] oldA = TestData.Bytes(0xA0, 1000);
            byte[] oldB = TestData.Bytes(0xB0, 1500);
            byte[] newA = TestData.Bytes(0xA1, newLengthA);
            byte[] newB = TestData.Bytes(0xB1, newLengthB);
            await writer.Files.WriteAllBytesAsync("/a", oldA, Ct);
            await writer.Files.WriteAllBytesAsync("/b", oldB, Ct);
            DfsFileInfo beforeA = await observer.Files.GetInfoAsync("/a", Ct);
            DfsFileInfo beforeB = await observer.Files.GetInfoAsync("/b", Ct);
            var oracle = new PairOracle(oldA, oldB, newA, newB);

            DfsTransaction tx = await writer.Transactions.BeginAsync(null, Ct);
            await using (tx)
            {
                await StageTwoFilesAsync(tx, writeKind, newA, newB);

                // Non-vacuity: the transaction itself sees both new versions.
                await AssertBytesAsync(newA, tx.Files.ReadAllBytesAsync("/a", Ct), "transaction reads its own staged /a");
                await AssertBytesAsync(newB, tx.Files.ReadAllBytesAsync("/b", Ct), "transaction reads its own staged /b");
                Assert.Equal("old/old", await oracle.ObserveTransactionalAsync(observer));

                // Race transactional and standalone observers against the commit.
                var observations = new List<(string Kind, string Pair)>();
                var commitDone = new TaskCompletionSource();
                Task observerLoop = ObserveUntilAsync(observer, oracle, commitDone.Task, observations);
                CommitToken token;
                try
                {
                    token = await tx.CommitAsync(Ct);
                }
                finally
                {
                    commitDone.TrySetResult();
                }

                await observerLoop;

                Assert.Equal(DfsTransactionState.Committed, tx.State);
                foreach ((string kind, string pair) in observations)
                {
                    if (kind == "tx")
                        Assert.True(pair is "old/old" or "new/new", $"A transactional observer saw a fractured pair {pair}.");
                    else
                        Assert.True(pair is "old/old" or "old/new" or "new/new",
                            $"A standalone observer read /a then /b and saw {pair}: /a new but a later read of /b old violates atomic publication.");
                }

                // After commit returned, every new observation sees both new versions.
                Assert.Equal("new/new", await oracle.ObserveTransactionalAsync(observer));
                Assert.Equal("new/new", await oracle.ObserveStandaloneAsync(observer));

                Assert.False(token.IsNone, "A committed transaction returns a real commit token.");
                Assert.Equal(cluster.ClusterId, token.ClusterId);
                Assert.Equal((await observer.GetInfoAsync(Ct)).Id, token.VolumeId);
                Assert.True(token.Sequence > beforeA.Revision && token.Sequence > beforeB.Revision,
                    "The commit is ordered after the versions it replaced.");

                DfsFileInfo afterA = await observer.Files.GetInfoAsync("/a", Ct);
                DfsFileInfo afterB = await observer.Files.GetInfoAsync("/b", Ct);
                Assert.Equal(beforeA.InodeId, afterA.InodeId);
                Assert.Equal(beforeB.InodeId, afterB.InodeId);
                Assert.Equal(newA.Length, afterA.Length);
                Assert.Equal(newB.Length, afterB.Length);
                Assert.Equal(token.Sequence, afterA.Revision);
                Assert.Equal(token.Sequence, afterB.Revision);

                DfsOperationOutcome outcome = await writer.Transactions.QueryOutcomeAsync(tx.Id, false, Ct);
                Assert.Equal(DfsOutcomeStatus.Committed, outcome.Status);
                Assert.Equal(token, outcome.Token);
            }

            await AssertContentAsync(observer.Files, "/a", newA, "committed /a");
            await AssertContentAsync(observer.Files, "/b", newB, "committed /b");
        }, Ct);

    private static async Task StageTwoFilesAsync(DfsTransaction tx, string kind, byte[] newA, byte[] newB)
    {
        switch (kind)
        {
            case "helpers":
                Assert.True((await tx.Files.WriteAllBytesAsync("/a", newA, Ct)).IsNone, "Transaction-bound helpers return CommitToken.None.");
                Assert.True((await tx.Files.WriteAllBytesAsync("/b", newB, Ct)).IsNone, "Transaction-bound helpers return CommitToken.None.");
                break;
            case "streams":
                await using (DfsStream a = await tx.Files.OpenAsync("/a", FileMode.Create, FileAccess.Write, FileShare.None, Ct))
                {
                    Assert.True(a.IsTransactionBound);
                    await a.WriteAsync(newA, Ct);
                }

                await using (DfsStream b = await tx.Files.OpenAsync("/b", FileMode.Truncate, FileAccess.Write, FileShare.None, Ct))
                {
                    Assert.True(b.IsTransactionBound);
                    await b.WriteAsync(newB, Ct);
                }

                break;
            case "mixed":
                await tx.Files.WriteAllBytesAsync("/a", newA, Ct);
                await using (DfsStream b = await tx.Files.OpenAsync("/b", FileMode.Open, FileAccess.ReadWrite, FileShare.None, Ct))
                {
                    await b.SetLengthAsync(0, Ct);
                    await b.WriteAsync(newB, Ct);
                    await b.FlushAsync(Ct);
                }

                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, null);
        }
    }

    private static async Task ObserveUntilAsync(DfsVolume observer, PairOracle oracle, Task stop, List<(string Kind, string Pair)> observations)
    {
        do
        {
            observations.Add(("tx", await oracle.ObserveTransactionalAsync(observer)));
            observations.Add(("standalone", await oracle.ObserveStandaloneAsync(observer)));
        }
        while (!stop.IsCompleted);
    }

    /// <summary>Classifies observed (/a, /b) contents as old/new/foreign.</summary>
    private sealed class PairOracle(byte[] oldA, byte[] oldB, byte[] newA, byte[] newB)
    {
        public string Classify(byte[] a, byte[] b) => Label(a, oldA, newA) + "/" + Label(b, oldB, newB);

        public async Task<string> ObserveTransactionalAsync(DfsVolume volume)
        {
            await using DfsTransaction ro = await volume.Transactions.BeginAsync(null, Ct);
            byte[] a = await ro.Files.ReadAllBytesAsync("/a", Ct);
            byte[] b = await ro.Files.ReadAllBytesAsync("/b", Ct);
            await ro.AbortAsync(Ct);
            return Classify(a, b);
        }

        public async Task<string> ObserveStandaloneAsync(DfsVolume volume)
        {
            byte[] a = await volume.Files.ReadAllBytesAsync("/a", Ct);
            byte[] b = await volume.Files.ReadAllBytesAsync("/b", Ct);
            return Classify(a, b);
        }

        private static string Label(byte[] actual, byte[] old, byte[] @new) =>
            actual.AsSpan().SequenceEqual(old) ? "old" : actual.AsSpan().SequenceEqual(@new) ? "new" : "foreign";
    }

    // ------------------------------------------------------------------------------------------------------------
    // TX-002 AbortLeavesNoVisibleChanges
    // ------------------------------------------------------------------------------------------------------------

    public enum AbortMethod { AbortAsync, AbortSync, DisposeAsync, DisposeSync }

    [Theory]
    [InlineData(AbortMethod.AbortAsync)]
    [InlineData(AbortMethod.AbortSync)]
    [InlineData(AbortMethod.DisposeAsync)]
    [InlineData(AbortMethod.DisposeSync)]
    [Catalog("TX-002", "U", "INV-02", "INV-07")]
    public Task TX_002_AbortLeavesNoVisibleChanges(AbortMethod method) =>
        ClusterTest.RunAsync(Cluster, async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c1");
            await using DfsClient observerClient = await ConnectAsync(cluster, "observer");
            DfsVolume vol = client.GetVolume(Vol);
            DfsVolume obs = observerClient.GetVolume(Vol);

            await SeedAbortFixtureAsync(vol);
            SortedDictionary<string, string> before = await CaptureTreeAsync(obs);

            DfsTransaction tx = await vol.Transactions.BeginAsync(null, Ct);
            await StageFileAndDirectoryChangesAsync(tx);

            // Non-vacuity: every staged change is visible inside the transaction.
            Assert.True(await tx.Files.ExistsAsync("/new/n1", Ct));
            Assert.False(await tx.Files.ExistsAsync("/del/d1", Ct));
            Assert.True(await tx.Files.ExistsAsync("/ren/dst", Ct));
            Assert.False(await tx.Files.ExistsAsync("/ren/src", Ct));
            Assert.True(await tx.Directories.ExistsAsync("/mvdir2", Ct));
            Assert.False(await tx.Directories.ExistsAsync("/emptydir", Ct));
            Assert.False(await tx.Directories.ExistsAsync("/tree", Ct));
            Assert.True(await tx.Directories.ExistsAsync("/newdir/sub/deeper", Ct));
            Assert.True(await tx.Files.ExistsAsync("/copy", Ct));
            Assert.Equal(3, (await tx.Files.GetInfoAsync("/keep/f4", Ct)).Length);
            Assert.True((await tx.Files.GetInfoAsync("/keep/f5", Ct)).Access.ReadOnlyForOthers);

            AssertTreesEqual(before, await CaptureTreeAsync(obs), "staged changes are private before the abort");

            switch (method)
            {
                case AbortMethod.AbortAsync:
                    await tx.AbortAsync(Ct);
                    break;
                case AbortMethod.AbortSync:
                    tx.Abort();
                    break;
                case AbortMethod.DisposeAsync:
                    await tx.DisposeAsync();
                    break;
                case AbortMethod.DisposeSync:
                    tx.Dispose();
                    break;
            }

            Assert.Equal(DfsTransactionState.Aborted, tx.State);
            AssertTreesEqual(before, await CaptureTreeAsync(obs), $"after {method}, another client sees no namespace or data change");
            AssertTreesEqual(before, await CaptureTreeAsync(vol), $"after {method}, the aborting client sees no namespace or data change");

            DfsOperationOutcome outcome = await vol.Transactions.QueryOutcomeAsync(tx.Id, false, Ct);
            Assert.Equal(DfsOutcomeStatus.Aborted, outcome.Status);

            await WaitNoActiveTransactionsAsync(cluster, "the aborted transaction's server state is released");
            await AssertWriterReservationFreeAsync(obs, "/keep/f1", "reservation taken by the aborted overwrite");
            await AssertWriterReservationFreeAsync(obs, "/keep/f3", "reservation taken by the aborted stream write");
            await AssertWriterReservationFreeAsync(obs, "/keep/f4", "reservation taken by the aborted truncation");

            // Disposal after abort is idempotent and still does not change anything.
            await tx.DisposeAsync();
            AssertTreesEqual(before, await CaptureTreeAsync(obs), "a second disposal changes nothing");
        }, Ct);

    private static async Task SeedAbortFixtureAsync(DfsVolume vol)
    {
        foreach (string dir in new[] { "/keep", "/del", "/ren", "/mvdir", "/emptydir", "/tree/x" })
            await vol.Directories.CreateDirectoryAsync(dir, Ct);
        await vol.Files.WriteAllBytesAsync("/keep/f1", TestData.Bytes(1, 3000), Ct);
        await vol.Files.WriteAllBytesAsync("/keep/f2", TestData.Bytes(2, 70_000), Ct);
        await vol.Files.WriteAllBytesAsync("/keep/f3", TestData.Bytes(3, 5000), Ct);
        await vol.Files.WriteAllBytesAsync("/keep/f4", TestData.Bytes(4, 9000), Ct);
        await vol.Files.WriteAllBytesAsync("/keep/f5", TestData.Bytes(5, 10), Ct);
        await vol.Files.WriteAllBytesAsync("/del/d1", TestData.Bytes(6, 100), Ct);
        await vol.Files.WriteAllBytesAsync("/ren/src", TestData.Bytes(7, 100), Ct);
        await vol.Files.WriteAllBytesAsync("/mvdir/child", TestData.Bytes(8, 10), Ct);
        await vol.Files.WriteAllBytesAsync("/tree/x/y", TestData.Bytes(9, 10), Ct);
        await vol.Files.WriteAllBytesAsync("/tree/z", TestData.Bytes(10, 10), Ct);
    }

    private static async Task StageFileAndDirectoryChangesAsync(DfsTransaction tx)
    {
        await tx.Files.WriteAllBytesAsync("/keep/f1", TestData.Bytes(11, 4000), Ct);
        await tx.Files.AppendAllBytesAsync("/keep/f2", TestData.Bytes(12, 500), Ct);
        await using (DfsStream f3 = await tx.Files.OpenAsync("/keep/f3", FileMode.Open, FileAccess.ReadWrite, FileShare.None, Ct))
        {
            f3.Seek(10, SeekOrigin.Begin);
            await f3.WriteAsync(TestData.Bytes(13, 100), Ct);
        }

        await using (DfsStream f4 = await tx.Files.OpenAsync("/keep/f4", FileMode.Open, FileAccess.ReadWrite, FileShare.None, Ct))
            await f4.SetLengthAsync(3, Ct);

        await tx.Directories.CreateDirectoryAsync("/new", Ct);
        await tx.Files.WriteAllBytesAsync("/new/n1", TestData.Bytes(14, 50), Ct);
        await tx.Files.DeleteAsync("/del/d1", Ct);
        await tx.Files.MoveAsync("/ren/src", "/ren/dst", false, Ct);
        await tx.Directories.MoveAsync("/mvdir", "/mvdir2", Ct);
        await tx.Directories.DeleteAsync("/emptydir", false, Ct);
        await tx.Directories.DeleteAsync("/tree", true, Ct);
        await tx.Directories.CreateDirectoryAsync("/newdir/sub/deeper", Ct);
        await tx.Files.CopyAsync("/keep/f5", "/copy", false, Ct);
        await tx.Files.SetAccessAsync("/keep/f5", new DfsInodeAccess(ReadOnlyForOthers: true), Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // TX-003 ReadOwnWritesAndDeletes
    // ------------------------------------------------------------------------------------------------------------

    public enum OwnOp
    {
        CreateFile, OverwriteFile, AppendFile, StreamPartialWrite, TruncateThenExtendViaStream, RenameFile, DeleteFile,
        DeleteThenRecreate, CreateDirectory, MoveDirectory, DeleteDirectoryRecursive, CopyOverExisting, RenameChainAndNameReuse,
    }

    [Theory]
    [InlineData(OwnOp.CreateFile)]
    [InlineData(OwnOp.OverwriteFile)]
    [InlineData(OwnOp.AppendFile)]
    [InlineData(OwnOp.StreamPartialWrite)]
    [InlineData(OwnOp.TruncateThenExtendViaStream)]
    [InlineData(OwnOp.RenameFile)]
    [InlineData(OwnOp.DeleteFile)]
    [InlineData(OwnOp.DeleteThenRecreate)]
    [InlineData(OwnOp.CreateDirectory)]
    [InlineData(OwnOp.MoveDirectory)]
    [InlineData(OwnOp.DeleteDirectoryRecursive)]
    [InlineData(OwnOp.CopyOverExisting)]
    [InlineData(OwnOp.RenameChainAndNameReuse)]
    [Catalog("TX-003", "U", "INV-04", "INV-07")]
    public Task TX_003_ReadOwnWritesAndDeletes(OwnOp op) =>
        ClusterTest.RunAsync(Cluster, async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c1");
            await using DfsClient observerClient = await ConnectAsync(cluster, "observer");
            DfsVolume vol = client.GetVolume(Vol);
            DfsVolume obs = observerClient.GetVolume(Vol);

            byte[] f0 = TestData.Bytes(1, 6000);
            byte[] g0 = TestData.Bytes(2, 100);
            byte[] child0 = TestData.Bytes(3, 20);
            byte[] leaf0 = TestData.Bytes(4, 10);
            byte[] x = TestData.Bytes(5, 777);
            await vol.Directories.CreateDirectoryAsync("/dir/sub", Ct);
            await vol.Files.WriteAllBytesAsync("/f", f0, Ct);
            await vol.Files.WriteAllBytesAsync("/g", g0, Ct);
            await vol.Files.WriteAllBytesAsync("/dir/child", child0, Ct);
            await vol.Files.WriteAllBytesAsync("/dir/sub/leaf", leaf0, Ct);

            var inodes = new Dictionary<string, InodeId>(StringComparer.Ordinal);
            foreach (string file in new[] { "/f", "/g", "/dir/child", "/dir/sub/leaf" })
                inodes[file] = (await obs.Files.GetInfoAsync(file, Ct)).InodeId;
            foreach (string dir in new[] { "/", "/dir", "/dir/sub" })
                inodes[dir] = (await obs.Directories.GetInfoAsync(dir, Ct)).InodeId;
            SortedDictionary<string, string> before = await CaptureTreeAsync(obs);

            await using DfsTransaction tx = await vol.Transactions.BeginAsync(null, Ct);
            IReadOnlyList<Expect> expected = await ApplyOwnOpAsync(tx, op, f0, g0, child0, leaf0, x);

            await VerifyAsync(tx.Files, tx.Directories, expected, inodes, "inside the transaction");
            AssertTreesEqual(before, await CaptureTreeAsync(obs), $"{op} is invisible to other clients before commit");

            CommitToken token = await tx.CommitAsync(Ct);
            Assert.False(token.IsNone);
            await VerifyAsync(obs.Files, obs.Directories, expected, inodes, "after commit, from another client");
        }, Ct);

    private static async Task<IReadOnlyList<Expect>> ApplyOwnOpAsync(DfsTransaction tx, OwnOp op, byte[] f0, byte[] g0, byte[] child0, byte[] leaf0, byte[] x)
    {
        switch (op)
        {
            case OwnOp.CreateFile:
                await tx.Files.WriteAllBytesAsync("/new", x, Ct);
                return [Expect.FileWith("/new", x, newInode: true), Expect.DirWith("/", ["dir", "f", "g", "new"], sameInodeAs: "/")];

            case OwnOp.OverwriteFile:
                await tx.Files.WriteAllBytesAsync("/f", x, Ct);
                return [Expect.FileWith("/f", x, sameInodeAs: "/f")];

            case OwnOp.AppendFile:
                await tx.Files.AppendAllBytesAsync("/f", x, Ct);
                return [Expect.FileWith("/f", [.. f0, .. x], sameInodeAs: "/f")];

            case OwnOp.StreamPartialWrite:
            {
                var model = new ReferenceFileModel(f0);
                await using DfsStream s = await tx.Files.OpenAsync("/f", FileMode.Open, FileAccess.ReadWrite, FileShare.None, Ct);
                s.Seek(100, SeekOrigin.Begin);
                await s.WriteAsync(x.AsMemory(0, 50), Ct);
                model.Write(100, x.AsSpan(0, 50));
                s.Seek(5990, SeekOrigin.Begin);
                await s.WriteAsync(x.AsMemory(50, 30), Ct); // extends the file by 20 bytes
                model.Write(5990, x.AsSpan(50, 30));

                // Read own unflushed writes through the same stream, straddling the staged range.
                s.Seek(90, SeekOrigin.Begin);
                byte[] window = new byte[70];
                int read = await ReadFullyAsync(s, window);
                Assert.Equal(70, read);
                AssertEx.BytesEqual(model.Read(90, 70, staged: true), window, "the bound stream reads its own staged bytes");
                Assert.Equal(model.StagedLength, await s.GetLengthAsync(Ct));
                model.Flush();
                return [Expect.FileWith("/f", model.Committed, sameInodeAs: "/f")];
            }

            case OwnOp.TruncateThenExtendViaStream:
            {
                await using DfsStream s = await tx.Files.OpenAsync("/f", FileMode.Open, FileAccess.ReadWrite, FileShare.None, Ct);
                await s.SetLengthAsync(10, Ct);
                await s.SetLengthAsync(20, Ct);
                byte[] expected = [.. f0[..10], .. new byte[10]];
                s.Seek(0, SeekOrigin.Begin);
                byte[] all = new byte[20];
                Assert.Equal(20, await ReadFullyAsync(s, all));
                AssertEx.BytesEqual(expected, all, "truncate then extend reads zeroes, never the discarded bytes");
                return [Expect.FileWith("/f", expected, sameInodeAs: "/f")];
            }

            case OwnOp.RenameFile:
                await tx.Files.MoveAsync("/f", "/f2", false, Ct);
                return [Expect.Absent("/f"), Expect.FileWith("/f2", f0, sameInodeAs: "/f")];

            case OwnOp.DeleteFile:
                await tx.Files.DeleteAsync("/f", Ct);
                await Assert.ThrowsAnyAsync<FileNotFoundException>(() => tx.Files.ReadAllBytesAsync("/f", Ct));
                await Assert.ThrowsAnyAsync<FileNotFoundException>(() => tx.Files.GetInfoAsync("/f", Ct));
                return [Expect.Absent("/f"), Expect.DirWith("/", ["dir", "g"], sameInodeAs: "/")];

            case OwnOp.DeleteThenRecreate:
                await tx.Files.DeleteAsync("/f", Ct);
                Assert.False(await tx.Files.ExistsAsync("/f", Ct));
                await tx.Files.WriteAllBytesAsync("/f", x, Ct);
                return [Expect.FileWith("/f", x, newInode: true)];

            case OwnOp.CreateDirectory:
                await tx.Directories.CreateDirectoryAsync("/d2/e", Ct);
                return [Expect.DirWith("/d2", ["e"]), Expect.DirWith("/d2/e", []), Expect.DirWith("/", ["d2", "dir", "f", "g"], sameInodeAs: "/")];

            case OwnOp.MoveDirectory:
                await tx.Directories.MoveAsync("/dir", "/dir2", Ct);
                await Assert.ThrowsAnyAsync<DirectoryNotFoundException>(() => tx.Directories.GetInfoAsync("/dir", Ct));
                return
                [
                    Expect.Absent("/dir"), Expect.Absent("/dir/child"),
                    Expect.DirWith("/dir2", ["child", "sub"], sameInodeAs: "/dir"),
                    Expect.DirWith("/dir2/sub", ["leaf"], sameInodeAs: "/dir/sub"),
                    Expect.FileWith("/dir2/child", child0, sameInodeAs: "/dir/child"),
                    Expect.FileWith("/dir2/sub/leaf", leaf0, sameInodeAs: "/dir/sub/leaf"),
                ];

            case OwnOp.DeleteDirectoryRecursive:
                await tx.Directories.DeleteAsync("/dir", true, Ct);
                return [Expect.Absent("/dir"), Expect.Absent("/dir/child"), Expect.Absent("/dir/sub"), Expect.Absent("/dir/sub/leaf")];

            case OwnOp.CopyOverExisting:
                await tx.Files.CopyAsync("/f", "/g", true, Ct);
                return [Expect.FileWith("/g", f0, sameInodeAs: "/g"), Expect.FileWith("/f", f0, sameInodeAs: "/f")];

            case OwnOp.RenameChainAndNameReuse:
                await tx.Files.MoveAsync("/f", "/x", false, Ct);
                await tx.Files.MoveAsync("/x", "/y", false, Ct);
                await tx.Files.WriteAllBytesAsync("/f", x, Ct);
                return [Expect.Absent("/x"), Expect.FileWith("/y", f0, sameInodeAs: "/f"), Expect.FileWith("/f", x, newInode: true), Expect.FileWith("/g", g0, sameInodeAs: "/g")];

            default:
                throw new ArgumentOutOfRangeException(nameof(op), op, null);
        }
    }

    private sealed record Expect(string Path, string Kind, byte[]? Content = null, string? SameInodeAs = null, bool NewInode = false, string[]? Children = null)
    {
        public static Expect Absent(string path) => new(path, "absent");

        public static Expect FileWith(string path, byte[] content, string? sameInodeAs = null, bool newInode = false) =>
            new(path, "file", content, sameInodeAs, newInode);

        public static Expect DirWith(string path, string[] children, string? sameInodeAs = null) =>
            new(path, "dir", null, sameInodeAs, false, children);
    }

    private static async Task VerifyAsync(DfsFileClient files, DfsDirectoryClient dirs, IReadOnlyList<Expect> expected,
        IReadOnlyDictionary<string, InodeId> before, string view)
    {
        foreach (Expect e in expected)
        {
            switch (e.Kind)
            {
                case "absent":
                    Assert.False(await files.ExistsAsync(e.Path, Ct), $"[{view}] no file may exist at {e.Path}");
                    Assert.False(await dirs.ExistsAsync(e.Path, Ct), $"[{view}] no directory may exist at {e.Path}");
                    break;
                case "file":
                {
                    Assert.True(await files.ExistsAsync(e.Path, Ct), $"[{view}] file {e.Path} must exist");
                    await AssertBytesAsync(e.Content!, files.ReadAllBytesAsync(e.Path, Ct), $"[{view}] contents of {e.Path}");
                    DfsFileInfo info = await files.GetInfoAsync(e.Path, Ct);
                    Assert.Equal(e.Content!.Length, info.Length);
                    if (e.SameInodeAs is not null)
                        Assert.True(before[e.SameInodeAs] == info.InodeId, $"[{view}] {e.Path} must keep the inode of {e.SameInodeAs} ({before[e.SameInodeAs]}), got {info.InodeId}");
                    if (e.NewInode)
                        Assert.DoesNotContain(info.InodeId, before.Values);
                    break;
                }

                case "dir":
                {
                    Assert.True(await dirs.ExistsAsync(e.Path, Ct), $"[{view}] directory {e.Path} must exist");
                    var names = new List<string>();
                    await foreach (DfsEntryInfo entry in dirs.EnumerateAsync(e.Path, null, Ct))
                        names.Add(entry.Name);
                    names.Sort(StringComparer.Ordinal);
                    Assert.Equal(e.Children!.Order(StringComparer.Ordinal).ToArray(), names.ToArray());
                    if (e.SameInodeAs is not null)
                        Assert.Equal(before[e.SameInodeAs], (await dirs.GetInfoAsync(e.Path, Ct)).InodeId);
                    break;
                }
            }
        }
    }

    private static async Task<int> ReadFullyAsync(Stream stream, byte[] buffer)
    {
        int total = 0;
        while (total < buffer.Length)
        {
            int read = await stream.ReadAsync(buffer.AsMemory(total), Ct);
            if (read == 0)
                break;
            total += read;
        }

        return total;
    }

    // ------------------------------------------------------------------------------------------------------------
    // TX-012 BoundStreamFlushOnlyStages
    // ------------------------------------------------------------------------------------------------------------

    public enum StageFlush { FlushAsync, FlushSync, WriteThrough }

    [Theory]
    [InlineData(false, 100, StageFlush.FlushAsync)]
    [InlineData(true, 100, StageFlush.FlushAsync)]
    [InlineData(false, 300_000, StageFlush.FlushAsync)]
    [InlineData(true, 300_000, StageFlush.FlushSync)]
    [InlineData(false, 4097, StageFlush.FlushSync)]
    [InlineData(true, 4096, StageFlush.WriteThrough)]
    [InlineData(false, 200_000, StageFlush.WriteThrough)]
    [Catalog("TX-012", "U", "INV-02", "INV-04")]
    public Task TX_012_BoundStreamFlushOnlyStages(bool existing, int length, StageFlush flush) =>
        ClusterTest.RunAsync(Cluster, async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c1");
            await using DfsClient observerClient = await ConnectAsync(cluster, "observer");
            DfsVolume vol = client.GetVolume(Vol);
            DfsVolume obs = observerClient.GetVolume(Vol);

            byte[] old = TestData.Bytes(7, 2000);
            byte[] data = TestData.Bytes(8, length);
            DfsFileInfo? beforeInfo = null;
            if (existing)
            {
                await vol.Files.WriteAllBytesAsync("/f", old, Ct);
                beforeInfo = await obs.Files.GetInfoAsync("/f", Ct);
            }

            SortedDictionary<string, string> before = await CaptureTreeAsync(obs);
            await using DfsTransaction tx = await vol.Transactions.BeginAsync(null, Ct);
            var options = new DfsOpenOptions
            {
                Mode = FileMode.Create,
                Access = FileAccess.ReadWrite,
                Share = FileShare.None,
                WriteMode = flush == StageFlush.WriteThrough ? DfsWriteMode.WriteThrough : DfsWriteMode.OnFlush,
            };

            await using (DfsStream s = await tx.Files.OpenAsync("/f", options, Ct))
            {
                Assert.True(s.IsTransactionBound, "Streams opened through a transaction facade are transaction-bound.");
                int third = length / 3;
                await s.WriteAsync(data.AsMemory(0, third), Ct);
                await s.WriteAsync(data.AsMemory(third, third), Ct);
                await s.WriteAsync(data.AsMemory(2 * third), Ct);
                switch (flush)
                {
                    case StageFlush.FlushAsync:
                        await s.FlushAsync(Ct);
                        break;
                    case StageFlush.FlushSync:
                        s.Flush();
                        break;
                    case StageFlush.WriteThrough:
                        break; // every write call already "published" — inside a transaction that only stages
                }

                // Nothing is publicly visible: same namespace, same bytes, same revision.
                AssertTreesEqual(before, await CaptureTreeAsync(obs), $"after {flush} of a transaction-bound stream");
                if (existing)
                {
                    await AssertContentAsync(obs.Files, "/f", old, "the committed version stays visible to others");
                    Assert.Equal(beforeInfo!.Revision, (await obs.Files.GetInfoAsync("/f", Ct)).Revision);
                }
                else
                {
                    await AssertFileAbsentAsync(obs.Files, "/f", "a file created inside a transaction stays private until commit");
                }

                Assert.Equal(DfsTransactionState.Active, tx.State);

                // The transaction reads its own staged data through the stream and the facade.
                Assert.Equal(length, s.Position);
                Assert.Equal(length, await s.GetLengthAsync(Ct));
                s.Seek(0, SeekOrigin.Begin);
                byte[] readBack = new byte[length];
                Assert.Equal(length, await ReadFullyAsync(s, readBack));
                AssertEx.BytesEqual(data, readBack, "bound stream reads its staged bytes");
                await AssertBytesAsync(data, tx.Files.ReadAllBytesAsync("/f", Ct), "the transaction facade sees flushed staged bytes");
            }

            AssertTreesEqual(before, await CaptureTreeAsync(obs), "disposing the bound stream still publishes nothing");

            CommitToken token = await tx.CommitAsync(Ct);
            await AssertContentAsync(obs.Files, "/f", data, "the staged bytes are published by the transaction commit");
            DfsFileInfo after = await obs.Files.GetInfoAsync("/f", Ct);
            Assert.Equal(token.Sequence, after.Revision);
            if (existing)
                Assert.Equal(beforeInfo!.InodeId, after.InodeId);
        }, Ct);

    // ------------------------------------------------------------------------------------------------------------
    // TX-013 BoundStreamDisposeDoesNotCommit
    // ------------------------------------------------------------------------------------------------------------

    public enum StreamDisposal { DisposeAsync, DisposeSync }

    public enum TxEnding { Commit, Abort }

    [Theory]
    [InlineData(StreamDisposal.DisposeAsync, TxEnding.Commit)]
    [InlineData(StreamDisposal.DisposeAsync, TxEnding.Abort)]
    [InlineData(StreamDisposal.DisposeSync, TxEnding.Commit)]
    [InlineData(StreamDisposal.DisposeSync, TxEnding.Abort)]
    [Catalog("TX-013", "U", "INV-02", "INV-04")]
    public Task TX_013_BoundStreamDisposeDoesNotCommit(StreamDisposal disposal, TxEnding ending) =>
        ClusterTest.RunAsync(Cluster, async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c1");
            await using DfsClient observerClient = await ConnectAsync(cluster, "observer");
            DfsVolume vol = client.GetVolume(Vol);
            DfsVolume obs = observerClient.GetVolume(Vol);

            byte[] s2Initial = TestData.Bytes(20, 70_000);
            await vol.Files.WriteAllBytesAsync("/s2", s2Initial, Ct);
            SortedDictionary<string, string> before = await CaptureTreeAsync(obs);

            byte[] s1Data = TestData.Bytes(21, 3000);
            byte[] s2Patch = TestData.Bytes(22, 1000);
            var s2Model = new ReferenceFileModel(s2Initial);
            s2Model.Write(500, s2Patch);
            s2Model.Flush();

            DfsTransaction tx = await vol.Transactions.BeginAsync(null, Ct);
            DfsStream s1 = await tx.Files.OpenAsync("/s1", FileMode.CreateNew, FileAccess.Write, FileShare.None, Ct);
            DfsStream s2 = await tx.Files.OpenAsync("/s2", FileMode.Open, FileAccess.ReadWrite, FileShare.None, Ct);
            await s1.WriteAsync(s1Data, Ct);                 // unflushed: disposal must stage it
            s2.Seek(500, SeekOrigin.Begin);
            await s2.WriteAsync(s2Patch, Ct);

            if (disposal == StreamDisposal.DisposeAsync)
            {
                await s1.DisposeAsync();
                await s2.DisposeAsync();
            }
            else
            {
                s1.Dispose();
                s2.Dispose();
            }

            Assert.Equal(DfsTransactionState.Active, tx.State);
            AssertTreesEqual(before, await CaptureTreeAsync(obs), "disposing every bound stream does not commit the transaction");
            await AssertBytesAsync(s1Data, tx.Files.ReadAllBytesAsync("/s1", Ct), "disposal staged /s1 for the transaction");
            await AssertBytesAsync(s2Model.Committed, tx.Files.ReadAllBytesAsync("/s2", Ct), "disposal staged /s2 for the transaction");

            // Even a pause does not make disposal an implicit commit.
            await cluster.DelayAsync(TimeSpan.FromMilliseconds(500), Ct);
            AssertTreesEqual(before, await CaptureTreeAsync(obs), "still nothing published before the explicit commit");

            if (ending == TxEnding.Commit)
            {
                CommitToken token = await tx.CommitAsync(Ct);
                await AssertContentAsync(obs.Files, "/s1", s1Data, "explicit commit publishes /s1");
                await AssertContentAsync(obs.Files, "/s2", s2Model.Committed, "explicit commit publishes /s2");
                Assert.Equal(token.Sequence, (await obs.Files.GetInfoAsync("/s1", Ct)).Revision);
                Assert.Equal(token.Sequence, (await obs.Files.GetInfoAsync("/s2", Ct)).Revision);
            }
            else
            {
                await tx.AbortAsync(Ct);
                AssertTreesEqual(before, await CaptureTreeAsync(obs), "aborting after stream disposal publishes nothing");
            }

            await tx.DisposeAsync();
        }, Ct);

    // ------------------------------------------------------------------------------------------------------------
    // TX-014 BoundStreamStandaloneCommitRejected
    // ------------------------------------------------------------------------------------------------------------

    [Theory]
    [InlineData(false, TxEnding.Commit)]
    [InlineData(true, TxEnding.Commit)]
    [InlineData(false, TxEnding.Abort)]
    [InlineData(true, TxEnding.Abort)]
    [Catalog("TX-014", "U", "INV-02")]
    public Task TX_014_BoundStreamStandaloneCommitRejected(bool withSnapshotOption, TxEnding ending) =>
        ClusterTest.RunAsync(Cluster, async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c1");
            await using DfsClient observerClient = await ConnectAsync(cluster, "observer");
            DfsVolume vol = client.GetVolume(Vol);
            DfsVolume obs = observerClient.GetVolume(Vol);
            const string snapshotName = "tx014-must-not-exist";
            var commitFilter = new MessageFilter(From: "client:c1", Type: MessageType.CommitFile);
            var txCommitFilter = new MessageFilter(From: "client:c1", Type: MessageType.TxCommit);

            SortedDictionary<string, string> before = await CaptureTreeAsync(obs);
            byte[] other = TestData.Bytes(30, 900);
            byte[] streamed = TestData.Bytes(31, 9000);

            await using DfsTransaction tx = await vol.Transactions.BeginAsync(null, Ct);
            await tx.Files.WriteAllBytesAsync("/other", other, Ct);
            DfsStream s = await tx.Files.OpenAsync("/s", FileMode.Create, FileAccess.Write, FileShare.None, Ct);
            await s.WriteAsync(streamed, Ct);
            long commitFilesBefore = cluster.Network.Log.CountMessages(commitFilter);
            long txCommitsBefore = cluster.Network.Log.CountMessages(txCommitFilter);

            InvalidOperationException rejected = withSnapshotOption
                ? await Assert.ThrowsAnyAsync<InvalidOperationException>(() => s.CommitAsync(new DfsMutationOptions { CommitSnapshotName = snapshotName }, Ct))
                : await Assert.ThrowsAnyAsync<InvalidOperationException>(() => s.CommitAsync(Ct));
            Assert.IsNotType<ObjectDisposedException>(rejected);
            InvalidOperationException rejectedSync = Assert.ThrowsAny<InvalidOperationException>(() => s.Commit());
            Assert.IsNotType<ObjectDisposedException>(rejectedSync);

            Assert.Equal(DfsTransactionState.Active, tx.State);
            Assert.Equal(commitFilesBefore, cluster.Network.Log.CountMessages(commitFilter));
            Assert.Equal(txCommitsBefore, cluster.Network.Log.CountMessages(txCommitFilter));
            AssertTreesEqual(before, await CaptureTreeAsync(obs), "a rejected standalone commit publishes no part of the transaction");
            Assert.DoesNotContain(snapshotName, (await obs.Snapshots.ListAsync(Ct)).Select(i => i.Name));

            await s.DisposeAsync();
            if (ending == TxEnding.Commit)
            {
                CommitToken token = await tx.CommitAsync(Ct);
                await AssertContentAsync(obs.Files, "/other", other, "the transaction commit publishes the helper write");
                await AssertContentAsync(obs.Files, "/s", streamed, "the transaction commit publishes the bound stream's data");
                Assert.Equal(token.Sequence, (await obs.Files.GetInfoAsync("/other", Ct)).Revision);
                Assert.Equal(token.Sequence, (await obs.Files.GetInfoAsync("/s", Ct)).Revision);
            }
            else
            {
                await tx.AbortAsync(Ct);
                AssertTreesEqual(before, await CaptureTreeAsync(obs), "abort after a rejected stream commit publishes nothing");
            }

            Assert.DoesNotContain(snapshotName, (await obs.Snapshots.ListAsync(Ct)).Select(i => i.Name));
        }, Ct);

    // ------------------------------------------------------------------------------------------------------------
    // TX-015 TransactionDisposeAborts
    // ------------------------------------------------------------------------------------------------------------

    public enum TxDisposal { DisposeAsync, DisposeSync, AwaitUsingScopeExit, ExceptionUnwind, DisposeWithOpenBoundStream }

    [Theory]
    [InlineData(TxDisposal.DisposeAsync)]
    [InlineData(TxDisposal.DisposeSync)]
    [InlineData(TxDisposal.AwaitUsingScopeExit)]
    [InlineData(TxDisposal.ExceptionUnwind)]
    [InlineData(TxDisposal.DisposeWithOpenBoundStream)]
    [Catalog("TX-015", "U", "INV-02", "INV-06")]
    public Task TX_015_TransactionDisposeAborts(TxDisposal disposal) =>
        ClusterTest.RunAsync(Cluster, async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c1");
            await using DfsClient observerClient = await ConnectAsync(cluster, "observer");
            DfsVolume vol = client.GetVolume(Vol);
            DfsVolume obs = observerClient.GetVolume(Vol);
            var txCommitFilter = new MessageFilter(From: "client:c1", Type: MessageType.TxCommit);
            var txAbortFilter = new MessageFilter(From: "client:c1", Type: MessageType.TxAbort);

            await vol.Files.WriteAllBytesAsync("/f", TestData.Bytes(40, 2500), Ct);
            SortedDictionary<string, string> before = await CaptureTreeAsync(obs);
            byte[] newF = TestData.Bytes(41, 3500);
            byte[] created = TestData.Bytes(42, 80);
            long abortsBefore = cluster.Network.Log.CountMessages(txAbortFilter);

            DfsTransaction? captured = null;
            DfsStream? openStream = null;
            async Task StageAsync(DfsTransaction t)
            {
                captured = t;
                await t.Files.WriteAllBytesAsync("/f", newF, Ct);
                await t.Files.WriteAllBytesAsync("/n", created, Ct);
                Assert.True(await t.Files.ExistsAsync("/n", Ct), "non-vacuity: the staged create is visible inside the transaction");
            }

            switch (disposal)
            {
                case TxDisposal.DisposeAsync:
                {
                    DfsTransaction t = await vol.Transactions.BeginAsync(null, Ct);
                    await StageAsync(t);
                    await t.DisposeAsync();
                    break;
                }

                case TxDisposal.DisposeSync:
                {
                    DfsTransaction t = await vol.Transactions.BeginAsync(null, Ct);
                    await StageAsync(t);
                    t.Dispose();
                    break;
                }

                case TxDisposal.AwaitUsingScopeExit:
                    await using (DfsTransaction t = await vol.Transactions.BeginAsync(null, Ct))
                        await StageAsync(t);
                    break;

                case TxDisposal.ExceptionUnwind:
                {
                    var marker = new InvalidOperationException("application failure inside the transaction body");
                    InvalidOperationException thrown = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                    {
                        await using DfsTransaction t = await vol.Transactions.BeginAsync(null, Ct);
                        await StageAsync(t);
                        throw marker;
                    });
                    Assert.Same(marker, thrown);
                    break;
                }

                case TxDisposal.DisposeWithOpenBoundStream:
                {
                    DfsTransaction t = await vol.Transactions.BeginAsync(null, Ct);
                    await StageAsync(t);
                    openStream = await t.Files.OpenAsync("/streamed", FileMode.CreateNew, FileAccess.Write, FileShare.None, Ct);
                    await openStream.WriteAsync(TestData.Bytes(43, 5000), Ct);   // pending, never flushed
                    await t.DisposeAsync();
                    break;
                }
            }

            DfsTransaction tx = Assert.IsType<DfsTransaction>(captured);
            Assert.Equal(DfsTransactionState.Aborted, tx.State);
            Assert.Equal(0, cluster.Network.Log.CountMessages(txCommitFilter));
            Assert.True(cluster.Network.Log.CountMessages(txAbortFilter) > abortsBefore, "Disposing an uncommitted transaction must request an abort (TxAbort).");
            AssertTreesEqual(before, await CaptureTreeAsync(obs), $"{disposal} never commits");

            Assert.Equal(DfsOutcomeStatus.Aborted, (await vol.Transactions.QueryOutcomeAsync(tx.Id, false, Ct)).Status);
            Assert.Equal(DfsOutcomeStatus.Aborted, (await obs.Transactions.QueryOutcomeAsync(tx.Id, true, Ct)).Status);
            await WaitNoActiveTransactionsAsync(cluster, "the disposed transaction's server state is released");
            await AssertWriterReservationFreeAsync(obs, "/f", "the disposed transaction's writer reservation");

            if (openStream is { } bound)
            {
                Exception? late = await CaptureAsync(() => bound.WriteAsync(TestData.Bytes(44, 10), Ct).AsTask());
                Assert.True(late is not null && IsCompletedTransactionRejection(late),
                    $"A bound stream must reject writes after its transaction was disposed; got {late?.GetType().Name ?? "success"}.");
                await CaptureAsync(() => bound.DisposeAsync().AsTask());
                await AssertFileAbsentAsync(obs.Files, "/streamed", "the open bound stream's pending data died with the transaction");
            }

            // Disposal is idempotent and never turns into a commit.
            await tx.DisposeAsync();
            tx.Dispose();
            Assert.Equal(DfsTransactionState.Aborted, tx.State);
            AssertTreesEqual(before, await CaptureTreeAsync(obs), "repeated disposal changes nothing");
        }, Ct);

    // ------------------------------------------------------------------------------------------------------------
    // TX-017 CommitRejectsOverlappingStreamIo
    // ------------------------------------------------------------------------------------------------------------

    public enum OverlapOrder { StreamFlushInFlightThenCommit, CommitInFlightThenStreamWrite }

    [Theory]
    [InlineData(OverlapOrder.StreamFlushInFlightThenCommit)]
    [InlineData(OverlapOrder.CommitInFlightThenStreamWrite)]
    [Catalog("TX-017", "U", "INV-02")]
    public Task TX_017_CommitRejectsOverlappingStreamIo(OverlapOrder order) =>
        ClusterTest.RunAsync(Cluster, async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c1");
            await using DfsClient observerClient = await ConnectAsync(cluster, "observer");
            DfsVolume vol = client.GetVolume(Vol);
            DfsVolume obs = observerClient.GetVolume(Vol);
            SortedDictionary<string, string> before = await CaptureTreeAsync(obs);

            byte[] first = TestData.Bytes(50, 100);
            byte[] rejected = TestData.Bytes(51, 100);
            byte[] other = TestData.Bytes(52, 300);

            await using DfsTransaction tx = await vol.Transactions.BeginAsync(null, Ct);
            await tx.Files.WriteAllBytesAsync("/other", other, Ct);
            DfsStream s = await tx.Files.OpenAsync("/s", FileMode.CreateNew, FileAccess.Write, FileShare.None, Ct);
            await s.WriteAsync(first, Ct);

            CommitToken token;
            if (order == OverlapOrder.StreamFlushInFlightThenCommit)
            {
                FaultArm arm = cluster.Faults.Arm(new FaultMatch(FaultSites.ClientRequestSend, Operation: DfsOperationKind.TxOp), FaultAction.Block());
                Task flush = s.FlushAsync(Ct);
                await arm.WaitTriggeredAsync(Liveness);

                InvalidOperationException overlap = await Assert.ThrowsAnyAsync<InvalidOperationException>(() => tx.CommitAsync(Ct));
                Assert.IsNotType<ObjectDisposedException>(overlap);
                Assert.Equal(DfsTransactionState.Active, tx.State);
                AssertTreesEqual(before, await CaptureTreeAsync(obs), "a commit rejected for overlapping stream I/O publishes nothing");

                arm.Release();
                await flush;
                cluster.Faults.AssertReached(arm);
                await s.DisposeAsync();
                token = await tx.CommitAsync(Ct);
                await AssertContentAsync(obs.Files, "/s", first, "the stream data staged by the overlapping flush is committed once");
            }
            else
            {
                FaultArm arm = cluster.Faults.Arm(new FaultMatch(FaultSites.ClientRequestSend, Operation: DfsOperationKind.TxCommit), FaultAction.Block());
                Task<CommitToken> commit = tx.CommitAsync(Ct);
                await arm.WaitTriggeredAsync(Liveness);
                Assert.Equal(DfsTransactionState.Committing, tx.State);

                Exception? write = await CaptureAsync(() => s.WriteAsync(rejected, Ct).AsTask());
                Assert.True(write is InvalidOperationException, $"Stream I/O overlapping the transaction commit must throw InvalidOperationException; got {write?.GetType().Name ?? "success"}.");
                Exception? helper = await CaptureAsync(() => tx.Files.WriteAllBytesAsync("/late", rejected, Ct));
                Assert.True(helper is InvalidOperationException, $"Facade I/O overlapping the commit must throw InvalidOperationException; got {helper?.GetType().Name ?? "success"}.");

                arm.Release();
                token = await commit;
                cluster.Faults.AssertReached(arm);
                await AssertContentAsync(obs.Files, "/s", first, "the commit contains exactly the data staged before it; the overlapping write is excluded");
                await AssertFileAbsentAsync(obs.Files, "/late", "a rejected overlapping facade write is not part of the transaction");
                await CaptureAsync(() => s.DisposeAsync().AsTask());
            }

            await AssertContentAsync(obs.Files, "/other", other, "the rest of the transaction commits atomically with the stream data");
            Assert.Equal(token.Sequence, (await obs.Files.GetInfoAsync("/s", Ct)).Revision);
            Assert.Equal(token.Sequence, (await obs.Files.GetInfoAsync("/other", Ct)).Revision);
        }, Ct);

    // ------------------------------------------------------------------------------------------------------------
    // TX-018 HandlesInvalidAfterCompletion
    // ------------------------------------------------------------------------------------------------------------

    public enum Completion { Commit, Abort, DisposeTransaction }

    [Theory]
    [InlineData(Completion.Commit)]
    [InlineData(Completion.Abort)]
    [InlineData(Completion.DisposeTransaction)]
    [Catalog("TX-018", "U", "INV-02", "INV-07")]
    public Task TX_018_HandlesInvalidAfterCompletion(Completion completion) =>
        ClusterTest.RunAsync(Cluster, async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c1");
            await using DfsClient observerClient = await ConnectAsync(cluster, "observer");
            DfsVolume vol = client.GetVolume(Vol);
            DfsVolume obs = observerClient.GetVolume(Vol);

            byte[] oldF = TestData.Bytes(60, 3000);
            byte[] newF = TestData.Bytes(61, 2000);
            byte[] g = TestData.Bytes(62, 400);
            await vol.Files.WriteAllBytesAsync("/f", oldF, Ct);
            await vol.Files.WriteAllBytesAsync("/g", g, Ct);
            SortedDictionary<string, string> before = await CaptureTreeAsync(obs);

            DfsTransaction tx = await vol.Transactions.BeginAsync(null, Ct);
            DfsStream writer = await tx.Files.OpenAsync("/f", FileMode.Open, FileAccess.ReadWrite, FileShare.None, Ct);
            await writer.SetLengthAsync(0, Ct);
            await writer.WriteAsync(newF, Ct);
            await writer.FlushAsync(Ct);
            DfsStream reader = await tx.Files.OpenAsync("/g", FileMode.Open, FileAccess.Read, FileShare.Read, Ct);
            Assert.Equal(1, await reader.ReadAsync(new byte[1], Ct));

            switch (completion)
            {
                case Completion.Commit:
                    await tx.CommitAsync(Ct);
                    break;
                case Completion.Abort:
                    await tx.AbortAsync(Ct);
                    break;
                case Completion.DisposeTransaction:
                    await tx.DisposeAsync();
                    break;
            }

            byte[] buffer = new byte[16];
            var attempts = new (string Name, Func<Task> Action)[]
            {
                ("writer.ReadAsync", () => writer.ReadAsync(buffer, Ct).AsTask()),
                ("writer.Read(span)", () => Task.FromResult(writer.Read(buffer.AsSpan()))),
                ("writer.ReadByte", () => Task.FromResult(writer.ReadByte())),
                ("writer.WriteAsync", () => writer.WriteAsync(TestData.Bytes(63, 16), Ct).AsTask()),
                ("writer.Write(span)", () => { writer.Write(TestData.Bytes(64, 16).AsSpan()); return Task.CompletedTask; }),
                ("writer.WriteByte", () => { writer.WriteByte(0x5A); return Task.CompletedTask; }),
                ("writer.SetLengthAsync", () => writer.SetLengthAsync(1, Ct)),
                ("writer.GetLengthAsync", () => writer.GetLengthAsync(Ct).AsTask()),
                ("reader.ReadAsync", () => reader.ReadAsync(buffer, Ct).AsTask()),
                ("tx.Files.ReadAllBytesAsync", () => tx.Files.ReadAllBytesAsync("/g", Ct)),
                ("tx.Files.ExistsAsync", () => tx.Files.ExistsAsync("/g", Ct)),
                ("tx.Files.WriteAllBytesAsync", () => tx.Files.WriteAllBytesAsync("/late", TestData.Bytes(65, 16), Ct)),
                ("tx.Files.OpenAsync", () => tx.Files.OpenAsync("/late2", FileMode.CreateNew, FileAccess.Write, FileShare.None, Ct)),
                ("tx.Directories.CreateDirectoryAsync", () => tx.Directories.CreateDirectoryAsync("/latedir", Ct)),
            };

            var violations = new List<string>();
            foreach ((string name, Func<Task> action) in attempts)
            {
                Exception? ex = await CaptureAsync(action);
                if (ex is null || !IsCompletedTransactionRejection(ex))
                    violations.Add($"{name}: {(ex is null ? "succeeded" : ex.GetType().Name + ": " + ex.Message)}");
            }

            Assert.True(violations.Count == 0,
                $"After {completion}, bound handles and facades must reject I/O:{Environment.NewLine}{string.Join(Environment.NewLine, violations)}");

            // Disposal remains safe and idempotent.
            await writer.DisposeAsync();
            writer.Dispose();
            await reader.DisposeAsync();
            await tx.DisposeAsync();
            tx.Dispose();

            if (completion == Completion.Commit)
            {
                Assert.Equal(DfsTransactionState.Committed, tx.State);
                Assert.Equal(DfsOutcomeStatus.Committed, (await obs.Transactions.QueryOutcomeAsync(tx.Id, false, Ct)).Status);
                await AssertContentAsync(obs.Files, "/f", newF, "the committed content is exactly what was staged before commit");
            }
            else
            {
                Assert.Equal(DfsTransactionState.Aborted, tx.State);
                Assert.Equal(DfsOutcomeStatus.Aborted, (await obs.Transactions.QueryOutcomeAsync(tx.Id, false, Ct)).Status);
                AssertTreesEqual(before, await CaptureTreeAsync(obs), "an aborted transaction leaves everything unchanged");
            }

            await AssertContentAsync(obs.Files, "/g", g, "the read-only bound handle never changed /g");
            await AssertFileAbsentAsync(obs.Files, "/late", "rejected late writes are never applied");
            await AssertFileAbsentAsync(obs.Files, "/late2", "rejected late opens never create files");
            Assert.False(await obs.Directories.ExistsAsync("/latedir", Ct), "rejected late directory creation is never applied");
        }, Ct);

    // ------------------------------------------------------------------------------------------------------------
    // TX-022 TransactionLimitsFailAtomically
    // ------------------------------------------------------------------------------------------------------------

    public enum TxLimit { Duration, TimeoutOption, TouchedInodes, StagedBytes, Pins }

    [Theory]
    [InlineData(TxLimit.Duration, false)]
    [InlineData(TxLimit.Duration, true)]
    [InlineData(TxLimit.TimeoutOption, false)]
    [InlineData(TxLimit.TimeoutOption, true)]
    [InlineData(TxLimit.TouchedInodes, false)]
    [InlineData(TxLimit.TouchedInodes, true)]
    [InlineData(TxLimit.StagedBytes, false)]
    [InlineData(TxLimit.StagedBytes, true)]
    [InlineData(TxLimit.Pins, false)]
    [InlineData(TxLimit.Pins, true)]
    [Catalog("TX-022", "U", "INV-02")]
    public Task TX_022_TransactionLimitsFailAtomically(TxLimit limit, bool beyondLimit) =>
        ClusterTest.RunAsync(LimitCluster(LimitsFor(limit)), async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c1");
            await using DfsClient observerClient = await ConnectAsync(cluster, "observer");
            DfsVolume vol = client.GetVolume(Vol);
            DfsVolume obs = observerClient.GetVolume(Vol);

            // The server advertises the configured limits (FSD §8.3).
            DfsVolumeLimits advertised = (await vol.GetInfoAsync(Ct)).Limits;
            DfsVolumeLimits configured = LimitsFor(limit);
            Assert.Equal(configured.MaxTransactionDuration, advertised.MaxTransactionDuration);
            Assert.Equal(configured.MaxTransactionTouchedInodes, advertised.MaxTransactionTouchedInodes);
            Assert.Equal(configured.MaxTransactionStagedBytes, advertised.MaxTransactionStagedBytes);
            Assert.Equal(configured.MaxPinsPerSession, advertised.MaxPinsPerSession);

            switch (limit)
            {
                case TxLimit.Duration:
                case TxLimit.TimeoutOption:
                    await DurationLimitAsync(cluster, vol, obs, limit, beyondLimit);
                    break;
                case TxLimit.TouchedInodes:
                    await TouchedInodesLimitAsync(vol, obs, beyondLimit);
                    break;
                case TxLimit.StagedBytes:
                    await StagedBytesLimitAsync(vol, obs, beyondLimit);
                    break;
                case TxLimit.Pins:
                    await PinLimitAsync(vol, obs, beyondLimit);
                    break;
            }
        }, Ct);

    [Fact]
    [Catalog("TX-022", "U", "INV-02")]
    public Task TX_022_TransactionLimitsFailAtomically_DescriptorBoundaryProbe() =>
        ClusterTest.RunAsync(LimitCluster(new DfsVolumeLimits { MaxTransactionDescriptorBytes = DescriptorLimit }), async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c1");
            await using DfsClient observerClient = await ConnectAsync(cluster, "observer");
            DfsVolume vol = client.GetVolume(Vol);
            DfsVolume obs = observerClient.GetVolume(Vol);
            Assert.Equal(DescriptorLimit, (await vol.GetInfoAsync(Ct)).Limits.MaxTransactionDescriptorBytes);

            // Each step adds one more created name (~125 UTF-8 bytes of path); descriptor size grows monotonically, so
            // the first failing step is boundary+1 and the step before it is the largest descriptor that fits.
            int lastCommitted = 0;
            for (int n = 1; n <= 64; n++)
            {
                string dir = $"/d{n:D2}";
                DfsTransaction tx = await vol.Transactions.BeginAsync(null, Ct);
                Exception? failure;
                await using (tx)
                {
                    failure = await CaptureAsync(async () =>
                    {
                        await tx.Directories.CreateDirectoryAsync(dir, Ct);
                        for (int i = 0; i < n; i++)
                            await tx.Files.WriteAllBytesAsync($"{dir}/{i:D4}{new string('n', 116)}", ReadOnlyMemory<byte>.Empty, Ct);
                        await tx.CommitAsync(Ct);
                    });
                }

                if (failure is null)
                {
                    int entries = 0;
                    await foreach (DfsEntryInfo _ in obs.Directories.EnumerateAsync(dir, null, Ct))
                        entries++;
                    Assert.Equal(n, entries);
                    lastCommitted = n;
                    continue;
                }

                DfsLimitExceededException ex = Assert.IsAssignableFrom<DfsLimitExceededException>(failure);
                Assert.Equal(DfsErrorCode.LimitExceeded, ex.ErrorCode);
                Assert.Equal(nameof(DfsVolumeLimits.MaxTransactionDescriptorBytes), ex.LimitName);
                Assert.Equal(DescriptorLimit, ex.Limit);
                Assert.True(ex.Requested > ex.Limit, $"The rejected descriptor ({ex.Requested} bytes) must exceed the limit ({ex.Limit}).");
                Assert.True(lastCommitted >= 1, "A descriptor below the limit must commit.");
                Assert.Equal(n - 1, lastCommitted);
                Assert.False(await obs.Directories.ExistsAsync(dir, Ct), "An over-limit transaction publishes no part of itself (not even its directory).");
                Assert.Equal(DfsTransactionState.Aborted, tx.State);
                for (int k = 1; k < n; k++)
                    Assert.True(await obs.Directories.ExistsAsync($"/d{k:D2}", Ct), $"Earlier committed transaction /d{k:D2} must remain.");
                return;
            }

            Assert.Fail($"MaxTransactionDescriptorBytes={DescriptorLimit} was never enforced for descriptors with up to 64 × 125-byte names.");
        }, Ct);

    private const int DescriptorLimit = 4096;

    private static TestClusterOptions LimitCluster(DfsVolumeLimits limits) => Cluster with
    {
        Mode = ClusterMode.Deterministic,
        Volumes = [new TestVolumeSpec(Vol) { Limits = limits }],
    };

    private static DfsVolumeLimits LimitsFor(TxLimit limit) => limit switch
    {
        TxLimit.Duration => new DfsVolumeLimits { MaxTransactionDuration = TimeSpan.FromSeconds(4) },
        TxLimit.TimeoutOption => new DfsVolumeLimits(),
        TxLimit.TouchedInodes => new DfsVolumeLimits { MaxTransactionTouchedInodes = 8 },
        TxLimit.StagedBytes => new DfsVolumeLimits { MaxTransactionStagedBytes = 256 * 1024 },
        TxLimit.Pins => new DfsVolumeLimits { MaxPinsPerSession = 4 },
        _ => throw new ArgumentOutOfRangeException(nameof(limit), limit, null),
    };

    private static async Task DurationLimitAsync(TestCluster cluster, DfsVolume vol, DfsVolume obs, TxLimit limit, bool beyond)
    {
        byte[] old = TestData.Bytes(70, 1000);
        byte[] updated = TestData.Bytes(71, 1200);
        await vol.Files.WriteAllBytesAsync("/f", old, Ct);
        SortedDictionary<string, string> before = await CaptureTreeAsync(obs);

        TimeSpan bound = limit == TxLimit.Duration ? TimeSpan.FromSeconds(4) : TimeSpan.FromSeconds(2);
        DfsTransactionOptions? options = limit == TxLimit.TimeoutOption ? new DfsTransactionOptions { Timeout = bound } : null;
        await using DfsTransaction tx = await vol.Transactions.BeginAsync(options, Ct);
        await tx.Files.WriteAllBytesAsync("/f", updated, Ct);
        await tx.Files.WriteAllBytesAsync("/n", updated, Ct);

        await cluster.DelayAsync(beyond ? bound + TimeSpan.FromSeconds(1) : bound - TimeSpan.FromMilliseconds(750), Ct);
        Exception? failure = await CaptureAsync(() => tx.CommitAsync(Ct));

        if (!beyond)
        {
            Assert.Null(failure);
            await AssertContentAsync(obs.Files, "/f", updated, "a transaction committed within its duration limit publishes everything");
            await AssertContentAsync(obs.Files, "/n", updated, "a transaction committed within its duration limit publishes everything");
            return;
        }

        Assert.NotNull(failure);
        bool legal = failure is DfsTransactionAbortedException { ErrorCode: DfsErrorCode.TransactionAborted }
            || failure is DfsLimitExceededException { ErrorCode: DfsErrorCode.LimitExceeded, LimitName: nameof(DfsVolumeLimits.MaxTransactionDuration) };
        Assert.True(legal, $"An expired transaction must fail with TransactionAborted or LimitExceeded(MaxTransactionDuration); got {failure.GetType().Name}: {failure.Message}");
        Assert.NotEqual(DfsTransactionState.Committed, tx.State);
        AssertTreesEqual(before, await CaptureTreeAsync(obs), "an expired transaction publishes no part of itself");
        await AssertWriterReservationFreeAsync(obs, "/f", "expiry releases the transaction's writer reservation");
    }

    private static async Task TouchedInodesLimitAsync(DfsVolume vol, DfsVolume obs, bool beyond)
    {
        // Files are pre-created so the transaction overwrites existing inodes only: it touches exactly the files it
        // writes; their parent directory is neither modified nor enumerated.
        await vol.Directories.CreateDirectoryAsync("/t", Ct);
        for (int i = 0; i < 9; i++)
            await vol.Files.WriteAllBytesAsync($"/t/{i}", TestData.Bytes((ulong)(100 + i), 64), Ct);
        SortedDictionary<string, string> before = await CaptureTreeAsync(obs);

        int touched = beyond ? 9 : 8;
        await using DfsTransaction tx = await vol.Transactions.BeginAsync(null, Ct);
        Exception? failure = await CaptureAsync(async () =>
        {
            for (int i = 0; i < touched; i++)
                await tx.Files.WriteAllBytesAsync($"/t/{i}", TestData.Bytes((ulong)(200 + i), 64), Ct);
            await tx.CommitAsync(Ct);
        });

        if (!beyond)
        {
            Assert.Null(failure);
            for (int i = 0; i < touched; i++)
                await AssertContentAsync(obs.Files, $"/t/{i}", TestData.Bytes((ulong)(200 + i), 64), "touching exactly the limit commits every file");
            return;
        }

        AssertLimitExceeded(failure, nameof(DfsVolumeLimits.MaxTransactionTouchedInodes), 8);
        Assert.NotEqual(DfsTransactionState.Committed, tx.State);
        AssertTreesEqual(before, await CaptureTreeAsync(obs), "touching one inode more than the limit publishes nothing");
    }

    private static async Task StagedBytesLimitAsync(DfsVolume vol, DfsVolume obs, bool beyond)
    {
        SortedDictionary<string, string> before = await CaptureTreeAsync(obs);
        byte[] first = TestData.Bytes(80, 128 * 1024);
        byte[] second = TestData.Bytes(81, 128 * 1024 + (beyond ? 1 : 0));
        await using DfsTransaction tx = await vol.Transactions.BeginAsync(null, Ct);
        Exception? failure = await CaptureAsync(async () =>
        {
            await tx.Files.WriteAllBytesAsync("/s1", first, Ct);
            await tx.Files.WriteAllBytesAsync("/s2", second, Ct);
            await tx.CommitAsync(Ct);
        });

        if (!beyond)
        {
            Assert.Null(failure);
            await AssertContentAsync(obs.Files, "/s1", first, "exactly MaxTransactionStagedBytes commits");
            await AssertContentAsync(obs.Files, "/s2", second, "exactly MaxTransactionStagedBytes commits");
            return;
        }

        AssertLimitExceeded(failure, nameof(DfsVolumeLimits.MaxTransactionStagedBytes), 256 * 1024);
        Assert.NotEqual(DfsTransactionState.Committed, tx.State);
        AssertTreesEqual(before, await CaptureTreeAsync(obs), "staging one byte more than the limit publishes nothing");
    }

    private static async Task PinLimitAsync(DfsVolume vol, DfsVolume obs, bool beyond)
    {
        SortedDictionary<string, string> before = await CaptureTreeAsync(obs);
        var transactions = new List<DfsTransaction>();
        try
        {
            for (int i = 0; i < 4; i++)
                transactions.Add(await vol.Transactions.BeginAsync(null, Ct));

            if (beyond)
            {
                Exception? fifth = await CaptureAsync(async () => transactions.Add(await vol.Transactions.BeginAsync(null, Ct)));
                AssertLimitExceeded(fifth, nameof(DfsVolumeLimits.MaxPinsPerSession), 4);
                Assert.Equal(4, transactions.Count);
            }

            for (int i = 0; i < transactions.Count; i++)
                await transactions[i].Files.WriteAllBytesAsync($"/p{i}", TestData.Bytes((ulong)(90 + i), 100), Ct);
            AssertTreesEqual(before, await CaptureTreeAsync(obs), "nothing is visible before the pinned transactions commit");

            foreach (DfsTransaction tx in transactions)
                await tx.CommitAsync(Ct);
            for (int i = 0; i < transactions.Count; i++)
                await AssertContentAsync(obs.Files, $"/p{i}", TestData.Bytes((ulong)(90 + i), 100), "transactions within the pin limit commit fully");
            await AssertFileAbsentAsync(obs.Files, "/p4", "the over-limit transaction never existed");

            // Completed transactions release their pins: a new transaction can begin.
            await using DfsTransaction again = await vol.Transactions.BeginAsync(null, Ct);
            Assert.Equal(DfsTransactionState.Active, again.State);
        }
        finally
        {
            foreach (DfsTransaction tx in transactions)
                await tx.DisposeAsync();
        }
    }

    private static void AssertLimitExceeded(Exception? failure, string limitName, long limit)
    {
        Assert.NotNull(failure);
        DfsLimitExceededException ex = Assert.IsAssignableFrom<DfsLimitExceededException>(failure);
        Assert.Equal(DfsErrorCode.LimitExceeded, ex.ErrorCode);
        Assert.Equal(limitName, ex.LimitName);
        Assert.Equal(limit, ex.Limit);
        Assert.True(ex.Requested > ex.Limit, $"Requested ({ex.Requested}) must exceed the limit ({ex.Limit}).");
    }

    // ------------------------------------------------------------------------------------------------------------
    // TX-023 CrossVolumeTransactionRejected
    // ------------------------------------------------------------------------------------------------------------

    public enum CrossVolumeCase { ForeignStreamNotEnlisted, ForeignHelperNotEnlisted, FacadeBoundToOwnVolume, PathNamingOtherVolumeRejected }

    [Theory]
    [InlineData(CrossVolumeCase.ForeignStreamNotEnlisted)]
    [InlineData(CrossVolumeCase.ForeignHelperNotEnlisted)]
    [InlineData(CrossVolumeCase.FacadeBoundToOwnVolume)]
    [InlineData(CrossVolumeCase.PathNamingOtherVolumeRejected)]
    [Catalog("TX-023", "U", "INV-02", "INV-07")]
    public Task TX_023_CrossVolumeTransactionRejected(CrossVolumeCase scenario) =>
        ClusterTest.RunAsync(Cluster with { Volumes = [new TestVolumeSpec(Vol), new TestVolumeSpec("vol2")] }, async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c1");
            await using DfsClient observerClient = await ConnectAsync(cluster, "observer");
            DfsVolume vol = client.GetVolume(Vol);
            DfsVolume vol2 = client.GetVolume("vol2");
            DfsVolume obs = observerClient.GetVolume(Vol);
            DfsVolume obs2 = observerClient.GetVolume("vol2");
            VolumeId volId = (await vol.GetInfoAsync(Ct)).Id;
            VolumeId vol2Id = (await vol2.GetInfoAsync(Ct)).Id;
            Assert.NotEqual(volId, vol2Id);

            byte[] inTx = TestData.Bytes(110, 500);
            byte[] foreign = TestData.Bytes(111, 700);
            await using DfsTransaction tx = await vol.Transactions.BeginAsync(null, Ct);
            Assert.Equal(volId, tx.ReadVersion.VolumeId);
            await tx.Files.WriteAllBytesAsync("/x", inTx, Ct);

            switch (scenario)
            {
                case CrossVolumeCase.ForeignStreamNotEnlisted:
                {
                    await using (DfsStream s = await vol2.Files.OpenAsync("/y", FileMode.Create, FileAccess.Write, FileShare.None, Ct))
                    {
                        Assert.False(s.IsTransactionBound, "A stream opened through another volume is never enlisted in this transaction.");
                        await s.WriteAsync(foreign, Ct);
                        CommitToken streamToken = await s.CommitAsync(Ct);
                        Assert.Equal(vol2Id, streamToken.VolumeId);
                    }

                    await AssertContentAsync(obs2.Files, "/y", foreign, "the foreign-volume stream published immediately, independent of the transaction");
                    await tx.AbortAsync(Ct);
                    await AssertContentAsync(obs2.Files, "/y", foreign, "aborting the transaction does not roll back another volume's commit");
                    await AssertFileAbsentAsync(obs.Files, "/x", "the aborted transaction's own write is discarded");
                    break;
                }

                case CrossVolumeCase.ForeignHelperNotEnlisted:
                {
                    CommitToken helperToken = await vol2.Files.WriteAllBytesAsync("/y", foreign, Ct);
                    Assert.False(helperToken.IsNone, "A non-bound helper commits on its own and returns a real token.");
                    Assert.Equal(vol2Id, helperToken.VolumeId);
                    await AssertContentAsync(obs2.Files, "/y", foreign, "the other volume's write is visible while the transaction is still active");
                    await tx.AbortAsync(Ct);
                    await AssertContentAsync(obs2.Files, "/y", foreign, "aborting the transaction does not roll back another volume's commit");
                    await AssertFileAbsentAsync(obs.Files, "/x", "the aborted transaction's own write is discarded");
                    break;
                }

                case CrossVolumeCase.FacadeBoundToOwnVolume:
                {
                    byte[] other = TestData.Bytes(112, 300);
                    await vol2.Files.WriteAllBytesAsync("/x", other, Ct);
                    DfsFileInfo vol2Before = await obs2.Files.GetInfoAsync("/x", Ct);
                    CommitToken token = await tx.CommitAsync(Ct);
                    Assert.Equal(volId, token.VolumeId);
                    await AssertContentAsync(obs.Files, "/x", inTx, "the transaction's path is resolved in its own volume");
                    await AssertContentAsync(obs2.Files, "/x", other, "the same path in another volume is untouched");
                    DfsFileInfo vol2After = await obs2.Files.GetInfoAsync("/x", Ct);
                    Assert.Equal(vol2Before.Revision, vol2After.Revision);
                    Assert.False(token.IsComparableWith(vol2After.ObservedAt), "Tokens of different volumes are never comparable.");
                    Assert.Throws<ArgumentException>(() => token.CompareTo(vol2After.ObservedAt));
                    break;
                }

                case CrossVolumeCase.PathNamingOtherVolumeRejected:
                {
                    await Assert.ThrowsAnyAsync<ArgumentException>(() => tx.Files.WriteAllBytesAsync("//vol2/z", foreign, Ct));
                    await Assert.ThrowsAnyAsync<ArgumentException>(() => tx.Files.WriteAllBytesAsync("vol2:/z", foreign, Ct));
                    await Assert.ThrowsAnyAsync<ArgumentException>(() => tx.Files.MoveAsync("/x", "//vol2/x", false, Ct));
                    Assert.Equal(DfsTransactionState.Active, tx.State);
                    CommitToken token = await tx.CommitAsync(Ct);
                    Assert.Equal(volId, token.VolumeId);
                    await AssertContentAsync(obs.Files, "/x", inTx, "the valid part of the transaction commits in its own volume");
                    await AssertFileAbsentAsync(obs2.Files, "/z", "no write leaked into the other volume");
                    await AssertFileAbsentAsync(obs2.Files, "/x", "no move leaked into the other volume");
                    await AssertFileAbsentAsync(obs.Files, "/z", "rejected paths are not reinterpreted inside this volume");
                    break;
                }
            }
        }, Ct);

    // ------------------------------------------------------------------------------------------------------------
    // TX-028 ApplicationInvariantIsNotInferred
    // ------------------------------------------------------------------------------------------------------------

    [Fact]
    [Catalog("TX-028", "U", "INV-02", "INV-04")]
    public Task TX_028_ApplicationInvariantIsNotInferred() =>
        ClusterTest.RunAsync(Cluster, async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c1");
            await using DfsClient observerClient = await ConnectAsync(cluster, "observer");
            DfsVolume vol = client.GetVolume(Vol);
            DfsVolume obs = observerClient.GetVolume(Vol);

            // Application invariant (unknown to the filesystem): balance(a) + balance(b) == 100.
            await vol.Directories.CreateDirectoryAsync("/acct", Ct);
            await vol.Files.WriteAllTextAsync("/acct/a", "100", null, Ct);
            await vol.Files.WriteAllTextAsync("/acct/b", "0", null, Ct);

            // 1. A transaction writing logically inconsistent, arbitrary bytes commits: ACID is about filesystem
            //    atomicity/isolation, not business rules.
            byte[] garbageJson = Encoding.UTF8.GetBytes("{\"ledger\": [1, 2,, ÿ not json");
            CommitToken token;
            await using (DfsTransaction tx = await vol.Transactions.BeginAsync(null, Ct))
            {
                await tx.Files.WriteAllTextAsync("/acct/a", "70", null, Ct);   // 30 units vanish
                await tx.Files.WriteAllBytesAsync("/acct/ledger.json", garbageJson, Ct);
                token = await tx.CommitAsync(Ct);
            }

            Assert.False(token.IsNone);
            Assert.Equal("70", await obs.Files.ReadAllTextAsync("/acct/a", null, Ct));
            Assert.Equal("0", await obs.Files.ReadAllTextAsync("/acct/b", null, Ct));
            await AssertContentAsync(obs.Files, "/acct/ledger.json", garbageJson, "arbitrary bytes are stored verbatim, never validated or repaired");

            // 2. A default snapshot is labelled FilesystemConsistent only and preserves the inconsistent bytes verbatim.
            DfsSnapshotInfo snapshot = await vol.Snapshots.CreateAsync("tx028", null, Ct);
            Assert.Equal(DfsConsistencyLabel.FilesystemConsistent, snapshot.Label);
            Assert.Empty(snapshot.Adapters);
            await using (DfsSnapshotView view = await obs.Snapshots.OpenAsync("tx028", Ct))
            {
                Assert.Equal("70", await view.Files.ReadAllTextAsync("/acct/a", null, Ct));
                await AssertBytesAsync(garbageJson, view.Files.ReadAllBytesAsync("/acct/ledger.json", Ct), "snapshot keeps the bytes verbatim");
            }

            // 3. Only an explicit application check enforces the business rule: the application reads both balances
            //    inside a transaction, detects the violation and aborts; the filesystem state is unchanged.
            SortedDictionary<string, string> before = await CaptureTreeAsync(obs);
            await using (DfsTransaction checkedTx = await vol.Transactions.BeginAsync(null, Ct))
            {
                int a = int.Parse(await checkedTx.Files.ReadAllTextAsync("/acct/a", null, Ct), System.Globalization.CultureInfo.InvariantCulture);
                int b = int.Parse(await checkedTx.Files.ReadAllTextAsync("/acct/b", null, Ct), System.Globalization.CultureInfo.InvariantCulture);
                await checkedTx.Files.WriteAllTextAsync("/acct/b", (b + 10).ToString(System.Globalization.CultureInfo.InvariantCulture), null, Ct);
                await checkedTx.Files.WriteAllTextAsync("/acct/a", (a - 10).ToString(System.Globalization.CultureInfo.InvariantCulture), null, Ct);
                bool applicationInvariantHolds = a + b == 100;
                Assert.False(applicationInvariantHolds, "The stored state violates the application's invariant; only the application can know.");
                await checkedTx.AbortAsync(Ct);
            }

            AssertTreesEqual(before, await CaptureTreeAsync(obs), "the application-checked transaction aborted and changed nothing");
        }, Ct);

    // ------------------------------------------------------------------------------------------------------------

    private static async Task<Exception?> CaptureAsync(Func<Task> action)
    {
        try
        {
            await action();
            return null;
        }
        catch (Exception ex)
        {
            return ex;
        }
    }
}
