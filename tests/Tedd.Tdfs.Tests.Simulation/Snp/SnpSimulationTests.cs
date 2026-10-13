using System.Reflection;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Testing.SnpApp;
using Tedd.Tdfs.Transport;
using static Tedd.Tdfs.Tests.Simulation.Snp.SnpHelpers;

namespace Tedd.Tdfs.Tests.Simulation.Snp;

/// <summary>
/// SNP simulation tests (test plan §7.19, FSD §11): snapshot boundaries, exclusion of private and uncommitted state,
/// copy-on-write retention, idempotent create/delete, minimum-boundary vs exact contracts, reference-safe deletion,
/// completeness validation, metadata-only capture, durability across cold restarts, restore into a new volume and
/// one-volume scope. All scenarios run in deterministic simulation (virtual time, seeded schedules).
/// </summary>
public sealed class SnpSimulationTests
{
    private static readonly int[] RaceLengths = [1, 100, 4095, 4096, 4097, 9000];

    // ============================================================================================================
    // SNP-001 SnapshotCapturesOneCommitBoundary
    // ============================================================================================================

    /// <summary>
    /// SNP-001: snapshots race with ordinary writes, appends, deletes, renames and multi-file transactions from other
    /// clients. Every snapshot must equal, file for file and byte for byte, the oracle state of exactly the commits at or
    /// before its boundary (one valid volume boundary); transaction files are never split; boundaries of sequentially
    /// created snapshots strictly increase. Run for several seeds (schedules) and for 3 and 5 voters.
    /// </summary>
    [Theory]
    [InlineData(3, 0x5001_0001UL)]
    [InlineData(3, 0x5001_0002UL)]
    [InlineData(3, 0x5001_0003UL)]
    [InlineData(5, 0x5001_0004UL)]
    [InlineData(5, 0x5001_0005UL)]
    [Catalog("SNP-001", "S", "INV-02", "INV-04", "INV-09", "INV-10", "INV-13", "INV-19")]
    public async Task SNP_001_SnapshotCapturesOneCommitBoundary(int nodeCount, ulong seed)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(Deterministic(nodeCount, seed), async cluster =>
        {
            await using DfsClient writerClient = await cluster.ConnectAsync(new TestClientOptions { Name = "writer" }, ct);
            await using DfsClient txClient = await cluster.ConnectAsync(new TestClientOptions { Name = "tx" }, ct);
            await using DfsClient snapClient = await cluster.ConnectAsync(new TestClientOptions { Name = "snap" }, ct);
            DfsVolume writer = writerClient.GetVolume(Volume);
            DfsVolume txVolume = txClient.GetVolume(Volume);
            DfsVolume snapVolume = snapClient.GetVolume(Volume);
            var random = new SeededRandom(cluster.Seed);

            await writer.Directories.CreateDirectoryAsync("/race", ct);
            var oracle = new CommitHistoryOracle();
            var current = new Dictionary<string, byte[]?>(StringComparer.Ordinal);
            for (int i = 0; i < 6; i++)
            {
                string path = $"/race/f{i}";
                byte[] bytes = TestData.Bytes(seed + (ulong)i, 64 + (i * 1000));
                oracle.Record(await writer.Files.WriteAllBytesAsync(path, bytes, ct), path, bytes);
                current[path] = bytes;
            }

            byte[] txInitial = TestData.Bytes(seed ^ 0x77, 200);
            CommitToken txInit = await txVolume.Files.WriteAllBytesAsync("/race/t0", txInitial, ct);
            oracle.Record(txInit, "/race/t0", txInitial);
            txInit = await txVolume.Files.WriteAllBytesAsync("/race/t1", txInitial, ct);
            oracle.Record(txInit, "/race/t1", txInitial);
            int commitsBeforeRace = oracle.Count;
            CommitToken firstRaceFloor = txInit;

            var snapshots = new List<DfsSnapshotInfo>();
            var lastWorkloadCommit = new CommitToken[1];
            await Task.WhenAll(
                OrdinaryWriterAsync(cluster, writer, oracle, current, ["/race/f0", "/race/f1"], random.Fork("w0"), ct),
                OrdinaryWriterAsync(cluster, writer, oracle, current, ["/race/f2", "/race/f3"], random.Fork("w1"), ct),
                OrdinaryWriterAsync(cluster, writer, oracle, current, ["/race/f4", "/race/f5"], random.Fork("w2"), ct),
                TransactionWriterAsync(cluster, txVolume, oracle, random.Fork("tx"), 10, lastWorkloadCommit, ct),
                SnapshotTakerAsync(cluster, snapVolume, snapshots, "race", 8, random.Fork("snap"), ct));

            Assert.True(oracle.Count > commitsBeforeRace + 40, $"workload must commit many mutations (recorded {oracle.Count})");
            Assert.Equal(8, snapshots.Count);
            for (int i = 0; i < snapshots.Count; i++)
            {
                Assert.Equal(DfsConsistencyLabel.FilesystemConsistent, snapshots[i].Label);
                Assert.True(snapshots[i].Boundary.IsComparableWith(firstRaceFloor), "snapshot boundaries belong to the volume's history");
                if (i > 0)
                {
                    Assert.True(snapshots[i].Boundary.CompareTo(snapshots[i - 1].Boundary) > 0,
                        $"sequentially created snapshots must have increasing boundaries ({snapshots[i - 1].Boundary} then {snapshots[i].Boundary})");
                }
            }

            // The race really happened: some snapshot boundary lies strictly inside the workload's commit range.
            CommitToken lastCommit = oracle.LatestAtOrBefore(firstRaceFloor with { Sequence = ulong.MaxValue }) ?? firstRaceFloor;
            Assert.Contains(snapshots, s => s.Boundary.CompareTo(firstRaceFloor) > 0 && s.Boundary.CompareTo(lastCommit) < 0);

            await using DfsClient readerClient = await cluster.ConnectAsync(new TestClientOptions { Name = "reader" }, ct);
            DfsVolume reader = readerClient.GetVolume(Volume);
            foreach (DfsSnapshotInfo snapshot in snapshots)
            {
                SortedDictionary<string, byte[]?> tree = await CaptureSnapshotAsync(reader, snapshot.Name, "/race", ct);
                SortedDictionary<string, byte[]> files = SnapshotTree.FilesOf(tree);
                oracle.AssertMatches(snapshot.Boundary, files, $"snapshot {snapshot.Name}");
                Assert.True(files.ContainsKey("/race/t0") && files.ContainsKey("/race/t1"), "transaction files exist in every snapshot");
                AssertEx.BytesEqual(files["/race/t0"], files["/race/t1"], $"snapshot {snapshot.Name}: a transaction is never split by a snapshot");
            }
        }, ct);
    }

    private static async Task OrdinaryWriterAsync(TestCluster cluster, DfsVolume vol, CommitHistoryOracle oracle, Dictionary<string, byte[]?> current,
        string[] paths, IRandomSource random, CancellationToken ct)
    {
        for (int step = 0; step < 14; step++)
        {
            string path = paths[random.Next(paths.Length)];
            byte[]? content;
            lock (current)
                content = current[path];
            int op = content is null ? 0 : random.Next(4);
            switch (op)
            {
                case 0:
                    {
                        byte[] next = TestData.Bytes(random.NextUInt64(), RaceLengths[random.Next(RaceLengths.Length)]);
                        CommitToken token = await vol.Files.WriteAllBytesAsync(path, next, ct);
                        oracle.Record(token, path, next);
                        content = next;
                        break;
                    }

                case 1:
                    {
                        byte[] suffix = TestData.Bytes(random.NextUInt64(), 1 + random.Next(300));
                        CommitToken token = await vol.Files.AppendAllBytesAsync(path, suffix, ct);
                        content = Concat(content!, suffix);
                        oracle.Record(token, path, content);
                        break;
                    }

                case 2:
                    {
                        CommitToken token = await vol.Files.DeleteAsync(path, ct);
                        Assert.False(token.IsNone, "deleting an existing file is a commit");
                        oracle.Record(token, path, null);
                        content = null;
                        break;
                    }

                default:
                    {
                        string moved = path + ".moved";
                        CommitToken away = await vol.Files.MoveAsync(path, moved, false, ct);
                        oracle.Record(away, new Dictionary<string, byte[]?>(StringComparer.Ordinal) { [path] = null, [moved] = content });
                        await cluster.DelayAsync(TimeSpan.FromMilliseconds(random.Next(15)), ct);
                        CommitToken back = await vol.Files.MoveAsync(moved, path, false, ct);
                        oracle.Record(back, new Dictionary<string, byte[]?>(StringComparer.Ordinal) { [moved] = null, [path] = content });
                        break;
                    }
            }

            lock (current)
                current[path] = content;
            await cluster.DelayAsync(TimeSpan.FromMilliseconds(random.Next(25)), ct);
        }
    }

    private static async Task TransactionWriterAsync(TestCluster cluster, DfsVolume vol, CommitHistoryOracle oracle, IRandomSource random,
        int count, CommitToken[] last, CancellationToken ct)
    {
        for (int i = 0; i < count; i++)
        {
            byte[] value = TestData.Bytes(random.NextUInt64(), 50 + random.Next(9000));
            var writes = new Dictionary<string, byte[]?>(StringComparer.Ordinal) { ["/race/t0"] = value, ["/race/t1"] = value };
            await using DfsTransaction tx = await vol.Transactions.BeginAsync(null, ct);
            await tx.Files.WriteAllBytesAsync("/race/t0", value, ct);
            await tx.Files.WriteAllBytesAsync("/race/t1", value, ct);
            if (i % 2 == 0)
            {
                byte[] extra = TestData.Bytes(random.NextUInt64(), 10 + random.Next(100));
                await tx.Files.WriteAllBytesAsync("/race/t2", extra, ct);
                writes["/race/t2"] = extra;
            }
            else
            {
                await tx.Files.DeleteAsync("/race/t2", ct);
                writes["/race/t2"] = null;
            }

            CommitToken token = await tx.CommitAsync(ct);
            Assert.Equal(DfsTransactionState.Committed, tx.State);
            oracle.Record(token, writes);
            last[0] = token;
            await cluster.DelayAsync(TimeSpan.FromMilliseconds(random.Next(30)), ct);
        }
    }

    private static async Task SnapshotTakerAsync(TestCluster cluster, DfsVolume vol, List<DfsSnapshotInfo> snapshots, string prefix, int count,
        IRandomSource random, CancellationToken ct)
    {
        for (int i = 0; i < count; i++)
        {
            await cluster.DelayAsync(TimeSpan.FromMilliseconds(5 + random.Next(60)), ct);
            DfsSnapshotInfo info = await vol.Snapshots.CreateAsync($"{prefix}-{i}", null, ct);
            lock (snapshots)
                snapshots.Add(info);
        }
    }

    // ============================================================================================================
    // SNP-002 SnapshotExcludesPrivateBuffers
    // ============================================================================================================

    public enum PendingChange
    {
        SmallOverwrite,
        LargeAutoStaged,
        ShrinkSetLength,
        ExtendSetLength,
        NewFileCreatedThenWritten,
        AppendStream,
        SparseWriteBeyondEof,
        ManyWriters,
    }

    /// <summary>
    /// SNP-002: while writers hold unflushed OnFlush changes (buffered, auto-staged beyond the buffer, pending length
    /// changes, sparse writes, a freshly created file, an append stream, several writers), a snapshot is created without
    /// waiting for them; it contains exactly the previously committed bytes. After the writers flush, live state shows
    /// the new revisions and the snapshot is unchanged.
    /// </summary>
    [Theory]
    [InlineData(PendingChange.SmallOverwrite)]
    [InlineData(PendingChange.LargeAutoStaged)]
    [InlineData(PendingChange.ShrinkSetLength)]
    [InlineData(PendingChange.ExtendSetLength)]
    [InlineData(PendingChange.NewFileCreatedThenWritten)]
    [InlineData(PendingChange.AppendStream)]
    [InlineData(PendingChange.SparseWriteBeyondEof)]
    [InlineData(PendingChange.ManyWriters)]
    [Catalog("SNP-002", "S", "INV-02", "INV-04", "INV-09", "INV-10", "INV-13", "INV-19")]
    public async Task SNP_002_SnapshotExcludesPrivateBuffers(PendingChange change)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(Deterministic(), async cluster =>
        {
            await using DfsClient observerClient = await cluster.ConnectAsync(new TestClientOptions { Name = "observer" }, ct);
            DfsVolume observer = observerClient.GetVolume(Volume);
            await observer.Directories.CreateDirectoryAsync("/buf", ct);
            string[] paths = change == PendingChange.ManyWriters ? ["/buf/f", "/buf/g", "/buf/h"] : ["/buf/f"];
            var baseContent = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            for (int i = 0; i < paths.Length; i++)
            {
                baseContent[paths[i]] = TestData.Bytes(0x5002_0000UL + (ulong)i, (2 * Chunk) + 5);
                await observer.Files.WriteAllBytesAsync(paths[i], baseContent[paths[i]], ct);
            }

            SortedDictionary<string, byte[]?> committed = await SnapshotTree.CaptureAsync(observer, "/buf", ct);
            NodeCounters countersBefore = await cluster.Inspect.GetTotalCountersAsync();

            var writers = new List<(DfsClient Client, DfsStream Stream, ReferenceFileModel Model, string Path)>();
            try
            {
                for (int i = 0; i < paths.Length; i++)
                {
                    DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = $"w{i}" }, ct);
                    (DfsStream stream, ReferenceFileModel model, string path) =
                        await StagePendingChangeAsync(client.GetVolume(Volume), change, paths[i], baseContent[paths[i]], ct);
                    writers.Add((client, stream, model, path));
                }

                SortedDictionary<string, byte[]?> expectedMid = SnapshotTree.Clone(committed);
                if (change == PendingChange.NewFileCreatedThenWritten)
                    expectedMid["/buf/new"] = [];  // the create is committed at open; its buffered bytes are not

                SnapshotTree.AssertEqual(expectedMid, await SnapshotTree.CaptureSharedAsync(observer, "/buf", ct), "live view of another client before any flush");
                foreach (var w in writers)
                {
                    Assert.False(w.Stream.IsFaulted);
                    if (change is not (PendingChange.ShrinkSetLength or PendingChange.ExtendSetLength))
                        Assert.True(w.Stream.PendingBytes > 0, $"{change}: the stream must hold unpublished bytes");
                }

                if (change == PendingChange.LargeAutoStaged)
                {
                    NodeCounters countersStaged = await cluster.Inspect.GetTotalCountersAsync();
                    Assert.True(countersStaged.ObjectPuts > countersBefore.ObjectPuts,
                        "writes beyond the buffer must be auto-staged to storage (still unpublished) so the leak check is meaningful");
                    Assert.True(writers[0].Stream.PendingBytes >= 5L * Chunk);
                }

                long start = cluster.Time.GetTimestamp();
                DfsSnapshotInfo mid = await observer.Snapshots.CreateAsync("mid", null, ct);
                TimeSpan took = cluster.Time.GetElapsedTime(start);
                Assert.True(took < Timings.LeaseDuration,
                    $"an ordinary snapshot must not wait for clients to flush or for their leases (took {took})");
                SnapshotTree.AssertEqual(expectedMid, await CaptureSnapshotAsync(observer, "mid", "/buf", ct),
                    $"{change}: snapshot taken while clients hold unflushed changes");

                // The writers are unaffected and still see their own staged changes.
                foreach (var w in writers)
                {
                    Assert.False(w.Stream.IsFaulted, "snapshot creation must not fault open writers");
                    if (w.Stream.CanSeek && w.Stream.CanRead)
                    {
                        w.Stream.Position = 0;
                        byte[] own = await ReadToEndAsync(w.Stream, ct);
                        AssertEx.BytesEqual(w.Model.Staged, own, "writer reads its own staged version");
                    }
                }

                // Publish and re-check: live shows the new revisions; the snapshot keeps the old bytes.
                SortedDictionary<string, byte[]?> expectedAfter = SnapshotTree.Clone(expectedMid);
                CommitToken lastFlush = CommitToken.None;
                foreach (var w in writers)
                {
                    lastFlush = await w.Stream.CommitAsync(ct);
                    w.Model.Flush();
                    expectedAfter[w.Path] = w.Model.Committed;
                    Assert.True(lastFlush.CompareTo(mid.Boundary) > 0, "the flush is published after the snapshot boundary");
                }

                SnapshotTree.AssertEqual(expectedAfter, await SnapshotTree.CaptureSharedAsync(observer, "/buf", ct), "live view after flush");
                SnapshotTree.AssertEqual(expectedMid, await CaptureSnapshotAsync(observer, "mid", "/buf", ct), "snapshot after the writers flushed");
                DfsSnapshotInfo after = await observer.Snapshots.CreateAsync("after", new DfsSnapshotOptions { MinimumCommit = lastFlush }, ct);
                Assert.True(after.Boundary.IsAtOrAfter(lastFlush));
                SnapshotTree.AssertEqual(expectedAfter, await CaptureSnapshotAsync(observer, "after", "/buf", ct), "snapshot after flush");
            }
            finally
            {
                foreach (var w in writers)
                {
                    await w.Stream.DisposeAsync();
                    await w.Client.DisposeAsync();
                }
            }
        }, ct);
    }

    private static async Task<(DfsStream Stream, ReferenceFileModel Model, string Path)> StagePendingChangeAsync(DfsVolume vol, PendingChange change,
        string path, byte[] baseContent, CancellationToken ct)
    {
        var model = new ReferenceFileModel(baseContent);
        DfsStream stream;
        switch (change)
        {
            case PendingChange.SmallOverwrite:
            case PendingChange.ManyWriters:
                {
                    stream = await vol.Files.OpenAsync(path, FileMode.Open, FileAccess.ReadWrite, FileShare.Read, ct);
                    byte[] data = TestData.Bytes(0x5002_1000UL + (ulong)path.Length, 200);
                    stream.Position = 100;
                    await stream.WriteAsync(data, ct);
                    model.Write(100, data);
                    break;
                }

            case PendingChange.LargeAutoStaged:
                {
                    stream = await vol.Files.OpenAsync(path, FileMode.Open, FileAccess.ReadWrite, FileShare.Read, ct);
                    long offset = 0;
                    foreach (int part in new[] { (2 * Chunk) + 3, (2 * Chunk) + 1, Chunk + 3 })
                    {
                        byte[] data = TestData.Bytes(0x5002_2000UL + (ulong)offset, part);
                        await stream.WriteAsync(data, ct);
                        model.Write(offset, data);
                        offset += part;
                    }

                    break;
                }

            case PendingChange.ShrinkSetLength:
                stream = await vol.Files.OpenAsync(path, FileMode.Open, FileAccess.ReadWrite, FileShare.Read, ct);
                await stream.SetLengthAsync(Chunk / 2, ct);
                model.SetLength(Chunk / 2);
                break;
            case PendingChange.ExtendSetLength:
                stream = await vol.Files.OpenAsync(path, FileMode.Open, FileAccess.ReadWrite, FileShare.Read, ct);
                await stream.SetLengthAsync(4L * Chunk, ct);
                model.SetLength(4L * Chunk);
                break;
            case PendingChange.NewFileCreatedThenWritten:
                {
                    path = "/buf/new";
                    model = new ReferenceFileModel([]);
                    stream = await vol.Files.OpenAsync(path, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read, ct);
                    byte[] data = TestData.Bytes(0x5002_3000UL, 1000);
                    await stream.WriteAsync(data, ct);
                    model.Write(0, data);
                    break;
                }

            case PendingChange.AppendStream:
                {
                    stream = await vol.Files.OpenAsync(path, FileMode.Append, FileAccess.Write, FileShare.Read, ct);
                    Assert.False(stream.CanSeek, "append streams are not seekable");
                    byte[] data = TestData.Bytes(0x5002_4000UL, 500);
                    await stream.WriteAsync(data, ct);
                    model.Write(model.StagedLength, data);
                    break;
                }

            case PendingChange.SparseWriteBeyondEof:
                {
                    stream = await vol.Files.OpenAsync(path, FileMode.Open, FileAccess.ReadWrite, FileShare.Read, ct);
                    byte[] data = TestData.Bytes(0x5002_5000UL, 10);
                    stream.Position = 4L * Chunk;
                    await stream.WriteAsync(data, ct);
                    model.Write(4L * Chunk, data);
                    break;
                }

            default:
                throw new ArgumentOutOfRangeException(nameof(change), change, null);
        }

        return (stream, model, path);
    }

    // ============================================================================================================
    // SNP-003 SnapshotExcludesUncommittedTransaction
    // ============================================================================================================

    public enum TxStage
    {
        StagedHelpers,
        BoundStreamFlushed,
        CommitRequestHeld,
        CommitRacingSnapshot,
        AbortedAfterSnapshot,
    }

    /// <summary>
    /// SNP-003: a snapshot taken while a transaction has staged file writes, a durably staged bound stream, a create, a
    /// delete, a directory rename and a directory creation — and while its commit request is still undelivered — contains
    /// none of them; after commit a snapshot contains all of them; a snapshot racing the commit contains all or nothing,
    /// consistently with its boundary; an aborted transaction never appears.
    /// </summary>
    [Theory]
    [InlineData(TxStage.StagedHelpers)]
    [InlineData(TxStage.BoundStreamFlushed)]
    [InlineData(TxStage.CommitRequestHeld)]
    [InlineData(TxStage.CommitRacingSnapshot)]
    [InlineData(TxStage.AbortedAfterSnapshot)]
    [Catalog("SNP-003", "S", "INV-02", "INV-04", "INV-09", "INV-10", "INV-13", "INV-19")]
    public async Task SNP_003_SnapshotExcludesUncommittedTransaction(TxStage stage)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(Deterministic(), async cluster =>
        {
            await using DfsClient txClient = await cluster.ConnectAsync(new TestClientOptions { Name = "tx" }, ct);
            await using DfsClient snapClient = await cluster.ConnectAsync(new TestClientOptions { Name = "snap" }, ct);
            DfsVolume txVol = txClient.GetVolume(Volume);
            DfsVolume snapVol = snapClient.GetVolume(Volume);

            byte[] a0 = TestData.Bytes(0x5003_0001, (2 * Chunk) + 9);
            byte[] b0 = TestData.Bytes(0x5003_0002, 300);
            await txVol.Directories.CreateDirectoryAsync("/tx/dir", ct);
            await txVol.Files.WriteAllBytesAsync("/tx/a", a0, ct);
            await txVol.Files.WriteAllBytesAsync("/tx/b", b0, ct);
            await txVol.Files.WriteAllBytesAsync("/tx/del", TestData.Bytes(0x5003_0003, 50), ct);
            await txVol.Files.WriteAllBytesAsync("/tx/dir/x", TestData.Bytes(0x5003_0004, 70), ct);
            SortedDictionary<string, byte[]?> pre = await SnapshotTree.CaptureAsync(snapVol, "/tx", ct);

            byte[] a1 = TestData.Bytes(0x5003_0011, (3 * Chunk) + 1);
            byte[] bPatch = TestData.Bytes(0x5003_0012, 64);
            byte[] n1 = TestData.Bytes(0x5003_0013, 5000);
            var bModel = new ReferenceFileModel(b0);
            bModel.Write(10, bPatch);
            bModel.Flush();
            SortedDictionary<string, byte[]?> post = SnapshotTree.Clone(pre);
            post["/tx/a"] = a1;
            post["/tx/b"] = bModel.Committed;
            post["/tx/new"] = n1;
            SetPath(post, "/tx/del", null);
            MovePath(post, "/tx/dir", "/tx/dir2");
            post["/tx/newdir"] = null;

            await using DfsTransaction tx = await txVol.Transactions.BeginAsync(null, ct);
            await tx.Files.WriteAllBytesAsync("/tx/a", a1, ct);
            DfsStream boundStream = await tx.Files.OpenAsync("/tx/b", FileMode.Open, FileAccess.ReadWrite, FileShare.Read, ct);
            Assert.True(boundStream.IsTransactionBound);
            boundStream.Position = 10;
            await boundStream.WriteAsync(bPatch, ct);
            await Assert.ThrowsAsync<InvalidOperationException>(() => boundStream.CommitAsync(ct));
            DfsStream? bound = boundStream;
            if (stage == TxStage.BoundStreamFlushed)
            {
                await boundStream.FlushAsync(ct);   // stages durably, publishes nothing
            }
            else
            {
                await boundStream.DisposeAsync();
                bound = null;
            }

            await tx.Files.WriteAllBytesAsync("/tx/new", n1, ct);
            await tx.Files.DeleteAsync("/tx/del", ct);
            await tx.Directories.MoveAsync("/tx/dir", "/tx/dir2", ct);
            await tx.Directories.CreateDirectoryAsync("/tx/newdir", ct);
            await AssertBytesAsync(a1, tx.Files.ReadAllBytesAsync("/tx/a", ct), "the transaction reads its own writes");
            SnapshotTree.AssertEqual(pre, await SnapshotTree.CaptureSharedAsync(snapVol, "/tx", ct), "other clients do not see uncommitted transaction state");

            DfsSnapshotInfo mid;
            CommitToken? commitToken = null;
            SortedDictionary<string, byte[]?> expectedFinal = post;
            switch (stage)
            {
                case TxStage.StagedHelpers:
                case TxStage.BoundStreamFlushed:
                    mid = await snapVol.Snapshots.CreateAsync("mid", null, ct);
                    SnapshotTree.AssertEqual(pre, await CaptureSnapshotAsync(snapVol, "mid", "/tx", ct), $"{stage}: snapshot during staging");
                    if (bound is not null)
                        await bound.DisposeAsync();
                    commitToken = await tx.CommitAsync(ct);
                    break;
                case TxStage.CommitRequestHeld:
                    {
                        using MessageHold hold = cluster.Network.Hold(new MessageFilter(Type: MessageType.TxCommit, Direction: LinkDirection.Request));
                        Task<CommitToken> commit = tx.CommitAsync(ct);
                        await hold.WaitForAsync(1, TimeSpan.FromSeconds(10));
                        mid = await snapVol.Snapshots.CreateAsync("mid", null, ct);
                        SnapshotTree.AssertEqual(pre, await CaptureSnapshotAsync(snapVol, "mid", "/tx", ct), "snapshot while the commit request is undelivered");
                        hold.ReleaseAll();
                        commitToken = await commit;
                        break;
                    }

                case TxStage.CommitRacingSnapshot:
                    {
                        Task<CommitToken> commit = tx.CommitAsync(ct);
                        Task<DfsSnapshotInfo> snap = snapVol.Snapshots.CreateAsync("mid", null, ct);
                        await Task.WhenAll(commit, snap);
                        commitToken = commit.Result;
                        mid = snap.Result;
                        SortedDictionary<string, byte[]?> racing = await CaptureSnapshotAsync(snapVol, "mid", "/tx", ct);
                        SortedDictionary<string, byte[]?> expectedRace = mid.Boundary.CompareTo(commitToken.Value) >= 0 ? post : pre;
                        SnapshotTree.AssertEqual(expectedRace, racing, "a snapshot racing a commit contains the whole transaction or none of it, matching its boundary");
                        break;
                    }

                case TxStage.AbortedAfterSnapshot:
                    mid = await snapVol.Snapshots.CreateAsync("mid", null, ct);
                    SnapshotTree.AssertEqual(pre, await CaptureSnapshotAsync(snapVol, "mid", "/tx", ct), "snapshot during staging");
                    await tx.AbortAsync(ct);
                    Assert.Equal(DfsTransactionState.Aborted, tx.State);
                    expectedFinal = pre;
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(stage), stage, null);
            }

            if (commitToken is not null && stage != TxStage.CommitRacingSnapshot)
                Assert.True(mid.Boundary.CompareTo(commitToken.Value) < 0, "the mid snapshot precedes the commit");

            DfsSnapshotInfo after = await snapVol.Snapshots.CreateAsync("after", commitToken is null ? null : new DfsSnapshotOptions { MinimumCommit = commitToken }, ct);
            SnapshotTree.AssertEqual(expectedFinal, await CaptureSnapshotAsync(snapVol, "after", "/tx", ct), "snapshot after the transaction ended");
            SnapshotTree.AssertEqual(expectedFinal, await SnapshotTree.CaptureAsync(snapVol, "/tx", ct), "live after the transaction ended");
            if (stage != TxStage.CommitRacingSnapshot)
                SnapshotTree.AssertEqual(pre, await CaptureSnapshotAsync(snapVol, "mid", "/tx", ct), "mid snapshot is immutable");
            Assert.True(after.Boundary.CompareTo(mid.Boundary) > 0);
            VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Volume);
            Assert.Equal(0, state.ActiveTransactions);
        }, ct);
    }

    // ============================================================================================================
    // SNP-004 is a unit-level test (Tedd.Tdfs.Tests.Unit/Snp).
    // ============================================================================================================

    // ============================================================================================================
    // SNP-005 CopyOnWritePreservesOldVersions
    // ============================================================================================================

    public enum CowMutation
    {
        PartialOverwrite,
        WholeOverwrite,
        Truncate,
        TruncateThenExtend,
        AppendAcrossInline,
        DeleteFile,
        DeleteDirectoryRecursive,
        RenameFile,
        RenameDirectory,
        RecreateSameName,
    }

    /// <summary>
    /// SNP-005: after a snapshot, live overwrite/truncate/extend/append/delete/rename/recreate leaves every historical
    /// byte, name, inode identity and revision of the snapshot readable — also after GC passes and a second round of
    /// changes with a second snapshot; the snapshot's objects keep their durable copies.
    /// </summary>
    [Theory]
    [InlineData(CowMutation.PartialOverwrite)]
    [InlineData(CowMutation.WholeOverwrite)]
    [InlineData(CowMutation.Truncate)]
    [InlineData(CowMutation.TruncateThenExtend)]
    [InlineData(CowMutation.AppendAcrossInline)]
    [InlineData(CowMutation.DeleteFile)]
    [InlineData(CowMutation.DeleteDirectoryRecursive)]
    [InlineData(CowMutation.RenameFile)]
    [InlineData(CowMutation.RenameDirectory)]
    [InlineData(CowMutation.RecreateSameName)]
    [Catalog("SNP-005", "S", "INV-02", "INV-04", "INV-09", "INV-10", "INV-13", "INV-19")]
    public async Task SNP_005_CopyOnWritePreservesOldVersions(CowMutation mutation)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(Deterministic(), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(null, ct);
            DfsVolume vol = client.GetVolume(Volume);
            SortedDictionary<string, byte[]?> fixture = await CreateCowFixtureAsync(vol, ct);
            IReadOnlyList<ObjectId> bigObjects = await ObjectsOfAsync(cluster, "/cow/big");

            DfsSnapshotInfo s1 = await vol.Snapshots.CreateAsync("s1", null, ct);
            SortedDictionary<string, byte[]?> s1Tree = await CaptureSnapshotAsync(vol, "s1", "/cow", ct);
            SnapshotTree.AssertEqual(fixture, s1Tree, "s1 captures the fixture");
            Dictionary<string, DfsFileInfo> s1Infos = await SnapshotFileInfosAsync(vol, "s1", s1Tree, ct);

            SortedDictionary<string, byte[]?> expectedLive = SnapshotTree.Clone(fixture);
            await ApplyCowMutationAsync(vol, mutation, expectedLive, ct);

            await RunGcPassesAsync(cluster, 8, ct);
            SnapshotTree.AssertEqual(expectedLive, await SnapshotTree.CaptureAsync(vol, "/cow", ct), $"live after {mutation}");
            SnapshotTree.AssertEqual(s1Tree, await CaptureSnapshotAsync(vol, "s1", "/cow", ct), $"s1 after live {mutation} and GC");
            await AssertSnapshotInfosUnchangedAsync(vol, "s1", s1Infos, ct);
            await AssertRetainedAsync(cluster, bigObjects, $"s1 still references the original /cow/big objects after {mutation}");

            DfsFileInfo s1Big = s1Infos["/cow/big"];
            if (mutation == CowMutation.RecreateSameName)
                Assert.NotEqual(s1Big.InodeId, (await vol.Files.GetInfoAsync("/cow/big", ct)).InodeId);
            if (mutation == CowMutation.RenameFile)
                Assert.Equal(s1Big.InodeId, (await vol.Files.GetInfoAsync("/cow/big-renamed", ct)).InodeId);

            // Round 2: a second snapshot of the mutated state, then further changes; both versions stay intact.
            DfsSnapshotInfo s2 = await vol.Snapshots.CreateAsync("s2", null, ct);
            Assert.True(s2.Boundary.CompareTo(s1.Boundary) > 0);
            SortedDictionary<string, byte[]?> s2Tree = await CaptureSnapshotAsync(vol, "s2", "/cow", ct);
            SnapshotTree.AssertEqual(expectedLive, s2Tree, "s2 captures the mutated state");
            await vol.Files.WriteAllBytesAsync("/cow/small", TestData.Bytes(0x5005_0099, 77), ct);
            await vol.Files.DeleteAsync("/cow/sparse", ct);
            await RunGcPassesAsync(cluster, 8, ct);
            SnapshotTree.AssertEqual(s1Tree, await CaptureSnapshotAsync(vol, "s1", "/cow", ct), "s1 after round 2");
            SnapshotTree.AssertEqual(s2Tree, await CaptureSnapshotAsync(vol, "s2", "/cow", ct), "s2 after round 2");
            await AssertSnapshotInfosUnchangedAsync(vol, "s1", s1Infos, ct);
        }, ct);
    }

    private static async Task<SortedDictionary<string, byte[]?>> CreateCowFixtureAsync(DfsVolume vol, CancellationToken ct)
    {
        byte[] small = TestData.Bytes(0x5005_0001, 100);
        byte[] big = TestData.Bytes(0x5005_0002, (3 * Chunk) + 17);
        byte[] tail = TestData.Bytes(0x5005_0003, 10);
        byte[] a = TestData.Bytes(0x5005_0004, 500);
        byte[] b = TestData.Bytes(0x5005_0005, Chunk + 1);
        await vol.Directories.CreateDirectoryAsync("/cow/dir", ct);
        await vol.Files.WriteAllBytesAsync("/cow/small", small, ct);
        await vol.Files.WriteAllBytesAsync("/cow/big", big, ct);
        await using (DfsStream sparse = await vol.Files.OpenAsync("/cow/sparse", FileMode.CreateNew, FileAccess.Write, FileShare.None, ct))
        {
            sparse.Position = 5L * Chunk;
            await sparse.WriteAsync(tail, ct);
            await sparse.FlushAsync(ct);
        }

        await vol.Files.WriteAllBytesAsync("/cow/dir/a", a, ct);
        await vol.Files.WriteAllBytesAsync("/cow/dir/b", b, ct);
        var sparseContent = new byte[(5 * Chunk) + tail.Length];
        tail.CopyTo(sparseContent, 5 * Chunk);
        return new SortedDictionary<string, byte[]?>(StringComparer.Ordinal)
        {
            ["/cow"] = null,
            ["/cow/big"] = big,
            ["/cow/dir"] = null,
            ["/cow/dir/a"] = a,
            ["/cow/dir/b"] = b,
            ["/cow/small"] = small,
            ["/cow/sparse"] = sparseContent,
        };
    }

    private static async Task ApplyCowMutationAsync(DfsVolume vol, CowMutation mutation, SortedDictionary<string, byte[]?> expected, CancellationToken ct)
    {
        byte[] big = expected["/cow/big"]!;
        switch (mutation)
        {
            case CowMutation.PartialOverwrite:
                {
                    byte[] patch = TestData.Bytes(0x5005_0101, 100);
                    var model = new ReferenceFileModel(big);
                    model.Write(Chunk + 5, patch);
                    model.Flush();
                    await using DfsStream s = await vol.Files.OpenAsync("/cow/big", FileMode.Open, FileAccess.ReadWrite, FileShare.Read, ct);
                    s.Position = Chunk + 5;
                    await s.WriteAsync(patch, ct);
                    await s.CommitAsync(ct);
                    expected["/cow/big"] = model.Committed;
                    break;
                }

            case CowMutation.WholeOverwrite:
                {
                    byte[] next = TestData.Bytes(0x5005_0102, 2 * Chunk);
                    await vol.Files.WriteAllBytesAsync("/cow/big", next, ct);
                    expected["/cow/big"] = next;
                    break;
                }

            case CowMutation.Truncate:
                {
                    await using DfsStream s = await vol.Files.OpenAsync("/cow/big", FileMode.Open, FileAccess.Write, FileShare.Read, ct);
                    await s.SetLengthAsync(Chunk / 2, ct);
                    await s.CommitAsync(ct);
                    expected["/cow/big"] = big[..(Chunk / 2)];
                    break;
                }

            case CowMutation.TruncateThenExtend:
                {
                    await using DfsStream s = await vol.Files.OpenAsync("/cow/big", FileMode.Open, FileAccess.Write, FileShare.Read, ct);
                    await s.SetLengthAsync(Chunk / 2, ct);
                    await s.CommitAsync(ct);
                    await s.SetLengthAsync(3L * Chunk, ct);
                    await s.CommitAsync(ct);
                    var extended = new byte[3 * Chunk];
                    big.AsSpan(0, Chunk / 2).CopyTo(extended);   // the rest are zeroes: clipped data never resurrects
                    expected["/cow/big"] = extended;
                    break;
                }

            case CowMutation.AppendAcrossInline:
                {
                    byte[] suffix = TestData.Bytes(0x5005_0103, 5000);
                    await vol.Files.AppendAllBytesAsync("/cow/small", suffix, ct);
                    expected["/cow/small"] = Concat(expected["/cow/small"]!, suffix);
                    break;
                }

            case CowMutation.DeleteFile:
                await vol.Files.DeleteAsync("/cow/big", ct);
                SetPath(expected, "/cow/big", null);
                break;
            case CowMutation.DeleteDirectoryRecursive:
                await vol.Directories.DeleteAsync("/cow/dir", true, ct);
                SetPath(expected, "/cow/dir", null);
                break;
            case CowMutation.RenameFile:
                await vol.Files.MoveAsync("/cow/big", "/cow/big-renamed", false, ct);
                MovePath(expected, "/cow/big", "/cow/big-renamed");
                break;
            case CowMutation.RenameDirectory:
                await vol.Directories.MoveAsync("/cow/dir", "/cow/dir-renamed", ct);
                MovePath(expected, "/cow/dir", "/cow/dir-renamed");
                break;
            case CowMutation.RecreateSameName:
                {
                    await vol.Files.DeleteAsync("/cow/big", ct);
                    byte[] next = TestData.Bytes(0x5005_0104, Chunk + 7);
                    await vol.Files.WriteAllBytesAsync("/cow/big", next, ct);
                    expected["/cow/big"] = next;
                    break;
                }

            default:
                throw new ArgumentOutOfRangeException(nameof(mutation), mutation, null);
        }
    }

    private static async Task<Dictionary<string, DfsFileInfo>> SnapshotFileInfosAsync(DfsVolume vol, string snapshot,
        IReadOnlyDictionary<string, byte[]?> tree, CancellationToken ct)
    {
        await using DfsSnapshotView view = await vol.Snapshots.OpenAsync(snapshot, ct);
        var infos = new Dictionary<string, DfsFileInfo>(StringComparer.Ordinal);
        foreach (KeyValuePair<string, byte[]?> entry in tree)
        {
            if (entry.Value is null)
                continue;
            DfsFileInfo info = await view.Files.GetInfoAsync(entry.Key, ct);
            Assert.Equal(entry.Value.LongLength, info.Length);
            infos[entry.Key] = info;
        }

        return infos;
    }

    private static async Task AssertSnapshotInfosUnchangedAsync(DfsVolume vol, string snapshot, Dictionary<string, DfsFileInfo> expected, CancellationToken ct)
    {
        await using DfsSnapshotView view = await vol.Snapshots.OpenAsync(snapshot, ct);
        foreach ((string path, DfsFileInfo before) in expected)
        {
            DfsFileInfo now = await view.Files.GetInfoAsync(path, ct);
            Assert.True(before.InodeId == now.InodeId, $"{snapshot}:{path} inode changed {before.InodeId.Value} -> {now.InodeId.Value}");
            Assert.True(before.Revision == now.Revision, $"{snapshot}:{path} revision changed {before.Revision} -> {now.Revision}");
            Assert.Equal(before.Length, now.Length);
            Assert.Equal(before.IsSparse, now.IsSparse);
        }
    }

    // ============================================================================================================
    // SNP-006 SnapshotCreateReplyLost
    // ============================================================================================================

    public enum CreateReplyLoss
    {
        SdkInternalRetry,
        ReplySendFaultDropped,
        ExplicitRetrySameClient,
        RetryFromNewClient,
        ReuseIdWithDifferentName,
    }

    /// <summary>
    /// SNP-006: snapshot creation commits but its reply is lost (network or reply-send fault). The SDK-internal retry, an
    /// application retry with the persisted operation id from the same or a new client, all return the same snapshot
    /// identity; there is exactly one snapshot record on every replica (one retention reference: deleting it once makes
    /// its exclusive objects reclaimable); reusing the id with a different name is rejected.
    /// </summary>
    [Theory]
    [InlineData(CreateReplyLoss.SdkInternalRetry)]
    [InlineData(CreateReplyLoss.ReplySendFaultDropped)]
    [InlineData(CreateReplyLoss.ExplicitRetrySameClient)]
    [InlineData(CreateReplyLoss.RetryFromNewClient)]
    [InlineData(CreateReplyLoss.ReuseIdWithDifferentName)]
    [Catalog("SNP-006", "S", "INV-02", "INV-04", "INV-09", "INV-10", "INV-13", "INV-19")]
    public async Task SNP_006_SnapshotCreateReplyLost(CreateReplyLoss kind)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(Deterministic(), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0", OperationTimeout = TimeSpan.FromSeconds(3) }, ct);
            DfsVolume vol = client.GetVolume(Volume);
            byte[] v1 = TestData.Bytes(0x5006_0001, (2 * Chunk) + 3);
            await vol.Directories.CreateDirectoryAsync("/s", ct);
            await vol.Files.WriteAllBytesAsync("/s/f", v1, ct);
            IReadOnlyList<ObjectId> v1Objects = await ObjectsOfAsync(cluster, "/s/f");
            const string name = "s1";

            DfsSnapshotInfo info;
            switch (kind)
            {
                case CreateReplyLoss.SdkInternalRetry:
                    {
                        MessageRule rule = cluster.Network.DropReplies(new MessageFilter(Type: MessageType.CreateSnapshot), 1);
                        info = await CreateRetryingUnknownAsync(vol, name, ct);
                        Assert.Equal(1, rule.Matched);
                        break;
                    }

                case CreateReplyLoss.ReplySendFaultDropped:
                    {
                        FaultArm arm = cluster.Faults.Arm(new FaultMatch(FaultSites.RpcReplySend, Operation: DfsOperationKind.CreateSnapshot),
                            FaultAction.DropReply());
                        info = await CreateRetryingUnknownAsync(vol, name, ct);
                        cluster.Faults.AssertReached(arm);
                        break;
                    }

                default:
                    {
                        DfsOperationId opId = await vol.NewOperationIdAsync(ct);
                        MessageRule rule = cluster.Network.DropReplies(new MessageFilter(Type: MessageType.CreateSnapshot), 10_000);

                        // The replies to the outcome queries the SDK sends to resolve the creation itself are lost too
                        // (PROTOCOL.md §5.1): otherwise it learns the committed outcome and the uncertainty never surfaces.
                        MessageRule lostQueries = cluster.Network.DropReplies(new MessageFilter(From: "client:c0", Type: MessageType.QueryOperation), 10_000);
                        DfsCommitOutcomeUnknownException unknown = await AssertEx.ThrowsDfsAsync<DfsCommitOutcomeUnknownException>(
                            () => vol.Snapshots.CreateAsync(name, new DfsSnapshotOptions { OperationId = opId }, ct), DfsErrorCode.OutcomeUnknown);
                        Assert.Equal(opId, unknown.OperationId);
                        Assert.True(rule.Matched >= 1, "the reply drop must have been reached");
                        rule.Cancel();
                        lostQueries.Cancel();

                        // While uncertain, there is never more than one record for the name.
                        Assert.True((await vol.Snapshots.ListAsync(ct)).Count(s => s.Name == name) <= 1);

                        if (kind == CreateReplyLoss.ReuseIdWithDifferentName)
                        {
                            await AssertEx.ThrowsDfsAsync<DfsOperationIdMismatchException>(
                                () => vol.Snapshots.CreateAsync("other-name", new DfsSnapshotOptions { OperationId = opId }, ct), DfsErrorCode.OperationIdMismatch);
                            await AssertEx.ThrowsDfsAsync<DfsException>(() => vol.Snapshots.GetAsync("other-name", ct), DfsErrorCode.SnapshotNotFound);
                        }

                        if (kind == CreateReplyLoss.RetryFromNewClient)
                        {
                            await using DfsClient second = await cluster.ConnectAsync(new TestClientOptions { Name = "c1" }, ct);
                            info = await second.GetVolume(Volume).Snapshots.CreateAsync(name, new DfsSnapshotOptions { OperationId = opId }, ct);
                        }
                        else
                        {
                            info = await vol.Snapshots.CreateAsync(name, new DfsSnapshotOptions { OperationId = opId }, ct);
                        }

                        DfsOperationOutcome outcome = await vol.QueryOperationAsync(opId, false, ct);
                        Assert.Equal(DfsOutcomeStatus.Committed, outcome.Status);
                        Assert.False(outcome.Token.IsNone);
                        break;
                    }
            }

            DfsSnapshotInfo stored = await vol.Snapshots.GetAsync(name, ct);
            AssertSnapshotInfoEqual(stored, info, "the retried creation returns the original snapshot identity");
            Assert.Single(await vol.Snapshots.ListAsync(ct));
            await cluster.WaitForReplicationAsync(Volume, ct);
            foreach (TestNode node in cluster.Nodes)
            {
                VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Volume, node.Name);
                SnapshotInspection record = Assert.Single(state.Snapshots);
                Assert.Equal(info.Id, record.Id);
                Assert.Equal(name, record.Name);
            }

            // Exactly one retention reference: after one delete, the snapshot-exclusive objects become reclaimable.
            await vol.Files.WriteAllBytesAsync("/s/f", TestData.Bytes(0x5006_0002, 64), ct);
            await RunGcPassesAsync(cluster, 6, ct);
            await AssertRetainedAsync(cluster, v1Objects, "the snapshot still references v1");
            await vol.Snapshots.DeleteAsync(name, ct);
            await AssertEx.ThrowsDfsAsync<DfsException>(() => vol.Snapshots.GetAsync(name, ct), DfsErrorCode.SnapshotNotFound);
            await WaitForReclaimAsync(cluster, v1Objects.ToArray(), "no duplicate retention reference may keep v1 alive after the single delete", ct: ct);
        }, ct);
    }

    /// <summary>
    /// Creates a snapshot whose first reply is lost. Both legal SDK behaviours are accepted (FSD §13, ARCHITECTURE §7/§9.2):
    /// an SDK-internal retry under the same operation id returns the stored outcome, or the SDK reports
    /// <see cref="DfsCommitOutcomeUnknownException"/> carrying the id and the application retries with that id. Either way
    /// the result must be the original snapshot (checked by the caller); a definite failure or a second record is a bug.
    /// </summary>
    private static async Task<DfsSnapshotInfo> CreateRetryingUnknownAsync(DfsVolume vol, string name, CancellationToken ct)
    {
        try
        {
            return await vol.Snapshots.CreateAsync(name, null, ct);
        }
        catch (DfsCommitOutcomeUnknownException unknown)
        {
            Assert.False(unknown.OperationId.IsEmpty, "an unknown outcome must carry the operation id for resolution");
            Assert.True((await vol.Snapshots.ListAsync(ct)).Count(s => s.Name == name) <= 1, "never more than one record while uncertain");
            return await vol.Snapshots.CreateAsync(name, new DfsSnapshotOptions { OperationId = unknown.OperationId }, ct);
        }
    }

    // ============================================================================================================
    // SNP-007 MinimumCommitBoundaryRespected
    // ============================================================================================================

    public enum MinimumSource
    {
        OwnLatestCommit,
        OtherClientCommit,
        AfterLeaderCrash,
        IsolatedFollowerSeedOnly,
        FutureSequence,
    }

    /// <summary>
    /// SNP-007: a snapshot requested with MinimumCommit = T returns a same-cluster/volume/epoch boundary at or after T
    /// that contains T's effect — from the committing client, from a fresh client, after the leader crashed, and from a
    /// client seeded only with a partitioned follower — and is never older than T. A token beyond the current history
    /// yields an error/timeout, never an older boundary.
    /// </summary>
    [Theory]
    [InlineData(MinimumSource.OwnLatestCommit)]
    [InlineData(MinimumSource.OtherClientCommit)]
    [InlineData(MinimumSource.AfterLeaderCrash)]
    [InlineData(MinimumSource.IsolatedFollowerSeedOnly)]
    [InlineData(MinimumSource.FutureSequence)]
    [Catalog("SNP-007", "S", "INV-02", "INV-04", "INV-09", "INV-10", "INV-13", "INV-19")]
    public async Task SNP_007_MinimumCommitBoundaryRespected(MinimumSource source)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(Deterministic(), async cluster =>
        {
            await using DfsClient writerClient = await cluster.ConnectAsync(new TestClientOptions { Name = "writer" }, ct);
            DfsVolume writer = writerClient.GetVolume(Volume);
            await writer.Directories.CreateDirectoryAsync("/m", ct);
            for (int i = 0; i < 20; i++)
                await writer.Files.WriteAllBytesAsync($"/m/noise{i % 4}", TestData.Bytes(0x5007_0000UL + (ulong)i, 300), ct);

            TestNode? isolated = null;
            if (source == MinimumSource.IsolatedFollowerSeedOnly)
            {
                TestNode leader = await cluster.GetLeaderAsync(Volume, ct);
                isolated = cluster.Nodes.First(n => n.Name != leader.Name);
                foreach (TestNode other in cluster.Nodes.Where(n => n.Name != isolated.Name))
                    cluster.Network.CutBoth(isolated.Name, other.Name);   // nodes only; client links stay up
            }

            byte[] target = TestData.Bytes(0x5007_0100, 3000);
            CommitToken t = await writer.Files.WriteAllBytesAsync("/m/target", target, ct);

            switch (source)
            {
                case MinimumSource.OwnLatestCommit:
                    await AssertAtOrAfterAsync(writer, "own", t, target, ct);
                    break;
                case MinimumSource.OtherClientCommit:
                    {
                        await using DfsClient fresh = await cluster.ConnectAsync(new TestClientOptions { Name = "fresh" }, ct);
                        await AssertAtOrAfterAsync(fresh.GetVolume(Volume), "fresh", t, target, ct);
                        break;
                    }

                case MinimumSource.AfterLeaderCrash:
                    {
                        TestNode leader = await cluster.GetLeaderAsync(Volume, ct);
                        await leader.CrashAsync(CrashMode.LoseUnflushed);
                        await using DfsClient fresh = await cluster.ConnectAsync(new TestClientOptions
                        {
                            Name = "fresh",
                            SeedNodes = cluster.Nodes.Where(n => n.Name != leader.Name).Select(n => n.Name).ToArray(),
                        }, ct);
                        await AssertAtOrAfterAsync(fresh.GetVolume(Volume), "after-crash", t, target, ct);
                        await leader.RestartAsync();
                        break;
                    }

                case MinimumSource.IsolatedFollowerSeedOnly:
                    {
                        Exception? error = await CaptureAsync(async () =>
                        {
                            await using DfsClient viaFollower = await cluster.ConnectAsync(new TestClientOptions
                            {
                                Name = "via-follower",
                                SeedNodes = [isolated!.Name],
                                OperationTimeout = TimeSpan.FromSeconds(3),
                                ConnectTimeout = TimeSpan.FromSeconds(3),
                            }, ct);
                            await AssertAtOrAfterAsync(viaFollower.GetVolume(Volume), "via-follower", t, target, ct);
                        });
                        if (error is not null)
                        {
                            Assert.False(error is Xunit.Sdk.XunitException, $"the follower-routed snapshot violated the minimum boundary: {error.Message}");
                            Assert.True(error is DfsUnavailableException or TimeoutException or DfsCommitOutcomeUnknownException
                                || (error is DfsException d && d.ErrorCode is DfsErrorCode.NotLeader or DfsErrorCode.Timeout or DfsErrorCode.Unavailable
                                    or DfsErrorCode.QuorumUnavailable or DfsErrorCode.NodeUnavailable or DfsErrorCode.OutcomeUnknown),
                                $"a client that can only reach a partitioned follower may fail with an availability/unknown-outcome error, never with an older boundary: {error}");
                            Assert.False(await SnapshotExistsAsync(writer, "via-follower", ct) && !(await writer.Snapshots.GetAsync("via-follower", ct)).Boundary.IsAtOrAfter(t),
                                "no snapshot older than T may be created");
                        }

                        cluster.Network.Heal();
                        await using DfsClient healed = await cluster.ConnectAsync(new TestClientOptions { Name = "healed", SeedNodes = [isolated!.Name] }, ct);
                        await AssertAtOrAfterAsync(healed.GetVolume(Volume), "after-heal", t, target, ct);

                        // A request that was in flight while partitioned may still commit after the heal; it can never be older than T.
                        if (await SnapshotExistsAsync(writer, "via-follower", ct))
                            Assert.True((await writer.Snapshots.GetAsync("via-follower", ct)).Boundary.IsAtOrAfter(t), "no snapshot older than T may ever be created");
                        break;
                    }

                case MinimumSource.FutureSequence:
                    {
                        CommitToken future = t with { Sequence = t.Sequence + 10_000 };
                        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5), cluster.Time);
                        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, cts.Token);
                        Exception? error = await CaptureAsync(() => writer.Snapshots.CreateAsync("future", new DfsSnapshotOptions { MinimumCommit = future }, linked.Token));
                        await cluster.DelayAsync(TimeSpan.FromSeconds(3), ct);
                        if (error is null || await SnapshotExistsAsync(writer, "future", ct))
                        {
                            DfsSnapshotInfo created = await writer.Snapshots.GetAsync("future", ct);
                            Assert.True(created.Boundary.IsAtOrAfter(future),
                                $"a snapshot for an unreached minimum must not be created with an older boundary ({created.Boundary} < {future})");
                        }

                        Assert.True(error is not null, "the volume never reaches the requested sequence, so creation cannot succeed");
                        break;
                    }

                default:
                    throw new ArgumentOutOfRangeException(nameof(source), source, null);
            }
        }, ct);
    }

    private static async Task AssertAtOrAfterAsync(DfsVolume vol, string name, CommitToken t, byte[] target, CancellationToken ct)
    {
        DfsSnapshotInfo info = await vol.Snapshots.CreateAsync(name, new DfsSnapshotOptions { MinimumCommit = t }, ct);
        Assert.True(info.Boundary.IsComparableWith(t), $"boundary {info.Boundary} must share cluster, volume and epoch with {t}");
        Assert.Equal(t.ClusterId, info.Boundary.ClusterId);
        Assert.Equal(t.VolumeId, info.Boundary.VolumeId);
        Assert.Equal(t.VolumeEpoch, info.Boundary.VolumeEpoch);
        Assert.True(info.Boundary.IsAtOrAfter(t), $"boundary {info.Boundary} must be at or after {t}");
        Assert.Equal(DfsConsistencyLabel.FilesystemConsistent, info.Label);
        await using DfsSnapshotView view = await vol.Snapshots.OpenAsync(name, ct);
        await AssertBytesAsync(target, view.Files.ReadAllBytesAsync("/m/target", ct), $"snapshot {name} contains the effect of T");
    }

    private static async Task<bool> SnapshotExistsAsync(DfsVolume vol, string name, CancellationToken ct) =>
        (await vol.Snapshots.ListAsync(ct)).Any(s => s.Name == name);

    // ============================================================================================================
    // SNP-008 MinimumCommitIsNotExactHistory
    // ============================================================================================================

    public enum LaterChange
    {
        Overwrite,
        Delete,
        Truncate,
        OverwriteWhileSnapshotRequestHeld,
    }

    /// <summary>
    /// SNP-008: when /h/f is changed after T and before capture, a snapshot with MinimumCommit = T captures the later
    /// state (boundary after the change, not T) and must not present T's version; only commit-and-snapshot preserves an
    /// exact version (its boundary equals the commit token and survives later overwrite/delete).
    /// </summary>
    [Theory]
    [InlineData(LaterChange.Overwrite)]
    [InlineData(LaterChange.Delete)]
    [InlineData(LaterChange.Truncate)]
    [InlineData(LaterChange.OverwriteWhileSnapshotRequestHeld)]
    [Catalog("SNP-008", "S", "INV-02", "INV-04", "INV-09", "INV-10", "INV-13", "INV-19")]
    public async Task SNP_008_MinimumCommitIsNotExactHistory(LaterChange change)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(Deterministic(), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, ct);
            await using DfsClient other = await cluster.ConnectAsync(new TestClientOptions { Name = "c1" }, ct);
            DfsVolume vol = client.GetVolume(Volume);
            DfsVolume otherVol = other.GetVolume(Volume);
            byte[] v1 = TestData.Bytes(0x5008_0001, Chunk + 100);
            byte[] v2 = TestData.Bytes(0x5008_0002, 700);
            await vol.Directories.CreateDirectoryAsync("/h", ct);
            CommitToken t = await vol.Files.WriteAllBytesAsync("/h/f", v1, ct);

            CommitToken t2;
            byte[]? expected;
            DfsSnapshotInfo info;
            if (change == LaterChange.OverwriteWhileSnapshotRequestHeld)
            {
                using MessageHold hold = cluster.Network.Hold(new MessageFilter(Type: MessageType.CreateSnapshot, Direction: LinkDirection.Request));
                Task<DfsSnapshotInfo> snap = otherVol.Snapshots.CreateAsync("after-t", new DfsSnapshotOptions { MinimumCommit = t }, ct);
                await hold.WaitForAsync(1, TimeSpan.FromSeconds(10));
                t2 = await vol.Files.WriteAllBytesAsync("/h/f", v2, ct);
                expected = v2;
                hold.ReleaseAll();
                info = await snap;
            }
            else
            {
                switch (change)
                {
                    case LaterChange.Overwrite:
                        t2 = await vol.Files.WriteAllBytesAsync("/h/f", v2, ct);
                        expected = v2;
                        break;
                    case LaterChange.Delete:
                        t2 = await vol.Files.DeleteAsync("/h/f", ct);
                        expected = null;
                        break;
                    case LaterChange.Truncate:
                        {
                            await using DfsStream s = await vol.Files.OpenAsync("/h/f", FileMode.Open, FileAccess.Write, FileShare.Read, ct);
                            await s.SetLengthAsync(10, ct);
                            t2 = await s.CommitAsync(ct);
                            expected = v1[..10];
                            break;
                        }

                    default:
                        throw new ArgumentOutOfRangeException(nameof(change), change, null);
                }

                info = await otherVol.Snapshots.CreateAsync("after-t", new DfsSnapshotOptions { MinimumCommit = t }, ct);
            }

            Assert.True(info.Boundary.IsAtOrAfter(t));
            Assert.True(info.Boundary.IsAtOrAfter(t2), $"the change at {t2} completed before the snapshot was captured, so boundary {info.Boundary} must include it");
            Assert.NotEqual(t, info.Boundary);
            Assert.Equal(DfsConsistencyLabel.FilesystemConsistent, info.Label);
            await AssertFileInSnapshotAsync(vol, "after-t", "/h/f", expected, "a minimum-boundary snapshot captures the later state");

            // The exact contract: commit-and-snapshot keeps precisely the committed version.
            byte[] v3 = TestData.Bytes(0x5008_0003, (2 * Chunk) + 1);
            CommitToken t3 = await vol.Files.WriteAllBytesAsync("/h/f", v3, new DfsMutationOptions { CommitSnapshotName = "exact" }, ct);
            DfsSnapshotInfo exact = await vol.Snapshots.GetAsync("exact", ct);
            Assert.Equal(t3, exact.Boundary);
            await vol.Files.WriteAllBytesAsync("/h/f", TestData.Bytes(0x5008_0004, 10), ct);
            await vol.Files.DeleteAsync("/h/f", ct);
            await RunGcPassesAsync(cluster, 6, ct);
            await AssertFileInSnapshotAsync(vol, "exact", "/h/f", v3, "commit-and-snapshot retains the exact version");
            await AssertFileInSnapshotAsync(vol, "after-t", "/h/f", expected, "the minimum-boundary snapshot is unchanged and never shows T's version");
        }, ct);
    }

    private static async Task AssertFileInSnapshotAsync(DfsVolume vol, string snapshot, string path, byte[]? expected, string because)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using DfsSnapshotView view = await vol.Snapshots.OpenAsync(snapshot, ct);
        if (expected is null)
        {
            Assert.False(await view.Files.ExistsAsync(path, ct), $"{because}: '{path}' must be absent in {snapshot}");
            return;
        }

        await AssertBytesAsync(expected, view.Files.ReadAllBytesAsync(path, ct), because);
    }

    // ============================================================================================================
    // SNP-009 CommitAndSnapshotPreservesExactVersion
    // ============================================================================================================

    public enum ExactCommitPath
    {
        WriteAllBytes,
        AppendAllBytes,
        Copy,
        FileMove,
        Delete,
        CreateDirectory,
        DirectoryMove,
        StreamCommitMultiExtent,
        TransactionCommitWithSnapshot,
        WriteAllBytesUnderConcurrentLoad,
    }

    /// <summary>
    /// SNP-009: every commit path that accepts a snapshot name creates the snapshot in the same metadata decision: its
    /// boundary equals the returned commit token and its tree equals exactly the volume state at that commit (including
    /// under a concurrent writer); immediately overwriting/deleting afterwards (and GC) does not change it.
    /// </summary>
    [Theory]
    [InlineData(ExactCommitPath.WriteAllBytes)]
    [InlineData(ExactCommitPath.AppendAllBytes)]
    [InlineData(ExactCommitPath.Copy)]
    [InlineData(ExactCommitPath.FileMove)]
    [InlineData(ExactCommitPath.Delete)]
    [InlineData(ExactCommitPath.CreateDirectory)]
    [InlineData(ExactCommitPath.DirectoryMove)]
    [InlineData(ExactCommitPath.StreamCommitMultiExtent)]
    [InlineData(ExactCommitPath.TransactionCommitWithSnapshot)]
    [InlineData(ExactCommitPath.WriteAllBytesUnderConcurrentLoad)]
    [Catalog("SNP-009", "S", "INV-02", "INV-04", "INV-09", "INV-10", "INV-13", "INV-19")]
    public async Task SNP_009_CommitAndSnapshotPreservesExactVersion(ExactCommitPath path)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(Deterministic(), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, ct);
            await using DfsClient loadClient = await cluster.ConnectAsync(new TestClientOptions { Name = "load" }, ct);
            DfsVolume vol = client.GetVolume(Volume);
            DfsVolume load = loadClient.GetVolume(Volume);
            const string exactName = "exact";
            var opts = new DfsMutationOptions { CommitSnapshotName = exactName };

            byte[] f0 = TestData.Bytes(0x5009_0001, (3 * Chunk) + 5);
            byte[] g0 = TestData.Bytes(0x5009_0002, 300);
            byte[] a0 = TestData.Bytes(0x5009_0003, 1000);
            await vol.Directories.CreateDirectoryAsync("/x/dir", ct);
            var oracle = new CommitHistoryOracle();
            oracle.Record(await vol.Files.WriteAllBytesAsync("/x/f", f0, ct), "/x/f", f0);
            oracle.Record(await vol.Files.WriteAllBytesAsync("/x/g", g0, ct), "/x/g", g0);
            oracle.Record(await vol.Files.WriteAllBytesAsync("/x/dir/a", a0, ct), "/x/dir/a", a0);
            SortedDictionary<string, byte[]?> expected = await SnapshotTree.CaptureAsync(vol, "/x", ct);

            using var stopLoad = new CancellationTokenSource();
            Task loadTask = path == ExactCommitPath.WriteAllBytesUnderConcurrentLoad
                ? LoadWriterAsync(cluster, load, oracle, stopLoad.Token)
                : Task.CompletedTask;
            int commitsBeforeLoad = oracle.Count;
            if (path == ExactCommitPath.WriteAllBytesUnderConcurrentLoad)
            {
                await cluster.WaitUntilAsync(() => Task.FromResult(oracle.Count >= commitsBeforeLoad + 5), TimeSpan.FromSeconds(30),
                    "the concurrent writer must be committing before the commit-and-snapshot", ct);
            }

            CommitToken? token = null;
            switch (path)
            {
                case ExactCommitPath.WriteAllBytes:
                case ExactCommitPath.WriteAllBytesUnderConcurrentLoad:
                    {
                        byte[] f1 = TestData.Bytes(0x5009_0011, (2 * Chunk) + 77);
                        token = await vol.Files.WriteAllBytesAsync("/x/f", f1, opts, ct);
                        expected["/x/f"] = f1;
                        oracle.Record(token.Value, "/x/f", f1);
                        break;
                    }

                case ExactCommitPath.AppendAllBytes:
                    {
                        byte[] suffix = TestData.Bytes(0x5009_0012, 5000);
                        token = await vol.Files.AppendAllBytesAsync("/x/g", suffix, opts, ct);
                        expected["/x/g"] = Concat(g0, suffix);
                        break;
                    }

                case ExactCommitPath.Copy:
                    token = await vol.Files.CopyAsync("/x/f", "/x/f-copy", false, opts, ct);
                    expected["/x/f-copy"] = f0;
                    break;
                case ExactCommitPath.FileMove:
                    token = await vol.Files.MoveAsync("/x/g", "/x/g-moved", false, opts, ct);
                    MovePath(expected, "/x/g", "/x/g-moved");
                    break;
                case ExactCommitPath.Delete:
                    token = await vol.Files.DeleteAsync("/x/g", opts, ct);
                    SetPath(expected, "/x/g", null);
                    break;
                case ExactCommitPath.CreateDirectory:
                    await vol.Directories.CreateDirectoryAsync("/x/newdir", opts, ct);
                    expected["/x/newdir"] = null;
                    break;
                case ExactCommitPath.DirectoryMove:
                    token = await vol.Directories.MoveAsync("/x/dir", "/x/dir-moved", opts, ct);
                    MovePath(expected, "/x/dir", "/x/dir-moved");
                    break;
                case ExactCommitPath.StreamCommitMultiExtent:
                    {
                        var model = new ReferenceFileModel(f0);
                        await using DfsStream s = await vol.Files.OpenAsync("/x/f", FileMode.Open, FileAccess.ReadWrite, FileShare.Read, ct);
                        foreach (long offset in new long[] { 7, Chunk - 3, (2L * Chunk) + 11 })
                        {
                            byte[] patch = TestData.Bytes(0x5009_0013 + (ulong)offset, 40);
                            s.Position = offset;
                            await s.WriteAsync(patch, ct);
                            model.Write(offset, patch);
                        }

                        await s.SetLengthAsync((4L * Chunk) + 1, ct);
                        model.SetLength((4L * Chunk) + 1);
                        token = await s.CommitAsync(opts, ct);
                        model.Flush();
                        expected["/x/f"] = model.Committed;
                        break;
                    }

                case ExactCommitPath.TransactionCommitWithSnapshot:
                    {
                        byte[] f1 = TestData.Bytes(0x5009_0014, Chunk + 9);
                        byte[] h1 = TestData.Bytes(0x5009_0015, 123);
                        await using DfsTransaction tx = await vol.Transactions.BeginAsync(null, ct);
                        await tx.Files.WriteAllBytesAsync("/x/f", f1, ct);
                        await tx.Files.DeleteAsync("/x/g", ct);
                        await tx.Files.WriteAllBytesAsync("/x/h", h1, ct);
                        DfsSnapshotInfo created = await tx.CommitWithSnapshotAsync(exactName, ct);
                        Assert.Equal(DfsTransactionState.Committed, tx.State);
                        token = created.Boundary;
                        expected["/x/f"] = f1;
                        SetPath(expected, "/x/g", null);
                        expected["/x/h"] = h1;
                        break;
                    }

                default:
                    throw new ArgumentOutOfRangeException(nameof(path), path, null);
            }

            if (path == ExactCommitPath.WriteAllBytesUnderConcurrentLoad)
            {
                // Keep the load running until it has committed after the exact commit too (the boundary is interior).
                CommitToken exactCommit = token!.Value;
                await cluster.WaitUntilAsync(
                    () => Task.FromResult(oracle.LatestAtOrBefore(exactCommit with { Sequence = ulong.MaxValue }) is { } latest && latest.CompareTo(exactCommit) > 0),
                    TimeSpan.FromSeconds(30), "the concurrent writer must also commit after the commit-and-snapshot", ct);
            }

            stopLoad.Cancel();
            await loadTask;

            DfsSnapshotInfo exact = await vol.Snapshots.GetAsync(exactName, ct);
            Assert.Equal(DfsConsistencyLabel.FilesystemConsistent, exact.Label);
            if (token is not null)
                Assert.True(token.Value == exact.Boundary, $"the snapshot boundary {exact.Boundary} must be the commit itself ({token})");

            // Immediately overwrite and delete everything the snapshot captured.
            await vol.Files.WriteAllBytesAsync("/x/f", TestData.Bytes(0x5009_0099, 10), ct);
            foreach (string file in new[] { "/x/g", "/x/g-moved", "/x/f-copy", "/x/h" })
                await vol.Files.DeleteAsync(file, ct);
            foreach (string dir in new[] { "/x/dir", "/x/dir-moved", "/x/newdir" })
            {
                if (await vol.Directories.ExistsAsync(dir, ct))
                    await vol.Directories.DeleteAsync(dir, true, ct);
            }

            await RunGcPassesAsync(cluster, 8, ct);
            SortedDictionary<string, byte[]?> tree = await CaptureSnapshotAsync(vol, exactName, "/x", ct);
            if (path == ExactCommitPath.WriteAllBytesUnderConcurrentLoad)
            {
                oracle.AssertMatches(exact.Boundary, SnapshotTree.FilesOf(tree), "exact snapshot under concurrent load");
                Assert.Contains(SnapshotTree.FilesOf(tree).Keys, k => k.StartsWith("/x/load", StringComparison.Ordinal));
                Assert.True(oracle.Count >= commitsBeforeLoad + 6, "the concurrent writer committed before and after the exact commit");
            }
            else
            {
                SnapshotTree.AssertEqual(expected, tree, $"{path}: the snapshot holds exactly the committed state");
            }
        }, ct);
    }

    /// <summary>
    /// SNP-009 (atomicity of the combined decision): when the snapshot name already exists, the commit-and-snapshot
    /// fails as a whole — the data change is not published and the existing snapshot is untouched.
    /// </summary>
    [Fact]
    [Catalog("SNP-009", "S", "INV-02", "INV-04", "INV-09", "INV-10", "INV-13", "INV-19")]
    public async Task SNP_009_CommitAndSnapshotPreservesExactVersion_NameCollisionIsAtomic()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(Deterministic(), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(null, ct);
            DfsVolume vol = client.GetVolume(Volume);
            byte[] f0 = TestData.Bytes(0x5009_1001, Chunk + 3);
            await vol.Directories.CreateDirectoryAsync("/x", ct);
            await vol.Files.WriteAllBytesAsync("/x/f", f0, ct);
            DfsSnapshotInfo taken = await vol.Snapshots.CreateAsync("taken", null, ct);
            DfsFileInfo before = await vol.Files.GetInfoAsync("/x/f", ct);

            await AssertEx.ThrowsDfsAsync<DfsAlreadyExistsException>(
                () => vol.Files.WriteAllBytesAsync("/x/f", TestData.Bytes(0x5009_1002, 50), new DfsMutationOptions { CommitSnapshotName = "taken" }, ct),
                DfsErrorCode.AlreadyExists);
            await AssertEx.ThrowsDfsAsync<DfsAlreadyExistsException>(
                () => vol.Files.AppendAllBytesAsync("/x/f", TestData.Bytes(0x5009_1003, 50), new DfsMutationOptions { CommitSnapshotName = "taken" }, ct),
                DfsErrorCode.AlreadyExists);

            await AssertBytesAsync(f0, vol.Files.ReadAllBytesAsync("/x/f", ct), "the failed combined decision published nothing");
            Assert.Equal(before.Revision, (await vol.Files.GetInfoAsync("/x/f", ct)).Revision);
            AssertSnapshotInfoEqual(taken, await vol.Snapshots.GetAsync("taken", ct), "the existing snapshot is untouched");
            Assert.Single(await vol.Snapshots.ListAsync(ct));
        }, ct);
    }

    private static async Task LoadWriterAsync(TestCluster cluster, DfsVolume vol, CommitHistoryOracle oracle, CancellationToken stop)
    {
        int i = 0;
        while (!stop.IsCancellationRequested)
        {
            string path = $"/x/load{i % 3}";
            byte[] bytes = TestData.Bytes(0x5009_5000UL + (ulong)i, 200 + (i % 7 * 900));
            CommitToken token = await vol.Files.WriteAllBytesAsync(path, bytes, CancellationToken.None);
            oracle.Record(token, path, bytes);
            i++;
            await cluster.DelayAsync(TimeSpan.FromMilliseconds(3), CancellationToken.None);
        }
    }

    // ============================================================================================================
    // SNP-010 is a unit-level test (Tedd.Tdfs.Tests.Unit/Snp).
    // ============================================================================================================

    // ============================================================================================================
    // SNP-011 DeleteSnapshotRespectsOtherReferences
    // ============================================================================================================

    public enum OtherReference
    {
        LiveState,
        OtherSnapshot,
        VersionPinnedReader,
        BackupTransfer,
    }

    /// <summary>
    /// SNP-011: deleting a snapshot whose objects are still referenced by live state, another snapshot, a version-pinned
    /// reader or an in-progress backup transfer reclaims none of them (checked continuously while GC runs) and the
    /// remaining reference reads exact bytes; objects referenced only by deleted snapshots are reclaimed (positive control
    /// proving GC ran).
    /// </summary>
    [Theory]
    [InlineData(OtherReference.LiveState)]
    [InlineData(OtherReference.OtherSnapshot)]
    [InlineData(OtherReference.VersionPinnedReader)]
    [InlineData(OtherReference.BackupTransfer)]
    [Catalog("SNP-011", "S", "INV-02", "INV-04", "INV-09", "INV-10", "INV-13", "INV-19")]
    public async Task SNP_011_DeleteSnapshotRespectsOtherReferences(OtherReference reference)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(Deterministic(backups: reference == OtherReference.BackupTransfer ? 1 : 0), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, ct);
            await using DfsClient readerClient = await cluster.ConnectAsync(new TestClientOptions { Name = "reader" }, ct);
            DfsVolume vol = client.GetVolume(Volume);

            // Positive control: objects only referenced by s0.
            await vol.Directories.CreateDirectoryAsync("/ref", ct);
            await vol.Files.WriteAllBytesAsync("/ref/ctl", TestData.Bytes(0x5011_0001, (2 * Chunk) + 3), ct);
            IReadOnlyList<ObjectId> ctlObjects = await ObjectsOfAsync(cluster, "/ref/ctl");
            await vol.Snapshots.CreateAsync("s0", null, ct);
            await vol.Files.WriteAllBytesAsync("/ref/ctl", TestData.Bytes(0x5011_0002, 100), ct);

            byte[] shared = TestData.Bytes(0x5011_0003, (3 * Chunk) + 11);
            await vol.Files.WriteAllBytesAsync("/ref/shared", shared, ct);
            IReadOnlyList<ObjectId> sharedObjects = await ObjectsOfAsync(cluster, "/ref/shared");

            DfsStream? pinned = null;
            MessageHold? hold = null;
            TestBackupNode? backup = null;
            try
            {
                switch (reference)
                {
                    case OtherReference.LiveState:
                        await vol.Snapshots.CreateAsync("s1", null, ct);
                        break;
                    case OtherReference.OtherSnapshot:
                        await vol.Snapshots.CreateAsync("s1", null, ct);
                        await vol.Snapshots.CreateAsync("s2", null, ct);
                        await vol.Files.WriteAllBytesAsync("/ref/shared", TestData.Bytes(0x5011_0004, 50), ct);
                        break;
                    case OtherReference.VersionPinnedReader:
                        pinned = await readerClient.GetVolume(Volume).Files.OpenAsync("/ref/shared", new DfsOpenOptions
                        {
                            Mode = FileMode.Open,
                            Access = FileAccess.Read,
                            Share = FileShare.ReadWrite | FileShare.Delete,
                            ReadView = DfsReadView.VersionPinned,
                        }, ct);
                        await vol.Snapshots.CreateAsync("s1", null, ct);
                        await vol.Files.WriteAllBytesAsync("/ref/shared", TestData.Bytes(0x5011_0005, 50), ct);
                        break;
                    case OtherReference.BackupTransfer:
                        {
                            await vol.Snapshots.CreateAsync("s1", null, ct);
                            await vol.Files.WriteAllBytesAsync("/ref/shared", TestData.Bytes(0x5011_0006, 50), ct);
                            backup = cluster.BackupNodes[0];
                            await backup.RegisterAsync(Volume, new DfsBackupSchedule(OnEverySnapshot: false));
                            hold = cluster.Network.Hold(new MessageFilter(To: backup.Name));
                            await vol.Backups.RequestSyncAsync(backup.TargetName, "s1", ct);
                            await cluster.WaitUntilAsync(async () =>
                            {
                                VolumeStateInspection st = await cluster.Inspect.GetVolumeStateAsync(Volume);
                                return st.Pins.Any(p => p.Kind == "Transfer" && p.Snapshot == "s1");
                            }, TimeSpan.FromSeconds(30), "the exporter must establish a transfer pin on s1 before contacting the target", ct);
                            break;
                        }

                    default:
                        throw new ArgumentOutOfRangeException(nameof(reference), reference, null);
                }

                await vol.Snapshots.DeleteAsync("s0", ct);
                await vol.Snapshots.DeleteAsync("s1", ct);
                await AssertEx.ThrowsDfsAsync<DfsException>(() => vol.Snapshots.GetAsync("s1", ct), DfsErrorCode.SnapshotNotFound);

                await WatchRetainedAsync(cluster, sharedObjects.ToArray(), GcInterval * 16, $"{reference} still references the shared objects", ct: ct);
                await WaitForReclaimAsync(cluster, ctlObjects.ToArray(), "objects referenced only by deleted snapshot s0 are reclaimed (GC ran)", ct: ct);
                await AssertRetainedAsync(cluster, sharedObjects, $"{reference} still references the shared objects after GC reclaimed s0's objects");

                switch (reference)
                {
                    case OtherReference.LiveState:
                        await AssertBytesAsync(shared, vol.Files.ReadAllBytesAsync("/ref/shared", ct), "live state");
                        break;
                    case OtherReference.OtherSnapshot:
                        await AssertFileInSnapshotAsync(vol, "s2", "/ref/shared", shared, "the other snapshot");
                        break;
                    case OtherReference.VersionPinnedReader:
                        pinned!.Position = 0;
                        await AssertBytesAsync(shared, ReadToEndAsync(pinned, ct), "the version-pinned reader");
                        await pinned.DisposeAsync();
                        pinned = null;
                        await WaitForReclaimAsync(cluster, sharedObjects.ToArray(), "after the last (pin) reference closes the objects become reclaimable", ct: ct);
                        break;
                    case OtherReference.BackupTransfer:
                        {
                            hold!.ReleaseAll();
                            hold.Dispose();
                            hold = null;
                            await cluster.WaitUntilAsync(async () => (await backup!.Inspect.GetPublishedSnapshotsAsync(Volume)).Contains("s1"),
                                TimeSpan.FromSeconds(60), "the transfer pinned before the delete completes and publishes s1", ct);
                            await using DfsClient backupClient = await cluster.ConnectBackupAsync(0, new TestClientOptions { Name = "breader", Credentials = backup!.ReaderCredentials }, ct);
                            await AssertFileInSnapshotAsync(backupClient.GetVolume(Volume), "s1", "/ref/shared", shared, "the transferred snapshot on the backup");
                            break;
                        }
                }
            }
            finally
            {
                hold?.Dispose();
                if (pinned is not null)
                    await pinned.DisposeAsync();
            }
        }, ct);
    }

    // ============================================================================================================
    // SNP-012 DeleteSnapshotWithActiveReader
    // ============================================================================================================

    public enum ReaderRelease
    {
        ViewDisposed,
        ReaderClientKilled,
    }

    /// <summary>
    /// SNP-012: deleting a snapshot's retention reference while a reader holds an open view succeeds (the name disappears
    /// for new opens), but the reader keeps reading exact bytes and directory listings for far longer than a pin
    /// lifetime while it stays alive; only after explicit close, or after the reader's client dies and its pin expires,
    /// are the snapshot-exclusive objects reclaimed.
    /// </summary>
    [Theory]
    [InlineData(ReaderRelease.ViewDisposed)]
    [InlineData(ReaderRelease.ReaderClientKilled)]
    [Catalog("SNP-012", "S", "INV-02", "INV-04", "INV-09", "INV-10", "INV-13", "INV-19")]
    public async Task SNP_012_DeleteSnapshotWithActiveReader(ReaderRelease release)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(Deterministic(), async cluster =>
        {
            await using DfsClient owner = await cluster.ConnectAsync(new TestClientOptions { Name = "owner" }, ct);
            DfsClient readerClient = await cluster.ConnectAsync(new TestClientOptions { Name = "reader" }, ct);
            DfsVolume vol = owner.GetVolume(Volume);
            byte[] v1 = TestData.Bytes(0x5012_0001, (4 * Chunk) + 9);
            byte[] side = TestData.Bytes(0x5012_0002, 333);
            await vol.Directories.CreateDirectoryAsync("/r/d", ct);
            await vol.Files.WriteAllBytesAsync("/r/f", v1, ct);
            await vol.Files.WriteAllBytesAsync("/r/d/side", side, ct);
            IReadOnlyList<ObjectId> v1Objects = await ObjectsOfAsync(cluster, "/r/f");
            await vol.Snapshots.CreateAsync("s1", null, ct);
            await vol.Files.WriteAllBytesAsync("/r/f", TestData.Bytes(0x5012_0003, 10), ct);

            DfsSnapshotView view = await readerClient.GetVolume(Volume).Snapshots.OpenAsync("s1", ct);
            try
            {
                byte[] head = new byte[Chunk];
                await using (DfsStream partial = await view.Files.OpenReadAsync("/r/f", ct))
                    Assert.Equal(Chunk, await partial.ReadAtLeastAsync(head, Chunk, true, ct));
                AssertEx.BytesEqual(v1.AsSpan(0, Chunk), head, "first chunk before the delete");

                await vol.Snapshots.DeleteAsync("s1", ct);
                await AssertEx.ThrowsDfsAsync<DfsException>(() => vol.Snapshots.GetAsync("s1", ct), DfsErrorCode.SnapshotNotFound);
                Assert.DoesNotContain(await vol.Snapshots.ListAsync(ct), s => s.Name == "s1");
                await AssertEx.ThrowsDfsAsync<DfsException>(() => vol.Snapshots.OpenAsync("s1", ct), DfsErrorCode.SnapshotNotFound);
                // The reader's pin survives the deletion of the retention reference (the pin may no longer resolve the
                // deleted name, so only its kind is checked; this test has exactly one snapshot reader).
                VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Volume);
                Assert.Contains(state.Pins, p => p.Kind == "SnapshotReader");

                // Well beyond the pin lifetime with the reader alive (renewing): objects stay; reads stay exact.
                await WatchRetainedAsync(cluster, v1Objects.ToArray(), Timings.PinDuration * 4, "an active reader pin protects the deleted snapshot", ct: ct);
                await AssertBytesAsync(v1, view.Files.ReadAllBytesAsync("/r/f", ct), "full read through the reader's pin after delete and GC");
                await AssertBytesAsync(side, view.Files.ReadAllBytesAsync("/r/d/side", ct), "other files of the pinned snapshot");
                var listing = new List<string>();
                await foreach (DfsEntryInfo entry in view.Directories.EnumerateAsync("/r", null, ct))
                    listing.Add(entry.Name);
                Assert.Equal(["d", "f"], listing.OrderBy(n => n, StringComparer.Ordinal).ToArray());

                if (release == ReaderRelease.ViewDisposed)
                {
                    await view.DisposeAsync();
                }
                else
                {
                    cluster.KillClient(readerClient);
                    // Until the pin expires the objects must remain.
                    await AssertRetainedAsync(cluster, v1Objects, "the killed reader's pin has not expired yet");
                }

                await WaitForReclaimAsync(cluster, v1Objects.ToArray(),
                    "after the reader pin is released (close) or expires (client death), the deleted snapshot's objects are reclaimed",
                    Timings.SessionTimeout + Timings.PinDuration + TimeSpan.FromSeconds(30), ct);
                VolumeStateInspection after = await cluster.Inspect.GetVolumeStateAsync(Volume);
                Assert.DoesNotContain(after.Pins, p => p.Kind == "SnapshotReader" || p.Snapshot == "s1");
            }
            finally
            {
                if (release == ReaderRelease.ViewDisposed)
                {
                    await view.DisposeAsync();
                    await readerClient.DisposeAsync();
                }
            }
        }, ct);
    }

    // ============================================================================================================
    // SNP-013 SnapshotDeletionReplyLost
    // ============================================================================================================

    public enum DeleteReplyLoss
    {
        SdkInternalRetry,
        UnknownOutcomeThenQuery,
        DelayedDuplicateAfterNameReuse,
    }

    /// <summary>
    /// SNP-013: a committed snapshot delete whose reply is lost is retried (SDK-internal, or resolved by operation id after
    /// an unknown outcome) without a second effect: the sibling snapshot sharing the objects keeps them, a recreated
    /// snapshot with the same name is never deleted by a delayed duplicate of the old request, and objects become
    /// reclaimable only once the last snapshot is deleted.
    /// </summary>
    [Theory]
    [InlineData(DeleteReplyLoss.SdkInternalRetry)]
    [InlineData(DeleteReplyLoss.UnknownOutcomeThenQuery)]
    [InlineData(DeleteReplyLoss.DelayedDuplicateAfterNameReuse)]
    [Catalog("SNP-013", "S", "INV-02", "INV-04", "INV-09", "INV-10", "INV-13", "INV-19")]
    public async Task SNP_013_SnapshotDeletionReplyLost(DeleteReplyLoss kind)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(Deterministic(), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0", OperationTimeout = TimeSpan.FromSeconds(3) }, ct);
            DfsVolume vol = client.GetVolume(Volume);
            byte[] v1 = TestData.Bytes(0x5013_0001, (2 * Chunk) + 7);
            await vol.Directories.CreateDirectoryAsync("/d", ct);
            await vol.Files.WriteAllBytesAsync("/d/f", v1, ct);
            IReadOnlyList<ObjectId> v1Objects = await ObjectsOfAsync(cluster, "/d/f");
            DfsSnapshotInfo s1 = await vol.Snapshots.CreateAsync("s1", null, ct);
            DfsSnapshotInfo s2 = await vol.Snapshots.CreateAsync("s2", null, ct);
            await vol.Files.WriteAllBytesAsync("/d/f", TestData.Bytes(0x5013_0002, 10), ct);

            switch (kind)
            {
                case DeleteReplyLoss.SdkInternalRetry:
                    {
                        MessageRule rule = cluster.Network.DropReplies(new MessageFilter(Type: MessageType.DeleteSnapshot), 1);
                        Exception? error = await CaptureAsync(() => vol.Snapshots.DeleteAsync("s1", ct));
                        Assert.Equal(1, rule.Matched);
                        if (error is not null)
                        {
                            // Legal alternative to an SDK-internal retry: the lost reply surfaces as an unknown outcome
                            // (never as SnapshotNotFound from re-executing the committed delete) and resolves to Committed.
                            DfsCommitOutcomeUnknownException unknown = Assert.IsAssignableFrom<DfsCommitOutcomeUnknownException>(error);
                            DfsOperationOutcome resolved = await vol.QueryOperationAsync(unknown.OperationId, true, ct);
                            Assert.Equal(DfsOutcomeStatus.Committed, resolved.Status);
                        }

                        break;
                    }

                case DeleteReplyLoss.UnknownOutcomeThenQuery:
                    {
                        MessageRule rule = cluster.Network.DropReplies(new MessageFilter(Type: MessageType.DeleteSnapshot), 10_000);

                        // The replies to the SDK's own resolving outcome queries are lost too (PROTOCOL.md §5.1).
                        MessageRule lostQueries = cluster.Network.DropReplies(new MessageFilter(From: "client:c0", Type: MessageType.QueryOperation), 10_000);
                        DfsCommitOutcomeUnknownException unknown = await AssertEx.ThrowsDfsAsync<DfsCommitOutcomeUnknownException>(
                            () => vol.Snapshots.DeleteAsync("s1", ct), DfsErrorCode.OutcomeUnknown);
                        rule.Cancel();
                        lostQueries.Cancel();
                        DfsOperationOutcome outcome = await vol.QueryOperationAsync(unknown.OperationId, true, ct);
                        Assert.Equal(DfsOutcomeStatus.Committed, outcome.Status);
                        // A new delete is a new operation on an absent name: it either reports absence or is a no-op
                        // (the API does not fix which); it must never delete anything else (checked below).
                        Exception? again = await CaptureAsync(() => vol.Snapshots.DeleteAsync("s1", ct));
                        if (again is not null)
                            Assert.Equal(DfsErrorCode.SnapshotNotFound, Assert.IsAssignableFrom<DfsException>(again).ErrorCode);
                        break;
                    }

                case DeleteReplyLoss.DelayedDuplicateAfterNameReuse:
                    {
                        CapturedMessage captured;
                        using (MessageHold hold = cluster.Network.Hold(new MessageFilter(Type: MessageType.DeleteSnapshot, Direction: LinkDirection.Request)))
                        {
                            Task delete = vol.Snapshots.DeleteAsync("s1", ct);
                            await hold.WaitForAsync(1, TimeSpan.FromSeconds(10));
                            captured = hold.Captured[0];
                            hold.ReleaseAll();
                            await delete;
                        }

                        DfsSnapshotInfo recreated = await vol.Snapshots.CreateAsync("s1", null, ct);
                        Assert.NotEqual(s1.Id, recreated.Id);
                        DfsErrorCode code = await cluster.Network.ReplayAsync(captured);
                        Assert.True(code is DfsErrorCode.None or DfsErrorCode.OperationExpired or DfsErrorCode.SnapshotNotFound,
                            $"a replayed old delete is answered from its stored outcome or rejected, got {code}");
                        DfsSnapshotInfo stillThere = await vol.Snapshots.GetAsync("s1", ct);
                        AssertSnapshotInfoEqual(recreated, stillThere, "a delayed duplicate of the old delete must not delete the recreated snapshot");
                        await vol.Snapshots.DeleteAsync("s1", ct);
                        break;
                    }

                default:
                    throw new ArgumentOutOfRangeException(nameof(kind), kind, null);
            }

            await AssertEx.ThrowsDfsAsync<DfsException>(() => vol.Snapshots.GetAsync("s1", ct), DfsErrorCode.SnapshotNotFound);
            AssertSnapshotInfoEqual(s2, await vol.Snapshots.GetAsync("s2", ct), "the sibling snapshot is untouched");
            Assert.Equal(["s2"], (await vol.Snapshots.ListAsync(ct)).Select(s => s.Name).ToArray());
            await WatchRetainedAsync(cluster, v1Objects.ToArray(), GcInterval * 12, "s2 still references v1 (no double decrement)", ct: ct);
            await AssertFileInSnapshotAsync(vol, "s2", "/d/f", v1, "s2 still reads v1");

            await vol.Snapshots.DeleteAsync("s2", ct);
            await WaitForReclaimAsync(cluster, v1Objects.ToArray(), "after the last snapshot is deleted, v1 is reclaimable", ct: ct);
        }, ct);
    }

    // ============================================================================================================
    // SNP-014 SnapshotManifestMissingDependency
    // ============================================================================================================

    public enum Damage
    {
        AllCopiesDeleted,
        AllCopiesCorrupted,
        AllHeaderCopiesCorrupted,
        OneCopyDeletedControl,
    }

    /// <summary>
    /// SNP-014: when every copy of a required object of a snapshot is lost or corrupted, the snapshot is not treated as
    /// fully recoverable: reads of the affected range fail with DfsIntegrityException (never zeroes or fabricated EOF),
    /// replica health reports the loss, backup closure verification refuses to publish it, and restore fails with an
    /// integrity error without creating a volume. With one surviving verified copy (control) everything succeeds.
    /// </summary>
    [Theory]
    [InlineData(Damage.AllCopiesDeleted)]
    [InlineData(Damage.AllCopiesCorrupted)]
    [InlineData(Damage.AllHeaderCopiesCorrupted)]
    [InlineData(Damage.OneCopyDeletedControl)]
    [Catalog("SNP-014", "S", "INV-02", "INV-04", "INV-09", "INV-10", "INV-13", "INV-19")]
    public async Task SNP_014_SnapshotManifestMissingDependency(Damage damage)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(Deterministic(backups: 1), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0", OperationTimeout = TimeSpan.FromSeconds(5) }, ct);
            DfsVolume vol = client.GetVolume(Volume);
            byte[] f1 = TestData.Bytes(0x5014_0001, (4 * Chunk) + 21);
            byte[] g = TestData.Bytes(0x5014_0002, 200);
            await vol.Files.WriteAllBytesAsync("/m/f", f1, ct);
            await vol.Files.WriteAllBytesAsync("/m/g", g, ct);
            FileInspection? file = await cluster.Inspect.GetFileAsync(Volume, "/m/f");
            Assert.NotNull(file);
            ExtentInspection victimExtent = file!.Extents.Count > 2 ? file.Extents[1] : file.Extents[0];
            ObjectId victim = victimExtent.Object;
            DfsSnapshotInfo s1 = await vol.Snapshots.CreateAsync("s1", null, ct);
            await vol.Files.WriteAllBytesAsync("/m/f", TestData.Bytes(0x5014_0003, 64), ct);   // live no longer references the victim

            await cluster.WaitForRepairAsync(Volume, TimeSpan.FromSeconds(30), ct);
            IReadOnlyList<string> holders = await cluster.Inspect.GetObjectHoldersAsync(victim);
            Assert.True(holders.Count >= AckCopies, $"victim must be durably replicated before damage ({holders.Count})");
            bool unrecoverable = damage != Damage.OneCopyDeletedControl;
            foreach (string holder in unrecoverable ? holders : holders.Take(1))
            {
                TestDisk disk = cluster.Node(holder).Disk;
                switch (damage)
                {
                    case Damage.AllCopiesDeleted:
                    case Damage.OneCopyDeletedControl:
                        await disk.DeleteObjectAsync(victim);
                        break;
                    case Damage.AllCopiesCorrupted:
                        await disk.CorruptObjectAsync(victim, 0);
                        break;
                    case Damage.AllHeaderCopiesCorrupted:
                        await disk.CorruptObjectHeaderAsync(victim);
                        break;
                }
            }

            // 1. Reads never fabricate data.
            await using (DfsSnapshotView view = await vol.Snapshots.OpenAsync("s1", ct))
            {
                await AssertBytesAsync(g, view.Files.ReadAllBytesAsync("/m/g", ct), "unaffected file of the snapshot");
                if (unrecoverable)
                {
                    await AssertEx.ThrowsDfsAsync<DfsIntegrityException>(() => view.Files.ReadAllBytesAsync("/m/f", ct), DfsErrorCode.Integrity);
                    await using DfsStream stream = await view.Files.OpenReadAsync("/m/f", ct);
                    stream.Position = victimExtent.Offset;
                    var buffer = new byte[64];
                    await AssertEx.ThrowsDfsAsync<DfsIntegrityException>(async () => await stream.ReadAtLeastAsync(buffer, 1, true, ct), DfsErrorCode.Integrity);
                    Assert.Equal(victimExtent.Offset, stream.Position);
                }
                else
                {
                    await AssertBytesAsync(f1, view.Files.ReadAllBytesAsync("/m/f", ct), "verified failover to a surviving copy");
                }
            }

            // 2. Health reports the loss honestly (or repair restores the control case).
            await cluster.Admin.StartScrubAsync(null, ct);
            if (unrecoverable)
            {
                await cluster.WaitUntilAsync(async () => (await cluster.Inspect.GetReplicaHealthAsync(Volume)).MissingObjects >= 1,
                    TimeSpan.FromSeconds(30), "replica health must report the snapshot's missing/corrupt object", ct);
            }
            else
            {
                await cluster.WaitForRepairAsync(Volume, TimeSpan.FromSeconds(60), ct);
                DfsReplicaHealth health = await cluster.Inspect.GetReplicaHealthAsync(Volume);
                Assert.Equal(0L, health.MissingObjects);
                Assert.Equal(0L, health.UnderReplicatedObjects);
            }

            // 3. Backup closure verification never publishes an incomplete snapshot.
            TestBackupNode backup = cluster.BackupNodes[0];
            await backup.RegisterAsync(Volume, new DfsBackupSchedule(OnEverySnapshot: false));
            await vol.Backups.RequestSyncAsync(backup.TargetName, "s1", ct);
            if (unrecoverable)
            {
                Exception? notPublished = await CaptureAsync(() => vol.Backups.AwaitBackupSnapshotAsync(backup.TargetName, "s1", TimeSpan.FromSeconds(15), ct));
                Assert.True(notPublished is DfsBackupNotReachedException or DfsIntegrityException
                        || (notPublished is DfsException { ErrorCode: DfsErrorCode.BackupNotReached or DfsErrorCode.Integrity }),
                    $"the barrier for an incomplete snapshot must fail (not reached / integrity), never succeed: {notPublished?.ToString() ?? "succeeded"}");
                Assert.DoesNotContain("s1", await backup.Inspect.GetPublishedSnapshotsAsync(Volume));
                DfsBackupStatus status = await vol.Backups.GetStatusAsync(backup.TargetName, ct);
                Assert.True(status.LatestPublished is null || status.LatestPublished.SnapshotName != "s1", "status must not certify the incomplete snapshot");
                Assert.False(string.IsNullOrEmpty(status.LastError), "the failed transfer must be reported explicitly");
            }
            else
            {
                DfsBackupCheckpoint checkpoint = await vol.Backups.AwaitBackupSnapshotAsync(backup.TargetName, "s1", TimeSpan.FromSeconds(60), ct);
                Assert.Equal(s1.Id, checkpoint.SnapshotId);
                Assert.True(checkpoint.IsExactSnapshot);
            }

            // 4. Restore fails explicitly instead of creating a volume with holes.
            if (unrecoverable)
            {
                await AssertEx.ThrowsDfsAsync<DfsIntegrityException>(() => vol.Snapshots.RestoreAsync("s1", "m-restored", ct), DfsErrorCode.Integrity);
                Assert.DoesNotContain("m-restored", await cluster.Admin.ListVolumesAsync(ct));
            }
            else
            {
                DfsVolumeInfo restored = await vol.Snapshots.RestoreAsync("s1", "m-restored", ct);
                Assert.NotEqual(s1.Boundary.VolumeId, restored.Id);
                await AssertBytesAsync(f1, client.GetVolume("m-restored").Files.ReadAllBytesAsync("/m/f", ct), "restored from verified copies");
            }

            // 5. The snapshot record itself is neither silently deleted nor relabelled.
            AssertSnapshotInfoEqual(s1, await vol.Snapshots.GetAsync("s1", ct), "the damaged snapshot record");
        }, ct);
    }

    // ============================================================================================================
    // SNP-015 ManySnapshotsBoundedMetadataWork
    // ============================================================================================================

    /// <summary>
    /// SNP-015: creating many snapshots costs metadata only: no object puts or payload bytes, per-snapshot journal growth,
    /// replication bytes, RPC bytes and log entries that do not depend on the volume's data size (a 4 MiB volume vs a
    /// 20 KiB volume), and listing returns every snapshot with strictly increasing boundaries.
    /// </summary>
    [Fact]
    [Catalog("SNP-015", "S", "INV-02", "INV-04", "INV-09", "INV-10", "INV-13", "INV-19")]
    public async Task SNP_015_ManySnapshotsBoundedMetadataWork()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        const int snapshotsPerVolume = 40;
        await ClusterTest.RunAsync(Deterministic(volumes: [new TestVolumeSpec("small"), new TestVolumeSpec("large")]), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(null, ct);
            DfsVolume small = client.GetVolume("small");
            DfsVolume large = client.GetVolume("large");
            await small.Directories.CreateDirectoryAsync("/s", ct);
            await large.Directories.CreateDirectoryAsync("/s", ct);
            for (int i = 0; i < 20; i++)
            {
                await small.Files.WriteAllBytesAsync($"/s/{i}", TestData.Bytes(0x5015_0000UL + (ulong)i, 1024), ct);
                await large.Files.WriteAllBytesAsync($"/s/{i}", TestData.Bytes(0x5015_1000UL + (ulong)i, 1024), ct);
            }

            long largePayload = 0;
            await large.Directories.CreateDirectoryAsync("/big", ct);
            for (int i = 0; i < 8; i++)
            {
                byte[] data = TestData.Bytes(0x5015_2000UL + (ulong)i, 8 * Chunk);
                await large.Files.WriteAllBytesAsync($"/big/{i}", data, ct);
                largePayload += data.Length;
            }

            await cluster.WaitForReplicationAsync("small", ct);
            await cluster.WaitForReplicationAsync("large", ct);
            await cluster.WaitForRepairAsync("small", TimeSpan.FromSeconds(60), ct);   // no background re-replication during measurement
            await cluster.WaitForRepairAsync("large", TimeSpan.FromSeconds(60), ct);
            SnapshotCost smallCost = await MeasureSnapshotsAsync(cluster, small, snapshotsPerVolume, ct);
            SnapshotCost largeCost = await MeasureSnapshotsAsync(cluster, large, snapshotsPerVolume, ct);

            ITestOutputHelper? output = TestContext.Current.TestOutputHelper;
            output?.WriteLine($"small: {smallCost}");
            output?.WriteLine($"large: {largeCost} (payload {largePayload} bytes)");

            foreach (SnapshotCost cost in new[] { smallCost, largeCost })
            {
                Assert.Equal(0, cost.ObjectPuts);
                Assert.Equal(0, cost.ObjectBytesReceived);
                Assert.True(cost.RaftEntriesPerSnapshot <= cluster.Nodes.Count * 4.0,
                    $"{cost.Volume}: {cost.RaftEntriesPerSnapshot:F1} log entries appended per snapshot (cluster-wide) exceeds a small constant");
            }

            Assert.True(largeCost.DiskBytesPerSnapshot <= (2 * Math.Max(0, smallCost.DiskBytesPerSnapshot)) + (16 * 1024),
                $"journal/disk growth per snapshot must not scale with data size (large {largeCost.DiskBytesPerSnapshot:F0} vs small {smallCost.DiskBytesPerSnapshot:F0})");
            Assert.True(largeCost.DiskBytesPerSnapshot < largePayload / 100.0,
                $"capture must not copy payload ({largeCost.DiskBytesPerSnapshot:F0} bytes per snapshot vs {largePayload} payload)");
            Assert.True(largeCost.RaftBytesPerSnapshot <= (3 * smallCost.RaftBytesPerSnapshot) + (8 * 1024),
                $"replication bytes per snapshot must not scale with data size (large {largeCost.RaftBytesPerSnapshot:F0} vs small {smallCost.RaftBytesPerSnapshot:F0})");
            Assert.True(largeCost.RpcBytesPerSnapshot <= (2 * smallCost.RpcBytesPerSnapshot) + 1024,
                $"create-snapshot RPC bytes must not scale with data size (large {largeCost.RpcBytesPerSnapshot:F0} vs small {smallCost.RpcBytesPerSnapshot:F0})");
        }, ct);
    }

    private sealed record SnapshotCost(string Volume, long ObjectPuts, long ObjectBytesReceived, double RaftEntriesPerSnapshot,
        double DiskBytesPerSnapshot, double RaftBytesPerSnapshot, double RpcBytesPerSnapshot, TimeSpan TimePerSnapshot);

    private static async Task<SnapshotCost> MeasureSnapshotsAsync(TestCluster cluster, DfsVolume vol, int count, CancellationToken ct)
    {
        CancellationToken token = ct;
        IReadOnlySet<ObjectId> objectsBefore = await cluster.Inspect.GetAllStoredObjectsAsync(vol.Name);
        NodeCounters before = await cluster.Inspect.GetTotalCountersAsync();
        long diskBefore = cluster.Nodes.Sum(n => n.Disk.UsedBytes);
        long raftBefore = cluster.Network.Log.CountBytes(new MessageFilter(Type: MessageType.RaftBatch));
        long rpcBefore = cluster.Network.Log.CountBytes(new MessageFilter(Operation: DfsOperationKind.CreateSnapshot));
        long start = cluster.Time.GetTimestamp();
        var infos = new List<DfsSnapshotInfo>();
        for (int i = 0; i < count; i++)
            infos.Add(await vol.Snapshots.CreateAsync($"m{i}", null, token));
        TimeSpan elapsed = cluster.Time.GetElapsedTime(start);
        await cluster.WaitForReplicationAsync(vol.Name, token);
        NodeCounters after = await cluster.Inspect.GetTotalCountersAsync();
        long diskAfter = cluster.Nodes.Sum(n => n.Disk.UsedBytes);
        long raftAfter = cluster.Network.Log.CountBytes(new MessageFilter(Type: MessageType.RaftBatch));
        long rpcAfter = cluster.Network.Log.CountBytes(new MessageFilter(Operation: DfsOperationKind.CreateSnapshot));

        IReadOnlySet<ObjectId> objectsAfter = await cluster.Inspect.GetAllStoredObjectsAsync(vol.Name);
        ObjectId[] created = objectsAfter.Except(objectsBefore).ToArray();
        Assert.True(created.Length == 0, $"{vol.Name}: snapshot capture created {created.Length} new data object(s); capture must only pin existing objects");

        IReadOnlyList<DfsSnapshotInfo> listed = await vol.Snapshots.ListAsync(token);
        Assert.Equal(count, listed.Count);
        for (int i = 1; i < infos.Count; i++)
            Assert.True(infos[i].Boundary.CompareTo(infos[i - 1].Boundary) > 0);

        return new SnapshotCost(vol.Name, after.ObjectPuts - before.ObjectPuts, after.ObjectBytesReceived - before.ObjectBytesReceived,
            (after.RaftEntriesAppended - before.RaftEntriesAppended) / (double)count, (diskAfter - diskBefore) / (double)count,
            (raftAfter - raftBefore) / (double)count, (rpcAfter - rpcBefore) / (double)count, elapsed / count);
    }

    // ============================================================================================================
    // SNP-016 SnapshotSurvivesColdRestart
    // ============================================================================================================

    public enum RestartKind
    {
        CrashLoseUnflushed,
        CrashTearLastWrite,
        CrashRandomSubset,
        GracefulStopAll,
        CrashAfterLogCompaction,
    }

    /// <summary>
    /// SNP-016: immediately after acknowledgement of filesystem-consistent, application-consistent (adapter records),
    /// held/expiring and exact commit-and-snapshot snapshots, every source node is crashed (or stopped) and restarted —
    /// also after the Raft log was compacted into a checkpoint. During recovery no wrong bytes are served; afterwards
    /// every snapshot's identity, boundary, label, adapters, retention, namespace, bytes, inode identities and revisions
    /// are unchanged on every replica and survive further GC.
    /// </summary>
    [Theory]
    [InlineData(RestartKind.CrashLoseUnflushed)]
    [InlineData(RestartKind.CrashTearLastWrite)]
    [InlineData(RestartKind.CrashRandomSubset)]
    [InlineData(RestartKind.GracefulStopAll)]
    [InlineData(RestartKind.CrashAfterLogCompaction)]
    [Catalog("SNP-016", "S", "INV-02", "INV-04", "INV-09", "INV-10", "INV-13", "INV-19")]
    public async Task SNP_016_SnapshotSurvivesColdRestart(RestartKind kind)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(Deterministic(), async cluster =>
        {
            SortedDictionary<string, byte[]?> fixture;
            Dictionary<string, SortedDictionary<string, byte[]?>> trees = new(StringComparer.Ordinal);
            Dictionary<string, Dictionary<string, DfsFileInfo>> infos = new(StringComparer.Ordinal);
            IReadOnlyList<DfsSnapshotInfo> expectedList;
            SortedDictionary<string, byte[]?> expectedLive;
            Dictionary<string, (ulong Boundary, DfsConsistencyLabel Label)> replicaView;
            await using (DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "before" }, ct))
            {
                DfsVolume vol = client.GetVolume(Volume);
                fixture = await CreateCowFixtureAsync(vol, ct);
                await vol.Snapshots.CreateAsync("s-fs", null, ct);
                var timeline = new AdapterTimeline(cluster.Time);
                var adapter = new RecordingSnapshotAdapter("app-db", timeline, "3.1.4",
                    new Dictionary<string, string>(StringComparer.Ordinal) { ["wal"] = "/cow/wal", ["scope"] = "/cow" });
                DfsSnapshotInfo app = await vol.Snapshots.CreateAsync("s-app", new DfsSnapshotOptions { Adapters = [adapter] }, ct);
                Assert.Equal(DfsConsistencyLabel.ApplicationConsistent, app.Label);
                await vol.Snapshots.CreateAsync("s-held", new DfsSnapshotOptions
                {
                    Retention = new DfsRetention(cluster.Time.GetUtcNow().AddDays(7), Hold: true),
                }, ct);
                CommitToken exactToken = await vol.Files.WriteAllBytesAsync("/cow/small", TestData.Bytes(0x5016_0001, 900),
                    new DfsMutationOptions { CommitSnapshotName = "s-exact" }, ct);
                Assert.Equal(exactToken, (await vol.Snapshots.GetAsync("s-exact", ct)).Boundary);

                // Live changes after the snapshots.
                await vol.Files.DeleteAsync("/cow/big", ct);
                await vol.Files.WriteAllBytesAsync("/cow/small", TestData.Bytes(0x5016_0002, 50), ct);
                await vol.Directories.DeleteAsync("/cow/dir", true, ct);
                if (kind == RestartKind.CrashAfterLogCompaction)
                {
                    await vol.Directories.CreateDirectoryAsync("/churn", ct);
                    for (int i = 0; i < Timings.RaftSnapshotThreshold + 50; i++)
                        await vol.Files.WriteAllBytesAsync($"/churn/{i % 5}", TestData.Bytes(0x5016_1000UL + (ulong)i, 40), ct);
                    await cluster.WaitUntilAsync(async () =>
                    {
                        GroupInspection group = await cluster.Inspect.GetVolumeGroupAsync(Volume);
                        return group.SnapshotIndex > 0;
                    }, TimeSpan.FromSeconds(30), "the volume log must be compacted into a checkpoint before the crash", ct);
                }

                expectedList = (await vol.Snapshots.ListAsync(ct)).OrderBy(s => s.Name, StringComparer.Ordinal).ToArray();
                Assert.Equal(["s-app", "s-exact", "s-fs", "s-held"], expectedList.Select(s => s.Name).ToArray());
                foreach (DfsSnapshotInfo s in expectedList)
                {
                    trees[s.Name] = await CaptureSnapshotAsync(vol, s.Name, "/cow", ct);
                    infos[s.Name] = await SnapshotFileInfosAsync(vol, s.Name, trees[s.Name], ct);
                }

                SnapshotTree.AssertEqual(fixture, trees["s-fs"], "s-fs captures the fixture");
                expectedLive = await SnapshotTree.CaptureAsync(vol, "/cow", ct);
                VolumeStateInspection leaderState = await cluster.Inspect.GetVolumeStateAsync(Volume);
                replicaView = leaderState.Snapshots.ToDictionary(s => s.Name, s => (s.BoundaryIndex, s.Label), StringComparer.Ordinal);
            }

            TestNode[] nodes = cluster.Nodes.ToArray();
            if (kind == RestartKind.GracefulStopAll)
            {
                foreach (TestNode node in nodes)
                    await node.StopAsync();
            }
            else
            {
                CrashMode mode = kind switch
                {
                    RestartKind.CrashTearLastWrite => CrashMode.TearLastWrite,
                    RestartKind.CrashRandomSubset => CrashMode.RandomSubset,
                    _ => CrashMode.LoseUnflushed,
                };
                await Task.WhenAll(nodes.Select(n => n.CrashAsync(mode)));
            }

            // Safety during recovery: with one node of three back there is no quorum; reads must fail or be exact.
            await nodes[0].RestartAsync();
            Exception? early = await CaptureAsync(async () =>
            {
                await using DfsClient partial = await cluster.ConnectAsync(new TestClientOptions
                {
                    Name = "early",
                    SeedNodes = [nodes[0].Name],
                    OperationTimeout = TimeSpan.FromSeconds(2),
                    ConnectTimeout = TimeSpan.FromSeconds(2),
                }, ct);
                await using DfsSnapshotView view = await partial.GetVolume(Volume).Snapshots.OpenAsync("s-fs", ct);
                byte[] bytes = await view.Files.ReadAllBytesAsync("/cow/big", ct);
                AssertEx.BytesEqual(trees["s-fs"]["/cow/big"], bytes, "a read served during recovery must be exact");
            });
            if (early is not null)
            {
                Assert.False(early is Xunit.Sdk.XunitException, $"wrong bytes served during recovery: {early.Message}");
                Assert.True(early is DfsException or TimeoutException or OperationCanceledException,
                    $"during recovery a read may be unavailable but must not fail in an unexpected way: {early}");
                Assert.False(early is DfsIntegrityException, $"recovery must not report intact snapshot data as corrupt: {early}");
            }

            foreach (TestNode node in nodes.Skip(1))
                await node.RestartAsync();
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), ct);

            await using DfsClient afterClient = await cluster.ConnectAsync(new TestClientOptions { Name = "after" }, ct);
            DfsVolume afterVol = afterClient.GetVolume(Volume);
            await AssertSnapshotsIntactAsync(afterVol, expectedList, trees, infos, ct);
            SnapshotTree.AssertEqual(expectedLive, await SnapshotTree.CaptureAsync(afterVol, "/cow", ct), "live state after restart");

            await cluster.WaitForReplicationAsync(Volume, ct);
            foreach (TestNode node in nodes)
            {
                VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Volume, node.Name);
                foreach ((string name, (ulong boundary, DfsConsistencyLabel label)) in replicaView)
                {
                    SnapshotInspection record = Assert.Single(state.Snapshots, s => s.Name == name);
                    Assert.Equal(boundary, record.BoundaryIndex);
                    Assert.Equal(label, record.Label);
                }
            }

            await RunGcPassesAsync(cluster, 10, ct);
            await AssertSnapshotsIntactAsync(afterVol, expectedList, trees, infos, ct);
        }, ct);
    }

    private static async Task AssertSnapshotsIntactAsync(DfsVolume vol, IReadOnlyList<DfsSnapshotInfo> expectedList,
        Dictionary<string, SortedDictionary<string, byte[]?>> trees, Dictionary<string, Dictionary<string, DfsFileInfo>> infos, CancellationToken ct)
    {
        IReadOnlyList<DfsSnapshotInfo> list = (await vol.Snapshots.ListAsync(ct)).OrderBy(s => s.Name, StringComparer.Ordinal).ToArray();
        Assert.Equal(expectedList.Select(s => s.Name), list.Select(s => s.Name));
        for (int i = 0; i < list.Count; i++)
        {
            AssertSnapshotInfoEqual(expectedList[i], list[i], $"snapshot {list[i].Name} metadata after cold restart");
            SnapshotTree.AssertEqual(trees[list[i].Name], await CaptureSnapshotAsync(vol, list[i].Name, "/cow", ct), $"snapshot {list[i].Name} contents after cold restart");
            await AssertSnapshotInfosUnchangedAsync(vol, list[i].Name, infos[list[i].Name], ct);
        }
    }

    // ============================================================================================================
    // SNP-017 SnapshotRestoreCreatesNewVolume
    // ============================================================================================================

    /// <summary>
    /// SNP-017: restoring a snapshot creates a new volume with a distinct id, its own epoch and incomparable tokens,
    /// containing exactly the snapshot's namespace and bytes in objects owned by the new volume; the source volume is not
    /// rolled back, the source snapshot is unchanged, the two evolve independently, existing names (including the live
    /// source) are rejected, and the restored volume survives deletion of the source snapshot and data.
    /// </summary>
    [Fact]
    [Catalog("SNP-017", "S", "INV-02", "INV-04", "INV-09", "INV-10", "INV-13", "INV-19")]
    public async Task SNP_017_SnapshotRestoreCreatesNewVolume()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(Deterministic(), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(null, ct);
            DfsVolume vol = client.GetVolume(Volume);
            byte[] a1 = TestData.Bytes(0x5017_0001, (2 * Chunk) + 5);
            await vol.Directories.CreateDirectoryAsync("/r/dir", ct);
            await vol.Files.WriteAllBytesAsync("/r/a", a1, ct);
            await vol.Files.WriteAllBytesAsync("/r/b", TestData.Bytes(0x5017_0002, 100), ct);
            await vol.Files.WriteAllBytesAsync("/r/dir/c", TestData.Bytes(0x5017_0003, Chunk + 1), ct);
            DfsSnapshotInfo s1 = await vol.Snapshots.CreateAsync("s1", null, ct);
            SortedDictionary<string, byte[]?> s1Tree = await CaptureSnapshotAsync(vol, "s1", "/", ct);

            CommitToken sourceToken = await vol.Files.WriteAllBytesAsync("/r/a", TestData.Bytes(0x5017_0004, 300), ct);
            await vol.Files.DeleteAsync("/r/b", ct);
            SortedDictionary<string, byte[]?> liveTree = await SnapshotTree.CaptureAsync(vol, "/", ct);
            DfsVolumeInfo source = await vol.GetInfoAsync(ct);

            DfsVolumeInfo restored = await vol.Snapshots.RestoreAsync("s1", "restored", ct);
            Assert.Equal("restored", restored.Name);
            Assert.NotEqual(source.Id, restored.Id);
            Assert.True(restored.Epoch >= 1, "the restored volume starts its own authority epoch");
            if (!restored.LastCommit.IsNone)
            {
                Assert.Equal(restored.Id, restored.LastCommit.VolumeId);
                Assert.False(restored.LastCommit.IsComparableWith(sourceToken));
            }

            DfsVolume rvol = client.GetVolume("restored");
            SnapshotTree.AssertEqual(s1Tree, await SnapshotTree.CaptureAsync(rvol, "/", ct), "the restored volume equals the snapshot");
            SnapshotTree.AssertEqual(liveTree, await SnapshotTree.CaptureAsync(vol, "/", ct), "the source volume is not rolled back");
            AssertSnapshotInfoEqual(s1, await vol.Snapshots.GetAsync("s1", ct), "the source snapshot is unchanged");
            SnapshotTree.AssertEqual(s1Tree, await CaptureSnapshotAsync(vol, "s1", "/", ct), "the source snapshot contents are unchanged");

            // Independent authority and history.
            byte[] r1 = TestData.Bytes(0x5017_0005, 777);
            CommitToken restoredToken = await rvol.Files.WriteAllBytesAsync("/r/a", r1, ct);
            Assert.Equal(restored.Id, restoredToken.VolumeId);
            Assert.False(restoredToken.IsComparableWith(sourceToken));
            Assert.Throws<ArgumentException>(() => restoredToken.CompareTo(sourceToken));
            await AssertBytesAsync(liveTree["/r/a"]!, vol.Files.ReadAllBytesAsync("/r/a", ct), "a write to the restored volume does not affect the source");
            SnapshotTree.AssertEqual(s1Tree, await CaptureSnapshotAsync(vol, "s1", "/", ct), "nor the source snapshot");
            await AssertEx.ThrowsDfsAsync<DfsCrossVolumeException>(
                () => rvol.Snapshots.CreateAsync("x", new DfsSnapshotOptions { MinimumCommit = sourceToken }, ct), DfsErrorCode.CrossVolume);
            Assert.Empty(await rvol.Snapshots.ListAsync(ct));

            // Objects are owned by the new volume (server-side copies, not shared references into the source).
            IReadOnlySet<ObjectId> restoredObjects = await cluster.Inspect.GetAllStoredObjectsAsync("restored");
            IReadOnlySet<ObjectId> sourceObjects = await cluster.Inspect.GetAllStoredObjectsAsync(Volume);
            Assert.NotEmpty(restoredObjects);
            Assert.Empty(restoredObjects.Intersect(sourceObjects));

            // Each restore is a distinct identity; existing names are rejected (no in-place rollback).
            DfsVolumeInfo restored2 = await vol.Snapshots.RestoreAsync("s1", "restored-2", ct);
            Assert.NotEqual(restored.Id, restored2.Id);
            Assert.NotEqual(source.Id, restored2.Id);
            await AssertEx.ThrowsDfsAsync<DfsAlreadyExistsException>(() => vol.Snapshots.RestoreAsync("s1", "restored", ct), DfsErrorCode.AlreadyExists);
            await AssertEx.ThrowsDfsAsync<DfsAlreadyExistsException>(() => vol.Snapshots.RestoreAsync("s1", Volume, ct), DfsErrorCode.AlreadyExists);
            SnapshotTree.AssertEqual(liveTree, await SnapshotTree.CaptureAsync(vol, "/", ct), "rejected restore over the live name changes nothing");

            // The restored volume is independent of the source snapshot and data.
            await vol.Snapshots.DeleteAsync("s1", ct);
            await vol.Files.DeleteAsync("/r/a", ct);
            await vol.Directories.DeleteAsync("/r/dir", true, ct);
            await RunGcPassesAsync(cluster, 10, ct);
            SortedDictionary<string, byte[]?> expectedRestored = SnapshotTree.Clone(s1Tree);
            expectedRestored["/r/a"] = r1;
            SnapshotTree.AssertEqual(expectedRestored, await SnapshotTree.CaptureAsync(rvol, "/", ct), "restored volume after the source snapshot and data are gone");
            SnapshotTree.AssertEqual(s1Tree, await SnapshotTree.CaptureAsync(client.GetVolume("restored-2"), "/", ct), "second restore");
        }, ct);
    }

    // ============================================================================================================
    // SNP-018 SnapshotScopeIsOneVolume
    // ============================================================================================================

    /// <summary>
    /// SNP-018: the core API offers no multi-volume snapshot; snapshots are created per volume, cover only their volume,
    /// carry incomparable boundaries (no global-atomicity claim), reject a minimum boundary from another volume, and
    /// commit-and-snapshot on one volume creates nothing on another.
    /// </summary>
    [Fact]
    [Catalog("SNP-018", "S", "INV-02", "INV-04", "INV-09", "INV-10", "INV-13", "INV-19")]
    public async Task SNP_018_SnapshotScopeIsOneVolume()
    {
        // API surface: no snapshot entry point accepts several volumes, and DfsClient has no cross-volume snapshot.
        foreach (MethodInfo method in typeof(DfsSnapshotClient).GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly))
        {
            foreach (ParameterInfo parameter in method.GetParameters())
            {
                // Implementations may add members (API.md); only a parameter that names or carries other volumes would
                // turn a snapshot entry point into a multi-volume one. RestoreAsync's single newVolumeName is the
                // destination of a restore, not a snapshot scope.
                string parameterName = parameter.Name ?? string.Empty;
                bool carriesVolumes = parameter.ParameterType == typeof(DfsVolume)
                    || typeof(IEnumerable<DfsVolume>).IsAssignableFrom(parameter.ParameterType)
                    || parameterName.Contains("volumes", StringComparison.OrdinalIgnoreCase)
                    || (parameter.ParameterType != typeof(string) && typeof(IEnumerable<string>).IsAssignableFrom(parameter.ParameterType)
                        && parameterName.Contains("volume", StringComparison.OrdinalIgnoreCase));
                Assert.False(carriesVolumes, $"{method.Name}({parameter.Name}) would allow a multi-volume snapshot in the core API");
            }
        }

        Assert.DoesNotContain(typeof(DfsClient).GetMethods(BindingFlags.Public | BindingFlags.Instance), m => m.Name.Contains("Snapshot", StringComparison.Ordinal));
        Assert.DoesNotContain(typeof(DfsSnapshotOptions).GetProperties(), p => p.Name.Contains("Volume", StringComparison.Ordinal));

        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(Deterministic(volumes: [new TestVolumeSpec("vol-a"), new TestVolumeSpec("vol-b")]), async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(null, ct);
            DfsVolume a = client.GetVolume("vol-a");
            DfsVolume b = client.GetVolume("vol-b");
            byte[] a1 = TestData.Bytes(0x5018_0001, 500);
            byte[] b1 = TestData.Bytes(0x5018_0002, 600);
            CommitToken ta = await a.Files.WriteAllBytesAsync("/x", a1, ct);
            CommitToken tb = await b.Files.WriteAllBytesAsync("/x", b1, ct);
            Assert.False(ta.IsComparableWith(tb));

            DfsSnapshotInfo sa = await a.Snapshots.CreateAsync("pair", null, ct);
            await a.Files.WriteAllBytesAsync("/x", TestData.Bytes(0x5018_0003, 10), ct);   // happens between the two captures
            DfsSnapshotInfo sb = await b.Snapshots.CreateAsync("pair", null, ct);
            Assert.Equal("vol-a", sa.Volume);
            Assert.Equal("vol-b", sb.Volume);
            Assert.NotEqual(sa.Id, sb.Id);
            Assert.False(sa.Boundary.IsComparableWith(sb.Boundary), "snapshots of different volumes have no common order");
            Assert.Throws<ArgumentException>(() => sa.Boundary.CompareTo(sb.Boundary));
            Assert.Equal(ta.VolumeId, sa.Boundary.VolumeId);
            Assert.Equal(tb.VolumeId, sb.Boundary.VolumeId);

            SortedDictionary<string, byte[]?> treeA = await CaptureSnapshotAsync(a, "pair", "/", ct);
            SortedDictionary<string, byte[]?> treeB = await CaptureSnapshotAsync(b, "pair", "/", ct);
            SnapshotTree.AssertEqual(new SortedDictionary<string, byte[]?>(StringComparer.Ordinal) { ["/x"] = a1 }, treeA, "vol-a snapshot covers vol-a only");
            SnapshotTree.AssertEqual(new SortedDictionary<string, byte[]?>(StringComparer.Ordinal) { ["/x"] = b1 }, treeB, "vol-b snapshot covers vol-b only");

            await AssertEx.ThrowsDfsAsync<DfsCrossVolumeException>(
                () => a.Snapshots.CreateAsync("cross", new DfsSnapshotOptions { MinimumCommit = tb }, ct), DfsErrorCode.CrossVolume);
            await AssertEx.ThrowsDfsAsync<DfsException>(() => a.Snapshots.GetAsync("cross", ct), DfsErrorCode.SnapshotNotFound);

            await a.Files.WriteAllBytesAsync("/x", a1, new DfsMutationOptions { CommitSnapshotName = "only-a" }, ct);
            await AssertEx.ThrowsDfsAsync<DfsException>(() => b.Snapshots.GetAsync("only-a", ct), DfsErrorCode.SnapshotNotFound);

            await a.Snapshots.DeleteAsync("pair", ct);
            AssertSnapshotInfoEqual(sb, await b.Snapshots.GetAsync("pair", ct), "deleting vol-a's snapshot does not touch vol-b's snapshot of the same name");
        }, ct);
    }

    private static async Task<byte[]> ReadToEndAsync(Stream stream, CancellationToken ct)
    {
        using var ms = new MemoryStream();
        await stream.CopyToAsync(ms, ct);
        return ms.ToArray();
    }
}
