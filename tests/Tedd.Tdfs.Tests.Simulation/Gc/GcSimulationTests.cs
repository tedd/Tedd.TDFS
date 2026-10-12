using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;
using static Tedd.Tdfs.Tests.Simulation.Gc.GcHelpers;

namespace Tedd.Tdfs.Tests.Simulation.Gc;

/// <summary>
/// GC — garbage collection, references and capacity reserves (test plan §7.23 and §9.7; FSD §§10.3, 11, 12.4, 14).
/// Deterministic whole-cluster simulations: collection runs concurrently (250 ms passes, virtual time) with the
/// workloads, and safety ("never collect a protected object") is checked during and after faults and recovery.
/// </summary>
public sealed class GcSimulationTests
{
    private const string Inv01 = "INV-01";
    private const string Inv09 = "INV-09";
    private const string Inv10 = "INV-10";
    private const string Inv16 = "INV-16";

    /// <summary>Historical reader kinds of GC-003.</summary>
    public enum HistoricalReader
    {
        /// <summary>A <see cref="DfsReadView.VersionPinned"/> stream handle.</summary>
        PinnedHandle,

        /// <summary>A lazy <c>ReadLinesAsync</c> enumeration (helper pins one revision for its lifetime).</summary>
        LazyReadLines,

        /// <summary>An open snapshot view whose snapshot is deleted meanwhile.</summary>
        SnapshotView,
    }

    /// <summary>Where unpublished uploads wait in GC-004.</summary>
    public enum StagingVariant
    {
        /// <summary>Complete chunks uploaded by a writable stream, not yet flushed.</summary>
        UploadedChunks,

        /// <summary>Commit request in flight (held in the network) while time passes.</summary>
        CommitInFlight,

        /// <summary>Data staged by a transaction-bound write before commit.</summary>
        TransactionStaged,
    }

    /// <summary>Publication path whose reply is lost in GC-005.</summary>
    public enum UnknownCommitKind
    {
        /// <summary>Standalone stream commit (CommitFile).</summary>
        StreamFlush,

        /// <summary>Explicit transaction commit (TxCommit).</summary>
        Transaction,
    }

    /// <summary>Underlying legal outcome explored by GC-005 (§9.7).</summary>
    public enum UnknownCommitBranch
    {
        /// <summary>The commit applied; only its reply was lost.</summary>
        Committed,

        /// <summary>The commit request never reached the leader; resolution fences it (authoritative abort).</summary>
        Aborted,
    }

    /// <summary>How a staging session is aborted in GC-007.</summary>
    public enum AbortPath
    {
        /// <summary><see cref="DfsStream.AbortAsync"/>.</summary>
        StreamAbort,

        /// <summary><see cref="DfsTransaction.AbortAsync"/>.</summary>
        TransactionAbort,

        /// <summary>Client process dies; its session expires.</summary>
        ClientCrash,
    }

    /// <summary>Snapshot/collection interleavings of GC-008.</summary>
    public enum SnapshotRace
    {
        /// <summary>Snapshot created while a sweep is blocked, then the live files change.</summary>
        SnapshotDuringSweep,

        /// <summary>Commit-and-snapshot while a sweep is blocked, then the live files change.</summary>
        CommitAndSnapshotDuringSweep,

        /// <summary>Live overwrite, then snapshot, then delete everything, all while a sweep is blocked.</summary>
        SnapshotThenOverwriteDuringSweep,
    }

    /// <summary>Which transition is blocked in GC-011.</summary>
    public enum RebalanceBlock
    {
        /// <summary>The first deletion of an old replica on the draining node.</summary>
        OldReplicaDelete,

        /// <summary>Replica copying to the new targets.</summary>
        Replication,
    }

    /// <summary>Offline reader kinds of GC-020.</summary>
    public enum DisconnectedReader
    {
        /// <summary>Version-pinned stream handle.</summary>
        PinnedHandle,

        /// <summary>Open snapshot view.</summary>
        SnapshotView,
    }

    // ------------------------------------------------------------------------------------------------------------
    // GC-001
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Single writer per file (whole-file writes, partial overwrites, appends, copies) and concurrent readers while GC
    /// passes run every 250 ms. Every read returns a linearizable whole revision, every object of the current roots
    /// keeps its acknowledged copies during the run, and superseded objects really are collected meanwhile.
    /// </summary>
    [Fact]
    [Catalog("GC-001", "S", Inv01, Inv09, Inv10, Inv16)]
    public async Task GC_001_CurrentRootObjectsNeverCollected()
    {
        await ClusterTest.RunAsync(Det(3), async cluster =>
        {
            int chunk = Chunk(cluster);
            await using DfsClient writerClient = await cluster.ConnectAsync(new TestClientOptions { Name = "w" }, Ct);
            await using DfsClient readerClient = await cluster.ConnectAsync(new TestClientOptions { Name = "r" }, Ct);
            DfsVolume w = writerClient.GetVolume(Volume);
            DfsVolume r = readerClient.GetVolume(Volume);
            string[] files = ["/f0", "/f1", "/f2", "/f3"];
            var model = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            for (int i = 0; i < files.Length; i++)
            {
                byte[] initial = TestData.Bytes(100 + (ulong)i, (2 * chunk) + 17);
                model[files[i]] = initial;
                await cluster.History.RecordAsync($"w{i}", "write", files[i], initial, () => w.Files.WriteAllBytesAsync(files[i], initial, Ct));
            }

            long deletedBefore = await GcDeletedAsync(cluster);
            int sweepsBefore = SweepHits(cluster);
            bool stop = false;

            async Task WriterAsync(int index)
            {
                string f = files[index];
                var rnd = new SeededRandom(1000 + (ulong)index);
                for (int round = 0; round < 10; round++)
                {
                    ulong seed = 2000 + ((ulong)index * 100) + (ulong)round;
                    byte[] next;
                    Func<Task<CommitToken>> mutate;
                    switch (rnd.Next(4))
                    {
                        case 0:
                        {
                            byte[] whole = TestData.Bytes(seed, (chunk * (1 + rnd.Next(3))) + rnd.Next(100));
                            next = whole;
                            mutate = () => w.Files.WriteAllBytesAsync(f, whole, Ct);
                            break;
                        }

                        case 1:
                        {
                            long offset = rnd.Next(model[f].Length);
                            byte[] patch = TestData.Bytes(seed, (chunk / 2) + 3);
                            next = Overlay(model[f], offset, patch);
                            mutate = () => OverwriteRangeAsync(w, f, offset, patch);
                            break;
                        }

                        case 2:
                        {
                            byte[] suffix = TestData.Bytes(seed, chunk / 3);
                            next = [.. model[f], .. suffix];
                            mutate = () => w.Files.AppendAllBytesAsync(f, suffix, Ct);
                            break;
                        }

                        default:
                        {
                            string source = f + ".src";
                            byte[] src = TestData.Bytes(seed, chunk + 5);
                            await w.Files.WriteAllBytesAsync(source, src, Ct);
                            next = src;
                            mutate = () => w.Files.CopyAsync(source, f, true, Ct);
                            break;
                        }
                    }

                    await cluster.History.RecordAsync($"w{index}", "write", f, next, mutate);
                    model[f] = next;
                    await cluster.DelayAsync(cluster.Options.Timings.GcInterval, Ct);
                }
            }

            async Task ReaderAsync(int id)
            {
                var rnd = new SeededRandom(3000 + (ulong)id);
                while (!stop)
                {
                    string f = files[rnd.Next(files.Length)];
                    await cluster.History.RecordAsync($"r{id}", "read", f, null, () => r.Files.ReadAllBytesAsync(f, Ct));
                    await cluster.DelayAsync(TimeSpan.FromMilliseconds(40), Ct);
                }
            }

            async Task AuditorAsync()
            {
                while (!stop)
                {
                    await AssertReachableHeldAsync(cluster, "during concurrent collection");
                    await cluster.DelayAsync(cluster.Options.Timings.GcInterval, Ct);
                }
            }

            Task observers = Task.WhenAll(ReaderAsync(0), ReaderAsync(1), AuditorAsync());
            await Task.WhenAll(Enumerable.Range(0, files.Length).Select(WriterAsync));
            stop = true;
            await observers;

            LinearizabilityChecker.Check(cluster.History.Operations, new RegisterModel()).AssertValid();
            Assert.True(SweepHits(cluster) > sweepsBefore, "no GC sweep ran during the workload (concurrency not exercised).");
            Assert.True(await GcDeletedAsync(cluster) > deletedBefore, "superseded objects were never collected while the workload ran.");

            await QuiesceGcAsync(cluster);
            foreach (string f in files)
            {
                AssertBytes(model[f], await r.Files.ReadAllBytesAsync(f, Ct), $"{f} after quiescence");
                await AssertHeldAsync(cluster, await FileObjectsAsync(cluster, f), AckCopies(cluster), $"current root of {f}");
            }

            await AssertReachableHeldAsync(cluster, "after quiescence");
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // GC-002
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Ten snapshots, a copy and live versions share extents; deleting snapshots one at a time (seeded order) never
    /// reclaims an object another root still reaches, and the last deletion lets every unshared object go.
    /// </summary>
    [Fact]
    [Catalog("GC-002", "S", Inv01, Inv09, Inv10, Inv16)]
    public async Task GC_002_SharedSnapshotReferencesSafe()
    {
        await ClusterTest.RunAsync(Det(3), async cluster =>
        {
            int chunk = Chunk(cluster);
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            DfsVolume v = client.GetVolume(Volume);
            var live = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            var snapshots = new Dictionary<string, Dictionary<string, byte[]>>(StringComparer.Ordinal);

            async Task Snapshot(string name)
            {
                await v.Snapshots.CreateAsync(name, null, Ct);
                snapshots[name] = new Dictionary<string, byte[]>(live, StringComparer.Ordinal);
            }

            live["/f"] = TestData.Bytes(2001, 4 * chunk);
            await v.Files.WriteAllBytesAsync("/f", live["/f"], Ct);
            await Snapshot("s1");
            byte[] patch = TestData.Bytes(2002, chunk);
            await OverwriteRangeAsync(v, "/f", 2 * chunk, patch);
            live["/f"] = Overlay(live["/f"], 2 * chunk, patch);
            await Snapshot("s2");
            await v.Files.CopyAsync("/f", "/g", false, Ct);
            live["/g"] = live["/f"];
            await Snapshot("s3");
            live["/f"] = TestData.Bytes(2003, 3 * chunk);
            await v.Files.WriteAllBytesAsync("/f", live["/f"], Ct);
            byte[] suffix = TestData.Bytes(2004, chunk / 2);
            await v.Files.AppendAllBytesAsync("/g", suffix, Ct);
            live["/g"] = [.. live["/g"], .. suffix];
            for (int i = 4; i <= 10; i++)
            {
                byte[] p = TestData.Bytes(2010 + (ulong)i, chunk / 4);
                long offset = (i % 4) * chunk;
                await OverwriteRangeAsync(v, "/g", offset, p);
                live["/g"] = Overlay(live["/g"], offset, p);
                await Snapshot($"s{i}");
            }

            await v.Files.DeleteAsync("/f", Ct);
            live.Remove("/f");

            var order = snapshots.Keys.Order(StringComparer.Ordinal).ToList();
            var rnd = new SeededRandom(2099);
            for (int i = order.Count - 1; i > 0; i--)
            {
                int j = rnd.Next(i + 1);
                (order[i], order[j]) = (order[j], order[i]);
            }

            foreach (string name in order)
            {
                await v.Snapshots.DeleteAsync(name, Ct);
                snapshots.Remove(name);
                await QuiesceGcAsync(cluster, 6);
                foreach ((string snapshot, Dictionary<string, byte[]> content) in snapshots)
                {
                    foreach ((string path, byte[] bytes) in content)
                        AssertBytes(bytes, await ReadSnapshotFileAsync(v, snapshot, path), $"{snapshot}:{path} after deleting {name}");
                }

                foreach ((string path, byte[] bytes) in live)
                    AssertBytes(bytes, await v.Files.ReadAllBytesAsync(path, Ct), $"live {path} after deleting {name}");
                await AssertReachableHeldAsync(cluster, $"after deleting snapshot {name}");
            }

            await WaitNoGarbageAsync(cluster, "objects referenced only by deleted snapshots are reclaimed");
            AssertBytes(live["/g"], await v.Files.ReadAllBytesAsync("/g", Ct), "/g at the end");
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // GC-003
    // ------------------------------------------------------------------------------------------------------------

    [Theory]
    [InlineData(HistoricalReader.PinnedHandle)]
    [InlineData(HistoricalReader.LazyReadLines)]
    [InlineData(HistoricalReader.SnapshotView)]
    [Catalog("GC-003", "S", Inv01, Inv09, Inv10, Inv16)]
    public async Task GC_003_ReaderPinProtectsHistoricalData(HistoricalReader reader)
    {
        await ClusterTest.RunAsync(Det(3), async cluster =>
        {
            int chunk = Chunk(cluster);
            await using DfsClient writerClient = await cluster.ConnectAsync(new TestClientOptions { Name = "w" }, Ct);
            await using DfsClient readerClient = await cluster.ConnectAsync(new TestClientOptions { Name = "r" }, Ct);
            DfsVolume w = writerClient.GetVolume(Volume);
            DfsVolume r = readerClient.GetVolume(Volume);

            async Task ReplaceDeleteAndCollectAsync(string path, Func<Task>? extra = null)
            {
                await w.Files.WriteAllBytesAsync(path, TestData.Bytes(3002, 3 * chunk), Ct);
                await w.Files.DeleteAsync(path, Ct);
                if (extra is not null)
                    await extra();
                await QuiesceGcAsync(cluster, 30); // far beyond PinDuration: the live reader keeps renewing its pin
                JumpWallClocks(cluster, TimeSpan.FromDays(1));
                await QuiesceGcAsync(cluster, 10);
            }

            switch (reader)
            {
                case HistoricalReader.PinnedHandle:
                {
                    byte[] v1 = TestData.Bytes(3001, (4 * chunk) + 5);
                    await w.Files.WriteAllBytesAsync("/f", v1, Ct);
                    HashSet<ObjectId> v1Objects = await FileObjectsAsync(cluster, "/f");
                    DfsStream stream = await r.Files.OpenAsync("/f", PinnedReader, Ct);
                    AssertBytes(v1[..1024], await ReadRangeAsync(stream, 0, 1024), "first read through the pinned handle");
                    await ReplaceDeleteAndCollectAsync("/f");
                    await AssertHeldAsync(cluster, v1Objects, AckCopies(cluster), "historical revision pinned by an open reader");
                    AssertBytes(v1[(2 * chunk)..], await ReadRangeAsync(stream, 2 * chunk, v1.Length - (2 * chunk)), "pinned handle reads the old revision after replace+delete+GC");
                    AssertBytes(v1, await ReadRangeAsync(stream, 0, v1.Length), "whole old revision");
                    await stream.DisposeAsync();
                    await WaitReclaimedAsync(cluster, v1Objects, "the historical revision is reclaimed once the reader closes");
                    break;
                }

                case HistoricalReader.LazyReadLines:
                {
                    string[] lines = [.. Enumerable.Range(0, 6000).Select(i => $"line {i:D6} {TestData.Utf8Text((ulong)i, 20)}")];
                    await w.Files.WriteAllLinesAsync("/t", lines, null, Ct);
                    HashSet<ObjectId> v1Objects = await FileObjectsAsync(cluster, "/t");
                    IAsyncEnumerator<string> lazy = r.Files.ReadLinesAsync("/t", null, Ct).GetAsyncEnumerator(Ct);
                    Assert.True(await lazy.MoveNextAsync());
                    Assert.Equal(lines[0], lazy.Current);
                    await ReplaceDeleteAndCollectAsync("/t");
                    await AssertHeldAsync(cluster, v1Objects, AckCopies(cluster), "revision pinned by a lazy line enumeration");
                    int n = 1;
                    while (await lazy.MoveNextAsync())
                    {
                        Assert.Equal(lines[n], lazy.Current);
                        n++;
                    }

                    Assert.Equal(lines.Length, n);
                    await lazy.DisposeAsync();
                    await WaitReclaimedAsync(cluster, v1Objects, "the enumerated revision is reclaimed after the enumeration ends");
                    break;
                }

                default:
                {
                    byte[] v1 = TestData.Bytes(3011, 3 * chunk);
                    await w.Files.WriteAllBytesAsync("/f", v1, Ct);
                    await w.Snapshots.CreateAsync("s1", null, Ct);
                    HashSet<ObjectId> v1Objects = await FileObjectsAsync(cluster, "/f");
                    DfsSnapshotView view = await r.Snapshots.OpenAsync("s1", Ct);
                    await ReplaceDeleteAndCollectAsync("/f", () => w.Snapshots.DeleteAsync("s1", Ct));
                    await AssertHeldAsync(cluster, v1Objects, AckCopies(cluster), "snapshot data pinned by an open view after the snapshot was deleted");
                    AssertBytes(v1, await view.Files.ReadAllBytesAsync("/f", Ct), "open snapshot view after delete+GC");
                    await view.DisposeAsync();
                    await WaitReclaimedAsync(cluster, v1Objects, "the deleted snapshot's data is reclaimed after the view closes");
                    break;
                }
            }
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // GC-004
    // ------------------------------------------------------------------------------------------------------------

    private static async Task AgeAsync(TestCluster cluster, TimeSpan duration)
    {
        JumpWallClocks(cluster, TimeSpan.FromDays(30));
        TimeSpan step = TimeSpan.FromSeconds(5);
        for (TimeSpan elapsed = TimeSpan.Zero; elapsed < duration; elapsed += step)
            await cluster.DelayAsync(step, Ct);
    }

    [Theory]
    [InlineData(StagingVariant.UploadedChunks)]
    [InlineData(StagingVariant.CommitInFlight)]
    [InlineData(StagingVariant.TransactionStaged)]
    [Catalog("GC-004", "S", Inv01, Inv09, Inv10, Inv16)]
    public async Task GC_004_StagingSessionProtectsUploads(StagingVariant variant)
    {
        await ClusterTest.RunAsync(Det(3), async cluster =>
        {
            int chunk = Chunk(cluster);
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            DfsVolume v = client.GetVolume(Volume);
            byte[] old = TestData.Bytes(4001, 2 * chunk);
            await v.Files.WriteAllBytesAsync("/f", old, Ct);
            HashSet<ObjectId> before = await StoredAsync(cluster);
            byte[] data = TestData.Bytes(4002, (5 * chunk) + 99);
            int sweeps = SweepHits(cluster);

            async Task<HashSet<ObjectId>> StagedAsync()
            {
                HashSet<ObjectId> staged = await StoredAsync(cluster);
                staged.ExceptWith(before);
                return staged;
            }

            switch (variant)
            {
                case StagingVariant.UploadedChunks:
                {
                    await using DfsStream stream = await v.Files.OpenAsync("/f", Writer, Ct);
                    await stream.WriteAsync(data, Ct);
                    await WaitAsync(cluster, async () => (await StagedAsync()).Count >= 3, TimeSpan.FromSeconds(10), "complete chunks are uploaded before publication");
                    Assert.True(stream.PendingBytes > 0);
                    HashSet<ObjectId> staged = await StagedAsync();
                    await AgeAsync(cluster, TimeSpan.FromSeconds(60));
                    Assert.True(SweepHits(cluster) > sweeps, "no GC sweep ran while the uploads were staged");
                    await AssertHeldAsync(cluster, staged, AckCopies(cluster), "uploads of a live staging session after 60 s and a 30-day wall-clock jump");
                    AssertBytes(old, await v.Files.ReadAllBytesAsync("/f", Ct), "staged bytes are not visible before publication");
                    CommitToken token = await stream.CommitAsync(Ct);
                    Assert.False(token.IsNone);
                    AssertBytes(data, await v.Files.ReadAllBytesAsync("/f", Ct), "published revision");
                    HashSet<ObjectId> published = await FileObjectsAsync(cluster, "/f");
                    Assert.True(staged.IsSubsetOf(published), "the staged uploads were not the ones published (lost and re-uploaded?)");
                    break;
                }

                case StagingVariant.CommitInFlight:
                {
                    DfsStream stream = await v.Files.OpenAsync("/f", Writer, Ct);
                    await stream.WriteAsync(data, Ct);
                    Task<CommitToken> commit;
                    HashSet<ObjectId> staged;
                    using (MessageHold hold = cluster.Network.Hold(new MessageFilter(From: "client:c0", Type: MessageType.CommitFile)))
                    {
                        commit = stream.CommitAsync(Ct);
                        await hold.WaitForAsync(1, TimeSpan.FromSeconds(10));
                        staged = await StagedAsync();
                        Assert.NotEmpty(staged);
                        await AgeAsync(cluster, TimeSpan.FromSeconds(4)); // below the client's operation timeout
                        Assert.True(SweepHits(cluster) > sweeps, "no GC sweep ran while the commit was in flight");
                        await AssertHeldAsync(cluster, staged, AckCopies(cluster), "uploads referenced by an in-flight commit");
                        hold.ReleaseAll();
                    }

                    CommitToken token = await commit;
                    Assert.False(token.IsNone);
                    await stream.DisposeAsync();
                    AssertBytes(data, await v.Files.ReadAllBytesAsync("/f", Ct), "revision published after the delayed commit");
                    await AssertHeldAsync(cluster, await FileObjectsAsync(cluster, "/f"), AckCopies(cluster), "published revision");
                    break;
                }

                default:
                {
                    await using DfsTransaction tx = await v.Transactions.BeginAsync(null, Ct);
                    await tx.Files.WriteAllBytesAsync("/f", data, Ct);
                    HashSet<ObjectId> staged = await StagedAsync();
                    Assert.NotEmpty(staged);
                    await AgeAsync(cluster, TimeSpan.FromSeconds(60));
                    Assert.True(SweepHits(cluster) > sweeps, "no GC sweep ran while the transaction was staged");
                    await AssertHeldAsync(cluster, staged, AckCopies(cluster), "uploads staged by an active transaction");
                    AssertBytes(old, await v.Files.ReadAllBytesAsync("/f", Ct), "uncommitted transaction is invisible");
                    CommitToken token = await tx.CommitAsync(Ct);
                    Assert.False(token.IsNone);
                    AssertBytes(data, await v.Files.ReadAllBytesAsync("/f", Ct), "transaction result");
                    break;
                }
            }

            await QuiesceGcAsync(cluster);
            AssertBytes(data, await v.Files.ReadAllBytesAsync("/f", Ct), "published revision after further collection");
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // GC-005 (test plan §9.7)
    // ------------------------------------------------------------------------------------------------------------

    [Theory]
    [InlineData(UnknownCommitKind.StreamFlush, UnknownCommitBranch.Committed)]
    [InlineData(UnknownCommitKind.StreamFlush, UnknownCommitBranch.Aborted)]
    [InlineData(UnknownCommitKind.Transaction, UnknownCommitBranch.Committed)]
    [InlineData(UnknownCommitKind.Transaction, UnknownCommitBranch.Aborted)]
    [Catalog("GC-005", "S", Inv01, Inv09, Inv10, Inv16)]
    public async Task GC_005_UnknownTransactionIsNotAbortedByAge(UnknownCommitKind kind, UnknownCommitBranch branch)
    {
        await ClusterTest.RunAsync(Det(3), async cluster =>
        {
            int chunk = Chunk(cluster);
            await using DfsClient c0 = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            await using DfsClient observer = await cluster.ConnectAsync(new TestClientOptions { Name = "obs" }, Ct);
            DfsVolume v = c0.GetVolume(Volume);
            DfsVolume ov = observer.GetVolume(Volume);
            byte[] old = TestData.Bytes(5001, 3 * chunk);
            await v.Files.WriteAllBytesAsync("/f", old, Ct);
            HashSet<ObjectId> before = await StoredAsync(cluster);
            byte[] next = TestData.Bytes(5002, (5 * chunk) + 7);

            DfsStream? stream = null;
            DfsTransaction? tx = null;
            if (kind == UnknownCommitKind.StreamFlush)
            {
                stream = await v.Files.OpenAsync("/f", Writer, Ct);
                await stream.WriteAsync(next, Ct);
            }
            else
            {
                tx = await v.Transactions.BeginAsync(null, Ct);
                await tx.Files.WriteAllBytesAsync("/f", next, Ct);
            }

            var commitFilter = new MessageFilter(From: "client:c0", Type: kind == UnknownCommitKind.StreamFlush ? MessageType.CommitFile : MessageType.TxCommit);
            MessageRule blockQueries = cluster.Network.DropNext(new MessageFilter(From: "client:c0", Type: MessageType.QueryOperation), int.MaxValue);
            MessageRule? lostReplies = branch == UnknownCommitBranch.Committed ? cluster.Network.DropReplies(commitFilter, int.MaxValue) : null;
            MessageHold? held = branch == UnknownCommitBranch.Aborted ? cluster.Network.Hold(commitFilter) : null;

            DfsCommitOutcomeUnknownException unknown = await AssertEx.ThrowsDfsAsync<DfsCommitOutcomeUnknownException>(
                () => stream is not null ? stream.CommitAsync(Ct) : tx!.CommitAsync(Ct), DfsErrorCode.OutcomeUnknown);
            Assert.False(unknown.OperationId.IsEmpty, "an unknown outcome must carry its operation id");
            DfsTransactionId? txId = null;
            if (tx is not null)
            {
                Assert.Equal(tx.Id, unknown.TransactionId);
                Assert.Equal(DfsTransactionState.Unknown, tx.State);
                txId = tx.Id;
            }

            if (lostReplies is not null)
                Assert.True(lostReplies.Matched > 0, "the commit reply was never dropped (fault point not reached)");
            if (held is not null)
                Assert.True(held.Count > 0, "the commit request was never held (fault point not reached)");

            HashSet<ObjectId> possiblyCommitted = await StoredAsync(cluster);
            possiblyCommitted.ExceptWith(before);
            Assert.NotEmpty(possiblyCommitted);

            async Task<DfsOutcomeStatus> PeekAsync() => txId is { } id
                ? (await ov.Transactions.QueryOutcomeAsync(id, resolve: false, Ct)).Status
                : (await ov.QueryOperationAsync(unknown.OperationId, resolve: false, Ct)).Status;

            // Phase 1 — time alone: wall clocks jump, staging ages, GC sweeps and compaction run repeatedly (§9.7).
            int sweeps = SweepHits(cluster);
            for (int round = 0; round < 4; round++)
            {
                JumpWallClocks(cluster, TimeSpan.FromDays(7));
                await cluster.DelayAsync(TimeSpan.FromSeconds(15), Ct);
                foreach (TestNode node in cluster.Nodes.Where(n => n.State == TestNodeState.Running))
                    await node.CompactStorageAsync();
                DfsOutcomeStatus status = await PeekAsync();
                if (branch == UnknownCommitBranch.Committed)
                    Assert.Equal(DfsOutcomeStatus.Committed, status);
                else
                    Assert.True(status is DfsOutcomeStatus.Pending or DfsOutcomeStatus.NotFound,
                        $"round {round}: an undecided commit was reported as {status} without any authoritative decision (a pending query is not an abort).");
                await AssertHeldAsync(cluster, possiblyCommitted, AckCopies(cluster), $"round {round}: possibly committed objects after elapsed time only");
            }

            Assert.True(SweepHits(cluster) > sweeps, "GC never swept while the outcome was unknown");

            // Phase 2 — the coordinator crashes and restarts inside the unknown window.
            TestNode leader = await cluster.GetLeaderAsync(Volume, Ct);
            await leader.CrashAsync(CrashMode.LoseUnflushed);
            await AssertHeldAsync(cluster, possiblyCommitted, 1, "possibly committed objects while the coordinator is down");
            await leader.RestartAsync();
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(30), Ct);
            await QuiesceGcAsync(cluster);
            foreach (TestNode node in cluster.Nodes)
                await node.CompactStorageAsync();
            DfsOutcomeStatus afterRestart = await PeekAsync();
            if (branch == UnknownCommitBranch.Committed)
                Assert.Equal(DfsOutcomeStatus.Committed, afterRestart);
            if (afterRestart != DfsOutcomeStatus.Aborted)
                await AssertHeldAsync(cluster, possiblyCommitted, AckCopies(cluster), "possibly committed objects after a coordinator crash/restart");

            // Heal and resolve before the final orphan cleanup.
            blockQueries.Cancel();
            lostReplies?.Cancel();
            DfsOperationOutcome resolved = txId is { } t
                ? await v.Transactions.QueryOutcomeAsync(t, resolve: true, Ct)
                : await v.QueryOperationAsync(unknown.OperationId, resolve: true, Ct);

            if (branch == UnknownCommitBranch.Committed)
            {
                Assert.Equal(DfsOutcomeStatus.Committed, resolved.Status);
                Assert.False(resolved.Token.IsNone);
                AssertBytes(next, await ov.Files.ReadAllBytesAsync("/f", Ct), "the committed revision is recoverable");
                await QuiesceGcAsync(cluster);
                await AssertHeldAsync(cluster, await FileObjectsAsync(cluster, "/f"), AckCopies(cluster), "committed revision after resolution");
                AssertBytes(next, await ov.Files.ReadAllBytesAsync("/f", Ct), "committed revision after further collection");
            }
            else
            {
                Assert.Equal(DfsOutcomeStatus.Aborted, resolved.Status);
                held!.ReleaseAll(); // the delayed commit arrives after the definite abort
                await QuiesceGcAsync(cluster, 4);
                AssertBytes(old, await ov.Files.ReadAllBytesAsync("/f", Ct), "a fenced commit never lands");
            }

            held?.Dispose();
            try
            {
                if (stream is not null)
                    await stream.DisposeAsync();
                if (tx is not null)
                    await tx.DisposeAsync();
            }
            catch (Exception ex) when (ex is IOException or InvalidOperationException)
            {
                // The faulted stream/transaction surfaces its unknown outcome again; resources are still released.
            }

            if (branch == UnknownCommitBranch.Aborted)
            {
                HashSet<ObjectId> orphans = [.. possiblyCommitted.Except(await ReachableAsync(cluster))];
                await WaitReclaimedAsync(cluster, orphans, "safe orphans are reclaimed once the outcome is authoritatively aborted");
                AssertBytes(old, await ov.Files.ReadAllBytesAsync("/f", Ct), "old revision after orphan cleanup");
            }
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // GC-006
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<string, CrashMode> PromotionCrashData()
    {
        var data = new TheoryData<string, CrashMode>();
        foreach (string site in new[] { FaultSites.CommitReceiptValidated, FaultSites.RaftAfterLocalAppend, FaultSites.RaftAfterCommitBeforeApply, FaultSites.ApplyAfterBeforeReply })
        {
            foreach (CrashMode mode in new[] { CrashMode.LoseUnflushed, CrashMode.KeepUnflushed, CrashMode.TearLastWrite })
                data.Add(site, mode);
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(PromotionCrashData))]
    [Catalog("GC-006", "S", Inv01, Inv09, Inv10, Inv16)]
    public async Task GC_006_StagingToLiveTransitionAtomic(string site, CrashMode mode)
    {
        await ClusterTest.RunAsync(Det(3), async cluster =>
        {
            int chunk = Chunk(cluster);
            await using DfsClient c0 = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            await using DfsClient observer = await cluster.ConnectAsync(new TestClientOptions { Name = "obs" }, Ct);
            DfsVolume v = c0.GetVolume(Volume);
            DfsVolume ov = observer.GetVolume(Volume);
            byte[] old = TestData.Bytes(6001, 3 * chunk);
            await v.Files.WriteAllBytesAsync("/f", old, Ct);
            HashSet<ObjectId> oldObjects = await FileObjectsAsync(cluster, "/f");
            HashSet<ObjectId> before = await StoredAsync(cluster);
            byte[] next = TestData.Bytes(6002, (4 * chunk) + 1);

            TestNode leader = await cluster.GetLeaderAsync(Volume, Ct);
            DfsStream stream = await v.Files.OpenAsync("/f", Writer, Ct);
            await stream.WriteAsync(next, Ct);
            FaultArm arm = cluster.Faults.Arm(new FaultMatch(site, Node: leader.Name, Volume: Volume, Operation: DfsOperationKind.CommitFile), FaultAction.CrashNode(mode));
            Exception? failure = await Record.ExceptionAsync(() => stream.CommitAsync(Ct));
            cluster.Faults.AssertReached(arm);
            DfsOperationId? opId = (failure as DfsCommitOutcomeUnknownException)?.OperationId;
            if (failure is not null && opId is null)
            {
                // Before a quorum commit, an SDK retry of the same operation at the new leader may obtain a definite
                // availability refusal (for example a receipt from the crashed node's old incarnation). That is the only
                // ordinary failure allowed, and it is checked below against the bytes (the old revision must remain).
                Assert.True((site is FaultSites.CommitReceiptValidated or FaultSites.RaftAfterLocalAppend) && failure is DfsUnavailableException,
                    $"a coordinator crash at {site} may only surface as an unknown outcome, got {failure.GetType().Name}: {failure.Message}");
            }

            // While the coordinator is down: one complete version, never a dangling/partial root. Copies on the crashed
            // node do not count, so a reachable object must keep at least one verified copy fewer than the ack count.
            byte[] seen = await ov.Files.ReadAllBytesAsync("/f", Ct);
            Assert.True(seen.AsSpan().SequenceEqual(old) || seen.AsSpan().SequenceEqual(next), "an observer saw neither the old nor the new complete revision");
            await AssertReachableHeldAsync(cluster, "every reachable object while the coordinator is down", minHolders: Math.Max(1, AckCopies(cluster) - 1));

            await leader.RestartAsync();
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(30), Ct);
            bool committed;
            if (failure is null)
            {
                committed = true;
            }
            else if (opId is { } id)
            {
                DfsOperationOutcome outcome = await v.QueryOperationAsync(id, resolve: true, Ct);
                Assert.True(outcome.Status is DfsOutcomeStatus.Committed or DfsOutcomeStatus.Aborted);
                committed = outcome.Status == DfsOutcomeStatus.Committed;
            }
            else
            {
                committed = false;
            }

            if (site is FaultSites.RaftAfterCommitBeforeApply or FaultSites.ApplyAfterBeforeReply)
                Assert.True(committed, $"an entry crashed at {site} was already committed by a quorum and must survive");

            if (committed)
            {
                AssertBytes(next, await ov.Files.ReadAllBytesAsync("/f", Ct), "committed revision after recovery");
                HashSet<ObjectId> live = await FileObjectsAsync(cluster, "/f");
                await AssertHeldAsync(cluster, live, AckCopies(cluster), "no committed root references a missing object");
                await QuiesceGcAsync(cluster, 20);
                await AssertHeldAsync(cluster, live, AckCopies(cluster), "staged-to-live ownership survives later collection");
                AssertBytes(next, await ov.Files.ReadAllBytesAsync("/f", Ct), "committed revision after collection");
            }
            else
            {
                AssertBytes(old, await ov.Files.ReadAllBytesAsync("/f", Ct), "previous revision stays authoritative");
                await AssertHeldAsync(cluster, oldObjects, AckCopies(cluster), "previous revision objects");
            }

            try
            {
                await stream.DisposeAsync();
            }
            catch (IOException)
            {
                // A faulted stream reports its fault on dispose.
            }

            await cluster.WaitForReplicationAsync(Volume, Ct);
            await cluster.Inspect.AssertReplicasConvergedAsync(Volume);
            HashSet<ObjectId> orphans = await StoredAsync(cluster);
            orphans.ExceptWith(before);
            orphans.ExceptWith(await ReachableAsync(cluster));
            await WaitReclaimedAsync(cluster, orphans, "uploads that were not published are reclaimed, never leaked");
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // GC-007
    // ------------------------------------------------------------------------------------------------------------

    [Theory]
    [InlineData(AbortPath.StreamAbort)]
    [InlineData(AbortPath.TransactionAbort)]
    [InlineData(AbortPath.ClientCrash)]
    [Catalog("GC-007", "S", Inv01, Inv09, Inv10, Inv16)]
    public async Task GC_007_AbortThenDelayedUploadSafe(AbortPath path)
    {
        await ClusterTest.RunAsync(Det(3), async cluster =>
        {
            int chunk = Chunk(cluster);
            DfsClient c0 = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            await using DfsClient observer = await cluster.ConnectAsync(new TestClientOptions { Name = "obs" }, Ct);
            DfsVolume v = c0.GetVolume(Volume);
            DfsVolume ov = observer.GetVolume(Volume);
            byte[] old = TestData.Bytes(7001, 2 * chunk);
            await v.Files.WriteAllBytesAsync("/f", old, Ct);
            HashSet<ObjectId> before = await StoredAsync(cluster);
            var stagingBefore = (await cluster.Inspect.GetVolumeStateAsync(Volume)).Staging.Select(s => s.Id).ToHashSet();
            byte[] data = TestData.Bytes(7002, 4 * chunk);
            List<CapturedMessage> captured;

            MessageHold hold = cluster.Network.Hold(new MessageFilter(From: "client:c0", Type: MessageType.PutObject));
            async Task PumpUntilAsync(Task work)
            {
                await hold.WaitForAsync(1, TimeSpan.FromSeconds(10));
                while (!work.IsCompleted)
                {
                    hold.ReleaseAll();
                    await cluster.DelayAsync(TimeSpan.FromMilliseconds(20), Ct);
                }

                await work;
            }

            switch (path)
            {
                case AbortPath.StreamAbort:
                {
                    DfsStream stream = await v.Files.OpenAsync("/f", Writer, Ct);
                    await PumpUntilAsync(stream.WriteAsync(data, Ct).AsTask());
                    captured = [.. hold.Captured];
                    hold.Dispose();
                    await stream.AbortAsync(Ct);
                    await stream.DisposeAsync();
                    break;
                }

                case AbortPath.TransactionAbort:
                {
                    DfsTransaction tx = await v.Transactions.BeginAsync(null, Ct);
                    await PumpUntilAsync(tx.Files.WriteAllBytesAsync("/t", data, Ct));
                    captured = [.. hold.Captured];
                    hold.Dispose();
                    await tx.AbortAsync(Ct);
                    Assert.Equal(DfsTransactionState.Aborted, tx.State);
                    await tx.DisposeAsync();
                    break;
                }

                default:
                {
                    DfsStream stream = await v.Files.OpenAsync("/f", Writer, Ct);
                    Task pending = stream.WriteAsync(data, Ct).AsTask(); // uploads stay held: never delivered before the abort
                    await hold.WaitForAsync(1, TimeSpan.FromSeconds(10));
                    cluster.KillClient(c0);
                    captured = [.. hold.Captured];
                    await WaitAsync(cluster, async () =>
                        (await cluster.Inspect.GetVolumeStateAsync(Volume)).Staging.Where(s => !stagingBefore.Contains(s.Id)).All(s => !s.Open),
                        cluster.Options.Timings.SessionTimeout + TimeSpan.FromSeconds(20), "the dead client's staging session is closed by authoritative session expiry");
                    cluster.Network.Heal();
                    hold.ReleaseAll(); // first delivery of the uploads happens only now, after the abort
                    hold.Dispose();
                    _ = pending;
                    break;
                }
            }

            Assert.NotEmpty(captured);
            var codes = new List<DfsErrorCode>();
            foreach (CapturedMessage message in captured.Where(m => m.Type == MessageType.PutObject))
                codes.Add(await cluster.Network.ReplayAsync(message));
            TestContext.Current.TestOutputHelper?.WriteLine($"replayed {codes.Count} uploads after the abort: {string.Join(",", codes)}");

            await QuiesceGcAsync(cluster, 4);
            if (path == AbortPath.TransactionAbort)
                Assert.False(await ov.Files.ExistsAsync("/t", Ct), "an aborted transaction's file appeared");
            AssertBytes(old, await ov.Files.ReadAllBytesAsync("/f", Ct), "aborted uploads are never published");
            HashSet<ObjectId> reachable = await ReachableAsync(cluster);
            Assert.True(reachable.IsSubsetOf(before), "an aborted/replayed upload became reachable");

            await WaitAsync(cluster, async () => (await StoredAsync(cluster)).IsSubsetOf(before), ReclaimBudget(cluster),
                "late uploads after an abort remain unreachable orphans and are reclaimed");

            // A second late replay after reclamation still cannot publish anything.
            foreach (CapturedMessage message in captured.Where(m => m.Type == MessageType.PutObject))
                await cluster.Network.ReplayAsync(message);
            await QuiesceGcAsync(cluster, 4);
            Assert.True((await ReachableAsync(cluster)).IsSubsetOf(before));
            await WaitAsync(cluster, async () => (await StoredAsync(cluster)).IsSubsetOf(before), ReclaimBudget(cluster), "replayed orphans are reclaimed again");
            AssertBytes(old, await ov.Files.ReadAllBytesAsync("/f", Ct), "final content");
            if (path != AbortPath.ClientCrash)
                await c0.DisposeAsync();
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // GC-008
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<SnapshotRace, int> SnapshotRaceData()
    {
        var data = new TheoryData<SnapshotRace, int>();
        foreach (SnapshotRace race in Enum.GetValues<SnapshotRace>())
        {
            data.Add(race, 1);
            data.Add(race, 3);
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(SnapshotRaceData))]
    [Catalog("GC-008", "S", Inv01, Inv09, Inv10, Inv16)]
    public async Task GC_008_SnapshotCreationRacesCollection(SnapshotRace race, int sweepOccurrence)
    {
        await ClusterTest.RunAsync(Det(3), async cluster =>
        {
            int chunk = Chunk(cluster);
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            DfsVolume v = client.GetVolume(Volume);
            byte[] a = TestData.Bytes(8001, 3 * chunk);
            byte[] b = TestData.Bytes(8002, 2 * chunk);
            await v.Files.WriteAllBytesAsync("/a", a, Ct);
            await v.Files.WriteAllBytesAsync("/b", b, Ct);
            for (int i = 0; i < 5; i++)
                await v.Files.WriteAllBytesAsync("/g", TestData.Bytes(8010 + (ulong)i, 3 * chunk), Ct); // garbage for the sweeper

            FaultArm arm = cluster.Faults.Arm(new FaultMatch(FaultSites.GcSweep, Occurrence: sweepOccurrence), FaultAction.Block());
            await arm.WaitTriggeredAsync(TimeSpan.FromSeconds(30));

            Dictionary<string, byte[]> expected;
            HashSet<ObjectId> snapshotObjects;
            switch (race)
            {
                case SnapshotRace.SnapshotDuringSweep:
                    await v.Snapshots.CreateAsync("s", null, Ct);
                    expected = new() { ["/a"] = a, ["/b"] = b };
                    snapshotObjects = [.. await FileObjectsAsync(cluster, "/a"), .. await FileObjectsAsync(cluster, "/b")];
                    await v.Files.WriteAllBytesAsync("/a", TestData.Bytes(8020, 3 * chunk), Ct);
                    await v.Files.DeleteAsync("/b", Ct);
                    break;
                case SnapshotRace.CommitAndSnapshotDuringSweep:
                {
                    byte[] newA = TestData.Bytes(8021, 4 * chunk);
                    await v.Files.WriteAllBytesAsync("/a", newA, new DfsMutationOptions { CommitSnapshotName = "s" }, Ct);
                    expected = new() { ["/a"] = newA, ["/b"] = b };
                    snapshotObjects = [.. await FileObjectsAsync(cluster, "/a"), .. await FileObjectsAsync(cluster, "/b")];
                    await v.Files.WriteAllBytesAsync("/a", TestData.Bytes(8022, chunk), Ct);
                    await v.Files.DeleteAsync("/b", Ct);
                    break;
                }

                default:
                {
                    byte[] newA = TestData.Bytes(8023, 2 * chunk);
                    await v.Files.WriteAllBytesAsync("/a", newA, Ct);
                    await v.Snapshots.CreateAsync("s", null, Ct);
                    expected = new() { ["/a"] = newA, ["/b"] = b };
                    snapshotObjects = [.. await FileObjectsAsync(cluster, "/a"), .. await FileObjectsAsync(cluster, "/b")];
                    await v.Files.DeleteAsync("/a", Ct);
                    await v.Files.DeleteAsync("/b", Ct);
                    break;
                }
            }

            cluster.Faults.AssertReached(arm);
            arm.Release();
            arm.Disarm();
            await QuiesceGcAsync(cluster, 20);

            await AssertHeldAsync(cluster, snapshotObjects, AckCopies(cluster), $"objects captured by a snapshot created during a sweep ({race})");
            foreach ((string p, byte[] bytes) in expected)
                AssertBytes(bytes, await ReadSnapshotFileAsync(v, "s", p), $"snapshot s:{p} after the racing sweep");
            await AssertReachableHeldAsync(cluster, "after the race");

            await v.Snapshots.DeleteAsync("s", Ct);
            HashSet<ObjectId> unshared = [.. snapshotObjects.Except(await ReachableAsync(cluster))];
            await WaitReclaimedAsync(cluster, unshared, "snapshot-only objects are reclaimed after the snapshot is deleted");
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // GC-009
    // ------------------------------------------------------------------------------------------------------------

    private static bool IsTransferPin(PinInspection pin) => pin.Kind.Contains("Transfer", StringComparison.OrdinalIgnoreCase);

    [Fact]
    [Catalog("GC-009", "S", Inv01, Inv09, Inv10, Inv16)]
    public async Task GC_009_BackupTransferPinProtectsClosure()
    {
        await ClusterTest.RunAsync(Det(3) with { BackupNodeCount = 1 }, async cluster =>
        {
            int chunk = Chunk(cluster);
            TestBackupNode b0 = cluster.BackupNodes[0];
            await b0.RegisterAsync(Volume, new DfsBackupSchedule(), new DfsBackupBudgets(TimeSpan.FromMinutes(30), 1L << 30));
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            DfsVolume v = client.GetVolume(Volume);
            byte[] a = TestData.Bytes(9001, 3 * chunk);
            byte[] b = TestData.Bytes(9002, 2 * chunk);
            await v.Files.WriteAllBytesAsync("/a", a, Ct);
            await v.Files.WriteAllBytesAsync("/b", b, Ct);
            await v.Snapshots.CreateAsync("s1", null, Ct);
            HashSet<ObjectId> closure = [.. await FileObjectsAsync(cluster, "/a"), .. await FileObjectsAsync(cluster, "/b")];

            using (MessageHold transfer = cluster.Network.Hold(new MessageFilter(Type: MessageType.BackupPutObject)))
            {
                await v.Backups.RequestSyncAsync(b0.TargetName, "s1", Ct);
                await transfer.WaitForAsync(1, TimeSpan.FromSeconds(30));

                // Live state changes and the snapshot's own retention reference goes away while the transfer runs.
                await v.Files.WriteAllBytesAsync("/a", TestData.Bytes(9003, chunk), Ct);
                await v.Files.DeleteAsync("/b", Ct);
                await v.Snapshots.DeleteAsync("s1", Ct);
                await QuiesceGcAsync(cluster, 20);
                JumpWallClocks(cluster, TimeSpan.FromDays(1));
                await QuiesceGcAsync(cluster, 10);

                VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Volume);
                Assert.Contains(state.Pins, IsTransferPin);
                await AssertHeldAsync(cluster, closure, AckCopies(cluster), "the transfer's dependency closure while the transfer is in progress");
                Assert.True((await cluster.Inspect.GetReplicaHealthAsync(Volume)).PinnedBytes > 0, "transfer-pinned bytes are not accounted");
                transfer.ReleaseAll();
            }

            DfsBackupCheckpoint checkpoint = await v.Backups.AwaitBackupSnapshotAsync(b0.TargetName, "s1", TimeSpan.FromSeconds(60), Ct);
            Assert.Equal("s1", checkpoint.SnapshotName);
            Assert.True(checkpoint.IsExactSnapshot);
            await using (DfsClient backup = await cluster.ConnectBackupAsync(0, null, Ct))
            {
                await using DfsSnapshotView view = await backup.GetVolume(Volume).Snapshots.OpenAsync("s1", Ct);
                AssertBytes(a, await view.Files.ReadAllBytesAsync("/a", Ct), "backup s1:/a");
                AssertBytes(b, await view.Files.ReadAllBytesAsync("/b", Ct), "backup s1:/b");
            }

            await WaitAsync(cluster, async () => !(await cluster.Inspect.GetVolumeStateAsync(Volume)).Pins.Any(IsTransferPin),
                TimeSpan.FromSeconds(30), "the transfer pin is released after completion");
            HashSet<ObjectId> released = [.. closure.Except(await ReachableAsync(cluster))];
            await WaitReclaimedAsync(cluster, released, "the closure is reclaimed once no transfer or snapshot needs it");
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // GC-010
    // ------------------------------------------------------------------------------------------------------------

    [Fact]
    [Catalog("GC-010", "S", Inv01, Inv09, Inv10, Inv16)]
    public async Task GC_010_TransferAbandonmentIsFenced()
    {
        await ClusterTest.RunAsync(Det(3) with { BackupNodeCount = 1 }, async cluster =>
        {
            int chunk = Chunk(cluster);
            TestBackupNode b0 = cluster.BackupNodes[0];
            await b0.RegisterAsync(Volume, new DfsBackupSchedule(), new DfsBackupBudgets(TimeSpan.FromSeconds(5), 1L << 30));
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            DfsVolume v = client.GetVolume(Volume);
            string target = b0.TargetName;

            byte[] a0 = TestData.Bytes(10_001, 2 * chunk);
            await v.Files.WriteAllBytesAsync("/a", a0, Ct);
            await v.Snapshots.CreateAsync("s0", null, Ct);
            await v.Backups.RequestSyncAsync(target, "s0", Ct);
            await v.Backups.AwaitBackupSnapshotAsync(target, "s0", TimeSpan.FromSeconds(60), Ct);
            DfsBackupStatus status0 = await v.Backups.GetStatusAsync(target, Ct);
            string? pointer0 = await b0.Inspect.GetPublishedPointerAsync(Volume);

            byte[] a1 = TestData.Bytes(10_002, 3 * chunk);
            byte[] c1 = TestData.Bytes(10_003, 2 * chunk);
            await v.Files.WriteAllBytesAsync("/a", a1, Ct);
            await v.Files.WriteAllBytesAsync("/c", c1, Ct);
            await v.Snapshots.CreateAsync("s1", null, Ct);
            HashSet<ObjectId> s1Only = [.. await FileObjectsAsync(cluster, "/a"), .. await FileObjectsAsync(cluster, "/c")];

            MessageHold objects = cluster.Network.Hold(new MessageFilter(Type: MessageType.BackupPutObject));
            MessageHold manifests = cluster.Network.Hold(new MessageFilter(Type: MessageType.BackupPutManifest));
            MessageHold publishes = cluster.Network.Hold(new MessageFilter(Type: MessageType.BackupPublish));
            await v.Backups.RequestSyncAsync(target, "s1", Ct);
            await objects.WaitForAsync(1, TimeSpan.FromSeconds(30));

            // The destination makes no progress beyond the source's pin-age budget: the lineage is abandoned.
            await cluster.DelayAsync(TimeSpan.FromSeconds(15), Ct);
            await WaitAsync(cluster, async () =>
            {
                DfsBackupStatus s = await v.Backups.GetStatusAsync(target, Ct);
                return s.ReseedRequired || s.State == DfsBackupState.ReseedRequired;
            }, TimeSpan.FromSeconds(30), "exceeding the transfer pin budget abandons the lineage and requires a reseed");
            await WaitAsync(cluster, async () => !(await cluster.Inspect.GetVolumeStateAsync(Volume)).Pins.Any(IsTransferPin),
                TimeSpan.FromSeconds(30), "the abandoned transfer releases its source pins");

            // The source reclaims the abandoned closure.
            byte[] a2 = TestData.Bytes(10_004, chunk);
            await v.Files.WriteAllBytesAsync("/a", a2, Ct);
            await v.Files.DeleteAsync("/c", Ct);
            await v.Snapshots.DeleteAsync("s1", Ct);
            HashSet<ObjectId> gone = [.. s1Only.Except(await ReachableAsync(cluster))];
            await WaitReclaimedAsync(cluster, gone, "the abandoned transfer no longer protects its closure");

            // Deliver the stale transfer's requests late, then replay them again.
            List<CapturedMessage> stale = [.. objects.Captured, .. manifests.Captured, .. publishes.Captured];
            Assert.NotEmpty(stale);
            objects.ReleaseAll();
            manifests.ReleaseAll();
            publishes.ReleaseAll();
            objects.Dispose();
            manifests.Dispose();
            publishes.Dispose();
            foreach (CapturedMessage message in stale)
                await cluster.Network.ReplayAsync(message);
            await QuiesceGcAsync(cluster, 8);

            IReadOnlyList<string> published = await b0.Inspect.GetPublishedSnapshotsAsync(Volume);
            await using (DfsClient backup = await cluster.ConnectBackupAsync(0, null, Ct))
            {
                DfsVolume bv = backup.GetVolume(Volume);
                if (published.Contains("s1"))
                {
                    // Only a complete s1 may ever be exposed.
                    await using DfsSnapshotView s1 = await bv.Snapshots.OpenAsync("s1", Ct);
                    AssertBytes(a1, await s1.Files.ReadAllBytesAsync("/a", Ct), "stale-lineage s1:/a");
                    AssertBytes(c1, await s1.Files.ReadAllBytesAsync("/c", Ct), "stale-lineage s1:/c");
                }
                else
                {
                    Assert.Equal(pointer0, await b0.Inspect.GetPublishedPointerAsync(Volume));
                }

                await using DfsSnapshotView s0 = await bv.Snapshots.OpenAsync("s0", Ct);
                AssertBytes(a0, await s0.Files.ReadAllBytesAsync("/a", Ct), "the last complete snapshot stays readable");
            }

            // The next sync is an explicit reseed under a new lineage.
            await v.Snapshots.CreateAsync("s2", null, Ct);
            await v.Backups.RequestSyncAsync(target, "s2", Ct);
            await v.Backups.AwaitBackupSnapshotAsync(target, "s2", TimeSpan.FromSeconds(60), Ct);
            DfsBackupStatus status2 = await v.Backups.GetStatusAsync(target, Ct);
            Assert.NotEqual(status0.LineageId, status2.LineageId);
            Assert.False(status2.ReseedRequired);
            await using (DfsClient backup = await cluster.ConnectBackupAsync(0, null, Ct))
            {
                await using DfsSnapshotView s2 = await backup.GetVolume(Volume).Snapshots.OpenAsync("s2", Ct);
                AssertBytes(a2, await s2.Files.ReadAllBytesAsync("/a", Ct), "reseeded s2:/a");
                Assert.False(await s2.Files.ExistsAsync("/c", Ct));
            }
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // GC-011
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// A node is drained (placement transition) while GC collects concurrent garbage. Old copies on the draining node
    /// must remain until verified replacements meet the policy: with replication blocked nothing is deleted from it, and
    /// the first old-replica deletion happens only once the object has its full target on other nodes.
    /// </summary>
    [Theory]
    [InlineData(RebalanceBlock.OldReplicaDelete)]
    [InlineData(RebalanceBlock.Replication)]
    [Catalog("GC-011", "S", Inv01, Inv09, Inv10, Inv16)]
    public async Task GC_011_RebalancePinsOldAndNewCopies(RebalanceBlock block)
    {
        await ClusterTest.RunAsync(Det(4), async cluster =>
        {
            int chunk = Chunk(cluster);
            int target = cluster.Options.GetEffectiveDefaultPolicy().TargetCopies;
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            DfsVolume v = client.GetVolume(Volume);
            var files = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            for (int i = 0; i < 6; i++)
            {
                files[$"/f{i}"] = TestData.Bytes(11_000 + (ulong)i, 3 * chunk);
                await v.Files.WriteAllBytesAsync($"/f{i}", files[$"/f{i}"], Ct);
            }

            TestNode victim = cluster.Nodes[0];
            int most = -1;
            foreach (TestNode node in cluster.Nodes)
            {
                int count = (await node.Inspect.ListObjectsAsync(Volume)).Count;
                if (count > most)
                {
                    most = count;
                    victim = node;
                }
            }

            HashSet<ObjectId> reachable = await ReachableAsync(cluster);
            HashSet<ObjectId> victimObjects = [.. (await victim.Inspect.ListObjectsAsync(Volume)).Select(o => o.Id).Where(reachable.Contains)];
            Assert.NotEmpty(victimObjects);

            async Task ChurnAsync(int round) =>
                await v.Files.WriteAllBytesAsync("/churn", TestData.Bytes(11_100 + (ulong)round, 2 * chunk), Ct); // garbage for concurrent GC

            async Task AssertReadsAsync(string when)
            {
                foreach ((string path, byte[] data) in files)
                    AssertBytes(data, await v.Files.ReadAllBytesAsync(path, Ct), $"{path} {when}");
            }

            if (block == RebalanceBlock.OldReplicaDelete)
            {
                ObjectId? examined = null;
                for (int attempt = 0; attempt < 20 && examined is null; attempt++)
                {
                    FaultArm arm = cluster.Faults.Arm(new FaultMatch(FaultSites.ReplicaDelete, Node: victim.Name), FaultAction.Block());
                    if (attempt == 0)
                        await cluster.Admin.DrainNodeAsync(victim.Name, Ct);
                    for (int round = 0; !arm.Triggered && round < 40; round++)
                    {
                        await ChurnAsync((attempt * 100) + round);
                        await QuiesceGcAsync(cluster, 2);
                        await AssertReachableHeldAsync(cluster, $"during the drain (attempt {attempt}, round {round})");
                    }

                    await arm.WaitTriggeredAsync(TimeSpan.FromSeconds(60));
                    cluster.Faults.AssertReached(arm);
                    FaultContext context = arm.Contexts[0];
                    Assert.True(context.Object is not null, "the replica-delete site must identify the object being deleted");
                    ObjectId deleting = context.Object!.Value;
                    if ((await ReachableAsync(cluster)).Contains(deleting))
                    {
                        IReadOnlyList<string> holders = await cluster.Inspect.GetObjectHoldersAsync(deleting);
                        int replacements = holders.Count(h => h != victim.Name);
                        Assert.True(replacements >= target,
                            $"the old copy of live object {deleting} on {victim.Name} is being deleted while only {replacements} verified replacement(s) exist (target {target}).");
                        Assert.True(await victim.Inspect.HasObjectAsync(deleting), "the old copy vanished before its deletion step");
                        await AssertReadsAsync("while an old-replica deletion is blocked");
                        examined = deleting;
                    }

                    arm.Release();
                    arm.Disarm();
                }

                Assert.True(examined is not null, $"no old replica of a live object was ever deleted from the draining node {victim.Name}");
            }
            else
            {
                using MessageHold replication = cluster.Network.Hold(new MessageFilter(Type: MessageType.ReplicateObject));
                await cluster.Admin.DrainNodeAsync(victim.Name, Ct);
                for (int round = 0; round < 8; round++)
                {
                    await ChurnAsync(round);
                    await QuiesceGcAsync(cluster, 4);
                    await AssertReachableHeldAsync(cluster, $"while replication is blocked (round {round})");
                    foreach (ObjectId id in victimObjects)
                    {
                        IReadOnlyList<string> others = [.. (await cluster.Inspect.GetObjectHoldersAsync(id)).Where(h => h != victim.Name)];
                        Assert.True(await victim.Inspect.HasObjectAsync(id) || others.Count >= target,
                            $"round {round}: {victim.Name} deleted its copy of {id} before {target} verified replacements existed ({others.Count}).");
                    }
                }

                await AssertReadsAsync("while replication is blocked");
                replication.ReleaseAll();
            }

            await WaitAsync(cluster, async () =>
            {
                DfsNodeInfo info = (await cluster.Admin.ListNodesAsync(Ct)).Single(n => n.Name == victim.Name);
                return info.Status == DfsNodeStatus.Drained;
            }, TimeSpan.FromMinutes(3), $"{victim.Name} finishes draining once replacements are verified");

            foreach (ObjectId id in await ReachableAsync(cluster))
            {
                IReadOnlyList<string> holders = await cluster.Inspect.GetObjectHoldersAsync(id);
                Assert.DoesNotContain(victim.Name, holders);
                Assert.True(holders.Count >= target, $"object {id} has {holders.Count} copies after the drain (target {target})");
            }

            await AssertReadsAsync("after the drain");
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // GC-012
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>Captures every message matching <paramref name="filter"/> while <paramref name="work"/> runs (released as they come).</summary>
    private static async Task<List<CapturedMessage>> CaptureWhileAsync(TestCluster cluster, MessageFilter filter, Func<Task> work)
    {
        using MessageHold hold = cluster.Network.Hold(filter);
        Task task = work();
        await hold.WaitForAsync(1, TimeSpan.FromSeconds(10));
        while (!task.IsCompleted)
        {
            hold.ReleaseAll();
            await cluster.DelayAsync(TimeSpan.FromMilliseconds(20), Ct);
        }

        await task;
        return [.. hold.Captured];
    }

    [Fact]
    [Catalog("GC-012", "S", Inv01, Inv09, Inv10, Inv16)]
    public async Task GC_012_ReferenceUpdatesIdempotent()
    {
        await ClusterTest.RunAsync(Det(3), async cluster =>
        {
            int chunk = Chunk(cluster);
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            DfsVolume v = client.GetVolume(Volume);
            byte[] v1 = TestData.Bytes(12_001, 4 * chunk);
            await v.Files.WriteAllBytesAsync("/f", v1, Ct);
            await v.Snapshots.CreateAsync("s1", null, Ct);
            await v.Snapshots.CreateAsync("s2", null, Ct);
            await v.Snapshots.CreateAsync("sx", null, Ct);
            await v.Files.CopyAsync("/f", "/g", false, Ct);
            HashSet<ObjectId> v1Objects = await FileObjectsAsync(cluster, "/f");

            // 1. Commit duplicated in transit and retried after lost replies: one logical effect.
            MessageRule dupCommit = cluster.Network.DuplicateNext(new MessageFilter(From: "client:c0", Type: MessageType.WriteAll), 3);
            MessageRule lostCommit = cluster.Network.DropReplies(new MessageFilter(From: "client:c0", Type: MessageType.WriteAll), 2);
            byte[] v2 = TestData.Bytes(12_002, 3 * chunk);
            await v.Files.WriteAllBytesAsync("/f", v2, Ct);
            Assert.True(dupCommit.Matched > 0 && lostCommit.Matched > 0, "commit duplication/reply loss was not exercised");
            dupCommit.Cancel();
            lostCommit.Cancel();
            AssertBytes(v2, await v.Files.ReadAllBytesAsync("/f", Ct), "/f after duplicated and retried commit");

            // 2. Snapshot delete: duplicated and retried (sx), then replayed after the name is reused (s1).
            MessageRule dupDelete = cluster.Network.DuplicateNext(new MessageFilter(From: "client:c0", Type: MessageType.DeleteSnapshot), 2);
            MessageRule lostDelete = cluster.Network.DropReplies(new MessageFilter(From: "client:c0", Type: MessageType.DeleteSnapshot), 1);
            await v.Snapshots.DeleteAsync("sx", Ct);
            Assert.True(dupDelete.Matched > 0 && lostDelete.Matched > 0, "snapshot-delete duplication/reply loss was not exercised");
            dupDelete.Cancel();
            lostDelete.Cancel();
            Assert.DoesNotContain(await v.Snapshots.ListAsync(Ct), s => s.Name == "sx");
            List<CapturedMessage> deletes = await CaptureWhileAsync(cluster, new MessageFilter(From: "client:c0", Type: MessageType.DeleteSnapshot),
                () => v.Snapshots.DeleteAsync("s1", Ct));
            await v.Snapshots.CreateAsync("s1", null, Ct); // new snapshot reusing the name (captures v2)
            foreach (CapturedMessage message in deletes)
                await cluster.Network.ReplayAsync(message);
            Assert.Contains(await v.Snapshots.ListAsync(Ct), s => s.Name == "s1");
            AssertBytes(v2, await ReadSnapshotFileAsync(v, "s1", "/f"), "the re-created s1 survives replays of the old delete");

            // 3. Abort duplicated (tx1), and an abort replayed later (tx2).
            DfsTransaction tx1 = await v.Transactions.BeginAsync(null, Ct);
            await tx1.Files.WriteAllBytesAsync("/tx", TestData.Bytes(12_003, 2 * chunk), Ct);
            MessageRule dupAbort = cluster.Network.DuplicateNext(new MessageFilter(From: "client:c0", Type: MessageType.TxAbort), 2);
            await tx1.AbortAsync(Ct);
            Assert.True(dupAbort.Matched > 0, "abort duplication was not exercised");
            dupAbort.Cancel();
            await tx1.DisposeAsync();
            DfsTransaction tx2 = await v.Transactions.BeginAsync(null, Ct);
            await tx2.Files.WriteAllBytesAsync("/tx", TestData.Bytes(12_004, 2 * chunk), Ct);
            List<CapturedMessage> aborts = await CaptureWhileAsync(cluster, new MessageFilter(From: "client:c0", Type: MessageType.TxAbort), () => tx2.AbortAsync(Ct));
            foreach (CapturedMessage message in aborts)
                await cluster.Network.ReplayAsync(message);
            Assert.False(await v.Files.ExistsAsync("/tx", Ct));
            await tx2.DisposeAsync();

            // 4. Copy retried after lost replies.
            MessageRule lostCopy = cluster.Network.DropReplies(new MessageFilter(From: "client:c0", Type: MessageType.Copy), 2);
            await v.Files.CopyAsync("/f", "/h", false, Ct);
            Assert.True(lostCopy.Matched > 0, "copy reply loss was not exercised");
            lostCopy.Cancel();

            await QuiesceGcAsync(cluster, 20);
            await AssertHeldAsync(cluster, v1Objects, AckCopies(cluster), "objects still shared by s2 and /g after replayed reference events");
            AssertBytes(v1, await ReadSnapshotFileAsync(v, "s2", "/f"), "s2:/f");
            AssertBytes(v1, await v.Files.ReadAllBytesAsync("/g", Ct), "/g");
            AssertBytes(v2, await v.Files.ReadAllBytesAsync("/f", Ct), "/f");
            AssertBytes(v2, await v.Files.ReadAllBytesAsync("/h", Ct), "/h");
            await AssertReachableHeldAsync(cluster, "after replayed reference events");

            // Releasing every reference reclaims everything exactly once: no underflow survivors, no leak.
            await v.Snapshots.DeleteAsync("s2", Ct);
            await v.Snapshots.DeleteAsync("s1", Ct);
            foreach (string path in new[] { "/f", "/g", "/h" })
                await v.Files.DeleteAsync(path, Ct);
            await WaitNoGarbageAsync(cluster, "every released object is reclaimed");
            Assert.Empty(await ReachableAsync(cluster));
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // GC-013
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<string, CrashMode, int> SweepCrashData()
    {
        var data = new TheoryData<string, CrashMode, int>();
        foreach (string target in new[] { "leader", "follower" })
        {
            foreach (CrashMode mode in new[] { CrashMode.LoseUnflushed, CrashMode.KeepUnflushed, CrashMode.TearLastWrite })
            {
                data.Add(target, mode, 1);
                data.Add(target, mode, 2);
            }
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(SweepCrashData))]
    [Catalog("GC-013", "S", Inv01, Inv09, Inv10, Inv16)]
    public async Task GC_013_CrashDuringSweep(string target, CrashMode mode, int occurrence)
    {
        await ClusterTest.RunAsync(Det(3), async cluster =>
        {
            int chunk = Chunk(cluster);
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            await using DfsClient stager = await cluster.ConnectAsync(new TestClientOptions { Name = "c1" }, Ct);
            DfsVolume v = client.GetVolume(Volume);
            var live = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            for (int i = 0; i < 4; i++)
            {
                live[$"/l{i}"] = TestData.Bytes(13_000 + (ulong)i, 2 * chunk);
                await v.Files.WriteAllBytesAsync($"/l{i}", live[$"/l{i}"], Ct);
            }

            await v.Snapshots.CreateAsync("s", null, Ct);
            var snapshot = new Dictionary<string, byte[]>(live, StringComparer.Ordinal);
            for (int i = 0; i < 4; i++)
                await v.Files.WriteAllBytesAsync("/g", TestData.Bytes(13_010 + (ulong)i, 2 * chunk), Ct);

            // Unpublished uploads that must survive the crash and the resumed collection.
            HashSet<ObjectId> beforeStage = await StoredAsync(cluster);
            byte[] staged = TestData.Bytes(13_020, 3 * chunk);
            await v.Files.WriteAllBytesAsync("/staged", TestData.Bytes(13_021, 10), Ct);
            DfsStream stagedStream = await stager.GetVolume(Volume).Files.OpenAsync("/staged", Writer, Ct);
            await stagedStream.WriteAsync(staged, Ct);
            await WaitAsync(cluster, async () => (await StoredAsync(cluster)).Except(beforeStage).Count() >= 2, TimeSpan.FromSeconds(10), "staged chunks uploaded");
            HashSet<ObjectId> stagedObjects = [.. (await StoredAsync(cluster)).Except(beforeStage).Except(await ReachableAsync(cluster))];

            live["/l0"] = TestData.Bytes(13_100, 2 * chunk);
            await v.Files.WriteAllBytesAsync("/l0", live["/l0"], Ct);

            TestNode leader = await cluster.GetLeaderAsync(Volume, Ct);
            TestNode node = target == "follower" ? cluster.Nodes.First(n => n.Name != leader.Name) : leader;
            FaultArm arm = cluster.Faults.Arm(new FaultMatch(FaultSites.GcSweep, Node: node.Name, Occurrence: occurrence), FaultAction.CrashNode(mode));

            // New commits keep changing reachability around the sweep (garbage file only, so the live model stays exact).
            int round = 0;
            while (!arm.Triggered && round < 200)
            {
                try
                {
                    await v.Files.WriteAllBytesAsync("/g", TestData.Bytes(13_400 + (ulong)round, chunk), Ct);
                }
                catch (DfsException) when (arm.Triggered)
                {
                    break; // this write raced the crash; its outcome is irrelevant to the garbage file
                }

                await cluster.DelayAsync(cluster.Options.Timings.GcInterval / 2, Ct);
                round++;
            }

            await arm.WaitTriggeredAsync(TimeSpan.FromSeconds(30));
            cluster.Faults.AssertReached(arm);
            await WaitAsync(cluster, () => Task.FromResult(node.State == TestNodeState.Crashed), TimeSpan.FromSeconds(10), "the sweeping node crashed");

            // Safety while the node is down.
            foreach ((string path, byte[] data) in live)
                AssertBytes(data, await v.Files.ReadAllBytesAsync(path, Ct), $"{path} while {node.Name} is down");
            await AssertHeldAsync(cluster, await ReachableAsync(cluster), 1, "reachable objects while the sweeping node is down");
            await AssertHeldAsync(cluster, stagedObjects, 1, "staged objects while the sweeping node is down");

            await node.RestartAsync();
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(30), Ct);
            live["/l1"] = TestData.Bytes(13_200, 2 * chunk);
            await v.Files.WriteAllBytesAsync("/l1", live["/l1"], Ct);
            await QuiesceGcAsync(cluster, 20);

            foreach ((string path, byte[] data) in live)
                AssertBytes(data, await v.Files.ReadAllBytesAsync(path, Ct), $"{path} after recovery");
            foreach ((string path, byte[] data) in snapshot)
                AssertBytes(data, await ReadSnapshotFileAsync(v, "s", path), $"s:{path} after recovery");
            await AssertReachableHeldAsync(cluster, "after the crashed sweep resumed");
            await AssertHeldAsync(cluster, stagedObjects, AckCopies(cluster), "staged uploads after the crashed sweep resumed");

            CommitToken token = await stagedStream.CommitAsync(Ct);
            Assert.False(token.IsNone);
            await stagedStream.DisposeAsync();
            AssertBytes(staged, await v.Files.ReadAllBytesAsync("/staged", Ct), "staged data published after recovery");
            await WaitNoGarbageAsync(cluster, "collection resumes after the crash and reclaims the garbage");
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // GC-014
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<string, CrashMode> CompactionCrashData()
    {
        var data = new TheoryData<string, CrashMode>();
        foreach (string site in new[] { FaultSites.CompactionCopy, FaultSites.CompactionPublish, FaultSites.CompactionDelete })
        {
            foreach (CrashMode mode in new[] { CrashMode.LoseUnflushed, CrashMode.KeepUnflushed, CrashMode.TearLastWrite })
                data.Add(site, mode);
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(CompactionCrashData))]
    [Catalog("GC-014", "S", Inv01, Inv09, Inv10, Inv16)]
    public async Task GC_014_CrashDuringCompactionAndGc(string site, CrashMode mode)
    {
        await ClusterTest.RunAsync(Det(3), async cluster =>
        {
            int chunk = Chunk(cluster);
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            DfsVolume v = client.GetVolume(Volume);
            var live = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            for (int i = 0; i < 12; i++)
            {
                live[$"/f{i}"] = TestData.Bytes(14_000 + (ulong)i, 2 * chunk);
                await v.Files.WriteAllBytesAsync($"/f{i}", live[$"/f{i}"], Ct);
            }

            for (int i = 0; i < 8; i++)
            {
                live[$"/f{i}"] = TestData.Bytes(14_100 + (ulong)i, (2 * chunk) + 1);
                await v.Files.WriteAllBytesAsync($"/f{i}", live[$"/f{i}"], Ct);
            }

            for (int i = 9; i < 12; i++)
            {
                await v.Files.DeleteAsync($"/f{i}", Ct);
                live.Remove($"/f{i}");
            }

            HashSet<ObjectId> garbage = await StoredAsync(cluster);
            garbage.ExceptWith(await ReachableAsync(cluster));
            Assert.NotEmpty(garbage);

            TestNode leader = await cluster.GetLeaderAsync(Volume, Ct);
            TestNode node = cluster.Nodes.First(n => n.Name != leader.Name);
            TestNode reference = cluster.Nodes.First(n => n.Name != node.Name);

            // The pass is held at the stage under test, so collection provably overlaps the relocation instead of
            // racing a pass that may finish (or crash) before any sweep runs.
            FaultArm arm = cluster.Faults.Arm(new FaultMatch(site, Node: node.Name), FaultAction.Block());
            Task compaction = node.CompactStorageAsync();
            await arm.WaitTriggeredAsync(TimeSpan.FromSeconds(30));
            cluster.Faults.AssertReached(arm);

            // While the pass is mid-flight, live records it may be relocating become garbage (superseded files) and GC
            // passes run, so reachability deletion interleaves with compaction on this node.
            int sweepsBefore = SweepHits(cluster);
            for (int i = 0; i < 4; i++)
            {
                live[$"/f{i}"] = TestData.Bytes(14_200 + (ulong)i, (2 * chunk) + 2);
                await v.Files.WriteAllBytesAsync($"/f{i}", live[$"/f{i}"], Ct);
            }

            HashSet<ObjectId> moreGarbage = await StoredAsync(cluster);
            moreGarbage.ExceptWith(await ReachableAsync(cluster));
            garbage.UnionWith(moreGarbage);
            await QuiesceGcAsync(cluster, 8);
            Assert.True(SweepHits(cluster) > sweepsBefore, $"no GC sweep ran while the compaction pass was held at {site}");
            Assert.False(compaction.IsCompleted, $"the compaction pass completed although it was held at {site}");

            // Power is lost while the pass is at the stage, after the interleaved collection. Every record the node held
            // for a reachable object at that moment was durable and must survive the interrupted relocation.
            HashSet<ObjectId> reachableAtCrash = await ReachableAsync(cluster);
            IReadOnlyList<ObjectRecordInfo> nodeIndex = await node.Inspect.ListObjectsAsync(Volume);
            List<ObjectRecordInfo> indexAtCrash = [.. nodeIndex.Where(r => reachableAtCrash.Contains(r.Id))];
            TestContext.Current.TestOutputHelper?.WriteLine(
                $"{site}: {garbage.Count(id => !nodeIndex.Any(r => r.Id == id))} of {garbage.Count} garbage object(s) already reclaimed from {node.Name} while its compaction was held");
            await node.CrashAsync(mode);
            arm.Release();
            arm.Disarm();
            Exception? interrupted = await Record.ExceptionAsync(() => compaction);
            Assert.NotNull(interrupted); // a pass cut short by the crash must not report completion

            await node.RestartAsync();
            HashSet<ObjectId> stillReachable = await ReachableAsync(cluster);
            List<ObjectRecordInfo> recovered = [.. await node.Inspect.ListObjectsAsync(Volume)];
            foreach (ObjectRecordInfo record in indexAtCrash.Where(r => stillReachable.Contains(r.Id)))
                Assert.True(recovered.Contains(record), $"{node.Name}: the durable record of reachable object {record.Id} was lost by the interrupted compaction ({site}, {mode}).");
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(30), Ct);
            await QuiesceGcAsync(cluster, 10);
            await node.CompactStorageAsync();
            await QuiesceGcAsync(cluster, 10);

            // The recovered index never points at a wrong object: every record matches the same object elsewhere.
            Dictionary<ObjectId, ObjectRecordInfo> healthy = (await reference.Inspect.ListObjectsAsync(Volume)).ToDictionary(r => r.Id);
            foreach (ObjectRecordInfo record in await node.Inspect.ListObjectsAsync(Volume))
            {
                if (healthy.TryGetValue(record.Id, out ObjectRecordInfo? good))
                    Assert.True(record.Length == good.Length && record.Checksum == good.Checksum, $"{node.Name}: index entry {record.Id} describes a different object than {reference.Name}'s");
            }

            await WaitAsync(cluster, async () => !(await node.Inspect.ListObjectsAsync(Volume)).Any(r => garbage.Contains(r.Id)),
                ReclaimBudget(cluster), "reclaimed objects do not reappear in the recovered index");
            await AssertReachableHeldAsync(cluster, "after crash during compaction + GC");
            foreach ((string path, byte[] data) in live)
                AssertBytes(data, await v.Files.ReadAllBytesAsync(path, Ct), $"{path} after crash during compaction");

            long failures = (await node.GetCountersAsync()).ChecksumFailures;
            await cluster.Admin.StartScrubAsync(node.Name, Ct);
            await QuiesceGcAsync(cluster, 8);
            Assert.Equal(failures, (await node.GetCountersAsync()).ChecksumFailures);
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // GC-015
    // ------------------------------------------------------------------------------------------------------------

    [Fact]
    [Catalog("GC-015", "S", Inv01, Inv09, Inv10, Inv16)]
    public async Task GC_015_QuotaCountsPinnedAndStagedBytes()
    {
        const long mib = 1L << 20;
        const long capacity = 64 * mib;
        const long reserve = 8 * mib;
        TestClusterOptions options = Det(3) with { NodeCapacityBytes = capacity, ReservedBytes = reserve, BackupNodeCount = 1 };
        await ClusterTest.RunAsync(options, async cluster =>
        {
            TestBackupNode b0 = cluster.BackupNodes[0];
            await b0.RegisterAsync(Volume, new DfsBackupSchedule(), new DfsBackupBudgets(TimeSpan.FromMinutes(30), 1L << 30));
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            await using DfsClient stager = await cluster.ConnectAsync(new TestClientOptions { Name = "c1" }, Ct);
            DfsVolume v = client.GetVolume(Volume);

            // 6 MiB visible only through a snapshot.
            for (int i = 0; i < 6; i++)
                await v.Files.WriteAllBytesAsync($"/p{i}", TestData.Bytes(15_000 + (ulong)i, (int)mib), Ct);
            await v.Snapshots.CreateAsync("keep", null, Ct);
            for (int i = 0; i < 6; i++)
                await v.Files.DeleteAsync($"/p{i}", Ct);

            // 3 MiB staged but unpublished.
            await v.Files.WriteAllBytesAsync("/staged", new byte[] { 1 }, Ct);
            DfsStream staged = await stager.GetVolume(Volume).Files.OpenAsync("/staged", Writer, Ct);
            await staged.WriteAsync(TestData.Bytes(15_100, (int)(3 * mib)), Ct);

            // A transfer pin on the snapshot.
            MessageHold transfer = cluster.Network.Hold(new MessageFilter(Type: MessageType.BackupPutObject));
            await v.Backups.RequestSyncAsync(b0.TargetName, "keep", Ct);
            await transfer.WaitForAsync(1, TimeSpan.FromSeconds(30));

            await v.Files.WriteAllBytesAsync("/small", TestData.Bytes(15_200, 10_000), Ct);
            await QuiesceGcAsync(cluster, 8);

            DfsReplicaHealth health = await cluster.Inspect.GetReplicaHealthAsync(Volume);
            Assert.True(health.PinnedBytes >= 6 * mib, $"pinned bytes {health.PinnedBytes} do not include the 6 MiB held only by the snapshot/transfer");
            Assert.True(health.StagedBytes >= (3 * mib) - cluster.Options.Timings.ChunkSize, $"staged bytes {health.StagedBytes} do not include the 3 MiB of unpublished uploads");
            long nodeStaged = 0, nodePinned = 0;
            foreach (TestNode node in cluster.Nodes)
            {
                nodeStaged += await node.Inspect.GetStagedBytesAsync();
                nodePinned += await node.Inspect.GetPinnedBytesAsync();
            }

            Assert.True(nodeStaged >= (3 * mib) - cluster.Options.Timings.ChunkSize, $"node staged accounting {nodeStaged}");
            Assert.True(nodePinned >= 6 * mib, $"node pinned accounting {nodePinned}");
            foreach (DfsNodeInfo info in await cluster.Admin.ListNodesAsync(Ct))
                Assert.True(info.UsedBytes >= (long)(9 * mib * 0.95), $"{info.Name} reports {info.UsedBytes} used bytes although it stores ~9 MiB of pinned+staged replicas");

            // Admission counts those bytes: only what really fits is accepted.
            long accepted = 0;
            Exception? refused = null;
            for (int i = 0; i < 40 && refused is null; i++)
            {
                try
                {
                    await v.Files.WriteAllBytesAsync($"/fill{i}", TestData.Bytes(15_300 + (ulong)i, (int)(2 * mib)), Ct);
                    accepted += 2 * mib;
                }
                catch (DfsUnavailableException ex) when (IsCapacityRefusal(ex))
                {
                    refused = ex;
                }
            }

            Assert.True(refused is not null, "admission never refused although pinned, staged and new bytes exceed the capacity");
            Assert.True(accepted <= capacity - reserve - (9 * mib) + (2 * mib),
                $"admission accepted {accepted / mib} MiB of new data as if pinned/staged bytes were free (limit {(capacity - reserve - (9 * mib)) / mib} MiB + one write).");

            // Releasing the pins and staging returns the space.
            await staged.AbortAsync(Ct);
            await staged.DisposeAsync();
            transfer.ReleaseAll();
            transfer.Dispose();
            await v.Backups.AwaitBackupSnapshotAsync(b0.TargetName, "keep", TimeSpan.FromSeconds(60), Ct);
            await v.Snapshots.DeleteAsync("keep", Ct);
            await WaitAsync(cluster, async () =>
            {
                try
                {
                    await v.Files.WriteAllBytesAsync("/after-release", TestData.Bytes(15_400, (int)(4 * mib)), Ct);
                    return true;
                }
                catch (DfsUnavailableException ex) when (IsCapacityRefusal(ex))
                {
                    return false;
                }
            }, ReclaimBudget(cluster), "space held by pins and staging becomes available after release");
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // GC-016
    // ------------------------------------------------------------------------------------------------------------

    [Fact]
    [Catalog("GC-016", "S", Inv01, Inv09, Inv10, Inv16)]
    public async Task GC_016_ReserveProtectsMetadataProgress()
    {
        const long mib = 1L << 20;
        const long capacity = 40 * mib;
        const long reserve = 12 * mib;
        await ClusterTest.RunAsync(Det(3) with { NodeCapacityBytes = capacity, ReservedBytes = reserve }, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            DfsVolume v = client.GetVolume(Volume);
            var filled = new List<string>();
            Exception? refused = null;
            for (int i = 0; i < 64 && refused is null; i++)
            {
                try
                {
                    await v.Files.WriteAllBytesAsync($"/fill{i}", TestData.Bytes(16_000 + (ulong)i, (int)mib), Ct);
                    filled.Add($"/fill{i}");
                }
                catch (DfsUnavailableException ex) when (IsCapacityRefusal(ex))
                {
                    refused = ex;
                }
            }

            Assert.True(refused is not null, "payload admission never stopped before the capacity limit");
            Assert.True(filled.Count >= 2, $"only {filled.Count} payload write(s) were admitted before the reserve limit");
            long maxObject = cluster.Options.Timings.MaxObjectSize;
            foreach (TestNode node in cluster.Nodes)
                Assert.True(node.Disk.UsedBytes <= capacity - reserve + maxObject + mib,
                    $"{node.Name}: payload admission consumed the reserve ({node.Disk.UsedBytes / mib} MiB used of {capacity / mib} MiB with a {reserve / mib} MiB reserve)");

            // Metadata keeps progressing on the reserve, including Raft checkpointing beyond the snapshot threshold.
            int metadataOps = cluster.Options.Timings.RaftSnapshotThreshold + 60;
            for (int i = 0; i < metadataOps; i++)
            {
                await v.Directories.CreateDirectoryAsync($"/meta/{i}", Ct);
                if (i % 50 == 0)
                {
                    await v.Snapshots.CreateAsync($"m{i}", null, Ct);
                    await v.Snapshots.DeleteAsync($"m{i}", Ct);
                    await v.Files.MoveAsync(filled[0], $"/moved{i}", false, Ct);
                    filled[0] = $"/moved{i}";
                }
            }

            await using (DfsTransaction tx = await v.Transactions.BeginAsync(null, Ct))
            {
                await tx.Directories.CreateDirectoryAsync("/tx-dir", Ct);
                await tx.Files.MoveAsync(filled[1], "/tx-dir/moved", false, Ct);
                await tx.CommitAsync(Ct);
            }

            filled[1] = "/tx-dir/moved";
            Assert.True(await v.Directories.ExistsAsync($"/meta/{metadataOps - 1}", Ct));
            foreach (string path in filled.Take(filled.Count / 2))
                await v.Files.DeleteAsync(path, Ct);

            await WaitAsync(cluster, async () =>
            {
                try
                {
                    await v.Files.WriteAllBytesAsync("/after", TestData.Bytes(16_500, (int)mib), Ct);
                    return true;
                }
                catch (DfsUnavailableException ex) when (IsCapacityRefusal(ex))
                {
                    return false;
                }
            }, ReclaimBudget(cluster), "payload admission resumes after deletions are reclaimed");
            foreach (TestNode node in cluster.Nodes)
                Assert.True(node.Disk.UsedBytes <= capacity - reserve + maxObject + mib, $"{node.Name} reserve after recovery");
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // GC-017
    // ------------------------------------------------------------------------------------------------------------

    [Fact]
    [Catalog("GC-017", "S", Inv01, Inv09, Inv10, Inv16)]
    public async Task GC_017_RepairReserveIsNotUserCapacity()
    {
        const long mib = 1L << 20;
        const long capacity = 48 * mib;
        const long reserve = 20 * mib;
        await ClusterTest.RunAsync(Det(4) with { NodeCapacityBytes = capacity, ReservedBytes = reserve }, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(ct: Ct);
            DfsVolume v = client.GetVolume(Volume);
            var files = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            Exception? refused = null;
            for (int i = 0; i < 80 && refused is null; i++)
            {
                byte[] data = TestData.Bytes(17_000 + (ulong)i, (int)mib);
                try
                {
                    await v.Files.WriteAllBytesAsync($"/f{i}", data, Ct);
                    files[$"/f{i}"] = data;
                }
                catch (DfsUnavailableException ex) when (IsCapacityRefusal(ex))
                {
                    refused = ex;
                }
            }

            Assert.True(refused is not null, "payload admission never stopped before the repair reserve");
            Assert.NotEmpty(files);

            // Before the failure the reserve is intact: admission stopped at capacity minus the reserve (plus at most one
            // object in flight and metadata slack), so the configured repair guarantee is real.
            long maxObject = cluster.Options.Timings.MaxObjectSize;
            foreach (TestNode node in cluster.Nodes)
                Assert.True(node.Disk.UsedBytes <= capacity - reserve + maxObject + mib,
                    $"{node.Name}: normal admission pre-consumed the repair reserve ({node.Disk.UsedBytes / mib} MiB used of {capacity / mib} MiB with a {reserve / mib} MiB reserve)");

            TestNode lost = cluster.Nodes[^1];
            await lost.CrashAsync(CrashMode.LoseUnflushed);
            await cluster.Admin.RemoveNodeAsync(lost.Name, force: true, Ct);
            await WaitAsync(cluster, async () => (await cluster.Inspect.GetReplicaHealthAsync(Volume)).UnderReplicatedObjects > 0 ||
                (await cluster.Inspect.GetTotalCountersAsync()).RepairBytes > 0, TimeSpan.FromSeconds(30), "the loss is detected as a replica deficit");

            // Normal admission cannot take the space reserved for this repair.
            DfsException duringRepair = await AssertEx.ThrowsDfsAsync<DfsException>(() => v.Files.WriteAllBytesAsync("/during-repair", TestData.Bytes(17_500, (int)mib), Ct));
            Assert.True(IsCapacityRefusal(duringRepair), $"a payload write during repair must be refused for capacity, got {duringRepair.GetType().Name} ({duringRepair.ErrorCode})");
            Assert.False(await v.Files.ExistsAsync("/during-repair", Ct));

            await cluster.WaitForRepairAsync(Volume, TimeSpan.FromMinutes(3), Ct);
            DfsReplicaHealth health = await cluster.Inspect.GetReplicaHealthAsync(Volume);
            Assert.Equal(0, health.UnderReplicatedObjects);
            Assert.Equal(0, health.ZoneDeficitObjects);
            Assert.Equal(0, health.MissingObjects);

            // The repair really ran in the reserve (the scenario exercised it), without exceeding the device.
            TestNode[] survivors = [.. cluster.Nodes.Where(n => n.Name != lost.Name)];
            Assert.Contains(survivors, n => n.Disk.UsedBytes > capacity - reserve);
            foreach (TestNode node in survivors)
                Assert.True(node.Disk.UsedBytes <= capacity, $"{node.Name} exceeds its device capacity after repair");
            foreach ((string path, byte[] data) in files.Take(6))
                AssertBytes(data, await v.Files.ReadAllBytesAsync(path, Ct), $"{path} after repair into the reserve");
            await AssertReachableHeldAsync(cluster, "after repair");
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // GC-018
    // ------------------------------------------------------------------------------------------------------------

    [Fact]
    [Catalog("GC-018", "S", Inv01, Inv09, Inv10, Inv16)]
    public async Task GC_018_DeletionEventuallyReclaims()
    {
        await ClusterTest.RunAsync(Det(3), async cluster =>
        {
            int chunk = Chunk(cluster);
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            await using DfsClient readerClient = await cluster.ConnectAsync(new TestClientOptions { Name = "c1" }, Ct);
            DfsVolume v = client.GetVolume(Volume);
            long deletedBefore = await GcDeletedAsync(cluster);

            foreach (string path in new[] { "/a", "/b", "/c" })
                await v.Files.WriteAllBytesAsync(path, TestData.Bytes(18_000 + (ulong)path[1], 3 * chunk), Ct);
            await v.Snapshots.CreateAsync("s1", null, Ct);
            await v.Files.WriteAllBytesAsync("/a", TestData.Bytes(18_010, 2 * chunk), Ct);
            await v.Snapshots.CreateAsync("s2", null, Ct);
            await v.Files.CopyAsync("/a", "/d", false, Ct);
            DfsStream reader = await readerClient.GetVolume(Volume).Files.OpenAsync("/b", PinnedReader, Ct);
            DfsStream writer = await v.Files.OpenAsync("/c", Writer, Ct);
            await writer.WriteAsync(TestData.Bytes(18_020, 3 * chunk), Ct);
            DfsTransaction tx = await v.Transactions.BeginAsync(null, Ct);
            await tx.Files.WriteAllBytesAsync("/tx", TestData.Bytes(18_030, 2 * chunk), Ct);
            await cluster.DelayAsync(TimeSpan.FromSeconds(1), Ct);
            HashSet<ObjectId> everything = await StoredAsync(cluster);

            // A faulty period: one node misses the deletions.
            cluster.Network.Isolate("n2");
            await v.Files.DeleteAsync("/a", Ct);
            await v.Files.DeleteAsync("/d", Ct);
            await v.Snapshots.DeleteAsync("s1", Ct);
            await QuiesceGcAsync(cluster, 4);
            cluster.Network.Heal();

            // Release every remaining reference.
            await writer.AbortAsync(Ct);
            await writer.DisposeAsync();
            await reader.DisposeAsync();
            await tx.AbortAsync(Ct);
            await tx.DisposeAsync();
            await v.Snapshots.DeleteAsync("s2", Ct);
            await v.Files.DeleteAsync("/b", Ct);
            await v.Files.DeleteAsync("/c", Ct);
            cluster.Faults.DisarmAll();

            await WaitAsync(cluster, async () => !(await StoredAsync(cluster)).Overlaps(everything), ReclaimBudget(cluster),
                "every released object is reclaimed within the maintenance budget once faults stop");
            await WaitNoGarbageAsync(cluster, "no unreachable object remains");
            DfsReplicaHealth health = await cluster.Inspect.GetReplicaHealthAsync(Volume);
            Assert.Equal(0, health.PendingGcObjects);
            Assert.Equal(0, health.StagedBytes);
            foreach (TestNode node in cluster.Nodes)
                Assert.Equal(0, await node.Inspect.GetStagedBytesAsync());
            Assert.True(await GcDeletedAsync(cluster) - deletedBefore >= everything.Count, "GC deletion counters do not account for the reclaimed objects");
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // GC-019
    // ------------------------------------------------------------------------------------------------------------

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    [Catalog("GC-019", "S", Inv01, Inv09, Inv10, Inv16)]
    public async Task GC_019_OrphanCleanupAfterClientCrash(bool crashLeaderToo)
    {
        await ClusterTest.RunAsync(Det(3), async cluster =>
        {
            int chunk = Chunk(cluster);
            await using DfsClient survivor = await cluster.ConnectAsync(new TestClientOptions { Name = "obs" }, Ct);
            DfsVolume sv = survivor.GetVolume(Volume);
            byte[] keep = TestData.Bytes(19_000, 3 * chunk);
            await sv.Files.WriteAllBytesAsync("/keep", keep, Ct);
            await sv.Files.WriteAllBytesAsync("/s1", TestData.Bytes(19_001, 100), Ct);
            HashSet<ObjectId> before = await StoredAsync(cluster);
            var stagingBefore = (await cluster.Inspect.GetVolumeStateAsync(Volume)).Staging.Select(s => s.Id).ToHashSet();

            DfsClient c1 = await cluster.ConnectAsync(new TestClientOptions { Name = "c1" }, Ct);
            DfsClient c2 = await cluster.ConnectAsync(new TestClientOptions { Name = "c2" }, Ct);
            DfsClient c3 = await cluster.ConnectAsync(new TestClientOptions { Name = "c3" }, Ct);
            DfsStream s1 = await c1.GetVolume(Volume).Files.OpenAsync("/s1", Writer, Ct);
            await s1.WriteAsync(TestData.Bytes(19_010, 4 * chunk), Ct);
            DfsTransaction tx = await c2.GetVolume(Volume).Transactions.BeginAsync(null, Ct);
            await tx.Files.WriteAllBytesAsync("/tx", TestData.Bytes(19_020, 3 * chunk), Ct);
            MessageHold c3Commit = cluster.Network.Hold(new MessageFilter(From: "client:c3", Type: MessageType.WriteAll));
            Task<CommitToken> c3Write = c3.GetVolume(Volume).Files.WriteAllBytesAsync("/c3", TestData.Bytes(19_030, 3 * chunk), Ct);
            await c3Commit.WaitForAsync(1, TimeSpan.FromSeconds(10));
            await WaitAsync(cluster, async () => (await StoredAsync(cluster)).Except(before).Count() >= 6, TimeSpan.FromSeconds(10), "the clients' uploads are staged");
            HashSet<ObjectId> orphans = [.. (await StoredAsync(cluster)).Except(before)];

            cluster.KillClient(c1);
            cluster.KillClient(c2);
            cluster.KillClient(c3);
            _ = c3Write;
            if (crashLeaderToo)
            {
                TestNode leader = await cluster.GetLeaderAsync(Volume, Ct);
                await leader.CrashAsync(CrashMode.LoseUnflushed);
                await leader.RestartAsync();
                await cluster.WaitForStableAsync(TimeSpan.FromSeconds(30), Ct);
            }

            // Before session expiry nothing may be reclaimed merely because the clients went silent.
            await AssertHeldAsync(cluster, orphans, 1, "uploads of clients whose sessions have not expired yet");

            await WaitAsync(cluster, async () =>
                (await cluster.Inspect.GetVolumeStateAsync(Volume)).Staging.Where(s => !stagingBefore.Contains(s.Id)).All(s => !s.Open),
                cluster.Options.Timings.SessionTimeout + TimeSpan.FromSeconds(30), "authoritative session expiry closes the dead clients' staging sessions");
            await WaitReclaimedAsync(cluster, orphans, "orphans of crashed clients are released and reclaimed");
            foreach (TestNode node in cluster.Nodes)
                Assert.Equal(0, await node.Inspect.GetStagedBytesAsync());

            // The dead client's delayed commit arrives late and is rejected.
            cluster.Network.Heal();
            c3Commit.ReleaseAll();
            c3Commit.Dispose();
            await QuiesceGcAsync(cluster, 4);
            Assert.False(await sv.Files.ExistsAsync("/c3", Ct), "a dead client's late commit published orphaned data");
            Assert.False(await sv.Files.ExistsAsync("/tx", Ct));
            AssertBytes(TestData.Bytes(19_001, 100), await sv.Files.ReadAllBytesAsync("/s1", Ct), "/s1 keeps its last committed content");
            AssertBytes(keep, await sv.Files.ReadAllBytesAsync("/keep", Ct), "/keep");
            await AssertReachableHeldAsync(cluster, "after orphan cleanup");
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // GC-020
    // ------------------------------------------------------------------------------------------------------------

    [Theory]
    [InlineData(DisconnectedReader.PinnedHandle)]
    [InlineData(DisconnectedReader.SnapshotView)]
    [Catalog("GC-020", "S", Inv01, Inv09, Inv10, Inv16)]
    public async Task GC_020_LongDisconnectedReaderIsBounded(DisconnectedReader kind)
    {
        await ClusterTest.RunAsync(Det(3), async cluster =>
        {
            int chunk = Chunk(cluster);
            await using DfsClient writerClient = await cluster.ConnectAsync(new TestClientOptions { Name = "w" }, Ct);
            DfsClient readerClient = await cluster.ConnectAsync(new TestClientOptions { Name = "r" }, Ct);
            DfsVolume w = writerClient.GetVolume(Volume);
            DfsVolume r = readerClient.GetVolume(Volume);
            byte[] v1 = TestData.Bytes(20_000, 8 * chunk);
            await w.Files.WriteAllBytesAsync("/f", v1, Ct);
            HashSet<ObjectId> v1Objects = await FileObjectsAsync(cluster, "/f");
            int pinsBefore = (await cluster.Inspect.GetVolumeStateAsync(Volume)).Pins.Count;

            DfsStream? stream = null;
            DfsSnapshotView? view = null;
            if (kind == DisconnectedReader.PinnedHandle)
            {
                stream = await r.Files.OpenAsync("/f", PinnedReader, Ct);
                AssertBytes(v1[..1024], await ReadRangeAsync(stream, 0, 1024), "first read before disconnection");
            }
            else
            {
                await w.Snapshots.CreateAsync("s1", null, Ct);
                view = await r.Snapshots.OpenAsync("s1", Ct);
            }

            Assert.True((await cluster.Inspect.GetVolumeStateAsync(Volume)).Pins.Count > pinsBefore, "the reader holds no pin");

            // The reader disappears for an unbounded time; the live volume moves on.
            cluster.Network.Isolate("client:r");
            await w.Files.WriteAllBytesAsync("/f", TestData.Bytes(20_001, 2 * chunk), Ct);
            if (view is not null)
                await w.Snapshots.DeleteAsync("s1", Ct);
            TimeSpan bound = cluster.Options.Timings.PinDuration + cluster.Options.Timings.SessionTimeout + ReclaimBudget(cluster);
            await WaitAsync(cluster, async () => !(await StoredAsync(cluster)).Overlaps(v1Objects), bound,
                "the disconnected reader's bounded pin expires and the old revision is reclaimed (no unbounded retention)");
            Assert.True((await cluster.Inspect.GetVolumeStateAsync(Volume)).Pins.Count <= pinsBefore, "the disconnected reader's pin was never released");

            // Connectivity returns: stale reads fail explicitly, never with substituted bytes.
            cluster.Network.Heal();
            Exception? stale = stream is not null
                ? await Record.ExceptionAsync(() => ReadRangeAsync(stream, 6 * chunk, 1000))
                : await Record.ExceptionAsync(() => view!.Files.ReadAllBytesAsync("/f", Ct));
            Assert.NotNull(stale);
            Assert.True(stale is DfsPinExpiredException or DfsLeaseLostException or DfsException { ErrorCode: DfsErrorCode.InvalidHandle or DfsErrorCode.SnapshotNotFound },
                $"a read through an expired pin must fail with an expiry error, got {stale.GetType().Name}: {stale.Message}");
            if (stale is DfsPinExpiredException pinExpired)
                Assert.Equal(DfsErrorCode.PinExpired, pinExpired.ErrorCode);

            try
            {
                if (stream is not null)
                    await stream.DisposeAsync();
                if (view is not null)
                    await view.DisposeAsync();
            }
            catch (DfsException)
            {
                // Releasing an already expired pin may report the expiry again.
            }

            await readerClient.DisposeAsync();
            await AssertReachableHeldAsync(cluster, "after the stale reader was bounded");
        }, Ct);
    }
}
