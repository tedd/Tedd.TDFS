using System.Text;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;
using static Tedd.Tdfs.Tests.Simulation.Ads.AdsHelpers;

namespace Tedd.Tdfs.Tests.Simulation.Ads;

/// <summary>How the ADS-019 transaction ends.</summary>
public enum TransactionEnd { Commit, Abort, CommitWithSnapshot }

/// <summary>Committed change made by another client while a transaction holds a read of <c>/c/f:s</c> (ADS-019).</summary>
public enum ConcurrentChange { OverwriteSameStream, AppendSameStream, HandleFlushSameStream, DeleteSameStream, DeleteFile, OtherFile, OtherStream }

/// <summary>How the ADS-020 snapshot boundary is captured.</summary>
public enum SnapshotCapture { Create, CommitAndSnapshot, TransactionCommitWithSnapshot }

/// <summary>What protects a deleted file's stream objects from collection (ADS-022).</summary>
public enum StreamPin { None, PinnedStreamReader, LiveStreamReaderShareDelete, SnapshotView, Transaction }

/// <summary>
/// ADS — alternate data streams: the S-level rows (FSD Addendum A §A4 ADS-02..ADS-05; catalog §A5.3). Every scenario runs
/// in deterministic simulation through the public SDK and the harness; expectations come from the test's own stream
/// models and the leader's applied state (<see cref="FileInspection.Streams"/>), checked during and after faults.
/// </summary>
public sealed class AdsSimulationTests
{
    private static readonly UTF8Encoding Utf8NoBom = new(encoderShouldEmitUTF8Identifier: false);

    // ------------------------------------------------------------------ ADS-016 PerStreamFencing

    public static TheoryData<bool, bool> FencingSchedules() => new()
    {
        { false, false },
        { true, false },
        { false, true },
        { true, true },
    };

    /// <summary>
    /// Writer A on <c>/f:s</c> stages data beyond its buffer, is paused (messages withheld, clock frozen) with a flush
    /// queued and is safely expired by the server; writer B is granted <c>/f:s</c> with a newer generation, while writers on
    /// the default stream and on <c>/f:t</c> are unaffected (the default stream's generation does not move). Variants: B
    /// committed or not before A resumes, metadata leader change before A resumes. Every publication of A's obsolete
    /// stream generation is rejected with LeaseLost and faults its stream; A's uploads are never referenced by the stream
    /// and are reclaimed once its staging is closed; the other writers keep publishing.
    /// </summary>
    [Theory]
    [MemberData(nameof(FencingSchedules))]
    [Catalog("ADS-016", "S", "INV-02", "INV-05", "INV-07", "INV-09")]
    public async Task ADS_016_PerStreamFencing(bool replacementCommitted, bool leaderChangeBeforeResume)
    {
        await ClusterTest.RunAsync(Det(), async cluster =>
        {
            TimeSpan lease = Lease(cluster);
            int chunk = Chunk(cluster);
            const string file = "/fence/f";
            const string stream = "/fence/f:s";
            const string other = "/fence/f:t";
            byte[] d0 = NonZero(1601, 4000), s0 = NonZero(1602, 5000), t0 = NonZero(1603, 3000);
            await using DfsClient setupClient = await ConnectAsync(cluster, "setup");
            DfsVolume setup = setupClient.GetVolume(Vol);
            await setup.Directories.CreateDirectoryAsync("/fence", Ct);
            await WriteModelAsync(setup.Files, file, new StreamModel(d0).With("s", s0).With("t", t0));
            FileInspection baseline = await InspectFileAsync(cluster, file);
            HashSet<StagingId> stagingBefore = (await cluster.Inspect.GetVolumeStateAsync(Vol)).Staging.Select(s => s.Id).ToHashSet();

            var clockA = new TestClock(cluster.Time);
            DfsClient clientA = await ConnectAsync(cluster, "a", clockA);
            DfsStream a = await clientA.GetVolume(Vol).Files.OpenAsync(stream, Writer(), Ct);
            ulong gA = a.WriterGeneration;
            Assert.True(gA > 0);
            HandleInspection aHandle = Assert.Single(await ActiveHandlesAsync(cluster, baseline.Inode));
            Assert.True(aHandle.IsWriter && aHandle.Generation == gA, "A's handle is the stream's writer with A's generation");
            Assert.True((await InspectFileAsync(cluster, file)).WriterGeneration == baseline.WriterGeneration,
                "a writer grant on /f:s must not advance the default stream's fencing generation");

            await a.WriteAsync(NonZero(1604, (3 * chunk) + 5), Ct);
            HashSet<StagingId> stagingA = [];
            await cluster.WaitUntilAsync(async () =>
            {
                VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Vol);
                stagingA = state.Staging.Where(s => s.Open && !stagingBefore.Contains(s.Id)).Select(s => s.Id).ToHashSet();
                return stagingA.Count > 0 && (await StoredObjectsOfStagingAsync(cluster, stagingA)).Count > 0;
            }, lease * 5, "A's writes beyond its buffer must be uploaded (unpublished) under A's own staging session", Ct);
            HashSet<ObjectId> uploadsA = await StoredObjectsOfStagingAsync(cluster, stagingA);

            MessageHold queue = cluster.Network.Hold(new MessageFilter(From: Net("a")));
            clockA.Freeze();
            Task<Exception?> staleFlush = CatchAsync(() => a.FlushAsync(Ct));
            await cluster.DelayAsync(lease / 4, Ct);
            Assert.False(staleFlush.IsCompleted, "A's flush cannot complete while every message of A is withheld");
            await cluster.WaitUntilAsync(async () => !(await ActiveHandlesAsync(cluster, baseline.Inode)).Any(h => h.Id == aHandle.Id),
                lease * 4, "the server must expire A's stream lease on its own clock while A is paused", Ct);

            await using DfsClient clientB = await ConnectAsync(cluster, "b");
            await using DfsClient clientC = await ConnectAsync(cluster, "c");
            DfsStream b = await clientB.GetVolume(Vol).Files.OpenAsync(stream, Writer(), Ct);
            ulong gB = b.WriterGeneration;
            Assert.True(gB > gA, $"the replacement writer on /f:s must get a newer stream generation ({gB} > {gA})");
            Assert.True((await InspectFileAsync(cluster, file)).WriterGeneration == baseline.WriterGeneration,
                "stream writer grants never advance the default stream's fencing generation");
            DfsStream cDefault = await clientC.GetVolume(Vol).Files.OpenAsync(file, Writer(), Ct);
            DfsStream cOther = await clientC.GetVolume(Vol).Files.OpenAsync(other, Writer(), Ct);

            byte[] expectedS = s0;
            ulong expectedRevision = (await InspectStreamAsync(cluster, file, "s")).Revision;
            if (replacementCommitted)
            {
                byte[] dataB = NonZero(1605, 7000);
                CommitToken tB = await ReplaceAndCommitAsync(b, dataB);
                expectedS = dataB;
                expectedRevision = tB.Sequence;
            }

            string? crashedLeader = null;
            if (leaderChangeBeforeResume)
            {
                TestNode oldLeader = await cluster.GetLeaderAsync(Vol, Ct);
                await oldLeader.CrashAsync();
                crashedLeader = oldLeader.Name;
                await WaitForNewLeaderAsync(cluster, oldLeader.Name, lease * 5);
            }

            async Task AssertStreamAuthorityAsync(string when, int expectedWriters = 3)
            {
                StreamInspection s = await InspectStreamAsync(cluster, file, "s");
                Assert.True(s.Revision == expectedRevision, $"{when}: /f:s revision {s.Revision}, expected {expectedRevision}");
                Assert.False(Objects(s.Extents).Overlaps(uploadsA), $"{when}: A's unpublished uploads must never be referenced by /f:s");
                BytesEqual(expectedS, await ReadCommittedAsync(setup.Files, stream), $"{when}: /f:s holds B's committed contents (or the original) only");
                HandleInspection[] writers = (await ActiveHandlesAsync(cluster, baseline.Inode)).Where(h => h.IsWriter).ToArray();
                Assert.DoesNotContain(writers, h => h.Id == aHandle.Id);
                Assert.True(writers.Length == expectedWriters, $"{when}: expected {expectedWriters} active writers (B on stream s, C on the default stream and stream t while open); found {writers.Length}");
            }

            await AssertStreamAuthorityAsync("before A resumes");

            queue.ReleaseAll();
            clockA.Unfreeze();
            Exception? flushError = await WithinAsync(cluster, staleFlush, lease * (leaderChangeBeforeResume ? 15 : 10), "A's queued flush must be answered once delivered");
            uploadsA.UnionWith(await StoredObjectsOfStagingAsync(cluster, stagingA));
            AssertLeaseLost(flushError, $"A's flush under the obsolete stream generation {gA}");
            Assert.True(a.IsFaulted, "lease loss must fault A's stream");
            Assert.True(a.Fault is DfsLeaseLostException, $"A's stream fault must be the lease loss, got {Describe(a.Fault)}");
            AssertLeaseLost(await CatchAsync(() => a.WriteAsync(new byte[] { 1, 2, 3 }, Ct).AsTask()), "write on A's faulted stream");
            AssertLeaseLost(await CatchAsync(() => a.CommitAsync(Ct)), "commit on A's faulted stream");
            AssertLeaseLost(await CatchAsync(() => a.DisposeAsync().AsTask()), "disposal must surface that A's accepted writes were never published");
            await AssertStreamAuthorityAsync("after A's queued flush and disposal were delivered");

            // The other writers were never affected and keep publishing.
            byte[] d1 = NonZero(1606, 6000), t1 = NonZero(1607, 2500), s2 = NonZero(1608, 4500);
            await ReplaceAndCommitAsync(cDefault, d1);
            await ReplaceAndCommitAsync(cOther, t1);
            CommitToken tB2 = await ReplaceAndCommitAsync(b, s2);
            expectedS = s2;
            expectedRevision = tB2.Sequence;
            await AssertStreamAuthorityAsync("after B and C published again");
            Assert.Equal(cDefault.WriterGeneration, (await InspectFileAsync(cluster, file)).WriterGeneration);
            await b.DisposeAsync();
            await cDefault.DisposeAsync();
            await cOther.DisposeAsync();
            await AssertStreamsAsync(setup.Files, file, new StreamModel(d1).With("s", s2).With("t", t1), "final contents");

            if (crashedLeader is not null)
            {
                await cluster.Node(crashedLeader).RestartAsync();
                await cluster.WaitForReplicationAsync(Vol, Ct);
                await cluster.Inspect.AssertReplicasConvergedAsync(Vol);
            }

            await cluster.Inspect.AssertNoSplitBrainAsync();
            await cluster.WaitUntilAsync(async () =>
            {
                VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Vol);
                bool closed = state.Staging.Where(s => stagingA.Contains(s.Id)).All(s => !s.Open);
                return closed && (await StoredObjectsOfStagingAsync(cluster, stagingA)).Count == 0;
            }, TimeSpan.FromSeconds(60), "A's abandoned staging must be closed and its unpublished uploads garbage-collected", Ct);
            await AssertStreamAuthorityAsync("after A's orphan uploads were reclaimed", expectedWriters: 0);
            await clientA.DisposeAsync();
        }, Ct);
    }

    /// <summary>
    /// Fencing generations are per stream: while A holds the writer reservation of <c>/f:s</c>, repeated writer grants
    /// and publications on <c>/f:t</c> and on the default stream (and a metadata leader change) never fence A — A keeps
    /// its generation and publishes — while a second writer on <c>/f:s</c> is refused.
    /// </summary>
    [Fact]
    [Catalog("ADS-016", "S", "INV-02", "INV-05", "INV-07")]
    public async Task ADS_016_PerStreamFencing_OtherStreamGrantsDoNotFence()
    {
        await ClusterTest.RunAsync(Det(), async cluster =>
        {
            const string file = "/g/f";
            await using DfsClient setupClient = await ConnectAsync(cluster, "setup");
            await using DfsClient clientA = await ConnectAsync(cluster, "a");
            await using DfsClient clientB = await ConnectAsync(cluster, "b");
            DfsVolume setup = setupClient.GetVolume(Vol);
            await setup.Directories.CreateDirectoryAsync("/g", Ct);
            await WriteModelAsync(setup.Files, file, new StreamModel(NonZero(1611, 3000)).With("s", NonZero(1612, 2000)).With("t", NonZero(1613, 1000)));

            DfsStream a = await clientA.GetVolume(Vol).Files.OpenAsync(file + ":s", Writer(), Ct);
            ulong gA = a.WriterGeneration;
            byte[] latest = NonZero(1614, 2500);
            await ReplaceAndCommitAsync(a, latest);
            byte[] defaultBytes = [];
            byte[] tBytes = [];
            for (int round = 0; round < 3; round++)
            {
                foreach (string path in new[] { file, file + ":t" })
                {
                    byte[] data = NonZero(1620 + (ulong)(round * 2) + (path == file ? 0UL : 1UL), 1500 + round);
                    await using (DfsStream w = await clientB.GetVolume(Vol).Files.OpenAsync(path, Writer(), Ct))
                        await ReplaceAndCommitAsync(w, data);
                    if (path == file)
                        defaultBytes = data;
                    else
                        tBytes = data;
                }

                AssertSharingViolation(await CatchAsync(async () => { await using DfsStream x = await clientB.GetVolume(Vol).Files.OpenAsync(file + ":s", Writer(), Ct); }),
                    $"round {round}: a second writer on /g/f:s while A holds it");
                latest = NonZero(1630 + (ulong)round, 2000 + round);
                CommitToken token = await ReplaceAndCommitAsync(a, latest);
                Assert.True(token.Sequence == (await InspectStreamAsync(cluster, file, "s")).Revision, $"round {round}: A's publication is the stream's current revision");
                Assert.Equal(gA, a.WriterGeneration);
                Assert.False(a.IsFaulted, $"round {round}: grants on other streams must not fence A");

                if (round == 1)
                {
                    TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
                    await leader.CrashAsync();
                    await WaitForNewLeaderAsync(cluster, leader.Name, Lease(cluster) * 5);
                    await leader.RestartAsync();
                }
            }

            await a.DisposeAsync();
            await AssertStreamsAsync(setup.Files, file, new StreamModel(defaultBytes).With("s", latest).With("t", tBytes), "after interleaved writers on three streams");
            await cluster.WaitForReplicationAsync(Vol, Ct);
            await cluster.Inspect.AssertReplicasConvergedAsync(Vol);
            await cluster.Inspect.AssertNoSplitBrainAsync();
        }, Ct);
    }

    // ------------------------------------------------------------------ ADS-019 StreamsInTransactions

    /// <summary>
    /// One transaction overwrites <c>/tx/f</c>, rewrites <c>/tx/f:s</c> through a transaction-bound stream (whose own
    /// CommitAsync is refused), creates <c>/tx/f:t</c>, deletes <c>/tx/f:u</c> and creates <c>/tx/g:x</c> (and thereby
    /// <c>/tx/g</c>). Inside, reads see these writes; outside, nothing is visible before the end, and a snapshot taken
    /// meanwhile holds the complete old state. Commit (also with snapshot) publishes everything in one decision — every
    /// changed stream carries the commit index; abort publishes nothing.
    /// </summary>
    [Theory]
    [InlineData(TransactionEnd.Commit)]
    [InlineData(TransactionEnd.Abort)]
    [InlineData(TransactionEnd.CommitWithSnapshot)]
    [Catalog("ADS-019", "S", "INV-02", "INV-04", "INV-07")]
    public async Task ADS_019_StreamsInTransactions(TransactionEnd end)
    {
        await ClusterTest.RunAsync(Det(), async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "tx");
            await using DfsClient observerClient = await ConnectAsync(cluster, "obs");
            DfsVolume v = client.GetVolume(Vol);
            DfsVolume ov = observerClient.GetVolume(Vol);
            await v.Directories.CreateDirectoryAsync("/tx", Ct);
            StreamModel before = new StreamModel(NonZero(1901, 5000)).With("s", NonZero(1902, (2 * Chunk(cluster)) + 7)).With("u", NonZero(1903, 300));
            await WriteModelAsync(v.Files, "/tx/f", before);
            byte[] d1 = NonZero(1904, 6000), s1 = NonZero(1905, 4000), t1 = NonZero(1906, 700), x1 = NonZero(1907, 900);
            StreamModel afterF = new StreamModel(d1).With("s", s1).With("t", t1);
            StreamModel afterG = new StreamModel([]).With("x", x1);

            await using DfsTransaction tx = await v.Transactions.BeginAsync(null, Ct);
            await tx.Files.WriteAllBytesAsync("/tx/f", d1, Ct);
            await using (DfsStream ts = await tx.Files.OpenAsync("/tx/f:s", Writer(), Ct))
            {
                Assert.True(ts.IsTransactionBound);
                await ReplaceStagedAsync(ts, s1);
                await ts.FlushAsync(Ct);
                await Assert.ThrowsAsync<InvalidOperationException>(() => ts.CommitAsync(Ct));
            }

            await tx.Files.WriteAllBytesAsync("/tx/f:t", t1, Ct);
            await tx.Files.DeleteAsync("/tx/f:u", Ct);
            await tx.Files.WriteAllBytesAsync("/tx/g:x", x1, Ct);

            await AssertStreamsAsync(tx.Files, "/tx/f", afterF, "inside the transaction (own writes)");
            await AssertStreamsAsync(tx.Files, "/tx/g", afterG, "inside the transaction (created through a stream)");
            Assert.False(await tx.Files.ExistsAsync("/tx/f:u", Ct), "the transaction sees its own stream deletion");
            await AssertStreamsAsync(ov.Files, "/tx/f", before, "outside, before the transaction ends (no dirty reads)");
            Assert.False(await ov.Files.ExistsAsync("/tx/g", Ct));
            Assert.False(await ov.Files.ExistsAsync("/tx/g:x", Ct));
            await ov.Snapshots.CreateAsync("mid", null, Ct);

            CommitToken? token = null;
            switch (end)
            {
                case TransactionEnd.Commit:
                    token = await tx.CommitAsync(Ct);
                    break;
                case TransactionEnd.Abort:
                    await tx.AbortAsync(Ct);
                    break;
                default:
                    token = (await tx.CommitWithSnapshotAsync("txsnap", Ct)).Boundary;
                    break;
            }

            if (token is null)
            {
                Assert.Equal(DfsTransactionState.Aborted, tx.State);
                await AssertStreamsAsync(ov.Files, "/tx/f", before, "after abort");
                Assert.False(await ov.Files.ExistsAsync("/tx/g", Ct));
                Assert.False(await ov.Files.ExistsAsync("/tx/f:t", Ct));
            }
            else
            {
                ulong index = token.Value.Sequence;
                Assert.Equal(DfsTransactionState.Committed, tx.State);
                await AssertStreamsAsync(ov.Files, "/tx/f", afterF, "after commit");
                await AssertStreamsAsync(ov.Files, "/tx/g", afterG, "after commit");
                Assert.False(await ov.Files.ExistsAsync("/tx/f:u", Ct));
                Assert.Equal(index, (await ov.Files.GetInfoAsync("/tx/f", Ct)).Revision);
                Assert.All(await ov.Files.ListStreamsAsync("/tx/f", Ct), s => Assert.True(s.Revision == index,
                    $"/tx/f:{s.Name} has revision {s.Revision}; every stream written by the transaction is published at its commit {index}"));
                Assert.Equal(index, (await ov.Files.GetInfoAsync("/tx/g", Ct)).Revision);
                Assert.Equal(index, (await ov.Files.GetInfoAsync("/tx/g:x", Ct)).Revision);
                if (end == TransactionEnd.CommitWithSnapshot)
                {
                    await using DfsSnapshotView snap = await ov.Snapshots.OpenAsync("txsnap", Ct);
                    Assert.Equal(token.Value, snap.Info.Boundary);
                    await AssertStreamsAsync(snap.Files, "/tx/f", afterF, "commit-and-snapshot captured the committed streams");
                    await AssertStreamsAsync(snap.Files, "/tx/g", afterG, "commit-and-snapshot captured the created file");
                }
            }

            await using (DfsSnapshotView mid = await ov.Snapshots.OpenAsync("mid", Ct))
            {
                await AssertStreamsAsync(mid.Files, "/tx/f", before, "a snapshot taken while the transaction staged holds the old state");
                Assert.False(await mid.Files.ExistsAsync("/tx/g", Ct));
            }
        }, Ct);
    }

    private static async Task ReplaceStagedAsync(DfsStream stream, byte[] data)
    {
        stream.Seek(0, SeekOrigin.Begin);
        await stream.WriteAsync(data, Ct);
        await stream.SetLengthAsync(data.Length, Ct);
    }

    public static TheoryData<ConcurrentChange> AllConcurrentChanges()
    {
        var data = new TheoryData<ConcurrentChange>();
        foreach (ConcurrentChange change in Enum.GetValues<ConcurrentChange>())
            data.Add(change);
        return data;
    }

    /// <summary>
    /// A transaction that read <c>/c/f:s</c> conflicts at commit with every committed change of that stream (overwrite,
    /// append, handle flush, stream delete, file delete): TransactionConflict carrying its id, no effect of its writes, an
    /// Aborted outcome, and the concurrent change intact. Controls: a change to another file never conflicts; a change to
    /// another stream of the same file may or may not conflict, but either outcome is all-or-nothing with no lost update.
    /// </summary>
    [Theory]
    [MemberData(nameof(AllConcurrentChanges))]
    [Catalog("ADS-019", "S", "INV-02", "INV-04", "INV-07")]
    public async Task ADS_019_StreamsInTransactions_ReadSetConflict(ConcurrentChange change)
    {
        await ClusterTest.RunAsync(Det(), async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "tx");
            await using DfsClient otherClient = await ConnectAsync(cluster, "other");
            DfsVolume v = client.GetVolume(Vol);
            DfsVolume ov = otherClient.GetVolume(Vol);
            await v.Directories.CreateDirectoryAsync("/c", Ct);
            byte[] d0 = NonZero(1911, 2000), s0 = NonZero(1912, 3000), t0 = NonZero(1913, 400), h0 = NonZero(1914, 50), output = NonZero(1915, 600);
            await WriteModelAsync(v.Files, "/c/f", new StreamModel(d0).With("s", s0).With("t", t0));
            await v.Files.WriteAllBytesAsync("/c/h", h0, Ct);

            await using DfsTransaction tx = await v.Transactions.BeginAsync(null, Ct);
            BytesEqual(s0, await tx.Files.ReadAllBytesAsync("/c/f:s", Ct), "the transaction reads the stream");
            await tx.Files.WriteAllBytesAsync("/c/out", output, Ct);

            byte[] changed = NonZero(1916, 1234);
            switch (change)
            {
                case ConcurrentChange.OverwriteSameStream:
                    await ov.Files.WriteAllBytesAsync("/c/f:s", changed, Ct);
                    break;
                case ConcurrentChange.AppendSameStream:
                    await ov.Files.AppendAllBytesAsync("/c/f:s", changed, Ct);
                    changed = [.. s0, .. changed];
                    break;
                case ConcurrentChange.HandleFlushSameStream:
                    await using (DfsStream w = await ov.Files.OpenAsync("/c/f:s", Writer(), Ct))
                    {
                        w.Seek(10, SeekOrigin.Begin);
                        await w.WriteAsync(changed.AsMemory(0, 100), Ct);
                        await w.FlushAsync(Ct);
                    }

                    byte[] patched = s0.ToArray();
                    changed.AsSpan(0, 100).CopyTo(patched.AsSpan(10));
                    changed = patched;
                    break;
                case ConcurrentChange.DeleteSameStream:
                    await ov.Files.DeleteAsync("/c/f:s", Ct);
                    break;
                case ConcurrentChange.DeleteFile:
                    await ov.Files.DeleteAsync("/c/f", Ct);
                    break;
                case ConcurrentChange.OtherFile:
                    await ov.Files.WriteAllBytesAsync("/c/h", changed, Ct);
                    break;
                default:
                    await ov.Files.WriteAllBytesAsync("/c/f:t", changed, Ct);
                    break;
            }

            Exception? commitError = await CatchAsync(() => tx.CommitAsync(Ct));
            bool committed = commitError is null;
            switch (change)
            {
                case ConcurrentChange.OtherFile:
                    Assert.True(committed, $"a change to another file must not conflict: {Describe(commitError)}");
                    break;
                case ConcurrentChange.OtherStream:
                    if (!committed)
                        AssertTransactionConflict(commitError, tx.Id, "a change to another stream of the read file");
                    break;
                default:
                    AssertTransactionConflict(commitError, tx.Id, $"{change} after the transaction read /c/f:s");
                    break;
            }

            if (committed)
            {
                Assert.Equal(DfsTransactionState.Committed, tx.State);
                BytesEqual(output, await ov.Files.ReadAllBytesAsync("/c/out", Ct), "the committed transaction's write");
            }
            else
            {
                Assert.Equal(DfsTransactionState.Aborted, tx.State);
                Assert.False(await ov.Files.ExistsAsync("/c/out", Ct), "a conflicting transaction has no effect");
                Assert.Equal(DfsOutcomeStatus.Aborted, (await v.Transactions.QueryOutcomeAsync(tx.Id, false, Ct)).Status);
            }

            // The concurrent change is intact (never rolled back by the transaction).
            switch (change)
            {
                case ConcurrentChange.DeleteSameStream:
                    Assert.False(await ov.Files.ExistsAsync("/c/f:s", Ct));
                    BytesEqual(t0, await ov.Files.ReadAllBytesAsync("/c/f:t", Ct), "sibling stream");
                    break;
                case ConcurrentChange.DeleteFile:
                    Assert.False(await ov.Files.ExistsAsync("/c/f", Ct));
                    Assert.False(await ov.Files.ExistsAsync("/c/f:s", Ct));
                    break;
                case ConcurrentChange.OtherFile:
                    BytesEqual(changed, await ov.Files.ReadAllBytesAsync("/c/h", Ct), "the other file's committed change");
                    BytesEqual(s0, await ov.Files.ReadAllBytesAsync("/c/f:s", Ct), "the read stream is unchanged");
                    break;
                case ConcurrentChange.OtherStream:
                    BytesEqual(changed, await ov.Files.ReadAllBytesAsync("/c/f:t", Ct), "the other stream's committed change (no lost update)");
                    BytesEqual(s0, await ov.Files.ReadAllBytesAsync("/c/f:s", Ct), "the read stream is unchanged");
                    break;
                default:
                    BytesEqual(changed, await ov.Files.ReadAllBytesAsync("/c/f:s", Ct), "the concurrent change of the read stream");
                    break;
            }
        }, Ct);
    }

    /// <summary>
    /// Write skew across two streams of one file (invariant a + b ≥ 1, both 1): two transactions each read both streams
    /// and zero their own; the second committer conflicts, the invariant holds, and the history (including a final
    /// read-only transaction) is strictly serializable per <see cref="SerializabilityChecker"/>.
    /// </summary>
    [Fact]
    [Catalog("ADS-019", "S", "INV-02", "INV-04", "INV-07")]
    public async Task ADS_019_StreamsInTransactions_WriteSkewAcrossStreams()
    {
        await ClusterTest.RunAsync(Det(), async cluster =>
        {
            await using DfsClient c1 = await ConnectAsync(cluster, "t1");
            await using DfsClient c2 = await ConnectAsync(cluster, "t2");
            DfsVolume v1 = c1.GetVolume(Vol);
            DfsVolume v2 = c2.GetVolume(Vol);
            const string a = "/w:a";
            const string b = "/w:b";
            await v1.Files.WriteAllBytesAsync("/w", NonZero(1921, 10), Ct);
            await v1.Files.WriteAllTextAsync(a, "1", Utf8NoBom, Ct);
            await v1.Files.WriteAllTextAsync(b, "1", Utf8NoBom, Ct);

            long begin1 = cluster.Time.GetTimestamp();
            await using DfsTransaction t1 = await v1.Transactions.BeginAsync(null, Ct);
            long begin2 = cluster.Time.GetTimestamp();
            await using DfsTransaction t2 = await v2.Transactions.BeginAsync(null, Ct);
            string a1 = await t1.Files.ReadAllTextAsync(a, Utf8NoBom, Ct), b1 = await t1.Files.ReadAllTextAsync(b, Utf8NoBom, Ct);
            string a2 = await t2.Files.ReadAllTextAsync(a, Utf8NoBom, Ct), b2 = await t2.Files.ReadAllTextAsync(b, Utf8NoBom, Ct);
            Assert.Equal(("1", "1", "1", "1"), (a1, b1, a2, b2));
            await t1.Files.WriteAllTextAsync(a, "0", Utf8NoBom, Ct);
            await t2.Files.WriteAllTextAsync(b, "0", Utf8NoBom, Ct);
            Exception? e1 = await CatchAsync(() => t1.CommitAsync(Ct));
            long end1 = cluster.Time.GetTimestamp();
            Exception? e2 = await CatchAsync(() => t2.CommitAsync(Ct));
            long end2 = cluster.Time.GetTimestamp();
            Assert.True(e1 is null, $"the first committer has nothing to conflict with: {Describe(e1)}");
            AssertTransactionConflict(e2, t2.Id, "the second transaction read a stream the first one changed (write skew)");

            long begin3 = cluster.Time.GetTimestamp();
            string finalA, finalB;
            await using (DfsTransaction t3 = await v1.Transactions.BeginAsync(null, Ct))
            {
                finalA = await t3.Files.ReadAllTextAsync(a, Utf8NoBom, Ct);
                finalB = await t3.Files.ReadAllTextAsync(b, Utf8NoBom, Ct);
                await t3.CommitAsync(Ct);
            }

            long end3 = cluster.Time.GetTimestamp();
            Assert.Equal(("0", "1"), (finalA, finalB));
            Assert.True(int.Parse(finalA, System.Globalization.CultureInfo.InvariantCulture) + int.Parse(finalB, System.Globalization.CultureInfo.InvariantCulture) >= 1,
                "the joint invariant a + b >= 1 holds");

            var history = new List<TxRecord>
            {
                new("T1", begin1, end1, OpOutcome.Ok, new Dictionary<string, string?> { [a] = a1, [b] = b1 }, new Dictionary<string, string?> { [a] = "0" }, []),
                new("T2", begin2, end2, OpOutcome.Failed, new Dictionary<string, string?> { [a] = a2, [b] = b2 }, new Dictionary<string, string?> { [b] = "0" }, []),
                new("T3", begin3, end3, OpOutcome.Ok, new Dictionary<string, string?> { [a] = finalA, [b] = finalB }, new Dictionary<string, string?>(), []),
            };
            SerializabilityChecker.CheckStrict(history, new Dictionary<string, string?> { [a] = "1", [b] = "1" }).AssertValid();
        }, Ct);
    }

    // ------------------------------------------------------------------ ADS-020 SnapshotIncludesStreams

    /// <summary>
    /// A snapshot (plain, commit-and-snapshot, transaction commit-with-snapshot) preserves every stream at its boundary —
    /// default and named streams of files, a file's stream set, directory streams — after later overwrites, appends,
    /// stream deletes/additions and the deletion of a whole file, also after GC passes (its objects stay held). The view
    /// is read-only for every stream mutation, lists no stream as an entry, and restoring it into a new volume yields the
    /// same streams, independent of the source.
    /// </summary>
    [Theory]
    [InlineData(SnapshotCapture.Create)]
    [InlineData(SnapshotCapture.CommitAndSnapshot)]
    [InlineData(SnapshotCapture.TransactionCommitWithSnapshot)]
    [Catalog("ADS-020", "S", "INV-02", "INV-07", "INV-09", "INV-10")]
    public async Task ADS_020_SnapshotIncludesStreams(SnapshotCapture capture)
    {
        await ClusterTest.RunAsync(Det(), async cluster =>
        {
            int chunk = Chunk(cluster);
            await using DfsClient client = await ConnectAsync(cluster, "c0");
            await using DfsClient observerClient = await ConnectAsync(cluster, "obs");
            DfsVolume v = client.GetVolume(Vol);
            DfsVolume ov = observerClient.GetVolume(Vol);
            await v.Directories.CreateDirectoryAsync("/s/d", Ct);
            StreamModel f0 = new StreamModel(NonZero(2001, (2 * chunk) + 1)).With("a", NonZero(2002, 500)).With("b", NonZero(2003, (3 * chunk) + 7)).With("c", []);
            StreamModel g0 = new StreamModel(NonZero(2004, 100)).With("z", NonZero(2005, chunk + 3));
            byte[] x0 = NonZero(2006, 250);
            await WriteModelAsync(v.Files, "/s/f", f0);
            await WriteModelAsync(v.Files, "/s/g", g0);
            await v.Files.WriteAllBytesAsync("/s/d:x", x0, Ct);

            switch (capture)
            {
                case SnapshotCapture.Create:
                    await v.Snapshots.CreateAsync("s1", null, Ct);
                    break;
                case SnapshotCapture.CommitAndSnapshot:
                {
                    byte[] a1 = NonZero(2007, chunk + 11);
                    await v.Files.WriteAllBytesAsync("/s/f:a", a1, new DfsMutationOptions { CommitSnapshotName = "s1" }, Ct);
                    f0.NamedStreams["a"] = a1;
                    break;
                }

                default:
                {
                    byte[] c1 = NonZero(2008, 333), g1 = NonZero(2009, 444);
                    await using DfsTransaction tx = await v.Transactions.BeginAsync(null, Ct);
                    await tx.Files.WriteAllBytesAsync("/s/f:c", c1, Ct);
                    await tx.Files.WriteAllBytesAsync("/s/g", g1, Ct);
                    await tx.CommitWithSnapshotAsync("s1", Ct);
                    f0.NamedStreams["c"] = c1;
                    g0.Default = g1;
                    break;
                }
            }

            HashSet<ObjectId> captured = AllObjects(await InspectFileAsync(cluster, "/s/f"));
            captured.UnionWith(AllObjects(await InspectFileAsync(cluster, "/s/g")));

            // Later changes.
            StreamModel f1 = f0.Clone();
            f1.NamedStreams["a"] = NonZero(2010, 900);
            await v.Files.WriteAllBytesAsync("/s/f:a", f1.NamedStreams["a"], Ct);
            byte[] extra = NonZero(2011, chunk);
            await v.Files.AppendAllBytesAsync("/s/f:b", extra, Ct);
            f1.NamedStreams["b"] = [.. f0.NamedStreams["b"], .. extra];
            await v.Files.DeleteAsync("/s/f:c", Ct);
            f1.NamedStreams.Remove("c");
            f1.With("new", NonZero(2012, 77));
            await v.Files.WriteAllBytesAsync("/s/f:new", f1.NamedStreams["new"], Ct);
            f1.Default = NonZero(2013, 3000);
            await v.Files.WriteAllBytesAsync("/s/f", f1.Default, Ct);
            await v.Files.DeleteAsync("/s/g", Ct);
            await v.Files.DeleteAsync("/s/d:x", Ct);
            byte[] y = NonZero(2014, 66);
            await v.Files.WriteAllBytesAsync("/s/d:y", y, Ct);
            await QuiesceGcAsync(cluster, 20);
            await AssertHeldAsync(cluster, captured, AckCopies(cluster), "objects of streams captured by snapshot s1");

            await using (DfsSnapshotView view = await ov.Snapshots.OpenAsync("s1", Ct))
            {
                await AssertStreamsAsync(view.Files, "/s/f", f0, "snapshot s1: /s/f");
                await AssertStreamsAsync(view.Files, "/s/g", g0, "snapshot s1: the later-deleted /s/g");
                await AssertDirectoryStreamsAsync(view.Files, "/s/d", new SortedDictionary<string, byte[]>(StringComparer.Ordinal) { ["x"] = x0 }, "snapshot s1: /s/d");
                Assert.False(await view.Files.ExistsAsync("/s/f:new", Ct));
                Assert.True(await view.Files.ExistsAsync("/s/f:c", Ct));
                DfsFileInfo info = await view.Files.GetInfoAsync("/s/f:b", Ct);
                Assert.True(info.IsStream);
                Assert.Equal(f0.NamedStreams["b"].Length, info.Length);
                Assert.Equal(new[] { "d", "f", "g" }, await NamesAsync(view.Directories, "/s"));

                Assert.True(view.Files.IsReadOnly);
                (string What, Func<Task> Action)[] mutations =
                [
                    ("WriteAllBytes(/s/f:a)", () => view.Files.WriteAllBytesAsync("/s/f:a", y, Ct)),
                    ("WriteAllBytes(/s/f:q) (new stream)", () => view.Files.WriteAllBytesAsync("/s/f:q", y, Ct)),
                    ("AppendAllBytes(/s/f:b)", () => view.Files.AppendAllBytesAsync("/s/f:b", y, Ct)),
                    ("Delete(/s/f:a)", () => view.Files.DeleteAsync("/s/f:a", Ct)),
                    ("Open(/s/f:a, write)", async () => { await using DfsStream s = await view.Files.OpenAsync("/s/f:a", Writer(), Ct); }),
                    ("Open(/s/f:q, CreateNew)", async () => { await using DfsStream s = await view.Files.OpenAsync("/s/f:q", Writer(FileMode.CreateNew, FileAccess.Write), Ct); }),
                    ("Copy(/s/g -> /s/f:a, overwrite)", () => view.Files.CopyAsync("/s/g", "/s/f:a", true, Ct)),
                    ("WriteAllBytes(/s/d:x)", () => view.Files.WriteAllBytesAsync("/s/d:x", y, Ct)),
                ];
                foreach ((string what, Func<Task> action) in mutations)
                    await AssertEx.ThrowsDfsAsync<DfsReadOnlyException>(action, DfsErrorCode.ReadOnly);
                await AssertStreamsAsync(view.Files, "/s/f", f0, "snapshot s1 after refused mutations");
            }

            await AssertStreamsAsync(ov.Files, "/s/f", f1, "the live file reflects the later changes");
            Assert.False(await ov.Files.ExistsAsync("/s/g", Ct));
            await AssertDirectoryStreamsAsync(ov.Files, "/s/d", new SortedDictionary<string, byte[]>(StringComparer.Ordinal) { ["y"] = y }, "live /s/d");

            DfsVolumeInfo restored = await v.Snapshots.RestoreAsync("s1", "vol-r", Ct);
            Assert.Equal("vol-r", restored.Name);
            DfsVolume r = client.GetVolume("vol-r");
            await AssertStreamsAsync(r.Files, "/s/f", f0, "restored volume: /s/f");
            await AssertStreamsAsync(r.Files, "/s/g", g0, "restored volume: /s/g");
            await AssertDirectoryStreamsAsync(r.Files, "/s/d", new SortedDictionary<string, byte[]>(StringComparer.Ordinal) { ["x"] = x0 }, "restored volume: /s/d");
            await r.Files.WriteAllBytesAsync("/s/f:a", NonZero(2015, 10), Ct);
            await AssertStreamsAsync(ov.Files, "/s/f", f1, "writing the restored volume does not touch the source");
        }, Ct);
    }

    // ------------------------------------------------------------------ ADS-021 BackupRestoreIncludesStreams

    private sealed class StreamTree
    {
        public SortedDictionary<string, StreamModel> Files { get; } = new(StringComparer.Ordinal);

        public SortedDictionary<string, SortedDictionary<string, byte[]>> DirectoryStreams { get; } = new(StringComparer.Ordinal);

        public StreamTree Clone()
        {
            var copy = new StreamTree();
            foreach ((string path, StreamModel model) in Files)
                copy.Files[path] = model.Clone();
            foreach ((string path, SortedDictionary<string, byte[]> streams) in DirectoryStreams)
                copy.DirectoryStreams[path] = new SortedDictionary<string, byte[]>(streams.ToDictionary(s => s.Key, s => s.Value.ToArray()), StringComparer.Ordinal);
            return copy;
        }
    }

    private static async Task AssertTreeAsync(DfsFileClient files, DfsDirectoryClient directories, StreamTree expected, string because)
    {
        string[] expectedNames = [.. expected.Files.Keys.Concat(expected.DirectoryStreams.Keys).Select(DfsPath.GetName).Order(StringComparer.Ordinal)];
        Assert.Equal(expectedNames, await NamesAsync(directories, "/b"));
        foreach ((string path, StreamModel model) in expected.Files)
            await AssertStreamsAsync(files, path, model, $"{because}: {path}");
        foreach ((string path, SortedDictionary<string, byte[]> streams) in expected.DirectoryStreams)
            await AssertDirectoryStreamsAsync(files, path, streams, $"{because}: {path}");
    }

    /// <summary>
    /// Backups carry streams: an exact backup of a snapshot holds every stream object and serves every stream (default,
    /// inline, multi-extent, empty, multibyte-named, a file created through a stream, directory streams) byte-exactly
    /// through the read-only backup host; an incremental snapshot with stream overwrites/deletes/additions is reproduced
    /// while the older one is kept; after an optional backup-node crash/restart, restoring either snapshot from the
    /// backup into a new live volume reproduces every stream, and the restored volume is independent of the source.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    [Catalog("ADS-021", "S", "INV-02", "INV-07", "INV-10", "INV-13")]
    public async Task ADS_021_BackupRestoreIncludesStreams(bool restartBackupBeforeRestore)
    {
        await ClusterTest.RunAsync(Det(backups: 1), async cluster =>
        {
            int chunk = Chunk(cluster);
            TestBackupNode b0 = cluster.BackupNodes[0];
            await b0.RegisterAsync(Vol, new DfsBackupSchedule(Interval: null, OnEverySnapshot: false), new DfsBackupBudgets(TimeSpan.FromHours(1), 1L << 30));
            await using DfsClient client = await ConnectAsync(cluster, "c0");
            DfsVolume v = client.GetVolume(Vol);
            VolumeId sourceId = (await v.GetInfoAsync(Ct)).Id;
            await v.Directories.CreateDirectoryAsync("/b/d", Ct);

            var tree1 = new StreamTree();
            tree1.Files["/b/f"] = new StreamModel(NonZero(2101, (2 * chunk) + 1)).With("inline", NonZero(2102, 200)).With("big", NonZero(2103, (4 * chunk) + 3))
                .With("empty", []).With("é", NonZero(2104, 77));
            tree1.Files["/b/g"] = new StreamModel([]).With("only", NonZero(2105, 3000));
            tree1.Files["/b/h"] = new StreamModel(NonZero(2106, 500));
            tree1.DirectoryStreams["/b/d"] = new SortedDictionary<string, byte[]>(StringComparer.Ordinal) { ["meta"] = NonZero(2107, 500) };
            await WriteModelAsync(v.Files, "/b/f", tree1.Files["/b/f"]);
            await v.Files.WriteAllBytesAsync("/b/g:only", tree1.Files["/b/g"].NamedStreams["only"], Ct);
            await v.Files.WriteAllBytesAsync("/b/h", tree1.Files["/b/h"].Default, Ct);
            await v.Files.WriteAllBytesAsync("/b/d:meta", tree1.DirectoryStreams["/b/d"]["meta"], Ct);

            DfsSnapshotInfo s1 = await v.Snapshots.CreateAsync("s1", null, Ct);
            await v.Backups.RequestSyncAsync(b0.TargetName, "s1", Ct);
            DfsBackupCheckpoint cp1 = await v.Backups.AwaitBackupSnapshotAsync(b0.TargetName, "s1", SyncTimeout, Ct);
            Assert.Equal((s1.Id, s1.Boundary, true), (cp1.SnapshotId, cp1.CoveredCommit, cp1.IsExactSnapshot));

            HashSet<ObjectId> s1Objects = [];
            foreach (string path in tree1.Files.Keys)
                s1Objects.UnionWith(AllObjects(await InspectFileAsync(cluster, path)));
            HashSet<ObjectId> backupObjects = (await b0.Inspect.ListObjectsAsync()).Select(o => o.Id).ToHashSet();
            Assert.True(s1Objects.IsSubsetOf(backupObjects),
                $"the backup must hold every object of every stream of s1; missing [{string.Join(", ", s1Objects.Except(backupObjects))}]");

            await using DfsClient backupClient = await cluster.ConnectBackupAsync(0, new TestClientOptions { Name = "bk", Credentials = b0.ReaderCredentials }, Ct);
            await using (DfsSnapshotView view = await backupClient.GetVolume(Vol).Snapshots.OpenAsync("s1", Ct))
                await AssertTreeAsync(view.Files, view.Directories, tree1, "backup s1");

            // Incremental: overwrite a range of a large stream, delete and add streams, delete a file.
            StreamTree tree2 = tree1.Clone();
            byte[] patch = NonZero(2108, 100);
            await using (DfsStream w = await v.Files.OpenAsync("/b/f:big", Writer(), Ct))
            {
                w.Seek(chunk + 5, SeekOrigin.Begin);
                await w.WriteAsync(patch, Ct);
            }

            byte[] big2 = tree2.Files["/b/f"].NamedStreams["big"];
            patch.CopyTo(big2, chunk + 5);
            await v.Files.DeleteAsync("/b/f:inline", Ct);
            tree2.Files["/b/f"].NamedStreams.Remove("inline");
            tree2.Files["/b/f"].With("new", NonZero(2109, chunk + 1));
            await v.Files.WriteAllBytesAsync("/b/f:new", tree2.Files["/b/f"].NamedStreams["new"], Ct);
            await v.Files.DeleteAsync("/b/d:meta", Ct);
            tree2.DirectoryStreams["/b/d"] = new SortedDictionary<string, byte[]>(StringComparer.Ordinal) { ["meta2"] = NonZero(2110, 40) };
            await v.Files.WriteAllBytesAsync("/b/d:meta2", tree2.DirectoryStreams["/b/d"]["meta2"], Ct);
            await v.Files.DeleteAsync("/b/h", Ct);
            tree2.Files.Remove("/b/h");

            DfsSnapshotInfo s2 = await v.Snapshots.CreateAsync("s2", null, Ct);
            await v.Backups.RequestSyncAsync(b0.TargetName, "s2", Ct);
            DfsBackupCheckpoint cp2 = await v.Backups.AwaitBackupSnapshotAsync(b0.TargetName, "s2", SyncTimeout, Ct);
            Assert.Equal((s2.Id, s2.Boundary, true), (cp2.SnapshotId, cp2.CoveredCommit, cp2.IsExactSnapshot));
            await using (DfsSnapshotView view = await backupClient.GetVolume(Vol).Snapshots.OpenAsync("s2", Ct))
                await AssertTreeAsync(view.Files, view.Directories, tree2, "backup s2");
            await using (DfsSnapshotView view = await backupClient.GetVolume(Vol).Snapshots.OpenAsync("s1", Ct))
                await AssertTreeAsync(view.Files, view.Directories, tree1, "backup s1 after s2 was published");

            if (restartBackupBeforeRestore)
            {
                await b0.CrashAsync();
                await b0.RestartAsync();
                await using DfsClient again = await cluster.ConnectBackupAsync(0, new TestClientOptions { Name = "bk2", Credentials = b0.ReaderCredentials }, Ct);
                await using DfsSnapshotView view = await again.GetVolume(Vol).Snapshots.OpenAsync("s2", Ct);
                await AssertTreeAsync(view.Files, view.Directories, tree2, "backup s2 after the backup node restarted");
            }

            foreach ((string snapshot, string newVolume, StreamTree expected) in new[] { ("s2", "vol-r2", tree2), ("s1", "vol-r1", tree1) })
            {
                DfsVolumeInfo restored = await cluster.Admin.RestoreFromBackupAsync(
                    new DfsRestoreFromBackupRequest([b0.Endpoint], b0.ReaderCredentials, Vol, snapshot, newVolume), Ct);
                Assert.Equal(newVolume, restored.Name);
                Assert.NotEqual(sourceId, restored.Id);
                DfsVolume rv = client.GetVolume(newVolume);
                await AssertTreeAsync(rv.Files, rv.Directories, expected, $"volume {newVolume} restored from backup {snapshot}");
            }

            DfsVolume r2 = client.GetVolume("vol-r2");
            await r2.Files.WriteAllBytesAsync("/b/f:new", NonZero(2111, 5), Ct);
            await AssertStreamsAsync(v.Files, "/b/f", tree2.Files["/b/f"], "writing the restored volume does not touch the source");
            await using DfsClient finalBackupClient = await cluster.ConnectBackupAsync(0, new TestClientOptions { Name = "bk3", Credentials = b0.ReaderCredentials }, Ct);
            await using (DfsSnapshotView view = await finalBackupClient.GetVolume(Vol).Snapshots.OpenAsync("s2", Ct))
                await AssertStreamsAsync(view.Files, "/b/f", tree2.Files["/b/f"], "nor the backup");
        }, Ct);
    }

    // ------------------------------------------------------------------ ADS-022 StreamGcAfterFileDelete

    /// <summary>
    /// After a file with large named streams is deleted, its stream objects stay durably held (≥ ack copies) and readable
    /// for as long as a pin protects them — a version-pinned or Share.Delete live handle on a stream, an open view of a
    /// since-deleted snapshot, an active transaction — even across many GC passes and a one-day wall-clock jump; once the
    /// pin is released (or with no pin) every object of the file's streams is eventually reclaimed, while an unrelated
    /// file's stream objects stay held and its bytes exact.
    /// </summary>
    [Theory]
    [InlineData(StreamPin.None)]
    [InlineData(StreamPin.PinnedStreamReader)]
    [InlineData(StreamPin.LiveStreamReaderShareDelete)]
    [InlineData(StreamPin.SnapshotView)]
    [InlineData(StreamPin.Transaction)]
    [Catalog("ADS-022", "S", "INV-01", "INV-09", "INV-16")]
    public async Task ADS_022_StreamGcAfterFileDelete(StreamPin pin)
    {
        await ClusterTest.RunAsync(Det(), async cluster =>
        {
            int chunk = Chunk(cluster);
            await using DfsClient writerClient = await ConnectAsync(cluster, "w");
            await using DfsClient readerClient = await ConnectAsync(cluster, "r");
            DfsVolume w = writerClient.GetVolume(Vol);
            DfsVolume r = readerClient.GetVolume(Vol);
            await w.Directories.CreateDirectoryAsync("/g", Ct);
            StreamModel f = new StreamModel(NonZero(2201, 2 * chunk)).With("s1", NonZero(2202, (5 * chunk) + 1)).With("s2", NonZero(2203, 3 * chunk));
            StreamModel keep = new StreamModel(NonZero(2204, 1000)).With("k", NonZero(2205, 2 * chunk));
            await WriteModelAsync(w.Files, "/g/f", f);
            await WriteModelAsync(w.Files, "/keep", keep);
            FileInspection inspected = await InspectFileAsync(cluster, "/g/f");
            HashSet<ObjectId> fileObjects = AllObjects(inspected);
            HashSet<ObjectId> s1Objects = Objects(inspected.Streams.Single(s => s.Name == "s1").Extents);
            HashSet<ObjectId> keepObjects = AllObjects(await InspectFileAsync(cluster, "/keep"));
            Assert.True(s1Objects.Count >= 2, "precondition: the stream spans several objects");
            Assert.False(fileObjects.Overlaps(keepObjects));

            DfsStream? handle = null;
            DfsSnapshotView? view = null;
            DfsTransaction? tx = null;
            switch (pin)
            {
                case StreamPin.PinnedStreamReader:
                    handle = await r.Files.OpenAsync("/g/f:s1", Reader(DfsReadView.VersionPinned), Ct);
                    break;
                case StreamPin.LiveStreamReaderShareDelete:
                    handle = await r.Files.OpenAsync("/g/f:s1", Reader(), Ct);
                    break;
                case StreamPin.SnapshotView:
                    await w.Snapshots.CreateAsync("p", null, Ct);
                    view = await r.Snapshots.OpenAsync("p", Ct);
                    await w.Snapshots.DeleteAsync("p", Ct);
                    break;
                case StreamPin.Transaction:
                    tx = await r.Transactions.BeginAsync(null, Ct);
                    Assert.True(await tx.Files.ExistsAsync("/g/f:s1", Ct));
                    break;
            }

            if (handle is not null)
            {
                var first = new byte[1024];
                await handle.ReadExactlyAsync(first, Ct);
                BytesEqual(f.NamedStreams["s1"][..1024], first, $"{pin}: first read through the stream handle");
            }

            await w.Files.DeleteAsync("/g/f", Ct);
            Assert.False(await w.Files.ExistsAsync("/g/f", Ct));
            Assert.False(await w.Files.ExistsAsync("/g/f:s1", Ct));
            await QuiesceGcAsync(cluster, 30);
            if (pin != StreamPin.Transaction)
                JumpWallClocks(cluster, TimeSpan.FromDays(1)); // elapsed (wall) time alone never authorises reclamation
            await QuiesceGcAsync(cluster, 10);

            if (pin != StreamPin.None)
            {
                await AssertHeldAsync(cluster, s1Objects, AckCopies(cluster), $"{pin}: objects of a pinned stream after its file was deleted");
                switch (pin)
                {
                    case StreamPin.PinnedStreamReader or StreamPin.LiveStreamReaderShareDelete:
                        BytesEqual(f.NamedStreams["s1"], await ReadToEndAsync(handle!), $"{pin}: the deleted file's stream through the open handle");
                        await handle!.DisposeAsync();
                        break;
                    case StreamPin.SnapshotView:
                        await AssertStreamsAsync(view!.Files, "/g/f", f, $"{pin}: every stream through the open view of the deleted snapshot");
                        await view.DisposeAsync();
                        break;
                    default:
                        await AssertStreamsAsync(tx!.Files, "/g/f", f, $"{pin}: every stream at the transaction's read version");
                        await tx.AbortAsync(Ct);
                        await tx.DisposeAsync();
                        break;
                }
            }

            await WaitReclaimedAsync(cluster, fileObjects, $"{pin}: every object of the deleted file's streams is eventually reclaimed");
            Assert.False((await cluster.Inspect.GetVolumeStateAsync(Vol)).ReachableObjects().Overlaps(fileObjects), "no reclaimed stream object is still reachable");
            await AssertHeldAsync(cluster, keepObjects, AckCopies(cluster), "an unrelated file's stream objects stay held");
            await AssertStreamsAsync(r.Files, "/keep", keep, "the unrelated file is intact");
        }, Ct);
    }
}
