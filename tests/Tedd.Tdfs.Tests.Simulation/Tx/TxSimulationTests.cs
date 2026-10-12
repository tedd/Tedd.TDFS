using System.Globalization;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;
using static Tedd.Tdfs.Tests.Simulation.Tx.TxHelpers;

namespace Tedd.Tdfs.Tests.Simulation.Tx;

/// <summary>
/// TX suite, level S: isolation, strict serializability and failure behaviour of transactions (FSD §8, §11, §13;
/// test plan §9.6) in deterministic whole-cluster simulation. Every concurrent-transaction scenario records its
/// transactions (and the standalone operations and observers that interact with them) with logical real-time stamps and
/// checks them with <see cref="SerializabilityChecker.CheckStrict"/>. Conflicts are only demanded where committing would
/// make the history non-serializable (lost updates, write-skew cycles, or an observer that pins the real-time order).
/// </summary>
public sealed class TxSimulationTests
{
    // ------------------------------------------------------------------------------------------------------------
    // TX-004 NoDirtyReads
    // ------------------------------------------------------------------------------------------------------------

    public enum StagedEnding { Abort, DisposeWithoutCommit, Conflict, Commit }

    [Theory]
    [InlineData(StagedEnding.Abort)]
    [InlineData(StagedEnding.DisposeWithoutCommit)]
    [InlineData(StagedEnding.Conflict)]
    [InlineData(StagedEnding.Commit)]
    [Catalog("TX-004", "S", "INV-02", "INV-04")]
    public Task TX_004_NoDirtyReads(StagedEnding ending) =>
        ClusterTest.RunAsync(Sim, async cluster =>
        {
            await using DfsClient writerClient = await ConnectAsync(cluster, "writer");
            await using DfsClient readerClient = await ConnectAsync(cluster, "reader");
            await using DfsClient otherClient = await ConnectAsync(cluster, "other");
            DfsVolume w = writerClient.GetVolume(Vol);
            DfsVolume r = readerClient.GetVolume(Vol);
            DfsVolume o = otherClient.GetVolume(Vol);

            var labels = new ContentLabels();
            byte[] x0 = labels.Add("x0", TestData.Bytes(400, 200_000));
            byte[] x1 = labels.Add("x1", TestData.Bytes(401, 180_000));
            byte[] n1 = labels.Add("n1", TestData.Bytes(402, 150_000));
            byte[] d0 = labels.Add("d0", TestData.Bytes(403, 3000));
            byte[] r0 = labels.Add("r0", TestData.Bytes(404, 100));
            byte[] g0 = labels.Add("g0", Utf8("g0"));
            byte[] g1 = labels.Add("g1", Utf8("g1"));
            byte[] gT = labels.Add("gT", Utf8("gT"));

            await w.Directories.CreateDirectoryAsync("/dir", Ct);
            await w.Files.WriteAllBytesAsync("/x", x0, Ct);
            await w.Files.WriteAllBytesAsync("/del", d0, Ct);
            await w.Files.WriteAllBytesAsync("/r", r0, Ct);
            await w.Files.WriteAllBytesAsync("/guard", g0, Ct);
            string[] keys = ["/x", "/new", "/del", "/r", "/r2", "/guard"];
            var initial = new Dictionary<string, string?> { ["/x"] = "x0", ["/del"] = "d0", ["/r"] = "r0", ["/guard"] = "g0" };
            var history = new TxHistory(cluster.History);

            // T stages large multi-extent data (uploaded under its staging session), a create, a delete, a rename
            // and a directory creation.
            TxLog t = history.Start("T");
            DfsTransaction tx = await w.Transactions.BeginAsync(null, Ct);
            if (ending == StagedEnding.Conflict)
                t.Read("/guard", await ReadLabelAsync(tx.Files, "/guard", labels));
            await tx.Files.WriteAllBytesAsync("/x", x1, Ct);
            t.Write("/x", "x1");
            await using (DfsStream s = await tx.Files.OpenAsync("/new", FileMode.CreateNew, FileAccess.Write, FileShare.Read, Ct))
            {
                await s.WriteAsync(n1, Ct);
                await s.FlushAsync(Ct);
            }

            t.Write("/new", "n1");
            await tx.Files.DeleteAsync("/del", Ct);
            t.Write("/del", null);
            await tx.Files.MoveAsync("/r", "/r2", false, Ct);
            t.Write("/r", null).Write("/r2", "r0");
            await tx.Directories.CreateDirectoryAsync("/dir/sub", Ct);
            Assert.Equal("x1", await ReadLabelAsync(tx.Files, "/x", labels)); // non-vacuity: staged data exists

            // While staged: transactional, standalone, live-stream and pinned-stream readers see only committed state.
            var initialView = new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                ["/x"] = "x0", ["/new"] = null, ["/del"] = "d0", ["/r"] = "r0", ["/r2"] = null, ["/guard"] = "g0",
            };
            for (int round = 0; round < 3; round++)
            {
                (Dictionary<string, string?> seen, _) = await ObserveAsync(r, history, $"R{round}", keys, labels);
                AssertView(initialView, seen, $"read-only transaction R{round} while T is staged");
                await AssertStandaloneViewAsync(r, history, labels, $"S{round}", initialView);
                Assert.DoesNotContain("sub", await ListNamesAsync(r.Directories, "/dir"));
                await cluster.DelayAsync(TimeSpan.FromMilliseconds(40), Ct);
            }

            Dictionary<string, string?> finalView;
            switch (ending)
            {
                case StagedEnding.Abort:
                    await tx.AbortAsync(Ct);
                    t.Complete(OpOutcome.Failed);
                    finalView = initialView;
                    break;
                case StagedEnding.DisposeWithoutCommit:
                    await tx.DisposeAsync();
                    t.Complete(OpOutcome.Failed);
                    finalView = initialView;
                    break;
                case StagedEnding.Conflict:
                {
                    TxResult wr = await StandaloneAsync(history, "W-guard", l => l.Write("/guard", "g1"), () => o.Files.WriteAllBytesAsync("/guard", g1, Ct));
                    Assert.Equal(OpOutcome.Ok, wr.Outcome);
                    DfsTransactionConflictException? early = await StageOrEarlyConflictAsync(() => tx.Files.WriteAllBytesAsync("/guard", gT, Ct));
                    if (early is null)
                        t.Write("/guard", "gT");
                    await CommitExpectingConflictAsync(tx, t, early, "T read /guard, which a committed standalone write changed (lost update)");
                    finalView = new Dictionary<string, string?>(initialView, StringComparer.Ordinal) { ["/guard"] = "g1" };
                    break;
                }

                default:
                {
                    // Readers race the commit; every observation is wholly before or wholly after it.
                    var done = new TaskCompletionSource();
                    var racing = new List<Dictionary<string, string?>>();
                    Task readers = RaceObserversAsync(r, history, labels, keys, done.Task, racing);
                    TxResult committed = await CommitAsync(tx, t);
                    done.SetResult();
                    await readers;
                    Assert.Equal(OpOutcome.Ok, committed.Outcome);
                    finalView = new Dictionary<string, string?>(StringComparer.Ordinal)
                    {
                        ["/x"] = "x1", ["/new"] = "n1", ["/del"] = null, ["/r"] = null, ["/r2"] = "r0", ["/guard"] = "g0",
                    };
                    foreach (Dictionary<string, string?> seen in racing)
                    {
                        bool before = ViewEquals(initialView, seen);
                        bool after = ViewEquals(finalView, seen);
                        Assert.True(before || after, $"A reader racing the commit saw a mixed view: {Describe(seen)}");
                    }

                    break;
                }
            }

            await tx.DisposeAsync();
            for (int round = 0; round < 2; round++)
            {
                (Dictionary<string, string?> seen, _) = await ObserveAsync(r, history, $"F{round}", keys, labels);
                AssertView(finalView, seen, $"read-only transaction after T ended ({ending})");
                await AssertStandaloneViewAsync(r, history, labels, $"FS{round}", finalView);
            }

            bool subVisible = (await ListNamesAsync(r.Directories, "/dir")).Contains("sub");
            Assert.Equal(ending == StagedEnding.Commit, subVisible);
            history.AssertStrictlySerializable(initial);
        }, Ct);

    private static async Task RaceObserversAsync(DfsVolume volume, TxHistory history, ContentLabels labels, IReadOnlyList<string> keys,
        Task stop, List<Dictionary<string, string?>> observations)
    {
        int i = 0;
        do
        {
            (Dictionary<string, string?> seen, _) = await ObserveAsync(volume, history, $"race{i++}", keys, labels);
            observations.Add(seen);
        }
        while (!stop.IsCompleted);
    }

    private static async Task AssertStandaloneViewAsync(DfsVolume volume, TxHistory history, ContentLabels labels, string id,
        IReadOnlyDictionary<string, string?> expected)
    {
        foreach ((string key, string? value) in expected)
        {
            TxLog log = history.Start($"{id}{key}");
            string? seen = await ReadLabelAsync(volume.Files, key, labels);
            log.Read(key, seen).Complete(OpOutcome.Ok);
            Assert.True(seen == value, $"Standalone read of {key} saw {seen ?? "absent"}, expected {value ?? "absent"} ({id}).");
        }

        // A live stream and a version-pinned stream read the same committed bytes.
        if (expected.TryGetValue("/x", out string? x) && x is not null)
        {
            foreach (DfsReadView view in new[] { DfsReadView.Live, DfsReadView.VersionPinned })
            {
                await using DfsStream s = await volume.Files.OpenAsync("/x", new DfsOpenOptions { ReadView = view, Share = FileShare.ReadWrite | FileShare.Delete }, Ct);
                var buffer = new MemoryStream();
                await s.CopyToAsync(buffer, Ct);
                Assert.Equal(x, labels.Of(buffer.ToArray(), $"{view} stream read of /x"));
            }
        }
    }

    private static void AssertView(IReadOnlyDictionary<string, string?> expected, IReadOnlyDictionary<string, string?> seen, string because) =>
        Assert.True(ViewEquals(expected, seen), $"{because}: expected {Describe(expected)}, saw {Describe(seen)}");

    private static bool ViewEquals(IReadOnlyDictionary<string, string?> expected, IReadOnlyDictionary<string, string?> seen) =>
        expected.All(kv => seen.TryGetValue(kv.Key, out string? v) && v == kv.Value);

    private static string Describe(IReadOnlyDictionary<string, string?> view) =>
        "{" + string.Join(", ", view.OrderBy(kv => kv.Key, StringComparer.Ordinal).Select(kv => $"{kv.Key}={kv.Value ?? "∅"}")) + "}";

    // ------------------------------------------------------------------------------------------------------------
    // TX-005 NoLostUpdate
    // ------------------------------------------------------------------------------------------------------------

    public enum LostUpdateSchedule { ReservationBlocksSecondWriter, FirstCommitsBeforeSecondWrites, MirrorOrder }

    [Theory]
    [InlineData(LostUpdateSchedule.ReservationBlocksSecondWriter)]
    [InlineData(LostUpdateSchedule.FirstCommitsBeforeSecondWrites)]
    [InlineData(LostUpdateSchedule.MirrorOrder)]
    [Catalog("TX-005", "S", "INV-02", "INV-04")]
    public Task TX_005_NoLostUpdate(LostUpdateSchedule schedule) =>
        ClusterTest.RunAsync(Sim, async cluster =>
        {
            await using DfsClient c1 = await ConnectAsync(cluster, "c1");
            await using DfsClient c2 = await ConnectAsync(cluster, "c2");
            await using DfsClient observer = await ConnectAsync(cluster, "observer");
            DfsVolume v1 = c1.GetVolume(Vol);
            DfsVolume v2 = c2.GetVolume(Vol);
            DfsVolume obs = observer.GetVolume(Vol);
            await v1.Files.WriteAllTextAsync("/ctr", "0", null, Ct);
            var initial = new Dictionary<string, string?> { ["/ctr"] = "0" };
            var history = new TxHistory(cluster.History);
            long conflictsBefore = (await cluster.Inspect.GetTotalCountersAsync()).TransactionConflicts;

            TxLog l1 = history.Start("T1");
            DfsTransaction t1 = await v1.Transactions.BeginAsync(null, Ct);
            TxLog l2 = history.Start("T2");
            DfsTransaction t2 = await v2.Transactions.BeginAsync(null, Ct);
            l1.Read("/ctr", await ReadLabelAsync(t1.Files, "/ctr"));
            l2.Read("/ctr", await ReadLabelAsync(t2.Files, "/ctr"));

            (DfsTransaction first, TxLog firstLog, DfsTransaction second, TxLog secondLog) =
                schedule == LostUpdateSchedule.MirrorOrder ? (t2, l2, t1, l1) : (t1, l1, t2, l2);

            await first.Files.WriteAllTextAsync("/ctr", "1", null, Ct);
            firstLog.Write("/ctr", "1");
            TxResult firstResult;
            DfsTransactionConflictException? early = null;
            const string because = "both transactions read version 0 and overwrote it; the later commit would discard the first update";
            if (schedule == LostUpdateSchedule.FirstCommitsBeforeSecondWrites)
            {
                firstResult = await CommitAsync(first, firstLog);
                Assert.Equal(OpOutcome.Ok, firstResult.Outcome);
                early = await StageOrEarlyConflictAsync(() => second.Files.WriteAllTextAsync("/ctr", "1", null, Ct));
                if (early is null)
                    secondLog.Write("/ctr", "1");
            }
            else
            {
                // The second writer cannot take the writer reservation the first holds (immediate, never waiting). The
                // implementation may also abort the refused transaction outright (LOCK-019 policy): then its next write
                // reports that typed abort early.
                Exception? blocked = await CaptureAsync(() => second.Files.WriteAllTextAsync("/ctr", "1", null, Ct));
                if (blocked is DfsTransactionConflictException earlyAbort)
                {
                    early = earlyAbort;
                }
                else if (blocked is not null)
                {
                    DfsSharingViolationException sv = Assert.IsAssignableFrom<DfsSharingViolationException>(blocked);
                    Assert.True(sv.ErrorCode is DfsErrorCode.SharingViolation or DfsErrorCode.WriterConflict, $"Unexpected code {sv.ErrorCode}.");
                    Assert.True(sv.IsRetryable);
                }
                else
                {
                    secondLog.Write("/ctr", "1");
                }

                firstResult = await CommitAsync(first, firstLog);
                Assert.Equal(OpOutcome.Ok, firstResult.Outcome);
                if (blocked is DfsSharingViolationException)
                {
                    // The reservation was released by the first commit; the stale read must still prevent a lost update.
                    early = await StageOrEarlyConflictAsync(() => second.Files.WriteAllTextAsync("/ctr", "1", null, Ct));
                    if (early is null)
                        secondLog.Write("/ctr", "1");
                }
            }

            TxResult secondResult = await CommitExpectingConflictAsync(second, secondLog, early, because);
            Assert.Equal(OpOutcome.Failed, secondResult.Outcome);
            Assert.Equal("1", await ReadLabelAsync(obs.Files, "/ctr"));

            // The application (not the SDK) explicitly retries the loser: it reads the new version and increments it.
            TxRun retry = await RunTxAsync(history, "T-retry", second == t1 ? v1 : v2, async (tx, log) =>
            {
                string? current = await ReadLabelAsync(tx.Files, "/ctr");
                log.Read("/ctr", current);
                Assert.Equal("1", current);
                await tx.Files.WriteAllTextAsync("/ctr", "2", null, Ct);
                log.Write("/ctr", "2");
            });
            Assert.Equal(OpOutcome.Ok, retry.Result.Outcome);
            Assert.Equal("2", await ReadLabelAsync(obs.Files, "/ctr"));

            await t1.DisposeAsync();
            await t2.DisposeAsync();
            if (early is null)
                Assert.True((await cluster.Inspect.GetTotalCountersAsync()).TransactionConflicts > conflictsBefore, "The commit-time validation conflict is counted in node metrics.");
            history.AssertStrictlySerializable(initial);
        }, Ct);

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(5)]
    [Catalog("TX-005", "S", "INV-02", "INV-04")]
    public Task TX_005_NoLostUpdate_ConcurrentIncrementers(int workers) =>
        ClusterTest.RunAsync(Sim, async cluster =>
        {
            var clients = new List<DfsClient>();
            try
            {
                for (int i = 0; i < workers; i++)
                    clients.Add(await ConnectAsync(cluster, $"w{i}"));
                await clients[0].GetVolume(Vol).Files.WriteAllTextAsync("/ctr", "0", null, Ct);
                var history = new TxHistory(cluster.History);
                int conflicts = 0;

                async Task WorkerAsync(int index)
                {
                    DfsVolume v = clients[index].GetVolume(Vol);
                    for (int attempt = 1; ; attempt++)
                    {
                        Assert.True(attempt <= 60, $"Worker {index} could not commit its increment in 60 explicit attempts.");
                        TxLog log = history.Start($"w{index}-{attempt}");
                        DfsTransaction tx = await v.Transactions.BeginAsync(null, Ct);
                        await using (tx)
                        {
                            string? current = await ReadLabelAsync(tx.Files, "/ctr");
                            log.Read("/ctr", current);
                            string next = (int.Parse(current!, CultureInfo.InvariantCulture) + 1).ToString(CultureInfo.InvariantCulture);
                            Exception? stage = await CaptureAsync(() => tx.Files.WriteAllTextAsync("/ctr", next, null, Ct));
                            if (stage is DfsSharingViolationException or DfsTransactionConflictException)
                            {
                                // Reservation refused, or the stale read detected early: abort and retry explicitly.
                                await CaptureAsync(() => tx.AbortAsync(Ct));
                                Assert.NotEqual(DfsTransactionState.Committed, tx.State);
                                log.Complete(OpOutcome.Failed);
                                if (stage is DfsTransactionConflictException)
                                    Interlocked.Increment(ref conflicts);
                            }
                            else if (stage is not null)
                            {
                                log.Complete(HistoryRecorder.Classify(stage));
                                throw stage;
                            }
                            else
                            {
                                log.Write("/ctr", next);
                                TxResult result = await CommitAsync(tx, log);
                                if (result.Outcome == OpOutcome.Ok)
                                    return;
                                Assert.IsAssignableFrom<DfsTransactionConflictException>(result.Error);
                                Interlocked.Increment(ref conflicts);
                            }
                        }

                        await cluster.DelayAsync(TimeSpan.FromMilliseconds(15 * (index + 1) + 7 * attempt), Ct);
                    }
                }

                await Task.WhenAll(Enumerable.Range(0, workers).Select(WorkerAsync));
                Assert.Equal(workers.ToString(CultureInfo.InvariantCulture), await ReadLabelAsync(clients[0].GetVolume(Vol).Files, "/ctr"));
                TestContext.Current.TestOutputHelper?.WriteLine($"{workers} incrementers, {conflicts} validation conflicts.");
                history.AssertStrictlySerializable(new Dictionary<string, string?> { ["/ctr"] = "0" });
            }
            finally
            {
                foreach (DfsClient c in clients)
                    await c.DisposeAsync();
            }
        }, Ct);

    // ------------------------------------------------------------------------------------------------------------
    // TX-006 WriteSkewRejected (test plan §9.6)
    // ------------------------------------------------------------------------------------------------------------

    public enum CommitOrder { FirstThenSecond, SecondThenFirst, Concurrent }

    [Theory]
    [InlineData(CommitOrder.FirstThenSecond)]
    [InlineData(CommitOrder.SecondThenFirst)]
    [InlineData(CommitOrder.Concurrent)]
    [Catalog("TX-006", "S", "INV-04")]
    public Task TX_006_WriteSkewRejected(CommitOrder order) =>
        ClusterTest.RunAsync(Sim, async cluster =>
        {
            (DfsClient c1, DfsClient c2, DfsClient co) = (await ConnectAsync(cluster, "c1"), await ConnectAsync(cluster, "c2"), await ConnectAsync(cluster, "observer"));
            await using (c1)
            await using (c2)
            await using (co)
            {
                DfsVolume v1 = c1.GetVolume(Vol), v2 = c2.GetVolume(Vol), obs = co.GetVolume(Vol);
                await v1.Files.WriteAllTextAsync("/a", "1", null, Ct);
                await v1.Files.WriteAllTextAsync("/b", "1", null, Ct);
                var initial = new Dictionary<string, string?> { ["/a"] = "1", ["/b"] = "1" };
                var history = new TxHistory(cluster.History);

                // Invariant (application): at least one of /a, /b stays "1". Each transaction checks it on its own reads.
                async Task<(DfsTransaction Tx, TxLog Log)> PlanAsync(DfsVolume v, string id, string target)
                {
                    TxLog log = history.Start(id);
                    DfsTransaction tx = await v.Transactions.BeginAsync(null, Ct);
                    string? a = await ReadLabelAsync(tx.Files, "/a");
                    string? b = await ReadLabelAsync(tx.Files, "/b");
                    log.Read("/a", a).Read("/b", b);
                    Assert.True(a == "1" && b == "1", $"{id} must read the initial state.");
                    await tx.Files.WriteAllTextAsync(target, "0", null, Ct);
                    log.Write(target, "0");
                    return (tx, log);
                }

                (DfsTransaction Tx, TxLog Log) p1 = await PlanAsync(v1, "T1", "/a");
                (DfsTransaction Tx, TxLog Log) p2 = await PlanAsync(v2, "T2", "/b");
                (TxResult r1, TxResult r2) = await CommitInOrderAsync(order, p1, p2);
                AssertExactlyOneCommitted(order, (p1.Tx, r1), (p2.Tx, r2));

                string? fa = await ReadLabelAsync(obs.Files, "/a");
                string? fb = await ReadLabelAsync(obs.Files, "/b");
                Assert.False(fa == "0" && fb == "0", "Write skew: both transactions committed {a:0, b:0} from their joint reads of {1, 1}.");
                Assert.Equal(r1.Outcome == OpOutcome.Ok ? "0" : "1", fa);
                Assert.Equal(r2.Outcome == OpOutcome.Ok ? "0" : "1", fb);

                await ObserveAsync(obs, history, "U", ["/a", "/b"]);
                await p1.Tx.DisposeAsync();
                await p2.Tx.DisposeAsync();
                history.AssertStrictlySerializable(initial);
            }
        }, Ct);

    [Theory]
    [InlineData(CommitOrder.FirstThenSecond)]
    [InlineData(CommitOrder.SecondThenFirst)]
    [InlineData(CommitOrder.Concurrent)]
    [Catalog("TX-006", "S", "INV-04")]
    public Task TX_006_WriteSkewRejected_AbsentNames(CommitOrder order) =>
        ClusterTest.RunAsync(Sim, async cluster =>
        {
            (DfsClient c1, DfsClient c2, DfsClient co) = (await ConnectAsync(cluster, "c1"), await ConnectAsync(cluster, "c2"), await ConnectAsync(cluster, "observer"));
            await using (c1)
            await using (c2)
            await using (co)
            {
                DfsVolume v1 = c1.GetVolume(Vol), v2 = c2.GetVolume(Vol), obs = co.GetVolume(Vol);
                await v1.Directories.CreateDirectoryAsync("/slots", Ct);
                var initial = new Dictionary<string, string?>();
                var history = new TxHistory(cluster.History);

                // Invariant (application): at most one of /slots/x, /slots/y exists. Each transaction reads both names
                // as absent and creates a different one.
                async Task<(DfsTransaction Tx, TxLog Log, Exception? StageError)> PlanAsync(DfsVolume v, string id, string target)
                {
                    TxLog log = history.Start(id);
                    DfsTransaction tx = await v.Transactions.BeginAsync(null, Ct);
                    Assert.False(await tx.Files.ExistsAsync("/slots/x", Ct));
                    Assert.False(await tx.Files.ExistsAsync("/slots/y", Ct));
                    log.Read("/slots/x", null).Read("/slots/y", null);
                    Exception? stage = await CaptureAsync(() => tx.Files.WriteAllTextAsync(target, id, null, Ct));
                    if (stage is null)
                        log.Write(target, id);
                    else
                        Assert.IsAssignableFrom<DfsSharingViolationException>(stage); // an early reservation refusal is a legal prevention
                    return (tx, log, stage);
                }

                (DfsTransaction Tx, TxLog Log, Exception? StageError) p1 = await PlanAsync(v1, "T1", "/slots/x");
                (DfsTransaction Tx, TxLog Log, Exception? StageError) p2 = await PlanAsync(v2, "T2", "/slots/y");
                TxResult r1, r2;
                if (p1.StageError is null && p2.StageError is null)
                {
                    (r1, r2) = await CommitInOrderAsync(order, (p1.Tx, p1.Log), (p2.Tx, p2.Log));
                    AssertExactlyOneCommitted(order, (p1.Tx, r1), (p2.Tx, r2));
                }
                else
                {
                    Assert.Null(p1.StageError); // the first stager cannot be refused
                    p2.Log.Complete(OpOutcome.Failed);
                    r2 = new TxResult(OpOutcome.Failed, p2.StageError, CommitToken.None);
                    r1 = await CommitAsync(p1.Tx, p1.Log);
                    Assert.Equal(OpOutcome.Ok, r1.Outcome);
                }

                bool x = await obs.Files.ExistsAsync("/slots/x", Ct);
                bool y = await obs.Files.ExistsAsync("/slots/y", Ct);
                Assert.False(x && y, "Phantom write skew: both transactions created a slot after reading both names as absent.");
                Assert.Equal(r1.Outcome == OpOutcome.Ok, x);
                Assert.Equal(r2.Outcome == OpOutcome.Ok, y);

                await ObserveAsync(obs, history, "U", ["/slots/x", "/slots/y"]);
                await p1.Tx.DisposeAsync();
                await p2.Tx.DisposeAsync();
                history.AssertStrictlySerializable(initial);
            }
        }, Ct);

    [Theory]
    [InlineData(CommitOrder.FirstThenSecond)]
    [InlineData(CommitOrder.SecondThenFirst)]
    [InlineData(CommitOrder.Concurrent)]
    [Catalog("TX-006", "S", "INV-04")]
    public Task TX_006_WriteSkewRejected_DirectoryPredicate(CommitOrder order) =>
        ClusterTest.RunAsync(Sim, async cluster =>
        {
            (DfsClient c1, DfsClient c2, DfsClient co) = (await ConnectAsync(cluster, "c1"), await ConnectAsync(cluster, "c2"), await ConnectAsync(cluster, "observer"));
            await using (c1)
            await using (c2)
            await using (co)
            {
                DfsVolume v1 = c1.GetVolume(Vol), v2 = c2.GetVolume(Vol), obs = co.GetVolume(Vol);
                await v1.Directories.CreateDirectoryAsync("/pool", Ct);
                await v1.Files.WriteAllTextAsync("/pool/m1", "m1", null, Ct);
                await v1.Files.WriteAllTextAsync("/pool/m2", "m2", null, Ct);
                var initial = new Dictionary<string, string?> { ["/pool/m1"] = "m1", ["/pool/m2"] = "m2" };
                var history = new TxHistory(cluster.History);

                // Invariant (application): /pool keeps at least one member. Each transaction enumerates the pool, sees two
                // members and removes a different one.
                async Task<(DfsTransaction Tx, TxLog Log, Exception? StageError)> PlanAsync(DfsVolume v, string id, string victim)
                {
                    TxLog log = history.Start(id);
                    DfsTransaction tx = await v.Transactions.BeginAsync(null, Ct);
                    string[] names = await ListNamesAsync(tx.Directories, "/pool");
                    Assert.Equal(["m1", "m2"], names);
                    log.Enumerated("/pool", names);
                    Exception? stage = await CaptureAsync(() => tx.Files.DeleteAsync("/pool/" + victim, Ct));
                    if (stage is null)
                        log.Write("/pool/" + victim, null);
                    else
                        Assert.IsAssignableFrom<DfsSharingViolationException>(stage); // an early reservation refusal (e.g. of the shared parent) is a legal prevention
                    return (tx, log, stage);
                }

                (DfsTransaction Tx, TxLog Log, Exception? StageError) p1 = await PlanAsync(v1, "T1", "m1");
                (DfsTransaction Tx, TxLog Log, Exception? StageError) p2 = await PlanAsync(v2, "T2", "m2");
                TxResult r1, r2;
                if (p1.StageError is null && p2.StageError is null)
                {
                    (r1, r2) = await CommitInOrderAsync(order, (p1.Tx, p1.Log), (p2.Tx, p2.Log));
                    AssertExactlyOneCommitted(order, (p1.Tx, r1), (p2.Tx, r2));
                }
                else
                {
                    Assert.Null(p1.StageError); // the first stager cannot be refused
                    p2.Log.Complete(OpOutcome.Failed);
                    r2 = new TxResult(OpOutcome.Failed, p2.StageError, CommitToken.None);
                    r1 = await CommitAsync(p1.Tx, p1.Log);
                    Assert.Equal(OpOutcome.Ok, r1.Outcome);
                }

                string[] remaining = await ListNamesAsync(obs.Directories, "/pool");
                Assert.NotEmpty(remaining);
                Assert.Equal(r1.Outcome == OpOutcome.Ok ? ["m2"] : ["m1"], remaining);

                TxLog u = history.Start("U");
                await using (DfsTransaction ro = await obs.Transactions.BeginAsync(null, Ct))
                {
                    u.Enumerated("/pool", await ListNamesAsync(ro.Directories, "/pool"));
                    await CommitAsync(ro, u);
                }

                await p1.Tx.DisposeAsync();
                await p2.Tx.DisposeAsync();
                history.AssertStrictlySerializable(initial);
            }
        }, Ct);

    public enum StandaloneOrder { StandaloneBeforeTransactionCommit, StandaloneAfterTransactionCommit, Concurrent }

    /// <summary>
    /// Test plan §9.6 standalone variant: T1 reads {/a, /b} and sets /a = 0 while an ordinary write sets /b = 0. When the
    /// ordinary write commits between T1's reads and T1's commit, T1 must conflict: FSD §8.1 requires read versions to be
    /// validated and ordinary mutations to participate in the same conflict system, and §9.6 forbids committing
    /// {a:0, b:0} on T1's original joint reads (even though a history serializing T1 first would also pass the
    /// strict-serializability checker). When the ordinary write follows T1's commit, {0, 0} is legal.
    /// </summary>
    [Theory]
    [InlineData(StandaloneOrder.StandaloneBeforeTransactionCommit)]
    [InlineData(StandaloneOrder.StandaloneAfterTransactionCommit)]
    [InlineData(StandaloneOrder.Concurrent)]
    [Catalog("TX-006", "S", "INV-04")]
    public Task TX_006_WriteSkewRejected_StandaloneMutation(StandaloneOrder order) =>
        ClusterTest.RunAsync(Sim, async cluster =>
        {
            (DfsClient c1, DfsClient c2, DfsClient co) = (await ConnectAsync(cluster, "c1"), await ConnectAsync(cluster, "c2"), await ConnectAsync(cluster, "observer"));
            await using (c1)
            await using (c2)
            await using (co)
            {
                DfsVolume v1 = c1.GetVolume(Vol), v2 = c2.GetVolume(Vol), obs = co.GetVolume(Vol);
                await v1.Files.WriteAllTextAsync("/a", "1", null, Ct);
                await v1.Files.WriteAllTextAsync("/b", "1", null, Ct);
                var initial = new Dictionary<string, string?> { ["/a"] = "1", ["/b"] = "1" };
                var history = new TxHistory(cluster.History);

                // T1 reads both and sets /a = 0; an ordinary standalone write (no transaction) sets /b = 0.
                TxLog l1 = history.Start("T1");
                DfsTransaction t1 = await v1.Transactions.BeginAsync(null, Ct);
                l1.Read("/a", await ReadLabelAsync(t1.Files, "/a")).Read("/b", await ReadLabelAsync(t1.Files, "/b"));
                await t1.Files.WriteAllTextAsync("/a", "0", null, Ct);
                l1.Write("/a", "0");

                Task<TxResult> Standalone() => StandaloneAsync(history, "W", l => l.Write("/b", "0"), () => v2.Files.WriteAllTextAsync("/b", "0", null, Ct));
                TxResult tr, wr;
                switch (order)
                {
                    case StandaloneOrder.StandaloneBeforeTransactionCommit:
                        wr = await Standalone();
                        tr = await CommitAsync(t1, l1);
                        Assert.Equal(OpOutcome.Ok, wr.Outcome);
                        AssertConflict(tr, t1, "a standalone write changed /b after T1 read it; T1 must not commit on stale reads");
                        Assert.Equal(("1", "0"), (await ReadLabelAsync(obs.Files, "/a"), await ReadLabelAsync(obs.Files, "/b")));
                        break;
                    case StandaloneOrder.StandaloneAfterTransactionCommit:
                        tr = await CommitAsync(t1, l1);
                        wr = await Standalone();
                        Assert.Equal(OpOutcome.Ok, tr.Outcome);
                        Assert.Equal(OpOutcome.Ok, wr.Outcome);
                        Assert.True(wr.Token.CompareTo(tr.Token) > 0, "The standalone write serializes after the transaction it followed.");
                        // {0,0} is legal here: the blind standalone write does not check the application's invariant (TX-028).
                        Assert.Equal(("0", "0"), (await ReadLabelAsync(obs.Files, "/a"), await ReadLabelAsync(obs.Files, "/b")));
                        break;
                    default:
                    {
                        Task<TxResult> commit = CommitAsync(t1, l1);
                        Task<TxResult> write = Standalone();
                        await Task.WhenAll(commit, write);
                        (tr, wr) = (await commit, await write);
                        Assert.Equal(OpOutcome.Ok, wr.Outcome);
                        if (tr.Outcome == OpOutcome.Ok)
                            Assert.True(tr.Token.CompareTo(wr.Token) < 0, "If both commit, T1 must have been applied before the standalone write it did not observe.");
                        else
                            AssertConflict(tr, t1, "T1 lost the race to the standalone write of /b it had read");
                        break;
                    }
                }

                await ObserveAsync(obs, history, "U", ["/a", "/b"]);
                await t1.DisposeAsync();
                history.AssertStrictlySerializable(initial);
            }
        }, Ct);

    private static async Task<(TxResult First, TxResult Second)> CommitInOrderAsync(CommitOrder order, (DfsTransaction Tx, TxLog Log) first, (DfsTransaction Tx, TxLog Log) second)
    {
        switch (order)
        {
            case CommitOrder.FirstThenSecond:
            {
                TxResult a = await CommitAsync(first.Tx, first.Log);
                TxResult b = await CommitAsync(second.Tx, second.Log);
                return (a, b);
            }

            case CommitOrder.SecondThenFirst:
            {
                TxResult b = await CommitAsync(second.Tx, second.Log);
                TxResult a = await CommitAsync(first.Tx, first.Log);
                return (a, b);
            }

            default:
            {
                Task<TxResult> a = CommitAsync(first.Tx, first.Log);
                Task<TxResult> b = CommitAsync(second.Tx, second.Log);
                await Task.WhenAll(a, b);
                return (await a, await b);
            }
        }
    }

    private static void AssertExactlyOneCommitted(CommitOrder order, (DfsTransaction Tx, TxResult Result) first, (DfsTransaction Tx, TxResult Result) second)
    {
        int committed = (first.Result.Outcome == OpOutcome.Ok ? 1 : 0) + (second.Result.Outcome == OpOutcome.Ok ? 1 : 0);
        Assert.True(committed == 1,
            $"Exactly one of two write-skewed transactions must commit ({order}); got first={first.Result.Outcome} {first.Result.Error?.GetType().Name}, second={second.Result.Outcome} {second.Result.Error?.GetType().Name}.");
        if (order == CommitOrder.FirstThenSecond)
            Assert.Equal(OpOutcome.Ok, first.Result.Outcome);
        if (order == CommitOrder.SecondThenFirst)
            Assert.Equal(OpOutcome.Ok, second.Result.Outcome);
        (DfsTransaction loserTx, TxResult loser) = first.Result.Outcome == OpOutcome.Ok ? second : first;
        AssertConflict(loser, loserTx, "the later commit's joint reads were invalidated by the earlier commit");
    }

    // ------------------------------------------------------------------------------------------------------------
    // TX-007 AbsentNameReadIsValidated
    // ------------------------------------------------------------------------------------------------------------

    public enum AbsenceObservation { Exists, GetInfo, ReadAllBytes, OpenForRead }

    public enum PhantomCreator { WriteAll, OpenCreateNew, MoveInto, CopyInto, OtherTransaction }

    public static TheoryData<AbsenceObservation, PhantomCreator> InsertIfAbsentCases()
    {
        var data = new TheoryData<AbsenceObservation, PhantomCreator>();
        foreach (AbsenceObservation observation in Enum.GetValues<AbsenceObservation>())
        {
            foreach (PhantomCreator creator in Enum.GetValues<PhantomCreator>())
                data.Add(observation, creator);
        }

        return data;
    }

    /// <summary>
    /// Insert-if-absent: T observes /slot/x absent, a concurrent creator commits /slot/x, then T writes /slot/x. If T
    /// committed it would silently replace the phantom it never saw (no serial order explains the result).
    /// </summary>
    [Theory]
    [MemberData(nameof(InsertIfAbsentCases))]
    [Catalog("TX-007", "S", "INV-04", "INV-07")]
    public Task TX_007_AbsentNameReadIsValidated(AbsenceObservation observation, PhantomCreator creator) =>
        ClusterTest.RunAsync(Sim, async cluster =>
        {
            (DfsClient c1, DfsClient c2, DfsClient co) = (await ConnectAsync(cluster, "c1"), await ConnectAsync(cluster, "c2"), await ConnectAsync(cluster, "observer"));
            await using (c1)
            await using (c2)
            await using (co)
            {
                DfsVolume v1 = c1.GetVolume(Vol), v2 = c2.GetVolume(Vol), obs = co.GetVolume(Vol);
                await v1.Directories.CreateDirectoryAsync("/slot", Ct);
                await v1.Files.WriteAllTextAsync("/src", "w", null, Ct);
                var initial = new Dictionary<string, string?> { ["/src"] = "w" };
                var history = new TxHistory(cluster.History);

                TxLog t = history.Start("T");
                DfsTransaction tx = await v1.Transactions.BeginAsync(null, Ct);
                await ObserveAbsentAsync(tx, "/slot/x", observation);
                t.Read("/slot/x", null);

                await CreatePhantomAsync(history, v2, creator, "/slot/x");
                Assert.True(await obs.Files.ExistsAsync("/slot/x", Ct), "The phantom is committed and visible to others.");

                DfsTransactionConflictException? early = await StageOrEarlyConflictAsync(() => tx.Files.WriteAllTextAsync("/slot/x", "t", null, Ct));
                if (early is null)
                    t.Write("/slot/x", "t");
                else
                    Assert.Equal(DfsErrorCode.TransactionConflict, early.ErrorCode);
                await CommitExpectingConflictAsync(tx, t, early, "T's absence read of /slot/x was invalidated by a committed create");

                // The final state is observed and checked with the history: T must not have replaced the phantom.
                TxRun final = await RunTxAsync(history, "U", obs, async (ro, log) => log.Read("/slot/x", await ReadLabelAsync(ro.Files, "/slot/x")));
                Assert.Equal(OpOutcome.Ok, final.Result.Outcome);
                Assert.Equal("w", await ReadLabelAsync(obs.Files, "/slot/x"));
                await tx.DisposeAsync();
                history.AssertStrictlySerializable(initial);
            }
        }, Ct);

    public enum ForcedCreator { WriteAll, CreateDirectory, MoveInto, OtherTransaction }

    /// <summary>
    /// Observer-forced phantom: T reads /slot/x absent and writes only /out. A creator commits /slot/x; observer U then
    /// sees /slot/x present and /out absent and commits. T &lt; creator &lt; U &lt; T is a cycle, so T must not commit.
    /// </summary>
    [Theory]
    [InlineData(ForcedCreator.WriteAll)]
    [InlineData(ForcedCreator.CreateDirectory)]
    [InlineData(ForcedCreator.MoveInto)]
    [InlineData(ForcedCreator.OtherTransaction)]
    [Catalog("TX-007", "S", "INV-04", "INV-07")]
    public Task TX_007_AbsentNameReadIsValidated_ObserverForcesOrder(ForcedCreator creator) =>
        ClusterTest.RunAsync(Sim, async cluster =>
        {
            (DfsClient c1, DfsClient c2, DfsClient co) = (await ConnectAsync(cluster, "c1"), await ConnectAsync(cluster, "c2"), await ConnectAsync(cluster, "observer"));
            await using (c1)
            await using (c2)
            await using (co)
            {
                DfsVolume v1 = c1.GetVolume(Vol), v2 = c2.GetVolume(Vol), obs = co.GetVolume(Vol);
                await v1.Directories.CreateDirectoryAsync("/slot", Ct);
                await v1.Files.WriteAllTextAsync("/src", "w", null, Ct);
                var initial = new Dictionary<string, string?> { ["/src"] = "w" };
                var history = new TxHistory(cluster.History);

                TxLog t = history.Start("T");
                DfsTransaction tx = await v1.Transactions.BeginAsync(null, Ct);
                Assert.False(await tx.Files.ExistsAsync("/slot/x", Ct));
                Assert.False(await tx.Directories.ExistsAsync("/slot/x", Ct));
                t.Read("/slot/x", null);
                await tx.Files.WriteAllTextAsync("/out", "t", null, Ct);
                t.Write("/out", "t");

                switch (creator)
                {
                    case ForcedCreator.WriteAll:
                        await CreatePhantomAsync(history, v2, PhantomCreator.WriteAll, "/slot/x");
                        break;
                    case ForcedCreator.MoveInto:
                        await CreatePhantomAsync(history, v2, PhantomCreator.MoveInto, "/slot/x");
                        break;
                    case ForcedCreator.OtherTransaction:
                        await CreatePhantomAsync(history, v2, PhantomCreator.OtherTransaction, "/slot/x");
                        break;
                    case ForcedCreator.CreateDirectory:
                    {
                        TxLog wd = history.Start("W-mkdir");
                        await v2.Directories.CreateDirectoryAsync("/slot/x", Ct);
                        wd.Write("/slot/x", "<dir>").Complete(OpOutcome.Ok);
                        break;
                    }
                }

                TxRun u = await RunTxAsync(history, "U", obs, async (ro, log) =>
                {
                    string? x = await PresenceLabelAsync(ro.Files, ro.Directories, "/slot/x");
                    Assert.NotNull(x);
                    log.Read("/slot/x", x).Read("/out", await ReadLabelAsync(ro.Files, "/out"));
                });
                Assert.Equal(OpOutcome.Ok, u.Result.Outcome);

                AssertConflict(await CommitAsync(tx, t), tx, "T read /slot/x as absent; a committed observer already saw it present and /out unwritten");
                Assert.False(await obs.Files.ExistsAsync("/out", Ct), "The conflicted transaction's write is not published.");
                await tx.DisposeAsync();
                history.AssertStrictlySerializable(initial);
            }
        }, Ct);

    [Fact]
    [Catalog("TX-007", "S", "INV-04", "INV-07")]
    public Task TX_007_AbsentNameReadIsValidated_CreationAfterCommit() =>
        ClusterTest.RunAsync(Sim, async cluster =>
        {
            await using DfsClient c1 = await ConnectAsync(cluster, "c1");
            await using DfsClient c2 = await ConnectAsync(cluster, "c2");
            DfsVolume v1 = c1.GetVolume(Vol), v2 = c2.GetVolume(Vol);
            await v1.Directories.CreateDirectoryAsync("/slot", Ct);
            var history = new TxHistory(cluster.History);

            TxRun t = await RunTxAsync(history, "T", v1, async (tx, log) =>
            {
                Assert.False(await tx.Files.ExistsAsync("/slot/x", Ct));
                log.Read("/slot/x", null);
                await tx.Files.WriteAllTextAsync("/out", "t", null, Ct);
                log.Write("/out", "t");
            });
            Assert.Equal(OpOutcome.Ok, t.Result.Outcome);
            TxResult w = await StandaloneAsync(history, "W", l => l.Write("/slot/x", "w"), () => v2.Files.WriteAllTextAsync("/slot/x", "w", null, Ct));
            Assert.Equal(OpOutcome.Ok, w.Outcome);
            Assert.True(w.Token.CompareTo(t.Result.Token) > 0);
            await ObserveAsync(v2, history, "U", ["/slot/x", "/out"]);
            history.AssertStrictlySerializable(new Dictionary<string, string?>());
        }, Ct);

    private static async Task ObserveAbsentAsync(DfsTransaction tx, string path, AbsenceObservation observation)
    {
        switch (observation)
        {
            case AbsenceObservation.Exists:
                Assert.False(await tx.Files.ExistsAsync(path, Ct));
                break;
            case AbsenceObservation.GetInfo:
                await Assert.ThrowsAnyAsync<FileNotFoundException>(() => tx.Files.GetInfoAsync(path, Ct));
                break;
            case AbsenceObservation.ReadAllBytes:
                await Assert.ThrowsAnyAsync<FileNotFoundException>(() => tx.Files.ReadAllBytesAsync(path, Ct));
                break;
            case AbsenceObservation.OpenForRead:
                await Assert.ThrowsAnyAsync<FileNotFoundException>(() => tx.Files.OpenAsync(path, FileMode.Open, FileAccess.Read, FileShare.Read, Ct));
                break;
        }
    }

    /// <summary>Creates <paramref name="path"/> with content "w" (records the operation) and asserts it committed.</summary>
    private static async Task CreatePhantomAsync(TxHistory history, DfsVolume v, PhantomCreator creator, string path)
    {
        switch (creator)
        {
            case PhantomCreator.WriteAll:
                Assert.Equal(OpOutcome.Ok, (await StandaloneAsync(history, "W", l => l.Write(path, "w"), () => v.Files.WriteAllTextAsync(path, "w", null, Ct))).Outcome);
                break;
            case PhantomCreator.OpenCreateNew:
            {
                // A creating open commits the (empty) file at open; the flush then publishes its contents.
                TxLog create = history.Start("W-open");
                DfsStream s = await v.Files.OpenAsync(path, FileMode.CreateNew, FileAccess.Write, FileShare.None, Ct);
                create.Write(path, string.Empty).Complete(OpOutcome.Ok);
                await using (s)
                {
                    TxLog flush = history.Start("W-flush");
                    await s.WriteAsync(Utf8("w"), Ct);
                    await s.FlushAsync(Ct);
                    flush.Write(path, "w").Complete(OpOutcome.Ok);
                }

                break;
            }

            case PhantomCreator.MoveInto:
                Assert.Equal(OpOutcome.Ok, (await StandaloneAsync(history, "W", l => l.Read("/src", "w").Write("/src", null).Write(path, "w"),
                    () => v.Files.MoveAsync("/src", path, false, Ct))).Outcome);
                break;
            case PhantomCreator.CopyInto:
                Assert.Equal(OpOutcome.Ok, (await StandaloneAsync(history, "W", l => l.Read("/src", "w").Write(path, "w"),
                    () => v.Files.CopyAsync("/src", path, false, Ct))).Outcome);
                break;
            case PhantomCreator.OtherTransaction:
            {
                TxRun w = await RunTxAsync(history, "W", v, async (tx, log) =>
                {
                    await tx.Files.WriteAllTextAsync(path, "w", null, Ct);
                    log.Write(path, "w");
                });
                Assert.Equal(OpOutcome.Ok, w.Result.Outcome);
                break;
            }
        }
    }

    // ------------------------------------------------------------------------------------------------------------
    // TX-008 DirectoryPredicateIsValidated
    // ------------------------------------------------------------------------------------------------------------

    public enum EnumerationKind { EnumerateAsync, ListPages, PatternEnumerate }

    /// <summary>
    /// Claim-next-slot: T enumerates /queue (job1, job2) and writes job3; a concurrent writer commits job3 first. If T
    /// committed it would overwrite an entry its enumeration never saw.
    /// </summary>
    [Theory]
    [InlineData(EnumerationKind.EnumerateAsync)]
    [InlineData(EnumerationKind.ListPages)]
    [InlineData(EnumerationKind.PatternEnumerate)]
    [Catalog("TX-008", "S", "INV-04", "INV-07")]
    public Task TX_008_DirectoryPredicateIsValidated(EnumerationKind kind) =>
        ClusterTest.RunAsync(Sim, async cluster =>
        {
            (DfsClient c1, DfsClient c2, DfsClient co) = (await ConnectAsync(cluster, "c1"), await ConnectAsync(cluster, "c2"), await ConnectAsync(cluster, "observer"));
            await using (c1)
            await using (c2)
            await using (co)
            {
                DfsVolume v1 = c1.GetVolume(Vol), v2 = c2.GetVolume(Vol), obs = co.GetVolume(Vol);
                await v1.Directories.CreateDirectoryAsync("/queue", Ct);
                await v1.Files.WriteAllTextAsync("/queue/job1", "j1", null, Ct);
                await v1.Files.WriteAllTextAsync("/queue/job2", "j2", null, Ct);
                var initial = new Dictionary<string, string?> { ["/queue/job1"] = "j1", ["/queue/job2"] = "j2" };
                var history = new TxHistory(cluster.History);

                TxLog t = history.Start("T");
                DfsTransaction tx = await v1.Transactions.BeginAsync(null, Ct);
                string[] names = await EnumerateAsync(tx.Directories, "/queue", kind);
                Assert.Equal(["job1", "job2"], names);
                t.Enumerated("/queue", names);
                string next = $"job{names.Length + 1}";

                TxResult w = await StandaloneAsync(history, "W", l => l.Write("/queue/" + next, "w"), () => v2.Files.WriteAllTextAsync("/queue/" + next, "w", null, Ct));
                Assert.Equal(OpOutcome.Ok, w.Outcome);

                DfsTransactionConflictException? early = await StageOrEarlyConflictAsync(() => tx.Files.WriteAllTextAsync("/queue/" + next, "t", null, Ct));
                if (early is null)
                    t.Write("/queue/" + next, "t");
                else
                    Assert.Equal(DfsErrorCode.TransactionConflict, early.ErrorCode);
                await CommitExpectingConflictAsync(tx, t, early, "T's enumeration predicate on /queue was invalidated by a committed matching entry");

                // The final state is observed and checked with the history: T must not have overwritten the new entry.
                TxRun final = await RunTxAsync(history, "U", obs, async (ro, log) =>
                    log.Enumerated("/queue", await ListNamesAsync(ro.Directories, "/queue")).Read("/queue/" + next, await ReadLabelAsync(ro.Files, "/queue/" + next)));
                Assert.Equal(OpOutcome.Ok, final.Result.Outcome);
                Assert.Equal("w", await ReadLabelAsync(obs.Files, "/queue/" + next));
                await tx.DisposeAsync();
                history.AssertStrictlySerializable(initial);
            }
        }, Ct);

    public enum PredicateChange { AddMatching, RemoveMatching, RenameIn, RenameOut, OtherTransactionAdds }

    /// <summary>
    /// Observer-forced predicate: T enumerates /pool and writes only /pool-count; a change to /pool commits; observer U
    /// sees the changed membership and /pool-count unwritten and commits. T must not commit.
    /// </summary>
    [Theory]
    [InlineData(PredicateChange.AddMatching)]
    [InlineData(PredicateChange.RemoveMatching)]
    [InlineData(PredicateChange.RenameIn)]
    [InlineData(PredicateChange.RenameOut)]
    [InlineData(PredicateChange.OtherTransactionAdds)]
    [Catalog("TX-008", "S", "INV-04", "INV-07")]
    public Task TX_008_DirectoryPredicateIsValidated_ObserverForcesOrder(PredicateChange change) =>
        ClusterTest.RunAsync(Sim, async cluster =>
        {
            (DfsClient c1, DfsClient c2, DfsClient co) = (await ConnectAsync(cluster, "c1"), await ConnectAsync(cluster, "c2"), await ConnectAsync(cluster, "observer"));
            await using (c1)
            await using (c2)
            await using (co)
            {
                DfsVolume v1 = c1.GetVolume(Vol), v2 = c2.GetVolume(Vol), obs = co.GetVolume(Vol);
                Dictionary<string, string?> initial = await SeedPoolAsync(v1);
                var history = new TxHistory(cluster.History);

                TxLog t = history.Start("T");
                DfsTransaction tx = await v1.Transactions.BeginAsync(null, Ct);
                string[] names = await ListNamesAsync(tx.Directories, "/pool");
                Assert.Equal(["m1", "m2"], names);
                t.Enumerated("/pool", names);
                await tx.Files.WriteAllTextAsync("/pool-count", "2", null, Ct);
                t.Write("/pool-count", "2");

                await ChangePoolAsync(history, v2, change);

                TxRun u = await RunTxAsync(history, "U", obs, async (ro, log) =>
                {
                    log.Enumerated("/pool", await ListNamesAsync(ro.Directories, "/pool"));
                    log.Read("/pool-count", await ReadLabelAsync(ro.Files, "/pool-count"));
                });
                Assert.Equal(OpOutcome.Ok, u.Result.Outcome);

                AssertConflict(await CommitAsync(tx, t), tx, $"{change}: T's enumeration of /pool is stale and a committed observer pins the order");
                Assert.False(await obs.Files.ExistsAsync("/pool-count", Ct));
                await tx.DisposeAsync();
                history.AssertStrictlySerializable(initial);
            }
        }, Ct);

    [Fact]
    [Catalog("TX-008", "S", "INV-04", "INV-07")]
    public Task TX_008_DirectoryPredicateIsValidated_ChangeAfterCommit() =>
        ClusterTest.RunAsync(Sim, async cluster =>
        {
            await using DfsClient c1 = await ConnectAsync(cluster, "c1");
            await using DfsClient c2 = await ConnectAsync(cluster, "c2");
            DfsVolume v1 = c1.GetVolume(Vol), v2 = c2.GetVolume(Vol);
            Dictionary<string, string?> initial = await SeedPoolAsync(v1);
            var history = new TxHistory(cluster.History);

            TxRun t = await RunTxAsync(history, "T", v1, async (tx, log) =>
            {
                log.Enumerated("/pool", await ListNamesAsync(tx.Directories, "/pool"));
                await tx.Files.WriteAllTextAsync("/pool-count", "2", null, Ct);
                log.Write("/pool-count", "2");
            });
            Assert.Equal(OpOutcome.Ok, t.Result.Outcome);
            await ChangePoolAsync(history, v2, PredicateChange.AddMatching);
            TxRun u = await RunTxAsync(history, "U", v2, async (ro, log) =>
            {
                string[] names = await ListNamesAsync(ro.Directories, "/pool");
                Assert.Equal(["m1", "m2", "m3"], names);
                log.Enumerated("/pool", names).Read("/pool-count", await ReadLabelAsync(ro.Files, "/pool-count"));
            });
            Assert.Equal(OpOutcome.Ok, u.Result.Outcome);
            history.AssertStrictlySerializable(initial);
        }, Ct);

    private static async Task<string[]> EnumerateAsync(DfsDirectoryClient directories, string path, EnumerationKind kind)
    {
        switch (kind)
        {
            case EnumerationKind.EnumerateAsync:
                return await ListNamesAsync(directories, path);
            case EnumerationKind.PatternEnumerate:
                return await ListNamesAsync(directories, path, new DfsEnumerationOptions { Pattern = "job*", Kinds = DfsEntryKinds.Files });
            default:
            {
                var names = new List<string>();
                string? continuation = null;
                CommitToken? pinned = null;
                do
                {
                    DfsDirectoryPage page = await directories.ListPageAsync(path, new DfsEnumerationOptions { PageSize = 1 }, continuation, Ct);
                    pinned ??= page.PinnedAt;
                    Assert.Equal(pinned, page.PinnedAt); // every page comes from the same pinned version
                    names.AddRange(page.Entries.Select(e => e.Name));
                    continuation = page.ContinuationToken;
                }
                while (continuation is not null);
                names.Sort(StringComparer.Ordinal);
                return names.ToArray();
            }
        }
    }

    private static async Task<Dictionary<string, string?>> SeedPoolAsync(DfsVolume v)
    {
        await v.Directories.CreateDirectoryAsync("/pool", Ct);
        await v.Directories.CreateDirectoryAsync("/staging", Ct);
        await v.Directories.CreateDirectoryAsync("/archive", Ct);
        await v.Files.WriteAllTextAsync("/pool/m1", "m1", null, Ct);
        await v.Files.WriteAllTextAsync("/pool/m2", "m2", null, Ct);
        await v.Files.WriteAllTextAsync("/staging/m9", "m9", null, Ct);
        return new Dictionary<string, string?> { ["/pool/m1"] = "m1", ["/pool/m2"] = "m2", ["/staging/m9"] = "m9" };
    }

    private static async Task ChangePoolAsync(TxHistory history, DfsVolume v, PredicateChange change)
    {
        TxResult result = change switch
        {
            PredicateChange.AddMatching => await StandaloneAsync(history, "W", l => l.Write("/pool/m3", "m3"), () => v.Files.WriteAllTextAsync("/pool/m3", "m3", null, Ct)),
            PredicateChange.RemoveMatching => await StandaloneAsync(history, "W", l => l.Write("/pool/m1", null), () => v.Files.DeleteAsync("/pool/m1", Ct)),
            PredicateChange.RenameIn => await StandaloneAsync(history, "W", l => l.Read("/staging/m9", "m9").Write("/staging/m9", null).Write("/pool/m9", "m9"),
                () => v.Files.MoveAsync("/staging/m9", "/pool/m9", false, Ct)),
            PredicateChange.RenameOut => await StandaloneAsync(history, "W", l => l.Read("/pool/m2", "m2").Write("/pool/m2", null).Write("/archive/m2", "m2"),
                () => v.Files.MoveAsync("/pool/m2", "/archive/m2", false, Ct)),
            _ => (await RunTxAsync(history, "W", v, async (tx, log) =>
            {
                await tx.Files.WriteAllTextAsync("/pool/m4", "m4", null, Ct);
                log.Write("/pool/m4", "m4");
            })).Result,
        };
        Assert.Equal(OpOutcome.Ok, result.Outcome);
    }

    // ------------------------------------------------------------------------------------------------------------
    // TX-009 StrictRealTimeOrder
    // ------------------------------------------------------------------------------------------------------------

    public enum RealTimeVariant
    {
        SameClient, OtherClientFollowerSeed, AfterLeaderCrash, AfterLeaderIsolation, StaleLeaderMinority,
        StandaloneWriteThenTransactionRead, TransactionWriteThenStandaloneRead, ChainOfThree,
    }

    [Theory]
    [InlineData(RealTimeVariant.SameClient)]
    [InlineData(RealTimeVariant.OtherClientFollowerSeed)]
    [InlineData(RealTimeVariant.AfterLeaderCrash)]
    [InlineData(RealTimeVariant.AfterLeaderIsolation)]
    [InlineData(RealTimeVariant.StaleLeaderMinority)]
    [InlineData(RealTimeVariant.StandaloneWriteThenTransactionRead)]
    [InlineData(RealTimeVariant.TransactionWriteThenStandaloneRead)]
    [InlineData(RealTimeVariant.ChainOfThree)]
    [Catalog("TX-009", "S", "INV-04")]
    public Task TX_009_StrictRealTimeOrder(RealTimeVariant variant) =>
        ClusterTest.RunAsync(Sim, async cluster =>
        {
            (TestNode leader, TestNode follower) = await LeaderAndFollowerAsync(cluster);
            IReadOnlyList<string>? bSeeds = variant switch
            {
                RealTimeVariant.OtherClientFollowerSeed or RealTimeVariant.TransactionWriteThenStandaloneRead => [follower.Name],
                RealTimeVariant.StaleLeaderMinority => [leader.Name],
                _ => null,
            };
            await using DfsClient c1 = await ConnectAsync(cluster, "c1");
            await using DfsClient c2 = await ConnectAsync(cluster, "c2", bSeeds, TimeSpan.FromSeconds(3));
            await using DfsClient c3 = await ConnectAsync(cluster, "c3");
            DfsVolume a = c1.GetVolume(Vol);
            DfsVolume b = variant == RealTimeVariant.SameClient ? a : c2.GetVolume(Vol);
            await a.Files.WriteAllTextAsync("/x", "x0", null, Ct);
            await a.Files.WriteAllTextAsync("/y", "y0", null, Ct);
            Assert.Equal("x0", await ReadLabelAsync(b.Files, "/x")); // warm b's routes before any fault
            var initial = new Dictionary<string, string?> { ["/x"] = "x0", ["/y"] = "y0" };
            var history = new TxHistory(cluster.History);

            if (variant == RealTimeVariant.StaleLeaderMinority)
            {
                // The old leader and client c2 are cut off with a minority; the majority elects a new leader and A commits there.
                string[] majority = [.. cluster.Nodes.Where(n => n.Name != leader.Name).Select(n => n.Name), "client:c1", "client:c3"];
                cluster.Network.Partition([leader.Name, "client:c2"], majority);
                await WaitForNewLeaderAsync(cluster, leader.Name);
            }

            // A completes before B is invoked.
            CommitToken tokenA;
            if (variant == RealTimeVariant.StandaloneWriteThenTransactionRead)
            {
                TxResult ar = await StandaloneAsync(history, "A", l => l.Write("/x", "A"), () => a.Files.WriteAllTextAsync("/x", "A", null, Ct));
                Assert.Equal(OpOutcome.Ok, ar.Outcome);
                tokenA = ar.Token;
            }
            else
            {
                TxRun ar = await RetryTransientAsync(cluster, () => RunTxAsync(history, "A", a, async (tx, log) =>
                {
                    await tx.Files.WriteAllTextAsync("/x", "A", null, Ct);
                    log.Write("/x", "A");
                }), "transaction A commits");
                Assert.Equal(OpOutcome.Ok, ar.Result.Outcome);
                tokenA = ar.Result.Token;
            }

            switch (variant)
            {
                case RealTimeVariant.AfterLeaderCrash:
                    await leader.CrashAsync(CrashMode.LoseUnflushed);
                    break;
                case RealTimeVariant.AfterLeaderIsolation:
                    cluster.Network.Isolate(leader.Name);
                    await WaitForNewLeaderAsync(cluster, leader.Name);
                    break;
                case RealTimeVariant.StaleLeaderMinority:
                {
                    // B through the stale minority leader must fail rather than serve the pre-A value.
                    Exception? stale = await CaptureAsync(async () =>
                    {
                        TxRun sb = await RunTxAsync(history, "B-stale", b, async (tx, log) =>
                        {
                            string? x = await ReadLabelAsync(tx.Files, "/x");
                            log.Read("/x", x);
                            Assert.Equal("A", x);
                        });
                        Assert.NotEqual(OpOutcome.Ok, sb.Result.Outcome);
                    });
                    Assert.True(stale is null || stale is not Xunit.Sdk.XunitException,
                        $"A minority leader served a transaction read that predates A: {stale?.Message}");
                    cluster.Network.Heal();
                    break;
                }
            }

            if (variant == RealTimeVariant.TransactionWriteThenStandaloneRead)
            {
                TxLog rb = history.Start("B");
                string? x = await ReadLabelAsync(b.Files, "/x");
                rb.Read("/x", x).Complete(OpOutcome.Ok);
                Assert.Equal("A", x);
                DfsFileInfo info = await b.Files.GetInfoAsync("/x", Ct);
                Assert.True(info.Revision >= tokenA.Sequence && info.ObservedAt.IsAtOrAfter(tokenA), "A later live read observes a version at or after A's commit.");
            }
            else
            {
                CommitToken readVersionB = CommitToken.None;
                TxRun rb = await RetryTransientAsync(cluster, () => RunTxAsync(history, "B", b, async (tx, log) =>
                {
                    readVersionB = tx.ReadVersion;
                    string? x = await ReadLabelAsync(tx.Files, "/x");
                    log.Read("/x", x);
                    Assert.Equal("A", x);
                    if (variant == RealTimeVariant.ChainOfThree)
                    {
                        await tx.Files.WriteAllTextAsync("/y", "B", null, Ct);
                        log.Write("/y", "B");
                    }
                }), "transaction B commits after A");
                Assert.Equal(OpOutcome.Ok, rb.Result.Outcome);
                Assert.True(readVersionB.IsAtOrAfter(tokenA), $"B began after A completed, so its read version {readVersionB} must be at or after A's commit {tokenA}.");
                if (variant == RealTimeVariant.ChainOfThree)
                {
                    Assert.True(rb.Result.Token.CompareTo(tokenA) > 0);
                    TxRun rc = await RunTxAsync(history, "C", c3.GetVolume(Vol), async (tx, log) =>
                    {
                        Assert.True(tx.ReadVersion.IsAtOrAfter(rb.Result.Token));
                        string? x = await ReadLabelAsync(tx.Files, "/x");
                        string? y = await ReadLabelAsync(tx.Files, "/y");
                        log.Read("/x", x).Read("/y", y);
                        Assert.Equal(("A", "B"), (x, y));
                    });
                    Assert.Equal(OpOutcome.Ok, rc.Result.Outcome);
                }
            }

            if (variant is RealTimeVariant.AfterLeaderIsolation or RealTimeVariant.StaleLeaderMinority)
                cluster.Network.Heal();
            if (variant == RealTimeVariant.AfterLeaderCrash)
                await leader.RestartAsync();
            await cluster.Inspect.AssertNoSplitBrainAsync();
            history.AssertStrictlySerializable(initial);
        }, Ct);

    // ------------------------------------------------------------------------------------------------------------
    // TX-010 ReadOnlyTransactionIsConsistent
    // ------------------------------------------------------------------------------------------------------------

    [Theory]
    [InlineData(16, 1)]
    [InlineData(16, 3)]
    [InlineData(150_000, 2)]
    [Catalog("TX-010", "S", "INV-02", "INV-04")]
    public Task TX_010_ReadOnlyTransactionIsConsistent(int fileSize, int readers) =>
        ClusterTest.RunAsync(Sim, async cluster =>
        {
            const int generations = 6;
            string[] files = ["/p", "/q", "/r"];
            var labels = new ContentLabels();
            byte[] Content(int gen, int file) => TestData.Bytes((ulong)(1000 * gen + file), fileSize);
            for (int g = 0; g <= generations; g++)
            {
                for (int f = 0; f < files.Length; f++)
                    labels.Add($"g{g}", Content(g, f));
            }

            await using DfsClient writerClient = await ConnectAsync(cluster, "writer");
            DfsVolume w = writerClient.GetVolume(Vol);
            for (int f = 0; f < files.Length; f++)
                await w.Files.WriteAllBytesAsync(files[f], Content(0, f), Ct);
            Dictionary<string, string?> initial = files.ToDictionary(f => f, _ => (string?)"g0", StringComparer.Ordinal);
            var history = new TxHistory(cluster.History);

            var readerClients = new List<DfsClient>();
            try
            {
                for (int i = 0; i < readers; i++)
                    readerClients.Add(await ConnectAsync(cluster, $"reader{i}"));

                var writerDone = new TaskCompletionSource();
                async Task WriterAsync()
                {
                    try
                    {
                        for (int g = 1; g <= generations; g++)
                        {
                            int gen = g;
                            TxRun run = await RunTxAsync(history, $"W{gen}", w, async (tx, log) =>
                            {
                                for (int f = 0; f < files.Length; f++)
                                {
                                    await tx.Files.WriteAllBytesAsync(files[f], Content(gen, f), Ct);
                                    log.Write(files[f], $"g{gen}");
                                }
                            });
                            Assert.Equal(OpOutcome.Ok, run.Result.Outcome);
                            await cluster.DelayAsync(TimeSpan.FromMilliseconds(37), Ct);
                        }
                    }
                    finally
                    {
                        writerDone.TrySetResult();
                    }
                }

                async Task ReaderAsync(int index)
                {
                    DfsVolume v = readerClients[index].GetVolume(Vol);
                    int lastGeneration = -1;
                    CommitToken lastReadVersion = CommitToken.None;
                    int observations = 0;
                    bool final = false;
                    while (true)
                    {
                        final = writerDone.Task.IsCompleted;
                        var seen = new List<string?>();
                        TxRun run = await RunTxAsync(history, $"R{index}-{observations++}", v, async (tx, log) =>
                        {
                            if (!lastReadVersion.IsNone)
                                Assert.True(tx.ReadVersion.IsAtOrAfter(lastReadVersion), "A later transaction of the same client never reads an older version.");
                            lastReadVersion = tx.ReadVersion;
                            for (int k = 0; k < files.Length; k++)
                            {
                                string file = files[(k + index) % files.Length];   // each reader uses its own order
                                string? label = await ReadLabelAsync(tx.Files, file, labels);
                                seen.Add(label);
                                log.Read(file, label);
                                await cluster.DelayAsync(TimeSpan.FromMilliseconds(13 * (index + 1)), Ct); // straddle concurrent commits
                            }
                        });
                        Assert.True(run.Result.Outcome is OpOutcome.Ok or OpOutcome.Failed);
                        Assert.True(seen.Distinct().Count() == 1, $"Reader {index} saw a fractured combination: {string.Join(", ", seen)}.");
                        int generation = int.Parse(seen[0]![1..], CultureInfo.InvariantCulture);
                        Assert.True(generation >= lastGeneration, $"Reader {index} went back in time: g{generation} after g{lastGeneration}.");
                        lastGeneration = generation;
                        if (final)
                        {
                            Assert.Equal(generations, generation);
                            return;
                        }
                    }
                }

                await Task.WhenAll(new[] { WriterAsync() }.Concat(Enumerable.Range(0, readers).Select(ReaderAsync)));
                history.AssertStrictlySerializable(initial);
            }
            finally
            {
                foreach (DfsClient c in readerClients)
                    await c.DisposeAsync();
            }
        }, Ct);

    // ------------------------------------------------------------------------------------------------------------
    // TX-011 OrdinaryWritesParticipateInConflicts
    // ------------------------------------------------------------------------------------------------------------

    public enum StandaloneMutation { WriteAllBytes, AppendAllBytes, StreamFlush, StreamWriteThrough, StreamSetLength, CopyOnto, MoveOnto, MoveAway, Delete }

    public enum ConflictConstruction { ReadModifyWrite, ObserverForced }

    public static TheoryData<StandaloneMutation, ConflictConstruction> OrdinaryWriteCases()
    {
        var data = new TheoryData<StandaloneMutation, ConflictConstruction>();
        foreach (StandaloneMutation mutation in Enum.GetValues<StandaloneMutation>())
        {
            data.Add(mutation, ConflictConstruction.ReadModifyWrite);
            data.Add(mutation, ConflictConstruction.ObserverForced);
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(OrdinaryWriteCases))]
    [Catalog("TX-011", "S", "INV-04")]
    public Task TX_011_OrdinaryWritesParticipateInConflicts(StandaloneMutation mutation, ConflictConstruction construction) =>
        ClusterTest.RunAsync(Sim, async cluster =>
        {
            (DfsClient c1, DfsClient c2, DfsClient co) = (await ConnectAsync(cluster, "c1"), await ConnectAsync(cluster, "c2"), await ConnectAsync(cluster, "observer"));
            await using (c1)
            await using (c2)
            await using (co)
            {
                DfsVolume v1 = c1.GetVolume(Vol), v2 = c2.GetVolume(Vol), obs = co.GetVolume(Vol);
                await v1.Files.WriteAllTextAsync("/f", "value-0", null, Ct);
                await v1.Files.WriteAllTextAsync("/src", "src", null, Ct);
                var initial = new Dictionary<string, string?> { ["/f"] = "value-0", ["/src"] = "src" };
                var history = new TxHistory(cluster.History);

                TxLog t = history.Start("T");
                DfsTransaction tx = await v1.Transactions.BeginAsync(null, Ct);
                string? read = await ReadLabelAsync(tx.Files, "/f");
                Assert.Equal("value-0", read);
                t.Read("/f", read);
                if (construction == ConflictConstruction.ObserverForced)
                {
                    await tx.Files.WriteAllTextAsync("/out", "t", null, Ct);
                    t.Write("/out", "t");
                }

                IReadOnlyDictionary<string, string?> after = await ApplyStandaloneMutationAsync(history, v2, mutation);

                DfsTransactionConflictException? early = null;
                if (construction == ConflictConstruction.ReadModifyWrite)
                {
                    // T now writes the value it computed from its stale read; committing would lose the standalone update.
                    // (The stale read may also be detected already while staging this write.)
                    early = await StageOrEarlyConflictAsync(() => tx.Files.WriteAllTextAsync("/f", read + "+T", null, Ct));
                    if (early is null)
                        t.Write("/f", read + "+T");
                }
                else
                {
                    TxRun u = await RunTxAsync(history, "U", obs, async (ro, log) =>
                        log.Read("/f", await ReadLabelAsync(ro.Files, "/f")).Read("/out", await ReadLabelAsync(ro.Files, "/out")));
                    Assert.Equal(OpOutcome.Ok, u.Result.Outcome);
                }

                await CommitExpectingConflictAsync(tx, t, early, $"{mutation} committed after T read /f; ordinary writes must participate in validation");

                // The final state is observed and checked with the history: the ordinary mutation's result survives.
                TxRun final = await RunTxAsync(history, "F", obs, async (ro, log) =>
                {
                    foreach (string key in after.Keys.Append("/out"))
                        log.Read(key, await ReadLabelAsync(ro.Files, key));
                });
                Assert.Equal(OpOutcome.Ok, final.Result.Outcome);
                foreach ((string key, string? expected) in after)
                    Assert.Equal(expected, await ReadLabelAsync(obs.Files, key));
                Assert.False(await obs.Files.ExistsAsync("/out", Ct));
                await tx.DisposeAsync();
                history.AssertStrictlySerializable(initial);
            }
        }, Ct);

    /// <summary>Applies one ordinary mutation and returns the expected resulting state of the touched keys.</summary>
    private static async Task<IReadOnlyDictionary<string, string?>> ApplyStandaloneMutationAsync(TxHistory history, DfsVolume v, StandaloneMutation mutation)
    {
        TxResult result;
        Dictionary<string, string?> after;
        switch (mutation)
        {
            case StandaloneMutation.WriteAllBytes:
                after = new() { ["/f"] = "w" };
                result = await StandaloneAsync(history, "W", l => l.Write("/f", "w"), () => v.Files.WriteAllBytesAsync("/f", Utf8("w"), Ct));
                break;
            case StandaloneMutation.AppendAllBytes:
                after = new() { ["/f"] = "value-0+a" };
                result = await StandaloneAsync(history, "W", l => l.Read("/f", "value-0").Write("/f", "value-0+a"), () => v.Files.AppendAllBytesAsync("/f", Utf8("+a"), Ct));
                break;
            case StandaloneMutation.StreamFlush:
                after = new() { ["/f"] = "value-0+s" };
                result = await StandaloneAsync(history, "W", l => l.Read("/f", "value-0").Write("/f", "value-0+s"), async () =>
                {
                    await using DfsStream s = await v.Files.OpenAsync("/f", FileMode.Open, FileAccess.ReadWrite, FileShare.None, Ct);
                    s.Seek(0, SeekOrigin.End);
                    await s.WriteAsync(Utf8("+s"), Ct);
                    return await s.CommitAsync(Ct);
                });
                break;
            case StandaloneMutation.StreamWriteThrough:
                after = new() { ["/f"] = "Value-0" };
                result = await StandaloneAsync(history, "W", l => l.Read("/f", "value-0").Write("/f", "Value-0"), async () =>
                {
                    await using DfsStream s = await v.Files.OpenAsync("/f", new DfsOpenOptions
                    {
                        Mode = FileMode.Open, Access = FileAccess.ReadWrite, Share = FileShare.None, WriteMode = DfsWriteMode.WriteThrough,
                    }, Ct);
                    await s.WriteAsync(Utf8("V"), Ct);
                    return (await s.GetInfoAsync(Ct)).ObservedAt;
                });
                break;
            case StandaloneMutation.StreamSetLength:
                after = new() { ["/f"] = "value" };
                result = await StandaloneAsync(history, "W", l => l.Read("/f", "value-0").Write("/f", "value"), async () =>
                {
                    await using DfsStream s = await v.Files.OpenAsync("/f", new DfsOpenOptions
                    {
                        Mode = FileMode.Open, Access = FileAccess.ReadWrite, Share = FileShare.None, WriteMode = DfsWriteMode.WriteThrough,
                    }, Ct);
                    await s.SetLengthAsync(5, Ct);
                    return (await s.GetInfoAsync(Ct)).ObservedAt;
                });
                break;
            case StandaloneMutation.CopyOnto:
                after = new() { ["/f"] = "src", ["/src"] = "src" };
                result = await StandaloneAsync(history, "W", l => l.Read("/src", "src").Write("/f", "src"), () => v.Files.CopyAsync("/src", "/f", true, Ct));
                break;
            case StandaloneMutation.MoveOnto:
                after = new() { ["/f"] = "src", ["/src"] = null };
                result = await StandaloneAsync(history, "W", l => l.Read("/src", "src").Write("/src", null).Write("/f", "src"), () => v.Files.MoveAsync("/src", "/f", true, Ct));
                break;
            case StandaloneMutation.MoveAway:
                after = new() { ["/f"] = null, ["/g"] = "value-0" };
                result = await StandaloneAsync(history, "W", l => l.Read("/f", "value-0").Write("/f", null).Write("/g", "value-0"), () => v.Files.MoveAsync("/f", "/g", false, Ct));
                break;
            default:
                after = new() { ["/f"] = null };
                result = await StandaloneAsync(history, "W", l => l.Write("/f", null), () => v.Files.DeleteAsync("/f", Ct));
                break;
        }

        Assert.True(result.Outcome == OpOutcome.Ok, $"The standalone {mutation} must commit: {result.Error}");
        return after;
    }

    /// <summary>
    /// Copy cycle: T reads the copy destination /d and writes the source /s; an ordinary Copy(/s → /d) commits in
    /// between. Neither serial order explains both, so T must not commit.
    /// </summary>
    [Fact]
    [Catalog("TX-011", "S", "INV-04")]
    public Task TX_011_OrdinaryWritesParticipateInConflicts_CopyCycle() =>
        ClusterTest.RunAsync(Sim, async cluster =>
        {
            await using DfsClient c1 = await ConnectAsync(cluster, "c1");
            await using DfsClient c2 = await ConnectAsync(cluster, "c2");
            DfsVolume v1 = c1.GetVolume(Vol), v2 = c2.GetVolume(Vol);
            await v1.Files.WriteAllTextAsync("/s", "s0", null, Ct);
            await v1.Files.WriteAllTextAsync("/d", "d0", null, Ct);
            var history = new TxHistory(cluster.History);

            TxLog t = history.Start("T");
            DfsTransaction tx = await v1.Transactions.BeginAsync(null, Ct);
            t.Read("/d", await ReadLabelAsync(tx.Files, "/d"));
            TxResult copy = await StandaloneAsync(history, "Copy", l => l.Read("/s", "s0").Write("/d", "s0"), () => v2.Files.CopyAsync("/s", "/d", true, Ct));
            Assert.Equal(OpOutcome.Ok, copy.Outcome);
            DfsTransactionConflictException? early = await StageOrEarlyConflictAsync(() => tx.Files.WriteAllTextAsync("/s", "sT", null, Ct));
            if (early is null)
                t.Write("/s", "sT");
            await CommitExpectingConflictAsync(tx, t, early, "T read /d before an ordinary Copy replaced it");
            Assert.Equal(("s0", "s0"), (await ReadLabelAsync(v2.Files, "/s"), await ReadLabelAsync(v2.Files, "/d")));
            await tx.DisposeAsync();
            history.AssertStrictlySerializable(new Dictionary<string, string?> { ["/s"] = "s0", ["/d"] = "d0" });
        }, Ct);

    /// <summary>While a transaction holds the writer reservation of /f, ordinary writers are refused immediately.</summary>
    [Fact]
    [Catalog("TX-011", "S", "INV-04")]
    public Task TX_011_OrdinaryWritesParticipateInConflicts_ReservationBlocksStandaloneWriters() =>
        ClusterTest.RunAsync(Sim, async cluster =>
        {
            await using DfsClient c1 = await ConnectAsync(cluster, "c1");
            await using DfsClient c2 = await ConnectAsync(cluster, "c2");
            DfsVolume v1 = c1.GetVolume(Vol), v2 = c2.GetVolume(Vol);
            await v1.Files.WriteAllTextAsync("/f", "f0", null, Ct);
            var history = new TxHistory(cluster.History);

            TxLog t = history.Start("T");
            DfsTransaction tx = await v1.Transactions.BeginAsync(null, Ct);
            await tx.Files.WriteAllTextAsync("/f", "fT", null, Ct);
            t.Write("/f", "fT");

            var attempts = new (string Name, Func<Task> Action)[]
            {
                ("WriteAllBytes", () => v2.Files.WriteAllBytesAsync("/f", Utf8("w"), Ct)),
                ("AppendAllBytes", () => v2.Files.AppendAllBytesAsync("/f", Utf8("+a"), Ct)),
                ("OpenWrite", async () => { await using DfsStream s = await v2.Files.OpenWriteAsync("/f", Ct); }),
                ("Open ReadWrite", async () => { await using DfsStream s = await v2.Files.OpenAsync("/f", FileMode.Open, FileAccess.ReadWrite, FileShare.Read, Ct); }),
            };
            foreach ((string name, Func<Task> action) in attempts)
            {
                Exception? ex = await CaptureAsync(action);
                DfsSharingViolationException sv = Assert.IsAssignableFrom<DfsSharingViolationException>(ex);
                Assert.True(sv.ErrorCode is DfsErrorCode.SharingViolation or DfsErrorCode.WriterConflict, $"{name}: unexpected code {sv.ErrorCode}");
                Assert.True(sv.IsRetryable, $"{name}: a writer conflict is retryable");
            }

            Assert.Equal("f0", await ReadLabelAsync(v2.Files, "/f"));
            TxResult committed = await CommitAsync(tx, t);
            Assert.Equal(OpOutcome.Ok, committed.Outcome);
            TxResult after = await StandaloneAsync(history, "W", l => l.Write("/f", "w"), () => v2.Files.WriteAllBytesAsync("/f", Utf8("w"), Ct));
            Assert.Equal(OpOutcome.Ok, after.Outcome);
            Assert.True(after.Token.CompareTo(committed.Token) > 0);
            Assert.Equal("w", await ReadLabelAsync(v2.Files, "/f"));
            await tx.DisposeAsync();
            history.AssertStrictlySerializable(new Dictionary<string, string?> { ["/f"] = "f0" });
        }, Ct);

    /// <summary>
    /// Blind writes (no reads) from a transaction and an ordinary helper: the later commit position decides the winner.
    /// When the ordinary write lands while T is open, strict serializability allows T either to commit (serialized after
    /// it) or to abort with a typed conflict; it may never commit "before" a write it then overwrites in the log.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    [Catalog("TX-011", "S", "INV-04")]
    public Task TX_011_OrdinaryWritesParticipateInConflicts_BlindWritesOrderedByCommitPosition(bool standaloneFirst) =>
        ClusterTest.RunAsync(Sim, async cluster =>
        {
            await using DfsClient c1 = await ConnectAsync(cluster, "c1");
            await using DfsClient c2 = await ConnectAsync(cluster, "c2");
            DfsVolume v1 = c1.GetVolume(Vol), v2 = c2.GetVolume(Vol);
            await v1.Files.WriteAllTextAsync("/f", "f0", null, Ct);
            var history = new TxHistory(cluster.History);

            TxLog t = history.Start("T");
            DfsTransaction tx = await v1.Transactions.BeginAsync(null, Ct);
            TxResult w = standaloneFirst
                ? await StandaloneAsync(history, "W", l => l.Write("/f", "w"), () => v2.Files.WriteAllTextAsync("/f", "w", null, Ct))
                : new TxResult(OpOutcome.Unknown, null, CommitToken.None);
            DfsTransactionConflictException? early = await StageOrEarlyConflictAsync(() => tx.Files.WriteAllTextAsync("/f", "t", null, Ct));
            TxResult tr;
            if (early is null)
            {
                t.Write("/f", "t");
                tr = await CommitAsync(tx, t);
            }
            else
            {
                Assert.True(standaloneFirst, $"Without a concurrent writer T's blind write cannot conflict: {early.Message}");
                tr = await CommitExpectingConflictAsync(tx, t, early, "early conflict with the concurrent blind write");
            }

            if (!standaloneFirst)
                w = await StandaloneAsync(history, "W", l => l.Write("/f", "w"), () => v2.Files.WriteAllTextAsync("/f", "w", null, Ct));

            Assert.Equal(OpOutcome.Ok, w.Outcome);
            string winner;
            if (standaloneFirst && tr.Outcome != OpOutcome.Ok)
            {
                if (early is null)
                    AssertConflict(tr, tx, "a transaction that does not commit after a concurrent blind write must abort with a typed conflict");
                winner = "w";
            }
            else
            {
                // With no concurrent writer (T commits before W begins) there is nothing to conflict with.
                Assert.Equal(OpOutcome.Ok, tr.Outcome);
                winner = tr.Token.CompareTo(w.Token) > 0 ? "t" : "w";
                Assert.Equal(standaloneFirst ? "t" : "w", winner);
            }

            TxRun final = await RunTxAsync(history, "U", v2, async (ro, log) => log.Read("/f", await ReadLabelAsync(ro.Files, "/f")));
            Assert.Equal(OpOutcome.Ok, final.Result.Outcome);
            Assert.Equal(winner, await ReadLabelAsync(v2.Files, "/f"));
            await tx.DisposeAsync();
            history.AssertStrictlySerializable(new Dictionary<string, string?> { ["/f"] = "f0" });
        }, Ct);

    // ------------------------------------------------------------------------------------------------------------
    // TX-016 CommitCollectsRemainingStaging
    // ------------------------------------------------------------------------------------------------------------

    public enum PendingKind { BufferedSmall, UploadedChunksPlusBuffer, PartialOverwriteOfExisting, SetLengthAndHole, FlushedThenPending, MultipleStreamsAndHelper }

    [Theory]
    [InlineData(PendingKind.BufferedSmall)]
    [InlineData(PendingKind.UploadedChunksPlusBuffer)]
    [InlineData(PendingKind.PartialOverwriteOfExisting)]
    [InlineData(PendingKind.SetLengthAndHole)]
    [InlineData(PendingKind.FlushedThenPending)]
    [InlineData(PendingKind.MultipleStreamsAndHelper)]
    [Catalog("TX-016", "S", "INV-01", "INV-02", "INV-07")]
    public Task TX_016_CommitCollectsRemainingStaging(PendingKind kind) =>
        ClusterTest.RunAsync(Sim, async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c1");
            await using DfsClient observer = await ConnectAsync(cluster, "observer");
            DfsVolume v = client.GetVolume(Vol), obs = observer.GetVolume(Vol);
            byte[] e0 = TestData.Bytes(500, 200_000);
            byte[] k0 = TestData.Bytes(501, 100_000);
            await v.Files.WriteAllBytesAsync("/e", e0, Ct);
            await v.Files.WriteAllBytesAsync("/k", k0, Ct);
            ulong kRevision = (await obs.Files.GetInfoAsync("/k", Ct)).Revision;
            ulong eRevision = (await obs.Files.GetInfoAsync("/e", Ct)).Revision;

            var expected = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            var openStreams = new List<DfsStream>();
            bool all = kind == PendingKind.MultipleStreamsAndHelper;
            DfsTransaction tx = await v.Transactions.BeginAsync(null, Ct);
            await using (tx)
            {
                if (kind == PendingKind.BufferedSmall || all)
                {
                    DfsStream s = await tx.Files.OpenAsync("/n1", FileMode.CreateNew, FileAccess.Write, FileShare.Read, Ct);
                    byte[] data = TestData.Bytes(510, 100);
                    await s.WriteAsync(data, Ct);
                    openStreams.Add(s);
                    expected["/n1"] = data;
                }

                if (kind == PendingKind.UploadedChunksPlusBuffer || all)
                {
                    DfsStream s = await tx.Files.OpenAsync("/n2", FileMode.CreateNew, FileAccess.Write, FileShare.Read, Ct);
                    byte[] data = TestData.Bytes(511, 300_007);
                    for (int offset = 0; offset < data.Length; offset += 10_000)
                        await s.WriteAsync(data.AsMemory(offset, Math.Min(10_000, data.Length - offset)), Ct);
                    Assert.Equal(data.Length, s.PendingBytes); // buffered + uploaded-but-unpublished bytes
                    openStreams.Add(s);
                    expected["/n2"] = data;
                }

                if (kind == PendingKind.PartialOverwriteOfExisting || all)
                {
                    var model = new ReferenceFileModel(e0);
                    DfsStream s = await tx.Files.OpenAsync("/e", FileMode.Open, FileAccess.ReadWrite, FileShare.Read, Ct);
                    byte[] p1 = TestData.Bytes(512, 5000);
                    byte[] p2 = TestData.Bytes(513, 100);
                    s.Seek(70_000, SeekOrigin.Begin);
                    await s.WriteAsync(p1, Ct);
                    model.Write(70_000, p1);
                    s.Seek(150_000, SeekOrigin.Begin);
                    await s.WriteAsync(p2, Ct);
                    model.Write(150_000, p2);
                    model.Flush();
                    openStreams.Add(s);
                    expected["/e"] = model.Committed;
                }

                if (kind == PendingKind.SetLengthAndHole)
                {
                    var model = new ReferenceFileModel(k0);
                    DfsStream s = await tx.Files.OpenAsync("/k", FileMode.Open, FileAccess.ReadWrite, FileShare.Read, Ct);
                    await s.SetLengthAsync(30_000, Ct);
                    model.SetLength(30_000);
                    byte[] p = TestData.Bytes(514, 1000);
                    s.Seek(50_000, SeekOrigin.Begin);
                    await s.WriteAsync(p, Ct);
                    model.Write(50_000, p);
                    model.Flush();
                    openStreams.Add(s);
                    expected["/k"] = model.Committed;
                }

                if (kind == PendingKind.FlushedThenPending)
                {
                    DfsStream s = await tx.Files.OpenAsync("/n3", FileMode.CreateNew, FileAccess.Write, FileShare.Read, Ct);
                    byte[] part1 = TestData.Bytes(515, 2000);
                    byte[] part2 = TestData.Bytes(516, 3000);
                    await s.WriteAsync(part1, Ct);
                    await s.FlushAsync(Ct);   // staged for the transaction
                    await s.WriteAsync(part2, Ct); // still pending in the stream
                    openStreams.Add(s);
                    expected["/n3"] = [.. part1, .. part2];
                }

                if (all)
                {
                    await openStreams[0].DisposeAsync();   // /n1: disposal staged it
                    openStreams.RemoveAt(0);
                    byte[] helper = TestData.Bytes(517, 7000);
                    await tx.Files.WriteAllBytesAsync("/h", helper, Ct);
                    expected["/h"] = helper;
                }

                // Nothing is published before the transaction commit.
                foreach (string path in expected.Keys)
                {
                    if (path == "/e")
                        await AssertBytesAsync(e0, obs.Files.ReadAllBytesAsync(path, Ct), "staged changes to /e are private");
                    else if (path == "/k")
                        await AssertBytesAsync(k0, obs.Files.ReadAllBytesAsync(path, Ct), "staged changes to /k are private");
                    else
                        await AssertFileAbsentAsync(obs.Files, path, "staged creations are private");
                }

                // Commit with no stream operation in flight: every pending change is collected atomically.
                CommitToken token = await tx.CommitAsync(Ct);
                Assert.Equal(DfsTransactionState.Committed, tx.State);
                foreach ((string path, byte[] bytes) in expected)
                {
                    await AssertBytesAsync(bytes, obs.Files.ReadAllBytesAsync(path, Ct), $"{path} contains every change staged before commit");
                    DfsFileInfo info = await obs.Files.GetInfoAsync(path, Ct);
                    Assert.Equal(bytes.Length, info.Length);
                    Assert.Equal(token.Sequence, info.Revision);
                }

                if (!expected.ContainsKey("/k"))
                    Assert.Equal(kRevision, (await obs.Files.GetInfoAsync("/k", Ct)).Revision);
                if (!expected.ContainsKey("/e"))
                    Assert.Equal(eRevision, (await obs.Files.GetInfoAsync("/e", Ct)).Revision);

                // Streams left open across the commit are unusable afterwards; disposal stays safe.
                foreach (DfsStream s in openStreams)
                {
                    Exception? late = await CaptureAsync(() => s.WriteAsync(Utf8("late"), Ct).AsTask());
                    Assert.True(late is not null && IsCompletedTransactionRejection(late), $"A bound stream must reject writes after commit; got {late?.GetType().Name ?? "success"}.");
                    await s.DisposeAsync();
                }
            }

            // Durability: the collected data survives loss of the metadata leader (INV-01).
            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            await leader.CrashAsync(CrashMode.LoseUnflushed);
            foreach ((string path, byte[] bytes) in expected)
            {
                byte[] actual = await RetryTransientAsync(cluster, () => obs.Files.ReadAllBytesAsync(path, Ct), $"reading {path} after the leader crashed");
                AssertEx.BytesEqual(bytes, actual, $"{path} after leader crash");
            }

            await leader.RestartAsync();
            await cluster.WaitForReplicationAsync(Vol, Ct);
            await cluster.Inspect.AssertReplicasConvergedAsync(Vol);
        }, Ct);

    // ------------------------------------------------------------------------------------------------------------
    // TX-019 LeaderCrashBeforeDecision / TX-020 LeaderCrashAfterDecision
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>Stage boundaries before the commit decision is durable (F04, F09, F10 before/after local append).</summary>
    public static TheoryData<string> PreDecisionSites() => new()
    {
        FaultSites.RpcRequestAccepted,
        FaultSites.CommitReceiptValidated,
        FaultSites.RaftBeforePropose,
        FaultSites.RaftAfterLocalAppend,
    };

    /// <summary>Stage boundaries after the commit decision is quorum-durable (F11, F12, F13).</summary>
    public static TheoryData<string> PostDecisionSites() => new()
    {
        FaultSites.RaftAfterCommitBeforeApply,
        FaultSites.ApplyAfterBeforeReply,
        FaultSites.RpcReplySend,
    };

    [Theory]
    [MemberData(nameof(PreDecisionSites))]
    [Catalog("TX-019", "S", "INV-01", "INV-02", "INV-06")]
    public Task TX_019_LeaderCrashBeforeDecision(string site) => LeaderCrashDuringCommitAsync(site, decisionDurable: false);

    [Theory]
    [MemberData(nameof(PostDecisionSites))]
    [Catalog("TX-020", "S", "INV-01", "INV-02", "INV-06")]
    public Task TX_020_LeaderCrashAfterDecision(string site) => LeaderCrashDuringCommitAsync(site, decisionDurable: true);

    private static Task LeaderCrashDuringCommitAsync(string site, bool decisionDurable) =>
        ClusterTest.RunAsync(Sim, async cluster =>
        {
            await using DfsClient writer = await ConnectAsync(cluster, "writer");
            await using DfsClient reader = await ConnectAsync(cluster, "reader");
            DfsVolume w = writer.GetVolume(Vol), r = reader.GetVolume(Vol);
            var labels = new ContentLabels();
            byte[] a0 = labels.Add("a0", TestData.Bytes(600, 120_000));
            byte[] a1 = labels.Add("a1", TestData.Bytes(601, 150_000));
            byte[] b0 = labels.Add("b0", TestData.Bytes(602, 64));
            byte[] b1 = labels.Add("b1", TestData.Bytes(603, 64));
            await w.Files.WriteAllBytesAsync("/a", a0, Ct);
            await w.Files.WriteAllBytesAsync("/b", b0, Ct);
            var initial = new Dictionary<string, string?> { ["/a"] = "a0", ["/b"] = "b0" };
            var history = new TxHistory(cluster.History);
            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);

            TxLog t = history.Start("T");
            DfsTransaction tx = await w.Transactions.BeginAsync(null, Ct);
            DfsStream pending = await tx.Files.OpenAsync("/a", FileMode.Open, FileAccess.ReadWrite, FileShare.Read, Ct);
            await pending.SetLengthAsync(0, Ct);
            await pending.WriteAsync(a1, Ct);   // left pending: the commit uploads it and validates its receipts
            await tx.Files.WriteAllBytesAsync("/b", b1, Ct);
            t.Write("/a", "a1").Write("/b", "b1");

            FaultArm arm = cluster.Faults.Arm(
                new FaultMatch(site, Node: leader.Name, Operation: site == FaultSites.CommitReceiptValidated ? null : DfsOperationKind.TxCommit),
                FaultAction.CrashNode(CrashMode.LoseUnflushed));

            // Only T's commit is in flight when the fault is armed: the observers below start after the crash fired, so
            // their own read-only commits (TxCommit to the same leader) can never be the operation that trips it.
            Task<TxResult> commit = CommitAsync(tx, t);
            await arm.WaitTriggeredAsync(Liveness);
            cluster.Faults.AssertReached(arm);
            Assert.Equal(TestNodeState.Crashed, leader.State);

            // Safety is checked during the failure and the recovery, not only after convergence.
            var stop = new TaskCompletionSource();
            var observations = new List<string>();
            Task readers = ObserveDuringRecoveryAsync(cluster, r, history, labels, stop.Task, observations);

            TxResult result = await commit;
            switch (result.Outcome)
            {
                case OpOutcome.Ok:
                    Assert.False(result.Token.IsNone);
                    break;
                case OpOutcome.Failed:
                    Assert.False(decisionDurable,
                        $"The commit decision was quorum-durable before the crash at {site}; a definite failure ({result.Error?.GetType().Name}) would misreport a committed transaction as rolled back.");
                    // TransactionAborted after the leader change, or a typed availability failure; the resolution below
                    // must confirm the definite failure was true.
                    Assert.True(result.Error is DfsTransactionConflictException or DfsUnavailableException,
                        $"A definite commit failure must be typed; got {result.Error?.GetType().Name}: {result.Error?.Message}");
                    break;
                default:
                {
                    DfsCommitOutcomeUnknownException unknown = Assert.IsAssignableFrom<DfsCommitOutcomeUnknownException>(result.Error);
                    Assert.Equal(tx.Id, unknown.TransactionId);
                    Assert.False(unknown.OperationId.IsEmpty);
                    Assert.Equal(DfsTransactionState.Unknown, tx.State);
                    break;
                }
            }

            DfsOperationOutcome resolved = await ResolveOutcomeAsync(cluster, r, tx.Id);
            if (result.Outcome == OpOutcome.Ok)
            {
                Assert.Equal(DfsOutcomeStatus.Committed, resolved.Status);
                Assert.Equal(result.Token, resolved.Token);
            }

            if (result.Outcome == OpOutcome.Failed)
                Assert.Equal(DfsOutcomeStatus.Aborted, resolved.Status);
            if (decisionDurable)
                Assert.Equal(DfsOutcomeStatus.Committed, resolved.Status);
            bool committed = resolved.Status == DfsOutcomeStatus.Committed;
            if (result.Outcome == OpOutcome.Unknown)
                t.Resolve(committed ? OpOutcome.Ok : OpOutcome.Failed);

            await cluster.WaitUntilAsync(() => Task.FromResult(observations.Count > 0 || readers.IsCompleted), Liveness,
                "a transactional observer completes a read of /a and /b during the recovery", Ct);
            stop.SetResult();
            await readers;
            Assert.NotEmpty(observations);
            if (!committed)
                Assert.All(observations, pair => Assert.Equal("a0/b0", pair)); // an aborted transaction was never visible

            await AssertBytesAsync(committed ? a1 : a0, RetryTransientAsync(cluster, () => r.Files.ReadAllBytesAsync("/a", Ct), "read /a"), "all of the transaction or none of it");
            await AssertBytesAsync(committed ? b1 : b0, RetryTransientAsync(cluster, () => r.Files.ReadAllBytesAsync("/b", Ct), "read /b"), "all of the transaction or none of it");
            if (committed)
            {
                Assert.Equal(resolved.Token.Sequence, (await r.Files.GetInfoAsync("/a", Ct)).Revision);
                Assert.Equal(resolved.Token.Sequence, (await r.Files.GetInfoAsync("/b", Ct)).Revision);
            }

            // Restart the crashed leader from its durable image: the decision is identical everywhere and final.
            await CaptureAsync(() => pending.DisposeAsync().AsTask());
            await leader.RestartAsync();
            await cluster.WaitForReplicationAsync(Vol, Ct);
            await cluster.Inspect.AssertReplicasConvergedAsync(Vol);
            await cluster.Inspect.AssertNoSplitBrainAsync();
            DfsOperationOutcome again = await RetryTransientAsync(cluster, () => r.Transactions.QueryOutcomeAsync(tx.Id, false, Ct), "re-query after restart");
            Assert.Equal(resolved.Status, again.Status);
            Assert.Equal(resolved.Token, again.Token);

            // An abort request can never reverse a decided outcome.
            await CaptureAsync(() => tx.AbortAsync(Ct));
            Assert.Equal(resolved.Status, (await RetryTransientAsync(cluster, () => r.Transactions.QueryOutcomeAsync(tx.Id, false, Ct), "re-query after abort")).Status);
            await AssertBytesAsync(committed ? a1 : a0, r.Files.ReadAllBytesAsync("/a", Ct), "abort after the decision changes nothing");
            await tx.DisposeAsync();
            history.AssertStrictlySerializable(initial);
        }, Ct);

    private static async Task ObserveDuringRecoveryAsync(TestCluster cluster, DfsVolume volume, TxHistory history, ContentLabels labels, Task stop, List<string> observations)
    {
        int i = 0;
        string? last = null;
        while (!stop.IsCompleted)
        {
            try
            {
                (Dictionary<string, string?> seen, _) = await ObserveAsync(volume, history, $"obs{i++}", ["/a", "/b"], labels);
                string pair = $"{seen["/a"]}/{seen["/b"]}";
                Assert.True(pair is "a0/b0" or "a1/b1", $"Fractured view during leader failure/recovery: {pair}");
                if (last == "a1/b1")
                    Assert.Equal("a1/b1", pair); // once the commit is visible it never disappears
                last = pair;
                observations.Add(pair);
            }
            catch (Exception ex) when (IsTransient(ex) || ex is DfsCommitOutcomeUnknownException)
            {
                // Authority is temporarily unavailable during the election; never a safety violation by itself.
            }

            await cluster.DelayAsync(TimeSpan.FromMilliseconds(50), Ct);
        }
    }

    // ------------------------------------------------------------------------------------------------------------
    // TX-021 RetryDoesNotRerunUserCode
    // ------------------------------------------------------------------------------------------------------------

    public enum RetryForcing { ReadConflict, LeaderChangeAbort, ConnectionResetDuringCommit, DuplicatedCommitRequest }

    [Theory]
    [InlineData(RetryForcing.ReadConflict)]
    [InlineData(RetryForcing.LeaderChangeAbort)]
    [InlineData(RetryForcing.ConnectionResetDuringCommit)]
    [InlineData(RetryForcing.DuplicatedCommitRequest)]
    [Catalog("TX-021", "S", "INV-04", "INV-06")]
    public Task TX_021_RetryDoesNotRerunUserCode(RetryForcing forcing) =>
        ClusterTest.RunAsync(Sim, async cluster =>
        {
            await using DfsClient app = await ConnectAsync(cluster, "app");
            await using DfsClient other = await ConnectAsync(cluster, "other");
            DfsVolume v = app.GetVolume(Vol), o = other.GetVolume(Vol);
            await v.Files.WriteAllTextAsync("/ctr", "10", null, Ct);
            var history = new TxHistory(cluster.History);
            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            var beginFilter = new MessageFilter(From: "client:app", Type: MessageType.TxBegin);
            var commitFilter = new MessageFilter(From: "client:app", Type: MessageType.TxCommit);
            int bodyRuns = 0;
            var sideEffects = new List<string>();   // stands for e-mails, payments, ... performed by the application body

            long beginsBefore = cluster.Network.Log.CountMessages(beginFilter);
            TxLog log = history.Start("app-1");
            DfsTransaction tx = await v.Transactions.BeginAsync(null, Ct);
            // SDK-internal retries of the begin itself (same operation id) are allowed; what matters is that nothing begins
            // again once the application body has run.
            long beginsAfterBegin = cluster.Network.Log.CountMessages(beginFilter);
            Assert.True(beginsAfterBegin > beginsBefore, "BeginAsync sends TxBegin.");
            FaultArm? arm = null;
            MessageRule? duplicate = null;
            TxResult first;
            string? firstRead;
            await using (tx)
            {
                bodyRuns++;
                sideEffects.Add("notify-1");
                firstRead = await ReadLabelAsync(tx.Files, "/ctr");
                log.Read("/ctr", firstRead);
                if (forcing == RetryForcing.ReadConflict)
                {
                    TxResult interfering = await StandaloneAsync(history, "other", l => l.Write("/ctr", "100"), () => o.Files.WriteAllTextAsync("/ctr", "100", null, Ct));
                    Assert.Equal(OpOutcome.Ok, interfering.Outcome);
                }

                string next = (int.Parse(firstRead!, CultureInfo.InvariantCulture) + 1).ToString(CultureInfo.InvariantCulture);
                DfsTransactionConflictException? early = await StageOrEarlyConflictAsync(() => tx.Files.WriteAllTextAsync("/ctr", next, null, Ct));
                if (early is not null)
                    Assert.Equal(RetryForcing.ReadConflict, forcing); // only the interfering write can invalidate the read this early
                else
                    log.Write("/ctr", next);
                switch (forcing)
                {
                    case RetryForcing.LeaderChangeAbort:
                        await leader.CrashAsync(CrashMode.LoseUnflushed);
                        await WaitForNewLeaderAsync(cluster, leader.Name);
                        break;
                    case RetryForcing.ConnectionResetDuringCommit:
                        arm = cluster.Faults.Arm(new FaultMatch(FaultSites.RpcReplySend, Node: leader.Name, Operation: DfsOperationKind.TxCommit),
                            FaultAction.Custom(_ =>
                            {
                                cluster.Network.ResetConnections("client:app");
                                return Task.CompletedTask;
                            }));
                        break;
                    case RetryForcing.DuplicatedCommitRequest:
                        duplicate = cluster.Network.DuplicateNext(commitFilter);
                        break;
                }

                first = forcing == RetryForcing.ReadConflict
                    ? await CommitExpectingConflictAsync(tx, log, early, "an ordinary write changed the counter the transaction read")
                    : await CommitAsync(tx, log);
            }

            // The SDK neither re-ran the body nor silently began another transaction to retry it.
            Assert.Equal(1, bodyRuns);
            Assert.Equal(["notify-1"], sideEffects);
            Assert.Equal(beginsAfterBegin, cluster.Network.Log.CountMessages(beginFilter));

            bool firstCommitted;
            switch (forcing)
            {
                case RetryForcing.ReadConflict:
                    Assert.Equal(OpOutcome.Failed, first.Outcome);
                    firstCommitted = false;
                    Assert.Equal("100", await ReadLabelAsync(o.Files, "/ctr"));
                    break;
                case RetryForcing.LeaderChangeAbort:
                {
                    Assert.NotEqual(OpOutcome.Ok, first.Outcome);
                    if (first.Outcome == OpOutcome.Failed)
                    {
                        DfsTransactionConflictException aborted = Assert.IsAssignableFrom<DfsTransactionConflictException>(first.Error);
                        Assert.True(aborted.IsRetryable);
                    }

                    DfsOperationOutcome resolved = await ResolveOutcomeAsync(cluster, o, tx.Id);
                    firstCommitted = resolved.Status == DfsOutcomeStatus.Committed;
                    if (first.Outcome == OpOutcome.Unknown)
                        log.Resolve(firstCommitted ? OpOutcome.Ok : OpOutcome.Failed);
                    Assert.Equal(firstCommitted ? "11" : "10", await RetryTransientAsync(cluster, () => ReadLabelAsync(o.Files, "/ctr"), "read /ctr"));
                    break;
                }

                case RetryForcing.ConnectionResetDuringCommit:
                {
                    cluster.Faults.AssertReached(arm!);
                    Assert.NotEqual(OpOutcome.Failed, first.Outcome);
                    DfsOperationOutcome resolved = await ResolveOutcomeAsync(cluster, o, tx.Id);
                    Assert.Equal(DfsOutcomeStatus.Committed, resolved.Status);
                    if (first.Outcome == OpOutcome.Unknown)
                        log.Resolve(OpOutcome.Ok);
                    firstCommitted = true;
                    Assert.Equal("11", await ReadLabelAsync(o.Files, "/ctr")); // applied exactly once, not 12
                    Assert.Equal(resolved.Token.Sequence, (await o.Files.GetInfoAsync("/ctr", Ct)).Revision);
                    break;
                }

                default:
                {
                    Assert.True(duplicate!.Matched >= 1, "The duplicated commit request was delivered twice.");
                    Assert.Equal(OpOutcome.Ok, first.Outcome);
                    firstCommitted = true;
                    Assert.Equal("11", await ReadLabelAsync(o.Files, "/ctr")); // the duplicate had no second effect
                    Assert.Equal(first.Token.Sequence, (await o.Files.GetInfoAsync("/ctr", Ct)).Revision);
                    Assert.Equal(DfsOutcomeStatus.Committed, (await o.Transactions.QueryOutcomeAsync(tx.Id, false, Ct)).Status);
                    break;
                }
            }

            if (!firstCommitted)
            {
                // Re-running the body is the application's explicit decision; it then sees the current state.
                TxRun retry = await RetryTransientAsync(cluster, () => RunTxAsync(history, $"app-{bodyRuns + 1}", v, async (t2, l2) =>
                {
                    bodyRuns++;
                    sideEffects.Add($"notify-{bodyRuns}");
                    string? current = await ReadLabelAsync(t2.Files, "/ctr");
                    l2.Read("/ctr", current);
                    string next = (int.Parse(current!, CultureInfo.InvariantCulture) + 1).ToString(CultureInfo.InvariantCulture);
                    await t2.Files.WriteAllTextAsync("/ctr", next, null, Ct);
                    l2.Write("/ctr", next);
                }), "the application's explicit retry commits");
                Assert.Equal(OpOutcome.Ok, retry.Result.Outcome);
                Assert.Equal(sideEffects.Count, bodyRuns);
                string expected = forcing == RetryForcing.ReadConflict ? "101" : "11";
                Assert.Equal(expected, await ReadLabelAsync(o.Files, "/ctr"));
            }

            if (forcing == RetryForcing.LeaderChangeAbort)
                await leader.RestartAsync();
            history.AssertStrictlySerializable(new Dictionary<string, string?> { ["/ctr"] = "10" });
        }, Ct);

    // ------------------------------------------------------------------------------------------------------------
    // TX-024 SnapshotNeverSplitsTransaction
    // ------------------------------------------------------------------------------------------------------------

    public enum SnapshotRace { SnapshotBeforeCommit, SnapshotWhileCommitHeldBeforePropose, SnapshotWhileCommitHeldBeforeApply, SnapshotAfterCommit, ConcurrentRounds }

    [Theory]
    [InlineData(SnapshotRace.SnapshotBeforeCommit)]
    [InlineData(SnapshotRace.SnapshotWhileCommitHeldBeforePropose)]
    [InlineData(SnapshotRace.SnapshotWhileCommitHeldBeforeApply)]
    [InlineData(SnapshotRace.SnapshotAfterCommit)]
    [InlineData(SnapshotRace.ConcurrentRounds)]
    [Catalog("TX-024", "S", "INV-02", "INV-04", "INV-09")]
    public Task TX_024_SnapshotNeverSplitsTransaction(SnapshotRace race) =>
        ClusterTest.RunAsync(Sim, async cluster =>
        {
            await using DfsClient writer = await ConnectAsync(cluster, "writer");
            await using DfsClient snapper = await ConnectAsync(cluster, "snapper");
            await using DfsClient reader = await ConnectAsync(cluster, "reader");
            DfsVolume w = writer.GetVolume(Vol), s = snapper.GetVolume(Vol), r = reader.GetVolume(Vol);
            var labels = new ContentLabels();
            int rounds = race == SnapshotRace.ConcurrentRounds ? 6 : 1;
            for (int g = 0; g <= rounds; g++)
            {
                labels.Add($"a{g}", TestData.Bytes((ulong)(700 + g), 100_000));
                labels.Add($"b{g}", TestData.Bytes((ulong)(800 + g), 32));
            }

            byte[] A(int g) => TestData.Bytes((ulong)(700 + g), 100_000);
            byte[] B(int g) => TestData.Bytes((ulong)(800 + g), 32);
            await w.Files.WriteAllBytesAsync("/a", A(0), Ct);
            await w.Files.WriteAllBytesAsync("/b", B(0), Ct);

            var taken = new List<(DfsSnapshotInfo Info, CommitToken TxToken, int Gen)>();
            for (int g = 1; g <= rounds; g++)
            {
                DfsTransaction tx = await w.Transactions.BeginAsync(null, Ct);
                await tx.Files.WriteAllBytesAsync("/a", A(g), Ct);
                await tx.Files.WriteAllBytesAsync("/b", B(g), Ct);
                string name = $"s{g}";
                DfsSnapshotInfo info;
                CommitToken token;
                switch (race)
                {
                    case SnapshotRace.SnapshotBeforeCommit:
                        info = await s.Snapshots.CreateAsync(name, null, Ct);
                        token = await tx.CommitAsync(Ct);
                        Assert.True(info.Boundary.CompareTo(token) < 0);
                        break;
                    case SnapshotRace.SnapshotAfterCommit:
                        token = await tx.CommitAsync(Ct);
                        info = await s.Snapshots.CreateAsync(name, null, Ct);
                        Assert.True(info.Boundary.CompareTo(token) >= 0);
                        break;
                    case SnapshotRace.SnapshotWhileCommitHeldBeforePropose:
                    case SnapshotRace.SnapshotWhileCommitHeldBeforeApply:
                    {
                        string site = race == SnapshotRace.SnapshotWhileCommitHeldBeforePropose ? FaultSites.RaftBeforePropose : FaultSites.RaftAfterCommitBeforeApply;
                        TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
                        FaultArm arm = cluster.Faults.Arm(new FaultMatch(site, Node: leader.Name, Operation: DfsOperationKind.TxCommit), FaultAction.Block());
                        Task<CommitToken> commit = tx.CommitAsync(Ct);
                        await arm.WaitTriggeredAsync(Liveness);
                        Task<DfsSnapshotInfo> snapshot = s.Snapshots.CreateAsync(name, null, Ct);
                        await cluster.DelayAsync(TimeSpan.FromMilliseconds(200), Ct); // the snapshot races the held commit
                        arm.Release();
                        token = await commit;
                        info = await snapshot;
                        cluster.Faults.AssertReached(arm);
                        if (race == SnapshotRace.SnapshotWhileCommitHeldBeforeApply)
                            Assert.True(info.Boundary.CompareTo(token) >= 0, "A snapshot requested after the transaction's entry was quorum-committed must include it.");
                        break;
                    }

                    default:
                    {
                        await cluster.DelayAsync(TimeSpan.FromMilliseconds(7 * g), Ct);
                        Task<CommitToken> commit = tx.CommitAsync(Ct);
                        if (g % 2 == 0)
                            await cluster.DelayAsync(TimeSpan.FromMilliseconds(3 * g), Ct);
                        Task<DfsSnapshotInfo> snapshot = s.Snapshots.CreateAsync(name, null, Ct);
                        await Task.WhenAll(commit, snapshot);
                        token = await commit;
                        info = await snapshot;
                        break;
                    }
                }

                await tx.DisposeAsync();
                taken.Add((info, token, g));
                await AssertSnapshotHasWholeTransactionAsync(r, labels, info, token, g);
            }

            // Later overwrites never change retained snapshots (their objects stay protected).
            for (int k = 0; k < 2; k++)
            {
                await w.Files.WriteAllBytesAsync("/a", TestData.Bytes((ulong)(900 + k), 1000), Ct);
                await w.Files.WriteAllBytesAsync("/b", TestData.Bytes((ulong)(950 + k), 10), Ct);
            }

            foreach ((DfsSnapshotInfo info, CommitToken token, int gen) in taken)
                await AssertSnapshotHasWholeTransactionAsync(r, labels, info, token, gen);
        }, Ct);

    private static async Task AssertSnapshotHasWholeTransactionAsync(DfsVolume volume, ContentLabels labels, DfsSnapshotInfo info, CommitToken txToken, int gen)
    {
        Assert.True(info.Boundary.IsComparableWith(txToken), "Snapshot boundaries and commit tokens of one volume epoch are comparable.");
        Assert.Equal(DfsConsistencyLabel.FilesystemConsistent, info.Label);
        bool includes = info.Boundary.CompareTo(txToken) >= 0;
        int expected = includes ? gen : gen - 1;
        await using DfsSnapshotView view = await volume.Snapshots.OpenAsync(info.Name, Ct);
        string? a = await ReadLabelAsync(view.Files, "/a", labels);
        string? b = await ReadLabelAsync(view.Files, "/b", labels);
        Assert.True(a == $"a{expected}" && b == $"b{expected}",
            $"Snapshot {info.Name} (boundary {info.Boundary}, transaction {txToken}) must contain all or none of generation {gen}: saw {a}/{b}, expected a{expected}/b{expected}.");
    }

    // ------------------------------------------------------------------------------------------------------------
    // TX-025 AtomicCommitAndSnapshot
    // ------------------------------------------------------------------------------------------------------------

    public enum ExactSnapshotSource { Transaction, StandaloneStream, WriteAllHelper }

    [Theory]
    [InlineData(ExactSnapshotSource.Transaction)]
    [InlineData(ExactSnapshotSource.StandaloneStream)]
    [InlineData(ExactSnapshotSource.WriteAllHelper)]
    [Catalog("TX-025", "S", "INV-01", "INV-02", "INV-09")]
    public Task TX_025_AtomicCommitAndSnapshot(ExactSnapshotSource source) =>
        ClusterTest.RunAsync(Sim, async cluster =>
        {
            await using DfsClient committer = await ConnectAsync(cluster, "c1");
            await using DfsClient overwriterClient = await ConnectAsync(cluster, "c2");
            await using DfsClient observer = await ConnectAsync(cluster, "observer");
            DfsVolume v = committer.GetVolume(Vol), o = overwriterClient.GetVolume(Vol), obs = observer.GetVolume(Vol);
            var labels = new ContentLabels();
            byte[] a0 = labels.Add("a0", TestData.Bytes(1100, 90_000));
            byte[] a1 = labels.Add("a1", TestData.Bytes(1101, 95_000));
            byte[] b0 = labels.Add("b0", TestData.Bytes(1102, 40));
            byte[] b1 = labels.Add("b1", TestData.Bytes(1103, 40));
            for (int k = 1; k <= 3; k++)
                labels.Add($"late{k}", TestData.Bytes((ulong)(1110 + k), 500));
            await v.Files.WriteAllBytesAsync("/a", a0, Ct);
            await v.Files.WriteAllBytesAsync("/b", b0, Ct);
            VolumeId volumeId = (await v.GetInfoAsync(Ct)).Id;

            // A concurrent writer keeps replacing /a, retrying while the committer holds the writer reservation.
            var start = new TaskCompletionSource();
            var overwrites = new List<(string Label, CommitToken Token)>();
            async Task OverwriteAsync()
            {
                await start.Task;
                for (int k = 1; k <= 3; k++)
                {
                    while (true)
                    {
                        Exception? ex = null;
                        try
                        {
                            overwrites.Add(($"late{k}", await o.Files.WriteAllBytesAsync("/a", TestData.Bytes((ulong)(1110 + k), 500), Ct)));
                        }
                        catch (DfsSharingViolationException e)
                        {
                            ex = e;
                        }

                        if (ex is null)
                            break;
                        await cluster.DelayAsync(TimeSpan.FromMilliseconds(20), Ct);
                    }
                }
            }

            Task overwriter = OverwriteAsync();
            CommitToken token;
            DfsSnapshotInfo exact;
            switch (source)
            {
                case ExactSnapshotSource.Transaction:
                {
                    DfsTransaction tx = await v.Transactions.BeginAsync(null, Ct);
                    await using (tx)
                    {
                        await tx.Files.WriteAllBytesAsync("/a", a1, Ct);
                        await tx.Files.WriteAllBytesAsync("/b", b1, Ct);
                        start.SetResult();
                        exact = await tx.CommitWithSnapshotAsync("exact", Ct);
                        Assert.Equal(DfsTransactionState.Committed, tx.State);
                    }

                    DfsOperationOutcome outcome = await obs.Transactions.QueryOutcomeAsync(tx.Id, false, Ct);
                    Assert.Equal(DfsOutcomeStatus.Committed, outcome.Status);
                    token = outcome.Token;
                    break;
                }

                case ExactSnapshotSource.StandaloneStream:
                {
                    await using (DfsStream stream = await v.Files.OpenAsync("/a", FileMode.Open, FileAccess.ReadWrite, FileShare.None, Ct))
                    {
                        await stream.SetLengthAsync(0, Ct);
                        await stream.WriteAsync(a1, Ct);
                        start.SetResult();
                        token = await stream.CommitAsync(new DfsMutationOptions { CommitSnapshotName = "exact" }, Ct);
                    }

                    exact = await obs.Snapshots.GetAsync("exact", Ct);
                    break;
                }

                default:
                {
                    start.SetResult();
                    // The overwriter now races this helper for /a's short implicit writer reservation; a refused attempt is
                    // retried explicitly (it published nothing and created no snapshot).
                    CommitToken? committed = null;
                    for (int attempt = 0; committed is null; attempt++)
                    {
                        Assert.True(attempt < 50, "The commit-and-snapshot helper never obtained /a's writer reservation.");
                        try
                        {
                            committed = await v.Files.WriteAllBytesAsync("/a", a1, new DfsMutationOptions { CommitSnapshotName = "exact" }, Ct);
                        }
                        catch (DfsSharingViolationException)
                        {
                            Assert.DoesNotContain("exact", (await obs.Snapshots.ListAsync(Ct)).Select(i => i.Name));
                            await cluster.DelayAsync(TimeSpan.FromMilliseconds(13), Ct);
                        }
                    }

                    token = committed.Value;
                    exact = await obs.Snapshots.GetAsync("exact", Ct);
                    break;
                }
            }

            await overwriter;
            Assert.Equal(3, overwrites.Count);

            // The snapshot was created in the same decision as the commit: same boundary, exact contents.
            Assert.Equal("exact", exact.Name);
            Assert.Equal(DfsConsistencyLabel.FilesystemConsistent, exact.Label);
            Assert.Equal(volumeId, exact.Boundary.VolumeId);
            Assert.Equal(token, exact.Boundary);
            await AssertExactSnapshotAsync(obs, labels, "exact", source == ExactSnapshotSource.Transaction ? "b1" : "b0");

            // Live /a is whatever serialized last; the exact snapshot is unaffected by the later overwrites.
            string expectedLive = overwrites.Where(x => x.Token.CompareTo(token) > 0).Select(x => x.Label).LastOrDefault() ?? "a1";
            Assert.Equal(expectedLive, await ReadLabelAsync(obs.Files, "/a", labels));

            // Contrast: a later snapshot with MinimumCommit = token is "at least" that boundary and may include later changes.
            DfsSnapshotInfo atLeast = await obs.Snapshots.CreateAsync("at-least", new DfsSnapshotOptions { MinimumCommit = token }, Ct);
            Assert.True(atLeast.Boundary.IsAtOrAfter(token));
            await using (DfsSnapshotView view = await obs.Snapshots.OpenAsync("at-least", Ct))
                Assert.Equal(expectedLive, await ReadLabelAsync(view.Files, "/a", labels));

            // The exact recovery point is durable across loss of the metadata leader.
            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            await leader.CrashAsync(CrashMode.LoseUnflushed);
            await RetryTransientAsync(cluster, async () =>
            {
                await AssertExactSnapshotAsync(obs, labels, "exact", source == ExactSnapshotSource.Transaction ? "b1" : "b0");
                return true;
            }, "the exact snapshot is readable after the leader crash");
            await leader.RestartAsync();
        }, Ct);

    private static async Task AssertExactSnapshotAsync(DfsVolume volume, ContentLabels labels, string name, string expectedB)
    {
        await using DfsSnapshotView view = await volume.Snapshots.OpenAsync(name, Ct);
        Assert.Equal("a1", await ReadLabelAsync(view.Files, "/a", labels));
        Assert.Equal(expectedB, await ReadLabelAsync(view.Files, "/b", labels));
    }

    /// <summary>If the snapshot half of a commit-and-snapshot decision cannot happen, the commit does not happen either.</summary>
    [Fact]
    [Catalog("TX-025", "S", "INV-01", "INV-02", "INV-09")]
    public Task TX_025_AtomicCommitAndSnapshot_DuplicateNameFailsWholeDecision() =>
        ClusterTest.RunAsync(Sim, async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c1");
            await using DfsClient observer = await ConnectAsync(cluster, "observer");
            DfsVolume v = client.GetVolume(Vol), obs = observer.GetVolume(Vol);
            await v.Files.WriteAllTextAsync("/a", "a0", null, Ct);
            DfsSnapshotInfo existing = await v.Snapshots.CreateAsync("dup", null, Ct);

            DfsTransaction tx = await v.Transactions.BeginAsync(null, Ct);
            await using (tx)
            {
                await tx.Files.WriteAllTextAsync("/a", "a1", null, Ct);
                Exception? ex = await CaptureAsync(() => tx.CommitWithSnapshotAsync("dup", Ct));
                DfsAlreadyExistsException dup = Assert.IsAssignableFrom<DfsAlreadyExistsException>(ex);
                Assert.Equal(DfsErrorCode.AlreadyExists, dup.ErrorCode);
                Assert.NotEqual(DfsTransactionState.Committed, tx.State);
            }

            Assert.Equal("a0", await ReadLabelAsync(obs.Files, "/a"));
            DfsOperationOutcome outcome = await ResolveOutcomeAsync(cluster, obs, tx.Id);
            Assert.Equal(DfsOutcomeStatus.Aborted, outcome.Status);
            Assert.Equal(existing.Boundary, (await obs.Snapshots.GetAsync("dup", Ct)).Boundary);
            await using DfsSnapshotView view = await obs.Snapshots.OpenAsync("dup", Ct);
            Assert.Equal("a0", await ReadLabelAsync(view.Files, "/a"));
        }, Ct);

    // ------------------------------------------------------------------------------------------------------------
    // TX-026 UnknownCommitCannotBeUndoneByDispose
    // ------------------------------------------------------------------------------------------------------------

    public enum ReplyLoss { IsolateClientAfterApply, NetworkDropsCommitReplies, FaultDropsCommitReplies }

    public enum UnknownFollowUp { DisposeAsync, AbortAsync }

    public static TheoryData<ReplyLoss, UnknownFollowUp> ReplyLossCases()
    {
        var data = new TheoryData<ReplyLoss, UnknownFollowUp>();
        foreach (ReplyLoss loss in Enum.GetValues<ReplyLoss>())
        {
            foreach (UnknownFollowUp followUp in Enum.GetValues<UnknownFollowUp>())
                data.Add(loss, followUp);
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(ReplyLossCases))]
    [Catalog("TX-026", "S", "INV-01", "INV-06")]
    public Task TX_026_UnknownCommitCannotBeUndoneByDispose(ReplyLoss loss, UnknownFollowUp followUp) =>
        ClusterTest.RunAsync(Sim, async cluster =>
        {
            await using DfsClient client = await ConnectAsync(cluster, "c1", null, TimeSpan.FromSeconds(3));
            await using DfsClient observer = await ConnectAsync(cluster, "observer");
            DfsVolume v = client.GetVolume(Vol), obs = observer.GetVolume(Vol);
            await v.Files.WriteAllTextAsync("/a", "a0", null, Ct);
            await v.Files.WriteAllTextAsync("/b", "b0", null, Ct);
            var history = new TxHistory(cluster.History);
            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);

            TxLog t = history.Start("T");
            DfsTransaction tx = await v.Transactions.BeginAsync(null, Ct);
            await tx.Files.WriteAllTextAsync("/a", "a1", null, Ct);
            await tx.Files.WriteAllTextAsync("/b", "b1", null, Ct);
            t.Write("/a", "a1").Write("/b", "b1");

            FaultArm? arm = null;
            MessageRule? drops = null;
            switch (loss)
            {
                case ReplyLoss.IsolateClientAfterApply:
                    arm = cluster.Faults.Arm(new FaultMatch(FaultSites.ApplyAfterBeforeReply, Node: leader.Name, Operation: DfsOperationKind.TxCommit),
                        FaultAction.Custom(_ =>
                        {
                            cluster.Network.Isolate("client:c1");
                            return Task.CompletedTask;
                        }));
                    break;
                case ReplyLoss.NetworkDropsCommitReplies:
                    drops = cluster.Network.DropReplies(new MessageFilter(From: "client:c1", Type: MessageType.TxCommit), 1000);
                    break;
                case ReplyLoss.FaultDropsCommitReplies:
                    arm = cluster.Faults.Arm(new FaultMatch(FaultSites.RpcReplySend, Node: leader.Name, Operation: DfsOperationKind.TxCommit, Count: 1000), FaultAction.DropReply());
                    break;
            }

            TxResult result = await CommitAsync(tx, t);
            if (arm is not null)
                cluster.Faults.AssertReached(arm);
            if (drops is not null)
                Assert.True(drops.Matched >= 1, "At least one commit reply was dropped.");
            Assert.True(result.Outcome != OpOutcome.Failed,
                $"The commit was applied before its reply was lost; reporting a definite failure ({result.Error?.GetType().Name}) misrepresents uncertainty as rollback.");
            if (result.Outcome == OpOutcome.Unknown)
            {
                DfsCommitOutcomeUnknownException unknown = Assert.IsAssignableFrom<DfsCommitOutcomeUnknownException>(result.Error);
                Assert.Equal(tx.Id, unknown.TransactionId);
                Assert.Equal(DfsTransactionState.Unknown, tx.State);
            }

            // Dispose/abort while the client does not know the outcome: never a rollback, never reported as aborted.
            Exception? followUpError = followUp == UnknownFollowUp.DisposeAsync
                ? await CaptureAsync(() => tx.DisposeAsync().AsTask())
                : await CaptureAsync(() => tx.AbortAsync(Ct));
            // While the client is still cut off, the abort request itself may fail (typed I/O failure or timeout); what it
            // must never do is report or cause a rollback.
            Assert.True(followUpError is null or InvalidOperationException or IOException or TimeoutException or OperationCanceledException,
                $"Unexpected {followUpError?.GetType().Name} from {followUp} after an unknown commit outcome.");
            Assert.NotEqual(DfsTransactionState.Aborted, tx.State);

            cluster.Network.Heal();
            drops?.Cancel();
            cluster.Faults.DisarmAll();

            DfsOperationOutcome resolved = await ResolveOutcomeAsync(cluster, obs, tx.Id);
            Assert.Equal(DfsOutcomeStatus.Committed, resolved.Status);
            if (result.Outcome == OpOutcome.Ok)
                Assert.Equal(result.Token, resolved.Token);
            else
                t.Resolve(OpOutcome.Ok);
            Assert.Equal(("a1", "b1"), (await ReadLabelAsync(obs.Files, "/a"), await ReadLabelAsync(obs.Files, "/b")));
            Assert.Equal(resolved.Token.Sequence, (await obs.Files.GetInfoAsync("/a", Ct)).Revision);
            Assert.Equal(resolved.Token.Sequence, (await obs.Files.GetInfoAsync("/b", Ct)).Revision);

            // The original client resolves the same decision by transaction id; a late abort still cannot reverse it.
            DfsOperationOutcome fromOrigin = await RetryTransientAsync(cluster, () => v.Transactions.QueryOutcomeAsync(tx.Id, false, Ct), "origin re-queries the outcome");
            Assert.Equal(DfsOutcomeStatus.Committed, fromOrigin.Status);
            Assert.Equal(resolved.Token, fromOrigin.Token);
            await CaptureAsync(() => tx.AbortAsync(Ct));
            Assert.NotEqual(DfsTransactionState.Aborted, tx.State);
            Assert.Equal(DfsOutcomeStatus.Committed, (await obs.Transactions.QueryOutcomeAsync(tx.Id, false, Ct)).Status);
            Assert.Equal("a1", await ReadLabelAsync(obs.Files, "/a"));

            // The decision is durable: it survives a crash of the current leader.
            TestNode current = await cluster.GetLeaderAsync(Vol, Ct);
            await current.CrashAsync(CrashMode.LoseUnflushed);
            DfsOperationOutcome afterCrash = await RetryTransientAsync(cluster, () => obs.Transactions.QueryOutcomeAsync(tx.Id, false, Ct), "query after leader crash");
            Assert.Equal(DfsOutcomeStatus.Committed, afterCrash.Status);
            Assert.Equal(resolved.Token, afterCrash.Token);
            Assert.Equal("b1", await RetryTransientAsync(cluster, () => ReadLabelAsync(obs.Files, "/b"), "read /b after leader crash"));
            await current.RestartAsync();
            await CaptureAsync(() => tx.DisposeAsync().AsTask());
            history.AssertStrictlySerializable(new Dictionary<string, string?> { ["/a"] = "a0", ["/b"] = "b0" });
        }, Ct);

    // ------------------------------------------------------------------------------------------------------------
    // TX-027 ReadValidationAfterStagingDelay
    // ------------------------------------------------------------------------------------------------------------

    public enum CommitPause { ClientBeforeSend, LeaderAcceptedRequest, LeaderBeforePropose, ClientSideDelay }

    public enum ConflictingChange { CompetingTransaction, StandaloneWithObserver }

    public static TheoryData<CommitPause, ConflictingChange> StagingDelayCases()
    {
        var data = new TheoryData<CommitPause, ConflictingChange>();
        foreach (CommitPause pause in Enum.GetValues<CommitPause>())
        {
            foreach (ConflictingChange change in Enum.GetValues<ConflictingChange>())
                data.Add(pause, change);
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(StagingDelayCases))]
    [Catalog("TX-027", "S", "INV-04")]
    public Task TX_027_ReadValidationAfterStagingDelay(CommitPause pause, ConflictingChange change) =>
        ClusterTest.RunAsync(Sim, async cluster =>
        {
            await using DfsClient c1 = await ConnectAsync(cluster, "c1");
            await using DfsClient c2 = await ConnectAsync(cluster, "c2");
            await using DfsClient c3 = await ConnectAsync(cluster, "c3");
            DfsVolume v1 = c1.GetVolume(Vol), v2 = c2.GetVolume(Vol), v3 = c3.GetVolume(Vol);
            var labels = new ContentLabels();
            byte[] staged = labels.Add("c1", TestData.Bytes(1200, 150_000));
            labels.Add("0", Utf8("0"));
            labels.Add("1", Utf8("1"));
            await v1.Files.WriteAllTextAsync("/a", "1", null, Ct);
            await v1.Files.WriteAllTextAsync("/b", "1", null, Ct);
            var history = new TxHistory(cluster.History);
            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);

            // T reads its dependencies, then stages a write and a large upload (staging takes time).
            TxLog t = history.Start("T");
            DfsTransaction tx = await v1.Transactions.BeginAsync(null, Ct);
            t.Read("/a", await ReadLabelAsync(tx.Files, "/a", labels)).Read("/b", await ReadLabelAsync(tx.Files, "/b", labels));
            await tx.Files.WriteAllTextAsync("/a", "0", null, Ct);
            await using (DfsStream s = await tx.Files.OpenAsync("/c", FileMode.CreateNew, FileAccess.Write, FileShare.Read, Ct))
            {
                await s.WriteAsync(staged, Ct);
                await s.FlushAsync(Ct);
            }

            t.Write("/a", "0").Write("/c", "c1");

            TxLog? l2 = null;
            DfsTransaction? t2 = null;
            if (change == ConflictingChange.CompetingTransaction)
            {
                l2 = history.Start("T2");
                t2 = await v2.Transactions.BeginAsync(null, Ct);
                l2.Read("/a", await ReadLabelAsync(t2.Files, "/a", labels)).Read("/b", await ReadLabelAsync(t2.Files, "/b", labels));
                await t2.Files.WriteAllTextAsync("/b", "0", null, Ct);
                l2.Write("/b", "0");
            }

            FaultArm? arm = pause switch
            {
                CommitPause.ClientBeforeSend => cluster.Faults.Arm(new FaultMatch(FaultSites.ClientRequestSend, Operation: DfsOperationKind.TxCommit), FaultAction.Block()),
                CommitPause.LeaderAcceptedRequest => cluster.Faults.Arm(new FaultMatch(FaultSites.RpcRequestAccepted, Node: leader.Name, Operation: DfsOperationKind.TxCommit), FaultAction.Block()),
                CommitPause.LeaderBeforePropose => cluster.Faults.Arm(new FaultMatch(FaultSites.RaftBeforePropose, Node: leader.Name, Operation: DfsOperationKind.TxCommit), FaultAction.Block()),
                _ => null,
            };

            Task<TxResult>? pausedCommit = null;
            if (arm is not null)
            {
                pausedCommit = CommitAsync(tx, t);
                await arm.WaitTriggeredAsync(Liveness);
            }
            else
            {
                await cluster.DelayAsync(TimeSpan.FromSeconds(1.5), Ct);
            }

            // The conflicting change commits while T is paused.
            if (change == ConflictingChange.CompetingTransaction)
            {
                Assert.Equal(OpOutcome.Ok, (await CommitAsync(t2!, l2!)).Outcome);
            }
            else
            {
                Assert.Equal(OpOutcome.Ok, (await StandaloneAsync(history, "W", l => l.Write("/b", "0"), () => v2.Files.WriteAllTextAsync("/b", "0", null, Ct))).Outcome);
                TxRun u = await RunTxAsync(history, "U", v3, async (ro, log) =>
                {
                    log.Read("/a", await ReadLabelAsync(ro.Files, "/a", labels));
                    log.Read("/b", await ReadLabelAsync(ro.Files, "/b", labels));
                    log.Read("/c", await ReadLabelAsync(ro.Files, "/c", labels));
                });
                Assert.Equal(OpOutcome.Ok, u.Result.Outcome);
            }

            TxResult result;
            if (arm is not null)
            {
                arm.Release();
                result = await pausedCommit!;
                cluster.Faults.AssertReached(arm);
            }
            else
            {
                await cluster.DelayAsync(TimeSpan.FromSeconds(1.5), Ct);
                result = await CommitAsync(tx, t);
            }

            AssertConflict(result, tx, $"T's read of /b changed while T was paused ({pause}); late validation must detect it");
            Assert.Equal("1", await ReadLabelAsync(v3.Files, "/a", labels));
            Assert.Equal("0", await ReadLabelAsync(v3.Files, "/b", labels));
            Assert.False(await v3.Files.ExistsAsync("/c", Ct), "The conflicted transaction's staged upload is never published.");
            await tx.DisposeAsync();
            if (t2 is not null)
                await t2.DisposeAsync();
            history.AssertStrictlySerializable(new Dictionary<string, string?> { ["/a"] = "1", ["/b"] = "1" });
        }, Ct);
}
