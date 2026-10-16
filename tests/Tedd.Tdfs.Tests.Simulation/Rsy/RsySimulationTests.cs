using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Transport;
using static Tedd.Tdfs.Tests.Simulation.Rsy.RsyHelpers;

namespace Tedd.Tdfs.Tests.Simulation.Rsy;

/// <summary>
/// RSY — resynchronization, repair, and stale-state exclusion (test plan §7.17, critical scenario §9.4; FSD §§9.2, 13,
/// 14.1 RCV-01..RCV-08). Every scenario runs in deterministic simulation with <see cref="NodeTimings.Fast"/> timings
/// (lease 2 s, session 10 s, outcome retention 30 s, Raft snapshot threshold 200 entries, chunk 64 KiB). Checkpoint
/// catch-up is forced by committing more than the snapshot threshold while the returning node ("C") is offline and
/// waiting until every online source's log starts after C's next index. Log-path scenarios raise the snapshot threshold
/// (<see cref="RsyHelpers.WithPath"/>) so background entries during long outages cannot compact C's missing range.
/// </summary>
public sealed class RsySimulationTests
{
    // ------------------------------------------------------------------------------------------------------------
    // RSY-001
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<int, OfflineMode> ShortGapCases => new()
    {
        { 3, OfflineMode.Isolate },
        { 3, OfflineMode.Stop },
        { 3, OfflineMode.Crash },
        { 5, OfflineMode.Isolate },
        { 5, OfflineMode.Crash },
    };

    /// <summary>
    /// A follower returns before its missing range is compacted: it must replay exactly the missing log range (each
    /// index applied once, no checkpoint install) and end with the authoritative state, which it can then serve.
    /// </summary>
    [Theory]
    [MemberData(nameof(ShortGapCases))]
    [Catalog("RSY-001", "S", "INV-01", "INV-07", "INV-18")]
    public Task RSY_001_ShortGapIncrementalCatchup(int nodeCount, OfflineMode mode) =>
        RunAsync(Sim(nodeCount).WithPath(CatchupPath.Log), async cluster =>
        {
            var (client, vol) = await ConnectAsync(cluster, "w");
            await using var clientScope = client;
            DfsFileClient files = vol.Files;
            await MkdirAsync(vol, "/g");
            byte[] base0 = TestData.Bytes(1001, 3 * Chunk + 17);
            await files.WriteAllBytesAsync("/g/base", base0, Ct);
            byte[] log0 = TestData.Bytes(1002, 100);
            await files.WriteAllBytesAsync("/g/log", log0, Ct);

            RsyOutage outage = await BeginOutageAsync(cluster, mode);
            TestNode c = outage.Roles.Returning;

            // Gap workload: a handful of entries, far below the snapshot threshold (raised for log-path scenarios).
            var model = new ReferenceFileModel(base0);
            byte[] patch = TestData.Bytes(1003, 10);
            await OverwriteAsync(files, "/g/base", Chunk - 5, patch);   // straddles the first extent boundary
            model.Write(Chunk - 5, patch);
            model.Flush();
            var expectedLog = new List<byte>(log0);
            for (int i = 0; i < 8; i++)
            {
                byte[] suffix = TestData.Bytes(1100 + (ulong)i, 33 + i);
                await files.AppendAllBytesAsync("/g/log", suffix, Ct);
                expectedLog.AddRange(suffix);
            }

            var small = new byte[4][];
            for (int i = 0; i < small.Length; i++)
            {
                small[i] = TestData.Bytes(1200 + (ulong)i, 200 + i);
                await files.WriteAllBytesAsync($"/g/f{i}", small[i], Ct);
            }

            await files.DeleteAsync("/g/f0", Ct);
            await files.MoveAsync("/g/f1", "/g/f1-moved", false, Ct);
            await MkdirAsync(vol, "/g/sub");

            // Missing range still in every source's log; incremental replay, each index applied exactly once.
            await EndOutageAsync(cluster, outage, CatchupPath.Log, vol, "/fill", 1300);
            await AssertMatchesLeaderAsync(cluster, c, ["/g/base", "/g/log", "/g/f0", "/g/f1", "/g/f1-moved", "/g/f2", "/g/f3"]);
            RaftGroupStats stats = await StatsAsync(c);
            Assert.True(stats.AppliedIndex > outage.ReturningAppliedIndex, "the returning replica applied the missing range");

            // The caught-up replica serves the authoritative state itself.
            await ServeFromAsync(cluster, c);
            await AssertBytesAsync(files, "/g/base", model.Committed, "overwrite replayed from the log");
            await AssertBytesAsync(files, "/g/log", expectedLog.ToArray(), "every append replayed exactly once");
            Assert.False(await files.ExistsAsync("/g/f0", Ct));
            Assert.False(await files.ExistsAsync("/g/f1", Ct));
            await AssertBytesAsync(files, "/g/f1-moved", small[1]);
            await AssertBytesAsync(files, "/g/f2", small[2]);
            await AssertBytesAsync(files, "/g/f3", small[3]);
            Assert.True(await vol.Directories.ExistsAsync("/g/sub", Ct));
        });

    // ------------------------------------------------------------------------------------------------------------
    // RSY-002 — the complete critical scenario of test plan §9.4
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Test plan §9.4 end to end: C (holding old inode I of /x, old mappings, an old request/session window and an old
    /// writer generation) is stopped; A/B overwrite and truncate /x, rename it, delete the renamed inode, recreate /x as
    /// inode J, commit a multi-file transaction, create/delete snapshots, fence the old writer, retire the old operation
    /// ids and compact past C's log. C returns, its first checkpoint install is interrupted by a crash, the retry
    /// completes; A is stopped and service continues through B/C with the authoritative state.
    /// </summary>
    [Theory]
    [InlineData(OfflineMode.Stop)]
    [InlineData(OfflineMode.Crash)]
    [Catalog("RSY-002", "S", "INV-01", "INV-05", "INV-06", "INV-07", "INV-09", "INV-18")]
    public Task RSY_002_LongGapRequiresCheckpoint(OfflineMode mode) =>
        RunAsync(Sim(3), async cluster =>
        {
            var (appClient, app) = await ConnectAsync(cluster, "app");
            await using var appScope = appClient;
            DfsFileClient files = app.Files;
            await MkdirAsync(app, "/keep");

            // ---- State C knows before it goes offline ----
            byte[] x0 = TestData.Bytes(2001, 3 * Chunk + 11);
            await files.WriteAllBytesAsync("/x", x0, Ct);
            InodeId inodeI = (await files.GetInfoAsync("/x", Ct)).InodeId;
            IReadOnlyList<ObjectId> objectsOfI = await ObjectsOfAsync(cluster, "/x");
            byte[] a0 = TestData.Bytes(2002, 300), b0 = TestData.Bytes(2003, Chunk + 9), c0 = TestData.Bytes(2004, 50);
            await files.WriteAllBytesAsync("/keep/a", a0, Ct);
            await files.WriteAllBytesAsync("/keep/b", b0, Ct);
            await files.WriteAllBytesAsync("/keep/c", c0, Ct);
            byte[] w0 = TestData.Bytes(2005, 2 * Chunk);
            await files.WriteAllBytesAsync("/w", w0, Ct);
            byte[] log0 = TestData.Bytes(2006, 64);
            await files.WriteAllBytesAsync("/log", log0, Ct);
            await app.Snapshots.CreateAsync("s0", null, Ct);

            // Old request/session window: a separate client process uses persisted operation ids.
            var (oldClient, old) = await ConnectAsync(cluster, "old");
            DfsOperationId idOld1 = await old.NewOperationIdAsync(Ct);
            byte[] s1 = TestData.Bytes(2007, 40);
            CommitToken tOld1 = await old.Files.AppendAllBytesAsync("/log", s1, new DfsMutationOptions { OperationId = idOld1 }, Ct);
            Assert.False(tOld1.IsNone);
            DfsOperationId idOld2 = await old.NewOperationIdAsync(Ct);
            byte[] r2 = TestData.Bytes(2008, 70);
            await old.Files.WriteAllBytesAsync("/r2", r2, new DfsMutationOptions { OperationId = idOld2 }, Ct);

            // Old writer generation: W-old holds /w with private staged bytes (paused process later).
            var woldClock = new TestClock(cluster.Time);
            var (woldClient, wold) = await ConnectAsync(cluster, "wold", clock: woldClock);
            await using var woldScope = woldClient;
            DfsStream staleWriter = await wold.Files.OpenAsync("/w", FileMode.Open, FileAccess.Write, FileShare.Read, Ct);
            ulong gOld = staleWriter.WriterGeneration;
            Assert.True(gOld > 0, "a writable open is bound to a writer generation");
            await staleWriter.WriteAsync(TestData.Bytes(2009, 500), Ct);   // staged only, never published

            // A reader whose only path into the cluster is C (cut from A/B while C catches up).
            var (readerClient, reader) = await ConnectAsync(cluster, "r", seeds: null, operationTimeout: TimeSpan.FromSeconds(2));
            await using var readerScope = readerClient;

            await cluster.WaitForReplicationAsync(Volume, Ct);
            RsyRoles roles = await PickRolesAsync(cluster);
            VolumeStateInspection cBefore = await StateAsync(roles.Returning);
            Assert.Equal(inodeI, cBefore.GetFile("/x")!.Inode);
            Assert.Contains(cBefore.Snapshots, s => s.Name == "s0");
            RsyOutage outage = await BeginOutageAsync(cluster, mode, roles);
            (TestNode a, TestNode b, TestNode c) = (roles.Leader, roles.Other, roles.Returning);

            // ---- Schedule on A/B while C is offline ----
            woldClock.Freeze();             // W-old's process is paused: no lease renewals
            cluster.KillClient(oldClient);  // the old session's process dies

            byte[] patch = TestData.Bytes(2010, 100);
            await OverwriteAsync(files, "/x", Chunk + 3, patch);
            await ResizeAsync(files, "/x", 1000);
            await files.MoveAsync("/x", "/x-renamed", false, Ct);
            await files.DeleteAsync("/x-renamed", Ct);
            byte[] x2 = TestData.Bytes(2011, 2 * Chunk + 5);
            await files.WriteAllBytesAsync("/x", x2, Ct);
            InodeId inodeJ = (await files.GetInfoAsync("/x", Ct)).InodeId;
            Assert.NotEqual(inodeI, inodeJ);

            byte[] a1 = TestData.Bytes(2012, 310), b1 = TestData.Bytes(2013, Chunk + 19), d1 = TestData.Bytes(2014, 90);
            DfsTransactionId txId;
            CommitToken txToken;
            await using (DfsTransaction tx = await app.Transactions.BeginAsync(null, Ct))
            {
                txId = tx.Id;
                await tx.Files.WriteAllBytesAsync("/keep/a", a1, Ct);
                await tx.Files.WriteAllBytesAsync("/keep/b", b1, Ct);
                await tx.Files.DeleteAsync("/keep/c", Ct);
                await tx.Files.WriteAllBytesAsync("/keep/d", d1, Ct);
                txToken = await tx.CommitAsync(Ct);
            }

            Assert.False(txToken.IsNone);

            DfsSnapshotInfo sA = await app.Snapshots.CreateAsync("sA", null, Ct);
            await app.Snapshots.CreateAsync("sB", null, Ct);
            await app.Snapshots.DeleteAsync("s0", Ct);
            await app.Snapshots.DeleteAsync("sB", Ct);

            // W-old's lease lapses; W-new takes over /w with a higher generation and publishes.
            await cluster.DelayAsync(Timings.LeaseDuration * 2 + TimeSpan.FromSeconds(1), Ct);
            byte[] w1 = TestData.Bytes(2015, Chunk + 77);
            ulong gNew;
            await using (DfsStream writer = await files.OpenAsync("/w", FileMode.Open, FileAccess.Write, FileShare.Read, Ct))
            {
                gNew = writer.WriterGeneration;
                await writer.SetLengthAsync(0, Ct);
                await writer.WriteAsync(w1, Ct);
                await writer.CommitAsync(Ct);
            }

            Assert.True(gNew > gOld, $"the replacement writer generation {gNew} must exceed the expired generation {gOld}");

            // Retire the old ids: the old session expires and its retained outcomes age out.
            await cluster.DelayAsync(Timings.SessionTimeout + Timings.OutcomeRetention + TimeSpan.FromSeconds(5), Ct);
            var (probeClient, probe) = await ConnectAsync(cluster, "probe");
            await using (probeClient)
                Assert.Equal(DfsOutcomeStatus.Expired, (await probe.QueryOperationAsync(idOld1, false, Ct)).Status);

            // Compact the missing metadata history on every source: only checkpoint catch-up is possible.
            await ForceCompactionAsync(cluster, app, RunningExcept(cluster, c), outage.ReturningLastIndex, "/fill", 2100);

            // ---- Return C; interrupt its first checkpoint installation once ----
            FaultArm interrupt = cluster.Faults.Arm(
                new FaultMatch(FaultSites.RaftSnapshotInstall, Node: c.Name, Volume: Volume),
                FaultAction.CrashNode(CrashMode.LoseUnflushed));
            Task firstReturn = ReturnAsync(cluster, c, mode);   // the crash may hit before the restart call returns
            await interrupt.WaitTriggeredAsync(CatchupTimeout);
            cluster.Faults.AssertReached(interrupt);
            await cluster.WaitUntilAsync(() => Task.FromResult(c.State == TestNodeState.Crashed), TimeSpan.FromSeconds(10),
                "the interrupted checkpoint installation crashes the returning node", Ct);
            await AwaitInterruptedRestartAsync(firstReturn, c);

            foreach (TestNode n in RunningExcept(cluster, c))
                cluster.Network.CutBoth("client:r", n.Name);
            await c.RestartAsync();

            // C must not serve stale live reads while it catches up (only /x = J with x2 is acceptable).
            bool stopReading = false;
            int okReads = 0;
            async Task ReadLoopAsync()
            {
                while (!stopReading)
                {
                    try
                    {
                        byte[] got = await reader.Files.ReadAllBytesAsync("/x", Ct);
                        AssertEx.BytesEqual(x2, got, "a live read through the catching-up replica must return the authoritative /x (inode J)");
                        okReads++;
                    }
                    catch (Exception ex) when (IsRefusal(ex))
                    {
                        // Refused while catching up (the reader's session may also lapse while it is cut off): legal.
                        // FileNotFound or old bytes would be a stale read and fail the test.
                    }

                    await cluster.DelayAsync(TimeSpan.FromMilliseconds(50), Ct);
                }
            }

            Task readLoop = ReadLoopAsync();
            await WaitCaughtUpAsync(cluster, c);
            stopReading = true;
            await readLoop;
            cluster.Network.Heal();

            // ---- Checkpoint recovery preserved everything ----
            await AssertCatchupPathAsync(cluster, c, CatchupPath.Checkpoint, before: null);
            RaftGroupStats cStats = await StatsAsync(c);
            Assert.True(cStats.SnapshotIndex > outage.ReturningLastIndex, "C installed a checkpoint covering its compacted gap");
            await AssertMatchesLeaderAsync(cluster, c, ["/x", "/x-renamed", "/keep/a", "/keep/b", "/keep/c", "/keep/d", "/w", "/log", "/r2"]);
            (VolumeStateInspection leaderState, VolumeStateInspection cState) = await StatesAtSameIndexAsync(cluster, c);
            Assert.Equal(inodeJ, cState.GetFile("/x")!.Inode);
            Assert.Null(cState.GetFile("/x-renamed"));
            Assert.Null(cState.GetFile("/keep/c"));
            Assert.NotNull(cState.GetFile("/keep/d"));
            Assert.Equal(new[] { "sA" }, cState.Snapshots.Select(s => s.Name).Order(StringComparer.Ordinal).ToArray());
            Assert.Equal(sA.Id, cState.Snapshots.Single().Id);
            Assert.Equal(leaderState.Snapshots.Single(), cState.Snapshots.Single());
            Assert.True(cState.GetFile("/w")!.WriterGeneration >= gNew, "fencing generation must not be reset by checkpoint recovery");
            SessionInspection? cOldSession = cState.Sessions.SingleOrDefault(s => s.Id == idOld1.Session);
            SessionInspection? leaderOldSession = leaderState.Sessions.SingleOrDefault(s => s.Id == idOld1.Session);
            Assert.Equal(leaderOldSession, cOldSession);
            if (cOldSession is not null)
                Assert.True(cOldSession.Expired || cOldSession.RetiredWatermark >= idOld1.Sequence, "C must retain the old ids' retirement");
            IReadOnlySet<ObjectId> cReachable = cState.ReachableObjects();
            Assert.Equal(leaderState.ReachableObjects().OrderBy(o => o.Value).ToArray(), cReachable.OrderBy(o => o.Value).ToArray());
            Assert.All(objectsOfI, o => Assert.DoesNotContain(o, cReachable));

            // ---- Stop A; service continues through B/C ----
            await a.StopAsync();
            await cluster.GetLeaderAsync(Volume, Ct);
            var (afterClient, after) = await ConnectAsync(cluster, "after", seeds: [b.Name, c.Name]);
            await using var afterScope = afterClient;
            await AssertServiceAsync(after);

            // Old writer resumes: its delayed publication is fenced.
            woldClock.Unfreeze();
            Exception fenced = await Assert.ThrowsAnyAsync<Exception>(() => staleWriter.FlushAsync(Ct));
            Assert.True(fenced is DfsLeaseLostException or DfsOperationExpiredException,
                $"an obsolete writer generation must be rejected (lease lost / session expired), got {fenced.GetType().Name}: {fenced.Message}");
            Assert.True(staleWriter.IsFaulted, "the fenced stream is faulted");
            await AssertBytesAsync(after.Files, "/w", w1, "W-new's committed contents remain authoritative");
            try
            {
                await staleWriter.DisposeAsync();
            }
            catch (Exception ex) when (ex is DfsLeaseLostException or DfsOperationExpiredException)
            {
                // Disposal surfaces the same fault; it never publishes.
            }

            // New work succeeds on B/C.
            byte[] fresh = TestData.Bytes(2016, Chunk + 1);
            await after.Files.WriteAllBytesAsync("/after", fresh, Ct);
            await AssertBytesAsync(after.Files, "/after", fresh);

            // And C itself, as leader, serves the same authoritative state.
            await ServeFromAsync(cluster, c);
            await AssertServiceAsync(after);

            // J's objects were written while C was offline (copies on A/B). With A stopped, repair must give B and C verified
            // copies (two hosts, two zones), and GC rounds must never take a live object below that.
            IReadOnlyList<ObjectId> objectsOfJ = await ObjectsOfAsync(cluster, "/x");
            await cluster.WaitUntilAsync(async () =>
            {
                foreach (ObjectId o in objectsOfJ)
                {
                    (int copies, int zones) = await cluster.Inspect.CountDurableDomainsAsync(o);
                    if (copies < 2 || zones < 2)
                        return false;
                }

                return true;
            }, RepairTimeout, "repair restores two verified copies of J's objects on B and C", Ct);
            await cluster.DelayAsync(Timings.GcInterval * 8, Ct);
            foreach (ObjectId o in objectsOfJ)
            {
                (int copies, int zones) = await cluster.Inspect.CountDurableDomainsAsync(o);
                Assert.True(copies >= 2 && zones >= 2, $"GC removed a live copy: object {o} of /x has {copies} copies in {zones} zones");
            }

            async Task AssertServiceAsync(DfsVolume v)
            {
                await AssertBytesAsync(v.Files, "/x", x2, "/x maps to J");
                Assert.Equal(inodeJ, (await v.Files.GetInfoAsync("/x", Ct)).InodeId);
                Assert.False(await v.Files.ExistsAsync("/x-renamed", Ct), "the deleted renamed inode does not resurrect");
                await AssertBytesAsync(v.Files, "/keep/a", a1);
                await AssertBytesAsync(v.Files, "/keep/b", b1);
                Assert.False(await v.Files.ExistsAsync("/keep/c", Ct));
                await AssertBytesAsync(v.Files, "/keep/d", d1);
                AssertCommittedOrRetired(await v.Transactions.QueryOutcomeAsync(txId, false, Ct), txToken);
                Assert.Equal(new[] { "sA" }, (await v.Snapshots.ListAsync(Ct)).Select(s => s.Name).ToArray());
                await AssertEx.ThrowsDfsAsync<DfsException>(() => v.Snapshots.GetAsync("s0", Ct), DfsErrorCode.SnapshotNotFound);
                await using (DfsSnapshotView view = await v.Snapshots.OpenAsync("sA", Ct))
                {
                    await AssertBytesAsync(view.Files, "/x", x2, "snapshot sA references J");
                    await AssertBytesAsync(view.Files, "/keep/a", a1, "snapshot sA contains the complete transaction");
                }

                // Replay rejection survives checkpoint recovery: the retired id is not executed again.
                await AssertEx.ThrowsDfsAsync<DfsOperationExpiredException>(
                    () => v.Files.AppendAllBytesAsync("/log", s1, new DfsMutationOptions { OperationId = idOld1 }, Ct),
                    DfsErrorCode.OperationExpired);
                await AssertEx.ThrowsDfsAsync<DfsOperationExpiredException>(
                    () => v.Files.WriteAllBytesAsync("/r2", r2, new DfsMutationOptions { OperationId = idOld2 }, Ct),
                    DfsErrorCode.OperationExpired);
                Assert.Equal(DfsOutcomeStatus.Expired, (await v.QueryOperationAsync(idOld1, false, Ct)).Status);
                await AssertBytesAsync(v.Files, "/log", Concat(log0, s1), "the old append took effect exactly once");
            }
        });

    // ------------------------------------------------------------------------------------------------------------
    // RSY-003
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<OfflineMode, StaleWindow, bool> StaleReadCases => new()
    {
        { OfflineMode.Isolate, StaleWindow.MinorityPartition, true },
        { OfflineMode.Stop, StaleWindow.HoldAppends, true },
        { OfflineMode.Stop, StaleWindow.HoldAppends, false },
        { OfflineMode.Crash, StaleWindow.BlockApply, true },
        { OfflineMode.Crash, StaleWindow.BlockApply, false },
        { OfflineMode.Isolate, StaleWindow.BlockApply, false },
    };

    /// <summary>
    /// The returning replica holds stale state and its catch-up/apply is delayed; a reader that enters the cluster
    /// through it must never observe a stale value for a new live read (history checked for linearizability). Once the
    /// replica is current, reads through it succeed with the current value.
    /// </summary>
    [Theory]
    [MemberData(nameof(StaleReadCases))]
    [Catalog("RSY-003", "S", "INV-07", "INV-18")]
    public Task RSY_003_OfflineReplicaNeverServesNewLiveReadsEarly(OfflineMode mode, StaleWindow window, bool readerOnlyReachesReturningNode) =>
        RunAsync(Sim(3).WithPath(CatchupPath.Log), async cluster =>   // log catch-up: the apply block sees every missing entry
        {
            RsyRoles roles = await PickRolesAsync(cluster);
            TestNode c = roles.Returning;
            var (wClient, w) = await ConnectAsync(cluster, "w");
            await using var wScope = wClient;
            var (rClient, r) = await ConnectAsync(cluster, "r", seeds: [c.Name], operationTimeout: TimeSpan.FromSeconds(2));
            await using var rScope = rClient;
            const string readerName = "client:r";

            byte[] v0 = TestData.Bytes(3000, 200);
            await w.Files.WriteAllBytesAsync("/x", v0, Ct);
            await cluster.WaitForReplicationAsync(Volume, Ct);
            HistoryRecorder history = cluster.History;
            int version = 0;
            byte[] latest = v0;
            async Task WriteNextAsync()
            {
                byte[] v = TestData.Bytes(3001 + (ulong)(++version), 200);
                await history.RecordAsync("w", "write", "/x", v, () => w.Files.WriteAllBytesAsync("/x", v, Ct));
                latest = v;
            }

            if (window == StaleWindow.MinorityPartition)
                cluster.Network.Partition([c.Name, readerName], [.. RunningExcept(cluster, c).Select(n => n.Name), "client:w"]);
            else
                await TakeOfflineAsync(cluster, c, mode);

            for (int i = 0; i < 3; i++)
                await WriteNextAsync();
            ulong commitWhileStale = (await GroupAsync(await cluster.GetLeaderAsync(Volume, Ct))).CommitIndex;

            MessageHold? holdAppends = null, holdChunks = null;
            FaultArm? blockApply = null;
            if (window == StaleWindow.HoldAppends)
            {
                holdAppends = cluster.Network.Hold(new MessageFilter(To: c.Name, Type: MessageType.RaftBatch));
                holdChunks = cluster.Network.Hold(new MessageFilter(To: c.Name, Type: MessageType.RaftSnapshotChunk));
            }
            FaultArm ArmBlockApply() => cluster.Faults.Arm(
                new FaultMatch(FaultSites.RaftAfterCommitBeforeApply, Node: c.Name, Volume: Volume, Count: int.MaxValue),
                FaultAction.Block());

            // Without a restart the block must be in place before the network heals. After a restart it is armed once the
            // node is up (so local journal replay is not blocked), and replication to C is gated until then: otherwise C
            // could receive and apply the missing entries between the restart and the arming, and never be stale.
            if (window == StaleWindow.BlockApply && mode == OfflineMode.Isolate)
                blockApply = ArmBlockApply();
            MessageHold? gateAppends = null, gateChunks = null;
            if (window == StaleWindow.BlockApply && blockApply is null)
            {
                gateAppends = cluster.Network.Hold(new MessageFilter(To: c.Name, Type: MessageType.RaftBatch));
                gateChunks = cluster.Network.Hold(new MessageFilter(To: c.Name, Type: MessageType.RaftSnapshotChunk));
            }

            if (window != StaleWindow.MinorityPartition)
            {
                await ReturnAsync(cluster, c, mode);
                if (window == StaleWindow.BlockApply && blockApply is null)
                {
                    blockApply = ArmBlockApply();
                    gateAppends!.Dispose();   // releases the gated messages: C now appends but cannot apply
                    gateChunks!.Dispose();
                }
                if (readerOnlyReachesReturningNode)
                {
                    foreach (TestNode n in RunningExcept(cluster, c))
                        cluster.Network.CutBoth(readerName, n.Name);
                }
            }

            // Stale window: reads through C concurrent with further writes.
            async Task WriterLoopAsync()
            {
                for (int i = 0; i < 4; i++)
                {
                    await cluster.DelayAsync(TimeSpan.FromMilliseconds(400), Ct);
                    await WriteNextAsync();
                }
            }

            Task writerLoop = WriterLoopAsync();
            DateTimeOffset end = cluster.Time.GetUtcNow() + TimeSpan.FromSeconds(3);
            int okReadsWhileStale = 0;
            while (cluster.Time.GetUtcNow() < end)
            {
                try
                {
                    await history.RecordAsync("r", "read", "/x", null, () => r.Files.ReadAllBytesAsync("/x", Ct));
                    okReadsWhileStale++;
                }
                catch (Exception ex) when (IsRefusal(ex))
                {
                    // Refusing is legal while the replica lacks an authoritative boundary.
                }

                await cluster.DelayAsync(TimeSpan.FromMilliseconds(100), Ct);
            }

            await writerLoop;

            // The scenario really exercised a stale replica.
            if (c.State == TestNodeState.Running)
            {
                GroupInspection stale = await GroupAsync(c);
                Assert.True(stale.AppliedIndex < commitWhileStale,
                    $"precondition: {c.Name} must still be stale (applied {stale.AppliedIndex} < commit {commitWhileStale}) during the probe window");
            }

            if (window == StaleWindow.MinorityPartition)
                Assert.Equal(0, okReadsWhileStale);   // the minority cannot answer new live reads at all
            if (holdAppends is not null)
                Assert.True(holdAppends.Count + (holdChunks?.Count ?? 0) > 0, "catch-up traffic to the returning node was held");
            if (blockApply is not null)
                cluster.Faults.AssertReached(blockApply);

            // Release the delay; the replica establishes its boundary and catches up.
            holdAppends?.Dispose();
            holdChunks?.Dispose();
            if (blockApply is not null)
            {
                blockApply.Disarm();
                blockApply.Release();
            }

            cluster.Network.Heal();
            await WaitCaughtUpAsync(cluster, c);
            await cluster.WaitUntilAsync(async () =>
            {
                try
                {
                    byte[] got = await history.RecordAsync("r", "read", "/x", null, () => r.Files.ReadAllBytesAsync("/x", Ct));
                    return got.AsSpan().SequenceEqual(latest);
                }
                catch (Exception ex) when (IsRefusal(ex))
                {
                    return false;
                }
            }, CatchupTimeout, "once the returning replica is current, live reads through it return the current value", Ct);

            CheckResult check = LinearizabilityChecker.Check(history.Operations.Where(o => o.Key == "/x").ToList(), new RegisterModel(v0));
            check.AssertValid();
        });

    // ------------------------------------------------------------------------------------------------------------
    // RSY-004
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<CatchupPath, OfflineMode> PathModeCases => new()
    {
        { CatchupPath.Log, OfflineMode.Stop },
        { CatchupPath.Log, OfflineMode.Isolate },
        { CatchupPath.Checkpoint, OfflineMode.Crash },
        { CatchupPath.Checkpoint, OfflineMode.Isolate },
    };

    /// <summary>
    /// The returning replica's stale version carries the largest timestamps (it was written while that replica led with a
    /// clock one day ahead). Several overwrites (partial, cross-extent, whole-extent, full replacement, append) happen
    /// while it is offline. Resync installs the authoritative current mapping — highest revision, not latest timestamp.
    /// </summary>
    [Theory]
    [MemberData(nameof(PathModeCases))]
    [Catalog("RSY-004", "S", "INV-01", "INV-07", "INV-18")]
    public Task RSY_004_CatchupIncludesOverwrites(CatchupPath path, OfflineMode mode) =>
        RunAsync(Sim(3).WithPath(path), async cluster =>
        {
            var (client, vol) = await ConnectAsync(cluster, "w");
            await using var clientScope = client;
            DfsFileClient files = vol.Files;
            RsyRoles initial = await PickRolesAsync(cluster);
            TestNode c = initial.Returning;

            // C leads with a clock one day ahead while it writes the version it will later hold stale.
            await ServeFromAsync(cluster, c);
            c.Clock.JumpWall(TimeSpan.FromDays(1));
            byte[] v0 = TestData.Bytes(4000, 3 * Chunk + 100);
            CommitToken t0 = await files.WriteAllBytesAsync("/f", v0, Ct);
            DfsFileInfo staleInfo = await files.GetInfoAsync("/f", Ct);
            await cluster.TransferLeadershipAsync(Volume, initial.Leader.Name, TimeSpan.FromSeconds(20), ct: Ct);
            RsyOutage outage = await BeginOutageAsync(cluster, mode, new RsyRoles(initial.Leader, initial.Other, c));
            Assert.True(outage.ReturningAppliedIndex >= staleInfo.Revision, "precondition: C holds the stale, latest-timestamped version");

            var model = new ReferenceFileModel(v0);
            async Task<CommitToken> OverAsync(long offset, byte[] data)
            {
                CommitToken t = await OverwriteAsync(files, "/f", offset, data);
                model.Write(offset, data);
                model.Flush();
                return t;
            }

            CommitToken last = await OverAsync(10, TestData.Bytes(4001, 1000));
            last = await OverAsync(Chunk - 50, TestData.Bytes(4002, 100));     // crosses an extent boundary
            last = await OverAsync(Chunk, TestData.Bytes(4003, Chunk));         // replaces one whole extent
            byte[] v4 = TestData.Bytes(4004, 2 * Chunk + 333);
            last = await files.WriteAllBytesAsync("/f", v4, Ct);                // shorter full replacement
            model = new ReferenceFileModel(v4);
            byte[] tail = TestData.Bytes(4005, 777);
            last = await files.AppendAllBytesAsync("/f", tail, Ct);
            model.Write(model.CommittedLength, tail);
            model.Flush();
            last = await OverAsync(5, TestData.Bytes(4006, 20));
            Assert.True(last.Sequence > t0.Sequence);

            await EndOutageAsync(cluster, outage, path, vol, "/fill", 4100);
            await AssertMatchesLeaderAsync(cluster, c, ["/f"]);
            FileInspection cFile = (await StateAsync(c)).GetFile("/f")!;
            Assert.Equal(last.Sequence, cFile.Revision);
            Assert.Equal(model.CommittedLength, cFile.Length);
            Assert.Equal(staleInfo.InodeId, cFile.Inode);

            await ServeFromAsync(cluster, c);
            DfsFileInfo info = await files.GetInfoAsync("/f", Ct);
            Assert.Equal(last.Sequence, info.Revision);
            Assert.Equal(staleInfo.InodeId, info.InodeId);
            Assert.True(info.ModifiedUtc < staleInfo.ModifiedUtc,
                "precondition/oracle: the authoritative revision carries an older timestamp than C's stale version, so timestamp merging would be wrong");
            await AssertBytesAsync(files, "/f", model.Committed, "resync installs the authoritative current mapping");
        });

    // ------------------------------------------------------------------------------------------------------------
    // RSY-005
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<CatchupPath, DeletionKind> DeletionCases => new()
    {
        { CatchupPath.Log, DeletionKind.File },
        { CatchupPath.Checkpoint, DeletionKind.File },
        { CatchupPath.Log, DeletionKind.RecursiveDirectory },
        { CatchupPath.Checkpoint, DeletionKind.RecursiveDirectory },
    };

    /// <summary>
    /// A file (or a directory tree) is deleted while the returning replica, which still stores the old contents, is
    /// offline. The returning replica's old objects never recreate the namespace entries; they are eventually reclaimed.
    /// </summary>
    [Theory]
    [MemberData(nameof(DeletionCases))]
    [Catalog("RSY-005", "S", "INV-07", "INV-09", "INV-18")]
    public Task RSY_005_CatchupIncludesDeletion(CatchupPath path, DeletionKind kind) =>
        RunAsync(Sim(3).WithPath(path), async cluster =>
        {
            var (client, vol) = await ConnectAsync(cluster, "w");
            await using var clientScope = client;
            DfsFileClient files = vol.Files;
            await MkdirAsync(vol, "/d/sub");
            byte[] x = TestData.Bytes(5001, 2 * Chunk + 3), y = TestData.Bytes(5002, Chunk + 1);
            byte[] z = TestData.Bytes(5003, 100), keep = TestData.Bytes(5004, 120);
            await files.WriteAllBytesAsync("/d/x", x, Ct);
            await files.WriteAllBytesAsync("/d/sub/y", y, Ct);
            await files.WriteAllBytesAsync("/d/sub/z", z, Ct);
            await files.WriteAllBytesAsync("/d/keep", keep, Ct);
            var deletedObjects = new HashSet<ObjectId>(await ObjectsOfAsync(cluster, "/d/x"));
            if (kind == DeletionKind.RecursiveDirectory)
                deletedObjects.UnionWith(await ObjectsOfAsync(cluster, "/d/sub/y"));
            Assert.NotEmpty(deletedObjects);

            await cluster.WaitForRepairAsync(Volume, RepairTimeout, Ct);   // the third copy is not part of the ack
            await cluster.WaitForReplicationAsync(Volume, Ct);
            RsyRoles roles = await PickRolesAsync(cluster);
            TestNode c = roles.Returning;
            foreach (ObjectId o in deletedObjects)
                Assert.True(await c.Inspect.HasObjectAsync(o), $"precondition: {c.Name} stores a copy of {o} before it goes offline");

            RsyOutage outage = await BeginOutageAsync(cluster, OfflineMode.Stop, roles);
            string[] deleted;
            if (kind == DeletionKind.File)
            {
                await files.DeleteAsync("/d/x", Ct);
                deleted = ["/d/x"];
            }
            else
            {
                await vol.Directories.DeleteAsync("/d", true, Ct);
                deleted = ["/d/x", "/d/sub/y", "/d/sub/z", "/d/keep"];
            }

            await EndOutageAsync(cluster, outage, path, vol, "/fill", 5100);

            // Let C report its inventory (which still lists the old objects) before checking for resurrection.
            await cluster.DelayAsync(Timings.InventoryInterval * 8, Ct);
            await AssertMatchesLeaderAsync(cluster, c, [.. deleted, "/d/keep"]);
            VolumeStateInspection cState = await StateAsync(c);
            foreach (string p in deleted)
                Assert.Null(cState.GetFile(p));
            IReadOnlySet<ObjectId> reachable = cState.ReachableObjects();
            Assert.All(deletedObjects, o => Assert.DoesNotContain(o, reachable));

            await ServeFromAsync(cluster, c);
            await AssertDeletedAsync();

            // The writing client goes away so its staging sessions are closed (by disposal or session expiry); the old
            // contents are then unreachable with closed staging: eventually reclaimed everywhere, including on C...
            await client.DisposeAsync();
            var (readerClient, reader) = await ConnectAsync(cluster, "v");
            await using var readerScope = readerClient;
            (vol, files) = (reader, reader.Files);
            await cluster.WaitUntilAsync(async () =>
            {
                foreach (ObjectId o in deletedObjects)
                {
                    if ((await cluster.Inspect.GetObjectHoldersAsync(o)).Count > 0)
                        return false;
                }

                return true;
            }, RepairTimeout, "objects of deleted files are reclaimed from every node, including the returning one", Ct);

            // ...and never recreated the namespace entries.
            await AssertDeletedAsync();

            async Task AssertDeletedAsync()
            {
                if (kind == DeletionKind.File)
                {
                    Assert.False(await files.ExistsAsync("/d/x", Ct));
                    await Assert.ThrowsAnyAsync<FileNotFoundException>(() => files.GetInfoAsync("/d/x", Ct));
                    var names = new List<string>();
                    await foreach (DfsEntryInfo e in vol.Directories.EnumerateAsync("/d", null, Ct))
                        names.Add(e.Name);
                    Assert.Equal(new[] { "keep", "sub" }, names.Order(StringComparer.Ordinal).ToArray());
                    await AssertBytesAsync(files, "/d/keep", keep);
                    await AssertBytesAsync(files, "/d/sub/y", y);
                }
                else
                {
                    Assert.False(await vol.Directories.ExistsAsync("/d", Ct));
                    Assert.False(await files.ExistsAsync("/d/x", Ct));
                    Assert.False(await files.ExistsAsync("/d/sub/y", Ct));
                    await Assert.ThrowsAnyAsync<DirectoryNotFoundException>(() => vol.Directories.GetInfoAsync("/d", Ct));
                    var names = new List<string>();
                    await foreach (DfsEntryInfo e in vol.Directories.EnumerateAsync("/", null, Ct))
                        names.Add(e.Name);
                    Assert.DoesNotContain("d", names);
                }
            }
        });

    // ------------------------------------------------------------------------------------------------------------
    // RSY-006
    // ------------------------------------------------------------------------------------------------------------

    private const int L0 = 3 * Chunk + 777;

    /// <summary>
    /// The file is shrunk (published) and then re-extended (published) while the replica is offline. After resync the
    /// replica's mapping has no extent in the discarded range, the gap reads as zeroes, and copies re-created by repair
    /// serve the same bytes (the discarded tail never returns).
    /// </summary>
    [Theory]
    [InlineData(0, L0, CatchupPath.Log)]
    [InlineData(1, 2 * Chunk, CatchupPath.Checkpoint)]
    [InlineData(Inline - 1, L0, CatchupPath.Log)]
    [InlineData(Inline, Inline + 1, CatchupPath.Checkpoint)]
    [InlineData(Inline + 1, Chunk + 5, CatchupPath.Log)]
    [InlineData(Chunk, 2 * Chunk, CatchupPath.Checkpoint)]
    [InlineData(Chunk + 1, L0, CatchupPath.Log)]
    [InlineData(2 * Chunk - 1, 3 * Chunk, CatchupPath.Checkpoint)]
    [InlineData(L0 - 1, L0 + Chunk, CatchupPath.Log)]
    [Catalog("RSY-006", "S", "INV-01", "INV-07", "INV-18")]
    public Task RSY_006_CatchupIncludesTruncation(int shrinkTo, int extendTo, CatchupPath path) =>
        RunAsync(Sim(3).WithPath(path), async cluster =>
        {
            var (client, vol) = await ConnectAsync(cluster, "w");
            await using var clientScope = client;
            DfsFileClient files = vol.Files;
            byte[] original = TestData.Bytes(6000 + (ulong)shrinkTo, L0);
            await files.WriteAllBytesAsync("/t", original, Ct);
            var originalObjects = new HashSet<ObjectId>(await ObjectsOfAsync(cluster, "/t"));
            Assert.NotEmpty(originalObjects);

            RsyOutage outage = await BeginOutageAsync(cluster, OfflineMode.Crash);
            TestNode c = outage.Roles.Returning;
            CommitToken shrunk = await ResizeAsync(files, "/t", shrinkTo);
            CommitToken extended = await ResizeAsync(files, "/t", extendTo);
            Assert.True(extended.Sequence > shrunk.Sequence);
            var expected = new byte[extendTo];
            original.AsSpan(0, shrinkTo).CopyTo(expected);

            await EndOutageAsync(cluster, outage, path, vol, "/fill", 6100);
            await AssertMatchesLeaderAsync(cluster, c, ["/t"]);
            FileInspection cFile = (await StateAsync(c)).GetFile("/t")!;
            Assert.Equal(extendTo, cFile.Length);
            Assert.Equal(extended.Sequence, cFile.Revision);
            // Prefix extents may keep (clipped) references to the original objects, but nothing that reaches into the
            // discarded range may map an original object: those bytes are the discarded tail. (An implementation may leave
            // the gap sparse or write fresh zero-filled objects; the bytes are checked below.)
            Assert.All(cFile.Extents, e => Assert.True(e.Offset + e.Length <= shrinkTo || !originalObjects.Contains(e.Object),
                $"extent [{e.Offset}, {e.Offset + e.Length}) maps original object {e.Object} into the discarded range [{shrinkTo}, {extendTo})"));

            await ServeFromAsync(cluster, c);
            await AssertBytesAsync(files, "/t", expected, "prefix kept, gap reads as zeroes, the discarded tail never returns");
            byte[] gap = await ReadRangeAsync(files, "/t", shrinkTo, Math.Min(extendTo - shrinkTo, 8192));
            Assert.Equal(Math.Min(extendTo - shrinkTo, 8192), gap.Length);
            Assert.All(gap, v => Assert.Equal(0, v));

            // Repaired copies: lose one replica's copies, let anti-entropy re-create them, then read from them only.
            IReadOnlyList<ObjectId> objects = await ObjectsOfAsync(cluster, "/t");
            if (objects.Count > 0)
            {
                TestNode repaired = outage.Roles.Other;
                foreach (ObjectId o in objects)
                    await repaired.Disk.DeleteObjectAsync(o);
                await cluster.WaitUntilAsync(async () =>
                {
                    foreach (ObjectId o in objects)
                    {
                        if (!await repaired.Inspect.HasObjectAsync(o))
                            return false;
                    }

                    return true;
                }, RepairTimeout, $"repair re-creates the lost copies on {repaired.Name}", Ct);
                foreach (TestNode n in RunningExcept(cluster, repaired))
                {
                    foreach (ObjectId o in objects)
                        await n.Disk.DeleteObjectAsync(o);
                }

                await AssertBytesAsync(files, "/t", expected, "repaired copies retain the zeroed gap and never restore the discarded tail");
            }
        });

    // ------------------------------------------------------------------------------------------------------------
    // RSY-007
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// While the replica is offline, names are swapped, a file is deleted and recreated with identical bytes, a directory
    /// is moved away and its name reused, and a file is renamed. Resync reproduces the current name→inode bindings
    /// (identity, not path strings or contents).
    /// </summary>
    [Theory]
    [InlineData(CatchupPath.Log)]
    [InlineData(CatchupPath.Checkpoint)]
    [Catalog("RSY-007", "S", "INV-07", "INV-18")]
    public Task RSY_007_CatchupIncludesRenameAndNameReuse(CatchupPath path) =>
        RunAsync(Sim(3).WithPath(path), async cluster =>
        {
            var (client, vol) = await ConnectAsync(cluster, "w");
            await using var clientScope = client;
            DfsFileClient files = vol.Files;
            byte[] bytesA = TestData.Bytes(7001, 100), bytesB = TestData.Bytes(7002, Chunk + 50);
            byte[] bytesF = TestData.Bytes(7003, 2 * Chunk), bytesG = TestData.Bytes(7004, 300);
            await MkdirAsync(vol, "/dir1");
            await files.WriteAllBytesAsync("/a", bytesA, Ct);
            await files.WriteAllBytesAsync("/b", bytesB, Ct);
            await files.WriteAllBytesAsync("/dir1/f", bytesF, Ct);
            await files.WriteAllBytesAsync("/dir1/g", bytesG, Ct);
            InodeId ia = (await files.GetInfoAsync("/a", Ct)).InodeId;
            InodeId ib = (await files.GetInfoAsync("/b", Ct)).InodeId;
            InodeId iF = (await files.GetInfoAsync("/dir1/f", Ct)).InodeId;
            InodeId iG = (await files.GetInfoAsync("/dir1/g", Ct)).InodeId;
            InodeId iDir1 = (await vol.Directories.GetInfoAsync("/dir1", Ct)).InodeId;

            RsyOutage outage = await BeginOutageAsync(cluster, OfflineMode.Stop);
            TestNode c = outage.Roles.Returning;

            await files.MoveAsync("/a", "/tmp", false, Ct);      // swap /a and /b through a temporary name
            await files.MoveAsync("/b", "/a", false, Ct);
            await files.MoveAsync("/tmp", "/b", false, Ct);
            await files.DeleteAsync("/dir1/f", Ct);               // recreate with identical bytes: only identity differs
            await files.WriteAllBytesAsync("/dir1/f", bytesF, Ct);
            InodeId jF = (await files.GetInfoAsync("/dir1/f", Ct)).InodeId;
            await vol.Directories.MoveAsync("/dir1", "/dir2", Ct); // move the directory away and reuse its name
            await MkdirAsync(vol, "/dir1");
            InodeId kDir1 = (await vol.Directories.GetInfoAsync("/dir1", Ct)).InodeId;
            await files.WriteAllBytesAsync("/dir1/g", bytesG, Ct);
            InodeId kG = (await files.GetInfoAsync("/dir1/g", Ct)).InodeId;
            await files.MoveAsync("/dir2/g", "/dir2/h", false, Ct);
            Assert.NotEqual(iF, jF);
            Assert.NotEqual(iDir1, kDir1);
            Assert.NotEqual(iG, kG);

            await EndOutageAsync(cluster, outage, path, vol, "/fill", 7100);
            await AssertMatchesLeaderAsync(cluster, c, ["/a", "/b", "/tmp", "/dir1/f", "/dir1/g", "/dir2/f", "/dir2/g", "/dir2/h"]);
            VolumeStateInspection cs = await StateAsync(c);
            Assert.Equal(ib, cs.GetFile("/a")!.Inode);
            Assert.Equal(ia, cs.GetFile("/b")!.Inode);
            Assert.Null(cs.GetFile("/tmp"));
            Assert.Null(cs.GetFile("/dir1/f"));
            Assert.Equal(jF, cs.GetFile("/dir2/f")!.Inode);
            Assert.Null(cs.GetFile("/dir2/g"));
            Assert.Equal(iG, cs.GetFile("/dir2/h")!.Inode);
            Assert.Equal(kG, cs.GetFile("/dir1/g")!.Inode);

            await ServeFromAsync(cluster, c);
            foreach ((string p, InodeId inode, byte[] bytes) in new[]
                     {
                         ("/a", ib, bytesB), ("/b", ia, bytesA), ("/dir2/f", jF, bytesF), ("/dir2/h", iG, bytesG), ("/dir1/g", kG, bytesG),
                     })
            {
                Assert.Equal(inode, (await files.GetInfoAsync(p, Ct)).InodeId);
                await AssertBytesAsync(files, p, bytes);
            }

            Assert.Equal(iDir1, (await vol.Directories.GetInfoAsync("/dir2", Ct)).InodeId);
            Assert.Equal(kDir1, (await vol.Directories.GetInfoAsync("/dir1", Ct)).InodeId);
            Assert.False(await files.ExistsAsync("/dir1/f", Ct));
            Assert.False(await files.ExistsAsync("/tmp", Ct));
            Assert.False(await files.ExistsAsync("/dir2/g", Ct));
        });

    // ------------------------------------------------------------------------------------------------------------
    // RSY-008
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// A multi-file transaction (writes, delete, create, rename) commits and another aborts while the replica is offline.
    /// Every observation of the replica's applied state during catch-up is entirely before or entirely after the committed
    /// transaction; the aborted one never appears; outcomes are queryable from the returning replica.
    /// </summary>
    [Theory]
    [InlineData(CatchupPath.Log)]
    [InlineData(CatchupPath.Checkpoint)]
    [Catalog("RSY-008", "S", "INV-01", "INV-02", "INV-07", "INV-18")]
    public Task RSY_008_CatchupIncludesTransactionBoundary(CatchupPath path) =>
        RunAsync(Sim(3).WithPath(path), async cluster =>
        {
            var (client, vol) = await ConnectAsync(cluster, "w");
            await using var clientScope = client;
            DfsFileClient files = vol.Files;
            await MkdirAsync(vol, "/t");
            byte[] a0 = TestData.Bytes(8001, 300), b0 = TestData.Bytes(8002, Chunk + 7), c0 = TestData.Bytes(8003, 40), e0 = TestData.Bytes(8004, 60);
            await files.WriteAllBytesAsync("/t/a", a0, Ct);
            await files.WriteAllBytesAsync("/t/b", b0, Ct);
            await files.WriteAllBytesAsync("/t/c", c0, Ct);
            await files.WriteAllBytesAsync("/t/e", e0, Ct);
            string[] txPaths = ["/t/a", "/t/b", "/t/c", "/t/d", "/t/e", "/t/e2"];
            await cluster.WaitForReplicationAsync(Volume, Ct);
            VolumeStateInspection preState = await LeaderStateAsync(cluster);
            Dictionary<string, FileInspection?> pre = txPaths.ToDictionary(p => p, p => preState.GetFile(p));

            RsyOutage outage = await BeginOutageAsync(cluster, OfflineMode.Stop);
            TestNode c = outage.Roles.Returning;

            byte[] a1 = TestData.Bytes(8011, 310), b1 = TestData.Bytes(8012, 2 * Chunk + 1), d1 = TestData.Bytes(8013, Chunk + 3);
            DfsTransactionId tx1Id;
            CommitToken tx1Token;
            await using (DfsTransaction tx1 = await vol.Transactions.BeginAsync(null, Ct))
            {
                tx1Id = tx1.Id;
                await tx1.Files.WriteAllBytesAsync("/t/a", a1, Ct);
                await tx1.Files.WriteAllBytesAsync("/t/b", b1, Ct);
                await tx1.Files.DeleteAsync("/t/c", Ct);
                await tx1.Files.WriteAllBytesAsync("/t/d", d1, Ct);
                await tx1.Files.MoveAsync("/t/e", "/t/e2", false, Ct);
                tx1Token = await tx1.CommitAsync(Ct);
                Assert.Equal(DfsTransactionState.Committed, tx1.State);
            }

            DfsTransactionId tx2Id;
            await using (DfsTransaction tx2 = await vol.Transactions.BeginAsync(null, Ct))
            {
                tx2Id = tx2.Id;
                await tx2.Files.WriteAllBytesAsync("/t/a", TestData.Bytes(8020, 999), Ct);
                await tx2.Files.DeleteAsync("/t/b", Ct);
                await tx2.AbortAsync(Ct);
                Assert.Equal(DfsTransactionState.Aborted, tx2.State);
            }

            VolumeStateInspection postState = await LeaderStateAsync(cluster);
            Dictionary<string, FileInspection?> post = txPaths.ToDictionary(p => p, p => postState.GetFile(p));
            Assert.Equal(tx1Token.Sequence, post["/t/a"]!.Revision);
            Assert.Equal(tx1Token.Sequence, post["/t/b"]!.Revision);
            Assert.Equal(tx1Token.Sequence, post["/t/d"]!.Revision);
            Assert.Null(post["/t/c"]);
            Assert.Null(post["/t/e"]);
            Assert.NotNull(post["/t/e2"]);

            IReadOnlyList<TestNode> sources = RunningExcept(cluster, c);
            if (path == CatchupPath.Checkpoint)
                await ForceCompactionAsync(cluster, vol, sources, outage.ReturningLastIndex, "/fill", 8100);
            else
                await AssertMissingRangeRetainedAsync(sources, outage.ReturningAppliedIndex);

            await ReturnAsync(cluster, c, outage.Mode);
            bool stop = false;
            int observations = 0;
            async Task<bool?> ObserveOnceAsync()
            {
                VolumeStateInspection? st = null;
                if (c.State == TestNodeState.Running)
                {
                    try
                    {
                        st = await c.Inspect.GetVolumeStateAsync(Volume);
                    }
                    catch (InvalidOperationException)
                    {
                        // the node left Running between the check and the call
                    }
                }

                if (st is null)
                    return null;
                observations++;
                bool isPre = txPaths.All(p => SameFile(pre[p], st.GetFile(p)));
                bool isPost = txPaths.All(p => SameFile(post[p], st.GetFile(p)));
                Assert.True(isPre || isPost,
                    $"{c.Name} exposed a half-applied transaction at applied index {st.AppliedIndex}: " +
                    string.Join(", ", txPaths.Select(p => $"{p}={(st.GetFile(p) is { } f ? $"{f.Inode}@{f.Revision}" : "absent")}")));
                return isPost;
            }

            async Task ObserveAsync()
            {
                while (!stop)
                {
                    await ObserveOnceAsync();
                    await cluster.DelayAsync(TimeSpan.FromMilliseconds(5), Ct);
                }
            }

            Task observer = ObserveAsync();
            await WaitCaughtUpAsync(cluster, c);
            stop = true;
            await observer;
            Assert.True(await ObserveOnceAsync() == true, "the caught-up replica holds the complete post-transaction state");
            Assert.True(observations > 1, "the replica's applied state was observed during the catch-up");
            await AssertCatchupPathAsync(cluster, c, path, null);
            await AssertMatchesLeaderAsync(cluster, c, txPaths);

            await ServeFromAsync(cluster, c);
            await AssertBytesAsync(files, "/t/a", a1, "the aborted transaction never took effect");
            await AssertBytesAsync(files, "/t/b", b1);
            Assert.False(await files.ExistsAsync("/t/c", Ct));
            await AssertBytesAsync(files, "/t/d", d1);
            Assert.False(await files.ExistsAsync("/t/e", Ct));
            await AssertBytesAsync(files, "/t/e2", e0);
            AssertCommittedOrRetired(await vol.Transactions.QueryOutcomeAsync(tx1Id, false, Ct), tx1Token);
            DfsOperationOutcome o2 = await vol.Transactions.QueryOutcomeAsync(tx2Id, false, Ct);
            Assert.True(o2.Status is DfsOutcomeStatus.Aborted or DfsOutcomeStatus.Expired,
                $"the aborted transaction must report Aborted (or Expired once its outcome was retired), never {o2.Status}");
        });

    // ------------------------------------------------------------------------------------------------------------
    // RSY-009
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Snapshots are created (plain, with retention, commit-and-snapshot), deleted and re-retained, and a snapshot reader
    /// pin is opened while the replica is offline. The recovered snapshot records, pins and reachable-object set equal the
    /// authoritative ones, snapshot contents are exact, and GC keeps every object a remaining snapshot needs.
    /// </summary>
    [Theory]
    [InlineData(CatchupPath.Log)]
    [InlineData(CatchupPath.Checkpoint)]
    [Catalog("RSY-009", "S", "INV-07", "INV-09", "INV-18")]
    public Task RSY_009_CatchupIncludesSnapshotsAndPins(CatchupPath path) =>
        RunAsync(Sim(3).WithPath(path), async cluster =>
        {
            var (client, vol) = await ConnectAsync(cluster, "w");
            await using var clientScope = client;
            DfsFileClient files = vol.Files;
            await MkdirAsync(vol, "/s");
            byte[] v0 = TestData.Bytes(9000, 2 * Chunk);
            await files.WriteAllBytesAsync("/s/f", v0, Ct);
            IReadOnlyList<ObjectId> objV0 = await ObjectsOfAsync(cluster, "/s/f");
            await vol.Snapshots.CreateAsync("s0", null, Ct);

            RsyOutage outage = await BeginOutageAsync(cluster, OfflineMode.Stop);
            TestNode c = outage.Roles.Returning;

            byte[] v1 = TestData.Bytes(9001, Chunk + 1), v2 = TestData.Bytes(9002, Chunk + 2);
            byte[] v3 = TestData.Bytes(9003, Chunk + 3), v4 = TestData.Bytes(9004, Chunk + 4);
            var protectedObjects = new HashSet<ObjectId>();
            await files.WriteAllBytesAsync("/s/f", v1, Ct);
            protectedObjects.UnionWith(await ObjectsOfAsync(cluster, "/s/f"));
            await vol.Snapshots.CreateAsync("s1", null, Ct);
            await files.WriteAllBytesAsync("/s/f", v2, Ct);
            protectedObjects.UnionWith(await ObjectsOfAsync(cluster, "/s/f"));
            // Whole seconds, so the round-trip comparison does not depend on the wire/at-rest timestamp precision.
            DateTimeOffset now = cluster.Time.GetUtcNow();
            var expires = new DateTimeOffset(now.UtcTicks - now.UtcTicks % TimeSpan.TicksPerSecond, TimeSpan.Zero) + TimeSpan.FromHours(1);
            DfsSnapshotInfo s2 = await vol.Snapshots.CreateAsync("s2", new DfsSnapshotOptions { Retention = new DfsRetention(expires, false) }, Ct);
            await vol.Snapshots.DeleteAsync("s0", Ct);
            CommitToken t3 = await files.WriteAllBytesAsync("/s/f", v3, new DfsMutationOptions { CommitSnapshotName = "s3" }, Ct);
            protectedObjects.UnionWith(await ObjectsOfAsync(cluster, "/s/f"));
            await vol.Snapshots.CreateAsync("s4", null, Ct);
            await vol.Snapshots.DeleteAsync("s4", Ct);
            await vol.Snapshots.SetRetentionAsync("s1", new DfsRetention(null, true), Ct);
            await files.WriteAllBytesAsync("/s/f", v4, Ct);
            protectedObjects.UnionWith(await ObjectsOfAsync(cluster, "/s/f"));
            await using DfsSnapshotView pinned = await vol.Snapshots.OpenAsync("s2", Ct);   // reader pin held across the catch-up

            await EndOutageAsync(cluster, outage, path, vol, "/fill", 9100);
            await AssertMatchesLeaderAsync(cluster, c, ["/s/f"]);
            (VolumeStateInspection ls, VolumeStateInspection cs) = await StatesAtSameIndexAsync(cluster, c);
            Assert.Equal(ls.Snapshots.OrderBy(s => s.Name, StringComparer.Ordinal).ToArray(), cs.Snapshots.OrderBy(s => s.Name, StringComparer.Ordinal).ToArray());
            Assert.Equal(new[] { "s1", "s2", "s3" }, cs.Snapshots.Select(s => s.Name).Order(StringComparer.Ordinal).ToArray());
            Assert.Equal(ls.Pins.OrderBy(p => p.Id).ToArray(), cs.Pins.OrderBy(p => p.Id).ToArray());
            Assert.Contains(cs.Pins, p => p.Snapshot == "s2" || p.Snapshot == s2.Id.Value.ToString());   // the reader pin survived catch-up
            IReadOnlySet<ObjectId> cReach = cs.ReachableObjects();
            Assert.Equal(ls.ReachableObjects().OrderBy(o => o.Value).ToArray(), cReach.OrderBy(o => o.Value).ToArray());
            Assert.All(protectedObjects, o => Assert.Contains(o, cReach));
            Assert.All(objV0, o => Assert.DoesNotContain(o, cReach));

            await ServeFromAsync(cluster, c);
            Assert.Equal(new[] { "s1", "s2", "s3" }, (await vol.Snapshots.ListAsync(Ct)).Select(s => s.Name).Order(StringComparer.Ordinal).ToArray());
            Assert.True((await vol.Snapshots.GetAsync("s1", Ct)).Retention.Hold);
            Assert.Equal(expires, (await vol.Snapshots.GetAsync("s2", Ct)).Retention.ExpiresUtc);
            Assert.Equal(t3, (await vol.Snapshots.GetAsync("s3", Ct)).Boundary);
            await AssertEx.ThrowsDfsAsync<DfsException>(() => vol.Snapshots.GetAsync("s0", Ct), DfsErrorCode.SnapshotNotFound);
            await AssertEx.ThrowsDfsAsync<DfsException>(() => vol.Snapshots.GetAsync("s4", Ct), DfsErrorCode.SnapshotNotFound);
            foreach ((string name, byte[] bytes) in new[] { ("s1", v1), ("s3", v3) })
            {
                await using DfsSnapshotView view = await vol.Snapshots.OpenAsync(name, Ct);
                await AssertBytesAsync(view.Files, "/s/f", bytes, $"snapshot {name}");
            }

            await AssertBytesAsync(pinned.Files, "/s/f", v2, "the reader pin opened during the outage still reads its snapshot");
            await AssertBytesAsync(files, "/s/f", v4);

            await cluster.DelayAsync(Timings.GcInterval * 8, Ct);
            foreach (ObjectId o in protectedObjects)
            {
                (int copies, int zones) = await cluster.Inspect.CountDurableDomainsAsync(o);
                Assert.True(copies >= 2 && zones >= 2, $"GC must keep snapshot-referenced object {o} at ack durability (has {copies}/{zones})");
            }
        });

    // ------------------------------------------------------------------------------------------------------------
    // RSY-010
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<CatchupPath, RetirementKind> RetirementCases => new()
    {
        { CatchupPath.Log, RetirementKind.SessionExpired },
        { CatchupPath.Checkpoint, RetirementKind.SessionExpired },
        { CatchupPath.Checkpoint, RetirementKind.OutcomeWindowExceeded },
    };

    /// <summary>
    /// Operation ids are retired (session expiry with outcome aging, or the per-session outcome window overflowing) while
    /// the replica is offline. After catch-up the returning replica, as leader, rejects their replay as expired — same
    /// payload or different — with no second effect, and reports them as expired.
    /// </summary>
    [Theory]
    [MemberData(nameof(RetirementCases))]
    [Catalog("RSY-010", "S", "INV-06", "INV-18")]
    public Task RSY_010_CatchupPreservesRequestRetirement(CatchupPath path, RetirementKind kind) =>
        RunAsync(Sim(3).WithPath(path), async cluster =>
        {
            var (appClient, app) = await ConnectAsync(cluster, "app");
            await using var appScope = appClient;
            await MkdirAsync(app, "/r");
            byte[] log0 = TestData.Bytes(10_000, 80);
            await app.Files.WriteAllBytesAsync("/r/log", log0, Ct);

            var (kClient, k) = await ConnectAsync(cluster, "k");
            DfsOperationId id1 = await k.NewOperationIdAsync(Ct);
            byte[] s1 = TestData.Bytes(10_001, 33);
            CommitToken t1 = await k.Files.AppendAllBytesAsync("/r/log", s1, new DfsMutationOptions { OperationId = id1 }, Ct);
            DfsOperationId id2 = await k.NewOperationIdAsync(Ct);
            byte[] w = TestData.Bytes(10_002, 500);
            CommitToken t2 = await k.Files.WriteAllBytesAsync("/r/w", w, new DfsMutationOptions { OperationId = id2 }, Ct);
            Assert.True(t2.Sequence > t1.Sequence);

            // While retained, a retry is answered from the outcome record (no second effect).
            Assert.Equal(t1, await k.Files.AppendAllBytesAsync("/r/log", s1, new DfsMutationOptions { OperationId = id1 }, Ct));
            await AssertBytesAsync(app.Files, "/r/log", Concat(log0, s1));

            RsyOutage outage = await BeginOutageAsync(cluster, OfflineMode.Stop);
            TestNode c = outage.Roles.Returning;
            if (kind == RetirementKind.SessionExpired)
            {
                cluster.KillClient(kClient);
                await cluster.DelayAsync(Timings.SessionTimeout + Timings.OutcomeRetention + TimeSpan.FromSeconds(5), Ct);
            }
            else
            {
                await using (kClient)
                {
                    // The window retires the lowest sequences, and the SDK draws its internal operation ids from blocks
                    // reserved in advance: operations issued after id2 (an application id, reserved on its own) may use
                    // sequences below it. Write until the window has moved past id2 (at least MaxOutcomesPerSession + 50
                    // operations; bounded).
                    int count = Timings.MaxOutcomesPerSession + 50;
                    int bound = Timings.MaxOutcomesPerSession * 4;
                    for (int i = 0; i < bound && (i < count || !await RetiredThroughAsync(cluster, id2)); i += 10)
                    {
                        await Task.WhenAll(Enumerable.Range(i, 10).Select(n =>
                            k.Files.WriteAllBytesAsync($"/r/other{n:D4}", TestData.Bytes(10_100 + (ulong)n, 8), Ct)));
                    }
                }
            }

            var (qClient, q) = await ConnectAsync(cluster, "q");
            await using (qClient)
            {
                Assert.Equal(DfsOutcomeStatus.Expired, (await q.QueryOperationAsync(id1, false, Ct)).Status);
                Assert.Equal(DfsOutcomeStatus.Expired, (await q.QueryOperationAsync(id2, false, Ct)).Status);
            }

            await EndOutageAsync(cluster, outage, path, app, "/fill", 10_200);
            await AssertMatchesLeaderAsync(cluster, c, ["/r/log", "/r/w"]);
            (VolumeStateInspection ls, VolumeStateInspection cs) = await StatesAtSameIndexAsync(cluster, c);
            SessionInspection? cSession = cs.Sessions.SingleOrDefault(s => s.Id == id1.Session);
            Assert.Equal(ls.Sessions.SingleOrDefault(s => s.Id == id1.Session), cSession);
            if (cSession is not null)
            {
                Assert.True(cSession.Expired || cSession.RetiredWatermark >= id2.Sequence,
                    $"{c.Name} must keep the retirement of session {id1.Session} (expired={cSession.Expired}, watermark={cSession.RetiredWatermark})");
            }

            await ServeFromAsync(cluster, c);
            var (zClient, z) = await ConnectAsync(cluster, "z");   // a different client process retries the persisted ids
            await using var zScope = zClient;
            await AssertEx.ThrowsDfsAsync<DfsOperationExpiredException>(
                () => z.Files.AppendAllBytesAsync("/r/log", s1, new DfsMutationOptions { OperationId = id1 }, Ct), DfsErrorCode.OperationExpired);
            await AssertEx.ThrowsDfsAsync<DfsOperationExpiredException>(
                () => z.Files.WriteAllBytesAsync("/r/w", TestData.Bytes(10_003, 64), new DfsMutationOptions { OperationId = id2 }, Ct),
                DfsErrorCode.OperationExpired);
            Assert.Equal(DfsOutcomeStatus.Expired, (await z.QueryOperationAsync(id1, false, Ct)).Status);
            Assert.Equal(DfsOutcomeStatus.Expired, (await z.QueryOperationAsync(id1, true, Ct)).Status);
            await AssertBytesAsync(z.Files, "/r/log", Concat(log0, s1), "the retired append is not replayed");
            await AssertBytesAsync(z.Files, "/r/w", w, "the retired write is not replayed");
            Assert.Equal(t1.Sequence, (await z.Files.GetInfoAsync("/r/log", Ct)).Revision);
            Assert.Equal(t2.Sequence, (await z.Files.GetInfoAsync("/r/w", Ct)).Revision);
        });

    /// <summary>True when the leader has retired <paramref name="id"/> and every earlier sequence of its session.</summary>
    private static async Task<bool> RetiredThroughAsync(TestCluster cluster, DfsOperationId id)
    {
        VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Volume);
        return state.Sessions.SingleOrDefault(s => s.Id == id.Session) is { } session && (session.Expired || session.RetiredWatermark >= id.Sequence);
    }

    // ------------------------------------------------------------------------------------------------------------
    // RSY-011
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// The writer lease of generation g1 is lost and two newer generations are granted while the replica is offline. The
    /// returning replica keeps the newest generation, rejects the paused old writer's publication when it is the leader,
    /// and continues the generation sequence for the next writer (never resets it).
    /// </summary>
    [Theory]
    [InlineData(CatchupPath.Log)]
    [InlineData(CatchupPath.Checkpoint)]
    [Catalog("RSY-011", "S", "INV-05", "INV-18")]
    public Task RSY_011_CatchupPreservesFenceGenerations(CatchupPath path) =>
        RunAsync(Sim(3).WithPath(path), async cluster =>
        {
            var (appClient, app) = await ConnectAsync(cluster, "app");
            await using var appScope = appClient;
            byte[] f0 = TestData.Bytes(11_000, Chunk + 10);
            await app.Files.WriteAllBytesAsync("/f", f0, Ct);

            var w1Clock = new TestClock(cluster.Time);
            var (w1Client, w1) = await ConnectAsync(cluster, "w1", clock: w1Clock);
            await using var w1Scope = w1Client;
            DfsStream stale = await w1.Files.OpenAsync("/f", FileMode.Open, FileAccess.Write, FileShare.Read, Ct);
            ulong g1 = stale.WriterGeneration;
            await stale.WriteAsync(TestData.Bytes(11_001, 300), Ct);   // private staged bytes

            await cluster.WaitForReplicationAsync(Volume, Ct);
            RsyRoles roles = await PickRolesAsync(cluster);
            TestNode c = roles.Returning;
            Assert.Equal(g1, (await StateAsync(c)).GetFile("/f")!.WriterGeneration);
            RsyOutage outage = await BeginOutageAsync(cluster, OfflineMode.Stop, roles);

            w1Clock.Freeze();   // paused writer process: no renewals
            await cluster.DelayAsync(Timings.LeaseDuration * 2 + TimeSpan.FromSeconds(1), Ct);
            byte[] f2 = TestData.Bytes(11_002, 2 * Chunk + 3);
            ulong g2, g3;
            await using (DfsStream s2 = await app.Files.OpenAsync("/f", FileMode.Open, FileAccess.Write, FileShare.Read, Ct))
            {
                g2 = s2.WriterGeneration;
                await s2.SetLengthAsync(0, Ct);
                await s2.WriteAsync(f2, Ct);
                await s2.CommitAsync(Ct);
            }

            await using (DfsStream s3 = await app.Files.OpenAsync("/f", FileMode.Open, FileAccess.Write, FileShare.Read, Ct))
                g3 = s3.WriterGeneration;
            Assert.True(g1 < g2 && g2 < g3, $"writer generations must increase: {g1} < {g2} < {g3}");
            FileInspection leaderFile = (await LeaderStateAsync(cluster)).GetFile("/f")!;
            Assert.True(leaderFile.WriterGeneration >= g3);

            await EndOutageAsync(cluster, outage, path, app, "/fill", 11_100);
            await AssertMatchesLeaderAsync(cluster, c, ["/f"]);
            FileInspection cFile = (await StateAsync(c)).GetFile("/f")!;
            Assert.True(cFile.WriterGeneration >= g3, $"{c.Name} must not reset the fencing generation ({cFile.WriterGeneration} < {g3})");

            await ServeFromAsync(cluster, c);
            w1Clock.Unfreeze();
            Exception fenced = await Assert.ThrowsAnyAsync<Exception>(() => stale.FlushAsync(Ct));
            Assert.True(fenced is DfsLeaseLostException or DfsOperationExpiredException,
                $"the obsolete generation {g1} must be rejected by the returning leader, got {fenced.GetType().Name}: {fenced.Message}");
            Assert.True(stale.IsFaulted);
            await AssertBytesAsync(app.Files, "/f", f2, "the newest generation's contents remain authoritative");
            Assert.Equal(cFile.Revision, (await StateAsync(c)).GetFile("/f")!.Revision);

            await using (DfsStream s4 = await app.Files.OpenAsync("/f", FileMode.Open, FileAccess.Write, FileShare.Read, Ct))
                Assert.True(s4.WriterGeneration > g3, $"the next writer through the returning leader continues the sequence ({s4.WriterGeneration} > {g3})");

            try
            {
                await stale.DisposeAsync();
            }
            catch (Exception ex) when (ex is DfsLeaseLostException or DfsOperationExpiredException)
            {
                // disposal surfaces the fault; it never publishes
            }
        });

    // ------------------------------------------------------------------------------------------------------------
    // RSY-012
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<ResyncStage, Interruption> InterruptCases
    {
        get
        {
            var data = new TheoryData<ResyncStage, Interruption>();
            foreach (ResyncStage stage in Enum.GetValues<ResyncStage>())
            {
                foreach (Interruption interruption in Enum.GetValues<Interruption>())
                    data.Add(stage, interruption);
            }

            return data;
        }
    }

    /// <summary>
    /// Checkpoint catch-up is interrupted (crash or disconnect) at each stage: chunk receive, verification, installation,
    /// suffix replay and activation, while a writer keeps producing a suffix. At every observation the returning replica's
    /// activated state is either its complete old state or a complete installed checkpoint (never a partial image); the
    /// retry completes from durable progress and converges to the authoritative state.
    /// </summary>
    [Theory]
    [MemberData(nameof(InterruptCases))]
    [Catalog("RSY-012", "S", "INV-01", "INV-07", "INV-18")]
    public Task RSY_012_ResyncInterruptedAtEveryStage(ResyncStage stage, Interruption interruption) =>
        RunAsync(Sim(3), async cluster =>
        {
            var (client, vol) = await ConnectAsync(cluster, "w");
            await using var clientScope = client;
            DfsFileClient files = vol.Files;
            await files.WriteAllBytesAsync("/x", TestData.Bytes(12_000, 2 * Chunk + 9), Ct);
            await files.WriteAllBytesAsync("/gone", TestData.Bytes(12_001, 77), Ct);
            string[] probePaths = ["/x", "/gone", "/new"];
            await cluster.WaitForReplicationAsync(Volume, Ct);
            VolumeStateInspection oldState = await LeaderStateAsync(cluster);
            Dictionary<string, FileInspection?> oldView = probePaths.ToDictionary(p => p, p => oldState.GetFile(p));

            RsyOutage outage = await BeginOutageAsync(cluster, OfflineMode.Stop);
            TestNode c = outage.Roles.Returning;
            byte[] x1 = TestData.Bytes(12_002, 3 * Chunk + 1), fresh = TestData.Bytes(12_003, Chunk + 2);
            await files.WriteAllBytesAsync("/x", x1, Ct);
            await files.DeleteAsync("/gone", Ct);
            await files.WriteAllBytesAsync("/new", fresh, Ct);
            await ForceCompactionAsync(cluster, vol, RunningExcept(cluster, c), outage.ReturningLastIndex, "/fill", 12_100);
            VolumeStateInspection newState = await LeaderStateAsync(cluster);
            Dictionary<string, FileInspection?> newView = probePaths.ToDictionary(p => p, p => newState.GetFile(p));

            // A steady writer keeps producing a log suffix (outside the probed paths) during the catch-up.
            await MkdirAsync(vol, "/suffix");
            bool stopWriting = false;
            int suffixWrites = 0;
            async Task SuffixWriterAsync()
            {
                while (!stopWriting)
                {
                    await files.WriteAllBytesAsync($"/suffix/s{suffixWrites:D5}", TestData.Bytes(12_500 + (ulong)suffixWrites, 32), Ct);
                    suffixWrites++;
                    await cluster.DelayAsync(TimeSpan.FromMilliseconds(20), Ct);
                }
            }

            Task suffixWriter = SuffixWriterAsync();
            string detail = stage switch
            {
                ResyncStage.Receive => ResyncStageDetails.Receive,
                ResyncStage.Verify => ResyncStageDetails.Verify,
                ResyncStage.Install => ResyncStageDetails.Install,
                ResyncStage.SuffixReplay => ResyncStageDetails.Suffix,
                ResyncStage.Activate => ResyncStageDetails.Activate,
                _ => throw new ArgumentOutOfRangeException(nameof(stage)),
            };
            FaultAction action = interruption == Interruption.Crash
                ? FaultAction.CrashNode(CrashMode.LoseUnflushed)
                : FaultAction.Custom(_ =>
                {
                    cluster.Network.Isolate(c.Name);
                    return Task.CompletedTask;
                });
            FaultArm arm = cluster.Faults.Arm(new FaultMatch(FaultSites.RaftSnapshotInstall, Node: c.Name, Volume: Volume),
                ctx => ctx.Detail == detail, action);

            // The interruption may hit before the restart call returns, so the restart is awaited only afterwards.
            Task restart = c.RestartAsync();
            await arm.WaitTriggeredAsync(CatchupTimeout);
            cluster.Faults.AssertReached(arm);
            Assert.All(arm.Contexts, ctx => Assert.Equal(detail, ctx.Detail));

            if (interruption == Interruption.Crash)
            {
                await cluster.WaitUntilAsync(() => Task.FromResult(c.State == TestNodeState.Crashed), TimeSpan.FromSeconds(10),
                    $"the crash at stage '{detail}' takes the returning node down", Ct);
                await AwaitInterruptedRestartAsync(restart, c);
                cluster.Network.Isolate(c.Name);   // inspect what recovered from durable progress before it can make more
                restart = c.RestartAsync();
            }

            // While cut off (both variants), only a complete image can be active. A node recovers locally while partitioned.
            await cluster.WaitUntilAsync(() => Task.FromResult(c.State == TestNodeState.Running), TimeSpan.FromSeconds(10),
                $"{c.Name} runs (cut off) after the interruption at '{detail}'", Ct);
            for (int i = 0; i < 10; i++)
            {
                await AssertWholeImageAsync();
                await cluster.DelayAsync(TimeSpan.FromMilliseconds(100), Ct);
            }

            cluster.Network.Heal();
            await restart;
            await WaitCaughtUpAsync(cluster, c);
            stopWriting = true;
            await suffixWriter;
            await WaitCaughtUpAsync(cluster, c);
            await AssertWholeImageAsync();

            RaftGroupStats stats = await StatsAsync(c);
            Assert.True(stats.SnapshotIndex > outage.ReturningLastIndex, "the retried catch-up installed a checkpoint covering the compacted gap");
            Assert.Equal((long)(stats.AppliedIndex - stats.StartIndex), stats.EntriesApplied + stats.EntriesCoveredByInstall);
            Assert.True(suffixWrites > 0);
            await AssertMatchesLeaderAsync(cluster, c, [.. probePaths, $"/suffix/s{suffixWrites - 1:D5}"]);
            await ServeFromAsync(cluster, c);
            await AssertBytesAsync(files, "/x", x1);
            await AssertBytesAsync(files, "/new", fresh);
            Assert.False(await files.ExistsAsync("/gone", Ct));

            async Task AssertWholeImageAsync()
            {
                if (c.State != TestNodeState.Running)
                    return;
                VolumeStateInspection? st = await c.Inspect.GetVolumeStateAsync(Volume);
                if (st is null)
                    return;
                bool isOld = probePaths.All(p => SameFile(oldView[p], st.GetFile(p)));
                bool isNew = probePaths.All(p => SameFile(newView[p], st.GetFile(p)));
                Assert.True(isOld || isNew,
                    $"{c.Name} activated a partial image at applied index {st.AppliedIndex} after an interruption at '{detail}'");
                if (isOld)
                    Assert.True(st.AppliedIndex <= outage.ReturningLastIndex, "an old image cannot claim progress beyond its own log");
                else
                    Assert.True(st.AppliedIndex > outage.ReturningLastIndex);
            }
        });

    // ------------------------------------------------------------------------------------------------------------
    // RSY-013
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// The catch-up source (the leader) fails part-way through a checkpoint transfer or suffix replay — by crashing, or by
    /// being partitioned with its in-flight messages delivered only after the new source finished. The returning replica
    /// completes against the new leader's valid boundary, never activates a mixture of the two histories, and rejects the
    /// old source's late messages. Five voters: while C's catch-up is held at the fault point the remaining three elect the
    /// new source without needing C's vote (with three voters the election would depend on the blocked node).
    /// </summary>
    [Theory]
    [InlineData(SourceFailure.CrashDuringReceive)]
    [InlineData(SourceFailure.PartitionDuringReceive)]
    [InlineData(SourceFailure.CrashDuringSuffix)]
    [Catalog("RSY-013", "S", "INV-01", "INV-07", "INV-18")]
    public Task RSY_013_SecondLeaderFailureDuringResync(SourceFailure failure) =>
        RunAsync(Sim(5), async cluster =>
        {
            var (client, vol) = await ConnectAsync(cluster, "w");
            await using var clientScope = client;
            DfsFileClient files = vol.Files;
            await files.WriteAllBytesAsync("/x", TestData.Bytes(13_000, 2 * Chunk), Ct);
            string[] probePaths = ["/x", "/y"];
            await cluster.WaitForReplicationAsync(Volume, Ct);
            VolumeStateInspection oldState = await LeaderStateAsync(cluster);
            Dictionary<string, FileInspection?> oldView = probePaths.ToDictionary(p => p, p => oldState.GetFile(p));

            RsyOutage outage = await BeginOutageAsync(cluster, OfflineMode.Stop);
            TestNode c = outage.Roles.Returning;
            byte[] x1 = TestData.Bytes(13_001, 3 * Chunk + 3), y1 = TestData.Bytes(13_002, Chunk + 1);
            await files.WriteAllBytesAsync("/x", x1, Ct);
            await files.WriteAllBytesAsync("/y", y1, Ct);
            await ForceCompactionAsync(cluster, vol, RunningExcept(cluster, c), outage.ReturningLastIndex, "/fill", 13_100);
            VolumeStateInspection newState = await LeaderStateAsync(cluster);
            Dictionary<string, FileInspection?> newView = probePaths.ToDictionary(p => p, p => newState.GetFile(p));

            await MkdirAsync(vol, "/suffix");
            bool stopWriting = false;
            int suffixWrites = 0;
            async Task SuffixWriterAsync()
            {
                while (!stopWriting)
                {
                    try
                    {
                        await files.WriteAllBytesAsync($"/suffix/s{suffixWrites:D5}", TestData.Bytes(13_500 + (ulong)suffixWrites, 32), Ct);
                        suffixWrites++;
                    }
                    catch (Exception ex) when (IsAvailabilityFailure(ex) || ex is DfsCommitOutcomeUnknownException)
                    {
                        // failover in progress
                    }

                    await cluster.DelayAsync(TimeSpan.FromMilliseconds(20), Ct);
                }
            }

            Task suffixWriter = SuffixWriterAsync();
            string detail = failure == SourceFailure.CrashDuringSuffix ? ResyncStageDetails.Suffix : ResyncStageDetails.Receive;
            FaultArm block = cluster.Faults.Arm(new FaultMatch(FaultSites.RaftSnapshotInstall, Node: c.Name, Volume: Volume),
                ctx => ctx.Detail == detail, FaultAction.Block());
            Task restart = c.RestartAsync();   // awaited after the block is released (it may be reached during the restart)
            await block.WaitTriggeredAsync(CatchupTimeout);
            cluster.Faults.AssertReached(block);

            TestNode source = await cluster.GetLeaderAsync(Volume, Ct);
            Assert.NotEqual(c.Name, source.Name);
            ulong sourceTerm = (await GroupAsync(source)).Term;
            MessageHold? lateMessages = null;
            if (failure == SourceFailure.PartitionDuringReceive)
            {
                // Everything the deposed source still sends to C is held back; it is cut from every other participant.
                lateMessages = cluster.Network.Hold(new MessageFilter(From: source.Name, To: c.Name));
                foreach (TestNode n in cluster.Nodes.Where(n => n.Name != source.Name && n.Name != c.Name))
                    cluster.Network.CutBoth(source.Name, n.Name);
                cluster.Network.CutBoth(source.Name, "client:w");
            }
            else
            {
                await source.CrashAsync(CrashMode.LoseUnflushed);
            }

            // A new leader is elected among the others (the stale returning node cannot win).
            await cluster.WaitUntilAsync(async () =>
            {
                TestNode l = await cluster.GetLeaderAsync(Volume, Ct);
                return l.Name != source.Name && (await GroupAsync(l)).Term > sourceTerm;
            }, CatchupTimeout, "a new leader takes over as catch-up source", Ct);
            TestNode newLeader = await cluster.GetLeaderAsync(Volume, Ct);
            Assert.NotEqual(c.Name, newLeader.Name);
            block.Release();
            await restart;

            // The returning node never activates a mixture while it switches sources.
            bool stopObserving = false;
            async Task ObserveAsync()
            {
                while (!stopObserving)
                {
                    if (c.State == TestNodeState.Running && await c.Inspect.GetVolumeStateAsync(Volume) is { } st)
                    {
                        bool isOld = probePaths.All(p => SameFile(oldView[p], st.GetFile(p)));
                        bool isNew = probePaths.All(p => SameFile(newView[p], st.GetFile(p)));
                        Assert.True(isOld || isNew, $"{c.Name} combined histories (applied index {st.AppliedIndex}) after its source failed");
                    }

                    await cluster.DelayAsync(TimeSpan.FromMilliseconds(10), Ct);
                }
            }

            Task observer = ObserveAsync();
            await WaitCaughtUpAsync(cluster, c);
            stopObserving = true;
            await observer;
            RaftGroupStats caughtUp = await StatsAsync(c);
            Assert.True(caughtUp.SnapshotsInstalled >= 1, "the replica completed via a valid checkpoint boundary from the new source");
            Assert.Equal((long)(caughtUp.AppliedIndex - caughtUp.StartIndex), caughtUp.EntriesApplied + caughtUp.EntriesCoveredByInstall);

            if (lateMessages is not null)
            {
                // The deposed source's late transfer/append messages arrive after the new source finished.
                Assert.True(lateMessages.Count > 0, "the deposed source's in-flight messages to the returning node were captured");
                lateMessages.ReleaseAll();
                lateMessages.Dispose();
                await cluster.DelayAsync(TimeSpan.FromSeconds(1), Ct);
                RaftGroupStats afterLate = await StatsAsync(c);
                Assert.True(afterLate.AppliedIndex >= caughtUp.AppliedIndex, "late messages from the old source never roll the replica back");
                Assert.Equal(caughtUp.SnapshotsInstalled, afterLate.SnapshotsInstalled);
                cluster.Network.Heal();
            }

            stopWriting = true;
            await suffixWriter;
            if (source.State == TestNodeState.Crashed)
                await source.RestartAsync();
            await WaitCaughtUpAsync(cluster, c);
            await WaitCaughtUpAsync(cluster, source);
            await cluster.Inspect.AssertNoSplitBrainAsync();
            await AssertMatchesLeaderAsync(cluster, c, probePaths);
            await AssertMatchesLeaderAsync(cluster, source, probePaths);
            await ServeFromAsync(cluster, c);
            await AssertBytesAsync(files, "/x", x1);
            await AssertBytesAsync(files, "/y", y1);
        });

    // ------------------------------------------------------------------------------------------------------------
    // RSY-014
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Writes and reads continue while the replica catches up through a checkpoint: it reaches a moving target while
    /// traffic continues, converges once traffic stops, and the client history is linearizable with every acknowledged
    /// append present exactly once.
    /// </summary>
    [Theory]
    [InlineData(3)]
    [InlineData(5)]
    [Catalog("RSY-014", "S", "INV-01", "INV-07", "INV-18")]
    public Task RSY_014_ConcurrentWritesDuringCatchup(int nodeCount) =>
        RunAsync(Sim(nodeCount), async cluster =>
        {
            var (wClient, w) = await ConnectAsync(cluster, "w");
            await using var wScope = wClient;
            var (rClient, r) = await ConnectAsync(cluster, "r", operationTimeout: TimeSpan.FromSeconds(2));
            await using var rScope = rClient;
            byte[] hot0 = TestData.Bytes(14_000, 256), log0 = TestData.Bytes(14_001, 64);
            await w.Files.WriteAllBytesAsync("/hot", hot0, Ct);
            await w.Files.WriteAllBytesAsync("/log", log0, Ct);

            RsyOutage outage = await BeginOutageAsync(cluster, OfflineMode.Stop);
            TestNode c = outage.Roles.Returning;
            await w.Files.WriteAllBytesAsync("/big", TestData.Bytes(14_002, 3 * Chunk), Ct);
            await ForceCompactionAsync(cluster, w, RunningExcept(cluster, c), outage.ReturningLastIndex, "/fill", 14_100);

            HistoryRecorder history = cluster.History;
            bool stop = false;
            int version = 0;
            byte[] latest = hot0;
            var appended = new List<byte[]>();
            async Task WriterAsync()
            {
                while (!stop)
                {
                    byte[] v = TestData.Bytes(14_200 + (ulong)(++version), 256);
                    await history.RecordAsync("w", "write", "/hot", v, () => w.Files.WriteAllBytesAsync("/hot", v, Ct));
                    latest = v;
                    byte[] suffix = TestData.Bytes(14_600 + (ulong)version, 16);
                    await w.Files.AppendAllBytesAsync("/log", suffix, Ct);
                    appended.Add(suffix);
                    await cluster.DelayAsync(TimeSpan.FromMilliseconds(10), Ct);
                }
            }

            async Task ReaderAsync()
            {
                while (!stop)
                {
                    try
                    {
                        await history.RecordAsync("r", "read", "/hot", null, () => r.Files.ReadAllBytesAsync("/hot", Ct));
                    }
                    catch (Exception ex) when (IsRefusal(ex))
                    {
                        // tolerated; never stale
                    }

                    await cluster.DelayAsync(TimeSpan.FromMilliseconds(15), Ct);
                }
            }

            Task writer = WriterAsync(), reader = ReaderAsync();
            await c.RestartAsync();
            for (int round = 0; round < 3; round++)
            {
                // A moving target: the commit index at this moment must be reached while writes continue.
                ulong target = (await GroupAsync(await cluster.GetLeaderAsync(Volume, Ct))).CommitIndex;
                await cluster.WaitUntilAsync(async () =>
                    c.State == TestNodeState.Running && await c.Inspect.GetGroupAsync(Volume) is { } g && g.AppliedIndex >= target,
                    CatchupTimeout, $"{c.Name} reaches commit index {target} while writes continue (round {round})", Ct);
            }

            stop = true;
            await writer;
            await reader;
            await WaitCaughtUpAsync(cluster, c);
            await AssertCatchupPathAsync(cluster, c, CatchupPath.Checkpoint, null);
            Assert.True(cluster.Faults.Coverage.Hits.Any(h => h.Site == FaultSites.RaftSnapshotInstall && h.Node == c.Name
                    && h.Volume == Volume && h.Detail == ResyncStageDetails.Suffix),
                "the concurrent writes produced a post-checkpoint suffix that the returning node replayed (checkpoint-plus-suffix)");
            await AssertMatchesLeaderAsync(cluster, c, ["/hot", "/log", "/big"]);
            Assert.True(version > 0);
            Assert.All(history.Operations.Where(o => o.Kind == "write"), o => Assert.Equal(OpOutcome.Ok, o.Outcome));
            LinearizabilityChecker.Check(history.Operations.Where(o => o.Key == "/hot").ToList(), new RegisterModel(hot0)).AssertValid();

            await ServeFromAsync(cluster, c);
            await AssertBytesAsync(w.Files, "/hot", latest);
            await AssertBytesAsync(w.Files, "/log", Concat([log0, .. appended]), "every acknowledged append exactly once, in order");
        });

    // ------------------------------------------------------------------------------------------------------------
    // RSY-015
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Transferred checkpoint chunks or object payloads are corrupted (bit flip or truncation in transit; corrupt source
    /// copy at rest). The receiver rejects the bad records, retries verified sources and never activates/stores a corrupt
    /// image; the final state and the repaired copies are byte-exact.
    /// </summary>
    [Theory]
    [InlineData(TransferCorruption.CheckpointChunkBitFlip)]
    [InlineData(TransferCorruption.CheckpointChunkTruncated)]
    [InlineData(TransferCorruption.ObjectPayloadBitFlip)]
    [InlineData(TransferCorruption.RepairSourceCorruptAtRest)]
    [Catalog("RSY-015", "S", "INV-01", "INV-07", "INV-14", "INV-18")]
    public Task RSY_015_ResyncChecksumMismatch(TransferCorruption corruption) =>
        RunAsync(Sim(3), cluster => corruption switch
        {
            TransferCorruption.CheckpointChunkBitFlip => CorruptCheckpointChunkAsync(cluster, truncate: false),
            TransferCorruption.CheckpointChunkTruncated => CorruptCheckpointChunkAsync(cluster, truncate: true),
            TransferCorruption.ObjectPayloadBitFlip => CorruptObjectPayloadAsync(cluster),
            TransferCorruption.RepairSourceCorruptAtRest => CorruptRepairSourceAsync(cluster),
            _ => throw new ArgumentOutOfRangeException(nameof(corruption)),
        });

    private static async Task CorruptCheckpointChunkAsync(TestCluster cluster, bool truncate)
    {
        var (client, vol) = await ConnectAsync(cluster, "w");
        await using var clientScope = client;
        await vol.Files.WriteAllBytesAsync("/x", TestData.Bytes(15_000, 2 * Chunk), Ct);
        await cluster.WaitForReplicationAsync(Volume, Ct);
        VolumeStateInspection oldState = await LeaderStateAsync(cluster);
        RsyOutage outage = await BeginOutageAsync(cluster, OfflineMode.Stop);
        TestNode c = outage.Roles.Returning;
        byte[] x1 = TestData.Bytes(15_001, 3 * Chunk + 5);
        await vol.Files.WriteAllBytesAsync("/x", x1, Ct);
        await ForceCompactionAsync(cluster, vol, RunningExcept(cluster, c), outage.ReturningLastIndex, "/fill", 15_100);
        VolumeStateInspection newState = await LeaderStateAsync(cluster);

        // Capture the first checkpoint chunk; the genuine one is lost and a corrupted copy is delivered instead.
        MessageHold hold = cluster.Network.Hold(new MessageFilter(To: c.Name, Type: MessageType.RaftSnapshotChunk));
        await c.RestartAsync();
        await hold.WaitForAsync(1, CatchupTimeout);
        CapturedMessage original = hold.Captured[0];
        hold.DropAll();
        hold.Dispose();
        // A bit flip in the middle of the frame lands in the checkpoint payload (the bulk of a chunk), not in the trailing
        // framing/extension fields, so the frame still decodes and only checksum verification can catch it.
        byte[] bad = truncate ? original.Frame[..(original.Frame.Length / 2)].ToArray() : Flip(original.Frame, original.Frame.Length / 2);
        DfsErrorCode reply = await cluster.Network.ReplayAsync(original with { Frame = bad });

        // Until a verified checkpoint arrives the replica keeps its complete old image.
        for (int i = 0; i < 5; i++)
        {
            if (await c.Inspect.GetVolumeStateAsync(Volume) is { } st)
            {
                bool isOld = SameFile(oldState.GetFile("/x"), st.GetFile("/x"));
                bool isNew = SameFile(newState.GetFile("/x"), st.GetFile("/x"));
                Assert.True(isOld || isNew, $"{c.Name} activated an image built from a corrupted chunk (applied {st.AppliedIndex})");
            }

            await cluster.DelayAsync(TimeSpan.FromMilliseconds(100), Ct);
        }

        await WaitCaughtUpAsync(cluster, c);
        RaftGroupStats stats = await StatsAsync(c);
        Assert.True(reply != DfsErrorCode.None || stats.SnapshotVerificationFailures >= 1 || stats.SnapshotInstallsAborted >= 1,
            $"the corrupted chunk must be rejected (reply {reply}, verification failures {stats.SnapshotVerificationFailures}, aborted installs {stats.SnapshotInstallsAborted})");
        Assert.True(stats.SnapshotsInstalled >= 1, "the replica retried and installed a verified checkpoint");
        Assert.Equal((long)(stats.AppliedIndex - stats.StartIndex), stats.EntriesApplied + stats.EntriesCoveredByInstall);
        await AssertMatchesLeaderAsync(cluster, c, ["/x"]);
        await ServeFromAsync(cluster, c);
        await AssertBytesAsync(vol.Files, "/x", x1);
    }

    private static async Task CorruptObjectPayloadAsync(TestCluster cluster)
    {
        var (client, vol) = await ConnectAsync(cluster, "w");
        await using var clientScope = client;
        RsyRoles roles = await PickRolesAsync(cluster);
        TestNode target = roles.Returning;
        MessageHold hold = cluster.Network.Hold(new MessageFilter(To: target.Name, Type: MessageType.PutObject));
        byte[] data = TestData.Bytes(15_200, 2 * Chunk);
        Task<CommitToken> write = vol.Files.WriteAllBytesAsync("/p", data, Ct);
        await hold.WaitForAsync(1, TimeSpan.FromSeconds(10));
        CapturedMessage original = hold.Captured[0];
        hold.DropAll();
        hold.Dispose();
        // The 64 KiB chunk payload is the bulk of the frame: a mid-frame flip corrupts payload bytes, not the capability.
        DfsErrorCode reply = await cluster.Network.ReplayAsync(original with { Frame = Flip(original.Frame, original.Frame.Length / 2) });
        Assert.True(reply is DfsErrorCode.Integrity or DfsErrorCode.ProtocolError,
            $"a storage node must reject a payload that does not match its declared checksum (got {reply})");

        CommitToken token = await write;   // ack 2-of-3 from intact uploads; the target is repaired from verified copies
        Assert.False(token.IsNone);
        IReadOnlyList<ObjectId> objects = await ObjectsOfAsync(cluster, "/p");
        await cluster.WaitUntilAsync(() => VerifiedOnAsync(cluster, target.Name, objects), RepairTimeout,
            $"{target.Name} ends up with verified copies of every object", Ct);
        foreach (TestNode n in RunningExcept(cluster, target))
            await DeleteCopiesAsync(n, objects);
        await AssertBytesAsync(vol.Files, "/p", data, "the only remaining copies (on the target) are verified bytes");
    }

    private static async Task CorruptRepairSourceAsync(TestCluster cluster)
    {
        var (client, vol) = await ConnectAsync(cluster, "w");
        await using var clientScope = client;
        byte[] data = TestData.Bytes(15_300, 3 * Chunk + 11);
        await vol.Files.WriteAllBytesAsync("/r", data, Ct);
        await cluster.WaitForRepairAsync(Volume, RepairTimeout, Ct);
        IReadOnlyList<ObjectId> objects = await ObjectsOfAsync(cluster, "/r");
        RsyRoles roles = await PickRolesAsync(cluster);
        (TestNode badSource, TestNode goodSource, TestNode target) = (roles.Leader, roles.Other, roles.Returning);
        long failuresBefore = (await badSource.GetCountersAsync()).ChecksumFailures + (await target.GetCountersAsync()).ChecksumFailures;

        foreach (ObjectId o in objects)
            await badSource.Disk.CorruptObjectAsync(o, await CorruptionOffsetAsync(cluster, "/r", o, 100));
        await DeleteCopiesAsync(target, objects);

        // Phase 1: the only reachable holder is corrupt (the verified holder is down). Repair must not store its bytes on
        // the target, reads fail explicitly, and the deficit is reported.
        await goodSource.StopAsync();
        await cluster.DelayAsync(Timings.InventoryInterval * 16, Ct);
        foreach (ObjectId o in objects)
            Assert.False(await target.Inspect.HasObjectAsync(o), $"{target.Name} stored object {o} copied from the corrupt source");
        Exception noSource = await Assert.ThrowsAnyAsync<Exception>(() => vol.Files.ReadAllBytesAsync("/r", Ct));
        Assert.True(noSource is DfsIntegrityException || IsAvailabilityFailure(noSource),
            $"with only a corrupt copy reachable the read must fail explicitly, got {noSource.GetType().Name}: {noSource.Message}");
        DfsReplicaHealth health = await cluster.Admin.GetReplicaHealthAsync(Volume, Ct);
        Assert.True(health.UnderReplicatedObjects + health.MissingObjects >= objects.Count,
            $"the missing/corrupt copies are reported, not counted as healthy (under-replicated {health.UnderReplicatedObjects}, missing {health.MissingObjects})");

        // Phase 2: the verified source returns; repair rebuilds both the lost and the corrupt copies from it only.
        await goodSource.RestartAsync();
        await cluster.WaitUntilAsync(async () =>
            await VerifiedOnAsync(cluster, target.Name, objects) && await VerifiedOnAsync(cluster, badSource.Name, objects),
            RepairTimeout, "repair rebuilds the lost and the corrupt copies from the verified source only", Ct);
        long failuresAfter = (await badSource.GetCountersAsync()).ChecksumFailures + (await target.GetCountersAsync()).ChecksumFailures;
        Assert.True(failuresAfter > failuresBefore, "the corrupt source copy was detected (scrub or verification of a pulled copy)");
        await AssertBytesAsync(vol.Files, "/r", data);
        await DeleteCopiesAsync(goodSource, objects);
        await DeleteCopiesAsync(badSource, objects);
        await AssertBytesAsync(vol.Files, "/r", data, "the target's repaired copies alone return the exact bytes");
    }

    // ------------------------------------------------------------------------------------------------------------
    // RSY-016
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// All but one payload copy is lost while metadata stays available. Health reports the actual deficit (never healthy
    /// while copies are missing), writes keep the unchanged commit policy (a strict ack-3 write fails while a host is down),
    /// reads stay exact, and repair rebuilds copies from the single verified survivor.
    /// </summary>
    [Theory]
    [InlineData(CopyLoss.SilentDeletionOnTwo)]
    [InlineData(CopyLoss.OfflineAndSilentDeletion)]
    [Catalog("RSY-016", "S", "INV-01", "INV-11", "INV-14", "INV-18")]
    public Task RSY_016_RepairFromOneVerifiedSurvivor(CopyLoss loss) =>
        RunAsync(Sim(3) with
        {
            Timings = NodeTimings.Fast with { RepairBytesPerSecond = 32 << 10 },
            Volumes = [VolumeWithPolicies()],
        }, async cluster =>
        {
            var (client, vol) = await ConnectAsync(cluster, "w");
            await using var clientScope = client;
            byte[] data = TestData.Bytes(16_000, 3 * Chunk + 21);
            await vol.Files.WriteAllBytesAsync("/f", data, Ct);
            await cluster.WaitForRepairAsync(Volume, RepairTimeout, Ct);
            IReadOnlyList<ObjectId> objects = await ObjectsOfAsync(cluster, "/f");
            RsyRoles roles = await PickRolesAsync(cluster);
            TestNode survivor = roles.Other;
            TestNode silent = roles.Leader;
            TestNode second = roles.Returning;

            // The offline holder goes first; the two deletions then happen back to back so repair (32 KiB/s, two seconds per
            // 64 KiB object) cannot re-create a copy before the "single survivor" precondition is checked.
            // (The harness deletes a copy through the running node's store, so the offline holder loses its copies just
            // before it stops.)
            await DeleteCopiesAsync(second, objects);   // offline: its stored copies are gone when it returns
            if (loss == CopyLoss.OfflineAndSilentDeletion)
                await second.StopAsync();
            await DeleteCopiesAsync(silent, objects);

            foreach (ObjectId o in objects)
                Assert.Equal(new[] { survivor.Name }, (await cluster.Inspect.GetObjectHoldersAsync(o)).ToArray());

            // The deficit becomes visible and the reported health never claims more copies than actually exist.
            await cluster.WaitUntilAsync(async () => (await cluster.Admin.GetReplicaHealthAsync(Volume, Ct)).UnderReplicatedObjects >= objects.Count,
                TimeSpan.FromSeconds(10), "inventory reports every object of /f as under-replicated", Ct);
            await AssertBytesAsync(vol.Files, "/f", data, "reads are served from the verified survivor");

            // Write availability follows real hosts: ack-3 needs three live hosts.
            if (loss == CopyLoss.OfflineAndSilentDeletion)
            {
                DfsPlacementUnavailableException ex = await AssertEx.ThrowsDfsAsync<DfsPlacementUnavailableException>(
                    () => vol.Files.WriteAllBytesAsync("/strict", TestData.Bytes(16_001, Chunk), new DfsMutationOptions { StoragePolicy = "strict3" }, Ct),
                    DfsErrorCode.PlacementUnavailable);
                Assert.Equal(3, ex.RequiredCopies);
                Assert.True(ex.AchievedCopies < 3, $"achieved copies {ex.AchievedCopies} must reflect the offline host");
                Assert.False(await vol.Files.ExistsAsync("/strict", Ct));
            }
            else
            {
                Assert.False((await vol.Files.WriteAllBytesAsync("/strict", TestData.Bytes(16_001, Chunk),
                    new DfsMutationOptions { StoragePolicy = "strict3" }, Ct)).IsNone);
            }

            Assert.False((await vol.Files.WriteAllBytesAsync("/default", TestData.Bytes(16_002, Chunk), Ct)).IsNone);

            int expectedCopies = loss == CopyLoss.SilentDeletionOnTwo ? 3 : 2;
            DateTimeOffset deadline = cluster.Time.GetUtcNow() + RepairTimeout;
            while (true)
            {
                DfsReplicaHealth health = await cluster.Admin.GetReplicaHealthAsync(Volume, Ct);
                int minCopies = int.MaxValue;
                foreach (ObjectId o in objects)
                    minCopies = Math.Min(minCopies, (await cluster.Inspect.CountDurableDomainsAsync(o)).Copies);
                if (health.UnderReplicatedObjects == 0)
                    Assert.True(minCopies >= 3, $"health reported no under-replication while an object had only {minCopies} verified copies");
                if (minCopies >= expectedCopies)
                    break;
                Assert.True(cluster.Time.GetUtcNow() < deadline, $"repair from the survivor must reach {expectedCopies} copies (min {minCopies})");
                await cluster.DelayAsync(TimeSpan.FromMilliseconds(250), Ct);
            }

            if (loss == CopyLoss.OfflineAndSilentDeletion)
            {
                Assert.True((await cluster.Admin.GetReplicaHealthAsync(Volume, Ct)).UnderReplicatedObjects >= objects.Count,
                    "with one host offline the target of three copies is still reported as unmet");
                await second.RestartAsync();
                await cluster.WaitForRepairAsync(Volume, RepairTimeout, Ct);
            }

            foreach (ObjectId o in objects)
                Assert.Equal((3, 3), await cluster.Inspect.CountDurableDomainsAsync(o));
            await DeleteCopiesAsync(survivor, objects);
            await AssertBytesAsync(vol.Files, "/f", data, "copies reconstructed from the survivor are exact");
        });

    // ------------------------------------------------------------------------------------------------------------
    // RSY-017
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Every required copy of an object is missing, corrupt, or on an offline host. Reads fail explicitly (integrity or
    /// unavailability) and never return zero-filled or truncated data; healthy extents of the same file stay readable;
    /// health reports the loss; repair cannot invent a source; the namespace entry is not treated as absent.
    /// </summary>
    [Theory]
    [InlineData(NoSourceKind.AllMissing)]
    [InlineData(NoSourceKind.AllCorrupt)]
    [InlineData(NoSourceKind.MixedMissingCorrupt)]
    [InlineData(NoSourceKind.MiddleExtentOnly)]
    [InlineData(NoSourceKind.SoleHolderOffline)]
    [Catalog("RSY-017", "S", "INV-01", "INV-11", "INV-14")]
    public Task RSY_017_NoHealthySourceForObject(NoSourceKind kind) =>
        RunAsync(Sim(3) with { Volumes = [VolumeWithPolicies()] }, async cluster =>
        {
            var (client, vol) = await ConnectAsync(cluster, "w");
            await using var clientScope = client;
            DfsFileClient files = vol.Files;
            bool single = kind == NoSourceKind.SoleHolderOffline;
            byte[] data = single ? TestData.Bytes(17_000, Chunk) : TestData.Bytes(17_001, 3 * Chunk + 5);
            if (single)
                await files.WriteAllBytesAsync("/f", data, new DfsMutationOptions { StoragePolicy = "single1" }, Ct);
            else
                await files.WriteAllBytesAsync("/f", data, Ct);
            await cluster.WaitForRepairAsync(Volume, RepairTimeout, Ct);
            FileInspection file = (await cluster.Inspect.GetFileAsync(Volume, "/f"))!;
            IReadOnlyList<ObjectId> objects = file.Extents.Select(e => e.Object).Distinct().ToList();
            List<ObjectId> affected = kind == NoSourceKind.MiddleExtentOnly ? [file.Extents[1].Object] : [.. objects];
            TestNode? offlineHolder = null;

            switch (kind)
            {
                case NoSourceKind.AllMissing:
                    foreach (TestNode n in cluster.Nodes)
                        await DeleteCopiesAsync(n, affected);
                    break;
                case NoSourceKind.AllCorrupt:
                    foreach (TestNode n in cluster.Nodes)
                    {
                        foreach (ObjectId o in affected)
                            await n.Disk.CorruptObjectAsync(o, 1);
                    }

                    break;
                case NoSourceKind.MixedMissingCorrupt:
                case NoSourceKind.MiddleExtentOnly:
                    await DeleteCopiesAsync(cluster[0], affected);
                    foreach (TestNode n in cluster.Nodes.Skip(1))
                    {
                        foreach (ObjectId o in affected)
                            await n.Disk.CorruptObjectAsync(o, 1);
                    }

                    break;
                case NoSourceKind.SoleHolderOffline:
                    IReadOnlyList<string> holders = await cluster.Inspect.GetObjectHoldersAsync(affected[0]);
                    Assert.Single(holders);
                    offlineHolder = cluster.Node(holders[0]);
                    await offlineHolder.StopAsync();
                    await cluster.GetLeaderAsync(Volume, Ct);
                    break;
            }

            await AssertExplicitFailureAsync();

            // The namespace entry and its metadata are intact: unavailable data is not "absent".
            Assert.True(await files.ExistsAsync("/f", Ct));
            Assert.Equal(data.Length, (await files.GetInfoAsync("/f", Ct)).Length);

            // Healthy extents of the same file remain readable byte-exactly.
            if (kind == NoSourceKind.MiddleExtentOnly)
            {
                ExtentInspection first = file.Extents[0];
                byte[] head = await ReadRangeAsync(files, "/f", first.Offset, (int)first.Length);
                AssertEx.BytesEqual(data.AsSpan((int)first.Offset, (int)first.Length), head, "a healthy extent stays readable");
            }

            await cluster.WaitUntilAsync(async () =>
            {
                DfsReplicaHealth h = await cluster.Admin.GetReplicaHealthAsync(Volume, Ct);
                return h.MissingObjects + h.UnderReplicatedObjects >= affected.Count;
            }, TimeSpan.FromSeconds(10), "the loss is reported as missing/under-replicated objects", Ct);

            // Background repair cannot invent a source: still no verified holder and still an explicit failure.
            await cluster.DelayAsync(Timings.InventoryInterval * 16, Ct);
            if (offlineHolder is null)
            {
                foreach (ObjectId o in affected)
                    Assert.Empty(await cluster.Inspect.GetObjectHoldersAsync(o));
                Assert.True((await cluster.Admin.GetReplicaHealthAsync(Volume, Ct)).MissingObjects >= affected.Count);
            }

            await AssertExplicitFailureAsync();

            if (offlineHolder is not null)
            {
                await offlineHolder.RestartAsync();
                await cluster.WaitUntilAsync(async () =>
                {
                    try
                    {
                        return (await files.ReadAllBytesAsync("/f", Ct)).AsSpan().SequenceEqual(data);
                    }
                    catch (Exception ex) when (IsAvailabilityFailure(ex))
                    {
                        return false;
                    }
                }, CatchupTimeout, "data becomes readable again once its only holder returns", Ct);
            }

            async Task AssertExplicitFailureAsync()
            {
                Exception whole = await Assert.ThrowsAnyAsync<Exception>(() => files.ReadAllBytesAsync("/f", Ct));
                AssertNoSourceFailure(whole);

                // A streaming read returns only genuine bytes before failing — never zero-filled data or a fabricated EOF.
                await using DfsStream s = await files.OpenReadAsync("/f", Ct);
                var buffer = new byte[data.Length];
                int total = 0;
                Exception? streamFailure = null;
                try
                {
                    while (total < buffer.Length)
                    {
                        int n = await s.ReadAsync(buffer.AsMemory(total), Ct);
                        Assert.True(n > 0, $"fabricated EOF at offset {total} of {buffer.Length}");
                        total += n;
                    }
                }
                catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
                {
                    streamFailure = ex;
                }

                Assert.NotNull(streamFailure);
                AssertNoSourceFailure(streamFailure!);
                AssertEx.BytesEqual(data.AsSpan(0, total), buffer.AsSpan(0, total), "bytes returned before the failure are genuine");
            }

            void AssertNoSourceFailure(Exception ex)
            {
                if (kind == NoSourceKind.SoleHolderOffline)
                {
                    // Intact bytes on an unreachable host are an availability condition, not an integrity loss.
                    Assert.True(IsAvailabilityFailure(ex) && ex is not DfsCommitOutcomeUnknownException,
                        $"an offline sole holder must surface as unavailability, got {ex.GetType().Name}: {ex.Message}");
                }
                else if (ex is DfsIntegrityException integrity)
                {
                    Assert.Equal(DfsErrorCode.Integrity, integrity.ErrorCode);
                    Assert.True(integrity.ObjectId is null || affected.Contains(integrity.ObjectId.Value),
                        $"the integrity failure names an affected object (got {integrity.ObjectId})");
                }
                else
                {
                    // RCV-06 allows explicit unavailability as well (e.g. once scrubbing quarantined every corrupt copy, or for
                    // an object no node holds any more); anything else — or success — is wrong.
                    Assert.True(ex is DfsUnavailableException || ex is DfsException { ErrorCode: DfsErrorCode.Unavailable or DfsErrorCode.NodeUnavailable },
                        $"no healthy source must surface as an integrity or unavailability error, got {ex.GetType().Name}: {ex.Message}");
                }
            }
        });

    // ------------------------------------------------------------------------------------------------------------
    // RSY-018
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// The returning replica still stores obsolete immutable objects for paths that were overwritten, or deleted and
    /// recreated with identical bytes. Inventory and placement repair never re-link them: authoritative inode roots,
    /// revisions and extent maps are unchanged, and the obsolete objects are eventually reclaimed.
    /// </summary>
    [Theory]
    [InlineData(CatchupPath.Log)]
    [InlineData(CatchupPath.Checkpoint)]
    [Catalog("RSY-018", "S", "INV-07", "INV-09", "INV-11", "INV-18")]
    public Task RSY_018_StaleObjectsAreNotAuthoritative(CatchupPath path) =>
        RunAsync(Sim(3).WithPath(path), async cluster =>
        {
            var (client, vol) = await ConnectAsync(cluster, "w");
            await using var clientScope = client;
            DfsFileClient files = vol.Files;
            byte[] f1 = TestData.Bytes(18_001, 3 * Chunk), g1 = TestData.Bytes(18_002, 2 * Chunk);
            await files.WriteAllBytesAsync("/f", f1, Ct);
            await files.WriteAllBytesAsync("/g", g1, Ct);
            InodeId oldG = (await files.GetInfoAsync("/g", Ct)).InodeId;
            var obsolete = new HashSet<ObjectId>(await ObjectsOfAsync(cluster, "/f"));
            obsolete.UnionWith(await ObjectsOfAsync(cluster, "/g"));
            await cluster.WaitForRepairAsync(Volume, RepairTimeout, Ct);
            RsyRoles roles = await PickRolesAsync(cluster);
            Assert.True(await HoldsAllAsync(roles.Returning, obsolete), $"precondition: {roles.Returning.Name} stores every soon-obsolete object");

            RsyOutage outage = await BeginOutageAsync(cluster, OfflineMode.Stop, roles);
            TestNode c = outage.Roles.Returning;
            byte[] f2 = TestData.Bytes(18_003, 3 * Chunk);
            await files.WriteAllBytesAsync("/f", f2, Ct);
            await files.DeleteAsync("/g", Ct);
            await files.WriteAllBytesAsync("/g", g1, Ct);            // same path, same bytes, new inode, new objects
            InodeId newG = (await files.GetInfoAsync("/g", Ct)).InodeId;
            Assert.NotEqual(oldG, newG);
            VolumeStateInspection authoritative = await LeaderStateAsync(cluster);
            FileInspection fAuth = authoritative.GetFile("/f")!, gAuth = authoritative.GetFile("/g")!;
            Assert.DoesNotContain(fAuth.Extents, e => obsolete.Contains(e.Object));
            Assert.DoesNotContain(gAuth.Extents, e => obsolete.Contains(e.Object));

            await EndOutageAsync(cluster, outage, path, vol, "/fill", 18_100);

            // Several inventory/repair rounds with C reporting its obsolete objects.
            await cluster.DelayAsync(Timings.InventoryInterval * 12, Ct);
            foreach (TestNode n in cluster.Nodes.Where(n => n.State == TestNodeState.Running))
            {
                VolumeStateInspection st = await StateAsync(n);
                AssertSameFile(fAuth, st.GetFile("/f"), $"{n.Name}: repair must not change the authoritative root of /f");
                AssertSameFile(gAuth, st.GetFile("/g"), $"{n.Name}: repair must not re-link /g to obsolete objects");
                Assert.All(obsolete, o => Assert.DoesNotContain(o, st.ReachableObjects()));
            }

            // The writing client goes away so its staging sessions are closed (disposal or session expiry); unreachable
            // objects with closed staging are then reclaimable.
            await client.DisposeAsync();
            var (readerClient, reader) = await ConnectAsync(cluster, "v");
            await using var readerScope = readerClient;
            files = reader.Files;
            await cluster.WaitUntilAsync(async () =>
            {
                foreach (ObjectId o in obsolete)
                {
                    if ((await cluster.Inspect.GetObjectHoldersAsync(o)).Count > 0)
                        return false;
                }

                return true;
            }, RepairTimeout, "obsolete objects are eventually reclaimed, including the returning node's copies", Ct);

            AssertSameFile(fAuth, (await LeaderStateAsync(cluster)).GetFile("/f"), "after reclamation");
            AssertSameFile(gAuth, (await LeaderStateAsync(cluster)).GetFile("/g"), "after reclamation");
            await ServeFromAsync(cluster, c);
            await AssertBytesAsync(files, "/f", f2);
            await AssertBytesAsync(files, "/g", g1);
            Assert.Equal(newG, (await files.GetInfoAsync("/g", Ct)).InodeId);
        });

    // ------------------------------------------------------------------------------------------------------------
    // RSY-019
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<PolicyDirection, bool> PolicyChangeCases => new()
    {
        { PolicyDirection.Increase, false },
        { PolicyDirection.Increase, true },
        { PolicyDirection.Decrease, false },
        { PolicyDirection.Decrease, true },
    };

    /// <summary>
    /// The required zones change (up or down, optionally with a crash at the reconfiguration transition) while repair of a
    /// lost copy is still running. Copies are never discarded below the recoverable level during the transition, the policy
    /// is declared effective only when every object meets it, the placement epoch never goes backwards, and after the
    /// transition each object holds exactly the new target.
    /// </summary>
    [Theory]
    [MemberData(nameof(PolicyChangeCases))]
    [Catalog("RSY-019", "S", "INV-01", "INV-09", "INV-11", "INV-18")]
    public Task RSY_019_RepairWithPolicyChange(PolicyDirection direction, bool crashAtTransition) =>
        RunAsync(Sim(5) with { Timings = NodeTimings.Fast with { RepairBytesPerSecond = 64 << 10 } }, async cluster =>
        {
            var (client, vol) = await ConnectAsync(cluster, "w");
            await using var clientScope = client;
            byte[] data = TestData.Bytes(19_000, 4 * Chunk + 9);
            await vol.Files.WriteAllBytesAsync("/m", data, Ct);
            await cluster.WaitForRepairAsync(Volume, RepairTimeout, Ct);
            IReadOnlyList<ObjectId> objects = await ObjectsOfAsync(cluster, "/m");
            foreach (ObjectId o in objects)
                Assert.Equal((3, 3), await cluster.Inspect.CountDurableDomainsAsync(o));

            // Start a repair: one holder of every object silently loses its copy.
            foreach (ObjectId o in objects)
            {
                string holder = (await cluster.Inspect.GetObjectHoldersAsync(o)).Order(StringComparer.Ordinal).First();
                await cluster.Node(holder).Disk.DeleteObjectAsync(o);
            }

            DfsStoragePolicy target = direction == PolicyDirection.Increase
                ? new DfsStoragePolicy { TargetCopies = 4, TargetDistinctZones = 4, DurableCopiesBeforeAck = 2, DurableZonesBeforeAck = 2 }
                : new DfsStoragePolicy { TargetCopies = 2, TargetDistinctZones = 2, DurableCopiesBeforeAck = 2, DurableZonesBeforeAck = 2 };
            ulong epoch = (await cluster.Admin.GetClusterStatusAsync(Ct)).PlacementEpoch;
            FaultArm? crash = crashAtTransition
                ? cluster.Faults.Arm(new FaultMatch(FaultSites.Reconfiguration), FaultAction.CrashNode(CrashMode.LoseUnflushed))
                : null;
            for (int attempt = 1; ; attempt++)
            {
                try
                {
                    await cluster.Admin.SetDefaultPolicyAsync(Volume, target, migrateExisting: true, Ct);
                    break;
                }
                catch (Exception ex) when (attempt < 5 && (IsAvailabilityFailure(ex) || ex is DfsCommitOutcomeUnknownException))
                {
                    // the crash at the transition hit the node serving the request; the same change is retried
                    foreach (TestNode n in cluster.Nodes.Where(n => n.State == TestNodeState.Crashed))
                        await n.RestartAsync();
                }
            }

            DateTimeOffset deadline = cluster.Time.GetUtcNow() + RepairTimeout * 2;
            while (true)
            {
                foreach (TestNode n in cluster.Nodes.Where(n => n.State == TestNodeState.Crashed))
                    await n.RestartAsync();
                if (cluster.Nodes.Any(n => n.State != TestNodeState.Running))
                {
                    await cluster.DelayAsync(TimeSpan.FromMilliseconds(100), Ct);
                    continue;
                }

                // Safety at every sample: no copy discarded below the recoverable two-copies-in-two-zones level. Ground truth
                // counts running nodes only, so a sample during which the armed crash took a node down is discarded (its
                // durable copies are intact and return with it).
                var sample = new List<(ObjectId Object, int Copies, int Zones)>();
                foreach (ObjectId o in objects)
                {
                    (int copies, int zones) = await cluster.Inspect.CountDurableDomainsAsync(o);
                    sample.Add((o, copies, zones));
                }

                if (cluster.Nodes.Any(n => n.State != TestNodeState.Running))
                    continue;
                foreach ((ObjectId o, int copies, int zones) in sample)
                    Assert.True(copies >= 2 && zones >= 2, $"object {o} dropped to {copies} copies in {zones} zones during the policy transition");

                DfsPolicyMigrationStatus status;
                try
                {
                    status = await cluster.Admin.GetPolicyMigrationStatusAsync(Volume, Ct);
                    ulong now = (await cluster.Admin.GetClusterStatusAsync(Ct)).PlacementEpoch;
                    Assert.True(now >= epoch, $"placement epoch went backwards ({now} < {epoch})");
                    epoch = now;
                }
                catch (Exception ex) when (IsAvailabilityFailure(ex))
                {
                    await cluster.DelayAsync(TimeSpan.FromMilliseconds(100), Ct);
                    continue;
                }

                if (status.Effective)
                {
                    Assert.Equal(target, status.Target);
                    var effectiveSample = new List<(ObjectId Object, int Copies, int Zones)>();
                    foreach (ObjectId o in objects)
                    {
                        (int copies, int zones) = await cluster.Inspect.CountDurableDomainsAsync(o);
                        effectiveSample.Add((o, copies, zones));
                    }

                    if (cluster.Nodes.Any(n => n.State != TestNodeState.Running))
                        continue;
                    foreach ((ObjectId o, int copies, int zones) in effectiveSample)
                    {
                        Assert.True(copies >= target.TargetCopies && zones >= target.TargetDistinctZones,
                            $"policy declared effective while object {o} has {copies} copies in {zones} zones");
                    }

                    break;
                }

                Assert.True(cluster.Time.GetUtcNow() < deadline, $"policy migration did not complete ({status.PendingObjects} objects pending)");
                await cluster.DelayAsync(TimeSpan.FromMilliseconds(100), Ct);
            }

            if (crash is not null)
                cluster.Faults.AssertReached(crash);

            // Old copies beyond the effective target are deleted only now; each object ends at exactly the new target.
            await cluster.WaitUntilAsync(async () =>
            {
                foreach (ObjectId o in objects)
                {
                    if (await cluster.Inspect.CountDurableDomainsAsync(o) != (target.TargetCopies, target.TargetDistinctZones))
                        return false;
                }

                return true;
            }, RepairTimeout, "every object converges to exactly the effective target placement", Ct);
            Assert.Equal(0, (await cluster.Admin.GetReplicaHealthAsync(Volume, Ct)).PlacementViolations);
            await AssertBytesAsync(vol.Files, "/m", data);
            Assert.Equal(target, (await vol.GetInfoAsync(Ct)).DefaultPolicy);
        });

    // ------------------------------------------------------------------------------------------------------------
    // RSY-020
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// A removed member (or one replaced by a new node in another zone) restarts with its old disk. It cannot vote or
    /// disturb the current term/leader, is not re-admitted to membership, receives no new objects, and its receipts never
    /// satisfy an acknowledgement policy (a strict 3-zone write fails while only it would make up the third zone).
    /// </summary>
    [Theory]
    [InlineData(StaleIdentity.Removed)]
    [InlineData(StaleIdentity.Replaced)]
    [Catalog("RSY-020", "S", "INV-05", "INV-11", "INV-18")]
    public Task RSY_020_RemovedNodeReappears(StaleIdentity identity) =>
        RunAsync(Sim(3) with { Volumes = [VolumeWithPolicies()] }, async cluster =>
        {
            var (client, vol) = await ConnectAsync(cluster, "w");
            await using var clientScope = client;
            byte[] before = TestData.Bytes(20_000, 2 * Chunk);
            await vol.Files.WriteAllBytesAsync("/before", before, Ct);
            await cluster.WaitForRepairAsync(Volume, RepairTimeout, Ct);
            RsyRoles roles = await PickRolesAsync(cluster);
            TestNode stale = roles.Returning;
            NodeId staleId = stale.Id;

            await stale.StopAsync();
            TestNode? replacement = null;
            if (identity == StaleIdentity.Replaced)
                replacement = await cluster.AddNodeAsync(new TestNodeSpec("n3", "z4"), join: true, Ct);
            await cluster.Admin.RemoveNodeAsync(stale.Name, force: true, Ct);
            await cluster.WaitUntilAsync(async () =>
            {
                GroupInspection control = await cluster.Inspect.GetControlGroupAsync();
                GroupInspection volume = await cluster.Inspect.GetVolumeGroupAsync(Volume);
                bool gone = !control.Voters.Contains(stale.Name) && !control.Learners.Contains(stale.Name)
                    && !volume.Voters.Contains(stale.Name) && !volume.Learners.Contains(stale.Name);
                bool admitted = replacement is null
                    || control.Voters.Contains(replacement.Name) || control.Learners.Contains(replacement.Name);
                return gone && admitted;
            }, CatchupTimeout, $"{stale.Name} is removed from every group (and the replacement admitted as a member)", Ct);

            byte[] after = TestData.Bytes(20_001, 2 * Chunk);
            await vol.Files.WriteAllBytesAsync("/after", after, Ct);

            // Baseline once membership changes and any leader balancing have settled: two samples a second apart agree.
            GroupInspection volBefore = await cluster.Inspect.GetVolumeGroupAsync(Volume);
            GroupInspection controlBefore = await cluster.Inspect.GetControlGroupAsync();
            for (int settle = 0; ; settle++)
            {
                await cluster.DelayAsync(TimeSpan.FromSeconds(1), Ct);
                GroupInspection v = await cluster.Inspect.GetVolumeGroupAsync(Volume);
                GroupInspection k = await cluster.Inspect.GetControlGroupAsync();
                if (v.Term == volBefore.Term && v.Leader == volBefore.Leader && k.Term == controlBefore.Term && k.Leader == controlBefore.Leader
                    && SameMembers(v, volBefore) && SameMembers(k, controlBefore))
                {
                    break;
                }

                Assert.True(settle < 10, "terms/leaders/members did not settle after the membership change (before the stale node returned)");
                (volBefore, controlBefore) = (v, k);
            }

            static bool SameMembers(GroupInspection a, GroupInspection b) =>
                a.Voters.Order(StringComparer.Ordinal).SequenceEqual(b.Voters.Order(StringComparer.Ordinal), StringComparer.Ordinal)
                && a.Learners.Order(StringComparer.Ordinal).SequenceEqual(b.Learners.Order(StringComparer.Ordinal), StringComparer.Ordinal);

            // The stale identity reappears with its old data.
            try
            {
                await stale.RestartAsync();
            }
            catch (Exception startFailure) when (startFailure is DfsException or InvalidOperationException or IOException)
            {
                // Refusing to start as a removed member is a legal (stronger) outcome.
            }

            await cluster.DelayAsync(TimeSpan.FromSeconds(5), Ct);   // many election timeouts
            GroupInspection volAfter = await cluster.Inspect.GetVolumeGroupAsync(Volume);
            GroupInspection controlAfter = await cluster.Inspect.GetControlGroupAsync();
            Assert.Equal(volBefore.Term, volAfter.Term);
            Assert.Equal(volBefore.Leader, volAfter.Leader);
            Assert.Equal(controlBefore.Term, controlAfter.Term);
            Assert.Equal(volBefore.Voters.Order(StringComparer.Ordinal).ToArray(), volAfter.Voters.Order(StringComparer.Ordinal).ToArray());
            Assert.DoesNotContain(stale.Name, volAfter.Voters);
            Assert.DoesNotContain(stale.Name, controlAfter.Voters);
            Assert.DoesNotContain(stale.Name, controlAfter.Learners);
            await cluster.Inspect.AssertNoSplitBrainAsync();
            DfsClusterStatus status = await cluster.Admin.GetClusterStatusAsync(Ct);
            Assert.All(status.Nodes.Where(n => n.Id == staleId), n =>
            {
                Assert.Equal(DfsNodeStatus.Removed, n.Status);
                Assert.False(n.IsControlVoter);
            });

            // New data is never placed on it, and its receipts never count.
            byte[] later = TestData.Bytes(20_002, 2 * Chunk);
            await vol.Files.WriteAllBytesAsync("/later", later, Ct);
            List<ObjectId> newObjects = [.. await ObjectsOfAsync(cluster, "/after"), .. await ObjectsOfAsync(cluster, "/later")];
            foreach (ObjectId o in newObjects)
                Assert.DoesNotContain(stale.Name, await cluster.Inspect.GetObjectHoldersAsync(o));

            if (replacement is not null)
            {
                // With the three real members the strict write succeeds without the stale node...
                Assert.False((await vol.Files.WriteAllBytesAsync("/strict-ok", TestData.Bytes(20_003, Chunk),
                    new DfsMutationOptions { StoragePolicy = "strict3" }, Ct)).IsNone);
                foreach (ObjectId o in await ObjectsOfAsync(cluster, "/strict-ok"))
                    Assert.DoesNotContain(stale.Name, await cluster.Inspect.GetObjectHoldersAsync(o));

                // ...and with one real member down, the stale node cannot stand in as the third copy/zone. The member taken
                // down keeps both quorums (whether or not the replacement was also made a volume voter), so the write fails on
                // placement, not on quorum.
                GroupInspection vg = await cluster.Inspect.GetVolumeGroupAsync(Volume);
                GroupInspection cg = await cluster.Inspect.GetControlGroupAsync();
                bool KeepsQuorum(GroupInspection g, string name) =>
                    g.Voters.Count(v => v != name && v != stale.Name && cluster.Node(v).State == TestNodeState.Running) * 2 > g.Voters.Count;
                TestNode down = cluster.Nodes.First(n => n.Name != stale.Name && n.State == TestNodeState.Running
                    && n.Name != vg.Leader && n.Name != cg.Leader && KeepsQuorum(vg, n.Name) && KeepsQuorum(cg, n.Name));
                await down.StopAsync();
            }

            DfsPlacementUnavailableException ex = await AssertEx.ThrowsDfsAsync<DfsPlacementUnavailableException>(
                () => vol.Files.WriteAllBytesAsync("/strict", TestData.Bytes(20_004, Chunk), new DfsMutationOptions { StoragePolicy = "strict3" }, Ct),
                DfsErrorCode.PlacementUnavailable);
            Assert.True(ex.AchievedCopies < 3 || ex.AchievedZones < 3, $"the removed identity's receipts were counted ({ex.AchievedCopies} copies, {ex.AchievedZones} zones)");
            Assert.False(await vol.Files.ExistsAsync("/strict", Ct));
            await AssertBytesAsync(vol.Files, "/before", before);
            await AssertBytesAsync(vol.Files, "/after", after);
        });

    // ------------------------------------------------------------------------------------------------------------
    // RSY-021
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// A fourth node is started from a copy of another node's disk (same node identity), concurrently with the original or
    /// while the original is down and then returns. At most one process is accepted per identity: the duplicate is not a
    /// second membership record, never leads or votes, and its copies/receipts are never counted as an independent
    /// replica (a strict 3-copy write that only the clone could satisfy fails).
    /// </summary>
    [Theory]
    [InlineData(CloneSchedule.Concurrent)]
    [InlineData(CloneSchedule.CloneWhileOriginalDown)]
    [Catalog("RSY-021", "S", "INV-05", "INV-11", "INV-18")]
    public Task RSY_021_DiskCloneNodeIdCollision(CloneSchedule schedule) =>
        RunAsync(Sim(4) with
        {
            ControlVoters = 3,
            Volumes = [VolumeWithPolicies(new DfsMetadataPolicy { Voters = 3, Placement = DfsVoterPlacement.Explicit, ExplicitVoterNodes = ["n0", "n1", "n2"] })],
        }, async cluster =>
        {
            var (client, vol) = await ConnectAsync(cluster, "w");
            await using var clientScope = client;
            byte[] data = TestData.Bytes(21_000, 2 * Chunk);
            await vol.Files.WriteAllBytesAsync("/a", data, Ct);
            Assert.False((await vol.Files.WriteAllBytesAsync("/pre", TestData.Bytes(21_001, Chunk),
                new DfsMutationOptions { StoragePolicy = "strict3" }, Ct)).IsNone, "precondition: strict3 is satisfiable with all four hosts");
            await cluster.WaitForRepairAsync(Volume, RepairTimeout, Ct);

            TestNode original = cluster.Node("n2"), clone = cluster.Node("n3"), other = cluster.Node("n0");
            Assert.DoesNotContain(clone.Name, (await cluster.Inspect.GetControlGroupAsync()).Voters);   // precondition: n3 holds no vote
            NodeId originalId = original.Id;
            await clone.StopAsync();
            await clone.CloneDiskFromAsync(original);
            if (schedule == CloneSchedule.CloneWhileOriginalDown)
                await original.StopAsync();

            bool cloneStarted = true;
            try
            {
                await clone.RestartAsync();
            }
            catch (Exception startFailure) when (startFailure is DfsException or InvalidOperationException or IOException)
            {
                cloneStarted = false;   // refusing to start with a foreign identity is a legal detection
            }

            if (schedule == CloneSchedule.CloneWhileOriginalDown)
            {
                await cluster.DelayAsync(TimeSpan.FromSeconds(2), Ct);
                await original.RestartAsync();
            }

            await cluster.DelayAsync(TimeSpan.FromSeconds(3), Ct);
            await cluster.Inspect.AssertNoSplitBrainAsync();
            DfsClusterStatus status = await cluster.Admin.GetClusterStatusAsync(Ct);
            Assert.Single(status.Nodes, n => n.Id == originalId);
            Assert.True(status.Nodes.Count(n => n.IsReachable && n.Status == DfsNodeStatus.Active) <= 4);
            GroupInspection volume = await cluster.Inspect.GetVolumeGroupAsync(Volume);
            Assert.Equal(new[] { "n0", "n1", "n2" }, volume.Voters.Order(StringComparer.Ordinal).ToArray());
            Assert.NotEqual(clone.Name, volume.Leader);
            if (cloneStarted && clone.State == TestNodeState.Running && await clone.Inspect.GetGroupAsync(Volume) is { } cloneGroup)
                Assert.NotEqual(Tedd.Tdfs.Consensus.RaftRole.Leader, cloneGroup.Role);

            // Only n1, n2 (original identity) and possibly the clone run: one identity cannot supply two copies. Fencing both
            // processes of the duplicated identity (losing the volume quorum) is also safe; counting them twice is not.
            await other.StopAsync();
            Exception failure = await Assert.ThrowsAnyAsync<Exception>(
                () => vol.Files.WriteAllBytesAsync("/strict", TestData.Bytes(21_002, Chunk), new DfsMutationOptions { StoragePolicy = "strict3" }, Ct));
            if (failure is DfsPlacementUnavailableException placement)
            {
                Assert.Equal(DfsErrorCode.PlacementUnavailable, placement.ErrorCode);
                Assert.True(placement.AchievedCopies <= 2, $"the cloned identity was counted as an independent replica ({placement.AchievedCopies} copies)");
            }
            else
            {
                Assert.True(IsAvailabilityFailure(failure), $"a strict write must fail explicitly, got {failure.GetType().Name}: {failure.Message}");
            }

            await other.RestartAsync();
            await cluster.WaitUntilAsync(async () =>
            {
                try
                {
                    DfsGroupStatus g = (await cluster.Admin.GetVolumeStatusAsync(Volume, Ct)).Group;
                    return g.HasQuorum && g.Leader is not null;
                }
                catch (Exception ex) when (IsAvailabilityFailure(ex))
                {
                    return false;
                }
            }, CatchupTimeout, "the volume regains a leader with quorum once n0 is back", Ct);
            await cluster.Inspect.AssertNoSplitBrainAsync();
            Assert.False(await vol.Files.ExistsAsync("/strict", Ct), "the rejected strict write never became visible");
            await AssertBytesAsync(vol.Files, "/a", data);
            await AssertBytesAsync(vol.Files, "/pre", TestData.Bytes(21_001, Chunk));
        });

    // ------------------------------------------------------------------------------------------------------------
    // RSY-022
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// The returning node's disk fills during catch-up (checkpoint install, or data repair after log catch-up). It keeps
    /// its last valid recoverable state (also across a crash), reports the failed install / missing copies instead of
    /// claiming health, and completes once space is available.
    /// </summary>
    [Theory]
    [InlineData(DiskFullPhase.CheckpointInstall)]
    [InlineData(DiskFullPhase.DataRepair)]
    [Catalog("RSY-022", "S", "INV-01", "INV-11", "INV-18")]
    public Task RSY_022_ResyncDiskFull(DiskFullPhase phase) =>
        RunAsync(Sim(3).WithPath(phase == DiskFullPhase.DataRepair ? CatchupPath.Log : CatchupPath.Checkpoint), async cluster =>
        {
            var (client, vol) = await ConnectAsync(cluster, "w");
            await using var clientScope = client;
            await vol.Files.WriteAllBytesAsync("/x", TestData.Bytes(22_000, 2 * Chunk), Ct);
            await cluster.WaitForReplicationAsync(Volume, Ct);
            VolumeStateInspection oldState = await LeaderStateAsync(cluster);

            RsyOutage outage = await BeginOutageAsync(cluster, OfflineMode.Stop);
            TestNode c = outage.Roles.Returning;
            byte[] x1 = TestData.Bytes(22_001, 4 * Chunk), big = TestData.Bytes(22_002, 8 * Chunk);
            await vol.Files.WriteAllBytesAsync("/x", x1, Ct);
            await vol.Files.WriteAllBytesAsync("/big", big, Ct);
            var newObjects = new HashSet<ObjectId>(await ObjectsOfAsync(cluster, "/x"));
            newObjects.UnionWith(await ObjectsOfAsync(cluster, "/big"));
            if (phase == DiskFullPhase.CheckpointInstall)
            {
                // Enough inline metadata that the checkpoint cannot fit in the remaining 16 KiB.
                await MkdirAsync(vol, "/meta");
                for (int i = 0; i < 400; i += 20)
                {
                    await Task.WhenAll(Enumerable.Range(i, 20).Select(n =>
                        vol.Files.WriteAllBytesAsync($"/meta/f{n:D3}", TestData.Bytes(22_200 + (ulong)n, 200), Ct)));
                }

                await ForceCompactionAsync(cluster, vol, RunningExcept(cluster, c), outage.ReturningLastIndex, "/fill", 22_100);
            }
            else
                await AssertMissingRangeRetainedAsync(RunningExcept(cluster, c), outage.ReturningAppliedIndex);

            // No room for the checkpoint (16 KiB), respectively for any 64 KiB data object (48 KiB: repair may legitimately
            // use the reserved repair space, so only the physical capacity can rule a copy out) while the few KiB of log
            // entries still fit.
            c.Disk.Faults.CapacityOverride = c.Disk.UsedBytes + (phase == DiskFullPhase.CheckpointInstall ? 16L << 10 : 48L << 10);
            await c.RestartAsync();

            if (phase == DiskFullPhase.CheckpointInstall)
            {
                await cluster.WaitUntilAsync(async () => (await StatsAsync(c)).SnapshotInstallsAborted >= 1, CatchupTimeout,
                    "the failed checkpoint install is reported", Ct);
                await AssertOldImageAsync();
                await c.CrashAsync(CrashMode.LoseUnflushed);
                await c.RestartAsync();
                await AssertOldImageAsync();   // the last valid recoverable state survives a crash while full
                await cluster.DelayAsync(TimeSpan.FromSeconds(2), Ct);
                GroupInspection stuck = await GroupAsync(c);
                Assert.True(stuck.AppliedIndex <= outage.ReturningLastIndex, "an uninstallable checkpoint is never reported as caught up");
                foreach (ObjectId o in newObjects)
                    Assert.DoesNotContain(c.Name, await cluster.Inspect.GetObjectHoldersAsync(o));
                DfsReplicaHealth health = await cluster.Admin.GetReplicaHealthAsync(Volume, Ct);
                Assert.True(health.UnderReplicatedObjects >= newObjects.Count,
                    $"the full node's missing copies must be reported (under-replicated {health.UnderReplicatedObjects} < {newObjects.Count})");
            }
            else
            {
                await WaitCaughtUpAsync(cluster, c);   // metadata fits in the reserve
                await cluster.DelayAsync(Timings.InventoryInterval * 12, Ct);
                foreach (ObjectId o in newObjects)
                    Assert.DoesNotContain(c.Name, await cluster.Inspect.GetObjectHoldersAsync(o));
                DfsReplicaHealth health = await cluster.Admin.GetReplicaHealthAsync(Volume, Ct);
                Assert.True(health.UnderReplicatedObjects >= newObjects.Count,
                    $"copies that could not be stored must be reported missing (under-replicated {health.UnderReplicatedObjects} < {newObjects.Count})");
                Assert.True(health.RepairBacklogBytes > 0);
                await AssertBytesAsync(vol.Files, "/big", big);
            }

            c.Disk.Faults.CapacityOverride = null;
            await WaitCaughtUpAsync(cluster, c);
            await cluster.WaitForRepairAsync(Volume, RepairTimeout, Ct);
            Assert.True(await VerifiedOnAsync(cluster, c.Name, newObjects), $"{c.Name} holds verified copies once space is available");
            await AssertMatchesLeaderAsync(cluster, c, ["/x", "/big"]);
            await ServeFromAsync(cluster, c);
            await AssertBytesAsync(vol.Files, "/x", x1);

            async Task AssertOldImageAsync()
            {
                VolumeStateInspection st = await StateAsync(c);
                Assert.True(st.AppliedIndex <= outage.ReturningLastIndex, $"{c.Name} activated a partial checkpoint while its disk was full");
                AssertSameFile(oldState.GetFile("/x"), st.GetFile("/x"), $"{c.Name} keeps its last valid state");
                Assert.Null(st.GetFile("/big"));
            }
        });

    // ------------------------------------------------------------------------------------------------------------
    // RSY-023
    // ------------------------------------------------------------------------------------------------------------

    private const long RepairBudget = 256L << 10;

    /// <summary>
    /// Repair of 2 MiB runs against continuous foreground traffic with a 256 KiB/s per-node repair budget, optionally while
    /// network bandwidth or destination capacity is starved and later restored. Every node honours the budget over every
    /// sampled window, foreground operations keep succeeding with bounded latency, and repair completes within a bound
    /// derived from the budget once bandwidth/capacity suffice.
    /// </summary>
    [Theory]
    [InlineData(RepairLimit.BudgetOnly)]
    [InlineData(RepairLimit.BandwidthStarvedThenRestored)]
    [InlineData(RepairLimit.CapacityStarvedThenRestored)]
    [Catalog("RSY-023", "S", "INV-11", "INV-16", "INV-17", "INV-18")]
    public Task RSY_023_RepairRateLimited(RepairLimit limit) =>
        RunAsync(Sim(3) with { Timings = NodeTimings.Fast with { RepairBytesPerSecond = RepairBudget } }, async cluster =>
        {
            var (client, vol) = await ConnectAsync(cluster, "w");
            await using var clientScope = client;
            await MkdirAsync(vol, "/r");
            await MkdirAsync(vol, "/fg");
            var objects = new HashSet<ObjectId>();
            byte[] probe = TestData.Bytes(23_000, 2 * Chunk);
            for (int i = 0; i < 16; i++)
            {
                byte[] bytes = i == 0 ? probe : TestData.Bytes(23_000 + (ulong)i, 2 * Chunk);
                await vol.Files.WriteAllBytesAsync($"/r/f{i:D2}", bytes, Ct);
                objects.UnionWith(await ObjectsOfAsync(cluster, $"/r/f{i:D2}"));
            }

            await cluster.WaitForRepairAsync(Volume, RepairTimeout, Ct);
            RsyRoles roles = await PickRolesAsync(cluster);
            TestNode target = roles.Returning;
            long repairBytesNeeded = 16L * 2 * Chunk;
            await DeleteCopiesAsync(target, objects);
            if (limit == RepairLimit.BandwidthStarvedThenRestored)
            {
                foreach (TestNode n in RunningExcept(cluster, target))
                {
                    cluster.Network.SetBandwidth(n.Name, target.Name, 8 << 10);
                    cluster.Network.SetBandwidth(target.Name, n.Name, 8 << 10);
                }
            }
            else if (limit == RepairLimit.CapacityStarvedThenRestored)
            {
                target.Disk.Faults.CapacityOverride = target.Disk.UsedBytes + (64 << 10);
            }

            // Foreground traffic for the whole run.
            bool stop = false;
            int fgOk = 0, fgFailed = 0;
            TimeSpan fgMaxLatency = TimeSpan.Zero;
            async Task ForegroundAsync()
            {
                int i = 0;
                while (!stop)
                {
                    long start = cluster.Time.GetTimestamp();
                    try
                    {
                        await vol.Files.WriteAllBytesAsync($"/fg/f{i++ % 32:D2}", TestData.Bytes(23_100 + (ulong)i, 16 << 10), Ct);
                        await AssertBytesAsync(vol.Files, "/r/f00", probe);
                        fgOk++;
                    }
                    catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
                    {
                        fgFailed++;
                    }

                    TimeSpan latency = cluster.Time.GetElapsedTime(start);
                    if (latency > fgMaxLatency)
                        fgMaxLatency = latency;
                    await cluster.DelayAsync(TimeSpan.FromMilliseconds(50), Ct);
                }
            }

            var samples = new List<(TimeSpan At, Dictionary<string, long> RepairBytes)>();
            long t0 = cluster.Time.GetTimestamp();
            async Task SampleAsync()
            {
                var perNode = new Dictionary<string, long>(StringComparer.Ordinal);
                foreach (TestNode n in cluster.Nodes.Where(n => n.State == TestNodeState.Running))
                    perNode[n.Name] = (await n.GetCountersAsync()).RepairBytes;
                samples.Add((cluster.Time.GetElapsedTime(t0), perNode));
            }

            Task foreground = ForegroundAsync();
            if (limit != RepairLimit.BudgetOnly)
            {
                for (int i = 0; i < 8; i++)
                {
                    await SampleAsync();
                    await cluster.DelayAsync(TimeSpan.FromMilliseconds(500), Ct);
                }

                Assert.False(await HoldsAllAsync(target, objects), "precondition: repair cannot complete while starved");
                foreach (TestNode n in RunningExcept(cluster, target))
                {
                    cluster.Network.SetBandwidth(n.Name, target.Name, long.MaxValue);
                    cluster.Network.SetBandwidth(target.Name, n.Name, long.MaxValue);
                }

                target.Disk.Faults.CapacityOverride = null;
            }

            TimeSpan restoredAt = cluster.Time.GetElapsedTime(t0);
            TimeSpan bound = TimeSpan.FromSeconds(repairBytesNeeded / (double)RepairBudget * 2 + 10);
            while (!await HoldsAllAsync(target, objects))
            {
                Assert.True(cluster.Time.GetElapsedTime(t0) - restoredAt < bound, $"repair did not complete within {bound} after resources were restored");
                await SampleAsync();
                await cluster.DelayAsync(TimeSpan.FromMilliseconds(500), Ct);
            }

            await SampleAsync();
            TimeSpan repairDuration = cluster.Time.GetElapsedTime(t0) - restoredAt;
            stop = true;
            await foreground;

            // Budget honoured by every node over every sampled window of at least one second (one second of burst allowed).
            for (int i = 0; i < samples.Count; i++)
            {
                for (int j = i + 1; j < samples.Count; j++)
                {
                    double seconds = (samples[j].At - samples[i].At).TotalSeconds;
                    if (seconds < 1)
                        continue;
                    foreach ((string node, long bytes) in samples[j].RepairBytes)
                    {
                        if (!samples[i].RepairBytes.TryGetValue(node, out long earlier) || bytes < earlier)
                            continue;   // node restarted between samples
                        long allowed = (long)(RepairBudget * (seconds + 1));
                        Assert.True(bytes - earlier <= allowed,
                            $"{node} repaired {bytes - earlier} bytes in {seconds:F1}s, above the {RepairBudget} B/s budget (allowed {allowed})");
                    }
                }
            }

            Assert.True(repairDuration.TotalSeconds >= repairBytesNeeded / (double)RepairBudget / 2,
                $"repair of {repairBytesNeeded} bytes finished in {repairDuration}, faster than the budget allows");
            Assert.Equal(0, fgFailed);
            Assert.True(fgOk > 0);
            Assert.True(fgMaxLatency < TimeSpan.FromSeconds(2), $"foreground latency {fgMaxLatency} while repairing");
            Log($"RSY-023 {limit}: repaired {repairBytesNeeded} B in {repairDuration.TotalSeconds:F1}s after restore " +
                $"({repairBytesNeeded / Math.Max(0.001, repairDuration.TotalSeconds):F0} B/s, budget {RepairBudget} B/s); foreground ok={fgOk} max latency={fgMaxLatency.TotalMilliseconds:F0}ms");
            await DeleteCopiesAsync(roles.Leader, objects);
            await DeleteCopiesAsync(roles.Other, objects);
            await AssertBytesAsync(vol.Files, "/r/f00", probe, "repaired copies are exact");
        });

    // ------------------------------------------------------------------------------------------------------------
    // RSY-024
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// A bounded, seeded sequence alternates corruption, pause/resume (availability flapping) and crash/restart
    /// announcements. After the faults stop and repair converges, placement is stable: over a long quiet window no repair
    /// bytes move, no replica is deleted, holder sets are unchanged and every object has exactly its target placement.
    /// </summary>
    [Theory]
    [InlineData(1UL)]
    [InlineData(2UL)]
    [InlineData(3UL)]
    [Catalog("RSY-024", "S", "INV-09", "INV-11", "INV-17", "INV-18")]
    public Task RSY_024_NoInfinitePingPongRepair(ulong scheduleSeed) =>
        RunAsync(Sim(3) with { Seed = 24_000 + scheduleSeed }, async cluster =>
        {
            var (client, vol) = await ConnectAsync(cluster, "w");
            await using var clientScope = client;
            await MkdirAsync(vol, "/p");
            var files = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            var objects = new HashSet<ObjectId>();
            for (int i = 0; i < 6; i++)
            {
                string path = $"/p/f{i}";
                files[path] = TestData.Bytes(24_100 + (ulong)i, Chunk + 100 * i);
                await vol.Files.WriteAllBytesAsync(path, files[path], Ct);
                objects.UnionWith(await ObjectsOfAsync(cluster, path));
            }

            await cluster.WaitForRepairAsync(Volume, RepairTimeout, Ct);
            var random = new SeededRandom(scheduleSeed);
            ObjectId[] objectList = objects.OrderBy(o => o.Value).ToArray();
            for (int step = 0; step < 9; step++)
            {
                TestNode n = cluster[random.Next(cluster.Nodes.Count)];
                switch (step % 3)
                {
                    case 0:
                        await n.Disk.CorruptObjectAsync(objectList[random.Next(objectList.Length)]);
                        break;
                    case 1:
                        await n.PauseAsync();
                        await cluster.DelayAsync(Timings.InventoryInterval * 3, Ct);
                        await n.ResumeAsync();
                        break;
                    default:
                        await n.CrashAsync(CrashMode.LoseUnflushed);
                        await cluster.DelayAsync(TimeSpan.FromSeconds(1), Ct);
                        await n.RestartAsync();
                        break;
                }

                await cluster.DelayAsync(Timings.InventoryInterval * 2, Ct);
            }

            // Faults stop: scrub every node so latent corruption from the sequence is found, then wait for convergence (product
            // view and harness ground truth).
            await cluster.WaitForStableAsync(CatchupTimeout, Ct);
            await cluster.Admin.StartScrubAsync(null, Ct);
            await cluster.WaitForRepairAsync(Volume, RepairTimeout, Ct);
            await cluster.WaitUntilAsync(async () =>
            {
                foreach (ObjectId o in objects)
                {
                    if (await cluster.Inspect.CountDurableDomainsAsync(o) != (3, 3))
                        return false;
                }

                return true;
            }, RepairTimeout, "every object regains three verified copies in three zones once faults stop", Ct);
            await cluster.WaitUntilAsync(async () =>
            {
                DfsReplicaHealth h = await cluster.Admin.GetReplicaHealthAsync(Volume, Ct);
                return h.UnderReplicatedObjects == 0 && h.ZoneDeficitObjects == 0 && h.MissingObjects == 0
                    && h.PlacementViolations == 0 && h.RepairBacklogBytes == 0;
            }, RepairTimeout, "replica health converges once faults stop", Ct);

            Dictionary<ObjectId, string[]> holdersBefore = await HoldersAsync(cluster, objects);
            var countersBefore = new Dictionary<string, NodeCounters>(StringComparer.Ordinal);
            foreach (TestNode n in cluster.Nodes)
                countersBefore[n.Name] = await n.GetCountersAsync();

            await cluster.DelayAsync(Timings.InventoryInterval * 40, Ct);   // a long quiet window (40 inventory rounds)

            Dictionary<ObjectId, string[]> holdersAfter = await HoldersAsync(cluster, objects);
            foreach (ObjectId o in objects)
            {
                Assert.Equal(holdersBefore[o], holdersAfter[o]);
                Assert.Equal((3, 3), await cluster.Inspect.CountDurableDomainsAsync(o));
            }

            foreach (TestNode n in cluster.Nodes)
            {
                NodeCounters after = await n.GetCountersAsync();
                NodeCounters before = countersBefore[n.Name];
                // A copy/delete cycle of a healthy replica needs a new store (puts/repair bytes) to stay invisible in the holder
                // sets checked above; quarantine cleanup of already-replaced corrupt records is not ping-pong.
                Assert.True(after.RepairBytes == before.RepairBytes, $"{n.Name} kept repairing healthy replicas ({after.RepairBytes - before.RepairBytes} bytes)");
                Assert.True(after.ObjectPuts == before.ObjectPuts, $"{n.Name} kept storing copies ({after.ObjectPuts - before.ObjectPuts})");
            }

            foreach ((string path, byte[] bytes) in files)
                await AssertBytesAsync(vol.Files, path, bytes);
        });

    // ------------------------------------------------------------------------------------------------------------
    // RSY-025
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// A background replication to one target is missed, or a stored copy is silently corrupted or deleted. Inventory /
    /// scrubbing finds the divergence, repair re-creates a verified copy from authoritative references, health returns to
    /// clean, and the repaired copy alone serves the exact bytes.
    /// </summary>
    [Theory]
    [InlineData(SilentDivergence.MissedReplication)]
    [InlineData(SilentDivergence.SilentCorruption)]
    [InlineData(SilentDivergence.SilentDeletion)]
    [Catalog("RSY-025", "S", "INV-01", "INV-11", "INV-14", "INV-18")]
    public Task RSY_025_AntiEntropyFindsSilentDivergence(SilentDivergence divergence) =>
        RunAsync(Sim(3), async cluster =>
        {
            var (client, vol) = await ConnectAsync(cluster, "w");
            await using var clientScope = client;
            RsyRoles roles = await PickRolesAsync(cluster);
            TestNode target = roles.Returning;
            byte[] data = TestData.Bytes(25_000, 2 * Chunk + 1);
            long checksumFailuresBefore = (await target.GetCountersAsync()).ChecksumFailures;
            IReadOnlyList<ObjectId> objects;

            if (divergence == SilentDivergence.MissedReplication)
            {
                // Every way a copy can reach the target is cut until the divergence is established: client uploads and pushed
                // replication to it, and repair pulls (GetObject) from it — otherwise anti-entropy could already have fixed the
                // gap before the precondition below is checked.
                MessageRule dropPuts = cluster.Network.DropNext(new MessageFilter(To: target.Name, Type: MessageType.PutObject), 100_000);
                MessageRule dropReplicas = cluster.Network.DropNext(new MessageFilter(To: target.Name, Type: MessageType.ReplicateObject), 100_000);
                MessageRule dropPulls = cluster.Network.DropNext(new MessageFilter(From: target.Name, Type: MessageType.GetObject), 100_000);
                CommitToken token = await vol.Files.WriteAllBytesAsync("/m", data, Ct);
                Assert.False(token.IsNone, "ack 2-of-3 succeeds without the third copy");
                objects = await ObjectsOfAsync(cluster, "/m");
                Assert.False(await HoldsAllAsync(target, objects), "precondition: the replication to the target was missed");
                foreach (ObjectId o in objects)
                {
                    (int copies, int zones) = await cluster.Inspect.CountDurableDomainsAsync(o);
                    Assert.True(copies >= 2 && zones >= 2);
                }

                dropPuts.Cancel();
                dropReplicas.Cancel();
                dropPulls.Cancel();
                Assert.True(dropPuts.Matched + dropReplicas.Matched > 0);
            }
            else
            {
                await vol.Files.WriteAllBytesAsync("/m", data, Ct);
                await cluster.WaitForRepairAsync(Volume, RepairTimeout, Ct);
                objects = await ObjectsOfAsync(cluster, "/m");
                Assert.True(await VerifiedOnAsync(cluster, target.Name, objects));
                if (divergence == SilentDivergence.SilentCorruption)
                {
                    foreach (ObjectId o in objects)
                        await target.Disk.CorruptObjectAsync(o, await CorruptionOffsetAsync(cluster, "/m", o, 7));
                    await cluster.Admin.StartScrubAsync(target.Name, Ct);
                }
                else
                {
                    await DeleteCopiesAsync(target, objects);
                }
            }

            await AssertBytesAsync(vol.Files, "/m", data, "reads stay verified while the divergence exists");
            await cluster.WaitUntilAsync(() => VerifiedOnAsync(cluster, target.Name, objects), RepairTimeout,
                $"anti-entropy restores a verified copy on {target.Name}", Ct);
            if (divergence == SilentDivergence.SilentCorruption)
                Assert.True((await target.GetCountersAsync()).ChecksumFailures > checksumFailuresBefore, "scrubbing detected the silent corruption");
            await cluster.WaitUntilAsync(async () =>
            {
                DfsReplicaHealth h = await cluster.Admin.GetReplicaHealthAsync(Volume, Ct);
                return h.UnderReplicatedObjects == 0 && h.MissingObjects == 0;
            }, RepairTimeout, "health is clean after repair", Ct);

            foreach (TestNode n in RunningExcept(cluster, target))
                await DeleteCopiesAsync(n, objects);
            await AssertBytesAsync(vol.Files, "/m", data, "the repaired copy alone serves the authoritative bytes");
        });

    // ------------------------------------------------------------------------------------------------------------
    // RSY-026
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// The checkpoint chunks a replica once received are captured and re-delivered after it holds newer state (a newer
    /// installed checkpoint, a later leader term, or a restart from newer durable state). The delayed older checkpoint is
    /// rejected: no install, no decrease of the applied index, no change of the current file mapping, also across a crash.
    /// </summary>
    [Theory]
    [InlineData(OldCheckpointDelivery.AfterNewerInstall)]
    [InlineData(OldCheckpointDelivery.AfterLeaderChange)]
    [InlineData(OldCheckpointDelivery.AfterRestart)]
    [Catalog("RSY-026", "S", "INV-01", "INV-07", "INV-18")]
    public Task RSY_026_CheckpointDoesNotRollbackCurrentState(OldCheckpointDelivery delivery) =>
        RunAsync(Sim(3), async cluster =>
        {
            var (client, vol) = await ConnectAsync(cluster, "w");
            await using var clientScope = client;
            await vol.Files.WriteAllBytesAsync("/x", TestData.Bytes(26_000, 2 * Chunk), Ct);
            RsyOutage outage = await BeginOutageAsync(cluster, OfflineMode.Stop);
            TestNode c = outage.Roles.Returning;
            await vol.Files.WriteAllBytesAsync("/x", TestData.Bytes(26_001, 2 * Chunk + 1), Ct);
            await ForceCompactionAsync(cluster, vol, RunningExcept(cluster, c), outage.ReturningLastIndex, "/fill", 26_100);

            // Capture every checkpoint chunk sent to C while letting them through.
            MessageHold spy = cluster.Network.Hold(new MessageFilter(To: c.Name, Type: MessageType.RaftSnapshotChunk));
            await c.RestartAsync();
            ulong target = (await GroupAsync(await cluster.GetLeaderAsync(Volume, Ct))).CommitIndex;
            await cluster.WaitUntilAsync(async () =>
            {
                spy.ReleaseAll();
                return c.State == TestNodeState.Running && await c.Inspect.GetGroupAsync(Volume) is { } g && g.AppliedIndex >= target;
            }, CatchupTimeout, $"{c.Name} catches up through the first checkpoint", Ct);
            List<CapturedMessage> oldChunks = [.. spy.Captured];
            spy.Dispose();
            Assert.NotEmpty(oldChunks);
            long installsAfterFirst = (await StatsAsync(c)).SnapshotsInstalled;
            Assert.True(installsAfterFirst >= 1);

            // Newer state is installed/committed on C.
            byte[] x2 = TestData.Bytes(26_002, 3 * Chunk + 2);
            await vol.Files.WriteAllBytesAsync("/x", x2, Ct);
            switch (delivery)
            {
                case OldCheckpointDelivery.AfterNewerInstall:
                    ulong cLast = (await GroupAsync(c)).LastIndex;
                    cluster.Network.Isolate(c.Name);
                    await ForceCompactionAsync(cluster, vol, RunningExcept(cluster, c), cLast, "/fill2", 26_500);
                    cluster.Network.Heal();
                    await WaitCaughtUpAsync(cluster, c);
                    Assert.True((await StatsAsync(c)).SnapshotsInstalled > installsAfterFirst, "C installed a newer checkpoint");
                    break;
                case OldCheckpointDelivery.AfterLeaderChange:
                    TestNode currentLeader = await cluster.GetLeaderAsync(Volume, Ct);
                    TestNode newLeader = RunningExcept(cluster, c).First(n => n.Name != currentLeader.Name);
                    ulong oldTerm = (await GroupAsync(currentLeader)).Term;
                    await cluster.TransferLeadershipAsync(Volume, newLeader.Name, TimeSpan.FromSeconds(20), ct: Ct);
                    Assert.True((await GroupAsync(newLeader)).Term > oldTerm, "the leader change moved to a newer term");
                    await vol.Files.AppendAllBytesAsync("/x", TestData.Bytes(26_003, 10), Ct);
                    x2 = Concat(x2, TestData.Bytes(26_003, 10));
                    await WaitCaughtUpAsync(cluster, c);
                    break;
                case OldCheckpointDelivery.AfterRestart:
                    await WaitCaughtUpAsync(cluster, c);
                    await c.StopAsync();
                    await c.RestartAsync();
                    await WaitCaughtUpAsync(cluster, c);
                    break;
            }

            await cluster.WaitForReplicationAsync(Volume, Ct);
            RaftGroupStats before = await StatsAsync(c);
            FileInspection current = (await StateAsync(c)).GetFile("/x")!;

            // Deliver the delayed older checkpoint.
            foreach (CapturedMessage m in oldChunks)
                await cluster.Network.ReplayAsync(m);
            for (int i = 0; i < 10; i++)
            {
                RaftGroupStats now = await StatsAsync(c);
                Assert.True(now.AppliedIndex >= before.AppliedIndex, $"{c.Name} rolled back from {before.AppliedIndex} to {now.AppliedIndex}");
                AssertSameFile(current, (await StateAsync(c)).GetFile("/x"), "the delayed older checkpoint must not replace live state");
                await cluster.DelayAsync(TimeSpan.FromMilliseconds(100), Ct);
            }

            RaftGroupStats after = await StatsAsync(c);
            Assert.Equal(before.SnapshotsInstalled, after.SnapshotsInstalled);
            Assert.True(after.SnapshotsRejectedStale > before.SnapshotsRejectedStale, "the stale checkpoint was explicitly rejected");
            await AssertMatchesLeaderAsync(cluster, c, ["/x"]);

            // Nothing of the old checkpoint was persisted either.
            await c.CrashAsync(CrashMode.LoseUnflushed);
            await c.RestartAsync();
            await WaitCaughtUpAsync(cluster, c);
            AssertSameFile(current, (await StateAsync(c)).GetFile("/x"), "after a crash the replica still holds the newer state");
            await ServeFromAsync(cluster, c);
            await AssertBytesAsync(vol.Files, "/x", x2);
        });
}
