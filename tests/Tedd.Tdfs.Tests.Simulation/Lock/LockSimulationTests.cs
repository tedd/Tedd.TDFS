using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Simulation.Lock;

/// <summary>How a racer tries to become the writer of an inode (LOCK-003).</summary>
public enum WriterAcquisition
{
    OpenWrite,
    OpenReadWrite,
    OpenOrCreateWrite,
    AppendMode,
    TruncateMode,
    CreateMode,
    OpenWriteHelper,
    TransactionWriter,
}

/// <summary>How the volume leader is lost (LOCK-011).</summary>
public enum LeaderLoss
{
    Crash,
    Isolate,
    Pause,
}

/// <summary>Cold restart of every durable metadata replica (LOCK-012).</summary>
public enum ColdRestart
{
    CrashLoseUnflushed,
    CrashKeepUnflushed,
    CrashRandomSubset,
    CrashTearLastWrite,
    GracefulStop,
}

/// <summary>Client clock manipulation (LOCK-014).</summary>
public enum ClientClockChange
{
    WallBackward,
    WallForward,
    Frozen,
    SlowRate,
    MonotonicJumpForward,
}

/// <summary>Which server clocks change (LOCK-015).</summary>
public enum ServerClockTarget
{
    Leader,
    Follower,
    AllNodes,
}

/// <summary>Server clock manipulation (LOCK-015).</summary>
public enum ServerClockChange
{
    WallForward,
    WallBackward,
    MonotonicJumpForward,
    Freeze,
    FastRate,
    SlowRate,
}

/// <summary>When the server clock change happens relative to the writer's lease (LOCK-015).</summary>
public enum ClockPhase
{
    /// <summary>While the writer renews normally.</summary>
    DuringRenewal,

    /// <summary>Just before the lease would expire, with the writer's renewals withheld.</summary>
    AroundExpiry,
}

/// <summary>The publication request left queued in the network (LOCK-016).</summary>
public enum QueuedPublication
{
    Flush,
    WriteThroughWrite,
    WriteThroughSetLength,
}

/// <summary>A mutation path attempted by a stale writer or its client (LOCK-017).</summary>
public enum StalePath
{
    StreamFlush,
    StreamCommit,
    StreamWriteThrough,
    StreamSetLengthWriteThrough,
    StreamDispose,
    HelperWriteAllBytes,
    HelperAppendAllBytes,
    HelperCopyOverwrite,
    HelperMoveOverwrite,
    HelperDelete,
    TransactionCommit,
    TruncateOpen,
    CreateOpen,
    ReplayedCommitFileRpc,
    DirectObjectPut,
    Restore,
}

/// <summary>How a transaction holding several reservations ends (LOCK-018).</summary>
public enum TransactionEnd
{
    Abort,
    DisposeWithoutCommit,
    CommitConflict,
    TimeoutExpiry,
    ClientKilled,
    LeaderChange,
}

/// <summary>
/// LOCK — writer leases, sharing and fencing (FSD §7, LOCK-01..LOCK-06; test plan §7.9 and §9.3): deterministic
/// whole-cluster simulations. Paused writers are modelled as in test plan §9.3 by withholding every message of the
/// writer's client (network hold) and freezing its clock, then delivering the queued messages after the replacement
/// writer was granted.
/// </summary>
public sealed class LockSimulationTests
{
    private const string Vol = LockHelpers.Vol;

    // ------------------------------------------------------------------------------------------------------------
    // LOCK-003
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<int, string> WriterRaces() => new()
    {
        { 3, "OpenWrite,OpenWrite" },
        { 3, "OpenWrite,OpenReadWrite,AppendMode" },
        { 3, "TruncateMode,OpenOrCreateWrite,CreateMode" },
        { 5, "OpenWrite,OpenReadWrite,OpenOrCreateWrite,AppendMode,OpenWriteHelper" },
        { 5, "TruncateMode,CreateMode,OpenWrite,OpenWrite,AppendMode" },
        { 3, "TransactionWriter,OpenWrite" },
        { 5, "TransactionWriter,TransactionWriter,TruncateMode" },
    };

    /// <summary>
    /// LOCK-003: several clients, each routed through a different seed node, race to become the writer of one inode
    /// through different acquisition paths (writable opens in every mode, the OpenWrite helper, transaction writer
    /// reservations). When all racers are handle opens the leader holds them at request acceptance until every one has
    /// arrived, so they are processed together. Exactly one becomes the writer, all others get typed sharing conflicts,
    /// losing truncating opens truncate nothing, only the winner can publish, and losers later acquire with strictly
    /// newer generations.
    /// </summary>
    [Theory]
    [MemberData(nameof(WriterRaces))]
    [Catalog("LOCK-003", "S", "INV-02", "INV-05", "INV-06", "INV-12")]
    public async Task LOCK_003_OneWriterPerInode(int nodeCount, string racerList)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        WriterAcquisition[] racers = racerList.Split(',').Select(Enum.Parse<WriterAcquisition>).ToArray();
        await ClusterTest.RunAsync(LockHelpers.Sim(nodeCount), async cluster =>
        {
            const string path = "/lock003/f";
            byte[] initial = TestData.Bytes(3001, 9000);
            byte[] winnerData = TestData.Bytes(3002, 5000);
            await using DfsClient setupClient = await LockHelpers.ConnectAsync(cluster, "setup", ct);
            DfsVolume setup = setupClient.GetVolume(Vol);
            await setup.Directories.CreateDirectoryAsync("/lock003", ct);
            await setup.Files.WriteAllBytesAsync(path, initial, ct);
            InodeId inode = (await setup.Files.GetInfoAsync(path, ct)).InodeId;

            var clients = new List<DfsClient>();
            try
            {
                for (int i = 0; i < racers.Length; i++)
                    clients.Add(await LockHelpers.ConnectAsync(cluster, "r" + i, ct, seeds: [cluster[i % nodeCount].Name]));

                bool handlesOnly = racers.All(r => r != WriterAcquisition.TransactionWriter);
                TestNode leader = await cluster.GetLeaderAsync(Vol, ct);
                FaultArm? gate = handlesOnly
                    ? cluster.Faults.Arm(new FaultMatch(FaultSites.RpcRequestAccepted, Node: leader.Name, Operation: DfsOperationKind.Open, Count: racers.Length),
                        FaultAction.Block())
                    : null;

                Task<(WriterHolder? Holder, Exception? Error)>[] attempts =
                    racers.Select((kind, i) => WriterHolder.AcquireAsync(clients[i].GetVolume(Vol), kind, path, ct)).ToArray();
                if (gate is not null)
                {
                    await cluster.WaitUntilAsync(() => Task.FromResult(gate.Hits >= racers.Length), TimeSpan.FromSeconds(20),
                        "every racing open must be accepted by the leader before any of them is processed", ct);
                    gate.Release();
                    cluster.Faults.AssertReached(gate);
                }

                (WriterHolder? Holder, Exception? Error)[] results = await Task.WhenAll(attempts);
                int[] winners = Enumerable.Range(0, racers.Length).Where(i => results[i].Holder is not null).ToArray();
                Assert.True(winners.Length == 1,
                    $"exactly one of [{racerList}] may become the writer, {winners.Length} did: " +
                    string.Join("; ", results.Select((r, i) => $"{i}:{racers[i]} → {(r.Holder is null ? LockHelpers.Describe(r.Error) : "granted")}")));
                int w = winners[0];
                WriterHolder winner = results[w].Holder!;
                ulong winnerGeneration = winner.Generation;
                for (int i = 0; i < racers.Length; i++)
                {
                    if (i != w)
                        LockHelpers.AssertSharingConflict(results[i].Error, $"racer {i} ({racers[i]}) lost to racer {w} ({racers[w]})");
                }

                await LockHelpers.AssertAtMostOneActiveWriterOnEveryReplicaAsync(cluster, inode, "right after the race");
                HandleInspection[] writers = await LockHelpers.ActiveWritersAsync(cluster, inode);
                Assert.True(writers.Length <= 1, $"{writers.Length} active writer handles after the race");
                if (winner.Transaction is null)
                {
                    HandleInspection only = Assert.Single(writers);
                    Assert.Equal(winnerGeneration, only.Generation);
                    Assert.Equal(winnerGeneration, (await LockHelpers.FileAsync(cluster, path)).WriterGeneration);
                }

                // A truncating open truncates only if it won: losing Truncate/Create opens changed nothing.
                bool winnerTruncates = winner.Transaction is null && racers[w] is WriterAcquisition.TruncateMode or WriterAcquisition.CreateMode;
                DfsFileInfo afterRace = await setup.Files.GetInfoAsync(path, ct);
                Assert.Equal(inode, afterRace.InodeId);
                Assert.Equal(winnerTruncates ? 0 : initial.Length, afterRace.Length);
                byte[] prior = winnerTruncates ? [] : initial;

                await winner.PublishAsync(winnerData, ct);
                await winner.DisposeAsync();
                byte[] expected = racers[w] == WriterAcquisition.AppendMode ? [.. prior, .. winnerData] : winnerData;
                LockHelpers.BytesEqual(expected, await LockHelpers.ReadCommittedAsync(setup, path, ct), "only the single winning writer published");

                // Losers acquire one at a time once the winner closed, always with a strictly newer fencing generation.
                ulong lastGeneration = winner.Transaction is null ? winnerGeneration : 0;
                for (int i = 0; i < racers.Length; i++)
                {
                    if (i == w)
                        continue;
                    (WriterHolder? retry, Exception? error) = await WriterHolder.AcquireAsync(clients[i].GetVolume(Vol), racers[i], path, ct);
                    if (retry is null)
                    {
                        Assert.Fail($"racer {i} ({racers[i]}) must acquire once the previous writer closed: {LockHelpers.Describe(error)}");
                        return;
                    }

                    if (retry.Transaction is null)
                    {
                        Assert.True(retry.Generation > lastGeneration, $"generation {retry.Generation} must exceed every earlier one ({lastGeneration})");
                        lastGeneration = retry.Generation;
                    }

                    await retry.DisposeAsync();
                    await LockHelpers.AssertAtMostOneActiveWriterOnEveryReplicaAsync(cluster, inode, $"after racer {i} re-acquired");
                }
            }
            finally
            {
                foreach (DfsClient client in clients)
                    await client.DisposeAsync();
            }
        }, ct);
    }

    /// <summary>
    /// LOCK-003 (paths): writer exclusivity belongs to the inode, not to a path. After the writer's file is renamed, a
    /// writable open through the new path conflicts; a new file created at the old path is a different inode and can
    /// get its own writer.
    /// </summary>
    [Fact]
    [Catalog("LOCK-003", "S", "INV-02", "INV-05", "INV-06", "INV-12")]
    public async Task LOCK_003_OneWriterPerInode_IndependentOfPath()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(LockHelpers.Sim(), async cluster =>
        {
            await using DfsClient c0 = await LockHelpers.ConnectAsync(cluster, "c0", ct);
            await using DfsClient c1 = await LockHelpers.ConnectAsync(cluster, "c1", ct);
            DfsVolume v0 = c0.GetVolume(Vol);
            DfsVolume v1 = c1.GetVolume(Vol);
            byte[] original = TestData.Bytes(3101, 4000);
            byte[] published = TestData.Bytes(3102, 2500);
            await v0.Directories.CreateDirectoryAsync("/lock003p/d1", ct);
            await v0.Directories.CreateDirectoryAsync("/lock003p/d2", ct);
            await v0.Files.WriteAllBytesAsync("/lock003p/d1/f", original, ct);
            InodeId inode = (await v0.Files.GetInfoAsync("/lock003p/d1/f", ct)).InodeId;

            await using DfsStream writer = await v0.Files.OpenAsync("/lock003p/d1/f", LockHelpers.Writer(share: FileShare.Read | FileShare.Delete), ct);
            await v1.Files.MoveAsync("/lock003p/d1/f", "/lock003p/d2/f", false, ct);
            Assert.Equal(inode, (await v1.Files.GetInfoAsync("/lock003p/d2/f", ct)).InodeId);

            (DfsStream? viaNewPath, Exception? error) = await LockHelpers.TryOpenAsync(v1, "/lock003p/d2/f", LockHelpers.Writer(), ct);
            if (viaNewPath is not null)
            {
                await viaNewPath.DisposeAsync();
                Assert.Fail("a second writer was granted on the same inode through its new path");
            }

            LockHelpers.AssertSharingConflict(error, "writable open of the renamed inode");

            await v1.Files.WriteAllBytesAsync("/lock003p/d1/f", TestData.Bytes(3103, 100), ct);
            InodeId recreated = (await v1.Files.GetInfoAsync("/lock003p/d1/f", ct)).InodeId;
            Assert.NotEqual(inode, recreated);
            await using DfsStream otherWriter = await v1.Files.OpenAsync("/lock003p/d1/f", LockHelpers.Writer(), ct);
            Assert.Single(await LockHelpers.ActiveWritersAsync(cluster, inode));
            Assert.Single(await LockHelpers.ActiveWritersAsync(cluster, recreated));

            await LockHelpers.ReplaceAndCommitAsync(writer, published, ct);
            LockHelpers.BytesEqual(published, await LockHelpers.ReadCommittedAsync(v1, "/lock003p/d2/f", ct), "the original writer publishes to its inode at the new path");
        }, ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // LOCK-008
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<int, bool> LostRenewalReplies() => new()
    {
        { 1, false },
        { 2, false },
        { 1, true },
        { 2, true },
    };

    /// <summary>
    /// LOCK-008: renewals that commit but whose replies are lost (1 or 2 in a row, optionally followed by a leader
    /// crash) are retried without creating anything new: the same handle id and generation stay active beyond the
    /// original lease term, no extra session or writer appears, a second writer is still refused and the original
    /// writer's publication is accepted with its original generation.
    /// </summary>
    [Theory]
    [MemberData(nameof(LostRenewalReplies))]
    [Catalog("LOCK-008", "S", "INV-02", "INV-05", "INV-06", "INV-12")]
    public async Task LOCK_008_LeaseRenewalReplyLost(int droppedReplies, bool leaderCrashAfterDrop)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(LockHelpers.Sim(), async cluster =>
        {
            TimeSpan lease = LockHelpers.Lease(cluster);
            const string path = "/lock008/f";
            byte[] initial = TestData.Bytes(8001, 3000);
            byte[] dataA = TestData.Bytes(8002, 6000);
            await using DfsClient setupClient = await LockHelpers.ConnectAsync(cluster, "setup", ct);
            DfsVolume setup = setupClient.GetVolume(Vol);
            await setup.Directories.CreateDirectoryAsync("/lock008", ct);
            await setup.Files.WriteAllBytesAsync(path, initial, ct);
            InodeId inode = (await setup.Files.GetInfoAsync(path, ct)).InodeId;

            await using DfsClient clientA = await LockHelpers.ConnectAsync(cluster, "a", ct);
            await using DfsStream a = await clientA.GetVolume(Vol).Files.OpenAsync(path, LockHelpers.Writer(), ct);
            ulong gA = a.WriterGeneration;
            await LockHelpers.StageReplaceAsync(a, dataA, ct);
            VolumeStateInspection before = await cluster.Inspect.GetVolumeStateAsync(Vol);
            HandleInspection handleA = Assert.Single(before.Handles, h => h.Inode == inode && h.IsWriter && !h.Expired);
            Assert.Equal(gA, handleA.Generation);
            ulong newestSessionBefore = before.Sessions.Max(s => s.Id);

            MessageRule lostReplies = cluster.Network.DropReplies(new MessageFilter(From: LockHelpers.Net("a"), Type: MessageType.KeepAlive), droppedReplies);
            await cluster.WaitUntilAsync(() => Task.FromResult(lostReplies.Matched >= droppedReplies), lease * 3,
                "A's renewals must be delivered and executed while their replies are lost", ct);

            string? crashed = null;
            if (leaderCrashAfterDrop)
            {
                TestNode leader = await cluster.GetLeaderAsync(Vol, ct);
                await leader.CrashAsync();
                crashed = leader.Name;
                await LockHelpers.WaitForNewLeaderAsync(cluster, leader.Name, lease * 5, ct);
            }

            // Run well past the original lease term: the committed renewals (and their retries) keep the SAME reservation.
            await cluster.DelayAsync(lease * 2, ct);
            VolumeStateInspection after = await cluster.Inspect.GetVolumeStateAsync(Vol);
            HandleInspection[] writers = after.Handles.Where(h => h.Inode == inode && h.IsWriter && !h.Expired).ToArray();
            HandleInspection still = Assert.Single(writers);
            Assert.Equal(handleA.Id, still.Id);
            Assert.Equal(gA, still.Generation);
            Assert.Equal(handleA.Session, still.Session);
            Assert.DoesNotContain(after.Sessions, s => s.Id > newestSessionBefore); // retries opened no new session
            Assert.Contains(after.Sessions, s => s.Id == handleA.Session && !s.Expired);
            Assert.Equal(1, after.Handles.Count(h => h.Session == handleA.Session && !h.Expired));
            Assert.False(a.IsFaulted, $"a lost renewal reply must not fault the writer: {LockHelpers.Describe(a.Fault)}");
            Assert.Equal(gA, a.WriterGeneration);

            await using DfsClient clientB = await LockHelpers.ConnectAsync(cluster, "b", ct);
            (DfsStream? b, Exception? error) = await LockHelpers.TryOpenAsync(clientB.GetVolume(Vol), path, LockHelpers.Writer(), ct);
            if (b is not null)
            {
                await b.DisposeAsync();
                Assert.Fail("retried renewals created room for an extra writer");
            }

            LockHelpers.AssertSharingConflict(error, "second writer while A's renewed reservation is valid");

            CommitToken token = await a.CommitAsync(ct);
            FileInspection file = await LockHelpers.FileAsync(cluster, path);
            Assert.Equal(token.Sequence, file.Revision);
            Assert.Equal(gA, file.WriterGeneration);
            LockHelpers.BytesEqual(dataA, await LockHelpers.ReadCommittedAsync(setup, path, ct), "the writer whose renewal replies were lost still publishes");
            await LockHelpers.AssertAtMostOneActiveWriterOnEveryReplicaAsync(cluster, inode, "after lost renewal replies");

            if (crashed is not null)
            {
                await cluster.Node(crashed).RestartAsync();
                await cluster.WaitForReplicationAsync(Vol, ct);
                await cluster.Inspect.AssertReplicasConvergedAsync(Vol);
                await LockHelpers.AssertWriterGenerationOnEveryReplicaAsync(cluster, path, gA, "after the crashed leader rejoined");
            }
        }, ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // LOCK-009
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<bool, bool, bool> ReplacementSchedules()
    {
        var data = new TheoryData<bool, bool, bool>();
        foreach (bool committed in new[] { false, true })
        {
            foreach (bool leaderChange in new[] { false, true })
            {
                foreach (bool reverse in new[] { false, true })
                    data.Add(committed, leaderChange, reverse);
            }
        }

        return data;
    }

    /// <summary>
    /// LOCK-009 (test plan §9.3): writer A (generation g) stages data beyond its buffer, is paused (messages withheld,
    /// clock frozen) with a renewal and a flush queued, and is safely expired; writer B gets a newer generation. Variants:
    /// B has / has not committed before A resumes (so A's expected base revision may still be current), a metadata
    /// leader change between expiry and A's return, forward/reverse delivery of A's queue. Every publication path of
    /// A's old generation is rejected with LeaseLost and faults its stream; B stays the only valid writer; A's orphan
    /// uploads are never published and are reclaimed once safely abandoned.
    /// </summary>
    /// <remarks>
    /// Whether a writable open (B's grant) itself changes the inode's revision is not specified (the generation lives in
    /// the inode record), so the uncommitted variant pins the revision observed right after B's grant: nothing A does
    /// may move it.
    /// </remarks>
    [Theory]
    [MemberData(nameof(ReplacementSchedules))]
    [Catalog("LOCK-009", "S", "INV-02", "INV-05", "INV-06", "INV-12")]
    public async Task LOCK_009_OldWriterResumesAfterReplacement(bool replacementCommitted, bool leaderChangeBeforeResume, bool reverseDelivery)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(LockHelpers.Sim(), async cluster =>
        {
            TimeSpan lease = LockHelpers.Lease(cluster);
            const string path = "/lock009/f";
            byte[] initial = TestData.Bytes(9001, 5000);
            byte[] staleData = TestData.Bytes(9002, 200 * 1024); // beyond the 64 KiB buffer: chunks are uploaded unpublished
            byte[] dataB = TestData.Bytes(9003, 7000);
            byte[] dataB2 = TestData.Bytes(9004, 3000);

            await using DfsClient setupClient = await LockHelpers.ConnectAsync(cluster, "setup", ct);
            DfsVolume setup = setupClient.GetVolume(Vol);
            await setup.Directories.CreateDirectoryAsync("/lock009", ct);
            await setup.Files.WriteAllBytesAsync(path, initial, ct);
            DfsFileInfo baseInfo = await setup.Files.GetInfoAsync(path, ct);
            HashSet<StagingId> stagingBefore = (await cluster.Inspect.GetVolumeStateAsync(Vol)).Staging.Select(s => s.Id).ToHashSet();

            var clockA = new TestClock(cluster.Time);
            DfsClient clientA = await LockHelpers.ConnectAsync(cluster, "a", ct, clock: clockA);
            DfsStream a = await clientA.GetVolume(Vol).Files.OpenAsync(path, LockHelpers.Writer(FileAccess.ReadWrite), ct);
            ulong gA = a.WriterGeneration;
            Assert.True(gA > 0);
            await a.WriteAsync(staleData, ct);

            HashSet<StagingId> stagingA = [];
            await cluster.WaitUntilAsync(async () =>
            {
                VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Vol);
                stagingA = state.Staging.Where(s => s.Open && !stagingBefore.Contains(s.Id)).Select(s => s.Id).ToHashSet();
                return stagingA.Count > 0 && (await LockHelpers.StoredObjectsOfStagingAsync(cluster, stagingA)).Count > 0;
            }, lease * 5, "A's writes beyond its buffer must be uploaded (unpublished) under A's own staging session", ct);
            HashSet<ObjectId> uploadsA = await LockHelpers.StoredObjectsOfStagingAsync(cluster, stagingA);

            // Pause A with a renewal and its flush queued in the network.
            MessageHold queue = cluster.Network.Hold(new MessageFilter(From: LockHelpers.Net("a")));
            await cluster.WaitUntilAsync(() => Task.FromResult(queue.Captured.Any(m => m.Type == MessageType.KeepAlive)), lease * 2,
                "one of A's renewals must be queued behind the pause", ct);
            clockA.Freeze();
            Task<Exception?> staleFlush = LockHelpers.CatchAsync(() => a.FlushAsync(ct));
            await cluster.DelayAsync(lease / 4, ct); // A's flush issues whatever it can; everything stays queued
            Assert.False(staleFlush.IsCompleted, "A's flush cannot complete while every message of A is withheld");

            await LockHelpers.WaitForWriterGoneAsync(cluster, baseInfo.InodeId, gA, lease * 4,
                "the server must expire A's lease on its own clock while A is paused", ct);

            await using DfsClient clientB = await LockHelpers.ConnectAsync(cluster, "b", ct);
            DfsVolume vb = clientB.GetVolume(Vol);
            await using DfsStream b = await vb.Files.OpenAsync(path, LockHelpers.Writer(), ct);
            ulong gB = b.WriterGeneration;
            Assert.True(gB > gA, $"replacement generation {gB} must exceed the expired generation {gA}");

            byte[] expected = initial;
            ulong expectedRevision = (await LockHelpers.FileAsync(cluster, path)).Revision;
            if (replacementCommitted)
            {
                CommitToken tB = await LockHelpers.ReplaceAndCommitAsync(b, dataB, ct);
                expected = dataB;
                expectedRevision = tB.Sequence;
            }

            string? crashedLeader = null;
            if (leaderChangeBeforeResume)
            {
                TestNode oldLeader = await cluster.GetLeaderAsync(Vol, ct);
                await oldLeader.CrashAsync();
                crashedLeader = oldLeader.Name;
                await LockHelpers.WaitForNewLeaderAsync(cluster, oldLeader.Name, lease * 5, ct);
            }

            async Task AssertBAuthoritativeAsync(string when)
            {
                await LockHelpers.AssertAtMostOneActiveWriterOnEveryReplicaAsync(cluster, baseInfo.InodeId, when);
                HandleInspection writer = Assert.Single(await LockHelpers.ActiveWritersAsync(cluster, baseInfo.InodeId));
                Assert.Equal(gB, writer.Generation);
                FileInspection file = await LockHelpers.FileAsync(cluster, path);
                Assert.Equal(baseInfo.InodeId, file.Inode);
                Assert.Equal(gB, file.WriterGeneration);
                Assert.Equal(expectedRevision, file.Revision);
                Assert.DoesNotContain(file.Extents, e => uploadsA.Contains(e.Object));
                LockHelpers.BytesEqual(expected, await LockHelpers.ReadCommittedAsync(vb, path, ct), $"{when}: B's committed contents (or the original) only");
            }

            await AssertBAuthoritativeAsync("before A resumes");

            queue.ReleaseAll(reverseDelivery);
            clockA.Unfreeze();
            // With a crashed leader, A's queue partly targets a dead node: allow A's SDK its own timeouts and rerouting.
            Exception? flushError = await LockHelpers.WithinAsync(cluster, staleFlush, lease * (leaderChangeBeforeResume ? 15 : 10),
                "A's queued flush must be answered once delivered", ct);
            uploadsA.UnionWith(await LockHelpers.StoredObjectsOfStagingAsync(cluster, stagingA)); // uploads delivered late
            LockHelpers.AssertLeaseLost(flushError, $"A's flush under obsolete generation {gA}");
            Assert.True(a.IsFaulted, "lease loss must fault A's stream");
            Assert.True(a.Fault is DfsLeaseLostException, $"A's stream fault must be the lease loss, got {LockHelpers.Describe(a.Fault)}");
            LockHelpers.AssertLeaseLost(await LockHelpers.CatchAsync(() => a.WriteAsync(new byte[] { 1, 2, 3 }, ct).AsTask()), "write on the faulted stream");
            LockHelpers.AssertLeaseLost(await LockHelpers.CatchAsync(() => a.CommitAsync(ct)), "commit on the faulted stream");
            LockHelpers.AssertLeaseLost(await LockHelpers.CatchAsync(() => a.DisposeAsync().AsTask()), "dispose must surface that A's accepted writes were never published");
            Assert.Equal(gA, a.WriterGeneration);

            await AssertBAuthoritativeAsync("after A's queued renewal, uploads, flush and disposal were delivered");
            foreach (TestNode node in LockHelpers.Running(cluster))
            {
                VolumeStateInspection? state = await node.Inspect.GetVolumeStateAsync(Vol);
                Assert.True(state is null || !state.Handles.Any(h => h.Generation == gA && !h.Expired), $"replica {node.Name} shows generation {gA} active again");
            }

            CommitToken tB2 = await LockHelpers.ReplaceAndCommitAsync(b, dataB2, ct);
            expected = dataB2;
            expectedRevision = tB2.Sequence;
            await AssertBAuthoritativeAsync("after B publishes again");

            if (crashedLeader is not null)
            {
                await cluster.Node(crashedLeader).RestartAsync();
                await cluster.WaitForReplicationAsync(Vol, ct);
                await cluster.Inspect.AssertReplicasConvergedAsync(Vol);
                await LockHelpers.AssertWriterGenerationOnEveryReplicaAsync(cluster, path, gB, "after the old leader rejoined");
            }

            await cluster.Inspect.AssertNoSplitBrainAsync();

            // A's orphan uploads are reclaimed only once its staging is safely closed (expired handle).
            await cluster.WaitUntilAsync(async () =>
            {
                VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Vol);
                bool closed = state.Staging.Where(s => stagingA.Contains(s.Id)).All(s => !s.Open);
                return closed && (await LockHelpers.StoredObjectsOfStagingAsync(cluster, stagingA)).Count == 0;
            }, TimeSpan.FromSeconds(60), "A's abandoned staging must be closed and its unpublished uploads garbage-collected", ct);
            await AssertBAuthoritativeAsync("after orphan reclamation");
            await clientA.DisposeAsync();
        }, ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // LOCK-010
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<bool, bool, bool> DelayedRenewalSchedules()
    {
        var data = new TheoryData<bool, bool, bool>();
        foreach (bool reverse in new[] { false, true })
        {
            foreach (bool leaderChange in new[] { false, true })
            {
                foreach (bool sessionExpired in new[] { false, true })
                    data.Add(reverse, leaderChange, sessionExpired);
            }
        }

        return data;
    }

    /// <summary>
    /// LOCK-010: A's renewals are withheld in the network until its lease (and optionally its session) expired and B
    /// was granted a newer generation; then the old renewals are delivered (forward/reverse, optionally after a leader
    /// change) and replayed once more — to their original destination and to the current leader, so that after a
    /// leader crash they still reach the authority instead of vanishing with the dead node. B remains the only
    /// authoritative writer on every replica, A's handle and session are not revived, a third writer is still refused
    /// and A's publication is rejected.
    /// </summary>
    [Theory]
    [MemberData(nameof(DelayedRenewalSchedules))]
    [Catalog("LOCK-010", "S", "INV-02", "INV-05", "INV-06", "INV-12")]
    public async Task LOCK_010_DelayedRenewalCannotReviveExpiredLease(bool reverseDelivery, bool leaderChangeBeforeDelivery, bool waitForSessionExpiry)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(LockHelpers.Sim(), async cluster =>
        {
            TimeSpan lease = LockHelpers.Lease(cluster);
            const string path = "/lock010/f";
            byte[] initial = TestData.Bytes(10_001, 3000);
            byte[] dataA = TestData.Bytes(10_002, 2000);
            byte[] dataB = TestData.Bytes(10_003, 4000);
            await using DfsClient setupClient = await LockHelpers.ConnectAsync(cluster, "setup", ct);
            DfsVolume setup = setupClient.GetVolume(Vol);
            await setup.Directories.CreateDirectoryAsync("/lock010", ct);
            await setup.Files.WriteAllBytesAsync(path, initial, ct);
            InodeId inode = (await setup.Files.GetInfoAsync(path, ct)).InodeId;

            await using DfsClient clientA = await LockHelpers.ConnectAsync(cluster, "a", ct);
            DfsStream a = await clientA.GetVolume(Vol).Files.OpenAsync(path, LockHelpers.Writer(), ct);
            ulong gA = a.WriterGeneration;
            await LockHelpers.StageReplaceAsync(a, dataA, ct);
            ulong sessionA = Assert.Single(await LockHelpers.ActiveWritersAsync(cluster, inode)).Session;

            MessageHold renewals = cluster.Network.Hold(new MessageFilter(From: LockHelpers.Net("a"), Type: MessageType.KeepAlive));
            await renewals.WaitForAsync(2, lease * 3);
            await LockHelpers.WaitForWriterGoneAsync(cluster, inode, gA, lease * 4, "A's lease must expire while its renewals are withheld", ct);
            if (waitForSessionExpiry)
            {
                await cluster.WaitUntilAsync(async () =>
                {
                    VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Vol);
                    return state.Sessions.All(s => s.Id != sessionA || s.Expired);
                }, LockHelpers.SessionTimeout(cluster) * 3, "A's session must expire while its keep-alives are withheld", ct);
            }

            await using DfsClient clientB = await LockHelpers.ConnectAsync(cluster, "b", ct);
            DfsVolume vb = clientB.GetVolume(Vol);
            await using DfsStream b = await vb.Files.OpenAsync(path, LockHelpers.Writer(), ct);
            ulong gB = b.WriterGeneration;
            Assert.True(gB > gA);

            string? crashed = null;
            if (leaderChangeBeforeDelivery)
            {
                TestNode leader = await cluster.GetLeaderAsync(Vol, ct);
                await leader.CrashAsync();
                crashed = leader.Name;
                await LockHelpers.WaitForNewLeaderAsync(cluster, leader.Name, lease * 5, ct);
            }

            CapturedMessage[] oldRenewals = renewals.Captured.ToArray();
            Assert.NotEmpty(oldRenewals);
            TestNode currentLeader = await cluster.GetLeaderAsync(Vol, ct);
            renewals.ReleaseAll(reverseDelivery);
            foreach (CapturedMessage renewal in reverseDelivery ? Enumerable.Reverse(oldRenewals) : oldRenewals)
            {
                // Delivered once more (the reply code is not judged: a KeepAlive may succeed at session level while its
                // handle entries are refused); the replicated state is checked below.
                await LockHelpers.CatchAsync(() => cluster.Network.ReplayAsync(renewal));
                if (!string.Equals(renewal.To, currentLeader.Name, StringComparison.Ordinal))
                    await LockHelpers.CatchAsync(() => cluster.Network.ReplayAsync(renewal with { To = currentLeader.Name }));
            }

            await cluster.DelayAsync(lease, ct);

            await LockHelpers.AssertAtMostOneActiveWriterOnEveryReplicaAsync(cluster, inode, "after delivering old renewals");
            HandleInspection writer = Assert.Single(await LockHelpers.ActiveWritersAsync(cluster, inode));
            Assert.Equal(gB, writer.Generation);
            foreach (TestNode node in LockHelpers.Running(cluster))
            {
                VolumeStateInspection? state = await node.Inspect.GetVolumeStateAsync(Vol);
                Assert.True(state is null || !state.Handles.Any(h => h.Generation == gA && !h.Expired), $"replica {node.Name}: an old renewal revived generation {gA}");
                if (waitForSessionExpiry && state is not null)
                    Assert.True(state.Sessions.All(s => s.Id != sessionA || s.Expired), $"replica {node.Name}: an old keep-alive revived A's expired session");
            }

            await LockHelpers.AssertWriterGenerationOnEveryReplicaAsync(cluster, path, gB, "old renewals must not restore the old generation");

            await using DfsClient clientC = await LockHelpers.ConnectAsync(cluster, "c", ct);
            (DfsStream? third, Exception? thirdError) = await LockHelpers.TryOpenAsync(clientC.GetVolume(Vol), path, LockHelpers.Writer(), ct);
            if (third is not null)
            {
                await third.DisposeAsync();
                Assert.Fail("an old renewal released or confused B's reservation");
            }

            LockHelpers.AssertSharingConflict(thirdError, "third writer while B is authoritative");

            Exception? staleCommit = await LockHelpers.CatchAsync(() => a.CommitAsync(ct));
            if (waitForSessionExpiry && staleCommit is DfsOperationExpiredException)
            {
                // A's whole session expired: its operation ids are refused as expired, which is equally definite.
                Assert.Equal(DfsErrorCode.OperationExpired, ((DfsException)staleCommit).ErrorCode);
            }
            else
            {
                LockHelpers.AssertLeaseLost(staleCommit, "A's publication after its old renewal was delivered");
            }

            CommitToken tB = await LockHelpers.ReplaceAndCommitAsync(b, dataB, ct);
            Assert.Equal(tB.Sequence, (await LockHelpers.FileAsync(cluster, path)).Revision);
            LockHelpers.BytesEqual(dataB, await LockHelpers.ReadCommittedAsync(vb, path, ct), "B remains the authoritative writer");

            if (crashed is not null)
            {
                await cluster.Node(crashed).RestartAsync();
                await cluster.WaitForReplicationAsync(Vol, ct);
                await cluster.Inspect.AssertReplicasConvergedAsync(Vol);
                await LockHelpers.AssertWriterGenerationOnEveryReplicaAsync(cluster, path, gB, "after the crashed leader rejoined");
            }

            await LockHelpers.CatchAsync(() => a.DisposeAsync().AsTask());
        }, ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // LOCK-011
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<LeaderLoss, int> LeaderLosses()
    {
        var data = new TheoryData<LeaderLoss, int>();
        foreach (LeaderLoss loss in Enum.GetValues<LeaderLoss>())
        {
            data.Add(loss, 3);
            data.Add(loss, 5);
        }

        return data;
    }

    /// <summary>
    /// LOCK-011: the volume leader is lost (crash, isolation or pause) while writer A holds a live lease on f1 and writer
    /// A2 — paused with its lease in flight — holds f2. The new leader recovers the persisted generations (no reset),
    /// grants no duplicate writer while A's lease may still be valid (it must treat every lease as renewed at its
    /// election, and B asks right after the election), never accepts a publication under a superseded generation,
    /// expires A2 on its own clock, grants B2 a strictly newer generation and rejects A2's stale publication; after the
    /// old leader returns every replica agrees on the newest generations.
    /// </summary>
    /// <remarks>
    /// Whether A itself keeps its authority depends on its SDK reaching the new leader within one lease term, which is
    /// availability, not safety: A's publication may commit under gA, fail for availability (resolved authoritatively),
    /// or fail with LeaseLost when its lease lapsed — in every case f1 gets no other writer generation.
    /// </remarks>
    [Theory]
    [MemberData(nameof(LeaderLosses))]
    [Catalog("LOCK-011", "S", "INV-02", "INV-05", "INV-06", "INV-12")]
    public async Task LOCK_011_LeaderChangePreservesFencing(LeaderLoss loss, int nodeCount)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(LockHelpers.Sim(nodeCount), async cluster =>
        {
            TimeSpan lease = LockHelpers.Lease(cluster);
            const string f1 = "/lock011/f1";
            const string f2 = "/lock011/f2";
            byte[] initial1 = TestData.Bytes(11_001, 3000);
            byte[] initial2 = TestData.Bytes(11_002, 3000);
            byte[] dataA = TestData.Bytes(11_003, 5000);
            byte[] dataA2 = TestData.Bytes(11_004, 5000);
            byte[] dataB2 = TestData.Bytes(11_005, 2500);

            await using DfsClient setupClient = await LockHelpers.ConnectAsync(cluster, "setup", ct);
            DfsVolume setup = setupClient.GetVolume(Vol);
            await setup.Directories.CreateDirectoryAsync("/lock011", ct);
            await setup.Files.WriteAllBytesAsync(f1, initial1, ct);
            await setup.Files.WriteAllBytesAsync(f2, initial2, ct);
            InodeId inode1 = (await setup.Files.GetInfoAsync(f1, ct)).InodeId;
            InodeId inode2 = (await setup.Files.GetInfoAsync(f2, ct)).InodeId;

            await using DfsClient clientA = await LockHelpers.ConnectAsync(cluster, "a", ct);
            DfsVolume va = clientA.GetVolume(Vol);
            DfsStream a = await va.Files.OpenAsync(f1, LockHelpers.Writer(), ct);
            ulong gA = a.WriterGeneration;
            await LockHelpers.StageReplaceAsync(a, dataA, ct);

            var clockA2 = new TestClock(cluster.Time);
            DfsClient clientA2 = await LockHelpers.ConnectAsync(cluster, "a2", ct, clock: clockA2);
            DfsStream a2 = await clientA2.GetVolume(Vol).Files.OpenAsync(f2, LockHelpers.Writer(), ct);
            ulong gA2 = a2.WriterGeneration;
            await LockHelpers.StageReplaceAsync(a2, dataA2, ct);
            await cluster.WaitForReplicationAsync(Vol, ct);
            PausedClient pausedA2 = PausedClient.Pause(cluster, "a2", clockA2); // lease in flight during the leader change

            TestNode oldLeader = await cluster.GetLeaderAsync(Vol, ct);
            switch (loss)
            {
                case LeaderLoss.Crash:
                    await oldLeader.CrashAsync();
                    break;
                case LeaderLoss.Isolate:
                    cluster.Network.Isolate(oldLeader.Name);
                    break;
                case LeaderLoss.Pause:
                    await oldLeader.PauseAsync();
                    break;
            }

            TestNode newLeader = await LockHelpers.WaitForNewLeaderAsync(cluster, oldLeader.Name, lease * 5, ct);
            Assert.Equal(gA, (await LockHelpers.FileAsync(cluster, f1, newLeader.Name)).WriterGeneration);
            Assert.Equal(gA2, (await LockHelpers.FileAsync(cluster, f2, newLeader.Name)).WriterGeneration);

            // No duplicate grant: the new leader must assume A's lease may still be valid.
            string[] survivors = LockHelpers.Running(cluster).Where(n => n.Name != oldLeader.Name).Select(n => n.Name).ToArray();
            await using DfsClient clientB = await LockHelpers.ConnectAsync(cluster, "b", ct, seeds: survivors);
            DfsVolume vb = clientB.GetVolume(Vol);
            (DfsStream? dup, Exception? dupError) = await LockHelpers.TryOpenAsync(vb, f1, LockHelpers.Writer(), ct);
            if (dup is not null)
            {
                await dup.DisposeAsync();
                Assert.Fail("the new leader granted a second writer while A's lease could still be valid");
            }

            LockHelpers.AssertSharingConflict(dupError, "writer open right after the leader change");

            // A's publication across the change: committed under gA, an availability failure resolved authoritatively,
            // or LeaseLost if its lease lapsed — never a commit under any other generation.
            Exception? aCommit = await LockHelpers.CatchAsync(() => a.CommitAsync(ct));
            if (aCommit is null)
            {
                LockHelpers.BytesEqual(dataA, await LockHelpers.ReadCommittedAsync(vb, f1, ct), "A's publication after the leader change");
            }
            else if (aCommit is DfsLeaseLostException)
            {
                LockHelpers.AssertLeaseLost(aCommit, "A's publication after its lease lapsed during the leader change");
                Assert.True(a.IsFaulted, "lease loss must fault A's stream");
                LockHelpers.BytesEqual(initial1, await LockHelpers.ReadCommittedAsync(vb, f1, ct), "a rejected publication of A changed nothing");
            }
            else
            {
                Assert.True(LockHelpers.IsAvailabilityOrUnknown(aCommit), $"A's commit may only fail for availability or lease loss, got {LockHelpers.Describe(aCommit)}");
                DfsOperationId? op = LockHelpers.OperationIdOf(aCommit);
                if (op is not null)
                {
                    DfsOperationOutcome outcome = await vb.QueryOperationAsync(op.Value, resolve: true, ct);
                    Assert.True(outcome.Status is DfsOutcomeStatus.Committed or DfsOutcomeStatus.Aborted,
                        $"resolve:true must make A's commit definite, got {outcome.Status}");
                    LockHelpers.BytesEqual(outcome.Status == DfsOutcomeStatus.Committed ? dataA : initial1,
                        await LockHelpers.ReadCommittedAsync(vb, f1, ct), "resolved outcome of A's commit");
                }
            }

            // No other writer was granted on f1 by the new leader, whatever happened to A.
            Assert.Equal(gA, (await LockHelpers.FileAsync(cluster, f1, newLeader.Name)).WriterGeneration);
            Assert.DoesNotContain(await LockHelpers.ActiveWritersAsync(cluster, inode1, newLeader.Name), h => h.Generation != gA);

            // A2 (paused) expires on the new leader's clock; B2 gets a strictly newer generation; A2's publication is rejected.
            await LockHelpers.WaitForWriterGoneAsync(cluster, inode2, gA2, lease * 4, "A2's lease must expire under the new leader", ct, newLeader.Name);
            await using DfsStream b2 = await vb.Files.OpenAsync(f2, LockHelpers.Writer(), ct);
            ulong gB2 = b2.WriterGeneration;
            Assert.True(gB2 > gA2, $"generation {gB2} granted by the new leader must exceed the persisted {gA2}");
            CommitToken tB2 = await LockHelpers.ReplaceAndCommitAsync(b2, dataB2, ct);

            // The old leader returns (as a follower) before A2 resumes, so A2's queued requests may reach it too.
            switch (loss)
            {
                case LeaderLoss.Crash:
                    await oldLeader.RestartAsync();
                    break;
                case LeaderLoss.Isolate:
                    cluster.Network.Heal();
                    break;
                case LeaderLoss.Pause:
                    await oldLeader.ResumeAsync();
                    break;
            }

            await cluster.WaitForReplicationAsync(Vol, ct);
            pausedA2.Resume();
            LockHelpers.AssertLeaseLost(await LockHelpers.WithinAsync(cluster, LockHelpers.CatchAsync(() => a2.CommitAsync(ct)), lease * 10, "A2's publication", ct),
                "A2's publication under its pre-election generation");
            await cluster.Inspect.AssertNoSplitBrainAsync();
            await cluster.Inspect.AssertReplicasConvergedAsync(Vol);
            await LockHelpers.AssertWriterGenerationOnEveryReplicaAsync(cluster, f2, gB2, "after the old leader returned");
            await LockHelpers.AssertAtMostOneActiveWriterOnEveryReplicaAsync(cluster, inode1, "f1 after the old leader returned");
            await LockHelpers.AssertAtMostOneActiveWriterOnEveryReplicaAsync(cluster, inode2, "f2 after the old leader returned");
            Assert.Equal(tB2.Sequence, (await LockHelpers.FileAsync(cluster, f2)).Revision);
            LockHelpers.BytesEqual(dataB2, await LockHelpers.ReadCommittedAsync(vb, f2, ct), "B2 is authoritative on f2; the returned old leader published nothing");

            await LockHelpers.CatchAsync(() => a.DisposeAsync().AsTask()); // may legitimately surface a lease loss
            await LockHelpers.CatchAsync(() => a2.DisposeAsync().AsTask());
            await clientA2.DisposeAsync();
        }, ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // LOCK-012
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<int, ColdRestart> ColdRestarts()
    {
        var data = new TheoryData<int, ColdRestart>();
        foreach (ColdRestart restart in Enum.GetValues<ColdRestart>())
            data.Add(3, restart);
        data.Add(5, ColdRestart.CrashLoseUnflushed);
        data.Add(5, ColdRestart.CrashRandomSubset);
        return data;
    }

    /// <summary>
    /// LOCK-012: after several writers accepted publications under increasing generations, every durable metadata
    /// replica is cold-restarted (crash in each crash mode, or graceful stop). Every replica recovers the last accepted
    /// generation of each inode from durable state, new writers receive generations strictly greater than every
    /// generation ever accepted, and a writer paused across the restart cannot publish.
    /// </summary>
    [Theory]
    [MemberData(nameof(ColdRestarts))]
    [Catalog("LOCK-012", "S", "INV-02", "INV-05", "INV-06", "INV-12")]
    public async Task LOCK_012_FullRestartPreservesGeneration(int nodeCount, ColdRestart restart)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(LockHelpers.Sim(nodeCount), async cluster =>
        {
            TimeSpan lease = LockHelpers.Lease(cluster);
            const string f1 = "/lock012/f1";
            const string f2 = "/lock012/f2";
            await using DfsClient setupClient = await LockHelpers.ConnectAsync(cluster, "setup", ct);
            DfsVolume setup = setupClient.GetVolume(Vol);
            await setup.Directories.CreateDirectoryAsync("/lock012", ct);
            await setup.Files.WriteAllBytesAsync(f1, TestData.Bytes(12_001, 2000), ct);
            await setup.Files.WriteAllBytesAsync(f2, TestData.Bytes(12_002, 2000), ct);
            InodeId inode1 = (await setup.Files.GetInfoAsync(f1, ct)).InodeId;

            var accepted = new List<ulong>();
            await using (DfsClient writerClient = await LockHelpers.ConnectAsync(cluster, "w", ct))
            {
                DfsVolume wv = writerClient.GetVolume(Vol);
                for (int k = 0; k < 3; k++)
                {
                    await using DfsStream w = await wv.Files.OpenAsync(f1, LockHelpers.Writer(), ct);
                    Assert.True(accepted.Count == 0 || w.WriterGeneration > accepted[^1], "generations increase monotonically");
                    accepted.Add(w.WriterGeneration);
                    await LockHelpers.ReplaceAndCommitAsync(w, TestData.Bytes(12_100 + (ulong)k, 3000), ct);
                }
            }

            ulong h;
            await using (DfsStream w2 = await setup.Files.OpenAsync(f2, LockHelpers.Writer(), ct))
            {
                h = w2.WriterGeneration;
                await LockHelpers.ReplaceAndCommitAsync(w2, TestData.Bytes(12_200, 1000), ct);
            }

            // Stale writer A: one accepted publication, then staged data, then paused across the restart.
            var clockA = new TestClock(cluster.Time);
            DfsClient clientA = await LockHelpers.ConnectAsync(cluster, "a", ct, clock: clockA);
            DfsStream a = await clientA.GetVolume(Vol).Files.OpenAsync(f1, LockHelpers.Writer(), ct);
            ulong gA = a.WriterGeneration;
            Assert.True(gA > accepted[^1]);
            accepted.Add(gA);
            CommitToken tA1 = await LockHelpers.ReplaceAndCommitAsync(a, TestData.Bytes(12_300, 4000), ct);
            await LockHelpers.StageReplaceAsync(a, TestData.Bytes(12_301, 4000), ct);
            PausedClient pausedA = PausedClient.Pause(cluster, "a", clockA);

            await cluster.WaitForReplicationAsync(Vol, ct);
            await LockHelpers.AssertWriterGenerationOnEveryReplicaAsync(cluster, f1, gA, "before the restart");

            TestNode[] allNodes = cluster.Nodes.ToArray();
            foreach (TestNode node in allNodes)
            {
                if (restart == ColdRestart.GracefulStop)
                {
                    await node.StopAsync();
                    continue;
                }

                await node.CrashAsync(restart switch
                {
                    ColdRestart.CrashLoseUnflushed => CrashMode.LoseUnflushed,
                    ColdRestart.CrashKeepUnflushed => CrashMode.KeepUnflushed,
                    ColdRestart.CrashRandomSubset => CrashMode.RandomSubset,
                    _ => CrashMode.TearLastWrite,
                });
            }

            foreach (TestNode node in allNodes)
                await node.RestartAsync();
            await cluster.WaitForStableAsync(null, ct);
            await cluster.WaitForReplicationAsync(Vol, ct);

            await LockHelpers.AssertWriterGenerationOnEveryReplicaAsync(cluster, f1, gA, "generation recovered from durable state after the cold restart");
            await LockHelpers.AssertWriterGenerationOnEveryReplicaAsync(cluster, f2, h, "untouched inode's generation recovered");
            Assert.Equal(tA1.Sequence, (await LockHelpers.FileAsync(cluster, f1)).Revision);

            await LockHelpers.WaitForWriterGoneAsync(cluster, inode1, gA, lease * 4, "the paused writer's lease must expire after the restart", ct);

            await using DfsClient clientC = await LockHelpers.ConnectAsync(cluster, "c", ct);
            DfsVolume vc = clientC.GetVolume(Vol);
            await using DfsStream c = await vc.Files.OpenAsync(f1, LockHelpers.Writer(), ct);
            ulong gC = c.WriterGeneration;
            Assert.True(accepted.All(g => gC > g), $"post-restart generation {gC} must exceed every accepted generation [{string.Join(", ", accepted)}]");
            await using (DfsStream c2 = await vc.Files.OpenAsync(f2, LockHelpers.Writer(), ct))
                Assert.True(c2.WriterGeneration > h, $"post-restart generation {c2.WriterGeneration} on f2 must exceed {h}");

            byte[] dataC = TestData.Bytes(12_400, 3500);
            CommitToken tC = await LockHelpers.ReplaceAndCommitAsync(c, dataC, ct);

            pausedA.Resume();
            LockHelpers.AssertLeaseLost(await LockHelpers.WithinAsync(cluster, LockHelpers.CatchAsync(() => a.CommitAsync(ct)), lease * 10, "A's publication", ct),
                "publication with a generation accepted before the cold restart");

            await LockHelpers.AssertWriterGenerationOnEveryReplicaAsync(cluster, f1, gC, "after the post-restart grant");
            await LockHelpers.AssertAtMostOneActiveWriterOnEveryReplicaAsync(cluster, inode1, "after the cold restart");
            Assert.Equal(tC.Sequence, (await LockHelpers.FileAsync(cluster, f1)).Revision);
            LockHelpers.BytesEqual(dataC, await LockHelpers.ReadCommittedAsync(vc, f1, ct), "the post-restart writer is authoritative");

            await LockHelpers.CatchAsync(() => a.DisposeAsync().AsTask());
            await clientA.DisposeAsync();
        }, ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // LOCK-013
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<int> PartitionSizes() => new() { 3, 5 };

    /// <summary>
    /// LOCK-013: the volume leader (plus one follower with 5 nodes) is cut off with client M. Throughout the partition
    /// the minority grants no new writer, accepts no publication from M's existing writer and applies nothing; the
    /// majority elects a leader, expires M's writer and grants new writers. After healing the majority's history wins,
    /// M's unknown operations resolve as aborted (or expired with M's session), never committed, and M's stale writer
    /// cannot publish.
    /// </summary>
    [Theory]
    [MemberData(nameof(PartitionSizes))]
    [Catalog("LOCK-013", "S", "INV-02", "INV-05", "INV-06", "INV-12")]
    public async Task LOCK_013_PartitionedMinorityCannotGrantWriter(int nodeCount)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(LockHelpers.Sim(nodeCount), async cluster =>
        {
            TimeSpan lease = LockHelpers.Lease(cluster);
            TimeSpan opTimeout = TimeSpan.FromSeconds(3);
            const string f = "/lock013/f";
            const string g = "/lock013/g";
            byte[] dataA = TestData.Bytes(13_001, 4000);
            byte[] minorityBytes = TestData.Bytes(13_002, 1000);
            byte[] dataB = TestData.Bytes(13_003, 4500);
            byte[] dataG = TestData.Bytes(13_004, 1500);

            TestNode leader = await cluster.GetLeaderAsync(Vol, ct);
            TestNode[] followers = LockHelpers.Running(cluster).Where(n => n.Name != leader.Name).OrderBy(n => n.Name, StringComparer.Ordinal).ToArray();
            string[] minority = nodeCount == 3 ? [leader.Name] : [leader.Name, followers[0].Name];
            string[] majority = LockHelpers.Running(cluster).Select(n => n.Name).Where(n => !minority.Contains(n)).ToArray();

            await using DfsClient setupClient = await LockHelpers.ConnectAsync(cluster, "setup", ct, seeds: majority);
            DfsVolume setup = setupClient.GetVolume(Vol);
            await setup.Directories.CreateDirectoryAsync("/lock013", ct);
            await setup.Files.WriteAllBytesAsync(f, TestData.Bytes(13_010, 3000), ct);
            await setup.Files.WriteAllBytesAsync(g, TestData.Bytes(13_011, 3000), ct);
            DfsFileInfo f0 = await setup.Files.GetInfoAsync(f, ct);
            DfsFileInfo g0 = await setup.Files.GetInfoAsync(g, ct);

            await using DfsClient clientM = await LockHelpers.ConnectAsync(cluster, "m", ct, operationTimeout: opTimeout, seeds: minority);
            DfsVolume vm = clientM.GetVolume(Vol);
            DfsStream a = await vm.Files.OpenAsync(f, LockHelpers.Writer(), ct);
            ulong gA = a.WriterGeneration;
            await LockHelpers.StageReplaceAsync(a, dataA, ct);
            await using DfsClient clientJ = await LockHelpers.ConnectAsync(cluster, "j", ct, seeds: majority);
            DfsVolume vj = clientJ.GetVolume(Vol);
            await cluster.WaitForReplicationAsync(Vol, ct);
            // Baseline after A's writable open (which may itself touch f's inode record) and before the partition.
            ulong fRevisionAtPartition = (await LockHelpers.FileAsync(cluster, f)).Revision;

            cluster.Network.Partition([.. minority, LockHelpers.Net("m")], [.. majority, LockHelpers.Net("j"), LockHelpers.Net("setup")]);

            TimeSpan bound = opTimeout * 6;
            var unknownOps = new List<DfsOperationId>();
            Exception? openError = await LockHelpers.CatchWithinAsync(cluster, async () =>
            {
                DfsStream s = await vm.Files.OpenAsync(g, LockHelpers.Writer(), ct);
                await s.DisposeAsync();
            }, bound, "minority writable open", ct);
            LockHelpers.AssertNotAuthorized(openError, "writer grant through the minority");
            Exception? flushError = await LockHelpers.CatchWithinAsync(cluster, () => a.CommitAsync(ct), bound, "minority flush", ct);
            LockHelpers.AssertNotAuthorized(flushError, "publication through the minority");
            Exception? writeAllError = await LockHelpers.CatchWithinAsync(cluster, () => vm.Files.WriteAllBytesAsync(g, minorityBytes, ct), bound, "minority WriteAllBytes", ct);
            LockHelpers.AssertNotAuthorized(writeAllError, "helper publication through the minority");
            foreach (Exception? error in new[] { openError, flushError, writeAllError })
            {
                if (LockHelpers.OperationIdOf(error) is DfsOperationId op)
                    unknownOps.Add(op);
            }

            // The minority's applied state never changed.
            foreach (string name in minority)
            {
                VolumeStateInspection? state = await cluster.Node(name).Inspect.GetVolumeStateAsync(Vol);
                Assert.NotNull(state);
                FileInspection? fm = state.GetFile(f);
                FileInspection? gm = state.GetFile(g);
                Assert.True(fm is not null && gm is not null);
                Assert.Equal(fRevisionAtPartition, fm!.Revision);
                Assert.Equal(g0.Revision, gm!.Revision);
                Assert.Equal(gA, fm.WriterGeneration);
                Assert.DoesNotContain(state.Handles, h => h.Inode == g0.InodeId && !h.Expired);
            }

            // The majority keeps authority: new leader, A expires there, new writers are granted.
            TestNode majorityLeader = await LockHelpers.WaitForNewLeaderAsync(cluster, leader.Name, lease * 5, ct);
            Assert.Contains(majorityLeader.Name, majority);
            await using (DfsStream wg = await vj.Files.OpenAsync(g, LockHelpers.Writer(), ct))
                await LockHelpers.ReplaceAndCommitAsync(wg, dataG, ct);
            await LockHelpers.WaitForWriterGoneAsync(cluster, f0.InodeId, gA, lease * 4, "A's lease must expire on the majority side", ct, majorityLeader.Name);
            await using DfsStream b = await vj.Files.OpenAsync(f, LockHelpers.Writer(), ct);
            ulong gB = b.WriterGeneration;
            Assert.True(gB > gA);
            CommitToken tB = await LockHelpers.ReplaceAndCommitAsync(b, dataB, ct);

            cluster.Network.Heal();
            await cluster.WaitForStableAsync(null, ct);
            await cluster.WaitForReplicationAsync(Vol, ct);

            Exception? retry = await LockHelpers.CatchWithinAsync(cluster, async () =>
            {
                await LockHelpers.StageReplaceAsync(a, minorityBytes, ct);
                await a.CommitAsync(ct);
            }, bound, "A's retry after healing", ct);
            Assert.True(retry is not null, "M's stale writer published after the partition healed");
            Assert.True(retry is DfsLeaseLostException || a.IsFaulted, $"A must be fenced (LeaseLost) or already faulted, got {LockHelpers.Describe(retry)}");

            foreach (DfsOperationId op in unknownOps)
            {
                // M's session may have expired on the majority side during the partition; an expired id is an honest
                // definite answer too. What must never happen is a commit.
                DfsOperationOutcome outcome = await vj.QueryOperationAsync(op, resolve: true, ct);
                Assert.True(outcome.Status is DfsOutcomeStatus.Aborted or DfsOutcomeStatus.Expired,
                    $"minority operation {op} must never commit, resolved as {outcome.Status}");
            }

            LockHelpers.BytesEqual(dataB, await LockHelpers.ReadCommittedAsync(vj, f, ct), "f holds the majority writer's contents");
            LockHelpers.BytesEqual(dataG, await LockHelpers.ReadCommittedAsync(vj, g, ct), "g holds the majority writer's contents");
            Assert.Equal(tB.Sequence, (await LockHelpers.FileAsync(cluster, f)).Revision);
            await LockHelpers.AssertWriterGenerationOnEveryReplicaAsync(cluster, f, gB, "after healing");
            await LockHelpers.AssertAtMostOneActiveWriterOnEveryReplicaAsync(cluster, f0.InodeId, "f after healing");
            await LockHelpers.AssertAtMostOneActiveWriterOnEveryReplicaAsync(cluster, g0.InodeId, "g after healing");
            await cluster.Inspect.AssertNoSplitBrainAsync();
            await cluster.Inspect.AssertReplicasConvergedAsync(Vol);
            await LockHelpers.CatchAsync(() => a.DisposeAsync().AsTask());
        }, ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // LOCK-014
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<ClientClockChange> ClientClockChanges()
    {
        var data = new TheoryData<ClientClockChange>();
        foreach (ClientClockChange change in Enum.GetValues<ClientClockChange>())
            data.Add(change);
        return data;
    }

    private static void ApplyClientClockChange(TestClock clock, ClientClockChange change, TimeSpan lease)
    {
        switch (change)
        {
            case ClientClockChange.WallBackward:
                clock.JumpWall(-TimeSpan.FromHours(1));
                break;
            case ClientClockChange.WallForward:
                clock.JumpWall(TimeSpan.FromHours(1));
                break;
            case ClientClockChange.Frozen:
                clock.Freeze();
                break;
            case ClientClockChange.SlowRate:
                clock.SetRate(0.1);
                break;
            case ClientClockChange.MonotonicJumpForward:
                clock.Advance(lease * 3);
                break;
        }
    }

    /// <summary>
    /// LOCK-014: the writer's client clock is jumped (wall backward/forward, monotonic forward), slowed or frozen
    /// (timers suspended) and its renewals cannot reach the server. Whatever the client believes about its lease, the
    /// server expires it on its own clock, grants B, and rejects A's publication with LeaseLost.
    /// </summary>
    [Theory]
    [MemberData(nameof(ClientClockChanges))]
    [Catalog("LOCK-014", "S", "INV-02", "INV-05", "INV-06", "INV-12")]
    public async Task LOCK_014_ClientClockJumpCannotExtendAuthority(ClientClockChange change)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(LockHelpers.Sim(), async cluster =>
        {
            TimeSpan lease = LockHelpers.Lease(cluster);
            const string path = "/lock014/f";
            byte[] dataA = TestData.Bytes(14_001, 3000);
            byte[] dataB = TestData.Bytes(14_002, 3500);
            await using DfsClient setupClient = await LockHelpers.ConnectAsync(cluster, "setup", ct);
            DfsVolume setup = setupClient.GetVolume(Vol);
            await setup.Directories.CreateDirectoryAsync("/lock014", ct);
            await setup.Files.WriteAllBytesAsync(path, TestData.Bytes(14_000, 2000), ct);
            InodeId inode = (await setup.Files.GetInfoAsync(path, ct)).InodeId;

            var clockA = new TestClock(cluster.Time);
            await using DfsClient clientA = await LockHelpers.ConnectAsync(cluster, "a", ct, clock: clockA);
            DfsStream a = await clientA.GetVolume(Vol).Files.OpenAsync(path, LockHelpers.Writer(), ct);
            ulong gA = a.WriterGeneration;
            await LockHelpers.StageReplaceAsync(a, dataA, ct);

            ApplyClientClockChange(clockA, change, lease);
            cluster.Network.Isolate(LockHelpers.Net("a"));
            await LockHelpers.WaitForWriterGoneAsync(cluster, inode, gA, lease * 4, "the server expires A on its own clock regardless of A's clock", ct);

            await using DfsClient clientB = await LockHelpers.ConnectAsync(cluster, "b", ct);
            DfsVolume vb = clientB.GetVolume(Vol);
            await using DfsStream b = await vb.Files.OpenAsync(path, LockHelpers.Writer(), ct);
            ulong gB = b.WriterGeneration;
            Assert.True(gB > gA);
            CommitToken tB = await LockHelpers.ReplaceAndCommitAsync(b, dataB, ct);

            cluster.Network.Heal();
            // The changed clock covered A's isolation, expiry and B's grant — the part under test. A's SDK then needs
            // working timers (reconnect backoff, request timeouts) to complete its next call within the bound.
            if (change == ClientClockChange.Frozen)
                clockA.Unfreeze();
            if (change == ClientClockChange.SlowRate)
                clockA.SetRate(1.0);
            Exception? stale = await LockHelpers.CatchWithinAsync(cluster, () => a.CommitAsync(ct), lease * 20, "A's publication after reconnecting", ct);
            LockHelpers.AssertLeaseLost(stale, $"A's publication with client clock change {change}");
            Assert.True(a.IsFaulted);

            await LockHelpers.AssertAtMostOneActiveWriterOnEveryReplicaAsync(cluster, inode, "after A reconnected");
            Assert.Equal(gB, Assert.Single(await LockHelpers.ActiveWritersAsync(cluster, inode)).Generation);
            Assert.Equal(tB.Sequence, (await LockHelpers.FileAsync(cluster, path)).Revision);
            LockHelpers.BytesEqual(dataB, await LockHelpers.ReadCommittedAsync(vb, path, ct), "B's contents are authoritative");

            clockA.SetRate(1.0);
            await LockHelpers.CatchAsync(() => a.DisposeAsync().AsTask());
        }, ct);
    }

    /// <summary>
    /// LOCK-014 (connected): the same client clock changes with the network intact, while A keeps publishing through its
    /// original handle and B keeps trying to take the inode. Legal outcomes: A keeps authority (B is refused) or A loses
    /// it (server-side expiry) and B takes over — but never both: no A publication commits after B's grant, and every
    /// replica holds at most one active writer at every step.
    /// </summary>
    /// <remarks>
    /// Which branch occurs is availability, not safety: a frozen/slowed client may still keep its lease if the server
    /// counts its publications as activity, and a client that distrusts its own clock may conservatively stop
    /// publishing. The deterministic replacement (renewals cannot arrive) is covered by the isolated variant above.
    /// </remarks>
    [Theory]
    [MemberData(nameof(ClientClockChanges))]
    [Catalog("LOCK-014", "S", "INV-02", "INV-05", "INV-06", "INV-12")]
    public async Task LOCK_014_ClientClockJumpCannotExtendAuthority_SingleWriterEitherWay(ClientClockChange change)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(LockHelpers.Sim(), async cluster =>
        {
            TimeSpan lease = LockHelpers.Lease(cluster);
            const string path = "/lock014c/f";
            byte[] initial = TestData.Bytes(14_100, 2000);
            await using DfsClient setupClient = await LockHelpers.ConnectAsync(cluster, "setup", ct);
            DfsVolume setup = setupClient.GetVolume(Vol);
            await setup.Directories.CreateDirectoryAsync("/lock014c", ct);
            await setup.Files.WriteAllBytesAsync(path, initial, ct);
            InodeId inode = (await setup.Files.GetInfoAsync(path, ct)).InodeId;

            var clockA = new TestClock(cluster.Time);
            await using DfsClient clientA = await LockHelpers.ConnectAsync(cluster, "a", ct, clock: clockA, operationTimeout: lease);
            DfsStream a = await clientA.GetVolume(Vol).Files.OpenAsync(path, LockHelpers.Writer(), ct);
            await using DfsClient clientB = await LockHelpers.ConnectAsync(cluster, "b", ct, operationTimeout: lease);
            DfsVolume vb = clientB.GetVolume(Vol);

            ApplyClientClockChange(clockA, change, lease);
            var probe = new SingleWriterProbe(cluster, a, vb, path, inode, ct);
            const int iterations = 12;
            for (int i = 0; i < iterations; i++)
            {
                await probe.StepAsync(i);
                await cluster.DelayAsync(lease / 2, ct);
            }

            if (change == ClientClockChange.Frozen)
                clockA.Unfreeze();
            clockA.SetRate(1.0);

            byte[]? expected = await probe.ResolveAsync(vb);
            probe.AssertFinalContents(expected, initial, await LockHelpers.ReadCommittedAsync(setup, path, ct), "contents equal the last committed publication in log order");

            if (probe.BGrantIteration is null)
            {
                // Legal branch "A keeps authority": B was refused throughout, so no generation other than A's was ever
                // granted (A's handle may have lapsed only after B's last attempt).
                Assert.All(await LockHelpers.ActiveWritersAsync(cluster, inode), h => Assert.Equal(probe.AGeneration, h.Generation));
                Assert.Equal(probe.AGeneration, (await LockHelpers.FileAsync(cluster, path)).WriterGeneration);
            }
            else
            {
                Assert.Equal(iterations - 1 - probe.BGrantIteration.Value, probe.AFailuresAfterGrant);
                Assert.DoesNotContain(await LockHelpers.ActiveWritersAsync(cluster, inode), h => h.Generation == probe.AGeneration);
            }

            await probe.DisposeStreamsAsync();
        }, ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // LOCK-015
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<ServerClockTarget, ServerClockChange, ClockPhase> ServerClockSchedules()
    {
        var data = new TheoryData<ServerClockTarget, ServerClockChange, ClockPhase>();
        foreach (ServerClockTarget target in Enum.GetValues<ServerClockTarget>())
        {
            foreach (ServerClockChange change in Enum.GetValues<ServerClockChange>())
            {
                foreach (ClockPhase phase in Enum.GetValues<ClockPhase>())
                    data.Add(target, change, phase);
            }
        }

        return data;
    }

    /// <summary>
    /// LOCK-015: independent server clock changes (wall jumps, monotonic jump, freeze, fast/slow rate) on the leader, a
    /// follower or every node, applied while the writer renews normally or just before its lease would expire with its
    /// renewals withheld (and delivered late afterwards). Availability may drop (early expiry, elections, timeouts) but
    /// safety holds at every step: at most one active writer on every replica, and no publication by A commits after B
    /// was granted — unknown outcomes are resolved authoritatively before judging.
    /// </summary>
    [Theory]
    [MemberData(nameof(ServerClockSchedules))]
    [Catalog("LOCK-015", "S", "INV-02", "INV-05", "INV-06", "INV-12")]
    public async Task LOCK_015_ServerClockChangesDoNotCreateTwoWriters(ServerClockTarget target, ServerClockChange change, ClockPhase phase)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(LockHelpers.Sim(), async cluster =>
        {
            TimeSpan lease = LockHelpers.Lease(cluster);
            const string path = "/lock015/f";
            byte[] initial = TestData.Bytes(15_000, 2000);
            await using DfsClient setupClient = await LockHelpers.ConnectAsync(cluster, "setup", ct);
            DfsVolume setup = setupClient.GetVolume(Vol);
            await setup.Directories.CreateDirectoryAsync("/lock015", ct);
            await setup.Files.WriteAllBytesAsync(path, initial, ct);
            InodeId inode = (await setup.Files.GetInfoAsync(path, ct)).InodeId;

            await using DfsClient clientA = await LockHelpers.ConnectAsync(cluster, "a", ct, operationTimeout: lease);
            DfsStream a = await clientA.GetVolume(Vol).Files.OpenAsync(path, LockHelpers.Writer(), ct);
            await using DfsClient clientB = await LockHelpers.ConnectAsync(cluster, "b", ct, operationTimeout: lease);
            DfsVolume vb = clientB.GetVolume(Vol);

            TestNode leader = await cluster.GetLeaderAsync(Vol, ct);
            TestNode[] targets = target switch
            {
                ServerClockTarget.Leader => [leader],
                ServerClockTarget.Follower => [LockHelpers.Running(cluster).First(n => n.Name != leader.Name)],
                _ => LockHelpers.Running(cluster).ToArray(),
            };

            MessageHold? withheldRenewals = null;
            if (phase == ClockPhase.AroundExpiry)
            {
                withheldRenewals = cluster.Network.Hold(new MessageFilter(From: LockHelpers.Net("a"), Type: MessageType.KeepAlive));
                await cluster.DelayAsync(lease * 0.8, ct);
            }

            TimeSpan wallJump = change switch
            {
                ServerClockChange.WallForward => TimeSpan.FromHours(1),
                ServerClockChange.WallBackward => -TimeSpan.FromHours(1),
                _ => TimeSpan.Zero,
            };
            foreach (TestNode node in targets)
            {
                switch (change)
                {
                    case ServerClockChange.WallForward:
                    case ServerClockChange.WallBackward:
                        node.Clock.JumpWall(wallJump);
                        break;
                    case ServerClockChange.MonotonicJumpForward:
                        node.Clock.Advance(lease * 3);
                        break;
                    case ServerClockChange.Freeze:
                        node.Clock.Freeze();
                        break;
                    case ServerClockChange.FastRate:
                        node.Clock.SetRate(4.0);
                        break;
                    case ServerClockChange.SlowRate:
                        node.Clock.SetRate(0.25);
                        break;
                }
            }

            var probe = new SingleWriterProbe(cluster, a, vb, path, inode, ct);
            const int iterations = 12;
            for (int i = 0; i < iterations; i++)
            {
                if (change == ServerClockChange.Freeze && i == 8)
                {
                    foreach (TestNode node in targets)
                        node.Clock.Unfreeze();
                }

                await probe.StepAsync(i);
                await cluster.DelayAsync(lease / 4, ct);
            }

            if (withheldRenewals is not null)
            {
                withheldRenewals.ReleaseAll(reverseOrder: true); // late, out-of-order renewals after the clock change
                await cluster.DelayAsync(lease / 4, ct);
                await probe.StepAsync(iterations);
            }

            foreach (TestNode node in targets)
            {
                node.Clock.Unfreeze();
                node.Clock.SetRate(1.0);
                if (wallJump != TimeSpan.Zero)
                    node.Clock.JumpWall(-wallJump);
            }

            await cluster.WaitForStableAsync(null, ct);
            byte[]? expected = await probe.ResolveAsync(vb);
            await using DfsClient verifier = await LockHelpers.ConnectAsync(cluster, "verify", ct);
            probe.AssertFinalContents(expected, initial, await LockHelpers.ReadCommittedAsync(verifier.GetVolume(Vol), path, ct),
                "contents equal the last committed publication in log order");
            await LockHelpers.AssertAtMostOneActiveWriterOnEveryReplicaAsync(cluster, inode, "after clocks were restored");
            await cluster.Inspect.AssertNoSplitBrainAsync();
            if (probe.BGrantIteration is not null)
                Assert.True((await LockHelpers.FileAsync(cluster, path)).WriterGeneration >= probe.BGeneration);
            await probe.DisposeStreamsAsync();
        }, ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // LOCK-016
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<QueuedPublication, bool> QueuedPublications()
    {
        var data = new TheoryData<QueuedPublication, bool>();
        foreach (QueuedPublication kind in Enum.GetValues<QueuedPublication>())
        {
            data.Add(kind, false);
            data.Add(kind, true);
        }

        return data;
    }

    /// <summary>
    /// LOCK-016: A's publication request is queued in the network; A gives up (its call fails without success), closes
    /// its handle and its reservation is released; B acquires the inode and commits. Delivering the old queued request
    /// (forward/reverse) and replaying it again cannot mutate B's inode: contents, revision and generation stay B's, the
    /// replays are refused, and A's operation id resolves as aborted.
    /// </summary>
    [Theory]
    [MemberData(nameof(QueuedPublications))]
    [Catalog("LOCK-016", "S", "INV-02", "INV-05", "INV-06", "INV-12")]
    public async Task LOCK_016_ClosedHandleDelayedRequestRejected(QueuedPublication kind, bool reverseDelivery)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(LockHelpers.Sim(), async cluster =>
        {
            TimeSpan lease = LockHelpers.Lease(cluster);
            const string path = "/lock016/f";
            byte[] initial = TestData.Bytes(16_000, 3000);
            byte[] dataA = TestData.Bytes(16_001, 2000);
            byte[] dataB = TestData.Bytes(16_002, 4000);
            byte[] dataB2 = TestData.Bytes(16_003, 1000);
            await using DfsClient setupClient = await LockHelpers.ConnectAsync(cluster, "setup", ct);
            DfsVolume setup = setupClient.GetVolume(Vol);
            await setup.Directories.CreateDirectoryAsync("/lock016", ct);
            await setup.Files.WriteAllBytesAsync(path, initial, ct);
            InodeId inode = (await setup.Files.GetInfoAsync(path, ct)).InodeId;

            await using DfsClient clientA = await LockHelpers.ConnectAsync(cluster, "a", ct, operationTimeout: lease);
            DfsStream a = await clientA.GetVolume(Vol).Files.OpenAsync(path,
                LockHelpers.Writer(writeMode: kind == QueuedPublication.Flush ? DfsWriteMode.OnFlush : DfsWriteMode.WriteThrough), ct);
            ulong gA = a.WriterGeneration;

            MessageHold queued = cluster.Network.Hold(new MessageFilter(From: LockHelpers.Net("a"), Type: MessageType.CommitFile));
            Task<Exception?> publication = kind switch
            {
                QueuedPublication.Flush => LockHelpers.CatchAsync(async () =>
                {
                    await LockHelpers.StageReplaceAsync(a, dataA, ct);
                    await a.FlushAsync(ct);
                }),
                QueuedPublication.WriteThroughWrite => LockHelpers.CatchAsync(() => a.WriteAsync(dataA, ct).AsTask()),
                _ => LockHelpers.CatchAsync(() => a.SetLengthAsync(1, ct)),
            };
            await queued.WaitForAsync(1, lease * 2);

            Exception? publicationError = await LockHelpers.WithinAsync(cluster, publication, lease * 10, "A's publication with its request queued", ct);
            Assert.True(publicationError is not null, "A's publication cannot succeed while its request never reached the server");
            Assert.True(LockHelpers.IsAvailabilityOrUnknown(publicationError),
                $"the outcome is unknown to A (never a fabricated definite result): {LockHelpers.Describe(publicationError)}");
            DfsOperationId? opA = LockHelpers.OperationIdOf(publicationError);

            // A releases its reservation (close; or, if the SDK keeps it while unresolved, safe expiry).
            await LockHelpers.WithinAsync(cluster, LockHelpers.CatchAsync(() => a.DisposeAsync().AsTask()), lease * 10, "disposing A's faulted stream", ct);
            await LockHelpers.WaitForWriterGoneAsync(cluster, inode, gA, lease * 4, "A's reservation must be released", ct);

            await using DfsClient clientB = await LockHelpers.ConnectAsync(cluster, "b", ct);
            DfsVolume vb = clientB.GetVolume(Vol);
            await using DfsStream b = await vb.Files.OpenAsync(path, LockHelpers.Writer(), ct);
            ulong gB = b.WriterGeneration;
            Assert.True(gB > gA);
            CommitToken tB = await LockHelpers.ReplaceAndCommitAsync(b, dataB, ct);

            CapturedMessage[] stale = queued.Captured.ToArray();
            Assert.NotEmpty(stale);
            queued.ReleaseAll(reverseDelivery);
            foreach (CapturedMessage message in stale)
            {
                DfsErrorCode code = await cluster.Network.ReplayAsync(message);
                Assert.True(code != DfsErrorCode.None, $"replayed {message.Type} from the closed handle was accepted");
            }

            await cluster.DelayAsync(lease, ct);
            FileInspection file = await LockHelpers.FileAsync(cluster, path);
            Assert.Equal(inode, file.Inode);
            Assert.Equal(tB.Sequence, file.Revision);
            Assert.Equal(dataB.Length, file.Length);
            await LockHelpers.AssertWriterGenerationOnEveryReplicaAsync(cluster, path, gB, "after the old queued request was delivered");
            await LockHelpers.AssertAtMostOneActiveWriterOnEveryReplicaAsync(cluster, inode, "after the old queued request was delivered");
            Assert.Equal(gB, Assert.Single(await LockHelpers.ActiveWritersAsync(cluster, inode)).Generation);
            LockHelpers.BytesEqual(dataB, await LockHelpers.ReadCommittedAsync(vb, path, ct), "the successor's inode state is untouched");

            if (opA is not null)
            {
                DfsOperationOutcome outcome = await vb.QueryOperationAsync(opA.Value, resolve: true, ct);
                Assert.True(outcome.Status == DfsOutcomeStatus.Aborted, $"A's queued publication {opA} must resolve as aborted, got {outcome.Status}");
            }

            CommitToken tB2 = await LockHelpers.ReplaceAndCommitAsync(b, dataB2, ct);
            Assert.True(tB2.CompareTo(tB) > 0);
            LockHelpers.BytesEqual(dataB2, await LockHelpers.ReadCommittedAsync(vb, path, ct), "B keeps publishing");
        }, ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // LOCK-017
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<StalePath> StalePaths()
    {
        var data = new TheoryData<StalePath>();
        foreach (StalePath path in Enum.GetValues<StalePath>())
            data.Add(path);
        return data;
    }

    /// <summary>
    /// LOCK-017: writer A is paused, expired and replaced by B (generation gB). Then A — or A's client — attempts a
    /// stale write through one path: its own stream (flush, commit, write-through, write-through SetLength, dispose),
    /// every convenience helper, a transaction commit, truncating/creating opens, a replay of its queued CommitFile RPC,
    /// a direct storage put without capability, or a restore (by A's client) of a snapshot taken while A was the writer.
    /// No path bypasses authority: stream paths fail with LeaseLost, reservation-taking paths with a sharing conflict,
    /// raw RPCs are refused, restore creates a new volume without carrying reservations and cannot touch the live inode
    /// — and B's inode keeps B's contents, revision and generation.
    /// </summary>
    [Theory]
    [MemberData(nameof(StalePaths))]
    [Catalog("LOCK-017", "S", "INV-02", "INV-05", "INV-06", "INV-12")]
    public async Task LOCK_017_AllMutationPathsUseFencing(StalePath stalePath)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(LockHelpers.Sim(), async cluster =>
        {
            TimeSpan lease = LockHelpers.Lease(cluster);
            const string path = "/lock017/f";
            const string src = "/lock017/src";
            byte[] dataA1 = TestData.Bytes(17_001, 3000);
            byte[] dataA2 = TestData.Bytes(17_002, 2500);
            byte[] dataB = TestData.Bytes(17_003, 3500);
            byte[] dataB2 = TestData.Bytes(17_004, 1200);
            byte[] srcBytes = TestData.Bytes(17_005, 900);
            await using DfsClient setupClient = await LockHelpers.ConnectAsync(cluster, "setup", ct);
            DfsVolume setup = setupClient.GetVolume(Vol);
            await setup.Directories.CreateDirectoryAsync("/lock017", ct);
            await setup.Files.WriteAllBytesAsync(path, TestData.Bytes(17_000, 2000), ct);
            await setup.Files.WriteAllBytesAsync(src, srcBytes, ct);
            InodeId inode = (await setup.Files.GetInfoAsync(path, ct)).InodeId;

            bool writeThrough = stalePath is StalePath.StreamWriteThrough or StalePath.StreamSetLengthWriteThrough;
            var clockA = new TestClock(cluster.Time);
            DfsClient clientA = await LockHelpers.ConnectAsync(cluster, "a", ct, clock: clockA);
            DfsVolume va = clientA.GetVolume(Vol);
            DfsStream a = await va.Files.OpenAsync(path,
                LockHelpers.Writer(FileAccess.ReadWrite, writeMode: writeThrough ? DfsWriteMode.WriteThrough : DfsWriteMode.OnFlush), ct);
            ulong gA = a.WriterGeneration;
            if (writeThrough)
            {
                await a.WriteAsync(dataA1, ct); // published by the call itself (valid generation)
            }
            else
            {
                await LockHelpers.ReplaceAndCommitAsync(a, dataA1, ct);
            }

            MessageHold? queuedCommit = null;
            Task<Exception?>? queuedFlush = null;
            if (stalePath == StalePath.ReplayedCommitFileRpc)
            {
                queuedCommit = cluster.Network.Hold(new MessageFilter(From: LockHelpers.Net("a"), Type: MessageType.CommitFile));
                queuedFlush = LockHelpers.CatchAsync(async () =>
                {
                    await LockHelpers.StageReplaceAsync(a, dataA2, ct);
                    await a.FlushAsync(ct);
                });
                await queuedCommit.WaitForAsync(1, lease * 2);
            }

            const string preSnapshot = "lock017-pre";
            if (stalePath == StalePath.Restore)
                await setup.Snapshots.CreateAsync(preSnapshot, null, ct); // holds A's accepted publication dataA1

            PausedClient pausedA = PausedClient.Pause(cluster, "a", clockA);
            await LockHelpers.WaitForWriterGoneAsync(cluster, inode, gA, lease * 4, "A's lease must expire while A is paused", ct);
            await using DfsClient clientB = await LockHelpers.ConnectAsync(cluster, "b", ct);
            DfsVolume vb = clientB.GetVolume(Vol);
            await using DfsStream b = await vb.Files.OpenAsync(path, LockHelpers.Writer(), ct);
            ulong gB = b.WriterGeneration;
            Assert.True(gB > gA);
            CommitToken tB = await LockHelpers.ReplaceAndCommitAsync(b, dataB, ct);
            pausedA.Resume();

            string what = $"stale path {stalePath}";
            switch (stalePath)
            {
                case StalePath.StreamFlush:
                    LockHelpers.AssertLeaseLost(await LockHelpers.CatchAsync(async () =>
                    {
                        await LockHelpers.StageReplaceAsync(a, dataA2, ct);
                        await a.FlushAsync(ct);
                    }), what);
                    break;
                case StalePath.StreamCommit:
                    LockHelpers.AssertLeaseLost(await LockHelpers.CatchAsync(async () =>
                    {
                        await LockHelpers.StageReplaceAsync(a, dataA2, ct);
                        await a.CommitAsync(ct);
                    }), what);
                    break;
                case StalePath.StreamWriteThrough:
                    LockHelpers.AssertLeaseLost(await LockHelpers.CatchAsync(() =>
                    {
                        a.Seek(0, SeekOrigin.Begin);
                        return a.WriteAsync(dataA2, ct).AsTask();
                    }), what);
                    break;
                case StalePath.StreamSetLengthWriteThrough:
                    LockHelpers.AssertLeaseLost(await LockHelpers.CatchAsync(() => a.SetLengthAsync(1, ct)), what);
                    break;
                case StalePath.StreamDispose:
                    LockHelpers.AssertLeaseLost(await LockHelpers.CatchAsync(async () =>
                    {
                        await LockHelpers.StageReplaceAsync(a, dataA2, ct);
                        await a.DisposeAsync();
                    }), what);
                    break;
                case StalePath.HelperWriteAllBytes:
                    LockHelpers.AssertSharingConflict(await LockHelpers.CatchAsync(() => va.Files.WriteAllBytesAsync(path, dataA2, ct)), what);
                    break;
                case StalePath.HelperAppendAllBytes:
                    LockHelpers.AssertSharingConflict(await LockHelpers.CatchAsync(() => va.Files.AppendAllBytesAsync(path, dataA2, ct)), what);
                    break;
                case StalePath.HelperCopyOverwrite:
                    LockHelpers.AssertSharingConflict(await LockHelpers.CatchAsync(() => va.Files.CopyAsync(src, path, true, ct)), what);
                    break;
                case StalePath.HelperMoveOverwrite:
                    LockHelpers.AssertSharingConflict(await LockHelpers.CatchAsync(() => va.Files.MoveAsync(src, path, true, ct)), what);
                    break;
                case StalePath.HelperDelete:
                    LockHelpers.AssertSharingConflict(await LockHelpers.CatchAsync(() => va.Files.DeleteAsync(path, ct)), what);
                    break;
                case StalePath.TransactionCommit:
                {
                    await using DfsTransaction tx = await va.Transactions.BeginAsync(null, ct);
                    Exception? txError = await LockHelpers.CatchAsync(async () =>
                    {
                        await tx.Files.WriteAllBytesAsync(path, dataA2, ct);
                        await tx.CommitAsync(ct);
                    });
                    LockHelpers.AssertSharingConflict(txError, what);
                    Assert.NotEqual(DfsTransactionState.Committed, tx.State);
                    break;
                }

                case StalePath.TruncateOpen:
                    LockHelpers.AssertSharingConflict(await LockHelpers.CatchAsync(async () =>
                    {
                        DfsStream s = await va.Files.OpenAsync(path, FileMode.Truncate, FileAccess.Write, FileShare.Read, ct);
                        await s.DisposeAsync();
                    }), what);
                    break;
                case StalePath.CreateOpen:
                    LockHelpers.AssertSharingConflict(await LockHelpers.CatchAsync(async () =>
                    {
                        DfsStream s = await va.Files.OpenAsync(path, FileMode.Create, FileAccess.ReadWrite, FileShare.Read, ct);
                        await s.DisposeAsync();
                    }), what);
                    break;
                case StalePath.ReplayedCommitFileRpc:
                {
                    CapturedMessage[] captured = queuedCommit!.Captured.ToArray();
                    Assert.NotEmpty(captured);
                    queuedCommit.ReleaseAll();
                    LockHelpers.AssertLeaseLost(await LockHelpers.WithinAsync(cluster, queuedFlush!, lease * 10, "A's queued flush", ct), what);
                    foreach (CapturedMessage message in captured)
                    {
                        DfsErrorCode code = await cluster.Network.ReplayAsync(message);
                        Assert.True(code != DfsErrorCode.None, $"{what}: replayed CommitFile of generation {gA} was accepted");
                    }

                    break;
                }

                case StalePath.DirectObjectPut:
                {
                    TestNode leader = await cluster.GetLeaderAsync(Vol, ct);
                    await using RawProtocolClient raw = await cluster.ConnectRawAsync(leader.Name, ct);
                    Assert.Equal(DfsErrorCode.None, await raw.AuthenticateAsync(cluster.AdminCredentials));
                    ObjectId forged = ObjectId.New(new SeededRandom(17_017));
                    DfsErrorCode code = await raw.PutObjectDirectAsync(Vol, forged, dataA2);
                    Assert.True(code != DfsErrorCode.None, $"{what}: storage accepted an object without a staging capability");
                    Assert.DoesNotContain(forged, await cluster.Inspect.GetAllStoredObjectsAsync(Vol));
                    LockHelpers.AssertLeaseLost(await LockHelpers.CatchAsync(async () =>
                    {
                        await LockHelpers.StageReplaceAsync(a, dataA2, ct);
                        await a.FlushAsync(ct);
                    }), $"{what}: the stale stream is still fenced");
                    break;
                }

                case StalePath.Restore:
                {
                    // A's client restores the snapshot taken while A was the writer. Restore always creates a NEW volume
                    // (FSD §11, ARCHITECTURE §5.7): it can neither overwrite B's live inode nor carry any reservation.
                    const string restoredName = "lock017-restored";
                    DfsVolumeInfo live = await va.GetInfoAsync(ct);
                    DfsVolumeInfo restored = await va.Snapshots.RestoreAsync(preSnapshot, restoredName, ct);
                    Assert.NotEqual(live.Id, restored.Id);
                    Assert.Equal(restoredName, restored.Name);
                    await cluster.GetLeaderAsync(restoredName, ct);
                    VolumeStateInspection restoredState = await cluster.Inspect.GetVolumeStateAsync(restoredName);
                    Assert.DoesNotContain(restoredState.Handles, h => !h.Expired); // neither A's nor B's reservation is carried over
                    DfsVolume rv = clientB.GetVolume(restoredName);
                    LockHelpers.BytesEqual(dataA1, await LockHelpers.ReadCommittedAsync(rv, path, ct), $"{what}: restored contents are the snapshot's");
                    await using (DfsStream fresh = await rv.Files.OpenAsync(path, LockHelpers.Writer(), ct))
                        Assert.True(fresh.WriterGeneration > 0, "a new writer of the restored volume is granted freshly");
                    LockHelpers.AssertLeaseLost(await LockHelpers.CatchAsync(async () =>
                    {
                        await LockHelpers.StageReplaceAsync(a, dataA2, ct);
                        await a.FlushAsync(ct);
                    }), $"{what}: restore did not renew the stale writer");
                    break;
                }
            }

            // B's inode is untouched by every stale path, and B keeps authority.
            await cluster.DelayAsync(lease / 2, ct);
            FileInspection file = await LockHelpers.FileAsync(cluster, path);
            Assert.Equal(inode, file.Inode);
            Assert.Equal(tB.Sequence, file.Revision);
            await LockHelpers.AssertWriterGenerationOnEveryReplicaAsync(cluster, path, gB, what);
            await LockHelpers.AssertAtMostOneActiveWriterOnEveryReplicaAsync(cluster, inode, what);
            Assert.Equal(gB, Assert.Single(await LockHelpers.ActiveWritersAsync(cluster, inode)).Generation);
            LockHelpers.BytesEqual(dataB, await LockHelpers.ReadCommittedAsync(vb, path, ct), $"{what}: B's committed contents");
            LockHelpers.BytesEqual(srcBytes, await LockHelpers.ReadCommittedAsync(vb, src, ct), $"{what}: the copy/move source is untouched");
            CommitToken tB2 = await LockHelpers.ReplaceAndCommitAsync(b, dataB2, ct);
            Assert.True(tB2.CompareTo(tB) > 0);

            await LockHelpers.CatchAsync(() => a.DisposeAsync().AsTask());
            await clientA.DisposeAsync();
        }, ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // LOCK-018
    // ------------------------------------------------------------------------------------------------------------

    public static TheoryData<TransactionEnd> TransactionEnds()
    {
        var data = new TheoryData<TransactionEnd>();
        foreach (TransactionEnd end in Enum.GetValues<TransactionEnd>())
            data.Add(end);
        return data;
    }

    /// <summary>
    /// LOCK-018: a transaction holding writer reservations on three inodes ends by abort, dispose, commit conflict,
    /// timeout, client death (session expiry) or leader change. Its reservations are released (immediately for explicit
    /// ends, by bounded server-side cleanup otherwise) and nothing it wrote becomes visible. A newer holder then takes
    /// one inode; late cleanup of the old transaction and replays of its reservation requests/abort/commit neither
    /// release nor steal the newer holder's reservation nor re-reserve the other inodes.
    /// </summary>
    [Theory]
    [MemberData(nameof(TransactionEnds))]
    [Catalog("LOCK-018", "S", "INV-02", "INV-05", "INV-06", "INV-12")]
    public async Task LOCK_018_TransactionReservationCleanup(TransactionEnd end)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(LockHelpers.Sim(), async cluster =>
        {
            TimeSpan lease = LockHelpers.Lease(cluster);
            TimeSpan session = LockHelpers.SessionTimeout(cluster);
            string[] targets = ["/lock018/a", "/lock018/b", "/lock018/c"];
            const string readSet = "/lock018/r";
            byte[][] baseline = [TestData.Bytes(18_001, 2000), TestData.Bytes(18_002, 2000), TestData.Bytes(18_003, 2000)];
            byte[] dataB = TestData.Bytes(18_010, 2200);

            await using DfsClient otherClient = await LockHelpers.ConnectAsync(cluster, "other", ct);
            await using DfsClient thirdClient = await LockHelpers.ConnectAsync(cluster, "third", ct);
            DfsVolume other = otherClient.GetVolume(Vol);
            DfsVolume third = thirdClient.GetVolume(Vol);
            await other.Directories.CreateDirectoryAsync("/lock018", ct);
            for (int i = 0; i < targets.Length; i++)
                await other.Files.WriteAllBytesAsync(targets[i], baseline[i], ct);
            await other.Files.WriteAllBytesAsync(readSet, TestData.Bytes(18_004, 500), ct);
            InodeId inodeA = (await other.Files.GetInfoAsync(targets[0], ct)).InodeId;

            // Long enough for the three reservations and the enforcement checks below (a few Raft round trips), short
            // enough to expire well before the session timeout.
            TimeSpan txTimeout = lease + lease / 2;
            DfsClient txClient = await LockHelpers.ConnectAsync(cluster, "t1", ct);
            DfsVolume tv = txClient.GetVolume(Vol);
            long txBegunAt = cluster.Time.GetTimestamp();
            DfsTransaction tx = await tv.Transactions.BeginAsync(
                end == TransactionEnd.TimeoutExpiry ? new DfsTransactionOptions { Timeout = txTimeout } : null, ct);
            if (end == TransactionEnd.CommitConflict)
                await tx.Files.ReadAllBytesAsync(readSet, ct);

            var captured = new List<CapturedMessage>();
            async Task CaptureAsync(MessageType type, Func<Task> action)
            {
                MessageHold hold = cluster.Network.Hold(new MessageFilter(From: LockHelpers.Net("t1"), Type: type));
                Task running = action();
                await hold.WaitForAsync(1, lease * 2);
                captured.AddRange(hold.Captured);
                hold.ReleaseAll();
                await running;
            }

            for (int i = 0; i < targets.Length; i++)
            {
                int index = i;
                // A transaction-bound helper is sent as its ordinary request type carrying the transaction in its header
                // (PROTOCOL.md: TxOp is only an optional batching envelope), so the WriteAll requests are what a replay reuses.
                await CaptureAsync(MessageType.WriteAll, () => tx.Files.WriteAllBytesAsync(targets[index], TestData.Bytes(18_100 + (ulong)index, 1800), ct));
            }

            foreach (string target in targets)
            {
                (DfsStream? s, Exception? e) = await LockHelpers.TryOpenAsync(other, target, LockHelpers.Writer(), ct);
                if (s is not null)
                {
                    await s.DisposeAsync();
                    Assert.Fail($"{target}: the transaction's writer reservation was not enforced");
                }

                LockHelpers.AssertSharingConflict(e, $"{target} reserved by the transaction");
            }

            if (end == TransactionEnd.TimeoutExpiry)
            {
                Assert.True(cluster.Time.GetElapsedTime(txBegunAt) < txTimeout,
                    $"test precondition: the reservations were verified before the {txTimeout} transaction timeout");
            }

            TestNode? crashedLeader = null;
            switch (end)
            {
                case TransactionEnd.Abort:
                    await CaptureAsync(MessageType.TxAbort, () => tx.AbortAsync(ct));
                    Assert.Equal(DfsTransactionState.Aborted, tx.State);
                    break;
                case TransactionEnd.DisposeWithoutCommit:
                    await CaptureAsync(MessageType.TxAbort, () => tx.DisposeAsync().AsTask());
                    break;
                case TransactionEnd.CommitConflict:
                {
                    await other.Files.WriteAllBytesAsync(readSet, TestData.Bytes(18_005, 600), ct);
                    Exception? commitError = null;
                    await CaptureAsync(MessageType.TxCommit, async () => commitError = await LockHelpers.CatchAsync(() => tx.CommitAsync(ct)));
                    if (commitError is not DfsTransactionConflictException conflict || conflict.ErrorCode != DfsErrorCode.TransactionConflict)
                    {
                        Assert.Fail($"a changed read set must abort the commit with TransactionConflict, got {LockHelpers.Describe(commitError)}");
                        return;
                    }

                    Assert.Equal(tx.Id, conflict.TransactionId);
                    Assert.Equal(DfsTransactionState.Aborted, tx.State);
                    break;
                }

                case TransactionEnd.TimeoutExpiry:
                {
                    await cluster.DelayAsync(txTimeout * 3, ct);
                    Exception? expired = await LockHelpers.CatchAsync(() => tx.CommitAsync(ct));
                    Assert.True(expired is DfsTransactionAbortedException or DfsLimitExceededException,
                        $"an expired transaction must refuse to commit with a typed abort (TransactionAborted, or the exceeded duration limit), got {LockHelpers.Describe(expired)}");
                    Assert.NotEqual(DfsTransactionState.Committed, tx.State);
                    break;
                }

                case TransactionEnd.ClientKilled:
                    cluster.KillClient(txClient);
                    break;
                case TransactionEnd.LeaderChange:
                {
                    crashedLeader = await cluster.GetLeaderAsync(Vol, ct);
                    await crashedLeader.CrashAsync();
                    await LockHelpers.WaitForNewLeaderAsync(cluster, crashedLeader.Name, lease * 5, ct);
                    Exception? aborted = await LockHelpers.CatchAsync(() => tx.CommitAsync(ct));
                    if (aborted is DfsCommitOutcomeUnknownException unknown)
                    {
                        // The commit request may have been in flight around the crash: resolve it authoritatively.
                        Assert.Equal(tx.Id, unknown.TransactionId);
                        DfsOperationOutcome outcome = await other.Transactions.QueryOutcomeAsync(tx.Id, resolve: true, ct);
                        Assert.True(outcome.Status == DfsOutcomeStatus.Aborted, $"a leader change aborts active transactions; the commit resolved as {outcome.Status}");
                    }
                    else
                    {
                        Assert.True(aborted is DfsTransactionAbortedException, $"a leader change aborts active transactions, got {LockHelpers.Describe(aborted)}");
                    }

                    break;
                }
            }

            // Release: immediate for explicit ends, bounded server-side cleanup otherwise.
            bool explicitEnd = end is TransactionEnd.Abort or TransactionEnd.DisposeWithoutCommit or TransactionEnd.CommitConflict;
            foreach (string target in targets)
            {
                if (explicitEnd)
                {
                    (DfsStream? s, Exception? e) = await LockHelpers.TryOpenAsync(other, target, LockHelpers.Writer(), ct);
                    if (s is null)
                    {
                        Assert.Fail($"{target}: reservation still held right after the transaction ended ({end}): {LockHelpers.Describe(e)}");
                        return;
                    }

                    await s.DisposeAsync();
                }
                else
                {
                    await cluster.WaitUntilAsync(async () =>
                    {
                        (DfsStream? s, Exception? e) = await LockHelpers.TryOpenAsync(other, target, LockHelpers.Writer(), ct);
                        if (s is null)
                            return false;
                        await s.DisposeAsync();
                        return true;
                    }, session * 3, $"{target}: the ended transaction's ({end}) reservation must be released by the server", ct);
                }
            }

            for (int i = 0; i < targets.Length; i++)
                LockHelpers.BytesEqual(baseline[i], await LockHelpers.ReadCommittedAsync(other, targets[i], ct), $"{targets[i]}: nothing of the ended transaction is visible");

            // A newer holder takes inode a.
            await using DfsStream newer = await other.Files.OpenAsync(targets[0], LockHelpers.Writer(), ct);
            ulong gNewer = newer.WriterGeneration;
            await LockHelpers.StageReplaceAsync(newer, dataB, ct);

            // Late cleanup and replays of the old transaction's requests.
            if (end != TransactionEnd.ClientKilled)
            {
                await LockHelpers.CatchAsync(() => tx.AbortAsync(ct));
                await LockHelpers.CatchAsync(() => tx.DisposeAsync().AsTask());
                foreach (CapturedMessage message in captured)
                    await LockHelpers.CatchAsync(() => cluster.Network.ReplayAsync(message));
                foreach (CapturedMessage message in Enumerable.Reverse(captured))
                    await LockHelpers.CatchAsync(() => cluster.Network.ReplayAsync(message));
            }

            await cluster.DelayAsync(lease, ct);

            await LockHelpers.AssertAtMostOneActiveWriterOnEveryReplicaAsync(cluster, inodeA, "after late cleanup");
            Assert.Equal(gNewer, Assert.Single(await LockHelpers.ActiveWritersAsync(cluster, inodeA)).Generation);
            (DfsStream? steal, Exception? stealError) = await LockHelpers.TryOpenAsync(third, targets[0], LockHelpers.Writer(), ct);
            if (steal is not null)
            {
                await steal.DisposeAsync();
                Assert.Fail("late cleanup of the old transaction released the newer holder's reservation");
            }

            LockHelpers.AssertSharingConflict(stealError, "the newer holder still owns a");
            CommitToken tNewer = await newer.CommitAsync(ct);
            Assert.Equal(tNewer.Sequence, (await LockHelpers.FileAsync(cluster, targets[0])).Revision);
            LockHelpers.BytesEqual(dataB, await LockHelpers.ReadCommittedAsync(third, targets[0], ct), "the newer holder publishes normally");

            for (int i = 1; i < targets.Length; i++)
            {
                (DfsStream? s, Exception? e) = await LockHelpers.TryOpenAsync(third, targets[i], LockHelpers.Writer(), ct);
                if (s is null)
                {
                    Assert.Fail($"{targets[i]}: a replayed request of the ended transaction re-reserved the inode: {LockHelpers.Describe(e)}");
                    return;
                }

                await s.DisposeAsync();
                LockHelpers.BytesEqual(baseline[i], await LockHelpers.ReadCommittedAsync(third, targets[i], ct), $"{targets[i]}: unchanged");
            }

            if (crashedLeader is not null)
            {
                await crashedLeader.RestartAsync();
                await cluster.WaitForReplicationAsync(Vol, ct);
                await cluster.Inspect.AssertReplicasConvergedAsync(Vol);
            }

            if (end != TransactionEnd.ClientKilled)
                await txClient.DisposeAsync();
        }, ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // LOCK-020
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// LOCK-020: the late-sync backup role never holds live locks. A writer keeps publishing while a backup capture and
    /// transfer run; the backup appears in no voter/learner set and owns no session or handle; lock/lease requests
    /// presented with the backup's identity are denied (acquisition of read or write handles, renewal of another
    /// writer's handle — which still expires on time); the backup host's own SDK surface rejects writable opens with
    /// DfsReadOnlyException, and its snapshot readers do not block an exclusive live writer.
    /// </summary>
    [Fact]
    [Catalog("LOCK-020", "S", "INV-02", "INV-05", "INV-06", "INV-12")]
    public async Task LOCK_020_BackupCannotAcquireLiveLocks()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await ClusterTest.RunAsync(LockHelpers.Sim() with { BackupNodeCount = 1 }, async cluster =>
        {
            TimeSpan lease = LockHelpers.Lease(cluster);
            const string path = "/lock020/f";
            byte[] initial = TestData.Bytes(20_001, 6000);
            byte[] dataA = TestData.Bytes(20_002, 7000);
            byte[] dataC = TestData.Bytes(20_003, 1000);
            TestBackupNode backup = cluster.BackupNodes[0];

            await using DfsClient setupClient = await LockHelpers.ConnectAsync(cluster, "setup", ct);
            DfsVolume setup = setupClient.GetVolume(Vol);
            await setup.Directories.CreateDirectoryAsync("/lock020", ct);
            await setup.Files.WriteAllBytesAsync(path, initial, ct);
            InodeId inode = (await setup.Files.GetInfoAsync(path, ct)).InodeId;

            var clockA = new TestClock(cluster.Time);
            DfsClient clientA = await LockHelpers.ConnectAsync(cluster, "a", ct, clock: clockA);
            DfsStream a = await clientA.GetVolume(Vol).Files.OpenAsync(path, LockHelpers.Writer(), ct);
            ulong gA = a.WriterGeneration;
            HandleInspection handleA = Assert.Single(await LockHelpers.ActiveWritersAsync(cluster, inode));
            await LockHelpers.StageReplaceAsync(a, dataA, ct);

            // Backup capture/transfer runs while A holds the writer reservation; A's publication is not blocked.
            await backup.RegisterAsync(Vol);
            await setup.Snapshots.CreateAsync("lock020-s1", null, ct);
            await setup.Backups.RequestSyncAsync(backup.TargetName, "lock020-s1", ct);
            CommitToken tA = await a.CommitAsync(ct);
            DfsBackupCheckpoint checkpoint = await setup.Backups.AwaitBackupSnapshotAsync(backup.TargetName, "lock020-s1", TimeSpan.FromSeconds(60), ct);
            Assert.Equal("lock020-s1", checkpoint.SnapshotName);
            Assert.Equal(tA.Sequence, (await LockHelpers.FileAsync(cluster, path)).Revision);

            async Task AssertBackupHoldsNothingAsync(string when)
            {
                GroupInspection control = await cluster.Inspect.GetControlGroupAsync();
                GroupInspection volume = await cluster.Inspect.GetVolumeGroupAsync(Vol);
                foreach (GroupInspection group in new[] { control, volume })
                {
                    Assert.DoesNotContain(backup.Name, group.Voters);
                    Assert.DoesNotContain(backup.Name, group.Learners);
                }

                VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Vol);
                var principalBySession = state.Sessions.ToDictionary(s => s.Id, s => s.Principal);
                foreach (HandleInspection h in state.Handles.Where(h => !h.Expired))
                {
                    Assert.True(principalBySession.TryGetValue(h.Session, out string? principal) && principal == cluster.AdminCredentials.Principal,
                        $"{when}: handle {h.Id.Value} belongs to a session that is not one of the live test clients");
                }

                Assert.DoesNotContain(state.Sessions, s => s.Principal == backup.ExportCredentials.Principal || s.Principal == backup.ReaderCredentials.Principal);
                Assert.True(state.Handles.Count(h => h.Inode == inode && !h.Expired) <= 1, $"{when}: only the live writer may hold the inode");
            }

            await AssertBackupHoldsNothingAsync("after the transfer");

            // Lock endpoints driven with the backup's identity: no acquisition, no renewal of A's handle.
            PausedClient pausedA = PausedClient.Pause(cluster, "a", clockA);
            long pausedAt = cluster.Time.GetTimestamp();
            TestNode leader = await cluster.GetLeaderAsync(Vol, ct);
            await using RawProtocolClient asBackup = await cluster.ConnectRawAsBackupAsync(0, leader.Name, ct);
            foreach (FileAccess access in new[] { FileAccess.Read, FileAccess.Write, FileAccess.ReadWrite })
            {
                RawOpenHandleResult result = await asBackup.OpenHandleAsync(Vol, path, FileMode.Open, access, FileShare.ReadWrite | FileShare.Delete);
                Assert.False(result.Granted, $"the backup role acquired a live {access} handle");
                Assert.NotEqual(DfsErrorCode.None, result.Code);
            }

            RawOpenHandleResult create = await asBackup.OpenHandleAsync(Vol, "/lock020/created-by-backup", FileMode.CreateNew, FileAccess.Write, FileShare.None);
            Assert.False(create.Granted);
            Assert.False(await setup.Files.ExistsAsync("/lock020/created-by-backup", ct));

            bool expired = false;
            while (!expired && cluster.Time.GetElapsedTime(pausedAt) < lease * 4)
            {
                DfsErrorCode renewal = await asBackup.KeepAliveAsync(Vol, [handleA.Id]);
                Assert.NotEqual(DfsErrorCode.None, renewal);
                await cluster.DelayAsync(lease / 4, ct);
                expired = !(await LockHelpers.ActiveWritersAsync(cluster, inode)).Any(h => h.Generation == gA);
            }

            Assert.True(expired, "A's lease must expire on schedule although the backup role kept 'renewing' it");

            // The backup host's SDK surface: writable opens rejected, snapshot readers take only local pins.
            await using DfsClient backupReader = await cluster.ConnectBackupAsync(0, new TestClientOptions { Name = "bk", Credentials = backup.ReaderCredentials }, ct);
            DfsVolume bv = backupReader.GetVolume(Vol);
            Exception? writable = await LockHelpers.CatchAsync(async () =>
            {
                DfsStream s = await bv.Files.OpenAsync(path, new DfsOpenOptions { Mode = FileMode.OpenOrCreate, Access = FileAccess.Write, Share = FileShare.None }, ct);
                await s.DisposeAsync();
            });
            Assert.True(writable is DfsReadOnlyException { ErrorCode: DfsErrorCode.ReadOnly }, $"backup host writable open: {LockHelpers.Describe(writable)}");
            Exception? helper = await LockHelpers.CatchAsync(() => bv.Files.WriteAllBytesAsync(path, dataC, ct));
            Assert.True(helper is DfsReadOnlyException, $"backup host WriteAllBytes: {LockHelpers.Describe(helper)}");

            await using DfsSnapshotView view = await bv.Snapshots.OpenAsync("lock020-s1", ct);
            await using DfsStream snapshotReader = await view.Files.OpenReadAsync(path, ct);
            LockHelpers.BytesEqual(initial, await LockHelpers.ReadToEndAsync(snapshotReader, ct), "the backup serves the exact snapshot (taken before A's commit)");

            await using DfsClient clientC = await LockHelpers.ConnectAsync(cluster, "c", ct);
            DfsVolume vc = clientC.GetVolume(Vol);
            await using (DfsStream exclusive = await vc.Files.OpenAsync(path, LockHelpers.Writer(share: FileShare.None), ct))
            {
                Assert.True(exclusive.WriterGeneration > gA);
                await LockHelpers.ReplaceAndCommitAsync(exclusive, dataC, ct);
                await AssertBackupHoldsNothingAsync("while a live FileShare.None writer coexists with a backup snapshot reader");
            }

            pausedA.Resume();
            LockHelpers.AssertLeaseLost(await LockHelpers.WithinAsync(cluster, LockHelpers.CatchAsync(() => a.CommitAsync(ct)), lease * 10, "A after resuming", ct),
                "A's lease was not extended by the backup's renewal attempts");
            LockHelpers.BytesEqual(dataC, await LockHelpers.ReadCommittedAsync(vc, path, ct), "live contents are the live writer's");
            await LockHelpers.CatchAsync(() => a.DisposeAsync().AsTask());
            await clientA.DisposeAsync();
        }, ct);
    }

    /// <summary>A writer reservation acquired through one of the <see cref="WriterAcquisition"/> paths.</summary>
    private sealed class WriterHolder : IAsyncDisposable
    {
        private WriterHolder(WriterAcquisition kind, DfsStream stream, DfsTransaction? transaction)
        {
            Kind = kind;
            Stream = stream;
            Transaction = transaction;
        }

        public WriterAcquisition Kind { get; }

        public DfsStream Stream { get; }

        public DfsTransaction? Transaction { get; }

        public ulong Generation => Stream.WriterGeneration;

        public static async Task<(WriterHolder? Holder, Exception? Error)> AcquireAsync(DfsVolume volume, WriterAcquisition kind, string path, CancellationToken ct)
        {
            DfsTransaction? tx = null;
            DfsStream? stream = null;
            try
            {
                switch (kind)
                {
                    case WriterAcquisition.OpenWrite:
                        stream = await volume.Files.OpenAsync(path, FileMode.Open, FileAccess.Write, FileShare.Read, ct);
                        break;
                    case WriterAcquisition.OpenReadWrite:
                        stream = await volume.Files.OpenAsync(path, FileMode.Open, FileAccess.ReadWrite, FileShare.Read, ct);
                        break;
                    case WriterAcquisition.OpenOrCreateWrite:
                        stream = await volume.Files.OpenAsync(path, FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read, ct);
                        break;
                    case WriterAcquisition.AppendMode:
                        stream = await volume.Files.OpenAsync(path, FileMode.Append, FileAccess.Write, FileShare.Read, ct);
                        break;
                    case WriterAcquisition.TruncateMode:
                        stream = await volume.Files.OpenAsync(path, FileMode.Truncate, FileAccess.Write, FileShare.Read, ct);
                        break;
                    case WriterAcquisition.CreateMode:
                        stream = await volume.Files.OpenAsync(path, FileMode.Create, FileAccess.ReadWrite, FileShare.Read, ct);
                        break;
                    case WriterAcquisition.OpenWriteHelper:
                        stream = await volume.Files.OpenWriteAsync(path, ct);
                        break;
                    case WriterAcquisition.TransactionWriter:
                        tx = await volume.Transactions.BeginAsync(null, ct);
                        stream = await tx.Files.OpenAsync(path, FileMode.Open, FileAccess.Write, FileShare.Read, ct);
                        await stream.WriteAsync(new byte[] { 0x5A }, ct);
                        await stream.FlushAsync(ct); // stages durably: requires the transaction's writer reservation
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(kind), kind, null);
                }

                return (new WriterHolder(kind, stream, tx), null);
            }
            catch (Exception ex)
            {
                if (stream is not null)
                    await LockHelpers.CatchAsync(() => stream.DisposeAsync().AsTask());
                if (tx is not null)
                    await LockHelpers.CatchAsync(() => tx.DisposeAsync().AsTask());
                return (null, ex);
            }
        }

        public async Task PublishAsync(byte[] data, CancellationToken ct)
        {
            if (Kind == WriterAcquisition.AppendMode)
                await Stream.WriteAsync(data, ct);
            else
                await LockHelpers.StageReplaceAsync(Stream, data, ct);

            if (Transaction is null)
            {
                await Stream.CommitAsync(ct);
            }
            else
            {
                await Stream.DisposeAsync();
                await Transaction.CommitAsync(ct);
            }
        }

        public async ValueTask DisposeAsync()
        {
            await Stream.DisposeAsync();
            if (Transaction is not null)
                await Transaction.DisposeAsync();
        }
    }
}
