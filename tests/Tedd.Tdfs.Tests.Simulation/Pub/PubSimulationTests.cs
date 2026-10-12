using Tedd.Tdfs.Server;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;
using static Tedd.Tdfs.Tests.Simulation.Pub.PubHelpers;

namespace Tedd.Tdfs.Tests.Simulation.Pub;

/// <summary>Publication API used by PUB-003.</summary>
public enum FlushApi
{
    /// <summary><see cref="Stream.FlushAsync(CancellationToken)"/>.</summary>
    FlushAsync,
    /// <summary><see cref="DfsStream.CommitAsync(CancellationToken)"/>.</summary>
    CommitAsync,
    /// <summary><see cref="Stream.Flush()"/> (realtime mode).</summary>
    FlushSync,
}

/// <summary>Shape of the multi-extent change published by PUB-003.</summary>
public enum EditShape
{
    /// <summary>Three distant edits (first/interior/last region) plus a length extension.</summary>
    DistantEditsAndExtend,
    /// <summary>Three distant edits plus a truncation that clips the last edit.</summary>
    DistantEditsAndTruncate,
    /// <summary>Two dozen small edits spread over the whole file.</summary>
    ManySmallExtents,
}

/// <summary>Data policy for PUB-004.</summary>
public enum DurabilityPolicyKind
{
    /// <summary>Target 3 / ack 2 copies in 2 zones (two held nodes leave one copy).</summary>
    AckTwoOfThree,
    /// <summary>Target 3 / ack 3 copies in 3 zones (one held node blocks success).</summary>
    AckThreeOfThree,
}

/// <summary>How PUB-005 disconnects the late-sync host.</summary>
public enum BackupDisconnect
{
    /// <summary>Network isolation of the backup node.</summary>
    Isolated,
    /// <summary>The backup process is paused.</summary>
    Paused,
    /// <summary>The backup process crashed.</summary>
    Crashed,
}

/// <summary>Live publication in PUB-005.</summary>
public enum LivePublish
{
    /// <summary>OnFlush staging + FlushAsync.</summary>
    Flush,
    /// <summary>OnFlush staging + CommitAsync.</summary>
    Commit,
    /// <summary>WriteThrough write.</summary>
    WriteThrough,
}

/// <summary>How PUB-008 observes each publication boundary.</summary>
public enum BoundaryObservation
{
    /// <summary>Flush, then an ordinary snapshot.</summary>
    FlushThenSnapshot,
    /// <summary>Commit-and-snapshot in the same metadata decision (exact recovery point).</summary>
    CommitAndSnapshotExact,
    /// <summary>Commit, then a snapshot with MinimumCommit = the returned token.</summary>
    CommitThenMinimumCommitSnapshot,
}

/// <summary>The second staged change in PUB-008.</summary>
public enum SecondEdit
{
    /// <summary>Overwrites inside A's range.</summary>
    SameExtent,
    /// <summary>Changes a distant extent.</summary>
    DistantExtent,
    /// <summary>Writes then truncates.</summary>
    Truncate,
    /// <summary>Writes past EOF and extends.</summary>
    Extend,
}

/// <summary>Length change exercised by PUB-009.</summary>
public enum LengthChange
{
    /// <summary>Shrink below the current position.</summary>
    Shrink,
    /// <summary>Extend (zero-filled).</summary>
    Extend,
    /// <summary>Truncate to zero.</summary>
    Zero,
    /// <summary>Shrink then extend: discarded bytes must not reappear.</summary>
    ShrinkThenExtend,
}

/// <summary>Staging step at which PUB-012 crashes.</summary>
public enum StagingCrashPoint
{
    /// <summary>Client process dies while uploading its first chunk (F03).</summary>
    ClientFirstChunkUpload,
    /// <summary>Client process dies while uploading an interior chunk (F03).</summary>
    ClientThirdChunkUpload,
    /// <summary>Client process dies while sending the commit request (F03).</summary>
    ClientCommitRequestSend,
    /// <summary>Storage node crashes after accepting an object upload (F04).</summary>
    StorageRequestAccepted,
    /// <summary>Metadata leader crashes after registering the staging session (F05).</summary>
    StagingRegistered,
    /// <summary>Storage node crashes during an object write, tearing it (F06).</summary>
    StorageObjectWriteTorn,
    /// <summary>Storage node crashes before the object's durability primitive, losing unflushed data (F07).</summary>
    StorageBeforeFlush,
    /// <summary>As <see cref="StorageBeforeFlush"/> but a random subset of unflushed writes survives (F07).</summary>
    StorageBeforeFlushRandomSubset,
    /// <summary>Storage node crashes after durable persistence, before its receipt is returned (F08).</summary>
    StorageAfterFlushBeforeReceipt,
    /// <summary>Metadata leader crashes after validating receipts (F09).</summary>
    LeaderReceiptsValidated,
    /// <summary>Metadata leader crashes immediately before proposing the commit (F10).</summary>
    LeaderBeforePropose,
}

/// <summary>Which nodes PUB-013 crashes after the durable decision.</summary>
public enum CommitCrashScope
{
    /// <summary>Only the metadata leader.</summary>
    LeaderOnly,
    /// <summary>Every live node.</summary>
    AllNodes,
}

/// <summary>Cancellation point of PUB-014 (all before any publish request is transmitted).</summary>
public enum CancelPoint
{
    /// <summary>The token is already cancelled when the call starts.</summary>
    TokenAlreadyCancelled,
    /// <summary>Cancelled at the client operation start site (F00).</summary>
    AtOperationStart,
    /// <summary>Cancelled after local validation/operation-id allocation (F01).</summary>
    AfterValidation,
    /// <summary>Cancelled while staged data is being uploaded, before the commit request (F03, chunk 2).</summary>
    DuringStagingUpload,
}

/// <summary>Server-side situation in PUB-015 when the client cancels after transmission.</summary>
public enum UncertainPoint
{
    /// <summary>Committed and applied; the leader holds the reply (F12).</summary>
    CommittedReplyWithheld,
    /// <summary>Committed; every reply is dropped on the wire (F13).</summary>
    CommittedReplyDropped,
    /// <summary>The leader accepted the request but has not processed it yet (F04).</summary>
    AcceptedBeforeProposal,
    /// <summary>The leader holds the request before proposing and then crashes (F10): it never commits.</summary>
    ProposalNeverMade,
}

/// <summary>Persistence failure during disposal in PUB-017.</summary>
public enum DisposeFailure
{
    /// <summary>Every storage node fails the object durability primitive.</summary>
    StorageFlushFails,
    /// <summary>Two of three storage nodes are full.</summary>
    DisksFull,
    /// <summary>The commit is applied but its reply is withheld and the client is cut off.</summary>
    CommitReplyWithheld,
    /// <summary>The metadata group loses its majority before disposal.</summary>
    QuorumLost,
}

/// <summary>How PUB-021 makes the writer lose its lease.</summary>
public enum LeaseLossCause
{
    /// <summary>The writer client is network-isolated (renewals cannot reach the leader).</summary>
    ClientIsolated,
    /// <summary>The writer client's clock is frozen (its renewal timer stops), process memory retained.</summary>
    ClientClockFrozen,
}

/// <summary>What happens to the inode after the old writer's lease expired (PUB-021).</summary>
public enum Replacement
{
    /// <summary>Nobody else opens the file.</summary>
    None,
    /// <summary>A new writer opens it but publishes nothing.</summary>
    OpenedOnly,
    /// <summary>A new writer opens it and commits a replacement revision.</summary>
    Committed,
}

/// <summary>The prerequisite PUB-023 withholds.</summary>
public enum Withheld
{
    /// <summary>A majority of the five metadata voters is unreachable (data placement still possible).</summary>
    MetadataQuorum,
    /// <summary>One of the three zones required by an ack-3/3 policy is unreachable (quorum still available).</summary>
    RequiredZoneForStrongPolicy,
    /// <summary>Two of three nodes never complete the object durability primitive (ack-2 policy).</summary>
    RequiredDiskDurability,
    /// <summary>A node required by an ack-3/3 policy is out of space.</summary>
    RequiredDiskFull,
}

/// <summary>
/// PUB — buffering, flush, commit and disposal (test plan §7.6; FSD §§6.3–6.4, 10.3, 13). Simulation-level rows run in
/// deterministic mode (in-memory transport, simulated storage, virtual time) unless a variant drives synchronous
/// network-bound SDK members, which run in realtime mode over the same in-memory stack.
/// </summary>
public sealed class PubSimulationTests
{
    // ------------------------------------------------------------------------------------------------------------
    // PUB-002
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>(client buffer, staged bytes, scattered writes).</summary>
    public static TheoryData<int, int, bool> Pub002Cases => new()
    {
        { 4 * 1024, 4 * 1024 + 1, false },          // buffer + 1
        { 64 * 1024, 64 * 1024 + 1, false },        // default buffer + 1
        { 64 * 1024, (1 << 20) + 1, false },        // more than one maximum-size object
        { 16 * 1024, 300_000, true },               // many scattered writes, several uploads
    };

    [Theory]
    [MemberData(nameof(Pub002Cases))]
    [Catalog("PUB-002", "S", "INV-02", "INV-09", "INV-16")]
    public Task PUB_002_AutoStagingDoesNotPublish(int bufferSize, int stagedBytes, bool scattered) =>
        ClusterTest.RunAsync(Deterministic(), async cluster =>
        {
            const string path = "/pub002/f";
            byte[] initial = TestData.Bytes(0x0002_0001, 100_000);
            await using DfsClient writerClient = await ConnectAsync(cluster, "c0", bufferSize);
            await using DfsClient readerClient = await ConnectAsync(cluster, "c1");
            DfsVolume wv = writerClient.GetVolume(Vol);
            DfsVolume rv = readerClient.GetVolume(Vol);
            await SeedFileAsync(wv, path, initial);
            DfsFileInfo before = await rv.Files.GetInfoAsync(path, Ct);
            IReadOnlySet<ObjectId> storedBefore = await cluster.Inspect.GetAllStoredObjectsAsync(Vol);
            long putsBefore = PutObjectsFrom(cluster, "c0");
            long publishBefore = PublishRequests(cluster, "c0");

            await using DfsStream liveReader = await rv.Files.OpenAsync(path, LiveReaderOptions(), Ct);
            await using DfsStream writer = await wv.Files.OpenAsync(path, WriterOptions(FileMode.Open, bufferSize: bufferSize), Ct);
            var model = new ReferenceFileModel(initial);
            if (!scattered)
            {
                byte[] data = TestData.Bytes(0x0002_0002, stagedBytes);
                await WriteAtAsync(writer, 0, data);
                model.Write(0, data);
            }
            else
            {
                int written = 0;
                for (int i = 0; written < stagedBytes; i++)
                {
                    int length = Math.Min(10_000, stagedBytes - written);
                    long offset = i * 37_123L % 400_000;
                    byte[] piece = TestData.Bytes(0x0002_0100 + (ulong)i, length);
                    await WriteAtAsync(writer, offset, piece);
                    model.Write(offset, piece);
                    written += length;
                }
            }

            // Precondition: the buffer overflowed, so staged data was uploaded early.
            Assert.True(PutObjectsFrom(cluster, "c0") > putsBefore, "exceeding the client buffer must upload staged data before publication");
            HashSet<ObjectId> uploaded = await NewObjectsSinceAsync(cluster, storedBefore);
            Assert.NotEmpty(uploaded);
            Assert.True(writer.PendingBytes > bufferSize, $"PendingBytes {writer.PendingBytes} must count uploaded-but-unpublished bytes");
            Assert.Equal(publishBefore, PublishRequests(cluster, "c0"));

            // Remote readers and every replica still see only the committed revision.
            await AssertCommittedAsync(rv, path, initial, "remote readers must exclude auto-staged bytes", before.Revision);
            await AssertBytesAsync(initial, ReadFromStartAsync(liveReader), "a live reader handle must exclude auto-staged bytes");
            await AssertReplicasAtRevisionAsync(cluster, path, before.Revision, initial.LongLength, "auto-staging is not publication");
            await AssertUnreachableAsync(cluster, uploaded, "auto-staged objects must not be referenced before flush");

            // Snapshots exclude them too.
            DfsSnapshotInfo mid = await rv.Snapshots.CreateAsync("pub002-mid", null, Ct);
            await using (DfsSnapshotView view = await rv.Snapshots.OpenAsync(mid.Name, Ct))
                await AssertBytesAsync(initial, view.Files.ReadAllBytesAsync(path, Ct), "a snapshot must exclude uncommitted auto-staged bytes");
            await AssertUnreachableAsync(cluster, uploaded, "a snapshot must not capture auto-staged objects");

            // The writer reads its own staging, and flush publishes it.
            await AssertBytesAsync(model.Staged, ReadFromStartAsync(writer), "the writer must read its own staged data");
            await writer.FlushAsync(Ct);
            model.Flush();
            DfsFileInfo after = await AssertCommittedAsync(rv, path, model.Committed, "flush publishes the staged revision");
            Assert.True(after.Revision > before.Revision);
            await using (DfsSnapshotView view = await rv.Snapshots.OpenAsync(mid.Name, Ct))
                await AssertBytesAsync(initial, view.Files.ReadAllBytesAsync(path, Ct), "the snapshot taken before flush is immutable");
        }, Ct);

    // ------------------------------------------------------------------------------------------------------------
    // PUB-003
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>(publication API, change shape).</summary>
    public static TheoryData<FlushApi, EditShape> Pub003Cases => new()
    {
        { FlushApi.FlushAsync, EditShape.DistantEditsAndExtend },
        { FlushApi.CommitAsync, EditShape.DistantEditsAndTruncate },
        { FlushApi.FlushAsync, EditShape.ManySmallExtents },
        { FlushApi.CommitAsync, EditShape.ManySmallExtents },
        { FlushApi.FlushSync, EditShape.DistantEditsAndExtend },
    };

    [Theory]
    [MemberData(nameof(Pub003Cases))]
    [Catalog("PUB-003", "S", "INV-02", "INV-03")]
    public Task PUB_003_FlushPublishesWholeFileRevision(FlushApi api, EditShape shape) =>
        ClusterTest.RunAsync(SimOrRealtime(api == FlushApi.FlushSync), async cluster =>
        {
            const string path = "/pub003/f";
            byte[] initial = TestData.Bytes(0x0003_0001, 1_200_000);
            await using DfsClient writerClient = await ConnectAsync(cluster, "c0", LargeBuffer);
            await using DfsClient readerClient = await ConnectAsync(cluster, "c1");
            await using DfsClient readerClient2 = await ConnectAsync(cluster, "c2");
            DfsVolume wv = writerClient.GetVolume(Vol);
            DfsVolume rv = readerClient.GetVolume(Vol);
            DfsVolume rv2 = readerClient2.GetVolume(Vol);
            await SeedFileAsync(wv, path, initial);
            DfsFileInfo before = await rv.Files.GetInfoAsync(path, Ct);

            await using DfsStream pinned = await rv.Files.OpenAsync(path, PinnedReaderOptions(), Ct);
            await using DfsStream writer = await wv.Files.OpenAsync(path, WriterOptions(FileMode.Open, bufferSize: LargeBuffer), Ct);
            var model = new ReferenceFileModel(initial);
            switch (shape)
            {
                case EditShape.DistantEditsAndExtend:
                case EditShape.DistantEditsAndTruncate:
                    foreach ((long offset, int length, ulong seed) in new (long, int, ulong)[] { (0, 100, 1), (600_017, 5_000, 2), (1_150_000, 70_000, 3) })
                    {
                        byte[] edit = TestData.Bytes(0x0003_0100 + seed, length);
                        await WriteAtAsync(writer, offset, edit);
                        model.Write(offset, edit);
                    }
                    long newLength = shape == EditShape.DistantEditsAndExtend ? 1_300_003 : 1_000_000;
                    await writer.SetLengthAsync(newLength, Ct);
                    model.SetLength(newLength);
                    break;
                case EditShape.ManySmallExtents:
                    for (int i = 0; i < 24; i++)
                    {
                        byte[] edit = TestData.Bytes(0x0003_0200 + (ulong)i, 777);
                        long offset = i * 50_000L + 13;
                        await WriteAtAsync(writer, offset, edit);
                        model.Write(offset, edit);
                    }
                    break;
            }

            byte[] expected = model.Staged;
            long writeId = cluster.History.Invoke("c0", "write", path, expected);
            Task<CommitToken> publication = api switch
            {
                FlushApi.FlushAsync => FlushWithoutTokenAsync(writer),
                FlushApi.CommitAsync => writer.CommitAsync(Ct),
                // Realtime mode only: the synchronous member runs on a pool thread so readers can interleave.
                FlushApi.FlushSync => Task.Run(() =>
                {
                    writer.Flush();
                    return CommitToken.None;
                }, Ct),
                _ => throw new ArgumentOutOfRangeException(nameof(api)),
            };
            Task readers = Task.WhenAll(
                ReadUntilPublishedAsync(cluster, rv, path, publication, "c1"),
                ReadUntilPublishedAsync(cluster, rv2, path, publication, "c2"));
            CommitToken token;
            try
            {
                token = await publication;
                cluster.History.Complete(writeId, OpOutcome.Ok);
            }
            catch (Exception ex)
            {
                cluster.History.Complete(writeId, HistoryRecorder.Classify(ex), ex);
                throw;
            }
            await readers;

            // Every concurrent observation is one complete revision: all changes together or none.
            foreach (HistoryOp op in cluster.History.Operations.Where(o => o.Kind == "read" && o.Outcome == OpOutcome.Ok))
            {
                byte[] seen = Assert.IsType<byte[]>(op.Output);
                Assert.True(seen.AsSpan().SequenceEqual(initial) || seen.AsSpan().SequenceEqual(expected),
                    $"Read #{op.Id} by {op.Process} observed {seen.Length} bytes that are neither the old nor the new complete revision.");
            }
            LinearizabilityChecker.Check(cluster.History.Operations, new RegisterModel(initial)).AssertValid();

            // One publication boundary.
            DfsFileInfo after = await AssertCommittedAsync(rv, path, expected, "flush publishes every staged change in one revision");
            Assert.True(after.Revision > before.Revision);
            if (!token.IsNone)
                Assert.Equal(token.Sequence, after.Revision);
            await cluster.WaitForReplicationAsync(Vol, Ct);
            await AssertReplicasAtRevisionAsync(cluster, path, after.Revision, expected.LongLength, "every replica applies the whole revision at once");

            // A version-pinned reader opened before the flush keeps the whole old revision.
            await AssertBytesAsync(initial, ReadFromStartAsync(pinned), "a pinned reader must keep its complete revision");
        }, Ct);

    private static async Task<CommitToken> FlushWithoutTokenAsync(DfsStream writer)
    {
        await writer.FlushAsync(Ct);
        return CommitToken.None;
    }

    private static async Task ReadUntilPublishedAsync(TestCluster cluster, DfsVolume volume, string path, Task publication, string process)
    {
        for (int i = 0; i < 10_000; i++)
        {
            bool last = publication.IsCompleted;
            await cluster.History.RecordAsync(process, "read", path, null, () => volume.Files.ReadAllBytesAsync(path, Ct));
            if (last)
                return;
        }
    }

    // ------------------------------------------------------------------------------------------------------------
    // PUB-004
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>(held storage site, policy).</summary>
    public static TheoryData<string, DurabilityPolicyKind> Pub004Cases => new()
    {
        { FaultSites.ObjectBeforeFlush, DurabilityPolicyKind.AckTwoOfThree },
        { FaultSites.ObjectAfterFlushBeforeReceipt, DurabilityPolicyKind.AckTwoOfThree },
        { FaultSites.ObjectWrite, DurabilityPolicyKind.AckTwoOfThree },
        { FaultSites.ObjectBeforeFlush, DurabilityPolicyKind.AckThreeOfThree },
        { FaultSites.ObjectAfterFlushBeforeReceipt, DurabilityPolicyKind.AckThreeOfThree },
    };

    [Theory]
    [MemberData(nameof(Pub004Cases))]
    [Catalog("PUB-004", "S", "INV-01", "INV-08", "INV-11")]
    public Task PUB_004_FlushWaitsForRequiredDurability(string heldSite, DurabilityPolicyKind policyKind)
    {
        TestClusterOptions options = policyKind == DurabilityPolicyKind.AckThreeOfThree
            ? Deterministic(TestClusterOptions.ThreeZones with { DefaultPolicy = StrongPolicy })
            : Deterministic();
        DfsStoragePolicy policy = options.GetEffectiveDefaultPolicy();
        return ClusterTest.RunAsync(options, async cluster =>
        {
            const string path = "/pub004/f";
            byte[] initial = TestData.Bytes(0x0004_0001, 100_000);
            byte[] data = TestData.Bytes(0x0004_0002, 300_000);
            await using DfsClient writerClient = await ConnectAsync(cluster, "c0", LargeBuffer);
            await using DfsClient readerClient = await ConnectAsync(cluster, "c1");
            DfsVolume wv = writerClient.GetVolume(Vol);
            DfsVolume rv = readerClient.GetVolume(Vol);
            await SeedFileAsync(wv, path, initial);
            DfsFileInfo before = await rv.Files.GetInfoAsync(path, Ct);

            await using DfsStream writer = await wv.Files.OpenAsync(path, WriterOptions(FileMode.Open, bufferSize: LargeBuffer), Ct);
            var model = new ReferenceFileModel(initial);
            await WriteAtAsync(writer, 0, data);
            model.Write(0, data);
            model.Flush();

            // Hold the disk acknowledgement on enough nodes that the policy cannot be met.
            string[] held = policyKind == DurabilityPolicyKind.AckTwoOfThree ? ["n1", "n2"] : ["n2"];
            string releasedFirst = held[0];
            long publishBefore = PublishRequests(cluster, "c0");
            Dictionary<string, FaultArm> arms = held.ToDictionary(n => n, n => cluster.Faults.Arm(
                new FaultMatch(heldSite, Node: n, Operation: DfsOperationKind.PutObject, Count: 100_000), FaultAction.Block()));

            Task flush = writer.FlushAsync(Ct);
            foreach (FaultArm arm in arms.Values)
            {
                await arm.WaitTriggeredAsync(TimeSpan.FromSeconds(30));
                cluster.Faults.AssertReached(arm);
            }

            await AssertStillPendingAsync(cluster, flush, TimeSpan.FromSeconds(3),
                "FlushAsync must not succeed before the required durable acknowledgements exist");
            foreach (string node in held)
                Assert.True(PutObjectsFrom(cluster, "c0", node) > 0, $"precondition: the upload to {node} was delivered over the network");
            Assert.Equal(publishBefore, PublishRequests(cluster, "c0"));
            await AssertCommittedAsync(rv, path, initial, "nothing is published while durability is held", before.Revision);
            await AssertReplicasAtRevisionAsync(cluster, path, before.Revision, initial.LongLength, "no replica applies the revision early");

            // Releasing one held node makes the policy satisfiable; the flush then completes.
            arms[releasedFirst].Disarm();
            arms[releasedFirst].Release();
            Exception? error = await CaptureWithinAsync(cluster, flush, TimeSpan.FromSeconds(30), "flush completes once the policy can be met");
            Assert.Null(error);
            await AssertCommittedAsync(rv, path, model.Committed, "the flushed revision is published");
            await AssertDurableAsync(cluster, path, policy, "success was reported only after the policy was met");

            foreach (FaultArm arm in arms.Values)
            {
                arm.Disarm();
                arm.Release();
            }
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // PUB-005
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>(how the backup is disconnected, live publication).</summary>
    public static TheoryData<BackupDisconnect, LivePublish> Pub005Cases => new()
    {
        { BackupDisconnect.Isolated, LivePublish.Flush },
        { BackupDisconnect.Paused, LivePublish.Commit },
        { BackupDisconnect.Crashed, LivePublish.WriteThrough },
        { BackupDisconnect.Isolated, LivePublish.WriteThrough },
        { BackupDisconnect.Crashed, LivePublish.Commit },
    };

    [Theory]
    [MemberData(nameof(Pub005Cases))]
    [Catalog("PUB-005", "S", "INV-01", "INV-15")]
    public Task PUB_005_FlushDoesNotWaitForBackup(BackupDisconnect disconnect, LivePublish publish) =>
        ClusterTest.RunAsync(Deterministic(TestClusterOptions.ThreeZones with { BackupNodeCount = 1 }), async cluster =>
        {
            const string path = "/pub005/f";
            byte[] initial = TestData.Bytes(0x0005_0001, 100_000);
            byte[] data = TestData.Bytes(0x0005_0002, 300_000);
            TestBackupNode backup = cluster.BackupNodes[0];
            string target = backup.TargetName;
            await backup.RegisterAsync(Vol, new DfsBackupSchedule(Interval: TimeSpan.FromSeconds(1)),
                new DfsBackupBudgets(MaxPinAge: TimeSpan.FromMinutes(30), MaxPinnedBytes: 1L << 30));

            await using DfsClient writerClient = await ConnectAsync(cluster, "c0");
            await using DfsClient readerClient = await ConnectAsync(cluster, "c1");
            DfsVolume wv = writerClient.GetVolume(Vol);
            DfsVolume rv = readerClient.GetVolume(Vol);
            CommitToken seeded = await SeedFileAsync(wv, path, initial);

            // Precondition: the late-sync path works while connected.
            DfsBackupCheckpoint first = await wv.Backups.AwaitBackupAsync(target, seeded, TimeSpan.FromSeconds(60), Ct);
            Assert.True(first.CoveredCommit.IsAtOrAfter(seeded));

            switch (disconnect)
            {
                case BackupDisconnect.Isolated:
                    cluster.Network.Isolate(backup.Name);
                    break;
                case BackupDisconnect.Paused:
                    await backup.PauseAsync();
                    break;
                case BackupDisconnect.Crashed:
                    await backup.CrashAsync();
                    break;
            }

            var model = new ReferenceFileModel(initial);
            DfsStream writer = await wv.Files.OpenAsync(path,
                WriterOptions(FileMode.Open, publish == LivePublish.WriteThrough ? DfsWriteMode.WriteThrough : DfsWriteMode.OnFlush), Ct);
            if (publish != LivePublish.WriteThrough)
                await WriteAtAsync(writer, 0, data);
            model.Write(0, data);
            model.Flush();

            async Task<CommitToken> PublishAsync()
            {
                switch (publish)
                {
                    case LivePublish.Commit:
                        return await writer.CommitAsync(Ct);
                    case LivePublish.Flush:
                        await writer.FlushAsync(Ct);
                        break;
                    default:
                        await WriteAtAsync(writer, 0, data);
                        break;
                }
                return (await wv.GetInfoAsync(Ct)).LastCommit;
            }

            Task<CommitToken> publication = PublishAsync();
            Exception? error = await CaptureWithinAsync(cluster, publication, TimeSpan.FromSeconds(15),
                "live publication must not wait for a disconnected late-sync host");
            Assert.Null(error);
            CommitToken token = await publication;
            await writer.DisposeAsync();

            await AssertCommittedAsync(rv, path, model.Committed, "the live commit is visible without the backup");
            await AssertDurableAsync(cluster, path, ThreeZonePolicy, "live durability was met by live nodes");
            var liveNodes = cluster.Nodes.Select(n => n.Name).ToHashSet(StringComparer.Ordinal);
            foreach (ObjectId obj in await ReferencedObjectsAsync(cluster, path))
            {
                IReadOnlyList<string> holders = await cluster.Inspect.GetObjectHoldersAsync(obj);
                Assert.DoesNotContain(backup.Name, holders);
                Assert.All(holders, h => Assert.Contains(h, liveNodes));
            }

            DfsBackupStatus status = await wv.Backups.GetStatusAsync(target, Ct);
            Assert.False(Covers(status.PublishedBoundary, token), $"the disconnected backup cannot have published {token}");

            // An explicit backup barrier times out without rolling back the live commit.
            DfsBackupNotReachedException notReached = await AssertEx.ThrowsDfsAsync<DfsBackupNotReachedException>(
                () => wv.Backups.AwaitBackupAsync(target, token, TimeSpan.FromSeconds(1), Ct));
            Assert.Equal(target, notReached.Target);
            await AssertCommittedAsync(rv, path, model.Committed, "a backup barrier timeout must not roll back the live commit");

            // The backup catches up independently once reconnected.
            switch (disconnect)
            {
                case BackupDisconnect.Isolated:
                    cluster.Network.Heal();
                    break;
                case BackupDisconnect.Paused:
                    await backup.ResumeAsync();
                    break;
                case BackupDisconnect.Crashed:
                    await backup.RestartAsync();
                    break;
            }
            await wv.Backups.RequestSyncAsync(target, null, Ct);
            DfsBackupCheckpoint caughtUp = await wv.Backups.AwaitBackupAsync(target, token, TimeSpan.FromSeconds(90), Ct);
            Assert.True(caughtUp.CoveredCommit.IsAtOrAfter(token));
        }, Ct);

    // ------------------------------------------------------------------------------------------------------------
    // PUB-006
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>Every write overload (synchronous ones run in realtime mode).</summary>
    public static TheoryData<WriteKind> Pub006Cases => new()
    {
        WriteKind.MemoryAsync,
        WriteKind.ArrayAsync,
        WriteKind.ArraySync,
        WriteKind.SpanSync,
        WriteKind.ByteByByte,
    };

    [Theory]
    [MemberData(nameof(Pub006Cases))]
    [Catalog("PUB-006", "S", "INV-01", "INV-02")]
    public Task PUB_006_WriteThroughCommitsEachCall(WriteKind kind) =>
        ClusterTest.RunAsync(SimOrRealtime(IsSynchronous(kind)), async cluster =>
        {
            const string path = "/pub006/f";
            byte[] initial = TestData.Bytes(0x0006_0001, 10_000);
            await using DfsClient writerClient = await ConnectAsync(cluster, "c0");
            await using DfsClient readerClient = await ConnectAsync(cluster, "c1");
            DfsVolume wv = writerClient.GetVolume(Vol);
            DfsVolume rv = readerClient.GetVolume(Vol);
            await SeedFileAsync(wv, path, initial);
            ulong lastRevision = (await rv.Files.GetInfoAsync(path, Ct)).Revision;

            (long Offset, int Length)[] calls = kind == WriteKind.ByteByByte
                ? new (long, int)[] { (0, 1), (1, 1), (4_096, 1), (70_000, 1) }
                : new (long, int)[] { (0, 100), (100, 5_000), (65_000, 2_000), (300_000, 10), (50, 10) };

            var model = new ReferenceFileModel(initial);
            DfsStream writer = await wv.Files.OpenAsync(path, WriterOptions(FileMode.Open, DfsWriteMode.WriteThrough), Ct);
            for (int i = 0; i < calls.Length; i++)
            {
                byte[] data = TestData.Bytes(0x0006_0100 + (ulong)i, calls[i].Length);
                writer.Seek(calls[i].Offset, SeekOrigin.Begin);
                await WriteAsync(writer, data, kind, Ct);
                model.Write(calls[i].Offset, data);
                model.Flush();

                Assert.Equal(0, writer.PendingBytes);
                DfsFileInfo info = await AssertCommittedAsync(rv, path, model.Committed,
                    $"WriteThrough call #{i} must be completely visible to independent live reads when it returns");
                Assert.True(info.Revision > lastRevision, $"call #{i} must publish its own revision");
                lastRevision = info.Revision;
                await AssertDurableAsync(cluster, path, ThreeZonePolicy, $"WriteThrough call #{i} returned");
            }
            await writer.DisposeAsync();

            // Each call met durability: a cold restart from durable images keeps the final state.
            await CrashAndRestartAllAsync(cluster);
            await AssertCommittedAsync(rv, path, model.Committed, "every WriteThrough call was durable when it returned", lastRevision);
        }, Ct);

    // ------------------------------------------------------------------------------------------------------------
    // PUB-008
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>(boundary observation, second staged change).</summary>
    public static TheoryData<BoundaryObservation, SecondEdit> Pub008Cases => new()
    {
        { BoundaryObservation.FlushThenSnapshot, SecondEdit.SameExtent },
        { BoundaryObservation.FlushThenSnapshot, SecondEdit.Extend },
        { BoundaryObservation.CommitAndSnapshotExact, SecondEdit.DistantExtent },
        { BoundaryObservation.CommitAndSnapshotExact, SecondEdit.Truncate },
        { BoundaryObservation.CommitThenMinimumCommitSnapshot, SecondEdit.DistantExtent },
        { BoundaryObservation.CommitThenMinimumCommitSnapshot, SecondEdit.Extend },
    };

    [Theory]
    [MemberData(nameof(Pub008Cases))]
    [Catalog("PUB-008", "S", "INV-01", "INV-02")]
    public Task PUB_008_SequentialFlushesHaveDistinctBoundaries(BoundaryObservation observation, SecondEdit secondEdit) =>
        ClusterTest.RunAsync(Deterministic(), async cluster =>
        {
            const string path = "/pub008/f";
            byte[] initial = TestData.Bytes(0x0008_0001, 200_000);
            await using DfsClient writerClient = await ConnectAsync(cluster, "c0");
            await using DfsClient readerClient = await ConnectAsync(cluster, "c1");
            DfsVolume wv = writerClient.GetVolume(Vol);
            DfsVolume rv = readerClient.GetVolume(Vol);
            await SeedFileAsync(wv, path, initial);
            await using DfsStream writer = await wv.Files.OpenAsync(path, WriterOptions(FileMode.Open), Ct);
            var model = new ReferenceFileModel(initial);

            async Task<(CommitToken Token, DfsSnapshotInfo Snapshot)> PublishAsync(string snapshotName)
            {
                switch (observation)
                {
                    case BoundaryObservation.FlushThenSnapshot:
                        await writer.FlushAsync(Ct);
                        return (CommitToken.None, await rv.Snapshots.CreateAsync(snapshotName, null, Ct));
                    case BoundaryObservation.CommitAndSnapshotExact:
                    {
                        CommitToken t = await writer.CommitAsync(new DfsMutationOptions { CommitSnapshotName = snapshotName }, Ct);
                        DfsSnapshotInfo s = await rv.Snapshots.GetAsync(snapshotName, Ct);
                        Assert.Equal(t, s.Boundary);
                        return (t, s);
                    }
                    default:
                    {
                        CommitToken t = await writer.CommitAsync(Ct);
                        DfsSnapshotInfo s = await rv.Snapshots.CreateAsync(snapshotName, new DfsSnapshotOptions { MinimumCommit = t }, Ct);
                        Assert.True(s.Boundary.IsAtOrAfter(t), "a MinimumCommit snapshot must not precede the token");
                        return (t, s);
                    }
                }
            }

            // A.
            byte[] a = TestData.Bytes(0x0008_0002, 20_000);
            await WriteAtAsync(writer, 10_000, a);
            model.Write(10_000, a);
            (CommitToken tA, DfsSnapshotInfo sA) = await PublishAsync("pub008-A");
            model.Flush();
            byte[] stateA = model.Committed;
            DfsFileInfo infoA = await AssertCommittedAsync(rv, path, stateA, "A is published");
            if (!tA.IsNone)
                Assert.Equal(tA.Sequence, infoA.Revision);

            // An unrelated commit in between, so the two boundaries are not adjacent sequences.
            await rv.Files.WriteAllBytesAsync("/pub008/other", TestData.Bytes(0x0008_0003, 500), Ct);

            // B.
            switch (secondEdit)
            {
                case SecondEdit.SameExtent:
                {
                    byte[] b = TestData.Bytes(0x0008_0004, 5_000);
                    await WriteAtAsync(writer, 12_000, b);
                    model.Write(12_000, b);
                    break;
                }
                case SecondEdit.DistantExtent:
                {
                    byte[] b = TestData.Bytes(0x0008_0004, 70_000);
                    await WriteAtAsync(writer, 120_000, b);
                    model.Write(120_000, b);
                    break;
                }
                case SecondEdit.Truncate:
                {
                    byte[] b = TestData.Bytes(0x0008_0004, 3_000);
                    await WriteAtAsync(writer, 5_000, b);
                    model.Write(5_000, b);
                    await writer.SetLengthAsync(60_000, Ct);
                    model.SetLength(60_000);
                    break;
                }
                case SecondEdit.Extend:
                {
                    byte[] b = TestData.Bytes(0x0008_0004, 30_000);
                    await WriteAtAsync(writer, 190_000, b);
                    model.Write(190_000, b);
                    await writer.SetLengthAsync(260_000, Ct);
                    model.SetLength(260_000);
                    break;
                }
            }

            // While B is only staged, a snapshot identifies the complete A revision.
            DfsSnapshotInfo mid = await rv.Snapshots.CreateAsync("pub008-mid", null, Ct);
            (CommitToken tB, DfsSnapshotInfo sB) = await PublishAsync("pub008-B");
            model.Flush();
            byte[] stateB = model.Committed;
            DfsFileInfo infoB = await AssertCommittedAsync(rv, path, stateB, "B is published");
            Assert.True(infoB.Revision > infoA.Revision, "each flush has its own revision");
            if (!tB.IsNone)
            {
                Assert.True(tA.IsComparableWith(tB));
                Assert.Equal(tA.ClusterId, tB.ClusterId);
                Assert.Equal(tA.VolumeId, tB.VolumeId);
                Assert.Equal(tA.VolumeEpoch, tB.VolumeEpoch);
                Assert.True(tB.CompareTo(tA) > 0, $"B's token {tB} must order after A's {tA}");
                Assert.True(tB.Sequence > tA.Sequence + 1, "the unrelated commit lies between the two boundaries");
                Assert.Equal(tB.Sequence, infoB.Revision);
            }
            Assert.True(sB.Boundary.CompareTo(sA.Boundary) > 0);
            Assert.True(mid.Boundary.CompareTo(sA.Boundary) >= 0 && mid.Boundary.CompareTo(sB.Boundary) < 0,
                "the mid snapshot lies between the two publications");

            await using (DfsSnapshotView view = await rv.Snapshots.OpenAsync(sA.Name, Ct))
                await AssertBytesAsync(stateA, view.Files.ReadAllBytesAsync(path, Ct), "snapshot A identifies exactly the complete A revision");
            await using (DfsSnapshotView view = await rv.Snapshots.OpenAsync(mid.Name, Ct))
                await AssertBytesAsync(stateA, view.Files.ReadAllBytesAsync(path, Ct), "staged B is excluded before its flush");
            await using (DfsSnapshotView view = await rv.Snapshots.OpenAsync(sB.Name, Ct))
                await AssertBytesAsync(stateB, view.Files.ReadAllBytesAsync(path, Ct), "snapshot B identifies exactly the complete B revision");
        }, Ct);

    // ------------------------------------------------------------------------------------------------------------
    // PUB-009
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>(write mode, length change, async API). Synchronous variants run in realtime mode.</summary>
    public static TheoryData<DfsWriteMode, LengthChange, bool> Pub009Cases => new()
    {
        { DfsWriteMode.OnFlush, LengthChange.Shrink, true },
        { DfsWriteMode.OnFlush, LengthChange.Extend, true },
        { DfsWriteMode.OnFlush, LengthChange.ShrinkThenExtend, true },
        { DfsWriteMode.OnFlush, LengthChange.Zero, false },
        { DfsWriteMode.WriteThrough, LengthChange.Shrink, true },
        { DfsWriteMode.WriteThrough, LengthChange.Extend, false },
        { DfsWriteMode.WriteThrough, LengthChange.Zero, true },
        { DfsWriteMode.WriteThrough, LengthChange.ShrinkThenExtend, true },
    };

    [Theory]
    [MemberData(nameof(Pub009Cases))]
    [Catalog("PUB-009", "S", "INV-02", "INV-07")]
    public Task PUB_009_SetLengthFollowsWriteMode(DfsWriteMode mode, LengthChange change, bool asyncApi) =>
        ClusterTest.RunAsync(SimOrRealtime(!asyncApi), async cluster =>
        {
            const string path = "/pub009/f";
            const long position = 100_000;
            byte[] initial = TestData.Bytes(0x0009_0001, 150_000);
            await using DfsClient writerClient = await ConnectAsync(cluster, "c0");
            await using DfsClient readerClient = await ConnectAsync(cluster, "c1");
            DfsVolume wv = writerClient.GetVolume(Vol);
            DfsVolume rv = readerClient.GetVolume(Vol);
            await SeedFileAsync(wv, path, initial);
            DfsFileInfo before = await rv.Files.GetInfoAsync(path, Ct);
            ulong lastRevision = before.Revision;

            long[] lengths = change switch
            {
                LengthChange.Shrink => [40_000],
                LengthChange.Extend => [400_000],
                LengthChange.Zero => [0],
                LengthChange.ShrinkThenExtend => [1_000, 200_000],
                _ => throw new ArgumentOutOfRangeException(nameof(change)),
            };

            var model = new ReferenceFileModel(initial);
            DfsStream writer = await wv.Files.OpenAsync(path, WriterOptions(FileMode.Open, mode), Ct);
            writer.Seek(position, SeekOrigin.Begin);
            foreach (long length in lengths)
            {
                if (asyncApi)
                    await writer.SetLengthAsync(length, Ct);
                else
                    writer.SetLength(length);
                model.SetLength(length);

                Assert.Equal(position, writer.Position);
                Assert.Equal(length, await writer.GetLengthAsync(Ct));
                if (mode == DfsWriteMode.WriteThrough)
                {
                    model.Flush();
                    Assert.Equal(0, writer.PendingBytes);
                    DfsFileInfo info = await AssertCommittedAsync(rv, path, model.Committed,
                        $"a successful WriteThrough SetLength({length}) is committed before it returns");
                    Assert.True(info.Revision > lastRevision);
                    lastRevision = info.Revision;
                }
                else
                {
                    await AssertCommittedAsync(rv, path, initial, $"an OnFlush SetLength({length}) stays private until flush", before.Revision);
                }
            }

            // The handle's own view (zero fill on extension; truncated bytes never reappear).
            await AssertBytesAsync(model.Staged, ReadFromStartAsync(writer), "the writer sees its length change");

            if (mode == DfsWriteMode.OnFlush)
            {
                if (asyncApi)
                    await writer.FlushAsync(Ct);
                else
                    writer.Flush();
                model.Flush();
                DfsFileInfo info = await AssertCommittedAsync(rv, path, model.Committed, "the next flush publishes the length change");
                Assert.True(info.Revision > before.Revision);
                lastRevision = info.Revision;
            }

            if (asyncApi)
                await writer.DisposeAsync();
            else
                writer.Dispose();

            await CrashAndRestartAllAsync(cluster);
            await AssertCommittedAsync(rv, path, model.Committed, "the committed length survives a cold restart", lastRevision);
        }, Ct);

    // ------------------------------------------------------------------------------------------------------------
    // PUB-010
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>(inline file, commit-and-snapshot).</summary>
    public static TheoryData<bool, bool> Pub010Cases => new()
    {
        { true, false },
        { false, false },
        { false, true },
        { true, true },
    };

    [Theory]
    [MemberData(nameof(Pub010Cases))]
    [Catalog("PUB-010", "S", "INV-06", "INV-13")]
    public Task PUB_010_CommitReturnsResolvableToken(bool inline, bool withSnapshot) =>
        ClusterTest.RunAsync(Deterministic(), async cluster =>
        {
            const string path = "/pub010/f";
            const string snapshotName = "pub010-exact";
            byte[] initial = TestData.Bytes(0x0010_0001, inline ? 100 : 100_000);
            byte[] edit = TestData.Bytes(0x0010_0002, inline ? 50 : 70_000);
            await using DfsClient writerClient = await ConnectAsync(cluster, "c0");
            await using DfsClient readerClient = await ConnectAsync(cluster, "c1");
            DfsVolume wv = writerClient.GetVolume(Vol);
            DfsVolume rv = readerClient.GetVolume(Vol);
            await SeedFileAsync(wv, path, initial);

            var model = new ReferenceFileModel(initial);
            await using DfsStream writer = await wv.Files.OpenAsync(path, WriterOptions(FileMode.Open), Ct);
            await WriteAtAsync(writer, 10, edit);
            model.Write(10, edit);
            DfsOperationId id = await wv.NewOperationIdAsync(Ct);
            CommitToken token = await writer.CommitAsync(new DfsMutationOptions
            {
                OperationId = id,
                CommitSnapshotName = withSnapshot ? snapshotName : null,
            }, Ct);
            model.Flush();

            // Identity: cluster / volume / epoch / sequence.
            Assert.False(token.IsNone);
            Assert.Equal(cluster.ClusterId, token.ClusterId);
            Assert.Equal(writerClient.Cluster.ClusterId, token.ClusterId);
            DfsVolumeInfo volume = await rv.GetInfoAsync(Ct);
            Assert.Equal(volume.Id, token.VolumeId);
            Assert.Equal(volume.Epoch, token.VolumeEpoch);
            Assert.True(token.Sequence > 0);
            Assert.True(volume.LastCommit.IsComparableWith(token) && volume.LastCommit.IsAtOrAfter(token));
            Assert.Equal(token, CommitToken.Parse(token.ToString()));

            // The token is the published decision.
            DfsFileInfo info = await AssertCommittedAsync(rv, path, model.Committed, "the committed revision is visible");
            Assert.Equal(token.Sequence, info.Revision);
            await cluster.WaitForReplicationAsync(Vol, Ct);
            await AssertReplicasAtRevisionAsync(cluster, path, token.Sequence, model.CommittedLength, "every replica applied the decision at the token's sequence");

            // The outcome is queryable by id from any client of the principal.
            foreach (DfsVolume v in new[] { wv, rv })
            {
                DfsOperationOutcome outcome = await v.QueryOperationAsync(id, resolve: false, Ct);
                Assert.Equal(id, outcome.Id);
                Assert.Equal(DfsOutcomeStatus.Committed, outcome.Status);
                Assert.Equal(token, outcome.Token);
                Assert.Equal(DfsErrorCode.None, outcome.Error);
            }
            DfsOperationOutcome resolved = await rv.QueryOperationAsync(id, resolve: true, Ct);
            Assert.Equal(DfsOutcomeStatus.Committed, resolved.Status);
            Assert.Equal(token, resolved.Token);

            if (withSnapshot)
            {
                DfsSnapshotInfo snapshot = await rv.Snapshots.GetAsync(snapshotName, Ct);
                Assert.Equal(token, snapshot.Boundary);
                await using DfsSnapshotView view = await rv.Snapshots.OpenAsync(snapshotName, Ct);
                await AssertBytesAsync(model.Committed, view.Files.ReadAllBytesAsync(path, Ct), "the exact snapshot holds the committed revision");
            }

            // The outcome survives a leader change.
            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            await leader.CrashAsync();
            TestNode newLeader = await cluster.GetLeaderAsync(Vol, Ct);
            Assert.NotEqual(leader.Name, newLeader.Name);
            DfsOperationOutcome afterFailover = await rv.QueryOperationAsync(id, resolve: false, Ct);
            Assert.Equal(DfsOutcomeStatus.Committed, afterFailover.Status);
            Assert.Equal(token, afterFailover.Token);
            await leader.RestartAsync();
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), Ct);

            // A later commit orders after it.
            byte[] edit2 = TestData.Bytes(0x0010_0003, 20);
            await WriteAtAsync(writer, 0, edit2);
            model.Write(0, edit2);
            CommitToken token2 = await writer.CommitAsync(Ct);
            model.Flush();
            Assert.True(token2.IsComparableWith(token));
            Assert.True(token2.CompareTo(token) > 0);
            DfsFileInfo info2 = await AssertCommittedAsync(rv, path, model.Committed, "the second commit is visible");
            Assert.Equal(token2.Sequence, info2.Revision);
        }, Ct);

    // ------------------------------------------------------------------------------------------------------------
    // PUB-011
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>(publication path, 1-based chunk whose upload is held). Each payload is exactly four aligned chunks.</summary>
    public static TheoryData<PublishPath, int> Pub011Cases => new()
    {
        { PublishPath.StreamFlush, 1 },
        { PublishPath.StreamFlush, 2 },
        { PublishPath.StreamFlush, 4 },
        { PublishPath.StreamCommit, 4 },
        { PublishPath.WriteThroughWrite, 3 },
        { PublishPath.WriteAllHelper, 1 },
        { PublishPath.WriteAllHelper, 3 },
        { PublishPath.AppendHelper, 2 },
    };

    [Theory]
    [MemberData(nameof(Pub011Cases))]
    [Catalog("PUB-011", "S", "INV-01", "INV-02", "INV-08")]
    public Task PUB_011_DataBeforeMetadataPublication(PublishPath kind, int heldChunk) =>
        ClusterTest.RunAsync(Deterministic(), async cluster =>
        {
            const string path = "/pub011/f";
            byte[] initial = TestData.Bytes(0x0011_0001, Chunk);
            byte[] payload = TestData.Bytes(0x0011_0002, 4 * Chunk);
            await using DfsClient writerClient = await ConnectAsync(cluster, "c0", LargeBuffer);
            await using DfsClient readerClient = await ConnectAsync(cluster, "c1");
            DfsVolume wv = writerClient.GetVolume(Vol);
            DfsVolume rv = readerClient.GetVolume(Vol);
            Publication publication = await PreparePublicationAsync(wv, path, kind, initial, payload, aligned: true);
            DfsFileInfo before = await rv.Files.GetInfoAsync(path, Ct);
            IReadOnlySet<ObjectId> storedBefore = await cluster.Inspect.GetAllStoredObjectsAsync(Vol);
            long publishBefore = PublishRequests(cluster, "c0");

            FaultArm arm = cluster.Faults.Arm(new FaultMatch(FaultSites.ClientUploadChunk, Occurrence: heldChunk), FaultAction.Block());
            Task<CommitToken> task = publication.RunAsync(Ct);
            await arm.WaitTriggeredAsync(TimeSpan.FromSeconds(30));
            cluster.Faults.AssertReached(arm);

            // The other chunks may finish; the held one cannot. Metadata publication and success stay prohibited.
            await AssertStillPendingAsync(cluster, task, TimeSpan.FromSeconds(3), "publication must wait for every newly referenced object");
            Assert.Equal(publishBefore, PublishRequests(cluster, "c0"));
            await AssertCommittedAsync(rv, path, initial, "no metadata publication while an object lacks durable receipts", before.Revision);
            await AssertReplicasAtRevisionAsync(cluster, path, before.Revision, initial.LongLength, "no replica references the incomplete object set");
            await AssertUnreachableAsync(cluster, await NewObjectsSinceAsync(cluster, storedBefore), "the already-uploaded objects are still unpublished");

            arm.Disarm();
            arm.Release();
            Exception? error = await CaptureWithinAsync(cluster, task, TimeSpan.FromSeconds(30), "publication completes once the held object is durable");
            Assert.Null(error);
            await AssertCommittedAsync(rv, path, publication.Expected, "the complete revision is published after the delayed object");
            await AssertDurableAsync(cluster, path, ThreeZonePolicy, "every object referenced by the publication met the policy");
            if (publication.Writer is not null)
                await publication.Writer.DisposeAsync();
        }, Ct);

    // ------------------------------------------------------------------------------------------------------------
    // PUB-012
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>Every staging step of §8.2 F03–F10 that precedes publication.</summary>
    public static TheoryData<StagingCrashPoint> Pub012Cases
    {
        get
        {
            var data = new TheoryData<StagingCrashPoint>();
            foreach (StagingCrashPoint point in Enum.GetValues<StagingCrashPoint>())
                data.Add(point);
            return data;
        }
    }

    [Theory]
    [MemberData(nameof(Pub012Cases))]
    [Catalog("PUB-012", "S", "INV-01", "INV-02", "INV-09")]
    public Task PUB_012_CrashBeforePublicationKeepsOldState(StagingCrashPoint point) =>
        ClusterTest.RunAsync(Deterministic(), async cluster =>
        {
            const string path = "/pub012/f";
            byte[] initial = TestData.Bytes(0x0012_0001, 200_000);
            byte[] e1 = TestData.Bytes(0x0012_0002, 3 * Chunk);
            byte[] e2 = TestData.Bytes(0x0012_0003, 70_000);
            DfsClient writerClient = await ConnectAsync(cluster, "c0", LargeBuffer);
            await using DfsClient readerClient = await ConnectAsync(cluster, "c1");
            DfsVolume wv = writerClient.GetVolume(Vol);
            DfsVolume rv = readerClient.GetVolume(Vol);
            await SeedFileAsync(wv, path, initial);
            DfsFileInfo before = await rv.Files.GetInfoAsync(path, Ct);
            IReadOnlySet<ObjectId> storedBefore = await cluster.Inspect.GetAllStoredObjectsAsync(Vol);
            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);

            (FaultMatch match, CrashMode mode) = StagingCrashMatch(point, leader.Name);
            FaultArm arm = cluster.Faults.Arm(match, FaultAction.Block());

            // Multi-extent staging: two distant edits (one beyond EOF) and a length change, published by one flush.
            async Task WriterWorkflowAsync()
            {
                DfsStream writer = await wv.Files.OpenAsync(path, WriterOptions(FileMode.Open, bufferSize: LargeBuffer), Ct);
                await WriteAtAsync(writer, 0, e1);
                await WriteAtAsync(writer, 300_000, e2);
                await writer.SetLengthAsync(400_000, Ct);
                await writer.FlushAsync(Ct);
            }

            Task workflow = WriterWorkflowAsync();
            await arm.WaitTriggeredAsync(TimeSpan.FromSeconds(30));
            cluster.Faults.AssertReached(arm);
            string hitAt = arm.Contexts[0].Node;

            // The staging participants die at this step: the writer process, and the server node where the step ran.
            cluster.KillClient(writerClient);
            TestNode? crashed = cluster.Nodes.FirstOrDefault(n => n.Name == hitAt);
            if (crashed is not null)
                await crashed.CrashAsync(mode);
            arm.Disarm();
            arm.Release();
            Assert.False(workflow.IsCompletedSuccessfully, "the publication cannot have succeeded");

            // Safety during recovery ...
            await cluster.GetLeaderAsync(Vol, Ct);
            await AssertCommittedAsync(rv, path, initial, "while the crashed participant is down the old revision stays authoritative", before.Revision);
            await AssertReplicasAtRevisionAsync(cluster, path, before.Revision, initial.LongLength, "no subset of the staged changes is applied during recovery");

            // ... and after it.
            if (crashed is not null)
                await crashed.RestartAsync();
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), Ct);
            await cluster.WaitForReplicationAsync(Vol, Ct);
            await AssertCommittedAsync(rv, path, initial, "recovery exposes the old committed file", before.Revision);
            await AssertReplicasAtRevisionAsync(cluster, path, before.Revision, initial.LongLength, "every recovered replica holds the old revision");
            await cluster.Inspect.AssertReplicasConvergedAsync(Vol);

            // The dead writer's staging is never published, even after the server reclaims its reservation.
            await cluster.WaitUntilAsync(() => TryOpenWriterAsync(rv, path), TimeSpan.FromSeconds(40),
                "the dead writer's reservation must be released by server-side lease/session expiry", Ct);
            await AssertCommittedAsync(rv, path, initial, "abandoned staging is never published", before.Revision);
            await AssertUnreachableAsync(cluster, await NewObjectsSinceAsync(cluster, storedBefore), "the dead writer's staged objects stay unreferenced");
        }, Ct);

    private static (FaultMatch Match, CrashMode Mode) StagingCrashMatch(StagingCrashPoint point, string leader) => point switch
    {
        StagingCrashPoint.ClientFirstChunkUpload => (new FaultMatch(FaultSites.ClientUploadChunk, Occurrence: 1), CrashMode.LoseUnflushed),
        StagingCrashPoint.ClientThirdChunkUpload => (new FaultMatch(FaultSites.ClientUploadChunk, Occurrence: 3), CrashMode.LoseUnflushed),
        StagingCrashPoint.ClientCommitRequestSend => (new FaultMatch(FaultSites.ClientRequestSend, Operation: DfsOperationKind.CommitFile), CrashMode.LoseUnflushed),
        StagingCrashPoint.StorageRequestAccepted => (new FaultMatch(FaultSites.RpcRequestAccepted, Operation: DfsOperationKind.PutObject, Occurrence: 2), CrashMode.LoseUnflushed),
        StagingCrashPoint.StagingRegistered => (new FaultMatch(FaultSites.StagingRegistered, Node: leader), CrashMode.LoseUnflushed),
        StagingCrashPoint.StorageObjectWriteTorn => (new FaultMatch(FaultSites.ObjectWrite, Operation: DfsOperationKind.PutObject, Occurrence: 3), CrashMode.TearLastWrite),
        StagingCrashPoint.StorageBeforeFlush => (new FaultMatch(FaultSites.ObjectBeforeFlush, Operation: DfsOperationKind.PutObject, Occurrence: 3), CrashMode.LoseUnflushed),
        StagingCrashPoint.StorageBeforeFlushRandomSubset => (new FaultMatch(FaultSites.ObjectBeforeFlush, Operation: DfsOperationKind.PutObject, Occurrence: 3), CrashMode.RandomSubset),
        StagingCrashPoint.StorageAfterFlushBeforeReceipt => (new FaultMatch(FaultSites.ObjectAfterFlushBeforeReceipt, Operation: DfsOperationKind.PutObject, Occurrence: 3), CrashMode.LoseUnflushed),
        StagingCrashPoint.LeaderReceiptsValidated => (new FaultMatch(FaultSites.CommitReceiptValidated, Node: leader, Operation: DfsOperationKind.CommitFile), CrashMode.LoseUnflushed),
        StagingCrashPoint.LeaderBeforePropose => (new FaultMatch(FaultSites.RaftBeforePropose, Node: leader, Operation: DfsOperationKind.CommitFile), CrashMode.LoseUnflushed),
        _ => throw new ArgumentOutOfRangeException(nameof(point), point, null),
    };

    // ------------------------------------------------------------------------------------------------------------
    // PUB-013
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>(post-decision site on the leader, crash scope).</summary>
    public static TheoryData<string, CommitCrashScope> Pub013Cases => new()
    {
        { FaultSites.RaftAfterCommitBeforeApply, CommitCrashScope.LeaderOnly },
        { FaultSites.RaftAfterCommitBeforeApply, CommitCrashScope.AllNodes },
        { FaultSites.ApplyAfterBeforeReply, CommitCrashScope.LeaderOnly },
        { FaultSites.ApplyAfterBeforeReply, CommitCrashScope.AllNodes },
        { FaultSites.RpcReplySend, CommitCrashScope.LeaderOnly },
        { FaultSites.RpcReplySend, CommitCrashScope.AllNodes },
    };

    [Theory]
    [MemberData(nameof(Pub013Cases))]
    [Catalog("PUB-013", "S", "INV-01", "INV-02", "INV-06")]
    public Task PUB_013_CrashAfterCommitKeepsNewState(string site, CommitCrashScope scope) =>
        ClusterTest.RunAsync(Deterministic(), async cluster =>
        {
            const string path = "/pub013/f";
            byte[] initial = TestData.Bytes(0x0013_0001, 200_000);
            byte[] payload = TestData.Bytes(0x0013_0002, 150_000);
            await using DfsClient writerClient = await ConnectAsync(cluster, "c0", LargeBuffer);
            await using DfsClient readerClient = await ConnectAsync(cluster, "c1");
            DfsVolume wv = writerClient.GetVolume(Vol);
            DfsVolume rv = readerClient.GetVolume(Vol);
            Publication publication = await PreparePublicationAsync(wv, path, PublishPath.StreamCommit, initial, payload);
            DfsOperationId id = publication.ReservedId!.Value;
            DfsStream writer = publication.Writer!;
            DfsFileInfo before = await rv.Files.GetInfoAsync(path, Ct);
            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);

            FaultArm arm = cluster.Faults.Arm(new FaultMatch(site, Node: leader.Name, Operation: DfsOperationKind.CommitFile), FaultAction.Block());
            long writeId = cluster.History.Invoke("c0", "write", path, publication.Expected, id);
            Task<CommitToken> commit = publication.RunAsync(Ct);
            await arm.WaitTriggeredAsync(TimeSpan.FromSeconds(30));
            cluster.Faults.AssertReached(arm);

            // Crash after the durable metadata decision, before the reply reaches the client.
            if (scope == CommitCrashScope.LeaderOnly)
            {
                await leader.CrashAsync(CrashMode.LoseUnflushed);
            }
            else
            {
                foreach (TestNode node in cluster.Nodes)
                    await node.CrashAsync(CrashMode.LoseUnflushed);
            }
            arm.Disarm();
            arm.Release();

            if (scope == CommitCrashScope.LeaderOnly)
            {
                // During recovery the surviving majority already serves the decision.
                TestNode successor = await cluster.GetLeaderAsync(Vol, Ct);
                Assert.NotEqual(leader.Name, successor.Name);
                await AssertCommittedAsync(rv, path, publication.Expected, "the committed decision is authoritative on the surviving majority");
                DfsOperationOutcome during = await rv.QueryOperationAsync(id, resolve: false, Ct);
                Assert.Equal(DfsOutcomeStatus.Committed, during.Status);
                await leader.RestartAsync();
            }
            else
            {
                foreach (TestNode node in cluster.Nodes)
                    await node.RestartAsync();
            }
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), Ct);
            await cluster.WaitForReplicationAsync(Vol, Ct);

            // The caller saw success with the decided token, or an unknown outcome carrying the id — never a definite failure.
            Exception? error = await CaptureWithinAsync(cluster, commit, TimeSpan.FromSeconds(60), "the commit call must terminate");
            DfsOperationOutcome outcome = await rv.QueryOperationAsync(id, resolve: false, Ct);
            Assert.Equal(DfsOutcomeStatus.Committed, outcome.Status);
            if (error is null)
            {
                Assert.Equal(outcome.Token, await commit);
                cluster.History.Complete(writeId, OpOutcome.Ok);
            }
            else
            {
                DfsCommitOutcomeUnknownException unknown = Assert.IsAssignableFrom<DfsCommitOutcomeUnknownException>(error);
                Assert.Equal(id, unknown.OperationId);
                Assert.True(writer.IsFaulted, "an indeterminate publication outcome must fault the stream");
                Assert.NotNull(await Record.ExceptionAsync(async () => await writer.WriteAsync(new byte[] { 1 }, Ct)));
                cluster.History.Complete(writeId, OpOutcome.Unknown, error);
                cluster.History.Resolve(writeId, OpOutcome.Ok);
            }

            DfsFileInfo info = await AssertCommittedAsync(rv, path, publication.Expected, "recovery preserves the full new revision");
            Assert.True(info.Revision > before.Revision);
            Assert.Equal(outcome.Token.Sequence, info.Revision);
            await AssertReplicasAtRevisionAsync(cluster, path, info.Revision, publication.Expected.LongLength, "every replica recovered the decision");
            await cluster.Inspect.AssertReplicasConvergedAsync(Vol);
            await AssertDurableAsync(cluster, path, ThreeZonePolicy, "the committed objects survived the crash");

            await cluster.History.RecordAsync("c1", "read", path, null, () => rv.Files.ReadAllBytesAsync(path, Ct));
            LinearizabilityChecker.Check(cluster.History.Operations, new RegisterModel(initial)).AssertValid();
            await Record.ExceptionAsync(async () => await writer.DisposeAsync());
        }, Ct);

    // ------------------------------------------------------------------------------------------------------------
    // PUB-014
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>(cancellation point, publication path). Payloads are four aligned chunks.</summary>
    public static TheoryData<CancelPoint, PublishPath> Pub014Cases => new()
    {
        { CancelPoint.TokenAlreadyCancelled, PublishPath.StreamFlush },
        { CancelPoint.TokenAlreadyCancelled, PublishPath.AppendHelper },
        { CancelPoint.AtOperationStart, PublishPath.StreamCommit },
        { CancelPoint.AtOperationStart, PublishPath.WriteAllHelper },
        { CancelPoint.AfterValidation, PublishPath.StreamFlush },
        { CancelPoint.AfterValidation, PublishPath.AppendHelper },
        { CancelPoint.AfterValidation, PublishPath.WriteThroughWrite },
        { CancelPoint.DuringStagingUpload, PublishPath.StreamFlush },
        { CancelPoint.DuringStagingUpload, PublishPath.WriteAllHelper },
        { CancelPoint.DuringStagingUpload, PublishPath.AppendHelper },
    };

    [Theory]
    [MemberData(nameof(Pub014Cases))]
    [Catalog("PUB-014", "S", "INV-02", "INV-06")]
    public Task PUB_014_CancelledBeforeRequestDoesNotPublish(CancelPoint point, PublishPath kind) =>
        ClusterTest.RunAsync(Deterministic(), async cluster =>
        {
            const string path = "/pub014/f";
            byte[] initial = TestData.Bytes(0x0014_0001, Chunk);
            byte[] payload = TestData.Bytes(0x0014_0002, 4 * Chunk);
            await using DfsClient writerClient = await ConnectAsync(cluster, "c0", LargeBuffer);
            await using DfsClient readerClient = await ConnectAsync(cluster, "c1");
            DfsVolume wv = writerClient.GetVolume(Vol);
            DfsVolume rv = readerClient.GetVolume(Vol);
            Publication publication = await PreparePublicationAsync(wv, path, kind, initial, payload, aligned: true);
            DfsFileInfo before = await rv.Files.GetInfoAsync(path, Ct);
            IReadOnlySet<ObjectId> storedBefore = await cluster.Inspect.GetAllStoredObjectsAsync(Vol);
            long publishBefore = PublishRequests(cluster, "c0");

            using var cts = CancellationTokenSource.CreateLinkedTokenSource(Ct);
            FaultAction cancel = FaultAction.Custom(_ =>
            {
                cts.Cancel();
                return Task.CompletedTask;
            });
            FaultArm? arm = point switch
            {
                CancelPoint.TokenAlreadyCancelled => null,
                CancelPoint.AtOperationStart => cluster.Faults.Arm(new FaultMatch(FaultSites.ClientOperationStart, Operation: publication.OperationKind), cancel),
                CancelPoint.AfterValidation => cluster.Faults.Arm(new FaultMatch(FaultSites.ClientValidated, Operation: publication.OperationKind), cancel),
                CancelPoint.DuringStagingUpload => cluster.Faults.Arm(new FaultMatch(FaultSites.ClientUploadChunk, Occurrence: 2), cancel),
                _ => throw new ArgumentOutOfRangeException(nameof(point)),
            };
            if (point == CancelPoint.TokenAlreadyCancelled)
                cts.Cancel();

            Exception? error = await CaptureWithinAsync(cluster, publication.RunAsync(cts.Token), TimeSpan.FromSeconds(30), "a cancelled call terminates");
            if (arm is not null)
            {
                cluster.Faults.AssertReached(arm);
                arm.Disarm();
            }

            // Nothing was transmitted, so the SDK knows nothing was published: plain cancellation.
            Assert.Equal(publishBefore, PublishRequests(cluster, "c0"));
            Assert.True(error is OperationCanceledException, $"expected OperationCanceledException, got {error?.GetType().FullName ?? "success"}: {error}");
            Assert.False(error is DfsCommitOutcomeUnknownException);
            await AssertCommittedAsync(rv, path, initial, "cancellation before publication leaves the committed contents", before.Revision);
            await AssertReplicasAtRevisionAsync(cluster, path, before.Revision, initial.LongLength, "no replica applied anything");
            await cluster.DelayAsync(TimeSpan.FromSeconds(3), Ct);
            await AssertCommittedAsync(rv, path, initial, "no delayed publication follows a pre-transmission cancellation", before.Revision);
            await AssertUnreachableAsync(cluster, await NewObjectsSinceAsync(cluster, storedBefore), "objects uploaded before the cancellation stay unpublished");

            // The cancelled call had no effect: an uncancelled retry applies the change exactly once.
            if (publication.Writer is { IsFaulted: true } faulted)
            {
                await Record.ExceptionAsync(async () => await faulted.DisposeAsync());
                await AssertCommittedAsync(rv, path, initial, "a faulted stream publishes nothing", before.Revision);
                return;
            }
            Exception? retryError = await CaptureWithinAsync(cluster, publication.RunAsync(Ct), TimeSpan.FromSeconds(30), "the uncancelled retry terminates");
            Assert.Null(retryError);
            await AssertCommittedAsync(rv, path, publication.Expected, "the retry applies the change exactly once (no duplicate append)");
            if (publication.Writer is not null)
                await publication.Writer.DisposeAsync();
        }, Ct);

    // ------------------------------------------------------------------------------------------------------------
    // PUB-015
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>(server-side situation, publication path).</summary>
    public static TheoryData<UncertainPoint, PublishPath> Pub015Cases => new()
    {
        { UncertainPoint.CommittedReplyWithheld, PublishPath.StreamCommit },
        { UncertainPoint.CommittedReplyWithheld, PublishPath.StreamFlush },
        { UncertainPoint.CommittedReplyWithheld, PublishPath.WriteAllHelper },
        { UncertainPoint.CommittedReplyWithheld, PublishPath.WriteThroughWrite },
        { UncertainPoint.CommittedReplyDropped, PublishPath.AppendHelper },
        { UncertainPoint.CommittedReplyDropped, PublishPath.StreamCommit },
        { UncertainPoint.AcceptedBeforeProposal, PublishPath.StreamFlush },
        { UncertainPoint.AcceptedBeforeProposal, PublishPath.WriteAllHelper },
        { UncertainPoint.ProposalNeverMade, PublishPath.StreamCommit },
        { UncertainPoint.ProposalNeverMade, PublishPath.AppendHelper },
    };

    [Theory]
    [MemberData(nameof(Pub015Cases))]
    [Catalog("PUB-015", "S", "INV-06")]
    public Task PUB_015_CancellationAfterPossibleCommitIsUnknown(UncertainPoint point, PublishPath kind) =>
        ClusterTest.RunAsync(Deterministic(), async cluster =>
        {
            const string path = "/pub015/f";
            byte[] initial = TestData.Bytes(0x0015_0001, 100_000);
            byte[] payload = TestData.Bytes(0x0015_0002, 150_000);
            await using DfsClient writerClient = await ConnectAsync(cluster, "c0", LargeBuffer);
            await using DfsClient readerClient = await ConnectAsync(cluster, "c1");
            DfsVolume wv = writerClient.GetVolume(Vol);
            DfsVolume rv = readerClient.GetVolume(Vol);
            Publication publication = await PreparePublicationAsync(wv, path, kind, initial, payload);
            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);

            string site = point switch
            {
                UncertainPoint.CommittedReplyWithheld => FaultSites.ApplyAfterBeforeReply,
                UncertainPoint.CommittedReplyDropped => FaultSites.RpcReplySend,
                UncertainPoint.AcceptedBeforeProposal => FaultSites.RpcRequestAccepted,
                UncertainPoint.ProposalNeverMade => FaultSites.RaftBeforePropose,
                _ => throw new ArgumentOutOfRangeException(nameof(point)),
            };
            FaultArm arm = point == UncertainPoint.CommittedReplyDropped
                ? cluster.Faults.Arm(new FaultMatch(site, Node: leader.Name, Operation: publication.OperationKind, Count: 1_000), FaultAction.DropReply())
                : cluster.Faults.Arm(new FaultMatch(site, Node: leader.Name, Operation: publication.OperationKind), FaultAction.Block());

            using var cts = CancellationTokenSource.CreateLinkedTokenSource(Ct);
            long writeId = cluster.History.Invoke("c0", "write", path, publication.Expected, publication.ReservedId);
            Task<CommitToken> task = publication.RunAsync(cts.Token);
            await arm.WaitTriggeredAsync(TimeSpan.FromSeconds(30));
            cluster.Faults.AssertReached(arm);

            // The request was transmitted. Cut the client off so it cannot establish the outcome, then cancel.
            cluster.Network.Isolate("client:c0");
            cts.Cancel();
            Exception? error = await CaptureWithinAsync(cluster, task, TimeSpan.FromSeconds(30), "cancellation after transmission still terminates the call");
            Assert.NotNull(error);
            Assert.False(error is OperationCanceledException,
                $"cancellation after a possible commit must not be reported as a definite cancellation: {error}");
            DfsCommitOutcomeUnknownException unknown = Assert.IsAssignableFrom<DfsCommitOutcomeUnknownException>(error);
            Assert.Equal(DfsErrorCode.OutcomeUnknown, unknown.ErrorCode);
            DfsOperationId id = OperationIdFor(publication, unknown);
            cluster.History.AttachOperationId(writeId, id);
            cluster.History.Complete(writeId, OpOutcome.Unknown, error);
            if (publication.Writer is not null)
            {
                Assert.True(publication.Writer.IsFaulted, "an indeterminate publication must fault the stream");
                Assert.NotNull(await Record.ExceptionAsync(async () => await publication.Writer.WriteAsync(new byte[] { 1 }, Ct)));
            }

            // Let the server side finish (or, for ProposalNeverMade, lose the request with its leader), then resolve.
            if (point == UncertainPoint.ProposalNeverMade)
                await leader.CrashAsync();
            arm.Disarm();
            arm.Release();
            cluster.Network.Heal();
            await cluster.GetLeaderAsync(Vol, Ct);

            DfsOutcomeStatus? required = point switch
            {
                UncertainPoint.CommittedReplyWithheld or UncertainPoint.CommittedReplyDropped => DfsOutcomeStatus.Committed,
                UncertainPoint.ProposalNeverMade => DfsOutcomeStatus.Aborted,
                _ => null,
            };
            DfsOperationOutcome outcome = await ResolveAndVerifyAsync(cluster, rv, publication, id, required);
            cluster.History.Resolve(writeId, outcome.Status == DfsOutcomeStatus.Committed ? OpOutcome.Ok : OpOutcome.Failed);

            if (point == UncertainPoint.ProposalNeverMade)
            {
                await leader.RestartAsync();
                await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), Ct);
                await AssertCommittedAsync(rv, path, initial, "the fenced operation cannot commit when the old leader returns");
            }

            await cluster.History.RecordAsync("c1", "read", path, null, () => rv.Files.ReadAllBytesAsync(path, Ct));
            LinearizabilityChecker.Check(cluster.History.Operations, new RegisterModel(initial)).AssertValid();
            if (publication.Writer is not null)
                await Record.ExceptionAsync(async () => await publication.Writer.DisposeAsync());
        }, Ct);

    // ------------------------------------------------------------------------------------------------------------
    // PUB-017
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>(persistence failure, DisposeAsync vs Dispose). Synchronous Dispose runs in realtime mode.</summary>
    public static TheoryData<DisposeFailure, bool> Pub017Cases => new()
    {
        { DisposeFailure.StorageFlushFails, true },
        { DisposeFailure.StorageFlushFails, false },
        { DisposeFailure.DisksFull, true },
        { DisposeFailure.CommitReplyWithheld, true },
        { DisposeFailure.CommitReplyWithheld, false },
        { DisposeFailure.QuorumLost, true },
        { DisposeFailure.QuorumLost, false },
    };

    [Theory]
    [MemberData(nameof(Pub017Cases))]
    [Catalog("PUB-017", "S", "INV-01", "INV-06", "INV-08")]
    public Task PUB_017_DisposeFlushFailureIsVisible(DisposeFailure failure, bool asyncDispose) =>
        ClusterTest.RunAsync(SimOrRealtime(!asyncDispose), async cluster =>
        {
            const string path = "/pub017/f";
            byte[] initial = TestData.Bytes(0x0017_0001, 100_000);
            byte[] payload = TestData.Bytes(0x0017_0002, 150_000);
            DfsClient writerClient = await ConnectAsync(cluster, "c0", LargeBuffer, operationTimeout: TimeSpan.FromSeconds(3));
            await using DfsClient readerClient = await ConnectAsync(cluster, "c1");
            DfsVolume wv = writerClient.GetVolume(Vol);
            DfsVolume rv = readerClient.GetVolume(Vol);
            Publication publication = await PreparePublicationAsync(wv, path, PublishPath.StreamFlush, initial, payload);
            DfsStream writer = publication.Writer!;
            DfsFileInfo before = await rv.Files.GetInfoAsync(path, Ct);
            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            TestNode[] followers = cluster.Nodes.Where(n => n.Name != leader.Name).ToArray();

            FaultArm? arm = null;
            switch (failure)
            {
                case DisposeFailure.StorageFlushFails:
                    arm = cluster.Faults.Arm(new FaultMatch(FaultSites.ObjectBeforeFlush, Operation: DfsOperationKind.PutObject, Count: 100_000), FaultAction.IoError());
                    break;
                case DisposeFailure.DisksFull:
                    foreach (TestNode node in followers)
                        node.Disk.Fill();
                    break;
                case DisposeFailure.CommitReplyWithheld:
                    arm = cluster.Faults.Arm(new FaultMatch(FaultSites.ApplyAfterBeforeReply, Node: leader.Name, Operation: DfsOperationKind.CommitFile), FaultAction.Block());
                    break;
                case DisposeFailure.QuorumLost:
                    foreach (TestNode node in followers)
                        await node.CrashAsync();
                    break;
            }

            // Realtime only for the synchronous variant: Dispose blocks its thread while the test steers the failure.
            Task dispose = asyncDispose ? writer.DisposeAsync().AsTask() : Task.Run(writer.Dispose, Ct);
            if (failure == DisposeFailure.CommitReplyWithheld)
            {
                await arm!.WaitTriggeredAsync(TimeSpan.FromSeconds(30));
                cluster.Network.Isolate("client:c0");
                arm.Disarm();
                arm.Release();
            }
            Exception? error = await CaptureWithinAsync(cluster, dispose, TimeSpan.FromSeconds(60), "disposal must terminate and surface the failure");
            if (arm is not null)
                cluster.Faults.AssertReached(arm);

            Assert.True(error is not null, "Dispose must surface the failed flush; it must never report a successful commit");
            Assert.False(error is OperationCanceledException, $"a persistence failure is not a cancellation: {error}");
            Assert.True(error is IOException or TimeoutException, $"unexpected failure type {error.GetType().FullName}: {error}");

            // Restore prerequisites, then establish the possible outcome.
            switch (failure)
            {
                case DisposeFailure.StorageFlushFails:
                    arm!.Disarm();
                    break;
                case DisposeFailure.CommitReplyWithheld:
                    cluster.Network.Heal();
                    break;
                case DisposeFailure.QuorumLost:
                    foreach (TestNode node in followers)
                        await node.RestartAsync();
                    break;
            }
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), Ct);

            if (error is DfsCommitOutcomeUnknownException unknown)
            {
                DfsOperationId id = OperationIdFor(publication, unknown);
                await ResolveAndVerifyAsync(cluster, rv, publication, id,
                    failure == DisposeFailure.CommitReplyWithheld ? DfsOutcomeStatus.Committed : null);
            }
            else
            {
                Assert.True(failure != DisposeFailure.CommitReplyWithheld,
                    "the commit was applied before the client lost contact: only an unknown-outcome error is truthful");
                await AssertCommittedAsync(rv, path, initial, "a definite disposal failure publishes nothing", before.Revision);
                await cluster.DelayAsync(TimeSpan.FromSeconds(3), Ct);
                await AssertCommittedAsync(rv, path, initial, "nor later", before.Revision);
            }

            // The reservation is eventually released (server-authoritative close or expiry).
            await cluster.WaitUntilAsync(() => TryOpenWriterAsync(rv, path), TimeSpan.FromSeconds(40),
                "the failed writer's reservation must eventually be released", Ct);
            await Record.ExceptionAsync(async () => await writerClient.DisposeAsync());
        }, Ct);

    // ------------------------------------------------------------------------------------------------------------
    // PUB-021
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>(lease-loss cause, replacement, staged bytes, leader change between expiry and return).</summary>
    public static TheoryData<LeaseLossCause, Replacement, int, bool> Pub021Cases => new()
    {
        { LeaseLossCause.ClientIsolated, Replacement.Committed, 1_000, false },
        { LeaseLossCause.ClientIsolated, Replacement.OpenedOnly, 300_000, false },
        { LeaseLossCause.ClientIsolated, Replacement.None, 1_000, false },
        { LeaseLossCause.ClientClockFrozen, Replacement.Committed, 300_000, false },
        { LeaseLossCause.ClientClockFrozen, Replacement.None, 1_000, false },
        { LeaseLossCause.ClientIsolated, Replacement.Committed, 300_000, true },
        { LeaseLossCause.ClientClockFrozen, Replacement.OpenedOnly, 1_000, true },
    };

    [Theory]
    [MemberData(nameof(Pub021Cases))]
    [Catalog("PUB-021", "S", "INV-02", "INV-05")]
    public Task PUB_021_LostLeaseFaultsDirtyStream(LeaseLossCause cause, Replacement replacement, int stagedLength, bool leaderChange) =>
        ClusterTest.RunAsync(Deterministic(), async cluster =>
        {
            const string path = "/pub021/f";
            byte[] initial = TestData.Bytes(0x0021_0001, 100_000);
            byte[] staged = TestData.Bytes(0x0021_0002, stagedLength);
            TestClock? clockA = cause == LeaseLossCause.ClientClockFrozen ? new TestClock(cluster.Time) : null;
            await using DfsClient clientA = await ConnectAsync(cluster, "c0", clock: clockA);
            await using DfsClient clientB = await ConnectAsync(cluster, "c1");
            DfsVolume av = clientA.GetVolume(Vol);
            DfsVolume bv = clientB.GetVolume(Vol);
            await SeedFileAsync(av, path, initial);
            IReadOnlySet<ObjectId> storedBefore = await cluster.Inspect.GetAllStoredObjectsAsync(Vol);

            DfsStream a = await av.Files.OpenAsync(path, WriterOptions(FileMode.Open), Ct);
            await WriteAtAsync(a, 0, staged);
            ulong generationA = a.WriterGeneration;
            InodeId inode = a.InodeId;
            Assert.True(generationA > 0, "a writable handle carries a fencing generation");
            HashSet<ObjectId> aUploads = await NewObjectsSinceAsync(cluster, storedBefore);

            // A keeps its process memory and staged data but cannot renew: only the server may expire it.
            if (cause == LeaseLossCause.ClientIsolated)
                cluster.Network.Isolate("client:c0");
            else
                clockA!.Freeze();
            await cluster.WaitUntilAsync(async () => !(await cluster.Inspect.GetVolumeStateAsync(Vol)).Handles
                    .Any(h => h.Inode == inode && h.IsWriter && !h.Expired && h.Generation == generationA),
                TimeSpan.FromSeconds(8), "the server must expire A's writer lease after LeaseDuration without renewal", Ct);

            TestNode? oldLeader = null;
            if (leaderChange)
            {
                oldLeader = await cluster.GetLeaderAsync(Vol, Ct);
                await oldLeader.CrashAsync();
                TestNode successor = await cluster.GetLeaderAsync(Vol, Ct);
                Assert.NotEqual(oldLeader.Name, successor.Name);
            }

            var model = new ReferenceFileModel(initial);
            DfsStream? b = null;
            if (replacement != Replacement.None)
            {
                b = await bv.Files.OpenAsync(path, WriterOptions(FileMode.Open), Ct);
                Assert.True(b.WriterGeneration > generationA,
                    $"the replacement writer's generation {b.WriterGeneration} must exceed the expired generation {generationA}");
                if (replacement == Replacement.Committed)
                {
                    byte[] bData = TestData.Bytes(0x0021_0003, 5_000);
                    await WriteAtAsync(b, 50_000, bData);
                    await b.FlushAsync(Ct);
                    model.Write(50_000, bData);
                    model.Flush();
                }
            }

            // A returns: its renewals and stale buffer can reach the cluster again.
            if (cause == LeaseLossCause.ClientIsolated)
                cluster.Network.Heal();
            else
                clockA!.Unfreeze();
            if (oldLeader is not null)
            {
                await oldLeader.RestartAsync();
                await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), Ct);
            }

            Exception? flushError = await CaptureWithinAsync(cluster, a.FlushAsync(Ct), TimeSpan.FromSeconds(30), "the stale writer's flush terminates");
            DfsLeaseLostException lost = Assert.IsAssignableFrom<DfsLeaseLostException>(flushError);
            Assert.Equal(DfsErrorCode.LeaseLost, lost.ErrorCode);
            Assert.True(a.IsFaulted, "lease loss must fault the dirty stream");
            Assert.IsAssignableFrom<DfsLeaseLostException>(a.Fault);
            Assert.NotNull(await Record.ExceptionAsync(async () => await a.WriteAsync(new byte[] { 7 }, Ct)));
            Assert.NotNull(await Record.ExceptionAsync(() => a.CommitAsync(Ct)));
            Assert.NotNull(await Record.ExceptionAsync(() => a.FlushAsync(Ct)));
            await Record.ExceptionAsync(async () => await a.DisposeAsync());

            // The replacement (or nobody) holds authority; A's stale staging was never published.
            await AssertCommittedAsync(bv, path, model.Committed, "a stale writer cannot publish by silently reacquiring authority");
            HandleInspection[] activeWriters = (await cluster.Inspect.GetVolumeStateAsync(Vol)).Handles
                .Where(h => h.Inode == inode && h.IsWriter && !h.Expired).ToArray();
            Assert.DoesNotContain(activeWriters, h => h.Generation == generationA);
            if (b is null)
            {
                Assert.Empty(activeWriters);
            }
            else
            {
                HandleInspection only = Assert.Single(activeWriters);
                Assert.Equal(b.WriterGeneration, only.Generation);
            }
            await AssertUnreachableAsync(cluster, aUploads, "the expired writer's uploads are not published");

            // The replacement remains the valid writer.
            if (b is not null)
            {
                byte[] more = TestData.Bytes(0x0021_0004, 100);
                await WriteAtAsync(b, 0, more);
                await b.FlushAsync(Ct);
                model.Write(0, more);
                model.Flush();
                await AssertCommittedAsync(bv, path, model.Committed, "the replacement writer keeps publishing");
                await b.DisposeAsync();
            }
        }, Ct);

    // ------------------------------------------------------------------------------------------------------------
    // PUB-023
    // ------------------------------------------------------------------------------------------------------------

    private static readonly TimeSpan Deadline = TimeSpan.FromSeconds(3);
    private static readonly TimeSpan DeadlineSlack = TimeSpan.FromSeconds(2);

    /// <summary>(withheld prerequisite, publication path).</summary>
    public static TheoryData<Withheld, PublishPath> Pub023Cases => new()
    {
        { Withheld.MetadataQuorum, PublishPath.StreamFlush },
        { Withheld.MetadataQuorum, PublishPath.WriteAllHelper },
        { Withheld.RequiredZoneForStrongPolicy, PublishPath.StreamFlush },
        { Withheld.RequiredZoneForStrongPolicy, PublishPath.AppendHelper },
        { Withheld.RequiredDiskDurability, PublishPath.StreamCommit },
        { Withheld.RequiredDiskDurability, PublishPath.WriteAllHelper },
        { Withheld.RequiredDiskFull, PublishPath.StreamFlush },
    };

    [Theory]
    [MemberData(nameof(Pub023Cases))]
    [Catalog("PUB-023", "S", "INV-08", "INV-11", "INV-17")]
    public Task PUB_023_FlushDeadlineBoundsWaiting(Withheld withheld, PublishPath kind)
    {
        TestClusterOptions options = withheld switch
        {
            Withheld.MetadataQuorum => Deterministic(TestClusterOptions.FiveNodes),
            Withheld.RequiredZoneForStrongPolicy or Withheld.RequiredDiskFull =>
                Deterministic(TestClusterOptions.ThreeZones with { DefaultPolicy = StrongPolicy }),
            _ => Deterministic(),
        };
        DfsStoragePolicy policy = options.GetEffectiveDefaultPolicy();
        return ClusterTest.RunAsync(options, async cluster =>
        {
            const string path = "/pub023/f";
            byte[] initial = TestData.Bytes(0x0023_0001, 100_000);
            byte[] payload = TestData.Bytes(0x0023_0002, 150_000);
            await using DfsClient writerClient = await ConnectAsync(cluster, "c0", LargeBuffer, operationTimeout: Deadline);
            await using DfsClient readerClient = await ConnectAsync(cluster, "c1");
            DfsVolume wv = writerClient.GetVolume(Vol);
            DfsVolume rv = readerClient.GetVolume(Vol);
            Publication publication = await PreparePublicationAsync(wv, path, kind, initial, payload);
            DfsFileInfo before = await rv.Files.GetInfoAsync(path, Ct);
            await AssertDurableAsync(cluster, path, policy, "precondition: the committed revision meets the policy");
            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            TestNode[] others = cluster.Nodes.Where(n => n.Name != leader.Name).ToArray();

            var arms = new List<FaultArm>();
            switch (withheld)
            {
                case Withheld.MetadataQuorum:
                    // 3 of 5 voters unreachable: the leader plus one follower still offer two zones for data.
                    foreach (TestNode node in others.Take(3))
                        cluster.Network.Isolate(node.Name);
                    break;
                case Withheld.RequiredZoneForStrongPolicy:
                    cluster.Network.Isolate(others[0].Name);
                    break;
                case Withheld.RequiredDiskDurability:
                    foreach (TestNode node in others)
                        arms.Add(cluster.Faults.Arm(new FaultMatch(FaultSites.ObjectBeforeFlush, Node: node.Name, Operation: DfsOperationKind.PutObject, Count: 100_000), FaultAction.Block()));
                    break;
                case Withheld.RequiredDiskFull:
                    others[0].Disk.Fill();
                    break;
            }

            long start = cluster.Time.GetTimestamp();
            Exception? error = await CaptureWithinAsync(cluster, publication.RunAsync(Ct), Deadline + TimeSpan.FromSeconds(10),
                "a publication whose prerequisites are withheld must terminate");
            TimeSpan elapsed = cluster.Time.GetElapsedTime(start);
            foreach (FaultArm arm in arms)
                cluster.Faults.AssertReached(arm);

            Assert.True(error is not null, "a publication whose prerequisites are withheld must not succeed");
            Assert.True(elapsed <= Deadline + DeadlineSlack, $"the publication terminated after {elapsed}; its deadline was {Deadline}");
            Assert.False(error is OperationCanceledException, $"a deadline is not a caller cancellation: {error}");
            switch (withheld)
            {
                case Withheld.RequiredZoneForStrongPolicy when error is not DfsCommitOutcomeUnknownException:
                {
                    DfsPlacementUnavailableException placement = Assert.IsAssignableFrom<DfsPlacementUnavailableException>(error);
                    Assert.Equal(DfsErrorCode.PlacementUnavailable, placement.ErrorCode);
                    Assert.Equal(3, placement.RequiredCopies);
                    Assert.True(placement.AchievedCopies < 3, $"achieved copies {placement.AchievedCopies}");
                    Assert.Equal(3, placement.RequiredZones);
                    Assert.True(placement.AchievedZones < 3, $"achieved zones {placement.AchievedZones}");
                    break;
                }
                case Withheld.RequiredDiskFull:
                    Assert.True(error is DfsCapacityException or DfsPlacementUnavailableException or DfsCommitOutcomeUnknownException,
                        $"a full required disk must surface as capacity/placement unavailability: {error}");
                    break;
                case Withheld.RequiredDiskDurability:
                    Assert.True(error is DfsPlacementUnavailableException or DfsCommitOutcomeUnknownException or TimeoutException
                        || error is DfsException { ErrorCode: DfsErrorCode.Timeout },
                        $"withheld durability must surface as placement unavailability or a deadline: {error}");
                    break;
                case Withheld.MetadataQuorum:
                    Assert.True(error is DfsQuorumUnavailableException or DfsCommitOutcomeUnknownException or TimeoutException
                        || error is DfsException { ErrorCode: DfsErrorCode.Timeout },
                        $"missing quorum must surface as quorum unavailability, an unknown outcome or a deadline: {error}");
                    break;
            }

            // While the prerequisite stays withheld nothing is published under a weaker policy.
            await cluster.DelayAsync(TimeSpan.FromSeconds(3), Ct);
            if (withheld == Withheld.MetadataQuorum)
            {
                await AssertReplicasAtRevisionAsync(cluster, path, before.Revision, initial.LongLength, "no replica applies a publication without quorum");
            }
            else
            {
                await AssertCommittedAsync(rv, path, initial, "no publication under a relaxed policy", before.Revision);
                await AssertDurableAsync(cluster, path, policy, "the visible revision still meets the full policy");
            }

            // Restore the prerequisite (a full disk cannot be emptied, so that variant stays withheld).
            switch (withheld)
            {
                case Withheld.MetadataQuorum:
                case Withheld.RequiredZoneForStrongPolicy:
                    cluster.Network.Heal();
                    break;
                case Withheld.RequiredDiskDurability:
                    foreach (FaultArm arm in arms)
                    {
                        arm.Disarm();
                        arm.Release();
                    }
                    break;
            }
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), Ct);

            if (error is DfsCommitOutcomeUnknownException unknown)
            {
                await ResolveAndVerifyAsync(cluster, rv, publication, OperationIdFor(publication, unknown));
            }
            else
            {
                await AssertCommittedAsync(rv, path, initial, "a definite failure is never published later", before.Revision);
            }

            if (withheld == Withheld.RequiredDiskFull)
                return;

            // The policy was never relaxed: with prerequisites restored the same publication meets it in full.
            if (publication.Writer is not null)
            {
                await Record.ExceptionAsync(() => publication.Writer.AbortAsync(Ct));
                await Record.ExceptionAsync(async () => await publication.Writer.DisposeAsync());
            }
            await cluster.WaitUntilAsync(() => TryOpenWriterAsync(rv, path), TimeSpan.FromSeconds(40),
                "the failed writer's reservation must be released", Ct);
            await rv.Files.WriteAllBytesAsync(path, publication.Expected, Ct);
            await AssertCommittedAsync(rv, path, publication.Expected, "the publication succeeds once prerequisites are restored");
            await AssertDurableAsync(cluster, path, policy, "the restored publication meets the unrelaxed policy");
        }, Ct);
    }
}
