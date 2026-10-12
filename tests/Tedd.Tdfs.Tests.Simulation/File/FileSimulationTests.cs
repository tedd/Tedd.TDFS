using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Transport;
using static Tedd.Tdfs.Tests.Simulation.File.FileHelpers;

namespace Tedd.Tdfs.Tests.Simulation.File;

/// <summary>
/// FILE suite, deterministic simulation level: helper atomicity and idempotency under injected faults, concurrent
/// source changes and authority loss (test plan §7.7; FSD §§5.2–5.4, 6, 8, 13).
/// </summary>
public sealed class FileSimulationTests
{
    // ------------------------------------------------------------------------------------------------------------
    // FILE-002 WriteAllFailureLeavesOldFile
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Every pre-publication boundary of a multi-chunk WriteAll (4 chunks). Occurrence is the 1-based matching hit; the
    /// client upload site is hit once per chunk, so hits 1/2/4 are the first/interior/last chunk.
    /// </summary>
    private static FaultMatch File002Match(string stage) => stage switch
    {
        "F01-client-validated" => new(FaultSites.ClientValidated, Operation: DfsOperationKind.WriteAll, Count: Always),
        "F03-upload-first-chunk" => new(FaultSites.ClientUploadChunk, Occurrence: 1, Count: Always),
        "F03-upload-interior-chunk" => new(FaultSites.ClientUploadChunk, Occurrence: 2, Count: Always),
        "F03-upload-last-chunk" => new(FaultSites.ClientUploadChunk, Occurrence: 4, Count: Always),
        "F05-staging-registered" => new(FaultSites.StagingRegistered, Count: Always),
        "F06-object-write" => new(FaultSites.ObjectWrite, Count: Always),
        "F07-object-before-flush" => new(FaultSites.ObjectBeforeFlush, Count: Always),
        "F08-object-after-flush-before-receipt" => new(FaultSites.ObjectAfterFlushBeforeReceipt, Count: Always),
        "F04-commit-request-accepted" => new(FaultSites.RpcRequestAccepted, Operation: DfsOperationKind.WriteAll, Count: Always),
        "F09-receipt-validated" => new(FaultSites.CommitReceiptValidated, Count: Always),
        "F10-before-propose" => new(FaultSites.RaftBeforePropose, Operation: DfsOperationKind.WriteAll, Count: Always),
        _ => throw new ArgumentOutOfRangeException(nameof(stage), stage, null),
    };

    [Theory]
    [InlineData("F01-client-validated", 21UL)]
    [InlineData("F03-upload-first-chunk", 22UL)]
    [InlineData("F03-upload-interior-chunk", 23UL)]
    [InlineData("F03-upload-last-chunk", 24UL)]
    [InlineData("F05-staging-registered", 25UL)]
    [InlineData("F06-object-write", 26UL)]
    [InlineData("F07-object-before-flush", 27UL)]
    [InlineData("F08-object-after-flush-before-receipt", 28UL)]
    [InlineData("F04-commit-request-accepted", 29UL)]
    [InlineData("F09-receipt-validated", 30UL)]
    [InlineData("F10-before-propose", 31UL)]
    [Catalog("FILE-002", "S", "INV-02", "INV-07", "INV-08")]
    public async Task FILE_002_WriteAllFailureLeavesOldFile(string stage, ulong seed)
    {
        await ClusterTest.RunAsync(Sim(seed), async cluster =>
        {
            await using DfsClient writerClient = await cluster.ConnectAsync(Named("writer"), Ct);
            await using DfsClient observerClient = await cluster.ConnectAsync(Named("observer"), Ct);
            DfsVolume writer = writerClient.GetVolume(Vol);
            DfsVolume observer = observerClient.GetVolume(Vol);
            const string path = "/f002/target.bin";
            await writer.Directories.CreateDirectoryAsync("/f002", Ct);
            byte[] oldBytes = TestData.Bytes(seed, 2 * Chunk + 5);
            byte[] newBytes = TestData.Bytes(seed + 1000, 3 * Chunk + 100);
            await writer.Files.WriteAllBytesAsync(path, oldBytes, Ct);
            DfsFileInfo before = await observer.Files.GetInfoAsync(path, Ct);
            IReadOnlySet<ObjectId> oldObjects = await ObjectsOfAsync(cluster, path);
            await cluster.WaitForRepairAsync(Vol, RecoveryWait, Ct);

            var gate = new FaultGate(ctx => new IOException($"injected failure at {ctx.Site} (occurrence {ctx.Occurrence})"));
            FaultArm arm = cluster.Faults.Arm(File002Match(stage), gate.Action);
            DfsOperationId opId = await writer.NewOperationIdAsync(Ct);
            Task<CommitToken> write = writer.Files.WriteAllBytesAsync(path, newBytes, new DfsMutationOptions { OperationId = opId }, Ct);

            await arm.WaitTriggeredAsync(FaultWait);
            // Suspended exactly at the boundary: the destination is the complete old revision, not pre-truncated.
            (byte[] Bytes, InodeId Inode)? held = await TryReadSharedAsync(observer, path);
            Assert.True(held is not null, $"the destination vanished while WriteAll was held at {stage}");
            AssertBytes(oldBytes, held.Value.Bytes, $"observer while WriteAll is held at {stage}");
            Assert.Equal(before.InodeId, held.Value.Inode);
            DfsFileInfo heldInfo = await observer.Files.GetInfoAsync(path, Ct);
            Assert.Equal(before.Revision, heldInfo.Revision);
            Assert.Equal(oldBytes.Length, heldInfo.Length);
            gate.Release();

            Exception? failure = await Record.ExceptionAsync(() => write);
            cluster.Faults.AssertReached(arm);
            Assert.True(gate.Failures >= 1, "the injected failure must have been delivered");
            cluster.Faults.DisarmAll();

            Assert.True(failure is not null, $"WriteAll must not report success when every attempt fails at {stage}");
            Assert.False(failure is OperationCanceledException, "an injected failure is not a cancellation");
            await AssertResolvesAbortedAsync(writer, failure, opId);
            await WaitForNoHandlesAsync(cluster, before.InodeId, "a failed WriteAll must release its implicit writer reservation");

            // The old destination is intact on every path: bytes, identity, revision and extent map.
            await AssertFileAsync(observer, path, oldBytes, before.InodeId, $"destination after WriteAll failed at {stage}");
            Assert.Equal(before.Revision, (await observer.Files.GetInfoAsync(path, Ct)).Revision);
            Assert.True(oldObjects.SetEquals(await ObjectsOfAsync(cluster, path)), "the failed write must not change the published extent map");
            await cluster.WaitForReplicationAsync(Vol, Ct);
            await cluster.Inspect.AssertReplicasConvergedAsync(Vol);

            // The writer reservation was released: a later replacement succeeds atomically with the same inode.
            CommitToken ok = await writer.Files.WriteAllBytesAsync(path, newBytes, Ct);
            Assert.False(ok.IsNone);
            await AssertFileAsync(observer, path, newBytes, before.InodeId, "a later WriteAll succeeds");
        }, Ct);
    }

    [Theory]
    [InlineData("publish-size-limit", 41UL)]
    [InlineData("max-file-length", 42UL)]
    [InlineData("placement-unavailable", 43UL)]
    [Catalog("FILE-002", "S", "INV-02", "INV-08")]
    public async Task FILE_002_WriteAllFailureLeavesOldFile_ValidationRejected(string kind, ulong seed)
    {
        var strict = new DfsStoragePolicy { TargetCopies = 3, TargetDistinctZones = 3, DurableCopiesBeforeAck = 3, DurableZonesBeforeAck = 3 };
        DfsVolumeLimits limits = kind switch
        {
            "publish-size-limit" => new DfsVolumeLimits { MaxPublishBytes = 3 * Chunk },
            "max-file-length" => new DfsVolumeLimits { MaxFileLength = 4 * Chunk },
            _ => new DfsVolumeLimits(),
        };
        TestClusterOptions options = Sim(seed) with
        {
            Volumes =
            [
                new TestVolumeSpec(Vol)
                {
                    Limits = limits,
                    NamedPolicies = new Dictionary<string, DfsStoragePolicy>(StringComparer.Ordinal) { ["strict"] = strict },
                },
            ],
        };

        await ClusterTest.RunAsync(options, async cluster =>
        {
            await using DfsClient writerClient = await cluster.ConnectAsync(Named("writer"), Ct);
            await using DfsClient observerClient = await cluster.ConnectAsync(Named("observer"), Ct);
            DfsVolume writer = writerClient.GetVolume(Vol);
            DfsVolume observer = observerClient.GetVolume(Vol);
            const string path = "/f002v.bin";
            byte[] oldBytes = TestData.Bytes(seed, 2 * Chunk);
            await writer.Files.WriteAllBytesAsync(path, oldBytes, new DfsMutationOptions { StoragePolicy = kind == "placement-unavailable" ? "strict" : null }, Ct);
            DfsFileInfo before = await observer.Files.GetInfoAsync(path, Ct);
            await cluster.WaitForRepairAsync(Vol, RecoveryWait, Ct);

            byte[] newBytes = kind switch
            {
                "publish-size-limit" => TestData.Bytes(seed + 1, 3 * Chunk + 1),
                "max-file-length" => TestData.Bytes(seed + 1, 4 * Chunk + 1),
                _ => TestData.Bytes(seed + 1, 2 * Chunk + 7),
            };
            if (kind == "placement-unavailable")
                await cluster[2].CrashAsync();

            DfsOperationId opId = await writer.NewOperationIdAsync(Ct);
            Exception? failure = await Record.ExceptionAsync(() => writer.Files.WriteAllBytesAsync(path, newBytes, new DfsMutationOptions { OperationId = opId }, Ct));
            switch (kind)
            {
                case "publish-size-limit":
                case "max-file-length":
                    Assert.True(failure is DfsLimitExceededException { ErrorCode: DfsErrorCode.LimitExceeded }, $"expected LimitExceeded, got {failure}");
                    var limit = (DfsLimitExceededException)failure!;
                    Assert.Equal(newBytes.Length, limit.Requested);
                    Assert.Equal(kind == "publish-size-limit" ? 3L * Chunk : 4L * Chunk, limit.Limit);
                    break;
                default:
                    Assert.True(failure is DfsPlacementUnavailableException { ErrorCode: DfsErrorCode.PlacementUnavailable },
                        $"ack 3 copies in 3 zones with one zone down must be PlacementUnavailable, got {failure}");
                    var placement = (DfsPlacementUnavailableException)failure!;
                    Assert.Equal(3, placement.RequiredCopies);
                    Assert.Equal(3, placement.RequiredZones);
                    Assert.True(placement.AchievedCopies <= 2, $"only two hosts are alive (achieved {placement.AchievedCopies})");
                    Assert.True(placement.AchievedZones <= 2, $"only two zones are alive (achieved {placement.AchievedZones})");
                    break;
            }

            await AssertResolvesAbortedAsync(writer, failure!, opId);
            await AssertFileAsync(observer, path, oldBytes, before.InodeId, $"destination after {kind} rejection");
            Assert.Equal(before.Revision, (await observer.Files.GetInfoAsync(path, Ct)).Revision);

            if (kind == "placement-unavailable")
            {
                // Policy is not silently weakened; once the zone returns the same replacement succeeds.
                await cluster[2].RestartAsync();
                await cluster.WaitForStableAsync(RecoveryWait, Ct);
                await writer.Files.WriteAllBytesAsync(path, newBytes, Ct);
                await AssertFileAsync(observer, path, newBytes, before.InodeId, "after the third zone returned");
            }
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // FILE-008 AppendHelperRetryIsIdempotent
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>How the append's response is lost.</summary>
    public enum AppendLoss
    {
        /// <summary>One reply is dropped (F13); the SDK may retry internally with the same id.</summary>
        ReplyDroppedOnce,

        /// <summary>Every reply is dropped; the client process dies; a new client queries and retries the persisted id.</summary>
        AllRepliesDroppedThenNewClient,

        /// <summary>The leader crashes after applying the append and before replying (F12).</summary>
        LeaderCrashAfterApply,

        /// <summary>The append request is duplicated on the wire.</summary>
        DuplicatedRequest,
    }

    [Theory]
    [InlineData(AppendLoss.ReplyDroppedOnce, 100, 81UL)]
    [InlineData(AppendLoss.ReplyDroppedOnce, Chunk + 17, 82UL)]
    [InlineData(AppendLoss.AllRepliesDroppedThenNewClient, 100, 83UL)]
    [InlineData(AppendLoss.AllRepliesDroppedThenNewClient, 2 * Chunk + 1, 84UL)]
    [InlineData(AppendLoss.LeaderCrashAfterApply, 100, 85UL)]
    [InlineData(AppendLoss.LeaderCrashAfterApply, Chunk + 17, 86UL)]
    [InlineData(AppendLoss.DuplicatedRequest, 100, 87UL)]
    [InlineData(AppendLoss.DuplicatedRequest, Chunk + 17, 88UL)]
    [Catalog("FILE-008", "S", "INV-02", "INV-06", "INV-07")]
    public async Task FILE_008_AppendHelperRetryIsIdempotent(AppendLoss loss, int suffixLength, ulong seed)
    {
        await ClusterTest.RunAsync(Sim(seed), async cluster =>
        {
            DfsClient first = await cluster.ConnectAsync(Named("c1"), Ct);
            DfsVolume v1 = first.GetVolume(Vol);
            const string path = "/f008.log";
            byte[] prefix = TestData.Bytes(seed, 3000);
            byte[] suffix = TestData.Bytes(seed + 1, suffixLength);
            byte[] otherSuffix = TestData.Bytes(seed + 2, suffixLength);
            byte[] expected = Concat(prefix, suffix);
            await v1.Files.WriteAllBytesAsync(path, prefix, Ct);
            InodeId inode = (await v1.Files.GetInfoAsync(path, Ct)).InodeId;

            // The application persists K before submitting the append.
            DfsOperationId k = await v1.NewOperationIdAsync(Ct);
            var withK = new DfsMutationOptions { OperationId = k };
            DfsVolume resolver = v1;
            DfsClient? second = null;
            TestNode? crashed = null;
            CommitToken? observedToken = null;

            switch (loss)
            {
                case AppendLoss.ReplyDroppedOnce:
                {
                    FaultArm arm = cluster.Faults.Arm(new FaultMatch(FaultSites.RpcReplySend, Operation: DfsOperationKind.Append), FaultAction.DropReply());
                    Exception? failure = null;
                    try
                    {
                        observedToken = await v1.Files.AppendAllBytesAsync(path, suffix, withK, Ct);
                    }
                    catch (DfsCommitOutcomeUnknownException ex)
                    {
                        failure = ex;
                        Assert.Equal(k, ex.OperationId);
                    }

                    cluster.Faults.AssertReached(arm);
                    if (failure is not null)
                        observedToken = await v1.Files.AppendAllBytesAsync(path, suffix, withK, Ct);
                    break;
                }

                case AppendLoss.AllRepliesDroppedThenNewClient:
                {
                    FaultArm arm = cluster.Faults.Arm(new FaultMatch(FaultSites.RpcReplySend, Operation: DfsOperationKind.Append, Count: Always), FaultAction.DropReply());
                    DfsCommitOutcomeUnknownException unknown = await AssertEx.ThrowsDfsAsync<DfsCommitOutcomeUnknownException>(
                        () => v1.Files.AppendAllBytesAsync(path, suffix, withK, Ct), DfsErrorCode.OutcomeUnknown);
                    Assert.Equal(k, unknown.OperationId);
                    cluster.Faults.AssertReached(arm);
                    cluster.KillClient(first);
                    cluster.Faults.DisarmAll();
                    second = await cluster.ConnectAsync(Named("c2"), Ct);
                    resolver = second.GetVolume(Vol);
                    DfsOperationOutcome queried = await resolver.QueryOperationAsync(k, resolve: false, Ct);
                    Assert.Equal(DfsOutcomeStatus.Committed, queried.Status); // the first client's uncertainty was not a rollback
                    observedToken = await resolver.Files.AppendAllBytesAsync(path, suffix, withK, Ct);
                    Assert.Equal(queried.Token, observedToken);
                    break;
                }

                case AppendLoss.LeaderCrashAfterApply:
                {
                    TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
                    crashed = leader;
                    FaultArm arm = cluster.Faults.Arm(new FaultMatch(FaultSites.ApplyAfterBeforeReply, Node: leader.Name, Operation: DfsOperationKind.Append),
                        FaultAction.CrashNode(CrashMode.LoseUnflushed));
                    try
                    {
                        observedToken = await v1.Files.AppendAllBytesAsync(path, suffix, withK, Ct);
                    }
                    catch (DfsCommitOutcomeUnknownException ex)
                    {
                        Assert.Equal(k, ex.OperationId);
                        DfsOperationOutcome resolved = await v1.QueryOperationAsync(k, resolve: true, Ct);
                        Assert.Equal(DfsOutcomeStatus.Committed, resolved.Status);
                        observedToken = await v1.Files.AppendAllBytesAsync(path, suffix, withK, Ct);
                        Assert.Equal(resolved.Token, observedToken);
                    }

                    cluster.Faults.AssertReached(arm);
                    TestNode newLeader = await cluster.GetLeaderAsync(Vol, Ct);
                    Assert.NotEqual(leader.Name, newLeader.Name);
                    break;
                }

                case AppendLoss.DuplicatedRequest:
                {
                    MessageRule rule = cluster.Network.DuplicateNext(new MessageFilter(Type: MessageType.Append, Direction: LinkDirection.Request), 1);
                    observedToken = await v1.Files.AppendAllBytesAsync(path, suffix, withK, Ct);
                    Assert.Equal(1, rule.Matched);
                    break;
                }
            }

            // Exactly one suffix: P + S, never P + S + S.
            await AssertFileAsync(resolver, path, expected, inode, $"append with lost response ({loss})");
            DfsOperationOutcome outcome = await resolver.QueryOperationAsync(k, resolve: false, Ct);
            Assert.Equal(DfsOutcomeStatus.Committed, outcome.Status);
            Assert.NotNull(observedToken);
            Assert.Equal(outcome.Token, observedToken.Value);

            // A further retry returns the original result without a second effect.
            Assert.Equal(outcome.Token, await resolver.Files.AppendAllBytesAsync(path, suffix, withK, Ct));
            await AssertFileAsync(resolver, path, expected, inode, "after an extra retry with the same id");

            // Reusing K with a different payload is rejected and changes nothing.
            await AssertEx.ThrowsDfsAsync<DfsOperationIdMismatchException>(
                () => resolver.Files.AppendAllBytesAsync(path, otherSuffix, withK, Ct), DfsErrorCode.OperationIdMismatch);
            await AssertFileAsync(resolver, path, expected, inode, "after changed-payload reuse of the id");

            if (crashed is not null)
            {
                await crashed.RestartAsync();
                await cluster.WaitForReplicationAsync(Vol, Ct);
                VolumeStateInspection restarted = await cluster.Inspect.GetVolumeStateAsync(Vol, crashed.Name);
                Assert.Equal(expected.Length, restarted.GetFile(path)!.Length);
                await cluster.Inspect.AssertReplicasConvergedAsync(Vol);
            }

            if (second is not null)
                await second.DisposeAsync();
            else
                await first.DisposeAsync();
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // FILE-009 CopyPinsSourceVersion
    // ------------------------------------------------------------------------------------------------------------

    [Theory]
    [InlineData(false, false, 91UL)]
    [InlineData(true, false, 92UL)]
    [InlineData(false, true, 93UL)]
    [InlineData(true, true, 94UL)]
    [InlineData(true, false, 95UL)]
    [InlineData(true, true, 96UL)]
    [Catalog("FILE-009", "S", "INV-02", "INV-03", "INV-07")]
    public async Task FILE_009_CopyPinsSourceVersion(bool multiExtent, bool overwriteExistingDestination, ulong seed)
    {
        await ClusterTest.RunAsync(Sim(seed), async cluster =>
        {
            await using DfsClient writerClient = await cluster.ConnectAsync(Named("writer"), Ct);
            await using DfsClient copierClient = await cluster.ConnectAsync(Named("copier"), Ct);
            DfsVolume writer = writerClient.GetVolume(Vol);
            DfsVolume copier = copierClient.GetVolume(Vol);
            const string src = "/f009/src.bin";
            await writer.Directories.CreateDirectoryAsync("/f009", Ct);
            int length = multiExtent ? 4 * Chunk + 3 : 600;
            byte[] current = TestData.Bytes(seed, length);
            await writer.Files.WriteAllBytesAsync(src, current, Ct);
            InodeId srcInode = (await writer.Files.GetInfoAsync(src, Ct)).InodeId;

            // revisions[i] = bytes of the i-th committed source revision; started/published count revisions whose
            // publication was invoked / has returned (revision 0 is the initial one).
            var revisions = new List<byte[]> { (byte[])current.Clone() };
            int started = 1;
            int published = 1;
            InodeId? fixedDestination = null;
            if (overwriteExistingDestination)
            {
                await copier.Files.WriteAllBytesAsync("/f009/dst.bin", TestData.Bytes(seed + 77, 1234), Ct);
                fixedDestination = (await copier.Files.GetInfoAsync("/f009/dst.bin", Ct)).InodeId;
            }

            await using DfsStream stream = await writer.Files.OpenAsync(src, FileMode.Open, FileAccess.ReadWrite, FileShare.Read, Ct);

            async Task PublishRevisionAsync(int k)
            {
                byte[] patch = TestData.Bytes(seed + 10_000 + (ulong)k, multiExtent ? 1000 : 100);
                int offset = multiExtent ? (k % 4) * Chunk + 500 : (k * 50) % 500;
                stream.Position = offset;
                await stream.WriteAsync(patch, Ct);
                patch.CopyTo(current, offset);
                started++;
                await stream.FlushAsync(Ct);
                revisions.Add((byte[])current.Clone());
                published++;
            }

            var copies = new List<(string Path, byte[] Bytes)>();

            async Task CopyOnceAsync(int c)
            {
                string dst = overwriteExistingDestination ? "/f009/dst.bin" : $"/f009/dst{c}.bin";
                int lower = published - 1;
                CommitToken token = await copier.Files.CopyAsync(src, dst, overwriteExistingDestination, Ct);
                int upper = started - 1;
                Assert.False(token.IsNone);
                byte[] copied = await copier.Files.ReadAllBytesAsync(dst, Ct);
                int match = revisions.FindIndex(r => r.AsSpan().SequenceEqual(copied));
                Assert.True(match >= 0, $"copy {c} produced a destination that is not any complete source revision (composite of several versions?)");
                Assert.True(match >= lower && match <= upper,
                    $"copy {c} returned source revision {match}, outside the revisions legal for its real-time interval [{lower}, {upper}]");
                DfsFileInfo dstInfo = await copier.Files.GetInfoAsync(dst, Ct);
                Assert.NotEqual(srcInode, dstInfo.InodeId);
                if (fixedDestination is { } fixedId)
                    Assert.Equal(fixedId, dstInfo.InodeId);
                copies.Add((dst, copied));
            }

            // Force one overlap: the first copy is held after the server accepted it while a new revision publishes.
            var gate = new FaultGate();
            FaultArm arm = cluster.Faults.Arm(new FaultMatch(FaultSites.RpcRequestAccepted, Operation: DfsOperationKind.Copy), gate.Action);
            Task heldCopy = CopyOnceAsync(0);
            await arm.WaitTriggeredAsync(FaultWait);
            await PublishRevisionAsync(1);
            gate.Release();
            await heldCopy;
            cluster.Faults.AssertReached(arm);

            // Then free-running concurrent publication and copies.
            async Task WriterLoopAsync()
            {
                for (int k = 2; k <= 7; k++)
                    await PublishRevisionAsync(k);
            }

            async Task CopierLoopAsync()
            {
                for (int c = 1; c <= 6; c++)
                    await CopyOnceAsync(c);
            }

            await Task.WhenAll(WriterLoopAsync(), CopierLoopAsync());

            // Later source changes never leak into earlier copies (objects are shared but immutable).
            await PublishRevisionAsync(8);
            await stream.DisposeAsync(); // release the writer handle before whole-file reads of the source
            if (!overwriteExistingDestination)
            {
                foreach ((string dst, byte[] bytes) in copies)
                    AssertBytes(bytes, await copier.Files.ReadAllBytesAsync(dst, Ct), $"{dst} after later source changes");
            }
            else
            {
                AssertBytes(copies[^1].Bytes, await copier.Files.ReadAllBytesAsync("/f009/dst.bin", Ct), "destination after later source changes");
            }

            AssertBytes(current, await copier.Files.ReadAllBytesAsync(src, Ct), "source holds the latest revision");
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // FILE-010 CopyFailureIsAtomic
    // ------------------------------------------------------------------------------------------------------------

    [Theory]
    [InlineData("F01-client-validated-throw", false, 101UL)]
    [InlineData("F01-client-validated-throw", true, 102UL)]
    [InlineData("F04-request-accepted-throw", false, 103UL)]
    [InlineData("F04-request-accepted-throw", true, 104UL)]
    [InlineData("F10-before-propose-throw", true, 105UL)]
    [InlineData("F10-after-local-append-leader-crash", false, 106UL)]
    [InlineData("F10-after-local-append-leader-crash", true, 107UL)]
    [InlineData("F11-after-commit-leader-crash", true, 108UL)]
    [InlineData("F13-all-replies-dropped", true, 109UL)]
    [InlineData("F03-request-send-cancel", true, 110UL)]
    [Catalog("FILE-010", "S", "INV-02", "INV-06", "INV-07")]
    public async Task FILE_010_CopyFailureIsAtomic(string interruption, bool destinationExists, ulong seed)
    {
        await ClusterTest.RunAsync(Sim(seed), async cluster =>
        {
            await using DfsClient copierClient = await cluster.ConnectAsync(Named("copier"), Ct);
            await using DfsClient observerClient = await cluster.ConnectAsync(Named("observer"), Ct);
            DfsVolume copier = copierClient.GetVolume(Vol);
            DfsVolume observer = observerClient.GetVolume(Vol);
            const string src = "/f010/src.bin";
            const string dst = "/f010/dst.bin";
            await copier.Directories.CreateDirectoryAsync("/f010", Ct);
            byte[] source = TestData.Bytes(seed, 3 * Chunk + 9);
            byte[]? oldDest = destinationExists ? TestData.Bytes(seed + 1, Chunk + 1) : null;
            await copier.Files.WriteAllBytesAsync(src, source, Ct);
            InodeId? dstInode = null;
            if (oldDest is not null)
            {
                await copier.Files.WriteAllBytesAsync(dst, oldDest, Ct);
                dstInode = (await copier.Files.GetInfoAsync(dst, Ct)).InodeId;
            }

            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(Ct);
            FaultGate? gate = null;
            FaultArm arm;
            switch (interruption)
            {
                case "F01-client-validated-throw":
                    gate = new FaultGate(ctx => new IOException("injected client failure"));
                    arm = cluster.Faults.Arm(new FaultMatch(FaultSites.ClientValidated, Operation: DfsOperationKind.Copy, Count: Always), gate.Action);
                    break;
                case "F04-request-accepted-throw":
                    gate = new FaultGate(ctx => new IOException("injected server failure"));
                    arm = cluster.Faults.Arm(new FaultMatch(FaultSites.RpcRequestAccepted, Operation: DfsOperationKind.Copy, Count: Always), gate.Action);
                    break;
                case "F10-before-propose-throw":
                    gate = new FaultGate(ctx => new IOException("injected proposal failure"));
                    arm = cluster.Faults.Arm(new FaultMatch(FaultSites.RaftBeforePropose, Operation: DfsOperationKind.Copy, Count: Always), gate.Action);
                    break;
                case "F10-after-local-append-leader-crash":
                    arm = cluster.Faults.Arm(new FaultMatch(FaultSites.RaftAfterLocalAppend, Node: leader.Name, Operation: DfsOperationKind.Copy), FaultAction.CrashNode());
                    break;
                case "F11-after-commit-leader-crash":
                    arm = cluster.Faults.Arm(new FaultMatch(FaultSites.RaftAfterCommitBeforeApply, Node: leader.Name, Operation: DfsOperationKind.Copy), FaultAction.CrashNode());
                    break;
                case "F13-all-replies-dropped":
                    arm = cluster.Faults.Arm(new FaultMatch(FaultSites.RpcReplySend, Operation: DfsOperationKind.Copy, Count: Always), FaultAction.DropReply());
                    break;
                case "F03-request-send-cancel":
                    arm = cluster.Faults.Arm(new FaultMatch(FaultSites.ClientRequestSend, Operation: DfsOperationKind.Copy), FaultAction.CancelCaller());
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(interruption));
            }

            // Observer: every read is the old destination/absence or the complete source, linearizably.
            var copyDone = new TaskCompletionSource();
            async Task ObserveAsync()
            {
                int afterDone = 0;
                for (int i = 0; i < 400 && afterDone < 3; i++)
                {
                    if (copyDone.Task.IsCompleted)
                        afterDone++;
                    long id = cluster.History.Invoke("observer", "read", dst, null);
                    try
                    {
                        byte[]? seen = (await TryReadSharedAsync(observer, dst))?.Bytes;
                        cluster.History.Complete(id, OpOutcome.Ok, seen);
                        bool legal = seen is null ? oldDest is null
                            : seen.AsSpan().SequenceEqual(source) || (oldDest is not null && seen.AsSpan().SequenceEqual(oldDest));
                        Assert.True(legal, $"observer saw a partial or foreign destination ({seen?.Length.ToString(System.Globalization.CultureInfo.InvariantCulture) ?? "absent"})");
                    }
                    catch (Exception ex) when (ex is DfsUnavailableException or TimeoutException
                        or DfsException { IsRetryable: true }
                        or DfsException { ErrorCode: DfsErrorCode.NotLeader or DfsErrorCode.NodeUnavailable or DfsErrorCode.Unavailable or DfsErrorCode.Timeout })
                    {
                        cluster.History.Complete(id, OpOutcome.Failed, ex); // a failed read has no effect
                        await cluster.DelayAsync(TimeSpan.FromMilliseconds(50), Ct);
                    }
                }
            }

            Task observing = ObserveAsync();
            DfsOperationId opId = await copier.NewOperationIdAsync(Ct);
            long copyOp = cluster.History.Invoke("copier", "write", dst, source, opId);
            Task<CommitToken> copy = copier.Files.CopyAsync(src, dst, true, new DfsMutationOptions { OperationId = opId }, cts.Token);
            if (gate is not null)
            {
                await arm.WaitTriggeredAsync(FaultWait);
                await AssertDestinationAsync(oldDest, "while the copy is held before publication");
                gate.Release();
            }

            Exception? failure = await Record.ExceptionAsync(() => copy);
            cluster.Faults.AssertReached(arm);
            cluster.Faults.DisarmAll();

            bool committed;
            if (failure is null)
            {
                committed = true;
                cluster.History.Complete(copyOp, OpOutcome.Ok, await copy);
            }
            else
            {
                cluster.History.Complete(copyOp, failure is DfsCommitOutcomeUnknownException or OperationCanceledException ? OpOutcome.Unknown : OpOutcome.Failed, failure);
                if (failure is DfsCommitOutcomeUnknownException unknown)
                    Assert.Equal(opId, unknown.OperationId);
                await cluster.WaitForStableAsync(RecoveryWait, Ct);
                DfsOperationOutcome resolved = await copier.QueryOperationAsync(opId, resolve: true, Ct);
                Assert.True(resolved.Status is DfsOutcomeStatus.Committed or DfsOutcomeStatus.Aborted, $"resolve must be definite, got {resolved.Status}");
                committed = resolved.Status == DfsOutcomeStatus.Committed;
                if (cluster.History.Get(copyOp).Outcome == OpOutcome.Unknown)
                    cluster.History.Resolve(copyOp, committed ? OpOutcome.Ok : OpOutcome.Failed);
                if (failure is OperationCanceledException)
                    Assert.False(committed, "cancellation may only be reported when the copy was not published");
                if (failure is not (DfsCommitOutcomeUnknownException or OperationCanceledException))
                    Assert.False(committed, $"a definite failure ({failure.GetType().Name}) must not hide a committed copy (INV-06)");
                if (interruption.EndsWith("-throw", StringComparison.Ordinal))
                    Assert.False(committed, "an interruption before publication must leave the copy unpublished");
            }

            copyDone.SetResult();
            await observing;
            await AssertDestinationAsync(committed ? source : oldDest, committed ? "committed copy is complete" : "interrupted copy left the old destination");
            if (dstInode is { } preserved && committed)
                Assert.Equal(preserved, (await observer.Files.GetInfoAsync(dst, Ct)).InodeId);
            AssertBytes(source, await observer.Files.ReadAllBytesAsync(src, Ct), "the source is never modified by a copy");
            LinearizabilityChecker.Check(cluster.History.Operations, new RegisterModel(oldDest)).AssertValid();

            foreach (TestNode node in cluster.Nodes.Where(n => n.State == TestNodeState.Crashed).ToList())
                await node.RestartAsync();
            await cluster.WaitForStableAsync(RecoveryWait, Ct);
            await cluster.WaitForReplicationAsync(Vol, Ct);
            await cluster.Inspect.AssertReplicasConvergedAsync(Vol);

            async Task AssertDestinationAsync(byte[]? expected, string because)
            {
                (byte[] Bytes, InodeId Inode)? seen = await TryReadSharedAsync(observer, dst);
                if (expected is null)
                {
                    Assert.True(seen is null, $"{because}: destination must be absent");
                    return;
                }

                Assert.True(seen is not null, $"{because}: destination must exist");
                AssertBytes(expected, seen.Value.Bytes, because);
                if (dstInode is { } id)
                    Assert.Equal(id, seen.Value.Inode);
            }
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // FILE-014 ExistsDistinguishesAbsenceFromFailure
    // ------------------------------------------------------------------------------------------------------------

    [Theory]
    [InlineData("absent-and-present", 141UL)]
    [InlineData("denied-no-grant", 142UL)]
    [InlineData("client-isolated", 143UL)]
    [InlineData("quorum-lost", 144UL)]
    [InlineData("leader-isolated-with-client", 145UL)]
    [InlineData("integrity-error", 146UL)]
    [InlineData("internal-error", 147UL)]
    [Catalog("FILE-014", "S", "INV-08", "INV-12")]
    public async Task FILE_014_ExistsDistinguishesAbsenceFromFailure(string condition, ulong seed)
    {
        TestClusterOptions options = Sim(seed) with { Volumes = [new TestVolumeSpec(Vol), new TestVolumeSpec("other")] };
        await ClusterTest.RunAsync(options, async cluster =>
        {
            await using DfsClient admin = await cluster.ConnectAsync(Named("admin"), Ct);
            DfsVolume av = admin.GetVolume(Vol);
            await av.Directories.CreateDirectoryAsync("/f014/dir", Ct);
            await av.Files.WriteAllBytesAsync("/f014/present.bin", TestData.Bytes(seed, 100), Ct);
            const string presentFile = "/f014/present.bin";
            const string absentFile = "/f014/absent.bin";
            const string presentDir = "/f014/dir";
            const string absentDir = "/f014/nodir";

            async Task<Exception?[]> ProbeAllAsync(DfsVolume v) =>
            [
                await Record.ExceptionAsync(() => v.Files.ExistsAsync(presentFile, Ct)),
                await Record.ExceptionAsync(() => v.Files.ExistsAsync(absentFile, Ct)),
                await Record.ExceptionAsync(() => v.Directories.ExistsAsync(presentDir, Ct)),
                await Record.ExceptionAsync(() => v.Directories.ExistsAsync(absentDir, Ct)),
            ];

            async Task AssertTruthfulAsync(DfsVolume v, string because)
            {
                Assert.True(await v.Files.ExistsAsync(presentFile, Ct), because);
                Assert.False(await v.Files.ExistsAsync(absentFile, Ct), because);
                Assert.False(await v.Files.ExistsAsync("/f014/nodir/deeper.bin", Ct), because);
                Assert.True(await v.Directories.ExistsAsync(presentDir, Ct), because);
                Assert.False(await v.Directories.ExistsAsync(absentDir, Ct), because);
            }

            static void AssertAllFailed(Exception?[] probes, Func<Exception, bool> expected, string what)
            {
                string[] names = ["file present", "file absent", "dir present", "dir absent"];
                for (int i = 0; i < probes.Length; i++)
                {
                    Assert.True(probes[i] is not null, $"{what}: Exists({names[i]}) returned an answer instead of propagating the failure");
                    Assert.False(probes[i] is FileNotFoundException or DirectoryNotFoundException, $"{what}: failure surfaced as absence");
                    Assert.True(expected(probes[i]!), $"{what}: Exists({names[i]}) surfaced {probes[i]}");
                }
            }

            static bool IsAvailabilityFailure(Exception ex) =>
                ex is DfsUnavailableException or TimeoutException
                    || ex is DfsException { ErrorCode: DfsErrorCode.Timeout or DfsErrorCode.NodeUnavailable or DfsErrorCode.Unavailable or DfsErrorCode.QuorumUnavailable or DfsErrorCode.NotLeader };

            // Liveness after the prerequisite is restored: bounded wait until Exists answers again.
            Task WaitAnswersAsync(DfsVolume v, string because) => cluster.WaitUntilAsync(async () =>
            {
                try
                {
                    return await v.Files.ExistsAsync(presentFile, Ct);
                }
                catch (Exception ex) when (IsAvailabilityFailure(ex))
                {
                    return false;
                }
            }, RecoveryWait, because, Ct);

            var fastTimeout = TimeSpan.FromSeconds(3);
            switch (condition)
            {
                case "absent-and-present":
                    await AssertTruthfulAsync(av, "healthy cluster");
                    break;

                case "denied-no-grant":
                {
                    TestPrincipal eve = await cluster.CreatePrincipalAsync("eve", Grant(DfsPermissions.Read | DfsPermissions.Write, "other"));
                    await using DfsClient eveClient = await cluster.ConnectAsync(Named("eve", eve.Credentials), Ct);
                    AssertAllFailed(await ProbeAllAsync(eveClient.GetVolume(Vol)), ex => ex is UnauthorizedAccessException, "principal without a grant");
                    break;
                }

                case "client-isolated":
                {
                    var probeOptions = Named("probe", operationTimeout: fastTimeout);
                    await using DfsClient probe = await cluster.ConnectAsync(probeOptions, Ct);
                    await AssertTruthfulAsync(probe.GetVolume(Vol), "before isolation");
                    cluster.Network.Isolate(probeOptions.NetworkName);
                    AssertAllFailed(await ProbeAllAsync(probe.GetVolume(Vol)), IsAvailabilityFailure, "isolated client");
                    cluster.Network.Heal();
                    await WaitAnswersAsync(probe.GetVolume(Vol), "the client recovers after the partition heals");
                    await AssertTruthfulAsync(probe.GetVolume(Vol), "after heal");
                    break;
                }

                case "quorum-lost":
                {
                    var probeOptions = Named("probe", operationTimeout: fastTimeout) with { SeedNodes = [cluster[0].Name] };
                    await using DfsClient probe = await cluster.ConnectAsync(probeOptions, Ct);
                    await cluster[1].CrashAsync();
                    await cluster[2].CrashAsync();
                    AssertAllFailed(await ProbeAllAsync(probe.GetVolume(Vol)), IsAvailabilityFailure, "metadata quorum lost");
                    await cluster[1].RestartAsync();
                    await cluster[2].RestartAsync();
                    await cluster.WaitForStableAsync(RecoveryWait, Ct);
                    await WaitAnswersAsync(probe.GetVolume(Vol), "Exists answers again once the quorum is restored");
                    await AssertTruthfulAsync(probe.GetVolume(Vol), "after quorum restored");
                    break;
                }

                case "leader-isolated-with-client":
                {
                    TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
                    var probeOptions = Named("probe", operationTimeout: fastTimeout) with { SeedNodes = [leader.Name] };
                    await using DfsClient probe = await cluster.ConnectAsync(probeOptions, Ct);
                    await AssertTruthfulAsync(probe.GetVolume(Vol), "before partition");
                    string[] others = cluster.Nodes.Where(n => n.Name != leader.Name).Select(n => n.Name).ToArray();
                    cluster.Network.Partition([leader.Name, probeOptions.NetworkName], others);
                    // The stale leader cannot complete a ReadIndex round: it must not answer from local state.
                    AssertAllFailed(await ProbeAllAsync(probe.GetVolume(Vol)), IsAvailabilityFailure, "client partitioned with a deposed leader");
                    cluster.Network.Heal();
                    await cluster.WaitForStableAsync(RecoveryWait, Ct);
                    await WaitAnswersAsync(probe.GetVolume(Vol), "Exists answers again after the partition heals");
                    await AssertTruthfulAsync(probe.GetVolume(Vol), "after heal");
                    await cluster.Inspect.AssertNoSplitBrainAsync();
                    break;
                }

                case "integrity-error":
                case "internal-error":
                {
                    int injected = 0;
                    FaultArm arm = cluster.Faults.Arm(new FaultMatch(FaultSites.RpcRequestAccepted, Count: Always), FaultAction.Custom(ctx =>
                    {
                        bool metadataRead = ctx.Operation is DfsOperationKind.Exists or DfsOperationKind.GetInfo or DfsOperationKind.Enumerate;
                        bool ours = ctx.Path is null || ctx.Path.StartsWith("/f014", StringComparison.Ordinal);
                        if (!metadataRead || !ours)
                            return Task.CompletedTask;
                        injected++;
                        throw condition == "integrity-error"
                            ? new DfsIntegrityException("injected metadata integrity failure")
                            : new DfsException(DfsErrorCode.Internal, "injected internal failure");
                    }));
                    Exception?[] probes = await ProbeAllAsync(av);
                    cluster.Faults.AssertReached(arm);
                    Assert.True(injected >= probes.Length, "every Exists probe must have reached the injected failure");
                    cluster.Faults.DisarmAll();
                    if (condition == "integrity-error")
                        AssertAllFailed(probes, ex => ex is DfsIntegrityException { ErrorCode: DfsErrorCode.Integrity }, "metadata integrity failure");
                    else
                        AssertAllFailed(probes, ex => ex is DfsException { ErrorCode: DfsErrorCode.Internal }, "internal server failure");
                    await AssertTruthfulAsync(av, "after the injected failure is removed");
                    break;
                }

                default:
                    throw new ArgumentOutOfRangeException(nameof(condition));
            }
        }, Ct);
    }
}
