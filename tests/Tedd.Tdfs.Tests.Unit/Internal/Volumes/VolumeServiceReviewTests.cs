using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Server.Volumes.Service;
using Tedd.Tdfs.Tests.Unit.Internal.Runtime;

namespace Tedd.Tdfs.Tests.Unit.Internal.Volumes;

/// <summary>
/// Adversarial service-level cases found in review, on a real multi-node runtime cluster with raw protocol messages:
/// authoritative refusals, snapshot reads of named streams, leases of handles whose reply is held, post-decision cleanup
/// fault points, inherited policies of created parents, append handles, racing creating appends and the per-stream
/// publication lock.
/// </summary>
public sealed class VolumeServiceReviewTests
{
    private static async Task<(RuntimeCluster Cluster, VolumeClient Client)> StartAsync(ulong seed, IFaultInjector? faults = null)
    {
        var cluster = new RuntimeCluster(3, seed: seed) { VolumeStateMachines = null };
        if (faults is not null)
            cluster.Faults = faults;
        await cluster.StartAllAsync();
        await cluster.WaitForRegistrationAsync();
        await cluster.CreateVolumeAsync("vol");
        var client = new VolumeClient(cluster, "vol");
        await client.OpenSessionAsync();
        return (cluster, client);
    }

    private static WriteAllMessage Write(VolumeClient c, string path, byte[] bytes, bool createParents = false)
        => new(c.Op(), path, bytes.Length, [RangeChange.ForInline(0, bytes)], [], default, null, null, createParents, 0);

    [Fact]
    public async Task ARefusedMutation_IsTheIdsDefiniteOutcome_ADelayedCopyNeverCommitsLater()
    {
        (RuntimeCluster cluster, VolumeClient c) = await StartAsync(seed: 701);
        await using RuntimeCluster _ = cluster;
        await c.OkAsync<WriteAllResultMessage>(Write(c, "/taken", [1]));

        // CreateNew on an existing name is refused ...
        OpenHandleMessage create = c.OpenRequest("/taken", FileMode.CreateNew, FileAccess.Write, FileShare.Read);
        await c.FailsAsync(DfsErrorCode.AlreadyExists, create);

        // ... and that refusal is the id's durable outcome: a query reports it (not NotFound), and after the name is freed a
        // delayed duplicate of the same frame gets the same answer instead of creating the file (OUT-013, FSD 13).
        var query = await c.OkAsync<QueryOperationResultMessage>(new QueryOperationMessage(c.Header(), create.Header.Operation, false, false));
        Assert.Equal(WireOutcomeStatus.Aborted, query.Status);
        Assert.Equal(DfsErrorCode.AlreadyExists, query.Error);
        await c.OkAsync<DeleteResultMessage>(new DeleteMessage(c.Op(), "/taken", false, false, null));
        await c.FailsAsync(DfsErrorCode.AlreadyExists, create);
        Assert.False((await c.OkAsync<GetInfoResultMessage>(new GetInfoMessage(c.Header(), "/taken", default, 0))).Exists);

        // A retryable refusal ("retrying the same operation may succeed", PROTOCOL.md 11.1) is not made definite: once the
        // conflicting handle is gone, the same id executes.
        OpenHandleResultMessage exclusive = await c.OpenAsync("/shared", FileMode.CreateNew, FileAccess.Write, FileShare.None);
        OpenHandleMessage second = c.OpenRequest("/shared", FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        await c.FailsAsync(DfsErrorCode.SharingViolation, second);
        await c.OkAsync<CloseHandleResultMessage>(new CloseHandleMessage(c.Header(), exclusive.Handle, exclusive.WriterGeneration));
        await c.OkAsync<OpenHandleResultMessage>(second);
    }

    [Fact]
    public async Task SnapshotReads_ServeNamedStreamsOfFilesAndDirectories()
    {
        (RuntimeCluster cluster, VolumeClient c) = await StartAsync(seed: 703);
        await using RuntimeCluster _ = cluster;
        await c.OkAsync<WriteAllResultMessage>(Write(c, "/d/f", [1, 1], createParents: true));
        await c.OkAsync<WriteAllResultMessage>(Write(c, "/d/f:s", [2, 2, 2]));
        await c.OkAsync<WriteAllResultMessage>(Write(c, "/d:x", [3]));
        await c.OkAsync<CreateSnapshotResultMessage>(new CreateSnapshotMessage(c.Op(), "s", null, DfsConsistencyLabel.FilesystemConsistent, [], null));
        await c.OkAsync<WriteAllResultMessage>(Write(c, "/d/f:s", [9]));
        await c.OkAsync<DeleteResultMessage>(new DeleteMessage(c.Op(), "/d:x", false, false, null));

        var opened = await c.OkAsync<OpenSnapshotResultMessage>(new OpenSnapshotMessage(c.Header(), "s"));
        var stream = await c.OkAsync<GetInfoResultMessage>(new GetInfoMessage(c.Header(), "/d/f:s", default, opened.SnapshotPin));
        Assert.True(stream.Info!.IsStream);
        var read = await c.OkAsync<ReadRangeResultMessage>(new ReadRangeMessage(c.Header(), default, opened.SnapshotPin, stream.Info.Inode, 0, 10) { Stream = "s" });
        Assert.Equal(new byte[] { 2, 2, 2 }, read.InlineData.ToArray());
        var file = await c.OkAsync<ReadRangeResultMessage>(new ReadRangeMessage(c.Header(), default, opened.SnapshotPin, stream.Info.Inode, 0, 10));
        Assert.Equal(new byte[] { 1, 1 }, file.InlineData.ToArray());

        var directory = await c.OkAsync<GetInfoResultMessage>(new GetInfoMessage(c.Header(), "/d:x", default, opened.SnapshotPin));
        var dirStream = await c.OkAsync<ReadRangeResultMessage>(new ReadRangeMessage(c.Header(), default, opened.SnapshotPin, directory.Info!.Inode, 0, 10)
        {
            Stream = "x",
        });
        Assert.Equal(new byte[] { 3 }, dirStream.InlineData.ToArray());
        await c.FailsAsync(DfsErrorCode.FileNotFound, new ReadRangeMessage(c.Header(), default, opened.SnapshotPin, stream.Info.Inode, 0, 10) { Stream = "none" });
    }

    [Fact]
    public void ReadRangeStream_RoundTripsOnTheWire()
    {
        var header = new RequestHeader("vol", Session: 3);
        foreach (string? stream in new[] { null, "s", "é" })
        {
            var message = new ReadRangeMessage(header, default, 7, new InodeId(9), 10, 20) { Stream = stream };
            var decoded = Assert.IsType<ReadRangeMessage>(MessageCodec.Decode(MessageCodec.Encode(message)));
            Assert.Equal(stream, decoded.Stream);
            Assert.Equal((7UL, 10L, 20L), (decoded.SnapshotPin, decoded.Offset, decoded.Length));
        }
    }

    [Fact]
    public async Task ANewHandle_IsNotExpiredWhileItsOpenReplyIsHeld()
    {
        var hold = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var faults = new RecordingFaultInjector
        {
            OnHit = async context =>
            {
                // Hold the reply path of the open after apply (F12) for several maintenance rounds.
                if (context.Site == FaultSites.ApplyAfterBeforeReply && context.Operation == DfsOperationKind.Open && context.Path == "/held")
                    await Task.WhenAny(hold.Task, Task.Delay(TimeSpan.FromMilliseconds(1500)));
            },
        };
        (RuntimeCluster cluster, VolumeClient c) = await StartAsync(seed: 705, faults);
        await using RuntimeCluster _ = cluster;

        // Let the leader be in office for longer than a lease, so a lease it has not seen yet must not be measured from its election.
        await Task.Delay(cluster.Timings.LeaseDuration + TimeSpan.FromMilliseconds(500), TestContext.Current.CancellationToken);
        await c.OkAsync<WriteAllResultMessage>(Write(c, "/warm", [1]));
        OpenHandleResultMessage h = await c.OpenAsync("/held", FileMode.CreateNew, FileAccess.Write, FileShare.Read);
        KeepAliveResultMessage renewed = await c.KeepAliveAsync(h.Handle);
        Assert.Empty(renewed.ExpiredHandles);
        await c.OkAsync<CommitFileResultMessage>(new CommitFileMessage(c.Op(), h.Handle, h.WriterGeneration, 0, -1, 1, [RangeChange.ForInline(0, new byte[] { 5 })],
            [], default, null));
    }

    [Fact]
    public async Task PostDecisionCleanup_IsAFaultPointOfReleasingOperations()
    {
        var faults = new RecordingFaultInjector();
        (RuntimeCluster cluster, VolumeClient c) = await StartAsync(seed: 707, faults);
        await using RuntimeCluster _ = cluster;
        RequestHeader write = c.Op();
        await c.OkAsync<WriteAllResultMessage>(new WriteAllMessage(write, "/w", 1, [RangeChange.ForInline(0, new byte[] { 1 })], [], default, null, null, false, 0));
        OpenHandleResultMessage h = await c.OpenAsync("/w", FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        await c.OkAsync<CloseHandleResultMessage>(new CloseHandleMessage(c.Op(), h.Handle, 0));
        await c.OkAsync<CreateSnapshotResultMessage>(new CreateSnapshotMessage(c.Op(), "snap", null, DfsConsistencyLabel.FilesystemConsistent, [], null));
        await c.OkAsync<DeleteSnapshotResultMessage>(new DeleteSnapshotMessage(c.Op(), "snap"));
        RequestHeader delete = c.Op();
        await c.OkAsync<DeleteResultMessage>(new DeleteMessage(delete, "/w", false, false, null));

        // F15 (cleanup.release) is reached by every operation whose decision releases reservations, pins or references
        // (NET-020 matrix: WriteAll, Delete, CloseHandle, DeleteSnapshot), with the operation identity and path.
        IReadOnlyList<FaultContext> cleanup = [.. faults.Hits.Where(x => x.Site == FaultSites.CleanupRelease)];
        Assert.All(cleanup, x => Assert.Equal(FaultStage.F15, x.Stage));
        Assert.Contains(cleanup, x => x.Operation == DfsOperationKind.WriteAll && x.OperationId == write.Operation && x.Path == "/w");
        Assert.Contains(cleanup, x => x.Operation == DfsOperationKind.Delete && x.OperationId == delete.Operation && x.Path == "/w");
        Assert.Contains(cleanup, x => x.Operation == DfsOperationKind.CloseHandle);
        Assert.Contains(cleanup, x => x.Operation == DfsOperationKind.DeleteSnapshot);

        // A replayed outcome releases nothing again.
        int before = faults.Hits.Count(x => x.Site == FaultSites.CleanupRelease);
        await c.OkAsync<DeleteResultMessage>(new DeleteMessage(delete, "/w", false, false, null));
        Assert.Equal(before, faults.Hits.Count(x => x.Site == FaultSites.CleanupRelease));
    }

    [Fact]
    public async Task WriteAllWithCreateParents_ValidatesReceiptsAgainstThePolicyTheFileInherits()
    {
        (RuntimeCluster cluster, VolumeClient c) = await StartAsync(seed: 709);
        await using RuntimeCluster _ = cluster;
        RawClient control = await cluster.ConnectAsync(await cluster.WaitForLeaderAsync("control"));
        var two = new DfsStoragePolicy { TargetCopies = 2, TargetDistinctZones = 1, DurableCopiesBeforeAck = 2, DurableZonesBeforeAck = 1 };
        await control.AdminAsync<AdminDefinePolicyBody, AdminEmptyBody>(AdminOp.DefinePolicy, new AdminDefinePolicyBody("vol", "two", two));
        await c.OkAsync<CreateDirectoryResultMessage>(new CreateDirectoryMessage(c.Op(), "/strong", false, null));
        await RuntimeCluster.WaitUntilAsync(async () => await c.CallAsync(new SetPolicyMessage(c.Op(), "/strong", "two")) is SetPolicyResultMessage,
            "the volume leader knows the new policy");

        // One durable copy satisfies the volume default but not "two", which the created directories and file inherit.
        CreateStagingResultMessage staging = await c.StagingAsync();
        byte[] data = new byte[20_000];
        new Random(709).NextBytes(data);
        (RangeChange change, IReadOnlyList<ObjectReceipt> receipts) = await c.UploadAsync(staging, 0, data, copies: 1);
        var weak = new WriteAllMessage(c.Op(), "/strong/a/b/file", data.Length, [change], receipts, staging.Staging, null, null, true, 0);
        ErrorReplyMessage refused = await c.FailsAsync(DfsErrorCode.PlacementUnavailable, weak);
        Assert.Equal(2, refused.Detail?.RequiredCopies);
        Assert.False((await c.OkAsync<GetInfoResultMessage>(new GetInfoMessage(c.Header(), "/strong/a", default, 0))).Exists);

        (RangeChange both, IReadOnlyList<ObjectReceipt> two2) = await c.UploadAsync(staging, 0, data, copies: 2);
        await c.OkAsync<WriteAllResultMessage>(new WriteAllMessage(c.Op(), "/strong/a/b/file", data.Length, [both], two2, staging.Staging, null, null, true, 0));
        var info = await c.OkAsync<GetInfoResultMessage>(new GetInfoMessage(c.Header(), "/strong/a/b/file", default, 0));
        Assert.Equal("two", info.Info!.StoragePolicy);
    }

    [Fact]
    public async Task CopyUnderAStrongerPolicy_DoesNotShareObjectsThatFailIt()
    {
        (RuntimeCluster cluster, VolumeClient c) = await StartAsync(seed: 715);
        await using RuntimeCluster _ = cluster;
        RawClient control = await cluster.ConnectAsync(await cluster.WaitForLeaderAsync("control"));
        var two = new DfsStoragePolicy { TargetCopies = 2, TargetDistinctZones = 1, DurableCopiesBeforeAck = 2, DurableZonesBeforeAck = 1 };
        await control.AdminAsync<AdminDefinePolicyBody, AdminEmptyBody>(AdminOp.DefinePolicy, new AdminDefinePolicyBody("vol", "two", two));

        // A file published with one durable copy (enough for the volume default) ...
        CreateStagingResultMessage staging = await c.StagingAsync();
        byte[] data = new byte[30_000];
        new Random(715).NextBytes(data);
        (RangeChange change, IReadOnlyList<ObjectReceipt> receipts) = await c.UploadAsync(staging, 0, data, copies: 1);
        await c.OkAsync<WriteAllResultMessage>(new WriteAllMessage(c.Op(), "/one", data.Length, [change], receipts, staging.Staging, null, null, false, 0));

        // ... copied under a policy acknowledging two copies: the copy must not reference an object with one copy (DEC-10).
        CopyResultMessage copied = null!;
        await RuntimeCluster.WaitUntilAsync(async () => (copied = (await c.CallAsync(new CopyMessage(c.Op(), "/one", "/two", false, 0, "two", null)) as CopyResultMessage)!)
            is not null, "the volume leader knows the new policy");
        RuntimeNode leader = await cluster.WaitForLeaderAsync("vol");
        VolumeStateInspection state = (await leader.Host!.Inspect.GetVolumeStateAsync("vol"))!;
        ExtentInspection extent = Assert.Single(Assert.IsType<FileInspection>(state.GetFile("/two")).Extents);
        Assert.NotEqual(change.Object, extent.Object);
        int holders = 0;
        foreach (RuntimeNode node in cluster.Running)
        {
            if (await node.Host!.InvokeAsync(s => Task.FromResult(s.ObjectStore.TryGetInfo(extent.Object) is not null)))
                holders++;
        }

        Assert.True(holders >= 2, $"the copy's object has {holders} copies");
        OpenHandleResultMessage reader = await c.OpenAsync("/two", FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        Assert.Equal(data, await c.ReadAsync(reader.Handle, 0, data.Length));

        // A copy under the same policy still shares the objects (metadata-only).
        await c.OkAsync<CopyResultMessage>(new CopyMessage(c.Op(), "/one", "/same", false, 0, null, null));
        state = (await leader.Host!.Inspect.GetVolumeStateAsync("vol"))!;
        Assert.Equal(change.Object, Assert.Single(Assert.IsType<FileInspection>(state.GetFile("/same")).Extents).Object);
        Assert.True(copied.Created);
    }

    [Fact]
    public async Task AppendHandles_CannotRewriteOrShortenThroughCommitFile()
    {
        (RuntimeCluster cluster, VolumeClient c) = await StartAsync(seed: 711);
        await using RuntimeCluster _ = cluster;
        await c.OkAsync<WriteAllResultMessage>(Write(c, "/log", [1, 2, 3, 4]));
        OpenHandleResultMessage h = await c.OpenAsync("/log", FileMode.Append, FileAccess.Write, FileShare.Read);
        await c.FailsAsync(DfsErrorCode.InvalidArgument, new CommitFileMessage(c.Op(), h.Handle, h.WriterGeneration, 0, -1, 2,
            [RangeChange.ForInline(0, new byte[] { 9, 9 })], [], default, null));
        await c.FailsAsync(DfsErrorCode.InvalidArgument, new CommitFileMessage(c.Op(), h.Handle, h.WriterGeneration, 0, -1, 4,
            [RangeChange.ForInline(1, new byte[] { 9 })], [], default, null));
        await c.OkAsync<CommitFileResultMessage>(new CommitFileMessage(c.Op(), h.Handle, h.WriterGeneration, 0, -1, 6,
            [RangeChange.ForInline(4, new byte[] { 5, 6 })], [], default, null));
        OpenHandleResultMessage reader = await c.OpenAsync("/log", FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        Assert.Equal(new byte[] { 1, 2, 3, 4, 5, 6 }, await c.ReadAsync(reader.Handle, 0, 6));
    }

    [Fact]
    public async Task ConcurrentCreatingAppends_KeepEverySuffix()
    {
        (RuntimeCluster cluster, VolumeClient c) = await StartAsync(seed: 713);
        await using RuntimeCluster _ = cluster;
        var appends = Enumerable.Range(1, 6)
            .Select(i => new AppendMessage(c.Op(), "/race", default, 0, 2, [RangeChange.ForInline(0, new byte[] { (byte)i, (byte)i })], [], default, true, null, null))
            .ToArray();
        IWireMessage[] replies = await Task.WhenAll(appends.Select(a => c.CallAsync(a)));
        int succeeded = replies.Count(r => r is AppendResultMessage);
        Assert.All(replies, r => Assert.True(r is AppendResultMessage or ErrorReplyMessage { Code: DfsErrorCode.WriterConflict }, $"unexpected {r}"));

        // Every acknowledged append is present exactly once; nothing acknowledged was overwritten.
        OpenHandleResultMessage reader = await c.OpenAsync("/race", FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        Assert.Equal(succeeded * 2, reader.Info.Length);
        byte[] content = await c.ReadAsync(reader.Handle, 0, reader.Info.Length);
        foreach (var (reply, index) in replies.Select((r, i) => (r, i + 1)))
        {
            if (reply is AppendResultMessage ok)
                Assert.Equal(new byte[] { (byte)index, (byte)index }, content[(int)ok.OldLength..(int)ok.NewLength]);
        }
    }

    [Fact]
    public async Task PublicationLock_ACancelledWaiterNeverBlocksTheNextOne()
    {
        var locks = new KeyedAsyncLock<string>(StringComparer.Ordinal);
        IDisposable holder = await locks.AcquireAsync("k", CancellationToken.None);
        using var cts = new CancellationTokenSource();
        Task<IDisposable> cancelled = locks.AcquireAsync("k", cts.Token);
        Task<IDisposable> next = locks.AcquireAsync("k", CancellationToken.None);
        await cts.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => cancelled);

        holder.Dispose();
        IDisposable second = await next.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        Assert.True(locks.IsHeld("k"));
        second.Dispose();
        second.Dispose();
        Assert.False(locks.IsHeld("k"));
        Assert.Equal(0, locks.HeldCount);
    }
}
