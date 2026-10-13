using System.Text;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Tests.Unit.Internal.Runtime;

namespace Tedd.Tdfs.Tests.Unit.Internal.Volumes;

/// <summary>
/// <c>RestoreSnapshot</c> end to end on a real multi-node runtime cluster: a complete new volume (new id, server-side object
/// copies owned by it, the snapshot's namespace exactly), stored outcomes for retries, validation before anything is
/// created, no service while the restore is incomplete, and restores into another storage pool.
/// </summary>
public sealed class VolumeServiceRestoreTests
{
    private static async Task<(RuntimeCluster Cluster, VolumeClient Client)> StartAsync(int nodes = 3, ulong seed = 301, IFaultInjector? faults = null)
    {
        var cluster = new RuntimeCluster(nodes, seed: seed) { VolumeStateMachines = null };
        if (faults is not null)
            cluster.Faults = faults;
        await cluster.StartAllAsync();
        await cluster.WaitForRegistrationAsync();
        await cluster.CreateVolumeAsync("vol");
        var client = new VolumeClient(cluster, "vol");
        await client.OpenSessionAsync();
        return (cluster, client);
    }

    /// <summary>Writes a small tree, snapshots it as <c>s1</c> and changes the live namespace afterwards.</summary>
    private static async Task<(byte[] Big, byte[] Small, byte[] Meta)> PrepareAsync(VolumeClient c)
    {
        byte[] big = Bytes(11, 150_000);
        byte[] small = Encoding.UTF8.GetBytes("small and inline");
        byte[] meta = [7, 7, 7];
        await c.OkAsync<CreateDirectoryResultMessage>(new CreateDirectoryMessage(c.Op(), "/docs/empty", true, null));
        OpenHandleResultMessage h = await c.OpenAsync("/docs/big.bin", FileMode.CreateNew, FileAccess.Write, FileShare.Read | FileShare.Delete);
        CreateStagingResultMessage staging = await c.StagingAsync(h.Handle);
        (RangeChange first, IReadOnlyList<ObjectReceipt> r1) = await c.UploadAsync(staging, 0, big[..100_000]);
        (RangeChange second, IReadOnlyList<ObjectReceipt> r2) = await c.UploadAsync(staging, 100_000, big[100_000..]);
        await c.OkAsync<CommitFileResultMessage>(new CommitFileMessage(c.Op(), h.Handle, h.WriterGeneration, 0, -1, big.Length, [first, second], [.. r1, .. r2],
            staging.Staging, null));
        await c.OkAsync<CloseHandleResultMessage>(new CloseHandleMessage(c.Header(), h.Handle, h.WriterGeneration));
        await c.OkAsync<WriteAllResultMessage>(new WriteAllMessage(c.Op(), "/docs/small.txt", small.Length, [RangeChange.ForInline(0, small)], [], default, null,
            null, false, 0));
        await c.OkAsync<WriteAllResultMessage>(new WriteAllMessage(c.Op(), "/docs/small.txt:meta", meta.Length, [RangeChange.ForInline(0, meta)], [], default,
            null, null, false, 0));
        await c.OkAsync<CreateSnapshotResultMessage>(new CreateSnapshotMessage(c.Op(), "s1", null, DfsConsistencyLabel.FilesystemConsistent, [], null));

        // Live changes after the boundary must not leak into the restore.
        await c.OkAsync<DeleteResultMessage>(new DeleteMessage(c.Op(), "/docs/big.bin", false, false, null));
        await c.OkAsync<WriteAllResultMessage>(new WriteAllMessage(c.Op(), "/docs/small.txt", 1, [RangeChange.ForInline(0, new byte[] { 1 })], [], default, null,
            null, false, 0));
        await c.OkAsync<WriteAllResultMessage>(new WriteAllMessage(c.Op(), "/late", 1, [RangeChange.ForInline(0, new byte[] { 2 })], [], default, null, null,
            false, 0));
        return (big, small, meta);
    }

    private static async Task AssertRestoredAsync(RuntimeCluster cluster, string volume, byte[] big, byte[] small, byte[] meta)
    {
        var r = new VolumeClient(cluster, volume);
        await r.OpenSessionAsync();
        OpenHandleResultMessage bigHandle = await r.OpenAsync("/docs/big.bin", FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        Assert.Equal(big, await r.ReadAsync(bigHandle.Handle, 0, big.Length));
        OpenHandleResultMessage smallHandle = await r.OpenAsync("/docs/small.txt", FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        Assert.Equal(small, await r.ReadAsync(smallHandle.Handle, 0, small.Length));
        OpenHandleResultMessage metaHandle = await r.OpenAsync("/docs/small.txt:meta", FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        Assert.Equal(meta, await r.ReadAsync(metaHandle.Handle, 0, meta.Length));
        Assert.True((await r.OkAsync<GetInfoResultMessage>(new GetInfoMessage(r.Header(), "/docs/empty", default, 0))).Exists);
        Assert.False((await r.OkAsync<GetInfoResultMessage>(new GetInfoMessage(r.Header(), "/late", default, 0))).Exists);
        // The restored volume is writable like any other.
        await r.OkAsync<WriteAllResultMessage>(new WriteAllMessage(r.Op(), "/after", 1, [RangeChange.ForInline(0, new byte[] { 3 })], [], default, null, null,
            false, 0));
    }

    [Fact]
    public async Task Restore_CreatesACompleteIndependentVolumeAndRetriesGetTheStoredOutcome()
    {
        (RuntimeCluster cluster, VolumeClient c) = await StartAsync();
        await using RuntimeCluster _ = cluster;
        (byte[] big, byte[] small, byte[] meta) = await PrepareAsync(c);

        var request = new RestoreSnapshotMessage(c.Op(), "s1", "restored");
        var restored = await c.OkAsync<RestoreSnapshotResultMessage>(request);
        Assert.Equal("restored", restored.Volume.Name);
        Assert.Equal(WireVolumeState.Active, restored.Volume.State);
        Assert.Equal(1UL, restored.Volume.Epoch);
        Assert.Equal(restored.Volume.Id, restored.LastCommit.VolumeId);
        VolumeEntry source = (await cluster.ConnectAsync(await cluster.WaitForLeaderAsync("vol")) is { } raw
            ? ((VolumeRouteResultMessage)await raw.CallOrThrowAsync<VolumeRouteResultMessage>(new GetVolumeRouteMessage("vol"))).Volume
            : throw new InvalidOperationException());
        Assert.NotEqual(source.Id, restored.Volume.Id);

        // The retry returns the stored outcome (same volume, same commit); nothing is created twice.
        var retry = await c.OkAsync<RestoreSnapshotResultMessage>(request);
        Assert.Equal((restored.Volume.Id, restored.LastCommit), (retry.Volume.Id, retry.LastCommit));
        await c.FailsAsync(DfsErrorCode.OperationIdMismatch, request with { NewVolumeName = "other" });

        await AssertRestoredAsync(cluster, "restored", big, small, meta);

        // The copies belong to the new volume: no object id is shared, and the source can drop its snapshot freely.
        VolumeStateInspection target = (await (await cluster.WaitForLeaderAsync("restored")).Host!.Inspect.GetVolumeStateAsync("restored"))!;
        VolumeStateInspection origin = (await (await cluster.WaitForLeaderAsync("vol")).Host!.Inspect.GetVolumeStateAsync("vol"))!;
        Assert.NotEmpty(target.ReachableObjects());
        Assert.Empty(target.ReachableObjects().Intersect(origin.ReachableObjects()));
        await c.OkAsync<DeleteSnapshotResultMessage>(new DeleteSnapshotMessage(c.Op(), "s1"));
        await AssertRestoredAsync(cluster, "restored", big, small, meta);

        // The source volume is unchanged by the restore.
        Assert.True((await c.OkAsync<GetInfoResultMessage>(new GetInfoMessage(c.Header(), "/late", default, 0))).Exists);
        Assert.False((await c.OkAsync<GetInfoResultMessage>(new GetInfoMessage(c.Header(), "/docs/big.bin", default, 0))).Exists);
    }

    [Fact]
    public async Task Restore_ValidatesBeforeCreatingOrCopyingAnything()
    {
        (RuntimeCluster cluster, VolumeClient c) = await StartAsync(seed: 303);
        await using RuntimeCluster _ = cluster;
        await c.OkAsync<WriteAllResultMessage>(new WriteAllMessage(c.Op(), "/f", 1, [RangeChange.ForInline(0, new byte[] { 1 })], [], default, null, null, false, 0));
        await c.OkAsync<CreateSnapshotResultMessage>(new CreateSnapshotMessage(c.Op(), "s1", null, DfsConsistencyLabel.FilesystemConsistent, [], null));

        await c.FailsAsync(DfsErrorCode.SnapshotNotFound, new RestoreSnapshotMessage(c.Op(), "missing", "r1"));
        await c.FailsAsync(DfsErrorCode.AlreadyExists, new RestoreSnapshotMessage(c.Op(), "s1", "vol"));
        await c.FailsAsync(DfsErrorCode.PoolNotFound, new RestoreSnapshotMessage(c.Op(), "s1", "r1").WithStoragePool("nopool"));
        await c.FailsAsync(DfsErrorCode.InvalidArgument, new RestoreSnapshotMessage(c.Op(), "s1", "bad/name"));
        await c.FailsAsync(DfsErrorCode.InvalidArgument, new RestoreSnapshotMessage(c.Header(), "s1", "r1"));

        // A two-node pool cannot hold the volume's three copies (POOL-03).
        RawClient control = await cluster.ConnectAsync(await cluster.WaitForLeaderAsync("control"));
        await control.AdminAsync<AdminPoolMembersBody, AdminEmptyBody>(AdminOp.CreatePool, new AdminPoolMembersBody("small", ["n1", "n2"]));
        await c.FailsAsync(DfsErrorCode.InvalidPolicy, new RestoreSnapshotMessage(c.Op(), "s1", "r1").WithStoragePool("small"));

        // A volume administrator who is not a cluster administrator may not restore.
        await control.AdminAsync<AdminGrantBody, AdminEmptyBody>(AdminOp.Grant, new AdminGrantBody("vol", "reader", DfsPermissions.All & ~DfsPermissions.ClusterAdmin));
        var reader = new VolumeClient(cluster, "vol", new DfsCredentials("reader", "reader-secret-1"));
        await reader.OpenSessionAsync();
        await reader.FailsAsync(DfsErrorCode.AccessDenied, new RestoreSnapshotMessage(reader.Op(), "s1", "r1"));

        var volumes = await control.AdminAsync<AdminEmptyBody, AdminNameListBody>(AdminOp.ListVolumes, new AdminEmptyBody());
        Assert.Equal(["vol"], volumes.Names);
    }

    [Fact]
    public async Task Restore_DoesNotServeTheNewVolumeBeforeItIsActivated()
    {
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var reached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var faults = new RecordingFaultInjector
        {
            OnHit = async context =>
            {
                if (context.Site == FaultSites.Reconfiguration && context.Operation == DfsOperationKind.RestoreSnapshot)
                {
                    reached.TrySetResult();
                    await gate.Task;
                }
            },
        };
        (RuntimeCluster cluster, VolumeClient c) = await StartAsync(seed: 307, faults: faults);
        await using RuntimeCluster _ = cluster;
        (byte[] big, byte[] small, byte[] meta) = await PrepareAsync(c);

        Task<IWireMessage> restore = c.CallAsync(new RestoreSnapshotMessage(c.Op(), "s1", "restored"));
        await reached.Task.WaitAsync(TimeSpan.FromSeconds(60), TestContext.Current.CancellationToken);

        // Installed but not activated: the volume exists in the map as Restoring and answers Unavailable.
        RuntimeNode leader = await cluster.WaitForLeaderAsync("restored");
        RawClient probe = await cluster.ConnectAsync(leader);
        var route = (VolumeRouteResultMessage)await probe.CallOrThrowAsync<VolumeRouteResultMessage>(new GetVolumeRouteMessage("restored"));
        Assert.Equal(WireVolumeState.Restoring, route.Volume.State);
        var refused = Assert.IsType<ErrorReplyMessage>(await probe.CallAsync(new OpenSessionMessage(new RequestHeader("restored"), "probe", new UInt128(9, 9))));
        Assert.Equal(DfsErrorCode.Unavailable, refused.Code);

        gate.TrySetResult();
        Assert.IsType<RestoreSnapshotResultMessage>(await restore);
        await AssertRestoredAsync(cluster, "restored", big, small, meta);
    }

    [Fact]
    public async Task Restore_InterruptedByALeaderCrash_IsResumedByTheNextLeader()
    {
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var reached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int hits = 0;
        var faults = new RecordingFaultInjector
        {
            OnHit = async context =>
            {
                // Hold the first activation until the coordinating leader is gone.
                if (context.Site == FaultSites.Reconfiguration && context.Operation == DfsOperationKind.RestoreSnapshot && Interlocked.Increment(ref hits) == 1)
                {
                    reached.TrySetResult();
                    await gate.Task;
                }
            },
        };
        (RuntimeCluster cluster, VolumeClient c) = await StartAsync(seed: 313, faults: faults);
        await using RuntimeCluster _ = cluster;
        (byte[] big, byte[] small, byte[] meta) = await PrepareAsync(c);

        var request = new RestoreSnapshotMessage(c.Op(), "s1", "restored");
        Task<IWireMessage> first = c.CallAsync(request);
        await reached.Task.WaitAsync(TimeSpan.FromSeconds(60), TestContext.Current.CancellationToken);
        cluster.Kill(await cluster.WaitForLeaderAsync("vol"));
        gate.TrySetResult();
        // The interrupted call fails with its connection, or its NotLeader retries reach the next leader and get the outcome.
        if (await Record.ExceptionAsync(() => first) is null)
        {
            IWireMessage reply = await first;
            Assert.True(reply is RestoreSnapshotResultMessage or ErrorReplyMessage, $"unexpected reply {reply.Type}");
        }

        // The next leader resumes the accepted restore from its committed intent on its own (no client retry needed) ...
        RawClient probe = await cluster.ConnectAsync(await cluster.WaitForLeaderAsync("control"));
        await RuntimeCluster.WaitUntilAsync(async () => await probe.CallAsync(new GetVolumeRouteMessage("restored")) is VolumeRouteResultMessage
        {
            Volume.State: WireVolumeState.Active,
        }, "the next leader completes the restore", TimeSpan.FromSeconds(60));

        // ... and the retry gets the recorded outcome.
        var restored = await c.OkAsync<RestoreSnapshotResultMessage>(request);
        Assert.Equal("restored", restored.Volume.Name);
        Assert.Equal(WireVolumeState.Active, restored.Volume.State);
        await AssertRestoredAsync(cluster, "restored", big, small, meta);
        var again = await c.OkAsync<RestoreSnapshotResultMessage>(request);
        Assert.Equal(restored.LastCommit, again.LastCommit);
    }

    [Fact]
    public async Task Restore_IntoAnotherPoolPlacesEveryCopyAndVoterInsideIt()
    {
        (RuntimeCluster cluster, VolumeClient c) = await StartAsync(nodes: 4, seed: 311);
        await using RuntimeCluster _ = cluster;
        (byte[] big, byte[] small, byte[] meta) = await PrepareAsync(c);
        RawClient control = await cluster.ConnectAsync(await cluster.WaitForLeaderAsync("control"));
        await control.AdminAsync<AdminPoolMembersBody, AdminEmptyBody>(AdminOp.CreatePool, new AdminPoolMembersBody("p", ["n1", "n2", "n3"]));

        var restored = await c.OkAsync<RestoreSnapshotResultMessage>(new RestoreSnapshotMessage(c.Op(), "s1", "pooled").WithStoragePool("p"));
        Assert.Equal("p", restored.Volume.StoragePool);
        var poolNodes = new HashSet<NodeId>(cluster.Running.Where(n => n.Name != "n0").Select(n => n.Id));
        Assert.All(restored.Volume.Voters, v => Assert.Contains(v, poolNodes));
        await AssertRestoredAsync(cluster, "pooled", big, small, meta);

        // Every copy of every restored object lives on the pool's nodes only.
        VolumeStateInspection state = (await (await cluster.WaitForLeaderAsync("pooled")).Host!.Inspect.GetVolumeStateAsync("pooled"))!;
        FileInspection file = Assert.IsType<FileInspection>(state.GetFile("/docs/big.bin"));
        Assert.NotEmpty(file.Extents);
        RuntimeNode outside = cluster.Running.Single(n => n.Name == "n0");
        foreach (ObjectId obj in state.ReachableObjects())
            Assert.False(await outside.Host!.InvokeAsync(s => Task.FromResult(s.ObjectStore.TryGetInfo(obj) is not null)), $"{obj} was copied outside the pool");
    }

    private static byte[] Bytes(int seed, int length)
    {
        var bytes = new byte[length];
        new Random(seed).NextBytes(bytes);
        return bytes;
    }
}
