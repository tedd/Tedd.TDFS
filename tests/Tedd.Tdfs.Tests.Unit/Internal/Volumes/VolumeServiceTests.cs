using System.Text;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Tests.Unit.Internal.Runtime;

namespace Tedd.Tdfs.Tests.Unit.Internal.Volumes;

/// <summary>
/// The volume service on a real multi-node runtime cluster (InMemoryNetwork, simulated disks, production volume state
/// machine), driven with raw protocol messages: sessions, opens, staging uploads with receipts, publication, reads with read
/// capabilities, namespace operations, fencing and lease expiry, outcome replay, ACLs, streams and snapshots.
/// </summary>
public sealed class VolumeServiceTests
{
    private static async Task<(RuntimeCluster Cluster, VolumeClient Client)> StartAsync(int nodes = 3, ulong seed = 101, string volume = "vol")
    {
        var cluster = new RuntimeCluster(nodes, seed: seed) { VolumeStateMachines = null };
        await cluster.StartAllAsync();
        await cluster.WaitForRegistrationAsync();
        await cluster.CreateVolumeAsync(volume);
        var client = new VolumeClient(cluster, volume);
        await client.OpenSessionAsync();
        return (cluster, client);
    }

    [Fact]
    public async Task WriteThroughAHandle_ReadBackThroughReadRangeAndObjects()
    {
        (RuntimeCluster cluster, VolumeClient c) = await StartAsync();
        await using RuntimeCluster _ = cluster;
        var dir = await c.OkAsync<CreateDirectoryResultMessage>(new CreateDirectoryMessage(c.Op(), "/docs", true, null));
        Assert.True(dir.Created);

        OpenHandleResultMessage handle = await c.OpenAsync("/docs/report.bin", FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read);
        Assert.True(handle.Created);
        Assert.True(handle.WriterGeneration > 0);
        CreateStagingResultMessage staging = await c.StagingAsync(handle.Handle);
        Assert.False(staging.Capability.IsEmpty);

        byte[] first = Bytes(1, 100_000);
        byte[] second = Bytes(2, 50_000);
        (RangeChange c1, IReadOnlyList<ObjectReceipt> r1) = await c.UploadAsync(staging, 0, first);
        (RangeChange c2, IReadOnlyList<ObjectReceipt> r2) = await c.UploadAsync(staging, first.Length, second);
        var commit = await c.OkAsync<CommitFileResultMessage>(new CommitFileMessage(c.Op(), handle.Handle, handle.WriterGeneration, 0, -1,
            first.Length + second.Length, [c1, c2], [.. r1, .. r2], staging.Staging, null));
        Assert.Equal(150_000, commit.NewLength);
        Assert.Equal(commit.Token.Sequence, commit.Revision);

        byte[] read = await c.ReadAsync(handle.Handle, 0, 150_000);
        Assert.Equal([.. first, .. second], read);

        var info = await c.OkAsync<GetInfoResultMessage>(new GetInfoMessage(c.Header(), "/docs/report.bin", default, 0));
        Assert.True(info.Exists);
        Assert.Equal(150_000, info.Info!.Length);
        Assert.Equal(2, info.Info.ExtentCount);
        Assert.False(info.Info.IsInline);

        // Every replica applied the same state.
        VolumeStateInspection? leaderState = await (await cluster.WaitForLeaderAsync("vol")).Host!.Inspect.GetVolumeStateAsync("vol");
        FileInspection file = Assert.IsType<FileInspection>(leaderState!.GetFile("/docs/report.bin"));
        Assert.Equal(150_000, file.Length);
        Assert.Equal(2, file.Extents.Count);
        Assert.Contains(c1.Object, leaderState.ReachableObjects());
        await AssertReplicasConvergeAsync(cluster);
    }

    [Fact]
    public async Task SmallFiles_AreInlineAndPlacementShortfallIsReportedWithDetails()
    {
        (RuntimeCluster cluster, VolumeClient c) = await StartAsync();
        await using RuntimeCluster _ = cluster;
        byte[] small = Encoding.UTF8.GetBytes("hello inline world");
        var written = await c.OkAsync<WriteAllResultMessage>(new WriteAllMessage(c.Op(), "/small.txt", small.Length,
            [RangeChange.ForInline(0, small)], [], default, null, null, false, 0));
        Assert.True(written.Created);
        OpenHandleResultMessage reader = await c.OpenAsync("/small.txt", FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        Assert.True(reader.Info.IsInline);
        Assert.Equal(small, await c.ReadAsync(reader.Handle, 0, small.Length));

        // An upload with no valid receipt cannot be published: PlacementUnavailable with the shortfall, nothing proposed.
        OpenHandleResultMessage writer = await c.OpenAsync("/big.bin", FileMode.CreateNew, FileAccess.Write, FileShare.Read);
        CreateStagingResultMessage staging = await c.StagingAsync(writer.Handle);
        (RangeChange change, IReadOnlyList<ObjectReceipt> unused) = await c.UploadAsync(staging, 0, Bytes(3, 9000));
        Assert.NotEmpty(unused);
        ErrorReplyMessage shortfall = await c.FailsAsync(DfsErrorCode.PlacementUnavailable, new CommitFileMessage(c.Op(), writer.Handle,
            writer.WriterGeneration, 0, -1, 9000, [change], [], staging.Staging, null));
        Assert.Equal(1, shortfall.Detail?.RequiredCopies);
        Assert.Equal(0, shortfall.Detail?.AchievedCopies);
    }

    [Fact]
    public async Task Retries_ReturnTheStoredOutcome_QueryResolvesAndFences()
    {
        (RuntimeCluster cluster, VolumeClient c) = await StartAsync();
        await using RuntimeCluster _ = cluster;
        RequestHeader op = c.Op();
        var request = new CreateDirectoryMessage(op, "/once", false, null);
        var first = await c.OkAsync<CreateDirectoryResultMessage>(request);
        var retry = await c.OkAsync<CreateDirectoryResultMessage>(request);
        Assert.Equal(first.Token, retry.Token);
        Assert.Equal(first.Info.Revision, retry.Info.Revision);
        await c.FailsAsync(DfsErrorCode.OperationIdMismatch, new CreateDirectoryMessage(op, "/other", false, null));

        var committed = await c.OkAsync<QueryOperationResultMessage>(new QueryOperationMessage(c.Header(), op.Operation, false, false));
        Assert.Equal(WireOutcomeStatus.Committed, committed.Status);
        Assert.Equal(first, MessageCodec.Decode(committed.ResultFrame.Span));

        DfsOperationId unused = c.NewOperationId();
        var notFound = await c.OkAsync<QueryOperationResultMessage>(new QueryOperationMessage(c.Header(), unused, false, false));
        Assert.Equal(WireOutcomeStatus.NotFound, notFound.Status);
        var fenced = await c.OkAsync<QueryOperationResultMessage>(new QueryOperationMessage(c.Header(), unused, false, true));
        Assert.Equal(WireOutcomeStatus.Aborted, fenced.Status);
        // The delayed original never executes (OUT-013).
        await c.FailsAsync(DfsErrorCode.Cancelled, new CreateDirectoryMessage(c.Header(unused), "/late", false, null));
        var late = await c.OkAsync<GetInfoResultMessage>(new GetInfoMessage(c.Header(), "/late", default, 0));
        Assert.False(late.Exists);

        // Another principal can neither replay nor query the id (OUT-004).
        var bob = new VolumeClient(cluster, "vol", new DfsCredentials("reader", "reader-secret-1"));
        ErrorReplyMessage denied = Assert.IsType<ErrorReplyMessage>(await bob.CallAsync(new OpenSessionMessage(new RequestHeader("vol"), "bob", new UInt128(1, 1))));
        Assert.Equal(DfsErrorCode.AccessDenied, denied.Code);
    }

    [Fact]
    public async Task Leases_ExpireWithoutKeepAlive_AndTheOldWriterIsFenced()
    {
        (RuntimeCluster cluster, VolumeClient c) = await StartAsync();
        await using RuntimeCluster _ = cluster;
        OpenHandleResultMessage old = await c.OpenAsync("/fenced", FileMode.CreateNew, FileAccess.Write, FileShare.Read);
        // A second writer conflicts while the lease is held.
        await c.FailsAsync(DfsErrorCode.WriterConflict, c.OpenRequest("/fenced", FileMode.Open, FileAccess.Write, FileShare.Read));

        // No keep-alive: the leader expires the lease (2 s in Fast timings) and a replacement writer is granted.
        OpenHandleResultMessage? replacement = null;
        await RuntimeCluster.WaitUntilAsync(async () =>
        {
            IWireMessage reply = await c.CallAsync(c.OpenRequest("/fenced", FileMode.Open, FileAccess.Write, FileShare.Read));
            replacement = reply as OpenHandleResultMessage;
            return replacement is not null;
        }, "the expired writer's reservation is released", TimeSpan.FromSeconds(30));
        Assert.Equal(old.WriterGeneration + 1, replacement!.WriterGeneration);

        byte[] data = [1, 2, 3];
        await c.FailsAsync(DfsErrorCode.LeaseLost, new CommitFileMessage(c.Op(), old.Handle, old.WriterGeneration, 0, -1, 3,
            [RangeChange.ForInline(0, data)], [], default, null));
        var keep = await c.KeepAliveAsync(old.Handle, replacement.Handle);
        Assert.Equal([old.Handle], keep.ExpiredHandles);
        await c.OkAsync<CommitFileResultMessage>(new CommitFileMessage(c.Op(), replacement.Handle, replacement.WriterGeneration, 0, -1, 3,
            [RangeChange.ForInline(0, data)], [], default, null));
    }

    [Fact]
    public async Task RenameAndDelete_HonourSharingAndKeepHandlesOnTheirInodes()
    {
        (RuntimeCluster cluster, VolumeClient c) = await StartAsync();
        await using RuntimeCluster _ = cluster;
        OpenHandleResultMessage h = await c.OpenAsync("/a.txt", FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read | FileShare.Delete);
        await c.OkAsync<CommitFileResultMessage>(new CommitFileMessage(c.Op(), h.Handle, h.WriterGeneration, 0, -1, 4,
            [RangeChange.ForInline(0, new byte[] { 1, 2, 3, 4 })], [], default, null));
        var moved = await c.OkAsync<MoveResultMessage>(new MoveMessage(c.Op(), "/a.txt", "/b.txt", false, false, null));
        Assert.Equal(h.Info.Inode, moved.Inode);
        var deleted = await c.OkAsync<DeleteResultMessage>(new DeleteMessage(c.Op(), "/b.txt", false, false, null));
        Assert.True(deleted.Existed);
        // The handle still reads its (detached) inode; the name is gone and is not recreated by writes (OPEN-017).
        Assert.Equal(new byte[] { 1, 2, 3, 4 }, await c.ReadAsync(h.Handle, 0, 4));
        await c.OkAsync<CommitFileResultMessage>(new CommitFileMessage(c.Op(), h.Handle, h.WriterGeneration, 0, -1, 2,
            [RangeChange.ForInline(0, new byte[] { 9, 9 })], [], default, null));
        Assert.False((await c.OkAsync<GetInfoResultMessage>(new GetInfoMessage(c.Header(), "/b.txt", default, 0))).Exists);
        var byHandle = await c.OkAsync<GetInfoResultMessage>(new GetInfoMessage(c.Header(), "", h.Handle, 0));
        Assert.Equal(string.Empty, byHandle.Info!.Path);
        Assert.Equal(2, byHandle.Info.Length);
        await c.OkAsync<CloseHandleResultMessage>(new CloseHandleMessage(c.Header(), h.Handle, h.WriterGeneration));
        RuntimeNode leader = await cluster.WaitForLeaderAsync("vol");
        VolumeStateInspection state = (await leader.Host!.Inspect.GetVolumeStateAsync("vol"))!;
        Assert.Equal(1, state.InodeCount);

        // A handle without FileShare.Delete blocks deletion.
        await c.OpenAsync("/locked", FileMode.CreateNew, FileAccess.Write, FileShare.Read);
        await c.FailsAsync(DfsErrorCode.SharingViolation, new DeleteMessage(c.Op(), "/locked", false, false, null));
        await c.FailsAsync(DfsErrorCode.InvalidPath, new DeleteMessage(c.Op(), "/bad/../x", false, false, null));
    }

    [Fact]
    public async Task Acls_DenyThroughDescriptorsAndReportEffectiveRights()
    {
        (RuntimeCluster cluster, VolumeClient admin) = await StartAsync();
        await using RuntimeCluster _ = cluster;
        RawClient control = await cluster.ConnectAsync(await cluster.WaitForLeaderAsync("control"));
        await control.AdminAsync<AdminGrantBody, AdminEmptyBody>(AdminOp.Grant, new AdminGrantBody("vol", "reader", DfsPermissions.Read | DfsPermissions.Write));
        await admin.OkAsync<WriteAllResultMessage>(new WriteAllMessage(admin.Op(), "/secret", 3, [RangeChange.ForInline(0, new byte[] { 1, 2, 3 })], [], default, null, null, false, 0));
        var deny = new DfsSecurityDescriptor(DfsIdentity.User("admin"),
            [new DfsAccessRule(DfsIdentity.User("reader"), DfsFileSystemRights.ReadData, DfsAccessControlType.Deny)]);
        await admin.OkAsync<SetAccessControlResultMessage>(new SetAccessControlMessage(admin.Op(), "/secret", deny));

        var acl = await admin.OkAsync<GetAccessControlResultMessage>(new GetAccessControlMessage(admin.Header(), "/secret", 0));
        Assert.Equal(DfsIdentity.User("admin"), acl.Descriptor.Owner);
        Assert.Equal(DfsAccessControlType.Deny, acl.Descriptor.Rules[0].Type);
        Assert.Contains(acl.Descriptor.Rules, r => r.IsInherited && r.InheritedFrom == "/" && r.Identity == DfsIdentity.Everyone);

        var reader = new VolumeClient(cluster, "vol", new DfsCredentials("reader", "reader-secret-1"));
        await reader.OpenSessionAsync();
        await reader.FailsAsync(DfsErrorCode.AccessDenied, reader.OpenRequest("/secret", FileMode.Open, FileAccess.Read, FileShare.ReadWrite));
        var rights = await reader.OkAsync<GetEffectiveRightsResultMessage>(new GetEffectiveRightsMessage(reader.Header(), "/secret", null, 0));
        Assert.Equal(0, (int)(rights.Rights & DfsFileSystemRights.ReadData));
        Assert.NotEqual(0, (int)(rights.Rights & DfsFileSystemRights.WriteData));
        var forReader = await admin.OkAsync<GetEffectiveRightsResultMessage>(new GetEffectiveRightsMessage(admin.Header(), "/secret", DfsIdentity.User("reader"), 0));
        Assert.Equal(rights.Rights, forReader.Rights);
        // The reader may create files elsewhere (inherited Everyone FullControl); it owns them.
        await reader.OkAsync<WriteAllResultMessage>(new WriteAllMessage(reader.Op(), "/mine", 1, [RangeChange.ForInline(0, new byte[] { 7 })], [], default, null, null, false, 0));
        var mine = await admin.OkAsync<GetInfoResultMessage>(new GetInfoMessage(admin.Header(), "/mine", default, 0));
        Assert.Equal("reader", mine.Info!.Owner);
    }

    [Fact]
    public async Task Streams_AreWrittenListedAndDescribedIndependently()
    {
        (RuntimeCluster cluster, VolumeClient c) = await StartAsync();
        await using RuntimeCluster _ = cluster;
        await c.OkAsync<WriteAllResultMessage>(new WriteAllMessage(c.Op(), "/f", 2, [RangeChange.ForInline(0, new byte[] { 1, 1 })], [], default, null, null, false, 0));
        await c.OkAsync<WriteAllResultMessage>(new WriteAllMessage(c.Op(), "/f:meta", 3, [RangeChange.ForInline(0, new byte[] { 2, 2, 2 })], [], default, null, null, false, 0));
        var streams = await c.OkAsync<ListStreamsResultMessage>(new ListStreamsMessage(c.Header(), "/f", 0));
        StreamInfoData meta = Assert.Single(streams.Streams);
        Assert.Equal(("meta", 3L), (meta.Name, meta.Length));
        var info = await c.OkAsync<GetInfoResultMessage>(new GetInfoMessage(c.Header(), "/f:meta", default, 0));
        Assert.True(info.Info!.IsStream);
        Assert.Equal(3, info.Info.Length);
        Assert.Equal(meta.Revision, info.Info.Revision);
        var file = await c.OkAsync<GetInfoResultMessage>(new GetInfoMessage(c.Header(), "/f", default, 0));
        Assert.Equal(2, file.Info!.Length);
        Assert.Equal(1, file.Info.StreamCount);
        Assert.False((await c.OkAsync<GetInfoResultMessage>(new GetInfoMessage(c.Header(), "/f:none", default, 0))).Exists);
        await c.FailsAsync(DfsErrorCode.InvalidPath, new ListStreamsMessage(c.Header(), "/f:meta", 0));
        OpenHandleResultMessage h = await c.OpenAsync("/f:meta", FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        Assert.Equal(new byte[] { 2, 2, 2 }, await c.ReadAsync(h.Handle, 0, 3));
    }

    [Fact]
    public async Task Snapshots_ServeTheBoundaryVersionThroughReaderPins()
    {
        (RuntimeCluster cluster, VolumeClient c) = await StartAsync();
        await using RuntimeCluster _ = cluster;
        await c.OkAsync<WriteAllResultMessage>(new WriteAllMessage(c.Op(), "/s/v", 3, [RangeChange.ForInline(0, new byte[] { 1, 2, 3 })], [], default, null, null, true, 0));
        var created = await c.OkAsync<CreateSnapshotResultMessage>(new CreateSnapshotMessage(c.Op(), "before", null, DfsConsistencyLabel.FilesystemConsistent, [], null));
        await c.OkAsync<WriteAllResultMessage>(new WriteAllMessage(c.Op(), "/s/v", 1, [RangeChange.ForInline(0, new byte[] { 9 })], [], default, null, null, false, 0));
        await c.OkAsync<DeleteResultMessage>(new DeleteMessage(c.Op(), "/s/v", false, false, null));
        Assert.False((await c.OkAsync<GetInfoResultMessage>(new GetInfoMessage(c.Header(), "/s/v", default, 0))).Exists);

        var list = await c.OkAsync<ListSnapshotsResultMessage>(new ListSnapshotsMessage(c.Header(), null));
        Assert.Equal("before", Assert.Single(list.Snapshots).Name);
        var opened = await c.OkAsync<OpenSnapshotResultMessage>(new OpenSnapshotMessage(c.Header(), "before"));
        Assert.True(opened.PinDuration > TimeSpan.Zero);
        var info = await c.OkAsync<GetInfoResultMessage>(new GetInfoMessage(c.Header(), "/s/v", default, opened.SnapshotPin));
        Assert.True(info.Exists);
        Assert.Equal(new byte[] { 1, 2, 3 }, await c.ReadAsync(default, 0, 3, opened.SnapshotPin, info.Info!.Inode));
        var page = await c.OkAsync<ListDirectoryResultMessage>(new ListDirectoryMessage(c.Header(), "/s", "*", WireEntryKinds.All, 10, null, opened.SnapshotPin));
        Assert.Equal("v", Assert.Single(page.Entries).Name);
        Assert.True((await c.OkAsync<CloseSnapshotResultMessage>(new CloseSnapshotMessage(c.Header(), opened.SnapshotPin))).Released);
        await c.FailsAsync(DfsErrorCode.PinExpired, new GetInfoMessage(c.Header(), "/s/v", default, opened.SnapshotPin));
        Assert.Equal(created.Snapshot.Id, list.Snapshots[0].Id);
        await c.OkAsync<DeleteSnapshotResultMessage>(new DeleteSnapshotMessage(c.Op(), "before"));
        await c.FailsAsync(DfsErrorCode.SnapshotNotFound, new DeleteSnapshotMessage(c.Op(), "before"));
    }

    [Fact]
    public async Task AppendCopyAndAccessFlags_WorkThroughTheService()
    {
        (RuntimeCluster cluster, VolumeClient c) = await StartAsync(seed: 113);
        await using RuntimeCluster _ = cluster;
        byte[] head = Bytes(21, 100);
        await c.OkAsync<WriteAllResultMessage>(new WriteAllMessage(c.Op(), "/log", head.Length, [RangeChange.ForInline(0, head)], [], default, null, null, false, 0));

        // Appending an object to an inline file: offsets are relative to the end at apply; the leader materializes the head.
        CreateStagingResultMessage staging = await c.StagingAsync();
        byte[] tail = Bytes(22, 10_000);
        (RangeChange change, IReadOnlyList<ObjectReceipt> receipts) = await c.UploadAsync(staging, 0, tail);
        var appended = await c.OkAsync<AppendResultMessage>(new AppendMessage(c.Op(), "/log", default, 0, tail.Length, [change], receipts, staging.Staging, false,
            null, null));
        Assert.Equal((100L, 10_100L), (appended.OldLength, appended.NewLength));
        byte[] expected = [.. head, .. tail];
        OpenHandleResultMessage reader = await c.OpenAsync("/log", FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        Assert.False(reader.Info.IsInline);
        Assert.Equal(expected, await c.ReadAsync(reader.Handle, 0, expected.Length));
        await c.OkAsync<CloseHandleResultMessage>(new CloseHandleMessage(c.Header(), reader.Handle, 0));

        // A copy shares the objects and keeps its content when the source changes afterwards.
        await c.OkAsync<WriteAllResultMessage>(new WriteAllMessage(c.Op(), "/log:note", 2, [RangeChange.ForInline(0, new byte[] { 5, 6 })], [], default, null,
            null, false, 0));
        var copied = await c.OkAsync<CopyResultMessage>(new CopyMessage(c.Op(), "/log", "/copy", false, 0, null, null));
        Assert.True(copied.Created);
        await c.OkAsync<WriteAllResultMessage>(new WriteAllMessage(c.Op(), "/log", 1, [RangeChange.ForInline(0, new byte[] { 0 })], [], default, null, null, false, 0));
        OpenHandleResultMessage copy = await c.OpenAsync("/copy", FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        Assert.Equal(expected, await c.ReadAsync(copy.Handle, 0, expected.Length));
        StreamInfoData note = Assert.Single((await c.OkAsync<ListStreamsResultMessage>(new ListStreamsMessage(c.Header(), "/copy", 0))).Streams);
        Assert.Equal("note", note.Name);

        // Access flags: OwnerOnly shuts out other principals (even with volume grants); unknown policies are refused.
        RawClient control = await cluster.ConnectAsync(await cluster.WaitForLeaderAsync("control"));
        await control.AdminAsync<AdminGrantBody, AdminEmptyBody>(AdminOp.Grant, new AdminGrantBody("vol", "reader", DfsPermissions.Read | DfsPermissions.Write));
        await c.OkAsync<SetAccessResultMessage>(new SetAccessMessage(c.Op(), "/copy", true, false));
        var other = new VolumeClient(cluster, "vol", new DfsCredentials("reader", "reader-secret-1"));
        await other.OpenSessionAsync();
        await other.FailsAsync(DfsErrorCode.AccessDenied, other.OpenRequest("/copy", FileMode.Open, FileAccess.Read, FileShare.ReadWrite));
        await other.FailsAsync(DfsErrorCode.AccessDenied, new SetAccessMessage(other.Op(), "/copy", false, false));
        await c.FailsAsync(DfsErrorCode.InvalidPolicy, new SetPolicyMessage(c.Op(), "/copy", "no-such-policy"));
    }

    [Fact]
    public async Task StrongerPolicies_NeverLeaveDataOnlyInline()
    {
        // Three voters in three zones: any majority is 2 hosts in 2 zones, which does not satisfy an ack of 3 copies (LAY-002).
        (RuntimeCluster cluster, VolumeClient c) = await StartAsync(seed: 117);
        await using RuntimeCluster _ = cluster;
        RawClient control = await cluster.ConnectAsync(await cluster.WaitForLeaderAsync("control"));
        var strong = new DfsStoragePolicy { TargetCopies = 3, TargetDistinctZones = 3, DurableCopiesBeforeAck = 3, DurableZonesBeforeAck = 3 };
        await control.AdminAsync<AdminDefinePolicyBody, AdminEmptyBody>(AdminOp.DefinePolicy, new AdminDefinePolicyBody("vol", "strong", strong));
        await RuntimeCluster.WaitUntilAsync(async () => (await c.CallAsync(new SetPolicyMessage(c.Header(), "/probe", "strong")))
            is not ErrorReplyMessage { Code: DfsErrorCode.InvalidPolicy }, "the volume leader knows the new policy");

        byte[] data = [1, 2, 3, 4, 5];
        await c.OkAsync<WriteAllResultMessage>(new WriteAllMessage(c.Op(), "/a", data.Length, [RangeChange.ForInline(0, data)], [], default, null, null, false, 0));
        await c.OkAsync<WriteAllResultMessage>(new WriteAllMessage(c.Op(), "/b", data.Length, [RangeChange.ForInline(0, data)], [], default, null, null, false, 0));
        Assert.True((await c.OkAsync<GetInfoResultMessage>(new GetInfoMessage(c.Header(), "/a", default, 0))).Info!.IsInline);

        // A new policy on an inline file, and a copy under the strong policy, store the bytes as objects on 3 hosts.
        await c.OkAsync<SetPolicyResultMessage>(new SetPolicyMessage(c.Op(), "/a", "strong"));
        await c.OkAsync<CopyResultMessage>(new CopyMessage(c.Op(), "/b", "/c", false, 0, "strong", null));
        RuntimeNode leader = await cluster.WaitForLeaderAsync("vol");
        VolumeStateInspection state = (await leader.Host!.Inspect.GetVolumeStateAsync("vol"))!;
        foreach (string path in new[] { "/a", "/c" })
        {
            FileInspection file = Assert.IsType<FileInspection>(state.GetFile(path));
            Assert.False(file.IsInline, $"{path} is inline under a policy inline metadata cannot guarantee");
            ExtentInspection extent = Assert.Single(file.Extents);
            int holders = 0;
            foreach (RuntimeNode node in cluster.Running)
            {
                if (await node.Host!.InvokeAsync(s => Task.FromResult(s.ObjectStore.TryGetInfo(extent.Object) is not null)))
                    holders++;
            }

            Assert.True(holders >= 3, $"{path}: {holders} copies");
            OpenHandleResultMessage h = await c.OpenAsync(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            Assert.Equal(data, await c.ReadAsync(h.Handle, 0, data.Length));
        }

        Assert.True(Assert.IsType<FileInspection>(state.GetFile("/b")).IsInline);
    }

    [Fact]
    public async Task LeaderFailover_TheNextLeaderExpiresAbandonedLeasesWithoutClientTraffic()
    {
        (RuntimeCluster cluster, VolumeClient c) = await StartAsync(seed: 109);
        await using RuntimeCluster _ = cluster;
        OpenHandleResultMessage h = await c.OpenAsync("/abandoned", FileMode.CreateNew, FileAccess.Write, FileShare.Read);
        RuntimeNode leader = await cluster.WaitForLeaderAsync("vol");
        cluster.Kill(leader);

        // No request reaches the new leader: its own maintenance establishes readiness and expires the lease.
        await RuntimeCluster.WaitUntilAsync(async () =>
        {
            RuntimeNode next = await cluster.WaitForLeaderAsync("vol");
            VolumeStateInspection? state = await next.Host!.Inspect.GetVolumeStateAsync("vol");
            return state is not null && state.Handles.All(x => x.Id != h.Handle);
        }, "the next leader expires the abandoned handle", TimeSpan.FromSeconds(60));
    }

    [Fact]
    public async Task Enumeration_PagesOneVersionAndContinuationsAreBound()
    {
        (RuntimeCluster cluster, VolumeClient c) = await StartAsync();
        await using RuntimeCluster _ = cluster;
        for (int i = 0; i < 7; i++)
            await c.OkAsync<WriteAllResultMessage>(new WriteAllMessage(c.Op(), "/d/f" + i, 1, [RangeChange.ForInline(0, new byte[] { (byte)i })], [], default, null, null, true, 0));
        var first = await c.OkAsync<ListDirectoryResultMessage>(new ListDirectoryMessage(c.Header(), "/d", "*", WireEntryKinds.All, 3, null, 0));
        Assert.Equal(new[] { "f0", "f1", "f2" }, first.Entries.Select(e => e.Name));
        Assert.NotNull(first.Continuation);
        Assert.NotEqual(0UL, first.EnumerationPin);
        // The directory changes; the enumeration still returns its pinned version.
        await c.OkAsync<DeleteResultMessage>(new DeleteMessage(c.Op(), "/d/f4", false, false, null));
        await c.OkAsync<WriteAllResultMessage>(new WriteAllMessage(c.Op(), "/d/f9", 1, [RangeChange.ForInline(0, new byte[] { 9 })], [], default, null, null, false, 0));
        await c.FailsAsync(DfsErrorCode.ContinuationExpired, new ListDirectoryMessage(c.Header(), "/d", "f*", WireEntryKinds.All, 3, first.Continuation, 0));
        var second = await c.OkAsync<ListDirectoryResultMessage>(new ListDirectoryMessage(c.Header(), "/d", "*", WireEntryKinds.All, 3, first.Continuation, 0));
        var third = await c.OkAsync<ListDirectoryResultMessage>(new ListDirectoryMessage(c.Header(), "/d", "*", WireEntryKinds.All, 3, second.Continuation, 0));
        Assert.Equal(new[] { "f3", "f4", "f5" }, second.Entries.Select(e => e.Name));
        Assert.Equal(new[] { "f6" }, third.Entries.Select(e => e.Name));
        Assert.Null(third.Continuation);
        Assert.Equal(first.PinnedAt, third.PinnedAt);
        // The pin is released at the end: the continuation is expired.
        await RuntimeCluster.WaitUntilAsync(async () =>
            await c.CallAsync(new ListDirectoryMessage(c.Header(), "/d", "*", WireEntryKinds.All, 3, first.Continuation, 0)) is ErrorReplyMessage
            { Code: DfsErrorCode.ContinuationExpired }, "the enumeration pin is released");
    }

    [Fact]
    public async Task LeaderFailover_KeepsCommittedStateAndStoredOutcomes()
    {
        (RuntimeCluster cluster, VolumeClient c) = await StartAsync(seed: 107);
        await using RuntimeCluster _ = cluster;
        RequestHeader op = c.Op();
        var request = new WriteAllMessage(op, "/durable", 2, [RangeChange.ForInline(0, new byte[] { 4, 2 })], [], default, null, null, false, 0);
        var first = await c.OkAsync<WriteAllResultMessage>(request);
        RuntimeNode leader = await cluster.WaitForLeaderAsync("vol");
        cluster.Kill(leader);
        var retry = await c.OkAsync<WriteAllResultMessage>(request);
        Assert.Equal(first.Token, retry.Token);
        var info = await c.OkAsync<GetInfoResultMessage>(new GetInfoMessage(c.Header(), "/durable", default, 0));
        Assert.Equal(first.Revision, info.Info!.Revision);
    }

    [Fact]
    public async Task InlineToExtentsAndBack_AreMaterializedByTheLeader()
    {
        (RuntimeCluster cluster, VolumeClient c) = await StartAsync();
        await using RuntimeCluster _ = cluster;
        byte[] head = Bytes(5, 1000);
        await c.OkAsync<WriteAllResultMessage>(new WriteAllMessage(c.Op(), "/grow", head.Length, [RangeChange.ForInline(0, head)], [], default, null, null, false, 0));
        OpenHandleResultMessage w = await c.OpenAsync("/grow", FileMode.Open, FileAccess.ReadWrite, FileShare.Read);
        CreateStagingResultMessage staging = await c.StagingAsync(w.Handle);
        byte[] tail = Bytes(6, 20_000);
        (RangeChange change, IReadOnlyList<ObjectReceipt> receipts) = await c.UploadAsync(staging, head.Length, tail);
        // Appending objects to an inline file: the leader stores the old inline bytes as an object (PROTOCOL.md §8.1).
        var grown = await c.OkAsync<CommitFileResultMessage>(new CommitFileMessage(c.Op(), w.Handle, w.WriterGeneration, 0, -1, head.Length + tail.Length, [change],
            receipts, staging.Staging, null));
        Assert.Equal(21_000, grown.NewLength);
        var info = await c.OkAsync<GetInfoResultMessage>(new GetInfoMessage(c.Header(), "/grow", default, 0));
        Assert.False(info.Info!.IsInline);
        byte[] expected = [.. head, .. tail];
        Assert.Equal(expected, await c.ReadAsync(w.Handle, 0, 21_000));

        // Shrinking below the threshold makes it inline again: the leader reads the kept bytes back.
        await c.OkAsync<CommitFileResultMessage>(new CommitFileMessage(c.Op(), w.Handle, w.WriterGeneration, 0, 1500, 1500, [], [], staging.Staging, null));
        info = await c.OkAsync<GetInfoResultMessage>(new GetInfoMessage(c.Header(), "/grow", default, 0));
        Assert.True(info.Info!.IsInline);
        Assert.Equal(expected[..1500], await c.ReadAsync(w.Handle, 0, 1500));
    }

    private static async Task AssertReplicasConvergeAsync(RuntimeCluster cluster)
    {
        await RuntimeCluster.WaitUntilAsync(async () =>
        {
            var digests = new HashSet<string>(StringComparer.Ordinal);
            foreach (RuntimeNode node in cluster.Running)
            {
                if (await node.Host!.Inspect.ComputeGroupDigestAsync("vol") is { } digest)
                    digests.Add(digest);
            }

            return digests.Count == 1;
        }, "every replica applied identical volume state");
    }

    private static byte[] Bytes(int seed, int length)
    {
        var bytes = new byte[length];
        new Random(seed).NextBytes(bytes);
        return bytes;
    }
}
