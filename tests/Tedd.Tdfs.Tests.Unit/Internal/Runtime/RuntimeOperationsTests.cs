using System.Text;
using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Data;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Server.Raft;
using Tedd.Tdfs.Server.Security;

namespace Tedd.Tdfs.Tests.Unit.Internal.Runtime;

/// <summary>Administrative operations, fault sites, voter replacement, leadership transfer and pausing.</summary>
public sealed class RuntimeOperationsTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task AdminOperations_ChangeTheControlStateAndAreDeduplicated()
    {
        await using var cluster = new RuntimeCluster(3, seed: 51);
        await cluster.StartAllAsync();
        await cluster.WaitForRegistrationAsync();
        VolumeEntry volume = await cluster.CreateVolumeAsync("vol");
        RuntimeNode leader = await cluster.WaitForLeaderAsync("control");
        RawClient admin = await cluster.ConnectAsync(leader);

        // Principals, deduplicated by request id.
        var request = AdminRequestMessage.Create(AdminOp.CreatePrincipal, new UInt128(5, 5), new AdminCreatePrincipalBody("alice", "alice-secret-1", false));
        await admin.CallOrThrowAsync<AdminResultMessage>(request);
        await admin.CallOrThrowAsync<AdminResultMessage>(request);
        var duplicate = Assert.IsType<ErrorReplyMessage>(await admin.CallAsync(
            AdminRequestMessage.Create(AdminOp.CreatePrincipal, new UInt128(5, 6), new AdminCreatePrincipalBody("alice", "x", false))));
        Assert.Equal(DfsErrorCode.AlreadyExists, duplicate.Code);
        await admin.AdminAsync<AdminGrantBody, AdminEmptyBody>(AdminOp.Grant, new AdminGrantBody("vol", "alice", DfsPermissions.Read | DfsPermissions.Write));
        await admin.AdminAsync<AdminGrantBody, AdminEmptyBody>(AdminOp.Revoke, new AdminGrantBody("vol", "alice", DfsPermissions.Write));
        DfsPermissions granted = await leader.Host!.InvokeAsync(s => s.Control.State.GetPermissions("alice", volume.Id));
        Assert.Equal(DfsPermissions.Read, granted);

        // Groups.
        await admin.AdminAsync<AdminGroupNameBody, AdminEmptyBody>(AdminOp.CreateGroup, new AdminGroupNameBody("eng"));
        await admin.AdminAsync<AdminGroupMemberBody, AdminEmptyBody>(AdminOp.AddGroupMember, new AdminGroupMemberBody("eng", "alice"));
        var groups = await admin.AdminAsync<AdminEmptyBody, AdminGroupListBody>(AdminOp.ListGroups, new AdminEmptyBody());
        Assert.Equal(["alice"], groups.Groups.Single(g => g.Name == "eng").Members);

        // Pools: default is implicit and lists every node; an explicit pool bumps the placement epoch.
        ulong epochBefore = await leader.Host.InvokeAsync(s => s.Control.State.PlacementEpoch);
        await admin.AdminAsync<AdminPoolMembersBody, AdminEmptyBody>(AdminOp.CreatePool, new AdminPoolMembersBody("fast", ["n1", "n2"]));
        var pools = await admin.AdminAsync<AdminEmptyBody, AdminPoolListBody>(AdminOp.ListPools, new AdminEmptyBody());
        Assert.Equal(["default", "fast"], pools.Pools.Select(p => p.Name).ToArray());
        Assert.Equal(["n0", "n1", "n2"], pools.Pools[0].Nodes);
        Assert.Equal(["vol"], pools.Pools[0].Volumes);
        Assert.True(await leader.Host.InvokeAsync(s => s.Control.State.PlacementEpoch) > epochBefore);
        var unknownNode = Assert.IsType<ErrorReplyMessage>(await admin.CallAsync(
            AdminRequestMessage.Create(AdminOp.CreatePool, new UInt128(8, 1), new AdminPoolMembersBody("slow", ["n7"]))));
        Assert.Equal(DfsErrorCode.InvalidArgument, unknownNode.Code);
        var status = await admin.AdminAsync<AdminPoolNameBody, AdminPoolStatusBody>(AdminOp.PoolStatus, new AdminPoolNameBody("fast"));
        Assert.Equal(["n1", "n2"], status.Info.Nodes);

        // Features, key rotation and the recovery-key escrow.
        await admin.AdminAsync<AdminSetFeatureBody, AdminEmptyBody>(AdminOp.SetFeature, new AdminSetFeatureBody(Server.Runtime.ProtocolFeatures.Probe, true));
        var cluster2 = await admin.AdminAsync<AdminEmptyBody, AdminClusterStatusBody>(AdminOp.ClusterStatus, new AdminEmptyBody());
        Assert.Contains(Server.Runtime.ProtocolFeatures.Probe, cluster2.ActiveFeatures);
        var unsupported = Assert.IsType<ErrorReplyMessage>(await admin.CallAsync(
            AdminRequestMessage.Create(AdminOp.SetFeature, new UInt128(8, 2), new AdminSetFeatureBody("no-such-feature", true))));
        Assert.Equal(DfsErrorCode.Unsupported, unsupported.Code);
        var rotation = await admin.AdminAsync<AdminVolumeNameBody, AdminKeyRotationBody>(AdminOp.RotateDataKey, new AdminVolumeNameBody("vol"));
        Assert.Equal(2UL, rotation.KeyVersion);
        var escrow = await admin.AdminAsync<AdminExportRecoveryKeyBody, AdminRecoveryKeyBody>(AdminOp.ExportRecoveryKey, new AdminExportRecoveryKeyBody("pass phrase"));
        Assert.Equal(new KeyHierarchy(cluster.Secret).StorageKek, DataKeys.ImportEscrow(escrow.Escrow.Span, "pass phrase"));
        Assert.Throws<DfsRecoveryKeyMissingException>(() => DataKeys.ImportEscrow(escrow.Escrow.Span, "wrong"));

        // Node administration.
        await admin.AdminAsync<AdminSetNodeLabelsBody, AdminEmptyBody>(AdminOp.SetNodeLabels,
            new AdminSetNodeLabelsBody("n2", new DfsTopologyLabels("r1", "z9", "rk1", "h2", "d1")));
        await admin.AdminAsync<AdminNodeNameBody, AdminEmptyBody>(AdminOp.DrainNode, new AdminNodeNameBody("n2"));
        if (leader.Name == "n2")
        {
            // A draining control voter leaves the control voters once enough active ones remain: when it led the group, the
            // remaining requests go to its successor.
            leader = await cluster.WaitForLeaderAsync("control", cluster.Nodes.Where(n => n.Name != "n2"));
            admin = await cluster.ConnectAsync(leader);
        }

        var nodes = await admin.AdminAsync<AdminEmptyBody, AdminNodeListBody>(AdminOp.ListNodes, new AdminEmptyBody());
        NodeEntry n2 = nodes.Nodes.Single(n => n.Name == "n2");
        Assert.Equal(WireNodeStatus.Draining, n2.Status);
        Assert.Equal("z9", n2.Labels.Zone);
        Assert.False(n2.PlacementEligible);

        // Volume-scoped operations belong to the volume service; this cluster runs the test state machine, which no volume
        // service serves.
        var volumeStatus = Assert.IsType<ErrorReplyMessage>(await admin.CallAsync(
            AdminRequestMessage.Create(AdminOp.VolumeStatus, new UInt128(8, 3), new AdminVolumeNameBody("vol"))));
        Assert.Equal(DfsErrorCode.Unavailable, volumeStatus.Code);

        // The audit log names principal, operation, outcome and scope; never secrets.
        var audit = await admin.AdminAsync<AdminAuditLogRequestBody, AdminNameListBody>(AdminOp.AuditLog, new AdminAuditLogRequestBody(100));
        string text = string.Join('\n', audit.Names);
        Assert.Contains("principal=admin op=CreatePrincipal outcome=ok scope=principal=alice", text, StringComparison.Ordinal);
        Assert.Contains("op=ExportRecoveryKey", text, StringComparison.Ordinal);
        Assert.Contains("op=CreateVolume outcome=ok scope=volume=vol", text, StringComparison.Ordinal);
        Assert.DoesNotContain("alice-secret-1", text, StringComparison.Ordinal);
        Assert.DoesNotContain("pass phrase", text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task FaultSites_AreReachedAndDropReplyIsHonoured()
    {
        await using var cluster = new RuntimeCluster(3, seed: 52);
        var faults = new RecordingFaultInjector();
        cluster.Faults = faults;
        await cluster.StartAllAsync();
        await cluster.WaitForRegistrationAsync();
        Assert.Contains(faults.Hits, h => h.Site == FaultSites.RecoveryJournalReplay);
        VolumeEntry volume = await cluster.CreateVolumeAsync("vol");
        RuntimeNode leader = await cluster.WaitForLeaderAsync("vol");
        faults.Clear();

        var operation = new DfsOperationId(3, 4);
        ApplyOutcome outcome = await leader.Host!.InvokeAsync(s => s.Raft.ProposeAsync(ProtocolInfo.VolumeGroup(volume.Id), Encoding.UTF8.GetBytes("x"),
            new ProposalContext(DfsOperationKind.WriteAll, operation, "/f"), CancellationToken.None));
        Assert.True(outcome.IsSuccess);
        await RuntimeCluster.WaitUntilAsync(() => Task.FromResult(faults.Hits.Count(h => h.Site == FaultSites.RaftAfterCommitBeforeApply && h.Volume == "vol") >= 3),
            "every replica reported the apply boundary");
        foreach (string site in new[] { FaultSites.RaftBeforePropose, FaultSites.RaftAfterLocalAppend, FaultSites.ApplyAfterBeforeReply })
        {
            FaultContext hit = Assert.Single(faults.Hits, h => h.Site == site && h.Volume == "vol");
            Assert.Equal(leader.Name, hit.Node);
            Assert.Equal("vol", hit.Volume);
            Assert.Equal(DfsOperationKind.WriteAll, hit.Operation);
            Assert.Equal(operation, hit.OperationId);
            Assert.Equal(FaultSites.StageOf(site), hit.Stage);
        }

        // Data path: object.write → object.before-flush → object.after-flush, inside rpc.request.accepted/rpc.reply.send.
        faults.Clear();
        var keys = new KeyHierarchy(cluster.Secret);
        byte[] data = Encoding.UTF8.GetBytes("payload");
        ObjectId obj = ObjectId.New(new SeededRandom(1));
        byte[] capability = DataCapability.ForStaging(cluster.ClusterId, volume.Id, 1, RuntimeCluster.AdminPrincipal, new StagingId(1),
            DateTimeOffset.UtcNow.AddMinutes(1)).Sign(keys.CapabilityKey).Encode();
        RawClient client = await cluster.ConnectAsync(leader);
        await client.CallOrThrowAsync<PutObjectResultMessage>(new PutObjectMessage(capability, volume.Id, new StagingId(1), obj, ObjectChecksum.Compute(data),
            data.Length, ObjectEncodings.Plain, 1, data));
        string[] order = faults.Hits.Where(h => h.Operation == DfsOperationKind.PutObject).Select(h => h.Site).ToArray();
        Assert.Equal([FaultSites.RpcRequestAccepted, FaultSites.ObjectWrite, FaultSites.ObjectBeforeFlush, FaultSites.ObjectAfterFlushBeforeReceipt,
            FaultSites.RpcReplySend], order);

        // A dropped reply: the request runs, the caller never hears back.
        faults.OnHit = h => h.Site == FaultSites.RpcReplySend && h.Operation == DfsOperationKind.Discover
            ? throw new FaultReplyDroppedException()
            : ValueTask.CompletedTask;
        await Assert.ThrowsAsync<TimeoutException>(() => client.Connection.CallAsync(MessageCodec.Encode(new GetClusterMapMessage()), TimeSpan.FromMilliseconds(500), Ct));
        faults.OnHit = null;
        await client.CallOrThrowAsync<ClusterMapResultMessage>(new GetClusterMapMessage());
    }

    [Fact]
    public async Task RemovedVoter_IsReplacedInTheVolumeGroup()
    {
        await using var cluster = new RuntimeCluster(4, seed: 53);
        await cluster.StartAllAsync();
        await cluster.WaitForRegistrationAsync();
        VolumeEntry volume = await cluster.CreateVolumeAsync("vol");
        RuntimeNode controlLeader = await cluster.WaitForLeaderAsync("control");
        RuntimeNode volumeLeader = await cluster.WaitForLeaderAsync("vol");
        RuntimeNode victim = cluster.Nodes.First(n => volume.Voters.Contains(n.Id) && n != controlLeader && n != volumeLeader);
        RuntimeNode spare = cluster.Nodes.Single(n => !volume.Voters.Contains(n.Id));
        await cluster.StopAsync(victim);

        RawClient admin = await cluster.ConnectAsync(controlLeader);
        await admin.AdminAsync<AdminRemoveNodeBody, AdminEmptyBody>(AdminOp.RemoveNode, new AdminRemoveNodeBody(victim.Name, Force: true));
        await RuntimeCluster.WaitUntilAsync(async () =>
        {
            RuntimeNode leader = await cluster.WaitForLeaderAsync("vol");
            GroupInspection? g = await leader.Host!.Inspect.GetGroupAsync("vol");
            return g is not null && g.Voters.Count == 3 && !g.Voters.Contains(victim.Name) && g.Voters.Contains(spare.Name) && g.Learners.Count == 0;
        }, "the volume group replaced the removed voter with the spare node");

        // The new voter hosts the group and applies new commands.
        RuntimeNode current = await cluster.WaitForLeaderAsync("vol");
        await RuntimeVolumeTests.ProposeAsync(current.Host!, ProtocolInfo.VolumeGroup(volume.Id), "after-replacement");
        await RuntimeCluster.WaitUntilAsync(async () => (await RuntimeVolumeTests.VolumeSummaryAsync(spare.Host!, "vol")).Count >= 1,
            "the replacement applied the new command");
        ClusterMapData map = await controlLeader.Host!.InvokeAsync(s => s.Control.BuildClusterMap());
        VolumeEntry entry = map.Volumes.Single(v => v.Name == "vol");
        Assert.DoesNotContain(victim.Id, entry.Voters);
        Assert.Contains(spare.Id, entry.Voters);
    }

    [Fact]
    public async Task LeadershipTransfer_MovesTheControlLeader()
    {
        await using var cluster = new RuntimeCluster(3, seed: 54);
        await cluster.StartAllAsync();
        await cluster.WaitForRegistrationAsync();
        RuntimeNode leader = await cluster.WaitForLeaderAsync("control");
        RuntimeNode target = cluster.Nodes.First(n => n != leader);
        await RuntimeCluster.WaitUntilAsync(async () =>
        {
            GroupInspection? l = await leader.Host!.Inspect.GetGroupAsync("control");
            GroupInspection? t = await target.Host!.Inspect.GetGroupAsync("control");
            return t is not null && l is not null && t.AppliedIndex == l.CommitIndex;
        }, "the target caught up");
        bool moved = await leader.Host!.InvokeAsync(s => s.Raft.TransferLeadershipAsync(ProtocolInfo.ControlGroup, target.Id, TimeSpan.FromSeconds(10)));
        Assert.True(moved);
        RuntimeNode next = await cluster.WaitForLeaderAsync("control");
        Assert.Equal(target.Name, next.Name);
    }

    [Fact]
    public async Task PausedLeader_LosesLeadershipAndRejoinsAsFollower()
    {
        await using var cluster = new RuntimeCluster(3, seed: 55);
        await cluster.StartAllAsync();
        await cluster.WaitForRegistrationAsync();
        RuntimeNode leader = await cluster.WaitForLeaderAsync("control");
        GroupInspection before = (await leader.Host!.Inspect.GetGroupAsync("control"))!;
        leader.Host.Pause();
        RuntimeNode next = await cluster.WaitForLeaderAsync("control", cluster.Nodes.Where(n => n != leader));
        Assert.NotEqual(leader.Name, next.Name);
        leader.Host.Resume();
        await RuntimeCluster.WaitUntilAsync(async () =>
        {
            GroupInspection? g = await leader.Host.Inspect.GetGroupAsync("control");
            return g is not null && g.Role == RaftRole.Follower && g.Leader == next.Name && g.Term > before.Term;
        }, "the resumed old leader follows the new leader");
    }
}
