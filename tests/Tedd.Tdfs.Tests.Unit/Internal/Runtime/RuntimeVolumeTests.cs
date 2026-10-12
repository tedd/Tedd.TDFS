using System.Text;
using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Server.Raft;
using Tedd.Tdfs.Server.Volumes;

namespace Tedd.Tdfs.Tests.Unit.Internal.Runtime;

/// <summary>Volume groups, leader failover, restart from disk and snapshot catch-up on the runtime core.</summary>
public sealed class RuntimeVolumeTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    internal static Task<ApplyOutcome> ProposeAsync(NodeHost host, string group, string command)
        => host.InvokeAsync(s => s.Raft.ProposeAsync(group, Encoding.UTF8.GetBytes(command), ProposalContext.None, CancellationToken.None));

    internal static Task<TestVolumeStateMachine?> VolumeStateAsync(NodeHost host, string volume)
        => host.InvokeAsync(s => s.Volumes.TryGet(volume)?.StateMachine as TestVolumeStateMachine);

    internal static Task<(long Count, string Hash, ulong Applied)> VolumeSummaryAsync(NodeHost host, string volume)
        => host.InvokeAsync(s =>
        {
            VolumeHost? h = s.Volumes.TryGet(volume);
            var sm = (TestVolumeStateMachine?)h?.StateMachine;
            return (sm?.Count ?? -1, sm?.Hash ?? string.Empty, h?.Group.AppliedIndex ?? 0);
        });

    [Fact]
    public async Task CreateVolume_FormsVolumeGroupOnChosenVotersWithALeader()
    {
        await using var cluster = new RuntimeCluster(4, seed: 21);
        await cluster.StartAllAsync();
        await cluster.WaitForRegistrationAsync();
        VolumeEntry volume = await cluster.CreateVolumeAsync("vol");

        Assert.Equal(3, volume.Voters.Count);
        Assert.Equal(3, volume.Voters.Distinct().Count());
        var voterNodes = cluster.Nodes.Where(n => volume.Voters.Contains(n.Id)).ToArray();
        Assert.Equal(3, voterNodes.Select(n => n.Labels.Zone).Distinct(StringComparer.Ordinal).Count());
        RuntimeNode outsider = cluster.Nodes.Single(n => !volume.Voters.Contains(n.Id));

        RuntimeNode leader = await cluster.WaitForLeaderAsync("vol");
        Assert.Contains(leader, voterNodes);
        foreach (RuntimeNode node in voterNodes)
        {
            GroupInspection g = await WaitForGroupAsync(node, "vol");
            Assert.Equal(volume.Voters.Count, g.Voters.Count);
        }

        Assert.Null(await outsider.Host!.Inspect.GetGroupAsync("vol"));

        for (int i = 0; i < 5; i++)
        {
            ApplyOutcome outcome = await ProposeAsync(leader.Host!, ProtocolInfo.VolumeGroup(volume.Id), "cmd-" + i);
            Assert.True(outcome.IsSuccess);
            Assert.Equal((long)(i + 1), outcome.Value);
            Assert.True(outcome.Index > 0);
        }

        string expected = (await VolumeSummaryAsync(leader.Host!, "vol")).Hash;
        await RuntimeCluster.WaitUntilAsync(async () =>
        {
            foreach (RuntimeNode node in voterNodes)
            {
                (long count, string hash, _) = await VolumeSummaryAsync(node.Host!, "vol");
                if (count != 5 || hash != expected)
                    return false;
            }

            return true;
        }, "every voter applied the five commands identically");

        // A follower refuses proposals with a leader hint; GetVolumeRoute(RequireLeader) answers on the leader only.
        RuntimeNode follower = voterNodes.First(n => n != leader);
        var notLeader = await Assert.ThrowsAsync<DfsUnavailableException>(() => ProposeAsync(follower.Host!, ProtocolInfo.VolumeGroup(volume.Id), "x"));
        Assert.Equal(DfsErrorCode.NotLeader, notLeader.ErrorCode);
        Assert.Equal(leader.Endpoint, notLeader.LeaderHint);
        RawClient client = await cluster.ConnectAsync(leader);
        var route = await client.CallOrThrowAsync<VolumeRouteResultMessage>(new GetVolumeRouteMessage("vol", RequireLeader: true));
        Assert.Equal(leader.Id, route.Leader);
        Assert.Equal(volume.Id, route.LastCommit.VolumeId);
        Assert.True(route.LastCommit.Sequence >= 5);
        RawClient followerClient = await cluster.ConnectAsync(follower);
        var redirect = Assert.IsType<ErrorReplyMessage>(await followerClient.CallAsync(new GetVolumeRouteMessage("vol", RequireLeader: true)));
        Assert.Equal(DfsErrorCode.NotLeader, redirect.Code);
        Assert.Equal(leader.Endpoint, redirect.LeaderHint);

        var names = await client.AdminAsync<AdminEmptyBody, AdminNameListBody>(AdminOp.ListVolumes, new AdminEmptyBody());
        Assert.Equal(["vol"], names.Names);
        var missing = Assert.IsType<ErrorReplyMessage>(await client.CallAsync(new GetVolumeRouteMessage("nope")));
        Assert.Equal(DfsErrorCode.VolumeNotFound, missing.Code);
    }

    [Fact]
    public async Task ControlLeaderCrash_ElectsANewLeaderAndKeepsCommittedState()
    {
        await using var cluster = new RuntimeCluster(3, seed: 22);
        await cluster.StartAllAsync();
        await cluster.WaitForRegistrationAsync();
        RuntimeNode leader = await cluster.WaitForLeaderAsync("control");
        GroupInspection before = (await leader.Host!.Inspect.GetGroupAsync("control"))!;
        RawClient admin = await cluster.ConnectAsync(leader);
        await admin.AdminAsync<AdminCreatePrincipalBody, AdminEmptyBody>(AdminOp.CreatePrincipal, new AdminCreatePrincipalBody("alice", "alice-secret-1", false));

        cluster.Kill(leader);
        RuntimeNode next = await cluster.WaitForLeaderAsync("control");
        Assert.NotEqual(leader.Name, next.Name);
        GroupInspection after = (await next.Host!.Inspect.GetGroupAsync("control"))!;
        Assert.True(after.Term > before.Term);

        // The committed principal survived the failover, and the new leader accepts configuration changes.
        RawClient alice = await cluster.ConnectAsync(next, new DfsCredentials("alice", "alice-secret-1"));
        Assert.Equal("alice", alice.Outcome.Result.Principal);
        RawClient admin2 = await cluster.ConnectAsync(next);
        await admin2.AdminAsync<AdminCreatePrincipalBody, AdminEmptyBody>(AdminOp.CreatePrincipal, new AdminCreatePrincipalBody("bob", "bob-secret-1", false));
        var status = await admin2.AdminAsync<AdminEmptyBody, AdminClusterStatusBody>(AdminOp.ClusterStatus, new AdminEmptyBody());
        Assert.Equal(next.Name, status.Control.Leader);
        Assert.True(status.Control.HasQuorum);
        Assert.Equal(3, status.Nodes.Count);
        Assert.False(status.Nodes.Single(n => n.Name == leader.Name).IsReachable);

        // A non-leader answers NotLeader with the new leader's endpoint.
        RuntimeNode other = cluster.Running.First(n => n != next);
        RawClient otherAdmin = await cluster.ConnectAsync(other);
        var error = Assert.IsType<ErrorReplyMessage>(await otherAdmin.CallAsync(
            AdminRequestMessage.Create(AdminOp.CreatePrincipal, new UInt128(9, 9), new AdminCreatePrincipalBody("carol", "carol-secret-1", false))));
        Assert.Equal(DfsErrorCode.NotLeader, error.Code);
        Assert.True(error.Retryable);
        Assert.Equal(next.Endpoint, error.LeaderHint);
    }

    [Fact]
    public async Task Restart_RecoversStateFromDiskAndBumpsTheIncarnation()
    {
        await using var cluster = new RuntimeCluster(3, seed: 23);
        await cluster.StartAllAsync();
        await cluster.WaitForRegistrationAsync();
        VolumeEntry volume = await cluster.CreateVolumeAsync("vol");
        string group = ProtocolInfo.VolumeGroup(volume.Id);
        RuntimeNode leader = await cluster.WaitForLeaderAsync("vol");
        for (int i = 0; i < 10; i++)
            await ProposeAsync(leader.Host!, group, "before-" + i);

        RuntimeNode stopped = cluster.Nodes.First(n => n != leader);
        RuntimeNode crashed = cluster.Nodes.Last(n => n != leader);
        await RuntimeCluster.WaitUntilAsync(async () => (await VolumeSummaryAsync(stopped.Host!, "vol")).Count == 10
            && (await VolumeSummaryAsync(crashed.Host!, "vol")).Count == 10, "followers applied the commands");
        ulong stoppedIncarnation = stopped.Host!.Incarnation;
        ulong crashedIncarnation = crashed.Host!.Incarnation;

        await cluster.StopAsync(stopped);
        cluster.Kill(crashed);
        await cluster.StartAsync(stopped);
        await cluster.StartAsync(crashed);
        Assert.Equal(stoppedIncarnation + 1, stopped.Host!.Incarnation);
        Assert.Equal(crashedIncarnation + 1, crashed.Host!.Incarnation);
        Assert.Equal(stopped.Id, stopped.Host.Id);

        // The state is recovered from the local journal (replay) and the cluster keeps working.
        await RuntimeCluster.WaitUntilAsync(async () => (await VolumeSummaryAsync(stopped.Host!, "vol")).Count == 10
            && (await VolumeSummaryAsync(crashed.Host!, "vol")).Count == 10, "restarted nodes recovered the applied commands");
        await cluster.WaitForRegistrationAsync();
        RuntimeNode volumeLeader = await cluster.WaitForLeaderAsync("vol");
        await ProposeAsync(volumeLeader.Host!, group, "after");
        string expected = (await VolumeSummaryAsync(volumeLeader.Host!, "vol")).Hash;
        await RuntimeCluster.WaitUntilAsync(async () =>
        {
            foreach (RuntimeNode node in cluster.Nodes)
            {
                if ((await VolumeSummaryAsync(node.Host!, "vol")) is not (11, var hash, _) || hash != expected)
                    return false;
            }

            return true;
        }, "every node applied the post-restart command identically");

        RuntimeNode controlLeader = await cluster.WaitForLeaderAsync("control");
        ClusterMapData map = await controlLeader.Host!.InvokeAsync(s => s.Control.BuildClusterMap());
        Assert.Equal(stoppedIncarnation + 1, map.Nodes.Single(n => n.Name == stopped.Name).Incarnation);
        Assert.Equal(crashedIncarnation + 1, map.Nodes.Single(n => n.Name == crashed.Name).Incarnation);
        Assert.Contains(map.Volumes, v => v.Name == "vol" && v.Id == volume.Id);
    }

    [Fact]
    public async Task LaggingNode_CatchesUpWithASnapshotAfterCompaction()
    {
        NodeTimings timings = NodeTimings.Fast with { RaftSnapshotThreshold = 20 };
        await using var cluster = new RuntimeCluster(3, timings: timings, seed: 24);
        await cluster.StartAllAsync();
        await cluster.WaitForRegistrationAsync();
        VolumeEntry volume = await cluster.CreateVolumeAsync("vol");
        string group = ProtocolInfo.VolumeGroup(volume.Id);
        RuntimeNode leader = await cluster.WaitForLeaderAsync("vol");
        RuntimeNode lagging = cluster.Nodes.First(n => n != leader);
        GroupInspection laggingBefore = await WaitForGroupAsync(lagging, "vol");

        await cluster.StopAsync(lagging);
        RuntimeNode current = await cluster.WaitForLeaderAsync("vol");
        for (int i = 0; i < 70; i++)
        {
            try
            {
                await ProposeAsync(current.Host!, group, "while-down-" + i);
            }
            catch (DfsUnavailableException)
            {
                current = await cluster.WaitForLeaderAsync("vol");
                i--;
            }
        }

        GroupInspection leaderView = (await current.Host!.Inspect.GetGroupAsync("vol"))!;
        Assert.True(leaderView.SnapshotIndex > laggingBefore.LastIndex + 1, "the leader compacted past the lagging node's log");

        await cluster.StartAsync(lagging);
        (long expectedCount, string expectedHash, _) = await VolumeSummaryAsync(current.Host!, "vol");
        await RuntimeCluster.WaitUntilAsync(async () =>
        {
            (long count, string hash, _) = await VolumeSummaryAsync(lagging.Host!, "vol");
            return count == expectedCount && hash == expectedHash;
        }, "the restarted node reaches the leader's state");
        GroupStats stats = (await lagging.Host!.Inspect.GetGroupStatsAsync("vol"))!;
        Assert.True(stats.SnapshotsInstalled >= 1, "the catch-up used a snapshot");
        Assert.True(stats.EntriesCoveredByInstall > 0);

        // The group keeps working with the caught-up member, and the digests match.
        await ProposeAsync(current.Host!, group, "after-catch-up");
        await RuntimeCluster.WaitUntilAsync(async () =>
            await lagging.Host!.Inspect.ComputeGroupDigestAsync("vol") == await current.Host!.Inspect.ComputeGroupDigestAsync("vol"),
            "the lagging replica's digest matches the leader's");
    }

    internal static async Task<GroupInspection> WaitForGroupAsync(RuntimeNode node, string group)
    {
        GroupInspection? result = null;
        await RuntimeCluster.WaitUntilAsync(async () => (result = await node.Host!.Inspect.GetGroupAsync(group)) is { Leader: not null },
            $"{node.Name} hosts group '{group}' with a known leader");
        return result!;
    }
}
