using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Server.Runtime;
using Tedd.Tdfs.Server.Security;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Tests.Unit.Internal.Runtime;

/// <summary>Seed join, voter promotion and removed-member fencing on the runtime core.</summary>
public sealed class RuntimeMembershipTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task SeedJoin_FourthNodeJoinsAsLearnerAndReceivesTheControlLog()
    {
        await using var cluster = new RuntimeCluster(3, seed: 31);
        await cluster.StartAllAsync();
        await cluster.WaitForRegistrationAsync();
        RuntimeNode leader = await cluster.WaitForLeaderAsync("control");
        RuntimeNode joiner = cluster.AddJoiningNode("z4", [cluster[2].Endpoint]);
        await cluster.StartAsync(joiner);

        // The join committed AddMember on the control leader (following its hint from the seed).
        ClusterMapData map = await leader.Host!.InvokeAsync(s => s.Control.BuildClusterMap());
        NodeEntry entry = map.Nodes.Single(n => n.Name == joiner.Name);
        Assert.Equal(joiner.Host!.Id, entry.Id);
        Assert.Equal(joiner.Endpoint, entry.Endpoints[0]);

        // The membership manager adds it as a learner of the control group; it receives the whole log.
        await RuntimeCluster.WaitUntilAsync(async () =>
        {
            GroupInspection? g = await leader.Host!.Inspect.GetGroupAsync("control");
            return g is not null && g.Learners.Contains(joiner.Name);
        }, "the joiner is a control learner");
        await RuntimeCluster.WaitUntilAsync(async () =>
        {
            GroupInspection? leaderGroup = await leader.Host!.Inspect.GetGroupAsync("control");
            GroupInspection? joinerGroup = await joiner.Host!.Inspect.GetGroupAsync("control");
            return joinerGroup is { Role: RaftRole.Learner } && joinerGroup.AppliedIndex >= leaderGroup!.CommitIndex && joinerGroup.Leader == leader.Name;
        }, "the joiner caught up with the control log");
        ClusterMapData joinerMap = await joiner.Host.InvokeAsync(s => s.Control.BuildClusterMap());
        Assert.Equal(cluster.ClusterId, joinerMap.ClusterId);
        Assert.Equal(4, joinerMap.Nodes.Count);
        await cluster.WaitForRegistrationAsync();

        // A client can authenticate on the joined node with principals replicated to it.
        RawClient client = await cluster.ConnectAsync(joiner);
        Assert.Equal(cluster.ClusterId, client.Outcome.Challenge.ClusterId);
    }

    [Fact]
    public async Task SeedJoin_PromotesLearnersUntilTheVoterTarget()
    {
        // A single-node bootstrap targets three control voters; two seed-joined nodes are promoted once caught up.
        await using var cluster = new RuntimeCluster(1, seed: 32);
        await cluster.StartAllAsync();
        await cluster.WaitForRegistrationAsync();
        RuntimeNode a = cluster.AddJoiningNode("z2", [cluster[0].Endpoint]);
        RuntimeNode b = cluster.AddJoiningNode("z3", [cluster[0].Endpoint]);
        await cluster.StartAsync(a);
        await cluster.StartAsync(b);
        await RuntimeCluster.WaitUntilAsync(async () =>
        {
            GroupInspection? g = await cluster[0].Host!.Inspect.GetGroupAsync("control");
            return g is not null && g.Voters.Count == 3 && g.Learners.Count == 0;
        }, "both joined nodes became control voters");
        await cluster.WaitForRegistrationAsync();

        // The promoted group survives the loss of the original node.
        cluster.Kill(cluster[0]);
        RuntimeNode leader = await cluster.WaitForLeaderAsync("control", [a, b]);
        Assert.Contains(leader, new[] { a, b });
    }

    [Fact]
    public async Task RemovedNode_IsRejectedAndCannotRegainAuthority()
    {
        await using var cluster = new RuntimeCluster(4, seed: 33);
        await cluster.StartAllAsync();
        await cluster.WaitForRegistrationAsync();
        RuntimeNode leader = await cluster.WaitForLeaderAsync("control");
        RuntimeNode victim = cluster.Nodes.First(n => n != leader && n.Index >= 3);
        NodeId victimId = victim.Id;
        await cluster.StopAsync(victim);

        RawClient admin = await cluster.ConnectAsync(leader);
        await admin.AdminAsync<AdminRemoveNodeBody, AdminEmptyBody>(AdminOp.RemoveNode, new AdminRemoveNodeBody(victim.Name, Force: true));
        var nodes = await admin.AdminAsync<AdminEmptyBody, AdminNodeListBody>(AdminOp.ListNodes, new AdminEmptyBody());
        Assert.Equal(WireNodeStatus.Removed, nodes.Nodes.Single(n => n.Name == victim.Name).Status);
        await RuntimeCluster.WaitUntilAsync(async () =>
        {
            GroupInspection? g = await leader.Host!.Inspect.GetGroupAsync("control");
            return g is not null && !g.Voters.Contains(victim.Name) && !g.Learners.Contains(victim.Name);
        }, "the removed node left the control group");
        GroupInspection controlBefore = (await leader.Host!.Inspect.GetGroupAsync("control"))!;

        // A handshake with the removed identity (right secret, newer incarnation) is refused with AccessDenied.
        var keys = new KeyHierarchy(cluster.Secret);
        IRpcTransport transport = cluster.Network.Factory.Create("impostor", "mem://impostor");
        await using (transport)
        {
            IRpcConnection connection = await transport.ConnectAsync(leader.Endpoint, Ct);
            var credentials = new NodeCredentials(cluster.ClusterId, victim.Name, victimId, 99, 1234, keys.NodeAuthKey, ProtocolFeatures.Supported);
            await Assert.ThrowsAsync<UnauthorizedAccessException>(() =>
                HandshakeClient.AuthenticateNodeAsync(connection, credentials, null, new SeededRandom(3), TimeSpan.FromSeconds(10), Ct));
        }

        // The removed node restarts with its old disk: it never leads and does not disturb the healthy group.
        await cluster.StartAsync(victim);
        for (int probe = 0; probe < 10; probe++)
        {
            await Task.Delay(100, Ct);
            GroupInspection? own = await victim.Host!.Inspect.GetGroupAsync("control");
            Assert.True(own is null || own.Role != RaftRole.Leader, "the removed node claims control leadership");
        }

        GroupInspection controlAfter = (await leader.Host.Inspect.GetGroupAsync("control"))!;
        Assert.Equal(controlBefore.Term, controlAfter.Term);
        Assert.Equal(leader.Name, controlAfter.Leader);
        ClusterMapData map = await leader.Host.InvokeAsync(s => s.Control.BuildClusterMap());
        Assert.Equal(WireNodeStatus.Removed, map.Nodes.Single(n => n.Name == victim.Name).Status);
        Assert.False(map.Nodes.Single(n => n.Name == victim.Name).IsControlVoter);
    }

    [Fact]
    public async Task StaleIncarnation_IsRejected()
    {
        await using var cluster = new RuntimeCluster(3, seed: 34);
        await cluster.StartAllAsync();
        await cluster.WaitForRegistrationAsync();
        RuntimeNode leader = await cluster.WaitForLeaderAsync("control");
        RuntimeNode other = cluster.Nodes.First(n => n != leader);
        var keys = new KeyHierarchy(cluster.Secret);
        IRpcTransport transport = cluster.Network.Factory.Create("old-process", "mem://old-process");
        await using (transport)
        {
            // Incarnation 0 is older than the registered incarnation 1.
            IRpcConnection connection = await transport.ConnectAsync(leader.Endpoint, Ct);
            var credentials = new NodeCredentials(cluster.ClusterId, other.Name, other.Id, 0, 77, keys.NodeAuthKey, ProtocolFeatures.Supported);
            DfsException error = await Assert.ThrowsAsync<DfsException>(() =>
                HandshakeClient.AuthenticateNodeAsync(connection, credentials, null, new SeededRandom(4), TimeSpan.FromSeconds(10), Ct));
            Assert.Equal(DfsErrorCode.StaleIncarnation, error.ErrorCode);

            // Same incarnation, different boot id: a cloned disk is fenced (RSY-021).
            IRpcConnection clone = await transport.ConnectAsync(leader.Endpoint, Ct);
            var cloneCredentials = credentials with { Incarnation = other.Host!.Incarnation, BootId = 4242 };
            DfsException cloneError = await Assert.ThrowsAsync<DfsException>(() =>
                HandshakeClient.AuthenticateNodeAsync(clone, cloneCredentials, null, new SeededRandom(5), TimeSpan.FromSeconds(10), Ct));
            Assert.Equal(DfsErrorCode.StaleIncarnation, cloneError.ErrorCode);
        }
    }
}
