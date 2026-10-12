using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Server.Control;
using Tedd.Tdfs.Server.Raft;
using Tedd.Tdfs.Server.Runtime;
using Tedd.Tdfs.Server.Security;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Tests.Unit.Internal.Runtime;

/// <summary>
/// Multi-node tests of the server runtime core (NodeHost, Raft hosting, control plane) on real node loops, an in-memory
/// network and simulated disks.
/// </summary>
public sealed class RuntimeClusterTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    [InlineData(5)]
    public async Task StaticBootstrap_ElectsControlLeaderAndRegistersEveryNode(int nodeCount)
    {
        await using var cluster = new RuntimeCluster(nodeCount, seed: (ulong)nodeCount);
        await cluster.StartAllAsync();
        RuntimeNode leader = await cluster.WaitForLeaderAsync("control");
        await cluster.WaitForRegistrationAsync();

        GroupInspection control = (await leader.Host!.Inspect.GetGroupAsync("control"))!;
        Assert.Equal(Math.Min(3, nodeCount), control.Voters.Count);
        Assert.Equal(nodeCount - Math.Min(3, nodeCount), control.Learners.Count);
        Assert.Equal(leader.Name, control.Leader);
        await RuntimeCluster.WaitUntilAsync(async () =>
        {
            foreach (RuntimeNode node in cluster.Nodes)
            {
                GroupInspection? g = await node.Host!.Inspect.GetGroupAsync("control");
                if (g is null || g.Leader != leader.Name || g.AppliedIndex < control.CommitIndex)
                    return false;
            }

            return true;
        }, "every node follows the same control leader and applied the bootstrap");

        foreach (RuntimeNode node in cluster.Nodes)
        {
            Assert.Equal(1UL, node.Host!.Incarnation);
            Assert.Equal(node.Id, node.Host.Id);
            ClusterMapData map = await node.Host.InvokeAsync(s => s.Control.BuildClusterMap());
            Assert.Equal(cluster.ClusterId, map.ClusterId);
            Assert.Equal(nodeCount, map.Nodes.Count);
        }

        await RuntimeCluster.WaitUntilAsync(async () =>
        {
            ClusterMapData map = await leader.Host!.InvokeAsync(s => s.Control.BuildClusterMap());
            return map.Nodes.All(n => n.Incarnation == 1);
        }, "every incarnation is registered in the control state");
    }

    [Fact]
    public async Task ClusterMap_IsServedByEveryNode()
    {
        await using var cluster = new RuntimeCluster(3, seed: 11);
        await cluster.StartAllAsync();
        await cluster.WaitForLeaderAsync("control");
        foreach (RuntimeNode node in cluster.Nodes)
        {
            RawClient client = await cluster.ConnectAsync(node);
            Assert.Equal(cluster.ClusterId, client.Outcome.Challenge.ClusterId);
            Assert.Equal(DfsPermissions.ClusterAdmin, client.Outcome.Result.ClusterPermissions);
            var result = await client.CallOrThrowAsync<ClusterMapResultMessage>(new GetClusterMapMessage());
            ClusterMapData map = Assert.IsType<ClusterMapData>(result.Map);
            Assert.Equal(cluster.ClusterId, map.ClusterId);
            Assert.Equal("runtime-test", map.ClusterName);
            Assert.Equal(["n0", "n1", "n2"], map.Nodes.Select(n => n.Name).ToArray());
            Assert.Equal(cluster.Nodes.Select(n => n.Endpoint).ToArray(), map.Nodes.Select(n => n.Endpoints[0]).ToArray());
            Assert.True(map.Nodes.All(n => n.IsControlVoter));
            Assert.True(result.ConfigIndex > 0);
            Assert.True(map.PlacementEpoch >= 1);

            var unchanged = await client.CallOrThrowAsync<ClusterMapResultMessage>(new GetClusterMapMessage(result.ConfigIndex));
            if (unchanged.ConfigIndex == result.ConfigIndex)
                Assert.Null(unchanged.Map);
        }
    }

    [Fact]
    public async Task ClientHandshake_AcceptsCorrectSecretAndRejectsWrongOnes()
    {
        await using var cluster = new RuntimeCluster(3, seed: 12);
        await cluster.StartAllAsync();
        await cluster.WaitForLeaderAsync("control");
        RuntimeNode node = cluster[1];

        RawClient reader = await cluster.ConnectAsync(node, new DfsCredentials("reader", "reader-secret-1"));
        Assert.True(reader.Outcome.Result.Accepted);
        Assert.Equal("reader", reader.Outcome.Result.Principal);
        Assert.Equal(DfsPermissions.None, reader.Outcome.Result.ClusterPermissions);
        Assert.Equal("n1", reader.Outcome.ServerIdentity.NodeName);

        await Assert.ThrowsAsync<DfsAuthenticationException>(() => cluster.ConnectAsync(node, new DfsCredentials("reader", "wrong-secret")));
        await Assert.ThrowsAsync<DfsAuthenticationException>(() => cluster.ConnectAsync(node, new DfsCredentials("ghost", "reader-secret-1")));
        await Assert.ThrowsAsync<DfsAuthenticationException>(() => cluster.ConnectAsync(node, new DfsCredentials("READER", "reader-secret-1")));
        await Assert.ThrowsAsync<DfsAuthenticationException>(() => cluster.ConnectAsync(node, new DfsCredentials("reader", RuntimeCluster.AdminSecret)));

        // A non-administrator cannot use administrative operations.
        var denied = await reader.CallAsync(AdminRequestMessage.Create(AdminOp.ListVolumes, new UInt128(1, 1), new AdminEmptyBody()));
        Assert.Equal(DfsErrorCode.AccessDenied, Assert.IsType<ErrorReplyMessage>(denied).Code);

        // Requests before (or without) a successful handshake are refused.
        IRpcTransport transport = cluster.Network.Factory.Create("client:anon", "mem://client-anon");
        await using (transport)
        {
            IRpcConnection connection = await transport.ConnectAsync(node.Endpoint, Ct);
            ReadOnlyMemory<byte> reply = await connection.CallAsync(MessageCodec.Encode(new GetClusterMapMessage()), TimeSpan.FromSeconds(10), Ct);
            var error = Assert.IsType<ErrorReplyMessage>(MessageCodec.Decode(reply.Span));
            Assert.Equal(DfsErrorCode.AuthenticationFailed, error.Code);
        }
    }

    [Fact]
    public async Task WrongClusterId_IsRejectedForClientsAndNodes()
    {
        await using var cluster = new RuntimeCluster(3, seed: 13);
        await cluster.StartAllAsync();
        await cluster.WaitForLeaderAsync("control");
        var otherCluster = new Guid(Enumerable.Range(1, 16).Select(i => (byte)i).ToArray());

        var clientError = await Assert.ThrowsAsync<DfsClusterIdentityException>(() => cluster.ConnectAsync(cluster[0], expectedCluster: otherCluster));
        Assert.Equal(DfsErrorCode.ClusterMismatch, clientError.ErrorCode);

        // A node presenting the right secret but expecting another cluster is refused before any proof is sent.
        var keys = new KeyHierarchy(cluster.Secret);
        IRpcTransport transport = cluster.Network.Factory.Create("intruder", "mem://intruder");
        await using (transport)
        {
            IRpcConnection connection = await transport.ConnectAsync(cluster[0].Endpoint, Ct);
            var credentials = new NodeCredentials(otherCluster, "intruder", NodeId.New(new SeededRandom(5)), 1, 7, keys.NodeAuthKey, ProtocolFeatures.Supported);
            var nodeError = await Assert.ThrowsAsync<DfsClusterIdentityException>(() =>
                HandshakeClient.AuthenticateNodeAsync(connection, credentials, null, new SeededRandom(6), TimeSpan.FromSeconds(10), Ct));
            Assert.Equal(DfsErrorCode.ClusterMismatch, nodeError.ErrorCode);
        }

        // A node with the right cluster id but a wrong secret fails authentication.
        var wrongKeys = new KeyHierarchy(new byte[32]);
        IRpcTransport transport2 = cluster.Network.Factory.Create("intruder2", "mem://intruder2");
        await using (transport2)
        {
            IRpcConnection connection = await transport2.ConnectAsync(cluster[0].Endpoint, Ct);
            var credentials = new NodeCredentials(cluster.ClusterId, "intruder2", NodeId.New(new SeededRandom(8)), 1, 9, wrongKeys.NodeAuthKey, ProtocolFeatures.Supported);
            var authError = await Assert.ThrowsAsync<DfsAuthenticationException>(() =>
                HandshakeClient.AuthenticateNodeAsync(connection, credentials, null, new SeededRandom(10), TimeSpan.FromSeconds(10), Ct));
            Assert.Equal(DfsErrorCode.AuthenticationFailed, authError.ErrorCode);
        }
    }
}
