using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;
using static Tedd.Tdfs.Tests.Unit.Internal.Harness.HarnessTestSupport;

namespace Tedd.Tdfs.Tests.Unit.Internal.Harness;

/// <summary>Seed graphs, raw protocol clients, recovery clusters, foreign cluster nodes, leadership transfer and the trace minimizer.</summary>
public sealed class ClusterFeatureTests
{
    [Theory]
    [InlineData(SeedGraph.Ring)]
    [InlineData(SeedGraph.Star)]
    [InlineData(SeedGraph.Line)]
    [InlineData(SeedGraph.RandomConnected)]
    public Task SeedJoinGraphsFormOneCluster(SeedGraph graph) =>
        ClusterTest.RunAsync(Options(5, ClusterMode.Deterministic, seed: 0x5EED0 + (ulong)graph, BootstrapMode.SeedJoin) with { Seeds = graph },
            NodeHostOnly, async cluster =>
            {
                GroupInspection control = await cluster.Inspect.GetControlGroupAsync();
                Assert.Equal(5, control.Voters.Count + control.Learners.Count);
                var clusterIds = new HashSet<Guid>();
                foreach (TestNode node in cluster.Nodes)
                    clusterIds.Add((await node.Inspect.GetControlSummaryAsync()).ClusterId);
                Assert.Equal(cluster.ClusterId, Assert.Single(clusterIds));
            }, Ct);

    [Fact]
    public Task RawClientAuthenticatesAndSendsVerbatimFrames() =>
        ClusterTest.RunAsync(Options(3, ClusterMode.Deterministic, seed: 0x8A3), NodeHostOnly, async cluster =>
        {
            await using RawProtocolClient wrong = await cluster.ConnectRawAsync("n0", Ct);
            Assert.True(wrong.IsConnected);
            Assert.Equal(DfsErrorCode.AuthenticationFailed, await wrong.AuthenticateAsync(new DfsCredentials("admin", "not-the-secret")));

            await using RawProtocolClient raw = await cluster.ConnectRawAsync("n0", Ct);
            Assert.Equal(DfsErrorCode.None, await raw.AuthenticateAsync(cluster.AdminCredentials));
            IWireMessage route = await raw.CallAsync(new GetVolumeRouteMessage("vol"));
            VolumeRouteResultMessage result = Assert.IsType<VolumeRouteResultMessage>(route);
            Assert.Equal("vol", result.Volume.Name);

            byte[] frame = MessageCodec.Encode(new GetClusterMapMessage());
            DfsErrorCode whole = await raw.SendFrameAsync(frame);
            Assert.Equal(DfsErrorCode.None, whole);
            Assert.Equal(whole, await raw.SendFrameInPiecesAsync(frame, [1]));
            Assert.Equal(whole, await raw.SendFrameInPiecesAsync(frame, Enumerable.Range(1, frame.Length - 1).ToArray()));
            await Assert.ThrowsAsync<ArgumentException>(() => raw.SendFrameInPiecesAsync(frame, [0]));

            Assert.NotEqual(DfsErrorCode.None, await raw.SendFrameAsync(new byte[] { 0xFF, 0xFF, 1, 2, 3 }));
            Assert.Equal(DfsErrorCode.ProtocolError, await raw.SendPartialFrameAndCloseAsync(frame, 2));
            Assert.False(raw.IsConnected);
        }, Ct);

    [Fact]
    public Task RecoveryClusterIsIndependentButSharesTheNetwork() =>
        ClusterTest.RunAsync(Options(3, ClusterMode.Deterministic, seed: 0x2EC0), NodeHostOnly, async cluster =>
        {
            TestCluster recovery = await cluster.StartRecoveryClusterAsync(TestClusterOptions.SingleNode with
            {
                Mode = ClusterMode.Deterministic,
                Volumes = [],
            }, Ct);
            Assert.NotEqual(cluster.ClusterId, recovery.ClusterId);
            Assert.Equal("r0", Assert.Single(recovery.Nodes).Name);
            Assert.NotEqual(cluster.AdminCredentials.Secret, recovery.AdminCredentials.Secret);
            Assert.StartsWith(cluster.DataRoot, recovery.DataRoot, StringComparison.Ordinal);
            await recovery.GetControlLeaderAsync(Ct);

            // One fabric: either cluster's network addresses every endpoint.
            cluster.Network.Isolate("r0");
            Assert.False(recovery.Fabric.Policy.IsLinkUp("r0", "n0"));
            cluster.Network.Heal();
            await Assert.ThrowsAsync<ArgumentException>(() => cluster.StartRecoveryClusterAsync(TestClusterOptions.SingleNode with
            {
                Mode = ClusterMode.Deterministic,
                Nodes = [new TestNodeSpec("n1", "z1")],
                Volumes = [],
            }, Ct));
            await recovery.DisposeAsync();
            Assert.All(cluster.Nodes, n => Assert.Equal(TestNodeState.Running, n.State));
        }, Ct);

    [Fact]
    public Task ForeignClusterNodeIsRefusedByClusterIdentity() =>
        ClusterTest.RunAsync(Options(3, ClusterMode.Deterministic, seed: 0xF0E), NodeHostOnly, async cluster =>
        {
            TestNode displaced = cluster[2];
            await Assert.ThrowsAsync<InvalidOperationException>(() => cluster.StartForeignClusterNodeAsync(displaced.Endpoint, Ct));
            await displaced.CrashAsync();
            NodeHost foreign = await cluster.StartForeignClusterNodeAsync(displaced.Endpoint, Ct);
            Assert.Equal(displaced.Endpoint, foreign.Endpoint);

            await using RawProtocolClient raw = await cluster.ConnectRawAsync(displaced.Name, Ct);
            Assert.Equal(DfsErrorCode.ClusterMismatch, await raw.AuthenticateAsync(cluster.AdminCredentials));
        }, Ct);

    [Fact]
    public Task LeadershipTransfersThroughTheProductionPath() =>
        ClusterTest.RunAsync(Options(3, ClusterMode.Deterministic, seed: 0x7F3), NodeHostOnly, async cluster =>
        {
            TestNode leader = await cluster.GetLeaderAsync("vol", Ct);
            TestNode target = cluster.Nodes.First(n => !ReferenceEquals(n, leader));
            GroupInspection before = await cluster.Inspect.GetVolumeGroupAsync("vol");
            Assert.Same(target, await cluster.TransferLeadershipAsync("vol", target.Name, ct: Ct));
            GroupInspection after = await cluster.Inspect.GetVolumeGroupAsync("vol");
            Assert.Equal(target.Name, after.Leader);
            Assert.True(after.Term > before.Term);
            Assert.Same(target, await cluster.TransferLeadershipAsync("vol", target.Name, ct: Ct)); // already leading

            TestNode controlLeader = await cluster.GetControlLeaderAsync(Ct);
            TestNode controlTarget = cluster.Nodes.First(n => !ReferenceEquals(n, controlLeader));
            await cluster.TransferLeadershipAsync(ProtocolInfo.ControlGroup, controlTarget.Name, waitUntilServing: false, ct: Ct);
            await Assert.ThrowsAsync<ArgumentException>(() => cluster.TransferLeadershipAsync("no-such-volume", target.Name, ct: Ct));
            await Assert.ThrowsAsync<ArgumentException>(() => cluster.TransferLeadershipAsync("vol", "n9", ct: Ct));
            await cluster.WaitForStableAsync(null, Ct);
            await cluster.Inspect.AssertNoSplitBrainAsync();
            Assert.Contains(cluster.Trace, l => l.Contains("transfer leadership of vol", StringComparison.Ordinal));
        }, Ct);

    [Fact]
    public async Task TraceMinimizerFindsOneMinimalFailingSubsequence()
    {
        static Task<string?> Run(IReadOnlyList<int> t) =>
            Task.FromResult<string?>(t.Contains(3) && t.Contains(7) && t.ToList().IndexOf(3) < t.ToList().IndexOf(7) ? "I" : t.Contains(9) ? "J" : null);
        int[] trace = Enumerable.Range(0, 12).ToArray();
        TraceMinimizationResult<int> result = await TraceMinimizer.MinimizeAsync<int>(trace, Run, 200, Ct);
        Assert.Equal("I", result.Invariant);
        Assert.Equal(new[] { 3, 7 }, result.Minimal);
        Assert.True(result.IsOneMinimal);
        Assert.False(result.BudgetExhausted);

        TraceMinimizationResult<int> cut = await TraceMinimizer.MinimizeAsync<int>(trace, Run, 1, Ct);
        Assert.True(cut.BudgetExhausted);
        Assert.False(cut.IsOneMinimal);
        Assert.Equal(1, cut.Runs);
        Assert.Equal(trace, cut.Minimal);
        await Assert.ThrowsAsync<ArgumentException>(() => TraceMinimizer.MinimizeAsync<int>([1, 2], Run, 10, Ct));
    }
}
