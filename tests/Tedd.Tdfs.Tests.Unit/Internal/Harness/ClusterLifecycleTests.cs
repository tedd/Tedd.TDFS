using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Transport;
using static Tedd.Tdfs.Tests.Unit.Internal.Harness.HarnessTestSupport;

namespace Tedd.Tdfs.Tests.Unit.Internal.Harness;

/// <summary>Node lifecycle through the harness: crash, restart (incarnation + 1), pause/resume, stop, disk replacement.</summary>
public sealed class ClusterLifecycleTests
{
    [Theory]
    [InlineData(ClusterMode.Deterministic)]
    [InlineData(ClusterMode.Realtime)]
    public Task CrashRestartPauseStop(ClusterMode mode) =>
        ClusterTest.RunAsync(Options(3, mode, seed: 0x11FE), NodeHostOnly, async cluster =>
        {
            TestNode leader = await cluster.GetControlLeaderAsync(Ct);
            ulong incarnation = leader.Incarnation;
            NodeId id = leader.Id;

            await leader.CrashAsync();
            Assert.Equal(TestNodeState.Crashed, leader.State);
            Assert.Null(leader.Host);
            Assert.Throws<InvalidOperationException>(() => leader.Inspect);
            await leader.CrashAsync(); // no-op when down

            TestNode successor = await cluster.GetControlLeaderAsync(Ct);
            Assert.NotSame(leader, successor);

            await leader.RestartAsync();
            Assert.Equal(TestNodeState.Running, leader.State);
            Assert.Equal(incarnation + 1, leader.Incarnation);
            Assert.Equal(id, leader.Id);
            await cluster.WaitForStableAsync(null, Ct);

            TestNode follower = cluster.Nodes.First(n => !ReferenceEquals(n, successor));
            await follower.PauseAsync();
            Assert.Equal(TestNodeState.Paused, follower.State);
            Assert.NotNull(follower.Host);
            await cluster.DelayAsync(TimeSpan.FromMilliseconds(500), Ct);
            await follower.ResumeAsync();
            Assert.Equal(TestNodeState.Running, follower.State);

            await follower.StopAsync();
            Assert.Equal(TestNodeState.Stopped, follower.State);
            await Assert.ThrowsAsync<InvalidOperationException>(() => successor.ReplaceDiskWithEmptyAsync()); // only when down
            ulong before = follower.Incarnation;
            await follower.RestartAsync();
            Assert.Equal(before + 1, follower.Incarnation);

            await cluster.WaitForStableAsync(null, Ct);
            await cluster.Inspect.AssertNoSplitBrainAsync();
            Assert.Contains(cluster.Trace, l => l.Contains($"node {leader.Name}: crash", StringComparison.Ordinal));
            Assert.Contains(cluster.Trace, l => l.Contains($"node {follower.Name}: pause", StringComparison.Ordinal));
            Assert.Contains(cluster.Trace, l => l.Contains($"node {follower.Name}: stop", StringComparison.Ordinal));
            Assert.Empty(cluster.AssumptionViolations);
        }, Ct);

    [Fact]
    public Task RestartedNodeCatchesUpAndServes() =>
        ClusterTest.RunAsync(Options(3, ClusterMode.Deterministic, seed: 0x5EED), NodeHostOnly, async cluster =>
        {
            TestNode controlLeader = await cluster.GetControlLeaderAsync(Ct);
            TestNode follower = cluster.Nodes.First(n => !ReferenceEquals(n, controlLeader));
            await follower.StopAsync();
            await cluster.CreatePrincipalAsync("p-while-down", new Dictionary<string, DfsPermissions> { ["vol"] = DfsPermissions.Read });
            await follower.RestartAsync();
            await cluster.WaitForStableAsync(null, Ct);
            GroupInspection control = await cluster.Inspect.GetControlGroupAsync();
            GroupInspection? mine = await follower.Inspect.GetGroupAsync(ProtocolInfo.ControlGroup);
            Assert.NotNull(mine);
            Assert.True(mine.AppliedIndex >= control.CommitIndex - 1);
            await cluster.Inspect.AssertReplicasConvergedAsync(ProtocolInfo.ControlGroup);
        }, Ct);

    [Fact]
    public Task DishonestFlushIsLabelledAtCrash() =>
        Assert.ThrowsAsync<AssumptionViolatedException>(() => ClusterTest.RunAsync(Options(3, ClusterMode.Deterministic, seed: 0xD15), NodeHostOnly,
            async cluster =>
            {
                TestNode node = cluster[1];
                node.Disk.SetDishonestFlush(true);
                Assert.True(node.Disk.IsDishonestFlush);
                await cluster.CreatePrincipalAsync("p-dishonest", new Dictionary<string, DfsPermissions>());
                await cluster.WaitForStableAsync(null, Ct);
                await node.CrashAsync();
                AssumptionViolation violation = Assert.Single(cluster.AssumptionViolations);
                Assert.Equal(AssumptionViolation.DurableFlush, violation.Assumption);
                Assert.Equal(node.Name, violation.Node);
                node.Disk.SetDishonestFlush(false);
                await node.RestartAsync();
            }, Ct));
}
