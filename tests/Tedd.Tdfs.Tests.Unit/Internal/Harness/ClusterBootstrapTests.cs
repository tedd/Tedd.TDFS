using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;
using static Tedd.Tdfs.Tests.Unit.Internal.Harness.HarnessTestSupport;

namespace Tedd.Tdfs.Tests.Unit.Internal.Harness;

/// <summary>
/// Cluster bootstrap through the harness (NodeHost only): static and seed-join bootstrap of 1, 3, 5 and 10 nodes in both
/// execution modes reaches a stable cluster (control and volume leaders, every node registered and caught up).
/// </summary>
public sealed class ClusterBootstrapTests
{
    public static TheoryData<ClusterMode, int, BootstrapMode> Shapes()
    {
        var data = new TheoryData<ClusterMode, int, BootstrapMode>();
        foreach (ClusterMode mode in Enum.GetValues<ClusterMode>())
        {
            foreach (int nodes in new[] { 1, 3, 5, 10 })
            {
                data.Add(mode, nodes, BootstrapMode.Static);
                if (nodes > 1)
                    data.Add(mode, nodes, BootstrapMode.SeedJoin);
            }
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(Shapes))]
    public Task Bootstrap_ReachesStableCluster(ClusterMode mode, int nodes, BootstrapMode bootstrap) =>
        ClusterTest.RunAsync(Options(nodes, mode, seed: 0xB007 + (ulong)nodes, bootstrap), NodeHostOnly, async cluster =>
        {
            Assert.Equal(nodes, cluster.Nodes.Count);
            Assert.All(cluster.Nodes, n => Assert.Equal(TestNodeState.Running, n.State));
            Assert.Equal(nodes, cluster.Nodes.Select(n => n.Id).Distinct().Count());

            TestNode control = await cluster.GetControlLeaderAsync(Ct);
            GroupInspection controlGroup = await cluster.Inspect.GetControlGroupAsync();
            Assert.Equal(RaftRole.Leader, controlGroup.Role);
            Assert.Equal(control.Name, controlGroup.Leader);
            Assert.Equal(Math.Min(nodes, 3), controlGroup.Voters.Count);

            TestNode leader = await cluster.GetLeaderAsync("vol", Ct);
            GroupInspection volume = await cluster.Inspect.GetVolumeGroupAsync("vol");
            Assert.Equal(leader.Name, volume.Leader);
            Assert.Equal(Math.Min(nodes, 3), volume.Voters.Count);

            await cluster.WaitForReplicationAsync("vol", Ct);
            await cluster.Inspect.AssertNoSplitBrainAsync();
            await cluster.Inspect.AssertReplicasConvergedAsync("vol");
            Assert.Contains(cluster.Trace, line => line.Contains("cluster: stable", StringComparison.Ordinal));
        }, Ct);
}
