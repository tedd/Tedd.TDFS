using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;
using static Tedd.Tdfs.Tests.Unit.Internal.Harness.HarnessTestSupport;

namespace Tedd.Tdfs.Tests.Unit.Internal.Harness;

/// <summary>Real transports and media: in-process QUIC with real disks, and out-of-process <c>Tedd.Tdfs.Host</c> nodes.</summary>
public sealed class TransportAndStorageTests
{
    [Fact]
    public Task QuicOnDisk_BootstrapsCrashesAndRecovers() =>
        ClusterTest.RunAsync(Options(3, ClusterMode.Realtime, seed: 0x0D15C) with { Transport = TransportKind.Quic, Storage = StorageKind.Disk },
            NodeHostOnly, async cluster =>
            {
                Assert.All(cluster.Nodes, n => Assert.StartsWith("quic://127.0.0.1:", n.Endpoint, StringComparison.Ordinal));
                Assert.All(cluster.Nodes, n => Assert.True(Directory.Exists(n.Disk.RootDirectory)));
                Assert.StartsWith(Path.GetFullPath(TestClusterOptions.ThreeZones.ResolveDataRoot()), cluster.DataRoot, StringComparison.OrdinalIgnoreCase);
                Assert.NotEmpty(cluster[0].Disk.Files());

                TestNode leader = await cluster.GetControlLeaderAsync(Ct);
                ulong incarnation = leader.Incarnation;
                await leader.CrashAsync(CrashMode.LoseUnflushed);
                await cluster.GetControlLeaderAsync(Ct);
                await leader.RestartAsync();
                Assert.Equal(incarnation + 1, leader.Incarnation);
                await cluster.WaitForStableAsync(null, Ct);
                await cluster.Inspect.AssertNoSplitBrainAsync();
            }, Ct);

    [Fact]
    public async Task OutOfProcess_HostProcessesFormAClusterAndSurviveKill()
    {
        TestClusterOptions options = Options(3, ClusterMode.Realtime, seed: 0x00F) with
        {
            Transport = TransportKind.Quic,
            Storage = StorageKind.Disk,
            OutOfProcess = true,
            Volumes = [],
        };
        await ClusterTest.RunAsync(options, NodeHostOnly with { StartTimeout = TimeSpan.FromSeconds(180) }, async cluster =>
        {
            Assert.All(cluster.Nodes, n => Assert.Equal(TestNodeState.Running, n.State));
            Assert.All(cluster.Nodes, n => Assert.Null(n.Host));
            Assert.All(cluster.Nodes, n => Assert.Throws<InvalidOperationException>(() => n.Inspect));

            TestNode leader = await cluster.GetControlLeaderAsync(Ct);
            ulong incarnation = leader.Incarnation;
            await leader.CrashAsync(); // kills the process
            Assert.Equal(TestNodeState.Crashed, leader.State);
            await cluster.WaitUntilAsync(async () =>
            {
                TestNode current = await cluster.GetControlLeaderAsync(Ct);
                return !ReferenceEquals(current, leader);
            }, TimeSpan.FromSeconds(60), "a surviving process leads the control group", Ct);

            await leader.RestartAsync();
            Assert.Equal(TestNodeState.Running, leader.State);
            Assert.True(leader.Incarnation > incarnation);
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), Ct);
            await cluster.CreatePrincipalAsync("oop-user", new Dictionary<string, DfsPermissions>());
        }, Ct);
    }
}
