using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Transport;
using Xunit.Sdk;
using static Tedd.Tdfs.Tests.Unit.Internal.Harness.HarnessTestSupport;

namespace Tedd.Tdfs.Tests.Unit.Internal.Harness;

/// <summary>Network control (partitions, directional cuts, holds, replay) and fault arms against a running cluster.</summary>
public sealed class NetworkAndFaultTests
{
    [Theory]
    [InlineData(ClusterMode.Deterministic)]
    [InlineData(ClusterMode.Realtime)]
    public Task IsolatedLeaderIsReplacedAndRejoinsAfterHeal(ClusterMode mode) =>
        ClusterTest.RunAsync(Options(3, mode, seed: 0x1501), NodeHostOnly, async cluster =>
        {
            TestNode leader = await cluster.GetControlLeaderAsync(Ct);
            GroupInspection before = await cluster.Inspect.GetControlGroupAsync();
            cluster.Network.Isolate(leader.Name);
            await cluster.WaitUntilAsync(async () =>
            {
                TestNode current = await cluster.GetControlLeaderAsync(Ct);
                return !ReferenceEquals(current, leader);
            }, TimeSpan.FromSeconds(30), "another node leads the control group", Ct);
            GroupInspection after = await cluster.Inspect.GetControlGroupAsync();
            Assert.True(after.Term > before.Term);

            cluster.Network.Heal();
            await cluster.WaitForStableAsync(null, Ct);
            GroupInspection? rejoined = await leader.Inspect.GetGroupAsync(ProtocolInfo.ControlGroup);
            Assert.NotNull(rejoined);
            Assert.NotEqual(RaftRole.Leader, rejoined.Role);
            await cluster.Inspect.AssertNoSplitBrainAsync();
            Assert.Contains(cluster.Trace, l => l.Contains("isolate", StringComparison.OrdinalIgnoreCase));
        }, Ct);

    [Fact]
    public Task CutsAndBlackHolesAreDirectional() =>
        ClusterTest.RunAsync(Options(3, ClusterMode.Deterministic, seed: 0xC07), NodeHostOnly, cluster =>
        {
            cluster.Network.Cut("n0", "n1");
            cluster.Network.BlackHole("n2", "n0");
            Assert.False(cluster.Fabric.Policy.IsLinkUp("n0", "n1"));
            Assert.True(cluster.Fabric.Policy.IsLinkUp("n1", "n0"));
            Assert.False(cluster.Fabric.Policy.IsLinkUp("n2", "n0"));
            Assert.True(cluster.Fabric.Policy.IsLinkUp("n0", "n2"));
            cluster.Network.Restore("n0", "n1");
            cluster.Network.Restore("n2", "n0");
            Assert.True(cluster.Fabric.Policy.IsLinkUp("n0", "n1"));
            Assert.True(cluster.Fabric.Policy.IsLinkUp("n2", "n0"));
            cluster.Network.Partition(["n0"], ["n1", "n2"]);
            Assert.False(cluster.Fabric.Policy.IsLinkUp("n0", "n2"));
            Assert.True(cluster.Fabric.Policy.IsLinkUp("n1", "n2"));
            cluster.Network.Heal();
            Assert.True(cluster.Fabric.Policy.IsLinkUp("n0", "n2"));
            return Task.CompletedTask;
        }, Ct);

    [Fact]
    public Task HeldRaftTrafficIsReleasedAndReplayable() =>
        ClusterTest.RunAsync(Options(3, ClusterMode.Deterministic, seed: 0x401D), NodeHostOnly, async cluster =>
        {
            TestNode leader = await cluster.GetControlLeaderAsync(Ct);
            TestNode follower = cluster.Nodes.First(n => !ReferenceEquals(n, leader));
            using MessageHold hold = cluster.Network.Hold(new MessageFilter(From: leader.Name, To: follower.Name, Type: MessageType.RaftBatch));
            await hold.WaitForAsync(2, TimeSpan.FromSeconds(10));
            Assert.True(hold.Count >= 2);
            Assert.All(hold.Captured, m => Assert.Equal(MessageType.RaftBatch, m.Type));
            CapturedMessage first = hold.Captured[0];
            hold.ReleaseAll();
            await cluster.WaitForStableAsync(null, Ct);

            // A replayed Raft batch is delivered again from the leader's identity; Raft treats it as stale/duplicate.
            DfsErrorCode replayed = await cluster.Network.ReplayAsync(first);
            Assert.Contains(replayed, new[] { DfsErrorCode.None, DfsErrorCode.Unavailable, DfsErrorCode.AccessDenied });
            await cluster.WaitForStableAsync(null, Ct);
            Assert.True(cluster.Network.Log.CountMessages(new MessageFilter(Type: MessageType.RaftBatch)) > 0);
        }, Ct);

    [Fact]
    public Task FaultArmsMatchOccurrencesAndReportCoverage() =>
        ClusterTest.RunAsync(Options(3, ClusterMode.Deterministic, seed: 0xFA17), NodeHostOnly, async cluster =>
        {
            TestNode leader = await cluster.GetControlLeaderAsync(Ct);
            TestNode follower = cluster.Nodes.First(n => !ReferenceEquals(n, leader));
            FaultArm window = cluster.Faults.Arm(new FaultMatch(FaultSites.RaftAfterCommitBeforeApply, Node: follower.Name, Occurrence: 2, Count: 2),
                FaultAction.Delay(TimeSpan.FromMilliseconds(5)));
            FaultArm never = cluster.Faults.Arm(new FaultMatch(FaultSites.RaftAfterCommitBeforeApply, Node: "no-such-node"),
                FaultAction.Delay(TimeSpan.FromMilliseconds(5)));
            for (int i = 0; i < 3; i++)
                await cluster.CreatePrincipalAsync("fault-" + i, new Dictionary<string, DfsPermissions>());
            await window.WaitTriggeredAsync(TimeSpan.FromSeconds(30));
            await cluster.WaitForStableAsync(null, Ct);
            cluster.Faults.AssertReached(window);
            Assert.True(window.Hits >= 3);
            Assert.Equal(2, window.Contexts.Count); // the action ran on exactly the 2nd and 3rd matching hits
            Assert.All(window.Contexts, c => Assert.Equal(follower.Name, c.Node));
            Assert.Throws<FaultPointNotReachedException>(() => cluster.Faults.AssertReached(never));
            await Assert.ThrowsAsync<TimeoutException>(() => never.WaitTriggeredAsync(TimeSpan.FromMilliseconds(200)));
            Assert.True(cluster.Faults.Coverage.WasReached(FaultSites.RaftAfterCommitBeforeApply, follower.Name));
            Assert.Throws<ArgumentException>(() => cluster.Faults.Arm(new FaultMatch("no.such.site"), FaultAction.Block()));
        }, Ct);

    [Fact]
    public Task BlockAndPauseFaultsHoldUntilReleased() =>
        ClusterTest.RunAsync(Options(3, ClusterMode.Deterministic, seed: 0xB10C), NodeHostOnly, async cluster =>
        {
            TestNode leader = await cluster.GetControlLeaderAsync(Ct);
            TestNode follower = cluster.Nodes.First(n => !ReferenceEquals(n, leader));
            FaultArm pause = cluster.Faults.Arm(new FaultMatch(FaultSites.RaftAfterCommitBeforeApply, Node: follower.Name), FaultAction.PauseNode());
            Task create = cluster.CreatePrincipalAsync("pause-a", new Dictionary<string, DfsPermissions>());
            await pause.WaitTriggeredAsync(TimeSpan.FromSeconds(30));
            Assert.Equal(TestNodeState.Paused, follower.State);
            await create; // the majority (leader + other follower) commits without the paused node
            pause.Release();
            Assert.Equal(TestNodeState.Running, follower.State);
            await cluster.WaitForStableAsync(null, Ct);

            TestNode other = cluster.Nodes.First(n => !ReferenceEquals(n, leader) && !ReferenceEquals(n, follower));
            FaultArm crash = cluster.Faults.Arm(new FaultMatch(FaultSites.RaftAfterCommitBeforeApply, Node: other.Name), FaultAction.CrashNode());
            await cluster.CreatePrincipalAsync("crash-a", new Dictionary<string, DfsPermissions>());
            await crash.WaitTriggeredAsync(TimeSpan.FromSeconds(30));
            Assert.Equal(TestNodeState.Crashed, other.State);
            await other.RestartAsync();
            await cluster.WaitForStableAsync(null, Ct);
            await cluster.Inspect.AssertReplicasConvergedAsync(ProtocolInfo.ControlGroup);
        }, Ct);

    [Fact]
    public Task NodeClocksAreIndependentButAdvanceWithVirtualTime() =>
        ClusterTest.RunAsync(Options(3, ClusterMode.Deterministic, seed: 0xC10C), NodeHostOnly, async cluster =>
        {
            Assert.Equal(3, cluster.Nodes.Select(n => n.Clock).Distinct().Count());
            Assert.All(cluster.Nodes, n => Assert.NotSame(cluster.Time, n.Clock));
            TestNode n0 = cluster[0];
            long wall0 = n0.Clock.GetUtcNow().UtcTicks - cluster.Time.GetUtcNow().UtcTicks;
            await cluster.DelayAsync(TimeSpan.FromSeconds(2), Ct);
            long wall1 = n0.Clock.GetUtcNow().UtcTicks - cluster.Time.GetUtcNow().UtcTicks;
            Assert.Equal(wall0, wall1); // same rate as virtual time
            n0.Clock.JumpWall(TimeSpan.FromHours(1));
            long jumped = n0.Clock.GetUtcNow().UtcTicks - cluster.Time.GetUtcNow().UtcTicks;
            Assert.Equal(wall1 + TimeSpan.FromHours(1).Ticks, jumped);
            Assert.NotEqual(cluster[1].Clock.GetUtcNow(), n0.Clock.GetUtcNow());
            await cluster.WaitForStableAsync(null, Ct); // a wall-clock jump never breaks consensus
        }, Ct);
}
