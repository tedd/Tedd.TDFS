using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;
using static Tedd.Tdfs.Tests.Unit.Internal.Harness.HarnessTestSupport;

namespace Tedd.Tdfs.Tests.Unit.Internal.Harness;

/// <summary>Deterministic mode replays exactly: the same seed gives an identical trace, digest, coverage and identities.</summary>
public sealed class DeterministicReplayTests
{
    private sealed record Fingerprint(Guid ClusterId, IReadOnlyList<string> Nodes, IReadOnlyList<string> Trace, string Digest,
        IReadOnlyList<string> Coverage, long Messages, long ElapsedTicks);

    [Theory]
    [InlineData(3)]
    [InlineData(5)]
    public async Task SameSeed_SameRun(int nodes)
    {
        TestClusterOptions options = Options(nodes, ClusterMode.Deterministic, seed: 0x5A11_0000 + (ulong)nodes);
        Fingerprint first = await ClusterTest.RunAsync(options, NodeHostOnly, ScenarioAsync, Ct);
        Fingerprint second = await ClusterTest.RunAsync(options, NodeHostOnly, ScenarioAsync, Ct);

        Assert.True(first.Trace.Count > 10);
        Assert.NotEmpty(first.Coverage);
        Assert.Equal(first.ClusterId, second.ClusterId);
        Assert.Equal(first.Nodes, second.Nodes);
        Assert.Equal(first.Trace, second.Trace);
        Assert.Equal(first.Digest, second.Digest);
        Assert.Equal(first.Coverage, second.Coverage);
        Assert.Equal(first.Messages, second.Messages);
        Assert.Equal(first.ElapsedTicks, second.ElapsedTicks);

        Fingerprint other = await ClusterTest.RunAsync(options with { Seed = options.Seed + 1 }, NodeHostOnly, ScenarioAsync, Ct);
        Assert.NotEqual(first.ClusterId, other.ClusterId);
        Assert.NotEqual(first.Nodes, other.Nodes);
    }

    private static async Task<Fingerprint> ScenarioAsync(TestCluster cluster)
    {
        long start = cluster.Time.GetTimestamp();
        TestNode leader = await cluster.GetControlLeaderAsync(Ct);
        TestNode follower = cluster.Nodes.First(n => !ReferenceEquals(n, leader));
        FaultArm delay = cluster.Faults.Arm(new FaultMatch(FaultSites.RaftAfterCommitBeforeApply, Node: follower.Name, Occurrence: 2),
            FaultAction.Delay(TimeSpan.FromMilliseconds(40)));
        await cluster.CreatePrincipalAsync("replay-a", new Dictionary<string, DfsPermissions> { ["vol"] = DfsPermissions.Read });
        await delay.WaitTriggeredAsync(TimeSpan.FromSeconds(30));
        cluster.Faults.AssertReached(delay);

        await follower.CrashAsync(CrashMode.RandomSubset);
        await cluster.CreatePrincipalAsync("replay-b", new Dictionary<string, DfsPermissions>());
        await follower.RestartAsync();
        await cluster.WaitForStableAsync(null, Ct);
        await cluster.DelayAsync(TimeSpan.FromSeconds(1), Ct);
        return new Fingerprint(cluster.ClusterId, cluster.Nodes.Select(n => $"{n.Name}:{n.Id}:{n.Incarnation}").ToArray(), cluster.Trace,
            await cluster.ComputeDigestAsync(Ct), cluster.Faults.Coverage.Hits.Select(h => h.ToString()).ToArray(),
            cluster.Network.Log.CountMessages(MessageFilter.Any), cluster.Time.GetElapsedTime(start).Ticks);
    }
}
