using Tedd.Tdfs.Testing.Simulation;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Tests.Unit.Internal.Simulation;

/// <summary>
/// The determinism self-test: a multi-node workload replays exactly from its seed, and different seeds explore different
/// interleavings (HAR-001 groundwork).
/// </summary>
public sealed class SimDeterminismTests
{
    [Fact]
    public void PingWorkload_ReplaysExactlyFromItsSeed()
    {
        SimRunRecord record = SimDeterminism.AssertReplayIsExact(seed => SimPingWorkload.Run(seed), seed: 42, runs: 3);

        Assert.True(record.Trace.Count > 300, $"expected a substantial trace, got {record.Trace.Count} lines");
        Assert.True(record.Steps > 200);
        Assert.Contains(record.Trace, line => line.Contains("n0 round", StringComparison.Ordinal));
        Assert.Contains(record.Trace, line => line.Contains("served 'ping", StringComparison.Ordinal));
        Assert.Contains(record.Trace, line => line.Contains("heard 'gossip", StringComparison.Ordinal));
        Assert.Contains(record.Trace, line => line.Contains("client echo", StringComparison.Ordinal));
        Assert.Contains("client:ok=6,failed=0", record.Digest, StringComparison.Ordinal);
        Assert.Contains("n0:pongs=12", record.Digest, StringComparison.Ordinal);
        Assert.Equal(42UL, record.Seed);
    }

    [Fact]
    public void PingWorkload_DifferentSeedsExploreDifferentInterleavings()
    {
        IReadOnlyList<SimRunRecord> records = SimDeterminism.AssertSeedsDiverge(seed => SimPingWorkload.Run(seed), 1, 2, 3, 4, 5);
        Assert.Equal(5, records.Select(r => r.Digest).Distinct().Count());
    }

    [Fact]
    public void PingWorkload_FifoMode_IsDeterministicToo()
    {
        var options = new SimPingWorkloadOptions { Mode = SimScheduleMode.Fifo, Nodes = 4, RoundsPerNode = 6 };
        SimDeterminism.AssertReplayIsExact(seed => SimPingWorkload.Run(seed, options), seed: 7);
    }

    [Fact]
    public void PingWorkload_OverLossyLinks_ReplaysExactly_IncludingTimeouts()
    {
        var options = new SimPingWorkloadOptions
        {
            Nodes = 4,
            RoundsPerNode = 15,
            ClientCalls = 4,
            CallTimeout = TimeSpan.FromMilliseconds(20),
            LinkPolicy = scheduler => new LossyPolicy(scheduler.ForkRandom("loss"), dropOneIn: 8),
        };

        SimRunRecord record = SimDeterminism.AssertReplayIsExact(seed => SimPingWorkload.Run(seed, options), seed: 9);
        Assert.Contains(record.Trace, line => line.Contains("timed out", StringComparison.Ordinal));
    }

    [Fact]
    public void AssertReplayIsExact_DetectsANondeterministicScenario()
    {
        int run = 0;
        SimRunRecord Scenario(ulong seed)
        {
            var s = new SimScheduler(seed);
            s.AddTrace("run " + run++); // leaks state from outside the simulation
            return SimRunRecord.Capture(s);
        }

        Assert.ThrowsAny<Xunit.Sdk.XunitException>(() => SimDeterminism.AssertReplayIsExact(Scenario, 1));
        Assert.Throws<ArgumentOutOfRangeException>(() => SimDeterminism.AssertReplayIsExact(Scenario, 1, runs: 1));
    }

    [Fact]
    public void AssertSeedsDiverge_DetectsSeedInsensitiveScenarios()
    {
        static SimRunRecord Constant(ulong seed) => SimRunRecord.Capture(new SimScheduler(seed, new SimSchedulerOptions { Mode = SimScheduleMode.Fifo }));
        Assert.ThrowsAny<Xunit.Sdk.XunitException>(() => SimDeterminism.AssertSeedsDiverge(Constant, 1, 2));
        Assert.Throws<ArgumentException>(() => SimDeterminism.AssertSeedsDiverge(Constant, 1));
        Assert.Throws<ArgumentException>(() => SimDeterminism.AssertSeedsDiverge(Constant, 1, 1));
    }

    [Fact]
    public void FirstDifference_PinpointsTheDivergence()
    {
        var a = new SimRunRecord(1, ["x", "y"], 5, 10, TimeSpan.FromSeconds(1), "d");
        Assert.Null(a.FirstDifference(a with { Seed = 2 }));
        Assert.Contains("trace line 1", a.FirstDifference(a with { Trace = ["x", "z"] }), StringComparison.Ordinal);
        Assert.Contains("trace lengths differ", a.FirstDifference(a with { Trace = ["x", "y", "w"] }), StringComparison.Ordinal);
        Assert.Contains("fingerprints", a.FirstDifference(a with { ScheduleFingerprint = 6 }), StringComparison.Ordinal);
        Assert.Contains("step counts", a.FirstDifference(a with { Steps = 11 }), StringComparison.Ordinal);
        Assert.Contains("virtual times", a.FirstDifference(a with { VirtualTime = TimeSpan.FromSeconds(2) }), StringComparison.Ordinal);
        Assert.Contains("digests", a.FirstDifference(a with { Digest = "e" }), StringComparison.Ordinal);
    }

    [Fact]
    public void WorkloadOptions_AreValidated()
    {
        Assert.Throws<ArgumentException>(() => SimPingWorkload.Run(1, new SimPingWorkloadOptions { Nodes = 1 }));
        Assert.Throws<ArgumentException>(() => SimPingWorkload.Run(1, new SimPingWorkloadOptions { CallTimeout = TimeSpan.Zero }));
    }

    /// <summary>Drops roughly one message in <c>dropOneIn</c>, drawing from the simulation's seeded randomness.</summary>
    private sealed class LossyPolicy(IRandomSource random, int dropOneIn) : ILinkPolicy
    {
        public LinkDecision Decide(string fromName, string toName, LinkDirection direction, ReadOnlySpan<byte> payloadPrefix) =>
            random.Next(dropOneIn) == 0 ? LinkDecision.Drop : LinkDecision.Deliver;

        public bool CanConnect(string fromName, string toName) => true;
    }
}
