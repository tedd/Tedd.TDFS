using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Tests.Unit.Internal.Runtime;

namespace Tedd.Tdfs.Tests.Unit.Internal.Harness;

/// <summary>Shared settings of the harness's internal tests (NodeHost-only clusters: no SDK client is needed).</summary>
internal static class HarnessTestSupport
{
    /// <summary>Cancellation of the running test.</summary>
    public static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>
    /// Start settings for clusters exercised without the SDK: the trivial deterministic test volume state machine (so
    /// volume groups exist even before the production volume service is part of the build) and no SDK admin client.
    /// </summary>
    public static HarnessStartOptions NodeHostOnly { get; } = new()
    {
        VolumeStateMachines = TestVolumeStateMachineFactory.Instance,
        ConnectAdminClient = false,
    };

    /// <summary>Options of a cluster with <paramref name="nodes"/> one-per-zone nodes and one volume.</summary>
    public static TestClusterOptions Options(int nodes, ClusterMode mode, ulong seed = 0, BootstrapMode bootstrap = BootstrapMode.Static) => new()
    {
        NodeCount = nodes,
        Mode = mode,
        Seed = seed,
        Bootstrap = bootstrap,
        ControlVoters = Math.Min(nodes, 3),
        Volumes = [new TestVolumeSpec("vol") { Metadata = new DfsMetadataPolicy { Voters = Math.Min(nodes, 3) } }],
        ScenarioTimeout = TimeSpan.FromMinutes(5),
    };
}
