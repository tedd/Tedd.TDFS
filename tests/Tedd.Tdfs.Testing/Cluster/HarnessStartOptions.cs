using Tedd.Tdfs.Server.Volumes;

namespace Tedd.Tdfs.Testing;

/// <summary>
/// Harness-internal start settings that are not part of the public <see cref="TestClusterOptions"/> (internal tests of the
/// harness itself, and hosts of the harness that need to substitute server plug-ins).
/// </summary>
internal sealed record HarnessStartOptions
{
    /// <summary>Production defaults: the build's volume state machine, an SDK admin client, a two-minute start budget.</summary>
    public static HarnessStartOptions Default { get; } = new();

    /// <summary>
    /// Volume state machine factory injected into every node (<c>NodeEnvironment.VolumeStateMachines</c>); null uses the
    /// build's production factory.
    /// </summary>
    public IVolumeStateMachineFactory? VolumeStateMachines { get; init; }

    /// <summary>
    /// Connect the SDK admin client (<see cref="TestCluster.Admin"/>) at start. When the SDK cannot connect because it is not
    /// implemented in this build, the failure is kept and reported when <see cref="TestCluster.Admin"/> is used.
    /// </summary>
    public bool ConnectAdminClient { get; init; } = true;

    /// <summary>Budget (harness time) for bootstrap, volume creation and reaching a stable cluster.</summary>
    public TimeSpan StartTimeout { get; init; } = TimeSpan.FromSeconds(120);
}
