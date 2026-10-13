using Tedd.Tdfs.Server;

namespace Tedd.Tdfs.Testing;

// Additions for the FMT suite (docs/api-additions/fmt-ops-perf.md): running a live node as a specific software release.
public sealed partial class TestNode
{
    private SoftwareVersionProfile? _softwareVersion;

    /// <summary>
    /// The release this node currently runs (or last ran, when it is stopped or crashed). Nodes started by the harness run
    /// <see cref="SoftwareVersionProfile.Current"/>.
    /// </summary>
    public SoftwareVersionProfile SoftwareVersion => _softwareVersion ?? SoftwareVersionProfile.Current;

    /// <summary>
    /// Restarts this node from its durable image as <paramref name="profile"/> (FMT-005..FMT-010; CHA-011 rolling upgrade
    /// to <see cref="SoftwareVersionProfile.Next"/> — this member replaces the <c>RestartWithSoftwareAsync</c> the CHA suite
    /// originally requested).
    /// </summary>
    /// <remarks>
    /// <para>
    /// A running or paused node is first stopped gracefully (exactly like <see cref="StopAsync"/>); a stopped or crashed node
    /// is started directly. The node then starts like <see cref="RestartAsync"/> (same name, node id and medium, incarnation
    /// + 1, optional <paramref name="reconfigure"/> applied to its <see cref="NodeOptions"/>) but behaves as the given release:
    /// it negotiates only the profile's protocol range, advertises only the profile's features in every handshake
    /// (node-to-node and client) and to the control group (cluster status / feature eligibility), writes the profile's disk
    /// format, reads only images the profile can read, and refuses to interpret messages, log entries or checkpoints that
    /// require a feature outside its set (it answers <see cref="DfsErrorCode.Unsupported"/> / stops serving the affected
    /// group rather than misreading state). The task completes when the node is running and has registered its
    /// incarnation with the control group (or, when the control group is unavailable, when it is running).
    /// </para>
    /// <para>
    /// Refusal: when the profile cannot interpret the durable image (see <see cref="SoftwareVersionProfile"/>), the task
    /// fails with a <see cref="DfsException"/> whose <see cref="DfsException.ErrorCode"/> is
    /// <see cref="DfsErrorCode.Unsupported"/>, the node remains <see cref="TestNodeState.Stopped"/>, and nothing was written to
    /// its medium. A refused node can be restarted again with another profile.
    /// </para>
    /// <para>
    /// Faults: armed faults apply during startup (journal replay, checkpoint install, format migration, incarnation
    /// registration). If an armed fault crashes the node during startup, the task completes (without throwing) once the node
    /// has crashed; <see cref="State"/> is then <see cref="TestNodeState.Crashed"/>.
    /// </para>
    /// <para>
    /// Implementation status: the node runtime has no release emulation (protocol range, feature set and disk format are
    /// fixed by the build), so only <see cref="SoftwareVersionProfile.Current"/> can be run; any other profile is rejected with
    /// <see cref="NotSupportedException"/> before the node is touched, rather than running the current build under another
    /// release's name.
    /// </para>
    /// </remarks>
    /// <param name="profile">The release to run.</param>
    /// <param name="reconfigure">Optional node-options transform applied before the start.</param>
    /// <exception cref="NotSupportedException"><paramref name="profile"/> is not the build under test.</exception>
    public async Task RestartAsSoftwareVersionAsync(SoftwareVersionProfile profile, Func<NodeOptions, NodeOptions>? reconfigure = null)
    {
        ArgumentNullException.ThrowIfNull(profile);
        if (!ReferenceEquals(profile, SoftwareVersionProfile.Current))
        {
            throw new NotSupportedException(
                $"Node {Name} cannot run release '{profile.Name}': the node runtime has no release emulation (protocol range, features and disk format are fixed by the build).");
        }

        Cluster.Record($"node {Name}: restart as release {profile.Name}");
        await RestartAsync(reconfigure);
        _softwareVersion = profile;
        if (State == TestNodeState.Running && !Cluster.Options.OutOfProcess)
            await Cluster.WaitForRegistrationAsync(this, Cluster.RegistrationTimeout);
    }
}
