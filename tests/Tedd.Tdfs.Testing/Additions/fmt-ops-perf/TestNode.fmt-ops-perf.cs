using Tedd.Tdfs.Server;

namespace Tedd.Tdfs.Testing;

// Additions for the FMT suite (docs/api-additions/fmt-ops-perf.md): running a live node as a specific software release.
public sealed partial class TestNode
{
    private SoftwareVersionProfile? _softwareVersion;

    /// <summary>
    /// The release this node currently runs (or last ran, when it is stopped or crashed). Nodes started by the harness run
    /// <see cref="SoftwareVersionProfile.Current"/> (or the initial release of a <see cref="VersionedClusterTest"/> scenario);
    /// every later start — <see cref="RestartAsync"/> included — keeps the release until
    /// <see cref="RestartAsSoftwareVersionAsync"/> selects another one.
    /// </summary>
    public SoftwareVersionProfile SoftwareVersion => _softwareVersion ?? Cluster.Options.InitialSoftwareVersion ?? SoftwareVersionProfile.Current;

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
    /// its medium. A refused node can be restarted again with another profile (it keeps reporting the release it last ran).
    /// </para>
    /// <para>
    /// Faults: armed faults apply during startup (journal replay, checkpoint install, format migration, incarnation
    /// registration). If an armed fault crashes the node during startup, the task completes (without throwing) once the node
    /// has crashed; <see cref="State"/> is then <see cref="TestNodeState.Crashed"/>.
    /// </para>
    /// <para>
    /// Implementation: the in-process node runtime emulates the profile (protocol range, features and readable formats, see
    /// <c>NodeEnvironment.Release</c>); every release of the window writes the build's own disk format, which they all read.
    /// Out-of-process hosts run the build under test only.
    /// </para>
    /// </remarks>
    /// <param name="profile">The release to run.</param>
    /// <param name="reconfigure">Optional node-options transform applied before the start.</param>
    /// <exception cref="NotSupportedException">An out-of-process node and a profile other than the build under test.</exception>
    public async Task RestartAsSoftwareVersionAsync(SoftwareVersionProfile profile, Func<NodeOptions, NodeOptions>? reconfigure = null)
    {
        ArgumentNullException.ThrowIfNull(profile);
        if (Cluster.Options.OutOfProcess && !ReferenceEquals(profile, SoftwareVersionProfile.Current))
        {
            throw new NotSupportedException(
                $"Node {Name} cannot run release '{profile.Name}': out-of-process hosts run the build under test (release emulation is in-process).");
        }

        Cluster.Record($"node {Name}: restart as release {profile.Name}");
        if (State is TestNodeState.Running or TestNodeState.Paused)
            await StopAsync();
        SoftwareVersionProfile previous = SoftwareVersion;
        _softwareVersion = profile;
        try
        {
            await RestartAsync(reconfigure);
        }
        catch (DfsException ex) when (ex.ErrorCode == DfsErrorCode.Unsupported)
        {
            _softwareVersion = previous; // the refused release never ran
            Cluster.Record($"node {Name}: release {profile.Name} refused the durable image ({ex.Message})");
            throw;
        }

        // Registration needs a control quorum: when too few control voters run (a whole-cluster restart brings them back one
        // at a time), the start completes once the node runs.
        if (State == TestNodeState.Running && !Cluster.Options.OutOfProcess && await ControlQuorumRunningAsync())
            await Cluster.WaitForRegistrationAsync(this, Cluster.RegistrationTimeout);
    }

    /// <summary>True when a majority of the control voters (as a running node knows them) is running.</summary>
    private async Task<bool> ControlQuorumRunningAsync()
    {
        foreach (TestNode node in Cluster.Nodes.Where(n => n.State == TestNodeState.Running))
        {
            try
            {
                if (await node.Inspect.GetGroupAsync(Protocol.ProtocolInfo.ControlGroup) is not { Voters.Count: > 0 } control)
                    continue;
                int running = control.Voters.Count(v => Cluster.FindNode(v) is { State: TestNodeState.Running });
                return running * 2 > control.Voters.Count;
            }
            catch (Exception ex) when (TestCluster.IsNodeGone(ex))
            {
                // Went away meanwhile: ask another node.
            }
        }

        return false;
    }
}
