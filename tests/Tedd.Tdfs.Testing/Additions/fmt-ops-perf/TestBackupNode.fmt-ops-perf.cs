namespace Tedd.Tdfs.Testing;

// Additions for the FMT suite (docs/api-additions/fmt-ops-perf.md): running a backup node as a specific software release.
public sealed partial class TestBackupNode
{
    private SoftwareVersionProfile? _softwareVersion;

    /// <summary>
    /// The release this backup node currently runs (or last ran). Harness-started backup nodes run
    /// <see cref="SoftwareVersionProfile.Current"/> (or the initial release of a <see cref="VersionedClusterTest"/> scenario);
    /// every later start keeps the release until <see cref="RestartAsSoftwareVersionAsync"/> selects another one.
    /// </summary>
    public SoftwareVersionProfile SoftwareVersion => _softwareVersion ?? Cluster.Options.InitialSoftwareVersion ?? SoftwareVersionProfile.Current;

    /// <summary>
    /// Restarts this backup node from its durable image (published snapshots, manifests, objects, retention state and its
    /// own key material) as <paramref name="profile"/> (FMT-010).
    /// </summary>
    /// <remarks>
    /// Same contract as <see cref="TestNode.RestartAsSoftwareVersionAsync"/>: a running node is stopped gracefully first; the
    /// restarted node negotiates only the profile's protocol range and writes the profile's backup format; when the profile
    /// cannot read the retained image the task fails with a <see cref="DfsException"/> whose
    /// <see cref="DfsException.ErrorCode"/> is <see cref="DfsErrorCode.Unsupported"/> and nothing is written to the medium.
    /// Its registration name (<see cref="TargetName"/>), endpoint and credentials are unchanged.
    /// <para>Implementation: as for live nodes, the in-process backup runtime emulates the profile (its protocol range and
    /// the highest format it reads, <c>NodeEnvironment.Release</c>); every release of the window stores the build's own
    /// formats, which they all read. Out-of-process hosts run the build under test only.</para>
    /// </remarks>
    /// <param name="profile">The release to run.</param>
    /// <exception cref="NotSupportedException">An out-of-process backup node and a profile other than the build under test.</exception>
    public async Task RestartAsSoftwareVersionAsync(SoftwareVersionProfile profile)
    {
        ArgumentNullException.ThrowIfNull(profile);
        if (Cluster.Options.OutOfProcess && !ReferenceEquals(profile, SoftwareVersionProfile.Current))
        {
            throw new NotSupportedException(
                $"Backup node {Name} cannot run release '{profile.Name}': out-of-process hosts run the build under test (release emulation is in-process).");
        }

        Cluster.Record($"backup {Name}: restart as release {profile.Name}");
        if (State is TestNodeState.Running or TestNodeState.Paused)
            await StopAsync();
        SoftwareVersionProfile previous = SoftwareVersion;
        _softwareVersion = profile;
        try
        {
            await RestartAsync();
        }
        catch (DfsException ex) when (ex.ErrorCode == DfsErrorCode.Unsupported)
        {
            _softwareVersion = previous; // the refused release never ran
            Cluster.Record($"backup {Name}: release {profile.Name} refused the durable image ({ex.Message})");
            throw;
        }
    }
}
