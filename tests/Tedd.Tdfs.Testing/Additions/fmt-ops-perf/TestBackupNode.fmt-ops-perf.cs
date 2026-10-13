namespace Tedd.Tdfs.Testing;

// Additions for the FMT suite (docs/api-additions/fmt-ops-perf.md): running a backup node as a specific software release.
public sealed partial class TestBackupNode
{
    private SoftwareVersionProfile? _softwareVersion;

    /// <summary>The release this backup node currently runs (or last ran). Harness-started backup nodes run <see cref="SoftwareVersionProfile.Current"/>.</summary>
    public SoftwareVersionProfile SoftwareVersion => _softwareVersion ?? SoftwareVersionProfile.Current;

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
    /// <para>Implementation status: as for live nodes, only <see cref="SoftwareVersionProfile.Current"/> can be run; any
    /// other profile is rejected with <see cref="NotSupportedException"/> before the node is touched.</para>
    /// </remarks>
    /// <param name="profile">The release to run.</param>
    /// <exception cref="NotSupportedException"><paramref name="profile"/> is not the build under test.</exception>
    public async Task RestartAsSoftwareVersionAsync(SoftwareVersionProfile profile)
    {
        ArgumentNullException.ThrowIfNull(profile);
        if (!ReferenceEquals(profile, SoftwareVersionProfile.Current))
        {
            throw new NotSupportedException(
                $"Backup node {Name} cannot run release '{profile.Name}': the node runtime has no release emulation.");
        }

        Cluster.Record($"backup {Name}: restart as release {profile.Name}");
        await RestartAsync();
        _softwareVersion = profile;
    }
}
