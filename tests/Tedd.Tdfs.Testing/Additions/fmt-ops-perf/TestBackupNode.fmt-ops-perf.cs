namespace Tedd.Tdfs.Testing;

// Additions for the FMT suite (docs/api-additions/fmt-ops-perf.md): running a backup node as a specific software release.
public sealed partial class TestBackupNode
{
    /// <summary>The release this backup node currently runs (or last ran). Harness-started backup nodes run <see cref="SoftwareVersionProfile.Current"/>.</summary>
    public SoftwareVersionProfile SoftwareVersion => throw Stub.NotYet("TestBackupNode.SoftwareVersion");

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
    /// </remarks>
    /// <param name="profile">The release to run.</param>
    public Task RestartAsSoftwareVersionAsync(SoftwareVersionProfile profile) =>
        throw Stub.NotYet("TestBackupNode.RestartAsSoftwareVersionAsync");
}
