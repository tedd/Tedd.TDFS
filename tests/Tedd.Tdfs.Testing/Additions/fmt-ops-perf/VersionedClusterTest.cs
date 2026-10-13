namespace Tedd.Tdfs.Testing;

/// <summary>
/// Entry point for cluster scenarios whose nodes start life on a specific software release (FMT-005, FMT-007..FMT-010;
/// docs/api-additions/fmt-ops-perf.md).
/// </summary>
public static class VersionedClusterTest
{
    /// <summary>
    /// Behaves exactly like <see cref="ClusterTest.RunAsync(TestClusterOptions, Func{TestCluster, Task}, CancellationToken)"/>
    /// (start, wait until stable, run the scenario — inside the simulation in deterministic mode — check harness invariants,
    /// dispose), except that every live node and every backup node is created and bootstrapped by
    /// <paramref name="initialRelease"/>: fresh media are formatted, and superblocks, journals, checkpoints, object segments
    /// and backup stores are written in that release's disk format; handshakes negotiate within its protocol range; only its
    /// features are understood. Inside the scenario every <see cref="TestNode.SoftwareVersion"/> and
    /// <see cref="TestBackupNode.SoftwareVersion"/> reports <paramref name="initialRelease"/> until a node is restarted with
    /// <see cref="TestNode.RestartAsSoftwareVersionAsync"/>. Clients created by the scenario always run the current SDK.
    /// </summary>
    /// <param name="options">Cluster options (any combination accepted by <see cref="ClusterTest.RunAsync(TestClusterOptions, Func{TestCluster, Task}, CancellationToken)"/>).</param>
    /// <param name="initialRelease">The release that creates the cluster; must be a member of <see cref="SoftwareVersionProfile.SupportedWindow"/>.</param>
    /// <param name="scenario">The scenario.</param>
    /// <param name="ct">Cancellation.</param>
    /// <remarks>
    /// Live nodes emulate the release in-process (see <see cref="TestNode.RestartAsSoftwareVersionAsync"/>); out-of-process
    /// clusters run the build under test only.
    /// </remarks>
    /// <exception cref="ArgumentException"><paramref name="initialRelease"/> is outside the supported window.</exception>
    /// <exception cref="NotSupportedException">An out-of-process cluster and a release other than the build under test.</exception>
    public static Task RunAsync(TestClusterOptions options, SoftwareVersionProfile initialRelease, Func<TestCluster, Task> scenario, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(initialRelease);
        ArgumentNullException.ThrowIfNull(scenario);
        if (!SoftwareVersionProfile.SupportedWindow.Contains(initialRelease))
            throw new ArgumentException($"Release '{initialRelease.Name}' is outside the supported window.", nameof(initialRelease));
        if (ReferenceEquals(initialRelease, SoftwareVersionProfile.Current))
            return ClusterTest.RunAsync(options, scenario, ct);
        if (options.OutOfProcess)
        {
            throw new NotSupportedException(
                $"A cluster cannot be created by release '{initialRelease.Name}' out of process: hosts run the build under test (release emulation is in-process).");
        }

        return ClusterTest.RunAsync(options with { InitialSoftwareVersion = initialRelease }, scenario, ct);
    }
}
