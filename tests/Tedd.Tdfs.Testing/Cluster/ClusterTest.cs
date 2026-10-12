namespace Tedd.Tdfs.Testing;

/// <summary>Entry point for cluster scenarios.</summary>
public static class ClusterTest
{
    /// <summary>
    /// Starts a cluster, waits until it is stable (control and all volume leaders elected, all nodes caught up), runs the
    /// scenario (inside the simulation in deterministic mode), checks harness invariants and disposes the cluster.
    /// </summary>
    public static Task RunAsync(TestClusterOptions options, Func<TestCluster, Task> scenario, CancellationToken ct = default) =>
        throw Stub.NotYet("ClusterTest.RunAsync");

    /// <summary>As <see cref="RunAsync(TestClusterOptions, Func{TestCluster, Task}, CancellationToken)"/>, returning the scenario result.</summary>
    public static Task<T> RunAsync<T>(TestClusterOptions options, Func<TestCluster, Task<T>> scenario, CancellationToken ct = default) =>
        throw Stub.NotYet("ClusterTest.RunAsync<T>");
}
