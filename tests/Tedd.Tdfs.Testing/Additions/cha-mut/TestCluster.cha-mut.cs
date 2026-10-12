namespace Tedd.Tdfs.Testing;

// Additions requested by the CHA/MUT suites (see docs/api-additions/cha-mut.md).
public sealed partial class TestCluster
{
    /// <summary>
    /// Starts an independent "recovery" cluster that shares this cluster's execution context but none of its identity,
    /// used for disaster-recovery scenarios that restore from this cluster's backup nodes onto clean hosts
    /// (CHA-006, CHA-010, CHA-016).
    /// </summary>
    /// <remarks>
    /// Required behaviour:
    /// <list type="bullet">
    /// <item>The recovery cluster is bootstrapped from scratch: a new <see cref="ClusterId"/>, a new cluster secret and key
    /// hierarchy, fresh empty disks, its own control group, its own "admin" principal and the volumes listed in
    /// <paramref name="options"/> (<see cref="TestClusterOptions.Volumes"/> may be empty). It never shares Raft groups,
    /// keys, sessions, caches or storage with this cluster.</item>
    /// <item>It runs in the same execution context: in <see cref="ClusterMode.Deterministic"/> mode on the same
    /// simulation scheduler and virtual clock; in realtime mode in the same process. <paramref name="options"/>.Mode,
    /// Transport and Storage must equal this cluster's, otherwise <see cref="ArgumentException"/>.</item>
    /// <item>It is attached to the same network fabric, so its nodes and clients can reach this cluster's backup nodes
    /// (and, subject to network rules, its live nodes). <see cref="Network"/> of either cluster controls the same fabric:
    /// partitions, cuts and holds may name endpoints of both clusters.</item>
    /// <item>Node names come from <paramref name="options"/>.Nodes, or default to "r0".."r{N-1}" (instead of "n0..") laid
    /// out per <see cref="TestClusterOptions.Topology"/>; backup nodes default to "rb0"... Any name that collides with an
    /// endpoint of this cluster (or of another recovery cluster) → <see cref="ArgumentException"/>.</item>
    /// <item>Returns once the recovery cluster is stable (control and volume leaders elected). Its <see cref="Faults"/>,
    /// <see cref="Inspect"/> and <see cref="History"/> are its own. It is disposed together with this cluster unless
    /// disposed earlier; disposing it never affects this cluster.</item>
    /// </list>
    /// </remarks>
    /// <param name="options">Recovery-cluster options (mode, transport and storage must match this cluster).</param>
    /// <param name="ct">Cancellation.</param>
    /// <returns>The started recovery cluster.</returns>
    public Task<TestCluster> StartRecoveryClusterAsync(TestClusterOptions options, CancellationToken ct = default) =>
        throw Stub.NotYet("TestCluster.StartRecoveryClusterAsync");
}
