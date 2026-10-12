using Tedd.Tdfs.Server;

namespace Tedd.Tdfs.Testing;

// Harness additions requested by the RST and SEC suites (see docs/api-additions/rst-sec.md). StartRecoveryClusterAsync
// is also used by the CHA suite (docs/api-additions/cha-mut.md) and is defined only here.
public sealed partial class TestCluster
{
    /// <summary>
    /// Starts an independent <b>recovery cluster</b> on a "clean host": a brand-new live cluster bootstrapped from scratch
    /// with a new <see cref="ClusterId"/>, a new cluster secret and key hierarchy, its own control group, its own "admin"
    /// principal, fresh empty disks under a new data root (a subfolder of this cluster's <see cref="DataRoot"/> that did not
    /// exist before the call), and no volumes unless <paramref name="options"/> lists some
    /// (<see cref="TestClusterOptions.Volumes"/> may be empty). It shares <b>no</b> Raft group, in-memory state, sessions,
    /// client caches, connections, credentials, keys or disks with this cluster's live nodes.
    /// </summary>
    /// <remarks>
    /// <para>Consolidated definition requested by the RST/SEC suites (RST-001..006, RST-008, RST-010, RST-011,
    /// RST-014..016) and the CHA suite (CHA-006, CHA-010, CHA-016); see docs/api-additions/rst-sec.md and
    /// docs/api-additions/cha-mut.md.</para>
    /// <para>Execution context: the recovery cluster runs in the same execution context as this cluster — in
    /// <see cref="ClusterMode.Deterministic"/> mode inside the same simulation (same scheduler, virtual clock and simulated
    /// network), so S-level scenarios can restore into a new cluster; in realtime mode in the same process (unless
    /// <see cref="TestClusterOptions.OutOfProcess"/> is set, see below).</para>
    /// <para>Reachability: the recovery cluster runs on the same transport fabric as this cluster so that it can reach
    /// this cluster's <see cref="BackupNodes"/> (for <see cref="DfsAdminClient.RestoreFromBackupAsync"/>) and, subject to
    /// network rules, its live nodes. For <see cref="TransportKind.Quic"/> the recovery cluster's nodes and clients trust
    /// the backup hosts' certificates exactly as an operator would configure them from the recovery runbook (pinning the
    /// backup host's certificate); they are <b>not</b> given any source live node's certificate, endpoint or secret.</para>
    /// <para>Naming: node names in <paramref name="options"/> must not collide with an endpoint of this cluster (node,
    /// backup node or client names) or of another recovery cluster; when <see cref="TestClusterOptions.Nodes"/> is null the
    /// nodes are named <c>r0..r{N-1}</c> (zones per <see cref="TestClusterOptions.Topology"/>) and backup nodes
    /// <c>rb0..</c>. Both clusters' <see cref="Network"/> objects control the same fabric and address every endpoint (either
    /// cluster's nodes, backup nodes and clients) by name, so partitions, cuts, isolations and holds may span both
    /// clusters. <see cref="TestNetwork.CountAttemptsTo"/> on either network counts attempts from any sender on the
    /// fabric.</para>
    /// <para>Options: <see cref="TestClusterOptions.Mode"/>, <see cref="TestClusterOptions.Transport"/> and (for
    /// in-process clusters) <see cref="TestClusterOptions.Storage"/> must equal this cluster's; with
    /// <see cref="TestClusterOptions.OutOfProcess"/> = true the recovery nodes are real <c>Tedd.Tdfs.Host</c> processes
    /// started from the published binaries with only the configuration the recovery runbook documents (generated
    /// certificate, new cluster secret, node identity initialisation). <see cref="TestClusterOptions.DataRoot"/> is
    /// ignored (always a fresh folder).</para>
    /// <para>Lifetime: the call returns once the recovery cluster is stable (control leader and every listed volume's
    /// leader elected). Its <see cref="Faults"/>, <see cref="Inspect"/>, <see cref="History"/> and
    /// <see cref="Admin"/> are its own. The caller may dispose the returned cluster earlier; disposing it never affects
    /// this cluster, and disposing this cluster also disposes every recovery cluster it started.</para>
    /// </remarks>
    /// <param name="options">Recovery-cluster options (mode, transport and in-process storage must match this cluster).</param>
    /// <param name="ct">Cancellation.</param>
    /// <returns>The started, stable recovery cluster.</returns>
    /// <exception cref="ArgumentException">Name collision or incompatible mode/transport/storage.</exception>
    public Task<TestCluster> StartRecoveryClusterAsync(TestClusterOptions options, CancellationToken ct = default) =>
        StartRecoveryClusterCoreAsync(options, ct);

    /// <summary>
    /// Adds a live node whose <see cref="NodeOptions"/> are transformed by <paramref name="configure"/> before the node
    /// starts (for example a wrong <see cref="NodeOptions.ClusterSecret"/>, a foreign <see cref="NodeOptions.Bootstrap"/>
    /// or explicit <see cref="NodeOptions.JoinSeeds"/>), for peer-identity tests (SEC-001).
    /// </summary>
    /// <remarks>
    /// When <paramref name="join"/> is true the node performs the normal seed join against this cluster's live nodes
    /// (unless <paramref name="configure"/> replaced the seeds). Unlike <see cref="AddNodeAsync(TestNodeSpec, bool, CancellationToken)"/>
    /// this overload does <b>not</b> throw when the cluster rejects the node: it completes once the node process has
    /// started and its join attempt has either been admitted (the node appears in the control state) or been
    /// definitively rejected (the rejecting peer answered with an authentication/cluster-identity/removed-member
    /// error, recorded in <see cref="Trace"/>), or after the join timeout of the node's timings. The node is added to
    /// <see cref="Nodes"/> in every case so tests can inspect, stop or crash it. It throws only for harness errors
    /// (duplicate name, invalid spec).
    /// </remarks>
    public Task<TestNode> AddNodeAsync(TestNodeSpec spec, Func<NodeOptions, NodeOptions> configure, bool join = true, CancellationToken ct = default) =>
        AddConfiguredNodeAsync(spec, configure, join, ct);
}
