using Tedd.Tdfs.Server;

namespace Tedd.Tdfs.Testing;

// Harness additions requested by the CON/NET suites (see docs/api-additions/con-net.md). TransferLeadershipAsync is also
// used by the RSY suite (docs/api-additions/rsy.md) and is defined only here.
public sealed partial class TestCluster
{
    /// <summary>
    /// Transfers leadership of a Raft group to a chosen node through the protocol's own leadership transfer and waits until
    /// that node leads the group. Returns the target node.
    /// </summary>
    /// <param name="group">"control" or a volume name.</param>
    /// <param name="targetNode">Name of a running voter of the group ("n0".."n9").</param>
    /// <param name="timeout">
    /// Bound on the transfer in cluster time (virtual time in <see cref="ClusterMode.Deterministic"/> mode); null = the larger
    /// of 10 s and 10 maximum election timeouts of the cluster's <see cref="NodeTimings"/>.
    /// </param>
    /// <param name="waitUntilServing">
    /// True (default): additionally wait until every reachable voter reports the target as leader and the target has
    /// applied its own no-op entry of the new term (so it can serve linearizable reads). False: complete as soon as the
    /// target reports <c>RaftRole.Leader</c> in a term higher than the one observed before the call, without waiting for the
    /// new leader to apply earlier committed entries (CON-009 relies on observing a leader whose apply lags its commit
    /// index).
    /// </param>
    /// <param name="ct">Cancellation.</param>
    /// <remarks>
    /// <para>Consolidated definition requested by the CON/NET suites (CON-009 <c>apply-delayed-new-leader-by-transfer</c>,
    /// CON-012, CON-013, CON-015, NET-014 <c>node-moves-to-new-endpoint</c>; they pass
    /// <paramref name="waitUntilServing"/> = false) and the RSY suite (RSY-004..RSY-011: verify that a returning node serves
    /// the authoritative state itself; default <paramref name="waitUntilServing"/>). See docs/api-additions/con-net.md and
    /// docs/api-additions/rsy.md.</para>
    /// <para>Required behaviour: asks the current leader of <paramref name="group"/> to transfer leadership to
    /// <paramref name="targetNode"/> through the production path (<c>RaftNode.TransferLeadership</c>: the current leader
    /// sends <c>TimeoutNow</c> once the target's log is caught up and the target wins an election in a higher term). It never
    /// forces a role change, edits persisted state or bypasses the vote/log-up-to-date checks. Returns immediately when the
    /// target already leads the group (and, with <paramref name="waitUntilServing"/>, serves it). The transfer is recorded
    /// in <see cref="Trace"/>.</para>
    /// </remarks>
    /// <exception cref="ArgumentException">The group or the node name is unknown.</exception>
    /// <exception cref="InvalidOperationException">The target is not running or is not a voter of the group.</exception>
    /// <exception cref="TimeoutException">
    /// The transfer did not complete within the timeout; the message names the group, the target and the observed
    /// leader/term.
    /// </exception>
    public Task<TestNode> TransferLeadershipAsync(string group, string targetNode, TimeSpan? timeout = null, bool waitUntilServing = true, CancellationToken ct = default) =>
        TransferLeadershipCoreAsync(group, targetNode, timeout, waitUntilServing, ct);

    /// <summary>
    /// Starts a single-node cluster with a different cluster identity on an endpoint of this cluster's network, to test that
    /// clients and nodes validate cluster identity on (re)connection (NET-014).
    /// </summary>
    /// <param name="listenEndpoint">
    /// Endpoint to listen on, typically the <see cref="TestNode.Endpoint"/> of a crashed/stopped node of this cluster
    /// (so the address a client has cached now leads to a foreign cluster).
    /// </param>
    /// <param name="ct">Cancellation.</param>
    /// <returns>The foreign node host (in-process); it is disposed together with this cluster.</returns>
    /// <remarks>
    /// Required behaviour: the foreign node uses a fresh random <c>ClusterId</c>, a different cluster secret and its own
    /// volume named like this cluster's first volume, and bootstraps the same principal names and secrets as this cluster
    /// (including "admin"), so that application authentication alone cannot tell the clusters apart and only the cluster
    /// identity check can. It attaches to this cluster's transport (in-memory/simulated network or QUIC) under the network
    /// name derived from <paramref name="listenEndpoint"/>, uses this cluster's <see cref="NodeTimings"/>, clock mode and
    /// fault/network controls, and runs until the cluster is disposed. While it runs, a <see cref="TestClientOptions.SeedNodes"/>
    /// entry naming the node whose endpoint it took resolves to the foreign node's endpoint. Throws
    /// <see cref="InvalidOperationException"/> when a running node of this cluster still listens on the endpoint.
    /// </remarks>
    public Task<NodeHost> StartForeignClusterNodeAsync(string listenEndpoint, CancellationToken ct = default) =>
        StartForeignClusterNodeCoreAsync(listenEndpoint, ct);
}
