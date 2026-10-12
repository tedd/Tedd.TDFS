using Tedd.Tdfs.Server;

namespace Tedd.Tdfs.Testing;

// Harness additions requested by the CON/NET suites (see docs/api-additions/con-net.md).
public sealed partial class TestCluster
{
    /// <summary>
    /// Transfers leadership of a Raft group to a chosen node and waits until that node is the group's leader.
    /// </summary>
    /// <param name="group">"control" or a volume name.</param>
    /// <param name="targetNode">Name of a running voter of the group ("n0".."n9").</param>
    /// <param name="ct">Cancellation.</param>
    /// <remarks>
    /// Required behaviour: asks the current leader of <paramref name="group"/> to transfer leadership to
    /// <paramref name="targetNode"/> through the production path (<c>RaftNode.TransferLeadership</c>, which sends
    /// <c>TimeoutNow</c> once the target's log is caught up), never by editing persisted state. Completes as soon as the
    /// target reports <c>RaftRole.Leader</c> for the group in a term higher than the one observed before the call; it does
    /// not wait for the new leader to apply earlier committed entries (CON-009 relies on observing a leader whose apply
    /// lags its commit index). Returns immediately when the target already leads. Throws <see cref="ArgumentException"/> when
    /// the group is unknown or the target is not a voter of it, <see cref="InvalidOperationException"/> when the target is
    /// not running, and <see cref="TimeoutException"/> (naming the group, the target and the observed leader/term) when
    /// the transfer does not complete within 10 election timeouts of the cluster's <see cref="NodeTimings"/>. Uses virtual
    /// time in <see cref="ClusterMode.Deterministic"/> mode. The transfer is recorded in <see cref="Trace"/>.
    /// </remarks>
    public Task TransferLeadershipAsync(string group, string targetNode, CancellationToken ct = default) =>
        throw Stub.NotYet("TestCluster.TransferLeadershipAsync");

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
        throw Stub.NotYet("TestCluster.StartForeignClusterNodeAsync");
}
