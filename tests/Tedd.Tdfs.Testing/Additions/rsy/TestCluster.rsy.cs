namespace Tedd.Tdfs.Testing;

public sealed partial class TestCluster
{
    /// <summary>
    /// Moves leadership of a Raft group to <paramref name="nodeName"/> through the protocol's own leadership transfer
    /// (the current leader sends <c>TimeoutNow</c> once the target is caught up; the target wins an election in a higher
    /// term). It never forces a role change or bypasses vote/log-up-to-date checks. Waits until the target reports
    /// <c>RaftRole.Leader</c> for the group, every reachable voter reports the target as leader, and the target has applied
    /// its own no-op entry of the new term (so it can serve linearizable reads). Returns the target node;
    /// when the target already leads the group it returns immediately.
    /// Added for the RSY suite (RSY-004..RSY-011: verify that a returning node serves the authoritative state itself;
    /// docs/api-additions/rsy.md).
    /// </summary>
    /// <param name="group">"control" or a volume name.</param>
    /// <param name="nodeName">The target live node (must be Running and a voter of the group).</param>
    /// <param name="timeout">Bound on the transfer (cluster time); default 10 s.</param>
    /// <param name="ct">Cancellation.</param>
    /// <exception cref="InvalidOperationException">The target is not running or is not a voter of the group.</exception>
    /// <exception cref="TimeoutException">The transfer did not complete within <paramref name="timeout"/>.</exception>
    public Task<TestNode> TransferLeadershipAsync(string group, string nodeName, TimeSpan? timeout = null, CancellationToken ct = default) =>
        throw Stub.NotYet("TestCluster.TransferLeadershipAsync");
}
