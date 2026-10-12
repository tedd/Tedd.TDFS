namespace Tedd.Tdfs.Server;

public sealed partial class NodeInspector
{
    /// <summary>
    /// Returns the replication/catch-up statistics of one Raft group hosted on this node ("control" or a volume name),
    /// or null when this node does not host the group. Executed on the node loop, so all fields describe the same
    /// instant (no entry is applied between reading <see cref="RaftGroupStats.AppliedIndex"/> and the counters).
    /// Added for the RSY suite (RSY-001, RSY-002, RSY-012, RSY-013, RSY-015, RSY-026; docs/api-additions/rsy.md).
    /// </summary>
    /// <param name="group">"control" or a volume name.</param>
    public Task<RaftGroupStats?> GetRaftGroupStatsAsync(string group)
        => throw new NotImplementedException("NodeInspector.GetRaftGroupStatsAsync: implemented in a later phase");
}
