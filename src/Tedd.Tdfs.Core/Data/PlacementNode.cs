namespace Tedd.Tdfs.Data;

/// <summary>A storage node candidate for placement.</summary>
/// <param name="Id">Node id (input to rendezvous hashing).</param>
/// <param name="Name">Unique node name.</param>
/// <param name="Labels">Failure-domain labels.</param>
/// <param name="Eligible">False for nodes that must not receive new copies (draining, removed, disallowed domain, wrong tier).</param>
/// <param name="FreeBytes">Free capacity; nodes with no free capacity (≤ 0) are treated as full and are never selected.</param>
public sealed record PlacementNode(NodeId Id, string Name, DfsTopologyLabels Labels, bool Eligible, long FreeBytes)
{
    /// <summary>True when the node can receive new copies (<see cref="Eligible"/> and not full).</summary>
    public bool CanReceive => Eligible && FreeBytes > 0;
}
