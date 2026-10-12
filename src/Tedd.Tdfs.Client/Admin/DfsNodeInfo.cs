namespace Tedd.Tdfs;

/// <summary>A live cluster member.</summary>
/// <param name="Id">Node id.</param>
/// <param name="Name">Node name.</param>
/// <param name="Labels">Topology labels.</param>
/// <param name="Endpoints">Node endpoints.</param>
/// <param name="Status">Membership status.</param>
/// <param name="Incarnation">Current incarnation.</param>
/// <param name="IsControlVoter">True when the node votes in the control group.</param>
/// <param name="IsReachable">True when the node is currently reachable.</param>
/// <param name="CapacityBytes">Storage capacity.</param>
/// <param name="UsedBytes">Storage used.</param>
public sealed record DfsNodeInfo(
    NodeId Id,
    string Name,
    DfsTopologyLabels Labels,
    IReadOnlyList<string> Endpoints,
    DfsNodeStatus Status,
    ulong Incarnation,
    bool IsControlVoter,
    bool IsReachable,
    long CapacityBytes,
    long UsedBytes);
