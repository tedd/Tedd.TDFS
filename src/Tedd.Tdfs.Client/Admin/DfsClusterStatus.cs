namespace Tedd.Tdfs;

/// <summary>Administrative status of the cluster.</summary>
/// <param name="Cluster">Cluster identity.</param>
/// <param name="Control">Control group status.</param>
/// <param name="Nodes">Live members.</param>
/// <param name="Volumes">Volume names.</param>
/// <param name="PlacementEpoch">Current placement epoch.</param>
/// <param name="ActiveFeatures">Cluster-wide active features.</param>
public sealed record DfsClusterStatus(
    ClusterIdentity Cluster,
    DfsGroupStatus Control,
    IReadOnlyList<DfsNodeInfo> Nodes,
    IReadOnlyList<string> Volumes,
    ulong PlacementEpoch,
    IReadOnlySet<string> ActiveFeatures);
