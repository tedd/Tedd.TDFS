namespace Tedd.Tdfs.Server;

/// <summary>An initial cluster member in a static bootstrap description.</summary>
/// <param name="Id">Node id.</param>
/// <param name="Name">Unique node name.</param>
/// <param name="Labels">Topology labels.</param>
/// <param name="Endpoints">Endpoints the node listens on.</param>
public sealed record BootstrapMember(NodeId Id, string Name, DfsTopologyLabels Labels, IReadOnlyList<string> Endpoints);
