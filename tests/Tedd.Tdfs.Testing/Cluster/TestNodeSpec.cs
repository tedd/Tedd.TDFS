namespace Tedd.Tdfs.Testing;

/// <summary>Placement and identity of one live test node.</summary>
/// <param name="Name">Unique node name ("n0".."n9" by convention).</param>
/// <param name="Zone">Zone label.</param>
/// <param name="Host">Host label; null means the node name (every node is its own host).</param>
/// <param name="Region">Region label.</param>
/// <param name="Rack">Rack label.</param>
/// <param name="Device">Device label.</param>
public sealed record TestNodeSpec(string Name, string Zone, string? Host = null, string Region = "r1", string Rack = "rk1", string Device = "d1")
{
    /// <summary>The effective host label (<see cref="Host"/> or, when null, <see cref="Name"/>).</summary>
    public string EffectiveHost => Host ?? Name;

    /// <summary>Topology labels for this node.</summary>
    public DfsTopologyLabels ToLabels() => new(Region, Zone, Rack, EffectiveHost, Device);
}
