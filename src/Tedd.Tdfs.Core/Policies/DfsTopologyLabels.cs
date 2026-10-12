namespace Tedd.Tdfs;

/// <summary>
/// Failure-domain labels of a node. Copies are counted by distinct <paramref name="Host"/>; zones by distinct
/// (<paramref name="Region"/>, <paramref name="Zone"/>). All comparisons are ordinal.
/// </summary>
/// <param name="Region">Region label.</param>
/// <param name="Zone">Zone label (unique within a region).</param>
/// <param name="Rack">Rack label.</param>
/// <param name="Host">Host label (physical machine).</param>
/// <param name="Device">Device label (disk).</param>
public sealed record DfsTopologyLabels(string Region, string Zone, string Rack, string Host, string Device)
{
    /// <summary>The zone failure-domain key (region, zone).</summary>
    public (string Region, string Zone) ZoneKey => (Region, Zone);
}
