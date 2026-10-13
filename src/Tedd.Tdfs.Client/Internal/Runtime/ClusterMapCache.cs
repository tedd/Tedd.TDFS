using Tedd.Tdfs.Data;
using Tedd.Tdfs.Protocol.Messages;

namespace Tedd.Tdfs.Client.Internal.Runtime;

/// <summary>
/// The client's view of the cluster map (members, endpoints, pools, volumes, placement epoch), refreshed from any live node
/// with <c>GetClusterMap</c>. Only maps of the bound cluster with a config index at least as new as the current one are
/// accepted. Thread-safe (immutable snapshots).
/// </summary>
internal sealed class ClusterMapCache
{
    private readonly Lock _sync = new();
    private readonly IReadOnlyList<string> _seeds;
    private ClusterMapData? _map;
    private Dictionary<NodeId, NodeEntry> _nodes = [];
    private string? _preferred;

    public ClusterMapCache(IReadOnlyList<string> seeds)
    {
        ArgumentNullException.ThrowIfNull(seeds);
        _seeds = seeds;
    }

    /// <summary>The current map, or null before the first one was received.</summary>
    public ClusterMapData? Current
    {
        get
        {
            lock (_sync)
                return _map;
        }
    }

    /// <summary>Config index of the current map (0 before the first one).</summary>
    public ulong ConfigIndex => Current?.ConfigIndex ?? 0;

    /// <summary>Installs <paramref name="map"/> when it is not older than the current one. Returns true when installed.</summary>
    public bool Update(ClusterMapData map)
    {
        ArgumentNullException.ThrowIfNull(map);
        lock (_sync)
        {
            if (_map is not null && (map.ClusterId != _map.ClusterId || map.ConfigIndex < _map.ConfigIndex))
                return false;
            _map = map;
            var nodes = new Dictionary<NodeId, NodeEntry>(map.Nodes.Count);
            foreach (NodeEntry node in map.Nodes)
                nodes[node.Id] = node;
            _nodes = nodes;
            return true;
        }
    }

    /// <summary>Remembers an endpoint that answered recently (tried first for discovery).</summary>
    public void MarkGood(string endpoint)
    {
        lock (_sync)
            _preferred = endpoint;
    }

    /// <summary>The member with id <paramref name="node"/>, if known.</summary>
    public NodeEntry? Node(NodeId node)
    {
        lock (_sync)
            return _nodes.GetValueOrDefault(node);
    }

    /// <summary>Preferred endpoint of a member (its first advertised endpoint), or null.</summary>
    public string? EndpointOf(NodeId node)
    {
        NodeEntry? entry = Node(node);
        return entry is { Endpoints.Count: > 0 } ? entry.Endpoints[0] : null;
    }

    /// <summary>The member whose endpoint list contains <paramref name="endpoint"/>, if known.</summary>
    public NodeEntry? NodeAt(string endpoint)
    {
        lock (_sync)
        {
            foreach (NodeEntry node in _nodes.Values)
            {
                foreach (string e in node.Endpoints)
                {
                    if (string.Equals(e, endpoint, StringComparison.Ordinal))
                        return node;
                }
            }
        }

        return null;
    }

    /// <summary>The volume named <paramref name="name"/> in the current map, if present.</summary>
    public VolumeEntry? Volume(string name)
    {
        ClusterMapData? map = Current;
        if (map is null)
            return null;
        foreach (VolumeEntry volume in map.Volumes)
        {
            if (string.Equals(volume.Name, name, StringComparison.Ordinal))
                return volume;
        }

        return null;
    }

    /// <summary>
    /// Endpoints to try for discovery and other requests any node can answer: the last endpoint that answered, the live
    /// members of the map, then the configured seeds (deduplicated, in that order).
    /// </summary>
    public IReadOnlyList<string> DiscoveryEndpoints()
    {
        var result = new List<string>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        lock (_sync)
        {
            if (_preferred is not null && seen.Add(_preferred))
                result.Add(_preferred);
            if (_map is not null)
            {
                foreach (NodeEntry node in _map.Nodes)
                {
                    if (node.Status is WireNodeStatus.Removed || node.Endpoints.Count == 0)
                        continue;
                    if (seen.Add(node.Endpoints[0]))
                        result.Add(node.Endpoints[0]);
                }
            }
        }

        foreach (string seed in _seeds)
        {
            if (seen.Add(seed))
                result.Add(seed);
        }

        return result;
    }

    /// <summary>
    /// Placement candidates of <paramref name="pool"/> (live, non-backup members; the <c>default</c> pool holds every live
    /// member, named pools the members listing them), with eligibility and free capacity as last reported.
    /// </summary>
    public IReadOnlyList<PlacementNode> PoolNodes(string pool)
    {
        ClusterMapData? map = Current;
        if (map is null)
            return [];
        bool isDefault = string.Equals(pool, DfsPoolNames.Default, StringComparison.Ordinal);
        var nodes = new List<PlacementNode>(map.Nodes.Count);
        foreach (NodeEntry node in map.Nodes)
        {
            if (node.IsBackup || node.Status is WireNodeStatus.Removed || node.Endpoints.Count == 0)
                continue;
            if (!isDefault && !node.Pools.Contains(pool))
                continue;
            bool eligible = node.PlacementEligible && node.Status == WireNodeStatus.Active;
            long free = node.CapacityBytes - node.UsedBytes;
            nodes.Add(new PlacementNode(node.Id, node.Name, node.Labels, eligible, free > 0 ? free : 0));
        }

        return nodes;
    }

    /// <summary>Every live, non-backup member (fallback search for object copies, LocateObject broadcast).</summary>
    public IReadOnlyList<NodeEntry> StorageNodes()
    {
        ClusterMapData? map = Current;
        if (map is null)
            return [];
        return map.Nodes.Where(n => !n.IsBackup && n.Status is not WireNodeStatus.Removed && n.Endpoints.Count > 0).ToArray();
    }
}
