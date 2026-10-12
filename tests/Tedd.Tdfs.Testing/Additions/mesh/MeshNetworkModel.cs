namespace Tedd.Tdfs.Testing.Mesh;

/// <summary>
/// Test-side mirror of node liveness and link state, maintained independently of the implementation (the scenario
/// updates it whenever it crashes, pauses, partitions or cuts something through the harness). It is the input of
/// <see cref="MeshOracle"/>.
/// </summary>
/// <remarks>
/// <para>
/// Link semantics follow <see cref="TestNetwork"/> and <see cref="LinkMesh"/>: node-to-node links are restricted by the
/// mesh (by creation index: Ring = ring neighbours, Star = everyone to the first node, Line = i-1/i+1); clients
/// (<c>client:{Name}</c>) reach every node unless a fault says otherwise. <see cref="Partition"/> places names in
/// groups (a name listed in no group is alone); <see cref="Cut"/>/<see cref="BlackHole"/> remove one direction;
/// <see cref="Heal"/> removes cuts, partitions, isolation <b>and</b> the mesh restriction (as <c>TestNetwork.Heal</c>
/// documents). A down node (crashed, stopped or paused) neither sends nor receives.
/// </para>
/// <para>Nodes added after construction link to everyone (meshes are never combined with node addition).</para>
/// </remarks>
public sealed class MeshNetworkModel
{
    private readonly List<TestNodeSpec> _nodes = [];
    private readonly Dictionary<string, int> _index = new(StringComparer.Ordinal);
    private readonly HashSet<string> _down = new(StringComparer.Ordinal);
    private readonly HashSet<string> _ineligible = new(StringComparer.Ordinal);
    private readonly HashSet<string> _isolated = new(StringComparer.Ordinal);
    private readonly HashSet<(string From, string To)> _cut = [];
    private Dictionary<string, int>? _partition;
    private int _meshSize;

    /// <summary>Creates the model for the initial node layout.</summary>
    public MeshNetworkModel(IReadOnlyList<TestNodeSpec> nodes, LinkMesh mesh = LinkMesh.Full)
    {
        ArgumentNullException.ThrowIfNull(nodes);
        foreach (TestNodeSpec spec in nodes)
            AddNode(spec);
        Mesh = mesh;
        _meshSize = _nodes.Count;
    }

    /// <summary>The node-to-node link restriction currently in force.</summary>
    public LinkMesh Mesh { get; private set; }

    /// <summary>All nodes ever added, in creation order.</summary>
    public IReadOnlyList<TestNodeSpec> Nodes => _nodes;

    /// <summary>Names of all nodes.</summary>
    public IEnumerable<string> NodeNames => _nodes.Select(n => n.Name);

    /// <summary>Names of nodes that are not down, in creation order.</summary>
    public IReadOnlyList<string> AliveNodes => _nodes.Where(n => !_down.Contains(n.Name)).Select(n => n.Name).ToArray();

    /// <summary>Names of nodes that are down.</summary>
    public IReadOnlyCollection<string> DownNodes => _down;

    /// <summary>True when the mesh is full and no partition, cut or isolation is in force.</summary>
    public bool IsFullyConnected => Mesh == LinkMesh.Full && _partition is null && _cut.Count == 0 && _isolated.Count == 0;

    /// <summary>The specification of a node.</summary>
    public TestNodeSpec Spec(string node) =>
        _index.TryGetValue(node, out int i) ? _nodes[i] : throw new ArgumentException($"Unknown node '{node}'.", nameof(node));

    /// <summary>True when <paramref name="name"/> is a node of the model.</summary>
    public bool IsNode(string name) => _index.ContainsKey(name);

    /// <summary>Adds a node (linked to everyone).</summary>
    public void AddNode(TestNodeSpec spec)
    {
        ArgumentNullException.ThrowIfNull(spec);
        if (!_index.TryAdd(spec.Name, _nodes.Count))
            throw new ArgumentException($"Duplicate node '{spec.Name}'.", nameof(spec));
        _nodes.Add(spec);
    }

    /// <summary>Marks a node down (crashed, stopped, paused) or back up.</summary>
    public void SetDown(string node, bool down)
    {
        _ = Spec(node);
        if (down)
            _down.Add(node);
        else
            _down.Remove(node);
    }

    /// <summary>True when the node is down.</summary>
    public bool IsDown(string node) => _down.Contains(node);

    /// <summary>Marks a node (in)eligible as a data-placement target (draining, drained, removed).</summary>
    public void SetEligible(string node, bool eligible)
    {
        _ = Spec(node);
        if (eligible)
            _ineligible.Remove(node);
        else
            _ineligible.Add(node);
    }

    /// <summary>True when the node may receive new data copies.</summary>
    public bool IsEligible(string node) => !_ineligible.Contains(node);

    /// <summary>Partitions names into groups (replaces any earlier partition).</summary>
    public void Partition(params string[][] groups)
    {
        ArgumentNullException.ThrowIfNull(groups);
        var map = new Dictionary<string, int>(StringComparer.Ordinal);
        for (int g = 0; g < groups.Length; g++)
        {
            foreach (string name in groups[g])
            {
                if (!map.TryAdd(name, g))
                    throw new ArgumentException($"'{name}' appears in more than one partition group.", nameof(groups));
            }
        }

        _partition = map;
    }

    /// <summary>Cuts every link of a name.</summary>
    public void Isolate(string name) => _isolated.Add(name);

    /// <summary>Cuts one direction.</summary>
    public void Cut(string from, string to) => _cut.Add((from, to));

    /// <summary>Cuts both directions.</summary>
    public void CutBoth(string a, string b)
    {
        Cut(a, b);
        Cut(b, a);
    }

    /// <summary>Half-open link: the connection stays open but messages from <paramref name="from"/> vanish (same reachability as a cut).</summary>
    public void BlackHole(string from, string to) => Cut(from, to);

    /// <summary>Restores one direction cut by <see cref="Cut"/>/<see cref="BlackHole"/>.</summary>
    public void Restore(string from, string to) => _cut.Remove((from, to));

    /// <summary>Removes every cut, partition, isolation and the mesh restriction.</summary>
    public void Heal()
    {
        _cut.Clear();
        _isolated.Clear();
        _partition = null;
        Mesh = LinkMesh.Full;
    }

    /// <summary>True when the link mesh permits a link between two nodes (non-nodes are always permitted).</summary>
    public bool MeshAllows(string a, string b)
    {
        if (Mesh == LinkMesh.Full || !_index.TryGetValue(a, out int i) || !_index.TryGetValue(b, out int j) || i == j)
            return true;
        if (i >= _meshSize || j >= _meshSize)
            return true;
        int n = _meshSize;
        return Mesh switch
        {
            LinkMesh.Ring => Math.Abs(i - j) == 1 || (n > 2 && Math.Abs(i - j) == n - 1),
            LinkMesh.Star => i == 0 || j == 0,
            LinkMesh.Line => Math.Abs(i - j) == 1,
            _ => true,
        };
    }

    /// <summary>True when a message from <paramref name="from"/> reaches <paramref name="to"/>.</summary>
    public bool CanSend(string from, string to)
    {
        if (string.Equals(from, to, StringComparison.Ordinal))
            return !_down.Contains(from);
        if (_down.Contains(from) || _down.Contains(to))
            return false;
        if (_isolated.Contains(from) || _isolated.Contains(to))
            return false;
        if (_cut.Contains((from, to)))
            return false;
        if (_partition is not null)
        {
            bool hasFrom = _partition.TryGetValue(from, out int gf);
            bool hasTo = _partition.TryGetValue(to, out int gt);
            if (!hasFrom || !hasTo || gf != gt)
                return false;
        }

        return !IsNode(from) || !IsNode(to) || MeshAllows(from, to);
    }

    /// <summary>True when a request/response exchange works in both directions (what consensus and receipts need).</summary>
    public bool Linked(string a, string b) => CanSend(a, b) && CanSend(b, a);

    /// <summary>A deep copy (for what-if expectations).</summary>
    public MeshNetworkModel Clone()
    {
        var copy = new MeshNetworkModel(_nodes, Mesh) { _meshSize = _meshSize };
        copy._down.UnionWith(_down);
        copy._ineligible.UnionWith(_ineligible);
        copy._isolated.UnionWith(_isolated);
        copy._cut.UnionWith(_cut);
        copy._partition = _partition is null ? null : new Dictionary<string, int>(_partition, StringComparer.Ordinal);
        return copy;
    }

    /// <summary>A one-line description for assertion messages.</summary>
    public string Describe() =>
        $"mesh={Mesh}, down=[{string.Join(",", _down.Order(StringComparer.Ordinal))}], ineligible=[{string.Join(",", _ineligible.Order(StringComparer.Ordinal))}], " +
        $"isolated=[{string.Join(",", _isolated.Order(StringComparer.Ordinal))}], cuts=[{string.Join(",", _cut.Select(c => c.From + ">" + c.To).Order(StringComparer.Ordinal))}], " +
        $"partition={(_partition is null ? "none" : string.Join("|", _partition.GroupBy(kv => kv.Value).OrderBy(g => g.Key).Select(g => string.Join(",", g.Select(kv => kv.Key).Order(StringComparer.Ordinal)))))}";
}
