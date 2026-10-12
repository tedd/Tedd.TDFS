using System.Globalization;
using Tedd.Tdfs.Data;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Unit.Pool;

/// <summary>Shared layouts, oracles and state fingerprints for the POOL suite (unit level: in-memory realtime clusters).</summary>
internal static class PoolHelpers
{
    public const string PoolA = "a";
    public const string PoolB = "b";
    public const string VolA = "va";
    public const string VolB = "vb";

    /// <summary>Logical update unit under <see cref="NodeTimings.Fast"/> (64 KiB); payloads of two chunks or more use extents.</summary>
    public const int Chunk = 64 * 1024;

    /// <summary>Bound for inventory/repair convergence (wall clock; inventory and repair run every 250 ms under Fast timings).</summary>
    public static readonly TimeSpan Converge = TimeSpan.FromSeconds(30);

    public static CancellationToken Ct => TestContext.Current.CancellationToken;

    public static DfsStoragePolicy Policy(int copies, int zones, int ackCopies, int ackZones) => new()
    {
        TargetCopies = copies,
        TargetDistinctZones = zones,
        DurableCopiesBeforeAck = ackCopies,
        DurableZonesBeforeAck = ackZones,
    };

    /// <summary>Target three copies in three zones, acknowledge two copies in two zones.</summary>
    public static DfsStoragePolicy ThreeZones { get; } = Policy(3, 3, 2, 2);

    /// <summary>A node (its own host) belonging to <paramref name="pools"/> besides <c>default</c>.</summary>
    public static TestNodeSpec Node(string name, string zone, params string[] pools) =>
        new(name, zone, Pools: pools.Length == 0 ? null : pools);

    /// <summary>A node with an explicit host label (co-located nodes share one).</summary>
    public static TestNodeSpec NodeOnHost(string name, string zone, string host, params string[] pools) =>
        new(name, zone, host, Pools: pools.Length == 0 ? null : pools);

    /// <summary>Six nodes: n0..n2 in pool <c>a</c> (zones z1..z3) and n3..n5 in pool <c>b</c> (zones z1..z3).</summary>
    public static IReadOnlyList<TestNodeSpec> TwoPools() =>
    [
        Node("n0", "z1", PoolA), Node("n1", "z2", PoolA), Node("n2", "z3", PoolA),
        Node("n3", "z1", PoolB), Node("n4", "z2", PoolB), Node("n5", "z3", PoolB),
    ];

    public static TestVolumeSpec Volume(string name, string pool, DfsStoragePolicy? policy = null) => new(name)
    {
        StoragePool = pool,
        DefaultPolicy = policy ?? ThreeZones,
    };

    public static TestClusterOptions Realtime(IReadOnlyList<TestNodeSpec> nodes, int controlVoters, params TestVolumeSpec[] volumes) => new()
    {
        Topology = TestTopology.Custom,
        Nodes = nodes,
        ControlVoters = controlVoters,
        Volumes = volumes,
    };

    /// <summary>Ordinal-sorted member names of <paramref name="pool"/> in a node layout (the independent membership oracle).</summary>
    public static string[] Members(IReadOnlyList<TestNodeSpec> nodes, string pool) =>
        nodes.Where(n => n.IsInPool(pool)).Select(n => n.Name).Order(StringComparer.Ordinal).ToArray();

    public static async Task<IReadOnlyList<ObjectId>> ObjectsOfAsync(TestCluster cluster, string volume, string path)
    {
        FileInspection? file = await cluster.Inspect.GetFileAsync(volume, path);
        Assert.NotNull(file);
        Assert.False(file.IsInline, $"{volume}:{path} must be stored in extents for placement accounting");
        Assert.NotEmpty(file.Extents);
        return file.Extents.Select(e => e.Object).Distinct().ToList();
    }

    public static async Task<IReadOnlySet<ObjectId>> ReachableAsync(TestCluster cluster, string volume) =>
        (await cluster.Inspect.GetVolumeStateAsync(volume)).ReachableObjects();

    public static async Task<IReadOnlyDictionary<string, DfsTopologyLabels>> LabelsAsync(TestCluster cluster) =>
        (await cluster.Admin.ListNodesAsync(Ct)).ToDictionary(n => n.Name, n => n.Labels, StringComparer.Ordinal);

    public static async Task<HashSet<string>> HoldersAsync(TestCluster cluster, ObjectId id) =>
        new(await cluster.Inspect.GetObjectHoldersAsync(id), StringComparer.Ordinal);

    public static (int Copies, int Zones) Domains(IEnumerable<string> holders, IReadOnlyDictionary<string, DfsTopologyLabels> labels) =>
        Placement.CountDomains(holders.Select(h => labels[h]));

    public static string Show(IEnumerable<string> names) => "[" + string.Join(",", names.Order(StringComparer.Ordinal)) + "]";

    /// <summary>Every object is held only inside <paramref name="pool"/>, on at least the given distinct hosts/zones.</summary>
    public static async Task AssertPlacedWithinAsync(TestCluster cluster, IEnumerable<ObjectId> objects, IReadOnlyCollection<string> pool,
        int copies, int zones, string because)
    {
        IReadOnlyDictionary<string, DfsTopologyLabels> labels = await LabelsAsync(cluster);
        foreach (ObjectId o in objects)
        {
            HashSet<string> holders = await HoldersAsync(cluster, o);
            string[] outside = holders.Where(h => !pool.Contains(h, StringComparer.Ordinal)).ToArray();
            Assert.True(outside.Length == 0, $"{because}: object {o} is held outside pool {Show(pool)} on {Show(outside)}");
            (int c, int z) = Domains(holders, labels);
            Assert.True(c >= copies && z >= zones,
                $"{because}: object {o} has {c} copies in {z} zones on {Show(holders)}; required {copies}/{zones} inside {Show(pool)}");
        }
    }

    public static async Task<ulong> PlacementEpochAsync(TestCluster cluster) =>
        (await cluster.Admin.GetClusterStatusAsync(Ct)).PlacementEpoch;

    public static async Task<DfsPoolInfo> PoolAsync(TestCluster cluster, string pool)
    {
        IReadOnlyList<DfsPoolInfo> pools = await cluster.Admin.ListPoolsAsync(Ct);
        DfsPoolInfo? info = pools.SingleOrDefault(p => string.Equals(p.Name, pool, StringComparison.Ordinal));
        Assert.True(info is not null, $"pool '{pool}' is not listed (pools: {Show(pools.Select(p => p.Name))})");
        return info;
    }

    /// <summary>A canonical text fingerprint of the pool table (names, members, volumes) for "nothing changed" checks.</summary>
    public static string DescribePools(IReadOnlyList<DfsPoolInfo> pools) =>
        string.Join(";", pools.Select(p => $"{p.Name}:{Show(p.Nodes)}:{Show(p.Volumes)}"));

    /// <summary>A canonical text fingerprint of the group table.</summary>
    public static string DescribeGroups(IReadOnlyList<DfsGroupInfo> groups) =>
        string.Join(";", groups.Select(g => $"{g.Name}:{Show(g.Members)}"));

    public static string Describe(Exception? ex) =>
        ex is null
            ? "no exception"
            : $"{ex.GetType().Name}{(ex is DfsException d ? string.Create(CultureInfo.InvariantCulture, $" ({d.ErrorCode})") : string.Empty)}: {ex.Message}";

    public static async Task<string[]> NamesAsync(DfsDirectoryClient directories, string path)
    {
        var names = new List<string>();
        await foreach (DfsEntryInfo entry in directories.EnumerateAsync(path, null, Ct))
            names.Add(entry.Name);
        return names.Order(StringComparer.Ordinal).ToArray();
    }

    /// <summary>All names of a directory, collected page by page through continuation tokens.</summary>
    public static async Task<string[]> PagedNamesAsync(DfsDirectoryClient directories, string path, int pageSize)
    {
        var names = new List<string>();
        var options = new DfsEnumerationOptions { PageSize = pageSize };
        string? token = null;
        for (int pages = 0; pages < 1000; pages++)
        {
            DfsDirectoryPage page = await directories.ListPageAsync(path, options, token, Ct);
            Assert.True(page.Entries.Count <= pageSize, $"page of {page.Entries.Count} entries exceeds the page size {pageSize}");
            names.AddRange(page.Entries.Select(e => e.Name));
            token = page.ContinuationToken;
            if (token is null)
                return names.Order(StringComparer.Ordinal).ToArray();
        }

        Assert.Fail($"paging {path} did not terminate");
        return [];
    }

    public static void Same(byte[] expected, byte[] actual, string? because = null) => AssertEx.BytesEqual(expected, actual, because);

    public static void Log(string message) => TestContext.Current.TestOutputHelper?.WriteLine(message);
}
