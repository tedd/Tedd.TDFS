using Tedd.Tdfs.Data;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Simulation.Pool;

/// <summary>
/// Shared layouts, placement oracles and inventory assertions for the POOL suite (deterministic simulation level).
/// Placement is always judged from durable state (node inventories and verified holders), never from client views.
/// </summary>
internal static class PoolHelpers
{
    public const string PoolA = "a";
    public const string PoolB = "b";
    public const string VolA = "va";
    public const string VolB = "vb";

    /// <summary>Logical update unit under <see cref="NodeTimings.Fast"/> (64 KiB); payloads of two chunks or more use extents.</summary>
    public const int Chunk = 64 * 1024;

    /// <summary>Bound for repair/migration convergence (virtual time; repair and inventory run every 250 ms under Fast timings).</summary>
    public static readonly TimeSpan Converge = TimeSpan.FromSeconds(60);

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

    /// <summary>Six nodes: n0..n2 in pool <c>a</c> (zones z1..z3) and n3..n5 in pool <c>b</c> (zones z1..z3).</summary>
    public static IReadOnlyList<TestNodeSpec> TwoPools() =>
    [
        Node("n0", "z1", PoolA), Node("n1", "z2", PoolA), Node("n2", "z3", PoolA),
        Node("n3", "z1", PoolB), Node("n4", "z2", PoolB), Node("n5", "z3", PoolB),
    ];

    /// <summary>A volume bound to <paramref name="pool"/>; voters are chosen from the pool unless explicit voters are given.</summary>
    public static TestVolumeSpec Volume(string name, string pool, DfsStoragePolicy? policy = null, params string[] explicitVoters) => new(name)
    {
        StoragePool = pool,
        DefaultPolicy = policy ?? ThreeZones,
        Metadata = explicitVoters.Length == 0
            ? new DfsMetadataPolicy()
            : new DfsMetadataPolicy { Voters = explicitVoters.Length, Placement = DfsVoterPlacement.Explicit, ExplicitVoterNodes = explicitVoters },
    };

    public static TestClusterOptions Deterministic(IReadOnlyList<TestNodeSpec> nodes, int controlVoters, params TestVolumeSpec[] volumes) => new()
    {
        Mode = ClusterMode.Deterministic,
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

    public static async Task<List<ObjectId>> ObjectsOfAllAsync(TestCluster cluster, string volume, IEnumerable<string> paths)
    {
        var all = new List<ObjectId>();
        foreach (string path in paths)
            all.AddRange(await ObjectsOfAsync(cluster, volume, path));
        return all;
    }

    /// <summary>One node's object-store inventory (optionally restricted to one volume's records).</summary>
    public static async Task<HashSet<ObjectId>> InventoryAsync(TestCluster cluster, string node, string? volume = null) =>
        [.. await cluster.Inspect.GetStoredObjectsOnNodeAsync(node, volume)];

    /// <summary>Objects referenced by the volume's current root, snapshots, transactions and pins (leader view).</summary>
    public static async Task<IReadOnlySet<ObjectId>> ReachableAsync(TestCluster cluster, string volume) =>
        (await cluster.Inspect.GetVolumeStateAsync(volume)).ReachableObjects();

    /// <summary>Authoritative topology labels by node name (cluster map).</summary>
    public static async Task<IReadOnlyDictionary<string, DfsTopologyLabels>> LabelsAsync(TestCluster cluster) =>
        (await cluster.Admin.ListNodesAsync(Ct)).ToDictionary(n => n.Name, n => n.Labels, StringComparer.Ordinal);

    public static async Task<HashSet<string>> HoldersAsync(TestCluster cluster, ObjectId id) =>
        new(await cluster.Inspect.GetObjectHoldersAsync(id), StringComparer.Ordinal);

    public static (int Copies, int Zones) Domains(IEnumerable<string> holders, IReadOnlyDictionary<string, DfsTopologyLabels> labels) =>
        Placement.CountDomains(holders.Select(h => labels[h]));

    public static string Show(IEnumerable<string> names) => "[" + string.Join(",", names.Order(StringComparer.Ordinal)) + "]";

    /// <summary>
    /// Every object is durably held only by nodes of <paramref name="pool"/>, on at least <paramref name="copies"/> distinct
    /// hosts in at least <paramref name="zones"/> distinct zones of that pool.
    /// </summary>
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

    /// <summary>
    /// No running node outside <paramref name="allowed"/> stores any object (staged or committed) of
    /// <paramref name="volume"/>, read from each node's own object-store inventory.
    /// </summary>
    public static async Task AssertVolumeConfinedAsync(TestCluster cluster, string volume, IReadOnlyCollection<string> allowed, string because)
    {
        foreach (TestNode node in cluster.Nodes)
        {
            if (allowed.Contains(node.Name, StringComparer.Ordinal) || node.State != TestNodeState.Running)
                continue;
            IReadOnlySet<ObjectId> stray = await cluster.Inspect.GetStoredObjectsOnNodeAsync(node.Name, volume);
            Assert.True(stray.Count == 0,
                $"{because}: node {node.Name} (outside {Show(allowed)}) stores {stray.Count} object(s) of volume '{volume}', e.g. {stray.FirstOrDefault()}");
        }
    }

    /// <summary>The nodes of <paramref name="pool"/> as placement candidates (ids and labels from the cluster map).</summary>
    public static async Task<IReadOnlyList<PlacementNode>> PlacementNodesAsync(TestCluster cluster, IReadOnlyCollection<string> pool) =>
        (await cluster.Admin.ListNodesAsync(Ct))
            .Where(n => pool.Contains(n.Name, StringComparer.Ordinal))
            .Select(n => new PlacementNode(n.Id, n.Name, n.Labels, Eligible: true, FreeBytes: Math.Max(1, n.CapacityBytes - n.UsedBytes)))
            .ToList();

    /// <summary>Deterministic HRW targets (ARCHITECTURE §6 / API C.2) of an object among the pool's nodes at an epoch.</summary>
    public static HashSet<string> Targets(ObjectId id, ulong epoch, DfsStoragePolicy policy, IReadOnlyList<PlacementNode> poolNodes) =>
        new(Placement.SelectTargets(id, epoch, policy, poolNodes).Select(n => n.Name), StringComparer.Ordinal);

    public static async Task<ulong> PlacementEpochAsync(TestCluster cluster) =>
        (await cluster.Admin.GetClusterStatusAsync(Ct)).PlacementEpoch;

    public static async Task<DfsPoolInfo> PoolAsync(TestCluster cluster, string pool)
    {
        IReadOnlyList<DfsPoolInfo> pools = await cluster.Admin.ListPoolsAsync(Ct);
        DfsPoolInfo? info = pools.SingleOrDefault(p => string.Equals(p.Name, pool, StringComparison.Ordinal));
        Assert.True(info is not null, $"pool '{pool}' is not listed (pools: {Show(pools.Select(p => p.Name))})");
        return info;
    }

    /// <summary>Writes <paramref name="data"/> through a stream (OnFlush) and publishes it with an explicit flush.</summary>
    public static async Task WriteStreamAsync(DfsVolume vol, string path, byte[] data, FileMode mode = FileMode.Create, string? policy = null, long position = 0)
    {
        DfsStream stream = await vol.Files.OpenAsync(path,
            new DfsOpenOptions { Mode = mode, Access = FileAccess.Write, Share = FileShare.Read, StoragePolicy = policy }, Ct);
        try
        {
            stream.Position = position;
            await stream.WriteAsync(data, Ct);
            await stream.FlushAsync(Ct);
        }
        catch
        {
            await DiscardAsync(stream);
            throw;
        }

        await stream.DisposeAsync();
    }

    /// <summary>Aborts and disposes a stream whose publication failed, without letting cleanup errors mask the outcome.</summary>
    public static async Task DiscardAsync(DfsStream stream)
    {
        try
        {
            await stream.AbortAsync(Ct);
        }
        catch (Exception ex) when (ex is IOException or InvalidOperationException or ObjectDisposedException or TimeoutException)
        {
            Log($"abort after a failed publication reported {ex.GetType().Name}: {ex.Message}");
        }

        try
        {
            await stream.DisposeAsync();
        }
        catch (Exception ex) when (ex is IOException or InvalidOperationException or TimeoutException)
        {
            Log($"dispose after a failed publication reported {ex.GetType().Name}: {ex.Message}");
        }
    }

    /// <summary>
    /// Runs a write that must fail because the pool cannot provide the acknowledgement copies. A placement failure must
    /// report a real deficit no larger than what the pool can provide; a silent (unreachable) target may instead surface as
    /// a timeout-class failure. Success — which would mean copies were counted outside the pool — fails the test.
    /// </summary>
    public static async Task AssertNoPlacementAsync(Func<Task> action, int maxAchievedCopies, bool silentTargets, string what)
    {
        try
        {
            await action();
        }
        catch (DfsPlacementUnavailableException ex)
        {
            Assert.Equal(DfsErrorCode.PlacementUnavailable, ex.ErrorCode);
            Assert.True(ex.IsRetryable, $"{what}: placement unavailability is retryable");
            Assert.True(ex.AchievedCopies <= maxAchievedCopies,
                $"{what}: reported {ex.AchievedCopies} achieved copies but the pool can provide at most {maxAchievedCopies}");
            Assert.True(ex.AchievedCopies < ex.RequiredCopies || ex.AchievedZones < ex.RequiredZones,
                $"{what}: a placement failure must report a deficit (achieved {ex.AchievedCopies}/{ex.AchievedZones}, required {ex.RequiredCopies}/{ex.RequiredZones})");
            return;
        }
        catch (Exception ex) when (silentTargets && ex is TimeoutException or DfsException { ErrorCode: DfsErrorCode.Timeout or DfsErrorCode.NodeUnavailable })
        {
            Log($"{what}: unreachable pool reported as {ex.GetType().Name} ({(ex as DfsException)?.ErrorCode})");
            return;
        }

        Assert.Fail($"{what}: succeeded although the pool could not provide the acknowledgement copies (outside nodes must never be used)");
    }

    public static void Same(byte[] expected, byte[] actual, string? because = null) => AssertEx.BytesEqual(expected, actual, because);

    public static void Log(string message) => TestContext.Current.TestOutputHelper?.WriteLine(message);
}

/// <summary>
/// A per-volume mixed write load with an independent byte model: WriteAll (multi-object), streamed writes with
/// intermediate flushes, appends, partial overwrites, copies and deletes — every publication path that creates objects.
/// </summary>
internal sealed class VolumeLoad(DfsVolume volume, ulong seed)
{
    public DfsVolume Volume { get; } = volume;

    public Dictionary<string, byte[]> Model { get; } = new(StringComparer.Ordinal);

    public HashSet<string> Deleted { get; } = new(StringComparer.Ordinal);

    public async Task RunRoundAsync(int round)
    {
        const int chunk = PoolHelpers.Chunk;
        CancellationToken ct = PoolHelpers.Ct;
        ulong s = seed + (ulong)round * 100;

        byte[] whole = TestData.Bytes(s + 1, 2 * chunk + 17 * round + 1);
        await Volume.Files.WriteAllBytesAsync($"/w{round}", whole, ct);
        Model[$"/w{round}"] = whole;

        byte[] streamed = TestData.Bytes(s + 2, 3 * chunk);
        await using (DfsStream stream = await Volume.Files.OpenAsync($"/s{round}",
                         new DfsOpenOptions { Mode = FileMode.Create, Access = FileAccess.Write, Share = FileShare.Read }, ct))
        {
            await stream.WriteAsync(streamed.AsMemory(0, chunk + 5), ct);
            await stream.FlushAsync(ct);
            await stream.WriteAsync(streamed.AsMemory(chunk + 5), ct);
            await stream.FlushAsync(ct);
        }

        Model[$"/s{round}"] = streamed;

        byte[] tail = TestData.Bytes(s + 3, chunk + round + 1);
        await Volume.Files.AppendAllBytesAsync("/log", tail, ct);
        Model["/log"] = Model.TryGetValue("/log", out byte[]? log) ? log.Concat(tail).ToArray() : tail;

        if (round > 0)
        {
            const int offset = chunk / 2;
            byte[] patch = TestData.Bytes(s + 4, 1000);
            await PoolHelpers.WriteStreamAsync(Volume, "/w0", patch, FileMode.Open, position: offset);
            patch.CopyTo(Model["/w0"], offset);
        }

        await Volume.Files.CopyAsync($"/w{round}", $"/c{round}", false, ct);
        Model[$"/c{round}"] = (byte[])whole.Clone();
        if (round > 0)
        {
            await Volume.Files.DeleteAsync($"/c{round - 1}", ct);
            Model.Remove($"/c{round - 1}");
            Deleted.Add($"/c{round - 1}");
        }
    }

    public async Task VerifyAsync(string because)
    {
        foreach ((string path, byte[] expected) in Model)
            PoolHelpers.Same(expected, await Volume.Files.ReadAllBytesAsync(path, PoolHelpers.Ct), $"{because}: {Volume.Name}:{path}");
        foreach (string path in Deleted)
            Assert.False(await Volume.Files.ExistsAsync(path, PoolHelpers.Ct), $"{because}: {Volume.Name}:{path} was deleted");
    }
}
