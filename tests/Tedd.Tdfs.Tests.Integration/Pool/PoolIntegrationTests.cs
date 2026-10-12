using Tedd.Tdfs.Data;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Integration.Pool;

/// <summary>
/// POOL — namespaces and storage pools; integration row over real QUIC transport and disk storage (FSD Addendum A §A2,
/// catalog §A5.2). Invariants: INV-08, INV-11, INV-18.
/// </summary>
public sealed class PoolIntegrationTests
{
    private const string PoolA = "a", PoolB = "b", VolA = "va", VolB = "vb";
    private const int Chunk = 64 * 1024;
    private static readonly TimeSpan Converge = TimeSpan.FromMinutes(2);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>
    /// Six real nodes (QUIC, disk) in two pools of three, one volume per pool (target 3 copies in 3 zones, acknowledge
    /// 2 in 2). Objects are disjoint by inventory before, during and after a crash of one node (in either pool): writes to
    /// both volumes keep succeeding inside their own pools while the node is down (never falling back across pools); after
    /// the restart the node still holds its durable pre-crash objects, holds nothing of the other pool's volume, and
    /// repair restores every object onto exactly its pool's three nodes; all bytes verify.
    /// </summary>
    [Theory]
    [InlineData("n1")]
    [InlineData("n4")]
    [Catalog("POOL-018", "I", "INV-08", "INV-11", "INV-18")]
    public async Task POOL_018_PoolsOverRealTransport(string crashed)
    {
        IReadOnlyList<TestNodeSpec> nodes =
        [
            new("n0", "z1", Pools: [PoolA]), new("n1", "z2", Pools: [PoolA]), new("n2", "z3", Pools: [PoolA]),
            new("n3", "z1", Pools: [PoolB]), new("n4", "z2", Pools: [PoolB]), new("n5", "z3", Pools: [PoolB]),
        ];
        var pools = new Dictionary<string, string[]>(StringComparer.Ordinal)
        {
            [VolA] = nodes.Where(n => n.IsInPool(PoolA)).Select(n => n.Name).ToArray(),
            [VolB] = nodes.Where(n => n.IsInPool(PoolB)).Select(n => n.Name).ToArray(),
        };
        string crashedVolume = pools[VolA].Contains(crashed) ? VolA : VolB;
        string otherVolume = crashedVolume == VolA ? VolB : VolA;
        DfsStoragePolicy policy = new() { TargetCopies = 3, TargetDistinctZones = 3, DurableCopiesBeforeAck = 2, DurableZonesBeforeAck = 2 };
        var options = new TestClusterOptions
        {
            Topology = TestTopology.Custom,
            Nodes = nodes,
            ControlVoters = 3,
            Volumes = [new TestVolumeSpec(VolA) { StoragePool = PoolA, DefaultPolicy = policy }, new TestVolumeSpec(VolB) { StoragePool = PoolB, DefaultPolicy = policy }],
            Mode = ClusterMode.Realtime,
            Transport = TransportKind.Quic,
            Storage = StorageKind.Disk,
            ScenarioTimeout = TimeSpan.FromMinutes(6),
        };

        await ClusterTest.RunAsync(options, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { OperationTimeout = TimeSpan.FromSeconds(20) }, Ct);
            var model = new List<(string Volume, string Path, byte[] Data)>();
            await WriteRoundAsync(0, 6);
            foreach (string volume in pools.Keys)
                await cluster.WaitForRepairAsync(volume, Converge, Ct);
            await AssertDisjointAsync("before the crash", fullTarget: true);

            IReadOnlySet<ObjectId> reachableBefore = (await cluster.Inspect.GetVolumeStateAsync(crashedVolume)).ReachableObjects();
            HashSet<ObjectId> heldBefore = [.. (await cluster.Inspect.GetStoredObjectsOnNodeAsync(crashed, crashedVolume)).Where(reachableBefore.Contains)];
            Assert.NotEmpty(heldBefore);

            await cluster.Node(crashed).CrashAsync();
            await WriteRoundAsync(100, 4);
            await AssertDisjointAsync($"while {crashed} is down", fullTarget: false);
            foreach ((string volume, string path, byte[] data) in model)
            {
                byte[] actual = await client.GetVolume(volume).Files.ReadAllBytesAsync(path, Ct);
                AssertEx.BytesEqual(data, actual, $"{volume}:{path} while {crashed} is down");
            }

            await cluster.Node(crashed).RestartAsync();
            await cluster.WaitForStableAsync(ct: Ct);
            foreach (string volume in pools.Keys)
                await cluster.WaitForRepairAsync(volume, Converge, Ct);

            IReadOnlySet<ObjectId> heldAfter = await cluster.Inspect.GetStoredObjectsOnNodeAsync(crashed, crashedVolume);
            ObjectId[] lost = heldBefore.Where(o => !heldAfter.Contains(o)).ToArray();
            Assert.True(lost.Length == 0, $"{crashed} lost {lost.Length} durable object(s) of {crashedVolume} across the crash");
            Assert.Empty(await cluster.Inspect.GetStoredObjectsOnNodeAsync(crashed, otherVolume));
            await AssertDisjointAsync("after restart and repair", fullTarget: true);
            foreach ((string volume, string path, byte[] data) in model)
            {
                byte[] actual = await client.GetVolume(volume).Files.ReadAllBytesAsync(path, Ct);
                AssertEx.BytesEqual(data, actual, $"{volume}:{path} after recovery");
            }

            async Task WriteRoundAsync(int first, int count)
            {
                await Task.WhenAll(pools.Keys.Select(async volume =>
                {
                    DfsVolume vol = client.GetVolume(volume);
                    for (int i = first; i < first + count; i++)
                    {
                        byte[] data = TestData.Bytes((volume == VolA ? 18_000UL : 19_000UL) + (ulong)i, 3 * Chunk + i);
                        await vol.Files.WriteAllBytesAsync($"/f{i}", data, Ct);
                        lock (model)
                            model.Add((volume, $"/f{i}", data));
                    }
                }));
            }

            async Task AssertDisjointAsync(string when, bool fullTarget)
            {
                IReadOnlyDictionary<string, DfsTopologyLabels> labels =
                    (await cluster.Admin.ListNodesAsync(Ct)).ToDictionary(n => n.Name, n => n.Labels, StringComparer.Ordinal);
                foreach ((string volume, string[] pool) in pools)
                {
                    foreach (TestNode node in cluster.Nodes)
                    {
                        if (pool.Contains(node.Name) || node.State != TestNodeState.Running)
                            continue;
                        IReadOnlySet<ObjectId> stray = await cluster.Inspect.GetStoredObjectsOnNodeAsync(node.Name, volume);
                        Assert.True(stray.Count == 0, $"{when}: {node.Name} (outside the pool of {volume}) stores {stray.Count} of its objects");
                    }

                    VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(volume);
                    foreach (ObjectId o in state.ReachableObjects())
                    {
                        IReadOnlyList<string> holders = await cluster.Inspect.GetObjectHoldersAsync(o);
                        Assert.True(holders.All(h => pool.Contains(h)), $"{when}: object {o} of {volume} held on [{string.Join(",", holders)}]");
                        (int copies, int zones) = Placement.CountDomains(holders.Select(h => labels[h]));
                        if (fullTarget)
                            Assert.True(holders.Order(StringComparer.Ordinal).SequenceEqual(pool), $"{when}: object {o} of {volume} must be on all of [{string.Join(",", pool)}], is on [{string.Join(",", holders)}]");
                        else
                            Assert.True(copies >= 2 && zones >= 2, $"{when}: object {o} of {volume} has {copies} copies in {zones} zones");
                    }
                }
            }
        }, Ct);
    }
}
