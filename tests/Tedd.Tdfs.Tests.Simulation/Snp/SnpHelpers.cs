using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Testing.SnpApp;

namespace Tedd.Tdfs.Tests.Simulation.Snp;

/// <summary>Shared helpers of the SNP simulation tests (deterministic clusters, object reachability and GC probes).</summary>
internal static class SnpHelpers
{
    public const string Volume = "vol";

    /// <summary>Minimum durable copies of the default ThreeZones policy (ack 2 in 2 zones).</summary>
    public const int AckCopies = 2;

    public static readonly NodeTimings Timings = NodeTimings.Fast;
    public static readonly int Chunk = NodeTimings.Fast.ChunkSize;
    public static readonly TimeSpan GcInterval = NodeTimings.Fast.GcInterval;

    /// <summary>A deterministic cluster: 3 nodes one per zone by default; 5+ nodes use 5 control and 5 volume voters.</summary>
    public static TestClusterOptions Deterministic(int nodeCount = 3, ulong seed = 0, int backups = 0, IReadOnlyList<TestVolumeSpec>? volumes = null)
    {
        TestClusterOptions options = TestClusterOptions.ThreeZones with
        {
            Mode = ClusterMode.Deterministic,
            Seed = seed,
            BackupNodeCount = backups,
            NodeCount = nodeCount,
            ControlVoters = nodeCount >= 5 ? 5 : Math.Min(3, nodeCount),
        };
        int voters = nodeCount >= 5 ? 5 : Math.Min(3, nodeCount);
        IReadOnlyList<TestVolumeSpec> specs = volumes ?? [new TestVolumeSpec(Volume)];
        return options with
        {
            Volumes = specs.Select(v => v with { Metadata = v.Metadata with { Voters = voters } }).ToArray(),
        };
    }

    /// <summary>Distinct objects referenced by the current version of a file (leader view); the file must use extents.</summary>
    public static async Task<IReadOnlyList<ObjectId>> ObjectsOfAsync(TestCluster cluster, string path, string volume = Volume)
    {
        FileInspection? file = await cluster.Inspect.GetFileAsync(volume, path);
        Assert.True(file is not null, $"'{path}' must exist on the leader");
        Assert.False(file!.IsInline, $"'{path}' must be stored in extents for this test");
        ObjectId[] ids = file.Extents.Select(e => e.Object).Distinct().ToArray();
        Assert.NotEmpty(ids);
        return ids;
    }

    /// <summary>Lets several GC and inventory intervals pass (virtual time in deterministic mode).</summary>
    public static Task RunGcPassesAsync(TestCluster cluster, int passes = 8, CancellationToken ct = default) =>
        cluster.DelayAsync((GcInterval * passes) + (Timings.InventoryInterval * 2), ct);

    /// <summary>Fails unless every object still has at least <paramref name="minHolders"/> verified durable holders.</summary>
    public static async Task AssertRetainedAsync(TestCluster cluster, IEnumerable<ObjectId> ids, string because, int minHolders = AckCopies)
    {
        foreach (ObjectId id in ids)
        {
            IReadOnlyList<string> holders = await cluster.Inspect.GetObjectHoldersAsync(id);
            Assert.True(holders.Count >= minHolders,
                $"object {id} has {holders.Count} verified holder(s) [{string.Join(", ", holders)}], expected at least {minHolders}: {because}");
        }
    }

    /// <summary>
    /// Safety during the window: polls every GC interval for <paramref name="duration"/> and fails as soon as any object
    /// drops below <paramref name="minHolders"/> holders (checked while GC runs, not only afterwards).
    /// </summary>
    public static async Task WatchRetainedAsync(TestCluster cluster, IReadOnlyCollection<ObjectId> ids, TimeSpan duration, string because,
        int minHolders = AckCopies, CancellationToken ct = default)
    {
        long start = cluster.Time.GetTimestamp();
        do
        {
            await AssertRetainedAsync(cluster, ids, because, minHolders);
            await cluster.DelayAsync(GcInterval, ct);
        }
        while (cluster.Time.GetElapsedTime(start) < duration);
        await AssertRetainedAsync(cluster, ids, because, minHolders);
    }

    /// <summary>Waits (bounded) until none of the objects is stored on any node any more (positive GC control).</summary>
    public static Task WaitForReclaimAsync(TestCluster cluster, IReadOnlyCollection<ObjectId> ids, string because, TimeSpan? timeout = null,
        CancellationToken ct = default) =>
        cluster.WaitUntilAsync(async () =>
        {
            IReadOnlySet<ObjectId> stored = await cluster.Inspect.GetAllStoredObjectsAsync();
            return ids.All(id => !stored.Contains(id));
        }, timeout ?? TimeSpan.FromSeconds(60), because, ct);

    /// <summary>Opens a snapshot view, captures the tree below <paramref name="root"/> and closes the view.</summary>
    public static async Task<SortedDictionary<string, byte[]?>> CaptureSnapshotAsync(DfsVolume volume, string snapshot, string root,
        CancellationToken ct)
    {
        await using DfsSnapshotView view = await volume.Snapshots.OpenAsync(snapshot, ct);
        Assert.Equal(snapshot, view.Info.Name);
        return await SnapshotTree.CaptureAsync(view, root, ct);
    }

    /// <summary>Asserts two snapshot descriptions are identical (adapter records and retention compared by value).</summary>
    public static void AssertSnapshotInfoEqual(DfsSnapshotInfo expected, DfsSnapshotInfo actual, string because)
    {
        Assert.True(expected.Id == actual.Id, $"{because}: id {actual.Id.Value} != {expected.Id.Value}");
        Assert.Equal(expected.Name, actual.Name);
        Assert.Equal(expected.Volume, actual.Volume);
        Assert.True(expected.Boundary == actual.Boundary, $"{because}: boundary {actual.Boundary} != {expected.Boundary}");
        Assert.True(expected.Label == actual.Label, $"{because}: label {actual.Label} != {expected.Label}");
        Assert.True(expected.CreatedUtc == actual.CreatedUtc, $"{because}: created {actual.CreatedUtc:O} != {expected.CreatedUtc:O}");
        Assert.True(expected.Retention == actual.Retention, $"{because}: retention {actual.Retention} != {expected.Retention}");
        AssertAdapterRecordsEqual(expected.Adapters, actual.Adapters, because);
    }

    /// <summary>Asserts adapter records are equal by id, version and restore requirements (order-insensitive by id).</summary>
    public static void AssertAdapterRecordsEqual(IReadOnlyList<DfsAdapterRecord> expected, IReadOnlyList<DfsAdapterRecord> actual, string because)
    {
        Assert.True(expected.Count == actual.Count, $"{because}: {actual.Count} adapter records, expected {expected.Count}");
        foreach (DfsAdapterRecord e in expected)
        {
            DfsAdapterRecord? a = actual.FirstOrDefault(r => string.Equals(r.Id, e.Id, StringComparison.Ordinal));
            Assert.True(a is not null, $"{because}: adapter record '{e.Id}' missing");
            Assert.Equal(e.Version, a!.Version);
            Assert.Equal(
                e.RestoreRequirements.OrderBy(p => p.Key, StringComparer.Ordinal).ToArray(),
                a.RestoreRequirements.OrderBy(p => p.Key, StringComparer.Ordinal).ToArray());
        }
    }

    /// <summary>Awaits <paramref name="actual"/> and asserts byte equality with <paramref name="expected"/>.</summary>
    public static async Task AssertBytesAsync(byte[] expected, Task<byte[]> actual, string because)
    {
        byte[] bytes = await actual;
        AssertEx.BytesEqual(expected, bytes, because);
    }

    /// <summary>Runs <paramref name="action"/> and returns its exception, or null.</summary>
    public static async Task<Exception?> CaptureAsync(Func<Task> action)
    {
        try
        {
            await action();
            return null;
        }
        catch (Exception ex)
        {
            return ex;
        }
    }

    /// <summary>Applies a tree mutation to a modelled tree: sets a file (null removes the path and its subtree).</summary>
    public static void SetPath(SortedDictionary<string, byte[]?> tree, string path, byte[]? fileContent, bool isDirectory = false)
    {
        if (fileContent is null && !isDirectory)
        {
            foreach (string key in tree.Keys.Where(k => k == path || k.StartsWith(path + "/", StringComparison.Ordinal)).ToArray())
                tree.Remove(key);
            return;
        }

        tree[path] = isDirectory ? null : fileContent;
    }

    /// <summary>Renames a path and its subtree in a modelled tree.</summary>
    public static void MovePath(SortedDictionary<string, byte[]?> tree, string from, string to)
    {
        foreach (string key in tree.Keys.Where(k => k == from || k.StartsWith(from + "/", StringComparison.Ordinal)).ToArray())
        {
            byte[]? value = tree[key];
            tree.Remove(key);
            tree[to + key[from.Length..]] = value;
        }
    }

    /// <summary>Concatenates byte arrays.</summary>
    public static byte[] Concat(params byte[][] parts)
    {
        var result = new byte[parts.Sum(p => p.Length)];
        int offset = 0;
        foreach (byte[] part in parts)
        {
            part.CopyTo(result, offset);
            offset += part.Length;
        }

        return result;
    }
}
