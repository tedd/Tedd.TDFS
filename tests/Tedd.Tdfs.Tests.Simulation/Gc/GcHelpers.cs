using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Simulation.Gc;

/// <summary>Shared GC helpers (internal static per test conventions).</summary>
internal static class GcHelpers
{
    public const string Volume = "vol";

    public static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Deterministic simulation cluster (virtual time, simulated disks, in-memory network).</summary>
    public static TestClusterOptions Det(int nodes = 3) => new()
    {
        NodeCount = nodes,
        ControlVoters = Math.Min(3, nodes),
        Mode = ClusterMode.Deterministic,
        ScenarioTimeout = TimeSpan.FromMinutes(15),
    };

    public static int Chunk(TestCluster cluster) => cluster.Options.Timings.ChunkSize;

    /// <summary>Durable copies the default policy requires before acknowledging (the floor GC must never cross).</summary>
    public static int AckCopies(TestCluster cluster) => cluster.Options.GetEffectiveDefaultPolicy().DurableCopiesBeforeAck;

    public static void AssertBytes(byte[] expected, byte[]? actual, string because) => AssertEx.BytesEqual(expected, actual, because);

    public static async Task<HashSet<ObjectId>> StoredAsync(TestCluster cluster, string volume = Volume) =>
        [.. await cluster.Inspect.GetAllStoredObjectsAsync(volume)];

    public static async Task<HashSet<ObjectId>> ReachableAsync(TestCluster cluster, string volume = Volume) =>
        [.. (await cluster.Inspect.GetVolumeStateAsync(volume)).ReachableObjects()];

    public static async Task<HashSet<ObjectId>> FileObjectsAsync(TestCluster cluster, string path, string volume = Volume)
    {
        FileInspection? file = await cluster.Inspect.GetFileAsync(volume, path);
        Assert.True(file is not null, $"'{path}' is not present in the leader's applied state");
        return [.. file!.Extents.Select(e => e.Object)];
    }

    /// <summary>Every id has at least <paramref name="minHolders"/> nodes durably holding a verified copy.</summary>
    public static async Task AssertHeldAsync(TestCluster cluster, IEnumerable<ObjectId> ids, int minHolders, string because)
    {
        foreach (ObjectId id in ids)
        {
            IReadOnlyList<string> holders = await cluster.Inspect.GetObjectHoldersAsync(id);
            Assert.True(holders.Count >= minHolders,
                $"{because}: object {id} has {holders.Count} verified holder(s) [{string.Join(",", holders)}], at least {minHolders} required (INV-09).");
        }
    }

    /// <summary>
    /// Every object reachable from an authoritative root (current, snapshots, pins, transactions) is still held by at least
    /// <paramref name="minHolders"/> nodes (default: the acknowledgement copy count). Safe to call while writers run: an
    /// object found under-held is re-checked against a fresh reachable set taken after its holders were counted. Reachability
    /// is lost only once (an unreachable object id is never referenced again), so an object still reachable then was
    /// reachable throughout and its missing copies are a violation, whereas one superseded meanwhile may legitimately have
    /// been collected.
    /// </summary>
    public static async Task AssertReachableHeldAsync(TestCluster cluster, string because, string volume = Volume, int? minHolders = null)
    {
        int min = minHolders ?? AckCopies(cluster);
        foreach (ObjectId id in await ReachableAsync(cluster, volume))
        {
            IReadOnlyList<string> holders = await cluster.Inspect.GetObjectHoldersAsync(id);
            if (holders.Count >= min || !(await ReachableAsync(cluster, volume)).Contains(id))
                continue;
            Assert.Fail($"{because}: reachable object {id} has {holders.Count} verified holder(s) [{string.Join(",", holders)}], at least {min} required (INV-09).");
        }
    }

    /// <summary>
    /// True for an admission refusal caused by full storage: the typed capacity error, or the placement error it turns
    /// into when too few placement targets can accept the upload.
    /// </summary>
    public static bool IsCapacityRefusal(Exception ex) =>
        ex is DfsCapacityException { ErrorCode: DfsErrorCode.CapacityExhausted } or DfsPlacementUnavailableException;

    /// <summary>Lets several GC/inventory passes run (virtual time in deterministic mode).</summary>
    public static Task QuiesceGcAsync(TestCluster cluster, int passes = 12) =>
        cluster.DelayAsync((cluster.Options.Timings.GcInterval * passes) + (cluster.Options.Timings.InventoryInterval * 4), Ct);

    /// <summary>Maintenance budget used for "eventually reclaimed" waits.</summary>
    public static TimeSpan ReclaimBudget(TestCluster cluster) =>
        (cluster.Options.Timings.GcInterval * 40) + (cluster.Options.Timings.InventoryInterval * 10) + cluster.Options.Timings.PinDuration
        + cluster.Options.Timings.SessionTimeout;

    public static void JumpWallClocks(TestCluster cluster, TimeSpan delta)
    {
        foreach (TestNode node in cluster.Nodes)
        {
            if (node.State == TestNodeState.Running)
                node.Clock.JumpWall(delta);
        }
    }

    public static async Task<long> GcDeletedAsync(TestCluster cluster) => (await cluster.Inspect.GetTotalCountersAsync()).GcDeletedObjects;

    public static int SweepHits(TestCluster cluster) => cluster.Faults.Coverage.CountsBySite().GetValueOrDefault(FaultSites.GcSweep);

    public static Task WaitAsync(TestCluster cluster, Func<Task<bool>> condition, TimeSpan timeout, string because) =>
        cluster.WaitUntilAsync(condition, timeout, because, Ct);

    public static Task WaitReclaimedAsync(TestCluster cluster, IReadOnlyCollection<ObjectId> ids, string because, string volume = Volume) =>
        WaitAsync(cluster, async () => !(await StoredAsync(cluster, volume)).Overlaps(ids), ReclaimBudget(cluster), because);

    /// <summary>Waits until the volume stores nothing that is unreachable (no leak, no pending garbage).</summary>
    public static Task WaitNoGarbageAsync(TestCluster cluster, string because, string volume = Volume) =>
        WaitAsync(cluster, async () =>
        {
            HashSet<ObjectId> stored = await StoredAsync(cluster, volume);
            stored.ExceptWith(await ReachableAsync(cluster, volume));
            return stored.Count == 0;
        }, ReclaimBudget(cluster), because);

    public static DfsOpenOptions PinnedReader => new()
    {
        Mode = FileMode.Open,
        Access = FileAccess.Read,
        Share = FileShare.ReadWrite | FileShare.Delete,
        ReadView = DfsReadView.VersionPinned,
    };

    public static DfsOpenOptions Writer => new() { Mode = FileMode.Open, Access = FileAccess.ReadWrite, Share = FileShare.Read };

    public static async Task<byte[]> ReadRangeAsync(DfsStream stream, long offset, int count)
    {
        stream.Seek(offset, SeekOrigin.Begin);
        var buffer = new byte[count];
        await stream.ReadExactlyAsync(buffer, Ct);
        return buffer;
    }

    /// <summary>Applies an overwrite of <paramref name="data"/> at <paramref name="offset"/> to a model copy.</summary>
    public static byte[] Overlay(byte[] current, long offset, byte[] data)
    {
        var result = new byte[Math.Max(current.Length, offset + data.Length)];
        current.CopyTo(result, 0);
        data.CopyTo(result, offset);
        return result;
    }

    /// <summary>Partially overwrites a file through a stream and publishes it with one flush.</summary>
    public static async Task<CommitToken> OverwriteRangeAsync(DfsVolume volume, string path, long offset, byte[] data)
    {
        await using DfsStream stream = await volume.Files.OpenAsync(path, Writer, Ct);
        stream.Seek(offset, SeekOrigin.Begin);
        await stream.WriteAsync(data, Ct);
        return await stream.CommitAsync(Ct);
    }

    public static async Task<byte[]> ReadSnapshotFileAsync(DfsVolume volume, string snapshot, string path)
    {
        await using DfsSnapshotView view = await volume.Snapshots.OpenAsync(snapshot, Ct);
        return await view.Files.ReadAllBytesAsync(path, Ct);
    }
}
