using System.Text;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Simulation.Ns;

/// <summary>Shared helpers of the NS suite (deterministic simulation level).</summary>
internal static class NsHelpers
{
    /// <summary>Name of the default test volume.</summary>
    public const string Vol = "vol";

    /// <summary>Fault-match count that keeps an arm firing for every matching hit.</summary>
    public const int Always = int.MaxValue;

    /// <summary>Logical update unit (chunk) of <c>NodeTimings.Fast</c>.</summary>
    public const int Chunk = 64 << 10;

    /// <summary>Bound for waiting on a fault point (virtual time).</summary>
    public static readonly TimeSpan FaultWait = TimeSpan.FromSeconds(20);

    /// <summary>Bound for release/reclamation/recovery waits (virtual time).</summary>
    public static readonly TimeSpan RecoveryWait = TimeSpan.FromSeconds(40);

    /// <summary>Register value "present" for subtree/name existence histories.</summary>
    public static readonly byte[] Present = [1];

    /// <summary>Register value "absent" for subtree/name existence histories.</summary>
    public static readonly byte[] Absent = [0];

    /// <summary>The test's cancellation token.</summary>
    public static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Deterministic cluster of <paramref name="nodeCount"/> nodes, one per zone.</summary>
    public static TestClusterOptions Sim(ulong seed, int nodeCount = 3) => nodeCount == 5
        ? TestClusterOptions.FiveNodes with { Mode = ClusterMode.Deterministic, Seed = seed }
        : TestClusterOptions.ThreeZones with { NodeCount = nodeCount, Mode = ClusterMode.Deterministic, Seed = seed };

    /// <summary>Client options with a distinct network identity.</summary>
    public static TestClientOptions Named(string name, DfsCredentials? credentials = null) =>
        new() { Name = name, Credentials = credentials };

    /// <summary>ASCII register value (for named states such as "S"/"D").</summary>
    public static byte[] State(string value) => Encoding.ASCII.GetBytes(value);

    /// <summary>Byte-for-byte equality on arrays (safe to call with awaited arguments).</summary>
    public static void AssertBytes(byte[] expected, byte[] actual, string? because = null) =>
        AssertEx.BytesEqual(expected, actual, because);

    /// <summary>Reads a stream from its current position to its end.</summary>
    public static async Task<byte[]> ReadToEndAsync(Stream stream)
    {
        using var buffer = new MemoryStream();
        var chunk = new byte[16 * 1024];
        while (true)
        {
            int read = await stream.ReadAsync(chunk.AsMemory(), Ct);
            if (read == 0)
                return buffer.ToArray();
            buffer.Write(chunk, 0, read);
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

    /// <summary>Collects an async enumeration.</summary>
    public static async Task<List<T>> ToListAsync<T>(IAsyncEnumerable<T> source)
    {
        var list = new List<T>();
        await foreach (T item in source)
            list.Add(item);
        return list;
    }

    /// <summary>A grant map for one volume.</summary>
    public static IReadOnlyDictionary<string, DfsPermissions> Grant(DfsPermissions permissions, string volume = Vol) =>
        new Dictionary<string, DfsPermissions>(StringComparer.Ordinal) { [volume] = permissions };

    /// <summary>Number of inodes in the leader's applied volume state.</summary>
    public static async Task<int> InodeCountAsync(TestCluster cluster)
    {
        VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Vol);
        return state.InodeCount;
    }

    /// <summary>Object ids referenced by a file's current extent map.</summary>
    public static async Task<HashSet<ObjectId>> ObjectsOfAsync(TestCluster cluster, string path)
    {
        FileInspection? file = await cluster.Inspect.GetFileAsync(Vol, path);
        Assert.NotNull(file);
        return file.Extents.Select(e => e.Object).ToHashSet();
    }

    /// <summary>Waits until none of <paramref name="objects"/> is stored on any node.</summary>
    public static Task WaitForObjectsReclaimedAsync(TestCluster cluster, IReadOnlySet<ObjectId> objects, string because) =>
        cluster.WaitUntilAsync(async () =>
        {
            IReadOnlySet<ObjectId> stored = await cluster.Inspect.GetAllStoredObjectsAsync(Vol);
            return !objects.Any(stored.Contains);
        }, RecoveryWait, because, Ct);

    /// <summary>Asserts every object is still stored somewhere (protected from GC).</summary>
    public static async Task AssertObjectsStoredAsync(TestCluster cluster, IReadOnlySet<ObjectId> objects, string because)
    {
        IReadOnlySet<ObjectId> stored = await cluster.Inspect.GetAllStoredObjectsAsync(Vol);
        foreach (ObjectId id in objects)
            Assert.True(stored.Contains(id), $"{because}: object {id} was reclaimed while still protected");
    }

    /// <summary>
    /// Records one existence probe as a register read: <paramref name="probe"/> returns true for "the observed state is
    /// <paramref name="whenTrue"/>" and false for <paramref name="whenFalse"/>.
    /// </summary>
    public static async Task ProbeAsync(TestCluster cluster, string process, string key, byte[] whenTrue, byte[] whenFalse, Func<Task<bool>> probe)
    {
        long id = cluster.History.Invoke(process, "read", key, null);
        bool result = await probe();
        cluster.History.Complete(id, OpOutcome.Ok, result ? whenTrue : whenFalse);
    }

    /// <summary>Records one probe whose observed register value is computed by <paramref name="probe"/>.</summary>
    public static async Task ProbeValueAsync(TestCluster cluster, string process, string key, Func<Task<byte[]>> probe)
    {
        long id = cluster.History.Invoke(process, "read", key, null);
        byte[] value = await probe();
        cluster.History.Complete(id, OpOutcome.Ok, value);
    }

    /// <summary>Records a mutation as a register write and runs it.</summary>
    public static async Task<T> WriteAsync<T>(TestCluster cluster, string process, string key, byte[] value, Func<Task<T>> mutation)
    {
        long id = cluster.History.Invoke(process, "write", key, value);
        try
        {
            T result = await mutation();
            cluster.History.Complete(id, OpOutcome.Ok, result);
            return result;
        }
        catch (Exception ex)
        {
            cluster.History.Complete(id, HistoryRecorder.Classify(ex), ex);
            throw;
        }
    }

    /// <summary>True when <paramref name="action"/> throws <see cref="DirectoryNotFoundException"/> or <see cref="FileNotFoundException"/>.</summary>
    public static async Task<bool> IsAbsentAsync(Func<Task> action)
    {
        try
        {
            await action();
            return false;
        }
        catch (Exception ex) when (ex is DirectoryNotFoundException or FileNotFoundException)
        {
            return true;
        }
    }
}
