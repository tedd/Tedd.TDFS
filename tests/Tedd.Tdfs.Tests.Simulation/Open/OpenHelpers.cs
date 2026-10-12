using System.Globalization;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Simulation.Open;

/// <summary>Result of awaiting a task for a bounded (virtual) time.</summary>
internal readonly record struct Bounded<T>(bool Completed, T? Result, Exception? Error);

/// <summary>Shared plumbing for OPEN simulation tests (deterministic clusters, sharing oracle, bounded waits).</summary>
internal static class OpenHelpers
{
    public const string Volume = "vol";

    public static CancellationToken Ct => TestContext.Current.CancellationToken;

    public static void Log(string message) => TestContext.Current.TestOutputHelper?.WriteLine(message);

    /// <summary>
    /// Deterministic simulation with <paramref name="nodeCount"/> hosts one per zone; 3 metadata/control voters (5 when
    /// at least five nodes exist), NodeTimings.Fast (lease 2 s, session 10 s).
    /// </summary>
    public static TestClusterOptions Deterministic(int nodeCount, ulong seed)
    {
        int voters = nodeCount >= 5 ? 5 : Math.Min(3, nodeCount);
        return new TestClusterOptions
        {
            NodeCount = nodeCount,
            Topology = TestTopology.OnePerZone,
            ControlVoters = voters,
            Mode = ClusterMode.Deterministic,
            Seed = seed,
            Volumes = [new TestVolumeSpec(Volume) { Metadata = new DfsMetadataPolicy { Voters = voters } }],
        };
    }

    /// <summary>Stable 64-bit FNV-1a hash used to derive per-case seeds from theory arguments.</summary>
    public static ulong SeedOf(params string[] parts)
    {
        ulong hash = 14695981039346656037UL;
        foreach (string part in parts)
        {
            foreach (char c in part)
            {
                hash ^= c;
                hash *= 1099511628211UL;
            }

            hash ^= 0xFF;
            hash *= 1099511628211UL;
        }

        return hash == 0 ? 1 : hash;
    }

    /// <summary>
    /// Bidirectional FileShare compatibility plus single-writer exclusivity (FSD §7 LOCK-01/LOCK-02); an independent
    /// restatement of the rule, not product code.
    /// </summary>
    public static bool IsCompatible(FileAccess a1, FileShare s1, FileAccess a2, FileShare s2)
    {
        bool r1 = (a1 & FileAccess.Read) != 0, w1 = (a1 & FileAccess.Write) != 0;
        bool r2 = (a2 & FileAccess.Read) != 0, w2 = (a2 & FileAccess.Write) != 0;
        if (r2 && (s1 & FileShare.Read) == 0)
            return false;
        if (w2 && (s1 & FileShare.Write) == 0)
            return false;
        if (r1 && (s2 & FileShare.Read) == 0)
            return false;
        if (w1 && (s2 & FileShare.Write) == 0)
            return false;
        return !(w1 && w2);
    }

    /// <summary>Every legal (access, share) handle: writable handles never grant FileShare.Write.</summary>
    public static IReadOnlyList<(FileAccess Access, FileShare Share)> LegalHandles { get; } = BuildLegalHandles();

    /// <summary>Failures that only mean "could not determine right now" (timeouts, lost authority, unknown outcome).</summary>
    public static bool IsIndeterminate(Exception ex) =>
        ex is TimeoutException or OperationCanceledException or DfsUnavailableException or DfsCommitOutcomeUnknownException
        || ex is DfsException { ErrorCode: DfsErrorCode.NotLeader or DfsErrorCode.Timeout or DfsErrorCode.Unavailable or DfsErrorCode.NodeUnavailable or DfsErrorCode.QuorumUnavailable };

    /// <summary>Awaits <paramref name="task"/> for at most <paramref name="timeout"/> of cluster (virtual in deterministic mode) time.</summary>
    public static async Task<Bounded<T>> WithinAsync<T>(TestCluster cluster, Task<T> task, TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource();
        Task delay = cluster.DelayAsync(timeout, cts.Token);
        Task first = await Task.WhenAny(task, delay);
        if (first != task)
            return new Bounded<T>(false, default, null);
        await cts.CancelAsync();
        try
        {
            return new Bounded<T>(true, await task, null);
        }
        catch (Exception ex)
        {
            return new Bounded<T>(true, default, ex);
        }
    }

    /// <summary>Live (non-expired) reservations on <paramref name="inode"/> at the volume leader.</summary>
    public static async Task<IReadOnlyList<HandleInspection>> LiveHandlesAsync(TestCluster cluster, InodeId inode)
    {
        VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Volume);
        return [.. state.Handles.Where(h => h.Inode == inode && !h.Expired)];
    }

    /// <summary>Applied inode count on every running volume voter.</summary>
    public static async Task<IReadOnlyDictionary<string, int>> InodeCountsAsync(TestCluster cluster)
    {
        GroupInspection group = await cluster.Inspect.GetVolumeGroupAsync(Volume);
        var counts = new SortedDictionary<string, int>(StringComparer.Ordinal);
        foreach (string voter in group.Voters)
        {
            TestNode node = cluster.Node(voter);
            if (node.State != TestNodeState.Running)
                continue;
            VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Volume, voter);
            counts[voter] = state.InodeCount;
        }

        return counts;
    }

    /// <summary>Waits for replication, then asserts no split brain and identical replicated state on caught-up replicas.</summary>
    public static async Task AssertConvergedAsync(TestCluster cluster)
    {
        await cluster.WaitForReplicationAsync(Volume, Ct);
        await cluster.Inspect.AssertNoSplitBrainAsync();
        await cluster.Inspect.AssertReplicasConvergedAsync(Volume);
    }

    /// <summary>Crashes the current volume leader, waits until another node leads, then restarts the crashed node.</summary>
    public static async Task FailOverVolumeLeaderAsync(TestCluster cluster)
    {
        TestNode old = await cluster.GetLeaderAsync(Volume, Ct);
        await old.CrashAsync(CrashMode.LoseUnflushed);
        await cluster.WaitUntilAsync(async () =>
        {
            GroupInspection g = await cluster.Inspect.GetVolumeGroupAsync(Volume);
            return g.Leader is not null && g.Leader != old.Name;
        }, TimeSpan.FromSeconds(10), "a surviving majority elects a new volume leader within 10 s after the leader crashed", Ct);
        await old.RestartAsync();
        await cluster.WaitForStableAsync(TimeSpan.FromSeconds(30), Ct);
    }

    /// <summary>Number of OpenHandle requests a client has put on the wire.</summary>
    public static long CountOpenRequests(TestCluster cluster, string clientName) =>
        cluster.Network.Log.CountMessages(new MessageFilter(From: "client:" + clientName, Type: MessageType.OpenHandle));

    public static string Describe(Exception? ex) =>
        ex is null ? "(no exception)" : $"{ex.GetType().Name}{(ex is DfsException d ? $"[{d.ErrorCode}]" : string.Empty)}: {ex.Message}";

    public static string Name(FileAccess access, FileShare share) =>
        string.Create(CultureInfo.InvariantCulture, $"{access}/{(share == FileShare.None ? "None" : share.ToString().Replace(", ", "+", StringComparison.Ordinal))}");

    public static byte[] Concat(params byte[][] parts)
    {
        var result = new byte[parts.Sum(p => p.Length)];
        int at = 0;
        foreach (byte[] p in parts)
        {
            p.CopyTo(result, at);
            at += p.Length;
        }

        return result;
    }

    public static byte[] Overlay(ReadOnlySpan<byte> original, ReadOnlySpan<byte> data, int offset = 0)
    {
        var result = new byte[Math.Max(original.Length, offset + data.Length)];
        original.CopyTo(result);
        data.CopyTo(result.AsSpan(offset));
        return result;
    }

    /// <summary>
    /// Reads the committed bytes through an explicit reader handle that is share-compatible with an active writer
    /// (Read access, FileShare.ReadWrite|Delete), as the test plan §5 requires for visibility checks while a writer is open.
    /// </summary>
    public static async Task<byte[]> ReadSharedAsync(DfsFileClient files, string path)
    {
        await using DfsStream reader = await files.OpenAsync(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete, Ct);
        return await ReadToEndAsync(reader);
    }

    public static async Task<byte[]> ReadToEndAsync(Stream stream)
    {
        using var buffer = new MemoryStream();
        var chunk = new byte[5003];
        while (true)
        {
            int n = await stream.ReadAsync(chunk, Ct);
            if (n == 0)
                return buffer.ToArray();
            buffer.Write(chunk, 0, n);
        }
    }

    /// <summary>Awaits <paramref name="actual"/> and asserts byte-for-byte equality.</summary>
    public static async Task AssertBytesAsync(byte[] expected, Task<byte[]> actual, string? because = null)
    {
        byte[] bytes = await actual;
        AssertEx.BytesEqual(expected, bytes, because);
    }

    private static List<(FileAccess, FileShare)> BuildLegalHandles()
    {
        var list = new List<(FileAccess, FileShare)>();
        foreach (FileAccess access in new[] { FileAccess.Read, FileAccess.Write, FileAccess.ReadWrite })
        {
            for (int bits = 0; bits < 8; bits++)
            {
                var share = (FileShare)bits; // None, Read, Write, Delete combinations
                if ((access & FileAccess.Write) != 0 && (share & FileShare.Write) != 0)
                    continue;
                list.Add((access, share));
            }
        }

        return list;
    }
}
