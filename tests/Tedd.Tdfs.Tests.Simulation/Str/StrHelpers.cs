using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Simulation.Str;

/// <summary>Shared helpers for the STR simulation rows (async-only: these scenarios run inside the deterministic simulation).</summary>
internal static class StrHelpers
{
    public const string Vol = "vol";

    /// <summary>Logical update unit under <see cref="NodeTimings.Fast"/> (64 KiB).</summary>
    public static int Chunk => NodeTimings.Fast.ChunkSize;

    /// <summary>Bound for liveness waits (virtual time in deterministic mode).</summary>
    public static readonly TimeSpan Bounded = TimeSpan.FromSeconds(30);

    public static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary><see cref="AssertEx.BytesEqual"/> over memory, so an argument list may contain an await.</summary>
    public static void BytesEqual(ReadOnlyMemory<byte> expected, ReadOnlyMemory<byte> actual, string because) =>
        AssertEx.BytesEqual(expected.Span, actual.Span, because);

    /// <summary>The three-zone core configuration (3 voters, target 3 / ack 2 in 2 zones) in deterministic mode.</summary>
    public static TestClusterOptions Deterministic() => TestClusterOptions.ThreeZones with { Mode = ClusterMode.Deterministic };

    public static void Log(string message) => TestContext.Current.TestOutputHelper?.WriteLine(message);

    public static Task<DfsClient> ConnectAsync(TestCluster cluster, string name, TimeSpan? operationTimeout = null, TestClock? clock = null) =>
        cluster.ConnectAsync(new TestClientOptions { Name = name, OperationTimeout = operationTimeout, Clock = clock }, Ct);

    /// <summary>Deterministic bytes without any zero byte.</summary>
    public static byte[] NonZero(ulong seed, int length)
    {
        byte[] data = TestData.Bytes(seed, length);
        for (int i = 0; i < data.Length; i++)
        {
            if (data[i] == 0)
                data[i] = (byte)(1 + (i % 251));
        }

        return data;
    }

    /// <summary>A reader granting every share so writers, deleters and renamers can coexist (test plan §5).</summary>
    public static DfsOpenOptions Reader(DfsReadView view = DfsReadView.Live) => new()
    {
        Mode = FileMode.Open,
        Access = FileAccess.Read,
        Share = FileShare.ReadWrite | FileShare.Delete,
        ReadView = view,
    };

    /// <summary>A writer granting readers.</summary>
    public static DfsOpenOptions Writer(FileMode mode = FileMode.Open, FileAccess access = FileAccess.Write,
        DfsWriteMode writeMode = DfsWriteMode.OnFlush) => new()
    {
        Mode = mode,
        Access = access,
        Share = FileShare.Read,
        WriteMode = writeMode,
    };

    /// <summary>
    /// Creates <paramref name="path"/> with <paramref name="data"/> published one <see cref="Chunk"/>-sized piece per flush,
    /// so every chunk is a separate published object (the extent layout does not depend on how WriteAll packs objects).
    /// </summary>
    public static async Task SeedOneObjectPerChunkAsync(DfsVolume volume, string path, byte[] data)
    {
        await using DfsStream w = await volume.Files.OpenAsync(path, Writer(FileMode.CreateNew, FileAccess.Write), Ct);
        for (int offset = 0; offset < data.Length; offset += Chunk)
        {
            await w.WriteAsync(data.AsMemory(offset, Math.Min(Chunk, data.Length - offset)), Ct);
            await w.FlushAsync(Ct);
        }
    }

    /// <summary>Reads from the current position until <paramref name="count"/> bytes or EOF, asserting Position tracks every call.</summary>
    public static async Task<byte[]> ReadUpToAsync(DfsStream s, int count)
    {
        var result = new byte[count];
        int total = 0;
        while (total < count)
        {
            long before = s.Position;
            int n = await s.ReadAsync(result.AsMemory(total, count - total), Ct);
            Assert.InRange(n, 0, count - total);
            Assert.Equal(before + n, s.Position);
            if (n == 0)
                break;
            total += n;
        }

        return result.AsSpan(0, total).ToArray();
    }

    /// <summary>Seeks to 0 and reads to EOF with async calls only.</summary>
    public static async Task<byte[]> ReadWholeAsync(DfsStream s)
    {
        s.Seek(0, SeekOrigin.Begin);
        using var ms = new MemoryStream();
        var buffer = new byte[96 * 1024];
        while (true)
        {
            int n = await s.ReadAsync(buffer, Ct);
            if (n == 0)
                break;
            ms.Write(buffer, 0, n);
        }

        return ms.ToArray();
    }

    /// <summary>Objects referenced by the current revision of <paramref name="path"/>.</summary>
    public static async Task<IReadOnlySet<ObjectId>> GetObjectsAsync(TestCluster cluster, string path)
    {
        FileInspection? file = await cluster.Inspect.GetFileAsync(Vol, path);
        Assert.True(file is not null, $"inspection must find {path}");
        return file!.Extents.Select(e => e.Object).ToHashSet();
    }

    /// <summary>First offset where <paramref name="actual"/> differs from <paramref name="expected"/> (or -1).</summary>
    public static int FirstDifference(ReadOnlySpan<byte> actual, ReadOnlySpan<byte> expected)
    {
        int common = actual.CommonPrefixLength(expected);
        return common == Math.Min(actual.Length, expected.Length) && actual.Length <= expected.Length ? -1 : common;
    }

    /// <summary>Awaits <paramref name="task"/> with a bound on the cluster clock (virtual in deterministic mode).</summary>
    public static async Task<T> Within<T>(this Task<T> task, TestCluster cluster, string because)
    {
        try
        {
            return await task.WaitAsync(Bounded, cluster.Time, Ct);
        }
        catch (TimeoutException) when (!task.IsCompleted)
        {
            throw new TimeoutException($"Not completed within {Bounded}: {because}");
        }
    }

    /// <summary>Awaits <paramref name="task"/> with a bound on the cluster clock (virtual in deterministic mode).</summary>
    public static async Task Within(this Task task, TestCluster cluster, string because)
    {
        try
        {
            await task.WaitAsync(Bounded, cluster.Time, Ct);
        }
        catch (TimeoutException) when (!task.IsCompleted)
        {
            throw new TimeoutException($"Not completed within {Bounded}: {because}");
        }
    }
}
