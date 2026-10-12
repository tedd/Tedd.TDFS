using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Simulation.File;

/// <summary>Shared helpers of the FILE suite (deterministic simulation level).</summary>
internal static class FileHelpers
{
    /// <summary>Name of the default test volume.</summary>
    public const string Vol = "vol";

    /// <summary>Fault-match count that keeps an arm firing for every matching hit (persistent fault).</summary>
    public const int Always = int.MaxValue;

    /// <summary>Logical update unit (chunk) of <c>NodeTimings.Fast</c>.</summary>
    public const int Chunk = 64 << 10;

    /// <summary>Bound for waiting on a fault point to be reached (virtual time).</summary>
    public static readonly TimeSpan FaultWait = TimeSpan.FromSeconds(20);

    /// <summary>Bound for release/reclamation/recovery waits (virtual time; several Fast leases and GC passes).</summary>
    public static readonly TimeSpan RecoveryWait = TimeSpan.FromSeconds(30);

    /// <summary>The test's cancellation token.</summary>
    public static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Deterministic three-zone cluster (3 nodes, 3 voters, target 3 / ack 2 in 2 zones).</summary>
    public static TestClusterOptions Sim(ulong seed) =>
        TestClusterOptions.ThreeZones with { Mode = ClusterMode.Deterministic, Seed = seed };

    /// <summary>Client options with a distinct network identity.</summary>
    public static TestClientOptions Named(string name, DfsCredentials? credentials = null, TimeSpan? operationTimeout = null) =>
        new() { Name = name, Credentials = credentials, OperationTimeout = operationTimeout };

    /// <summary>Byte-for-byte equality on arrays (safe to call with awaited arguments).</summary>
    public static void AssertBytes(byte[] expected, byte[] actual, string? because = null) =>
        AssertEx.BytesEqual(expected, actual, because);

    /// <summary>Asserts the file's full contents and (optionally) its inode identity.</summary>
    public static async Task AssertFileAsync(DfsVolume volume, string path, byte[] expected, InodeId? inode = null, string? because = null)
    {
        byte[] actual = await volume.Files.ReadAllBytesAsync(path, Ct);
        AssertEx.BytesEqual(expected, actual, because ?? path);
        DfsFileInfo info = await volume.Files.GetInfoAsync(path, Ct);
        Assert.Equal(expected.Length, info.Length);
        if (inode is { } id)
            Assert.Equal(id, info.InodeId);
    }

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

    /// <summary>
    /// Reads the whole committed file through an explicit live handle sharing ReadWrite|Delete (compatible with any
    /// concurrent writer reservation, test plan section 5); null for genuine absence. Returns bytes and the inode read.
    /// </summary>
    public static async Task<(byte[] Bytes, InodeId Inode)?> TryReadSharedAsync(DfsVolume volume, string path)
    {
        try
        {
            await using DfsStream stream = await volume.Files.OpenAsync(path, new DfsOpenOptions
            {
                Mode = FileMode.Open,
                Access = FileAccess.Read,
                Share = FileShare.ReadWrite | FileShare.Delete,
            }, Ct);
            return (await ReadToEndAsync(stream), stream.InodeId);
        }
        catch (FileNotFoundException)
        {
            return null;
        }
    }

    /// <summary>Reads a file, returning null for genuine absence.</summary>
    public static async Task<byte[]?> TryReadAsync(DfsVolume volume, string path)
    {
        try
        {
            return await volume.Files.ReadAllBytesAsync(path, Ct);
        }
        catch (FileNotFoundException)
        {
            return null;
        }
    }

    /// <summary>Waits (bounded) until no live handle references <paramref name="inode"/>.</summary>
    public static Task WaitForNoHandlesAsync(TestCluster cluster, InodeId inode, string because) =>
        cluster.WaitUntilAsync(async () =>
        {
            VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Vol);
            return state.Handles.All(h => h.Inode != inode || h.Expired);
        }, RecoveryWait, because, Ct);

    /// <summary>
    /// A failed mutation must be definitely not committed: an unknown outcome must carry the operation id, and resolving
    /// the id must yield <see cref="DfsOutcomeStatus.Aborted"/>.
    /// </summary>
    public static async Task AssertResolvesAbortedAsync(DfsVolume volume, Exception failure, DfsOperationId opId)
    {
        if (failure is DfsCommitOutcomeUnknownException unknown)
            Assert.Equal(opId, unknown.OperationId);
        DfsOperationOutcome outcome = await volume.QueryOperationAsync(opId, resolve: true, Ct);
        Assert.Equal(opId, outcome.Id);
        Assert.Equal(DfsOutcomeStatus.Aborted, outcome.Status);
    }

    /// <summary>Object ids referenced by the current extent map of a file (empty for inline files).</summary>
    public static async Task<IReadOnlySet<ObjectId>> ObjectsOfAsync(TestCluster cluster, string path)
    {
        FileInspection? file = await cluster.Inspect.GetFileAsync(Vol, path);
        Assert.NotNull(file);
        return file.Extents.Select(e => e.Object).ToHashSet();
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

    /// <summary>A grant map for one volume.</summary>
    public static IReadOnlyDictionary<string, DfsPermissions> Grant(DfsPermissions permissions, string volume = Vol) =>
        new Dictionary<string, DfsPermissions>(StringComparer.Ordinal) { [volume] = permissions };
}

/// <summary>
/// A fault action that holds the first matching hit until <see cref="Release"/> (so the test can observe the system
/// while the operation is suspended at that exact boundary) and then, optionally, fails that hit and every later
/// matching hit with an injected exception (a persistent fault that SDK retries cannot get past).
/// Deterministic-simulation safe: it only awaits a <see cref="TaskCompletionSource"/> completed by the scenario.
/// </summary>
internal sealed class FaultGate
{
    private readonly TaskCompletionSource _release = new();
    private readonly Func<FaultContext, Exception>? _failure;

    public FaultGate(Func<FaultContext, Exception>? failure = null) => _failure = failure;

    /// <summary>Matching hits seen by this gate.</summary>
    public int Hits { get; private set; }

    /// <summary>Hits that were failed with the injected exception.</summary>
    public int Failures { get; private set; }

    /// <summary>The harness action.</summary>
    public FaultAction Action => FaultAction.Custom(async context =>
    {
        Hits++;
        if (Hits == 1)
            await _release.Task;
        if (_failure is not null)
        {
            Failures++;
            throw _failure(context);
        }
    });

    /// <summary>Lets the held hit continue.</summary>
    public void Release() => _release.TrySetResult();
}
