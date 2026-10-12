using System.Text;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Unit.File;

/// <summary>Shared helpers of the FILE suite (unit level).</summary>
internal static class FileHelpers
{
    /// <summary>Name of the default test volume.</summary>
    public const string Vol = "vol";

    /// <summary>Fault-match count that keeps an arm firing for every matching hit (persistent fault).</summary>
    public const int Always = int.MaxValue;

    /// <summary>Inline threshold of the default test volume.</summary>
    public const int Inline = 4096;

    /// <summary>Logical update unit (chunk) of <c>NodeTimings.Fast</c>.</summary>
    public const int Chunk = 64 << 10;

    /// <summary>Bound for waiting on a fault point to be reached.</summary>
    public static readonly TimeSpan FaultWait = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Bound for server-side release of handles/reservations/staging after a helper ends: generous multiple of the
    /// Fast lease (2 s) so an implementation that only releases by lease expiry still passes, while a leak fails.
    /// </summary>
    public static readonly TimeSpan ReleaseWait = TimeSpan.FromSeconds(10);

    /// <summary>The test's cancellation token.</summary>
    public static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Client options with a distinct network identity.</summary>
    public static TestClientOptions Named(string name, DfsCredentials? credentials = null) =>
        new() { Name = name, Credentials = credentials };

    /// <summary>Strict UTF-8 without BOM (the documented default text encoding).</summary>
    public static readonly UTF8Encoding Utf8NoBom = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    /// <summary>The UTF-8 byte order mark.</summary>
    public static readonly byte[] Utf8Bom = [0xEF, 0xBB, 0xBF];

    /// <summary>Byte-for-byte equality on arrays (safe to call with awaited arguments).</summary>
    public static void AssertBytes(byte[] expected, byte[] actual, string? because = null) =>
        AssertEx.BytesEqual(expected, actual, because);

    /// <summary>Byte-for-byte equality on spans.</summary>
    public static void AssertBytes(ReadOnlySpan<byte> expected, ReadOnlySpan<byte> actual, string? because = null) =>
        AssertEx.BytesEqual(expected, actual, because);

    /// <summary>Reads a stream to its end with the stream's own async read path.</summary>
    public static async Task<byte[]> ReadToEndAsync(Stream stream, CancellationToken ct)
    {
        using var buffer = new MemoryStream();
        var chunk = new byte[16 * 1024];
        while (true)
        {
            int read = await stream.ReadAsync(chunk.AsMemory(), ct);
            if (read == 0)
                return buffer.ToArray();
            buffer.Write(chunk, 0, read);
        }
    }

    /// <summary>
    /// Reads the whole committed file through an explicit live handle that shares ReadWrite|Delete, so the observation
    /// is compatible with any concurrent writer reservation (test plan section 5: visibility checks must not be sharing
    /// conflicts). Returns the bytes and the inode the handle names.
    /// </summary>
    public static async Task<(byte[] Bytes, InodeId Inode)> ReadSharedAsync(DfsVolume volume, string path)
    {
        await using DfsStream stream = await volume.Files.OpenAsync(path, new DfsOpenOptions
        {
            Mode = FileMode.Open,
            Access = FileAccess.Read,
            Share = FileShare.ReadWrite | FileShare.Delete,
        }, Ct);
        return (await ReadToEndAsync(stream, Ct), stream.InodeId);
    }

    /// <summary>Asserts the file's full contents (and optionally its inode identity and length metadata).</summary>
    public static async Task AssertFileAsync(DfsVolume volume, string path, ReadOnlyMemory<byte> expected, InodeId? inode = null, string? because = null)
    {
        byte[] actual = await volume.Files.ReadAllBytesAsync(path, Ct);
        AssertEx.BytesEqual(expected.Span, actual, because ?? path);
        DfsFileInfo info = await volume.Files.GetInfoAsync(path, Ct);
        Assert.Equal(expected.Length, info.Length);
        if (inode is { } id)
            Assert.Equal(id, info.InodeId);
    }

    /// <summary>Number of handles the volume leader currently tracks for <paramref name="inode"/>.</summary>
    public static async Task<int> CountHandlesAsync(TestCluster cluster, InodeId inode)
    {
        VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Vol);
        return state.Handles.Count(h => h.Inode == inode && !h.Expired);
    }

    /// <summary>Waits (bounded) until no live handle references <paramref name="inode"/>.</summary>
    public static Task WaitForNoHandlesAsync(TestCluster cluster, InodeId inode, string because) =>
        cluster.WaitUntilAsync(async () => await CountHandlesAsync(cluster, inode) == 0, ReleaseWait, because, Ct);

    /// <summary>Waits (bounded) until no staging session of the volume is open.</summary>
    public static Task WaitForNoOpenStagingAsync(TestCluster cluster, string because) =>
        cluster.WaitUntilAsync(async () =>
        {
            VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Vol);
            return state.Staging.All(s => !s.Open);
        }, ReleaseWait, because, Ct);

    /// <summary>
    /// A failed mutation must be definitely not committed: an unknown outcome must carry the operation id and resolve to
    /// <see cref="DfsOutcomeStatus.Aborted"/>; any other failure must also resolve to Aborted (never Committed).
    /// </summary>
    public static async Task AssertResolvesAbortedAsync(DfsVolume volume, Exception failure, DfsOperationId opId)
    {
        if (failure is DfsCommitOutcomeUnknownException unknown)
            Assert.Equal(opId, unknown.OperationId);
        DfsOperationOutcome outcome = await volume.QueryOperationAsync(opId, resolve: true, Ct);
        Assert.Equal(opId, outcome.Id);
        Assert.Equal(DfsOutcomeStatus.Aborted, outcome.Status);
    }

    /// <summary>Independent line oracle: <see cref="StreamReader.ReadLine()"/> over UTF-8 with BOM detection.</summary>
    public static List<string> OracleLines(byte[] bytes)
    {
        using var reader = new StreamReader(new MemoryStream(bytes), new UTF8Encoding(false), detectEncodingFromByteOrderMarks: true);
        var lines = new List<string>();
        while (reader.ReadLine() is { } line)
            lines.Add(line);
        return lines;
    }

    /// <summary>Collects an async enumeration.</summary>
    public static async Task<List<T>> ToListAsync<T>(IAsyncEnumerable<T> source)
    {
        var list = new List<T>();
        await foreach (T item in source)
            list.Add(item);
        return list;
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
