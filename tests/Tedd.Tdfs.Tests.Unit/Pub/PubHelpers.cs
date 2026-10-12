using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Unit.Pub;

/// <summary>Which <see cref="Stream"/> write overload a PUB test drives.</summary>
public enum WriteKind
{
    /// <summary><see cref="Stream.Write(byte[], int, int)"/>.</summary>
    ArraySync,
    /// <summary><see cref="Stream.Write(ReadOnlySpan{byte})"/>.</summary>
    SpanSync,
    /// <summary><see cref="Stream.WriteAsync(byte[], int, int, CancellationToken)"/>.</summary>
    ArrayAsync,
    /// <summary><see cref="Stream.WriteAsync(ReadOnlyMemory{byte}, CancellationToken)"/>.</summary>
    MemoryAsync,
    /// <summary><see cref="Stream.WriteByte(byte)"/> once per byte.</summary>
    ByteByByte,
}

/// <summary>Shared helpers for the PUB unit tests (FSD §§6.3–6.4, 10.3, 13).</summary>
internal static class PubHelpers
{
    /// <summary>The harness volume.</summary>
    public const string Vol = "vol";

    /// <summary>NodeTimings.Fast logical update unit (chunk) size.</summary>
    public const int Chunk = 64 * 1024;

    /// <summary>Default SDK stream buffer.</summary>
    public const int DefaultBuffer = 64 * 1024;

    /// <summary>The test's cancellation token.</summary>
    public static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>The default (target 3 / ack 2 copies in 2 zones) policy of <see cref="TestClusterOptions.ThreeZones"/>.</summary>
    public static DfsStoragePolicy ThreeZonePolicy => TestClusterOptions.ThreeZones.GetEffectiveDefaultPolicy();

    /// <summary>A writer open: <c>Share.Read</c> (the compatible partner of a <c>Share.ReadWrite</c> reader, test plan §5).</summary>
    public static DfsOpenOptions WriterOptions(FileMode mode = FileMode.Open, DfsWriteMode writeMode = DfsWriteMode.OnFlush,
        FileAccess access = FileAccess.ReadWrite, int bufferSize = DefaultBuffer) => new()
        {
            Mode = mode,
            Access = access,
            Share = FileShare.Read,
            WriteMode = writeMode,
            BufferSize = bufferSize,
        };

    /// <summary>An append open (write-only, <c>Share.Read</c>).</summary>
    public static DfsOpenOptions AppendOptions() => new()
    {
        Mode = FileMode.Append,
        Access = FileAccess.Write,
        Share = FileShare.Read,
    };

    /// <summary>A live reader that tolerates a concurrent writer (<c>Share.ReadWrite</c>).</summary>
    public static DfsOpenOptions LiveReaderOptions() => new()
    {
        Mode = FileMode.Open,
        Access = FileAccess.Read,
        Share = FileShare.ReadWrite,
        ReadView = DfsReadView.Live,
    };

    /// <summary>
    /// An independent observer: a version-pinned read handle that grants <c>ReadWrite | Delete</c>, so it is compatible
    /// with any active writer (test plan §5). Observations go through this explicit handle instead of the
    /// <c>ReadAll*</c> helpers, whose sharing mode is not specified and may legitimately conflict with an open writer.
    /// </summary>
    public static DfsOpenOptions ObserverOptions() => new()
    {
        Mode = FileMode.Open,
        Access = FileAccess.Read,
        Share = FileShare.ReadWrite | FileShare.Delete,
        ReadView = DfsReadView.VersionPinned,
    };

    /// <summary>Reads the whole committed file through one pinned observer handle: one revision, bytes and revision together.</summary>
    public static async Task<(byte[] Bytes, ulong Revision)> ReadPinnedAsync(DfsVolume volume, string path)
    {
        await using DfsStream reader = await volume.Files.OpenAsync(path, ObserverOptions(), Ct);
        byte[] bytes = await ReadFromStartAsync(reader, Ct);
        return (bytes, reader.OpenRevision);
    }

    /// <summary>Connects a named client.</summary>
    public static Task<DfsClient> ConnectAsync(TestCluster cluster, string name, int? bufferSize = null, TimeSpan? operationTimeout = null) =>
        cluster.ConnectAsync(new TestClientOptions { Name = name, BufferSize = bufferSize, OperationTimeout = operationTimeout }, Ct);

    /// <summary>Creates the parent directory and writes <paramref name="content"/> as the committed starting state.</summary>
    public static async Task<CommitToken> SeedFileAsync(DfsVolume volume, string path, byte[] content)
    {
        string parent = DfsPath.GetParent(path);
        if (parent != DfsPath.Root)
            await volume.Directories.CreateDirectoryAsync(parent, Ct);
        return await volume.Files.WriteAllBytesAsync(path, content, Ct);
    }

    /// <summary>Drives one write overload.</summary>
    public static async Task WriteAsync(DfsStream stream, byte[] data, WriteKind kind, CancellationToken ct)
    {
        switch (kind)
        {
            case WriteKind.ArraySync:
                stream.Write(data, 0, data.Length);
                break;
            case WriteKind.SpanSync:
                stream.Write(data.AsSpan());
                break;
            case WriteKind.ArrayAsync:
                await stream.WriteAsync(data, 0, data.Length, ct);
                break;
            case WriteKind.MemoryAsync:
                await stream.WriteAsync(data.AsMemory(), ct);
                break;
            case WriteKind.ByteByByte:
                foreach (byte b in data)
                    stream.WriteByte(b);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, null);
        }
    }

    /// <summary>Reads the whole view of <paramref name="stream"/> from offset 0 (restoring nothing).</summary>
    public static async Task<byte[]> ReadFromStartAsync(DfsStream stream, CancellationToken ct)
    {
        stream.Seek(0, SeekOrigin.Begin);
        using var ms = new MemoryStream();
        var buffer = new byte[32 * 1024];
        int n;
        while ((n = await stream.ReadAsync(buffer.AsMemory(), ct)) > 0)
            ms.Write(buffer, 0, n);
        return ms.ToArray();
    }

    /// <summary>Awaits <paramref name="actual"/> and asserts byte equality with <paramref name="expected"/>.</summary>
    public static async Task AssertBytesAsync(byte[] expected, Task<byte[]> actual, string because)
    {
        byte[] bytes = await actual;
        AssertEx.BytesEqual(expected, bytes, because);
    }

    /// <summary>
    /// Asserts the committed bytes and length (and optionally the revision) seen by an independent observer. The bytes
    /// come from one pinned observer handle (<see cref="ObserverOptions"/>) and must belong to the revision GetInfo reports.
    /// </summary>
    public static async Task<DfsFileInfo> AssertCommittedAsync(DfsVolume observer, string path, byte[] expected, string because,
        ulong? expectedRevision = null)
    {
        (byte[] actual, ulong pinnedRevision) = await ReadPinnedAsync(observer, path);
        AssertEx.BytesEqual(expected, actual, because);
        DfsFileInfo info = await observer.Files.GetInfoAsync(path, Ct);
        Assert.True(expected.LongLength == info.Length, $"Expected length {expected.LongLength} but observed {info.Length}: {because}");
        Assert.True(pinnedRevision == info.Revision,
            $"The pinned read observed revision {pinnedRevision} but GetInfo reports {info.Revision} with no concurrent writer: {because}");
        if (expectedRevision is { } revision)
            Assert.True(revision == info.Revision, $"Expected revision {revision} but observed {info.Revision}: {because}");
        return info;
    }

    /// <summary>Objects referenced by the current revision of <paramref name="path"/> (empty for inline files).</summary>
    public static async Task<HashSet<ObjectId>> ReferencedObjectsAsync(TestCluster cluster, string path, string volume = Vol)
    {
        FileInspection? file = await cluster.Inspect.GetFileAsync(volume, path);
        Assert.True(file is not null, $"Inspector cannot resolve {path}.");
        return file.Extents.Select(e => e.Object).ToHashSet();
    }

    /// <summary>Asserts every object referenced by <paramref name="path"/> is durably held on at least the policy's ack copies/zones.</summary>
    public static async Task AssertDurableAsync(TestCluster cluster, string path, DfsStoragePolicy policy, string because)
    {
        FileInspection? file = await cluster.Inspect.GetFileAsync(Vol, path);
        Assert.True(file is not null, $"Inspector cannot resolve {path}: {because}");
        foreach (ExtentInspection extent in file.Extents)
        {
            (int copies, int zones) = await cluster.Inspect.CountDurableDomainsAsync(extent.Object);
            Assert.True(copies >= policy.DurableCopiesBeforeAck,
                $"Object {extent.Object} at offset {extent.Offset} has {copies} durable copies, policy requires {policy.DurableCopiesBeforeAck}: {because}");
            Assert.True(zones >= policy.DurableZonesBeforeAck,
                $"Object {extent.Object} at offset {extent.Offset} spans {zones} zones, policy requires {policy.DurableZonesBeforeAck}: {because}");
        }
    }

    /// <summary>
    /// Tries to take the writer reservation from another client. True when it was granted (and immediately released);
    /// false on a typed sharing/writer conflict.
    /// </summary>
    public static async Task<bool> TryOpenWriterAsync(DfsVolume volume, string path)
    {
        try
        {
            DfsStream probe = await volume.Files.OpenAsync(path, WriterOptions(FileMode.Open, access: FileAccess.Write), Ct);
            await probe.DisposeAsync();
            return true;
        }
        catch (DfsSharingViolationException)
        {
            return false;
        }
    }

    /// <summary>Publication requests (CommitFile/WriteAll/Append) sent by a client so far.</summary>
    public static long PublishRequests(TestCluster cluster, string clientName)
    {
        string from = "client:" + clientName;
        return cluster.Network.Log.CountMessages(new MessageFilter(From: from, Type: MessageType.CommitFile))
            + cluster.Network.Log.CountMessages(new MessageFilter(From: from, Type: MessageType.WriteAll))
            + cluster.Network.Log.CountMessages(new MessageFilter(From: from, Type: MessageType.Append));
    }

    /// <summary>Crashes every live node (abrupt, losing unflushed state) and restarts them from their durable images.</summary>
    public static async Task CrashAndRestartAllAsync(TestCluster cluster, CrashMode mode = CrashMode.LoseUnflushed)
    {
        foreach (TestNode node in cluster.Nodes)
            await node.CrashAsync(mode);
        foreach (TestNode node in cluster.Nodes)
            await node.RestartAsync();
        await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), Ct);
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
