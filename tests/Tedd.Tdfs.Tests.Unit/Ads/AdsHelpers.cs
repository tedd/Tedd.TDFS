using System.Globalization;
using System.Text;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Unit.Ads;

/// <summary>
/// Shared helpers for the ADS unit rows (Addendum A §A4, alternate data streams). Every row runs against an in-memory
/// cluster through the public SDK; expected contents come from the test's own deterministic data (an independent
/// per-file stream model), never from the system under test.
/// </summary>
internal static class AdsHelpers
{
    /// <summary>The test volume created by every <see cref="TestClusterOptions"/> preset.</summary>
    public const string Vol = "vol";

    /// <summary>Inline threshold of <see cref="TestVolumeSpec"/> (default 4 KiB).</summary>
    public const int Inline = 4096;

    /// <summary>Default <see cref="DfsOpenOptions.BufferSize"/>.</summary>
    public const int StreamBuffer = 64 * 1024;

    /// <summary>Key of the default (unnamed) stream in a <see cref="StreamModel"/>.</summary>
    public const string DefaultStream = DfsStreamPath.DefaultStreamName;

    /// <summary>Logical update unit under <see cref="NodeTimings.Fast"/> (64 KiB).</summary>
    public static int Chunk => NodeTimings.Fast.ChunkSize;

    /// <summary>Bound for every liveness wait in these rows.</summary>
    public static readonly TimeSpan Bounded = TimeSpan.FromSeconds(30);

    public static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Three nodes, one per zone, three voters, target 3 / ack 2 in 2 zones (realtime, in-memory).</summary>
    public static TestClusterOptions Core => TestClusterOptions.ThreeZones;

    public static void Log(string message) => TestContext.Current.TestOutputHelper?.WriteLine(message);

    public static Task<DfsClient> ConnectAsync(TestCluster cluster, string name) =>
        cluster.ConnectAsync(new TestClientOptions { Name = name }, Ct);

    /// <summary><see cref="AssertEx.BytesEqual"/> over memory, so an argument list may contain an await.</summary>
    public static void BytesEqual(ReadOnlyMemory<byte> expected, ReadOnlyMemory<byte> actual, string because) =>
        AssertEx.BytesEqual(expected.Span, actual.Span, because);

    /// <summary>Deterministic bytes without any zero byte, so a zero-filled or resurrected range is always detectable.</summary>
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

    /// <summary>A writer that grants readers (writers may not grant <see cref="FileShare.Write"/>, LOCK-02).</summary>
    public static DfsOpenOptions Writer(FileMode mode = FileMode.Open, FileAccess access = FileAccess.Write,
        DfsWriteMode writeMode = DfsWriteMode.OnFlush, FileShare share = FileShare.Read) => new()
    {
        Mode = mode,
        Access = access,
        Share = share,
        WriteMode = writeMode,
    };

    /// <summary>Reads from position 0 to EOF.</summary>
    public static async Task<byte[]> ReadToEndAsync(Stream stream)
    {
        if (stream.CanSeek)
            stream.Seek(0, SeekOrigin.Begin);
        using var buffer = new MemoryStream();
        await stream.CopyToAsync(buffer, Ct);
        return buffer.ToArray();
    }

    /// <summary>
    /// Reads the committed contents of <paramref name="path"/> (file or stream path) as another client sees them,
    /// through a pinned reader that grants every share, so an open writer is never a setup-level sharing conflict.
    /// </summary>
    public static async Task<byte[]> ReadCommittedAsync(DfsVolume volume, string path)
    {
        await using DfsStream reader = await volume.Files.OpenAsync(path, Reader(DfsReadView.VersionPinned), Ct);
        return await ReadToEndAsync(reader);
    }

    public static async Task<Exception?> CatchAsync(Func<Task> action)
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

    public static Func<Task> Sync(Action action) => () =>
    {
        action();
        return Task.CompletedTask;
    };

    public static string Describe(Exception? ex) => ex is null
        ? "no exception (the operation succeeded)"
        : $"{ex.GetType().FullName}{(ex is DfsException d ? $" [{d.ErrorCode}]" : string.Empty)}: {ex.Message}";

    /// <summary>
    /// An invalid path surfaces as an <see cref="ArgumentException"/> that identifies the path (FSD §13, ADS-01): it maps to
    /// <see cref="DfsErrorCode.InvalidPath"/> (canonical ParamName "path" or a tagged code), or — the ordinary .NET
    /// convention for two-path operations, as accepted by the PATH suite — names the method's "sourcePath" or
    /// "destinationPath" parameter.
    /// </summary>
    public static void AssertInvalidPath(Exception? ex, string because)
    {
        if (ex is not ArgumentException argument)
        {
            Assert.Fail($"{because}: expected an ArgumentException (invalid path) but got {Describe(ex)}");
            return;
        }

        Assert.True(DfsErrors.ToCode(argument) == DfsErrorCode.InvalidPath || argument.ParamName is "path" or "sourcePath" or "destinationPath",
            $"{because}: the argument exception must identify the path (InvalidPath, or ParamName path/sourcePath/destinationPath); got code {DfsErrors.ToCode(argument)}, ParamName '{argument.ParamName}' ({Describe(ex)})");
    }

    /// <summary>
    /// Absence of a path whose parent directory is itself gone: FSD §13 maps a missing file to
    /// <see cref="FileNotFoundException"/> and a missing directory to <see cref="DirectoryNotFoundException"/>; either is
    /// a correct report when the whole parent was removed.
    /// </summary>
    public static void AssertFileOrParentNotFound(Exception? ex, string because) =>
        Assert.True(ex is FileNotFoundException or DirectoryNotFoundException,
            $"{because}: expected FileNotFoundException or DirectoryNotFoundException but got {Describe(ex)}");

    /// <summary>A sharing/writer conflict surfaces as a retryable <see cref="DfsSharingViolationException"/> (FSD §13, LOCK-03).</summary>
    public static void AssertSharingViolation(Exception? ex, string because)
    {
        if (ex is not DfsSharingViolationException conflict)
        {
            Assert.Fail($"{because}: expected DfsSharingViolationException but got {Describe(ex)}");
            return;
        }

        Assert.True(conflict.ErrorCode is DfsErrorCode.SharingViolation or DfsErrorCode.WriterConflict,
            $"{because}: expected SharingViolation/WriterConflict, got {conflict.ErrorCode}");
        Assert.True(conflict.IsRetryable, $"{because}: sharing conflicts are retryable");
    }

    public static void AssertFileNotFound(Exception? ex, string because) =>
        Assert.True(ex is FileNotFoundException, $"{because}: expected FileNotFoundException but got {Describe(ex)}");

    /// <summary>The leader's applied view of a file (default stream + named streams); fails when absent.</summary>
    public static async Task<FileInspection> InspectFileAsync(TestCluster cluster, string filePath)
    {
        FileInspection? file = await cluster.Inspect.GetFileAsync(Vol, filePath);
        Assert.True(file is not null, $"{filePath} must exist in the leader's applied state");
        return file!;
    }

    /// <summary>The applied view of one named stream; fails when the file or the stream is absent.</summary>
    public static async Task<StreamInspection> InspectStreamAsync(TestCluster cluster, string filePath, string streamName)
    {
        FileInspection file = await InspectFileAsync(cluster, filePath);
        StreamInspection? stream = file.Streams.SingleOrDefault(s => string.Equals(s.Name, streamName, StringComparison.Ordinal));
        Assert.True(stream is not null, $"{filePath} must have the named stream '{streamName}' in the applied state; it has [{string.Join(", ", file.Streams.Select(s => s.Name))}]");
        return stream!;
    }

    public static HashSet<ObjectId> Objects(IEnumerable<ExtentInspection> extents) => extents.Select(e => e.Object).ToHashSet();

    /// <summary>Every object referenced by any stream (default and named) of the file.</summary>
    public static HashSet<ObjectId> AllObjects(FileInspection file)
    {
        HashSet<ObjectId> set = Objects(file.Extents);
        foreach (StreamInspection stream in file.Streams)
            set.UnionWith(Objects(stream.Extents));
        return set;
    }

    /// <summary>
    /// Asserts the complete stream model of a file through the SDK: default-stream bytes, the exact named-stream set
    /// (ListStreams, ordinal by name, exact lengths) and every named stream's bytes.
    /// </summary>
    public static async Task AssertStreamsAsync(DfsVolume volume, string filePath, StreamModel expected, string because)
    {
        BytesEqual(expected.Default, await volume.Files.ReadAllBytesAsync(filePath, Ct), $"{because}: default stream of {filePath}");
        IReadOnlyList<DfsStreamInfo> listed = await volume.Files.ListStreamsAsync(filePath, Ct);
        string[] expectedNames = expected.NamedStreams.Keys.ToArray();
        string[] actualNames = listed.Select(s => s.Name).ToArray();
        Assert.True(expectedNames.SequenceEqual(actualNames, StringComparer.Ordinal),
            $"{because}: named streams of {filePath} are [{string.Join(", ", actualNames)}], expected (ordinal) [{string.Join(", ", expectedNames)}]");
        foreach (DfsStreamInfo info in listed)
        {
            byte[] bytes = expected.NamedStreams[info.Name];
            Assert.True(info.Length == bytes.Length, $"{because}: ListStreams reports {filePath}:{info.Name} with length {info.Length}, expected {bytes.Length}");
            BytesEqual(bytes, await volume.Files.ReadAllBytesAsync(DfsStreamPath.Combine(filePath, info.Name), Ct), $"{because}: {filePath}:{info.Name}");
        }
    }

    /// <summary>
    /// A deterministic text description of the whole namespace (entries with inode, revision and length, and every
    /// entry's named streams with revision and length). Two equal descriptions mean nothing observable changed.
    /// </summary>
    public static async Task<string> DescribeNamespaceAsync(DfsVolume volume)
    {
        var sb = new StringBuilder();
        await DescribeDirectoryAsync(volume, DfsPath.Root, sb);
        return sb.ToString();
    }

    private static async Task DescribeDirectoryAsync(DfsVolume volume, string directory, StringBuilder sb)
    {
        await foreach (DfsEntryInfo entry in volume.Directories.EnumerateAsync(directory, null, Ct))
        {
            sb.Append(CultureInfo.InvariantCulture, $"{entry.Path}|{entry.Kind}|{entry.InodeId.Value}|{entry.Revision}|{entry.Length}\n");
            foreach (DfsStreamInfo stream in await volume.Files.ListStreamsAsync(entry.Path, Ct))
                sb.Append(CultureInfo.InvariantCulture, $"  :{stream.Name}|{stream.Revision}|{stream.Length}\n");
            if (entry.Kind == DfsEntryKind.Directory)
                await DescribeDirectoryAsync(volume, entry.Path, sb);
        }
    }

    /// <summary>Names of the entries directly under <paramref name="directory"/>, in enumeration order.</summary>
    public static async Task<List<DfsEntryInfo>> EntriesAsync(DfsVolume volume, string directory, DfsEnumerationOptions? options = null)
    {
        var entries = new List<DfsEntryInfo>();
        await foreach (DfsEntryInfo entry in volume.Directories.EnumerateAsync(directory, options, Ct))
            entries.Add(entry);
        return entries;
    }

    /// <summary>Writes a file's default stream and every named stream of <paramref name="model"/> (one helper call each).</summary>
    public static async Task<Dictionary<string, CommitToken>> WriteModelAsync(DfsVolume volume, string filePath, StreamModel model)
    {
        var tokens = new Dictionary<string, CommitToken>(StringComparer.Ordinal)
        {
            [DefaultStream] = await volume.Files.WriteAllBytesAsync(filePath, model.Default, Ct),
        };
        foreach (KeyValuePair<string, byte[]> stream in model.NamedStreams)
            tokens[stream.Key] = await volume.Files.WriteAllBytesAsync(DfsStreamPath.Combine(filePath, stream.Key), stream.Value, Ct);
        return tokens;
    }

    /// <summary>
    /// Creates <paramref name="path"/> (file or stream path) with <paramref name="data"/> published one
    /// <see cref="Chunk"/>-sized piece per flush, so every chunk is a separate published object.
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

    /// <summary>Asserts every byte in <paramref name="data"/> is zero.</summary>
    public static void AssertAllZero(ReadOnlySpan<byte> data, long baseOffset, string because)
    {
        int index = data.IndexOfAnyExcept((byte)0);
        Assert.True(index < 0, $"Expected zeroes but found 0x{(index < 0 ? 0 : data[index]):X2} at offset {baseOffset + Math.Max(0, index)} ({because}).");
    }

    /// <summary>Applies an overwrite of <paramref name="data"/> at <paramref name="offset"/> to a model copy.</summary>
    public static byte[] Overlay(byte[] current, long offset, byte[] data)
    {
        var result = new byte[Math.Max(current.Length, offset + data.Length)];
        current.CopyTo(result, 0);
        data.CopyTo(result, offset);
        return result;
    }

    /// <summary>Returns <paramref name="current"/> resized to <paramref name="length"/> (zero-extended or clipped).</summary>
    public static byte[] Resize(byte[] current, long length)
    {
        var result = new byte[length];
        current.AsSpan(0, (int)Math.Min(current.Length, length)).CopyTo(result);
        return result;
    }
}

/// <summary>Independent expected view of one file: default-stream bytes plus named streams (ordinal by name).</summary>
internal sealed class StreamModel
{
    public StreamModel(byte[] defaultStream) => Default = defaultStream.ToArray();

    public byte[] Default { get; set; }

    public SortedDictionary<string, byte[]> NamedStreams { get; } = new(StringComparer.Ordinal);

    public StreamModel With(string name, byte[] data)
    {
        NamedStreams[name] = data.ToArray();
        return this;
    }

    public StreamModel Clone()
    {
        var copy = new StreamModel(Default);
        foreach (KeyValuePair<string, byte[]> stream in NamedStreams)
            copy.NamedStreams[stream.Key] = stream.Value.ToArray();
        return copy;
    }
}
