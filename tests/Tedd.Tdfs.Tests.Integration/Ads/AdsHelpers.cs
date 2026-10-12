using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Integration.Ads;

/// <summary>Shared helpers for the ADS integration rows (real QUIC transport and on-disk storage).</summary>
internal static class AdsHelpers
{
    public const string Vol = "vol";

    public static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Three zones, one node each, over QUIC with on-disk storage (real transport/persistence boundaries).</summary>
    public static TestClusterOptions RealBoundaries() => TestClusterOptions.ThreeZones with
    {
        Transport = TransportKind.Quic,
        Storage = StorageKind.Disk,
        ScenarioTimeout = TimeSpan.FromMinutes(6),
    };

    public static void Log(string message) => TestContext.Current.TestOutputHelper?.WriteLine(message);

    public static Task<DfsClient> ConnectAsync(TestCluster cluster, string name, IReadOnlyList<string>? seeds = null) =>
        cluster.ConnectAsync(new TestClientOptions { Name = name, SeedNodes = seeds }, Ct);

    public static void BytesEqual(byte[] expected, byte[] actual, string because) => AssertEx.BytesEqual(expected, actual, because);

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

    public static HashSet<ObjectId> AllObjects(FileInspection file)
    {
        HashSet<ObjectId> set = file.Extents.Select(e => e.Object).ToHashSet();
        foreach (StreamInspection stream in file.Streams)
            set.UnionWith(stream.Extents.Select(e => e.Object));
        return set;
    }

    public static async Task WriteModelAsync(DfsFileClient files, string filePath, StreamModel model)
    {
        await files.WriteAllBytesAsync(filePath, model.Default, Ct);
        foreach (KeyValuePair<string, byte[]> stream in model.NamedStreams)
            await files.WriteAllBytesAsync(DfsStreamPath.Combine(filePath, stream.Key), stream.Value, Ct);
    }

    /// <summary>Default-stream bytes, the exact named-stream set (ordinal, lengths) and every named stream's bytes.</summary>
    public static async Task AssertStreamsAsync(DfsFileClient files, string filePath, StreamModel expected, string because)
    {
        BytesEqual(expected.Default, await files.ReadAllBytesAsync(filePath, Ct), $"{because}: default stream of {filePath}");
        IReadOnlyList<DfsStreamInfo> listed = await files.ListStreamsAsync(filePath, Ct);
        Assert.True(expected.NamedStreams.Keys.SequenceEqual(listed.Select(s => s.Name), StringComparer.Ordinal),
            $"{because}: named streams of {filePath} are [{string.Join(", ", listed.Select(s => s.Name))}], expected [{string.Join(", ", expected.NamedStreams.Keys)}]");
        foreach (DfsStreamInfo info in listed)
        {
            byte[] bytes = expected.NamedStreams[info.Name];
            Assert.True(info.Length == bytes.Length, $"{because}: {filePath}:{info.Name} length {info.Length}, expected {bytes.Length}");
            BytesEqual(bytes, await files.ReadAllBytesAsync(DfsStreamPath.Combine(filePath, info.Name), Ct), $"{because}: {filePath}:{info.Name}");
        }
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
}
