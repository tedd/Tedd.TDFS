using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Simulation.Lay;

/// <summary>Shared setup and layout oracles for the LAY suite (deterministic simulation level).</summary>
internal static class LayHelpers
{
    public const string Vol = "vol";

    /// <summary>Default inline threshold of <see cref="TestVolumeSpec"/> (4 KiB).</summary>
    public const int DefaultInline = 4096;

    /// <summary>Logical update unit under <see cref="NodeTimings.Fast"/> (64 KiB).</summary>
    public static int Chunk => NodeTimings.Fast.ChunkSize;

    public static CancellationToken Ct => TestContext.Current.CancellationToken;

    public static DfsStoragePolicy Policy(int copies, int zones, int ackCopies, int ackZones) => new()
    {
        TargetCopies = copies,
        TargetDistinctZones = zones,
        DurableCopiesBeforeAck = ackCopies,
        DurableZonesBeforeAck = ackZones,
    };

    /// <summary>Three zones, one node each, deterministic simulation, one volume configured by <paramref name="configure"/>.</summary>
    public static TestClusterOptions Deterministic(Func<TestVolumeSpec, TestVolumeSpec>? configure = null)
    {
        var volume = new TestVolumeSpec(Vol);
        if (configure is not null)
            volume = configure(volume);
        return TestClusterOptions.ThreeZones with { Mode = ClusterMode.Deterministic, Volumes = [volume] };
    }

    public static async Task<FileInspection> InspectAsync(TestCluster cluster, string path)
    {
        FileInspection? file = await cluster.Inspect.GetFileAsync(Vol, path);
        Assert.NotNull(file);
        return file;
    }

    /// <summary>Structural layout invariants (see the unit-level twin).</summary>
    public static void AssertWellFormed(FileInspection file, string context = "")
    {
        if (file.IsInline)
        {
            Assert.True(file.Extents.Count == 0, $"{context}: inline file references extents");
            return;
        }

        long previousEnd = 0;
        foreach (ExtentInspection e in file.Extents)
        {
            Assert.True(e.Length > 0 && e.Offset >= previousEnd && e.Offset + e.Length <= file.Length,
                $"{context}: malformed extent [{e.Offset},{e.Offset + e.Length}) after {previousEnd}, file length {file.Length}");
            Assert.True(e.ObjectOffset >= 0 && e.ObjectOffset + e.Length <= e.ObjectLength,
                $"{context}: extent at {e.Offset} exceeds its object");
            previousEnd = e.Offset + e.Length;
        }
    }

    public static IReadOnlySet<ObjectId> Objects(FileInspection file) => file.Extents.Select(e => e.Object).ToHashSet();

    /// <summary>(object, object offset) storing logical byte <paramref name="offset"/>, or null.</summary>
    public static (ObjectId Object, long ObjectOffset)? PositionAt(FileInspection file, long offset)
    {
        foreach (ExtentInspection e in file.Extents)
        {
            if (offset >= e.Offset && offset < e.Offset + e.Length)
                return (e.Object, e.ObjectOffset + (offset - e.Offset));
        }

        return null;
    }

    public static async Task<byte[]> ReadCompatibleAsync(DfsVolume volume, string path)
    {
        await using DfsStream stream = await volume.Files.OpenAsync(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, Ct);
        using var output = new MemoryStream();
        await stream.CopyToAsync(output, 64 * 1024, Ct);
        return output.ToArray();
    }

    public static async Task WriteAtAsync(Stream stream, long offset, byte[] data)
    {
        stream.Seek(offset, SeekOrigin.Begin);
        await stream.WriteAsync(data.AsMemory(), Ct);
    }

    /// <summary>Objects stored anywhere in the volume right now.</summary>
    public static Task<IReadOnlySet<ObjectId>> StoredAsync(TestCluster cluster) => cluster.Inspect.GetAllStoredObjectsAsync(Vol);

    public static void Same(ReadOnlyMemory<byte> expected, ReadOnlyMemory<byte> actual, string? because = null) =>
        AssertEx.BytesEqual(expected.Span, actual.Span, because);

    public static void Log(string message) => TestContext.Current.TestOutputHelper?.WriteLine(message);
}
