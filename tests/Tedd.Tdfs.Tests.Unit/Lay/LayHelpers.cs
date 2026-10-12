using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Unit.Lay;

/// <summary>Shared setup and layout oracles for the LAY suite (unit level).</summary>
internal static class LayHelpers
{
    /// <summary>The volume every LAY scenario uses.</summary>
    public const string Vol = "vol";

    /// <summary>Logical update unit under <see cref="NodeTimings.Fast"/> (ARCHITECTURE §16: 64 KiB).</summary>
    public static int Chunk => NodeTimings.Fast.ChunkSize;

    /// <summary>Maximum object size under <see cref="NodeTimings.Fast"/> (1 MiB).</summary>
    public static int MaxObject => NodeTimings.Fast.MaxObjectSize;

    /// <summary>Default inline threshold of <see cref="TestVolumeSpec"/> (4 KiB).</summary>
    public const int DefaultInline = 4096;

    /// <summary>Per-unit encoding/verification granularity of stored objects (ARCHITECTURE §6/§11: 64 KiB units).</summary>
    public const int EncodingUnit = 64 * 1024;

    public static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Three zones, one node each, with one volume configured by <paramref name="configure"/>.</summary>
    public static TestClusterOptions ThreeZones(Func<TestVolumeSpec, TestVolumeSpec>? configure = null)
    {
        var volume = new TestVolumeSpec(Vol);
        if (configure is not null)
            volume = configure(volume);
        return TestClusterOptions.ThreeZones with { Volumes = [volume] };
    }

    /// <summary>The file's layout as seen by the volume leader; fails when the file does not exist.</summary>
    public static async Task<FileInspection> InspectAsync(TestCluster cluster, string path)
    {
        FileInspection? file = await cluster.Inspect.GetFileAsync(Vol, path);
        Assert.NotNull(file);
        return file;
    }

    /// <summary>
    /// Structural layout invariants: inline files carry no extents; extents are sorted, nonempty, non-overlapping, lie
    /// inside <c>[0, Length)</c>, and reference valid ranges of their objects.
    /// </summary>
    public static void AssertWellFormed(FileInspection file, string context = "")
    {
        Assert.True(file.Length >= 0, $"{context}: negative length {file.Length}");
        if (file.IsInline)
        {
            Assert.True(file.Extents.Count == 0, $"{context}: an inline file must not reference extents (has {file.Extents.Count})");
            return;
        }

        long previousEnd = 0;
        foreach (ExtentInspection e in file.Extents)
        {
            Assert.True(e.Length > 0, $"{context}: empty extent at {e.Offset}");
            Assert.True(e.Offset >= previousEnd, $"{context}: extent at {e.Offset} overlaps or precedes the previous extent ending at {previousEnd}");
            Assert.True(e.Offset + e.Length <= file.Length, $"{context}: extent [{e.Offset},{e.Offset + e.Length}) exceeds the file length {file.Length}");
            Assert.True(e.ObjectOffset >= 0 && e.ObjectOffset + e.Length <= e.ObjectLength,
                $"{context}: extent at {e.Offset} maps object range [{e.ObjectOffset},{e.ObjectOffset + e.Length}) outside object length {e.ObjectLength}");
            previousEnd = e.Offset + e.Length;
        }
    }

    /// <summary>Asserts that the extents cover <c>[start, end)</c> without gaps.</summary>
    public static void AssertCovers(FileInspection file, long start, long end, string context = "")
    {
        long cursor = start;
        foreach (ExtentInspection e in file.Extents.Where(e => e.Offset + e.Length > start && e.Offset < end).OrderBy(e => e.Offset))
        {
            Assert.True(e.Offset <= cursor, $"{context}: gap in extents at [{cursor},{e.Offset})");
            cursor = Math.Max(cursor, e.Offset + e.Length);
        }

        Assert.True(cursor >= end, $"{context}: extents stop at {cursor} before {end}");
    }

    /// <summary>The (object, object offset) that stores logical byte <paramref name="offset"/>, or null for a hole/inline byte.</summary>
    public static (ObjectId Object, long ObjectOffset)? PositionAt(FileInspection file, long offset)
    {
        foreach (ExtentInspection e in file.Extents)
        {
            if (offset >= e.Offset && offset < e.Offset + e.Length)
                return (e.Object, e.ObjectOffset + (offset - e.Offset));
        }

        return null;
    }

    /// <summary>Distinct objects referenced by the file's current mapping.</summary>
    public static IReadOnlySet<ObjectId> Objects(FileInspection file) => file.Extents.Select(e => e.Object).ToHashSet();

    /// <summary>Reads a whole file through a live handle opened with a sharing mode compatible with any writer, in <paramref name="bufferSize"/> pieces.</summary>
    public static async Task<byte[]> ReadCompatibleAsync(DfsVolume volume, string path, int bufferSize = 64 * 1024)
    {
        await using DfsStream stream = await volume.Files.OpenAsync(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, Ct);
        return await ReadToEndAsync(stream, bufferSize);
    }

    /// <summary>Reads from the current position to the end in <paramref name="bufferSize"/> pieces.</summary>
    public static async Task<byte[]> ReadToEndAsync(Stream stream, int bufferSize)
    {
        using var output = new MemoryStream();
        byte[] buffer = new byte[bufferSize];
        int read;
        while ((read = await stream.ReadAsync(buffer.AsMemory(), Ct)) > 0)
            output.Write(buffer, 0, read);
        return output.ToArray();
    }

    /// <summary>Reads exactly <paramref name="count"/> bytes at <paramref name="offset"/> (fewer only at EOF).</summary>
    public static async Task<byte[]> ReadRangeAsync(Stream stream, long offset, int count)
    {
        stream.Seek(offset, SeekOrigin.Begin);
        byte[] buffer = new byte[count];
        int total = 0;
        while (total < count)
        {
            int read = await stream.ReadAsync(buffer.AsMemory(total), Ct);
            if (read == 0)
                break;
            total += read;
        }

        return buffer.AsSpan(0, total).ToArray();
    }

    /// <summary>Seeks and writes.</summary>
    public static async Task WriteAtAsync(Stream stream, long offset, byte[] data)
    {
        stream.Seek(offset, SeekOrigin.Begin);
        await stream.WriteAsync(data.AsMemory(), Ct);
    }

    /// <summary>Concatenates byte arrays.</summary>
    public static byte[] Concat(params byte[][] parts) => parts.SelectMany(p => p).ToArray();

    /// <summary>Asserts that every byte of <paramref name="actual"/> in <c>[start, end)</c> is zero.</summary>
    public static void AssertZeros(byte[] actual, long start, long end, string context)
    {
        for (long i = start; i < end; i++)
        {
            if (actual[i] != 0)
                Assert.Fail($"{context}: expected a hole (zero) at offset {i} but found 0x{actual[i]:X2}");
        }
    }

    /// <summary>Byte-exact comparison usable with awaited arguments (spans cannot cross an await).</summary>
    public static void Same(ReadOnlyMemory<byte> expected, ReadOnlyMemory<byte> actual, string? because = null) =>
        AssertEx.BytesEqual(expected.Span, actual.Span, because);

    /// <summary>Writes a metric line to the test output.</summary>
    public static void Log(string message) => TestContext.Current.TestOutputHelper?.WriteLine(message);
}
