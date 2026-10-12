using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Simulation.Prop;

/// <summary>Shared infrastructure for the PROP suite (generators, seeded property runner, oracles, shrinker).</summary>
internal static class PropHelpers
{
    /// <summary>Volume used by every PROP scenario.</summary>
    public const string Vol = "vol";

    /// <summary>Inode number of the root in <see cref="ReferenceNamespaceModel"/> (API.md §F: "root inode 1").</summary>
    public const long RootModelInode = 1;

    /// <summary>Inline threshold of the test volume (TestVolumeSpec default).</summary>
    public const int InlineThreshold = 4096;

    /// <summary>Default stream buffer size (DfsOpenOptions default).</summary>
    public const int BufferSize = 64 * 1024;

    /// <summary>Logical update unit of <see cref="NodeTimings.Fast"/> (64 KiB).</summary>
    public static int ChunkSize => NodeTimings.Fast.ChunkSize;

    /// <summary>The xunit cancellation token of the running test.</summary>
    public static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Deterministic-simulation options derived from <paramref name="baseOptions"/> (ThreeZones by default) with the property seed.</summary>
    public static TestClusterOptions Sim(ulong seed, TestClusterOptions? baseOptions = null) =>
        (baseOptions ?? TestClusterOptions.ThreeZones) with { Mode = ClusterMode.Deterministic, Seed = seed };

    /// <summary>Writes a line to the test output.</summary>
    public static void Log(string line) => TestContext.Current.TestOutputHelper?.WriteLine(line);

    /// <summary>
    /// Runs one seeded property execution. Any failure is rethrown with the test id, the seed (the cluster seed and the
    /// generator seed are the same value) and the tail of the generated step trace so the history can be replayed.
    /// </summary>
    public static async Task RunPropertyAsync(string testId, ulong seed, Func<PropertyTrace, Task> body)
    {
        var trace = new PropertyTrace(testId, seed);
        try
        {
            await body(trace);
        }
        catch (Exception ex)
        {
            string message = $"{testId} property failed for seed {seed} (0x{seed:X16}) after {trace.Count} generated steps; " +
                $"replay with the same seed. Last steps:{Environment.NewLine}{trace.Tail()}";
            Log(message);
            throw new Xunit.Sdk.XunitException(message, ex);
        }
    }

    /// <summary>Reads up to <paramref name="count"/> bytes at <paramref name="offset"/> (short reads are continued until EOF).</summary>
    public static async Task<byte[]> ReadRangeAsync(Stream stream, long offset, int count)
    {
        stream.Seek(offset, SeekOrigin.Begin);
        var buffer = new byte[count];
        int total = 0;
        while (total < count)
        {
            int n = await stream.ReadAsync(buffer.AsMemory(total, count - total), Ct);
            if (n == 0)
                break;
            Assert.True(n > 0 && n <= count - total, $"ReadAsync returned {n} for a request of {count - total} bytes");
            total += n;
        }

        Assert.Equal(offset + total, stream.Position);
        return buffer.AsSpan(0, total).ToArray();
    }

    /// <summary>Reads a whole file, or null when it does not exist (genuine absence only).</summary>
    public static async Task<byte[]?> TryReadAllBytesAsync(DfsFileClient files, string path)
    {
        try
        {
            return await files.ReadAllBytesAsync(path, Ct);
        }
        catch (FileNotFoundException)
        {
            return null;
        }
    }

    /// <summary>Asserts that <paramref name="action"/> fails with one of the allowed canonical error codes and returns the exception.</summary>
    public static async Task<Exception> ExpectFailureAsync(Func<Task> action, string because, params DfsErrorCode[] allowed)
    {
        try
        {
            await action();
        }
        catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
        {
            DfsErrorCode code = DfsErrors.ToCode(ex);
            Assert.True(allowed.Contains(code),
                $"{because}: expected one of [{string.Join(", ", allowed)}] but got {code} ({ex.GetType().Name}: {ex.Message})");
            return ex;
        }

        Assert.Fail($"{because}: expected a failure with one of [{string.Join(", ", allowed)}] but the operation succeeded.");
        throw new InvalidOperationException("unreachable");
    }

    /// <summary>Awaits <paramref name="actual"/> and compares it with <paramref name="expected"/> (first mismatch offset reported).</summary>
    public static async Task BytesEqualAsync(byte[] expected, Task<byte[]> actual, string because)
    {
        byte[] bytes = await actual;
        AssertEx.BytesEqual(expected, bytes, because);
    }

    /// <summary>Hex SHA-256 of a byte array (for compact boundary comparisons).</summary>
    public static string Hash(ReadOnlySpan<byte> data) => Convert.ToHexString(SHA256.HashData(data))[..16];

    /// <summary>Deterministic content for (seed, id, length).</summary>
    public static byte[] Content(ulong seed, int id, int length) =>
        TestData.Bytes(Derive(seed, "content-" + id.ToString(CultureInfo.InvariantCulture)), length);

    /// <summary>Derives an independent deterministic seed for <paramref name="purpose"/> (via the public <see cref="IRandomSource.Fork"/>).</summary>
    public static ulong Derive(ulong seed, string purpose) => new SeededRandom(seed).Fork(purpose).NextUInt64();

    /// <summary>Folds two seeds into one reportable seed (PROP-007 cross product).</summary>
    public static ulong Combine(ulong a, ulong b) => Derive(a, "x" + b.ToString(CultureInfo.InvariantCulture));

    /// <summary>"length:hash" of a byte array (publication-boundary fingerprint).</summary>
    public static string Boundary(ReadOnlySpan<byte> bytes) => bytes.Length.ToString(CultureInfo.InvariantCulture) + ":" + Hash(bytes);

    /// <summary>Captures the namespace+content image of a volume (or view) and returns it with its digest.</summary>
    public static async Task<VolumeImage> CaptureAsync(DfsVolume volume) => await VolumeImage.CaptureAsync(volume.Files, volume.Directories);

    /// <summary>Asserts that a captured image equals the expected one (bytes, kinds, paths; optionally inode identities).</summary>
    public static void AssertImage(VolumeImage expected, VolumeImage actual, bool compareInodes, string because)
    {
        string? diff = actual.Diff(expected, compareInodes);
        Assert.True(diff is null, $"{because}: {diff}");
    }
}

/// <summary>Step trace of one generated history (reported with the seed on failure).</summary>
internal sealed class PropertyTrace(string testId, ulong seed)
{
    private readonly List<string> _steps = [];

    public string TestId { get; } = testId;
    public ulong Seed { get; } = seed;
    public int Count => _steps.Count;
    public IReadOnlyList<string> Steps => _steps;

    public void Step(string description) => _steps.Add($"{_steps.Count + 1}. {description}");

    public string Tail(int count = 40) => string.Join(Environment.NewLine, _steps.TakeLast(count));
}

/// <summary>Monotonic logical clock for transaction histories (single counter, like HistoryRecorder's).</summary>
internal sealed class LogicalClock
{
    private long _now;

    public long Tick() => Interlocked.Increment(ref _now);
}

/// <summary>
/// Seeded generator of lengths and offsets biased towards boundary values (0, 1, inline threshold ±1, chunk ±1,
/// buffer ±1, 2×chunk ±1, the current end of file ±1 and any extra limits) rather than mostly easy random values (PROP-013).
/// </summary>
internal sealed class BoundaryBiasedSizes
{
    private readonly IRandomSource _random;
    private readonly int _inline;
    private readonly int _chunk;
    private readonly int _buffer;
    private readonly Dictionary<string, int> _classes = new(StringComparer.Ordinal);

    public BoundaryBiasedSizes(IRandomSource random, int inlineThreshold, int chunkSize, int bufferSize, IEnumerable<long>? extraBoundaries = null)
    {
        _random = random;
        _inline = inlineThreshold;
        _chunk = chunkSize;
        _buffer = bufferSize;
        var set = new SortedSet<long>(TestData.BoundaryLengths(inlineThreshold, chunkSize, bufferSize));
        foreach (long extra in extraBoundaries ?? [])
        {
            for (long d = -1; d <= 1; d++)
            {
                if (extra + d >= 0)
                    set.Add(extra + d);
            }
        }

        Boundaries = set.ToArray();
    }

    /// <summary>Sorted boundary values the generator is biased towards.</summary>
    public IReadOnlyList<long> Boundaries { get; }

    /// <summary>Percentage of draws taken from the boundary set (the rest are uniform).</summary>
    public int BoundaryPercent { get; init; } = 70;

    /// <summary>Number of generated values.</summary>
    public int Generated { get; private set; }

    /// <summary>Counts of generated values by boundary class (see <see cref="Classify"/>).</summary>
    public IReadOnlyDictionary<string, int> Classes => _classes;

    /// <summary>A length in [0, max], biased to boundaries.</summary>
    public long NextLength(long max)
    {
        long value;
        if (max <= 0)
        {
            value = 0;
        }
        else if (_random.Next(100) < BoundaryPercent)
        {
            long[] candidates = Boundaries.Where(b => b <= max).ToArray();
            value = candidates.Length == 0 ? 0 : candidates[_random.Next(candidates.Length)];
        }
        else
        {
            value = Uniform(max);
        }

        Record(value, endOfFile: null);
        return value;
    }

    /// <summary>An offset in [0, max], biased to boundaries and to the current end of file ±1.</summary>
    public long NextOffset(long currentLength, long max)
    {
        long value;
        if (max <= 0)
        {
            value = 0;
        }
        else if (_random.Next(100) < BoundaryPercent)
        {
            var candidates = new List<long>(Boundaries.Where(b => b <= max));
            foreach (long eof in new[] { currentLength - 1, currentLength, currentLength + 1 })
            {
                if (eof >= 0 && eof <= max)
                {
                    candidates.Add(eof);
                    candidates.Add(eof); // the end of file is the most interesting position
                }
            }

            value = candidates.Count == 0 ? 0 : candidates[_random.Next(candidates.Count)];
        }
        else
        {
            value = Uniform(max);
        }

        Record(value, currentLength);
        return value;
    }

    private long Uniform(long max) => max >= int.MaxValue ? (long)(_random.NextUInt64() % (ulong)(max + 1)) : _random.Next((int)max + 1);

    private void Record(long value, long? endOfFile)
    {
        Generated++;
        string cls = Classify(value, _inline, _chunk, _buffer);
        if (cls == "other" && endOfFile is long eof && Math.Abs(value - eof) <= 1)
            cls = value == eof ? "eof" : value < eof ? "eof-1" : "eof+1";
        _classes[cls] = _classes.GetValueOrDefault(cls) + 1;
        if (endOfFile is long e && Math.Abs(value - e) <= 1)
        {
            string eofClass = value == e ? "eof" : value < e ? "eof-1" : "eof+1";
            if (eofClass != cls)
                _classes[eofClass] = _classes.GetValueOrDefault(eofClass) + 1;
        }
    }

    /// <summary>Boundary class of a value: "0", "1", "inline-1", "inline", "inline+1", "chunk±", "2chunk±", "buffer±" or "other".</summary>
    public static string Classify(long value, int inlineThreshold, int chunkSize, int bufferSize)
    {
        if (value == 0)
            return "0";
        if (value == 1)
            return "1";
        foreach ((string name, long pivot) in new[] { ("inline", (long)inlineThreshold), ("chunk", chunkSize), ("buffer", bufferSize), ("2chunk", 2L * chunkSize) })
        {
            if (value == pivot - 1)
                return name + "-1";
            if (value == pivot)
                return name;
            if (value == pivot + 1)
                return name + "+1";
        }

        return "other";
    }

    /// <summary>Overflow and limit edge cases exercised as negative inputs (PROP-013).</summary>
    public static IReadOnlyList<(string Name, long Value)> EdgeCases(DfsVolumeLimits limits) =>
    [
        ("negative", -1),
        ("long.MaxValue", long.MaxValue),
        ("long.MaxValue-1", long.MaxValue - 1),
        ("int.MaxValue+1", (long)int.MaxValue + 1),
        ("maxFileLength-1", limits.MaxFileLength - 1),
        ("maxFileLength", limits.MaxFileLength),
        ("maxFileLength+1", limits.MaxFileLength + 1),
        ("maxPublishBytes-1", limits.MaxPublishBytes - 1),
        ("maxPublishBytes", limits.MaxPublishBytes),
        ("maxPublishBytes+1", limits.MaxPublishBytes + 1),
        ("txStagedBytes-1", limits.MaxTransactionStagedBytes - 1),
        ("txStagedBytes", limits.MaxTransactionStagedBytes),
        ("txStagedBytes+1", limits.MaxTransactionStagedBytes + 1),
    ];
}

/// <summary>One entry of a captured namespace image.</summary>
internal sealed record ImageEntry(DfsEntryKind Kind, byte[]? Content, InodeId Inode);

/// <summary>A complete namespace + content image of a volume or snapshot view.</summary>
internal sealed class VolumeImage
{
    public SortedDictionary<string, ImageEntry> Entries { get; } = new(StringComparer.Ordinal);

    /// <summary>Captures every directory and file (with full contents) reachable from "/".</summary>
    public static async Task<VolumeImage> CaptureAsync(DfsFileClient files, DfsDirectoryClient directories)
    {
        var image = new VolumeImage();
        var pending = new Queue<string>();
        pending.Enqueue("/");
        while (pending.Count > 0)
        {
            string directory = pending.Dequeue();
            await foreach (DfsEntryInfo entry in directories.EnumerateAsync(directory, null, PropHelpers.Ct))
            {
                Assert.Equal(DfsPath.Combine(directory, entry.Name), entry.Path);
                if (entry.Kind == DfsEntryKind.Directory)
                {
                    image.Entries[entry.Path] = new ImageEntry(DfsEntryKind.Directory, null, entry.InodeId);
                    pending.Enqueue(entry.Path);
                }
                else
                {
                    byte[] content = await files.ReadAllBytesAsync(entry.Path, PropHelpers.Ct);
                    Assert.Equal(entry.Length, content.LongLength);
                    image.Entries[entry.Path] = new ImageEntry(DfsEntryKind.File, content, entry.InodeId);
                }
            }
        }

        return image;
    }

    /// <summary>Builds the expected image of a <see cref="TreeModel"/> (inodes unknown, compared by content only).</summary>
    public static VolumeImage FromModel(TreeModel model)
    {
        var image = new VolumeImage();
        foreach (string dir in model.Directories)
            image.Entries[dir] = new ImageEntry(DfsEntryKind.Directory, null, default);
        foreach ((string path, byte[] content) in model.Files)
            image.Entries[path] = new ImageEntry(DfsEntryKind.File, content, default);
        return image;
    }

    /// <summary>Null when equal, otherwise the first difference (paths, kinds, bytes and optionally inode identities).</summary>
    public string? Diff(VolumeImage expected, bool compareInodes)
    {
        foreach ((string path, ImageEntry exp) in expected.Entries)
        {
            if (!Entries.TryGetValue(path, out ImageEntry? act))
                return $"missing {exp.Kind} {path}";
            if (act.Kind != exp.Kind)
                return $"{path}: kind {act.Kind}, expected {exp.Kind}";
            if (exp.Kind == DfsEntryKind.File && !act.Content!.AsSpan().SequenceEqual(exp.Content))
                return $"{path}: content differs (length {act.Content!.Length} vs expected {exp.Content!.Length}, sha {PropHelpers.Hash(act.Content)} vs {PropHelpers.Hash(exp.Content)})";
            if (compareInodes && act.Inode != exp.Inode)
                return $"{path}: inode {act.Inode.Value}, expected {exp.Inode.Value}";
        }

        foreach (string path in Entries.Keys)
        {
            if (!expected.Entries.ContainsKey(path))
                return $"unexpected entry {path}";
        }

        return null;
    }

    /// <summary>Content digest of the whole image (paths, kinds and bytes; no inode numbers).</summary>
    public string Digest()
    {
        var sb = new StringBuilder();
        foreach ((string path, ImageEntry e) in Entries)
            sb.Append(path).Append(e.Kind == DfsEntryKind.Directory ? "/" : ":" + PropHelpers.Hash(e.Content)).Append('|');
        return PropHelpers.Hash(Encoding.UTF8.GetBytes(sb.ToString()));
    }
}

/// <summary>
/// Minimal independent namespace+content model used as the oracle for metamorphic histories (PROP-005/009/010/011/012).
/// Paths must be normalised; the root is implicit.
/// </summary>
internal sealed class TreeModel
{
    public SortedDictionary<string, byte[]> Files { get; } = new(StringComparer.Ordinal);
    public SortedSet<string> Directories { get; } = new(StringComparer.Ordinal);

    public TreeModel Clone()
    {
        var copy = new TreeModel();
        foreach ((string p, byte[] c) in Files)
            copy.Files[p] = c;
        foreach (string d in Directories)
            copy.Directories.Add(d);
        return copy;
    }

    public void MkDirs(string path)
    {
        if (path == "/")
            return;
        foreach (string part in Ancestors(path).Append(path))
            Directories.Add(part);
    }

    public void Write(string path, byte[] content)
    {
        MkDirs(DfsPath.GetParent(path));
        Files[path] = content;
    }

    public void Append(string path, byte[] suffix) =>
        Files[path] = Files.TryGetValue(path, out byte[]? old) ? [.. old, .. suffix] : suffix;

    public bool DeleteFile(string path) => Files.Remove(path);

    public void DeleteTree(string path)
    {
        foreach (string f in Files.Keys.Where(p => DfsPath.IsAncestorOrSelf(path, p)).ToArray())
            Files.Remove(f);
        foreach (string d in Directories.Where(p => DfsPath.IsAncestorOrSelf(path, p)).ToArray())
            Directories.Remove(d);
    }

    public void MoveFile(string from, string to)
    {
        byte[] content = Files[from];
        Files.Remove(from);
        Files[to] = content;
    }

    public bool IsEmptyDirectory(string path) =>
        !Files.Keys.Any(p => p != path && DfsPath.IsAncestorOrSelf(path, p)) && !Directories.Any(p => p != path && DfsPath.IsAncestorOrSelf(path, p));

    private static IEnumerable<string> Ancestors(string path)
    {
        if (path == "/")
            yield break;
        IReadOnlyList<string> parts = DfsPath.Split(path);
        string current = "/";
        for (int i = 0; i < parts.Count - 1; i++)
        {
            current = DfsPath.Combine(current, parts[i]);
            yield return current;
        }
    }
}

/// <summary>Delta-debugging shrinker for failing generated histories (PROP-015).</summary>
internal static class Shrinker
{
    /// <summary>
    /// Reduces <paramref name="failing"/> to a 1-minimal sub-sequence that still produces exactly the same violation
    /// signature (never a different failure). Returns the minimal history and the number of oracle evaluations.
    /// </summary>
    public static async Task<(IReadOnlyList<T> Minimal, int Evaluations)> ShrinkAsync<T>(
        IReadOnlyList<T> failing, Func<IReadOnlyList<T>, Task<string?>> violationSignature, int maxEvaluations = 400)
    {
        string original = await violationSignature(failing)
            ?? throw new ArgumentException("The history to shrink does not reproduce a violation.", nameof(failing));
        int evaluations = 1;
        var current = failing.ToList();
        int granularity = 2;
        while (current.Count >= 2 && evaluations < maxEvaluations)
        {
            int chunk = (int)Math.Ceiling(current.Count / (double)granularity);
            bool reduced = false;
            for (int start = 0; start < current.Count && evaluations < maxEvaluations; start += chunk)
            {
                List<T> candidate = current.Take(start).Concat(current.Skip(start + chunk)).ToList();
                if (candidate.Count == 0)
                    continue;
                evaluations++;
                if (await violationSignature(candidate) == original)
                {
                    current = candidate;
                    granularity = Math.Max(granularity - 1, 2);
                    reduced = true;
                    break;
                }
            }

            if (!reduced)
            {
                if (granularity >= current.Count)
                    break;
                granularity = Math.Min(current.Count, granularity * 2);
            }
        }

        // Final pass: 1-minimality (no single element can be removed).
        for (int i = 0; i < current.Count && evaluations < maxEvaluations;)
        {
            List<T> candidate = current.Where((_, k) => k != i).ToList();
            evaluations++;
            if (candidate.Count > 0 && await violationSignature(candidate) == original)
                current = candidate;
            else
                i++;
        }

        return (current, evaluations);
    }
}

/// <summary>One step of a PROP-015 append/retry schedule.</summary>
internal sealed record ScheduleOp(string Kind, int File, int Data)
{
    public static readonly string[] Kinds = ["append", "retry", "write", "read"];

    public override string ToString() => $"{Kind} f{File} d{Data}";

    public static ScheduleOp Parse(string line)
    {
        string[] parts = line.Trim().Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length != 3 || !Kinds.Contains(parts[0]) || parts[1][0] != 'f' || parts[2][0] != 'd')
            throw new FormatException($"Malformed schedule step '{line}'.");
        return new ScheduleOp(parts[0], int.Parse(parts[1][1..], CultureInfo.InvariantCulture), int.Parse(parts[2][1..], CultureInfo.InvariantCulture));
    }
}

/// <summary>A named, retained regression schedule (PROP-015).</summary>
internal sealed record RegressionSchedule(string Name, string Violation, IReadOnlyList<ScheduleOp> Ops)
{
    public string Serialize()
    {
        var sb = new StringBuilder();
        sb.Append("# name: ").AppendLine(Name);
        sb.Append("# violation: ").AppendLine(Violation);
        foreach (ScheduleOp op in Ops)
            sb.AppendLine(op.ToString());
        return sb.ToString();
    }

    public static RegressionSchedule Parse(string text)
    {
        string? name = null;
        string? violation = null;
        var ops = new List<ScheduleOp>();
        foreach (string raw in text.Split('\n'))
        {
            string line = raw.TrimEnd('\r');
            if (line.StartsWith("# name: ", StringComparison.Ordinal))
                name = line["# name: ".Length..];
            else if (line.StartsWith("# violation: ", StringComparison.Ordinal))
                violation = line["# violation: ".Length..];
            else if (line.Trim().Length > 0)
                ops.Add(ScheduleOp.Parse(line));
        }

        return new RegressionSchedule(name ?? throw new FormatException("missing name"), violation ?? throw new FormatException("missing violation"), ops);
    }
}

/// <summary>Executes PROP-015 schedules against a volume with an independent per-file oracle.</summary>
internal static class ScheduleRunner
{
    /// <summary>
    /// Runs <paramref name="ops"/> under the path prefix <paramref name="prefix"/> and returns the violation signature
    /// ("content-mismatch:fN", "retry-token-mismatch:fN", "error:Code:kind") or null when the oracle holds.
    /// </summary>
    public static async Task<string?> RunAsync(DfsVolume volume, string prefix, IReadOnlyList<ScheduleOp> ops, ulong seed)
    {
        await volume.Directories.CreateDirectoryAsync(prefix, PropHelpers.Ct);
        var model = new Dictionary<int, byte[]>();
        var lastAppend = new Dictionary<int, (DfsOperationId Id, byte[] Data, CommitToken Token)>();
        string? tokenViolation = null; // reported only when no content violation (the logical effect is the primary oracle)
        foreach (ScheduleOp op in ops)
        {
            string path = DfsPath.Combine(prefix, "f" + op.File.ToString(CultureInfo.InvariantCulture));
            byte[] data = PropHelpers.Content(seed, op.Data, 1 + (op.Data % 7) * 13);
            try
            {
                switch (op.Kind)
                {
                    case "append":
                    {
                        DfsOperationId id = await volume.NewOperationIdAsync(PropHelpers.Ct);
                        CommitToken token = await volume.Files.AppendAllBytesAsync(path, data, new DfsMutationOptions { OperationId = id }, PropHelpers.Ct);
                        model[op.File] = model.TryGetValue(op.File, out byte[]? old) ? [.. old, .. data] : data;
                        lastAppend[op.File] = (id, data, token);
                        break;
                    }

                    case "retry":
                        if (lastAppend.TryGetValue(op.File, out var previous))
                        {
                            CommitToken again = await volume.Files.AppendAllBytesAsync(path, previous.Data, new DfsMutationOptions { OperationId = previous.Id }, PropHelpers.Ct);
                            if (again != previous.Token)
                                tokenViolation ??= $"retry-token-mismatch:f{op.File}";
                        }

                        break;
                    case "write":
                        await volume.Files.WriteAllBytesAsync(path, data, PropHelpers.Ct);
                        model[op.File] = data;
                        break;
                    case "read":
                    {
                        byte[]? actual = await PropHelpers.TryReadAllBytesAsync(volume.Files, path);
                        byte[]? expected = model.GetValueOrDefault(op.File);
                        if (!SameContent(actual, expected))
                            return $"content-mismatch:f{op.File}";
                        break;
                    }
                }
            }
            catch (Exception ex) when (ex is not Xunit.Sdk.XunitException and not NotImplementedException)
            {
                return $"error:{DfsErrors.ToCode(ex)}:{op.Kind}";
            }
        }

        foreach (int file in model.Keys.Order())
        {
            byte[]? actual = await PropHelpers.TryReadAllBytesAsync(volume.Files, DfsPath.Combine(prefix, "f" + file.ToString(CultureInfo.InvariantCulture)));
            if (!SameContent(actual, model[file]))
                return $"content-mismatch:f{file}";
        }

        return tokenViolation;
    }

    private static bool SameContent(byte[]? a, byte[]? b) => a is null ? b is null : b is not null && a.AsSpan().SequenceEqual(b);
}

/// <summary>Retained, named regression schedules replayed by PROP-015 against the unmutated system.</summary>
internal static class RegressionCorpus
{
    /// <summary>Checked-in schedules (each reproduced and minimised from a negative-control failure).</summary>
    public static IReadOnlyList<RegressionSchedule> Retained { get; } =
    [
        new("append-retried-with-same-id", "content-mismatch:f0", [new("append", 0, 1), new("retry", 0, 0)]),
        new("append-overwritten-then-retried", "content-mismatch:f0", [new("append", 0, 1), new("write", 0, 2), new("retry", 0, 0)]),
        new("interleaved-retries-two-files", "content-mismatch:f1", [new("append", 0, 1), new("append", 1, 2), new("retry", 0, 0), new("append", 1, 3), new("retry", 1, 0), new("read", 1, 0)]),
        new("retry-after-second-append", "content-mismatch:f0", [new("append", 0, 1), new("append", 0, 2), new("retry", 0, 0), new("read", 0, 0)]),
    ];

    /// <summary>Directory where newly minimised schedules are retained.</summary>
    public static string Directory => Path.Combine(new TestClusterOptions().ResolveDataRoot(), "prop-regressions");
}

/// <summary>
/// Checks histories whose reads declare their view (PROP-016): <c>Input</c> of a "read" is "live", "pin:P",
/// "snapshot:S" or "backup:S". Live reads are checked for linearizability; every pinned/snapshot/backup read must equal
/// the other reads of the same pin and is linearized as one read at the pin's establishment interval (the "pin-open"
/// or "snapshot-create" operation with the same label). Historical values are therefore accepted only for pinned views.
/// </summary>
internal static class ViewChecker
{
    public const string Live = "live";

    /// <summary>Converts the labelled history into a plain register history, or returns an error.</summary>
    public static (IReadOnlyList<HistoryOp> History, string? Error) Convert(IReadOnlyList<HistoryOp> history)
    {
        var result = new List<HistoryOp>();
        var establishments = new Dictionary<string, HistoryOp>(StringComparer.Ordinal);
        foreach (HistoryOp op in history.Where(o => o.Kind is "pin-open" or "snapshot-create"))
        {
            string label = (string)op.Input!;
            if (op.Outcome == OpOutcome.Ok)
                establishments[label] = op;
        }

        var pinnedValues = new Dictionary<string, (byte[]? Value, HistoryOp First)>(StringComparer.Ordinal);
        foreach (HistoryOp op in history)
        {
            switch (op.Kind)
            {
                case "pin-open":
                case "snapshot-create":
                    continue;
                case "read" when op.Input is not string:
                    return (result, $"read #{op.Id} does not declare its view (live, pin:P, snapshot:S or backup:S)");
                case "read" when op.Input is string label && label != Live:
                {
                    if (!label.StartsWith("pin:", StringComparison.Ordinal) && !label.StartsWith("snapshot:", StringComparison.Ordinal)
                        && !label.StartsWith("backup:", StringComparison.Ordinal))
                        return (result, $"read #{op.Id} declares an unknown view '{label}'");
                    if (op.Outcome != OpOutcome.Ok)
                        continue;
                    string pin = label.StartsWith("backup:", StringComparison.Ordinal) ? "snapshot:" + label["backup:".Length..] : label;
                    if (!establishments.ContainsKey(pin))
                        return (result, $"read #{op.Id} uses view '{label}' that was never established");
                    byte[]? value = op.Output as byte[];
                    if (pinnedValues.TryGetValue(pin, out var seen))
                    {
                        bool same = value is null ? seen.Value is null : seen.Value is not null && value.AsSpan().SequenceEqual(seen.Value);
                        if (!same)
                            return (result, $"pinned view '{pin}' returned two different values (reads #{seen.First.Id} and #{op.Id})");
                    }
                    else
                    {
                        pinnedValues[pin] = (value, op);
                    }

                    continue;
                }

                default:
                    result.Add(op);
                    break;
            }
        }

        foreach ((string pin, (byte[]? value, HistoryOp _)) in pinnedValues)
        {
            HistoryOp est = establishments[pin];
            result.Add(est with { Kind = "read", Input = pin, Output = value, Outcome = OpOutcome.Ok });
        }

        return (result.OrderBy(o => o.InvokeTime).ThenBy(o => o.Id).ToArray(), null);
    }

    /// <summary>Converts and checks; an inconsistent pinned view is an invalid result.</summary>
    public static CheckResult Check(IReadOnlyList<HistoryOp> history)
    {
        (IReadOnlyList<HistoryOp> converted, string? error) = Convert(history);
        if (error is not null)
            return CheckResult.Invalid(error, 0);
        return LinearizabilityChecker.Check(converted, new RegisterModel()); // every key starts absent
    }
}

/// <summary>Kinds of steps of a generated namespace/content program (PROP-009..PROP-012).</summary>
internal enum ProgKind { Write, Append, Copy, Move, Delete, Mkdir, DeleteDir, Read, List }

/// <summary>One generated program step; every generated step is valid in the model state it was generated for.</summary>
internal sealed record ProgOp(ProgKind Kind, string Path, string? Target = null, int DataId = 0, int Length = 0, bool Flag = false)
{
    /// <summary>True for steps that change committed state.</summary>
    public bool IsMutation => Kind is not (ProgKind.Read or ProgKind.List);

    /// <summary>The protocol operation family carrying this step (for message filters).</summary>
    public DfsOperationKind Operation => Kind switch
    {
        ProgKind.Write => DfsOperationKind.WriteAll,
        ProgKind.Append => DfsOperationKind.Append,
        ProgKind.Copy => DfsOperationKind.Copy,
        ProgKind.Move => DfsOperationKind.Move,
        ProgKind.Delete or ProgKind.DeleteDir => DfsOperationKind.Delete,
        ProgKind.Mkdir => DfsOperationKind.CreateDirectory,
        ProgKind.Read => DfsOperationKind.Read,
        _ => DfsOperationKind.Enumerate,
    };

    public override string ToString() =>
        $"{Kind} {Path}{(Target is null ? "" : " -> " + Target)}{(Length > 0 || Kind is ProgKind.Write or ProgKind.Append ? $" len={Length} d{DataId}" : "")}{(Flag ? " (overwrite/recursive)" : "")}";
}

/// <summary>A generated step with the result the independent model expects.</summary>
internal sealed record ProgStep(ProgOp Op, string Expected);

/// <summary>
/// Seeded generator of valid namespace/content programs over a small tree (/p, /p/x, /p/y, /p/x/z) with an independent
/// <see cref="TreeModel"/> oracle. Content lengths are biased towards boundary sizes.
/// </summary>
internal sealed class ProgramGenerator
{
    public static readonly string[] Dirs = ["/p", "/p/x", "/p/y", "/p/x/z"];
    private static readonly string[] Names = ["a", "b", "c", "é", "日"];
    private readonly IRandomSource _random;
    private readonly ulong _seed;
    private readonly long _maxLength;
    private int _dataId;

    public ProgramGenerator(ulong seed, string purpose, long? maxLength = null)
    {
        _seed = seed;
        _random = new SeededRandom(PropHelpers.Derive(seed, purpose));
        Sizes = new BoundaryBiasedSizes(_random.Fork("sizes"), PropHelpers.InlineThreshold, PropHelpers.ChunkSize, PropHelpers.BufferSize);
        _maxLength = maxLength ?? 2L * PropHelpers.ChunkSize + 1;
    }

    /// <summary>Relative weights of Write, Append, Copy, Move, Delete, Mkdir, DeleteDir, Read, List.</summary>
    public int[] Weights { get; init; } = [25, 12, 8, 8, 8, 7, 4, 16, 12];

    public TreeModel Model { get; } = new();

    public BoundaryBiasedSizes Sizes { get; }

    public List<ProgStep> Generate(int count)
    {
        var steps = new List<ProgStep>(count);
        for (int i = 0; i < count; i++)
            steps.Add(Step(Choose()));
        return steps;
    }

    /// <summary>Applies an explicit (valid) step to the model and returns it with its expected result.</summary>
    public ProgStep Step(ProgOp op) => new(op, ProgramRunner.Apply(Model, op, _seed));

    private ProgOp Choose()
    {
        string[] dirs = Dirs.Where(Model.Directories.Contains).ToArray();
        if (dirs.Length == 0)
            return new ProgOp(ProgKind.Mkdir, "/p");
        string[] files = Model.Files.Keys.ToArray();
        string NewFilePath() => DfsPath.Combine(dirs[_random.Next(dirs.Length)], Names[_random.Next(Names.Length)]);
        ProgOp Write(string path) => new(ProgKind.Write, path, DataId: ++_dataId, Length: (int)Sizes.NextLength(_maxLength));

        int total = Weights.Sum();
        int roll = _random.Next(total);
        int kind = 0;
        while (roll >= Weights[kind])
            roll -= Weights[kind++];
        switch ((ProgKind)kind)
        {
            case ProgKind.Write:
                return Write(NewFilePath());
            case ProgKind.Append:
            {
                string path = files.Length > 0 && _random.Next(3) != 0 ? files[_random.Next(files.Length)] : NewFilePath();
                return new ProgOp(ProgKind.Append, path, DataId: ++_dataId, Length: (int)Sizes.NextLength(Math.Min(_maxLength, PropHelpers.ChunkSize + 1L)));
            }

            case ProgKind.Copy:
            case ProgKind.Move:
            {
                if (files.Length == 0)
                    return Write(NewFilePath());
                string from = files[_random.Next(files.Length)];
                string to = NewFilePath();
                if (to == from)
                    return new ProgOp(ProgKind.Read, from);
                return new ProgOp((ProgKind)kind, from, to, Flag: Model.Files.ContainsKey(to));
            }

            case ProgKind.Delete:
                return files.Length == 0 ? Write(NewFilePath()) : new ProgOp(ProgKind.Delete, files[_random.Next(files.Length)]);
            case ProgKind.Mkdir:
                return new ProgOp(ProgKind.Mkdir, Dirs[_random.Next(Dirs.Length)]);
            case ProgKind.DeleteDir:
            {
                string[] removable = dirs.Where(d => d != "/p").ToArray();
                return removable.Length == 0 ? new ProgOp(ProgKind.Mkdir, "/p/x") : new ProgOp(ProgKind.DeleteDir, removable[_random.Next(removable.Length)], Flag: true);
            }

            case ProgKind.Read:
                return files.Length == 0 ? new ProgOp(ProgKind.List, dirs[_random.Next(dirs.Length)]) : new ProgOp(ProgKind.Read, files[_random.Next(files.Length)]);
            default:
                return new ProgOp(ProgKind.List, dirs[_random.Next(dirs.Length)]);
        }
    }
}

/// <summary>Executes program steps through the public SDK and applies them to the model.</summary>
internal static class ProgramRunner
{
    /// <summary>Applies <paramref name="op"/> to <paramref name="model"/> and returns the expected result string.</summary>
    public static string Apply(TreeModel model, ProgOp op, ulong seed)
    {
        switch (op.Kind)
        {
            case ProgKind.Write:
                model.Write(op.Path, PropHelpers.Content(seed, op.DataId, op.Length));
                return "ok";
            case ProgKind.Append:
                model.Append(op.Path, PropHelpers.Content(seed, op.DataId, op.Length));
                return "ok";
            case ProgKind.Copy:
                model.Files[op.Target!] = model.Files[op.Path];
                return "ok";
            case ProgKind.Move:
                model.MoveFile(op.Path, op.Target!);
                return "ok";
            case ProgKind.Delete:
                Assert.True(model.DeleteFile(op.Path), $"generator produced an invalid delete of {op.Path}");
                return "ok";
            case ProgKind.Mkdir:
                model.MkDirs(op.Path);
                return "ok";
            case ProgKind.DeleteDir:
                model.DeleteTree(op.Path);
                return "ok";
            case ProgKind.Read:
                return "bytes:" + PropHelpers.Boundary(model.Files[op.Path]);
            default:
                return "list:" + string.Join(",", Children(model, op.Path));
        }
    }

    /// <summary>Executes <paramref name="op"/> (with an optional explicit operation id) and returns (result, commit token).</summary>
    public static async Task<(string Result, CommitToken Token)> ExecuteAsync(DfsVolume volume, ProgOp op, ulong seed, DfsOperationId? id = null)
    {
        var options = new DfsMutationOptions { OperationId = id };
        CancellationToken ct = PropHelpers.Ct;
        switch (op.Kind)
        {
            case ProgKind.Write:
                return ("ok", await volume.Files.WriteAllBytesAsync(op.Path, PropHelpers.Content(seed, op.DataId, op.Length), options, ct));
            case ProgKind.Append:
                return ("ok", await volume.Files.AppendAllBytesAsync(op.Path, PropHelpers.Content(seed, op.DataId, op.Length), options, ct));
            case ProgKind.Copy:
                return ("ok", await volume.Files.CopyAsync(op.Path, op.Target!, op.Flag, options, ct));
            case ProgKind.Move:
                return ("ok", await volume.Files.MoveAsync(op.Path, op.Target!, op.Flag, options, ct));
            case ProgKind.Delete:
                return ("ok", await volume.Files.DeleteAsync(op.Path, options, ct));
            case ProgKind.Mkdir:
            {
                DfsDirectoryInfo info = await volume.Directories.CreateDirectoryAsync(op.Path, options, ct);
                Assert.Equal(op.Path, info.Path);
                return ("ok", CommitToken.None);
            }

            case ProgKind.DeleteDir:
                return ("ok", await volume.Directories.DeleteAsync(op.Path, op.Flag, options, ct));
            case ProgKind.Read:
                return ("bytes:" + PropHelpers.Boundary(await volume.Files.ReadAllBytesAsync(op.Path, ct)), CommitToken.None);
            default:
            {
                var names = new List<string>();
                await foreach (DfsEntryInfo entry in volume.Directories.EnumerateAsync(op.Path, null, ct))
                    names.Add(entry.Name);
                names.Sort(StringComparer.Ordinal);
                return ("list:" + string.Join(",", names), CommitToken.None);
            }
        }
    }

    /// <summary>Names of the direct children of <paramref name="dir"/> in the model, ordinal order.</summary>
    public static IReadOnlyList<string> Children(TreeModel model, string dir) =>
        model.Files.Keys.Concat(model.Directories)
            .Where(p => p != "/" && p != dir && DfsPath.GetParent(p) == dir)
            .Select(DfsPath.GetName)
            .Order(StringComparer.Ordinal)
            .ToArray();
}

/// <summary>
/// Durable starting image of every node of a cluster (raw disk files) plus the generator inputs, captured before a
/// generated history runs so that a failing (workload seed, fault seed) pair can be replayed from the same image (PROP-007).
/// </summary>
internal sealed class StartingImage
{
    public required ulong WorkloadSeed { get; init; }
    public required ulong FaultSeed { get; init; }
    public required ulong ClusterSeed { get; init; }
    public required IReadOnlyDictionary<string, IReadOnlyDictionary<string, byte[]>> NodeFiles { get; init; }
    public required IReadOnlyDictionary<string, byte[]> InitialContents { get; init; }

    public static StartingImage Capture(TestCluster cluster, ulong workloadSeed, ulong faultSeed, IReadOnlyDictionary<string, byte[]> initialContents)
    {
        var nodes = new SortedDictionary<string, IReadOnlyDictionary<string, byte[]>>(StringComparer.Ordinal);
        foreach (TestNode node in cluster.Nodes)
        {
            var files = new SortedDictionary<string, byte[]>(StringComparer.Ordinal);
            foreach (string path in node.Disk.Files())
                files[path] = node.Disk.ReadRaw(path);
            nodes[node.Name] = files;
        }

        return new StartingImage
        {
            WorkloadSeed = workloadSeed,
            FaultSeed = faultSeed,
            ClusterSeed = cluster.Seed,
            NodeFiles = nodes,
            InitialContents = initialContents,
        };
    }

    /// <summary>Writes the image under <paramref name="directory"/>/starting-image/&lt;node&gt;/&lt;path&gt;.</summary>
    public void Save(string directory)
    {
        string root = Path.Combine(directory, "starting-image");
        foreach ((string node, IReadOnlyDictionary<string, byte[]> files) in NodeFiles)
        {
            foreach ((string path, byte[] bytes) in files)
            {
                string relative = path.Replace('\\', '/').TrimStart('/').Replace('/', Path.DirectorySeparatorChar);
                string target = Path.Combine(root, node, relative);
                System.IO.Directory.CreateDirectory(Path.GetDirectoryName(target)!);
                File.WriteAllBytes(target, bytes);
            }
        }

        var sb = new StringBuilder();
        sb.AppendLine(CultureInfo.InvariantCulture, $"workloadSeed={WorkloadSeed}");
        sb.AppendLine(CultureInfo.InvariantCulture, $"faultSeed={FaultSeed}");
        sb.AppendLine(CultureInfo.InvariantCulture, $"clusterSeed={ClusterSeed}");
        foreach ((string path, byte[] content) in InitialContents)
            sb.AppendLine(CultureInfo.InvariantCulture, $"initial {path} {PropHelpers.Boundary(content)}");
        File.WriteAllText(Path.Combine(directory, "starting-image.txt"), sb.ToString());
    }
}
