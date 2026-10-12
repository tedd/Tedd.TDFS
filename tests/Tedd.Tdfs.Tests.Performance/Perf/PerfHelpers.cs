using System.Diagnostics;
using System.Globalization;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Performance.Perf;

/// <summary>Shared helpers for the PERF tests: cluster configurations, latency statistics, memory probes and run records.</summary>
internal static class PerfHelpers
{
    public const string Volume = "vol";

    public static CancellationToken Ct => TestContext.Current.CancellationToken;

    public static void Log(string message) => TestContext.Current.TestOutputHelper?.WriteLine(message);

    /// <summary>Realtime three-zone cluster (in-memory transport, simulated storage), target 3 / ack 2 in 2 zones.</summary>
    public static TestClusterOptions Realtime(int backupNodes = 0) => TestClusterOptions.ThreeZones with
    {
        BackupNodeCount = backupNodes,
        ScenarioTimeout = TimeSpan.FromMinutes(3),
    };

    /// <summary>Realtime three-zone cluster whose node data lives on the real filesystem (payloads stay out of the managed heap).</summary>
    public static TestClusterOptions RealtimeOnDisk() => Realtime() with { Storage = StorageKind.Disk };

    public static DfsStoragePolicy Policy(int target, int zones, int ack, int ackZones) => new()
    {
        TargetCopies = target,
        TargetDistinctZones = zones,
        DurableCopiesBeforeAck = ack,
        DurableZonesBeforeAck = ackZones,
    };

    public static string Describe(DfsStoragePolicy policy) =>
        $"target {policy.TargetCopies} copies/{policy.TargetDistinctZones} zones, ack {policy.DurableCopiesBeforeAck} copies/{policy.DurableZonesBeforeAck} zones";

    public static string Describe(TestClusterOptions options) =>
        $"{options.EffectiveNodeCount} nodes {options.Topology}, {options.Transport}/{options.Storage}, {options.Mode}, voters {options.EffectiveControlVoters}";

    /// <summary>Reports one metric together with the policy and topology it was measured under (FSD §15: never a bare number).</summary>
    public static void Report(string metric, double value, string unit, string context) =>
        Log(string.Create(CultureInfo.InvariantCulture, $"METRIC {metric} = {value:0.###} {unit} [{context}]"));

    public static void BytesEqual(byte[] expected, byte[] actual, string? because = null) => AssertEx.BytesEqual(expected, actual, because);

    /// <summary>Live managed memory after a full blocking collection (excludes garbage).</summary>
    public static long LiveBytes()
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        return GC.GetTotalMemory(forceFullCollection: true);
    }

    public static async Task<long> TotalReceivedObjectBytesAsync(TestCluster cluster) => (await cluster.Inspect.GetTotalCountersAsync()).ObjectBytesReceived;

    public static async Task<IReadOnlyList<ObjectId>> ObjectsOfAsync(TestCluster cluster, string path)
    {
        FileInspection? file = await cluster.Inspect.GetFileAsync(Volume, path);
        Assert.True(file is not null, $"{path} must exist");
        return file.Extents.Select(e => e.Object).Distinct().ToArray();
    }

    /// <summary>Deterministic block of a large synthetic stream (regenerated on demand, so the test never holds the whole file).</summary>
    public static byte[] Block(ulong seed, long blockIndex, int length) => TestData.Bytes(seed ^ (ulong)blockIndex * 0x9E3779B97F4A7C15UL, length);
}

/// <summary>Latency sample with nearest-rank percentiles.</summary>
internal sealed class LatencyRecorder
{
    private readonly List<TimeSpan> _samples = [];
    private readonly Lock _lock = new();

    public int Count
    {
        get
        {
            lock (_lock)
                return _samples.Count;
        }
    }

    public void Add(TimeSpan sample)
    {
        lock (_lock)
            _samples.Add(sample);
    }

    public async Task<T> TimeAsync<T>(Func<Task<T>> action)
    {
        long start = Stopwatch.GetTimestamp();
        T result = await action();
        Add(Stopwatch.GetElapsedTime(start));
        return result;
    }

    public async Task TimeAsync(Func<Task> action)
    {
        long start = Stopwatch.GetTimestamp();
        await action();
        Add(Stopwatch.GetElapsedTime(start));
    }

    public TimeSpan Percentile(double p)
    {
        lock (_lock)
        {
            Assert.NotEmpty(_samples);
            TimeSpan[] sorted = _samples.OrderBy(s => s).ToArray();
            int rank = (int)Math.Ceiling(p / 100.0 * sorted.Length);
            return sorted[Math.Clamp(rank - 1, 0, sorted.Length - 1)];
        }
    }

    public TimeSpan Total
    {
        get
        {
            lock (_lock)
                return TimeSpan.FromTicks(_samples.Sum(s => s.Ticks));
        }
    }

    public string Summary(string label) => string.Create(CultureInfo.InvariantCulture,
        $"{label}: n={Count} p50={Percentile(50).TotalMilliseconds:0.00}ms p95={Percentile(95).TotalMilliseconds:0.00}ms p99={Percentile(99).TotalMilliseconds:0.00}ms");
}

/// <summary>Write-only stream that verifies a synthetic stream block by block and never retains the data.</summary>
internal sealed class VerifyingSink : Stream
{
    private readonly ulong _seed;
    private readonly int _blockSize;
    private readonly TimeSpan _perWriteDelay;
    private byte[] _expectedBlock = [];
    private long _expectedBlockIndex = -1;

    public VerifyingSink(ulong seed, int blockSize, TimeSpan perWriteDelay = default)
    {
        _seed = seed;
        _blockSize = blockSize;
        _perWriteDelay = perWriteDelay;
    }

    public long Received { get; private set; }

    public override bool CanRead => false;
    public override bool CanSeek => false;
    public override bool CanWrite => true;
    public override long Length => Received;
    public override long Position { get => Received; set => throw new NotSupportedException(); }
    public override void Flush() { }
    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();

    public override void Write(byte[] buffer, int offset, int count) => Write(buffer.AsSpan(offset, count));

    public override void Write(ReadOnlySpan<byte> buffer)
    {
        while (!buffer.IsEmpty)
        {
            long block = Received / _blockSize;
            int within = (int)(Received % _blockSize);
            if (block != _expectedBlockIndex)
            {
                _expectedBlock = PerfHelpers.Block(_seed, block, _blockSize);
                _expectedBlockIndex = block;
            }

            int n = Math.Min(buffer.Length, _blockSize - within);
            if (!buffer[..n].SequenceEqual(_expectedBlock.AsSpan(within, n)))
                throw new InvalidDataException($"Streamed content differs within block {block} at offset {Received}.");
            Received += n;
            buffer = buffer[n..];
        }
    }

    public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        if (_perWriteDelay > TimeSpan.Zero)
            await Task.Delay(_perWriteDelay, cancellationToken);
        Write(buffer.Span);
    }

    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        WriteAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
}

/// <summary>
/// Everything that determines whether two performance results are comparable (PERF-016): build, runtime, OS, hardware,
/// storage/transport, topology, timings, durability policy, workload and seed, and cache state.
/// </summary>
internal sealed record PerfRunRecord(
    string Build,
    string Runtime,
    string OperatingSystem,
    string Architecture,
    int ProcessorCount,
    string Storage,
    string Transport,
    string Mode,
    string Topology,
    string Policy,
    string Timings,
    bool EncryptAtRest,
    string Workload,
    ulong Seed,
    string CacheState)
{
    public static PerfRunRecord Capture(TestCluster cluster, string workload, string cacheState)
    {
        TestClusterOptions o = cluster.Options;
        Assembly product = typeof(DfsClient).Assembly;
        string build = product.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion
            ?? product.GetName().Version?.ToString() ?? "unknown";
        string topology = string.Join(";", o.GetNodeSpecs().Select(s => $"{s.Name}@{s.Region}/{s.Zone}/{s.Rack}/{s.EffectiveHost}/{s.Device}"));
        return new PerfRunRecord(build, RuntimeInformation.FrameworkDescription, RuntimeInformation.OSDescription,
            RuntimeInformation.ProcessArchitecture.ToString(), Environment.ProcessorCount, o.Storage.ToString(), o.Transport.ToString(),
            o.Mode.ToString(), topology, PerfHelpers.Describe(o.GetEffectiveDefaultPolicy()), o.Timings.ToString(), o.EncryptAtRest,
            workload, cluster.Seed, cacheState);
    }

    /// <summary>Stable hash of every comparability-relevant field.</summary>
    [System.Text.Json.Serialization.JsonIgnore]
    public string CompatibilityKey => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(JsonSerializer.Serialize(this))));

    /// <summary>True when results of <paramref name="a"/> and <paramref name="b"/> may be compared; otherwise names the first difference.</summary>
    public static bool CanCompare(PerfRunRecord a, PerfRunRecord b, out string? reason)
    {
        foreach (PropertyInfo property in typeof(PerfRunRecord).GetProperties().Where(p => p.Name != nameof(CompatibilityKey)))
        {
            object? x = property.GetValue(a);
            object? y = property.GetValue(b);
            if (!Equals(x, y))
            {
                reason = $"{property.Name} differs: '{x}' vs '{y}'";
                return false;
            }
        }

        reason = null;
        return true;
    }

    public string ToJson() => JsonSerializer.Serialize(this);
}
