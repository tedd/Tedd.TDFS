using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Simulation.Ops;

/// <summary>Shared helpers for the OPS simulation tests.</summary>
internal static class OpsHelpers
{
    public const string Volume = "vol";

    public static CancellationToken Ct => TestContext.Current.CancellationToken;

    public static void Log(string message) => TestContext.Current.TestOutputHelper?.WriteLine(message);

    /// <summary>Three nodes, one per zone, three voters, target 3 / ack 2 in 2 zones, deterministic simulation.</summary>
    public static TestClusterOptions ThreeZones(int backupNodes = 0) => TestClusterOptions.ThreeZones with
    {
        Mode = ClusterMode.Deterministic,
        BackupNodeCount = backupNodes,
        ScenarioTimeout = TimeSpan.FromMinutes(10),
    };

    /// <summary>N nodes one per zone with three control and three volume voters and the given default data policy.</summary>
    public static TestClusterOptions Nodes(int count, DfsStoragePolicy? policy = null, int volumeVoters = 3, int backupNodes = 0) => new()
    {
        NodeCount = count,
        Topology = TestTopology.OnePerZone,
        ControlVoters = Math.Min(volumeVoters, count),
        Volumes = [new TestVolumeSpec(Volume) { Metadata = new DfsMetadataPolicy { Voters = Math.Min(volumeVoters, count) } }],
        DefaultPolicy = policy,
        BackupNodeCount = backupNodes,
        Mode = ClusterMode.Deterministic,
        ScenarioTimeout = TimeSpan.FromMinutes(10),
    };

    public static DfsStoragePolicy Policy(int target, int zones, int ack, int ackZones) => new()
    {
        TargetCopies = target,
        TargetDistinctZones = zones,
        DurableCopiesBeforeAck = ack,
        DurableZonesBeforeAck = ackZones,
    };

    /// <summary>Byte-exact comparison taking arrays, so call sites may await inside the argument list.</summary>
    public static void BytesEqual(byte[] expected, byte[] actual, string? because = null) => AssertEx.BytesEqual(expected, actual, because);

    /// <summary>Writes deterministic files and returns path → content.</summary>
    public static async Task<Dictionary<string, byte[]>> SeedFilesAsync(DfsVolume volume, string directory, int count, int length, ulong seed)
    {
        await volume.Directories.CreateDirectoryAsync(directory, Ct);
        var files = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        for (int i = 0; i < count; i++)
        {
            string path = $"{directory}/f{i}";
            byte[] content = TestData.Bytes(seed + (ulong)i, length);
            await volume.Files.WriteAllBytesAsync(path, content, Ct);
            files[path] = content;
        }

        return files;
    }

    /// <summary>Reads every file back and compares bytes exactly.</summary>
    public static async Task VerifyFilesAsync(DfsVolume volume, IReadOnlyDictionary<string, byte[]> files, string because)
    {
        foreach ((string path, byte[] content) in files)
            BytesEqual(content, await volume.Files.ReadAllBytesAsync(path, Ct), $"{path}: {because}");
    }

    /// <summary>Objects referenced by the current extent map of each file (inline files reference none).</summary>
    public static async Task<IReadOnlyList<ObjectId>> ObjectsOfAsync(TestCluster cluster, IEnumerable<string> paths)
    {
        var result = new List<ObjectId>();
        foreach (string path in paths)
        {
            FileInspection? file = await cluster.Inspect.GetFileAsync(Volume, path);
            Assert.True(file is not null, $"{path} must exist");
            result.AddRange(file.Extents.Select(e => e.Object).Distinct());
        }

        return result;
    }

    /// <summary>
    /// Independent replica accounting computed from durable holders (not from the admin metrics): for every object
    /// reachable from the current root and snapshots, the verified durable copies and distinct zones.
    /// </summary>
    public static async Task<IndependentHealth> ComputeIndependentHealthAsync(TestCluster cluster, DfsStoragePolicy policy)
    {
        VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Volume);
        int objects = 0, under = 0, zoneDeficit = 0, missing = 0;
        foreach (ObjectId id in state.ReachableObjects())
        {
            objects++;
            (int copies, int zones) = await cluster.Inspect.CountDurableDomainsAsync(id);
            if (copies == 0)
                missing++;
            if (copies < policy.TargetCopies)
                under++;
            if (zones < policy.TargetDistinctZones)
                zoneDeficit++;
        }

        return new IndependentHealth(objects, under, zoneDeficit, missing);
    }

    /// <summary>
    /// Reads the reported replica health between two independent computations and, when the cluster did not change in
    /// between, asserts that the report matches durable reality exactly. Retries a bounded number of times while repair is
    /// still moving data.
    /// </summary>
    public static async Task<(DfsReplicaHealth Reported, IndependentHealth Actual)> AssertHealthMatchesRealityAsync(
        TestCluster cluster, DfsAdminClient admin, DfsStoragePolicy policy, string because)
    {
        for (int attempt = 0; attempt < 20; attempt++)
        {
            IndependentHealth before = await ComputeIndependentHealthAsync(cluster, policy);
            DfsReplicaHealth reported = await admin.GetReplicaHealthAsync(Volume, Ct);
            IndependentHealth after = await ComputeIndependentHealthAsync(cluster, policy);
            if (before != after)
            {
                await cluster.DelayAsync(TimeSpan.FromMilliseconds(250), Ct);
                continue;
            }

            Log($"{because}: reported under={reported.UnderReplicatedObjects} zoneDeficit={reported.ZoneDeficitObjects} missing={reported.MissingObjects}; actual {after}");
            Assert.True(reported.UnderReplicatedObjects == after.UnderReplicated,
                $"{because}: reported {reported.UnderReplicatedObjects} under-replicated objects, durable reality is {after.UnderReplicated}");
            Assert.True(reported.ZoneDeficitObjects == after.ZoneDeficit,
                $"{because}: reported {reported.ZoneDeficitObjects} zone-deficit objects, durable reality is {after.ZoneDeficit}");
            Assert.True(reported.MissingObjects == after.Missing,
                $"{because}: reported {reported.MissingObjects} missing objects, durable reality is {after.Missing}");
            Assert.True(reported.Objects >= after.Objects, $"{because}: reported {reported.Objects} objects but {after.Objects} are referenced");
            return (reported, after);
        }

        Assert.Fail($"{because}: the replica state never stayed stable long enough to compare the report with durable reality");
        throw new InvalidOperationException("unreachable");
    }

    /// <summary>True for failures that are legal while nodes, leaders or quorums come and go.</summary>
    public static bool IsAvailabilityOrUnknown(Exception ex) => ex is DfsUnavailableException or DfsCommitOutcomeUnknownException
        or TimeoutException or DfsSharingViolationException or DfsTransactionAbortedException
        || ex is DfsException { ErrorCode: DfsErrorCode.Timeout or DfsErrorCode.OutcomeUnknown or DfsErrorCode.NotLeader or DfsErrorCode.Unavailable };

    /// <summary>Percentile of a latency sample (nearest-rank).</summary>
    public static TimeSpan Percentile(IReadOnlyList<TimeSpan> samples, double percentile)
    {
        Assert.NotEmpty(samples);
        TimeSpan[] sorted = samples.OrderBy(s => s).ToArray();
        int rank = (int)Math.Ceiling(percentile / 100.0 * sorted.Length);
        return sorted[Math.Clamp(rank - 1, 0, sorted.Length - 1)];
    }

    /// <summary>Elapsed harness time (virtual in deterministic mode) of an action.</summary>
    public static async Task<TimeSpan> TimeAsync(TestCluster cluster, Func<Task> action)
    {
        long start = cluster.Time.GetTimestamp();
        await action();
        return cluster.Time.GetElapsedTime(start);
    }
}

/// <summary>Replica accounting computed independently from durable holders.</summary>
internal sealed record IndependentHealth(int Objects, int UnderReplicated, int ZoneDeficit, int Missing);

/// <summary>
/// Register/append workload recorded in the cluster history with explicit, persisted operation ids so unknown outcomes can
/// be resolved authoritatively afterwards. Each key is a file under <c>directory</c>; keys starting with "log" are append
/// logs, all others are registers written with WriteAll. Runs as plain async methods (simulation compatible).
/// </summary>
internal sealed class HistoryWorkload
{
    private readonly TestCluster _cluster;
    private readonly string _directory;
    private readonly List<Task> _writers = [];
    private Task _reader = Task.CompletedTask;
    private readonly List<Exception> _unexpected = [];
    private readonly System.Threading.Lock _lock = new();
    private readonly Dictionary<string, ulong> _seq = new(StringComparer.Ordinal);
    private volatile bool _stopping;
    private int _okWrites;
    private int _attempts;

    public HistoryWorkload(TestCluster cluster, string directory)
    {
        _cluster = cluster;
        _directory = directory;
    }

    public int OkWrites => Volatile.Read(ref _okWrites);

    public int Attempts => Volatile.Read(ref _attempts);

    public IReadOnlyList<Exception> UnexpectedErrors
    {
        get
        {
            lock (_lock)
                return _unexpected.ToArray();
        }
    }

    public string PathOf(string key) => $"{_directory}/{key}";

    /// <summary>Starts one writer loop per (client, key) pair and one reader loop over all keys on <paramref name="reader"/>.</summary>
    public void Start(IReadOnlyList<(DfsClient Client, string Key)> writers, DfsClient? reader, int valueBytes, TimeSpan pause, int? maxOpsPerWriter = null)
    {
        foreach ((DfsClient client, string key) in writers)
            _writers.Add(WriterLoopAsync(client, key, valueBytes, pause, maxOpsPerWriter));
        if (reader is not null)
            _reader = ReaderLoopAsync(reader, writers.Select(w => w.Key).Distinct().ToArray(), pause);
    }

    /// <summary>Signals the loops to stop and waits for them.</summary>
    public async Task StopAsync()
    {
        _stopping = true;
        await Task.WhenAll(_writers);
        await _reader;
    }

    /// <summary>Waits for writer loops started with a bounded op count, then stops the reader loop.</summary>
    public async Task CompletionAsync()
    {
        await Task.WhenAll(_writers);
        _stopping = true;
        await _reader;
    }

    public async Task WriteAsync(DfsClient client, string key, byte[] value)
    {
        DfsVolume vol = client.GetVolume(OpsHelpers.Volume);
        DfsOperationId id;
        try
        {
            id = await vol.NewOperationIdAsync(OpsHelpers.Ct);
        }
        catch (Exception ex) when (OpsHelpers.IsAvailabilityOrUnknown(ex))
        {
            return; // nothing was invoked
        }

        Interlocked.Increment(ref _attempts);
        bool append = key.StartsWith("log", StringComparison.Ordinal);
        long h = _cluster.History.Invoke("w:" + key, append ? "append" : "write", key, value, id);
        try
        {
            if (append)
                await vol.Files.AppendAllBytesAsync(PathOf(key), value, new DfsMutationOptions { OperationId = id }, OpsHelpers.Ct);
            else
                await vol.Files.WriteAllBytesAsync(PathOf(key), value, new DfsMutationOptions { OperationId = id }, OpsHelpers.Ct);
            _cluster.History.Complete(h, OpOutcome.Ok);
            Interlocked.Increment(ref _okWrites);
        }
        catch (Exception ex) when (OpsHelpers.IsAvailabilityOrUnknown(ex))
        {
            _cluster.History.Complete(h, HistoryRecorder.Classify(ex));
        }
        catch (Exception ex)
        {
            _cluster.History.Complete(h, OpOutcome.Unknown);
            lock (_lock)
                _unexpected.Add(ex);
        }
    }

    public async Task ReadAsync(DfsClient client, string key, string process = "reader")
    {
        DfsVolume vol = client.GetVolume(OpsHelpers.Volume);
        long h = _cluster.History.Invoke(process, "read", key, null);
        try
        {
            byte[] value = await vol.Files.ReadAllBytesAsync(PathOf(key), OpsHelpers.Ct);
            _cluster.History.Complete(h, OpOutcome.Ok, value);
        }
        catch (FileNotFoundException)
        {
            _cluster.History.Complete(h, OpOutcome.Ok, key.StartsWith("log", StringComparison.Ordinal) ? Array.Empty<byte>() : null);
        }
        catch (Exception ex) when (OpsHelpers.IsAvailabilityOrUnknown(ex))
        {
            _cluster.History.Complete(h, OpOutcome.Failed); // a failed read has no effect
        }
        catch (Exception ex)
        {
            _cluster.History.Complete(h, OpOutcome.Failed);
            lock (_lock)
                _unexpected.Add(ex);
        }
    }

    /// <summary>
    /// Resolves every unknown write authoritatively with <c>QueryOperationAsync(resolve: true)</c>; the answer must be
    /// definite (Committed or Aborted), never Pending/NotFound.
    /// </summary>
    public async Task ResolveUnknownsAsync(DfsClient client)
    {
        DfsVolume vol = client.GetVolume(OpsHelpers.Volume);
        foreach (HistoryOp op in _cluster.History.Operations)
        {
            if (op.Outcome != OpOutcome.Unknown || op.OperationId is not { } id || op.Kind == "read")
                continue;
            DfsOperationOutcome outcome = await vol.QueryOperationAsync(id, resolve: true, OpsHelpers.Ct);
            Assert.True(outcome.Status is DfsOutcomeStatus.Committed or DfsOutcomeStatus.Aborted,
                $"resolve:true for {id} must be definite but was {outcome.Status}");
            _cluster.History.Resolve(op.Id, outcome.Status == DfsOutcomeStatus.Committed ? OpOutcome.Ok : OpOutcome.Failed);
        }
    }

    /// <summary>One final read of every key (after recovery), recorded in the history.</summary>
    public async Task FinalReadsAsync(DfsClient client, IEnumerable<string> keys)
    {
        foreach (string key in keys)
            await ReadAsync(client, key, "final");
    }

    /// <summary>Checks registers against <see cref="RegisterModel"/> and logs against <see cref="AppendLogModel"/>.</summary>
    public void AssertLinearizable()
    {
        IReadOnlyList<HistoryOp> all = _cluster.History.Operations.Where(IsMine).ToArray();
        LinearizabilityChecker.Check(all.Where(o => !o.Key.StartsWith("log", StringComparison.Ordinal)).ToArray(), new RegisterModel()).AssertValid();
        HistoryOp[] logs = all.Where(o => o.Key.StartsWith("log", StringComparison.Ordinal)).ToArray();
        if (logs.Length > 0)
            LinearizabilityChecker.Check(logs, new AppendLogModel()).AssertValid();
        Assert.Empty(UnexpectedErrors);
    }

    private static bool IsMine(HistoryOp op) => op.Kind is "write" or "append" or "read";

    /// <summary>FNV-1a over UTF-16 code units (process-independent, unlike string.GetHashCode).</summary>
    private static ulong StableHash(string s)
    {
        ulong h = 14695981039346656037UL;
        foreach (char c in s)
        {
            h ^= c;
            h *= 1099511628211UL;
        }

        return h;
    }

    private byte[] NextValue(string key, int length)
    {
        ulong n;
        lock (_lock)
        {
            _seq.TryGetValue(key, out n);
            _seq[key] = n + 1;
        }

        return TestData.Bytes((StableHash(key) << 20) ^ n ^ 0x5EED, length);
    }

    private async Task WriterLoopAsync(DfsClient client, string key, int valueBytes, TimeSpan pause, int? maxOps)
    {
        for (int n = 0; !_stopping && (maxOps is null || n < maxOps); n++)
        {
            int length = key.StartsWith("log", StringComparison.Ordinal) ? 32 : valueBytes;
            await WriteAsync(client, key, NextValue(key, length));
            await _cluster.DelayAsync(pause, CancellationToken.None);
        }
    }

    private async Task ReaderLoopAsync(DfsClient reader, IReadOnlyList<string> keys, TimeSpan pause)
    {
        for (int n = 0; !_stopping; n++)
        {
            await ReadAsync(reader, keys[n % keys.Count]);
            await _cluster.DelayAsync(pause, CancellationToken.None);
        }
    }
}
