using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Simulation.Cha;

/// <summary>Shared setup, observation and oracle helpers for the compound-failure (CHA) scenarios.</summary>
internal static class ChaHelpers
{
    /// <summary>The volume every scenario uses.</summary>
    public const string Vol = "vol";

    /// <summary>A file length spanning several logical update units (chunk = 64 KiB under fast timings) and well above the inline threshold.</summary>
    public const int MultiExtentLength = (3 * 64 * 1024) + 4321;

    /// <summary>Writer lease under <see cref="NodeTimings.Fast"/> (2 s).</summary>
    public static TimeSpan Lease => NodeTimings.Fast.LeaseDuration;

    /// <summary>Session timeout under <see cref="NodeTimings.Fast"/> (10 s).</summary>
    public static TimeSpan SessionTimeout => NodeTimings.Fast.SessionTimeout;

    /// <summary>Raft log-compaction threshold under <see cref="NodeTimings.Fast"/> (200 entries).</summary>
    public static int RaftSnapshotThreshold => NodeTimings.Fast.RaftSnapshotThreshold;

    /// <summary>The xunit cancellation token of the running test.</summary>
    public static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Deterministic-simulation variant of <paramref name="options"/> with a generous virtual scenario budget.</summary>
    public static TestClusterOptions Sim(TestClusterOptions options, int virtualMinutes = 10) =>
        options with { Mode = ClusterMode.Deterministic, ScenarioTimeout = TimeSpan.FromMinutes(virtualMinutes) };

    /// <summary>Writes a progress/evidence line to the test output.</summary>
    public static void Log(string message) => TestContext.Current.TestOutputHelper?.WriteLine(message);

    /// <summary>Connects a named client.</summary>
    public static Task<DfsClient> ConnectAsync(TestCluster cluster, string name, IReadOnlyList<string>? seeds = null,
        TimeSpan? operationTimeout = null) =>
        cluster.ConnectAsync(new TestClientOptions { Name = name, SeedNodes = seeds, OperationTimeout = operationTimeout }, Ct);

    /// <summary>Reads a whole file, returning null only for genuine absence.</summary>
    public static async Task<byte[]?> TryReadAllAsync(DfsFileClient files, string path)
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

    /// <summary>Awaits <paramref name="actual"/> and asserts byte-for-byte equality with <paramref name="expected"/>.</summary>
    public static async Task AssertBytesAsync(byte[] expected, Task<byte[]> actual, string? because = null)
    {
        byte[] observed = await actual;
        AssertEx.BytesEqual(expected, observed, because);
    }

    /// <summary>Reads a stream from its current position to EOF.</summary>
    public static async Task<byte[]> ReadToEndAsync(Stream stream)
    {
        using var buffer = new MemoryStream();
        byte[] chunk = new byte[16 * 1024];
        while (true)
        {
            int read = await stream.ReadAsync(chunk, Ct);
            if (read == 0)
                return buffer.ToArray();
            buffer.Write(chunk, 0, read);
        }
    }

    /// <summary>Distinct immutable objects referenced by the current revision of <paramref name="path"/> (leader view).</summary>
    public static async Task<IReadOnlyList<ObjectId>> ObjectsOfAsync(TestCluster cluster, string path, string volume = Vol)
    {
        FileInspection? file = await cluster.Inspect.GetFileAsync(volume, path);
        Assert.True(file is not null, $"{path} must exist in the authoritative state of {volume}");
        return file.Extents.Select(e => e.Object).Distinct().ToArray();
    }

    /// <summary>Nodes currently running.</summary>
    public static IReadOnlyList<TestNode> Running(TestCluster cluster) =>
        cluster.Nodes.Where(n => n.State == TestNodeState.Running).ToArray();

    /// <summary>Names of the given nodes.</summary>
    public static string[] Names(IEnumerable<TestNode> nodes) => nodes.Select(n => n.Name).ToArray();

    /// <summary>
    /// Waits until one of <paramref name="candidates"/> reports itself leader of <paramref name="group"/> (and is not
    /// <paramref name="excluding"/>), as seen by the candidates' own inspectors; used while a partition hides other nodes.
    /// </summary>
    public static async Task<TestNode> WaitForLeaderAmongAsync(TestCluster cluster, IReadOnlyList<TestNode> candidates, string group,
        string? excluding, TimeSpan timeout, string because)
    {
        TestNode? found = null;
        await cluster.WaitUntilAsync(async () =>
        {
            foreach (TestNode node in candidates)
            {
                if (node.State != TestNodeState.Running || node.Name == excluding)
                    continue;
                GroupInspection? g = await node.Inspect.GetGroupAsync(group);
                if (g is not null && g.Role == RaftRole.Leader)
                {
                    found = node;
                    return true;
                }
            }

            return false;
        }, timeout, because, Ct);
        return found!;
    }

    /// <summary>
    /// One election-safety sample: records every (group, term) → leader observed on running nodes and returns a violation
    /// when two different nodes claim leadership of the same group in the same term (at any time in the scenario).
    /// </summary>
    public static async Task<string?> SampleLeadersAsync(TestCluster cluster, Dictionary<(string Group, ulong Term), string> seen)
    {
        foreach (TestNode node in Running(cluster))
        {
            IReadOnlyList<GroupInspection> groups = await node.Inspect.GetGroupsAsync();
            foreach (GroupInspection g in groups.Where(g => g.Role == RaftRole.Leader))
            {
                (string, ulong) key = (g.Group, g.Term);
                if (seen.TryGetValue(key, out string? other) && !string.Equals(other, node.Name, StringComparison.Ordinal))
                    return $"election safety violated: group '{g.Group}' term {g.Term} led by both {other} and {node.Name}";
                seen[key] = node.Name;
            }
        }

        return null;
    }

    /// <summary>
    /// Opens <paramref name="path"/> for writing (Open, Write, Share.Read), retrying sharing/writer conflicts until an
    /// earlier writer's reservation has been released by server-side expiry.
    /// </summary>
    public static async Task<DfsStream> OpenWriterEventuallyAsync(TestCluster cluster, DfsVolume volume, string path, TimeSpan timeout,
        FileMode mode = FileMode.Open)
    {
        DfsStream? stream = null;
        await cluster.WaitUntilAsync(async () =>
        {
            try
            {
                stream = await volume.Files.OpenAsync(path, new DfsOpenOptions { Mode = mode, Access = FileAccess.Write, Share = FileShare.Read }, Ct);
                return true;
            }
            catch (DfsSharingViolationException)
            {
                return false;
            }
        }, timeout, $"a new writer can open {path} once the previous writer's lease expired server-side", Ct);
        return stream!;
    }

    /// <summary>True when the exception is an explicit availability/timeout failure (never a fabricated result).</summary>
    public static bool IsExplicitUnavailability(Exception ex) =>
        ex is DfsUnavailableException or TimeoutException or DfsCommitOutcomeUnknownException
        || ex is DfsException { ErrorCode: DfsErrorCode.NotLeader or DfsErrorCode.Timeout or DfsErrorCode.Unavailable or DfsErrorCode.NodeUnavailable or DfsErrorCode.QuorumUnavailable }
        || ex is OperationCanceledException;

    /// <summary>Asserts that the current state of every replica of <paramref name="volume"/> agrees on <paramref name="path"/>.</summary>
    public static async Task<IReadOnlyList<FileInspection?>> ReplicaViewsAsync(TestCluster cluster, string path, string volume = Vol)
    {
        var views = new List<FileInspection?>();
        foreach (TestNode node in Running(cluster))
        {
            VolumeStateInspection? state = await node.Inspect.GetVolumeStateAsync(volume);
            if (state is not null)
                views.Add(state.GetFile(path));
        }

        return views;
    }

    /// <summary>Waits until <paramref name="objects"/> are no longer stored anywhere (garbage collection liveness).</summary>
    public static Task WaitUntilReclaimedAsync(TestCluster cluster, IReadOnlyCollection<ObjectId> objects, TimeSpan timeout, string because,
        string volume = Vol) =>
        cluster.WaitUntilAsync(async () =>
        {
            IReadOnlySet<ObjectId> stored = await cluster.Inspect.GetAllStoredObjectsAsync(volume);
            return !objects.Any(stored.Contains);
        }, timeout, because, Ct);

    /// <summary>Restarts every crashed or stopped node and waits for a stable cluster (scenario cleanup before harness checks).</summary>
    public static async Task RestoreAllNodesAsync(TestCluster cluster)
    {
        cluster.Network.Heal();
        foreach (TestNode node in cluster.Nodes)
        {
            if (node.State is TestNodeState.Crashed or TestNodeState.Stopped)
                await node.RestartAsync();
            else if (node.State == TestNodeState.Paused)
                await node.ResumeAsync();
        }

        await cluster.WaitForStableAsync(TimeSpan.FromSeconds(60), Ct);
    }
}

/// <summary>
/// Periodically evaluates a safety predicate while a scenario runs (test plan §3.3 / VER-05: safety is checked during
/// failures and recovery, not only after convergence). A sample whose nodes changed state mid-inspection is skipped.
/// </summary>
internal sealed class SafetyMonitor
{
    private readonly TestCluster _cluster;
    private readonly string _name;
    private readonly TimeSpan _interval;
    private readonly Func<Task<string?>> _check;
    private readonly CancellationTokenSource _stop = new();
    private readonly List<string> _violations = [];
    private Task _loop = Task.CompletedTask;

    private SafetyMonitor(TestCluster cluster, string name, TimeSpan interval, Func<Task<string?>> check)
    {
        _cluster = cluster;
        _name = name;
        _interval = interval;
        _check = check;
    }

    /// <summary>Number of completed samples.</summary>
    public int Samples { get; private set; }

    /// <summary>Violations observed so far.</summary>
    public IReadOnlyList<string> Violations => _violations;

    /// <summary>Starts sampling every <paramref name="interval"/> (harness clock; virtual in deterministic mode).</summary>
    public static SafetyMonitor Start(TestCluster cluster, string name, TimeSpan interval, Func<Task<string?>> check)
    {
        var monitor = new SafetyMonitor(cluster, name, interval, check);
        monitor._loop = monitor.LoopAsync();
        return monitor;
    }

    /// <summary>Stops sampling and asserts that at least <paramref name="minSamples"/> samples ran and none failed.</summary>
    public async Task StopAndAssertAsync(int minSamples = 1)
    {
        await _stop.CancelAsync();
        await _loop;
        Assert.True(Samples >= minSamples, $"safety monitor '{_name}' ran only {Samples} samples (expected at least {minSamples}); missing evidence is not a pass");
        Assert.True(_violations.Count == 0, $"safety monitor '{_name}' observed violations:{Environment.NewLine}{string.Join(Environment.NewLine, _violations)}");
    }

    private async Task LoopAsync()
    {
        while (!_stop.IsCancellationRequested)
        {
            try
            {
                Task<string?> sample = _check();
                Task finished = await Task.WhenAny(sample, _cluster.DelayAsync(TimeSpan.FromSeconds(5), _stop.Token));
                if (ReferenceEquals(finished, sample))
                {
                    string? violation = await sample;
                    Samples++;
                    if (violation is not null)
                        _violations.Add($"[{_cluster.Time.GetUtcNow():O}] {violation}");
                }
            }
            catch (InvalidOperationException)
            {
                // A node crashed/stopped between the state check and its inspection; skip this sample.
            }
            catch (OperationCanceledException) when (_stop.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _violations.Add($"safety check threw {ex.GetType().Name}: {ex.Message}");
            }

            try
            {
                await _cluster.DelayAsync(_interval, _stop.Token);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }
}

/// <summary>Monotonic logical clock for building <see cref="TxRecord"/> histories (one event per tick).</summary>
internal sealed class LogicalClock
{
    private long _now;

    /// <summary>Returns the next timestamp.</summary>
    public long Tick() => Interlocked.Increment(ref _now);
}
