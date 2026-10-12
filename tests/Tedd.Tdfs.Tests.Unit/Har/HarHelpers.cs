using System.Diagnostics;
using System.Globalization;
using System.Net.Sockets;
using System.Reflection;
using System.Reflection.Emit;
using System.Security.Cryptography;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Unit.Har;

/// <summary>Clock manipulations applied to one clock in HAR-003.</summary>
public enum ClockStep
{
    Advance,
    JumpWallForward,
    JumpWallBackward,
    Freeze,
    SetRate,
}

/// <summary>Ways a simulated component can escape the deterministic scheduler (HAR-002).</summary>
public enum HarEscape
{
    TaskRun,
    ThreadPoolWorkItem,
    SystemTimer,
    TaskDelayWithoutTimeProvider,
    SocketReceive,
}

/// <summary>Flush faults exercised by HAR-005.</summary>
public enum FlushFault
{
    FailFileFlush,
    FailFileFlushThenRetry,
    FailDirectoryFlush,
    ReadOnlyDevice,
    DelayedFlushInterruptedByCrash,
    ShortWrite,
    PathFilteredFailure,
}

/// <summary>Cluster-level flush faults exercised by HAR-005.</summary>
public enum ClusterFlushFault
{
    IoError,
    DelayBeyondDeadline,
}

/// <summary>Node process failure modes compared by HAR-008.</summary>
public enum NodeFailureMode
{
    Pause,
    Crash,
}

/// <summary>Which message direction is lost in HAR-006/HAR-009.</summary>
public enum MessageLoss
{
    DropRequests,
    DropReplies,
}

/// <summary>Which client/cluster direction is cut in HAR-006.</summary>
public enum ClientCut
{
    ClientToCluster,
    ClusterToClient,
}

/// <summary>Node-to-node link fault in HAR-006.</summary>
public enum LinkFault
{
    Cut,
    BlackHole,
}

/// <summary>Prerequisite withheld in HAR-015.</summary>
public enum WithheldPrerequisite
{
    QuorumCrashedMajority,
    QuorumPartitioned,
    Capacity,
}

/// <summary>Which devices lie about durability in HAR-018.</summary>
public enum DishonestDevices
{
    None,
    One,
    All,
}

/// <summary>Corruptions of byte-array outputs fed to the independent file oracle (HAR-010).</summary>
public enum ByteCorruption
{
    BitFlip,
    TruncatedTail,
    ZeroFilledTail,
    ExtraTrailingBytes,
    StaleCommittedImage,
    ResurrectedTruncatedBytes,
    TornMix,
}

/// <summary>Corruptions of namespace listings fed to the independent namespace oracle (HAR-010).</summary>
public enum NamespaceCorruption
{
    ResurrectedDeletedName,
    MissingEntry,
    InodeChangedByMove,
    StaleEntryAfterDirectoryMove,
    CopyOverwriteAllocatedNewInode,
    ReusedInodeForNewFile,
}

/// <summary>An unflushed change applied to a simulated file (HAR-001/HAR-004 crash images).</summary>
internal abstract record PendingChange
{
    public sealed record Write(long Offset, byte[] Data) : PendingChange;

    public sealed record SetLength(long Length) : PendingChange;
}

/// <summary>Outcome of the scripted simulated-medium crash used by HAR-001 and HAR-004.</summary>
internal sealed record CrashScriptResult(
    byte[] Baseline,
    IReadOnlyList<PendingChange> Pending,
    byte[] PreCrashView,
    byte[] RebootedView,
    byte[] DurableImage,
    bool UnflushedEntryExists,
    byte[]? UnflushedEntryContent,
    byte[] UnflushedEntryData,
    IReadOnlyList<string> AllFilesAfterCrash,
    bool OldHandleRejected);

/// <summary>Everything that must be identical when a deterministic run is replayed from the same seed (HAR-001).</summary>
internal sealed record ReplayFingerprint(
    ulong Seed,
    Guid ClusterId,
    IReadOnlyList<string> Nodes,
    IReadOnlyList<string> Outcomes,
    IReadOnlyList<string> ArmEvents,
    IReadOnlyList<string> CoverageHits,
    IReadOnlyList<string> Trace,
    string ClusterDigest,
    IReadOnlyList<string> NodeDigests,
    IReadOnlyList<string> History,
    long ElapsedVirtualTicks,
    long MessagesDelivered,
    long BytesDelivered);

/// <summary>What one isolated cluster run observed (HAR-016).</summary>
internal sealed record IsolationProbe(
    ulong Seed,
    Guid ClusterId,
    string DataRoot,
    IReadOnlyList<string> Endpoints,
    IReadOnlyList<string> NodeIds,
    IReadOnlyList<string> RootEntriesAtStart,
    IReadOnlyList<string> RootEntriesAfterWrite,
    string Marker,
    bool MarkerReadBackExact,
    string Digest);

/// <summary>State of the deliberately unfenced lock/storage toy protocol used by HAR-014.</summary>
/// <param name="Holder">Current lock holder ("A", "B") or null.</param>
/// <param name="Epoch">Newest granted lock epoch.</param>
/// <param name="AHeld">Epoch client A believes it holds (0 = none).</param>
/// <param name="BHeld">Epoch client B believes it holds (0 = none).</param>
/// <param name="StaleWriteAccepted">True once storage accepted a write from an epoch older than <paramref name="Epoch"/>.</param>
internal sealed record FenceState(string? Holder, int Epoch, int AHeld, int BHeld, bool StaleWriteAccepted)
{
    public const string Invariant = "INV-05: storage accepted a write from an obsolete lock epoch";

    public static FenceState Initial { get; } = new(null, 0, 0, 0, false);

    public string Key => $"{Holder ?? "-"}|{Epoch}|{AHeld}|{BHeld}|{StaleWriteAccepted}";

    /// <summary>All enabled actions. The storage does not check epochs (the bug); noise actions keep the state.</summary>
    public IEnumerable<(string Action, FenceState Next)> Next()
    {
        yield return ("A.noop", this);
        yield return ("B.noop", this);
        if (Holder is null && Epoch < 3)
        {
            yield return ("A.acquire", this with { Holder = "A", Epoch = Epoch + 1, AHeld = Epoch + 1 });
            yield return ("B.acquire", this with { Holder = "B", Epoch = Epoch + 1, BHeld = Epoch + 1 });
        }

        if (Holder is not null)
            yield return ("expire", this with { Holder = null });
        if (AHeld > 0)
            yield return ("A.write", this with { StaleWriteAccepted = StaleWriteAccepted || AHeld < Epoch });
        if (BHeld > 0)
            yield return ("B.write", this with { StaleWriteAccepted = StaleWriteAccepted || BHeld < Epoch });
        if (Holder == "A")
            yield return ("A.release", this with { Holder = null, AHeld = 0 });
        if (Holder == "B")
            yield return ("B.release", this with { Holder = null, BHeld = 0 });
    }

    public string? Check() => StaleWriteAccepted ? Invariant : null;

    /// <summary>Replays a saved trace of action names; returns the violated invariant, null, or "invalid-trace".</summary>
    public static string? Replay(IReadOnlyList<string> trace, out IReadOnlyList<FenceState> states)
    {
        var visited = new List<FenceState> { Initial };
        FenceState state = Initial;
        foreach (string action in trace)
        {
            FenceState? next = null;
            foreach ((string candidate, FenceState successor) in state.Next())
            {
                if (string.Equals(candidate, action, StringComparison.Ordinal))
                {
                    next = successor;
                    break;
                }
            }

            if (next is null)
            {
                states = visited;
                return "invalid-trace";
            }

            state = next;
            visited.Add(state);
        }

        states = visited;
        return state.Check();
    }
}

/// <summary>Shared helpers of the HAR suite.</summary>
internal static class HarHelpers
{
    public const string Vol = "vol";

    public static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>A deterministic simulation cluster (in-memory transport, simulated disks, virtual time).</summary>
    public static TestClusterOptions Deterministic(int nodes = 3, ulong seed = 0) =>
        TestClusterOptions.ThreeZones with
        {
            NodeCount = nodes,
            Mode = ClusterMode.Deterministic,
            Seed = seed,
            ScenarioTimeout = TimeSpan.FromMinutes(20),
        };

    public static string Hex(ReadOnlySpan<byte> data) => Convert.ToHexString(data);

    /// <summary>Awaits <paramref name="actual"/> and asserts byte equality (spans cannot live across an await).</summary>
    public static async Task BytesEqualAsync(byte[] expected, Task<byte[]> actual, string? because = null)
    {
        byte[] value = await actual;
        AssertEx.BytesEqual(expected, value, because);
    }

    public static string Sha(ReadOnlySpan<byte> data) => Convert.ToHexString(SHA256.HashData(data))[..16];

    /// <summary>"Type(ErrorCode)" for history fingerprints.</summary>
    public static string Describe(Exception ex) =>
        ex is DfsException dfs ? $"{ex.GetType().Name}({dfs.ErrorCode})" : ex.GetType().Name;

    /// <summary>
    /// True for failures that honestly report unavailability or an unknown outcome (never a false absence, a fabricated
    /// success or an unrelated error).
    /// </summary>
    public static bool IsAvailabilityFailure(Exception ex) => ex switch
    {
        DfsUnavailableException => true,
        DfsCommitOutcomeUnknownException => true,
        TimeoutException => true,
        DfsException { ErrorCode: DfsErrorCode.Timeout or DfsErrorCode.NotLeader or DfsErrorCode.NodeUnavailable or DfsErrorCode.QuorumUnavailable
            or DfsErrorCode.Unavailable or DfsErrorCode.OutcomeUnknown or DfsErrorCode.PlacementUnavailable or DfsErrorCode.CapacityExhausted } => true,
        _ => false,
    };

    // ---------------------------------------------------------------- history builders (HAR-009..012, HAR-017)

    public static HistoryOp Op(long id, string process, string kind, string key, object? input, long invoke, long? complete,
        OpOutcome outcome, object? output = null) =>
        new(id, process, kind, key, input, invoke, complete, outcome, output, null);

    public static byte[] V(string text) => System.Text.Encoding.UTF8.GetBytes(text);

    public static TxRecord Tx(string id, long invoke, long? complete, OpOutcome outcome, (string Path, string? Value)[] reads,
        (string Path, string? Value)[] writes, params (string Directory, string[] Names)[] predicates) =>
        new(id, invoke, complete, outcome,
            reads.ToDictionary(r => r.Path, r => r.Value, StringComparer.Ordinal),
            writes.ToDictionary(w => w.Path, w => w.Value, StringComparer.Ordinal),
            predicates.Select(p => (p.Directory, (IReadOnlySet<string>)new HashSet<string>(p.Names, StringComparer.Ordinal))).ToList());

    public static Dictionary<string, string?> State(params (string Path, string? Value)[] values) =>
        values.ToDictionary(v => v.Path, v => v.Value, StringComparer.Ordinal);

    /// <summary>
    /// Generates a legal concurrent register history by construction: every applied operation takes effect at a hidden
    /// linearization point strictly inside its interval, and read outputs are computed from that hidden order. Unknown
    /// operations (last op of some processes) may or may not have taken effect.
    /// </summary>
    public static IReadOnlyList<HistoryOp> GenerateLegalRegisterHistory(ulong seed, int processes, int opsPerProcess, int keys)
    {
        var random = new SeededRandom(seed);
        var planned = new List<(int Process, string Kind, string Key, byte[]? Value, long Invoke, long Lin, long? Complete, bool Applied, OpOutcome Outcome)>();
        for (int p = 0; p < processes; p++)
        {
            long t = random.Next(20);
            for (int i = 0; i < opsPerProcess; i++)
            {
                long invoke = t;
                long duration = 2 + random.Next(40);
                long lin = invoke + 1 + random.Next((int)duration - 1);
                long complete = invoke + duration;
                bool isWrite = random.Next(2) == 0;
                string key = "k" + random.Next(keys).ToString(CultureInfo.InvariantCulture);
                byte[]? value = isWrite ? V($"p{p}-o{i}-s{seed}") : null;
                bool last = i == opsPerProcess - 1;
                bool unknown = last && random.Next(3) == 0;
                bool applied = !unknown || random.Next(2) == 0;
                planned.Add((p, isWrite ? "write" : "read", key, value, invoke * 16 + p, lin * 16 + p, unknown ? null : complete * 16 + p, applied,
                    unknown ? OpOutcome.Unknown : OpOutcome.Ok));
                t = complete + 1 + random.Next(5);
            }
        }

        var current = new Dictionary<string, byte[]?>(StringComparer.Ordinal);
        var outputs = new Dictionary<int, byte[]?>();
        foreach (int index in Enumerable.Range(0, planned.Count).Where(i => planned[i].Applied).OrderBy(i => planned[i].Lin))
        {
            var op = planned[index];
            if (op.Kind == "write")
                current[op.Key] = op.Value;
            else
                outputs[index] = current.TryGetValue(op.Key, out byte[]? v) ? v : null;
        }

        var history = new List<HistoryOp>();
        for (int i = 0; i < planned.Count; i++)
        {
            var op = planned[i];
            history.Add(new HistoryOp(i + 1, "p" + op.Process.ToString(CultureInfo.InvariantCulture), op.Kind, op.Key, op.Value, op.Invoke, op.Complete,
                op.Outcome, op.Kind == "read" && op.Outcome == OpOutcome.Ok ? outputs[i] : null, null));
        }

        return history;
    }

    // ---------------------------------------------------------------- simulated medium scripts (HAR-001, HAR-004)

    /// <summary>Applies changes to a plain byte array (independent of the medium's paged buffers).</summary>
    public static byte[] ApplyChanges(byte[] image, IEnumerable<PendingChange> changes)
    {
        byte[] current = image.ToArray();
        foreach (PendingChange change in changes)
        {
            switch (change)
            {
                case PendingChange.Write w when w.Data.Length > 0:
                    long end = w.Offset + w.Data.Length;
                    if (end > current.Length)
                        Array.Resize(ref current, (int)end);
                    w.Data.CopyTo(current, (int)w.Offset);
                    break;
                case PendingChange.SetLength s:
                    Array.Resize(ref current, (int)s.Length);
                    break;
            }
        }

        return current;
    }

    /// <summary>Every image a crash in <paramref name="mode"/> may legally leave behind (hex strings).</summary>
    public static HashSet<string> AllowedCrashImages(byte[] durable, IReadOnlyList<PendingChange> pending, CrashMode mode)
    {
        var images = new HashSet<string>(StringComparer.Ordinal);
        switch (mode)
        {
            case CrashMode.LoseUnflushed:
                images.Add(Hex(durable));
                break;
            case CrashMode.KeepUnflushed:
                images.Add(Hex(ApplyChanges(durable, pending)));
                break;
            case CrashMode.RandomSubset:
                for (int mask = 0; mask < 1 << pending.Count; mask++)
                    images.Add(Hex(ApplyChanges(durable, pending.Where((_, i) => (mask & (1 << i)) != 0))));
                break;
            case CrashMode.TearLastWrite:
                int last = pending.Select((c, i) => (c, i)).Last(x => x.c is PendingChange.Write).i;
                var torn = (PendingChange.Write)pending[last];
                for (int keep = 0; keep < torn.Data.Length; keep++)
                {
                    var changes = pending.ToList();
                    changes[last] = torn with { Data = torn.Data[..keep] };
                    images.Add(Hex(ApplyChanges(durable, changes)));
                }

                break;
        }

        return images;
    }

    public static async Task<byte[]> ReadAllAsync(IStorageFile file)
    {
        var buffer = new byte[file.Length];
        if (buffer.Length > 0)
            await file.ReadFullyAsync(0, buffer, Ct);
        return buffer;
    }

    /// <summary>
    /// Durable baseline + unflushed changes on "d/f", plus "d/g" whose data was flushed but whose directory entry was not;
    /// then a crash in <paramref name="mode"/> and a reboot view of both files.
    /// </summary>
    public static async Task<CrashScriptResult> RunCrashScriptAsync(CrashMode mode, ulong crashSeed, ulong dataSeed = 7)
    {
        CancellationToken ct = Ct;
        var medium = new SimulatedMedium("har-crash", 1 << 20, new SeededRandom(crashSeed));
        byte[] baseline = TestData.Bytes(dataSeed, 4096);
        await medium.CreateDirectoryAsync("d", ct);
        await medium.FlushDirectoryAsync("", ct);
        IStorageFile f = await medium.OpenAsync("d/f", StorageOpenMode.CreateNew, ct);
        await f.WriteFullyAsync(0, baseline, ct);
        await f.FlushAsync(ct);
        await medium.FlushDirectoryAsync("d", ct);
        AssertEx.BytesEqual(baseline, medium.ReadDurable("d/f"), "baseline must be durable after file and directory flush");

        var pending = new List<PendingChange>
        {
            new PendingChange.Write(100, TestData.Bytes(dataSeed + 1, 64)),
            new PendingChange.SetLength(3000),
            new PendingChange.Write(5000, TestData.Bytes(dataSeed + 2, 100)),
        };

        // The last write is the bitwise complement of what it overwrites, so any torn prefix is distinguishable.
        byte[] underLast = ApplyChanges(baseline, pending).AsSpan(10, 200).ToArray();
        pending.Add(new PendingChange.Write(10, underLast.Select(b => (byte)~b).ToArray()));
        foreach (PendingChange change in pending)
        {
            switch (change)
            {
                case PendingChange.Write w:
                    Assert.Equal(w.Data.Length, await f.WriteAsync(w.Offset, w.Data, ct));
                    break;
                case PendingChange.SetLength s:
                    await f.SetLengthAsync(s.Length, ct);
                    break;
            }
        }

        byte[] preCrash = await ReadAllAsync(f);
        AssertEx.BytesEqual(ApplyChanges(baseline, pending), preCrash, "the volatile view shows every unflushed change");
        AssertEx.BytesEqual(baseline, medium.ReadDurable("d/f"), "unflushed changes are not durable before the crash");

        byte[] gData = TestData.Bytes(dataSeed + 4, 333);
        IStorageFile g = await medium.OpenAsync("d/g", StorageOpenMode.CreateNew, ct);
        await g.WriteFullyAsync(0, gData, ct);
        await g.FlushAsync(ct); // data barrier only; the directory entry stays volatile

        medium.Crash(mode);

        bool oldHandleRejected;
        try
        {
            await f.ReadAsync(0, new byte[1], ct);
            oldHandleRejected = false;
        }
        catch (ObjectDisposedException)
        {
            oldHandleRejected = true;
        }

        await using IStorageFile rebooted = await medium.OpenAsync("d/f", StorageOpenMode.OpenExisting, ct);
        byte[] rebootedView = await ReadAllAsync(rebooted);
        byte[] durable = medium.ReadDurable("d/f");
        bool gExists = await medium.ExistsAsync("d/g", ct);
        byte[]? gContent = null;
        if (gExists)
        {
            await using IStorageFile g2 = await medium.OpenAsync("d/g", StorageOpenMode.OpenExisting, ct);
            gContent = await ReadAllAsync(g2);
        }

        return new CrashScriptResult(baseline, pending, preCrash, rebootedView, durable, gExists, gContent, gData, medium.AllFiles(), oldHandleRejected);
    }

    // ---------------------------------------------------------------- HAR-001 replay scenario

    /// <summary>A fixed schedule of operations and faults whose every observable is captured for replay comparison.</summary>
    public static async Task<ReplayFingerprint> RunReplayScenarioAsync(TestCluster cluster)
    {
        CancellationToken ct = Ct;
        long start = cluster.Time.GetTimestamp();
        var outcomes = new List<string>();
        TestNode leader = await cluster.GetLeaderAsync(Vol, ct);
        TestNode follower = cluster.Nodes.First(n => !string.Equals(n.Name, leader.Name, StringComparison.Ordinal));
        await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { OperationTimeout = TimeSpan.FromSeconds(3) }, ct);
        DfsVolume vol = client.GetVolume(Vol);

        FaultArm crash = cluster.Faults.Arm(new FaultMatch(FaultSites.ObjectBeforeFlush, Node: follower.Name, Volume: Vol),
            FaultAction.CrashNode(CrashMode.RandomSubset));
        FaultArm delay = cluster.Faults.Arm(new FaultMatch(FaultSites.RaftAfterCommitBeforeApply, Node: leader.Name, Occurrence: 3),
            FaultAction.Delay(TimeSpan.FromMilliseconds(40)));
        MessageRule dropReply = cluster.Network.DropReplies(new MessageFilter(From: "client:c0", Operation: DfsOperationKind.Append), count: 1);

        async Task Step(string kind, string key, Func<Task<CommitToken>> action)
        {
            try
            {
                CommitToken token = await cluster.History.RecordAsync("c0", kind, key, null, action);
                outcomes.Add($"{kind} {key} ok {token}");
            }
            catch (Exception ex)
            {
                outcomes.Add($"{kind} {key} {HistoryRecorder.Classify(ex)} {Describe(ex)}");
            }
        }

        async Task Read(string key)
        {
            try
            {
                byte[] bytes = await cluster.History.RecordAsync("c0", "read", key, null, () => vol.Files.ReadAllBytesAsync(key, ct));
                outcomes.Add($"read {key} ok {bytes.Length} {Sha(bytes)}");
            }
            catch (Exception ex)
            {
                outcomes.Add($"read {key} {HistoryRecorder.Classify(ex)} {Describe(ex)}");
            }
        }

        await Step("write", "/har001-a", () => vol.Files.WriteAllBytesAsync("/har001-a", TestData.Bytes(1, 1000), ct));
        await Step("write", "/har001-big", () => vol.Files.WriteAllBytesAsync("/har001-big", TestData.Bytes(2, 200 * 1024 + 7), ct));
        for (int i = 0; i < 3; i++)
        {
            byte[] entry = TestData.Bytes(10 + (ulong)i, 300);
            await Step("append", "/har001-log", () => vol.Files.AppendAllBytesAsync("/har001-log", entry, ct));
        }

        await Read("/har001-a");
        await Read("/har001-big");
        await Read("/har001-log");
        if (follower.State != TestNodeState.Running)
            await follower.RestartAsync();
        await cluster.WaitForReplicationAsync(Vol, ct);
        await Step("move", "/har001-a", () => vol.Files.MoveAsync("/har001-a", "/har001-b", overwrite: false, ct));
        await Step("tx", "/har001-tx", async () =>
        {
            await using DfsTransaction tx = await vol.Transactions.BeginAsync(null, ct);
            await tx.Files.WriteAllBytesAsync("/har001-tx", TestData.Bytes(3, 5000), ct);
            await tx.Files.DeleteAsync("/har001-big", ct);
            return await tx.CommitAsync(ct);
        });
        await cluster.DelayAsync(TimeSpan.FromSeconds(2), ct);
        await Read("/har001-b");
        await Read("/har001-tx");

        var armEvents = new List<string>
        {
            $"crash hits={crash.Hits} triggered={crash.Triggered}",
            $"delay hits={delay.Hits} triggered={delay.Triggered}",
            $"drop-reply matched={dropReply.Matched} remaining={dropReply.Remaining}",
        };
        armEvents.AddRange(crash.Contexts.Select(c => "crash@" + c));
        armEvents.AddRange(delay.Contexts.Select(c => "delay@" + c));

        var nodeDigests = new List<string>();
        foreach (TestNode node in cluster.Nodes.Where(n => n.State == TestNodeState.Running))
            nodeDigests.Add($"{node.Name}:{node.Incarnation}:{await node.Inspect.ComputeStateDigestAsync()}");

        return new ReplayFingerprint(
            cluster.Seed,
            cluster.ClusterId,
            cluster.Nodes.Select(n => $"{n.Name}:{n.Id}:{n.Labels}").ToList(),
            outcomes,
            armEvents,
            cluster.Faults.Coverage.Hits.Select(h => h.ToString()).ToList(),
            cluster.Trace.ToList(),
            await cluster.ComputeDigestAsync(ct),
            nodeDigests,
            cluster.History.Operations.Select(o =>
                $"{o.Id}|{o.Process}|{o.Kind}|{o.Key}|{o.InvokeTime}|{o.CompleteTime}|{o.ResolveTime}|{o.Outcome}|{o.OperationId}").ToList(),
            cluster.Time.GetElapsedTime(start).Ticks,
            cluster.Network.Log.CountMessages(MessageFilter.Any),
            cluster.Network.Log.CountBytes(MessageFilter.Any));
    }

    public static void AssertSameReplay(ReplayFingerprint expected, ReplayFingerprint actual)
    {
        Assert.Equal(expected.Seed, actual.Seed);
        Assert.Equal(expected.ClusterId, actual.ClusterId);
        Assert.Equal(expected.Nodes, actual.Nodes);
        Assert.Equal(expected.Outcomes, actual.Outcomes);
        Assert.Equal(expected.ArmEvents, actual.ArmEvents);
        Assert.Equal(expected.CoverageHits, actual.CoverageHits);
        Assert.Equal(expected.Trace, actual.Trace);
        Assert.Equal(expected.ClusterDigest, actual.ClusterDigest);
        Assert.Equal(expected.NodeDigests, actual.NodeDigests);
        Assert.Equal(expected.History, actual.History);
        Assert.Equal(expected.ElapsedVirtualTicks, actual.ElapsedVirtualTicks);
        Assert.Equal(expected.MessagesDelivered, actual.MessagesDelivered);
        Assert.Equal(expected.BytesDelivered, actual.BytesDelivered);
    }

    // ---------------------------------------------------------------- HAR-002 escapes

    /// <summary>Performs work whose completion arrives on a thread the simulation does not control.</summary>
    public static async Task EscapeAsync(HarEscape escape)
    {
        switch (escape)
        {
            case HarEscape.TaskRun:
            {
                int value = await Task.Run(() =>
                {
                    Thread.Sleep(30);
                    return 42;
                });
                Assert.Equal(42, value);
                break;
            }

            case HarEscape.ThreadPoolWorkItem:
            {
                var tcs = new TaskCompletionSource<int>();
                ThreadPool.QueueUserWorkItem(_ =>
                {
                    Thread.Sleep(30);
                    tcs.SetResult(7);
                });
                Assert.Equal(7, await tcs.Task);
                break;
            }

            case HarEscape.SystemTimer:
            {
                var tcs = new TaskCompletionSource<int>();
                using var timer = new Timer(_ => tcs.TrySetResult(3), null, 30, Timeout.Infinite);
                Assert.Equal(3, await tcs.Task);
                break;
            }

            case HarEscape.TaskDelayWithoutTimeProvider:
                await Task.Delay(30);
                break;

            case HarEscape.SocketReceive:
            {
                using var listener = new TcpListener(System.Net.IPAddress.Loopback, 0);
                listener.Start();
                var endpoint = (System.Net.IPEndPoint)listener.LocalEndpoint;
                using var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                Task<Socket> accept = listener.AcceptSocketAsync();
                await socket.ConnectAsync(endpoint);
                using Socket server = await accept;
                var sender = new Thread(() =>
                {
                    Thread.Sleep(30);
                    server.Send(new byte[] { 1, 2, 3 });
                });
                sender.Start();
                var buffer = new byte[3];
                int received = await socket.ReceiveAsync(buffer.AsMemory(), SocketFlags.None);
                sender.Join();
                Assert.Equal(3, received);
                break;
            }

            default:
                throw new ArgumentOutOfRangeException(nameof(escape), escape, null);
        }
    }

    // ---------------------------------------------------------------- HAR-014 cluster schedule

    /// <summary>
    /// Executes a schedule of steps against a deterministic cluster (optionally with server-side operation dedup disabled,
    /// a negative control) and returns the violated invariant of the "/h14-log" append history, or null when it holds.
    /// Steps: "write:{name}:{seed}", "append:{n}", "append-lost-reply:{n}", "pause-follower", "resume-follower", "delay:{ms}".
    /// </summary>
    public static Task<string?> RunDedupScheduleAsync(IReadOnlyList<string> steps, ulong seed, bool dedupDisabled)
    {
        TestClusterOptions options = Deterministic(3, seed) with
        {
            Mutations = dedupDisabled ? new SafetyMutations { DisableOperationDedup = true } : SafetyMutations.None,
        };
        return ClusterTest.RunAsync<string?>(options, async cluster =>
        {
            CancellationToken ct = Ct;
            const string log = "/h14-log";
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { OperationTimeout = TimeSpan.FromSeconds(2) }, ct);
            DfsVolume vol = client.GetVolume(Vol);
            await vol.Files.WriteAllBytesAsync(log, ReadOnlyMemory<byte>.Empty, ct);
            TestNode? paused = null;

            async Task AppendAsync(int n, bool loseReply)
            {
                byte[] entry = TestData.Bytes(1000 + (ulong)n, 64);
                DfsOperationId id = await vol.NewOperationIdAsync(ct);
                long hid = cluster.History.Invoke("c0", "append", log, entry, id);
                MessageRule? rule = loseReply
                    ? cluster.Network.DropReplies(new MessageFilter(From: "client:c0", Operation: DfsOperationKind.Append), count: 1)
                    : null;
                try
                {
                    await vol.Files.AppendAllBytesAsync(log, entry, new DfsMutationOptions { OperationId = id }, ct);
                    cluster.History.Complete(hid, OpOutcome.Ok);
                }
                catch (Exception ex) when (HistoryRecorder.Classify(ex) == OpOutcome.Unknown)
                {
                    // The documented recovery path: retry with the same operation identity.
                    await vol.Files.AppendAllBytesAsync(log, entry, new DfsMutationOptions { OperationId = id }, ct);
                    cluster.History.Resolve(hid, OpOutcome.Ok);
                }
                finally
                {
                    rule?.Cancel();
                }
            }

            foreach (string step in steps)
            {
                string[] parts = step.Split(':');
                switch (parts[0])
                {
                    case "write":
                        await vol.Files.WriteAllBytesAsync("/h14-" + parts[1], TestData.Bytes(ulong.Parse(parts[2], CultureInfo.InvariantCulture), 512), ct);
                        break;
                    case "append":
                        await AppendAsync(int.Parse(parts[1], CultureInfo.InvariantCulture), loseReply: false);
                        break;
                    case "append-lost-reply":
                        await AppendAsync(int.Parse(parts[1], CultureInfo.InvariantCulture), loseReply: true);
                        break;
                    case "pause-follower":
                        if (paused is null)
                        {
                            TestNode leader = await cluster.GetLeaderAsync(Vol, ct);
                            paused = cluster.Nodes.First(n => !string.Equals(n.Name, leader.Name, StringComparison.Ordinal));
                            await paused.PauseAsync();
                        }

                        break;
                    case "resume-follower":
                        if (paused is not null)
                        {
                            await paused.ResumeAsync();
                            paused = null;
                        }

                        break;
                    case "delay":
                        await cluster.DelayAsync(TimeSpan.FromMilliseconds(int.Parse(parts[1], CultureInfo.InvariantCulture)), ct);
                        break;
                    default:
                        throw new ArgumentException($"Unknown step '{step}'.", nameof(steps));
                }
            }

            if (paused is not null)
                await paused.ResumeAsync();
            await cluster.History.RecordAsync("c0", "read", log, null, () => vol.Files.ReadAllBytesAsync(log, ct));
            CheckResult result = LinearizabilityChecker.Check(
                cluster.History.Operations.Where(o => string.Equals(o.Key, log, StringComparison.Ordinal)).ToList(), new AppendLogModel());
            if (result.IsInconclusive)
                return "inconclusive";
            return result.IsValid ? null : "INV-06";
        });
    }

    // ---------------------------------------------------------------- HAR-016 isolation probe

    public static async Task<IsolationProbe> ProbeIsolationAsync(TestCluster cluster)
    {
        CancellationToken ct = Ct;
        await using DfsClient client = await cluster.ConnectAsync(null, ct);
        DfsVolume vol = client.GetVolume(Vol);
        var before = new List<string>();
        await foreach (DfsEntryInfo entry in vol.Directories.EnumerateAsync("/", null, ct))
            before.Add(entry.Path);
        string marker = "/iso-" + cluster.Seed.ToString("x16", CultureInfo.InvariantCulture);
        byte[] content = TestData.Bytes(cluster.Seed, 777);
        await vol.Files.WriteAllBytesAsync(marker, content, ct);
        byte[] back = await vol.Files.ReadAllBytesAsync(marker, ct);
        var after = new List<string>();
        await foreach (DfsEntryInfo entry in vol.Directories.EnumerateAsync("/", null, ct))
            after.Add(entry.Path);
        return new IsolationProbe(
            cluster.Seed,
            cluster.ClusterId,
            cluster.DataRoot,
            cluster.Nodes.Select(n => n.Endpoint).ToList(),
            cluster.Nodes.Select(n => n.Id.ToString()).ToList(),
            before,
            after,
            marker,
            back.AsSpan().SequenceEqual(content),
            await cluster.ComputeDigestAsync(ct));
    }

    public static bool IsSameOrNestedDirectory(string a, string b)
    {
        string fa = Path.GetFullPath(a).TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar) + Path.DirectorySeparatorChar;
        string fb = Path.GetFullPath(b).TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar) + Path.DirectorySeparatorChar;
        return fa.StartsWith(fb, StringComparison.OrdinalIgnoreCase) || fb.StartsWith(fa, StringComparison.OrdinalIgnoreCase);
    }
}

/// <summary>A reference to an implementation member found in IL.</summary>
internal sealed record AuditFinding(UncontrolledDependencyKind Kind, string Location, string Target)
{
    public override string ToString() => $"{Kind}: {Location} -> {Target}";
}

/// <summary>Result of an IL audit.</summary>
internal sealed record AuditReport(IReadOnlyList<AuditFinding> Findings, int TypesScanned, int MethodsScanned, IReadOnlyList<string> Unresolved);

/// <summary>Which rule sets an audit applies.</summary>
[Flags]
internal enum AuditRules
{
    None = 0,

    /// <summary>Wall clock, randomness, timers, thread pool, threads, sockets, disk.</summary>
    Core = 1,

    /// <summary>ConfigureAwait(false) leaves the node loop (server code).</summary>
    ServerContext = 2,

    /// <summary>Task.Yield, SemaphoreSlim, RunContinuationsAsynchronously (SDK request path, ARCHITECTURE §9.6).</summary>
    SdkPrimitives = 4,

    Server = Core | ServerContext,
    Sdk = Core | SdkPrimitives,
    All = Core | ServerContext | SdkPrimitives,
}

/// <summary>
/// Static IL audit of simulated components (HAR-002) and reference scanner for oracle independence (HAR-010). Walks the
/// IL of every method (including compiler-generated state machines and closures) and resolves member tokens.
/// </summary>
internal static class IlAudit
{
    private const BindingFlags AllDeclared = BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.NonPublic |
        BindingFlags.Instance | BindingFlags.Static;

    private static readonly OpCode?[] OneByte = new OpCode?[256];
    private static readonly OpCode?[] TwoByte = new OpCode?[256];

    static IlAudit()
    {
        foreach (FieldInfo field in typeof(OpCodes).GetFields(BindingFlags.Public | BindingFlags.Static))
        {
            if (field.GetValue(null) is not OpCode op)
                continue;
            ushort value = unchecked((ushort)op.Value);
            if (op.Size == 1)
                OneByte[value & 0xFF] = op;
            else
                TwoByte[value & 0xFF] = op;
        }
    }

    /// <summary>Audits the given types and all their nested types.</summary>
    public static AuditReport ScanTypes(IEnumerable<Type> roots, AuditRules rules)
    {
        var types = new List<Type>();
        var stack = new Stack<Type>(roots);
        while (stack.Count > 0)
        {
            Type type = stack.Pop();
            types.Add(type);
            foreach (Type nested in type.GetNestedTypes(BindingFlags.Public | BindingFlags.NonPublic))
                stack.Push(nested);
        }

        return Scan(types, rules, []);
    }

    /// <summary>Audits every type of an assembly except those whose outermost declaring type is excluded.</summary>
    public static AuditReport ScanAssembly(Assembly assembly, AuditRules rules, Func<Type, bool>? exclude = null)
    {
        var unresolved = new List<string>();
        Type[] types;
        try
        {
            types = assembly.GetTypes();
        }
        catch (ReflectionTypeLoadException ex)
        {
            types = ex.Types.Where(t => t is not null).Select(t => t!).ToArray();
            unresolved.AddRange(ex.LoaderExceptions.Select(e => "type load: " + e?.Message));
        }

        IEnumerable<Type> selected = exclude is null ? types : types.Where(t => !exclude(Outermost(t)));
        return Scan(selected.ToList(), rules, unresolved);
    }

    /// <summary>
    /// All types referenced (signatures and IL) by <paramref name="roots"/> and, transitively, by every referenced type for
    /// which <paramref name="follow"/> returns true.
    /// </summary>
    public static (IReadOnlySet<Type> Referenced, int TypesScanned, IReadOnlyList<string> Unresolved) ReferencedTypes(IEnumerable<Type> roots, Func<Type, bool> follow)
    {
        var referenced = new HashSet<Type>();
        var scanned = new HashSet<Type>();
        var unresolved = new List<string>();
        var queue = new Queue<Type>(roots);

        void Note(Type? type)
        {
            if (type is null)
                return;
            while (type.HasElementType)
                type = type.GetElementType()!;
            if (type.IsGenericParameter)
                return;
            if (type.IsGenericType && !type.IsGenericTypeDefinition)
            {
                foreach (Type argument in type.GetGenericArguments())
                    Note(argument);
                type = type.GetGenericTypeDefinition();
            }

            if (referenced.Add(type) && follow(type) && !scanned.Contains(type))
                queue.Enqueue(type);
        }

        while (queue.Count > 0)
        {
            Type type = queue.Dequeue();
            if (!scanned.Add(type))
                continue;
            referenced.Add(type);
            Note(type.BaseType);
            foreach (Type iface in type.GetInterfaces())
                Note(iface);
            foreach (Type nested in type.GetNestedTypes(BindingFlags.Public | BindingFlags.NonPublic))
                Note(nested);
            foreach (FieldInfo field in type.GetFields(AllDeclared))
                Note(field.FieldType);
            foreach (PropertyInfo property in type.GetProperties(AllDeclared))
                Note(property.PropertyType);
            foreach (MethodBase method in Methods(type))
            {
                if (method is MethodInfo mi)
                    Note(mi.ReturnType);
                foreach (ParameterInfo parameter in method.GetParameters())
                    Note(parameter.ParameterType);
                foreach ((MemberInfo member, _) in References(method, unresolved))
                {
                    switch (member)
                    {
                        case Type t:
                            Note(t);
                            break;
                        case MethodBase mb:
                            Note(mb.DeclaringType);
                            if (mb is MethodInfo rmi)
                                Note(rmi.ReturnType);
                            foreach (ParameterInfo p in SafeParameters(mb))
                                Note(p.ParameterType);
                            break;
                        case FieldInfo f:
                            Note(f.DeclaringType);
                            Note(f.FieldType);
                            break;
                        default:
                            Note(member.DeclaringType);
                            break;
                    }
                }
            }
        }

        return (referenced, scanned.Count, unresolved);
    }

    private static AuditReport Scan(IReadOnlyList<Type> types, AuditRules rules, List<string> unresolved)
    {
        var findings = new List<AuditFinding>();
        int methods = 0;
        foreach (Type type in types)
        {
            foreach (MethodBase method in Methods(type))
            {
                methods++;
                foreach ((MemberInfo member, int? preceding) in References(method, unresolved))
                {
                    UncontrolledDependencyKind? kind = Classify(member, preceding, rules);
                    if (kind is not null)
                        findings.Add(new AuditFinding(kind.Value, $"{method.DeclaringType?.FullName}::{method.Name}", $"{member.DeclaringType?.FullName}::{member.Name}"));
                }
            }
        }

        return new AuditReport(findings, types.Count, methods, unresolved);
    }

    private static Type Outermost(Type type)
    {
        while (type.DeclaringType is not null)
            type = type.DeclaringType;
        return type;
    }

    private static IEnumerable<MethodBase> Methods(Type type)
    {
        IEnumerable<MethodBase> methods;
        try
        {
            methods = type.GetMethods(AllDeclared).Cast<MethodBase>().Concat(type.GetConstructors(AllDeclared)).ToList();
        }
        catch (Exception ex) when (ex is TypeLoadException or FileNotFoundException or FileLoadException)
        {
            methods = [];
        }

        return methods;
    }

    private static ParameterInfo[] SafeParameters(MethodBase method)
    {
        try
        {
            return method.GetParameters();
        }
        catch (Exception ex) when (ex is TypeLoadException or FileNotFoundException or FileLoadException)
        {
            return [];
        }
    }

    /// <summary>Member tokens referenced by a method body, each with the int constant loaded immediately before (if any).</summary>
    private static List<(MemberInfo Member, int? PrecedingInt)> References(MethodBase method, List<string> unresolved)
    {
        var result = new List<(MemberInfo, int?)>();
        byte[]? il;
        try
        {
            il = method.GetMethodBody()?.GetILAsByteArray();
        }
        catch (Exception ex) when (ex is InvalidOperationException or NotSupportedException or BadImageFormatException)
        {
            il = null;
        }

        if (il is null)
            return result;

        Type[]? typeArguments = method.DeclaringType is { IsGenericType: true } declaring ? declaring.GetGenericArguments() : null;
        Type[]? methodArguments = method is MethodInfo { IsGenericMethod: true } generic ? generic.GetGenericArguments() : null;
        int position = 0;
        int? lastInt = null;
        while (position < il.Length)
        {
            byte first = il[position++];
            OpCode? decoded = first == 0xFE ? (position < il.Length ? TwoByte[il[position++]] : null) : OneByte[first];
            if (decoded is not OpCode op)
            {
                unresolved.Add($"{method.DeclaringType?.FullName}::{method.Name}: undecodable IL at {position - 1}");
                break;
            }

            int operand = position;
            position += op.OperandType switch
            {
                OperandType.InlineNone => 0,
                OperandType.ShortInlineBrTarget or OperandType.ShortInlineI or OperandType.ShortInlineVar => 1,
                OperandType.InlineVar => 2,
                OperandType.InlineI8 or OperandType.InlineR => 8,
                OperandType.InlineSwitch => 4 + (4 * BitConverter.ToInt32(il, operand)),
                _ => 4,
            };

            int? thisInt = op.Value switch
            {
                0x15 => -1,
                >= 0x16 and <= 0x1E => op.Value - 0x16,
                0x1F => unchecked((sbyte)il[operand]),
                0x20 => BitConverter.ToInt32(il, operand),
                _ => null,
            };

            if (op.OperandType is OperandType.InlineMethod or OperandType.InlineField or OperandType.InlineType or OperandType.InlineTok)
            {
                int token = BitConverter.ToInt32(il, operand);
                try
                {
                    MemberInfo? member = method.Module.ResolveMember(token, typeArguments, methodArguments);
                    if (member is not null)
                        result.Add((member, lastInt));
                }
                catch (Exception ex) when (ex is ArgumentException or BadImageFormatException or FileNotFoundException or FileLoadException
                    or TypeLoadException or MissingMemberException)
                {
                    unresolved.Add($"{method.DeclaringType?.FullName}::{method.Name}: token 0x{token:X8}: {ex.GetType().Name}");
                }
            }

            lastInt = thisInt;
        }

        return result;
    }

    private static UncontrolledDependencyKind? Classify(MemberInfo member, int? preceding, AuditRules rules)
    {
        Type? declaring = member.DeclaringType;
        if (declaring is null)
            return null;
        Type type = declaring.IsGenericType ? declaring.GetGenericTypeDefinition() : declaring;
        string name = member.Name;
        bool isConstructor = member is ConstructorInfo;
        ParameterInfo[] parameters = member is MethodBase mb ? SafeParameters(mb) : [];
        bool hasTimeProvider = parameters.Any(p => p.ParameterType == typeof(TimeProvider));
        bool isTask = type == typeof(Task) || type == typeof(Task<>);
        bool isAwaitable = isTask || type == typeof(ValueTask) || type == typeof(ValueTask<>);

        if ((rules & AuditRules.Core) != 0)
        {
            if (type == typeof(DateTime) && name is "get_UtcNow" or "get_Now" or "get_Today")
                return UncontrolledDependencyKind.WallClock;
            if (type == typeof(DateTimeOffset) && name is "get_UtcNow" or "get_Now")
                return UncontrolledDependencyKind.WallClock;
            if (type == typeof(Environment) && name is "get_TickCount" or "get_TickCount64")
                return UncontrolledDependencyKind.WallClock;
            if (type == typeof(Stopwatch))
                return UncontrolledDependencyKind.WallClock;
            if (type == typeof(Random) && (isConstructor || name == "get_Shared"))
                return UncontrolledDependencyKind.Random;
            if (type == typeof(Guid) && name is "NewGuid" or "CreateVersion7")
                return UncontrolledDependencyKind.Random;
            if (type == typeof(RandomNumberGenerator) || type.IsSubclassOf(typeof(RandomNumberGenerator)))
                return UncontrolledDependencyKind.Random;
            if ((type == typeof(Timer) || type == typeof(System.Timers.Timer)) && isConstructor)
                return UncontrolledDependencyKind.Timer;
            if (type == typeof(PeriodicTimer) && isConstructor && !hasTimeProvider)
                return UncontrolledDependencyKind.Timer;
            if (isTask && name == "Delay" && !hasTimeProvider)
                return UncontrolledDependencyKind.Timer;
            if (isAwaitable && name == "WaitAsync" && parameters.Any(p => p.ParameterType == typeof(TimeSpan)) && !hasTimeProvider)
                return UncontrolledDependencyKind.Timer;
            // A timeout source created without a TimeProvider runs on the system timer. (CancelAfter is not flagged: on a
            // source constructed with a TimeProvider it uses that provider, which a static scan cannot tell apart.)
            if (type == typeof(CancellationTokenSource) && isConstructor
                && parameters.Any(p => p.ParameterType == typeof(TimeSpan) || p.ParameterType == typeof(int)) && !hasTimeProvider)
                return UncontrolledDependencyKind.Timer;
            if (isTask && name == "Run")
                return UncontrolledDependencyKind.ThreadPool;
            if ((type == typeof(TaskFactory) || type == typeof(TaskFactory<>)) && name == "StartNew")
                return UncontrolledDependencyKind.ThreadPool;
            if (type == typeof(ThreadPool) || type == typeof(Parallel))
                return UncontrolledDependencyKind.ThreadPool;
            if (type == typeof(Thread) && (isConstructor || name is "Sleep" or "Start" or "UnsafeStart"))
                return UncontrolledDependencyKind.Thread;
            if (string.Equals(type.Namespace, "System.Net.Sockets", StringComparison.Ordinal) || type == typeof(HttpClient)
                || type == typeof(HttpMessageInvoker) || type == typeof(System.Net.Dns))
                return UncontrolledDependencyKind.Socket;
            if (type == typeof(File) || type == typeof(Directory) || type == typeof(FileInfo) || type == typeof(DirectoryInfo)
                || type == typeof(RandomAccess) || type == typeof(FileSystemWatcher) || type == typeof(DriveInfo)
                || (type == typeof(FileStream) && isConstructor)
                || ((type == typeof(StreamReader) || type == typeof(StreamWriter)) && isConstructor && parameters.Length > 0 && parameters[0].ParameterType == typeof(string))
                || (type == typeof(Path) && name is "GetTempPath" or "GetTempFileName"))
                return UncontrolledDependencyKind.Disk;
        }

        if ((rules & AuditRules.ServerContext) != 0 && name == "ConfigureAwait"
            && (isAwaitable || type == typeof(System.Threading.Tasks.TaskAsyncEnumerableExtensions)))
        {
            Type? last = parameters.Length > 0 ? parameters[^1].ParameterType : null;
            if (last == typeof(bool) && preceding == 0)
                return UncontrolledDependencyKind.ContextEscape;
            if (last == typeof(ConfigureAwaitOptions) && preceding is int options && (options & (int)ConfigureAwaitOptions.ContinueOnCapturedContext) == 0)
                return UncontrolledDependencyKind.ContextEscape;
        }

        if ((rules & AuditRules.SdkPrimitives) != 0)
        {
            if (type == typeof(Task) && name == "Yield")
                return UncontrolledDependencyKind.ContextEscape;
            if (type == typeof(SemaphoreSlim) && isConstructor)
                return UncontrolledDependencyKind.ContextEscape;
            if ((type == typeof(TaskCompletionSource) || type == typeof(TaskCompletionSource<>)) && isConstructor
                && parameters.Any(p => p.ParameterType == typeof(TaskCreationOptions))
                && preceding is int creation && (creation & (int)TaskCreationOptions.RunContinuationsAsynchronously) != 0)
                return UncontrolledDependencyKind.ContextEscape;
        }

        return null;
    }
}

#pragma warning disable CA5394 // insecure randomness: deliberate negative control, never executed
#pragma warning disable CA2000 // dispose objects: never executed
#pragma warning disable CA1822, CA1031, CA1062, CA2008, CA2016

/// <summary>
/// A deliberately nondeterministic "simulated component" (HAR-002 negative control). Its methods are never executed; the
/// static audit must report every direct dependency.
/// </summary>
internal sealed class RogueSimulatedComponent
{
    public DateTime ReadsWallClock() => DateTime.UtcNow;

    public DateTimeOffset ReadsWallClockOffset() => DateTimeOffset.UtcNow;

    public long ReadsTickCount() => Environment.TickCount64;

    public long ReadsStopwatch() => Stopwatch.GetTimestamp();

    public int UsesSharedRandom() => Random.Shared.Next();

    public int UsesNewRandom() => new Random(1).Next();

    public Guid UsesNewGuid() => Guid.NewGuid();

    public byte[] UsesCryptoRandom() => RandomNumberGenerator.GetBytes(8);

    public Timer StartsTimer(TimerCallback callback) => new(callback, null, 10, Timeout.Infinite);

    public Task DelaysWithoutTimeProvider() => Task.Delay(10);

    public CancellationTokenSource TimesOutWithoutTimeProvider() => new(10);

    public Task RunsOnThreadPool() => Task.Run(() => { });

    public bool QueuesWorkItem() => ThreadPool.QueueUserWorkItem(_ => { });

    public void StartsThread() => new Thread(() => { }).Start();

    public void SleepsThread() => Thread.Sleep(1);

    public Socket OpensSocket() => new(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);

    public TcpClient OpensTcpClient() => new();

    public byte[] ReadsDisk(string path) => File.ReadAllBytes(path);

    public FileStream OpensFileStream(string path) => new(path, FileMode.Open);

    public async Task<int> EscapesContextAsync(Task<int> task) => await task.ConfigureAwait(false);

    public async Task YieldsAsync() => await Task.Yield();

    public SemaphoreSlim CreatesSemaphore() => new(1);

    public TaskCompletionSource<int> RunsContinuationsAsynchronously() => new(TaskCreationOptions.RunContinuationsAsynchronously);
}

/// <summary>A well-behaved simulated component using only injected dependencies (HAR-002 control).</summary>
internal sealed class CleanSimulatedComponent(TimeProvider time, IRandomSource random, IStorageMedium medium)
{
    public DateTimeOffset Now() => time.GetUtcNow();

    public long Timestamp() => time.GetTimestamp();

    public ulong Draw() => random.NextUInt64();

    public Task DelayAsync(CancellationToken ct) => Task.Delay(TimeSpan.FromMilliseconds(5), time, ct);

    public ITimer StartTimer(TimerCallback callback) => time.CreateTimer(callback, null, TimeSpan.FromMilliseconds(5), Timeout.InfiniteTimeSpan);

    public Task<int> WaitsWithTimeProvider(Task<int> task) => task.WaitAsync(TimeSpan.FromSeconds(1), time);

    public async Task<int> AwaitsOnCapturedContextAsync(Task<int> task) => await task;

    public async Task<int> WritesThroughMediumAsync(byte[] data, CancellationToken ct)
    {
        await using IStorageFile file = await medium.OpenAsync("component/state", StorageOpenMode.OpenOrCreate, ct);
        int written = await file.WriteAsync(0, data, ct);
        await file.FlushAsync(ct);
        return written;
    }
}

#pragma warning restore CA1822, CA1031, CA1062, CA2008, CA2016
#pragma warning restore CA2000
#pragma warning restore CA5394
