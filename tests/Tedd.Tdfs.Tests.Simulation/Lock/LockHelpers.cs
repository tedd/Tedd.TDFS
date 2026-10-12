using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Storage;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Simulation.Lock;

/// <summary>
/// Shared helpers of the LOCK simulation tests: deterministic cluster setup, writer/reader option shapes, handle and
/// fencing inspection on every replica, paused-client modelling and exception classification.
/// </summary>
internal static class LockHelpers
{
    public const string Vol = "vol";

    /// <summary>Deterministic in-memory cluster with simulated storage: 3 nodes (one per zone) or 5 nodes/5 voters.</summary>
    public static TestClusterOptions Sim(int nodes = 3) => nodes switch
    {
        3 => TestClusterOptions.ThreeZones with { Mode = ClusterMode.Deterministic },
        5 => TestClusterOptions.FiveNodes with { Mode = ClusterMode.Deterministic },
        _ => throw new ArgumentOutOfRangeException(nameof(nodes), nodes, "LOCK simulations use 3 or 5 nodes."),
    };

    public static TimeSpan Lease(TestCluster cluster) => cluster.Options.Timings.LeaseDuration;

    public static TimeSpan SessionTimeout(TestCluster cluster) => cluster.Options.Timings.SessionTimeout;

    /// <summary>Network identity of a test client ("client:{name}").</summary>
    public static string Net(string clientName) => new TestClientOptions { Name = clientName }.NetworkName;

    public static Task<DfsClient> ConnectAsync(TestCluster cluster, string name, CancellationToken ct, TestClock? clock = null,
        TimeSpan? operationTimeout = null, IReadOnlyList<string>? seeds = null) =>
        cluster.ConnectAsync(new TestClientOptions { Name = name, Clock = clock, OperationTimeout = operationTimeout, SeedNodes = seeds }, ct);

    /// <summary>Writer shape used by most scenarios: writable, sharing Read (so compatible readers can verify contents).</summary>
    public static DfsOpenOptions Writer(FileAccess access = FileAccess.Write, FileShare share = FileShare.Read,
        DfsWriteMode writeMode = DfsWriteMode.OnFlush, FileMode mode = FileMode.Open) =>
        new() { Mode = mode, Access = access, Share = share, WriteMode = writeMode };

    /// <summary>Maximally sharing live reader: coexists with any valid writer that shares Read.</summary>
    public static DfsOpenOptions SharingReader() =>
        new() { Mode = FileMode.Open, Access = FileAccess.Read, Share = FileShare.ReadWrite | FileShare.Delete };

    public static async Task<byte[]> ReadToEndAsync(Stream stream, CancellationToken ct)
    {
        using var buffer = new MemoryStream();
        await stream.CopyToAsync(buffer, ct);
        return buffer.ToArray();
    }

    public static async Task<byte[]> ReadCommittedAsync(DfsVolume volume, string path, CancellationToken ct)
    {
        await using DfsStream stream = await volume.Files.OpenAsync(path, SharingReader(), ct);
        return await ReadToEndAsync(stream, ct);
    }

    /// <summary>Stages <paramref name="data"/> as the complete new contents of a seekable writable stream.</summary>
    public static async Task StageReplaceAsync(DfsStream stream, byte[] data, CancellationToken ct)
    {
        stream.Seek(0, SeekOrigin.Begin);
        await stream.WriteAsync(data, ct);
        await stream.SetLengthAsync(data.Length, ct);
    }

    public static async Task<CommitToken> ReplaceAndCommitAsync(DfsStream stream, byte[] data, CancellationToken ct)
    {
        await StageReplaceAsync(stream, data, ct);
        return await stream.CommitAsync(ct);
    }

    /// <summary>Array overload of <see cref="AssertEx.BytesEqual"/> (spans cannot be held across an awaited argument).</summary>
    public static void BytesEqual(byte[] expected, byte[] actual, string because) => AssertEx.BytesEqual(expected, actual, because);

    public static IReadOnlyList<TestNode> Running(TestCluster cluster) =>
        cluster.Nodes.Where(n => n.State == TestNodeState.Running).ToArray();

    public static async Task<HandleInspection[]> ActiveHandlesAsync(TestCluster cluster, InodeId inode, string? node = null, string volume = Vol)
    {
        VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(volume, node);
        return state.Handles.Where(h => h.Inode == inode && !h.Expired).ToArray();
    }

    public static async Task<HandleInspection[]> ActiveWritersAsync(TestCluster cluster, InodeId inode, string? node = null, string volume = Vol) =>
        (await ActiveHandlesAsync(cluster, inode, node, volume)).Where(h => h.IsWriter).ToArray();

    public static async Task<FileInspection> FileAsync(TestCluster cluster, string path, string? node = null, string volume = Vol)
    {
        VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(volume, node);
        FileInspection? file = state.GetFile(path);
        Assert.True(file is not null, $"{path} must exist in the applied state of {node ?? "the leader"}");
        return file!;
    }

    /// <summary>
    /// Safety oracle for INV-05: at every replica's applied index at most one non-expired writable handle exists on the
    /// inode (an expiry is always applied before a successor grant, so this holds on lagging replicas too).
    /// </summary>
    public static async Task AssertAtMostOneActiveWriterOnEveryReplicaAsync(TestCluster cluster, InodeId inode, string because, string volume = Vol)
    {
        foreach (TestNode node in Running(cluster))
        {
            VolumeStateInspection? state = await node.Inspect.GetVolumeStateAsync(volume);
            if (state is null)
                continue; // the volume is not hosted on this node
            HandleInspection[] writers = state.Handles.Where(h => h.Inode == inode && h.IsWriter && !h.Expired).ToArray();
            Assert.True(writers.Length <= 1,
                $"{because}: replica {node.Name} at applied index {state.AppliedIndex} has {writers.Length} active writers on inode {inode.Value} " +
                $"(generations {string.Join(", ", writers.Select(w => w.Generation))})");
        }
    }

    /// <summary>Every running replica that hosts the volume and has the file records <paramref name="generation"/> as its writer generation.</summary>
    public static async Task AssertWriterGenerationOnEveryReplicaAsync(TestCluster cluster, string path, ulong generation, string because, string volume = Vol)
    {
        foreach (TestNode node in Running(cluster))
        {
            VolumeStateInspection? state = await node.Inspect.GetVolumeStateAsync(volume);
            FileInspection? file = state?.GetFile(path);
            if (file is null)
                continue;
            Assert.True(file.WriterGeneration == generation,
                $"{because}: replica {node.Name} records writer generation {file.WriterGeneration} for {path}, expected {generation}");
        }
    }

    /// <summary>
    /// Waits until no non-expired writable handle with <paramref name="generation"/> exists on the leader (or on
    /// <paramref name="node"/> when given, e.g. the majority-side leader during a partition).
    /// </summary>
    public static Task WaitForWriterGoneAsync(TestCluster cluster, InodeId inode, ulong generation, TimeSpan timeout, string because,
        CancellationToken ct, string? node = null) =>
        cluster.WaitUntilAsync(async () => !(await ActiveWritersAsync(cluster, inode, node)).Any(h => h.Generation == generation), timeout, because, ct);

    /// <summary>Highest applied index of <paramref name="volume"/> over every running replica.</summary>
    public static async Task<ulong> MaxAppliedIndexAsync(TestCluster cluster, string volume = Vol)
    {
        ulong max = 0;
        foreach (TestNode node in Running(cluster))
        {
            VolumeStateInspection? state = await node.Inspect.GetVolumeStateAsync(volume);
            if (state is not null && state.AppliedIndex > max)
                max = state.AppliedIndex;
        }

        return max;
    }

    /// <summary>Waits until a running node other than <paramref name="oldLeader"/> leads the volume group.</summary>
    public static async Task<TestNode> WaitForNewLeaderAsync(TestCluster cluster, string oldLeader, TimeSpan timeout, CancellationToken ct, string volume = Vol)
    {
        TestNode? found = null;
        await cluster.WaitUntilAsync(async () =>
        {
            foreach (TestNode node in Running(cluster))
            {
                if (node.Name == oldLeader)
                    continue;
                GroupInspection? group = await node.Inspect.GetGroupAsync(volume);
                if (group is not null && group.Role == RaftRole.Leader)
                {
                    found = node;
                    return true;
                }
            }

            return false;
        }, timeout, $"a leader of '{volume}' other than {oldLeader} must be elected", ct);
        return found!;
    }

    /// <summary>Objects stored on any running node that were uploaded under one of <paramref name="staging"/>.</summary>
    public static async Task<HashSet<ObjectId>> StoredObjectsOfStagingAsync(TestCluster cluster, IReadOnlySet<StagingId> staging, string volume = Vol)
    {
        var result = new HashSet<ObjectId>();
        foreach (TestNode node in Running(cluster))
        {
            foreach (ObjectRecordInfo record in await node.Inspect.ListObjectsAsync(volume))
            {
                if (staging.Contains(record.Staging))
                    result.Add(record.Id);
            }
        }

        return result;
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

    public static async Task<(DfsStream? Stream, Exception? Error)> TryOpenAsync(DfsVolume volume, string path, DfsOpenOptions options, CancellationToken ct)
    {
        try
        {
            return (await volume.Files.OpenAsync(path, options, ct), null);
        }
        catch (Exception ex)
        {
            return (null, ex);
        }
    }

    /// <summary>Runs <paramref name="action"/> and fails the test if it is still pending after <paramref name="bound"/> of harness time.</summary>
    public static async Task<Exception?> CatchWithinAsync(TestCluster cluster, Func<Task> action, TimeSpan bound, string because, CancellationToken ct)
    {
        Task<Exception?> attempt = CatchAsync(action);
        Task deadline = cluster.DelayAsync(bound, ct);
        if (await Task.WhenAny(attempt, deadline) != attempt)
            Assert.Fail($"{because}: still pending after {bound}");
        return await attempt;
    }

    /// <summary>Awaits <paramref name="task"/> but fails the test if it is still pending after <paramref name="bound"/> of harness time.</summary>
    public static async Task<T> WithinAsync<T>(TestCluster cluster, Task<T> task, TimeSpan bound, string because, CancellationToken ct)
    {
        if (await Task.WhenAny(task, cluster.DelayAsync(bound, ct)) != task)
            Assert.Fail($"{because}: still pending after {bound}");
        return await task;
    }

    public static string Describe(Exception? ex) => ex is null
        ? "no exception (the operation succeeded)"
        : $"{ex.GetType().FullName}{(ex is DfsException d ? $" [{d.ErrorCode}]" : string.Empty)}: {ex.Message}";

    public static void AssertSharingConflict(Exception? ex, string because)
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

    /// <summary>Lost writer authority surfaces as <see cref="DfsLeaseLostException"/> with <see cref="DfsErrorCode.LeaseLost"/> (FSD §13).</summary>
    public static void AssertLeaseLost(Exception? ex, string because)
    {
        if (ex is not DfsLeaseLostException lost)
        {
            Assert.Fail($"{because}: expected DfsLeaseLostException but got {Describe(ex)}");
            return;
        }

        Assert.Equal(DfsErrorCode.LeaseLost, lost.ErrorCode);
    }

    /// <summary>True for failures that only report reduced availability or an unresolved outcome (never a definite success).</summary>
    public static bool IsAvailabilityOrUnknown(Exception? ex) =>
        ex is DfsUnavailableException or DfsCommitOutcomeUnknownException or TimeoutException
        || ex is DfsException { ErrorCode: DfsErrorCode.Timeout or DfsErrorCode.NotLeader or DfsErrorCode.Unavailable or DfsErrorCode.NodeUnavailable or DfsErrorCode.QuorumUnavailable };

    /// <summary>
    /// A request that needed authority the caller's side of a partition does not have must fail — with an
    /// availability error, an unknown outcome, or lost authority — never succeed.
    /// </summary>
    public static void AssertNotAuthorized(Exception? ex, string because)
    {
        if (ex is null)
            Assert.Fail($"{because}: succeeded without valid metadata authority");
        Assert.True(IsAvailabilityOrUnknown(ex) || ex is DfsLeaseLostException,
            $"{because}: expected an availability/unknown-outcome/lease-lost failure, got {Describe(ex)}");
    }

    /// <summary>Operation id carried by a failure, when the SDK reported one.</summary>
    public static DfsOperationId? OperationIdOf(Exception? ex) => ex switch
    {
        DfsCommitOutcomeUnknownException unknown => unknown.OperationId,
        DfsException dfs => dfs.OperationId,
        _ => null,
    };
}

/// <summary>
/// Models a paused client process (test plan §9.3): every message it sends is captured by a network hold and its clock
/// is frozen so its renewal/timeout timers stop; <see cref="Resume"/> delivers the queued messages (optionally in
/// reverse order) and lets its clock run again. Memory (staged data, handles) is retained throughout.
/// </summary>
internal sealed class PausedClient
{
    private readonly TestClock _clock;

    private PausedClient(TestClock clock, MessageHold hold)
    {
        _clock = clock;
        Hold = hold;
    }

    public MessageHold Hold { get; }

    public static PausedClient Pause(TestCluster cluster, string clientName, TestClock clock)
    {
        MessageHold hold = cluster.Network.Hold(new MessageFilter(From: LockHelpers.Net(clientName)));
        clock.Freeze();
        return new PausedClient(clock, hold);
    }

    public void Resume(bool reverseOrder = false)
    {
        Hold.ReleaseAll(reverseOrder);
        _clock.Unfreeze();
    }
}

/// <summary>A publication that is known (or was resolved) to have committed.</summary>
internal sealed record ProbePublication(string Writer, int Iteration, byte[] Marker, CommitToken Token);

/// <summary>A publication whose outcome was unknown when it returned.</summary>
internal sealed record ProbeUnknown(string Writer, int Iteration, DfsOperationId Operation, byte[] Marker, bool AfterReplacementGrant);

/// <summary>
/// Sequential single-writer safety probe (LOCK-014/LOCK-015): writer A keeps publishing through its ORIGINAL handle
/// while writer B repeatedly tries to take the inode. Once B has been granted, A must never publish again; at every
/// step every replica holds at most one active writer; unknown outcomes are resolved authoritatively before judging.
/// </summary>
internal sealed class SingleWriterProbe
{
    private readonly TestCluster _cluster;
    private readonly DfsStream _a;
    private readonly DfsVolume _bVolume;
    private readonly string _path;
    private readonly InodeId _inode;
    private readonly CancellationToken _ct;
    private readonly TimeSpan _stepBound;
    private readonly List<(int Iteration, byte[] Marker, Task<CommitToken> Attempt, bool AfterReplacementGrant)> _pendingA = [];
    private DfsStream? _b;

    public SingleWriterProbe(TestCluster cluster, DfsStream a, DfsVolume bVolume, string path, InodeId inode, CancellationToken ct)
    {
        _cluster = cluster;
        _a = a;
        _bVolume = bVolume;
        _path = path;
        _inode = inode;
        _ct = ct;
        _stepBound = cluster.Options.Timings.LeaseDuration * 5;
        AGeneration = a.WriterGeneration;
    }

    public ulong AGeneration { get; }

    public int? BGrantIteration { get; private set; }

    public ulong BGeneration { get; private set; }

    /// <summary>
    /// Highest applied index of any running replica observed right after B's grant returned: an upper bound of the
    /// grant's log index (the granting leader applied the grant before replying). The maximum over all replicas is used
    /// because "the leader" may be ambiguous while server clocks are frozen or jumped.
    /// </summary>
    public ulong BGrantAppliedUpperBound { get; private set; }

    /// <summary>
    /// Unknown publications whose authoritative resolution answered <see cref="DfsOutcomeStatus.Expired"/> (only allowed
    /// for writer A, whose session may expire while its renewals are withheld or server clocks run fast).
    /// </summary>
    public List<ProbeUnknown> Indeterminate { get; } = [];

    public List<ProbePublication> Committed { get; } = [];

    public List<ProbeUnknown> Unknown { get; } = [];

    public int ASuccesses { get; private set; }

    public int AFailuresAfterGrant { get; private set; }

    public static byte[] Marker(string writer, int iteration)
    {
        byte[] marker = TestData.Bytes((ulong)(writer == "A" ? 1_000_000 : 2_000_000) + (ulong)iteration, 48);
        marker[0] = (byte)writer[0];
        marker[1] = (byte)iteration;
        return marker;
    }

    public async Task StepAsync(int iteration)
    {
        bool grantedBefore = BGrantIteration is not null;
        byte[] markerA = Marker("A", iteration);
        Task<CommitToken> attempt = LockHelpers.ReplaceAndCommitAsync(_a, markerA, _ct);
        if (await Task.WhenAny(attempt, _cluster.DelayAsync(_stepBound, _ct)) != attempt)
        {
            // A's own clock may be frozen/slowed so its SDK cannot time out: keep the call pending and judge it later.
            _pendingA.Add((iteration, markerA, attempt, grantedBefore));
            if (grantedBefore)
                AFailuresAfterGrant++;
        }
        else
        {
            Exception? aError = null;
            try
            {
                CommitToken token = await attempt;
                Committed.Add(new ProbePublication("A", iteration, markerA, token));
            }
            catch (Exception ex)
            {
                aError = ex;
            }

            if (aError is null)
            {
                ASuccesses++;
                Assert.False(grantedBefore,
                    $"iteration {iteration}: A published with fencing generation {AGeneration} after B was granted generation {BGeneration} at iteration {BGrantIteration}");
            }
            else
            {
                if (aError is DfsCommitOutcomeUnknownException unknown)
                    Unknown.Add(new ProbeUnknown("A", iteration, unknown.OperationId, markerA, grantedBefore));
                if (grantedBefore)
                    AFailuresAfterGrant++;
            }
        }

        if (_b is null)
        {
            (DfsStream? opened, Exception? error) = await LockHelpers.TryOpenAsync(_bVolume, _path, LockHelpers.Writer(), _ct);
            if (opened is not null)
            {
                _b = opened;
                if (BGrantIteration is null)
                {
                    BGrantIteration = iteration;
                    BGeneration = opened.WriterGeneration;
                    Assert.True(BGeneration > AGeneration, $"B's generation {BGeneration} must exceed A's {AGeneration}");
                    BGrantAppliedUpperBound = await LockHelpers.MaxAppliedIndexAsync(_cluster);
                }
            }
            else if (error is DfsSharingViolationException)
            {
                LockHelpers.AssertSharingConflict(error, $"iteration {iteration}: B's open while A may still hold the inode");
            }
            else
            {
                // A refused open is safe whatever the reason; it must just never be a fabricated grant or an untyped
                // failure. OperationExpired: B's session may expire while server clocks jump/run fast.
                Assert.True(LockHelpers.IsAvailabilityOrUnknown(error) || error is DfsOperationExpiredException,
                    $"iteration {iteration}: B's open failed with neither a sharing conflict nor reduced availability: {LockHelpers.Describe(error)}");
            }
        }
        else
        {
            byte[] markerB = Marker("B", iteration);
            DfsStream b = _b;
            Exception? bError = await LockHelpers.CatchAsync(async () =>
            {
                CommitToken token = await LockHelpers.ReplaceAndCommitAsync(b, markerB, _ct);
                Committed.Add(new ProbePublication("B", iteration, markerB, token));
            });
            if (bError is DfsCommitOutcomeUnknownException unknown)
                Unknown.Add(new ProbeUnknown("B", iteration, unknown.OperationId, markerB, true));
            if (bError is not null && b.IsFaulted)
            {
                await LockHelpers.CatchAsync(() => b.DisposeAsync().AsTask());
                _b = null;
            }
        }

        await LockHelpers.AssertAtMostOneActiveWriterOnEveryReplicaAsync(_cluster, _inode, $"single-writer probe iteration {iteration}");
    }

    /// <summary>
    /// Resolves every unknown outcome through <paramref name="resolver"/> (fencing it if still pending), checks that no
    /// A publication committed after B's grant, and returns the contents the file must now hold (null: none committed).
    /// </summary>
    public async Task<byte[]?> ResolveAsync(DfsVolume resolver)
    {
        // Calls of A that were still pending (frozen/slow client clock) are judged now that its clock runs again.
        foreach ((int iteration, byte[] marker, Task<CommitToken> attempt, bool afterGrant) in _pendingA)
        {
            if (await Task.WhenAny(attempt, _cluster.DelayAsync(_stepBound * 4, _ct)) != attempt)
                Assert.Fail($"A's publication from iteration {iteration} never completed after its clock was restored");
            try
            {
                CommitToken token = await attempt;
                Assert.False(afterGrant, $"A's pending publication from iteration {iteration} committed after B had been granted");
                Committed.Add(new ProbePublication("A", iteration, marker, token));
            }
            catch (DfsCommitOutcomeUnknownException unknown)
            {
                Unknown.Add(new ProbeUnknown("A", iteration, unknown.OperationId, marker, afterGrant));
            }
            catch (Exception)
            {
                // A definite failure: nothing was published.
            }
        }

        foreach (ProbeUnknown unknown in Unknown)
        {
            DfsOperationOutcome outcome = await resolver.QueryOperationAsync(unknown.Operation, resolve: true, _ct);
            if (outcome.Status == DfsOutcomeStatus.Expired && unknown.Writer == "A")
            {
                // A's session expired (its renewals were withheld / server clocks ran fast): the id is honestly
                // reported as expired. Its effect, if any, is judged through the final contents.
                Indeterminate.Add(unknown);
                continue;
            }

            Assert.True(outcome.Status is DfsOutcomeStatus.Committed or DfsOutcomeStatus.Aborted,
                $"resolve:true must produce a definite outcome for {unknown.Operation} ({unknown.Writer}), got {outcome.Status}");
            if (outcome.Status == DfsOutcomeStatus.Committed)
            {
                Assert.False(unknown.Writer == "A" && unknown.AfterReplacementGrant,
                    $"A's publication {unknown.Operation} (iteration {unknown.Iteration}) committed although B had already been granted");
                Committed.Add(new ProbePublication(unknown.Writer, unknown.Iteration, unknown.Marker, outcome.Token));
            }
        }

        if (BGrantIteration is not null)
        {
            foreach (ProbePublication publication in Committed.Where(p => p.Writer == "A"))
            {
                Assert.True(publication.Token.Sequence <= BGrantAppliedUpperBound,
                    $"A's publication at log index {publication.Token.Sequence} (iteration {publication.Iteration}) was applied after B's grant (≤ {BGrantAppliedUpperBound})");
            }
        }

        return Committed.Count == 0 ? null : Committed.MaxBy(p => p.Token.Sequence)!.Marker;
    }

    /// <summary>
    /// Final-contents oracle after <see cref="ResolveAsync"/>: the file holds the last committed publication in log
    /// order (or <paramref name="initial"/>), or — only when an outcome stayed indeterminate — the marker of an
    /// indeterminate publication that was legal when issued. Contents equal to a publication A issued after B's grant
    /// are a fencing violation.
    /// </summary>
    public void AssertFinalContents(byte[]? expected, byte[] initial, byte[] actual, string because)
    {
        byte[] last = expected ?? initial;
        if (actual.AsSpan().SequenceEqual(last))
            return;

        foreach (ProbeUnknown unknown in Indeterminate)
        {
            if (actual.AsSpan().SequenceEqual(unknown.Marker))
            {
                Assert.False(unknown.AfterReplacementGrant,
                    $"{because}: the file holds A's publication from iteration {unknown.Iteration}, issued after B had been granted");
                return;
            }
        }

        LockHelpers.BytesEqual(last, actual, because);
    }

    public async Task DisposeStreamsAsync()
    {
        await LockHelpers.CatchAsync(() => _a.DisposeAsync().AsTask());
        if (_b is not null)
            await LockHelpers.CatchAsync(() => _b.DisposeAsync().AsTask());
    }
}
