using System.Text;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Simulation.Tx;

/// <summary>Shared setup, history recording and assertions for the TX suite's simulation tests.</summary>
internal static class TxHelpers
{
    /// <summary>Volume created by the default harness configuration.</summary>
    public const string Vol = "vol";

    /// <summary>Bounded wait for liveness conditions (NodeTimings.Fast: election 150–300 ms, lease 2 s, session 10 s).</summary>
    public static readonly TimeSpan Liveness = TimeSpan.FromSeconds(15);

    /// <summary>The test's cancellation token.</summary>
    public static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Deterministic three-node, one-per-zone cluster (target 3 / ack 2 in 2 zones, three voters).</summary>
    public static TestClusterOptions Sim => TestClusterOptions.ThreeZones with { Mode = ClusterMode.Deterministic };

    /// <summary>UTF-8 bytes of <paramref name="text"/>.</summary>
    public static byte[] Utf8(string text) => Encoding.UTF8.GetBytes(text);

    /// <summary>Connects a client named <paramref name="name"/> (network identity <c>client:{name}</c>).</summary>
    public static Task<DfsClient> ConnectAsync(TestCluster cluster, string name, IReadOnlyList<string>? seedNodes = null, TimeSpan? operationTimeout = null) =>
        cluster.ConnectAsync(new TestClientOptions { Name = name, SeedNodes = seedNodes, OperationTimeout = operationTimeout }, Ct);

    /// <summary>Reads a UTF-8 text file; null only for genuine absence.</summary>
    public static async Task<string?> TryReadTextAsync(DfsFileClient files, string path)
    {
        try
        {
            return Encoding.UTF8.GetString(await files.ReadAllBytesAsync(path, Ct));
        }
        catch (FileNotFoundException)
        {
            return null;
        }
        catch (DirectoryNotFoundException)
        {
            return null;
        }
    }

    /// <summary>Reads a file's bytes; null only for genuine absence.</summary>
    public static async Task<byte[]?> TryReadAsync(DfsFileClient files, string path)
    {
        try
        {
            return await files.ReadAllBytesAsync(path, Ct);
        }
        catch (FileNotFoundException)
        {
            return null;
        }
        catch (DirectoryNotFoundException)
        {
            return null;
        }
    }

    /// <summary>Awaits <paramref name="actual"/> and asserts its bytes equal <paramref name="expected"/>.</summary>
    public static async Task AssertBytesAsync(byte[] expected, Task<byte[]> actual, string because)
    {
        byte[] bytes = await actual;
        AssertEx.BytesEqual(expected, bytes, because);
    }

    /// <summary>Asserts that no file exists at <paramref name="path"/>.</summary>
    public static async Task AssertFileAbsentAsync(DfsFileClient files, string path, string because)
    {
        Assert.False(await files.ExistsAsync(path, Ct), $"{path} must not exist: {because}");
    }

    /// <summary>Names (not paths) of the entries directly under <paramref name="directory"/>, ordinal order.</summary>
    public static async Task<string[]> ListNamesAsync(DfsDirectoryClient directories, string directory, DfsEnumerationOptions? options = null)
    {
        var names = new List<string>();
        await foreach (DfsEntryInfo entry in directories.EnumerateAsync(directory, options, Ct))
            names.Add(entry.Name);
        names.Sort(StringComparer.Ordinal);
        return names.ToArray();
    }

    /// <summary>Runs <paramref name="action"/> and returns its exception (null on success).</summary>
    public static async Task<Exception?> CaptureAsync(Func<Task> action)
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

    /// <summary>
    /// True for failures that only mean "authority/route temporarily unavailable, nothing applied": the caller may retry
    /// explicitly. Integrity failures, conflicts and unknown outcomes are never transient.
    /// </summary>
    public static bool IsTransient(Exception ex) =>
        ex is DfsUnavailableException or TimeoutException or DfsTransactionAbortedException
        || ex is DfsException { ErrorCode: DfsErrorCode.NotLeader or DfsErrorCode.QuorumUnavailable or DfsErrorCode.NodeUnavailable or DfsErrorCode.Unavailable or DfsErrorCode.Timeout };

    /// <summary>
    /// Retries <paramref name="action"/> on transient failures (explicit application-level retry, e.g. while a new
    /// leader is elected) until it succeeds or <see cref="Liveness"/> virtual time elapses.
    /// </summary>
    public static async Task<T> RetryTransientAsync<T>(TestCluster cluster, Func<Task<T>> action, string because)
    {
        DateTimeOffset deadline = cluster.Time.GetUtcNow() + Liveness;
        while (true)
        {
            try
            {
                return await action();
            }
            catch (Exception ex) when (IsTransient(ex) && cluster.Time.GetUtcNow() < deadline)
            {
                await cluster.DelayAsync(TimeSpan.FromMilliseconds(100), Ct);
            }
            catch (Exception ex) when (IsTransient(ex))
            {
                throw new TimeoutException($"Not achieved within {Liveness}: {because}", ex);
            }
        }
    }

    /// <summary>Resolves a transaction's outcome definitely (fencing it if unfinished), retrying through elections.</summary>
    public static Task<DfsOperationOutcome> ResolveOutcomeAsync(TestCluster cluster, DfsVolume volume, DfsTransactionId id) =>
        RetryTransientAsync(cluster, async () =>
        {
            DfsOperationOutcome outcome = await volume.Transactions.QueryOutcomeAsync(id, true, Ct);
            Assert.True(outcome.Status is DfsOutcomeStatus.Committed or DfsOutcomeStatus.Aborted,
                $"QueryOutcomeAsync(resolve: true) must return a definite outcome, got {outcome.Status}.");
            return outcome;
        }, $"resolving the outcome of {id}");

    /// <summary>Returns the current leader of the volume group and one follower.</summary>
    public static async Task<(TestNode Leader, TestNode Follower)> LeaderAndFollowerAsync(TestCluster cluster, string volume = Vol)
    {
        TestNode leader = await cluster.GetLeaderAsync(volume, Ct);
        TestNode follower = cluster.Nodes.First(n => n.Name != leader.Name && n.State == TestNodeState.Running);
        return (leader, follower);
    }

    /// <summary>Waits until a running node other than <paramref name="oldLeader"/> leads the volume group.</summary>
    public static Task WaitForNewLeaderAsync(TestCluster cluster, string oldLeader, string volume = Vol) =>
        cluster.WaitUntilAsync(async () =>
        {
            foreach (TestNode node in cluster.Nodes)
            {
                if (node.Name == oldLeader || node.State != TestNodeState.Running)
                    continue;
                GroupInspection? group = await node.Inspect.GetGroupAsync(volume);
                if (group is not null && group.Role == Tedd.Tdfs.Consensus.RaftRole.Leader)
                    return true;
            }

            return false;
        }, Liveness, $"a node other than {oldLeader} becomes leader of {volume}", Ct);

    /// <summary>Waits until the volume leader reports no active transactions.</summary>
    public static Task WaitNoActiveTransactionsAsync(TestCluster cluster, string because, string volume = Vol) =>
        cluster.WaitUntilAsync(async () =>
        {
            VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(volume);
            return state.ActiveTransactions == 0;
        }, Liveness, because, Ct);

    /// <summary>
    /// Commits <paramref name="tx"/>, completing <paramref name="log"/> with the client-observed outcome
    /// (Ok, Failed for definite aborts/conflicts, Unknown for outcome-unknown).
    /// </summary>
    public static async Task<TxResult> CommitAsync(DfsTransaction tx, TxLog log)
    {
        try
        {
            CommitToken token = await tx.CommitAsync(Ct);
            log.Complete(OpOutcome.Ok);
            return new TxResult(OpOutcome.Ok, null, token);
        }
        catch (Exception ex)
        {
            OpOutcome outcome = HistoryRecorder.Classify(ex);
            log.Complete(outcome);
            return new TxResult(outcome, ex, CommitToken.None);
        }
    }

    /// <summary>Runs a standalone (non-transactional) mutation recorded as a single-operation transaction.</summary>
    public static async Task<TxResult> StandaloneAsync(TxHistory history, string id, Action<TxLog> describe, Func<Task<CommitToken>> action)
    {
        TxLog log = history.Start(id);
        describe(log);
        try
        {
            CommitToken token = await action();
            log.Complete(OpOutcome.Ok);
            return new TxResult(OpOutcome.Ok, null, token);
        }
        catch (Exception ex)
        {
            OpOutcome outcome = HistoryRecorder.Classify(ex);
            log.Complete(outcome);
            return new TxResult(outcome, ex, CommitToken.None);
        }
    }

    /// <summary>
    /// Reads <paramref name="path"/> and returns its label: null when absent, the UTF-8 text when
    /// <paramref name="labels"/> is null, otherwise the label of the matching known version (fails on unknown bytes).
    /// </summary>
    public static async Task<string?> ReadLabelAsync(DfsFileClient files, string path, ContentLabels? labels = null)
    {
        byte[]? bytes = await TryReadAsync(files, path);
        if (bytes is null)
            return null;
        return labels is null ? Encoding.UTF8.GetString(bytes) : labels.Of(bytes, path);
    }

    /// <summary>"&lt;dir&gt;" for a directory, the file's label for a file, null when nothing exists at <paramref name="path"/>.</summary>
    public static async Task<string?> PresenceLabelAsync(DfsFileClient files, DfsDirectoryClient directories, string path)
    {
        if (await directories.ExistsAsync(path, Ct))
            return "<dir>";
        return await ReadLabelAsync(files, path);
    }

    /// <summary>
    /// Runs a recorded read-only transaction reading <paramref name="keys"/> and commits it. Returns what it saw and
    /// the commit result (a read-only commit may legally conflict; its reads are still checked).
    /// </summary>
    public static async Task<(Dictionary<string, string?> Seen, TxResult Result)> ObserveAsync(DfsVolume volume, TxHistory history, string id,
        IReadOnlyList<string> keys, ContentLabels? labels = null)
    {
        var seen = new Dictionary<string, string?>(StringComparer.Ordinal);
        TxRun run = await RunTxAsync(history, id, volume, async (tx, log) =>
        {
            foreach (string key in keys)
            {
                string? value = await ReadLabelAsync(tx.Files, key, labels);
                seen[key] = value;
                log.Read(key, value);
            }
        });
        return (seen, run.Result);
    }

    /// <summary>
    /// Begins a recorded transaction, runs <paramref name="body"/> and commits. Failures before the commit request
    /// complete the record as not committed and propagate; the commit outcome is recorded by <see cref="CommitAsync"/>.
    /// </summary>
    public static async Task<TxRun> RunTxAsync(TxHistory history, string id, DfsVolume volume, Func<DfsTransaction, TxLog, Task> body)
    {
        TxLog log = history.Start(id);
        DfsTransaction tx;
        try
        {
            tx = await volume.Transactions.BeginAsync(null, Ct);
        }
        catch
        {
            log.Complete(OpOutcome.Failed);
            throw;
        }

        await using (tx)
        {
            try
            {
                await body(tx, log);
            }
            catch (Exception ex)
            {
                log.Complete(HistoryRecorder.Classify(ex, requestMayHaveBeenSent: false));
                throw;
            }

            TxResult result = await CommitAsync(tx, log);
            return new TxRun(tx, log, result);
        }
    }

    /// <summary>
    /// True when <paramref name="ex"/> is an acceptable rejection of I/O through a bound handle whose transaction has
    /// completed: <see cref="InvalidOperationException"/> (including <see cref="ObjectDisposedException"/>) or a
    /// <see cref="DfsException"/> reporting an invalid handle or an ended transaction.
    /// </summary>
    public static bool IsCompletedTransactionRejection(Exception ex) =>
        ex is InvalidOperationException
        || ex is DfsException { ErrorCode: DfsErrorCode.InvalidHandle or DfsErrorCode.TransactionAborted };

    /// <summary>
    /// Runs a transactional write staged after one of the transaction's dependencies may already have been invalidated.
    /// Returns null when the write was staged, or the typed conflict when the implementation detected the invalid
    /// dependency (or aborted the transaction) early — FSD §8.3 only requires conflicts to abort with a typed exception,
    /// not that detection waits for CommitAsync. Every other failure propagates.
    /// </summary>
    public static async Task<DfsTransactionConflictException?> StageOrEarlyConflictAsync(Func<Task> stage)
    {
        try
        {
            await stage();
            return null;
        }
        catch (DfsTransactionConflictException ex)
        {
            return ex;
        }
    }

    /// <summary>
    /// Completes a transaction that must not commit: when staging already reported an early conflict, records it as
    /// failed and proves a later commit attempt is refused too; otherwise commits and asserts a validation conflict.
    /// </summary>
    public static async Task<TxResult> CommitExpectingConflictAsync(DfsTransaction tx, TxLog log, DfsTransactionConflictException? early, string because)
    {
        if (early is null)
        {
            TxResult result = await CommitAsync(tx, log);
            AssertConflict(result, tx, because);
            return result;
        }

        log.Complete(OpOutcome.Failed);
        Assert.True(early.IsRetryable, $"An early transaction conflict is retryable by the application ({because}).");
        Assert.Equal(tx.Id, early.TransactionId);
        Exception? late = await CaptureAsync(() => tx.CommitAsync(Ct));
        Assert.True(late is DfsTransactionConflictException or InvalidOperationException,
            $"After an early conflict the transaction can never commit ({because}); CommitAsync returned {late?.GetType().Name ?? "success"}.");
        Assert.NotEqual(DfsTransactionState.Committed, tx.State);
        return new TxResult(OpOutcome.Failed, early, CommitToken.None);
    }

    /// <summary>Asserts the transaction was rejected by validation (TransactionConflict), i.e. definitely not committed.</summary>
    public static void AssertConflict(TxResult result, DfsTransaction tx, string because)
    {
        Assert.True(result.Outcome == OpOutcome.Failed, $"Expected a transaction conflict ({because}); got {result.Outcome} {result.Error?.GetType().Name}: {result.Error?.Message}");
        DfsTransactionConflictException conflict = Assert.IsAssignableFrom<DfsTransactionConflictException>(result.Error);
        Assert.Equal(DfsErrorCode.TransactionConflict, conflict.ErrorCode);
        Assert.True(conflict.IsRetryable, "Transaction conflicts are retryable by the application.");
        Assert.Equal(tx.Id, conflict.TransactionId);
        Assert.Equal(DfsTransactionState.Aborted, tx.State);
    }
}

/// <summary>Outcome of a recorded commit.</summary>
internal sealed record TxResult(OpOutcome Outcome, Exception? Error, CommitToken Token);

/// <summary>A completed recorded transaction (the transaction object is disposed; its id/state remain readable).</summary>
internal sealed record TxRun(DfsTransaction Tx, TxLog Log, TxResult Result);

/// <summary>Maps known file versions to short labels so histories can be checked by value; unknown bytes fail the test.</summary>
internal sealed class ContentLabels
{
    private readonly List<(string Label, byte[] Bytes)> _known = [];

    /// <summary>Registers <paramref name="bytes"/> under <paramref name="label"/> and returns the bytes.</summary>
    public byte[] Add(string label, byte[] bytes)
    {
        lock (_known)
            _known.Add((label, bytes));
        return bytes;
    }

    /// <summary>The label of <paramref name="bytes"/>; fails the test when they match no known committed or staged version.</summary>
    public string Of(byte[] bytes, string where)
    {
        lock (_known)
        {
            foreach ((string label, byte[] known) in _known)
            {
                if (known.AsSpan().SequenceEqual(bytes))
                    return label;
            }
        }

        Assert.Fail($"{where}: read returned {bytes.Length} bytes that match no committed or staged version (invented, torn or mixed content).");
        return "?";
    }
}

/// <summary>
/// Records transactions as <see cref="TxRecord"/>s for <see cref="SerializabilityChecker.CheckStrict"/>. Invocation and
/// completion times come from the cluster's <see cref="HistoryRecorder"/> logical clock, so real-time order between
/// transactions (and with other recorded operations) is preserved.
/// </summary>
internal sealed class TxHistory(HistoryRecorder recorder)
{
    private readonly List<TxLog> _logs = [];
    private readonly Dictionary<string, int> _ids = new(StringComparer.Ordinal);

    /// <summary>
    /// Starts recording a transaction; call before <c>BeginAsync</c> so its invocation precedes the begin. A repeated
    /// <paramref name="id"/> (e.g. an explicit retry attempt) is made unique by appending <c>#n</c>.
    /// </summary>
    public TxLog Start(string id)
    {
        lock (_logs)
        {
            int seen = _ids.TryGetValue(id, out int n) ? n : 0;
            _ids[id] = seen + 1;
            string unique = seen == 0 ? id : $"{id}#{seen + 1}";
            long op = recorder.Invoke(unique, "tx", unique, null);
            var log = new TxLog(recorder, unique, op);
            _logs.Add(log);
            return log;
        }
    }

    /// <summary>All recorded transactions.</summary>
    public IReadOnlyList<TxRecord> Records()
    {
        lock (_logs)
            return _logs.Select(l => l.ToRecord()).ToArray();
    }

    /// <summary>Checks strict serializability against <paramref name="initial"/> and fails the test unless valid.</summary>
    public void AssertStrictlySerializable(IReadOnlyDictionary<string, string?> initial)
    {
        IReadOnlyList<TxRecord> records = Records();
        CheckResult result = SerializabilityChecker.CheckStrict(records, initial);
        TestContext.Current.TestOutputHelper?.WriteLine($"SerializabilityChecker: {result.Explanation} (states {result.StatesExplored})");
        result.AssertValid();
    }
}

/// <summary>Reads, writes and predicate reads of one recorded transaction (values are labels, null = absent).</summary>
internal sealed class TxLog(HistoryRecorder recorder, string id, long op)
{
    private readonly Dictionary<string, string?> _reads = new(StringComparer.Ordinal);
    private readonly Dictionary<string, string?> _writes = new(StringComparer.Ordinal);
    private readonly List<(string Directory, IReadOnlySet<string> Names)> _predicates = [];

    public string Id => id;

    /// <summary>The recorder's current outcome for this transaction.</summary>
    public OpOutcome Outcome => recorder.Get(op).Outcome;

    /// <summary>Records a read of <paramref name="key"/> unless the transaction already wrote it.</summary>
    public TxLog Read(string key, string? value)
    {
        if (!_writes.ContainsKey(key))
            _reads.TryAdd(key, value);
        return this;
    }

    /// <summary>Records an enumeration of the files directly under <paramref name="directory"/> (before own writes).</summary>
    public TxLog Enumerated(string directory, IEnumerable<string> names)
    {
        _predicates.Add((directory, new HashSet<string>(names, StringComparer.Ordinal)));
        return this;
    }

    /// <summary>Records a write (null = delete).</summary>
    public TxLog Write(string key, string? value)
    {
        _writes[key] = value;
        return this;
    }

    public void Complete(OpOutcome outcome) => recorder.Complete(op, outcome);

    public void Resolve(OpOutcome outcome) => recorder.Resolve(op, outcome);

    public TxRecord ToRecord()
    {
        HistoryOp h = recorder.Get(op);
        return new TxRecord(id, h.InvokeTime, h.CompleteTime, h.Outcome,
            new Dictionary<string, string?>(_reads, StringComparer.Ordinal),
            new Dictionary<string, string?>(_writes, StringComparer.Ordinal),
            _predicates.ToArray())
        {
            ResolveTime = h.ResolveTime,
        };
    }
}
