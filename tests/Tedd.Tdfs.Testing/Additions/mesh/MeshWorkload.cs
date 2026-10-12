using System.Buffers.Binary;
using System.Globalization;
using System.Text;

namespace Tedd.Tdfs.Testing.Mesh;

/// <summary>Shape of the concurrent multi-client workload run by <see cref="MeshWorkload"/>.</summary>
public sealed record MeshWorkloadOptions
{
    /// <summary>Number of concurrent clients (processes <c>w0..</c>, network names <c>client:w0..</c>).</summary>
    public int Clients { get; init; } = 3;

    /// <summary>Whole-file registers (writes, reads, deletes) checked with <see cref="RegisterModel"/>.</summary>
    public int RegisterKeys { get; init; } = 3;

    /// <summary>Append-only logs checked with <see cref="AppendLogModel"/>.</summary>
    public int Logs { get; init; } = 1;

    /// <summary>Rename tokens (one file moved between two names) checked with <see cref="MeshRenameModel"/>.</summary>
    public int RenameDirectories { get; init; } = 1;

    /// <summary>Transaction keys checked with <see cref="SerializabilityChecker"/> (0 disables transactions).</summary>
    public int TransactionKeys { get; init; } = 3;

    /// <summary>Minimum operations per client before a stop request is honoured.</summary>
    public int MinOperationsPerClient { get; init; } = 6;

    /// <summary>Hard bound on operations per client (keeps histories checkable).</summary>
    public int MaxOperationsPerClient { get; init; } = 400;

    /// <summary>Pause between operations of one client (harness clock).</summary>
    public TimeSpan ThinkTime { get; init; } = TimeSpan.FromMilliseconds(40);

    /// <summary>SDK operation timeout of workload clients.</summary>
    public TimeSpan OperationTimeout { get; init; } = TimeSpan.FromSeconds(3);

    /// <summary>Small payload length (inline-sized).</summary>
    public int SmallPayload { get; init; } = 700;

    /// <summary>Large payload length (above the inline threshold, extent-backed).</summary>
    public int LargePayload { get; init; } = 9_000;

    /// <summary>Seed node names of client <c>i</c>; null means every live node, rotated so each client enters through a different node.</summary>
    public Func<int, IReadOnlyList<string>>? SeedsForClient { get; init; }

    /// <summary>Workload root directory.</summary>
    public string Root { get; init; } = "/mesh/w";
}

/// <summary>
/// Concurrent multi-client workload (writes, reads, deletes, appends, renames, unique acknowledged writes and
/// transactions) recorded in the cluster's <see cref="HistoryRecorder"/> and a transaction list, then checked after
/// recovery: per-key linearizability (<see cref="RegisterModel"/>, <see cref="AppendLogModel"/>,
/// <see cref="MeshRenameModel"/>), strict serializability of transactions, no acknowledged write lost, no effect of a
/// definite failure, unknown outcomes resolved by operation/transaction id, and only typed failures.
/// </summary>
public sealed class MeshWorkload
{
    private readonly TestCluster _cluster;
    private readonly string _volume;
    private readonly MeshWorkloadOptions _options;
    private readonly CancellationToken _ct;
    private readonly object _lock = new();
    private readonly List<DfsClient> _clients = [];
    private readonly List<long> _pendingHistory = [];
    private readonly List<LedgerEntry> _ledger = [];
    private readonly List<TxBuilder> _transactions = [];
    private readonly List<string> _violations = [];
    private readonly Dictionary<string, string?> _txInitial = new(StringComparer.Ordinal);
    private readonly List<Task> _loops = [];
    private readonly HashSet<string> _finished = new(StringComparer.Ordinal);
    private DfsClient? _setup;
    private long _txClock;
    private volatile bool _stop;

    private MeshWorkload(TestCluster cluster, string volume, MeshWorkloadOptions options, CancellationToken ct)
    {
        _cluster = cluster;
        _volume = volume;
        _options = options;
        _ct = ct;
    }

    /// <summary>Process names of the workload clients.</summary>
    public IReadOnlyList<string> Processes => Enumerable.Range(0, _options.Clients).Select(i => "w" + i.ToString(CultureInfo.InvariantCulture)).ToArray();

    /// <summary>Network names of every client this workload uses (workers and the setup/verifier client).</summary>
    public IReadOnlyList<string> NetworkNames => Processes.Append("wsetup").Select(p => "client:" + p).ToArray();

    /// <summary>
    /// Unexpected failures observed so far: anything but typed availability (<see cref="DfsUnavailableException"/>),
    /// unknown outcome with an id (mutations only), sharing/transaction conflicts, lease loss and expired session
    /// ids/pins after an outage longer than the session timeout. Bare timeouts are
    /// violations: a deadline must surface as a typed availability error or, after possible transmission, as an unknown
    /// outcome carrying the operation id (FSD §13).
    /// </summary>
    public IReadOnlyList<string> Violations
    {
        get
        {
            lock (_lock)
                return _violations.ToArray();
        }
    }

    /// <summary>Creates the directory layout and initial files, connects the clients and starts their loops.</summary>
    public static async Task<MeshWorkload> StartAsync(TestCluster cluster, string volume, MeshWorkloadOptions options, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(cluster);
        ArgumentNullException.ThrowIfNull(options);
        var workload = new MeshWorkload(cluster, volume, options, ct);
        await workload.SetupAsync();
        for (int i = 0; i < options.Clients; i++)
            workload._loops.Add(workload.RunClientAsync(i));
        return workload;
    }

    /// <summary>Largest invocation time recorded so far (use as a mark for <see cref="OkOperationsSince"/>).</summary>
    public long Mark() => _cluster.History.Operations.Select(o => Math.Max(o.InvokeTime, o.CompleteTime ?? 0)).DefaultIfEmpty(0).Max();

    /// <summary>Workload operations of <paramref name="process"/> that completed Ok and were invoked after <paramref name="mark"/>.</summary>
    public int OkOperationsSince(string process, long mark) =>
        _cluster.History.Operations.Count(o => o.Process == process && o.Outcome == OpOutcome.Ok && o.InvokeTime > mark);

    /// <summary>True while the loop of <paramref name="process"/> is still issuing operations (it ends at stop or at the operation bound).</summary>
    public bool IsRunning(string process)
    {
        lock (_lock)
            return !_finished.Contains(process);
    }

    /// <summary>Requests the loops to stop (after their minimum operation count) and waits for them.</summary>
    public async Task StopAsync()
    {
        _stop = true;
        await Task.WhenAll(_loops);
    }

    /// <summary>
    /// After recovery: resolves unknown outcomes, records final reads, and asserts linearizability per key, strict
    /// serializability, acknowledged-write durability, no effect of definite failures and the absence of violations.
    /// </summary>
    public async Task VerifyAsync()
    {
        if (!_stop)
            await StopAsync();
        DfsVolume v = _setup!.GetVolume(_volume);
        HistoryRecorder h = _cluster.History;

        // 1. Narrow the remaining unknown outcomes by operation/transaction id (resolve: true fences ids that never
        //    committed). Most were already narrowed by their client right after availability returned.
        await ResolvePendingAsync(v, process: null, final: true);

        // 2. Final quiescent reads (part of the checked histories).
        for (int k = 0; k < _options.RegisterKeys; k++)
            await RecordReadAsync(v, "final", RegisterPath(k), "reg:");
        for (int k = 0; k < _options.Logs; k++)
            await RecordReadAsync(v, "final", LogPath(k), "log:");
        for (int k = 0; k < _options.RenameDirectories; k++)
            await RecordProbeAsync(v, "final", k);

        // 3. Checks.
        Assert.True(Violations.Count == 0, "workload observed untyped or impossible failures:" + Environment.NewLine + string.Join(Environment.NewLine, Violations));
        HistoryOp[] all = h.Operations.ToArray();
        LinearizabilityChecker.Check(all.Where(o => o.Key.StartsWith("reg:", StringComparison.Ordinal)).ToArray(), new RegisterModel()).AssertValid();
        LinearizabilityChecker.Check(all.Where(o => o.Key.StartsWith("log:", StringComparison.Ordinal)).ToArray(), new AppendLogModel()).AssertValid();
        LinearizabilityChecker.Check(all.Where(o => o.Key.StartsWith("ren:", StringComparison.Ordinal)).ToArray(), new MeshRenameModel()).AssertValid();

        if (_options.TransactionKeys > 0)
        {
            var finalReads = new Dictionary<string, string?>(StringComparer.Ordinal);
            long inv = Interlocked.Increment(ref _txClock);
            for (int k = 0; k < _options.TransactionKeys; k++)
                finalReads[TxPath(k)] = await ReadTextOrNullAsync(v.Files, TxPath(k));
            IReadOnlySet<string> names = await ListFileNamesAsync(v.Directories, TxDirectory);
            long done = Interlocked.Increment(ref _txClock);
            var records = _transactions.Select(t => t.Build()).ToList();
            records.Add(new TxRecord("final", inv, done, OpOutcome.Ok, finalReads, new Dictionary<string, string?>(), [(TxDirectory, names)]));
            SerializabilityChecker.CheckStrict(records, _txInitial).AssertValid();
        }

        // 4. Acknowledged unique writes survive with exact bytes; definite failures left no effect.
        foreach (LedgerEntry entry in _ledger)
        {
            byte[]? actual = await ReadOrNullAsync(v.Files, entry.Path);
            switch (entry.Status)
            {
                case OpOutcome.Ok:
                    Assert.True(actual is not null, $"acknowledged write {entry.Path} by {entry.Process} was lost (INV-01)");
                    AssertEx.BytesEqual(entry.Data, actual, $"acknowledged write {entry.Path}");
                    break;
                case OpOutcome.Failed:
                    Assert.True(actual is null, $"write {entry.Path} reported a definite failure ({entry.Error}) but took effect (INV-06/INV-08)");
                    break;
                default:
                    if (actual is not null)
                        AssertEx.BytesEqual(entry.Data, actual, $"unresolved write {entry.Path} took effect with different bytes");
                    break;
            }
        }
    }

    // ------------------------------------------------------------------------------------------------------------

    private string RegisterPath(int k) => $"{_options.Root}/r/k{k}";

    private string LogPath(int k) => $"{_options.Root}/l/log{k}";

    private string RenameDirectory(int k) => $"{_options.Root}/n/m{k}";

    private string TxDirectory => _options.Root + "/t";

    private string TxPath(int k) => $"{TxDirectory}/t{k}";

    private async Task SetupAsync()
    {
        _setup = await ConnectAsync("wsetup", null);
        DfsVolume v = _setup.GetVolume(_volume);
        foreach (string dir in new[] { _options.Root + "/r", _options.Root + "/l", _options.Root + "/acked", TxDirectory })
            await v.Directories.CreateDirectoryAsync(dir, _ct);
        for (int k = 0; k < _options.Logs; k++)
            await v.Files.WriteAllBytesAsync(LogPath(k), ReadOnlyMemory<byte>.Empty, _ct);
        for (int k = 0; k < _options.RenameDirectories; k++)
        {
            await v.Directories.CreateDirectoryAsync(RenameDirectory(k), _ct);
            await v.Files.WriteAllBytesAsync(RenameDirectory(k) + "/a", Encoding.UTF8.GetBytes("token" + k.ToString(CultureInfo.InvariantCulture)), _ct);
        }

        for (int k = 0; k < _options.TransactionKeys; k++)
        {
            await v.Files.WriteAllTextAsync(TxPath(k), "init", null, _ct);
            _txInitial[TxPath(k)] = "init";
        }

        for (int i = 0; i < _options.Clients; i++)
        {
            IReadOnlyList<string> seeds = _options.SeedsForClient?.Invoke(i) ?? Rotate(_cluster.Nodes.Select(n => n.Name).ToArray(), i);
            _clients.Add(await ConnectAsync("w" + i.ToString(CultureInfo.InvariantCulture), seeds));
        }
    }

    private static string[] Rotate(string[] names, int by) =>
        names.Length == 0 ? names : names.Skip(by % names.Length).Concat(names.Take(by % names.Length)).ToArray();

    private Task<DfsClient> ConnectAsync(string name, IReadOnlyList<string>? seeds) =>
        _cluster.ConnectAsync(new TestClientOptions { Name = name, SeedNodes = seeds, OperationTimeout = _options.OperationTimeout }, _ct);

    private async Task RunClientAsync(int index)
    {
        string process = "w" + index.ToString(CultureInfo.InvariantCulture);
        IRandomSource rng = new SeededRandom(_cluster.Seed ^ 0x4D455348UL).Fork(process);
        DfsVolume v = _clients[index].GetVolume(_volume);
        int done = 0;
        while ((!_stop || done < _options.MinOperationsPerClient) && done < _options.MaxOperationsPerClient && !_ct.IsCancellationRequested)
        {
            int roll = rng.Next(100);
            int seq = done;
            if (roll < 22)
                await WriteRegisterAsync(v, process, rng, seq);
            else if (roll < 44)
                await RecordReadAsync(v, process, RegisterPath(rng.Next(_options.RegisterKeys)), "reg:");
            else if (roll < 48)
                await DeleteRegisterAsync(v, process, rng);
            else if (roll < 58 && _options.Logs > 0)
                await AppendAsync(v, process, rng, index, seq);
            else if (roll < 63 && _options.Logs > 0)
                await RecordReadAsync(v, process, LogPath(rng.Next(_options.Logs)), "log:");
            else if (roll < 71 && _options.RenameDirectories > 0)
                await MoveAsync(v, process, rng);
            else if (roll < 77 && _options.RenameDirectories > 0)
                await RecordProbeAsync(v, process, rng.Next(_options.RenameDirectories));
            else if (roll < 86 || _options.TransactionKeys == 0)
                await LedgerWriteAsync(v, process, rng, seq);
            else
                await TransactionAsync(v, process, rng, seq);
            done++;
            if (done % 4 == 0)
                await ResolvePendingAsync(v, process, final: false);
            await _cluster.DelayAsync(_options.ThinkTime, _ct);
        }

        lock (_lock)
            _finished.Add(process);
    }

    /// <summary>
    /// Narrows unknown outcomes (history operations, unique writes, transactions) of one process (or all) by querying
    /// their ids with <c>resolve: true</c>. In-loop calls make one attempt per item (an outage simply leaves it pending);
    /// the final call retries. Outcomes are narrowed promptly so they are resolved before outcome retention expires.
    /// </summary>
    private async Task ResolvePendingAsync(DfsVolume v, string? process, bool final)
    {
        HistoryRecorder h = _cluster.History;
        long[] pending;
        LedgerEntry[] ledger;
        TxBuilder[] transactions;
        lock (_lock)
        {
            pending = _pendingHistory.Where(id => process is null || h.Get(id).Process == process).ToArray();
            ledger = _ledger.Where(e => e.Status == OpOutcome.Unknown && e.OperationId is not null && (process is null || e.Process == process)).ToArray();
            transactions = _transactions.Where(t => t.Outcome == OpOutcome.Unknown && t.TransactionId is not null && (process is null || t.Process == process)).ToArray();
        }

        foreach (long id in pending)
        {
            HistoryOp op = h.Get(id);
            bool done = op.Outcome != OpOutcome.Unknown || op.OperationId is null;
            if (!done)
            {
                DfsOperationOutcome? outcome = await QueryAsync(v, op.OperationId!.Value, final);
                if (outcome?.Status == DfsOutcomeStatus.Committed && outcome.Error == DfsErrorCode.None)
                {
                    h.Resolve(id, OpOutcome.Ok, op.Kind == "move" ? "moved" : null);
                    done = true;
                }
                else if (outcome?.Status == DfsOutcomeStatus.Aborted && !IsSemantic(outcome.Error))
                {
                    h.Resolve(id, OpOutcome.Failed);
                    done = true;
                }
            }

            if (done)
            {
                lock (_lock)
                    _pendingHistory.Remove(id);
            }
        }

        foreach (LedgerEntry entry in ledger)
        {
            DfsOperationOutcome? outcome = await QueryAsync(v, entry.OperationId!.Value, final);
            if (outcome?.Status == DfsOutcomeStatus.Committed && outcome.Error == DfsErrorCode.None)
                entry.Status = OpOutcome.Ok;
            else if (outcome?.Status == DfsOutcomeStatus.Aborted && !IsSemantic(outcome.Error))
                entry.Status = OpOutcome.Failed;
        }

        foreach (TxBuilder tx in transactions)
        {
            DfsOperationOutcome? outcome = null;
            for (int attempt = 0; attempt < (final ? 10 : 1) && outcome is null; attempt++)
            {
                try
                {
                    outcome = await v.Transactions.QueryOutcomeAsync(tx.TransactionId!.Value, resolve: true, _ct);
                }
                catch (Exception ex) when (MeshOracle.Unwrap(ex) is DfsUnavailableException or DfsCommitOutcomeUnknownException)
                {
                    if (final)
                        await _cluster.DelayAsync(TimeSpan.FromMilliseconds(500), _ct);
                }
            }

            if (outcome?.Status == DfsOutcomeStatus.Committed)
            {
                tx.Outcome = OpOutcome.Ok;
                tx.ResolveTime = Interlocked.Increment(ref _txClock);
            }
            else if (outcome?.Status == DfsOutcomeStatus.Aborted)
            {
                tx.Outcome = OpOutcome.Failed;
            }
        }
    }

    private byte[] Payload(IRandomSource rng, string process, int seq, bool large)
    {
        int length = large ? _options.LargePayload : _options.SmallPayload;
        byte[] data = TestData.Bytes(rng.NextUInt64(), Math.Max(16, length));
        Encoding.ASCII.GetBytes((process + "#" + seq.ToString(CultureInfo.InvariantCulture)).PadRight(12)[..12]).CopyTo(data, 0);
        BinaryPrimitives.WriteInt32LittleEndian(data.AsSpan(12), seq);
        return data;
    }

    private async Task WriteRegisterAsync(DfsVolume v, string process, IRandomSource rng, int seq)
    {
        string path = RegisterPath(rng.Next(_options.RegisterKeys));
        byte[] data = Payload(rng, process, seq, rng.Next(2) == 0);
        long id = _cluster.History.Invoke(process, "write", "reg:" + path, data);
        try
        {
            await v.Files.WriteAllBytesAsync(path, data, _ct);
            _cluster.History.Complete(id, OpOutcome.Ok);
        }
        catch (Exception ex)
        {
            Fail(id, ex, mutation: true, "write " + path);
        }
    }

    private async Task DeleteRegisterAsync(DfsVolume v, string process, IRandomSource rng)
    {
        string path = RegisterPath(rng.Next(_options.RegisterKeys));
        long id = _cluster.History.Invoke(process, "delete", "reg:" + path, null);
        try
        {
            await v.Files.DeleteAsync(path, _ct);
            _cluster.History.Complete(id, OpOutcome.Ok);
        }
        catch (Exception ex)
        {
            Fail(id, ex, mutation: true, "delete " + path);
        }
    }

    private async Task AppendAsync(DfsVolume v, string process, IRandomSource rng, int index, int seq)
    {
        string path = LogPath(rng.Next(_options.Logs));
        byte[] entry = new byte[16];
        BinaryPrimitives.WriteUInt32LittleEndian(entry, 0x4D455348u);
        BinaryPrimitives.WriteInt32LittleEndian(entry.AsSpan(4), index);
        BinaryPrimitives.WriteInt32LittleEndian(entry.AsSpan(8), seq);
        BinaryPrimitives.WriteUInt32LittleEndian(entry.AsSpan(12), (uint)rng.Next(int.MaxValue));
        long id = _cluster.History.Invoke(process, "append", "log:" + path, entry);
        try
        {
            await v.Files.AppendAllBytesAsync(path, entry, _ct);
            _cluster.History.Complete(id, OpOutcome.Ok);
        }
        catch (Exception ex)
        {
            Fail(id, ex, mutation: true, "append " + path);
        }
    }

    private async Task RecordReadAsync(DfsVolume v, string process, string path, string prefix)
    {
        long id = _cluster.History.Invoke(process, "read", prefix + path, null);
        try
        {
            byte[] data = await v.Files.ReadAllBytesAsync(path, _ct);
            _cluster.History.Complete(id, OpOutcome.Ok, data);
        }
        catch (FileNotFoundException)
        {
            _cluster.History.Complete(id, OpOutcome.Ok, null);
        }
        catch (Exception ex)
        {
            if (process == "final")
                throw new InvalidOperationException($"final read of {path} after recovery failed: {MeshOracle.Describe(ex)}", ex);
            FailRead(id, ex, "read " + path);
        }
    }

    private async Task MoveAsync(DfsVolume v, string process, IRandomSource rng)
    {
        int k = rng.Next(_options.RenameDirectories);
        bool aToB = rng.Next(2) == 0;
        string from = aToB ? "a" : "b";
        string to = aToB ? "b" : "a";
        string dir = RenameDirectory(k);
        long id = _cluster.History.Invoke(process, "move", "ren:" + dir, from + ">" + to);
        try
        {
            await v.Files.MoveAsync(dir + "/" + from, dir + "/" + to, overwrite: false, _ct);
            _cluster.History.Complete(id, OpOutcome.Ok, "moved");
        }
        catch (FileNotFoundException)
        {
            _cluster.History.Complete(id, OpOutcome.Ok, "absent");
        }
        catch (DfsAlreadyExistsException)
        {
            _cluster.History.Complete(id, OpOutcome.Ok, "dest-exists");
        }
        catch (Exception ex)
        {
            Fail(id, ex, mutation: true, $"move {dir}/{from}->{to}");
        }
    }

    private async Task RecordProbeAsync(DfsVolume v, string process, int k)
    {
        string dir = RenameDirectory(k);
        long id = _cluster.History.Invoke(process, "probe", "ren:" + dir, null);
        try
        {
            IReadOnlySet<string> names = await ListFileNamesAsync(v.Directories, dir);
            string observed = (names.Contains("a"), names.Contains("b")) switch
            {
                (true, true) => "both",
                (true, false) => "a",
                (false, true) => "b",
                _ => "none",
            };
            _cluster.History.Complete(id, OpOutcome.Ok, observed);
        }
        catch (Exception ex)
        {
            if (process == "final")
                throw new InvalidOperationException($"final listing of {dir} after recovery failed: {MeshOracle.Describe(ex)}", ex);
            FailRead(id, ex, "list " + dir);
        }
    }

    private async Task LedgerWriteAsync(DfsVolume v, string process, IRandomSource rng, int seq)
    {
        var entry = new LedgerEntry(process, $"{_options.Root}/acked/{process}-{seq.ToString(CultureInfo.InvariantCulture)}", Payload(rng, process, seq, rng.Next(3) == 0));
        lock (_lock)
            _ledger.Add(entry);
        try
        {
            await v.Files.WriteAllBytesAsync(entry.Path, entry.Data, _ct);
            entry.Status = OpOutcome.Ok;
        }
        catch (Exception ex)
        {
            Exception e = MeshOracle.Unwrap(ex);
            entry.Error = MeshOracle.Describe(e);
            entry.Status = MeshOracle.ClassifyOutcome(e);
            if (e is DfsCommitOutcomeUnknownException u)
                entry.OperationId = u.OperationId;
            NoteViolation(e, mutation: true, "ledger write " + entry.Path);
        }
    }

    private async Task TransactionAsync(DfsVolume v, string process, IRandomSource rng, int seq)
    {
        var tx = new TxBuilder(process, $"{process}-{seq.ToString(CultureInfo.InvariantCulture)}", Interlocked.Increment(ref _txClock));
        lock (_lock)
            _transactions.Add(tx);
        bool commitSent = false;
        DfsTransaction? t = null;
        try
        {
            t = await v.Transactions.BeginAsync(null, _ct);
            tx.TransactionId = t.Id;
            int a = rng.Next(_options.TransactionKeys);
            int b = rng.Next(_options.TransactionKeys);
            tx.Reads[TxPath(a)] = await ReadTextOrNullAsync(t.Files, TxPath(a));
            if (b != a)
                tx.Reads[TxPath(b)] = await ReadTextOrNullAsync(t.Files, TxPath(b));
            if (rng.Next(3) == 0)
                tx.Predicates.Add((TxDirectory, await ListFileNamesAsync(t.Directories, TxDirectory)));
            int w = rng.Next(_options.TransactionKeys);
            if (rng.Next(6) == 0)
            {
                await t.Files.DeleteAsync(TxPath(w), _ct);
                tx.Writes[TxPath(w)] = null;
            }
            else
            {
                string value = "tx:" + tx.Id;
                await t.Files.WriteAllTextAsync(TxPath(w), value, null, _ct);
                tx.Writes[TxPath(w)] = value;
            }

            commitSent = true;
            await t.CommitAsync(_ct);
            tx.Outcome = OpOutcome.Ok;
            tx.CompleteTime = Interlocked.Increment(ref _txClock);
        }
        catch (Exception ex)
        {
            Exception e = MeshOracle.Unwrap(ex);
            tx.CompleteTime = Interlocked.Increment(ref _txClock);
            tx.Error = MeshOracle.Describe(e);
            if (e is DfsCommitOutcomeUnknownException u)
            {
                tx.Outcome = OpOutcome.Unknown;
                tx.TransactionId ??= u.TransactionId;
            }
            else if (!commitSent || e is DfsTransactionConflictException or DfsUnavailableException or DfsSharingViolationException)
            {
                tx.Outcome = OpOutcome.Failed;
            }
            else
            {
                tx.Outcome = MeshOracle.ClassifyOutcome(e);
            }

            NoteViolation(e, mutation: true, "transaction " + tx.Id);
        }

        // Disposal aborts an uncommitted transaction and never commits (FSD §8). It is separate from the commit so that a
        // failing abort during an outage cannot mask (and misclassify) the commit's own outcome.
        if (t is not null)
        {
            try
            {
                await t.DisposeAsync();
            }
            catch (Exception ex)
            {
                Exception e = MeshOracle.Unwrap(ex);
                if (e is not (DfsUnavailableException or DfsCommitOutcomeUnknownException or DfsTransactionConflictException))
                    NoteViolation(e, mutation: false, "disposing transaction " + tx.Id);
            }
        }
    }

    private void Fail(long id, Exception ex, bool mutation, string what)
    {
        Exception e = MeshOracle.Unwrap(ex);
        OpOutcome outcome = MeshOracle.ClassifyOutcome(e);
        _cluster.History.Complete(id, outcome, e);
        if (outcome == OpOutcome.Unknown && e is DfsCommitOutcomeUnknownException u)
        {
            _cluster.History.AttachOperationId(id, u.OperationId);
            lock (_lock)
                _pendingHistory.Add(id);
        }

        NoteViolation(e, mutation, what);
    }

    /// <summary>A read that returned no result has no effect and no observation; it is recorded as Failed (excluded).</summary>
    private void FailRead(long id, Exception ex, string what)
    {
        Exception e = MeshOracle.Unwrap(ex);
        _cluster.History.Complete(id, OpOutcome.Failed, e);
        NoteViolation(e, mutation: false, what);
    }

    private void NoteViolation(Exception e, bool mutation, string what)
    {
        bool legal = e switch
        {
            DfsPlacementUnavailableException p => p.AchievedCopies < p.RequiredCopies || p.AchievedZones < p.RequiredZones,
            DfsUnavailableException => true,
            DfsCommitOutcomeUnknownException u => mutation && !u.OperationId.IsEmpty,
            DfsSharingViolationException => true,
            DfsTransactionConflictException => true,
            DfsLeaseLostException => true,

            // A client cut off from the authority longer than the session timeout (partitions, isolation) has its
            // session expired: in-flight ids are rejected as expired (FSD §13) and an internal read handle/pin may have
            // expired (LOCK-05). Both are explicit, typed outcomes; an expired mutation is recorded as unknown.
            DfsOperationExpiredException => true,
            DfsPinExpiredException => true,
            OperationCanceledException => _ct.IsCancellationRequested,
            _ => false,
        };
        if (!legal)
        {
            lock (_lock)
                _violations.Add($"{what}: {MeshOracle.Describe(e)}");
        }
    }

    private static bool IsSemantic(DfsErrorCode code) => code is DfsErrorCode.FileNotFound or DfsErrorCode.DirectoryNotFound or DfsErrorCode.AlreadyExists
        or DfsErrorCode.DirectoryNotEmpty or DfsErrorCode.NotADirectory or DfsErrorCode.IsADirectory;

    private async Task<DfsOperationOutcome?> QueryAsync(DfsVolume v, DfsOperationId id, bool final)
    {
        for (int attempt = 0; attempt < (final ? 10 : 1); attempt++)
        {
            try
            {
                return await v.QueryOperationAsync(id, resolve: true, _ct);
            }
            catch (Exception ex) when (MeshOracle.Unwrap(ex) is DfsUnavailableException or DfsCommitOutcomeUnknownException)
            {
                if (final)
                    await _cluster.DelayAsync(TimeSpan.FromMilliseconds(500), _ct);
            }
        }

        return null;
    }

    private async Task<string?> ReadTextOrNullAsync(DfsFileClient files, string path)
    {
        try
        {
            return await files.ReadAllTextAsync(path, null, _ct);
        }
        catch (FileNotFoundException)
        {
            return null;
        }
    }

    private async Task<byte[]?> ReadOrNullAsync(DfsFileClient files, string path)
    {
        try
        {
            return await files.ReadAllBytesAsync(path, _ct);
        }
        catch (FileNotFoundException)
        {
            return null;
        }
    }

    private async Task<IReadOnlySet<string>> ListFileNamesAsync(DfsDirectoryClient directories, string dir)
    {
        var names = new SortedSet<string>(StringComparer.Ordinal);
        await foreach (DfsEntryInfo entry in directories.EnumerateAsync(dir, new DfsEnumerationOptions { Kinds = DfsEntryKinds.Files }, _ct))
            names.Add(entry.Name);
        return names;
    }

    private sealed class LedgerEntry(string process, string path, byte[] data)
    {
        public string Process { get; } = process;
        public string Path { get; } = path;
        public byte[] Data { get; } = data;
        public OpOutcome Status { get; set; } = OpOutcome.Unknown;
        public DfsOperationId? OperationId { get; set; }
        public string? Error { get; set; }
    }

    private sealed class TxBuilder(string process, string id, long invokeTime)
    {
        public string Process { get; } = process;
        public string Id { get; } = id;
        public long InvokeTime { get; } = invokeTime;
        public long? CompleteTime { get; set; }
        public long? ResolveTime { get; set; }
        public OpOutcome Outcome { get; set; } = OpOutcome.Unknown;
        public DfsTransactionId? TransactionId { get; set; }
        public string? Error { get; set; }
        public Dictionary<string, string?> Reads { get; } = new(StringComparer.Ordinal);
        public Dictionary<string, string?> Writes { get; } = new(StringComparer.Ordinal);
        public List<(string Directory, IReadOnlySet<string> Names)> Predicates { get; } = [];

        public TxRecord Build() => new(Id, InvokeTime, CompleteTime, Outcome, Reads, Writes, Predicates) { ResolveTime = ResolveTime };
    }
}
