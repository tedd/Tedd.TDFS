using System.Globalization;

namespace Tedd.Tdfs.Testing.Mesh;

/// <summary>
/// A single-process namespace script (create directory/file, move file/directory, delete file/directory, including
/// deliberately invalid steps) executed through faults and compared with <see cref="ReferenceNamespaceModel"/> (PROP-002
/// style). Every step uses an operation id reserved before transmission; definite availability failures are retried
/// with the same id, unknown outcomes are resolved with <c>QueryOperationAsync(resolve: true)</c> before the next step,
/// so the model is updated exactly once per committed step. At the end the whole subtree (paths, kinds) and the inode
/// identities observed after creation must match the model.
/// </summary>
public sealed class MeshNamespaceScript
{
    private readonly TestCluster _cluster;
    private readonly DfsVolume _volume;
    private readonly string _root;
    private readonly int _operations;
    private readonly CancellationToken _ct;
    private readonly ReferenceNamespaceModel _model = new();
    private readonly Dictionary<long, InodeId> _identity = [];
    private readonly List<string> _log = [];
    private readonly TimeSpan _maxWait;
    private Task? _run;

    private MeshNamespaceScript(TestCluster cluster, DfsVolume volume, string root, int operations, TimeSpan maxWait, CancellationToken ct)
    {
        _cluster = cluster;
        _volume = volume;
        _root = root;
        _operations = operations;
        _maxWait = maxWait;
        _ct = ct;
    }

    /// <summary>Steps executed (for diagnostics).</summary>
    public IReadOnlyList<string> Log => _log;

    /// <summary>Creates the root and starts the script in the background (it waits through outages).</summary>
    public static async Task<MeshNamespaceScript> StartAsync(TestCluster cluster, DfsClient client, string volume, string root, int operations,
        TimeSpan maxWait, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(cluster);
        ArgumentNullException.ThrowIfNull(client);
        var script = new MeshNamespaceScript(cluster, client.GetVolume(volume), root, operations, maxWait, ct);
        await script._volume.Directories.CreateDirectoryAsync(root, ct);
        Assert.True(script._model.CreateDirectory(root, recursive: true, out string? error), "model rejected the script root: " + error);
        script._run = script.RunAsync();
        return script;
    }

    /// <summary>Waits for the remaining steps (after recovery) and compares the namespace with the model.</summary>
    public async Task VerifyAsync()
    {
        await _run!;
        var actual = new SortedDictionary<string, (DfsEntryKind Kind, InodeId Inode)>(StringComparer.Ordinal);
        var pending = new Queue<string>();
        pending.Enqueue(_root);
        while (pending.Count > 0)
        {
            string dir = pending.Dequeue();
            await foreach (DfsEntryInfo entry in _volume.Directories.EnumerateAsync(dir, null, _ct))
            {
                actual[entry.Path] = (entry.Kind, entry.InodeId);
                if (entry.Kind == DfsEntryKind.Directory)
                    pending.Enqueue(entry.Path);
            }
        }

        string[] expected = _model.AllPaths().Where(p => p.StartsWith(_root + "/", StringComparison.Ordinal)).ToArray();
        string[] missing = expected.Where(p => !actual.ContainsKey(p)).ToArray();
        string[] extra = actual.Keys.Where(p => !_model.Exists(p)).ToArray();
        Assert.True(missing.Length == 0 && extra.Length == 0,
            $"namespace under {_root} differs from the reference model: missing [{string.Join(", ", missing)}], unexpected [{string.Join(", ", extra)}]; " +
            $"steps: {string.Join(" | ", _log)}");
        foreach (string path in expected)
        {
            bool isDir = _model.IsDirectory(path);
            Assert.True(actual[path].Kind == (isDir ? DfsEntryKind.Directory : DfsEntryKind.File), $"{path} has kind {actual[path].Kind} but the model says {(isDir ? "directory" : "file")}");
            if (_identity.TryGetValue(_model.InodeOf(path), out InodeId inode))
                Assert.True(actual[path].Inode == inode, $"{path} resolves to inode {actual[path].Inode.Value} but the model's inode was observed as {inode.Value} after creation (identity not preserved across moves, INV-07)");
        }

        InodeId[] inodes = actual.Values.Select(v => v.Inode).ToArray();
        Assert.True(inodes.Distinct().Count() == inodes.Length, "two paths resolve to the same inode (no hard links exist)");
    }

    private async Task RunAsync()
    {
        IRandomSource rng = new SeededRandom(_cluster.Seed ^ 0x4E53UL).Fork("ns");
        for (int step = 0; step < _operations && !_ct.IsCancellationRequested; step++)
        {
            (string kind, string a, string b, bool flag) = Next(rng, step);
            DfsOperationId id = await ReserveIdAsync();
            await ExecuteStepAsync(step, kind, a, b, flag, id);
            await _cluster.DelayAsync(TimeSpan.FromMilliseconds(30), _ct);
        }
    }

    private (string Kind, string A, string B, bool Flag) Next(IRandomSource rng, int step)
    {
        string[] paths = _model.AllPaths().Where(p => p.StartsWith(_root + "/", StringComparison.Ordinal)).ToArray();
        string[] dirs = paths.Where(_model.IsDirectory).Prepend(_root).ToArray();
        string[] files = paths.Where(_model.IsFile).ToArray();
        string s = step.ToString(CultureInfo.InvariantCulture);
        string Pick(string[] from) => from[rng.Next(from.Length)];
        int roll = rng.Next(100);
        if (roll < 18 || dirs.Length < 3)
            return ("mkdir", Pick(dirs) + "/d" + s, string.Empty, false);
        if (roll < 40 || files.Length == 0)
            return ("create", rng.Next(10) == 0 && files.Length > 0 ? Pick(files) : Pick(dirs) + "/f" + s, string.Empty, false);
        if (roll < 58)
        {
            string source = Pick(files);
            string[] others = files.Where(f => f != source).ToArray();
            bool reuse = rng.Next(5) == 0 && others.Length > 0;
            return ("movefile", source, reuse ? Pick(others) : Pick(dirs) + "/m" + s, reuse && rng.Next(2) == 0);
        }

        if (roll < 70 && dirs.Length > 1)
        {
            string source = dirs.Skip(1).ToArray()[rng.Next(dirs.Length - 1)];
            return ("movedir", source, Pick(dirs) + "/v" + s, false);
        }

        if (roll < 85)
            return ("deletefile", rng.Next(8) == 0 ? _root + "/absent" + s : Pick(files), string.Empty, false);
        if (dirs.Length > 1)
            return ("deletedir", dirs.Skip(1).ToArray()[rng.Next(dirs.Length - 1)], string.Empty, rng.Next(2) == 0);
        return ("mkdir", _root + "/d" + s, string.Empty, false);
    }

    private async Task<DfsOperationId> ReserveIdAsync()
    {
        DateTimeOffset deadline = _cluster.Time.GetUtcNow() + _maxWait;
        while (true)
        {
            try
            {
                return await _volume.NewOperationIdAsync(_ct);
            }
            catch (Exception ex) when (IsAvailability(ex))
            {
                if (_cluster.Time.GetUtcNow() >= deadline)
                    throw new TimeoutException($"namespace script could not reserve an operation id within {_maxWait}: {MeshOracle.Describe(ex)}", ex);
                await _cluster.DelayAsync(TimeSpan.FromMilliseconds(250), _ct);
            }
        }
    }

    private async Task ExecuteStepAsync(int step, string kind, string a, string b, bool flag, DfsOperationId id)
    {
        DateTimeOffset deadline = _cluster.Time.GetUtcNow() + _maxWait;
        while (true)
        {
            Exception? failure = null;
            try
            {
                await ExecuteAsync(kind, a, b, flag, id);
            }
            catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
            {
                failure = MeshOracle.Unwrap(ex);
            }

            if (failure is null)
            {
                await ApplyAsync(step, kind, a, b, flag, succeeded: true, null);
                return;
            }

            if (IsSemantic(failure, out string code))
            {
                await ApplyAsync(step, kind, a, b, flag, succeeded: false, code);
                return;
            }

            if (failure is DfsCommitOutcomeUnknownException || failure is TimeoutException || failure is DfsException { ErrorCode: DfsErrorCode.Timeout })
            {
                DfsOperationOutcome outcome = await ResolveAsync(id, deadline);
                if (outcome.Status == DfsOutcomeStatus.Committed)
                {
                    bool ok = outcome.Error == DfsErrorCode.None;
                    await ApplyAsync(step, kind, a, b, flag, ok, ok ? null : outcome.Error.ToString());
                    return;
                }

                if (outcome.Status == DfsOutcomeStatus.Aborted && IsSemanticCode(outcome.Error))
                {
                    await ApplyAsync(step, kind, a, b, flag, succeeded: false, outcome.Error.ToString());
                    return;
                }

                _log.Add($"#{step} {kind} {a} {b}: aborted after unknown outcome ({outcome.Error})");
                return; // definitely not applied
            }

            if (!IsAvailability(failure))
                throw new InvalidOperationException($"namespace step #{step} {kind} {a} {b} failed with an untyped error: {MeshOracle.Describe(failure)}", failure);
            if (_cluster.Time.GetUtcNow() >= deadline)
                throw new TimeoutException($"namespace step #{step} {kind} {a} {b} stayed unavailable for {_maxWait}: {MeshOracle.Describe(failure)}");
            await _cluster.DelayAsync(TimeSpan.FromMilliseconds(250), _ct); // definite failure: retry with the same id
        }
    }

    private async Task ExecuteAsync(string kind, string a, string b, bool flag, DfsOperationId id)
    {
        var options = new DfsMutationOptions { OperationId = id };
        switch (kind)
        {
            case "mkdir":
                await _volume.Directories.CreateDirectoryAsync(a, options, _ct);
                break;
            case "create":
                DfsStream stream = await _volume.Files.OpenAsync(a,
                    new DfsOpenOptions { Mode = FileMode.CreateNew, Access = FileAccess.Write, Share = FileShare.None, OperationId = id }, _ct);
                try
                {
                    await stream.DisposeAsync();
                }
                catch (Exception ex) when (IsAvailability(ex))
                {
                    // The create committed at open; closing the handle may fail during an outage (the lease expires).
                }

                break;
            case "movefile":
                await _volume.Files.MoveAsync(a, b, flag, options, _ct);
                break;
            case "movedir":
                await _volume.Directories.MoveAsync(a, b, options, _ct);
                break;
            case "deletefile":
                await _volume.Files.DeleteAsync(a, options, _ct);
                break;
            case "deletedir":
                await _volume.Directories.DeleteAsync(a, flag, options, _ct);
                break;
            default:
                throw new ArgumentException("Unknown step " + kind);
        }
    }

    private async Task ApplyAsync(int step, string kind, string a, string b, bool flag, bool succeeded, string? code)
    {
        bool modelOk;
        string? modelError;
        switch (kind)
        {
            case "mkdir":
                modelOk = _model.CreateDirectory(a, recursive: true, out modelError);
                break;
            case "create":
                modelOk = _model.CreateFile(a, out modelError);
                break;
            case "movefile":
            case "movedir":
                modelOk = _model.Move(a, b, kind == "movefile" && flag, out modelError);
                break;
            case "deletefile":
                modelOk = _model.Delete(a, recursive: false, out modelError);
                if (!modelOk && modelError is "FileNotFound" or "DirectoryNotFound")
                    modelOk = true; // Files.Delete of an absent file succeeds (FSD §5.4)
                break;
            case "deletedir":
                modelOk = _model.Delete(a, flag, out modelError);
                break;
            default:
                throw new ArgumentException("Unknown step " + kind);
        }

        _log.Add($"#{step} {kind} {a} {b}{(flag ? " (flag)" : string.Empty)} => impl {(succeeded ? "ok" : code)}, model {(modelOk ? "ok" : modelError)}");
        Assert.True(modelOk == succeeded,
            $"namespace step #{step} {kind} {a} {b}: implementation {(succeeded ? "succeeded" : "failed with " + code)} but the reference model {(modelOk ? "succeeded" : "failed with " + modelError)}");
        if (succeeded && kind is "mkdir" or "create")
        {
            try
            {
                InodeId inode = kind == "mkdir"
                    ? (await _volume.Directories.GetInfoAsync(a, _ct)).InodeId
                    : (await _volume.Files.GetInfoAsync(a, _ct)).InodeId;
                if (_model.TryGetInode(a, out long modelInode))
                    _identity.TryAdd(modelInode, inode);
            }
            catch (Exception ex) when (IsAvailability(ex))
            {
                // identity capture is best effort during outages
            }
        }
    }

    private async Task<DfsOperationOutcome> ResolveAsync(DfsOperationId id, DateTimeOffset deadline)
    {
        while (true)
        {
            try
            {
                DfsOperationOutcome outcome = await _volume.QueryOperationAsync(id, resolve: true, _ct);
                Assert.True(outcome.Status is DfsOutcomeStatus.Committed or DfsOutcomeStatus.Aborted,
                    $"QueryOperationAsync(resolve: true) for {id} must be definite, got {outcome.Status} (OUT-013/014)");
                return outcome;
            }
            catch (Exception ex) when (IsAvailability(ex))
            {
                if (_cluster.Time.GetUtcNow() >= deadline)
                    throw new TimeoutException($"could not resolve {id} within {_maxWait}: {MeshOracle.Describe(ex)}", ex);
                await _cluster.DelayAsync(TimeSpan.FromMilliseconds(250), _ct);
            }
        }
    }

    private static bool IsAvailability(Exception ex) =>
        MeshOracle.Unwrap(ex) is DfsUnavailableException or DfsCommitOutcomeUnknownException or TimeoutException or DfsLeaseLostException
            or DfsException { ErrorCode: DfsErrorCode.Timeout };

    private static bool IsSemantic(Exception ex, out string code)
    {
        code = ex switch
        {
            FileNotFoundException => "FileNotFound",
            DirectoryNotFoundException => "DirectoryNotFound",
            DfsAlreadyExistsException => "AlreadyExists",
            DfsDirectoryNotEmptyException => "DirectoryNotEmpty",
            DfsException d when IsSemanticCode(d.ErrorCode) => d.ErrorCode.ToString(),
            ArgumentException => "InvalidArgument",
            _ => string.Empty,
        };
        return code.Length > 0;
    }

    private static bool IsSemanticCode(DfsErrorCode code) => code is DfsErrorCode.FileNotFound or DfsErrorCode.DirectoryNotFound or DfsErrorCode.AlreadyExists
        or DfsErrorCode.DirectoryNotEmpty or DfsErrorCode.NotADirectory or DfsErrorCode.IsADirectory or DfsErrorCode.InvalidArgument or DfsErrorCode.InvalidPath;
}
