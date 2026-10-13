using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Immutable;
using System.Security.Cryptography;
using Tedd.Tdfs.Data;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Control;
using Tedd.Tdfs.Server.Data;
using Tedd.Tdfs.Server.Runtime;
using Tedd.Tdfs.Server.Volumes.Apply;
using Tedd.Tdfs.Server.Volumes.Commands;
using Tedd.Tdfs.Server.Volumes.State;

namespace Tedd.Tdfs.Server.Volumes.Service;

/// <summary>
/// Snapshot restore into a new volume (PROTOCOL.md §8.5, ARCHITECTURE §5.7). The source volume's leader coordinates:
/// <list type="number">
/// <item><description>it accepts the request with a committed <c>RestoreBegin</c> (an intent that keeps the snapshot root reachable);</description></item>
/// <item><description>the control leader creates the new volume in the <see cref="WireVolumeState.Restoring"/> state (never served while incomplete);</description></item>
/// <item><description>the new volume's leader opens the install, allocating the staging session the copies are bound to;</description></item>
/// <item><description>every referenced object is copied server-side to a new id owned by the new volume (<c>CopyObjects</c> on its holders in the
/// new volume's pool, or read and re-placed there), with receipts meeting the policy;</description></item>
/// <item><description>the rewritten inodes are staged and installed atomically (<c>RestoreChunk</c>/<c>RestoreComplete</c>);</description></item>
/// <item><description>the control leader activates the volume, and <c>RestoreFinish</c> records the outcome under the request's operation id.</description></item>
/// </list>
/// Every step is idempotent (deduplicated request ids, deterministic object ids, restore source tags), so a new leader resumes
/// an accepted restore from the start; a restore that cannot complete removes its volume and records the failure.
/// </summary>
internal sealed partial class VolumeService
{
    /// <summary>Largest encoded inode payload of one <c>RestoreInstall</c> stage request.</summary>
    private const int RestoreChunkBytes = 512 * 1024;

    /// <summary>Most inodes of one stage request.</summary>
    private const int RestoreChunkInodes = 4096;

    /// <summary>Most copies of one <c>CopyObjects</c> request.</summary>
    private const int RestoreCopyBatch = 256;

    /// <summary>Format byte of the encoded inode records of a <c>RestoreInstall</c> stage request.</summary>
    private const byte RestoreRecordsFormat = 1;

    private readonly Dictionary<DfsOperationId, Task<VolumeApplyResult>> _restoreRuns = [];
    private CancellationTokenSource? _restoreCancel;

    /// <summary>Longest time one restore step keeps retrying transient failures before the run yields (it resumes later).</summary>
    private TimeSpan RestoreStepTimeout
        => TimeSpan.FromTicks(Math.Max(TimeSpan.FromSeconds(10).Ticks, _s.Timings.Tick.Ticks * _s.Timings.ElectionTicksMax * 20));

    // ------------------------------------------------------------------------------------------------- source side

    /// <summary><c>RestoreSnapshot</c>: accepts the restore (or finds the accepted one) and waits for its outcome.</summary>
    public async Task<IWireMessage> RestoreSnapshotAsync(RpcContext context, RestoreSnapshotMessage request)
    {
        CancellationToken ct = context.Cancellation;
        RejectTransaction(request);
        await EnsureLeaderReadyAsync(ct);
        OperationHeader op = Header(context, request, DfsOperationKind.RestoreSnapshot);
        if (!op.HasOperation)
            throw new ArgumentException("RestoreSnapshot requests must carry an operation id (OUT-001).");
        await ReadBarrierAsync(ct);
        GateDecision gate = OperationGate.Check(State, op);
        switch (gate.Kind)
        {
            case GateKind.Stored:
                return WithOperation(VolumeStateMachine.StoredResult(gate.Outcome!), op.Id);
            case GateKind.Expired:
                return new ErrorReplyMessage(DfsErrorCode.OperationExpired, gate.Message, op.Id);
            case GateKind.Mismatch:
                return new ErrorReplyMessage(DfsErrorCode.OperationIdMismatch, gate.Message, op.Id);
            case GateKind.Invalid:
                return new ErrorReplyMessage(DfsErrorCode.InvalidArgument, gate.Message, op.Id);
        }

        if (State.Restores.TryGetValue(op.Id, out RestoreIntentRecord? accepted))
        {
            if (accepted.PayloadHash != op.PayloadHash || !string.Equals(accepted.Principal, op.Principal, StringComparison.Ordinal))
                return new ErrorReplyMessage(DfsErrorCode.OperationIdMismatch, $"Operation {op.Id} is in flight for a different restore.", op.Id);
        }
        else
        {
            CheckRestoreRequest(op, request);
            var begin = new RestoreBeginCommand(op.Id, op.PayloadHash, request.Name, request.NewVolumeName, request.StoragePool);
            VolumeApplyResult result = await ProposeAsync(Stamp(begin, op with { Id = default }), ct);
            if (!result.IsSuccess)
                return WithOperation(result, op.Id);
            if (result.Value is OutcomeRecord stored)
                return WithOperation(VolumeStateMachine.StoredResult(stored), op.Id);
        }

        VolumeApplyResult outcome;
        try
        {
            outcome = await StartRestore(op.Id).WaitAsync(ct);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            // This leader lost its term while the restore ran; the next leader resumes it from the committed intent.
            throw Group.NotLeader();
        }

        return WithOperation(outcome, op.Id);
    }

    /// <summary>Leader-side checks before accepting a restore (apply re-checks authorization and the snapshot).</summary>
    private void CheckRestoreRequest(OperationHeader op, RestoreSnapshotMessage request)
    {
        RequireGrant(op.Principal, DfsPermissions.VolumeAdmin, "restore snapshot");
        if (!op.IsAdmin)
        {
            _s.Control.ReportSecurityEvent("volume:restore snapshot", "denied", "volume=" + Name, op.Principal);
            throw new UnauthorizedAccessException($"Principal '{op.Principal}' must be a cluster administrator to restore a snapshot into a new volume.");
        }

        if (!State.Snapshots.ContainsKey(request.Name))
            throw DfsErrors.ToException(DfsErrorCode.SnapshotNotFound, $"Snapshot '{request.Name}' does not exist.");
        if (ControlStateMachine.GetVolumeNameError(request.NewVolumeName) is { } nameError)
            throw new ArgumentException(nameError);
        ControlState control = _s.Control.State;
        if (control.FindVolumeByName(request.NewVolumeName) is not null)
            throw new DfsAlreadyExistsException($"Volume '{request.NewVolumeName}' already exists.");
        string pool = request.StoragePool ?? Record.StoragePool;
        if (DfsPoolNames.GetError(pool) is { } poolError)
            throw new ArgumentException(poolError);
        if (!control.PoolExists(pool))
            throw new DfsException(DfsErrorCode.PoolNotFound, $"Storage pool '{pool}' does not exist.");
        if (ControlStateMachine.CheckPolicy(control, Record.DefaultPolicy, pool) is { } error)
            throw new DfsException(error.Code, error.Message);
        foreach (KeyValuePair<string, DfsStoragePolicy> named in Record.NamedPolicies)
        {
            if (ControlStateMachine.CheckPolicy(control, named.Value, pool) is { } namedError)
                throw new DfsException(namedError.Code, $"Policy '{named.Key}': {namedError.Message}");
        }
    }

    private static IWireMessage WithOperation(VolumeApplyResult result, DfsOperationId id)
    {
        if (result.IsSuccess)
            return result.Reply ?? throw new InvalidOperationException("The restore produced no reply.");
        return result.Reply is ErrorReplyMessage error ? error with { OperationId = id } : new ErrorReplyMessage(result.Code, result.Message, id);
    }

    /// <summary>The run of an accepted restore (a running one, or a new run on this leader).</summary>
    private Task<VolumeApplyResult> StartRestore(DfsOperationId id)
    {
        if (_restoreRuns.TryGetValue(id, out Task<VolumeApplyResult>? running) && !running.IsCompleted)
            return running;
        _restoreCancel ??= new CancellationTokenSource();
        Task<VolumeApplyResult> run = RunRestoreAsync(id, _restoreCancel.Token);
        _restoreRuns[id] = run;
        _ = ObserveRestoreAsync(id, run);
        return run;
    }

    private async Task ObserveRestoreAsync(DfsOperationId id, Task<VolumeApplyResult> run)
    {
        try
        {
            await run;
        }
        catch (OperationCanceledException)
        {
            _s.Debug("volume", NodeServices.Inv($"Volume {Name}: restore {id} stopped on this leader (leadership changed)."));
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            _s.Warn("volume", NodeServices.Inv($"Volume {Name}: restore {id} did not finish yet and is resumed later: {ex.GetType().Name}: {ex.Message}"));
        }
    }

    /// <summary>Resumes accepted restores without a run on this leader (leader maintenance).</summary>
    private void ResumeRestores()
    {
        foreach (DfsOperationId id in State.Restores.Keys)
        {
            if (!_restoreRuns.TryGetValue(id, out Task<VolumeApplyResult>? run) || run.IsCompleted)
                _ = StartRestore(id);
        }
    }

    /// <summary>Stops every restore run of this leader (leadership lost or changed term).</summary>
    private void CancelRestores()
    {
        _restoreCancel?.Cancel();
        _restoreCancel = null;
        _restoreRuns.Clear();
    }

    private async Task<VolumeApplyResult> RunRestoreAsync(DfsOperationId id, CancellationToken ct)
    {
        if (!State.Restores.TryGetValue(id, out RestoreIntentRecord? intent))
            throw new InvalidOperationException($"Restore {id} is not in progress.");
        var progress = new RestoreProgress();
        ErrorReplyMessage failure;
        try
        {
            RestoreSnapshotResultMessage reply = await ExecuteRestoreAsync(intent, progress, ct);
            _s.Info("volume", NodeServices.Inv($"Volume {Name}: restored snapshot '{intent.Snapshot}' into volume {reply.Volume.Name} ({reply.Volume.Id})."));
            return await FinishRestoreAsync(intent, DfsErrorCode.None, string.Empty, MessageCodec.Encode(reply), ct);
        }
        catch (Exception ex) when (!ct.IsCancellationRequested && IsDefiniteRestoreFailure(ex))
        {
            failure = ErrorReplyMessage.FromException(ex, id);
            _s.Warn("volume", NodeServices.Inv(
                $"Volume {Name}: restore {id} of snapshot '{intent.Snapshot}' into '{intent.NewVolumeName}' failed: {failure.Code}: {failure.Message}"));
        }

        if (progress.Target is { } target)
            await ControlStepAsync(WireRestoreAction.Abandon, intent, target, ct);
        return await FinishRestoreAsync(intent, failure.Code, failure.Message, MessageCodec.Encode(failure), ct);
    }

    private async Task<RestoreSnapshotResultMessage> ExecuteRestoreAsync(RestoreIntentRecord intent, RestoreProgress progress, CancellationToken ct)
    {
        string source = RestoreSourceOf(intent);
        VolumeEntry created = await ControlStepAsync(WireRestoreAction.Create, intent, default, ct)
            ?? throw new DfsException(DfsErrorCode.Cancelled, $"The restore into '{intent.NewVolumeName}' was abandoned before it completed.");
        progress.Target = created.Id;
        VolumeRecord target = await WaitForControlRecordAsync(created.Id, ct);

        RestoreInstallResultMessage opened = await InstallAsync(target, WireRestorePhase.Begin, source, default, ReadOnlyMemory<byte>.Empty, 0, ct);
        CommitToken installed = opened.Token;
        if (installed.IsNone)
        {
            IReadOnlyList<InodeRecord> inodes = await CopyRestoreObjectsAsync(intent, target, opened.Staging, ct);
            foreach ((ReadOnlyMemory<byte> records, int count) in EncodeRestoreChunks(inodes))
                await InstallAsync(target, WireRestorePhase.Stage, source, opened.Staging, records, count, ct);
            RestoreInstallResultMessage done = await InstallAsync(target, WireRestorePhase.Complete, source, opened.Staging, ReadOnlyMemory<byte>.Empty,
                inodes.Count, ct);
            installed = done.Token;
        }

        VolumeEntry active = await ControlStepAsync(WireRestoreAction.Activate, intent, target.Id, ct)
            ?? throw new DfsException(DfsErrorCode.VolumeNotFound, $"Volume '{intent.NewVolumeName}' disappeared before it was activated.");
        return new RestoreSnapshotResultMessage(active, installed);
    }

    private async Task<VolumeApplyResult> FinishRestoreAsync(RestoreIntentRecord intent, DfsErrorCode code, string message, ReadOnlyMemory<byte> reply,
        CancellationToken ct)
    {
        (ImmutableArray<string> identities, bool admin) = IdentitiesOf(intent.Principal);
        var header = new OperationHeader(intent.Operation, intent.Principal, DfsOperationKind.RestoreSnapshot, intent.PayloadHash, 0, 0, identities, admin,
            GrantsOf(intent.Principal), MutationFlags());
        return await ProposeAsync(Stamp(new RestoreFinishCommand(intent.Operation, code, message, reply), header), ct);
    }

    /// <summary>Transient failures are retried (and a run that still fails is resumed later); everything else ends the restore.</summary>
    private static bool IsDefiniteRestoreFailure(Exception ex) => ex is not OperationCanceledException && !IsTransientRestoreFailure(ex, volumeMayLag: false);

    private static bool IsTransientRestoreFailure(Exception ex, bool volumeMayLag) => ex switch
    {
        DfsException { ErrorCode: DfsErrorCode.NotLeader or DfsErrorCode.Unavailable or DfsErrorCode.QuorumUnavailable or DfsErrorCode.NodeUnavailable
            or DfsErrorCode.Timeout or DfsErrorCode.StaleIncarnation } => true,
        DfsException { ErrorCode: DfsErrorCode.VolumeNotFound } => volumeMayLag,
        DfsException => false,
        TimeoutException or IOException or NodeLoopStoppedException => true,
        _ => false,
    };

    /// <summary>Runs <paramref name="call"/>, retrying transient failures with backoff for at most <see cref="RestoreStepTimeout"/>.</summary>
    private async Task<T> RetryRestoreStepAsync<T>(string what, Func<CancellationToken, Task<T>> call, CancellationToken ct, bool volumeMayLag = false)
    {
        long started = _s.Time.GetTimestamp();
        TimeSpan delay = _s.Timings.Tick;
        TimeSpan maxDelay = _s.Timings.Tick * Math.Max(1, _s.Timings.ElectionTicksMax);
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            try
            {
                return await call(ct);
            }
            catch (Exception ex) when (!ct.IsCancellationRequested && IsTransientRestoreFailure(ex, volumeMayLag))
            {
                if (_s.Time.GetElapsedTime(started) > RestoreStepTimeout)
                    throw new DfsUnavailableException(DfsErrorCode.Timeout, $"{what} did not succeed within {RestoreStepTimeout}: {ex.Message}", ex);
                _s.Debug("volume", NodeServices.Inv($"Volume {Name}: {what} failed transiently, retrying: {ex.Message}"));
                await Task.Delay(delay, _s.Time, ct);
                delay = delay * 2 > maxDelay ? maxDelay : delay * 2;
            }
        }
    }

    /// <summary>A control-plane step of the restore at the control leader (deduplicated by a request id derived from the intent).</summary>
    private Task<VolumeEntry?> ControlStepAsync(WireRestoreAction action, RestoreIntentRecord intent, VolumeId volume, CancellationToken ct)
    {
        var message = new RestoreVolumeMessage(RestoreRequestId(intent, action), action, Record.Id, volume, intent.NewVolumeName, intent.StoragePool,
            intent.Principal);
        return RetryRestoreStepAsync("restore " + action.ToString().ToLowerInvariant(), async token =>
        {
            ControlHost control = _s.Control;
            if (control.IsLeader)
                return (await new RestoreControl(_s).ExecuteAsync(message, token)).Volume;
            if (control.Leader is not { } leader || leader == _s.NodeId)
                throw new DfsUnavailableException(DfsErrorCode.NotLeader, "No control leader is known.");
            IWireMessage reply = await _s.Peers.CallAsync(leader, message, ct: token);
            return ((RestoreVolumeResultMessage)reply).Volume;
        }, ct);
    }

    /// <summary>Waits until this node's control replica knows the new volume (its record names the voters and the pool).</summary>
    private Task<VolumeRecord> WaitForControlRecordAsync(VolumeId id, CancellationToken ct)
        => RetryRestoreStepAsync("learning the new volume", async token =>
        {
            if (_s.Control.State.FindVolume(id) is { } record)
                return record;
            await Barrier.WaitAsync(token);
            return _s.Control.State.FindVolume(id) ?? throw new DfsException(DfsErrorCode.VolumeNotFound, $"Volume {id} is not known here yet.");
        }, ct, volumeMayLag: true);

    /// <summary>One install step at the new volume's leader.</summary>
    private Task<RestoreInstallResultMessage> InstallAsync(VolumeRecord target, WireRestorePhase phase, string source, StagingId staging,
        ReadOnlyMemory<byte> records, int count, CancellationToken ct)
    {
        var message = new RestoreInstallMessage(target.Id, source, phase, staging, records, count);
        return RetryRestoreStepAsync("restore install " + phase.ToString().ToLowerInvariant(), async token =>
        {
            VolumeRecord current = _s.Control.State.FindVolume(target.Id) ?? target;
            if (_s.Volumes.TryGet(target.Id)?.Service is { IsLeader: true } local)
                return await local.RestoreInstallCoreAsync(message, token);
            return (RestoreInstallResultMessage)await CallGroupLeaderAsync(current, message, token);
        }, ct, volumeMayLag: true);
    }

    /// <summary>
    /// Sends a request to the leader of another volume's group: the locally known leader first, then each voter (following a
    /// NotLeader hint when it names a member).
    /// </summary>
    private async Task<IWireMessage> CallGroupLeaderAsync(VolumeRecord record, IWireMessage message, CancellationToken ct)
    {
        var candidates = new List<NodeId>();
        if (_s.Raft.LeaderOf(record.GroupName) is { } known)
            candidates.Add(known);
        candidates.AddRange(record.Voters);
        var tried = new HashSet<NodeId>();
        Exception? last = null;
        for (int i = 0; i < candidates.Count; i++)
        {
            NodeId node = candidates[i];
            if (node == _s.NodeId || !tried.Add(node))
                continue;
            try
            {
                return await _s.Peers.CallAsync(node, message, ct: ct);
            }
            catch (DfsUnavailableException ex) when (ex.ErrorCode == DfsErrorCode.NotLeader)
            {
                last = ex;
                if (ex.LeaderHint is { } hint && MemberAt(hint) is { } hinted && !tried.Contains(hinted))
                    candidates.Insert(i + 1, hinted);
            }
            catch (Exception ex) when (!ct.IsCancellationRequested && IsTransientRestoreFailure(ex, volumeMayLag: true))
            {
                last = ex;
            }
        }

        throw last as DfsException ?? new DfsUnavailableException(DfsErrorCode.NotLeader, $"No leader of volume '{record.Name}' answered: {last?.Message}", last);
    }

    private NodeId? MemberAt(string endpoint)
    {
        foreach (MemberRecord m in _s.Control.State.Members.Values)
        {
            if (m.IsLive && m.Endpoints.Contains(endpoint, StringComparer.Ordinal))
                return m.Id;
        }

        return null;
    }

    /// <summary>
    /// Copies every object the snapshot's namespace references to a new id owned by <paramref name="target"/>, bound to the
    /// restore staging session, and returns the namespace with its extents rewritten to the copies. Holders inside the target
    /// pool copy locally (<c>CopyObjects</c>, no data transfer); an object whose copies do not meet its policy's durable copies
    /// and zones is read (verified) and stored on the target pool's placement targets instead.
    /// </summary>
    private async Task<IReadOnlyList<InodeRecord>> CopyRestoreObjectsAsync(RestoreIntentRecord intent, VolumeRecord target, StagingId staging,
        CancellationToken ct)
    {
        IReadOnlyList<InodeRecord> tree = NamespaceTree(intent.Root);
        ControlState control = _s.Control.State;
        ulong epoch = control.PlacementEpoch;
        var objects = new SortedDictionary<ObjectId, RestoreObject>(ObjectIdComparer.Instance);
        foreach (InodeRecord inode in tree)
        {
            (DfsStoragePolicy policy, string pool) = PolicyOf(target, inode.PolicyName);
            foreach (StreamRecord stream in inode.Streams.Values)
            {
                if (stream.IsInline)
                    continue;
                foreach (ExtentRef extent in stream.Extents)
                {
                    if (!objects.TryGetValue(extent.Object, out RestoreObject? o))
                    {
                        o = new RestoreObject(extent, RestoreObjectId(target.Id, extent.Object), policy, pool);
                        objects[extent.Object] = o;
                    }
                    else if (Stronger(policy, o.Policy))
                    {
                        o.Policy = policy;
                        o.Pool = pool;
                    }
                }
            }
        }

        // Server-side copies on the holders that belong to the object's target pool.
        var perHolder = new SortedDictionary<NodeId, List<RestoreObject>>(NodeIdComparer.Instance);
        foreach (RestoreObject o in objects.Values)
        {
            HashSet<NodeId> members = [.. control.PoolMembers(o.Pool).Where(m => m.IsLive).Select(m => m.Id)];
            foreach (NodeId holder in LocationsOf(o.Source))
            {
                if (!members.Contains(holder))
                    continue;
                if (!perHolder.TryGetValue(holder, out List<RestoreObject>? list))
                    perHolder[holder] = list = [];
                list.Add(o);
            }
        }

        foreach ((NodeId holder, List<RestoreObject> list) in perHolder)
        {
            foreach (RestoreObject[] batch in list.Chunk(RestoreCopyBatch))
            {
                try
                {
                    IReadOnlyList<ObjectReceipt> receipts = await RetryRestoreStepAsync("copying restored objects",
                        token => CopyOnHolderAsync(holder, target, staging, epoch, batch, token), ct, volumeMayLag: true);
                    foreach (ObjectReceipt receipt in receipts)
                    {
                        foreach (RestoreObject o in batch)
                        {
                            if (o.Target == receipt.Object)
                                o.Receipts.Add(receipt);
                        }
                    }
                }
                catch (Exception ex) when (!ct.IsCancellationRequested && ex is DfsException or IOException or TimeoutException or UnauthorizedAccessException)
                {
                    // The copies this holder could not make are re-placed below.
                    _s.Debug("volume", NodeServices.Inv($"Volume {Name}: CopyObjects on {_s.Control.NameOf(holder)} failed: {ex.Message}"));
                }
            }
        }

        foreach (RestoreObject o in objects.Values)
        {
            ExtentRef e = o.Source;
            ReceiptEvaluation copies = Receipts.Evaluate(o.Receipts, _s.Keys.ReceiptKey, _s.Control.State, o.Target, e.Checksum, e.ObjectLength, e.Encoding,
                target.Id, staging, o.Pool);
            if (copies.Copies > 0 && copies.Copies >= o.Policy.DurableCopiesBeforeAck && copies.Zones >= o.Policy.DurableZonesBeforeAck)
            {
                o.Holders = [.. copies.Holders];
                continue;
            }

            byte[] data = await Io.ReadObjectAsync(Record, e.Object, e.Checksum, e.ObjectLength, LocationsOf(e), Record.StoragePool, ct);
            StoredObject stored = await RetryRestoreStepAsync("re-placing a restored object",
                token => Io.StoreObjectAsync(target, data, staging, o.Policy, o.Pool, token, o.Target, DfsOperationKind.RestoreSnapshot), ct, volumeMayLag: true);
            o.Holders = stored.Holders;
        }

        var result = new List<InodeRecord>(tree.Count);
        foreach (InodeRecord inode in tree)
        {
            if (inode.Streams.Values.All(s => s.IsInline || s.Extents.IsEmpty))
            {
                result.Add(inode);
                continue;
            }

            var streams = ImmutableSortedDictionary.CreateBuilder<string, StreamRecord>(StringComparer.Ordinal);
            foreach (KeyValuePair<string, StreamRecord> pair in inode.Streams)
            {
                StreamRecord stream = pair.Value;
                if (!stream.IsInline && !stream.Extents.IsEmpty)
                {
                    stream = stream with
                    {
                        Extents = [.. stream.Extents.Select(x =>
                        {
                            RestoreObject o = objects[x.Object];
                            return x with { Object = o.Target, Staging = staging, PlacementEpoch = epoch, Locations = o.Holders };
                        })],
                    };
                }

                streams[pair.Key] = stream;
            }

            result.Add(inode with { Streams = streams.ToImmutable() });
        }

        return result;
    }

    /// <summary>One <c>CopyObjects</c> batch on a holder (the local store directly when this node is the holder).</summary>
    private async Task<IReadOnlyList<ObjectReceipt>> CopyOnHolderAsync(NodeId holder, VolumeRecord target, StagingId staging, ulong epoch,
        IReadOnlyList<RestoreObject> batch, CancellationToken ct)
    {
        if (holder != _s.NodeId)
        {
            var message = new CopyObjectsMessage(Record.Id, target.Id, staging, epoch, [.. batch.Select(o => new ObjectCopy(o.Source.Object, o.Target))]);
            var reply = (CopyObjectsResultMessage)await _s.Peers.CallAsync(holder, message, ct: ct);
            return reply.Receipts;
        }

        VolumeRecord current = _s.Control.State.FindVolume(target.Id)
            ?? throw new DfsException(DfsErrorCode.VolumeNotFound, $"Volume {target.Id} is not known here yet.");
        var receipts = new List<ObjectReceipt>();
        foreach (RestoreObject o in batch)
        {
            if (_s.ObjectStore.TryGetInfo(o.Source.Object) is not { } info || info.Volume != Record.Id)
                continue;
            var data = new byte[info.Length];
            int read = await _s.ObjectStore.ReadAsync(o.Source.Object, 0, data, ct);
            if (read != data.Length || ObjectChecksum.Compute(data) != info.Checksum)
                continue;
            (ObjectReceipt receipt, _) = await _s.Objects.StoreAsync(o.Target, current, staging, info.Checksum, info.Length, ObjectEncodings.Plain, epoch, data,
                DfsOperationKind.RestoreSnapshot, ct);
            receipts.Add(receipt);
        }

        return receipts;
    }

    // ------------------------------------------------------------------------------------------------- target side

    /// <summary><c>RestoreInstall</c> (node peers): one install step of a restore into this (restoring) volume.</summary>
    public async Task<IWireMessage> RestoreInstallAsync(RpcContext context, RestoreInstallMessage request)
        => await RestoreInstallCoreAsync(request, context.Cancellation);

    internal async Task<RestoreInstallResultMessage> RestoreInstallCoreAsync(RestoreInstallMessage request, CancellationToken ct)
    {
        await EnsureLeaderReadyAsync(ct);
        await ReadBarrierAsync(ct);
        if (string.IsNullOrEmpty(request.Source))
            throw new ArgumentException("A restore needs a source.");
        bool same = State.Restore is { } current && string.Equals(current.Source, request.Source, StringComparison.Ordinal);
        if (Record.State != WireVolumeState.Restoring && !same)
            throw new ArgumentException($"Volume '{Name}' is not being restored.");
        VolumeCommand? command = request.Phase switch
        {
            WireRestorePhase.Begin => same ? null : new RestoreChunkCommand(request.Source, 0, []),
            WireRestorePhase.Stage => new RestoreChunkCommand(request.Source, request.Staging.Value, DecodeRestoreRecords(request.Inodes, request.InodeCount)),
            WireRestorePhase.Complete => new RestoreCompleteCommand(request.Source, request.InodeCount),
            _ => throw new ArgumentException($"Unknown restore phase {request.Phase}."),
        };
        RestoreRecord record;
        if (command is null)
        {
            record = State.Restore!;
        }
        else
        {
            VolumeApplyResult result = await ProposeAsync(Stamp(command, null), ct);
            if (!result.IsSuccess)
                throw DfsErrors.ToException(result.Code, result.Message);
            record = result.Value as RestoreRecord ?? throw new DfsException(DfsErrorCode.Internal, "The restore step produced no restore record.");
        }

        return new RestoreInstallResultMessage(new StagingId(record.Staging), record.Complete ? State.Inodes.Count : record.Staged.Count,
            record.Complete ? State.TokenAt(record.CompletedIndex) : CommitToken.None);
    }

    // ------------------------------------------------------------------------------------------------- helpers

    /// <summary>The inodes of the namespace tree under the root (detached inodes held only by open handles are not restored).</summary>
    private static IReadOnlyList<InodeRecord> NamespaceTree(ImmutableSortedDictionary<ulong, InodeRecord> root)
    {
        var result = new SortedDictionary<ulong, InodeRecord>();
        var pending = new Stack<ulong>();
        pending.Push(InodeRecord.RootId);
        while (pending.Count > 0)
        {
            ulong id = pending.Pop();
            if (result.ContainsKey(id) || !root.TryGetValue(id, out InodeRecord? inode))
                continue;
            result[id] = inode;
            foreach (ulong child in inode.Children.Values)
                pending.Push(child);
        }

        return [.. result.Values];
    }

    internal static (DfsStoragePolicy Policy, string Pool) PolicyOf(VolumeRecord volume, string? name)
    {
        DfsStoragePolicy policy = name is { Length: > 0 } && volume.NamedPolicies.TryGetValue(name, out DfsStoragePolicy? named) ? named : volume.DefaultPolicy;
        return (policy, policy.Pool ?? volume.StoragePool);
    }

    internal static bool Stronger(DfsStoragePolicy candidate, DfsStoragePolicy current)
        => (candidate.DurableCopiesBeforeAck, candidate.DurableZonesBeforeAck, candidate.TargetCopies)
            .CompareTo((current.DurableCopiesBeforeAck, current.DurableZonesBeforeAck, current.TargetCopies)) > 0;

    /// <summary>Encodes the restored inodes into stage requests (format byte, then the records).</summary>
    internal static List<(ReadOnlyMemory<byte> Records, int Count)> EncodeRestoreChunks(IReadOnlyList<InodeRecord> inodes)
    {
        var chunks = new List<(ReadOnlyMemory<byte>, int)>();
        var buffer = new ArrayBufferWriter<byte>();
        int count = 0;
        foreach (InodeRecord inode in inodes)
        {
            var w = new WireWriter(buffer);
            if (count == 0)
                w.WriteByte(RestoreRecordsFormat);
            StateCodec.WriteInode(ref w, inode);
            count++;
            if (buffer.WrittenCount >= RestoreChunkBytes || count >= RestoreChunkInodes)
            {
                chunks.Add((buffer.WrittenMemory.ToArray(), count));
                buffer.Clear();
                count = 0;
            }
        }

        if (count > 0)
            chunks.Add((buffer.WrittenMemory.ToArray(), count));
        return chunks;
    }

    /// <summary>Decodes the inode records of a stage request (malformed → InvalidArgument).</summary>
    private static ImmutableArray<InodeRecord> DecodeRestoreRecords(ReadOnlyMemory<byte> records, int count)
    {
        if (count <= 0 || count > RestoreChunkInodes)
            throw new ArgumentException($"A restore stage carries 1..{RestoreChunkInodes} inode records, not {count}.");
        try
        {
            var r = new WireReader(records.Span, WireLimits.Default);
            byte format = r.ReadByte();
            if (format != RestoreRecordsFormat)
                throw new ArgumentException($"Unsupported restore record format {format}.");
            var builder = ImmutableArray.CreateBuilder<InodeRecord>(count);
            for (int i = 0; i < count; i++)
                builder.Add(StateCodec.ReadInode(ref r));
            r.EnsureEnd();
            return builder.MoveToImmutable();
        }
        catch (DfsProtocolException ex)
        {
            throw new ArgumentException("Malformed restore inode records: " + ex.Message, ex);
        }
    }

    /// <summary>The source tag binding every install step of one restore.</summary>
    private string RestoreSourceOf(RestoreIntentRecord intent)
        => NodeServices.Inv($"snapshot {intent.Snapshot} ({intent.SnapshotId}) of volume {Record.Id}, operation {intent.Operation}");

    /// <summary>Deterministic id of a control step of a restore (dedup across retries, crashes and leaders).</summary>
    private UInt128 RestoreRequestId(RestoreIntentRecord intent, WireRestoreAction action)
        => HashId("tdfs.restore.control", Record.Id.Value, intent.Operation.Session, intent.Operation.Sequence, (ulong)action);

    /// <summary>Deterministic id of the copy of <paramref name="source"/> owned by <paramref name="target"/>.</summary>
    internal static ObjectId RestoreObjectId(VolumeId target, ObjectId source)
        => new(HashId("tdfs.restore.object", target.Value, (ulong)(source.Value >> 64), (ulong)source.Value, 0));

    private static UInt128 HashId(string purpose, Guid scope, ulong a, ulong b, ulong c)
    {
        Span<byte> input = stackalloc byte[64 + 16 + 24];
        int length = System.Text.Encoding.UTF8.GetBytes(purpose, input);
        scope.TryWriteBytes(input[length..]);
        length += 16;
        BinaryPrimitives.WriteUInt64LittleEndian(input[length..], a);
        BinaryPrimitives.WriteUInt64LittleEndian(input[(length + 8)..], b);
        BinaryPrimitives.WriteUInt64LittleEndian(input[(length + 16)..], c);
        length += 24;
        Span<byte> hash = stackalloc byte[32];
        SHA256.HashData(input[..length], hash);
        UInt128 value = BinaryPrimitives.ReadUInt128LittleEndian(hash);
        return value == UInt128.Zero ? UInt128.One : value;
    }

    /// <summary>Where a run got to (the volume to abandon when it fails definitively).</summary>
    private sealed class RestoreProgress
    {
        public VolumeId? Target { get; set; }
    }

    /// <summary>One object of a restore: its source extent (identity, checksum, length, holders), its copy and the policy it must meet.</summary>
    private sealed class RestoreObject(ExtentRef source, ObjectId target, DfsStoragePolicy policy, string pool)
    {
        public ExtentRef Source { get; } = source;

        public ObjectId Target { get; } = target;

        public DfsStoragePolicy Policy { get; set; } = policy;

        public string Pool { get; set; } = pool;

        public List<ObjectReceipt> Receipts { get; } = [];

        public ImmutableArray<NodeId> Holders { get; set; } = [];
    }
}
