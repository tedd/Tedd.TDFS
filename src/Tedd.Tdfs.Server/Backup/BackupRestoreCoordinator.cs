using System.Buffers.Binary;
using System.Collections.Immutable;
using System.Security.Cryptography;
using System.Text;
using Tedd.Tdfs.Data;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Control;
using Tedd.Tdfs.Server.Raft;
using Tedd.Tdfs.Server.Runtime;
using Tedd.Tdfs.Server.Security;
using Tedd.Tdfs.Server.Volumes.Service;
using Tedd.Tdfs.Server.Volumes.State;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Server.Backup;

/// <summary>
/// Restore from a backup host into a new volume (<c>AdminRequest(RestoreFromBackup)</c>, FSD §12.5, PROTOCOL.md §8.6), run by the
/// control leader of the restoring cluster — which may be an independent recovery cluster that never contacts a source node:
/// <list type="number">
/// <item><description>authenticate to the backup host with the request's reader credentials and pin the published snapshot
/// (<c>BackupExportRestore(Describe)</c>; recovery key material unlocks a host that lost its own, RST-005);</description></item>
/// <item><description>read and verify the manifest, then create the volume in the <see cref="WireVolumeState.Restoring"/> state with a new
/// identity and the source's configuration (never served while incomplete);</description></item>
/// <item><description>copy every object of the closure from the backup host (verified) to new ids owned by the new volume, placed
/// with receipts meeting its policies, rewrite the extents and install the namespace atomically (<c>RestoreInstall</c>);</description></item>
/// <item><description>activate the volume (new authority, its own epoch) and release the backup pin.</description></item>
/// </list>
/// Every step is idempotent (request ids derived from the restore source, deterministic object ids, the install's source tag), so
/// a retried request — after a crash or a control leader change — resumes the restoring volume it created instead of creating
/// another. Validation failures and missing key material fail before anything is created.
/// </summary>
internal sealed class BackupRestoreCoordinator
{
    private const int ObjectChunk = 4 << 20;
    private const int CopyConcurrency = 4;

    private readonly NodeServices _s;
    private readonly Dictionary<string, Run> _runs = new(StringComparer.Ordinal);

    public BackupRestoreCoordinator(NodeServices services)
    {
        ArgumentNullException.ThrowIfNull(services);
        _s = services;
    }

    private TimeSpan CallTimeout
    {
        get
        {
            TimeSpan t = _s.Timings.Tick * _s.Timings.ElectionTicksMax * 10;
            return t < TimeSpan.FromSeconds(2) ? TimeSpan.FromSeconds(2) : t;
        }
    }

    private TimeSpan StepTimeout
        => TimeSpan.FromTicks(Math.Max(TimeSpan.FromSeconds(10).Ticks, _s.Timings.Tick.Ticks * _s.Timings.ElectionTicksMax * 20));

    /// <summary>The <c>AdminOp.RestoreFromBackup</c> handler (ClusterAdmin, control leader).</summary>
    public async ValueTask<AdminResultMessage> HandleAsync(RpcContext context, AdminRequestMessage request)
    {
        string principal = context.Principal ?? string.Empty;
        ControlHost control = _s.Control;
        if (!control.IsLeader)
            throw control.NotLeader();
        if (!control.State.IsClusterAdmin(principal))
            throw new UnauthorizedAccessException($"Principal '{principal}' must be a cluster administrator to restore from a backup host.");
        AdminRestoreFromBackupBody body = request.ReadBody<AdminRestoreFromBackupBody>();
        Validate(body);
        string identity = string.Join('\n', [.. body.BackupEndpoints, body.SourceVolume, body.SnapshotName, body.BackupCredentials.Principal]);
        if (_runs.TryGetValue(body.NewVolumeName, out Run? run) && !run.Task.IsCompleted)
        {
            if (!string.Equals(run.Identity, identity, StringComparison.Ordinal))
                throw new DfsAlreadyExistsException($"Volume '{body.NewVolumeName}' is already being restored from another source.");
        }
        else
        {
            run = new Run(identity, RunAsync(principal, body));
            _runs[body.NewVolumeName] = run;
            _ = ForgetWhenDoneAsync(body.NewVolumeName, run);
        }

        AdminVolumeInfoBody result = await run.Task.WaitAsync(context.Cancellation);
        return AdminResultMessage.Create(request.Op, result);
    }

    private static void Validate(AdminRestoreFromBackupBody body)
    {
        if (body.BackupEndpoints.Count == 0 || body.BackupEndpoints.Any(string.IsNullOrEmpty))
            throw new ArgumentException("At least one non-empty backup endpoint is required.");
        if (body.BackupCredentials is null || string.IsNullOrEmpty(body.BackupCredentials.Principal))
            throw new ArgumentException("Backup credentials are required.");
        if (string.IsNullOrEmpty(body.SourceVolume) || string.IsNullOrEmpty(body.SnapshotName))
            throw new ArgumentException("The source volume and snapshot must be named.");
        if (ControlStateMachine.GetVolumeNameError(body.NewVolumeName) is { } error)
            throw new ArgumentException(error);
    }

    private async Task ForgetWhenDoneAsync(string name, Run run)
    {
        try
        {
            await run.Task;
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            _s.Debug("restore", NodeServices.Inv($"Restore of volume '{name}' from a backup host ended: {ex.GetType().Name}: {ex.Message}"));
        }
        finally
        {
            if (_runs.TryGetValue(name, out Run? current) && ReferenceEquals(current, run))
                _runs.Remove(name);
        }
    }

    // ------------------------------------------------------------------------------------------------- the restore

    private async Task<AdminVolumeInfoBody> RunAsync(string principal, AdminRestoreFromBackupBody body)
    {
        using CancellationTokenSource lifetime = new();
        CancellationToken ct = lifetime.Token;
        ExportSession session = await OpenAsync(body, ct);
        try
        {
            return await RestoreAsync(principal, body, session, ct);
        }
        finally
        {
            await session.ReleaseAsync(CallTimeout);
        }
    }

    private async Task<AdminVolumeInfoBody> RestoreAsync(string principal, AdminRestoreFromBackupBody body, ExportSession session, CancellationToken ct)
    {
        ControlHost control = _s.Control;
        if (!control.IsLeader)
            throw control.NotLeader();
        await control.Group!.ReadIndexAsync(ct);
        string source = SourceTag(session.Descriptor, body.SourceVolume, body.SnapshotName);
        ControlState state = control.State;
        VolumeRecord? existing = state.FindVolumeByName(body.NewVolumeName);
        VolumeRecord target;
        if (existing is not null)
        {
            if (!CreatedBy(state, source, body.NewVolumeName, existing.Id))
                throw new DfsAlreadyExistsException($"Volume '{body.NewVolumeName}' already exists.");
            target = existing;
            _s.Info("restore", NodeServices.Inv($"Resuming the restore of volume '{target.Name}' ({target.Id}) from snapshot '{body.SnapshotName}' of a backup host."));
        }
        else
        {
            target = await CreateAsync(principal, body, session, source, ct);
        }

        try
        {
            RestoreInstallResultMessage opened = await InstallAsync(target, WireRestorePhase.Begin, source, default, ReadOnlyMemory<byte>.Empty, 0, ct);
            CommitToken installed = opened.Token;
            if (installed.IsNone)
            {
                IReadOnlyList<InodeRecord> inodes = await CopyObjectsAsync(session, target, opened.Staging, ct);
                foreach ((ReadOnlyMemory<byte> records, int count) in VolumeService.EncodeRestoreChunks(inodes))
                    await InstallAsync(target, WireRestorePhase.Stage, source, opened.Staging, records, count, ct);
                RestoreInstallResultMessage done = await InstallAsync(target, WireRestorePhase.Complete, source, opened.Staging, ReadOnlyMemory<byte>.Empty,
                    inodes.Count, ct);
                installed = done.Token;
            }

            VolumeRecord active = await ActivateAsync(principal, target, ct);
            _s.Info("restore", NodeServices.Inv($"Restored snapshot '{body.SnapshotName}' of volume '{body.SourceVolume}' from a backup host into volume '{active.Name}' ({active.Id})."));
            return new AdminVolumeInfoBody(control.ToVolumeEntry(active), installed);
        }
        catch (Exception ex) when (!ct.IsCancellationRequested && IsDefinite(ex))
        {
            _s.Warn("restore", NodeServices.Inv($"The restore into volume '{target.Name}' failed definitively and is abandoned: {ex.Message}"));
            await AbandonAsync(principal, target);
            throw;
        }
    }

    /// <summary>Creates the restoring volume (new identity, the source's configuration bound to an existing pool).</summary>
    private async Task<VolumeRecord> CreateAsync(string principal, AdminRestoreFromBackupBody body, ExportSession session, string source,
        CancellationToken ct)
    {
        ControlHost control = _s.Control;
        ControlState state = control.State;
        BackupVolumeConfig config = session.Manifest.Config;
        string pool = state.PoolExists(config.StoragePool) ? config.StoragePool : DfsPoolNames.Default;
        if (ControlStateMachine.CheckPolicy(state, config.DefaultPolicy, pool) is { } policyError)
            throw new DfsException(policyError.Code, policyError.Message);
        var named = new SortedDictionary<string, DfsStoragePolicy>(StringComparer.Ordinal);
        foreach (KeyValuePair<string, DfsStoragePolicy> pair in config.NamedPolicies)
        {
            if (ControlStateMachine.CheckPolicy(state, pair.Value, pool) is { } namedError)
                throw new DfsException(namedError.Code, $"Policy '{pair.Key}': {namedError.Message}");
            named[pair.Key] = pair.Value;
        }

        UInt128 requestId = NextCreateId(state, source, body.NewVolumeName);
        var id = VolumeId.New(_s.Random);
        IReadOnlyList<NodeId> voters = MembershipManager.ChooseVolumeVoters(state, id, config.Metadata, pool);
        var creation = new VolumeCreation(id, body.NewVolumeName, 1, principal, _s.Time.GetUtcNow(), 0, config.Metadata, config.DefaultPolicy, named,
            config.InlineThreshold, config.Limits, pool, null, config.EnforceTraverseChecking, voters);
        byte[] dataKey = DataKeys.NewDataKey(_s.Random);
        byte[] wrapped = DataKeys.Wrap(_s.Keys.StorageKek, id, 1, dataKey, _s.Random);
        CryptographicOperations.ZeroMemory(dataKey);
        ApplyOutcome outcome = await control.ProposeAsync(new CreateRestoringVolumeCommand(control.NewHeader(requestId, principal), creation, wrapped), ct);
        if (!outcome.IsSuccess)
            throw outcome.ToException();
        VolumeRecord record = outcome.Value as VolumeRecord ?? control.State.FindVolume(id)
            ?? throw new DfsException(DfsErrorCode.Internal, "The restoring volume is missing from the control state.");
        _s.Info("restore", NodeServices.Inv($"Created volume {record.Name} ({record.Id}) to restore snapshot '{body.SnapshotName}' from a backup host; it is not served until the restore completes."));
        return record;
    }

    private async Task<VolumeRecord> ActivateAsync(string principal, VolumeRecord target, CancellationToken ct)
    {
        ControlHost control = _s.Control;
        if (control.State.FindVolume(target.Id) is { State: WireVolumeState.Active } already)
            return already;
        await _s.HitAsync(FaultStage.F21, FaultSites.Reconfiguration, target.Name, DfsOperationKind.RestoreSnapshot, ct: ct);
        if (!control.IsLeader)
            throw control.NotLeader();
        ApplyOutcome outcome = await control.ProposeAsync(
            new FinishRestoringVolumeCommand(control.NewHeader(HashId("tdfs.backup-restore.activate", target.Id.Value.ToString("N")), principal), target.Id, true),
            ct);
        if (!outcome.IsSuccess)
            throw outcome.ToException();
        return outcome.Value as VolumeRecord ?? control.State.FindVolume(target.Id)
            ?? throw new DfsException(DfsErrorCode.VolumeNotFound, $"Volume '{target.Name}' disappeared before it was activated.");
    }

    private async Task AbandonAsync(string principal, VolumeRecord target)
    {
        ControlHost control = _s.Control;
        if (!control.IsLeader || control.State.FindVolume(target.Id) is not { State: WireVolumeState.Restoring })
            return;
        using CancellationTokenSource deadline = _s.CreateDeadline();
        try
        {
            await control.ProposeAsync(new FinishRestoringVolumeCommand(
                control.NewHeader(HashId("tdfs.backup-restore.abandon", target.Id.Value.ToString("N")), principal), target.Id, false), deadline.Token);
        }
        catch (Exception ex) when (ex is DfsException or OperationCanceledException or IOException)
        {
            _s.Warn("restore", NodeServices.Inv($"Abandoning the restoring volume '{target.Name}' failed: {ex.Message}"));
        }
    }

    /// <summary>
    /// Copies every closure object from the backup host to a new id owned by <paramref name="target"/> (verified on receipt, placed
    /// with receipts meeting the strongest policy referencing it) and returns the namespace with rewritten extents.
    /// </summary>
    private async Task<IReadOnlyList<InodeRecord>> CopyObjectsAsync(ExportSession session, VolumeRecord target, StagingId staging, CancellationToken ct)
    {
        BackupManifest manifest = session.Manifest;
        ulong epoch = _s.Control.State.PlacementEpoch;
        var plan = new SortedDictionary<ObjectId, (DfsStoragePolicy Policy, string Pool)>(BackupManifest.ObjectIdOrder.Instance);
        foreach (InodeRecord inode in manifest.Inodes)
        {
            (DfsStoragePolicy policy, string pool) = VolumeService.PolicyOf(target, inode.PolicyName);
            foreach (StreamRecord stream in inode.Streams.Values)
            {
                if (stream.IsInline)
                    continue;
                foreach (ExtentRef e in stream.Extents)
                {
                    if (!plan.TryGetValue(e.Object, out (DfsStoragePolicy Policy, string Pool) current) || VolumeService.Stronger(policy, current.Policy))
                        plan[e.Object] = (policy, pool);
                }
            }
        }

        var io = new ObjectIo(_s);
        var holders = new Dictionary<ObjectId, ImmutableArray<NodeId>>();
        var running = new List<Task>();
        foreach (BackupObjectRef obj in manifest.Closure)
        {
            while (running.Count >= CopyConcurrency)
            {
                Task done = await Task.WhenAny(running);
                running.Remove(done);
                await done;
            }

            (DfsStoragePolicy policy, string pool) = plan[obj.Id];
            running.Add(CopyOneAsync(session, io, target, staging, obj, policy, pool, holders, ct));
        }

        await Task.WhenAll(running);
        var result = new List<InodeRecord>(manifest.Inodes.Count);
        foreach (InodeRecord inode in manifest.Inodes)
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
                        Extents = [.. stream.Extents.Select(x => x with
                        {
                            Object = VolumeService.RestoreObjectId(target.Id, x.Object),
                            Staging = staging,
                            PlacementEpoch = epoch,
                            Locations = holders[x.Object],
                        })],
                    };
                }

                streams[pair.Key] = stream;
            }

            result.Add(inode with { Streams = streams.ToImmutable() });
        }

        return result;
    }

    private async Task CopyOneAsync(ExportSession session, ObjectIo io, VolumeRecord target, StagingId staging, BackupObjectRef obj, DfsStoragePolicy policy,
        string pool, Dictionary<ObjectId, ImmutableArray<NodeId>> holders, CancellationToken ct)
    {
        byte[] data = await session.ReadObjectAsync(obj, ct);
        ObjectId copy = VolumeService.RestoreObjectId(target.Id, obj.Id);
        StoredObject stored = await RetryAsync("storing a restored object", async token =>
        {
            VolumeRecord current = _s.Control.State.FindVolume(target.Id) ?? target;
            return await io.StoreObjectAsync(current, data, staging, policy, pool, token, copy, DfsOperationKind.RestoreSnapshot);
        }, ct);
        holders[obj.Id] = stored.Holders;
    }

    // ------------------------------------------------------------------------------------------------- install steps

    /// <summary>One install step at the restoring volume's leader (locally when this node leads it).</summary>
    private Task<RestoreInstallResultMessage> InstallAsync(VolumeRecord target, WireRestorePhase phase, string source, StagingId staging,
        ReadOnlyMemory<byte> records, int count, CancellationToken ct)
    {
        var message = new RestoreInstallMessage(target.Id, source, phase, staging, records, count);
        return RetryAsync("restore install " + phase.ToString().ToLowerInvariant(), async token =>
        {
            if (_s.Volumes.TryGet(target.Id)?.Service is { IsLeader: true } local)
                return await local.RestoreInstallCoreAsync(message, token);
            VolumeRecord current = _s.Control.State.FindVolume(target.Id)
                ?? throw new DfsException(DfsErrorCode.VolumeNotFound, $"Volume '{target.Name}' no longer exists.");
            return (RestoreInstallResultMessage)await CallGroupLeaderAsync(current, message, token);
        }, ct);
    }

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
            catch (Exception ex) when (!ct.IsCancellationRequested && IsTransient(ex))
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

    /// <summary>Runs <paramref name="call"/>, retrying transient failures with backoff for at most the step timeout.</summary>
    private async Task<T> RetryAsync<T>(string what, Func<CancellationToken, Task<T>> call, CancellationToken ct)
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
            catch (Exception ex) when (!ct.IsCancellationRequested && IsTransient(ex))
            {
                if (_s.Time.GetElapsedTime(started) > StepTimeout)
                    throw new DfsUnavailableException(DfsErrorCode.Timeout, $"{what} did not succeed within {StepTimeout}: {ex.Message}", ex);
                await Task.Delay(delay, _s.Time, ct);
                delay = delay * 2 > maxDelay ? maxDelay : delay * 2;
            }
        }
    }

    private static bool IsTransient(Exception ex) => ex switch
    {
        DfsException { ErrorCode: DfsErrorCode.NotLeader or DfsErrorCode.Unavailable or DfsErrorCode.QuorumUnavailable or DfsErrorCode.NodeUnavailable
            or DfsErrorCode.Timeout or DfsErrorCode.StaleIncarnation or DfsErrorCode.VolumeNotFound } => true,
        DfsException => false,
        TimeoutException or IOException or RpcConnectionException or NodeLoopStoppedException => true,
        _ => false,
    };

    /// <summary>Failures that end the restore (its volume is removed); transient ones leave it for a resumed request.</summary>
    private static bool IsDefinite(Exception ex) => ex is not OperationCanceledException && !IsTransient(ex) && ex is DfsException or UnauthorizedAccessException
        or ArgumentException or DfsRetentionProtectedException;

    // ------------------------------------------------------------------------------------------------- identities

    /// <summary>The source tag binding every install step of one restore (independent of the request and the coordinator).</summary>
    private static string SourceTag(BackupRestoreDescriptor descriptor, string sourceVolume, string snapshotName)
        => NodeServices.Inv($"backup {descriptor.SourceClusterId:N}/{descriptor.SourceVolumeId} epoch {descriptor.SourceEpoch} snapshot {descriptor.Snapshot.Id} ({sourceVolume}/{snapshotName})");

    /// <summary>True when <paramref name="volume"/> was created by a restore of the same source into the same name.</summary>
    private static bool CreatedBy(ControlState state, string source, string name, VolumeId volume)
    {
        for (int generation = 0; generation < 64; generation++)
        {
            UInt128 id = CreateId(source, name, generation);
            if (!state.AdminOutcomes.TryGetValue(id, out AdminOutcome? outcome))
                return false;
            if (outcome.Code == DfsErrorCode.None && new VolumeId(outcome.RefGuid) == volume)
                return true;
        }

        return false;
    }

    /// <summary>The create request id of a new attempt (earlier generations created volumes that are gone).</summary>
    private static UInt128 NextCreateId(ControlState state, string source, string name)
    {
        for (int generation = 0; ; generation++)
        {
            UInt128 id = CreateId(source, name, generation);
            if (!state.AdminOutcomes.TryGetValue(id, out AdminOutcome? outcome) || (outcome.Code == DfsErrorCode.None && state.FindVolume(new VolumeId(outcome.RefGuid)) is not null))
                return id;
        }
    }

    private static UInt128 CreateId(string source, string name, int generation)
        => HashId("tdfs.backup-restore.create", source + "\0" + name + "\0" + generation.ToString(System.Globalization.CultureInfo.InvariantCulture));

    private static UInt128 HashId(string purpose, string value)
    {
        byte[] hash = SHA256.HashData(Encoding.UTF8.GetBytes(purpose + "\0" + value));
        UInt128 id = BinaryPrimitives.ReadUInt128LittleEndian(hash);
        return id == UInt128.Zero ? UInt128.One : id;
    }

    // ------------------------------------------------------------------------------------------------- backup host session

    private async Task<ExportSession> OpenAsync(AdminRestoreFromBackupBody body, CancellationToken ct)
    {
        IRpcTransport transport = _s.BackupExports?.Transport ?? throw new InvalidOperationException("The node has no transport for backup connections.");
        Exception? last = null;
        foreach (string endpoint in body.BackupEndpoints)
        {
            IRpcConnection? raw = null;
            using var deadline = new CancellationTokenSource(CallTimeout, _s.Time);
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, deadline.Token);
            try
            {
                raw = await transport.ConnectAsync(endpoint, linked.Token);
                HandshakeOutcome outcome = await HandshakeClient.AuthenticatePrincipalAsync(raw, Guid.Empty, _s.NodeName, body.BackupCredentials, _s.Random,
                    CallTimeout, PeerRole.Client, linked.Token);
                if (outcome.Challenge.ServerRole != PeerRole.Backup)
                    throw new ArgumentException($"The endpoint {endpoint} is not a backup host.");
                var session = new ExportSession(this, raw, body.SourceVolume, body.SnapshotName);
                raw = null;
                try
                {
                    await session.DescribeAsync(body.RecoveryKey, ct);
                    return session;
                }
                catch
                {
                    await session.ReleaseAsync(CallTimeout);
                    throw;
                }
            }
            catch (Exception ex) when (ex is DfsException dfs
                                           ? dfs.ErrorCode is DfsErrorCode.Unavailable or DfsErrorCode.NodeUnavailable or DfsErrorCode.Timeout
                                           : ex is RpcConnectionException or IOException or TimeoutException
                                             || (ex is OperationCanceledException && deadline.IsCancellationRequested && !ct.IsCancellationRequested))
            {
                last = ex;
            }
            finally
            {
                if (raw is not null)
                    await DisposeQuietlyAsync(raw);
            }
        }

        throw new DfsUnavailableException(DfsErrorCode.NodeUnavailable, $"No backup host endpoint could be reached: {last?.Message}", last);
    }

    private static async Task DisposeQuietlyAsync(IRpcConnection connection)
    {
        try
        {
            await connection.DisposeAsync();
        }
        catch (Exception ex) when (ex is IOException or ObjectDisposedException or RpcConnectionException)
        {
            // Closing anyway.
        }
    }

    /// <summary>A pinned published snapshot on a backup host and its verified manifest.</summary>
    private sealed class ExportSession(BackupRestoreCoordinator owner, IRpcConnection connection, string sourceVolume, string snapshotName)
    {
        private ulong _pin;
        private bool _released;

        public BackupRestoreDescriptor Descriptor { get; private set; } = null!;

        public BackupManifest Manifest { get; private set; } = null!;

        /// <summary>Pins the snapshot and reads its verified manifest.</summary>
        public async Task DescribeAsync(ReadOnlyMemory<byte> recoveryKey, CancellationToken ct)
        {
            BackupExportRestoreResultMessage described = await CallAsync(
                new BackupExportRestoreMessage(sourceVolume, snapshotName, BackupRestorePart.Describe, 0, default, 0, 0, recoveryKey), ct);
            _pin = described.ReaderPin;
            Descriptor = described.Descriptor ?? throw new DfsProtocolException("The backup host did not describe the snapshot.");
            if (Descriptor.ManifestLength <= 0 || Descriptor.ManifestLength > BackupReceiver.MaxManifestBytes)
                throw new DfsIntegrityException($"The backup host announced a manifest of {Descriptor.ManifestLength} bytes.");
            var manifest = new byte[Descriptor.ManifestLength];
            long offset = 0;
            while (offset < manifest.Length)
            {
                BackupExportRestoreResultMessage chunk = await CallAsync(
                    new BackupExportRestoreMessage(sourceVolume, snapshotName, BackupRestorePart.Manifest, _pin, default, offset, ObjectChunk, ReadOnlyMemory<byte>.Empty), ct);
                if (chunk.Offset != offset || chunk.Data.Length == 0 || offset + chunk.Data.Length > manifest.Length)
                    throw new DfsIntegrityException("The backup host returned an inconsistent manifest chunk.");
                chunk.Data.Span.CopyTo(manifest.AsSpan((int)offset));
                offset += chunk.Data.Length;
            }

            if (BackupManifest.Checksum(manifest) != Descriptor.ManifestChecksum)
                throw new DfsIntegrityException("The manifest read from the backup host does not match its checksum.");
            BackupManifest decoded;
            try
            {
                decoded = BackupManifest.Decode(manifest);
            }
            catch (DfsProtocolException ex) when (ex.ErrorCode != DfsErrorCode.Unsupported)
            {
                throw new DfsIntegrityException("The manifest read from the backup host is malformed: " + ex.Message, ex);
            }

            if (decoded.GetValidationError() is { } error)
                throw new DfsIntegrityException("The manifest read from the backup host is inconsistent: " + error + ".");
            if (decoded.SnapshotId != Descriptor.Snapshot.Id || decoded.VolumeId != Descriptor.SourceVolumeId)
                throw new DfsIntegrityException("The manifest read from the backup host describes another snapshot.");
            Manifest = decoded;
        }

        /// <summary>Reads one closure object (verified against the manifest's length and checksum).</summary>
        public async Task<byte[]> ReadObjectAsync(BackupObjectRef obj, CancellationToken ct)
        {
            var data = new byte[obj.Length];
            long offset = 0;
            while (true)
            {
                BackupExportRestoreResultMessage chunk = await CallAsync(
                    new BackupExportRestoreMessage(sourceVolume, snapshotName, BackupRestorePart.Object, _pin, obj.Id, offset, ObjectChunk, ReadOnlyMemory<byte>.Empty), ct);
                if (chunk.Offset != offset || chunk.TotalLength != obj.Length || offset + chunk.Data.Length > data.Length)
                    throw new DfsIntegrityException(obj.Id, $"The backup host returned an inconsistent range of object {obj.Id}.");
                chunk.Data.Span.CopyTo(data.AsSpan((int)offset));
                offset += chunk.Data.Length;
                if (chunk.Last || offset >= data.Length)
                    break;
                if (chunk.Data.Length == 0)
                    throw new DfsIntegrityException(obj.Id, $"The backup host made no progress reading object {obj.Id}.");
            }

            if (offset != obj.Length || ObjectChecksum.Compute(data) != obj.Checksum)
                throw new DfsIntegrityException(obj.Id, $"Object {obj.Id} read from the backup host failed verification.");
            return data;
        }

        /// <summary>Releases the pin (best effort) and closes the connection.</summary>
        public async Task ReleaseAsync(TimeSpan timeout)
        {
            if (_released)
                return;
            _released = true;
            if (_pin != 0)
            {
                try
                {
                    using var deadline = new CancellationTokenSource(timeout, owner._s.Time);
                    await CallAsync(new BackupExportRestoreMessage(sourceVolume, snapshotName, BackupRestorePart.Release, _pin, default, 0, 0,
                        ReadOnlyMemory<byte>.Empty), deadline.Token);
                }
                catch (Exception ex) when (ex is DfsException or IOException or TimeoutException or RpcConnectionException or OperationCanceledException
                                               or UnauthorizedAccessException)
                {
                    // The pin expires on the backup host on its own.
                }
            }

            await DisposeQuietlyAsync(connection);
        }

        private async Task<BackupExportRestoreResultMessage> CallAsync(BackupExportRestoreMessage message, CancellationToken ct)
        {
            ReadOnlyMemory<byte> reply = await connection.CallAsync(MessageCodec.Encode(message), owner.CallTimeout * 4, ct);
            IWireMessage decoded = MessageCodec.Decode(reply.Span);
            if (decoded is ErrorReplyMessage error)
                throw error.ToException();
            return decoded as BackupExportRestoreResultMessage
                ?? throw new DfsProtocolException($"The backup host answered BackupExportRestore with {decoded.Type}.");
        }
    }

    private sealed record Run(string Identity, Task<AdminVolumeInfoBody> Task);
}
