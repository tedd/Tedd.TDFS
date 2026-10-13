using System.Security.Cryptography;
using System.Text;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Backup;
using Tedd.Tdfs.Server.Control;
using Tedd.Tdfs.Server.Runtime;
using Tedd.Tdfs.Server.Volumes.Apply;
using Tedd.Tdfs.Server.Volumes.Commands;
using Tedd.Tdfs.Server.Volumes.State;

namespace Tedd.Tdfs.Server.Volumes.Service;

/// <summary>
/// Late-sync backup requests of the volume leader (FSD §12, ARCHITECTURE §12, PROTOCOL.md §8.6): target registration
/// (stored in the volume's control record through the control leader, <c>BackupTargetUpdate</c>), status, sync requests
/// (committed transfer pins) and bounded backup barriers. The exporter itself (<see cref="TargetExporter"/>) runs per target
/// on the leader and only changes volume state through committed entries.
/// </summary>
internal sealed partial class VolumeService
{
    private const int MaxTargetNameBytes = 255;
    private static readonly TimeSpan MaxAwait = TimeSpan.FromMinutes(10);

    /// <summary>Longest time a sync request waits for its transfer to start before it is answered.</summary>
    private static readonly TimeSpan RequestSyncStartWait = TimeSpan.FromSeconds(1);

    // ------------------------------------------------------------------------------------------------- exporter access

    /// <summary>The applied (committed) state of this replica, independent of any request's transaction view.</summary>
    internal VolumeState CommittedState => _sm.State;

    /// <summary>True while this node leads the group and applied every entry committed before its term.</summary>
    internal bool IsReadyLeader => IsLeader && _readyTerm == Group.Term;

    /// <summary>Establishes leader readiness (ReadIndex once per term) for background work.</summary>
    internal Task EnsureBackupLeaderReadyAsync(CancellationToken ct) => EnsureLeaderReadyAsync(ct);

    /// <summary>Proposes an exporter housekeeping entry (null when it could not be committed).</summary>
    internal Task<VolumeApplyResult?> ProposeBackupHousekeepingAsync(VolumeCommand command) => ProposeHousekeepingAsync(command);

    /// <summary>Waits until the local control replica reflects every control decision committed before the call.</summary>
    internal Task WaitForControlAsync(CancellationToken ct) => Barrier.WaitAsync(ct);

    /// <summary>Reads one object of an exported snapshot, verified against its checksum, from any holder (BAK-010).</summary>
    internal Task<byte[]> ReadObjectForBackupAsync(ExtentRef extent, CancellationToken ct)
        => Io.ReadObjectAsync(Record, extent.Object, extent.Checksum, extent.ObjectLength, LocationsOf(extent), Record.StoragePool, ct);

    /// <summary>Status of every registered target (volume status, BAK-001).</summary>
    internal IReadOnlyList<BackupStatusData> BackupStatuses()
    {
        VolumeExporter? exporter = _s.BackupExports?.For(this);
        DateTimeOffset now = _s.Time.GetUtcNow();
        return Record.BackupTargets.Keys.Order(StringComparer.Ordinal)
            .Select(t => TargetExporter.BuildStatus(_sm.State, t, exporter?.Target(t), now))
            .ToArray();
    }

    // ------------------------------------------------------------------------------------------------- BackupRegister

    /// <summary>
    /// <c>BackupRegister</c>: <see cref="BackupTargetAction.List"/> reads the registered targets; register, configure and remove
    /// require BackupAdmin, wrap the export secret with the cluster key-encryption key and are committed to the volume's control
    /// record by the control leader (deduplicated by a request id derived from the operation id). Removing a target also releases
    /// its transfer pins and drops its progress; the target keeps its published snapshots under its own retention.
    /// </summary>
    public async Task<IWireMessage> BackupRegisterAsync(RpcContext context, BackupRegisterMessage request)
    {
        RejectTransaction(request);
        string principal = context.Principal ?? string.Empty;
        CancellationToken ct = context.Cancellation;
        if (request.Action == BackupTargetAction.List)
        {
            if (GrantsOf(principal) == DfsPermissions.None && !_s.Control.State.IsClusterAdmin(principal))
                throw new UnauthorizedAccessException($"Principal '{principal}' has no grant on volume '{Name}'.");
            ulong applied = await ReadBarrierAsync(ct);
            string[] targets = [.. Record.BackupTargets.Keys.Order(StringComparer.Ordinal)];
            return new BackupRegisterResultMessage(targets, Token(applied));
        }

        if (!Enum.IsDefined(request.Action))
            throw new ArgumentException($"Unknown backup target action {(int)request.Action}.");
        CheckTargetName(request.Target);
        RequireBackupAdmin(principal, "backup " + request.Action.ToString().ToLowerInvariant());
        if (request.Header.Operation.IsEmpty)
            throw new ArgumentException("BackupRegister changes must carry an operation id (OUT-001).");
        await EnsureLeaderReadyAsync(ct);
        await Barrier.WaitAsync(ct);
        UInt128 requestId = BackupRequestId(Record.Id, request.Header.Operation, request.Action);
        BackupTargetUpdateMessage update;
        switch (request.Action)
        {
            case BackupTargetAction.Register:
            {
                DfsCredentials credentials = request.ExportCredentials
                    ?? throw new ArgumentException("A backup target registration needs export credentials.");
                if (string.IsNullOrEmpty(credentials.Principal) || string.IsNullOrEmpty(credentials.Secret))
                    throw new ArgumentException("The export credentials need a principal and a secret.");
                if (request.Endpoints.Count == 0 || request.Endpoints.Any(string.IsNullOrEmpty))
                    throw new ArgumentException("At least one non-empty backup endpoint is required.");
                (BackupScheduleData schedule, BackupBudgetsData budgets) = CheckSchedule(request.Schedule, request.Budgets);
                byte[] wrapped = BackupTargetControl.WrapExportSecret(_s.Keys.StorageKek, Record.Id, request.Target, credentials.Secret, _s.Random);
                update = new BackupTargetUpdateMessage(requestId, Record.Id, BackupTargetAction.Register, principal, request.Target, request.Endpoints,
                    credentials.Principal, wrapped, schedule, budgets);
                break;
            }

            case BackupTargetAction.Configure:
            {
                (BackupScheduleData schedule, BackupBudgetsData budgets) = CheckSchedule(request.Schedule, request.Budgets);
                if (!Record.BackupTargets.TryGetValue(request.Target, out BackupTargetRecord? existing))
                    throw new DfsException(DfsErrorCode.BackupTargetNotFound, $"Backup target '{request.Target}' is not registered on volume '{Name}'.");
                update = new BackupTargetUpdateMessage(requestId, Record.Id, BackupTargetAction.Configure, principal, request.Target, existing.Endpoints,
                    existing.ExportPrincipal, existing.WrappedExportSecret, schedule, budgets);
                break;
            }

            default:
            {
                if (!Record.BackupTargets.ContainsKey(request.Target))
                    throw new DfsException(DfsErrorCode.BackupTargetNotFound, $"Backup target '{request.Target}' is not registered on volume '{Name}'.");
                update = new BackupTargetUpdateMessage(requestId, Record.Id, BackupTargetAction.Remove, principal, request.Target, [], string.Empty,
                    ReadOnlyMemory<byte>.Empty, null, null);
                break;
            }
        }

        BackupTargetUpdateResultMessage result = await UpdateBackupTargetAsync(update, ct);
        await WaitForControlIndexAsync(result.ControlIndex, ct);
        if (request.Action == BackupTargetAction.Remove)
            await ProposeHousekeepingAsync(new RemoveBackupTargetStateCommand(request.Target));
        if (_s.BackupExports?.For(this) is { } exporter)
        {
            await exporter.ReconcileAsync();
            exporter.Target(request.Target)?.Wake(resetBackoff: true);
        }

        return new BackupRegisterResultMessage(result.Targets, Token(_sm.State.AppliedIndex));
    }

    /// <summary>Applies a registration change on the control leader (locally when this node leads the control group).</summary>
    private async Task<BackupTargetUpdateResultMessage> UpdateBackupTargetAsync(BackupTargetUpdateMessage update, CancellationToken ct)
    {
        TimeSpan delay = _s.Timings.Tick * _s.Timings.HeartbeatTicks;
        long started = _s.Time.GetTimestamp();
        TimeSpan budget = _s.Peers.DefaultCallTimeout * 4;
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            try
            {
                ControlHost control = _s.Control;
                if (control.IsLeader)
                    return await new BackupTargetControl(_s).ExecuteAsync(update, ct);
                if (control.Leader is not { } leader || leader == _s.NodeId)
                    throw new DfsUnavailableException(DfsErrorCode.NotLeader, "No control leader is known.");
                return (BackupTargetUpdateResultMessage)await _s.Peers.CallAsync(leader, update, ct: ct);
            }
            catch (Exception ex) when (!ct.IsCancellationRequested && _s.Time.GetElapsedTime(started) < budget
                && (ex is TimeoutException or IOException or Transport.RpcConnectionException
                    || DfsErrors.ToCode(ex) is DfsErrorCode.NotLeader or DfsErrorCode.Unavailable or DfsErrorCode.NodeUnavailable or DfsErrorCode.Timeout))
            {
                await Task.Delay(delay, _s.Time, ct);
                if (delay < TimeSpan.FromSeconds(1))
                    delay *= 2;
            }
        }
    }

    /// <summary>Waits (bounded) until the local control replica applied <paramref name="index"/>.</summary>
    private async Task WaitForControlIndexAsync(ulong index, CancellationToken ct)
    {
        long started = _s.Time.GetTimestamp();
        while (_s.Control.State.AppliedIndex < index && _s.Time.GetElapsedTime(started) < _s.Peers.DefaultCallTimeout * 2)
        {
            await Barrier.WaitAsync(ct);
            if (_s.Control.State.AppliedIndex < index)
                await Task.Delay(_s.Timings.Tick, _s.Time, ct);
        }
    }

    private static UInt128 BackupRequestId(VolumeId volume, DfsOperationId operation, BackupTargetAction action)
    {
        byte[] hash = SHA256.HashData(Encoding.UTF8.GetBytes(string.Create(System.Globalization.CultureInfo.InvariantCulture,
            $"tdfs/backup-target/v1\0{volume.Value:N}\0{operation.Session}\0{operation.Sequence}\0{(int)action}")));
        return System.Buffers.Binary.BinaryPrimitives.ReadUInt128LittleEndian(hash);
    }

    private static void CheckTargetName(string? target)
    {
        if (string.IsNullOrEmpty(target))
            throw new ArgumentException("A backup target name is required.");
        if (Encoding.UTF8.GetByteCount(target) > MaxTargetNameBytes || target.Any(char.IsControl))
            throw new ArgumentException($"'{target}' is not a valid backup target name.");
    }

    private static (BackupScheduleData Schedule, BackupBudgetsData Budgets) CheckSchedule(BackupScheduleData? schedule, BackupBudgetsData? budgets)
    {
        if (schedule is null || budgets is null)
            throw new ArgumentException("A schedule and budgets are required.");
        if (schedule.Interval is { } interval && interval < TimeSpan.Zero)
            throw new ArgumentException("The schedule interval must not be negative.");
        if (budgets.MaxPinAge <= TimeSpan.Zero || budgets.MaxPinnedBytes < 0 || budgets.MaxBytesPerSecond <= 0 || budgets.MaxConcurrentObjects <= 0)
            throw new ArgumentException("Backup budgets must be positive (pinned bytes non-negative).");
        return (schedule, budgets);
    }

    private void RequireBackupAdmin(string principal, string what)
    {
        if (_s.Control.State.IsClusterAdmin(principal))
            return;
        RequireGrant(principal, DfsPermissions.BackupAdmin, what);
    }

    private void RequireBackupRead(string principal, string what)
    {
        if (_s.Control.State.IsClusterAdmin(principal) || (GrantsOf(principal) & (DfsPermissions.Read | DfsPermissions.BackupAdmin)) != 0)
            return;
        _s.Control.ReportSecurityEvent("volume:" + what, "denied", "volume=" + Name, principal);
        throw new UnauthorizedAccessException($"Principal '{principal}' needs the Read or BackupAdmin grant on volume '{Name}' ({what}).");
    }

    private async Task<BackupTargetRecord> RequireTargetAsync(string target, CancellationToken ct)
    {
        if (Record.BackupTargets.TryGetValue(target, out BackupTargetRecord? record))
            return record;
        await Barrier.WaitAsync(ct);
        return Record.BackupTargets.TryGetValue(target, out record)
            ? record
            : throw new DfsException(DfsErrorCode.BackupTargetNotFound, $"Backup target '{target}' is not registered on volume '{Name}'.");
    }

    // ------------------------------------------------------------------------------------------------- BackupStatus

    /// <summary><c>BackupStatus</c>: received, durable and published boundaries reported separately (BAK-016).</summary>
    public async Task<IWireMessage> BackupStatusAsync(RpcContext context, BackupStatusMessage request)
    {
        RejectTransaction(request);
        string principal = context.Principal ?? string.Empty;
        CheckTargetName(request.Target);
        RequireBackupRead(principal, "backup status");
        await ReadBarrierAsync(context.Cancellation);
        await RequireTargetAsync(request.Target, context.Cancellation);
        TargetExporter? runner = _s.BackupExports?.For(this)?.Target(request.Target);
        return new BackupStatusResultMessage(TargetExporter.BuildStatus(_sm.State, request.Target, runner, _s.Time.GetUtcNow()));
    }

    // ------------------------------------------------------------------------------------------------- BackupRequestSync

    /// <summary>
    /// <c>BackupRequestSync</c>: commits a transfer pin of the named snapshot for the target, or — without a name — of the latest
    /// snapshot when nothing changed since it, otherwise of an export snapshot of the current committed namespace; then wakes the
    /// target's exporter. Stored outcome per operation id.
    /// </summary>
    public async Task<IWireMessage> BackupRequestSyncAsync(RpcContext context, BackupRequestSyncMessage request)
    {
        RejectTransaction(request);
        string principal = context.Principal ?? string.Empty;
        CheckTargetName(request.Target);
        if (request.SnapshotName is { Length: 0 })
            throw new ArgumentException("The snapshot name must not be empty (null requests the latest commit).");
        RequireBackupAdmin(principal, "request backup sync");
        await EnsureLeaderReadyAsync(context.Cancellation);
        await RequireTargetAsync(request.Target, context.Cancellation);
        PinRecord? pinned = null;
        IWireMessage reply = await MutateAsync(context, request, DfsOperationKind.BackupTransfer, (op, _) =>
        {
            string? snapshot = request.SnapshotName;
            string? export = null;
            if (snapshot is null)
            {
                VolumeState state = _sm.State;
                SnapshotRecord? latest = state.Snapshots.Values.MaxBy(s => s.BoundaryIndex);
                if (latest is not null && ReferenceEquals(latest.Root, state.Inodes))
                    snapshot = latest.Name;
                else
                    export = TargetExporter.ExportName(request.Target, _s.Time.GetUtcNow());
            }

            return Task.FromResult<VolumeCommand>(Stamp(new RequestBackupSyncCommand(request.Target, snapshot, export), op));
        }, finish: result =>
        {
            pinned = result.Value as PinRecord;
            _s.BackupExports?.For(this)?.Wake(request.Target);
            return result.Reply ?? new BackupRequestSyncResultMessage(true, CommitToken.None);
        });

        // Answer once the transfer is under way (bounded) when it is the target's next one: the caller observes its effects in
        // order with the reply. A request queued behind earlier transfers is answered at once.
        if (pinned is not null && IsNextTransfer(pinned, request.Target) && _s.BackupExports?.For(this)?.Target(request.Target) is { } runner)
            await runner.WaitForStartAsync(pinned.Id, RequestSyncStartWait);
        return reply;
    }

    /// <summary>True when <paramref name="pin"/> is the first transfer pin of <paramref name="target"/> in transfer order.</summary>
    private bool IsNextTransfer(PinRecord pin, string target)
        => _sm.State.Pins.Values
            .Where(p => p.Kind == PinKind.Transfer && string.Equals(p.Target, target, StringComparison.Ordinal))
            .OrderBy(p => p.PinIndex)
            .ThenBy(p => p.Id)
            .FirstOrDefault()?.Id == pin.Id;

    // ------------------------------------------------------------------------------------------------- BackupAwait

    /// <summary>
    /// <c>BackupAwait</c>: a bounded wait for the first published checkpoint of this volume authority covering a commit, or for an
    /// exact snapshot durably retained on the target (BAK-017, BAK-018). A timeout answers <c>Reached = false</c> with the latest
    /// published checkpoint and never changes live state (BAK-019).
    /// </summary>
    public async Task<IWireMessage> BackupAwaitAsync(RpcContext context, BackupAwaitMessage request)
    {
        RejectTransaction(request);
        string principal = context.Principal ?? string.Empty;
        CheckTargetName(request.Target);
        if ((request.Commit is null) == (request.SnapshotName is null))
            throw new ArgumentException("Await either a commit or a snapshot.");
        if (request.SnapshotName is { Length: 0 })
            throw new ArgumentException("The snapshot name must not be empty.");
        if (request.Timeout < TimeSpan.Zero)
            throw new ArgumentException("The timeout must not be negative.");
        RequireBackupRead(principal, "await backup");
        CancellationToken ct = context.Cancellation;
        await ReadBarrierAsync(ct);
        await RequireTargetAsync(request.Target, ct);
        VolumeState state = _sm.State;
        SnapshotId? exactId = null;
        if (request.Commit is { } commit)
        {
            if (commit.IsNone)
                throw new ArgumentException("CommitToken.None is not a commit boundary.");
            if (commit.ClusterId != state.Config.ClusterId || commit.VolumeId != state.Config.Id)
                throw new ArgumentException($"The commit token {commit} belongs to another volume; it cannot be compared with volume '{Name}'.");
            if (commit.VolumeEpoch != state.Config.Epoch)
                throw new DfsEpochMismatchException($"The commit token {commit} belongs to epoch {commit.VolumeEpoch} of volume '{Name}' (now epoch {state.Config.Epoch}).");
        }
        else if (state.Snapshots.TryGetValue(request.SnapshotName!, out SnapshotRecord? snapshot))
        {
            exactId = snapshot.Id;
        }
        else if (state.Pins.Values.FirstOrDefault(p => p.Kind == PinKind.Transfer && string.Equals(p.Snapshot, request.SnapshotName, StringComparison.Ordinal))
                 is { } pin)
        {
            exactId = VolumeStateMachine.SnapshotIdAt(state.Config, pin.PinIndex);
        }

        TargetExporter runner = _s.BackupExports?.For(this)?.Target(request.Target) ?? throw Group.NotLeader();
        TimeSpan timeout = request.Timeout > MaxAwait ? MaxAwait : request.Timeout;
        return await runner.AwaitAsync(request.Commit, exactId, exactId is null ? request.SnapshotName : null, timeout, ct);
    }
}
