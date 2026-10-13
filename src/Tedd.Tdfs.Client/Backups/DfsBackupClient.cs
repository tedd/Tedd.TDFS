using Tedd.Tdfs.Client.Internal;
using Tedd.Tdfs.Client.Internal.Facades;
using Tedd.Tdfs.Client.Internal.Runtime;
using Tedd.Tdfs.Protocol.Messages;

namespace Tedd.Tdfs;

/// <summary>
/// Late-sync backup targets of a volume (FSD §12): registration, configuration, status, sync requests and bounded waits
/// for a commit or snapshot to be published on a target. A timeout never rolls back the live commit.
/// </summary>
public sealed class DfsBackupClient
{
    private static readonly TimeSpan AwaitMargin = TimeSpan.FromSeconds(2);

    internal DfsBackupClient(DfsVolume volume)
    {
        ArgumentNullException.ThrowIfNull(volume);
        Volume = volume;
    }

    /// <summary>The owning volume facade.</summary>
    internal DfsVolume Volume { get; }

    private ClientRuntime Runtime => Volume.Runtime;

    /// <summary>Registers a backup target (endpoints, export credentials, schedule and budgets).</summary>
    public async Task RegisterTargetAsync(DfsBackupTargetRegistration registration, CancellationToken ct = default)
    {
        using OperationScope scope = Begin(DfsOperationKind.Admin, ct);
        await scope.StartedAsync().ConfigureAwait(false);
        ArgumentNullException.ThrowIfNull(registration);
        Validation.Name(registration.Name, nameof(registration));
        if (registration.Endpoints is null || registration.Endpoints.Count == 0 || registration.Endpoints.Any(string.IsNullOrEmpty))
            throw new ArgumentException("At least one non-empty backup endpoint is required.", nameof(registration));
        ArgumentNullException.ThrowIfNull(registration.ExportCredentials, nameof(registration));
        Validation.Schedule(registration.Schedule, nameof(registration));
        Validation.Budgets(registration.Budgets, nameof(registration));
        await scope.ValidatedAsync().ConfigureAwait(false);
        await Volume.Context.MutateAsync<BackupRegisterResultMessage>(scope, "BackupRegister", null,
            header => new BackupRegisterMessage(header, BackupTargetAction.Register, registration.Name, registration.Endpoints, registration.ExportCredentials,
                ToWire(registration.Schedule), ToWire(registration.Budgets))).ConfigureAwait(false);
    }

    /// <summary>Removes a backup target (its published snapshots stay on the target under their own retention).</summary>
    public async Task RemoveTargetAsync(string target, CancellationToken ct = default)
    {
        using OperationScope scope = Begin(DfsOperationKind.Admin, ct);
        await scope.StartedAsync().ConfigureAwait(false);
        Validation.Name(target, nameof(target));
        await scope.ValidatedAsync().ConfigureAwait(false);
        await Volume.Context.MutateAsync<BackupRegisterResultMessage>(scope, "BackupRegister", null,
            header => new BackupRegisterMessage(header, BackupTargetAction.Remove, target, [], null, null, null)).ConfigureAwait(false);
    }

    /// <summary>Synchronous counterpart of <see cref="RemoveTargetAsync"/>.</summary>
    public void RemoveTarget(string target) =>
        SyncBridge.Run(() => RemoveTargetAsync(target, CancellationToken.None));

    /// <summary>Changes a target's schedule and budgets.</summary>
    public async Task ConfigureAsync(string target, DfsBackupSchedule schedule, DfsBackupBudgets budgets, CancellationToken ct = default)
    {
        using OperationScope scope = Begin(DfsOperationKind.Admin, ct);
        await scope.StartedAsync().ConfigureAwait(false);
        Validation.Name(target, nameof(target));
        Validation.Schedule(schedule);
        Validation.Budgets(budgets);
        await scope.ValidatedAsync().ConfigureAwait(false);
        await Volume.Context.MutateAsync<BackupRegisterResultMessage>(scope, "BackupRegister", null,
            header => new BackupRegisterMessage(header, BackupTargetAction.Configure, target, [], null, ToWire(schedule), ToWire(budgets))).ConfigureAwait(false);
    }

    /// <summary>Synchronous counterpart of <see cref="ConfigureAsync"/>.</summary>
    public void Configure(string target, DfsBackupSchedule schedule, DfsBackupBudgets budgets) =>
        SyncBridge.Run(() => ConfigureAsync(target, schedule, budgets, CancellationToken.None));

    /// <summary>Names of the registered targets.</summary>
    public async Task<IReadOnlyList<string>> ListTargetsAsync(CancellationToken ct = default)
    {
        using OperationScope scope = Begin(DfsOperationKind.GetInfo, ct);
        await scope.StartedAsync().ConfigureAwait(false);
        await scope.ValidatedAsync().ConfigureAwait(false);
        BackupRegisterResultMessage result = await Volume.Context.ReadAsync<BackupRegisterResultMessage>(scope, "BackupRegister(list)",
            header => new BackupRegisterMessage(header, BackupTargetAction.List, string.Empty, [], null, null, null)).ConfigureAwait(false);
        return result.Targets;
    }

    /// <summary>Synchronous counterpart of <see cref="ListTargetsAsync"/>.</summary>
    public IReadOnlyList<string> ListTargets() =>
        SyncBridge.Run(() => ListTargetsAsync(CancellationToken.None));

    /// <summary>Progress and health of a target.</summary>
    public async Task<DfsBackupStatus> GetStatusAsync(string target, CancellationToken ct = default)
    {
        using OperationScope scope = Begin(DfsOperationKind.GetInfo, ct);
        await scope.StartedAsync().ConfigureAwait(false);
        Validation.Name(target, nameof(target));
        await scope.ValidatedAsync().ConfigureAwait(false);
        BackupStatusResultMessage result = await Volume.Context.ReadAsync<BackupStatusResultMessage>(scope, "BackupStatus",
            header => new BackupStatusMessage(header, target)).ConfigureAwait(false);
        return WireConvert.ToBackupStatus(result.Status);
    }

    /// <summary>Synchronous counterpart of <see cref="GetStatusAsync"/>.</summary>
    public DfsBackupStatus GetStatus(string target) =>
        SyncBridge.Run(() => GetStatusAsync(target, CancellationToken.None));

    /// <summary>Asks for a transfer now (optionally of a named snapshot).</summary>
    public async Task RequestSyncAsync(string target, string? snapshotName = null, CancellationToken ct = default)
    {
        using OperationScope scope = Begin(DfsOperationKind.BackupTransfer, ct);
        await scope.StartedAsync().ConfigureAwait(false);
        Validation.Name(target, nameof(target));
        if (snapshotName is { Length: 0 })
            throw new ArgumentException("The snapshot name must not be empty (use null for the latest commit).", nameof(snapshotName));
        await scope.ValidatedAsync().ConfigureAwait(false);
        await Volume.Context.MutateAsync<BackupRequestSyncResultMessage>(scope, "BackupRequestSync", null,
            header => new BackupRequestSyncMessage(header, target, snapshotName)).ConfigureAwait(false);
    }

    /// <summary>Synchronous counterpart of <see cref="RequestSyncAsync"/>.</summary>
    public void RequestSync(string target, string? snapshotName = null) =>
        SyncBridge.Run(() => RequestSyncAsync(target, snapshotName, CancellationToken.None));

    /// <summary>
    /// Waits until a published snapshot on <paramref name="target"/> covers <paramref name="commit"/>.
    /// </summary>
    /// <exception cref="DfsBackupNotReachedException">The commit was not published on the target within <paramref name="timeout"/>.</exception>
    public Task<DfsBackupCheckpoint> AwaitBackupAsync(string target, CommitToken commit, TimeSpan timeout, CancellationToken ct = default) =>
        AwaitAsync(target, commit, null, timeout, ct);

    /// <summary>Synchronous counterpart of <see cref="AwaitBackupAsync"/>.</summary>
    public DfsBackupCheckpoint AwaitBackup(string target, CommitToken commit, TimeSpan timeout) =>
        SyncBridge.Run(() => AwaitBackupAsync(target, commit, timeout, CancellationToken.None));

    /// <summary>Waits until the exact snapshot <paramref name="snapshotName"/> is published on <paramref name="target"/>.</summary>
    /// <exception cref="DfsBackupNotReachedException">The snapshot was not published on the target within <paramref name="timeout"/>.</exception>
    public Task<DfsBackupCheckpoint> AwaitBackupSnapshotAsync(string target, string snapshotName, TimeSpan timeout, CancellationToken ct = default) =>
        AwaitAsync(target, null, snapshotName ?? throw new ArgumentNullException(nameof(snapshotName)), timeout, ct);

    /// <summary>Synchronous counterpart of <see cref="AwaitBackupSnapshotAsync"/>.</summary>
    public DfsBackupCheckpoint AwaitBackupSnapshot(string target, string snapshotName, TimeSpan timeout) =>
        SyncBridge.Run(() => AwaitBackupSnapshotAsync(target, snapshotName, timeout, CancellationToken.None));

    private async Task<DfsBackupCheckpoint> AwaitAsync(string target, CommitToken? commit, string? snapshotName, TimeSpan timeout, CancellationToken ct)
    {
        TimeSpan operationTimeout = Runtime.Options.OperationTimeout;
        TimeSpan budget = timeout > TimeSpan.Zero && timeout + AwaitMargin > operationTimeout ? timeout + AwaitMargin : operationTimeout;
        using OperationScope scope = Runtime.BeginOperation(DfsOperationKind.BackupAwait, Volume.Name, null, ct, budget);
        await scope.StartedAsync().ConfigureAwait(false);
        Validation.Name(target, nameof(target));
        Validation.PositiveTimeout(timeout, nameof(timeout));
        if (snapshotName is { Length: 0 })
            throw new ArgumentException("The snapshot name must not be empty.", nameof(snapshotName));
        await scope.ValidatedAsync().ConfigureAwait(false);
        long started = Runtime.Time.GetTimestamp();
        BackupAwaitResultMessage result = await Volume.Context.ReadAsync<BackupAwaitResultMessage>(scope, "BackupAwait", header =>
            {
                // A retried wait only waits for what is left of the caller's timeout.
                TimeSpan left = timeout - Runtime.Time.GetElapsedTime(started);
                return new BackupAwaitMessage(header, target, commit, snapshotName, left > TimeSpan.Zero ? left : TimeSpan.FromMilliseconds(1));
            }, attemptTimeout: timeout + AwaitMargin).ConfigureAwait(false);
        if (result is { Reached: true, Checkpoint: { } checkpoint })
            return checkpoint;
        string what = snapshotName is not null ? $"snapshot '{snapshotName}'" : $"commit {commit}";
        throw new DfsBackupNotReachedException(target, result.LastPublished,
            $"Backup target '{target}' did not publish {what} within {timeout}; the live commit is unaffected.");
    }

    private OperationScope Begin(DfsOperationKind kind, CancellationToken ct) => Runtime.BeginOperation(kind, Volume.Name, null, ct);

    private static BackupScheduleData ToWire(DfsBackupSchedule schedule) => new(schedule.Interval, schedule.OnEverySnapshot);

    private static BackupBudgetsData ToWire(DfsBackupBudgets budgets) =>
        new(budgets.MaxPinAge, budgets.MaxPinnedBytes, budgets.MaxBytesPerSecond, budgets.MaxConcurrentObjects);
}
