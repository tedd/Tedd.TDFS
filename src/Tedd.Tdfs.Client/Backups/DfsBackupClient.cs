using Tedd.Tdfs.Client.Internal;

namespace Tedd.Tdfs;

/// <summary>
/// Late-sync backup targets of a volume (FSD §12). <see cref="RegisterTargetAsync"/> is async-only; every other
/// network-bound method has a synchronous counterpart.
/// </summary>
public sealed class DfsBackupClient
{
    internal DfsBackupClient(DfsVolume volume)
    {
        ArgumentNullException.ThrowIfNull(volume);
        Volume = volume;
    }

    /// <summary>The volume this facade operates on.</summary>
    internal DfsVolume Volume { get; }

    /// <summary>Registers a backup target for the volume (async-only).</summary>
    public Task RegisterTargetAsync(DfsBackupTargetRegistration registration, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsBackupClient.RegisterTargetAsync: implemented in a later phase");

    /// <summary>Removes a backup target registration (the target keeps its published snapshots).</summary>
    public Task RemoveTargetAsync(string target, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsBackupClient.RemoveTargetAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="RemoveTargetAsync"/>.</summary>
    public void RemoveTarget(string target) =>
        SyncBridge.Run(() => RemoveTargetAsync(target, CancellationToken.None));

    /// <summary>Changes a target's schedule and budgets.</summary>
    public Task ConfigureAsync(string target, DfsBackupSchedule schedule, DfsBackupBudgets budgets, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsBackupClient.ConfigureAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="ConfigureAsync"/>.</summary>
    public void Configure(string target, DfsBackupSchedule schedule, DfsBackupBudgets budgets) =>
        SyncBridge.Run(() => ConfigureAsync(target, schedule, budgets, CancellationToken.None));

    /// <summary>Lists the registered target names.</summary>
    public Task<IReadOnlyList<string>> ListTargetsAsync(CancellationToken ct = default) =>
        throw new NotImplementedException("DfsBackupClient.ListTargetsAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="ListTargetsAsync"/>.</summary>
    public IReadOnlyList<string> ListTargets() =>
        SyncBridge.Run(() => ListTargetsAsync(CancellationToken.None));

    /// <summary>Reads a target's progress (received, durable and published boundaries).</summary>
    public Task<DfsBackupStatus> GetStatusAsync(string target, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsBackupClient.GetStatusAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="GetStatusAsync"/>.</summary>
    public DfsBackupStatus GetStatus(string target) =>
        SyncBridge.Run(() => GetStatusAsync(target, CancellationToken.None));

    /// <summary>Requests a synchronisation now, optionally of a specific snapshot.</summary>
    public Task RequestSyncAsync(string target, string? snapshotName = null, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsBackupClient.RequestSyncAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="RequestSyncAsync"/>.</summary>
    public void RequestSync(string target, string? snapshotName = null) =>
        SyncBridge.Run(() => RequestSyncAsync(target, snapshotName, CancellationToken.None));

    /// <summary>
    /// Waits for the first published checkpoint covering <paramref name="commit"/>; throws
    /// <see cref="DfsBackupNotReachedException"/> on timeout (live commits are never rolled back).
    /// </summary>
    public Task<DfsBackupCheckpoint> AwaitBackupAsync(string target, CommitToken commit, TimeSpan timeout, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsBackupClient.AwaitBackupAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="AwaitBackupAsync"/>.</summary>
    public DfsBackupCheckpoint AwaitBackup(string target, CommitToken commit, TimeSpan timeout) =>
        SyncBridge.Run(() => AwaitBackupAsync(target, commit, timeout, CancellationToken.None));

    /// <summary>
    /// Waits until exactly snapshot <paramref name="snapshotName"/> is durably published on the target; throws
    /// <see cref="DfsBackupNotReachedException"/> on timeout.
    /// </summary>
    public Task<DfsBackupCheckpoint> AwaitBackupSnapshotAsync(string target, string snapshotName, TimeSpan timeout, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsBackupClient.AwaitBackupSnapshotAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="AwaitBackupSnapshotAsync"/>.</summary>
    public DfsBackupCheckpoint AwaitBackupSnapshot(string target, string snapshotName, TimeSpan timeout) =>
        SyncBridge.Run(() => AwaitBackupSnapshotAsync(target, snapshotName, timeout, CancellationToken.None));
}
