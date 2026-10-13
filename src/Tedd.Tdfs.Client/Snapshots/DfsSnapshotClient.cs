using Tedd.Tdfs.Client.Internal;
using Tedd.Tdfs.Client.Internal.Facades;
using Tedd.Tdfs.Client.Internal.Runtime;
using Tedd.Tdfs.Client.Internal.Sessions;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;

namespace Tedd.Tdfs;

/// <summary>
/// Snapshots of a volume (FSD §11): creation (optionally application-consistent through adapters), listing, read-only
/// views, retention, deletion and restore into a new volume.
/// </summary>
public sealed class DfsSnapshotClient
{
    internal DfsSnapshotClient(DfsVolume volume, bool isReadOnly = false)
    {
        ArgumentNullException.ThrowIfNull(volume);
        Volume = volume;
        IsReadOnly = isReadOnly;
    }

    /// <summary>The owning volume facade.</summary>
    internal DfsVolume Volume { get; }

    /// <summary>True on backup hosts (create/delete rejected).</summary>
    public bool IsReadOnly { get; }

    private ClientRuntime Runtime => Volume.Runtime;

    /// <summary>
    /// Creates a snapshot. With adapters: each is prepared in turn (bounded by <see cref="DfsSnapshotOptions.AdapterTimeout"/>),
    /// the snapshot is captured, then every prepared adapter is resumed. A failed prepare aborts the snapshot (the failed
    /// adapter is aborted, prepared ones resumed). A resume failure does not undo the snapshot; it is reported in
    /// <see cref="DfsSnapshotInfo.AdapterResumeFailure"/>.
    /// </summary>
    public async Task<DfsSnapshotInfo> CreateAsync(string name, DfsSnapshotOptions? options = null, CancellationToken ct = default)
    {
        using OperationScope scope = Runtime.BeginOperation(DfsOperationKind.CreateSnapshot, Volume.Name, null, ct);
        await scope.StartedAsync().ConfigureAwait(false);
        Validation.Name(name, nameof(name));
        options ??= new DfsSnapshotOptions();
        if (options.Adapters is null)
            throw new ArgumentException("Adapters must not be null (use an empty list).", nameof(options));
        if (options.Adapters.Any(a => a is null))
            throw new ArgumentException("Adapters must not contain null entries.", nameof(options));
        Validation.PositiveTimeout(options.AdapterTimeout, nameof(options));
        if (options.OperationId is { IsEmpty: true })
            throw new ArgumentException("OperationId must not be the empty id; obtain one with DfsVolume.NewOperationIdAsync.", nameof(options));
        if (IsReadOnly)
            throw new DfsReadOnlyException("Snapshots cannot be created on a backup host.");
        await scope.ValidatedAsync().ConfigureAwait(false);

        var adapters = new SnapshotAdapters(Runtime, Volume.Name, name, options.Adapters, options.AdapterTimeout);
        await adapters.PrepareAsync(scope).ConfigureAwait(false);
        DfsConsistencyLabel label = options.Adapters.Count > 0 ? DfsConsistencyLabel.ApplicationConsistent : DfsConsistencyLabel.FilesystemConsistent;
        AdapterRecordData[] records = options.Adapters.Select(WireConvert.ToWire).ToArray();
        CreateSnapshotResultMessage created;
        try
        {
            created = await Volume.Context.MutateAsync<CreateSnapshotResultMessage>(scope, "CreateSnapshot", options.OperationId,
                header => new CreateSnapshotMessage(header, name, options.MinimumCommit, label, records, options.Retention)).ConfigureAwait(false);
        }
        catch (DfsCommitOutcomeUnknownException unknown) when (adapters.PreparedCount > 0)
        {
            // Applications stay paused until the capture can no longer happen later: fence it first (bounded).
            (WireOutcomeStatus status, CreateSnapshotResultMessage? fenced) = await FenceAsync(unknown.OperationId, options.AdapterTimeout).ConfigureAwait(false);
            if (fenced is null)
            {
                // Abort tells an adapter that the capture failed; a capture that committed (its reply not retained) or whose
                // fate stays open only resumes them.
                if (status == WireOutcomeStatus.Aborted)
                    await adapters.AbortAndResumeAsync().ConfigureAwait(false);
                else
                    await adapters.ResumeAsync().ConfigureAwait(false);
                throw;
            }

            created = fenced;
        }
        catch (Exception)
        {
            await adapters.AbortAndResumeAsync().ConfigureAwait(false);
            throw;
        }

        Exception? resumeFailure = await adapters.ResumeAsync().ConfigureAwait(false);
        return WireConvert.ToSnapshotInfo(created.Snapshot, resumeFailure);
    }

    /// <summary>Synchronous counterpart of <see cref="CreateAsync"/>.</summary>
    public DfsSnapshotInfo Create(string name, DfsSnapshotOptions? options = null) =>
        SyncBridge.Run(() => CreateAsync(name, options, CancellationToken.None));

    /// <summary>
    /// Fences an uncertain capture: its final status (<see cref="WireOutcomeStatus.Pending"/> when the fence itself failed)
    /// and its reply when it committed and the reply was retained.
    /// </summary>
    private async Task<(WireOutcomeStatus Status, CreateSnapshotResultMessage? Created)> FenceAsync(DfsOperationId operation, TimeSpan budget)
    {
        try
        {
            using OperationScope fence = Runtime.BeginBackground(DfsOperationKind.QueryOperation, Volume.Name, budget);
            QueryOperationResultMessage result = await Volume.Context.QueryAsync(operation, isTransaction: false, resolve: true, fence).ConfigureAwait(false);
            if (result.Status == WireOutcomeStatus.Committed && !result.ResultFrame.IsEmpty
                && MessageCodec.Decode(result.ResultFrame.Span) is CreateSnapshotResultMessage committed)
            {
                return (result.Status, committed);
            }

            return (result.Status, null);
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            Runtime.Emit("snapshot.fence-failed", $"Fencing snapshot operation {operation} failed ({ex.GetType().Name}).", operation, volume: Volume.Name,
                exception: ex);
        }

        return (WireOutcomeStatus.Pending, null);
    }

    /// <summary>Lists the volume's snapshots.</summary>
    public async Task<IReadOnlyList<DfsSnapshotInfo>> ListAsync(CancellationToken ct = default)
    {
        using OperationScope scope = Runtime.BeginOperation(DfsOperationKind.GetInfo, Volume.Name, null, ct);
        await scope.StartedAsync().ConfigureAwait(false);
        await scope.ValidatedAsync().ConfigureAwait(false);
        ListSnapshotsResultMessage result = await Volume.Context.ReadAsync<ListSnapshotsResultMessage>(scope, "ListSnapshots",
            header => new ListSnapshotsMessage(header, null)).ConfigureAwait(false);
        return result.Snapshots.Select(s => WireConvert.ToSnapshotInfo(s)).ToArray();
    }

    /// <summary>Synchronous counterpart of <see cref="ListAsync"/>.</summary>
    public IReadOnlyList<DfsSnapshotInfo> List() =>
        SyncBridge.Run(() => ListAsync(CancellationToken.None));

    /// <summary>Metadata of one snapshot (<see cref="DfsException"/> with <see cref="DfsErrorCode.SnapshotNotFound"/> when absent).</summary>
    public async Task<DfsSnapshotInfo> GetAsync(string name, CancellationToken ct = default)
    {
        using OperationScope scope = Runtime.BeginOperation(DfsOperationKind.GetInfo, Volume.Name, null, ct);
        await scope.StartedAsync().ConfigureAwait(false);
        Validation.Name(name, nameof(name));
        await scope.ValidatedAsync().ConfigureAwait(false);
        ListSnapshotsResultMessage result = await Volume.Context.ReadAsync<ListSnapshotsResultMessage>(scope, "ListSnapshots",
            header => new ListSnapshotsMessage(header, name)).ConfigureAwait(false);
        SnapshotInfoData? found = result.Snapshots.FirstOrDefault(s => string.Equals(s.Name, name, StringComparison.Ordinal));
        return found is null
            ? throw DfsErrors.ToException(DfsErrorCode.SnapshotNotFound, $"Snapshot '{name}' does not exist on volume '{Volume.Name}'.")
            : WireConvert.ToSnapshotInfo(found);
    }

    /// <summary>Synchronous counterpart of <see cref="GetAsync"/>.</summary>
    public DfsSnapshotInfo Get(string name) =>
        SyncBridge.Run(() => GetAsync(name, CancellationToken.None));

    /// <summary>Opens a read-only view of a snapshot (holds a reader pin until disposed).</summary>
    public async Task<DfsSnapshotView> OpenAsync(string name, CancellationToken ct = default)
    {
        using OperationScope scope = Runtime.BeginOperation(DfsOperationKind.OpenSnapshot, Volume.Name, null, ct);
        await scope.StartedAsync().ConfigureAwait(false);
        Validation.Name(name, nameof(name));
        await scope.ValidatedAsync().ConfigureAwait(false);
        VolumeContext.Bound<OpenSnapshotResultMessage> bound;
        try
        {
            bound = await Volume.Context.MutateBoundAsync<OpenSnapshotResultMessage>(scope, "OpenSnapshot", null,
                header => new OpenSnapshotMessage(header, name)).ConfigureAwait(false);
        }
        catch (DfsCommitOutcomeUnknownException unknown)
        {
            // Opening a view changes nothing visible: an unanswered open is an availability failure (an orphan pin expires).
            throw new DfsUnavailableException(DfsErrorCode.Unavailable, $"Opening snapshot '{name}' did not complete.", unknown.InnerException ?? unknown);
        }

        OpenSnapshotResultMessage opened = bound.Reply;
        var view = new DfsSnapshotView(Volume, WireConvert.ToSnapshotInfo(opened.Snapshot), opened.SnapshotPin, bound.Binding);
        Volume.Context.RegisterPin(opened.SnapshotPin, bound.Session ?? Volume.Context.CurrentSession, view);
        return view;
    }

    /// <summary>Synchronous counterpart of <see cref="OpenAsync"/>.</summary>
    public DfsSnapshotView Open(string name) =>
        SyncBridge.Run(() => OpenAsync(name, CancellationToken.None));

    /// <summary>Deletes a snapshot's retention reference (fails while retention protects it).</summary>
    public async Task DeleteAsync(string name, CancellationToken ct = default)
    {
        using OperationScope scope = Runtime.BeginOperation(DfsOperationKind.DeleteSnapshot, Volume.Name, null, ct);
        await scope.StartedAsync().ConfigureAwait(false);
        Validation.Name(name, nameof(name));
        await scope.ValidatedAsync().ConfigureAwait(false);
        await Volume.Context.MutateAsync<DeleteSnapshotResultMessage>(scope, "DeleteSnapshot", null,
            header => new DeleteSnapshotMessage(header, name)).ConfigureAwait(false);
    }

    /// <summary>Synchronous counterpart of <see cref="DeleteAsync"/>.</summary>
    public void Delete(string name) =>
        SyncBridge.Run(() => DeleteAsync(name, CancellationToken.None));

    /// <summary>Changes a snapshot's retention (expiry and legal hold).</summary>
    public async Task SetRetentionAsync(string name, DfsRetention retention, CancellationToken ct = default)
    {
        using OperationScope scope = Runtime.BeginOperation(DfsOperationKind.CreateSnapshot, Volume.Name, null, ct);
        await scope.StartedAsync().ConfigureAwait(false);
        Validation.Name(name, nameof(name));
        ArgumentNullException.ThrowIfNull(retention);
        await scope.ValidatedAsync().ConfigureAwait(false);
        await Volume.Context.MutateAsync<SetRetentionResultMessage>(scope, "SetRetention", null,
            header => new SetRetentionMessage(header, name, retention)).ConfigureAwait(false);
    }

    /// <summary>Synchronous counterpart of <see cref="SetRetentionAsync"/>.</summary>
    public void SetRetention(string name, DfsRetention retention) =>
        SyncBridge.Run(() => SetRetentionAsync(name, retention, CancellationToken.None));

    /// <summary>Restores a snapshot into a new volume (new id, epoch 1); the source volume is unchanged.</summary>
    public async Task<DfsVolumeInfo> RestoreAsync(string name, string newVolumeName, CancellationToken ct = default)
    {
        using OperationScope scope = Runtime.BeginOperation(DfsOperationKind.RestoreSnapshot, Volume.Name, null, ct);
        await scope.StartedAsync().ConfigureAwait(false);
        Validation.Name(name, nameof(name));
        Validation.Name(newVolumeName, nameof(newVolumeName));
        if (!DfsPath.TryNormalizeComponent(newVolumeName, out _, out string? error))
            throw new ArgumentException($"Invalid volume name: {error}", nameof(newVolumeName));
        if (IsReadOnly)
            throw new DfsReadOnlyException("Restores are not started through a backup host's read-only view.");
        await scope.ValidatedAsync().ConfigureAwait(false);
        RestoreSnapshotResultMessage restored = await Volume.Context.MutateAsync<RestoreSnapshotResultMessage>(scope, "RestoreSnapshot", null,
            header => new RestoreSnapshotMessage(header, name, newVolumeName)).ConfigureAwait(false);
        await Runtime.RefreshMapAsync(scope).ConfigureAwait(false);
        return WireConvert.ToVolumeInfo(restored.Volume, restored.LastCommit);
    }

    /// <summary>Synchronous counterpart of <see cref="RestoreAsync"/>.</summary>
    public DfsVolumeInfo Restore(string name, string newVolumeName) =>
        SyncBridge.Run(() => RestoreAsync(name, newVolumeName, CancellationToken.None));
}
