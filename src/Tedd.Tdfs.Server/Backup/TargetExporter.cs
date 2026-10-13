using System.Collections.Immutable;
using System.Globalization;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Control;
using Tedd.Tdfs.Server.Runtime;
using Tedd.Tdfs.Server.Volumes.Apply;
using Tedd.Tdfs.Server.Volumes.Commands;
using Tedd.Tdfs.Server.Volumes.Service;
using Tedd.Tdfs.Server.Volumes.State;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Server.Backup;

/// <summary>
/// The exporter of one backup target on the volume leader (ARCHITECTURE §12, FSD §12.2). It works through the target's transfer
/// pins in boundary order: <c>BackupBegin</c> under the lineage recorded in the volume's backup progress (a full reseed after
/// abandonment), the target's inventory hint, verified object streaming (read verified from live replicas, bandwidth and
/// concurrency limited), the encrypted manifest, then <c>BackupPublish</c>, resending whatever the target's own closure
/// verification reports missing. A publication is recorded with <c>RecordBackupProgress</c> and the pin released. While the
/// target is unreachable the pin budgets apply: exceeding the age or byte budget abandons the lineage (pins released, reseed
/// required). Schedules (interval, every snapshot) add pins through committed entries. Status (received, durable and
/// published boundaries, BAK-016) is tracked here, leader-locally.
/// </summary>
internal sealed class TargetExporter
{
    private const int InventoryBatch = 4096;
    private const int ManifestChunk = 256 * 1024;
    private const int MaxPublishRounds = 8;
    private const long MinPutBytesPerSecond = 16 * 1024;
    private static readonly TimeSpan MinBackoff = TimeSpan.FromMilliseconds(200);
    private static readonly TimeSpan MaxBackoff = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan PublishTimeout = TimeSpan.FromSeconds(60);
    private static readonly TimeSpan PinnedBytesInterval = TimeSpan.FromSeconds(1);

    private readonly VolumeExporter _volume;
    private readonly NodeServices _s;
    private readonly CancellationTokenSource _cts;
    private readonly Dictionary<ulong, long> _pinSeen = [];
    private readonly Dictionary<ulong, CancellationTokenSource> _startWaiters = [];
    private readonly HashSet<ulong> _startedPins = [];

    // Waits are delays cancelled by a signal, so their continuations always resume on the node loop.
    private CancellationTokenSource? _wakeWait;
    private bool _woken;
    private CancellationTokenSource _changed = new();
    private ExportConnection? _connection;
    private Task<ExportConnection>? _connecting;
    private TimeSpan _backoff;
    private long _rateNext;
    private long _pinnedAt;
    private bool _pinnedComputed;
    private long _intervalAt;
    private bool _intervalArmed;
    private ImmutableSortedDictionary<ulong, InodeRecord>? _lastExportedRoot;
    private int _suspended;
    private Task _step = Task.CompletedTask;
    private SealedManifest? _sealed;
    private bool _stopped;

    public TargetExporter(VolumeExporter volume, string name)
    {
        _volume = volume;
        _s = volume.S;
        Name = name;
        _cts = CancellationTokenSource.CreateLinkedTokenSource(volume.Stopping);
    }

    /// <summary>Target name.</summary>
    public string Name { get; }

    /// <summary>Transfer state.</summary>
    public WireBackupState State { get; private set; } = WireBackupState.Idle;

    /// <summary>Last transfer error (cleared by the next publication).</summary>
    public string? LastError { get; private set; }

    /// <summary>Object bytes sent to the target by this leader (monotonic).</summary>
    public long ReceivedBytes { get; private set; }

    /// <summary>Highest boundary the target reported durable (verified closure), possibly not yet published.</summary>
    public CommitToken DurableBoundary { get; private set; }

    /// <summary>True while the last attempt could not reach the target.</summary>
    public bool Offline { get; private set; }

    /// <summary>Source bytes held by this target's transfer pins (the unpublished history kept for it, see <c>UpdatePinnedBytes</c>).</summary>
    public long PinnedSourceBytes { get; private set; }

    /// <summary>The latest publication of this volume authority the target itself reported in its last answered query.</summary>
    public DfsBackupCheckpoint? ObservedPublished { get; private set; }

    private VolumeService Service => _volume.Service;

    private BackupTargetRecord? Record => Service.Record.BackupTargets.GetValueOrDefault(Name);

    /// <summary>Default timeout of the export protocol's small requests.</summary>
    internal TimeSpan SmallTimeout
    {
        get
        {
            TimeSpan t = _s.Timings.Tick * _s.Timings.ElectionTicksMax * 10;
            return t < TimeSpan.FromSeconds(2) ? TimeSpan.FromSeconds(2) : t;
        }
    }

    private TimeSpan IdlePoll => _s.Timings.Tick * 20;

    // ------------------------------------------------------------------------------------------------- lifecycle

    /// <summary>Starts the runner loop.</summary>
    public void Start() => _ = RunAsync();

    /// <summary>Stops the runner (its in-flight step is cancelled).</summary>
    public void Stop()
    {
        if (_stopped)
            return;
        _stopped = true;
        try
        {
            _cts.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // Already stopped.
        }

        SignalWake();
        SignalChanged();
        SignalAllStarted();
        DropConnection(_connection);
    }

    /// <summary>
    /// Wakes the runner (a sync was requested or the configuration changed). With <paramref name="resetBackoff"/> the target is
    /// tried again at once, as if reachable: unreachability budgets apply only after that attempt failed too.
    /// </summary>
    public void Wake(bool resetBackoff)
    {
        if (resetBackoff)
        {
            _backoff = TimeSpan.Zero;
            Offline = false;
        }

        SignalWake();
    }

    private void SignalWake()
    {
        _woken = true;
        _wakeWait?.Cancel();
    }

    /// <summary>Suspends automatic transfers (manual export driver) once the in-flight step finished.</summary>
    public async Task SuspendAsync()
    {
        _suspended++;
        try
        {
            await _step;
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            // The step's failure was handled by the runner.
        }
    }

    /// <summary>Resumes automatic transfers.</summary>
    public void Resume()
    {
        if (_suspended > 0)
            _suspended--;
        Wake(resetBackoff: true);
    }

    private async Task RunAsync()
    {
        CancellationToken ct = _cts.Token;
        while (!ct.IsCancellationRequested && _volume.IsCurrent)
        {
            TimeSpan wait = IdlePoll;
            try
            {
                Task<bool> step = StepAsync(ct);
                _step = step;
                if (await step)
                {
                    _backoff = TimeSpan.Zero;
                    continue;
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex) when (ex is not OutOfMemoryException)
            {
                OnFailure(ex);
                _backoff = _backoff == TimeSpan.Zero ? MinBackoff : _backoff * 2 > MaxBackoff ? MaxBackoff : _backoff * 2;
                wait = _backoff;
            }

            await WaitAsync(wait, ct);
        }

        DropConnection(_connection);
    }

    /// <summary>Waits <paramref name="wait"/> or until woken (a wake that arrived during the step ends the wait at once).</summary>
    private async Task WaitAsync(TimeSpan wait, CancellationToken ct)
    {
        if (!_woken && !ct.IsCancellationRequested)
        {
            using var delay = CancellationTokenSource.CreateLinkedTokenSource(ct);
            _wakeWait = delay;
            try
            {
                await Task.Delay(wait, _s.Time, delay.Token);
            }
            catch (OperationCanceledException)
            {
                // Woken or stopped.
            }
            finally
            {
                if (ReferenceEquals(_wakeWait, delay))
                    _wakeWait = null;
            }
        }

        _woken = false;
    }

    // ------------------------------------------------------------------------------------------------- one step

    /// <summary>One unit of work; true when something was done (the runner continues immediately).</summary>
    private async Task<bool> StepAsync(CancellationToken ct)
    {
        if (_suspended > 0)
            return false;
        if (!Service.IsReadyLeader)
        {
            using CancellationTokenSource deadline = _s.CreateDeadline();
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, deadline.Token);
            try
            {
                await Service.EnsureBackupLeaderReadyAsync(linked.Token);
            }
            catch (Exception ex) when (ex is DfsException or OperationCanceledException or IOException && !ct.IsCancellationRequested)
            {
                return false;
            }

            if (!Service.IsReadyLeader)
                return false;
        }

        if (Record is not { } record)
            return false;
        VolumeState state = Service.CommittedState;
        BackupProgressRecord? progress = state.BackupProgress.GetValueOrDefault(Name);
        List<PinRecord> pins = PinsOf(state);
        TrackPins(pins);
        UpdatePinnedBytes(state, progress, pins);

        // An abandoned lineage accumulates no source history while its target stays unreachable: automatic schedules
        // resume once the target answers again (an explicit sync request still pins its snapshot).
        bool dormant = progress?.ReseedRequired == true && Offline;
        if (pins.Count == 0)
        {
            SignalAllStarted();
            if (dormant && !await ReachableAsync(record, ct))
                return false;
            if (await ScheduleAsync(record, state, progress))
                return true;
            if (State is not (WireBackupState.Offline or WireBackupState.Failed) || LastError is null)
                SetState(progress?.ReseedRequired == true ? WireBackupState.ReseedRequired : WireBackupState.Idle);
            return false;
        }

        if (!dormant && await PinNewerSnapshotAsync(record, state, pins))
            return true;

        if (Offline && BudgetExceeded(record, pins) is { } reason)
        {
            await AbandonAsync(reason);
            return true;
        }

        await TransferAsync(record, state, progress, pins[0], ct);
        return true;
    }

    private List<PinRecord> PinsOf(VolumeState state)
        => state.Pins.Values
            .Where(p => p.Kind == PinKind.Transfer && string.Equals(p.Target, Name, StringComparison.Ordinal) && p.Root is not null && p.Snapshot is not null)
            .OrderBy(p => p.PinIndex)
            .ThenBy(p => p.Id)
            .ToList();

    private void TrackPins(List<PinRecord> pins)
    {
        long now = _s.Time.GetTimestamp();
        foreach (PinRecord pin in pins)
            _pinSeen.TryAdd(pin.Id, now);
        foreach (ulong id in _pinSeen.Keys.ToArray())
        {
            if (!pins.Any(p => p.Id == id))
                _pinSeen.Remove(id);
        }

        _startedPins.RemoveWhere(id => !pins.Any(p => p.Id == id));
    }

    /// <summary>The exceeded budget while the target is unreachable, or null.</summary>
    private string? BudgetExceeded(BackupTargetRecord record, List<PinRecord> pins)
    {
        long now = _s.Time.GetTimestamp();
        foreach (PinRecord pin in pins)
        {
            if (_pinSeen.TryGetValue(pin.Id, out long seen) && _s.Time.GetElapsedTime(seen, now) > record.MaxPinAge)
                return BackupServices.Inv($"the transfer pin of snapshot '{pin.Snapshot}' is older than the {record.MaxPinAge} budget while the target is unreachable");
        }

        if (PinnedSourceBytes > record.MaxPinnedBytes)
            return BackupServices.Inv($"{PinnedSourceBytes} source bytes are retained for the unreachable target, over the {record.MaxPinnedBytes}-byte budget");
        return null;
    }

    /// <summary>
    /// Recomputes (at most once a second) the source bytes held by this target's transfer pins: the objects of its pinned
    /// snapshots that its last published snapshot does not reference (while the source still retains that snapshot;
    /// otherwise every pinned object counts) — the unpublished history the source keeps for the target.
    /// </summary>
    private void UpdatePinnedBytes(VolumeState state, BackupProgressRecord? progress, List<PinRecord> pins)
    {
        if (pins.Count == 0)
        {
            PinnedSourceBytes = 0;
            _pinnedComputed = false;
            return;
        }

        long now = _s.Time.GetTimestamp();
        if (_pinnedComputed && _s.Time.GetElapsedTime(_pinnedAt, now) < PinnedBytesInterval)
            return;
        _pinnedComputed = true;
        _pinnedAt = now;
        var published = new HashSet<ObjectId>();
        if (progress is { ReseedRequired: false, LastSnapshot: { } last } && state.Snapshots.TryGetValue(last, out SnapshotRecord? latest)
            && latest.Id == progress.LastSnapshotId)
        {
            AddObjects(latest.Root, published);
        }

        var held = new Dictionary<ObjectId, long>();
        foreach (PinRecord pin in pins)
        {
            foreach (InodeRecord inode in pin.Root!.Values)
            {
                foreach (StreamRecord stream in inode.Streams.Values)
                {
                    if (stream.IsInline)
                        continue;
                    foreach (ExtentRef e in stream.Extents)
                    {
                        if (!published.Contains(e.Object))
                            held[e.Object] = e.ObjectLength;
                    }
                }
            }
        }

        PinnedSourceBytes = held.Values.Sum();
    }

    /// <summary>True when the target answers (a connection is established); false while it stays unreachable.</summary>
    private async Task<bool> ReachableAsync(BackupTargetRecord record, CancellationToken ct)
    {
        try
        {
            await GetConnectionAsync(record, SmallTimeout, ct);
        }
        catch (Exception ex) when (IsConnectivity(ex) && !ct.IsCancellationRequested)
        {
            return false;
        }

        Offline = false;
        return true;
    }

    /// <summary>
    /// <see cref="BackupTargetRecord.OnEverySnapshot"/>: a snapshot newer than every pinned one is pinned as soon as it exists,
    /// also while an earlier transfer is pending (its owner may delete it before its turn comes).
    /// </summary>
    private async Task<bool> PinNewerSnapshotAsync(BackupTargetRecord record, VolumeState state, List<PinRecord> pins)
    {
        if (!record.OnEverySnapshot)
            return false;
        ulong pinned = pins.Max(p => p.PinIndex);
        SnapshotRecord? next = state.Snapshots.Values.Where(s => s.BoundaryIndex > pinned).MinBy(s => s.BoundaryIndex);
        if (next is null)
            return false;
        VolumeApplyResult? result = await Service.ProposeBackupHousekeepingAsync(new RequestBackupSyncCommand(Name, next.Name, null));
        return result is { IsSuccess: true };
    }

    private static void AddObjects(ImmutableSortedDictionary<ulong, InodeRecord> root, HashSet<ObjectId> into)
    {
        foreach (InodeRecord inode in root.Values)
        {
            foreach (StreamRecord stream in inode.Streams.Values)
            {
                if (stream.IsInline)
                    continue;
                foreach (ExtentRef e in stream.Extents)
                    into.Add(e.Object);
            }
        }
    }

    // ------------------------------------------------------------------------------------------------- schedules

    /// <summary>Adds a transfer pin when a schedule asks for one; true when a pin was proposed.</summary>
    private async Task<bool> ScheduleAsync(BackupTargetRecord record, VolumeState state, BackupProgressRecord? progress)
    {
        if (record.OnEverySnapshot)
        {
            // Every snapshot after the covered boundary, in order; a reseed starts from the latest one.
            ulong covered = progress?.CoveredIndex ?? 0;
            IEnumerable<SnapshotRecord> newer = state.Snapshots.Values.Where(s => s.BoundaryIndex > covered);
            SnapshotRecord? next = progress?.ReseedRequired == true ? newer.MaxBy(s => s.BoundaryIndex) : newer.MinBy(s => s.BoundaryIndex);
            if (next is not null)
            {
                VolumeApplyResult? result = await Service.ProposeBackupHousekeepingAsync(new RequestBackupSyncCommand(Name, next.Name, null));
                return result is { IsSuccess: true };
            }
        }

        if (record.Interval is { } interval && interval > TimeSpan.Zero)
        {
            long now = _s.Time.GetTimestamp();
            if (_intervalArmed && _s.Time.GetElapsedTime(_intervalAt, now) < interval)
                return false;
            _intervalArmed = true;
            _intervalAt = now;
            if (ReferenceEquals(_lastExportedRoot, state.Inodes))
                return false;
            if (progress?.LastSnapshot is { } last && state.Snapshots.TryGetValue(last, out SnapshotRecord? lastSnapshot)
                && lastSnapshot.Id == progress.LastSnapshotId && ReferenceEquals(lastSnapshot.Root, state.Inodes))
            {
                return false;
            }

            VolumeApplyResult? result = await Service.ProposeBackupHousekeepingAsync(new RequestBackupSyncCommand(Name, null, ExportName(Name, _s.Time.GetUtcNow())));
            return result is { IsSuccess: true };
        }

        return false;
    }

    /// <summary>Name of an export snapshot created for a target without a named snapshot.</summary>
    public static string ExportName(string target, DateTimeOffset now)
        => "export-" + target + "-" + now.UtcDateTime.ToString("yyyyMMdd'T'HHmmss'.'fffffff", CultureInfo.InvariantCulture);

    // ------------------------------------------------------------------------------------------------- transfer

    private async Task TransferAsync(BackupTargetRecord record, VolumeState state, BackupProgressRecord? progress, PinRecord pin, CancellationToken ct)
    {
        ExportSnapshot snapshot = ExportSnapshot.Of(state, pin, _s.Control.State.ClusterName);

        // F17: the source transfer pin is held; the transfer of this snapshot starts.
        await _s.HitAsync(FaultStage.F17, FaultSites.SnapshotPin, Service.Name, DfsOperationKind.BackupTransfer, detail: Name, ct: ct);
        (Guid lineage, bool full, SnapshotId? baseSnapshot) = await LineageAsync(progress);
        var key = new BackupTransferKey(state.Config.ClusterId, state.Config.Id, state.Config.Epoch, lineage, snapshot.Id);
        ExportConnection connection = await GetConnectionAsync(record, SmallTimeout, ct);
        SealedManifest manifest = Seal(key, snapshot, connection.Secret);

        // The manifest travels together with BackupBegin (one round trip less; the target stages it right after the begin). Its
        // outcome is only a hint: the manifest is sent again below unless the target confirmed it complete.
        Task<BackupBeginResultMessage> beginning = CallAsync<BackupBeginResultMessage>(connection, BeginMessage(key, snapshot, baseSnapshot, full), SmallTimeout, ct);
        Task<bool> pipelined = TrySendManifestAsync(connection, key, manifest, ct);
        SignalStarted(pin.Id);
        // The pipelined manifest never throws; objects do not wait for it (it is awaited before publishing).
        BackupBeginResultMessage begin = await beginning;

        Offline = false;
        if (!begin.Accepted)
        {
            if (begin.ReseedRequired)
            {
                await AbandonAsync("the target cannot continue the incremental lineage");
                return;
            }

            throw new DfsException(DfsErrorCode.Unavailable, $"Backup target '{Name}' did not accept the transfer of snapshot '{snapshot.Name}'.");
        }

        if (begin.AlreadyPublished)
        {
            BackupRetainedSnapshotData? retained = await TryRetentionAsync(connection, snapshot, ct);
            await CompleteAsync(pin, snapshot, lineage, retained?.Retention.ExpiresUtc, retained?.Retention.Hold ?? false);
            return;
        }

        SetState(WireBackupState.Transferring);

        // F17: the inventory hint is about to be requested.
        await _s.HitAsync(FaultStage.F17, FaultSites.BackupInventory, Service.Name, DfsOperationKind.BackupTransfer, detail: Name, ct: ct);
        var missing = new List<ObjectId>();
        for (int offset = 0; offset < manifest.Manifest.Closure.Count; offset += InventoryBatch)
        {
            ObjectId[] batch = manifest.Manifest.Closure.Skip(offset).Take(InventoryBatch).Select(c => c.Id).ToArray();
            BackupInventoryResultMessage inventory = await CallAsync<BackupInventoryResultMessage>(connection, new BackupInventoryMessage(key, batch), SmallTimeout, ct);
            missing.AddRange(inventory.Missing);
        }

        await SendObjectsAsync(connection, key, manifest, missing, record, ct);
        if (!await pipelined)
            await SendManifestAsync(connection, key, manifest, ct);
        for (int round = 0; round < MaxPublishRounds; round++)
        {
            SetState(WireBackupState.Verifying);
            BackupPublishResultMessage result = await PublishAsync(connection, key, snapshot, manifest, ct);
            if (result.Published)
            {
                await CompleteAsync(pin, snapshot, lineage, result.RetentionExpiryUtc, result.Held);
                return;
            }

            if (result.ManifestIncomplete)
            {
                await SendManifestAsync(connection, key, manifest, ct);
                continue;
            }

            if (result.MissingObjects.Count == 0)
                break;

            // The target's own verification found gaps (a false inventory hint, lost or rejected objects): resend them.
            SetState(WireBackupState.Transferring);
            await SendObjectsAsync(connection, key, manifest, result.MissingObjects, record, ct);
        }

        throw new DfsException(DfsErrorCode.Integrity, $"Backup target '{Name}' refused to publish snapshot '{snapshot.Name}' after {MaxPublishRounds} attempts.");
    }

    internal static BackupBeginMessage BeginMessage(BackupTransferKey key, ExportSnapshot snapshot, SnapshotId? baseSnapshot, bool full)
        => new(key, snapshot.ClusterName, snapshot.VolumeName, snapshot.Name, snapshot.Boundary, snapshot.Record.CreatedUtc, snapshot.Record.Label,
            snapshot.Adapters, snapshot.Record.Retention, baseSnapshot, full);

    /// <summary>The lineage of the next transfer: a new one (recorded first) for the first transfer, a full reseed after abandonment.</summary>
    internal async Task<(Guid Lineage, bool Full, SnapshotId? Base)> LineageAsync(BackupProgressRecord? progress)
    {
        if (progress is null)
        {
            Guid lineage = NewLineage();
            var record = new BackupProgressRecord(Name, null, default, 0, null, false, false, lineage, _s.Time.GetUtcNow());
            VolumeApplyResult? result = await Service.ProposeBackupHousekeepingAsync(new RecordBackupProgressCommand(record));
            if (result is not { IsSuccess: true })
                throw new DfsUnavailableException(DfsErrorCode.Unavailable, "The backup lineage could not be recorded.");
            BackupProgressRecord applied = Service.CommittedState.BackupProgress.GetValueOrDefault(Name) ?? record;
            return (applied.Lineage, true, null);
        }

        if (progress.ReseedRequired || progress.LastSnapshot is null)
            return (progress.Lineage, true, null);
        return (progress.Lineage, false, progress.LastSnapshotId);
    }

    private Guid NewLineage()
    {
        Span<byte> bytes = stackalloc byte[16];
        _s.Random.Fill(bytes);
        return new Guid(bytes);
    }

    /// <summary>Abandons the lineage: pins released and reseed required (replicated), so source GC is never held by the target.</summary>
    private async Task AbandonAsync(string reason)
    {
        VolumeApplyResult? result = await Service.ProposeBackupHousekeepingAsync(new AbandonTransferLineageCommand(Name, NewLineage()));
        if (result is not { IsSuccess: true })
            throw new DfsUnavailableException(DfsErrorCode.Unavailable, "The backup lineage could not be abandoned.");
        _pinSeen.Clear();
        PinnedSourceBytes = 0;
        _pinnedComputed = false;
        _sealed = null;
        LastError = "Lineage abandoned: " + reason + ".";
        SetState(WireBackupState.ReseedRequired);
        _s.Warn("backup", BackupServices.Inv($"Volume {Service.Name}: backup target '{Name}' lineage abandoned ({reason}); the next transfer is a full reseed."));
    }

    /// <summary>Records the publication and releases the transfer pin.</summary>
    internal async Task CompleteAsync(PinRecord pin, ExportSnapshot snapshot, Guid lineage, DateTimeOffset? expiry, bool held)
    {
        var progress = new BackupProgressRecord(Name, snapshot.Name, snapshot.Id, snapshot.Record.BoundaryIndex, expiry, held, false, lineage, _s.Time.GetUtcNow());
        VolumeApplyResult? recorded = await Service.ProposeBackupHousekeepingAsync(new RecordBackupProgressCommand(progress));
        if (recorded is not { IsSuccess: true })
            throw new DfsUnavailableException(DfsErrorCode.Unavailable, "The backup progress could not be recorded.");
        await Service.ProposeBackupHousekeepingAsync(new ReleasePinCommand(pin.Id));
        if (!DurableBoundary.IsComparableWith(snapshot.Boundary) || DurableBoundary.CompareTo(snapshot.Boundary) < 0)
            DurableBoundary = snapshot.Boundary;
        _lastExportedRoot = pin.Root;
        _pinSeen.Remove(pin.Id);
        _sealed = null;
        Offline = false;
        LastError = null;
        SetState(WireBackupState.Idle);
        _s.Debug("backup", BackupServices.Inv($"Volume {Service.Name}: snapshot '{snapshot.Name}' published on backup target '{Name}'."));
    }

    // ------------------------------------------------------------------------------------------------- objects

    private async Task SendObjectsAsync(ExportConnection connection, BackupTransferKey key, SealedManifest manifest, IEnumerable<ObjectId> ids,
        BackupTargetRecord record, CancellationToken ct)
    {
        var queue = new Queue<ExtentRef>();
        foreach (ObjectId id in ids.Distinct())
        {
            if (!manifest.Extents.TryGetValue(id, out ExtentRef? extent))
                throw new DfsIntegrityException(id, $"Backup target '{Name}' asked for object {id}, which is not part of the snapshot.");
            queue.Enqueue(extent);
        }

        // At most MaxConcurrentObjects workers drain the queue; the first failure stops the others.
        using var failed = CancellationTokenSource.CreateLinkedTokenSource(ct);
        Exception? failure = null;
        var workers = new Task[Math.Min(Math.Max(1, record.MaxConcurrentObjects), queue.Count)];
        for (int i = 0; i < workers.Length; i++)
            workers[i] = WorkAsync();
        foreach (Task worker in workers)
            await worker;
        if (failure is not null)
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(failure).Throw();

        async Task WorkAsync()
        {
            while (!failed.IsCancellationRequested && queue.TryDequeue(out ExtentRef? extent))
            {
                try
                {
                    await SendObjectAsync(connection, key, extent, record, failed.Token);
                }
                catch (Exception ex) when (ex is not OutOfMemoryException)
                {
                    failure ??= ex;
                    failed.Cancel();
                    return;
                }
            }
        }
    }

    private async Task SendObjectAsync(ExportConnection connection, BackupTransferKey key, ExtentRef extent, BackupTargetRecord record, CancellationToken ct)
    {
        await RateLimitAsync(extent.ObjectLength, record.MaxBytesPerSecond, ct);
        byte[] data = await Service.ReadObjectForBackupAsync(extent, ct);
        _s.Metrics.BackupSent(data.Length);
        ReceivedBytes += data.Length;
        SignalChanged();
        await CallAsync<BackupPutObjectResultMessage>(connection,
            new BackupPutObjectMessage(key, extent.Object, extent.Checksum, extent.ObjectLength, extent.Encoding, data), PutTimeout(data.Length), ct);
    }

    internal TimeSpan PutTimeout(long bytes) => SmallTimeout + TimeSpan.FromSeconds((double)bytes / MinPutBytesPerSecond);

    /// <summary>Waits until <paramref name="bytes"/> may be sent under <paramref name="rate"/> bytes per second.</summary>
    private async Task RateLimitAsync(long bytes, long rate, CancellationToken ct)
    {
        if (rate <= 0 || rate == long.MaxValue)
            return;
        long now = _s.Time.GetTimestamp();
        while (_rateNext > now)
        {
            TimeSpan wait = _s.Time.GetElapsedTime(now, _rateNext);
            await Task.Delay(wait > TimeSpan.FromMilliseconds(1) ? wait : TimeSpan.FromMilliseconds(1), _s.Time, ct);
            now = _s.Time.GetTimestamp();
        }

        long cost = (long)Math.Ceiling((double)bytes * _s.Time.TimestampFrequency / rate);
        _rateNext = Math.Max(now, _rateNext) + cost;
    }

    // ------------------------------------------------------------------------------------------------- manifest and publication

    private SealedManifest Seal(BackupTransferKey key, ExportSnapshot snapshot, string secret)
    {
        if (_sealed is { } cached && cached.Key == key)
            return cached;
        BackupManifest manifest = snapshot.BuildManifest(key.LineageId, Service.Record);
        byte[] sealedBytes = BackupManifest.Seal(manifest.Encode(), BackupManifest.TransitKey(secret, key), key, _s.Random);
        _sealed = new SealedManifest(key, manifest, sealedBytes, BackupManifest.Checksum(sealedBytes), ExtentsOf(snapshot.Record.Root));
        return _sealed;
    }

    /// <summary>One extent per object of a snapshot root (location hints for verified reads).</summary>
    public static Dictionary<ObjectId, ExtentRef> ExtentsOf(ImmutableSortedDictionary<ulong, InodeRecord> root)
    {
        var map = new Dictionary<ObjectId, ExtentRef>();
        foreach (InodeRecord inode in BackupManifest.Tree(root))
        {
            foreach (StreamRecord stream in inode.Streams.Values)
            {
                if (stream.IsInline)
                    continue;
                foreach (ExtentRef e in stream.Extents)
                    map.TryAdd(e.Object, e);
            }
        }

        return map;
    }

    private async Task SendManifestAsync(ExportConnection connection, BackupTransferKey key, SealedManifest manifest, CancellationToken ct)
    {
        long total = manifest.Sealed.Length;
        long offset = 0;
        for (int sends = 0; sends < 4 + (2 * (int)(total / ManifestChunk)); sends++)
        {
            int take = (int)Math.Min(ManifestChunk, total - offset);
            bool last = offset + take == total;
            var message = new BackupPutManifestMessage(key, offset, total, manifest.Checksum, manifest.Sealed.AsMemory((int)offset, take), last);
            BackupPutManifestResultMessage result = await CallAsync<BackupPutManifestResultMessage>(connection, message, SmallTimeout, ct);
            if (result.Complete)
                return;
            if (result.NextOffset < 0 || result.NextOffset > total)
                throw new DfsProtocolException($"Backup target '{Name}' asked for manifest offset {result.NextOffset} of {total}.");
            offset = result.NextOffset;
            if (offset == total)
                throw new DfsIntegrityException($"Backup target '{Name}' did not accept the complete manifest.");
        }

        throw new DfsProtocolException($"Backup target '{Name}' did not make progress receiving the manifest.");
    }

    /// <summary>Sends the manifest; false (never an exception) when the target did not confirm it complete.</summary>
    private async Task<bool> TrySendManifestAsync(ExportConnection connection, BackupTransferKey key, SealedManifest manifest, CancellationToken ct)
    {
        try
        {
            await SendManifestAsync(connection, key, manifest, ct);
            return true;
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            return false;
        }
    }

    /// <summary>Sends <c>BackupPublish</c>; while it is in flight, polls the target for its durable boundary (BAK-016).</summary>
    private async Task<BackupPublishResultMessage> PublishAsync(ExportConnection connection, BackupTransferKey key, ExportSnapshot snapshot,
        SealedManifest manifest, CancellationToken ct)
    {
        var message = new BackupPublishMessage(key, snapshot.Name, snapshot.Boundary, manifest.Checksum, manifest.Sealed.Length, snapshot.Record.Retention);
        Task<BackupPublishResultMessage> publish = CallAsync<BackupPublishResultMessage>(connection, message, PublishTimeout, ct);
        using var polling = CancellationTokenSource.CreateLinkedTokenSource(ct);
        Task poller = PollDurableAsync(connection, snapshot, polling.Token);
        try
        {
            return await publish;
        }
        finally
        {
            polling.Cancel();
            await poller;
        }
    }

    /// <summary>Polls the target for the durable boundary of an in-flight publication until cancelled; never throws.</summary>
    private async Task PollDurableAsync(ExportConnection connection, ExportSnapshot snapshot, CancellationToken ct)
    {
        TimeSpan poll = _s.Timings.Tick * 20;
        try
        {
            while (true)
            {
                await Task.Delay(poll, _s.Time, ct);
                if (await TryRetentionAsync(connection, snapshot, ct) is { } durable && durable.Covered == snapshot.Boundary && !ct.IsCancellationRequested)
                {
                    if (!DurableBoundary.IsComparableWith(snapshot.Boundary) || DurableBoundary.CompareTo(snapshot.Boundary) < 0)
                        DurableBoundary = snapshot.Boundary;
                    SetState(WireBackupState.Publishing);
                }
            }
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            // Cancelled once the publication was answered, or the target cannot answer queries: the publication decides.
        }
    }

    /// <summary>The target's record of <paramref name="snapshot"/> (published or verified-staged), or null.</summary>
    private async Task<BackupRetainedSnapshotData?> TryRetentionAsync(ExportConnection connection, ExportSnapshot snapshot, CancellationToken ct)
    {
        try
        {
            BackupRetentionResultMessage result = await CallAsync<BackupRetentionResultMessage>(connection,
                new BackupRetentionMessage(snapshot.ClusterId, snapshot.VolumeName, snapshot.Name, null), SmallTimeout, ct, dropOnTimeout: false);
            return result.Snapshots.LastOrDefault(s => s.Snapshot == snapshot.Id);
        }
        catch (Exception ex) when (IsTransient(ex) && !ct.IsCancellationRequested)
        {
            return null;
        }
    }

    // ------------------------------------------------------------------------------------------------- status and barriers

    /// <summary>Status of the target (progress from the replicated state, transfer state from this leader).</summary>
    public BackupStatusData Status(VolumeState state) => BuildStatus(state, Name, this, _s.Time.GetUtcNow());

    /// <summary>Status of a target from the replicated state alone (no runner on this node).</summary>
    public static BackupStatusData BuildStatus(VolumeState state, string target, TargetExporter? runner, DateTimeOffset now)
    {
        BackupProgressRecord? progress = state.BackupProgress.GetValueOrDefault(target);
        CommitToken published = progress?.LastSnapshot is not null ? state.TokenAt(progress.CoveredIndex) : CommitToken.None;
        DfsBackupCheckpoint? latest = ProgressCheckpoint(state, target);

        // A publication the target reported before the exporter recorded it (both describe the same authority).
        if (runner?.ObservedPublished is { } observed && observed.CoveredCommit.VolumeId == state.Config.Id && observed.CoveredCommit.VolumeEpoch == state.Config.Epoch
            && (published.IsNone || observed.CoveredCommit.CompareTo(published) > 0))
        {
            published = observed.CoveredCommit;
            latest = observed;
        }

        CommitToken durable = published;
        if (runner is not null && !runner.DurableBoundary.IsNone && runner.DurableBoundary.VolumeId == state.Config.Id
            && runner.DurableBoundary.VolumeEpoch == state.Config.Epoch && (durable.IsNone || runner.DurableBoundary.CompareTo(durable) > 0))
        {
            durable = runner.DurableBoundary;
        }

        TimeSpan? age = null;
        if (latest is not null)
        {
            DateTimeOffset at = state.Snapshots.TryGetValue(latest.SnapshotName, out SnapshotRecord? snapshot) && snapshot.Id == latest.SnapshotId
                ? snapshot.CreatedUtc
                : progress?.UpdatedUtc ?? now;
            age = now - at;
            if (age < TimeSpan.Zero)
                age = TimeSpan.Zero;
        }

        WireBackupState st = runner?.State ?? WireBackupState.Idle;
        if (st == WireBackupState.Idle && progress?.ReseedRequired == true)
            st = WireBackupState.ReseedRequired;
        return new BackupStatusData(target, st, runner?.ReceivedBytes ?? 0, durable, published, latest, age, progress?.ReseedRequired ?? false,
            runner?.PinnedSourceBytes ?? 0, runner?.LastError, progress?.Lineage ?? Guid.Empty);
    }

    /// <summary>The latest publication recorded in the volume's backup progress, as a checkpoint.</summary>
    public static DfsBackupCheckpoint? ProgressCheckpoint(VolumeState state, string target)
    {
        BackupProgressRecord? progress = state.BackupProgress.GetValueOrDefault(target);
        if (progress?.LastSnapshot is not { } name)
            return null;
        return new DfsBackupCheckpoint(target, progress.LastSnapshotId, name, state.TokenAt(progress.CoveredIndex), progress.RetentionExpiryUtc, progress.Held,
            false);
    }

    /// <summary>
    /// <c>BackupAwait</c>: waits (at most <paramref name="timeout"/>) until the target itself reports a published snapshot of this
    /// volume authority covering <paramref name="commit"/> (the first such checkpoint) or, for an exact barrier, the snapshot
    /// itself durably retained. Never changes live state.
    /// </summary>
    public async Task<BackupAwaitResultMessage> AwaitAsync(CommitToken? commit, SnapshotId? exactId, string? exactName, TimeSpan timeout, CancellationToken ct)
    {
        long start = _s.Time.GetTimestamp();
        while (true)
        {
            if (!_volume.IsCurrent)
                throw Service.Group.NotLeader();
            CancellationToken changed = _changed.Token;
            TimeSpan left = timeout - _s.Time.GetElapsedTime(start);
            IReadOnlyList<BackupRetainedSnapshotData>? retained = null;
            if (left > TimeSpan.Zero && Record is { } record)
                retained = await TryQueryRetainedAsync(record, left < SmallTimeout ? left : SmallTimeout, ct);
            VolumeState state = Service.CommittedState;
            (DfsBackupCheckpoint? checkpoint, DfsBackupCheckpoint? last) = Evaluate(state, retained, commit, exactId, exactName);
            if (retained is not null)
                ObservedPublished = last;
            if (checkpoint is not null)
                return new BackupAwaitResultMessage(true, checkpoint, last);
            left = timeout - _s.Time.GetElapsedTime(start);
            if (left <= TimeSpan.Zero)
                return new BackupAwaitResultMessage(false, null, last);
            TimeSpan poll = _s.Timings.Tick * 50;
            using (var delay = CancellationTokenSource.CreateLinkedTokenSource(ct, changed))
            {
                try
                {
                    await Task.Delay(left < poll ? left : poll, _s.Time, delay.Token);
                }
                catch (OperationCanceledException) when (!ct.IsCancellationRequested)
                {
                    // The runner's state changed: evaluate again.
                }
            }

            ct.ThrowIfCancellationRequested();
        }
    }

    private (DfsBackupCheckpoint? Checkpoint, DfsBackupCheckpoint? Last) Evaluate(VolumeState state, IReadOnlyList<BackupRetainedSnapshotData>? retained,
        CommitToken? commit, SnapshotId? exactId, string? exactName)
    {
        if (retained is null)
            return (null, ProgressCheckpoint(state, Name));
        VolumeConfig config = state.Config;
        List<BackupRetainedSnapshotData> published = retained
            .Where(r => r.Published && r.Covered.ClusterId == config.ClusterId && r.Covered.VolumeId == config.Id && r.Covered.VolumeEpoch == config.Epoch)
            .OrderBy(r => r.Covered.Sequence)
            .ToList();
        DfsBackupCheckpoint? last = published.Count > 0 ? ToCheckpoint(published[^1], false) : null;
        BackupRetainedSnapshotData? match = null;
        bool exact = false;
        if (commit is { } c)
        {
            match = published.FirstOrDefault(r => r.Covered.Sequence >= c.Sequence);
        }
        else if (exactId is { } id)
        {
            match = published.LastOrDefault(r => r.Snapshot == id);
            exact = true;
        }
        else if (exactName is not null)
        {
            match = published.LastOrDefault(r => string.Equals(r.SnapshotName, exactName, StringComparison.Ordinal));
            exact = true;
        }

        return (match is null ? null : ToCheckpoint(match, exact), last);
    }

    private DfsBackupCheckpoint ToCheckpoint(BackupRetainedSnapshotData r, bool exact)
        => new(Name, r.Snapshot, r.SnapshotName, r.Covered, r.Retention.ExpiresUtc, r.Retention.Hold, exact);

    private async Task<IReadOnlyList<BackupRetainedSnapshotData>?> TryQueryRetainedAsync(BackupTargetRecord record, TimeSpan timeout, CancellationToken ct)
    {
        using var deadline = new CancellationTokenSource(timeout, _s.Time);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, deadline.Token);
        try
        {
            ExportConnection connection = await GetConnectionAsync(record, timeout, linked.Token);
            BackupRetentionResultMessage result = await CallAsync<BackupRetentionResultMessage>(connection,
                new BackupRetentionMessage(Service.CommittedState.Config.ClusterId, Service.Name, null, null), timeout, linked.Token, dropOnTimeout: false);
            return result.Snapshots;
        }
        catch (Exception ex) when ((IsTransient(ex) || ex is OperationCanceledException) && !ct.IsCancellationRequested)
        {
            return null;
        }
    }

    // ------------------------------------------------------------------------------------------------- connections

    /// <summary>The authenticated connection to the target (shared by the runner and barrier queries).</summary>
    public async Task<ExportConnection> GetConnectionAsync(BackupTargetRecord record, TimeSpan timeout, CancellationToken ct)
    {
        if (_connection is { } current && current.Raw.IsOpen && current.Matches(record))
            return current;
        if (_connection is { } stale)
            DropConnection(stale);
        Task<ExportConnection> connecting = _connecting ??= ConnectAsync(record, timeout);
        try
        {
            return await connecting.WaitAsync(ct);
        }
        finally
        {
            if (connecting.IsCompleted && ReferenceEquals(_connecting, connecting))
                _connecting = null;
        }
    }

    private async Task<ExportConnection> ConnectAsync(BackupTargetRecord record, TimeSpan timeout)
    {
        string secret = BackupTargetControl.UnwrapExportSecret(_s.Keys.StorageKek, Service.Record.Id, Name, record.WrappedExportSecret.Span);
        var credentials = new DfsCredentials(record.ExportPrincipal, secret);
        Exception? last = null;
        foreach (string endpoint in record.Endpoints)
        {
            if (_cts.IsCancellationRequested)
                break;
            IRpcConnection? raw = null;
            using var deadline = new CancellationTokenSource(timeout, _s.Time);
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token, deadline.Token);
            try
            {
                raw = await _volume.Owner.Transport.ConnectAsync(endpoint, linked.Token);
                await HandshakeClient.AuthenticatePrincipalAsync(raw, _s.ClusterId, _s.NodeName, credentials, _s.Random, timeout, PeerRole.Backup, linked.Token);
                var connection = new ExportConnection(raw, endpoint, record.ExportPrincipal, secret, record.WrappedExportSecret);
                if (_stopped)
                {
                    DropConnection(connection);
                    throw new OperationCanceledException(_cts.Token);
                }

                _connection = connection;
                return connection;
            }
            catch (Exception ex) when (IsTransient(ex) || ex is OperationCanceledException or DfsAuthenticationException or UnauthorizedAccessException)
            {
                last = ex is OperationCanceledException && deadline.IsCancellationRequested
                    ? new TimeoutException($"Connecting to backup target '{Name}' at {endpoint} timed out.", ex)
                    : ex;
                if (raw is not null)
                    await DisposeQuietlyAsync(raw);
            }
        }

        throw last ?? new DfsUnavailableException(DfsErrorCode.Unavailable, $"Backup target '{Name}' has no endpoint.");
    }

    /// <summary>
    /// Sends a request on an export connection; an error reply is thrown as its exception. A broken connection is dropped (the
    /// next request reconnects), and so is one whose request timed out — a silently dead link — unless
    /// <paramref name="dropOnTimeout"/> is false: a query bounded by a caller's short deadline proves nothing about the link
    /// and must not tear down the transfer sharing it.
    /// </summary>
    public async Task<T> CallAsync<T>(ExportConnection connection, IWireMessage request, TimeSpan timeout, CancellationToken ct, bool dropOnTimeout = true)
        where T : class, IWireMessage
    {
        ReadOnlyMemory<byte> reply;
        try
        {
            reply = await connection.Raw.CallAsync(MessageCodec.Encode(request), timeout, ct);
        }
        catch (Exception ex) when (ex is RpcConnectionException or IOException or ObjectDisposedException || (ex is TimeoutException && dropOnTimeout))
        {
            DropConnection(connection);
            throw;
        }

        IWireMessage message = MessageCodec.Decode(reply.Span);
        if (message is ErrorReplyMessage error)
            throw error.ToException();
        return message as T ?? throw new DfsProtocolException($"Backup target '{Name}' answered {request.Type} with {message.Type}.");
    }

    private void DropConnection(ExportConnection? connection)
    {
        if (connection is null)
            return;
        if (ReferenceEquals(_connection, connection))
            _connection = null;
        _ = DisposeQuietlyAsync(connection.Raw);
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

    // ------------------------------------------------------------------------------------------------- state

    /// <summary>
    /// Completes once this runner sent the first requests of a transfer of <paramref name="pin"/> (or an attempt ended), at the
    /// latest after <paramref name="timeout"/>: a sync request is answered once its transfer is under way.
    /// </summary>
    public async Task WaitForStartAsync(ulong pin, TimeSpan timeout)
    {
        if (_stopped || _startedPins.Contains(pin))
            return;
        if (!_startWaiters.TryGetValue(pin, out CancellationTokenSource? started))
            _startWaiters[pin] = started = new CancellationTokenSource();
        try
        {
            await Task.Delay(timeout, _s.Time, started.Token);
        }
        catch (OperationCanceledException)
        {
            // Started (or the attempt ended).
        }
    }

    private void SignalStarted(ulong pin)
    {
        _startedPins.Add(pin);
        if (_startWaiters.Remove(pin, out CancellationTokenSource? started))
            Release(started);
    }

    private void SignalAllStarted()
    {
        CancellationTokenSource[] waiters = [.. _startWaiters.Values];
        _startWaiters.Clear();
        foreach (CancellationTokenSource waiter in waiters)
            Release(waiter);
    }

    private static void Release(CancellationTokenSource signal)
    {
        signal.Cancel();
        signal.Dispose();
    }

    private void OnFailure(Exception ex)
    {
        SignalAllStarted();
        Offline = IsConnectivity(ex);
        LastError = ex.Message;
        SetState(Offline ? WireBackupState.Offline : WireBackupState.Failed);
        _s.Debug("backup", BackupServices.Inv($"Volume {Service.Name}: transfer to backup target '{Name}' failed ({DfsErrors.ToCode(ex)}): {ex.Message}"));
    }

    private void SetState(WireBackupState state)
    {
        if (State == state)
            return;
        State = state;
        SignalChanged();
    }

    private void SignalChanged()
    {
        CancellationTokenSource changed = _changed;
        _changed = new CancellationTokenSource();
        Release(changed);
    }

    /// <summary>True for failures that mean the target could not be reached (budgets apply while it stays so).</summary>
    private static bool IsConnectivity(Exception ex)
    {
        if (ex is DfsException dfs)
            return dfs.ErrorCode is DfsErrorCode.Unavailable or DfsErrorCode.NodeUnavailable or DfsErrorCode.Timeout or DfsErrorCode.NotLeader;
        return ex is RpcConnectionException or TimeoutException or IOException or OperationCanceledException;
    }

    private static bool IsTransient(Exception ex)
        => ex is DfsException or IOException or TimeoutException or RpcConnectionException or ObjectDisposedException or UnauthorizedAccessException;

    /// <summary>A snapshot pinned for transfer, described identically by every leader.</summary>
    public sealed class ExportSnapshot
    {
        private ExportSnapshot(SnapshotRecord record, CommitToken boundary, string clusterName, Guid clusterId, string volumeName)
        {
            Record = record;
            Boundary = boundary;
            ClusterName = clusterName;
            ClusterId = clusterId;
            VolumeName = volumeName;
        }

        public SnapshotRecord Record { get; }

        public SnapshotId Id => Record.Id;

        public string Name => Record.Name;

        public CommitToken Boundary { get; }

        public string ClusterName { get; }

        public Guid ClusterId { get; }

        public string VolumeName { get; }

        public IReadOnlyList<AdapterRecordData> Adapters => Record.Adapters.Select(a => new AdapterRecordData(a.Id, a.Version, a.RestoreRequirements)).ToArray();

        /// <summary>The snapshot a transfer pin protects (the source record's metadata while it exists, the pin's otherwise).</summary>
        public static ExportSnapshot Of(VolumeState state, PinRecord pin, string clusterName)
        {
            string name = pin.Snapshot ?? throw new ArgumentException("A transfer pin names its snapshot.", nameof(pin));
            ulong boundary = pin.PinIndex;
            SnapshotRecord? source = state.Snapshots.TryGetValue(name, out SnapshotRecord? found) && found.BoundaryIndex == boundary ? found : null;
            DateTimeOffset created = source?.CreatedUtc ?? VolumeStateMachine.TransferCreatedUtc(pin) ?? DateTimeOffset.UnixEpoch;
            var record = new SnapshotRecord(VolumeStateMachine.SnapshotIdAt(state.Config, boundary), name, boundary,
                source?.Label ?? DfsConsistencyLabel.FilesystemConsistent, source?.Adapters ?? ImmutableList<AdapterRecord>.Empty, created,
                source?.ExpiresUtc, source?.Hold ?? false, pin.Root ?? throw new ArgumentException("A transfer pin keeps its root.", nameof(pin)));
            return new ExportSnapshot(record, state.TokenAt(boundary), clusterName, state.Config.ClusterId, state.Config.Name);
        }

        /// <summary>The manifest of this snapshot under <paramref name="lineage"/>.</summary>
        public BackupManifest BuildManifest(Guid lineage, VolumeRecord volume)
        {
            var config = new BackupVolumeConfig(volume.Creation.Metadata, volume.DefaultPolicy, volume.NamedPolicies, volume.Creation.InlineThreshold,
                volume.Creation.Limits, volume.StoragePool, volume.Creation.EnforceTraverseChecking);
            return BackupManifest.FromSnapshot(ClusterId, ClusterName, Boundary.VolumeId, VolumeName, Boundary.VolumeEpoch, lineage, Record, Boundary, config);
        }
    }

    /// <summary>A manifest sealed for one transfer key (kept so retries resend the identical bytes).</summary>
    public sealed record SealedManifest(BackupTransferKey Key, BackupManifest Manifest, byte[] Sealed, UInt128 Checksum, Dictionary<ObjectId, ExtentRef> Extents);
}

/// <summary>An authenticated connection of an exporter to its backup target.</summary>
internal sealed record ExportConnection(IRpcConnection Raw, string Endpoint, string Principal, string Secret, ReadOnlyMemory<byte> WrappedSecret)
{
    /// <summary>True when the connection was opened with the registration's current endpoints and credentials.</summary>
    public bool Matches(BackupTargetRecord record)
        => record.Endpoints.Contains(Endpoint, StringComparer.Ordinal) && string.Equals(record.ExportPrincipal, Principal, StringComparison.Ordinal)
            && record.WrappedExportSecret.Span.SequenceEqual(WrappedSecret.Span);
}
