using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Control;
using Tedd.Tdfs.Server.Runtime;
using Tedd.Tdfs.Server.Volumes.Apply;
using Tedd.Tdfs.Server.Volumes.Commands;
using Tedd.Tdfs.Server.Volumes.Service;
using Tedd.Tdfs.Server.Volumes.State;

namespace Tedd.Tdfs.Server.Backup;

/// <summary>One object as sent by <see cref="ManualBackupTransfer.SendObjectAsync"/> (the fields of <c>BackupPutObject</c>).</summary>
/// <param name="Object">Object id.</param>
/// <param name="Checksum">Declared checksum.</param>
/// <param name="Length">Declared length.</param>
/// <param name="Encoding">Declared encoding.</param>
/// <param name="Data">Payload.</param>
internal sealed record ManualBackupObject(ObjectId Object, UInt128 Checksum, long Length, byte Encoding, ReadOnlyMemory<byte> Data);

/// <summary>Outcome of <see cref="ManualBackupTransfer.PublishAsync"/>.</summary>
/// <param name="Error">None when published, otherwise the refusal.</param>
/// <param name="Published">True when the target published and acknowledged.</param>
/// <param name="Missing">Closure objects the target reported missing.</param>
/// <param name="Checkpoint">The acknowledgement as an exact checkpoint.</param>
internal sealed record ManualPublishOutcome(DfsErrorCode Error, bool Published, IReadOnlyList<ObjectId> Missing, DfsBackupCheckpoint? Checkpoint);

/// <summary>
/// A step-by-step export of one snapshot to one backup target, driven by a test harness (BAK-006, -007, -009, -010) through the
/// same messages, credentials, encryption, lineage and progress recording as the automatic exporter, whose runner for the
/// target is suspended meanwhile. Every member runs on the volume leader's loop.
/// </summary>
internal sealed class ManualBackupTransfer
{
    private const int ManifestChunk = 256 * 1024;

    private readonly NodeServices _s;
    private readonly VolumeService _service;
    private readonly TargetExporter _runner;
    private readonly PinRecord _pin;
    private readonly TargetExporter.ExportSnapshot _snapshot;
    private readonly BackupManifest _manifest;
    private readonly Dictionary<ObjectId, ExtentRef> _extents;
    private readonly bool _full;
    private readonly SnapshotId? _base;
    private (UInt128 Checksum, long Length)? _lastManifest;
    private bool _published;
    private bool _disposed;

    private ManualBackupTransfer(NodeServices services, VolumeService service, TargetExporter runner, PinRecord pin, TargetExporter.ExportSnapshot snapshot,
        Guid lineage, bool full, SnapshotId? baseSnapshot)
    {
        _s = services;
        _service = service;
        _runner = runner;
        _pin = pin;
        _snapshot = snapshot;
        _full = full;
        _base = baseSnapshot;
        Lineage = lineage;
        VolumeState state = service.CommittedState;
        Key = new BackupTransferKey(state.Config.ClusterId, state.Config.Id, state.Config.Epoch, lineage, snapshot.Id);
        _manifest = snapshot.BuildManifest(lineage, service.Record);
        _extents = TargetExporter.ExtentsOf(snapshot.Record.Root);
    }

    public string Volume => _service.Name;

    public string Target => _runner.Name;

    public string SnapshotName => _snapshot.Name;

    public SnapshotId SnapshotId => _snapshot.Id;

    public CommitToken Boundary => _snapshot.Boundary;

    public Guid Lineage { get; }

    public BackupTransferKey Key { get; }

    /// <summary>The snapshot's dependency closure (sorted, distinct).</summary>
    public IReadOnlyList<ObjectId> Closure => _manifest.ClosureIds;

    /// <summary>Suspends the target's automatic exporter, pins the snapshot for the target and resolves the lineage.</summary>
    public static async Task<ManualBackupTransfer> BeginAsync(NodeServices services, VolumeService service, string target, string snapshotName)
    {
        VolumeExporter exporter = services.BackupExports?.For(service) ?? throw service.Group.NotLeader();
        TargetExporter runner = exporter.Target(target)
            ?? throw new DfsException(DfsErrorCode.BackupTargetNotFound, $"Backup target '{target}' is not registered on volume '{service.Name}'.");
        await runner.SuspendAsync();
        try
        {
            using CancellationTokenSource deadline = services.CreateDeadline();
            await service.EnsureBackupLeaderReadyAsync(deadline.Token);
            VolumeApplyResult result = await service.ProposeBackupHousekeepingAsync(new RequestBackupSyncCommand(target, snapshotName, null))
                ?? throw new DfsUnavailableException(DfsErrorCode.Unavailable, "The transfer pin could not be committed.");
            if (!result.IsSuccess)
                throw DfsErrors.ToException(result.Code, result.Message);
            PinRecord pin = result.Value as PinRecord ?? throw new DfsException(DfsErrorCode.Internal, "The transfer pin is missing.");
            VolumeState state = service.CommittedState;
            (Guid lineage, bool full, SnapshotId? baseSnapshot) = await runner.LineageAsync(state.BackupProgress.GetValueOrDefault(target));
            var snapshot = TargetExporter.ExportSnapshot.Of(state, pin, services.Control.State.ClusterName);
            return new ManualBackupTransfer(services, service, runner, pin, snapshot, lineage, full, baseSnapshot);
        }
        catch
        {
            runner.Resume();
            throw;
        }
    }

    /// <summary><c>BackupBegin</c>; None when accepted.</summary>
    public async Task<DfsErrorCode> BeginTransferAsync()
    {
        try
        {
            ExportConnection connection = await ConnectAsync();
            BackupBeginResultMessage reply = await _runner.CallAsync<BackupBeginResultMessage>(connection,
                TargetExporter.BeginMessage(Key, _snapshot, _base, _full), _runner.SmallTimeout, CancellationToken.None);
            if (reply.Accepted)
                return DfsErrorCode.None;
            return reply.ReseedRequired ? DfsErrorCode.EpochMismatch : DfsErrorCode.Unavailable;
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            return DfsErrors.ToCode(ex);
        }
    }

    /// <summary><c>BackupInventory</c> for the closure: the subset the target's hint claims to hold.</summary>
    public async Task<IReadOnlySet<ObjectId>> RequestInventoryAsync()
    {
        ExportConnection connection = await ConnectAsync();
        var missing = new HashSet<ObjectId>();
        IReadOnlyList<ObjectId> closure = Closure;
        for (int offset = 0; offset < closure.Count; offset += 4096)
        {
            BackupInventoryResultMessage reply = await _runner.CallAsync<BackupInventoryResultMessage>(connection,
                new BackupInventoryMessage(Key, closure.Skip(offset).Take(4096).ToArray()), _runner.SmallTimeout, CancellationToken.None);
            missing.UnionWith(reply.Missing);
        }

        return closure.Where(id => !missing.Contains(id)).ToHashSet();
    }

    /// <summary>Reads a closure object from a verified live replica.</summary>
    public async Task<ManualBackupObject> ReadObjectAsync(ObjectId id)
    {
        if (!_extents.TryGetValue(id, out ExtentRef? extent))
            throw new ArgumentException($"Object {id} is not part of snapshot '{SnapshotName}'.", nameof(id));
        byte[] data = await _service.ReadObjectForBackupAsync(extent, CancellationToken.None);
        return new ManualBackupObject(id, extent.Checksum, extent.ObjectLength, extent.Encoding, data);
    }

    /// <summary>Sends one closure object (optionally altered in transit); None once the target reports it durable.</summary>
    public async Task<DfsErrorCode> SendObjectAsync(ObjectId id, Func<ManualBackupObject, Task<ManualBackupObject>>? tamper)
    {
        ManualBackupObject genuine = await ReadObjectAsync(id);
        ManualBackupObject sent = tamper is null ? genuine : await tamper(genuine);
        try
        {
            ExportConnection connection = await ConnectAsync();
            BackupPutObjectResultMessage reply = await _runner.CallAsync<BackupPutObjectResultMessage>(connection,
                new BackupPutObjectMessage(Key, sent.Object, sent.Checksum, sent.Length, sent.Encoding, sent.Data), _runner.PutTimeout(sent.Data.Length),
                CancellationToken.None);
            return reply.Durable ? DfsErrorCode.None : DfsErrorCode.Unavailable;
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            return DfsErrors.ToCode(ex);
        }
    }

    /// <summary>
    /// Sends the encrypted manifest (optionally altered before sealing and/or after sealing); None once the target durably staged it.
    /// </summary>
    public async Task<DfsErrorCode> SendManifestAsync(Func<BackupManifest, BackupManifest>? alterPlaintext, Func<byte[], byte[]>? alterSealed)
    {
        try
        {
            ExportConnection connection = await ConnectAsync();
            BackupManifest manifest = alterPlaintext is null ? _manifest : alterPlaintext(_manifest);
            byte[] sealedBytes = BackupManifest.Seal(manifest.Encode(), BackupManifest.TransitKey(connection.Secret, Key), Key, _s.Random);
            if (alterSealed is not null)
                sealedBytes = alterSealed(sealedBytes);
            UInt128 checksum = BackupManifest.Checksum(sealedBytes);
            long total = sealedBytes.Length;
            long offset = 0;
            while (true)
            {
                int take = (int)Math.Min(ManifestChunk, total - offset);
                BackupPutManifestResultMessage reply = await _runner.CallAsync<BackupPutManifestResultMessage>(connection,
                    new BackupPutManifestMessage(Key, offset, total, checksum, sealedBytes.AsMemory((int)offset, take), offset + take == total), _runner.SmallTimeout,
                    CancellationToken.None);
                if (reply.Complete)
                {
                    _lastManifest = (checksum, total);
                    return DfsErrorCode.None;
                }

                if (reply.NextOffset <= offset || reply.NextOffset > total)
                    return DfsErrorCode.ProtocolError;
                offset = reply.NextOffset;
            }
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            return DfsErrors.ToCode(ex);
        }
    }

    /// <summary><c>BackupPublish</c>; a publication records the backup progress exactly like the automatic exporter.</summary>
    public async Task<ManualPublishOutcome> PublishAsync()
    {
        (UInt128 checksum, long length) = _lastManifest ?? (UInt128.Zero, 0);
        BackupPublishResultMessage reply;
        try
        {
            ExportConnection connection = await ConnectAsync();
            reply = await _runner.CallAsync<BackupPublishResultMessage>(connection,
                new BackupPublishMessage(Key, SnapshotName, Boundary, checksum, length, _snapshot.Record.Retention), TimeSpan.FromSeconds(60), CancellationToken.None);
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            return new ManualPublishOutcome(DfsErrors.ToCode(ex), false, [], null);
        }

        if (!reply.Published)
            return new ManualPublishOutcome(DfsErrorCode.Integrity, false, reply.MissingObjects, null);
        if (!_published)
        {
            await _runner.CompleteAsync(_pin, _snapshot, Lineage, reply.RetentionExpiryUtc, reply.Held);
            _published = true;
        }

        return new ManualPublishOutcome(DfsErrorCode.None, true, [],
            new DfsBackupCheckpoint(Target, SnapshotId, SnapshotName, reply.CoveredCommit, reply.RetentionExpiryUtc, reply.Held, true));
    }

    /// <summary>Releases the transfer pin when the snapshot was not published and resumes the automatic exporter.</summary>
    public async Task DisposeAsync()
    {
        if (_disposed)
            return;
        _disposed = true;
        try
        {
            if (!_published && _service.IsLeader)
                await _service.ProposeBackupHousekeepingAsync(new ReleasePinCommand(_pin.Id));
        }
        finally
        {
            _runner.Resume();
        }
    }

    private Task<ExportConnection> ConnectAsync()
    {
        BackupTargetRecord record = _service.Record.BackupTargets.GetValueOrDefault(Target)
            ?? throw new DfsException(DfsErrorCode.BackupTargetNotFound, $"Backup target '{Target}' is no longer registered.");
        return _runner.GetConnectionAsync(record, _runner.SmallTimeout, CancellationToken.None);
    }
}
