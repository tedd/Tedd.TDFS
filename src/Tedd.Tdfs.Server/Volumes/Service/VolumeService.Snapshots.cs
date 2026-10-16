using System.Collections.Immutable;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Runtime;
using Tedd.Tdfs.Server.Volumes.Apply;
using Tedd.Tdfs.Server.Volumes.Commands;
using Tedd.Tdfs.Server.Volumes.State;

namespace Tedd.Tdfs.Server.Volumes.Service;

internal sealed partial class VolumeService
{
    /// <summary>
    /// <c>CreateSnapshot</c> (FSD §11): with <c>MinimumCommit</c> the leader first waits until it applied that commit (same
    /// cluster/volume/epoch, else ClusterMismatch, CrossVolume or EpochMismatch). F17 is hit while the capture's pin is established.
    /// </summary>
    public async Task<IWireMessage> CreateSnapshotAsync(RpcContext context, CreateSnapshotMessage request)
    {
        string principal = context.Principal ?? string.Empty;
        RequireGrant(principal, DfsPermissions.Snapshot, "create snapshot");
        if (SnapshotNames.GetError(request.Name) is { } nameError)
            throw new ArgumentException(nameError);
        if (request.MinimumCommit is { IsNone: false } minimum)
        {
            // A token of another cluster, another volume (a restored volume is a new volume) or another authority epoch can
            // never be ordered against this volume's boundaries (SNP-010, SNP-018): the typed identity error, not a wait.
            CommitToken current = Token(0);
            if (minimum.ClusterId != current.ClusterId)
                throw new DfsClusterIdentityException($"Commit {minimum} was issued by another cluster; it cannot bound a snapshot of volume '{Name}'.");
            if (minimum.VolumeId != current.VolumeId)
                throw new DfsCrossVolumeException($"Commit {minimum} belongs to another volume; a snapshot of volume '{Name}' covers only its own commits.");
            if (minimum.VolumeEpoch != current.VolumeEpoch)
                throw new DfsEpochMismatchException($"Commit {minimum} does not belong to volume '{Name}' epoch {Record.Epoch}.");
        }

        ImmutableArray<AdapterRecord> adapters = [.. request.Adapters.Select(a => new AdapterRecord(a.Id, a.Version,
            ImmutableSortedDictionary.CreateRange(StringComparer.Ordinal, a.RestoreRequirements)))];
        return await MutateAsync(context, request, DfsOperationKind.CreateSnapshot, async (op, ct) =>
        {
            if (request.MinimumCommit is { IsNone: false } minimum2 && Group.AppliedIndex < minimum2.Sequence)
                await Group.WaitForAppliedAsync(minimum2.Sequence, ct);
            await _s.HitAsync(FaultStage.F17, FaultSites.SnapshotPin, Name, DfsOperationKind.CreateSnapshot, op.Id, detail: request.Name, ct: ct);
            return Stamp(new CreateSnapshotCommand(request.Name, request.Label, adapters, request.Retention?.ExpiresUtc, request.Retention?.Hold ?? false), op);
        });
    }

    /// <summary><c>ListSnapshots</c>: ascending boundary order (ReadIndex read).</summary>
    public async Task<IWireMessage> ListSnapshotsAsync(RpcContext context, ListSnapshotsMessage request)
    {
        RejectTransaction(request);
        string principal = context.Principal ?? string.Empty;
        RequireGrant(principal, DfsPermissions.Read, "list snapshots");
        ulong applied = await ReadBarrierAsync(context.Cancellation);
        VolumeState s = State;
        var snapshots = s.Snapshots.Values
            .Where(x => request.Name is null || string.Equals(x.Name, request.Name, StringComparison.Ordinal))
            .OrderBy(x => x.BoundaryIndex)
            .Select(x => VolumeStateMachine.DescribeSnapshot(s, x))
            .ToArray();
        return new ListSnapshotsResultMessage(snapshots, Token(applied));
    }

    /// <summary>
    /// <c>DeleteSnapshot</c>: removes the retention reference (RetentionProtected while held or unexpired). Deleting a recovery
    /// point is a security-relevant administrative action (FSD §14): the decision is audited with its outcome (SEC-016).
    /// </summary>
    public async Task<IWireMessage> DeleteSnapshotAsync(RpcContext context, DeleteSnapshotMessage request)
    {
        string principal = context.Principal ?? string.Empty;
        RequireGrant(principal, DfsPermissions.Snapshot, "delete snapshot");
        IWireMessage reply = await MutateAsync(context, request, DfsOperationKind.DeleteSnapshot,
            (op, _) => Task.FromResult<VolumeCommand>(Stamp(new DeleteSnapshotCommand(request.Name), op)), cleanup: true);
        // Answered once the record is in the audit log (read-your-writes for the deleting administrator's auditor).
        await _s.Control.ReportSecurityEventAndWaitAsync("volume:delete snapshot", reply is ErrorReplyMessage error ? "failed:" + error.Code : "ok",
            "volume=" + Name + " snapshot=" + request.Name, principal);
        return reply;
    }

    /// <summary><c>SetRetention</c>: shortening or removing a hold requires RetentionAdmin.</summary>
    public Task<IWireMessage> SetRetentionAsync(RpcContext context, SetRetentionMessage request)
    {
        RequireGrant(context.Principal ?? string.Empty, DfsPermissions.Snapshot, "set retention");
        return MutateAsync(context, request, DfsOperationKind.Admin,
            (op, _) => Task.FromResult<VolumeCommand>(Stamp(new SetSnapshotRetentionCommand(request.Name, request.Retention.ExpiresUtc, request.Retention.Hold), op)));
    }

    /// <summary><c>OpenSnapshot</c>: a committed snapshot-reader pin owned by the caller's session (renewed by <c>KeepAlive</c>).</summary>
    public async Task<IWireMessage> OpenSnapshotAsync(RpcContext context, OpenSnapshotMessage request)
    {
        RequireGrant(context.Principal ?? string.Empty, DfsPermissions.Read, "open snapshot");
        IWireMessage reply = await MutateAsync(context, request, DfsOperationKind.OpenSnapshot, async (op, ct) =>
        {
            await _s.HitAsync(FaultStage.F17, FaultSites.SnapshotPin, Name, DfsOperationKind.OpenSnapshot, op.HasOperation ? op.Id : null, detail: request.Name,
                ct: ct);
            return Stamp(new AcquirePinCommand(PinKind.SnapshotReader, request.Header.Session, request.Name, 0, string.Empty, null), op);
        }, requireOperation: false);
        if (reply is OpenSnapshotResultMessage opened)
        {
            RenewPin(opened.SnapshotPin);
            return opened with { PinDuration = _s.Timings.PinDuration };
        }

        return reply;
    }

    /// <summary><c>CloseSnapshot</c>: releases a reader or enumeration pin of the caller's session (idempotent).</summary>
    public async Task<IWireMessage> CloseSnapshotAsync(RpcContext context, CloseSnapshotMessage request)
    {
        IWireMessage reply = await MutateAsync(context, request, DfsOperationKind.OpenSnapshot,
            (op, _) => Task.FromResult<VolumeCommand>(Stamp(new ReleasePinCommand(request.SnapshotPin), op)), requireOperation: false, cleanup: true);
        _pinRenewals.Remove(request.SnapshotPin);
        return reply;
    }
}
