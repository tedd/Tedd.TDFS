using System.Collections.Immutable;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Volumes.Commands;
using Tedd.Tdfs.Server.Volumes.State;

namespace Tedd.Tdfs.Server.Volumes.Apply;

internal sealed partial class VolumeStateMachine
{
    private static (VolumeState, VolumeApplyResult) ApplyCreateSnapshot(VolumeState s, ApplyContext ctx, CreateSnapshotCommand c)
    {
        RequireGrant(ctx, DfsPermissions.Snapshot, "create snapshot");
        if (SnapshotNames.GetError(c.Name) is { } error)
            throw Fail(DfsErrorCode.InvalidArgument, error);
        if (s.Snapshots.ContainsKey(c.Name))
            throw Fail(DfsErrorCode.AlreadyExists, $"Snapshot '{c.Name}' already exists.");
        if (!Enum.IsDefined(c.Label))
            throw Fail(DfsErrorCode.InvalidArgument, $"Unknown consistency label {(int)c.Label}.");
        ImmutableArray<AdapterRecord> adapters = OrEmpty(c.Adapters);
        if (c.Label == DfsConsistencyLabel.ApplicationConsistent && adapters.Length == 0)
            throw Fail(DfsErrorCode.InvalidArgument, "An ApplicationConsistent snapshot must record the adapters that prepared it.");
        var record = new SnapshotRecord(SnapshotIdAt(s.Config, ctx.Index), c.Name, ctx.Index, c.Label, [.. adapters], ctx.Now, c.ExpiresUtc, c.Hold, s.Inodes);
        s = s with { Snapshots = s.Snapshots.Add(c.Name, record) };
        return (s, VolumeApplyResult.Ok(new CreateSnapshotResultMessage(DescribeSnapshot(s, record)), record));
    }

    private static (VolumeState, VolumeApplyResult) ApplyDeleteSnapshot(VolumeState s, ApplyContext ctx, DeleteSnapshotCommand c)
    {
        RequireGrant(ctx, DfsPermissions.Snapshot, "delete snapshot");
        if (!s.Snapshots.TryGetValue(c.Name, out SnapshotRecord? snapshot))
            throw Fail(DfsErrorCode.SnapshotNotFound, $"Snapshot '{c.Name}' does not exist.");
        if (snapshot.IsProtected(ctx.Now))
        {
            throw Fail(DfsErrorCode.RetentionProtected, snapshot.Hold
                ? $"Snapshot '{c.Name}' is under a retention hold."
                : $"Snapshot '{c.Name}' is retained until {snapshot.ExpiresUtc:O}.");
        }

        // Only the retention reference goes; readers and transfers keep their pinned roots (FSD §11.1).
        s = s with { Snapshots = s.Snapshots.Remove(c.Name) };
        return (s, VolumeApplyResult.Ok(new DeleteSnapshotResultMessage(true, s.TokenAt(ctx.Index))));
    }

    private static (VolumeState, VolumeApplyResult) ApplySetRetention(VolumeState s, ApplyContext ctx, SetSnapshotRetentionCommand c)
    {
        RequireGrant(ctx, DfsPermissions.Snapshot, "set retention");
        if (!s.Snapshots.TryGetValue(c.Name, out SnapshotRecord? snapshot))
            throw Fail(DfsErrorCode.SnapshotNotFound, $"Snapshot '{c.Name}' does not exist.");
        bool removesHold = snapshot.Hold && !c.Hold;
        bool shortens = snapshot.ExpiresUtc is { } old && (c.ExpiresUtc is not { } next || next < old);
        if ((removesHold || shortens) && (ctx.Op is not { } op || (op.Grants & DfsPermissions.RetentionAdmin) == 0))
        {
            throw Fail(DfsErrorCode.RetentionProtected,
                $"Shortening the retention of snapshot '{c.Name}' or removing its hold requires the RetentionAdmin grant.");
        }

        SnapshotRecord updated = snapshot with { ExpiresUtc = c.ExpiresUtc, Hold = c.Hold };
        s = s with { Snapshots = s.Snapshots.SetItem(c.Name, updated) };
        return (s, VolumeApplyResult.Ok(new SetRetentionResultMessage(DescribeSnapshot(s, updated), s.TokenAt(ctx.Index))));
    }

    private static (VolumeState, VolumeApplyResult) ApplyAcquirePin(VolumeState s, ApplyContext ctx, AcquirePinCommand c)
    {
        string principal = ctx.Op?.Principal ?? string.Empty;
        ulong session = 0;
        if (c.PinKind != PinKind.Transfer)
        {
            session = RequireSession(s, ctx, c.Session).Id;
            int held = ReadPinsOf(s, session);
            if (held >= s.Config.Limits.MaxPinsPerSession)
            {
                throw Fail(DfsErrorCode.LimitExceeded, $"The session already holds {held} pins.",
                    LimitDetail(nameof(DfsVolumeLimits.MaxPinsPerSession), s.Config.Limits.MaxPinsPerSession, held + 1));
            }
        }

        switch (c.PinKind)
        {
            case PinKind.SnapshotReader:
            {
                RequireGrant(ctx, DfsPermissions.Read, "open snapshot");
                if (c.Snapshot is null || !s.Snapshots.TryGetValue(c.Snapshot, out SnapshotRecord? snapshot))
                    throw Fail(DfsErrorCode.SnapshotNotFound, $"Snapshot '{c.Snapshot}' does not exist.");
                (s, PinRecord pin) = AddPin(s, PinKind.SnapshotReader, session, principal, ctx.Index, snapshot.BoundaryIndex, 0, snapshot.Name, 0,
                    string.Empty, snapshot.Root, null);
                var reply = new OpenSnapshotResultMessage(pin.Id, DescribeSnapshot(s, snapshot), TimeSpan.Zero);
                return (s, VolumeApplyResult.Ok(reply, pin));
            }

            case PinKind.Enumeration:
            {
                RequireGrant(ctx, DfsPermissions.Read, "enumerate");
                if (s.Find(c.Directory) is not { IsDirectory: true })
                    throw Fail(DfsErrorCode.DirectoryNotFound, $"Directory inode {c.Directory} does not exist.");
                (s, PinRecord pin) = AddPin(s, PinKind.Enumeration, session, principal, ctx.Index, ctx.Index, 0, null, c.Directory, c.Binding, s.Inodes, null);
                return (s, VolumeApplyResult.Ok(null, pin));
            }

            case PinKind.Transfer:
            {
                RequireGrant(ctx, DfsPermissions.BackupExport, "backup transfer");
                if (c.Snapshot is null || !s.Snapshots.TryGetValue(c.Snapshot, out SnapshotRecord? snapshot))
                    throw Fail(DfsErrorCode.SnapshotNotFound, $"Snapshot '{c.Snapshot}' does not exist.");
                (s, PinRecord pin) = AddPin(s, PinKind.Transfer, 0, principal, ctx.Index, snapshot.BoundaryIndex, 0, snapshot.Name, 0, string.Empty,
                    snapshot.Root, c.Target);
                return (s, VolumeApplyResult.Ok(null, pin));
            }

            default:
                throw Fail(DfsErrorCode.InvalidArgument, $"Pins of kind {c.PinKind} are not acquired explicitly.");
        }
    }

    private static (VolumeState, VolumeApplyResult) ApplyReleasePin(VolumeState s, ApplyContext ctx, ReleasePinCommand c)
    {
        string principal = ctx.Op?.Principal ?? string.Empty;
        if (!s.Pins.TryGetValue(c.Pin, out PinRecord? pin) || pin.Kind == PinKind.Handle)
            return (s, VolumeApplyResult.Ok(new CloseSnapshotResultMessage(false)));
        bool owner = string.Equals(pin.Principal, principal, StringComparison.Ordinal) && (pin.Session == 0 || pin.Session == ctx.Op?.RequestSession);
        if (!owner)
            return (s, VolumeApplyResult.Ok(new CloseSnapshotResultMessage(false)));
        s = s with { Pins = s.Pins.Remove(pin.Id) };
        return (s, VolumeApplyResult.Ok(new CloseSnapshotResultMessage(true), pin));
    }

    private static (VolumeState, VolumeApplyResult) ApplyExpirePins(VolumeState s, ApplyContext ctx, ExpirePinsCommand c)
    {
        var expired = new List<ulong>();
        foreach (ulong id in OrEmpty(c.Pins).Order())
        {
            if (s.Pins.TryGetValue(id, out PinRecord? pin) && pin.Kind is PinKind.SnapshotReader or PinKind.Enumeration)
            {
                s = s with { Pins = s.Pins.Remove(id) };
                expired.Add(id);
            }
        }

        _ = ctx;
        return (s, VolumeApplyResult.Ok(null, expired));
    }
}
