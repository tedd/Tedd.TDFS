using Tedd.Tdfs.Protocol;

namespace Tedd.Tdfs.Server.Volumes.Commands;

/// <summary>
/// Pins a snapshot for transfer to a backup target (FSD §12.2; <c>BackupRequestSync</c> and the exporter's schedules). With
/// <see cref="Snapshot"/> the named snapshot is pinned; otherwise the committed namespace at this entry is pinned as an export
/// snapshot named <see cref="ExportName"/> (no user-visible snapshot record is created). The pin belongs to the volume leader
/// (empty principal, no session) and is released by the exporter once the target published the snapshot, or by
/// <see cref="AbandonTransferLineageCommand"/>. A pin that already exists for the same target and snapshot is reused.
/// Client commands require the BackupAdmin grant (or a cluster administrator).
/// </summary>
/// <param name="Target">Backup target name.</param>
/// <param name="Snapshot">Snapshot to pin; null = pin the current committed namespace as <paramref name="ExportName"/>.</param>
/// <param name="ExportName">Name of the export snapshot created when <paramref name="Snapshot"/> is null.</param>
internal sealed record RequestBackupSyncCommand(string Target, string? Snapshot, string? ExportName) : VolumeCommand
{
    public override VolumeCommandKind Kind => VolumeCommandKind.RequestBackupSync;

    public override DfsOperationKind OperationKind => DfsOperationKind.BackupTransfer;

    internal override void WriteBody(ref WireWriter w)
    {
        w.WriteString(Target);
        w.WriteOptionalString(Snapshot);
        w.WriteOptionalString(ExportName);
    }

    internal static RequestBackupSyncCommand ReadBody(ref WireReader r) => new(r.ReadString(), r.ReadOptionalString(), r.ReadOptionalString());
}

/// <summary>
/// Forgets a removed backup target in the volume state: releases its transfer pins and drops its progress (FSD §12.1; the
/// target keeps its published snapshots under its own retention). Leader housekeeping after the registration was removed.
/// </summary>
/// <param name="Target">Backup target name.</param>
internal sealed record RemoveBackupTargetStateCommand(string Target) : VolumeCommand
{
    public override VolumeCommandKind Kind => VolumeCommandKind.RemoveBackupTargetState;

    public override DfsOperationKind OperationKind => DfsOperationKind.Admin;

    internal override void WriteBody(ref WireWriter w) => w.WriteString(Target);

    internal static RemoveBackupTargetStateCommand ReadBody(ref WireReader r) => new(r.ReadString());
}
