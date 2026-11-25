namespace Tedd.Tdfs.Server.Volumes.Commands;

internal static partial class CommandCodec
{
    /// <summary>One line per command kind (INTERNALS.md §1.2).</summary>
    private static partial void RegisterAll(Dictionary<VolumeCommandKind, CommandBodyReader> readers)
    {
        readers.Add(VolumeCommandKind.OpenSession, OpenSessionCommand.ReadBody);
        readers.Add(VolumeCommandKind.KeepAlive, KeepAliveCommand.ReadBody);
        readers.Add(VolumeCommandKind.ExpireSessions, ExpireSessionsCommand.ReadBody);
        readers.Add(VolumeCommandKind.ExpireHandles, ExpireHandlesCommand.ReadBody);
        readers.Add(VolumeCommandKind.FenceOperation, FenceOperationCommand.ReadBody);
        readers.Add(VolumeCommandKind.ReserveOperationIds, ReserveOperationIdsCommand.ReadBody);

        readers.Add(VolumeCommandKind.CreateDirectory, CreateDirectoryCommand.ReadBody);
        readers.Add(VolumeCommandKind.Move, MoveCommand.ReadBody);
        readers.Add(VolumeCommandKind.Delete, DeleteCommand.ReadBody);

        readers.Add(VolumeCommandKind.ExportOpen, ExportOpenCommand.ReadBody);
        readers.Add(VolumeCommandKind.ExportControl, ExportControlCommand.ReadBody);
        readers.Add(VolumeCommandKind.OpenHandle, OpenHandleCommand.ReadBody);
        readers.Add(VolumeCommandKind.CloseHandle, CloseHandleCommand.ReadBody);
        readers.Add(VolumeCommandKind.CreateStaging, CreateStagingCommand.ReadBody);
        readers.Add(VolumeCommandKind.CloseStaging, CloseStagingCommand.ReadBody);
        readers.Add(VolumeCommandKind.CommitFile, CommitFileCommand.ReadBody);
        readers.Add(VolumeCommandKind.WriteAll, WriteAllCommand.ReadBody);
        readers.Add(VolumeCommandKind.Append, AppendCommand.ReadBody);
        readers.Add(VolumeCommandKind.Copy, CopyCommand.ReadBody);
        readers.Add(VolumeCommandKind.ReclaimDetached, ReclaimDetachedCommand.ReadBody);

        readers.Add(VolumeCommandKind.TxBegin, TxBeginCommand.ReadBody);
        readers.Add(VolumeCommandKind.TxCommit, TxCommitCommand.ReadBody);
        readers.Add(VolumeCommandKind.TxAbort, TxAbortCommand.ReadBody);
        readers.Add(VolumeCommandKind.TxAcquireReservation, TxAcquireReservationCommand.ReadBody);

        readers.Add(VolumeCommandKind.CreateSnapshot, CreateSnapshotCommand.ReadBody);
        readers.Add(VolumeCommandKind.DeleteSnapshot, DeleteSnapshotCommand.ReadBody);
        readers.Add(VolumeCommandKind.SetSnapshotRetention, SetSnapshotRetentionCommand.ReadBody);
        readers.Add(VolumeCommandKind.AcquirePin, AcquirePinCommand.ReadBody);
        readers.Add(VolumeCommandKind.ReleasePin, ReleasePinCommand.ReadBody);
        readers.Add(VolumeCommandKind.ExpirePins, ExpirePinsCommand.ReadBody);
        readers.Add(VolumeCommandKind.ExpireSnapshots, ExpireSnapshotsCommand.ReadBody);

        readers.Add(VolumeCommandKind.PruneDerefLog, PruneDerefLogCommand.ReadBody);
        readers.Add(VolumeCommandKind.RestoreChunk, RestoreChunkCommand.ReadBody);
        readers.Add(VolumeCommandKind.RestoreComplete, RestoreCompleteCommand.ReadBody);
        readers.Add(VolumeCommandKind.RestoreBegin, RestoreBeginCommand.ReadBody);
        readers.Add(VolumeCommandKind.RestoreFinish, RestoreFinishCommand.ReadBody);

        readers.Add(VolumeCommandKind.RecordBackupProgress, RecordBackupProgressCommand.ReadBody);
        readers.Add(VolumeCommandKind.AbandonTransferLineage, AbandonTransferLineageCommand.ReadBody);
        readers.Add(VolumeCommandKind.RequestBackupSync, RequestBackupSyncCommand.ReadBody);
        readers.Add(VolumeCommandKind.RemoveBackupTargetState, RemoveBackupTargetStateCommand.ReadBody);

        readers.Add(VolumeCommandKind.SetPolicy, SetPolicyCommand.ReadBody);
        readers.Add(VolumeCommandKind.SetAcl, SetAclCommand.ReadBody);
        readers.Add(VolumeCommandKind.SetOwner, SetOwnerCommand.ReadBody);
        readers.Add(VolumeCommandKind.RotateDataKey, RotateDataKeyCommand.ReadBody);
        readers.Add(VolumeCommandKind.MigratePolicy, MigratePolicyCommand.ReadBody);
    }
}
