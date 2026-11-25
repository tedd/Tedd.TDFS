using System.Collections.Frozen;

namespace Tedd.Tdfs.Protocol;

/// <summary>Classification helpers for <see cref="MessageType"/>.</summary>
public static class MessageTypes
{
    private static readonly FrozenSet<MessageType> Defined = Enum.GetValues<MessageType>().ToFrozenSet();

    /// <summary>Every defined message type, in ascending order.</summary>
    public static IReadOnlyList<MessageType> All { get; } = Enum.GetValues<MessageType>().Order().ToArray();

    /// <summary>True when <paramref name="type"/> is a member of the catalogue.</summary>
    public static bool IsDefined(MessageType type) => Defined.Contains(type);

    /// <summary>True for reply types (<see cref="MessageType.ErrorReply"/> and every <c>*Result</c>/challenge type).</summary>
    public static bool IsReply(MessageType type) => Defined.Contains(type) && ((ushort)type & 1) == 1;

    /// <summary>True for request types.</summary>
    public static bool IsRequest(MessageType type) => Defined.Contains(type) && ((ushort)type & 1) == 0;

    /// <summary>Returns the success reply type of a request (the error reply is always <see cref="MessageType.ErrorReply"/>).</summary>
    /// <exception cref="ArgumentException"><paramref name="request"/> is not a defined request type.</exception>
    public static MessageType ReplyOf(MessageType request)
    {
        if (!IsRequest(request))
            throw new ArgumentException($"Message type {request} is not a request.", nameof(request));
        var reply = (MessageType)((ushort)request + 1);
        return Defined.Contains(reply)
            ? reply
            : throw new ArgumentException($"Message type {request} has no reply type.", nameof(request));
    }

    /// <summary>Returns the request type answered by a success reply type.</summary>
    /// <exception cref="ArgumentException"><paramref name="reply"/> is not a reply type with a unique request.</exception>
    public static MessageType RequestOf(MessageType reply)
    {
        if (!IsReply(reply) || reply == MessageType.ErrorReply)
            throw new ArgumentException($"Message type {reply} is not a request-specific reply.", nameof(reply));
        return (MessageType)((ushort)reply - 1);
    }

    /// <summary>Maps a message type (request or reply) to its logical operation kind; undefined types map to <see cref="DfsOperationKind.Unknown"/>.</summary>
    public static DfsOperationKind OperationOf(MessageType type) => type switch
    {
        MessageType.ErrorReply => DfsOperationKind.Unknown,

        MessageType.Hello or MessageType.HelloChallenge or MessageType.HelloProof or MessageType.HelloResult
            => DfsOperationKind.Authenticate,
        MessageType.GetClusterMap or MessageType.ClusterMapResult or MessageType.GetVolumeRoute or MessageType.VolumeRouteResult
            => DfsOperationKind.Discover,
        MessageType.JoinCluster or MessageType.JoinClusterResult or MessageType.RegisterIncarnation or MessageType.RegisterIncarnationResult
            => DfsOperationKind.MembershipChange,
        MessageType.AuditEvent or MessageType.AuditEventResult => DfsOperationKind.Admin,

        MessageType.OpenSession or MessageType.OpenSessionResult or MessageType.ReserveOperationIds or MessageType.ReserveOperationIdsResult
            => DfsOperationKind.OpenSession,
        MessageType.KeepAlive or MessageType.KeepAliveResult => DfsOperationKind.KeepAlive,
        MessageType.QueryOperation or MessageType.QueryOperationResult => DfsOperationKind.QueryOperation,

        MessageType.ExportControl or MessageType.ExportControlResult => DfsOperationKind.ExportControl,
        MessageType.ExportOpen or MessageType.ExportOpenResult or MessageType.OpenHandle or MessageType.OpenHandleResult => DfsOperationKind.Open,
        MessageType.CloseHandle or MessageType.CloseHandleResult => DfsOperationKind.CloseHandle,
        MessageType.ReadRange or MessageType.ReadRangeResult => DfsOperationKind.Read,
        MessageType.GetInfo or MessageType.GetInfoResult => DfsOperationKind.GetInfo,
        MessageType.ListDirectory or MessageType.ListDirectoryResult => DfsOperationKind.Enumerate,
        MessageType.CreateStaging or MessageType.CreateStagingResult => DfsOperationKind.CreateStaging,
        MessageType.CommitFile or MessageType.CommitFileResult => DfsOperationKind.CommitFile,
        MessageType.WriteAll or MessageType.WriteAllResult => DfsOperationKind.WriteAll,
        MessageType.Append or MessageType.AppendResult => DfsOperationKind.Append,
        MessageType.Copy or MessageType.CopyResult => DfsOperationKind.Copy,
        MessageType.Move or MessageType.MoveResult => DfsOperationKind.Move,
        MessageType.Delete or MessageType.DeleteResult => DfsOperationKind.Delete,
        MessageType.CreateDirectory or MessageType.CreateDirectoryResult => DfsOperationKind.CreateDirectory,
        MessageType.SetPolicy or MessageType.SetPolicyResult => DfsOperationKind.PolicyChange,
        MessageType.SetAccess or MessageType.SetAccessResult => DfsOperationKind.Admin,
        MessageType.GetAccessControl or MessageType.GetAccessControlResult or MessageType.GetEffectiveRights or MessageType.GetEffectiveRightsResult
            or MessageType.ListStreams or MessageType.ListStreamsResult => DfsOperationKind.GetInfo,
        MessageType.SetAccessControl or MessageType.SetAccessControlResult or MessageType.SetOwner or MessageType.SetOwnerResult
            => DfsOperationKind.Admin,

        MessageType.TxBegin or MessageType.TxBeginResult => DfsOperationKind.TxBegin,
        MessageType.TxOp or MessageType.TxOpResult => DfsOperationKind.TxOp,
        MessageType.TxCommit or MessageType.TxCommitResult => DfsOperationKind.TxCommit,
        MessageType.TxAbort or MessageType.TxAbortResult => DfsOperationKind.TxAbort,

        MessageType.CreateSnapshot or MessageType.CreateSnapshotResult => DfsOperationKind.CreateSnapshot,
        MessageType.ListSnapshots or MessageType.ListSnapshotsResult => DfsOperationKind.Enumerate,
        MessageType.DeleteSnapshot or MessageType.DeleteSnapshotResult => DfsOperationKind.DeleteSnapshot,
        MessageType.OpenSnapshot or MessageType.OpenSnapshotResult or MessageType.CloseSnapshot or MessageType.CloseSnapshotResult
            => DfsOperationKind.OpenSnapshot,
        MessageType.SetRetention or MessageType.SetRetentionResult => DfsOperationKind.Admin,
        MessageType.RestoreSnapshot or MessageType.RestoreSnapshotResult => DfsOperationKind.RestoreSnapshot,

        // Node-internal control and install steps of a restore: reported as administrative traffic at the request-path fault
        // sites, so a RestoreSnapshot match addresses the client's request (OUT-001); their committed entries and object
        // writes are still tagged RestoreSnapshot (RST-007).
        MessageType.RestoreVolume or MessageType.RestoreVolumeResult or MessageType.RestoreInstall or MessageType.RestoreInstallResult
            => DfsOperationKind.Admin,

        MessageType.PutObject or MessageType.PutObjectResult => DfsOperationKind.PutObject,
        MessageType.GetObject or MessageType.GetObjectResult or MessageType.LocateObject or MessageType.LocateObjectResult
            => DfsOperationKind.ReadObject,
        MessageType.ReplicateObject or MessageType.ReplicateObjectResult or MessageType.InventoryReport or MessageType.InventoryReportResult
            or MessageType.ReportUsage or MessageType.ReportUsageResult
            => DfsOperationKind.Repair,
        MessageType.DeleteObjects or MessageType.DeleteObjectsResult => DfsOperationKind.Gc,
        MessageType.CopyObjects or MessageType.CopyObjectsResult => DfsOperationKind.RestoreSnapshot,

        MessageType.RaftBatch or MessageType.RaftBatchResult => DfsOperationKind.RaftAppend,
        MessageType.RaftSnapshotChunk or MessageType.RaftSnapshotChunkResult => DfsOperationKind.RaftSnapshot,

        MessageType.AdminRequest or MessageType.AdminResult => DfsOperationKind.Admin,

        MessageType.BackupRegister or MessageType.BackupRegisterResult or MessageType.BackupStatus or MessageType.BackupStatusResult or
        MessageType.BackupRequestSync or MessageType.BackupRequestSyncResult or MessageType.BackupRetention or MessageType.BackupRetentionResult or
        MessageType.BackupTargetUpdate or MessageType.BackupTargetUpdateResult
            => DfsOperationKind.Admin,
        MessageType.BackupAwait or MessageType.BackupAwaitResult => DfsOperationKind.BackupAwait,
        MessageType.BackupBegin or MessageType.BackupBeginResult or MessageType.BackupInventory or MessageType.BackupInventoryResult or
        MessageType.BackupPutObject or MessageType.BackupPutObjectResult or MessageType.BackupPutManifest or MessageType.BackupPutManifestResult
            => DfsOperationKind.BackupTransfer,
        MessageType.BackupPublish or MessageType.BackupPublishResult => DfsOperationKind.BackupPublish,
        MessageType.BackupExportRestore or MessageType.BackupExportRestoreResult => DfsOperationKind.RestoreSnapshot,

        _ => DfsOperationKind.Unknown,
    };
}
