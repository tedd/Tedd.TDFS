using Tedd.Tdfs.Protocol.Messages;

namespace Tedd.Tdfs.Client.Internal.Facades;

/// <summary>Conversions between wire records and the public SDK records.</summary>
internal static class WireConvert
{
    /// <summary>Name under which the SDK reports (and accepts) the volume's default storage policy.</summary>
    public const string DefaultPolicyName = "default";

    /// <summary>Public file metadata (the wire's empty policy name — the volume default — is reported as "default").</summary>
    public static DfsFileInfo ToFileInfo(FileInfoData info, CommitToken observedAt, long? lengthOverride = null) =>
        new(info.Path, info.Inode, info.Revision, lengthOverride ?? info.Length, info.CreatedUtc, info.ModifiedUtc, info.Owner,
            new DfsInodeAccess(info.OwnerOnly, info.ReadOnlyForOthers), info.StoragePolicy.Length == 0 ? DefaultPolicyName : info.StoragePolicy,
            info.IsInline, info.IsSparse, info.ExtentCount, observedAt, info.IsStream, info.StreamName, info.StreamCount) { BasicInformation = BasicInformationCodec.FromExtensions(info.Extensions) };

    /// <summary>Public directory metadata.</summary>
    public static DfsDirectoryInfo ToDirectoryInfo(FileInfoData info, CommitToken observedAt) =>
        new(info.Path, info.Inode, info.Revision, info.CreatedUtc, info.ModifiedUtc, info.Owner, info.EntryCount, observedAt) { BasicInformation = BasicInformationCodec.FromExtensions(info.Extensions) };

    /// <summary>Public directory entry (path combined with the listed directory).</summary>
    public static DfsEntryInfo ToEntry(string directory, DirectoryEntryData entry) =>
        new(entry.Name, DfsPath.Combine(directory, entry.Name), entry.Kind == WireEntryKind.Directory ? DfsEntryKind.Directory : DfsEntryKind.File,
            entry.Inode, entry.Revision, entry.Length, entry.ModifiedUtc);

    /// <summary>Wire entry-kind filter.</summary>
    public static WireEntryKinds ToWire(DfsEntryKinds kinds) => (WireEntryKinds)(byte)kinds;

    /// <summary>Public snapshot metadata.</summary>
    public static DfsSnapshotInfo ToSnapshotInfo(SnapshotInfoData data, Exception? resumeFailure = null) =>
        new(data.Id, data.Name, data.Volume, data.Boundary, data.Label,
            data.Adapters.Select(a => new DfsAdapterRecord(a.Id, a.Version, a.RestoreRequirements)).ToArray(), data.CreatedUtc, data.Retention,
            resumeFailure);

    /// <summary>Wire adapter record.</summary>
    public static AdapterRecordData ToWire(IDfsSnapshotAdapter adapter) =>
        new(adapter.Id, adapter.Version, adapter.RestoreRequirements ?? new Dictionary<string, string>(StringComparer.Ordinal));

    /// <summary>Public volume metadata.</summary>
    public static DfsVolumeInfo ToVolumeInfo(VolumeEntry entry, CommitToken lastCommit) =>
        new(entry.Id, entry.Name, entry.Epoch, entry.Metadata, entry.DefaultPolicy, entry.NamedPolicies, entry.InlineThreshold, entry.Limits, lastCommit,
            entry.StoragePool, entry.EnforceTraverseChecking);

    /// <summary>Public outcome status.</summary>
    public static DfsOutcomeStatus ToStatus(WireOutcomeStatus status) => status switch
    {
        WireOutcomeStatus.Committed => DfsOutcomeStatus.Committed,
        WireOutcomeStatus.Aborted => DfsOutcomeStatus.Aborted,
        WireOutcomeStatus.Pending => DfsOutcomeStatus.Pending,
        WireOutcomeStatus.Expired => DfsOutcomeStatus.Expired,
        _ => DfsOutcomeStatus.NotFound,
    };

    /// <summary>Public outcome.</summary>
    public static DfsOperationOutcome ToOutcome(DfsOperationId id, QueryOperationResultMessage result) =>
        new(id, ToStatus(result.Status), result.Token, result.Error, result.Detail);

    /// <summary>Public node status.</summary>
    public static DfsNodeStatus ToNodeStatus(NodeEntry node) => node.Status switch
    {
        WireNodeStatus.Draining => DfsNodeStatus.Draining,
        WireNodeStatus.Drained => DfsNodeStatus.Drained,
        WireNodeStatus.Removed => DfsNodeStatus.Removed,
        _ => node.IsReachable ? DfsNodeStatus.Active : DfsNodeStatus.Unreachable,
    };

    /// <summary>Public node metadata.</summary>
    public static DfsNodeInfo ToNodeInfo(NodeEntry node) =>
        new(node.Id, node.Name, node.Labels, node.Endpoints, ToNodeStatus(node), node.Incarnation, node.IsControlVoter, node.IsReachable, node.CapacityBytes,
            node.UsedBytes);

    /// <summary>Public group status.</summary>
    public static DfsGroupStatus ToGroupStatus(GroupStatusData group) =>
        new(group.Group, group.Voters, group.Learners, group.Leader, group.Term, group.CommitIndex, group.AppliedIndex, group.HasQuorum);

    /// <summary>Public replica health.</summary>
    public static DfsReplicaHealth ToReplicaHealth(ReplicaHealthData data) =>
        new(data.Objects, data.UnderReplicatedObjects, data.ZoneDeficitObjects, data.MissingObjects, data.PlacementViolations, data.RepairBacklogBytes,
            data.PendingGcObjects, data.StagedBytes, data.PinnedBytes);

    /// <summary>Public backup state.</summary>
    public static DfsBackupState ToBackupState(WireBackupState state) => state switch
    {
        WireBackupState.Transferring => DfsBackupState.Transferring,
        WireBackupState.Verifying => DfsBackupState.Verifying,
        WireBackupState.Publishing => DfsBackupState.Publishing,
        WireBackupState.Offline => DfsBackupState.Offline,
        WireBackupState.ReseedRequired => DfsBackupState.ReseedRequired,
        WireBackupState.Failed => DfsBackupState.Failed,
        _ => DfsBackupState.Idle,
    };

    /// <summary>Public backup status.</summary>
    public static DfsBackupStatus ToBackupStatus(BackupStatusData data) =>
        new(data.Target, ToBackupState(data.State), data.ReceivedBytes, data.DurableBoundary, data.PublishedBoundary, data.LatestPublished,
            data.RecoveryPointAge, data.ReseedRequired, data.PinnedSourceBytes, data.LastError, data.LineageId);

    /// <summary>Public pool metadata.</summary>
    public static DfsPoolInfo ToPoolInfo(PoolInfoData data) => new(data.Name, data.Nodes, data.Volumes, data.CapacityBytes, data.UsedBytes);
}
