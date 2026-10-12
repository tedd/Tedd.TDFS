namespace Tedd.Tdfs.Protocol;

/// <summary>
/// Protocol message catalogue. Frame = <c>[u16 MessageType][body]</c>. Requests have even values (from 10 upwards)
/// and their replies the following odd value; <see cref="ErrorReply"/> answers any request. Values are part of the
/// wire format and must never change.
/// </summary>
public enum MessageType : ushort
{
    /// <summary>Error reply to any request.</summary>
    ErrorReply = 1,

    // handshake / discovery

    /// <summary>Handshake hello.</summary>
    Hello = 10,
    /// <summary>Handshake challenge (reply to <see cref="Hello"/>).</summary>
    HelloChallenge = 11,
    /// <summary>Handshake proof.</summary>
    HelloProof = 12,
    /// <summary>Handshake result (reply to <see cref="HelloProof"/>).</summary>
    HelloResult = 13,
    /// <summary>Cluster map request.</summary>
    GetClusterMap = 14,
    /// <summary>Cluster map reply.</summary>
    ClusterMapResult = 15,
    /// <summary>Volume route request.</summary>
    GetVolumeRoute = 16,
    /// <summary>Volume route reply.</summary>
    VolumeRouteResult = 17,
    /// <summary>Join cluster request.</summary>
    JoinCluster = 18,
    /// <summary>Join cluster reply.</summary>
    JoinClusterResult = 19,
    /// <summary>Register node incarnation request.</summary>
    RegisterIncarnation = 20,
    /// <summary>Register node incarnation reply.</summary>
    RegisterIncarnationResult = 21,

    // sessions / outcomes

    /// <summary>Open session request.</summary>
    OpenSession = 30,
    /// <summary>Open session reply.</summary>
    OpenSessionResult = 31,
    /// <summary>Keep-alive request.</summary>
    KeepAlive = 32,
    /// <summary>Keep-alive reply.</summary>
    KeepAliveResult = 33,
    /// <summary>Operation outcome query.</summary>
    QueryOperation = 34,
    /// <summary>Operation outcome reply.</summary>
    QueryOperationResult = 35,
    /// <summary>Operation id reservation request.</summary>
    ReserveOperationIds = 36,
    /// <summary>Operation id reservation reply.</summary>
    ReserveOperationIdsResult = 37,

    // namespace & files (client -> volume leader)

    /// <summary>Open handle request.</summary>
    OpenHandle = 40,
    /// <summary>Open handle reply.</summary>
    OpenHandleResult = 41,
    /// <summary>Close handle request.</summary>
    CloseHandle = 42,
    /// <summary>Close handle reply.</summary>
    CloseHandleResult = 43,
    /// <summary>Read range request.</summary>
    ReadRange = 44,
    /// <summary>Read range reply.</summary>
    ReadRangeResult = 45,
    /// <summary>Get info request.</summary>
    GetInfo = 46,
    /// <summary>Get info reply.</summary>
    GetInfoResult = 47,
    /// <summary>List directory request.</summary>
    ListDirectory = 48,
    /// <summary>List directory reply.</summary>
    ListDirectoryResult = 49,
    /// <summary>Create staging request.</summary>
    CreateStaging = 50,
    /// <summary>Create staging reply.</summary>
    CreateStagingResult = 51,
    /// <summary>Commit file request.</summary>
    CommitFile = 52,
    /// <summary>Commit file reply.</summary>
    CommitFileResult = 53,
    /// <summary>Write-all request.</summary>
    WriteAll = 54,
    /// <summary>Write-all reply.</summary>
    WriteAllResult = 55,
    /// <summary>Append request.</summary>
    Append = 56,
    /// <summary>Append reply.</summary>
    AppendResult = 57,
    /// <summary>Copy request.</summary>
    Copy = 58,
    /// <summary>Copy reply.</summary>
    CopyResult = 59,
    /// <summary>Move request.</summary>
    Move = 60,
    /// <summary>Move reply.</summary>
    MoveResult = 61,
    /// <summary>Delete request.</summary>
    Delete = 62,
    /// <summary>Delete reply.</summary>
    DeleteResult = 63,
    /// <summary>Create directory request.</summary>
    CreateDirectory = 64,
    /// <summary>Create directory reply.</summary>
    CreateDirectoryResult = 65,
    /// <summary>Set storage policy request.</summary>
    SetPolicy = 66,
    /// <summary>Set storage policy reply.</summary>
    SetPolicyResult = 67,
    /// <summary>Set inode access request.</summary>
    SetAccess = 68,
    /// <summary>Set inode access reply.</summary>
    SetAccessResult = 69,

    // transactions

    /// <summary>Transaction begin request.</summary>
    TxBegin = 80,
    /// <summary>Transaction begin reply.</summary>
    TxBeginResult = 81,
    /// <summary>Transaction operation request.</summary>
    TxOp = 82,
    /// <summary>Transaction operation reply.</summary>
    TxOpResult = 83,
    /// <summary>Transaction commit request.</summary>
    TxCommit = 84,
    /// <summary>Transaction commit reply.</summary>
    TxCommitResult = 85,
    /// <summary>Transaction abort request.</summary>
    TxAbort = 86,
    /// <summary>Transaction abort reply.</summary>
    TxAbortResult = 87,

    // snapshots

    /// <summary>Create snapshot request.</summary>
    CreateSnapshot = 90,
    /// <summary>Create snapshot reply.</summary>
    CreateSnapshotResult = 91,
    /// <summary>List snapshots request.</summary>
    ListSnapshots = 92,
    /// <summary>List snapshots reply.</summary>
    ListSnapshotsResult = 93,
    /// <summary>Delete snapshot request.</summary>
    DeleteSnapshot = 94,
    /// <summary>Delete snapshot reply.</summary>
    DeleteSnapshotResult = 95,
    /// <summary>Open snapshot view request.</summary>
    OpenSnapshot = 96,
    /// <summary>Open snapshot view reply.</summary>
    OpenSnapshotResult = 97,
    /// <summary>Close snapshot view request.</summary>
    CloseSnapshot = 98,
    /// <summary>Close snapshot view reply.</summary>
    CloseSnapshotResult = 99,
    /// <summary>Set snapshot retention request.</summary>
    SetRetention = 100,
    /// <summary>Set snapshot retention reply.</summary>
    SetRetentionResult = 101,
    /// <summary>Restore snapshot request.</summary>
    RestoreSnapshot = 102,
    /// <summary>Restore snapshot reply.</summary>
    RestoreSnapshotResult = 103,

    // data plane (client/node -> storage node)

    /// <summary>Put object request.</summary>
    PutObject = 110,
    /// <summary>Put object reply (receipt).</summary>
    PutObjectResult = 111,
    /// <summary>Get object request.</summary>
    GetObject = 112,
    /// <summary>Get object reply.</summary>
    GetObjectResult = 113,
    /// <summary>Locate object request.</summary>
    LocateObject = 114,
    /// <summary>Locate object reply.</summary>
    LocateObjectResult = 115,
    /// <summary>Replicate object request (repair pull).</summary>
    ReplicateObject = 116,
    /// <summary>Replicate object reply.</summary>
    ReplicateObjectResult = 117,
    /// <summary>Delete objects request (GC).</summary>
    DeleteObjects = 118,
    /// <summary>Delete objects reply.</summary>
    DeleteObjectsResult = 119,
    /// <summary>Inventory report request.</summary>
    InventoryReport = 120,
    /// <summary>Inventory report reply.</summary>
    InventoryReportResult = 121,
    /// <summary>Copy objects request (restore).</summary>
    CopyObjects = 122,
    /// <summary>Copy objects reply.</summary>
    CopyObjectsResult = 123,

    // consensus (node <-> node)

    /// <summary>Raft message batch.</summary>
    RaftBatch = 130,
    /// <summary>Raft message batch reply.</summary>
    RaftBatchResult = 131,
    /// <summary>Raft snapshot chunk.</summary>
    RaftSnapshotChunk = 132,
    /// <summary>Raft snapshot chunk reply.</summary>
    RaftSnapshotChunkResult = 133,

    // administration

    /// <summary>Administrative request.</summary>
    AdminRequest = 140,
    /// <summary>Administrative reply.</summary>
    AdminResult = 141,

    // backup

    /// <summary>Backup target registration request.</summary>
    BackupRegister = 150,
    /// <summary>Backup target registration reply.</summary>
    BackupRegisterResult = 151,
    /// <summary>Backup status request.</summary>
    BackupStatus = 152,
    /// <summary>Backup status reply.</summary>
    BackupStatusResult = 153,
    /// <summary>Backup sync request.</summary>
    BackupRequestSync = 154,
    /// <summary>Backup sync reply.</summary>
    BackupRequestSyncResult = 155,
    /// <summary>Backup barrier wait request.</summary>
    BackupAwait = 156,
    /// <summary>Backup barrier wait reply.</summary>
    BackupAwaitResult = 157,
    /// <summary>Backup transfer begin.</summary>
    BackupBegin = 160,
    /// <summary>Backup transfer begin reply.</summary>
    BackupBeginResult = 161,
    /// <summary>Backup target inventory request.</summary>
    BackupInventory = 162,
    /// <summary>Backup target inventory reply.</summary>
    BackupInventoryResult = 163,
    /// <summary>Backup object transfer.</summary>
    BackupPutObject = 164,
    /// <summary>Backup object transfer reply.</summary>
    BackupPutObjectResult = 165,
    /// <summary>Backup manifest transfer.</summary>
    BackupPutManifest = 166,
    /// <summary>Backup manifest transfer reply.</summary>
    BackupPutManifestResult = 167,
    /// <summary>Backup publication request.</summary>
    BackupPublish = 168,
    /// <summary>Backup publication reply (acknowledgement).</summary>
    BackupPublishResult = 169,
    /// <summary>Backup retention request.</summary>
    BackupRetention = 170,
    /// <summary>Backup retention reply.</summary>
    BackupRetentionResult = 171,
    /// <summary>Restore-from-backup export request.</summary>
    BackupExportRestore = 172,
    /// <summary>Restore-from-backup export reply.</summary>
    BackupExportRestoreResult = 173,
}
