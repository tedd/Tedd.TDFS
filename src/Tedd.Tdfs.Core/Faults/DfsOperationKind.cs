namespace Tedd.Tdfs;

/// <summary>Logical operation kinds used for fault matching, message filtering and diagnostics.</summary>
public enum DfsOperationKind
{
    /// <summary>Unknown or not applicable.</summary>
    Unknown,
    /// <summary>Client connection establishment.</summary>
    Connect,
    /// <summary>Cluster map / route discovery.</summary>
    Discover,
    /// <summary>Handshake authentication.</summary>
    Authenticate,
    /// <summary>Session open / operation id reservation.</summary>
    OpenSession,
    /// <summary>Lease and session renewal.</summary>
    KeepAlive,
    /// <summary>Open a file handle.</summary>
    Open,
    /// <summary>Close a handle.</summary>
    CloseHandle,
    /// <summary>Read file metadata/ranges.</summary>
    Read,
    /// <summary>Read object bytes from a storage node.</summary>
    ReadObject,
    /// <summary>Upload an object to a storage node.</summary>
    PutObject,
    /// <summary>Create a staging session.</summary>
    CreateStaging,
    /// <summary>Publish a file revision.</summary>
    CommitFile,
    /// <summary>Atomic whole-file replacement.</summary>
    WriteAll,
    /// <summary>Atomic append.</summary>
    Append,
    /// <summary>File copy.</summary>
    Copy,
    /// <summary>Move/rename.</summary>
    Move,
    /// <summary>Delete.</summary>
    Delete,
    /// <summary>Create directory.</summary>
    CreateDirectory,
    /// <summary>Directory enumeration and listings.</summary>
    Enumerate,
    /// <summary>Metadata query.</summary>
    GetInfo,
    /// <summary>Existence query.</summary>
    Exists,
    /// <summary>Transaction begin.</summary>
    TxBegin,
    /// <summary>Transaction operation.</summary>
    TxOp,
    /// <summary>Transaction commit.</summary>
    TxCommit,
    /// <summary>Transaction abort.</summary>
    TxAbort,
    /// <summary>Snapshot creation.</summary>
    CreateSnapshot,
    /// <summary>Snapshot deletion.</summary>
    DeleteSnapshot,
    /// <summary>Open/close a snapshot view.</summary>
    OpenSnapshot,
    /// <summary>Restore a snapshot into a new volume.</summary>
    RestoreSnapshot,
    /// <summary>Operation outcome query.</summary>
    QueryOperation,
    /// <summary>Backup data transfer.</summary>
    BackupTransfer,
    /// <summary>Backup publication.</summary>
    BackupPublish,
    /// <summary>Backup barrier wait.</summary>
    BackupAwait,
    /// <summary>Garbage collection.</summary>
    Gc,
    /// <summary>Replica repair / anti-entropy.</summary>
    Repair,
    /// <summary>Scrubbing.</summary>
    Scrub,
    /// <summary>Object store compaction.</summary>
    Compaction,
    /// <summary>Raft log replication / votes.</summary>
    RaftAppend,
    /// <summary>Raft vote.</summary>
    RaftVote,
    /// <summary>Raft snapshot transfer.</summary>
    RaftSnapshot,
    /// <summary>Cluster membership change.</summary>
    MembershipChange,
    /// <summary>Storage policy change.</summary>
    PolicyChange,
    /// <summary>Key rotation.</summary>
    KeyRotation,
    /// <summary>Administrative operation.</summary>
    Admin,
    /// <summary>Live export profile, mapping or handle operation.</summary>
    ExportControl,
}
