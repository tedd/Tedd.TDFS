namespace Tedd.Tdfs;

/// <summary>
/// Canonical error codes shared by SDK and server. Values are part of the wire format and must never change.
/// See <see cref="DfsErrors"/> for the mapping to exception types.
/// </summary>
public enum DfsErrorCode
{
    /// <summary>No error.</summary>
    None = 0,
    /// <summary>Unexpected internal failure.</summary>
    Internal = 1,
    /// <summary>Invalid argument or option.</summary>
    InvalidArgument = 2,
    /// <summary>Invalid path or name.</summary>
    InvalidPath = 3,
    /// <summary>File not found.</summary>
    FileNotFound = 4,
    /// <summary>Directory not found.</summary>
    DirectoryNotFound = 5,
    /// <summary>Entry already exists.</summary>
    AlreadyExists = 6,
    /// <summary>Directory is not empty.</summary>
    DirectoryNotEmpty = 7,
    /// <summary>A path component is not a directory.</summary>
    NotADirectory = 8,
    /// <summary>The target is a directory.</summary>
    IsADirectory = 9,
    /// <summary>Permission denied.</summary>
    AccessDenied = 10,
    /// <summary>Authentication failed.</summary>
    AuthenticationFailed = 11,
    /// <summary>Share reservation conflict.</summary>
    SharingViolation = 12,
    /// <summary>Another writer holds writer authority.</summary>
    WriterConflict = 13,
    /// <summary>Transaction validation conflict.</summary>
    TransactionConflict = 14,
    /// <summary>Transaction aborted (leader change, expiry, limits).</summary>
    TransactionAborted = 15,
    /// <summary>Writer lease/authority lost.</summary>
    LeaseLost = 16,
    /// <summary>Read pin expired.</summary>
    PinExpired = 17,
    /// <summary>Contacted node is not the leader.</summary>
    NotLeader = 18,
    /// <summary>Consensus quorum unavailable.</summary>
    QuorumUnavailable = 19,
    /// <summary>Required data placement cannot be achieved.</summary>
    PlacementUnavailable = 20,
    /// <summary>Node unavailable.</summary>
    NodeUnavailable = 21,
    /// <summary>Integrity verification failed.</summary>
    Integrity = 22,
    /// <summary>Commit outcome unknown.</summary>
    OutcomeUnknown = 23,
    /// <summary>Operation id retention expired.</summary>
    OperationExpired = 24,
    /// <summary>Operation id reused with different arguments or principal.</summary>
    OperationIdMismatch = 25,
    /// <summary>A configured limit was exceeded.</summary>
    LimitExceeded = 26,
    /// <summary>Storage capacity exhausted.</summary>
    CapacityExhausted = 27,
    /// <summary>Operation spans volumes.</summary>
    CrossVolume = 28,
    /// <summary>Target is read-only.</summary>
    ReadOnly = 29,
    /// <summary>Unsupported feature or option.</summary>
    Unsupported = 30,
    /// <summary>Malformed protocol message.</summary>
    ProtocolError = 31,
    /// <summary>Peer belongs to a different cluster.</summary>
    ClusterMismatch = 32,
    /// <summary>Volume epoch mismatch.</summary>
    EpochMismatch = 33,
    /// <summary>Timed out.</summary>
    Timeout = 34,
    /// <summary>Cancelled before publication.</summary>
    Cancelled = 35,
    /// <summary>Snapshot not found.</summary>
    SnapshotNotFound = 36,
    /// <summary>Backup target not found.</summary>
    BackupTargetNotFound = 37,
    /// <summary>Stale node incarnation.</summary>
    StaleIncarnation = 38,
    /// <summary>Recovery key material missing.</summary>
    RecoveryKeyMissing = 39,
    /// <summary>Invalid storage or metadata policy.</summary>
    InvalidPolicy = 40,
    /// <summary>Enumeration continuation expired.</summary>
    ContinuationExpired = 41,
    /// <summary>Unknown, closed or expired handle.</summary>
    InvalidHandle = 42,
    /// <summary>Volume not found.</summary>
    VolumeNotFound = 43,
    /// <summary>Generic availability failure.</summary>
    Unavailable = 44,
    /// <summary>Snapshot adapter failed.</summary>
    AdapterFailed = 45,
    /// <summary>Deletion blocked by retention protection.</summary>
    RetentionProtected = 46,
    /// <summary>Backup barrier not reached in time.</summary>
    BackupNotReached = 47,
}
