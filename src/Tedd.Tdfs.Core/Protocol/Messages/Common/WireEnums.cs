namespace Tedd.Tdfs.Protocol.Messages;

// Protocol-level enums. Values are part of the wire format and must never change; new members get new values.
// Where a member mirrors a public SDK enum (DfsNodeStatus, DfsOutcomeStatus, DfsBackupState, DfsEntryKind(s)), the
// numeric values are identical so the SDK can convert with a plain cast.

/// <summary>Role a peer declares (in <see cref="HelloMessage"/>) or a server announces (in <see cref="HelloChallengeMessage"/>).</summary>
public enum PeerRole : byte
{
    /// <summary>An SDK client (or a live exporter authenticating to a backup node with export credentials).</summary>
    Client = 0,

    /// <summary>A live cluster node (authenticated with the cluster secret).</summary>
    Node = 1,

    /// <summary>A late-sync backup node (read-only; never a Raft member).</summary>
    Backup = 2,
}

/// <summary>Membership status of a node in the cluster map (same values as the SDK's <c>DfsNodeStatus</c>).</summary>
public enum WireNodeStatus : byte
{
    /// <summary>Serving normally.</summary>
    Active = 0,

    /// <summary>Being drained: no new placements; data is moved away.</summary>
    Draining = 1,

    /// <summary>Drained: holds no required data.</summary>
    Drained = 2,

    /// <summary>Removed from the cluster; its identity is rejected by peers.</summary>
    Removed = 3,

    /// <summary>Member that the control leader currently cannot reach.</summary>
    Unreachable = 4,
}

/// <summary>Lifecycle state of a volume in the cluster map.</summary>
public enum WireVolumeState : byte
{
    /// <summary>Serving normally.</summary>
    Active = 0,

    /// <summary>Volume group is being created; not yet serving.</summary>
    Creating = 1,

    /// <summary>Volume is being deleted; requests are rejected.</summary>
    Deleting = 2,

    /// <summary>Volume is being restored (from a snapshot or backup); not yet activated.</summary>
    Restoring = 3,
}

/// <summary>Outcome of an operation id (same values as the SDK's <c>DfsOutcomeStatus</c>).</summary>
public enum WireOutcomeStatus : byte
{
    /// <summary>Committed; the token and stored result identify the commit.</summary>
    Committed = 0,

    /// <summary>Definitely not committed and fenced: it can never commit later.</summary>
    Aborted = 1,

    /// <summary>Known to be in flight; the outcome is not yet decided.</summary>
    Pending = 2,

    /// <summary>Never seen (only possible without <c>Resolve</c>; never a definite abort).</summary>
    NotFound = 3,

    /// <summary>Outside the retained outcome window; never replayed as a new operation.</summary>
    Expired = 4,
}

/// <summary>Kind of a namespace entry (same values as the SDK's <c>DfsEntryKind</c>).</summary>
public enum WireEntryKind : byte
{
    /// <summary>A file.</summary>
    File = 0,

    /// <summary>A directory.</summary>
    Directory = 1,
}

/// <summary>Entry kind filter for enumeration (same values as the SDK's <c>DfsEntryKinds</c>). Decoded without validation; the server rejects 0 and unknown bits.</summary>
[Flags]
public enum WireEntryKinds : byte
{
    /// <summary>No kinds (invalid as a filter).</summary>
    None = 0,

    /// <summary>Files.</summary>
    Files = 1,

    /// <summary>Directories.</summary>
    Directories = 2,

    /// <summary>Files and directories.</summary>
    All = 3,
}

/// <summary>Kind of a <see cref="RangeChange"/> in a publication.</summary>
public enum RangeChangeKind : byte
{
    /// <summary>The range maps to a byte range of an uploaded object (covered by a receipt).</summary>
    Data = 0,

    /// <summary>The range becomes a hole (reads return zeroes); existing extents in the range are removed.</summary>
    Zero = 1,

    /// <summary>The range is given inline in the request (only valid when the resulting file is stored inline).</summary>
    Inline = 2,
}

/// <summary>Kind of a <see cref="RaftWireMessage"/>; mirrors the Consensus project's message records.</summary>
public enum RaftWireKind : byte
{
    /// <summary>(Pre-)vote request.</summary>
    VoteRequest = 1,

    /// <summary>(Pre-)vote response.</summary>
    VoteResponse = 2,

    /// <summary>Log replication.</summary>
    AppendEntries = 3,

    /// <summary>Log replication response.</summary>
    AppendResponse = 4,

    /// <summary>Snapshot chunk.</summary>
    InstallSnapshot = 5,

    /// <summary>Snapshot chunk response.</summary>
    InstallSnapshotResponse = 6,

    /// <summary>Leadership transfer trigger.</summary>
    TimeoutNow = 7,

    /// <summary>Leader heartbeat.</summary>
    Heartbeat = 8,

    /// <summary>Heartbeat response.</summary>
    HeartbeatResponse = 9,
}

/// <summary>What a <see cref="BackupRegisterMessage"/> does with a backup target registration.</summary>
public enum BackupTargetAction : byte
{
    /// <summary>Register a new target (endpoints, export credentials, schedule, budgets).</summary>
    Register = 0,

    /// <summary>Remove a target (releases its transfer pins).</summary>
    Remove = 1,

    /// <summary>Change the schedule and budgets of an existing target.</summary>
    Configure = 2,

    /// <summary>List registered target names (no change).</summary>
    List = 3,
}

/// <summary>State of a backup target (same values as the SDK's <c>DfsBackupState</c>).</summary>
public enum WireBackupState : byte
{
    /// <summary>Nothing to transfer.</summary>
    Idle = 0,

    /// <summary>Transferring objects or manifest.</summary>
    Transferring = 1,

    /// <summary>Target verifying the dependency closure.</summary>
    Verifying = 2,

    /// <summary>Target publishing.</summary>
    Publishing = 3,

    /// <summary>Target unreachable.</summary>
    Offline = 4,

    /// <summary>Incremental lineage abandoned; a full reseed is required.</summary>
    ReseedRequired = 5,

    /// <summary>Last transfer failed.</summary>
    Failed = 6,
}

/// <summary>Which part of a published backup snapshot a <see cref="BackupExportRestoreMessage"/> requests.</summary>
public enum BackupRestorePart : byte
{
    /// <summary>Pin the snapshot and describe it (manifest size/checksum, object counts).</summary>
    Describe = 0,

    /// <summary>A chunk of the snapshot manifest.</summary>
    Manifest = 1,

    /// <summary>A chunk of one object.</summary>
    Object = 2,

    /// <summary>Release the reader pin taken by <see cref="Describe"/>.</summary>
    Release = 3,
}

/// <summary>Scope of a <see cref="DataCapability"/>.</summary>
public enum DataCapabilityKind : byte
{
    /// <summary>Allows <c>PutObject</c> into one open staging session.</summary>
    Staging = 1,

    /// <summary>Allows <c>GetObject</c> on an explicit set of objects.</summary>
    Read = 2,
}

/// <summary>Administrative operation carried by <see cref="AdminRequestMessage"/> (body records in <c>AdminBodies</c>).</summary>
public enum AdminOp : ushort
{
    /// <summary>Cluster status (<see cref="AdminEmptyBody"/> → <see cref="AdminClusterStatusBody"/>).</summary>
    ClusterStatus = 1,

    /// <summary>List nodes (<see cref="AdminEmptyBody"/> → <see cref="AdminNodeListBody"/>).</summary>
    ListNodes = 2,

    /// <summary>Add a node (<see cref="AdminAddNodeBody"/> → <see cref="NodeEntry"/>).</summary>
    AddNode = 3,

    /// <summary>Drain a node (<see cref="AdminNodeNameBody"/> → <see cref="AdminEmptyBody"/>).</summary>
    DrainNode = 4,

    /// <summary>Remove a node (<see cref="AdminRemoveNodeBody"/> → <see cref="AdminEmptyBody"/>).</summary>
    RemoveNode = 5,

    /// <summary>Change node labels (<see cref="AdminSetNodeLabelsBody"/> → <see cref="AdminEmptyBody"/>).</summary>
    SetNodeLabels = 6,

    /// <summary>Create a volume (<see cref="AdminVolumeSpecBody"/> → <see cref="AdminVolumeInfoBody"/>).</summary>
    CreateVolume = 10,

    /// <summary>Delete a volume (<see cref="AdminVolumeNameBody"/> → <see cref="AdminEmptyBody"/>).</summary>
    DeleteVolume = 11,

    /// <summary>List volume names (<see cref="AdminEmptyBody"/> → <see cref="AdminNameListBody"/>).</summary>
    ListVolumes = 12,

    /// <summary>Volume status (<see cref="AdminVolumeNameBody"/> → <see cref="AdminVolumeStatusBody"/>).</summary>
    VolumeStatus = 13,

    /// <summary>Set a volume's default policy (<see cref="AdminSetDefaultPolicyBody"/> → <see cref="AdminEmptyBody"/>).</summary>
    SetDefaultPolicy = 20,

    /// <summary>Define a named policy (<see cref="AdminDefinePolicyBody"/> → <see cref="AdminEmptyBody"/>).</summary>
    DefinePolicy = 21,

    /// <summary>Policy migration status (<see cref="AdminVolumeNameBody"/> → <see cref="AdminPolicyMigrationBody"/>).</summary>
    PolicyMigrationStatus = 22,

    /// <summary>Create a principal (<see cref="AdminCreatePrincipalBody"/> → <see cref="AdminEmptyBody"/>).</summary>
    CreatePrincipal = 30,

    /// <summary>Delete a principal (<see cref="AdminPrincipalNameBody"/> → <see cref="AdminEmptyBody"/>).</summary>
    DeletePrincipal = 31,

    /// <summary>Grant volume permissions (<see cref="AdminGrantBody"/> → <see cref="AdminEmptyBody"/>).</summary>
    Grant = 32,

    /// <summary>Revoke volume permissions (<see cref="AdminGrantBody"/> → <see cref="AdminEmptyBody"/>).</summary>
    Revoke = 33,

    /// <summary>Rotate a volume data key (<see cref="AdminVolumeNameBody"/> → <see cref="AdminKeyRotationBody"/>).</summary>
    RotateDataKey = 40,

    /// <summary>Export the recovery key escrow (<see cref="AdminExportRecoveryKeyBody"/> → <see cref="AdminRecoveryKeyBody"/>).</summary>
    ExportRecoveryKey = 41,

    /// <summary>Start a scrub (<see cref="AdminStartScrubBody"/> → <see cref="AdminEmptyBody"/>).</summary>
    StartScrub = 50,

    /// <summary>Start a repair pass (<see cref="AdminVolumeNameBody"/> → <see cref="AdminEmptyBody"/>).</summary>
    StartRepair = 51,

    /// <summary>Replica health (<see cref="AdminVolumeNameBody"/> → <see cref="ReplicaHealthData"/>).</summary>
    ReplicaHealth = 52,

    /// <summary>Restore a backup snapshot into a new volume (<see cref="AdminRestoreFromBackupBody"/> → <see cref="AdminVolumeInfoBody"/>).</summary>
    RestoreFromBackup = 60,

    /// <summary>Enable or disable a feature (<see cref="AdminSetFeatureBody"/> → <see cref="AdminEmptyBody"/>).</summary>
    SetFeature = 70,

    /// <summary>Read the audit log (<see cref="AdminAuditLogRequestBody"/> → <see cref="AdminNameListBody"/>).</summary>
    AuditLog = 71,

    /// <summary>Create a principal group (<see cref="AdminGroupNameBody"/> → <see cref="AdminEmptyBody"/>; Addendum A).</summary>
    CreateGroup = 80,

    /// <summary>Delete a principal group (<see cref="AdminGroupNameBody"/> → <see cref="AdminEmptyBody"/>).</summary>
    DeleteGroup = 81,

    /// <summary>Add a principal to a group (<see cref="AdminGroupMemberBody"/> → <see cref="AdminEmptyBody"/>).</summary>
    AddGroupMember = 82,

    /// <summary>Remove a principal from a group (<see cref="AdminGroupMemberBody"/> → <see cref="AdminEmptyBody"/>).</summary>
    RemoveGroupMember = 83,

    /// <summary>List groups (<see cref="AdminEmptyBody"/> → <see cref="AdminGroupListBody"/>).</summary>
    ListGroups = 84,

    /// <summary>Create a storage pool (<see cref="AdminPoolMembersBody"/> → <see cref="AdminEmptyBody"/>; Addendum A).</summary>
    CreatePool = 90,

    /// <summary>Replace a pool's node set (<see cref="AdminPoolMembersBody"/> → <see cref="AdminEmptyBody"/>).</summary>
    SetPoolMembers = 91,

    /// <summary>Delete an unreferenced pool (<see cref="AdminPoolNameBody"/> → <see cref="AdminEmptyBody"/>).</summary>
    DeletePool = 92,

    /// <summary>List pools (<see cref="AdminEmptyBody"/> → <see cref="AdminPoolListBody"/>).</summary>
    ListPools = 93,

    /// <summary>Status of one pool (<see cref="AdminPoolNameBody"/> → <see cref="AdminPoolStatusBody"/>).</summary>
    PoolStatus = 94,
}

/// <summary>Object payload encodings (the <c>Encoding</c> byte of objects, extents, changes and receipts).</summary>
public static class ObjectEncodings
{
    /// <summary>Plain bytes: the object payload is exactly the logical file bytes.</summary>
    public const byte Plain = 0;
}
