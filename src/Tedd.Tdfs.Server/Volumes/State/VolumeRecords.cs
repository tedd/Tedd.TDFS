using System.Collections.Immutable;

namespace Tedd.Tdfs.Server.Volumes.State;

/// <summary>Lifecycle of a client session (ARCHITECTURE §7).</summary>
internal enum SessionState : byte
{
    /// <summary>Renewed by keep-alives; owns handles, pins and staging sessions.</summary>
    Active = 0,

    /// <summary>Expired: its resources were released; outcomes are retained for the session's outcome retention.</summary>
    Expired = 1,

    /// <summary>Fully retired: outcomes dropped; every sequence is expired. Kept as a tombstone until the session watermark passes it.</summary>
    Retired = 2,
}

/// <summary>
/// A block of operation sequences reserved by <c>ReserveOperationIds</c> (PROTOCOL.md §9.3). Reserved ids are known to the
/// volume and outcome retirement by count never advances the retired watermark past an unused reserved id.
/// </summary>
/// <param name="First">First reserved sequence.</param>
/// <param name="Count">Number of reserved sequences.</param>
/// <param name="Used">How many of them received an outcome so far.</param>
internal sealed record ReservedRange(ulong First, ulong Count, ulong Used)
{
    /// <summary>Last reserved sequence.</summary>
    public ulong Last => First + Count - 1;

    /// <summary>True when <paramref name="sequence"/> lies in the range.</summary>
    public bool Contains(ulong sequence) => sequence >= First && sequence <= Last;
}

/// <summary>
/// The retained outcome of one operation id (ARCHITECTURE §5.2): what was executed, by whom, with which payload identity, and
/// the exact reply (success or error frame) so that a retry or an outcome query returns the same answer without executing
/// again.
/// </summary>
/// <param name="Sequence">Operation sequence (within the session).</param>
/// <param name="Operation">Kind of the original request.</param>
/// <param name="PayloadHash">Payload identity (<c>OperationPayload.ComputeHash</c>, 16 bytes as little-endian <see cref="UInt128"/>).</param>
/// <param name="Principal">Principal that issued the operation.</param>
/// <param name="Code">Outcome code (<see cref="DfsErrorCode.None"/> = committed successfully).</param>
/// <param name="Message">Failure message (empty on success).</param>
/// <param name="Reply">Encoded reply frame: the success reply, or an <c>ErrorReply</c> for a failed apply.</param>
/// <param name="Index">Raft index of the deciding entry.</param>
/// <param name="Fenced">True when the outcome was recorded by <c>FenceOperation</c> (definite abort, OUT-013).</param>
internal sealed record OutcomeRecord(ulong Sequence, DfsOperationKind Operation, UInt128 PayloadHash, string Principal, DfsErrorCode Code, string Message,
    ReadOnlyMemory<byte> Reply, ulong Index, bool Fenced)
{
    /// <summary>True when the operation committed successfully.</summary>
    public bool IsSuccess => Code == DfsErrorCode.None;
}

/// <summary>A client session (ARCHITECTURE §5.1, §7).</summary>
/// <param name="Id">Session id (allocated monotonically, never reused).</param>
/// <param name="Principal">Owning principal.</param>
/// <param name="ClientName">Informational client name.</param>
/// <param name="Nonce">Open nonce (retries of the same logical open return this session).</param>
/// <param name="State">Lifecycle state.</param>
/// <param name="CreatedIndex">Raft index of the <c>OpenSession</c> entry.</param>
/// <param name="LastActivityUtc">Leader wall clock of the last renewal (informational).</param>
/// <param name="ExpiredUtc">Leader wall clock when the session expired (null while active).</param>
/// <param name="RetiredWatermark">Every sequence at or below this is retired (expired unless an outcome is retained).</param>
/// <param name="HighestSequence">Highest sequence that received an outcome or was reserved.</param>
/// <param name="Reserved">Reserved sequence blocks still holding unused ids (ascending).</param>
/// <param name="Outcomes">Retained outcomes by sequence.</param>
/// <param name="MaxOutcomes">Outcomes retained before the oldest are retired (leader configuration at open).</param>
/// <param name="OutcomeRetention">Time outcomes are retained after the session expires.</param>
/// <param name="LeaseDuration">Handle lease duration announced at open.</param>
/// <param name="SessionTimeout">Session timeout announced at open.</param>
internal sealed record SessionRecord(ulong Id, string Principal, string ClientName, UInt128 Nonce, SessionState State, ulong CreatedIndex,
    DateTimeOffset LastActivityUtc, DateTimeOffset? ExpiredUtc, ulong RetiredWatermark, ulong HighestSequence, ImmutableList<ReservedRange> Reserved,
    ImmutableSortedDictionary<ulong, OutcomeRecord> Outcomes, int MaxOutcomes, TimeSpan OutcomeRetention, TimeSpan LeaseDuration, TimeSpan SessionTimeout)
{
    /// <summary>Empty outcome map.</summary>
    public static ImmutableSortedDictionary<ulong, OutcomeRecord> NoOutcomes { get; } = ImmutableSortedDictionary<ulong, OutcomeRecord>.Empty;
}

/// <summary>How a handle reads.</summary>
internal enum HandleView : byte
{
    /// <summary>Every read selects the current committed revision (ReadIndex).</summary>
    Live = 0,

    /// <summary>Every read uses the revision current at open (<see cref="HandleRecord.PinnedInode"/>).</summary>
    VersionPinned = 1,
}

/// <summary>
/// An open handle on one stream of an inode (ARCHITECTURE §5.1, §5.3). The handle keeps the access granted at open
/// (ACL-07); its lease is tracked by the leader and expired with <c>ExpireHandles</c>.
/// </summary>
/// <param name="Id">Handle id.</param>
/// <param name="Inode">Opened inode.</param>
/// <param name="Stream">Opened stream (<c>""</c> = default stream).</param>
/// <param name="Session">Owning session.</param>
/// <param name="Principal">Principal that opened the handle.</param>
/// <param name="Mode">Open mode.</param>
/// <param name="Access">Requested access.</param>
/// <param name="Share">Sharing granted to other handles.</param>
/// <param name="IsWriter">True for a writable handle (holds the stream's writer reservation).</param>
/// <param name="WriterGeneration">Fencing generation bound to a writable handle (0 for readers).</param>
/// <param name="View">Read view.</param>
/// <param name="PinnedInode">The inode version a version-pinned handle reads (null for live handles).</param>
/// <param name="PinId">GC pin held by the handle.</param>
/// <param name="OpenedIndex">Raft index of the open.</param>
/// <param name="GrantedRights">ACL rights evaluated at open (informational; handles keep their access).</param>
/// <param name="IsAppend">True for append handles.</param>
internal sealed record HandleRecord(ulong Id, ulong Inode, string Stream, ulong Session, string Principal, FileMode Mode, FileAccess Access, FileShare Share,
    bool IsWriter, ulong WriterGeneration, HandleView View, InodeRecord? PinnedInode, ulong PinId, ulong OpenedIndex, DfsFileSystemRights GrantedRights,
    bool IsAppend)
{
    public ulong ExportRoot { get; init; }
    public bool DeleteAccess { get; init; }
    public bool DeleteOnClose { get; init; }
    // The requesting handle closed while another handle still held the inode.
    public ulong CommittedDeleteRoot { get; init; }
    public bool SharedWriter => IsWriter && (Share & FileShare.Write) != 0;

    /// <summary>The (inode, stream) key of the handle.</summary>
    public StreamKey Key => new(Inode, Stream);

    /// <summary>True when the handle may read.</summary>
    public bool CanRead => (Access & FileAccess.Read) != 0;
}

/// <summary>State of a staging session.</summary>
internal enum StagingState : byte
{
    Open = 0,
    Closed = 1,
}

/// <summary>
/// A staging session (ARCHITECTURE §5.4, §8): uploads are bound to it; closing it is the authoritative decision that its
/// unreferenced objects are garbage.
/// </summary>
/// <param name="Id">Staging id.</param>
/// <param name="Session">Owning client session (0 = leader-owned).</param>
/// <param name="Handle">Owning handle (0 = bound to the session).</param>
/// <param name="Transaction">Owning transaction (empty = none).</param>
/// <param name="Principal">Principal the session belongs to (capabilities are issued to it).</param>
/// <param name="State">Open or closed.</param>
/// <param name="CreatedIndex">Raft index of creation.</param>
/// <param name="ClosedIndex">Raft index of closing (0 while open).</param>
/// <param name="CreatedUtc">Leader wall clock of creation.</param>
internal sealed record StagingRecord(ulong Id, ulong Session, ulong Handle, DfsOperationId Transaction, string Principal, StagingState State,
    ulong CreatedIndex, ulong ClosedIndex, DateTimeOffset CreatedUtc)
{
    /// <summary>True while publications may still reference it.</summary>
    public bool IsOpen => State == StagingState.Open;
}

/// <summary>An application adapter recorded with a snapshot (FSD §11.3).</summary>
/// <param name="Id">Adapter id.</param>
/// <param name="Version">Adapter version.</param>
/// <param name="RestoreRequirements">Restore requirements (ordinal keys).</param>
internal sealed record AdapterRecord(string Id, string Version, ImmutableSortedDictionary<string, string> RestoreRequirements);

/// <summary>
/// A retained snapshot (ARCHITECTURE §5.7): the volume root captured at <see cref="BoundaryIndex"/> by structural sharing.
/// </summary>
/// <param name="Id">Snapshot id (derived deterministically from the volume id and the boundary index).</param>
/// <param name="Name">Unique name.</param>
/// <param name="BoundaryIndex">Raft index of the creating entry (the snapshot contains everything up to and including it).</param>
/// <param name="Label">Consistency label.</param>
/// <param name="Adapters">Adapters whose prepare succeeded.</param>
/// <param name="CreatedUtc">Leader wall clock of the creating entry.</param>
/// <param name="ExpiresUtc">Retention expiry (null = none).</param>
/// <param name="Hold">Retention hold.</param>
/// <param name="Root">Inode map at the boundary.</param>
internal sealed record SnapshotRecord(SnapshotId Id, string Name, ulong BoundaryIndex, DfsConsistencyLabel Label, ImmutableList<AdapterRecord> Adapters,
    DateTimeOffset CreatedUtc, DateTimeOffset? ExpiresUtc, bool Hold, ImmutableSortedDictionary<ulong, InodeRecord> Root)
{
    /// <summary>The retention as a public value.</summary>
    public DfsRetention Retention => new(ExpiresUtc, Hold);

    /// <summary>True when deletion is protected at <paramref name="now"/> (hold or unexpired expiry).</summary>
    public bool IsProtected(DateTimeOffset now) => Hold || (ExpiresUtc is { } e && e > now);
}

/// <summary>Kind of a GC pin (ARCHITECTURE §5.1).</summary>
internal enum PinKind : byte
{
    /// <summary>Protects the revisions an open handle may still read.</summary>
    Handle = 0,

    /// <summary>A snapshot reader (<c>OpenSnapshot</c>).</summary>
    SnapshotReader = 1,

    /// <summary>A backup transfer of a snapshot.</summary>
    Transfer = 2,

    /// <summary>A pinned directory enumeration (<c>ListDirectory</c> continuation).</summary>
    Enumeration = 3,
}

/// <summary>
/// A pin protecting older versions from garbage collection (ARCHITECTURE §5.1, §8). Reader and enumeration pins are renewed
/// by <c>KeepAlive</c> and expired by the leader; handle pins live as long as their handle.
/// </summary>
/// <param name="Id">Pin id.</param>
/// <param name="Kind">Pin kind.</param>
/// <param name="Session">Owning session (0 = leader-owned, for example a transfer pin).</param>
/// <param name="Principal">Principal that acquired the pin.</param>
/// <param name="CreatedIndex">Raft index at which the pin was acquired.</param>
/// <param name="PinIndex">Protection index: dereferenced objects at or above it stay protected.</param>
/// <param name="Handle">Owning handle (handle pins).</param>
/// <param name="Snapshot">Snapshot name (snapshot-reader and transfer pins).</param>
/// <param name="Directory">Enumerated directory (enumeration pins).</param>
/// <param name="Binding">Continuation binding of an enumeration (pattern, kinds, principal, snapshot view); empty otherwise.</param>
/// <param name="Root">The pinned namespace version (enumeration pins over the live view); null otherwise.</param>
/// <param name="Target">Backup target (transfer pins).</param>
internal sealed record PinRecord(ulong Id, PinKind Kind, ulong Session, string Principal, ulong CreatedIndex, ulong PinIndex, ulong Handle, string? Snapshot,
    ulong Directory, string Binding, ImmutableSortedDictionary<ulong, InodeRecord>? Root, string? Target);

/// <summary>State of a transaction (ARCHITECTURE §5.6).</summary>
internal enum TxState : byte
{
    Active = 0,
    Committed = 1,
    Aborted = 2,
}

/// <summary>
/// A transaction record (ARCHITECTURE §5.6). Transactions are implemented in the next phase; the record is part of the
/// state model now so checkpoints and later features only add logic.
/// </summary>
/// <param name="Id">Transaction id (the begin operation id).</param>
/// <param name="Session">Owning session.</param>
/// <param name="Principal">Owning principal.</param>
/// <param name="State">Lifecycle state.</param>
/// <param name="ReadIndex">Read index R the transaction reads from.</param>
/// <param name="Staging">Staging session of the transaction.</param>
/// <param name="PinId">GC pin protecting the read version.</param>
/// <param name="DeadlineUtc">Leader wall-clock deadline.</param>
/// <param name="Reservations">Writer reservations taken by the transaction.</param>
/// <param name="FinalIndex">Raft index of the commit or abort (0 while active).</param>
internal sealed record TxRecord(DfsOperationId Id, ulong Session, string Principal, TxState State, ulong ReadIndex, ulong Staging, ulong PinId,
    DateTimeOffset DeadlineUtc, ImmutableSortedSet<StreamKey> Reservations, ulong FinalIndex);

/// <summary>
/// One dereference: an object stopped being referenced by the current root at <see cref="Index"/> (ARCHITECTURE §8). Readers
/// pinned below the index may still need it, so GC keeps it while any pin is at or below the index.
/// </summary>
/// <param name="Index">Raft index of the dereferencing entry.</param>
/// <param name="Object">Dereferenced object.</param>
internal readonly record struct DerefEntry(ulong Index, ObjectId Object);

/// <summary>Backup progress of one registered target (ARCHITECTURE §12).</summary>
/// <param name="Target">Target name.</param>
/// <param name="LastSnapshot">Last acknowledged (published) snapshot name.</param>
/// <param name="LastSnapshotId">Its id.</param>
/// <param name="CoveredIndex">Raft index covered by it.</param>
/// <param name="RetentionExpiryUtc">Retention expiry enforced by the target.</param>
/// <param name="Held">True when the target holds it under a retention hold.</param>
/// <param name="ReseedRequired">True when the incremental lineage was abandoned.</param>
/// <param name="Lineage">Current transfer lineage.</param>
/// <param name="UpdatedUtc">Leader wall clock of the last update.</param>
internal sealed record BackupProgressRecord(string Target, string? LastSnapshot, SnapshotId LastSnapshotId, ulong CoveredIndex, DateTimeOffset? RetentionExpiryUtc,
    bool Held, bool ReseedRequired, Guid Lineage, DateTimeOffset UpdatedUtc);

/// <summary>Progress of a restore that installs a snapshot root into this (new) volume.</summary>
/// <param name="Source">Description of the source (volume and snapshot).</param>
/// <param name="Complete">True once the root was installed.</param>
/// <param name="Staged">Inodes received so far (installed atomically on completion).</param>
/// <param name="Staging">Staging session the copied objects were stored under (open until completion).</param>
/// <param name="CompletedIndex">Index of the entry that installed the namespace (0 while incomplete).</param>
internal sealed record RestoreRecord(string Source, bool Complete, ImmutableSortedDictionary<ulong, InodeRecord> Staged, ulong Staging, ulong CompletedIndex);

/// <summary>
/// A restore of one of this volume's snapshots into a new volume that has been accepted but whose outcome is not recorded
/// yet (PROTOCOL.md §8.5). The leader drives it (control plane, object copies, install, activation) and resumes it after a
/// leadership change; the snapshot's root is kept here so its objects stay reachable even if the snapshot is deleted
/// meanwhile. The outcome is recorded under <see cref="Operation"/> by <c>RestoreFinish</c>, which removes the intent.
/// </summary>
/// <param name="Operation">Operation id of the <c>RestoreSnapshot</c> request.</param>
/// <param name="PayloadHash">Payload identity of the request.</param>
/// <param name="Principal">Requesting principal.</param>
/// <param name="Snapshot">Snapshot name.</param>
/// <param name="SnapshotId">Snapshot id.</param>
/// <param name="BoundaryIndex">Snapshot boundary index.</param>
/// <param name="NewVolumeName">Name of the volume to create.</param>
/// <param name="StoragePool">Pool of the new volume (null = this volume's pool).</param>
/// <param name="Root">The snapshot root being restored.</param>
/// <param name="CreatedIndex">Index of the accepting entry.</param>
/// <param name="CreatedUtc">Leader time of the accepting entry.</param>
internal sealed record RestoreIntentRecord(DfsOperationId Operation, UInt128 PayloadHash, string Principal, string Snapshot, SnapshotId SnapshotId,
    ulong BoundaryIndex, string NewVolumeName, string? StoragePool, ImmutableSortedDictionary<ulong, InodeRecord> Root, ulong CreatedIndex,
    DateTimeOffset CreatedUtc);
