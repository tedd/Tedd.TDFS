# Tedd.TDFS — API Contract

Binding signatures. Bodies are omitted. `ct` = `CancellationToken ct = default` (always the final parameter).
Every network-bound public async method has a synchronous counterpart unless marked *(async-only)*; the sync
counterpart has the same name without `Async`, the same parameters minus `ct`, and returns the unwrapped result.
Implementations may add members, but may not change or remove these. Tests may only rely on members listed here
(plus `internal` members listed under "Test-visible internals", reachable via `InternalsVisibleTo`).

---

## A. `Tedd.Tdfs.Core` — namespace `Tedd.Tdfs`

### A.1 Identifiers and tokens

```csharp
public readonly record struct VolumeId(Guid Value) { public static VolumeId New(IRandomSource r); public override string ToString(); }
public readonly record struct NodeId(Guid Value)   { public static NodeId New(IRandomSource r); public override string ToString(); }
public readonly record struct InodeId(ulong Value) { public static readonly InodeId Root /* = 1 */; }
public readonly record struct ObjectId(UInt128 Value) { public static ObjectId New(IRandomSource r); public static ObjectId Parse(string s); public override string ToString(); /* 32 hex chars */ }
public readonly record struct SnapshotId(Guid Value);
public readonly record struct HandleId(ulong Value);
public readonly record struct StagingId(ulong Value);

public readonly record struct DfsOperationId(ulong Session, ulong Sequence)
{
    public bool IsEmpty { get; }
    public override string ToString();                 // "op:{Session:x}-{Sequence:x}"
    public static DfsOperationId Parse(string s);
    public static bool TryParse(string? s, out DfsOperationId id);
}
public readonly record struct DfsTransactionId(DfsOperationId Operation) { public override string ToString(); /* "tx:..." */ public static DfsTransactionId Parse(string s); }

public readonly record struct CommitToken(Guid ClusterId, VolumeId VolumeId, ulong VolumeEpoch, ulong Sequence)
{
    public static readonly CommitToken None;           // all zero
    public bool IsNone { get; }
    public bool IsComparableWith(CommitToken other);   // same cluster, volume, epoch, both non-None
    public int CompareTo(CommitToken other);           // throws ArgumentException when !IsComparableWith (OUT-018)
    public bool IsAtOrAfter(CommitToken other);        // throws ArgumentException when !IsComparableWith
    public override string ToString();                 // "ct:{cluster}/{volume}/{epoch}/{seq}"
    public static CommitToken Parse(string s);
}

public readonly record struct ClusterIdentity(Guid ClusterId, string Name);
```

### A.2 Errors

```csharp
public enum DfsErrorCode
{
    None = 0, Internal = 1, InvalidArgument = 2, InvalidPath = 3, FileNotFound = 4, DirectoryNotFound = 5,
    AlreadyExists = 6, DirectoryNotEmpty = 7, NotADirectory = 8, IsADirectory = 9, AccessDenied = 10,
    AuthenticationFailed = 11, SharingViolation = 12, WriterConflict = 13, TransactionConflict = 14,
    TransactionAborted = 15, LeaseLost = 16, PinExpired = 17, NotLeader = 18, QuorumUnavailable = 19,
    PlacementUnavailable = 20, NodeUnavailable = 21, Integrity = 22, OutcomeUnknown = 23, OperationExpired = 24,
    OperationIdMismatch = 25, LimitExceeded = 26, CapacityExhausted = 27, CrossVolume = 28, ReadOnly = 29,
    Unsupported = 30, ProtocolError = 31, ClusterMismatch = 32, EpochMismatch = 33, Timeout = 34, Cancelled = 35,
    SnapshotNotFound = 36, BackupTargetNotFound = 37, StaleIncarnation = 38, RecoveryKeyMissing = 39,
    InvalidPolicy = 40, ContinuationExpired = 41, InvalidHandle = 42, VolumeNotFound = 43, Unavailable = 44,
    AdapterFailed = 45, RetentionProtected = 46, BackupNotReached = 47
}

public class DfsException : IOException
{
    public DfsException(DfsErrorCode code, string message, Exception? inner = null);
    public DfsErrorCode ErrorCode { get; }
    public bool IsRetryable { get; init; }
    public DfsOperationId? OperationId { get; init; }
}
public class DfsSharingViolationException : DfsException      // ErrorCode SharingViolation or WriterConflict; IsRetryable = true
public class DfsTransactionConflictException : DfsException   // ErrorCode TransactionConflict; IsRetryable = true; DfsTransactionId TransactionId
public class DfsTransactionAbortedException : DfsTransactionConflictException // ErrorCode TransactionAborted (leader change/expiry/limits)
public class DfsLeaseLostException : DfsException              // ErrorCode LeaseLost
public class DfsPinExpiredException : DfsException             // ErrorCode PinExpired or ContinuationExpired
public class DfsUnavailableException : DfsException            // base availability; IsRetryable = true
public class DfsQuorumUnavailableException : DfsUnavailableException     // ErrorCode QuorumUnavailable
public class DfsPlacementUnavailableException : DfsUnavailableException  // ErrorCode PlacementUnavailable; int RequiredCopies, AchievedCopies, RequiredZones, AchievedZones
public class DfsCapacityException : DfsUnavailableException    // ErrorCode CapacityExhausted
public class DfsIntegrityException : DfsException              // ErrorCode Integrity; ObjectId? ObjectId
public class DfsCommitOutcomeUnknownException : DfsException   // ErrorCode OutcomeUnknown; required DfsOperationId OperationId; DfsTransactionId? TransactionId
public class DfsOperationExpiredException : DfsException        // ErrorCode OperationExpired
public class DfsOperationIdMismatchException : DfsException    // ErrorCode OperationIdMismatch
public class DfsLimitExceededException : DfsException          // ErrorCode LimitExceeded; string LimitName; long Limit; long Requested
public class DfsCrossVolumeException : DfsException            // ErrorCode CrossVolume
public class DfsReadOnlyException : DfsException               // ErrorCode ReadOnly (snapshot views, backup hosts)
public class DfsAlreadyExistsException : DfsException          // ErrorCode AlreadyExists
public class DfsDirectoryNotEmptyException : DfsException      // ErrorCode DirectoryNotEmpty
public class DfsClusterIdentityException : DfsException        // ErrorCode ClusterMismatch
public class DfsProtocolException : DfsException               // ErrorCode ProtocolError or Unsupported(required feature)
public class DfsEpochMismatchException : DfsException          // ErrorCode EpochMismatch
public class DfsAuthenticationException : UnauthorizedAccessException { public DfsErrorCode ErrorCode => AuthenticationFailed; }
public class DfsSnapshotAdapterException : DfsException        // ErrorCode AdapterFailed; string AdapterId; string Phase
public class DfsRecoveryKeyMissingException : DfsException     // ErrorCode RecoveryKeyMissing
public class DfsBackupNotReachedException : TimeoutException   // AwaitBackup* timeout; string Target; DfsBackupCheckpoint? LastPublished
public class DfsRetentionProtectedException : UnauthorizedAccessException // deleting protected backup history

public static class DfsErrors   // canonical mapping used by SDK and server (FMT-011)
{
    public static Exception ToException(DfsErrorCode code, string message, string? path = null, DfsOperationId? op = null);
    public static DfsErrorCode ToCode(Exception ex);
}
```
Standard mappings: `FileNotFound` → `FileNotFoundException`, `DirectoryNotFound` → `DirectoryNotFoundException`,
`AccessDenied` → `UnauthorizedAccessException`, `InvalidPath`/`InvalidArgument` → `ArgumentException`,
`Unsupported` (deferred OS feature) → `NotSupportedException`, `Cancelled` → `OperationCanceledException`,
`NotADirectory`/`IsADirectory` → `IOException` (`DfsException`).

### A.3 Paths

```csharp
public static class DfsPath
{
    public const int MaxComponentBytes = 255;
    public const int MaxPathBytes = 4096;
    public const string Root = "/";
    public static string Normalize(string path);                    // validates + NFC; throws ArgumentException (ParamName "path")
    public static bool TryNormalize(string? path, out string normalized, out string? error);
    public static IReadOnlyList<string> Split(string normalizedPath); // "/" -> []
    public static string Combine(string normalizedParent, string name);
    public static string GetParent(string normalizedPath);           // "/" for top-level; throws for "/"
    public static string GetName(string normalizedPath);
    public static bool IsAncestorOrSelf(string ancestor, string path);
    public static void ValidateComponent(string name);              // NFC-normalised component rules
}
```
Rules: absolute; `/` separator only; root `/`; no empty components, no trailing slash except root, no `.`/`..`,
no NUL, no backslash, no unpaired surrogates; NFC normalisation; component ≤ 255 UTF-8 bytes and full path ≤ 4096
UTF-8 bytes **after** normalisation; ordinal, case-sensitive; percent sequences are literal characters.

### A.4 Policies, topology, capabilities

```csharp
public sealed record DfsStoragePolicy
{
    public int TargetCopies { get; init; } = 3;
    public int TargetDistinctZones { get; init; } = 3;
    public int DurableCopiesBeforeAck { get; init; } = 2;
    public int DurableZonesBeforeAck { get; init; } = 2;
    public static DfsStoragePolicy Default { get; }
    public void Validate();   // throws DfsException(InvalidPolicy): counts < 1, ack > target, zones > copies, ackZones > ackCopies, ackZones > targetZones
}
public enum DfsVoterPlacement { OnePerZone, Explicit, Any }
public sealed record DfsMetadataPolicy
{
    public int Voters { get; init; } = 3;
    public DfsVoterPlacement Placement { get; init; } = DfsVoterPlacement.OnePerZone;
    public IReadOnlyList<string>? ExplicitVoterNodes { get; init; }   // node names, for Explicit
}
public sealed record DfsTopologyLabels(string Region, string Zone, string Rack, string Host, string Device);

public sealed record DfsVolumeLimits
{
    public long MaxFileLength { get; init; } = long.MaxValue / 2;
    public long MaxPublishBytes { get; init; } = 64L << 30;
    public TimeSpan MaxTransactionDuration { get; init; } = TimeSpan.FromMinutes(5);
    public int MaxTransactionTouchedInodes { get; init; } = 10_000;
    public int MaxTransactionDescriptorBytes { get; init; } = 4 << 20;
    public long MaxTransactionStagedBytes { get; init; } = 16L << 30;
    public int MaxPinsPerSession { get; init; } = 4096;
    public int MaxHandlesPerSession { get; init; } = 4096;
}

public sealed record DfsServerCapabilities
{
    public int ProtocolVersion { get; init; }
    public IReadOnlySet<string> Features { get; init; }
    public long MaxFileLength { get; init; }
    public int MaxPathBytes { get; init; }
    public int MaxComponentBytes { get; init; }
    public long MaxPublishBytes { get; init; }
    public int InlineThreshold { get; init; }
    public int ChunkSize { get; init; }
    public int MaxObjectSize { get; init; }
    public DfsVolumeLimits TransactionLimits { get; init; }
    public TimeSpan LeaseDuration { get; init; }
}

[Flags] public enum DfsPermissions
{
    None = 0, Read = 1, Write = 2, Snapshot = 4, BackupExport = 8, BackupAdmin = 16, RetentionAdmin = 32,
    VolumeAdmin = 64, ClusterAdmin = 128, All = 255
}
public sealed record DfsCredentials(string Principal, string Secret);
```

### A.5 Fault injection, randomness, mutations (public, used by harness and server)

```csharp
public enum FaultStage { F00, F01, F02, F03, F04, F05, F06, F07, F08, F09, F10, F11, F12, F13, F14, F15, F16, F17, F18, F19, F20, F21 }

public enum DfsOperationKind
{
    Unknown, Connect, Discover, Authenticate, OpenSession, KeepAlive, Open, CloseHandle, Read, ReadObject,
    PutObject, CreateStaging, CommitFile, WriteAll, Append, Copy, Move, Delete, CreateDirectory, Enumerate,
    GetInfo, Exists, TxBegin, TxOp, TxCommit, TxAbort, CreateSnapshot, DeleteSnapshot, OpenSnapshot, RestoreSnapshot,
    QueryOperation, BackupTransfer, BackupPublish, BackupAwait, Gc, Repair, Scrub, Compaction, RaftAppend,
    RaftVote, RaftSnapshot, MembershipChange, PolicyChange, KeyRotation, Admin
}

public readonly record struct FaultContext(
    FaultStage Stage, string Site, string Node, string? Volume = null, DfsOperationKind Operation = DfsOperationKind.Unknown,
    DfsOperationId? OperationId = null, string? Path = null, ObjectId? Object = null, int Occurrence = 0, string? Detail = null);

public interface IFaultInjector
{
    bool IsEnabled { get; }                                  // false => callers may skip building contexts
    ValueTask HitAsync(FaultContext context, CancellationToken ct = default); // may throw, delay, pause or crash the node
}
public sealed class NoFaults : IFaultInjector { public static readonly NoFaults Instance; }

public static class FaultSites   // stable site names; each constant documents its stage
{
    // Client (F00-F03, F13-F14)
    public const string ClientOperationStart = "client.op.start";               // F00
    public const string ClientValidated = "client.op.validated";                // F01
    public const string ClientConnect = "client.connect";                       // F02
    public const string ClientRequestSend = "client.request.send";              // F03
    public const string ClientUploadChunk = "client.upload.chunk";              // F03 (per chunk; Occurrence = chunk index)
    public const string ClientReplyReceived = "client.reply.received";          // F14
    // Server request path
    public const string RpcRequestAccepted = "rpc.request.accepted";            // F04
    public const string StagingRegistered = "staging.registered";              // F05
    public const string ObjectWrite = "object.write";                           // F06
    public const string ObjectBeforeFlush = "object.before-flush";              // F07
    public const string ObjectAfterFlushBeforeReceipt = "object.after-flush";   // F08
    public const string CommitReceiptValidated = "commit.receipt-validated";    // F09
    public const string RaftBeforePropose = "raft.before-propose";              // F10
    public const string RaftAfterLocalAppend = "raft.after-local-append";       // F10
    public const string RaftAfterCommitBeforeApply = "raft.after-commit";       // F11
    public const string ApplyAfterBeforeReply = "apply.after";                  // F12
    public const string RpcReplySend = "rpc.reply.send";                        // F13
    public const string CleanupRelease = "cleanup.release";                     // F15
    public const string RecoveryJournalReplay = "recovery.journal-replay";      // F16
    public const string RecoveryCheckpointInstall = "recovery.checkpoint-install"; // F16
    public const string RaftSnapshotInstall = "raft.snapshot-install";          // F16
    public const string SnapshotPin = "snapshot.pin";                           // F17
    public const string BackupInventory = "backup.inventory";                   // F17
    public const string BackupObjectReceived = "backup.object-received";        // F18
    public const string BackupBeforeClosureVerify = "backup.before-verify";     // F18
    public const string BackupBeforePointer = "backup.before-pointer";          // F19
    public const string BackupAfterPointer = "backup.after-pointer";            // F19
    public const string BackupAck = "backup.ack";                               // F19
    public const string GcSweep = "gc.sweep";                                   // F20
    public const string CompactionCopy = "compaction.copy";                     // F20
    public const string CompactionPublish = "compaction.publish";               // F20
    public const string CompactionDelete = "compaction.delete";                 // F20
    public const string ReplicaDelete = "replica.delete";                       // F20
    public const string Reconfiguration = "reconfig.transition";                // F21
    public const string KeyRotation = "key.rotation";                           // F21
    public static FaultStage StageOf(string site);
    public static IReadOnlyList<string> All { get; }
}

public interface IRandomSource
{
    ulong NextUInt64();
    int Next(int maxExclusive);
    void Fill(Span<byte> destination);
    IRandomSource Fork(string purpose);   // independent deterministic stream
}
public sealed class SeededRandom : IRandomSource { public SeededRandom(ulong seed); }
public sealed class SystemRandomSource : IRandomSource { public static readonly SystemRandomSource Instance; }

public sealed record SafetyMutations   // negative controls only (MUT-*); all false in production
{
    public bool SkipFencingCheck { get; init; }
    public bool AcknowledgeBeforeFlush { get; init; }
    public bool CommitMetadataBeforeData { get; init; }
    public bool QuorumFromReachableNodes { get; init; }
    public bool DisableOperationDedup { get; init; }
    public bool TreatUnknownAsAborted { get; init; }        // SDK-side mutation
    public bool SkipPredicateValidation { get; init; }
    public bool AllowStaleLiveReads { get; init; }
    public bool IgnoreSnapshotPins { get; init; }
    public bool PublishBackupBeforeClosure { get; init; }
    public bool SkipChecksumVerification { get; init; }
    public bool CountSameZoneTwice { get; init; }
    public bool ForgetExpiredIdWatermark { get; init; }
    public bool RelaxPolicyOnTimeout { get; init; }
    public static SafetyMutations None { get; }
    public bool Any { get; }
}
```

---

## B. `Tedd.Tdfs.Client` — namespace `Tedd.Tdfs`

### B.1 Client

```csharp
public sealed record DfsClientOptions
{
    public required IReadOnlyList<string> SeedEndpoints { get; init; }   // "quic://127.0.0.1:7100", "mem://n0"
    public Guid? ExpectedClusterId { get; init; }
    public required DfsCredentials Credentials { get; init; }
    public string ClientName { get; init; } = "client";
    public TimeSpan ConnectTimeout { get; init; } = TimeSpan.FromSeconds(10);
    public TimeSpan OperationTimeout { get; init; } = TimeSpan.FromSeconds(30);
    public TimeSpan ShutdownTimeout { get; init; } = TimeSpan.FromSeconds(5);
    public int DefaultBufferSize { get; init; } = 64 * 1024;
    public long MaxMaterializeBytes { get; init; } = 256L << 20;
    public int MaxRetryAttempts { get; init; } = 8;
    public TimeSpan RetryBaseDelay { get; init; } = TimeSpan.FromMilliseconds(20);
    public TimeSpan RetryMaxDelay { get; init; } = TimeSpan.FromSeconds(1);
    public DfsTransportSecurity Security { get; init; } = new();
    public TimeProvider TimeProvider { get; init; } = TimeProvider.System;
    public IDfsDiagnostics? Diagnostics { get; init; }
    public void Validate();   // ArgumentException for: no seeds, unparsable endpoint, null/empty credentials,
                              // nonpositive buffers/timeouts/limits, retry delays out of order (SDK-001)
    // advanced (internal set via DfsClientOptionsInternal for tests): transport factory, random source, fault injector, mutations
}
public sealed record DfsTransportSecurity
{
    public IReadOnlyList<byte[]> PinnedSpkiSha256 { get; init; } = [];
    public Func<X509Certificate2, bool>? ValidateServerCertificate { get; init; }
    public bool AllowUntrustedForDevelopment { get; init; }
}
public interface IDfsDiagnostics { void OnEvent(DfsDiagnosticEvent e); }
public sealed record DfsDiagnosticEvent(string Name, DfsOperationId? OperationId, DfsTransactionId? TransactionId,
    string? Volume, CommitToken? Token, string Message, Exception? Exception);   // never payloads/secrets

public sealed class DfsClient : IAsyncDisposable, IDisposable
{
    public static Task<DfsClient> ConnectAsync(DfsClientOptions options, CancellationToken ct = default);
    public static DfsClient Connect(DfsClientOptions options);
    public ClusterIdentity Cluster { get; }
    public DfsServerCapabilities Capabilities { get; }
    public string Principal { get; }
    public bool IsDisposed { get; }
    public DfsVolume GetVolume(string name);          // no I/O; validates name; same instance per name
    public DfsAdminClient Admin { get; }
    public ValueTask DisposeAsync();                   // idempotent; rejects new work (ObjectDisposedException);
    public void Dispose();                             // completes/cancels in-flight work within ShutdownTimeout;
                                                       // never commits transactions or flushes streams
}

public sealed class DfsVolume
{
    public string Name { get; }
    public DfsClient Client { get; }
    public DfsFileClient Files { get; }
    public DfsDirectoryClient Directories { get; }
    public DfsTransactionClient Transactions { get; }
    public DfsSnapshotClient Snapshots { get; }
    public DfsBackupClient Backups { get; }
    public Task<DfsVolumeInfo> GetInfoAsync(CancellationToken ct = default);
    public Task<DfsOperationId> NewOperationIdAsync(CancellationToken ct = default);   // reserves an id in this client's session
    public Task<DfsOperationOutcome> QueryOperationAsync(DfsOperationId id, bool resolve = false, CancellationToken ct = default);
}

public sealed record DfsVolumeInfo(VolumeId Id, string Name, ulong Epoch, DfsMetadataPolicy Metadata,
    DfsStoragePolicy DefaultPolicy, IReadOnlyDictionary<string, DfsStoragePolicy> NamedPolicies,
    int InlineThreshold, DfsVolumeLimits Limits, CommitToken LastCommit);

public enum DfsOutcomeStatus { Committed, Aborted, Pending, NotFound, Expired }
public sealed record DfsOperationOutcome(DfsOperationId Id, DfsOutcomeStatus Status, CommitToken Token, DfsErrorCode Error, string? Detail);
```

### B.2 Options for files

```csharp
public enum DfsWriteMode { OnFlush, WriteThrough }
public enum DfsReadView { Live, VersionPinned }

public sealed record DfsOpenOptions
{
    public FileMode Mode { get; init; } = FileMode.Open;
    public FileAccess Access { get; init; } = FileAccess.Read;
    public FileShare Share { get; init; } = FileShare.Read;
    public DfsWriteMode WriteMode { get; init; } = DfsWriteMode.OnFlush;
    public DfsReadView ReadView { get; init; } = DfsReadView.Live;
    public int BufferSize { get; init; } = 64 * 1024;
    public FileOptions Options { get; init; } = FileOptions.None;   // anything but None/Asynchronous/SequentialScan/RandomAccess -> NotSupportedException
    public string? StoragePolicy { get; init; }                       // named policy for newly created files
    public DfsOperationId? OperationId { get; init; }                 // for creating/truncating opens
}
public sealed record DfsMutationOptions
{
    public DfsOperationId? OperationId { get; init; }
    public string? StoragePolicy { get; init; }
    public string? CommitSnapshotName { get; init; }                  // commit-and-snapshot in the same decision
}
```

### B.3 File facade

```csharp
public sealed class DfsFileClient
{
    public bool IsReadOnly { get; }                    // snapshot views and backup hosts
    public bool IsTransactionBound { get; }
    // Open family
    public Task<DfsStream> OpenAsync(string path, DfsOpenOptions options, CancellationToken ct = default);
    public Task<DfsStream> OpenAsync(string path, FileMode mode, FileAccess access, FileShare share, CancellationToken ct = default);
    public Task<DfsStream> OpenReadAsync(string path, CancellationToken ct = default);       // Open, Read, Share.Read, Live
    public Task<DfsStream> OpenWriteAsync(string path, CancellationToken ct = default);      // OpenOrCreate, Write, Share.Read; no truncation
    public Task<DfsStream> CreateAsync(string path, CancellationToken ct = default);        // Create, ReadWrite, Share.Read
    // Whole-file helpers (atomic, durable on return, preserve inode identity; return CommitToken.None when transaction-bound)
    public Task<byte[]> ReadAllBytesAsync(string path, CancellationToken ct = default);
    public Task<string> ReadAllTextAsync(string path, Encoding? encoding = null, CancellationToken ct = default);
    public Task<string[]> ReadAllLinesAsync(string path, Encoding? encoding = null, CancellationToken ct = default);
    public IAsyncEnumerable<string> ReadLinesAsync(string path, Encoding? encoding = null, CancellationToken ct = default);
    public IEnumerable<string> ReadLines(string path, Encoding? encoding = null);
    public Task<CommitToken> WriteAllBytesAsync(string path, ReadOnlyMemory<byte> bytes, CancellationToken ct = default);
    public Task<CommitToken> WriteAllBytesAsync(string path, ReadOnlyMemory<byte> bytes, DfsMutationOptions options, CancellationToken ct = default);
    public Task<CommitToken> WriteAllTextAsync(string path, string contents, Encoding? encoding = null, CancellationToken ct = default);
    public Task<CommitToken> WriteAllLinesAsync(string path, IEnumerable<string> lines, Encoding? encoding = null, CancellationToken ct = default);
    public Task<CommitToken> AppendAllBytesAsync(string path, ReadOnlyMemory<byte> bytes, CancellationToken ct = default);
    public Task<CommitToken> AppendAllBytesAsync(string path, ReadOnlyMemory<byte> bytes, DfsMutationOptions options, CancellationToken ct = default);
    public Task<CommitToken> AppendAllTextAsync(string path, string contents, Encoding? encoding = null, CancellationToken ct = default);
    public Task<CommitToken> AppendAllLinesAsync(string path, IEnumerable<string> lines, Encoding? encoding = null, CancellationToken ct = default);
    public Task<CommitToken> CopyAsync(string sourcePath, string destinationPath, bool overwrite = false, CancellationToken ct = default);
    public Task<CommitToken> CopyAsync(string sourcePath, string destinationPath, bool overwrite, DfsMutationOptions options, CancellationToken ct = default);
    public Task<CommitToken> MoveAsync(string sourcePath, string destinationPath, bool overwrite = false, CancellationToken ct = default);
    public Task<CommitToken> MoveAsync(string sourcePath, string destinationPath, bool overwrite, DfsMutationOptions options, CancellationToken ct = default);
    public Task<CommitToken> DeleteAsync(string path, CancellationToken ct = default);   // absent -> success (returns CommitToken.None or a no-op token)
    public Task<CommitToken> DeleteAsync(string path, DfsMutationOptions options, CancellationToken ct = default);
    public Task<bool> ExistsAsync(string path, CancellationToken ct = default);          // false only for genuine absence
    public Task<DfsFileInfo> GetInfoAsync(string path, CancellationToken ct = default);  // FileNotFoundException when absent
    public Task<CommitToken> SetStoragePolicyAsync(string path, string policyName, CancellationToken ct = default);
    public Task<CommitToken> SetAccessAsync(string path, DfsInodeAccess access, CancellationToken ct = default);
    // + synchronous counterparts of every method above (Open, OpenRead, OpenWrite, Create, ReadAllBytes, ...)
}

public sealed record DfsFileInfo(string Path, InodeId InodeId, ulong Revision, long Length, DateTimeOffset CreatedUtc,
    DateTimeOffset ModifiedUtc, string Owner, DfsInodeAccess Access, string StoragePolicy, bool IsInline,
    bool IsSparse, int ExtentCount, CommitToken ObservedAt);
public sealed record DfsInodeAccess(bool OwnerOnly = false, bool ReadOnlyForOthers = false);
```

### B.4 Directory facade

```csharp
[Flags] public enum DfsEntryKinds { Files = 1, Directories = 2, All = 3 }
public enum DfsEntryKind { File, Directory }
public sealed record DfsEnumerationOptions
{
    public string Pattern { get; init; } = "*";          // '*' and '?' wildcards, ordinal
    public DfsEntryKinds Kinds { get; init; } = DfsEntryKinds.All;
    public int PageSize { get; init; } = 1000;           // 1..10000
}
public sealed record DfsEntryInfo(string Name, string Path, DfsEntryKind Kind, InodeId InodeId, ulong Revision, long Length, DateTimeOffset ModifiedUtc);
public sealed record DfsDirectoryInfo(string Path, InodeId InodeId, ulong Revision, DateTimeOffset CreatedUtc, DateTimeOffset ModifiedUtc, string Owner, int EntryCount, CommitToken ObservedAt);
public sealed record DfsDirectoryPage(IReadOnlyList<DfsEntryInfo> Entries, string? ContinuationToken, CommitToken PinnedAt);

public sealed class DfsDirectoryClient
{
    public bool IsReadOnly { get; }
    public bool IsTransactionBound { get; }
    public Task<DfsDirectoryInfo> CreateDirectoryAsync(string path, CancellationToken ct = default);           // recursive, idempotent
    public Task<DfsDirectoryInfo> CreateDirectoryAsync(string path, DfsMutationOptions options, CancellationToken ct = default);
    public Task<bool> ExistsAsync(string path, CancellationToken ct = default);
    public Task<DfsDirectoryInfo> GetInfoAsync(string path, CancellationToken ct = default);                   // DirectoryNotFoundException
    public Task<CommitToken> MoveAsync(string sourcePath, string destinationPath, CancellationToken ct = default);
    public Task<CommitToken> MoveAsync(string sourcePath, string destinationPath, DfsMutationOptions options, CancellationToken ct = default);
    public Task<CommitToken> DeleteAsync(string path, bool recursive = false, CancellationToken ct = default);  // absent -> DirectoryNotFoundException; nonempty && !recursive -> DfsDirectoryNotEmptyException
    public Task<CommitToken> DeleteAsync(string path, bool recursive, DfsMutationOptions options, CancellationToken ct = default);
    public IAsyncEnumerable<DfsEntryInfo> EnumerateAsync(string path, DfsEnumerationOptions? options = null, CancellationToken ct = default); // one pinned version
    public IEnumerable<DfsEntryInfo> Enumerate(string path, DfsEnumerationOptions? options = null);
    public Task<DfsDirectoryPage> ListPageAsync(string path, DfsEnumerationOptions? options = null, string? continuationToken = null, CancellationToken ct = default);
    // + synchronous counterparts
}
```

### B.5 Stream

```csharp
public sealed class DfsStream : Stream
{
    // Stream overrides: CanRead, CanWrite, CanSeek, CanTimeout(false), Length (may do network I/O), Position,
    // Read(byte[],int,int), Read(Span<byte>), ReadAsync(byte[],int,int,ct), ReadAsync(Memory<byte>,ct), ReadByte,
    // Write(byte[],int,int), Write(ReadOnlySpan<byte>), WriteAsync(byte[],int,int,ct), WriteAsync(ReadOnlyMemory<byte>,ct),
    // WriteByte, Seek, SetLength, Flush, FlushAsync(ct), CopyTo/CopyToAsync (base semantics), Dispose(bool), DisposeAsync,
    // ReadTimeout/WriteTimeout (throw InvalidOperationException).
    public string Path { get; }                          // path at open time (informational)
    public InodeId InodeId { get; }
    public ulong OpenRevision { get; }
    public DfsReadView ReadView { get; }
    public DfsWriteMode WriteMode { get; }
    public FileAccess Access { get; }
    public FileShare Share { get; }
    public bool IsAppend { get; }
    public bool IsTransactionBound { get; }
    public ulong WriterGeneration { get; }               // 0 for read-only handles
    public bool IsFaulted { get; }
    public Exception? Fault { get; }
    public long PendingBytes { get; }                    // staged, unpublished bytes (buffered + uploaded)
    public ValueTask<long> GetLengthAsync(CancellationToken ct = default);
    public ValueTask<DfsFileInfo> GetInfoAsync(CancellationToken ct = default);
    public Task SetLengthAsync(long length, CancellationToken ct = default);
    public Task<CommitToken> CommitAsync(CancellationToken ct = default);      // InvalidOperationException when transaction-bound
    public Task<CommitToken> CommitAsync(DfsMutationOptions options, CancellationToken ct = default); // commit-and-snapshot
    public Task AbortAsync(CancellationToken ct = default);                    // discard staged changes, release reservation
}
```

### B.6 Transactions

```csharp
public sealed record DfsTransactionOptions { public TimeSpan? Timeout { get; init; } public DfsOperationId? OperationId { get; init; } }
public enum DfsTransactionState { Active, Committing, Committed, Aborted, Unknown }
public sealed class DfsTransactionClient
{
    public Task<DfsTransaction> BeginAsync(DfsTransactionOptions? options = null, CancellationToken ct = default);
    public DfsTransaction Begin(DfsTransactionOptions? options = null);
    public Task<DfsOperationOutcome> QueryOutcomeAsync(DfsTransactionId id, bool resolve = false, CancellationToken ct = default);
}
public sealed class DfsTransaction : IAsyncDisposable, IDisposable
{
    public DfsTransactionId Id { get; }
    public DfsTransactionState State { get; }
    public CommitToken ReadVersion { get; }
    public DfsFileClient Files { get; }                  // transaction-bound
    public DfsDirectoryClient Directories { get; }       // transaction-bound
    public Task<CommitToken> CommitAsync(CancellationToken ct = default);
    public Task<DfsSnapshotInfo> CommitWithSnapshotAsync(string snapshotName, CancellationToken ct = default);
    public Task AbortAsync(CancellationToken ct = default);
    public CommitToken Commit(); public void Abort();
    public ValueTask DisposeAsync(); public void Dispose();   // abort if not committed; never commit
}
```

### B.7 Snapshots

```csharp
public enum DfsConsistencyLabel { FilesystemConsistent, ApplicationConsistent }
public sealed record DfsRetention(DateTimeOffset? ExpiresUtc = null, bool Hold = false);
public sealed record DfsSnapshotOptions
{
    public CommitToken? MinimumCommit { get; init; }
    public IReadOnlyList<IDfsSnapshotAdapter> Adapters { get; init; } = [];
    public TimeSpan AdapterTimeout { get; init; } = TimeSpan.FromSeconds(30);
    public DfsRetention? Retention { get; init; }
    public DfsOperationId? OperationId { get; init; }
}
public interface IDfsSnapshotAdapter
{
    string Id { get; }
    string Version { get; }
    IReadOnlyDictionary<string, string> RestoreRequirements { get; }
    Task PrepareAsync(DfsSnapshotAdapterContext context, CancellationToken ct);  // checkpoint/pause
    Task ResumeAsync(DfsSnapshotAdapterContext context, CancellationToken ct);   // always attempted after a successful prepare
    Task AbortAsync(DfsSnapshotAdapterContext context, CancellationToken ct);    // called when capture fails/times out
}
public sealed record DfsSnapshotAdapterContext(string Volume, string SnapshotName);
public sealed record DfsAdapterRecord(string Id, string Version, IReadOnlyDictionary<string, string> RestoreRequirements);
public sealed record DfsSnapshotInfo(SnapshotId Id, string Name, string Volume, CommitToken Boundary,
    DfsConsistencyLabel Label, IReadOnlyList<DfsAdapterRecord> Adapters, DateTimeOffset CreatedUtc, DfsRetention Retention,
    Exception? AdapterResumeFailure);
public sealed class DfsSnapshotClient
{
    public bool IsReadOnly { get; }      // true on backup hosts (create/delete rejected)
    public Task<DfsSnapshotInfo> CreateAsync(string name, DfsSnapshotOptions? options = null, CancellationToken ct = default);
    public Task<IReadOnlyList<DfsSnapshotInfo>> ListAsync(CancellationToken ct = default);
    public Task<DfsSnapshotInfo> GetAsync(string name, CancellationToken ct = default);   // DfsException(SnapshotNotFound)
    public Task<DfsSnapshotView> OpenAsync(string name, CancellationToken ct = default);
    public Task DeleteAsync(string name, CancellationToken ct = default);
    public Task SetRetentionAsync(string name, DfsRetention retention, CancellationToken ct = default);
    public Task<DfsVolumeInfo> RestoreAsync(string name, string newVolumeName, CancellationToken ct = default);
    // + synchronous counterparts
}
public sealed class DfsSnapshotView : IAsyncDisposable, IDisposable
{
    public DfsSnapshotInfo Info { get; }
    public DfsFileClient Files { get; }            // IsReadOnly = true; mutators throw DfsReadOnlyException
    public DfsDirectoryClient Directories { get; } // IsReadOnly = true
    public ValueTask DisposeAsync(); public void Dispose();   // releases the reader pin
}
```

### B.8 Backups

```csharp
public sealed record DfsBackupSchedule(TimeSpan? Interval = null, bool OnEverySnapshot = false);
public sealed record DfsBackupBudgets(TimeSpan MaxPinAge, long MaxPinnedBytes, long MaxBytesPerSecond = long.MaxValue, int MaxConcurrentObjects = 8);
public sealed record DfsBackupTargetRegistration(string Name, IReadOnlyList<string> Endpoints, DfsCredentials ExportCredentials,
    DfsBackupSchedule Schedule, DfsBackupBudgets Budgets);
public sealed record DfsBackupCheckpoint(string Target, SnapshotId SnapshotId, string SnapshotName, CommitToken CoveredCommit,
    DateTimeOffset? RetentionExpiryUtc, bool Held, bool IsExactSnapshot);
public enum DfsBackupState { Idle, Transferring, Verifying, Publishing, Offline, ReseedRequired, Failed }
public sealed record DfsBackupStatus(string Target, DfsBackupState State, long ReceivedBytes, CommitToken DurableBoundary,
    CommitToken PublishedBoundary, DfsBackupCheckpoint? LatestPublished, TimeSpan? RecoveryPointAge, bool ReseedRequired,
    long PinnedSourceBytes, string? LastError, Guid LineageId);
public sealed class DfsBackupClient
{
    public Task RegisterTargetAsync(DfsBackupTargetRegistration registration, CancellationToken ct = default);   // (async-only)
    public Task RemoveTargetAsync(string target, CancellationToken ct = default);
    public Task ConfigureAsync(string target, DfsBackupSchedule schedule, DfsBackupBudgets budgets, CancellationToken ct = default);
    public Task<IReadOnlyList<string>> ListTargetsAsync(CancellationToken ct = default);
    public Task<DfsBackupStatus> GetStatusAsync(string target, CancellationToken ct = default);
    public Task RequestSyncAsync(string target, string? snapshotName = null, CancellationToken ct = default);
    public Task<DfsBackupCheckpoint> AwaitBackupAsync(string target, CommitToken commit, TimeSpan timeout, CancellationToken ct = default);
    public Task<DfsBackupCheckpoint> AwaitBackupSnapshotAsync(string target, string snapshotName, TimeSpan timeout, CancellationToken ct = default);
}
```

### B.9 Administration *(async-only)*

```csharp
public enum DfsNodeStatus { Active, Draining, Drained, Removed, Unreachable }
public sealed record DfsNodeInfo(NodeId Id, string Name, DfsTopologyLabels Labels, IReadOnlyList<string> Endpoints,
    DfsNodeStatus Status, ulong Incarnation, bool IsControlVoter, bool IsReachable, long CapacityBytes, long UsedBytes);
public sealed record DfsGroupStatus(string Group, IReadOnlyList<string> Voters, IReadOnlyList<string> Learners, string? Leader,
    ulong Term, ulong CommitIndex, ulong AppliedIndex, bool HasQuorum);
public sealed record DfsReplicaHealth(long Objects, long UnderReplicatedObjects, long ZoneDeficitObjects,
    long MissingObjects, long PlacementViolations, long RepairBacklogBytes, long PendingGcObjects, long StagedBytes, long PinnedBytes);
public sealed record DfsVolumeStatus(DfsVolumeInfo Info, DfsGroupStatus Group, DfsReplicaHealth Replicas,
    IReadOnlyList<DfsBackupStatus> Backups);
public sealed record DfsClusterStatus(ClusterIdentity Cluster, DfsGroupStatus Control, IReadOnlyList<DfsNodeInfo> Nodes,
    IReadOnlyList<string> Volumes, ulong PlacementEpoch, IReadOnlySet<string> ActiveFeatures);
public sealed record DfsVolumeSpec
{
    public required string Name { get; init; }
    public DfsMetadataPolicy Metadata { get; init; } = new();
    public DfsStoragePolicy DefaultPolicy { get; init; } = DfsStoragePolicy.Default;
    public IReadOnlyDictionary<string, DfsStoragePolicy> NamedPolicies { get; init; } = new Dictionary<string, DfsStoragePolicy>();
    public int InlineThreshold { get; init; } = 4096;
    public DfsVolumeLimits Limits { get; init; } = new();
}
public sealed record DfsRestoreFromBackupRequest(IReadOnlyList<string> BackupEndpoints, DfsCredentials BackupCredentials,
    string SourceVolume, string SnapshotName, string NewVolumeName, byte[]? RecoveryKey = null);
public sealed class DfsAdminClient
{
    public Task<DfsClusterStatus> GetClusterStatusAsync(CancellationToken ct = default);
    public Task<IReadOnlyList<DfsNodeInfo>> ListNodesAsync(CancellationToken ct = default);
    public Task<DfsNodeInfo> AddNodeAsync(string name, DfsTopologyLabels labels, IReadOnlyList<string> endpoints, CancellationToken ct = default);
    public Task DrainNodeAsync(string name, CancellationToken ct = default);
    public Task RemoveNodeAsync(string name, bool force = false, CancellationToken ct = default);
    public Task SetNodeLabelsAsync(string name, DfsTopologyLabels labels, CancellationToken ct = default);
    public Task<DfsVolumeInfo> CreateVolumeAsync(DfsVolumeSpec spec, CancellationToken ct = default);
    public Task DeleteVolumeAsync(string name, CancellationToken ct = default);
    public Task<IReadOnlyList<string>> ListVolumesAsync(CancellationToken ct = default);
    public Task<DfsVolumeStatus> GetVolumeStatusAsync(string name, CancellationToken ct = default);
    public Task SetDefaultPolicyAsync(string volume, DfsStoragePolicy policy, bool migrateExisting, CancellationToken ct = default);
    public Task DefinePolicyAsync(string volume, string policyName, DfsStoragePolicy policy, CancellationToken ct = default);
    public Task<DfsPolicyMigrationStatus> GetPolicyMigrationStatusAsync(string volume, CancellationToken ct = default);
    public Task CreatePrincipalAsync(string principal, string secret, bool clusterAdmin = false, CancellationToken ct = default);
    public Task DeletePrincipalAsync(string principal, CancellationToken ct = default);
    public Task GrantAsync(string volume, string principal, DfsPermissions permissions, CancellationToken ct = default);
    public Task RevokeAsync(string volume, string principal, DfsPermissions permissions, CancellationToken ct = default);
    public Task RotateDataKeyAsync(string volume, CancellationToken ct = default);
    public Task<byte[]> ExportRecoveryKeyAsync(string passphrase, CancellationToken ct = default);
    public Task StartScrubAsync(string? node = null, CancellationToken ct = default);
    public Task StartRepairAsync(string volume, CancellationToken ct = default);
    public Task<DfsReplicaHealth> GetReplicaHealthAsync(string volume, CancellationToken ct = default);
    public Task<DfsVolumeInfo> RestoreFromBackupAsync(DfsRestoreFromBackupRequest request, CancellationToken ct = default);
    public Task SetFeatureAsync(string feature, bool enabled, CancellationToken ct = default);
    public Task<IReadOnlyList<string>> GetAuditLogAsync(int max = 1000, CancellationToken ct = default);
}
public sealed record DfsPolicyMigrationStatus(string Volume, DfsStoragePolicy Target, bool Effective, long PendingObjects);
```

---

## C. Test-visible internals (namespaces as shown)

### C.1 Wire codec — `Tedd.Tdfs.Protocol` (Core)

```csharp
public ref struct WireWriter
{
    public WireWriter(IBufferWriter<byte> output);
    public void WriteByte(byte v); public void WriteBool(bool v);
    public void WriteUInt16(ushort v); public void WriteUInt32(uint v); public void WriteUInt64(ulong v); public void WriteInt64(long v);  // little-endian fixed
    public void WriteVarUInt(ulong v); public void WriteVarInt(long v);        // LEB128 / zigzag
    public void WriteGuid(Guid v); public void WriteUInt128(UInt128 v);
    public void WriteBytes(ReadOnlySpan<byte> v);                               // varuint length prefix
    public void WriteString(string v);                                          // UTF-8, varuint length prefix
    public void WriteExtensions(ReadOnlySpan<WireExtension> extensions);        // terminal extension block
}
public readonly record struct WireExtension(uint FieldId, bool Required, ReadOnlyMemory<byte> Value);
public ref struct WireReader
{
    public WireReader(ReadOnlySpan<byte> input, WireLimits limits);
    public int Remaining { get; }
    public byte ReadByte(); public bool ReadBool(); public ushort ReadUInt16(); public uint ReadUInt32();
    public ulong ReadUInt64(); public long ReadInt64(); public ulong ReadVarUInt(); public long ReadVarInt();
    public Guid ReadGuid(); public UInt128 ReadUInt128();
    public ReadOnlySpan<byte> ReadBytes(); public string ReadString();
    public void ReadExtensions(ISet<uint> understoodFieldIds, Action<WireExtension>? onOptional = null); // unknown Required -> DfsProtocolException(Unsupported)
    public void EnsureEnd();                                                    // trailing bytes -> DfsProtocolException
}
public sealed record WireLimits(int MaxStringBytes = 64 * 1024, int MaxBytesLength = 64 << 20, int MaxCollectionCount = 1 << 20)
{ public static WireLimits Default { get; } }
// All malformed input (truncation, overlong varint > 10 bytes, length > limit or > remaining, invalid UTF-8) -> DfsProtocolException.

public interface IWireMessage { MessageType Type { get; } void Write(ref WireWriter writer); }
public enum MessageType : ushort { /* full catalogue defined in Core/Protocol/MessageType.cs */ }
public static class MessageCodec
{
    public static byte[] Encode(IWireMessage message);                          // [u16 type][body]
    public static IWireMessage Decode(ReadOnlySpan<byte> frame, WireLimits? limits = null);
    public static bool IsKnown(MessageType type);
}
public static class ErrorCodec   // FMT-011
{
    public static byte[] Encode(DfsErrorCode code, string message, DfsOperationId? op, bool retryable);
    public static (DfsErrorCode Code, string Message, DfsOperationId? Op, bool Retryable) Decode(ReadOnlySpan<byte> data);
}
```

### C.2 Checksums and placement — `Tedd.Tdfs.Data` (Core)

```csharp
public static class ObjectChecksum
{
    public static UInt128 Compute(ReadOnlySpan<byte> data);                      // XxHash128
    public static uint Crc32C(ReadOnlySpan<byte> data);
}
public sealed record PlacementNode(NodeId Id, string Name, DfsTopologyLabels Labels, bool Eligible, long FreeBytes);
public static class Placement
{
    // Deterministic HRW selection; result is the ordered target list (TargetCopies long when possible).
    public static IReadOnlyList<PlacementNode> SelectTargets(ObjectId obj, ulong placementEpoch, DfsStoragePolicy policy, IReadOnlyList<PlacementNode> nodes);
    public static IReadOnlyList<PlacementNode> FallbackOrder(ObjectId obj, ulong placementEpoch, IReadOnlyList<PlacementNode> nodes);
    public static (int Copies, int Zones) CountDomains(IEnumerable<DfsTopologyLabels> holders);   // distinct hosts / distinct (region,zone)
    public static bool IsSatisfiable(DfsStoragePolicy policy, IReadOnlyList<PlacementNode> nodes, out string? reason);
}
```

### C.3 Storage — `Tedd.Tdfs.Storage`

```csharp
public enum StorageOpenMode { CreateNew, OpenExisting, OpenOrCreate, Truncate }
public interface IStorageMedium
{
    string Name { get; }
    long CapacityBytes { get; }
    long UsedBytes { get; }
    ValueTask<IStorageFile> OpenAsync(string path, StorageOpenMode mode, CancellationToken ct = default);
    ValueTask<bool> ExistsAsync(string path, CancellationToken ct = default);
    ValueTask DeleteAsync(string path, CancellationToken ct = default);
    ValueTask RenameAsync(string from, string to, bool overwrite, CancellationToken ct = default);
    ValueTask<IReadOnlyList<string>> ListAsync(string directory, CancellationToken ct = default);
    ValueTask CreateDirectoryAsync(string directory, CancellationToken ct = default);
    ValueTask FlushDirectoryAsync(string directory, CancellationToken ct = default);   // persists create/rename/delete
}
public interface IStorageFile : IAsyncDisposable
{
    string Path { get; }
    long Length { get; }
    ValueTask<int> ReadAsync(long offset, Memory<byte> buffer, CancellationToken ct = default);
    ValueTask<int> WriteAsync(long offset, ReadOnlyMemory<byte> data, CancellationToken ct = default);  // may be short
    ValueTask FlushAsync(CancellationToken ct = default);                                                // durability barrier
    ValueTask SetLengthAsync(long length, CancellationToken ct = default);
}
public static class StorageFileExtensions
{
    public static ValueTask WriteFullyAsync(this IStorageFile f, long offset, ReadOnlyMemory<byte> data, CancellationToken ct = default); // loops short writes; 0-progress -> IOException
    public static ValueTask ReadFullyAsync(this IStorageFile f, long offset, Memory<byte> buffer, CancellationToken ct = default);
}
public sealed class FileSystemMedium : IStorageMedium { public FileSystemMedium(string rootDirectory, long capacityBytes = long.MaxValue); }

public enum CrashMode { LoseUnflushed, KeepUnflushed, RandomSubset, TearLastWrite }
public sealed class SimulatedMedium : IStorageMedium
{
    public SimulatedMedium(string name, long capacityBytes, IRandomSource random);
    public void Crash(CrashMode mode);                    // volatile state handled per mode; open files invalidated
    public SimulatedMediumFaults Faults { get; }
    public IReadOnlyList<string> AllFiles();             // durable+volatile view for inspection
    public byte[] ReadDurable(string path);              // durable image only
    public void CorruptByte(string path, long offset, byte xorMask = 0xFF);
    public SimulatedMedium CloneDurable(string newName); // disk clone (RSY-021)
    public long FlushCount { get; }
}
public sealed class SimulatedMediumFaults
{
    public bool ReadOnly { get; set; }
    public long? CapacityOverride { get; set; }            // ENOSPC when exceeded
    public int FailNextFlushes { get; set; }               // IOException
    public int FailNextWrites { get; set; }                // IOException (EIO)
    public int ShortNextWrites { get; set; }               // returns half the bytes
    public int FailNextReads { get; set; }
    public TimeSpan? IoDelay { get; set; }                 // via the node's TimeProvider
    public Func<string, bool>? PathFilter { get; set; }    // restrict faults to matching paths
}
public sealed record ObjectRecordInfo(ObjectId Id, VolumeId Volume, StagingId Staging, long Length, UInt128 Checksum, byte Encoding, uint KeyVersion);
public sealed class ObjectStore : IAsyncDisposable   // used by NodeHost; tests use it directly for DSK-*
{
    public static ValueTask<ObjectStore> OpenAsync(IStorageMedium medium, string directory, ObjectStoreOptions options, IFaultInjector faults, CancellationToken ct = default);
    public ValueTask<ObjectRecordInfo> PutAsync(ObjectId id, VolumeId volume, StagingId staging, ReadOnlyMemory<byte> plaintext, CancellationToken ct = default); // durable on return; idempotent; conflicting content -> DfsIntegrityException
    public ValueTask<int> ReadAsync(ObjectId id, long offset, Memory<byte> destination, CancellationToken ct = default);  // verified; corrupt -> DfsIntegrityException; missing -> DfsException(FileNotFound? no: Integrity w/ detail "missing")
    public ValueTask<bool> ContainsAsync(ObjectId id, CancellationToken ct = default);
    public ValueTask DeleteAsync(ObjectId id, CancellationToken ct = default);
    public IReadOnlyList<ObjectRecordInfo> List(VolumeId? volume = null);
    public ValueTask<ScrubResult> ScrubAsync(int maxObjects, CancellationToken ct = default);
    public ValueTask CompactAsync(CancellationToken ct = default);
    public long LiveBytes { get; } public long TotalBytes { get; }
}
public sealed record ObjectStoreOptions(long SegmentSize = 64 << 20, long ReservedBytes = 16 << 20, int MaxObjectSize = 4 << 20, IRecordCipher? Cipher = null);
public sealed record ScrubResult(int Checked, IReadOnlyList<ObjectId> Corrupt);
public interface IRecordCipher { int Overhead { get; } void Encrypt(ReadOnlySpan<byte> aad, ReadOnlySpan<byte> plain, Span<byte> output, uint keyVersion); bool TryDecrypt(ReadOnlySpan<byte> aad, ReadOnlySpan<byte> sealedData, Span<byte> plain, uint keyVersion); }
public sealed class Journal : IAsyncDisposable   // append-only CRC'd records (used by RaftLogStore, backups)
{
    public static ValueTask<Journal> OpenAsync(IStorageMedium medium, string directory, JournalOptions options, CancellationToken ct = default); // replays; truncates torn tail; corrupt interior -> DfsIntegrityException
    public IReadOnlyList<JournalRecord> Records { get; }  // replayed records (index, payload)
    public ValueTask<long> AppendAsync(ReadOnlyMemory<byte> payload, CancellationToken ct = default);   // not durable until FlushAsync
    public ValueTask FlushAsync(CancellationToken ct = default);
    public ValueTask TruncateAfterAsync(long index, CancellationToken ct = default);
    public ValueTask DropBeforeAsync(long index, CancellationToken ct = default);
}
public sealed record JournalOptions(long SegmentSize = 16 << 20, IRecordCipher? Cipher = null);
public sealed record JournalRecord(long Index, ReadOnlyMemory<byte> Payload);
public static class AtomicFile    // temp + flush + rename + dir flush
{
    public static ValueTask WriteAsync(IStorageMedium medium, string path, ReadOnlyMemory<byte> content, CancellationToken ct = default);
    public static ValueTask<byte[]?> ReadAsync(IStorageMedium medium, string path, CancellationToken ct = default); // null if absent; torn/corrupt -> DfsIntegrityException
}
```

### C.4 Consensus — `Tedd.Tdfs.Consensus`

```csharp
public enum RaftRole { Follower, PreCandidate, Candidate, Leader, Learner }
public readonly record struct RaftPeer(NodeId Id);
public sealed record RaftConfig(NodeId Self, int ElectionTicksMin = 30, int ElectionTicksMax = 60, int HeartbeatTicks = 5,
    bool PreVote = true, bool CheckQuorum = true, int MaxEntriesPerAppend = 256, int MaxInflightAppends = 64);
public sealed record RaftConfState(IReadOnlyList<NodeId> Voters, IReadOnlyList<NodeId> Learners);
public readonly record struct RaftHardState(ulong Term, NodeId? Vote, ulong Commit);
public enum RaftEntryKind : byte { Normal, ConfChange, Noop }
public sealed record RaftEntry(ulong Index, ulong Term, RaftEntryKind Kind, ReadOnlyMemory<byte> Data);
public enum RaftConfChangeKind { AddLearner, PromoteLearner, RemoveNode }
public sealed record RaftConfChange(RaftConfChangeKind Kind, NodeId Node);
public sealed record RaftSnapshotMeta(ulong Index, ulong Term, RaftConfState ConfState);
public abstract record RaftMessage(NodeId From, NodeId To, ulong Term);   // concrete: VoteRequest(PreVote flag), VoteResponse, AppendEntries, AppendResponse, InstallSnapshot(chunked), InstallSnapshotResponse, TimeoutNow, Heartbeat, HeartbeatResponse (each with ReadIndex context)
public sealed record RaftReadState(ulong Index, ulong RequestContext);
public sealed record RaftReady(RaftHardState? HardState, IReadOnlyList<RaftEntry> EntriesToPersist, IReadOnlyList<RaftEntry> CommittedEntries,
    IReadOnlyList<RaftMessage> Messages, RaftSnapshotMeta? SnapshotToInstall, ReadOnlyMemory<byte> SnapshotData,
    IReadOnlyList<RaftReadState> ReadStates, RaftRole Role, NodeId? Leader);
public interface IRaftLogView
{
    ulong FirstIndex { get; } ulong LastIndex { get; }
    ulong TermAt(ulong index);                       // throws if compacted (caller checks snapshot)
    IReadOnlyList<RaftEntry> Entries(ulong from, ulong toExclusive, int maxCount);
    RaftSnapshotMeta Snapshot { get; }
}
public sealed class InMemoryRaftLog : IRaftLogView { /* for tests & model checking */ public void Append(IEnumerable<RaftEntry> e); public void ApplySnapshot(RaftSnapshotMeta m); }
public sealed class RaftNode
{
    public RaftNode(RaftConfig config, RaftHardState hardState, RaftConfState confState, IRaftLogView log, IRandomSource random);
    public RaftRole Role { get; } public ulong Term { get; } public NodeId? Leader { get; }
    public ulong CommitIndex { get; } public ulong AppliedIndex { get; }
    public RaftConfState ConfState { get; }
    public void Tick();
    public void Step(RaftMessage message);
    public bool TryPropose(ReadOnlyMemory<byte> data, out ulong index);           // false if not leader / transfer in progress
    public bool TryProposeConfChange(RaftConfChange change, out ulong index);    // false if one pending
    public void ReadIndex(ulong requestContext);                                 // result in ReadStates (or dropped if not leader)
    public void TransferLeadership(NodeId target);
    public bool HasReady();
    public RaftReady GetReady();
    public void Advance(RaftReady ready);                                        // after persisting HardState/entries, applying committed
    public void ReportSnapshotStatus(NodeId peer, bool succeeded);
    public void ReportUnreachable(NodeId peer);
    public RaftNodeDebugState DebugState { get; }                                // for invariant checkers
}
public sealed record RaftNodeDebugState(NodeId Self, RaftRole Role, ulong Term, NodeId? Vote, NodeId? Leader, ulong Commit, ulong Applied, ulong LastIndex, ulong LastTerm, RaftConfState Conf);
```

### C.5 Server runtime — `Tedd.Tdfs.Server`

```csharp
public sealed record NodeOptions
{
    public required string Name { get; init; }
    public required DfsTopologyLabels Labels { get; init; }
    public required string ListenEndpoint { get; init; }           // "quic://127.0.0.1:0" or "mem://n0"
    public required string DataDirectory { get; init; }            // path within the medium
    public required ReadOnlyMemory<byte> ClusterSecret { get; init; }
    public ClusterBootstrap? Bootstrap { get; init; }              // static bootstrap or single-node bootstrap
    public IReadOnlyList<string> JoinSeeds { get; init; } = [];
    public NodeTimings Timings { get; init; } = NodeTimings.Production;
    public long CapacityBytes { get; init; } = long.MaxValue;
    public long ReservedBytes { get; init; } = 64L << 20;
    public SafetyMutations Mutations { get; init; } = SafetyMutations.None;
    public bool EncryptAtRest { get; init; } = true;
    public string? CertificatePath { get; init; }                  // PFX for QUIC; null => generated self-signed (dev/test)
}
public sealed record NodeTimings(TimeSpan Tick, int HeartbeatTicks, int ElectionTicksMin, int ElectionTicksMax,
    TimeSpan LeaseDuration, TimeSpan SessionTimeout, int MaxOutcomesPerSession, TimeSpan OutcomeRetention,
    TimeSpan GcInterval, TimeSpan InventoryInterval, TimeSpan ScrubInterval, int RaftSnapshotThreshold,
    int ChunkSize, int MaxObjectSize, TimeSpan PinDuration, TimeSpan EnumerationPinDuration, long RepairBytesPerSecond)
{ public static NodeTimings Production { get; } public static NodeTimings Fast { get; } }
public sealed record ClusterBootstrap(Guid ClusterId, string ClusterName, IReadOnlyList<BootstrapMember> Members,
    IReadOnlyList<string> ControlVoters, IReadOnlyList<BootstrapPrincipal> Principals);
public sealed record BootstrapMember(NodeId Id, string Name, DfsTopologyLabels Labels, IReadOnlyList<string> Endpoints);
public sealed record BootstrapPrincipal(string Name, string Secret, bool ClusterAdmin);

public sealed class NodeEnvironment
{
    public required IStorageMedium Medium { get; init; }
    public required IRpcTransportFactory TransportFactory { get; init; }
    public required TimeProvider Time { get; init; }
    public required IRandomSource Random { get; init; }
    public IFaultInjector Faults { get; init; } = NoFaults.Instance;
    public INodeLog Log { get; init; } = NullNodeLog.Instance;
    public INodeLoopFactory LoopFactory { get; init; } = ThreadNodeLoopFactory.Instance;  // sim supplies its own
}
public interface INodeLog { void Write(NodeLogLevel level, string node, string category, string message); }
public enum NodeLogLevel { Trace, Debug, Info, Warn, Error }

public sealed class NodeHost : IAsyncDisposable
{
    public static Task<NodeHost> StartAsync(NodeOptions options, NodeEnvironment environment, CancellationToken ct = default);
    public string Name { get; }
    public NodeId Id { get; }
    public ulong Incarnation { get; }
    public string Endpoint { get; }                    // actual bound endpoint
    public NodeInspector Inspect { get; }              // read-only diagnostics (posts to the node loop)
    public Task StopAsync(CancellationToken ct = default);   // graceful: stop accepting, drain, flush, close
    public void Kill();                                // abrupt: stop the loop immediately; no flush, no cleanup (crash)
    public void Pause(); public void Resume();         // freeze/unfreeze the node loop (memory retained)
    public ValueTask DisposeAsync();
}
public sealed class BackupNodeHost : IAsyncDisposable
{
    public static Task<BackupNodeHost> StartAsync(BackupNodeOptions options, NodeEnvironment environment, CancellationToken ct = default);
    public string Name { get; } public string Endpoint { get; } public BackupInspector Inspect { get; }
    public Task StopAsync(CancellationToken ct = default); public void Kill(); public void Pause(); public void Resume();
    public ValueTask DisposeAsync();
}
public sealed record BackupNodeOptions
{
    public required string Name { get; init; }
    public required string ListenEndpoint { get; init; }
    public required string DataDirectory { get; init; }
    public required Guid SourceClusterId { get; init; }
    public required ReadOnlyMemory<byte> BackupSecret { get; init; }      // backup KEK root; missing -> RecoveryKeyMissing on start
    public required IReadOnlyList<BootstrapPrincipal> Principals { get; init; }   // readers, export principal (no retention rights), retention admin
    public IReadOnlyDictionary<string, DfsPermissions> Grants { get; init; } = new Dictionary<string, DfsPermissions>();
    public DfsRetention DefaultRetention { get; init; } = new(null, false);
    public int KeepLastSnapshots { get; init; } = 16;
    public long CapacityBytes { get; init; } = long.MaxValue;
    public NodeTimings Timings { get; init; } = NodeTimings.Production;
}

public sealed class NodeInspector   // all async, executed on the node loop
{
    public Task<NodeCounters> GetCountersAsync();
    public Task<IReadOnlyList<GroupInspection>> GetGroupsAsync();        // control + volume groups on this node
    public Task<GroupInspection?> GetGroupAsync(string group);           // "control" or volume name
    public Task<IReadOnlyList<ObjectRecordInfo>> ListObjectsAsync(string? volume = null);
    public Task<bool> HasObjectAsync(ObjectId id);
    public Task<VolumeStateInspection?> GetVolumeStateAsync(string volume);   // this replica's applied state view
    public Task<long> GetStagedBytesAsync(); public Task<long> GetPinnedBytesAsync();
    public Task<string> ComputeStateDigestAsync();                      // hash of applied replicated state (all groups)
}
public sealed record GroupInspection(string Group, RaftRole Role, ulong Term, string? Leader, ulong CommitIndex, ulong AppliedIndex,
    ulong LastIndex, ulong SnapshotIndex, IReadOnlyList<string> Voters, IReadOnlyList<string> Learners);
public sealed record NodeCounters(long ObjectsStored, long ObjectBytesStored, long ObjectBytesReceived, long ObjectPuts,
    long ReceiptsIssued, long ObjectReads, long ObjectBytesRead, long ChecksumFailures, long RaftEntriesAppended,
    long RaftEntriesApplied, long Elections, long LeaseExpiries, long TransactionConflicts, long GcDeletedObjects,
    long GcDeletedBytes, long RepairBytes, long BackupBytesSent, long RpcRequestsHandled, long RejectedStaleWrites);
public sealed record VolumeStateInspection(VolumeId Id, ulong Epoch, ulong AppliedIndex, int InodeCount,
    IReadOnlyList<HandleInspection> Handles, IReadOnlyList<SnapshotInspection> Snapshots, IReadOnlyList<SessionInspection> Sessions,
    IReadOnlyList<PinInspection> Pins, IReadOnlyList<StagingInspection> Staging, int ActiveTransactions,
    IReadOnlyList<BackupProgressInspection> Backups)
{
    public FileInspection? GetFile(string path);
    public IReadOnlySet<ObjectId> ReachableObjects();
}
public sealed record FileInspection(string Path, InodeId Inode, ulong Revision, long Length, bool IsInline, ulong WriterGeneration,
    string Policy, IReadOnlyList<ExtentInspection> Extents);
public sealed record ExtentInspection(long Offset, long Length, ObjectId Object, long ObjectOffset, long ObjectLength);
public sealed record HandleInspection(HandleId Id, InodeId Inode, ulong Session, FileAccess Access, FileShare Share, bool IsWriter, ulong Generation, bool Expired);
public sealed record SnapshotInspection(SnapshotId Id, string Name, ulong BoundaryIndex, DfsConsistencyLabel Label, int InodeCount);
public sealed record SessionInspection(ulong Id, string Principal, bool Expired, ulong RetiredWatermark, int RetainedOutcomes);
public sealed record PinInspection(ulong Id, string Kind, ulong CreatedIndex, string? Snapshot);
public sealed record StagingInspection(StagingId Id, bool Open, ulong CreatedIndex);
public sealed record BackupProgressInspection(string Target, string? LastSnapshot, ulong CoveredIndex, bool ReseedRequired, Guid Lineage);
public sealed class BackupInspector
{
    public Task<IReadOnlyList<string>> GetPublishedSnapshotsAsync(string volume);
    public Task<string?> GetPublishedPointerAsync(string volume);
    public Task<IReadOnlyList<ObjectRecordInfo>> ListObjectsAsync();
    public Task<long> GetStagedBytesAsync();
}
```

### C.6 Transport — `Tedd.Tdfs.Transport`

```csharp
public readonly record struct RpcAddress(string Scheme, string Host, int Port, string? Name)
{ public static RpcAddress Parse(string endpoint); public override string ToString(); }
public sealed record RpcPeerIdentity(string? NodeName, NodeId? NodeId, ulong Incarnation, string? Principal, bool IsNode, bool IsBackup);
public interface IRpcConnection : IAsyncDisposable
{
    RpcAddress Remote { get; }
    bool IsOpen { get; }
    Task<ReadOnlyMemory<byte>> CallAsync(ReadOnlyMemory<byte> request, TimeSpan timeout, CancellationToken ct = default); // TimeoutException / RpcConnectionException
    ValueTask SendAsync(ReadOnlyMemory<byte> message, CancellationToken ct = default);
    RpcPeerIdentity? Identity { get; set; }
    event Action<IRpcConnection, Exception?>? Closed;
}
public interface IRpcHandler
{
    ValueTask<ReadOnlyMemory<byte>?> HandleAsync(IRpcConnection connection, ReadOnlyMemory<byte> request, bool expectsReply, CancellationToken ct);
}
public interface IRpcTransport : IAsyncDisposable
{
    string LocalEndpoint { get; }                    // actual bound endpoint after StartAsync
    Task StartAsync(IRpcHandler handler, CancellationToken ct = default);
    Task<IRpcConnection> ConnectAsync(string endpoint, CancellationToken ct = default);
}
public interface IRpcTransportFactory { IRpcTransport Create(string localName, string listenEndpoint); }
public sealed class RpcConnectionException : IOException { public RpcConnectionException(string message, Exception? inner = null); }
public enum LinkDecisionKind { Deliver, Drop, Delay, Duplicate, BlackHole }
public readonly record struct LinkDecision(LinkDecisionKind Kind, TimeSpan Delay = default);
public enum LinkDirection { Request, Reply, OneWay }
public interface ILinkPolicy
{
    LinkDecision Decide(string fromName, string toName, LinkDirection direction, ReadOnlySpan<byte> payloadPrefix);
    bool CanConnect(string fromName, string toName);
}
public sealed class InMemoryNetwork { public InMemoryNetwork(ILinkPolicy? policy = null); public IRpcTransportFactory Factory { get; } public ILinkPolicy Policy { get; set; } }
public sealed class QuiclyTransportFactory : IRpcTransportFactory
{
    public QuiclyTransportFactory(QuiclyTransportOptions options);
}
public sealed record QuiclyTransportOptions(X509Certificate2 Certificate, IReadOnlyList<byte[]> TrustedSpkiPins, ILinkPolicy? LinkPolicy = null, int MaxConnectionsPerAddress = 4096);

// Additions (implementation phase): both built-in transports implement IRpcTransportControl.
public interface IRpcTransportControl
{
    string LocalName { get; }
    int OpenConnectionCount { get; }
    void ResetConnections(string? remoteName = null);   // abortive; both ends see Closed(exception); listening continues
}
// InMemoryNetwork: + InMemoryNetwork(ILinkPolicy? policy, TimeProvider timeProvider); TimeProvider TimeProvider { get; };
//                  IReadOnlyList<string> Endpoints { get; }; void ResetConnections(string name);   // every connection with `name` at either end
// QuiclyTransportFactory: + QuiclyTransportOptions Options { get; }; static X509Certificate2 CreateDevelopmentCertificate();
//                  static X509Certificate2 CreateDevelopmentCertificate(string subject, TimeSpan validity); static byte[] ComputeSpkiPin(X509Certificate2 certificate);
// QuiclyTransportOptions (init): TimeProvider TimeProvider = System; TimeSpan ConnectTimeout = 10 s; Func<X509Certificate2, bool>? ValidateServerCertificate;
//                  bool AllowUntrustedForDevelopment; int MaxMessageBytes = 256 MiB; long MaxReassemblyBytesPerConnection = 512 MiB;
//                  int MaxInboundConnections = 4096; Action<string>? Diagnostic.   Certificate may be null for connect-only transports.
```
Semantics (both transports): delivery is asynchronous (thread pool, never on the sender's stack); the receiving end consults
`ILinkPolicy.Decide(from, to, direction, payload)` once per message with the **complete** payload; handlers are invoked in
arrival order per connection (not awaited before the next message); a handler exception becomes an `ErrorCodec`
`Internal` error reply and a request to a never-started transport an `Unavailable` one; replies after a timeout are
discarded; closing fails pending calls with `RpcConnectionException`; `Closed` carries null only for a local
`DisposeAsync`/transport dispose. A remote close is ordered behind the messages already received on the connection (a reply
that arrived before the close still completes its call). Closing cancels the handlers' token asynchronously: cancellation
callbacks and the handler code they resume never run on the closing thread (or the QUIC pump). QUIC: single-frame messages
(≤ 60 KiB) keep send order on the ordered channel; fragmented ones use the unordered data channel and may be overtaken by
later small messages. A graceful close (`DisposeAsync` of a connection or transport) fails messages still queued in the
transport but first lets everything already handed to QUIC (every message whose `SendAsync` completed) be acknowledged by
the peer, bounded by a 2 s grace period, before the QUIC session is closed.

---

## D. Test harness — `Tedd.Tdfs.Testing`

### D.1 Running a cluster scenario

```csharp
public enum ClusterMode { Realtime, Deterministic }
public enum TransportKind { InMemory, Quic }
public enum StorageKind { Simulated, Disk }
public enum BootstrapMode { Static, SeedJoin }
public enum SeedGraph { Full, Ring, Star, Line, RandomConnected }      // who knows whom at startup (SeedJoin)
public enum LinkMesh { Full, Ring, Star, Line }                        // which node-to-node links exist (fault model)
public enum TestTopology { OnePerZone, SingleZone, TwoPlusOne, Custom }

public sealed record TestNodeSpec(string Name, string Zone, string? Host = null, string Region = "r1", string Rack = "rk1", string Device = "d1");

public sealed record TestVolumeSpec(string Name)
{
    public DfsMetadataPolicy Metadata { get; init; } = new();
    public DfsStoragePolicy? DefaultPolicy { get; init; }              // null -> cluster default for its size
    public IReadOnlyDictionary<string, DfsStoragePolicy> NamedPolicies { get; init; } = new Dictionary<string, DfsStoragePolicy>();
    public int InlineThreshold { get; init; } = 4096;
    public DfsVolumeLimits? Limits { get; init; }
}

public sealed record TestClusterOptions
{
    public int NodeCount { get; init; } = 3;                          // 1..10 (ignored when Nodes is set)
    public IReadOnlyList<TestNodeSpec>? Nodes { get; init; }
    public TestTopology Topology { get; init; } = TestTopology.OnePerZone;
    public int ControlVoters { get; init; } = 3;                      // clipped to node count
    public IReadOnlyList<TestVolumeSpec> Volumes { get; init; } = [new("vol")];
    public DfsStoragePolicy? DefaultPolicy { get; init; }             // null -> {target=min(3,n), zones=min(3,zones), ack=min(2,n), ackZones=min(2,zones)}
    public int BackupNodeCount { get; init; } = 0;
    public ClusterMode Mode { get; init; } = ClusterMode.Realtime;
    public TransportKind Transport { get; init; } = TransportKind.InMemory;
    public StorageKind Storage { get; init; } = StorageKind.Simulated;
    public BootstrapMode Bootstrap { get; init; } = BootstrapMode.Static;
    public SeedGraph Seeds { get; init; } = SeedGraph.Full;
    public LinkMesh Mesh { get; init; } = LinkMesh.Full;
    public ulong Seed { get; init; } = 0;                             // 0 -> derive from test name; always reported on failure
    public NodeTimings Timings { get; init; } = NodeTimings.Fast;
    public bool EncryptAtRest { get; init; } = true;
    public SafetyMutations Mutations { get; init; } = SafetyMutations.None;
    public long NodeCapacityBytes { get; init; } = 1L << 30;
    public long ReservedBytes { get; init; } = 8L << 20;
    public TimeSpan ScenarioTimeout { get; init; } = TimeSpan.FromSeconds(120);   // realtime wall clock / deterministic virtual time
    public bool OutOfProcess { get; init; } = false;                 // real Tedd.Tdfs.Host processes (Quic + Disk only)
    public string? DataRoot { get; init; }                            // default: $TDFS_TEST_ROOT or %TEMP%/tdfs-tests, per test subfolder
    public Action<string, NodeOptions>? ConfigureNode { get; init; }  // last-chance per-node override (name, options) -> use `with` via ConfigureNodeOptions
    public Func<string, NodeOptions, NodeOptions>? ConfigureNodeOptions { get; init; }
    public static TestClusterOptions SingleNode { get; }              // 1 node, 1 voter, policy 1/1/1/1
    public static TestClusterOptions ThreeZones { get; }              // default 3-node one-per-zone
    public static TestClusterOptions FiveNodes { get; }               // 5 nodes, 5 voters, zones z1..z5
}

public static class ClusterTest
{
    // Starts a cluster, waits until stable (control + all volume leaders elected, all nodes caught up),
    // runs the scenario (inside the simulation in Deterministic mode), checks harness invariants, disposes.
    public static Task RunAsync(TestClusterOptions options, Func<TestCluster, Task> scenario, CancellationToken ct = default);
    public static Task<T> RunAsync<T>(TestClusterOptions options, Func<TestCluster, Task<T>> scenario, CancellationToken ct = default);
}
```

### D.2 TestCluster

```csharp
public sealed class TestCluster : IAsyncDisposable
{
    public static Task<TestCluster> StartAsync(TestClusterOptions options, CancellationToken ct = default); // prefer ClusterTest.RunAsync
    public TestClusterOptions Options { get; }
    public ClusterMode Mode { get; }
    public ulong Seed { get; }
    public Guid ClusterId { get; }
    public string DataRoot { get; }
    public IReadOnlyList<TestNode> Nodes { get; }                    // live nodes in creation order (n0, n1, ...)
    public IReadOnlyList<TestBackupNode> BackupNodes { get; }        // b0, b1, ...
    public TestNode this[int index] { get; }
    public TestNode Node(string name);
    public TestNetwork Network { get; }
    public TestFaults Faults { get; }
    public ClusterInspector Inspect { get; }
    public HistoryRecorder History { get; }
    public TimeProvider Time { get; }                                 // harness/test clock (virtual in Deterministic mode)
    public DfsAdminClient Admin { get; }                              // admin client connected as "admin"
    public DfsCredentials AdminCredentials { get; }                   // principal "admin" (ClusterAdmin, all grants)

    public Task<DfsClient> ConnectAsync(TestClientOptions? options = null, CancellationToken ct = default);
    public DfsClientOptions CreateClientOptions(TestClientOptions? options = null);
    public Task<DfsClient> ConnectBackupAsync(int backupIndex = 0, TestClientOptions? options = null, CancellationToken ct = default);
    public Task<TestPrincipal> CreatePrincipalAsync(string name, IReadOnlyDictionary<string, DfsPermissions> grants, bool clusterAdmin = false);

    public Task<TestNode> AddNodeAsync(TestNodeSpec spec, bool join = true, CancellationToken ct = default);
    public Task<TestBackupNode> AddBackupNodeAsync(string? name = null, CancellationToken ct = default);
    public Task<TestNode> GetLeaderAsync(string volume, CancellationToken ct = default);    // waits for a leader
    public Task<TestNode> GetControlLeaderAsync(CancellationToken ct = default);
    public Task WaitForStableAsync(TimeSpan? timeout = null, CancellationToken ct = default);
    public Task WaitForReplicationAsync(string volume, CancellationToken ct = default);     // all reachable voters applied == leader commit
    public Task WaitForRepairAsync(string volume, TimeSpan? timeout = null, CancellationToken ct = default); // no under-replication
    public Task WaitUntilAsync(Func<Task<bool>> condition, TimeSpan timeout, string because, CancellationToken ct = default); // TimeoutException with message
    public Task DelayAsync(TimeSpan delay, CancellationToken ct = default);                // virtual in Deterministic mode
    public Task<string> ComputeDigestAsync(CancellationToken ct = default);                // cluster-wide replicated-state digest
    public IReadOnlyList<string> Trace { get; }                                              // fault/network/node events (replay artifact)
    public ValueTask DisposeAsync();
}

public sealed record TestClientOptions
{
    public string Name { get; init; } = "c0";                         // network identity "client:{Name}"
    public DfsCredentials? Credentials { get; init; }                 // default admin
    public IReadOnlyList<string>? SeedNodes { get; init; }            // node names; default all live nodes
    public TimeSpan? OperationTimeout { get; init; }                  // default 10 s
    public TimeSpan? ConnectTimeout { get; init; }
    public int? BufferSize { get; init; }
    public long? MaxMaterializeBytes { get; init; }
    public Guid? ExpectedClusterId { get; init; }                     // default: the cluster's id
    public TestClock? Clock { get; init; }                            // default: a fresh client clock (independent)
    public SafetyMutations Mutations { get; init; } = SafetyMutations.None;
    public IDfsDiagnostics? Diagnostics { get; init; }
}
public sealed record TestPrincipal(string Name, DfsCredentials Credentials);
```

### D.3 Nodes, disks, clocks

```csharp
public enum TestNodeState { Running, Paused, Crashed, Stopped }
public sealed class TestNode
{
    public string Name { get; } public int Index { get; } public NodeId Id { get; }
    public DfsTopologyLabels Labels { get; } public string Endpoint { get; }
    public TestNodeState State { get; } public ulong Incarnation { get; }
    public TestClock Clock { get; }
    public TestDisk Disk { get; }
    public NodeInspector Inspect { get; }                             // throws InvalidOperationException unless Running
    public NodeHost? Host { get; }                                    // null unless Running/Paused (in-process only)
    public Task CrashAsync(CrashMode mode = CrashMode.LoseUnflushed); // abrupt; disk keeps only what the mode allows
    public Task StopAsync();                                          // graceful
    public Task RestartAsync(Func<NodeOptions, NodeOptions>? reconfigure = null);   // from durable image; incarnation+1
    public Task PauseAsync(); public Task ResumeAsync();
    public Task ReplaceDiskWithEmptyAsync();                          // node must be stopped/crashed
    public Task CloneDiskFromAsync(TestNode source);                  // node must be stopped/crashed (RSY-021)
    public Task<NodeCounters> GetCountersAsync();
}
public sealed class TestBackupNode
{
    public string Name { get; } public int Index { get; } public string Endpoint { get; }
    public TestNodeState State { get; } public TestDisk Disk { get; } public TestClock Clock { get; }
    public BackupInspector Inspect { get; }
    public string TargetName { get; }                                 // registration name used in DfsBackupClient ("b0")
    public DfsCredentials ExportCredentials { get; } public DfsCredentials ReaderCredentials { get; } public DfsCredentials RetentionAdminCredentials { get; }
    public Task CrashAsync(CrashMode mode = CrashMode.LoseUnflushed); public Task StopAsync(); public Task RestartAsync();
    public Task PauseAsync(); public Task ResumeAsync();
    public Task DestroyDiskAsync();                                   // permanent loss
    public Task RemoveRecoveryKeyAsync();                             // RST-005
    public Task RegisterAsync(string volume, DfsBackupSchedule? schedule = null, DfsBackupBudgets? budgets = null); // registers this target on a live volume
}
public sealed class TestDisk
{
    public string Name { get; }
    public SimulatedMedium? Simulated { get; }                       // null for StorageKind.Disk
    public SimulatedMediumFaults Faults { get; }                     // for Disk storage implemented by a faulting wrapper
    public string? RootDirectory { get; }                            // for StorageKind.Disk
    public void Fill(long leaveFreeBytes = 0);                       // ENOSPC
    public void SetReadOnly(bool readOnly);
    public Task CorruptObjectAsync(ObjectId id, long offset = 0, byte xorMask = 0xFF); // flips stored payload bytes on this node
    public Task CorruptObjectHeaderAsync(ObjectId id);                 // identity/length metadata corruption
    public Task DeleteObjectAsync(ObjectId id);                        // silent loss (anti-entropy tests)
    public IReadOnlyList<string> Files();
    public byte[] ReadRaw(string path);                                // raw durable bytes (encryption-at-rest inspection)
    public long UsedBytes { get; }
}
public sealed class TestClock : TimeProvider
{
    public void Advance(TimeSpan delta);            // jump forward (wall + monotonic)
    public void JumpWall(TimeSpan delta);           // wall-clock only (may be negative); monotonic unaffected
    public void Freeze(); public void Unfreeze();   // stop/resume advancing (timers stop firing)
    public void SetRate(double rate);               // drift
    public bool IsFrozen { get; }
}
```

### D.4 Network control

```csharp
public sealed class TestNetwork
{
    // Names: "n0".."n9", backup "b0".., client "client:c0". Both directions unless stated.
    public void Partition(params string[][] groups);                 // nodes not in the same group cannot exchange messages
    public void Isolate(string name);                                // cut every link of name
    public void Cut(string from, string to);                         // one direction only
    public void CutBoth(string a, string b);
    public void BlackHole(string from, string to);                   // connection stays open, messages vanish (half-open)
    public void Restore(string from, string to);
    public void Heal();                                              // remove all cuts/black holes/mesh restrictions
    public void SetLatency(string from, string to, TimeSpan latency);
    public void SetBandwidth(string from, string to, long bytesPerSecond);
    public void ResetConnections(string name);                       // close all connections of name (they reconnect)
    public MessageRule DropNext(MessageFilter filter, int count = 1);
    public MessageRule DropReplies(MessageFilter filter, int count = 1);   // request delivered & executed; reply lost
    public MessageRule DuplicateNext(MessageFilter filter, int count = 1);
    public MessageRule DelayNext(MessageFilter filter, TimeSpan delay, int count = 1);
    public MessageHold Hold(MessageFilter filter);                   // capture until released
    public MessageLog Log { get; }
}
public sealed record MessageFilter(string? From = null, string? To = null, MessageType? Type = null, DfsOperationKind? Operation = null,
    LinkDirection? Direction = null, Func<IWireMessage, bool>? Predicate = null);
public sealed class MessageRule { public int Remaining { get; } public int Matched { get; } public void Cancel(); }
public sealed class MessageHold : IDisposable
{
    public int Count { get; }
    public Task WaitForAsync(int count, TimeSpan timeout);
    public void ReleaseAll(bool reverseOrder = false);
    public void DropAll();
    public void Dispose();   // releases remaining
}
public sealed class MessageLog
{
    public long CountMessages(MessageFilter filter);
    public long CountBytes(MessageFilter filter);
    public void Reset();
}
```

### D.5 Faults

```csharp
public abstract record FaultAction
{
    public static FaultAction CrashNode(CrashMode mode = CrashMode.LoseUnflushed);
    public static FaultAction PauseNode();                      // pauses the node where the fault hit
    public static FaultAction Block();                          // the hitting operation waits until FaultArm.Release()
    public static FaultAction Throw(Func<Exception> factory);
    public static FaultAction IoError();                        // IOException
    public static FaultAction DiskFull();
    public static FaultAction Delay(TimeSpan delay);
    public static FaultAction DropReply();                      // server executes, reply is discarded
    public static FaultAction CancelCaller();                   // cancel the client's token at this point (client sites)
    public static FaultAction Custom(Func<FaultContext, Task> action);
}
public sealed record FaultMatch(string Site, string? Node = null, string? Volume = null, DfsOperationKind? Operation = null,
    string? PathPrefix = null, int Occurrence = 1, int Count = 1);   // Site may end with '*' (prefix match)
public sealed class FaultArm
{
    public FaultMatch Match { get; } public FaultAction Action { get; }
    public int Hits { get; } public bool Triggered { get; }
    public IReadOnlyList<FaultContext> Contexts { get; }
    public Task WaitTriggeredAsync(TimeSpan timeout);           // throws if never reached (HAR-013)
    public void Release();                                      // releases Block/PauseNode
    public void Disarm();
}
public sealed class TestFaults
{
    public FaultArm Arm(FaultMatch match, FaultAction action);
    public void DisarmAll();
    public FaultCoverageRegistry Coverage { get; }
    public void AssertReached(FaultArm arm);                    // Xunit assertion failure if not triggered
}
public sealed class FaultCoverageRegistry
{
    public IReadOnlyList<FaultContext> Hits { get; }
    public bool WasReached(string site, string? node = null);
    public IReadOnlyDictionary<string, int> CountsBySite();
    public CampaignReport CreateReport(string campaign, int generated, int applicable, int excluded);
}
public sealed record CampaignReport(string Campaign, int Generated, int Applicable, int Reached, int Completed, int Passed, int Failed, int Excluded, IReadOnlyList<string> Notes);
```

### D.6 Cluster inspection

```csharp
public sealed class ClusterInspector
{
    public Task<GroupInspection> GetControlGroupAsync();
    public Task<GroupInspection> GetVolumeGroupAsync(string volume);
    public Task<VolumeStateInspection> GetVolumeStateAsync(string volume, string? node = null);   // default: leader
    public Task<FileInspection?> GetFileAsync(string volume, string path);
    public Task<IReadOnlyList<string>> GetObjectHoldersAsync(ObjectId id);        // nodes durably holding a verified copy
    public Task<(int Copies, int Zones)> CountDurableDomainsAsync(ObjectId id);
    public Task<DfsReplicaHealth> GetReplicaHealthAsync(string volume);
    public Task<IReadOnlySet<ObjectId>> GetAllStoredObjectsAsync(string? volume = null);
    public Task<NodeCounters> GetTotalCountersAsync();
    public Task AssertNoSplitBrainAsync();                                        // at most one leader per term per group
    public Task AssertReplicasConvergedAsync(string volume);                      // identical digests on all caught-up replicas
}
```

### D.7 Histories, models, checkers

```csharp
public enum OpOutcome { Ok, Failed, Unknown }
public sealed record HistoryOp(long Id, string Process, string Kind, string Key, object? Input, long InvokeTime, long? CompleteTime,
    OpOutcome Outcome, object? Output, DfsOperationId? OperationId);
public sealed class HistoryRecorder
{
    public long Invoke(string process, string kind, string key, object? input, DfsOperationId? opId = null);
    public void Complete(long id, OpOutcome outcome, object? output = null);
    public void Resolve(long id, OpOutcome outcome, object? output = null);   // narrowing an Unknown later
    public IReadOnlyList<HistoryOp> Operations { get; }
    public Task<T> RecordAsync<T>(string process, string kind, string key, object? input, Func<Task<T>> action);  // Unknown on DfsCommitOutcomeUnknownException/timeout
}
public sealed record CheckResult(bool IsValid, bool IsInconclusive, string Explanation, long StatesExplored)
{ public void AssertValid(); }   // fails the test unless IsValid && !IsInconclusive (HAR-017)

public interface ISequentialModel<TState>   // no `notnull` constraint: RegisterModel's state is byte[]? (nullable warnings are errors)
{
    TState Initial { get; }
    bool Step(TState state, HistoryOp op, out TState next);   // false if op's output is impossible in state
    string Key(TState state);                                 // canonical memo key: equal keys <=> behaviourally equal states
}
public static class LinearizabilityChecker
{
    // Wing & Gong / Lowe search with memoisation; per-key partitioning when keys are independent.
    public static CheckResult Check<TState>(IReadOnlyList<HistoryOp> history, ISequentialModel<TState> model, long maxStates = 5_000_000);
}
public sealed class RegisterModel : ISequentialModel<byte[]?> { }            // kinds "write"(input byte[]), "read"(output byte[]?), "delete"
public sealed class AppendLogModel : ISequentialModel<ImmutableList<byte[]>> { } // kinds "append", "read"

public sealed record TxRecord(string Id, long InvokeTime, long? CompleteTime, OpOutcome Outcome,
    IReadOnlyDictionary<string, string?> Reads, IReadOnlyDictionary<string, string?> Writes,
    IReadOnlyList<(string Directory, IReadOnlySet<string> Names)> PredicateReads);
public static class SerializabilityChecker
{
    public static CheckResult CheckStrict(IReadOnlyList<TxRecord> transactions, IReadOnlyDictionary<string, string?> initial, long maxStates = 2_000_000);
}

public sealed class ReferenceFileModel      // independent byte-array model of one file (PROP-001)
{
    public byte[] Committed { get; } public byte[] Staged { get; }
    public void Write(long offset, ReadOnlySpan<byte> data); public void SetLength(long length);
    public void Flush(); public void Abort();
    public byte[] Read(long offset, int count, bool staged);
}
public sealed class ReferenceNamespaceModel // independent namespace tree model (PROP-002)
{
    public bool CreateFile(string path, out string? error); public bool CreateDirectory(string path, out string? error);
    public bool Move(string from, string to, bool overwrite, out string? error); public bool Delete(string path, bool recursive, out string? error);
    public bool Exists(string path); public IReadOnlyList<string> List(string directory); public long InodeOf(string path);
}

public sealed record ModelCheckResult(bool Completed, long StatesExplored, int MaxDepth, string? Violation, IReadOnlyList<string>? CounterexampleTrace, IReadOnlyList<string> Bounds)
{ public void AssertNoViolation(); public void AssertCompleted(); }
public static class ModelChecker
{
    // Explicit-state BFS over a deterministic transition system with canonical state keys.
    public static ModelCheckResult Explore<TState>(TState initial, Func<TState, IEnumerable<(string Action, TState Next)>> next,
        Func<TState, string?> invariant, Func<TState, string> key, int maxDepth, long maxStates);
}

public sealed class RaftSimulation    // deterministic harness over pure RaftNode cores (CON/MOD/MUT)
{
    public RaftSimulation(int voters, ulong seed, int learners = 0, SafetyMutations? mutations = null);
    public IReadOnlyList<RaftNode> Nodes { get; }
    public void Partition(params int[][] groups); public void Heal();
    public void Crash(int node); public void Restart(int node);        // restart from persisted hard state + persisted log only
    public void DropProbability(double p); public void DuplicateProbability(double p); public void ReorderMessages(bool enable);
    public bool Propose(int node, byte[] data);
    public void RunTicks(int ticks);
    public void RunUntil(Func<bool> condition, int maxTicks);
    public int? LeaderIndex { get; }
    public IReadOnlyList<byte[]> AppliedLog(int node);
    public string? CheckSafety();          // election safety, log matching, leader completeness, state-machine safety; null when OK
    public IReadOnlyList<string> Trace { get; }
}
```

### D.8 Catalog metadata

```csharp
[AttributeUsage(AttributeTargets.Method)]
public sealed class CatalogAttribute : Attribute, ITraitAttribute   // xunit v3 trait source
{
    public CatalogAttribute(string testId, string level, params string[] invariants);  // emits traits TestId, Suite, Level, Invariant
}
public static class TestCatalog
{
    public static IReadOnlyList<CatalogEntry> All { get; }   // the 586 named cases parsed from the test plan (id, level, name, suite)
}
public sealed record CatalogEntry(string Id, string Suite, string Level, string Name, string Assertion);
public static class TestData
{
    public static byte[] Bytes(ulong seed, int length);                       // deterministic incompressible
    public static byte[] Compressible(int length, byte value = 0x41);
    public static string Utf8Text(ulong seed, int chars, bool multibyte = true);
    public static IReadOnlyList<long> BoundaryLengths(int inlineThreshold, int chunkSize, int bufferSize);   // 0,1,t-1,t,t+1,...
}
public static class AssertEx
{
    public static Task<T> ThrowsDfsAsync<T>(Func<Task> action, DfsErrorCode? code = null) where T : Exception;
    public static void BytesEqual(ReadOnlySpan<byte> expected, ReadOnlySpan<byte> actual, string? because = null);  // reports first mismatch offset
    public static Task EventuallyAsync(Func<Task<bool>> condition, TimeSpan timeout, string because);
}
```

---

## E. Protocol message catalogue — `Tedd.Tdfs.Protocol.MessageType` (Core)

Frame = `[u16 MessageType][body]`. Replies use the `*Result` type or `ErrorReply`. `MessageTypes.OperationOf(type)`
maps each type to a `DfsOperationKind` (used by `MessageFilter.Operation`). Bodies are defined by the implementation
in `Core/Protocol/Messages/*.cs` (one record per type implementing `IWireMessage`, with a static `Read(ref WireReader)`).

```csharp
public enum MessageType : ushort
{
    ErrorReply = 1,
    // handshake / discovery
    Hello = 10, HelloChallenge = 11, HelloProof = 12, HelloResult = 13,
    GetClusterMap = 14, ClusterMapResult = 15, GetVolumeRoute = 16, VolumeRouteResult = 17,
    JoinCluster = 18, JoinClusterResult = 19, RegisterIncarnation = 20, RegisterIncarnationResult = 21,
    // sessions / outcomes
    OpenSession = 30, OpenSessionResult = 31, KeepAlive = 32, KeepAliveResult = 33,
    QueryOperation = 34, QueryOperationResult = 35, ReserveOperationIds = 36, ReserveOperationIdsResult = 37,
    // namespace & files (client -> volume leader)
    OpenHandle = 40, OpenHandleResult = 41, CloseHandle = 42, CloseHandleResult = 43,
    ReadRange = 44, ReadRangeResult = 45, GetInfo = 46, GetInfoResult = 47, ListDirectory = 48, ListDirectoryResult = 49,
    CreateStaging = 50, CreateStagingResult = 51, CommitFile = 52, CommitFileResult = 53,
    WriteAll = 54, WriteAllResult = 55, Append = 56, AppendResult = 57, Copy = 58, CopyResult = 59,
    Move = 60, MoveResult = 61, Delete = 62, DeleteResult = 63, CreateDirectory = 64, CreateDirectoryResult = 65,
    SetPolicy = 66, SetPolicyResult = 67, SetAccess = 68, SetAccessResult = 69,
    // transactions
    TxBegin = 80, TxBeginResult = 81, TxOp = 82, TxOpResult = 83, TxCommit = 84, TxCommitResult = 85, TxAbort = 86, TxAbortResult = 87,
    // snapshots
    CreateSnapshot = 90, CreateSnapshotResult = 91, ListSnapshots = 92, ListSnapshotsResult = 93,
    DeleteSnapshot = 94, DeleteSnapshotResult = 95, OpenSnapshot = 96, OpenSnapshotResult = 97,
    CloseSnapshot = 98, CloseSnapshotResult = 99, SetRetention = 100, SetRetentionResult = 101,
    RestoreSnapshot = 102, RestoreSnapshotResult = 103,
    // data plane (client/node -> storage node)
    PutObject = 110, PutObjectResult = 111, GetObject = 112, GetObjectResult = 113, LocateObject = 114, LocateObjectResult = 115,
    ReplicateObject = 116, ReplicateObjectResult = 117, DeleteObjects = 118, DeleteObjectsResult = 119,
    InventoryReport = 120, InventoryReportResult = 121, CopyObjects = 122, CopyObjectsResult = 123,
    // consensus (node <-> node)
    RaftBatch = 130, RaftBatchResult = 131, RaftSnapshotChunk = 132, RaftSnapshotChunkResult = 133,
    // administration
    AdminRequest = 140, AdminResult = 141,
    // backup
    BackupRegister = 150, BackupRegisterResult = 151, BackupStatus = 152, BackupStatusResult = 153,
    BackupRequestSync = 154, BackupRequestSyncResult = 155, BackupAwait = 156, BackupAwaitResult = 157,
    BackupBegin = 160, BackupBeginResult = 161, BackupInventory = 162, BackupInventoryResult = 163,
    BackupPutObject = 164, BackupPutObjectResult = 165, BackupPutManifest = 166, BackupPutManifestResult = 167,
    BackupPublish = 168, BackupPublishResult = 169, BackupRetention = 170, BackupRetentionResult = 171,
    BackupExportRestore = 172, BackupExportRestoreResult = 173
}
public static class MessageTypes
{
    public static DfsOperationKind OperationOf(MessageType type);
    public static bool IsReply(MessageType type);
    public static MessageType ReplyOf(MessageType request);
}
```

## F. Additional harness members

```csharp
// TestCluster
public void KillClient(DfsClient client);    // simulate client process death: isolate it and abandon it without cleanup
public Task<RawProtocolClient> ConnectRawAsync(string nodeName, CancellationToken ct = default);

public sealed class RawProtocolClient : IAsyncDisposable    // bypasses all SDK validation (PATH-012, SEC-004/005/014/015)
{
    public bool IsConnected { get; }
    public Task<DfsErrorCode> AuthenticateAsync(DfsCredentials credentials);
    public Task<DfsErrorCode> SendPathOperationAsync(string volume, DfsOperationKind kind, string rawPath, string? rawPath2 = null);
        // kinds: CreateDirectory, Delete, Open (CreateNew, write), Move, GetInfo, WriteAll; path sent verbatim (wire UTF-8 of the string)
    public Task<DfsErrorCode> SendRawPathBytesAsync(string volume, DfsOperationKind kind, byte[] rawUtf8PathBytes); // malformed UTF-8
    public Task<DfsErrorCode> SendFrameAsync(ReadOnlyMemory<byte> frame);          // arbitrary bytes; ProtocolError if the server closes
    public Task<DfsErrorCode> GetObjectDirectAsync(string volume, ObjectId id);    // no capability
    public Task<DfsErrorCode> PutObjectDirectAsync(string volume, ObjectId id, ReadOnlyMemory<byte> data); // no capability
    public Task<IWireMessage> CallAsync(IWireMessage request);                     // typed round-trip (ErrorReply returned as message)
    public ValueTask DisposeAsync();
}

// MessageHold additionally exposes captured frames for replay tests
public IReadOnlyList<CapturedMessage> Captured { get; }
public sealed record CapturedMessage(string From, string To, LinkDirection Direction, MessageType Type, ReadOnlyMemory<byte> Frame);
// TestNetwork
public Task<DfsErrorCode> ReplayAsync(CapturedMessage message);   // re-deliver a previously captured frame from its original sender identity

// Added while implementing the harness (pure logic, usable by tests)
// TestClock
public TestClock(TimeProvider? baseProvider = null);   // starts at the base wall time; TimestampFrequency = TimeSpan.TicksPerSecond
public double Rate { get; }
// TestClusterOptions
public const int MaxNodes = 10;
public int EffectiveNodeCount { get; } public int EffectiveControlVoters { get; }
public IReadOnlyList<TestNodeSpec> GetNodeSpecs();          // Nodes, or n0..n{N-1} laid out per Topology (TwoPlusOne: first ceil(2n/3) in z1)
public DfsStoragePolicy GetEffectiveDefaultPolicy();        // DefaultPolicy, or min(3,hosts)/min(3,zones)/min(2,hosts)/min(2,zones) (clamped valid)
public ulong ResolveSeed(string testName); public string ResolveDataRoot(); public void Validate();
// TestNodeSpec: string EffectiveHost; DfsTopologyLabels ToLabels();   TestVolumeSpec: DfsVolumeSpec ToVolumeSpec(DfsStoragePolicy clusterDefault);
// TestClientOptions: string NetworkName ("client:{Name}")
// HistoryOp / TxRecord
public long? ResolveTime { get; init; }      // set by HistoryRecorder.Resolve; an Ok op's effect lies in [InvokeTime, ResolveTime ?? CompleteTime]
public long? EffectDeadline { get; }         // Ok: ResolveTime ?? CompleteTime; otherwise null (unbounded)
public bool WasResolved { get; }             // HistoryOp only
// HistoryRecorder
public bool TrustCancellationAsAbort { get; set; }   // default false: OperationCanceledException after async start -> Unknown
public int Count { get; } public HistoryOp Get(long id); public void AttachOperationId(long id, DfsOperationId operationId);
public static OpOutcome Classify(Exception exception, bool requestMayHaveBeenSent = true, bool trustCancellation = false);
// CheckResult: static Valid(...), Invalid(...), Inconclusive(...)
// ReferenceFileModel: ReferenceFileModel(ReadOnlySpan<byte> initial); long CommittedLength, StagedLength; bool HasPendingChanges
// ReferenceNamespaceModel (errors are DfsErrorCode names; root inode 1; new inodes monotonic from 2)
public bool CreateDirectory(string path, bool recursive, out string? error);
public bool CopyFile(string from, string to, bool overwrite, out string? error);   // overwrite keeps the destination inode
public bool IsFile(string path); public bool IsDirectory(string path); public bool TryGetInode(string path, out long inode);
public IReadOnlyList<string> AllPaths(); public int Count { get; } public long LastAllocatedInode { get; }
// FaultMatch: void Validate(); bool MatchesSite(string site); bool Matches(in FaultContext context); bool TriggersOn(int matchingHit)
// FaultAction cases (nested sealed records): CrashNodeFault(Mode), PauseNodeFault, BlockFault, ThrowFault(Factory), IoErrorFault,
//   DiskFullFault, DelayFault(Duration), DropReplyFault, CancelCallerFault, CustomFault(Action)
// MessageFilter: static Any; bool Matches(string from, string to, LinkDirection direction, ReadOnlySpan<byte> frame);
//   bool Matches(string from, string to, LinkDirection direction, MessageType type, IWireMessage? message)
// CampaignReport: int Unreached; bool IsComplete
// CatalogAttribute: string TestId, Suite, Level; IReadOnlyList<string> Invariants
// TestCatalog: const int ExpectedCount = 586; IReadOnlyList<string> Suites; CatalogEntry? Find(string id); IReadOnlyList<CatalogEntry> Parse(string markdown)
// CatalogEntry: string MethodName ("OPEN_001_CreateNewAbsent")
```

## G. Test authoring conventions

* Location by level: `U` → `tests/Tedd.Tdfs.Tests.Unit`, `S` → `.Simulation`, `I`/`H` → `.Integration`, `M` → `.Models`,
  `P` → `.Performance`. File `tests/Tedd.Tdfs.Tests.<Level>/<Suite>/<Suite><Level>Tests.cs` (e.g.
  `Open/OpenUnitTests.cs`), namespace `Tedd.Tdfs.Tests.<Level>.<Suite>`, class `<Suite><Level>Tests`
  (e.g. `OpenUnitTests`, `OpenSimulationTests`). Shared helpers for a suite go in `<Suite>/<Suite>Helpers.cs` of the
  same project (internal static).
* One test method per catalog row (more allowed for variants), name `{SUITE}_{NNN}_{CatalogName}` (e.g.
  `OPEN_001_CreateNewAbsent`), attributed `[Fact]` or `[Theory]` + `[Catalog("OPEN-001", "U", "INV-02", "INV-07")]`.
* Every cluster scenario runs through `ClusterTest.RunAsync(options, async cluster => { ... })`. Use
  `TestContext.Current.CancellationToken` where a token is needed.
* `S`-level scenarios set `Mode = ClusterMode.Deterministic` unless they need real time; `I`-level use
  `Transport = TransportKind.Quic` and `Storage = StorageKind.Disk` where real boundaries matter.
* `H`-level tests that need hardware power cuts call `Assert.Skip("requires <rig>; status: not run")` with the reason
  (test plan §2.1: not-run is never a pass).
* Assertions must check the specific required property (bytes, identity, outcome, counts) — never only "no exception".
  Fault-injection tests must assert the fault point was reached (`TestFaults.AssertReached(arm)`).

---

## H. Addendum A — permissions, pools, streams ([spec](spec/fsd-addendum-acl-pools-streams.md))

### H.1 Core (namespace `Tedd.Tdfs`)

```csharp
[Flags] public enum DfsFileSystemRights   // NTFS values
{
    ReadData = 0x1, ListDirectory = 0x1, WriteData = 0x2, CreateFiles = 0x2, AppendData = 0x4, CreateDirectories = 0x4,
    ReadExtendedAttributes = 0x8, WriteExtendedAttributes = 0x10, ExecuteFile = 0x20, Traverse = 0x20,
    DeleteSubdirectoriesAndFiles = 0x40, ReadAttributes = 0x80, WriteAttributes = 0x100, Delete = 0x10000,
    ReadPermissions = 0x20000, ChangePermissions = 0x40000, TakeOwnership = 0x80000, Synchronize = 0x100000,
    Read = ReadData | ReadExtendedAttributes | ReadAttributes | ReadPermissions | Synchronize,           // 0x120089
    Write = WriteData | AppendData | WriteExtendedAttributes | WriteAttributes | Synchronize,            // 0x100116
    ReadAndExecute = Read | ExecuteFile,                                                                // 0x1200A9
    Modify = ReadAndExecute | Write | Delete,                                                           // 0x1301BF
    FullControl = 0x1F01FF
}
public enum DfsAccessControlType { Allow = 0, Deny = 1 }
[Flags] public enum DfsInheritanceFlags { None = 0, ContainerInherit = 1, ObjectInherit = 2 }
[Flags] public enum DfsPropagationFlags { None = 0, NoPropagateInherit = 1, InheritOnly = 2 }
public enum DfsIdentityKind { User, Group, WellKnown }
public readonly record struct DfsIdentity(DfsIdentityKind Kind, string Name)
{
    public static DfsIdentity Everyone { get; } public static DfsIdentity AuthenticatedUsers { get; }
    public static DfsIdentity Administrators { get; } public static DfsIdentity CreatorOwner { get; }
    public static DfsIdentity User(string name); public static DfsIdentity Group(string name);
    public static DfsIdentity Parse(string s);  public static bool TryParse(string? s, out DfsIdentity id);
    public override string ToString();          // "user:alice", "group:eng", "Everyone", "AuthenticatedUsers", "Administrators", "CreatorOwner"
}
public sealed record DfsAccessRule(DfsIdentity Identity, DfsFileSystemRights Rights, DfsAccessControlType Type = DfsAccessControlType.Allow,
    DfsInheritanceFlags Inheritance = DfsInheritanceFlags.None, DfsPropagationFlags Propagation = DfsPropagationFlags.None)
{
    public bool IsInherited { get; init; }          // set only in returned descriptors
    public string? InheritedFrom { get; init; }     // path of the ancestor the ACE was inherited from
}
public sealed record DfsSecurityDescriptor(DfsIdentity Owner, IReadOnlyList<DfsAccessRule> Rules, bool AreAccessRulesProtected = false)
{
    public static DfsSecurityDescriptor DefaultRoot(DfsIdentity owner);   // Everyone FullControl CI|OI
    public void Validate();                                               // ArgumentException for invalid flags/identities
}
public static class DfsAccessEvaluator   // the single production implementation of ACL-03/ACL-04 (pure, deterministic)
{
    // chain: descriptor of the target first, then each ancestor's descriptor up to the root (nearest first).
    public static IReadOnlyList<DfsAccessRule> EffectiveRules(IReadOnlyList<DfsSecurityDescriptor> chainNearestFirst, bool targetIsDirectory, DfsIdentity targetOwner);
    public static DfsFileSystemRights GrantedRights(IReadOnlyList<DfsAccessRule> effectiveRules, DfsIdentity owner, IReadOnlySet<DfsIdentity> callerIdentities, bool callerIsAdministrator);
    public static bool IsGranted(IReadOnlyList<DfsAccessRule> effectiveRules, DfsIdentity owner, IReadOnlySet<DfsIdentity> callerIdentities, bool callerIsAdministrator, DfsFileSystemRights requested);
}
public static class DfsStreamPath
{
    public const string DataType = "$DATA";
    public const int MaxStreamsPerInode = 1024;
    // Normalized paths may end in ":stream". Splits "/a/f:s" -> ("/a/f", "s"); "/a/f" -> ("/a/f", null).
    public static (string FilePath, string? StreamName) Split(string normalizedPath);
    public static string Combine(string normalizedFilePath, string? streamName);
    public static bool HasStream(string normalizedPath);
}
// DfsPath.Normalize additionally implements ADS-01 (":$DATA" suffix normalisation, stream rules).
// DfsErrorCode additions: PoolNotFound = 48, PoolInUse = 49, GroupNotFound = 50, PrincipalNotFound = 51.
public sealed record DfsStreamInfo(string Name, long Length, ulong Revision);
```

### H.2 Client

```csharp
// DfsFileClient and DfsDirectoryClient (+ sync counterparts)
public Task<DfsSecurityDescriptor> GetAccessControlAsync(string path, CancellationToken ct = default);           // includes inherited rules (IsInherited = true)
public Task<CommitToken> SetAccessControlAsync(string path, DfsSecurityDescriptor descriptor, CancellationToken ct = default); // explicit rules only; inherited entries in the input are ignored
public Task<CommitToken> SetOwnerAsync(string path, DfsIdentity owner, CancellationToken ct = default);
public Task<DfsFileSystemRights> GetEffectiveRightsAsync(string path, DfsIdentity? identity = null, CancellationToken ct = default); // null = caller
// DfsFileClient only
public Task<IReadOnlyList<DfsStreamInfo>> ListStreamsAsync(string path, CancellationToken ct = default);
// DfsFileInfo gains: bool IsStream, string? StreamName, int StreamCount (positional record parameters appended at the end with defaults)
// DfsVolumeSpec gains: string StoragePool = "default"; DfsSecurityDescriptor? RootSecurity; bool EnforceTraverseChecking = false
// DfsStoragePolicy gains: string? Pool = null (null = the volume's pool)
// DfsVolumeInfo gains (appended, defaults): string StoragePool, bool EnforceTraverseChecking
// DfsInodeAccess / SetAccessAsync remain as shortcuts: OwnerOnly -> protected {Owner FullControl, Administrators FullControl};
//   ReadOnlyForOthers -> protected {Owner FullControl, Administrators FullControl, Everyone Read}; both false -> unprotected, no explicit rules.

public sealed record DfsGroupInfo(string Name, IReadOnlyList<string> Members);
public sealed record DfsPoolInfo(string Name, IReadOnlyList<string> Nodes, IReadOnlyList<string> Volumes, long CapacityBytes, long UsedBytes);
public sealed record DfsPoolStatus(DfsPoolInfo Info, DfsReplicaHealth Replicas, IReadOnlyList<string> DrainingNodes);
// DfsAdminClient additions
public Task CreateGroupAsync(string group, CancellationToken ct = default);
public Task DeleteGroupAsync(string group, CancellationToken ct = default);
public Task AddGroupMemberAsync(string group, string principal, CancellationToken ct = default);
public Task RemoveGroupMemberAsync(string group, string principal, CancellationToken ct = default);
public Task<IReadOnlyList<DfsGroupInfo>> ListGroupsAsync(CancellationToken ct = default);
public Task CreatePoolAsync(string pool, IReadOnlyList<string> nodeNames, CancellationToken ct = default);
public Task SetPoolMembersAsync(string pool, IReadOnlyList<string> nodeNames, CancellationToken ct = default);
public Task DeletePoolAsync(string pool, CancellationToken ct = default);
public Task<IReadOnlyList<DfsPoolInfo>> ListPoolsAsync(CancellationToken ct = default);
public Task<DfsPoolStatus> GetPoolStatusAsync(string pool, CancellationToken ct = default);
```

### H.3 Harness

```csharp
// TestNodeSpec gains: IReadOnlyList<string>? Pools = null (pools created at bootstrap; "default" always implied)
// TestVolumeSpec gains: string StoragePool = "default"; DfsSecurityDescriptor? RootSecurity; bool EnforceTraverseChecking = false
// TestCluster gains:
public Task CreateGroupAsync(string group, params string[] members);
public Task<TestPrincipal> CreateUserAsync(string name, DfsPermissions volumeGrants = DfsPermissions.Read | DfsPermissions.Write, params string[] groups); // grants on every test volume
// ClusterInspector gains:
public Task<IReadOnlyList<string>> GetPoolNodesAsync(string pool);
public Task<IReadOnlySet<ObjectId>> GetStoredObjectsOnNodeAsync(string node, string? volume = null);
// TestCatalog gains: public static IReadOnlyList<CatalogEntry> Addendum { get; }  // 78 rows parsed from the addendum (embedded resource)
// FileInspection gains (appended): IReadOnlyList<StreamInspection> Streams; record StreamInspection(string Name, long Length, ulong Revision, IReadOnlyList<ExtentInspection> Extents)
```
