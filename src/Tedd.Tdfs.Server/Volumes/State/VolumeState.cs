using System.Collections.Immutable;
using Tedd.Tdfs.Server.Control;

namespace Tedd.Tdfs.Server.Volumes.State;

/// <summary>
/// Immutable configuration of a volume taken from its creation record: identity, epoch and the creation-time policies and
/// limits. Part of the replicated state so apply never consults node-local configuration.
/// </summary>
/// <param name="Id">Volume id.</param>
/// <param name="Name">Volume name.</param>
/// <param name="Epoch">Volume epoch.</param>
/// <param name="ClusterId">Cluster id (commit tokens).</param>
/// <param name="Creator">Creating principal.</param>
/// <param name="CreatedUtc">Creation time.</param>
/// <param name="InlineThreshold">Largest stream length stored inline.</param>
/// <param name="Limits">Volume limits.</param>
/// <param name="StoragePool">Storage pool of the volume.</param>
/// <param name="EnforceTraverseChecking">Require Traverse on every ancestor directory (ACL-05).</param>
/// <param name="DefaultPolicy">Default storage policy at creation.</param>
/// <param name="NamedPolicies">Named storage policies at creation (ordinal).</param>
internal sealed record VolumeConfig(VolumeId Id, string Name, ulong Epoch, Guid ClusterId, string Creator, DateTimeOffset CreatedUtc, int InlineThreshold,
    DfsVolumeLimits Limits, string StoragePool, bool EnforceTraverseChecking, DfsStoragePolicy DefaultPolicy,
    ImmutableSortedDictionary<string, DfsStoragePolicy> NamedPolicies);

/// <summary>
/// The applied state of a volume (ARCHITECTURE §5.1): an immutable persistent structure; every applied entry produces a new
/// root, so roots can be retained cheaply for snapshots, pinned enumerations and version-pinned handles. All collections are
/// ordinal/ordered so iteration, checkpoints and replies are deterministic.
/// </summary>
internal sealed record VolumeState
{
    /// <summary>Immutable configuration.</summary>
    public required VolumeConfig Config { get; init; }

    /// <summary>Index of the last applied entry.</summary>
    public ulong AppliedIndex { get; init; }

    /// <summary>Leader wall clock carried by the last applied entry (deterministic "now" of apply).</summary>
    public DateTimeOffset LeaderTime { get; init; }

    /// <summary>The current namespace: every inode, including detached ones not yet reclaimed.</summary>
    public required ImmutableSortedDictionary<ulong, InodeRecord> Inodes { get; init; }

    /// <summary>Inodes removed from the namespace but kept for their open handles (NS-06, OPEN-017).</summary>
    public ImmutableSortedSet<ulong> Detached { get; init; } = ImmutableSortedSet<ulong>.Empty;

    /// <summary>Current writer fencing generation of every stream that ever had a writer (LOCK-04, ADS-05).</summary>
    public ImmutableSortedDictionary<StreamKey, ulong> WriterGenerations { get; init; } = ImmutableSortedDictionary.Create<StreamKey, ulong>(StreamKeyComparer.Instance);

    /// <summary>Next inode id (allocated monotonically, never reused).</summary>
    public ulong NextInodeId { get; init; } = InodeRecord.RootId + 1;

    /// <summary>Next handle id.</summary>
    public ulong NextHandleId { get; init; } = 1;

    /// <summary>Next staging id.</summary>
    public ulong NextStagingId { get; init; } = 1;

    /// <summary>Next session id.</summary>
    public ulong NextSessionId { get; init; } = 1;

    /// <summary>Next pin id.</summary>
    public ulong NextPinId { get; init; } = 1;

    /// <summary>Open handles.</summary>
    public ImmutableSortedDictionary<ulong, HandleRecord> Handles { get; init; } = ImmutableSortedDictionary<ulong, HandleRecord>.Empty;

    /// <summary>Client sessions (active, expired and retired tombstones above <see cref="RetiredSessionsBelow"/>).</summary>
    public ImmutableSortedDictionary<ulong, SessionRecord> Sessions { get; init; } = ImmutableSortedDictionary<ulong, SessionRecord>.Empty;

    /// <summary>Every session below this id is fully retired (its operation ids are expired).</summary>
    public ulong RetiredSessionsBelow { get; init; } = 1;

    /// <summary>Transactions.</summary>
    public ImmutableSortedDictionary<DfsOperationId, TxRecord> Transactions { get; init; } =
        ImmutableSortedDictionary.Create<DfsOperationId, TxRecord>(OperationIdComparer.Instance);

    /// <summary>Staging sessions (open, and closed ones until GC prunes them).</summary>
    public ImmutableSortedDictionary<ulong, StagingRecord> Staging { get; init; } = ImmutableSortedDictionary<ulong, StagingRecord>.Empty;

    /// <summary>Retained snapshots by name.</summary>
    public ImmutableSortedDictionary<string, SnapshotRecord> Snapshots { get; init; } = ImmutableSortedDictionary.Create<string, SnapshotRecord>(StringComparer.Ordinal);

    /// <summary>GC pins.</summary>
    public ImmutableSortedDictionary<ulong, PinRecord> Pins { get; init; } = ImmutableSortedDictionary<ulong, PinRecord>.Empty;

    /// <summary>Objects dereferenced from the current root, in index order (ARCHITECTURE §8).</summary>
    public ImmutableList<DerefEntry> DerefLog { get; init; } = ImmutableList<DerefEntry>.Empty;

    /// <summary>Backup progress per registered target.</summary>
    public ImmutableSortedDictionary<string, BackupProgressRecord> BackupProgress { get; init; } =
        ImmutableSortedDictionary.Create<string, BackupProgressRecord>(StringComparer.Ordinal);

    /// <summary>Data-key versions recorded by <c>RotateDataKey</c> (ascending; at least the creation version 1).</summary>
    public ImmutableSortedSet<uint> KeyVersions { get; init; } = [1u];

    /// <summary>Default policy being migrated to (<c>MigratePolicy</c>), or null.</summary>
    public DfsStoragePolicy? MigrationTarget { get; init; }

    /// <summary>Restore in progress or completed (restored volumes only).</summary>
    public RestoreRecord? Restore { get; init; }

    /// <summary>Accepted restores of this volume's snapshots into new volumes whose outcome is not recorded yet.</summary>
    public ImmutableSortedDictionary<DfsOperationId, RestoreIntentRecord> Restores { get; init; } =
        ImmutableSortedDictionary.Create<DfsOperationId, RestoreIntentRecord>(OperationIdComparer.Instance);

    /// <summary>
    /// True for a transaction's private working view on the leader (never replicated or checkpointed): nothing in it is
    /// committed, so its replies carry <see cref="CommitToken.None"/> (PROTOCOL.md §8.4).
    /// </summary>
    public bool TransactionView { get; init; }

    /// <summary>The root directory.</summary>
    public InodeRecord Root => Inodes[InodeRecord.RootId];

    /// <summary>The commit token of <paramref name="index"/> in this volume (none inside a transaction's working view).</summary>
    public CommitToken TokenAt(ulong index) => TransactionView ? CommitToken.None : new(Config.ClusterId, Config.Id, Config.Epoch, index);

    /// <summary>The inode with <paramref name="id"/>, or null.</summary>
    public InodeRecord? Find(ulong id) => Inodes.GetValueOrDefault(id);

    /// <summary>
    /// First session number of the volume <paramref name="volume"/>: a deterministic function of the volume id in
    /// [2^24, 2^48] (a multiple of 2^24, plus one). Session numbers are part of every operation id, and an id is bound to
    /// the volume that issued it (PROTOCOL.md §5.1); numbering each volume's sessions from its own base keeps an id of one
    /// volume from naming a session of another (where it would execute as that session's operation — SEC-010), as the
    /// expected-volume binding of an application id is only known from its first transmission.
    /// </summary>
    public static ulong SessionBase(VolumeId volume)
    {
        Span<byte> bytes = stackalloc byte[16];
        volume.Value.TryWriteBytes(bytes);
        ulong folded = System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(bytes)
                       ^ System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(bytes[8..]);
        return (((folded & 0xFF_FFFF) | 1) << 24) + 1;
    }

    /// <summary>
    /// True when <paramref name="session"/> is a number this volume issued (it may since have been retired); false for a
    /// number it never issued. Volumes persisted before per-volume session bases number their sessions from 1.
    /// </summary>
    public bool IssuedSession(ulong session)
    {
        if (session == 0 || session >= NextSessionId)
            return false;
        ulong first = SessionBase(Config.Id);
        return session >= first || NextSessionId < first;
    }

    /// <summary>The writer generation of a stream (0 = never had a writer).</summary>
    public ulong WriterGenerationOf(StreamKey key) => WriterGenerations.GetValueOrDefault(key);

    /// <summary>
    /// The initial state at log index 0, derived only from the volume's creation data and the cluster id (identical on
    /// every replica, including members added later).
    /// </summary>
    public static VolumeState Initial(VolumeCreation creation, Guid clusterId)
    {
        ArgumentNullException.ThrowIfNull(creation);
        var named = ImmutableSortedDictionary.CreateRange(StringComparer.Ordinal, creation.NamedPolicies);
        var config = new VolumeConfig(creation.Id, creation.Name, creation.Epoch, clusterId, creation.Creator, creation.CreatedUtc,
            Math.Max(0, creation.InlineThreshold), creation.Limits, creation.StoragePool, creation.EnforceTraverseChecking, creation.DefaultPolicy, named);
        DfsIdentity owner = DfsIdentity.TryParse("user:" + creation.Creator, out DfsIdentity user) ? user : DfsIdentity.Administrators;
        DfsSecurityDescriptor rootSecurity = creation.RootSecurity is { } supplied
            ? new DfsSecurityDescriptor(supplied.Owner, supplied.ExplicitRules, supplied.AreAccessRulesProtected)
            : DfsSecurityDescriptor.DefaultRoot(owner);
        var root = new InodeRecord(InodeRecord.RootId, InodeKind.Directory, 0, 0, creation.CreatedUtc, creation.CreatedUtc, rootSecurity, string.Empty,
            0, string.Empty, InodeRecord.NoStreams, InodeRecord.NoChildren);
        ulong sessionBase = SessionBase(creation.Id);
        return new VolumeState
        {
            Config = config,
            LeaderTime = creation.CreatedUtc,
            NextSessionId = sessionBase,
            RetiredSessionsBelow = sessionBase,
            Inodes = ImmutableSortedDictionary<ulong, InodeRecord>.Empty.Add(root.Id, root),
        };
    }
}
