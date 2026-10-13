using System.Collections.Immutable;
using Tedd.Tdfs.Protocol;

namespace Tedd.Tdfs.Server.Volumes.Commands;

/// <summary>
/// Volume log command kinds (INTERNALS.md §1.2 ranges: 1-99 core/sessions, 100-199 namespace, 200-299 handles/publication,
/// 300-399 transactions, 400-499 snapshots/pins, 500-599 GC/repair/restore, 600-699 backup, 700-799 policy/security/keys).
/// Values are persisted in the log: never renumber.
/// </summary>
internal enum VolumeCommandKind : ushort
{
    OpenSession = 10,
    KeepAlive = 11,
    ExpireSessions = 12,
    ExpireHandles = 13,
    FenceOperation = 14,
    ReserveOperationIds = 15,

    CreateDirectory = 100,
    Move = 101,
    Delete = 102,

    OpenHandle = 200,
    CloseHandle = 201,
    CreateStaging = 202,
    CloseStaging = 203,
    CommitFile = 204,
    WriteAll = 205,
    Append = 206,
    Copy = 207,
    ReclaimDetached = 208,

    TxBegin = 300,
    TxCommit = 301,
    TxAbort = 302,
    TxAcquireReservation = 303,

    CreateSnapshot = 400,
    DeleteSnapshot = 401,
    SetSnapshotRetention = 402,
    AcquirePin = 403,
    ReleasePin = 404,
    ExpirePins = 405,

    PruneDerefLog = 500,
    RestoreChunk = 510,
    RestoreComplete = 511,
    RestoreBegin = 512,
    RestoreFinish = 513,

    RecordBackupProgress = 600,
    AbandonTransferLineage = 601,

    SetPolicy = 700,
    SetAcl = 701,
    SetOwner = 702,
    RotateDataKey = 703,
    MigratePolicy = 704,
}

/// <summary>Leader-side switches carried by a command so negative-control mutations stay deterministic (MUT-*).</summary>
[Flags]
internal enum CommandFlags : byte
{
    None = 0,

    /// <summary>SafetyMutations.DisableOperationDedup: execute even when an outcome is retained.</summary>
    DisableDedup = 1,

    /// <summary>SafetyMutations.ForgetExpiredIdWatermark: ignore retirement watermarks in the operation gate.</summary>
    ForgetWatermark = 2,

    /// <summary>SafetyMutations.SkipFencingCheck: accept publications with a stale writer generation.</summary>
    SkipFencing = 4,

    /// <summary>
    /// SafetyMutations.SkipPredicateValidation: a transaction commit does not validate its predicates (absent-name lookups
    /// and directory enumerations).
    /// </summary>
    SkipPredicates = 8,
}

/// <summary>
/// Identity and authorization context of a client command (ARCHITECTURE §5.2, §17): the operation id and its payload identity
/// for the operation gate, the principal, and the identity set and volume grants the deciding leader resolved (apply re-checks
/// ACLs deterministically with them, ACL-08). <see cref="Id"/> may be empty for requests without an operation id (then the
/// gate is skipped).
/// </summary>
/// <param name="Id">Operation id (empty = none).</param>
/// <param name="Principal">Requesting principal.</param>
/// <param name="Operation">Request kind (bound to the id).</param>
/// <param name="PayloadHash">Payload identity of the request (bound to the id).</param>
/// <param name="RequestSession">Session in the request header (0 = none).</param>
/// <param name="AckWatermark">Client acknowledgement watermark of <paramref name="RequestSession"/>.</param>
/// <param name="Identities">Caller identity set (ordinal identity strings).</param>
/// <param name="IsAdmin">True when the caller is a cluster administrator (Administrators).</param>
/// <param name="Grants">Volume grants the leader resolved for the principal.</param>
/// <param name="Flags">Negative-control switches.</param>
internal sealed record OperationHeader(DfsOperationId Id, string Principal, DfsOperationKind Operation, UInt128 PayloadHash, ulong RequestSession,
    ulong AckWatermark, ImmutableArray<string> Identities, bool IsAdmin, DfsPermissions Grants, CommandFlags Flags)
{
    private IReadOnlySet<DfsIdentity>? _identitySet;

    /// <summary>The caller identity set (parsed once).</summary>
    public IReadOnlySet<DfsIdentity> IdentitySet => _identitySet ??= ParseIdentities(Identities, IsAdmin);

    /// <summary>The principal as an identity (<c>user:x</c>).</summary>
    public DfsIdentity User => DfsIdentity.TryParse("user:" + Principal, out DfsIdentity id) ? id : DfsIdentity.Everyone;

    /// <summary>True when the command carries an operation id.</summary>
    public bool HasOperation => !Id.IsEmpty;

    internal void Write(ref WireWriter w)
    {
        w.WriteOperationId(Id);
        w.WriteString(Principal);
        w.WriteVarInt((int)Operation);
        w.WriteUInt128(PayloadHash);
        w.WriteVarUInt(RequestSession);
        w.WriteVarUInt(AckWatermark);
        w.WriteCount(Identities.Length);
        foreach (string id in Identities)
            w.WriteString(id);
        w.WriteBool(IsAdmin);
        w.WriteVarInt((int)Grants);
        w.WriteByte((byte)Flags);
    }

    internal static OperationHeader Read(ref WireReader r)
    {
        DfsOperationId id = r.ReadOperationId();
        string principal = r.ReadString();
        var operation = (DfsOperationKind)r.ReadVarInt32();
        UInt128 hash = r.ReadUInt128();
        ulong session = r.ReadVarUInt();
        ulong ack = r.ReadVarUInt();
        int count = r.ReadCount(1);
        var identities = ImmutableArray.CreateBuilder<string>(count);
        for (int i = 0; i < count; i++)
            identities.Add(r.ReadString());
        bool isAdmin = r.ReadBool();
        var grants = (DfsPermissions)r.ReadVarInt32();
        var flags = (CommandFlags)r.ReadByte();
        return new OperationHeader(id, principal, operation, hash, session, ack, identities.MoveToImmutable(), isAdmin, grants, flags);
    }

    private static HashSet<DfsIdentity> ParseIdentities(ImmutableArray<string> identities, bool isAdmin)
    {
        var set = new HashSet<DfsIdentity>();
        foreach (string text in identities)
        {
            if (DfsIdentity.TryParse(text, out DfsIdentity id))
                set.Add(id);
        }

        if (isAdmin)
            set.Add(DfsIdentity.Administrators);
        return set;
    }
}

/// <summary>
/// Base of every volume log command. Every command carries the leader's wall clock (<see cref="LeaderTime"/>, the only "now"
/// apply may use, ARCHITECTURE §3.1 rule 5); client commands carry an <see cref="OperationHeader"/>.
/// </summary>
internal abstract record VolumeCommand
{
    /// <summary>Command kind (persisted).</summary>
    public abstract VolumeCommandKind Kind { get; }

    /// <summary>Leader wall clock when the command was built.</summary>
    public DateTimeOffset LeaderTime { get; init; }

    /// <summary>Operation header of client commands (null for leader housekeeping commands).</summary>
    public OperationHeader? Op { get; init; }

    /// <summary>Protocol operation kind for fault contexts.</summary>
    public virtual DfsOperationKind OperationKind => Op?.Operation ?? DfsOperationKind.Unknown;

    /// <summary>Path for fault contexts, when the command addresses one.</summary>
    public virtual string? Path => null;

    /// <summary>Writes the command-specific body.</summary>
    internal abstract void WriteBody(ref WireWriter w);
}
