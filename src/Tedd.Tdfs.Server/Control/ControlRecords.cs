using System.Globalization;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;

namespace Tedd.Tdfs.Server.Control;

/// <summary>Deterministic ordering of node ids (by GUID value).</summary>
internal sealed class NodeIdComparer : IComparer<NodeId>
{
    public static readonly NodeIdComparer Instance = new();

    public int Compare(NodeId x, NodeId y) => x.Value.CompareTo(y.Value);
}

/// <summary>Deterministic ordering of volume ids (by GUID value).</summary>
internal sealed class VolumeIdComparer : IComparer<VolumeId>
{
    public static readonly VolumeIdComparer Instance = new();

    public int Compare(VolumeId x, VolumeId y) => x.Value.CompareTo(y.Value);
}

/// <summary>A cluster member (live node) in the control state.</summary>
/// <param name="Id">Node id.</param>
/// <param name="Name">Unique node name.</param>
/// <param name="Labels">Failure-domain labels.</param>
/// <param name="Endpoints">Listen endpoints, preferred first.</param>
/// <param name="Status">Membership status.</param>
/// <param name="Incarnation">Registered incarnation (0 until the first registration).</param>
/// <param name="BootId">Boot id registered with <paramref name="Incarnation"/> (0 = none yet).</param>
/// <param name="CapacityBytes">Configured capacity.</param>
/// <param name="UsedBytes">Last reported usage (advisory).</param>
/// <param name="ProtocolMax">Newest protocol version the node speaks.</param>
/// <param name="Features">Features the node supports (ordinal order).</param>
/// <param name="JoinedIndex">Control index at which the member was added.</param>
/// <param name="PendingAdmission">True for an administratively admitted member whose process has not joined yet.</param>
internal sealed record MemberRecord(NodeId Id, string Name, DfsTopologyLabels Labels, IReadOnlyList<string> Endpoints, WireNodeStatus Status,
    ulong Incarnation, UInt128 BootId, long CapacityBytes, long UsedBytes, int ProtocolMax, IReadOnlyList<string> Features, ulong JoinedIndex,
    bool PendingAdmission)
{
    /// <summary>True unless removed.</summary>
    public bool IsLive => Status != WireNodeStatus.Removed;

    /// <summary>True when new data and voter roles may be placed on the node.</summary>
    public bool IsPlacementEligible => Status == WireNodeStatus.Active && !PendingAdmission;
}

/// <summary>Immutable creation data of a volume: the input of its deterministic initial state (identical on every replica).</summary>
internal sealed record VolumeCreation(VolumeId Id, string Name, ulong Epoch, string Creator, DateTimeOffset CreatedUtc, ulong CreatedIndex,
    DfsMetadataPolicy Metadata, DfsStoragePolicy DefaultPolicy, IReadOnlyDictionary<string, DfsStoragePolicy> NamedPolicies, int InlineThreshold,
    DfsVolumeLimits Limits, string StoragePool, DfsSecurityDescriptor? RootSecurity, bool EnforceTraverseChecking, IReadOnlyList<NodeId> InitialVoters);

/// <summary>One data-encryption-key version of a volume, wrapped by the cluster KEK.</summary>
internal sealed record DataKeyVersion(uint Version, ReadOnlyMemory<byte> WrappedKey, ulong CreatedIndex);

/// <summary>A registered late-sync backup target of a volume (export secret wrapped by the KEK).</summary>
internal sealed record BackupTargetRecord(string Name, IReadOnlyList<string> Endpoints, string ExportPrincipal, ReadOnlyMemory<byte> WrappedExportSecret,
    TimeSpan? Interval, bool OnEverySnapshot, TimeSpan MaxPinAge, long MaxPinnedBytes, long MaxBytesPerSecond, int MaxConcurrentObjects, ulong RegisteredIndex);

/// <summary>A volume in the control state.</summary>
/// <param name="Creation">Immutable creation data.</param>
/// <param name="State">Lifecycle state.</param>
/// <param name="Voters">Desired voters of the volume group (reconciled by the volume leader).</param>
/// <param name="Learners">Desired learners of the volume group.</param>
/// <param name="DefaultPolicy">Current default storage policy.</param>
/// <param name="NamedPolicies">Current named policies (ordinal keys).</param>
/// <param name="MigrationTarget">Default policy being migrated to (effective once the volume service reports completion).</param>
/// <param name="DataKeys">Data-key versions, ascending.</param>
/// <param name="BackupTargets">Registered backup targets by name.</param>
internal sealed record VolumeRecord(VolumeCreation Creation, WireVolumeState State, IReadOnlyList<NodeId> Voters, IReadOnlyList<NodeId> Learners,
    DfsStoragePolicy DefaultPolicy, IReadOnlyDictionary<string, DfsStoragePolicy> NamedPolicies, DfsStoragePolicy? MigrationTarget,
    IReadOnlyList<DataKeyVersion> DataKeys, IReadOnlyDictionary<string, BackupTargetRecord> BackupTargets)
{
    public VolumeId Id => Creation.Id;

    public string Name => Creation.Name;

    public ulong Epoch => Creation.Epoch;

    public string StoragePool => Creation.StoragePool;

    /// <summary>Wire group name of the volume's Raft group.</summary>
    public string GroupName => ProtocolInfo.VolumeGroup(Creation.Id);

    /// <summary>Current data-key version (0 when none).</summary>
    public uint CurrentKeyVersion => DataKeys.Count == 0 ? 0 : DataKeys[^1].Version;

    /// <summary>True when <paramref name="node"/> is a desired voter or learner.</summary>
    public bool Hosts(NodeId node) => Voters.Contains(node) || Learners.Contains(node);
}

/// <summary>A principal with its SCRAM verifier (never the secret) and per-volume grants.</summary>
internal sealed record PrincipalRecord(string Name, ReadOnlyMemory<byte> Salt, int Iterations, ReadOnlyMemory<byte> StoredKey, ReadOnlyMemory<byte> ServerKey,
    bool ClusterAdmin, IReadOnlyDictionary<VolumeId, DfsPermissions> Grants, ulong CreatedIndex)
{
    /// <summary>The handshake verifier.</summary>
    public PrincipalVerifier Verifier => new(Salt.ToArray(), Iterations, StoredKey.ToArray(), ServerKey.ToArray());
}

/// <summary>A principal group (Addendum A, ACL-01).</summary>
internal sealed record GroupRecord(string Name, IReadOnlyList<string> Members, ulong CreatedIndex);

/// <summary>An explicit storage pool (Addendum A, POOL-01); <c>default</c> is implicit.</summary>
internal sealed record PoolRecord(string Name, IReadOnlyList<NodeId> Nodes, ulong CreatedIndex);

/// <summary>One audit entry (never secrets or payloads; SEC-016).</summary>
internal sealed record AuditEntry(ulong Index, DateTimeOffset Time, string Principal, string Operation, string Outcome, string Scope)
{
    /// <summary>The audit line returned by <c>AdminOp.AuditLog</c>.</summary>
    public override string ToString()
        => string.Create(CultureInfo.InvariantCulture,
            $"{Time.UtcDateTime:yyyy-MM-ddTHH:mm:ss.fffZ} #{Index} principal={Principal} op={Operation} outcome={Outcome} scope={Scope}");
}

/// <summary>Retained outcome of a deduplicated administrative request.</summary>
internal sealed record AdminOutcome(UInt128 RequestId, ControlCommandKind Kind, DfsErrorCode Code, string Message, Guid RefGuid, ulong RefNumber, ulong Index);
