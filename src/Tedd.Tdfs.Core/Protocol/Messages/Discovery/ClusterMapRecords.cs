namespace Tedd.Tdfs.Protocol.Messages;

/// <summary>One member of the cluster map (live node or registered backup node).</summary>
/// <param name="Id">Node identity.</param>
/// <param name="Name">Unique node name.</param>
/// <param name="Labels">Failure-domain labels (copies count distinct hosts, zones count distinct region/zone pairs).</param>
/// <param name="Endpoints">Endpoints the node listens on (<c>quic://host:port</c> or <c>mem://name</c>), preferred first.</param>
/// <param name="Status">Membership status.</param>
/// <param name="Incarnation">Latest registered incarnation; receipts and Raft traffic with an older incarnation are rejected.</param>
/// <param name="IsBackup">True for a backup node (never a data copy, never a Raft member).</param>
/// <param name="IsControlVoter">True when the node is a voter of the control group.</param>
/// <param name="IsReachable">Control leader's current view of reachability (advisory).</param>
/// <param name="PlacementEligible">True when new object placements may target the node (active, not draining, not full).</param>
/// <param name="CapacityBytes">Configured capacity.</param>
/// <param name="UsedBytes">Last reported used bytes (advisory; free = capacity − used).</param>
public sealed record NodeEntry(
    NodeId Id,
    string Name,
    DfsTopologyLabels Labels,
    IReadOnlyList<string> Endpoints,
    WireNodeStatus Status,
    ulong Incarnation,
    bool IsBackup,
    bool IsControlVoter,
    bool IsReachable,
    bool PlacementEligible,
    long CapacityBytes,
    long UsedBytes) : IWireRecord<NodeEntry>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 16 + 1 + 5 + 1 + 1 + 1 + 4 + 1 + 1 + 1;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteNodeId(Id);
        writer.WriteString(Name);
        writer.WriteTopologyLabels(Labels);
        writer.WriteStringList(Endpoints);
        writer.WriteDefinedEnum(Status);
        writer.WriteVarUInt(Incarnation);
        writer.WriteBool(IsBackup);
        writer.WriteBool(IsControlVoter);
        writer.WriteBool(IsReachable);
        writer.WriteBool(PlacementEligible);
        writer.WriteVarInt(CapacityBytes);
        writer.WriteVarInt(UsedBytes);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static NodeEntry Read(ref WireReader reader)
    {
        NodeId id = reader.ReadNodeId();
        string name = reader.ReadString();
        DfsTopologyLabels labels = reader.ReadTopologyLabels();
        IReadOnlyList<string> endpoints = reader.ReadStringList();
        WireNodeStatus status = reader.ReadEnum<WireNodeStatus>();
        ulong incarnation = reader.ReadVarUInt();
        bool isBackup = reader.ReadBool();
        bool isVoter = reader.ReadBool();
        bool reachable = reader.ReadBool();
        bool eligible = reader.ReadBool();
        long capacity = reader.ReadVarInt();
        long used = reader.ReadVarInt();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new NodeEntry(id, name, labels, endpoints, status, incarnation, isBackup, isVoter, reachable, eligible, capacity, used)
        {
            Extensions = extensions,
        };
    }
}

/// <summary>One volume of the cluster map: identity, authority epoch, routing and policies.</summary>
/// <param name="Id">Volume identity.</param>
/// <param name="Name">Volume name.</param>
/// <param name="Epoch">Volume epoch (increments on restore/promotion; requests may pin it via <see cref="RequestHeader.ExpectedVolumeEpoch"/>).</param>
/// <param name="State">Lifecycle state.</param>
/// <param name="Voters">Voters of the volume Raft group.</param>
/// <param name="Learners">Learners of the volume Raft group.</param>
/// <param name="LeaderHint">Last known leader of the volume group (advisory).</param>
/// <param name="Metadata">Metadata (voter) policy.</param>
/// <param name="DefaultPolicy">Default storage policy for new files.</param>
/// <param name="NamedPolicies">Named alternative storage policies (ordinal names).</param>
/// <param name="InlineThreshold">Largest file length stored inline in metadata.</param>
/// <param name="Limits">Volume limits.</param>
public sealed record VolumeEntry(
    VolumeId Id,
    string Name,
    ulong Epoch,
    WireVolumeState State,
    IReadOnlyList<NodeId> Voters,
    IReadOnlyList<NodeId> Learners,
    NodeId? LeaderHint,
    DfsMetadataPolicy Metadata,
    DfsStoragePolicy DefaultPolicy,
    IReadOnlyDictionary<string, DfsStoragePolicy> NamedPolicies,
    int InlineThreshold,
    DfsVolumeLimits Limits) : IWireRecord<VolumeEntry>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 16 + 1 + 1 + 1 + 1 + 1 + 1 + 3 + 5 + 1 + 1 + 8 + 1 + 1 + 1;

    /// <summary>Storage pool the volume is bound to (Addendum A, POOL-02; trailing field).</summary>
    public string StoragePool { get; init; } = DfsPoolNames.Default;

    /// <summary>True when the volume enforces Traverse on ancestor directories (Addendum A, ACL-05; trailing field).</summary>
    public bool EnforceTraverseChecking { get; init; }

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteVolumeId(Id);
        writer.WriteString(Name);
        writer.WriteVarUInt(Epoch);
        writer.WriteDefinedEnum(State);
        writer.WriteNodeIdList(Voters);
        writer.WriteNodeIdList(Learners);
        writer.WriteOptionalNodeId(LeaderHint);
        writer.WriteMetadataPolicy(Metadata);
        writer.WriteStoragePolicy(DefaultPolicy);
        writer.WritePolicyMap(NamedPolicies);
        writer.WriteVarInt32(InlineThreshold);
        writer.WriteVolumeLimits(Limits);
        writer.WriteString(StoragePool ?? DfsPoolNames.Default);
        writer.WriteBool(EnforceTraverseChecking);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static VolumeEntry Read(ref WireReader reader)
    {
        VolumeId id = reader.ReadVolumeId();
        string name = reader.ReadString();
        ulong epoch = reader.ReadVarUInt();
        WireVolumeState state = reader.ReadEnum<WireVolumeState>();
        IReadOnlyList<NodeId> voters = reader.ReadNodeIdList();
        IReadOnlyList<NodeId> learners = reader.ReadNodeIdList();
        NodeId? leader = reader.ReadOptionalNodeId();
        DfsMetadataPolicy metadata = reader.ReadMetadataPolicy();
        DfsStoragePolicy defaultPolicy = reader.ReadStoragePolicy();
        IReadOnlyDictionary<string, DfsStoragePolicy> named = reader.ReadPolicyMap();
        int inline = reader.ReadVarInt32();
        DfsVolumeLimits limits = reader.ReadVolumeLimits();
        string pool = reader.ReadString();
        bool enforceTraverse = reader.ReadBool();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new VolumeEntry(id, name, epoch, state, voters, learners, leader, metadata, defaultPolicy, named, inline, limits)
        {
            StoragePool = pool,
            EnforceTraverseChecking = enforceTraverse,
            Extensions = extensions,
        };
    }
}

/// <summary>
/// The cluster map: identity, configuration position, members, volumes and capabilities. Served by every live node from
/// its applied control state (control learners have it too); a backup node serves a map containing only itself and the
/// volumes it holds published snapshots of.
/// </summary>
/// <param name="ClusterId">Cluster identity.</param>
/// <param name="ClusterName">Cluster name.</param>
/// <param name="ConfigIndex">Control-group applied index the map was built from (monotonic; used for change detection).</param>
/// <param name="PlacementEpoch">Current placement epoch (versioned placement map, ARCHITECTURE §6).</param>
/// <param name="Nodes">Members, ordered by node name (ordinal).</param>
/// <param name="Volumes">Volumes, ordered by volume name (ordinal).</param>
/// <param name="ControlLeader">Last known control leader (advisory; administrative requests go there).</param>
/// <param name="Capabilities">Cluster-wide capabilities and limits.</param>
public sealed record ClusterMapData(
    Guid ClusterId,
    string ClusterName,
    ulong ConfigIndex,
    ulong PlacementEpoch,
    IReadOnlyList<NodeEntry> Nodes,
    IReadOnlyList<VolumeEntry> Volumes,
    NodeId? ControlLeader,
    DfsServerCapabilities Capabilities) : IWireRecord<ClusterMapData>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 16 + 1 + 1 + 1 + 1 + 1 + 1 + 18 + 1;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteGuid(ClusterId);
        writer.WriteString(ClusterName);
        writer.WriteVarUInt(ConfigIndex);
        writer.WriteVarUInt(PlacementEpoch);
        writer.WriteList(Nodes);
        writer.WriteList(Volumes);
        writer.WriteOptionalNodeId(ControlLeader);
        writer.WriteServerCapabilities(Capabilities);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static ClusterMapData Read(ref WireReader reader)
    {
        Guid cluster = reader.ReadGuid();
        string name = reader.ReadString();
        ulong configIndex = reader.ReadVarUInt();
        ulong placementEpoch = reader.ReadVarUInt();
        IReadOnlyList<NodeEntry> nodes = reader.ReadList<NodeEntry>();
        IReadOnlyList<VolumeEntry> volumes = reader.ReadList<VolumeEntry>();
        NodeId? controlLeader = reader.ReadOptionalNodeId();
        DfsServerCapabilities capabilities = reader.ReadServerCapabilities();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new ClusterMapData(cluster, name, configIndex, placementEpoch, nodes, volumes, controlLeader, capabilities)
        {
            Extensions = extensions,
        };
    }
}
