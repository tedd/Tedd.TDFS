namespace Tedd.Tdfs.Protocol.Messages;

/// <summary>Requests the cluster map (any authenticated peer → any live or backup node).</summary>
/// <param name="KnownConfigIndex">Config index of the map the caller already has; when it equals the server's current
/// index the reply omits the map. 0 always returns the map.</param>
public sealed record GetClusterMapMessage(ulong KnownConfigIndex = 0) : IWireMessage<GetClusterMapMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.GetClusterMap;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteVarUInt(KnownConfigIndex);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static GetClusterMapMessage Read(ref WireReader reader)
    {
        ulong known = reader.ReadVarUInt();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new GetClusterMapMessage(known) { Extensions = extensions };
    }
}

/// <summary>Reply to <see cref="GetClusterMapMessage"/>.</summary>
/// <param name="ConfigIndex">Server's current map config index.</param>
/// <param name="Map">The map; null when the caller's <see cref="GetClusterMapMessage.KnownConfigIndex"/> is current.</param>
public sealed record ClusterMapResultMessage(ulong ConfigIndex, ClusterMapData? Map) : IWireMessage<ClusterMapResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.ClusterMapResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteVarUInt(ConfigIndex);
        writer.WriteOptional(Map);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static ClusterMapResultMessage Read(ref WireReader reader)
    {
        ulong configIndex = reader.ReadVarUInt();
        ClusterMapData? map = reader.ReadOptional<ClusterMapData>();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new ClusterMapResultMessage(configIndex, map) { Extensions = extensions };
    }
}

/// <summary>Requests the route of one volume (any authenticated peer → any live node).</summary>
/// <param name="Volume">Volume name.</param>
/// <param name="RequireLeader">When true the request must be served by the volume leader after a ReadIndex barrier, and
/// the reply carries the volume's last applied commit (used by <c>DfsVolume.GetInfoAsync</c>). A non-leader answers
/// <see cref="DfsErrorCode.NotLeader"/> with a leader hint.</param>
public sealed record GetVolumeRouteMessage(string Volume, bool RequireLeader = false) : IWireMessage<GetVolumeRouteMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.GetVolumeRoute;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteString(Volume);
        writer.WriteBool(RequireLeader);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static GetVolumeRouteMessage Read(ref WireReader reader)
    {
        string volume = reader.ReadString();
        bool requireLeader = reader.ReadBool();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new GetVolumeRouteMessage(volume, requireLeader) { Extensions = extensions };
    }
}

/// <summary>Reply to <see cref="GetVolumeRouteMessage"/>.</summary>
/// <param name="Volume">The volume's map entry.</param>
/// <param name="Leader">Current (or last known) leader of the volume group.</param>
/// <param name="LeaderEndpoint">Preferred endpoint of <paramref name="Leader"/>.</param>
/// <param name="Term">Raft term of the leadership the reply is based on (0 when unknown).</param>
/// <param name="LastCommit">Last applied commit of the volume (only with <see cref="GetVolumeRouteMessage.RequireLeader"/>; otherwise <see cref="CommitToken.None"/>).</param>
public sealed record VolumeRouteResultMessage(
    VolumeEntry Volume,
    NodeId? Leader,
    string? LeaderEndpoint,
    ulong Term,
    CommitToken LastCommit) : IWireMessage<VolumeRouteResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.VolumeRouteResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Volume);
        writer.WriteOptionalNodeId(Leader);
        writer.WriteOptionalString(LeaderEndpoint);
        writer.WriteVarUInt(Term);
        writer.WriteCommitToken(LastCommit);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static VolumeRouteResultMessage Read(ref WireReader reader)
    {
        VolumeEntry volume = reader.ReadRecord<VolumeEntry>();
        NodeId? leader = reader.ReadOptionalNodeId();
        string? endpoint = reader.ReadOptionalString();
        ulong term = reader.ReadVarUInt();
        CommitToken lastCommit = reader.ReadCommitToken();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new VolumeRouteResultMessage(volume, leader, endpoint, term, lastCommit) { Extensions = extensions };
    }
}

/// <summary>
/// Seed-join request (joining node → any live node; non-leaders answer <see cref="DfsErrorCode.NotLeader"/> with the
/// control leader hint). The control leader commits <c>AddMember</c> and replies after it is applied; the node then
/// becomes a learner of the control group. Idempotent: a repeated join of the same (id, name) returns the current
/// membership. A removed node id is rejected (<see cref="DfsErrorCode.AccessDenied"/>; re-admission needs an explicit
/// administrative join, CON-016).
/// </summary>
/// <param name="Node">Joining node identity.</param>
/// <param name="Name">Joining node name (unique).</param>
/// <param name="Labels">Failure-domain labels.</param>
/// <param name="Endpoints">Listen endpoints.</param>
/// <param name="Incarnation">Current incarnation of the joining node.</param>
/// <param name="CapacityBytes">Configured capacity.</param>
/// <param name="ProtocolMax">Newest protocol version the node speaks (for feature eligibility, FMT-009).</param>
/// <param name="Features">Features the node supports.</param>
/// <param name="BootId">Process boot id of the joining node (see <see cref="HelloMessage.BootId"/>); registered with the incarnation.</param>
public sealed record JoinClusterMessage(
    NodeId Node,
    string Name,
    DfsTopologyLabels Labels,
    IReadOnlyList<string> Endpoints,
    ulong Incarnation,
    long CapacityBytes,
    ushort ProtocolMax,
    IReadOnlySet<string> Features,
    UInt128 BootId = default) : IWireMessage<JoinClusterMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.JoinCluster;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteNodeId(Node);
        writer.WriteString(Name);
        writer.WriteTopologyLabels(Labels);
        writer.WriteStringList(Endpoints);
        writer.WriteVarUInt(Incarnation);
        writer.WriteVarInt(CapacityBytes);
        writer.WriteVarUInt(ProtocolMax);
        writer.WriteStringSet(Features);
        writer.WriteUInt128(BootId);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static JoinClusterMessage Read(ref WireReader reader)
    {
        NodeId node = reader.ReadNodeId();
        string name = reader.ReadString();
        DfsTopologyLabels labels = reader.ReadTopologyLabels();
        IReadOnlyList<string> endpoints = reader.ReadStringList();
        ulong incarnation = reader.ReadVarUInt();
        long capacity = reader.ReadVarInt();
        ushort protocolMax = reader.ReadVarUInt16();
        IReadOnlySet<string> features = reader.ReadStringSet();
        UInt128 bootId = reader.ReadUInt128();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new JoinClusterMessage(node, name, labels, endpoints, incarnation, capacity, protocolMax, features, bootId) { Extensions = extensions };
    }
}

/// <summary>Reply to <see cref="JoinClusterMessage"/>.</summary>
/// <param name="Accepted">True when the node is (now) a member; false when the receiver could not decide (retry at <paramref name="LeaderEndpoint"/>).</param>
/// <param name="Leader">Control leader (hint).</param>
/// <param name="LeaderEndpoint">Endpoint of the control leader (hint).</param>
/// <param name="Map">Cluster map including the new member (when accepted).</param>
public sealed record JoinClusterResultMessage(
    bool Accepted,
    NodeId? Leader,
    string? LeaderEndpoint,
    ClusterMapData? Map) : IWireMessage<JoinClusterResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.JoinClusterResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteBool(Accepted);
        writer.WriteOptionalNodeId(Leader);
        writer.WriteOptionalString(LeaderEndpoint);
        writer.WriteOptional(Map);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static JoinClusterResultMessage Read(ref WireReader reader)
    {
        bool accepted = reader.ReadBool();
        NodeId? leader = reader.ReadOptionalNodeId();
        string? endpoint = reader.ReadOptionalString();
        ClusterMapData? map = reader.ReadOptional<ClusterMapData>();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new JoinClusterResultMessage(accepted, leader, endpoint, map) { Extensions = extensions };
    }
}

/// <summary>
/// Registers a restarted node's new incarnation with the control group (node → control leader) before the node issues
/// receipts (RCV-02). Also updates the node's endpoints (a restarted node may listen on a new port) and reported
/// capacity. Rejected with <see cref="DfsErrorCode.StaleIncarnation"/> when the incarnation is lower than the registered
/// one, or equal to it with a different <c>BootId</c> (two processes started from a cloned disk pick the same
/// incarnation; the one registered second is fenced and must stop serving, RSY-021), and with
/// <see cref="DfsErrorCode.AccessDenied"/> for a removed node. Idempotent for the same (node, incarnation, boot id).
/// Raft traffic of a newer, not yet registered incarnation is accepted: committing the registration needs a control
/// quorum, which after a full-cluster restart consists only of not yet registered incarnations.
/// </summary>
/// <param name="Node">Node identity.</param>
/// <param name="Incarnation">New incarnation (greater than the registered one; equal only for a retry with the same boot id).</param>
/// <param name="Name">Node name (must match the membership).</param>
/// <param name="Labels">Current labels from the node configuration.</param>
/// <param name="Endpoints">Current listen endpoints.</param>
/// <param name="CapacityBytes">Configured capacity.</param>
/// <param name="BootId">Process boot id (see <see cref="HelloMessage.BootId"/>).</param>
public sealed record RegisterIncarnationMessage(
    NodeId Node,
    ulong Incarnation,
    string Name,
    DfsTopologyLabels Labels,
    IReadOnlyList<string> Endpoints,
    long CapacityBytes,
    UInt128 BootId = default) : IWireMessage<RegisterIncarnationMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.RegisterIncarnation;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteNodeId(Node);
        writer.WriteVarUInt(Incarnation);
        writer.WriteString(Name);
        writer.WriteTopologyLabels(Labels);
        writer.WriteStringList(Endpoints);
        writer.WriteVarInt(CapacityBytes);
        writer.WriteUInt128(BootId);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static RegisterIncarnationMessage Read(ref WireReader reader)
    {
        NodeId node = reader.ReadNodeId();
        ulong incarnation = reader.ReadVarUInt();
        string name = reader.ReadString();
        DfsTopologyLabels labels = reader.ReadTopologyLabels();
        IReadOnlyList<string> endpoints = reader.ReadStringList();
        long capacity = reader.ReadVarInt();
        UInt128 bootId = reader.ReadUInt128();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new RegisterIncarnationMessage(node, incarnation, name, labels, endpoints, capacity, bootId) { Extensions = extensions };
    }
}

/// <summary>Reply to <see cref="RegisterIncarnationMessage"/> (sent after the registration is committed and applied).</summary>
/// <param name="Accepted">True when the incarnation is registered.</param>
/// <param name="RegisteredIncarnation">Incarnation now registered for the node.</param>
/// <param name="Status">Membership status of the node.</param>
/// <param name="ConfigIndex">Control config index containing the registration.</param>
/// <param name="PlacementEpoch">Current placement epoch.</param>
public sealed record RegisterIncarnationResultMessage(
    bool Accepted,
    ulong RegisteredIncarnation,
    WireNodeStatus Status,
    ulong ConfigIndex,
    ulong PlacementEpoch) : IWireMessage<RegisterIncarnationResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.RegisterIncarnationResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteBool(Accepted);
        writer.WriteVarUInt(RegisteredIncarnation);
        writer.WriteDefinedEnum(Status);
        writer.WriteVarUInt(ConfigIndex);
        writer.WriteVarUInt(PlacementEpoch);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static RegisterIncarnationResultMessage Read(ref WireReader reader)
    {
        bool accepted = reader.ReadBool();
        ulong registered = reader.ReadVarUInt();
        WireNodeStatus status = reader.ReadEnum<WireNodeStatus>();
        ulong configIndex = reader.ReadVarUInt();
        ulong placementEpoch = reader.ReadVarUInt();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new RegisterIncarnationResultMessage(accepted, registered, status, configIndex, placementEpoch) { Extensions = extensions };
    }
}

/// <summary>
/// A security event (denied request, failed authentication) observed by a node that does not lead the control group,
/// forwarded to the control leader, which records it in the audit log (node → control leader; SEC-016). Never carries
/// payload bytes or secrets.
/// </summary>
/// <param name="Operation">Operation that was refused (for example <c>volume:write</c>, <c>authenticate</c>).</param>
/// <param name="Outcome">Outcome (for example <c>denied</c>).</param>
/// <param name="Scope">Scope of the event (volume, peer, required permission).</param>
/// <param name="Principal">Principal the event concerns (<c>-</c> when unknown).</param>
public sealed record AuditEventMessage(string Operation, string Outcome, string Scope, string Principal) : IWireMessage<AuditEventMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.AuditEvent;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteString(Operation);
        writer.WriteString(Outcome);
        writer.WriteString(Scope);
        writer.WriteString(Principal);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static AuditEventMessage Read(ref WireReader reader)
    {
        string operation = reader.ReadString();
        string outcome = reader.ReadString();
        string scope = reader.ReadString();
        string principal = reader.ReadString();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new AuditEventMessage(operation, outcome, scope, principal) { Extensions = extensions };
    }
}

/// <summary>Reply to <see cref="AuditEventMessage"/>.</summary>
/// <param name="Accepted">True when the control leader queued the record (false when its flood bound dropped it).</param>
public sealed record AuditEventResultMessage(bool Accepted) : IWireMessage<AuditEventResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.AuditEventResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteBool(Accepted);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static AuditEventResultMessage Read(ref WireReader reader)
    {
        bool accepted = reader.ReadBool();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new AuditEventResultMessage(accepted) { Extensions = extensions };
    }
}
