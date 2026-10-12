namespace Tedd.Tdfs.Protocol.Messages;

// Administrative bodies of Addendum A (groups and storage pools). Same conventions as AdminBodies.cs.

/// <summary>Request naming one principal group (<see cref="AdminOp.CreateGroup"/>, <see cref="AdminOp.DeleteGroup"/>).</summary>
/// <param name="Group">Group name (a valid <see cref="DfsIdentity"/> name).</param>
public sealed record AdminGroupNameBody(
    string Group) : IWireRecord<AdminGroupNameBody>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 2;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteString(Group);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static AdminGroupNameBody Read(ref WireReader reader)
    {
        string group = reader.ReadString();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new AdminGroupNameBody(group) { Extensions = extensions };
    }
}

/// <summary>Request of <see cref="AdminOp.AddGroupMember"/> and <see cref="AdminOp.RemoveGroupMember"/>.</summary>
/// <param name="Group">Group name.</param>
/// <param name="Principal">Principal name.</param>
public sealed record AdminGroupMemberBody(
    string Group,
    string Principal) : IWireRecord<AdminGroupMemberBody>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 3;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteString(Group);
        writer.WriteString(Principal);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static AdminGroupMemberBody Read(ref WireReader reader)
    {
        string group = reader.ReadString();
        string principal = reader.ReadString();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new AdminGroupMemberBody(group, principal) { Extensions = extensions };
    }
}

/// <summary>One principal group (mirrors the SDK's <c>DfsGroupInfo</c>).</summary>
/// <param name="Name">Group name.</param>
/// <param name="Members">Member principals, ordinal order.</param>
public sealed record GroupInfoData(
    string Name,
    IReadOnlyList<string> Members) : IWireRecord<GroupInfoData>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 3;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteString(Name);
        writer.WriteStringList(Members);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static GroupInfoData Read(ref WireReader reader)
    {
        string name = reader.ReadString();
        IReadOnlyList<string> members = reader.ReadStringList();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new GroupInfoData(name, members) { Extensions = extensions };
    }
}

/// <summary>Result of <see cref="AdminOp.ListGroups"/>.</summary>
/// <param name="Groups">Groups ordered by name (ordinal).</param>
public sealed record AdminGroupListBody(
    IReadOnlyList<GroupInfoData> Groups) : IWireRecord<AdminGroupListBody>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 2;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteList(Groups);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static AdminGroupListBody Read(ref WireReader reader)
    {
        IReadOnlyList<GroupInfoData> groups = reader.ReadList<GroupInfoData>();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new AdminGroupListBody(groups) { Extensions = extensions };
    }
}

/// <summary>Request of <see cref="AdminOp.CreatePool"/> and <see cref="AdminOp.SetPoolMembers"/>.</summary>
/// <param name="Pool">Pool name (<see cref="DfsPoolNames"/> rules).</param>
/// <param name="Nodes">Member node names (non-empty, existing, not removed).</param>
public sealed record AdminPoolMembersBody(
    string Pool,
    IReadOnlyList<string> Nodes) : IWireRecord<AdminPoolMembersBody>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 3;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteString(Pool);
        writer.WriteStringList(Nodes);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static AdminPoolMembersBody Read(ref WireReader reader)
    {
        string pool = reader.ReadString();
        IReadOnlyList<string> nodes = reader.ReadStringList();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new AdminPoolMembersBody(pool, nodes) { Extensions = extensions };
    }
}

/// <summary>Request naming one pool (<see cref="AdminOp.DeletePool"/>, <see cref="AdminOp.PoolStatus"/>).</summary>
/// <param name="Pool">Pool name.</param>
public sealed record AdminPoolNameBody(
    string Pool) : IWireRecord<AdminPoolNameBody>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 2;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteString(Pool);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static AdminPoolNameBody Read(ref WireReader reader)
    {
        string pool = reader.ReadString();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new AdminPoolNameBody(pool) { Extensions = extensions };
    }
}

/// <summary>One storage pool (mirrors the SDK's <c>DfsPoolInfo</c>).</summary>
/// <param name="Name">Pool name.</param>
/// <param name="Nodes">Member node names, ordinal order.</param>
/// <param name="Volumes">Names of volumes bound to the pool, ordinal order.</param>
/// <param name="CapacityBytes">Sum of the members' configured capacities.</param>
/// <param name="UsedBytes">Sum of the members' last reported usage (advisory).</param>
public sealed record PoolInfoData(
    string Name,
    IReadOnlyList<string> Nodes,
    IReadOnlyList<string> Volumes,
    long CapacityBytes,
    long UsedBytes) : IWireRecord<PoolInfoData>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 6;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteString(Name);
        writer.WriteStringList(Nodes);
        writer.WriteStringList(Volumes);
        writer.WriteVarInt(CapacityBytes);
        writer.WriteVarInt(UsedBytes);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static PoolInfoData Read(ref WireReader reader)
    {
        string name = reader.ReadString();
        IReadOnlyList<string> nodes = reader.ReadStringList();
        IReadOnlyList<string> volumes = reader.ReadStringList();
        long capacity = reader.ReadVarInt();
        long used = reader.ReadVarInt();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new PoolInfoData(name, nodes, volumes, capacity, used) { Extensions = extensions };
    }
}

/// <summary>Result of <see cref="AdminOp.ListPools"/>.</summary>
/// <param name="Pools">Pools ordered by name (ordinal), the implicit <c>default</c> pool included.</param>
public sealed record AdminPoolListBody(
    IReadOnlyList<PoolInfoData> Pools) : IWireRecord<AdminPoolListBody>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 2;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteList(Pools);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static AdminPoolListBody Read(ref WireReader reader)
    {
        IReadOnlyList<PoolInfoData> pools = reader.ReadList<PoolInfoData>();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new AdminPoolListBody(pools) { Extensions = extensions };
    }
}

/// <summary>Result of <see cref="AdminOp.PoolStatus"/> (mirrors the SDK's <c>DfsPoolStatus</c>).</summary>
/// <param name="Info">The pool.</param>
/// <param name="Replicas">Replica health aggregated over the pool's volumes.</param>
/// <param name="DrainingNodes">Member nodes being drained (ordinal order).</param>
public sealed record AdminPoolStatusBody(
    PoolInfoData Info,
    ReplicaHealthData Replicas,
    IReadOnlyList<string> DrainingNodes) : IWireRecord<AdminPoolStatusBody>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 18;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Info);
        writer.WriteRecord(Replicas);
        writer.WriteStringList(DrainingNodes);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static AdminPoolStatusBody Read(ref WireReader reader)
    {
        PoolInfoData info = reader.ReadRecord<PoolInfoData>();
        ReplicaHealthData replicas = reader.ReadRecord<ReplicaHealthData>();
        IReadOnlyList<string> draining = reader.ReadStringList();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new AdminPoolStatusBody(info, replicas, draining) { Extensions = extensions };
    }
}
