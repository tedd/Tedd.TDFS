using System.Buffers;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;

namespace Tedd.Tdfs.Server.Control;

/// <summary>Kinds of control-group log commands (ARCHITECTURE §2.4, §14, Addendum A). Values are durable: never reuse.</summary>
internal enum ControlCommandKind : ushort
{
    BootstrapCluster = 1,
    AddMember = 10,
    RegisterIncarnation = 11,
    AdmitMember = 12,
    SetMemberStatus = 13,
    SetMemberLabels = 14,
    CreateVolume = 20,
    DeleteVolume = 21,
    SetVolumeVoters = 22,
    SetDefaultPolicy = 23,
    DefinePolicy = 24,
    AddDataKey = 25,
    CompletePolicyMigration = 26,
    CreatePrincipal = 30,
    DeletePrincipal = 31,
    SetGrant = 32,
    CreateGroup = 40,
    DeleteGroup = 41,
    AddGroupMember = 42,
    RemoveGroupMember = 43,
    CreatePool = 50,
    SetPoolMembers = 51,
    DeletePool = 52,
    RegisterBackupTarget = 60,
    RemoveBackupTarget = 61,
    SetFeature = 70,
    AppendAudit = 80,
}

/// <summary>
/// Header of every control command: the deciding leader's identity data. Time is the leader's wall clock carried in the
/// entry (apply never reads a clock, ARCHITECTURE §3.1 rule 5).
/// </summary>
/// <param name="RequestId">Administrative request id (0 = not deduplicated).</param>
/// <param name="Principal">Principal on whose behalf the command was proposed ("" for node-internal commands).</param>
/// <param name="Time">Leader wall clock when proposing.</param>
internal readonly record struct CommandHeader(UInt128 RequestId, string Principal, DateTimeOffset Time);

/// <summary>Base of control commands.</summary>
internal abstract record ControlCommand(CommandHeader Header)
{
    public abstract ControlCommandKind Kind { get; }

    /// <summary>Protocol operation kind for fault contexts.</summary>
    public virtual DfsOperationKind FaultOperation => DfsOperationKind.Admin;
}

/// <summary>Bootstrap member description inside <see cref="BootstrapClusterCommand"/>.</summary>
internal sealed record BootstrapMemberData(NodeId Id, string Name, DfsTopologyLabels Labels, IReadOnlyList<string> Endpoints);

/// <summary>Principal with its verifier (never the secret).</summary>
internal sealed record PrincipalData(string Name, ReadOnlyMemory<byte> Salt, int Iterations, ReadOnlyMemory<byte> StoredKey, ReadOnlyMemory<byte> ServerKey, bool ClusterAdmin);

internal sealed record BootstrapClusterCommand(CommandHeader Header, Guid ClusterId, string ClusterName, int ControlVoterTarget,
    IReadOnlyList<BootstrapMemberData> Members, IReadOnlyList<PrincipalData> Principals, IReadOnlyList<string> MemberFeatures) : ControlCommand(Header)
{
    public override ControlCommandKind Kind => ControlCommandKind.BootstrapCluster;

    public override DfsOperationKind FaultOperation => DfsOperationKind.MembershipChange;
}

internal sealed record AddMemberCommand(CommandHeader Header, NodeId Id, string Name, DfsTopologyLabels Labels, IReadOnlyList<string> Endpoints,
    ulong Incarnation, UInt128 BootId, long CapacityBytes, int ProtocolMax, IReadOnlyList<string> Features) : ControlCommand(Header)
{
    public override ControlCommandKind Kind => ControlCommandKind.AddMember;

    public override DfsOperationKind FaultOperation => DfsOperationKind.MembershipChange;
}

internal sealed record RegisterIncarnationCommand(CommandHeader Header, NodeId Id, ulong Incarnation, UInt128 BootId, DfsTopologyLabels Labels,
    IReadOnlyList<string> Endpoints, long CapacityBytes) : ControlCommand(Header)
{
    public override ControlCommandKind Kind => ControlCommandKind.RegisterIncarnation;

    public override DfsOperationKind FaultOperation => DfsOperationKind.MembershipChange;
}

internal sealed record AdmitMemberCommand(CommandHeader Header, NodeId Id, string Name, DfsTopologyLabels Labels, IReadOnlyList<string> Endpoints)
    : ControlCommand(Header)
{
    public override ControlCommandKind Kind => ControlCommandKind.AdmitMember;
}

internal sealed record SetMemberStatusCommand(CommandHeader Header, NodeId Id, WireNodeStatus Status, bool Force) : ControlCommand(Header)
{
    public override ControlCommandKind Kind => ControlCommandKind.SetMemberStatus;
}

internal sealed record SetMemberLabelsCommand(CommandHeader Header, NodeId Id, DfsTopologyLabels Labels) : ControlCommand(Header)
{
    public override ControlCommandKind Kind => ControlCommandKind.SetMemberLabels;
}

internal sealed record CreateVolumeCommand(CommandHeader Header, VolumeCreation Creation, ReadOnlyMemory<byte> WrappedDataKey) : ControlCommand(Header)
{
    public override ControlCommandKind Kind => ControlCommandKind.CreateVolume;
}

internal sealed record DeleteVolumeCommand(CommandHeader Header, VolumeId Volume) : ControlCommand(Header)
{
    public override ControlCommandKind Kind => ControlCommandKind.DeleteVolume;
}

internal sealed record SetVolumeVotersCommand(CommandHeader Header, VolumeId Volume, IReadOnlyList<NodeId> Voters, IReadOnlyList<NodeId> Learners)
    : ControlCommand(Header)
{
    public override ControlCommandKind Kind => ControlCommandKind.SetVolumeVoters;

    public override DfsOperationKind FaultOperation => DfsOperationKind.MembershipChange;
}

internal sealed record SetDefaultPolicyCommand(CommandHeader Header, VolumeId Volume, DfsStoragePolicy Policy, bool Migrate) : ControlCommand(Header)
{
    public override ControlCommandKind Kind => ControlCommandKind.SetDefaultPolicy;

    public override DfsOperationKind FaultOperation => DfsOperationKind.PolicyChange;
}

internal sealed record DefinePolicyCommand(CommandHeader Header, VolumeId Volume, string PolicyName, DfsStoragePolicy Policy) : ControlCommand(Header)
{
    public override ControlCommandKind Kind => ControlCommandKind.DefinePolicy;

    public override DfsOperationKind FaultOperation => DfsOperationKind.PolicyChange;
}

internal sealed record CompletePolicyMigrationCommand(CommandHeader Header, VolumeId Volume) : ControlCommand(Header)
{
    public override ControlCommandKind Kind => ControlCommandKind.CompletePolicyMigration;

    public override DfsOperationKind FaultOperation => DfsOperationKind.PolicyChange;
}

internal sealed record AddDataKeyCommand(CommandHeader Header, VolumeId Volume, uint Version, ReadOnlyMemory<byte> WrappedKey) : ControlCommand(Header)
{
    public override ControlCommandKind Kind => ControlCommandKind.AddDataKey;

    public override DfsOperationKind FaultOperation => DfsOperationKind.KeyRotation;
}

internal sealed record CreatePrincipalCommand(CommandHeader Header, PrincipalData Principal) : ControlCommand(Header)
{
    public override ControlCommandKind Kind => ControlCommandKind.CreatePrincipal;
}

internal sealed record DeletePrincipalCommand(CommandHeader Header, string Name) : ControlCommand(Header)
{
    public override ControlCommandKind Kind => ControlCommandKind.DeletePrincipal;
}

internal sealed record SetGrantCommand(CommandHeader Header, VolumeId Volume, string Principal, DfsPermissions Permissions, bool Revoke) : ControlCommand(Header)
{
    public override ControlCommandKind Kind => ControlCommandKind.SetGrant;
}

internal sealed record GroupCommand(CommandHeader Header, ControlCommandKind GroupKind, string Group, string? Principal) : ControlCommand(Header)
{
    public override ControlCommandKind Kind => GroupKind;
}

internal sealed record PoolCommand(CommandHeader Header, ControlCommandKind PoolKind, string Pool, IReadOnlyList<NodeId> Nodes) : ControlCommand(Header)
{
    public override ControlCommandKind Kind => PoolKind;
}

internal sealed record RegisterBackupTargetCommand(CommandHeader Header, VolumeId Volume, BackupTargetRecord Target) : ControlCommand(Header)
{
    public override ControlCommandKind Kind => ControlCommandKind.RegisterBackupTarget;
}

internal sealed record RemoveBackupTargetCommand(CommandHeader Header, VolumeId Volume, string Target) : ControlCommand(Header)
{
    public override ControlCommandKind Kind => ControlCommandKind.RemoveBackupTarget;
}

internal sealed record SetFeatureCommand(CommandHeader Header, string Feature, bool Enabled) : ControlCommand(Header)
{
    public override ControlCommandKind Kind => ControlCommandKind.SetFeature;
}

internal sealed record AppendAuditCommand(CommandHeader Header, string Operation, string Outcome, string Scope) : ControlCommand(Header)
{
    public override ControlCommandKind Kind => ControlCommandKind.AppendAudit;
}

/// <summary>
/// Canonical encoding of control commands: <c>u8 format | u16 kind | header | fields | extension block</c>. Decoding is
/// strict (unknown kinds and malformed bodies are rejected); an undecodable committed command is applied as a failed
/// no-op on every replica alike.
/// </summary>
internal static class ControlCommandCodec
{
    private const byte Format = 1;

    public static byte[] Encode(ControlCommand command)
    {
        ArgumentNullException.ThrowIfNull(command);
        var buffer = new ArrayBufferWriter<byte>(256);
        var w = new WireWriter(buffer);
        w.WriteByte(Format);
        w.WriteUInt16((ushort)command.Kind);
        w.WriteUInt128(command.Header.RequestId);
        w.WriteString(command.Header.Principal ?? string.Empty);
        w.WriteDateTimeOffset(command.Header.Time);
        switch (command)
        {
            case BootstrapClusterCommand c:
                w.WriteGuid(c.ClusterId);
                w.WriteString(c.ClusterName);
                w.WriteVarInt32(c.ControlVoterTarget);
                w.WriteCount(c.Members.Count);
                foreach (BootstrapMemberData m in c.Members)
                {
                    w.WriteNodeId(m.Id);
                    w.WriteString(m.Name);
                    w.WriteTopologyLabels(m.Labels);
                    w.WriteStringList(m.Endpoints);
                }

                w.WriteCount(c.Principals.Count);
                foreach (PrincipalData p in c.Principals)
                    ControlCodec.WritePrincipalData(ref w, p);
                w.WriteStringList(c.MemberFeatures);
                break;
            case AddMemberCommand c:
                w.WriteNodeId(c.Id);
                w.WriteString(c.Name);
                w.WriteTopologyLabels(c.Labels);
                w.WriteStringList(c.Endpoints);
                w.WriteVarUInt(c.Incarnation);
                w.WriteUInt128(c.BootId);
                w.WriteVarInt(c.CapacityBytes);
                w.WriteVarInt32(c.ProtocolMax);
                w.WriteStringList(c.Features);
                break;
            case RegisterIncarnationCommand c:
                w.WriteNodeId(c.Id);
                w.WriteVarUInt(c.Incarnation);
                w.WriteUInt128(c.BootId);
                w.WriteTopologyLabels(c.Labels);
                w.WriteStringList(c.Endpoints);
                w.WriteVarInt(c.CapacityBytes);
                break;
            case AdmitMemberCommand c:
                w.WriteNodeId(c.Id);
                w.WriteString(c.Name);
                w.WriteTopologyLabels(c.Labels);
                w.WriteStringList(c.Endpoints);
                break;
            case SetMemberStatusCommand c:
                w.WriteNodeId(c.Id);
                w.WriteDefinedEnum(c.Status);
                w.WriteBool(c.Force);
                break;
            case SetMemberLabelsCommand c:
                w.WriteNodeId(c.Id);
                w.WriteTopologyLabels(c.Labels);
                break;
            case CreateVolumeCommand c:
                ControlCodec.WriteCreation(ref w, c.Creation);
                w.WriteBytes(c.WrappedDataKey.Span);
                break;
            case DeleteVolumeCommand c:
                w.WriteVolumeId(c.Volume);
                break;
            case SetVolumeVotersCommand c:
                w.WriteVolumeId(c.Volume);
                w.WriteNodeIdList(c.Voters);
                w.WriteNodeIdList(c.Learners);
                break;
            case SetDefaultPolicyCommand c:
                w.WriteVolumeId(c.Volume);
                w.WriteStoragePolicy(c.Policy);
                w.WriteBool(c.Migrate);
                break;
            case DefinePolicyCommand c:
                w.WriteVolumeId(c.Volume);
                w.WriteString(c.PolicyName);
                w.WriteStoragePolicy(c.Policy);
                break;
            case CompletePolicyMigrationCommand c:
                w.WriteVolumeId(c.Volume);
                break;
            case AddDataKeyCommand c:
                w.WriteVolumeId(c.Volume);
                w.WriteVarUInt(c.Version);
                w.WriteBytes(c.WrappedKey.Span);
                break;
            case CreatePrincipalCommand c:
                ControlCodec.WritePrincipalData(ref w, c.Principal);
                break;
            case DeletePrincipalCommand c:
                w.WriteString(c.Name);
                break;
            case SetGrantCommand c:
                w.WriteVolumeId(c.Volume);
                w.WriteString(c.Principal);
                w.WriteEnum(c.Permissions);
                w.WriteBool(c.Revoke);
                break;
            case GroupCommand c:
                w.WriteString(c.Group);
                w.WriteOptionalString(c.Principal);
                break;
            case PoolCommand c:
                w.WriteString(c.Pool);
                w.WriteNodeIdList(c.Nodes);
                break;
            case RegisterBackupTargetCommand c:
                w.WriteVolumeId(c.Volume);
                ControlCodec.WriteBackupTarget(ref w, c.Target);
                break;
            case RemoveBackupTargetCommand c:
                w.WriteVolumeId(c.Volume);
                w.WriteString(c.Target);
                break;
            case SetFeatureCommand c:
                w.WriteString(c.Feature);
                w.WriteBool(c.Enabled);
                break;
            case AppendAuditCommand c:
                w.WriteString(c.Operation);
                w.WriteString(c.Outcome);
                w.WriteString(c.Scope);
                break;
            default:
                throw new ArgumentException($"Unknown control command {command.GetType().Name}.", nameof(command));
        }

        w.WriteExtensionBlock([]);
        return buffer.WrittenSpan.ToArray();
    }

    /// <summary>Decodes a command.</summary>
    /// <exception cref="DfsProtocolException">The bytes are malformed or the kind is unknown.</exception>
    public static ControlCommand Decode(ReadOnlySpan<byte> data)
    {
        var r = new WireReader(data, WireLimits.Default);
        if (r.ReadByte() != Format)
            throw WireReader.Malformed("Unknown control command format.");
        var kind = (ControlCommandKind)r.ReadUInt16();
        var header = new CommandHeader(r.ReadUInt128(), r.ReadString(), r.ReadDateTimeOffset());
        ControlCommand command;
        switch (kind)
        {
            case ControlCommandKind.BootstrapCluster:
            {
                Guid clusterId = r.ReadGuid();
                string clusterName = r.ReadString();
                int target = r.ReadVarInt32();
                int memberCount = r.ReadCount(20);
                var members = new BootstrapMemberData[memberCount];
                for (int i = 0; i < memberCount; i++)
                    members[i] = new BootstrapMemberData(r.ReadNodeId(), r.ReadString(), r.ReadTopologyLabels(), r.ReadStringList());
                int principalCount = r.ReadCount(6);
                var principals = new PrincipalData[principalCount];
                for (int i = 0; i < principalCount; i++)
                    principals[i] = ControlCodec.ReadPrincipalData(ref r);
                command = new BootstrapClusterCommand(header, clusterId, clusterName, target, members, principals, r.ReadStringList());
                break;
            }

            case ControlCommandKind.AddMember:
                command = new AddMemberCommand(header, r.ReadNodeId(), r.ReadString(), r.ReadTopologyLabels(), r.ReadStringList(), r.ReadVarUInt(),
                    r.ReadUInt128(), r.ReadVarInt(), r.ReadVarInt32(), r.ReadStringList());
                break;
            case ControlCommandKind.RegisterIncarnation:
                command = new RegisterIncarnationCommand(header, r.ReadNodeId(), r.ReadVarUInt(), r.ReadUInt128(), r.ReadTopologyLabels(),
                    r.ReadStringList(), r.ReadVarInt());
                break;
            case ControlCommandKind.AdmitMember:
                command = new AdmitMemberCommand(header, r.ReadNodeId(), r.ReadString(), r.ReadTopologyLabels(), r.ReadStringList());
                break;
            case ControlCommandKind.SetMemberStatus:
                command = new SetMemberStatusCommand(header, r.ReadNodeId(), r.ReadEnum<WireNodeStatus>(), r.ReadBool());
                break;
            case ControlCommandKind.SetMemberLabels:
                command = new SetMemberLabelsCommand(header, r.ReadNodeId(), r.ReadTopologyLabels());
                break;
            case ControlCommandKind.CreateVolume:
                command = new CreateVolumeCommand(header, ControlCodec.ReadCreation(ref r), r.ReadBytesCopy());
                break;
            case ControlCommandKind.DeleteVolume:
                command = new DeleteVolumeCommand(header, r.ReadVolumeId());
                break;
            case ControlCommandKind.SetVolumeVoters:
                command = new SetVolumeVotersCommand(header, r.ReadVolumeId(), r.ReadNodeIdList(), r.ReadNodeIdList());
                break;
            case ControlCommandKind.SetDefaultPolicy:
                command = new SetDefaultPolicyCommand(header, r.ReadVolumeId(), r.ReadStoragePolicy(), r.ReadBool());
                break;
            case ControlCommandKind.DefinePolicy:
                command = new DefinePolicyCommand(header, r.ReadVolumeId(), r.ReadString(), r.ReadStoragePolicy());
                break;
            case ControlCommandKind.CompletePolicyMigration:
                command = new CompletePolicyMigrationCommand(header, r.ReadVolumeId());
                break;
            case ControlCommandKind.AddDataKey:
            {
                VolumeId volume = r.ReadVolumeId();
                ulong version = r.ReadVarUInt();
                if (version is 0 or > uint.MaxValue)
                    throw WireReader.Malformed("Data key version out of range.");
                command = new AddDataKeyCommand(header, volume, (uint)version, r.ReadBytesCopy());
                break;
            }

            case ControlCommandKind.CreatePrincipal:
                command = new CreatePrincipalCommand(header, ControlCodec.ReadPrincipalData(ref r));
                break;
            case ControlCommandKind.DeletePrincipal:
                command = new DeletePrincipalCommand(header, r.ReadString());
                break;
            case ControlCommandKind.SetGrant:
                command = new SetGrantCommand(header, r.ReadVolumeId(), r.ReadString(), r.ReadEnumRaw<DfsPermissions>(), r.ReadBool());
                break;
            case ControlCommandKind.CreateGroup or ControlCommandKind.DeleteGroup or ControlCommandKind.AddGroupMember or ControlCommandKind.RemoveGroupMember:
                command = new GroupCommand(header, kind, r.ReadString(), r.ReadOptionalString());
                break;
            case ControlCommandKind.CreatePool or ControlCommandKind.SetPoolMembers or ControlCommandKind.DeletePool:
                command = new PoolCommand(header, kind, r.ReadString(), r.ReadNodeIdList());
                break;
            case ControlCommandKind.RegisterBackupTarget:
                command = new RegisterBackupTargetCommand(header, r.ReadVolumeId(), ControlCodec.ReadBackupTarget(ref r));
                break;
            case ControlCommandKind.RemoveBackupTarget:
                command = new RemoveBackupTargetCommand(header, r.ReadVolumeId(), r.ReadString());
                break;
            case ControlCommandKind.SetFeature:
                command = new SetFeatureCommand(header, r.ReadString(), r.ReadBool());
                break;
            case ControlCommandKind.AppendAudit:
                command = new AppendAuditCommand(header, r.ReadString(), r.ReadString(), r.ReadString());
                break;
            default:
                throw WireReader.Malformed($"Unknown control command kind {(ushort)kind}.");
        }

        r.ReadExtensions();
        r.EnsureEnd();
        return command;
    }
}
