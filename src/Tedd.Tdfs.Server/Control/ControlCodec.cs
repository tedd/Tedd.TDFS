using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;

namespace Tedd.Tdfs.Server.Control;

/// <summary>Canonical binary encoding of control records (commands and checkpoints).</summary>
internal static class ControlCodec
{
    public static void WritePrincipalData(ref WireWriter w, PrincipalData p)
    {
        w.WriteString(p.Name);
        w.WriteBytes(p.Salt.Span);
        w.WriteVarInt32(p.Iterations);
        w.WriteBytes(p.StoredKey.Span);
        w.WriteBytes(p.ServerKey.Span);
        w.WriteBool(p.ClusterAdmin);
    }

    public static PrincipalData ReadPrincipalData(ref WireReader r)
        => new(r.ReadString(), r.ReadBytesCopy(), r.ReadVarInt32(), r.ReadBytesCopy(), r.ReadBytesCopy(), r.ReadBool());

    public static void WriteCreation(ref WireWriter w, VolumeCreation c)
    {
        w.WriteVolumeId(c.Id);
        w.WriteString(c.Name);
        w.WriteVarUInt(c.Epoch);
        w.WriteString(c.Creator);
        w.WriteDateTimeOffset(c.CreatedUtc);
        w.WriteVarUInt(c.CreatedIndex);
        w.WriteMetadataPolicy(c.Metadata);
        w.WriteStoragePolicy(c.DefaultPolicy);
        w.WritePolicyMap(c.NamedPolicies);
        w.WriteVarInt32(c.InlineThreshold);
        w.WriteVolumeLimits(c.Limits);
        w.WriteString(c.StoragePool);
        w.WriteOptionalSecurityDescriptor(c.RootSecurity);
        w.WriteBool(c.EnforceTraverseChecking);
        w.WriteNodeIdList(c.InitialVoters);
    }

    public static VolumeCreation ReadCreation(ref WireReader r)
        => new(r.ReadVolumeId(), r.ReadString(), r.ReadVarUInt(), r.ReadString(), r.ReadDateTimeOffset(), r.ReadVarUInt(), r.ReadMetadataPolicy(),
            r.ReadStoragePolicy(), r.ReadPolicyMap(), r.ReadVarInt32(), r.ReadVolumeLimits(), r.ReadString(), r.ReadOptionalSecurityDescriptor(),
            r.ReadBool(), r.ReadNodeIdList());

    public static void WriteBackupTarget(ref WireWriter w, BackupTargetRecord t)
    {
        w.WriteString(t.Name);
        w.WriteStringList(t.Endpoints);
        w.WriteString(t.ExportPrincipal);
        w.WriteBytes(t.WrappedExportSecret.Span);
        w.WriteOptionalTimeSpan(t.Interval);
        w.WriteBool(t.OnEverySnapshot);
        w.WriteTimeSpan(t.MaxPinAge);
        w.WriteVarInt(t.MaxPinnedBytes);
        w.WriteVarInt(t.MaxBytesPerSecond);
        w.WriteVarInt32(t.MaxConcurrentObjects);
        w.WriteVarUInt(t.RegisteredIndex);
    }

    public static BackupTargetRecord ReadBackupTarget(ref WireReader r)
        => new(r.ReadString(), r.ReadStringList(), r.ReadString(), r.ReadBytesCopy(), r.ReadOptionalTimeSpan(), r.ReadBool(), r.ReadTimeSpan(),
            r.ReadVarInt(), r.ReadVarInt(), r.ReadVarInt32(), r.ReadVarUInt());

    public static void WriteMember(ref WireWriter w, MemberRecord m)
    {
        w.WriteNodeId(m.Id);
        w.WriteString(m.Name);
        w.WriteTopologyLabels(m.Labels);
        w.WriteStringList(m.Endpoints);
        w.WriteDefinedEnum(m.Status);
        w.WriteVarUInt(m.Incarnation);
        w.WriteUInt128(m.BootId);
        w.WriteVarInt(m.CapacityBytes);
        w.WriteVarInt(m.UsedBytes);
        w.WriteVarInt32(m.ProtocolMax);
        w.WriteStringList(m.Features);
        w.WriteVarUInt(m.JoinedIndex);
        w.WriteBool(m.PendingAdmission);
    }

    public static MemberRecord ReadMember(ref WireReader r)
        => new(r.ReadNodeId(), r.ReadString(), r.ReadTopologyLabels(), r.ReadStringList(), r.ReadEnum<WireNodeStatus>(), r.ReadVarUInt(),
            r.ReadUInt128(), r.ReadVarInt(), r.ReadVarInt(), r.ReadVarInt32(), r.ReadStringList(), r.ReadVarUInt(), r.ReadBool());

    public static void WriteVolume(ref WireWriter w, VolumeRecord v)
    {
        WriteCreation(ref w, v.Creation);
        w.WriteDefinedEnum(v.State);
        w.WriteNodeIdList(v.Voters);
        w.WriteNodeIdList(v.Learners);
        w.WriteStoragePolicy(v.DefaultPolicy);
        w.WritePolicyMap(v.NamedPolicies);
        w.WriteOptionalStoragePolicy(v.MigrationTarget);
        w.WriteCount(v.DataKeys.Count);
        foreach (DataKeyVersion k in v.DataKeys)
        {
            w.WriteVarUInt(k.Version);
            w.WriteBytes(k.WrappedKey.Span);
            w.WriteVarUInt(k.CreatedIndex);
        }

        w.WriteCount(v.BackupTargets.Count);
        foreach (BackupTargetRecord t in v.BackupTargets.Values)
            WriteBackupTarget(ref w, t);
    }

    public static VolumeRecord ReadVolume(ref WireReader r)
    {
        VolumeCreation creation = ReadCreation(ref r);
        WireVolumeState state = r.ReadEnum<WireVolumeState>();
        IReadOnlyList<NodeId> voters = r.ReadNodeIdList();
        IReadOnlyList<NodeId> learners = r.ReadNodeIdList();
        DfsStoragePolicy policy = r.ReadStoragePolicy();
        IReadOnlyDictionary<string, DfsStoragePolicy> named = r.ReadPolicyMap();
        DfsStoragePolicy? migration = r.ReadOptionalStoragePolicy();
        int keyCount = r.ReadCount(3);
        var keys = new DataKeyVersion[keyCount];
        for (int i = 0; i < keyCount; i++)
        {
            ulong version = r.ReadVarUInt();
            if (version is 0 or > uint.MaxValue)
                throw WireReader.Malformed("Data key version out of range.");
            keys[i] = new DataKeyVersion((uint)version, r.ReadBytesCopy(), r.ReadVarUInt());
        }

        int targetCount = r.ReadCount(10);
        var targets = new SortedDictionary<string, BackupTargetRecord>(StringComparer.Ordinal);
        for (int i = 0; i < targetCount; i++)
        {
            BackupTargetRecord t = ReadBackupTarget(ref r);
            targets[t.Name] = t;
        }

        return new VolumeRecord(creation, state, voters, learners, policy, named, migration, keys, targets);
    }

    public static void WritePrincipal(ref WireWriter w, PrincipalRecord p)
    {
        w.WriteString(p.Name);
        w.WriteBytes(p.Salt.Span);
        w.WriteVarInt32(p.Iterations);
        w.WriteBytes(p.StoredKey.Span);
        w.WriteBytes(p.ServerKey.Span);
        w.WriteBool(p.ClusterAdmin);
        w.WriteCount(p.Grants.Count);
        foreach (KeyValuePair<VolumeId, DfsPermissions> g in p.Grants)
        {
            w.WriteVolumeId(g.Key);
            w.WriteEnum(g.Value);
        }

        w.WriteVarUInt(p.CreatedIndex);
    }

    public static PrincipalRecord ReadPrincipal(ref WireReader r)
    {
        string name = r.ReadString();
        ReadOnlyMemory<byte> salt = r.ReadBytesCopy();
        int iterations = r.ReadVarInt32();
        ReadOnlyMemory<byte> stored = r.ReadBytesCopy();
        ReadOnlyMemory<byte> server = r.ReadBytesCopy();
        bool admin = r.ReadBool();
        int grantCount = r.ReadCount(17);
        var grants = new SortedDictionary<VolumeId, DfsPermissions>(VolumeIdComparer.Instance);
        for (int i = 0; i < grantCount; i++)
            grants[r.ReadVolumeId()] = r.ReadEnumRaw<DfsPermissions>();
        return new PrincipalRecord(name, salt, iterations, stored, server, admin, grants, r.ReadVarUInt());
    }
}
