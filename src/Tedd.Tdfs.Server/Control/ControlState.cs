using System.Buffers;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;

namespace Tedd.Tdfs.Server.Control;

/// <summary>
/// Replicated cluster configuration held by the control group (ARCHITECTURE §2.4, Addendum A): identity, members, placement
/// epoch, volumes, principals and grants, groups, pools, data-key versions, backup targets, features and a bounded audit
/// log. Every collection is ordinal/ordered so iteration, checkpoints and digests are deterministic. Mutated only by
/// <see cref="ControlStateMachine"/>; read on the node loop.
/// </summary>
internal sealed partial class ControlState
{
    /// <summary>Audit entries retained (most recent).</summary>
    public const int MaxAuditEntries = 2048;

    /// <summary>Administrative request outcomes retained for deduplication.</summary>
    public const int MaxAdminOutcomes = 2048;

    private const byte CheckpointFormat = 1;

    public Guid ClusterId { get; set; }

    public string ClusterName { get; set; } = string.Empty;

    /// <summary>Number of control voters the membership manager maintains.</summary>
    public int ControlVoterTarget { get; set; } = 3;

    /// <summary>Versioned placement map epoch (bumped by membership, label and pool changes).</summary>
    public ulong PlacementEpoch { get; set; }

    /// <summary>Index of the last applied control entry (maintained by the state machine).</summary>
    public ulong AppliedIndex { get; set; }

    public SortedDictionary<NodeId, MemberRecord> Members { get; } = new(NodeIdComparer.Instance);

    public SortedDictionary<VolumeId, VolumeRecord> Volumes { get; } = new(VolumeIdComparer.Instance);

    public SortedDictionary<string, PrincipalRecord> Principals { get; } = new(StringComparer.Ordinal);

    public SortedDictionary<string, GroupRecord> Groups { get; } = new(StringComparer.Ordinal);

    public SortedDictionary<string, PoolRecord> Pools { get; } = new(StringComparer.Ordinal);

    public SortedSet<string> Features { get; } = new(StringComparer.Ordinal);

    /// <summary>Audit log, oldest first.</summary>
    public List<AuditEntry> Audit { get; } = [];

    public SortedDictionary<UInt128, AdminOutcome> AdminOutcomes { get; } = new();

    /// <summary>True once the bootstrap command was applied.</summary>
    public bool IsBootstrapped => ClusterId != Guid.Empty;

    public MemberRecord? FindMember(NodeId id) => Members.GetValueOrDefault(id);

    /// <summary>The live member with <paramref name="name"/> (removed members are ignored unless no live one exists).</summary>
    public MemberRecord? FindMemberByName(string name)
    {
        MemberRecord? removed = null;
        foreach (MemberRecord m in Members.Values)
        {
            if (!string.Equals(m.Name, name, StringComparison.Ordinal))
                continue;
            if (m.IsLive)
                return m;
            removed ??= m;
        }

        return removed;
    }

    public VolumeRecord? FindVolume(VolumeId id) => Volumes.GetValueOrDefault(id);

    public VolumeRecord? FindVolumeByName(string name)
    {
        foreach (VolumeRecord v in Volumes.Values)
        {
            if (string.Equals(v.Name, name, StringComparison.Ordinal))
                return v;
        }

        return null;
    }

    public PrincipalRecord? FindPrincipal(string name) => Principals.GetValueOrDefault(name);

    /// <summary>Volume grants of <paramref name="principal"/> on <paramref name="volume"/> (none for unknown principals).</summary>
    public DfsPermissions GetPermissions(string principal, VolumeId volume)
        => Principals.TryGetValue(principal, out PrincipalRecord? p) && p.Grants.TryGetValue(volume, out DfsPermissions perms) ? perms : DfsPermissions.None;

    /// <summary>True when the principal exists and is a cluster administrator.</summary>
    public bool IsClusterAdmin(string principal) => Principals.TryGetValue(principal, out PrincipalRecord? p) && p.ClusterAdmin;

    /// <summary>Groups containing <paramref name="principal"/> (ordinal order).</summary>
    public IReadOnlyList<string> GroupsOf(string principal)
    {
        var result = new List<string>();
        foreach (GroupRecord g in Groups.Values)
        {
            if (g.Members.Contains(principal, StringComparer.Ordinal))
                result.Add(g.Name);
        }

        return result;
    }

    /// <summary>True for <c>default</c> and every explicit pool.</summary>
    public bool PoolExists(string pool) => string.Equals(pool, DfsPoolNames.Default, StringComparison.Ordinal) || Pools.ContainsKey(pool);

    /// <summary>Live members of <paramref name="pool"/> in id order (<c>default</c>: every live member).</summary>
    public IReadOnlyList<MemberRecord> PoolMembers(string pool)
    {
        var result = new List<MemberRecord>();
        if (string.Equals(pool, DfsPoolNames.Default, StringComparison.Ordinal))
        {
            foreach (MemberRecord m in Members.Values)
            {
                if (m.IsLive)
                    result.Add(m);
            }

            return result;
        }

        if (Pools.TryGetValue(pool, out PoolRecord? record))
        {
            foreach (NodeId id in record.Nodes)
            {
                if (Members.TryGetValue(id, out MemberRecord? m) && m.IsLive)
                    result.Add(m);
            }
        }

        return result;
    }

    /// <summary>Appends an audit entry, keeping the most recent <see cref="MaxAuditEntries"/>.</summary>
    public void AppendAudit(AuditEntry entry)
    {
        Audit.Add(entry);
        if (Audit.Count > MaxAuditEntries)
            Audit.RemoveRange(0, Audit.Count - MaxAuditEntries);
    }

    /// <summary>Records an administrative outcome, keeping the most recent <see cref="MaxAdminOutcomes"/> (lowest index evicted).</summary>
    public void RecordAdminOutcome(AdminOutcome outcome)
    {
        AdminOutcomes[outcome.RequestId] = outcome;
        if (AdminOutcomes.Count <= MaxAdminOutcomes)
            return;
        UInt128 oldest = default;
        ulong oldestIndex = ulong.MaxValue;
        foreach (KeyValuePair<UInt128, AdminOutcome> pair in AdminOutcomes)
        {
            if (pair.Value.Index < oldestIndex)
            {
                oldestIndex = pair.Value.Index;
                oldest = pair.Key;
            }
        }

        AdminOutcomes.Remove(oldest);
    }

    /// <summary>Removes every piece of state (before restoring a checkpoint).</summary>
    public void Clear()
    {
        ClusterId = Guid.Empty;
        ClusterName = string.Empty;
        ControlVoterTarget = 3;
        PlacementEpoch = 0;
        AppliedIndex = 0;
        Members.Clear();
        Volumes.Clear();
        Principals.Clear();
        Groups.Clear();
        Pools.Clear();
        Features.Clear();
        Audit.Clear();
        AdminOutcomes.Clear();
        PoolEpochs.Clear();
    }

    /// <summary>Canonical serialization (checkpoints and digests): identical states produce identical bytes.</summary>
    public void Write(IBufferWriter<byte> output)
    {
        var w = new WireWriter(output);
        w.WriteByte(CheckpointFormat);
        w.WriteGuid(ClusterId);
        w.WriteString(ClusterName);
        w.WriteVarInt32(ControlVoterTarget);
        w.WriteVarUInt(PlacementEpoch);
        w.WriteVarUInt(AppliedIndex);
        w.WriteCount(Members.Count);
        foreach (MemberRecord m in Members.Values)
            ControlCodec.WriteMember(ref w, m);
        w.WriteCount(Volumes.Count);
        foreach (VolumeRecord v in Volumes.Values)
            ControlCodec.WriteVolume(ref w, v);
        w.WriteCount(Principals.Count);
        foreach (PrincipalRecord p in Principals.Values)
            ControlCodec.WritePrincipal(ref w, p);
        w.WriteCount(Groups.Count);
        foreach (GroupRecord g in Groups.Values)
        {
            w.WriteString(g.Name);
            w.WriteStringList(g.Members);
            w.WriteVarUInt(g.CreatedIndex);
        }

        w.WriteCount(Pools.Count);
        foreach (PoolRecord p in Pools.Values)
        {
            w.WriteString(p.Name);
            w.WriteNodeIdList(p.Nodes);
            w.WriteVarUInt(p.CreatedIndex);
        }

        w.WriteStringSet(Features);
        w.WriteCount(Audit.Count);
        foreach (AuditEntry a in Audit)
        {
            w.WriteVarUInt(a.Index);
            w.WriteDateTimeOffset(a.Time);
            w.WriteString(a.Principal);
            w.WriteString(a.Operation);
            w.WriteString(a.Outcome);
            w.WriteString(a.Scope);
        }

        w.WriteCount(AdminOutcomes.Count);
        foreach (AdminOutcome o in AdminOutcomes.Values)
        {
            w.WriteUInt128(o.RequestId);
            w.WriteUInt16((ushort)o.Kind);
            w.WriteVarInt((int)o.Code);
            w.WriteString(o.Message);
            w.WriteGuid(o.RefGuid);
            w.WriteVarUInt(o.RefNumber);
            w.WriteVarUInt(o.Index);
        }

        w.WriteExtensionBlock(WriteExtensions());
    }

    /// <summary>Replaces this state with a serialized one.</summary>
    /// <exception cref="DfsIntegrityException">The data is malformed.</exception>
    public void Read(ReadOnlySpan<byte> data)
    {
        Clear();
        try
        {
            var r = new WireReader(data, new WireLimits(MaxBytesLength: int.MaxValue));
            if (r.ReadByte() != CheckpointFormat)
                throw new DfsIntegrityException("Unsupported control checkpoint format.");
            ClusterId = r.ReadGuid();
            ClusterName = r.ReadString();
            ControlVoterTarget = r.ReadVarInt32();
            PlacementEpoch = r.ReadVarUInt();
            AppliedIndex = r.ReadVarUInt();
            int members = r.ReadCount(20);
            for (int i = 0; i < members; i++)
            {
                MemberRecord m = ControlCodec.ReadMember(ref r);
                Members[m.Id] = m;
            }

            int volumes = r.ReadCount(20);
            for (int i = 0; i < volumes; i++)
            {
                VolumeRecord v = ControlCodec.ReadVolume(ref r);
                Volumes[v.Id] = v;
            }

            int principals = r.ReadCount(6);
            for (int i = 0; i < principals; i++)
            {
                PrincipalRecord p = ControlCodec.ReadPrincipal(ref r);
                Principals[p.Name] = p;
            }

            int groups = r.ReadCount(3);
            for (int i = 0; i < groups; i++)
            {
                var g = new GroupRecord(r.ReadString(), r.ReadStringList(), r.ReadVarUInt());
                Groups[g.Name] = g;
            }

            int pools = r.ReadCount(3);
            for (int i = 0; i < pools; i++)
            {
                var p = new PoolRecord(r.ReadString(), r.ReadNodeIdList(), r.ReadVarUInt());
                Pools[p.Name] = p;
            }

            foreach (string f in r.ReadStringSet())
                Features.Add(f);
            int audit = r.ReadCount(6);
            for (int i = 0; i < audit; i++)
                Audit.Add(new AuditEntry(r.ReadVarUInt(), r.ReadDateTimeOffset(), r.ReadString(), r.ReadString(), r.ReadString(), r.ReadString()));
            int outcomes = r.ReadCount(20);
            for (int i = 0; i < outcomes; i++)
            {
                var o = new AdminOutcome(r.ReadUInt128(), (ControlCommandKind)r.ReadUInt16(), (DfsErrorCode)(int)r.ReadVarInt(), r.ReadString(),
                    r.ReadGuid(), r.ReadVarUInt(), r.ReadVarUInt());
                AdminOutcomes[o.RequestId] = o;
            }

            r.ReadExtensions(UnderstoodExtensions, ReadExtension);
            r.EnsureEnd();
        }
        catch (DfsProtocolException ex)
        {
            Clear();
            throw new DfsIntegrityException("Malformed control checkpoint: " + ex.Message, ex);
        }
    }
}
