namespace Tedd.Tdfs.Protocol;

/// <summary>
/// Wire encoding of Addendum A security types (PROTOCOL.md §2.3 <c>SecurityDescriptor</c>, <c>AccessRule</c>).
/// </summary>
/// <remarks>
/// <para><c>Identity</c> = <c>string</c> text form (<see cref="DfsIdentity.ToString"/>: <c>user:x</c>, <c>group:x</c> or a
/// well-known name); decoders reject text that does not parse.</para>
/// <para><c>AccessRule</c> = <c>Identity</c>, <c>varuint</c> rights (raw bits; receivers validate), <c>enum
/// DfsAccessControlType (raw)</c>, <c>enum DfsInheritanceFlags (raw)</c>, <c>enum DfsPropagationFlags (raw)</c>,
/// <c>bool</c> IsInherited, <c>string?</c> InheritedFrom.</para>
/// <para><c>SecurityDescriptor</c> = <c>Identity</c> owner, <c>bool</c> AreAccessRulesProtected, <c>list&lt;AccessRule&gt;</c>.
/// Values are not validated by the codec (receivers answer <c>InvalidArgument</c>).</para>
/// </remarks>
public static class WireSecurityCodecExtensions
{
    /// <summary>Smallest possible encoding of an access rule (empty identity text is rejected, so at least 2 bytes of it).</summary>
    private const int MinRuleSize = 2 + 1 + 1 + 1 + 1 + 1 + 1;

    /// <summary>Writes an identity in its text form.</summary>
    public static void WriteIdentity(this ref WireWriter writer, DfsIdentity value) => writer.WriteString(value.ToString());

    /// <summary>Reads an identity; malformed text is a protocol error.</summary>
    public static DfsIdentity ReadIdentity(this ref WireReader reader)
    {
        string text = reader.ReadString();
        if (!DfsIdentity.TryParse(text, out DfsIdentity identity))
            throw WireReader.Malformed("Invalid identity text on the wire.");
        return identity;
    }

    /// <summary>Writes an access rule.</summary>
    public static void WriteAccessRule(this ref WireWriter writer, DfsAccessRule value)
    {
        ArgumentNullException.ThrowIfNull(value);
        writer.WriteIdentity(value.Identity);
        writer.WriteVarUInt(unchecked((uint)value.Rights));
        writer.WriteEnum(value.Type);
        writer.WriteEnum(value.Inheritance);
        writer.WriteEnum(value.Propagation);
        writer.WriteBool(value.IsInherited);
        writer.WriteOptionalString(value.InheritedFrom);
    }

    /// <summary>Reads an access rule (not validated).</summary>
    public static DfsAccessRule ReadAccessRule(this ref WireReader reader)
    {
        DfsIdentity identity = reader.ReadIdentity();
        ulong rights = reader.ReadVarUInt();
        if (rights > uint.MaxValue)
            throw WireReader.Malformed("Access rights do not fit 32 bits.");
        DfsAccessControlType type = reader.ReadEnumRaw<DfsAccessControlType>();
        DfsInheritanceFlags inheritance = reader.ReadEnumRaw<DfsInheritanceFlags>();
        DfsPropagationFlags propagation = reader.ReadEnumRaw<DfsPropagationFlags>();
        bool inherited = reader.ReadBool();
        string? from = reader.ReadOptionalString();
        return new DfsAccessRule(identity, unchecked((DfsFileSystemRights)(uint)rights), type, inheritance, propagation)
        {
            IsInherited = inherited,
            InheritedFrom = from,
        };
    }

    /// <summary>Writes a security descriptor.</summary>
    public static void WriteSecurityDescriptor(this ref WireWriter writer, DfsSecurityDescriptor value)
    {
        ArgumentNullException.ThrowIfNull(value);
        writer.WriteIdentity(value.Owner);
        writer.WriteBool(value.AreAccessRulesProtected);
        IReadOnlyList<DfsAccessRule> rules = value.Rules;
        writer.WriteCount(rules.Count);
        foreach (DfsAccessRule rule in rules)
            writer.WriteAccessRule(rule);
    }

    /// <summary>Reads a security descriptor (not validated).</summary>
    public static DfsSecurityDescriptor ReadSecurityDescriptor(this ref WireReader reader)
    {
        DfsIdentity owner = reader.ReadIdentity();
        bool isProtected = reader.ReadBool();
        int count = reader.ReadCount(MinRuleSize);
        var rules = new DfsAccessRule[count];
        for (int i = 0; i < count; i++)
            rules[i] = reader.ReadAccessRule();
        return new DfsSecurityDescriptor(owner, rules, isProtected);
    }

    /// <summary>Writes an optional security descriptor (presence flag + value).</summary>
    public static void WriteOptionalSecurityDescriptor(this ref WireWriter writer, DfsSecurityDescriptor? value)
    {
        writer.WriteBool(value is not null);
        if (value is not null)
            writer.WriteSecurityDescriptor(value);
    }

    /// <summary>Reads an optional security descriptor.</summary>
    public static DfsSecurityDescriptor? ReadOptionalSecurityDescriptor(this ref WireReader reader)
        => reader.ReadBool() ? reader.ReadSecurityDescriptor() : null;
}
