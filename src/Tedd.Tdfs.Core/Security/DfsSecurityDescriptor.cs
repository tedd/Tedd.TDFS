using System.Text;

namespace Tedd.Tdfs;

/// <summary>
/// Security descriptor of an inode (Addendum A, ACL-02): owner, discretionary ACL and the <i>protected</i> flag that
/// blocks inheritance from the parent. Stored descriptors hold explicit entries only; descriptors returned by
/// <c>GetAccessControl</c> additionally contain the inherited entries (<see cref="DfsAccessRule.IsInherited"/>).
/// </summary>
/// <remarks>
/// Equality compares <see cref="Owner"/>, <see cref="AreAccessRulesProtected"/> and <see cref="Rules"/> element-wise
/// (order-sensitive). The rule list is copied on construction, so later changes to the caller's list have no effect.
/// </remarks>
/// <param name="Owner">Owning identity (never <see cref="DfsIdentity.CreatorOwner"/>).</param>
/// <param name="Rules">Access control entries.</param>
/// <param name="AreAccessRulesProtected">True to ignore inheritable entries of ancestors.</param>
public sealed record DfsSecurityDescriptor(DfsIdentity Owner, IReadOnlyList<DfsAccessRule> Rules, bool AreAccessRulesProtected = false)
{
    private readonly IReadOnlyList<DfsAccessRule> _rules = Copy(Rules);

    /// <summary>Access control entries (explicit entries, plus inherited entries in returned descriptors).</summary>
    public IReadOnlyList<DfsAccessRule> Rules
    {
        get => _rules;
        init => _rules = Copy(value);
    }

    /// <summary>The explicit (non-inherited) entries in order.</summary>
    public IReadOnlyList<DfsAccessRule> ExplicitRules => _rules.Where(r => !r.IsInherited).ToArray();

    /// <summary>
    /// The default root descriptor of a new volume (ACL-06): owner = <paramref name="owner"/>, ACL =
    /// <c>Everyone: FullControl (ContainerInherit | ObjectInherit)</c>, not protected.
    /// </summary>
    /// <exception cref="ArgumentException">The owner is <c>default</c> or <see cref="DfsIdentity.CreatorOwner"/>.</exception>
    public static DfsSecurityDescriptor DefaultRoot(DfsIdentity owner)
    {
        ValidateOwner(owner);
        return new DfsSecurityDescriptor(owner,
        [
            new DfsAccessRule(DfsIdentity.Everyone, DfsFileSystemRights.FullControl, DfsAccessControlType.Allow,
                DfsInheritanceFlags.ContainerInherit | DfsInheritanceFlags.ObjectInherit),
        ]);
    }

    /// <summary>
    /// The descriptor equivalent of the legacy <c>DfsInodeAccess</c> flags (API.md §H.2):
    /// <paramref name="ownerOnly"/> → protected {owner FullControl, Administrators FullControl};
    /// <paramref name="readOnlyForOthers"/> → protected {owner FullControl, Administrators FullControl, Everyone Read};
    /// neither → not protected, no explicit entries. <paramref name="ownerOnly"/> wins when both are set. The explicit
    /// entries are <c>ContainerInherit | ObjectInherit</c> so a directory's children receive the same access.
    /// </summary>
    /// <exception cref="ArgumentException">The owner is <c>default</c> or <see cref="DfsIdentity.CreatorOwner"/>.</exception>
    public static DfsSecurityDescriptor FromAccessFlags(DfsIdentity owner, bool ownerOnly, bool readOnlyForOthers)
    {
        ValidateOwner(owner);
        if (!ownerOnly && !readOnlyForOthers)
            return new DfsSecurityDescriptor(owner, []);

        const DfsInheritanceFlags both = DfsInheritanceFlags.ContainerInherit | DfsInheritanceFlags.ObjectInherit;
        var rules = new List<DfsAccessRule>(3)
        {
            new(owner, DfsFileSystemRights.FullControl, DfsAccessControlType.Allow, both),
            new(DfsIdentity.Administrators, DfsFileSystemRights.FullControl, DfsAccessControlType.Allow, both),
        };
        if (!ownerOnly)
            rules.Add(new DfsAccessRule(DfsIdentity.Everyone, DfsFileSystemRights.Read, DfsAccessControlType.Allow, both));
        return new DfsSecurityDescriptor(owner, rules, AreAccessRulesProtected: true);
    }

    /// <summary>A copy holding only the explicit entries (what a store or <c>SetAccessControl</c> keeps).</summary>
    public DfsSecurityDescriptor WithoutInheritedRules() =>
        _rules.Any(r => r.IsInherited) ? this with { Rules = ExplicitRules } : this;

    /// <summary>
    /// Validates the descriptor for storing: a valid owner that is not <see cref="DfsIdentity.CreatorOwner"/>, and every
    /// explicit entry valid per <see cref="DfsAccessRule.GetValidationError"/>. Inherited entries are ignored (they are
    /// never stored).
    /// </summary>
    /// <exception cref="ArgumentException">The descriptor is invalid; the message names the violated rule.</exception>
    public void Validate()
    {
        string? error = GetValidationError();
        if (error is not null)
            throw new ArgumentException(error, "descriptor");
    }

    /// <summary>Returns the first violated rule (see <see cref="Validate"/>), or null when valid.</summary>
    public string? GetValidationError()
    {
        string? ownerError = GetOwnerError(Owner);
        if (ownerError is not null)
            return ownerError;
        for (int i = 0; i < _rules.Count; i++)
        {
            DfsAccessRule rule = _rules[i];
            if (rule.IsInherited)
                continue;
            string? error = rule.GetValidationError();
            if (error is not null)
                return $"Rule {i}: {error}";
        }

        return null;
    }

    /// <inheritdoc />
    public bool Equals(DfsSecurityDescriptor? other) =>
        other is not null
        && Owner == other.Owner
        && AreAccessRulesProtected == other.AreAccessRulesProtected
        && _rules.SequenceEqual(other._rules);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(Owner);
        hash.Add(AreAccessRulesProtected);
        foreach (DfsAccessRule rule in _rules)
            hash.Add(rule);
        return hash.ToHashCode();
    }

    private bool PrintMembers(StringBuilder builder)
    {
        builder.Append("Owner = ").Append(Owner).Append(", AreAccessRulesProtected = ").Append(AreAccessRulesProtected)
            .Append(", Rules = [");
        for (int i = 0; i < _rules.Count; i++)
        {
            if (i > 0)
                builder.Append(", ");
            DfsAccessRule r = _rules[i];
            builder.Append(r.Type).Append(' ').Append(r.Identity).Append(' ').Append(DfsAccessRule.FormatRights(r.Rights));
            if (r.Inheritance != DfsInheritanceFlags.None)
                builder.Append(" (").Append(r.Inheritance).Append(')');
            if (r.Propagation != DfsPropagationFlags.None)
                builder.Append(" [").Append(r.Propagation).Append(']');
            if (r.IsInherited)
                builder.Append(" inherited from ").Append(r.InheritedFrom ?? "?");
        }

        builder.Append(']');
        return true;
    }

    private static IReadOnlyList<DfsAccessRule> Copy(IReadOnlyList<DfsAccessRule> rules)
    {
        ArgumentNullException.ThrowIfNull(rules, nameof(Rules));
        var copy = new DfsAccessRule[rules.Count];
        for (int i = 0; i < copy.Length; i++)
            copy[i] = rules[i] ?? throw new ArgumentException($"Rule {i} is null.", nameof(Rules));
        return Array.AsReadOnly(copy);
    }

    private static void ValidateOwner(DfsIdentity owner)
    {
        string? error = GetOwnerError(owner);
        if (error is not null)
            throw new ArgumentException(error, nameof(owner));
    }

    private static string? GetOwnerError(DfsIdentity owner)
    {
        if (owner.IsDefault)
            return "The owner must be a valid identity (default(DfsIdentity) is not valid).";
        if (owner == DfsIdentity.CreatorOwner)
            return $"'{DfsIdentity.CreatorOwnerName}' cannot own an inode; it is an inheritance template only.";
        return null;
    }
}
