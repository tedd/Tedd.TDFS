namespace Tedd.Tdfs;

/// <summary>
/// The single production implementation of NTFS-like inheritance (Addendum A, ACL-03) and access evaluation (ACL-04).
/// Pure and deterministic: no I/O, no clocks, no hash-order dependence.
/// </summary>
/// <remarks>
/// <para><b>Inheritance (dynamic, equal to a completed NTFS propagation).</b> The ACL of every object is its explicit
/// entries in canonical order (Deny before Allow, otherwise in stored order) followed by the entries it inherits from
/// its parent's ACL, in the parent's ACL order. An object whose descriptor is protected (or the root) inherits nothing.
/// For each entry of the parent's ACL:</para>
/// <list type="bullet">
/// <item>child directory, entry has <c>ContainerInherit</c>: inherited; with <c>NoPropagateInherit</c> the copy has no
/// inheritance/propagation flags (applies to the child only), otherwise it keeps its inheritance flags and loses
/// <c>InheritOnly</c>;</item>
/// <item>child directory, entry has <c>ObjectInherit</c> only: without <c>NoPropagateInherit</c> inherited as
/// <c>ObjectInherit | InheritOnly</c> (passes to the directory's files without applying to the directory); with it, not
/// inherited;</item>
/// <item>child file, entry has <c>ObjectInherit</c>: inherited with no flags; otherwise not inherited.</item>
/// </list>
/// <para>An inherited <see cref="DfsIdentity.CreatorOwner"/> entry that applies to the target becomes an entry for the
/// target's (current) owner; if it is also still inheritable, an <c>InheritOnly</c> <see cref="DfsIdentity.CreatorOwner"/>
/// template is kept after it (as NTFS does). Explicit <see cref="DfsIdentity.CreatorOwner"/> entries are never
/// substituted and never match a caller.</para>
/// <para>The resulting order is: explicit Deny, explicit Allow, then the inherited entries generation by generation —
/// all entries from the parent (its Deny before its Allow), then those from the grandparent, and so on. This is NTFS
/// canonical order: the nearest ancestor's entries, Allow and Deny alike, are evaluated before any farther ancestor's.</para>
/// <para><b>Evaluation.</b> Entries are evaluated in order; <c>InheritOnly</c> entries and entries for identities the caller
/// does not hold are skipped. Each right bit is decided by the first matching entry that mentions it. A request is granted
/// only when every requested bit is allowed (no partial grants). The owner is implicitly granted
/// <see cref="OwnerImplicitRights"/> and administrators <see cref="AdministratorImplicitRights"/>; implicit rights cannot
/// be denied by entries. <see cref="DfsFileSystemRights.Synchronize"/> is ignored in requests.</para>
/// <para><b>Identity matching.</b> <see cref="DfsIdentity.Everyone"/> matches every caller;
/// <see cref="DfsIdentity.Administrators"/> matches when the caller is an administrator; any other identity matches when
/// it is in the caller's identity set (see <see cref="CallerIdentities"/>); <see cref="DfsIdentity.CreatorOwner"/> never
/// matches. The caller is the owner when the owner identity matches the caller by the same rules (e.g. a group owner).</para>
/// </remarks>
public static class DfsAccessEvaluator
{
    /// <summary>Rights the owner always has (ACL-04).</summary>
    public const DfsFileSystemRights OwnerImplicitRights = DfsFileSystemRights.ReadPermissions | DfsFileSystemRights.ChangePermissions;

    /// <summary>Rights members of <see cref="DfsIdentity.Administrators"/> always have (ACL-04).</summary>
    public const DfsFileSystemRights AdministratorImplicitRights = DfsFileSystemRights.TakeOwnership | DfsFileSystemRights.ChangePermissions;

    private const DfsFileSystemRights AllRights = DfsFileSystemRights.FullControl;

    /// <summary>
    /// The ACL of the target after inheritance (explicit entries, then inherited entries with
    /// <see cref="DfsAccessRule.IsInherited"/> = true), in canonical evaluation order. Includes <c>InheritOnly</c>
    /// entries (as <c>GetAccessControl</c> shows them); evaluation skips those.
    /// </summary>
    /// <param name="chainNearestFirst">Descriptor of the target first, then each ancestor's descriptor towards the root
    /// (it may stop early at a protected descriptor). Inherited entries inside these descriptors are ignored.</param>
    /// <param name="targetIsDirectory">True when the target is a directory.</param>
    /// <param name="targetOwner">The target's owner (substituted for inherited <see cref="DfsIdentity.CreatorOwner"/> entries).</param>
    /// <exception cref="ArgumentException">Empty chain, null or invalid descriptor entries, or an invalid owner.</exception>
    public static IReadOnlyList<DfsAccessRule> EffectiveRules(IReadOnlyList<DfsSecurityDescriptor> chainNearestFirst, bool targetIsDirectory, DfsIdentity targetOwner)
        => EffectiveRules(chainNearestFirst, targetIsDirectory, targetOwner, targetPath: null);

    /// <summary>
    /// <see cref="EffectiveRules(IReadOnlyList{DfsSecurityDescriptor}, bool, DfsIdentity)"/>, additionally setting
    /// <see cref="DfsAccessRule.InheritedFrom"/>: element <c>i</c> of the chain belongs to the <c>i</c>-th ancestor of
    /// <paramref name="targetPath"/> (a stream path is treated as its file).
    /// </summary>
    /// <exception cref="ArgumentException">As above, or <paramref name="targetPath"/> is not normalized or has fewer
    /// ancestors than the chain has descriptors.</exception>
    public static IReadOnlyList<DfsAccessRule> EffectiveRules(IReadOnlyList<DfsSecurityDescriptor> chainNearestFirst, bool targetIsDirectory, DfsIdentity targetOwner, string? targetPath)
    {
        ArgumentNullException.ThrowIfNull(chainNearestFirst);
        if (chainNearestFirst.Count == 0)
            throw new ArgumentException("The chain must contain at least the target's descriptor.", nameof(chainNearestFirst));
        for (int i = 0; i < chainNearestFirst.Count; i++)
        {
            if (chainNearestFirst[i] is null)
                throw new ArgumentException($"Chain element {i} is null.", nameof(chainNearestFirst));
        }

        ValidateOwner(targetOwner, nameof(targetOwner));
        string[]? paths = targetPath is null ? null : AncestorPaths(targetPath, chainNearestFirst.Count);

        // The nearest protected descriptor (or the farthest supplied one) is where inheritance starts.
        int top = chainNearestFirst.Count - 1;
        for (int i = 0; i < chainNearestFirst.Count; i++)
        {
            if (chainNearestFirst[i].AreAccessRulesProtected)
            {
                top = i;
                break;
            }
        }

        var result = new List<DfsAccessRule>();
        foreach (Entry e in ExplicitEntries(chainNearestFirst[0], 0))
            result.Add(e.ToRule(isInherited: false, inheritedFrom: null));
        if (top == 0)
            return result.AsReadOnly();

        // Propagate from the top ancestor down to the parent (all directories), then into the target.
        List<Entry> acl = ExplicitEntries(chainNearestFirst[top], top);
        for (int i = top - 1; i >= 1; i--)
        {
            List<Entry> next = ExplicitEntries(chainNearestFirst[i], i);
            Inherit(acl, childIsDirectory: true, next);
            acl = next;
        }

        var inherited = new List<Entry>();
        Inherit(acl, targetIsDirectory, inherited);
        foreach (Entry e in inherited)
        {
            string? from = paths?[e.Source];
            if (e.Identity == DfsIdentity.CreatorOwner && e.AppliesToObject)
            {
                // NTFS splits an applicable CREATOR OWNER entry into an entry for the owner and, if still inheritable,
                // an inherit-only template for further descendants.
                result.Add((e with { Identity = targetOwner, Inheritance = DfsInheritanceFlags.None, Propagation = DfsPropagationFlags.None })
                    .ToRule(isInherited: true, from));
                if (e.Inheritance != DfsInheritanceFlags.None)
                    result.Add((e with { Propagation = DfsPropagationFlags.InheritOnly }).ToRule(isInherited: true, from));
            }
            else
            {
                result.Add(e.ToRule(isInherited: true, from));
            }
        }

        return result.AsReadOnly();
    }

    /// <summary>
    /// The rights granted to the caller: implicit owner/administrator rights plus every bit whose first matching
    /// applicable entry is an Allow. <see cref="DfsFileSystemRights.Synchronize"/> is reported as the entries decide it.
    /// For every request <c>r</c>: <c>IsGranted(r)</c> ⇔ <c>(GrantedRights &amp; r') == r'</c> with
    /// <c>r' = r &amp; ~Synchronize</c>.
    /// </summary>
    /// <param name="effectiveRules">Entries in evaluation order (the result of <c>EffectiveRules</c>).</param>
    /// <param name="owner">The target's owner.</param>
    /// <param name="callerIdentities">The caller's resolved identity set (see <see cref="CallerIdentities"/>).</param>
    /// <param name="callerIsAdministrator">True when the caller has ClusterAdmin (also implied when the set contains
    /// <see cref="DfsIdentity.Administrators"/>).</param>
    public static DfsFileSystemRights GrantedRights(IReadOnlyList<DfsAccessRule> effectiveRules, DfsIdentity owner,
        IReadOnlySet<DfsIdentity> callerIdentities, bool callerIsAdministrator)
    {
        ArgumentNullException.ThrowIfNull(effectiveRules);
        ArgumentNullException.ThrowIfNull(callerIdentities);
        ValidateOwner(owner, nameof(owner));
        bool isAdmin = callerIsAdministrator || callerIdentities.Contains(DfsIdentity.Administrators);

        DfsFileSystemRights granted = ImplicitRights(owner, callerIdentities, isAdmin);
        DfsFileSystemRights decided = granted;
        foreach (DfsAccessRule rule in effectiveRules)
        {
            if (rule is null || !rule.AppliesToObject || !Matches(rule.Identity, callerIdentities, isAdmin))
                continue;
            DfsFileSystemRights bits = rule.Rights & AllRights & ~decided;
            if (bits == 0)
                continue;
            if (rule.Type == DfsAccessControlType.Allow)
                granted |= bits;
            decided |= bits;
        }

        return granted;
    }

    /// <summary>
    /// True when every requested right (ignoring <see cref="DfsFileSystemRights.Synchronize"/>) is granted: walking the
    /// entries in order, the request is denied as soon as a matching applicable Deny names a right not yet allowed, and
    /// granted once all requested rights are allowed. Rights never allowed are denied.
    /// </summary>
    /// <exception cref="ArgumentException"><paramref name="requested"/> has undefined bits or requests nothing but
    /// <see cref="DfsFileSystemRights.Synchronize"/>.</exception>
    public static bool IsGranted(IReadOnlyList<DfsAccessRule> effectiveRules, DfsIdentity owner, IReadOnlySet<DfsIdentity> callerIdentities,
        bool callerIsAdministrator, DfsFileSystemRights requested)
    {
        ArgumentNullException.ThrowIfNull(effectiveRules);
        ArgumentNullException.ThrowIfNull(callerIdentities);
        ValidateOwner(owner, nameof(owner));
        if ((requested & ~AllRights) != 0)
            throw new ArgumentException($"The request uses undefined rights bits 0x{(int)(requested & ~AllRights):X}.", nameof(requested));
        DfsFileSystemRights remaining = requested & ~DfsFileSystemRights.Synchronize;
        if (remaining == 0)
            throw new ArgumentException("The request must name at least one right other than Synchronize.", nameof(requested));

        bool isAdmin = callerIsAdministrator || callerIdentities.Contains(DfsIdentity.Administrators);
        remaining &= ~ImplicitRights(owner, callerIdentities, isAdmin);
        foreach (DfsAccessRule rule in effectiveRules)
        {
            if (remaining == 0)
                break;
            if (rule is null || !rule.AppliesToObject || !Matches(rule.Identity, callerIdentities, isAdmin))
                continue;
            if ((rule.Rights & remaining) == 0)
                continue;
            if (rule.Type != DfsAccessControlType.Allow)
                return false;
            remaining &= ~rule.Rights;
        }

        return remaining == 0;
    }

    /// <summary>
    /// The identity set of a caller as the deciding leader resolves it (ARCHITECTURE §17): <c>user:principal</c>, each
    /// group, <see cref="DfsIdentity.Everyone"/>, <see cref="DfsIdentity.AuthenticatedUsers"/>, and
    /// <see cref="DfsIdentity.Administrators"/> when <paramref name="isClusterAdmin"/>.
    /// </summary>
    /// <exception cref="ArgumentException">An invalid principal or group name.</exception>
    public static IReadOnlySet<DfsIdentity> CallerIdentities(string principal, IEnumerable<string> groups, bool isClusterAdmin)
    {
        ArgumentNullException.ThrowIfNull(principal);
        ArgumentNullException.ThrowIfNull(groups);
        var set = new HashSet<DfsIdentity> { DfsIdentity.User(principal), DfsIdentity.Everyone, DfsIdentity.AuthenticatedUsers };
        foreach (string group in groups)
            set.Add(DfsIdentity.Group(group));
        if (isClusterAdmin)
            set.Add(DfsIdentity.Administrators);
        return set;
    }

    private static DfsFileSystemRights ImplicitRights(DfsIdentity owner, IReadOnlySet<DfsIdentity> callerIdentities, bool isAdmin)
    {
        DfsFileSystemRights rights = 0;
        if (Matches(owner, callerIdentities, isAdmin))
            rights |= OwnerImplicitRights;
        if (isAdmin)
            rights |= AdministratorImplicitRights;
        return rights;
    }

    private static bool Matches(DfsIdentity identity, IReadOnlySet<DfsIdentity> callerIdentities, bool isAdmin)
    {
        if (identity.IsDefault || identity == DfsIdentity.CreatorOwner)
            return false;
        if (identity == DfsIdentity.Everyone)
            return true;
        if (identity == DfsIdentity.Administrators && isAdmin)
            return true;
        return callerIdentities.Contains(identity);
    }

    private static void ValidateOwner(DfsIdentity owner, string paramName)
    {
        if (owner.IsDefault)
            throw new ArgumentException("The owner must be a valid identity (default(DfsIdentity) is not valid).", paramName);
        if (owner == DfsIdentity.CreatorOwner)
            throw new ArgumentException($"'{DfsIdentity.CreatorOwnerName}' cannot own an inode.", paramName);
    }

    // Explicit entries of one descriptor in canonical order: Deny entries, then Allow entries (each in stored order).
    private static List<Entry> ExplicitEntries(DfsSecurityDescriptor descriptor, int source)
    {
        var entries = new List<Entry>(descriptor.Rules.Count);
        for (int pass = 0; pass < 2; pass++)
        {
            bool wantDeny = pass == 0;
            for (int i = 0; i < descriptor.Rules.Count; i++)
            {
                DfsAccessRule rule = descriptor.Rules[i];
                if (rule.IsInherited)
                    continue;
                if (pass == 0)
                {
                    string? error = rule.GetValidationError();
                    if (error is not null)
                        throw new ArgumentException($"Chain element {source}, rule {i}: {error}", "chainNearestFirst");
                }

                if ((rule.Type == DfsAccessControlType.Deny) == wantDeny)
                    entries.Add(new Entry(rule.Identity, rule.Rights, rule.Type, rule.Inheritance, rule.Propagation, source));
            }
        }

        return entries;
    }

    // Appends to 'child' the entries a child of the given kind inherits from 'parentAcl', in parentAcl order.
    private static void Inherit(List<Entry> parentAcl, bool childIsDirectory, List<Entry> child)
    {
        foreach (Entry e in parentAcl)
        {
            bool containerInherit = (e.Inheritance & DfsInheritanceFlags.ContainerInherit) != 0;
            bool objectInherit = (e.Inheritance & DfsInheritanceFlags.ObjectInherit) != 0;
            bool noPropagate = (e.Propagation & DfsPropagationFlags.NoPropagateInherit) != 0;
            if (childIsDirectory)
            {
                if (containerInherit)
                {
                    child.Add(noPropagate
                        ? e with { Inheritance = DfsInheritanceFlags.None, Propagation = DfsPropagationFlags.None }
                        : e with { Propagation = DfsPropagationFlags.None });
                }
                else if (objectInherit && !noPropagate)
                {
                    child.Add(e with { Inheritance = DfsInheritanceFlags.ObjectInherit, Propagation = DfsPropagationFlags.InheritOnly });
                }
            }
            else if (objectInherit)
            {
                child.Add(e with { Inheritance = DfsInheritanceFlags.None, Propagation = DfsPropagationFlags.None });
            }
        }
    }

    private static string[] AncestorPaths(string targetPath, int count)
    {
        if (!DfsPath.IsNormalized(targetPath))
            throw new ArgumentException("The target path must be a normalized path (DfsPath.Normalize).", nameof(targetPath));
        var paths = new string[count];
        paths[0] = DfsStreamPath.GetFilePath(targetPath);
        for (int i = 1; i < count; i++)
        {
            if (DfsPath.IsRoot(paths[i - 1]))
                throw new ArgumentException($"The chain has {count} descriptors but '{targetPath}' has only {i - 1} ancestors.", nameof(targetPath));
            paths[i] = DfsPath.GetParent(paths[i - 1]);
        }

        return paths;
    }

    private readonly record struct Entry(DfsIdentity Identity, DfsFileSystemRights Rights, DfsAccessControlType Type,
        DfsInheritanceFlags Inheritance, DfsPropagationFlags Propagation, int Source)
    {
        public bool AppliesToObject => (Propagation & DfsPropagationFlags.InheritOnly) == 0;

        public DfsAccessRule ToRule(bool isInherited, string? inheritedFrom) =>
            new(Identity, Rights, Type, Inheritance, Propagation) { IsInherited = isInherited, InheritedFrom = inheritedFrom };
    }
}
