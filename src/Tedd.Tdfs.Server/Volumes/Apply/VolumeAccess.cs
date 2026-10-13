using Tedd.Tdfs.Server.Volumes.State;

namespace Tedd.Tdfs.Server.Volumes.Apply;

/// <summary>
/// ACL evaluation over a namespace version (Addendum A, ACL-03/04/05): effective entries are computed dynamically from the
/// ancestor chain with <see cref="DfsAccessEvaluator"/> (the single production evaluator). Pure and deterministic, so the
/// leader's decision and the apply-time re-check (ACL-08) agree for the same identity set.
/// </summary>
internal static class VolumeAccess
{
    private const DfsFileSystemRights Traverse = DfsFileSystemRights.Traverse;

    /// <summary>The rights granted on the first element of <paramref name="chainNearestFirst"/>.</summary>
    public static DfsFileSystemRights Rights(IReadOnlyList<InodeRecord> chainNearestFirst, IReadOnlySet<DfsIdentity> identities, bool isAdmin)
    {
        if (chainNearestFirst.Count == 0)
            return 0;
        InodeRecord target = chainNearestFirst[0];
        var descriptors = new DfsSecurityDescriptor[chainNearestFirst.Count];
        for (int i = 0; i < descriptors.Length; i++)
            descriptors[i] = chainNearestFirst[i].Security;
        IReadOnlyList<DfsAccessRule> rules = DfsAccessEvaluator.EffectiveRules(descriptors, target.IsDirectory, target.Owner);
        return DfsAccessEvaluator.GrantedRights(rules, target.Owner, identities, isAdmin);
    }

    /// <summary>True when every bit of <paramref name="required"/> (ignoring Synchronize) is granted.</summary>
    public static bool Has(DfsFileSystemRights granted, DfsFileSystemRights required)
    {
        DfsFileSystemRights r = required & ~DfsFileSystemRights.Synchronize;
        return (granted & r) == r;
    }

    /// <summary>The effective entries of the target (explicit then inherited, with <c>InheritedFrom</c> paths).</summary>
    public static IReadOnlyList<DfsAccessRule> EffectiveRules(IReadOnlyList<InodeRecord> chainNearestFirst, string? targetPath)
    {
        InodeRecord target = chainNearestFirst[0];
        var descriptors = new DfsSecurityDescriptor[chainNearestFirst.Count];
        for (int i = 0; i < descriptors.Length; i++)
            descriptors[i] = chainNearestFirst[i].Security;
        return DfsAccessEvaluator.EffectiveRules(descriptors, target.IsDirectory, target.Owner, targetPath);
    }

    /// <summary>
    /// Traverse enforcement (ACL-05, ACL-025): with <c>EnforceTraverseChecking</c>, every existing proper ancestor directory of
    /// the path (root and parent included) must grant Traverse. Returns the first denied ancestor path, or null.
    /// </summary>
    public static bool TraverseAllowed(bool enforce, Resolution resolution, IReadOnlySet<DfsIdentity> identities, bool isAdmin)
    {
        if (!enforce)
            return true;
        IReadOnlyList<InodeRecord> ancestors = resolution.Ancestors;
        for (int i = 0; i < ancestors.Count; i++)
        {
            var chain = new List<InodeRecord>(i + 1);
            for (int j = i; j >= 0; j--)
                chain.Add(ancestors[j]);
            if (!Has(Rights(chain, identities, isAdmin), Traverse))
                return false;
        }

        return true;
    }
}
