namespace Tedd.Tdfs.Tests.Unit.Internal.Security;

/// <summary>Builders shared by the security unit tests.</summary>
internal static class SecurityTestHelpers
{
    public const DfsInheritanceFlags CI = DfsInheritanceFlags.ContainerInherit;
    public const DfsInheritanceFlags OI = DfsInheritanceFlags.ObjectInherit;
    public const DfsInheritanceFlags CIOI = DfsInheritanceFlags.ContainerInherit | DfsInheritanceFlags.ObjectInherit;
    public const DfsPropagationFlags NP = DfsPropagationFlags.NoPropagateInherit;
    public const DfsPropagationFlags IO = DfsPropagationFlags.InheritOnly;

    public static readonly DfsIdentity Alice = DfsIdentity.User("alice");
    public static readonly DfsIdentity Bob = DfsIdentity.User("bob");
    public static readonly DfsIdentity Carol = DfsIdentity.User("carol");
    public static readonly DfsIdentity Admin = DfsIdentity.User("admin");
    public static readonly DfsIdentity Eng = DfsIdentity.Group("eng");

    public static DfsAccessRule Allow(DfsIdentity id, DfsFileSystemRights rights,
        DfsInheritanceFlags inheritance = DfsInheritanceFlags.None, DfsPropagationFlags propagation = DfsPropagationFlags.None) =>
        new(id, rights, DfsAccessControlType.Allow, inheritance, propagation);

    public static DfsAccessRule Deny(DfsIdentity id, DfsFileSystemRights rights,
        DfsInheritanceFlags inheritance = DfsInheritanceFlags.None, DfsPropagationFlags propagation = DfsPropagationFlags.None) =>
        new(id, rights, DfsAccessControlType.Deny, inheritance, propagation);

    public static DfsSecurityDescriptor Sd(DfsIdentity owner, params DfsAccessRule[] rules) => new(owner, rules);

    public static DfsSecurityDescriptor Protected(DfsIdentity owner, params DfsAccessRule[] rules) => new(owner, rules, AreAccessRulesProtected: true);

    /// <summary>Caller identity set of a non-admin user with groups.</summary>
    public static IReadOnlySet<DfsIdentity> Caller(string user, params string[] groups) =>
        DfsAccessEvaluator.CallerIdentities(user, groups, isClusterAdmin: false);

    /// <summary>Rights granted to <paramref name="caller"/> on the target of <paramref name="chain"/>.</summary>
    public static DfsFileSystemRights Granted(DfsSecurityDescriptor[] chain, bool isDirectory, IReadOnlySet<DfsIdentity> caller, bool isAdmin = false)
    {
        IReadOnlyList<DfsAccessRule> rules = DfsAccessEvaluator.EffectiveRules(chain, isDirectory, chain[0].Owner);
        return DfsAccessEvaluator.GrantedRights(rules, chain[0].Owner, caller, isAdmin);
    }

    /// <summary>Whether <paramref name="requested"/> is granted to <paramref name="caller"/> on the target of <paramref name="chain"/>.</summary>
    public static bool Can(DfsSecurityDescriptor[] chain, bool isDirectory, IReadOnlySet<DfsIdentity> caller, DfsFileSystemRights requested, bool isAdmin = false)
    {
        IReadOnlyList<DfsAccessRule> rules = DfsAccessEvaluator.EffectiveRules(chain, isDirectory, chain[0].Owner);
        bool granted = DfsAccessEvaluator.IsGranted(rules, chain[0].Owner, caller, isAdmin, requested);
        // Invariant (ACL-034): IsGranted agrees with GrantedRights for every request.
        DfsFileSystemRights all = DfsAccessEvaluator.GrantedRights(rules, chain[0].Owner, caller, isAdmin);
        DfsFileSystemRights wanted = requested & ~DfsFileSystemRights.Synchronize;
        Assert.Equal((all & wanted) == wanted, granted);
        return granted;
    }
}
