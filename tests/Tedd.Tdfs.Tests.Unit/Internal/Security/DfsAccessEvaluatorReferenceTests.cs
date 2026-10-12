using R = Tedd.Tdfs.DfsFileSystemRights;

namespace Tedd.Tdfs.Tests.Unit.Internal.Security;

/// <summary>
/// Randomized cross-check of <see cref="DfsAccessEvaluator"/> against an independent reference model. The production
/// code simulates NTFS propagation level by level; the reference instead decides directly, per explicit entry, whether
/// it reaches the target (by the entry's distance, flags and the target kind, as documented for NTFS ACE flags), orders
/// entries by generation, and evaluates every right bit separately ("the first matching entry naming the bit decides").
/// </summary>
public sealed class DfsAccessEvaluatorReferenceTests
{
    private static readonly DfsIdentity[] Principals =
    [
        DfsIdentity.User("a"), DfsIdentity.User("b"), DfsIdentity.Group("g"), DfsIdentity.Group("h"),
        DfsIdentity.Everyone, DfsIdentity.AuthenticatedUsers, DfsIdentity.Administrators, DfsIdentity.CreatorOwner,
    ];

    private static readonly DfsIdentity[] Owners =
    [
        DfsIdentity.User("a"), DfsIdentity.User("b"), DfsIdentity.User("c"), DfsIdentity.Group("g"), DfsIdentity.Administrators,
    ];

    // A few bits only, so entries overlap and order matters.
    private static readonly R[] RightBits = [R.ReadData, R.WriteData, R.Delete, R.ReadPermissions, R.ChangePermissions, R.TakeOwnership, R.Synchronize];

    private static readonly R[] AllSingleBits = Enumerable.Range(0, 31)
        .Select(i => (R)(1 << i)).Where(b => (b & R.FullControl) != 0).ToArray();

    private sealed record RefEntry(DfsIdentity Identity, R Rights, DfsAccessControlType Type, bool IsInherited, string? From);

    [Fact]
    public void EvaluatorMatchesIndependentReferenceOnRandomTrees()
    {
        var random = new Random(0x7DF5AC10);
        int checkedRequests = 0;
        int granted = 0;
        for (int iteration = 0; iteration < 6000; iteration++)
        {
            int depth = random.Next(0, 5);                     // number of ancestors of the target
            bool targetIsDirectory = depth == 0 || random.Next(2) == 0;
            var chain = new DfsSecurityDescriptor[depth + 1];
            for (int i = 0; i <= depth; i++)
                chain[i] = RandomDescriptor(random);
            string path = depth == 0 ? "/" : "/" + string.Join("/", Enumerable.Range(0, depth).Select(i => "n" + i));
            DfsIdentity owner = chain[0].Owner;

            IReadOnlyList<DfsAccessRule> effective = DfsAccessEvaluator.EffectiveRules(chain, targetIsDirectory, owner, path);
            List<RefEntry> reference = ReferenceApplicable(chain, targetIsDirectory, owner, path);

            // 1. The applicable entries (what evaluation sees), in order, are identical.
            RefEntry[] actualApplicable = effective.Where(r => r.AppliesToObject)
                .Select(r => new RefEntry(r.Identity, r.Rights, r.Type, r.IsInherited, r.InheritedFrom)).ToArray();
            Assert.Equal(reference, actualApplicable);

            // 2. Explicit entries come first and are exactly the target's own (deny before allow).
            Assert.Equal(chain[0].Rules.Count, effective.TakeWhile(r => !r.IsInherited).Count());
            Assert.All(effective.Skip(chain[0].Rules.Count), r => Assert.True(r.IsInherited));

            // 3. Decisions match the per-bit reference for several callers and requests.
            for (int c = 0; c < 4; c++)
            {
                (IReadOnlySet<DfsIdentity> caller, bool isAdmin) = RandomCaller(random);
                R expected = ReferenceGranted(reference, owner, caller, isAdmin);
                R actual = DfsAccessEvaluator.GrantedRights(effective, owner, caller, isAdmin);
                Assert.True(expected == actual,
                    $"iteration {iteration}: granted 0x{(int)actual:X} but reference says 0x{(int)expected:X} for {Describe(caller, isAdmin)} on {path} ({(targetIsDirectory ? "dir" : "file")})");

                for (int q = 0; q < 6; q++)
                {
                    R request = RandomRequest(random);
                    R wanted = request & ~R.Synchronize;
                    bool expectedDecision = (expected & wanted) == wanted;
                    bool decision = DfsAccessEvaluator.IsGranted(effective, owner, caller, isAdmin, request);
                    Assert.True(expectedDecision == decision,
                        $"iteration {iteration}: request 0x{(int)request:X} decided {decision}, reference {expectedDecision}");
                    checkedRequests++;
                    if (decision)
                        granted++;
                }
            }
        }

        // The random space must exercise both outcomes substantially.
        Assert.True(granted > checkedRequests / 20, $"only {granted} of {checkedRequests} requests were granted");
        Assert.True(granted < checkedRequests - (checkedRequests / 20), $"{granted} of {checkedRequests} requests were granted");
    }

    [Fact]
    public void IsGrantedEqualsPerBitDecisionForEveryRequestSubset()
    {
        // Exhaustive over all 2^7 subsets of a small alphabet, for a fixed tricky ACL.
        DfsSecurityDescriptor[] chain =
        [
            new(DfsIdentity.User("a"), [
                new DfsAccessRule(DfsIdentity.Group("g"), R.WriteData | R.Delete, DfsAccessControlType.Deny),
                new DfsAccessRule(DfsIdentity.User("a"), R.ReadData | R.WriteData),
            ]),
            new(DfsIdentity.User("b"), [
                new DfsAccessRule(DfsIdentity.Everyone, R.ReadData | R.Delete | R.TakeOwnership, DfsAccessControlType.Allow, DfsInheritanceFlags.ObjectInherit),
                new DfsAccessRule(DfsIdentity.User("a"), R.ChangePermissions, DfsAccessControlType.Deny, DfsInheritanceFlags.ObjectInherit),
            ]),
        ];
        IReadOnlyList<DfsAccessRule> rules = DfsAccessEvaluator.EffectiveRules(chain, false, chain[0].Owner);
        IReadOnlySet<DfsIdentity> caller = DfsAccessEvaluator.CallerIdentities("a", ["g"], false);
        R granted = DfsAccessEvaluator.GrantedRights(rules, chain[0].Owner, caller, false);
        // a: owner (RP, CP implicit); WriteData+Delete denied by g; ReadData allowed; TakeOwnership from Everyone.
        Assert.Equal(R.ReadPermissions | R.ChangePermissions | R.ReadData | R.TakeOwnership, granted);

        for (int mask = 1; mask < 1 << RightBits.Length; mask++)
        {
            R request = 0;
            for (int i = 0; i < RightBits.Length; i++)
            {
                if ((mask & (1 << i)) != 0)
                    request |= RightBits[i];
            }

            if ((request & ~R.Synchronize) == 0)
                continue;
            R wanted = request & ~R.Synchronize;
            Assert.Equal((granted & wanted) == wanted, DfsAccessEvaluator.IsGranted(rules, chain[0].Owner, caller, false, request));
        }
    }

    // ---- Reference model --------------------------------------------------------------------------------------

    // Applicable entries in evaluation order. Explicit entries at distance d from the target (0 = the target itself)
    // reach it when no descriptor strictly below the source (the target included) is protected, and:
    //   d = 0: not InheritOnly (explicit CreatorOwner entries stay CreatorOwner and match nobody);
    //   d = 1: directory target needs ContainerInherit; file target needs ObjectInherit (InheritOnly/NoPropagate irrelevant);
    //   d > 1: NoPropagateInherit must be clear, and the same ContainerInherit/ObjectInherit requirement holds
    //          (ContainerInherit carries an entry down through directories; ObjectInherit alone travels down as an
    //          inherit-only template that reaches files at any depth but never applies to directories).
    // Inherited CreatorOwner entries apply to the target's owner.
    private static List<RefEntry> ReferenceApplicable(DfsSecurityDescriptor[] chain, bool targetIsDirectory, DfsIdentity owner, string targetPath)
    {
        var result = new List<RefEntry>();
        string? sourcePath = targetPath;
        for (int d = 0; d < chain.Length; d++)
        {
            if (d > 0 && chain[d - 1].AreAccessRulesProtected)
                break;
            if (d > 0 && sourcePath is not null)
                sourcePath = sourcePath == "/" ? null : sourcePath[..Math.Max(1, sourcePath.LastIndexOf('/'))];

            foreach (DfsAccessControlType type in new[] { DfsAccessControlType.Deny, DfsAccessControlType.Allow })
            {
                foreach (DfsAccessRule rule in chain[d].Rules.Where(r => !r.IsInherited && r.Type == type))
                {
                    bool ci = (rule.Inheritance & DfsInheritanceFlags.ContainerInherit) != 0;
                    bool oi = (rule.Inheritance & DfsInheritanceFlags.ObjectInherit) != 0;
                    bool np = (rule.Propagation & DfsPropagationFlags.NoPropagateInherit) != 0;
                    bool io = (rule.Propagation & DfsPropagationFlags.InheritOnly) != 0;
                    bool kindMatches = targetIsDirectory ? ci : oi;
                    bool applies = d switch
                    {
                        0 => !io,
                        1 => kindMatches,
                        _ => !np && kindMatches,
                    };
                    if (!applies)
                        continue;
                    DfsIdentity identity = d > 0 && rule.Identity == DfsIdentity.CreatorOwner ? owner : rule.Identity;
                    result.Add(new RefEntry(identity, rule.Rights, rule.Type, d > 0, d > 0 ? sourcePath : null));
                }
            }
        }

        return result;
    }

    private static R ReferenceGranted(List<RefEntry> entries, DfsIdentity owner, IReadOnlySet<DfsIdentity> caller, bool callerIsAdmin)
    {
        bool isAdmin = callerIsAdmin || caller.Contains(DfsIdentity.Administrators);
        bool Holds(DfsIdentity id) =>
            id != DfsIdentity.CreatorOwner
            && (id == DfsIdentity.Everyone || (id == DfsIdentity.Administrators && isAdmin) || caller.Contains(id));

        R granted = 0;
        if (Holds(owner))
            granted |= R.ReadPermissions | R.ChangePermissions;
        if (isAdmin)
            granted |= R.TakeOwnership | R.ChangePermissions;
        foreach (R bit in AllSingleBits)
        {
            if ((granted & bit) != 0)
                continue;
            foreach (RefEntry e in entries)
            {
                if (!Holds(e.Identity) || (e.Rights & bit) == 0)
                    continue;
                if (e.Type == DfsAccessControlType.Allow)
                    granted |= bit;
                break;
            }
        }

        return granted;
    }

    // ---- Random generation -----------------------------------------------------------------------------------

    private static DfsSecurityDescriptor RandomDescriptor(Random random)
    {
        int count = random.Next(0, 5);
        var rules = new List<DfsAccessRule>(count);
        for (int i = 0; i < count; i++)
        {
            DfsIdentity identity = Principals[random.Next(Principals.Length)];
            R rights = 0;
            while (rights == 0)
            {
                foreach (R bit in RightBits)
                {
                    if (random.Next(3) == 0)
                        rights |= bit;
                }

                if (random.Next(6) == 0)
                    rights = R.FullControl;
            }

            var inheritance = (DfsInheritanceFlags)random.Next(4);
            if (identity == DfsIdentity.CreatorOwner && inheritance == DfsInheritanceFlags.None)
                inheritance = (DfsInheritanceFlags)random.Next(1, 4);
            DfsPropagationFlags propagation = inheritance == DfsInheritanceFlags.None ? DfsPropagationFlags.None : (DfsPropagationFlags)random.Next(4);
            DfsAccessControlType type = random.Next(3) == 0 ? DfsAccessControlType.Deny : DfsAccessControlType.Allow;
            rules.Add(new DfsAccessRule(identity, rights, type, inheritance, propagation));
        }

        var descriptor = new DfsSecurityDescriptor(Owners[random.Next(Owners.Length)], rules, AreAccessRulesProtected: random.Next(8) == 0);
        descriptor.Validate();
        return descriptor;
    }

    private static (IReadOnlySet<DfsIdentity> Caller, bool IsAdmin) RandomCaller(Random random)
    {
        string user = random.Next(3) switch { 0 => "a", 1 => "b", _ => "c" };
        var groups = new List<string>();
        if (random.Next(2) == 0)
            groups.Add("g");
        if (random.Next(3) == 0)
            groups.Add("h");
        bool isAdmin = random.Next(5) == 0;
        var set = new HashSet<DfsIdentity>(DfsAccessEvaluator.CallerIdentities(user, groups, isAdmin));
        if (random.Next(6) == 0)
            set.Remove(DfsIdentity.AuthenticatedUsers);
        if (random.Next(10) == 0)
            set.Add(DfsIdentity.CreatorOwner);     // must never match
        // Sometimes pass the admin flag only through the set, or only through the parameter.
        bool flag = isAdmin && random.Next(3) != 0;
        if (isAdmin && !flag)
            set.Add(DfsIdentity.Administrators);
        else if (isAdmin && random.Next(2) == 0)
            set.Remove(DfsIdentity.Administrators);
        return (set, flag);
    }

    private static R RandomRequest(Random random)
    {
        while (true)
        {
            R request = 0;
            foreach (R bit in RightBits)
            {
                if (random.Next(4) == 0)
                    request |= bit;
            }

            if (random.Next(10) == 0)
                request |= AllSingleBits[random.Next(AllSingleBits.Length)];
            if ((request & ~R.Synchronize) != 0)
                return request;
        }
    }

    private static string Describe(IReadOnlySet<DfsIdentity> caller, bool isAdmin) =>
        "{" + string.Join(", ", caller.Select(c => c.ToString()).Order(StringComparer.Ordinal)) + "}" + (isAdmin ? " admin" : "");
}
