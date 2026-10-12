using static Tedd.Tdfs.Tests.Unit.Internal.Security.SecurityTestHelpers;
using R = Tedd.Tdfs.DfsFileSystemRights;

namespace Tedd.Tdfs.Tests.Unit.Internal.Security;

/// <summary>Scenario tests for <see cref="DfsAccessEvaluator"/> (Addendum A, ACL-03 and ACL-04).</summary>
public sealed class DfsAccessEvaluatorTests
{
    private static readonly IReadOnlySet<DfsIdentity> AliceInEng = Caller("alice", "eng");
    private static readonly IReadOnlySet<DfsIdentity> BobInEng = Caller("bob", "eng");
    private static readonly IReadOnlySet<DfsIdentity> AliceOnly = Caller("alice");
    private static readonly IReadOnlySet<DfsIdentity> BobOnly = Caller("bob");
    private static readonly IReadOnlySet<DfsIdentity> CarolOnly = Caller("carol");

    // ---- Default root (ACL-001, ACL-06) ------------------------------------------------------------------------

    [Fact]
    public void DefaultRoot_NewFileShowsInheritedEveryoneFullControl()
    {
        DfsSecurityDescriptor root = DfsSecurityDescriptor.DefaultRoot(Admin);
        DfsSecurityDescriptor file = Sd(Alice);

        IReadOnlyList<DfsAccessRule> rules = DfsAccessEvaluator.EffectiveRules([file, root], false, Alice, "/f");

        DfsAccessRule rule = Assert.Single(rules);
        Assert.Equal(DfsIdentity.Everyone, rule.Identity);
        Assert.Equal(R.FullControl, rule.Rights);
        Assert.Equal(DfsAccessControlType.Allow, rule.Type);
        Assert.Equal(DfsInheritanceFlags.None, rule.Inheritance);
        Assert.Equal(DfsPropagationFlags.None, rule.Propagation);
        Assert.True(rule.IsInherited);
        Assert.Equal("/", rule.InheritedFrom);
        Assert.Equal(R.FullControl, DfsAccessEvaluator.GrantedRights(rules, Alice, BobOnly, false));
    }

    [Fact]
    public void DefaultRoot_NewDirectoryInheritsWithInheritanceFlags()
    {
        IReadOnlyList<DfsAccessRule> rules = DfsAccessEvaluator.EffectiveRules(
            [Sd(Alice), DfsSecurityDescriptor.DefaultRoot(Admin)], true, Alice, "/d");

        DfsAccessRule rule = Assert.Single(rules);
        Assert.Equal(CIOI, rule.Inheritance);
        Assert.Equal(DfsPropagationFlags.None, rule.Propagation);
        Assert.True(rule.IsInherited);
        Assert.Equal("/", rule.InheritedFrom);
    }

    [Fact]
    public void Root_HasOnlyExplicitRules()
    {
        DfsSecurityDescriptor root = DfsSecurityDescriptor.DefaultRoot(Admin);
        IReadOnlyList<DfsAccessRule> rules = DfsAccessEvaluator.EffectiveRules([root], true, Admin, "/");
        DfsAccessRule rule = Assert.Single(rules);
        Assert.False(rule.IsInherited);
        Assert.Null(rule.InheritedFrom);
        Assert.Equal(CIOI, rule.Inheritance);
    }

    // ---- Canonical order (ACL-004, ACL-005, ACL-006) ---------------------------------------------------------

    [Fact]
    public void ExplicitDenyBeatsExplicitAllowRegardlessOfStoredOrder()
    {
        // Allow is stored first; canonical order still evaluates the Deny first.
        DfsSecurityDescriptor[] chain = [Protected(Admin, Allow(Eng, R.FullControl), Deny(Alice, R.ReadData))];

        Assert.False(Can(chain, false, AliceInEng, R.ReadData));
        Assert.True(Can(chain, false, AliceInEng, R.WriteData));
        Assert.Equal(R.FullControl & ~R.ReadData, Granted(chain, false, AliceInEng));
        Assert.Equal(R.FullControl, Granted(chain, false, BobInEng));

        IReadOnlyList<DfsAccessRule> rules = DfsAccessEvaluator.EffectiveRules(chain, false, Admin);
        Assert.Equal(DfsAccessControlType.Deny, rules[0].Type);
        Assert.Equal(DfsAccessControlType.Allow, rules[1].Type);
    }

    [Fact]
    public void ExplicitAllowBeatsInheritedDeny()
    {
        DfsSecurityDescriptor parent = Protected(Admin, Deny(DfsIdentity.Everyone, R.ReadData, CIOI), Allow(DfsIdentity.Everyone, R.FullControl, CIOI));
        DfsSecurityDescriptor file = Sd(Admin, Allow(Alice, R.ReadData));
        DfsSecurityDescriptor[] chain = [file, parent];

        Assert.True(Can(chain, false, AliceOnly, R.ReadData));
        Assert.False(Can(chain, false, BobOnly, R.ReadData));
        Assert.True(Can(chain, false, BobOnly, R.WriteData));
    }

    [Fact]
    public void ExplicitDenyBeatsInheritedAllow()
    {
        DfsSecurityDescriptor parent = Protected(Admin, Allow(DfsIdentity.Everyone, R.FullControl, OI));
        DfsSecurityDescriptor file = Sd(Admin, Deny(Alice, R.WriteData));
        DfsSecurityDescriptor[] chain = [file, parent];

        Assert.False(Can(chain, false, AliceOnly, R.WriteData));
        Assert.True(Can(chain, false, AliceOnly, R.ReadData));
        Assert.True(Can(chain, false, BobOnly, R.WriteData));
    }

    [Fact]
    public void NearestAncestorAllowBeatsFartherAncestorDeny()
    {
        // NTFS canonical order is per generation: everything inherited from the parent (Deny and Allow) precedes
        // everything inherited from the grandparent.
        DfsSecurityDescriptor root = DfsSecurityDescriptor.DefaultRoot(Admin);
        DfsSecurityDescriptor a = Sd(Admin, Deny(Alice, R.WriteData, CIOI));
        DfsSecurityDescriptor b = Sd(Admin, Allow(Alice, R.WriteData, CIOI));
        DfsSecurityDescriptor f = Sd(Admin);
        DfsSecurityDescriptor[] chain = [f, b, a, root];

        Assert.True(Can(chain, false, AliceOnly, R.WriteData));

        IReadOnlyList<DfsAccessRule> rules = DfsAccessEvaluator.EffectiveRules(chain, false, Admin, "/a/b/f");
        Assert.Equal(new string?[] { "/a/b", "/a", "/" }, rules.Select(r => r.InheritedFrom));
        Assert.All(rules, r => Assert.True(r.IsInherited));
        Assert.Equal(new[] { DfsAccessControlType.Allow, DfsAccessControlType.Deny, DfsAccessControlType.Allow }, rules.Select(r => r.Type));
    }

    [Fact]
    public void NearestAncestorDenyBeatsFartherAncestorAllow()
    {
        DfsSecurityDescriptor root = DfsSecurityDescriptor.DefaultRoot(Admin);
        DfsSecurityDescriptor a = Sd(Admin, Allow(Alice, R.WriteData, CIOI));
        DfsSecurityDescriptor b = Sd(Admin, Deny(Alice, R.WriteData, CIOI));
        DfsSecurityDescriptor[] chain = [Sd(Admin), b, a, root];

        Assert.False(Can(chain, false, AliceOnly, R.WriteData));
        Assert.True(Can(chain, false, BobOnly, R.WriteData));
        // Directories inherit the same way (ContainerInherit).
        Assert.False(Can(chain, true, AliceOnly, R.CreateFiles));
    }

    [Fact]
    public void WithinOneGeneration_DenyPrecedesAllow()
    {
        DfsSecurityDescriptor parent = Protected(Admin, Allow(Alice, R.ReadData, OI), Deny(Alice, R.ReadData, OI));
        DfsSecurityDescriptor[] chain = [Sd(Admin), parent];
        Assert.False(Can(chain, false, AliceOnly, R.ReadData));
    }

    // ---- All-or-nothing (ACL-007) ------------------------------------------------------------------------------

    [Fact]
    public void PartialRightsAreNotGranted()
    {
        DfsSecurityDescriptor[] chain = [Protected(Bob, Allow(Alice, R.ReadData))];

        Assert.False(Can(chain, false, AliceOnly, R.ReadData | R.WriteData));
        Assert.True(Can(chain, false, AliceOnly, R.ReadData));
        Assert.Equal(R.ReadData, Granted(chain, false, AliceOnly));
    }

    [Fact]
    public void RightsNeverMentionedAreDenied()
    {
        DfsSecurityDescriptor[] chain = [Protected(Bob)];
        Assert.False(Can(chain, false, AliceOnly, R.ReadData));
        Assert.Equal((R)0, Granted(chain, false, AliceOnly));
    }

    [Fact]
    public void DenyOfOneRequestedBitDeniesTheWholeRequest()
    {
        DfsSecurityDescriptor[] chain = [Protected(Bob, Allow(Alice, R.ReadData), Deny(Alice, R.WriteData), Allow(Alice, R.FullControl))];
        Assert.False(Can(chain, false, AliceOnly, R.ReadData | R.WriteData));
        Assert.True(Can(chain, false, AliceOnly, R.ReadData | R.Delete));
    }

    // ---- Inheritance flags (ACL-008 .. ACL-011) --------------------------------------------------------------

    [Fact]
    public void ObjectInheritAppliesToFiles_ContainerInheritToDirectories()
    {
        // Root grants alice via OI, bob via CI, carol via CI|OI.
        DfsSecurityDescriptor root = Protected(Admin, Allow(Alice, R.ReadData, OI), Allow(Bob, R.ReadData, CI), Allow(Carol, R.ReadData, CIOI));
        DfsSecurityDescriptor[] file = [Sd(Admin), root];
        DfsSecurityDescriptor[] dir = [Sd(Admin), root];
        DfsSecurityDescriptor[] grandchildFile = [Sd(Admin), Sd(Admin), root];
        DfsSecurityDescriptor[] grandchildDir = [Sd(Admin), Sd(Admin), root];

        Assert.True(Can(file, false, AliceOnly, R.ReadData));
        Assert.False(Can(file, false, BobOnly, R.ReadData));
        Assert.True(Can(file, false, CarolOnly, R.ReadData));

        Assert.False(Can(dir, true, AliceOnly, R.ListDirectory));
        Assert.True(Can(dir, true, BobOnly, R.ListDirectory));
        Assert.True(Can(dir, true, CarolOnly, R.ListDirectory));

        // ObjectInherit passes through subdirectories (as an inherit-only template) to files at any depth.
        Assert.True(Can(grandchildFile, false, AliceOnly, R.ReadData));
        Assert.False(Can(grandchildFile, false, BobOnly, R.ReadData));
        Assert.True(Can(grandchildFile, false, CarolOnly, R.ReadData));

        Assert.False(Can(grandchildDir, true, AliceOnly, R.ListDirectory));
        Assert.True(Can(grandchildDir, true, BobOnly, R.ListDirectory));
        Assert.True(Can(grandchildDir, true, CarolOnly, R.ListDirectory));
    }

    [Fact]
    public void ObjectInheritOnlyEntry_IsAnInheritOnlyTemplateOnSubdirectories()
    {
        DfsSecurityDescriptor root = Protected(Admin, Allow(Alice, R.ReadData, OI));
        IReadOnlyList<DfsAccessRule> rules = DfsAccessEvaluator.EffectiveRules([Sd(Admin), root], true, Admin, "/d");
        DfsAccessRule rule = Assert.Single(rules);
        Assert.Equal(OI, rule.Inheritance);
        Assert.Equal(IO, rule.Propagation);
        Assert.False(rule.AppliesToObject);
    }

    [Fact]
    public void InheritOnlyEntryDoesNotApplyToItsOwnObject()
    {
        DfsSecurityDescriptor root = Protected(Admin);
        DfsSecurityDescriptor d = Sd(Admin, Allow(Alice, R.ReadData, CIOI, IO));

        Assert.False(Can([d, root], true, AliceOnly, R.ListDirectory));
        Assert.True(Can([Sd(Admin), d, root], false, AliceOnly, R.ReadData));
        Assert.True(Can([Sd(Admin), d, root], true, AliceOnly, R.ListDirectory));
        // The inherited copy on a subdirectory applies there (InheritOnly is cleared).
        DfsAccessRule onChild = Assert.Single(DfsAccessEvaluator.EffectiveRules([Sd(Admin), d, root], true, Admin));
        Assert.Equal(CIOI, onChild.Inheritance);
        Assert.Equal(DfsPropagationFlags.None, onChild.Propagation);
    }

    [Fact]
    public void NoPropagateInheritReachesImmediateChildrenOnly()
    {
        DfsSecurityDescriptor root = Protected(Admin);
        DfsSecurityDescriptor d = Sd(Admin, Allow(Alice, R.ReadData, CIOI, NP));

        Assert.True(Can([d, root], true, AliceOnly, R.ListDirectory));                  // applies to d itself
        Assert.True(Can([Sd(Admin), d, root], false, AliceOnly, R.ReadData));           // /d/f
        Assert.True(Can([Sd(Admin), d, root], true, AliceOnly, R.ListDirectory));       // /d/e
        Assert.False(Can([Sd(Admin), Sd(Admin), d, root], false, AliceOnly, R.ReadData));    // /d/e/f
        Assert.False(Can([Sd(Admin), Sd(Admin), d, root], true, AliceOnly, R.ListDirectory)); // /d/e/g

        // The inherited copy loses its inheritability.
        DfsAccessRule copy = Assert.Single(DfsAccessEvaluator.EffectiveRules([Sd(Admin), d, root], true, Admin));
        Assert.Equal(DfsInheritanceFlags.None, copy.Inheritance);
        Assert.Equal(DfsPropagationFlags.None, copy.Propagation);
        Assert.False(copy.IsInheritable);
    }

    [Fact]
    public void NoPropagateObjectInherit_IsNotInheritedBySubdirectories()
    {
        DfsSecurityDescriptor root = Protected(Admin);
        DfsSecurityDescriptor d = Sd(Admin, Allow(Alice, R.ReadData, OI, NP));

        Assert.True(Can([Sd(Admin), d, root], false, AliceOnly, R.ReadData));
        Assert.Empty(DfsAccessEvaluator.EffectiveRules([Sd(Admin), d, root], true, Admin));
        Assert.False(Can([Sd(Admin), Sd(Admin), d, root], false, AliceOnly, R.ReadData));
    }

    [Fact]
    public void ProtectedDescriptorBlocksInheritance()
    {
        DfsSecurityDescriptor root = DfsSecurityDescriptor.DefaultRoot(Admin);
        DfsSecurityDescriptor d = Protected(Admin, Allow(Alice, R.ReadData, CIOI));

        Assert.True(Can([d, root], true, AliceOnly, R.ListDirectory));
        Assert.Equal((R)0, Granted([d, root], true, BobOnly));
        Assert.Equal(R.ReadData, Granted([Sd(Admin), d, root], false, AliceOnly));
        Assert.Equal((R)0, Granted([Sd(Admin), d, root], false, BobOnly));

        // Without protection, Everyone FullControl from the root reaches both.
        DfsSecurityDescriptor open = d with { AreAccessRulesProtected = false };
        Assert.Equal(R.FullControl, Granted([Sd(Admin), open, root], false, BobOnly));

        // A protected file inherits nothing at all.
        Assert.Empty(DfsAccessEvaluator.EffectiveRules([Protected(Admin), d, root], false, Admin));
    }

    [Fact]
    public void ProtectedAncestorStillPassesItsOwnInheritableEntries()
    {
        DfsSecurityDescriptor root = Protected(Admin, Allow(Bob, R.FullControl, CIOI));
        DfsSecurityDescriptor a = Protected(Admin, Allow(Alice, R.ReadData, CIOI));
        DfsSecurityDescriptor[] chain = [Sd(Admin), Sd(Admin), a, root];
        Assert.True(Can(chain, false, AliceOnly, R.ReadData));
        Assert.False(Can(chain, false, BobOnly, R.ReadData));
        Assert.Equal(new string?[] { "/a" }, DfsAccessEvaluator.EffectiveRules(chain, false, Admin, "/a/b/f").Select(r => r.InheritedFrom));
    }

    // ---- CreatorOwner (ACL-012) --------------------------------------------------------------------------------

    [Fact]
    public void CreatorOwnerGrantsToEachInheritingObjectsOwner()
    {
        DfsSecurityDescriptor root = Protected(Admin);
        DfsSecurityDescriptor d = Sd(Admin, Allow(DfsIdentity.CreatorOwner, R.FullControl, CIOI, IO), Allow(DfsIdentity.Everyone, R.ReadData, CIOI));

        DfsSecurityDescriptor[] aliceFile = [Sd(Alice), d, root];
        DfsSecurityDescriptor[] bobFile = [Sd(Bob), d, root];
        Assert.Equal(R.FullControl, Granted(aliceFile, false, AliceOnly));
        Assert.Equal(R.ReadData, Granted(aliceFile, false, BobOnly));
        Assert.Equal(R.FullControl, Granted(bobFile, false, BobOnly));
        Assert.Equal(R.ReadData, Granted(bobFile, false, AliceOnly));
        // Not on the directory itself (inherit-only) and never to a caller holding "CreatorOwner".
        var spoof = new HashSet<DfsIdentity>(BobOnly) { DfsIdentity.CreatorOwner };
        Assert.Equal(R.ReadData, Granted(aliceFile, false, spoof));
    }

    [Fact]
    public void CreatorOwnerOnSubdirectory_SplitsIntoOwnerEntryAndTemplate()
    {
        DfsSecurityDescriptor root = Protected(Admin);
        DfsSecurityDescriptor d = Sd(Admin, Allow(DfsIdentity.CreatorOwner, R.Modify, CIOI, IO));

        IReadOnlyList<DfsAccessRule> rules = DfsAccessEvaluator.EffectiveRules([Sd(Alice), d, root], true, Alice, "/d/e");
        Assert.Equal(2, rules.Count);
        Assert.Equal(Alice, rules[0].Identity);
        Assert.Equal(DfsInheritanceFlags.None, rules[0].Inheritance);
        Assert.True(rules[0].AppliesToObject);
        Assert.Equal(DfsIdentity.CreatorOwner, rules[1].Identity);
        Assert.Equal(CIOI, rules[1].Inheritance);
        Assert.Equal(IO, rules[1].Propagation);
        Assert.All(rules, r => Assert.Equal("/d", r.InheritedFrom));

        // The template keeps working below: /d/e/f owned by bob.
        DfsSecurityDescriptor[] grandchild = [Sd(Bob), Sd(Alice), d, root];
        Assert.Equal(R.Modify | DfsAccessEvaluator.OwnerImplicitRights, Granted(grandchild, false, BobOnly));
        Assert.Equal((R)0, Granted(grandchild, false, AliceOnly));
    }

    [Fact]
    public void CreatorOwnerWithNoPropagate_YieldsOnlyTheOwnerEntry()
    {
        DfsSecurityDescriptor d = Protected(Admin, Allow(DfsIdentity.CreatorOwner, R.ReadData, CIOI, NP));
        IReadOnlyList<DfsAccessRule> rules = DfsAccessEvaluator.EffectiveRules([Sd(Alice), d], true, Alice);
        DfsAccessRule rule = Assert.Single(rules);
        Assert.Equal(Alice, rule.Identity);
        Assert.Equal(DfsInheritanceFlags.None, rule.Inheritance);
    }

    [Fact]
    public void ExplicitCreatorOwnerEntryDoesNotApplyToItsOwnObject()
    {
        DfsSecurityDescriptor d = Protected(Alice, Allow(DfsIdentity.CreatorOwner, R.FullControl, CIOI));
        Assert.Equal(DfsAccessEvaluator.OwnerImplicitRights, Granted([d], true, AliceOnly));
        Assert.Equal(DfsIdentity.CreatorOwner, Assert.Single(DfsAccessEvaluator.EffectiveRules([d], true, Alice)).Identity);
    }

    [Fact]
    public void CreatorOwnerFollowsTheCurrentOwner()
    {
        // Dynamic inheritance (ACL-03): changing the owner changes who the inherited CreatorOwner entry grants.
        DfsSecurityDescriptor d = Protected(Admin, Allow(DfsIdentity.CreatorOwner, R.WriteData, OI));
        DfsSecurityDescriptor file = Sd(Alice);
        Assert.True(Can([file, d], false, AliceOnly, R.WriteData));
        DfsSecurityDescriptor taken = file with { Owner = Bob };
        Assert.False(Can([taken, d], false, AliceOnly, R.WriteData));
        Assert.True(Can([taken, d], false, BobOnly, R.WriteData));
    }

    // ---- Implicit rights (ACL-013, ACL-014) -------------------------------------------------------------------

    [Fact]
    public void OwnerAlwaysHasReadAndChangePermissions()
    {
        DfsSecurityDescriptor[] chain =
        [
            Protected(Alice, Deny(Eng, R.ReadPermissions | R.ChangePermissions), Deny(Alice, R.FullControl), Allow(DfsIdentity.Everyone, R.FullControl)),
        ];

        Assert.True(Can(chain, false, AliceInEng, R.ReadPermissions));
        Assert.True(Can(chain, false, AliceInEng, R.ChangePermissions));
        Assert.True(Can(chain, false, AliceInEng, R.ReadPermissions | R.ChangePermissions));
        Assert.False(Can(chain, false, AliceInEng, R.ReadData));
        Assert.Equal(R.ReadPermissions | R.ChangePermissions, Granted(chain, false, AliceInEng));
        // A non-owner member of the group is denied.
        Assert.False(Can(chain, false, BobInEng, R.ReadPermissions));
        Assert.True(Can(chain, false, CarolOnly, R.ReadPermissions));
    }

    [Fact]
    public void GroupOwnerGivesImplicitRightsToMembers()
    {
        DfsSecurityDescriptor[] chain = [Protected(Eng)];
        Assert.Equal(DfsAccessEvaluator.OwnerImplicitRights, Granted(chain, false, BobInEng));
        Assert.Equal((R)0, Granted(chain, false, CarolOnly));
    }

    [Fact]
    public void AdministratorsAlwaysHaveTakeOwnershipAndChangePermissionsOnly()
    {
        DfsSecurityDescriptor[] chain = [Protected(Alice, Deny(DfsIdentity.Everyone, R.FullControl))];
        IReadOnlySet<DfsIdentity> admin = DfsAccessEvaluator.CallerIdentities("admin", [], isClusterAdmin: true);

        Assert.Equal(R.TakeOwnership | R.ChangePermissions, Granted(chain, false, admin, isAdmin: true));
        Assert.True(Can(chain, false, admin, R.TakeOwnership, isAdmin: true));
        Assert.False(Can(chain, false, admin, R.ReadData, isAdmin: true));
        Assert.False(Can(chain, false, admin, R.ReadPermissions, isAdmin: true));
        // The set alone (Administrators member) also counts as administrator.
        Assert.Equal(R.TakeOwnership | R.ChangePermissions, Granted(chain, false, admin, isAdmin: false));
        Assert.Equal((R)0, Granted(chain, false, BobOnly));
    }

    [Fact]
    public void AdministratorsEntryMatchesAdministratorsOnly()
    {
        DfsSecurityDescriptor[] chain = [Protected(Alice, Allow(DfsIdentity.Administrators, R.ReadData))];
        Assert.True(Can(chain, false, BobOnly, R.ReadData, isAdmin: true));
        Assert.False(Can(chain, false, BobOnly, R.ReadData, isAdmin: false));
    }

    // ---- Identity matching ------------------------------------------------------------------------------------

    [Fact]
    public void EveryoneMatchesEvenAnEmptyIdentitySet()
    {
        DfsSecurityDescriptor[] chain = [Protected(Alice, Allow(DfsIdentity.Everyone, R.ReadData))];
        Assert.True(Can(chain, false, new HashSet<DfsIdentity>(), R.ReadData));
    }

    [Fact]
    public void GroupAndAuthenticatedUsersMatchThroughTheIdentitySet()
    {
        DfsSecurityDescriptor[] chain = [Protected(Admin, Allow(Eng, R.ReadData), Allow(DfsIdentity.AuthenticatedUsers, R.ReadAttributes))];
        Assert.True(Can(chain, false, AliceInEng, R.ReadData));
        Assert.False(Can(chain, false, CarolOnly, R.ReadData));
        Assert.True(Can(chain, false, CarolOnly, R.ReadAttributes));
        Assert.False(Can(chain, false, new HashSet<DfsIdentity> { Carol }, R.ReadAttributes));
    }

    [Fact]
    public void CallerIdentities_ContainsUserGroupsAndWellKnowns()
    {
        IReadOnlySet<DfsIdentity> set = DfsAccessEvaluator.CallerIdentities("alice", ["eng", "ops"], isClusterAdmin: true);
        Assert.True(set.SetEquals(
            [Alice, Eng, DfsIdentity.Group("ops"), DfsIdentity.Everyone, DfsIdentity.AuthenticatedUsers, DfsIdentity.Administrators]));
        Assert.Equal(6, set.Count);
        Assert.DoesNotContain(DfsIdentity.Administrators, DfsAccessEvaluator.CallerIdentities("alice", [], false));
        Assert.Throws<ArgumentException>(() => DfsAccessEvaluator.CallerIdentities("bad:name", [], false));
    }

    // ---- Synchronize and request validation -------------------------------------------------------------------

    [Fact]
    public void SynchronizeIsIgnoredInRequests()
    {
        DfsSecurityDescriptor[] readOnly = [Protected(Bob, Allow(Alice, R.Read))];
        Assert.True(Can(readOnly, false, AliceOnly, R.Read | R.Synchronize));
        Assert.Equal(R.Read, Granted(readOnly, false, AliceOnly));

        DfsSecurityDescriptor[] denySync = [Protected(Bob, Deny(Alice, R.Synchronize), Allow(Alice, R.FullControl))];
        Assert.True(Can(denySync, false, AliceOnly, R.ReadData | R.Synchronize));
        Assert.Equal(R.FullControl & ~R.Synchronize, Granted(denySync, false, AliceOnly));

        DfsSecurityDescriptor[] full = [Protected(Bob, Allow(Alice, R.FullControl))];
        Assert.Equal(R.FullControl, Granted(full, false, AliceOnly));
    }

    [Fact]
    public void InvalidRequestsAreRejected()
    {
        IReadOnlyList<DfsAccessRule> rules = [Allow(Alice, R.FullControl)];
        Assert.Throws<ArgumentException>(() => DfsAccessEvaluator.IsGranted(rules, Bob, AliceOnly, false, R.Synchronize));
        Assert.Throws<ArgumentException>(() => DfsAccessEvaluator.IsGranted(rules, Bob, AliceOnly, false, 0));
        Assert.Throws<ArgumentException>(() => DfsAccessEvaluator.IsGranted(rules, Bob, AliceOnly, false, (R)0x200));
        Assert.Throws<ArgumentException>(() => DfsAccessEvaluator.IsGranted(rules, default, AliceOnly, false, R.ReadData));
        Assert.Throws<ArgumentException>(() => DfsAccessEvaluator.GrantedRights(rules, DfsIdentity.CreatorOwner, AliceOnly, false));
        Assert.Throws<ArgumentNullException>(() => DfsAccessEvaluator.GrantedRights(null!, Bob, AliceOnly, false));
        Assert.Throws<ArgumentNullException>(() => DfsAccessEvaluator.GrantedRights(rules, Bob, null!, false));
    }

    // ---- EffectiveRules argument handling ----------------------------------------------------------------------

    [Fact]
    public void EffectiveRules_IgnoresInheritedEntriesInStoredDescriptors()
    {
        DfsAccessRule bogus = Allow(Bob, R.FullControl) with { IsInherited = true, InheritedFrom = "/x" };
        DfsSecurityDescriptor file = Sd(Alice, bogus, Allow(Carol, R.ReadData));
        IReadOnlyList<DfsAccessRule> rules = DfsAccessEvaluator.EffectiveRules([file, Protected(Admin)], false, Alice);
        DfsAccessRule only = Assert.Single(rules);
        Assert.Equal(Carol, only.Identity);
        Assert.False(only.IsInherited);
    }

    [Fact]
    public void EffectiveRules_StreamPathUsesItsFile()
    {
        IReadOnlyList<DfsAccessRule> rules = DfsAccessEvaluator.EffectiveRules(
            [Sd(Alice), Sd(Admin), DfsSecurityDescriptor.DefaultRoot(Admin)], false, Alice, "/d/f:s");
        Assert.Equal("/", Assert.Single(rules).InheritedFrom);
    }

    [Fact]
    public void EffectiveRules_RejectsInvalidArguments()
    {
        DfsSecurityDescriptor root = DfsSecurityDescriptor.DefaultRoot(Admin);
        Assert.Throws<ArgumentException>(() => DfsAccessEvaluator.EffectiveRules([], false, Alice));
        Assert.Throws<ArgumentException>(() => DfsAccessEvaluator.EffectiveRules([null!], false, Alice));
        Assert.Throws<ArgumentException>(() => DfsAccessEvaluator.EffectiveRules([Sd(Alice), root], false, default));
        Assert.Throws<ArgumentException>(() => DfsAccessEvaluator.EffectiveRules([Sd(Alice), root], false, DfsIdentity.CreatorOwner));
        // More descriptors than the path has levels.
        Assert.Throws<ArgumentException>(() => DfsAccessEvaluator.EffectiveRules([Sd(Alice), Sd(Admin), root], false, Alice, "/f"));
        Assert.Throws<ArgumentException>(() => DfsAccessEvaluator.EffectiveRules([Sd(Alice), root], false, Alice, "/a//f"));
        // Invalid explicit entries are rejected rather than silently evaluated.
        Assert.Throws<ArgumentException>(() => DfsAccessEvaluator.EffectiveRules([Sd(Alice, Allow(Bob, 0)), root], false, Alice));
        Assert.Throws<ArgumentException>(() => DfsAccessEvaluator.EffectiveRules([Sd(Alice), Sd(Admin, Allow(Bob, R.ReadData, propagation: IO))], false, Alice));
    }

    [Fact]
    public void EffectiveRules_ShorterChainIsAllowed()
    {
        // A chain may stop early (e.g. at a protected ancestor); paths are assigned from the target upwards.
        IReadOnlyList<DfsAccessRule> rules = DfsAccessEvaluator.EffectiveRules(
            [Sd(Alice), Protected(Admin, Allow(Bob, R.ReadData, OI))], false, Alice, "/a/b/f");
        Assert.Equal("/a/b", Assert.Single(rules).InheritedFrom);
    }

    [Fact]
    public void EffectiveRules_ResultIsCanonicalAcrossGenerations()
    {
        DfsSecurityDescriptor root = Protected(Admin, Allow(Carol, R.ReadData, CIOI), Deny(Carol, R.WriteData, CIOI));
        DfsSecurityDescriptor a = Sd(Admin, Allow(Bob, R.ReadData, CIOI), Deny(Bob, R.WriteData, CIOI));
        DfsSecurityDescriptor f = Sd(Admin, Allow(Alice, R.ReadData), Deny(Alice, R.WriteData));

        IReadOnlyList<DfsAccessRule> rules = DfsAccessEvaluator.EffectiveRules([f, a, root], false, Admin, "/a/f");
        var expected = new (DfsIdentity, DfsAccessControlType, string?)[]
        {
            (Alice, DfsAccessControlType.Deny, null), (Alice, DfsAccessControlType.Allow, null),
            (Bob, DfsAccessControlType.Deny, "/a"), (Bob, DfsAccessControlType.Allow, "/a"),
            (Carol, DfsAccessControlType.Deny, "/"), (Carol, DfsAccessControlType.Allow, "/"),
        };
        Assert.Equal(expected, rules.Select(r => (r.Identity, r.Type, r.InheritedFrom)));
    }
}
