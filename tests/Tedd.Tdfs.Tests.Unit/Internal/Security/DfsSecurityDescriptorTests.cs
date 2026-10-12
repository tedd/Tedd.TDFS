using static Tedd.Tdfs.Tests.Unit.Internal.Security.SecurityTestHelpers;
using R = Tedd.Tdfs.DfsFileSystemRights;

namespace Tedd.Tdfs.Tests.Unit.Internal.Security;

/// <summary>Descriptor construction, validation and equality (Addendum A, ACL-02, ACL-06).</summary>
public sealed class DfsSecurityDescriptorTests
{
    [Fact]
    public void DefaultRootIsEveryoneFullControlInheritable()
    {
        DfsSecurityDescriptor sd = DfsSecurityDescriptor.DefaultRoot(Alice);
        Assert.Equal(Alice, sd.Owner);
        Assert.False(sd.AreAccessRulesProtected);
        DfsAccessRule rule = Assert.Single(sd.Rules);
        Assert.Equal(new DfsAccessRule(DfsIdentity.Everyone, R.FullControl, DfsAccessControlType.Allow, CIOI), rule);
        Assert.False(rule.IsInherited);
        sd.Validate();
        Assert.Throws<ArgumentException>(() => DfsSecurityDescriptor.DefaultRoot(DfsIdentity.CreatorOwner));
        Assert.Throws<ArgumentException>(() => DfsSecurityDescriptor.DefaultRoot(default));
    }

    public static TheoryData<string, DfsSecurityDescriptor> InvalidDescriptors => new()
    {
        { "creator-owner owner", Sd(DfsIdentity.CreatorOwner) },
        { "default owner", Sd(default) },
        { "no rights", Sd(Alice, Allow(Bob, 0)) },
        { "undefined rights bit", Sd(Alice, Allow(Bob, (R)0x200)) },
        { "generic rights bit", Sd(Alice, Allow(Bob, (R)0x10000000)) },
        { "undefined type", Sd(Alice, new DfsAccessRule(Bob, R.ReadData, (DfsAccessControlType)5)) },
        { "inherit-only without inheritance", Sd(Alice, Allow(Bob, R.ReadData, DfsInheritanceFlags.None, IO)) },
        { "no-propagate without inheritance", Sd(Alice, Allow(Bob, R.ReadData, DfsInheritanceFlags.None, NP)) },
        { "creator-owner not inheritable", Sd(Alice, Allow(DfsIdentity.CreatorOwner, R.ReadData)) },
        { "default identity", Sd(Alice, Allow(default, R.ReadData)) },
        { "undefined inheritance flag", Sd(Alice, Allow(Bob, R.ReadData, (DfsInheritanceFlags)4)) },
        { "undefined propagation flag", Sd(Alice, Allow(Bob, R.ReadData, CI, (DfsPropagationFlags)4)) },
    };

    [Theory]
    [MemberData(nameof(InvalidDescriptors))]
    public void ValidateRejects(string reason, DfsSecurityDescriptor descriptor)
    {
        Assert.NotNull(descriptor.GetValidationError());
        ArgumentException ex = Assert.Throws<ArgumentException>(descriptor.Validate);
        Assert.False(string.IsNullOrEmpty(ex.Message), reason);
        Assert.Equal(DfsErrorCode.InvalidArgument, DfsErrors.ToCode(ex));
    }

    [Fact]
    public void ValidateAccepts()
    {
        Sd(Alice).Validate();
        Sd(Eng, Allow(Bob, R.Synchronize)).Validate();
        Sd(DfsIdentity.Administrators, Deny(DfsIdentity.Everyone, R.FullControl)).Validate();
        Sd(Alice,
            Allow(DfsIdentity.CreatorOwner, R.FullControl, CIOI, IO),
            Allow(DfsIdentity.CreatorOwner, R.ReadData, OI),
            Allow(Bob, R.ReadData, OI, IO),
            Allow(Bob, R.ReadData, CI, NP),
            Allow(Bob, R.ReadData, CIOI, NP | IO)).Validate();
        Protected(Alice).Validate();
    }

    [Fact]
    public void ValidateIgnoresInheritedEntries()
    {
        DfsAccessRule inherited = Allow(Bob, 0) with { IsInherited = true };
        DfsSecurityDescriptor sd = Sd(Alice, inherited, Allow(Carol, R.ReadData));
        sd.Validate();
        Assert.Equal(new[] { Allow(Carol, R.ReadData) }, sd.ExplicitRules);
        DfsSecurityDescriptor stripped = sd.WithoutInheritedRules();
        Assert.Equal(Sd(Alice, Allow(Carol, R.ReadData)), stripped);
        Assert.Same(stripped, stripped.WithoutInheritedRules());
    }

    [Fact]
    public void EqualityComparesRulesElementWise()
    {
        DfsSecurityDescriptor a = Sd(Alice, Allow(Bob, R.Read), Deny(Carol, R.Write));
        DfsSecurityDescriptor b = new(Alice, new List<DfsAccessRule> { Allow(Bob, R.Read), Deny(Carol, R.Write) });
        Assert.Equal(a, b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
        Assert.NotEqual(a, Sd(Alice, Deny(Carol, R.Write), Allow(Bob, R.Read)));     // order matters
        Assert.NotEqual(a, a with { AreAccessRulesProtected = true });
        Assert.NotEqual(a, a with { Owner = Bob });
        Assert.NotEqual(a, Sd(Alice, Allow(Bob, R.Read), Deny(Carol, R.Write) with { IsInherited = true }));
    }

    [Fact]
    public void RulesAreCopiedOnConstruction()
    {
        var list = new List<DfsAccessRule> { Allow(Bob, R.Read) };
        DfsSecurityDescriptor sd = new(Alice, list);
        list.Add(Allow(Carol, R.Read));
        Assert.Single(sd.Rules);
        DfsSecurityDescriptor changed = sd with { Rules = list };
        list.Clear();
        Assert.Equal(2, changed.Rules.Count);
        Assert.Throws<ArgumentNullException>(() => new DfsSecurityDescriptor(Alice, null!));
        Assert.Throws<ArgumentException>(() => new DfsSecurityDescriptor(Alice, [null!]));
    }

    [Fact]
    public void FromAccessFlagsMapsLegacyFlags()
    {
        DfsSecurityDescriptor none = DfsSecurityDescriptor.FromAccessFlags(Alice, ownerOnly: false, readOnlyForOthers: false);
        Assert.False(none.AreAccessRulesProtected);
        Assert.Empty(none.Rules);

        DfsSecurityDescriptor ownerOnly = DfsSecurityDescriptor.FromAccessFlags(Alice, ownerOnly: true, readOnlyForOthers: true);
        Assert.True(ownerOnly.AreAccessRulesProtected);
        Assert.Equal(
            new[] { Allow(Alice, R.FullControl, CIOI), Allow(DfsIdentity.Administrators, R.FullControl, CIOI) },
            ownerOnly.Rules);

        DfsSecurityDescriptor readOnly = DfsSecurityDescriptor.FromAccessFlags(Alice, ownerOnly: false, readOnlyForOthers: true);
        Assert.True(readOnly.AreAccessRulesProtected);
        Assert.Equal(
            new[] { Allow(Alice, R.FullControl, CIOI), Allow(DfsIdentity.Administrators, R.FullControl, CIOI), Allow(DfsIdentity.Everyone, R.Read, CIOI) },
            readOnly.Rules);

        // Effective semantics: others may only read; owner keeps full control.
        Assert.True(Can([readOnly], false, Caller("bob"), R.Read));
        Assert.False(Can([readOnly], false, Caller("bob"), R.WriteData));
        Assert.Equal(R.FullControl, Granted([readOnly], false, Caller("alice")));
        Assert.Equal((R)0, Granted([ownerOnly], false, Caller("bob")));
        Assert.Throws<ArgumentException>(() => DfsSecurityDescriptor.FromAccessFlags(DfsIdentity.CreatorOwner, true, false));
    }

    [Fact]
    public void AccessRuleHelpers()
    {
        Assert.True(Allow(Bob, R.Read, OI).IsInheritable);
        Assert.False(Allow(Bob, R.Read).IsInheritable);
        Assert.False(Allow(Bob, R.Read, CI, IO).AppliesToObject);
        Assert.True(Allow(Bob, R.Read, CI, NP).AppliesToObject);
        Assert.Throws<ArgumentException>(() => Allow(Bob, 0).Validate());
        Allow(Bob, R.Read).Validate();
    }

    [Theory]
    [InlineData(0, "None")]
    [InlineData(0x2, "WriteData")]
    [InlineData(0x1F01FF, "FullControl")]
    [InlineData(0x301BF, "Modify")]
    [InlineData(0x20089 | 0x116, "Read|Write")]
    [InlineData(0x1F01FF & ~0x100000, "Modify|DeleteSubdirectoriesAndFiles|ChangePermissions|TakeOwnership")]
    [InlineData(0x1 | 0x200, "ReadData|0x200")]
    public void FormatRightsIsUnambiguous(int rights, string expected)
    {
        Assert.Equal(expected, DfsAccessRule.FormatRights((R)rights));
    }

    [Fact]
    public void ToStringListsRules()
    {
        string text = Sd(Alice, Deny(Bob, R.WriteData, OI, IO), Allow(DfsIdentity.Everyone, R.Read) with { IsInherited = true, InheritedFrom = "/" }).ToString();
        Assert.Contains("user:alice", text, StringComparison.Ordinal);
        Assert.Contains("Deny user:bob WriteData", text, StringComparison.Ordinal);
        Assert.Contains("inherited from /", text, StringComparison.Ordinal);
    }
}
