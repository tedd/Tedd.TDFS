namespace Tedd.Tdfs.Tests.Unit.Internal.Security;

/// <summary>Identity text forms, validation and well-known identities (Addendum A, ACL-01, ACL-003).</summary>
public sealed class DfsIdentityTests
{
    [Fact]
    public void WellKnownIdentitiesHaveCanonicalNames()
    {
        Assert.Equal("Everyone", DfsIdentity.Everyone.ToString());
        Assert.Equal("AuthenticatedUsers", DfsIdentity.AuthenticatedUsers.ToString());
        Assert.Equal("Administrators", DfsIdentity.Administrators.ToString());
        Assert.Equal("CreatorOwner", DfsIdentity.CreatorOwner.ToString());
        foreach (DfsIdentity id in new[] { DfsIdentity.Everyone, DfsIdentity.AuthenticatedUsers, DfsIdentity.Administrators, DfsIdentity.CreatorOwner })
        {
            Assert.Equal(DfsIdentityKind.WellKnown, id.Kind);
            Assert.True(id.IsWellKnown);
            Assert.False(id.IsDefault);
            Assert.Equal(id, DfsIdentity.Parse(id.ToString()));
        }
    }

    [Fact]
    public void UserAndGroupFormat()
    {
        Assert.Equal("user:alice", DfsIdentity.User("alice").ToString());
        Assert.Equal("group:eng", DfsIdentity.Group("eng").ToString());
        Assert.Equal(DfsIdentityKind.User, DfsIdentity.User("alice").Kind);
        Assert.Equal("alice", DfsIdentity.User("alice").Name);
        Assert.Equal(DfsIdentityKind.Group, DfsIdentity.Group("eng").Kind);
        (DfsIdentityKind kind, string name) = DfsIdentity.Group("eng");
        Assert.Equal((DfsIdentityKind.Group, "eng"), (kind, name));
    }

    public static TheoryData<string> ValidTexts => new()
    {
        "user:alice", "group:eng", "user:Alice", "user:a.b-c_d@example.org", "user:with space", "group:Everyone",
        "user:Administrators", "user:éøå", "user:é", "user:\U0001F600", "user:" + new string('x', 255),
        "user:" + string.Concat(Enumerable.Repeat("é", 127)) + "x", "Everyone", "AuthenticatedUsers", "Administrators", "CreatorOwner",
    };

    [Theory]
    [MemberData(nameof(ValidTexts))]
    public void ParseFormatRoundTrip(string text)
    {
        DfsIdentity id = DfsIdentity.Parse(text);
        Assert.Equal(text, id.ToString());
        Assert.True(DfsIdentity.TryParse(text, out DfsIdentity parsed));
        Assert.Equal(id, parsed);
        Assert.Equal(id, new DfsIdentity(id.Kind, id.Name));
    }

    public static TheoryData<string> InvalidTexts => new()
    {
        "", "user:", "group:", "everyone", "EVERYONE", "Everyone ", " Everyone", "everyOne", "User:alice", "USER:alice",
        "Group:eng", "role:x", "alice", "user", "user:a:b", "group::x", "user:\u0001", "user:a\u007F", "user:a\u0085",
        "user:a\nb", "user:a\tb", "user:" + new string('x', 256),
        "user:" + string.Concat(Enumerable.Repeat("é", 128)), "creatorowner", "Creator Owner",
    };

    [Theory]
    [MemberData(nameof(InvalidTexts))]
    public void MalformedOrUnknownIdentitiesAreRejected(string text)
    {
        Assert.False(DfsIdentity.TryParse(text, out DfsIdentity id));
        Assert.True(id.IsDefault);
        Assert.Throws<FormatException>(() => DfsIdentity.Parse(text));
    }

    [Fact]
    public void UnpairedSurrogatesAreRejected()
    {
        // Kept out of theory data: malformed UTF-16 does not survive test-case serialization.
        foreach (string text in new[] { "user:\uD800", "user:a\uDC00b", "group:\uDBFFx" })
        {
            Assert.False(DfsIdentity.TryParse(text, out _));
            Assert.Throws<FormatException>(() => DfsIdentity.Parse(text));
        }

        Assert.Throws<ArgumentException>(() => DfsIdentity.User("\uD800"));
        Assert.Throws<ArgumentException>(() => DfsIdentity.Group("a\uDC00"));
    }

    [Fact]
    public void NullHandling()
    {
        Assert.False(DfsIdentity.TryParse(null, out _));
        Assert.Throws<ArgumentNullException>(() => DfsIdentity.Parse(null!));
        Assert.Throws<ArgumentNullException>(() => DfsIdentity.User(null!));
        Assert.Throws<ArgumentNullException>(() => new DfsIdentity(DfsIdentityKind.WellKnown, null!));
        Assert.False(DfsIdentity.IsValidName(null));
    }

    [Fact]
    public void ConstructorValidates()
    {
        Assert.Throws<ArgumentException>(() => DfsIdentity.User(""));
        Assert.Throws<ArgumentException>(() => DfsIdentity.User("a:b"));
        Assert.Throws<ArgumentException>(() => DfsIdentity.Group("x\u0000"));
        Assert.Throws<ArgumentException>(() => DfsIdentity.User(new string('y', 256)));
        Assert.Throws<ArgumentException>(() => new DfsIdentity(DfsIdentityKind.WellKnown, "Bogus"));
        Assert.Throws<ArgumentException>(() => new DfsIdentity(DfsIdentityKind.WellKnown, "everyone"));
        Assert.Throws<ArgumentException>(() => new DfsIdentity((DfsIdentityKind)7, "x"));
        Assert.True(DfsIdentity.IsValidName("alice"));
        Assert.False(DfsIdentity.IsValidName("a:b"));
        Assert.False(DfsIdentity.IsValidName(""));
    }

    [Fact]
    public void EqualityIsOrdinalAndKindSensitive()
    {
        Assert.Equal(DfsIdentity.User("a"), DfsIdentity.Parse("user:a"));
        Assert.NotEqual(DfsIdentity.User("a"), DfsIdentity.Group("a"));
        Assert.NotEqual(DfsIdentity.User("a"), DfsIdentity.User("A"));
        // Names are not Unicode-normalized: they name principals exactly.
        Assert.NotEqual(DfsIdentity.User("é"), DfsIdentity.User("é"));
        Assert.NotEqual(DfsIdentity.Everyone, DfsIdentity.User("Everyone"));
        Assert.Equal(DfsIdentity.User("x").GetHashCode(), DfsIdentity.Parse("user:x").GetHashCode());
    }

    [Fact]
    public void DefaultIdentityIsRecognisable()
    {
        DfsIdentity id = default;
        Assert.True(id.IsDefault);
        Assert.False(id.IsWellKnown);
        Assert.Equal(string.Empty, id.ToString());
        Assert.NotEqual(DfsIdentity.User("a"), id);
    }
}
