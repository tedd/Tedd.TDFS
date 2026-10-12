using System.Security.AccessControl;
using System.Text;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;
using static Tedd.Tdfs.Tests.Unit.Acl.AclHelpers;
using static Tedd.Tdfs.Tests.Unit.Acl.AclOperationMatrix;

namespace Tedd.Tdfs.Tests.Unit.Acl;

/// <summary>
/// ACL — NTFS-like permissions (FSD Addendum A, ACL-01..ACL-08), unit level. Cluster decisions are cross-checked against
/// the independent <see cref="NtfsAclReference"/> (eager NTFS propagation + AccessCheck), never against
/// <see cref="DfsAccessEvaluator"/>.
/// </summary>
public sealed class AclUnitTests
{
    private static readonly DfsIdentity Everyone = DfsIdentity.Everyone;
    private static readonly DfsIdentity Administrators = DfsIdentity.Administrators;
    private static readonly DfsIdentity CreatorOwner = DfsIdentity.CreatorOwner;
    private const DfsFileSystemRights Read = DfsFileSystemRights.Read;
    private const DfsFileSystemRights Write = DfsFileSystemRights.Write;
    private const DfsFileSystemRights Modify = DfsFileSystemRights.Modify;

    // =============================================================================================================
    // ACL-001
    // =============================================================================================================

    private static readonly DfsSecurityDescriptor CustomRoot = new(AdminId,
        [Allow(Administrators, FC, CIOI), Deny(U("mallory"), FC, CIOI), Allow(Everyone, Read, CIOI)], AreAccessRulesProtected: true);

    /// <summary>
    /// A new volume root is owned by its creator with exactly <c>Everyone: FullControl (CI|OI)</c>; new files and
    /// directories show that entry as inherited from <c>/</c> with the NTFS-propagated flags; a volume created by another
    /// administrator is owned by that administrator; a volume spec's RootSecurity replaces the default exactly.
    /// </summary>
    [Fact]
    [Catalog("ACL-001", "U", "INV-12")]
    public Task ACL_001_DefaultRootDescriptor() =>
        ClusterTest.RunAsync(Options(Volume("custom") with { RootSecurity = CustomRoot }), async cluster =>
        {
            await using AclWorld w = await AclWorld.CreateAsync(cluster);

            DfsSecurityDescriptor root = await w.Admin.Directories.GetAccessControlAsync("/", Ct);
            Assert.Equal(AdminId, root.Owner);
            Assert.False(root.AreAccessRulesProtected);
            DfsAccessRule rootRule = Assert.Single(root.Rules);
            Assert.Equal(Everyone, rootRule.Identity);
            Assert.Equal(FC, rootRule.Rights);
            Assert.Equal(DfsAccessControlType.Allow, rootRule.Type);
            Assert.Equal(CIOI, rootRule.Inheritance);
            Assert.Equal(DfsPropagationFlags.None, rootRule.Propagation);
            Assert.False(rootRule.IsInherited);
            Assert.Equal(DfsSecurityDescriptor.DefaultRoot(AdminId), root);

            await w.MkfileAsync("/f.txt", TestData.Bytes(101, 100));
            DfsSecurityDescriptor file = await w.Admin.Files.GetAccessControlAsync("/f.txt", Ct);
            Assert.Equal(AdminId, file.Owner);
            Assert.False(file.AreAccessRulesProtected);
            Assert.Empty(file.ExplicitRules);
            DfsAccessRule fileRule = Assert.Single(file.Rules);
            Assert.True(fileRule.IsInherited, "the file's entry must be reported as inherited");
            Assert.Equal("/", fileRule.InheritedFrom);
            Assert.Equal(Everyone, fileRule.Identity);
            Assert.Equal(FC, fileRule.Rights);
            Assert.Equal(DfsAccessControlType.Allow, fileRule.Type);
            Assert.Equal(DfsInheritanceFlags.None, fileRule.Inheritance);
            Assert.Equal(DfsPropagationFlags.None, fileRule.Propagation);

            await w.MkdirAsync("/d");
            DfsSecurityDescriptor dir = await w.Admin.Directories.GetAccessControlAsync("/d", Ct);
            DfsAccessRule dirRule = Assert.Single(dir.Rules);
            Assert.True(dirRule.IsInherited);
            Assert.Equal("/", dirRule.InheritedFrom);
            Assert.Equal(CIOI, dirRule.Inheritance);
            Assert.Equal(FC, dirRule.Rights);

            AclUser alice = await w.AddUserAsync("alice");
            await alice.Volume.Files.WriteAllBytesAsync("/d/a.bin", TestData.Bytes(102, 10), Ct);
            w.Ref.AddFile("/d/a.bin", alice.Identity);
            DfsSecurityDescriptor aliceFile = await w.Admin.Files.GetAccessControlAsync("/d/a.bin", Ct);
            Assert.Equal(alice.Identity, aliceFile.Owner);
            Assert.Equal("/", Assert.Single(aliceFile.Rules).InheritedFrom);

            await w.CrossCheckAsync(["/", "/f.txt", "/d", "/d/a.bin"], [AdminName, "alice"]);
            Assert.Equal(FC, await w.Admin.Files.GetEffectiveRightsAsync("/f.txt", null, Ct));

            // A volume created by another administrator is owned by that administrator.
            AclUser ops = await w.AddAdminUserAsync("ops");
            await ops.Client.Admin.CreateVolumeAsync(new DfsVolumeSpec
            {
                Name = "opsvol",
                Metadata = new DfsMetadataPolicy { Voters = 1 },
                DefaultPolicy = cluster.Options.GetEffectiveDefaultPolicy(),
            }, Ct);
            await cluster.Admin.GrantAsync("opsvol", "ops", DfsPermissions.All, Ct);
            DfsSecurityDescriptor opsRoot = await ops.Client.GetVolume("opsvol").Directories.GetAccessControlAsync("/", Ct);
            Assert.Equal(DfsSecurityDescriptor.DefaultRoot(U("ops")), opsRoot);

            // A volume spec that supplies a root descriptor gets exactly that descriptor (reported in canonical order:
            // the explicit Deny first, then the Allows in stored order).
            DfsSecurityDescriptor custom = await w.AdminClient.GetVolume("custom").Directories.GetAccessControlAsync("/", Ct);
            Assert.Equal(CustomRoot.Owner, custom.Owner);
            Assert.True(custom.AreAccessRulesProtected);
            Assert.Equal(
            [
                "explicit Deny user:mallory FullControl [CIOI]",
                "explicit Allow Administrators FullControl [CIOI]",
                "explicit Allow Everyone Read [CIOI]",
            ], NtfsAclReference.Describe(custom));
        });

    // =============================================================================================================
    // ACL-002
    // =============================================================================================================
#pragma warning disable CA1416 // System.Security.AccessControl enums are only compared by value (cross-platform); the literal NTFS table below is the primary oracle.

    /// <summary>NTFS access mask values (MS-DTYP / .NET FileSystemRights), written out literally as the oracle.</summary>
    private static readonly Dictionary<string, int> NtfsMask = new(StringComparer.Ordinal)
    {
        ["ReadData"] = 0x1, ["ListDirectory"] = 0x1, ["WriteData"] = 0x2, ["CreateFiles"] = 0x2, ["AppendData"] = 0x4,
        ["CreateDirectories"] = 0x4, ["ReadExtendedAttributes"] = 0x8, ["WriteExtendedAttributes"] = 0x10, ["ExecuteFile"] = 0x20,
        ["Traverse"] = 0x20, ["DeleteSubdirectoriesAndFiles"] = 0x40, ["ReadAttributes"] = 0x80, ["WriteAttributes"] = 0x100,
        ["Delete"] = 0x10000, ["ReadPermissions"] = 0x20000, ["ChangePermissions"] = 0x40000, ["TakeOwnership"] = 0x80000,
        ["Synchronize"] = 0x100000, ["Read"] = 0x20089, ["Write"] = 0x116, ["ReadAndExecute"] = 0x200A9, ["Modify"] = 0x301BF,
        ["FullControl"] = 0x1F01FF,
    };

    public static TheoryData<string> NtfsRightNames => new(NtfsMask.Keys);

    /// <summary>Every DfsFileSystemRights name has the NTFS mask value, equal to System.Security.AccessControl.FileSystemRights.</summary>
    [Theory]
    [MemberData(nameof(NtfsRightNames))]
    [Catalog("ACL-002", "U", "INV-12")]
    public void ACL_002_RightsBitValuesMatchNtfs(string name)
    {
        Assert.True(Enum.TryParse(name, ignoreCase: false, out DfsFileSystemRights dfs), $"DfsFileSystemRights lacks {name}");
        Assert.Equal(NtfsMask[name], (int)dfs);
        Assert.Equal((int)Enum.Parse<FileSystemRights>(name), (int)dfs);
    }

    /// <summary>The enum has exactly the NTFS names; composites are the documented unions; flag and type enums match.</summary>
    [Fact]
    [Catalog("ACL-002", "U", "INV-12")]
    public void ACL_002_RightsBitValuesMatchNtfs_CompositesAndFlags()
    {
        Assert.Equal(Enum.GetNames<FileSystemRights>().Order(StringComparer.Ordinal), Enum.GetNames<DfsFileSystemRights>().Order(StringComparer.Ordinal));
        foreach (string name in Enum.GetNames<FileSystemRights>())
            Assert.Equal((int)Enum.Parse<FileSystemRights>(name), (int)Enum.Parse<DfsFileSystemRights>(name));

        const DfsFileSystemRights singles = DfsFileSystemRights.ReadData | DfsFileSystemRights.WriteData | DfsFileSystemRights.AppendData
            | DfsFileSystemRights.ReadExtendedAttributes | DfsFileSystemRights.WriteExtendedAttributes | DfsFileSystemRights.ExecuteFile
            | DfsFileSystemRights.DeleteSubdirectoriesAndFiles | DfsFileSystemRights.ReadAttributes | DfsFileSystemRights.WriteAttributes
            | DfsFileSystemRights.Delete | DfsFileSystemRights.ReadPermissions | DfsFileSystemRights.ChangePermissions
            | DfsFileSystemRights.TakeOwnership | DfsFileSystemRights.Synchronize;
        Assert.Equal(singles, FC);
        Assert.Equal(DfsFileSystemRights.ReadData | DfsFileSystemRights.ReadExtendedAttributes | DfsFileSystemRights.ReadAttributes | DfsFileSystemRights.ReadPermissions, Read);
        Assert.Equal(DfsFileSystemRights.WriteData | DfsFileSystemRights.AppendData | DfsFileSystemRights.WriteExtendedAttributes | DfsFileSystemRights.WriteAttributes, Write);
        Assert.Equal(Read | DfsFileSystemRights.ExecuteFile, DfsFileSystemRights.ReadAndExecute);
        Assert.Equal(DfsFileSystemRights.ReadAndExecute | Write | DfsFileSystemRights.Delete, Modify);
        Assert.Equal(0, (int)(Modify & DfsFileSystemRights.Synchronize));

        Assert.Equal((int)AccessControlType.Allow, (int)DfsAccessControlType.Allow);
        Assert.Equal((int)AccessControlType.Deny, (int)DfsAccessControlType.Deny);
        Assert.Equal((int)InheritanceFlags.ContainerInherit, (int)DfsInheritanceFlags.ContainerInherit);
        Assert.Equal((int)InheritanceFlags.ObjectInherit, (int)DfsInheritanceFlags.ObjectInherit);
        Assert.Equal((int)PropagationFlags.NoPropagateInherit, (int)DfsPropagationFlags.NoPropagateInherit);
        Assert.Equal((int)PropagationFlags.InheritOnly, (int)DfsPropagationFlags.InheritOnly);
        Assert.Equal("ReadData|WriteData", DfsAccessRule.FormatRights(DfsFileSystemRights.ReadData | DfsFileSystemRights.CreateFiles));
    }
#pragma warning restore CA1416

    // =============================================================================================================
    // ACL-003
    // =============================================================================================================

    private static readonly Dictionary<string, (string Text, DfsIdentityKind Kind, string Name)> ValidIdentityCases = new(StringComparer.Ordinal)
    {
        ["user"] = ("user:alice", DfsIdentityKind.User, "alice"),
        ["group"] = ("group:eng", DfsIdentityKind.Group, "eng"),
        ["everyone"] = ("Everyone", DfsIdentityKind.WellKnown, "Everyone"),
        ["authenticated-users"] = ("AuthenticatedUsers", DfsIdentityKind.WellKnown, "AuthenticatedUsers"),
        ["administrators"] = ("Administrators", DfsIdentityKind.WellKnown, "Administrators"),
        ["creator-owner"] = ("CreatorOwner", DfsIdentityKind.WellKnown, "CreatorOwner"),
        ["non-ascii"] = ("user:Ålesund", DfsIdentityKind.User, "Ålesund"),
        ["space-and-punctuation"] = ("group:dev ops.team-1_@x", DfsIdentityKind.Group, "dev ops.team-1_@x"),
        ["decomposed-kept-as-is"] = ("user:é", DfsIdentityKind.User, "é"),
        ["max-255-ascii"] = ("user:" + new string('a', 255), DfsIdentityKind.User, new string('a', 255)),
        ["max-255-multibyte"] = ("user:" + string.Concat(Enumerable.Repeat("é", 127)) + "a", DfsIdentityKind.User, string.Concat(Enumerable.Repeat("é", 127)) + "a"),
        ["surrogate-pair"] = ("group:\U0001F600", DfsIdentityKind.Group, "\U0001F600"),
        ["user-named-like-well-known"] = ("user:Everyone", DfsIdentityKind.User, "Everyone"),
        ["group-named-user"] = ("group:user", DfsIdentityKind.Group, "user"),
    };

    private static readonly Dictionary<string, string> InvalidIdentityCases = new(StringComparer.Ordinal)
    {
        ["empty"] = "",
        ["bare-name"] = "alice",
        ["empty-user-name"] = "user:",
        ["empty-group-name"] = "group:",
        ["lowercase-well-known"] = "everyone",
        ["uppercase-well-known"] = "ADMINISTRATORS",
        ["spaced-well-known"] = "Creator Owner",
        ["trailing-space"] = "Everyone ",
        ["leading-space"] = " Everyone",
        ["capitalised-prefix"] = "User:alice",
        ["uppercase-prefix"] = "GROUP:eng",
        ["unknown-kind-role"] = "role:admins",
        ["unknown-kind-service"] = "service:backup",
        ["unknown-kind-machine"] = "machine:n0",
        ["colon-in-user-name"] = "user:a:b",
        ["colon-in-group-name"] = "group:a:b",
        ["nul"] = "user:a\0b",
        ["tab"] = "user:a\tb",
        ["newline"] = "group:eng\n",
        ["del-control"] = "user:\u007f",
        ["c1-control"] = "user:x\u0085",
        ["256-ascii-bytes"] = "user:" + new string('a', 256),
        ["256-multibyte-bytes"] = "user:" + string.Concat(Enumerable.Repeat("é", 128)),
        ["lone-high-surrogate"] = "user:\ud800",
        ["lone-low-surrogate"] = "group:x\udc00",
    };

    public static TheoryData<string> ValidIdentityKeys => new(ValidIdentityCases.Keys);

    public static TheoryData<string> InvalidIdentityKeys => new(InvalidIdentityCases.Keys);

    /// <summary>Valid text forms parse to the right kind and name and format back to the same text.</summary>
    [Theory]
    [MemberData(nameof(ValidIdentityKeys))]
    [Catalog("ACL-003", "U", "INV-12")]
    public void ACL_003_IdentityParsingAndWellKnowns(string caseKey)
    {
        (string text, DfsIdentityKind kind, string name) = ValidIdentityCases[caseKey];
        DfsIdentity id = DfsIdentity.Parse(text);
        Assert.Equal(kind, id.Kind);
        Assert.Equal(name, id.Name);
        Assert.Equal(text, id.ToString());
        Assert.False(id.IsDefault);
        Assert.Equal(kind == DfsIdentityKind.WellKnown, id.IsWellKnown);
        Assert.True(DfsIdentity.TryParse(text, out DfsIdentity again));
        Assert.Equal(id, again);
        Assert.Equal(id, DfsIdentity.Parse(id.ToString()));
        switch (kind)
        {
            case DfsIdentityKind.User:
                Assert.Equal(DfsIdentity.User(name), id);
                Assert.True(DfsIdentity.IsValidName(name));
                Assert.NotEqual(DfsIdentity.Group(name), id);
                break;
            case DfsIdentityKind.Group:
                Assert.Equal(DfsIdentity.Group(name), id);
                Assert.True(DfsIdentity.IsValidName(name));
                Assert.NotEqual(DfsIdentity.User(name), id);
                break;
            default:
                DfsIdentity expected = name switch
                {
                    DfsIdentity.EveryoneName => DfsIdentity.Everyone,
                    DfsIdentity.AuthenticatedUsersName => DfsIdentity.AuthenticatedUsers,
                    DfsIdentity.AdministratorsName => DfsIdentity.Administrators,
                    _ => DfsIdentity.CreatorOwner,
                };
                Assert.Equal(expected, id);
                break;
        }
    }

    /// <summary>Malformed text, unknown kinds, invalid names and case variants of prefixes/well-knowns are rejected.</summary>
    [Theory]
    [MemberData(nameof(InvalidIdentityKeys))]
    [Catalog("ACL-003", "U", "INV-12")]
    public void ACL_003_IdentityParsingAndWellKnowns_RejectsMalformed(string caseKey)
    {
        string text = InvalidIdentityCases[caseKey];
        Assert.Throws<FormatException>(() => DfsIdentity.Parse(text));
        Assert.False(DfsIdentity.TryParse(text, out DfsIdentity id), $"TryParse accepted {caseKey}");
        Assert.True(id.IsDefault);
        foreach (string prefix in new[] { DfsIdentity.UserPrefix, DfsIdentity.GroupPrefix })
        {
            if (text.StartsWith(prefix, StringComparison.Ordinal))
            {
                string name = text[prefix.Length..];
                Assert.False(DfsIdentity.IsValidName(name));
                Assert.ThrowsAny<ArgumentException>(() => prefix == DfsIdentity.UserPrefix ? DfsIdentity.User(name) : DfsIdentity.Group(name));
            }
        }
    }

    /// <summary>Constructors validate kind and name; identities compare ordinally without normalization.</summary>
    [Fact]
    [Catalog("ACL-003", "U", "INV-12")]
    public void ACL_003_IdentityParsingAndWellKnowns_ConstructionAndEquality()
    {
        Assert.ThrowsAny<ArgumentException>(() => new DfsIdentity((DfsIdentityKind)7, "x"));
        Assert.ThrowsAny<ArgumentException>(() => new DfsIdentity(DfsIdentityKind.WellKnown, "Nobody"));
        Assert.ThrowsAny<ArgumentException>(() => new DfsIdentity(DfsIdentityKind.WellKnown, "everyone"));
        Assert.ThrowsAny<ArgumentException>(() => DfsIdentity.User(""));
        Assert.ThrowsAny<ArgumentException>(() => DfsIdentity.Group("a:b"));
        Assert.Throws<ArgumentNullException>(() => DfsIdentity.Parse(null!));
        Assert.False(DfsIdentity.TryParse(null, out _));
        Assert.True(default(DfsIdentity).IsDefault);
        Assert.False(default(DfsIdentity).IsWellKnown);
        Assert.Equal(string.Empty, default(DfsIdentity).ToString());
        Assert.NotEqual(DfsIdentity.User("alice"), DfsIdentity.User("Alice"));
        Assert.NotEqual(DfsIdentity.User("é"), DfsIdentity.User("é"));
        Assert.NotEqual(DfsIdentity.User("eng"), DfsIdentity.Group("eng"));
        Assert.Equal(DfsIdentity.Everyone, new DfsIdentity(DfsIdentityKind.WellKnown, DfsIdentity.EveryoneName));
        Assert.Equal(new[] { "Everyone", "AuthenticatedUsers", "Administrators", "CreatorOwner" },
            new[] { DfsIdentity.Everyone, DfsIdentity.AuthenticatedUsers, DfsIdentity.Administrators, DfsIdentity.CreatorOwner }.Select(i => i.ToString()));
    }

    // =============================================================================================================
    // ACL-004 .. ACL-014: evaluation and inheritance matrices (cross-checked against the reference)
    // =============================================================================================================

    /// <summary>
    /// An explicit Deny ReadData for alice beats an explicit Allow FullControl for her group, whatever order the entries
    /// were stored in; the deny is bit-precise (her writes still succeed) and does not affect other group members.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    [Catalog("ACL-004", "U", "INV-08", "INV-12")]
    public Task ACL_004_ExplicitDenyBeatsAllow(bool denyStoredFirst) =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            await using AclWorld w = await AclWorld.CreateAsync(cluster);
            await w.CreateGroupAsync("eng");
            AclUser alice = await w.AddUserAsync("alice", "eng");
            AclUser bob = await w.AddUserAsync("bob", "eng");
            await w.MkdirAsync("/t");
            await w.ProtectAsync("/t");
            byte[] content = await w.MkfileAsync("/t/f", TestData.Bytes(401, 5000));
            DfsAccessRule deny = Deny(alice.Identity, DfsFileSystemRights.ReadData);
            DfsAccessRule allow = Allow(G("eng"), FC);
            await w.SetAclAsync("/t/f", false, denyStoredFirst ? [deny, allow] : [allow, deny]);

            Assert.False(w.Ref.Can("/t/f", "alice", DfsFileSystemRights.ReadData), "reference: deny must win");
            await AssertDeniedAsync(() => alice.Volume.Files.ReadAllBytesAsync("/t/f", Ct), "explicit Deny ReadData for alice");
            await AssertDeniedAsync(() => alice.Volume.Files.OpenReadAsync("/t/f", Ct), "explicit Deny ReadData for alice (open)");
            await AssertNoHandlesAsync(cluster, Vol, "alice", "denied open");
            await AssertBytesAsync(content, bob.Volume.Files.ReadAllBytesAsync("/t/f", Ct), "bob is only allowed via the group");
            Assert.Equal(FC & ~DfsFileSystemRights.ReadData, await w.Admin.Files.GetEffectiveRightsAsync("/t/f", alice.Identity, Ct));
            Assert.Equal(FC, await w.Admin.Files.GetEffectiveRightsAsync("/t/f", bob.Identity, Ct));

            // Canonical order: GetAccessControl reports the explicit Deny before the explicit Allow.
            DfsSecurityDescriptor acl = await w.Admin.Files.GetAccessControlAsync("/t/f", Ct);
            Assert.Equal(DfsAccessControlType.Deny, acl.ExplicitRules[0].Type);
            Assert.Equal(DfsAccessControlType.Allow, acl.ExplicitRules[1].Type);
            await w.CrossCheckAsync(["/t", "/t/f"], [AdminName, "alice", "bob"]);

            // The deny is bit-precise: alice still writes through the group's FullControl.
            byte[] next = TestData.Bytes(402, 4100);
            await alice.Volume.Files.WriteAllBytesAsync("/t/f", next, Ct);
            await AssertBytesAsync(next, bob.Volume.Files.ReadAllBytesAsync("/t/f", Ct), "alice's write");
        });

    /// <summary>Explicit Allow on a child beats an inherited Deny; explicit Deny on a child beats an inherited Allow.</summary>
    [Fact]
    [Catalog("ACL-005", "U", "INV-08", "INV-12")]
    public Task ACL_005_CanonicalOrderInheritedVsExplicit() =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            await using AclWorld w = await AclWorld.CreateAsync(cluster);
            await w.AddUserAsync("alice");
            await w.MkdirAsync("/t");
            await w.ProtectAsync("/t", Allow(U("alice"), Read, CIOI));
            await w.MkdirAsync("/t/p");
            await w.SetAclAsync("/t/p", false, Deny(U("alice"), DfsFileSystemRights.ReadData, CIOI));
            await w.MkfileAsync("/t/p/a", TestData.Bytes(501, 300));
            await w.MkfileAsync("/t/p/b", TestData.Bytes(502, 300));
            await w.SetAclAsync("/t/p/a", false, Allow(U("alice"), DfsFileSystemRights.ReadData));
            await w.MkdirAsync("/t/q");
            await w.SetAclAsync("/t/q", false, Allow(U("alice"), Read, CIOI));
            await w.MkfileAsync("/t/q/c", TestData.Bytes(503, 300));
            await w.MkfileAsync("/t/q/d", TestData.Bytes(504, 300));
            await w.SetAclAsync("/t/q/c", false, Deny(U("alice"), DfsFileSystemRights.ReadData));

            var expected = new Dictionary<string, bool> { ["/t/p/a"] = true, ["/t/p/b"] = false, ["/t/q/c"] = false, ["/t/q/d"] = true };
            foreach ((string path, bool allowed) in expected)
            {
                Assert.True(allowed == w.Ref.Can(path, "alice", DfsFileSystemRights.ReadData), $"reference disagrees with the canonical-order expectation for {path}");
                await w.AssertReadDecisionAsync("alice", path, await w.Admin.Files.ReadAllBytesAsync(path, Ct));
            }

            DfsSecurityDescriptor a = await w.Admin.Files.GetAccessControlAsync("/t/p/a", Ct);
            Assert.False(a.Rules[0].IsInherited);
            Assert.Equal(DfsAccessControlType.Allow, a.Rules[0].Type);
            Assert.Equal("/t/p", a.Rules[1].InheritedFrom);
            Assert.Equal(DfsAccessControlType.Deny, a.Rules[1].Type);
            await w.CrossCheckAsync(w.Ref.AllPaths().Where(p => p.StartsWith("/t", StringComparison.Ordinal)), [AdminName, "alice"]);
        });

    /// <summary>
    /// Inherited entries are evaluated generation by generation, nearest ancestor first (NTFS canonical order): a parent's
    /// Allow beats a grandparent's Deny and vice versa, per right bit.
    /// </summary>
    [Fact]
    [Catalog("ACL-006", "U", "INV-08", "INV-12")]
    public Task ACL_006_NearestAncestorWins() =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            await using AclWorld w = await AclWorld.CreateAsync(cluster);
            await w.AddUserAsync("alice");
            await w.MkdirAsync("/t");
            await w.ProtectAsync("/t");
            DfsAccessRule A(DfsFileSystemRights r) => Allow(U("alice"), r, CIOI);
            DfsAccessRule D(DfsFileSystemRights r) => Deny(U("alice"), r, CIOI);
            const DfsFileSystemRights rd = DfsFileSystemRights.ReadData, wd = DfsFileSystemRights.WriteData;

            // (chain from farthest to nearest ancestor, expected read, expected write)
            var chains = new (DfsAccessRule?[] Chain, bool Read, bool Write)[]
            {
                ([A(Read), D(Read)], false, false),
                ([D(Read), A(Read)], true, false),
                ([D(Read), A(Read), null], true, false),
                ([A(Read), D(Read), null], false, false),
                ([A(rd | wd), D(wd)], true, false),
                ([D(rd | wd), A(wd), null], false, true),
                ([A(Read), null, D(rd), A(rd)], true, false),
            };
            var files = new List<(string Path, bool Read, bool Write, byte[] Content)>();
            for (int i = 0; i < chains.Length; i++)
            {
                string dir = "/t/c" + i;
                await w.MkdirAsync(dir);
                for (int level = 0; level < chains[i].Chain.Length; level++)
                {
                    if (level > 0)
                    {
                        dir += "/l" + level;
                        await w.MkdirAsync(dir);
                    }

                    if (chains[i].Chain[level] is { } rule)
                        await w.SetAclAsync(dir, false, rule);
                }

                byte[] content = await w.MkfileAsync(dir + "/f", TestData.Bytes((ulong)(600 + i), 200));
                files.Add((dir + "/f", chains[i].Read, chains[i].Write, content));
            }

            foreach ((string path, bool read, bool write, byte[] content) in files)
            {
                Assert.True(read == w.Ref.Can(path, "alice", rd), $"reference read decision for {path}");
                Assert.True(write == w.Ref.Can(path, "alice", wd), $"reference write decision for {path}");
                await w.AssertReadDecisionAsync("alice", path, content);
                if (write)
                {
                    await w["alice"].Volume.Files.WriteAllBytesAsync(path, content, Ct);
                }
                else
                {
                    await AssertDeniedAsync(() => w["alice"].Volume.Files.WriteAllBytesAsync(path, TestData.Bytes(699, 10), Ct), $"write {path}");
                    await AssertBytesAsync(content, w.Admin.Files.ReadAllBytesAsync(path, Ct), "denied write left the data");
                }

                // Inherited entries are reported nearest ancestor first.
                DfsSecurityDescriptor acl = await w.Admin.Files.GetAccessControlAsync(path, Ct);
                string[] sources = acl.Rules.Where(r => r.IsInherited).Select(r => r.InheritedFrom!).Distinct().ToArray();
                Assert.Equal(sources.OrderByDescending(s => s.Length), sources);
            }

            await w.CrossCheckAsync(w.Ref.AllPaths().Where(p => p.StartsWith("/t", StringComparison.Ordinal)), [AdminName, "alice"]);
        });

    /// <summary>A request for ReadData|WriteData with only ReadData allowed is denied whole: no handle, no change.</summary>
    [Fact]
    [Catalog("ACL-007", "U", "INV-08", "INV-12")]
    public Task ACL_007_PartialRightsAreNotGranted() =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            await using AclWorld w = await AclWorld.CreateAsync(cluster);
            AclUser alice = await w.AddUserAsync("alice");
            await w.MkdirAsync("/t");
            await w.ProtectAsync("/t");
            byte[] content = await w.MkfileAsync("/t/f", TestData.Bytes(701, 6000));
            await w.SetAclAsync("/t/f", false, Allow(alice.Identity, DfsFileSystemRights.ReadData));
            Assert.Equal(DfsFileSystemRights.ReadData, await w.Admin.Files.GetEffectiveRightsAsync("/t/f", alice.Identity, Ct));
            await w.AssertRightsMatchAsync("/t/f", "alice");
            string before = await DigestAsync(w.Admin, "/t");
            InodeId inode = (await w.Admin.Files.GetInfoAsync("/t/f", Ct)).InodeId;

            var requests = new (FileMode Mode, FileAccess Access)[]
            {
                (FileMode.Open, FileAccess.ReadWrite), (FileMode.Open, FileAccess.Write), (FileMode.OpenOrCreate, FileAccess.ReadWrite),
                (FileMode.Append, FileAccess.Write), (FileMode.Truncate, FileAccess.Write), (FileMode.Create, FileAccess.ReadWrite),
            };
            foreach ((FileMode mode, FileAccess access) in requests)
            {
                await AssertDeniedAsync(async () =>
                {
                    await using DfsStream s = await alice.Volume.Files.OpenAsync("/t/f", mode, access, FileShare.Read, Ct);
                }, $"open {mode}/{access} with only ReadData");
                Assert.Empty(await HandlesOfAsync(cluster, Vol, "alice", inode));
            }

            Assert.Equal(before, await DigestAsync(w.Admin, "/t"));

            await using (DfsStream reader = await alice.Volume.Files.OpenAsync("/t/f", FileMode.Open, FileAccess.Read, FileShare.Read, Ct))
            {
                HandleInspection handle = Assert.Single(await HandlesOfAsync(cluster, Vol, "alice", inode));
                Assert.Equal(FileAccess.Read, handle.Access);
                Assert.False(handle.IsWriter);
                byte[] read = await ReadToEndAsync(reader);
                AssertEx.BytesEqual(content, read, "the read-only open");
            }
        });

    /// <summary>ObjectInherit reaches files only, ContainerInherit directories only, both reach both (at every depth).</summary>
    [Fact]
    [Catalog("ACL-008", "U", "INV-08", "INV-12")]
    public Task ACL_008_ObjectVsContainerInherit() =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            await using AclWorld w = await AclWorld.CreateAsync(cluster);
            await w.AddUserAsync("alice");
            await w.MkdirAsync("/t");
            await w.ProtectAsync("/t");
            var expected = new Dictionary<string, bool>(StringComparer.Ordinal);
            foreach ((string name, DfsInheritanceFlags flags, bool files, bool dirs) in new[]
                     { ("oi", OI, true, false), ("ci", CI, false, true), ("both", CIOI, true, true) })
            {
                string d = "/t/" + name;
                await BuildTreeAsync(w, d);
                await w.SetAclAsync(d, false, Allow(U("alice"), Read, flags));
                expected[d] = true;
                expected[d + "/f"] = files;
                expected[d + "/s"] = dirs;
                expected[d + "/s/g"] = files;
                expected[d + "/s/s2"] = dirs;
            }

            foreach ((string path, bool allowed) in expected)
            {
                bool dir = w.IsDirectory(path);
                Assert.True(allowed == w.Ref.Can(path, "alice", DfsFileSystemRights.ReadData), $"reference decision for {path}");
                await w.AssertReadDecisionAsync("alice", path, dir ? null : await w.Admin.Files.ReadAllBytesAsync(path, Ct));
            }

            DfsSecurityDescriptor template = await w.Admin.Directories.GetAccessControlAsync("/t/oi/s", Ct);
            DfsAccessRule passOn = Assert.Single(template.Rules, r => r.Identity == U("alice"));
            Assert.False(passOn.AppliesToObject, "an OI-only entry reaches a subdirectory only as an inherit-only template");
            Assert.Equal(OI, passOn.Inheritance);
            await w.CrossCheckAsync(expected.Keys, [AdminName, "alice"]);
        });

    /// <summary>An InheritOnly entry grants nothing on the directory it is set on but applies to its children.</summary>
    [Fact]
    [Catalog("ACL-009", "U", "INV-08", "INV-12")]
    public Task ACL_009_InheritOnlyNotApplied() =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            await using AclWorld w = await AclWorld.CreateAsync(cluster);
            await w.AddUserAsync("alice");
            await w.MkdirAsync("/t");
            await w.ProtectAsync("/t");
            await BuildTreeAsync(w, "/t/io");
            await w.SetAclAsync("/t/io", false, Allow(U("alice"), Read, CIOI, IO));
            await BuildTreeAsync(w, "/t/io2");
            await w.SetAclAsync("/t/io2", false, Allow(U("alice"), Read, OI, IO));
            var expected = new Dictionary<string, bool>(StringComparer.Ordinal)
            {
                ["/t/io"] = false, ["/t/io/f"] = true, ["/t/io/s"] = true, ["/t/io/s/g"] = true, ["/t/io/s/s2"] = true,
                ["/t/io2"] = false, ["/t/io2/f"] = true, ["/t/io2/s"] = false, ["/t/io2/s/g"] = true, ["/t/io2/s/s2"] = false,
            };
            foreach ((string path, bool allowed) in expected)
            {
                Assert.True(allowed == w.Ref.Can(path, "alice", DfsFileSystemRights.ReadData), $"reference decision for {path}");
                await w.AssertReadDecisionAsync("alice", path, w.IsDirectory(path) ? null : await w.Admin.Files.ReadAllBytesAsync(path, Ct));
            }

            Assert.Equal(0, (int)(await w.Admin.Directories.GetEffectiveRightsAsync("/t/io", U("alice"), Ct) & Read & ~DfsFileSystemRights.Synchronize));
            DfsAccessRule own = Assert.Single((await w.Admin.Directories.GetAccessControlAsync("/t/io", Ct)).ExplicitRules, r => r.Identity == U("alice"));
            Assert.False(own.AppliesToObject);
            await w.CrossCheckAsync(expected.Keys, [AdminName, "alice"]);
        });

    /// <summary>A NoPropagateInherit entry reaches immediate children but not grandchildren.</summary>
    [Fact]
    [Catalog("ACL-010", "U", "INV-08", "INV-12")]
    public Task ACL_010_NoPropagateInherit() =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            await using AclWorld w = await AclWorld.CreateAsync(cluster);
            await w.AddUserAsync("alice");
            await w.MkdirAsync("/t");
            await w.ProtectAsync("/t");
            await BuildTreeAsync(w, "/t/np");
            await w.SetAclAsync("/t/np", false, Allow(U("alice"), Read, CIOI, NP));
            await BuildTreeAsync(w, "/t/np2");
            await w.SetAclAsync("/t/np2", false, Allow(U("alice"), Read, CIOI, NP | IO));
            var expected = new Dictionary<string, bool>(StringComparer.Ordinal)
            {
                ["/t/np"] = true, ["/t/np/f"] = true, ["/t/np/s"] = true, ["/t/np/s/g"] = false, ["/t/np/s/s2"] = false,
                ["/t/np2"] = false, ["/t/np2/f"] = true, ["/t/np2/s"] = true, ["/t/np2/s/g"] = false, ["/t/np2/s/s2"] = false,
            };
            foreach ((string path, bool allowed) in expected)
            {
                Assert.True(allowed == w.Ref.Can(path, "alice", DfsFileSystemRights.ReadData), $"reference decision for {path}");
                await w.AssertReadDecisionAsync("alice", path, w.IsDirectory(path) ? null : await w.Admin.Files.ReadAllBytesAsync(path, Ct));
            }

            DfsAccessRule child = Assert.Single((await w.Admin.Directories.GetAccessControlAsync("/t/np/s", Ct)).Rules, r => r.Identity == U("alice"));
            Assert.True(child.IsInherited);
            Assert.Equal(DfsInheritanceFlags.None, child.Inheritance);
            Assert.Equal(DfsPropagationFlags.None, child.Propagation);
            await w.CrossCheckAsync(expected.Keys, [AdminName, "alice"]);
        });

    /// <summary>
    /// Protecting a descriptor removes every inherited entry from evaluation for that subtree (allows and denies alike);
    /// clearing the flag restores inheritance.
    /// </summary>
    [Fact]
    [Catalog("ACL-011", "U", "INV-08", "INV-12")]
    public Task ACL_011_ProtectedDescriptorBlocksInheritance() =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            await using AclWorld w = await AclWorld.CreateAsync(cluster);
            await w.AddUserAsync("alice");
            await w.AddUserAsync("bob");
            await w.MkdirAsync("/t");
            await w.ProtectAsync("/t", Allow(U("alice"), Read, CIOI), Deny(U("bob"), Read, CIOI));
            await BuildTreeAsync(w, "/t/p");
            string[] subtree = ["/t/p", "/t/p/f", "/t/p/s", "/t/p/s/g"];

            async Task CheckAsync(bool aliceAllowed, bool bobAllowed, string phase)
            {
                foreach (string path in subtree)
                {
                    Assert.True(aliceAllowed == w.Ref.Can(path, "alice", DfsFileSystemRights.ReadData), $"reference alice {path} {phase}");
                    Assert.True(bobAllowed == w.Ref.Can(path, "bob", DfsFileSystemRights.ReadData), $"reference bob {path} {phase}");
                    byte[]? content = w.IsDirectory(path) ? null : await w.Admin.Files.ReadAllBytesAsync(path, Ct);
                    await w.AssertReadDecisionAsync("alice", path, content);
                    await w.AssertReadDecisionAsync("bob", path, content);
                }

                await w.CrossCheckAsync(subtree, [AdminName, "alice", "bob"]);
            }

            await CheckAsync(aliceAllowed: true, bobAllowed: false, "inheriting");

            await w.SetAclAsync("/t/p", true, AdminsFull, Allow(U("bob"), Read, CIOI));
            await CheckAsync(aliceAllowed: false, bobAllowed: true, "protected");
            DfsSecurityDescriptor p = await w.Admin.Directories.GetAccessControlAsync("/t/p", Ct);
            Assert.True(p.AreAccessRulesProtected);
            Assert.DoesNotContain(p.Rules, r => r.IsInherited);
            DfsSecurityDescriptor f = await w.Admin.Files.GetAccessControlAsync("/t/p/f", Ct);
            Assert.All(f.Rules.Where(r => r.IsInherited), r => Assert.Equal("/t/p", r.InheritedFrom));

            await w.SetAclAsync("/t/p", false, Allow(U("bob"), Read, CIOI));
            await CheckAsync(aliceAllowed: true, bobAllowed: true, "unprotected again (bob's explicit allow at /t/p precedes the deny from /t)");
        });

    /// <summary>
    /// An inheritable CreatorOwner entry grants its rights to each inheriting object's current owner and to nobody else;
    /// directories keep an inherit-only CreatorOwner template; explicit CreatorOwner entries match nobody.
    /// </summary>
    [Fact]
    [Catalog("ACL-012", "U", "INV-08", "INV-12")]
    public Task ACL_012_CreatorOwnerSubstitution() =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            await using AclWorld w = await AclWorld.CreateAsync(cluster);
            await w.CreateGroupAsync("eng");
            AclUser alice = await w.AddUserAsync("alice", "eng");
            AclUser bob = await w.AddUserAsync("bob", "eng");
            await w.MkdirAsync("/t");
            await w.ProtectAsync("/t");
            await w.MkdirAsync("/t/co");
            const DfsFileSystemRights create = DfsFileSystemRights.CreateFiles | DfsFileSystemRights.CreateDirectories
                | DfsFileSystemRights.ListDirectory | DfsFileSystemRights.ReadAttributes;
            await w.ProtectAsync("/t/co", Allow(CreatorOwner, FC, CIOI), Allow(G("eng"), create));

            byte[] a = TestData.Bytes(1201, 700), b = TestData.Bytes(1202, 800), x = TestData.Bytes(1203, 900);
            await alice.Volume.Files.WriteAllBytesAsync("/t/co/a.txt", a, Ct);
            w.Ref.AddFile("/t/co/a.txt", alice.Identity);
            await bob.Volume.Files.WriteAllBytesAsync("/t/co/b.txt", b, Ct);
            w.Ref.AddFile("/t/co/b.txt", bob.Identity);
            await alice.Volume.Directories.CreateDirectoryAsync("/t/co/ad", Ct);
            w.Ref.AddDirectory("/t/co/ad", alice.Identity);
            await alice.Volume.Files.WriteAllBytesAsync("/t/co/ad/x.txt", x, Ct);
            w.Ref.AddFile("/t/co/ad/x.txt", alice.Identity);

            await AssertBytesAsync(a, alice.Volume.Files.ReadAllBytesAsync("/t/co/a.txt", Ct), "creator reads its file");
            await AssertBytesAsync(b, bob.Volume.Files.ReadAllBytesAsync("/t/co/b.txt", Ct), "creator reads its file");
            await AssertDeniedAsync(() => bob.Volume.Files.ReadAllBytesAsync("/t/co/a.txt", Ct), "CreatorOwner does not match bob on alice's file");
            await AssertDeniedAsync(() => alice.Volume.Files.ReadAllBytesAsync("/t/co/b.txt", Ct), "CreatorOwner does not match alice on bob's file");
            await AssertBytesAsync(x, alice.Volume.Files.ReadAllBytesAsync("/t/co/ad/x.txt", Ct), "nested creation through the template");
            await AssertDeniedAsync(() => ToListAsync(bob.Volume.Directories.EnumerateAsync("/t/co/ad", null, Ct)), "bob on alice's directory");

            DfsSecurityDescriptor fileAcl = await w.Admin.Files.GetAccessControlAsync("/t/co/a.txt", Ct);
            Assert.DoesNotContain(fileAcl.Rules, r => r.Identity == CreatorOwner);
            DfsAccessRule substituted = Assert.Single(fileAcl.Rules, r => r.Identity == alice.Identity);
            Assert.True(substituted.IsInherited);
            Assert.Equal(FC, substituted.Rights);
            DfsSecurityDescriptor dirAcl = await w.Admin.Directories.GetAccessControlAsync("/t/co/ad", Ct);
            Assert.Contains(dirAcl.Rules, r => r.Identity == alice.Identity && r.AppliesToObject && r.Inheritance == DfsInheritanceFlags.None);
            Assert.Contains(dirAcl.Rules, r => r.Identity == CreatorOwner && !r.AppliesToObject && r.Inheritance == CIOI);

            // Substitution follows the current owner.
            await w.SetOwnerAsync("/t/co/a.txt", bob.Identity);
            await AssertBytesAsync(a, bob.Volume.Files.ReadAllBytesAsync("/t/co/a.txt", Ct), "new owner");
            await AssertDeniedAsync(() => alice.Volume.Files.ReadAllBytesAsync("/t/co/a.txt", Ct), "former owner");

            // Explicit CreatorOwner entries are never substituted: the owner gets only its implicit rights.
            await w.MkdirAsync("/t/co2", alice.Identity);
            await w.ProtectAsync("/t/co2", Allow(CreatorOwner, Read, CIOI));
            await AssertDeniedAsync(() => ToListAsync(alice.Volume.Directories.EnumerateAsync("/t/co2", null, Ct)), "explicit CreatorOwner matches nobody");
            Assert.Equal(NtfsAclReference.OwnerImplicit, await w.Admin.Directories.GetEffectiveRightsAsync("/t/co2", alice.Identity, Ct));

            await w.CrossCheckAsync(w.Ref.AllPaths().Where(p => p.StartsWith("/t", StringComparison.Ordinal)), [AdminName, "alice", "bob"]);
        });

    /// <summary>
    /// The owner can read and change permissions even when the ACL denies those rights to it (via a group, directly,
    /// or as a member of an owning group); non-owners denied the same rights cannot.
    /// </summary>
    [Fact]
    [Catalog("ACL-013", "U", "INV-08", "INV-12")]
    public Task ACL_013_OwnerImplicitRights() =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            await using AclWorld w = await AclWorld.CreateAsync(cluster);
            await w.CreateGroupAsync("eng");
            AclUser alice = await w.AddUserAsync("alice", "eng");
            AclUser bob = await w.AddUserAsync("bob", "eng");
            AclUser dave = await w.AddUserAsync("dave");
            await w.MkdirAsync("/t");
            await w.ProtectAsync("/t");
            const DfsFileSystemRights rpcp = DfsFileSystemRights.ReadPermissions | DfsFileSystemRights.ChangePermissions;
            byte[] g = await w.MkfileAsync("/t/g.txt", TestData.Bytes(1301, 100), alice.Identity);
            await w.SetAclAsync("/t/g.txt", false, Deny(G("eng"), rpcp));
            await w.MkfileAsync("/t/u.txt", TestData.Bytes(1302, 100), alice.Identity);
            await w.SetAclAsync("/t/u.txt", false, Deny(alice.Identity, FC));
            await w.MkfileAsync("/t/grp.txt", TestData.Bytes(1303, 100), G("eng"));
            await w.SetAclAsync("/t/grp.txt", false, Deny(G("eng"), FC));

            Assert.Equal(rpcp, await w.Admin.Files.GetEffectiveRightsAsync("/t/u.txt", alice.Identity, Ct));
            Assert.Equal(rpcp, await w.Admin.Files.GetEffectiveRightsAsync("/t/g.txt", alice.Identity, Ct));
            Assert.Equal((DfsFileSystemRights)0, await w.Admin.Files.GetEffectiveRightsAsync("/t/g.txt", bob.Identity, Ct));
            Assert.Equal(rpcp, await w.Admin.Files.GetEffectiveRightsAsync("/t/grp.txt", bob.Identity, Ct));
            await w.CrossCheckAsync(["/t/g.txt", "/t/u.txt", "/t/grp.txt"], [AdminName, "alice", "bob", "dave"]);

            // The owner reads the descriptor; a non-owner denied by the same group entry cannot.
            Assert.Equal(alice.Identity, (await alice.Volume.Files.GetAccessControlAsync("/t/g.txt", Ct)).Owner);
            Assert.Equal(alice.Identity, (await alice.Volume.Files.GetAccessControlAsync("/t/u.txt", Ct)).Owner);
            await AssertDeniedAsync(() => bob.Volume.Files.GetAccessControlAsync("/t/g.txt", Ct), "bob is denied ReadPermissions via eng and is not the owner");
            await AssertDeniedAsync(() => alice.Volume.Files.ReadAllBytesAsync("/t/g.txt", Ct), "ownership implies no ReadData");

            // Members of an owning group are owners; outsiders are not.
            Assert.Equal(G("eng"), (await bob.Volume.Files.GetAccessControlAsync("/t/grp.txt", Ct)).Owner);
            await AssertDeniedAsync(() => dave.Volume.Files.GetAccessControlAsync("/t/grp.txt", Ct), "dave is not a member of the owning group");

            // Non-owner cannot change permissions; the owner can, and the change takes effect.
            string before = await DigestAsync(w.Admin, "/t");
            await AssertDeniedAsync(() => bob.Volume.Files.SetAccessControlAsync("/t/g.txt", new DfsSecurityDescriptor(alice.Identity, [Allow(bob.Identity, FC)]), Ct), "bob changing permissions");
            Assert.Equal(before, await DigestAsync(w.Admin, "/t"));
            DfsAccessRule[] newRules = [Deny(G("eng"), rpcp), Allow(alice.Identity, Read)];
            await alice.Volume.Files.SetAccessControlAsync("/t/g.txt", new DfsSecurityDescriptor(alice.Identity, newRules), Ct);
            w.Ref.SetExplicit("/t/g.txt", newRules, isProtected: false);
            await AssertBytesAsync(g, alice.Volume.Files.ReadAllBytesAsync("/t/g.txt", Ct), "after the owner granted itself Read");
            await alice.Volume.Files.SetAccessControlAsync("/t/u.txt", new DfsSecurityDescriptor(alice.Identity, [Deny(alice.Identity, FC)]), Ct);
            await w.CrossCheckAsync(["/t/g.txt", "/t/u.txt", "/t/grp.txt"], [AdminName, "alice", "bob", "dave"]);
        });

    /// <summary>
    /// TakeOwnership lets a user make itself the owner but not anyone else; Administrators may set any owner and always
    /// keep TakeOwnership/ChangePermissions even when explicitly denied; others are denied and nothing changes.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    [Catalog("ACL-014", "U", "INV-08", "INV-12")]
    public Task ACL_014_TakeOwnershipRules(bool directory) =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            await using AclWorld w = await AclWorld.CreateAsync(cluster);
            AclUser alice = await w.AddUserAsync("alice");
            AclUser bob = await w.AddUserAsync("bob");
            AclUser carol = await w.AddAdminUserAsync("carol");
            await w.MkdirAsync("/t");
            await w.ProtectAsync("/t");
            const string path = "/t/x";
            if (directory)
                await w.MkdirAsync(path);
            else
                await w.MkfileAsync(path, TestData.Bytes(1401, 300));
            await w.SetAclAsync(path, false, Allow(alice.Identity, DfsFileSystemRights.TakeOwnership),
                Deny(Administrators, DfsFileSystemRights.TakeOwnership | DfsFileSystemRights.ChangePermissions));
            await w.CrossCheckAsync([path], [AdminName, "alice", "bob", "carol"]);

            async Task<DfsIdentity> OwnerAsync() => (await w.GetAclAsAsync(w.Admin, path)).Owner;
            string before = await DigestAsync(w.Admin, "/t");
            await AssertDeniedAsync(() => w.SetOwnerAsAsync(bob.Volume, path, bob.Identity), "bob has no TakeOwnership");
            await AssertDeniedAsync(() => w.SetOwnerAsAsync(bob.Volume, path, alice.Identity), "bob setting another owner");
            await AssertDeniedAsync(() => w.SetOwnerAsAsync(alice.Volume, path, bob.Identity), "TakeOwnership does not allow giving ownership away");
            Assert.Equal(before, await DigestAsync(w.Admin, "/t"));
            Assert.Equal(AdminId, await OwnerAsync());

            await w.SetOwnerAsAsync(alice.Volume, path, alice.Identity);
            w.Ref.SetOwner(path, alice.Identity);
            Assert.Equal(alice.Identity, await OwnerAsync());

            await w.SetOwnerAsAsync(carol.Volume, path, bob.Identity);
            w.Ref.SetOwner(path, bob.Identity);
            Assert.Equal(bob.Identity, await OwnerAsync());
            await Assert.ThrowsAnyAsync<ArgumentException>(() => w.SetOwnerAsAsync(carol.Volume, path, CreatorOwner));
            Assert.Equal(bob.Identity, await OwnerAsync());

            // Administrators keep ChangePermissions despite the explicit Deny.
            DfsAccessRule[] rules = [Allow(alice.Identity, DfsFileSystemRights.TakeOwnership), Allow(U("dave"), Read)];
            await w.SetAclAsAsync(carol.Volume, path, new DfsSecurityDescriptor(bob.Identity, rules));
            w.Ref.SetExplicit(path, rules, isProtected: false);
            await w.CrossCheckAsync([path], [AdminName, "alice", "bob", "carol"]);
        });

    // =============================================================================================================
    // ACL-015
    // =============================================================================================================

    public static TheoryData<MatrixOp> MatrixOps => new(Enum.GetValues<MatrixOp>());

    /// <summary>
    /// For one operation of the ACL-05 table: principals holding exactly the required right(s) (each alternative of an
    /// OR separately) succeed with the specified effect; principals holding every right except the required one(s) get
    /// UnauthorizedAccessException with namespace, data, streams and descriptors unchanged and no handle. Decisions and
    /// effective rights agree with the reference.
    /// </summary>
    [Theory]
    [MemberData(nameof(MatrixOps))]
    [Catalog("ACL-015", "U", "INV-08", "INV-12")]
    public Task ACL_015_OperationRightMatrix(MatrixOp op) =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            MatrixCase c = AclOperationMatrix.Get(op);
            await using AclWorld w = await AclWorld.CreateAsync(cluster);
            await w.AddUserAsync(Bystander);
            await w.MkdirAsync("/m");
            await w.ProtectAsync("/m");
            var exact = new List<(string User, string Root)>();
            var lacking = new List<(string User, string Root)>();
            for (int i = 0; i < c.Exact.Length; i++)
            {
                string user = "x" + i, root = "/m/x" + i;
                if (c.ExactIsAdministrator)
                    await w.AddAdminUserAsync(user);
                else
                    await w.AddUserAsync(user);
                await BuildAsync(w, root, user, c.Exact[i], c.Extras);
                exact.Add((user, root));
            }

            for (int j = 0; j < c.Lacking.Length; j++)
            {
                string user = "l" + j, root = "/m/l" + j;
                await w.AddUserAsync(user);
                await BuildAsync(w, root, user, c.Lacking[j], c.Extras);
                lacking.Add((user, root));
            }

            bool RefAllows(string user, string root) =>
                c.Rows.All(row => w.Ref.Allows(row, user, TargetOf(op, root), DestinationOf(op, root)));
            foreach ((string user, string root) in exact)
            {
                Assert.True(RefAllows(user, root), $"matrix design: {user} must hold exactly the right for {op}");
                foreach (string p in w.Ref.AllPaths().Where(p => p.StartsWith(root, StringComparison.Ordinal)))
                    await w.AssertRightsMatchAsync(p, user);
            }

            foreach ((string user, string root) in lacking)
            {
                Assert.False(RefAllows(user, root), $"matrix design: {user} must lack the right for {op}");
                foreach (string p in w.Ref.AllPaths().Where(p => p.StartsWith(root, StringComparison.Ordinal)))
                    await w.AssertRightsMatchAsync(p, user);

                string before = await DigestAsync(w.Admin, root);
                await AssertDeniedAsync(() => PerformAsync(op, w[user].Volume, root, user), $"{op} by {user} lacking the required right");
                Assert.Equal(before, await DigestAsync(w.Admin, root));
                await AssertNoHandlesAsync(cluster, Vol, user, $"denied {op}");
            }

            foreach ((string user, string root) in exact)
            {
                var inodes = new Dictionary<string, InodeId>(StringComparer.Ordinal);
                foreach (MNode node in Enum.GetValues<MNode>())
                {
                    string p = PathOf(root, node);
                    inodes[p] = w.IsDirectory(p)
                        ? (await w.Admin.Directories.GetInfoAsync(p, Ct)).InodeId
                        : (await w.Admin.Files.GetInfoAsync(p, Ct)).InodeId;
                }

                object? result = await PerformAsync(op, w[user].Volume, root, user);
                await AssertEffectAndApplyAsync(op, w, root, user, result, inodes);
            }

            await w.CrossCheckAsync(w.Ref.AllPaths().Where(p => p.StartsWith("/m", StringComparison.Ordinal)),
                exact.Select(e => e.User).Concat(lacking.Select(l => l.User)).Append(AdminName));
        });

    /// <summary>
    /// Every operation row of the addendum's ACL-05 table is exercised by the matrix, every matrix case has exact and
    /// lacking grant sets, and on a pure reference model every exact set allows and every lacking set denies.
    /// </summary>
    [Fact]
    [Catalog("ACL-015", "U", "INV-12")]
    public void ACL_015_OperationRightMatrix_CoversEveryTableRow()
    {
        IReadOnlyList<string> rows = SpecTableOperations();
        Assert.Equal(13, rows.Count);
        foreach (string row in rows)
            Assert.True(TableCoverage.ContainsKey(row), $"ACL-05 row '{row}' is not exercised by ACL-015");
        Assert.Equal(rows.Order(StringComparer.Ordinal), TableCoverage.Keys.Order(StringComparer.Ordinal));
        Assert.Equal(Enum.GetValues<MatrixOp>().Order(), Cases.Select(c => c.Op).Order());
        Assert.All(TableCoverage.Values.SelectMany(v => v), op => Assert.Contains(Cases, c => c.Op == op));
        Assert.All(Enum.GetValues<MatrixOp>(), op => Assert.Contains(TableCoverage.Values, ops => ops.Contains(op)));

        foreach (MatrixCase c in Cases)
        {
            Assert.NotEmpty(c.Exact);
            Assert.NotEmpty(c.Lacking);
            var reference = new NtfsAclReference(AdminId);
            reference.AddPrincipal(AdminName, isAdmin: true);
            reference.AddPrincipal(Bystander);
            reference.AddDirectory("/m", AdminId);
            reference.SetExplicit("/m", [AdminsFull], isProtected: true);
            void Build(string root, string user, MGrant[] grants, bool admin)
            {
                reference.AddPrincipal(user, admin);
                reference.AddDirectory(root, AdminId);
                reference.SetExplicit(root, [AdminsFull], isProtected: true);
                foreach (MNode n in new[] { MNode.Src, MNode.Dst })
                    reference.AddDirectory(PathOf(root, n), AdminId);
                reference.AddFile(PathOf(root, MNode.F), AdminId);
                reference.AddFile(PathOf(root, MNode.T), AdminId);
                reference.AddDirectory(PathOf(root, MNode.D), AdminId);
                reference.AddFile(PathOf(root, MNode.G), AdminId);
                foreach ((MNode node, DfsAccessRule[] rules) in RulesByNode(user, grants, c.Extras))
                    reference.SetExplicit(PathOf(root, node), rules, false);
            }

            for (int i = 0; i < c.Exact.Length; i++)
            {
                Build("/m/x" + i, "x" + i, c.Exact[i], c.ExactIsAdministrator);
                Assert.True(c.Rows.All(r => reference.Allows(r, "x" + i, TargetOf(c.Op, "/m/x" + i), DestinationOf(c.Op, "/m/x" + i))), $"{c.Op} exact set {i}");
            }

            for (int j = 0; j < c.Lacking.Length; j++)
            {
                Build("/m/l" + j, "l" + j, c.Lacking[j], false);
                Assert.False(c.Rows.All(r => reference.Allows(r, "l" + j, TargetOf(c.Op, "/m/l" + j), DestinationOf(c.Op, "/m/l" + j))), $"{c.Op} lacking set {j}");
            }
        }
    }

    // =============================================================================================================
    // ACL-016 .. ACL-019
    // =============================================================================================================

    public enum Grant3
    {
        None,
        Allow,
        Deny,
    }

    /// <summary>
    /// Delete is allowed with DeleteSubdirectoriesAndFiles on the parent even when Delete on the entry is not granted or
    /// is explicitly denied, allowed with Delete on the entry, and denied when neither is allowed (nothing changes).
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    [Catalog("ACL-016", "U", "INV-08", "INV-12")]
    public Task ACL_016_DeleteViaParentDeleteChild(bool directory) =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            await using AclWorld w = await AclWorld.CreateAsync(cluster);
            AclUser alice = await w.AddUserAsync("alice");
            await w.MkdirAsync("/t");
            await w.ProtectAsync("/t");
            var combos = new List<(string Parent, string Entry, bool Allowed)>();
            int i = 0;
            foreach (Grant3 onEntry in Enum.GetValues<Grant3>())
            {
                foreach (Grant3 onParent in Enum.GetValues<Grant3>())
                {
                    string parent = "/t/c" + i++, entry = parent + "/x";
                    await w.MkdirAsync(parent);
                    if (directory)
                        await w.MkdirAsync(entry);
                    else
                        await w.MkfileAsync(entry, TestData.Bytes((ulong)(1600 + i), 300));
                    if (onParent != Grant3.None)
                        await w.SetAclAsync(parent, false, onParent == Grant3.Allow ? Allow(alice.Identity, DfsFileSystemRights.DeleteSubdirectoriesAndFiles) : Deny(alice.Identity, DfsFileSystemRights.DeleteSubdirectoriesAndFiles));
                    if (onEntry != Grant3.None)
                        await w.SetAclAsync(entry, false, onEntry == Grant3.Allow ? Allow(alice.Identity, DfsFileSystemRights.Delete) : Deny(alice.Identity, DfsFileSystemRights.Delete));
                    bool allowed = onEntry == Grant3.Allow || onParent == Grant3.Allow;
                    Assert.True(allowed == w.Ref.Allows(AclTableRow.Delete, "alice", entry), $"reference delete decision (entry {onEntry}, parent {onParent})");
                    combos.Add((parent, entry, allowed));
                }
            }

            await w.CrossCheckAsync(w.Ref.AllPaths().Where(p => p.StartsWith("/t/", StringComparison.Ordinal)), [AdminName, "alice"]);
            foreach ((string parent, string entry, bool allowed) in combos)
            {
                string before = await DigestAsync(w.Admin, parent);
                Func<Task> delete = directory
                    ? () => alice.Volume.Directories.DeleteAsync(entry, false, Ct)
                    : () => alice.Volume.Files.DeleteAsync(entry, Ct);
                if (allowed)
                {
                    await delete();
                    Assert.False(directory ? await w.Admin.Directories.ExistsAsync(entry, Ct) : await w.Admin.Files.ExistsAsync(entry, Ct), $"{entry} still exists");
                    w.Ref.Remove(entry);
                }
                else
                {
                    await AssertDeniedAsync(delete, $"delete {entry} without Delete or parent DeleteSubdirectoriesAndFiles");
                    Assert.Equal(before, await DigestAsync(w.Admin, parent));
                }
            }
        });

    /// <summary>Which delete right alice holds on the move source.</summary>
    public enum SourceSide
    {
        None,
        InodeDelete,
        ParentDeleteChild,
    }

    public static TheoryData<bool, SourceSide, bool> RenameCases
    {
        get
        {
            var data = new TheoryData<bool, SourceSide, bool>();
            foreach (bool dir in new[] { false, true })
            {
                foreach (SourceSide s in Enum.GetValues<SourceSide>())
                {
                    data.Add(dir, s, false);
                    data.Add(dir, s, true);
                }
            }

            return data;
        }
    }

    /// <summary>
    /// A move needs delete rights at the source and the create right at the destination parent (and delete rights on an
    /// overwritten destination); each missing side is denied atomically: source, destination and overwritten entry are
    /// all unchanged.
    /// </summary>
    [Theory]
    [MemberData(nameof(RenameCases))]
    [Catalog("ACL-017", "U", "INV-08", "INV-12")]
    public Task ACL_017_RenameNeedsBothSides(bool directory, SourceSide source, bool destinationCreate) =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            await using AclWorld w = await AclWorld.CreateAsync(cluster);
            AclUser alice = await w.AddUserAsync("alice");
            await w.MkdirAsync("/t");
            await w.ProtectAsync("/t");
            await w.MkdirAsync("/t/src");
            await w.MkdirAsync("/t/dst");
            DfsFileSystemRights createRight = directory ? DfsFileSystemRights.CreateDirectories : DfsFileSystemRights.CreateFiles;
            if (directory)
            {
                await w.MkdirAsync("/t/src/x");
                await w.MkfileAsync("/t/src/x/inner", TestData.Bytes(1701, 100));
            }
            else
            {
                await w.MkfileAsync("/t/src/x", TestData.Bytes(1702, 5000));
            }

            if (source == SourceSide.InodeDelete)
                await w.SetAclAsync("/t/src/x", false, Allow(alice.Identity, DfsFileSystemRights.Delete));
            if (source == SourceSide.ParentDeleteChild)
                await w.SetAclAsync("/t/src", false, Allow(alice.Identity, DfsFileSystemRights.DeleteSubdirectoriesAndFiles));
            else
                await w.SetAclAsync("/t/src", false, Allow(alice.Identity, FC & ~DfsFileSystemRights.DeleteSubdirectoriesAndFiles));
            await w.SetAclAsync("/t/dst", false, Allow(alice.Identity, destinationCreate ? createRight : FC & ~createRight));

            bool allowed = source != SourceSide.None && destinationCreate;
            Assert.True(allowed == w.Ref.Allows(AclTableRow.Move, "alice", "/t/src/x", "/t/dst/x"), "reference move decision");
            await w.CrossCheckAsync(["/t/src", "/t/src/x", "/t/dst"], ["alice"]);
            InodeId inode = directory ? (await w.Admin.Directories.GetInfoAsync("/t/src/x", Ct)).InodeId : (await w.Admin.Files.GetInfoAsync("/t/src/x", Ct)).InodeId;
            string before = await DigestAsync(w.Admin, "/t");
            Func<Task> move = directory
                ? () => alice.Volume.Directories.MoveAsync("/t/src/x", "/t/dst/x", Ct)
                : () => alice.Volume.Files.MoveAsync("/t/src/x", "/t/dst/x", false, Ct);
            if (!allowed)
            {
                await AssertDeniedAsync(move, $"move with source side {source}, destination create {destinationCreate}");
                Assert.Equal(before, await DigestAsync(w.Admin, "/t"));
                return;
            }

            await move();
            w.Ref.Move("/t/src/x", "/t/dst/x");
            Assert.Equal(inode, directory ? (await w.Admin.Directories.GetInfoAsync("/t/dst/x", Ct)).InodeId : (await w.Admin.Files.GetInfoAsync("/t/dst/x", Ct)).InodeId);
            Assert.False(directory ? await w.Admin.Directories.ExistsAsync("/t/src/x", Ct) : await w.Admin.Files.ExistsAsync("/t/src/x", Ct));

            if (directory)
                return;

            // Overwriting an existing destination additionally needs delete rights on it.
            await w.MkfileAsync("/t/src/y", TestData.Bytes(1703, 600));
            await w.SetAclAsync("/t/src/y", false, Allow(alice.Identity, DfsFileSystemRights.Delete));
            await w.MkfileAsync("/t/dst/z", TestData.Bytes(1704, 700));
            Assert.False(w.Ref.Allows(AclTableRow.Move, "alice", "/t/src/y", "/t/dst/z"), "reference: overwrite without delete on the destination");
            string beforeOverwrite = await DigestAsync(w.Admin, "/t");
            await AssertDeniedAsync(() => alice.Volume.Files.MoveAsync("/t/src/y", "/t/dst/z", true, Ct), "overwrite without delete rights on the destination");
            Assert.Equal(beforeOverwrite, await DigestAsync(w.Admin, "/t"));
            await w.SetAclAsync("/t/dst/z", false, Allow(alice.Identity, DfsFileSystemRights.Delete));
            Assert.True(w.Ref.Allows(AclTableRow.Move, "alice", "/t/src/y", "/t/dst/z"));
            InodeId y = (await w.Admin.Files.GetInfoAsync("/t/src/y", Ct)).InodeId;
            await alice.Volume.Files.MoveAsync("/t/src/y", "/t/dst/z", true, Ct);
            w.Ref.Move("/t/src/y", "/t/dst/z");
            Assert.Equal(y, (await w.Admin.Files.GetInfoAsync("/t/dst/z", Ct)).InodeId);
            await w.CrossCheckAsync(w.Ref.AllPaths().Where(p => p.StartsWith("/t", StringComparison.Ordinal)), [AdminName, "alice"]);
        });

    /// <summary>
    /// A copy's descriptor is inherited from the destination parent with the caller as owner: the source's owner,
    /// explicit entries and protection are not copied (also not for its streams' governance).
    /// </summary>
    [Fact]
    [Catalog("ACL-018", "U", "INV-08", "INV-12")]
    public Task ACL_018_CopyInheritsDestinationAcl() =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            await using AclWorld w = await AclWorld.CreateAsync(cluster);
            await w.CreateGroupAsync("eng");
            AclUser alice = await w.AddUserAsync("alice", "eng");
            AclUser bob = await w.AddUserAsync("bob");
            AclUser carol = await w.AddUserAsync("carol");
            await w.MkdirAsync("/t");
            await w.ProtectAsync("/t");
            await w.MkdirAsync("/t/s");
            byte[] content = await w.MkfileAsync("/t/s/f", TestData.Bytes(1801, 9000), bob.Identity);
            byte[] stream = TestData.Bytes(1802, 4000);
            await w.Admin.Files.WriteAllBytesAsync("/t/s/f:meta", stream, Ct);
            await w.SetAclAsync("/t/s/f", true, AdminsFull, Allow(bob.Identity, FC), Allow(alice.Identity, DfsFileSystemRights.ReadData),
                Deny(carol.Identity, Read), Allow(carol.Identity, Write));
            await w.MkdirAsync("/t/d");
            await w.SetAclAsync("/t/d", false, Allow(G("eng"), DfsFileSystemRights.CreateFiles), Allow(G("eng"), Read, OI), Allow(CreatorOwner, FC, OI));

            await alice.Volume.Files.CopyAsync("/t/s/f", "/t/d/c", false, Ct);
            w.Ref.Copy("/t/s/f", "/t/d/c", alice.Identity);
            await AssertBytesAsync(content, alice.Volume.Files.ReadAllBytesAsync("/t/d/c", Ct), "copied default stream");
            await AssertBytesAsync(stream, alice.Volume.Files.ReadAllBytesAsync("/t/d/c:meta", Ct), "copied named stream");

            DfsSecurityDescriptor copy = await w.Admin.Files.GetAccessControlAsync("/t/d/c", Ct);
            Assert.Equal(alice.Identity, copy.Owner);
            Assert.Empty(copy.ExplicitRules);
            Assert.False(copy.AreAccessRulesProtected);
            Assert.DoesNotContain(copy.Rules, r => r.Identity == bob.Identity || r.Identity == carol.Identity);
            Assert.All(copy.Rules, r => Assert.True(r.IsInherited));

            await AssertDeniedAsync(() => bob.Volume.Files.ReadAllBytesAsync("/t/d/c", Ct), "the source owner's explicit FullControl is not copied");
            await AssertDeniedAsync(() => bob.Volume.Files.ReadAllBytesAsync("/t/d/c:meta", Ct), "nor for the copy's streams");
            await AssertDeniedAsync(() => carol.Volume.Files.WriteAllBytesAsync("/t/d/c", TestData.Bytes(1803, 10), Ct), "the source's Allow carol Write is not copied");
            await w.CrossCheckAsync(["/t/s/f", "/t/d", "/t/d/c"], [AdminName, "alice", "bob", "carol"]);

            await w.Admin.Files.CopyAsync("/t/s/f", "/t/d/c2", false, Ct);
            w.Ref.Copy("/t/s/f", "/t/d/c2", AdminId);
            Assert.Equal(AdminId, (await w.Admin.Files.GetAccessControlAsync("/t/d/c2", Ct)).Owner);
            await w.CrossCheckAsync(["/t/d/c2"], [AdminName, "alice", "bob", "carol"]);
        });

    /// <summary>
    /// Moving a file or directory recomputes inherited entries from the new ancestors immediately (whole subtree); the
    /// explicit entries travel with the inode.
    /// </summary>
    [Fact]
    [Catalog("ACL-019", "U", "INV-08", "INV-12")]
    public Task ACL_019_MoveRecomputesInheritance() =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            await using AclWorld w = await AclWorld.CreateAsync(cluster);
            AclUser alice = await w.AddUserAsync("alice");
            AclUser carol = await w.AddUserAsync("carol");
            await w.MkdirAsync("/t");
            await w.ProtectAsync("/t");
            await w.MkdirAsync("/t/allow");
            await w.SetAclAsync("/t/allow", false, Allow(alice.Identity, Read, CIOI));
            await w.MkdirAsync("/t/deny");
            await w.SetAclAsync("/t/deny", false, Deny(alice.Identity, Read, CIOI));
            byte[] f = await w.MkfileAsync("/t/allow/f", TestData.Bytes(1901, 5000));
            await w.SetAclAsync("/t/allow/f", false, Allow(carol.Identity, Read));
            await w.MkdirAsync("/t/allow/sub");
            await w.SetAclAsync("/t/allow/sub", false, Allow(carol.Identity, Read, CIOI));
            byte[] g = await w.MkfileAsync("/t/allow/sub/g", TestData.Bytes(1902, 300));
            InodeId inode = (await w.Admin.Files.GetInfoAsync("/t/allow/f", Ct)).InodeId;
            string[] users = [AdminName, "alice", "carol"];

            await w.AssertReadDecisionAsync("alice", "/t/allow/f", f);
            await w.AssertReadDecisionAsync("alice", "/t/allow/sub/g", g);

            await w.Admin.Files.MoveAsync("/t/allow/f", "/t/deny/f", false, Ct);
            w.Ref.Move("/t/allow/f", "/t/deny/f");
            await AssertDeniedAsync(() => alice.Volume.Files.ReadAllBytesAsync("/t/deny/f", Ct), "inherited Deny of the new parent applies at once");
            await AssertBytesAsync(f, carol.Volume.Files.ReadAllBytesAsync("/t/deny/f", Ct), "explicit entries travel with the inode");
            DfsSecurityDescriptor moved = await w.Admin.Files.GetAccessControlAsync("/t/deny/f", Ct);
            Assert.Equal(inode, (await w.Admin.Files.GetInfoAsync("/t/deny/f", Ct)).InodeId);
            Assert.Contains(moved.ExplicitRules, r => r.Identity == carol.Identity && r.Type == DfsAccessControlType.Allow);
            Assert.DoesNotContain(moved.Rules, r => r.InheritedFrom == "/t/allow");
            Assert.Contains(moved.Rules, r => r.IsInherited && r.InheritedFrom == "/t/deny");
            await w.CrossCheckAsync(["/t/deny/f"], users);

            await w.Admin.Directories.MoveAsync("/t/allow/sub", "/t/deny/sub", Ct);
            w.Ref.Move("/t/allow/sub", "/t/deny/sub");
            Assert.False(w.Ref.Can("/t/deny/sub", "alice", DfsFileSystemRights.ListDirectory));
            Assert.False(w.Ref.Can("/t/deny/sub/g", "alice", DfsFileSystemRights.ReadData));
            await w.AssertReadDecisionAsync("alice", "/t/deny/sub", null);
            await w.AssertReadDecisionAsync("alice", "/t/deny/sub/g", g);
            // The directory's explicit entry travels with it and keeps reaching its subtree from the new place.
            await AssertBytesAsync(g, carol.Volume.Files.ReadAllBytesAsync("/t/deny/sub/g", Ct), "a moved directory's explicit inheritable entry");
            Assert.Contains((await w.Admin.Directories.GetAccessControlAsync("/t/deny/sub", Ct)).ExplicitRules, r => r.Identity == carol.Identity);
            await w.CrossCheckAsync(["/t/deny/sub", "/t/deny/sub/g"], users);

            await w.Admin.Files.MoveAsync("/t/deny/f", "/t/allow/f2", false, Ct);
            w.Ref.Move("/t/deny/f", "/t/allow/f2");
            await AssertBytesAsync(f, alice.Volume.Files.ReadAllBytesAsync("/t/allow/f2", Ct), "moving back restores access");
            await w.CrossCheckAsync(w.Ref.AllPaths().Where(p => p.StartsWith("/t", StringComparison.Ordinal)), users);
        });

    // =============================================================================================================
    // ACL-021 .. ACL-025
    // =============================================================================================================

    /// <summary>
    /// Group membership changes govern operations authorized after they commit; existing handles keep the access granted
    /// at open (reads and writes) until closed.
    /// </summary>
    [Fact]
    [Catalog("ACL-021", "U", "INV-08", "INV-12")]
    public Task ACL_021_GroupMembershipEffect() =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            await using AclWorld w = await AclWorld.CreateAsync(cluster);
            await w.CreateGroupAsync("eng");
            AclUser alice = await w.AddUserAsync("alice");
            await w.AddUserAsync("bob", "eng");
            await w.MkdirAsync("/t");
            await w.ProtectAsync("/t", Allow(G("eng"), Modify, CIOI));
            byte[] content = await w.MkfileAsync("/t/f", TestData.Bytes(2101, 7000));
            byte[] wcontent = await w.MkfileAsync("/t/w.bin", TestData.Bytes(2102, 3000));
            string[] paths = ["/t", "/t/f", "/t/w.bin"];

            await AssertDeniedAsync(() => alice.Volume.Files.ReadAllBytesAsync("/t/f", Ct), "alice is not in eng yet");
            await w.CrossCheckAsync(paths, ["alice", "bob"]);

            await w.SetMembershipAsync("eng", "alice", member: true);
            await AssertBytesAsync(content, alice.Volume.Files.ReadAllBytesAsync("/t/f", Ct), "first operation after joining eng");
            await w.CrossCheckAsync(paths, ["alice", "bob"]);

            await using DfsStream reader = await alice.Volume.Files.OpenAsync("/t/f", FileMode.Open, FileAccess.Read, FileShare.ReadWrite, Ct);
            DfsStream writer = await alice.Volume.Files.OpenAsync("/t/w.bin", FileMode.Open, FileAccess.Write, FileShare.Read, Ct);
            try
            {
                await w.SetMembershipAsync("eng", "alice", member: false);
                await AssertDeniedAsync(() => alice.Volume.Files.ReadAllBytesAsync("/t/f", Ct), "first operation after leaving eng");
                await AssertDeniedAsync(() => alice.Volume.Files.OpenReadAsync("/t/w.bin", Ct), "new open after leaving eng");
                await w.CrossCheckAsync(paths, ["alice", "bob"]);

                byte[] viaHandle = new byte[content.Length];
                reader.Seek(0, SeekOrigin.Begin);
                await reader.ReadExactlyAsync(viaHandle, Ct);
                AssertEx.BytesEqual(content, viaHandle, "an open read handle keeps its granted access");

                byte[] update = TestData.Bytes(2103, 1000);
                await writer.WriteAsync(update, Ct);
                await writer.FlushAsync(Ct);
                byte[] expected = [.. update, .. wcontent.AsSpan(update.Length)];
                await AssertBytesAsync(expected, w.Admin.Files.ReadAllBytesAsync("/t/w.bin", Ct), "an open write handle keeps its granted access");
            }
            finally
            {
                await writer.DisposeAsync();
            }

            await reader.DisposeAsync();
            await AssertDeniedAsync(() => alice.Volume.Files.OpenReadAsync("/t/f", Ct), "after closing, a new open is evaluated again");
        });

    /// <summary>The volume grant and the ACL are both required: either alone is denied, with data unchanged.</summary>
    [Fact]
    [Catalog("ACL-022", "U", "INV-08", "INV-12")]
    public Task ACL_022_VolumeGrantAndAclBothRequired() =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            await using AclWorld w = await AclWorld.CreateAsync(cluster);
            AclUser vg = await w.AddUserAsync("vg", DfsPermissions.Read);
            AclUser nr = await w.AddUserAsync("nr", DfsPermissions.Write);
            AclUser ac = await w.AddUserAsync("ac", DfsPermissions.Read | DfsPermissions.Write);
            await w.MkdirAsync("/t");
            await w.ProtectAsync("/t", Allow(vg.Identity, FC, CIOI), Allow(nr.Identity, FC, CIOI), Allow(ac.Identity, Read, CIOI));
            byte[] content = await w.MkfileAsync("/t/f", TestData.Bytes(2201, 6000));
            await w.MkfileAsync("/t/g", TestData.Bytes(2202, 600));
            await w.SetAclAsync("/t/g", false, Allow(ac.Identity, DfsFileSystemRights.WriteData));
            await w.CrossCheckAsync(["/t", "/t/f", "/t/g"], ["vg", "nr", "ac"]);
            Assert.Equal(FC, await w.Admin.Files.GetEffectiveRightsAsync("/t/f", vg.Identity, Ct));
            string before = await DigestAsync(w.Admin, "/t");

            await AssertDeniedAsync(() => vg.Volume.Files.WriteAllBytesAsync("/t/f", TestData.Bytes(2203, 10), Ct), "ACL FullControl without the volume Write grant");
            await AssertDeniedAsync(() => vg.Volume.Files.WriteAllBytesAsync("/t/new", TestData.Bytes(2204, 10), Ct), "create without the volume Write grant");
            await AssertDeniedAsync(() => vg.Volume.Directories.CreateDirectoryAsync("/t/newdir", Ct), "mkdir without the volume Write grant");
            await AssertDeniedAsync(() => ac.Volume.Files.WriteAllBytesAsync("/t/f", TestData.Bytes(2205, 10), Ct), "volume Write grant without ACL WriteData");
            await AssertDeniedAsync(() => nr.Volume.Files.ReadAllBytesAsync("/t/f", Ct), "ACL FullControl without the volume Read grant");
            Assert.Equal(before, await DigestAsync(w.Admin, "/t"));

            await AssertBytesAsync(content, vg.Volume.Files.ReadAllBytesAsync("/t/f", Ct), "volume Read + ACL ReadData");
            byte[] update = TestData.Bytes(2206, 900);
            await ac.Volume.Files.WriteAllBytesAsync("/t/g", update, Ct);
            await AssertBytesAsync(update, w.Admin.Files.ReadAllBytesAsync("/t/g", Ct), "volume Write + ACL WriteData");
        });

    public enum InfoTarget
    {
        File,
        Directory,
        Stream,
    }

    /// <summary>
    /// Exists and GetInfo need ReadAttributes on the entry or ListDirectory on its parent; lacking both throws
    /// UnauthorizedAccessException — never false and never FileNotFound.
    /// </summary>
    [Theory]
    [InlineData(InfoTarget.File)]
    [InlineData(InfoTarget.Directory)]
    [InlineData(InfoTarget.Stream)]
    [Catalog("ACL-023", "U", "INV-08", "INV-12")]
    public Task ACL_023_ExistsAndInfoNeedAttributeRights(InfoTarget kind) =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            await using AclWorld w = await AclWorld.CreateAsync(cluster);
            AclUser alice = await w.AddUserAsync("alice");
            await w.MkdirAsync("/t");
            await w.ProtectAsync("/t");
            const DfsFileSystemRights ra = DfsFileSystemRights.ReadAttributes, ld = DfsFileSystemRights.ListDirectory;
            var cases = new List<(string Target, bool Allowed)>();
            int i = 0;
            foreach ((bool entryRa, bool parentLd) in new[] { (true, false), (false, true), (true, true), (false, false) })
            {
                string parent = "/t/c" + i++, entry = parent + "/x";
                await w.MkdirAsync(parent);
                if (kind == InfoTarget.Directory)
                {
                    await w.MkdirAsync(entry);
                }
                else
                {
                    await w.MkfileAsync(entry, TestData.Bytes((ulong)(2300 + i), 500));
                    await w.Admin.Files.WriteAllBytesAsync(entry + ":s", TestData.Bytes((ulong)(2310 + i), 50), Ct);
                }

                await w.SetAclAsync(entry, false, Allow(alice.Identity, entryRa ? ra : FC & ~ra));
                await w.SetAclAsync(parent, false, Allow(alice.Identity, parentLd ? ld : FC & ~ld));
                string target = kind == InfoTarget.Stream ? entry + ":s" : entry;
                bool allowed = entryRa || parentLd;
                Assert.True(allowed == w.Ref.Allows(AclTableRow.GetInfo, "alice", target), $"reference decision (ReadAttributes {entryRa}, parent ListDirectory {parentLd})");
                cases.Add((target, allowed));
            }

            await w.CrossCheckAsync(w.Ref.AllPaths().Where(p => p.StartsWith("/t/", StringComparison.Ordinal)), ["alice"]);
            foreach ((string target, bool allowed) in cases)
            {
                InodeId expectedInode = kind == InfoTarget.Directory
                    ? (await w.Admin.Directories.GetInfoAsync(target, Ct)).InodeId
                    : (await w.Admin.Files.GetInfoAsync(target, Ct)).InodeId;
                Func<Task<bool>> exists = kind == InfoTarget.Directory
                    ? () => alice.Volume.Directories.ExistsAsync(target, Ct)
                    : () => alice.Volume.Files.ExistsAsync(target, Ct);
                Func<Task<InodeId>> info = kind == InfoTarget.Directory
                    ? async () => (await alice.Volume.Directories.GetInfoAsync(target, Ct)).InodeId
                    : async () => (await alice.Volume.Files.GetInfoAsync(target, Ct)).InodeId;
                if (allowed)
                {
                    Assert.True(await exists(), $"Exists({target}) with the attribute right");
                    Assert.Equal(expectedInode, await info());
                }
                else
                {
                    await AssertDeniedAsync(exists, $"Exists({target}) without ReadAttributes or parent ListDirectory must not return false");
                    await AssertDeniedAsync(info, $"GetInfo({target}) without ReadAttributes or parent ListDirectory");
                }
            }
        });

    /// <summary>
    /// Enumerating needs ListDirectory on the directory; with it, every entry is listed (names, kinds, inodes) even when
    /// the entries' own ACLs deny the caller everything.
    /// </summary>
    [Fact]
    [Catalog("ACL-024", "U", "INV-08", "INV-12")]
    public Task ACL_024_EnumerationNeedsListDirectory() =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            await using AclWorld w = await AclWorld.CreateAsync(cluster);
            AclUser alice = await w.AddUserAsync("alice");
            await w.MkdirAsync("/t");
            await w.ProtectAsync("/t");
            await w.MkdirAsync("/t/e");
            await w.SetAclAsync("/t/e", false, Allow(alice.Identity, FC & ~DfsFileSystemRights.ListDirectory));
            await w.MkfileAsync("/t/e/a", TestData.Bytes(2401, 100));
            await w.SetAclAsync("/t/e/a", false, Deny(alice.Identity, FC));
            await w.MkdirAsync("/t/e/b");
            await w.ProtectAsync("/t/e/b");
            await w.MkfileAsync("/t/e/c", TestData.Bytes(2402, 200));

            await AssertDeniedAsync(() => ToListAsync(alice.Volume.Directories.EnumerateAsync("/t/e", null, Ct)), "enumerate without ListDirectory");
            await AssertDeniedAsync(() => alice.Volume.Directories.ListPageAsync("/t/e", null, null, Ct), "list page without ListDirectory");

            await w.SetAclAsync("/t/e", false, Allow(alice.Identity, DfsFileSystemRights.ListDirectory));
            List<DfsEntryInfo> adminView = await ToListAsync(w.Admin.Directories.EnumerateAsync("/t/e", null, Ct));
            List<DfsEntryInfo> aliceView = await ToListAsync(alice.Volume.Directories.EnumerateAsync("/t/e", null, Ct));
            Assert.Equal(new[] { "a", "b", "c" }, aliceView.Select(e => e.Name).Order(StringComparer.Ordinal));
            Assert.Equal(adminView.OrderBy(e => e.Name, StringComparer.Ordinal).Select(e => (e.Name, e.Kind, e.InodeId)),
                aliceView.OrderBy(e => e.Name, StringComparer.Ordinal).Select(e => (e.Name, e.Kind, e.InodeId)));
            DfsDirectoryPage page = await alice.Volume.Directories.ListPageAsync("/t/e", new DfsEnumerationOptions { Pattern = "a*" }, null, Ct);
            Assert.Equal("a", Assert.Single(page.Entries).Name);

            await AssertDeniedAsync(() => alice.Volume.Files.ReadAllBytesAsync("/t/e/a", Ct), "listing an entry does not grant reading it");
            await AssertDeniedAsync(() => ToListAsync(alice.Volume.Directories.EnumerateAsync("/t/e/b", null, Ct)), "listing a subdirectory's name does not grant listing it");
            await w.CrossCheckAsync(["/t/e", "/t/e/a", "/t/e/b", "/t/e/c"], ["alice"]);
        });

    /// <summary>
    /// With EnforceTraverseChecking off, access below /a ignores Traverse on /a; with it on, a missing Traverse on any
    /// ancestor denies reads, info, existence checks, enumeration and creation.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    [Catalog("ACL-025", "U", "INV-08", "INV-12")]
    public Task ACL_025_EnforceTraverseOption(bool enforce) =>
        ClusterTest.RunAsync(Options(Volume("tvol") with { EnforceTraverseChecking = true }), async cluster =>
        {
            await using AclWorld w = await AclWorld.CreateAsync(cluster, enforce ? "tvol" : Vol);
            AclUser alice = await w.AddUserAsync("alice");
            Assert.Equal(enforce, (await w.Admin.GetInfoAsync(Ct)).EnforceTraverseChecking);
            const DfsFileSystemRights traverse = DfsFileSystemRights.Traverse;
            const DfsFileSystemRights onB = DfsFileSystemRights.ListDirectory | DfsFileSystemRights.CreateFiles | DfsFileSystemRights.ReadAttributes;
            await w.MkdirAsync("/a");
            await w.ProtectAsync("/a");
            await w.MkdirAsync("/a/b");
            await w.ProtectAsync("/a/b", Allow(alice.Identity, onB | traverse));
            byte[] content = await w.MkfileAsync("/a/b/f", TestData.Bytes(2501, 3000));
            await w.SetAclAsync("/a/b/f", false, Allow(alice.Identity, Read));

            int phase = 0;
            async Task CheckAsync(string label)
            {
                phase++;
                string created = $"/a/b/new{phase}.bin";
                var probes = new (string Name, AclTableRow Row, string Path, Func<Task> Action)[]
                {
                    ("read", AclTableRow.Read, "/a/b/f", () => alice.Volume.Files.ReadAllBytesAsync("/a/b/f", Ct)),
                    ("info", AclTableRow.GetInfo, "/a/b/f", () => alice.Volume.Files.GetInfoAsync("/a/b/f", Ct)),
                    ("exists", AclTableRow.GetInfo, "/a/b/f", async () => Assert.True(await alice.Volume.Files.ExistsAsync("/a/b/f", Ct))),
                    ("enumerate", AclTableRow.Enumerate, "/a/b", () => ToListAsync(alice.Volume.Directories.EnumerateAsync("/a/b", null, Ct))),
                    ("create", AclTableRow.CreateFile, created, () => alice.Volume.Files.WriteAllBytesAsync(created, TestData.Bytes((ulong)(2510 + phase), 64), Ct)),
                };
                foreach ((string name, AclTableRow row, string path, Func<Task> action) in probes)
                {
                    bool allowed = w.Ref.Allows(row, "alice", path, enforceTraverse: enforce);
                    if (allowed)
                    {
                        await action();
                        if (row == AclTableRow.CreateFile)
                            w.Ref.AddFile(path, alice.Identity);
                    }
                    else
                    {
                        await AssertDeniedAsync(action, $"{name} {path} ({label}, enforce={enforce})");
                    }
                }

                await AssertBytesAsync(content, w.Admin.Files.ReadAllBytesAsync("/a/b/f", Ct), label);
            }

            Assert.Equal(!enforce, w.Ref.Allows(AclTableRow.Read, "alice", "/a/b/f", enforceTraverse: enforce));
            await CheckAsync("no Traverse on /a");
            await w.SetAclAsync("/a", true, AdminsFull, Allow(alice.Identity, traverse));
            Assert.True(w.Ref.Allows(AclTableRow.Read, "alice", "/a/b/f", enforceTraverse: enforce));
            await CheckAsync("Traverse on /a and /a/b");
            await w.SetAclAsync("/a/b", true, AdminsFull, Allow(alice.Identity, onB));
            Assert.Equal(!enforce, w.Ref.Allows(AclTableRow.Read, "alice", "/a/b/f", enforceTraverse: enforce));
            await CheckAsync("no Traverse on /a/b");
            await w.CrossCheckAsync(w.Ref.AllPaths(), [AdminName, "alice"]);
        });

    // =============================================================================================================
    // ACL-032
    // =============================================================================================================

    /// <summary>Named streams are governed by their file's descriptor; denying ReadData on the file denies every stream.</summary>
    [Fact]
    [Catalog("ACL-032", "U", "INV-08", "INV-12")]
    public Task ACL_032_StreamsShareFileAcl() =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            await using AclWorld w = await AclWorld.CreateAsync(cluster);
            AclUser alice = await w.AddUserAsync("alice");
            await w.MkdirAsync("/t");
            await w.ProtectAsync("/t", Allow(alice.Identity, Read, OI));
            byte[] main = await w.MkfileAsync("/t/f", TestData.Bytes(3201, 5000));
            byte[] s1 = TestData.Bytes(3202, 6000), s2 = TestData.Bytes(3203, 100);
            await w.Admin.Files.WriteAllBytesAsync("/t/f:s1", s1, Ct);
            await w.Admin.Files.WriteAllBytesAsync("/t/f:s2", s2, Ct);

            await AssertBytesAsync(main, alice.Volume.Files.ReadAllBytesAsync("/t/f", Ct), "default stream");
            await AssertBytesAsync(s1, alice.Volume.Files.ReadAllBytesAsync("/t/f:s1", Ct), "named stream s1");
            await AssertBytesAsync(s2, alice.Volume.Files.ReadAllBytesAsync("/t/f:s2", Ct), "named stream s2");

            DfsSecurityDescriptor fileAcl = await w.Admin.Files.GetAccessControlAsync("/t/f", Ct);
            foreach (string s in new[] { "/t/f:s1", "/t/f:s2" })
            {
                DfsSecurityDescriptor streamAcl = await w.Admin.Files.GetAccessControlAsync(s, Ct);
                Assert.Equal(fileAcl.Owner, streamAcl.Owner);
                Assert.Equal(NtfsAclReference.Describe(fileAcl), NtfsAclReference.Describe(streamAcl));
                Assert.Equal(w.Ref.Granted("/t/f", "alice"), await w.Admin.Files.GetEffectiveRightsAsync(s, alice.Identity, Ct));
            }

            await w.SetAclAsync("/t/f", false, Deny(alice.Identity, DfsFileSystemRights.ReadData));
            await w.Admin.Files.WriteAllBytesAsync("/t/f:s3", TestData.Bytes(3204, 70), Ct);
            foreach (string p in new[] { "/t/f", "/t/f:s1", "/t/f:s2", "/t/f:s3" })
            {
                Assert.False(w.Ref.Can(p, "alice", DfsFileSystemRights.ReadData));
                await AssertDeniedAsync(() => alice.Volume.Files.ReadAllBytesAsync(p, Ct), $"ReadAllBytes({p}) with ReadData denied on the file");
                await AssertDeniedAsync(() => alice.Volume.Files.OpenReadAsync(p, Ct), $"OpenRead({p}) with ReadData denied on the file");
                Assert.Equal((await w.Admin.Files.GetInfoAsync(p, Ct)).Length, (await alice.Volume.Files.GetInfoAsync(p, Ct)).Length);
            }

            string before = await DigestAsync(w.Admin, "/t");
            await AssertDeniedAsync(() => alice.Volume.Files.WriteAllBytesAsync("/t/f:s1", TestData.Bytes(3205, 10), Ct), "stream write without WriteData on the file");
            Assert.Equal(before, await DigestAsync(w.Admin, "/t"));

            await w.SetAclAsync("/t/f", false, Deny(alice.Identity, DfsFileSystemRights.ReadData), Allow(alice.Identity, DfsFileSystemRights.WriteData));
            byte[] update = TestData.Bytes(3206, 1500);
            await alice.Volume.Files.WriteAllBytesAsync("/t/f:s2", update, Ct);
            await AssertBytesAsync(update, w.Admin.Files.ReadAllBytesAsync("/t/f:s2", Ct), "stream written with the file's WriteData");
            await AssertBytesAsync(main, w.Admin.Files.ReadAllBytesAsync("/t/f", Ct), "default stream untouched");
            await AssertDeniedAsync(() => alice.Volume.Files.ReadAllBytesAsync("/t/f:s2", Ct), "still no ReadData");
            await w.CrossCheckAsync(["/t", "/t/f"], [AdminName, "alice"]);
        });

    // =============================================================================================================
    // ACL-034
    // =============================================================================================================

    /// <summary>
    /// GetEffectiveRights returns exactly the reference's granted rights for user identities (inherited, group-derived,
    /// owner-implicit, administrator-implicit and CreatorOwner-substituted bits) and for the caller itself.
    /// </summary>
    [Fact]
    [Catalog("ACL-034", "U", "INV-12")]
    public Task ACL_034_GetEffectiveRights() =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            await using AclWorld w = await AclWorld.CreateAsync(cluster);
            await w.CreateGroupAsync("eng");
            await w.CreateGroupAsync("ops");
            AclUser alice = await w.AddUserAsync("alice", "eng");
            await w.AddUserAsync("bob", "eng", "ops");
            await w.AddAdminUserAsync("carol", "ops");
            await w.AddUserAsync("dave");
            await w.MkdirAsync("/t");
            await w.ProtectAsync("/t", Allow(G("eng"), Read, CIOI), Deny(G("ops"), Write, CIOI));
            await w.MkdirAsync("/t/d");
            await w.SetAclAsync("/t/d", false, Allow(U("bob"), Modify, CI), Allow(CreatorOwner, FC, OI), Deny(U("alice"), DfsFileSystemRights.ReadAttributes, CIOI, NP));
            await w.MkfileAsync("/t/d/f", TestData.Bytes(3401, 100), alice.Identity);
            await w.MkdirAsync("/t/d/s");
            await w.MkfileAsync("/t/d/s/g", TestData.Bytes(3402, 100), U("dave"));
            await w.SetAclAsync("/t/d/s/g", false, Allow(DfsIdentity.AuthenticatedUsers, DfsFileSystemRights.ReadAttributes), Allow(Everyone, DfsFileSystemRights.ExecuteFile));
            string[] users = [AdminName, "alice", "bob", "carol", "dave"];
            string[] paths = ["/t", "/t/d", "/t/d/f", "/t/d/s", "/t/d/s/g"];
            await w.CrossCheckAsync(paths, users);

            // Spot values derived by hand from the NTFS rules:
            // alice owns f: CreatorOwner (OI on /t/d) gives her FullControl, but the NP deny of ReadAttributes reaches f.
            Assert.Equal(FC & ~DfsFileSystemRights.ReadAttributes, await w.Admin.Files.GetEffectiveRightsAsync("/t/d/f", alice.Identity, Ct));
            // On /t/d itself: eng's Read minus the explicit ReadAttributes deny.
            Assert.Equal(Read & ~DfsFileSystemRights.ReadAttributes, await w.Admin.Directories.GetEffectiveRightsAsync("/t/d", alice.Identity, Ct));
            Assert.Equal((DfsFileSystemRights)0, await w.Admin.Directories.GetEffectiveRightsAsync("/t", U("dave"), Ct));
            // dave owns g two levels below /t/d: the CreatorOwner template (OI|IO on /t/d/s) still substitutes him.
            Assert.Equal(FC, await w.Admin.Files.GetEffectiveRightsAsync("/t/d/s/g", U("dave"), Ct));
            // carol is an administrator in ops: Administrators FullControl minus ops' Write deny (evaluated first).
            Assert.Equal(FC & ~Write, await w.Admin.Directories.GetEffectiveRightsAsync("/t/d", U("carol"), Ct));
            // bob (eng, ops) on /t/d: his explicit Modify precedes the inherited ops Write deny, so Write survives.
            Assert.Equal(Modify, await w.Admin.Directories.GetEffectiveRightsAsync("/t/d", U("bob"), Ct));
            // ...but on the file f (Modify is CI only) the inherited deny wins: eng Read only.
            Assert.Equal(Read, await w.Admin.Files.GetEffectiveRightsAsync("/t/d/f", U("bob"), Ct));
            foreach (string p in paths)
                Assert.Equal(w.Ref.Granted(p, AdminName), await w.RightsAsAsync(w.Admin, p, null));
        });

    public static TheoryData<int> RandomTreeSeeds => new([1, 2, 3, 4]);

    /// <summary>
    /// Seeded random trees (allow/deny, all inheritance and propagation flags, CreatorOwner, protection, user and group
    /// owners): GetEffectiveRights and GetAccessControl of every node equal the reference for every principal, and real
    /// read/list decisions follow the reported ReadData bit.
    /// </summary>
    [Theory]
    [MemberData(nameof(RandomTreeSeeds))]
    [Catalog("ACL-034", "U", "INV-08", "INV-12")]
    public Task ACL_034_GetEffectiveRights_RandomTrees(int seed) =>
        ClusterTest.RunAsync(Options(), async cluster =>
        {
            await using AclWorld w = await AclWorld.CreateAsync(cluster);
            await w.CreateGroupAsync("eng");
            await w.CreateGroupAsync("ops");
            await w.AddUserAsync("alice", "eng");
            await w.AddUserAsync("bob", "eng", "ops");
            await w.AddAdminUserAsync("carol", "ops");
            await w.AddUserAsync("dave");
            string[] users = [AdminName, "alice", "bob", "carol", "dave"];
            var random = new Random(seed);
            var contents = new Dictionary<string, byte[]>(StringComparer.Ordinal);

            await w.MkdirAsync("/r");
            await w.ProtectAsync("/r", RandomRules(random, isDirectory: true));
            foreach ((string path, bool dir) in RandomTreeLayout)
            {
                if (dir)
                    await w.MkdirAsync(path);
                else
                    contents[path] = await w.MkfileAsync(path, TestData.Bytes((ulong)(seed * 100 + contents.Count), 64 + contents.Count));
            }

            foreach ((string path, bool dir) in RandomTreeLayout)
            {
                if (random.Next(4) == 0)
                    await w.SetOwnerAsync(path, RandomOwners[random.Next(RandomOwners.Length)]);
                bool protect = random.Next(5) == 0;
                DfsAccessRule[] rules = RandomRules(random, dir);
                await w.SetAclAsync(path, protect, protect ? [AdminsFull, .. rules] : rules);
            }

            string[] all = w.Ref.AllPaths().Where(p => p.StartsWith("/r", StringComparison.Ordinal)).ToArray();
            await w.CrossCheckAsync(all, users);
            foreach (string p in all)
            {
                foreach (string u in users.Where(u => u != AdminName))
                    await w.AssertReadDecisionAsync(u, p, contents.GetValueOrDefault(p));
            }
        });

    private static readonly (string Path, bool Directory)[] RandomTreeLayout =
    [
        ("/r/a", true), ("/r/a/x.bin", false), ("/r/a/b", true), ("/r/a/b/y.bin", false), ("/r/a/b/c", true), ("/r/a/b/c/z.bin", false),
        ("/r/a/b/c/d", true), ("/r/a/b/c/d/w.bin", false), ("/r/p", true), ("/r/p/q.bin", false), ("/r/p/s", true), ("/r/p/s/v.bin", false),
    ];

    private static readonly DfsIdentity[] RandomOwners = [AdminId, U("alice"), U("bob"), U("dave"), G("eng")];

    private static readonly DfsIdentity[] RandomIdentities = [U("alice"), U("bob"), U("dave"), G("eng"), G("ops"), DfsIdentity.CreatorOwner];

    private static readonly DfsFileSystemRights[] RandomRights =
    [
        DfsFileSystemRights.ReadData, DfsFileSystemRights.WriteData, DfsFileSystemRights.AppendData, DfsFileSystemRights.ReadAttributes,
        DfsFileSystemRights.Delete, DfsFileSystemRights.ReadPermissions, DfsFileSystemRights.ChangePermissions, DfsFileSystemRights.TakeOwnership,
        DfsFileSystemRights.DeleteSubdirectoriesAndFiles, DfsFileSystemRights.Traverse, Read, Write, Modify, FC, DfsFileSystemRights.ReadAndExecute,
        DfsFileSystemRights.ReadData | DfsFileSystemRights.WriteData,
    ];

    /// <summary>0..3 random explicit entries valid per DfsSecurityDescriptor.Validate (never naming Administrators/admin).</summary>
    internal static DfsAccessRule[] RandomRules(Random random, bool isDirectory)
    {
        int count = random.Next(4);
        var rules = new DfsAccessRule[count];
        for (int i = 0; i < count; i++)
        {
            DfsInheritanceFlags inheritance = isDirectory || random.Next(4) == 0 ? (DfsInheritanceFlags)random.Next(4) : DfsInheritanceFlags.None;
            DfsPropagationFlags propagation = inheritance == DfsInheritanceFlags.None ? DfsPropagationFlags.None : (DfsPropagationFlags)random.Next(4);
            DfsIdentity who = RandomIdentities[random.Next(RandomIdentities.Length)];
            if (who == DfsIdentity.CreatorOwner && inheritance == DfsInheritanceFlags.None)
                inheritance = (DfsInheritanceFlags)(1 + random.Next(3));
            DfsFileSystemRights rights = RandomRights[random.Next(RandomRights.Length)];
            // CreatorOwner entries stay Allow: a substituted Deny on an admin-owned node would lock out the harness admin
            // that builds and observes the tree.
            bool deny = who != DfsIdentity.CreatorOwner && random.Next(3) == 0;
            rules[i] = new DfsAccessRule(who, rights, deny ? DfsAccessControlType.Deny : DfsAccessControlType.Allow, inheritance, propagation);
        }

        return rules;
    }

    /// <summary>
    /// The reference model reproduces hand-computed NTFS outcomes, and on 400 random chains it agrees with the product
    /// evaluator entry-for-entry (a differential check; the reference remains the oracle for every cluster test).
    /// </summary>
    [Fact]
    [Catalog("ACL-034", "U", "INV-12")]
    public void ACL_034_GetEffectiveRights_ReferenceModelSelfCheck()
    {
        // Hand-computed NTFS examples.
        var r = new NtfsAclReference(AdminId);
        r.AddPrincipal(AdminName, isAdmin: true);
        r.AddPrincipal("alice");
        r.AddPrincipal("bob");
        r.AddGroup("eng", "alice");
        r.AddDirectory("/g", AdminId);
        r.SetExplicit("/g", [AdminsFull, Deny(U("alice"), DfsFileSystemRights.ReadData, CIOI), Allow(CreatorOwner, Modify, CIOI)], true);
        r.AddDirectory("/g/p", U("bob"));
        r.SetExplicit("/g/p", [Allow(G("eng"), Read, CIOI), Allow(U("alice"), DfsFileSystemRights.WriteData, OI, NP)], false);
        r.AddFile("/g/p/f", U("alice"));
        r.AddDirectory("/g/p/s", AdminId);
        r.AddFile("/g/p/s/h", AdminId);
        Assert.Equal(Read | Modify | DfsFileSystemRights.WriteData | NtfsAclReference.OwnerImplicit, r.Granted("/g/p/f", "alice"));
        Assert.Equal(Read, r.Granted("/g/p/s/h", "alice"));
        Assert.True(r.Can("/g/p/f", "alice", DfsFileSystemRights.ReadData), "the parent's Allow precedes the grandparent's Deny");
        Assert.False(r.Can("/g/p/s/h", "alice", DfsFileSystemRights.WriteData), "OI|NP does not reach grandchildren");
        Assert.Equal(Modify | NtfsAclReference.OwnerImplicit, r.Granted("/g/p", "bob"));
        Assert.Equal((DfsFileSystemRights)0, r.Granted("/g/p/f", "bob"));
        Assert.Equal(FC | NtfsAclReference.AdminImplicit, r.Granted("/g/p", AdminName));
        Assert.Equal(
        [
            "explicit Allow group:eng Read [CIOI]",
            "explicit Allow user:alice WriteData [OINP]",
            "inherited Deny user:alice ReadData [CIOI] from /g",
            "inherited Allow Administrators FullControl [CIOI] from /g",
            "inherited Allow user:bob Modify [] from /g",
            "inherited Allow CreatorOwner Modify [CIOIIO] from /g",
        ], r.DescribeAcl("/g/p"));
        Assert.Equal(
        [
            "inherited Allow group:eng Read [] from /g/p",
            "inherited Allow user:alice WriteData [] from /g/p",
            "inherited Deny user:alice ReadData [] from /g",
            "inherited Allow Administrators FullControl [] from /g",
            "inherited Allow user:alice Modify [] from /g",
        ], r.DescribeAcl("/g/p/f"));

        // Differential check against the product evaluator on random chains.
        var random = new Random(34034);
        string[] principals = ["alice", "bob", "dave", "carol"];
        for (int iteration = 0; iteration < 400; iteration++)
        {
            int depth = 1 + random.Next(4);
            var reference = new NtfsAclReference(AdminId, RandomRules(random, true), random.Next(3) == 0);
            reference.AddPrincipal("alice");
            reference.AddPrincipal("bob");
            reference.AddPrincipal("dave");
            reference.AddPrincipal("carol", isAdmin: true);
            reference.AddGroup("eng", "alice", "bob");
            reference.AddGroup("ops", "bob", "carol");
            string path = "";
            for (int level = 0; level < depth; level++)
            {
                path += "/n" + level;
                bool dir = level < depth - 1 || random.Next(2) == 0;
                DfsIdentity owner = RandomOwners[random.Next(RandomOwners.Length)];
                if (dir)
                    reference.AddDirectory(path, owner);
                else
                    reference.AddFile(path, owner);
                reference.SetExplicit(path, RandomRules(random, dir), random.Next(4) == 0);
            }

            var chain = new List<DfsSecurityDescriptor>();
            string? p = path;
            while (p is not null)
            {
                chain.Add(new DfsSecurityDescriptor(reference.OwnerOf(p), reference.ExplicitOf(p), reference.IsProtected(p)));
                p = p == "/" ? null : NtfsAclReference.ParentOf(p);
            }

            bool isDir = reference.IsDirectory(path);
            DfsIdentity targetOwner = reference.OwnerOf(path);
            IReadOnlyList<DfsAccessRule> product = DfsAccessEvaluator.EffectiveRules(chain, isDir, targetOwner, path);
            Assert.Equal(reference.DescribeAcl(path), NtfsAclReference.Describe(new DfsSecurityDescriptor(targetOwner, product)));
            foreach (string who in principals)
            {
                string[] groups = new[] { "eng", "ops" }.Where(g => reference.IsMember(g, who)).ToArray();
                IReadOnlySet<DfsIdentity> ids = DfsAccessEvaluator.CallerIdentities(who, groups, reference.IsAdmin(who));
                Assert.Equal(reference.Granted(path, who), DfsAccessEvaluator.GrantedRights(product, targetOwner, ids, reference.IsAdmin(who)));
            }
        }
    }

    // =============================================================================================================
    // Shared tree builder
    // =============================================================================================================

    /// <summary>Creates d, d/f (file), d/s (dir), d/s/g (file), d/s/s2 (dir) as admin.</summary>
    private static async Task BuildTreeAsync(AclWorld w, string d)
    {
        await w.MkdirAsync(d);
        await w.MkfileAsync(d + "/f", Encoding.UTF8.GetBytes("file " + d));
        await w.MkdirAsync(d + "/s");
        await w.MkfileAsync(d + "/s/g", Encoding.UTF8.GetBytes("nested " + d));
        await w.MkdirAsync(d + "/s/s2");
    }
}
