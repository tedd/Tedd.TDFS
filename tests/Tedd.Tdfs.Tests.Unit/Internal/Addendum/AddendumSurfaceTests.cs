using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Unit.Internal.Addendum;

/// <summary>
/// Pure-logic parts of the Addendum A API surface: catalog, pool names, error codes, record compatibility and harness
/// specifications (API.md §H).
/// </summary>
public sealed class AddendumSurfaceTests
{
    // ---- Catalog -----------------------------------------------------------------------------------------------

    [Fact]
    public void AddendumCatalogHas78CasesAndTheTestPlanStill586()
    {
        Assert.Equal(TestCatalog.AddendumExpectedCount, TestCatalog.Addendum.Count);
        Assert.Equal(78, TestCatalog.Addendum.Count);
        Assert.Equal(TestCatalog.ExpectedCount, TestCatalog.All.Count);
        Assert.Equal(new[] { "ACL", "POOL", "ADS" }, TestCatalog.AddendumSuites);
        Assert.Equal(34, TestCatalog.Addendum.Count(e => e.Suite == "ACL"));
        Assert.Equal(18, TestCatalog.Addendum.Count(e => e.Suite == "POOL"));
        Assert.Equal(26, TestCatalog.Addendum.Count(e => e.Suite == "ADS"));
        Assert.Equal(78, TestCatalog.Addendum.Select(e => e.Id).Distinct(StringComparer.Ordinal).Count());
        Assert.Empty(TestCatalog.Addendum.Select(e => e.Id).Intersect(TestCatalog.All.Select(e => e.Id), StringComparer.Ordinal));
        Assert.DoesNotContain(TestCatalog.Suites, s => s is "ACL" or "POOL" or "ADS");
    }

    [Fact]
    public void AddendumEntriesAreNumberedContiguouslyWithValidLevels()
    {
        foreach (IGrouping<string, CatalogEntry> suite in TestCatalog.Addendum.GroupBy(e => e.Suite))
        {
            int n = 1;
            foreach (CatalogEntry entry in suite)
            {
                Assert.Equal($"{suite.Key}-{n:D3}", entry.Id);
                Assert.Contains(entry.Level, new[] { "U", "S", "I" });
                Assert.False(string.IsNullOrWhiteSpace(entry.Name));
                Assert.False(string.IsNullOrWhiteSpace(entry.Assertion));
                n++;
            }
        }
    }

    [Fact]
    public void FindAnyCoversBothCatalogs()
    {
        CatalogEntry acl = TestCatalog.FindAny("ACL-001")!;
        Assert.Equal("DefaultRootDescriptor", acl.Name);
        Assert.Equal("U", acl.Level);
        Assert.Equal("ACL_001_DefaultRootDescriptor", acl.MethodName);
        Assert.Equal("PoolsOverRealTransport", TestCatalog.FindAny("POOL-018")!.Name);
        Assert.Equal("I", TestCatalog.FindAny("ADS-026")!.Level);
        Assert.Null(TestCatalog.Find("ACL-001"));
        Assert.NotNull(TestCatalog.FindAny("OPEN-001"));
        Assert.Null(TestCatalog.FindAny("ACL-035"));
        // Assertions keep '|' inside backticks.
        Assert.Contains("CI|OI", acl.Assertion, StringComparison.Ordinal);
    }

    [Fact]
    public void ParsersOnlyReadTheirOwnSection()
    {
        const string markdown = "## 7. Plan\n| OPEN-001 | U | `A` | x |\n## A5. Addendum\n| ACL-001 | U | `B` | y |\n";
        Assert.Equal("OPEN-001", Assert.Single(TestCatalog.Parse(markdown)).Id);
        Assert.Equal("ACL-001", Assert.Single(TestCatalog.ParseAddendum(markdown)).Id);
    }

    [Fact]
    public void CatalogAttributeAcceptsAddendumIds()
    {
        var attribute = new CatalogAttribute("POOL-018", "I", "INV-08");
        Assert.Equal("POOL", attribute.Suite);
        Assert.Contains(new KeyValuePair<string, string>("Suite", "POOL"), attribute.GetTraits());
        Assert.Contains(new KeyValuePair<string, string>("TestId", "POOL-018"), attribute.GetTraits());
        Assert.Equal("ACL", new CatalogAttribute("ACL-034", "U").Suite);
        Assert.Equal("ADS", new CatalogAttribute("ADS-001", "U", "INV-02").Suite);
    }

    // ---- Pools, policies and error codes -----------------------------------------------------------------------

    [Fact]
    public void PoolNames()
    {
        foreach (string good in new[] { "default", "fast", "tier-1", "ssd.pool_2", "A", "9z", new string('p', 64) })
            Assert.True(DfsPoolNames.IsValid(good), good);
        foreach (string? bad in new[] { null, "", "-x", ".x", "_x", "a b", "a/b", "a:b", "é", new string('p', 65) })
            Assert.False(DfsPoolNames.IsValid(bad), bad);
        Assert.Throws<ArgumentException>(() => DfsPoolNames.Validate("a b"));
        Assert.Throws<ArgumentNullException>(() => DfsPoolNames.Validate(null!));
        Assert.Equal("default", DfsPoolNames.Default);
    }

    [Fact]
    public void StoragePolicyPoolIsValidatedAsAName()
    {
        new DfsStoragePolicy { Pool = "fast" }.Validate();
        new DfsStoragePolicy().Validate();
        Assert.Null(DfsStoragePolicy.Default.Pool);
        DfsException ex = Assert.Throws<DfsException>(() => new DfsStoragePolicy { Pool = "bad name" }.Validate());
        Assert.Equal(DfsErrorCode.InvalidPolicy, ex.ErrorCode);
        Assert.NotEqual(DfsStoragePolicy.Default, DfsStoragePolicy.Default with { Pool = "fast" });
    }

    [Theory]
    [InlineData(DfsErrorCode.PoolNotFound, 48)]
    [InlineData(DfsErrorCode.PoolInUse, 49)]
    [InlineData(DfsErrorCode.GroupNotFound, 50)]
    [InlineData(DfsErrorCode.PrincipalNotFound, 51)]
    public void AddendumErrorCodesRoundTrip(DfsErrorCode code, int value)
    {
        Assert.Equal(value, (int)code);
        Exception ex = DfsErrors.ToException(code, "msg", "/p");
        DfsException dfs = Assert.IsType<DfsException>(ex);
        Assert.Equal(code, dfs.ErrorCode);
        Assert.False(dfs.IsRetryable);
        Assert.Equal(code, DfsErrors.ToCode(ex));
        Assert.Equal("/p", DfsErrors.GetPath(ex));
    }

    // ---- Record compatibility ---------------------------------------------------------------------------------

    [Fact]
    public void AppendedRecordParametersDefault()
    {
        var info = new DfsFileInfo("/f", new InodeId(5), 1, 10, DateTimeOffset.UnixEpoch, DateTimeOffset.UnixEpoch, "alice",
            new DfsInodeAccess(), "default", false, false, 1, CommitToken.None);
        Assert.False(info.IsStream);
        Assert.Null(info.StreamName);
        Assert.Equal(0, info.StreamCount);
        Assert.True((info with { IsStream = true, StreamName = "s", StreamCount = 2 }).IsStream);

        var volume = new DfsVolumeInfo(default, "vol", 1, new DfsMetadataPolicy(), DfsStoragePolicy.Default,
            new Dictionary<string, DfsStoragePolicy>(), 4096, new DfsVolumeLimits(), CommitToken.None);
        Assert.Equal("default", volume.StoragePool);
        Assert.False(volume.EnforceTraverseChecking);

        var spec = new DfsVolumeSpec { Name = "vol" };
        Assert.Equal("default", spec.StoragePool);
        Assert.Null(spec.RootSecurity);
        Assert.False(spec.EnforceTraverseChecking);

        var file = new FileInspection("/f", new InodeId(5), 1, 0, true, 0, "default", []);
        Assert.NotNull(file.Streams);
        Assert.Empty(file.Streams);
        var withStreams = new FileInspection("/f", new InodeId(5), 1, 0, true, 0, "default", [], [new StreamInspection("s", 3, 7, [])]);
        Assert.Equal("s", Assert.Single(withStreams.Streams).Name);

        Assert.Equal(new DfsStreamInfo("s", 3, 7), new DfsStreamInfo("s", 3, 7));
        Assert.Equal("eng", new DfsGroupInfo("eng", ["alice"]).Name);
        var pool = new DfsPoolInfo("fast", ["n0"], ["vol"], 100, 10);
        Assert.Same(pool, new DfsPoolStatus(pool, new DfsReplicaHealth(0, 0, 0, 0, 0, 0, 0, 0, 0), []).Info);
    }

    // ---- Harness specifications --------------------------------------------------------------------------------

    [Fact]
    public void TestVolumeSpecMapsAddendumFields()
    {
        DfsSecurityDescriptor root = DfsSecurityDescriptor.DefaultRoot(DfsIdentity.User("admin"));
        var spec = new TestVolumeSpec("vol") { StoragePool = "fast", RootSecurity = root, EnforceTraverseChecking = true };
        DfsVolumeSpec volume = spec.ToVolumeSpec(DfsStoragePolicy.Default);
        Assert.Equal("fast", volume.StoragePool);
        Assert.Same(root, volume.RootSecurity);
        Assert.True(volume.EnforceTraverseChecking);

        DfsVolumeSpec plain = new TestVolumeSpec("v2").ToVolumeSpec(DfsStoragePolicy.Default);
        Assert.Equal("default", plain.StoragePool);
        Assert.Null(plain.RootSecurity);
        Assert.False(plain.EnforceTraverseChecking);
    }

    [Fact]
    public void TestNodeSpecPools()
    {
        var plain = new TestNodeSpec("n0", "z1");
        Assert.Null(plain.Pools);
        Assert.Empty(plain.EffectivePools);
        Assert.True(plain.IsInPool("default"));
        Assert.False(plain.IsInPool("fast"));

        var fast = new TestNodeSpec("n1", "z2", Pools: ["fast"]);
        Assert.True(fast.IsInPool("fast"));
        Assert.True(fast.IsInPool("default"));
    }

    [Fact]
    public void ClusterOptionsValidatePoolsAndDerivePerPoolPolicies()
    {
        TestNodeSpec[] nodes =
        [
            new("n0", "z1", Pools: ["fast"]), new("n1", "z2", Pools: ["fast"]), new("n2", "z3"), new("n3", "z3", Pools: ["slow"]),
        ];
        var options = new TestClusterOptions
        {
            Nodes = nodes,
            Topology = TestTopology.Custom,
            Volumes = [new TestVolumeSpec("vol"), new TestVolumeSpec("hot") { StoragePool = "fast" }],
        };
        options.Validate();
        Assert.Equal(new[] { "fast", "slow" }, options.GetPoolNames());

        DfsStoragePolicy fastPolicy = options.GetEffectiveDefaultPolicy("fast");
        Assert.Equal(2, fastPolicy.TargetCopies);
        Assert.Equal(2, fastPolicy.TargetDistinctZones);
        Assert.Equal(2, fastPolicy.DurableCopiesBeforeAck);
        Assert.Equal(2, fastPolicy.DurableZonesBeforeAck);
        DfsStoragePolicy slowPolicy = options.GetEffectiveDefaultPolicy("slow");
        Assert.Equal(1, slowPolicy.TargetCopies);
        Assert.Equal(options.GetEffectiveDefaultPolicy(), options.GetEffectiveDefaultPolicy("default"));
        Assert.Throws<ArgumentException>(() => options.GetEffectiveDefaultPolicy("none"));

        Assert.Throws<ArgumentException>(() => (options with { Volumes = [new TestVolumeSpec("v") { StoragePool = "missing" }] }).Validate());
        Assert.Throws<ArgumentException>(() => (options with { Volumes = [new TestVolumeSpec("v") { StoragePool = "bad name" }] }).Validate());
        Assert.Throws<ArgumentException>(() => (options with { Nodes = [new TestNodeSpec("n0", "z1", Pools: ["default"])] }).Validate());
        Assert.Throws<ArgumentException>(() => (options with { Nodes = [new TestNodeSpec("n0", "z1", Pools: ["a b"])] }).Validate());
        Assert.Throws<ArgumentException>(() => (options with
        {
            Volumes = [new TestVolumeSpec("v") { RootSecurity = new DfsSecurityDescriptor(DfsIdentity.CreatorOwner, []) }],
        }).Validate());
    }
}
