using System.Globalization;
using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Testing.Mesh;

namespace Tedd.Tdfs.Tests.Unit.Mesh;

/// <summary>
/// MESH oracle and matrix evidence (test plan §2.1 / HAR-010 spirit): the availability oracle used by every MESH
/// scenario is checked against hand-computed quorum, placement, replica and inline arithmetic, and the pairwise matrix
/// generator is checked to keep every critical combination and cover every valid pair. These run without a cluster.
/// </summary>
public sealed class MeshUnitTests
{
    private static MeshNetworkModel Net(int nodes, TestTopology topology = TestTopology.OnePerZone, LinkMesh mesh = LinkMesh.Full) =>
        new(new TestClusterOptions { NodeCount = nodes, Topology = topology }.GetNodeSpecs(), mesh);

    private static string[] Nodes(int count) => Enumerable.Range(0, count).Select(i => "n" + i.ToString(CultureInfo.InvariantCulture)).ToArray();

    private static DfsStoragePolicy Ack(int copies, int zones, int target = 3, int targetZones = 3) => new()
    {
        TargetCopies = target,
        TargetDistinctZones = targetZones,
        DurableCopiesBeforeAck = copies,
        DurableZonesBeforeAck = zones,
    };

    // ============================================================================================================
    // MESH-034
    // ============================================================================================================

    public static TheoryData<int, int> VotersAndCrashes()
    {
        var data = new TheoryData<int, int>();
        for (int voters = 1; voters <= 7; voters++)
        {
            for (int crashed = 0; crashed <= voters; crashed++)
                data.Add(voters, crashed);
        }

        return data;
    }

    /// <summary>
    /// MESH-034 (quorum arithmetic): in a full mesh with V configured voters of which k crashed, a client reaching every
    /// node gets Must exactly when V-k ≥ ⌊V/2⌋+1 and Never otherwise (cause MetadataQuorum while any voter is alive,
    /// Unreachable when none is); the tolerance is ⌊(V-1)/2⌋ so even groups gain nothing over the odd group below.
    /// </summary>
    [Theory]
    [MemberData(nameof(VotersAndCrashes))]
    [Catalog("MESH-034", "U", "INV-11", "INV-20")]
    public void MESH_034_AvailabilityOracleQuorumArithmetic(int voters, int crashed)
    {
        MeshNetworkModel net = Net(Math.Max(voters, 1) + 1);
        string[] group = Nodes(voters);
        foreach (string node in group.Take(crashed))
            net.SetDown(node, true);
        MeshExpectation e = MeshOracle.Quorum(net, group, "client:c", MeshCause.MetadataQuorum);
        int majority = voters / 2 + 1;
        Assert.Equal(majority, MeshOracle.Majority(voters));
        Assert.Equal((voters - 1) / 2, MeshOracle.Tolerance(voters));
        if (voters - crashed >= majority)
        {
            Assert.Equal(MeshTri.Must, e.Outcome);
            Assert.Empty(e.Causes);
        }
        else
        {
            Assert.Equal(MeshTri.Never, e.Outcome);
            Assert.Equal(crashed < voters ? MeshCause.MetadataQuorum : MeshCause.Unreachable, Assert.Single(e.Causes));
        }

        if (voters % 2 == 0)
            Assert.Equal(MeshOracle.Tolerance(voters - 1), MeshOracle.Tolerance(voters));
    }

    /// <summary>
    /// MESH-034 (partitions, meshes, one-way links, clients): a clique partition serves only clients inside the majority
    /// group; a partial link mesh is Either (liveness not fixed by the specification) unless no connected majority exists
    /// (Never); one-way cuts break consensus links in both directions; a client reaching only part of the quorum side is
    /// Either; an isolated client is Never with cause Unreachable.
    /// </summary>
    [Fact]
    [Catalog("MESH-034", "U", "INV-11", "INV-20")]
    public void MESH_034_AvailabilityOracleTopologyCases()
    {
        string[] v3 = Nodes(3);
        MeshNetworkModel net = Net(5);
        net.Partition(["n0", "n1", "client:a"], ["n2", "n3", "n4", "client:b"]);
        Assert.Equal(MeshTri.Must, MeshOracle.Quorum(net, v3, "client:a", MeshCause.MetadataQuorum).Outcome);
        MeshExpectation b = MeshOracle.Quorum(net, v3, "client:b", MeshCause.MetadataQuorum);
        Assert.Equal(MeshTri.Never, b.Outcome);
        Assert.Contains(MeshCause.Unreachable, b.Causes);

        net.Partition(["n0", "client:a"], ["n1", "client:b"], ["n2", "n3", "n4"]);
        Assert.Equal(MeshTri.Never, MeshOracle.Quorum(net, v3, "client:a", MeshCause.MetadataQuorum).Outcome);
        Assert.Equal(MeshCause.MetadataQuorum, Assert.Single(MeshOracle.Quorum(net, v3, "client:a", MeshCause.MetadataQuorum).Causes));

        MeshNetworkModel line = Net(5, mesh: LinkMesh.Line);
        Assert.False(line.Linked("n0", "n2"));
        Assert.True(line.Linked("n1", "n2"));
        Assert.True(line.Linked("client:x", "n4"));
        Assert.Equal(MeshTri.Either, MeshOracle.Quorum(line, Nodes(5), "client:x", MeshCause.MetadataQuorum).Outcome);
        line.SetDown("n2", true);
        Assert.Equal(MeshTri.Never, MeshOracle.Quorum(line, Nodes(5), "client:x", MeshCause.MetadataQuorum).Outcome);
        line.Heal();
        Assert.Equal(LinkMesh.Full, line.Mesh);
        Assert.True(line.Linked("n0", "n3"), "heal removes the mesh restriction");
        Assert.False(line.Linked("n0", "n2"), "heal does not revive a crashed node");

        MeshNetworkModel star = Net(4, mesh: LinkMesh.Star);
        Assert.True(star.Linked("n0", "n3"));
        Assert.False(star.Linked("n1", "n3"));
        MeshNetworkModel ring = Net(5, mesh: LinkMesh.Ring);
        Assert.True(ring.Linked("n0", "n4"));
        Assert.False(ring.Linked("n0", "n2"));

        MeshNetworkModel cut = Net(3);
        foreach (string other in new[] { "n1", "n2" })
            cut.Cut("n0", other);
        Assert.False(cut.Linked("n0", "n1"));
        Assert.True(cut.CanSend("n1", "n0"));
        MeshExpectation withoutN0 = MeshOracle.Quorum(cut, v3, "client:c", MeshCause.MetadataQuorum);
        Assert.Equal(MeshTri.Must, withoutN0.Outcome); // n1+n2 hold a majority and form a clique

        MeshNetworkModel partial = Net(3);
        partial.CutBoth("client:c", "n0");
        Assert.Equal(MeshTri.Either, MeshOracle.Quorum(partial, v3, "client:c", MeshCause.MetadataQuorum).Outcome);
        partial.Isolate("client:c");
        MeshExpectation isolated = MeshOracle.Quorum(partial, v3, "client:c", MeshCause.MetadataQuorum);
        Assert.Equal(MeshTri.Never, isolated.Outcome);
        Assert.Equal(MeshCause.Unreachable, Assert.Single(isolated.Causes));

        MeshNetworkModel paused = Net(3);
        paused.SetDown("n1", true);
        Assert.False(paused.CanSend("client:c", "n1"));
        Assert.False(paused.CanSend("n1", "n0"));
    }

    /// <summary>
    /// MESH-034 (data policy, replicas, inline rule, failure classification): extent writes need the acknowledged number
    /// of distinct live hosts and zones the client reaches (hosts sharing a label count once); reads need a live reachable
    /// holder of every object; inline storage requires length ≤ threshold and a metadata majority whose worst case meets
    /// the acknowledgement policy; failures are legal only with the declared type, code and unchanged policy counts.
    /// </summary>
    [Fact]
    [Catalog("MESH-034", "U", "INV-08", "INV-11", "INV-14", "INV-20")]
    public void MESH_034_AvailabilityOraclePlacementAndClassification()
    {
        MeshNetworkModel net = Net(3);
        IReadOnlySet<string> side = MeshOracle.QuorumSide(net, Nodes(3))!;
        Assert.Equal(MeshTri.Must, MeshOracle.Placement(net, Ack(2, 2), "client:c", side).Outcome);
        net.SetDown("n1", true);
        net.SetDown("n2", true);
        MeshExpectation one = MeshOracle.Placement(net, Ack(2, 2), "client:c", side);
        Assert.Equal(MeshTri.Never, one.Outcome);
        Assert.Equal(MeshCause.Placement, Assert.Single(one.Causes));
        Assert.Equal(MeshTri.Must, MeshOracle.Placement(net, Ack(1, 1), "client:c", new HashSet<string> { "n0" }).Outcome);

        MeshNetworkModel twoPlusOne = Net(3, TestTopology.TwoPlusOne);
        twoPlusOne.SetDown("n2", true); // the only z2 node
        Assert.Equal(MeshTri.Never, MeshOracle.Placement(twoPlusOne, Ack(2, 2, 3, 2), "client:c", MeshOracle.QuorumSide(twoPlusOne, Nodes(3))).Outcome);
        Assert.Equal(MeshTri.Must, MeshOracle.Placement(twoPlusOne, Ack(2, 1, 3, 2), "client:c", MeshOracle.QuorumSide(twoPlusOne, Nodes(3))).Outcome);

        var sharedHost = new MeshNetworkModel([new TestNodeSpec("a", "z1", "h"), new TestNodeSpec("b", "z2", "h"), new TestNodeSpec("c", "z3")]);
        Assert.Equal((2, 3), MeshOracle.CountDomains(sharedHost, ["a", "b", "c"]));
        sharedHost.SetEligible("c", false);
        Assert.Equal(MeshTri.Never, MeshOracle.Placement(sharedHost, Ack(2, 1), "client:x", null).Outcome);

        MeshNetworkModel holders = Net(4);
        IReadOnlyList<IReadOnlyList<string>> objects = [["n0", "n1"], ["n2", "n3"]];
        Assert.Equal(MeshTri.Must, MeshOracle.Holders(holders, objects, "client:c").Outcome);
        holders.SetDown("n2", true);
        Assert.Equal(MeshTri.Must, MeshOracle.Holders(holders, objects, "client:c").Outcome);
        holders.SetDown("n3", true);
        Assert.Equal(MeshCause.NoHealthyReplica, Assert.Single(MeshOracle.Holders(holders, objects, "client:c").Causes));

        IReadOnlyList<TestNodeSpec> perZone = new TestClusterOptions { NodeCount = 3, Topology = TestTopology.OnePerZone }.GetNodeSpecs();
        IReadOnlyList<TestNodeSpec> split = new TestClusterOptions { NodeCount = 3, Topology = TestTopology.TwoPlusOne }.GetNodeSpecs();
        IReadOnlyList<TestNodeSpec> five = [new("a", "z1"), new("b", "z1"), new("c", "z2"), new("d", "z2"), new("e", "z3")];
        Assert.Equal((2, 2), MeshOracle.GuaranteedMetadataDurability(perZone));
        Assert.Equal((2, 1), MeshOracle.GuaranteedMetadataDurability(split));
        Assert.Equal((3, 2), MeshOracle.GuaranteedMetadataDurability(five));
        Assert.Equal((1, 1), MeshOracle.GuaranteedMetadataDurability(perZone.Take(1).ToArray()));
        Assert.True(MeshOracle.StoresInline(4096, 4096, Ack(2, 2), perZone));
        Assert.False(MeshOracle.StoresInline(4097, 4096, Ack(2, 2), perZone));
        Assert.False(MeshOracle.StoresInline(10, 4096, Ack(3, 3), perZone));
        Assert.False(MeshOracle.StoresInline(10, 4096, Ack(2, 2, 3, 2), split));
        Assert.Equal(10, MeshOracle.Subsets(5, 3).Count());

        DfsStoragePolicy policy = Ack(2, 2);
        var quorum = new HashSet<MeshCause> { MeshCause.MetadataQuorum };
        var placement = new HashSet<MeshCause> { MeshCause.Placement };
        Assert.Null(MeshOracle.CheckFailure(new DfsQuorumUnavailableException("q"), quorum, mutation: false, policy));
        Assert.NotNull(MeshOracle.CheckFailure(new DfsUnavailableException("u"), quorum, mutation: false, policy));
        Assert.Null(MeshOracle.CheckFailure(new DfsCommitOutcomeUnknownException(new DfsOperationId(1, 2), "?"), quorum, mutation: true, policy));
        Assert.NotNull(MeshOracle.CheckFailure(new DfsCommitOutcomeUnknownException(new DfsOperationId(1, 2), "?"), quorum, mutation: false, policy));
        Assert.NotNull(MeshOracle.CheckFailure(new DfsCommitOutcomeUnknownException(new DfsOperationId(1, 2), "?"), placement, mutation: true, policy));
        Assert.Null(MeshOracle.CheckFailure(new DfsPlacementUnavailableException("p", 2, 1, 2, 1), placement, mutation: true, policy));
        Assert.NotNull(MeshOracle.CheckFailure(new DfsPlacementUnavailableException("p", 1, 0, 1, 0), placement, mutation: true, policy)); // weakened policy
        Assert.NotNull(MeshOracle.CheckFailure(new DfsPlacementUnavailableException("p", 2, 2, 2, 2), placement, mutation: true, policy));
        Assert.NotNull(MeshOracle.CheckFailure(new IOException("io"), quorum, mutation: true, policy));
        Assert.NotNull(MeshOracle.CheckFailure(new DfsQuorumUnavailableException("q"), new HashSet<MeshCause>(), mutation: true, policy));
        Assert.Null(MeshOracle.CheckFailure(new DfsIntegrityException("missing"), new HashSet<MeshCause> { MeshCause.NoHealthyReplica }, false, policy));

        MeshExpectation combined = MeshExpectation.Combine(MeshExpectation.Must("m"), MeshExpectation.Never(MeshCause.Placement, "p"));
        Assert.Equal(MeshTri.Never, combined.Outcome);
        Assert.Equal(MeshCause.Placement, Assert.Single(combined.Causes));
        MeshExpectation either = MeshExpectation.Combine(MeshExpectation.Either([MeshCause.Unreachable], "e"), MeshExpectation.Must("m"));
        Assert.Equal(MeshTri.Either, either.Outcome);
        Assert.Equal(MeshTri.Must, MeshExpectation.Combine(MeshExpectation.Must("a"), MeshExpectation.Must("b")).Outcome);
    }

    /// <summary>
    /// MESH-034 (rename model): the rename-token model accepts only histories where every probe sees exactly the name the
    /// token currently has, rejects torn renames ("none"/"both") and impossible moves, and lets unknown moves take effect
    /// or not.
    /// </summary>
    [Fact]
    [Catalog("MESH-034", "U", "INV-02", "INV-20")]
    public void MESH_034_RenameModelRejectsTornRenames()
    {
        var model = new MeshRenameModel();
        HistoryOp Op(string kind, object? input, OpOutcome outcome, object? output) => new(1, "p", kind, "ren:x", input, 1, 2, outcome, output, null);
        Assert.True(model.Step("a", Op("move", "a>b", OpOutcome.Ok, "moved"), out string next));
        Assert.Equal("b", next);
        Assert.False(model.Step("a", Op("move", "b>a", OpOutcome.Ok, "moved"), out _));
        Assert.True(model.Step("a", Op("move", "b>a", OpOutcome.Ok, "absent"), out next));
        Assert.Equal("a", next);
        Assert.False(model.Step("b", Op("move", "b>a", OpOutcome.Ok, "absent"), out _));
        Assert.False(model.Step("a", Op("probe", null, OpOutcome.Ok, "both"), out _));
        Assert.False(model.Step("a", Op("probe", null, OpOutcome.Ok, "none"), out _));
        Assert.True(model.Step("a", Op("probe", null, OpOutcome.Ok, "a"), out _));
        Assert.True(model.Step("a", Op("move", "a>b", OpOutcome.Unknown, null), out next));
        Assert.Equal("b", next);

        var history = new List<HistoryOp>
        {
            new(1, "p1", "move", "ren:x", "a>b", 1, 2, OpOutcome.Ok, "moved", null),
            new(2, "p2", "probe", "ren:x", null, 3, 4, OpOutcome.Ok, "a", null),
        };
        Assert.False(LinearizabilityChecker.Check(history, model).IsValid);
        history[1] = history[1] with { Output = "b" };
        LinearizabilityChecker.Check(history, model).AssertValid();
    }

    // ============================================================================================================
    // MESH-035
    // ============================================================================================================

    /// <summary>
    /// MESH-035: the pairwise generator keeps every valid critical row exactly once (node count × failure variant
    /// exhaustive), covers every coverable pair of non-critical values and of variant × non-critical values, emits only
    /// valid configurations, reports critical rows without any valid completion, and is deterministic. Configuration
    /// encoding round-trips and every generated policy validates.
    /// </summary>
    [Fact]
    [Catalog("MESH-035", "U", "INV-20")]
    public void MESH_035_PairwiseMatrixCoversEveryValidPair()
    {
        int[] nodes = [2, 3, 4, 5, 6, 7, 8, 9, 10];
        string[] variants = ["WithLeader", "FollowersOnly"];
        List<IReadOnlyDictionary<string, string>> critical = nodes
            .SelectMany(n => variants.Select(v => (IReadOnlyDictionary<string, string>)new Dictionary<string, string>
            {
                ["n"] = n.ToString(CultureInfo.InvariantCulture),
                ["variant"] = v,
            }))
            .ToList();
        bool Valid(IReadOnlyDictionary<string, string> row)
        {
            MeshConfig c = MeshConfig.FromRow(row);
            return c.Invalid() is null && (c.Variant != "WithLeader" || c.VolumeVoters >= 3);
        }

        static bool Filter(string a, string b) => a != "n" && b != "n";
        MeshPairwiseResult first = MeshPairwise.Generate(critical, MeshConfig.AllDimensions, Valid, Filter);
        MeshPairwiseResult second = MeshPairwise.Generate(critical, MeshConfig.AllDimensions, Valid, Filter);
        Assert.Equal(first.Rows.Select(Key), second.Rows.Select(Key));
        Assert.Empty(MeshPairwise.UncoveredPairs(first, critical, MeshConfig.AllDimensions, Valid, Filter));
        Assert.Equal(first.CoverablePairs, first.CoveredPairs);
        Assert.Equal("n=2;variant=WithLeader", Assert.Single(first.InvalidCriticalRows)); // two nodes cannot hold three voters
        Assert.All(first.Rows, row => Assert.True(Valid(row), "generated an invalid row: " + Key(row)));
        foreach (IReadOnlyDictionary<string, string> c in critical.Where(c => !(c["n"] == "2" && c["variant"] == "WithLeader")))
            Assert.Contains(first.Rows, r => r["n"] == c["n"] && r["variant"] == c["variant"]);
        Assert.Equal(critical.Count - 1, first.Rows.Take(critical.Count - 1).Select(r => r["n"] + r["variant"]).Distinct().Count());

        MeshPairwiseResult impossible = MeshPairwise.Generate(
            [new Dictionary<string, string> { ["n"] = "2", ["variant"] = "WithLeader" }], MeshConfig.AllDimensions, Valid, Filter);
        Assert.Single(impossible.InvalidCriticalRows);
        Assert.Empty(impossible.Rows);

        foreach (IReadOnlyDictionary<string, string> row in first.Rows)
        {
            MeshConfig config = MeshConfig.FromRow(row);
            MeshConfig parsed = MeshConfig.Parse(config.Nodes, config.Format());
            Assert.Equal(config.Format(), parsed.Format());
            Assert.True(config.Format().Length <= 50, "theory display must not truncate the configuration: " + config.Format());
            config.Policy().Validate();
            Assert.Equal(config.Ack, config.Policy().DurableCopiesBeforeAck);
        }

        Assert.NotNull(new MeshConfig { Nodes = 2, VolumeVoters = 3 }.Invalid());
        Assert.NotNull(new MeshConfig { Nodes = 2, Ack = 3 }.Invalid());
        Assert.NotNull(new MeshConfig { Nodes = 5, Boot = MeshBoot.SeedRing, Mesh = LinkMesh.Line }.Invalid());
        Assert.Null(new MeshConfig { Nodes = 5, Boot = MeshBoot.SeedStar, Mesh = LinkMesh.Star, VolumeVoters = 3 }.Invalid());
        Assert.Equal(new[] { "n3", "n4", "n5" }, new MeshConfig { Nodes = 10, Mesh = LinkMesh.Line, VolumeVoters = 3 }.CentralVoters(3)!);
        Assert.Equal(new[] { "n0", "n1", "n2" }, new MeshConfig { Nodes = 10, Mesh = LinkMesh.Star }.CentralVoters(3)!);
        Assert.Null(new MeshConfig { Nodes = 10 }.CentralVoters(3));

        static string Key(IReadOnlyDictionary<string, string> row) => string.Join(";", row.OrderBy(kv => kv.Key, StringComparer.Ordinal).Select(kv => kv.Key + "=" + kv.Value));
    }
}
