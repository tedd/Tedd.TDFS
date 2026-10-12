using Tedd.Tdfs.Data;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Unit.Rep;

/// <summary>Shared setup and replica-accounting oracles for the REP suite (unit level).</summary>
internal static class RepHelpers
{
    public const string Vol = "vol";

    public static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Builds a policy (target copies, target zones, ack copies, ack zones).</summary>
    public static DfsStoragePolicy Policy(int copies, int zones, int ackCopies, int ackZones) => new()
    {
        TargetCopies = copies,
        TargetDistinctZones = zones,
        DurableCopiesBeforeAck = ackCopies,
        DurableZonesBeforeAck = ackZones,
    };

    /// <summary>Objects referenced by the file's current mapping (leader view).</summary>
    public static async Task<IReadOnlyList<ObjectId>> ObjectsOfAsync(TestCluster cluster, string path, string volume = Vol)
    {
        FileInspection? file = await cluster.Inspect.GetFileAsync(volume, path);
        Assert.NotNull(file);
        Assert.False(file.IsInline, $"{path} must be stored in extents for replica accounting");
        return file.Extents.Select(e => e.Object).Distinct().ToList();
    }

    /// <summary>Labels of the named nodes as the harness configured them.</summary>
    public static IReadOnlyList<DfsTopologyLabels> LabelsOf(TestCluster cluster, IEnumerable<string> nodes) =>
        nodes.Select(n => cluster.Node(n).Labels).ToList();

    /// <summary>
    /// Asserts that every object of <paramref name="path"/> is durably held on at least <paramref name="copies"/> distinct
    /// hosts in <paramref name="zones"/> distinct zones, cross-checking the inspector's domain count against the holders'
    /// topology labels (copies by host, zones by region/zone — never by endpoint).
    /// </summary>
    public static async Task AssertDurableAsync(TestCluster cluster, string path, int copies, int zones, string because)
    {
        foreach (ObjectId o in await ObjectsOfAsync(cluster, path))
        {
            IReadOnlyList<string> holders = await cluster.Inspect.GetObjectHoldersAsync(o);
            (int c, int z) = await cluster.Inspect.CountDurableDomainsAsync(o);
            (int lc, int lz) = Placement.CountDomains(LabelsOf(cluster, holders));
            Assert.Equal((lc, lz), (c, z));
            Assert.True(c >= copies && z >= zones,
                $"{because}: object {o} has {c} copies in {z} zones on [{string.Join(",", holders)}]; required {copies}/{zones}");
        }
    }

    /// <summary>Placement candidates as the cluster map describes them (eligible = active).</summary>
    public static async Task<IReadOnlyList<PlacementNode>> PlacementNodesAsync(DfsAdminClient admin)
    {
        IReadOnlyList<DfsNodeInfo> nodes = await admin.ListNodesAsync(Ct);
        return nodes.Select(n => new PlacementNode(n.Id, n.Name, n.Labels, n.Status == DfsNodeStatus.Active, Math.Max(1, n.CapacityBytes - n.UsedBytes))).ToList();
    }

    /// <summary>Asserts that a placement failure is reported as such (not as a quorum failure) with honest counts.</summary>
    public static void AssertPlacementCounts(DfsPlacementUnavailableException ex, int requiredCopies, int maxAchievedCopies, int requiredZones, int maxAchievedZones)
    {
        Assert.Equal(DfsErrorCode.PlacementUnavailable, ex.ErrorCode);
        Assert.True(ex.IsRetryable, "placement unavailability is a retryable availability condition");
        Assert.Equal(requiredCopies, ex.RequiredCopies);
        Assert.Equal(requiredZones, ex.RequiredZones);
        Assert.True(ex.AchievedCopies <= maxAchievedCopies, $"reported {ex.AchievedCopies} achieved copies; at most {maxAchievedCopies} exist");
        Assert.True(ex.AchievedZones <= maxAchievedZones, $"reported {ex.AchievedZones} achieved zones; at most {maxAchievedZones} exist");
    }

    /// <summary>Byte-exact comparison usable with awaited arguments.</summary>
    public static void Same(ReadOnlyMemory<byte> expected, ReadOnlyMemory<byte> actual, string? because = null) =>
        AssertEx.BytesEqual(expected.Span, actual.Span, because);

    public static void Log(string message) => TestContext.Current.TestOutputHelper?.WriteLine(message);
}
