using Tedd.Tdfs.Data;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Simulation.Rep;

/// <summary>Shared setup and replica-accounting oracles for the REP suite (deterministic simulation level).</summary>
internal static class RepHelpers
{
    public const string Vol = "vol";

    public const int Chunk = 64 * 1024;

    public static CancellationToken Ct => TestContext.Current.CancellationToken;

    public static DfsStoragePolicy Policy(int copies, int zones, int ackCopies, int ackZones) => new()
    {
        TargetCopies = copies,
        TargetDistinctZones = zones,
        DurableCopiesBeforeAck = ackCopies,
        DurableZonesBeforeAck = ackZones,
    };

    /// <summary>Deterministic simulation with <paramref name="nodeCount"/> nodes one per zone and the given default policy.</summary>
    public static TestClusterOptions Deterministic(int nodeCount = 3, DfsStoragePolicy? policy = null) =>
        TestClusterOptions.ThreeZones with { Mode = ClusterMode.Deterministic, NodeCount = nodeCount, DefaultPolicy = policy };

    /// <summary>Explicit metadata voters for <see cref="Vol"/>.</summary>
    public static TestVolumeSpec VolumeWithVoters(params string[] voters) => new(Vol)
    {
        Metadata = new DfsMetadataPolicy { Voters = voters.Length, Placement = DfsVoterPlacement.Explicit, ExplicitVoterNodes = voters },
    };

    public static async Task<IReadOnlyList<ObjectId>> ObjectsOfAsync(TestCluster cluster, string path, string volume = Vol)
    {
        FileInspection? file = await cluster.Inspect.GetFileAsync(volume, path);
        Assert.NotNull(file);
        Assert.False(file.IsInline, $"{path} must be stored in extents for replica accounting");
        return file.Extents.Select(e => e.Object).Distinct().ToList();
    }

    /// <summary>Current authoritative topology labels by node name (from the cluster map).</summary>
    public static async Task<IReadOnlyDictionary<string, DfsTopologyLabels>> CurrentLabelsAsync(DfsAdminClient admin) =>
        (await admin.ListNodesAsync(Ct)).ToDictionary(n => n.Name, n => n.Labels, StringComparer.Ordinal);

    /// <summary>Domain counts of a holder set under a given label map.</summary>
    public static (int Copies, int Zones) Domains(IEnumerable<string> holders, IReadOnlyDictionary<string, DfsTopologyLabels> labels) =>
        Placement.CountDomains(holders.Select(h => labels[h]));

    /// <summary>
    /// Every object of <paramref name="path"/> is durably held on ≥ <paramref name="copies"/> distinct hosts in ≥
    /// <paramref name="zones"/> distinct zones; the inspector's count must agree with the authoritative labels of the holders.
    /// </summary>
    public static async Task AssertDurableAsync(TestCluster cluster, string path, int copies, int zones, string because, string volume = Vol)
    {
        IReadOnlyDictionary<string, DfsTopologyLabels> labels = await CurrentLabelsAsync(cluster.Admin);
        foreach (ObjectId o in await ObjectsOfAsync(cluster, path, volume))
        {
            IReadOnlyList<string> holders = await cluster.Inspect.GetObjectHoldersAsync(o);
            (int c, int z) = await cluster.Inspect.CountDurableDomainsAsync(o);
            Assert.Equal(Domains(holders, labels), (c, z));
            Assert.True(c >= copies && z >= zones,
                $"{because}: object {o} has {c} copies in {z} zones on [{string.Join(",", holders)}]; required {copies}/{zones}");
        }
    }

    public static void AssertPlacementCounts(DfsPlacementUnavailableException ex, int requiredCopies, int maxAchievedCopies, int requiredZones, int maxAchievedZones)
    {
        Assert.Equal(DfsErrorCode.PlacementUnavailable, ex.ErrorCode);
        Assert.Equal(requiredCopies, ex.RequiredCopies);
        Assert.Equal(requiredZones, ex.RequiredZones);
        Assert.True(ex.AchievedCopies <= maxAchievedCopies, $"reported {ex.AchievedCopies} achieved copies; at most {maxAchievedCopies} exist");
        Assert.True(ex.AchievedZones <= maxAchievedZones, $"reported {ex.AchievedZones} achieved zones; at most {maxAchievedZones} exist");
    }

    /// <summary>
    /// Runs an operation that must not succeed without metadata authority. Returns the exception when the outcome may
    /// have been committed (<see cref="DfsCommitOutcomeUnknownException"/>) so the caller can resolve it after recovery;
    /// otherwise asserts a non-placement availability failure (FSD §13: quorum loss is distinct from placement).
    /// </summary>
    public static async Task<DfsCommitOutcomeUnknownException?> AssertNoAuthorityAsync(Func<Task> action, string what)
    {
        try
        {
            await action();
        }
        catch (DfsCommitOutcomeUnknownException unknown)
        {
            Assert.False(unknown.OperationId.IsEmpty, $"{what}: an unknown outcome must carry its operation id");
            return unknown;
        }
        catch (DfsPlacementUnavailableException ex)
        {
            Assert.Fail($"{what}: quorum loss was misreported as placement unavailability: {ex.Message}");
        }
        catch (DfsUnavailableException ex)
        {
            Assert.True(ex.IsRetryable, $"{what}: availability failures are retryable");
            return null;
        }
        catch (DfsException ex) when (ex.ErrorCode is DfsErrorCode.Timeout or DfsErrorCode.NotLeader or DfsErrorCode.QuorumUnavailable)
        {
            return null;
        }
        catch (TimeoutException)
        {
            return null;
        }

        Assert.Fail($"{what}: succeeded without a metadata majority");
        return null;
    }

    public static void Same(ReadOnlyMemory<byte> expected, ReadOnlyMemory<byte> actual, string? because = null) =>
        AssertEx.BytesEqual(expected.Span, actual.Span, because);

    public static void Log(string message) => TestContext.Current.TestOutputHelper?.WriteLine(message);
}
