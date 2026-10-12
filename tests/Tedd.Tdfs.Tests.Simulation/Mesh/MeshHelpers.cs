using System.Globalization;
using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Testing.Mesh;

namespace Tedd.Tdfs.Tests.Simulation.Mesh;

/// <summary>
/// Matrices and small utilities of the MESH simulation suite. Every family enumerates its critical dimensions
/// exhaustively (node count 2..10 × failure kind/variant) and reduces the non-critical dimensions (bootstrap and seed
/// graph, link mesh, zone topology, control voters, volume voters, data acknowledgement count, crash mode) with the
/// constraint-aware pairwise generator <see cref="MeshPairwise"/>; see docs/test-notes/mesh.md.
/// </summary>
internal static class MeshHelpers
{
    public const string Vol = MeshScenario.Volume;

    public static readonly int[] NodeCounts = [2, 3, 4, 5, 6, 7, 8, 9, 10];

    private static readonly Dictionary<string, TheoryData<int, string>> Cache = new(StringComparer.Ordinal);
    private static readonly object CacheLock = new();

    /// <summary>
    /// Generator results per family (critical rows, excluded critical rows, extra rows, coverable/covered pairs) — the
    /// reduction denominators reported in docs/test-notes/mesh.md.
    /// </summary>
    internal static readonly Dictionary<string, MeshPairwiseResult> Reports = new(StringComparer.Ordinal);

    /// <summary>Deterministic in-memory simulation options for a configuration.</summary>
    public static TestClusterOptions Sim(MeshConfig config) => config.ToClusterOptions(ClusterMode.Deterministic);

    public static MeshConfig Config(int nodes, string config) => MeshConfig.Parse(nodes, config);

    public static IReadOnlyDictionary<string, string> Row(params (string Key, string Value)[] items) =>
        items.ToDictionary(i => i.Key, i => i.Value, StringComparer.Ordinal);

    public static string N(int n) => n.ToString(CultureInfo.InvariantCulture);

    /// <summary>Array overload of <see cref="AssertEx.BytesEqual"/> (spans cannot cross an awaited argument).</summary>
    public static void BytesEqual(byte[] expected, byte[] actual, string because) => AssertEx.BytesEqual(expected, actual, because);

    /// <summary>
    /// Dimension pairs that must be covered: every pair of non-critical dimensions and every (critical variant,
    /// non-critical) pair; node count × non-critical pairs are not required (node count × variant is exhaustive).
    /// </summary>
    public static bool PairFilter(string a, string b) => a != "n" && b != "n";

    /// <summary>Generates (and caches) a family's theory data.</summary>
    public static TheoryData<int, string> Matrix(string family, Func<IEnumerable<IReadOnlyDictionary<string, string>>> critical,
        IReadOnlyList<MeshDimension> nonCritical, Func<MeshConfig, bool>? constraint = null)
    {
        lock (CacheLock)
        {
            if (Cache.TryGetValue(family, out TheoryData<int, string>? cached))
                return cached;
            MeshPairwiseResult result = MeshPairwise.Generate(critical().ToList(), nonCritical, row =>
            {
                MeshConfig c = MeshConfig.FromRow(row);
                return c.Invalid() is null && (constraint?.Invoke(c) ?? true);
            }, PairFilter);
            Reports[family] = result;
            var data = new TheoryData<int, string>();
            foreach (IReadOnlyDictionary<string, string> row in result.Rows)
            {
                MeshConfig c = MeshConfig.FromRow(row);
                data.Add(c.Nodes, c.Format());
            }

            Cache[family] = data;
            return data;
        }
    }

    /// <summary>Critical rows n × variant.</summary>
    public static IEnumerable<IReadOnlyDictionary<string, string>> ByNodes(IEnumerable<int> nodes, params string[] variants)
    {
        foreach (int n in nodes)
        {
            if (variants.Length == 0)
                yield return Row(("n", N(n)));
            foreach (string variant in variants)
                yield return Row(("n", N(n)), ("variant", variant));
        }
    }

    /// <summary>Non-critical dimensions without crash mode (families that never crash).</summary>
    public static IReadOnlyList<MeshDimension> NoCrash => MeshConfig.DimensionsExcept("crash");

    /// <summary>Non-critical dimensions with the mesh fixed to Full (families whose exactness needs a clique).</summary>
    public static IReadOnlyList<MeshDimension> FullMesh(params string[] alsoExcept) => MeshConfig.DimensionsExcept(["mesh", .. alsoExcept]);

    public static MeshScenarioOptions Options(bool workload = true, bool namespaceScript = true, IReadOnlyList<string>? probeClients = null) => new()
    {
        Workload = workload,
        NamespaceScript = namespaceScript,
        ProbeClients = probeClients ?? ["p0"],
    };

    /// <summary>
    /// Runs an operation the oracle says must succeed, retrying only typed availability failures (and unknown outcomes)
    /// until <see cref="MeshScenarioOptions.MustWithin"/> elapses on the harness clock; any other failure, or still
    /// failing at the deadline, fails the test with <paramref name="because"/>.
    /// </summary>
    public static async Task<T> MustAsync<T>(MeshScenario s, Func<Task<T>> action, string because)
    {
        DateTimeOffset deadline = s.Cluster.Time.GetUtcNow() + s.Options.MustWithin;
        while (true)
        {
            try
            {
                return await action();
            }
            catch (Exception ex) when (MeshOracle.Unwrap(ex) is DfsUnavailableException or DfsCommitOutcomeUnknownException)
            {
                if (s.Cluster.Time.GetUtcNow() >= deadline)
                    Assert.Fail($"{because}: still failing after {s.Options.MustWithin}: {MeshOracle.Describe(MeshOracle.Unwrap(ex))}; net: {s.Net.Describe()}");
            }

            await s.Cluster.DelayAsync(TimeSpan.FromMilliseconds(250), s.Ct);
        }
    }

    /// <summary>
    /// An expectation for conditions under which the specification promises no progress (for example links slower than
    /// the election timeout): a required success becomes "success or a typed availability failure"; an impossibility
    /// stays impossible.
    /// </summary>
    public static MeshExpectation NotPromised(MeshExpectation expectation, MeshProbeKind kind, string why) =>
        expectation.Outcome == MeshTri.Must
            ? MeshExpectation.Either([kind == MeshProbeKind.ControlChange ? MeshCause.ControlQuorum : MeshCause.MetadataQuorum, MeshCause.Unreachable],
                why + "; otherwise " + expectation.Reason)
            : expectation;

    /// <summary>Names of nodes, deterministic order.</summary>
    public static string[] Names(MeshScenario s) => s.Net.NodeNames.ToArray();

    /// <summary>Picks up to <paramref name="count"/> items preferring those not in <paramref name="avoid"/>, deterministic order.</summary>
    public static string[] Prefer(IEnumerable<string> candidates, int count, IEnumerable<string> avoid)
    {
        var avoidSet = new HashSet<string>(avoid, StringComparer.Ordinal);
        string[] all = candidates.ToArray();
        return all.Where(c => !avoidSet.Contains(c)).Concat(all.Where(avoidSet.Contains)).Take(count).ToArray();
    }

    /// <summary>All set partitions of {0..n-1} as group lists (restricted growth strings).</summary>
    public static IEnumerable<int[][]> SetPartitions(int n)
    {
        int[] rgs = new int[n];
        while (true)
        {
            int blocks = rgs.Max() + 1;
            yield return Enumerable.Range(0, blocks).Select(b => Enumerable.Range(0, n).Where(i => rgs[i] == b).ToArray()).ToArray();
            int i = n - 1;
            while (i > 0)
            {
                int maxPrefix = rgs.Take(i).Max();
                if (rgs[i] <= maxPrefix)
                {
                    rgs[i]++;
                    for (int j = i + 1; j < n; j++)
                        rgs[j] = 0;
                    break;
                }

                i--;
            }

            if (i == 0)
                yield break;
        }
    }

    public static string EncodePartition(int[][] groups) => string.Join("|", groups.Select(g => string.Concat(g.Select(i => i.ToString(CultureInfo.InvariantCulture)))));

    public static string[][] DecodePartition(string text) =>
        text.Split('|').Select(g => g.Select(ch => "n" + ch).ToArray()).ToArray();
}
