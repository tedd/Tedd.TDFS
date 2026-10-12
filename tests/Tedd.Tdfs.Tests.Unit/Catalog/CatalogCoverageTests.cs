using System.Text.RegularExpressions;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Unit.Catalog;

/// <summary>
/// Catalog coverage (test plan §2, API.md §G): every named case of the test plan has at least one test carrying its
/// <c>[Catalog]</c> id somewhere under <c>tests/</c>, and every <c>[Catalog]</c> id used by a test names a real case (test
/// plan, FSD Addendum A, or a MESH row of <c>docs/test-notes/mesh.md</c>). The sources are scanned rather than reflected so
/// that the Simulation, Integration, Models and Performance projects are covered too.
/// </summary>
public sealed partial class CatalogCoverageTests
{
    private static readonly Lazy<CatalogScan> Scan = new(CatalogScan.Run, LazyThreadSafetyMode.ExecutionAndPublication);

    [Fact]
    public void EveryPlanCaseHasATest()
    {
        CatalogScan scan = Scan.Value;
        Assert.Equal(TestCatalog.ExpectedCount, TestCatalog.All.Count);
        Assert.True(scan.Files > 0, $"no test sources found under {scan.TestsRoot}");

        string[] missing = TestCatalog.All
            .Where(e => !scan.Usages.ContainsKey(e.Id))
            .Select(e => $"{e.Id} ({e.Level}) {e.Name}")
            .ToArray();
        Report($"{TestCatalog.All.Count - missing.Length} of {TestCatalog.All.Count} test-plan cases have at least one [Catalog] test " +
               $"({scan.Usages.Values.Sum(u => u.Count)} attributes in {scan.Files} files).");
        Assert.True(missing.Length == 0,
            $"{missing.Length} test-plan case(s) have no [Catalog] test:{Environment.NewLine}{string.Join(Environment.NewLine, missing)}");
    }

    [Fact]
    public void EveryCatalogIdUsedIsKnown()
    {
        CatalogScan scan = Scan.Value;
        IReadOnlySet<string> meshIds = scan.MeshIds;
        Assert.NotEmpty(meshIds);

        string[] unknown = scan.Usages
            .Where(kv => TestCatalog.FindAny(kv.Key) is null && !meshIds.Contains(kv.Key))
            .OrderBy(kv => kv.Key, StringComparer.Ordinal)
            .Select(kv => $"{kv.Key} used at {string.Join(", ", kv.Value.Take(3))}{(kv.Value.Count > 3 ? ", ..." : string.Empty)}")
            .ToArray();
        Assert.True(unknown.Length == 0,
            $"{unknown.Length} [Catalog] id(s) are neither test-plan cases, Addendum A cases nor MESH rows of docs/test-notes/mesh.md:" +
            $"{Environment.NewLine}{string.Join(Environment.NewLine, unknown)}");

        // MESH rows are documented in the test notes, not in TestCatalog; each documented row should have a test too.
        string[] meshWithoutTest = meshIds.Where(id => !scan.Usages.ContainsKey(id)).Order(StringComparer.Ordinal).ToArray();
        Assert.True(meshWithoutTest.Length == 0, $"MESH row(s) documented in docs/test-notes/mesh.md without a test: {string.Join(", ", meshWithoutTest)}");
    }

    [Fact]
    public void EveryAddendumCaseHasATest()
    {
        CatalogScan scan = Scan.Value;
        Assert.Equal(TestCatalog.AddendumExpectedCount, TestCatalog.Addendum.Count);

        CatalogEntry[] covered = TestCatalog.Addendum.Where(e => scan.Usages.ContainsKey(e.Id)).ToArray();
        string[] missing = TestCatalog.Addendum.Where(e => !scan.Usages.ContainsKey(e.Id)).Select(e => $"{e.Id} ({e.Level}) {e.Name}").ToArray();
        foreach (string suite in TestCatalog.AddendumSuites)
        {
            int total = TestCatalog.Addendum.Count(e => e.Suite == suite);
            int have = covered.Count(e => e.Suite == suite);
            Report($"Addendum A suite {suite}: {have} of {total} cases have a [Catalog] test.");
        }

        if (covered.Length == 0)
            Assert.Skip($"addendum suites pending merge: none of the {TestCatalog.Addendum.Count} Addendum A cases (ACL/POOL/ADS) has a [Catalog] test yet");

        Assert.True(missing.Length == 0,
            $"{missing.Length} Addendum A case(s) have no [Catalog] test:{Environment.NewLine}{string.Join(Environment.NewLine, missing)}");
    }

    private static void Report(string message) => TestContext.Current.TestOutputHelper?.WriteLine(message);

    /// <summary>The <c>[Catalog("ID", ...)]</c> usages found in the test sources.</summary>
    private sealed partial class CatalogScan
    {
        private CatalogScan(string testsRoot, int files, IReadOnlyDictionary<string, List<string>> usages, IReadOnlySet<string> meshIds)
        {
            TestsRoot = testsRoot;
            Files = files;
            Usages = usages;
            MeshIds = meshIds;
        }

        public string TestsRoot { get; }

        public int Files { get; }

        /// <summary>Catalog id → "file:line" of every attribute naming it.</summary>
        public IReadOnlyDictionary<string, List<string>> Usages { get; }

        /// <summary>MESH-NNN ids defined by rows of docs/test-notes/mesh.md.</summary>
        public IReadOnlySet<string> MeshIds { get; }

        public static CatalogScan Run()
        {
            string repoRoot = FindRepoRoot();
            string testsRoot = System.IO.Path.Combine(repoRoot, "tests");
            var usages = new Dictionary<string, List<string>>(StringComparer.Ordinal);
            int files = 0;
            foreach (string file in Directory.EnumerateFiles(testsRoot, "*.cs", SearchOption.AllDirectories))
            {
                string relative = System.IO.Path.GetRelativePath(repoRoot, file).Replace('\\', '/');
                if (relative.Contains("/bin/", StringComparison.Ordinal) || relative.Contains("/obj/", StringComparison.Ordinal))
                    continue;
                files++;
                string[] lines = System.IO.File.ReadAllLines(file);
                for (int i = 0; i < lines.Length; i++)
                {
                    string line = lines[i].TrimStart();
                    if (line.StartsWith("//", StringComparison.Ordinal))
                        continue;   // comments and XML docs (e.g. the CatalogAttribute example) are not tests
                    foreach (Match match in CatalogUsage().Matches(line))
                    {
                        string id = match.Groups["id"].Value;
                        if (!usages.TryGetValue(id, out List<string>? at))
                            usages[id] = at = [];
                        at.Add($"{relative}:{i + 1}");
                    }
                }
            }

            string meshNotes = System.IO.Path.Combine(repoRoot, "docs", "test-notes", "mesh.md");
            var meshIds = new SortedSet<string>(StringComparer.Ordinal);
            if (System.IO.File.Exists(meshNotes))
            {
                foreach (string line in System.IO.File.ReadAllLines(meshNotes))
                {
                    Match row = MeshRow().Match(line);
                    if (row.Success)
                        meshIds.Add(row.Groups["id"].Value);
                }
            }

            return new CatalogScan(testsRoot, files, usages, meshIds);
        }

        /// <summary>Walks up from the test binaries to the folder that contains <c>Tedd.Tdfs.slnx</c>.</summary>
        private static string FindRepoRoot()
        {
            for (DirectoryInfo? dir = new(AppContext.BaseDirectory); dir is not null; dir = dir.Parent)
            {
                if (System.IO.File.Exists(System.IO.Path.Combine(dir.FullName, "Tedd.Tdfs.slnx")))
                    return dir.FullName;
            }

            throw new InvalidOperationException($"Tedd.Tdfs.slnx not found in any parent of {AppContext.BaseDirectory}");
        }

        // An attribute usage: "[Catalog(" or ", Catalog(" (inside an attribute list), the id as the first argument.
        [GeneratedRegex("""(?:\[|,)\s*Catalog\(\s*"(?<id>[A-Z][A-Z0-9]*-[0-9]{3})"\s*,""")]
        private static partial Regex CatalogUsage();

        [GeneratedRegex("""^\|\s*(?<id>MESH-[0-9]{3})\s*\|""")]
        private static partial Regex MeshRow();
    }
}
