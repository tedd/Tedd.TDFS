using System.Text.RegularExpressions;

namespace Tedd.Tdfs.Testing;

/// <summary>
/// The named test cases of the test plan (section 7), parsed from the embedded <c>test-plan.md</c>, and the cases of
/// FSD Addendum A (section A5), parsed from the embedded <c>fsd-addendum-acl-pools-streams.md</c>.
/// </summary>
public static partial class TestCatalog
{
    /// <summary>Number of named cases the test plan declares (§1).</summary>
    public const int ExpectedCount = 586;

    /// <summary>Number of named cases Addendum A declares (§A5: ACL 34, POOL 18, ADS 26).</summary>
    public const int AddendumExpectedCount = 78;

    private const string ResourceName = "test-plan.md";
    private const string AddendumResourceName = "fsd-addendum-acl-pools-streams.md";
    private const string PlanCatalogHeading = "## 7.";
    private const string AddendumCatalogHeading = "## A5.";
    private static readonly Lazy<IReadOnlyList<CatalogEntry>> Entries = new(
        () => Load(ResourceName, Parse), LazyThreadSafetyMode.ExecutionAndPublication);
    private static readonly Lazy<IReadOnlyList<CatalogEntry>> AddendumEntries = new(
        () => Load(AddendumResourceName, ParseAddendum), LazyThreadSafetyMode.ExecutionAndPublication);
    private static readonly Lazy<IReadOnlyDictionary<string, CatalogEntry>> ById = new(
        () => All.ToDictionary(e => e.Id, StringComparer.Ordinal), LazyThreadSafetyMode.ExecutionAndPublication);
    private static readonly Lazy<IReadOnlyDictionary<string, CatalogEntry>> AnyById = new(
        () => All.Concat(Addendum).ToDictionary(e => e.Id, StringComparer.Ordinal), LazyThreadSafetyMode.ExecutionAndPublication);

    /// <summary>All named cases of the test plan in document order (Addendum A cases are in <see cref="Addendum"/>).</summary>
    public static IReadOnlyList<CatalogEntry> All => Entries.Value;

    /// <summary>The named cases of FSD Addendum A (suites ACL, POOL, ADS) in document order.</summary>
    public static IReadOnlyList<CatalogEntry> Addendum => AddendumEntries.Value;

    /// <summary>Suite prefixes of <see cref="Addendum"/> in document order.</summary>
    public static IReadOnlyList<string> AddendumSuites => Addendum.Select(e => e.Suite).Distinct(StringComparer.Ordinal).ToArray();

    /// <summary>Suite prefixes in document order.</summary>
    public static IReadOnlyList<string> Suites => All.Select(e => e.Suite).Distinct(StringComparer.Ordinal).ToArray();

    /// <summary>The test-plan entry with <paramref name="id"/>, or null (Addendum A ids: <see cref="FindAny"/>).</summary>
    public static CatalogEntry? Find(string id) =>
        id is not null && ById.Value.TryGetValue(id, out CatalogEntry? entry) ? entry : null;

    /// <summary>The entry with <paramref name="id"/> from the test plan or Addendum A, or null.</summary>
    public static CatalogEntry? FindAny(string id) =>
        id is not null && AnyById.Value.TryGetValue(id, out CatalogEntry? entry) ? entry : null;

    /// <summary>Parses catalog rows (<c>| ID | Level | `Name` | assertion |</c>) of section 7 from test-plan markdown.</summary>
    public static IReadOnlyList<CatalogEntry> Parse(string markdown) => ParseSection(markdown, PlanCatalogHeading);

    /// <summary>Parses catalog rows of section A5 from Addendum A markdown.</summary>
    public static IReadOnlyList<CatalogEntry> ParseAddendum(string markdown) => ParseSection(markdown, AddendumCatalogHeading);

    private static IReadOnlyList<CatalogEntry> ParseSection(string markdown, string sectionHeadingPrefix)
    {
        ArgumentNullException.ThrowIfNull(markdown);
        var entries = new List<CatalogEntry>();
        bool inCatalog = false;
        foreach (string rawLine in markdown.Split('\n'))
        {
            string line = rawLine.TrimEnd('\r');
            if (line.StartsWith("## ", StringComparison.Ordinal))
            {
                inCatalog = line.StartsWith(sectionHeadingPrefix, StringComparison.Ordinal);
                continue;
            }

            if (!inCatalog)
                continue;
            Match match = RowPattern().Match(line);
            if (!match.Success)
                continue;
            string suite = match.Groups["suite"].Value;
            entries.Add(new CatalogEntry(
                suite + "-" + match.Groups["num"].Value,
                suite,
                match.Groups["level"].Value,
                match.Groups["name"].Value,
                match.Groups["assertion"].Value.Trim()));
        }

        return entries;
    }

    private static IReadOnlyList<CatalogEntry> Load(string resourceName, Func<string, IReadOnlyList<CatalogEntry>> parse)
    {
        using Stream stream = typeof(TestCatalog).Assembly.GetManifestResourceStream(resourceName)
            ?? throw new InvalidOperationException($"Embedded resource '{resourceName}' is missing from {typeof(TestCatalog).Assembly.GetName().Name}.");
        using var reader = new StreamReader(stream);
        return parse(reader.ReadToEnd());
    }

    [GeneratedRegex(@"^\|\s*(?<suite>[A-Z]+)-(?<num>\d{3})\s*\|\s*(?<level>[USIHPM])\s*\|\s*`(?<name>[^`]+)`\s*\|(?<assertion>.*)\|\s*$", RegexOptions.CultureInvariant)]
    private static partial Regex RowPattern();
}
