using System.Text.RegularExpressions;

namespace Tedd.Tdfs.Testing;

/// <summary>The named test cases of the test plan (section 7), parsed from the embedded <c>test-plan.md</c>.</summary>
public static partial class TestCatalog
{
    /// <summary>Number of named cases the test plan declares (§1).</summary>
    public const int ExpectedCount = 586;

    private const string ResourceName = "test-plan.md";
    private static readonly Lazy<IReadOnlyList<CatalogEntry>> Entries = new(Load, LazyThreadSafetyMode.ExecutionAndPublication);
    private static readonly Lazy<IReadOnlyDictionary<string, CatalogEntry>> ById = new(
        () => All.ToDictionary(e => e.Id, StringComparer.Ordinal), LazyThreadSafetyMode.ExecutionAndPublication);

    /// <summary>All named cases in document order.</summary>
    public static IReadOnlyList<CatalogEntry> All => Entries.Value;

    /// <summary>Suite prefixes in document order.</summary>
    public static IReadOnlyList<string> Suites => All.Select(e => e.Suite).Distinct(StringComparer.Ordinal).ToArray();

    /// <summary>The entry with <paramref name="id"/>, or null.</summary>
    public static CatalogEntry? Find(string id) =>
        id is not null && ById.Value.TryGetValue(id, out CatalogEntry? entry) ? entry : null;

    /// <summary>Parses catalog rows (<c>| ID | Level | `Name` | assertion |</c>) of section 7 from test-plan markdown.</summary>
    public static IReadOnlyList<CatalogEntry> Parse(string markdown)
    {
        ArgumentNullException.ThrowIfNull(markdown);
        var entries = new List<CatalogEntry>();
        bool inCatalog = false;
        foreach (string rawLine in markdown.Split('\n'))
        {
            string line = rawLine.TrimEnd('\r');
            if (line.StartsWith("## ", StringComparison.Ordinal))
            {
                inCatalog = line.StartsWith("## 7.", StringComparison.Ordinal);
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

    private static IReadOnlyList<CatalogEntry> Load()
    {
        using Stream stream = typeof(TestCatalog).Assembly.GetManifestResourceStream(ResourceName)
            ?? throw new InvalidOperationException($"Embedded resource '{ResourceName}' is missing from {typeof(TestCatalog).Assembly.GetName().Name}.");
        using var reader = new StreamReader(stream);
        return Parse(reader.ReadToEnd());
    }

    [GeneratedRegex(@"^\|\s*(?<suite>[A-Z]+)-(?<num>\d{3})\s*\|\s*(?<level>[USIHPM])\s*\|\s*`(?<name>[^`]+)`\s*\|(?<assertion>.*)\|\s*$", RegexOptions.CultureInvariant)]
    private static partial Regex RowPattern();
}
