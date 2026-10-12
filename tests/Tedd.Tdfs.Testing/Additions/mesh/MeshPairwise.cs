using System.Collections.ObjectModel;

namespace Tedd.Tdfs.Testing.Mesh;

/// <summary>A non-critical matrix dimension; the first value is the preferred default (used on ties).</summary>
/// <param name="Name">Dimension name (row key).</param>
/// <param name="Values">Values, preferred default first.</param>
public sealed record MeshDimension(string Name, IReadOnlyList<string> Values);

/// <summary>Result of <see cref="MeshPairwise.Generate"/>.</summary>
/// <param name="Rows">Generated rows: every valid critical row once (in input order), then extra rows needed for pair coverage.</param>
/// <param name="CriticalRows">Number of critical rows given.</param>
/// <param name="InvalidCriticalRows">Critical rows for which no valid non-critical assignment exists (excluded, listed).</param>
/// <param name="ExtraRows">Rows added beyond the critical ones to cover remaining pairs.</param>
/// <param name="CoverablePairs">Pairs (dimension=value, dimension=value) involving a non-critical dimension that occur in at least one valid row.</param>
/// <param name="CoveredPairs">Coverable pairs covered by <paramref name="Rows"/> (equals <paramref name="CoverablePairs"/> on success).</param>
public sealed record MeshPairwiseResult(IReadOnlyList<IReadOnlyDictionary<string, string>> Rows, int CriticalRows,
    IReadOnlyList<string> InvalidCriticalRows, int ExtraRows, int CoverablePairs, int CoveredPairs);

/// <summary>
/// Deterministic constraint-aware covering-array generator: every critical row (the exhaustive product of the
/// critical dimensions, e.g. node count × failure kind) is kept exactly once and completed with the non-critical
/// assignment that covers the most still-uncovered pairs; extra rows are added until every pair of values that can
/// occur together in a valid row (at least one side non-critical) is covered. Pairs among critical dimensions are
/// exhaustive by construction. Ties prefer earlier values (defaults first), so the result is stable.
/// </summary>
public static class MeshPairwise
{
    /// <summary>Generates the rows.</summary>
    /// <param name="criticalRows">Exhaustive critical combinations (all with the same keys).</param>
    /// <param name="nonCritical">Dimensions reduced pairwise.</param>
    /// <param name="isValid">Validity constraint over a complete row.</param>
    /// <param name="pairFilter">
    /// Which dimension pairs must be covered (default: every pair with at least one non-critical dimension). Pairs where
    /// both dimensions are critical are always exhaustive and never counted.
    /// </param>
    public static MeshPairwiseResult Generate(IReadOnlyList<IReadOnlyDictionary<string, string>> criticalRows,
        IReadOnlyList<MeshDimension> nonCritical, Func<IReadOnlyDictionary<string, string>, bool> isValid,
        Func<string, string, bool>? pairFilter = null)
    {
        ArgumentNullException.ThrowIfNull(criticalRows);
        ArgumentNullException.ThrowIfNull(nonCritical);
        ArgumentNullException.ThrowIfNull(isValid);

        // Item ids for (dimension, value).
        var items = new Dictionary<(string, string), int>();
        string[] criticalNames = criticalRows.Count == 0 ? [] : criticalRows[0].Keys.Order(StringComparer.Ordinal).ToArray();
        foreach (IReadOnlyDictionary<string, string> row in criticalRows)
        {
            foreach (string name in criticalNames)
                items.TryAdd((name, row[name]), items.Count);
        }

        foreach (MeshDimension dim in nonCritical)
        {
            foreach (string value in dim.Values)
                items.TryAdd((dim.Name, value), items.Count);
        }

        var nonCriticalNames = new HashSet<string>(nonCritical.Select(d => d.Name), StringComparer.Ordinal);
        int k = items.Count;
        var coverable = new bool[k * k];
        var covered = new bool[k * k];

        // All non-critical assignments, defaults first (odometer over value indices).
        List<string[]> assignments = [];
        int[] counter = new int[nonCritical.Count];
        while (true)
        {
            assignments.Add(nonCritical.Select((d, i) => d.Values[counter[i]]).ToArray());
            int pos = nonCritical.Count - 1;
            while (pos >= 0 && ++counter[pos] == nonCritical[pos].Values.Count)
            {
                counter[pos] = 0;
                pos--;
            }

            if (pos < 0)
                break;
        }

        IReadOnlyDictionary<string, string> Build(IReadOnlyDictionary<string, string> critical, string[] assignment)
        {
            var row = new Dictionary<string, string>(critical, StringComparer.Ordinal);
            for (int i = 0; i < nonCritical.Count; i++)
                row[nonCritical[i].Name] = assignment[i];
            return new ReadOnlyDictionary<string, string>(row);
        }

        IEnumerable<int> PairKeys(IReadOnlyDictionary<string, string> row)
        {
            KeyValuePair<string, string>[] entries = row.OrderBy(kv => kv.Key, StringComparer.Ordinal).ToArray();
            for (int i = 0; i < entries.Length; i++)
            {
                for (int j = i + 1; j < entries.Length; j++)
                {
                    if (!nonCriticalNames.Contains(entries[i].Key) && !nonCriticalNames.Contains(entries[j].Key))
                        continue;
                    if (pairFilter is not null && !pairFilter(entries[i].Key, entries[j].Key))
                        continue;
                    int a = items[(entries[i].Key, entries[i].Value)];
                    int b = items[(entries[j].Key, entries[j].Value)];
                    yield return a < b ? a * k + b : b * k + a;
                }
            }
        }

        // Valid assignments per critical row (with their pair keys) and the coverable universe.
        var validPerRow = new List<List<(IReadOnlyDictionary<string, string> Row, int[] Keys)>>(criticalRows.Count);
        var invalid = new List<string>();
        foreach (IReadOnlyDictionary<string, string> critical in criticalRows)
        {
            var valid = new List<(IReadOnlyDictionary<string, string>, int[])>();
            foreach (string[] assignment in assignments)
            {
                IReadOnlyDictionary<string, string> row = Build(critical, assignment);
                if (!isValid(row))
                    continue;
                int[] keys = PairKeys(row).ToArray();
                valid.Add((row, keys));
                foreach (int key in keys)
                    coverable[key] = true;
            }

            if (valid.Count == 0)
                invalid.Add(string.Join(";", critical.Select(kv => kv.Key + "=" + kv.Value)));
            validPerRow.Add(valid);
        }

        int Gain(int[] keys)
        {
            int gain = 0;
            foreach (int key in keys)
            {
                if (!covered[key])
                    gain++;
            }

            return gain;
        }

        void Mark(int[] keys)
        {
            foreach (int key in keys)
                covered[key] = true;
        }

        var rows = new List<IReadOnlyDictionary<string, string>>();
        foreach (List<(IReadOnlyDictionary<string, string> Row, int[] Keys)> valid in validPerRow)
        {
            if (valid.Count == 0)
                continue;
            int best = 0;
            int bestGain = Gain(valid[0].Keys);
            for (int i = 1; i < valid.Count; i++)
            {
                int gain = Gain(valid[i].Keys);
                if (gain > bestGain)
                {
                    best = i;
                    bestGain = gain;
                }
            }

            rows.Add(valid[best].Row);
            Mark(valid[best].Keys);
        }

        int criticalCount = rows.Count;
        while (true)
        {
            (IReadOnlyDictionary<string, string> Row, int[] Keys)? best = null;
            int bestGain = 0;
            foreach (List<(IReadOnlyDictionary<string, string> Row, int[] Keys)> valid in validPerRow)
            {
                foreach ((IReadOnlyDictionary<string, string> Row, int[] Keys) candidate in valid)
                {
                    int gain = Gain(candidate.Keys);
                    if (gain > bestGain)
                    {
                        best = candidate;
                        bestGain = gain;
                    }
                }
            }

            if (best is null)
                break;
            rows.Add(best.Value.Row);
            Mark(best.Value.Keys);
        }

        int coverableCount = coverable.Count(c => c);
        int coveredCount = Enumerable.Range(0, k * k).Count(i => coverable[i] && covered[i]);
        return new MeshPairwiseResult(rows, criticalRows.Count, invalid, rows.Count - criticalCount, coverableCount, coveredCount);
    }

    /// <summary>All uncovered coverable pairs of a result (diagnostics; recomputes from the rows).</summary>
    public static IReadOnlyList<string> UncoveredPairs(MeshPairwiseResult result, IReadOnlyList<IReadOnlyDictionary<string, string>> criticalRows,
        IReadOnlyList<MeshDimension> nonCritical, Func<IReadOnlyDictionary<string, string>, bool> isValid, Func<string, string, bool>? pairFilter = null)
    {
        ArgumentNullException.ThrowIfNull(result);
        var nonCriticalNames = new HashSet<string>(nonCritical.Select(d => d.Name), StringComparer.Ordinal);

        IEnumerable<string> Pairs(IReadOnlyDictionary<string, string> row)
        {
            KeyValuePair<string, string>[] entries = row.OrderBy(kv => kv.Key, StringComparer.Ordinal).ToArray();
            for (int i = 0; i < entries.Length; i++)
            {
                for (int j = i + 1; j < entries.Length; j++)
                {
                    if ((nonCriticalNames.Contains(entries[i].Key) || nonCriticalNames.Contains(entries[j].Key))
                        && (pairFilter is null || pairFilter(entries[i].Key, entries[j].Key)))
                        yield return $"{entries[i].Key}={entries[i].Value} & {entries[j].Key}={entries[j].Value}";
                }
            }
        }

        var covered = new HashSet<string>(result.Rows.SelectMany(Pairs), StringComparer.Ordinal);
        var coverable = new SortedSet<string>(StringComparer.Ordinal);
        foreach (IReadOnlyDictionary<string, string> critical in criticalRows)
        {
            foreach (IReadOnlyDictionary<string, string> row in Expand(critical, nonCritical))
            {
                if (isValid(row))
                    coverable.UnionWith(Pairs(row));
            }
        }

        return coverable.Where(p => !covered.Contains(p)).ToArray();
    }

    /// <summary>Every completion of a critical row with all non-critical value combinations.</summary>
    public static IEnumerable<IReadOnlyDictionary<string, string>> Expand(IReadOnlyDictionary<string, string> critical, IReadOnlyList<MeshDimension> nonCritical)
    {
        IEnumerable<Dictionary<string, string>> rows = [new Dictionary<string, string>(critical, StringComparer.Ordinal)];
        foreach (MeshDimension dim in nonCritical)
        {
            rows = rows.SelectMany(r => dim.Values.Select(v => new Dictionary<string, string>(r, StringComparer.Ordinal) { [dim.Name] = v }));
        }

        return rows.Select(r => (IReadOnlyDictionary<string, string>)new ReadOnlyDictionary<string, string>(r));
    }
}
