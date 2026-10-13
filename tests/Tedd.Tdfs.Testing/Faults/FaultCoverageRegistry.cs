using System.Globalization;

namespace Tedd.Tdfs.Testing;

/// <summary>Records every fault-point hit so tests can prove intended points were reached.</summary>
/// <remarks>
/// Hits are recorded in the order the injector observed them, under the same lock that counts the hits of every armed
/// fault, so the <c>Occurrence</c>-th recorded hit matching an arm's <see cref="FaultMatch"/> is the hit its action ran on
/// (HAR-013).
/// </remarks>
public sealed class FaultCoverageRegistry
{
    private readonly object _gate = new();
    private readonly List<FaultContext> _hits = [];
    private TestFaults? _faults;

    internal FaultCoverageRegistry()
    {
    }

    /// <summary>All hits in order.</summary>
    public IReadOnlyList<FaultContext> Hits
    {
        get
        {
            lock (_gate)
                return [.. _hits];
        }
    }

    /// <summary>True when <paramref name="site"/> was hit (optionally on <paramref name="node"/>).</summary>
    public bool WasReached(string site, string? node = null)
    {
        ArgumentNullException.ThrowIfNull(site);
        lock (_gate)
        {
            foreach (FaultContext hit in _hits)
            {
                if (string.Equals(hit.Site, site, StringComparison.Ordinal) && (node is null || string.Equals(hit.Node, node, StringComparison.Ordinal)))
                    return true;
            }
        }

        return false;
    }

    /// <summary>Hit counts per site.</summary>
    public IReadOnlyDictionary<string, int> CountsBySite()
    {
        var counts = new SortedDictionary<string, int>(StringComparer.Ordinal);
        lock (_gate)
        {
            foreach (FaultContext hit in _hits)
                counts[hit.Site] = counts.TryGetValue(hit.Site, out int c) ? c + 1 : 1;
        }

        return counts;
    }

    /// <summary>Builds a campaign report (test plan §8.4).</summary>
    /// <remarks>
    /// <see cref="CampaignReport.Reached"/> counts the faults armed in this run whose trigger window was actually hit
    /// (<see cref="FaultArm.Triggered"/>), capped at <paramref name="applicable"/>; <see cref="CampaignReport.Completed"/>
    /// and <see cref="CampaignReport.Passed"/> count those whose action also ran to completion (an action that crashed the
    /// node or still holds the operation has not completed). An armed fault whose site was never reached, or reached only
    /// outside its occurrence window, is not reached, and is listed in <see cref="CampaignReport.Notes"/>.
    /// </remarks>
    /// <exception cref="ArgumentOutOfRangeException">A count is negative.</exception>
    /// <exception cref="ArgumentException">Applicable plus excluded cases exceed the generated cases.</exception>
    public CampaignReport CreateReport(string campaign, int generated, int applicable, int excluded)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(campaign);
        ArgumentOutOfRangeException.ThrowIfNegative(generated);
        ArgumentOutOfRangeException.ThrowIfNegative(applicable);
        ArgumentOutOfRangeException.ThrowIfNegative(excluded);
        if (applicable + excluded > generated)
            throw new ArgumentException($"Applicable ({applicable}) plus excluded ({excluded}) cases exceed the generated cases ({generated}).");

        IReadOnlyList<FaultArm> arms = _faults?.Arms ?? [];
        int reached = 0;
        int completed = 0;
        var notes = new List<string>();
        foreach (FaultArm arm in arms)
        {
            if (arm.Triggered)
            {
                reached++;
                if (arm.CompletedActions > 0)
                    completed++;
                else
                    notes.Add($"{arm.Describe()}: reached, but its action has not completed");
            }
            else
            {
                int siteHits = CountHits(arm.Match);
                notes.Add(siteHits == 0
                    ? $"{arm.Describe()}: not reached (no hit matched the site and identity)"
                    : string.Create(CultureInfo.InvariantCulture, $"{arm.Describe()}: not reached ({arm.Hits} matching hit(s), {siteHits} hit(s) of the site and identity, outside the occurrence window)"));
            }
        }

        if (excluded > 0)
            notes.Add(string.Create(CultureInfo.InvariantCulture, $"{excluded} case(s) excluded by the campaign"));
        reached = Math.Min(reached, applicable);
        completed = Math.Min(completed, reached);
        return new CampaignReport(campaign, generated, applicable, reached, completed, completed, 0, excluded, notes);
    }

    /// <summary>Number of recorded hits that match <paramref name="match"/>'s site and identity (ignoring the occurrence window).</summary>
    internal int CountHits(FaultMatch match)
    {
        ArgumentNullException.ThrowIfNull(match);
        int count = 0;
        lock (_gate)
        {
            foreach (FaultContext hit in _hits)
            {
                if (match.Matches(hit))
                    count++;
            }
        }

        return count;
    }

    internal void Attach(TestFaults faults) => _faults = faults;

    /// <summary>Records one hit. Caller holds the injector lock (hit order equals arm counting order).</summary>
    internal void Record(in FaultContext context)
    {
        lock (_gate)
            _hits.Add(context);
    }
}
