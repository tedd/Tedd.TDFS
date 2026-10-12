namespace Tedd.Tdfs.Testing;

/// <summary>Selects fault-point hits for a <see cref="FaultArm"/>. Null members match anything.</summary>
/// <param name="Site">Site name from <see cref="FaultSites"/>; may end with '*' for a prefix match.</param>
/// <param name="Node">Node name.</param>
/// <param name="Volume">Volume name.</param>
/// <param name="Operation">Operation kind.</param>
/// <param name="PathPrefix">Ordinal path prefix (only hits that carry a path can match).</param>
/// <param name="Occurrence">The 1-based matching hit at which the action first triggers.</param>
/// <param name="Count">Number of consecutive matching hits (from <paramref name="Occurrence"/>) that trigger the action.</param>
public sealed record FaultMatch(string Site, string? Node = null, string? Volume = null, DfsOperationKind? Operation = null,
    string? PathPrefix = null, int Occurrence = 1, int Count = 1)
{
    /// <summary>True when <see cref="Site"/> ends with '*'.</summary>
    public bool IsSitePrefix => Site.EndsWith('*');

    /// <summary>Validates the match.</summary>
    /// <exception cref="ArgumentException">Empty site, occurrence below 1 or count below 1.</exception>
    public void Validate()
    {
        if (string.IsNullOrEmpty(Site))
            throw new ArgumentException("Site must be a site name or a prefix ending with '*'.", nameof(Site));
        int star = Site.IndexOf('*', StringComparison.Ordinal);
        if (star >= 0 && star != Site.Length - 1)
            throw new ArgumentException("'*' is only allowed at the end of Site.", nameof(Site));
        if (Occurrence < 1)
            throw new ArgumentOutOfRangeException(nameof(Occurrence), Occurrence, "Occurrence is 1-based.");
        if (Count < 1)
            throw new ArgumentOutOfRangeException(nameof(Count), Count, "Count must be at least 1.");
    }

    /// <summary>True when <paramref name="site"/> matches <see cref="Site"/> (exact, or prefix when it ends with '*').</summary>
    public bool MatchesSite(string site)
    {
        ArgumentNullException.ThrowIfNull(site);
        return IsSitePrefix
            ? site.StartsWith(Site.AsSpan(0, Site.Length - 1), StringComparison.Ordinal)
            : string.Equals(site, Site, StringComparison.Ordinal);
    }

    /// <summary>
    /// True when <paramref name="context"/> matches site, node, volume, operation and path prefix. Occurrence/count
    /// windows are applied by the arm, which counts matching hits.
    /// </summary>
    public bool Matches(in FaultContext context)
    {
        if (context.Site is null || !MatchesSite(context.Site))
            return false;
        if (Node is not null && !string.Equals(Node, context.Node, StringComparison.Ordinal))
            return false;
        if (Volume is not null && !string.Equals(Volume, context.Volume, StringComparison.Ordinal))
            return false;
        if (Operation is not null && Operation.Value != context.Operation)
            return false;
        if (PathPrefix is not null && (context.Path is null || !context.Path.StartsWith(PathPrefix, StringComparison.Ordinal)))
            return false;
        return true;
    }

    /// <summary>True when the <paramref name="matchingHit"/>-th (1-based) matching hit falls in the trigger window.</summary>
    public bool TriggersOn(int matchingHit) => matchingHit >= Occurrence && matchingHit - Occurrence < Count;
}
