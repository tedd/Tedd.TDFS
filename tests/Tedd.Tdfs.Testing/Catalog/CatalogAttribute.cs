using Xunit.v3;

namespace Tedd.Tdfs.Testing;

/// <summary>
/// Links a test method to its catalog row (test plan, or FSD Addendum A suites ACL/POOL/ADS) and emits xunit traits:
/// <c>TestId</c>, <c>Suite</c> (the id prefix before '-'), <c>Level</c> and one <c>Invariant</c> trait per invariant.
/// </summary>
/// <example><c>[Fact, Catalog("OPEN-001", "U", "INV-02", "INV-07")]</c>, <c>[Fact, Catalog("ACL-004", "U", "INV-12")]</c></example>
[AttributeUsage(AttributeTargets.Method)]
public sealed class CatalogAttribute : Attribute, ITraitAttribute
{
    /// <summary>Valid level letters (test plan §2).</summary>
    public const string Levels = "USIHPM";

    /// <summary>Creates the attribute.</summary>
    /// <param name="testId">Catalog id "SUITE-NNN".</param>
    /// <param name="level">Level letter: U, S, I, H, P or M.</param>
    /// <param name="invariants">Invariant ids (for example "INV-02").</param>
    /// <exception cref="ArgumentException">Malformed id, level or invariant.</exception>
    public CatalogAttribute(string testId, string level, params string[] invariants)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(testId);
        ArgumentException.ThrowIfNullOrWhiteSpace(level);
        int dash = testId.IndexOf('-', StringComparison.Ordinal);
        if (dash <= 0 || dash == testId.Length - 1)
            throw new ArgumentException($"Test id '{testId}' must have the form SUITE-NNN.", nameof(testId));
        if (level.Length != 1 || !Levels.Contains(level[0], StringComparison.Ordinal))
            throw new ArgumentException($"Level '{level}' must be one of {string.Join(", ", Levels.ToCharArray())}.", nameof(level));
        invariants ??= [];
        foreach (string invariant in invariants)
        {
            if (string.IsNullOrWhiteSpace(invariant))
                throw new ArgumentException("Invariant ids must not be empty.", nameof(invariants));
        }

        TestId = testId;
        Suite = testId[..dash];
        Level = level;
        Invariants = invariants.ToArray();
    }

    /// <summary>Catalog id.</summary>
    public string TestId { get; }

    /// <summary>Suite prefix (before '-').</summary>
    public string Suite { get; }

    /// <summary>Level letter.</summary>
    public string Level { get; }

    /// <summary>Invariant ids.</summary>
    public IReadOnlyList<string> Invariants { get; }

    /// <inheritdoc />
    public IReadOnlyCollection<KeyValuePair<string, string>> GetTraits()
    {
        var traits = new List<KeyValuePair<string, string>>(3 + Invariants.Count)
        {
            new("TestId", TestId),
            new("Suite", Suite),
            new("Level", Level),
        };
        foreach (string invariant in Invariants)
            traits.Add(new("Invariant", invariant));
        return traits;
    }
}
