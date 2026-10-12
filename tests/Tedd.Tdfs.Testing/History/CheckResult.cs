namespace Tedd.Tdfs.Testing;

/// <summary>Result of a history check.</summary>
/// <param name="IsValid">True when a legal explanation (linearization/serialization) was found.</param>
/// <param name="IsInconclusive">True when the search was cut off by its bound; never a pass (HAR-017).</param>
/// <param name="Explanation">Human-readable explanation (counterexample, witness summary or bound reached).</param>
/// <param name="StatesExplored">Number of distinct search states explored.</param>
public sealed record CheckResult(bool IsValid, bool IsInconclusive, string Explanation, long StatesExplored)
{
    /// <summary>A successful check.</summary>
    public static CheckResult Valid(string explanation, long statesExplored) => new(true, false, explanation, statesExplored);

    /// <summary>A definite violation.</summary>
    public static CheckResult Invalid(string explanation, long statesExplored) => new(false, false, explanation, statesExplored);

    /// <summary>A check cut off by its bound.</summary>
    public static CheckResult Inconclusive(string explanation, long statesExplored) => new(false, true, explanation, statesExplored);

    /// <summary>Fails the test unless <see cref="IsValid"/> and not <see cref="IsInconclusive"/>.</summary>
    public void AssertValid()
    {
        if (IsInconclusive)
            Assert.Fail($"History check inconclusive (not a pass) after {StatesExplored} states: {Explanation}");
        if (!IsValid)
            Assert.Fail($"History check failed after {StatesExplored} states: {Explanation}");
    }

    /// <inheritdoc />
    public override string ToString() =>
        (IsInconclusive ? "Inconclusive" : IsValid ? "Valid" : "Invalid") + $" ({StatesExplored} states): {Explanation}";
}
