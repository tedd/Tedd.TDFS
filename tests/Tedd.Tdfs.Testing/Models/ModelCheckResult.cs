namespace Tedd.Tdfs.Testing;

/// <summary>Result of a bounded explicit-state exploration (<see cref="ModelChecker"/>).</summary>
/// <param name="Completed">True when the whole state space within the bounds was explored with no bound reached and no violation.</param>
/// <param name="StatesExplored">Distinct states visited (including the initial state).</param>
/// <param name="MaxDepth">Deepest depth visited (initial state = 0).</param>
/// <param name="Violation">The first invariant violation found (shortest by BFS), or null.</param>
/// <param name="CounterexampleTrace">Actions from the initial state to the violating state, or null.</param>
/// <param name="Bounds">The configured bounds and every bound that was actually reached (MOD-012 report material).</param>
public sealed record ModelCheckResult(bool Completed, long StatesExplored, int MaxDepth, string? Violation, IReadOnlyList<string>? CounterexampleTrace,
    IReadOnlyList<string> Bounds)
{
    /// <summary>Fails the test when a violation was found, printing the counterexample trace.</summary>
    public void AssertNoViolation()
    {
        if (Violation is null)
            return;
        string trace = CounterexampleTrace is null || CounterexampleTrace.Count == 0
            ? "(initial state)"
            : string.Join(Environment.NewLine, CounterexampleTrace.Select((a, i) => $"  {i + 1}. {a}"));
        Assert.Fail($"Invariant violated after {StatesExplored} states: {Violation}{Environment.NewLine}Counterexample:{Environment.NewLine}{trace}");
    }

    /// <summary>Fails the test unless the bounded exploration completed (a cut-off exploration is never a pass, HAR-017).</summary>
    public void AssertCompleted()
    {
        if (Completed)
            return;
        string reason = Violation is not null ? "stopped at a violation: " + Violation : "bounds reached";
        Assert.Fail($"Model exploration incomplete ({reason}) after {StatesExplored} states, depth {MaxDepth}. Bounds: {string.Join("; ", Bounds)}");
    }
}
