namespace Tedd.Tdfs.Testing;

/// <summary>Result of <see cref="TraceMinimizer.MinimizeAsync{TStep}"/>.</summary>
/// <typeparam name="TStep">Trace step type (an operation, fault or schedule decision).</typeparam>
/// <param name="Original">The failing trace that was minimized (unchanged).</param>
/// <param name="Minimal">
/// The smallest failing trace found: an order-preserving subsequence of <see cref="Original"/> for which the run still
/// reports <see cref="Invariant"/>.
/// </param>
/// <param name="Invariant">The invariant reported by the run of <see cref="Original"/> (and of <see cref="Minimal"/>).</param>
/// <param name="Runs">Number of executions of the run delegate, including the initial confirmation of the original.</param>
/// <param name="IsOneMinimal">
/// True when every trace obtained by removing a single step from <see cref="Minimal"/> was executed and none of them
/// reported <see cref="Invariant"/> (1-minimality was verified, not assumed).
/// </param>
/// <param name="BudgetExhausted">True when minimization stopped because the run budget was reached (never 1-minimal then).</param>
public sealed record TraceMinimizationResult<TStep>(IReadOnlyList<TStep> Original, IReadOnlyList<TStep> Minimal, string Invariant, int Runs,
    bool IsOneMinimal, bool BudgetExhausted);

/// <summary>
/// Delta-debugging (ddmin) shrinker for failing operation/fault traces (HAR-014). The harness owns it so that every
/// failure found by a randomized or exhaustive campaign can be reduced to a minimal replay artifact.
/// </summary>
public static class TraceMinimizer
{
    /// <summary>
    /// Minimizes <paramref name="failingTrace"/>.
    /// <list type="number">
    /// <item>Runs <paramref name="failingTrace"/> once; when the run returns null (no failure) throws
    /// <see cref="ArgumentException"/> (a passing trace cannot be minimized).</item>
    /// <item>Applies ddmin over order-preserving subsequences (complements first, then subsets, refining the granularity),
    /// accepting a candidate only when its run returns the <b>same</b> invariant id (ordinal) as the original; a candidate
    /// that fails a different invariant, passes, or throws is rejected (exceptions from the run delegate other than
    /// <see cref="OperationCanceledException"/> are treated as "different failure").</item>
    /// <item>Finally verifies 1-minimality by running every single-step removal of the result.</item>
    /// </list>
    /// Candidates are executed sequentially, in a deterministic order, and identical candidates are never re-run (results
    /// are cached by the candidate's index set). Stops early when <paramref name="maxRuns"/> executions were made, returning
    /// the smallest failing trace found so far with <see cref="TraceMinimizationResult{TStep}.BudgetExhausted"/> set.
    /// </summary>
    /// <param name="failingTrace">The failing trace (at least one step).</param>
    /// <param name="run">
    /// Executes a candidate trace from the same seed and starting images and returns the violated invariant id, or null
    /// when the candidate passes.
    /// </param>
    /// <param name="maxRuns">Execution budget (at least 1).</param>
    /// <param name="ct">Cancellation.</param>
    public static Task<TraceMinimizationResult<TStep>> MinimizeAsync<TStep>(IReadOnlyList<TStep> failingTrace,
        Func<IReadOnlyList<TStep>, Task<string?>> run, int maxRuns, CancellationToken ct = default) =>
        throw Stub.NotYet("TraceMinimizer.MinimizeAsync");
}
