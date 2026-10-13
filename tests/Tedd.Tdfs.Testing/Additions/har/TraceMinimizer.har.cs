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
    public static async Task<TraceMinimizationResult<TStep>> MinimizeAsync<TStep>(IReadOnlyList<TStep> failingTrace,
        Func<IReadOnlyList<TStep>, Task<string?>> run, int maxRuns, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(failingTrace);
        ArgumentNullException.ThrowIfNull(run);
        if (failingTrace.Count == 0)
            throw new ArgumentException("A trace to minimize needs at least one step.", nameof(failingTrace));
        ArgumentOutOfRangeException.ThrowIfLessThan(maxRuns, 1);
        TStep[] original = [.. failingTrace];

        ct.ThrowIfCancellationRequested();
        string? invariant;
        try
        {
            invariant = await run(original);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw new ArgumentException("The run of the original trace threw instead of reporting an invariant; there is nothing to minimize.",
                nameof(failingTrace), ex);
        }

        if (invariant is null)
            throw new ArgumentException("The original trace passes; a passing trace cannot be minimized.", nameof(failingTrace));

        var search = new Search<TStep>(original, run, maxRuns, invariant, ct);
        search.Remember(Enumerable.Range(0, original.Length).ToArray(), sameFailure: true);
        int[] current = Enumerable.Range(0, original.Length).ToArray();

        // ddmin: complements first, then subsets, refining the granularity.
        int granularity = 2;
        while (current.Length >= 2 && !search.Exhausted)
        {
            int[][] chunks = Split(current, granularity);
            int[]? reduced = null;
            foreach (int[] chunk in chunks)
            {
                int[] complement = current.Except(chunk).ToArray();
                if (complement.Length == 0)
                    continue;
                if (await search.FailsSameWayAsync(complement))
                {
                    reduced = complement;
                    granularity = Math.Max(granularity - 1, 2);
                    break;
                }

                if (search.Exhausted)
                    break;
            }

            if (reduced is null && !search.Exhausted)
            {
                foreach (int[] chunk in chunks)
                {
                    if (chunk.Length == current.Length)
                        continue;
                    if (await search.FailsSameWayAsync(chunk))
                    {
                        reduced = chunk;
                        granularity = 2;
                        break;
                    }

                    if (search.Exhausted)
                        break;
                }
            }

            if (reduced is not null)
            {
                current = reduced;
                continue;
            }

            if (search.Exhausted || granularity >= current.Length)
                break;
            granularity = Math.Min(granularity * 2, current.Length);
        }

        // Verify 1-minimality: every single-step removal must not reproduce the invariant (a removal that does is taken and
        // the verification restarts on the smaller trace).
        bool oneMinimal = false;
        while (!search.Exhausted)
        {
            int[]? smaller = null;
            for (int i = 0; i < current.Length && !search.Exhausted; i++)
            {
                int[] candidate = [.. current[..i], .. current[(i + 1)..]];
                if (await search.FailsSameWayAsync(candidate))
                {
                    smaller = candidate;
                    break;
                }
            }

            if (search.Exhausted)
                break;
            if (smaller is null)
            {
                oneMinimal = true;
                break;
            }

            current = smaller;
        }

        TStep[] minimal = current.Select(i => original[i]).ToArray();
        return new TraceMinimizationResult<TStep>(original, minimal, invariant, search.Runs, oneMinimal, search.Exhausted);
    }

    /// <summary>Splits <paramref name="indices"/> into <paramref name="parts"/> contiguous chunks whose sizes differ by at most one.</summary>
    private static int[][] Split(int[] indices, int parts)
    {
        parts = Math.Min(parts, indices.Length);
        var chunks = new int[parts][];
        int start = 0;
        for (int i = 0; i < parts; i++)
        {
            int size = (indices.Length - start) / (parts - i);
            chunks[i] = indices[start..(start + size)];
            start += size;
        }

        return chunks;
    }

    /// <summary>Executes candidates within the budget, caching each candidate's verdict by its index set.</summary>
    private sealed class Search<TStep>(TStep[] original, Func<IReadOnlyList<TStep>, Task<string?>> run, int maxRuns, string invariant,
        CancellationToken ct)
    {
        private readonly Dictionary<string, bool> _verdicts = new(StringComparer.Ordinal);

        /// <summary>Executions of the run delegate so far (the initial confirmation included).</summary>
        public int Runs { get; private set; } = 1;

        /// <summary>True once a candidate could not be executed because the budget was used up.</summary>
        public bool Exhausted { get; private set; }

        public void Remember(int[] candidate, bool sameFailure) => _verdicts[Key(candidate)] = sameFailure;

        /// <summary>True when the candidate's run reports the same invariant (cached; never executed twice).</summary>
        public async Task<bool> FailsSameWayAsync(int[] candidate)
        {
            string key = Key(candidate);
            if (_verdicts.TryGetValue(key, out bool known))
                return known;
            if (Runs >= maxRuns)
            {
                Exhausted = true;
                return false;
            }

            ct.ThrowIfCancellationRequested();
            Runs++;
            string? reported;
            try
            {
                reported = await run(candidate.Select(i => original[i]).ToArray());
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                reported = null; // a candidate that throws is a different failure, never the same one
            }

            bool same = string.Equals(reported, invariant, StringComparison.Ordinal);
            _verdicts[key] = same;
            return same;
        }

        private static string Key(int[] candidate) => string.Join(',', candidate);
    }
}
