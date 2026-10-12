namespace Tedd.Tdfs.Testing;

/// <summary>
/// Linearizability checker (Wing &amp; Gong search with Lowe's memoisation of (linearized set, model state)).
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item><see cref="OpOutcome.Ok"/> operations must be linearized inside [invoke, <see cref="HistoryOp.EffectDeadline"/>].</item>
/// <item><see cref="OpOutcome.Unknown"/> operations (timed out, outcome unknown, still in flight) may be linearized at any
/// point after their invocation, or never; they are never dropped and never assumed failed.</item>
/// <item><see cref="OpOutcome.Failed"/> operations did not take effect and are excluded.</item>
/// <item>Real-time order: an Ok operation A precedes B whenever A's effect deadline is before B's invocation.</item>
/// <item>Operations with different <see cref="HistoryOp.Key"/> are independent objects and are checked separately
/// (linearizability is compositional); give all operations of one multi-key object the same key.</item>
/// </list>
/// The search is exhaustive within <c>maxStates</c> distinct states (summed over keys); when the bound is reached the
/// result is inconclusive, never valid (HAR-017).
/// </remarks>
public static class LinearizabilityChecker
{
    /// <summary>Checks <paramref name="history"/> against <paramref name="model"/>.</summary>
    /// <exception cref="ArgumentException">Malformed history (null entries, completion before invocation).</exception>
    public static CheckResult Check<TState>(IReadOnlyList<HistoryOp> history, ISequentialModel<TState> model, long maxStates = 5_000_000)
    {
        ArgumentNullException.ThrowIfNull(history);
        ArgumentNullException.ThrowIfNull(model);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxStates);

        foreach (HistoryOp? op in history)
        {
            if (op is null)
                throw new ArgumentException("History contains null.", nameof(history));
            if (op.Key is null || op.Kind is null)
                throw new ArgumentException($"Operation #{op.Id} has no key or kind.", nameof(history));
            if (!Enum.IsDefined(op.Outcome))
                throw new ArgumentException($"Operation #{op.Id} has an undefined outcome.", nameof(history));
            if (op.CompleteTime is long complete && complete < op.InvokeTime)
                throw new ArgumentException($"Operation #{op.Id} completes ({complete}) before it is invoked ({op.InvokeTime}).", nameof(history));
            if (op.ResolveTime is long resolved && resolved < op.InvokeTime)
                throw new ArgumentException($"Operation #{op.Id} is resolved ({resolved}) before it is invoked ({op.InvokeTime}).", nameof(history));
        }

        List<IGrouping<string, HistoryOp>> partitions = history
            .Where(op => op.Outcome != OpOutcome.Failed)
            .GroupBy(op => op.Key, StringComparer.Ordinal)
            .OrderBy(g => g.Count())
            .ThenBy(g => g.Key, StringComparer.Ordinal)
            .ToList();
        int excluded = history.Count(op => op.Outcome == OpOutcome.Failed);

        long explored = 0;
        int okTotal = 0;
        int unknownTotal = 0;
        for (int p = 0; p < partitions.Count; p++)
        {
            IGrouping<string, HistoryOp> partition = partitions[p];
            HistoryOp[] ops = partition.OrderBy(op => op.InvokeTime).ThenBy(op => op.Id).ToArray();
            long[] invoke = ops.Select(op => op.InvokeTime).ToArray();
            long[] deadline = ops.Select(op => op.Outcome == OpOutcome.Ok ? op.EffectDeadline ?? long.MaxValue : long.MaxValue).ToArray();
            bool[] required = ops.Select(op => op.Outcome == OpOutcome.Ok).ToArray();
            int ok = required.Count(r => r);
            int unknown = ops.Length - ok;
            okTotal += ok;
            unknownTotal += unknown;

            OrderSearchResult result = OrderSearch.Run(
                invoke,
                deadline,
                required,
                model.Initial,
                model.Key,
                (TState state, int item, out TState next) => model.Step(state, ops[item], out next),
                maxStates - explored);
            explored += result.StatesExplored;

            switch (result.Status)
            {
                case OrderSearchStatus.NotFound:
                    return CheckResult.Invalid(
                        $"Key '{partition.Key}': no linearization of {ok} completed (+{unknown} unknown) operations exists. " +
                        $"Longest legal prefix places {result.BestRequiredPlaced}/{ok} completed operations: " +
                        HistoryFormat.Ops(result.BestPrefix.Select(i => HistoryFormat.Op(ops[i]))) +
                        $". Model state after it: {HistoryFormat.Text(result.BestStateKey, 96)}. " +
                        "None of the operations allowed next is consistent with that state: " +
                        HistoryFormat.Ops(result.BestCandidates.Select(i => HistoryFormat.Op(ops[i]))) + ".",
                        explored);
                case OrderSearchStatus.BudgetExhausted:
                    return CheckResult.Inconclusive(
                        $"State budget of {maxStates} exhausted on key '{partition.Key}' (key {p + 1} of {partitions.Count}, " +
                        $"{ok} completed and {unknown} unknown operations; best prefix placed {result.BestRequiredPlaced}/{ok}).",
                        explored);
            }
        }

        return CheckResult.Valid(
            $"Linearizable: {okTotal} completed and {unknownTotal} unknown operations over {partitions.Count} keys " +
            $"({excluded} failed operations excluded).",
            explored);
    }
}
