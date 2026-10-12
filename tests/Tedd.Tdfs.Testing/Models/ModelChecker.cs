namespace Tedd.Tdfs.Testing;

/// <summary>Explicit-state breadth-first model checker over a deterministic transition system with canonical state keys.</summary>
public static class ModelChecker
{
    /// <summary>
    /// Explores every state reachable from <paramref name="initial"/> within <paramref name="maxDepth"/> transitions and
    /// <paramref name="maxStates"/> distinct states (identified by <paramref name="key"/>), checking
    /// <paramref name="invariant"/> (null = holds, otherwise a violation message) on every visited state including the
    /// initial one. Stops at the first violation, which is a shortest counterexample because the search is breadth-first.
    /// </summary>
    /// <remarks>
    /// <see cref="ModelCheckResult.Completed"/> is true only when no bound cut the exploration short: no state at
    /// <paramref name="maxDepth"/> has an unvisited successor and the state budget was not exhausted.
    /// </remarks>
    public static ModelCheckResult Explore<TState>(
        TState initial,
        Func<TState, IEnumerable<(string Action, TState Next)>> next,
        Func<TState, string?> invariant,
        Func<TState, string> key,
        int maxDepth,
        long maxStates)
    {
        ArgumentNullException.ThrowIfNull(next);
        ArgumentNullException.ThrowIfNull(invariant);
        ArgumentNullException.ThrowIfNull(key);
        ArgumentOutOfRangeException.ThrowIfNegative(maxDepth);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxStates);

        var bounds = new List<string> { $"maxDepth={maxDepth}", $"maxStates={maxStates}" };
        var nodes = new List<Node>();
        var visited = new HashSet<string>(StringComparer.Ordinal);
        var queue = new Queue<(TState State, int Node)>();

        string initialKey = KeyOf(key, initial);
        visited.Add(initialKey);
        nodes.Add(new Node(-1, null, 0));
        string? violation = invariant(initial);
        if (violation is not null)
            return Violated(nodes, 0, violation, initialKey, bounds);
        queue.Enqueue((initial, 0));

        int deepest = 0;
        long frontierWithSuccessors = 0;
        bool stateBoundHit = false;
        while (queue.Count > 0 && !stateBoundHit)
        {
            (TState state, int nodeIndex) = queue.Dequeue();
            int depth = nodes[nodeIndex].Depth;
            if (depth >= maxDepth)
            {
                foreach (var (_, successor) in next(state) ?? [])
                {
                    if (!visited.Contains(KeyOf(key, successor)))
                    {
                        frontierWithSuccessors++;
                        break;
                    }
                }

                continue;
            }

            foreach ((string action, TState successor) in next(state) ?? [])
            {
                string successorKey = KeyOf(key, successor);
                if (visited.Contains(successorKey))
                    continue;
                if (nodes.Count >= maxStates)
                {
                    stateBoundHit = true;
                    break;
                }

                visited.Add(successorKey);
                nodes.Add(new Node(nodeIndex, action ?? "(unnamed action)", depth + 1));
                int successorIndex = nodes.Count - 1;
                if (depth + 1 > deepest)
                    deepest = depth + 1;

                violation = invariant(successor);
                if (violation is not null)
                    return Violated(nodes, successorIndex, violation, successorKey, bounds);
                queue.Enqueue((successor, successorIndex));
            }
        }

        if (frontierWithSuccessors > 0)
            bounds.Add($"depth bound {maxDepth} reached: {frontierWithSuccessors} states at that depth have unexplored successors");
        if (stateBoundHit)
            bounds.Add($"state bound {maxStates} reached with {queue.Count + 1} discovered states not fully expanded");
        bool completed = frontierWithSuccessors == 0 && !stateBoundHit;
        return new ModelCheckResult(completed, nodes.Count, deepest, null, null, bounds);
    }

    private static string KeyOf<TState>(Func<TState, string> key, TState state) =>
        key(state) ?? throw new InvalidOperationException("The state key function returned null.");

    private static ModelCheckResult Violated(List<Node> nodes, int index, string violation, string stateKey, List<string> bounds)
    {
        var trace = new List<string>();
        for (int i = index; i > 0; i = nodes[i].Parent)
            trace.Add(nodes[i].Action!);
        trace.Reverse();
        int depth = nodes[index].Depth;
        bounds.Add($"stopped at the first violation (depth {depth})");
        string shownKey = stateKey.Length <= 200 ? stateKey : stateKey[..200] + "…";
        int deepest = nodes.Max(n => n.Depth);
        return new ModelCheckResult(false, nodes.Count, deepest, $"{violation} [state {shownKey}]", trace, bounds);
    }

    private readonly record struct Node(int Parent, string? Action, int Depth);
}
