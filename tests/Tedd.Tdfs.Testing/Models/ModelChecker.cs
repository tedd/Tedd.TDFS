using System.Collections.Concurrent;

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

    /// <summary>
    /// Parallel variant of <see cref="Explore{TState}"/> for large state spaces whose states are expensive to rebuild:
    /// <paramref name="degreeOfParallelism"/> workers (default: one per processor) expand states from a shared work stack
    /// (depth-first-like order, so only the frontier is held in memory) against one shared visited set of 128-bit state
    /// keys. Every reachable state within the bounds is visited exactly once whatever the interleaving of the workers, so a
    /// completed exploration covers the same states as <see cref="Explore{TState}"/> and
    /// <see cref="ModelCheckResult.StatesExplored"/> is deterministic; which of several violations is reported (with the path
    /// that reached it) may differ between runs. Completion follows the same rule: no state at <paramref name="maxDepth"/>
    /// with an unvisited successor and the state budget not exhausted.
    /// </summary>
    /// <remarks>
    /// <paramref name="next"/>, <paramref name="invariant"/> and <paramref name="key"/> are called concurrently for different
    /// states (each state is expanded by one worker only) and must not share mutable state. A state may be reached first
    /// through a longer path than its shortest one; that only matters when <paramref name="maxDepth"/> is close to the
    /// longest schedule, so the depth bound should be set well above it.
    /// </remarks>
    public static ModelCheckResult ExploreParallel<TState>(
        TState initial,
        Func<TState, IEnumerable<(string Action, TState Next)>> next,
        Func<TState, string?> invariant,
        Func<TState, UInt128> key,
        int maxDepth,
        long maxStates,
        int? degreeOfParallelism = null)
    {
        ArgumentNullException.ThrowIfNull(key);
        var visited = new ConcurrentDictionary<UInt128, byte>();
        return ExploreParallelCore(initial, next, invariant, state => visited.ContainsKey(key(state)), state => visited.TryAdd(key(state), 0), maxDepth,
            maxStates, degreeOfParallelism, "exact keys");
    }

    /// <summary>
    /// Parallel exploration with simulation-based pruning (subsumption): <paramref name="abstraction"/> splits a state into a
    /// core (everything its invariant and its future behaviour depend on except what the abstraction lists separately), the
    /// budgets it has consumed and a multiset of pending items (for example messages in flight). The caller guarantees that
    /// a state with the same core, componentwise no more consumed budget and a superset of the items can do everything the
    /// other one can (it may always leave the extra items unused) and satisfies the invariant exactly when it does. A state
    /// simulated by a state that was already visited is then skipped: every behaviour from it is explored from the visited
    /// one, so a completed exploration still covers every reachable behaviour and every reachable core within the bounds.
    /// </summary>
    /// <remarks>
    /// Otherwise as <see cref="ExploreParallel{TState}"/>. <see cref="ModelCheckResult.StatesExplored"/> counts the states
    /// that were not skipped; it can vary slightly between runs, because whether a state is skipped depends on whether a
    /// state simulating it was visited first.
    /// </remarks>
    public static ModelCheckResult ExploreParallelWithSubsumption<TState>(
        TState initial,
        Func<TState, IEnumerable<(string Action, TState Next)>> next,
        Func<TState, string?> invariant,
        Func<TState, SubsumptionKey> abstraction,
        int maxDepth,
        long maxStates,
        int? degreeOfParallelism = null)
    {
        ArgumentNullException.ThrowIfNull(abstraction);
        var exact = new ConcurrentDictionary<UInt128, byte>();
        var buckets = new ConcurrentDictionary<UInt128, SubsumptionBucket>();
        bool Visit(TState state)
        {
            SubsumptionKey key = abstraction(state);
            if (!exact.TryAdd(Fingerprint(key), 0))
                return false;
            return buckets.GetOrAdd(key.Core, static _ => new SubsumptionBucket()).TryAdd(key);
        }

        bool Seen(TState state)
        {
            SubsumptionKey key = abstraction(state);
            return exact.ContainsKey(Fingerprint(key)) || (buckets.TryGetValue(key.Core, out SubsumptionBucket? bucket) && bucket.SimulatedByRecent(key));
        }

        return ExploreParallelCore(initial, next, invariant, Seen, Visit, maxDepth, maxStates, degreeOfParallelism, "subsumption");
    }

    /// <summary>
    /// A 128-bit fingerprint of the whole of <paramref name="key"/> (exact duplicates): as for
    /// <see cref="ExploreParallel{TState}"/>, distinct states are assumed to have distinct 128-bit keys.
    /// </summary>
    private static UInt128 Fingerprint(SubsumptionKey key)
    {
        var hash = new System.IO.Hashing.XxHash128();
        UInt128 core = key.Core;
        hash.Append(System.Runtime.InteropServices.MemoryMarshal.AsBytes(new ReadOnlySpan<UInt128>(in core)));
        hash.Append(System.Runtime.InteropServices.MemoryMarshal.AsBytes(key.Consumed.AsSpan()));
        hash.Append(System.Runtime.InteropServices.MemoryMarshal.AsBytes(key.Items.AsSpan()));
        return hash.GetCurrentHashAsUInt128();
    }

    /// <summary>
    /// The most recently visited states of one core, which a new state of that core is checked against for simulation.
    /// Checking only recent states keeps a visit cheap however many states share a core; it only means that some simulated
    /// states are visited anyway (the pruning is an optimisation; every pruned state is simulated by a visited one). Readers
    /// never lock (many workers check the same busy cores): every slot holds an immutable entry published atomically.
    /// </summary>
    private sealed class SubsumptionBucket
    {
        private const int Window = 256;
        private readonly Lock _gate = new();
        private readonly Entry?[] _recent = new Entry?[Window];
        private int _next;

        /// <summary>Records <paramref name="key"/> (not visited before); false when a recently visited state simulates it.</summary>
        public bool TryAdd(SubsumptionKey key)
        {
            if (SimulatedByRecent(key))
                return false;
            var entry = new Entry(key);
            lock (_gate)
            {
                Volatile.Write(ref _recent[_next], entry);
                _next = (_next + 1) % Window;
            }

            return true;
        }

        /// <summary>True when a recently visited state simulates <paramref name="key"/>.</summary>
        public bool SimulatedByRecent(SubsumptionKey key)
        {
            for (int i = 0; i < Window; i++)
            {
                Entry? entry = Volatile.Read(ref _recent[i]);
                if (entry is not null && Simulates(entry.Key, key))
                    return true;
            }

            return false;
        }

        private sealed class Entry(SubsumptionKey key)
        {
            public SubsumptionKey Key { get; } = key;
        }

        private static bool Simulates(SubsumptionKey bigger, SubsumptionKey smaller)
        {
            if (bigger.Consumed.Length != smaller.Consumed.Length || bigger.Items.Length < smaller.Items.Length)
                return false;
            for (int i = 0; i < bigger.Consumed.Length; i++)
            {
                if (bigger.Consumed[i] > smaller.Consumed[i])
                    return false;
            }

            // Multiset inclusion of two sorted arrays.
            int j = 0;
            foreach (UInt128 item in smaller.Items)
            {
                while (j < bigger.Items.Length && bigger.Items[j] < item)
                    j++;
                if (j == bigger.Items.Length || bigger.Items[j] != item)
                    return false;
                j++;
            }

            return true;
        }
    }

    /// <summary>
    /// The parallel exploration shared by <see cref="ExploreParallel{TState}"/> and
    /// <see cref="ExploreParallelWithSubsumption{TState}"/>: <paramref name="seen"/> tells whether a state needs no visit and
    /// <paramref name="visit"/> records a state, returning false when it needs none (both called concurrently).
    /// </summary>
    private static ModelCheckResult ExploreParallelCore<TState>(
        TState initial,
        Func<TState, IEnumerable<(string Action, TState Next)>> next,
        Func<TState, string?> invariant,
        Func<TState, bool> seen,
        Func<TState, bool> visit,
        int maxDepth,
        long maxStates,
        int? degreeOfParallelism,
        string pruning)
    {
        ArgumentNullException.ThrowIfNull(next);
        ArgumentNullException.ThrowIfNull(invariant);
        ArgumentOutOfRangeException.ThrowIfNegative(maxDepth);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxStates);
        int workers = degreeOfParallelism ?? Environment.ProcessorCount;
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(workers);

        var bounds = new List<string> { $"maxDepth={maxDepth}", $"maxStates={maxStates}", $"order=parallel depth-first ({workers} workers, {pruning})" };
        visit(initial);
        string? initialViolation = invariant(initial);
        if (initialViolation is not null)
        {
            bounds.Add("stopped at the first violation (depth 0)");
            return new ModelCheckResult(false, 1, 0, $"{initialViolation} [initial state]", [], bounds);
        }

        var work = new ConcurrentStack<Frame<TState>>();
        work.Push(new Frame<TState>(initial, 0, null));
        var gate = new object();
        long states = 1;
        int deepest = 0;
        long frontierWithSuccessors = 0;
        int active = 0;
        int stop = 0;
        bool stateBoundHit = false;
        string? violation = null;
        List<string>? trace = null;
        Exception? failure = null;

        void Worker()
        {
            try
            {
                var spin = new SpinWait();
                while (Volatile.Read(ref stop) == 0)
                {
                    Interlocked.Increment(ref active);
                    if (!work.TryPop(out Frame<TState> frame))
                    {
                        Interlocked.Decrement(ref active);
                        if (Volatile.Read(ref active) == 0 && work.IsEmpty)
                            return;
                        spin.SpinOnce();
                        continue;
                    }

                    spin.Reset();
                    try
                    {
                        Expand(frame);
                    }
                    finally
                    {
                        Interlocked.Decrement(ref active);
                    }
                }
            }
            catch (Exception ex)
            {
                lock (gate)
                    failure ??= ex;
                Volatile.Write(ref stop, 1);
            }
        }

        void Expand(Frame<TState> frame)
        {
            foreach ((string action, TState successor) in next(frame.State) ?? [])
            {
                if (Volatile.Read(ref stop) != 0)
                    return;
                if (frame.Depth >= maxDepth)
                {
                    if (seen(successor))
                        continue;

                    // A state at the depth bound with an unvisited successor: the exploration is cut short there.
                    Interlocked.Increment(ref frontierWithSuccessors);
                    return;
                }

                if (Interlocked.Read(ref states) >= maxStates)
                {
                    if (seen(successor))
                        continue;
                    lock (gate)
                        stateBoundHit = true;
                    Volatile.Write(ref stop, 1);
                    return;
                }

                // Records the successor unless it needs no visit (visited, or simulated by a visited state).
                if (!visit(successor))
                    continue;
                Interlocked.Increment(ref states);
                int depth = frame.Depth + 1;
                int observed;
                while (depth > (observed = Volatile.Read(ref deepest)) && Interlocked.CompareExchange(ref deepest, depth, observed) != observed)
                {
                }

                var trail = new Trail(action ?? "(unnamed action)", frame.Trail);
                string? found = invariant(successor);
                if (found is not null)
                {
                    lock (gate)
                    {
                        if (violation is null)
                        {
                            violation = found;
                            trace = [];
                            for (Trail? t = trail; t is not null; t = t.Parent)
                                trace.Add(t.Action);
                            trace.Reverse();
                            bounds.Add($"stopped at the first violation found (depth {depth})");
                        }
                    }

                    Volatile.Write(ref stop, 1);
                    return;
                }

                work.Push(new Frame<TState>(successor, depth, trail));
            }
        }

        var threads = new Thread[workers];
        for (int i = 0; i < workers; i++)
        {
            threads[i] = new Thread(Worker) { IsBackground = true, Name = "model-checker-" + i.ToString(System.Globalization.CultureInfo.InvariantCulture) };
            threads[i].Start();
        }

        foreach (Thread thread in threads)
            thread.Join();
        if (failure is not null)
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Throw(failure);
        if (violation is not null)
            return new ModelCheckResult(false, Interlocked.Read(ref states), deepest, violation, trace, bounds);
        if (frontierWithSuccessors > 0)
            bounds.Add($"depth bound {maxDepth} reached: {frontierWithSuccessors} states at that depth have unexplored successors");
        if (stateBoundHit)
            bounds.Add($"state bound {maxStates} reached with {work.Count} discovered states not fully expanded");
        bool completed = frontierWithSuccessors == 0 && !stateBoundHit;
        return new ModelCheckResult(completed, Interlocked.Read(ref states), deepest, null, null, bounds);
    }

    private readonly record struct Frame<TState>(TState State, int Depth, Trail? Trail);

    /// <summary>The actions that reached a state, as a list linked towards the initial state.</summary>
    private sealed record Trail(string Action, Trail? Parent);

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
