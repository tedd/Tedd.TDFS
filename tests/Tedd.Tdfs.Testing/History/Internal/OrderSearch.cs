namespace Tedd.Tdfs.Testing;

/// <summary>Outcome of an <see cref="OrderSearch"/>.</summary>
internal enum OrderSearchStatus
{
    Found,
    NotFound,
    BudgetExhausted,
}

/// <summary>Result of an <see cref="OrderSearch"/>.</summary>
internal sealed record OrderSearchResult(
    OrderSearchStatus Status,
    long StatesExplored,
    IReadOnlyList<int> Witness,
    int BestRequiredPlaced,
    IReadOnlyList<int> BestPrefix,
    string BestStateKey,
    IReadOnlyList<int> BestCandidates);

/// <summary>Applies item <paramref name="item"/> to <paramref name="state"/>; false when impossible in that state.</summary>
internal delegate bool OrderStep<TState>(TState state, int item, out TState next);

/// <summary>
/// Wing &amp; Gong / Lowe style search for a total order of interval items that respects real-time precedence and a
/// sequential specification, with memoisation.
/// </summary>
/// <remarks>
/// <para>
/// Items are indexed in ascending invocation order. Item <c>a</c> must precede item <c>b</c> when <c>a</c> is required
/// (it definitely took effect) and <c>deadline[a] &lt; invoke[b]</c>; every item satisfies <c>deadline ≥ invoke</c>.
/// Optional items (unknown outcome) may be placed at any point after their invocation or never; they never constrain
/// other items. The search succeeds once every required item is placed.
/// </para>
/// <para>
/// A search state is (R = placed required items, U = placed optional items, model state). Soundness of the pruning:
/// </para>
/// <list type="number">
/// <item>Which items may come next depends only on R (optional items impose no precedence), and their effect only on
/// the model state. Hence every continuation available from (R, U, s) is also available from (R, U', s) for any
/// U' ⊆ U (the extra optional items can simply stay unplaced). So once (R, U', s) has been explored, any
/// (R, U, s) with U ⊇ U' is skipped (dominance memoisation; equal states are the special case U = U').</item>
/// <item>Placing an optional item that leaves the model state key unchanged is skipped: the resulting state is
/// dominated by the current one by rule 1.</item>
/// <item>Absorption: when the current frame was reached by placing an optional item o, a candidate c is skipped if
/// applying c at the parent frame (which has the same R, hence the same candidates) succeeds with the same resulting
/// state key: (R+c, U+o, s') is dominated by (R+c, U, s'), which the parent explores itself (for example an unknown write
/// that is overwritten before anyone observes it).</item>
/// </list>
/// Candidates are tried in invocation order (required and optional interleaved), which follows the recorded real-time
/// order. Rules 1 and 2 are always applied; rule 3 changes which branches are visited and in what order, so the search
/// runs as a portfolio: a first pass without absorption (usually finds witnesses for legal histories fastest) on a small
/// slice of the budget, then a second pass with absorption (usually refutes illegal histories with many unknown
/// operations fastest) on the rest. Each pass is a complete search, so whichever ends first gives a definite answer.
/// </remarks>
internal static class OrderSearch
{
    /// <summary>Budget share of the first (non-absorbing) pass.</summary>
    private const long FirstPassDivisor = 20;

    /// <summary>Minimum budget of the first pass.</summary>
    private const long FirstPassMinimum = 20_000;

    public static OrderSearchResult Run<TState>(
        long[] invoke,
        long[] deadline,
        bool[] required,
        TState initial,
        Func<TState, string> key,
        OrderStep<TState> step,
        long budget)
    {
        long firstBudget = Math.Min(budget, Math.Max(FirstPassMinimum, budget / FirstPassDivisor));
        OrderSearchResult first = RunPass(invoke, deadline, required, initial, key, step, firstBudget, absorb: false);
        if (first.Status != OrderSearchStatus.BudgetExhausted || first.StatesExplored >= budget)
            return first;
        OrderSearchResult second = RunPass(invoke, deadline, required, initial, key, step, budget - first.StatesExplored, absorb: true);
        return second with { StatesExplored = first.StatesExplored + second.StatesExplored };
    }

    private static OrderSearchResult RunPass<TState>(
        long[] invoke,
        long[] deadline,
        bool[] required,
        TState initial,
        Func<TState, string> key,
        OrderStep<TState> step,
        long budget,
        bool absorb)
    {
        int n = invoke.Length;
        var requiredItems = new List<int>();
        var optionalItems = new List<int>();
        var slot = new int[n]; // position within requiredItems or optionalItems
        for (int i = 0; i < n; i++)
        {
            if (i > 0 && invoke[i] < invoke[i - 1])
                throw new ArgumentException("Items must be sorted by invocation time.", nameof(invoke));
            if (deadline[i] < invoke[i])
                throw new ArgumentException("Every item's deadline must not precede its invocation.", nameof(deadline));
            if (required[i])
            {
                slot[i] = requiredItems.Count;
                requiredItems.Add(i);
            }
            else
            {
                slot[i] = optionalItems.Count;
                optionalItems.Add(i);
            }
        }

        int requiredTotal = requiredItems.Count;
        var placedRequired = new ulong[(requiredTotal + 63) / 64];
        var placedOptional = new ulong[(optionalItems.Count + 63) / 64];
        var memo = new Dictionary<MemoKey, List<ulong[]>>();

        string initialKey = key(initial);
        Remember(memo, placedRequired, placedOptional, initialKey);
        long explored = 1;

        if (requiredTotal == 0)
            return new OrderSearchResult(OrderSearchStatus.Found, explored, [], 0, [], initialKey, []);
        if (budget <= explored)
            return new OrderSearchResult(OrderSearchStatus.BudgetExhausted, explored, [], 0, [], initialKey, []);

        int[] initialCandidates = Candidates(0);
        var frames = new List<Frame<TState>> { new(initial, initialKey, initialCandidates, -1, 0) };
        int requiredPlaced = 0;
        int bestRequired = 0;
        int[] bestPrefix = [];
        string bestKey = initialKey;
        int[] bestCandidates = initialCandidates;

        while (frames.Count > 0)
        {
            Frame<TState> frame = frames[^1];
            if (frame.Next == frame.Candidates.Length)
            {
                frames.RemoveAt(frames.Count - 1);
                if (frame.Via >= 0)
                {
                    Unmark(frame.Via);
                    if (required[frame.Via])
                        requiredPlaced--;
                }

                continue;
            }

            int c = frame.Candidates[frame.Next++];
            if (!step(frame.State, c, out TState next))
                continue;
            string nextKey = key(next);
            if (!required[c] && string.Equals(nextKey, frame.Key, StringComparison.Ordinal))
                continue;
            if (absorb && frame.Via >= 0 && !required[frame.Via])
            {
                // Absorption: this frame was reached by placing an optional item. If c applied at the parent (same R, so c
                // is equally eligible there) reaches the same state, the parent's own branch dominates this one.
                Frame<TState> parent = frames[^2];
                if (step(parent.State, c, out TState direct) && string.Equals(key(direct), nextKey, StringComparison.Ordinal))
                    continue;
            }

            Mark(c);
            if (!Remember(memo, placedRequired, placedOptional, nextKey))
            {
                Unmark(c);
                continue;
            }

            explored++;
            int firstUnplaced = frame.FirstUnplacedRequired;
            if (required[c])
            {
                requiredPlaced++;
                while (firstUnplaced < requiredTotal && IsSet(placedRequired, firstUnplaced))
                    firstUnplaced++;
            }

            if (requiredPlaced == requiredTotal)
            {
                var witness = new List<int>(frames.Count);
                for (int i = 1; i < frames.Count; i++)
                    witness.Add(frames[i].Via);
                witness.Add(c);
                return new OrderSearchResult(OrderSearchStatus.Found, explored, witness, requiredPlaced, witness, nextKey, []);
            }

            int[] nextCandidates = Candidates(firstUnplaced);
            frames.Add(new Frame<TState>(next, nextKey, nextCandidates, c, firstUnplaced));

            if (requiredPlaced > bestRequired)
            {
                bestRequired = requiredPlaced;
                bestPrefix = PathOf(frames);
                bestKey = nextKey;
                bestCandidates = nextCandidates;
            }

            if (explored >= budget)
                return new OrderSearchResult(OrderSearchStatus.BudgetExhausted, explored, [], bestRequired, bestPrefix, bestKey, bestCandidates);
        }

        return new OrderSearchResult(OrderSearchStatus.NotFound, explored, [], bestRequired, bestPrefix, bestKey, bestCandidates);

        // Unplaced items that may be placed next: invocation not later than the earliest unplaced required deadline.
        int[] Candidates(int firstUnplacedRequired)
        {
            long bound = long.MaxValue;
            for (int p = firstUnplacedRequired; p < requiredTotal; p++)
            {
                int i = requiredItems[p];
                if (invoke[i] > bound)
                    break; // later items have deadline >= invoke > bound
                if (!IsSet(placedRequired, p) && deadline[i] < bound)
                    bound = deadline[i];
            }

            // Merge required and optional candidates in invocation (= index) order.
            var result = new List<int>();
            int pr = firstUnplacedRequired;
            int po = 0;
            while (true)
            {
                while (pr < requiredTotal && IsSet(placedRequired, pr))
                    pr++;
                while (po < optionalItems.Count && IsSet(placedOptional, po))
                    po++;
                int nextRequired = pr < requiredTotal && invoke[requiredItems[pr]] <= bound ? requiredItems[pr] : int.MaxValue;
                int nextOptional = po < optionalItems.Count && invoke[optionalItems[po]] <= bound ? optionalItems[po] : int.MaxValue;
                if (nextRequired == int.MaxValue && nextOptional == int.MaxValue)
                    break;
                if (nextRequired < nextOptional)
                {
                    result.Add(nextRequired);
                    pr++;
                }
                else
                {
                    result.Add(nextOptional);
                    po++;
                }
            }

            return result.ToArray();
        }

        void Mark(int item)
        {
            if (required[item])
                Set(placedRequired, slot[item]);
            else
                Set(placedOptional, slot[item]);
        }

        void Unmark(int item)
        {
            if (required[item])
                Clear(placedRequired, slot[item]);
            else
                Clear(placedOptional, slot[item]);
        }
    }

    /// <summary>
    /// Records (R, U, state) unless an already-explored (R, U', state) with U' ⊆ U dominates it; returns false when
    /// dominated. Stored sets that the new U dominates are dropped so each bucket stays an antichain.
    /// </summary>
    private static bool Remember(Dictionary<MemoKey, List<ulong[]>> memo, ulong[] placedRequired, ulong[] placedOptional, string stateKey)
    {
        var memoKey = new MemoKey(placedRequired, stateKey, copy: false);
        if (!memo.TryGetValue(memoKey, out List<ulong[]>? bucket))
        {
            memo.Add(new MemoKey(placedRequired, stateKey, copy: true), [(ulong[])placedOptional.Clone()]);
            return true;
        }

        foreach (ulong[] seen in bucket)
        {
            if (IsSubset(seen, placedOptional))
                return false;
        }

        bucket.RemoveAll(seen => IsSubset(placedOptional, seen));
        bucket.Add((ulong[])placedOptional.Clone());
        return true;
    }

    private static bool IsSubset(ulong[] subset, ulong[] superset)
    {
        for (int w = 0; w < subset.Length; w++)
        {
            if ((subset[w] & ~superset[w]) != 0)
                return false;
        }

        return true;
    }

    private static int[] PathOf<TState>(List<Frame<TState>> frames)
    {
        var path = new int[frames.Count - 1];
        for (int i = 1; i < frames.Count; i++)
            path[i - 1] = frames[i].Via;
        return path;
    }

    private static bool IsSet(ulong[] bits, int i) => (bits[i >> 6] & (1UL << (i & 63))) != 0;

    private static void Set(ulong[] bits, int i) => bits[i >> 6] |= 1UL << (i & 63);

    private static void Clear(ulong[] bits, int i) => bits[i >> 6] &= ~(1UL << (i & 63));

    private sealed class Frame<TState>(TState state, string key, int[] candidates, int via, int firstUnplacedRequired)
    {
        public TState State { get; } = state;

        public string Key { get; } = key;

        public int[] Candidates { get; } = candidates;

        public int Via { get; } = via;

        public int FirstUnplacedRequired { get; } = firstUnplacedRequired;

        public int Next { get; set; }
    }

    private readonly struct MemoKey : IEquatable<MemoKey>
    {
        private readonly ulong[] _bits;
        private readonly string _state;
        private readonly int _hash;

        public MemoKey(ulong[] bits, string state, bool copy)
        {
            _bits = copy ? (ulong[])bits.Clone() : bits;
            _state = state;
            var hash = new HashCode();
            foreach (ulong word in bits)
                hash.Add(word);
            hash.Add(state, StringComparer.Ordinal);
            _hash = hash.ToHashCode();
        }

        public bool Equals(MemoKey other) =>
            _hash == other._hash
            && string.Equals(_state, other._state, StringComparison.Ordinal)
            && _bits.AsSpan().SequenceEqual(other._bits);

        public override bool Equals(object? obj) => obj is MemoKey other && Equals(other);

        public override int GetHashCode() => _hash;
    }
}
