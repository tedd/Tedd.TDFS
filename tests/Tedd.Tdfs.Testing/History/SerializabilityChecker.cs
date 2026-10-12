using System.Collections.Immutable;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;

namespace Tedd.Tdfs.Testing;

/// <summary>Strict-serializability checker for key/value transaction histories over file paths (INV-04).</summary>
/// <remarks>
/// Searches for a serial order of all committed (<see cref="OpOutcome.Ok"/>) transactions, optionally interleaved with
/// any subset of <see cref="OpOutcome.Unknown"/> ones (each may or may not have committed), excluding
/// <see cref="OpOutcome.Failed"/> ones, such that real-time order is respected (a committed transaction whose effect
/// deadline precedes another's begin is ordered first) and every placed transaction's <see cref="TxRecord.Reads"/> and
/// <see cref="TxRecord.PredicateReads"/> equal the state at its position. The search memoises (placed set, state) and is
/// bounded by <c>maxStates</c>; reaching the bound yields an inconclusive result, never a pass.
/// </remarks>
public static class SerializabilityChecker
{
    private const int InlineStateKeyChars = 512;
    private static readonly ConditionalWeakTable<ImmutableSortedDictionary<string, string>, string> Keys = new();

    /// <summary>Checks strict serializability of <paramref name="transactions"/> starting from <paramref name="initial"/> (null values = absent).</summary>
    /// <exception cref="ArgumentException">Malformed records (null members, duplicate ids, completion before begin).</exception>
    public static CheckResult CheckStrict(IReadOnlyList<TxRecord> transactions, IReadOnlyDictionary<string, string?> initial, long maxStates = 2_000_000)
    {
        ArgumentNullException.ThrowIfNull(transactions);
        ArgumentNullException.ThrowIfNull(initial);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxStates);

        var ids = new HashSet<string>(StringComparer.Ordinal);
        foreach (TxRecord? tx in transactions)
        {
            if (tx is null)
                throw new ArgumentException("Transactions contain null.", nameof(transactions));
            if (tx.Id is null || tx.Reads is null || tx.Writes is null || tx.PredicateReads is null)
                throw new ArgumentException($"Transaction '{tx.Id}' has a null member.", nameof(transactions));
            if (!ids.Add(tx.Id))
                throw new ArgumentException($"Duplicate transaction id '{tx.Id}'.", nameof(transactions));
            if (!Enum.IsDefined(tx.Outcome))
                throw new ArgumentException($"Transaction '{tx.Id}' has an undefined outcome.", nameof(transactions));
            if (tx.CompleteTime is long complete && complete < tx.InvokeTime)
                throw new ArgumentException($"Transaction '{tx.Id}' completes ({complete}) before it begins ({tx.InvokeTime}).", nameof(transactions));
            if (tx.ResolveTime is long resolved && resolved < tx.InvokeTime)
                throw new ArgumentException($"Transaction '{tx.Id}' is resolved ({resolved}) before it begins ({tx.InvokeTime}).", nameof(transactions));
            foreach ((string directory, IReadOnlySet<string> names) in tx.PredicateReads)
            {
                if (directory is null || names is null)
                    throw new ArgumentException($"Transaction '{tx.Id}' has a null predicate read.", nameof(transactions));
            }
        }

        TxRecord[] txs = transactions
            .Where(t => t.Outcome != OpOutcome.Failed)
            .OrderBy(t => t.InvokeTime)
            .ThenBy(t => t.Id, StringComparer.Ordinal)
            .ToArray();
        int excluded = transactions.Count - txs.Length;
        long[] invoke = txs.Select(t => t.InvokeTime).ToArray();
        long[] deadline = txs.Select(t => t.Outcome == OpOutcome.Ok ? t.EffectDeadline ?? long.MaxValue : long.MaxValue).ToArray();
        bool[] required = txs.Select(t => t.Outcome == OpOutcome.Ok).ToArray();
        int committed = required.Count(r => r);
        int unknown = txs.Length - committed;

        ImmutableSortedDictionary<string, string> start = ImmutableSortedDictionary.CreateRange(
            StringComparer.Ordinal,
            initial.Where(kv => kv.Value is not null).Select(kv => new KeyValuePair<string, string>(kv.Key, kv.Value!)));

        OrderSearchResult result = OrderSearch.Run(
            invoke,
            deadline,
            required,
            start,
            StateKey,
            (ImmutableSortedDictionary<string, string> state, int item, out ImmutableSortedDictionary<string, string> next) =>
                TryApply(state, txs[item], out next),
            maxStates);

        return result.Status switch
        {
            OrderSearchStatus.Found => CheckResult.Valid(
                $"Strictly serializable: {committed} committed and {unknown} unknown transactions ({excluded} aborted excluded). " +
                "Serial order: " + HistoryFormat.Ops(result.Witness.Select(i => txs[i].Id)) + ".",
                result.StatesExplored),
            OrderSearchStatus.NotFound => CheckResult.Invalid(
                $"No strict serialization of {committed} committed (+{unknown} unknown) transactions exists. " +
                $"Longest legal prefix places {result.BestRequiredPlaced}/{committed} committed transactions: " +
                HistoryFormat.Ops(result.BestPrefix.Select(i => txs[i].Id)) +
                $". State after it: {HistoryFormat.Text(result.BestStateKey, 160)}. " +
                "None of the transactions allowed next observes that state: " +
                HistoryFormat.Ops(result.BestCandidates.Select(i => Describe(txs[i]))) + ".",
                result.StatesExplored),
            _ => CheckResult.Inconclusive(
                $"State budget of {maxStates} exhausted ({committed} committed, {unknown} unknown transactions; " +
                $"best prefix placed {result.BestRequiredPlaced}/{committed}).",
                result.StatesExplored),
        };
    }

    private static bool TryApply(ImmutableSortedDictionary<string, string> state, TxRecord tx, out ImmutableSortedDictionary<string, string> next)
    {
        next = state;
        foreach ((string path, string? expected) in tx.Reads)
        {
            string? actual = state.TryGetValue(path, out string? value) ? value : null;
            if (!string.Equals(actual, expected, StringComparison.Ordinal))
                return false;
        }

        foreach ((string directory, IReadOnlySet<string> names) in tx.PredicateReads)
        {
            if (!NamesMatch(state, directory, names))
                return false;
        }

        if (tx.Writes.Count == 0)
            return true;
        ImmutableSortedDictionary<string, string>.Builder builder = state.ToBuilder();
        foreach ((string path, string? value) in tx.Writes)
        {
            if (value is null)
                builder.Remove(path);
            else
                builder[path] = value;
        }

        next = builder.ToImmutable();
        return true;
    }

    private static bool NamesMatch(ImmutableSortedDictionary<string, string> state, string directory, IReadOnlySet<string> names)
    {
        string prefix = directory.EndsWith('/') ? directory : directory + "/";
        int found = 0;
        foreach (string path in state.Keys)
        {
            if (!path.StartsWith(prefix, StringComparison.Ordinal))
                continue;
            string name = path[prefix.Length..];
            if (name.Length == 0 || name.Contains('/', StringComparison.Ordinal))
                continue;
            if (!names.Contains(name))
                return false;
            found++;
        }

        return found == names.Count;
    }

    private static string StateKey(ImmutableSortedDictionary<string, string> state) =>
        Keys.GetValue(state, static s =>
        {
            var sb = new StringBuilder();
            foreach ((string path, string value) in s)
            {
                sb.Append(path.Length).Append(':').Append(path).Append('=').Append(value.Length).Append(':').Append(value).Append(';');
            }

            if (sb.Length <= InlineStateKeyChars)
                return sb.ToString();
            return "h" + Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(sb.ToString())));
        });

    private static string Describe(TxRecord tx)
    {
        static string Map(IReadOnlyDictionary<string, string?> map) =>
            "{" + string.Join(", ", map.Take(6).Select(kv => kv.Key + "=" + (kv.Value is null ? "∅" : HistoryFormat.Text(kv.Value, 24)))) +
            (map.Count > 6 ? ", ..." : string.Empty) + "}";

        string predicates = tx.PredicateReads.Count == 0
            ? string.Empty
            : " list=" + string.Join(", ", tx.PredicateReads.Take(3).Select(p => p.Directory + ":[" + string.Join(",", p.Names.Order(StringComparer.Ordinal).Take(6)) + "]"));
        string window = "[" + tx.InvokeTime + ", " + (tx.EffectDeadline?.ToString(System.Globalization.CultureInfo.InvariantCulture) ?? "∞") + "]";
        return $"{tx.Id} {tx.Outcome} {window} reads={Map(tx.Reads)} writes={Map(tx.Writes)}{predicates}";
    }
}
