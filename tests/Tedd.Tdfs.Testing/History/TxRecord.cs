namespace Tedd.Tdfs.Testing;

/// <summary>One transaction of a history checked by <see cref="SerializabilityChecker"/>.</summary>
/// <param name="Id">Transaction identity (unique within the history).</param>
/// <param name="InvokeTime">Logical time the transaction began.</param>
/// <param name="CompleteTime">Logical time the commit outcome was observed; null while in flight.</param>
/// <param name="Outcome">Ok = committed, Failed = definitely aborted, Unknown = may or may not have committed.</param>
/// <param name="Reads">
/// Values read from the store, keyed by file path (null = observed absent). Record only reads that were not preceded by
/// the transaction's own write to the same path.
/// </param>
/// <param name="Writes">Values written, keyed by path (null = deleted). Applied atomically at the serialization point.</param>
/// <param name="PredicateReads">
/// Directory enumerations: the exact set of names observed directly under <c>Directory</c> (files only; a name is a key
/// <c>Directory + "/" + name</c> without further '/'), evaluated before the transaction's own writes.
/// </param>
public sealed record TxRecord(string Id, long InvokeTime, long? CompleteTime, OpOutcome Outcome,
    IReadOnlyDictionary<string, string?> Reads, IReadOnlyDictionary<string, string?> Writes,
    IReadOnlyList<(string Directory, IReadOnlySet<string> Names)> PredicateReads)
{
    /// <summary>Logical time at which an unknown commit outcome was resolved; null when never ambiguous.</summary>
    public long? ResolveTime { get; init; }

    /// <summary>End of the window in which a committed transaction's serialization point must lie; null (unbounded) otherwise.</summary>
    public long? EffectDeadline => Outcome == OpOutcome.Ok ? ResolveTime ?? CompleteTime : null;
}
