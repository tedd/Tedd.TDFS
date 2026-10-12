namespace Tedd.Tdfs.Testing;

/// <summary>
/// A deterministic sequential specification used by <see cref="LinearizabilityChecker"/>. States must be treated as
/// immutable values: <see cref="Step"/> never mutates its input state.
/// </summary>
/// <typeparam name="TState">State type (may be a nullable reference type, for example <c>byte[]?</c>).</typeparam>
public interface ISequentialModel<TState>
{
    /// <summary>The initial state.</summary>
    TState Initial { get; }

    /// <summary>
    /// Applies <paramref name="op"/> to <paramref name="state"/>. Returns false when the operation's recorded output is
    /// impossible in that state. Outputs of non-<see cref="OpOutcome.Ok"/> operations are not authoritative and must not
    /// be checked; such operations only contribute their possible effect.
    /// </summary>
    bool Step(TState state, HistoryOp op, out TState next);

    /// <summary>
    /// A canonical key for <paramref name="state"/>: equal keys must mean behaviourally equal states. Used to memoise the
    /// search, so a collision would make the checker unsound.
    /// </summary>
    string Key(TState state);
}
