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

/// <summary>
/// A sequential model whose states only grow in a way that lets the checker discard a partial order early: a state from
/// which a pending completed operation can never be satisfied, whatever is applied later, is a dead end.
/// </summary>
/// <typeparam name="TState">State type.</typeparam>
/// <remarks>
/// Purely an optimisation of the search (it never changes a verdict): <see cref="CanStillSatisfy"/> must only return
/// false when no sequence of further operations of the history can lead from <paramref name="state"/> to a state in which
/// <paramref name="pending"/> steps successfully.
/// </remarks>
public interface IPrunableSequentialModel<TState> : ISequentialModel<TState>
{
    /// <summary>False when <paramref name="pending"/> (an <see cref="OpOutcome.Ok"/> operation not yet placed) can never step successfully after <paramref name="state"/>.</summary>
    bool CanStillSatisfy(TState state, HistoryOp pending);
}
