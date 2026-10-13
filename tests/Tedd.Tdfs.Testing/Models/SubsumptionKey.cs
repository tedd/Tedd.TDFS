namespace Tedd.Tdfs.Testing;

/// <summary>
/// The abstraction of a state for <see cref="ModelChecker.ExploreParallelWithSubsumption{TState}"/>: a state with the same
/// <paramref name="Core"/>, componentwise no more <paramref name="Consumed"/> budget and a superset of the
/// <paramref name="Items"/> simulates this one.
/// </summary>
/// <param name="Core">Canonical key of everything except the consumed budgets and the pending items.</param>
/// <param name="Consumed">Consumed budgets (one entry per budget; the same length for every state of a model).</param>
/// <param name="Items">Canonical keys of the pending items, sorted ascending (a multiset: equal keys repeat).</param>
public readonly record struct SubsumptionKey(UInt128 Core, int[] Consumed, UInt128[] Items);
