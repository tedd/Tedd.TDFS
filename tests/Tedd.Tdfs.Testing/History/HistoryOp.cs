namespace Tedd.Tdfs.Testing;

/// <summary>One operation of a recorded history.</summary>
/// <param name="Id">Unique id within the history.</param>
/// <param name="Process">Logical client/process that issued it (operations of one process do not overlap).</param>
/// <param name="Kind">Operation kind understood by the model ("read", "write", "append", ...).</param>
/// <param name="Key">Object the operation addresses; operations on different keys are checked independently.</param>
/// <param name="Input">Operation input (for example the bytes written).</param>
/// <param name="InvokeTime">Logical time of invocation.</param>
/// <param name="CompleteTime">Logical time the client observed completion; null while in flight.</param>
/// <param name="Outcome">What the client knows about the effect (in-flight operations are <see cref="OpOutcome.Unknown"/>).</param>
/// <param name="Output">Operation output (for example the bytes read), or the exception for failed/unknown operations.</param>
/// <param name="OperationId">Server operation id, when known (retry identity).</param>
public sealed record HistoryOp(long Id, string Process, string Kind, string Key, object? Input, long InvokeTime, long? CompleteTime,
    OpOutcome Outcome, object? Output, DfsOperationId? OperationId)
{
    /// <summary>
    /// Logical time at which an <see cref="OpOutcome.Unknown"/> outcome was narrowed by <see cref="HistoryRecorder.Resolve"/>;
    /// null when the outcome was never ambiguous. A resolved operation's effect lies somewhere in
    /// [<see cref="InvokeTime"/>, <see cref="ResolveTime"/>], not necessarily before <see cref="CompleteTime"/>.
    /// </summary>
    public long? ResolveTime { get; init; }

    /// <summary>True when the outcome was ambiguous to the client and later narrowed.</summary>
    public bool WasResolved => ResolveTime is not null;

    /// <summary>
    /// End of the window in which the operation's effect must lie, as used by the checkers: <see cref="ResolveTime"/> for
    /// resolved operations, otherwise <see cref="CompleteTime"/> for <see cref="OpOutcome.Ok"/>; null (unbounded) otherwise.
    /// </summary>
    public long? EffectDeadline => Outcome == OpOutcome.Ok ? ResolveTime ?? CompleteTime : null;
}
