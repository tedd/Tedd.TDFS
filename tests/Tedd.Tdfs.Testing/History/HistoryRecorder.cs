namespace Tedd.Tdfs.Testing;

/// <summary>
/// Thread-safe recorder of invocation/completion histories. Every event (invoke, complete, resolve) receives a
/// distinct logical timestamp from one monotonic counter taken under a lock, so if an operation completes before
/// another is invoked (in real time) its completion timestamp is strictly smaller than the other's invocation timestamp.
/// </summary>
/// <remarks>
/// Unknown outcomes are never dropped or treated as failures (HAR-009, test plan §3.4): in-flight operations are
/// recorded as <see cref="OpOutcome.Unknown"/> until completed, and an unknown outcome can later be narrowed with
/// <see cref="Resolve"/>.
/// </remarks>
public sealed partial class HistoryRecorder
{
    private readonly object _lock = new();
    private readonly List<HistoryOp> _ops = [];
    private long _clock;

    /// <summary>
    /// When true, an <see cref="OperationCanceledException"/> that surfaces after the action started asynchronously is
    /// trusted as the SDK's definite "not executed" claim and recorded as <see cref="OpOutcome.Failed"/> (use this to hold
    /// the SDK to its cancellation contract, for example MUT-006). Default false: such cancellations are recorded as
    /// <see cref="OpOutcome.Unknown"/> because the request may already have been sent.
    /// </summary>
    public bool TrustCancellationAsAbort { get; set; }

    /// <summary>A snapshot of all operations in invocation order.</summary>
    public IReadOnlyList<HistoryOp> Operations
    {
        get
        {
            lock (_lock)
                return _ops.ToArray();
        }
    }

    /// <summary>Number of recorded operations.</summary>
    public int Count
    {
        get
        {
            lock (_lock)
                return _ops.Count;
        }
    }

    /// <summary>Records an invocation and returns its id (outcome <see cref="OpOutcome.Unknown"/> until completed).</summary>
    public long Invoke(string process, string kind, string key, object? input, DfsOperationId? opId = null)
    {
        ArgumentNullException.ThrowIfNull(process);
        ArgumentNullException.ThrowIfNull(kind);
        ArgumentNullException.ThrowIfNull(key);
        lock (_lock)
        {
            long id = _ops.Count + 1;
            long time = ++_clock;
            _ops.Add(new HistoryOp(id, process, kind, key, input, time, null, OpOutcome.Unknown, null, opId));
            return id;
        }
    }

    /// <summary>
    /// Records the client-observed completion of an operation. If the operation was already resolved while still in
    /// flight, a consistent completion (Unknown or the resolved outcome) is ignored.
    /// </summary>
    /// <exception cref="ArgumentException">Unknown id or undefined outcome.</exception>
    /// <exception cref="InvalidOperationException">
    /// The operation already completed, or the client outcome contradicts an earlier definite resolution (a violation of
    /// outcome integrity, INV-06).
    /// </exception>
    public void Complete(long id, OpOutcome outcome, object? output = null)
    {
        if (!Enum.IsDefined(outcome))
            throw new ArgumentException($"Undefined outcome {outcome}.", nameof(outcome));
        lock (_lock)
        {
            int index = IndexOf(id);
            HistoryOp op = _ops[index];
            if (op.CompleteTime is not null)
            {
                bool resolvedInFlight = op.ResolveTime is not null && op.ResolveTime == op.CompleteTime;
                if (resolvedInFlight && (outcome == OpOutcome.Unknown || outcome == op.Outcome))
                    return;
                if (resolvedInFlight)
                    throw new InvalidOperationException($"Operation {id} was resolved as {op.Outcome} but the client later observed {outcome} (contradictory outcomes).");
                throw new InvalidOperationException($"Operation {id} already completed with {op.Outcome}.");
            }

            _ops[index] = op with { CompleteTime = ++_clock, Outcome = outcome, Output = output };
        }
    }

    /// <summary>
    /// Narrows an <see cref="OpOutcome.Unknown"/> outcome (for example after querying the operation id). For
    /// <see cref="OpOutcome.Ok"/> the effect is known to lie between invocation and this call; an in-flight operation is
    /// completed at the same time.
    /// </summary>
    /// <exception cref="ArgumentException">Unknown id, or <paramref name="outcome"/> is not Ok/Failed.</exception>
    /// <exception cref="InvalidOperationException">The operation's outcome is not unknown.</exception>
    public void Resolve(long id, OpOutcome outcome, object? output = null)
    {
        if (outcome is not (OpOutcome.Ok or OpOutcome.Failed))
            throw new ArgumentException("Resolve narrows to Ok or Failed.", nameof(outcome));
        lock (_lock)
        {
            int index = IndexOf(id);
            HistoryOp op = _ops[index];
            if (op.Outcome != OpOutcome.Unknown)
                throw new InvalidOperationException($"Operation {id} is not unknown (it is {op.Outcome}).");
            long time = ++_clock;
            _ops[index] = op with { Outcome = outcome, Output = output, CompleteTime = op.CompleteTime ?? time, ResolveTime = time };
        }
    }

    /// <summary>Attaches the server operation id once it is known (retry identity).</summary>
    public void AttachOperationId(long id, DfsOperationId operationId)
    {
        lock (_lock)
        {
            int index = IndexOf(id);
            _ops[index] = _ops[index] with { OperationId = operationId };
        }
    }

    /// <summary>The current record of an operation.</summary>
    public HistoryOp Get(long id)
    {
        lock (_lock)
            return _ops[IndexOf(id)];
    }

    /// <summary>
    /// Invokes <paramref name="action"/> and records it: success → <see cref="OpOutcome.Ok"/> with the result as output;
    /// <see cref="DfsCommitOutcomeUnknownException"/>, <see cref="TimeoutException"/>, outcome-unknown/timeout
    /// <see cref="DfsException"/>s and cancellation after the action started asynchronously →
    /// <see cref="OpOutcome.Unknown"/>; any other exception → <see cref="OpOutcome.Failed"/>. The exception is the output
    /// of failed/unknown operations and is rethrown.
    /// </summary>
    public async Task<T> RecordAsync<T>(string process, string kind, string key, object? input, Func<Task<T>> action)
    {
        ArgumentNullException.ThrowIfNull(action);
        long id = Invoke(process, kind, key, input);
        Task<T> task;
        try
        {
            task = action() ?? throw new InvalidOperationException("The recorded action returned a null task.");
        }
        catch (Exception ex)
        {
            Complete(id, Classify(ex, requestMayHaveBeenSent: false, TrustCancellationAsAbort), ex);
            throw;
        }

        bool startedAsynchronously = !task.IsCompleted;
        T result;
        try
        {
            result = await task; // keep the caller's context: scenarios may run inside the deterministic simulation
        }
        catch (Exception ex)
        {
            Complete(id, Classify(ex, startedAsynchronously, TrustCancellationAsAbort), ex);
            throw;
        }

        Complete(id, OpOutcome.Ok, result);
        return result;
    }

    /// <summary>
    /// Classifies an exception observed by a client: <see cref="OpOutcome.Unknown"/> for outcome-unknown errors, timeouts
    /// and (unless <paramref name="trustCancellation"/>) cancellations that may follow a sent request; otherwise
    /// <see cref="OpOutcome.Failed"/>.
    /// </summary>
    public static OpOutcome Classify(Exception exception, bool requestMayHaveBeenSent = true, bool trustCancellation = false)
    {
        ArgumentNullException.ThrowIfNull(exception);
        if (ChainContains(exception, static e => e is DfsCommitOutcomeUnknownException
                || e is DfsException { ErrorCode: DfsErrorCode.OutcomeUnknown }))
            return OpOutcome.Unknown;
        if (exception is TimeoutException || exception is DfsException { ErrorCode: DfsErrorCode.Timeout })
            return OpOutcome.Unknown;
        if (exception is OperationCanceledException)
            return requestMayHaveBeenSent && !trustCancellation ? OpOutcome.Unknown : OpOutcome.Failed;
        return OpOutcome.Failed;
    }

    private static bool ChainContains(Exception exception, Func<Exception, bool> predicate)
    {
        var pending = new Stack<Exception>();
        pending.Push(exception);
        int guard = 0;
        while (pending.Count > 0 && guard++ < 256)
        {
            Exception current = pending.Pop();
            if (predicate(current))
                return true;
            if (current is AggregateException aggregate)
            {
                foreach (Exception inner in aggregate.InnerExceptions)
                    pending.Push(inner);
            }
            else if (current.InnerException is not null)
            {
                pending.Push(current.InnerException);
            }
        }

        return false;
    }

    private int IndexOf(long id)
    {
        if (id < 1 || id > _ops.Count)
            throw new ArgumentException($"Unknown operation id {id}.", nameof(id));
        return (int)(id - 1);
    }
}
