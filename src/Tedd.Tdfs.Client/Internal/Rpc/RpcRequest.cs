using Tedd.Tdfs.Client.Internal.Runtime;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;

namespace Tedd.Tdfs.Client.Internal.Rpc;

/// <summary>How a request may be retried and how a lost reply is classified.</summary>
internal enum CallKind
{
    /// <summary>Reads (and other requests without effect): retried freely; a lost reply is an availability failure.</summary>
    Read,

    /// <summary>
    /// Mutations carrying an operation id from the first transmission: retried with the same id; once possibly transmitted,
    /// a missing reply is resolved through <c>QueryOperation(resolve)</c> or reported as an unknown outcome.
    /// </summary>
    Mutation,

    /// <summary>
    /// Naturally idempotent session/control requests without an operation id (OpenSession with its nonce, KeepAlive,
    /// ReserveOperationIds, CloseHandle, QueryOperation, AdminRequest with its request id): retried freely; a lost reply
    /// is an availability failure.
    /// </summary>
    Idempotent,
}

/// <summary>Chooses where a request goes and learns from redirects and failures (volume leader, control leader, a fixed node).</summary>
internal interface IRpcRouter
{
    /// <summary>Endpoint for the next attempt, or null when none is currently known (the invoker backs off and asks again).</summary>
    ValueTask<string?> ResolveAsync(OperationScope scope, TimeSpan attemptTimeout);

    /// <summary>The node at <paramref name="endpoint"/> is not the leader; <paramref name="hint"/> names the leader when known.</summary>
    void OnNotLeader(string endpoint, string? hint);

    /// <summary>The node at <paramref name="endpoint"/> could not be reached or did not answer in time.</summary>
    void OnUnreachable(string endpoint);

    /// <summary>The node at <paramref name="endpoint"/> answered.</summary>
    void OnAnswered(string endpoint);

    /// <summary>
    /// True when requests are served by the leader of a consensus group (control or volume): exhausting every attempt
    /// without reaching a leader that answers then means that no leader with a quorum could be reached.
    /// </summary>
    bool RoutesToGroupLeader { get; }

    /// <summary>
    /// What this router knows about the group having no leader right now (for example a route discovery in which every node
    /// that answered knew no leader), or null: used when a request finds no time left for an attempt of its own, because
    /// the operation's time went into waiting for another request to the same group (a shared session open).
    /// </summary>
    Exception? LeaderlessEvidence => null;
}

/// <summary>One logical request executed by <see cref="RpcInvoker"/> (possibly over several attempts).</summary>
internal sealed class RpcRequest
{
    /// <summary>Short name for diagnostics.</summary>
    public required string Name { get; init; }

    /// <summary>The success reply type.</summary>
    public required MessageType ReplyType { get; init; }

    /// <summary>Builds the request for one attempt (headers carry the current session and acknowledgement watermark).</summary>
    public required Func<OperationScope, ValueTask<IWireMessage>> Build { get; init; }

    /// <summary>Routing.</summary>
    public required IRpcRouter Router { get; init; }

    /// <summary>Retry and classification policy.</summary>
    public CallKind Kind { get; init; } = CallKind.Read;

    /// <summary>The mutation's operation id (fault contexts, unknown-outcome identity). May change through <see cref="ReplaceOperation"/>.</summary>
    public DfsOperationId? Operation { get; set; }

    /// <summary>Transaction the request belongs to (carried by unknown-outcome exceptions).</summary>
    public DfsTransactionId? Transaction { get; init; }

    /// <summary>Path addressed (exceptions such as <see cref="FileNotFoundException"/> carry it).</summary>
    public string? Path { get; init; }

    /// <summary>Per-attempt reply timeout; null derives one from the operation timeout.</summary>
    public TimeSpan? AttemptTimeout { get; init; }

    /// <summary>
    /// Resolves the outcome of a possibly transmitted mutation whose reply never arrived: returns the success reply when it
    /// committed, throws the definite failure when it did not, returns null when the outcome stays unknown.
    /// </summary>
    public Func<OperationScope, Task<IWireMessage?>>? Resolve { get; init; }

    /// <summary>
    /// Called for error replies before the generic handling; returns true when the request should be retried (for example
    /// after re-binding to a re-created volume or replacing a never-used expired operation id). The count passed is the
    /// number of transmissions of the current id, the answered one included; every transmission counts, also one answered
    /// <c>NotLeader</c> (a duplicated delivery of it may have been proposed while that node still led).
    /// </summary>
    public Func<ErrorReplyMessage, OperationScope, int, ValueTask<bool>>? OnError { get; init; }

    /// <summary>Called when the request was answered or failed definitely (its id needs no further protection).</summary>
    public Action? OnDefinite { get; init; }

    /// <summary>Number of attempts that were (possibly) transmitted so far (maintained by <see cref="RpcInvoker"/>).</summary>
    public int Transmissions { get; set; }

    /// <summary>Replaces <see cref="Operation"/> (only for SDK-allocated ids that never took effect).</summary>
    public void ReplaceOperation(DfsOperationId id) => Operation = id;
}
