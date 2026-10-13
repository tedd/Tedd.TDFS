using Tedd.Tdfs.Client.Internal.Runtime;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Client.Internal.Rpc;

/// <summary>
/// Executes requests with leader routing (redirects follow <c>NotLeader</c> hints), retries of the same operation id with
/// bounded jittered backoff from the injected randomness, per-attempt and per-operation deadlines, and the classification
/// of failures (ARCHITECTURE §7, §9.2, FSD §13):
/// <list type="bullet">
/// <item>nothing transmitted → <see cref="OperationCanceledException"/> (caller), <see cref="ObjectDisposedException"/>
/// (client shutdown) or the last availability failure (deadline);</item>
/// <item>a possibly transmitted mutation without a reply → resolved through <c>QueryOperation(resolve)</c> when time allows,
/// otherwise <see cref="DfsCommitOutcomeUnknownException"/> carrying the operation (and transaction) id — never a definite
/// abort (unless the <see cref="SafetyMutations.TreatUnknownAsAborted"/> negative control is enabled).</item>
/// </list>
/// A transmission answered with <c>NotLeader</c> is settled (the node refuses before proposing, nothing was executed): a
/// mutation whose every transmission was refused that way failed definitively.
/// </summary>
internal sealed class RpcInvoker
{
    private const int MaxRedirects = 16;
    private readonly ClientRuntime _runtime;

    public RpcInvoker(ClientRuntime runtime)
    {
        ArgumentNullException.ThrowIfNull(runtime);
        _runtime = runtime;
    }

    /// <summary>Default per-attempt reply timeout: a third of the operation timeout (at least 100 ms), never past the deadline.</summary>
    public TimeSpan DefaultAttemptTimeout(OperationScope scope)
    {
        TimeSpan third = TimeSpan.FromTicks(Math.Max(_runtime.Options.OperationTimeout.Ticks / 3, TimeSpan.FromMilliseconds(100).Ticks));
        TimeSpan remaining = scope.Remaining;
        return ClientRuntime.Positive(remaining < third ? remaining : third);
    }

    /// <summary>Executes <paramref name="request"/> and returns its success reply.</summary>
    public async Task<IWireMessage> InvokeAsync(RpcRequest request, OperationScope scope)
    {
        bool mutation = request.Kind == CallKind.Mutation;
        bool transmitted = false;   // a mutation transmission may have been executed (it is not settled by a definite refusal)
        int unsettled = 0;
        Exception? leaderless = null; // a node answered that it knows no leader
        int transmissions = 0;
        int failures = 0;
        int redirects = 0;
        Exception? last = null;
        if (mutation)
            scope.OperationId = request.Operation;
        TimeSpan budget = mutation ? scope.MutationRetryBudget : scope.Timeout;

        while (!scope.Token.IsCancellationRequested && scope.Elapsed < budget && failures <= _runtime.Options.MaxRetryAttempts)
        {
            TimeSpan attemptTimeout = request.AttemptTimeout is { } fixedTimeout
                ? ClientRuntime.Positive(fixedTimeout < scope.Remaining ? fixedTimeout : scope.Remaining)
                : DefaultAttemptTimeout(scope);
            if (mutation)
            {
                // Attempts of a mutation end within its retry budget, so the outcome can still be resolved in time.
                TimeSpan left = budget - scope.Elapsed;
                attemptTimeout = ClientRuntime.Positive(left < attemptTimeout ? left : attemptTimeout);
            }

            // 1. Route.
            string? endpoint;
            try
            {
                endpoint = await request.Router.ResolveAsync(scope, attemptTimeout).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (scope.Token.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex) when (IsTransient(ex))
            {
                last = ex;
                failures++;
                if (!await BackoffAsync(failures, scope).ConfigureAwait(false))
                    break;
                continue;
            }

            if (endpoint is null)
            {
                // The nodes asked for the route know no leader (or none could be asked).
                leaderless = new DfsUnavailableException(DfsErrorCode.NotLeader, $"{request.Name}: no leader is currently known.");
                last ??= leaderless;
                failures++;
                if (!await BackoffAsync(failures, scope).ConfigureAwait(false))
                    break;
                continue;
            }

            // 2. Connection.
            ClientConnection connection;
            try
            {
                connection = await _runtime.Pool.GetAsync(endpoint, scope, attemptTimeout).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (scope.Token.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex) when (IsTransient(ex) || ex is DfsClusterIdentityException)
            {
                last = ex;
                request.Router.OnUnreachable(endpoint);
                failures++;
                if (!await BackoffAsync(failures, scope).ConfigureAwait(false))
                    break;
                continue;
            }
            catch (Exception ex) when (transmitted)
            {
                // A failure before this attempt's send cannot hide the earlier, possibly applied transmission.
                last = ex;
                break;
            }

            // 3. Build (may open the volume session first) and send.
            IWireMessage message;
            try
            {
                message = await request.Build(scope).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (scope.Token.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex) when (IsTransient(ex))
            {
                last = ex;
                failures++;
                if (!await BackoffAsync(failures, scope).ConfigureAwait(false))
                    break;
                continue;
            }
            catch (Exception ex) when (transmitted)
            {
                last = ex;
                break;
            }

            DfsOperationKind wireKind = WireKindOf(message, request);
            try
            {
                await scope.HitAsync(FaultStage.F03, FaultSites.ClientRequestSend, wireKind, request.Operation, occurrence: transmissions).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (scope.Token.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex) when (transmitted)
            {
                last = ex;
                break;
            }

            if (scope.Token.IsCancellationRequested)
                break;

            byte[] frame = MessageCodec.Encode(message);
            long attemptStarted = _runtime.Time.GetTimestamp();
            Task<ReadOnlyMemory<byte>> pending = connection.StartCall(frame, attemptTimeout, scope.Token);

            // A call refused before sending (connection closed) transmitted nothing. A call that completed as cancelled may
            // already have handed its frame to the transport (the caller's token can fire while the call starts), so it
            // counts as transmitted: its id is never reused for another request (the token was checked just above).
            bool refused = pending.IsFaulted && pending.Exception?.GetBaseException() is RpcConnectionException;
            if (!refused)
            {
                transmissions++;
                request.Transmissions = transmissions;
                if (mutation)
                {
                    unsettled++;
                    transmitted = true;
                }
            }

            ReadOnlyMemory<byte> raw;
            try
            {
                raw = await pending.ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (scope.Token.IsCancellationRequested)
            {
                // The operation ended (deadline, caller or shutdown) while this attempt was outstanding. When the attempt had
                // also run for its whole timeout, the connection did not answer in time: it is treated exactly like an attempt
                // timeout (a silently dead link must not stay pooled), whichever of the two timers happened to fire first.
                if (_runtime.Time.GetElapsedTime(attemptStarted) >= attemptTimeout)
                {
                    _runtime.Pool.Invalidate(connection);
                    request.Router.OnUnreachable(endpoint);
                    _runtime.Emit("rpc.attempt-failed", $"{request.Name} to {endpoint}: no reply within the attempt timeout ({attemptTimeout}).",
                        request.Operation, request.Transaction, scope.Volume);
                }

                break;
            }
            catch (Exception ex) when (ex is TimeoutException or RpcConnectionException or IOException or ObjectDisposedException)
            {
                last = ex;
                _runtime.Pool.Invalidate(connection);
                request.Router.OnUnreachable(endpoint);
                _runtime.Emit("rpc.attempt-failed", $"{request.Name} to {endpoint}: {ex.GetType().Name}.", request.Operation, request.Transaction, scope.Volume,
                    exception: ex);
                failures++;
                if (!await BackoffAsync(failures, scope).ConfigureAwait(false))
                    break;
                continue;
            }

            IWireMessage reply;
            try
            {
                reply = MessageCodec.Decode(raw.Span);
            }
            catch (DfsProtocolException ex)
            {
                _runtime.Pool.Invalidate(connection);
                last = ex;
                failures++;
                if (!await BackoffAsync(failures, scope).ConfigureAwait(false))
                    break;
                continue;
            }

            try
            {
                await scope.HitAsync(FaultStage.F14, FaultSites.ClientReplyReceived, wireKind, request.Operation, occurrence: transmissions - 1)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (scope.Token.IsCancellationRequested)
            {
                // The reply arrived: a known success is never reported as cancelled.
                if (reply.Type == request.ReplyType)
                    return Succeeded(request, endpoint, reply);
                break;
            }
#pragma warning disable CA1031 // An injected receive-path failure is treated like a transport failure after transmission.
            catch (Exception ex)
#pragma warning restore CA1031
            {
                last = ex;
                failures++;
                if (!await BackoffAsync(failures, scope).ConfigureAwait(false))
                    break;
                continue;
            }

            if (reply.Type == request.ReplyType)
                return Succeeded(request, endpoint, reply);

            if (reply is not ErrorReplyMessage error)
            {
                _runtime.Pool.Invalidate(connection);
                var violation = new DfsProtocolException($"{request.Name}: expected {request.ReplyType} from {endpoint}, received {reply.Type}.");

                // The node answered something unexpected after receiving the mutation: it may have applied it.
                if (mutation)
                    throw Unknown(request, request.Operation.GetValueOrDefault(), violation);
                throw violation;
            }

            request.Router.OnAnswered(endpoint);
            DfsOperationId? before = request.Operation;
            if (request.OnError is not null && await request.OnError(error, scope, transmissions).ConfigureAwait(false))
            {
                last = error.ToException(request.Path);
                failures++;
                if (request.Operation != before)
                {
                    // The hook replaced an id that was definitively rejected without effect: the new id starts afresh.
                    transmitted = false;
                    unsettled = 0;
                    transmissions = 0;
                    request.Transmissions = 0;
                    scope.OperationId = request.Operation;
                }

                continue;
            }

            switch (error.Code)
            {
                case DfsErrorCode.NotLeader:
                    last = error.ToException(request.Path);
                    if (mutation && --unsettled == 0)
                        transmitted = false; // every transmission so far was refused before it was proposed
                    leaderless = error.LeaderHint is { Length: > 0 } ? null : last; // a hint means a leader exists
                    request.Router.OnNotLeader(endpoint, error.LeaderHint);
                    if (error.LeaderHint is { Length: > 0 } hint && !string.Equals(hint, endpoint, StringComparison.Ordinal) && redirects < MaxRedirects)
                    {
                        redirects++;
                        continue;
                    }

                    failures++;
                    if (!await BackoffAsync(failures, scope).ConfigureAwait(false))
                        goto Exhausted;
                    continue;

                case DfsErrorCode.Unavailable:
                case DfsErrorCode.NodeUnavailable:
                case DfsErrorCode.QuorumUnavailable:
                case DfsErrorCode.Timeout:
                    last = error.ToException(request.Path);
                    failures++;
                    if (!await BackoffAsync(failures, scope).ConfigureAwait(false))
                        goto Exhausted;
                    continue;

                case DfsErrorCode.OutcomeUnknown when mutation:
                    last = error.ToException(request.Path);
                    failures++;
                    if (!await BackoffAsync(failures, scope).ConfigureAwait(false))
                        goto Exhausted;
                    continue;

                default:
                    request.OnDefinite?.Invoke();
                    throw error.ToException(request.Path);
            }
        }

    Exhausted:
        // When a node reported that no leader is known, that (not a later connection failure to a dead node) explains why
        // the request could not be served: the group has no quorum (FSD §13, typed availability failures).
        if (leaderless is not null && !Availability.IsLeaderless(last))
            last = leaderless;
        return await FinishWithoutReplyAsync(request, scope, mutation && transmitted, last).ConfigureAwait(false);
    }

    /// <summary>
    /// Sends <paramref name="message"/> once to <paramref name="endpoint"/> (data plane, discovery probes): the reply or an
    /// error reply is returned; transport failures and cancellation propagate. Hits F03/F14 with
    /// <paramref name="faultKind"/> (default: the message's operation kind).
    /// </summary>
    public async Task<IWireMessage> SendOnceAsync(string endpoint, IWireMessage message, OperationScope scope, TimeSpan timeout,
        DfsOperationKind? faultKind = null, ObjectId? obj = null, int occurrence = 0, CancellationToken token = default)
    {
        CancellationToken ct = token.CanBeCanceled ? token : scope.Token;
        ClientConnection connection = await _runtime.Pool.GetAsync(endpoint, scope, timeout).ConfigureAwait(false);
        DfsOperationKind kind = faultKind ?? MessageTypes.OperationOf(message.Type);
        await scope.HitAsync(FaultStage.F03, FaultSites.ClientRequestSend, kind, null, occurrence, obj).ConfigureAwait(false);
        ct.ThrowIfCancellationRequested();
        ReadOnlyMemory<byte> raw;
        try
        {
            raw = await connection.StartCall(MessageCodec.Encode(message), ClientRuntime.Positive(timeout), ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is TimeoutException or RpcConnectionException or IOException)
        {
            _runtime.Pool.Invalidate(connection);
            throw;
        }

        IWireMessage reply = MessageCodec.Decode(raw.Span);
        await scope.HitAsync(FaultStage.F14, FaultSites.ClientReplyReceived, kind, null, occurrence, obj).ConfigureAwait(false);
        _runtime.Map.MarkGood(endpoint);
        return reply;
    }

    private IWireMessage Succeeded(RpcRequest request, string endpoint, IWireMessage reply)
    {
        request.Router.OnAnswered(endpoint);
        request.OnDefinite?.Invoke();
        return reply;
    }

    private async Task<IWireMessage> FinishWithoutReplyAsync(RpcRequest request, OperationScope scope, bool possiblyTransmittedMutation, Exception? last)
    {
        if (!possiblyTransmittedMutation)
        {
            request.OnDefinite?.Invoke();
            throw scope.NotTransmittedFailure(last);
        }

        DfsOperationId operation = request.Operation.GetValueOrDefault();
        if (_runtime.Mutations.TreatUnknownAsAborted)
        {
            // Negative control (MUT-006): the uncertain outcome is misreported as a definite abort.
            request.OnDefinite?.Invoke();
            throw new OperationCanceledException($"{request.Name} ({operation}) did not complete; reported as aborted (TreatUnknownAsAborted).", last);
        }

        if (scope.IsAbortedByCaller)
        {
            Exception cause = scope.NotTransmittedFailure(last);
            throw Unknown(request, operation, cause);
        }

        if (request.Resolve is not null && !scope.Token.IsCancellationRequested && scope.Remaining > TimeSpan.Zero)
        {
            try
            {
                IWireMessage? resolved = await request.Resolve(scope).ConfigureAwait(false);
                if (resolved is not null)
                {
                    request.OnDefinite?.Invoke();
                    return resolved;
                }
            }
            catch (ResolvedFailureException definite)
            {
                request.OnDefinite?.Invoke();
                throw definite.Failure;
            }
            catch (Exception ex) when (ex is not ResolvedFailureException)
            {
                _runtime.Emit("rpc.resolve-failed", $"{request.Name}: resolving the outcome of {operation} failed ({ex.GetType().Name}).", operation,
                    request.Transaction, scope.Volume, exception: ex);
                if (scope.IsAbortedByCaller)
                    throw Unknown(request, operation, scope.NotTransmittedFailure(last));
            }
        }

        throw Unknown(request, operation, last ?? new TimeoutException($"{request.Name} did not complete within its {scope.Timeout} deadline."));
    }

    /// <summary>The unknown-outcome failure of <paramref name="request"/> (its operation and transaction id, the cause as inner exception).</summary>
    public DfsCommitOutcomeUnknownException CreateUnknown(RpcRequest request, Exception cause) =>
        Unknown(request, request.Operation.GetValueOrDefault(), cause);

    private DfsCommitOutcomeUnknownException Unknown(RpcRequest request, DfsOperationId operation, Exception cause)
    {
        _runtime.Emit("rpc.outcome-unknown", $"{request.Name}: the outcome of {operation} is unknown.", operation, request.Transaction, null, exception: cause);
        return new DfsCommitOutcomeUnknownException(operation, request.Transaction,
            $"The outcome of {request.Name} (operation {operation}) is unknown: the request may have taken effect. Resolve it with QueryOperationAsync.",
            cause);
    }

    private async Task<bool> BackoffAsync(int failures, OperationScope scope)
    {
        if (failures > _runtime.Options.MaxRetryAttempts)
            return false;
        TimeSpan delay = _runtime.Backoff(failures);
        TimeSpan limit = scope.OperationId is null ? scope.Remaining : scope.MutationRetryBudget - scope.Elapsed;
        if (limit <= TimeSpan.Zero)
            return false;
        if (delay >= limit)
            delay = limit;
        try
        {
            await Waits.DelayAsync(_runtime.Time, delay, scope.Token).ConfigureAwait(false);
            return !scope.Token.IsCancellationRequested;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }

    /// <summary>
    /// The operation kind of a request at the client wire sites: transaction-bound requests (other than the transaction's
    /// own begin/commit/abort and outcome queries) are <see cref="DfsOperationKind.TxOp"/> operations.
    /// </summary>
    private static DfsOperationKind WireKindOf(IWireMessage message, RpcRequest request) =>
        request.Transaction is not null
        && message.Type is not (MessageType.TxBegin or MessageType.TxCommit or MessageType.TxAbort or MessageType.QueryOperation)
            ? DfsOperationKind.TxOp
            : MessageTypes.OperationOf(message.Type);

    /// <summary>Failures worth another attempt (possibly elsewhere): transport problems and retryable availability errors.</summary>
    public static bool IsTransient(Exception ex) => ex switch
    {
        TimeoutException or RpcConnectionException => true,
        DfsUnavailableException => true,
        DfsException d => d.IsRetryable && d.ErrorCode is DfsErrorCode.NotLeader or DfsErrorCode.Unavailable or DfsErrorCode.NodeUnavailable
            or DfsErrorCode.QuorumUnavailable or DfsErrorCode.Timeout,
        IOException => ex is not DfsException,
        _ => false,
    };
}

/// <summary>Thrown by <see cref="RpcRequest.Resolve"/> when the resolution proved a definite failure (carried in <see cref="Failure"/>).</summary>
internal sealed class ResolvedFailureException : Exception
{
    public ResolvedFailureException(Exception failure)
        : base(failure.Message, failure)
    {
        Failure = failure;
    }

    public ResolvedFailureException()
        : this(new InvalidOperationException("Unspecified resolved failure."))
    {
    }

    public ResolvedFailureException(string message)
        : this(new InvalidOperationException(message))
    {
    }

    public ResolvedFailureException(string message, Exception inner)
        : base(message, inner)
    {
        Failure = inner;
    }

    /// <summary>The definite failure to report.</summary>
    public Exception Failure { get; }
}
