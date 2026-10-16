using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Runtime;
using Tedd.Tdfs.Server.Volumes.Apply;
using Tedd.Tdfs.Server.Volumes.Commands;
using Tedd.Tdfs.Server.Volumes.State;

namespace Tedd.Tdfs.Server.Volumes.Service;

internal sealed partial class VolumeService
{
    /// <summary><c>OpenSession</c> (PROTOCOL.md §9.3): committed <c>OpenSession</c> entry; retries with the same nonce return the same session.</summary>
    public async Task<IWireMessage> OpenSessionAsync(RpcContext context, OpenSessionMessage request)
    {
        CancellationToken ct = context.Cancellation;
        RejectTransaction(request);
        await EnsureLeaderReadyAsync(ct);
        string principal = context.Principal ?? string.Empty;
        if (GrantsOf(principal) == DfsPermissions.None)
        {
            // Every volume operation of such a principal ends here (it needs a session first): the denial is audited (SEC-018).
            _s.Control.ReportSecurityEvent("volume:open session", "denied", "volume=" + Name + " requires=any grant", principal);
            throw new UnauthorizedAccessException($"Principal '{principal}' has no grant on volume '{Name}'.");
        }
        OperationHeader op = Header(context, request, DfsOperationKind.OpenSession, default);
        NodeTimings t = _s.Timings;
        var command = Stamp(new OpenSessionCommand(request.ClientName, request.Nonce, t.LeaseDuration, t.SessionTimeout, t.MaxOutcomesPerSession,
            t.OutcomeRetention), op);
        VolumeApplyResult result = await ProposeAsync(command, ct);
        if (result.IsSuccess && result.Value is SessionRecord session)
            RenewSession(session.Id);
        return Finish(result, null);
    }

    /// <summary><c>KeepAlive</c>: committed entry renewing the session, its listed handles and reader pins; pin advances recorded.</summary>
    public async Task<IWireMessage> KeepAliveAsync(RpcContext context, KeepAliveMessage request)
    {
        CancellationToken ct = context.Cancellation;
        RejectTransaction(request);
        await EnsureLeaderReadyAsync(ct);
        OperationHeader op = Header(context, request, DfsOperationKind.KeepAlive, default);

        // Handles and pins of transactions' private views are not in the replicated state: they live as long as their
        // transaction's view on this leader (the transaction itself expires at its deadline).
        HashSet<ulong> privateHandles = [.. _transactions.Values.SelectMany(t => t.Handles.Keys)];
        HashSet<ulong> privatePins = [.. _transactions.Values.SelectMany(t => t.Pins.Keys)];
        var command = Stamp(new KeepAliveCommand(request.Header.Session,
            [.. request.Handles.Select(h => h.Value).Where(h => h < VolumeStateMachine.TransactionHandleBase)],
            [.. request.PinUpdates.Where(p => p.Handle.Value < VolumeStateMachine.TransactionHandleBase).Select(p => new PinAdvance(p.Handle.Value, p.PinIndex))],
            [.. request.ReaderPins.Where(p => p < VolumeStateMachine.TransactionPinBase)]), op);
        HandleId[] lostPrivateHandles = [.. request.Handles.Where(h => h.Value >= VolumeStateMachine.TransactionHandleBase && !privateHandles.Contains(h.Value))];
        ulong[] lostPrivatePins = [.. request.ReaderPins.Where(p => p >= VolumeStateMachine.TransactionPinBase && !privatePins.Contains(p))];

        // Renew the leases before the entry commits: a renewal that arrives in time must not lose the race with an expiry
        // proposed meanwhile by this leader (the reply reports what the committed state says).
        if (State.Sessions.TryGetValue(request.Header.Session, out SessionRecord? known) && known.State == SessionState.Active
            && string.Equals(known.Principal, op.Principal, StringComparison.Ordinal))
        {
            RenewSession(known.Id);
            foreach (HandleId handle in request.Handles)
            {
                if (State.Handles.TryGetValue(handle.Value, out HandleRecord? h) && h.Session == known.Id)
                    RenewHandle(handle.Value);
            }

            foreach (ulong pin in request.ReaderPins)
            {
                if (State.Pins.TryGetValue(pin, out PinRecord? p) && p.Session == known.Id)
                    RenewPin(pin);
            }
        }

        VolumeApplyResult result = await ProposeAsync(command, ct);
        if (result.IsSuccess && result.Value is KeepAliveValue renewed && renewed.Session != 0)
        {
            RenewSession(renewed.Session);
            foreach (ulong handle in renewed.RenewedHandles)
                RenewHandle(handle);
            foreach (ulong pin in renewed.RenewedPins)
                RenewPin(pin);
        }

        return Finish(result, reply => reply.Reply is KeepAliveResultMessage k
            ? k with
            {
                LeaseDuration = _s.Timings.LeaseDuration,
                ServerWallClock = _s.Time.GetUtcNow(),
                ExpiredHandles = [.. k.ExpiredHandles, .. lostPrivateHandles],
                ExpiredPins = [.. k.ExpiredPins, .. lostPrivatePins],
            }
            : reply.Reply!);
    }

    /// <summary>
    /// <c>QueryOperation</c> (PROTOCOL.md §5.1): retained outcome, Pending while in flight on this leader, NotFound or Expired;
    /// with <c>Resolve</c> an id without outcome is fenced (committed <c>FenceOperation</c>) so the answer is definite.
    /// </summary>
    public async Task<IWireMessage> QueryOperationAsync(RpcContext context, QueryOperationMessage request)
    {
        CancellationToken ct = context.Cancellation;
        RejectTransaction(request);
        string principal = context.Principal ?? string.Empty;
        if (GrantsOf(principal) == DfsPermissions.None)
            throw new UnauthorizedAccessException($"Principal '{principal}' has no grant on volume '{Name}'.");
        DfsOperationId target = request.Target;
        if (!request.Resolve && !request.IsTransaction && _inFlight.TryGetValue(target, out InFlight? inFlight))
        {
            // An operation in flight on this node is answered from local knowledge, without a read barrier (which a stalled
            // log could never pass): applied here means decided (committed outcomes and recorded refusals are final),
            // otherwise it is Pending — never NotFound (OUT-014).
            if (!string.Equals(inFlight.Principal, principal, StringComparison.Ordinal))
                throw new DfsOperationIdMismatchException($"Operation {target} belongs to another principal.");
            QueryOperationResultMessage local = VolumeStateMachine.QueryOutcome(State, target, principal, out _, out _);
            if (local.Status is WireOutcomeStatus.Committed or WireOutcomeStatus.Aborted)
                return local;
            return new QueryOperationResultMessage(target, WireOutcomeStatus.Pending, CommitToken.None, DfsErrorCode.None, "The operation is in flight.",
                inFlight.Operation, ReadOnlyMemory<byte>.Empty);
        }

        await ReadBarrierAsync(ct);
        if (_inFlight.TryGetValue(target, out InFlight? running))
        {
            if (!string.Equals(running.Principal, principal, StringComparison.Ordinal))
                throw new DfsOperationIdMismatchException($"Operation {target} belongs to another principal.");
            if (!request.Resolve)
                return new QueryOperationResultMessage(target, WireOutcomeStatus.Pending, CommitToken.None, DfsErrorCode.None, "The operation is in flight.",
                    running.Operation, ReadOnlyMemory<byte>.Empty);
        }

        QueryOperationResultMessage answer;
        try
        {
            bool fenceable;
            answer = request.IsTransaction
                ? VolumeStateMachine.QueryTransaction(State, target, principal, out fenceable)
                : VolumeStateMachine.QueryOutcome(State, target, principal, out _, out fenceable);
            if (!request.Resolve || !fenceable)
                return answer;
            if (request.IsTransaction)
                DropTransaction(target);
        }
        catch (ApplyFailure failure)
        {
            return new ErrorReplyMessage(failure.Code, failure.Message, target);
        }

        OperationHeader op = Header(context, request, DfsOperationKind.QueryOperation, default);
        VolumeApplyResult result = await ProposeAsync(Stamp(new FenceOperationCommand(target, request.IsTransaction), op), ct);
        return Finish(result, null);
    }

    /// <summary><c>ReserveOperationIds</c>: reserves a block of sequences in the caller's session (committed).</summary>
    public async Task<IWireMessage> ReserveOperationIdsAsync(RpcContext context, ReserveOperationIdsMessage request)
    {
        CancellationToken ct = context.Cancellation;
        RejectTransaction(request);
        await EnsureLeaderReadyAsync(ct);
        if (request.Count is < 1 or > 65536)
            throw new ArgumentException("Between 1 and 65536 sequences can be reserved at once.");
        OperationHeader op = Header(context, request, DfsOperationKind.OpenSession, default);
        VolumeApplyResult result = await ProposeAsync(Stamp(new ReserveOperationIdsCommand(request.Header.Session, request.Count), op), ct);
        return Finish(result, null);
    }
}
