using System.Collections.Immutable;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Volumes.Commands;
using Tedd.Tdfs.Server.Volumes.State;

namespace Tedd.Tdfs.Server.Volumes.Apply;

/// <summary>Result value of a keep-alive: what the leader's lease tracker should renew.</summary>
/// <param name="Session">The session (0 when it expired).</param>
/// <param name="RenewedHandles">Listed handles that are active and owned by the session.</param>
/// <param name="RenewedPins">Listed reader pins that are held by the session.</param>
internal sealed record KeepAliveValue(ulong Session, IReadOnlyList<ulong> RenewedHandles, IReadOnlyList<ulong> RenewedPins);

internal sealed partial class VolumeStateMachine
{
    /// <summary>Maximum reserved-sequence blocks a session may hold.</summary>
    public const int MaxReservedRanges = 1024;

    private static (VolumeState, VolumeApplyResult) ApplyOpenSession(VolumeState s, ApplyContext ctx, OpenSessionCommand c)
    {
        OperationHeader op = ctx.Op ?? throw Fail(DfsErrorCode.AccessDenied, "OpenSession needs a principal.");
        if (op.Grants == DfsPermissions.None)
            throw Fail(DfsErrorCode.AccessDenied, $"Principal '{op.Principal}' has no grant on volume '{s.Config.Name}'.");
        if (c.Nonce != UInt128.Zero)
        {
            foreach (SessionRecord existing in s.Sessions.Values)
            {
                if (existing.Nonce == c.Nonce && existing.State == SessionState.Active && string.Equals(existing.Principal, op.Principal, StringComparison.Ordinal))
                    return (s, VolumeApplyResult.Ok(OpenReply(s, existing, ctx), existing));
            }
        }

        var session = new SessionRecord(s.NextSessionId, op.Principal, c.ClientName, c.Nonce, SessionState.Active, ctx.Index, ctx.Now, null, 0, 0,
            ImmutableList<ReservedRange>.Empty, SessionRecord.NoOutcomes, Math.Max(1, c.MaxOutcomes), c.OutcomeRetention, c.LeaseDuration, c.SessionTimeout);
        s = s with { NextSessionId = s.NextSessionId + 1, Sessions = s.Sessions.Add(session.Id, session) };
        return (s, VolumeApplyResult.Ok(OpenReply(s, session, ctx), session));
    }

    private static OpenSessionResultMessage OpenReply(VolumeState s, SessionRecord session, ApplyContext ctx)
        => new(session.Id, session.HighestSequence + 1, session.LeaseDuration, session.SessionTimeout, ctx.Now, session.MaxOutcomes, session.OutcomeRetention,
            s.TokenAt(ctx.Index));

    private static (VolumeState, VolumeApplyResult) ApplyKeepAlive(VolumeState s, ApplyContext ctx, KeepAliveCommand c)
    {
        string principal = ctx.Op?.Principal ?? string.Empty;
        ImmutableArray<ulong> handles = OrEmpty(c.Handles);
        ImmutableArray<ulong> readerPins = OrEmpty(c.ReaderPins);
        if (!s.Sessions.TryGetValue(c.Session, out SessionRecord? session) || session.State != SessionState.Active
            || !string.Equals(session.Principal, principal, StringComparison.Ordinal))
        {
            var expiredReply = new KeepAliveResultMessage(handles.Select(h => new HandleId(h)).ToArray(), readerPins.ToArray(), true, TimeSpan.Zero, ctx.Now);
            return (s, VolumeApplyResult.Ok(expiredReply, new KeepAliveValue(0, [], [])));
        }

        session = session with { LastActivityUtc = ctx.Now };
        s = s with { Sessions = s.Sessions.SetItem(session.Id, session) };

        var expiredHandles = new List<HandleId>();
        var renewedHandles = new List<ulong>();
        foreach (ulong id in handles)
        {
            if (s.Handles.TryGetValue(id, out HandleRecord? h) && h.Session == session.Id)
                renewedHandles.Add(id);
            else
                expiredHandles.Add(new HandleId(id));
        }

        foreach (PinAdvance update in OrEmpty(c.PinUpdates))
        {
            if (!s.Handles.TryGetValue(update.Handle, out HandleRecord? h) || h.Session != session.Id || h.View != HandleView.Live)
                continue;
            if (!s.Pins.TryGetValue(h.PinId, out PinRecord? pin))
                continue;
            ulong advanced = Math.Min(Math.Max(pin.PinIndex, update.PinIndex), ctx.Index);
            if (advanced != pin.PinIndex)
                s = s with { Pins = s.Pins.SetItem(pin.Id, pin with { PinIndex = advanced }) };
        }

        var expiredPins = new List<ulong>();
        var renewedPins = new List<ulong>();
        foreach (ulong id in readerPins)
        {
            if (s.Pins.TryGetValue(id, out PinRecord? pin) && pin.Session == session.Id && pin.Kind != PinKind.Handle)
                renewedPins.Add(id);
            else
                expiredPins.Add(id);
        }

        var reply = new KeepAliveResultMessage(expiredHandles, expiredPins, false, session.LeaseDuration, ctx.Now);
        return (s, VolumeApplyResult.Ok(reply, new KeepAliveValue(session.Id, renewedHandles, renewedPins)));
    }

    private static (VolumeState, VolumeApplyResult) ApplyExpireSessions(VolumeState s, ApplyContext ctx, ExpireSessionsCommand c)
    {
        var expired = new List<ulong>();
        foreach (ulong id in OrEmpty(c.Sessions).Order())
        {
            if (!s.Sessions.TryGetValue(id, out SessionRecord? session) || session.State != SessionState.Active)
                continue;
            s = ReleaseSession(s, id, ctx.Index);
            s = s with { Sessions = s.Sessions.SetItem(id, session with { State = SessionState.Expired, ExpiredUtc = ctx.Now, Reserved = [] }) };
            expired.Add(id);
        }

        s = RetireSessions(s, ctx.Now);
        return (s, VolumeApplyResult.Ok(null, expired));
    }

    /// <summary>
    /// Fully retires expired sessions whose outcome retention elapsed (every id of theirs is expired from now on), and drops
    /// retired tombstones below the contiguous retired-session watermark.
    /// </summary>
    internal static VolumeState RetireSessions(VolumeState s, DateTimeOffset now)
    {
        foreach (SessionRecord session in s.Sessions.Values.ToArray())
        {
            if (session.State == SessionState.Expired && session.ExpiredUtc is { } at && at + session.OutcomeRetention <= now)
            {
                SessionRecord retired = session with
                {
                    State = SessionState.Retired,
                    Outcomes = SessionRecord.NoOutcomes,
                    RetiredWatermark = Math.Max(session.RetiredWatermark, session.HighestSequence),
                };
                s = s with { Sessions = s.Sessions.SetItem(session.Id, retired) };
            }
        }

        ulong below = s.RetiredSessionsBelow;
        ImmutableSortedDictionary<ulong, SessionRecord> sessions = s.Sessions;
        while (below < s.NextSessionId)
        {
            if (sessions.TryGetValue(below, out SessionRecord? next) && next.State != SessionState.Retired)
                break;
            sessions = sessions.Remove(below);
            below++;
        }

        return s with { Sessions = sessions, RetiredSessionsBelow = below };
    }

    private static (VolumeState, VolumeApplyResult) ApplyExpireHandles(VolumeState s, ApplyContext ctx, ExpireHandlesCommand c)
    {
        var expired = new List<ulong>();
        foreach (ulong id in OrEmpty(c.Handles).Order())
        {
            if (!s.Handles.TryGetValue(id, out HandleRecord? h))
                continue;
            s = ReleaseHandle(s, h, ctx.Index);
            expired.Add(id);
        }

        return (s, VolumeApplyResult.Ok(null, expired));
    }

    private static (VolumeState, VolumeApplyResult) ApplyFence(VolumeState s, ApplyContext ctx, FenceOperationCommand c)
    {
        string principal = ctx.Op?.Principal ?? string.Empty;
        QueryOperationResultMessage reply = QueryOutcome(s, c.Target, principal, out SessionRecord? session, out bool fenceable);
        if (!fenceable || session is null)
            return (s, VolumeApplyResult.Ok(reply));
        const string message = "The operation was fenced by an outcome query before it executed; it can never commit.";
        var outcome = new OutcomeRecord(c.Target.Sequence, DfsOperationKind.Unknown, UInt128.Zero, principal, DfsErrorCode.Cancelled, message,
            Protocol.MessageCodec.Encode(new ErrorReplyMessage(DfsErrorCode.Cancelled, message, c.Target)), ctx.Index, Fenced: true);
        s = OperationGate.Store(s, session, outcome);
        return (s, VolumeApplyResult.Ok(new QueryOperationResultMessage(c.Target, WireOutcomeStatus.Aborted, CommitToken.None, DfsErrorCode.Cancelled, message,
            DfsOperationKind.Unknown, ReadOnlyMemory<byte>.Empty)));
    }

    /// <summary>
    /// The outcome of an operation id as an outcome query answers it (PROTOCOL.md §9.3). <paramref name="fenceable"/> is true
    /// when the id has no outcome yet and could still execute (only such ids are fenced).
    /// </summary>
    internal static QueryOperationResultMessage QueryOutcome(VolumeState s, DfsOperationId target, string principal, out SessionRecord? session,
        out bool fenceable)
    {
        fenceable = false;
        session = null;
        if (target.IsEmpty || target.Sequence == 0)
            return Status(target, WireOutcomeStatus.NotFound, "The operation id is empty.");
        if (!s.Sessions.TryGetValue(target.Session, out session))
        {
            return Status(target, target.Session < s.NextSessionId ? WireOutcomeStatus.Expired : WireOutcomeStatus.NotFound,
                target.Session < s.NextSessionId ? "The operation's session is retired." : "This volume never issued the operation's session.");
        }

        if (!string.Equals(session.Principal, principal, StringComparison.Ordinal))
            throw Fail(DfsErrorCode.OperationIdMismatch, $"Operation {target} belongs to another principal.");
        if (session.Outcomes.TryGetValue(target.Sequence, out OutcomeRecord? outcome))
        {
            if (outcome.IsSuccess)
            {
                return new QueryOperationResultMessage(target, WireOutcomeStatus.Committed, s.TokenAt(outcome.Index), DfsErrorCode.None, null, outcome.Operation,
                    outcome.Reply);
            }

            return new QueryOperationResultMessage(target, WireOutcomeStatus.Aborted, CommitToken.None, outcome.Code, outcome.Message, outcome.Operation,
                ReadOnlyMemory<byte>.Empty);
        }

        if (session.State != SessionState.Active || target.Sequence <= session.RetiredWatermark)
            return Status(target, WireOutcomeStatus.Expired, "The operation id is outside the retained outcome window.");
        fenceable = true;
        return Status(target, WireOutcomeStatus.NotFound, "No outcome is recorded for the operation.");
    }

    private static QueryOperationResultMessage Status(DfsOperationId target, WireOutcomeStatus status, string detail)
        => new(target, status, CommitToken.None, DfsErrorCode.None, detail, DfsOperationKind.Unknown, ReadOnlyMemory<byte>.Empty);

    private static (VolumeState, VolumeApplyResult) ApplyReserve(VolumeState s, ApplyContext ctx, ReserveOperationIdsCommand c)
    {
        SessionRecord session = RequireSession(s, ctx, c.Session);
        if (c.Count is < 1 or > 65536)
            throw Fail(DfsErrorCode.InvalidArgument, "Between 1 and 65536 sequences can be reserved at once.");
        if (session.Reserved.Count >= MaxReservedRanges)
            throw Fail(DfsErrorCode.LimitExceeded, $"The session already holds {MaxReservedRanges} reserved blocks.",
                new ErrorDetailData(0, 0, 0, 0, "MaxReservedOperationBlocks", MaxReservedRanges, session.Reserved.Count + 1, null, null, null));
        ulong first = Math.Max(session.HighestSequence, session.RetiredWatermark) + 1;
        var range = new ReservedRange(first, (ulong)c.Count, 0);
        session = session with { Reserved = session.Reserved.Add(range), HighestSequence = range.Last };
        s = s with { Sessions = s.Sessions.SetItem(session.Id, session) };
        return (s, VolumeApplyResult.Ok(new ReserveOperationIdsResultMessage(session.Id, first, c.Count)));
    }
}
