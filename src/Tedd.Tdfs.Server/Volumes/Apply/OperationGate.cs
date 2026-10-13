using System.Collections.Immutable;
using Tedd.Tdfs.Server.Volumes.Commands;
using Tedd.Tdfs.Server.Volumes.State;

namespace Tedd.Tdfs.Server.Volumes.Apply;

/// <summary>Decision of the operation gate.</summary>
internal enum GateKind : byte
{
    Execute = 0,
    Stored = 1,
    Expired = 2,
    Mismatch = 3,
    Invalid = 4,
}

/// <summary>Result of <see cref="OperationGate.Check"/>.</summary>
internal readonly record struct GateDecision(GateKind Kind, string Message, OutcomeRecord? Outcome)
{
    public static GateDecision Execute => new(GateKind.Execute, string.Empty, null);
}

/// <summary>
/// The operation gate of ARCHITECTURE §5.2 (FSD §13): binds every operation id to its principal, request kind and payload
/// identity, returns stored outcomes instead of executing twice, rejects retired or expired ids, and maintains the durable
/// retirement watermarks (OUT-012/019). Pure functions over <see cref="VolumeState"/>; used by apply and, for answering
/// retries early, by the leader.
/// </summary>
internal static class OperationGate
{
    /// <summary>Hard bound of retained outcomes per session, even while reserved ids are pending (memory safety).</summary>
    public const int HardOutcomeFactor = 4;

    /// <summary>Classifies an operation against the applied state.</summary>
    public static GateDecision Check(VolumeState state, OperationHeader op)
    {
        DfsOperationId id = op.Id;
        if (!state.Sessions.TryGetValue(id.Session, out SessionRecord? session))
        {
            return id.Session < state.NextSessionId
                ? new GateDecision(GateKind.Expired, $"Operation {id} belongs to a retired session.", null)
                : new GateDecision(GateKind.Invalid, $"Operation {id} names a session this volume never issued.", null);
        }

        if (!string.Equals(session.Principal, op.Principal, StringComparison.Ordinal))
            return new GateDecision(GateKind.Mismatch, $"Operation {id} belongs to another principal.", null);
        if (session.State == SessionState.Retired)
            return new GateDecision(GateKind.Expired, $"Operation {id}: its session is retired.", null);
        if (id.Sequence == 0)
            return new GateDecision(GateKind.Invalid, "Operation sequences start at 1.", null);

        if (session.Outcomes.TryGetValue(id.Sequence, out OutcomeRecord? outcome) && (op.Flags & CommandFlags.DisableDedup) == 0)
        {
            // A fenced id answers every later request with its definite abort, whatever the request (OUT-013).
            if (outcome.Fenced)
                return new GateDecision(GateKind.Stored, string.Empty, outcome);
            if (outcome.Operation != op.Operation || outcome.PayloadHash != op.PayloadHash
                || !string.Equals(outcome.Principal, op.Principal, StringComparison.Ordinal))
            {
                return new GateDecision(GateKind.Mismatch, $"Operation {id} was already used for a different request.", outcome);
            }

            return new GateDecision(GateKind.Stored, string.Empty, outcome);
        }

        if ((op.Flags & CommandFlags.ForgetWatermark) == 0)
        {
            if (session.State == SessionState.Expired)
                return new GateDecision(GateKind.Expired, $"Operation {id}: its session expired.", null);
            if (id.Sequence <= session.RetiredWatermark)
                return new GateDecision(GateKind.Expired, $"Operation {id} is below the retired watermark {session.RetiredWatermark}.", null);
        }

        return GateDecision.Execute;
    }

    /// <summary>Stores the outcome of an executed operation and retires the oldest beyond the session's bound.</summary>
    public static VolumeState Record(VolumeState state, OperationHeader op, VolumeApplyResult result, ulong index)
    {
        if (!state.Sessions.TryGetValue(op.Id.Session, out SessionRecord? session) || session.State == SessionState.Retired)
            return state;
        var outcome = new OutcomeRecord(op.Id.Sequence, op.Operation, op.PayloadHash, op.Principal, result.Code, result.Message, result.Frame, index,
            Fenced: false);
        return Store(state, session, outcome);
    }

    /// <summary>Stores <paramref name="outcome"/> in <paramref name="session"/> (fenced or executed).</summary>
    public static VolumeState Store(VolumeState state, SessionRecord session, OutcomeRecord outcome)
    {
        bool isNew = !session.Outcomes.ContainsKey(outcome.Sequence);
        ImmutableList<ReservedRange> reserved = session.Reserved;
        if (isNew)
        {
            for (int i = 0; i < reserved.Count; i++)
            {
                ReservedRange range = reserved[i];
                if (!range.Contains(outcome.Sequence))
                    continue;
                ReservedRange used = range with { Used = range.Used + 1 };
                reserved = used.Used >= used.Count ? reserved.RemoveAt(i) : reserved.SetItem(i, used);
                break;
            }
        }

        session = session with
        {
            Outcomes = session.Outcomes.SetItem(outcome.Sequence, outcome),
            HighestSequence = Math.Max(session.HighestSequence, outcome.Sequence),
            Reserved = reserved,
        };
        session = RetireByCount(session);
        return state with { Sessions = state.Sessions.SetItem(session.Id, session) };
    }

    /// <summary>
    /// Retires outcomes of <paramref name="sessionId"/> below the client's acknowledgement watermark (the client will never
    /// retry or query them), advancing the durable retired watermark. Only the session's own principal can acknowledge its
    /// outcomes: a watermark sent with another principal's session id is ignored (it would expire that principal's
    /// operation ids, OUT-004).
    /// </summary>
    public static VolumeState Acknowledge(VolumeState state, ulong sessionId, ulong ackWatermark, string principal)
    {
        if (!state.Sessions.TryGetValue(sessionId, out SessionRecord? session) || session.State != SessionState.Active
            || !string.Equals(session.Principal, principal, StringComparison.Ordinal))
        {
            return state;
        }

        ulong below = ackWatermark - 1;
        ulong cap = FirstUnusedReserved(session) is { } first ? Math.Min(below, first - 1) : below;
        if (cap <= session.RetiredWatermark)
            return state;
        ImmutableSortedDictionary<ulong, OutcomeRecord> outcomes = session.Outcomes;
        foreach (ulong seq in outcomes.Keys)
        {
            if (seq > cap)
                break;
            outcomes = outcomes.Remove(seq);
        }

        session = session with { Outcomes = outcomes, RetiredWatermark = cap, HighestSequence = Math.Max(session.HighestSequence, cap) };
        return state with { Sessions = state.Sessions.SetItem(session.Id, session) };
    }

    private static SessionRecord RetireByCount(SessionRecord session)
    {
        int max = Math.Max(1, session.MaxOutcomes);
        if (session.Outcomes.Count <= max)
            return session;
        ImmutableSortedDictionary<ulong, OutcomeRecord> outcomes = session.Outcomes;
        ulong watermark = session.RetiredWatermark;
        ImmutableList<ReservedRange> reserved = session.Reserved;
        while (outcomes.Count > max)
        {
            ulong oldest = outcomes.Keys.First();
            if (FirstUnused(reserved) is { } blocking && blocking < oldest)
            {
                // Retiring past an unused reserved id would expire it before use: keep the outcome (soft bound) unless the
                // hard bound is exceeded, in which case the oldest blocking reservation is abandoned.
                if (outcomes.Count <= max * HardOutcomeFactor)
                    break;
                reserved = reserved.RemoveAt(0);
                continue;
            }

            outcomes = outcomes.Remove(oldest);
            watermark = Math.Max(watermark, oldest);
        }

        return session with { Outcomes = outcomes, RetiredWatermark = watermark, Reserved = reserved };
    }

    private static ulong? FirstUnusedReserved(SessionRecord session) => FirstUnused(session.Reserved);

    private static ulong? FirstUnused(ImmutableList<ReservedRange> reserved) => reserved.Count == 0 ? null : reserved[0].First;
}
