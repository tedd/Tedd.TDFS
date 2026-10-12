using System.Text;

namespace Tedd.Tdfs.Tests.Models.Mod;

/// <summary>
/// MOD-004: operation-id deduplication, bounded outcome retention, durable retirement watermark, session expiry and
/// definite fencing of unresolved ids (FSD §13, ARCHITECTURE §5.2 OperationGate, §7).
/// </summary>
internal sealed class IdempotencySpec : ProtocolSpec<IdempotencySpec.State>
{
    private const int Seqs = 2;           // operation sequences 1 and 2 of one session
    private const int Payloads = 2;       // canonical payload of seq s is s-1; the other one is an incompatible reuse
    private const int MaxCopies = 2;      // in-flight copies per (seq, payload)
    private const int MaxSends = 2;       // client transmissions per seq (original + retry)
    private const int MaxOutcomes = 1;    // retained outcomes per session before the oldest is retired

    private const int NoOutcome = 0;
    private const int Aborted = 3;        // 1 + payload = committed with that payload

    /// <summary>Retained outcomes are not consulted (SafetyMutations.DisableOperationDedup).</summary>
    public const string DisableOperationDedup = "DisableOperationDedup";

    /// <summary>Retiring an outcome forgets it without advancing the durable watermark (SafetyMutations.ForgetExpiredIdWatermark).</summary>
    public const string ForgetExpiredIdWatermark = "ForgetExpiredIdWatermark";

    internal sealed class State
    {
        public bool Expired;
        public int Watermark;
        public int[] Outcome = new int[Seqs];
        public int[] Effects = new int[Seqs];       // ghost: logical executions (capped at 2)
        public bool[] Fenced = new bool[Seqs];      // ghost: a definite abort was recorded
        public bool[] Done = new bool[Seqs];        // client learned the committed result
        public int ClientAck;
        public int[] Sends = new int[Seqs];
        public bool[] MismatchSent = new bool[Seqs];
        public int[] InFlight = new int[Seqs * Payloads];

        public State Clone() => new()
        {
            Expired = Expired,
            Watermark = Watermark,
            Outcome = (int[])Outcome.Clone(),
            Effects = (int[])Effects.Clone(),
            Fenced = (bool[])Fenced.Clone(),
            Done = (bool[])Done.Clone(),
            ClientAck = ClientAck,
            Sends = (int[])Sends.Clone(),
            MismatchSent = (bool[])MismatchSent.Clone(),
            InFlight = (int[])InFlight.Clone(),
        };
    }

    public override string TestId => "MOD-004";
    public override string Name => "IdempotencyAndRetirementModel";

    public override string Summary =>
        "One server-issued session with two client-allocated sequences. Requests (originals, retries, incompatible reuse with a " +
        "different payload) are delayed, duplicated, reordered or lost. The leader applies the OperationGate: expired session or " +
        "retired id → OperationExpired; retained outcome → stored result or OperationIdMismatch; otherwise execute and store. " +
        "Outcomes are retired when the client acknowledges them or when more than MaxOutcomesPerSession are retained, advancing " +
        "the durable retired watermark. QueryOperation(resolve:true) fences an unresolved id with a definite abort.";

    public override SpecBounds Bounds { get; } = new(64, 3_000_000,
        ["sessions=1", "sequences=2", "payloads per id=2 (canonical + incompatible)", $"copies per message<={MaxCopies}",
         $"client sends per id<={MaxSends}", $"MaxOutcomesPerSession={MaxOutcomes}"]);

    public override IReadOnlyList<string> Properties { get; } =
    [
        "One accepted operation id has at most one logical effect, across retries, duplicates, delayed copies, outcome compaction and session retirement.",
        "An id definitely aborted (fenced) never executes later.",
    ];

    public override IReadOnlyList<string> Assumptions { get; } =
    [
        "Outcome records, the retired watermark and session state are replicated durable state (they survive leader change and restart).",
        "Session ids are never reused (sessions below RetiredSessionsBelow are rejected).",
    ];

    public override IReadOnlyList<string> Abstractions { get; } =
    [
        "The operation effect is a counter; principal/opKind binding is folded into the payload identity.",
        "Leader-side coalescing of in-flight duplicates is not modelled (it only removes behaviours).",
    ];

    public override IReadOnlyList<string> RefinementGaps { get; } =
    [
        "Actions 'SendMismatched', 'Duplicate', 'Drop', 'ClientAck' and 'ExpireSession' are produced by client behaviour, TestNetwork rules and session timeouts, not by a fault site.",
        "That every mutating command passes through the OperationGate (including transactions, snapshots and backup admin) is not proved here (OUT and MUT DisableOperationDedup tests).",
    ];

    public override IReadOnlyDictionary<string, IReadOnlyList<string>> ActionFaultSites { get; } = ModHelpers.Map(
        ("Send", [FaultSites.ClientRequestSend, FaultSites.ClientReplyReceived]),
        ("SendMismatched", []),
        ("Duplicate", []),
        ("Drop", []),
        ("Deliver", [FaultSites.RpcRequestAccepted, FaultSites.RaftBeforePropose, FaultSites.RaftAfterCommitBeforeApply, FaultSites.ApplyAfterBeforeReply, FaultSites.RpcReplySend]),
        ("ClientAck", []),
        ("RetireAcked", [FaultSites.CleanupRelease]),
        ("Fence", [FaultSites.RaftBeforePropose, FaultSites.ApplyAfterBeforeReply]),
        ("ExpireSession", []));

    public override IReadOnlyList<BrokenVariant> BrokenVariants { get; } =
    [
        new(DisableOperationDedup, "retained outcomes are ignored and every delivered copy executes", nameof(SafetyMutations.DisableOperationDedup)),
        new(ForgetExpiredIdWatermark, "retired outcomes are dropped without advancing the durable retired watermark", nameof(SafetyMutations.ForgetExpiredIdWatermark)),
    ];

    protected override State Initial(string variant) => new();

    private static int Canonical(int seq) => seq; // seq index 0 -> payload 0, seq index 1 -> payload 1

    protected override IEnumerable<(string Action, State Next)> Next(State s, string variant)
    {
        for (int q = 0; q < Seqs; q++)
        {
            int canon = Canonical(q);
            int canonSlot = q * Payloads + canon;
            if (s.Sends[q] < MaxSends && s.InFlight[canonSlot] < MaxCopies)
            {
                State t = s.Clone();
                t.Sends[q]++;
                t.InFlight[canonSlot]++;
                yield return ($"Send(seq{q + 1})", t);
            }

            int otherSlot = q * Payloads + (1 - canon);
            if (!s.MismatchSent[q] && s.InFlight[otherSlot] < MaxCopies)
            {
                State t = s.Clone();
                t.MismatchSent[q] = true;
                t.InFlight[otherSlot]++;
                yield return ($"SendMismatched(seq{q + 1})", t);
            }

            for (int p = 0; p < Payloads; p++)
            {
                int slot = q * Payloads + p;
                if (s.InFlight[slot] == 0)
                    continue;
                State delivered = s.Clone();
                delivered.InFlight[slot]--;
                Gate(delivered, q, p, variant);
                yield return ($"Deliver(seq{q + 1},p{p})", delivered);

                State dropped = s.Clone();
                dropped.InFlight[slot]--;
                yield return ($"Drop(seq{q + 1},p{p})", dropped);

                if (s.InFlight[slot] < MaxCopies)
                {
                    State dup = s.Clone();
                    dup.InFlight[slot]++;
                    yield return ($"Duplicate(seq{q + 1},p{p})", dup);
                }
            }

            if (s.Done[q] && s.ClientAck < q + 1)
            {
                State t = s.Clone();
                t.ClientAck = q + 1;
                yield return ($"ClientAck(seq{q + 1})", t);
            }

            if (q + 1 <= s.ClientAck && s.Outcome[q] != NoOutcome)
            {
                State t = s.Clone();
                Retire(t, q, variant);
                yield return ($"RetireAcked(seq{q + 1})", t);
            }

            if (s.Outcome[q] == NoOutcome && q + 1 > s.Watermark && !s.Expired)
            {
                State t = s.Clone();
                t.Outcome[q] = Aborted;
                t.Fenced[q] = true;
                yield return ($"Fence(seq{q + 1})", t);
            }
        }

        if (!s.Expired)
        {
            State t = s.Clone();
            t.Expired = true;
            yield return ("ExpireSession", t);
        }
    }

    private static void Gate(State t, int q, int p, string variant)
    {
        bool canonical = p == Canonical(q);
        if (t.Expired)
            return; // OperationExpired (session expired)
        if (variant != DisableOperationDedup)
        {
            if (t.Outcome[q] != NoOutcome)
            {
                // Stored outcome (or OperationIdMismatch for an incompatible payload); never a second effect.
                if (canonical && t.Outcome[q] == 1 + p)
                    t.Done[q] = true;
                return;
            }

            if (q + 1 <= t.Watermark)
                return; // OperationExpired (retired id)
        }

        t.Effects[q] = Math.Min(2, t.Effects[q] + 1);
        t.Outcome[q] = 1 + p;
        if (canonical)
            t.Done[q] = true;

        int retained = t.Outcome.Count(o => o != NoOutcome);
        if (retained > MaxOutcomes)
        {
            for (int oldest = 0; oldest < Seqs; oldest++)
            {
                if (oldest != q && t.Outcome[oldest] != NoOutcome)
                {
                    Retire(t, oldest, variant);
                    break;
                }
            }
        }
    }

    private static void Retire(State t, int q, string variant)
    {
        t.Outcome[q] = NoOutcome;
        if (variant != ForgetExpiredIdWatermark)
            t.Watermark = Math.Max(t.Watermark, q + 1);
    }

    protected override string? Invariant(State s, string variant)
    {
        for (int q = 0; q < Seqs; q++)
        {
            if (s.Effects[q] > 1)
                return $"operation id seq{q + 1} executed {s.Effects[q]} logical effects";
            if (s.Fenced[q] && s.Effects[q] > 0)
                return $"operation id seq{q + 1} has both a logical effect and a definite (fenced) abort outcome";
        }

        return null;
    }

    protected override string Key(State s)
    {
        var sb = new StringBuilder(48);
        sb.Append(s.Expired ? 'X' : '-').Append(s.Watermark).Append('O');
        ModHelpers.AppendInts(sb, s.Outcome);
        sb.Append('E');
        ModHelpers.AppendInts(sb, s.Effects);
        sb.Append('F');
        ModHelpers.AppendBools(sb, s.Fenced);
        sb.Append('D');
        ModHelpers.AppendBools(sb, s.Done);
        sb.Append('A').Append(s.ClientAck).Append('S');
        ModHelpers.AppendInts(sb, s.Sends);
        sb.Append('M');
        ModHelpers.AppendBools(sb, s.MismatchSent);
        sb.Append('I');
        ModHelpers.AppendInts(sb, s.InFlight);
        return sb.ToString();
    }
}
