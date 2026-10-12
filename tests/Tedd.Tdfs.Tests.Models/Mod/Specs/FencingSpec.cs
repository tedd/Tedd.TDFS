using System.Text;

namespace Tedd.Tdfs.Tests.Models.Mod;

/// <summary>
/// MOD-002: writer leases and fencing generations across expiry, delayed renewal, client restart, metadata leader
/// change and delayed/duplicated publication messages (FSD §7 LOCK-03..05, ARCHITECTURE §5.3, test plan §9.3).
/// </summary>
internal sealed class FencingSpec : ProtocolSpec<FencingSpec.State>
{
    private const int Clients = 2;
    private const int MaxGen = 3;
    private const int MaxCopies = 2;
    private const int MaxInFlight = 3;
    private const int MaxEpoch = 1;
    private const int MaxClientRestarts = 1;

    private const int Renew = 0;
    private const int Publish = 1;

    /// <summary>The publication check ignores the fencing generation (SafetyMutations.SkipFencingCheck).</summary>
    public const string SkipFencingCheck = "SkipFencingCheck";

    /// <summary>A delayed renewal from an expired handle silently re-activates its old generation.</summary>
    public const string RenewalResurrectsExpiredGeneration = "RenewalResurrectsExpiredGeneration";

    internal sealed class State
    {
        public int Gen;                       // inode writer generation (persisted in the inode record)
        public int ActiveClient = -1;         // active writable handle (server view)
        public int ActiveGen;
        public int Epoch;                     // metadata leader epoch
        public int[] Known = new int[Clients]; // generation each client believes it holds (0 = none / faulted)
        public int[] Msg = new int[2 * Clients * (MaxGen + 1)]; // in-flight copies by (kind, client, gen)
        public int CommittedClient = -1;      // writer of the current published revision
        public int CommittedGen;
        public int ClientRestarts;
        public string? Violation;             // ghost

        public State Clone() => new()
        {
            Gen = Gen,
            ActiveClient = ActiveClient,
            ActiveGen = ActiveGen,
            Epoch = Epoch,
            Known = (int[])Known.Clone(),
            Msg = (int[])Msg.Clone(),
            CommittedClient = CommittedClient,
            CommittedGen = CommittedGen,
            ClientRestarts = ClientRestarts,
            Violation = Violation,
        };

        public int InFlight => Msg.Sum();
    }

    private static int Slot(int kind, int client, int gen) => (kind * Clients + client) * (MaxGen + 1) + gen;

    private static string KindName(int kind) => kind == Renew ? "Renew" : "Publish";

    public override string TestId => "MOD-002";
    public override string Name => "FencingAcrossEpochsModel";

    public override string Summary =>
        "One inode, two writer clients. The metadata authority grants writable handles with a monotonically increasing writer " +
        "generation, expires leases on its own (server-authoritative) clock, survives leader changes with conservatively renewed " +
        "leases, and accepts a publication only when the handle is active and its generation equals the inode's current " +
        "generation. Clients may pause, restart and send renewals/publications that are delayed, duplicated, reordered or lost.";

    public override SpecBounds Bounds { get; } = new(80, 3_000_000,
        ["clients=2", $"writer generations<={MaxGen}", $"copies per in-flight message<={MaxCopies}", $"in-flight messages<={MaxInFlight}",
         $"leader epochs<={MaxEpoch + 1}", $"client restarts<={MaxClientRestarts}"]);

    public override IReadOnlyList<string> Properties { get; } =
    [
        "Every accepted publication carries the inode's current writer generation (an obsolete generation never publishes after replacement).",
        "The active writable handle always holds the current generation (an obsolete generation never regains authority through renewal, restart or reconnection).",
    ];

    public override IReadOnlyList<string> Assumptions { get; } =
    [
        "Lease expiry is decided only by the metadata authority's clock; client clocks are never consulted (LOCK-04, LOCK-14).",
        "The writer generation and active handles are part of the replicated, durable volume state (they survive leader changes).",
        "A new leader treats every lease as renewed at its election (conservative), so leader change never shortens a lease.",
    ];

    public override IReadOnlyList<string> Abstractions { get; } =
    [
        "Time is abstracted: 'Expire' may happen at any moment after grant (covers every renewal delay).",
        "Data uploads/receipts are abstracted into the publication message (covered by MOD-003).",
        "Message contents are reduced to (kind, client, generation).",
    ];

    public override IReadOnlyList<string> RefinementGaps { get; } =
    [
        "Actions 'Expire', 'LeaderChange', 'ClientRestart', 'Drop' and 'Duplicate' have no single fault site: they are produced by TestClock/DelayAsync, leader crashes, TestCluster.KillClient and TestNetwork rules (LOCK-009/010, OFF-021, CHA-003).",
        "That the implementation checks (HandleId, WriterGeneration) on every publication path (CommitFile, Append, SetLength, tx commit) is not proved here; MUT SkipFencingCheck tests it.",
    ];

    public override IReadOnlyDictionary<string, IReadOnlyList<string>> ActionFaultSites { get; } = ModHelpers.Map(
        ("Open", [FaultSites.RaftBeforePropose, FaultSites.ApplyAfterBeforeReply]),
        ("Close", [FaultSites.CleanupRelease]),
        ("ClientRestart", []),
        ("SendRenew", [FaultSites.ClientRequestSend]),
        ("SendPublish", [FaultSites.ClientRequestSend, FaultSites.CommitReceiptValidated]),
        ("DeliverRenew", [FaultSites.RpcRequestAccepted]),
        ("DeliverPublish", [FaultSites.RpcRequestAccepted, FaultSites.RaftBeforePropose, FaultSites.RaftAfterCommitBeforeApply]),
        ("Drop", []),
        ("Duplicate", []),
        ("Expire", []),
        ("LeaderChange", []));

    public override IReadOnlyList<BrokenVariant> BrokenVariants { get; } =
    [
        new(SkipFencingCheck, "publication accepted from any previously granted generation", nameof(SafetyMutations.SkipFencingCheck)),
        new(RenewalResurrectsExpiredGeneration, "a delayed renewal re-activates an expired handle with its old generation"),
    ];

    protected override State Initial(string variant) => new();

    protected override IEnumerable<(string Action, State Next)> Next(State s, string variant)
    {
        for (int c = 0; c < Clients; c++)
        {
            if (s.ActiveClient == -1 && s.Gen < MaxGen && s.Known[c] == 0)
            {
                State t = s.Clone();
                t.Gen++;
                t.ActiveClient = c;
                t.ActiveGen = t.Gen;
                t.Known[c] = t.Gen;
                yield return ($"Open(c{c},g{t.Gen})", t);
            }

            if (s.Known[c] != 0 && s.ActiveClient == c && s.ActiveGen == s.Known[c])
            {
                State t = s.Clone();
                t.ActiveClient = -1;
                t.ActiveGen = 0;
                t.Known[c] = 0;
                yield return ($"Close(c{c})", t);
            }

            if (s.Known[c] != 0 && s.ClientRestarts < MaxClientRestarts)
            {
                State t = s.Clone();
                t.Known[c] = 0;
                t.ClientRestarts++;
                yield return ($"ClientRestart(c{c})", t);
            }

            if (s.Known[c] != 0 && s.InFlight < MaxInFlight)
            {
                for (int kind = 0; kind < 2; kind++)
                {
                    int slot = Slot(kind, c, s.Known[c]);
                    if (s.Msg[slot] >= MaxCopies)
                        continue;
                    State t = s.Clone();
                    t.Msg[slot]++;
                    yield return ($"Send{KindName(kind)}(c{c},g{s.Known[c]})", t);
                }
            }
        }

        for (int kind = 0; kind < 2; kind++)
        {
            for (int c = 0; c < Clients; c++)
            {
                for (int g = 1; g <= MaxGen; g++)
                {
                    int slot = Slot(kind, c, g);
                    if (s.Msg[slot] == 0)
                        continue;

                    State delivered = s.Clone();
                    delivered.Msg[slot]--;
                    if (kind == Renew)
                        DeliverRenew(delivered, c, g, variant);
                    else
                        DeliverPublish(delivered, c, g, variant);
                    yield return ($"Deliver{KindName(kind)}(c{c},g{g})", delivered);

                    State dropped = s.Clone();
                    dropped.Msg[slot]--;
                    yield return ($"Drop({KindName(kind)},c{c},g{g})", dropped);

                    if (s.Msg[slot] < MaxCopies && s.InFlight < MaxInFlight)
                    {
                        State dup = s.Clone();
                        dup.Msg[slot]++;
                        yield return ($"Duplicate({KindName(kind)},c{c},g{g})", dup);
                    }
                }
            }
        }

        if (s.ActiveClient != -1)
        {
            State t = s.Clone();
            t.ActiveClient = -1;
            t.ActiveGen = 0;
            yield return ($"Expire(c{s.ActiveClient},g{s.ActiveGen})", t);
        }

        if (s.Epoch < MaxEpoch)
        {
            State t = s.Clone();
            t.Epoch++;
            yield return ($"LeaderChange(e{t.Epoch})", t);
        }
    }

    private static void DeliverRenew(State t, int c, int g, string variant)
    {
        if (t.ActiveClient == c && t.ActiveGen == g)
            return; // renewed
        if (variant == RenewalResurrectsExpiredGeneration && t.ActiveClient == -1)
        {
            t.ActiveClient = c;
            t.ActiveGen = g;
            return;
        }

        if (t.Known[c] == g)
            t.Known[c] = 0; // LeaseLost: the stream is faulted
    }

    private static void DeliverPublish(State t, int c, int g, string variant)
    {
        bool accept = variant == SkipFencingCheck
            ? g >= 1
            : t.ActiveClient == c && t.ActiveGen == g && g == t.Gen;
        if (!accept)
        {
            if (t.Known[c] == g)
                t.Known[c] = 0; // LeaseLost
            return;
        }

        if (g != t.Gen)
            t.Violation ??= $"publication by c{c} with obsolete generation {g} accepted after generation {t.Gen} was granted";
        t.CommittedClient = c;
        t.CommittedGen = g;
    }

    protected override string? Invariant(State s, string variant)
    {
        if (s.Violation is not null)
            return "fencing: " + s.Violation;
        if (s.ActiveClient != -1 && s.ActiveGen != s.Gen)
            return $"fencing: obsolete generation {s.ActiveGen} of c{s.ActiveClient} regained authority while the inode generation is {s.Gen}";
        return null;
    }

    protected override string Key(State s)
    {
        var sb = new StringBuilder(64);
        sb.Append(s.Gen).Append('A').Append(s.ActiveClient).Append(':').Append(s.ActiveGen).Append('E').Append(s.Epoch).Append('K');
        ModHelpers.AppendInts(sb, s.Known);
        sb.Append('M');
        ModHelpers.AppendInts(sb, s.Msg);
        sb.Append('C').Append(s.CommittedClient).Append(':').Append(s.CommittedGen).Append('R').Append(s.ClientRestarts)
            .Append(s.Violation is null ? '.' : '!');
        return sb.ToString();
    }
}
