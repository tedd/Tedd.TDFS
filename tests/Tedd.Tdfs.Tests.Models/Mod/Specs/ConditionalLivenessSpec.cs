using System.Text;

namespace Tedd.Tdfs.Tests.Models.Mod;

/// <summary>
/// MOD-011: conditional liveness of one client mutation (FSD §13, §14.1 RCV-08, test plan §3.3, INV-17). Progress
/// ("the client definitely learns the committed result") is checked only in states where the declared assumptions hold:
/// quorum available, client↔leader communication available, reclaimable capacity, no further faults, and fair
/// scheduling of system actions. Outside the assumptions the model must report unavailability/uncertainty truthfully
/// instead of fabricating success or a definite failure.
/// </summary>
internal sealed class ConditionalLivenessSpec : ProtocolSpec<ConditionalLivenessSpec.State>
{
    private const int FaultBudget = 3;
    private const int InnerSearchLimit = 10_000;

    // Client knowledge.
    private const int Idle = 0;
    private const int Sent = 1;
    private const int Admitted = 2;       // request accepted by the leader, awaiting the reply
    private const int KnowsCommitted = 3; // goal
    private const int ToldUnavailable = 4;
    private const int Unknown = 5;

    /// <summary>The leader drops a pending proposal on quorum loss and the client waits for a reply without a deadline.</summary>
    public const string DropPendingAndWaitForever = "DropPendingProposalAndWaitForever";

    /// <summary>The leader answers success while the quorum is unavailable (SafetyMutations.QuorumFromReachableNodes analogue).</summary>
    public const string FabricateSuccessWithoutQuorum = "FabricateSuccessWithoutQuorum";

    /// <summary>A client timeout after possible submission is reported as a definite failure (SafetyMutations.TreatUnknownAsAborted).</summary>
    public const string ReportTimeoutAsDefiniteFailure = "ReportTimeoutAsDefiniteFailure";

    internal sealed class State
    {
        public bool Quorum = true;
        public bool Network = true;
        public bool Capacity = true;
        public int Budget = FaultBudget;
        public bool Pending;           // proposed, not yet committed
        public bool Committed;         // server-side outcome
        public int Client;             // client knowledge

        public State Clone() => (State)MemberwiseClone();
    }

    public override string TestId => "MOD-011";
    public override string Name => "ConditionalLivenessModel";

    public override string Summary =>
        "One client mutation with a stable operation id against a leader whose quorum, client link and admission capacity " +
        "can fail and be restored by the environment (at most three failures). System actions: send/retry with the same id, " +
        "admission (rejecting with CapacityExhausted/QuorumUnavailable before proposing), commit when quorum is available, " +
        "reply, client deadline (outcome unknown), dedup of retries, and GC/compaction restoring capacity when quorum is available.";

    public override SpecBounds Bounds { get; } = new(64, 1_000_000,
        [$"environment failures<={FaultBudget} (quorum, network, capacity)", "one operation id", $"inner fair-path search<={InnerSearchLimit} states"]);

    public override IReadOnlyList<string> Properties { get; } =
    [
        "Conditional progress: in every reachable state where quorum, communication and reclaimable capacity are available and no further failures occur, every fair execution of system actions reaches a state where the client knows the committed result (no deadlock, no livelock).",
        "Truthful reporting: the client never learns 'committed' unless the operation committed, and never learns a definite failure for an operation that is pending or committed.",
    ];

    public override IReadOnlyList<string> Assumptions { get; } =
    [
        "Strong fairness: system actions (retry, admission, commit, reply, GC) that are enabled infinitely often are eventually taken.",
        "Eventual communication: after the last failure the client↔leader link stays up.",
        "Quorum: a majority of configured voters is eventually available and stays available.",
        "Capacity: reclaimable space exists, so GC restores admission once quorum is available.",
        "Outside these assumptions only safety (truthful reporting) is claimed, never progress.",
    ];

    public override IReadOnlyList<string> Abstractions { get; } =
    [
        "Leader election and replication are folded into the 'Quorum' flag.",
        "Time is abstracted: client deadlines fire whenever the request cannot currently make progress.",
        "Liveness is checked as AF(goal) under strong fairness (equivalently: the goal stays reachable from every system-reachable state) from each assumption-satisfying state (bounded inner search).",
    ];

    public override IReadOnlyList<string> RefinementGaps { get; } =
    [
        "Actions 'FailQuorum', 'FailNetwork', 'FailCapacity', 'RestoreQuorum', 'RestoreNetwork', 'RestoreCapacity' and 'ClientDeadline' are environment/time events, not fault sites (TestNetwork, TestDisk, TestClock drive them in OPS/OFF tests).",
        "Real-time bounds are not modelled; OPS bounded-liveness tests must supply explicit timeouts (WaitUntilAsync) under the same assumptions.",
    ];

    public override IReadOnlyDictionary<string, IReadOnlyList<string>> ActionFaultSites { get; } = ModHelpers.Map(
        ("FailQuorum", []),
        ("FailNetwork", []),
        ("FailCapacity", []),
        ("RestoreQuorum", []),
        ("RestoreNetwork", []),
        ("RestoreCapacity", []),
        ("Send", [FaultSites.ClientOperationStart, FaultSites.ClientRequestSend]),
        ("Admit", [FaultSites.RpcRequestAccepted, FaultSites.RaftBeforePropose]),
        ("Commit", [FaultSites.RaftAfterCommitBeforeApply, FaultSites.ApplyAfterBeforeReply]),
        ("Reply", [FaultSites.RpcReplySend, FaultSites.ClientReplyReceived]),
        ("ClientDeadline", []),
        ("Gc", [FaultSites.GcSweep, FaultSites.CompactionDelete]));

    public override IReadOnlyList<BrokenVariant> BrokenVariants { get; } =
    [
        new(DropPendingAndWaitForever, "a pending proposal is dropped on quorum loss and the client waits for the reply without a deadline (liveness lost although assumptions are restored)"),
        new(FabricateSuccessWithoutQuorum, "the leader reports success while the quorum is unavailable", nameof(SafetyMutations.QuorumFromReachableNodes)),
        new(ReportTimeoutAsDefiniteFailure, "a deadline after possible submission is reported as a definite failure", nameof(SafetyMutations.TreatUnknownAsAborted)),
    ];

    protected override State Initial(string variant) => new();

    protected override IEnumerable<(string Action, State Next)> Next(State s, string variant)
    {
        foreach ((string action, State next) in Environment(s, variant))
            yield return (action, next);
        foreach ((string action, State next) in SystemActions(s, variant))
            yield return (action, next);
    }

    private static IEnumerable<(string Action, State Next)> Environment(State s, string variant)
    {
        if (s.Budget > 0)
        {
            if (s.Quorum)
            {
                State t = s.Clone();
                t.Quorum = false;
                t.Budget--;
                if (variant == DropPendingAndWaitForever)
                    t.Pending = false;
                yield return ("FailQuorum", t);
            }

            if (s.Network)
            {
                State t = s.Clone();
                t.Network = false;
                t.Budget--;
                yield return ("FailNetwork", t);
            }

            if (s.Capacity)
            {
                State t = s.Clone();
                t.Capacity = false;
                t.Budget--;
                yield return ("FailCapacity", t);
            }
        }

        if (!s.Quorum)
        {
            State t = s.Clone();
            t.Quorum = true;
            yield return ("RestoreQuorum", t);
        }

        if (!s.Network)
        {
            State t = s.Clone();
            t.Network = true;
            yield return ("RestoreNetwork", t);
        }

        if (!s.Capacity)
        {
            State t = s.Clone();
            t.Capacity = true;
            yield return ("RestoreCapacity", t);
        }
    }

    /// <summary>System actions: the ones fairness applies to.</summary>
    private static IEnumerable<(string Action, State Next)> SystemActions(State s, string variant)
    {
        if (s.Client is Idle or ToldUnavailable or Unknown)
        {
            State t = s.Clone();
            t.Client = Sent;
            yield return ("Send(same operation id)", t);
        }

        if (s.Client == Sent && s.Network)
        {
            State t = s.Clone();
            if (s.Committed)
            {
                t.Client = KnowsCommitted; // retry answered from the retained outcome
                yield return ("Admit(stored outcome)", t);
            }
            else if (s.Pending)
            {
                t.Client = Admitted;       // coalesced with the in-flight proposal
                yield return ("Admit(coalesced)", t);
            }
            else if (!s.Capacity)
            {
                t.Client = ToldUnavailable;
                yield return ("Admit(rejected:CapacityExhausted)", t);
            }
            else if (!s.Quorum)
            {
                if (variant == FabricateSuccessWithoutQuorum)
                {
                    t.Client = KnowsCommitted;
                    yield return ("Admit(fabricated success)", t);
                }
                else
                {
                    t.Client = ToldUnavailable;
                    yield return ("Admit(rejected:QuorumUnavailable)", t);
                }
            }
            else
            {
                t.Pending = true;
                t.Client = Admitted;
                yield return ("Admit(proposed)", t);
            }
        }

        if (s.Pending && s.Quorum)
        {
            State t = s.Clone();
            t.Pending = false;
            t.Committed = true;
            yield return ("Commit", t);
        }

        if (s.Client == Admitted && s.Committed && s.Network)
        {
            State t = s.Clone();
            t.Client = KnowsCommitted;
            yield return ("Reply", t);
        }

        bool stuck = !s.Network || (s.Client == Admitted && !s.Committed && (!s.Quorum || !s.Pending));
        if (s.Client is Sent or Admitted && stuck && !(variant == DropPendingAndWaitForever && s.Network))
        {
            State t = s.Clone();
            t.Client = variant == ReportTimeoutAsDefiniteFailure ? ToldUnavailable : Unknown;
            yield return ("ClientDeadline", t);
        }

        if (!s.Capacity && s.Quorum)
        {
            State t = s.Clone();
            t.Capacity = true;
            yield return ("Gc(reclaims space)", t);
        }

    }

    private static bool AssumptionsHold(State s) => s.Quorum && s.Network && s.Budget == 0;

    protected override string? Invariant(State s, string variant)
    {
        if (s.Client == KnowsCommitted && !s.Committed)
            return "truthful reporting: the client was told 'committed' for an operation that did not commit";
        if (s.Client == ToldUnavailable && (s.Pending || s.Committed))
            return "truthful reporting: a definite failure was reported for an operation that is pending or committed";

        bool reachable = GoalInevitableUnderFairness(s, variant, out string? reason);
        if (AssumptionsHold(s))
        {
            Count("statesWhereAssumptionsHold");
            if (!reachable)
                return "conditional liveness violated under restored assumptions: " + reason;
        }
        else if (!reachable)
        {
            Count("statesWithoutProgressBecauseAssumptionsFail");
        }

        return null;
    }

    /// <summary>
    /// AF(goal) under strong fairness of system actions with the environment frozen: for a finite system this holds iff
    /// the goal remains reachable from every state reachable from <paramref name="start"/> by system actions (no deadlock
    /// and no trap component without progress).
    /// </summary>
    private static bool GoalInevitableUnderFairness(State start, string variant, out string? reason)
    {
        var states = new Dictionary<string, State>(StringComparer.Ordinal);
        var predecessors = new Dictionary<string, List<string>>(StringComparer.Ordinal);
        var queue = new Queue<string>();
        string startKey = KeyOf(start);
        states[startKey] = start;
        predecessors[startKey] = [];
        queue.Enqueue(startKey);
        while (queue.Count > 0)
        {
            if (states.Count > InnerSearchLimit)
            {
                reason = $"inner fair-path search exceeded {InnerSearchLimit} states (inconclusive is not a pass)";
                return false;
            }

            string key = queue.Dequeue();
            State state = states[key];
            if (state.Client == KnowsCommitted)
                continue; // goal is absorbing for this check
            foreach ((_, State next) in SystemActions(state, variant))
            {
                string nextKey = KeyOf(next);
                if (!states.ContainsKey(nextKey))
                {
                    states[nextKey] = next;
                    predecessors[nextKey] = [];
                    queue.Enqueue(nextKey);
                }

                predecessors[nextKey].Add(key);
            }
        }

        var canReachGoal = new HashSet<string>(StringComparer.Ordinal);
        var back = new Queue<string>();
        foreach ((string key, State state) in states)
        {
            if (state.Client == KnowsCommitted && canReachGoal.Add(key))
                back.Enqueue(key);
        }

        while (back.Count > 0)
        {
            foreach (string pred in predecessors[back.Dequeue()])
            {
                if (canReachGoal.Add(pred))
                    back.Enqueue(pred);
            }
        }

        foreach ((string key, State state) in states.OrderBy(kv => kv.Key, StringComparer.Ordinal))
        {
            if (canReachGoal.Contains(key))
                continue;
            bool deadlock = !SystemActions(state, variant).Any();
            reason = (deadlock ? "deadlock" : "trap without progress") +
                $" (client={ClientName(state.Client)}, pending={state.Pending}, committed={state.Committed}, quorum={state.Quorum}, network={state.Network}, capacity={state.Capacity})";
            return false;
        }

        reason = null;
        return true;
    }

    private static string ClientName(int client) => client switch
    {
        Idle => "Idle",
        Sent => "Sent",
        Admitted => "Admitted",
        KnowsCommitted => "KnowsCommitted",
        ToldUnavailable => "ToldUnavailable",
        _ => "Unknown",
    };

    private static string KeyOf(State s)
    {
        var sb = new StringBuilder(16);
        ModHelpers.AppendBools(sb, [s.Quorum, s.Network, s.Capacity, s.Pending, s.Committed]);
        sb.Append(s.Budget).Append(s.Client);
        return sb.ToString();
    }

    protected override string Key(State s) => KeyOf(s);
}
