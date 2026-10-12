using System.Text;

namespace Tedd.Tdfs.Tests.Models.Mod;

/// <summary>
/// MOD-009: single-server membership changes, effective on append, one pending change at a time, with the
/// "commit an entry of the current term first" rule (ARCHITECTURE §4, FSD §14.1 RCV-01/RCV-04). Old/new voter
/// configurations coexist on delayed nodes; only the safe protocol may transfer authority.
/// </summary>
internal sealed class MembershipSpec : ProtocolSpec<MembershipSpec.State>
{
    private const int N = 4;               // n0..n3; n3 is the node being added
    private const int MaxTerm = 2;
    private const int MaxLog = 3;
    private const int Noop = 0;

    // Configurations as member bit masks.
    private const int C0 = 0b0111;         // {n0,n1,n2} initial
    private const int CAdd = 0b1111;       // {n0,n1,n2,n3}
    private const int CRemove = 0b0110;    // {n1,n2}
    private const int CFinal = 0b1110;     // {n1,n2,n3}

    /// <summary>Two voters are replaced in one configuration change (C0 → {n1,n2,n3} directly).</summary>
    public const string ReplaceTwoVotersAtOnce = "ReplaceTwoVotersAtOnce";

    /// <summary>A new change is appended without waiting for the previous change and a current-term entry to commit.</summary>
    public const string NoCommitBeforeNextChange = "NoCommitBeforeNextChange";

    /// <summary>Quorum is counted among nodes that hold the candidate's configuration entry rather than the configured voters (SafetyMutations.QuorumFromReachableNodes analogue).</summary>
    public const string QuorumOfAnyTwoNodes = "QuorumOfAnyTwoNodes";

    internal sealed class State
    {
        public int[] Term = new int[N];
        public int[] Vote = [-1, -1, -1, -1];
        public bool[] Leader = new bool[N];
        public int[][] Log = [[], [], [], []];   // entry = term * 16 + config (0 = noop)
        public int[] Commit = new int[N];
        public int[] Match = new int[N * N];
        public int[] LeaderOfTerm = new int[MaxTerm + 1];
        public bool TwoLeaders;
        public int[] Committed = new int[MaxLog];  // ghost: entry committed at index (0 = none; entries are never 0 because term >= 1)
        public string? CommitConflict;

        public State()
        {
            Array.Fill(LeaderOfTerm, -1);
        }

        public State Clone()
        {
            var t = new State
            {
                Term = (int[])Term.Clone(),
                Vote = (int[])Vote.Clone(),
                Leader = (bool[])Leader.Clone(),
                Log = Log.Select(l => (int[])l.Clone()).ToArray(),
                Commit = (int[])Commit.Clone(),
                Match = (int[])Match.Clone(),
                LeaderOfTerm = (int[])LeaderOfTerm.Clone(),
                TwoLeaders = TwoLeaders,
                Committed = (int[])Committed.Clone(),
                CommitConflict = CommitConflict,
            };
            return t;
        }
    }

    private static int EntryTerm(int e) => e / 16;
    private static int EntryConfig(int e) => e % 16;

    private static int ConfigOf(int[] log)
    {
        for (int i = log.Length - 1; i >= 0; i--)
        {
            if (EntryConfig(log[i]) != Noop)
                return EntryConfig(log[i]);
        }

        return C0;
    }

    private static int LastConfigIndex(int[] log)
    {
        for (int i = log.Length - 1; i >= 0; i--)
        {
            if (EntryConfig(log[i]) != Noop)
                return i + 1;
        }

        return 0;
    }

    private static int MajorityOf(int config) => ModHelpers.Bits(config) / 2 + 1;

    private static IEnumerable<int> Successors(int config, string variant)
    {
        if (config == C0)
        {
            yield return CAdd;
            yield return CRemove;
            if (variant == ReplaceTwoVotersAtOnce)
                yield return CFinal;
        }
        else if (config is CAdd or CRemove)
        {
            yield return CFinal;
        }
    }

    private static readonly string[] NodeNames = ["n0", "n1", "n2", "n3"];

    public override string TestId => "MOD-009";
    public override string Name => "MembershipTransitionModel";

    public override string Summary =>
        "Four nodes; voters start as {n0,n1,n2}. Configuration entries take effect on append. The leader may propose a " +
        "single-server change (add n3 or remove n0, then the other) only when its previous configuration entry and an entry " +
        "of its current term are committed. Elections use the candidate's latest configuration; delayed nodes keep old " +
        "configurations; AppendEntries replicate any prefix; leaders step down at any time.";

    public override SpecBounds Bounds { get; } = new(64, 4_000_000,
        ["nodes=4", "configurations: {n0,n1,n2} → {n0..n3} or {n1,n2} → {n1,n2,n3}", $"terms<={MaxTerm}", $"log length<={MaxLog} (noop per election)"]);

    public override IReadOnlyList<string> Properties { get; } =
    [
        "Election safety across configurations: at most one leader per term, even while delayed nodes use older configurations.",
        "Committed entries (including configuration entries) are never overwritten with different entries.",
    ];

    public override IReadOnlyList<string> Assumptions { get; } =
    [
        "Unreachable voters remain in the quorum denominator (RCV-01); only committed configuration transitions change voters.",
        "Terms, votes and logs are durable across restarts.",
    ];

    public override IReadOnlyList<string> Abstractions { get; } =
    [
        "Learner catch-up is modelled by allowing AppendEntries to any node (learners receive the log but are not counted).",
        "Atomic elections/appends as in MOD-001; failed candidacies (term bumps without a leader) and empty heartbeats are omitted (covered by MOD-001).",
        "Removed nodes keep their old state (they may still campaign using an old configuration).",
    ];

    public override IReadOnlyList<string> RefinementGaps { get; } =
    [
        "Actions 'Elect' and 'StepDown' have no named fault site (vote messages, ticks, crashes); they are exercised by partitions and crashes in CON/RSY tests.",
        "The control-group AddMember/promote pipeline (ARCHITECTURE §2.5) is not modelled beyond single-server voter changes.",
    ];

    public override IReadOnlyDictionary<string, IReadOnlyList<string>> ActionFaultSites { get; } = ModHelpers.Map(
        ("Elect", []),
        ("Append", [FaultSites.RaftAfterLocalAppend]),
        ("AdvanceCommit", [FaultSites.RaftAfterCommitBeforeApply]),
        ("ProposeConfig", [FaultSites.Reconfiguration, FaultSites.RaftBeforePropose]),
        ("StepDown", []));

    public override IReadOnlyList<BrokenVariant> BrokenVariants { get; } =
    [
        new(ReplaceTwoVotersAtOnce, "one change replaces two voters ({n0,n1,n2} → {n1,n2,n3}), so old and new majorities can be disjoint"),
        new(NoCommitBeforeNextChange, "a new configuration change is appended before the previous change and a current-term entry are committed"),
        new(QuorumOfAnyTwoNodes, "elections succeed with any two votes regardless of the configured voter count", nameof(SafetyMutations.QuorumFromReachableNodes)),
    ];

    protected override State Initial(string variant) => new();

    protected override IEnumerable<(string Action, State Next)> Next(State s, string variant)
    {
        for (int c = 0; c < N; c++)
        {
            int config = ConfigOf(s.Log[c]);
            if ((config & (1 << c)) == 0)
                continue; // only configured voters campaign

            int term = s.Term[c] + 1;
            if (term > MaxTerm)
                continue;
            int needed = variant == QuorumOfAnyTwoNodes ? 2 : MajorityOf(config);
            int universe = variant == QuorumOfAnyTwoNodes ? (1 << N) - 1 : config;
            for (int quorum = 0; quorum < 1 << N; quorum++)
            {
                if ((quorum & (1 << c)) == 0 || (quorum & ~universe) != 0 || ModHelpers.Bits(quorum) < needed)
                    continue;
                bool ok = true;
                for (int v = 0; v < N && ok; v++)
                {
                    if (v == c || (quorum & (1 << v)) == 0)
                        continue;
                    bool available = s.Term[v] < term || (s.Term[v] == term && (s.Vote[v] == -1 || s.Vote[v] == c));
                    ok = available && UpToDate(s.Log[c], s.Log[v]);
                }

                if (!ok)
                    continue;
                State t = s.Clone();
                for (int v = 0; v < N; v++)
                {
                    if ((quorum & (1 << v)) == 0)
                        continue;
                    t.Term[v] = term;
                    t.Vote[v] = c;
                    t.Leader[v] = false;
                }

                t.Leader[c] = true;
                if (t.Log[c].Length < MaxLog)
                    t.Log[c] = [.. t.Log[c], term * 16 + Noop];
                for (int f = 0; f < N; f++)
                    t.Match[c * N + f] = 0;
                t.Match[c * N + c] = t.Log[c].Length;
                if (t.LeaderOfTerm[term] != -1 && t.LeaderOfTerm[term] != c)
                    t.TwoLeaders = true;
                t.LeaderOfTerm[term] = c;
                yield return ($"Elect(n{c},T{term},{ModHelpers.Set(quorum, NodeNames)})", t);
            }
        }

        for (int l = 0; l < N; l++)
        {
            if (!s.Leader[l])
                continue;
            int config = ConfigOf(s.Log[l]);

            for (int f = 0; f < N; f++)
            {
                if (f == l)
                    continue;
                for (int k = 1; k <= s.Log[l].Length; k++)
                    yield return ($"Append(n{l}->n{f},prefix{k})", AppendEntries(s, l, f, k));
            }

            for (int idx = s.Log[l].Length; idx > s.Commit[l]; idx--)
            {
                int acks = 0;
                for (int f = 0; f < N; f++)
                {
                    if ((config & (1 << f)) == 0)
                        continue;
                    int m = f == l ? s.Log[l].Length : s.Match[l * N + f];
                    if (m >= idx)
                        acks++;
                }

                if (acks >= MajorityOf(config) && EntryTerm(s.Log[l][idx - 1]) == s.Term[l])
                {
                    State t = s.Clone();
                    t.Commit[l] = idx;
                    for (int i = 0; i < idx; i++)
                    {
                        if (t.Committed[i] == 0)
                            t.Committed[i] = t.Log[l][i];
                        else if (t.Committed[i] != t.Log[l][i])
                            t.CommitConflict ??= $"index {i + 1} committed as {Describe(t.Committed[i])} and later as {Describe(t.Log[l][i])}";
                    }

                    yield return ($"AdvanceCommit(n{l},{idx})", t);
                    break;
                }
            }

            if (s.Log[l].Length < MaxLog)
            {
                bool previousCommitted = LastConfigIndex(s.Log[l]) <= s.Commit[l];
                bool currentTermCommitted = s.Commit[l] > 0 && EntryTerm(s.Log[l][s.Commit[l] - 1]) == s.Term[l];
                bool allowed = variant == NoCommitBeforeNextChange || (previousCommitted && currentTermCommitted);
                if (allowed)
                {
                    foreach (int next in Successors(config, variant))
                    {
                        State t = s.Clone();
                        t.Log[l] = [.. s.Log[l], s.Term[l] * 16 + next];
                        t.Match[l * N + l] = t.Log[l].Length;
                        yield return ($"ProposeConfig(n{l},{ModHelpers.Set(next, NodeNames)})", t);
                    }
                }
            }

            {
                State t = s.Clone();
                t.Leader[l] = false;
                yield return ($"StepDown(n{l})", t);
            }
        }
    }

    private static string Describe(int entry) =>
        EntryConfig(entry) == Noop ? $"noop@T{EntryTerm(entry)}" : $"config{ModHelpers.Set(EntryConfig(entry), NodeNames)}@T{EntryTerm(entry)}";

    private static State AppendEntries(State s, int l, int f, int k)
    {
        State t = s.Clone();
        if (s.Term[l] < s.Term[f])
        {
            t.Term[l] = s.Term[f];
            t.Vote[l] = -1;
            t.Leader[l] = false;
            return t;
        }

        if (s.Term[f] < s.Term[l])
        {
            t.Term[f] = s.Term[l];
            t.Vote[f] = -1;
        }

        t.Leader[f] = false;
        var followerLog = s.Log[f].ToList();
        for (int i = 0; i < k; i++)
        {
            if (i < followerLog.Count)
            {
                if (followerLog[i] != s.Log[l][i])
                {
                    followerLog.RemoveRange(i, followerLog.Count - i);
                    followerLog.Add(s.Log[l][i]);
                }
            }
            else
            {
                followerLog.Add(s.Log[l][i]);
            }
        }

        t.Log[f] = followerLog.ToArray();
        t.Match[l * N + f] = Math.Max(t.Match[l * N + f], k);
        int propagated = Math.Min(s.Commit[l], k);
        if (propagated > t.Commit[f])
            t.Commit[f] = propagated;
        return t;
    }

    private static bool UpToDate(int[] candidate, int[] voter)
    {
        int cLast = candidate.Length == 0 ? 0 : EntryTerm(candidate[^1]);
        int vLast = voter.Length == 0 ? 0 : EntryTerm(voter[^1]);
        return cLast > vLast || (cLast == vLast && candidate.Length >= voter.Length);
    }

    protected override string? Invariant(State s, string variant)
    {
        if (s.TwoLeaders)
            return "election safety: two leaders elected in one term across configurations";
        for (int a = 0; a < N; a++)
        {
            for (int b = a + 1; b < N; b++)
            {
                if (s.Leader[a] && s.Leader[b] && s.Term[a] == s.Term[b])
                    return $"election safety: n{a} and n{b} lead term {s.Term[a]}";
            }
        }

        if (s.CommitConflict is not null)
            return "committed entry overwritten: " + s.CommitConflict;
        for (int n = 0; n < N; n++)
        {
            for (int i = 0; i < s.Commit[n]; i++)
            {
                if (i >= s.Log[n].Length || s.Log[n][i] != s.Committed[i])
                    return $"n{n} treats index {i + 1} as committed but holds a different entry";
            }
        }

        return null;
    }

    protected override string Key(State s)
    {
        var sb = new StringBuilder(128);
        for (int n = 0; n < N; n++)
        {
            sb.Append('T').Append(s.Term[n]).Append('V').Append(s.Vote[n]).Append(s.Leader[n] ? 'L' : 'F').Append('[');
            ModHelpers.AppendInts(sb, s.Log[n]);
            sb.Append(']').Append('C').Append(s.Commit[n]);
            if (s.Leader[n])
            {
                sb.Append('M');
                ModHelpers.AppendInts(sb, s.Match.AsSpan(n * N, N));
            }

            sb.Append('|');
        }

        ModHelpers.AppendInts(sb, s.LeaderOfTerm);
        sb.Append(s.TwoLeaders ? '!' : '.');
        ModHelpers.AppendInts(sb, s.Committed);
        sb.Append(s.CommitConflict is null ? '.' : '!');
        return sb.ToString();
    }
}
