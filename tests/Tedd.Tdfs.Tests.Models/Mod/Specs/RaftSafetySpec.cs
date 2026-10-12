using System.Text;

namespace Tedd.Tdfs.Tests.Models.Mod;

/// <summary>
/// MOD-001: abstract Raft (atomic elections and AppendEntries) over 3 servers. Checks election safety, log matching,
/// leader completeness and state-machine safety under step-downs (crashes), message loss (any action may never happen),
/// duplication (actions repeat) and arbitrary partitions (any majority quorum may be chosen).
/// </summary>
internal sealed class RaftSafetySpec : ProtocolSpec<RaftSafetySpec.State>
{
    private const int N = 3;
    private const int MaxTerm = 3;
    private const int MaxLog = 2;
    private const int Majority = N / 2 + 1;

    /// <summary>Votes are granted without the "candidate log at least as up to date" check (Raft §5.4.1 removed).</summary>
    public const string VoteWithoutUpToDateCheck = "VoteWithoutLogUpToDateCheck";

    /// <summary>A vote may be granted twice in the same term (votedFor not persisted / ignored).</summary>
    public const string DoubleVoteInTerm = "DoubleVoteInSameTerm";

    internal sealed class State
    {
        public int[] Term = new int[N];
        public int[] Vote = [-1, -1, -1];
        public bool[] Leader = new bool[N];
        public int[][] Log = [[], [], []];
        public int[] Commit = new int[N];
        public int[] Match = new int[N * N];              // Match[l*N+f], meaningful while l is leader
        public int[] LeaderOfTerm = [-1, -1, -1, -1];    // ghost, index = term
        public bool TwoLeadersInTerm;                     // ghost
        public int[] CommittedEntry = new int[MaxLog];    // ghost: term of the entry committed at index (0 = none)
        public int[] CommittedInTerm = new int[MaxLog];   // ghost: leader term in which it became committed
        public string? CommitConflict;                    // ghost

        public State Clone() => new()
        {
            Term = (int[])Term.Clone(),
            Vote = (int[])Vote.Clone(),
            Leader = (bool[])Leader.Clone(),
            Log = Log.Select(l => (int[])l.Clone()).ToArray(),
            Commit = (int[])Commit.Clone(),
            Match = (int[])Match.Clone(),
            LeaderOfTerm = (int[])LeaderOfTerm.Clone(),
            TwoLeadersInTerm = TwoLeadersInTerm,
            CommittedEntry = (int[])CommittedEntry.Clone(),
            CommittedInTerm = (int[])CommittedInTerm.Clone(),
            CommitConflict = CommitConflict,
        };
    }

    public override string TestId => "MOD-001";
    public override string Name => "ConsensusSafetyModel";

    public override string Summary =>
        "Raft with PreVote/CheckQuorum abstracted away: atomic elections over any majority quorum, atomic AppendEntries of any " +
        "log prefix (with conflict truncation), leader commit by matchIndex majority restricted to current-term entries, and " +
        "leader step-down (crash/restart with persistent term/vote/log). Ghost variables record every elected leader per term " +
        "and every committed (index, term).";

    public override SpecBounds Bounds { get; } = new(64, 3_000_000,
        ["servers=3 (majority 2)", $"terms=0..{MaxTerm}", $"log length<={MaxLog}", "one client value per entry (entry = its term)"]);

    public override IReadOnlyList<string> Properties { get; } =
    [
        "Election safety: at most one leader is ever elected per term.",
        "Log matching: entries with equal index and term have identical prefixes.",
        "State-machine safety: no two different entries are ever committed at the same index; every server's committed prefix equals the committed history.",
        "Leader completeness: an entry committed in term t is present in the log of every leader of a term > t.",
    ];

    public override IReadOnlyList<string> Assumptions { get; } =
    [
        "Crash-stop/restart with durable term, vote and log (persisted before votes/acks are sent, ARCHITECTURE §4).",
        "Messages may be lost, duplicated or delayed arbitrarily (any enabled action may be taken or never taken).",
        "No Byzantine behaviour; configuration is fixed (membership changes are covered by MOD-009).",
    ];

    public override IReadOnlyList<string> Abstractions { get; } =
    [
        "Elections and AppendEntries are atomic steps; vote and append messages are not represented individually.",
        "PreVote and CheckQuorum are omitted (they only restrict behaviours, so safety of the larger behaviour set implies safety with them).",
        "Log compaction/InstallSnapshot is not modelled (log length <= 2).",
        "matchIndex only grows; a leader's acknowledgement bookkeeping is reset on election.",
    ];

    public override IReadOnlyList<string> RefinementGaps { get; } =
    [
        "Refinement of RaftNode (Tedd.Tdfs.Consensus) to this model is not proved; MOD-001 additionally drives the real RaftNode through RaftSimulation (CheckSafety) with many seeds and partitions.",
        "Actions 'Timeout', 'Elect' and 'StepDown' are driven by election ticks, vote messages and crashes, not by a named fault site (FaultSites has no vote site); they are exercised with FaultAction.CrashNode at any site, TestNetwork partitions and election-timer jitter in simulation.",
        "Durable persistence ordering (hard state before replies) is assumed, not modelled; it is covered by CON-004/DSK tests.",
    ];

    public override IReadOnlyDictionary<string, IReadOnlyList<string>> ActionFaultSites { get; } = ModHelpers.Map(
        ("Timeout", []),
        ("Elect", []),
        ("ClientRequest", [FaultSites.RaftBeforePropose, FaultSites.RaftAfterLocalAppend]),
        ("Append", [FaultSites.RaftAfterLocalAppend]),
        ("AdvanceCommit", [FaultSites.RaftAfterCommitBeforeApply, FaultSites.ApplyAfterBeforeReply]),
        ("StepDown", []));

    public override IReadOnlyList<BrokenVariant> BrokenVariants { get; } =
    [
        new(VoteWithoutUpToDateCheck, "voters grant votes to candidates whose log is less up to date (election restriction removed)"),
        new(DoubleVoteInTerm, "a voter may vote for a second candidate in a term it already voted in"),
    ];

    protected override State Initial(string variant) => new();

    protected override IEnumerable<(string Action, State Next)> Next(State s, string variant)
    {
        for (int c = 0; c < N; c++)
        {
            // Failed candidacy: term advances, self-vote, no leader.
            if (s.Term[c] < MaxTerm)
            {
                State t = s.Clone();
                t.Term[c]++;
                t.Vote[c] = c;
                t.Leader[c] = false;
                yield return ($"Timeout(n{c})", t);
            }

            // Successful election in term Term[c]+1 by any majority quorum containing c.
            int term = s.Term[c] + 1;
            if (term <= MaxTerm)
            {
                for (int mask = 0; mask < 1 << N; mask++)
                {
                    if ((mask & (1 << c)) == 0 || ModHelpers.Bits(mask) < Majority)
                        continue;
                    bool ok = true;
                    for (int v = 0; v < N && ok; v++)
                    {
                        if (v == c || (mask & (1 << v)) == 0)
                            continue;
                        bool voteAvailable = s.Term[v] < term || (s.Term[v] == term && (s.Vote[v] == -1 || s.Vote[v] == c));
                        if (variant == DoubleVoteInTerm)
                            voteAvailable = s.Term[v] <= term;
                        bool upToDate = variant == VoteWithoutUpToDateCheck || UpToDate(s.Log[c], s.Log[v]);
                        ok = voteAvailable && upToDate;
                    }

                    if (!ok)
                        continue;
                    State t = s.Clone();
                    for (int v = 0; v < N; v++)
                    {
                        if ((mask & (1 << v)) == 0)
                            continue;
                        t.Term[v] = term;
                        t.Vote[v] = c;
                        t.Leader[v] = false;
                    }

                    t.Leader[c] = true;
                    for (int f = 0; f < N; f++)
                        t.Match[c * N + f] = 0;
                    t.Match[c * N + c] = t.Log[c].Length;
                    if (t.LeaderOfTerm[term] != -1 && t.LeaderOfTerm[term] != c)
                        t.TwoLeadersInTerm = true;
                    t.LeaderOfTerm[term] = c;
                    yield return ($"Elect(n{c},T{term},{ModHelpers.Set(mask, ["n0", "n1", "n2"])})", t);
                }
            }
        }

        for (int l = 0; l < N; l++)
        {
            if (!s.Leader[l])
                continue;

            if (s.Log[l].Length < MaxLog)
            {
                State t = s.Clone();
                t.Log[l] = [.. s.Log[l], s.Term[l]];
                t.Match[l * N + l] = t.Log[l].Length;
                yield return ($"ClientRequest(n{l},T{s.Term[l]})", t);
            }

            for (int f = 0; f < N; f++)
            {
                if (f == l)
                    continue;
                for (int k = 0; k <= s.Log[l].Length; k++)
                    yield return ($"Append(n{l}->n{f},prefix{k})", AppendEntries(s, l, f, k));
            }

            int newCommit = -1;
            for (int idx = s.Log[l].Length; idx > s.Commit[l]; idx--)
            {
                int acks = 0;
                for (int f = 0; f < N; f++)
                {
                    int m = f == l ? s.Log[l].Length : s.Match[l * N + f];
                    if (m >= idx)
                        acks++;
                }

                if (acks >= Majority && s.Log[l][idx - 1] == s.Term[l])
                {
                    newCommit = idx;
                    break;
                }
            }

            if (newCommit > 0)
            {
                State t = s.Clone();
                t.Commit[l] = newCommit;
                RecordCommitted(t, l, newCommit);
                yield return ($"AdvanceCommit(n{l},{newCommit})", t);
            }

            {
                State t = s.Clone();
                t.Leader[l] = false;
                yield return ($"StepDown(n{l})", t);
            }
        }
    }

    private static State AppendEntries(State s, int l, int f, int k)
    {
        State t = s.Clone();
        if (s.Term[l] < s.Term[f])
        {
            // Response carries a higher term: the stale leader steps down.
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
        int[] leaderLog = s.Log[l];
        var followerLog = s.Log[f].ToList();
        for (int i = 0; i < k; i++)
        {
            if (i < followerLog.Count)
            {
                if (followerLog[i] != leaderLog[i])
                {
                    followerLog.RemoveRange(i, followerLog.Count - i);
                    followerLog.Add(leaderLog[i]);
                }
            }
            else
            {
                followerLog.Add(leaderLog[i]);
            }
        }

        t.Log[f] = followerLog.ToArray();
        t.Match[l * N + f] = Math.Max(t.Match[l * N + f], k);
        int propagated = Math.Min(s.Commit[l], k);
        if (propagated > t.Commit[f])
            t.Commit[f] = propagated;
        return t;
    }

    private static void RecordCommitted(State t, int l, int upTo)
    {
        for (int i = 0; i < upTo; i++)
        {
            int entry = t.Log[l][i];
            if (t.CommittedEntry[i] == 0)
            {
                t.CommittedEntry[i] = entry;
                t.CommittedInTerm[i] = t.Term[l];
            }
            else if (t.CommittedEntry[i] != entry)
            {
                t.CommitConflict ??= $"index {i + 1} committed with term {t.CommittedEntry[i]} and later with term {entry} by n{l}";
            }
        }
    }

    private static bool UpToDate(int[] candidate, int[] voter)
    {
        int cLast = candidate.Length == 0 ? 0 : candidate[^1];
        int vLast = voter.Length == 0 ? 0 : voter[^1];
        return cLast > vLast || (cLast == vLast && candidate.Length >= voter.Length);
    }

    protected override string? Invariant(State s, string variant)
    {
        if (s.TwoLeadersInTerm)
            return "election safety: two leaders elected in the same term";
        for (int a = 0; a < N; a++)
        {
            for (int b = a + 1; b < N; b++)
            {
                if (s.Leader[a] && s.Leader[b] && s.Term[a] == s.Term[b])
                    return $"election safety: n{a} and n{b} are both leaders of term {s.Term[a]}";
                int common = Math.Min(s.Log[a].Length, s.Log[b].Length);
                for (int i = 0; i < common; i++)
                {
                    if (s.Log[a][i] == s.Log[b][i] && !s.Log[a].AsSpan(0, i + 1).SequenceEqual(s.Log[b].AsSpan(0, i + 1)))
                        return $"log matching: n{a} and n{b} agree at index {i + 1} but differ before it";
                }
            }
        }

        if (s.CommitConflict is not null)
            return "state-machine safety: " + s.CommitConflict;
        for (int n = 0; n < N; n++)
        {
            for (int i = 0; i < s.Commit[n]; i++)
            {
                if (i >= s.Log[n].Length || s.Log[n][i] != s.CommittedEntry[i])
                    return $"state-machine safety: n{n} considers index {i + 1} committed but holds a different entry";
            }
        }

        for (int l = 0; l < N; l++)
        {
            if (!s.Leader[l])
                continue;
            for (int i = 0; i < MaxLog; i++)
            {
                if (s.CommittedEntry[i] != 0 && s.CommittedInTerm[i] < s.Term[l]
                    && (i >= s.Log[l].Length || s.Log[l][i] != s.CommittedEntry[i]))
                    return $"leader completeness: leader n{l} of term {s.Term[l]} lacks entry {i + 1} committed in term {s.CommittedInTerm[i]}";
            }
        }

        return null;
    }

    protected override string Key(State s)
    {
        var sb = new StringBuilder(96);
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

        sb.Append('G');
        ModHelpers.AppendInts(sb, s.LeaderOfTerm);
        sb.Append(s.TwoLeadersInTerm ? '!' : '.');
        ModHelpers.AppendInts(sb, s.CommittedEntry);
        sb.Append('/');
        ModHelpers.AppendInts(sb, s.CommittedInTerm);
        sb.Append(s.CommitConflict is null ? '.' : '!');
        return sb.ToString();
    }
}
