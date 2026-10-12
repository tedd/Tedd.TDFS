using System.Collections.Immutable;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using Tedd.Tdfs.Consensus;

namespace Tedd.Tdfs.Tests.Models.Con;

/// <summary>
/// Bounds of one bounded-exhaustive election model (CON-001). Every budget is consumed by an environment action, so the
/// reachable state space is finite and the exploration can complete: elections (<see cref="Timeouts"/>), leader
/// heartbeat rounds, client proposals, message drops, message duplications, node crashes and single-server membership
/// changes (<see cref="ConfChanges"/>: promote a learner or remove a voter, proposed by whichever node currently leads,
/// so elections interleave with configuration transitions). Restarts are not budgeted separately: a node can only
/// restart after a budgeted crash.
/// </summary>
internal sealed record RaftModelBounds(
    int Voters,
    bool PreVote,
    bool CheckQuorum,
    int Timeouts,
    int Heartbeats,
    int Proposals,
    int Drops,
    int Duplicates,
    int Crashes,
    int Learners = 0,
    int ConfChanges = 0)
{
    /// <summary>Raft timing used by every model node: tiny tick counts keep macro actions short.</summary>
    public const int ElectionTicksMin = 3;

    /// <summary>Upper bound of the randomized election timeout.</summary>
    public const int ElectionTicksMax = 4;

    /// <summary>Heartbeat interval in ticks.</summary>
    public const int HeartbeatTicks = 1;

    public override string ToString() =>
        string.Create(CultureInfo.InvariantCulture,
            $"voters={Voters} learners={Learners} preVote={PreVote} checkQuorum={CheckQuorum} timeouts<={Timeouts} heartbeats<={Heartbeats} proposals<={Proposals} drops<={Drops} duplicates<={Duplicates} crashes<={Crashes} confChanges<={ConfChanges}");
}

/// <summary>
/// A state of the CON-001 model as seen by <see cref="ModelChecker"/>: the schedule of environment actions that
/// reaches it (replayed against fresh production <see cref="RaftNode"/> cores), its canonical key and the first
/// invariant violation observed along the schedule (null when none).
/// </summary>
internal sealed record RaftModelState(ImmutableArray<string> Schedule, string Key, string? Violation);

/// <summary>
/// Deterministic host for a set of production <see cref="RaftNode"/> cores used for bounded-exhaustive exploration
/// (CON-001). The host behaves like a correct <c>RaftGroupHost</c>: it persists hard state and entries before it
/// releases the ready's messages, applies committed entries, and on restart rebuilds the core from the persisted hard
/// state and log only. The network is an explicit multiset of in-flight messages; the explorer chooses which message
/// is delivered, dropped or duplicated next, so every interleaving of deliveries is a separate schedule.
/// </summary>
/// <remarks>
/// The canonical key of a state is the per-node input history (every tick, step, proposal, crash and restart a core
/// received, in order) plus the in-flight multiset and the consumed budgets. A <see cref="RaftNode"/> is a
/// deterministic function of its constructor arguments (including its per-life seeded random source) and its input
/// sequence, so two schedules with equal keys are behaviourally identical; merging them is sound and removes only
/// interleavings that differ in the relative order of events at different nodes.
/// The invariant trackers (leaders per term, committed entries, vote grants) are order-independent unions/minima over
/// per-node outputs, so they are also functions of the key.
/// </remarks>
internal sealed class RaftModelCluster
{
    private readonly RaftModelBounds _bounds;
    private readonly ulong _seed;
    private readonly NodeId[] _ids;
    private readonly Dictionary<NodeId, int> _indexOf = [];
    private readonly RaftConfState _conf;
    private readonly RaftNode?[] _nodes;
    private readonly InMemoryRaftLog[] _logs;
    private readonly RaftHardState[] _hard;
    private readonly int[] _lives;
    private readonly List<string>[] _inputs;
    private readonly List<byte[]>[] _applied;
    private readonly List<(string Key, RaftMessage Message)> _inflight = [];
    private readonly SortedDictionary<ulong, SortedSet<int>> _leadersByTerm = [];
    private readonly SortedDictionary<ulong, (ulong Term, string Data, ulong ObservedTerm)> _committed = [];
    private readonly Dictionary<(int Voter, ulong Term), int> _votes = [];
    private int _timeouts;
    private int _heartbeats;
    private int _proposals;
    private int _drops;
    private int _duplicates;
    private int _crashes;
    private int _confChanges;
    private string? _violation;

    public RaftModelCluster(RaftModelBounds bounds, ulong seed)
    {
        _bounds = bounds;
        _seed = seed;
        int n = bounds.Voters + bounds.Learners;
        _ids = new NodeId[n];
        for (int i = 0; i < n; i++)
        {
            var bytes = new byte[16];
            bytes[0] = (byte)(i + 1);
            bytes[15] = 0xC0;
            _ids[i] = new NodeId(new Guid(bytes));
            _indexOf[_ids[i]] = i;
        }

        // The bootstrap configuration (the "snapshot at index 0"). A correct host persists configuration only through the
        // log and snapshot metadata (IRaftStorage), so a restarted core receives this bootstrap configuration plus its
        // persisted log, from which the configuration in effect (changes are effective on append) is re-derived.
        _conf = new RaftConfState(_ids.Take(bounds.Voters).ToArray(), _ids.Skip(bounds.Voters).ToArray());
        _nodes = new RaftNode?[n];
        _logs = new InMemoryRaftLog[n];
        _hard = new RaftHardState[n];
        _lives = new int[n];
        _inputs = new List<string>[n];
        _applied = new List<byte[]>[n];
        for (int i = 0; i < n; i++)
        {
            _logs[i] = new InMemoryRaftLog();
            _hard[i] = new RaftHardState(0, null, 0);
            _inputs[i] = [];
            _applied[i] = [];
            _nodes[i] = CreateCore(i);
        }
    }

    /// <summary>True when some running core currently believes it is the leader.</summary>
    public bool AnyLeader => _nodes.Any(n => n is not null && n.Role == RaftRole.Leader);

    /// <summary>First invariant violation observed, or null.</summary>
    public string? Violation => _violation;

    /// <summary>Number of membership changes a leader accepted along the schedule (vacuity check of configuration rows).</summary>
    public int AcceptedConfChanges { get; private set; }

    /// <summary>Replays <paramref name="schedule"/> from the initial state.</summary>
    public static RaftModelCluster Replay(RaftModelBounds bounds, ulong seed, IEnumerable<string> schedule)
    {
        var cluster = new RaftModelCluster(bounds, seed);
        foreach (string action in schedule)
        {
            cluster.Apply(action);
            if (cluster._violation is not null)
                break;
        }

        return cluster;
    }

    /// <summary>The initial model state for <see cref="ModelChecker.Explore{TState}"/>.</summary>
    public static RaftModelState Initial(RaftModelBounds bounds, ulong seed)
    {
        var cluster = new RaftModelCluster(bounds, seed);
        return new RaftModelState([], cluster.ComputeKey(), cluster.CheckInvariants());
    }

    /// <summary>Successor function for <see cref="ModelChecker.Explore{TState}"/>.</summary>
    public static IEnumerable<(string Action, RaftModelState Next)> Successors(RaftModelBounds bounds, ulong seed, RaftModelState state)
    {
        if (state.Violation is not null)
            yield break;
        RaftModelCluster current = Replay(bounds, seed, state.Schedule);
        foreach (string action in current.EnabledActions())
        {
            ImmutableArray<string> schedule = state.Schedule.Add(action);
            RaftModelCluster successor = Replay(bounds, seed, schedule);
            yield return (action, new RaftModelState(schedule, successor.ComputeKey(), successor._violation ?? successor.CheckInvariants()));
        }
    }

    /// <summary>Every environment action enabled in this state, in a canonical order.</summary>
    public IReadOnlyList<string> EnabledActions()
    {
        var actions = new List<string>();
        var distinct = new SortedSet<string>(StringComparer.Ordinal);
        foreach ((string key, RaftMessage message) in _inflight)
        {
            if (!distinct.Add(key))
                continue;
            int to = _indexOf[message.To];
            if (_nodes[to] is not null)
            {
                actions.Add("deliver " + key);
                if (_duplicates < _bounds.Duplicates)
                    actions.Add("dup " + key);
            }

            if (_drops < _bounds.Drops)
                actions.Add("drop " + key);
        }

        for (int i = 0; i < _nodes.Length; i++)
        {
            RaftNode? node = _nodes[i];
            if (node is null)
            {
                actions.Add("restart n" + i.ToString(CultureInfo.InvariantCulture));
                continue;
            }

            string name = "n" + i.ToString(CultureInfo.InvariantCulture);
            bool leader = node.Role == RaftRole.Leader;
            if (!leader && node.Role != RaftRole.Learner && _timeouts < _bounds.Timeouts)
                actions.Add("timeout " + name);
            if (leader && _heartbeats < _bounds.Heartbeats)
                actions.Add("heartbeat " + name);
            if (leader && _proposals < _bounds.Proposals)
                actions.Add("propose " + name);
            if (leader && _confChanges < _bounds.ConfChanges)
            {
                RaftConfState conf = node.ConfState;
                foreach (NodeId learner in conf.Learners.OrderBy(id => _indexOf[id]))
                    actions.Add("promote " + name + " " + Name(learner));
                foreach (NodeId voter in conf.Voters.Where(id => id != _ids[i]).OrderBy(id => _indexOf[id]))
                    actions.Add("remove " + name + " " + Name(voter));
            }

            if (_crashes < _bounds.Crashes)
                actions.Add("crash " + name);
        }

        return actions;
    }

    /// <summary>Applies one environment action.</summary>
    public void Apply(string action)
    {
        int space = action.IndexOf(' ', StringComparison.Ordinal);
        string verb = action[..space];
        string arg = action[(space + 1)..];
        switch (verb)
        {
            case "deliver":
            case "dup":
            case "drop":
            {
                int position = _inflight.FindIndex(m => string.Equals(m.Key, arg, StringComparison.Ordinal));
                if (position < 0)
                    throw new InvalidOperationException($"Message '{arg}' is not in flight.");
                (string key, RaftMessage message) = _inflight[position];
                if (verb == "drop")
                {
                    _inflight.RemoveAt(position);
                    _drops++;
                    return;
                }

                if (verb == "dup")
                    _duplicates++;
                else
                    _inflight.RemoveAt(position);
                int to = _indexOf[message.To];
                RaftNode node = _nodes[to] ?? throw new InvalidOperationException($"Recipient n{to} is crashed.");
                _inputs[to].Add("step " + key);
                node.Step(message);
                Drain(to);
                return;
            }

            case "timeout":
            {
                int i = ParseNode(arg);
                RaftNode node = _nodes[i] ?? throw new InvalidOperationException($"n{i} is crashed.");
                _timeouts++;
                int maxTicks = (RaftModelBounds.ElectionTicksMax * 2) + 2;
                int ticks = 0;
                RaftRole before = node.Role;
                ulong termBefore = node.Term;
                while (ticks < maxTicks)
                {
                    ticks++;
                    node.Tick();
                    bool campaigning = node.Role != before || node.Term != termBefore || HasOutgoingVoteRequest(node);
                    Drain(i);
                    if (campaigning)
                        break;
                }

                _inputs[i].Add("tick*" + ticks.ToString(CultureInfo.InvariantCulture));
                return;
            }

            case "heartbeat":
            {
                int i = ParseNode(arg);
                RaftNode node = _nodes[i] ?? throw new InvalidOperationException($"n{i} is crashed.");
                _heartbeats++;
                for (int t = 0; t < RaftModelBounds.HeartbeatTicks; t++)
                {
                    node.Tick();
                    Drain(i);
                }

                _inputs[i].Add("tick*" + RaftModelBounds.HeartbeatTicks.ToString(CultureInfo.InvariantCulture));
                return;
            }

            case "propose":
            {
                int i = ParseNode(arg);
                RaftNode node = _nodes[i] ?? throw new InvalidOperationException($"n{i} is crashed.");
                string payload = "p" + _proposals.ToString(CultureInfo.InvariantCulture);
                _proposals++;
                bool accepted = node.TryPropose(Encoding.ASCII.GetBytes(payload), out _);
                _inputs[i].Add("propose " + payload + (accepted ? "" : " (rejected)"));
                Drain(i);
                return;
            }

            case "promote":
            case "remove":
            {
                // "<verb> <leader> <target>": the leader proposes one single-server membership change.
                string[] parts = arg.Split(' ');
                int i = ParseNode(parts[0]);
                int target = ParseNode(parts[1]);
                RaftNode node = _nodes[i] ?? throw new InvalidOperationException($"n{i} is crashed.");
                _confChanges++;
                var change = new RaftConfChange(verb == "promote" ? RaftConfChangeKind.PromoteLearner : RaftConfChangeKind.RemoveNode, _ids[target]);
                bool accepted = node.TryProposeConfChange(change, out _);
                if (accepted)
                    AcceptedConfChanges++;
                _inputs[i].Add(verb + " " + parts[1] + (accepted ? "" : " (rejected)"));
                Drain(i);
                return;
            }

            case "crash":
            {
                int i = ParseNode(arg);
                if (_nodes[i] is null)
                    throw new InvalidOperationException($"n{i} is already crashed.");
                _crashes++;
                _nodes[i] = null;
                _applied[i] = [];
                _inputs[i].Add("crash");
                return;
            }

            case "restart":
            {
                int i = ParseNode(arg);
                if (_nodes[i] is not null)
                    throw new InvalidOperationException($"n{i} is running.");
                _lives[i]++;
                _inputs[i].Add("restart");
                _nodes[i] = CreateCore(i);
                Drain(i);
                return;
            }

            default:
                throw new InvalidOperationException($"Unknown action '{action}'.");
        }
    }

    /// <summary>Canonical key (SHA-256 of the per-node input histories, in-flight multiset and consumed budgets).</summary>
    public string ComputeKey()
    {
        var sb = new StringBuilder();
        for (int i = 0; i < _inputs.Length; i++)
        {
            sb.Append('n').Append(i).Append('[');
            foreach (string input in _inputs[i])
                sb.Append(input).Append('\u001f');
            sb.Append(']').Append('\n');
        }

        string[] inflight = _inflight.Select(m => m.Key).ToArray();
        Array.Sort(inflight, StringComparer.Ordinal);
        sb.Append("inflight[");
        foreach (string key in inflight)
            sb.Append(key).Append('\u001f');
        sb.Append(']');
        sb.Append(CultureInfo.InvariantCulture, $"budget[{_timeouts},{_heartbeats},{_proposals},{_drops},{_duplicates},{_crashes},{_confChanges}]");
        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(sb.ToString())));
    }

    /// <summary>
    /// Checks the Raft safety properties on the current state: election safety (at most one leader per term, over the
    /// whole history), single vote per voter and term, log matching, committed-entry stability, leader completeness and
    /// state-machine safety (applied sequences are prefix-consistent).
    /// </summary>
    public string? CheckInvariants()
    {
        if (_violation is not null)
            return _violation;

        foreach ((ulong term, SortedSet<int> leaders) in _leadersByTerm)
        {
            if (leaders.Count > 1)
                return $"election safety: term {term} had leaders {string.Join(", ", leaders.Select(l => "n" + l))}";
        }

        for (int a = 0; a < _logs.Length; a++)
        {
            for (int b = a + 1; b < _logs.Length; b++)
            {
                string? mismatch = LogMatching(a, b);
                if (mismatch is not null)
                    return mismatch;
            }
        }

        for (int i = 0; i < _nodes.Length; i++)
        {
            RaftNode? node = _nodes[i];
            if (node is null || node.Role != RaftRole.Leader)
                continue;
            foreach ((ulong index, (ulong term, string data, ulong observedTerm)) in _committed)
            {
                if (node.Term < observedTerm)
                    continue;
                if (!_logs[i].TryGetTerm(index, out ulong leaderTerm) || leaderTerm != term)
                    return $"leader completeness: leader n{i} (term {node.Term}) lacks entry {index}@{term} ({data}) committed by term {observedTerm}";
            }
        }

        for (int a = 0; a < _applied.Length; a++)
        {
            for (int b = a + 1; b < _applied.Length; b++)
            {
                int common = Math.Min(_applied[a].Count, _applied[b].Count);
                for (int k = 0; k < common; k++)
                {
                    if (!_applied[a][k].AsSpan().SequenceEqual(_applied[b][k]))
                        return $"state-machine safety: n{a} and n{b} applied different commands at position {k}";
                }
            }
        }

        return null;
    }

    private RaftNode CreateCore(int i)
    {
        var config = new RaftConfig(_ids[i], RaftModelBounds.ElectionTicksMin, RaftModelBounds.ElectionTicksMax, RaftModelBounds.HeartbeatTicks,
            _bounds.PreVote, _bounds.CheckQuorum, MaxEntriesPerAppend: 16, MaxInflightAppends: 4);
        var random = new SeededRandom(SeededRandom.DeriveSeed(_seed, string.Create(CultureInfo.InvariantCulture, $"n{i}/life{_lives[i]}")));
        return new RaftNode(config, _hard[i], _conf, _logs[i], random);
    }

    private void Drain(int i)
    {
        RaftNode node = _nodes[i] ?? throw new InvalidOperationException($"n{i} is crashed.");
        int guard = 0;
        while (node.HasReady())
        {
            if (++guard > 1000)
            {
                _violation ??= $"liveness of the host loop: n{i} kept producing ready work";
                return;
            }

            RaftReady ready = node.GetReady();

            // Persist first (hard state + entries), exactly as RaftGroupHost must (ARCHITECTURE §4).
            if (ready.SnapshotToInstall is not null)
                _logs[i].ApplySnapshot(ready.SnapshotToInstall);
            if (ready.EntriesToPersist.Count > 0)
                _logs[i].Append(ready.EntriesToPersist);
            if (ready.HardState is { } hard)
            {
                if (hard.Term < _hard[i].Term)
                    _violation ??= $"n{i} persisted a term regression {_hard[i].Term} -> {hard.Term}";
                if (hard.Term == _hard[i].Term && _hard[i].Vote is { } oldVote && hard.Vote is { } newVote && oldVote != newVote)
                    _violation ??= $"n{i} changed its persisted vote in term {hard.Term} from {Name(oldVote)} to {Name(newVote)}";
                _hard[i] = hard;
            }

            // Then release the messages.
            foreach (RaftMessage message in ready.Messages)
            {
                if (message is VoteResponse { PreVote: false, Reject: false } grant)
                {
                    int candidate = _indexOf[grant.To];
                    if (_hard[i].Term < grant.Term || _hard[i].Vote != grant.To)
                        _violation ??= $"n{i} sent a vote for {Name(grant.To)} in term {grant.Term} before persisting it (persisted term {_hard[i].Term}, vote {(_hard[i].Vote is { } v ? Name(v) : "none")})";
                    if (_votes.TryGetValue((i, grant.Term), out int earlier) && earlier != candidate)
                        _violation ??= $"n{i} granted votes to n{earlier} and n{candidate} in term {grant.Term}";
                    _votes[(i, grant.Term)] = candidate;
                }

                if (message is AppendResponse { Reject: false } ack && ack.MatchIndex > _logs[i].LastIndex)
                    _violation ??= $"n{i} acknowledged index {ack.MatchIndex} beyond its persisted last index {_logs[i].LastIndex}";

                _inflight.Add((Render(message), message));
            }

            // Apply.
            foreach (RaftEntry entry in ready.CommittedEntries)
            {
                if (entry.Kind == RaftEntryKind.Normal && entry.Data.Length > 0)
                    _applied[i].Add(entry.Data.ToArray());
            }

            if (ready.Role == RaftRole.Leader)
            {
                if (!_leadersByTerm.TryGetValue(node.Term, out SortedSet<int>? leaders))
                    _leadersByTerm[node.Term] = leaders = [];
                leaders.Add(i);
            }

            node.Advance(ready);
            RecordCommitted(i, node);
        }

        if (node.Role == RaftRole.Leader)
        {
            if (!_leadersByTerm.TryGetValue(node.Term, out SortedSet<int>? leaders))
                _leadersByTerm[node.Term] = leaders = [];
            leaders.Add(i);
        }
    }

    private void RecordCommitted(int i, RaftNode node)
    {
        ulong commit = node.CommitIndex;
        if (commit > _logs[i].LastIndex)
        {
            _violation ??= $"n{i} reports commit index {commit} beyond its persisted last index {_logs[i].LastIndex}";
            return;
        }

        for (ulong index = _logs[i].FirstIndex; index <= commit; index++)
        {
            RaftEntry entry = _logs[i].EntryAt(index);
            string data = Convert.ToHexString(entry.Data.Span) + "/" + entry.Kind;
            if (_committed.TryGetValue(index, out var known))
            {
                if (known.Term != entry.Term || !string.Equals(known.Data, data, StringComparison.Ordinal))
                {
                    _violation ??= $"committed entry {index} changed: {known.Term}:{known.Data} vs n{i} {entry.Term}:{data}";
                    return;
                }

                if (node.Term < known.ObservedTerm)
                    _committed[index] = known with { ObservedTerm = node.Term };
            }
            else
            {
                _committed[index] = (entry.Term, data, node.Term);
            }
        }
    }

    private string? LogMatching(int a, int b)
    {
        InMemoryRaftLog la = _logs[a];
        InMemoryRaftLog lb = _logs[b];
        ulong last = Math.Min(la.LastIndex, lb.LastIndex);
        ulong first = Math.Max(la.FirstIndex, lb.FirstIndex);
        for (ulong index = last; index >= first && index > 0; index--)
        {
            if (la.TermAt(index) != lb.TermAt(index))
                continue;
            for (ulong k = first; k <= index; k++)
            {
                RaftEntry ea = la.EntryAt(k);
                RaftEntry eb = lb.EntryAt(k);
                if (ea.Term != eb.Term || ea.Kind != eb.Kind || !ea.Data.Span.SequenceEqual(eb.Data.Span))
                    return $"log matching: n{a} and n{b} agree on entry {index}@{la.TermAt(index)} but differ at {k}";
            }

            return null;
        }

        return null;
    }

    private static bool HasOutgoingVoteRequest(RaftNode node) => node.Role is RaftRole.PreCandidate or RaftRole.Candidate;

    private string Name(NodeId id) => _indexOf.TryGetValue(id, out int i) ? "n" + i.ToString(CultureInfo.InvariantCulture) : id.ToString();

    private static int ParseNode(string arg) => int.Parse(arg.AsSpan(1), NumberStyles.None, CultureInfo.InvariantCulture);

    private string Render(RaftMessage message)
    {
        string head = $"{Name(message.From)}->{Name(message.To)} t{message.Term}";
        return message switch
        {
            VoteRequest m => $"VoteReq {head} pv={m.PreVote} last={m.LastLogIndex}@{m.LastLogTerm} xfer={m.LeadershipTransfer}",
            VoteResponse m => $"VoteResp {head} pv={m.PreVote} reject={m.Reject}",
            AppendEntries m => $"Append {head} prev={m.PrevLogIndex}@{m.PrevLogTerm} commit={m.Commit} ctx={m.ReadContext} entries=[{string.Join(",", m.Entries.Select(e => $"{e.Index}@{e.Term}:{e.Kind}:{Convert.ToHexString(e.Data.Span)}"))}]",
            AppendResponse m => $"AppendResp {head} reject={m.Reject} match={m.MatchIndex} hint={m.RejectHint}@{m.RejectHintTerm} ctx={m.ReadContext}",
            Heartbeat m => $"Heartbeat {head} commit={m.Commit} ctx={m.ReadContext}",
            HeartbeatResponse m => $"HeartbeatResp {head} ctx={m.ReadContext}",
            TimeoutNow => $"TimeoutNow {head}",
            InstallSnapshot m => $"Snapshot {head} meta={m.Meta.Index}@{m.Meta.Term} off={m.Offset} len={m.Data.Length} hash={Convert.ToHexString(SHA256.HashData(m.Data.Span))[..16]} done={m.Done}",
            InstallSnapshotResponse m => $"SnapshotResp {head} idx={m.SnapshotIndex} next={m.NextOffset} reject={m.Reject}",
            _ => $"{message.GetType().Name} {head} {message}",
        };
    }
}
