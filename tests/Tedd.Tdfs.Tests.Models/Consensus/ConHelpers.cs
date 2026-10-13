using System.Globalization;
using System.IO.Hashing;
using System.Runtime.InteropServices;
using System.Text;
using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Testing;

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
/// A state of the CON-001 model as seen by <see cref="ModelChecker"/>: the model cluster in that state (production
/// <see cref="RaftNode"/> cores, their durable state and the network) and its canonical key.
/// </summary>
internal sealed class RaftModelState(RaftModelCluster cluster, SubsumptionKey key)
{
    private string? _violation;
    private bool _checked;

    /// <summary>The model cluster in this state (never modified).</summary>
    public RaftModelCluster Cluster { get; } = cluster;

    /// <summary>Canonical key and subsumption abstraction (see <see cref="RaftModelCluster.ComputeKey"/>).</summary>
    public SubsumptionKey Key { get; } = key;

    /// <summary>
    /// The first invariant violation observed along the schedule that reached this state or in this state (null when none);
    /// evaluated on first use, so states the explorer has already visited under another schedule are never checked twice.
    /// </summary>
    public string? Violation
    {
        get
        {
            if (!_checked)
            {
                _violation = Cluster.Violation ?? Cluster.CheckInvariants();
                _checked = true;
            }

            return _violation;
        }
    }
}

/// <summary>
/// Deterministic host for a set of production <see cref="RaftNode"/> cores used for bounded-exhaustive exploration
/// (CON-001). The host behaves like a correct <c>RaftGroupHost</c>: it persists hard state and entries before it
/// releases the ready's messages, applies committed entries, and on restart rebuilds the core from the persisted hard
/// state and log only. The network is an explicit multiset of in-flight messages; the explorer chooses which message
/// is delivered, dropped or duplicated next, so every interleaving of deliveries is a separate schedule.
/// </summary>
/// <remarks>
/// <para>The key of a state is abstract (<see cref="ComputeKey"/>). Its core is, for every node, whether it runs, the
/// complete in-memory state of its core (<c>RaftNode.DescribeCanonicalState</c>: term, vote, leader, role, timers,
/// membership history, progress, votes, read state, volatile log), its persisted hard state and durable log, and its
/// applied commands; plus the history trackers the invariants are checked against (leaders per term, vote grants per
/// voter and term, committed entries with the lowest term that observed them). Beside the core it lists the consumed
/// budgets and the in-flight message multiset. A core's future is determined by its configuration, its described state
/// and its durable log: the only randomness, the drawn election timeout, never matters because a timeout action ticks a
/// node until it campaigns whatever it drew (see <c>DescribeCanonicalState(abstractElectionTimer)</c>). Two schedules
/// with equal keys therefore have identical futures and identical invariant results, so merging them is sound. Unlike a
/// key over the full per-node input history this also merges schedules that deliver the same messages to one node in a
/// different order, or that deliver messages that change nothing (stale or duplicate ones).</para>
/// <para>The key is symmetry-reduced. Nothing in a core or in this host depends on which node is which beyond identity
/// (cores never order node ids and use a membership only as sets; payloads are numbered globally; the drawn randomness
/// is abstracted away), so a permutation of the nodes that preserves the initial roles (voters among voters, learners
/// among learners) maps every schedule onto a schedule with the permuted outcome, and states that differ only by such a
/// renaming have the same future up to that renaming and satisfy the (renaming-invariant) safety properties alike. The
/// key writes every node by its position in a canonical order, so only one representative of each orbit is explored.</para>
/// <para>States are also pruned by simulation (<see cref="ModelChecker.ExploreParallelWithSubsumption{TState}"/>): a
/// state with the same core, no more consumed budget and a superset of the in-flight messages can take every action the
/// other can, with the same effect on every node (an extra message may simply never be delivered, and the discarding of
/// messages that can never have an effect again, <see cref="PruneDeadMessages"/>, depends on the cores and, through rules
/// that only apply once a budget is spent, on the consumed budgets, so the state with no more consumed budget discards
/// no more), and the invariants depend on the core alone; so a state simulated by one already visited has no behaviour
/// of its own to explore. In particular message loss, explicit or as a message that is never delivered, is covered by the
/// state that still holds the message.</para>
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
    private readonly List<byte[]>[] _applied;
    private readonly List<Inflight> _inflight = [];
    private SortedDictionary<ulong, SortedSet<int>> _leadersByTerm = [];
    private SortedDictionary<ulong, (RaftEntry Entry, ulong ObservedTerm)> _committed = [];
    private Dictionary<(int Voter, ulong Term), int> _votes = [];
    private int _timeouts;
    private int _heartbeats;
    private int _proposals;
    private int _drops;
    private int _duplicates;
    private int _crashes;
    private int _confChanges;
    private long _voteRequestsSent;
    private string? _violation;

    // Copy-on-write: a successor shares its parent's cores, durable logs, applied lists and history trackers until an
    // action is about to change them (Touch, OwnTrackers); a state is never modified once its key is computed, so shared
    // objects are only read (concurrently, by any worker).
    private readonly bool[] _owned;
    private bool _trackersOwned;

    // Hash caches (key computation dominates the exploration cost): a node's description, and its part of the history
    // trackers, change only when an action touches that node, so a successor reuses its parent's hashes for every other
    // node. The canonical hash depends on the names of the other nodes too and is cached per renaming.
    private readonly UInt128?[] _signature;
    private readonly HashCache?[] _canonical;
    private HashCache? _trackerHashes;

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
        _applied = new List<byte[]>[n];
        _signature = new UInt128?[n];
        _canonical = new HashCache?[n];
        _owned = new bool[n];
        Array.Fill(_owned, true);
        _trackersOwned = true;
        for (int i = 0; i < n; i++)
        {
            _logs[i] = new InMemoryRaftLog();
            _hard[i] = new RaftHardState(0, null, 0);
            _applied[i] = [];
            _nodes[i] = CreateCore(i);
        }
    }

    private RaftModelCluster(RaftModelCluster source)
    {
        _bounds = source._bounds;
        _seed = source._seed;
        _ids = source._ids;
        _indexOf = source._indexOf;
        _conf = source._conf;
        int n = source._nodes.Length;
        _nodes = new RaftNode?[n];
        _logs = new InMemoryRaftLog[n];
        _hard = (RaftHardState[])source._hard.Clone();
        _lives = (int[])source._lives.Clone();
        _applied = new List<byte[]>[n];
        _signature = (UInt128?[])source._signature.Clone();
        _canonical = (HashCache?[])source._canonical.Clone();
        _trackerHashes = source._trackerHashes;
        _owned = new bool[n];
        Array.Copy(source._nodes, _nodes, n);
        Array.Copy(source._logs, _logs, n);
        Array.Copy(source._applied, _applied, n);
        _inflight.AddRange(source._inflight);
        _leadersByTerm = source._leadersByTerm;
        _committed = source._committed;
        _votes = source._votes;
        _timeouts = source._timeouts;
        _heartbeats = source._heartbeats;
        _proposals = source._proposals;
        _drops = source._drops;
        _duplicates = source._duplicates;
        _crashes = source._crashes;
        _confChanges = source._confChanges;
        _voteRequestsSent = source._voteRequestsSent;
        _violation = source._violation;
        AcceptedConfChanges = source.AcceptedConfChanges;
    }

    /// <summary>An independent copy of this state (cores, durable logs, network and trackers).</summary>
    public RaftModelCluster Clone() => new(this);

    /// <summary>True when some running core currently believes it is the leader.</summary>
    public bool AnyLeader => _nodes.Any(n => n is not null && n.Role == RaftRole.Leader);

    /// <summary>First invariant violation observed, or null.</summary>
    public string? Violation => _violation;

    /// <summary>Number of membership changes a leader accepted along the schedule (vacuity check of configuration rows).</summary>
    public int AcceptedConfChanges { get; private set; }

    /// <summary>The initial model state for <see cref="ModelChecker"/>.</summary>
    public static RaftModelState Initial(RaftModelBounds bounds, ulong seed)
    {
        var cluster = new RaftModelCluster(bounds, seed);
        return new RaftModelState(cluster, cluster.ComputeKey());
    }

    /// <summary>
    /// Successor function for <see cref="ModelChecker"/>: every enabled action applied to its own copy of the state (the
    /// state itself is never modified, so the explorer may keep it while it enumerates its successors lazily).
    /// </summary>
    public static IEnumerable<(string Action, RaftModelState Next)> Successors(RaftModelState state)
    {
        if (state.Violation is not null)
            yield break;
        RaftModelCluster current = state.Cluster;
        foreach (string action in current.EnabledActions())
        {
            RaftModelCluster successor = current.Clone();
            successor.Apply(action);
            yield return (action, new RaftModelState(successor, successor.ComputeKey()));
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
        ApplyCore(action);
        PruneDeadMessages();
    }

    /// <summary>
    /// Removes in-flight messages whose delivery can never have an effect again (<see cref="RaftNode.Step"/> ignores them
    /// without output now and in every reachable future), so they cannot distinguish states. Terms never decrease (a node
    /// persists its term before anything depends on it, and restarts from it), a node becomes a candidate only by moving to
    /// a new term, and a leader only by winning the election of its own candidate term. Hence:
    /// <list type="bullet">
    /// <item>any response, and <see cref="TimeoutNow"/>, whose term is below the recipient's term is ignored forever;</item>
    /// <item>a (non-pre-vote) <see cref="VoteResponse"/> of the recipient's current term counts only while the recipient is the
    /// candidate of that term (it can never become one again in that term);</item>
    /// <item>an append, heartbeat or snapshot response of the recipient's current term counts only while the recipient leads
    /// that term (a node never leads a term again after it stopped leading it).</item>
    /// </list>
    /// A pre-vote response stays relevant while its future term is above the recipient's term: a pre-candidate may start
    /// another pre-vote round in the same term.
    /// <para>Besides, an answer to a running leader of its term that the leader's progress tracking already covers is spent
    /// (<c>RaftNode.IsSpentLeaderAnswer</c>: a stale acknowledgement or rejection, or the heartbeat answer of a fully
    /// replicated follower once no proposal or membership change can be made any more; under CheckQuorum only once the
    /// heartbeat budget is spent, as only heartbeat actions tick a leader): it is ignored without output while the leader
    /// leads that term, and by the rules above afterwards. The model never removes a member and adds it back, as
    /// that method requires.</para>
    /// <para>Finally, once no new term can arise (the election budget is spent, the model makes no membership changes, so
    /// no leader hands over with <see cref="TimeoutNow"/>, and no pre-candidate can still win its pre-vote), every term is
    /// at most the highest term M any node has now. A leader message or vote request whose term is below its running
    /// recipient's (<c>RaftNode.IsStale</c>) only makes the recipient answer the sender with the recipient's term at that
    /// time, at most M. If the sender is already at M and neither a candidate nor a pre-candidate (it can never become one
    /// of M again), that answer is ignored whenever it arrives: an answer below the sender's term, or of its term to a
    /// non-candidate, non-leader, is dead by the rules above, and the append answer of a leader of M is spent when the
    /// leader already heard from that follower (checked with <c>IsSpentLeaderAnswer</c> on the answer it would get).</para>
    /// <para>In that situation no node is or can become a pre-candidate any more, so every pre-vote answer that does not raise
    /// its recipient's term is dead, and so is a pre-vote request of a sender already at M (its answer is a grant or carries a
    /// term of at most M). A vote request of term M to a running node of term M that knows a leader and voted for someone
    /// else (or not at all) is rejected without effect, and will be for ever unless the node restarts (the crash budget must
    /// be spent): within a term a known leader is only replaced by another, and the vote is then fixed; the rejection is dead
    /// unless the sender is still a candidate (it cannot become one of M again). The answers
    /// (append, heartbeat and snapshot answers, vote answers other than pre-vote grants) addressed to a node whose term is
    /// below theirs are interchangeable one-shot term notifications: the first one delivered makes the node a follower of
    /// that term without a leader (every membership is static, so the sender is always a member), and since the node can
    /// never become a candidate or leader of that term, all the others are then dead. Only one of them per recipient and
    /// term is kept (the one with the smallest key, so that the choice depends on the set only).</para>
    /// <para>Once, in addition, no proposal or membership change can be made and the crash budget is spent, a leader
    /// message of the leader of M to a running follower of M that changes nothing there (<c>RaftNode.IsRedundantLeaderMessage</c>:
    /// the follower already holds and commits what it carries) is dead when the leader is fully replicated to that
    /// follower (<c>RaftNode.IsFullyReplicatedTo</c>): the follower stays its follower with the same timer (it is never
    /// ticked or restarted again), its log and commit index only grow, so the message stays redundant, and every answer it
    /// can produce is spent at the leader.</para>
    /// </summary>
    private void PruneDeadMessages()
    {
        bool logsFixed = _proposals >= _bounds.Proposals && _confChanges >= _bounds.ConfChanges;
        bool ticksOver = _heartbeats >= _bounds.Heartbeats; // only heartbeat actions tick a leader
        bool termsFixed = _timeouts >= _bounds.Timeouts && _bounds.ConfChanges == 0 && !_nodes.Any(n => n is { Role: RaftRole.PreCandidate });
        ulong maxTerm = 0;
        for (int i = 0; i < _nodes.Length; i++)
            maxTerm = Math.Max(maxTerm, Math.Max(_hard[i].Term, _nodes[i]?.Term ?? 0));
        _inflight.RemoveAll(entry =>
        {
            RaftMessage m = entry.Message;
            int to = _indexOf[m.To];
            RaftNode? node = _nodes[to];
            ulong term = Math.Max(_hard[to].Term, node?.Term ?? 0);
            RaftRole role = node?.Role ?? RaftRole.Follower; // a crashed node restarts as a follower of its persisted term
            return m switch
            {
                VoteResponse { PreVote: true, Reject: false } when termsFixed => true,
                VoteResponse { PreVote: true } => m.Term < term || (termsFixed && m.Term <= term),
                VoteResponse => m.Term < term || (m.Term == term && role != RaftRole.Candidate),
                AppendResponse or HeartbeatResponse or InstallSnapshotResponse => m.Term < term || (m.Term == term && role != RaftRole.Leader)
                    || (node is not null && node.IsSpentLeaderAnswer(m, logsFixed, ticksOver)),
                TimeoutNow => m.Term < term,
                VoteRequest { PreVote: true } => termsFixed && TermOf(_indexOf[m.From]) == maxTerm,
                VoteRequest { PreVote: false } when termsFixed && _crashes >= _bounds.Crashes && node is not null && m.Term == maxTerm
                    && node.Term == maxTerm && node.Leader is not null && node.Vote != m.From =>
                    (_nodes[_indexOf[m.From]]?.Role ?? RaftRole.Follower) != RaftRole.Candidate,
                AppendEntries or Heartbeat when termsFixed && logsFixed && _crashes >= _bounds.Crashes && m.Term == maxTerm && node is not null
                    && node.IsRedundantLeaderMessage(m) =>
                    _nodes[_indexOf[m.From]] is { } leader && leader.IsFullyReplicatedTo(m.To, maxTerm, logsFixed, ticksOver),
                AppendEntries or Heartbeat or InstallSnapshot or VoteRequest { PreVote: false } =>
                    termsFixed && node is not null && node.IsStale(m) && StaleAnswerIsDead(m, maxTerm, logsFixed, ticksOver),
                _ => false,
            };
        });

        if (termsFixed)
            KeepOneTermNotification();
    }

    /// <summary>The term of node <paramref name="i"/>: its running core's, else (crashed) its persisted one.</summary>
    private ulong TermOf(int i) => Math.Max(_hard[i].Term, _nodes[i]?.Term ?? 0);

    /// <summary>Keeps one of the interchangeable term notifications per recipient and term (see <see cref="PruneDeadMessages"/>).</summary>
    private void KeepOneTermNotification()
    {
        Dictionary<(int To, ulong Term), string>? kept = null;
        foreach (Inflight entry in _inflight)
        {
            if (TermNotification(entry.Message) is not { } target)
                continue;
            kept ??= [];
            if (!kept.TryGetValue(target, out string? smallest) || string.CompareOrdinal(entry.Key, smallest) < 0)
                kept[target] = entry.Key;
        }

        if (kept is null)
            return;
        var keptOnce = new HashSet<(int To, ulong Term)>();
        _inflight.RemoveAll(entry => TermNotification(entry.Message) is { } target
            && (!string.Equals(entry.Key, kept[target], StringComparison.Ordinal) || !keptOnce.Add(target)));
    }

    /// <summary>The (recipient, term) of an answer that would raise its recipient's term, or null.</summary>
    private (int To, ulong Term)? TermNotification(RaftMessage m)
    {
        if (m is not (AppendResponse or HeartbeatResponse or InstallSnapshotResponse or VoteResponse) || m is VoteResponse { PreVote: true, Reject: false })
            return null;
        int to = _indexOf[m.To];
        ulong term = Math.Max(_hard[to].Term, _nodes[to]?.Term ?? 0);
        return m.Term > term ? (to, m.Term) : null;
    }

    /// <summary>
    /// True when the answer to the stale message <paramref name="m"/> (carrying its recipient's term, at most
    /// <paramref name="maxTerm"/>) is ignored by its sender whenever it arrives (see <see cref="PruneDeadMessages"/>).
    /// </summary>
    private bool StaleAnswerIsDead(RaftMessage m, ulong maxTerm, bool logsFixed, bool ticksOver)
    {
        int from = _indexOf[m.From];
        RaftNode? sender = _nodes[from];
        if (Math.Max(_hard[from].Term, sender?.Term ?? 0) != maxTerm)
            return false;
        return (sender?.Role ?? RaftRole.Follower) switch
        {
            RaftRole.Follower or RaftRole.Learner => true,
            RaftRole.Leader => m is VoteRequest
                || sender!.IsSpentLeaderAnswer(new AppendResponse(m.To, m.From, maxTerm, Reject: false, MatchIndex: 0, RejectHint: 0), logsFixed,
                    ticksOver),
            _ => false,
        };
    }

    private void ApplyCore(string action)
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
                (_, RaftMessage message) = _inflight[position];
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
                Touch(to);
                RaftNode node = _nodes[to] ?? throw new InvalidOperationException($"Recipient n{to} is crashed.");
                node.Step(message);
                Drain(to);
                return;
            }

            case "timeout":
            {
                int i = ParseNode(arg);
                Touch(i);
                RaftNode node = _nodes[i] ?? throw new InvalidOperationException($"n{i} is crashed.");
                _timeouts++;

                // One election attempt: tick until the node starts a new campaign (its role or term changes, or it sends
                // new vote requests: a pre-candidate or candidate campaigns again), or until it evidently cannot campaign.
                int maxTicks = (RaftModelBounds.ElectionTicksMax * 2) + 2;
                int ticks = 0;
                RaftRole before = node.Role;
                ulong termBefore = node.Term;
                while (ticks < maxTicks)
                {
                    ticks++;
                    long requestsBefore = _voteRequestsSent;
                    node.Tick();
                    Drain(i);
                    if (node.Role != before || node.Term != termBefore || _voteRequestsSent > requestsBefore)
                        break;
                }

                return;
            }

            case "heartbeat":
            {
                int i = ParseNode(arg);
                Touch(i);
                RaftNode node = _nodes[i] ?? throw new InvalidOperationException($"n{i} is crashed.");
                _heartbeats++;
                for (int t = 0; t < RaftModelBounds.HeartbeatTicks; t++)
                {
                    node.Tick();
                    Drain(i);
                }

                return;
            }

            case "propose":
            {
                int i = ParseNode(arg);
                Touch(i);
                RaftNode node = _nodes[i] ?? throw new InvalidOperationException($"n{i} is crashed.");
                string payload = "p" + _proposals.ToString(CultureInfo.InvariantCulture);
                _proposals++;
                node.TryPropose(Encoding.ASCII.GetBytes(payload), out _);
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
                Touch(i);
                RaftNode node = _nodes[i] ?? throw new InvalidOperationException($"n{i} is crashed.");
                _confChanges++;
                var change = new RaftConfChange(verb == "promote" ? RaftConfChangeKind.PromoteLearner : RaftConfChangeKind.RemoveNode, _ids[target]);
                if (node.TryProposeConfChange(change, out _))
                    AcceptedConfChanges++;
                Drain(i);
                return;
            }

            case "crash":
            {
                int i = ParseNode(arg);
                if (_nodes[i] is null)
                    throw new InvalidOperationException($"n{i} is already crashed.");
                _crashes++;
                Touch(i);
                _nodes[i] = null;
                _applied[i] = [];
                return;
            }

            case "restart":
            {
                int i = ParseNode(arg);
                if (_nodes[i] is not null)
                    throw new InvalidOperationException($"n{i} is running.");
                _lives[i]++;
                Touch(i);
                _nodes[i] = CreateCore(i);
                Drain(i);
                return;
            }

            default:
                throw new InvalidOperationException($"Unknown action '{action}'.");
        }
    }

    /// <summary>
    /// Canonical, symmetry-reduced abstract key (see the type remarks), split for simulation-based pruning
    /// (<see cref="ModelChecker.ExploreParallelWithSubsumption{TState}"/>) into the core (every node's description and the
    /// history trackers), the consumed budgets and the in-flight messages. Nodes are ordered by a renaming-invariant
    /// signature (a hash of their own description with themselves written "S" and every other node "?", plus their part of
    /// the history trackers); among the renamings that respect that order (nodes with equal signatures permuted among
    /// themselves) the one with the smallest core hash is used, ties broken by the smallest sorted list of message hashes.
    /// Every state of an orbit therefore gets the same key.
    /// </summary>
    /// <remarks>
    /// A state with the same core, no more consumed budget and a superset of the in-flight messages simulates this one: it can
    /// take every action this one can with the same effect on the nodes (it may always leave its extra messages undelivered,
    /// and it discards no more messages than this one, see the type remarks), and the invariants depend only on the core.
    /// Message loss is therefore covered both by explicit drops and by never delivering a message.
    /// </remarks>
    public SubsumptionKey ComputeKey()
    {
        int n = _nodes.Length;
        var signatures = new UInt128[n];
        for (int i = 0; i < n; i++)
            signatures[i] = _signature[i] ??= Signature(i);
        int[] order = Enumerable.Range(0, n).OrderBy(i => signatures[i]).ToArray();

        // Positions [start, end) of runs of equal signatures: nodes within a run may be renamed among themselves.
        var runs = new List<(int Start, int End)>();
        for (int start = 0; start < n;)
        {
            int end = start + 1;
            while (end < n && signatures[order[end]] == signatures[order[start]])
                end++;
            if (end - start > 1)
                runs.Add((start, end));
            start = end;
        }

        UInt128 bestCore = UInt128.MaxValue;
        UInt128[]? bestItems = null;
        var position = new int[n];
        foreach (int[] candidate in Arrangements(order, runs))
        {
            for (int k = 0; k < n; k++)
                position[candidate[k]] = k;
            UInt128 core = CoreHash(candidate, position, out ulong renaming);
            if (core > bestCore)
                continue;
            UInt128[] items = Items(position, renaming);
            if (core < bestCore || CompareItems(items, bestItems!) < 0)
            {
                bestCore = core;
                bestItems = items;
            }
        }

        int[] consumed = [_timeouts, _heartbeats, _proposals, _drops, _duplicates, _crashes, _confChanges];
        return new SubsumptionKey(bestCore, consumed, bestItems!);
    }

    private static int CompareItems(UInt128[] a, UInt128[] b)
    {
        for (int i = 0; i < Math.Min(a.Length, b.Length); i++)
        {
            int c = a[i].CompareTo(b[i]);
            if (c != 0)
                return c;
        }

        return a.Length.CompareTo(b.Length);
    }

    /// <summary>The renaming-invariant signature of node <paramref name="i"/> (see <see cref="ComputeKey"/>).</summary>
    private UInt128 Signature(int i)
    {
        string Anonymous(NodeId id) => _indexOf[id] == i ? "S" : "?";
        var sb = new StringBuilder(1024);
        sb.Append(i < _bounds.Voters ? 'V' : 'L');
        AppendNode(sb, i, Anonymous);
        sb.Append("|led=");
        foreach ((ulong term, SortedSet<int> leaders) in _leadersByTerm)
        {
            if (leaders.Contains(i))
                sb.Append(term.ToString(CultureInfo.InvariantCulture)).Append(',');
        }

        sb.Append("|voted=");
        foreach (((int voter, ulong term), int candidate) in _votes.Where(v => v.Key.Voter == i).OrderBy(v => v.Key.Term))
            sb.Append(CultureInfo.InvariantCulture, $"{term}:{(candidate == i ? "S" : "?")},");
        return Hash(sb.ToString());
    }

    /// <summary>The hash of the core (nodes and trackers) with node <c>order[k]</c> written as <c>N{k}</c>.</summary>
    private UInt128 CoreHash(int[] order, int[] position, out ulong renaming)
    {
        string Canonical(NodeId id) => Names[position[_indexOf[id]]];
        renaming = 0;
        for (int i = 0; i < position.Length; i++)
            renaming |= (ulong)position[i] << (4 * i);

        int n = order.Length;
        Span<UInt128> parts = stackalloc UInt128[n + 1];
        for (int k = 0; k < n; k++)
        {
            int i = order[k];
            if (!HashCache.TryFind(_canonical[i], renaming, out UInt128 nodeHash))
            {
                var sb = new StringBuilder(1024);
                sb.Append(i < _bounds.Voters ? 'V' : 'L');
                AppendNode(sb, i, Canonical);
                nodeHash = Hash(sb.ToString());
                _canonical[i] = new HashCache(renaming, nodeHash, _canonical[i]);
            }

            parts[k] = nodeHash;
        }

        if (_violation is null && HashCache.TryFind(_trackerHashes, renaming, out UInt128 trackerHash))
        {
            parts[n] = trackerHash;
            return XxHash128.HashToUInt128(MemoryMarshal.AsBytes(parts));
        }

        var trackers = new StringBuilder(256);
        trackers.Append("leaders[");
        foreach ((ulong term, SortedSet<int> leaders) in _leadersByTerm)
            trackers.Append(CultureInfo.InvariantCulture, $"{term}:{string.Join(" ", leaders.Select(l => position[l]).Order())},");
        trackers.Append("]votes[");
        foreach (string vote in _votes.Select(v => string.Create(CultureInfo.InvariantCulture, $"{position[v.Key.Voter]}/{v.Key.Term}:{position[v.Value]}")).Order(StringComparer.Ordinal))
            trackers.Append(vote).Append(',');
        trackers.Append("]committed[");
        foreach ((ulong _, (RaftEntry entry, ulong observedTerm)) in _committed)
            trackers.Append(RaftNode.DescribeEntry(entry, Canonical)).Append(CultureInfo.InvariantCulture, $"/{observedTerm},");
        trackers.Append("]violation[").Append(_violation is not null).Append(']');
        parts[n] = Hash(trackers.ToString());
        if (_violation is null)
            _trackerHashes = new HashCache(renaming, parts[n], _trackerHashes);
        return XxHash128.HashToUInt128(MemoryMarshal.AsBytes(parts));
    }

    /// <summary>The sorted hashes of the in-flight messages under the renaming of <paramref name="position"/>.</summary>
    private UInt128[] Items(int[] position, ulong renaming)
    {
        string Canonical(NodeId id) => Names[position[_indexOf[id]]];
        var items = new UInt128[_inflight.Count];
        for (int k = 0; k < items.Length; k++)
            items[k] = _inflight[k].Hash(renaming, Canonical);
        Array.Sort(items);
        return items;
    }

    private static UInt128 Hash(string text) => XxHash128.HashToUInt128(MemoryMarshal.AsBytes(text.AsSpan()));


    /// <summary>Every order obtained from <paramref name="order"/> by permuting the nodes within each run independently.</summary>
    private static IEnumerable<int[]> Arrangements(int[] order, List<(int Start, int End)> runs)
    {
        if (runs.Count == 0)
        {
            yield return order;
            yield break;
        }

        var current = (int[])order.Clone();
        foreach (int[] arrangement in Permute(current, runs, 0))
            yield return arrangement;

        static IEnumerable<int[]> Permute(int[] current, List<(int Start, int End)> runs, int run)
        {
            if (run == runs.Count)
            {
                yield return current;
                yield break;
            }

            (int start, int end) = runs[run];
            foreach (bool _ in Heap(current, start, end - start))
            {
                foreach (int[] arrangement in Permute(current, runs, run + 1))
                    yield return arrangement;
            }
        }

        // Heap's algorithm over current[start .. start+count): yields once per permutation (the array is permuted in place).
        static IEnumerable<bool> Heap(int[] a, int start, int count)
        {
            var c = new int[count];
            yield return true;
            int i = 0;
            while (i < count)
            {
                if (c[i] < i)
                {
                    int j = i % 2 == 0 ? 0 : c[i];
                    (a[start + j], a[start + i]) = (a[start + i], a[start + j]);
                    yield return true;
                    c[i]++;
                    i = 0;
                }
                else
                {
                    c[i] = 0;
                    i++;
                }
            }
        }
    }

    /// <summary>
    /// An action is about to change node <paramref name="i"/>: takes a private copy of its core, durable log and applied list
    /// when they are still shared with the parent state, and forgets its cached hashes.
    /// </summary>
    private void Touch(int i)
    {
        if (!_owned[i])
        {
            _logs[i] = _logs[i].Clone();
            _applied[i] = [.. _applied[i]];
            if (_nodes[i] is { } core)
                _nodes[i] = core.Clone(_logs[i], CreateRandom(i));
            _owned[i] = true;
        }

        _signature[i] = null;
        _canonical[i] = null;
    }

    /// <summary>The history trackers are about to change: takes private copies when they are shared with the parent state.</summary>
    private void OwnTrackers()
    {
        _trackerHashes = null;
        if (_trackersOwned)
            return;
        var leaders = new SortedDictionary<ulong, SortedSet<int>>();
        foreach ((ulong term, SortedSet<int> set) in _leadersByTerm)
            leaders[term] = [.. set];
        _leadersByTerm = leaders;
        _committed = new SortedDictionary<ulong, (RaftEntry Entry, ulong ObservedTerm)>(_committed);
        _votes = new Dictionary<(int Voter, ulong Term), int>(_votes);
        _trackersOwned = true;
    }

    /// <summary>An immutable list of hashes by renaming (shared by the states whose described part is the same).</summary>
    private sealed record HashCache(ulong Renaming, UInt128 Value, HashCache? Next)
    {
        public static bool TryFind(HashCache? cache, ulong renaming, out UInt128 value)
        {
            for (HashCache? entry = cache; entry is not null; entry = entry.Next)
            {
                if (entry.Renaming == renaming)
                {
                    value = entry.Value;
                    return true;
                }
            }

            value = default;
            return false;
        }
    }

    /// <summary>The canonical names <c>N0</c>, <c>N1</c>, ... of the node positions.</summary>
    private static readonly string[] Names = Enumerable.Range(0, 16).Select(k => "N" + k.ToString(CultureInfo.InvariantCulture)).ToArray();

    /// <summary>
    /// A message in flight (immutable, shared by every state that holds it) with a cache of its hashes under the renamings
    /// used so far: workers fill it concurrently without locking (an entry is published atomically; a duplicate is harmless).
    /// </summary>
    private sealed class Inflight(string key, RaftMessage message)
    {
        private HashEntry? _hashes;

        /// <summary>The message's canonical text with the model's node names (identifies it in actions).</summary>
        public string Key { get; } = key;

        public RaftMessage Message { get; } = message;

        public void Deconstruct(out string key, out RaftMessage message)
        {
            key = Key;
            message = Message;
        }

        /// <summary>The hash of the message with node ids written by <paramref name="name"/>, the renaming <paramref name="renaming"/>.</summary>
        public UInt128 Hash(ulong renaming, Func<NodeId, string> name)
        {
            for (HashEntry? entry = Volatile.Read(ref _hashes); entry is not null; entry = entry.Next)
            {
                if (entry.Renaming == renaming)
                    return entry.Value;
            }

            UInt128 hash = RaftModelCluster.Hash(RaftNode.DescribeMessage(Message, name));
            HashEntry? head;
            do
            {
                head = Volatile.Read(ref _hashes);
            }
            while (Interlocked.CompareExchange(ref _hashes, new HashEntry(renaming, hash, head), head) != head);
            return hash;
        }

        private sealed record HashEntry(ulong Renaming, UInt128 Value, HashEntry? Next);
    }

    /// <summary>Appends one node's run state, core state, durable state and applied commands, node ids written by <paramref name="name"/>.</summary>
    private void AppendNode(StringBuilder sb, int i, Func<NodeId, string> name)
    {
        sb.Append('[').Append(_nodes[i] is { } core ? core.DescribeCanonicalState(abstractElectionTimer: true, name) : "crashed");
        RaftHardState hard = _hard[i];
        sb.Append(CultureInfo.InvariantCulture, $";hard={hard.Term}/{(hard.Vote is { } vote ? name(vote) : "-")}/{hard.Commit}");
        InMemoryRaftLog log = _logs[i];
        sb.Append(CultureInfo.InvariantCulture, $";durable={log.Snapshot.Index}@{log.Snapshot.Term}/").Append(RaftNode.DescribeConf(log.Snapshot.ConfState, name)).Append(':');
        for (ulong index = log.FirstIndex; index <= log.LastIndex; index++)
            sb.Append(RaftNode.DescribeEntry(log.EntryAt(index), name)).Append(',');
        sb.Append(";applied=");
        foreach (byte[] command in _applied[i])
            sb.Append(Convert.ToHexString(command)).Append(',');
        sb.Append(']');
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
            foreach ((ulong index, (RaftEntry entry, ulong observedTerm)) in _committed)
            {
                if (node.Term < observedTerm)
                    continue;
                if (!_logs[i].TryGetTerm(index, out ulong leaderTerm) || leaderTerm != entry.Term)
                    return $"leader completeness: leader n{i} (term {node.Term}) lacks entry {index}@{entry.Term} ({RaftNode.DescribeEntry(entry, Name)}) committed by term {observedTerm}";
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
        return new RaftNode(config, _hard[i], _conf, _logs[i], CreateRandom(i));
    }

    /// <summary>
    /// The random source of a core (its randomized election timeouts). Which values it draws never matters for the model:
    /// a timeout action ticks until the node campaigns, whatever its drawn timeout (see the type remarks).
    /// </summary>
    private SeededRandom CreateRandom(int i) =>
        new(SeededRandom.DeriveSeed(_seed, string.Create(CultureInfo.InvariantCulture, $"n{i}/life{_lives[i]}")));

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
                    if (!_votes.TryGetValue((i, grant.Term), out int recorded) || recorded != candidate)
                    {
                        OwnTrackers();
                        _votes[(i, grant.Term)] = candidate;
                    }
                }

                if (message is VoteRequest)
                    _voteRequestsSent++;

                if (message is AppendResponse { Reject: false } ack && ack.MatchIndex > _logs[i].LastIndex)
                    _violation ??= $"n{i} acknowledged index {ack.MatchIndex} beyond its persisted last index {_logs[i].LastIndex}";

                _inflight.Add(new Inflight(RaftNode.DescribeMessage(message, Name), message));
            }

            // Apply.
            foreach (RaftEntry entry in ready.CommittedEntries)
            {
                if (entry.Kind == RaftEntryKind.Normal && entry.Data.Length > 0)
                    _applied[i].Add(entry.Data.ToArray());
            }

            if (ready.Role == RaftRole.Leader)
                RecordLeader(i, node.Term);

            node.Advance(ready);
            RecordCommitted(i, node);
        }

        if (node.Role == RaftRole.Leader)
            RecordLeader(i, node.Term);
    }

    private void RecordLeader(int i, ulong term)
    {
        if (_leadersByTerm.TryGetValue(term, out SortedSet<int>? known) && known.Contains(i))
            return;
        OwnTrackers();
        if (!_leadersByTerm.TryGetValue(term, out SortedSet<int>? leaders))
            _leadersByTerm[term] = leaders = [];
        leaders.Add(i);
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
            if (_committed.TryGetValue(index, out (RaftEntry Entry, ulong ObservedTerm) known))
            {
                if (known.Entry.Term != entry.Term || known.Entry.Kind != entry.Kind || !known.Entry.Data.Span.SequenceEqual(entry.Data.Span))
                {
                    _violation ??= $"committed entry {index} changed: {RaftNode.DescribeEntry(known.Entry, Name)} vs n{i} {RaftNode.DescribeEntry(entry, Name)}";
                    return;
                }

                if (node.Term < known.ObservedTerm)
                {
                    OwnTrackers();
                    _committed[index] = known with { ObservedTerm = node.Term };
                }
            }
            else
            {
                OwnTrackers();
                _committed[index] = (entry, node.Term);
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

    private string Name(NodeId id) => _indexOf.TryGetValue(id, out int i) ? "n" + i.ToString(CultureInfo.InvariantCulture) : id.ToString();

    private static int ParseNode(string arg) => int.Parse(arg.AsSpan(1), NumberStyles.None, CultureInfo.InvariantCulture);
}
