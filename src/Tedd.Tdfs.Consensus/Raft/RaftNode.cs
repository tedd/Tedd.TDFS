namespace Tedd.Tdfs.Consensus;

/// <summary>
/// Pure Raft state machine (etcd/raft style): no I/O and no clock. Driven by <see cref="Tick"/>, <see cref="Step"/> and
/// proposals; produces <see cref="RaftReady"/> work items that the host persists, sends and applies before calling
/// <see cref="Advance"/>. Supports PreVote, CheckQuorum, ReadIndex, InstallSnapshot, learners, single-server membership
/// changes and leadership transfer.
/// </summary>
/// <remarks>
/// <para><b>Host contract.</b> Loop: <c>while (node.HasReady()) { var rd = node.GetReady(); ... node.Advance(rd); }</c>
/// where "..." is, in this order:</para>
/// <list type="number">
/// <item>Install <see cref="RaftReady.SnapshotToInstall"/> (restore the state machine from
/// <see cref="RaftReady.SnapshotData"/> and make the durable log start after the snapshot), persist
/// <see cref="RaftReady.HardState"/> and append <see cref="RaftReady.EntriesToPersist"/> — replacing every durable entry
/// at or after the first one's index (or, see <see cref="IRaftLogView"/>, keeping a durable suffix that matches them) —
/// all durably (flush barrier). The durable store is the <see cref="IRaftLogView"/> passed to the constructor, so it
/// must reflect these writes when <see cref="Advance"/> is called.</item>
/// <item>Send <see cref="RaftReady.Messages"/> (votes and append acknowledgements depend on step 1).</item>
/// <item>Apply <see cref="RaftReady.CommittedEntries"/> in order and serve <see cref="RaftReady.ReadStates"/> once the
/// applied index reaches each read's index.</item>
/// <item>Call <see cref="Advance"/> with the same ready.</item>
/// </list>
/// <para>At most one ready is outstanding: <see cref="HasReady"/> returns false and <see cref="GetReady"/> throws until the
/// previous one is advanced. Other calls (<see cref="Step"/>, <see cref="Tick"/>, proposals) are allowed while a ready is
/// outstanding; their effects go into the next ready.</para>
/// <para><b>Membership.</b> The <c>confState</c> constructor argument is the membership in effect at the log's snapshot
/// index (the bootstrap membership for a log without snapshot); configuration-change entries in the retained log are
/// replayed on top of it. A configuration takes effect as soon as its entry is appended and is reverted if that entry is
/// truncated. Learners never start elections and their votes are counted only by candidates whose configuration lists
/// them as voters (a learner that was promoted but has not yet seen the promotion can thus still help elect a leader).</para>
/// <para><b>Snapshots.</b> A leader obtains snapshot payloads from <see cref="RaftNodeOptions.SnapshotSource"/> when a
/// follower needs compacted entries, and streams them in <see cref="InstallSnapshot"/> chunks. A follower assembles the
/// chunks and hands the result to the host through <see cref="RaftReady.SnapshotToInstall"/>.</para>
/// <para><b>Reads.</b> <see cref="ReadIndex"/> requests are confirmed with a heartbeat quorum after the leader has committed
/// an entry of its current term; requests on non-leaders, and requests pending when leadership is lost, are dropped
/// (the caller times out and retries).</para>
/// </remarks>
public sealed partial class RaftNode
{
    private readonly RaftConfig _cfg;
    private readonly RaftNodeOptions _options;
    private readonly IRandomSource _random;
    private readonly RaftLog _log;
    private readonly NodeId _self;
    private readonly bool _reachableQuorum;

    private ulong _term;
    private NodeId? _vote;
    private NodeId? _lead;
    private NodeState _state;
    private int _electionElapsed;
    private int _heartbeatElapsed;
    private int _randomizedElectionTimeout;
    private long _tickCount;

    // Membership: base (as of _confBaseIndex) plus configuration entries appended after it.
    private RaftConfState _confBase;
    private ulong _confBaseIndex;
    private readonly List<(ulong Index, RaftConfState Conf)> _confHistory = [];
    private RaftConfState _conf;

    // Leader state.
    private readonly Dictionary<NodeId, RaftProgress> _progress = [];
    private ulong _pendingConfIndex;
    private NodeId? _leadTransferee;

    // Candidate state.
    private readonly Dictionary<NodeId, bool> _votes = [];

    // ReadIndex state.
    private readonly List<ReadRequest> _readQueue = [];
    private readonly List<ulong> _deferredReads = [];
    private ulong _readSeq;
    private List<RaftReadState> _readStates = [];

    // QuorumFromReachableNodes mutation: tick at which each peer was last heard from.
    private readonly Dictionary<NodeId, long> _lastHeard = [];

    // Follower snapshot assembly.
    private IncomingSnapshot? _incoming;

    // Ready bookkeeping.
    private List<RaftMessage> _msgs = [];
    private RaftHardState _prevHardState;
    private RaftRole _prevRole;
    private NodeId? _prevLeader;
    private RaftReady? _outstanding;

    /// <summary>Creates a node from persisted state.</summary>
    /// <param name="config">Static configuration.</param>
    /// <param name="hardState">Persisted term/vote/commit.</param>
    /// <param name="confState">Membership as of the log's snapshot index (bootstrap membership for a log without snapshot).</param>
    /// <param name="log">Durable log view (entries persisted so far and the latest snapshot).</param>
    /// <param name="random">Source for randomized election timeouts.</param>
    public RaftNode(RaftConfig config, RaftHardState hardState, RaftConfState confState, IRaftLogView log, IRandomSource random)
        : this(config, hardState, confState, log, random, null)
    {
    }

    /// <summary>Creates a node from persisted state with optional collaborators (snapshot source, mutations).</summary>
    /// <param name="config">Static configuration.</param>
    /// <param name="hardState">Persisted term/vote/commit.</param>
    /// <param name="confState">Membership as of the log's snapshot index (bootstrap membership for a log without snapshot).</param>
    /// <param name="log">Durable log view (entries persisted so far and the latest snapshot).</param>
    /// <param name="random">Source for randomized election timeouts.</param>
    /// <param name="options">Optional collaborators and limits; null for <see cref="RaftNodeOptions.Default"/>.</param>
    /// <exception cref="ArgumentException">The configuration, membership or persisted state is invalid or inconsistent.</exception>
    public RaftNode(RaftConfig config, RaftHardState hardState, RaftConfState confState, IRaftLogView log, IRandomSource random,
        RaftNodeOptions? options)
    {
        ArgumentNullException.ThrowIfNull(config);
        ArgumentNullException.ThrowIfNull(confState);
        ArgumentNullException.ThrowIfNull(log);
        ArgumentNullException.ThrowIfNull(random);
        config.Validate();
        options ??= RaftNodeOptions.Default;
        options.Validate();
        confState.Validate();

        _cfg = config;
        _options = options;
        _random = random;
        _self = config.Self;
        _reachableQuorum = options.Mutations.QuorumFromReachableNodes;

        RaftSnapshotMeta snapshot = log.Snapshot ?? throw new ArgumentException("The log view has no snapshot metadata.", nameof(log));
        if (log.FirstIndex != snapshot.Index + 1)
            throw new ArgumentException($"Inconsistent log view: first index {log.FirstIndex} does not follow snapshot index {snapshot.Index}.", nameof(log));
        if (log.LastIndex < snapshot.Index)
            throw new ArgumentException($"Inconsistent log view: last index {log.LastIndex} precedes snapshot index {snapshot.Index}.", nameof(log));
        ulong lastTerm = log.LastIndex == snapshot.Index ? snapshot.Term : log.TermAt(log.LastIndex);
        if (hardState.Term < lastTerm)
            throw new ArgumentException($"Persisted term {hardState.Term} is lower than the last log term {lastTerm}.", nameof(hardState));
        ulong commit = Math.Max(hardState.Commit, snapshot.Index);
        if (commit > log.LastIndex)
            throw new ArgumentException($"Persisted commit index {hardState.Commit} is beyond the last durable index {log.LastIndex}; committed entries are missing.", nameof(hardState));
        ulong applied = options.Applied ?? snapshot.Index;
        if (applied < snapshot.Index || applied > commit)
            throw new ArgumentException($"Applied index {applied} must lie within [{snapshot.Index}, {commit}].", nameof(options));

        _log = new RaftLog(log, commit, applied);
        _term = hardState.Term;
        _vote = hardState.Vote is { IsEmpty: true } ? null : hardState.Vote;
        _prevHardState = hardState;

        _confBase = confState;
        _confBaseIndex = snapshot.Index;
        _conf = confState;
        ReplayConfEntries(log);
        if (_reachableQuorum)
        {
            // Negative control: every configured voter counts as reachable at start-up, and then only while heard from.
            foreach (NodeId v in _conf.Voters)
                _lastHeard[v] = 0;
        }

        Reset(_term);
        _state = NodeState.Follower;
        _prevRole = Role;
        _prevLeader = _lead;
    }

    private enum NodeState
    {
        Follower,
        PreCandidate,
        Candidate,
        Leader,
    }

    /// <summary>The node's configuration.</summary>
    public RaftConfig Config => _cfg;

    /// <summary>The node's options.</summary>
    public RaftNodeOptions Options => _options;

    /// <summary>This node's id.</summary>
    public NodeId Id => _self;

    /// <summary>Current role (<see cref="RaftRole.Learner"/> for a follower that is a learner in the current membership).</summary>
    public RaftRole Role => _state switch
    {
        NodeState.Leader => RaftRole.Leader,
        NodeState.Candidate => RaftRole.Candidate,
        NodeState.PreCandidate => RaftRole.PreCandidate,
        _ => _conf.IsLearner(_self) ? RaftRole.Learner : RaftRole.Follower,
    };

    /// <summary>Current term.</summary>
    public ulong Term => _term;

    /// <summary>Vote cast in the current term, if any.</summary>
    public NodeId? Vote => _vote;

    /// <summary>Known leader, if any.</summary>
    public NodeId? Leader => _lead;

    /// <summary>Highest committed index.</summary>
    public ulong CommitIndex => _log.Committed;

    /// <summary>Highest index applied by the host (advanced by <see cref="Advance"/>).</summary>
    public ulong AppliedIndex => _log.Applied;

    /// <summary>Index of the last log entry (including entries not yet persisted).</summary>
    public ulong LastIndex => _log.LastIndex;

    /// <summary>Term of the last log entry.</summary>
    public ulong LastTerm => _log.LastTerm;

    /// <summary>First index whose entry is still available (snapshot index + 1).</summary>
    public ulong FirstIndex => _log.FirstIndex;

    /// <summary>Current membership (includes appended but uncommitted configuration changes).</summary>
    public RaftConfState ConfState => _conf;

    /// <summary>Current hard state (term, vote, commit), whether or not it has been persisted yet.</summary>
    public RaftHardState HardState => new(_term, _vote, _log.Committed);

    /// <summary>Target of an in-progress leadership transfer (leader only).</summary>
    public NodeId? LeadTransferee => _leadTransferee;

    /// <summary>
    /// True when this node is leader and cannot accept a configuration change yet: a change has not been committed, or the
    /// leader has not yet committed an entry of its own term (its election no-op).
    /// </summary>
    public bool HasPendingConfChange => _state == NodeState.Leader && _pendingConfIndex > _log.Committed;

    /// <summary>Number of ReadIndex requests waiting for confirmation (leader only).</summary>
    public int PendingReadCount => _readQueue.Count + _deferredReads.Count;

    /// <summary>Internal state for invariant checkers.</summary>
    public RaftNodeDebugState DebugState
        => new(_self, Role, _term, _vote, _lead, _log.Committed, _log.Applied, _log.LastIndex, _log.LastTerm, _conf);

    /// <summary>
    /// True when this node may campaign: it is a voter in its latest membership, or in the last membership it knows to be
    /// committed. The second case keeps a voter that an uncommitted entry removes electable until the removal commits
    /// (Raft dissertation §4.2.2): otherwise a leader that removed itself and lost its quorum before the removal committed
    /// could leave the remaining voters unable to elect anyone (it holds the newest log but would never campaign).
    /// </summary>
    private bool Promotable => _conf.IsVoter(_self) || _confBase.IsVoter(_self);

    /// <summary>Advances the logical clock by one tick (elections and heartbeats).</summary>
    public void Tick()
    {
        _tickCount++;
        if (_state == NodeState.Leader)
            TickHeartbeat();
        else
            TickElection();
    }

    /// <summary>
    /// Proposes a command; false when not leader or a leadership transfer is in progress. The data is copied.
    /// </summary>
    public bool TryPropose(ReadOnlyMemory<byte> data, out ulong index)
    {
        index = 0;
        if (_state != NodeState.Leader || _leadTransferee is not null)
            return false;
        index = AppendLeaderEntry(RaftEntryKind.Normal, data.ToArray());
        BroadcastAppend();
        return true;
    }

    /// <summary>
    /// Proposes a membership change; false when not leader, a leadership transfer is in progress, another change is not
    /// yet committed, the leader has not yet committed an entry of its term, or the change is not applicable to the
    /// current membership (see <see cref="RaftConfState.CanApply"/>).
    /// </summary>
    public bool TryProposeConfChange(RaftConfChange change, out ulong index)
    {
        ArgumentNullException.ThrowIfNull(change);
        index = 0;
        if (_state != NodeState.Leader || _leadTransferee is not null)
            return false;
        if (_pendingConfIndex > _log.Committed)
            return false;
        if (!_conf.CanApply(change, out _))
            return false;
        _pendingConfIndex = _log.LastIndex + 1;
        index = AppendLeaderEntry(RaftEntryKind.ConfChange, change.Encode());
        BroadcastAppend();
        return true;
    }

    /// <summary>
    /// Requests a linearizable read point; the result appears in <see cref="RaftReady.ReadStates"/> once leadership is
    /// confirmed by a quorum (dropped when not leader or when leadership is lost before confirmation).
    /// </summary>
    public void ReadIndex(ulong requestContext)
    {
        if (_state != NodeState.Leader)
            return;
        if (!CommittedInCurrentTerm)
        {
            _deferredReads.Add(requestContext);
            return;
        }

        StartReads([requestContext]);
    }

    /// <summary>
    /// Transfers leadership to <paramref name="target"/> (a voter): proposals are refused while the transfer is in progress;
    /// the target receives <see cref="TimeoutNow"/> once its log is up to date. The transfer is abandoned after an election
    /// timeout. Ignored when not leader or when the target is not a voter.
    /// </summary>
    public void TransferLeadership(NodeId target)
    {
        if (_state != NodeState.Leader || target == _self || !_conf.IsVoter(target))
            return;
        if (_leadTransferee == target)
            return;
        _leadTransferee = target;
        _electionElapsed = 0;
        if (!_progress.TryGetValue(target, out RaftProgress? pr))
            return;
        if (pr.Match == _log.LastIndex)
            SendTimeoutNow(target);
        else
            SendAppend(pr, sendIfEmpty: true);
    }

    /// <summary>True when <see cref="GetReady"/> would return work (false while a ready awaits <see cref="Advance"/>).</summary>
    public bool HasReady()
    {
        if (_outstanding is not null)
            return false;
        return HardState != _prevHardState
            || _log.UnstableCount > 0
            || _log.PendingSnapshot is not null
            || _msgs.Count > 0
            || _readStates.Count > 0
            || _log.HasNextCommittedEntries
            || Role != _prevRole
            || _lead != _prevLeader;
    }

    /// <summary>Returns pending work; call <see cref="Advance"/> after handling it.</summary>
    /// <exception cref="InvalidOperationException">The previous ready has not been advanced.</exception>
    public RaftReady GetReady()
    {
        if (_outstanding is not null)
            throw new InvalidOperationException("The previous RaftReady has not been advanced.");
        RaftHardState hs = HardState;
        RaftSnapshot? snapshot = _log.PendingSnapshot;
        List<RaftMessage> msgs = _msgs;
        _msgs = [];
        List<RaftReadState> reads = _readStates;
        _readStates = [];
        var ready = new RaftReady(
            hs != _prevHardState ? hs : null,
            _log.UnstableEntries(),
            _log.NextCommittedEntries(),
            msgs,
            snapshot?.Meta,
            snapshot?.Data ?? ReadOnlyMemory<byte>.Empty,
            reads,
            Role,
            _lead);
        _outstanding = ready;
        return ready;
    }

    /// <summary>Acknowledges that a ready was persisted, sent and applied.</summary>
    /// <exception cref="InvalidOperationException">
    /// <paramref name="ready"/> is not the outstanding ready, or the durable log does not reflect it.
    /// </exception>
    public void Advance(RaftReady ready)
    {
        ArgumentNullException.ThrowIfNull(ready);
        if (!ReferenceEquals(ready, _outstanding))
            throw new InvalidOperationException("Advance must be called with the ready returned by the last GetReady.");
        _outstanding = null;

        if (ready.HardState is { } hs)
            _prevHardState = hs;

        ulong viewLast = _log.LastIndex;
        if (ready.SnapshotToInstall is { } meta)
        {
            if (_log.Stable.Snapshot.Index < meta.Index)
                throw new InvalidOperationException($"The host did not install the snapshot at index {meta.Index} (durable snapshot index {_log.Stable.Snapshot.Index}).");
            _log.StableSnapTo(meta.Index);
            _log.AppliedTo(meta.Index);
        }

        if (ready.EntriesToPersist.Count > 0)
        {
            RaftEntry last = ready.EntriesToPersist[^1];
            VerifyPersisted(last);
            _log.StableTo(last.Index, last.Term);
        }

        if (_log.LastIndex > viewLast)
        {
            // The host kept durable entries beyond what this node had in view (after a snapshot whose last entry it holds
            // with the same term, or a write whose entries all matched a longer durable suffix). They are part of the log
            // again, so their configuration changes take effect again, exactly as a restart from this log replays them.
            List<RaftEntry>? revealed = _log.Slice(viewLast + 1, _log.LastIndex + 1, 0);
            if (revealed is { Count: > 0 })
                OnEntriesAppended(viewLast + 1, revealed);
        }

        if (ready.CommittedEntries.Count > 0)
            _log.AppliedTo(ready.CommittedEntries[^1].Index);

        _prevRole = ready.Role;
        _prevLeader = ready.Leader;
    }

    /// <summary>Reports the result of sending a snapshot to <paramref name="peer"/> through a host-side channel.</summary>
    public void ReportSnapshotStatus(NodeId peer, bool succeeded)
    {
        if (_state != NodeState.Leader || !_progress.TryGetValue(peer, out RaftProgress? pr) || pr.State != RaftProgressState.Snapshot)
            return;
        if (!succeeded)
            pr.ClearPendingSnapshot();
        pr.BecomeProbe();
        pr.ProbeSent = true; // wait for the next heartbeat response before probing
    }

    /// <summary>Reports that <paramref name="peer"/> could not be reached (pauses optimistic replication).</summary>
    public void ReportUnreachable(NodeId peer)
    {
        if (_state != NodeState.Leader || !_progress.TryGetValue(peer, out RaftProgress? pr))
            return;
        if (pr.State == RaftProgressState.Replicate)
            pr.BecomeProbe();
    }

    /// <summary>Leader-side replication state of every other member (empty when not leader), in membership order.</summary>
    public IReadOnlyList<RaftPeerProgress> GetProgress()
    {
        if (_state != NodeState.Leader)
            return [];
        var result = new List<RaftPeerProgress>();
        foreach (NodeId id in Members())
        {
            if (id != _self && _progress.TryGetValue(id, out RaftProgress? pr))
                result.Add(pr.ToPublic(_conf.IsLearner(id)));
        }

        return result;
    }

    private void VerifyPersisted(RaftEntry last)
    {
        IRaftLogView stable = _log.Stable;
        if (stable.LastIndex < last.Index)
            throw new InvalidOperationException($"The host did not persist entry {last.Index} (durable last index {stable.LastIndex}).");
        if (last.Index <= stable.Snapshot.Index)
            return; // already compacted into a snapshot by the host
        ulong term;
        try
        {
            term = stable.TermAt(last.Index);
        }
        catch (ArgumentOutOfRangeException ex)
        {
            throw new InvalidOperationException($"The host did not persist entry {last.Index}.", ex);
        }

        if (term != last.Term)
            throw new InvalidOperationException($"The durable entry {last.Index} has term {term}, expected {last.Term}.");
    }

    private void Send(RaftMessage message) => _msgs.Add(message);

    /// <summary>Voters then learners, in configuration order (deterministic iteration).</summary>
    private IEnumerable<NodeId> Members()
    {
        foreach (NodeId v in _conf.Voters)
            yield return v;
        foreach (NodeId l in _conf.Learners)
            yield return l;
    }

    private sealed class ReadRequest(ulong seq, ulong index, ulong context)
    {
        public ulong Seq { get; } = seq;
        public ulong Index { get; } = index;
        public ulong Context { get; } = context;
        public HashSet<NodeId> Acks { get; } = [];
    }

    private sealed class IncomingSnapshot(ulong term, RaftSnapshotMeta meta)
    {
        public ulong Term { get; } = term;
        public RaftSnapshotMeta Meta { get; } = meta;
        public MemoryStream Buffer { get; } = new();
    }
}
