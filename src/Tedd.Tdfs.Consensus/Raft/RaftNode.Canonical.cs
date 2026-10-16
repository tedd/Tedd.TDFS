using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace Tedd.Tdfs.Consensus;

public sealed partial class RaftNode
{
    /// <summary>
    /// A canonical description of every part of this core's in-memory state that can influence its future behaviour or its
    /// output: term, vote, leader, role, election/heartbeat timers and the drawn election timeout, membership history,
    /// leader progress (including in-flight windows), campaign votes, read-index state, a partially received snapshot,
    /// output not yet handed to the host, the last ready's bookkeeping and the volatile part of the log (unstable tail,
    /// received snapshot, commit and applied indexes). Two cores with the same configuration, the same description, the same
    /// durable log (<see cref="IRaftLogView"/>, owned by the host) and the same future random draws behave identically on
    /// every input sequence, which lets a bounded model checker merge schedules that reach them (CON-001).
    /// </summary>
    /// <param name="abstractElectionTimer">See remarks.</param>
    /// <param name="name">
    /// How node ids are written (default: the id itself). Collections keyed by node id are ordered by their written form, so
    /// a model checker that names nodes by their position in a canonical order describes symmetric states identically.
    /// </param>
    /// <remarks>
    /// <para>Not used on the request path; the random source is the host's and is described by the host.</para>
    /// <para>With <paramref name="abstractElectionTimer"/> the election timer is described only as far as anything but the
    /// election timeout itself depends on it: the elapsed ticks capped at <see cref="RaftConfig.ElectionTicksMin"/> (the
    /// CheckQuorum interval, the leader lease and the recent-leader check compare against it), without the drawn
    /// randomized timeout. That is exact for a host that ticks a non-leader only until it starts campaigning (or a bounded
    /// number of ticks when it cannot campaign): the randomized timeout then only decides how many ticks that takes, and
    /// the campaign resets the timer. The host must then also not describe its random source.</para>
    /// <para>The core never orders node ids (iterating over peers only decides the order of the messages it emits, which
    /// a host may deliver in any order), so renaming nodes consistently maps behaviour onto behaviour.</para>
    /// </remarks>
    internal string DescribeCanonicalState(bool abstractElectionTimer = false, Func<NodeId, string>? name = null)
    {
        Func<NodeId, string> namer = name ?? DefaultName;
        var sb = new StringBuilder(256);
        IFormatProvider inv = CultureInfo.InvariantCulture;
        sb.Append(inv, $"t={_term};v={Id(_vote)};l={Id(_lead)};s={_state};he={_heartbeatElapsed}");
        if (abstractElectionTimer)
            sb.Append(inv, $";eem={Math.Min(_electionElapsed, _cfg.ElectionTicksMin)}");
        else
            sb.Append(inv, $";ee={_electionElapsed};ret={_randomizedElectionTimeout}");
        if (_reachableQuorum)
        {
            sb.Append(inv, $";tc={_tickCount};heard=");
            AppendSorted(sb, _lastHeard.Select(h => string.Create(inv, $"{namer(h.Key)}:{h.Value}")));
        }

        sb.Append(";cb=").Append(DescribeConf(_confBase, namer)).Append(inv, $"@{_confBaseIndex};ch=");
        foreach ((ulong index, RaftConfState conf) in _confHistory)
            sb.Append(inv, $"{index}:").Append(DescribeConf(conf, namer)).Append(',');
        sb.Append(";c=").Append(DescribeConf(_conf, namer)).Append(inv, $";pci={_pendingConfIndex};lt={Id(_leadTransferee)};pr=");
        AppendSorted(sb, _progress.Values.Select(p => p.DescribeCanonicalState(namer)));
        sb.Append(";votes=");
        AppendSorted(sb, _votes.Select(v => string.Create(inv, $"{namer(v.Key)}:{v.Value}")));
        sb.Append(inv, $";rs={_readSeq};rq=");
        foreach (ReadRequest read in _readQueue)
            sb.Append(inv, $"{read.Seq}/{read.Index}/{read.Context}/[{string.Join(" ", read.Acks.Select(namer).Order(StringComparer.Ordinal))}],");
        sb.Append(";dr=").Append(string.Join(",", _deferredReads));
        sb.Append(";rst=");
        foreach (RaftReadState state in _readStates)
            sb.Append(inv, $"{state.Index}/{state.RequestContext},");
        if (_incoming is { } incoming)
        {
            sb.Append(inv, $";in={incoming.Term}/{incoming.Meta.Index}@{incoming.Meta.Term}/").Append(DescribeConf(incoming.Meta.ConfState, namer))
                .Append(inv, $"/{incoming.Buffer.Length}/")
                .Append(Convert.ToHexString(SHA256.HashData(incoming.Buffer.GetBuffer().AsSpan(0, (int)incoming.Buffer.Length))));
        }

        sb.Append(inv, $";msgs={_msgs.Count}:");
        foreach (RaftMessage message in _msgs)
            sb.Append(DescribeMessage(message, namer)).Append(',');
        sb.Append(inv, $";phs={_prevHardState.Term}/{Id(_prevHardState.Vote)}/{_prevHardState.Commit};prole={_prevRole};plead={Id(_prevLeader)};out={_outstanding is not null}");
        sb.Append(inv, $";log={_log.Committed}/{_log.Applied}/{_log.UnstableOffset}:");
        foreach (RaftEntry entry in _log.UnstableEntries())
            sb.Append(DescribeEntry(entry, namer)).Append(',');
        if (_log.PendingSnapshot is { } snapshot)
        {
            sb.Append(inv, $";snap={snapshot.Meta.Index}@{snapshot.Meta.Term}/").Append(DescribeConf(snapshot.Meta.ConfState, namer)).Append('/')
                .Append(Convert.ToHexString(SHA256.HashData(snapshot.Data.Span)));
        }

        return sb.ToString();

        string Id(NodeId? id) => id is { } value ? namer(value) : "-";
    }

    /// <summary>
    /// True when <paramref name="message"/> carries a term below this core's. Delivering it then changes nothing in this
    /// core, now or in any later state (terms never decrease): the core at most answers the sender with its own term at that
    /// time (an append answer to a leader message, a rejection to a vote request) so that a stale sender catches up. A
    /// bounded model checker uses this to recognise messages that can never have an effect again (CON-001). Conservative:
    /// false under the reachable-quorum mutation, which records every message.
    /// </summary>
    internal bool IsStale(RaftMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);
        return message.Term < _term && message.To == _self && !_reachableQuorum;
    }

    /// <summary>
    /// True when this core leads the term of <paramref name="message"/>, an answer of one of its followers, and delivering
    /// the message has no effect now and can have none in any state the core reaches while it still leads that term (once
    /// it stops leading the term, every answer of that term is ignored anyway). A bounded model checker may then discard
    /// the message: states that differ only by it behave identically (CON-001). Covered: an acknowledgement at or below the
    /// follower's known match, a rejection below it, and, when the leader's log can no longer grow
    /// (<paramref name="logFixed"/>: no proposal or membership change can be made any more), a rejection at the match of a
    /// fully replicated follower in replicate state and a heartbeat answer of such a follower. Under CheckQuorum, only once
    /// the host will never tick a leader again (<paramref name="leaderTicksOver"/>): its CheckQuorum interval, the only
    /// place that clears <c>RecentActive</c>, then never elapses again.
    /// </summary>
    /// <remarks>
    /// <para>This follows from the leader's handling of answers (<c>HandleAppendResponse</c>, <c>HandleHeartbeatResponse</c>):
    /// such an answer only sets <c>RecentActive</c> (already set) and finds nothing to update, so it changes nothing and sends
    /// nothing. It stays so because, while the core leads the term, a follower's <c>Match</c> never decreases,
    /// <c>Next &gt; Match</c> always holds, <c>RecentActive</c> is cleared only by a CheckQuorum interval elapsing, and a fully
    /// replicated follower of a log that cannot grow never leaves replicate state, never has a probe or append in flight and
    /// is never sent entries again.</para>
    /// <para>Conservative: false under CheckQuorum while the leader may still be ticked, under the reachable-quorum mutation
    /// (every message is recorded), for read-index answers, and when the follower has no progress entry. The caller must
    /// not remove a member and add it back while this core leads the term (its progress would restart from zero); the
    /// CON-001 model has no such action.</para>
    /// </remarks>
    internal bool IsSpentLeaderAnswer(RaftMessage message, bool logFixed, bool leaderTicksOver)
    {
        ArgumentNullException.ThrowIfNull(message);
        if (_state != NodeState.Leader || message.Term != _term || message.To != _self || (_cfg.CheckQuorum && !leaderTicksOver) || _reachableQuorum)
            return false;
        if (!_progress.TryGetValue(message.From, out RaftProgress? pr) || !pr.RecentActive)
            return false;
        bool replicated = logFixed && pr.State == RaftProgressState.Replicate && pr.Match == _log.LastIndex;
        return message switch
        {
            AppendResponse { Reject: false } ack => ack.ReadContext == 0 && ack.MatchIndex <= pr.Match,
            AppendResponse { Reject: true } reject => reject.ReadContext == 0 && (reject.MatchIndex < pr.Match || (replicated && reject.MatchIndex <= pr.Match)),
            HeartbeatResponse answer => answer.ReadContext == 0 && replicated && !pr.ProbeSent && !pr.InflightsFull,
            _ => false,
        };
    }

    /// <summary>
    /// True when this core follows the sender of <paramref name="message"/>, a leader message of this core's term, and
    /// delivering it changes nothing here: the election timer is already reset, the sender is already the known leader, no
    /// snapshot is being received, and the log already holds every entry the message carries while the commit index
    /// already covers what it would commit. The core then only answers, with an append acknowledgement of at most its last
    /// index or a heartbeat answer. A bounded model checker combines this with the leader's
    /// <see cref="IsFullyReplicatedTo"/> to recognise a message that can never have an effect again (CON-001).
    /// Conservative: false for read-index traffic, snapshots, a log mismatch and under the reachable-quorum mutation.
    /// </summary>
    internal bool IsRedundantLeaderMessage(RaftMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);
        if (_state != NodeState.Follower || message.Term != _term || message.To != _self || _lead != message.From || _electionElapsed != 0
            || _incoming is not null || _reachableQuorum)
        {
            return false;
        }

        switch (message)
        {
            case Heartbeat heartbeat:
                return heartbeat.ReadContext == 0 && heartbeat.Commit <= _log.Committed;
            case AppendEntries append:
                if (append.ReadContext != 0 || !IsWellFormedMessage(append))
                    return false;
                if (append.PrevLogIndex < _log.Committed)
                    return true; // answered with the commit index, nothing else happens
                IReadOnlyList<RaftEntry> entries = append.Entries ?? [];
                ulong lastNew = append.PrevLogIndex + (ulong)entries.Count;
                return _log.MatchTerm(append.PrevLogIndex, append.PrevLogTerm) && FindConflict(entries) < 0
                    && Math.Min(append.Commit, lastNew) <= _log.Committed;
            default:
                return false;
        }
    }

    /// <summary>
    /// True when this core leads <paramref name="term"/> and <paramref name="follower"/> is fully replicated in a way that
    /// lasts: its log can no longer grow (<paramref name="logFixed"/>), the follower's acknowledged match is the whole log in
    /// replicate state with no probe or append in flight, and it answered recently (under CheckQuorum only once the host will
    /// never tick a leader again, <paramref name="leaderTicksOver"/>). Every append acknowledgement (of at most the log's
    /// last index) and every heartbeat answer of that follower is then spent (<see cref="IsSpentLeaderAnswer"/>), now and for
    /// as long as this core leads the term (CON-001).
    /// </summary>
    internal bool IsFullyReplicatedTo(NodeId follower, ulong term, bool logFixed, bool leaderTicksOver)
    {
        if (_state != NodeState.Leader || _term != term || !logFixed || (_cfg.CheckQuorum && !leaderTicksOver) || _reachableQuorum)
            return false;
        return _progress.TryGetValue(follower, out RaftProgress? pr) && pr.RecentActive && pr.State == RaftProgressState.Replicate
            && pr.Match == _log.LastIndex && !pr.ProbeSent && !pr.InflightsFull;
    }

    /// <summary>
    /// Canonical text of a membership: its voters and its learners as sets (ids written by <paramref name="name"/>, sorted).
    /// The core uses a membership only as two sets (the list order merely decides the order of the messages it emits), so
    /// the order of the lists is not part of the state.
    /// </summary>
    internal static string DescribeConf(RaftConfState conf, Func<NodeId, string>? name = null)
    {
        ArgumentNullException.ThrowIfNull(conf);
        Func<NodeId, string> namer = name ?? DefaultName;
        return $"V[{string.Join(" ", conf.Voters.Select(namer).Order(StringComparer.Ordinal))}]L[{string.Join(" ", conf.Learners.Select(namer).Order(StringComparer.Ordinal))}]";
    }

    /// <summary>
    /// An independent copy of this core over <paramref name="log"/> (which must hold the same durable entries and snapshot as
    /// this core's log view) drawing from <paramref name="random"/>: every field described by
    /// <see cref="DescribeCanonicalState"/> is copied, so the copy behaves exactly like this core from here on. Used by
    /// bounded model checking to branch a state without replaying its schedule (CON-001).
    /// </summary>
    /// <exception cref="InvalidOperationException">A ready is outstanding (the host must advance it first).</exception>
    internal RaftNode Clone(IRaftLogView log, IRandomSource random)
    {
        ArgumentNullException.ThrowIfNull(log);
        ArgumentNullException.ThrowIfNull(random);
        if (_outstanding is not null)
            throw new InvalidOperationException("A core with an outstanding ready cannot be cloned.");
        return new RaftNode(this, log, random);
    }

    private RaftNode(RaftNode source, IRaftLogView log, IRandomSource random)
    {
        _cfg = source._cfg;
        _options = source._options;
        _random = random;
        _log = source._log.CloneOnto(log);
        _self = source._self;
        _reachableQuorum = source._reachableQuorum;
        _term = source._term;
        _vote = source._vote;
        _lead = source._lead;
        _state = source._state;
        _electionElapsed = source._electionElapsed;
        _heartbeatElapsed = source._heartbeatElapsed;
        _randomizedElectionTimeout = source._randomizedElectionTimeout;
        _tickCount = source._tickCount;
        _confBase = source._confBase;
        _confBaseIndex = source._confBaseIndex;
        _confHistory.AddRange(source._confHistory);
        _conf = source._conf;
        foreach (KeyValuePair<NodeId, RaftProgress> progress in source._progress)
            _progress[progress.Key] = progress.Value.Clone();
        _pendingConfIndex = source._pendingConfIndex;
        _leadTransferee = source._leadTransferee;
        foreach (KeyValuePair<NodeId, bool> vote in source._votes)
            _votes[vote.Key] = vote.Value;
        foreach (ReadRequest read in source._readQueue)
        {
            var copy = new ReadRequest(read.Seq, read.Index, read.Context);
            copy.Acks.UnionWith(read.Acks);
            _readQueue.Add(copy);
        }

        _deferredReads.AddRange(source._deferredReads);
        _readSeq = source._readSeq;
        _readStates = [.. source._readStates];
        foreach (KeyValuePair<NodeId, long> heard in source._lastHeard)
            _lastHeard[heard.Key] = heard.Value;
        if (source._incoming is { } incoming)
        {
            _incoming = new IncomingSnapshot(incoming.Term, incoming.Meta);
            _incoming.Buffer.Write(incoming.Buffer.GetBuffer(), 0, (int)incoming.Buffer.Length);
        }

        _msgs = [.. source._msgs];
        _prevHardState = source._prevHardState;
        _prevRole = source._prevRole;
        _prevLeader = source._prevLeader;
    }

    /// <summary>
    /// Canonical text of an entry (index, term, kind and payload; a membership change is written as its kind and node, the
    /// node written by <paramref name="name"/>).
    /// </summary>
    internal static string DescribeEntry(RaftEntry entry, Func<NodeId, string>? name = null)
    {
        ArgumentNullException.ThrowIfNull(entry);
        if (entry.Kind == RaftEntryKind.ConfChange && RaftConfChange.TryDecode(entry.Data.Span, out RaftConfChange? change))
            return string.Create(CultureInfo.InvariantCulture, $"{entry.Index}@{entry.Term}:{entry.Kind}:{change.Kind}:{(name ?? DefaultName)(change.Node)}");
        return string.Create(CultureInfo.InvariantCulture, $"{entry.Index}@{entry.Term}:{entry.Kind}:{Convert.ToHexString(entry.Data.Span)}");
    }

    /// <summary>Canonical text of a message (every field; node ids written by <paramref name="name"/>).</summary>
    internal static string DescribeMessage(RaftMessage message, Func<NodeId, string>? name = null)
    {
        ArgumentNullException.ThrowIfNull(message);
        Func<NodeId, string> namer = name ?? DefaultName;
        IFormatProvider inv = CultureInfo.InvariantCulture;
        string head = string.Create(inv, $"{namer(message.From)}->{namer(message.To)} t{message.Term}");
        return message switch
        {
            VoteRequest m => string.Create(inv, $"VoteReq {head} pv={m.PreVote} last={m.LastLogIndex}@{m.LastLogTerm} xfer={m.LeadershipTransfer}"),
            VoteResponse m => string.Create(inv, $"VoteResp {head} pv={m.PreVote} reject={m.Reject}"),
            AppendEntries m => string.Create(inv,
                $"Append {head} prev={m.PrevLogIndex}@{m.PrevLogTerm} commit={m.Commit} ctx={m.ReadContext} entries=[{string.Join(",", m.Entries.Select(e => DescribeEntry(e, namer)))}]"),
            AppendResponse m => string.Create(inv, $"AppendResp {head} reject={m.Reject} match={m.MatchIndex} hint={m.RejectHint}@{m.RejectHintTerm} ctx={m.ReadContext}"),
            Heartbeat m => string.Create(inv, $"Heartbeat {head} commit={m.Commit} ctx={m.ReadContext}"),
            HeartbeatResponse m => string.Create(inv, $"HeartbeatResp {head} ctx={m.ReadContext}"),
            TimeoutNow => "TimeoutNow " + head,
            InstallSnapshot m => string.Create(inv,
                $"Snapshot {head} meta={m.Meta.Index}@{m.Meta.Term}/{DescribeConf(m.Meta.ConfState, namer)} off={m.Offset} len={m.Data.Length} hash={Convert.ToHexString(SHA256.HashData(m.Data.Span))} done={m.Done}"),
            InstallSnapshotResponse m => string.Create(inv, $"SnapshotResp {head} idx={m.SnapshotIndex} next={m.NextOffset} reject={m.Reject}"),
            _ => throw new ArgumentException($"Unknown message type {message.GetType().Name}.", nameof(message)),
        };
    }

    private static string DefaultName(NodeId id) => id.ToString();

    private static void AppendSorted(StringBuilder sb, IEnumerable<string> items)
    {
        foreach (string item in items.Order(StringComparer.Ordinal))
            sb.Append(item).Append(',');
    }
}
