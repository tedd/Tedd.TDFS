namespace Tedd.Tdfs.Consensus;

public sealed partial class RaftNode
{
    /// <summary>
    /// Processes an inbound message. Malformed messages and messages not addressed to this node are ignored (before any
    /// state change, so they cannot raise the term). Messages from nodes outside the latest membership are ignored too,
    /// except leader-originated replication messages (a lagging node must accept the log from a leader it does not know
    /// yet), <see cref="TimeoutNow"/> from the leader this node follows (the hand-over of a leader that committed its own
    /// removal) and vote requests while no leader has been heard from within the minimum election timeout.
    /// </summary>
    /// <remarks>
    /// Vote requests from non-members are processed unless a leader was heard from recently because this node's
    /// membership may lag behind the candidate's (Raft dissertation §4.1: servers vote for candidates outside their
    /// configuration); without that, a newly promoted voter can be the only electable node yet be ignored by peers that
    /// have not seen its promotion, and a lagging node that still remembers a departed leader would ignore every candidate
    /// it does not know. While the leader is live they are ignored, so removed servers cannot disrupt a healthy group
    /// (§4.2.3). Either way safety is unaffected: ignoring a message is equivalent to losing it.
    /// </remarks>
    public void Step(RaftMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);
        RaftMessage m = message;
        if (m.To != _self || m.From == _self || m.From.IsEmpty || m.Term == 0)
            return;
        if (!IsWellFormedMessage(m))
            return; // validated before any state change: a malformed message must not even raise the term

        bool leaderMessage = m is AppendEntries or Heartbeat or InstallSnapshot;

        // A leader that committed its own removal hands over with TimeoutNow; by then the target's membership no longer
        // lists it, so accept TimeoutNow from the leader this node follows even when it is not a member any more.
        bool fromOwnLeader = m is TimeoutNow && _lead is { } lead && m.From == lead;
        if (!leaderMessage && !fromOwnLeader && !_conf.Contains(m.From) && !(m is VoteRequest && !HeardFromLeaderRecently))
            return; // removed or unknown peers cannot influence a group with a live leader or its bookkeeping

        if (_reachableQuorum)
            _lastHeard[m.From] = _tickCount;

        if (m.Term > _term)
        {
            if (m is VoteRequest vr)
            {
                bool inLease = _cfg.CheckQuorum && _lead is not null && _electionElapsed < _cfg.ElectionTicksMin;
                if (inLease && !vr.LeadershipTransfer)
                    return; // a leader is known to be alive: do not let this candidate disrupt it
                if (!vr.PreVote)
                    BecomeFollower(m.Term, null);
            }
            else if (m is VoteResponse { PreVote: true, Reject: false })
            {
                // A granted pre-vote carries the future term; it does not change ours.
            }
            else
            {
                BecomeFollower(m.Term, leaderMessage ? m.From : null);
            }
        }
        else if (m.Term < _term)
        {
            // Answer with the newer term (Raft §5.1) so the sender catches up: a stale leader steps down, and a stale
            // candidate adopts our term. Without this, a node whose only reachable peers ignore it (for example a learner
            // whose term was raised by a removed node, or a voter that campaigns below everyone else's term while the
            // others cannot win) may never learn the current term and the group can stall.
            if (leaderMessage)
                Send(new AppendResponse(_self, m.From, _term, Reject: false, MatchIndex: 0, RejectHint: 0));
            else if (m is VoteRequest stale)
                Send(new VoteResponse(_self, m.From, _term, stale.PreVote, Reject: true));

            return;
        }

        if (m is VoteRequest request)
        {
            HandleVoteRequest(request);
            return;
        }

        switch (_state)
        {
            case NodeState.Leader:
                StepLeader(m);
                break;
            case NodeState.Candidate:
            case NodeState.PreCandidate:
                StepCandidate(m);
                break;
            default:
                StepFollower(m);
                break;
        }
    }

    /// <summary>
    /// True when a leader is known and was heard from within the minimum election timeout (a leader counts itself). Vote
    /// requests from nodes outside this node's membership are ignored only then (Raft dissertation §4.2.3): a lagging
    /// node may know neither its new voters nor that its old leader is gone, and if it ignored candidates for as long as
    /// it remembered a leader, a group that needs its vote could stay leaderless forever (a learner never campaigns, so
    /// nothing would ever clear that memory).
    /// </summary>
    private bool HeardFromLeaderRecently
        => _lead is not null && (_state == NodeState.Leader || _electionElapsed < _cfg.ElectionTicksMin);

    /// <summary>
    /// True when this node leads, or knows a leader it heard from within the minimum election timeout (host use: a
    /// follower or learner that has such recent contact is not cut off from its leader).
    /// </summary>
    internal bool HasRecentLeaderContact => HeardFromLeaderRecently;

    /// <summary>Structural validation of fields whose violation no correct peer can produce.</summary>
    private static bool IsWellFormedMessage(RaftMessage m) => m switch
    {
        AppendEntries ae => IsWellFormed(ae, ae.Entries ?? []),
        InstallSnapshot snap => snap.Meta is { ConfState: not null } meta && meta.Term <= snap.Term,
        VoteRequest vr => vr.LastLogTerm <= vr.Term,
        _ => true,
    };

    private void HandleVoteRequest(VoteRequest m)
    {
        if (_rebuilding)
        {
            // A member that lost its durable state cannot know whom it voted for or what it acknowledged.
            Send(new VoteResponse(_self, m.From, _term, m.PreVote, Reject: true));
            return;
        }

        bool canVote = _vote == m.From
            || (_vote is null && _lead is null)
            || (m.PreVote && m.Term > _term);
        if (canVote && _log.IsUpToDate(m.LastLogIndex, m.LastLogTerm))
        {
            Send(new VoteResponse(_self, m.From, m.Term, m.PreVote, Reject: false));
            if (!m.PreVote)
            {
                _electionElapsed = 0;
                _vote = m.From;
            }
        }
        else
        {
            Send(new VoteResponse(_self, m.From, _term, m.PreVote, Reject: true));
        }
    }

    private void StepFollower(RaftMessage m)
    {
        switch (m)
        {
            case AppendEntries ae:
                _electionElapsed = 0;
                _lead = m.From;
                HandleAppendEntries(ae);
                NoteRebuildProgress(ae.Commit);
                break;
            case Heartbeat hb:
                _electionElapsed = 0;
                _lead = m.From;
                HandleHeartbeat(hb);
                NoteRebuildProgress(0);
                break;
            case InstallSnapshot snap:
                _electionElapsed = 0;
                _lead = m.From;
                HandleInstallSnapshot(snap);
                break;
            case TimeoutNow:
                if (Promotable)
                    Campaign(preVote: false, transfer: true);
                break;
        }
    }

    private void StepCandidate(RaftMessage m)
    {
        switch (m)
        {
            case AppendEntries ae:
                BecomeFollower(_term, m.From);
                HandleAppendEntries(ae);
                break;
            case Heartbeat hb:
                BecomeFollower(_term, m.From);
                HandleHeartbeat(hb);
                break;
            case InstallSnapshot snap:
                BecomeFollower(_term, m.From);
                HandleInstallSnapshot(snap);
                break;
            case VoteResponse vr:
                bool relevant = _state == NodeState.PreCandidate ? vr.PreVote : !vr.PreVote;
                if (!relevant || !_conf.IsVoter(vr.From))
                    return;
                _votes[vr.From] = !vr.Reject;
                switch (TallyVotes())
                {
                    case VoteResult.Won:
                        if (_state == NodeState.PreCandidate)
                        {
                            Campaign(preVote: false, transfer: false);
                        }
                        else
                        {
                            BecomeLeader();
                            BroadcastAppend();
                        }

                        break;
                    case VoteResult.Lost:
                        BecomeFollower(_term, null);
                        break;
                }

                break;
        }
    }

    private void TickElection()
    {
        if (_electionElapsed < int.MaxValue)
            _electionElapsed++;
        if (Promotable && _electionElapsed >= _randomizedElectionTimeout)
        {
            _electionElapsed = 0;
            Campaign(preVote: _cfg.PreVote, transfer: false);
        }
    }

    private void TickHeartbeat()
    {
        _heartbeatElapsed++;
        if (_electionElapsed < int.MaxValue)
            _electionElapsed++;

        if (_electionElapsed >= _cfg.ElectionTicksMin)
        {
            _electionElapsed = 0;
            if (_cfg.CheckQuorum)
            {
                if (!QuorumActive())
                {
                    BecomeFollower(_term, null);
                    return;
                }

                foreach (RaftProgress pr in _progress.Values)
                    pr.RecentActive = false;
            }

            if (_leadTransferee is not null)
                _leadTransferee = null; // the transfer did not complete within an election timeout
        }

        if (_heartbeatElapsed >= _cfg.HeartbeatTicks)
        {
            _heartbeatElapsed = 0;

            // Carry the newest pending read round so that a lost confirmation round (heartbeat or response) is retried:
            // a response to it confirms every request registered before it. Otherwise pending reads would wait forever
            // for a new ReadIndex call.
            BroadcastHeartbeat(_readQueue.Count > 0 ? _readQueue[^1].Seq : 0);
        }
    }

    private void Campaign(bool preVote, bool transfer)
    {
        if (_state == NodeState.Leader || !Promotable)
            return;

        ulong voteTerm;
        if (preVote)
        {
            BecomePreCandidate();
            voteTerm = _term + 1;
        }
        else
        {
            BecomeCandidate();
            voteTerm = _term;
        }

        _votes[_self] = true;
        if (TallyVotes() == VoteResult.Won)
        {
            if (preVote)
            {
                Campaign(preVote: false, transfer: transfer);
            }
            else
            {
                BecomeLeader();
                BroadcastAppend();
            }

            return;
        }

        ulong lastIndex = _log.LastIndex;
        ulong lastTerm = _log.LastTerm;
        foreach (NodeId voter in _conf.Voters)
        {
            if (voter != _self)
                Send(new VoteRequest(_self, voter, voteTerm, preVote, lastIndex, lastTerm, transfer));
        }
    }

    private enum VoteResult
    {
        Pending,
        Won,
        Lost,
    }

    private VoteResult TallyVotes()
    {
        int granted = 0;
        int rejected = 0;
        foreach (NodeId voter in _conf.Voters)
        {
            if (_votes.TryGetValue(voter, out bool g))
            {
                if (g)
                    granted++;
                else
                    rejected++;
            }
        }

        int quorum = QuorumSize();
        if (granted >= quorum)
            return VoteResult.Won;
        if (_conf.Voters.Count - rejected < quorum)
            return VoteResult.Lost;
        return VoteResult.Pending;
    }

    private void Reset(ulong term)
    {
        if (_term != term)
        {
            _term = term;
            _vote = null;
            _incoming = null; // a partial snapshot from a previous term's leader can never be completed
        }

        _lead = null;
        _electionElapsed = 0;
        _heartbeatElapsed = 0;
        ResetRandomizedElectionTimeout();
        _leadTransferee = null;
        _votes.Clear();
        _progress.Clear();
        _readQueue.Clear();
        _deferredReads.Clear();
        _pendingConfIndex = 0;
    }

    /// <summary>Draws the election timeout uniformly from [ElectionTicksMin, ElectionTicksMax] (both inclusive).</summary>
    private void ResetRandomizedElectionTimeout()
        => _randomizedElectionTimeout = _cfg.ElectionTicksMin + _random.Next(_cfg.ElectionTicksMax - _cfg.ElectionTicksMin + 1);

    private void BecomeFollower(ulong term, NodeId? lead)
    {
        Reset(term);
        _state = NodeState.Follower;
        _lead = lead;
    }

    private void BecomePreCandidate()
    {
        // A pre-candidate keeps its term and vote; it only probes whether an election could succeed.
        _state = NodeState.PreCandidate;
        _lead = null;
        _votes.Clear();
        _progress.Clear();
        _readQueue.Clear();
        _deferredReads.Clear();
        _leadTransferee = null;
        _electionElapsed = 0;
        ResetRandomizedElectionTimeout();
    }

    private void BecomeCandidate()
    {
        Reset(_term + 1);
        _vote = _self;
        _state = NodeState.Candidate;
    }

    private void BecomeLeader()
    {
        Reset(_term);
        _state = NodeState.Leader;
        _lead = _self;
        ulong next = _log.LastIndex + 1;
        foreach (NodeId id in Members())
        {
            if (id != _self)
                _progress[id] = new RaftProgress(id, next, _cfg.MaxInflightAppends, recentActive: false);
        }

        // A configuration takes effect when appended, so this leader may append a change only after an entry of its own
        // term has been committed under the configuration it inherited (the fix for the single-server membership change
        // bug, Ongaro, raft-dev 2015). Otherwise a leader that inherits a fully committed log could append and commit a
        // change with a quorum of the new configuration while an older leader's uncommitted change elects a rival leader
        // with a quorum of its configuration, and both commit different entries at the same index. Treating the no-op as
        // the pending change also covers any configuration entry in the uncommitted tail.
        _pendingConfIndex = _log.LastIndex + 1;
        AppendLeaderEntry(RaftEntryKind.Noop, ReadOnlyMemory<byte>.Empty);
    }

    /// <summary>Ticks within which a peer counts as reachable for the <see cref="SafetyMutations.QuorumFromReachableNodes"/> mutation.</summary>
    private int ReachabilityWindow => 2 * _cfg.ElectionTicksMax;

    /// <summary>Majority size: of the configured voters, or (negative control) of the voters heard from recently.</summary>
    private int QuorumSize()
    {
        IReadOnlyList<NodeId> voters = _conf.Voters;
        if (!_reachableQuorum)
            return (voters.Count / 2) + 1;
        int reachable = 0;
        foreach (NodeId v in voters)
        {
            if (v == _self || (_lastHeard.TryGetValue(v, out long at) && _tickCount - at <= ReachabilityWindow))
                reachable++;
        }

        return (reachable / 2) + 1;
    }

    /// <summary>True when a quorum of voters (counting this node) was active during the last CheckQuorum interval.</summary>
    private bool QuorumActive()
    {
        int active = 0;
        foreach (NodeId v in _conf.Voters)
        {
            if (v == _self || (_progress.TryGetValue(v, out RaftProgress? pr) && pr.RecentActive))
                active++;
        }

        return active >= QuorumSize();
    }
}
