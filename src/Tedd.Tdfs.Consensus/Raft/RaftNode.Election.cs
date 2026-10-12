namespace Tedd.Tdfs.Consensus;

public sealed partial class RaftNode
{
    /// <summary>
    /// Processes an inbound message. Malformed messages and messages not addressed to this node are ignored. Messages from
    /// nodes outside the latest membership are ignored too, except leader-originated replication messages (a lagging node
    /// must accept the log from a leader it does not know yet) and vote requests while no leader is known.
    /// </summary>
    /// <remarks>
    /// Vote requests from non-members are processed when no leader is known because this node's membership may lag
    /// behind the candidate's (Raft dissertation §4.1: servers vote for candidates outside their configuration); without
    /// that, a newly promoted voter can be the only electable node yet be ignored by peers that have not seen its
    /// promotion. While a leader is known they are ignored, so removed servers cannot disrupt a healthy group. Either way
    /// safety is unaffected: ignoring a message is equivalent to losing it.
    /// </remarks>
    public void Step(RaftMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);
        RaftMessage m = message;
        if (m.To != _self || m.From == _self || m.From.IsEmpty || m.Term == 0)
            return;

        bool leaderMessage = m is AppendEntries or Heartbeat or InstallSnapshot;
        if (!leaderMessage && !_conf.Contains(m.From) && !(m is VoteRequest && _lead is null))
            return; // removed or unknown peers cannot influence a group with a leader or its bookkeeping

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

    private void HandleVoteRequest(VoteRequest m)
    {
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
                break;
            case Heartbeat hb:
                _electionElapsed = 0;
                _lead = m.From;
                HandleHeartbeat(hb);
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
            BroadcastHeartbeat(0);
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

    private void ResetRandomizedElectionTimeout()
        => _randomizedElectionTimeout = _cfg.ElectionTicksMin + _random.Next(_cfg.ElectionTicksMax - _cfg.ElectionTicksMin);

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

        // Conservatively treat any configuration entry in the uncommitted tail as pending: no new change until all
        // entries up to here are committed.
        _pendingConfIndex = _log.LastIndex;
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
