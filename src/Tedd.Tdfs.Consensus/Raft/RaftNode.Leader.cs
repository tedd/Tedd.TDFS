namespace Tedd.Tdfs.Consensus;

public sealed partial class RaftNode
{
    /// <summary>True when the commit index covers an entry of the current term (Raft §5.4.2; required for ReadIndex).</summary>
    private bool CommittedInCurrentTerm => _log.TryTerm(_log.Committed, out ulong t) && t == _term;

    private void StepLeader(RaftMessage m)
    {
        switch (m)
        {
            case AppendResponse ar:
                HandleAppendResponse(ar);
                break;
            case HeartbeatResponse hr:
                HandleHeartbeatResponse(hr);
                break;
            case InstallSnapshotResponse sr:
                HandleSnapshotResponse(sr);
                break;
        }

        // VoteResponse (stale), TimeoutNow and same-term leader messages cannot be valid for a leader: ignored.
    }

    /// <summary>Appends an entry to the leader's log and returns its index.</summary>
    private ulong AppendLeaderEntry(RaftEntryKind kind, ReadOnlyMemory<byte> data)
    {
        var entry = new RaftEntry(_log.LastIndex + 1, _term, kind, data);
        _log.Append(entry);
        if (kind == RaftEntryKind.ConfChange && RaftConfChange.TryDecode(data.Span, out RaftConfChange? change))
            ApplyConfEntry(entry.Index, change);
        MaybeCommit();
        return entry.Index;
    }

    /// <summary>Advances the commit index to the highest current-term index replicated on a quorum of voters.</summary>
    private bool MaybeCommit()
    {
        IReadOnlyList<NodeId> voters = _conf.Voters;
        if (voters.Count == 0)
            return false;
        var matches = new ulong[voters.Count];
        for (int i = 0; i < voters.Count; i++)
        {
            NodeId v = voters[i];
            matches[i] = v == _self
                ? _log.LastIndex
                : _progress.TryGetValue(v, out RaftProgress? pr) ? pr.Match : 0;
        }

        Array.Sort(matches);
        Array.Reverse(matches);
        int quorum = QuorumSize();
        if (quorum > matches.Length)
            return false;
        ulong candidate = matches[quorum - 1];
        if (candidate <= _log.Committed || !_log.TryTerm(candidate, out ulong term) || term != _term)
            return false;
        _log.CommitTo(candidate);
        OnCommitAdvanced();
        return true;
    }

    private void OnCommitAdvanced()
    {
        CompactConfHistory();
        if (_state != NodeState.Leader)
            return;

        if (_deferredReads.Count > 0 && CommittedInCurrentTerm)
        {
            ulong[] deferred = [.. _deferredReads];
            _deferredReads.Clear();
            StartReads(deferred);
        }

        if (!_conf.IsVoter(_self) && _log.Committed >= _pendingConfIndex)
        {
            // This leader's removal is committed: hand over to an up-to-date voter and step down.
            foreach (NodeId v in _conf.Voters)
            {
                if (_progress.TryGetValue(v, out RaftProgress? pr) && pr.Match == _log.LastIndex)
                {
                    SendTimeoutNow(v);
                    break;
                }
            }

            BecomeFollower(_term, null);
        }
    }

    private void BroadcastAppend()
    {
        if (_state != NodeState.Leader)
            return;
        foreach (NodeId id in Members())
        {
            if (id != _self && _progress.TryGetValue(id, out RaftProgress? pr))
                SendAppend(pr, sendIfEmpty: true);
        }
    }

    private void BroadcastHeartbeat(ulong readContext)
    {
        foreach (NodeId id in Members())
        {
            if (id == _self || !_progress.TryGetValue(id, out RaftProgress? pr))
                continue;
            ulong commit = Math.Min(pr.Match, _log.Committed);
            Send(new Heartbeat(_self, id, _term, commit, readContext));
        }
    }

    /// <summary>Sends the next append to <paramref name="pr"/> (or a snapshot when the entries are compacted).</summary>
    private bool SendAppend(RaftProgress pr, bool sendIfEmpty)
    {
        if (pr.IsPaused)
            return false;
        ulong prevIndex = pr.Next - 1;
        if (!_log.TryTerm(prevIndex, out ulong prevTerm))
            return SendSnapshot(pr);
        List<RaftEntry>? entries = _log.Slice(pr.Next, _log.LastIndex + 1, _cfg.MaxEntriesPerAppend);
        if (entries is null)
            return SendSnapshot(pr);
        if (entries.Count == 0 && !sendIfEmpty)
            return false;

        Send(new AppendEntries(_self, pr.Peer, _term, prevIndex, prevTerm, entries, _log.Committed));
        if (entries.Count > 0 && pr.State == RaftProgressState.Replicate)
        {
            ulong last = entries[^1].Index;
            pr.Next = last + 1;
            pr.AddInflight(last);
        }
        else if (pr.State == RaftProgressState.Probe)
        {
            pr.ProbeSent = true;
        }

        return true;
    }

    private bool SendSnapshot(RaftProgress pr)
    {
        if (!pr.RecentActive)
        {
            // Do not stream a snapshot to a peer that has not answered recently; retry once it responds.
            pr.ProbeSent = true;
            return false;
        }

        RaftSnapshot? snapshot = _options.SnapshotSource?.GetSnapshot();
        if (snapshot is null || snapshot.Meta is null || snapshot.Meta.ConfState is null
            || snapshot.Meta.Index + 1 < _log.FirstIndex || snapshot.Meta.Index > _log.LastIndex)
        {
            pr.ProbeSent = true; // unavailable or unusable right now; retry at the next heartbeat response
            return false;
        }

        pr.BecomeSnapshot(snapshot);
        SendSnapshotChunk(pr);
        return true;
    }

    private void SendSnapshotChunk(RaftProgress pr)
    {
        RaftSnapshot snapshot = pr.Snapshot!;
        int total = snapshot.Data.Length;
        long offset = Math.Clamp(pr.SnapshotOffset, 0, total);
        int length = (int)Math.Min(_options.SnapshotChunkBytes, total - offset);
        bool done = offset + length >= total;
        Send(new InstallSnapshot(_self, pr.Peer, _term, snapshot.Meta, offset, snapshot.Data.Slice((int)offset, length), done));
    }

    private void SendTimeoutNow(NodeId target) => Send(new TimeoutNow(_self, target, _term));

    private void HandleAppendResponse(AppendResponse m)
    {
        if (!_progress.TryGetValue(m.From, out RaftProgress? pr))
            return;
        pr.RecentActive = true;

        if (m.Reject)
        {
            ulong hint = m.RejectHint;
            if (m.RejectHintTerm > 0)
                hint = _log.FindConflictByTerm(m.RejectHint, m.RejectHintTerm).Index;
            if (pr.MaybeDecrTo(m.MatchIndex, hint))
            {
                if (pr.State == RaftProgressState.Replicate)
                    pr.BecomeProbe();
                SendAppend(pr, sendIfEmpty: true);
            }

            return;
        }

        if (m.MatchIndex > _log.LastIndex)
            return; // cannot match entries this leader does not have: malformed or misrouted

        bool wasPaused = pr.IsPaused;
        if (!pr.MaybeUpdate(m.MatchIndex))
            return;

        switch (pr.State)
        {
            case RaftProgressState.Probe:
                pr.BecomeReplicate();
                break;
            case RaftProgressState.Snapshot when pr.Match >= pr.PendingSnapshot:
                pr.BecomeProbe();
                pr.BecomeReplicate();
                break;
            case RaftProgressState.Replicate:
                pr.FreeInflightsUpTo(pr.Match);
                break;
        }

        if (MaybeCommit())
        {
            BroadcastAppend();
        }
        else if (wasPaused)
        {
            SendAppend(pr, sendIfEmpty: true);
        }

        if (_state != NodeState.Leader)
            return; // stepped down after committing its own removal

        while (SendAppend(pr, sendIfEmpty: false))
        {
        }

        if (_leadTransferee == m.From && pr.Match == _log.LastIndex)
            SendTimeoutNow(m.From);
    }

    private void HandleHeartbeatResponse(HeartbeatResponse m)
    {
        if (!_progress.TryGetValue(m.From, out RaftProgress? pr))
            return;
        pr.RecentActive = true;
        pr.ProbeSent = false;
        if (pr.State == RaftProgressState.Replicate && pr.InflightsFull)
            pr.FreeFirstInflight();

        if (pr.State == RaftProgressState.Snapshot)
            SendSnapshotChunk(pr); // retransmit the next expected chunk (it or its answer may have been lost)
        else if (pr.Match < _log.LastIndex)
            SendAppend(pr, sendIfEmpty: true);

        if (m.ReadContext != 0)
            HandleReadAck(m.From, m.ReadContext);
    }

    private void HandleSnapshotResponse(InstallSnapshotResponse m)
    {
        if (!_progress.TryGetValue(m.From, out RaftProgress? pr))
            return;
        pr.RecentActive = true;
        if (pr.State != RaftProgressState.Snapshot || pr.Snapshot is null || m.SnapshotIndex != pr.PendingSnapshot)
            return;

        int total = pr.Snapshot.Data.Length;
        if (m.Reject)
        {
            if (m.NextOffset < 0 || m.NextOffset > total)
            {
                // The follower refuses this snapshot; fall back to probing and retry later.
                pr.ClearPendingSnapshot();
                pr.BecomeProbe();
                pr.ProbeSent = true;
                return;
            }

            pr.SnapshotOffset = m.NextOffset;
            SendSnapshotChunk(pr);
            return;
        }

        if (m.NextOffset > pr.SnapshotOffset && m.NextOffset <= total)
        {
            pr.SnapshotOffset = m.NextOffset;
            if (pr.SnapshotOffset < total)
                SendSnapshotChunk(pr);
        }

        // Completion is signalled by the follower's AppendResponse after it installed the snapshot.
    }

    private void StartReads(IReadOnlyList<ulong> contexts)
    {
        if (contexts.Count == 0)
            return;
        bool selfVotes = _conf.IsVoter(_self);
        foreach (ulong context in contexts)
        {
            var request = new ReadRequest(++_readSeq, _log.Committed, context);
            if (selfVotes)
                request.Acks.Add(_self);
            _readQueue.Add(request);
        }

        ReleaseConfirmedReads();
        if (_readQueue.Count > 0)
            BroadcastHeartbeat(_readSeq);
    }

    private void HandleReadAck(NodeId from, ulong seq)
    {
        if (!_conf.IsVoter(from))
            return;
        bool any = false;
        foreach (ReadRequest request in _readQueue)
        {
            // A response to round `seq` proves leadership after every earlier request was registered.
            if (request.Seq > seq)
                break;
            request.Acks.Add(from);
            any = true;
        }

        if (any)
            ReleaseConfirmedReads();
    }

    private void ReleaseConfirmedReads()
    {
        int quorum = QuorumSize();
        int released = 0;
        foreach (ReadRequest request in _readQueue)
        {
            int acks = 0;
            foreach (NodeId v in _conf.Voters)
            {
                if (request.Acks.Contains(v))
                    acks++;
            }

            if (acks < quorum)
                break;
            _readStates.Add(new RaftReadState(request.Index, request.Context));
            released++;
        }

        if (released > 0)
            _readQueue.RemoveRange(0, released);
    }
}
