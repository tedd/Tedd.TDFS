namespace Tedd.Tdfs.Consensus;

public sealed partial class RaftNode
{
    private void HandleAppendEntries(AppendEntries m)
    {
        IReadOnlyList<RaftEntry> entries = m.Entries ?? [];
        if (!IsWellFormed(m, entries))
            return;

        if (m.PrevLogIndex < _log.Committed)
        {
            // Everything up to our commit index is known to match; let the leader skip ahead.
            Send(new AppendResponse(_self, m.From, _term, Reject: false, MatchIndex: _log.Committed, RejectHint: 0));
            return;
        }

        if (_log.MatchTerm(m.PrevLogIndex, m.PrevLogTerm))
        {
            ulong lastNew = m.PrevLogIndex + (ulong)entries.Count;
            int conflict = FindConflict(entries);
            if (conflict >= 0)
            {
                ulong conflictIndex = entries[conflict].Index;
                if (conflictIndex <= _log.Committed)
                    return; // would rewrite committed history: never happens with a correct leader
                RaftEntry[] tail = [.. entries.Skip(conflict)];
                _log.TruncateAndAppend(tail);
                OnEntriesAppended(conflictIndex, tail);
            }

            if (_log.CommitTo(Math.Min(m.Commit, lastNew)))
                OnCommitAdvanced();
            Send(new AppendResponse(_self, m.From, _term, Reject: false, MatchIndex: lastNew, RejectHint: 0));
            return;
        }

        ulong hintIndex = Math.Min(m.PrevLogIndex, _log.LastIndex);
        (ulong index, ulong term) = _log.FindConflictByTerm(hintIndex, m.PrevLogTerm);
        Send(new AppendResponse(_self, m.From, _term, Reject: true, MatchIndex: m.PrevLogIndex, RejectHint: index, RejectHintTerm: term));
    }

    /// <summary>Rejects structurally invalid appends (gaps, terms above the leader's, decreasing terms, unknown kinds).</summary>
    private static bool IsWellFormed(AppendEntries m, IReadOnlyList<RaftEntry> entries)
    {
        if (entries.Count > 0 && m.PrevLogIndex > ulong.MaxValue - (ulong)entries.Count)
            return false;
        ulong previousTerm = m.PrevLogTerm;
        if (m.PrevLogTerm > m.Term)
            return false;
        for (int i = 0; i < entries.Count; i++)
        {
            RaftEntry? e = entries[i];
            if (e is null
                || e.Index != m.PrevLogIndex + 1 + (ulong)i
                || e.Term > m.Term
                || e.Term < previousTerm
                || e.Term == 0
                || e.Kind is not (RaftEntryKind.Normal or RaftEntryKind.ConfChange or RaftEntryKind.Noop))
                return false;
            previousTerm = e.Term;
        }

        return true;
    }

    /// <summary>Position of the first entry that is missing locally or conflicts with a local entry; -1 when none.</summary>
    private int FindConflict(IReadOnlyList<RaftEntry> entries)
    {
        for (int i = 0; i < entries.Count; i++)
        {
            if (!_log.MatchTerm(entries[i].Index, entries[i].Term))
                return i;
        }

        return -1;
    }

    private void HandleHeartbeat(Heartbeat m)
    {
        // The leader only advertises a commit index up to what it knows we hold (min(match, commit)).
        if (_log.CommitTo(m.Commit))
            OnCommitAdvanced();
        Send(new HeartbeatResponse(_self, m.From, _term, m.ReadContext));
    }

    private void HandleInstallSnapshot(InstallSnapshot m)
    {
        RaftSnapshotMeta? meta = m.Meta;
        if (meta is null || meta.ConfState is null || meta.Term > m.Term)
            return;

        if (meta.Index <= _log.Committed)
        {
            _incoming = null;
            Send(new AppendResponse(_self, m.From, _term, Reject: false, MatchIndex: _log.Committed, RejectHint: 0));
            return;
        }

        if (m.Offset < 0)
        {
            RejectSnapshot(m, -1);
            return;
        }

        if (m.Offset == 0)
        {
            _incoming = new IncomingSnapshot(_term, meta);
        }
        else if (_incoming is null || _incoming.Term != _term || _incoming.Meta.Index != meta.Index || _incoming.Meta.Term != meta.Term)
        {
            RejectSnapshot(m, 0);
            return;
        }
        else if (m.Offset != _incoming.Buffer.Length)
        {
            if (m.Offset < _incoming.Buffer.Length)
                Send(new InstallSnapshotResponse(_self, m.From, _term, meta.Index, _incoming.Buffer.Length, Reject: false)); // duplicate
            else
                RejectSnapshot(m, _incoming.Buffer.Length);
            return;
        }

        if (_incoming.Buffer.Length + (long)m.Data.Length > _options.MaxSnapshotBytes)
        {
            _incoming = null;
            RejectSnapshot(m, -1);
            return;
        }

        _incoming.Buffer.Write(m.Data.Span);
        if (!m.Done)
        {
            Send(new InstallSnapshotResponse(_self, m.From, _term, meta.Index, _incoming.Buffer.Length, Reject: false));
            return;
        }

        var snapshot = new RaftSnapshot(meta, _incoming.Buffer.ToArray());
        _incoming = null;
        if (Restore(snapshot))
            Send(new AppendResponse(_self, m.From, _term, Reject: false, MatchIndex: _log.LastIndex, RejectHint: 0));
        else
            Send(new AppendResponse(_self, m.From, _term, Reject: false, MatchIndex: _log.Committed, RejectHint: 0));
    }

    private void RejectSnapshot(InstallSnapshot m, long nextOffset)
        => Send(new InstallSnapshotResponse(_self, m.From, _term, m.Meta.Index, nextOffset, Reject: true));

    /// <summary>Installs a received snapshot; false when it is not newer than our committed state.</summary>
    private bool Restore(RaftSnapshot snapshot)
    {
        RaftSnapshotMeta meta = snapshot.Meta;
        if (meta.Index <= _log.Committed)
            return false;
        if (_log.MatchTerm(meta.Index, meta.Term))
        {
            // We already hold the snapshot's last entry: just fast-forward the commit index.
            if (_log.CommitTo(meta.Index))
                OnCommitAdvanced();
            return false;
        }

        try
        {
            meta.ConfState.Validate();
        }
        catch (ArgumentException)
        {
            return false;
        }

        _log.Restore(snapshot);
        _confBase = meta.ConfState;
        _confBaseIndex = meta.Index;
        _confHistory.Clear();
        SetConf(meta.ConfState);
        return true;
    }

    /// <summary>Updates the membership after appending <paramref name="appended"/> starting at <paramref name="firstIndex"/>.</summary>
    private void OnEntriesAppended(ulong firstIndex, IReadOnlyList<RaftEntry> appended)
    {
        bool truncated = _confHistory.Count > 0 && _confHistory[^1].Index >= firstIndex;
        if (truncated)
        {
            _confHistory.RemoveAll(h => h.Index >= firstIndex);
            SetConf(_confHistory.Count > 0 ? _confHistory[^1].Conf : _confBase);
        }

        foreach (RaftEntry e in appended)
        {
            if (e.Kind == RaftEntryKind.ConfChange && RaftConfChange.TryDecode(e.Data.Span, out RaftConfChange? change))
                ApplyConfEntry(e.Index, change);
        }
    }

    /// <summary>Makes the configuration carried by the entry at <paramref name="index"/> effective (on append).</summary>
    private void ApplyConfEntry(ulong index, RaftConfChange change)
    {
        RaftConfState next = _conf.Apply(change);
        _confHistory.Add((index, next));
        SetConf(next);
    }

    private void SetConf(RaftConfState conf)
    {
        _conf = conf;
        if (_state != NodeState.Leader)
            return;

        foreach (NodeId id in _progress.Keys.ToArray())
        {
            if (!conf.Contains(id))
                _progress.Remove(id);
        }

        foreach (NodeId id in Members())
        {
            if (id != _self && !_progress.ContainsKey(id))
                _progress[id] = new RaftProgress(id, _log.LastIndex + 1, _cfg.MaxInflightAppends, recentActive: true);
        }

        if (_leadTransferee is { } t && !conf.IsVoter(t))
            _leadTransferee = null;
    }

    /// <summary>Folds configuration entries at or below the commit index into the base (they can no longer be truncated).</summary>
    private void CompactConfHistory()
    {
        int fold = 0;
        while (fold < _confHistory.Count && _confHistory[fold].Index <= _log.Committed)
            fold++;
        if (fold == 0)
            return;
        (_confBaseIndex, _confBase) = _confHistory[fold - 1];
        _confHistory.RemoveRange(0, fold);
    }

    /// <summary>Rebuilds the membership from configuration entries in the durable log (construction).</summary>
    private void ReplayConfEntries(IRaftLogView log)
    {
        const int batch = 1024;
        ulong next = log.FirstIndex;
        ulong last = log.LastIndex;
        while (next <= last)
        {
            IReadOnlyList<RaftEntry> entries = log.Entries(next, last + 1, batch);
            if (entries.Count == 0)
                throw new ArgumentException($"The log view returned no entries for [{next}, {last}].", nameof(log));
            foreach (RaftEntry e in entries)
            {
                if (e.Index != next)
                    throw new ArgumentException($"The log view returned entry {e.Index} where {next} was expected.", nameof(log));
                if (e.Kind == RaftEntryKind.ConfChange && RaftConfChange.TryDecode(e.Data.Span, out RaftConfChange? change))
                {
                    _conf = _conf.Apply(change);
                    _confHistory.Add((e.Index, _conf));
                }

                next++;
            }
        }

        CompactConfHistory();
    }
}
