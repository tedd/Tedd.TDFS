namespace Tedd.Tdfs.Consensus;

/// <summary>
/// The log as seen by a <see cref="RaftNode"/>: the host's durable <see cref="IRaftLogView"/> followed by an in-memory
/// unstable tail (entries not yet handed to and acknowledged by the host), plus an optional received snapshot that has
/// not been installed yet. Indices at or above <see cref="_offset"/> are answered from the unstable tail, which
/// supersedes any (conflicting) durable entries at those indices until the host persists the tail.
/// </summary>
internal sealed class RaftLog
{
    private readonly IRaftLogView _stable;
    private List<RaftEntry> _unstable = [];
    private ulong _offset;
    private RaftSnapshot? _pendingSnapshot;

    public RaftLog(IRaftLogView stable, ulong committed, ulong applied)
    {
        _stable = stable;
        _offset = stable.LastIndex + 1;
        Committed = committed;
        Applied = applied;
    }

    /// <summary>
    /// A copy of this log's volatile part (unstable tail, received snapshot, commit and applied indexes) over
    /// <paramref name="stable"/>, which must hold the same durable entries as <see cref="Stable"/> (model checking).
    /// </summary>
    public RaftLog CloneOnto(IRaftLogView stable) => new(stable, Committed, Applied)
    {
        _unstable = [.. _unstable],
        _offset = _offset,
        _pendingSnapshot = _pendingSnapshot,
    };

    /// <summary>Highest index known to be committed.</summary>
    public ulong Committed { get; private set; }

    /// <summary>Highest index applied by the host (advanced by <see cref="RaftNode.Advance"/>).</summary>
    public ulong Applied { get; private set; }

    /// <summary>The durable view.</summary>
    public IRaftLogView Stable => _stable;

    /// <summary>Received snapshot awaiting installation by the host, if any.</summary>
    public RaftSnapshot? PendingSnapshot => _pendingSnapshot;

    /// <summary>Index of the first unstable entry.</summary>
    public ulong UnstableOffset => _offset;

    /// <summary>Number of unstable entries.</summary>
    public int UnstableCount => _unstable.Count;

    /// <summary>Index of the snapshot the log starts after.</summary>
    public ulong SnapshotIndex => _pendingSnapshot?.Meta.Index ?? _stable.Snapshot.Index;

    /// <summary>First index whose entry is available.</summary>
    public ulong FirstIndex => SnapshotIndex + 1;

    /// <summary>Last index of the log.</summary>
    public ulong LastIndex
        => _unstable.Count > 0
            ? _offset + (ulong)_unstable.Count - 1
            : _pendingSnapshot?.Meta.Index ?? _stable.LastIndex;

    /// <summary>Term of the last entry (snapshot term when empty).</summary>
    public ulong LastTerm => TryTerm(LastIndex, out ulong t) ? t : 0;

    /// <summary>Term of the entry at <paramref name="index"/>; false when compacted or beyond the last index.</summary>
    public bool TryTerm(ulong index, out ulong term)
    {
        term = 0;
        if (_pendingSnapshot is not null)
        {
            ulong snap = _pendingSnapshot.Meta.Index;
            if (index == snap)
            {
                term = _pendingSnapshot.Meta.Term;
                return true;
            }

            if (index < snap)
                return false;
        }

        if (index >= _offset)
        {
            ulong pos = index - _offset;
            if (pos < (ulong)_unstable.Count)
            {
                term = _unstable[(int)pos].Term;
                return true;
            }

            return false;
        }

        if (_pendingSnapshot is not null)
            return false;

        RaftSnapshotMeta stableSnap = _stable.Snapshot;
        if (index == stableSnap.Index)
        {
            term = stableSnap.Term;
            return true;
        }

        if (index < stableSnap.Index || index > _stable.LastIndex)
            return false;
        try
        {
            term = _stable.TermAt(index);
            return true;
        }
        catch (ArgumentOutOfRangeException)
        {
            return false;
        }
    }

    /// <summary>True when the entry at <paramref name="index"/> exists and has <paramref name="term"/>.</summary>
    public bool MatchTerm(ulong index, ulong term) => TryTerm(index, out ulong t) && t == term;

    /// <summary>
    /// True when a log ending at (<paramref name="lastIndex"/>, <paramref name="lastTerm"/>) is at least as up to date as
    /// this one (Raft §5.4.1).
    /// </summary>
    public bool IsUpToDate(ulong lastIndex, ulong lastTerm)
    {
        ulong myTerm = LastTerm;
        return lastTerm > myTerm || (lastTerm == myTerm && lastIndex >= LastIndex);
    }

    /// <summary>
    /// Returns entries of [<paramref name="from"/>, <paramref name="toExclusive"/>) limited to <paramref name="maxCount"/>
    /// (&lt;= 0: unlimited), or null when <paramref name="from"/> is compacted.
    /// </summary>
    public List<RaftEntry>? Slice(ulong from, ulong toExclusive, int maxCount)
    {
        if (from < FirstIndex)
            return null;
        ulong hi = Math.Min(toExclusive, LastIndex + 1);
        var result = new List<RaftEntry>();
        if (from >= hi)
            return result;
        int limit = maxCount <= 0 ? int.MaxValue : maxCount;

        ulong next = from;
        if (next < _offset)
        {
            ulong stableHi = Math.Min(hi, _offset);
            while (next < stableHi && result.Count < limit)
            {
                IReadOnlyList<RaftEntry> chunk;
                try
                {
                    chunk = _stable.Entries(next, stableHi, limit - result.Count);
                }
                catch (ArgumentOutOfRangeException)
                {
                    return result.Count == 0 ? null : result;
                }

                if (chunk.Count == 0)
                    break;
                foreach (RaftEntry e in chunk)
                {
                    if (e.Index != next)
                        throw new InvalidOperationException($"The durable log returned entry {e.Index} where {next} was expected.");
                    result.Add(e);
                    next++;
                }
            }

            if (next < stableHi)
                return result;
        }

        while (next < hi && result.Count < limit)
        {
            result.Add(_unstable[(int)(next - _offset)]);
            next++;
        }

        return result;
    }

    /// <summary>Returns the entry at <paramref name="index"/>, or null when unavailable.</summary>
    public RaftEntry? EntryAt(ulong index)
    {
        List<RaftEntry>? one = Slice(index, index + 1, 1);
        return one is { Count: 1 } ? one[0] : null;
    }

    /// <summary>Appends entries directly after <see cref="LastIndex"/> (leader path).</summary>
    public void Append(RaftEntry entry)
    {
        if (entry.Index != LastIndex + 1)
            throw new InvalidOperationException($"Entry {entry.Index} does not follow the last index {LastIndex}.");
        if (_unstable.Count == 0)
            _offset = entry.Index;
        _unstable.Add(entry);
    }

    /// <summary>
    /// Appends contiguous entries starting at or before <see cref="LastIndex"/> + 1, replacing every existing entry at or
    /// after the first one (the caller has established that they conflict).
    /// </summary>
    public void TruncateAndAppend(IReadOnlyList<RaftEntry> entries)
    {
        if (entries.Count == 0)
            return;
        ulong first = entries[0].Index;
        if (first <= Committed)
            throw new InvalidOperationException($"Refusing to overwrite committed entry {first} (commit {Committed}).");
        if (first > LastIndex + 1)
            throw new InvalidOperationException($"Entry {first} would leave a gap after the last index {LastIndex}.");

        if (_unstable.Count > 0 && first == _offset + (ulong)_unstable.Count)
        {
            _unstable.AddRange(entries);
        }
        else if (first <= _offset || _unstable.Count == 0)
        {
            _offset = first;
            _unstable = [.. entries];
        }
        else
        {
            int keep = (int)(first - _offset);
            _unstable.RemoveRange(keep, _unstable.Count - keep);
            _unstable.AddRange(entries);
        }
    }

    /// <summary>Unstable entries to hand to the host for persistence.</summary>
    public RaftEntry[] UnstableEntries() => [.. _unstable];

    /// <summary>
    /// Marks entries up to (<paramref name="index"/>, <paramref name="term"/>) as persisted by the host. When the unstable
    /// tail becomes empty, the view continues with whatever the durable log holds after <paramref name="index"/> (a host
    /// may keep a durable suffix that matches what it was asked to write), so <see cref="LastIndex"/> and
    /// <see cref="TryTerm"/> always describe the same log.
    /// </summary>
    public void StableTo(ulong index, ulong term)
    {
        if (_pendingSnapshot is not null)
            return; // the snapshot is installed first and may discard durable entries: hand the tail over again after it
        if (index < _offset || _unstable.Count == 0)
            return;
        ulong pos = index - _offset;
        if (pos >= (ulong)_unstable.Count || _unstable[(int)pos].Term != term)
            return; // superseded by a later truncation
        _unstable.RemoveRange(0, (int)pos + 1);
        _offset = _unstable.Count > 0 ? index + 1 : Math.Max(index, _stable.LastIndex) + 1;
    }

    /// <summary>Marks the pending snapshot at <paramref name="index"/> as installed by the host.</summary>
    public void StableSnapTo(ulong index)
    {
        if (_pendingSnapshot is not null && _pendingSnapshot.Meta.Index == index)
            _pendingSnapshot = null;
        if (_unstable.Count == 0)
            _offset = LastIndex + 1;
    }

    /// <summary>Replaces the whole log with a received snapshot (follower install path).</summary>
    public void Restore(RaftSnapshot snapshot)
    {
        _pendingSnapshot = snapshot;
        _unstable = [];
        _offset = snapshot.Meta.Index + 1;
        Committed = snapshot.Meta.Index;
    }

    /// <summary>Raises the commit index (never lowers it; never beyond <see cref="LastIndex"/>).</summary>
    public bool CommitTo(ulong index)
    {
        ulong target = Math.Min(index, LastIndex);
        if (target <= Committed)
            return false;
        Committed = target;
        return true;
    }

    /// <summary>Records that entries up to <paramref name="index"/> were applied by the host.</summary>
    public void AppliedTo(ulong index)
    {
        if (index > Applied)
            Applied = index;
    }

    /// <summary>First index that the next batch of committed entries starts at.</summary>
    public ulong NextApplyIndex => Math.Max(Applied, SnapshotIndex) + 1;

    /// <summary>True when committed entries are waiting to be applied.</summary>
    public bool HasNextCommittedEntries => Committed >= NextApplyIndex && LastIndex >= NextApplyIndex;

    /// <summary>Committed entries not yet applied.</summary>
    public List<RaftEntry> NextCommittedEntries()
    {
        ulong lo = NextApplyIndex;
        ulong hi = Math.Min(Committed, LastIndex);
        if (hi < lo)
            return [];
        List<RaftEntry>? entries = Slice(lo, hi + 1, 0);
        if (entries is null)
            throw new InvalidOperationException($"Committed entries from {lo} are no longer available (first index {FirstIndex}); the host compacted unapplied entries.");
        return entries;
    }

    /// <summary>
    /// Largest index at or below <paramref name="index"/> whose term is at most <paramref name="term"/> (fast backtracking
    /// of conflicting suffixes); stops at the first unavailable index.
    /// </summary>
    public (ulong Index, ulong Term) FindConflictByTerm(ulong index, ulong term)
    {
        index = Math.Min(index, LastIndex);
        while (true)
        {
            if (!TryTerm(index, out ulong t))
                return (index, 0);
            if (t <= term || index == 0)
                return (index, t);
            index--;
        }
    }
}
