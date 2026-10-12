namespace Tedd.Tdfs.Consensus;

/// <summary>List-backed <see cref="IRaftLogView"/> for tests and model checking. Not thread-safe.</summary>
public sealed class InMemoryRaftLog : IRaftLogView
{
    private readonly List<RaftEntry> _entries = [];
    private RaftSnapshotMeta _snapshot;

    /// <summary>Creates an empty log (no snapshot).</summary>
    public InMemoryRaftLog()
        : this(RaftSnapshotMeta.Empty)
    {
    }

    /// <summary>Creates an empty log that starts after <paramref name="snapshot"/>.</summary>
    public InMemoryRaftLog(RaftSnapshotMeta snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        _snapshot = snapshot;
    }

    /// <inheritdoc />
    public ulong FirstIndex => _snapshot.Index + 1;

    /// <inheritdoc />
    public ulong LastIndex => _snapshot.Index + (ulong)_entries.Count;

    /// <summary>Term of the last entry (the snapshot term when no entries are retained).</summary>
    public ulong LastTerm => _entries.Count == 0 ? _snapshot.Term : _entries[^1].Term;

    /// <summary>Number of retained entries (after the snapshot).</summary>
    public int Count => _entries.Count;

    /// <inheritdoc />
    public RaftSnapshotMeta Snapshot => _snapshot;

    /// <inheritdoc />
    public ulong TermAt(ulong index)
    {
        if (TryGetTerm(index, out ulong term))
            return term;
        if (index < _snapshot.Index)
            throw new ArgumentOutOfRangeException(nameof(index), $"Index {index} is compacted (snapshot index {_snapshot.Index}).");
        throw new ArgumentOutOfRangeException(nameof(index), $"Index {index} is beyond the last index {LastIndex}.");
    }

    /// <summary>Gets the term at <paramref name="index"/> when it is the snapshot index or a retained entry.</summary>
    public bool TryGetTerm(ulong index, out ulong term)
    {
        if (index == _snapshot.Index)
        {
            term = _snapshot.Term;
            return true;
        }

        if (index > _snapshot.Index && index <= LastIndex)
        {
            term = _entries[ToPosition(index)].Term;
            return true;
        }

        term = 0;
        return false;
    }

    /// <summary>Returns the entry at <paramref name="index"/>.</summary>
    /// <exception cref="ArgumentOutOfRangeException">The index is not retained.</exception>
    public RaftEntry EntryAt(ulong index)
    {
        if (index < FirstIndex || index > LastIndex)
            throw new ArgumentOutOfRangeException(nameof(index), $"Index {index} is outside the retained range [{FirstIndex}, {LastIndex}].");
        return _entries[ToPosition(index)];
    }

    /// <summary>
    /// Returns at most <paramref name="maxCount"/> entries of [<paramref name="from"/>, <paramref name="toExclusive"/>);
    /// <paramref name="maxCount"/> &lt;= 0 means no limit.
    /// </summary>
    public IReadOnlyList<RaftEntry> Entries(ulong from, ulong toExclusive, int maxCount)
    {
        if (from < FirstIndex)
            throw new ArgumentOutOfRangeException(nameof(from), $"Index {from} is compacted (first retained index {FirstIndex}).");
        if (toExclusive > LastIndex + 1)
            throw new ArgumentOutOfRangeException(nameof(toExclusive), $"Range end {toExclusive} is beyond the last index {LastIndex} + 1.");
        if (toExclusive <= from)
            return [];

        ulong count = toExclusive - from;
        if (maxCount > 0 && count > (ulong)maxCount)
            count = (ulong)maxCount;
        return _entries.GetRange(ToPosition(from), (int)count).ToArray();
    }

    /// <summary>
    /// Appends contiguous entries. Entries already covered by the snapshot are ignored; an entry whose index and term
    /// match an existing entry is skipped; at the first index whose term differs, the existing entry and everything
    /// after it are removed (conflicting suffix) before appending the rest.
    /// </summary>
    /// <exception cref="ArgumentException">The entries are not contiguous or would leave a gap after <see cref="LastIndex"/>.</exception>
    public void Append(IEnumerable<RaftEntry> e)
    {
        ArgumentNullException.ThrowIfNull(e);
        List<RaftEntry> batch = [.. e];
        if (batch.Count == 0)
            return;

        for (int i = 0; i < batch.Count; i++)
        {
            if (batch[i] is null)
                throw new ArgumentException("Entries must not be null.", nameof(e));
            if (i > 0 && batch[i].Index != batch[i - 1].Index + 1)
                throw new ArgumentException($"Entries must be contiguous: {batch[i - 1].Index} is followed by {batch[i].Index}.", nameof(e));
        }

        if (batch[0].Index > LastIndex + 1)
            throw new ArgumentException($"Entry {batch[0].Index} would leave a gap after the last index {LastIndex}.", nameof(e));

        foreach (RaftEntry entry in batch)
        {
            if (entry.Index <= _snapshot.Index)
                continue;
            if (entry.Index <= LastIndex)
            {
                int position = ToPosition(entry.Index);
                if (_entries[position].Term == entry.Term)
                    continue;
                _entries.RemoveRange(position, _entries.Count - position);
            }

            _entries.Add(entry);
        }
    }

    /// <summary>
    /// Installs a snapshot. When the log holds an entry with the snapshot's index and term, the entries after it are
    /// retained; otherwise the whole log is discarded.
    /// </summary>
    /// <exception cref="InvalidOperationException">The snapshot is older than the current one.</exception>
    public void ApplySnapshot(RaftSnapshotMeta m)
    {
        ArgumentNullException.ThrowIfNull(m);
        if (m.Index < _snapshot.Index)
            throw new InvalidOperationException($"Snapshot at index {m.Index} is older than the current snapshot at index {_snapshot.Index}.");

        if (TryGetTerm(m.Index, out ulong term) && term == m.Term)
            _entries.RemoveRange(0, (int)(m.Index - _snapshot.Index));
        else
            _entries.Clear();
        _snapshot = m;
    }

    /// <summary>
    /// Discards entries up to and including <paramref name="index"/>, recording them as a snapshot. Uses
    /// <paramref name="confState"/> as the snapshot membership, or the current snapshot's membership when null.
    /// No-op when <paramref name="index"/> is at or below the current snapshot.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="index"/> is beyond <see cref="LastIndex"/>.</exception>
    public void Compact(ulong index, RaftConfState? confState = null)
    {
        if (index <= _snapshot.Index)
            return;
        if (index > LastIndex)
            throw new ArgumentOutOfRangeException(nameof(index), $"Cannot compact to {index}: the last index is {LastIndex}.");
        ulong term = TermAt(index);
        _entries.RemoveRange(0, (int)(index - _snapshot.Index));
        _snapshot = new RaftSnapshotMeta(index, term, confState ?? _snapshot.ConfState);
    }

    /// <summary>Removes every entry after <paramref name="index"/> (index must not be below the snapshot index).</summary>
    public void TruncateAfter(ulong index)
    {
        if (index < _snapshot.Index)
            throw new ArgumentOutOfRangeException(nameof(index), $"Cannot truncate below the snapshot index {_snapshot.Index}.");
        if (index >= LastIndex)
            return;
        int keep = (int)(index - _snapshot.Index);
        _entries.RemoveRange(keep, _entries.Count - keep);
    }

    /// <summary>Returns an independent copy (entries are immutable and shared).</summary>
    public InMemoryRaftLog Clone()
    {
        var copy = new InMemoryRaftLog(_snapshot);
        copy._entries.AddRange(_entries);
        return copy;
    }

    private int ToPosition(ulong index) => (int)(index - _snapshot.Index - 1);
}
