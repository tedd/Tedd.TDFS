using System.Buffers.Binary;

namespace Tedd.Tdfs.Storage;

/// <summary>
/// Durable Raft log of one consensus group: hard state, snapshot metadata and the entries after the snapshot.
/// </summary>
/// <remarks>
/// <para><b>Layout</b> (inside the group directory): <c>hardstate</c> and <c>snapshot</c> are <see cref="AtomicFile"/>s;
/// entries live in a <see cref="Journal"/> in <c>log/</c> whose record index is the Raft index and whose payload is
/// <c>u64 term | u8 kind | data</c>.</para>
/// <para><b>Durability contract.</b> <see cref="AppendAsync"/> buffers (it durably truncates a conflicting suffix first);
/// <see cref="FlushAsync"/> is the barrier that must complete before votes or append acknowledgements depending on the
/// entries are sent. <see cref="SaveHardStateAsync"/> is durable on return and refuses a commit index beyond the
/// durable log. <see cref="CompactThroughAsync"/> records the snapshot metadata durably before discarding entries, so
/// a crash at any point recovers either the old or the new snapshot with a consistent suffix.</para>
/// <para><b>Recovery</b> validates contiguity, non-decreasing terms, the term at the snapshot index and that the
/// committed index is still present; violations throw <see cref="DfsIntegrityException"/> (the group must stop rather
/// than silently lose acknowledged entries).</para>
/// </remarks>
public sealed class RaftLogStore : IAsyncDisposable
{
    /// <summary>File name of the hard state.</summary>
    public const string HardStateFileName = "hardstate";

    /// <summary>File name of the snapshot metadata.</summary>
    public const string SnapshotFileName = "snapshot";

    /// <summary>Directory name of the entry journal.</summary>
    public const string LogDirectoryName = "log";

    private readonly IStorageMedium _medium;
    private readonly string _directory;
    private readonly RaftLogStoreOptions _options;
    private readonly List<LogEntryRecord> _entries = [];
    private readonly SerialGate _gate = new();
    private readonly Journal _journal;
    private RaftLogSnapshotMeta _snapshot;
    private RaftLogHardState _hardState;
    private bool _disposed;

    private RaftLogStore(IStorageMedium medium, string directory, RaftLogStoreOptions options, Journal journal, RaftLogSnapshotMeta snapshot, RaftLogHardState hardState)
    {
        _medium = medium;
        _directory = directory;
        _options = options;
        _journal = journal;
        _snapshot = snapshot;
        _hardState = hardState;
    }

    /// <summary>The persisted hard state.</summary>
    public RaftLogHardState HardState => _hardState;

    /// <summary>Metadata of the latest snapshot (<see cref="RaftLogSnapshotMeta.Empty"/> initially).</summary>
    public RaftLogSnapshotMeta Snapshot => _snapshot;

    /// <summary>Index of the first retained entry (snapshot index + 1).</summary>
    public ulong FirstIndex => _snapshot.Index + 1;

    /// <summary>Index of the last entry, including entries not yet flushed (the snapshot index when no entries are retained).</summary>
    public ulong LastIndex => _snapshot.Index + (ulong)_entries.Count;

    /// <summary>Term of the last entry (the snapshot term when no entries are retained).</summary>
    public ulong LastTerm => _entries.Count == 0 ? _snapshot.Term : _entries[^1].Term;

    /// <summary>Highest index that is durable (flushed entries, or the snapshot index).</summary>
    public ulong DurableIndex => Math.Max(_snapshot.Index, Math.Min(LastIndex, (ulong)Math.Max(0, _journal.DurableIndex)));

    /// <summary>Opens (or creates) the log of one group in <paramref name="directory"/> and replays it.</summary>
    /// <exception cref="DfsIntegrityException">The journal is damaged in a way that loses durable entries, or the recovered log is inconsistent.</exception>
    public static async ValueTask<RaftLogStore> OpenAsync(IStorageMedium medium, string directory, RaftLogStoreOptions options, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(medium);
        ArgumentNullException.ThrowIfNull(directory);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(options.Faults, nameof(options));
        if (options.MaxEntrySize <= 0 || options.MaxEntrySize > JournalOptions.DefaultMaxRecordSize * 16)
            throw new ArgumentOutOfRangeException(nameof(options), "MaxEntrySize is out of range.");

        string dir = StoragePath.Normalize(directory);
        if (!await medium.ExistsAsync(dir, ct))
            await medium.CreateDirectoryDurableAsync(dir, ct);
        if (options.Faults.IsEnabled)
        {
            await options.Faults.HitAsync(new FaultContext(FaultStage.F16, FaultSites.RecoveryJournalReplay, options.NodeName, options.GroupName,
                DfsOperationKind.RaftAppend, Detail: dir), ct);
        }

        string hardPath = StoragePath.Combine(dir, HardStateFileName);
        string snapPath = StoragePath.Combine(dir, SnapshotFileName);
        byte[]? hardBytes = await AtomicFile.ReadAsync(medium, hardPath, ct);
        RaftLogHardState hard = hardBytes is null ? RaftLogHardState.Initial : RaftLogHardState.Decode(hardBytes, hardPath);
        byte[]? snapBytes = await AtomicFile.ReadAsync(medium, snapPath, ct);
        bool resetPending = false;
        RaftLogSnapshotMeta snapshot = snapBytes is null ? RaftLogSnapshotMeta.Empty : RaftLogSnapshotMeta.Decode(snapBytes, snapPath, out resetPending);
        if (snapshot.Index > long.MaxValue - 1)
            throw new DfsIntegrityException($"Raft snapshot metadata '{snapPath}' has an out-of-range index.");

        var journalOptions = new JournalOptions(options.SegmentSize, options.Cipher)
        {
            FirstIndex = (long)snapshot.Index + 1,
            MaxRecordSize = options.MaxEntrySize + RaftLogStoreOptions.EntryHeaderSize,
        };
        Journal journal = await Journal.OpenAsync(medium, StoragePath.Combine(dir, LogDirectoryName), journalOptions, ct);
        var store = new RaftLogStore(medium, dir, options, journal, snapshot, hard);
        try
        {
            if (resetPending)
            {
                await store.ResetJournalAsync(snapshot.Index + 1, ct);
                await AtomicFile.WriteAsync(medium, snapPath, snapshot.Encode(resetPending: false), ct);
            }
            else
            {
                await store.LoadEntriesAsync(ct);
            }

            if (hard.Commit > store.LastIndex)
            {
                throw new DfsIntegrityException(
                    $"Raft log '{dir}': the hard state commits index {hard.Commit} but the durable log ends at {store.LastIndex}.");
            }
        }
        catch
        {
            await journal.DisposeAsync();
            throw;
        }

        return store;
    }

    /// <summary>Durably replaces the hard state.</summary>
    /// <exception cref="InvalidOperationException">The commit index is beyond the durable log (flush entries first).</exception>
    public async ValueTask SaveHardStateAsync(RaftLogHardState state, CancellationToken ct = default)
    {
        using SerialGate.Releaser _ = await _gate.EnterAsync(ct);
        ThrowIfDisposed();
        if (state.Commit > DurableIndex)
        {
            throw new InvalidOperationException(
                $"Cannot persist commit index {state.Commit}: entries are only durable through {DurableIndex}. Flush the entries first.");
        }

        await AtomicFile.WriteAsync(_medium, StoragePath.Combine(_directory, HardStateFileName), state.Encode(), ct);
        _hardState = state;
    }

    /// <summary>
    /// Appends contiguous entries. Entries already covered by the snapshot or already present with the same term are
    /// skipped; at the first index whose term differs the existing suffix is durably removed first. Appended entries
    /// are not durable until <see cref="FlushAsync"/>.
    /// </summary>
    /// <exception cref="ArgumentException">The entries are not contiguous, would leave a gap, have decreasing terms or an oversized payload.</exception>
    /// <exception cref="InvalidOperationException">The batch conflicts with a committed entry.</exception>
    public async ValueTask AppendAsync(IReadOnlyList<LogEntryRecord> entries, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(entries);
        for (int i = 0; i < entries.Count; i++)
        {
            LogEntryRecord? e = entries[i] ?? throw new ArgumentException("Entries must not be null.", nameof(entries));
            if (e.Index == 0)
                throw new ArgumentException("Raft indexes start at 1.", nameof(entries));
            if (e.Data.Length > _options.MaxEntrySize)
                throw new ArgumentException($"Entry {e.Index} has {e.Data.Length} bytes; the limit is {_options.MaxEntrySize}.", nameof(entries));
            if (i > 0 && e.Index != entries[i - 1].Index + 1)
                throw new ArgumentException($"Entries must be contiguous: {entries[i - 1].Index} is followed by {e.Index}.", nameof(entries));
            if (i > 0 && e.Term < entries[i - 1].Term)
                throw new ArgumentException($"Entry terms must not decrease (index {e.Index}).", nameof(entries));
        }

        using SerialGate.Releaser _ = await _gate.EnterAsync(ct);
        ThrowIfDisposed();
        if (entries.Count == 0)
            return;
        if (entries[0].Index > LastIndex + 1)
            throw new ArgumentException($"Entry {entries[0].Index} would leave a gap after the last index {LastIndex}.", nameof(entries));

        int next = 0;
        while (next < entries.Count && entries[next].Index <= _snapshot.Index)
            next++;
        while (next < entries.Count && entries[next].Index <= LastIndex)
        {
            LogEntryRecord e = entries[next];
            if (EntryAt(e.Index).Term == e.Term)
            {
                next++;
                continue;
            }

            if (e.Index <= _hardState.Commit)
                throw new InvalidOperationException($"Entry {e.Index} (term {e.Term}) conflicts with a committed entry.");
            await _journal.TruncateAfterAsync((long)e.Index - 1, ct);
            int keep = checked((int)(e.Index - 1 - _snapshot.Index));
            _entries.RemoveRange(keep, _entries.Count - keep);
            break;
        }

        for (; next < entries.Count; next++)
        {
            LogEntryRecord e = entries[next];
            if (e.Term < LastTerm)
                throw new ArgumentException($"Entry {e.Index} has term {e.Term}, below the last term {LastTerm}.", nameof(entries));
            var copy = new LogEntryRecord(e.Index, e.Term, e.Kind, e.Data.ToArray());
            long index = await _journal.AppendAsync(EncodeEntry(copy), ct);
            if ((ulong)index != e.Index)
                throw new InvalidOperationException($"Journal assigned index {index} to Raft entry {e.Index}.");
            _entries.Add(copy);
        }
    }

    /// <summary>Durability barrier for every appended entry.</summary>
    public async ValueTask FlushAsync(CancellationToken ct = default)
    {
        using SerialGate.Releaser _ = await _gate.EnterAsync(ct);
        ThrowIfDisposed();
        await _journal.FlushAsync(ct);
    }

    /// <summary>Returns at most <paramref name="maxCount"/> entries of [<paramref name="from"/>, <paramref name="toExclusive"/>); <paramref name="maxCount"/> ≤ 0 means no limit.</summary>
    /// <exception cref="ArgumentOutOfRangeException">The range starts below <see cref="FirstIndex"/> (compacted) or ends beyond <see cref="LastIndex"/> + 1.</exception>
    public IReadOnlyList<LogEntryRecord> Entries(ulong from, ulong toExclusive, int maxCount)
    {
        ThrowIfDisposed();
        if (from < FirstIndex)
            throw new ArgumentOutOfRangeException(nameof(from), $"Index {from} is compacted (first retained index {FirstIndex}).");
        if (toExclusive > LastIndex + 1)
            throw new ArgumentOutOfRangeException(nameof(toExclusive), $"Range end {toExclusive} is beyond the last index {LastIndex} + 1.");
        if (toExclusive <= from)
            return [];
        ulong count = toExclusive - from;
        if (maxCount > 0 && count > (ulong)maxCount)
            count = (ulong)maxCount;
        return _entries.GetRange((int)(from - FirstIndex), (int)count).ToArray();
    }

    /// <summary>Term of the entry at <paramref name="index"/>; the snapshot index is answered from the snapshot metadata.</summary>
    /// <exception cref="ArgumentOutOfRangeException">The index is compacted or beyond <see cref="LastIndex"/>.</exception>
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
            term = EntryAt(index).Term;
            return true;
        }

        term = 0;
        return false;
    }

    /// <summary>
    /// Records a snapshot covering the log through <paramref name="index"/> and discards the covered entries. When the
    /// log holds (<paramref name="index"/>, <paramref name="term"/>) the later entries are kept (local compaction);
    /// otherwise the whole log is discarded (snapshot installed from a leader). No-op when
    /// <paramref name="index"/> is not beyond the current snapshot. Durable on return.
    /// </summary>
    public async ValueTask CompactThroughAsync(ulong index, ulong term, ReadOnlyMemory<byte> confState, CancellationToken ct = default)
    {
        using SerialGate.Releaser _ = await _gate.EnterAsync(ct);
        ThrowIfDisposed();
        if (index <= _snapshot.Index)
            return;
        if (index > long.MaxValue - 1)
            throw new ArgumentOutOfRangeException(nameof(index));

        var meta = new RaftLogSnapshotMeta(index, term, confState.ToArray());
        string snapPath = StoragePath.Combine(_directory, SnapshotFileName);
        if (TryGetTerm(index, out ulong existing) && existing == term)
        {
            await AtomicFile.WriteAsync(_medium, snapPath, meta.Encode(resetPending: false), ct);
            _entries.RemoveRange(0, checked((int)(index - _snapshot.Index)));
            _snapshot = meta;
            await _journal.DropBeforeAsync((long)index + 1, ct);
        }
        else
        {
            await AtomicFile.WriteAsync(_medium, snapPath, meta.Encode(resetPending: true), ct);
            _entries.Clear();
            _snapshot = meta;
            await ResetJournalAsync(index + 1, ct);
            await AtomicFile.WriteAsync(_medium, snapPath, meta.Encode(resetPending: false), ct);
        }
    }

    /// <summary>Flushes and closes the journal.</summary>
    public async ValueTask DisposeAsync()
    {
        using SerialGate.Releaser _ = await _gate.EnterAsync();
        if (_disposed)
            return;
        _disposed = true;
        await _journal.DisposeAsync();
    }

    /// <inheritdoc />
    public override string ToString() => $"RaftLogStore({_medium.Name}:{_directory}, snapshot {_snapshot.Index}, last {LastIndex})";

    private LogEntryRecord EntryAt(ulong index) => _entries[(int)(index - _snapshot.Index - 1)];

    private void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(_disposed, this);

    private static byte[] EncodeEntry(LogEntryRecord e)
    {
        var payload = new byte[RaftLogStoreOptions.EntryHeaderSize + e.Data.Length];
        BinaryPrimitives.WriteUInt64LittleEndian(payload, e.Term);
        payload[8] = e.Kind;
        e.Data.Span.CopyTo(payload.AsSpan(RaftLogStoreOptions.EntryHeaderSize));
        return payload;
    }

    private async ValueTask LoadEntriesAsync(CancellationToken ct)
    {
        long needed = (long)_snapshot.Index + 1;
        if (_journal.NextIndex < needed)
        {
            // The snapshot is ahead of every journal record (a crash interrupted the drop): finish discarding.
            await _journal.DropBeforeAsync(needed, ct);
            return;
        }

        if (_journal.FirstIndex > needed)
        {
            throw new DfsIntegrityException(
                $"Raft log '{_directory}': the journal starts at {_journal.FirstIndex} but the snapshot ends at {_snapshot.Index}; entries are missing.");
        }

        ulong lastTerm = _snapshot.Term;
        foreach (JournalRecord record in _journal.Records)
        {
            ReadOnlyMemory<byte> payload = record.Payload;
            if (payload.Length < RaftLogStoreOptions.EntryHeaderSize)
                throw new DfsIntegrityException($"Raft log '{_directory}': entry {record.Index} is malformed.");
            ulong term = BinaryPrimitives.ReadUInt64LittleEndian(payload.Span);
            if (record.Index < needed)
            {
                if ((ulong)record.Index == _snapshot.Index && _snapshot.Index > 0 && term != _snapshot.Term)
                {
                    throw new DfsIntegrityException(
                        $"Raft log '{_directory}': entry {record.Index} has term {term} but the snapshot records term {_snapshot.Term}.");
                }

                continue;
            }

            if (term < lastTerm)
                throw new DfsIntegrityException($"Raft log '{_directory}': entry {record.Index} has term {term}, below the preceding term {lastTerm}.");
            lastTerm = term;
            _entries.Add(new LogEntryRecord((ulong)record.Index, term, payload.Span[8], payload[RaftLogStoreOptions.EntryHeaderSize..]));
        }
    }

    /// <summary>Durably empties the journal so it continues at <paramref name="next"/>.</summary>
    private async ValueTask ResetJournalAsync(ulong next, CancellationToken ct)
    {
        if (_journal.LastIndex >= _journal.FirstIndex)
            await _journal.TruncateAfterAsync(_journal.FirstIndex - 1, ct);
        await _journal.DropBeforeAsync((long)next, ct);
        if (_journal.NextIndex != (long)next)
        {
            throw new DfsIntegrityException(
                $"Raft log '{_directory}': the journal continues at {_journal.NextIndex} but the snapshot requires {next}.");
        }
    }
}
