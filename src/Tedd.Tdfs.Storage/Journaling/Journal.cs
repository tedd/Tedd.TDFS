using System.Buffers.Binary;

namespace Tedd.Tdfs.Storage;

/// <summary>
/// Append-only journal of CRC-protected records in segment files (used by the Raft log store and backups).
/// Opening replays the records, truncates a torn tail and rejects corrupt interior records.
/// </summary>
/// <remarks>
/// <para><b>Layout.</b> The directory holds segment files named <c>{firstIndex:x16}.jseg</c>. A segment starts with a
/// 24-byte header (<c>u32 magic "TDJS" | u16 version | u16 flags | i64 firstIndex | u32 reserved | u32 CRC-32C</c>)
/// followed by records. Header flag 1 marks a <i>reset</i> segment (created by <see cref="DropBeforeAsync"/> past the
/// last record, before the old segments are deleted): recovery treats every older segment as obsolete. A record is a
/// 40-byte header (<c>u32 magic "TJRL" | u8 kind | u8 flags | u16 0 | u32 storedLength | u32 keyVersion | i64 index |
/// i64 durableThrough | u32 0 | u32 header CRC-32C</c>), the stored payload and a 4-byte trailer (CRC-32C of header +
/// payload). Entry records carry contiguous indexes; marker records
/// carry no payload and certify durability (one is written, without an extra barrier, after every successful flush, and
/// flushed on clean close).</para>
/// <para><b>Encryption.</b> With a cipher, payloads are sealed with the cipher's current key version and associated
/// data = the record index (8 bytes, little-endian); the key version is stored in the record header.</para>
/// <para><b>Durability.</b> <see cref="AppendAsync"/> only buffers. <see cref="FlushAsync"/> writes the buffered records and
/// issues a flush barrier on the segment file (and on the directory when a segment was created).
/// <see cref="TruncateAfterAsync"/> and <see cref="DropBeforeAsync"/> are durable on return. A failed write or flush
/// faults the journal: every later call throws and the journal must be reopened (recovery decides what survived).</para>
/// <para><b>Recovery.</b> Every record written after a successful flush carries <c>durableThrough</c> = the highest index
/// known durable at write time, so a valid record found after a damaged one proves whether the damaged record had been
/// made durable. At open, damage in a sealed (non-last) segment, or damage in the last segment that a later valid record
/// proves durable, throws <see cref="DfsIntegrityException"/>; any other damage in the last segment is a torn tail and
/// is truncated. Corruption of the final flushed batch is indistinguishable from a torn write only when its marker
/// never reached the device (power loss right after the flush); it is then treated as a torn tail. Bytes inside a
/// record whose header is intact (payload, possibly supplied by a client) are never searched for later records. Several
/// segments are deleted one durable step at a time, so file systems that persist unflushed unlinks in any order still
/// leave a contiguous run of segments.</para>
/// <para>Calls are serialized internally; the journal is intended for one owner on a node loop.</para>
/// </remarks>
public sealed class Journal : IAsyncDisposable
{
    /// <summary>File extension of journal segments.</summary>
    public const string SegmentExtension = ".jseg";

    /// <summary>Segment format version written by this implementation.</summary>
    public const ushort FormatVersion = 1;

    internal const int SegmentHeaderSize = 24;
    internal const int RecordHeaderSize = 40;
    internal const int TrailerSize = 4;
    internal const byte KindEntry = 1;
    internal const byte KindMarker = 2;
    internal const byte FlagEncrypted = 1;

    /// <summary>Segment header flag: the segment starts a fresh journal and every older segment is obsolete.</summary>
    internal const ushort SegmentFlagReset = 1;

    private readonly IStorageMedium _medium;
    private readonly string _directory;
    private readonly JournalOptions _options;
    private readonly IRecordCipher? _cipher;
    private readonly int _cipherOverhead;
    private readonly List<Segment> _segments = [];
    private readonly List<PendingRecord> _pending = [];
    private readonly SerialGate _gate = new();
    private IReadOnlyList<JournalRecord> _records = [];
    private long _nextIndex;
    private long _writtenIndex;
    private long _durableIndex;
    private long _certifiedOnDisk;
    private bool _markerUnflushed;
    private Exception? _fault;
    private bool _disposed;

    private Journal(IStorageMedium medium, string directory, JournalOptions options)
    {
        _medium = medium;
        _directory = directory;
        _options = options;
        _cipher = options.Cipher;
        _cipherOverhead = _cipher is null ? 0 : CipherSupport.ValidateOverhead(_cipher);
    }

    /// <summary>Records replayed at open time (index, payload), in index order.</summary>
    public IReadOnlyList<JournalRecord> Records => _records;

    /// <summary>The journal directory (medium-relative, normalized).</summary>
    public string Directory => _directory;

    /// <summary>Index of the first retained record (equals <see cref="NextIndex"/> when the journal is empty).</summary>
    public long FirstIndex
    {
        get
        {
            if (_segments.Count > 0)
                return _segments[0].FirstIndex;
            return _pending.Count > 0 ? _pending[0].Index : _nextIndex;
        }
    }

    /// <summary>Index of the last appended record, including records not yet flushed (<see cref="FirstIndex"/> − 1 when empty).</summary>
    public long LastIndex => _nextIndex - 1;

    /// <summary>Index the next <see cref="AppendAsync"/> will assign.</summary>
    public long NextIndex => _nextIndex;

    /// <summary>Highest index known to be durable (every record up to it survived a flush barrier).</summary>
    public long DurableIndex => _durableIndex;

    /// <summary>True after a failed write or flush; the journal must be reopened.</summary>
    public bool IsFaulted => _fault is not null;

    /// <summary>Opens (or creates) the journal in <paramref name="directory"/> and replays it. A corrupt non-tail record throws <see cref="DfsIntegrityException"/>.</summary>
    /// <exception cref="DfsIntegrityException">A sealed segment is damaged, segments are not contiguous, an authenticated payload fails decryption, or damage in the last segment is proven durable.</exception>
    /// <exception cref="DfsRecoveryKeyMissingException">A record is encrypted and the needed key is unavailable.</exception>
    /// <exception cref="DfsProtocolException">A segment uses a newer format version (<see cref="DfsErrorCode.Unsupported"/>).</exception>
    public static async ValueTask<Journal> OpenAsync(IStorageMedium medium, string directory, JournalOptions options, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(medium);
        ArgumentNullException.ThrowIfNull(directory);
        ArgumentNullException.ThrowIfNull(options);
        if (options.SegmentSize < SegmentHeaderSize + RecordHeaderSize + TrailerSize)
            throw new ArgumentOutOfRangeException(nameof(options), $"SegmentSize must be at least {SegmentHeaderSize + RecordHeaderSize + TrailerSize} bytes.");
        if (options.MaxRecordSize <= 0 || options.MaxRecordSize > int.MaxValue - RecordHeaderSize - TrailerSize - CipherSupport.MaxOverhead)
            throw new ArgumentOutOfRangeException(nameof(options), "MaxRecordSize is out of range.");
        if (options.FirstIndex < 0)
            throw new ArgumentOutOfRangeException(nameof(options), "FirstIndex must not be negative.");

        string dir = StoragePath.Normalize(directory);
        if (!await medium.ExistsAsync(dir, ct))
            await medium.CreateDirectoryDurableAsync(dir, ct);

        var journal = new Journal(medium, dir, options);
        try
        {
            await journal.RecoverAsync(ct);
        }
        catch
        {
            await journal.CloseFilesAsync();
            throw;
        }

        return journal;
    }

    /// <summary>Appends a record and returns its index. Not durable until <see cref="FlushAsync"/>.</summary>
    /// <exception cref="ArgumentOutOfRangeException">The payload exceeds <see cref="JournalOptions.MaxRecordSize"/>.</exception>
    public async ValueTask<long> AppendAsync(ReadOnlyMemory<byte> payload, CancellationToken ct = default)
    {
        if (payload.Length > _options.MaxRecordSize)
            throw new ArgumentOutOfRangeException(nameof(payload), $"Record of {payload.Length} bytes exceeds the {_options.MaxRecordSize}-byte limit.");
        using SerialGate.Releaser _ = await _gate.EnterAsync(ct);
        ThrowIfUnusable();
        long index = _nextIndex++;
        _pending.Add(new PendingRecord(index, payload.ToArray()));
        return index;
    }

    /// <summary>Durability barrier for every appended record.</summary>
    public async ValueTask FlushAsync(CancellationToken ct = default)
    {
        using SerialGate.Releaser _ = await _gate.EnterAsync(ct);
        ThrowIfUnusable();
        await FlushCoreAsync(ct);
    }

    /// <summary>Removes every record with an index greater than <paramref name="index"/>. Durable on return.</summary>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="index"/> is below <see cref="FirstIndex"/> − 1.</exception>
    public async ValueTask TruncateAfterAsync(long index, CancellationToken ct = default)
    {
        using SerialGate.Releaser _ = await _gate.EnterAsync(ct);
        ThrowIfUnusable();
        long first = FirstIndex;
        if (index < first - 1)
            throw new ArgumentOutOfRangeException(nameof(index), $"Cannot truncate after {index}: the first retained index is {first}.");
        if (index >= _nextIndex - 1)
            return;

        _pending.RemoveAll(p => p.Index > index);
        if (index >= _writtenIndex)
        {
            _nextIndex = index + 1;
            return;
        }

        try
        {
            int keepSegment = _segments.FindLastIndex(s => s.FirstIndex <= index + 1);
            if (keepSegment < 0)
                throw new InvalidOperationException($"No segment holds index {index + 1}.");

            // Later segments go first (newest first), each one durably before the next (file systems need not persist
            // unflushed unlinks in order), so a crash can only leave a contiguous prefix of the old log.
            for (int s = _segments.Count - 1; s > keepSegment; s--)
            {
                await CloseSegmentFileAsync(_segments[s]);
                await _medium.DeleteAsync(_segments[s].Path, ct);
                await _medium.FlushDirectoryAsync(_directory, ct);
                _segments.RemoveAt(s);
            }

            Segment seg = _segments[keepSegment];
            int keep = checked((int)(index + 1 - seg.FirstIndex));
            long newLength = keep == 0 ? SegmentHeaderSize : seg.EntryEnds[keep - 1];
            seg.File ??= await _medium.OpenAsync(seg.Path, StorageOpenMode.OpenExisting, ct);
            await seg.File.SetLengthAsync(newLength, ct);
            await seg.File.FlushAsync(ct);
            seg.EntryEnds.RemoveRange(keep, seg.EntryEnds.Count - keep);
            seg.Length = newLength;

            _nextIndex = index + 1;
            _writtenIndex = index;
            _durableIndex = Math.Min(_durableIndex, index);
            _certifiedOnDisk = Math.Min(_certifiedOnDisk, index);
        }
        catch (Exception ex)
        {
            _fault = ex;
            throw;
        }
    }

    /// <summary>
    /// Drops records with an index lower than <paramref name="index"/> (segment granularity: a segment is deleted only
    /// when all of its records are below <paramref name="index"/>). Buffered records are flushed first. When
    /// <paramref name="index"/> is beyond the last record the journal becomes empty and continues at
    /// <paramref name="index"/> (recorded durably in a new empty reset segment, created atomically before the old
    /// segments are deleted, so a crash leaves either the old records or the new start). Durable on return.
    /// </summary>
    public async ValueTask DropBeforeAsync(long index, CancellationToken ct = default)
    {
        using SerialGate.Releaser _ = await _gate.EnterAsync(ct);
        ThrowIfUnusable();
        if (index <= FirstIndex)
            return;

        await FlushCoreAsync(ct);
        try
        {
            int deleteCount = 0;
            for (int s = 0; s < _segments.Count; s++)
            {
                Segment seg = _segments[s];
                bool isLast = s == _segments.Count - 1;
                if (isLast && seg.Count == 0 && seg.FirstIndex >= index)
                    break;
                long endExclusive = isLast ? seg.FirstIndex + seg.Count : _segments[s + 1].FirstIndex;
                if (endExclusive > index)
                    break;
                deleteCount++;
            }

            bool all = deleteCount == _segments.Count;
            if (all)
            {
                // The new start index is recorded durably (in a reset segment) BEFORE anything is deleted: a crash
                // then leaves either the old segments only, or a reset segment that makes every older one obsolete
                // (recovery discards them). Deleting first would leave an empty directory that forgets the new start.
                long newFirst = Math.Max(index, _nextIndex);
                await CreateResetSegmentAsync(newFirst, ct);
                _nextIndex = newFirst;
                _writtenIndex = newFirst - 1;
                _durableIndex = newFirst - 1;
                _certifiedOnDisk = newFirst - 1;
            }

            if (deleteCount > 0)
            {
                // Oldest first, and (unless a reset segment already makes them all obsolete) each one durably before
                // the next, since file systems need not persist unflushed unlinks in order: a crash then leaves a
                // contiguous suffix of segments.
                for (int s = 0; s < deleteCount; s++)
                {
                    await CloseSegmentFileAsync(_segments[s]);
                    await _medium.DeleteAsync(_segments[s].Path, ct);
                    if (!all || s == deleteCount - 1)
                        await _medium.FlushDirectoryAsync(_directory, ct);
                }

                _segments.RemoveRange(0, deleteCount);
            }
        }
        catch (Exception ex)
        {
            _fault = ex;
            throw;
        }
    }

    /// <summary>
    /// Flushes buffered records, writes a durability marker (so corruption of the final batch is detectable at the next
    /// open) and closes the journal files. Safe to call more than once.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        using SerialGate.Releaser _ = await _gate.EnterAsync();
        if (_disposed)
            return;
        try
        {
            if (_fault is null)
            {
                await FlushCoreAsync(CancellationToken.None);
                await WriteMarkerBestEffortAsync(flush: true);
            }
        }
        finally
        {
            _disposed = true;
            await CloseFilesAsync();
        }
    }

    /// <inheritdoc />
    public override string ToString() => $"Journal({_medium.Name}:{_directory}, [{FirstIndex}..{LastIndex}])";

    /// <summary>
    /// Drops the reference to the records replayed at open (<see cref="Records"/> becomes empty) once the owner has
    /// taken what it needs, so payloads the owner later discards (for example after log compaction) can be freed.
    /// </summary>
    internal void ReleaseReplayedRecords() => _records = [];

    internal static string SegmentFileName(long firstIndex) => StorageFormat.Hex16((ulong)firstIndex) + SegmentExtension;

    internal static bool TryParseSegmentFileName(string name, out long firstIndex)
    {
        firstIndex = 0;
        if (!name.EndsWith(SegmentExtension, StringComparison.Ordinal))
            return false;
        if (!StorageFormat.TryParseHex16(name.AsSpan(0, name.Length - SegmentExtension.Length), out ulong value) || value > long.MaxValue)
            return false;
        firstIndex = (long)value;
        return true;
    }

    private string SegmentPath(long firstIndex) => StoragePath.Combine(_directory, SegmentFileName(firstIndex));

    private void ThrowIfUnusable()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_fault is not null)
            throw new IOException($"Journal '{_directory}' failed earlier and must be reopened: {_fault.Message}", _fault);
    }

    private int EncodedSize(int payloadLength)
        => RecordHeaderSize + payloadLength + (_cipher is null ? 0 : _cipherOverhead) + TrailerSize;

    private async ValueTask FlushCoreAsync(CancellationToken ct)
    {
        if (_pending.Count == 0)
            return;
        ct.ThrowIfCancellationRequested();
        try
        {
            bool createdSegment = false;
            long durableAtStart = _durableIndex;
            int i = 0;
            while (i < _pending.Count)
            {
                Segment? active = _segments.Count > 0 ? _segments[^1] : null;
                int firstSize = EncodedSize(_pending[i].Payload.Length);
                if (active is null || (active.Count > 0 && active.Length + firstSize > _options.SegmentSize))
                {
                    if (active?.File is not null)
                    {
                        // Seal the full segment before the next one exists: sealed segments are always complete.
                        await active.File.FlushAsync(ct);
                        await CloseSegmentFileAsync(active);
                    }

                    active = await CreateSegmentAsync(_pending[i].Index, ct);
                    createdSegment = true;
                }
                else
                {
                    active.File ??= await _medium.OpenAsync(active.Path, StorageOpenMode.OpenExisting, ct);
                }

                int start = i;
                long projected = active.Length;
                int bytes = 0;
                do
                {
                    int size = EncodedSize(_pending[i].Payload.Length);
                    bytes = checked(bytes + size);
                    projected += size;
                    i++;
                }
                while (i < _pending.Count && projected + EncodedSize(_pending[i].Payload.Length) <= _options.SegmentSize);

                var buffer = new byte[bytes];
                int offset = 0;
                var ends = new List<long>(i - start);
                for (int k = start; k < i; k++)
                {
                    PendingRecord record = _pending[k];
                    offset += EncodeRecord(buffer.AsSpan(offset), KindEntry, record.Index, durableAtStart, record.Payload);
                    ends.Add(active.Length + offset);
                }

                await active.File!.WriteFullyAsync(active.Length, buffer, ct);
                active.Length += bytes;
                active.EntryEnds.AddRange(ends);
                _writtenIndex = _pending[i - 1].Index;
            }

            await _segments[^1].File!.FlushAsync(ct);
            if (createdSegment)
                await _medium.FlushDirectoryAsync(_directory, ct);
            _durableIndex = _writtenIndex;
            _certifiedOnDisk = Math.Max(_certifiedOnDisk, durableAtStart);
            _markerUnflushed = false;
            _pending.Clear();
        }
        catch (Exception ex)
        {
            _fault = ex;
            throw;
        }

        // Certify the batch right away (no extra barrier): if the process dies before the next flush, a marker that
        // reached the device still lets recovery tell later corruption of this batch from a torn write.
        await WriteMarkerBestEffortAsync(flush: false);
    }

    /// <summary>
    /// Writes a marker certifying every durable record (when not certified yet), optionally followed by a flush; a
    /// marker left unflushed by an earlier call is flushed when <paramref name="flush"/> is set. Failures are ignored:
    /// the marker only strengthens corruption detection, and a partially written marker is overwritten by the next
    /// append or cut as a torn tail at the next open.
    /// </summary>
    private async ValueTask WriteMarkerBestEffortAsync(bool flush)
    {
        if (_segments.Count == 0)
            return;
        Segment active = _segments[^1];
        try
        {
            if (_durableIndex > _certifiedOnDisk && _durableIndex >= FirstIndex)
            {
                active.File ??= await _medium.OpenAsync(active.Path, StorageOpenMode.OpenExisting);
                var marker = new byte[RecordHeaderSize + TrailerSize];
                EncodeRecord(marker, KindMarker, _durableIndex, _durableIndex, ReadOnlySpan<byte>.Empty);
                await active.File.WriteFullyAsync(active.Length, marker);
                active.Length += marker.Length;
                _certifiedOnDisk = _durableIndex;
                _markerUnflushed = true;
            }

            if (flush && _markerUnflushed && active.File is not null)
            {
                await active.File.FlushAsync();
                _markerUnflushed = false;
            }
        }
        catch (Exception ex) when (StorageFormat.IsCleanupTolerable(ex))
        {
            // The marker only strengthens corruption detection; a journal without it is still correct.
        }
    }

    private async ValueTask<Segment> CreateSegmentAsync(long firstIndex, CancellationToken ct)
    {
        string path = SegmentPath(firstIndex);
        IStorageFile file = await _medium.OpenAsync(path, StorageOpenMode.Truncate, ct);
        try
        {
            await file.WriteFullyAsync(0, EncodeSegmentHeader(firstIndex), ct);
        }
        catch
        {
            await StorageFormat.CloseQuietlyAsync(file);
            throw;
        }

        var seg = new Segment(firstIndex, path) { File = file, Length = SegmentHeaderSize };
        _segments.Add(seg);
        return seg;
    }

    /// <summary>
    /// Durably creates an empty reset segment starting at <paramref name="firstIndex"/> (header flag
    /// <see cref="SegmentFlagReset"/>: every older segment is obsolete). The segment appears atomically (temp file, flush,
    /// rename, directory flush), so a crash never leaves a torn reset segment behind a gap.
    /// </summary>
    private async ValueTask<Segment> CreateResetSegmentAsync(long firstIndex, CancellationToken ct)
    {
        string path = SegmentPath(firstIndex);
        string temp = path + AtomicFile.TempSuffix;
        IStorageFile staged = await _medium.OpenAsync(temp, StorageOpenMode.Truncate, ct);
        try
        {
            await staged.WriteFullyAsync(0, EncodeSegmentHeader(firstIndex, SegmentFlagReset), ct);
            await staged.FlushAsync(ct);
        }
        finally
        {
            await StorageFormat.CloseQuietlyAsync(staged);
        }

        await _medium.RenameAsync(temp, path, overwrite: true, ct);
        await _medium.FlushDirectoryAsync(_directory, ct);
        IStorageFile file = await _medium.OpenAsync(path, StorageOpenMode.OpenExisting, ct);
        var seg = new Segment(firstIndex, path) { File = file, Length = SegmentHeaderSize };
        _segments.Add(seg);
        return seg;
    }

    /// <summary>True when the segment at <paramref name="path"/> has a valid header carrying <see cref="SegmentFlagReset"/>.</summary>
    private async ValueTask<bool> IsResetSegmentAsync(string path, long first, CancellationToken ct)
    {
        IStorageFile file = await _medium.OpenAsync(path, StorageOpenMode.OpenExisting, ct);
        try
        {
            if (file.Length < SegmentHeaderSize)
                return false;
            var header = new byte[SegmentHeaderSize];
            await file.ReadFullyAsync(0, header, ct);
            return ParseSegmentHeader(header, first, out _, out ushort flags) == HeaderStatus.Ok && (flags & SegmentFlagReset) != 0;
        }
        finally
        {
            await StorageFormat.CloseQuietlyAsync(file);
        }
    }

    private async ValueTask RecoverAsync(CancellationToken ct)
    {
        IReadOnlyList<string> names = await _medium.ListAsync(_directory, ct);
        var found = new List<long>();
        bool cleaned = false;
        foreach (string name in names)
        {
            if (TryParseSegmentFileName(name, out long first))
            {
                found.Add(first);
            }
            else if (name.EndsWith(SegmentExtension + AtomicFile.TempSuffix, StringComparison.Ordinal))
            {
                // A reset segment that was never published (crash before its rename).
                await _medium.DeleteAsync(StoragePath.Combine(_directory, name), ct);
                cleaned = true;
            }
        }

        found.Sort();

        // The newest reset segment (written by DropBeforeAsync past the end before it deletes anything) makes every
        // older segment obsolete: they are leftovers of an interrupted drop and are deleted durably, oldest first.
        for (int i = found.Count - 1; i > 0; i--)
        {
            if (!await IsResetSegmentAsync(SegmentPath(found[i]), found[i], ct))
                continue;
            for (int k = 0; k < i; k++)
                await _medium.DeleteAsync(SegmentPath(found[k]), ct);
            found.RemoveRange(0, i);
            cleaned = true;
            break;
        }

        if (cleaned)
            await _medium.FlushDirectoryAsync(_directory, ct);

        var records = new List<JournalRecord>();
        long expected = -1;
        long maxCertified = long.MinValue;
        for (int i = 0; i < found.Count; i++)
        {
            bool isLast = i == found.Count - 1;
            long first = found[i];
            string path = SegmentPath(first);
            IStorageFile file = await _medium.OpenAsync(path, StorageOpenMode.OpenExisting, ct);
            bool keepOpen = false;
            try
            {
                byte[] data = await StorageFormat.ReadAllAsync(file, ct);
                var seg = new Segment(first, path);
                HeaderStatus header = ParseSegmentHeader(data, first, out ushort version);
                if (header == HeaderStatus.Unsupported)
                {
                    throw new DfsProtocolException(DfsErrorCode.Unsupported,
                        $"Journal segment '{path}' uses format version {version}; this software supports version {FormatVersion}.");
                }

                if (header != HeaderStatus.Ok)
                {
                    long firstExpected = expected < 0 ? first : expected;
                    if (!isLast)
                        throw new DfsIntegrityException($"Journal segment '{path}' has a damaged header and is not the last segment.");
                    if (expected >= 0 && first != expected)
                        throw new DfsIntegrityException($"Journal segment '{path}' starts at {first} but the previous segment ends at {expected - 1}.");
                    if (ScanCertification(data, 0) >= firstExpected)
                        throw new DfsIntegrityException($"Journal segment '{path}' has a damaged header but later records prove its contents durable.");

                    // Torn creation: nothing in this segment was ever acknowledged. Rewrite an empty segment.
                    await file.SetLengthAsync(0, ct);
                    await file.WriteFullyAsync(0, EncodeSegmentHeader(first), ct);
                    await file.FlushAsync(ct);
                    seg.Length = SegmentHeaderSize;
                    seg.File = file;
                    keepOpen = true;
                    _segments.Add(seg);
                    expected = first;
                    continue;
                }

                if (expected >= 0 && first != expected)
                    throw new DfsIntegrityException($"Journal segment '{path}' starts at {first} but the previous segment ends at {expected - 1}.");
                expected = first;

                int pos = SegmentHeaderSize;
                int truncateAt = -1;
                while (pos < data.Length)
                {
                    ParseStatus status = TryParseRecord(data, pos, expected, out ParsedRecord rec);
                    if (status == ParseStatus.Ok)
                    {
                        if (rec.Kind == KindEntry)
                        {
                            records.Add(new JournalRecord(rec.Index, DecodePayload(data, rec, path)));
                            seg.EntryEnds.Add(pos + rec.TotalLength);
                            expected++;
                            maxCertified = Math.Max(maxCertified, rec.DurableThrough);
                        }
                        else
                        {
                            maxCertified = Math.Max(maxCertified, rec.Index);
                        }

                        pos += rec.TotalLength;
                        continue;
                    }

                    if (!isLast)
                    {
                        throw new DfsIntegrityException(
                            $"Journal segment '{path}' is damaged at offset {pos} (record {expected}); sealed segments must be complete.");
                    }

                    if (ScanCertification(data, DamagedRecordEnd(data, pos)) >= expected)
                    {
                        throw new DfsIntegrityException(
                            $"Journal record {expected} at offset {pos} of '{path}' is corrupt, but later records prove it was durable.");
                    }

                    truncateAt = pos;
                    break;
                }

                seg.Length = truncateAt >= 0 ? truncateAt : data.Length;
                if (isLast)
                {
                    if (truncateAt >= 0)
                        await file.SetLengthAsync(truncateAt, ct);

                    // Whatever was recovered becomes durable before anything depends on it.
                    await file.FlushAsync(ct);
                    seg.File = file;
                    keepOpen = true;
                }

                _segments.Add(seg);
            }
            finally
            {
                if (!keepOpen)
                    await StorageFormat.CloseQuietlyAsync(file);
            }
        }

        if (_segments.Count > 0)
        {
            Segment last = _segments[^1];
            _nextIndex = last.FirstIndex + last.Count;
            await _medium.FlushDirectoryAsync(_directory, ct);
        }
        else
        {
            _nextIndex = _options.FirstIndex;
        }

        _writtenIndex = _nextIndex - 1;
        _durableIndex = _nextIndex - 1;
        _certifiedOnDisk = maxCertified == long.MinValue ? FirstIndex - 1 : Math.Min(maxCertified, _durableIndex);
        _records = records.AsReadOnly();
    }

    private byte[] DecodePayload(byte[] data, ParsedRecord rec, string path)
    {
        ReadOnlySpan<byte> stored = data.AsSpan(rec.PayloadOffset, rec.StoredLength);
        if ((rec.Flags & FlagEncrypted) == 0)
            return stored.ToArray();
        Span<byte> aad = stackalloc byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(aad, rec.Index);
        return CipherSupport.TryDecrypt(_cipher, aad, stored, rec.KeyVersion, $"Journal record {rec.Index} in '{path}'")
            ?? throw new DfsIntegrityException($"Journal record {rec.Index} in '{path}' failed authenticated decryption.");
    }

    private int EncodeRecord(Span<byte> destination, byte kind, long index, long durableThrough, ReadOnlySpan<byte> payload)
    {
        bool encrypt = _cipher is not null && kind == KindEntry;
        int stored = encrypt ? payload.Length + _cipherOverhead : payload.Length;
        int total = RecordHeaderSize + stored + TrailerSize;
        Span<byte> record = destination[..total];
        uint keyVersion = 0;
        if (encrypt)
        {
            keyVersion = CipherSupport.CurrentVersion(_cipher!);
            Span<byte> aad = stackalloc byte[8];
            BinaryPrimitives.WriteInt64LittleEndian(aad, index);
            _cipher!.Encrypt(aad, payload, record.Slice(RecordHeaderSize, stored), keyVersion);
        }
        else
        {
            payload.CopyTo(record[RecordHeaderSize..]);
        }

        BinaryPrimitives.WriteUInt32LittleEndian(record, StorageFormat.JournalRecordMagic);
        record[4] = kind;
        record[5] = encrypt ? FlagEncrypted : (byte)0;
        BinaryPrimitives.WriteUInt16LittleEndian(record[6..], 0);
        BinaryPrimitives.WriteUInt32LittleEndian(record[8..], (uint)stored);
        BinaryPrimitives.WriteUInt32LittleEndian(record[12..], keyVersion);
        BinaryPrimitives.WriteInt64LittleEndian(record[16..], index);
        BinaryPrimitives.WriteInt64LittleEndian(record[24..], durableThrough);
        BinaryPrimitives.WriteUInt32LittleEndian(record[32..], 0);
        StorageFormat.SealCrc(record, 36);
        StorageFormat.SealCrc(record, RecordHeaderSize + stored);
        return total;
    }

    private ParseStatus TryParseRecord(ReadOnlySpan<byte> data, int pos, long? expected, out ParsedRecord rec)
    {
        ParseStatus header = TryParseRecordHeader(data, pos, out rec);
        if (header != ParseStatus.Ok)
            return header;
        if (data.Length - pos < rec.TotalLength)
            return ParseStatus.Incomplete;
        if (!StorageFormat.CheckCrc(data.Slice(pos, rec.TotalLength), RecordHeaderSize + rec.StoredLength))
            return ParseStatus.Corrupt;
        if (expected is long e && ((rec.Kind == KindEntry && rec.Index != e) || (rec.Kind == KindMarker && rec.Index >= e)))
            return ParseStatus.Corrupt;
        return ParseStatus.Ok;
    }

    /// <summary>
    /// Validates the 40-byte record header at <paramref name="pos"/> (magic, header CRC and field consistency) without
    /// looking at the payload or trailer. On success <paramref name="rec"/> describes the record's claimed extent.
    /// </summary>
    private ParseStatus TryParseRecordHeader(ReadOnlySpan<byte> data, int pos, out ParsedRecord rec)
    {
        rec = default;
        if (data.Length - pos < RecordHeaderSize)
            return ParseStatus.Incomplete;
        ReadOnlySpan<byte> h = data.Slice(pos, RecordHeaderSize);
        if (BinaryPrimitives.ReadUInt32LittleEndian(h) != StorageFormat.JournalRecordMagic || !StorageFormat.CheckCrc(h, 36))
            return ParseStatus.Corrupt;
        byte kind = h[4];
        byte flags = h[5];
        uint stored = BinaryPrimitives.ReadUInt32LittleEndian(h[8..]);
        uint keyVersion = BinaryPrimitives.ReadUInt32LittleEndian(h[12..]);
        long index = BinaryPrimitives.ReadInt64LittleEndian(h[16..]);
        long durable = BinaryPrimitives.ReadInt64LittleEndian(h[24..]);
        if (kind is not (KindEntry or KindMarker)
            || (flags & ~FlagEncrypted) != 0
            || BinaryPrimitives.ReadUInt16LittleEndian(h[6..]) != 0
            || BinaryPrimitives.ReadUInt32LittleEndian(h[32..]) != 0
            || stored > (uint)(_options.MaxRecordSize + CipherSupport.MaxOverhead)
            || ((flags & FlagEncrypted) != 0) != (keyVersion != 0)
            || (kind == KindMarker && (stored != 0 || flags != 0))
            || (kind == KindEntry && durable >= index)
            || index < 0)
        {
            return ParseStatus.Corrupt;
        }

        int total = RecordHeaderSize + (int)stored + TrailerSize;
        rec = new ParsedRecord(kind, flags, keyVersion, index, durable, pos + RecordHeaderSize, (int)stored, total);
        return ParseStatus.Ok;
    }

    /// <summary>
    /// Offset after the damaged record at <paramref name="pos"/>: the end of its claimed extent when its header is intact
    /// (only its payload or trailer is damaged or missing), otherwise <paramref name="pos"/> + 1.
    /// </summary>
    private long DamagedRecordEnd(ReadOnlySpan<byte> data, int pos)
        => TryParseRecordHeader(data, pos, out ParsedRecord rec) == ParseStatus.Ok ? (long)pos + rec.TotalLength : pos + 1L;

    /// <summary>
    /// Highest durability certificate carried by any valid record at or after <paramref name="from"/>. A record whose
    /// header is intact is skipped as a whole even when its payload or trailer is damaged: bytes inside a record are
    /// payload (possibly supplied by a client), never records, so they can neither certify nor hide anything.
    /// </summary>
    private long ScanCertification(ReadOnlySpan<byte> data, long from)
    {
        long certified = long.MinValue;
        long limit = data.Length - RecordHeaderSize;
        long q = Math.Max(from, 0);
        while (q <= limit)
        {
            int at = (int)q;
            if (BinaryPrimitives.ReadUInt32LittleEndian(data[at..]) == StorageFormat.JournalRecordMagic
                && TryParseRecordHeader(data, at, out ParsedRecord header) == ParseStatus.Ok)
            {
                if (TryParseRecord(data, at, null, out ParsedRecord rec) == ParseStatus.Ok)
                    certified = Math.Max(certified, rec.Kind == KindEntry ? rec.DurableThrough : rec.Index);
                q += header.TotalLength;
            }
            else
            {
                q++;
            }
        }

        return certified;
    }

    private static byte[] EncodeSegmentHeader(long firstIndex, ushort flags = 0)
    {
        var header = new byte[SegmentHeaderSize];
        BinaryPrimitives.WriteUInt32LittleEndian(header, StorageFormat.JournalSegmentMagic);
        BinaryPrimitives.WriteUInt16LittleEndian(header.AsSpan(4), FormatVersion);
        BinaryPrimitives.WriteUInt16LittleEndian(header.AsSpan(6), flags);
        BinaryPrimitives.WriteInt64LittleEndian(header.AsSpan(8), firstIndex);
        BinaryPrimitives.WriteUInt32LittleEndian(header.AsSpan(16), 0);
        StorageFormat.SealCrc(header, 20);
        return header;
    }

    private static HeaderStatus ParseSegmentHeader(ReadOnlySpan<byte> data, long expectedFirst, out ushort version)
        => ParseSegmentHeader(data, expectedFirst, out version, out _);

    private static HeaderStatus ParseSegmentHeader(ReadOnlySpan<byte> data, long expectedFirst, out ushort version, out ushort flags)
    {
        version = 0;
        flags = 0;
        if (data.Length < SegmentHeaderSize)
            return HeaderStatus.Damaged;
        if (BinaryPrimitives.ReadUInt32LittleEndian(data) != StorageFormat.JournalSegmentMagic || !StorageFormat.CheckCrc(data, 20))
            return HeaderStatus.Damaged;
        version = BinaryPrimitives.ReadUInt16LittleEndian(data[4..]);
        flags = BinaryPrimitives.ReadUInt16LittleEndian(data[6..]);
        if (version != FormatVersion || (flags & ~SegmentFlagReset) != 0)
            return HeaderStatus.Unsupported;
        if (BinaryPrimitives.ReadInt64LittleEndian(data[8..]) != expectedFirst)
            return HeaderStatus.Damaged;
        return HeaderStatus.Ok;
    }

    private static async ValueTask CloseSegmentFileAsync(Segment segment)
    {
        IStorageFile? file = segment.File;
        segment.File = null;
        await StorageFormat.CloseQuietlyAsync(file);
    }

    private async ValueTask CloseFilesAsync()
    {
        foreach (Segment segment in _segments)
            await CloseSegmentFileAsync(segment);
    }

    private enum ParseStatus
    {
        Ok,
        Incomplete,
        Corrupt,
    }

    private enum HeaderStatus
    {
        Ok,
        Damaged,
        Unsupported,
    }

    private readonly record struct PendingRecord(long Index, byte[] Payload);

    private readonly record struct ParsedRecord(byte Kind, byte Flags, uint KeyVersion, long Index, long DurableThrough, int PayloadOffset, int StoredLength, int TotalLength);

    private sealed class Segment(long firstIndex, string path)
    {
        public long FirstIndex { get; } = firstIndex;

        public string Path { get; } = path;

        public IStorageFile? File { get; set; }

        public long Length { get; set; }

        /// <summary>End offset of each entry record, in index order.</summary>
        public List<long> EntryEnds { get; } = [];

        public int Count => EntryEnds.Count;
    }
}
