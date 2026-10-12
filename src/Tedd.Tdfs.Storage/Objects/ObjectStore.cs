using Tedd.Tdfs.Data;

namespace Tedd.Tdfs.Storage;

/// <summary>
/// Packed, append-only object store: objects are records in segment files with a self-describing, checksummed header;
/// the index is rebuilt from the segments on open. Writes are durable when <see cref="PutAsync"/> returns.
/// </summary>
/// <remarks>
/// <para><b>Records.</b> Each record header carries the object identity (id, volume, staging session), plaintext and
/// stored lengths, encoding, key version, the XxHash128 of the plaintext and one seeded XxHash64 per 64 KiB unit, all
/// covered by a CRC-32C. Range reads verify (and decrypt) only the units they touch; a read never returns unverified
/// bytes. Deletes write durable tombstones; quarantine records durably retire one damaged record.</para>
/// <para><b>Recovery.</b> Opening finishes or rolls back an interrupted compaction, scans every segment in order and
/// rebuilds the index. Damaged regions are skipped and reported in <see cref="Quarantined"/> (zero-filled regions left
/// by lost unflushed writes are skipped silently); damaged records are never indexed. In the active (last) segment every
/// payload is verified, a torn tail is truncated and whatever was recovered is flushed before use.</para>
/// <para><b>Capacity.</b> Puts are rejected with <see cref="DfsCapacityException"/> when the medium's used bytes plus the
/// record would exceed its capacity minus <see cref="ObjectStoreOptions.ReservedBytes"/>; deletes, quarantine and
/// compaction may use the reserve.</para>
/// <para><b>Concurrency.</b> Intended for one node loop; calls may interleave at awaits and the store keeps its index
/// consistent across such interleavings (internal state is additionally guarded for callers on other threads).</para>
/// </remarks>
public sealed partial class ObjectStore : IAsyncDisposable
{
    private readonly IStorageMedium _medium;
    private readonly string _directory;
    private readonly ObjectStoreOptions _options;
    private readonly IFaultInjector _faults;
    private readonly IRecordCipher? _cipher;
    private readonly int _cipherOverhead;
    private readonly Lock _sync = new();
    private readonly Dictionary<ObjectId, IndexEntry> _index = [];
    private readonly SortedSet<ObjectId> _order = new(ObjectIdComparer.Instance);
    private readonly List<SegmentFile> _segments = [];
    private readonly List<QuarantinedRecord> _quarantined = [];
    private readonly Dictionary<ObjectId, long> _recentDeletes = [];
    private readonly SerialGate _writeGate = new();
    private readonly SerialGate _compactionGate = new();
    private SegmentFile? _active;
    private CompactionRun? _pendingCompaction;
    private ulong _nextMajor = 1;
    private long _liveBytes;
    private long _checksumFailures;
    private long _writeSequence;
    private int _inflightPuts;
    private ObjectId? _scrubCursor;
    private bool _disposed;

    private ObjectStore(IStorageMedium medium, string directory, ObjectStoreOptions options, IFaultInjector faults)
    {
        _medium = medium;
        _directory = directory;
        _options = options;
        _faults = faults;
        _cipher = options.Cipher;
        _cipherOverhead = _cipher is null ? 0 : CipherSupport.ValidateOverhead(_cipher);
    }

    /// <summary>Total bytes of live (referenced by the index) records.</summary>
    public long LiveBytes
    {
        get
        {
            lock (_sync)
                return _liveBytes;
        }
    }

    /// <summary>Total bytes of all segment files, including dead records awaiting compaction.</summary>
    public long TotalBytes
    {
        get
        {
            lock (_sync)
                return TotalBytesLocked();
        }
    }

    /// <summary>Number of indexed objects.</summary>
    public int ObjectCount
    {
        get
        {
            lock (_sync)
                return _index.Count;
        }
    }

    /// <summary>Number of verification failures observed (reads, scrub, compaction, open).</summary>
    public long ChecksumFailures => Interlocked.Read(ref _checksumFailures);

    /// <summary>Records quarantined by this instance (at open, by reads of idempotent puts, by scrub or compaction).</summary>
    public IReadOnlyList<QuarantinedRecord> Quarantined
    {
        get
        {
            lock (_sync)
                return [.. _quarantined];
        }
    }

    /// <summary>The store directory (medium-relative, normalized).</summary>
    public string Directory => _directory;

    /// <summary>Opens (or creates) the store in <paramref name="directory"/>, rebuilding the index from the segments.</summary>
    /// <exception cref="DfsIntegrityException">An interrupted compaction cannot be completed or rolled back.</exception>
    /// <exception cref="DfsRecoveryKeyMissingException">Records in the active segment are encrypted and their key is unavailable.</exception>
    /// <exception cref="DfsProtocolException">A segment or record uses a newer format version.</exception>
    public static async ValueTask<ObjectStore> OpenAsync(IStorageMedium medium, string directory, ObjectStoreOptions options, IFaultInjector faults, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(medium);
        ArgumentNullException.ThrowIfNull(directory);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(faults);
        if (options.SegmentSize < SegmentHeaderSize + FixedHeaderSize + 4)
            throw new ArgumentOutOfRangeException(nameof(options), $"SegmentSize must be at least {SegmentHeaderSize + FixedHeaderSize + 4} bytes.");
        if (options.ReservedBytes < 0)
            throw new ArgumentOutOfRangeException(nameof(options), "ReservedBytes must not be negative.");
        if (options.MaxObjectSize < 0)
            throw new ArgumentOutOfRangeException(nameof(options), "MaxObjectSize must not be negative.");
        ArgumentNullException.ThrowIfNull(options.Mutations, nameof(options));
        ArgumentNullException.ThrowIfNull(options.NodeName, nameof(options));

        string dir = StoragePath.Normalize(directory);
        if (!await medium.ExistsAsync(dir, ct))
            await medium.CreateDirectoryDurableAsync(dir, ct);

        var store = new ObjectStore(medium, dir, options, faults);
        try
        {
            await store.LoadAsync(ct);
        }
        catch
        {
            await store.CloseAllFilesAsync();
            throw;
        }

        return store;
    }

    /// <summary>Stores an object durably (flush barrier before return). Idempotent for identical content; conflicting content for the same id throws <see cref="DfsIntegrityException"/>.</summary>
    /// <remarks>
    /// When the id is already stored with the same volume, length and checksum, the existing copy is re-verified: an
    /// intact copy bound to the same staging session is returned as is; a copy bound to another staging session is
    /// rewritten under the new session (so garbage collection sees the newest owner); a damaged copy is quarantined
    /// and replaced.
    /// </remarks>
    /// <exception cref="ArgumentException"><paramref name="id"/> is empty.</exception>
    /// <exception cref="DfsLimitExceededException">The object exceeds <see cref="ObjectStoreOptions.MaxObjectSize"/>.</exception>
    /// <exception cref="DfsIntegrityException">The id is stored with different content or volume.</exception>
    /// <exception cref="DfsCapacityException">Admission would consume the reserved capacity, or the device is full.</exception>
    public async ValueTask<ObjectRecordInfo> PutAsync(ObjectId id, VolumeId volume, StagingId staging, ReadOnlyMemory<byte> plaintext, CancellationToken ct = default)
    {
        if (id.IsEmpty)
            throw new ArgumentException("The object id must not be empty.", nameof(id));
        if (plaintext.Length > _options.MaxObjectSize)
            throw new DfsLimitExceededException("MaxObjectSize", _options.MaxObjectSize, plaintext.Length);
        ThrowIfDisposed();
        UInt128 checksum = ObjectChecksum.Compute(plaintext.Span);
        if (_faults.IsEnabled)
            await _faults.HitAsync(new FaultContext(FaultStage.F06, FaultSites.ObjectWrite, _options.NodeName, Operation: DfsOperationKind.PutObject, Object: id), ct);

        SegmentFile segment;
        IndexEntry entry;
        long sequence;
        using (await _writeGate.EnterAsync(ct))
        {
            ThrowIfDisposed();
            IndexEntry? existing = Lookup(id);
            byte[]? quarantine = null;
            if (existing is not null)
            {
                ObjectRecordInfo info = existing.Info;
                if (info.Volume != volume || info.Checksum != checksum || info.Length != plaintext.Length)
                    throw new DfsIntegrityException(id, $"Object {id} is already stored with different content or owner; objects are immutable.");
                bool intact = await VerifyGuardedAsync(existing, ct);
                if (intact && info.Staging == staging)
                    return info;
                if (!intact)
                    quarantine = BuildMarkerRecord(KindQuarantine, id, info.Volume, existing.Segment.Key, existing.Offset);
            }

            byte[] record = BuildObjectRecord(id, volume, staging, plaintext.Span, checksum, out RecordHeader header);
            byte[] buffer = quarantine is null ? record : [.. quarantine, .. record];
            EnsureCapacity(buffer.Length);
            (segment, long offset) = await AppendLockedAsync(buffer, quarantine is null ? 1 : 2, ct);
            if (quarantine is not null)
                RetireDamaged(existing!, "copy failed verification during an idempotent put");

            entry = new IndexEntry(InfoOf(header), segment, offset + (quarantine?.Length ?? 0), header);
            lock (_sync)
            {
                sequence = ++_writeSequence;
                entry.Sequence = sequence;
                if (_options.Mutations.AcknowledgeBeforeFlush)
                {
                    // Negative control (MUT-*): acknowledge without the durability barrier.
                    IndexLocked(entry);
                    return entry.Info;
                }

                segment.InflightWrites++;
                _inflightPuts++;
            }
        }

        try
        {
            if (_faults.IsEnabled)
                await _faults.HitAsync(new FaultContext(FaultStage.F07, FaultSites.ObjectBeforeFlush, _options.NodeName, Operation: DfsOperationKind.PutObject, Object: id), ct);
            IStorageFile file = segment.File ?? throw new ObjectDisposedException(nameof(ObjectStore));
            try
            {
                await file.FlushAsync(ct);
            }
            catch (Exception ex) when (StorageErrors.IsDiskFull(ex))
            {
                throw new DfsCapacityException($"Storage is full while flushing object {id}.", ex);
            }

            lock (_sync)
            {
                ThrowIfDisposed();
                bool deletedLater = _recentDeletes.TryGetValue(id, out long deletedAt) && deletedAt > sequence;
                bool newerCopy = _index.TryGetValue(id, out IndexEntry? current) && current.Sequence > sequence;
                if (!deletedLater && !newerCopy)
                    IndexLocked(entry);
            }

            return entry.Info;
        }
        finally
        {
            TaskCompletionSource? drained = null;
            lock (_sync)
            {
                if (--segment.InflightWrites == 0)
                {
                    drained = segment.Drained;
                    segment.Drained = null;
                }

                if (--_inflightPuts == 0)
                    _recentDeletes.Clear();
            }

            drained?.TrySetResult();
        }
    }

    /// <summary>Reads verified plaintext bytes; corruption or a missing object throws <see cref="DfsIntegrityException"/>.</summary>
    /// <returns>The number of bytes read (0 at or beyond the end of the object).</returns>
    /// <exception cref="DfsObjectMissingException">The object is not stored here.</exception>
    /// <exception cref="DfsIntegrityException">A touched unit (or, for a whole-object read, the object checksum) failed verification.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="offset"/> is negative or beyond the object length.</exception>
    public async ValueTask<int> ReadAsync(ObjectId id, long offset, Memory<byte> destination, CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        IndexEntry e = AcquireForRead(id);
        try
        {
            long length = e.Info.Length;
            if (offset > length)
                throw new ArgumentOutOfRangeException(nameof(offset), $"Offset {offset} is beyond the end of object {id} ({length} bytes).");
            int count = (int)Math.Min(destination.Length, length - offset);
            if (count == 0)
                return 0;

            bool verify = !_options.Mutations.SkipChecksumVerification;
            int unitSize = e.Header.UnitSize;
            int firstUnit = (int)(offset / unitSize);
            int lastUnit = (int)((offset + count - 1) / unitSize);
            long storedStart = e.StoredUnitOffset(firstUnit);
            long storedEnd = e.StoredUnitOffset(lastUnit) + e.StoredUnitLength(lastUnit);
            var stored = new byte[storedEnd - storedStart];
            try
            {
                await e.Segment.File!.ReadFullyAsync(e.PayloadOffset + storedStart, stored, ct);
            }
            catch (EndOfStreamException ex)
            {
                CountFailure();
                throw new DfsIntegrityException(id, $"Object {id} is truncated in '{e.Segment.Path}'.", ex);
            }

            WholeObjectHash? whole = verify && offset == 0 && count == length ? new WholeObjectHash() : null;
            var plain = new byte[Math.Min(unitSize, length)];
            int written = 0;
            for (int u = firstUnit; u <= lastUnit; u++)
            {
                int plainLength = e.PlainUnitLength(u);
                Span<byte> unit = plain.AsSpan(0, plainLength);
                if (!TryOpenUnit(e, u, stored.AsSpan((int)(e.StoredUnitOffset(u) - storedStart), e.StoredUnitLength(u)), unit, verify))
                {
                    CountFailure();
                    throw new DfsIntegrityException(id, $"Object {id} failed verification in unit {u} ('{e.Segment.Path}' offset {e.Offset}).");
                }

                whole?.Append(unit);
                long unitStart = (long)u * unitSize;
                int from = (int)Math.Max(0, offset - unitStart);
                int to = (int)Math.Min(plainLength, offset + count - unitStart);
                unit[from..to].CopyTo(destination.Span[written..]);
                written += to - from;
            }

            if (whole is not null && whole.Value != e.Info.Checksum)
            {
                CountFailure();
                throw new DfsIntegrityException(id, $"Object {id} failed whole-object checksum verification.");
            }

            return count;
        }
        finally
        {
            await ReleaseAsync(e.Segment);
        }
    }

    /// <summary>True when the store holds a record for <paramref name="id"/>.</summary>
    public ValueTask<bool> ContainsAsync(ObjectId id, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        lock (_sync)
        {
            ThrowIfDisposed();
            return new ValueTask<bool>(_index.ContainsKey(id));
        }
    }

    /// <summary>Returns the index entry for <paramref name="id"/>, or null when absent.</summary>
    public ObjectRecordInfo? TryGetInfo(ObjectId id)
    {
        lock (_sync)
        {
            ThrowIfDisposed();
            return _index.TryGetValue(id, out IndexEntry? e) ? e.Info : null;
        }
    }

    /// <summary>Deletes an object durably (tombstone). Deleting an absent object is a no-op.</summary>
    public async ValueTask DeleteAsync(ObjectId id, CancellationToken ct = default)
    {
        using SerialGate.Releaser _ = await _writeGate.EnterAsync(ct);
        ThrowIfDisposed();
        IndexEntry? existing = Lookup(id);
        if (existing is null)
            return;
        byte[] tombstone = BuildMarkerRecord(KindTombstone, id, existing.Info.Volume);
        (SegmentFile segment, long offset) = await AppendLockedAsync(tombstone, 1, ct);
        try
        {
            await segment.File!.FlushAsync(ct);
        }
        catch
        {
            await RollbackAppendAsync(segment, offset, tombstone.Length, 1);
            throw;
        }

        lock (_sync)
        {
            long sequence = ++_writeSequence;
            if (_index.TryGetValue(id, out IndexEntry? current) && ReferenceEquals(current, existing))
                RemoveLocked(id);
            if (_inflightPuts > 0)
                _recentDeletes[id] = sequence;
        }
    }

    /// <summary>Lists the index, optionally for one volume, ordered by object id.</summary>
    public IReadOnlyList<ObjectRecordInfo> List(VolumeId? volume = null)
    {
        lock (_sync)
        {
            ThrowIfDisposed();
            var result = new List<ObjectRecordInfo>(_index.Count);
            foreach (ObjectId id in _order)
            {
                ObjectRecordInfo info = _index[id].Info;
                if (volume is null || info.Volume == volume.Value)
                    result.Add(info);
            }

            return result;
        }
    }

    /// <summary>Physical placement of an object's record, or null when absent (diagnostics and corruption tests).</summary>
    public ObjectPhysicalLocation? GetPhysicalLocation(ObjectId id)
    {
        lock (_sync)
        {
            ThrowIfDisposed();
            if (!_index.TryGetValue(id, out IndexEntry? e))
                return null;
            return new ObjectPhysicalLocation(e.Segment.Path, e.Offset, e.Header.HeaderLength, e.PayloadOffset, e.Header.StoredLength,
                e.Header.UnitSize, e.Header.Encoding);
        }
    }

    /// <summary>Closes the segment files. Later calls throw <see cref="ObjectDisposedException"/>.</summary>
    public async ValueTask DisposeAsync()
    {
        lock (_sync)
        {
            if (_disposed)
                return;
            _disposed = true;
        }

        await CloseAllFilesAsync();
    }

    /// <inheritdoc />
    public override string ToString() => $"ObjectStore({_medium.Name}:{_directory})";

    private static ObjectRecordInfo InfoOf(RecordHeader h)
        => new(h.Id, h.Volume, h.Staging, h.PlaintextLength, h.Checksum, h.Encoding, h.KeyVersion);

    private string PathOf(string fileName) => StoragePath.Combine(_directory, fileName);

    private string ManifestPath => PathOf(ManifestFileName);

    private void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(_disposed, this);

    private void CountFailure() => Interlocked.Increment(ref _checksumFailures);

    private long TotalBytesLocked()
    {
        long total = 0;
        foreach (SegmentFile s in _segments)
            total += s.Length;
        return total;
    }

    private IndexEntry? Lookup(ObjectId id)
    {
        lock (_sync)
            return _index.TryGetValue(id, out IndexEntry? e) ? e : null;
    }

    private void IndexLocked(IndexEntry entry)
    {
        ObjectId id = entry.Info.Id;
        if (_index.TryGetValue(id, out IndexEntry? previous))
            _liveBytes -= previous.RecordLength;
        else
            _order.Add(id);
        _index[id] = entry;
        _liveBytes += entry.RecordLength;
    }

    private void RemoveLocked(ObjectId id)
    {
        if (_index.Remove(id, out IndexEntry? previous))
        {
            _liveBytes -= previous.RecordLength;
            _order.Remove(id);
        }
    }

    /// <summary>Removes a damaged entry from the index (if still current) and records the quarantine.</summary>
    private void RetireDamaged(IndexEntry entry, string reason)
    {
        lock (_sync)
        {
            if (_index.TryGetValue(entry.Info.Id, out IndexEntry? current) && ReferenceEquals(current, entry))
                RemoveLocked(entry.Info.Id);
            _quarantined.Add(new QuarantinedRecord(entry.Info.Id, entry.Segment.Path, entry.Offset, reason));
        }

        CountFailure();
    }

    private void EnsureCapacity(long recordBytes)
    {
        SegmentFile? active = _active;
        bool rollover = active is null || (active.RecordCount > 0 && active.Length + recordBytes > _options.SegmentSize);
        long needed = recordBytes + (rollover ? SegmentHeaderSize : 0);
        long limit = _medium.CapacityBytes - _options.ReservedBytes;
        long used = _medium.UsedBytes;
        if (needed > limit - used)
        {
            throw new DfsCapacityException(
                $"Object store on '{_medium.Name}' cannot admit {needed} bytes: {used} of {_medium.CapacityBytes} bytes used and {_options.ReservedBytes} bytes reserved.");
        }
    }

    /// <summary>Appends records to the active segment. The caller holds the write gate; nothing is flushed.</summary>
    private async ValueTask<(SegmentFile Segment, long Offset)> AppendLockedAsync(byte[] buffer, int recordCount, CancellationToken ct)
    {
        SegmentFile segment = await EnsureActiveSegmentAsync(buffer.Length, ct);
        long offset = segment.Length;
        try
        {
            await segment.File!.WriteFullyAsync(offset, buffer, ct);
        }
        catch (Exception ex)
        {
            await TruncateQuietlyAsync(segment, offset);
            if (StorageErrors.IsDiskFull(ex))
                throw new DfsCapacityException($"Storage is full while writing to '{segment.Path}'.", ex);
            throw;
        }

        lock (_sync)
        {
            segment.Length = offset + buffer.Length;
            segment.RecordCount += recordCount;
        }

        return (segment, offset);
    }

    /// <summary>Undoes an append whose flush failed (the caller still holds the write gate).</summary>
    private async ValueTask RollbackAppendAsync(SegmentFile segment, long offset, int length, int recordCount)
    {
        lock (_sync)
        {
            if (segment.Length != offset + length)
                return;
            segment.Length = offset;
            segment.RecordCount -= recordCount;
        }

        await TruncateQuietlyAsync(segment, offset);
    }

    private static async ValueTask TruncateQuietlyAsync(SegmentFile segment, long length)
    {
        try
        {
            if (segment.File is { } file)
                await file.SetLengthAsync(length, CancellationToken.None);
        }
        catch (Exception cleanup) when (StorageFormat.IsCleanupTolerable(cleanup))
        {
            // Leftover bytes past the logical end are overwritten by the next append or truncated at the next open.
        }
    }

    private async ValueTask<SegmentFile> EnsureActiveSegmentAsync(int recordBytes, CancellationToken ct)
    {
        SegmentFile? active = _active;
        if (active is not null && !(active.RecordCount > 0 && active.Length + recordBytes > _options.SegmentSize))
            return active;

        var key = new SegmentKey(_nextMajor++, 0);
        string path = PathOf(SegmentFileName(key));
        IStorageFile file = await _medium.OpenAsync(path, StorageOpenMode.Truncate, ct);
        try
        {
            await file.WriteFullyAsync(0, EncodeSegmentHeader(key), ct);
            await file.FlushAsync(ct);
            await _medium.FlushDirectoryAsync(_directory, ct);
        }
        catch (Exception ex)
        {
            await StorageFormat.CloseQuietlyAsync(file);
            try
            {
                await _medium.DeleteAsync(path, CancellationToken.None);
            }
            catch (Exception cleanup) when (StorageFormat.IsCleanupTolerable(cleanup))
            {
            }

            if (StorageErrors.IsDiskFull(ex))
                throw new DfsCapacityException($"Storage is full while creating segment '{path}'.", ex);
            throw;
        }

        var segment = new SegmentFile(key, path) { File = file, Length = SegmentHeaderSize };
        lock (_sync)
        {
            _segments.Add(segment);
            _segments.Sort(static (a, b) => a.Key.CompareTo(b.Key));
        }

        _active = segment;
        return segment;
    }

    private IndexEntry AcquireForRead(ObjectId id)
    {
        lock (_sync)
        {
            ThrowIfDisposed();
            if (!_index.TryGetValue(id, out IndexEntry? e))
                throw new DfsObjectMissingException(id);
            if (e.Segment.File is null)
                throw new DfsIntegrityException(id, $"Object {id} refers to a closed segment '{e.Segment.Path}'.");
            e.Segment.Refs++;
            return e;
        }
    }

    /// <summary>Takes a read reference on the entry's segment when the entry is still current.</summary>
    private bool TryAcquire(IndexEntry e)
    {
        lock (_sync)
        {
            if (_disposed || e.Segment.File is null)
                return false;
            if (!_index.TryGetValue(e.Info.Id, out IndexEntry? current) || !ReferenceEquals(current, e))
                return false;
            e.Segment.Refs++;
            return true;
        }
    }

    private async ValueTask ReleaseAsync(SegmentFile segment)
    {
        IStorageFile? close = null;
        lock (_sync)
        {
            segment.Refs--;
            if (segment.Refs == 0 && (segment.Retired || _disposed))
            {
                close = segment.File;
                segment.File = null;
            }
        }

        await StorageFormat.CloseQuietlyAsync(close);
    }

    /// <summary>Verifies an entry's full payload while holding a read reference; false when damaged or unreadable.</summary>
    private async ValueTask<bool> VerifyGuardedAsync(IndexEntry e, CancellationToken ct)
    {
        if (!TryAcquire(e))
            return false;
        try
        {
            return await VerifyPayloadAsync(e.Segment, e.PayloadOffset, e.Header, e.Info.Checksum, ct);
        }
        catch (IOException ex) when (ex is not DfsException)
        {
            return false;
        }
        finally
        {
            await ReleaseAsync(e.Segment);
        }
    }

    /// <summary>Reads and verifies every unit and the whole-object checksum of a stored payload.</summary>
    private async ValueTask<bool> VerifyPayloadAsync(SegmentFile segment, long payloadOffset, RecordHeader h, UInt128 checksum, CancellationToken ct)
    {
        var view = new IndexEntry(InfoOf(h), segment, payloadOffset - h.HeaderLength, h);
        int unitCount = h.UnitHashes.Length;
        const int UnitsPerRead = 16;
        var whole = new WholeObjectHash();
        var plain = new byte[Math.Min(h.UnitSize, Math.Max(h.PlaintextLength, 1))];
        for (int first = 0; first < unitCount; first += UnitsPerRead)
        {
            int last = Math.Min(unitCount, first + UnitsPerRead) - 1;
            long start = view.StoredUnitOffset(first);
            long end = view.StoredUnitOffset(last) + view.StoredUnitLength(last);
            var stored = new byte[end - start];
            try
            {
                await segment.File!.ReadFullyAsync(payloadOffset + start, stored, ct);
            }
            catch (EndOfStreamException)
            {
                return false;
            }

            for (int u = first; u <= last; u++)
            {
                Span<byte> unit = plain.AsSpan(0, view.PlainUnitLength(u));
                if (!TryOpenUnit(view, u, stored.AsSpan((int)(view.StoredUnitOffset(u) - start), view.StoredUnitLength(u)), unit, verify: true))
                    return false;
                whole.Append(unit);
            }
        }

        return whole.Value == checksum;
    }

    /// <summary>Decrypts (when sealed) one unit into <paramref name="plain"/> and checks its unit hash.</summary>
    private bool TryOpenUnit(IndexEntry e, int unit, ReadOnlySpan<byte> stored, Span<byte> plain, bool verify)
    {
        RecordHeader h = e.Header;
        if (h.Encoding == ObjectEncoding.Plain)
        {
            if (stored.Length != plain.Length)
                return false;
            stored.CopyTo(plain);
        }
        else
        {
            Span<byte> aad = stackalloc byte[UnitAadSize];
            WriteUnitAad(aad, h.Id, h.Volume, unit, h.PlaintextLength);
            if (!CipherSupport.TryDecryptInto(_cipher, aad, stored, plain, h.KeyVersion, $"Object {h.Id}"))
                return false;
        }

        return !verify || UnitHash(h.Id, unit, plain) == h.UnitHashes[unit];
    }

    private async ValueTask CloseAllFilesAsync()
    {
        List<IStorageFile> files = [];
        lock (_sync)
        {
            IEnumerable<SegmentFile> all = _pendingCompaction is { Switched: false } pending ? _segments.Concat(pending.Outputs) : _segments;
            foreach (SegmentFile s in all)
            {
                if (s.File is not null && s.Refs == 0)
                {
                    files.Add(s.File);
                    s.File = null;
                }
            }

            _active = null;
        }

        foreach (IStorageFile f in files)
            await StorageFormat.CloseQuietlyAsync(f);
    }

    /// <summary>Ordinal comparer of object ids (by 128-bit value).</summary>
    private sealed class ObjectIdComparer : IComparer<ObjectId>
    {
        public static readonly ObjectIdComparer Instance = new();

        public int Compare(ObjectId x, ObjectId y) => x.Value.CompareTo(y.Value);
    }

    /// <summary>One segment file and its bookkeeping.</summary>
    private sealed class SegmentFile(SegmentKey key, string path)
    {
        public SegmentKey Key { get; } = key;

        public string Path { get; } = path;

        /// <summary>Path while the file is an unpublished compaction output.</summary>
        public string? CompactingPath { get; init; }

        public IStorageFile? File { get; set; }

        public long Length { get; set; }

        public int RecordCount { get; set; }

        /// <summary>Readers holding the file open.</summary>
        public int Refs { get; set; }

        /// <summary>Puts written to this segment whose flush and indexing have not finished.</summary>
        public int InflightWrites { get; set; }

        /// <summary>Signalled when <see cref="InflightWrites"/> drops to zero (compaction waits on it).</summary>
        public TaskCompletionSource? Drained { get; set; }

        /// <summary>Replaced by a compaction; the file is closed when the last reader releases it.</summary>
        public bool Retired { get; set; }
    }

    /// <summary>Index entry: where an object's current record lives.</summary>
    private sealed class IndexEntry(ObjectRecordInfo info, SegmentFile segment, long offset, RecordHeader header)
    {
        public ObjectRecordInfo Info { get; } = info;

        public SegmentFile Segment { get; } = segment;

        public long Offset { get; } = offset;

        public RecordHeader Header { get; } = header;

        /// <summary>Write order among records written by this instance (0 for records loaded at open).</summary>
        public long Sequence { get; set; }

        public long PayloadOffset => Offset + Header.HeaderLength;

        public long RecordLength => Header.RecordLength;

        private int UnitOverhead => Header.UnitHashes.Length == 0 ? 0 : (int)((Header.StoredLength - Header.PlaintextLength) / Header.UnitHashes.Length);

        public int PlainUnitLength(int unit) => (int)Math.Min(Header.UnitSize, Header.PlaintextLength - ((long)unit * Header.UnitSize));

        public long StoredUnitOffset(int unit) => (long)unit * (Header.UnitSize + UnitOverhead);

        public int StoredUnitLength(int unit) => PlainUnitLength(unit) + UnitOverhead;
    }
}
