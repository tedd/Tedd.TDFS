namespace Tedd.Tdfs.Storage;

// Compaction and scrubbing.
//
// Compaction is incremental: one step rewrites one segment (oldest first), so the extra space it needs is bounded by one
// segment of live records and always fits in the reserve, and a store filled to its admission limit can still reclaim
// space. A step (crash-safe at every point):
//   1. The input segment S is sealed when it is the active one (new puts go to a segment with a higher major) and
//      in-flight puts written to it are waited for.                                    [fault site compaction.copy]
//   2. Its records are copied in file order into an ".ocmp" output keyed (S.Major, S.Minor + 1): the key orders after S
//      and before every newer segment, so replay order is unchanged. Live object records are re-verified (damaged ones
//      are quarantined instead); dead object records and damaged regions are dropped; tombstones and quarantine records
//      are dropped only when nothing they act on can remain (below). The output and the directory are flushed.
//   3. The manifest (inputs + outputs) is written durably with AtomicFile: the commit point. [compaction.publish]
//   4. The output is renamed to ".oseg", the directory flushed, the in-memory index switched.
//   5. The input is deleted, the directory flushed, the manifest deleted.              [compaction.delete]
// Recovery: a manifest whose outputs all exist is rolled forward; otherwise (or without a manifest) ".ocmp" files are
// discarded and the inputs remain authoritative.
//
// Markers. A tombstone acts on older records of its object; a quarantine record retires the record at its target
// location. When every segment older than S is clean (holds no dead object record: everything in it is referenced by
// the index or is itself a marker), no older record a marker of S could act on exists any more (a live record older
// than a tombstone of the same object cannot exist), so S's markers are dropped. Otherwise they are kept, except
// quarantine records whose target segment is gone or is an input of the step. A pass visits segments oldest first, so
// the segments before S are compacted (made clean) before S is reached.
public sealed partial class ObjectStore
{
    /// <summary>Compacts every segment that holds reclaimable bytes (dead records, droppable markers), oldest first (crash-safe).</summary>
    /// <remarks>
    /// Does nothing when no segment holds reclaimable bytes. The active segment is sealed first when it holds any.
    /// Compaction may use the reserved capacity; each step needs at most one segment of extra space. An interrupted
    /// compaction that already reached its commit point is completed by the next call (or by the next open).
    /// </remarks>
    public async ValueTask CompactAsync(CancellationToken ct = default) => await CompactSegmentsAsync(0, ct);

    /// <summary>
    /// Compacts, oldest first, every segment whose reclaimable bytes reach <paramref name="minReclaimableFraction"/> of
    /// the configured segment size (0: any reclaimable byte). Segments created while the pass runs are left to the next pass.
    /// </summary>
    /// <param name="minReclaimableFraction">Threshold as a fraction of <see cref="ObjectStoreOptions.SegmentSize"/>, 0..1.</param>
    /// <param name="ct">Cancellation (checked between steps and inside copies).</param>
    /// <returns>The number of segments compacted.</returns>
    public async ValueTask<int> CompactSegmentsAsync(double minReclaimableFraction, CancellationToken ct = default)
    {
        if (double.IsNaN(minReclaimableFraction) || minReclaimableFraction < 0 || minReclaimableFraction > 1)
            throw new ArgumentOutOfRangeException(nameof(minReclaimableFraction), "The threshold must be between 0 and 1.");
        using SerialGate.Releaser _ = await _compactionGate.EnterAsync(ct);
        ThrowIfDisposed();
        if (_pendingCompaction is { } pending)
            await CompleteCommittedAsync(pending, ct);

        long threshold = Math.Max(1, (long)(minReclaimableFraction * _options.SegmentSize));
        SegmentKey bound;
        lock (_sync)
        {
            if (_segments.Count == 0)
                return 0;
            bound = _segments[^1].Key;
        }

        var produced = new HashSet<SegmentKey>();
        int compacted = 0;
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            List<SegmentFile> inputs;
            bool dropMarkers;
            using (await _writeGate.EnterAsync(ct))
            {
                ThrowIfDisposed();
                (inputs, dropMarkers) = PickCompactionInputs(threshold, bound, produced);
                if (inputs.Count == 0)
                    break;

                // New writes go to a newer segment from now on, so the current one stops being the newest (the only one
                // payload-verified at open): make it complete and durable first, including records of puts that are
                // still waiting for their own barrier.
                if (_active is { } active && inputs.Contains(active))
                {
                    await SealLockedAsync(active, ct);
                    _active = null;
                }
            }

            await WaitForInflightWritesAsync(inputs);
            await CompactStepAsync(inputs, dropMarkers, produced, ct);
            compacted += inputs.Count;
        }

        return compacted;
    }

    /// <summary>
    /// Chooses the next step's inputs: the oldest segment (up to <paramref name="bound"/>, not produced by this pass) whose
    /// reclaimable bytes reach <paramref name="threshold"/>, extended by the newer segments of its major when the key
    /// after it is taken (layouts written by multi-output compactions). Called under the write gate.
    /// </summary>
    private (List<SegmentFile> Inputs, bool DropMarkers) PickCompactionInputs(long threshold, SegmentKey bound, HashSet<SegmentKey> produced)
    {
        lock (_sync)
        {
            bool olderClean = true;
            for (int i = 0; i < _segments.Count; i++)
            {
                SegmentFile s = _segments[i];
                if (s.Key.CompareTo(bound) > 0)
                    break;
                long reclaimable = s.DeadObjectBytes + (olderClean ? s.MarkerBytes : 0);
                if (!produced.Contains(s.Key) && reclaimable > 0 && reclaimable >= threshold)
                {
                    var inputs = new List<SegmentFile> { s };
                    for (int j = i + 1; j < _segments.Count && _segments[j].Key.Major == s.Key.Major; j++)
                        inputs.Add(_segments[j]);
                    return (inputs, olderClean);
                }

                if (s.DeadObjectBytes > 0)
                    olderClean = false;
            }

            return ([], false);
        }
    }

    /// <summary>One compaction step: copy, commit, publish and delete the inputs.</summary>
    private async ValueTask CompactStepAsync(List<SegmentFile> inputs, bool dropMarkers, HashSet<SegmentKey> produced, CancellationToken ct)
    {
        if (_faults.IsEnabled)
            await _faults.HitAsync(new FaultContext(FaultStage.F20, FaultSites.CompactionCopy, _options.NodeName, Operation: DfsOperationKind.Compaction), ct);

        var run = new CompactionRun(inputs);
        try
        {
            await CopyRecordsAsync(run, dropMarkers, ct);
            await _medium.FlushDirectoryAsync(_directory, ct);
            if (_faults.IsEnabled)
                await _faults.HitAsync(new FaultContext(FaultStage.F20, FaultSites.CompactionPublish, _options.NodeName, Operation: DfsOperationKind.Compaction), ct);
            ThrowIfDisposed();
            await AtomicFile.WriteAsync(_medium, ManifestPath, EncodeManifest([.. run.Inputs.Select(s => s.Key)], [.. run.Outputs.Select(s => s.Key)]), ct);
            run.Committed = true;
        }
        catch (Exception ex)
        {
            if (!run.Committed)
                await AbandonOutputsAsync(run);
            if (StorageErrors.IsDiskFull(ex) && ex is not DfsCapacityException)
                throw new DfsCapacityException("Storage is full while compacting the object store.", ex);
            throw;
        }

        foreach (SegmentFile output in run.Outputs)
            produced.Add(output.Key);
        _pendingCompaction = run;
        await CompleteCommittedAsync(run, ct);
    }

    /// <summary>Verifies up to <paramref name="maxObjects"/> stored objects and quarantines corrupt ones.</summary>
    /// <remarks>
    /// Objects are visited in object-id order, continuing after the previous pass (wrapping around). A record that fails
    /// verification or cannot be read is removed from the index and retired with a durable quarantine record; it is
    /// reported in the result and in <see cref="Quarantined"/>.
    /// </remarks>
    public async ValueTask<ScrubResult> ScrubAsync(int maxObjects, CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(maxObjects);
        ThrowIfDisposed();
        List<IndexEntry> batch = PickScrubBatch(maxObjects);
        var corrupt = new List<IndexEntry>();
        int checkedCount = 0;
        long checkedBytes = 0;
        foreach (IndexEntry e in batch)
        {
            ct.ThrowIfCancellationRequested();
            if (!TryAcquire(e))
                continue;
            bool intact;
            try
            {
                intact = await VerifyRecordAsync(e, ct);
            }
            finally
            {
                await ReleaseAsync(e.Segment);
            }

            checkedCount++;
            checkedBytes += e.Info.Length;
            if (!intact)
                corrupt.Add(e);
        }

        if (corrupt.Count > 0)
            await QuarantineAsync(corrupt, "failed verification during scrub", ct);
        return new ScrubResult(checkedCount, [.. corrupt.Select(e => e.Info.Id)]) { CheckedBytes = checkedBytes };
    }

    /// <summary>
    /// Verifies one stored object now (its durable record header and every payload unit against the whole-object
    /// checksum). A damaged copy is quarantined durably (removed from the index, retired with a quarantine record) and
    /// reported in <see cref="Quarantined"/>.
    /// </summary>
    /// <returns>True when the store holds an intact copy; false when it holds none (absent, or damaged and quarantined now).</returns>
    /// <exception cref="ObjectDisposedException">The store was closed.</exception>
    public async ValueTask<bool> VerifyAsync(ObjectId id, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        for (int attempt = 0; attempt < 3; attempt++)
        {
            IndexEntry? e = Lookup(id);
            if (e is null)
                return false;
            switch (TryAcquireCurrent(e))
            {
                case AcquireResult.NotCurrent:
                    continue; // replaced or deleted meanwhile: judge the current record
                case AcquireResult.Closed:
                    throw new ObjectDisposedException(nameof(ObjectStore), $"The object store was closed while verifying object {id}.");
            }

            bool intact;
            try
            {
                intact = await VerifyRecordAsync(e, ct);
            }
            finally
            {
                await ReleaseAsync(e.Segment);
            }

            if (intact)
                return true;
            await QuarantineAsync([e], "failed verification", ct);
            return false;
        }

        return Lookup(id) is not null;
    }

    /// <summary>Verifies an acquired entry's durable header and payload; unreadable counts as damaged.</summary>
    private async ValueTask<bool> VerifyRecordAsync(IndexEntry e, CancellationToken ct)
    {
        try
        {
            await VerifyStoredHeaderAsync(e, ct);
            return await VerifyPayloadAsync(e.Segment, e.PayloadOffset, e.Header, e.Info.Checksum, ct);
        }
        catch (DfsIntegrityException)
        {
            return false;
        }
        catch (IOException ex) when (ex is not DfsException)
        {
            return false;
        }
    }

    private List<IndexEntry> PickScrubBatch(int maxObjects)
    {
        lock (_sync)
        {
            ThrowIfDisposed();
            var batch = new List<IndexEntry>(Math.Min(maxObjects, _index.Count));
            if (maxObjects == 0 || _order.Count == 0)
                return batch;
            IEnumerable<ObjectId> after = _scrubCursor is { } cursor && cursor.Value != UInt128.MaxValue
                ? _order.GetViewBetween(new ObjectId(cursor.Value + 1), new ObjectId(UInt128.MaxValue))
                : [];
            IEnumerable<ObjectId> before = _scrubCursor is { } c2
                ? _order.GetViewBetween(new ObjectId(UInt128.Zero), c2)
                : _order;
            foreach (ObjectId id in after.Concat(before))
            {
                if (batch.Count == maxObjects)
                    break;
                batch.Add(_index[id]);
            }

            if (batch.Count > 0)
                _scrubCursor = batch[^1].Info.Id;
            return batch;
        }
    }

    /// <summary>Durably retires damaged records (quarantine records) and removes them from the index.</summary>
    private async ValueTask QuarantineAsync(IReadOnlyList<IndexEntry> entries, string reason, CancellationToken ct)
    {
        using (await _writeGate.EnterAsync(ct))
        {
            ThrowIfDisposed();
            var records = new List<byte[]>();
            foreach (IndexEntry e in entries)
                records.Add(BuildMarkerRecord(KindQuarantine, e.Info.Id, e.Info.Volume, e.Segment.Key, e.Offset));
            byte[] buffer = [.. records.SelectMany(r => r)];
            try
            {
                (SegmentFile segment, long offset) = await AppendMarkersLockedAsync(buffer, records.Count, ct);
                try
                {
                    await FlushSegmentAsync(segment, ct);
                }
                catch
                {
                    await RollbackAppendAsync(segment, offset, buffer.Length, records.Count, buffer.Length);
                    throw;
                }
            }
            catch (IOException ex) when (ex is not DfsRecoveryKeyMissingException)
            {
                // The damaged records still leave the index; without the durable marker the next open re-verifies
                // the active segment and the next scrub finds sealed ones again.
            }

            foreach (IndexEntry e in entries)
                RetireDamaged(e, reason);
        }
    }

    /// <summary>Waits until every put written to <paramref name="segments"/> has been flushed and indexed (or failed).</summary>
    private async ValueTask WaitForInflightWritesAsync(IReadOnlyList<SegmentFile> segments)
    {
        foreach (SegmentFile segment in segments)
        {
            while (true)
            {
                Task? wait = null;
                lock (_sync)
                {
                    if (segment.InflightWrites > 0)
                    {
                        segment.Drained ??= new TaskCompletionSource();
                        wait = segment.Drained.Task;
                    }
                }

                if (wait is null)
                    break;
                await wait;
            }
        }
    }

    /// <summary>
    /// Copies the records of the step's inputs that must survive, in file order, into new output segments: live object
    /// records (re-verified) and the markers that are still needed. A read error or a closed store abandons the step.
    /// </summary>
    private async ValueTask CopyRecordsAsync(CompactionRun run, bool dropMarkers, CancellationToken ct)
    {
        SegmentKey lastInput = run.Inputs[^1].Key;
        uint minor = lastInput.Minor;
        var inputKeys = new HashSet<SegmentKey>(run.Inputs.Select(s => s.Key));
        SegmentFile? current = null;
        foreach (SegmentFile input in run.Inputs)
        {
            foreach ((RecordHeader h, long offset) in await ReadRecordHeadersAsync(input, ct))
            {
                ct.ThrowIfCancellationRequested();
                byte[]? raw;
                IndexEntry? live = null;
                if (h.Kind == KindObject)
                {
                    lock (_sync)
                    {
                        if (_index.TryGetValue(h.Id, out IndexEntry? e) && ReferenceEquals(e.Segment, input) && e.Offset == offset)
                            live = e;
                    }

                    if (live is null)
                        continue; // superseded, deleted or retired: dropped
                    raw = await ReadLiveRecordAsync(run, live, ct);
                    if (raw is null)
                        continue;
                }
                else
                {
                    if (dropMarkers || !MarkerStillNeeded(h, inputKeys))
                        continue;
                    raw = BuildMarkerRecord(h.Kind, h.Id, h.Volume, h.TargetKey, h.TargetOffset);
                }

                if (current is null || (current.RecordCount > 0 && current.Length + raw.Length > _options.SegmentSize))
                {
                    if (current is not null)
                        await current.File!.FlushAsync(ct);
                    if (minor == uint.MaxValue)
                        throw new IOException($"Object store '{_directory}' cannot name another compaction output after {lastInput}.");
                    current = await CreateOutputAsync(new SegmentKey(lastInput.Major, ++minor), ct);
                    run.Outputs.Add(current);
                }

                await current.File!.WriteFullyAsync(current.Length, raw, ct);
                if (live is not null)
                    run.Moves.Add((live, new IndexEntry(live.Info, current, current.Length, live.Header) { Sequence = live.Sequence }));
                else
                    current.MarkerBytes += raw.Length;
                current.Length += raw.Length;
                current.RecordCount++;
            }
        }

        if (current is not null)
            await current.File!.FlushAsync(ct);

        // Damaged records must not be re-indexed if this step is abandoned: retire them durably now.
        if (run.Corrupt.Count > 0)
            await QuarantineAsync(run.Corrupt, "failed verification during compaction", ct);
    }

    /// <summary>
    /// True when a marker that cannot be dropped wholesale still acts on something: every tombstone (an older record of
    /// its object may remain), and quarantine records whose target segment still exists outside the step's inputs.
    /// </summary>
    private bool MarkerStillNeeded(RecordHeader marker, HashSet<SegmentKey> inputKeys)
    {
        if (marker.Kind != KindQuarantine)
            return true;
        if (inputKeys.Contains(marker.TargetKey))
            return false; // the retired record is in an input and is not copied (it is not live)
        lock (_sync)
            return _segments.Any(s => s.Key == marker.TargetKey);
    }

    /// <summary>Reads and re-verifies a live record for copying; null when it is damaged (recorded for quarantine) or no longer current.</summary>
    private async ValueTask<byte[]?> ReadLiveRecordAsync(CompactionRun run, IndexEntry e, CancellationToken ct)
    {
        switch (TryAcquireCurrent(e))
        {
            case AcquireResult.NotCurrent:
                return null; // deleted, replaced or retired meanwhile: nothing to copy
            case AcquireResult.Closed:
                // The store was closed: a record that could not be read is not a record that is no longer live.
                throw new ObjectDisposedException(nameof(ObjectStore), "The object store was closed during compaction.");
        }

        byte[]? raw;
        try
        {
            raw = new byte[e.RecordLength];
            await e.Segment.File!.ReadFullyAsync(e.Offset, raw, ct);
            if (!VerifyRawRecord(e, raw))
                raw = null;
        }
        catch (EndOfStreamException)
        {
            // The record is cut short on the device: damaged. Any other read error propagates and abandons the step:
            // the bytes may be intact, and a committed step deletes whatever it did not copy.
            raw = null;
        }
        finally
        {
            await ReleaseAsync(e.Segment);
        }

        if (raw is null)
            run.Corrupt.Add(e);
        return raw;
    }

    /// <summary>
    /// Lists the valid records of a sealed segment in file order (header CRC checked; damaged regions are skipped, as
    /// at open). Holds a read reference on the file meanwhile.
    /// </summary>
    private async ValueTask<List<(RecordHeader Header, long Offset)>> ReadRecordHeadersAsync(SegmentFile segment, CancellationToken ct)
    {
        long length;
        lock (_sync)
        {
            if (_disposed || segment.File is null)
                throw new ObjectDisposedException(nameof(ObjectStore), "The object store was closed during compaction.");
            segment.Refs++;
            length = segment.Length;
        }

        var records = new List<(RecordHeader, long)>();
        try
        {
            IStorageFile file = segment.File!;
            long pos = SegmentHeaderSize;
            while (pos < length)
            {
                ct.ThrowIfCancellationRequested();
                RecordHeader? h = await TryReadHeaderAsync(file, segment.Path, pos, length, ct);
                if (h is not null && pos + h.RecordLength <= length)
                {
                    records.Add((h, pos));
                    pos += h.RecordLength;
                    continue;
                }

                pos = h is not null ? length : await FindNextRecordAsync(file, segment.Path, pos + 1, length, ct);
            }
        }
        finally
        {
            await ReleaseAsync(segment);
        }

        return records;
    }

    /// <summary>Re-validates a record read for copying: header CRC, identity and every payload unit.</summary>
    private bool VerifyRawRecord(IndexEntry e, byte[] raw)
    {
        int headerLength = e.Header.HeaderLength;
        if (raw.Length < headerLength || !TryParseHeader(raw.AsSpan(0, headerLength), e.Segment.Path, e.Offset, out RecordHeader h)
            || h.Id != e.Info.Id || h.Checksum != e.Info.Checksum || h.StoredLength != e.Header.StoredLength)
        {
            return false;
        }

        var whole = new WholeObjectHash();
        var plain = new byte[Math.Min(h.UnitSize, Math.Max(h.PlaintextLength, 1))];
        for (int u = 0; u < h.UnitHashes.Length; u++)
        {
            Span<byte> unit = plain.AsSpan(0, e.PlainUnitLength(u));
            ReadOnlySpan<byte> stored = raw.AsSpan(headerLength + (int)e.StoredUnitOffset(u), e.StoredUnitLength(u));
            if (!TryOpenUnit(e, u, stored, unit, verify: true))
                return false;
            whole.Append(unit);
        }

        return whole.Value == e.Info.Checksum;
    }

    private async ValueTask<SegmentFile> CreateOutputAsync(SegmentKey key, CancellationToken ct)
    {
        string compactingPath = PathOf(SegmentFileName(key, compacting: true));
        IStorageFile file = await _medium.OpenAsync(compactingPath, StorageOpenMode.Truncate, ct);
        try
        {
            await file.WriteFullyAsync(0, EncodeSegmentHeader(key), ct);
        }
        catch
        {
            await StorageFormat.CloseQuietlyAsync(file);
            throw;
        }

        return new SegmentFile(key, PathOf(SegmentFileName(key))) { CompactingPath = compactingPath, File = file, Length = SegmentHeaderSize };
    }

    private async ValueTask AbandonOutputsAsync(CompactionRun run)
    {
        try
        {
            foreach (SegmentFile output in run.Outputs)
            {
                IStorageFile? file = output.File;
                output.File = null;
                await StorageFormat.CloseQuietlyAsync(file);
                await _medium.DeleteAsync(output.CompactingPath!, CancellationToken.None);
            }

            await _medium.FlushDirectoryAsync(_directory, CancellationToken.None);
            await AtomicFile.DeleteAsync(_medium, ManifestPath, CancellationToken.None);
        }
        catch (Exception cleanup) when (StorageFormat.IsCleanupTolerable(cleanup))
        {
            // Unpublished outputs (and a manifest whose outputs are incomplete) are discarded at the next open.
        }
    }

    /// <summary>Publishes a committed compaction: rename outputs, switch the index, delete inputs and the manifest.</summary>
    private async ValueTask CompleteCommittedAsync(CompactionRun run, CancellationToken ct)
    {
        if (_disposed)
        {
            // A closed store leaves a committed compaction to the next open, which rolls it forward.
            if (!run.Switched)
            {
                foreach (SegmentFile output in run.Outputs)
                {
                    IStorageFile? file = output.File;
                    output.File = null;
                    await StorageFormat.CloseQuietlyAsync(file);
                }
            }

            ThrowIfDisposed();
        }

        if (!run.Renamed)
        {
            foreach (SegmentFile output in run.Outputs)
            {
                IStorageFile? file = output.File;
                output.File = null;
                await StorageFormat.CloseQuietlyAsync(file);
                if (await _medium.ExistsAsync(output.CompactingPath!, ct))
                    await _medium.RenameAsync(output.CompactingPath!, output.Path, overwrite: true, ct);
            }

            await _medium.FlushDirectoryAsync(_directory, ct);
            foreach (SegmentFile output in run.Outputs)
            {
                output.File ??= await _medium.OpenAsync(output.Path, StorageOpenMode.OpenExisting, ct);
                output.Length = output.File.Length;
            }

            run.Renamed = true;
        }

        if (!run.Switched)
        {
            var close = new List<IStorageFile>();
            lock (_sync)
            {
                foreach ((IndexEntry old, IndexEntry moved) in run.Moves)
                {
                    if (_index.TryGetValue(old.Info.Id, out IndexEntry? current) && ReferenceEquals(current, old))
                    {
                        _index[old.Info.Id] = moved;
                        old.Segment.LiveBytes -= old.RecordLength;
                        moved.Segment.LiveBytes += moved.RecordLength;
                    }
                }

                foreach (SegmentFile input in run.Inputs)
                {
                    _segments.Remove(input);
                    input.Retired = true;
                    if (input.Refs == 0 && input.File is not null)
                    {
                        close.Add(input.File);
                        input.File = null;
                    }
                }

                _segments.AddRange(run.Outputs);
                _segments.Sort(static (a, b) => a.Key.CompareTo(b.Key));
            }

            run.Switched = true;
            foreach (IStorageFile file in close)
                await StorageFormat.CloseQuietlyAsync(file);
        }

        if (_faults.IsEnabled)
            await _faults.HitAsync(new FaultContext(FaultStage.F20, FaultSites.CompactionDelete, _options.NodeName, Operation: DfsOperationKind.Compaction), ct);
        foreach (SegmentFile input in run.Inputs)
        {
            if (await _medium.ExistsAsync(input.Path, ct))
                await _medium.DeleteAsync(input.Path, ct);
        }

        await _medium.FlushDirectoryAsync(_directory, ct);
        await AtomicFile.DeleteAsync(_medium, ManifestPath, ct);
        _pendingCompaction = null;
    }

    /// <summary>State of one compaction, kept until it has been fully published.</summary>
    private sealed class CompactionRun(List<SegmentFile> inputs)
    {
        public List<SegmentFile> Inputs { get; } = inputs;

        public List<SegmentFile> Outputs { get; } = [];

        public List<(IndexEntry Old, IndexEntry Moved)> Moves { get; } = [];

        public List<IndexEntry> Corrupt { get; } = [];

        public bool Committed { get; set; }

        public bool Renamed { get; set; }

        public bool Switched { get; set; }
    }
}
