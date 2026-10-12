namespace Tedd.Tdfs.Storage;

// Compaction and scrubbing.
//
// Compaction protocol (crash-safe at every step):
//   1. Seal the active segment (new puts go to a new segment with a higher major) and wait for in-flight puts on the
//      sealed segments. Inputs = every existing segment; outputs are keyed (maxInput.Major, maxInput.Minor + 1 + n),
//      so they order after every input and before every newer segment.
//   2. Copy every live record (re-verified; damaged ones are quarantined instead; a read error or a closed store
//      abandons the run) into ".ocmp" output files, flush them and the directory. [fault site compaction.copy before]
//   3. Durably write the manifest (inputs + outputs) with AtomicFile: the commit point. [compaction.publish before]
//   4. Rename outputs to ".oseg", flush the directory, switch the in-memory index.
//   5. Delete the inputs, flush the directory, delete the manifest.              [compaction.delete before]
// Recovery: a manifest whose outputs all exist is rolled forward; otherwise (or without a manifest) ".ocmp" files are
// discarded and the inputs remain authoritative. Tombstones are dropped by compaction because every older record is
// in the input set.
public sealed partial class ObjectStore
{
    /// <summary>Copies live records out of sparse segments and deletes the old segments (crash-safe).</summary>
    /// <remarks>
    /// Does nothing when no dead bytes exist. Compaction may use the reserved capacity. An interrupted compaction that
    /// already reached its commit point is completed by the next call (or by the next open).
    /// </remarks>
    public async ValueTask CompactAsync(CancellationToken ct = default)
    {
        using SerialGate.Releaser _ = await _compactionGate.EnterAsync(ct);
        ThrowIfDisposed();
        if (_pendingCompaction is { } pending)
            await CompleteCommittedAsync(pending, ct);

        List<SegmentFile> inputs;
        using (await _writeGate.EnterAsync(ct))
        {
            ThrowIfDisposed();
            lock (_sync)
            {
                long dead = TotalBytesLocked() - _liveBytes - ((long)_segments.Count * SegmentHeaderSize);
                if (_segments.Count == 0 || dead <= 0)
                    return;
                inputs = [.. _segments];
            }

            // New writes go to a newer segment from now on, so the current one stops being the newest (the only one
            // payload-verified at open): make it complete and durable first, including records of puts that are
            // still waiting for their own barrier.
            if (_active is { } sealing)
                await SealLockedAsync(sealing, ct);
            _active = null;
        }

        await WaitForInflightWritesAsync(inputs);
        if (_faults.IsEnabled)
            await _faults.HitAsync(new FaultContext(FaultStage.F20, FaultSites.CompactionCopy, _options.NodeName, Operation: DfsOperationKind.Compaction), ct);

        var run = new CompactionRun(inputs);
        try
        {
            await CopyLiveRecordsAsync(run, ct);
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
        foreach (IndexEntry e in batch)
        {
            ct.ThrowIfCancellationRequested();
            if (!TryAcquire(e))
                continue;
            bool intact;
            try
            {
                intact = await VerifyPayloadAsync(e.Segment, e.PayloadOffset, e.Header, e.Info.Checksum, ct);
            }
            catch (IOException ex) when (ex is not DfsException)
            {
                intact = false;
            }
            finally
            {
                await ReleaseAsync(e.Segment);
            }

            checkedCount++;
            if (!intact)
                corrupt.Add(e);
        }

        if (corrupt.Count > 0)
            await QuarantineAsync(corrupt, "failed verification during scrub", ct);
        return new ScrubResult(checkedCount, [.. corrupt.Select(e => e.Info.Id)]);
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
                (SegmentFile segment, long offset) = await AppendLockedAsync(buffer, records.Count, mayRollOver: false, ct);
                try
                {
                    await FlushSegmentAsync(segment, ct);
                }
                catch
                {
                    await RollbackAppendAsync(segment, offset, buffer.Length, records.Count);
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

    private async ValueTask CopyLiveRecordsAsync(CompactionRun run, CancellationToken ct)
    {
        var inputSet = new HashSet<SegmentFile>(run.Inputs);
        List<IndexEntry> live;
        lock (_sync)
        {
            live = [.. _index.Values.Where(e => inputSet.Contains(e.Segment)).OrderBy(e => e.Segment.Key).ThenBy(e => e.Offset)];
        }

        SegmentKey maxKey = run.Inputs[^1].Key;
        uint minor = maxKey.Minor;
        SegmentFile? current = null;
        foreach (IndexEntry e in live)
        {
            ct.ThrowIfCancellationRequested();
            switch (TryAcquireCurrent(e))
            {
                case AcquireResult.NotCurrent:
                    continue; // deleted, replaced or retired meanwhile: nothing to copy
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
                // The record is cut short on the device: damaged. Any other read error propagates and abandons the
                // compaction: the bytes may be intact, and a committed compaction deletes whatever it did not copy.
                raw = null;
            }
            finally
            {
                await ReleaseAsync(e.Segment);
            }

            if (raw is null)
            {
                run.Corrupt.Add(e);
                continue;
            }

            if (current is null || (current.RecordCount > 0 && current.Length + raw.Length > _options.SegmentSize))
            {
                if (current is not null)
                    await current.File!.FlushAsync(ct);
                if (minor == uint.MaxValue)
                    throw new IOException($"Object store '{_directory}' cannot name another compaction output after {maxKey}.");
                current = await CreateOutputAsync(new SegmentKey(maxKey.Major, ++minor), ct);
                run.Outputs.Add(current);
            }

            await current.File!.WriteFullyAsync(current.Length, raw, ct);
            run.Moves.Add((e, new IndexEntry(e.Info, current, current.Length, e.Header) { Sequence = e.Sequence }));
            current.Length += raw.Length;
            current.RecordCount++;
        }

        if (current is not null)
            await current.File!.FlushAsync(ct);

        // Damaged records must not be re-indexed if this compaction is abandoned: retire them durably now.
        if (run.Corrupt.Count > 0)
            await QuarantineAsync(run.Corrupt, "failed verification during compaction", ct);
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
                        _index[old.Info.Id] = moved;
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
