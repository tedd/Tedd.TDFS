using System.Buffers.Binary;

namespace Tedd.Tdfs.Storage;

// Opening: compaction roll-forward/roll-back and the index rebuild scan.
public sealed partial class ObjectStore
{
    private const int ScanWindow = 1 << 20;

    private async ValueTask LoadAsync(CancellationToken ct)
    {
        await RecoverCompactionAsync(ct);

        IReadOnlyList<string> names = await _medium.ListAsync(_directory, ct);
        var keys = new List<SegmentKey>();
        foreach (string name in names)
        {
            if (TryParseSegmentFileName(name, out SegmentKey key, out bool compacting) && !compacting)
                keys.Add(key);
        }

        keys.Sort();
        foreach (SegmentKey key in keys)
        {
            string path = PathOf(SegmentFileName(key));
            IStorageFile file = await _medium.OpenAsync(path, StorageOpenMode.OpenExisting, ct);
            _segments.Add(new SegmentFile(key, path) { File = file, Length = file.Length });
        }

        var quarantine = new Dictionary<(SegmentKey, long), byte[]>();
        for (int i = 0; i < _segments.Count; i++)
            await ScanSegmentAsync(_segments[i], i == _segments.Count - 1, quarantine, ct);

        if (_segments.Count > 0)
        {
            _active = _segments[^1];
            _nextMajor = _segments.Max(s => s.Key.Major) + 1;
        }

        if (quarantine.Count > 0)
        {
            byte[] buffer = [.. quarantine.OrderBy(q => q.Key.Item1).ThenBy(q => q.Key.Item2).SelectMany(q => q.Value)];
            (SegmentFile segment, _) = await AppendLockedAsync(buffer, quarantine.Count, ct);
            await segment.File!.FlushAsync(ct);
        }

        await _medium.FlushDirectoryAsync(_directory, ct);
    }

    /// <summary>Scans one segment, applying valid records to the index and skipping (and reporting) damage.</summary>
    /// <param name="segment">Segment to scan (its file is open).</param>
    /// <param name="isActive">True for the newest segment: payloads are verified and a torn tail is cut.</param>
    /// <param name="quarantine">Damaged active-segment records that still need a durable quarantine record, keyed by location.</param>
    /// <param name="ct">Cancellation token.</param>
    private async ValueTask ScanSegmentAsync(SegmentFile segment, bool isActive, Dictionary<(SegmentKey, long), byte[]> quarantine, CancellationToken ct)
    {
        IStorageFile file = segment.File!;
        long length = file.Length;
        var header = new byte[SegmentHeaderSize];
        bool headerOk = false;
        if (length >= SegmentHeaderSize)
        {
            await file.ReadFullyAsync(0, header, ct);
            headerOk = CheckSegmentHeader(header, segment.Key, segment.Path);
        }

        if (!headerOk)
        {
            if (isActive && (length <= SegmentHeaderSize || await FindNextRecordAsync(file, segment.Path, SegmentHeaderSize, length, ct) >= length))
            {
                // Torn creation of the newest segment: nothing in it was ever acknowledged.
                await file.SetLengthAsync(0, ct);
                await file.WriteFullyAsync(0, EncodeSegmentHeader(segment.Key), ct);
                await file.FlushAsync(ct);
                segment.Length = SegmentHeaderSize;
                return;
            }

            Report(null, segment.Path, 0, "segment header is damaged");
            if (length <= SegmentHeaderSize)
            {
                segment.Length = length;
                return;
            }
        }

        long pos = SegmentHeaderSize;
        while (pos < length)
        {
            ct.ThrowIfCancellationRequested();
            RecordHeader? h = await TryReadHeaderAsync(file, segment.Path, pos, length, ct);
            if (h is not null && pos + h.RecordLength <= length)
            {
                if (isActive && h.Kind == KindObject && !await VerifyPayloadAsync(segment, pos + h.HeaderLength, h, h.Checksum, ct))
                {
                    Report(h.Id, segment.Path, pos, "payload failed verification at open");
                    CountFailure();
                    quarantine[(segment.Key, pos)] = BuildMarkerRecord(KindQuarantine, h.Id, h.Volume, segment.Key, pos);
                }
                else
                {
                    if (h.Kind == KindQuarantine)
                        quarantine.Remove((h.TargetKey, h.TargetOffset)); // already retired durably
                    ApplyRecord(h, segment, pos);
                }

                segment.RecordCount++;
                pos += h.RecordLength;
                continue;
            }

            long next = await FindNextRecordAsync(file, segment.Path, pos + 1, length, ct);
            if (next >= length && isActive)
            {
                // Torn tail of the active segment (unacknowledged writes): cut it off.
                await file.SetLengthAsync(pos, ct);
                length = pos;
                break;
            }

            if (!await IsZeroRegionAsync(file, pos, next, ct))
                Report(null, segment.Path, pos, $"damaged region of {next - pos} bytes");
            pos = next;
        }

        segment.Length = length;
        if (isActive)
            await file.FlushAsync(ct);
    }

    private void ApplyRecord(RecordHeader h, SegmentFile segment, long offset)
    {
        lock (_sync)
        {
            switch (h.Kind)
            {
                case KindObject:
                    IndexLocked(new IndexEntry(InfoOf(h), segment, offset, h));
                    break;
                case KindTombstone:
                    RemoveLocked(h.Id);
                    break;
                case KindQuarantine:
                    if (_index.TryGetValue(h.Id, out IndexEntry? e) && e.Segment.Key == h.TargetKey && e.Offset == h.TargetOffset)
                        RemoveLocked(h.Id);
                    break;
            }
        }
    }

    private void Report(ObjectId? id, string path, long offset, string reason)
    {
        lock (_sync)
            _quarantined.Add(new QuarantinedRecord(id, path, offset, reason));
    }

    /// <summary>Reads and validates the record header at <paramref name="pos"/>; null when damaged or incomplete.</summary>
    private static async ValueTask<RecordHeader?> TryReadHeaderAsync(IStorageFile file, string path, long pos, long length, CancellationToken ct)
    {
        if (length - pos < FixedHeaderSize + 4)
            return null;
        var fixedPart = new byte[FixedHeaderSize];
        await file.ReadFullyAsync(pos, fixedPart, ct);
        int headerLength = ImpliedHeaderLength(fixedPart);
        if (headerLength < 0 || headerLength > length - pos)
            return null;
        var raw = new byte[headerLength];
        fixedPart.CopyTo(raw, 0);
        await file.ReadFullyAsync(pos + FixedHeaderSize, raw.AsMemory(FixedHeaderSize), ct);
        return TryParseHeader(raw, path, pos, out RecordHeader header) ? header : null;
    }

    /// <summary>Offset of the next valid, complete record at or after <paramref name="from"/>, or <paramref name="length"/>.</summary>
    private static async ValueTask<long> FindNextRecordAsync(IStorageFile file, string path, long from, long length, CancellationToken ct)
    {
        Span<byte> magicBytes = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(magicBytes, StorageFormat.ObjectRecordMagic);
        byte[] magic = magicBytes.ToArray();
        var buffer = new byte[ScanWindow];
        long windowStart = from;
        while (windowStart < length)
        {
            int n = (int)Math.Min(buffer.Length, length - windowStart);
            await file.ReadFullyAsync(windowStart, buffer.AsMemory(0, n), ct);
            int searchFrom = 0;
            while (searchFrom < n)
            {
                int hit = buffer.AsSpan(searchFrom, n - searchFrom).IndexOf(magic);
                if (hit < 0)
                    break;
                long candidate = windowStart + searchFrom + hit;
                RecordHeader? h = await TryReadHeaderAsync(file, path, candidate, length, ct);
                if (h is not null && candidate + h.RecordLength <= length)
                    return candidate;
                searchFrom += hit + 1;
            }

            if (windowStart + n >= length)
                break;
            windowStart += n - (magic.Length - 1);
        }

        return length;
    }

    private static async ValueTask<bool> IsZeroRegionAsync(IStorageFile file, long from, long to, CancellationToken ct)
    {
        var buffer = new byte[(int)Math.Min(ScanWindow, Math.Max(0, to - from))];
        for (long pos = from; pos < to;)
        {
            int n = (int)Math.Min(buffer.Length, to - pos);
            await file.ReadFullyAsync(pos, buffer.AsMemory(0, n), ct);
            if (buffer.AsSpan(0, n).ContainsAnyExcept((byte)0))
                return false;
            pos += n;
        }

        return true;
    }

    /// <summary>Finishes (manifest present and outputs complete) or rolls back an interrupted compaction.</summary>
    private async ValueTask RecoverCompactionAsync(CancellationToken ct)
    {
        byte[]? manifest = await AtomicFile.ReadAsync(_medium, ManifestPath, ct);
        if (manifest is not null)
        {
            (List<SegmentKey> inputs, List<SegmentKey> outputs) = DecodeManifest(manifest);
            var present = new HashSet<string>(await _medium.ListAsync(_directory, ct), StringComparer.Ordinal);
            bool outputsPresent = outputs.All(k => present.Contains(SegmentFileName(k)) || present.Contains(SegmentFileName(k, compacting: true)));
            if (outputsPresent)
            {
                foreach (SegmentKey k in outputs)
                {
                    if (!present.Contains(SegmentFileName(k)))
                        await _medium.RenameAsync(PathOf(SegmentFileName(k, compacting: true)), PathOf(SegmentFileName(k)), overwrite: true, ct);
                }

                await _medium.FlushDirectoryAsync(_directory, ct);
                foreach (SegmentKey k in inputs)
                {
                    if (present.Contains(SegmentFileName(k)))
                        await _medium.DeleteAsync(PathOf(SegmentFileName(k)), ct);
                }

                await _medium.FlushDirectoryAsync(_directory, ct);
            }
            else
            {
                if (!inputs.All(k => present.Contains(SegmentFileName(k))))
                {
                    throw new DfsIntegrityException(
                        $"Object store '{_directory}': an interrupted compaction lost output segments after deleting inputs; data is missing.");
                }

                foreach (SegmentKey k in outputs)
                {
                    foreach (string name in new[] { SegmentFileName(k), SegmentFileName(k, compacting: true) })
                    {
                        if (present.Contains(name))
                            await _medium.DeleteAsync(PathOf(name), ct);
                    }
                }

                await _medium.FlushDirectoryAsync(_directory, ct);
            }

            await AtomicFile.DeleteAsync(_medium, ManifestPath, ct);
        }

        // Outputs of a compaction that never committed.
        bool removed = false;
        foreach (string name in await _medium.ListAsync(_directory, ct))
        {
            if (TryParseSegmentFileName(name, out _, out bool compacting) && compacting)
            {
                await _medium.DeleteAsync(PathOf(name), ct);
                removed = true;
            }
        }

        if (removed)
            await _medium.FlushDirectoryAsync(_directory, ct);
    }

    private static byte[] EncodeManifest(IReadOnlyList<SegmentKey> inputs, IReadOnlyList<SegmentKey> outputs)
    {
        var buffer = new byte[1 + 4 + (inputs.Count * 12) + 4 + (outputs.Count * 12)];
        Span<byte> s = buffer;
        s[0] = 1;
        int pos = 1;
        foreach (IReadOnlyList<SegmentKey> list in new[] { inputs, outputs })
        {
            BinaryPrimitives.WriteInt32LittleEndian(s[pos..], list.Count);
            pos += 4;
            foreach (SegmentKey k in list)
            {
                BinaryPrimitives.WriteUInt64LittleEndian(s[pos..], k.Major);
                BinaryPrimitives.WriteUInt32LittleEndian(s[(pos + 8)..], k.Minor);
                pos += 12;
            }
        }

        return buffer;
    }

    private (List<SegmentKey> Inputs, List<SegmentKey> Outputs) DecodeManifest(ReadOnlySpan<byte> data)
    {
        if (data.Length < 1 || data[0] != 1)
            throw new DfsIntegrityException($"Object store '{_directory}': the compaction manifest has an unknown format.");
        int pos = 1;
        var result = new List<SegmentKey>[2];
        for (int list = 0; list < 2; list++)
        {
            if (data.Length - pos < 4)
                throw new DfsIntegrityException($"Object store '{_directory}': the compaction manifest is truncated.");
            int count = BinaryPrimitives.ReadInt32LittleEndian(data[pos..]);
            pos += 4;
            if (count < 0 || count > (data.Length - pos) / 12)
                throw new DfsIntegrityException($"Object store '{_directory}': the compaction manifest is malformed.");
            result[list] = new List<SegmentKey>(count);
            for (int i = 0; i < count; i++)
            {
                result[list].Add(new SegmentKey(BinaryPrimitives.ReadUInt64LittleEndian(data[pos..]), BinaryPrimitives.ReadUInt32LittleEndian(data[(pos + 8)..])));
                pos += 12;
            }
        }

        if (pos != data.Length)
            throw new DfsIntegrityException($"Object store '{_directory}': the compaction manifest has trailing bytes.");
        return (result[0], result[1]);
    }
}
