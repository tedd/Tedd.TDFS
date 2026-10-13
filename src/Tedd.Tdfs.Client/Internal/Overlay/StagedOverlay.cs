namespace Tedd.Tdfs.Client.Internal.Overlay;

/// <summary>An object uploaded under a staging session and not yet published.</summary>
/// <param name="Id">Object id (random, chosen by the uploader).</param>
/// <param name="Checksum">XxHash128 of the whole object.</param>
/// <param name="Length">Object length.</param>
/// <param name="Receipts">Receipts proving the durable copies.</param>
internal sealed record UploadedObject(ObjectId Id, UInt128 Checksum, long Length, IReadOnlyList<Protocol.Messages.ObjectReceipt> Receipts);

/// <summary>
/// One dirty range of the staged view: <c>[Offset, Offset + Length)</c> of the file maps to buffered bytes
/// (<see cref="Buffer"/> from <see cref="BufferOffset"/>), to bytes of an uploaded object (<see cref="Object"/> from
/// <see cref="ObjectOffset"/>), or to both: a readable writer keeps the bytes it uploaded early (staging capabilities never
/// authorize reading them back), so its reads are served from memory while the publication references the object.
/// Immutable (a snapshot of the overlay can share segments).
/// </summary>
internal sealed record StagedSegment(long Offset, long Length, byte[]? Buffer, int BufferOffset, bool OwnsTail, UploadedObject? Object, long ObjectOffset)
{
    /// <summary>End of the range (exclusive).</summary>
    public long End => Offset + Length;

    /// <summary>True for buffered bytes that are not uploaded yet.</summary>
    public bool IsBuffered => Buffer is not null && Object is null;

    /// <summary>True when the bytes are available in memory (buffered, or kept after an early upload).</summary>
    public bool HasBytes => Buffer is not null;

    /// <summary>The part of this segment covering <c>[start, end)</c> (inside the segment).</summary>
    public StagedSegment Slice(long start, long end, bool ownsTail)
    {
        long delta = start - Offset;
        return this with
        {
            Offset = start,
            Length = end - start,
            BufferOffset = Buffer is null ? BufferOffset : checked(BufferOffset + (int)delta),
            ObjectOffset = Object is null ? ObjectOffset : ObjectOffset + delta,
            OwnsTail = IsBuffered && ownsTail,
        };
    }

    /// <summary>The buffered bytes of this segment.</summary>
    public ReadOnlyMemory<byte> Bytes => Buffer is null ? ReadOnlyMemory<byte>.Empty : Buffer.AsMemory(BufferOffset, checked((int)Length));
}

/// <summary>Where a range of the staged view comes from.</summary>
internal enum StagedSource
{
    /// <summary>Buffered staged bytes.</summary>
    Buffered,

    /// <summary>Bytes of an uploaded, unpublished object.</summary>
    Uploaded,

    /// <summary>The published base revision.</summary>
    Base,

    /// <summary>Zeroes (sparse gap, or bytes discarded by a truncation and not rewritten).</summary>
    Zero,
}

/// <summary>A contiguous piece of a staged read.</summary>
internal readonly record struct StagedPiece(StagedSource Source, long Offset, int Length, StagedSegment? Segment);

/// <summary>
/// The private staged view of a writable stream relative to its published base revision (FSD §6.2, PROTOCOL.md §8.1):
/// dirty ranges (buffered or uploaded), the staged length, and the lowest length reached since the base
/// (<see cref="MinLength"/>, published as <c>TruncateTo</c> so truncation followed by extension never reveals discarded
/// bytes, STR-011). Not thread-safe: the owning stream serializes calls through its gate.
/// </summary>
internal sealed class StagedOverlay
{
    /// <summary>Largest buffered segment grown by appending adjacent writes.</summary>
    public const int MaxCoalescedSegment = 1 << 20;

    private List<StagedSegment> _segments = [];
    private long _pendingBytes;
    private long _bufferedBytes;

    public StagedOverlay(long baseLength)
    {
        BaseLength = baseLength;
        Length = baseLength;
        MinLength = baseLength;
    }

    /// <summary>Length of the published base revision.</summary>
    public long BaseLength { get; private set; }

    /// <summary>Staged length.</summary>
    public long Length { get; private set; }

    /// <summary>Lowest length reached since the base (bytes from here on come from writes or are zero).</summary>
    public long MinLength { get; private set; }

    /// <summary>The dirty ranges in ascending order.</summary>
    public IReadOnlyList<StagedSegment> Segments => _segments;

    /// <summary>True when the staged view differs from the base.</summary>
    public bool IsDirty => _segments.Count > 0 || Length != BaseLength || MinLength != BaseLength;

    /// <summary>
    /// Staged, unpublished bytes (buffered and uploaded). Safe to read while the owning stream works (it backs the
    /// ungated <see cref="DfsStream.PendingBytes"/>): a maintained total, never an enumeration of a list another call may
    /// be changing.
    /// </summary>
    public long PendingBytes => Volatile.Read(ref _pendingBytes);

    /// <summary>Buffered (not yet uploaded) bytes.</summary>
    public long BufferedBytes => Volatile.Read(ref _bufferedBytes);

    /// <summary>Adds (or with a negative sign removes) a segment's bytes to the maintained totals.</summary>
    private void Count(StagedSegment segment, int sign)
    {
        Volatile.Write(ref _pendingBytes, _pendingBytes + (sign * segment.Length));
        if (segment.IsBuffered)
            Volatile.Write(ref _bufferedBytes, _bufferedBytes + (sign * segment.Length));
    }

    /// <summary>Recomputes the maintained totals from the segment list.</summary>
    private void Recount()
    {
        long pending = 0;
        long buffered = 0;
        foreach (StagedSegment segment in _segments)
        {
            pending += segment.Length;
            if (segment.IsBuffered)
                buffered += segment.Length;
        }

        Volatile.Write(ref _pendingBytes, pending);
        Volatile.Write(ref _bufferedBytes, buffered);
    }

    /// <summary>The truncation point to publish (<c>-1</c> when the base is never clipped).</summary>
    public long TruncateTo => MinLength < BaseLength ? MinLength : -1;

    /// <summary>Captures the state (cheap: segments are immutable).</summary>
    public Snapshot Capture() => new(BaseLength, Length, MinLength, [.. _segments]);

    /// <summary>Restores a captured state exactly (a failed call leaves no partial effect, PUB-022).</summary>
    public void Restore(Snapshot snapshot)
    {
        BaseLength = snapshot.BaseLength;
        Length = snapshot.Length;
        MinLength = snapshot.MinLength;
        _segments = [.. snapshot.Segments];
        Recount();
    }

    /// <summary>Marks the staged view as published: it becomes the new base (of <paramref name="newBaseLength"/> bytes).</summary>
    public void Reset(long newBaseLength)
    {
        BaseLength = newBaseLength;
        Length = newBaseLength;
        MinLength = newBaseLength;
        _segments = [];
        Recount();
    }

    /// <summary>Stages <paramref name="data"/> (a private copy owned by the overlay from now on) at <paramref name="offset"/>.</summary>
    public void Write(long offset, ReadOnlyMemory<byte> data)
    {
        if (data.IsEmpty)
            return;
        long end = checked(offset + data.Length);

        // Append to the previous buffered segment when the write continues it (WriteByte, small sequential writes).
        int before = FindLastStartingBefore(offset + 1);
        if (before >= 0)
        {
            StagedSegment previous = _segments[before];
            if (previous.IsBuffered && previous.OwnsTail && previous.End == offset && previous.Length + data.Length <= MaxCoalescedSegment
                && (before + 1 >= _segments.Count || _segments[before + 1].Offset >= end))
            {
                int used = previous.BufferOffset + (int)previous.Length;
                byte[] buffer = previous.Buffer!;
                if (buffer.Length - used < data.Length)
                {
                    int capacity = (int)Math.Min(MaxCoalescedSegment, Math.Max(previous.Length + data.Length, Math.Max(256, previous.Length * 2)));
                    var grown = new byte[capacity];
                    previous.Bytes.Span.CopyTo(grown);
                    buffer = grown;
                    used = (int)previous.Length;
                    previous = previous with { Buffer = grown, BufferOffset = 0 };
                }

                data.Span.CopyTo(buffer.AsSpan(used));
                _segments[before] = previous with { Length = previous.Length + data.Length };
                Volatile.Write(ref _pendingBytes, _pendingBytes + data.Length);
                Volatile.Write(ref _bufferedBytes, _bufferedBytes + data.Length);
                Length = Math.Max(Length, end);
                return;
            }
        }

        byte[] copy = data.ToArray();
        Insert(new StagedSegment(offset, data.Length, copy, 0, OwnsTail: true, null, 0));
        Length = Math.Max(Length, end);
    }

    /// <summary>Stages <paramref name="owned"/> at <paramref name="offset"/>; the overlay takes ownership of the array.</summary>
    public void WriteOwned(long offset, byte[] owned)
    {
        ArgumentNullException.ThrowIfNull(owned);
        if (owned.Length == 0)
            return;
        int before = FindLastStartingBefore(offset + 1);
        if (before >= 0 && _segments[before] is { IsBuffered: true, OwnsTail: true } previous && previous.End == offset)
        {
            // Continues the previous buffered range: append (coalescing keeps many small writes cheap).
            Write(offset, owned);
            return;
        }

        long end = checked(offset + owned.Length);
        Insert(new StagedSegment(offset, owned.Length, owned, 0, OwnsTail: true, null, 0));
        Length = Math.Max(Length, end);
    }

    /// <summary>
    /// Replaces <c>[offset, offset + length)</c> with the bytes of an uploaded object starting at
    /// <paramref name="objectOffset"/> (several dirty ranges may share one packed object).
    /// </summary>
    public void ReplaceWithUploaded(long offset, long length, UploadedObject uploaded, long objectOffset = 0)
        => ReplaceWithUploaded(offset, length, uploaded, objectOffset, retained: null);

    /// <summary>
    /// Replaces <c>[offset, offset + length)</c> with the bytes of an uploaded object starting at
    /// <paramref name="objectOffset"/>, keeping <paramref name="retained"/> (exactly <paramref name="length"/> bytes) to
    /// serve reads of a readable writer.
    /// </summary>
    public void ReplaceWithUploaded(long offset, long length, UploadedObject uploaded, long objectOffset, byte[]? retained)
    {
        if (retained is not null && retained.Length != length)
            throw new ArgumentException("The retained bytes must cover the replaced range exactly.", nameof(retained));
        Insert(new StagedSegment(offset, length, retained, 0, OwnsTail: false, uploaded, objectOffset));
    }

    /// <summary>Sets the staged length (truncation clips dirty ranges; growth leaves a zero gap).</summary>
    public void SetLength(long length)
    {
        if (length < Length)
        {
            var kept = new List<StagedSegment>(_segments.Count);
            foreach (StagedSegment segment in _segments)
            {
                if (segment.Offset >= length)
                    continue;
                kept.Add(segment.End > length ? segment.Slice(segment.Offset, length, ownsTail: false) : segment);
            }

            _segments = kept;
            Recount();
            MinLength = Math.Min(MinLength, length);
        }

        Length = length;
    }

    /// <summary>Describes where each byte of <c>[offset, offset + count)</c> (clipped to the staged length) comes from.</summary>
    public List<StagedPiece> Map(long offset, int count)
    {
        var pieces = new List<StagedPiece>();
        long end = Math.Min(Length, offset + (long)count);
        long position = offset;
        int index = FindFirstEndingAfter(position);
        while (position < end)
        {
            StagedSegment? next = index < _segments.Count ? _segments[index] : null;
            if (next is not null && next.Offset <= position)
            {
                long pieceEnd = Math.Min(end, next.End);
                pieces.Add(new StagedPiece(next.HasBytes ? StagedSource.Buffered : StagedSource.Uploaded, position, (int)(pieceEnd - position), next));
                position = pieceEnd;
                index++;
                continue;
            }

            long gapEnd = Math.Min(end, next?.Offset ?? end);
            if (position < MinLength)
            {
                long baseEnd = Math.Min(gapEnd, MinLength);
                pieces.Add(new StagedPiece(StagedSource.Base, position, (int)(baseEnd - position), null));
                position = baseEnd;
                continue;
            }

            pieces.Add(new StagedPiece(StagedSource.Zero, position, (int)(gapEnd - position), null));
            position = gapEnd;
        }

        return pieces;
    }

    /// <summary>
    /// Inserts <paramref name="segment"/>, replacing whatever it overlaps (the parts of overlapped segments outside its
    /// range are kept). Binary search plus one range replacement: cost proportional to the overlapped segments, not to a
    /// rebuild of the whole overlay (LAY-017: an adversarially fragmented stream has many thousands of ranges).
    /// </summary>
    private void Insert(StagedSegment segment)
    {
        long start = segment.Offset;
        long end = segment.End;
        int first = FindFirstEndingAfter(start);
        int last = first;
        while (last < _segments.Count && _segments[last].Offset < end)
            last++;

        var replacement = new List<StagedSegment>(3);
        if (first < last)
        {
            StagedSegment head = _segments[first];
            StagedSegment tail = _segments[last - 1];
            if (head.Offset < start)
                replacement.Add(head.Slice(head.Offset, start, ownsTail: false));
            replacement.Add(segment);
            if (tail.End > end)
                replacement.Add(tail.Slice(end, tail.End, tail.OwnsTail));
            for (int i = first; i < last; i++)
                Count(_segments[i], -1);
            _segments.RemoveRange(first, last - first);
        }
        else
        {
            replacement.Add(segment);
        }

        _segments.InsertRange(first, replacement);
        foreach (StagedSegment added in replacement)
            Count(added, +1);
    }

    private int FindLastStartingBefore(long position)
    {
        int lo = 0;
        int hi = _segments.Count - 1;
        int found = -1;
        while (lo <= hi)
        {
            int mid = (lo + hi) >>> 1;
            if (_segments[mid].Offset < position)
            {
                found = mid;
                lo = mid + 1;
            }
            else
            {
                hi = mid - 1;
            }
        }

        return found;
    }

    private int FindFirstEndingAfter(long position)
    {
        int lo = 0;
        int hi = _segments.Count;
        while (lo < hi)
        {
            int mid = (lo + hi) >>> 1;
            if (_segments[mid].End <= position)
                lo = mid + 1;
            else
                hi = mid;
        }

        return lo;
    }

    /// <summary>A captured overlay state.</summary>
    internal sealed record Snapshot(long BaseLength, long Length, long MinLength, IReadOnlyList<StagedSegment> Segments);
}
