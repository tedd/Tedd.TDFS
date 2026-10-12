using System.Collections.Immutable;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Volumes.Commands;

namespace Tedd.Tdfs.Server.Volumes.State;

/// <summary>A logical range of stream content: an object extent or inline bytes.</summary>
internal interface IContentSegment
{
    long Offset { get; }

    long Length { get; }

    /// <summary>The part of the segment inside <c>[start, end)</c>.</summary>
    IContentSegment SliceSegment(long start, long end);
}

/// <summary>Inline bytes covering <c>[Offset, Offset + Length)</c>.</summary>
internal sealed record InlineSegment(long Offset, long Length, ReadOnlyMemory<byte> Data) : IContentSegment
{
    public IContentSegment SliceSegment(long start, long end)
    {
        long s = Math.Max(start, Offset);
        long e = Math.Min(end, Offset + Length);
        return new InlineSegment(s, e - s, Data.Slice((int)(s - Offset), (int)(e - s)));
    }
}

/// <summary>Result of applying a publication to a stream.</summary>
/// <param name="Stream">The new stream (null on failure).</param>
/// <param name="Code">Failure code (<see cref="DfsErrorCode.None"/> on success).</param>
/// <param name="Message">Failure message.</param>
internal readonly record struct ContentResult(StreamRecord? Stream, DfsErrorCode Code, string Message)
{
    public static ContentResult Fail(DfsErrorCode code, string message) => new(null, code, message);
}

/// <summary>
/// The extent-map algebra of publications (PROTOCOL.md §8.1, ARCHITECTURE §5.1): clip at the truncation watermark, apply
/// changes in order (later ones override earlier ones; untouched extents keep their objects, AT-07), set the final length
/// (growth creates a hole; clipped data never resurrects, STR-011), then produce the chosen representation. Pure and
/// deterministic; shared by apply and by the leader's planning.
/// </summary>
internal static class ContentAlgebra
{
    /// <summary>
    /// Applies <paramref name="changes"/> to <paramref name="baseStream"/> (null = empty) producing a stream revision at
    /// <paramref name="index"/>. When <paramref name="finalInline"/> the result must consist of inline bytes only;
    /// otherwise of object extents only (mixed results mean the leader did not materialize, and fail).
    /// </summary>
    public static ContentResult Apply(StreamRecord? baseStream, long truncateTo, IReadOnlyList<StreamChange> changes, long newLength, bool finalInline,
        int inlineThreshold, ulong index)
    {
        if (Plan(baseStream, truncateTo, changes, newLength, out List<IContentSegment>? small, out ImmutableList<ExtentRef>.Builder? large, out string? error)
            is { } failure)
        {
            return ContentResult.Fail(failure, error!);
        }

        ulong created = baseStream?.CreatedIndex ?? index;
        if (large is not null)
        {
            if (finalInline)
            {
                return large.Count == 0
                    ? Inline(new byte[newLength], newLength, inlineThreshold, index, created)
                    : ContentResult.Fail(DfsErrorCode.InvalidArgument, "An inline publication still references objects.");
            }

            return new ContentResult(new StreamRecord(newLength, false, ReadOnlyMemory<byte>.Empty, large.ToImmutable(), index, created), DfsErrorCode.None,
                string.Empty);
        }

        if (finalInline)
        {
            if (newLength > inlineThreshold)
                return ContentResult.Fail(DfsErrorCode.InvalidArgument, $"A {newLength}-byte stream exceeds the inline threshold {inlineThreshold}.");
            var bytes = new byte[newLength];
            foreach (IContentSegment seg in small!)
            {
                if (seg is not InlineSegment inline)
                    return ContentResult.Fail(DfsErrorCode.InvalidArgument, "An inline publication still references objects.");
                inline.Data.Span.CopyTo(bytes.AsSpan((int)inline.Offset));
            }

            return Inline(bytes, newLength, inlineThreshold, index, created);
        }

        var extents = ImmutableList.CreateBuilder<ExtentRef>();
        foreach (IContentSegment seg in small!)
        {
            if (seg is not ExtentRef extent)
                return ContentResult.Fail(DfsErrorCode.InvalidArgument, "Inline bytes must be stored as objects for this stream (the leader did not materialize them).");
            AppendCoalesced(extents, extent);
        }

        return new ContentResult(new StreamRecord(newLength, false, ReadOnlyMemory<byte>.Empty, extents.ToImmutable(), index, created), DfsErrorCode.None,
            string.Empty);
    }

    /// <summary>
    /// Computes the logical result of a publication without choosing a representation: either an extent-only builder
    /// (<paramref name="large"/>, when neither the base nor the changes involve inline bytes) or a small segment list that
    /// may mix inline bytes and extents (<paramref name="small"/>, clipped to the final length).
    /// </summary>
    /// <returns>Null on success, otherwise the failure code (with <paramref name="error"/>).</returns>
    public static DfsErrorCode? Plan(StreamRecord? baseStream, long truncateTo, IReadOnlyList<StreamChange> changes, long newLength,
        out List<IContentSegment>? small, out ImmutableList<ExtentRef>.Builder? large, out string? error)
    {
        small = null;
        large = null;
        error = null;
        if (newLength < 0)
        {
            error = "The new length must not be negative.";
            return DfsErrorCode.InvalidArgument;
        }

        foreach (StreamChange c in changes)
        {
            if (Validate(c) is { } invalid)
            {
                error = invalid;
                return DfsErrorCode.InvalidArgument;
            }
        }

        bool inlineInvolved = baseStream is { IsInline: true, Length: > 0 } || changes.Any(c => c.ChangeKind == RangeChangeKind.Inline);
        long cut = truncateTo >= 0 ? truncateTo : long.MaxValue;
        if (!inlineInvolved)
        {
            ImmutableList<ExtentRef>.Builder list = (baseStream is { IsInline: false } ? baseStream.Extents : ImmutableList<ExtentRef>.Empty).ToBuilder();
            if (baseStream is not null && cut < baseStream.Length)
                Clip(list, cut);
            foreach (StreamChange c in changes)
            {
                Punch(list, c.Offset, c.Offset + c.Length);
                if (c.ChangeKind == RangeChangeKind.Data)
                    Insert(list, c.Extent! with { Offset = c.Offset, Length = c.Length });
            }

            Clip(list, newLength);
            large = list;
            return null;
        }

        // Inline bytes involved: the base is small (inline) or the result is about to become inline; clip the base to the
        // final length first so the working list stays bounded by the final content.
        var segments = new List<IContentSegment>();
        long baseCut = Math.Min(cut, newLength);
        if (baseStream is { IsInline: true, Length: > 0 })
        {
            long keep = Math.Min(baseStream.Length, baseCut);
            if (keep > 0)
                segments.Add(new InlineSegment(0, keep, baseStream.Inline[..(int)keep]));
        }
        else if (baseStream is { IsInline: false })
        {
            foreach (ExtentRef e in baseStream.Extents)
            {
                if (e.Offset >= baseCut)
                    break;
                segments.Add(e.End > baseCut ? e.Slice(e.Offset, baseCut) : e);
            }
        }

        foreach (StreamChange c in changes)
        {
            Punch(segments, c.Offset, c.Offset + c.Length);
            if (c.ChangeKind == RangeChangeKind.Data)
                Insert(segments, c.Extent! with { Offset = c.Offset, Length = c.Length });
            else if (c.ChangeKind == RangeChangeKind.Inline)
                Insert(segments, new InlineSegment(c.Offset, c.Length, c.Inline));
        }

        Clip(segments, newLength);
        small = segments;
        return null;
    }

    /// <summary>Structural validation of one change (non-negative, no overflow, kind-specific fields consistent).</summary>
    public static string? Validate(StreamChange c)
    {
        if (c.Offset < 0 || c.Length <= 0 || c.Offset > long.MaxValue - c.Length)
            return $"Invalid change range [{c.Offset}, +{c.Length}).";
        switch (c.ChangeKind)
        {
            case RangeChangeKind.Data:
                if (c.Extent is not { } e)
                    return "A data change needs an object.";
                if (e.Object.IsEmpty || e.ObjectOffset < 0 || e.ObjectLength < 0 || e.ObjectOffset > e.ObjectLength - c.Length)
                    return $"Data change [{c.Offset}, +{c.Length}) maps outside object {e.Object} ({e.ObjectLength} bytes) at {e.ObjectOffset}.";
                if (!c.Inline.IsEmpty)
                    return "A data change must not carry inline bytes.";
                return null;
            case RangeChangeKind.Zero:
                return c.Extent is null && c.Inline.IsEmpty ? null : "A zero change must not carry an object or bytes.";
            case RangeChangeKind.Inline:
                if (c.Extent is not null)
                    return "An inline change must not reference an object.";
                return c.Inline.Length == c.Length ? null : $"An inline change carries {c.Inline.Length} bytes for a {c.Length}-byte range.";
            default:
                return $"Unknown change kind {(int)c.ChangeKind}.";
        }
    }

    /// <summary>Removes coverage of <c>[start, end)</c>, splitting segments that straddle the range.</summary>
    public static void Punch<T>(IList<T> list, long start, long end)
        where T : class, IContentSegment
    {
        int i = FirstEndingAfter(list, start);
        while (i < list.Count && list[i].Offset < end)
        {
            T seg = list[i];
            list.RemoveAt(i);
            if (seg.Offset < start)
            {
                list.Insert(i, (T)seg.SliceSegment(seg.Offset, start));
                i++;
            }

            if (seg.Offset + seg.Length > end)
            {
                list.Insert(i, (T)seg.SliceSegment(end, seg.Offset + seg.Length));
                i++;
            }
        }
    }

    /// <summary>Inserts a segment into a range already punched free, merging contiguous extents of the same object.</summary>
    public static void Insert<T>(IList<T> list, T seg)
        where T : class, IContentSegment
    {
        int i = FirstEndingAfter(list, seg.Offset);
        list.Insert(i, seg);
        if (seg is not ExtentRef)
            return;
        if (i + 1 < list.Count && TryMerge(list[i], list[i + 1]) is T next)
        {
            list[i] = next;
            list.RemoveAt(i + 1);
        }

        if (i > 0 && TryMerge(list[i - 1], list[i]) is T previous)
        {
            list[i - 1] = previous;
            list.RemoveAt(i);
        }
    }

    /// <summary>Removes everything at or beyond <paramref name="length"/>.</summary>
    public static void Clip<T>(IList<T> list, long length)
        where T : class, IContentSegment
    {
        int i = FirstEndingAfter(list, length);
        if (i < list.Count && list[i].Offset < length)
        {
            list[i] = (T)list[i].SliceSegment(list[i].Offset, length);
            i++;
        }

        while (list.Count > i)
            list.RemoveAt(list.Count - 1);
    }

    /// <summary>Index of the first segment whose end lies after <paramref name="position"/> (binary search).</summary>
    public static int FirstEndingAfter<T>(IList<T> list, long position)
        where T : class, IContentSegment
    {
        int lo = 0;
        int hi = list.Count;
        while (lo < hi)
        {
            int mid = (lo + hi) >>> 1;
            T seg = list[mid];
            if (seg.Offset + seg.Length <= position)
                lo = mid + 1;
            else
                hi = mid;
        }

        return lo;
    }

    /// <summary>The extents of <paramref name="stream"/> intersecting <c>[start, end)</c> (whole extents, ascending).</summary>
    public static List<ExtentRef> Intersecting(StreamRecord stream, long start, long end, int maxCount)
    {
        var result = new List<ExtentRef>();
        if (stream.IsInline || end <= start)
            return result;
        ImmutableList<ExtentRef> extents = stream.Extents;
        int lo = 0;
        int hi = extents.Count;
        while (lo < hi)
        {
            int mid = (lo + hi) >>> 1;
            if (extents[mid].End <= start)
                lo = mid + 1;
            else
                hi = mid;
        }

        for (int i = lo; i < extents.Count && extents[i].Offset < end && result.Count < maxCount; i++)
            result.Add(extents[i]);
        return result;
    }

    private static object? TryMerge(object left, object right)
    {
        if (left is not ExtentRef a || right is not ExtentRef b)
            return null;
        if (a.End != b.Offset || a.Object != b.Object || a.ObjectOffset + a.Length != b.ObjectOffset || a.Checksum != b.Checksum
            || a.ObjectLength != b.ObjectLength || a.Encoding != b.Encoding)
        {
            return null;
        }

        return a with { Length = a.Length + b.Length };
    }

    private static void AppendCoalesced(ImmutableList<ExtentRef>.Builder list, ExtentRef extent)
    {
        if (list.Count > 0 && TryMerge(list[^1], extent) is ExtentRef merged)
            list[^1] = merged;
        else
            list.Add(extent);
    }

    private static ContentResult Inline(byte[] bytes, long length, int threshold, ulong index, ulong created)
    {
        if (length > threshold && length > 0)
            return ContentResult.Fail(DfsErrorCode.InvalidArgument, $"A {length}-byte stream exceeds the inline threshold {threshold}.");
        return new ContentResult(new StreamRecord(length, true, bytes, ImmutableList<ExtentRef>.Empty, index, created), DfsErrorCode.None, string.Empty);
    }
}
