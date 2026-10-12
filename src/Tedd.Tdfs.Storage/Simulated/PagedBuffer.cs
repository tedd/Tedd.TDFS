namespace Tedd.Tdfs.Storage;

/// <summary>
/// Growable sparse byte buffer made of fixed-size pages. Invariant: every byte at a position at or beyond
/// <see cref="Length"/> inside an allocated page is zero, so extending the length always exposes zeroes.
/// </summary>
internal sealed class PagedBuffer
{
    internal const int PageSize = 64 * 1024;

    private readonly List<byte[]?> _pages = [];

    /// <summary>Logical length in bytes.</summary>
    public long Length { get; private set; }

    /// <summary>Deep copy.</summary>
    public PagedBuffer Clone()
    {
        var copy = new PagedBuffer { Length = Length };
        foreach (byte[]? page in _pages)
            copy._pages.Add(page is null ? null : (byte[])page.Clone());
        return copy;
    }

    /// <summary>Reads up to <paramref name="destination"/>.Length bytes at <paramref name="offset"/>; returns the count read.</summary>
    public int Read(long offset, Span<byte> destination)
    {
        if (offset >= Length || destination.IsEmpty)
            return 0;
        int count = (int)Math.Min(destination.Length, Length - offset);
        int done = 0;
        while (done < count)
        {
            long position = offset + done;
            int pageIndex = (int)(position / PageSize);
            int inPage = (int)(position % PageSize);
            int chunk = Math.Min(count - done, PageSize - inPage);
            byte[]? page = pageIndex < _pages.Count ? _pages[pageIndex] : null;
            if (page is null)
                destination.Slice(done, chunk).Clear();
            else
                page.AsSpan(inPage, chunk).CopyTo(destination.Slice(done, chunk));
            done += chunk;
        }

        return count;
    }

    /// <summary>Writes <paramref name="data"/> at <paramref name="offset"/>, extending the length as needed.</summary>
    public void Write(long offset, ReadOnlySpan<byte> data)
    {
        if (data.IsEmpty)
            return;
        long end = offset + data.Length;
        int done = 0;
        while (done < data.Length)
        {
            long position = offset + done;
            int pageIndex = (int)(position / PageSize);
            int inPage = (int)(position % PageSize);
            int chunk = Math.Min(data.Length - done, PageSize - inPage);
            while (_pages.Count <= pageIndex)
                _pages.Add(null);
            byte[] page = _pages[pageIndex] ??= new byte[PageSize];
            data.Slice(done, chunk).CopyTo(page.AsSpan(inPage, chunk));
            done += chunk;
        }

        if (end > Length)
            Length = end;
    }

    /// <summary>Truncates (zeroing the dropped tail) or extends (exposing zeroes) the buffer.</summary>
    public void SetLength(long length)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        if (length < Length)
        {
            int keepPages = (int)((length + PageSize - 1) / PageSize);
            if (_pages.Count > keepPages)
                _pages.RemoveRange(keepPages, _pages.Count - keepPages);
            int tail = (int)(length % PageSize);
            if (tail != 0 && keepPages > 0 && keepPages <= _pages.Count)
                _pages[keepPages - 1]?.AsSpan(tail).Clear();
        }

        Length = length;
    }

    /// <summary>XORs one byte with <paramref name="mask"/>; the offset must be below <see cref="Length"/>.</summary>
    public void XorByte(long offset, byte mask)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(offset, Length);
        int pageIndex = (int)(offset / PageSize);
        while (_pages.Count <= pageIndex)
            _pages.Add(null);
        byte[] page = _pages[pageIndex] ??= new byte[PageSize];
        page[(int)(offset % PageSize)] ^= mask;
    }

    /// <summary>Copies the logical content into a new array.</summary>
    public byte[] ToArray()
    {
        if (Length > Array.MaxLength)
            throw new InvalidOperationException("Buffer too large to materialize.");
        var bytes = new byte[Length];
        Read(0, bytes);
        return bytes;
    }
}
