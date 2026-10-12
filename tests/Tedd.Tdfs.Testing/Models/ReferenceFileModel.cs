namespace Tedd.Tdfs.Testing;

/// <summary>
/// Independent byte-array model of one file (PROP-001): a committed image plus a staged (unpublished) image.
/// Writes and length changes apply to the staged image; <see cref="Flush"/> publishes it, <see cref="Abort"/> discards it.
/// Bytes never written inside the length (sparse holes, extension after truncation) read as zero.
/// </summary>
/// <remarks>Deliberately simple and shares no code with the implementation (HAR-010). Sizes are limited to <see cref="Array.MaxLength"/>.</remarks>
public sealed class ReferenceFileModel
{
    private byte[] _committed;
    private byte[] _staged;   // bytes at index >= _stagedLength are always zero
    private int _stagedLength;

    /// <summary>Creates an empty file.</summary>
    public ReferenceFileModel()
        : this(ReadOnlySpan<byte>.Empty)
    {
    }

    /// <summary>Creates a file whose committed and staged content is <paramref name="initial"/>.</summary>
    public ReferenceFileModel(ReadOnlySpan<byte> initial)
    {
        _committed = initial.ToArray();
        _staged = initial.ToArray();
        _stagedLength = _staged.Length;
    }

    /// <summary>A copy of the committed (published) content.</summary>
    public byte[] Committed => (byte[])_committed.Clone();

    /// <summary>A copy of the staged content (what the writer's own reads see).</summary>
    public byte[] Staged => _staged.AsSpan(0, _stagedLength).ToArray();

    /// <summary>Committed length.</summary>
    public long CommittedLength => _committed.Length;

    /// <summary>Staged length.</summary>
    public long StagedLength => _stagedLength;

    /// <summary>True when the staged image differs from the committed one.</summary>
    public bool HasPendingChanges => !_staged.AsSpan(0, _stagedLength).SequenceEqual(_committed);

    /// <summary>Writes <paramref name="data"/> at <paramref name="offset"/> of the staged image, zero-filling any gap past the end.</summary>
    /// <exception cref="ArgumentOutOfRangeException">Negative offset, or the end exceeds the model's maximum size.</exception>
    public void Write(long offset, ReadOnlySpan<byte> data)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        long end = checked(offset + data.Length);
        if (end > Array.MaxLength)
            throw new ArgumentOutOfRangeException(nameof(offset), offset, $"The model supports files up to {Array.MaxLength} bytes.");
        if (data.IsEmpty)
            return;
        EnsureCapacity((int)end);
        data.CopyTo(_staged.AsSpan((int)offset));
        if (end > _stagedLength)
            _stagedLength = (int)end;
    }

    /// <summary>Sets the staged length: truncation discards bytes, extension adds zeros.</summary>
    /// <exception cref="ArgumentOutOfRangeException">Negative length or above the model's maximum size.</exception>
    public void SetLength(long length)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        if (length > Array.MaxLength)
            throw new ArgumentOutOfRangeException(nameof(length), length, $"The model supports files up to {Array.MaxLength} bytes.");
        int newLength = (int)length;
        if (newLength < _stagedLength)
            Array.Clear(_staged, newLength, _stagedLength - newLength); // keep the zero-tail invariant
        else
            EnsureCapacity(newLength);
        _stagedLength = newLength;
    }

    /// <summary>Publishes the staged image (it becomes the committed image).</summary>
    public void Flush() => _committed = _staged.AsSpan(0, _stagedLength).ToArray();

    /// <summary>Discards staged changes (the staged image becomes the committed image again).</summary>
    public void Abort()
    {
        _staged = (byte[])_committed.Clone();
        _stagedLength = _staged.Length;
    }

    /// <summary>
    /// Reads up to <paramref name="count"/> bytes at <paramref name="offset"/> of the staged or committed image; returns
    /// fewer bytes at the end and an empty array at or beyond the end.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">Negative offset or count.</exception>
    public byte[] Read(long offset, int count, bool staged)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        ReadOnlySpan<byte> image = staged ? _staged.AsSpan(0, _stagedLength) : _committed;
        if (offset >= image.Length)
            return [];
        int available = image.Length - (int)offset;
        return image.Slice((int)offset, Math.Min(available, count)).ToArray();
    }

    private void EnsureCapacity(int required)
    {
        if (required <= _staged.Length)
            return;
        long grown = Math.Max(required, Math.Min((long)Array.MaxLength, Math.Max(16L, (long)_staged.Length * 2)));
        var bigger = new byte[grown];
        _staged.AsSpan(0, _stagedLength).CopyTo(bigger);
        _staged = bigger;
    }
}
