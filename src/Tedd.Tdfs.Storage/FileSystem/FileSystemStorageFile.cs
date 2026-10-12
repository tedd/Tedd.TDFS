using Microsoft.Win32.SafeHandles;

namespace Tedd.Tdfs.Storage;

/// <summary>An open file of a <see cref="FileSystemMedium"/>.</summary>
internal sealed class FileSystemStorageFile : IStorageFile
{
    private readonly FileSystemMedium _medium;
    private readonly SafeFileHandle _handle;
    private volatile bool _disposed;

    internal FileSystemStorageFile(FileSystemMedium medium, SafeFileHandle handle, string path)
    {
        _medium = medium;
        _handle = handle;
        Path = path;
    }

    /// <inheritdoc />
    public string Path { get; }

    /// <inheritdoc />
    public long Length
    {
        get
        {
            ThrowIfDisposed();
            return RandomAccess.GetLength(_handle);
        }
    }

    /// <inheritdoc />
    public ValueTask<int> ReadAsync(long offset, Memory<byte> buffer, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        return RandomAccess.ReadAsync(_handle, buffer, offset, ct);
    }

    /// <inheritdoc />
    public async ValueTask<int> WriteAsync(long offset, ReadOnlyMemory<byte> data, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        if (data.IsEmpty)
            return 0;
        long growth = Math.Max(0, offset + data.Length - RandomAccess.GetLength(_handle));
        _medium.ReserveGrowth(Path, growth);
        try
        {
            await RandomAccess.WriteAsync(_handle, data, offset, ct);
        }
        catch
        {
            _medium.AdjustUsed(-growth);
            throw;
        }

        return data.Length;
    }

    /// <inheritdoc />
    public ValueTask FlushAsync(CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        ThrowIfDisposed();
        RandomAccess.FlushToDisk(_handle);
        return default;
    }

    /// <inheritdoc />
    public ValueTask SetLengthAsync(long length, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        ThrowIfDisposed();
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        long current = RandomAccess.GetLength(_handle);
        long delta = length - current;
        if (delta > 0)
            _medium.ReserveGrowth(Path, delta);
        try
        {
            RandomAccess.SetLength(_handle, length);
        }
        catch
        {
            if (delta > 0)
                _medium.AdjustUsed(-delta);
            throw;
        }

        if (delta < 0)
            _medium.AdjustUsed(delta);
        return default;
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            _disposed = true;
            _handle.Dispose();
        }

        return default;
    }

    /// <inheritdoc />
    public override string ToString() => $"{_medium.Name}:{Path}";

    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(Path, $"The file handle for '{Path}' has been disposed.");
    }
}
