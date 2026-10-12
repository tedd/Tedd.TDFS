using Tedd.Tdfs.Storage;

namespace Tedd.Tdfs.Testing;

/// <summary>
/// The view of a disk one node process (incarnation) uses. When the process crashes or stops, the view is fenced: every
/// later call through it — including through files it opened — fails with <see cref="ObjectDisposedException"/>, so code of
/// a dead incarnation that is still unwinding (realtime threads, a work item running when the loop stopped) can never
/// change the disk image the next incarnation starts from.
/// </summary>
internal sealed class FencedMedium : IStorageMedium
{
    private readonly IStorageMedium _inner;
    private readonly string _owner;
    private volatile bool _fenced;

    public FencedMedium(IStorageMedium inner, string owner)
    {
        _inner = inner;
        _owner = owner;
    }

    /// <summary>The medium behind the fence.</summary>
    public IStorageMedium Inner => _inner;

    /// <summary>True once fenced.</summary>
    public bool IsFenced => _fenced;

    /// <inheritdoc />
    public string Name => _inner.Name;

    /// <inheritdoc />
    public long CapacityBytes => _inner.CapacityBytes;

    /// <inheritdoc />
    public long UsedBytes => _inner.UsedBytes;

    /// <summary>Fences the view (idempotent).</summary>
    public void Fence() => _fenced = true;

    /// <inheritdoc />
    public async ValueTask<IStorageFile> OpenAsync(string path, StorageOpenMode mode, CancellationToken ct = default)
    {
        ThrowIfFenced(path);
        IStorageFile file = await _inner.OpenAsync(path, mode, ct);
        if (_fenced)
        {
            await file.DisposeAsync();
            ThrowIfFenced(path);
        }

        return new FencedFile(this, file);
    }

    /// <inheritdoc />
    public ValueTask<bool> ExistsAsync(string path, CancellationToken ct = default)
    {
        ThrowIfFenced(path);
        return _inner.ExistsAsync(path, ct);
    }

    /// <inheritdoc />
    public ValueTask DeleteAsync(string path, CancellationToken ct = default)
    {
        ThrowIfFenced(path);
        return _inner.DeleteAsync(path, ct);
    }

    /// <inheritdoc />
    public ValueTask RenameAsync(string from, string to, bool overwrite, CancellationToken ct = default)
    {
        ThrowIfFenced(from);
        return _inner.RenameAsync(from, to, overwrite, ct);
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<string>> ListAsync(string directory, CancellationToken ct = default)
    {
        ThrowIfFenced(directory);
        return _inner.ListAsync(directory, ct);
    }

    /// <inheritdoc />
    public ValueTask CreateDirectoryAsync(string directory, CancellationToken ct = default)
    {
        ThrowIfFenced(directory);
        return _inner.CreateDirectoryAsync(directory, ct);
    }

    /// <inheritdoc />
    public ValueTask FlushDirectoryAsync(string directory, CancellationToken ct = default)
    {
        ThrowIfFenced(directory);
        return _inner.FlushDirectoryAsync(directory, ct);
    }

    /// <inheritdoc />
    public override string ToString() => $"FencedMedium({_owner}: {_inner})";

    internal void ThrowIfFenced(string path)
    {
        if (_fenced)
            throw new ObjectDisposedException(path, $"The process {_owner} that opened '{path}' has crashed or stopped; its view of the disk is fenced.");
    }

    private sealed class FencedFile : IStorageFile
    {
        private readonly FencedMedium _medium;
        private readonly IStorageFile _inner;

        public FencedFile(FencedMedium medium, IStorageFile inner)
        {
            _medium = medium;
            _inner = inner;
        }

        public string Path => _inner.Path;

        public long Length
        {
            get
            {
                _medium.ThrowIfFenced(Path);
                return _inner.Length;
            }
        }

        public ValueTask<int> ReadAsync(long offset, Memory<byte> buffer, CancellationToken ct = default)
        {
            _medium.ThrowIfFenced(Path);
            return _inner.ReadAsync(offset, buffer, ct);
        }

        public ValueTask<int> WriteAsync(long offset, ReadOnlyMemory<byte> data, CancellationToken ct = default)
        {
            _medium.ThrowIfFenced(Path);
            return _inner.WriteAsync(offset, data, ct);
        }

        public ValueTask FlushAsync(CancellationToken ct = default)
        {
            _medium.ThrowIfFenced(Path);
            return _inner.FlushAsync(ct);
        }

        public ValueTask SetLengthAsync(long length, CancellationToken ct = default)
        {
            _medium.ThrowIfFenced(Path);
            return _inner.SetLengthAsync(length, ct);
        }

        public ValueTask DisposeAsync() => _inner.DisposeAsync();
    }
}
