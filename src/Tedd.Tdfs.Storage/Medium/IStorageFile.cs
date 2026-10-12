namespace Tedd.Tdfs.Storage;

/// <summary>An open file on an <see cref="IStorageMedium"/>. Supports positional reads and writes.</summary>
public interface IStorageFile : IAsyncDisposable
{
    /// <summary>The medium-relative path the file was opened with.</summary>
    string Path { get; }

    /// <summary>Current (not necessarily durable) length in bytes.</summary>
    long Length { get; }

    /// <summary>Reads up to <paramref name="buffer"/>.Length bytes at <paramref name="offset"/>; returns 0 at or beyond end of file.</summary>
    ValueTask<int> ReadAsync(long offset, Memory<byte> buffer, CancellationToken ct = default);

    /// <summary>Writes at <paramref name="offset"/>, extending the file as needed. May write fewer bytes than requested (short write).</summary>
    /// <returns>The number of bytes written.</returns>
    ValueTask<int> WriteAsync(long offset, ReadOnlyMemory<byte> data, CancellationToken ct = default);

    /// <summary>Durability barrier: on successful completion all previous writes and length changes of this file are durable.</summary>
    ValueTask FlushAsync(CancellationToken ct = default);

    /// <summary>Truncates or extends (with zeroes) the file.</summary>
    ValueTask SetLengthAsync(long length, CancellationToken ct = default);
}
