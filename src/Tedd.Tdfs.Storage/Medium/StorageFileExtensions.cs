namespace Tedd.Tdfs.Storage;

/// <summary>Loops over short reads and writes of an <see cref="IStorageFile"/>.</summary>
public static class StorageFileExtensions
{
    /// <summary>Writes all of <paramref name="data"/> at <paramref name="offset"/>, looping over short writes.</summary>
    /// <exception cref="IOException">A write made no progress (returned 0) or reported more bytes than requested.</exception>
    public static async ValueTask WriteFullyAsync(this IStorageFile f, long offset, ReadOnlyMemory<byte> data, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(f);
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        int written = 0;
        while (written < data.Length)
        {
            ct.ThrowIfCancellationRequested();
            int remaining = data.Length - written;
            int n = await f.WriteAsync(offset + written, data[written..], ct);
            if (n <= 0)
                throw new IOException($"Write to '{f.Path}' at offset {offset + written} made no progress ({remaining} bytes remaining).");
            if (n > remaining)
                throw new IOException($"Write to '{f.Path}' reported {n} bytes written but only {remaining} were requested.");
            written += n;
        }
    }

    /// <summary>Fills <paramref name="buffer"/> from <paramref name="offset"/>, looping over short reads.</summary>
    /// <exception cref="EndOfStreamException">The file ended before the buffer was filled.</exception>
    public static async ValueTask ReadFullyAsync(this IStorageFile f, long offset, Memory<byte> buffer, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(f);
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        int read = 0;
        while (read < buffer.Length)
        {
            ct.ThrowIfCancellationRequested();
            int n = await f.ReadAsync(offset + read, buffer[read..], ct);
            if (n <= 0)
                throw new EndOfStreamException($"File '{f.Path}' ended at offset {offset + read}; {buffer.Length - read} more bytes were required.");
            read += n;
        }
    }
}
