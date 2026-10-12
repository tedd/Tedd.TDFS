namespace Tedd.Tdfs.Storage;

/// <summary>Atomic replacement of small files: write temp, flush, rename over the target, flush the directory.</summary>
public static class AtomicFile
{
    /// <summary>Atomically and durably replaces <paramref name="path"/> with <paramref name="content"/>.</summary>
    public static ValueTask WriteAsync(IStorageMedium medium, string path, ReadOnlyMemory<byte> content, CancellationToken ct = default)
        => throw new NotImplementedException("AtomicFile.WriteAsync: implemented in a later phase");

    /// <summary>Reads a file written by <see cref="WriteAsync"/>; null when absent; a torn or corrupt file throws <see cref="DfsIntegrityException"/>.</summary>
    public static ValueTask<byte[]?> ReadAsync(IStorageMedium medium, string path, CancellationToken ct = default)
        => throw new NotImplementedException("AtomicFile.ReadAsync: implemented in a later phase");
}
