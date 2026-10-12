namespace Tedd.Tdfs.Storage;

/// <summary>Durability helpers on top of <see cref="IStorageMedium"/>.</summary>
public static class StorageMediumExtensions
{
    /// <summary>
    /// Creates <paramref name="directory"/> (and missing ancestors) and flushes every ancestor directory, from the root
    /// down, so the whole chain of directory entries is durable on return.
    /// </summary>
    public static async ValueTask CreateDirectoryDurableAsync(this IStorageMedium medium, string directory, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(medium);
        string normalized = StoragePath.Normalize(directory);
        await medium.CreateDirectoryAsync(normalized, ct);
        if (normalized.Length == 0)
            return;

        string current = StoragePath.Root;
        foreach (string component in normalized.Split('/'))
        {
            await medium.FlushDirectoryAsync(current, ct);
            current = StoragePath.Combine(current, component);
        }
    }

    /// <summary>Reads a whole file into memory; returns null when the file is absent.</summary>
    public static async ValueTask<byte[]?> TryReadAllBytesAsync(this IStorageMedium medium, string path, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(medium);
        if (!await medium.ExistsAsync(path, ct))
            return null;

        IStorageFile file;
        try
        {
            file = await medium.OpenAsync(path, StorageOpenMode.OpenExisting, ct);
        }
        catch (FileNotFoundException)
        {
            return null;
        }

        await using (file)
        {
            long length = file.Length;
            if (length > Array.MaxLength)
                throw new IOException($"File '{path}' is too large to read into memory ({length} bytes).");
            var bytes = new byte[length];
            await file.ReadFullyAsync(0, bytes, ct);
            return bytes;
        }
    }
}
