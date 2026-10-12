namespace Tedd.Tdfs.Storage;

/// <summary>
/// A local storage device as seen by one node. Paths are relative to the medium root and use '/' separators
/// (see <see cref="StoragePath"/>); the root directory is "". File data becomes durable only after
/// <see cref="IStorageFile.FlushAsync"/>; namespace changes (file creation, rename, delete, directory creation) become
/// durable only after <see cref="FlushDirectoryAsync"/> on the affected parent directory.
/// </summary>
public interface IStorageMedium
{
    /// <summary>Diagnostic name of the medium.</summary>
    string Name { get; }

    /// <summary>Total capacity in bytes; writes beyond it fail with a disk-full <see cref="IOException"/>.</summary>
    long CapacityBytes { get; }

    /// <summary>Bytes currently used by files on the medium.</summary>
    long UsedBytes { get; }

    /// <summary>Opens a file for reading and writing.</summary>
    /// <exception cref="FileNotFoundException"><see cref="StorageOpenMode.OpenExisting"/> and the file is absent.</exception>
    /// <exception cref="DirectoryNotFoundException">The parent directory does not exist.</exception>
    /// <exception cref="IOException"><see cref="StorageOpenMode.CreateNew"/> and the file exists, or a device error.</exception>
    ValueTask<IStorageFile> OpenAsync(string path, StorageOpenMode mode, CancellationToken ct = default);

    /// <summary>True when a file or directory exists at <paramref name="path"/>.</summary>
    ValueTask<bool> ExistsAsync(string path, CancellationToken ct = default);

    /// <summary>Deletes a file, or an empty directory. Deleting an absent path is a no-op.</summary>
    /// <exception cref="IOException">The path is a non-empty directory, or a device error.</exception>
    ValueTask DeleteAsync(string path, CancellationToken ct = default);

    /// <summary>Renames a file.</summary>
    /// <exception cref="FileNotFoundException">The source file is absent.</exception>
    /// <exception cref="IOException">The destination exists and <paramref name="overwrite"/> is false.</exception>
    ValueTask RenameAsync(string from, string to, bool overwrite, CancellationToken ct = default);

    /// <summary>Lists the names (not paths) of the files and subdirectories directly inside a directory, sorted ordinally.</summary>
    /// <exception cref="DirectoryNotFoundException">The directory does not exist.</exception>
    ValueTask<IReadOnlyList<string>> ListAsync(string directory, CancellationToken ct = default);

    /// <summary>Creates a directory and any missing ancestors (idempotent). Not durable until the parents are flushed.</summary>
    ValueTask CreateDirectoryAsync(string directory, CancellationToken ct = default);

    /// <summary>Durability barrier for the namespace: persists create/rename/delete of entries inside <paramref name="directory"/>.</summary>
    ValueTask FlushDirectoryAsync(string directory, CancellationToken ct = default);
}
