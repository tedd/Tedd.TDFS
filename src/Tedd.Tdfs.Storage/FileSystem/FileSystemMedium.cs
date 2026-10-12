using Microsoft.Win32.SafeHandles;

namespace Tedd.Tdfs.Storage;

/// <summary>
/// <see cref="IStorageMedium"/> over a real directory, using <see cref="RandomAccess"/> and <see cref="SafeFileHandle"/>.
/// Paths are relative to the root and use '/' separators. <see cref="IStorageFile.FlushAsync"/> maps to
/// <see cref="RandomAccess.FlushToDisk"/>; <see cref="FlushDirectoryAsync"/> fsyncs the directory on Unix-like systems
/// (best effort) and is a no-op on Windows, where NTFS journals directory metadata. Capacity accounting is best-effort:
/// it tracks the bytes of files under the root as changed through this instance.
/// </summary>
public sealed class FileSystemMedium : IStorageMedium
{
    private readonly string _root;
    private readonly Lock _usedGate = new();
    private long _usedBytes = -1;

    /// <summary>Creates a medium rooted at <paramref name="rootDirectory"/> (created if missing).</summary>
    /// <param name="rootDirectory">Root directory on the local file system.</param>
    /// <param name="capacityBytes">Capacity; writes that would grow the used bytes beyond it fail with ENOSPC.</param>
    public FileSystemMedium(string rootDirectory, long capacityBytes = long.MaxValue)
    {
        ArgumentException.ThrowIfNullOrEmpty(rootDirectory);
        ArgumentOutOfRangeException.ThrowIfNegative(capacityBytes);
        _root = Path.GetFullPath(rootDirectory);
        Directory.CreateDirectory(_root);
        CapacityBytes = capacityBytes;
        Name = _root;
    }

    /// <summary>The root directory (full path).</summary>
    public string RootDirectory => _root;

    /// <inheritdoc />
    public string Name { get; }

    /// <inheritdoc />
    public long CapacityBytes { get; }

    /// <inheritdoc />
    public long UsedBytes
    {
        get
        {
            lock (_usedGate)
            {
                EnsureUsedInitializedLocked();
                return _usedBytes;
            }
        }
    }

    /// <summary>Maps a medium-relative path to a full local path (validated to stay under the root).</summary>
    public string GetFullPath(string path)
    {
        string normalized = StoragePath.Normalize(path);
        if (normalized.Length == 0)
            return _root;
        string full = Path.GetFullPath(Path.Combine(_root, normalized.Replace('/', Path.DirectorySeparatorChar)));
        string rootWithSeparator = Path.EndsInDirectorySeparator(_root) ? _root : _root + Path.DirectorySeparatorChar;
        if (!full.StartsWith(rootWithSeparator, OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal))
            throw new ArgumentException($"Storage path '{path}' escapes the medium root.", nameof(path));
        return full;
    }

    /// <inheritdoc />
    public ValueTask<IStorageFile> OpenAsync(string path, StorageOpenMode mode, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        string normalized = StoragePath.Normalize(path);
        if (normalized.Length == 0)
            throw new ArgumentException("A file path is required (the root directory is not a file).", nameof(path));
        string full = GetFullPath(normalized);

        FileMode fileMode = mode switch
        {
            StorageOpenMode.CreateNew => FileMode.CreateNew,
            StorageOpenMode.OpenExisting => FileMode.Open,
            StorageOpenMode.OpenOrCreate => FileMode.OpenOrCreate,
            StorageOpenMode.Truncate => FileMode.Create,
            _ => throw new ArgumentOutOfRangeException(nameof(mode)),
        };

        long previousLength = mode == StorageOpenMode.Truncate && File.Exists(full) ? new FileInfo(full).Length : 0;
        SafeFileHandle handle = File.OpenHandle(full, fileMode, FileAccess.ReadWrite, FileShare.ReadWrite | FileShare.Delete, FileOptions.Asynchronous);
        if (previousLength > 0)
            AdjustUsed(-previousLength);
        return new ValueTask<IStorageFile>(new FileSystemStorageFile(this, handle, normalized));
    }

    /// <inheritdoc />
    public ValueTask<bool> ExistsAsync(string path, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        string full = GetFullPath(path);
        return new ValueTask<bool>(File.Exists(full) || Directory.Exists(full));
    }

    /// <inheritdoc />
    public ValueTask DeleteAsync(string path, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        string normalized = StoragePath.Normalize(path);
        if (normalized.Length == 0)
            throw new ArgumentException("The root directory cannot be deleted.", nameof(path));
        string full = GetFullPath(normalized);
        if (File.Exists(full))
        {
            long length = new FileInfo(full).Length;
            File.Delete(full);
            AdjustUsed(-length);
        }
        else if (Directory.Exists(full))
        {
            Directory.Delete(full, recursive: false);
        }

        return default;
    }

    /// <inheritdoc />
    public ValueTask RenameAsync(string from, string to, bool overwrite, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        string fromFull = GetFullPath(from);
        string toFull = GetFullPath(to);
        if (string.Equals(fromFull, toFull, StringComparison.Ordinal))
        {
            if (!File.Exists(fromFull))
                throw new FileNotFoundException($"Could not find file '{from}'.", from);
            return default;
        }

        long replaced = overwrite && File.Exists(toFull) ? new FileInfo(toFull).Length : 0;
        File.Move(fromFull, toFull, overwrite);
        if (replaced > 0)
            AdjustUsed(-replaced);
        return default;
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<string>> ListAsync(string directory, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        string full = GetFullPath(directory);
        var names = new List<string>();
        foreach (string entry in Directory.EnumerateFileSystemEntries(full))
            names.Add(Path.GetFileName(entry));
        names.Sort(StringComparer.Ordinal);
        return new ValueTask<IReadOnlyList<string>>(names);
    }

    /// <inheritdoc />
    public ValueTask CreateDirectoryAsync(string directory, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        Directory.CreateDirectory(GetFullPath(directory));
        return default;
    }

    /// <inheritdoc />
    public ValueTask FlushDirectoryAsync(string directory, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        string full = GetFullPath(directory);
        if (!Directory.Exists(full))
            throw new DirectoryNotFoundException($"Could not find directory '{directory}'.");
        if (!OperatingSystem.IsWindows())
            DirectorySync.TryFsyncDirectory(full);
        return default;
    }

    /// <inheritdoc />
    public override string ToString() => $"FileSystemMedium({_root})";

    /// <summary>Checks capacity for growing the used bytes by <paramref name="growth"/> and reserves it.</summary>
    internal void ReserveGrowth(string path, long growth)
    {
        if (growth <= 0)
            return;
        lock (_usedGate)
        {
            EnsureUsedInitializedLocked();
            if (CapacityBytes != long.MaxValue && _usedBytes + growth > CapacityBytes)
                throw StorageErrors.DiskFull(path);
            _usedBytes += growth;
        }
    }

    /// <summary>Adjusts the tracked used bytes.</summary>
    internal void AdjustUsed(long delta)
    {
        if (delta == 0)
            return;
        lock (_usedGate)
        {
            if (_usedBytes < 0)
                return; // not yet initialized: the lazy scan will see the real sizes
            _usedBytes = Math.Max(0, _usedBytes + delta);
        }
    }

    private void EnsureUsedInitializedLocked()
    {
        if (_usedBytes >= 0)
            return;
        long total = 0;
        try
        {
            foreach (string file in Directory.EnumerateFiles(_root, "*", SearchOption.AllDirectories))
            {
                try
                {
                    total += new FileInfo(file).Length;
                }
                catch (IOException)
                {
                    // File vanished during the scan.
                }
            }
        }
        catch (DirectoryNotFoundException)
        {
            total = 0;
        }

        _usedBytes = total;
    }
}
