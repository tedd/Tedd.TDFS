namespace Tedd.Tdfs.Testing;

/// <summary>
/// Independent namespace-tree model (PROP-002): files and directories with inode identities.
/// </summary>
/// <remarks>
/// <para>
/// Paths are compared ordinally and must already be normalised by the caller (absolute, '/'-separated, no empty, "." or
/// ".." components, no trailing '/' except the root). The root "/" always exists, has inode 1 and cannot be created,
/// moved or deleted. New inodes are allocated monotonically from 2 and never reused; compare identities (same/different),
/// not raw numbers, against the implementation.
/// </para>
/// <para>
/// Errors are reported as <see cref="DfsErrorCode"/> names: <c>InvalidPath</c>, <c>DirectoryNotFound</c> (missing parent),
/// <c>NotADirectory</c> (a parent is a file), <c>FileNotFound</c> (missing source/target), <c>AlreadyExists</c>,
/// <c>IsADirectory</c>, <c>DirectoryNotEmpty</c>, <c>InvalidArgument</c> (directory moved into itself or a descendant).
/// A failed operation never changes the model.
/// </para>
/// Deliberately simple and shares no code with the implementation (HAR-010).
/// </remarks>
public sealed class ReferenceNamespaceModel
{
    /// <summary>Inode of the root directory.</summary>
    public const long RootInode = 1;

    private readonly SortedDictionary<string, Entry> _entries = new(StringComparer.Ordinal);
    private long _nextInode = RootInode + 1;

    /// <summary>Creates a model containing only the root directory.</summary>
    public ReferenceNamespaceModel() => _entries["/"] = new Entry(true, RootInode);

    /// <summary>Number of entries including the root.</summary>
    public int Count => _entries.Count;

    /// <summary>Highest inode number allocated so far.</summary>
    public long LastAllocatedInode => _nextInode - 1;

    /// <summary>Creates an empty file (create-new semantics): the parent must be an existing directory and the name absent.</summary>
    public bool CreateFile(string path, out string? error)
    {
        if (!Check(path, allowRoot: false, out error))
            return false;
        if (!CheckParent(path, out error))
            return false;
        if (_entries.ContainsKey(path))
            return Fail(DfsErrorCode.AlreadyExists, out error);
        _entries[path] = new Entry(false, _nextInode++);
        return true;
    }

    /// <summary>
    /// Creates one directory: the parent must be an existing directory. An existing directory is success (idempotent,
    /// inode unchanged); an existing file is <c>AlreadyExists</c>. The root counts as an existing directory.
    /// </summary>
    public bool CreateDirectory(string path, out string? error) => CreateDirectory(path, recursive: false, out error);

    /// <summary>
    /// Creates a directory; with <paramref name="recursive"/> missing ancestors are created first (a file anywhere on the
    /// path fails with <c>NotADirectory</c>/<c>AlreadyExists</c> and creates nothing).
    /// </summary>
    public bool CreateDirectory(string path, bool recursive, out string? error)
    {
        if (!Check(path, allowRoot: true, out error))
            return false;
        if (path == "/")
            return true;

        if (!recursive)
        {
            if (!CheckParent(path, out error))
                return false;
        }
        else
        {
            foreach (string ancestor in Ancestors(path))
            {
                if (_entries.TryGetValue(ancestor, out Entry? existing) && !existing.IsDirectory)
                    return Fail(DfsErrorCode.NotADirectory, out error);
            }
        }

        if (_entries.TryGetValue(path, out Entry? entry))
        {
            if (entry.IsDirectory)
                return true;
            return Fail(DfsErrorCode.AlreadyExists, out error);
        }

        if (recursive)
        {
            foreach (string ancestor in Ancestors(path))
            {
                if (!_entries.ContainsKey(ancestor))
                    _entries[ancestor] = new Entry(true, _nextInode++);
            }
        }

        _entries[path] = new Entry(true, _nextInode++);
        return true;
    }

    /// <summary>
    /// Atomically moves a file or directory subtree; inodes are preserved. With <paramref name="overwrite"/> an existing
    /// destination file is replaced (its inode disappears) by a source file; directories are never replaced.
    /// Moving onto itself is a successful no-op.
    /// </summary>
    public bool Move(string from, string to, bool overwrite, out string? error)
    {
        if (!Check(from, allowRoot: false, out error) || !Check(to, allowRoot: false, out error))
            return false;
        if (!_entries.TryGetValue(from, out Entry? source))
            return Fail(ParentIsDirectory(from) ? DfsErrorCode.FileNotFound : DfsErrorCode.DirectoryNotFound, out error);
        if (string.Equals(from, to, StringComparison.Ordinal))
            return true;
        if (source.IsDirectory && IsStrictDescendant(from, to))
            return Fail(DfsErrorCode.InvalidArgument, out error);
        if (!CheckParent(to, out error))
            return false;

        if (_entries.TryGetValue(to, out Entry? destination))
        {
            if (!overwrite)
                return Fail(DfsErrorCode.AlreadyExists, out error);
            if (destination.IsDirectory)
                return Fail(DfsErrorCode.IsADirectory, out error);
            if (source.IsDirectory)
                return Fail(DfsErrorCode.AlreadyExists, out error);
            _entries.Remove(to);
        }

        List<KeyValuePair<string, Entry>> moved = SubtreeOf(from);
        foreach (KeyValuePair<string, Entry> item in moved)
            _entries.Remove(item.Key);
        foreach (KeyValuePair<string, Entry> item in moved)
            _entries[to + item.Key[from.Length..]] = item.Value;
        return true;
    }

    /// <summary>
    /// Copies a file (not a directory). An existing destination requires <paramref name="overwrite"/> and keeps its inode
    /// (content replacement, FSD §5.2); otherwise a new inode is allocated. Copying a path onto itself fails with
    /// <c>InvalidArgument</c>.
    /// </summary>
    public bool CopyFile(string from, string to, bool overwrite, out string? error)
    {
        if (!Check(from, allowRoot: false, out error) || !Check(to, allowRoot: false, out error))
            return false;
        if (!_entries.TryGetValue(from, out Entry? source))
            return Fail(ParentIsDirectory(from) ? DfsErrorCode.FileNotFound : DfsErrorCode.DirectoryNotFound, out error);
        if (source.IsDirectory)
            return Fail(DfsErrorCode.IsADirectory, out error);
        if (!CheckParent(to, out error))
            return false;
        if (string.Equals(from, to, StringComparison.Ordinal))
            return Fail(DfsErrorCode.InvalidArgument, out error);
        if (_entries.TryGetValue(to, out Entry? destination))
        {
            if (!overwrite)
                return Fail(DfsErrorCode.AlreadyExists, out error);
            if (destination.IsDirectory)
                return Fail(DfsErrorCode.IsADirectory, out error);
            return true; // content replaced in place; identity preserved
        }

        _entries[to] = new Entry(false, _nextInode++);
        return true;
    }

    /// <summary>
    /// Deletes a file or directory. A nonempty directory requires <paramref name="recursive"/> (else
    /// <c>DirectoryNotEmpty</c>); a missing path fails with <c>FileNotFound</c> (or <c>DirectoryNotFound</c> when its parent
    /// is missing). The root cannot be deleted.
    /// </summary>
    public bool Delete(string path, bool recursive, out string? error)
    {
        if (!Check(path, allowRoot: false, out error))
            return false;
        if (!_entries.TryGetValue(path, out Entry? entry))
            return Fail(ParentIsDirectory(path) ? DfsErrorCode.FileNotFound : DfsErrorCode.DirectoryNotFound, out error);
        List<KeyValuePair<string, Entry>> subtree = SubtreeOf(path);
        if (entry.IsDirectory && subtree.Count > 1 && !recursive)
            return Fail(DfsErrorCode.DirectoryNotEmpty, out error);
        foreach (KeyValuePair<string, Entry> item in subtree)
            _entries.Remove(item.Key);
        return true;
    }

    /// <summary>True when <paramref name="path"/> exists (file or directory).</summary>
    public bool Exists(string path) => path is not null && _entries.ContainsKey(path);

    /// <summary>True when <paramref name="path"/> exists and is a file.</summary>
    public bool IsFile(string path) => path is not null && _entries.TryGetValue(path, out Entry? e) && !e.IsDirectory;

    /// <summary>True when <paramref name="path"/> exists and is a directory.</summary>
    public bool IsDirectory(string path) => path is not null && _entries.TryGetValue(path, out Entry? e) && e.IsDirectory;

    /// <summary>Names of the direct children of a directory, in ordinal order.</summary>
    /// <exception cref="DirectoryNotFoundException">The directory does not exist.</exception>
    /// <exception cref="IOException">The path is a file.</exception>
    public IReadOnlyList<string> List(string directory)
    {
        ArgumentNullException.ThrowIfNull(directory);
        if (!_entries.TryGetValue(directory, out Entry? entry))
            throw new DirectoryNotFoundException($"Model: directory '{directory}' does not exist.");
        if (!entry.IsDirectory)
            throw new IOException($"Model: '{directory}' is a file.");
        string prefix = directory == "/" ? "/" : directory + "/";
        var names = new List<string>();
        foreach (string path in _entries.Keys)
        {
            if (path.Length <= prefix.Length || !path.StartsWith(prefix, StringComparison.Ordinal))
                continue;
            string rest = path[prefix.Length..];
            if (!rest.Contains('/', StringComparison.Ordinal))
                names.Add(rest);
        }

        names.Sort(StringComparer.Ordinal);
        return names;
    }

    /// <summary>The inode of an existing path.</summary>
    /// <exception cref="KeyNotFoundException">The path does not exist.</exception>
    public long InodeOf(string path)
    {
        ArgumentNullException.ThrowIfNull(path);
        return _entries.TryGetValue(path, out Entry? entry)
            ? entry.Inode
            : throw new KeyNotFoundException($"Model: '{path}' does not exist.");
    }

    /// <summary>The inode of a path, or false when absent.</summary>
    public bool TryGetInode(string path, out long inode)
    {
        if (path is not null && _entries.TryGetValue(path, out Entry? entry))
        {
            inode = entry.Inode;
            return true;
        }

        inode = 0;
        return false;
    }

    /// <summary>All paths (including "/") in ordinal order.</summary>
    public IReadOnlyList<string> AllPaths() => _entries.Keys.ToArray();

    private static bool Check(string path, bool allowRoot, out string? error)
    {
        if (!IsValidPath(path))
            return Fail(DfsErrorCode.InvalidPath, out error);
        if (!allowRoot && path == "/")
            return Fail(DfsErrorCode.InvalidPath, out error);
        error = null;
        return true;
    }

    /// <summary>The model's (deliberately minimal) path rule; callers are expected to pass normalised paths.</summary>
    private static bool IsValidPath(string path)
    {
        if (string.IsNullOrEmpty(path) || path[0] != '/')
            return false;
        if (path == "/")
            return true;
        if (path[^1] == '/')
            return false;
        foreach (string component in path[1..].Split('/'))
        {
            if (component.Length == 0 || component == "." || component == ".." || component.Contains('\0', StringComparison.Ordinal)
                || component.Contains('\\', StringComparison.Ordinal))
                return false;
        }

        return true;
    }

    private bool CheckParent(string path, out string? error)
    {
        string parent = ParentOf(path);
        if (!_entries.TryGetValue(parent, out Entry? entry))
            return Fail(DfsErrorCode.DirectoryNotFound, out error);
        if (!entry.IsDirectory)
            return Fail(DfsErrorCode.NotADirectory, out error);
        error = null;
        return true;
    }

    private bool ParentIsDirectory(string path) => _entries.TryGetValue(ParentOf(path), out Entry? e) && e.IsDirectory;

    private static string ParentOf(string path)
    {
        int slash = path.LastIndexOf('/');
        return slash <= 0 ? "/" : path[..slash];
    }

    /// <summary>Proper ancestors of <paramref name="path"/> from the top, excluding the root.</summary>
    private static IEnumerable<string> Ancestors(string path)
    {
        for (int i = 1; i < path.Length; i++)
        {
            if (path[i] == '/')
                yield return path[..i];
        }
    }

    private static bool IsStrictDescendant(string ancestor, string path) =>
        path.Length > ancestor.Length && path.StartsWith(ancestor, StringComparison.Ordinal) && path[ancestor.Length] == '/';

    /// <summary>The entry at <paramref name="path"/> and all its descendants.</summary>
    private List<KeyValuePair<string, Entry>> SubtreeOf(string path)
    {
        var result = new List<KeyValuePair<string, Entry>>();
        foreach (KeyValuePair<string, Entry> item in _entries)
        {
            if (string.Equals(item.Key, path, StringComparison.Ordinal) || IsStrictDescendant(path, item.Key))
                result.Add(item);
        }

        return result;
    }

    private static bool Fail(DfsErrorCode code, out string? error)
    {
        error = code.ToString();
        return false;
    }

    private sealed record Entry(bool IsDirectory, long Inode);
}
