using System.Text;

namespace Tedd.Tdfs.Testing.SnpApp;

/// <summary>
/// Captures and compares complete namespace + byte images of a volume, a snapshot view or a backup view through the
/// public SDK facades (an independent oracle view: directories are enumerated, every file is read in full).
/// </summary>
/// <remarks>
/// A tree is an ordinal-sorted map from absolute path to content: <c>null</c> marks a directory, a byte array is a
/// file's complete contents. The captured root itself is included as a directory entry unless it is <c>/</c>.
/// </remarks>
public static class SnapshotTree
{
    /// <summary>Captures every directory and file below <paramref name="root"/> (empty when the root does not exist).</summary>
    public static async Task<SortedDictionary<string, byte[]?>> CaptureAsync(DfsFileClient files, DfsDirectoryClient directories,
        string root = "/", CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(files);
        ArgumentNullException.ThrowIfNull(directories);
        string normalized = DfsPath.Normalize(root);
        var tree = new SortedDictionary<string, byte[]?>(StringComparer.Ordinal);
        if (!string.Equals(normalized, DfsPath.Root, StringComparison.Ordinal))
        {
            if (!await directories.ExistsAsync(normalized, ct))
                return tree;
            tree[normalized] = null;
        }

        await WalkAsync(files, directories, normalized, tree, ct);
        return tree;
    }

    /// <summary>Captures a live volume below <paramref name="root"/>.</summary>
    public static Task<SortedDictionary<string, byte[]?>> CaptureAsync(DfsVolume volume, string root = "/", CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(volume);
        return CaptureAsync(volume.Files, volume.Directories, root, ct);
    }

    /// <summary>Captures a snapshot view below <paramref name="root"/>.</summary>
    public static Task<SortedDictionary<string, byte[]?>> CaptureAsync(DfsSnapshotView view, string root = "/", CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(view);
        return CaptureAsync(view.Files, view.Directories, root, ct);
    }

    /// <summary>
    /// Captures a live volume below <paramref name="root"/> while other clients may hold writable handles or transaction
    /// writer reservations: every file is read through an explicit read-only, version-pinned open that grants
    /// <c>FileShare.ReadWrite | FileShare.Delete</c> (the compatible reader pairing of FSD §7 / test plan §5), so a
    /// setup-level sharing conflict can never masquerade as a visibility result.
    /// </summary>
    public static async Task<SortedDictionary<string, byte[]?>> CaptureSharedAsync(DfsVolume volume, string root = "/", CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(volume);
        string normalized = DfsPath.Normalize(root);
        var tree = new SortedDictionary<string, byte[]?>(StringComparer.Ordinal);
        if (!string.Equals(normalized, DfsPath.Root, StringComparison.Ordinal))
        {
            if (!await volume.Directories.ExistsAsync(normalized, ct))
                return tree;
            tree[normalized] = null;
        }

        await WalkSharedAsync(volume, normalized, tree, ct);
        return tree;
    }

    /// <summary>Only the file entries of a tree.</summary>
    public static SortedDictionary<string, byte[]> FilesOf(IReadOnlyDictionary<string, byte[]?> tree)
    {
        ArgumentNullException.ThrowIfNull(tree);
        var files = new SortedDictionary<string, byte[]>(StringComparer.Ordinal);
        foreach (KeyValuePair<string, byte[]?> pair in tree)
        {
            if (pair.Value is not null)
                files[pair.Key] = pair.Value;
        }

        return files;
    }

    /// <summary>A deep copy of a tree.</summary>
    public static SortedDictionary<string, byte[]?> Clone(IReadOnlyDictionary<string, byte[]?> tree)
    {
        ArgumentNullException.ThrowIfNull(tree);
        var copy = new SortedDictionary<string, byte[]?>(StringComparer.Ordinal);
        foreach (KeyValuePair<string, byte[]?> pair in tree)
            copy[pair.Key] = pair.Value is null ? null : (byte[])pair.Value.Clone();
        return copy;
    }

    /// <summary>
    /// Fails the test unless both trees contain exactly the same paths, the same kinds (file/directory) and byte-identical
    /// file contents; the message lists missing/extra paths and the first differing offset.
    /// </summary>
    public static void AssertEqual(IReadOnlyDictionary<string, byte[]?> expected, IReadOnlyDictionary<string, byte[]?> actual, string because)
    {
        ArgumentNullException.ThrowIfNull(expected);
        ArgumentNullException.ThrowIfNull(actual);
        string[] missing = expected.Keys.Except(actual.Keys, StringComparer.Ordinal).OrderBy(k => k, StringComparer.Ordinal).ToArray();
        string[] extra = actual.Keys.Except(expected.Keys, StringComparer.Ordinal).OrderBy(k => k, StringComparer.Ordinal).ToArray();
        if (missing.Length > 0 || extra.Length > 0)
        {
            var sb = new StringBuilder($"Namespace differs ({because}).");
            if (missing.Length > 0)
                sb.Append(" Missing: ").Append(string.Join(", ", missing)).Append('.');
            if (extra.Length > 0)
                sb.Append(" Unexpected: ").Append(string.Join(", ", extra)).Append('.');
            Assert.Fail(sb.ToString());
        }

        foreach (KeyValuePair<string, byte[]?> pair in expected)
        {
            byte[]? other = actual[pair.Key];
            if (pair.Value is null || other is null)
            {
                if (pair.Value is not null || other is not null)
                    Assert.Fail($"Entry kind differs at '{pair.Key}' ({because}): expected {(pair.Value is null ? "directory" : "file")}, actual {(other is null ? "directory" : "file")}.");
                continue;
            }

            AssertEx.BytesEqual(pair.Value, other, $"{because}: contents of '{pair.Key}'");
        }
    }

    /// <summary>True when both trees are equal in the sense of <see cref="AssertEqual"/>.</summary>
    public static bool AreEqual(IReadOnlyDictionary<string, byte[]?> a, IReadOnlyDictionary<string, byte[]?> b)
    {
        ArgumentNullException.ThrowIfNull(a);
        ArgumentNullException.ThrowIfNull(b);
        if (a.Count != b.Count)
            return false;
        foreach (KeyValuePair<string, byte[]?> pair in a)
        {
            if (!b.TryGetValue(pair.Key, out byte[]? other))
                return false;
            if (pair.Value is null || other is null)
            {
                if (pair.Value is not null || other is not null)
                    return false;
                continue;
            }

            if (!pair.Value.AsSpan().SequenceEqual(other))
                return false;
        }

        return true;
    }

    private static async Task WalkAsync(DfsFileClient files, DfsDirectoryClient directories, string directory,
        SortedDictionary<string, byte[]?> tree, CancellationToken ct)
    {
        var subdirectories = new List<string>();
        await foreach (DfsEntryInfo entry in directories.EnumerateAsync(directory, null, ct))
        {
            if (entry.Kind == DfsEntryKind.Directory)
            {
                tree[entry.Path] = null;
                subdirectories.Add(entry.Path);
            }
            else
            {
                tree[entry.Path] = await files.ReadAllBytesAsync(entry.Path, ct);
            }
        }

        foreach (string sub in subdirectories)
            await WalkAsync(files, directories, sub, tree, ct);
    }

    private static async Task WalkSharedAsync(DfsVolume volume, string directory, SortedDictionary<string, byte[]?> tree, CancellationToken ct)
    {
        var subdirectories = new List<string>();
        await foreach (DfsEntryInfo entry in volume.Directories.EnumerateAsync(directory, null, ct))
        {
            if (entry.Kind == DfsEntryKind.Directory)
            {
                tree[entry.Path] = null;
                subdirectories.Add(entry.Path);
                continue;
            }

            await using DfsStream stream = await volume.Files.OpenAsync(entry.Path, new DfsOpenOptions
            {
                Mode = FileMode.Open,
                Access = FileAccess.Read,
                Share = FileShare.ReadWrite | FileShare.Delete,
                ReadView = DfsReadView.VersionPinned,
            }, ct);
            using var buffer = new MemoryStream();
            await stream.CopyToAsync(buffer, ct);
            tree[entry.Path] = buffer.ToArray();
        }

        foreach (string sub in subdirectories)
            await WalkSharedAsync(volume, sub, tree, ct);
    }
}
