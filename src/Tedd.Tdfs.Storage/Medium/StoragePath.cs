namespace Tedd.Tdfs.Storage;

/// <summary>
/// Medium-relative path rules: components separated by '/', no leading or trailing separator, no "." or ".."
/// components; '\' is accepted on input and converted to '/'. The root directory is the empty string.
/// </summary>
public static class StoragePath
{
    /// <summary>The root directory ("").</summary>
    public const string Root = "";

    /// <summary>Normalizes a medium-relative path.</summary>
    /// <exception cref="ArgumentException">The path contains a ".." component, a NUL character or a drive/volume prefix.</exception>
    public static string Normalize(string path)
    {
        ArgumentNullException.ThrowIfNull(path);
        if (path.Length == 0)
            return Root;
        if (path.Contains('\0', StringComparison.Ordinal))
            throw new ArgumentException("Storage paths must not contain NUL characters.", nameof(path));
        if (path.Contains(':', StringComparison.Ordinal))
            throw new ArgumentException($"Storage path '{path}' must be relative to the medium root.", nameof(path));

        string[] parts = path.Replace('\\', '/').Split('/', StringSplitOptions.RemoveEmptyEntries);
        var kept = new List<string>(parts.Length);
        foreach (string part in parts)
        {
            if (part == ".")
                continue;
            if (part == "..")
                throw new ArgumentException($"Storage path '{path}' must not contain '..' components.", nameof(path));
            kept.Add(part);
        }

        return string.Join('/', kept);
    }

    /// <summary>Returns the parent directory of a normalized path ("" for top-level entries).</summary>
    /// <exception cref="ArgumentException">The path is the root.</exception>
    public static string GetParent(string normalizedPath)
    {
        ArgumentNullException.ThrowIfNull(normalizedPath);
        if (normalizedPath.Length == 0)
            throw new ArgumentException("The root directory has no parent.", nameof(normalizedPath));
        int slash = normalizedPath.LastIndexOf('/');
        return slash < 0 ? Root : normalizedPath[..slash];
    }

    /// <summary>Returns the last component of a normalized path.</summary>
    public static string GetName(string normalizedPath)
    {
        ArgumentNullException.ThrowIfNull(normalizedPath);
        int slash = normalizedPath.LastIndexOf('/');
        return slash < 0 ? normalizedPath : normalizedPath[(slash + 1)..];
    }

    /// <summary>Combines a normalized directory with a relative path and normalizes the result.</summary>
    public static string Combine(string directory, string relative)
    {
        ArgumentNullException.ThrowIfNull(directory);
        ArgumentNullException.ThrowIfNull(relative);
        string dir = Normalize(directory);
        string rel = Normalize(relative);
        if (dir.Length == 0)
            return rel;
        return rel.Length == 0 ? dir : dir + "/" + rel;
    }

    /// <summary>True when <paramref name="path"/> lies directly inside <paramref name="directory"/> (both normalized).</summary>
    public static bool IsDirectChild(string directory, string path)
    {
        ArgumentNullException.ThrowIfNull(directory);
        ArgumentNullException.ThrowIfNull(path);
        return path.Length != 0 && string.Equals(GetParent(path), directory, StringComparison.Ordinal);
    }

    /// <summary>True when <paramref name="path"/> lies anywhere below <paramref name="directory"/> (both normalized).</summary>
    public static bool IsDescendant(string directory, string path)
    {
        ArgumentNullException.ThrowIfNull(directory);
        ArgumentNullException.ThrowIfNull(path);
        if (path.Length == 0)
            return false;
        if (directory.Length == 0)
            return true;
        return path.Length > directory.Length + 1
            && path[directory.Length] == '/'
            && path.StartsWith(directory, StringComparison.Ordinal);
    }
}
