using System.Text;

namespace Tedd.Tdfs;

/// <summary>
/// Helpers for alternate data stream paths (Addendum A, ADS-01). A normalized path may end in <c>:stream</c>
/// (<c>"/a/f:s"</c>); a path without one addresses the default (unnamed) stream. Syntax and normalization rules are
/// those of <see cref="DfsPath"/>.
/// </summary>
public static class DfsStreamPath
{
    /// <summary>The only supported stream type (<c>name:stream:$DATA</c>, <c>name::$DATA</c>).</summary>
    public const string DataType = "$DATA";

    /// <summary>Default maximum number of named streams per inode (ADS-02).</summary>
    public const int MaxStreamsPerInode = 1024;

    /// <summary>Name of the default stream in stream maps (the default stream has no name in paths).</summary>
    public const string DefaultStreamName = "";

    /// <summary>Splits <c>"/a/f:s"</c> into <c>("/a/f", "s")</c>; a path without a stream yields <c>(path, null)</c>.</summary>
    /// <exception cref="ArgumentException">The path is not normalized.</exception>
    public static (string FilePath, string? StreamName) Split(string normalizedPath)
    {
        DfsPath.RequireNormalized(normalizedPath, nameof(normalizedPath));
        int colon = StreamColon(normalizedPath);
        return colon < 0 ? (normalizedPath, null) : (normalizedPath[..colon], normalizedPath[(colon + 1)..]);
    }

    /// <summary>
    /// Builds the path of stream <paramref name="streamName"/> of a file. A null or empty stream name addresses the
    /// default stream and returns <paramref name="normalizedFilePath"/> unchanged. The stream name is validated and
    /// NFC-normalized.
    /// </summary>
    /// <exception cref="ArgumentException">The file path is not normalized, already addresses a stream, or is the root;
    /// the stream name is invalid; or the result exceeds <see cref="DfsPath.MaxPathBytes"/>.</exception>
    public static string Combine(string normalizedFilePath, string? streamName)
    {
        DfsPath.RequireNormalized(normalizedFilePath, nameof(normalizedFilePath));
        if (HasStreamCore(normalizedFilePath))
            throw DfsPath.InvalidPath("The path already addresses a stream.", nameof(normalizedFilePath));
        if (string.IsNullOrEmpty(streamName))
            return normalizedFilePath;
        if (DfsPath.IsRoot(normalizedFilePath))
            throw DfsPath.InvalidPath("The root directory has no streams.", nameof(normalizedFilePath));
        string? error = DfsPath.TryNormalizeStreamNameCore(streamName, out string? normalizedStream);
        if (error is not null)
            throw DfsPath.InvalidPath(error, nameof(streamName));
        string combined = string.Concat(normalizedFilePath, ":", normalizedStream);
        int bytes = Encoding.UTF8.GetByteCount(combined);
        if (bytes > DfsPath.MaxPathBytes)
            throw DfsPath.InvalidPath($"The path is {bytes} UTF-8 bytes after normalization; the limit is {DfsPath.MaxPathBytes}.", nameof(streamName));
        return combined;
    }

    /// <summary>True when the normalized path addresses a named stream.</summary>
    /// <exception cref="ArgumentException">The path is not normalized.</exception>
    public static bool HasStream(string normalizedPath)
    {
        DfsPath.RequireNormalized(normalizedPath, nameof(normalizedPath));
        return HasStreamCore(normalizedPath);
    }

    /// <summary>The file part of a normalized path (the path itself when it has no stream).</summary>
    /// <exception cref="ArgumentException">The path is not normalized.</exception>
    public static string GetFilePath(string normalizedPath) => Split(normalizedPath).FilePath;

    /// <summary>The stream name of a normalized path, or null for the default stream.</summary>
    /// <exception cref="ArgumentException">The path is not normalized.</exception>
    public static string? GetStreamName(string normalizedPath) => Split(normalizedPath).StreamName;

    /// <summary>Validates a stream name (entry-name rules, no ':', not starting with '$') and returns its NFC form.</summary>
    /// <exception cref="ArgumentNullException"><paramref name="streamName"/> is null.</exception>
    /// <exception cref="ArgumentException">The name is invalid (ParamName "streamName").</exception>
    public static string NormalizeStreamName(string streamName)
    {
        ArgumentNullException.ThrowIfNull(streamName);
        string? error = DfsPath.TryNormalizeStreamNameCore(streamName, out string? normalized);
        return error is null ? normalized! : throw DfsPath.InvalidPath(error, nameof(streamName));
    }

    /// <summary>Validates a stream name and returns its NFC form without throwing.</summary>
    public static bool TryNormalizeStreamName(string? streamName, out string normalized, out string? error)
    {
        error = DfsPath.TryNormalizeStreamNameCore(streamName, out string? result);
        normalized = result ?? string.Empty;
        return error is null;
    }

    /// <summary>True when an already-normalized path has a stream suffix (no validation).</summary>
    internal static bool HasStreamCore(string normalizedPath) => StreamColon(normalizedPath) >= 0;

    // Index of the stream separator in the final component of a normalized path, or -1.
    private static int StreamColon(string normalizedPath)
    {
        int slash = normalizedPath.LastIndexOf(DfsPath.Separator);
        return normalizedPath.IndexOf(DfsPath.StreamSeparator, slash + 1);
    }
}
