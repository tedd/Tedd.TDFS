using System.Text;
using Tedd.Tdfs.Text;

namespace Tedd.Tdfs;

/// <summary>
/// Volume path validation and normalization (FSD NS-01..NS-03, Addendum A ADS-01). Paths are absolute and
/// volume-relative, use '/' as the only separator, are case-sensitive and compared ordinally, and are normalized to
/// Unicode NFC. The final component may address a named stream (<c>name:stream</c>).
/// </summary>
/// <remarks>
/// <para>Rules: absolute; root is "/"; no empty components, no trailing '/' except the root, no "." or ".." components,
/// no NUL, no backslash, no unpaired surrogates; NFC normalization (platform-independent, see
/// <see cref="DfsUnicode"/>); each component at most <see cref="MaxComponentBytes"/> and the full path at most
/// <see cref="MaxPathBytes"/> UTF-8 bytes <b>after</b> normalization. Percent sequences are literal characters and are
/// never decoded.</para>
/// <para>Streams (ADS-01): ':' (<see cref="StreamSeparator"/>) is not an ordinary name character. It may appear only in
/// the final component, as <c>name:stream</c>, <c>name:stream:$DATA</c> (normalized to <c>name:stream</c>) or
/// <c>name::$DATA</c> (the default stream, normalized to <c>name</c>). Rejected: ':' in any other component, an empty
/// file name, an empty stream name (other than <c>::$DATA</c>), any stream type other than <c>$DATA</c> (ordinal,
/// case-sensitive), further separators, a stream on the root, and stream names starting with '$' (reserved for stream
/// types). The file name and the stream name each follow the component rules (including the
/// <see cref="MaxComponentBytes"/> limit); the path limit applies to the whole normalized path including the stream.
/// See <see cref="DfsStreamPath"/> to split or build stream paths.</para>
/// <para>All failures throw <see cref="ArgumentException"/> tagged with <see cref="DfsErrorCode.InvalidPath"/>
/// (see <see cref="DfsErrors.ToCode"/>). Methods taking a "normalizedPath" require input already returned by
/// <see cref="Normalize"/>.</para>
/// </remarks>
public static class DfsPath
{
    /// <summary>Maximum UTF-8 bytes per component (and per stream name) after normalization.</summary>
    public const int MaxComponentBytes = 255;

    /// <summary>Maximum UTF-8 bytes of a complete normalized path (including any stream suffix).</summary>
    public const int MaxPathBytes = 4096;

    /// <summary>The root path.</summary>
    public const string Root = "/";

    /// <summary>The only path separator.</summary>
    public const char Separator = '/';

    /// <summary>Separates the file name from a stream name (and a stream name from its type) in the final component.</summary>
    public const char StreamSeparator = ':';

    // NFC shrinks a string by at most a factor of 4 (one composed character from at most four code points), and each
    // UTF-16 code unit of the result needs at least one UTF-8 byte, so longer inputs can never fit the byte limits.
    private const int MaxInputCharsPerByte = 4;

    // Longest possible raw final component: name ':' stream ':' "$DATA".
    private const int MaxFinalComponentBytes = MaxComponentBytes + 1 + MaxComponentBytes + 1 + 5;

    /// <summary>Validates and NFC-normalizes <paramref name="path"/>.</summary>
    /// <exception cref="ArgumentNullException"><paramref name="path"/> is null.</exception>
    /// <exception cref="ArgumentException">The path is invalid (ParamName "path").</exception>
    public static string Normalize(string path)
    {
        ArgumentNullException.ThrowIfNull(path);
        string? error = TryNormalizeCore(path, out string? normalized);
        return error is null ? normalized! : throw InvalidPath(error, nameof(path));
    }

    /// <summary>Validates and NFC-normalizes <paramref name="path"/> without throwing.</summary>
    /// <param name="path">Candidate path.</param>
    /// <param name="normalized">The normalized path, or an empty string on failure.</param>
    /// <param name="error">A description of the violated rule, or null on success.</param>
    public static bool TryNormalize(string? path, out string normalized, out string? error)
    {
        error = TryNormalizeCore(path, out string? result);
        normalized = result ?? string.Empty;
        return error is null;
    }

    /// <summary>True when <paramref name="path"/> is valid and already normalized.</summary>
    public static bool IsNormalized(string? path)
        => TryNormalizeCore(path, out string? normalized) is null && string.Equals(normalized, path, StringComparison.Ordinal);

    /// <summary>True for the root path.</summary>
    public static bool IsRoot(string? path) => string.Equals(path, Root, StringComparison.Ordinal);

    /// <summary>
    /// Splits a normalized path into its components; the root yields an empty list. A stream path's last element is the
    /// stream-qualified name (<c>"/a/f:s"</c> → <c>["a", "f:s"]</c>).
    /// </summary>
    public static IReadOnlyList<string> Split(string normalizedPath)
    {
        RequireNormalized(normalizedPath, nameof(normalizedPath));
        if (normalizedPath.Length == 1)
            return [];
        return normalizedPath[1..].Split(Separator);
    }

    /// <summary>
    /// Appends a final component to a normalized parent path. The name is validated and NFC-normalized; it may be
    /// stream-qualified (<c>"f:s"</c>, <c>"f:s:$DATA"</c>, <c>"f::$DATA"</c>) and is then normalized as by
    /// <see cref="Normalize"/>, so <c>Combine(GetParent(p), GetName(p)) == p</c> for every normalized non-root path.
    /// </summary>
    /// <exception cref="ArgumentException">The parent is not normalized or addresses a stream, the name is invalid, or the
    /// result exceeds <see cref="MaxPathBytes"/>.</exception>
    public static string Combine(string normalizedParent, string name)
    {
        RequireNormalized(normalizedParent, nameof(normalizedParent));
        if (DfsStreamPath.HasStreamCore(normalizedParent))
            throw InvalidPath("A stream path cannot have children.", nameof(normalizedParent));
        ArgumentNullException.ThrowIfNull(name);
        string? error = TryNormalizeFinalName(name, isTopLevel: normalizedParent.Length == 1, out string? component);
        if (error is not null)
            throw InvalidPath(error, nameof(name));
        string combined = normalizedParent.Length == 1
            ? string.Concat(Root, component)
            : string.Concat(normalizedParent, "/", component);
        int bytes = Encoding.UTF8.GetByteCount(combined);
        if (bytes > MaxPathBytes)
            throw InvalidPath(PathTooLong(bytes), nameof(name));
        return combined;
    }

    /// <summary>
    /// Returns the parent directory of a normalized path ("/" for top-level entries). For a stream path this is the
    /// parent of its file (<c>"/a/f:s"</c> → <c>"/a"</c>).
    /// </summary>
    /// <exception cref="ArgumentException">The path is the root, or is not normalized.</exception>
    public static string GetParent(string normalizedPath)
    {
        RequireNormalized(normalizedPath, nameof(normalizedPath));
        if (normalizedPath.Length == 1)
            throw InvalidPath("The root path has no parent.", nameof(normalizedPath));
        int slash = normalizedPath.LastIndexOf(Separator);
        return slash == 0 ? Root : normalizedPath[..slash];
    }

    /// <summary>Returns the last component of a normalized path (empty for the root); stream-qualified for a stream path (<c>"f:s"</c>).</summary>
    public static string GetName(string normalizedPath)
    {
        RequireNormalized(normalizedPath, nameof(normalizedPath));
        if (normalizedPath.Length == 1)
            return string.Empty;
        return normalizedPath[(normalizedPath.LastIndexOf(Separator) + 1)..];
    }

    /// <summary>
    /// True when <paramref name="ancestor"/> equals <paramref name="path"/> or contains it (component-wise, ordinal). A
    /// file contains its streams (<c>"/a/f"</c> contains <c>"/a/f:s"</c>); a stream path contains only itself.
    /// </summary>
    public static bool IsAncestorOrSelf(string ancestor, string path)
    {
        RequireNormalized(ancestor, nameof(ancestor));
        RequireNormalized(path, nameof(path));
        if (ancestor.Length == 1)
            return true;
        if (string.Equals(ancestor, path, StringComparison.Ordinal))
            return true;
        if (path.Length <= ancestor.Length || !path.StartsWith(ancestor, StringComparison.Ordinal))
            return false;
        char next = path[ancestor.Length];
        return next == Separator || (next == StreamSeparator && !DfsStreamPath.HasStreamCore(ancestor));
    }

    /// <summary>
    /// Validates a single entry name (a file or directory name, or a stream name): rules are applied to its NFC form.
    /// Entry names never contain ':' (use <see cref="Combine"/> or <see cref="DfsStreamPath.Combine"/> for streams).
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is null.</exception>
    /// <exception cref="ArgumentException">The name is invalid (ParamName "name").</exception>
    public static void ValidateComponent(string name) => _ = NormalizeComponent(name);

    /// <summary>Validates a single entry name (see <see cref="ValidateComponent"/>) and returns its NFC form.</summary>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is null.</exception>
    /// <exception cref="ArgumentException">The name is invalid (ParamName "name").</exception>
    public static string NormalizeComponent(string name)
    {
        ArgumentNullException.ThrowIfNull(name);
        string? error = TryNormalizeComponentCore(name, out string? normalized);
        return error is null ? normalized! : throw InvalidPath(error, nameof(name));
    }

    /// <summary>Validates a single entry name and returns its NFC form without throwing.</summary>
    public static bool TryNormalizeComponent(string? name, out string normalized, out string? error)
    {
        error = TryNormalizeComponentCore(name, out string? result);
        normalized = result ?? string.Empty;
        return error is null;
    }

    /// <summary>Number of UTF-8 bytes of <paramref name="value"/> (well-formed UTF-16 assumed).</summary>
    public static int GetUtf8ByteCount(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return Encoding.UTF8.GetByteCount(value);
    }

    /// <summary>Creates an <see cref="ArgumentException"/> tagged with <see cref="DfsErrorCode.InvalidPath"/>.</summary>
    internal static ArgumentException InvalidPath(string message, string paramName)
    {
        var ex = new ArgumentException(message, paramName);
        ex.Data[DfsErrors.ErrorCodeDataKey] = DfsErrorCode.InvalidPath;
        ex.Data[DfsErrors.RetryableDataKey] = false;
        return ex;
    }

    /// <summary>Throws an InvalidPath <see cref="ArgumentException"/> unless <paramref name="path"/> is normalized.</summary>
    internal static void RequireNormalized(string path, string paramName)
    {
        ArgumentNullException.ThrowIfNull(path, paramName);
        string? error = TryNormalizeCore(path, out string? normalized);
        if (error is not null)
            throw InvalidPath(error, paramName);
        if (!string.Equals(normalized, path, StringComparison.Ordinal))
            throw InvalidPath("The path is not in normalized form; normalize it with DfsPath.Normalize first.", paramName);
    }

    /// <summary>Validates and NFC-normalizes a stream name (entry-name rules, and it must not start with '$').</summary>
    internal static string? TryNormalizeStreamNameCore(string? name, out string? normalized)
    {
        normalized = null;
        if (name is null)
            return "The stream name must not be null.";
        if (name.Length == 0)
            return "The stream name must not be empty.";
        string? error = TryNormalizeComponentCore(name, out string? nfc);
        if (error is not null)
            return error;
        error = CheckStreamName(nfc);
        if (error is not null)
            return error;
        normalized = nfc;
        return null;
    }

    private static string? TryNormalizeCore(string? path, out string? normalized)
    {
        normalized = null;
        if (path is null)
            return "The path must not be null.";
        if (path.Length == 0)
            return "The path must not be empty.";
        if (path[0] != Separator)
            return "The path must be absolute and start with '/'.";
        if (path.Length > MaxPathBytes * MaxInputCharsPerByte)
            return $"The path is too long; the limit is {MaxPathBytes} UTF-8 bytes after normalization.";

        string? error = CheckCharacters(path);
        if (error is not null)
            return error;

        string nfc = Nfc.Normalize(path);
        if (nfc.Length == 1)
        {
            normalized = Root;
            return null;
        }

        if (nfc[^1] == Separator)
            return "The path must not end with '/' (only the root may).";

        int start = 1;
        while (true)
        {
            int end = nfc.IndexOf(Separator, start);
            if (end < 0)
            {
                // Final component: it may carry stream syntax (ADS-01).
                error = TryNormalizeFinalComponent(nfc.AsSpan(start), isTopLevel: start == 1, out string? replacement);
                if (error is not null)
                    return error;
                if (replacement is not null)
                    nfc = string.Concat(nfc.AsSpan(0, start), replacement);
                break;
            }

            ReadOnlySpan<char> component = nfc.AsSpan(start, end - start);
            error = CheckComponent(component);
            if (error is not null)
                return error;
            if (component.Contains(StreamSeparator))
                return "A ':' stream separator is only allowed in the final path component.";
            start = end + 1;
        }

        int bytes = Encoding.UTF8.GetByteCount(nfc);
        if (bytes > MaxPathBytes)
            return PathTooLong(bytes);

        normalized = nfc;
        return null;
    }

    private static string? TryNormalizeComponentCore(string? name, out string? normalized)
    {
        normalized = null;
        if (name is null)
            return "The name must not be null.";
        if (name.Length == 0)
            return "The name must not be empty.";
        if (name.Length > MaxComponentBytes * MaxInputCharsPerByte)
            return $"The name is too long; the limit is {MaxComponentBytes} UTF-8 bytes after normalization.";
        if (name.Contains(Separator, StringComparison.Ordinal))
            return "The name must not contain '/'.";
        if (name.Contains(StreamSeparator, StringComparison.Ordinal))
            return "The name must not contain ':' (the stream separator).";

        string? error = CheckCharacters(name);
        if (error is not null)
            return error;

        string nfc = Nfc.Normalize(name);
        error = CheckComponent(nfc);
        if (error is not null)
            return error;

        normalized = nfc;
        return null;
    }

    // Validates a raw (not yet normalized) final component that may be stream-qualified; returns its normalized form.
    private static string? TryNormalizeFinalName(string name, bool isTopLevel, out string? normalized)
    {
        normalized = null;
        if (name.Length == 0)
            return "The name must not be empty.";
        if (name.Length > MaxFinalComponentBytes * MaxInputCharsPerByte)
            return $"The name is too long; the limit is {MaxComponentBytes} UTF-8 bytes per name after normalization.";
        if (name.Contains(Separator, StringComparison.Ordinal))
            return "The name must not contain '/'.";
        string? error = CheckCharacters(name);
        if (error is not null)
            return error;
        string nfc = Nfc.Normalize(name);
        error = TryNormalizeFinalComponent(nfc, isTopLevel, out string? replacement);
        if (error is not null)
            return error;
        normalized = replacement ?? nfc;
        return null;
    }

    // Validates an NFC final component. 'replacement' is the normalized component when it differs from the input
    // (a ':$DATA' suffix was removed), otherwise null.
    private static string? TryNormalizeFinalComponent(ReadOnlySpan<char> component, bool isTopLevel, out string? replacement)
    {
        replacement = null;
        int colon = component.IndexOf(StreamSeparator);
        if (colon < 0)
            return CheckComponent(component);

        ReadOnlySpan<char> name = component[..colon];
        ReadOnlySpan<char> stream = component[(colon + 1)..];
        bool hasType = false;
        int typeColon = stream.IndexOf(StreamSeparator);
        if (typeColon >= 0)
        {
            ReadOnlySpan<char> type = stream[(typeColon + 1)..];
            if (type.Contains(StreamSeparator))
                return "The final path component has more than two ':' separators; expected 'name:stream' or 'name:stream:$DATA'.";
            if (!type.SequenceEqual(DfsStreamPath.DataType))
                return $"Unsupported stream type '{type}' (only '{DfsStreamPath.DataType}' is allowed, case-sensitive).";
            stream = stream[..typeColon];
            hasType = true;
        }

        if (name.IsEmpty)
            return isTopLevel ? "The root directory has no streams." : "A stream path needs a non-empty file name before ':'.";
        string? error = CheckComponent(name);
        if (error is not null)
            return error;

        if (stream.IsEmpty)
        {
            if (!hasType)
                return "The stream name must not be empty (address the default stream as 'name' or 'name::$DATA').";
            replacement = name.ToString();   // "name::$DATA" is the default stream
            return null;
        }

        error = CheckComponent(stream) ?? CheckStreamName(stream);
        if (error is not null)
            return error;
        if (hasType)
            replacement = string.Concat(name, ":", stream);
        return null;
    }

    private static string? CheckStreamName(ReadOnlySpan<char> stream) =>
        !stream.IsEmpty && stream[0] == '$' ? "Stream names must not start with '$' (reserved for stream types)." : null;

    private static string? CheckCharacters(ReadOnlySpan<char> value)
    {
        if (value.Contains('\0'))
            return "The path contains a NUL character.";
        if (value.Contains('\\'))
            return "The path contains a backslash; '/' is the only separator.";
        if (DfsUnicode.IndexOfUnpairedSurrogate(value) >= 0)
            return "The path contains malformed Unicode (an unpaired surrogate).";
        return null;
    }

    private static string? CheckComponent(ReadOnlySpan<char> component)
    {
        if (component.IsEmpty)
            return "The path contains an empty component (repeated '/').";
        if (component is "." or "..")
            return "Path components '.' and '..' are not allowed.";
        int bytes = Encoding.UTF8.GetByteCount(component);
        if (bytes > MaxComponentBytes)
            return ComponentTooLong(bytes);
        return null;
    }

    private static string PathTooLong(int bytes)
        => $"The path is {bytes} UTF-8 bytes after normalization; the limit is {MaxPathBytes}.";

    private static string ComponentTooLong(int bytes)
        => $"A path component is {bytes} UTF-8 bytes after normalization; the limit is {MaxComponentBytes}.";
}
