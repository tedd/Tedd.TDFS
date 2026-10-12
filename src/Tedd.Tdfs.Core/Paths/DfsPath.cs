using System.Text;
using Tedd.Tdfs.Text;

namespace Tedd.Tdfs;

/// <summary>
/// Volume path validation and normalization (FSD NS-01..NS-03). Paths are absolute and volume-relative, use '/' as
/// the only separator, are case-sensitive and compared ordinally, and are normalized to Unicode NFC.
/// </summary>
/// <remarks>
/// Rules: absolute; root is "/"; no empty components, no trailing '/' except the root, no "." or ".." components,
/// no NUL, no backslash, no unpaired surrogates; NFC normalization (platform-independent, see
/// <see cref="DfsUnicode"/>); each component at most <see cref="MaxComponentBytes"/> and the full path at most
/// <see cref="MaxPathBytes"/> UTF-8 bytes <b>after</b> normalization. Percent sequences are literal characters and are
/// never decoded. All failures throw <see cref="ArgumentException"/> tagged with <see cref="DfsErrorCode.InvalidPath"/>
/// (see <see cref="DfsErrors.ToCode"/>). Methods taking a "normalizedPath" require input already returned by
/// <see cref="Normalize"/>.
/// </remarks>
public static class DfsPath
{
    /// <summary>Maximum UTF-8 bytes per component after normalization.</summary>
    public const int MaxComponentBytes = 255;

    /// <summary>Maximum UTF-8 bytes of a complete normalized path.</summary>
    public const int MaxPathBytes = 4096;

    /// <summary>The root path.</summary>
    public const string Root = "/";

    /// <summary>The only path separator.</summary>
    public const char Separator = '/';

    // NFC shrinks a string by at most a factor of 4 (one composed character from at most four code points), and each
    // UTF-16 code unit of the result needs at least one UTF-8 byte, so longer inputs can never fit the byte limits.
    private const int MaxInputCharsPerByte = 4;

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

    /// <summary>Splits a normalized path into its components; the root yields an empty list.</summary>
    public static IReadOnlyList<string> Split(string normalizedPath)
    {
        RequireNormalized(normalizedPath, nameof(normalizedPath));
        if (normalizedPath.Length == 1)
            return [];
        return normalizedPath[1..].Split(Separator);
    }

    /// <summary>Appends a component to a normalized parent path. The name is validated and NFC-normalized.</summary>
    /// <exception cref="ArgumentException">The parent is not normalized, the name is invalid, or the result exceeds <see cref="MaxPathBytes"/>.</exception>
    public static string Combine(string normalizedParent, string name)
    {
        RequireNormalized(normalizedParent, nameof(normalizedParent));
        string component = NormalizeComponent(name);
        string combined = normalizedParent.Length == 1
            ? string.Concat(Root, component)
            : string.Concat(normalizedParent, "/", component);
        int bytes = Encoding.UTF8.GetByteCount(combined);
        if (bytes > MaxPathBytes)
            throw InvalidPath(PathTooLong(bytes), nameof(name));
        return combined;
    }

    /// <summary>Returns the parent of a normalized path ("/" for top-level entries).</summary>
    /// <exception cref="ArgumentException">The path is the root, or is not normalized.</exception>
    public static string GetParent(string normalizedPath)
    {
        RequireNormalized(normalizedPath, nameof(normalizedPath));
        if (normalizedPath.Length == 1)
            throw InvalidPath("The root path has no parent.", nameof(normalizedPath));
        int slash = normalizedPath.LastIndexOf(Separator);
        return slash == 0 ? Root : normalizedPath[..slash];
    }

    /// <summary>Returns the last component of a normalized path (empty for the root).</summary>
    public static string GetName(string normalizedPath)
    {
        RequireNormalized(normalizedPath, nameof(normalizedPath));
        if (normalizedPath.Length == 1)
            return string.Empty;
        return normalizedPath[(normalizedPath.LastIndexOf(Separator) + 1)..];
    }

    /// <summary>True when <paramref name="ancestor"/> equals <paramref name="path"/> or contains it (component-wise, ordinal).</summary>
    public static bool IsAncestorOrSelf(string ancestor, string path)
    {
        RequireNormalized(ancestor, nameof(ancestor));
        RequireNormalized(path, nameof(path));
        if (ancestor.Length == 1)
            return true;
        if (string.Equals(ancestor, path, StringComparison.Ordinal))
            return true;
        return path.Length > ancestor.Length
            && path[ancestor.Length] == Separator
            && path.StartsWith(ancestor, StringComparison.Ordinal);
    }

    /// <summary>Validates a single name: rules are applied to its NFC form.</summary>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is null.</exception>
    /// <exception cref="ArgumentException">The name is invalid (ParamName "name").</exception>
    public static void ValidateComponent(string name) => _ = NormalizeComponent(name);

    /// <summary>Validates a single name and returns its NFC form.</summary>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is null.</exception>
    /// <exception cref="ArgumentException">The name is invalid (ParamName "name").</exception>
    public static string NormalizeComponent(string name)
    {
        ArgumentNullException.ThrowIfNull(name);
        string? error = TryNormalizeComponentCore(name, out string? normalized);
        return error is null ? normalized! : throw InvalidPath(error, nameof(name));
    }

    /// <summary>Validates a single name and returns its NFC form without throwing.</summary>
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

    private static void RequireNormalized(string path, string paramName)
    {
        ArgumentNullException.ThrowIfNull(path, paramName);
        string? error = TryNormalizeCore(path, out string? normalized);
        if (error is not null)
            throw InvalidPath(error, paramName);
        if (!string.Equals(normalized, path, StringComparison.Ordinal))
            throw InvalidPath("The path is not in normalized (NFC) form; normalize it with DfsPath.Normalize first.", paramName);
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
                end = nfc.Length;
            error = CheckComponent(nfc.AsSpan(start, end - start));
            if (error is not null)
                return error;
            if (end == nfc.Length)
                break;
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
