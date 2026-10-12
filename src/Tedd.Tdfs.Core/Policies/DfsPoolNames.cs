namespace Tedd.Tdfs;

/// <summary>Storage pool naming rules (Addendum A, POOL-01).</summary>
/// <remarks>
/// A pool name is 1..<see cref="MaxLength"/> ASCII characters from <c>[A-Za-z0-9._-]</c> and starts with a letter or
/// digit. Names are case-sensitive and compared ordinally. <see cref="Default"/> always exists and cannot be created or
/// deleted.
/// </remarks>
public static class DfsPoolNames
{
    /// <summary>The implicit pool containing every live node not explicitly excluded from it.</summary>
    public const string Default = "default";

    /// <summary>Maximum length of a pool name.</summary>
    public const int MaxLength = 64;

    /// <summary>True when <paramref name="name"/> is a syntactically valid pool name.</summary>
    public static bool IsValid(string? name) => name is not null && GetError(name) is null;

    /// <summary>Validates a pool name.</summary>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is null.</exception>
    /// <exception cref="ArgumentException">The name is invalid.</exception>
    public static void Validate(string name, string paramName = "pool")
    {
        ArgumentNullException.ThrowIfNull(name, paramName);
        string? error = GetError(name);
        if (error is not null)
            throw new ArgumentException(error, paramName);
    }

    /// <summary>Returns why <paramref name="name"/> is invalid, or null when valid.</summary>
    public static string? GetError(string name)
    {
        ArgumentNullException.ThrowIfNull(name);
        if (name.Length == 0)
            return "A pool name must not be empty.";
        if (name.Length > MaxLength)
            return $"A pool name must be at most {MaxLength} characters (was {name.Length}).";
        if (!char.IsAsciiLetterOrDigit(name[0]))
            return $"A pool name must start with an ASCII letter or digit ('{name}').";
        foreach (char c in name)
        {
            if (!char.IsAsciiLetterOrDigit(c) && c is not ('.' or '_' or '-'))
                return $"A pool name may only contain ASCII letters, digits, '.', '_' and '-' ('{name}').";
        }

        return null;
    }
}
