using System.Globalization;

namespace Tedd.Tdfs;

/// <summary>
/// Stable identity of a mutating operation: a server-issued per-volume <paramref name="Session"/> and a
/// client-allocated, monotonic <paramref name="Sequence"/>. String form: <c>op:{Session:x}-{Sequence:x}</c>.
/// </summary>
/// <param name="Session">Server-issued session number.</param>
/// <param name="Sequence">Client-allocated sequence within the session.</param>
public readonly record struct DfsOperationId(ulong Session, ulong Sequence)
{
    /// <summary>String prefix of the canonical form.</summary>
    public const string Prefix = "op:";

    /// <summary>True when both session and sequence are zero (no operation id).</summary>
    public bool IsEmpty => Session == 0 && Sequence == 0;

    /// <summary>Returns <c>op:{Session:x}-{Sequence:x}</c> (lowercase hex, no leading zeros).</summary>
    public override string ToString() => Format(Prefix, Session, Sequence);

    /// <summary>Parses the form produced by <see cref="ToString"/>.</summary>
    /// <exception cref="FormatException">The string is not a valid operation id.</exception>
    public static DfsOperationId Parse(string s)
    {
        ArgumentNullException.ThrowIfNull(s);
        return TryParse(s, out var id) ? id : throw new FormatException("Invalid operation id; expected 'op:{session:x}-{sequence:x}'.");
    }

    /// <summary>Tries to parse the form produced by <see cref="ToString"/>.</summary>
    public static bool TryParse(string? s, out DfsOperationId id)
    {
        id = default;
        return s is not null && TryParseBody(s.AsSpan(), Prefix, out id);
    }

    internal static string Format(string prefix, ulong session, ulong sequence)
        => string.Concat(prefix, session.ToString("x", CultureInfo.InvariantCulture), "-", sequence.ToString("x", CultureInfo.InvariantCulture));

    internal static bool TryParseBody(ReadOnlySpan<char> s, string prefix, out DfsOperationId id)
    {
        id = default;
        if (!s.StartsWith(prefix, StringComparison.Ordinal))
            return false;
        s = s[prefix.Length..];
        int dash = s.IndexOf('-');
        if (dash < 0)
            return false;
        if (!IdentifierFormat.TryParseHex64(s[..dash], out ulong session) ||
            !IdentifierFormat.TryParseHex64(s[(dash + 1)..], out ulong sequence))
            return false;
        id = new DfsOperationId(session, sequence);
        return true;
    }
}
