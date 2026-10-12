using System.Globalization;

namespace Tedd.Tdfs;

/// <summary>Shared strict parsing helpers for identifier string forms (culture-invariant, ordinal).</summary>
internal static class IdentifierFormat
{
    /// <summary>Parses 1..16 hexadecimal digits (either case) into a <see cref="ulong"/>.</summary>
    public static bool TryParseHex64(ReadOnlySpan<char> s, out ulong value)
    {
        value = 0;
        if (s.Length is < 1 or > 16 || !IsHex(s))
            return false;
        return ulong.TryParse(s, NumberStyles.AllowHexSpecifier, CultureInfo.InvariantCulture, out value);
    }

    /// <summary>Parses 1..20 decimal digits (no sign, no whitespace) into a <see cref="ulong"/>.</summary>
    public static bool TryParseDecimal64(ReadOnlySpan<char> s, out ulong value)
    {
        value = 0;
        if (s.Length is < 1 or > 20)
            return false;
        foreach (char c in s)
        {
            if (c is < '0' or > '9')
                return false;
        }

        return ulong.TryParse(s, NumberStyles.None, CultureInfo.InvariantCulture, out value);
    }

    /// <summary>Parses a GUID in the canonical "D" format (8-4-4-4-12 hex digits).</summary>
    public static bool TryParseGuid(ReadOnlySpan<char> s, out Guid value) => Guid.TryParseExact(s, "D", out value);

    /// <summary>True when every character is an ASCII hexadecimal digit.</summary>
    public static bool IsHex(ReadOnlySpan<char> s)
    {
        foreach (char c in s)
        {
            if (!char.IsAsciiHexDigit(c))
                return false;
        }

        return true;
    }

    /// <summary>Formats a GUID in canonical lowercase "D" form.</summary>
    public static string FormatGuid(Guid value) => value.ToString("D", CultureInfo.InvariantCulture);
}
