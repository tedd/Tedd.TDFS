using Tedd.Tdfs.Text;

namespace Tedd.Tdfs;

/// <summary>
/// Platform-independent Unicode helpers used for name identity. Use these instead of
/// <see cref="string.Normalize()"/>, which is a no-op when <c>InvariantGlobalization</c> is enabled.
/// </summary>
public static class DfsUnicode
{
    /// <summary>Version of the Unicode Character Database pinned for NFC normalization.</summary>
    public const string UnicodeVersion = Nfc.UnicodeVersion;

    /// <summary>Returns the NFC (canonical composition) form of <paramref name="value"/>; lone surrogates pass through unchanged.</summary>
    public static string ToNfc(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return Nfc.Normalize(value);
    }

    /// <summary>True when <paramref name="value"/> is already in NFC.</summary>
    public static bool IsNfc(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return Nfc.IsNormalized(value);
    }

    /// <summary>Returns the canonical combining class of a Unicode scalar value.</summary>
    public static int GetCanonicalCombiningClass(int codePoint) => Nfc.GetCombiningClass(codePoint);

    /// <summary>Returns the index of the first unpaired surrogate in <paramref name="value"/>, or -1 when the text is well-formed UTF-16.</summary>
    public static int IndexOfUnpairedSurrogate(ReadOnlySpan<char> value)
    {
        for (int i = 0; i < value.Length; i++)
        {
            char c = value[i];
            if (char.IsHighSurrogate(c))
            {
                if (i + 1 < value.Length && char.IsLowSurrogate(value[i + 1]))
                {
                    i++;
                    continue;
                }

                return i;
            }

            if (char.IsLowSurrogate(c))
                return i;
        }

        return -1;
    }
}
