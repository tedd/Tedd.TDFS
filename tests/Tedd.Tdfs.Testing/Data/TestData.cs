using System.Text;

namespace Tedd.Tdfs.Testing;

/// <summary>Deterministic test data generators (identical output for identical arguments on every platform).</summary>
public static class TestData
{
    // BMP characters by UTF-8 width; all precomposed (NFC-stable), none are controls, separators, BOM or surrogates.
    private const string Ascii = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 .,-_;:!?()[]{}+=*&%$#@~^'\"<>/|";
    private const string TwoByte = "æøåÆØÅéèêëüöäßñçÇ©®±µ¶¿ÐÞþðŁłŒœŠšŽžαβγδεζηθλμπσωЖЯжя";
    private const string ThreeByte = "€‰™←↑→↓∀∂∃∅∇∈∑√∞≈≠≤≥中文日本語한국어ひらがなカタカナ";

    /// <summary>Deterministic incompressible bytes for <paramref name="seed"/>.</summary>
    public static byte[] Bytes(ulong seed, int length)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        var data = new byte[length];
        new SeededRandom(seed).Fill(data);
        return data;
    }

    /// <summary>Maximally compressible bytes (every byte equals <paramref name="value"/>).</summary>
    public static byte[] Compressible(int length, byte value = 0x41)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        var data = new byte[length];
        data.AsSpan().Fill(value);
        return data;
    }

    /// <summary>
    /// Deterministic valid text of exactly <paramref name="chars"/> UTF-16 code units without line breaks. With
    /// <paramref name="multibyte"/> it mixes 1-, 2-, 3- and 4-byte UTF-8 sequences (surrogate pairs are never split).
    /// </summary>
    public static string Utf8Text(ulong seed, int chars, bool multibyte = true)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(chars);
        var random = new SeededRandom(seed);
        var sb = new StringBuilder(chars);
        while (sb.Length < chars)
        {
            int remaining = chars - sb.Length;
            int kind = multibyte ? random.Next(4) : 0;
            switch (kind)
            {
                case 1:
                    sb.Append(TwoByte[random.Next(TwoByte.Length)]);
                    break;
                case 2:
                    sb.Append(ThreeByte[random.Next(ThreeByte.Length)]);
                    break;
                case 3 when remaining >= 2:
                    // Supplementary planes: emoji U+1F300..U+1F5FF and CJK Extension B U+20000..U+200FF.
                    int codePoint = random.Next(2) == 0 ? 0x1F300 + random.Next(0x300) : 0x20000 + random.Next(0x100);
                    sb.Append(char.ConvertFromUtf32(codePoint));
                    break;
                default:
                    sb.Append(Ascii[random.Next(Ascii.Length)]);
                    break;
            }
        }

        return sb.ToString();
    }

    /// <summary>
    /// Sorted distinct nonnegative boundary lengths: 0, 1, t-1, t, t+1 (inline threshold), c-1, c, c+1 (chunk size),
    /// b-1, b, b+1 (buffer size) and 2c-1, 2c, 2c+1.
    /// </summary>
    public static IReadOnlyList<long> BoundaryLengths(int inlineThreshold, int chunkSize, int bufferSize)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(inlineThreshold);
        ArgumentOutOfRangeException.ThrowIfNegative(chunkSize);
        ArgumentOutOfRangeException.ThrowIfNegative(bufferSize);
        var set = new SortedSet<long> { 0, 1 };
        foreach (long pivot in new[] { (long)inlineThreshold, chunkSize, bufferSize, 2L * chunkSize })
        {
            for (long delta = -1; delta <= 1; delta++)
            {
                long value = pivot + delta;
                if (value >= 0)
                    set.Add(value);
            }
        }

        return set.ToArray();
    }
}
