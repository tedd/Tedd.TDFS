using System.Buffers;
using System.Text;

namespace Tedd.Tdfs.Text;

/// <summary>
/// Self-contained Unicode Normalization Form C (UAX #15) over pinned Unicode Character Database tables.
/// </summary>
/// <remarks>
/// <see cref="string.Normalize()"/> is a no-op when the process runs with <c>InvariantGlobalization</c> (as every
/// Tedd.TDFS process does), and OS/ICU-backed normalization would make name identity depend on the platform's Unicode
/// version. This implementation is deterministic on every platform. Lone surrogates are passed through unchanged as
/// opaque starters (callers that require well-formed text must reject them first).
/// </remarks>
internal static partial class Nfc
{
    private const int SBase = 0xAC00;
    private const int LBase = 0x1100;
    private const int VBase = 0x1161;
    private const int TBase = 0x11A7;
    private const int LCount = 19;
    private const int VCount = 21;
    private const int TCount = 28;
    private const int NCount = VCount * TCount;
    private const int SCount = LCount * NCount;

    /// <summary>Strings made only of characters below this value are already NFC (combining class 0, NFC_QC=Yes).</summary>
    private const char FastPathLimit = '\u0300';

    /// <summary>No character below this value has a canonical decomposition.</summary>
    private const int FirstDecomposable = 0xC0;

    private const int StackThreshold = 256;

    /// <summary>Every code point that can be the second element of a canonical composition (sorted).</summary>
    private static readonly int[] CompositionSeconds = BuildCompositionSeconds();

    /// <summary>Returns the NFC form of <paramref name="value"/> (the same instance when already normalized).</summary>
    public static string Normalize(string value)
    {
        int i = 0;
        while (i < value.Length && value[i] < FastPathLimit)
            i++;
        if (i == value.Length || IsQuickYes(value, i))
            return value;

        string result = NormalizeSlow(value);
        return string.Equals(result, value, StringComparison.Ordinal) ? value : result;
    }

    /// <summary>True when <paramref name="value"/> is already in NFC.</summary>
    public static bool IsNormalized(string value) => ReferenceEquals(Normalize(value), value);

    /// <summary>Returns the canonical combining class of a code point.</summary>
    public static int GetCombiningClass(int codePoint)
    {
        if (codePoint < FastPathLimit)
            return 0;
        ReadOnlySpan<uint> data = CombiningClassData;
        int lo = 0;
        int hi = data.Length - 1;
        while (lo <= hi)
        {
            int mid = (lo + hi) >>> 1;
            int cp = (int)(data[mid] >> 8);
            if (cp == codePoint)
                return (int)(data[mid] & 0xFF);
            if (cp < codePoint)
                lo = mid + 1;
            else
                hi = mid - 1;
        }

        return 0;
    }

    /// <summary>
    /// True when every code point from <paramref name="start"/> on is a starter without a canonical decomposition
    /// (precomposed Hangul syllables excepted) that can never be the second element of a composition: such text is
    /// already NFC, so the common case (for example CJK names) needs no allocation.
    /// </summary>
    private static bool IsQuickYes(string value, int start)
    {
        for (int i = start; i < value.Length; i++)
        {
            int cp = value[i];
            if (cp < FastPathLimit)
                continue;
            if (char.IsHighSurrogate(value[i]) && i + 1 < value.Length && char.IsLowSurrogate(value[i + 1]))
            {
                cp = char.ConvertToUtf32(value[i], value[i + 1]);
                i++;
            }

            if ((uint)(cp - SBase) < SCount)
                continue; // precomposed syllable; a following T jamo is caught below as a composition second
            if (GetCombiningClass(cp) != 0 || FindDecomposition(cp) >= 0 || IsCompositionSecond(cp))
                return false;
        }

        return true;
    }

    private static bool IsCompositionSecond(int cp) => Array.BinarySearch(CompositionSeconds, cp) >= 0;

    private static int[] BuildCompositionSeconds()
    {
        var seconds = new SortedSet<int>();
        foreach (ulong key in CompositionKeys)
            seconds.Add((int)(key & 0x1FFFFF));
        for (int v = VBase; v < VBase + VCount; v++)
            seconds.Add(v);
        for (int t = TBase + 1; t < TBase + TCount; t++)
            seconds.Add(t);
        return [.. seconds];
    }

    private static string NormalizeSlow(string value)
    {
        // Full canonical decomposition expands each code point to at most 4 code points (Hangul: 3).
        int capacity = checked(value.Length * 4);
        int[]? rented = null;
        Span<int> cps = capacity <= StackThreshold ? stackalloc int[StackThreshold] : (rented = ArrayPool<int>.Shared.Rent(capacity));
        byte[]? rentedClasses = null;
        Span<byte> classes = capacity <= StackThreshold ? stackalloc byte[StackThreshold] : (rentedClasses = ArrayPool<byte>.Shared.Rent(capacity));
        try
        {
            int n = Decompose(value, cps);
            for (int k = 0; k < n; k++)
                classes[k] = (byte)GetCombiningClass(cps[k]);
            CanonicalOrder(cps[..n], classes[..n]);
            n = Compose(cps[..n], classes[..n]);
            return BuildString(cps[..n]);
        }
        finally
        {
            if (rented is not null)
                ArrayPool<int>.Shared.Return(rented);
            if (rentedClasses is not null)
                ArrayPool<byte>.Shared.Return(rentedClasses);
        }
    }

    private static int Decompose(string value, Span<int> output)
    {
        int n = 0;
        for (int i = 0; i < value.Length; i++)
        {
            int cp = value[i];
            if (char.IsHighSurrogate(value[i]) && i + 1 < value.Length && char.IsLowSurrogate(value[i + 1]))
            {
                cp = char.ConvertToUtf32(value[i], value[i + 1]);
                i++;
            }

            n += DecomposeCodePoint(cp, output[n..]);
        }

        return n;
    }

    private static int DecomposeCodePoint(int cp, Span<int> output)
    {
        if (cp < FirstDecomposable)
        {
            output[0] = cp;
            return 1;
        }

        int sIndex = cp - SBase;
        if ((uint)sIndex < SCount)
        {
            output[0] = LBase + (sIndex / NCount);
            output[1] = VBase + ((sIndex % NCount) / TCount);
            int t = sIndex % TCount;
            if (t == 0)
                return 2;
            output[2] = TBase + t;
            return 3;
        }

        int index = FindDecomposition(cp);
        if (index < 0)
        {
            output[0] = cp;
            return 1;
        }

        uint entry = DecompositionIndex[index];
        int offset = (int)(entry >> 4);
        int length = (int)(entry & 0xF);
        ReadOnlySpan<uint> pool = DecompositionPool.Slice(offset, length);
        for (int k = 0; k < length; k++)
            output[k] = (int)pool[k];
        return length;
    }

    private static int FindDecomposition(int cp)
    {
        ReadOnlySpan<uint> keys = DecompositionKeys;
        int lo = 0;
        int hi = keys.Length - 1;
        while (lo <= hi)
        {
            int mid = (lo + hi) >>> 1;
            uint key = keys[mid];
            if (key == (uint)cp)
                return mid;
            if (key < (uint)cp)
                lo = mid + 1;
            else
                hi = mid - 1;
        }

        return -1;
    }

    /// <summary>Canonical ordering: stable sort of each run of non-starters by combining class.</summary>
    private static void CanonicalOrder(Span<int> cps, Span<byte> classes)
    {
        for (int i = 1; i < cps.Length; i++)
        {
            byte cc = classes[i];
            if (cc == 0)
                continue;
            int j = i;
            while (j > 0 && classes[j - 1] > cc)
            {
                (cps[j - 1], cps[j]) = (cps[j], cps[j - 1]);
                (classes[j - 1], classes[j]) = (classes[j], classes[j - 1]);
                j--;
            }
        }
    }

    /// <summary>Canonical composition (UAX #15 §1.3); returns the new length.</summary>
    private static int Compose(Span<int> cps, ReadOnlySpan<byte> classes)
    {
        if (cps.Length == 0)
            return 0;

        int starterPos = 0;
        int starter = cps[0];
        int lastClass = classes[0] == 0 ? 0 : 256; // a leading non-starter can never be composed with
        int outPos = 1;
        for (int i = 1; i < cps.Length; i++)
        {
            int ch = cps[i];
            int chClass = classes[i];
            // Not blocked: adjacent to the starter, or every intervening mark has a lower combining class.
            if (lastClass == 0 || lastClass < chClass)
            {
                int composite = ComposePair(starter, ch);
                if (composite >= 0)
                {
                    cps[starterPos] = composite;
                    starter = composite;
                    continue;
                }
            }

            if (chClass == 0)
            {
                starterPos = outPos;
                starter = ch;
            }

            lastClass = chClass;
            cps[outPos++] = ch;
        }

        return outPos;
    }

    private static int ComposePair(int first, int second)
    {
        int lIndex = first - LBase;
        if ((uint)lIndex < LCount)
        {
            int vIndex = second - VBase;
            if ((uint)vIndex < VCount)
                return SBase + (((lIndex * VCount) + vIndex) * TCount);
            return -1;
        }

        int sIndex = first - SBase;
        if ((uint)sIndex < SCount && sIndex % TCount == 0)
        {
            int tIndex = second - TBase;
            if (tIndex > 0 && tIndex < TCount)
                return first + tIndex;
            return -1;
        }

        if (second < FastPathLimit)
            return -1; // no primary composite has a second element below U+0300

        ulong key = ((ulong)(uint)first << 21) | (uint)second;
        ReadOnlySpan<ulong> keys = CompositionKeys;
        int lo = 0;
        int hi = keys.Length - 1;
        while (lo <= hi)
        {
            int mid = (lo + hi) >>> 1;
            ulong k = keys[mid];
            if (k == key)
                return (int)CompositionValues[mid];
            if (k < key)
                lo = mid + 1;
            else
                hi = mid - 1;
        }

        return -1;
    }

    private static string BuildString(ReadOnlySpan<int> cps)
    {
        var sb = new StringBuilder(cps.Length + 8);
        foreach (int cp in cps)
        {
            if (cp is >= 0xD800 and <= 0xDFFF)
                sb.Append((char)cp); // lone surrogate passed through verbatim
            else if (cp < 0x10000)
                sb.Append((char)cp);
            else
                sb.Append(char.ConvertFromUtf32(cp));
        }

        return sb.ToString();
    }
}
