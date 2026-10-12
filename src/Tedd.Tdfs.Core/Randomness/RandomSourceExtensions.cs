namespace Tedd.Tdfs;

/// <summary>Convenience draws on top of <see cref="IRandomSource"/>.</summary>
public static class RandomSourceExtensions
{
    /// <summary>Returns a random RFC 4122 version-4 GUID drawn from <paramref name="random"/>.</summary>
    public static Guid NextGuid(this IRandomSource random)
    {
        ArgumentNullException.ThrowIfNull(random);
        Span<byte> bytes = stackalloc byte[16];
        random.Fill(bytes);
        bytes[7] = (byte)((bytes[7] & 0x0F) | 0x40); // version 4 (Guid byte layout is little-endian for the first 3 fields)
        bytes[8] = (byte)((bytes[8] & 0x3F) | 0x80); // RFC 4122 variant
        return new Guid(bytes);
    }

    /// <summary>Returns a uniformly distributed double in <c>[0, 1)</c>.</summary>
    public static double NextDouble(this IRandomSource random)
    {
        ArgumentNullException.ThrowIfNull(random);
        return (random.NextUInt64() >> 11) * (1.0 / (1UL << 53));
    }

    /// <summary>Returns a uniformly distributed value in <c>[minInclusive, maxExclusive)</c>.</summary>
    public static int Next(this IRandomSource random, int minInclusive, int maxExclusive)
    {
        ArgumentNullException.ThrowIfNull(random);
        if (maxExclusive < minInclusive)
            throw new ArgumentOutOfRangeException(nameof(maxExclusive), "maxExclusive must be >= minInclusive.");
        ulong range = (ulong)((long)maxExclusive - minInclusive);
        return (int)(minInclusive + (long)BoundedUInt64(random, range));
    }

    /// <summary>Returns a uniformly distributed value in <c>[0, maxExclusive)</c> (0 when <paramref name="maxExclusive"/> is 0).</summary>
    public static ulong NextUInt64(this IRandomSource random, ulong maxExclusive)
    {
        ArgumentNullException.ThrowIfNull(random);
        return BoundedUInt64(random, maxExclusive);
    }

    /// <summary>Returns a new array of <paramref name="count"/> random bytes.</summary>
    public static byte[] NextBytes(this IRandomSource random, int count)
    {
        ArgumentNullException.ThrowIfNull(random);
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        var bytes = new byte[count];
        random.Fill(bytes);
        return bytes;
    }

    /// <summary>Shuffles <paramref name="list"/> in place (Fisher-Yates) using <paramref name="random"/>.</summary>
    public static void Shuffle<T>(this IRandomSource random, IList<T> list)
    {
        ArgumentNullException.ThrowIfNull(random);
        ArgumentNullException.ThrowIfNull(list);
        for (int i = list.Count - 1; i > 0; i--)
        {
            int j = random.Next(i + 1);
            (list[i], list[j]) = (list[j], list[i]);
        }
    }

    /// <summary>Unbiased bounded draw shared by <see cref="IRandomSource"/> implementations (Lemire's method).</summary>
    internal static int Bounded(IRandomSource random, int maxExclusive)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(maxExclusive);
        return maxExclusive <= 1 ? 0 : (int)BoundedUInt64(random, (ulong)maxExclusive);
    }

    private static ulong BoundedUInt64(IRandomSource random, ulong range)
    {
        if (range <= 1)
            return 0;
        ulong high = Math.BigMul(random.NextUInt64(), range, out ulong low);
        if (low < range)
        {
            ulong threshold = (0UL - range) % range;
            while (low < threshold)
                high = Math.BigMul(random.NextUInt64(), range, out low);
        }

        return high;
    }
}
