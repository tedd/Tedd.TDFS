namespace Tedd.Tdfs;

/// <summary>
/// Injected source of randomness. Server and SDK code must take all randomness from an <see cref="IRandomSource"/>
/// so that simulations replay exactly from a seed.
/// </summary>
public interface IRandomSource
{
    /// <summary>Returns the next uniformly distributed 64-bit value.</summary>
    ulong NextUInt64();

    /// <summary>Returns a uniformly distributed value in <c>[0, maxExclusive)</c>; returns 0 when <paramref name="maxExclusive"/> is 0 or 1.</summary>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="maxExclusive"/> is negative.</exception>
    int Next(int maxExclusive);

    /// <summary>Fills <paramref name="destination"/> with random bytes.</summary>
    void Fill(Span<byte> destination);

    /// <summary>
    /// Returns an independent deterministic stream derived from this source's seed and <paramref name="purpose"/>.
    /// Forking does not consume values from this source.
    /// </summary>
    IRandomSource Fork(string purpose);
}
