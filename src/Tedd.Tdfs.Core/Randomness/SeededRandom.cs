using System.Buffers.Binary;
using System.IO.Hashing;
using System.Numerics;
using System.Text;

namespace Tedd.Tdfs;

/// <summary>
/// Fast deterministic random source (xoshiro256** seeded through SplitMix64). Identical seeds produce identical
/// streams on every platform. Not thread-safe; give each component its own instance via <see cref="Fork"/>.
/// </summary>
/// <remarks>
/// <see cref="Fork"/> derives the child seed from this instance's construction seed and a stable XxHash64 of the
/// UTF-8 purpose string, so forks do not depend on how many values were consumed. Forking the same purpose twice yields
/// identical streams; include a distinguishing suffix (for example an incarnation) when independence is required.
/// </remarks>
public sealed class SeededRandom : IRandomSource
{
    private ulong _s0;
    private ulong _s1;
    private ulong _s2;
    private ulong _s3;

    /// <summary>Creates a generator for <paramref name="seed"/>.</summary>
    public SeededRandom(ulong seed)
    {
        Seed = seed;
        ulong sm = seed;
        _s0 = SplitMix64(ref sm);
        _s1 = SplitMix64(ref sm);
        _s2 = SplitMix64(ref sm);
        _s3 = SplitMix64(ref sm);
        if ((_s0 | _s1 | _s2 | _s3) == 0)
            _s0 = 0x9E3779B97F4A7C15UL; // xoshiro must never have an all-zero state
    }

    /// <summary>The seed this generator was created with.</summary>
    public ulong Seed { get; }

    /// <inheritdoc />
    public ulong NextUInt64()
    {
        ulong result = BitOperations.RotateLeft(_s1 * 5, 7) * 9;
        ulong t = _s1 << 17;
        _s2 ^= _s0;
        _s3 ^= _s1;
        _s1 ^= _s2;
        _s0 ^= _s3;
        _s2 ^= t;
        _s3 = BitOperations.RotateLeft(_s3, 45);
        return result;
    }

    /// <inheritdoc />
    public int Next(int maxExclusive) => RandomSourceExtensions.Bounded(this, maxExclusive);

    /// <inheritdoc />
    public void Fill(Span<byte> destination)
    {
        while (destination.Length >= sizeof(ulong))
        {
            BinaryPrimitives.WriteUInt64LittleEndian(destination, NextUInt64());
            destination = destination[sizeof(ulong)..];
        }

        if (!destination.IsEmpty)
        {
            Span<byte> tail = stackalloc byte[sizeof(ulong)];
            BinaryPrimitives.WriteUInt64LittleEndian(tail, NextUInt64());
            tail[..destination.Length].CopyTo(destination);
        }
    }

    /// <inheritdoc />
    public IRandomSource Fork(string purpose) => new SeededRandom(DeriveSeed(Seed, purpose));

    /// <summary>Derives a child seed from <paramref name="seed"/> and a stable hash of <paramref name="purpose"/>.</summary>
    public static ulong DeriveSeed(ulong seed, string purpose)
    {
        ArgumentNullException.ThrowIfNull(purpose);
        byte[] bytes = Encoding.UTF8.GetBytes(purpose);
        ulong h = XxHash64.HashToUInt64(bytes, unchecked((long)seed));
        ulong sm = h ^ seed;
        return SplitMix64(ref sm);
    }

    private static ulong SplitMix64(ref ulong state)
    {
        ulong z = state += 0x9E3779B97F4A7C15UL;
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9UL;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBUL;
        return z ^ (z >> 31);
    }
}
