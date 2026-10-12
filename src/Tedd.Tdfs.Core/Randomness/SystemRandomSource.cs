using System.Buffers.Binary;
using System.Security.Cryptography;

namespace Tedd.Tdfs;

/// <summary>Cryptographically strong, non-deterministic random source backed by <see cref="RandomNumberGenerator"/>. Thread-safe.</summary>
public sealed class SystemRandomSource : IRandomSource
{
    /// <summary>The shared instance.</summary>
    public static readonly SystemRandomSource Instance = new();

    private SystemRandomSource()
    {
    }

    /// <inheritdoc />
    public ulong NextUInt64()
    {
        Span<byte> buffer = stackalloc byte[sizeof(ulong)];
        RandomNumberGenerator.Fill(buffer);
        return BinaryPrimitives.ReadUInt64LittleEndian(buffer);
    }

    /// <inheritdoc />
    public int Next(int maxExclusive)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(maxExclusive);
        return maxExclusive <= 1 ? 0 : RandomNumberGenerator.GetInt32(maxExclusive);
    }

    /// <inheritdoc />
    public void Fill(Span<byte> destination) => RandomNumberGenerator.Fill(destination);

    /// <summary>Returns this instance: every draw is already independent.</summary>
    public IRandomSource Fork(string purpose)
    {
        ArgumentNullException.ThrowIfNull(purpose);
        return this;
    }
}
