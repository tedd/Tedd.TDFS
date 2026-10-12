using System.Security.Cryptography;

namespace Tedd.Tdfs.Crypto;

/// <summary>HMAC-SHA256 helpers with constant-time verification.</summary>
internal static class HmacHelpers
{
    /// <summary>Size of an HMAC-SHA256 tag in bytes.</summary>
    public const int TagSize = 32;

    /// <summary>Computes HMAC-SHA256 of <paramref name="data"/>.</summary>
    public static byte[] Compute(ReadOnlySpan<byte> key, ReadOnlySpan<byte> data) => HMACSHA256.HashData(key, data);

    /// <summary>Computes HMAC-SHA256 of <paramref name="data"/> into <paramref name="destination"/> (at least <see cref="TagSize"/> bytes).</summary>
    public static void Compute(ReadOnlySpan<byte> key, ReadOnlySpan<byte> data, Span<byte> destination)
        => HMACSHA256.HashData(key, data, destination);

    /// <summary>Verifies an HMAC-SHA256 tag in constant time.</summary>
    public static bool Verify(ReadOnlySpan<byte> key, ReadOnlySpan<byte> data, ReadOnlySpan<byte> tag)
    {
        if (tag.Length != TagSize)
            return false;
        Span<byte> expected = stackalloc byte[TagSize];
        HMACSHA256.HashData(key, data, expected);
        return CryptographicOperations.FixedTimeEquals(expected, tag);
    }
}
