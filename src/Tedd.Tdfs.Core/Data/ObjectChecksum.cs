using System.IO.Hashing;

namespace Tedd.Tdfs.Data;

/// <summary>Checksums used for stored and transferred data.</summary>
public static class ObjectChecksum
{
    /// <summary>Object content checksum: XxHash128 (seed 0) of the plaintext.</summary>
    public static UInt128 Compute(ReadOnlySpan<byte> data) => XxHash128.HashToUInt128(data);

    /// <summary>CRC-32C (Castagnoli) of <paramref name="data"/>, as used for record headers and journal records.</summary>
    public static uint Crc32C(ReadOnlySpan<byte> data) => Crc32CAlgorithm.Append(0, data);

    /// <summary>Continues a CRC-32C over more data: <c>AppendCrc32C(Crc32C(a), b) == Crc32C(a + b)</c>.</summary>
    public static uint AppendCrc32C(uint crc, ReadOnlySpan<byte> data) => Crc32CAlgorithm.Append(crc, data);
}
