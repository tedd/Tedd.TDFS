using System.Buffers.Binary;
using System.Runtime.Intrinsics.X86;
using ArmCrc32 = System.Runtime.Intrinsics.Arm.Crc32;

namespace Tedd.Tdfs.Data;

/// <summary>
/// CRC-32C (Castagnoli, reflected polynomial 0x82F63B78). Uses SSE4.2 or ARMv8 CRC instructions when available and a
/// slicing-by-8 table otherwise; all paths produce identical results.
/// </summary>
internal static class Crc32CAlgorithm
{
    private const uint Polynomial = 0x82F63B78u;

    private static readonly uint[] Table = BuildTable();

    /// <summary>Continues a CRC-32C: <c>Append(Compute(a), b) == Compute(a + b)</c>; start with 0.</summary>
    public static uint Append(uint crc, ReadOnlySpan<byte> data) => ~Update(~crc, data);

    /// <summary>Updates a raw (pre-inverted) CRC state.</summary>
    internal static uint Update(uint state, ReadOnlySpan<byte> data)
    {
        if (Sse42.X64.IsSupported)
        {
            ulong s = state;
            while (data.Length >= 8)
            {
                s = Sse42.X64.Crc32(s, BinaryPrimitives.ReadUInt64LittleEndian(data));
                data = data[8..];
            }

            state = (uint)s;
            foreach (byte b in data)
                state = Sse42.Crc32(state, b);
            return state;
        }

        if (Sse42.IsSupported)
        {
            while (data.Length >= 4)
            {
                state = Sse42.Crc32(state, BinaryPrimitives.ReadUInt32LittleEndian(data));
                data = data[4..];
            }

            foreach (byte b in data)
                state = Sse42.Crc32(state, b);
            return state;
        }

        if (ArmCrc32.Arm64.IsSupported)
        {
            while (data.Length >= 8)
            {
                state = ArmCrc32.Arm64.ComputeCrc32C(state, BinaryPrimitives.ReadUInt64LittleEndian(data));
                data = data[8..];
            }

            foreach (byte b in data)
                state = ArmCrc32.ComputeCrc32C(state, b);
            return state;
        }

        if (ArmCrc32.IsSupported)
        {
            while (data.Length >= 4)
            {
                state = ArmCrc32.ComputeCrc32C(state, BinaryPrimitives.ReadUInt32LittleEndian(data));
                data = data[4..];
            }

            foreach (byte b in data)
                state = ArmCrc32.ComputeCrc32C(state, b);
            return state;
        }

        return UpdateSoftware(state, data);
    }

    /// <summary>Portable slicing-by-8 implementation (also used to cross-check the hardware paths in tests).</summary>
    internal static uint UpdateSoftware(uint state, ReadOnlySpan<byte> data)
    {
        uint[] t = Table;
        while (data.Length >= 8)
        {
            uint lo = BinaryPrimitives.ReadUInt32LittleEndian(data) ^ state;
            uint hi = BinaryPrimitives.ReadUInt32LittleEndian(data[4..]);
            state = t[(7 * 256) + (lo & 0xFF)] ^ t[(6 * 256) + ((lo >> 8) & 0xFF)] ^
                    t[(5 * 256) + ((lo >> 16) & 0xFF)] ^ t[(4 * 256) + (lo >> 24)] ^
                    t[(3 * 256) + (hi & 0xFF)] ^ t[(2 * 256) + ((hi >> 8) & 0xFF)] ^
                    t[(1 * 256) + ((hi >> 16) & 0xFF)] ^ t[hi >> 24];
            data = data[8..];
        }

        foreach (byte b in data)
            state = t[(state ^ b) & 0xFF] ^ (state >> 8);
        return state;
    }

    private static uint[] BuildTable()
    {
        var table = new uint[8 * 256];
        for (uint i = 0; i < 256; i++)
        {
            uint c = i;
            for (int k = 0; k < 8; k++)
                c = (c & 1) != 0 ? (c >> 1) ^ Polynomial : c >> 1;
            table[i] = c;
        }

        for (int slice = 1; slice < 8; slice++)
        {
            for (int i = 0; i < 256; i++)
            {
                uint prev = table[((slice - 1) * 256) + i];
                table[(slice * 256) + i] = (prev >> 8) ^ table[prev & 0xFF];
            }
        }

        return table;
    }
}
