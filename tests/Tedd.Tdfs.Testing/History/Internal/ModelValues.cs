using System.Security.Cryptography;

namespace Tedd.Tdfs.Testing;

/// <summary>Conversions and canonical keys shared by the sequential models.</summary>
internal static class ModelValues
{
    /// <summary>Inline (hex) keys up to this many bytes; longer values are keyed by length + SHA-256.</summary>
    private const int InlineKeyBytes = 64;

    /// <summary>Converts a recorded byte payload; null stays null.</summary>
    /// <exception cref="ArgumentException">The value is not a byte payload.</exception>
    public static byte[]? ToBytes(object? value, HistoryOp op, string role) => value switch
    {
        null => null,
        byte[] bytes => bytes,
        ReadOnlyMemory<byte> rom => rom.ToArray(),
        Memory<byte> mem => mem.ToArray(),
        ArraySegment<byte> segment => segment.ToArray(),
        _ => throw new ArgumentException(
            $"Operation #{op.Id} ({op.Kind}): {role} must be a byte payload (byte[]/ReadOnlyMemory<byte>), not {value.GetType().Name}."),
    };

    /// <summary>A collision-resistant canonical key of a byte payload.</summary>
    public static string BytesKey(ReadOnlySpan<byte> bytes) =>
        bytes.Length <= InlineKeyBytes
            ? "x" + Convert.ToHexString(bytes)
            : "h" + bytes.Length.ToString(System.Globalization.CultureInfo.InvariantCulture) + ":" + Convert.ToHexString(SHA256.HashData(bytes));
}
