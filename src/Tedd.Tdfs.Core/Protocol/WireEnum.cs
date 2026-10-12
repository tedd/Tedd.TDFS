using System.Runtime.CompilerServices;

namespace Tedd.Tdfs.Protocol;

/// <summary>
/// Allocation-free conversion between an enum and the 64-bit integer carried on the wire. Signed underlying types are
/// sign-extended; unsigned ones are zero-extended (a <see cref="ulong"/>-based enum round-trips through an unchecked
/// reinterpretation).
/// </summary>
/// <typeparam name="TEnum">The enum type.</typeparam>
internal static class WireEnum<TEnum> where TEnum : struct, Enum
{
    private static readonly TypeCode Code = Type.GetTypeCode(Enum.GetUnderlyingType(typeof(TEnum)));

    /// <summary>Returns the underlying value of <paramref name="value"/>.</summary>
    public static long ToInt64(TEnum value) => Code switch
    {
        TypeCode.SByte => Unsafe.As<TEnum, sbyte>(ref value),
        TypeCode.Byte => Unsafe.As<TEnum, byte>(ref value),
        TypeCode.Int16 => Unsafe.As<TEnum, short>(ref value),
        TypeCode.UInt16 => Unsafe.As<TEnum, ushort>(ref value),
        TypeCode.Int32 => Unsafe.As<TEnum, int>(ref value),
        TypeCode.UInt32 => Unsafe.As<TEnum, uint>(ref value),
        TypeCode.Int64 => Unsafe.As<TEnum, long>(ref value),
        TypeCode.UInt64 => unchecked((long)Unsafe.As<TEnum, ulong>(ref value)),
        _ => throw new NotSupportedException($"Unsupported enum underlying type {Code}."),
    };

    /// <summary>Converts <paramref name="raw"/> to <typeparamref name="TEnum"/> when it fits the underlying type.</summary>
    public static bool TryFromInt64(long raw, out TEnum value)
    {
        value = default;
        switch (Code)
        {
            case TypeCode.SByte when raw is >= sbyte.MinValue and <= sbyte.MaxValue:
            {
                sbyte v = (sbyte)raw;
                value = Unsafe.As<sbyte, TEnum>(ref v);
                return true;
            }
            case TypeCode.Byte when raw is >= byte.MinValue and <= byte.MaxValue:
            {
                byte v = (byte)raw;
                value = Unsafe.As<byte, TEnum>(ref v);
                return true;
            }
            case TypeCode.Int16 when raw is >= short.MinValue and <= short.MaxValue:
            {
                short v = (short)raw;
                value = Unsafe.As<short, TEnum>(ref v);
                return true;
            }
            case TypeCode.UInt16 when raw is >= ushort.MinValue and <= ushort.MaxValue:
            {
                ushort v = (ushort)raw;
                value = Unsafe.As<ushort, TEnum>(ref v);
                return true;
            }
            case TypeCode.Int32 when raw is >= int.MinValue and <= int.MaxValue:
            {
                int v = (int)raw;
                value = Unsafe.As<int, TEnum>(ref v);
                return true;
            }
            case TypeCode.UInt32 when raw is >= uint.MinValue and <= uint.MaxValue:
            {
                uint v = (uint)raw;
                value = Unsafe.As<uint, TEnum>(ref v);
                return true;
            }
            case TypeCode.Int64:
            {
                value = Unsafe.As<long, TEnum>(ref raw);
                return true;
            }
            case TypeCode.UInt64:
            {
                ulong v = unchecked((ulong)raw);
                value = Unsafe.As<ulong, TEnum>(ref v);
                return true;
            }
            default:
                return false;
        }
    }
}
