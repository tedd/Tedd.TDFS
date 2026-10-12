using System.Buffers.Binary;
using System.Collections.Frozen;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Unicode;

namespace Tedd.Tdfs.Protocol;

/// <summary>
/// Strict decoder for the Tedd.TDFS wire format. Every malformed input — truncation, varints longer than 10 bytes or
/// overflowing 64 bits, non-minimal varints, invalid booleans, lengths beyond <see cref="WireLimits"/> or the remaining
/// input, invalid UTF-8, malformed or duplicate extensions, trailing bytes — throws <see cref="DfsProtocolException"/>
/// with <see cref="DfsErrorCode.ProtocolError"/>. An unknown required extension throws
/// <see cref="DfsProtocolException"/> with <see cref="DfsErrorCode.Unsupported"/>.
/// </summary>
public ref struct WireReader
{
    /// <summary>Extension flag bit marking a required extension; all other flag bits are reserved and must be zero.</summary>
    internal const byte ExtensionRequiredFlag = 1;

    private const int MaxVarIntBytes = 10;

    private readonly ReadOnlySpan<byte> _input;
    private readonly WireLimits _limits;
    private int _position;

    /// <summary>Creates a reader over <paramref name="input"/> with the given limits (null means <see cref="WireLimits.Default"/>).</summary>
    public WireReader(ReadOnlySpan<byte> input, WireLimits limits)
    {
        _input = input;
        _limits = limits ?? WireLimits.Default;
        _position = 0;
    }

    /// <summary>Bytes not yet consumed.</summary>
    public readonly int Remaining => _input.Length - _position;

    /// <summary>Bytes consumed so far.</summary>
    public readonly int Position => _position;

    /// <summary>The limits in effect.</summary>
    public readonly WireLimits Limits => _limits ?? WireLimits.Default;

    /// <summary>Reads one byte.</summary>
    public byte ReadByte()
    {
        if (_position >= _input.Length)
            ThrowTruncated();
        return _input[_position++];
    }

    /// <summary>Reads a boolean; only 0 and 1 are valid.</summary>
    public bool ReadBool()
    {
        byte b = ReadByte();
        return b switch
        {
            0 => false,
            1 => true,
            _ => throw Malformed($"Invalid boolean value {b}."),
        };
    }

    /// <summary>Reads a little-endian 16-bit integer.</summary>
    public ushort ReadUInt16() => BinaryPrimitives.ReadUInt16LittleEndian(ReadSpan(sizeof(ushort)));

    /// <summary>Reads a little-endian 32-bit integer.</summary>
    public uint ReadUInt32() => BinaryPrimitives.ReadUInt32LittleEndian(ReadSpan(sizeof(uint)));

    /// <summary>Reads a little-endian signed 32-bit integer.</summary>
    public int ReadInt32() => BinaryPrimitives.ReadInt32LittleEndian(ReadSpan(sizeof(int)));

    /// <summary>Reads a little-endian 64-bit integer.</summary>
    public ulong ReadUInt64() => BinaryPrimitives.ReadUInt64LittleEndian(ReadSpan(sizeof(ulong)));

    /// <summary>Reads a little-endian signed 64-bit integer.</summary>
    public long ReadInt64() => BinaryPrimitives.ReadInt64LittleEndian(ReadSpan(sizeof(long)));

    /// <summary>Reads an unsigned LEB128 varint (at most 10 bytes, minimal encoding, no 64-bit overflow).</summary>
    public ulong ReadVarUInt()
    {
        ulong result = 0;
        int shift = 0;
        for (int i = 0; i < MaxVarIntBytes; i++)
        {
            if (_position >= _input.Length)
                throw Malformed("Truncated varint.");
            byte b = _input[_position++];
            if (i == MaxVarIntBytes - 1)
            {
                if ((b & 0x80) != 0)
                    throw Malformed("Varint longer than 10 bytes.");
                if (b > 1)
                    throw Malformed("Varint overflows 64 bits.");
            }

            result |= (ulong)(b & 0x7F) << shift;
            if ((b & 0x80) == 0)
            {
                if (b == 0 && i > 0)
                    throw Malformed("Non-minimal varint encoding.");
                return result;
            }

            shift += 7;
        }

        throw Malformed("Varint longer than 10 bytes.");
    }

    /// <summary>Reads a zigzag-encoded signed varint.</summary>
    public long ReadVarInt()
    {
        ulong u = ReadVarUInt();
        return unchecked((long)(u >> 1) ^ -(long)(u & 1));
    }

    /// <summary>Reads a varuint that must fit in a non-negative <see cref="int"/>.</summary>
    public int ReadVarInt32NonNegative()
    {
        ulong v = ReadVarUInt();
        if (v > int.MaxValue)
            throw Malformed($"Value {v} exceeds the 32-bit range.");
        return (int)v;
    }

    /// <summary>Reads a varuint that must fit in a <see cref="uint"/>.</summary>
    public uint ReadVarUInt32()
    {
        ulong v = ReadVarUInt();
        if (v > uint.MaxValue)
            throw Malformed($"Value {v} exceeds the 32-bit range.");
        return (uint)v;
    }

    /// <summary>Reads a GUID (16 bytes, standard little-endian layout).</summary>
    public Guid ReadGuid() => new(ReadSpan(16), bigEndian: false);

    /// <summary>Reads a 128-bit little-endian integer.</summary>
    public UInt128 ReadUInt128() => BinaryPrimitives.ReadUInt128LittleEndian(ReadSpan(16));

    /// <summary>Reads a varuint-length-prefixed byte array (bounded by <see cref="WireLimits.MaxBytesLength"/>); the result aliases the input.</summary>
    public ReadOnlySpan<byte> ReadBytes()
    {
        ulong length = ReadVarUInt();
        if (length > (ulong)Math.Max(0, Limits.MaxBytesLength))
            throw Malformed($"Byte field length {length} exceeds the limit of {Limits.MaxBytesLength}.");
        if (length > (ulong)Remaining)
            throw Malformed($"Byte field length {length} exceeds the {Remaining} remaining bytes.");
        return ReadSpan((int)length);
    }

    /// <summary>Reads a byte array field into a new array.</summary>
    public byte[] ReadBytesToArray() => ReadBytes().ToArray();

    /// <summary>Reads a varuint-length-prefixed, strictly validated UTF-8 string (bounded by <see cref="WireLimits.MaxStringBytes"/>).</summary>
    public string ReadString()
    {
        ulong length = ReadVarUInt();
        if (length > (ulong)Math.Max(0, Limits.MaxStringBytes))
            throw Malformed($"String length {length} exceeds the limit of {Limits.MaxStringBytes}.");
        if (length > (ulong)Remaining)
            throw Malformed($"String length {length} exceeds the {Remaining} remaining bytes.");
        ReadOnlySpan<byte> bytes = ReadSpan((int)length);
        if (!Utf8.IsValid(bytes))
            throw Malformed("String is not valid UTF-8.");
        return Encoding.UTF8.GetString(bytes);
    }

    /// <summary>Reads a presence flag followed by a string when present.</summary>
    public string? ReadOptionalString() => ReadBool() ? ReadString() : null;

    /// <summary>
    /// Reads a collection count (bounded by <see cref="WireLimits.MaxCollectionCount"/>) and checks that
    /// <c>count * minElementBytes</c> bytes remain, so a hostile count cannot force a large allocation.
    /// </summary>
    public int ReadCount(int minElementBytes = 1)
    {
        ulong count = ReadVarUInt();
        if (count > (ulong)Math.Max(0, Limits.MaxCollectionCount))
            throw Malformed($"Collection count {count} exceeds the limit of {Limits.MaxCollectionCount}.");
        if (minElementBytes > 0 && count * (ulong)minElementBytes > (ulong)Remaining)
            throw Malformed($"Collection count {count} cannot fit in the {Remaining} remaining bytes.");
        return (int)count;
    }

    /// <summary>Reads a <see cref="DfsOperationId"/> (two varuints).</summary>
    public DfsOperationId ReadOperationId()
    {
        ulong session = ReadVarUInt();
        ulong sequence = ReadVarUInt();
        return new DfsOperationId(session, sequence);
    }

    /// <summary>Reads a presence flag followed by an operation id when present.</summary>
    public DfsOperationId? ReadOptionalOperationId() => ReadBool() ? ReadOperationId() : null;

    /// <summary>Reads a <see cref="CommitToken"/>.</summary>
    public CommitToken ReadCommitToken()
    {
        Guid cluster = ReadGuid();
        Guid volume = ReadGuid();
        ulong epoch = ReadVarUInt();
        ulong sequence = ReadVarUInt();
        return new CommitToken(cluster, new VolumeId(volume), epoch, sequence);
    }

    /// <summary>Reads an <see cref="ObjectId"/>.</summary>
    public ObjectId ReadObjectId() => new(ReadUInt128());

    /// <summary>Reads a <see cref="NodeId"/>.</summary>
    public NodeId ReadNodeId() => new(ReadGuid());

    /// <summary>Reads a <see cref="VolumeId"/>.</summary>
    public VolumeId ReadVolumeId() => new(ReadGuid());

    /// <summary>Reads a <see cref="TimeSpan"/> (zigzag varint ticks).</summary>
    public TimeSpan ReadTimeSpan() => TimeSpan.FromTicks(ReadVarInt());

    /// <summary>Reads a UTC <see cref="DateTimeOffset"/> (zigzag varint UTC ticks).</summary>
    public DateTimeOffset ReadDateTimeOffset()
    {
        long ticks = ReadVarInt();
        if (ticks < DateTime.MinValue.Ticks || ticks > DateTime.MaxValue.Ticks)
            throw Malformed($"Timestamp ticks {ticks} are out of range.");
        return new DateTimeOffset(ticks, TimeSpan.Zero);
    }

    /// <summary>
    /// Reads an enum value encoded as a zigzag varint and verifies it fits the enum's underlying type and is a defined
    /// member. Use for protocol enums whose unknown values are malformed input.
    /// </summary>
    public TEnum ReadEnum<TEnum>() where TEnum : struct, Enum
    {
        TEnum value = ReadEnumRaw<TEnum>();
        if (!Enum.IsDefined(value))
            throw Malformed($"Undefined {typeof(TEnum).Name} value {WireEnum<TEnum>.ToInt64(value)}.");
        return value;
    }

    /// <summary>
    /// Reads an enum value encoded as a zigzag varint and verifies only that it fits the enum's underlying type. Use for
    /// flag enums and for enums whose value the receiver validates semantically (for example <see cref="FileMode"/> or
    /// <see cref="FileShare"/>, where an invalid value must produce <see cref="DfsErrorCode.InvalidArgument"/> rather than
    /// a protocol error).
    /// </summary>
    public TEnum ReadEnumRaw<TEnum>() where TEnum : struct, Enum
    {
        long raw = ReadVarInt();
        if (!WireEnum<TEnum>.TryFromInt64(raw, out TEnum value))
            throw Malformed($"Value {raw} does not fit the underlying type of {typeof(TEnum).Name}.");
        return value;
    }

    /// <summary>
    /// Reads the terminal extension block. Every accepted extension — understood ones and unknown optional ones — is
    /// passed to <paramref name="onOptional"/> in wire order (so callers can both consume understood fields and
    /// preserve unknown optional ones). An unknown <em>required</em> extension throws <see cref="DfsProtocolException"/>
    /// with <see cref="DfsErrorCode.Unsupported"/>; duplicate field ids or reserved flag bits are malformed.
    /// </summary>
    public void ReadExtensions(ISet<uint> understoodFieldIds, Action<WireExtension>? onOptional = null)
    {
        ArgumentNullException.ThrowIfNull(understoodFieldIds);
        int count = ReadCount(minElementBytes: 3);
        Span<uint> seenSmall = stackalloc uint[Math.Min(count, 32)];
        HashSet<uint>? seenLarge = count > 32 ? new HashSet<uint>() : null;
        for (int i = 0; i < count; i++)
        {
            uint fieldId = ReadVarUInt32();
            byte flags = ReadByte();
            if ((flags & ~ExtensionRequiredFlag) != 0)
                throw Malformed($"Extension {fieldId} uses reserved flag bits 0x{flags:x2}.");
            ReadOnlySpan<byte> value = ReadBytes();

            if (seenLarge is not null)
            {
                if (!seenLarge.Add(fieldId))
                    throw Malformed($"Duplicate extension field id {fieldId}.");
            }
            else
            {
                if (seenSmall[..i].Contains(fieldId))
                    throw Malformed($"Duplicate extension field id {fieldId}.");
                seenSmall[i] = fieldId;
            }

            bool required = (flags & ExtensionRequiredFlag) != 0;
            if (!understoodFieldIds.Contains(fieldId) && required)
                throw new DfsProtocolException(DfsErrorCode.Unsupported, $"The message requires unsupported extension field {fieldId}.");
            onOptional?.Invoke(new WireExtension(fieldId, required, value.ToArray()));
        }
    }

    /// <summary>Reads the terminal extension block, ignoring optional extensions and rejecting any required one.</summary>
    public void ReadExtensions() => ReadExtensions(FrozenSet<uint>.Empty);

    /// <summary>
    /// Consumes an empty extension block (a single zero count byte) when one is next, without allocating; returns false
    /// and consumes nothing otherwise.
    /// </summary>
    internal bool TryReadEmptyExtensions()
    {
        if (_position < _input.Length && _input[_position] == 0)
        {
            _position++;
            return true;
        }

        return false;
    }

    /// <summary>Throws <see cref="DfsProtocolException"/> when unread bytes remain.</summary>
    public readonly void EnsureEnd()
    {
        if (Remaining != 0)
            throw Malformed($"{Remaining} trailing bytes after the message body.");
    }

    /// <summary>Creates a <see cref="DfsErrorCode.ProtocolError"/> exception.</summary>
    public static DfsProtocolException Malformed(string message, Exception? inner = null) => new(DfsErrorCode.ProtocolError, message, inner);

    private ReadOnlySpan<byte> ReadSpan(int count)
    {
        if (count > Remaining)
            ThrowTruncated();
        ReadOnlySpan<byte> span = _input.Slice(_position, count);
        _position += count;
        return span;
    }

    [DoesNotReturn]
    private readonly void ThrowTruncated() => throw Malformed($"Truncated message: needed more than the {Remaining} remaining bytes.");
}
