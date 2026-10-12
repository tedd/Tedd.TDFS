using System.Buffers;
using System.Buffers.Binary;

namespace Tedd.Tdfs.Protocol;

/// <summary>
/// Encoder for the Tedd.TDFS wire format: little-endian fixed-width integers, LEB128 varints (zigzag for signed),
/// varuint-length-prefixed byte arrays and UTF-8 strings, and a terminal extension block per message.
/// </summary>
public ref struct WireWriter
{
    private readonly IBufferWriter<byte> _output;
    private long _written;

    /// <summary>Creates a writer appending to <paramref name="output"/>.</summary>
    public WireWriter(IBufferWriter<byte> output)
    {
        ArgumentNullException.ThrowIfNull(output);
        _output = output;
        _written = 0;
    }

    /// <summary>Number of bytes written through this writer.</summary>
    public readonly long BytesWritten => _written;

    /// <summary>Writes one byte.</summary>
    public void WriteByte(byte v)
    {
        GetSpan(1)[0] = v;
        Advance(1);
    }

    /// <summary>Writes a boolean as one byte (0 or 1).</summary>
    public void WriteBool(bool v) => WriteByte(v ? (byte)1 : (byte)0);

    /// <summary>Writes a little-endian 16-bit integer.</summary>
    public void WriteUInt16(ushort v)
    {
        BinaryPrimitives.WriteUInt16LittleEndian(GetSpan(sizeof(ushort)), v);
        Advance(sizeof(ushort));
    }

    /// <summary>Writes a little-endian 32-bit integer.</summary>
    public void WriteUInt32(uint v)
    {
        BinaryPrimitives.WriteUInt32LittleEndian(GetSpan(sizeof(uint)), v);
        Advance(sizeof(uint));
    }

    /// <summary>Writes a little-endian signed 32-bit integer.</summary>
    public void WriteInt32(int v) => WriteUInt32(unchecked((uint)v));

    /// <summary>Writes a little-endian 64-bit integer.</summary>
    public void WriteUInt64(ulong v)
    {
        BinaryPrimitives.WriteUInt64LittleEndian(GetSpan(sizeof(ulong)), v);
        Advance(sizeof(ulong));
    }

    /// <summary>Writes a little-endian signed 64-bit integer.</summary>
    public void WriteInt64(long v) => WriteUInt64(unchecked((ulong)v));

    /// <summary>Writes an unsigned LEB128 varint (1..10 bytes, minimal encoding).</summary>
    public void WriteVarUInt(ulong v)
    {
        Span<byte> span = GetSpan(10);
        int i = 0;
        while (v >= 0x80)
        {
            span[i++] = (byte)(v | 0x80);
            v >>= 7;
        }

        span[i++] = (byte)v;
        Advance(i);
    }

    /// <summary>Writes a signed varint using zigzag encoding.</summary>
    public void WriteVarInt(long v) => WriteVarUInt(unchecked((ulong)((v << 1) ^ (v >> 63))));

    /// <summary>Writes a GUID as 16 bytes in the standard little-endian GUID layout.</summary>
    public void WriteGuid(Guid v)
    {
        if (!v.TryWriteBytes(GetSpan(16), bigEndian: false, out int written) || written != 16)
            throw new InvalidOperationException("Failed to write GUID.");
        Advance(16);
    }

    /// <summary>Writes a 128-bit integer as 16 little-endian bytes.</summary>
    public void WriteUInt128(UInt128 v)
    {
        BinaryPrimitives.WriteUInt128LittleEndian(GetSpan(16), v);
        Advance(16);
    }

    /// <summary>Writes a varuint length followed by the bytes.</summary>
    public void WriteBytes(ReadOnlySpan<byte> v)
    {
        WriteVarUInt((ulong)v.Length);
        WriteRaw(v);
    }

    /// <summary>Writes a string as strict UTF-8 with a varuint length prefix.</summary>
    /// <exception cref="ArgumentNullException"><paramref name="v"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="v"/> contains an unpaired surrogate (never silently replaced).</exception>
    public void WriteString(string v)
    {
        ArgumentNullException.ThrowIfNull(v);
        int count = WireText.Strict.GetByteCount(v);
        WriteVarUInt((ulong)count);
        if (count == 0)
            return;
        Span<byte> span = GetSpan(count);
        int written = WireText.Strict.GetBytes(v, span);
        Advance(written);
    }

    /// <summary>
    /// Writes pre-encoded bytes as a string field (varuint length + bytes) without validation. Intended for raw
    /// protocol tests that must send malformed UTF-8; normal code uses <see cref="WriteString"/>.
    /// </summary>
    public void WriteUtf8(ReadOnlySpan<byte> utf8) => WriteBytes(utf8);

    /// <summary>Writes bytes without a length prefix.</summary>
    public void WriteRaw(ReadOnlySpan<byte> v)
    {
        while (!v.IsEmpty)
        {
            Span<byte> span = GetSpan(Math.Min(v.Length, 64 * 1024));
            int n = Math.Min(span.Length, v.Length);
            v[..n].CopyTo(span);
            Advance(n);
            v = v[n..];
        }
    }

    /// <summary>Writes a collection count as a varuint.</summary>
    public void WriteCount(int count)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        WriteVarUInt((ulong)count);
    }

    /// <summary>Writes a presence flag followed by the string when non-null.</summary>
    public void WriteOptionalString(string? v)
    {
        WriteBool(v is not null);
        if (v is not null)
            WriteString(v);
    }

    /// <summary>Writes a <see cref="DfsOperationId"/> as two varuints.</summary>
    public void WriteOperationId(DfsOperationId v)
    {
        WriteVarUInt(v.Session);
        WriteVarUInt(v.Sequence);
    }

    /// <summary>Writes a presence flag followed by the operation id when present.</summary>
    public void WriteOptionalOperationId(DfsOperationId? v)
    {
        WriteBool(v.HasValue);
        if (v.HasValue)
            WriteOperationId(v.Value);
    }

    /// <summary>Writes a <see cref="CommitToken"/> (cluster GUID, volume GUID, epoch varuint, sequence varuint).</summary>
    public void WriteCommitToken(CommitToken v)
    {
        WriteGuid(v.ClusterId);
        WriteGuid(v.VolumeId.Value);
        WriteVarUInt(v.VolumeEpoch);
        WriteVarUInt(v.Sequence);
    }

    /// <summary>Writes an <see cref="ObjectId"/> as 16 little-endian bytes.</summary>
    public void WriteObjectId(ObjectId v) => WriteUInt128(v.Value);

    /// <summary>Writes a <see cref="NodeId"/>.</summary>
    public void WriteNodeId(NodeId v) => WriteGuid(v.Value);

    /// <summary>Writes a <see cref="VolumeId"/>.</summary>
    public void WriteVolumeId(VolumeId v) => WriteGuid(v.Value);

    /// <summary>Writes a <see cref="TimeSpan"/> as zigzag varint ticks.</summary>
    public void WriteTimeSpan(TimeSpan v) => WriteVarInt(v.Ticks);

    /// <summary>Writes a <see cref="DateTimeOffset"/> as zigzag varint UTC ticks (the offset is not preserved).</summary>
    public void WriteDateTimeOffset(DateTimeOffset v) => WriteVarInt(v.UtcTicks);

    /// <summary>Writes the terminal extension block: count, then (field id varuint, flags byte, value bytes) per entry.</summary>
    /// <exception cref="ArgumentException">Two extensions share a field id.</exception>
    public void WriteExtensions(ReadOnlySpan<WireExtension> extensions)
    {
        for (int i = 1; i < extensions.Length; i++)
        {
            for (int j = 0; j < i; j++)
            {
                if (extensions[i].FieldId == extensions[j].FieldId)
                    throw new ArgumentException($"Duplicate extension field id {extensions[i].FieldId}.", nameof(extensions));
            }
        }

        WriteVarUInt((ulong)extensions.Length);
        foreach (WireExtension ext in extensions)
        {
            WriteVarUInt(ext.FieldId);
            WriteByte(ext.Required ? WireReader.ExtensionRequiredFlag : (byte)0);
            WriteBytes(ext.Value.Span);
        }
    }

    /// <summary>Writes an empty terminal extension block.</summary>
    public void WriteNoExtensions() => WriteVarUInt(0);

    private readonly Span<byte> GetSpan(int sizeHint)
    {
        if (_output is null)
            throw new InvalidOperationException("WireWriter was not initialized with an output buffer.");
        return _output.GetSpan(sizeHint);
    }

    private void Advance(int count)
    {
        _output.Advance(count);
        _written += count;
    }
}
