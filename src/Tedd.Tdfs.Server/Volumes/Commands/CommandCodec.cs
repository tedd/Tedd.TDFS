using System.Buffers;
using System.Collections.Frozen;
using System.Collections.Immutable;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Server.Volumes.State;

namespace Tedd.Tdfs.Server.Volumes.Commands;

/// <summary>Reads the body of one command kind.</summary>
internal delegate VolumeCommand CommandBodyReader(ref WireReader reader);

/// <summary>
/// Canonical encoding of volume log commands: <c>[u8 format][u16 kind][timestamp leaderTime][OperationHeader?][body]
/// [extension block]</c>. Decoding is strict (unknown kinds, malformed bodies and trailing bytes are protocol errors).
/// Registrations live in <c>CommandCodec.Registrations.cs</c>.
/// </summary>
internal static partial class CommandCodec
{
    /// <summary>Command format version.</summary>
    public const byte Format = 1;

    private static readonly FrozenDictionary<VolumeCommandKind, CommandBodyReader> Readers = BuildReaders();

    /// <summary>Command extension carrying <see cref="VolumeCommand.FencingFallback"/> (negative controls only).</summary>
    private const uint FencingFallbackExtension = 1;

    private static readonly FrozenSet<uint> UnderstoodExtensions = FrozenSet.ToFrozenSet([FencingFallbackExtension]);

    /// <summary>Encodes a command.</summary>
    public static byte[] Encode(VolumeCommand command)
    {
        ArgumentNullException.ThrowIfNull(command);
        var buffer = new ArrayBufferWriter<byte>(256);
        var w = new WireWriter(buffer);
        w.WriteByte(Format);
        w.WriteUInt16((ushort)command.Kind);
        w.WriteDateTimeOffset(command.LeaderTime);
        w.WriteBool(command.Op is not null);
        command.Op?.Write(ref w);
        command.WriteBody(ref w);
        if (command.FencingFallback is { } fallback)
        {
            var ext = new ArrayBufferWriter<byte>(32);
            var e = new WireWriter(ext);
            e.WriteVarUInt(fallback.Inode);
            e.WriteString(fallback.Stream);
            w.WriteExtensions([new WireExtension(FencingFallbackExtension, false, ext.WrittenMemory.ToArray())]);
        }
        else
        {
            w.WriteNoExtensions();
        }

        return buffer.WrittenSpan.ToArray();
    }

    /// <summary>Decodes a command.</summary>
    /// <exception cref="DfsProtocolException">Malformed or unknown command.</exception>
    public static VolumeCommand Decode(ReadOnlySpan<byte> data)
    {
        var r = new WireReader(data, new WireLimits(MaxBytesLength: int.MaxValue, MaxCollectionCount: 1 << 24));
        byte format = r.ReadByte();
        if (format != Format)
            throw WireReader.Malformed($"Unsupported volume command format {format}.");
        var kind = (VolumeCommandKind)r.ReadUInt16();
        if (!Readers.TryGetValue(kind, out CommandBodyReader? reader))
            throw new DfsProtocolException(DfsErrorCode.Unsupported, $"Unknown volume command kind {(ushort)kind}.");
        DateTimeOffset time = r.ReadDateTimeOffset();
        OperationHeader? op = r.ReadBool() ? OperationHeader.Read(ref r) : null;
        VolumeCommand body = reader(ref r);
        StreamKey? fallback = null;
        r.ReadExtensions(UnderstoodExtensions, ext =>
        {
            if (ext.FieldId == FencingFallbackExtension)
            {
                var e = new WireReader(ext.Value.Span, WireLimits.Default);
                fallback = new StreamKey(e.ReadVarUInt(), e.ReadString());
            }
        });
        r.EnsureEnd();
        return body with { LeaderTime = time, Op = op, FencingFallback = fallback };
    }

    /// <summary>The kind of an encoded command without decoding the body (null when malformed).</summary>
    public static VolumeCommandKind? PeekKind(ReadOnlySpan<byte> data)
        => data.Length >= 3 && data[0] == Format ? (VolumeCommandKind)(data[1] | (data[2] << 8)) : null;

    /// <summary>True when <paramref name="kind"/> has a registered reader.</summary>
    public static bool IsKnown(VolumeCommandKind kind) => Readers.ContainsKey(kind);

    internal static void WriteUInt64s(ref WireWriter w, ImmutableArray<ulong> values)
    {
        w.WriteCount(values.IsDefault ? 0 : values.Length);
        if (values.IsDefault)
            return;
        foreach (ulong v in values)
            w.WriteVarUInt(v);
    }

    internal static ImmutableArray<ulong> ReadUInt64s(ref WireReader r)
    {
        int count = r.ReadCount(1);
        var result = ImmutableArray.CreateBuilder<ulong>(count);
        for (int i = 0; i < count; i++)
            result.Add(r.ReadVarUInt());
        return result.MoveToImmutable();
    }

    private static FrozenDictionary<VolumeCommandKind, CommandBodyReader> BuildReaders()
    {
        var readers = new Dictionary<VolumeCommandKind, CommandBodyReader>();
        RegisterAll(readers);
        return readers.ToFrozenDictionary();
    }

    private static partial void RegisterAll(Dictionary<VolumeCommandKind, CommandBodyReader> readers);
}
