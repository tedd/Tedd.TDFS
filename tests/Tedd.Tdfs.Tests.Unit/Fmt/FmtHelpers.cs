using System.Buffers;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Unit.Fmt;

/// <summary>Writes one value with a <see cref="WireWriter"/> (ref structs cannot be captured by ordinary lambdas).</summary>
internal delegate void WireWrite(ref WireWriter writer);

/// <summary>Reads one value with a <see cref="WireReader"/>; the result is boxed for comparison.</summary>
internal delegate object? WireRead(ref WireReader reader);

/// <summary>A golden wire vector: the exact bytes a value must encode to, and the value those bytes must decode to.</summary>
internal sealed record WireVector(string Name, byte[] Expected, object? Value, WireWrite Write, WireRead Read)
{
    public override string ToString() => Name;
}

/// <summary>A golden text vector for an identifier/token string form.</summary>
internal sealed record TextVector(string Name, Func<string> Format, string Expected, Func<string, object> Parse, object Value)
{
    public override string ToString() => Name;
}

/// <summary>A golden error-reply frame (FMT-001 "errors", FMT-011).</summary>
internal sealed record ErrorFrameVector(string Name, Func<byte[]> Encode, byte[] Expected,
    DfsErrorCode Code, string Message, DfsOperationId? Op, bool Retryable, string? LeaderHint, DfsTransactionId? Transaction)
{
    public override string ToString() => Name;
}

/// <summary>Shared helpers for the FMT unit tests.</summary>
internal static class FmtHelpers
{
    public const string Volume = "vol";

    public static CancellationToken Ct => TestContext.Current.CancellationToken;

    public static void Log(string message) => TestContext.Current.TestOutputHelper?.WriteLine(message);

    /// <summary>Encodes with a fresh writer over a buffer of the given initial capacity (1 forces many buffer growths).</summary>
    public static byte[] Encode(WireWrite write, int initialCapacity = 256)
    {
        var buffer = new ArrayBufferWriter<byte>(initialCapacity);
        var writer = new WireWriter(buffer);
        write(ref writer);
        return buffer.WrittenSpan.ToArray();
    }

    /// <summary>Decodes <paramref name="bytes"/> and requires the reader to consume every byte.</summary>
    public static object? DecodeExact(byte[] bytes, WireRead read, WireLimits? limits = null)
    {
        var reader = new WireReader(bytes, limits ?? WireLimits.Default);
        object? value = read(ref reader);
        int remaining = reader.Remaining;
        reader.EnsureEnd();
        Assert.Equal(0, remaining);
        return value;
    }

    /// <summary>Asserts that decoding <paramref name="bytes"/> fails with a <see cref="DfsProtocolException"/> carrying <paramref name="expected"/>.</summary>
    public static DfsProtocolException DecodeFails(byte[] bytes, WireRead read, DfsErrorCode expected, WireLimits? limits = null, string? because = null)
    {
        DfsProtocolException? caught = null;
        object? decoded = null;
        try
        {
            var reader = new WireReader(bytes, limits ?? WireLimits.Default);
            decoded = read(ref reader);
            reader.EnsureEnd();
        }
        catch (DfsProtocolException ex)
        {
            caught = ex;
        }

        Assert.True(caught is not null,
            $"Decoding {Convert.ToHexString(bytes)} must fail with {expected}{(because is null ? string.Empty : $" ({because})")} but produced '{decoded}'.");
        Assert.Equal(expected, caught!.ErrorCode);
        return caught;
    }

    /// <summary>Asserts that decoding a complete frame fails with <paramref name="expected"/>, through both the generic and the error codec.</summary>
    public static void FrameDecodeFails(byte[] frame, DfsErrorCode expected, string because)
    {
        DfsProtocolException generic = Assert.Throws<DfsProtocolException>(() => MessageCodec.Decode(frame));
        Assert.True(generic.ErrorCode == expected, $"{because}: MessageCodec.Decode reported {generic.ErrorCode}, expected {expected}.");
        DfsProtocolException viaErrorCodec = Assert.Throws<DfsProtocolException>(() => ErrorCodec.Decode(frame));
        Assert.True(viaErrorCodec.ErrorCode == expected, $"{because}: ErrorCodec.Decode reported {viaErrorCodec.ErrorCode}, expected {expected}.");
    }

    /// <summary>Value equality for boxed decoded values (byte arrays compare by content).</summary>
    public static bool ValuesEqual(object? expected, object? actual) => (expected, actual) switch
    {
        (byte[] e, byte[] a) => e.AsSpan().SequenceEqual(a),
        _ => Equals(expected, actual),
    };

    /// <summary>Byte-exact comparison taking arrays, so call sites may await inside the argument list.</summary>
    public static void BytesEqual(byte[] expected, byte[] actual, string? because = null) => AssertEx.BytesEqual(expected, actual, because);

    public static byte[] Concat(params byte[][] parts)
    {
        int length = 0;
        foreach (byte[] p in parts)
            length += p.Length;
        var result = new byte[length];
        int offset = 0;
        foreach (byte[] p in parts)
        {
            p.CopyTo(result, offset);
            offset += p.Length;
        }

        return result;
    }

    /// <summary>Builds a string from UTF-16 code units (keeps non-ASCII and unpaired-surrogate test values explicit in source).</summary>
    public static string Chars(params int[] codeUnits) => new(codeUnits.Select(c => (char)c).ToArray());

    public static byte[] Repeat(byte value, int count)
    {
        var result = new byte[count];
        result.AsSpan().Fill(value);
        return result;
    }

    public static TheoryData<string> Names(IEnumerable<string> names)
    {
        var data = new TheoryData<string>();
        foreach (string name in names)
            data.Add(name);
        return data;
    }

    /// <summary>Formats extensions read back from a block as "id:required:HEX|..." for comparison.</summary>
    public static string Describe(IEnumerable<WireExtension> extensions) =>
        string.Join("|", extensions.Select(e => $"{e.FieldId}:{(e.Required ? 1 : 0)}:{Convert.ToHexString(e.Value.Span)}"));

    /// <summary>Reads an extension block, accepting the given field ids, and describes every delivered extension.</summary>
    public static string ReadExtensionBlock(ref WireReader reader, params uint[] understood)
    {
        var delivered = new List<WireExtension>();
        reader.ReadExtensions(new HashSet<uint>(understood), delivered.Add);
        return Describe(delivered);
    }
}

/// <summary>
/// Golden vectors for the wire format (API.md C.1, PROTOCOL.md §§2, 9.1): little-endian fixed-width integers, LEB128
/// varints (zigzag for signed), varuint-length-prefixed UTF-8 strings and byte arrays, GUIDs in the standard little-endian
/// layout, 128-bit ids little-endian, optional values as a 0/1 presence byte plus the value, the <c>ErrorReply</c> field
/// order (code, message, operation id?, retryable, leader hint?, transaction id?, detail?, extensions) and the terminal
/// extension block <c>{count, (fieldId, flags, bytes)*}</c>. Every expected byte array was computed by hand from those
/// rules; none is produced by the encoder under test.
/// </summary>
internal static class FmtGolden
{
    public static readonly Guid SampleGuid = new("00112233-4455-6677-8899-aabbccddeeff");
    public static readonly Guid SampleGuid2 = new("ffeeddcc-bbaa-9988-7766-554433221100");

    // Data1 0x00112233 LE, Data2 0x4455 LE, Data3 0x6677 LE, Data4 as written.
    public static readonly byte[] SampleGuidBytes =
        [0x33, 0x22, 0x11, 0x00, 0x55, 0x44, 0x77, 0x66, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF];

    public static readonly byte[] SampleGuid2Bytes =
        [0xCC, 0xDD, 0xEE, 0xFF, 0xAA, 0xBB, 0x88, 0x99, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, 0x00];

    public static readonly ObjectId SampleObject = new(new UInt128(0x0011223344556677UL, 0x8899AABBCCDDEEFFUL));

    // 128-bit value little-endian: least significant byte (0xFF) first.
    public static readonly byte[] SampleObjectBytes =
        [0xFF, 0xEE, 0xDD, 0xCC, 0xBB, 0xAA, 0x99, 0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, 0x00];

    public static readonly CommitToken SampleToken = new(SampleGuid, new VolumeId(SampleGuid2), 1, 300);

    // ErrorCodec.Encode(OutcomeUnknown, "x", op:1234-1, retryable:false), ErrorReply layout of PROTOCOL.md §9.1
    // (Code, Message, OperationId?, Retryable, LeaderHint?, TransactionId?, Detail?, extensions):
    // [u16 type=1][varint zigzag(23)=0x2E]["x"=01 78][op present 01, session 0x1234=B4 24, seq 01][retryable 00]
    // [hint absent 00][tx absent 00][detail absent 00][ext count 00]
    public static readonly byte[] OutcomeUnknownFrame =
        [0x01, 0x00, 0x2E, 0x01, 0x78, 0x01, 0xB4, 0x24, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00];

    // The same frame without its terminal extension count, for building frames with custom extension blocks.
    public static readonly byte[] OutcomeUnknownFrameWithoutExtensions =
        [0x01, 0x00, 0x2E, 0x01, 0x78, 0x01, 0xB4, 0x24, 0x01, 0x00, 0x00, 0x00, 0x00];

    private static readonly Lazy<IReadOnlyDictionary<string, WireVector>> WireVectorTable = new(BuildWireVectors);
    private static readonly Lazy<IReadOnlyDictionary<string, TextVector>> TextVectorTable = new(BuildTextVectors);
    private static readonly Lazy<IReadOnlyDictionary<string, ErrorFrameVector>> ErrorFrameTable = new(BuildErrorFrames);

    public static IReadOnlyDictionary<string, WireVector> WireVectors => WireVectorTable.Value;

    public static IReadOnlyDictionary<string, TextVector> TextVectors => TextVectorTable.Value;

    public static IReadOnlyDictionary<string, ErrorFrameVector> ErrorFrames => ErrorFrameTable.Value;

    private static IReadOnlyDictionary<string, WireVector> BuildWireVectors()
    {
        var list = new List<WireVector>
        {
            // ---- unsigned LEB128 varints (revisions, lengths, offsets, inode ids, counts) ----
            VarU("varuint/0", 0, [0x00]),
            VarU("varuint/1", 1, [0x01]),
            VarU("varuint/127", 127, [0x7F]),
            VarU("varuint/128", 128, [0x80, 0x01]),
            VarU("varuint/300", 300, [0xAC, 0x02]),
            VarU("varuint/16383", 16383, [0xFF, 0x7F]),
            VarU("varuint/16384", 16384, [0x80, 0x80, 0x01]),
            VarU("varuint/2^32-1", uint.MaxValue, [0xFF, 0xFF, 0xFF, 0xFF, 0x0F]),
            VarU("varuint/2^32", 1UL << 32, [0x80, 0x80, 0x80, 0x80, 0x10]),
            VarU("varuint/2^63", 1UL << 63, [0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01]),
            VarU("varuint/2^64-1", ulong.MaxValue, [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01]),
            VarU("revision/1000000", 1_000_000, [0xC0, 0x84, 0x3D]),

            // ---- zigzag signed varints ----
            VarS("varint/0", 0, [0x00]),
            VarS("varint/-1", -1, [0x01]),
            VarS("varint/1", 1, [0x02]),
            VarS("varint/-2", -2, [0x03]),
            VarS("varint/63", 63, [0x7E]),
            VarS("varint/-64", -64, [0x7F]),
            VarS("varint/64", 64, [0x80, 0x01]),
            VarS("varint/-65", -65, [0x81, 0x01]),
            VarS("varint/int64-max", long.MaxValue, [0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01]),
            VarS("varint/int64-min", long.MinValue, [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01]),

            // ---- little-endian fixed-width ----
            new("byte/0xAB", [0xAB], (byte)0xAB, (ref WireWriter w) => w.WriteByte(0xAB), (ref WireReader r) => r.ReadByte()),
            new("bool/false", [0x00], false, (ref WireWriter w) => w.WriteBool(false), (ref WireReader r) => r.ReadBool()),
            new("bool/true", [0x01], true, (ref WireWriter w) => w.WriteBool(true), (ref WireReader r) => r.ReadBool()),
            new("u16/0x1234", [0x34, 0x12], (ushort)0x1234, (ref WireWriter w) => w.WriteUInt16(0x1234), (ref WireReader r) => r.ReadUInt16()),
            new("u16/max", [0xFF, 0xFF], ushort.MaxValue, (ref WireWriter w) => w.WriteUInt16(ushort.MaxValue), (ref WireReader r) => r.ReadUInt16()),
            new("u32/0x12345678", [0x78, 0x56, 0x34, 0x12], 0x12345678u, (ref WireWriter w) => w.WriteUInt32(0x12345678u), (ref WireReader r) => r.ReadUInt32()),
            new("u32/0x80000000", [0x00, 0x00, 0x00, 0x80], 0x80000000u, (ref WireWriter w) => w.WriteUInt32(0x80000000u), (ref WireReader r) => r.ReadUInt32()),
            new("u64/0x0102030405060708", [0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01], 0x0102030405060708UL,
                (ref WireWriter w) => w.WriteUInt64(0x0102030405060708UL), (ref WireReader r) => r.ReadUInt64()),
            new("i64/-2", [0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF], -2L, (ref WireWriter w) => w.WriteInt64(-2), (ref WireReader r) => r.ReadInt64()),
            new("i64/min", [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80], long.MinValue,
                (ref WireWriter w) => w.WriteInt64(long.MinValue), (ref WireReader r) => r.ReadInt64()),

            // ---- identifiers ----
            new("guid/sample", SampleGuidBytes, SampleGuid, (ref WireWriter w) => w.WriteGuid(SampleGuid), (ref WireReader r) => r.ReadGuid()),
            new("nodeid/sample", SampleGuidBytes, new NodeId(SampleGuid), (ref WireWriter w) => w.WriteNodeId(new NodeId(SampleGuid)), (ref WireReader r) => r.ReadNodeId()),
            new("volumeid/sample", SampleGuid2Bytes, new VolumeId(SampleGuid2), (ref WireWriter w) => w.WriteVolumeId(new VolumeId(SampleGuid2)), (ref WireReader r) => r.ReadVolumeId()),
            new("uint128/1", [0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00], UInt128.One,
                (ref WireWriter w) => w.WriteUInt128(UInt128.One), (ref WireReader r) => r.ReadUInt128()),
            new("objectid/sample", SampleObjectBytes, SampleObject, (ref WireWriter w) => w.WriteObjectId(SampleObject), (ref WireReader r) => r.ReadObjectId()),
            new("opid/1234-1", [0xB4, 0x24, 0x01], new DfsOperationId(0x1234, 1),
                (ref WireWriter w) => w.WriteOperationId(new DfsOperationId(0x1234, 1)), (ref WireReader r) => r.ReadOperationId()),
            new("opid/max", [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01],
                new DfsOperationId(ulong.MaxValue, ulong.MaxValue),
                (ref WireWriter w) => w.WriteOperationId(new DfsOperationId(ulong.MaxValue, ulong.MaxValue)), (ref WireReader r) => r.ReadOperationId()),
            new("opid/optional-present", [0x01, 0x01, 0x02], new DfsOperationId(1, 2),
                (ref WireWriter w) => w.WriteOptionalOperationId(new DfsOperationId(1, 2)), (ref WireReader r) => r.ReadOptionalOperationId()),
            new("opid/optional-absent", [0x00], null,
                (ref WireWriter w) => w.WriteOptionalOperationId(null), (ref WireReader r) => r.ReadOptionalOperationId()),

            // ---- commit tokens: cluster GUID, volume GUID, epoch varuint, sequence varuint ----
            new("token/sample", FmtHelpers.Concat(SampleGuidBytes, SampleGuid2Bytes, [0x01], [0xAC, 0x02]), SampleToken,
                (ref WireWriter w) => w.WriteCommitToken(SampleToken), (ref WireReader r) => r.ReadCommitToken()),
            new("token/none", FmtHelpers.Concat(new byte[32], [0x00, 0x00]), CommitToken.None,
                (ref WireWriter w) => w.WriteCommitToken(CommitToken.None), (ref WireReader r) => r.ReadCommitToken()),

            // ---- paths and strings (UTF-8, varuint byte-length prefix; paths travel in normalised NFC form) ----
            Str("path/root", "/", [0x01, 0x2F]),
            Str("path/ascii", "/docs/a.txt", [0x0B, 0x2F, 0x64, 0x6F, 0x63, 0x73, 0x2F, 0x61, 0x2E, 0x74, 0x78, 0x74]),
            Str("path/multibyte", "/" + FmtHelpers.Chars(0x00E6, 0x20AC) + char.ConvertFromUtf32(0x1F600), [0x0A, 0x2F, 0xC3, 0xA6, 0xE2, 0x82, 0xAC, 0xF0, 0x9F, 0x98, 0x80]),
            new("path/nfc-normalised", [0x06, 0x2F, 0x63, 0x61, 0x66, 0xC3, 0xA9], "/caf" + FmtHelpers.Chars(0x00E9),
                (ref WireWriter w) => w.WriteString(DfsPath.Normalize("/cafe" + FmtHelpers.Chars(0x0301))), (ref WireReader r) => r.ReadString()),
            Str("path/component-255-bytes", "/" + new string('a', 255), FmtHelpers.Concat([0x80, 0x02, 0x2F], FmtHelpers.Repeat(0x61, 255))),
            Str("string/empty", string.Empty, [0x00]),

            // ---- byte arrays ----
            new("bytes/empty", [0x00], Array.Empty<byte>(), (ref WireWriter w) => w.WriteBytes([]), (ref WireReader r) => r.ReadBytes().ToArray()),
            new("bytes/deadbeef", [0x04, 0xDE, 0xAD, 0xBE, 0xEF], new byte[] { 0xDE, 0xAD, 0xBE, 0xEF },
                (ref WireWriter w) => w.WriteBytes([0xDE, 0xAD, 0xBE, 0xEF]), (ref WireReader r) => r.ReadBytes().ToArray()),

            // ---- extent field sequence (composed by this test from primitives): offset, length, object, object offset, object length ----
            new("extent/fields", FmtHelpers.Concat([0x80, 0x80, 0x04], [0x80, 0x20], SampleObjectBytes, [0x00], [0x80, 0x20]),
                (65536UL, 4096UL, SampleObject, 0UL, 4096UL),
                (ref WireWriter w) =>
                {
                    w.WriteVarUInt(65536);
                    w.WriteVarUInt(4096);
                    w.WriteObjectId(SampleObject);
                    w.WriteVarUInt(0);
                    w.WriteVarUInt(4096);
                },
                (ref WireReader r) => (r.ReadVarUInt(), r.ReadVarUInt(), r.ReadObjectId(), r.ReadVarUInt(), r.ReadVarUInt())),

            // ---- manifest entry list (composed by this test from primitives): count, then (path, inode, revision) ----
            new("manifest/entries", [0x02, 0x02, 0x2F, 0x61, 0x02, 0x05, 0x02, 0x2F, 0x62, 0x03, 0x06], "/a:2:5;/b:3:6",
                (ref WireWriter w) =>
                {
                    w.WriteVarUInt(2);
                    w.WriteString("/a");
                    w.WriteVarUInt(2);
                    w.WriteVarUInt(5);
                    w.WriteString("/b");
                    w.WriteVarUInt(3);
                    w.WriteVarUInt(6);
                },
                (ref WireReader r) =>
                {
                    ulong count = r.ReadVarUInt();
                    var parts = new List<string>();
                    for (ulong i = 0; i < count; i++)
                        parts.Add($"{r.ReadString()}:{r.ReadVarUInt()}:{r.ReadVarUInt()}");
                    return string.Join(";", parts);
                }),

            // ---- terminal extension blocks ----
            new("extensions/empty", [0x00], string.Empty,
                (ref WireWriter w) => w.WriteExtensions([]), (ref WireReader r) => FmtHelpers.ReadExtensionBlock(ref r)),
            new("extensions/optional-and-required", [0x02, 0x01, 0x00, 0x01, 0xAA, 0xAC, 0x02, 0x01, 0x00], "1:0:AA|300:1:",
                (ref WireWriter w) => w.WriteExtensions(new WireExtension[] { new(1, false, new byte[] { 0xAA }), new(300, true, Array.Empty<byte>()) }),
                (ref WireReader r) => FmtHelpers.ReadExtensionBlock(ref r, 1, 300)),
        };

        return list.ToDictionary(v => v.Name, StringComparer.Ordinal);
    }

    private static WireVector VarU(string name, ulong value, byte[] bytes) =>
        new(name, bytes, value, (ref WireWriter w) => w.WriteVarUInt(value), (ref WireReader r) => r.ReadVarUInt());

    private static WireVector VarS(string name, long value, byte[] bytes) =>
        new(name, bytes, value, (ref WireWriter w) => w.WriteVarInt(value), (ref WireReader r) => r.ReadVarInt());

    private static WireVector Str(string name, string value, byte[] bytes) =>
        new(name, bytes, value, (ref WireWriter w) => w.WriteString(value), (ref WireReader r) => r.ReadString());

    private static IReadOnlyDictionary<string, TextVector> BuildTextVectors()
    {
        var list = new List<TextVector>
        {
            new("opid/1234-1", () => new DfsOperationId(0x1234, 1).ToString(), "op:1234-1", s => DfsOperationId.Parse(s), new DfsOperationId(0x1234, 1)),
            new("opid/max", () => new DfsOperationId(ulong.MaxValue, ulong.MaxValue).ToString(), "op:ffffffffffffffff-ffffffffffffffff",
                s => DfsOperationId.Parse(s), new DfsOperationId(ulong.MaxValue, ulong.MaxValue)),
            new("txid/ab-10", () => new DfsTransactionId(new DfsOperationId(0xAB, 0x10)).ToString(), "tx:ab-10",
                s => DfsTransactionId.Parse(s), new DfsTransactionId(new DfsOperationId(0xAB, 0x10))),
            new("token/sample", () => SampleToken.ToString(),
                "ct:00112233-4455-6677-8899-aabbccddeeff/ffeeddcc-bbaa-9988-7766-554433221100/1/300", s => CommitToken.Parse(s), SampleToken),
            new("token/none", () => CommitToken.None.ToString(),
                "ct:00000000-0000-0000-0000-000000000000/00000000-0000-0000-0000-000000000000/0/0", s => CommitToken.Parse(s), CommitToken.None),
            new("token/max", () => new CommitToken(SampleGuid, new VolumeId(SampleGuid2), ulong.MaxValue, ulong.MaxValue).ToString(),
                "ct:00112233-4455-6677-8899-aabbccddeeff/ffeeddcc-bbaa-9988-7766-554433221100/18446744073709551615/18446744073709551615",
                s => CommitToken.Parse(s), new CommitToken(SampleGuid, new VolumeId(SampleGuid2), ulong.MaxValue, ulong.MaxValue)),
            new("objectid/sample", () => SampleObject.ToString(), "00112233445566778899aabbccddeeff", s => ObjectId.Parse(s), SampleObject),
            new("objectid/one", () => new ObjectId(UInt128.One).ToString(), "00000000000000000000000000000001", s => ObjectId.Parse(s), new ObjectId(UInt128.One)),
        };
        return list.ToDictionary(v => v.Name, StringComparer.Ordinal);
    }

    private static IReadOnlyDictionary<string, ErrorFrameVector> BuildErrorFrames()
    {
        var list = new List<ErrorFrameVector>
        {
            new("outcome-unknown", () => ErrorCodec.Encode(DfsErrorCode.OutcomeUnknown, "x", new DfsOperationId(0x1234, 1), retryable: false),
                OutcomeUnknownFrame, DfsErrorCode.OutcomeUnknown, "x", new DfsOperationId(0x1234, 1), false, null, null),

            // zigzag(4) = 0x08; empty message; no op; not retryable; no hint; no tx; no detail; no extensions.
            new("file-not-found", () => ErrorCodec.Encode(DfsErrorCode.FileNotFound, string.Empty, null, retryable: false),
                [0x01, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00], DfsErrorCode.FileNotFound, string.Empty, null, false, null, null),

            // zigzag(12) = 0x18; "busy" = 62 75 73 79; no op; retryable; no hint; no tx; no detail; no extensions.
            new("sharing-violation-retryable", () => ErrorCodec.Encode(DfsErrorCode.SharingViolation, "busy", null, retryable: true),
                [0x01, 0x00, 0x18, 0x04, 0x62, 0x75, 0x73, 0x79, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00],
                DfsErrorCode.SharingViolation, "busy", null, true, null, null),

            // Unknown-outcome transaction commit with leader hint "n1" (6E 31) and transaction id tx:1-1; no detail; no extensions.
            new("outcome-unknown-transaction",
                () => MessageCodec.Encode(new ErrorReplyMessage(DfsErrorCode.OutcomeUnknown, string.Empty, new DfsOperationId(1, 2), true, "n1",
                    new DfsTransactionId(new DfsOperationId(1, 1)))),
                [0x01, 0x00, 0x2E, 0x00, 0x01, 0x01, 0x02, 0x01, 0x01, 0x02, 0x6E, 0x31, 0x01, 0x01, 0x01, 0x00, 0x00],
                DfsErrorCode.OutcomeUnknown, string.Empty, new DfsOperationId(1, 2), true, "n1", new DfsTransactionId(new DfsOperationId(1, 1))),

            // A code unknown to this version (1000): zigzag(1000) = 2000 = D0 0F; must be carried verbatim.
            new("future-code-1000", () => ErrorCodec.Encode((DfsErrorCode)1000, string.Empty, null, retryable: false),
                [0x01, 0x00, 0xD0, 0x0F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00], (DfsErrorCode)1000, string.Empty, null, false, null, null),
        };
        return list.ToDictionary(v => v.Name, StringComparer.Ordinal);
    }
}
