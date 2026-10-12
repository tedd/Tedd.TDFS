using System.IO.Hashing;
using System.Numerics;
using System.Text;
using Tedd.Tdfs.Data;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Tests.Unit.Fmt;

/// <summary>
/// FMT suite, level U: wire formats, error serialization and canonical identity (FSD §§3.2, 10, 12, 15, 17; test plan
/// §7.25). Golden vectors are literal byte arrays computed by hand from the encoding rules in API.md C.1, so they pin the
/// format independently of the encoder under test. Cluster-backed rows run against <see cref="TestClusterOptions.ThreeZones"/>.
/// </summary>
public sealed class FmtUnitTests
{
    private const string Vol = FmtHelpers.Volume;

    private static CancellationToken Ct => FmtHelpers.Ct;

    public static TheoryData<string> WireVectorNames => FmtHelpers.Names(FmtGolden.WireVectors.Keys);

    public static TheoryData<string> TextVectorNames => FmtHelpers.Names(FmtGolden.TextVectors.Keys);

    public static TheoryData<string> ErrorFrameNames => FmtHelpers.Names(FmtGolden.ErrorFrames.Keys);

    public static TheoryData<int> AllErrorCodes
    {
        get
        {
            var data = new TheoryData<int>();
            foreach (DfsErrorCode code in Enum.GetValues<DfsErrorCode>())
                data.Add((int)code);
            return data;
        }
    }

    // ============================================================================================================
    // FMT-001 WireRoundTripGoldenVectors
    // ============================================================================================================

    /// <summary>
    /// Every golden vector (varints, fixed-width integers, GUID/node/volume/object ids, operation ids, revisions, commit
    /// tokens, paths, byte arrays, extent and manifest field sequences, extension blocks) encodes to exactly the golden
    /// bytes, decodes from them to exactly the golden value consuming every byte, is independent of output-buffer growth,
    /// and every truncation or trailing byte is rejected as a protocol error.
    /// </summary>
    [Theory, Catalog("FMT-001", "U", "INV-01", "INV-06", "INV-08", "INV-10", "INV-18")]
    [MemberData(nameof(WireVectorNames))]
    public void FMT_001_WireRoundTripGoldenVectors(string vector)
    {
        WireVector v = FmtGolden.WireVectors[vector];
        FmtHelpers.Log($"{v.Name}: {Convert.ToHexString(v.Expected)}");

        AssertEx.BytesEqual(v.Expected, FmtHelpers.Encode(v.Write), $"encoding of {v.Name}");
        AssertEx.BytesEqual(v.Expected, FmtHelpers.Encode(v.Write, initialCapacity: 1), $"encoding of {v.Name} into a 1-byte initial buffer");

        object? decoded = FmtHelpers.DecodeExact(v.Expected, v.Read);
        Assert.True(FmtHelpers.ValuesEqual(v.Value, decoded), $"{v.Name}: decoded '{decoded}' but the golden value is '{v.Value}'.");

        for (int cut = 0; cut < v.Expected.Length; cut++)
            FmtHelpers.DecodeFails(v.Expected[..cut], v.Read, DfsErrorCode.ProtocolError, because: $"{v.Name} truncated to {cut} bytes");

        FmtHelpers.DecodeFails(FmtHelpers.Concat(v.Expected, [0x00]), v.Read, DfsErrorCode.ProtocolError, because: $"{v.Name} with a trailing byte");
    }

    /// <summary>The canonical text forms of operation ids, transaction ids, commit tokens and object ids are pinned and round-trip exactly.</summary>
    [Theory, Catalog("FMT-001", "U", "INV-01", "INV-06", "INV-08", "INV-10", "INV-18")]
    [MemberData(nameof(TextVectorNames))]
    public void FMT_001_WireRoundTripGoldenVectors_TextForms(string vector)
    {
        TextVector v = FmtGolden.TextVectors[vector];
        string formatted = v.Format();
        Assert.Equal(v.Expected, formatted);
        object parsed = v.Parse(v.Expected);
        Assert.Equal(v.Value, parsed);
        Assert.Equal(v.Expected, parsed.ToString());
    }

    /// <summary>
    /// Error replies are part of the wire format: each golden error frame (unknown outcome with operation id, not-found,
    /// retryable sharing violation, unknown transaction outcome with leader hint and transaction id, and a code from a
    /// newer version) encodes to the golden bytes, decodes to the golden fields and re-encodes byte-identically.
    /// </summary>
    [Theory, Catalog("FMT-001", "U", "INV-01", "INV-06", "INV-08", "INV-10", "INV-18")]
    [MemberData(nameof(ErrorFrameNames))]
    public void FMT_001_WireRoundTripGoldenVectors_ErrorFrames(string vector)
    {
        ErrorFrameVector v = FmtGolden.ErrorFrames[vector];
        AssertEx.BytesEqual(v.Expected, v.Encode(), $"error frame {v.Name}");

        (DfsErrorCode code, string message, DfsOperationId? op, bool retryable) = ErrorCodec.Decode(v.Expected);
        Assert.Equal(v.Code, code);
        Assert.Equal(v.Message, message);
        Assert.Equal(v.Op, op);
        Assert.Equal(v.Retryable, retryable);

        ErrorReplyMessage reply = MessageCodec.Decode<ErrorReplyMessage>(v.Expected);
        Assert.Equal(v.LeaderHint, reply.LeaderHint);
        Assert.Equal(v.Transaction, reply.TransactionId);
        AssertEx.BytesEqual(v.Expected, MessageCodec.Encode(reply), $"re-encoding of decoded {v.Name}");

        for (int cut = 0; cut < v.Expected.Length; cut++)
            Assert.Throws<DfsProtocolException>(() => MessageCodec.Decode(v.Expected[..cut]));
    }

    // ============================================================================================================
    // FMT-002 UnknownRequiredFeatureRejected
    // ============================================================================================================

    public static TheoryData<long, string> RequiredExtensionPlacements
    {
        get
        {
            var data = new TheoryData<long, string>();
            foreach (long id in new long[] { 0, 1, 127, 128, 65535, uint.MaxValue })
            {
                foreach (string position in new[] { "only", "first", "middle", "last" })
                    data.Add(id, position);
            }

            return data;
        }
    }

    /// <summary>
    /// A message whose extension block contains a required field this reader does not understand is rejected with
    /// <see cref="DfsErrorCode.Unsupported"/> — wherever the field sits among optional ones and whatever its id — and never
    /// decoded with the field ignored. The same field marked optional decodes to the unchanged base message.
    /// </summary>
    [Theory, Catalog("FMT-002", "U", "INV-01", "INV-06", "INV-08", "INV-10", "INV-18")]
    [MemberData(nameof(RequiredExtensionPlacements))]
    public void FMT_002_UnknownRequiredFeatureRejected(long fieldId, string position)
    {
        var required = new WireExtension((uint)fieldId, Required: true, new byte[] { 0x01, 0x02, 0x03 });
        WireExtension[] optional =
        [
            new(2001, false, new byte[] { 0xAA }),
            new(2002, false, Array.Empty<byte>()),
            new(2003, false, new byte[] { 0xBB, 0xCC }),
        ];
        WireExtension[] block = position switch
        {
            "only" => [required],
            "first" => [required, .. optional],
            "middle" => [optional[0], required, optional[1], optional[2]],
            "last" => [.. optional, required],
            _ => throw new ArgumentOutOfRangeException(nameof(position)),
        };

        byte[] frame = FmtHelpers.Concat(FmtGolden.OutcomeUnknownFrameWithoutExtensions, FmtHelpers.Encode((ref WireWriter w) => w.WriteExtensions(block)));
        FmtHelpers.FrameDecodeFails(frame, DfsErrorCode.Unsupported, $"required unknown extension {fieldId} ({position})");
        DfsProtocolException viaException = Assert.Throws<DfsProtocolException>(() => ErrorCodec.DecodeToException(frame));
        Assert.Equal(DfsErrorCode.Unsupported, viaException.ErrorCode);

        // Control: the identical block with the field optional decodes to exactly the base error reply.
        WireExtension[] relaxed = block.Select(e => e with { Required = false }).ToArray();
        byte[] relaxedFrame = FmtHelpers.Concat(FmtGolden.OutcomeUnknownFrameWithoutExtensions, FmtHelpers.Encode((ref WireWriter w) => w.WriteExtensions(relaxed)));
        Assert.Equal(ErrorCodec.Decode(FmtGolden.OutcomeUnknownFrame), ErrorCodec.Decode(relaxedFrame));
    }

    /// <summary>
    /// Reader contract for extension blocks: understood required fields are accepted and delivered with their exact bytes,
    /// unknown optional ones are delivered in wire order, and an unknown required field is refused even when other fields
    /// of the same block are understood (understanding one field never licenses ignoring another).
    /// </summary>
    [Fact, Catalog("FMT-002", "U", "INV-01", "INV-06", "INV-08", "INV-10", "INV-18")]
    public void FMT_002_UnknownRequiredFeatureRejected_ExtensionReaderContract()
    {
        WireExtension[] extensions =
        [
            new WireExtension(7, true, new byte[] { 0x01 }),
            new WireExtension(8, false, new byte[] { 0x02, 0x03 }),
            new WireExtension(9, true, new byte[] { 0x04 }),
        ];
        byte[] block = FmtHelpers.Encode((ref WireWriter w) => w.WriteExtensions(extensions));
        // Hand-computed: count 3; (07,01,[01 01]); (08,00,[02 02 03]); (09,01,[01 04]).
        AssertEx.BytesEqual(new byte[] { 0x03, 0x07, 0x01, 0x01, 0x01, 0x08, 0x00, 0x02, 0x02, 0x03, 0x09, 0x01, 0x01, 0x04 }, block, "extension block");

        Assert.Equal("7:1:01|8:0:0203|9:1:04", FmtHelpers.DecodeExact(block, (ref WireReader r) => FmtHelpers.ReadExtensionBlock(ref r, 7, 9)));
        FmtHelpers.DecodeFails(block, (ref WireReader r) => FmtHelpers.ReadExtensionBlock(ref r), DfsErrorCode.Unsupported, because: "no field understood");
        FmtHelpers.DecodeFails(block, (ref WireReader r) => FmtHelpers.ReadExtensionBlock(ref r, 7), DfsErrorCode.Unsupported, because: "required field 9 not understood");
        FmtHelpers.DecodeFails(block, (ref WireReader r) => FmtHelpers.ReadExtensionBlock(ref r, 8, 9), DfsErrorCode.Unsupported, because: "required field 7 not understood");
        FmtHelpers.DecodeFails(block, (ref WireReader r) =>
        {
            r.ReadExtensions();
            return null;
        }, DfsErrorCode.Unsupported, because: "parameterless ReadExtensions understands nothing");
    }

    public static TheoryData<string> RawPathKinds => FmtHelpers.Names(["CreateDirectory", "WriteAll", "Delete"]);

    /// <summary>
    /// End to end: a namespace request carrying a required extension the server does not understand is refused with
    /// <see cref="DfsErrorCode.Unsupported"/> and has no effect (no entry created, deleted or replaced, inode count
    /// unchanged); the same request with the field marked optional is processed normally, proving the refusal came from the
    /// required flag.
    /// </summary>
    [Theory, Catalog("FMT-002", "U", "INV-01", "INV-06", "INV-08", "INV-10", "INV-18")]
    [MemberData(nameof(RawPathKinds))]
    public async Task FMT_002_UnknownRequiredFeatureRejected_ServerRefusesRequest(string kindName)
    {
        DfsOperationKind kind = Enum.Parse<DfsOperationKind>(kindName);
        await ClusterTest.RunAsync(TestClusterOptions.ThreeZones, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            DfsVolume vol = client.GetVolume(Vol);
            await vol.Directories.CreateDirectoryAsync("/fmt002", Ct);
            string path = "/fmt002/" + kindName.ToLowerInvariant();
            byte[] original = TestData.Bytes(2, 6000);
            DfsFileInfo? before = null;
            if (kind == DfsOperationKind.Delete)
            {
                await vol.Files.WriteAllBytesAsync(path, original, Ct);
                before = await vol.Files.GetInfoAsync(path, Ct);
            }

            int inodesBefore = (await cluster.Inspect.GetVolumeStateAsync(Vol)).InodeCount;
            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            await using RawProtocolClient raw = await cluster.ConnectRawAsync(leader.Name, Ct);
            Assert.Equal(DfsErrorCode.None, await raw.AuthenticateAsync(cluster.AdminCredentials));

            var required = new WireExtension(0x00F0_0D01, Required: true, new byte[] { 0x01, 0x02, 0x03 });
            DfsErrorCode refused = await raw.SendPathOperationWithExtensionsAsync(Vol, kind, path, [required]);
            Assert.Equal(DfsErrorCode.Unsupported, refused);

            switch (kind)
            {
                case DfsOperationKind.CreateDirectory:
                    Assert.False(await vol.Directories.ExistsAsync(path, Ct), "a refused CreateDirectory must not create the directory");
                    break;
                case DfsOperationKind.WriteAll:
                    Assert.False(await vol.Files.ExistsAsync(path, Ct), "a refused WriteAll must not create the file");
                    break;
                default:
                    DfsFileInfo after = await vol.Files.GetInfoAsync(path, Ct);
                    Assert.Equal(before!.InodeId, after.InodeId);
                    Assert.Equal(before.Revision, after.Revision);
                    FmtHelpers.BytesEqual(original, await vol.Files.ReadAllBytesAsync(path, Ct), "file after a refused Delete");
                    break;
            }

            Assert.Equal(inodesBefore, (await cluster.Inspect.GetVolumeStateAsync(Vol)).InodeCount);

            DfsErrorCode accepted = await raw.SendPathOperationWithExtensionsAsync(Vol, kind, path, [required with { Required = false }]);
            Assert.Equal(DfsErrorCode.None, accepted);
            switch (kind)
            {
                case DfsOperationKind.CreateDirectory:
                    Assert.True(await vol.Directories.ExistsAsync(path, Ct));
                    break;
                case DfsOperationKind.WriteAll:
                    Assert.True(await vol.Files.ExistsAsync(path, Ct));
                    break;
                default:
                    Assert.False(await vol.Files.ExistsAsync(path, Ct));
                    break;
            }
        }, Ct);
    }

    // ============================================================================================================
    // FMT-003 OptionalFieldsRemainCompatible
    // ============================================================================================================

    public static TheoryData<string> OptionalExtensionShapes =>
        FmtHelpers.Names(["empty-value", "one-byte", "1000-bytes", "20-fields", "max-field-id", "looks-like-a-frame"]);

    private static WireExtension[] OptionalShape(string shape) => shape switch
    {
        "empty-value" => [new WireExtension(500, false, Array.Empty<byte>())],
        "one-byte" => [new WireExtension(501, false, new byte[] { 0x7F })],
        "1000-bytes" => [new WireExtension(502, false, TestData.Bytes(502, 1000))],
        "20-fields" => Enumerable.Range(0, 20).Select(i => new WireExtension((uint)(600 + i), false, TestData.Bytes((ulong)i, i))).ToArray(),
        "max-field-id" => [new WireExtension(uint.MaxValue, false, new byte[] { 0x01, 0x02 })],
        // An optional value that itself looks like a complete error frame must not be interpreted.
        "looks-like-a-frame" => [new WireExtension(503, false, FmtGolden.ErrorFrames["file-not-found"].Expected)],
        _ => throw new ArgumentOutOfRangeException(nameof(shape)),
    };

    /// <summary>
    /// A newer peer may append optional fields: this reader decodes the message to exactly the same code, message,
    /// operation identity, retryability, leader hint and transaction id, produces the same typed exception, and
    /// re-encodes the defined fields byte-identically to the base frame (identity and checksum of the defined content
    /// are unchanged).
    /// </summary>
    [Theory, Catalog("FMT-003", "U", "INV-01", "INV-06", "INV-08", "INV-10", "INV-18")]
    [MemberData(nameof(OptionalExtensionShapes))]
    public void FMT_003_OptionalFieldsRemainCompatible(string shape)
    {
        WireExtension[] extensions = OptionalShape(shape);
        byte[] frame = FmtHelpers.Concat(FmtGolden.OutcomeUnknownFrameWithoutExtensions, FmtHelpers.Encode((ref WireWriter w) => w.WriteExtensions(extensions)));

        Assert.Equal(ErrorCodec.Decode(FmtGolden.OutcomeUnknownFrame), ErrorCodec.Decode(frame));
        ErrorReplyMessage reply = MessageCodec.Decode<ErrorReplyMessage>(frame);
        ErrorReplyMessage baseline = MessageCodec.Decode<ErrorReplyMessage>(FmtGolden.OutcomeUnknownFrame);
        Assert.Equal(baseline, reply);
        byte[] reencoded = MessageCodec.Encode(reply);
        AssertEx.BytesEqual(FmtGolden.OutcomeUnknownFrame, reencoded, "defined fields re-encoded after ignoring optional extensions");
        Assert.Equal(ObjectChecksum.Compute(FmtGolden.OutcomeUnknownFrame), ObjectChecksum.Compute(reencoded));

        Exception ex = ErrorCodec.DecodeToException(frame);
        DfsCommitOutcomeUnknownException unknown = Assert.IsAssignableFrom<DfsCommitOutcomeUnknownException>(ex);
        Assert.Equal(new DfsOperationId(0x1234, 1), unknown.OperationId);
        Assert.False(unknown.IsRetryable);
    }

    /// <summary>A relay that reads optional extensions it does not understand can forward them byte-identically (values and order preserved).</summary>
    [Fact, Catalog("FMT-003", "U", "INV-01", "INV-06", "INV-08", "INV-10", "INV-18")]
    public void FMT_003_OptionalFieldsRemainCompatible_UnknownOptionalFieldsForwardedVerbatim()
    {
        WireExtension[] original = [.. OptionalShape("20-fields"), .. OptionalShape("1000-bytes"), .. OptionalShape("max-field-id")];
        byte[] block = FmtHelpers.Encode((ref WireWriter w) => w.WriteExtensions(original));

        var delivered = new List<WireExtension>();
        var reader = new WireReader(block, WireLimits.Default);
        reader.ReadExtensions(new HashSet<uint>(), delivered.Add);
        reader.EnsureEnd();

        Assert.Equal(original.Length, delivered.Count);
        Assert.Equal(FmtHelpers.Describe(original), FmtHelpers.Describe(delivered));
        WireExtension[] forwarded = delivered.ToArray();
        AssertEx.BytesEqual(block, FmtHelpers.Encode((ref WireWriter w) => w.WriteExtensions(forwarded)), "forwarded extension block");
    }

    /// <summary>
    /// An error code introduced by a newer version is carried verbatim: the exception reports the exact code, operation id
    /// and retryability; it never degrades into Internal, not-found or cancellation.
    /// </summary>
    [Theory, Catalog("FMT-003", "U", "INV-01", "INV-06", "INV-08", "INV-10", "INV-18")]
    [InlineData(48)]
    [InlineData(100)]
    [InlineData(1000)]
    [InlineData(int.MaxValue)]
    public void FMT_003_OptionalFieldsRemainCompatible_FutureErrorCodePreserved(int rawCode)
    {
        var code = (DfsErrorCode)rawCode;
        Assert.False(Enum.IsDefined(code), "the code must be unknown to this version for the test to be meaningful");
        var op = new DfsOperationId(0x55, 0x66);
        byte[] frame = ErrorCodec.Encode(code, "from a newer server", op, retryable: true);

        Assert.Equal((code, "from a newer server", (DfsOperationId?)op, true), ErrorCodec.Decode(frame));
        Exception ex = ErrorCodec.DecodeToException(frame, "/p");
        DfsException dfs = Assert.IsType<DfsException>(ex);
        Assert.Equal(code, dfs.ErrorCode);
        Assert.Equal(op, dfs.OperationId);
        Assert.True(dfs.IsRetryable);
        Assert.Equal(code, DfsErrors.ToCode(ex));
        Assert.NotEqual(DfsErrorCode.Internal, DfsErrors.ToCode(ex));
    }

    /// <summary>
    /// End to end: namespace requests carrying unknown optional extensions are processed exactly like the same requests
    /// without them — the result is observably identical to an extension-free control request, inode identity and
    /// revisions follow commit order, and every replica converges to the same state digest.
    /// </summary>
    [Theory, Catalog("FMT-003", "U", "INV-01", "INV-06", "INV-08", "INV-10", "INV-18")]
    [InlineData("CreateDirectory")]
    [InlineData("WriteAll")]
    public async Task FMT_003_OptionalFieldsRemainCompatible_ServerIgnoresUnknownOptionalFields(string kindName)
    {
        DfsOperationKind kind = Enum.Parse<DfsOperationKind>(kindName);
        await ClusterTest.RunAsync(TestClusterOptions.ThreeZones, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct);
            DfsVolume vol = client.GetVolume(Vol);
            await vol.Directories.CreateDirectoryAsync("/fmt003", Ct);
            TestNode leader = await cluster.GetLeaderAsync(Vol, Ct);
            await using RawProtocolClient raw = await cluster.ConnectRawAsync(leader.Name, Ct);
            Assert.Equal(DfsErrorCode.None, await raw.AuthenticateAsync(cluster.AdminCredentials));

            string control = $"/fmt003/{kindName}-control";
            string extended = $"/fmt003/{kindName}-extended";
            Assert.Equal(DfsErrorCode.None, await raw.SendPathOperationAsync(Vol, kind, control));
            WireExtension[] optional = [.. OptionalShape("20-fields"), .. OptionalShape("1000-bytes"), .. OptionalShape("looks-like-a-frame")];
            Assert.Equal(DfsErrorCode.None, await raw.SendPathOperationWithExtensionsAsync(Vol, kind, extended, optional));

            if (kind == DfsOperationKind.CreateDirectory)
            {
                DfsDirectoryInfo c = await vol.Directories.GetInfoAsync(control, Ct);
                DfsDirectoryInfo e = await vol.Directories.GetInfoAsync(extended, Ct);
                Assert.NotEqual(c.InodeId, e.InodeId);
                Assert.True(e.Revision > c.Revision, "the later request must have the later revision");
                Assert.Equal(c.EntryCount, e.EntryCount);
            }
            else
            {
                DfsFileInfo c = await vol.Files.GetInfoAsync(control, Ct);
                DfsFileInfo e = await vol.Files.GetInfoAsync(extended, Ct);
                Assert.NotEqual(c.InodeId, e.InodeId);
                Assert.True(e.Revision > c.Revision, "the later request must have the later revision");
                Assert.Equal(c.Length, e.Length);
                Assert.Equal(c.IsInline, e.IsInline);
                byte[] controlBytes = await vol.Files.ReadAllBytesAsync(control, Ct);
                FmtHelpers.BytesEqual(controlBytes, await vol.Files.ReadAllBytesAsync(extended, Ct), "content written with optional extensions");
            }

            await cluster.WaitForReplicationAsync(Vol, Ct);
            await cluster.Inspect.AssertReplicasConvergedAsync(Vol);
        }, Ct);
    }

    // ============================================================================================================
    // FMT-004 EndianAndIntegerBoundaries
    // ============================================================================================================

    /// <summary>
    /// Fixed-width integers are little-endian on every architecture: the encoding equals an explicit little-endian
    /// oracle (not the host byte order) at zero, one, sign/high-bit boundaries and maxima, and decodes back exactly.
    /// </summary>
    [Fact, Catalog("FMT-004", "U", "INV-01", "INV-06", "INV-08", "INV-10", "INV-18")]
    public void FMT_004_EndianAndIntegerBoundaries()
    {
        FmtHelpers.Log($"Host is little-endian: {BitConverter.IsLittleEndian}; the wire must not depend on it.");
        foreach (ushort v in new ushort[] { 0, 1, 0x00FF, 0x0100, 0x1234, 0x7FFF, 0x8000, ushort.MaxValue })
        {
            byte[] expected = [(byte)v, (byte)(v >> 8)];
            AssertEx.BytesEqual(expected, FmtHelpers.Encode((ref WireWriter w) => w.WriteUInt16(v)), $"u16 {v}");
            Assert.Equal(v, (ushort)FmtHelpers.DecodeExact(expected, (ref WireReader r) => r.ReadUInt16())!);
        }

        foreach (uint v in new uint[] { 0, 1, 0xFF, 0x100, 0x12345678, 0x7FFFFFFF, 0x80000000, uint.MaxValue })
        {
            byte[] expected = [(byte)v, (byte)(v >> 8), (byte)(v >> 16), (byte)(v >> 24)];
            AssertEx.BytesEqual(expected, FmtHelpers.Encode((ref WireWriter w) => w.WriteUInt32(v)), $"u32 {v}");
            Assert.Equal(v, (uint)FmtHelpers.DecodeExact(expected, (ref WireReader r) => r.ReadUInt32())!);
        }

        foreach (ulong v in new ulong[] { 0, 1, 0xFF, 0x100, 0x0102030405060708, 0x7FFFFFFFFFFFFFFF, 0x8000000000000000, ulong.MaxValue })
        {
            byte[] expected = new byte[8];
            for (int i = 0; i < 8; i++)
                expected[i] = (byte)(v >> (8 * i));
            AssertEx.BytesEqual(expected, FmtHelpers.Encode((ref WireWriter w) => w.WriteUInt64(v)), $"u64 {v}");
            Assert.Equal(v, (ulong)FmtHelpers.DecodeExact(expected, (ref WireReader r) => r.ReadUInt64())!);
        }

        foreach (long v in new long[] { 0, 1, -1, -2, 255, -256, long.MaxValue, long.MinValue, long.MinValue + 1 })
        {
            byte[] expected = new byte[8];
            ulong bits = unchecked((ulong)v);
            for (int i = 0; i < 8; i++)
                expected[i] = (byte)(bits >> (8 * i));
            AssertEx.BytesEqual(expected, FmtHelpers.Encode((ref WireWriter w) => w.WriteInt64(v)), $"i64 {v}");
            Assert.Equal(v, (long)FmtHelpers.DecodeExact(expected, (ref WireReader r) => r.ReadInt64())!);
        }

        // 128-bit values: least significant byte first; GUIDs: Data1/Data2/Data3 little-endian, Data4 as written.
        UInt128 big = (UInt128.One << 127) | UInt128.One;
        byte[] bigBytes = new byte[16];
        bigBytes[0] = 0x01;
        bigBytes[15] = 0x80;
        AssertEx.BytesEqual(bigBytes, FmtHelpers.Encode((ref WireWriter w) => w.WriteUInt128(big)), "u128 2^127+1");
        Assert.Equal(big, (UInt128)FmtHelpers.DecodeExact(bigBytes, (ref WireReader r) => r.ReadUInt128())!);
        AssertEx.BytesEqual(FmtGolden.SampleGuidBytes, FmtHelpers.Encode((ref WireWriter w) => w.WriteGuid(FmtGolden.SampleGuid)), "guid layout");
    }

    /// <summary>
    /// Unsigned LEB128 at every bit length 0..64 (2^k-1, 2^k, 2^k+1): the encoding equals an independent reference encoder,
    /// has the minimal length ceil(bits/7) (at least one byte, at most ten) and decodes back exactly.
    /// </summary>
    [Fact, Catalog("FMT-004", "U", "INV-01", "INV-06", "INV-08", "INV-10", "INV-18")]
    public void FMT_004_EndianAndIntegerBoundaries_VarUIntEveryBitLength()
    {
        var values = new SortedSet<ulong> { 0, ulong.MaxValue };
        for (int k = 0; k < 64; k++)
        {
            ulong p = 1UL << k;
            values.Add(p);
            values.Add(p - 1);
            values.Add(p + 1);
        }

        foreach (ulong v in values)
        {
            byte[] expected = ReferenceLeb128(v);
            int bits = 64 - BitOperations.LeadingZeroCount(v);
            Assert.Equal(Math.Max(1, (bits + 6) / 7), expected.Length);
            Assert.InRange(expected.Length, 1, 10);
            AssertEx.BytesEqual(expected, FmtHelpers.Encode((ref WireWriter w) => w.WriteVarUInt(v)), $"varuint {v}");
            Assert.Equal(v, (ulong)FmtHelpers.DecodeExact(expected, (ref WireReader r) => r.ReadVarUInt())!);
        }
    }

    /// <summary>
    /// Zigzag signed varints at ±2^k, ±(2^k-1), zero and the 64-bit extremes: the encoding equals the reference zigzag
    /// mapping (n ≥ 0 → 2n, n &lt; 0 → −2n−1) followed by LEB128, and decodes back exactly.
    /// </summary>
    [Fact, Catalog("FMT-004", "U", "INV-01", "INV-06", "INV-08", "INV-10", "INV-18")]
    public void FMT_004_EndianAndIntegerBoundaries_ZigZagBoundaries()
    {
        var values = new SortedSet<long> { 0, 1, -1, long.MaxValue, long.MinValue, long.MinValue + 1, long.MaxValue - 1 };
        for (int k = 0; k < 63; k++)
        {
            long p = 1L << k;
            values.Add(p);
            values.Add(-p);
            values.Add(p - 1);
            values.Add(-(p - 1));
            values.Add(-p - 1);
        }

        foreach (long v in values)
        {
            ulong zig = v >= 0 ? (ulong)v * 2 : (ulong)(-(v + 1)) * 2 + 1;
            byte[] expected = ReferenceLeb128(zig);
            AssertEx.BytesEqual(expected, FmtHelpers.Encode((ref WireWriter w) => w.WriteVarInt(v)), $"varint {v}");
            Assert.Equal(v, (long)FmtHelpers.DecodeExact(expected, (ref WireReader r) => r.ReadVarInt())!);
        }
    }

    public static TheoryData<string, string> MalformedVarints => new()
    {
        { "empty", "" },
        { "truncated-1", "80" },
        { "truncated-2", "FFFF" },
        { "truncated-9", "808080808080808080" },
        { "overflow-bit-64", "FFFFFFFFFFFFFFFFFF02" },
        { "overflow-high-bits", "FFFFFFFFFFFFFFFFFF7F" },
        { "ten-bytes-continuation", "80808080808080808080" },
        { "eleven-bytes", "FFFFFFFFFFFFFFFFFF8101" },
        { "non-minimal-zero", "8000" },
        { "non-minimal-127", "FF00" },
        { "non-minimal-3-bytes", "808000" },
        { "non-minimal-10-bytes", "80808080808080808000" },
    };

    /// <summary>
    /// Malformed varints — truncated, longer than ten bytes, overflowing 64 bits, or non-minimal — are rejected as protocol
    /// errors by both the unsigned and the zigzag reader, never wrapped or silently accepted.
    /// </summary>
    [Theory, Catalog("FMT-004", "U", "INV-01", "INV-06", "INV-08", "INV-10", "INV-18")]
    [MemberData(nameof(MalformedVarints))]
    public void FMT_004_EndianAndIntegerBoundaries_MalformedVarintsRejected(string name, string hex)
    {
        byte[] bytes = Convert.FromHexString(hex);
        FmtHelpers.DecodeFails(bytes, (ref WireReader r) => r.ReadVarUInt(), DfsErrorCode.ProtocolError, because: name);
        FmtHelpers.DecodeFails(bytes, (ref WireReader r) => r.ReadVarInt(), DfsErrorCode.ProtocolError, because: name);
    }

    /// <summary>Truncated fixed-width fields of every width are rejected as protocol errors, never zero-filled.</summary>
    [Theory, Catalog("FMT-004", "U", "INV-01", "INV-06", "INV-08", "INV-10", "INV-18")]
    [InlineData("byte", 0)]
    [InlineData("bool", 0)]
    [InlineData("u16", 1)]
    [InlineData("u32", 3)]
    [InlineData("u64", 7)]
    [InlineData("i64", 7)]
    [InlineData("guid", 15)]
    [InlineData("u128", 15)]
    public void FMT_004_EndianAndIntegerBoundaries_TruncatedFixedWidthRejected(string width, int available)
    {
        byte[] bytes = FmtHelpers.Repeat(0x01, available);
        WireRead read = width switch
        {
            "byte" => (ref WireReader r) => r.ReadByte(),
            "bool" => (ref WireReader r) => r.ReadBool(),
            "u16" => (ref WireReader r) => r.ReadUInt16(),
            "u32" => (ref WireReader r) => r.ReadUInt32(),
            "u64" => (ref WireReader r) => r.ReadUInt64(),
            "i64" => (ref WireReader r) => r.ReadInt64(),
            "guid" => (ref WireReader r) => r.ReadGuid(),
            "u128" => (ref WireReader r) => r.ReadUInt128(),
            _ => throw new ArgumentOutOfRangeException(nameof(width)),
        };
        FmtHelpers.DecodeFails(bytes, read, DfsErrorCode.ProtocolError, because: $"{width} with {available} bytes");
    }

    /// <summary>
    /// Length prefixes at the boundary: a length equal to the remaining bytes and to the configured limit is accepted; one
    /// more than either, or an enormous (2^63) length, is rejected before any allocation of that size.
    /// </summary>
    [Fact, Catalog("FMT-004", "U", "INV-01", "INV-06", "INV-08", "INV-10", "INV-18")]
    public void FMT_004_EndianAndIntegerBoundaries_LengthPrefixBoundaries()
    {
        var small = new WireLimits(MaxStringBytes: 4, MaxBytesLength: 4, MaxCollectionCount: 4);
        WireRead bytesRead = (ref WireReader r) => r.ReadBytes().ToArray();
        WireRead stringRead = (ref WireReader r) => r.ReadString();

        Assert.True(FmtHelpers.ValuesEqual(new byte[] { 0xAA, 0xBB }, FmtHelpers.DecodeExact([0x02, 0xAA, 0xBB], bytesRead)));
        FmtHelpers.DecodeFails([0x03, 0xAA, 0xBB], bytesRead, DfsErrorCode.ProtocolError, because: "length exceeds remaining");
        Assert.True(FmtHelpers.ValuesEqual(new byte[] { 1, 2, 3, 4 }, FmtHelpers.DecodeExact([0x04, 1, 2, 3, 4], bytesRead, small)));
        FmtHelpers.DecodeFails([0x05, 1, 2, 3, 4, 5], bytesRead, DfsErrorCode.ProtocolError, small, "length exceeds MaxBytesLength");
        Assert.Equal("aaaa", FmtHelpers.DecodeExact([0x04, 0x61, 0x61, 0x61, 0x61], stringRead, small));
        FmtHelpers.DecodeFails([0x05, 0x61, 0x61, 0x61, 0x61, 0x61], stringRead, DfsErrorCode.ProtocolError, small, "length exceeds MaxStringBytes");

        byte[] huge = FmtHelpers.Concat([0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01], [0x00]);
        FmtHelpers.DecodeFails(huge, bytesRead, DfsErrorCode.ProtocolError, because: "2^63 byte length");
        FmtHelpers.DecodeFails(huge, stringRead, DfsErrorCode.ProtocolError, because: "2^63 string length");
    }

    public static TheoryData<string, string> InvalidUtf8 => new()
    {
        { "bad-continuation", "02C328" },
        { "overlong-slash", "02C0AF" },
        { "overlong-3-byte", "03E080AF" },
        { "utf16-surrogate", "03EDA080" },
        { "beyond-10FFFF", "04F4908080" },
        { "invalid-byte-FF", "01FF" },
        { "truncated-sequence", "02E282" },
        { "lone-continuation", "0180" },
    };

    /// <summary>Strings (paths included) must be strict UTF-8: malformed or overlong sequences and encoded surrogates are protocol errors, never replaced.</summary>
    [Theory, Catalog("FMT-004", "U", "INV-01", "INV-06", "INV-08", "INV-10", "INV-18")]
    [MemberData(nameof(InvalidUtf8))]
    public void FMT_004_EndianAndIntegerBoundaries_InvalidUtf8Rejected(string name, string hex)
    {
        FmtHelpers.DecodeFails(Convert.FromHexString(hex), (ref WireReader r) => r.ReadString(), DfsErrorCode.ProtocolError, because: name);
    }

    // Unpaired surrogates cannot travel through theory-data serialization intact, so the values are built here and indexed.
    private static readonly string[] UnpairedSurrogateStrings =
    [
        "a" + FmtHelpers.Chars(0xD800),
        FmtHelpers.Chars(0xDC00) + "b",
        "/x" + FmtHelpers.Chars(0xD83D) + "/y",
        "/" + FmtHelpers.Chars(0xDE00),
    ];

    /// <summary>The encoder refuses strings with unpaired surrogates instead of substituting replacement characters.</summary>
    [Theory, Catalog("FMT-004", "U", "INV-01", "INV-06", "INV-08", "INV-10", "INV-18")]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public void FMT_004_EndianAndIntegerBoundaries_UnpairedSurrogateNotEncoded(int index)
    {
        string value = UnpairedSurrogateStrings[index];
        Assert.True(value.Any(char.IsSurrogate), "the test value must contain an unpaired surrogate");
        Assert.ThrowsAny<ArgumentException>(() => FmtHelpers.Encode((ref WireWriter w) => w.WriteString(value)));
    }

    /// <summary>Booleans are exactly one byte 0 or 1; any other value is malformed rather than truthy.</summary>
    [Theory, Catalog("FMT-004", "U", "INV-01", "INV-06", "INV-08", "INV-10", "INV-18")]
    [InlineData(2)]
    [InlineData(0x80)]
    [InlineData(0xFF)]
    public void FMT_004_EndianAndIntegerBoundaries_BooleanOnlyZeroOrOne(int value)
    {
        FmtHelpers.DecodeFails([(byte)value], (ref WireReader r) => r.ReadBool(), DfsErrorCode.ProtocolError, because: $"bool byte {value}");
    }

    /// <summary>
    /// A 32-bit error code field carried as a 64-bit varint: values within int range (including int.MinValue and
    /// int.MaxValue) round-trip; a value beyond the 32-bit range is a protocol error rather than being truncated.
    /// </summary>
    [Fact, Catalog("FMT-004", "U", "INV-01", "INV-06", "INV-08", "INV-10", "INV-18")]
    public void FMT_004_EndianAndIntegerBoundaries_ErrorCodeRange()
    {
        byte[] tail = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00]; // empty message, no op, not retryable, no hint, no tx, no extensions
        foreach (long code in new[] { (long)int.MinValue, -1L, 0L, (long)int.MaxValue })
        {
            byte[] frame = FmtHelpers.Concat([0x01, 0x00], FmtHelpers.Encode((ref WireWriter w) => w.WriteVarInt(code)), tail);
            Assert.Equal((DfsErrorCode)(int)code, ErrorCodec.Decode(frame).Code);
        }

        // zigzag(int.MaxValue) = 0xFFFFFFFE = FE FF FF FF 0F (hand-computed).
        AssertEx.BytesEqual(new byte[] { 0xFE, 0xFF, 0xFF, 0xFF, 0x0F }, FmtHelpers.Encode((ref WireWriter w) => w.WriteVarInt(int.MaxValue)), "zigzag int.MaxValue");

        foreach (long code in new[] { (long)int.MaxValue + 1, (long)int.MinValue - 1, 1L << 40, long.MinValue })
        {
            byte[] frame = FmtHelpers.Concat([0x01, 0x00], FmtHelpers.Encode((ref WireWriter w) => w.WriteVarInt(code)), tail);
            FmtHelpers.FrameDecodeFails(frame, DfsErrorCode.ProtocolError, $"error code {code} outside the 32-bit range");
        }
    }

    // ============================================================================================================
    // FMT-011 ErrorSerializationPreservesOutcome
    // ============================================================================================================

    /// <summary>
    /// Every error code, with and without an operation id and with both retryability values, survives the error frame
    /// exactly; the recreated exception has the canonical CLR type for the code (API.md A.2 standard mappings), the same
    /// code, operation id and retryability, and only FileNotFound/DirectoryNotFound/Cancelled codes may surface as
    /// not-found or cancellation.
    /// </summary>
    [Theory, Catalog("FMT-011", "U", "INV-01", "INV-06", "INV-08", "INV-10", "INV-18")]
    [MemberData(nameof(AllErrorCodes))]
    public void FMT_011_ErrorSerializationPreservesOutcome(int rawCode)
    {
        var code = (DfsErrorCode)rawCode;
        foreach (bool retryable in new[] { false, true })
        {
            foreach (DfsOperationId? op in new DfsOperationId?[] { null, new DfsOperationId(0x77, 0x99) })
            {
                string message = $"m-{code}-{retryable}";
                byte[] frame = ErrorCodec.Encode(code, message, op, retryable);
                Assert.Equal((code, message, op, retryable), ErrorCodec.Decode(frame));

                Exception ex = ErrorCodec.DecodeToException(frame, "/fmt011/p");
                Assert.IsType(ExpectedExceptionType(code), ex);
                Assert.Equal(code, DfsErrors.ToCode(ex));
                Assert.Equal(retryable, DfsErrors.IsRetryable(ex));
                if (op is not null)
                    Assert.Equal(op, DfsErrors.GetOperationId(ex));
                else
                    Assert.True(DfsErrors.GetOperationId(ex) is null or { IsEmpty: true }, $"{code}: no operation id was sent");

                if (code is not (DfsErrorCode.FileNotFound or DfsErrorCode.DirectoryNotFound))
                    Assert.False(ex is FileNotFoundException or DirectoryNotFoundException, $"{code} degraded into a not-found exception");
                if (code != DfsErrorCode.Cancelled)
                    Assert.False(ex is OperationCanceledException, $"{code} degraded into a cancellation");
                if (code == DfsErrorCode.OutcomeUnknown)
                {
                    Assert.Equal(OpOutcome.Unknown, HistoryRecorder.Classify(ex));
                    if (op is not null)
                        Assert.Equal(op.Value, Assert.IsAssignableFrom<DfsCommitOutcomeUnknownException>(ex).OperationId);
                }
            }
        }
    }

    /// <summary>
    /// Exceptions created through the canonical mapping keep their type, code, operation identity and retryability across
    /// <see cref="ErrorReplyMessage.FromException"/> → frame → <see cref="ErrorCodec.DecodeToException"/>.
    /// </summary>
    [Theory, Catalog("FMT-011", "U", "INV-01", "INV-06", "INV-08", "INV-10", "INV-18")]
    [MemberData(nameof(AllErrorCodes))]
    public void FMT_011_ErrorSerializationPreservesOutcome_TypedExceptionRoundTrip(int rawCode)
    {
        var code = (DfsErrorCode)rawCode;
        var op = new DfsOperationId(0x10, 0x20);
        Exception original = DfsErrors.ToException(code, $"original {code}", "/fmt011/q", op);

        byte[] frame = MessageCodec.Encode(ErrorReplyMessage.FromException(original));
        Exception decoded = ErrorCodec.DecodeToException(frame, "/fmt011/q");

        Assert.Equal(original.GetType(), decoded.GetType());
        Assert.Equal(DfsErrors.ToCode(original), DfsErrors.ToCode(decoded));
        Assert.Equal(DfsErrors.IsRetryable(original), DfsErrors.IsRetryable(decoded));
        Assert.Equal(DfsErrors.GetOperationId(original), DfsErrors.GetOperationId(decoded));
        Assert.Contains($"original {code}", decoded.Message, StringComparison.Ordinal);
        if (decoded is FileNotFoundException fnf)
            Assert.Equal("/fmt011/q", fnf.FileName);
    }

    /// <summary>
    /// An unknown transaction commit keeps its operation id and transaction id; a conflict keeps its transaction id; a
    /// NotLeader reply keeps its leader hint — none of them degrades into a definite failure.
    /// </summary>
    [Fact, Catalog("FMT-011", "U", "INV-01", "INV-06", "INV-08", "INV-10", "INV-18")]
    public void FMT_011_ErrorSerializationPreservesOutcome_TransactionAndLeaderContext()
    {
        var op = new DfsOperationId(3, 9);
        var tx = new DfsTransactionId(new DfsOperationId(3, 4));

        var unknown = new DfsCommitOutcomeUnknownException(op, tx, "commit outcome unknown");
        Exception decodedUnknown = ErrorCodec.DecodeToException(MessageCodec.Encode(ErrorReplyMessage.FromException(unknown)));
        DfsCommitOutcomeUnknownException u = Assert.IsType<DfsCommitOutcomeUnknownException>(decodedUnknown);
        Assert.Equal(op, u.OperationId);
        Assert.Equal(tx, u.TransactionId);
        Assert.Equal(OpOutcome.Unknown, HistoryRecorder.Classify(u));

        var conflict = new DfsTransactionConflictException(tx, "conflict");
        Exception decodedConflict = ErrorCodec.DecodeToException(MessageCodec.Encode(ErrorReplyMessage.FromException(conflict)));
        DfsTransactionConflictException c = Assert.IsType<DfsTransactionConflictException>(decodedConflict);
        Assert.Equal(tx, c.TransactionId);
        Assert.True(c.IsRetryable);

        var notLeader = new DfsUnavailableException(DfsErrorCode.NotLeader, "not leader") { LeaderHint = "mem://n2" };
        Exception decodedNotLeader = ErrorCodec.DecodeToException(MessageCodec.Encode(ErrorReplyMessage.FromException(notLeader)));
        DfsUnavailableException nl = Assert.IsType<DfsUnavailableException>(decodedNotLeader);
        Assert.Equal(DfsErrorCode.NotLeader, nl.ErrorCode);
        Assert.Equal("mem://n2", nl.LeaderHint);
        Assert.True(nl.IsRetryable);
    }

    /// <summary>
    /// End to end over the real request path: typed server errors arrive as their documented exception types, a sharing
    /// conflict is retryable, and a committed write whose every reply is lost surfaces as
    /// <see cref="DfsCommitOutcomeUnknownException"/> carrying the caller's operation id — not as not-found, cancellation
    /// or a timeout — and resolves to Committed with the written bytes.
    /// </summary>
    [Fact, Catalog("FMT-011", "U", "INV-01", "INV-06", "INV-08", "INV-10", "INV-18")]
    public async Task FMT_011_ErrorSerializationPreservesOutcome_TypedErrorsOverTheWire()
    {
        await ClusterTest.RunAsync(TestClusterOptions.ThreeZones, async cluster =>
        {
            await using DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0", OperationTimeout = TimeSpan.FromSeconds(3) }, Ct);
            await using DfsClient other = await cluster.ConnectAsync(new TestClientOptions { Name = "c1" }, Ct);
            DfsVolume vol = client.GetVolume(Vol);
            DfsVolume otherVol = other.GetVolume(Vol);
            await vol.Directories.CreateDirectoryAsync("/fmt011/dir", Ct);
            await vol.Files.WriteAllBytesAsync("/fmt011/dir/f", TestData.Bytes(11, 64), Ct);

            FileNotFoundException fnf = await AssertEx.ThrowsDfsAsync<FileNotFoundException>(() => vol.Files.GetInfoAsync("/fmt011/missing", Ct));
            Assert.Equal(DfsErrorCode.FileNotFound, DfsErrors.ToCode(fnf));
            DirectoryNotFoundException dnf = await AssertEx.ThrowsDfsAsync<DirectoryNotFoundException>(() => vol.Directories.GetInfoAsync("/fmt011/nodir", Ct));
            Assert.Equal(DfsErrorCode.DirectoryNotFound, DfsErrors.ToCode(dnf));
            await AssertEx.ThrowsDfsAsync<DfsAlreadyExistsException>(
                () => vol.Files.OpenAsync("/fmt011/dir/f", FileMode.CreateNew, FileAccess.Write, FileShare.None, Ct), DfsErrorCode.AlreadyExists);
            await AssertEx.ThrowsDfsAsync<DfsDirectoryNotEmptyException>(
                () => vol.Directories.DeleteAsync("/fmt011/dir", recursive: false, Ct), DfsErrorCode.DirectoryNotEmpty);

            await using (DfsStream writer = await vol.Files.OpenAsync("/fmt011/dir/f", FileMode.Open, FileAccess.Write, FileShare.None, Ct))
            {
                Assert.True(writer.CanWrite);
                DfsSharingViolationException sharing = await AssertEx.ThrowsDfsAsync<DfsSharingViolationException>(
                    () => otherVol.Files.OpenReadAsync("/fmt011/dir/f", Ct));
                Assert.Contains(sharing.ErrorCode, new[] { DfsErrorCode.SharingViolation, DfsErrorCode.WriterConflict });
                Assert.True(sharing.IsRetryable);
            }

            DfsOperationId id = await vol.NewOperationIdAsync(Ct);
            byte[] payload = TestData.Bytes(12, 10_000);
            // Every reply to c0 is lost (including any outcome query the SDK might try), so the client cannot learn the outcome.
            MessageRule drop = cluster.Network.DropReplies(new MessageFilter(From: "client:c0"), count: 100_000);
            Exception? failure = await Record.ExceptionAsync(
                () => vol.Files.WriteAllBytesAsync("/fmt011/unknown", payload, new DfsMutationOptions { OperationId = id }, Ct));
            drop.Cancel();
            Assert.True(drop.Matched >= 1, "the reply-drop rule must have matched the WriteAll request");
            Assert.True(cluster.Network.Log.CountMessages(new MessageFilter(From: "client:c0", Type: MessageType.WriteAll, Direction: LinkDirection.Request)) >= 1,
                "the WriteAll request must have been sent");
            Assert.NotNull(failure);
            Assert.False(failure is FileNotFoundException or OperationCanceledException or TimeoutException,
                $"a lost reply after commit must not surface as {failure.GetType().Name}");
            DfsCommitOutcomeUnknownException unknown = Assert.IsAssignableFrom<DfsCommitOutcomeUnknownException>(failure);
            Assert.Equal(id, unknown.OperationId);
            Assert.Equal(DfsErrorCode.OutcomeUnknown, unknown.ErrorCode);
            Assert.Equal(OpOutcome.Unknown, HistoryRecorder.Classify(unknown));

            DfsOperationOutcome outcome = await otherVol.QueryOperationAsync(id, resolve: true, Ct);
            Assert.Equal(id, outcome.Id);
            Assert.Equal(DfsOutcomeStatus.Committed, outcome.Status);
            Assert.False(outcome.Token.IsNone);
            FmtHelpers.BytesEqual(payload, await otherVol.Files.ReadAllBytesAsync("/fmt011/unknown", Ct), "committed bytes of the unknown-outcome write");
        }, Ct);
    }

    // ============================================================================================================
    // FMT-012 ManifestIdentityAndHashStable
    // ============================================================================================================

    public static TheoryData<string, string, int> NoncanonicalFrames => new()
    {
        // name, frame hex, expected error code. Canonical frame: 01002E017801B4240100000000.
        { "non-minimal-code", "0100AE00017801B4240100000000", (int)DfsErrorCode.ProtocolError },
        { "non-minimal-string-length", "01002E81007801B4240100000000", (int)DfsErrorCode.ProtocolError },
        { "non-minimal-session", "01002E017801B4A4000100000000", (int)DfsErrorCode.ProtocolError },
        { "presence-flag-2", "01002E017802B4240100000000", (int)DfsErrorCode.ProtocolError },
        { "retryable-flag-2", "01002E017801B4240102000000", (int)DfsErrorCode.ProtocolError },
        { "hint-flag-FF", "01002E017801B4240100FF0000", (int)DfsErrorCode.ProtocolError },
        { "reserved-extension-flag", "01002E017801B4240100000001050200", (int)DfsErrorCode.ProtocolError },
        { "duplicate-extension-id", "01002E017801B4240100000002050000050000", (int)DfsErrorCode.ProtocolError },
        { "non-minimal-extension-count", "01002E017801B424010000008000", (int)DfsErrorCode.ProtocolError },
        { "trailing-byte", "01002E017801B424010000000000", (int)DfsErrorCode.ProtocolError },
        { "truncated", "01002E017801B42401000000", (int)DfsErrorCode.ProtocolError },
        { "overlong-utf8-message", "01002E02C1B801B4240100000000", (int)DfsErrorCode.ProtocolError },
        { "unknown-message-type", "FFFF2E017801B4240100000000", (int)DfsErrorCode.ProtocolError },
        { "required-unknown-extension", "01002E017801B4240100000001050100", (int)DfsErrorCode.Unsupported },
    };

    /// <summary>
    /// The decoder accepts exactly one byte representation per message: each alternative encoding of the golden error
    /// frame (non-minimal varints, flag bytes other than 0/1, reserved extension bits, duplicate extension ids, trailing
    /// bytes, overlong UTF-8, unknown types, required unknown fields) is rejected, so two parsers can never disagree about
    /// which bytes denote a given manifest/message.
    /// </summary>
    [Theory, Catalog("FMT-012", "U", "INV-01", "INV-06", "INV-08", "INV-10", "INV-18")]
    [MemberData(nameof(NoncanonicalFrames))]
    public void FMT_012_ManifestIdentityAndHashStable_NoncanonicalRepresentationsRejected(string name, string hex, int expectedCode)
    {
        byte[] frame = Convert.FromHexString(hex);
        Assert.False(frame.AsSpan().SequenceEqual(FmtGolden.OutcomeUnknownFrame), $"{name} must differ from the canonical frame");
        FmtHelpers.FrameDecodeFails(frame, (DfsErrorCode)expectedCode, name);
    }

    /// <summary>
    /// Canonical encoding is unique and stable: decoding and re-encoding every golden frame reproduces the same bytes, and
    /// repeated encodings of equal values hash identically (content identity derives from the canonical bytes only).
    /// </summary>
    [Fact, Catalog("FMT-012", "U", "INV-01", "INV-06", "INV-08", "INV-10", "INV-18")]
    public void FMT_012_ManifestIdentityAndHashStable_CanonicalEncodingIsUnique()
    {
        foreach (ErrorFrameVector v in FmtGolden.ErrorFrames.Values)
        {
            byte[] reencoded = MessageCodec.Encode(MessageCodec.Decode(v.Expected));
            AssertEx.BytesEqual(v.Expected, reencoded, $"re-encoding {v.Name}");
            Assert.Equal(ObjectChecksum.Compute(v.Expected), ObjectChecksum.Compute(reencoded));
            Assert.Equal(ObjectChecksum.Crc32C(v.Expected), ObjectChecksum.Crc32C(v.Encode()));
        }

        foreach (WireVector v in FmtGolden.WireVectors.Values)
        {
            byte[] first = FmtHelpers.Encode(v.Write);
            byte[] second = FmtHelpers.Encode(v.Write, initialCapacity: 3);
            Assert.Equal(ObjectChecksum.Compute(first), ObjectChecksum.Compute(second));
        }
    }

    /// <summary>
    /// Integrity hashes are stable known algorithms: CRC-32C matches the standard check value for "123456789", the object
    /// checksum is XXH128 (seed 0) — known answer for the empty input and agreement with an independent XXH128 — and both are
    /// independent of buffer alignment, while any single-bit change alters both.
    /// </summary>
    [Fact, Catalog("FMT-012", "U", "INV-01", "INV-06", "INV-08", "INV-10", "INV-18")]
    public void FMT_012_ManifestIdentityAndHashStable_ChecksumsAreStable()
    {
        Assert.Equal(0xE3069283u, ObjectChecksum.Crc32C("123456789"u8));
        Assert.Equal(0u, ObjectChecksum.Crc32C(ReadOnlySpan<byte>.Empty));
        Assert.Equal(new UInt128(0x99AA06D3014798D8UL, 0x6001C324468D497FUL), ObjectChecksum.Compute(ReadOnlySpan<byte>.Empty));

        foreach (int length in new[] { 1, 15, 16, 17, 4096, 65536, 65537 })
        {
            byte[] data = TestData.Bytes((ulong)length, length);
            UInt128 checksum = ObjectChecksum.Compute(data);
            Assert.Equal(XxHash128.HashToUInt128(data), checksum);

            byte[] shifted = new byte[length + 3];
            data.CopyTo(shifted, 3);
            Assert.Equal(checksum, ObjectChecksum.Compute(shifted.AsSpan(3)));
            Assert.Equal(ObjectChecksum.Crc32C(data), ObjectChecksum.Crc32C(shifted.AsSpan(3)));

            foreach (int bit in new[] { 0, length * 8 / 2, length * 8 - 1 })
            {
                byte[] flipped = (byte[])data.Clone();
                flipped[bit / 8] ^= (byte)(1 << (bit % 8));
                Assert.NotEqual(checksum, ObjectChecksum.Compute(flipped));
                Assert.NotEqual(ObjectChecksum.Crc32C(data), ObjectChecksum.Crc32C(flipped));
            }
        }
    }

    // Composed (NFC) and decomposed spellings, built from code units so the source stays ASCII-only.
    private static readonly (string Composed, string Decomposed)[] EquivalentPathPairs =
    [
        ("/caf" + FmtHelpers.Chars(0x00E9), "/cafe" + FmtHelpers.Chars(0x0301)),
        ("/docs/r" + FmtHelpers.Chars(0x00E9) + "sum" + FmtHelpers.Chars(0x00E9) + "/" + FmtHelpers.Chars(0x00C5) + "ngstr" + FmtHelpers.Chars(0x00F6) + "m",
            "/docs/re" + FmtHelpers.Chars(0x0301) + "sume" + FmtHelpers.Chars(0x0301) + "/A" + FmtHelpers.Chars(0x030A) + "ngstro" + FmtHelpers.Chars(0x0308) + "m"),
        ("/" + FmtHelpers.Chars(0xAC00), "/" + FmtHelpers.Chars(0x1100, 0x1161)),
        ("/a%2Fb", "/a%2Fb"),
    ];

    /// <summary>
    /// Equivalent names have one identity: canonically-equivalent inputs normalise to the same NFC path, the same wire bytes
    /// and the same checksum, while names differing only in case remain distinct (ordinal, case-sensitive identity).
    /// </summary>
    [Theory, Catalog("FMT-012", "U", "INV-01", "INV-06", "INV-08", "INV-10", "INV-18")]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public void FMT_012_ManifestIdentityAndHashStable_EquivalentPathsShareOneIdentity(int pair)
    {
        (string composed, string decomposed) = EquivalentPathPairs[pair];
        string a = DfsPath.Normalize(composed);
        string b = DfsPath.Normalize(decomposed);
        Assert.Equal(a, b);
        Assert.True(a.IsNormalized(NormalizationForm.FormC));
        Assert.Equal(composed.Normalize(NormalizationForm.FormC), a);

        byte[] wa = FmtHelpers.Encode((ref WireWriter w) => w.WriteString(a));
        byte[] wb = FmtHelpers.Encode((ref WireWriter w) => w.WriteString(b));
        AssertEx.BytesEqual(wa, wb, "wire bytes of equivalent paths");
        Assert.Equal(ObjectChecksum.Compute(wa), ObjectChecksum.Compute(wb));

        string upper = DfsPath.Normalize(composed.ToUpperInvariant());
        if (!string.Equals(upper, a, StringComparison.Ordinal))
        {
            byte[] wu = FmtHelpers.Encode((ref WireWriter w) => w.WriteString(upper));
            Assert.False(wu.AsSpan().SequenceEqual(wa), "case-different names must not share an encoding");
            Assert.NotEqual(ObjectChecksum.Compute(wa), ObjectChecksum.Compute(wu));
        }
    }

    /// <summary>
    /// Identifier strings denote values, not spellings: accepted variant spellings (uppercase hex, leading zeros) parse to the
    /// same identity and format back to the single canonical form; malformed spellings are rejected rather than coerced.
    /// </summary>
    [Fact, Catalog("FMT-012", "U", "INV-01", "INV-06", "INV-08", "INV-10", "INV-18")]
    public void FMT_012_ManifestIdentityAndHashStable_IdentifierSpellingsCanonicalise()
    {
        Assert.Equal(FmtGolden.SampleObject, ObjectId.Parse("00112233445566778899AABBCCDDEEFF"));
        Assert.Equal("00112233445566778899aabbccddeeff", ObjectId.Parse("00112233445566778899AABBCCDDEEFF").ToString());
        Assert.Equal(new DfsOperationId(0x1234, 1), DfsOperationId.Parse("op:01234-01"));
        Assert.Equal("op:1234-1", DfsOperationId.Parse("op:01234-01").ToString());
        Assert.Equal("op:abc-def", DfsOperationId.Parse("op:ABC-DEF").ToString());
        CommitToken upper = CommitToken.Parse("ct:00112233-4455-6677-8899-AABBCCDDEEFF/FFEEDDCC-BBAA-9988-7766-554433221100/1/300");
        Assert.Equal(FmtGolden.SampleToken, upper);
        Assert.Equal(FmtGolden.TextVectors["token/sample"].Expected, upper.ToString());

        foreach (string bad in new[] { "op:", "op:1", "op:-1", "op:1-", "op:1-2-3", "OP:1-2", " op:1-2", "op:1-2 ", "op:g-1", "op:+1-2",
                     "op:1-11111111111111111", "tx:1-2" })
            Assert.False(DfsOperationId.TryParse(bad, out _), $"'{bad}' must not parse as an operation id");

        const string g1 = "00112233-4455-6677-8899-aabbccddeeff";
        const string g2 = "ffeeddcc-bbaa-9988-7766-554433221100";
        foreach (string bad in new[] { $"ct:{g1}/{g2}/1", $"ct:{g1}/{g2}/1/2/3", $"CT:{g1}/{g2}/1/2", $"ct:{g1}/{g2}/-1/2", $"ct:{g1}/{g2}/1/+2",
                     $"ct:{g1.Replace("-", string.Empty, StringComparison.Ordinal)}/{g2}/1/2", $"ct:{g1}/{g2}/1/18446744073709551616", $"ct:{g1}/{g2}/ 1/2" })
            Assert.Throws<FormatException>(() => CommitToken.Parse(bad));

        foreach (string bad in new[] { "00112233445566778899aabbccddeef", "00112233445566778899aabbccddeeff0",
                     "0x112233445566778899aabbccddeeff", "g0112233445566778899aabbccddeeff", "00112233-4455-6677-8899-aabbccddeeff" })
            Assert.Throws<FormatException>(() => ObjectId.Parse(bad));
    }

    /// <summary>
    /// Replicated manifests have one stable identity: after namespace churn and Raft checkpoints, every replica holds the
    /// same per-file extent manifest (objects, offsets, revisions, inode ids) as the leader, and restarting every node from
    /// its durable checkpoint image reproduces exactly the same manifests and snapshot list.
    /// </summary>
    [Fact, Catalog("FMT-012", "U", "INV-01", "INV-06", "INV-08", "INV-10", "INV-18")]
    public async Task FMT_012_ManifestIdentityAndHashStable()
    {
        TestClusterOptions options = TestClusterOptions.ThreeZones with { Timings = NodeTimings.Fast with { RaftSnapshotThreshold = 20 } };
        await ClusterTest.RunAsync(options, async cluster =>
        {
            var paths = new List<string>();
            await using (DfsClient client = await cluster.ConnectAsync(new TestClientOptions { Name = "c0" }, Ct))
            {
                DfsVolume vol = client.GetVolume(Vol);
                await vol.Directories.CreateDirectoryAsync("/fmt012", Ct);
                int[] sizes = [0, 100, 4096, 4097, 70_000, 200_000];
                for (int i = 0; i < 30; i++)
                    await vol.Files.WriteAllBytesAsync($"/fmt012/f{i}", TestData.Bytes((ulong)(1200 + i), sizes[i % sizes.Length]), Ct);
                await vol.Files.MoveAsync("/fmt012/f0", "/fmt012/g0", overwrite: false, Ct);
                await vol.Files.DeleteAsync("/fmt012/f1", Ct);
                await vol.Snapshots.CreateAsync("fmt012-s1", null, Ct);
                await vol.Files.WriteAllBytesAsync("/fmt012/f4", TestData.Bytes(1299, 90_000), Ct);
                await vol.Files.AppendAllBytesAsync("/fmt012/f5", TestData.Bytes(1298, 1000), Ct);
                paths.Add("/fmt012/g0");
                for (int i = 2; i < 30; i++)
                    paths.Add($"/fmt012/f{i}");
            }

            await cluster.WaitForReplicationAsync(Vol, Ct);
            await cluster.Inspect.AssertReplicasConvergedAsync(Vol);
            Dictionary<string, string> before = await DescribeManifestsAsync(cluster, paths, node: null);
            foreach (TestNode node in cluster.Nodes)
                Assert.Equal(before, await DescribeManifestsAsync(cluster, paths, node.Name));
            string snapshotsBefore = DescribeSnapshots(await cluster.Inspect.GetVolumeStateAsync(Vol));
            foreach (TestNode node in cluster.Nodes)
            {
                GroupInspection? group = await node.Inspect.GetGroupAsync(Vol);
                Assert.NotNull(group);
                Assert.True(group.SnapshotIndex > 0, $"{node.Name} must have taken a Raft checkpoint (threshold 20 entries)");
            }

            foreach (TestNode node in cluster.Nodes)
                await node.StopAsync();
            foreach (TestNode node in cluster.Nodes)
                await node.RestartAsync();
            await cluster.WaitForStableAsync(TimeSpan.FromSeconds(30), Ct);
            await cluster.WaitForReplicationAsync(Vol, Ct);

            Assert.Equal(before, await DescribeManifestsAsync(cluster, paths, node: null));
            foreach (TestNode node in cluster.Nodes)
                Assert.Equal(before, await DescribeManifestsAsync(cluster, paths, node.Name));
            Assert.Equal(snapshotsBefore, DescribeSnapshots(await cluster.Inspect.GetVolumeStateAsync(Vol)));
            await cluster.Inspect.AssertReplicasConvergedAsync(Vol);
        }, Ct);
    }

    // ------------------------------------------------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------------------------------------------------

    private static byte[] ReferenceLeb128(ulong value)
    {
        var bytes = new List<byte>();
        do
        {
            byte group = (byte)(value & 0x7F);
            value >>= 7;
            if (value != 0)
                group |= 0x80;
            bytes.Add(group);
        }
        while (value != 0);
        return bytes.ToArray();
    }

    private static Type ExpectedExceptionType(DfsErrorCode code) => code switch
    {
        DfsErrorCode.FileNotFound => typeof(FileNotFoundException),
        DfsErrorCode.DirectoryNotFound => typeof(DirectoryNotFoundException),
        DfsErrorCode.AccessDenied => typeof(UnauthorizedAccessException),
        DfsErrorCode.InvalidPath or DfsErrorCode.InvalidArgument => typeof(ArgumentException),
        DfsErrorCode.Unsupported => typeof(NotSupportedException),
        DfsErrorCode.Cancelled => typeof(OperationCanceledException),
        DfsErrorCode.Timeout => typeof(TimeoutException),
        DfsErrorCode.AuthenticationFailed => typeof(DfsAuthenticationException),
        DfsErrorCode.SharingViolation or DfsErrorCode.WriterConflict => typeof(DfsSharingViolationException),
        DfsErrorCode.TransactionConflict => typeof(DfsTransactionConflictException),
        DfsErrorCode.TransactionAborted => typeof(DfsTransactionAbortedException),
        DfsErrorCode.LeaseLost => typeof(DfsLeaseLostException),
        DfsErrorCode.PinExpired or DfsErrorCode.ContinuationExpired => typeof(DfsPinExpiredException),
        DfsErrorCode.NotLeader or DfsErrorCode.NodeUnavailable or DfsErrorCode.Unavailable => typeof(DfsUnavailableException),
        DfsErrorCode.QuorumUnavailable => typeof(DfsQuorumUnavailableException),
        DfsErrorCode.PlacementUnavailable => typeof(DfsPlacementUnavailableException),
        DfsErrorCode.CapacityExhausted => typeof(DfsCapacityException),
        DfsErrorCode.Integrity => typeof(DfsIntegrityException),
        DfsErrorCode.OutcomeUnknown => typeof(DfsCommitOutcomeUnknownException),
        DfsErrorCode.OperationExpired => typeof(DfsOperationExpiredException),
        DfsErrorCode.OperationIdMismatch => typeof(DfsOperationIdMismatchException),
        DfsErrorCode.LimitExceeded => typeof(DfsLimitExceededException),
        DfsErrorCode.CrossVolume => typeof(DfsCrossVolumeException),
        DfsErrorCode.ReadOnly => typeof(DfsReadOnlyException),
        DfsErrorCode.ProtocolError => typeof(DfsProtocolException),
        DfsErrorCode.ClusterMismatch => typeof(DfsClusterIdentityException),
        DfsErrorCode.EpochMismatch => typeof(DfsEpochMismatchException),
        DfsErrorCode.RecoveryKeyMissing => typeof(DfsRecoveryKeyMissingException),
        DfsErrorCode.AdapterFailed => typeof(DfsSnapshotAdapterException),
        DfsErrorCode.AlreadyExists => typeof(DfsAlreadyExistsException),
        DfsErrorCode.DirectoryNotEmpty => typeof(DfsDirectoryNotEmptyException),
        DfsErrorCode.RetentionProtected => typeof(DfsRetentionProtectedException),
        DfsErrorCode.BackupNotReached => typeof(DfsBackupNotReachedException),
        _ => typeof(DfsException),
    };

    private static async Task<Dictionary<string, string>> DescribeManifestsAsync(TestCluster cluster, IReadOnlyList<string> paths, string? node)
    {
        VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(Vol, node);
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (string path in paths)
        {
            FileInspection? file = state.GetFile(path);
            Assert.True(file is not null, $"{path} must exist on {node ?? "the leader"}");
            var sb = new StringBuilder();
            sb.Append($"inode={file.Inode.Value};rev={file.Revision};len={file.Length};inline={file.IsInline};policy={file.Policy}");
            foreach (ExtentInspection e in file.Extents.OrderBy(e => e.Offset))
                sb.Append($";[{e.Offset}+{e.Length}->{e.Object}@{e.ObjectOffset}+{e.ObjectLength}]");
            result[path] = sb.ToString();
        }

        return result;
    }

    private static string DescribeSnapshots(VolumeStateInspection state) =>
        string.Join("|", state.Snapshots.OrderBy(s => s.Name, StringComparer.Ordinal)
            .Select(s => $"{s.Id.Value}:{s.Name}:{s.BoundaryIndex}:{s.Label}:{s.InodeCount}"));
}
