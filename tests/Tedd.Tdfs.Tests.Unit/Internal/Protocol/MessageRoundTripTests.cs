using System.Buffers;
using System.Reflection;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;

namespace Tedd.Tdfs.Tests.Unit.Internal.Protocol;

/// <summary>
/// Round-trip and robustness tests for every registered protocol message (FMT-001..004, SEC-015): encode → decode →
/// structurally equal and byte-identical on re-encode; unknown optional extensions preserved; unknown required
/// extensions rejected as unsupported; every truncation and trailing garbage rejected as a protocol error.
/// </summary>
public sealed class MessageRoundTripTests
{
    private const int SeedsPerType = 40;

    public static TheoryData<MessageType> AllTypes()
    {
        var data = new TheoryData<MessageType>();
        foreach (MessageType type in MessageTypes.All)
            data.Add(type);
        return data;
    }

    [Fact]
    public void EveryCatalogueTypeHasExactlyOneRegisteredRecord()
    {
        Assert.Equal(MessageTypes.All, MessageCodec.RegisteredTypes);
        var clrTypes = new HashSet<Type>();
        foreach (MessageType type in MessageTypes.All)
        {
            Type? clr = MessageCodec.GetMessageClrType(type);
            Assert.NotNull(clr);
            Assert.True(clrTypes.Add(clr), $"{clr} registered twice");
            Assert.Equal(type + "Message", clr.Name);
            Assert.True(clr.IsSealed, $"{clr} must be sealed");
            Assert.Equal("Tedd.Tdfs.Protocol.Messages", clr.Namespace);
            var wireType = (MessageType)clr.GetProperty("WireType", BindingFlags.Public | BindingFlags.Static)!.GetValue(null)!;
            Assert.Equal(type, wireType);
            Assert.NotNull(clr.GetProperty("Extensions"));
        }
    }

    [Fact]
    public void EveryRequestHasARegisteredReplyAndOperationKind()
    {
        foreach (MessageType type in MessageTypes.All.Where(MessageTypes.IsRequest))
        {
            MessageType reply = MessageTypes.ReplyOf(type);
            Assert.True(MessageCodec.IsKnown(reply), $"{type} reply {reply} is not registered");
            Assert.Equal(type, MessageTypes.RequestOf(reply));
            Assert.NotEqual(DfsOperationKind.Unknown, MessageTypes.OperationOf(type));
            Assert.Equal(MessageTypes.OperationOf(type), MessageTypes.OperationOf(reply));
        }
    }

    [Theory]
    [MemberData(nameof(AllTypes))]
    public void RandomInstancesRoundTrip(MessageType type)
    {
        for (int seed = 0; seed < SeedsPerType; seed++)
        {
            IWireMessage message = ProtocolSamples.Random(seed * 7919 + (int)type).CreateMessage(type);
            AssertRoundTrip(message, $"{type} seed {seed}");
        }
    }

    [Theory]
    [MemberData(nameof(AllTypes))]
    public void MinimalInstanceRoundTrips(MessageType type)
    {
        IWireMessage message = ProtocolSamples.Minimal().CreateMessage(type);
        AssertRoundTrip(message, $"{type} minimal");
    }

    [Theory]
    [MemberData(nameof(AllTypes))]
    public void EveryTruncationIsRejected(MessageType type)
    {
        for (int seed = 0; seed < 3; seed++)
        {
            byte[] frame = MessageCodec.Encode(ProtocolSamples.Random(seed + 17 * (int)type).CreateMessage(type));
            for (int length = 0; length < frame.Length; length++)
            {
                DfsProtocolException ex = Assert.Throws<DfsProtocolException>(() => MessageCodec.Decode(frame.AsSpan(0, length)));
                Assert.Equal(DfsErrorCode.ProtocolError, ex.ErrorCode);
            }
        }
    }

    [Theory]
    [MemberData(nameof(AllTypes))]
    public void TrailingBytesAreRejected(MessageType type)
    {
        byte[] frame = MessageCodec.Encode(ProtocolSamples.Random(5).CreateMessage(type));
        byte[] padded = [.. frame, 0x00];
        DfsProtocolException ex = Assert.Throws<DfsProtocolException>(() => MessageCodec.Decode(padded));
        Assert.Equal(DfsErrorCode.ProtocolError, ex.ErrorCode);
    }

    [Theory]
    [MemberData(nameof(AllTypes))]
    public void UnknownOptionalExtensionIsPreservedAndReEncoded(MessageType type)
    {
        IWireMessage message = ProtocolSamples.Minimal().CreateMessage(type);
        byte[] frame = MessageCodec.Encode(message);
        Assert.Equal(0, frame[^1]); // minimal instance: empty terminal extension block

        byte[] extended = [.. frame.AsSpan(0, frame.Length - 1), .. ExtensionBlock(fieldId: 4242, required: false, "future"u8)];
        IWireMessage decoded = MessageCodec.Decode(extended);
        var extensions = (IReadOnlyList<WireExtension>)decoded.GetType().GetProperty("Extensions")!.GetValue(decoded)!;
        WireExtension extension = Assert.Single(extensions);
        Assert.Equal(4242u, extension.FieldId);
        Assert.False(extension.Required);
        Assert.Equal("future"u8.ToArray(), extension.Value.ToArray());

        // Everything except the extension block decodes as before, and re-encoding reproduces the extended frame.
        WireDeepEquality.AssertEqual(message, WithExtensions(decoded, []));
        Assert.Equal(extended, MessageCodec.Encode(decoded));
    }

    [Theory]
    [MemberData(nameof(AllTypes))]
    public void UnknownRequiredExtensionIsUnsupported(MessageType type)
    {
        byte[] frame = MessageCodec.Encode(ProtocolSamples.Minimal().CreateMessage(type));
        byte[] extended = [.. frame.AsSpan(0, frame.Length - 1), .. ExtensionBlock(fieldId: 7, required: true, [1, 2, 3])];
        DfsProtocolException ex = Assert.Throws<DfsProtocolException>(() => MessageCodec.Decode(extended));
        Assert.Equal(DfsErrorCode.Unsupported, ex.ErrorCode);
    }

    [Theory]
    [MemberData(nameof(AllTypes))]
    public void CorruptedFramesOnlyEverFailWithProtocolErrors(MessageType type)
    {
        var random = new Random(1234 + (int)type);
        for (int iteration = 0; iteration < 60; iteration++)
        {
            byte[] frame = MessageCodec.Encode(ProtocolSamples.Random(iteration).CreateMessage(type));
            int flips = random.Next(1, 4);
            for (int i = 0; i < flips; i++)
            {
                int at = random.Next(MessageCodec.HeaderSize, frame.Length);
                frame[at] ^= (byte)random.Next(1, 256);
            }

            try
            {
                IWireMessage decoded = MessageCodec.Decode(frame);
                // A corruption that still decodes must re-encode canonically to the same bytes.
                Assert.Equal(frame, MessageCodec.Encode(decoded));
            }
            catch (DfsProtocolException ex)
            {
                Assert.True(ex.ErrorCode is DfsErrorCode.ProtocolError or DfsErrorCode.Unsupported, ex.ErrorCode.ToString());
            }
        }
    }

    [Fact]
    public void HostileCollectionCountIsRejectedBeforeAllocation()
    {
        // KeepAlive: header, then a handle list claiming 2^20 entries with no bytes behind it.
        byte[] frame = MessageCodec.Encode(new KeepAliveMessage(new RequestHeader("v"), [], [], []));
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new WireWriter(buffer);
        writer.WriteUInt16((ushort)MessageType.KeepAlive);
        new RequestHeader("v").Write(ref writer);
        writer.WriteVarUInt(1 << 20);
        byte[] hostile = buffer.WrittenSpan.ToArray();
        Assert.True(hostile.Length < frame.Length + 8);
        DfsProtocolException ex = Assert.Throws<DfsProtocolException>(() => MessageCodec.Decode(hostile));
        Assert.Equal(DfsErrorCode.ProtocolError, ex.ErrorCode);
        Assert.Contains("cannot fit", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void OversizedFieldsRespectDecoderLimits()
    {
        var message = new PutObjectMessage(new byte[4], default, default, default, UInt128.Zero, 100, 0, 0, new byte[100]);
        byte[] frame = MessageCodec.Encode(message);
        Assert.IsType<PutObjectMessage>(MessageCodec.Decode(frame));
        DfsProtocolException ex = Assert.Throws<DfsProtocolException>(() => MessageCodec.Decode(frame, new WireLimits(MaxBytesLength: 64)));
        Assert.Equal(DfsErrorCode.ProtocolError, ex.ErrorCode);

        var named = new GetVolumeRouteMessage(new string('v', 300));
        DfsProtocolException ex2 = Assert.Throws<DfsProtocolException>(() => MessageCodec.Decode(MessageCodec.Encode(named), new WireLimits(MaxStringBytes: 256)));
        Assert.Equal(DfsErrorCode.ProtocolError, ex2.ErrorCode);
    }

    [Fact]
    public void UnknownMessageTypeIsRejected()
    {
        DfsProtocolException ex = Assert.Throws<DfsProtocolException>(() => MessageCodec.Decode([0xFF, 0xFF, 0x00]));
        Assert.Equal(DfsErrorCode.ProtocolError, ex.ErrorCode);
    }

    [Fact]
    public void NestedRecordExtensionsArePreserved()
    {
        NodeEntry node = (NodeEntry)ProtocolSamples.Minimal().Create(typeof(NodeEntry)) with
        {
            Extensions = [new WireExtension(9, false, new byte[] { 1, 2 })],
        };
        var map = (ClusterMapData)ProtocolSamples.Minimal().Create(typeof(ClusterMapData)) with { Nodes = [node] };
        var result = new ClusterMapResultMessage(3, map);
        var decoded = MessageCodec.Decode<ClusterMapResultMessage>(MessageCodec.Encode(result));
        WireExtension extension = Assert.Single(decoded.Map!.Nodes[0].Extensions);
        Assert.Equal(9u, extension.FieldId);
        Assert.Equal(new byte[] { 1, 2 }, extension.Value.ToArray());
    }

    [Fact]
    public void RawEnumsCarryUnvalidatedValuesForServerSideValidation()
    {
        // The server, not the decoder, rejects bad modes/shares (InvalidArgument), so these must survive the wire.
        var open = new OpenHandleMessage(new RequestHeader("v"), "/x", (FileMode)99, (FileAccess)0, FileShare.Inheritable | FileShare.Delete | (FileShare)0x40,
            (FileOptions)int.MinValue, false, false, null);
        var decoded = MessageCodec.Decode<OpenHandleMessage>(MessageCodec.Encode(open));
        Assert.Equal((FileMode)99, decoded.Mode);
        Assert.Equal((FileAccess)0, decoded.Access);
        Assert.Equal(FileShare.Inheritable | FileShare.Delete | (FileShare)0x40, decoded.Share);
        Assert.Equal((FileOptions)int.MinValue, decoded.Options);
    }

    [Fact]
    public void StrictEnumsRejectUndefinedValues()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new WireWriter(buffer);
        writer.WriteUInt16((ushort)MessageType.TxAbortResult);
        writer.WriteVarInt(77); // undefined WireOutcomeStatus
        writer.WriteCommitToken(CommitToken.None);
        writer.WriteNoExtensions();
        DfsProtocolException ex = Assert.Throws<DfsProtocolException>(() => MessageCodec.Decode(buffer.WrittenSpan));
        Assert.Equal(DfsErrorCode.ProtocolError, ex.ErrorCode);
    }

    [Fact]
    public void UndefinedStrictEnumsFailFastOnEncode()
    {
        // Every receiver would reject these, so the sender refuses to produce them.
        var raft = new RaftBatchMessage(default, 1, [new RaftEnvelope("control", (RaftWireMessage)ProtocolSamples.Minimal().Create(typeof(RaftWireMessage)) with { Kind = 0 })]);
        Assert.Throws<ArgumentOutOfRangeException>(() => MessageCodec.Encode(raft));
        var abort = new TxAbortResultMessage((WireOutcomeStatus)42, CommitToken.None);
        Assert.Throws<ArgumentOutOfRangeException>(() => MessageCodec.Encode(abort));
    }

    [Fact]
    public void FixedSizeFieldsAreEnforcedOnBothSides()
    {
        Assert.Throws<ArgumentException>(() => MessageCodec.Encode(new HelloProofMessage(new byte[31])));
        Assert.Throws<ArgumentException>(() => MessageCodec.Encode(new HelloMessage(Guid.Empty, 1, 1, new HashSet<string>(), PeerRole.Client, "c", null, 0, "p", new byte[16])));

        var buffer = new ArrayBufferWriter<byte>();
        var writer = new WireWriter(buffer);
        writer.WriteUInt16((ushort)MessageType.HelloProof);
        writer.WriteBytes(new byte[33]);
        writer.WriteNoExtensions();
        DfsProtocolException ex = Assert.Throws<DfsProtocolException>(() => MessageCodec.Decode(buffer.WrittenSpan));
        Assert.Equal(DfsErrorCode.ProtocolError, ex.ErrorCode);
    }

    [Fact]
    public void MissingRequiredNestedRecordFailsFastOnEncode()
    {
        var message = new OpenSessionMessage(null!, "c", UInt128.One);
        ArgumentNullException ex = Assert.Throws<ArgumentNullException>(() => MessageCodec.Encode(message));
        Assert.Contains(nameof(RequestHeader), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void NullCollectionsEncodeAsEmpty()
    {
        var message = new KeepAliveMessage(new RequestHeader("v"), null!, null!, null!);
        var decoded = MessageCodec.Decode<KeepAliveMessage>(MessageCodec.Encode(message));
        Assert.Empty(decoded.Handles);
        Assert.Empty(decoded.PinUpdates);
        Assert.Empty(decoded.ReaderPins);
    }

    internal static void AssertRoundTrip(IWireMessage message, string because)
    {
        byte[] frame = MessageCodec.Encode(message);
        Assert.True(MessageCodec.TryReadType(frame, out MessageType type), because);
        Assert.Equal(message.Type, type);
        IWireMessage decoded = MessageCodec.Decode(frame);
        Assert.IsType(message.GetType(), decoded);
        string? difference = WireDeepEquality.FindDifference(message, decoded, because);
        Assert.True(difference is null, difference);
        Assert.Equal(frame, MessageCodec.Encode(decoded));
    }

    private static byte[] ExtensionBlock(uint fieldId, bool required, ReadOnlySpan<byte> value)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new WireWriter(buffer);
        writer.WriteExtensions([new WireExtension(fieldId, required, value.ToArray())]);
        return buffer.WrittenSpan.ToArray();
    }

    private static object WithExtensions(object record, IReadOnlyList<WireExtension> extensions)
    {
        MethodInfo clone = record.GetType().GetMethod("<Clone>$")!;
        object copy = clone.Invoke(record, null)!;
        record.GetType().GetProperty("Extensions")!.SetValue(copy, extensions);
        return copy;
    }
}
