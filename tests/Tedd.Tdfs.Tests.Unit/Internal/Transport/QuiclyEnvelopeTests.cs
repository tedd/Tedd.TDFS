using Tedd.Tdfs.Transport.Internal;
using static Tedd.Tdfs.Tests.Unit.Internal.Transport.TransportTestData;

namespace Tedd.Tdfs.Tests.Unit.Internal.Transport;

/// <summary>The QUIC RPC envelope codec and fragment reassembly (pure, no network).</summary>
public sealed class QuiclyEnvelopeTests
{
    [Fact]
    public void Frame_RoundTrips()
    {
        byte[] buffer = new byte[QuiclyEnvelope.MaxFrameSize];
        byte[] chunk = Pattern(1000, 3);
        int length = QuiclyEnvelope.WriteFrame(buffer, RpcMessageKind.Request, 0x0102030405060708, 2, 5, chunk);

        Assert.Equal(QuiclyEnvelope.HeaderSize + chunk.Length, length);
        Assert.True(QuiclyEnvelope.TryParse(buffer.AsSpan(0, length), out EnvelopeHeader header, out ReadOnlySpan<byte> parsed, out string? error), error);
        Assert.Equal(new EnvelopeHeader(RpcMessageKind.Request, 0x0102030405060708, 2, 5), header);
        Assert.True(parsed.SequenceEqual(chunk));
    }

    [Fact]
    public void Header_IsLittleEndian_AndStable()
    {
        byte[] buffer = new byte[QuiclyEnvelope.HeaderSize];
        QuiclyEnvelope.WriteFrame(buffer, RpcMessageKind.Reply, 1, 0, 1, default);
        Assert.Equal(new byte[] { 2, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0 }, buffer);
    }

    [Theory]
    [InlineData(0, 1)]
    [InlineData(1, 1)]
    [InlineData(QuiclyEnvelope.MaxChunkSize, 1)]
    [InlineData(QuiclyEnvelope.MaxChunkSize + 1, 2)]
    [InlineData(QuiclyEnvelope.MaxChunkSize * 3, 3)]
    [InlineData(8 << 20, 137)]
    public void FragmentCount_CoversThePayload(int length, int expected)
        => Assert.Equal(expected, QuiclyEnvelope.FragmentCount(length));

    [Fact]
    public void TryParse_RejectsMalformedFrames()
    {
        byte[] buffer = new byte[QuiclyEnvelope.MaxFrameSize + 1];

        Assert.False(QuiclyEnvelope.TryParse(buffer.AsSpan(0, QuiclyEnvelope.HeaderSize - 1), out _, out _, out _));
        Assert.False(QuiclyEnvelope.TryParse(buffer, out _, out _, out _)); // longer than a frame

        Assert.False(Parse(kind: 0, correlation: 1, index: 0, count: 1, chunk: 1)); // unknown kind
        Assert.False(Parse(kind: 9, correlation: 1, index: 0, count: 1, chunk: 1));
        Assert.False(Parse(kind: 1, correlation: 1, index: 0, count: 0, chunk: 1)); // zero fragments
        Assert.False(Parse(kind: 1, correlation: 1, index: 3, count: 3, chunk: 1)); // index out of range
        Assert.False(Parse(kind: 1, correlation: 1, index: 0, count: 2, chunk: 0)); // empty fragment
        Assert.False(Parse(kind: 4, correlation: 1, index: 0, count: 1, chunk: 1)); // preamble with a correlation
        Assert.False(Parse(kind: 4, correlation: 0, index: 0, count: 2, chunk: 1)); // fragmented preamble
        Assert.True(Parse(kind: 3, correlation: 7, index: 0, count: 1, chunk: 0));  // empty one-way message is fine
    }

    [Fact]
    public void Names_AndAuthTokens_RoundTrip_AndRejectGarbage()
    {
        byte[] token = QuiclyEnvelope.EncodeAuthToken("client:c0");
        Assert.True(QuiclyEnvelope.TryDecodeAuthToken(token, out string? name));
        Assert.Equal("client:c0", name);

        Assert.False(QuiclyEnvelope.TryDecodeAuthToken([], out _));
        Assert.False(QuiclyEnvelope.TryDecodeAuthToken("TDFS"u8, out _));
        Assert.False(QuiclyEnvelope.TryDecodeAuthToken("XDFS\u0001n0"u8, out _));
        Assert.False(QuiclyEnvelope.TryDecodeAuthToken("TDFS\u0002n0"u8, out _));
        Assert.False(QuiclyEnvelope.TryDecodeName([0xC3, 0x28], out _)); // invalid UTF-8
        Assert.False(QuiclyEnvelope.TryDecodeName("a\nb"u8, out _));
        Assert.False(QuiclyEnvelope.TryDecodeName(new byte[QuiclyEnvelope.MaxNameBytes + 1], out _));
        Assert.True(QuiclyEnvelope.TryDecodeName("nœd"u8, out string? unicode));
        Assert.Equal("nœd", unicode);

        Assert.Throws<ArgumentException>(() => QuiclyEnvelope.ValidateName("", "name"));
        Assert.Throws<ArgumentException>(() => QuiclyEnvelope.ValidateName("bad\tname", "name"));
        Assert.Throws<ArgumentException>(() => QuiclyEnvelope.ValidateName("\ud800", "name"));
        Assert.Throws<ArgumentException>(() => QuiclyEnvelope.ValidateName(new string('x', QuiclyEnvelope.MaxNameBytes + 1), "name"));
        QuiclyEnvelope.ValidateName("n0", "name");
    }

    [Fact]
    public void Reassembler_AssemblesFragmentsInAnyOrder()
    {
        var reassembler = new QuiclyReassembler(1 << 20, 4 << 20);
        byte[] payload = Pattern(QuiclyEnvelope.MaxChunkSize * 3 + 100, 9);
        int count = QuiclyEnvelope.FragmentCount(payload.Length);
        int[] order = [2, 0, 3, 1];

        byte[]? result = null;
        foreach (int index in order)
        {
            Assert.Null(result);
            int offset = index * QuiclyEnvelope.MaxChunkSize;
            int length = Math.Min(QuiclyEnvelope.MaxChunkSize, payload.Length - offset);
            result = reassembler.Add(new EnvelopeHeader(RpcMessageKind.Reply, 5, (uint)index, (uint)count), payload.AsSpan(offset, length));
        }

        Assert.NotNull(result);
        Assert.Equal(payload, result);
        Assert.Equal(0, reassembler.PendingMessages);
        Assert.Equal(0, reassembler.BufferedBytes);
    }

    [Fact]
    public void Reassembler_KeepsKindsAndCorrelationsApart()
    {
        var reassembler = new QuiclyReassembler(1 << 20, 4 << 20);
        Assert.Null(reassembler.Add(new EnvelopeHeader(RpcMessageKind.Request, 1, 0, 2), [1]));
        Assert.Null(reassembler.Add(new EnvelopeHeader(RpcMessageKind.Reply, 1, 0, 2), [2]));
        Assert.Null(reassembler.Add(new EnvelopeHeader(RpcMessageKind.Request, 2, 1, 2), [3]));
        Assert.Equal(3, reassembler.PendingMessages);
        Assert.Equal(new byte[] { 2, 4 }, reassembler.Add(new EnvelopeHeader(RpcMessageKind.Reply, 1, 1, 2), [4]));
        Assert.Equal(new byte[] { 1, 5 }, reassembler.Add(new EnvelopeHeader(RpcMessageKind.Request, 1, 1, 2), [5]));
        Assert.Equal(1, reassembler.PendingMessages);
        reassembler.Clear();
        Assert.Equal(0, reassembler.PendingMessages);
        Assert.Equal(0, reassembler.BufferedBytes);
    }

    [Fact]
    public void Reassembler_RejectsInconsistentOrExcessiveInput()
    {
        var reassembler = new QuiclyReassembler(maxMessageBytes: 3 * QuiclyEnvelope.MaxChunkSize, maxBufferedBytes: 4 * QuiclyEnvelope.MaxChunkSize, maxConcurrentMessages: 2);
        byte[] chunk = new byte[QuiclyEnvelope.MaxChunkSize];

        // Declared fragment count beyond the message limit.
        Assert.Throws<QuiclyProtocolException>(() => reassembler.Add(new EnvelopeHeader(RpcMessageKind.Request, 1, 0, 4), chunk));
        // Duplicate fragment.
        reassembler.Add(new EnvelopeHeader(RpcMessageKind.Request, 2, 0, 3), chunk);
        Assert.Throws<QuiclyProtocolException>(() => reassembler.Add(new EnvelopeHeader(RpcMessageKind.Request, 2, 0, 3), chunk));
        // Inconsistent fragment count.
        Assert.Throws<QuiclyProtocolException>(() => reassembler.Add(new EnvelopeHeader(RpcMessageKind.Request, 2, 1, 2), chunk));
        // Too many messages in progress.
        reassembler.Add(new EnvelopeHeader(RpcMessageKind.Request, 3, 0, 3), chunk);
        Assert.Throws<QuiclyProtocolException>(() => reassembler.Add(new EnvelopeHeader(RpcMessageKind.Request, 4, 0, 3), chunk));
        // Buffered bytes cap (two messages hold two chunks; two more would exceed four).
        reassembler.Add(new EnvelopeHeader(RpcMessageKind.Request, 2, 1, 3), chunk);
        reassembler.Add(new EnvelopeHeader(RpcMessageKind.Request, 3, 1, 3), chunk);
        Assert.Throws<QuiclyProtocolException>(() => reassembler.Add(new EnvelopeHeader(RpcMessageKind.Request, 2, 2, 3), chunk));
        // Empty fragments and single-frame messages are not reassembled.
        Assert.Throws<QuiclyProtocolException>(() => reassembler.Add(new EnvelopeHeader(RpcMessageKind.Request, 5, 0, 2), []));
        Assert.Throws<QuiclyProtocolException>(() => reassembler.Add(new EnvelopeHeader(RpcMessageKind.Request, 6, 0, 1), chunk));
    }

    [Fact]
    public void Reassembler_EnforcesTheMessageSizeLimit()
    {
        var reassembler = new QuiclyReassembler(maxMessageBytes: QuiclyEnvelope.MaxChunkSize + 10, maxBufferedBytes: 1 << 20);
        byte[] chunk = new byte[QuiclyEnvelope.MaxChunkSize];
        reassembler.Add(new EnvelopeHeader(RpcMessageKind.OneWay, 1, 0, 2), chunk);
        Assert.Throws<QuiclyProtocolException>(() => reassembler.Add(new EnvelopeHeader(RpcMessageKind.OneWay, 1, 1, 2), new byte[11]));
    }

    private static bool Parse(byte kind, ulong correlation, uint index, uint count, int chunk)
    {
        byte[] frame = new byte[QuiclyEnvelope.HeaderSize + chunk];
        frame[0] = kind;
        BitConverter.TryWriteBytes(frame.AsSpan(1, 8), correlation);
        BitConverter.TryWriteBytes(frame.AsSpan(9, 4), index);
        BitConverter.TryWriteBytes(frame.AsSpan(13, 4), count);
        return QuiclyEnvelope.TryParse(frame, out _, out _, out _);
    }
}
