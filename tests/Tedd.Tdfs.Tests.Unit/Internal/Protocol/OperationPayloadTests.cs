using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;

namespace Tedd.Tdfs.Tests.Unit.Internal.Protocol;

/// <summary>Operation payload identity (OUT-002/003) and the <see cref="IVolumeRequest"/> contract.</summary>
public sealed class OperationPayloadTests
{
    private static readonly DfsOperationId Op = new(7, 42);

    private static WriteAllMessage Sample(string path = "/a.txt", byte fill = 1)
        => new(new RequestHeader("vol", new VolumeId(Guid.NewGuid()), 3, Session: 7, Operation: Op, AckWatermark: 40),
            path, 3, [RangeChange.ForInline(0, new byte[] { fill, fill, fill })], [], default, null, null, false, 0);

    [Fact]
    public void EveryHeaderBearingMessageIsAVolumeRequest()
    {
        int count = 0;
        foreach (MessageType type in MessageTypes.All)
        {
            Type clr = MessageCodec.GetMessageClrType(type)!;
            bool hasHeader = clr.GetProperty("Header")?.PropertyType == typeof(RequestHeader);
            Assert.Equal(hasHeader, typeof(IVolumeRequest).IsAssignableFrom(clr));
            if (!hasHeader)
                continue;
            count++;
            Assert.True(MessageTypes.IsRequest(type), $"{type} carries a request header but is not a request");

            var request = (IVolumeRequest)ProtocolSamples.Random(count).CreateMessage(type);
            var header = new RequestHeader("other", Session: 99, Operation: new DfsOperationId(1, 2));
            IVolumeRequest changed = request.WithHeader(header);
            Assert.Same(header, changed.Header);
            Assert.Equal(request.GetType(), changed.GetType());
            // Everything except the header (including extensions) is preserved.
            WireDeepEquality.AssertEqual(request, changed.WithHeader(request.Header));
        }

        Assert.Equal(34, count);
    }

    [Fact]
    public void RetriesFromAnotherSessionOrWithAnAdvancedWatermarkKeepTheSameIdentity()
    {
        WriteAllMessage original = Sample();
        WriteAllMessage retry = original with
        {
            Header = original.Header with { Session = 900, AckWatermark = 41, ExpectedVolumeId = default, ExpectedVolumeEpoch = 0, Extensions = [new WireExtension(5, false, new byte[] { 1 })] },
        };
        Assert.Equal(OperationPayload.ComputeHash(original), OperationPayload.ComputeHash(retry));
        Assert.Equal(OperationPayload.HashSize, OperationPayload.ComputeHash(original).Length);
    }

    [Fact]
    public void DifferentArgumentsOrIdentityChangeThePayloadIdentity()
    {
        byte[] baseline = OperationPayload.ComputeHash(Sample());
        Assert.NotEqual(baseline, OperationPayload.ComputeHash(Sample(path: "/b.txt")));
        Assert.NotEqual(baseline, OperationPayload.ComputeHash(Sample(fill: 2)));
        WriteAllMessage sample = Sample();
        Assert.NotEqual(baseline, OperationPayload.ComputeHash(sample with { Header = sample.Header with { Operation = new DfsOperationId(7, 43) } }));
        Assert.NotEqual(baseline, OperationPayload.ComputeHash(sample with { Header = sample.Header with { Volume = "vol2" } }));
        Assert.NotEqual(baseline, OperationPayload.ComputeHash(sample with { Header = sample.Header with { Transaction = new DfsTransactionId(Op) } }));
        Assert.NotEqual(baseline, OperationPayload.ComputeHash(sample with { CreateParents = true }));
        Assert.NotEqual(baseline, OperationPayload.ComputeHash(sample with { Extensions = [new WireExtension(5, false, new byte[] { 1 })] }));
        // Different request kinds with the same id never share an identity.
        var delete = new DeleteMessage(sample.Header, "/a.txt", false, false, null);
        Assert.NotEqual(baseline, OperationPayload.ComputeHash(delete));
    }

    [Fact]
    public void NormalizeClearsOnlyRoutingAndRetryFields()
    {
        var header = new RequestHeader("vol", new VolumeId(Guid.NewGuid()), 4, 5, Op, 6, new DfsTransactionId(Op)) { Extensions = [new WireExtension(1, false, ReadOnlyMemory<byte>.Empty)] };
        RequestHeader normalized = OperationPayload.Normalize(header);
        Assert.Equal("vol", normalized.Volume);
        Assert.Equal(Op, normalized.Operation);
        Assert.Equal(new DfsTransactionId(Op), normalized.Transaction);
        Assert.True(normalized.ExpectedVolumeId.IsEmpty);
        Assert.Equal(0UL, normalized.ExpectedVolumeEpoch);
        Assert.Equal(0UL, normalized.Session);
        Assert.Equal(0UL, normalized.AckWatermark);
        Assert.Empty(normalized.Extensions);
    }
}
