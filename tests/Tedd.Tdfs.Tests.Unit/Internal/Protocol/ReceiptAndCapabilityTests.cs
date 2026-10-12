using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;

namespace Tedd.Tdfs.Tests.Unit.Internal.Protocol;

/// <summary>MAC binding of object receipts and data capabilities (SEC-004/005/007, REP receipts).</summary>
public sealed class ReceiptAndCapabilityTests
{
    private static readonly byte[] Key = Enumerable.Range(1, 32).Select(i => (byte)i).ToArray();
    private static readonly byte[] OtherKey = Enumerable.Range(2, 32).Select(i => (byte)i).ToArray();

    private static ObjectReceipt SampleReceipt() => new ObjectReceipt(
        new ObjectId(new UInt128(1, 2)), new VolumeId(Guid.Parse("8f0a8d5c-7a3e-4d6f-9f1b-5f7e9a1c2b3d")), new StagingId(9),
        new UInt128(0xDEAD, 0xBEEF), 4096, ObjectEncodings.Plain, new NodeId(Guid.Parse("1b2c3d4e-0000-4000-8000-0000000000aa")),
        3, 11, 638_000_000_000_000_000, ReadOnlyMemory<byte>.Empty).Sign(Key);

    [Fact]
    public void SignedReceiptVerifiesOnlyWithItsKey()
    {
        ObjectReceipt receipt = SampleReceipt();
        Assert.Equal(ProtocolInfo.MacSize, receipt.Mac.Length);
        Assert.True(receipt.VerifyMac(Key));
        Assert.False(receipt.VerifyMac(OtherKey));
        Assert.False((receipt with { Mac = ReadOnlyMemory<byte>.Empty }).VerifyMac(Key));
    }

    public static TheoryData<string> ReceiptFields() =>
    [
        nameof(ObjectReceipt.Object), nameof(ObjectReceipt.Volume), nameof(ObjectReceipt.Staging), nameof(ObjectReceipt.Checksum),
        nameof(ObjectReceipt.Length), nameof(ObjectReceipt.Encoding), nameof(ObjectReceipt.Node), nameof(ObjectReceipt.Incarnation),
        nameof(ObjectReceipt.PlacementEpoch), nameof(ObjectReceipt.DurableAtTicks),
    ];

    [Theory]
    [MemberData(nameof(ReceiptFields))]
    public void TamperingAnyReceiptFieldBreaksTheMac(string field)
    {
        ObjectReceipt receipt = SampleReceipt();
        ObjectReceipt tampered = field switch
        {
            nameof(ObjectReceipt.Object) => receipt with { Object = new ObjectId(receipt.Object.Value + 1) },
            nameof(ObjectReceipt.Volume) => receipt with { Volume = new VolumeId(Guid.Empty) },
            nameof(ObjectReceipt.Staging) => receipt with { Staging = new StagingId(10) },
            nameof(ObjectReceipt.Checksum) => receipt with { Checksum = receipt.Checksum ^ 1 },
            nameof(ObjectReceipt.Length) => receipt with { Length = 4095 },
            nameof(ObjectReceipt.Encoding) => receipt with { Encoding = 1 },
            nameof(ObjectReceipt.Node) => receipt with { Node = new NodeId(Guid.Empty) },
            nameof(ObjectReceipt.Incarnation) => receipt with { Incarnation = 4 },
            nameof(ObjectReceipt.PlacementEpoch) => receipt with { PlacementEpoch = 12 },
            nameof(ObjectReceipt.DurableAtTicks) => receipt with { DurableAtTicks = 1 },
            _ => throw new ArgumentOutOfRangeException(nameof(field)),
        };
        Assert.False(tampered.VerifyMac(Key), field);
    }

    [Fact]
    public void ReceiptSurvivesTheWireAndStillVerifies()
    {
        ObjectReceipt receipt = SampleReceipt();
        var reply = new PutObjectResultMessage(receipt, AlreadyPresent: false);
        var decoded = MessageCodec.Decode<PutObjectResultMessage>(MessageCodec.Encode(reply));
        Assert.True(decoded.Receipt.VerifyMac(Key));
        Assert.True(decoded.Receipt.Covers(receipt.Object, receipt.Checksum, receipt.Length, receipt.Encoding));
        Assert.False(decoded.Receipt.Covers(receipt.Object, receipt.Checksum, receipt.Length + 1, receipt.Encoding));
    }

    [Fact]
    public void UnsignedReceiptCannotBeEncoded()
    {
        ObjectReceipt unsigned = SampleReceipt() with { Mac = ReadOnlyMemory<byte>.Empty };
        Assert.Throws<ArgumentException>(() => WireRecordCodec.Encode(unsigned));
    }

    [Fact]
    public void ReadCapabilityIsCanonicalAndScoped()
    {
        var a = new ObjectId(5);
        var b = new ObjectId(1);
        var c = new ObjectId(9);
        DataCapability capability = DataCapability.ForRead(Guid.NewGuid(), new VolumeId(Guid.NewGuid()), 2, "alice", [a, b, a, c], DateTimeOffset.UnixEpoch.AddDays(1))
            .Sign(Key);
        Assert.Equal([b, a, c], capability.Objects);
        Assert.True(capability.AllowsRead(a));
        Assert.True(capability.AllowsRead(b));
        Assert.True(capability.AllowsRead(c));
        Assert.False(capability.AllowsRead(new ObjectId(6)));
        Assert.True(capability.VerifyMac(Key));
        Assert.False(capability.VerifyMac(OtherKey));

        DataCapability decoded = DataCapability.Decode(capability.Encode());
        WireDeepEquality.AssertEqual(capability, decoded);
        Assert.True(decoded.VerifyMac(Key));
    }

    [Fact]
    public void StagingCapabilityGrantsNoReads()
    {
        DataCapability capability = DataCapability.ForStaging(Guid.NewGuid(), new VolumeId(Guid.NewGuid()), 1, "bob", new StagingId(4), DateTimeOffset.UnixEpoch).Sign(Key);
        Assert.Equal(DataCapabilityKind.Staging, capability.Kind);
        Assert.False(capability.AllowsRead(new ObjectId(1)));
        Assert.True(capability.IsExpired(DateTimeOffset.UnixEpoch));
        Assert.False(capability.IsExpired(DateTimeOffset.UnixEpoch.AddTicks(-1)));
    }

    [Fact]
    public void TamperedCapabilitiesFailVerification()
    {
        DataCapability capability = DataCapability.ForRead(Guid.NewGuid(), new VolumeId(Guid.NewGuid()), 2, "alice", [new ObjectId(1)], DateTimeOffset.UnixEpoch).Sign(Key);
        Assert.False((capability with { Principal = "mallory" }).VerifyMac(Key));
        Assert.False((capability with { VolumeEpoch = 3 }).VerifyMac(Key));
        Assert.False((capability with { ExpiresUtc = capability.ExpiresUtc.AddYears(1) }).VerifyMac(Key));
        Assert.False((capability with { Objects = [new ObjectId(1), new ObjectId(2)] }).VerifyMac(Key));
        Assert.False((capability with { Kind = DataCapabilityKind.Staging }).VerifyMac(Key));
        Assert.False((capability with { Extensions = [new WireExtension(1, true, new byte[] { 1 })] }).VerifyMac(Key));
    }

    [Fact]
    public void MalformedCapabilityTokensAreRejected()
    {
        Assert.False(DataCapability.TryDecode([1, 2, 3], out DataCapability? none));
        Assert.Null(none);

        // Objects out of order are a non-canonical token and rejected by both encoder and decoder.
        DataCapability unordered = DataCapability.ForRead(Guid.Empty, default, 0, "p", [new ObjectId(1)], DateTimeOffset.UnixEpoch)
            with { Objects = [new ObjectId(2), new ObjectId(1)] };
        Assert.Throws<ArgumentException>(() => unordered.Encode());

        byte[] token = DataCapability.ForRead(Guid.Empty, default, 0, "p", [new ObjectId(1), new ObjectId(2)], DateTimeOffset.UnixEpoch).Encode();
        // Layout: kind(1) cluster(16) volume(16) epoch(1) principal(2) staging(1) count(1) → first object id at 38.
        const int first = 1 + 16 + 16 + 1 + 2 + 1 + 1;
        Assert.Equal(2, token[first - 1]);
        Assert.Equal(1, token[first]);
        token[first] = 3; // object 1 becomes 3 → order 3, 2
        Assert.False(DataCapability.TryDecode(token, out _));

        byte[] trailing = [.. DataCapability.ForStaging(Guid.Empty, default, 0, "p", new StagingId(1), DateTimeOffset.UnixEpoch).Encode(), 0];
        Assert.Throws<DfsProtocolException>(() => DataCapability.Decode(trailing));
    }
}
