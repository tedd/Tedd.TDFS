using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;

namespace Tedd.Tdfs.Tests.Unit.Internal.Protocol;

/// <summary>
/// Fixed byte vectors (FMT-001/004): the wire layout documented in PROTOCOL.md, derived by hand. A change to any of
/// these vectors is a wire-format break and needs a protocol version/feature decision, not a test update.
/// </summary>
public sealed class GoldenVectorTests
{
    private static readonly Guid ClusterGuid = Guid.Parse("00112233-4455-6677-8899-aabbccddeeff");
    private static readonly Guid VolumeGuid = Guid.Parse("10203040-5060-7080-90a0-b0c0d0e0f001");

    // GUIDs use the standard little-endian layout: the first three groups are byte-swapped.
    private const string ClusterGuidHex = "33221100554477668899AABBCCDDEEFF";
    private const string VolumeGuidHex = "403020106050807090A0B0C0D0E0F001";

    [Fact]
    public void GetClusterMap()
        => AssertVector(new GetClusterMapMessage(300), "0E00" + "AC02" + "00");

    [Fact]
    public void ErrorReplyPreservesUnknownOutcomeIdentity()
        => AssertVector(
            new ErrorReplyMessage(DfsErrorCode.OutcomeUnknown, "x", new DfsOperationId(1, 2), Retryable: true),
            "0100" + "2E" + "0178" + "010102" + "01" + "00" + "00" + "00");

    [Fact]
    public void HelloProof()
        => AssertVector(new HelloProofMessage(Enumerable.Repeat((byte)0xAB, 32).ToArray()),
            "0C00" + "20" + string.Concat(Enumerable.Repeat("AB", 32)) + "00");

    [Fact]
    public void DeleteWithRequestHeader()
        => AssertVector(
            new DeleteMessage(
                new RequestHeader("vol", default, ExpectedVolumeEpoch: 2, Session: 5, Operation: new DfsOperationId(5, 7), AckWatermark: 6),
                "/a", Recursive: true, ExpectDirectory: false, SnapshotName: null),
            "3E00" +
            "03766F6C" + new string('0', 32) + "02" + "05" + "0507" + "06" + "00" + "00" + // header
            "022F61" + "01" + "00" + "00" + // path, recursive, expectDirectory, no snapshot
            "00"); // extension block

    [Fact]
    public void TxAbortResultWithCommitToken()
        => AssertVector(
            new TxAbortResultMessage(WireOutcomeStatus.Committed, new CommitToken(ClusterGuid, new VolumeId(VolumeGuid), 1, 300)),
            "5700" + "00" + ClusterGuidHex + VolumeGuidHex + "01" + "AC02" + "00");

    [Fact]
    public void NegativeAndExtremeIntegers()
        => AssertVector(
            new RaftSnapshotChunkResultMessage(Term: ulong.MaxValue, SnapshotIndex: 0, NextOffset: -1, Reject: true),
            "8500" + "FFFFFFFFFFFFFFFFFF01" + "00" + "01" + "01" + "00");

    [Fact]
    public void ExtensionBlockLayout()
        => AssertVector(
            new GetClusterMapMessage(0) { Extensions = [new WireExtension(300, false, new byte[] { 0xCA, 0xFE })] },
            "0E00" + "00" + "01" + "AC02" + "00" + "02CAFE");

    [Fact]
    public void ObjectReceiptMacInputLayout()
    {
        var receipt = new ObjectReceipt(
            new ObjectId(UInt128.One), new VolumeId(VolumeGuid), new StagingId(2), new UInt128(0, 3), 4, 5,
            new NodeId(ClusterGuid), 6, 7, 8, new byte[32]);
        string expected =
            Convert.ToHexString("tdfs/receipt/v1\0"u8) +
            "01000000000000000000000000000000" + // object id
            VolumeGuidHex +
            "0200000000000000" + // staging
            "03000000000000000000000000000000" + // checksum
            "0400000000000000" + // length
            "05" + // encoding
            ClusterGuidHex + // node
            "0600000000000000" + "0700000000000000" + "0800000000000000"; // incarnation, placement epoch, durable-at
        Assert.Equal(ObjectReceipt.MacInputSize, receipt.GetMacInput().Length);
        Assert.Equal(expected, Convert.ToHexString(receipt.GetMacInput()));
    }

    private static void AssertVector(IWireMessage message, string expectedHex)
    {
        byte[] frame = MessageCodec.Encode(message);
        Assert.Equal(expectedHex, Convert.ToHexString(frame));
        IWireMessage decoded = MessageCodec.Decode(Convert.FromHexString(expectedHex));
        WireDeepEquality.AssertEqual(message, decoded);
    }
}
