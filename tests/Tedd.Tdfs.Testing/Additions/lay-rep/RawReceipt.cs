namespace Tedd.Tdfs.Testing;

/// <summary>
/// A storage receipt as returned by <see cref="RawStagedWriter.PutObjectAsync"/>: the decoded fields (informational) and
/// the exact wire encoding including the MAC (ARCHITECTURE §6). <see cref="RawStagedWriter.CommitExtentsAsync"/> sends
/// <see cref="Wire"/> verbatim, so a test can replay a receipt for another commit or tamper with its bytes.
/// </summary>
/// <param name="Object">Object id the receipt certifies.</param>
/// <param name="Checksum">Object checksum (xxHash128 of the plaintext) bound by the receipt.</param>
/// <param name="Length">Object plaintext length bound by the receipt.</param>
/// <param name="Encoding">Object encoding bound by the receipt.</param>
/// <param name="Node">Issuing node id.</param>
/// <param name="NodeName">Issuing node name.</param>
/// <param name="Incarnation">Issuing node incarnation.</param>
/// <param name="PlacementEpoch">Placement epoch the receipt was issued under.</param>
/// <param name="Wire">Exact encoded receipt (fields + MAC).</param>
public sealed record RawReceipt(ObjectId Object, UInt128 Checksum, long Length, byte Encoding, NodeId Node, string NodeName,
    ulong Incarnation, ulong PlacementEpoch, ReadOnlyMemory<byte> Wire)
{
    /// <summary>A copy whose last MAC byte is XOR-ed with 0xFF (the record stays decodable; only the MAC is wrong).</summary>
    /// <remarks>
    /// The MAC is not the last byte of the wire encoding: the record ends with its extension block, so flipping the final
    /// byte would corrupt the record framing instead of the MAC.
    /// </remarks>
    public RawReceipt WithTamperedMac()
    {
        Protocol.Messages.ObjectReceipt receipt = Protocol.WireRecordCodec.Decode<Protocol.Messages.ObjectReceipt>(Wire.Span);
        byte[] mac = receipt.Mac.ToArray();
        if (mac.Length > 0)
            mac[^1] ^= 0xFF;
        return this with { Wire = Protocol.WireRecordCodec.Encode(receipt with { Mac = mac }) };
    }
}

/// <summary>Result of <see cref="RawStagedWriter.PutObjectAsync"/>.</summary>
/// <param name="Error">Server error code (<see cref="DfsErrorCode.None"/> when a receipt was issued).</param>
/// <param name="Receipt">The receipt, or null when none was issued.</param>
public sealed record RawPutResult(DfsErrorCode Error, RawReceipt? Receipt);

/// <summary>
/// One extent ref of a raw <c>CommitFile</c> (ARCHITECTURE §5.1 <c>ExtentRef</c>). Every field is sent verbatim.
/// </summary>
/// <param name="Offset">Logical file offset.</param>
/// <param name="Length">Logical length.</param>
/// <param name="Object">Referenced object.</param>
/// <param name="ObjectOffset">Offset inside the object.</param>
/// <param name="ObjectLength">Declared object length.</param>
/// <param name="Checksum">Declared object checksum.</param>
/// <param name="Encoding">Declared object encoding.</param>
public sealed record RawExtentSpec(long Offset, long Length, ObjectId Object, long ObjectOffset, long ObjectLength, UInt128 Checksum, byte Encoding)
{
    /// <summary>An extent mapping <c>[offset, offset + length)</c> to <c>[objectOffset, objectOffset + length)</c> of the object certified by <paramref name="receipt"/>.</summary>
    public static RawExtentSpec FromReceipt(RawReceipt receipt, long offset, long length, long objectOffset = 0)
    {
        ArgumentNullException.ThrowIfNull(receipt);
        return new RawExtentSpec(offset, length, receipt.Object, objectOffset, receipt.Length, receipt.Checksum, receipt.Encoding);
    }
}
