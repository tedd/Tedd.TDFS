namespace Tedd.Tdfs.Storage;

/// <summary>Stored encodings of object payloads (<see cref="ObjectRecordInfo.Encoding"/>).</summary>
public static class ObjectEncoding
{
    /// <summary>Plaintext payload.</summary>
    public const byte Plain = 0;

    /// <summary>Payload sealed per 64 KiB unit with the store's <see cref="IRecordCipher"/> (nonce + ciphertext + tag per unit).</summary>
    public const byte SealedUnits = 1;
}

/// <summary>Physical placement of an object record (diagnostics and corruption tests).</summary>
/// <param name="SegmentPath">Medium-relative path of the segment file.</param>
/// <param name="RecordOffset">Offset of the record header in the segment.</param>
/// <param name="HeaderLength">Length of the record header (including unit hashes and header CRC).</param>
/// <param name="PayloadOffset">Offset of the first stored payload byte in the segment.</param>
/// <param name="StoredLength">Stored payload length (plaintext length plus per-unit encryption overhead).</param>
/// <param name="UnitSize">Plaintext bytes per verification unit.</param>
/// <param name="Encoding">Stored encoding (<see cref="ObjectEncoding"/>).</param>
public sealed record ObjectPhysicalLocation(string SegmentPath, long RecordOffset, int HeaderLength, long PayloadOffset, long StoredLength, int UnitSize, byte Encoding);

/// <summary>A stored record that failed verification and was removed from the index (quarantined).</summary>
/// <param name="Id">Object id, when the record header was readable.</param>
/// <param name="SegmentPath">Segment holding the damaged bytes.</param>
/// <param name="Offset">Offset of the damaged record or region.</param>
/// <param name="Reason">Why the record was quarantined.</param>
public sealed record QuarantinedRecord(ObjectId? Id, string SegmentPath, long Offset, string Reason);

/// <summary>
/// The requested object is not held by this store (<see cref="DfsErrorCode.Integrity"/> with a "missing" detail):
/// callers must try another replica and must never substitute zeroes.
/// </summary>
public sealed class DfsObjectMissingException : DfsIntegrityException
{
    /// <summary>Creates the exception for <paramref name="objectId"/>.</summary>
    public DfsObjectMissingException(ObjectId objectId)
        : base(objectId, $"Object {objectId} is missing from this store.")
    {
    }

    /// <summary>Always true: the object is absent rather than corrupt.</summary>
    public bool IsMissing => true;
}
