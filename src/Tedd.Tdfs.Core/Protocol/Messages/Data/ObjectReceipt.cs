using System.Buffers.Binary;
using System.Security.Cryptography;
using Tedd.Tdfs.Crypto;

namespace Tedd.Tdfs.Protocol.Messages;

/// <summary>
/// Signed durability receipt for one stored object (ARCHITECTURE §6). A storage node issues it only after the object
/// record and its segment flush completed; the volume leader validates every receipt (MAC, binding to the committed
/// change, node incarnation current, placement epoch valid, distinct hosts/zones) before proposing a publication.
/// </summary>
/// <param name="Object">Stored object.</param>
/// <param name="Volume">Volume the object was stored for.</param>
/// <param name="Staging">Staging session the object was uploaded under (decides GC eligibility).</param>
/// <param name="Checksum">XxHash128 of the object's plaintext.</param>
/// <param name="Length">Plaintext length of the object.</param>
/// <param name="Encoding">Object encoding (<see cref="ObjectEncodings"/>).</param>
/// <param name="Node">Storage node that holds the durable copy.</param>
/// <param name="Incarnation">Incarnation of <paramref name="Node"/> that issued the receipt.</param>
/// <param name="PlacementEpoch">Placement epoch the upload targeted.</param>
/// <param name="DurableAtTicks">Storing node's clock (UTC ticks) when durability was reached (diagnostic; never used for ordering).</param>
/// <param name="Mac">HMAC-SHA256 with the cluster receipt key over <see cref="GetMacInput"/> (exactly <see cref="ProtocolInfo.MacSize"/> bytes).</param>
public sealed record ObjectReceipt(
    ObjectId Object,
    VolumeId Volume,
    StagingId Staging,
    UInt128 Checksum,
    long Length,
    byte Encoding,
    NodeId Node,
    ulong Incarnation,
    ulong PlacementEpoch,
    long DurableAtTicks,
    ReadOnlyMemory<byte> Mac) : IWireRecord<ObjectReceipt>
{
    /// <summary>Size of the canonical MAC input in bytes.</summary>
    public const int MacInputSize = 16 + 16 + 16 + 8 + 16 + 8 + 1 + 16 + 8 + 8 + 8;

    private static ReadOnlySpan<byte> MacLabel => "tdfs/receipt/v1\0"u8;

    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode; not covered by the MAC).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 16 + 16 + 1 + 16 + 1 + 1 + 16 + 1 + 1 + 1 + 1 + ProtocolInfo.MacSize + 1;

    /// <summary>
    /// Returns the canonical MAC input: the 16-byte label <c>"tdfs/receipt/v1\0"</c> followed by fixed-width
    /// little-endian fields in declaration order — object id (16), volume GUID (16), staging id (u64), checksum (16),
    /// length (i64), encoding (u8), node GUID (16), incarnation (u64), placement epoch (u64), durable-at ticks (i64).
    /// </summary>
    public byte[] GetMacInput()
    {
        var buffer = new byte[MacInputSize];
        Span<byte> s = buffer;
        MacLabel.CopyTo(s);
        s = s[MacLabel.Length..];
        BinaryPrimitives.WriteUInt128LittleEndian(s, Object.Value);
        s = s[16..];
        WriteGuid(ref s, Volume.Value);
        BinaryPrimitives.WriteUInt64LittleEndian(s, Staging.Value);
        s = s[8..];
        BinaryPrimitives.WriteUInt128LittleEndian(s, Checksum);
        s = s[16..];
        BinaryPrimitives.WriteInt64LittleEndian(s, Length);
        s = s[8..];
        s[0] = Encoding;
        s = s[1..];
        WriteGuid(ref s, Node.Value);
        BinaryPrimitives.WriteUInt64LittleEndian(s, Incarnation);
        s = s[8..];
        BinaryPrimitives.WriteUInt64LittleEndian(s, PlacementEpoch);
        s = s[8..];
        BinaryPrimitives.WriteInt64LittleEndian(s, DurableAtTicks);
        return buffer;
    }

    /// <summary>Returns a copy of this receipt carrying the MAC computed with <paramref name="receiptKey"/>.</summary>
    public ObjectReceipt Sign(ReadOnlySpan<byte> receiptKey) => this with { Mac = HmacHelpers.Compute(receiptKey, GetMacInput()) };

    /// <summary>Verifies the MAC in constant time.</summary>
    public bool VerifyMac(ReadOnlySpan<byte> receiptKey)
        => Mac.Length == ProtocolInfo.MacSize && HmacHelpers.Verify(receiptKey, GetMacInput(), Mac.Span);

    /// <summary>True when the receipt covers exactly the object identity, checksum, length and encoding of a published range's object.</summary>
    public bool Covers(ObjectId obj, UInt128 checksum, long objectLength, byte encoding)
        => Object == obj && Checksum == checksum && Length == objectLength && Encoding == encoding;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        if (Mac.Length != ProtocolInfo.MacSize)
            throw new ArgumentException($"Receipt MAC must be exactly {ProtocolInfo.MacSize} bytes.", nameof(Mac));
        writer.WriteObjectId(Object);
        writer.WriteVolumeId(Volume);
        writer.WriteStagingId(Staging);
        writer.WriteUInt128(Checksum);
        writer.WriteVarInt(Length);
        writer.WriteByte(Encoding);
        writer.WriteNodeId(Node);
        writer.WriteVarUInt(Incarnation);
        writer.WriteVarUInt(PlacementEpoch);
        writer.WriteVarInt(DurableAtTicks);
        writer.WriteBytes(Mac.Span);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static ObjectReceipt Read(ref WireReader reader)
    {
        ObjectId obj = reader.ReadObjectId();
        VolumeId volume = reader.ReadVolumeId();
        StagingId staging = reader.ReadStagingId();
        UInt128 checksum = reader.ReadUInt128();
        long length = reader.ReadVarInt();
        byte encoding = reader.ReadByte();
        NodeId node = reader.ReadNodeId();
        ulong incarnation = reader.ReadVarUInt();
        ulong placementEpoch = reader.ReadVarUInt();
        long durableAt = reader.ReadVarInt();
        ReadOnlyMemory<byte> mac = reader.ReadFixedBytes(ProtocolInfo.MacSize, nameof(Mac));
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new ObjectReceipt(obj, volume, staging, checksum, length, encoding, node, incarnation, placementEpoch, durableAt, mac)
        {
            Extensions = extensions,
        };
    }

    private static void WriteGuid(ref Span<byte> s, Guid value)
    {
        if (!value.TryWriteBytes(s, bigEndian: false, out int written) || written != 16)
            throw new CryptographicException("Failed to encode a GUID into the MAC input.");
        s = s[16..];
    }
}
