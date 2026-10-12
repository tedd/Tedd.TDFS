using System.Buffers;
using Tedd.Tdfs.Crypto;

namespace Tedd.Tdfs.Protocol.Messages;

/// <summary>
/// Short-lived, MAC-protected authorization for data-plane requests (ARCHITECTURE §5.4, §11). The volume leader issues
/// it; storage nodes verify it without contacting the leader. Object ids alone grant nothing (SEC-004): a
/// <see cref="DataCapabilityKind.Staging"/> capability allows <c>PutObject</c> into one open staging session, a
/// <see cref="DataCapabilityKind.Read"/> capability allows <c>GetObject</c> on an explicit set of objects. Clients treat
/// the encoded token (<see cref="Encode"/>) as opaque bytes.
/// </summary>
/// <param name="Kind">Capability scope.</param>
/// <param name="ClusterId">Issuing cluster.</param>
/// <param name="Volume">Volume the objects belong to.</param>
/// <param name="VolumeEpoch">Volume epoch at issue time (a capability of an older epoch is rejected, SEC-007).</param>
/// <param name="Principal">Principal the capability was issued to (verifiers compare it with the connection's principal).</param>
/// <param name="Staging">Staging session (<see cref="DataCapabilityKind.Staging"/>); empty for read capabilities.</param>
/// <param name="Objects">Readable objects in strictly ascending order (<see cref="DataCapabilityKind.Read"/>); empty for staging capabilities.</param>
/// <param name="ExpiresUtc">Expiry on the issuer's wall clock; verifiers reject expired capabilities using their own clock.</param>
/// <param name="Mac">HMAC-SHA256 with the cluster capability key over <see cref="GetMacInput"/>; empty before <see cref="Sign"/>.</param>
public sealed record DataCapability(
    DataCapabilityKind Kind,
    Guid ClusterId,
    VolumeId Volume,
    ulong VolumeEpoch,
    string Principal,
    StagingId Staging,
    IReadOnlyList<ObjectId> Objects,
    DateTimeOffset ExpiresUtc,
    ReadOnlyMemory<byte> Mac) : IWireRecord<DataCapability>
{
    private static ReadOnlySpan<byte> MacLabel => "tdfs/capability/v1\0"u8;

    /// <summary>Optional extensions (covered by the MAC, so issuers can add scope restrictions that old verifiers must not ignore).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 1 + 16 + 16 + 1 + 1 + 1 + 1 + 1 + 1 + 1;

    /// <summary>Creates an unsigned staging capability.</summary>
    public static DataCapability ForStaging(Guid clusterId, VolumeId volume, ulong volumeEpoch, string principal, StagingId staging, DateTimeOffset expiresUtc)
        => new(DataCapabilityKind.Staging, clusterId, volume, volumeEpoch, principal, staging, [], expiresUtc, ReadOnlyMemory<byte>.Empty);

    /// <summary>Creates an unsigned read capability for <paramref name="objects"/> (sorted and de-duplicated).</summary>
    public static DataCapability ForRead(Guid clusterId, VolumeId volume, ulong volumeEpoch, string principal, IEnumerable<ObjectId> objects, DateTimeOffset expiresUtc)
    {
        ArgumentNullException.ThrowIfNull(objects);
        ObjectId[] sorted = objects.Distinct().ToArray();
        Array.Sort(sorted, static (a, b) => a.Value.CompareTo(b.Value));
        return new(DataCapabilityKind.Read, clusterId, volume, volumeEpoch, principal, default, sorted, expiresUtc, ReadOnlyMemory<byte>.Empty);
    }

    /// <summary>
    /// Returns the canonical MAC input: the label <c>"tdfs/capability/v1\0"</c> followed by the wire encoding of every
    /// field except <see cref="Mac"/>, followed by the extension block.
    /// </summary>
    public byte[] GetMacInput()
    {
        var buffer = new ArrayBufferWriter<byte>(64 + 16 * (Objects?.Count ?? 0));
        var writer = new WireWriter(buffer);
        writer.WriteRaw(MacLabel);
        WriteFields(ref writer);
        writer.WriteExtensionBlock(Extensions);
        return buffer.WrittenSpan.ToArray();
    }

    /// <summary>Returns a copy carrying the MAC computed with <paramref name="capabilityKey"/>.</summary>
    public DataCapability Sign(ReadOnlySpan<byte> capabilityKey) => this with { Mac = HmacHelpers.Compute(capabilityKey, GetMacInput()) };

    /// <summary>Verifies the MAC in constant time.</summary>
    public bool VerifyMac(ReadOnlySpan<byte> capabilityKey)
        => Mac.Length == ProtocolInfo.MacSize && HmacHelpers.Verify(capabilityKey, GetMacInput(), Mac.Span);

    /// <summary>True when the capability has expired at <paramref name="nowUtc"/>.</summary>
    public bool IsExpired(DateTimeOffset nowUtc) => nowUtc >= ExpiresUtc;

    /// <summary>True when this is a read capability listing <paramref name="obj"/>.</summary>
    public bool AllowsRead(ObjectId obj)
    {
        if (Kind != DataCapabilityKind.Read || Objects is null)
            return false;
        int lo = 0, hi = Objects.Count - 1;
        while (lo <= hi)
        {
            int mid = lo + ((hi - lo) >> 1);
            int cmp = Objects[mid].Value.CompareTo(obj.Value);
            if (cmp == 0)
                return true;
            if (cmp < 0)
                lo = mid + 1;
            else
                hi = mid - 1;
        }

        return false;
    }

    /// <summary>Encodes the capability as an opaque token.</summary>
    public byte[] Encode()
    {
        var buffer = new ArrayBufferWriter<byte>(96 + 16 * (Objects?.Count ?? 0));
        var writer = new WireWriter(buffer);
        Write(ref writer);
        return buffer.WrittenSpan.ToArray();
    }

    /// <summary>Decodes a token produced by <see cref="Encode"/>.</summary>
    /// <exception cref="DfsProtocolException">The token is malformed (every decoding failure surfaces as this type, so
    /// <see cref="TryDecode"/> never lets another exception escape from a hostile token).</exception>
    public static DataCapability Decode(ReadOnlySpan<byte> token, WireLimits? limits = null)
        => WireRecordCodec.Decode<DataCapability>(token, limits);

    /// <summary>Tries to decode a token; false when it is malformed or requires unsupported extensions.</summary>
    public static bool TryDecode(ReadOnlySpan<byte> token, out DataCapability? capability, WireLimits? limits = null)
    {
        try
        {
            capability = Decode(token, limits);
            return true;
        }
        catch (DfsProtocolException)
        {
            capability = null;
            return false;
        }
    }

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        if (Mac.Length != 0 && Mac.Length != ProtocolInfo.MacSize)
            throw new ArgumentException($"Capability MAC must be empty or exactly {ProtocolInfo.MacSize} bytes.", nameof(Mac));
        WriteFields(ref writer);
        writer.WriteBytes(Mac.Span);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static DataCapability Read(ref WireReader reader)
    {
        DataCapabilityKind kind = reader.ReadEnum<DataCapabilityKind>();
        Guid cluster = reader.ReadGuid();
        VolumeId volume = reader.ReadVolumeId();
        ulong epoch = reader.ReadVarUInt();
        string principal = reader.ReadString();
        StagingId staging = reader.ReadStagingId();
        IReadOnlyList<ObjectId> objects = reader.ReadObjectIdList();
        for (int i = 1; i < objects.Count; i++)
        {
            if (objects[i - 1].Value >= objects[i].Value)
                throw WireReader.Malformed("Capability objects must be distinct and in ascending order.");
        }

        DateTimeOffset expires = reader.ReadDateTimeOffset();
        ReadOnlyMemory<byte> mac = reader.ReadEmptyOrFixedBytes(ProtocolInfo.MacSize, nameof(Mac));
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new DataCapability(kind, cluster, volume, epoch, principal, staging, objects, expires, mac) { Extensions = extensions };
    }

    private void WriteFields(ref WireWriter writer)
    {
        if (Objects is not null)
        {
            for (int i = 1; i < Objects.Count; i++)
            {
                if (Objects[i - 1].Value >= Objects[i].Value)
                    throw new ArgumentException("Capability objects must be distinct and in ascending order (use ForRead).", nameof(Objects));
            }
        }

        writer.WriteDefinedEnum(Kind);
        writer.WriteGuid(ClusterId);
        writer.WriteVolumeId(Volume);
        writer.WriteVarUInt(VolumeEpoch);
        writer.WriteString(Principal);
        writer.WriteStagingId(Staging);
        writer.WriteObjectIdList(Objects);
        writer.WriteDateTimeOffset(ExpiresUtc);
    }
}
