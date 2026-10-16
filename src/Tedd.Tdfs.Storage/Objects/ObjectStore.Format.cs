using System.Buffers.Binary;
using System.IO.Hashing;
using Tedd.Tdfs.Data;

namespace Tedd.Tdfs.Storage;

// On-disk format of object segments.
//
// Segment file "{major:x16}-{minor:x8}.oseg" (".ocmp" while a compaction output is unpublished), ordered by
// (major, minor). Header (32 bytes): u32 magic "TOSG" | u16 version | u16 flags | u64 major | u32 minor | u32 0 | u32 0 |
// u32 CRC-32C.
//
// Record header (fixed 96 bytes, then unit hashes, extra bytes and a CRC):
//   0 u32 magic "TOBR" | 4 u8 kind | 5 u8 version | 6 u8 encoding | 7 u8 flags (0)
//   8 u128 objectId | 24 guid volumeId | 40 u64 stagingId | 48 u64 plaintextLength | 56 u64 storedLength
//   64 u32 keyVersion | 68 u32 unitSize | 72 u128 checksum (XxHash128 of plaintext) | 88 u32 unitCount | 92 u32 extraLength
//   96 u64[unitCount] unit hashes (XxHash64 of each plaintext unit, seeded with the object id and unit index)
//   .. extra (quarantine records: u64 target major | u32 target minor | u64 target offset)
//   .. u32 CRC-32C over every header byte before it
// followed by storedLength payload bytes: units in order, each plaintext (encoding 0) or sealed with associated data
// objectId | volumeId | u32 unit index | u64 plaintextLength (encoding 1).
public sealed partial class ObjectStore
{
    /// <summary>File extension of published object segments.</summary>
    public const string SegmentExtension = ".oseg";

    /// <summary>File extension of compaction outputs that are not yet published.</summary>
    public const string CompactingExtension = ".ocmp";

    /// <summary>Name of the durable compaction manifest (written with <see cref="AtomicFile"/>).</summary>
    public const string ManifestFileName = "compaction.manifest";

    /// <summary>Plaintext bytes per verification unit (64 KiB).</summary>
    public const int UnitSize = 64 * 1024;

    /// <summary>Segment and record format version written by this implementation.</summary>
    public const ushort FormatVersion = 1;

    internal const int SegmentHeaderSize = 32;
    internal const int FixedHeaderSize = 96;
    internal const byte KindObject = 1;
    internal const byte KindTombstone = 2;
    internal const byte KindQuarantine = 3;
    internal const int QuarantineExtraSize = 20;
    private const int MaxUnitCount = 1 << 20;
    private const int MaxUnitSize = 16 << 20;
    private const int MaxExtraLength = 1024;

    internal static string SegmentFileName(SegmentKey key, bool compacting = false)
        => StorageFormat.Hex16(key.Major) + "-" + StorageFormat.Hex8(key.Minor) + (compacting ? CompactingExtension : SegmentExtension);

    internal static bool TryParseSegmentFileName(string name, out SegmentKey key, out bool compacting)
    {
        key = default;
        compacting = false;
        string ext;
        if (name.EndsWith(SegmentExtension, StringComparison.Ordinal))
            ext = SegmentExtension;
        else if (name.EndsWith(CompactingExtension, StringComparison.Ordinal))
            ext = CompactingExtension;
        else
            return false;
        ReadOnlySpan<char> stem = name.AsSpan(0, name.Length - ext.Length);
        if (stem.Length != 25 || stem[16] != '-')
            return false;
        if (!StorageFormat.TryParseHex16(stem[..16], out ulong major) || !StorageFormat.TryParseHex8(stem[17..], out uint minor))
            return false;
        key = new SegmentKey(major, minor);
        compacting = ext == CompactingExtension;
        return true;
    }

    internal static byte[] EncodeSegmentHeader(SegmentKey key)
    {
        var header = new byte[SegmentHeaderSize];
        Span<byte> s = header;
        BinaryPrimitives.WriteUInt32LittleEndian(s, StorageFormat.ObjectSegmentMagic);
        BinaryPrimitives.WriteUInt16LittleEndian(s[4..], FormatVersion);
        BinaryPrimitives.WriteUInt64LittleEndian(s[8..], key.Major);
        BinaryPrimitives.WriteUInt32LittleEndian(s[16..], key.Minor);
        StorageFormat.SealCrc(s, 28);
        return header;
    }

    /// <summary>Validates a segment header; returns false when damaged, throws for a newer format.</summary>
    internal static bool CheckSegmentHeader(ReadOnlySpan<byte> header, SegmentKey key, string path)
    {
        if (header.Length < SegmentHeaderSize
            || BinaryPrimitives.ReadUInt32LittleEndian(header) != StorageFormat.ObjectSegmentMagic
            || !StorageFormat.CheckCrc(header, 28))
        {
            return false;
        }

        ushort version = BinaryPrimitives.ReadUInt16LittleEndian(header[4..]);
        if (version != FormatVersion || BinaryPrimitives.ReadUInt16LittleEndian(header[6..]) != 0)
        {
            throw new DfsProtocolException(DfsErrorCode.Unsupported,
                $"Object segment '{path}' uses format version {version}; this software supports version {FormatVersion}.");
        }

        return BinaryPrimitives.ReadUInt64LittleEndian(header[8..]) == key.Major
            && BinaryPrimitives.ReadUInt32LittleEndian(header[16..]) == key.Minor;
    }

    internal static int UnitCountFor(long plaintextLength) => checked((int)((plaintextLength + UnitSize - 1) / UnitSize));

    internal static long UnitSeed(ObjectId id, int unit)
    {
        ulong lo = (ulong)id.Value;
        ulong hi = (ulong)(id.Value >> 64);
        return (long)(lo ^ (hi * 0x9E3779B97F4A7C15UL) ^ ((ulong)(uint)unit * 0xC2B2AE3D27D4EB4FUL));
    }

    internal static ulong UnitHash(ObjectId id, int unit, ReadOnlySpan<byte> plain) => XxHash64.HashToUInt64(plain, UnitSeed(id, unit));

    internal static void WriteUnitAad(Span<byte> aad, ObjectId id, VolumeId volume, int unit, long plaintextLength)
    {
        id.WriteBytes(aad);
        volume.Value.TryWriteBytes(aad[16..]);
        BinaryPrimitives.WriteUInt32LittleEndian(aad[32..], (uint)unit);
        BinaryPrimitives.WriteInt64LittleEndian(aad[36..], plaintextLength);
    }

    internal const int UnitAadSize = 44;

    /// <summary>Builds a complete object record (header + payload).</summary>
    private byte[] BuildObjectRecord(ObjectId id, VolumeId volume, StagingId staging, ReadOnlySpan<byte> plain, UInt128 checksum, uint requestedKeyVersion,
        out RecordHeader header)
    {
        int unitCount = UnitCountFor(plain.Length);
        (IRecordCipher? cipher, byte encoding) = SealingCipher(volume);
        bool encrypt = cipher is not null;
        int overhead = encrypt ? (ReferenceEquals(cipher, _cipher) ? _cipherOverhead : CipherSupport.ValidateOverhead(cipher!)) : 0;
        uint keyVersion = 0;
        if (encrypt)
        {
            keyVersion = requestedKeyVersion != 0 ? requestedKeyVersion : CipherSupport.CurrentVersion(cipher!);
            if (!cipher!.CanDecrypt(keyVersion))
                throw new DfsRecoveryKeyMissingException($"Object {id}: key version {keyVersion} of volume {volume} is not available on this node.");
        }

        long stored = (long)plain.Length + ((long)unitCount * overhead);
        int headerLength = FixedHeaderSize + (8 * unitCount) + 4;
        var record = new byte[checked(headerLength + stored)];
        var hashes = new ulong[unitCount];
        Span<byte> aad = stackalloc byte[UnitAadSize];
        long storedOffset = headerLength;
        for (int u = 0; u < unitCount; u++)
        {
            int start = u * UnitSize;
            int length = Math.Min(UnitSize, plain.Length - start);
            ReadOnlySpan<byte> unit = plain.Slice(start, length);
            hashes[u] = UnitHash(id, u, unit);
            if (encrypt)
            {
                WriteUnitAad(aad, id, volume, u, plain.Length);
                cipher!.Encrypt(aad, unit, record.AsSpan((int)storedOffset, length + overhead), keyVersion);
                storedOffset += length + overhead;
            }
            else
            {
                unit.CopyTo(record.AsSpan((int)storedOffset));
                storedOffset += length;
            }
        }

        header = new RecordHeader(KindObject, encoding, id, volume, staging,
            plain.Length, stored, keyVersion, UnitSize, checksum, hashes, default, 0, headerLength);
        WriteHeader(record, header);
        return record;
    }

    /// <summary>
    /// The cipher new records of <paramref name="volume"/> are sealed with and the resulting encoding: the volume's data keys
    /// when per-volume keys are configured (missing key material throws <see cref="DfsRecoveryKeyMissingException"/>),
    /// otherwise the node-wide cipher, otherwise none (plaintext).
    /// </summary>
    private (IRecordCipher? Cipher, byte Encoding) SealingCipher(VolumeId volume)
    {
        if (_options.VolumeKeys is { } keys)
        {
            IRecordCipher cipher = keys.GetCipher(volume)
                ?? throw new DfsRecoveryKeyMissingException($"No data-encryption key of volume {volume} is available on this node.");
            return (cipher, ObjectEncoding.SealedVolumeUnits);
        }

        return _cipher is null ? (null, ObjectEncoding.Plain) : (_cipher, ObjectEncoding.SealedUnits);
    }

    /// <summary>Builds a tombstone (delete) or quarantine record (header only).</summary>
    private static byte[] BuildMarkerRecord(byte kind, ObjectId id, VolumeId volume, SegmentKey targetKey = default, long targetOffset = 0)
    {
        int extra = kind == KindQuarantine ? QuarantineExtraSize : 0;
        int headerLength = FixedHeaderSize + extra + 4;
        var record = new byte[headerLength];
        var header = new RecordHeader(kind, ObjectEncoding.Plain, id, volume, default, 0, 0, 0, UnitSize, 0, [],
            targetKey, targetOffset, headerLength);
        WriteHeader(record, header);
        return record;
    }

    private static void WriteHeader(Span<byte> record, RecordHeader h)
    {
        BinaryPrimitives.WriteUInt32LittleEndian(record, StorageFormat.ObjectRecordMagic);
        record[4] = h.Kind;
        record[5] = (byte)FormatVersion;
        record[6] = h.Encoding;
        record[7] = 0;
        h.Id.WriteBytes(record[8..]);
        h.Volume.Value.TryWriteBytes(record[24..]);
        BinaryPrimitives.WriteUInt64LittleEndian(record[40..], h.Staging.Value);
        BinaryPrimitives.WriteInt64LittleEndian(record[48..], h.PlaintextLength);
        BinaryPrimitives.WriteInt64LittleEndian(record[56..], h.StoredLength);
        BinaryPrimitives.WriteUInt32LittleEndian(record[64..], h.KeyVersion);
        BinaryPrimitives.WriteUInt32LittleEndian(record[68..], (uint)h.UnitSize);
        BinaryPrimitives.WriteUInt128LittleEndian(record[72..], h.Checksum);
        BinaryPrimitives.WriteUInt32LittleEndian(record[88..], (uint)h.UnitHashes.Length);
        int extra = h.Kind == KindQuarantine ? QuarantineExtraSize : 0;
        BinaryPrimitives.WriteUInt32LittleEndian(record[92..], (uint)extra);
        int pos = FixedHeaderSize;
        foreach (ulong hash in h.UnitHashes)
        {
            BinaryPrimitives.WriteUInt64LittleEndian(record[pos..], hash);
            pos += 8;
        }

        if (extra > 0)
        {
            BinaryPrimitives.WriteUInt64LittleEndian(record[pos..], h.TargetKey.Major);
            BinaryPrimitives.WriteUInt32LittleEndian(record[(pos + 8)..], h.TargetKey.Minor);
            BinaryPrimitives.WriteInt64LittleEndian(record[(pos + 12)..], h.TargetOffset);
            pos += extra;
        }

        StorageFormat.SealCrc(record, pos);
    }

    /// <summary>Header length implied by the fixed part, or -1 when the fixed part is implausible.</summary>
    private static int ImpliedHeaderLength(ReadOnlySpan<byte> fixedPart)
    {
        if (BinaryPrimitives.ReadUInt32LittleEndian(fixedPart) != StorageFormat.ObjectRecordMagic)
            return -1;
        uint unitCount = BinaryPrimitives.ReadUInt32LittleEndian(fixedPart[88..]);
        uint extra = BinaryPrimitives.ReadUInt32LittleEndian(fixedPart[92..]);
        if (unitCount > MaxUnitCount || extra > MaxExtraLength)
            return -1;
        return FixedHeaderSize + (int)(unitCount * 8) + (int)extra + 4;
    }

    /// <summary>Parses and validates a complete header. Returns false when damaged; throws for a newer record version.</summary>
    private static bool TryParseHeader(ReadOnlySpan<byte> raw, string path, long offset, out RecordHeader header)
    {
        header = default!;
        int headerLength = raw.Length;
        if (headerLength < FixedHeaderSize + 4 || ImpliedHeaderLength(raw) != headerLength || !StorageFormat.CheckCrc(raw, headerLength - 4))
            return false;

        byte kind = raw[4];
        byte version = raw[5];
        byte encoding = raw[6];
        if (version != FormatVersion)
        {
            throw new DfsProtocolException(DfsErrorCode.Unsupported,
                $"Object record at offset {offset} of '{path}' uses format version {version}; this software supports version {FormatVersion}.");
        }

        var id = ObjectId.FromBytes(raw[8..]);
        var volume = new VolumeId(new Guid(raw.Slice(24, 16)));
        var staging = new StagingId(BinaryPrimitives.ReadUInt64LittleEndian(raw[40..]));
        long plainLength = BinaryPrimitives.ReadInt64LittleEndian(raw[48..]);
        long stored = BinaryPrimitives.ReadInt64LittleEndian(raw[56..]);
        uint keyVersion = BinaryPrimitives.ReadUInt32LittleEndian(raw[64..]);
        uint unitSize = BinaryPrimitives.ReadUInt32LittleEndian(raw[68..]);
        UInt128 checksum = BinaryPrimitives.ReadUInt128LittleEndian(raw[72..]);
        int unitCount = (int)BinaryPrimitives.ReadUInt32LittleEndian(raw[88..]);
        int extra = (int)BinaryPrimitives.ReadUInt32LittleEndian(raw[92..]);
        if (raw[7] != 0 || id.IsEmpty || plainLength < 0 || stored < 0 || unitSize == 0 || unitSize > MaxUnitSize)
            return false;

        SegmentKey targetKey = default;
        long targetOffset = 0;
        switch (kind)
        {
            case KindObject:
                if (extra != 0 || unitCount != (int)((plainLength + unitSize - 1) / unitSize))
                    return false;
                if (encoding == ObjectEncoding.Plain)
                {
                    if (stored != plainLength || keyVersion != 0)
                        return false;
                }
                else if (encoding is ObjectEncoding.SealedUnits or ObjectEncoding.SealedVolumeUnits)
                {
                    if (keyVersion == 0 || stored < plainLength)
                        return false;
                    long overhead = stored - plainLength;
                    if (unitCount == 0 ? overhead != 0 : overhead % unitCount != 0 || overhead / unitCount > CipherSupport.MaxOverhead)
                        return false;
                }
                else
                {
                    return false;
                }

                break;
            case KindTombstone:
            case KindQuarantine:
                if (plainLength != 0 || stored != 0 || unitCount != 0 || encoding != ObjectEncoding.Plain || keyVersion != 0)
                    return false;
                if (extra != (kind == KindQuarantine ? QuarantineExtraSize : 0))
                    return false;
                if (kind == KindQuarantine)
                {
                    int p = FixedHeaderSize;
                    targetKey = new SegmentKey(BinaryPrimitives.ReadUInt64LittleEndian(raw[p..]), BinaryPrimitives.ReadUInt32LittleEndian(raw[(p + 8)..]));
                    targetOffset = BinaryPrimitives.ReadInt64LittleEndian(raw[(p + 12)..]);
                }

                break;
            default:
                return false;
        }

        var hashes = new ulong[unitCount];
        for (int u = 0; u < unitCount; u++)
            hashes[u] = BinaryPrimitives.ReadUInt64LittleEndian(raw[(FixedHeaderSize + (u * 8))..]);
        header = new RecordHeader(kind, encoding, id, volume, staging, plainLength, stored, keyVersion, (int)unitSize, checksum,
            hashes, targetKey, targetOffset, headerLength);
        return true;
    }

    /// <summary>Order key of a segment file: (major, minor) compared lexicographically.</summary>
    internal readonly record struct SegmentKey(ulong Major, uint Minor) : IComparable<SegmentKey>
    {
        public int CompareTo(SegmentKey other)
        {
            int c = Major.CompareTo(other.Major);
            return c != 0 ? c : Minor.CompareTo(other.Minor);
        }

        public override string ToString() => SegmentFileName(this);
    }

    /// <summary>Decoded record header.</summary>
    private sealed record RecordHeader(
        byte Kind,
        byte Encoding,
        ObjectId Id,
        VolumeId Volume,
        StagingId Staging,
        long PlaintextLength,
        long StoredLength,
        uint KeyVersion,
        int UnitSize,
        UInt128 Checksum,
        ulong[] UnitHashes,
        SegmentKey TargetKey,
        long TargetOffset,
        int HeaderLength)
    {
        public long RecordLength => HeaderLength + StoredLength;
    }

    /// <summary>XxHash128 of the concatenated plaintext of every unit (same value as <see cref="ObjectChecksum.Compute"/>).</summary>
    private sealed class WholeObjectHash
    {
        private readonly XxHash128 _hash = new();

        public void Append(ReadOnlySpan<byte> unit) => _hash.Append(unit);

        public UInt128 Value => _hash.GetCurrentHashAsUInt128();
    }
}
