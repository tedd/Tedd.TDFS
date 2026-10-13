using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Immutable;
using System.Security.Cryptography;
using System.Text;
using Tedd.Tdfs.Crypto;
using Tedd.Tdfs.Data;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Volumes.State;

namespace Tedd.Tdfs.Server.Backup;

/// <summary>One object of a backup snapshot's dependency closure: identity and the content it must have.</summary>
/// <param name="Id">Object id.</param>
/// <param name="Length">Plaintext length.</param>
/// <param name="Checksum">XxHash128 of the plaintext.</param>
/// <param name="Encoding">Object encoding.</param>
internal readonly record struct BackupObjectRef(ObjectId Id, long Length, UInt128 Checksum, byte Encoding);

/// <summary>
/// The configuration a restored volume takes over from its source (FSD §12.4: a backup is interpretable without the lost
/// source's metadata service): metadata policy, storage policies, inline threshold, limits, pool and traverse checking.
/// </summary>
internal sealed record BackupVolumeConfig(DfsMetadataPolicy Metadata, DfsStoragePolicy DefaultPolicy, IReadOnlyDictionary<string, DfsStoragePolicy> NamedPolicies,
    int InlineThreshold, DfsVolumeLimits Limits, string StoragePool, bool EnforceTraverseChecking);

/// <summary>
/// The manifest of one exported snapshot (ARCHITECTURE §12, PROTOCOL.md §8.6): source identity and boundary, consistency label
/// and adapter records, the requested retention, the volume configuration, the complete namespace (every inode of the
/// snapshot's tree with its streams, extent maps and security descriptor) and the dependency closure. The format is owned by
/// the backup module; in transit it is sealed with a key derived from the export credentials, at rest with the backup node's
/// own data key.
/// </summary>
internal sealed record BackupManifest(
    Guid SourceClusterId,
    string SourceClusterName,
    VolumeId VolumeId,
    string VolumeName,
    ulong Epoch,
    Guid Lineage,
    SnapshotId SnapshotId,
    string SnapshotName,
    CommitToken Boundary,
    DateTimeOffset CreatedUtc,
    DfsConsistencyLabel Label,
    IReadOnlyList<AdapterRecordData> Adapters,
    DfsRetention RequestedRetention,
    BackupVolumeConfig Config,
    IReadOnlyList<InodeRecord> Inodes,
    IReadOnlyList<BackupObjectRef> Closure)
{
    /// <summary>Plaintext format version written by this build.</summary>
    public const ushort FormatVersion = 1;

    /// <summary>Sealed (transit) format version.</summary>
    public const ushort SealedFormatVersion = 1;

    private const uint PlainMagic = 0x4D424454;   // "TDBM"
    private const uint SealedMagic = 0x58424454;  // "TDBX"
    private const int SealedHeaderSize = 4 + 2 + 2;
    private const int NonceSize = 12;
    private const int TagSize = 16;
    private const string TransitPurpose = "backup-manifest-transit";

    private static readonly WireLimits ManifestLimits = new(MaxStringBytes: 1 << 20, MaxBytesLength: int.MaxValue, MaxCollectionCount: 1 << 28);

    /// <summary>Distinct closure object ids (ascending).</summary>
    public IReadOnlyList<ObjectId> ClosureIds => Closure.Select(c => c.Id).ToArray();

    /// <summary>
    /// Builds the manifest of a snapshot root: the inodes of its namespace tree (attached to the root; detached inodes held only
    /// by open handles are not part of a snapshot image) and the distinct objects of every stream's extents.
    /// </summary>
    public static BackupManifest FromSnapshot(Guid clusterId, string clusterName, VolumeId volumeId, string volumeName, ulong epoch, Guid lineage,
        SnapshotRecord snapshot, CommitToken boundary, BackupVolumeConfig config)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        IReadOnlyList<InodeRecord> tree = Tree(snapshot.Root);
        var closure = new SortedDictionary<ObjectId, BackupObjectRef>(ObjectIdOrder.Instance);
        foreach (InodeRecord inode in tree)
        {
            foreach (StreamRecord stream in inode.Streams.Values)
            {
                if (stream.IsInline)
                    continue;
                foreach (ExtentRef e in stream.Extents)
                    closure[e.Object] = new BackupObjectRef(e.Object, e.ObjectLength, e.Checksum, e.Encoding);
            }
        }

        AdapterRecordData[] adapters = snapshot.Adapters.Select(a => new AdapterRecordData(a.Id, a.Version, a.RestoreRequirements)).ToArray();
        return new BackupManifest(clusterId, clusterName, volumeId, volumeName, epoch, lineage, snapshot.Id, snapshot.Name, boundary, snapshot.CreatedUtc,
            snapshot.Label, adapters, snapshot.Retention, config, tree, [.. closure.Values]);
    }

    /// <summary>The inodes reachable from the root of <paramref name="root"/> (ascending id).</summary>
    public static IReadOnlyList<InodeRecord> Tree(ImmutableSortedDictionary<ulong, InodeRecord> root)
    {
        var result = new SortedDictionary<ulong, InodeRecord>();
        var pending = new Stack<ulong>();
        pending.Push(InodeRecord.RootId);
        while (pending.Count > 0)
        {
            ulong id = pending.Pop();
            if (result.ContainsKey(id) || !root.TryGetValue(id, out InodeRecord? inode))
                continue;
            result[id] = inode;
            foreach (ulong child in inode.Children.Values)
                pending.Push(child);
        }

        return [.. result.Values];
    }

    /// <summary>The namespace as an inode map (the snapshot root).</summary>
    public ImmutableSortedDictionary<ulong, InodeRecord> Root()
    {
        var builder = ImmutableSortedDictionary.CreateBuilder<ulong, InodeRecord>();
        foreach (InodeRecord inode in Inodes)
            builder[inode.Id] = inode;
        return builder.ToImmutable();
    }

    /// <summary>
    /// Structural validation of a received manifest (BAK-010): a root directory, unique inodes, every directory entry naming an
    /// inode of the manifest whose parent and name agree, extents inside their objects and streams, and a closure equal to the
    /// objects the extents reference with consistent lengths and checksums. Returns null when valid, else the problem.
    /// </summary>
    public string? GetValidationError()
    {
        var inodes = new Dictionary<ulong, InodeRecord>(Inodes.Count);
        foreach (InodeRecord inode in Inodes)
        {
            if (!inodes.TryAdd(inode.Id, inode))
                return $"inode {inode.Id} appears twice";
        }

        if (!inodes.TryGetValue(InodeRecord.RootId, out InodeRecord? root) || !root.IsDirectory)
            return "the root directory is missing";
        var referenced = new Dictionary<ObjectId, BackupObjectRef>();
        var reachable = new HashSet<ulong> { InodeRecord.RootId };
        var pending = new Stack<ulong>();
        pending.Push(InodeRecord.RootId);
        while (pending.Count > 0)
        {
            InodeRecord node = inodes[pending.Pop()];
            foreach (KeyValuePair<string, ulong> child in node.Children)
            {
                if (!inodes.TryGetValue(child.Value, out InodeRecord? c))
                    return $"directory {node.Id} lists '{child.Key}' as inode {child.Value}, which the manifest does not contain";
                if (c.ParentId != node.Id || !string.Equals(c.Name, child.Key, StringComparison.Ordinal))
                    return $"inode {c.Id} does not name directory {node.Id} entry '{child.Key}' as its location";
                if (!reachable.Add(c.Id))
                    return $"inode {c.Id} is linked twice";
                if (c.IsDirectory)
                    pending.Push(c.Id);
            }
        }

        if (reachable.Count != inodes.Count)
            return $"{inodes.Count - reachable.Count} inode(s) are not reachable from the root";
        foreach (InodeRecord inode in inodes.Values)
        {
            if (!inode.IsDirectory && !inode.Streams.ContainsKey(DfsStreamPath.DefaultStreamName))
                return $"file inode {inode.Id} has no default stream";
            foreach (KeyValuePair<string, StreamRecord> pair in inode.Streams)
            {
                StreamRecord s = pair.Value;
                if (s.Length < 0)
                    return $"stream '{pair.Key}' of inode {inode.Id} has a negative length";
                if (s.IsInline)
                {
                    if (s.Inline.Length != s.Length || !s.Extents.IsEmpty)
                        return $"inline stream '{pair.Key}' of inode {inode.Id} is inconsistent";
                    continue;
                }

                long cursor = 0;
                foreach (ExtentRef e in s.Extents)
                {
                    if (e.Length <= 0 || e.Offset < cursor || e.End > s.Length || e.ObjectOffset < 0 || e.ObjectOffset + e.Length > e.ObjectLength)
                        return $"an extent of stream '{pair.Key}' of inode {inode.Id} is out of range";
                    cursor = e.End;
                    var reference = new BackupObjectRef(e.Object, e.ObjectLength, e.Checksum, e.Encoding);
                    if (referenced.TryGetValue(e.Object, out BackupObjectRef seen) && seen != reference)
                        return $"object {e.Object} is described inconsistently by two extents";
                    referenced[e.Object] = reference;
                }
            }
        }

        if (referenced.Count != Closure.Count)
            return $"the closure lists {Closure.Count} object(s) but the extents reference {referenced.Count}";
        ObjectId previous = default;
        for (int i = 0; i < Closure.Count; i++)
        {
            BackupObjectRef entry = Closure[i];
            if (i > 0 && entry.Id.Value <= previous.Value)
                return "the closure is not sorted";
            previous = entry.Id;
            if (!referenced.TryGetValue(entry.Id, out BackupObjectRef expected) || expected != entry)
                return $"closure object {entry.Id} does not match the extents referencing it";
        }

        if (Boundary.IsNone || Boundary.VolumeId != VolumeId || Boundary.VolumeEpoch != Epoch || Boundary.ClusterId != SourceClusterId)
            return "the boundary does not belong to the manifest's volume";
        return null;
    }

    // ------------------------------------------------------------------------------------------------- plaintext codec

    /// <summary>Canonical plaintext encoding.</summary>
    public byte[] Encode()
    {
        var buffer = new ArrayBufferWriter<byte>(4096);
        var w = new WireWriter(buffer);
        w.WriteUInt32(PlainMagic);
        w.WriteUInt16(FormatVersion);
        w.WriteGuid(SourceClusterId);
        w.WriteString(SourceClusterName);
        w.WriteVolumeId(VolumeId);
        w.WriteString(VolumeName);
        w.WriteVarUInt(Epoch);
        w.WriteGuid(Lineage);
        w.WriteSnapshotId(SnapshotId);
        w.WriteString(SnapshotName);
        w.WriteCommitToken(Boundary);
        w.WriteDateTimeOffset(CreatedUtc);
        w.WriteDefinedEnum(Label);
        w.WriteCount(Adapters.Count);
        foreach (AdapterRecordData a in Adapters)
            w.WriteRecord(a);
        w.WriteRetention(RequestedRetention);
        w.WriteMetadataPolicy(Config.Metadata);
        w.WriteStoragePolicy(Config.DefaultPolicy);
        w.WritePolicyMap(Config.NamedPolicies);
        w.WriteVarInt32(Config.InlineThreshold);
        w.WriteVolumeLimits(Config.Limits);
        w.WriteString(Config.StoragePool);
        w.WriteBool(Config.EnforceTraverseChecking);
        w.WriteCount(Inodes.Count);
        foreach (InodeRecord inode in Inodes)
            StateCodec.WriteInode(ref w, inode);
        w.WriteCount(Closure.Count);
        foreach (BackupObjectRef o in Closure)
        {
            w.WriteObjectId(o.Id);
            w.WriteVarInt(o.Length);
            w.WriteUInt128(o.Checksum);
            w.WriteByte(o.Encoding);
        }

        return buffer.WrittenSpan.ToArray();
    }

    /// <summary>Decodes a plaintext manifest (strict).</summary>
    /// <exception cref="DfsProtocolException">Malformed bytes or an unsupported (newer) format.</exception>
    public static BackupManifest Decode(ReadOnlySpan<byte> data)
    {
        var r = new WireReader(data, ManifestLimits);
        if (r.ReadUInt32() != PlainMagic)
            throw WireReader.Malformed("The data is not a backup manifest.");
        ushort version = r.ReadUInt16();
        if (version != FormatVersion)
            throw new DfsProtocolException(DfsErrorCode.Unsupported, $"Backup manifest format {version} is not supported (this build reads {FormatVersion}).");
        Guid cluster = r.ReadGuid();
        string clusterName = r.ReadString();
        VolumeId volume = r.ReadVolumeId();
        string volumeName = r.ReadString();
        ulong epoch = r.ReadVarUInt();
        Guid lineage = r.ReadGuid();
        SnapshotId snapshot = r.ReadSnapshotId();
        string snapshotName = r.ReadString();
        CommitToken boundary = r.ReadCommitToken();
        DateTimeOffset created = r.ReadDateTimeOffset();
        DfsConsistencyLabel label = r.ReadEnum<DfsConsistencyLabel>();
        int adapterCount = r.ReadCount(AdapterRecordData.MinEncodedSize);
        var adapters = new AdapterRecordData[adapterCount];
        for (int i = 0; i < adapterCount; i++)
            adapters[i] = r.ReadRecord<AdapterRecordData>();
        DfsRetention retention = r.ReadRetention();
        DfsMetadataPolicy metadata = r.ReadMetadataPolicy();
        DfsStoragePolicy defaultPolicy = r.ReadStoragePolicy();
        IReadOnlyDictionary<string, DfsStoragePolicy> named = r.ReadPolicyMap();
        int inline = r.ReadVarInt32();
        DfsVolumeLimits limits = r.ReadVolumeLimits();
        string pool = r.ReadString();
        bool traverse = r.ReadBool();
        int inodeCount = r.ReadCount(16);
        var inodes = new InodeRecord[inodeCount];
        for (int i = 0; i < inodeCount; i++)
            inodes[i] = StateCodec.ReadInode(ref r);
        int closureCount = r.ReadCount(36);
        var closure = new BackupObjectRef[closureCount];
        for (int i = 0; i < closureCount; i++)
            closure[i] = new BackupObjectRef(r.ReadObjectId(), r.ReadVarInt(), r.ReadUInt128(), r.ReadByte());
        r.EnsureEnd();
        return new BackupManifest(cluster, clusterName, volume, volumeName, epoch, lineage, snapshot, snapshotName, boundary, created, label, adapters, retention,
            new BackupVolumeConfig(metadata, defaultPolicy, named, inline, limits, pool, traverse), inodes, closure);
    }

    // ------------------------------------------------------------------------------------------------- transit sealing

    /// <summary>
    /// The transit key of a transfer: HKDF over the export principal's secret, salted with the transfer identity, so a sealed
    /// manifest only opens for the backup node that holds the same export credentials and only for this transfer.
    /// </summary>
    public static byte[] TransitKey(string exportSecret, BackupTransferKey key)
    {
        ArgumentException.ThrowIfNullOrEmpty(exportSecret);
        ArgumentNullException.ThrowIfNull(key);
        return KeyDerivation.DeriveKey(Encoding.UTF8.GetBytes(exportSecret), TransitPurpose, 32, KeyBytes(key));
    }

    /// <summary>Seals a plaintext manifest with AES-256-GCM (header: magic, format; nonce; ciphertext; tag), bound to the transfer.</summary>
    public static byte[] Seal(ReadOnlySpan<byte> plaintext, ReadOnlySpan<byte> transitKey, BackupTransferKey key, IRandomSource random)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(random);
        var sealedBytes = new byte[SealedHeaderSize + NonceSize + plaintext.Length + TagSize];
        Span<byte> s = sealedBytes;
        BinaryPrimitives.WriteUInt32LittleEndian(s, SealedMagic);
        BinaryPrimitives.WriteUInt16LittleEndian(s[4..], SealedFormatVersion);
        BinaryPrimitives.WriteUInt16LittleEndian(s[6..], 0);
        Span<byte> nonce = s.Slice(SealedHeaderSize, NonceSize);
        random.Fill(nonce);
        byte[] aad = Aad(s[..SealedHeaderSize], key);
        using var aes = new AesGcm(transitKey, TagSize);
        aes.Encrypt(nonce, plaintext, s.Slice(SealedHeaderSize + NonceSize, plaintext.Length), s[^TagSize..], aad);
        return sealedBytes;
    }

    /// <summary>Opens a sealed manifest; an altered or foreign manifest fails with <see cref="DfsIntegrityException"/>.</summary>
    public static byte[] Open(ReadOnlySpan<byte> sealedBytes, ReadOnlySpan<byte> transitKey, BackupTransferKey key)
    {
        ArgumentNullException.ThrowIfNull(key);
        if (sealedBytes.Length < SealedHeaderSize + NonceSize + TagSize || BinaryPrimitives.ReadUInt32LittleEndian(sealedBytes) != SealedMagic)
            throw new DfsIntegrityException("The received manifest is not a sealed backup manifest.");
        ushort version = BinaryPrimitives.ReadUInt16LittleEndian(sealedBytes[4..]);
        if (version != SealedFormatVersion || BinaryPrimitives.ReadUInt16LittleEndian(sealedBytes[6..]) != 0)
            throw new DfsProtocolException(DfsErrorCode.Unsupported, $"Sealed manifest format {version} is not supported.");
        int length = sealedBytes.Length - SealedHeaderSize - NonceSize - TagSize;
        var plaintext = new byte[length];
        byte[] aad = Aad(sealedBytes[..SealedHeaderSize], key);
        try
        {
            using var aes = new AesGcm(transitKey, TagSize);
            aes.Decrypt(sealedBytes.Slice(SealedHeaderSize, NonceSize), sealedBytes.Slice(SealedHeaderSize + NonceSize, length), sealedBytes[^TagSize..],
                plaintext, aad);
        }
        catch (AuthenticationTagMismatchException ex)
        {
            throw new DfsIntegrityException("The received manifest failed authentication (altered in transit or sealed for another transfer).", ex);
        }

        return plaintext;
    }

    /// <summary>XxHash128 of a manifest's bytes (as transferred).</summary>
    public static UInt128 Checksum(ReadOnlySpan<byte> bytes) => ObjectChecksum.Compute(bytes);

    /// <summary>The canonical bytes of a transfer key (salt and associated data).</summary>
    public static byte[] KeyBytes(BackupTransferKey key)
    {
        var buffer = new ArrayBufferWriter<byte>(80);
        var w = new WireWriter(buffer);
        w.WriteGuid(key.SourceClusterId);
        w.WriteVolumeId(key.Volume);
        w.WriteVarUInt(key.Epoch);
        w.WriteGuid(key.LineageId);
        w.WriteSnapshotId(key.Snapshot);
        return buffer.WrittenSpan.ToArray();
    }

    private static byte[] Aad(ReadOnlySpan<byte> header, BackupTransferKey key) => [.. header, .. KeyBytes(key)];

    /// <summary>Ascending object id order.</summary>
    internal sealed class ObjectIdOrder : IComparer<ObjectId>
    {
        public static readonly ObjectIdOrder Instance = new();

        public int Compare(ObjectId x, ObjectId y) => x.Value.CompareTo(y.Value);
    }
}
