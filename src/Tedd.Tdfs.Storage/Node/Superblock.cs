using System.Buffers;
using Tedd.Tdfs.Protocol;

namespace Tedd.Tdfs.Storage;

/// <summary>
/// Durable node identity stored in an <see cref="AtomicFile"/>: cluster, node id and name, start incarnation and the
/// format/software version gates.
/// </summary>
/// <remarks>
/// <para>Payload: <c>u32 magic "TDSB" | varuint formatVersion | varuint minSoftwareVersion | guid clusterId |
/// guid nodeId | string nodeName | u64 incarnation | extension block</c>. Unknown required extensions, a newer format
/// version, or a minimum software version above <see cref="SoftwareVersion"/> are refused with
/// <see cref="DfsProtocolException"/> (<see cref="DfsErrorCode.Unsupported"/>), so a downgraded binary never
/// misinterprets newer state (FMT-006).</para>
/// <para><see cref="BumpIncarnationAsync"/> must be called (and complete) on every start before the node serves, so
/// receipts and Raft messages of earlier incarnations can be fenced.</para>
/// </remarks>
/// <param name="FormatVersion">Superblock format version.</param>
/// <param name="ClusterId">Cluster this node belongs to.</param>
/// <param name="NodeId">Node identity.</param>
/// <param name="NodeName">Unique node name.</param>
/// <param name="Incarnation">Start counter (0 until the first start).</param>
/// <param name="MinSoftwareVersion">Lowest software version allowed to open this node's storage.</param>
public sealed record Superblock(int FormatVersion, Guid ClusterId, NodeId NodeId, string NodeName, ulong Incarnation, int MinSoftwareVersion)
{
    /// <summary>Superblock format version written by this software.</summary>
    public const int CurrentFormatVersion = 1;

    /// <summary>
    /// Storage software version of this build: the highest <see cref="MinSoftwareVersion"/> marker it can open. It is one
    /// above <see cref="BaseSoftwareVersion"/> because this build implements an irreversible format feature that raises the
    /// marker (FMT-006); a release lacking that feature (version <see cref="BaseSoftwareVersion"/>) is refused such images.
    /// </summary>
    public const int SoftwareVersion = 2;

    /// <summary>
    /// The <see cref="MinSoftwareVersion"/> a new superblock carries: every release of the current format can open it until an
    /// irreversible format feature raises the marker (<see cref="RaiseMinSoftwareVersionAsync"/>).
    /// </summary>
    public const int BaseSoftwareVersion = 1;

    /// <summary>Conventional file name of the superblock inside a node data directory.</summary>
    public const string DefaultFileName = "superblock";

    /// <summary>Loads the superblock at <paramref name="path"/>; null when absent.</summary>
    /// <param name="medium">Storage medium.</param>
    /// <param name="path">Superblock path.</param>
    /// <param name="ct">Cancellation.</param>
    /// <param name="softwareVersion">Software version of the reader (default: this build, <see cref="SoftwareVersion"/>).</param>
    /// <exception cref="DfsIntegrityException">The file is torn or corrupt.</exception>
    /// <exception cref="DfsProtocolException">The superblock requires a newer format or software version.</exception>
    public static async ValueTask<Superblock?> LoadAsync(IStorageMedium medium, string path, CancellationToken ct = default, int softwareVersion = SoftwareVersion)
    {
        byte[]? content = await AtomicFile.ReadAsync(medium, path, ct);
        return content is null ? null : Decode(content, path, softwareVersion);
    }

    /// <summary>Durably creates a new superblock (incarnation 0).</summary>
    /// <exception cref="IOException">A superblock already exists at <paramref name="path"/>.</exception>
    /// <exception cref="ArgumentException">An identifier is empty or the name is blank.</exception>
    public static ValueTask<Superblock> CreateAsync(IStorageMedium medium, string path, Guid clusterId, NodeId nodeId, string nodeName, CancellationToken ct = default)
        => CreateAsync(medium, path, clusterId, nodeId, nodeName, 0, ct);

    /// <summary>
    /// Durably creates a new superblock that continues an existing identity at <paramref name="incarnation"/>: a member
    /// whose disk was replaced by an empty one (OFF-020) keeps its node id, and its next start must be newer than every
    /// incarnation the cluster has registered for it.
    /// </summary>
    /// <exception cref="IOException">A superblock already exists at <paramref name="path"/>.</exception>
    /// <exception cref="ArgumentException">An identifier is empty or the name is blank.</exception>
    public static async ValueTask<Superblock> CreateAsync(IStorageMedium medium, string path, Guid clusterId, NodeId nodeId, string nodeName, ulong incarnation,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(medium);
        ArgumentException.ThrowIfNullOrWhiteSpace(nodeName);
        if (clusterId == Guid.Empty)
            throw new ArgumentException("The cluster id must not be empty.", nameof(clusterId));
        if (nodeId.IsEmpty)
            throw new ArgumentException("The node id must not be empty.", nameof(nodeId));
        if (await medium.ExistsAsync(path, ct))
            throw StorageErrors.FileExists(path);
        var superblock = new Superblock(CurrentFormatVersion, clusterId, nodeId, nodeName, incarnation, BaseSoftwareVersion);
        await AtomicFile.WriteAsync(medium, path, superblock.Encode(), ct);
        return superblock;
    }

    /// <summary>Durably increments the incarnation and returns the new superblock (durable before return).</summary>
    /// <exception cref="FileNotFoundException">No superblock exists at <paramref name="path"/>.</exception>
    public static async ValueTask<Superblock> BumpIncarnationAsync(IStorageMedium medium, string path, CancellationToken ct = default,
        int softwareVersion = SoftwareVersion)
    {
        Superblock current = await LoadAsync(medium, path, ct, softwareVersion)
            ?? throw new FileNotFoundException($"No superblock exists at '{path}'.", path);
        if (current.Incarnation == ulong.MaxValue)
            throw new InvalidOperationException("The node incarnation counter is exhausted.");
        Superblock next = current with { Incarnation = current.Incarnation + 1, FormatVersion = CurrentFormatVersion };
        await AtomicFile.WriteAsync(medium, path, next.Encode(), ct);
        return next;
    }

    /// <summary>
    /// Durably raises <see cref="MinSoftwareVersion"/> (irreversible format activation that blocks downgrades).
    /// Lowering is ignored.
    /// </summary>
    public static async ValueTask<Superblock> RaiseMinSoftwareVersionAsync(IStorageMedium medium, string path, int minSoftwareVersion, CancellationToken ct = default,
        int softwareVersion = SoftwareVersion)
    {
        if (minSoftwareVersion > softwareVersion)
            throw new ArgumentOutOfRangeException(nameof(minSoftwareVersion), $"This software (version {softwareVersion}) cannot require version {minSoftwareVersion}.");
        Superblock current = await LoadAsync(medium, path, ct, softwareVersion)
            ?? throw new FileNotFoundException($"No superblock exists at '{path}'.", path);
        if (minSoftwareVersion <= current.MinSoftwareVersion)
            return current;
        Superblock next = current with { MinSoftwareVersion = minSoftwareVersion };
        await AtomicFile.WriteAsync(medium, path, next.Encode(), ct);
        return next;
    }

    /// <summary>Serializes the superblock payload (framing and CRC are added by <see cref="AtomicFile"/>).</summary>
    public byte[] Encode()
    {
        var buffer = new ArrayBufferWriter<byte>(128);
        var writer = new WireWriter(buffer);
        writer.WriteUInt32(StorageFormat.SuperblockMagic);
        writer.WriteVarUInt((ulong)FormatVersion);
        writer.WriteVarUInt((ulong)MinSoftwareVersion);
        writer.WriteGuid(ClusterId);
        writer.WriteGuid(NodeId.Value);
        writer.WriteString(NodeName);
        writer.WriteUInt64(Incarnation);
        writer.WriteExtensions([]);
        return buffer.WrittenSpan.ToArray();
    }

    /// <summary>Parses a superblock payload.</summary>
    /// <param name="payload">Payload.</param>
    /// <param name="path">Path (diagnostics).</param>
    /// <param name="softwareVersion">Software version of the reader (default: this build, <see cref="SoftwareVersion"/>).</param>
    /// <exception cref="DfsIntegrityException">The payload is malformed.</exception>
    /// <exception cref="DfsProtocolException">The payload requires a newer format or software version.</exception>
    public static Superblock Decode(ReadOnlySpan<byte> payload, string path = "superblock", int softwareVersion = SoftwareVersion)
    {
        try
        {
            var reader = new WireReader(payload, WireLimits.Default);
            if (reader.ReadUInt32() != StorageFormat.SuperblockMagic)
                throw new DfsIntegrityException($"Superblock '{path}' has an invalid magic number.");
            ulong format = reader.ReadVarUInt();
            if (format == 0 || format > CurrentFormatVersion)
            {
                throw new DfsProtocolException(DfsErrorCode.Unsupported,
                    $"Superblock '{path}' uses format version {format}; this software supports version {CurrentFormatVersion}.");
            }

            ulong minSoftware = reader.ReadVarUInt();
            if (minSoftware > (ulong)Math.Max(0, softwareVersion))
            {
                throw new DfsProtocolException(DfsErrorCode.Unsupported,
                    $"Superblock '{path}' requires software version {minSoftware}; this software is version {softwareVersion}.");
            }

            Guid clusterId = reader.ReadGuid();
            var nodeId = new NodeId(reader.ReadGuid());
            string nodeName = reader.ReadString();
            ulong incarnation = reader.ReadUInt64();
            reader.ReadExtensions(new HashSet<uint>());
            reader.EnsureEnd();
            if (clusterId == Guid.Empty || nodeId.IsEmpty || string.IsNullOrWhiteSpace(nodeName))
                throw new DfsIntegrityException($"Superblock '{path}' has an empty identity field.");
            return new Superblock((int)format, clusterId, nodeId, nodeName, incarnation, (int)minSoftware);
        }
        catch (DfsProtocolException ex) when (ex.ErrorCode == DfsErrorCode.ProtocolError)
        {
            throw new DfsIntegrityException($"Superblock '{path}' is malformed: {ex.Message}", ex);
        }
    }
}
