namespace Tedd.Tdfs.Protocol.Messages;

/// <summary>
/// First message on every connection (client → server, node → node). Declares the peer's role, identity, protocol
/// range and features, and contributes a fresh nonce to the authentication transcript. Answered by
/// <see cref="HelloChallengeMessage"/> (or an error reply such as <see cref="DfsErrorCode.ClusterMismatch"/>).
/// </summary>
/// <param name="ExpectedClusterId">Cluster the peer expects; <see cref="Guid.Empty"/> accepts any cluster.</param>
/// <param name="ProtocolMin">Oldest protocol version the peer speaks.</param>
/// <param name="ProtocolMax">Newest protocol version the peer speaks.</param>
/// <param name="Features">Feature names the peer supports (ordinal set).</param>
/// <param name="Role">Declared role.</param>
/// <param name="Name">Client name or node name (informational for clients; must match the membership for nodes).</param>
/// <param name="NodeId">Node identity (nodes only).</param>
/// <param name="Incarnation">Node incarnation (nodes only; 0 for clients).</param>
/// <param name="Principal">Principal to authenticate (clients only).</param>
/// <param name="Nonce">Fresh random nonce of exactly <see cref="ProtocolInfo.NonceSize"/> bytes.</param>
/// <param name="BootId">Process boot id of a node peer (0 for clients): a random value chosen at every process start and
/// registered with the incarnation (<see cref="RegisterIncarnationMessage"/>). Two processes started from a cloned disk
/// share node id and incarnation but not the boot id, so the one that is not registered is told apart and fenced
/// (<see cref="DfsErrorCode.StaleIncarnation"/>, RSY-021).</param>
public sealed record HelloMessage(
    Guid ExpectedClusterId,
    ushort ProtocolMin,
    ushort ProtocolMax,
    IReadOnlySet<string> Features,
    PeerRole Role,
    string Name,
    NodeId? NodeId,
    ulong Incarnation,
    string? Principal,
    ReadOnlyMemory<byte> Nonce,
    UInt128 BootId = default) : IWireMessage<HelloMessage>
{
    /// <summary>
    /// Extension field id of <see cref="TransportCertificate"/> (optional; value: the length-prefixed 32-byte SPKI SHA-256 of the
    /// certificate the node's listener presents, then its validity as two timestamps). An extension because the message
    /// shipped without it (INTERNALS.md §4).
    /// </summary>
    public const uint TransportCertificateExtension = 1;

    /// <summary>Optional extensions preserved from the wire (re-emitted on encode), including <see cref="TransportCertificateExtension"/>.</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <summary>
    /// The transport certificate a node peer declares (its listener's SPKI pin and validity), from
    /// <see cref="TransportCertificateExtension"/>; null when absent or malformed. Bound into the handshake transcript like
    /// every other byte of the hello, so the node proof covers it: a node with a pinned transport refuses a node peer whose
    /// declared certificate is unknown to it or outside its validity (SEC-001), also on connections that peer opened (where
    /// TLS authenticates only the listening side).
    /// </summary>
    public (byte[] SpkiSha256, DateTimeOffset NotBefore, DateTimeOffset NotAfter)? TransportCertificate
    {
        get
        {
            foreach (WireExtension extension in Extensions)
            {
                if (extension.FieldId != TransportCertificateExtension)
                    continue;
                try
                {
                    var reader = new WireReader(extension.Value.Span, WireLimits.Default);
                    byte[] pin = reader.ReadFixedBytes(32, "SpkiSha256").ToArray();
                    DateTimeOffset notBefore = reader.ReadDateTimeOffset();
                    DateTimeOffset notAfter = reader.ReadDateTimeOffset();
                    return (pin, notBefore, notAfter);
                }
                catch (DfsProtocolException)
                {
                    return null;
                }
            }

            return null;
        }
    }

    /// <summary>A copy declaring a node's transport certificate (sets <see cref="TransportCertificateExtension"/>).</summary>
    public HelloMessage WithTransportCertificate(ReadOnlySpan<byte> spkiSha256, DateTimeOffset notBefore, DateTimeOffset notAfter)
    {
        if (spkiSha256.Length != 32)
            throw new ArgumentException("An SPKI pin is 32 bytes.", nameof(spkiSha256));
        var buffer = new System.Buffers.ArrayBufferWriter<byte>(64);
        var writer = new WireWriter(buffer);
        writer.WriteBytes(spkiSha256);
        writer.WriteDateTimeOffset(notBefore);
        writer.WriteDateTimeOffset(notAfter);
        WireExtension[] others = [.. Extensions.Where(e => e.FieldId != TransportCertificateExtension)];
        return this with { Extensions = [new WireExtension(TransportCertificateExtension, Required: false, buffer.WrittenMemory.ToArray()), .. others] };
    }

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.Hello;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        if (Nonce.Length != ProtocolInfo.NonceSize)
            throw new ArgumentException($"Nonce must be exactly {ProtocolInfo.NonceSize} bytes.", nameof(Nonce));
        writer.WriteGuid(ExpectedClusterId);
        writer.WriteVarUInt(ProtocolMin);
        writer.WriteVarUInt(ProtocolMax);
        writer.WriteStringSet(Features);
        writer.WriteDefinedEnum(Role);
        writer.WriteString(Name);
        writer.WriteOptionalNodeId(NodeId);
        writer.WriteVarUInt(Incarnation);
        writer.WriteOptionalString(Principal);
        writer.WriteBytes(Nonce.Span);
        writer.WriteUInt128(BootId);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static HelloMessage Read(ref WireReader reader)
    {
        Guid cluster = reader.ReadGuid();
        ushort min = reader.ReadVarUInt16();
        ushort max = reader.ReadVarUInt16();
        IReadOnlySet<string> features = reader.ReadStringSet();
        PeerRole role = reader.ReadEnum<PeerRole>();
        string name = reader.ReadString();
        NodeId? nodeId = reader.ReadOptionalNodeId();
        ulong incarnation = reader.ReadVarUInt();
        string? principal = reader.ReadOptionalString();
        ReadOnlyMemory<byte> nonce = reader.ReadFixedBytes(ProtocolInfo.NonceSize, nameof(Nonce));
        UInt128 bootId = reader.ReadUInt128();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new HelloMessage(cluster, min, max, features, role, name, nodeId, incarnation, principal, nonce, bootId) { Extensions = extensions };
    }
}

/// <summary>
/// Server's answer to <see cref="HelloMessage"/>: cluster identity, the selected protocol version, the server's
/// identity and nonce, and the parameters the peer needs to compute its proof. For client principals the salt and
/// iteration count of the principal's PBKDF2 verifier are returned; for an unknown principal the server returns a
/// deterministic fake salt so principal existence is not disclosed. For nodes the salt is empty and the iteration
/// count is 0 (the proof key is the HKDF-derived node authentication key).
/// </summary>
/// <param name="ClusterId">Cluster identity (for a backup node: the source cluster it serves).</param>
/// <param name="ClusterName">Cluster name.</param>
/// <param name="ProtocolVersion">Selected protocol version (the highest version in both ranges).</param>
/// <param name="Features">Features active cluster-wide that the peer also supports.</param>
/// <param name="ServerName">Name of the answering node.</param>
/// <param name="ServerRole">Role of the answering node (<see cref="PeerRole.Backup"/> marks a read-only backup host).</param>
/// <param name="ServerNodeId">Node identity of the answering node.</param>
/// <param name="ServerIncarnation">Current incarnation of the answering node.</param>
/// <param name="ServerNonce">Fresh random nonce of exactly <see cref="ProtocolInfo.NonceSize"/> bytes.</param>
/// <param name="PrincipalSalt">PBKDF2 salt of the principal's verifier (empty for node peers).</param>
/// <param name="Pbkdf2Iterations">PBKDF2-SHA256 iteration count of the verifier (0 for node peers).</param>
public sealed record HelloChallengeMessage(
    Guid ClusterId,
    string ClusterName,
    ushort ProtocolVersion,
    IReadOnlySet<string> Features,
    string ServerName,
    PeerRole ServerRole,
    NodeId? ServerNodeId,
    ulong ServerIncarnation,
    ReadOnlyMemory<byte> ServerNonce,
    ReadOnlyMemory<byte> PrincipalSalt,
    int Pbkdf2Iterations) : IWireMessage<HelloChallengeMessage>
{
    /// <summary>
    /// Extension field id of <see cref="ServerBootId"/> (optional; value: the boot id as 16 bytes, little-endian). An
    /// extension because the message shipped without it (INTERNALS.md §4): a peer that does not know it ignores it
    /// (FMT-003), and a node that receives a challenge without it checks the server by incarnation only.
    /// </summary>
    public const uint ServerBootIdExtension = 1;

    private static readonly System.Collections.Frozen.FrozenSet<uint> UnderstoodExtensions =
        System.Collections.Frozen.FrozenSet.ToFrozenSet([ServerBootIdExtension]);

    /// <summary>Extensions preserved from the wire (re-emitted on encode), including <see cref="ServerBootIdExtension"/>.</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <summary>
    /// Process boot id of a live node answering a node peer, from <see cref="ServerBootIdExtension"/> (0 when absent). Bound
    /// into the transcript like every other byte of the challenge: a node that opened the connection tells a process started
    /// from a cloned disk (registered incarnation, other boot id) apart from the registered one, exactly as the answering
    /// side does with <see cref="HelloMessage.BootId"/> (<see cref="DfsErrorCode.StaleIncarnation"/>, RSY-021), also for
    /// requests the answering node later sends back over this connection.
    /// </summary>
    public UInt128 ServerBootId
    {
        get
        {
            foreach (WireExtension extension in Extensions)
            {
                if (extension.FieldId == ServerBootIdExtension && extension.Value.Length == 16)
                    return System.Buffers.Binary.BinaryPrimitives.ReadUInt128LittleEndian(extension.Value.Span);
            }

            return UInt128.Zero;
        }
    }

    /// <summary>A copy that presents <paramref name="bootId"/> (sets <see cref="ServerBootIdExtension"/>; 0 removes it).</summary>
    public HelloChallengeMessage WithServerBootId(UInt128 bootId)
    {
        WireExtension[] others = [.. Extensions.Where(e => e.FieldId != ServerBootIdExtension)];
        if (bootId == UInt128.Zero)
            return this with { Extensions = others };
        byte[] value = new byte[16];
        System.Buffers.Binary.BinaryPrimitives.WriteUInt128LittleEndian(value, bootId);
        return this with { Extensions = [new WireExtension(ServerBootIdExtension, Required: false, value), .. others] };
    }

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.HelloChallenge;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        if (ServerNonce.Length != ProtocolInfo.NonceSize)
            throw new ArgumentException($"ServerNonce must be exactly {ProtocolInfo.NonceSize} bytes.", nameof(ServerNonce));
        writer.WriteGuid(ClusterId);
        writer.WriteString(ClusterName);
        writer.WriteVarUInt(ProtocolVersion);
        writer.WriteStringSet(Features);
        writer.WriteString(ServerName);
        writer.WriteDefinedEnum(ServerRole);
        writer.WriteOptionalNodeId(ServerNodeId);
        writer.WriteVarUInt(ServerIncarnation);
        writer.WriteBytes(ServerNonce.Span);
        writer.WriteBytes(PrincipalSalt.Span);
        writer.WriteVarInt32(Pbkdf2Iterations);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static HelloChallengeMessage Read(ref WireReader reader)
    {
        Guid cluster = reader.ReadGuid();
        string clusterName = reader.ReadString();
        ushort version = reader.ReadVarUInt16();
        IReadOnlySet<string> features = reader.ReadStringSet();
        string serverName = reader.ReadString();
        PeerRole role = reader.ReadEnum<PeerRole>();
        NodeId? nodeId = reader.ReadOptionalNodeId();
        ulong incarnation = reader.ReadVarUInt();
        ReadOnlyMemory<byte> nonce = reader.ReadFixedBytes(ProtocolInfo.NonceSize, nameof(ServerNonce));
        ReadOnlyMemory<byte> salt = reader.ReadBytesCopy();
        int iterations = reader.ReadVarInt32();
        List<WireExtension>? list = null;
        reader.ReadExtensions(UnderstoodExtensions, e =>
        {
            if (e.FieldId == ServerBootIdExtension && e.Value.Length != 16)
                throw WireReader.Malformed($"The server boot id extension must hold 16 bytes, not {e.Value.Length}.");
            (list ??= []).Add(e);
        });
        return new HelloChallengeMessage(cluster, clusterName, version, features, serverName, role, nodeId, incarnation, nonce, salt, iterations)
        {
            Extensions = list is null ? [] : list.ToArray(),
        };
    }
}

/// <summary>
/// Peer's proof of knowledge of its secret, computed over the handshake transcript (see <c>HandshakeAuth</c> and
/// PROTOCOL.md §3). Answered by <see cref="HelloResultMessage"/>.
/// </summary>
/// <param name="Proof">Proof of exactly <see cref="ProtocolInfo.MacSize"/> bytes.</param>
public sealed record HelloProofMessage(ReadOnlyMemory<byte> Proof) : IWireMessage<HelloProofMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.HelloProof;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        if (Proof.Length != ProtocolInfo.MacSize)
            throw new ArgumentException($"Proof must be exactly {ProtocolInfo.MacSize} bytes.", nameof(Proof));
        writer.WriteBytes(Proof.Span);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static HelloProofMessage Read(ref WireReader reader)
    {
        ReadOnlyMemory<byte> proof = reader.ReadFixedBytes(ProtocolInfo.MacSize, nameof(Proof));
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new HelloProofMessage(proof) { Extensions = extensions };
    }
}

/// <summary>
/// Final handshake message. On acceptance it carries the authenticated principal, its cluster-level permissions, the
/// server capabilities and the server's mutual-authentication MAC over the transcript; the peer must verify the MAC
/// before sending any other request. On rejection <see cref="Accepted"/> is false, <see cref="Error"/> explains why
/// (<see cref="DfsErrorCode.AuthenticationFailed"/>, <see cref="DfsErrorCode.StaleIncarnation"/>, ...) and the server
/// closes the connection.
/// </summary>
/// <param name="Accepted">True when the peer is authenticated.</param>
/// <param name="Error">Rejection reason (<see cref="DfsErrorCode.None"/> when accepted).</param>
/// <param name="Message">Human-readable detail (never secrets).</param>
/// <param name="Principal">Authenticated principal (node name for node peers).</param>
/// <param name="ClusterPermissions">Cluster-level permissions of the principal (<see cref="DfsPermissions.ClusterAdmin"/>).</param>
/// <param name="Capabilities">Server capabilities and limits (null when rejected).</param>
/// <param name="ServerMac">Server proof of exactly <see cref="ProtocolInfo.MacSize"/> bytes (empty when rejected).</param>
public sealed record HelloResultMessage(
    bool Accepted,
    DfsErrorCode Error,
    string Message,
    string Principal,
    DfsPermissions ClusterPermissions,
    DfsServerCapabilities? Capabilities,
    ReadOnlyMemory<byte> ServerMac) : IWireMessage<HelloResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.HelloResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        if (ServerMac.Length != 0 && ServerMac.Length != ProtocolInfo.MacSize)
            throw new ArgumentException($"ServerMac must be empty or exactly {ProtocolInfo.MacSize} bytes.", nameof(ServerMac));
        writer.WriteBool(Accepted);
        writer.WriteEnum(Error);
        writer.WriteString(Message ?? string.Empty);
        writer.WriteString(Principal ?? string.Empty);
        writer.WriteEnum(ClusterPermissions);
        writer.WriteBool(Capabilities is not null);
        if (Capabilities is not null)
            writer.WriteServerCapabilities(Capabilities);
        writer.WriteBytes(ServerMac.Span);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static HelloResultMessage Read(ref WireReader reader)
    {
        bool accepted = reader.ReadBool();
        DfsErrorCode error = reader.ReadEnumRaw<DfsErrorCode>();
        string message = reader.ReadString();
        string principal = reader.ReadString();
        DfsPermissions permissions = reader.ReadEnumRaw<DfsPermissions>();
        DfsServerCapabilities? capabilities = reader.ReadBool() ? reader.ReadServerCapabilities() : null;
        ReadOnlyMemory<byte> mac = reader.ReadEmptyOrFixedBytes(ProtocolInfo.MacSize, nameof(ServerMac));
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new HelloResultMessage(accepted, error, message, principal, permissions, capabilities, mac) { Extensions = extensions };
    }
}
