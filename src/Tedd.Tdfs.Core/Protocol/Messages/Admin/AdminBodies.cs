namespace Tedd.Tdfs.Protocol.Messages;

/// <summary>
/// Empty administrative body.
/// </summary>
public sealed record AdminEmptyBody() : IWireRecord<AdminEmptyBody>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 1;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static AdminEmptyBody Read(ref WireReader reader)
    {
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new AdminEmptyBody() { Extensions = extensions };
    }
}

/// <summary>
/// Result of <see cref="AdminOp.ClusterStatus"/>.
/// </summary>
/// <param name="ClusterId">Cluster identity.</param>
/// <param name="ClusterName">Cluster name.</param>
/// <param name="Control">Control group status.</param>
/// <param name="Nodes">Members.</param>
/// <param name="Volumes">Volume names.</param>
/// <param name="PlacementEpoch">Current placement epoch.</param>
/// <param name="ActiveFeatures">Features active cluster-wide.</param>
public sealed record AdminClusterStatusBody(
    Guid ClusterId,
    string ClusterName,
    GroupStatusData Control,
    IReadOnlyList<NodeEntry> Nodes,
    IReadOnlyList<string> Volumes,
    ulong PlacementEpoch,
    IReadOnlySet<string> ActiveFeatures) : IWireRecord<AdminClusterStatusBody>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 31;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteGuid(ClusterId);
        writer.WriteString(ClusterName);
        writer.WriteRecord(Control);
        writer.WriteList(Nodes);
        writer.WriteStringList(Volumes);
        writer.WriteVarUInt(PlacementEpoch);
        writer.WriteStringSet(ActiveFeatures);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static AdminClusterStatusBody Read(ref WireReader reader)
    {
        Guid clusterId = reader.ReadGuid();
        string clusterName = reader.ReadString();
        GroupStatusData control = reader.ReadRecord<GroupStatusData>();
        IReadOnlyList<NodeEntry> nodes = reader.ReadList<NodeEntry>();
        IReadOnlyList<string> volumes = reader.ReadStringList();
        ulong placementEpoch = reader.ReadVarUInt();
        IReadOnlySet<string> activeFeatures = reader.ReadStringSet();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new AdminClusterStatusBody(clusterId, clusterName, control, nodes, volumes, placementEpoch, activeFeatures) { Extensions = extensions };
    }
}

/// <summary>
/// Result of <see cref="AdminOp.ListNodes"/>.
/// </summary>
/// <param name="Nodes">Members ordered by name.</param>
public sealed record AdminNodeListBody(
    IReadOnlyList<NodeEntry> Nodes) : IWireRecord<AdminNodeListBody>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 2;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteList(Nodes);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static AdminNodeListBody Read(ref WireReader reader)
    {
        IReadOnlyList<NodeEntry> nodes = reader.ReadList<NodeEntry>();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new AdminNodeListBody(nodes) { Extensions = extensions };
    }
}

/// <summary>
/// Request of <see cref="AdminOp.AddNode"/> (explicit admission; also re-admission of a removed identity under a new
/// lineage).
/// </summary>
/// <param name="Name">Node name.</param>
/// <param name="Labels">Failure-domain labels.</param>
/// <param name="Endpoints">Endpoints of the node.</param>
public sealed record AdminAddNodeBody(
    string Name,
    DfsTopologyLabels Labels,
    IReadOnlyList<string> Endpoints) : IWireRecord<AdminAddNodeBody>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 8;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteString(Name);
        writer.WriteTopologyLabels(Labels);
        writer.WriteStringList(Endpoints);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static AdminAddNodeBody Read(ref WireReader reader)
    {
        string name = reader.ReadString();
        DfsTopologyLabels labels = reader.ReadTopologyLabels();
        IReadOnlyList<string> endpoints = reader.ReadStringList();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new AdminAddNodeBody(name, labels, endpoints) { Extensions = extensions };
    }
}

/// <summary>
/// Request naming one node (<see cref="AdminOp.DrainNode"/>).
/// </summary>
/// <param name="Name">Node name.</param>
public sealed record AdminNodeNameBody(
    string Name) : IWireRecord<AdminNodeNameBody>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 2;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteString(Name);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static AdminNodeNameBody Read(ref WireReader reader)
    {
        string name = reader.ReadString();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new AdminNodeNameBody(name) { Extensions = extensions };
    }
}

/// <summary>
/// Request of <see cref="AdminOp.RemoveNode"/>.
/// </summary>
/// <param name="Name">Node name.</param>
/// <param name="Force">Remove even if the node still holds required data or votes (unsafe; audited).</param>
public sealed record AdminRemoveNodeBody(
    string Name,
    bool Force) : IWireRecord<AdminRemoveNodeBody>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 3;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteString(Name);
        writer.WriteBool(Force);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static AdminRemoveNodeBody Read(ref WireReader reader)
    {
        string name = reader.ReadString();
        bool force = reader.ReadBool();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new AdminRemoveNodeBody(name, force) { Extensions = extensions };
    }
}

/// <summary>
/// Request of <see cref="AdminOp.SetNodeLabels"/>.
/// </summary>
/// <param name="Name">Node name.</param>
/// <param name="Labels">New labels (bumps the placement epoch).</param>
public sealed record AdminSetNodeLabelsBody(
    string Name,
    DfsTopologyLabels Labels) : IWireRecord<AdminSetNodeLabelsBody>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 7;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteString(Name);
        writer.WriteTopologyLabels(Labels);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static AdminSetNodeLabelsBody Read(ref WireReader reader)
    {
        string name = reader.ReadString();
        DfsTopologyLabels labels = reader.ReadTopologyLabels();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new AdminSetNodeLabelsBody(name, labels) { Extensions = extensions };
    }
}

/// <summary>
/// Request of <see cref="AdminOp.CreateVolume"/> (mirrors the SDK's <c>DfsVolumeSpec</c>).
/// </summary>
/// <param name="Name">Volume name.</param>
/// <param name="Metadata">Metadata (voter) policy.</param>
/// <param name="DefaultPolicy">Default storage policy.</param>
/// <param name="NamedPolicies">Named storage policies.</param>
/// <param name="InlineThreshold">Inline threshold in bytes.</param>
/// <param name="Limits">Volume limits.</param>
public sealed record AdminVolumeSpecBody(
    string Name,
    DfsMetadataPolicy Metadata,
    DfsStoragePolicy DefaultPolicy,
    IReadOnlyDictionary<string, DfsStoragePolicy> NamedPolicies,
    int InlineThreshold,
    DfsVolumeLimits Limits) : IWireRecord<AdminVolumeSpecBody>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 19;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteString(Name);
        writer.WriteMetadataPolicy(Metadata);
        writer.WriteStoragePolicy(DefaultPolicy);
        writer.WritePolicyMap(NamedPolicies);
        writer.WriteVarInt32(InlineThreshold);
        writer.WriteVolumeLimits(Limits);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static AdminVolumeSpecBody Read(ref WireReader reader)
    {
        string name = reader.ReadString();
        DfsMetadataPolicy metadata = reader.ReadMetadataPolicy();
        DfsStoragePolicy defaultPolicy = reader.ReadStoragePolicy();
        IReadOnlyDictionary<string, DfsStoragePolicy> namedPolicies = reader.ReadPolicyMap();
        int inlineThreshold = reader.ReadVarInt32();
        DfsVolumeLimits limits = reader.ReadVolumeLimits();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new AdminVolumeSpecBody(name, metadata, defaultPolicy, namedPolicies, inlineThreshold, limits) { Extensions = extensions };
    }
}

/// <summary>
/// Volume description returned by <see cref="AdminOp.CreateVolume"/> and <see cref="AdminOp.RestoreFromBackup"/>.
/// </summary>
/// <param name="Volume">Volume entry.</param>
/// <param name="LastCommit">Last commit of the volume.</param>
public sealed record AdminVolumeInfoBody(
    VolumeEntry Volume,
    CommitToken LastCommit) : IWireRecord<AdminVolumeInfoBody>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 75;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Volume);
        writer.WriteCommitToken(LastCommit);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static AdminVolumeInfoBody Read(ref WireReader reader)
    {
        VolumeEntry volume = reader.ReadRecord<VolumeEntry>();
        CommitToken lastCommit = reader.ReadCommitToken();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new AdminVolumeInfoBody(volume, lastCommit) { Extensions = extensions };
    }
}

/// <summary>
/// Request naming one volume.
/// </summary>
/// <param name="Volume">Volume name.</param>
public sealed record AdminVolumeNameBody(
    string Volume) : IWireRecord<AdminVolumeNameBody>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 2;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteString(Volume);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static AdminVolumeNameBody Read(ref WireReader reader)
    {
        string volume = reader.ReadString();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new AdminVolumeNameBody(volume) { Extensions = extensions };
    }
}

/// <summary>
/// A list of names or lines (<see cref="AdminOp.ListVolumes"/>, <see cref="AdminOp.AuditLog"/>).
/// </summary>
/// <param name="Names">Names or audit lines.</param>
public sealed record AdminNameListBody(
    IReadOnlyList<string> Names) : IWireRecord<AdminNameListBody>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 2;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteStringList(Names);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static AdminNameListBody Read(ref WireReader reader)
    {
        IReadOnlyList<string> names = reader.ReadStringList();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new AdminNameListBody(names) { Extensions = extensions };
    }
}

/// <summary>
/// Result of <see cref="AdminOp.VolumeStatus"/>.
/// </summary>
/// <param name="Volume">Volume entry.</param>
/// <param name="LastCommit">Last commit of the volume.</param>
/// <param name="Group">Volume group status.</param>
/// <param name="Replicas">Replica health.</param>
/// <param name="Backups">Backup target status.</param>
public sealed record AdminVolumeStatusBody(
    VolumeEntry Volume,
    CommitToken LastCommit,
    GroupStatusData Group,
    ReplicaHealthData Replicas,
    IReadOnlyList<BackupStatusData> Backups) : IWireRecord<AdminVolumeStatusBody>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 95;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteRecord(Volume);
        writer.WriteCommitToken(LastCommit);
        writer.WriteRecord(Group);
        writer.WriteRecord(Replicas);
        writer.WriteList(Backups);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static AdminVolumeStatusBody Read(ref WireReader reader)
    {
        VolumeEntry volume = reader.ReadRecord<VolumeEntry>();
        CommitToken lastCommit = reader.ReadCommitToken();
        GroupStatusData group = reader.ReadRecord<GroupStatusData>();
        ReplicaHealthData replicas = reader.ReadRecord<ReplicaHealthData>();
        IReadOnlyList<BackupStatusData> backups = reader.ReadList<BackupStatusData>();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new AdminVolumeStatusBody(volume, lastCommit, group, replicas, backups) { Extensions = extensions };
    }
}

/// <summary>
/// Request of <see cref="AdminOp.SetDefaultPolicy"/>.
/// </summary>
/// <param name="Volume">Volume name.</param>
/// <param name="Policy">New default policy (validated; impossible policies are rejected with InvalidPolicy).</param>
/// <param name="MigrateExisting">True to migrate existing objects (reported effective only after migration).</param>
public sealed record AdminSetDefaultPolicyBody(
    string Volume,
    DfsStoragePolicy Policy,
    bool MigrateExisting) : IWireRecord<AdminSetDefaultPolicyBody>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 7;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteString(Volume);
        writer.WriteStoragePolicy(Policy);
        writer.WriteBool(MigrateExisting);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static AdminSetDefaultPolicyBody Read(ref WireReader reader)
    {
        string volume = reader.ReadString();
        DfsStoragePolicy policy = reader.ReadStoragePolicy();
        bool migrateExisting = reader.ReadBool();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new AdminSetDefaultPolicyBody(volume, policy, migrateExisting) { Extensions = extensions };
    }
}

/// <summary>
/// Request of <see cref="AdminOp.DefinePolicy"/>.
/// </summary>
/// <param name="Volume">Volume name.</param>
/// <param name="PolicyName">Policy name.</param>
/// <param name="Policy">Policy definition.</param>
public sealed record AdminDefinePolicyBody(
    string Volume,
    string PolicyName,
    DfsStoragePolicy Policy) : IWireRecord<AdminDefinePolicyBody>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 7;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteString(Volume);
        writer.WriteString(PolicyName);
        writer.WriteStoragePolicy(Policy);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static AdminDefinePolicyBody Read(ref WireReader reader)
    {
        string volume = reader.ReadString();
        string policyName = reader.ReadString();
        DfsStoragePolicy policy = reader.ReadStoragePolicy();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new AdminDefinePolicyBody(volume, policyName, policy) { Extensions = extensions };
    }
}

/// <summary>
/// Result of <see cref="AdminOp.PolicyMigrationStatus"/>.
/// </summary>
/// <param name="Volume">Volume name.</param>
/// <param name="Target">Target default policy.</param>
/// <param name="Effective">True once migration has completed.</param>
/// <param name="PendingObjects">Objects still to migrate.</param>
public sealed record AdminPolicyMigrationBody(
    string Volume,
    DfsStoragePolicy Target,
    bool Effective,
    long PendingObjects) : IWireRecord<AdminPolicyMigrationBody>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 8;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteString(Volume);
        writer.WriteStoragePolicy(Target);
        writer.WriteBool(Effective);
        writer.WriteVarInt(PendingObjects);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static AdminPolicyMigrationBody Read(ref WireReader reader)
    {
        string volume = reader.ReadString();
        DfsStoragePolicy target = reader.ReadStoragePolicy();
        bool effective = reader.ReadBool();
        long pendingObjects = reader.ReadVarInt();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new AdminPolicyMigrationBody(volume, target, effective, pendingObjects) { Extensions = extensions };
    }
}

/// <summary>
/// Request of <see cref="AdminOp.CreatePrincipal"/>. The server stores only a salted PBKDF2 verifier.
/// </summary>
/// <param name="Principal">Principal name.</param>
/// <param name="Secret">Initial secret (never logged).</param>
/// <param name="ClusterAdmin">Grant cluster administration.</param>
public sealed record AdminCreatePrincipalBody(
    string Principal,
    string Secret,
    bool ClusterAdmin) : IWireRecord<AdminCreatePrincipalBody>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 4;

    /// <summary>Returns a description with the secret redacted (SEC-016).</summary>
    public override string ToString() => $"AdminCreatePrincipalBody {{ Principal = {Principal}, Secret = ***, ClusterAdmin = {ClusterAdmin} }}";

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteString(Principal);
        writer.WriteString(Secret);
        writer.WriteBool(ClusterAdmin);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static AdminCreatePrincipalBody Read(ref WireReader reader)
    {
        string principal = reader.ReadString();
        string secret = reader.ReadString();
        bool clusterAdmin = reader.ReadBool();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new AdminCreatePrincipalBody(principal, secret, clusterAdmin) { Extensions = extensions };
    }
}

/// <summary>
/// Request of <see cref="AdminOp.DeletePrincipal"/>.
/// </summary>
/// <param name="Principal">Principal name.</param>
public sealed record AdminPrincipalNameBody(
    string Principal) : IWireRecord<AdminPrincipalNameBody>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 2;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteString(Principal);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static AdminPrincipalNameBody Read(ref WireReader reader)
    {
        string principal = reader.ReadString();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new AdminPrincipalNameBody(principal) { Extensions = extensions };
    }
}

/// <summary>
/// Request of <see cref="AdminOp.Grant"/> and <see cref="AdminOp.Revoke"/>.
/// </summary>
/// <param name="Volume">Volume name.</param>
/// <param name="Principal">Principal name.</param>
/// <param name="Permissions">Permissions to grant or revoke (unknown bits are rejected by the server).</param>
public sealed record AdminGrantBody(
    string Volume,
    string Principal,
    DfsPermissions Permissions) : IWireRecord<AdminGrantBody>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 4;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteString(Volume);
        writer.WriteString(Principal);
        writer.WriteEnum(Permissions);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static AdminGrantBody Read(ref WireReader reader)
    {
        string volume = reader.ReadString();
        string principal = reader.ReadString();
        DfsPermissions permissions = reader.ReadEnumRaw<DfsPermissions>();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new AdminGrantBody(volume, principal, permissions) { Extensions = extensions };
    }
}

/// <summary>
/// Result of <see cref="AdminOp.RotateDataKey"/>.
/// </summary>
/// <param name="Volume">Volume name.</param>
/// <param name="KeyVersion">New data-key version.</param>
public sealed record AdminKeyRotationBody(
    string Volume,
    ulong KeyVersion) : IWireRecord<AdminKeyRotationBody>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 3;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteString(Volume);
        writer.WriteVarUInt(KeyVersion);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static AdminKeyRotationBody Read(ref WireReader reader)
    {
        string volume = reader.ReadString();
        ulong keyVersion = reader.ReadVarUInt();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new AdminKeyRotationBody(volume, keyVersion) { Extensions = extensions };
    }
}

/// <summary>
/// Request of <see cref="AdminOp.ExportRecoveryKey"/>.
/// </summary>
/// <param name="Passphrase">Passphrase the escrow is wrapped under (never logged).</param>
public sealed record AdminExportRecoveryKeyBody(
    string Passphrase) : IWireRecord<AdminExportRecoveryKeyBody>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 2;

    /// <summary>Returns a description with the passphrase redacted (SEC-016).</summary>
    public override string ToString() => "AdminExportRecoveryKeyBody { Passphrase = *** }";

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteString(Passphrase);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static AdminExportRecoveryKeyBody Read(ref WireReader reader)
    {
        string passphrase = reader.ReadString();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new AdminExportRecoveryKeyBody(passphrase) { Extensions = extensions };
    }
}

/// <summary>
/// Result of <see cref="AdminOp.ExportRecoveryKey"/>.
/// </summary>
/// <param name="Escrow">KEK escrow wrapped under the passphrase.</param>
public sealed record AdminRecoveryKeyBody(
    ReadOnlyMemory<byte> Escrow) : IWireRecord<AdminRecoveryKeyBody>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 2;

    /// <summary>Returns a description without key material (SEC-016).</summary>
    public override string ToString() => $"AdminRecoveryKeyBody {{ Escrow = <{Escrow.Length} bytes> }}";

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteBytes(Escrow.Span);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static AdminRecoveryKeyBody Read(ref WireReader reader)
    {
        ReadOnlyMemory<byte> escrow = reader.ReadBytesCopy();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new AdminRecoveryKeyBody(escrow) { Extensions = extensions };
    }
}

/// <summary>
/// Request of <see cref="AdminOp.StartScrub"/>.
/// </summary>
/// <param name="Node">Node to scrub; null = all nodes.</param>
public sealed record AdminStartScrubBody(
    string? Node) : IWireRecord<AdminStartScrubBody>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 2;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteOptionalString(Node);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static AdminStartScrubBody Read(ref WireReader reader)
    {
        string? node = reader.ReadOptionalString();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new AdminStartScrubBody(node) { Extensions = extensions };
    }
}

/// <summary>
/// Request of <see cref="AdminOp.RestoreFromBackup"/> (mirrors the SDK's <c>DfsRestoreFromBackupRequest</c>).
/// </summary>
/// <param name="BackupEndpoints">Endpoints of the backup node.</param>
/// <param name="BackupCredentials">Credentials for the backup node (reader of the snapshot).</param>
/// <param name="SourceVolume">Source volume name on the backup node.</param>
/// <param name="SnapshotName">Published snapshot to restore.</param>
/// <param name="NewVolumeName">Name of the new live volume.</param>
/// <param name="RecoveryKey">Recovery key material when the backup node needs it (empty = none).</param>
public sealed record AdminRestoreFromBackupBody(
    IReadOnlyList<string> BackupEndpoints,
    DfsCredentials BackupCredentials,
    string SourceVolume,
    string SnapshotName,
    string NewVolumeName,
    ReadOnlyMemory<byte> RecoveryKey) : IWireRecord<AdminRestoreFromBackupBody>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 8;

    /// <summary>Returns a description without credentials or key material (SEC-016).</summary>
    public override string ToString()
        => $"AdminRestoreFromBackupBody {{ SourceVolume = {SourceVolume}, SnapshotName = {SnapshotName}, NewVolumeName = {NewVolumeName}, BackupCredentials = {BackupCredentials}, RecoveryKey = <{RecoveryKey.Length} bytes> }}";

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteStringList(BackupEndpoints);
        writer.WriteCredentials(BackupCredentials);
        writer.WriteString(SourceVolume);
        writer.WriteString(SnapshotName);
        writer.WriteString(NewVolumeName);
        writer.WriteBytes(RecoveryKey.Span);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static AdminRestoreFromBackupBody Read(ref WireReader reader)
    {
        IReadOnlyList<string> backupEndpoints = reader.ReadStringList();
        DfsCredentials backupCredentials = reader.ReadCredentials();
        string sourceVolume = reader.ReadString();
        string snapshotName = reader.ReadString();
        string newVolumeName = reader.ReadString();
        ReadOnlyMemory<byte> recoveryKey = reader.ReadBytesCopy();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new AdminRestoreFromBackupBody(backupEndpoints, backupCredentials, sourceVolume, snapshotName, newVolumeName, recoveryKey) { Extensions = extensions };
    }
}

/// <summary>
/// Request of <see cref="AdminOp.SetFeature"/>.
/// </summary>
/// <param name="Feature">Feature name.</param>
/// <param name="Enabled">True to activate (only once every member supports it, FMT-009).</param>
public sealed record AdminSetFeatureBody(
    string Feature,
    bool Enabled) : IWireRecord<AdminSetFeatureBody>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 3;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteString(Feature);
        writer.WriteBool(Enabled);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static AdminSetFeatureBody Read(ref WireReader reader)
    {
        string feature = reader.ReadString();
        bool enabled = reader.ReadBool();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new AdminSetFeatureBody(feature, enabled) { Extensions = extensions };
    }
}

/// <summary>
/// Request of <see cref="AdminOp.AuditLog"/>.
/// </summary>
/// <param name="Max">Maximum number of most recent entries.</param>
public sealed record AdminAuditLogRequestBody(
    int Max) : IWireRecord<AdminAuditLogRequestBody>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public static int MinEncodedSize => 2;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteVarInt32(Max);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static AdminAuditLogRequestBody Read(ref WireReader reader)
    {
        int max = reader.ReadVarInt32();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new AdminAuditLogRequestBody(max) { Extensions = extensions };
    }
}
