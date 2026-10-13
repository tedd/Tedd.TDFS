namespace Tedd.Tdfs.Protocol.Messages;

/// <summary>Control-plane step of a restore (<see cref="RestoreVolumeMessage"/>).</summary>
public enum WireRestoreAction : byte
{
    /// <summary>Create the new volume in the <see cref="WireVolumeState.Restoring"/> state (not served).</summary>
    Create = 0,

    /// <summary>Activate the restored volume (<see cref="WireVolumeState.Restoring"/> → <see cref="WireVolumeState.Active"/>).</summary>
    Activate = 1,

    /// <summary>Remove a volume that is still <see cref="WireVolumeState.Restoring"/> (a restore that cannot complete).</summary>
    Abandon = 2,
}

/// <summary>Install step of a restore on the new volume's leader (<see cref="RestoreInstallMessage"/>).</summary>
public enum WireRestorePhase : byte
{
    /// <summary>Open (or find) the restore: returns the staging session the copied objects are stored under.</summary>
    Begin = 0,

    /// <summary>Stage a chunk of inode records (not visible until <see cref="Complete"/>).</summary>
    Stage = 1,

    /// <summary>Install every staged inode atomically as the volume's namespace.</summary>
    Complete = 2,
}

/// <summary>
/// Control-plane step of a restore (restore coordinator → control leader; node peers only, PROTOCOL.md §8.5). The coordinator
/// authorized the requesting principal (VolumeAdmin of the source and ClusterAdmin) before sending; the control leader
/// re-checks that the principal is a cluster administrator. <see cref="WireRestoreAction.Create"/> creates the new volume
/// in the <see cref="WireVolumeState.Restoring"/> state with the source volume's metadata policy, current default and named
/// storage policies, inline threshold, limits and traverse setting, bound to <see cref="StoragePool"/> (voters chosen
/// from that pool); <see cref="WireRestoreAction.Activate"/> makes it <see cref="WireVolumeState.Active"/>;
/// <see cref="WireRestoreAction.Abandon"/> removes it while it is still restoring. Every action is deduplicated by
/// <see cref="RequestId"/> (a retried create returns the volume created by the first attempt). Errors: AlreadyExists,
/// InvalidArgument (volume name), PoolNotFound, InvalidPolicy, PlacementUnavailable (no voters), VolumeNotFound,
/// AccessDenied, NotLeader.
/// </summary>
/// <param name="RequestId">Deduplication id of the action (derived by the coordinator from the source volume, the restore's operation id and the action).</param>
/// <param name="Action">Step to perform.</param>
/// <param name="SourceVolume">Volume whose snapshot is restored.</param>
/// <param name="Volume">The restoring volume (<see cref="WireRestoreAction.Activate"/>, <see cref="WireRestoreAction.Abandon"/>); empty for <see cref="WireRestoreAction.Create"/>.</param>
/// <param name="NewVolumeName">Name of the new volume.</param>
/// <param name="StoragePool">Storage pool of the new volume; null = the source volume's pool.</param>
/// <param name="Principal">Principal that requested the restore (becomes the creator of the new volume).</param>
public sealed record RestoreVolumeMessage(
    UInt128 RequestId,
    WireRestoreAction Action,
    VolumeId SourceVolume,
    VolumeId Volume,
    string NewVolumeName,
    string? StoragePool,
    string Principal) : IWireMessage<RestoreVolumeMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.RestoreVolume;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteUInt128(RequestId);
        writer.WriteEnum(Action);
        writer.WriteVolumeId(SourceVolume);
        writer.WriteVolumeId(Volume);
        writer.WriteString(NewVolumeName);
        writer.WriteOptionalString(StoragePool);
        writer.WriteString(Principal);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static RestoreVolumeMessage Read(ref WireReader reader)
    {
        UInt128 requestId = reader.ReadUInt128();
        WireRestoreAction action = reader.ReadEnum<WireRestoreAction>();
        VolumeId source = reader.ReadVolumeId();
        VolumeId volume = reader.ReadVolumeId();
        string name = reader.ReadString();
        string? pool = reader.ReadOptionalString();
        string principal = reader.ReadString();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new RestoreVolumeMessage(requestId, action, source, volume, name, pool, principal) { Extensions = extensions };
    }
}

/// <summary>Reply to <see cref="RestoreVolumeMessage"/>.</summary>
/// <param name="Volume">The restoring (after Create) or activated (after Activate) volume; null after Abandon.</param>
public sealed record RestoreVolumeResultMessage(VolumeEntry? Volume) : IWireMessage<RestoreVolumeResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.RestoreVolumeResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteOptional(Volume);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static RestoreVolumeResultMessage Read(ref WireReader reader)
    {
        VolumeEntry? volume = reader.ReadOptional<VolumeEntry>();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new RestoreVolumeResultMessage(volume) { Extensions = extensions };
    }
}

/// <summary>
/// Installs a restored namespace into a <see cref="WireVolumeState.Restoring"/> volume (restore coordinator → the new
/// volume's leader; node peers only, PROTOCOL.md §8.5). <see cref="WireRestorePhase.Begin"/> opens the restore bound to
/// <see cref="Source"/> (idempotent) and returns the staging session copies are stored under (it stays open, protecting
/// the copies from GC, until the install completes); <see cref="WireRestorePhase.Stage"/> stages a chunk of inode records
/// (committed volume entries tagged <c>RestoreSnapshot</c>, invisible until complete); <see cref="WireRestorePhase.Complete"/>
/// installs the staged inodes atomically in one entry. Every phase is idempotent for the same <see cref="Source"/>.
/// Errors: VolumeNotFound, InvalidArgument (not a restoring volume, another source, a malformed record), Integrity (staged
/// count mismatch), NotLeader.
/// </summary>
/// <param name="Volume">The restoring volume.</param>
/// <param name="Source">Opaque restore source tag binding all phases of one restore.</param>
/// <param name="Phase">Install step.</param>
/// <param name="Staging">The restore staging session (Stage, Complete); empty for Begin.</param>
/// <param name="Inodes">Stage: encoded inode records (server volume-state codec, format-versioned); empty otherwise.</param>
/// <param name="InodeCount">Stage: records in <see cref="Inodes"/>; Complete: total number of inodes the namespace must contain.</param>
public sealed record RestoreInstallMessage(
    VolumeId Volume,
    string Source,
    WireRestorePhase Phase,
    StagingId Staging,
    ReadOnlyMemory<byte> Inodes,
    int InodeCount) : IWireMessage<RestoreInstallMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.RestoreInstall;

    /// <inheritdoc />
    public override string ToString()
        => $"RestoreInstallMessage {{ Volume = {Volume}, Source = {Source}, Phase = {Phase}, Staging = {Staging}, Inodes = <{Inodes.Length} bytes>, InodeCount = {InodeCount} }}";

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteVolumeId(Volume);
        writer.WriteString(Source);
        writer.WriteEnum(Phase);
        writer.WriteStagingId(Staging);
        writer.WriteBytes(Inodes.Span);
        writer.WriteVarInt(InodeCount);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static RestoreInstallMessage Read(ref WireReader reader)
    {
        VolumeId volume = reader.ReadVolumeId();
        string source = reader.ReadString();
        WireRestorePhase phase = reader.ReadEnum<WireRestorePhase>();
        StagingId staging = reader.ReadStagingId();
        ReadOnlyMemory<byte> inodes = reader.ReadBytesCopy();
        int count = reader.ReadVarInt32();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new RestoreInstallMessage(volume, source, phase, staging, inodes, count) { Extensions = extensions };
    }
}

/// <summary>Reply to <see cref="RestoreInstallMessage"/>.</summary>
/// <param name="Staging">The restore staging session.</param>
/// <param name="StagedCount">Inodes staged so far (after Complete: inodes installed).</param>
/// <param name="Token">Complete: commit of the installed namespace; otherwise the commit of the phase's entry.</param>
public sealed record RestoreInstallResultMessage(
    StagingId Staging,
    int StagedCount,
    CommitToken Token) : IWireMessage<RestoreInstallResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.RestoreInstallResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteStagingId(Staging);
        writer.WriteVarInt(StagedCount);
        writer.WriteCommitToken(Token);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static RestoreInstallResultMessage Read(ref WireReader reader)
    {
        StagingId staging = reader.ReadStagingId();
        int count = reader.ReadVarInt32();
        CommitToken token = reader.ReadCommitToken();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new RestoreInstallResultMessage(staging, count, token) { Extensions = extensions };
    }
}
