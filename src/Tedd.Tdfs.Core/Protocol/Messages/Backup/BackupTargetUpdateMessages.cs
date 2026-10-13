namespace Tedd.Tdfs.Protocol.Messages;

/// <summary>
/// Applies a backup target registration change of a volume to the control state (volume leader serving
/// <see cref="BackupRegisterMessage"/> → control leader; node peers only, PROTOCOL.md §8.6). The volume leader authorized
/// the requesting principal (BackupAdmin on the volume) and wrapped the export secret with the cluster key-encryption key
/// before sending; the control leader re-checks the grant on its own state. <see cref="BackupTargetAction.Register"/> and
/// <see cref="BackupTargetAction.Configure"/> store the complete registration (Configure requires an existing target);
/// <see cref="BackupTargetAction.Remove"/> removes it. Deduplicated by <see cref="RequestId"/> through the control group's
/// administrative outcomes. Errors: VolumeNotFound, BackupTargetNotFound, InvalidArgument, AccessDenied, NotLeader.
/// </summary>
/// <param name="RequestId">Deduplication id (derived by the volume leader from the volume and the request's operation id).</param>
/// <param name="Volume">The volume.</param>
/// <param name="Action">Change to apply (<see cref="BackupTargetAction.List"/> is not allowed).</param>
/// <param name="Principal">Principal that requested the change.</param>
/// <param name="Target">Target name.</param>
/// <param name="Endpoints">Target endpoints (Register, Configure).</param>
/// <param name="ExportPrincipal">Principal the exporter authenticates as on the target (Register, Configure).</param>
/// <param name="WrappedExportSecret">The export principal's secret wrapped with the cluster key-encryption key (Register, Configure).</param>
/// <param name="Schedule">Schedule (Register, Configure).</param>
/// <param name="Budgets">Budgets (Register, Configure).</param>
public sealed record BackupTargetUpdateMessage(
    UInt128 RequestId,
    VolumeId Volume,
    BackupTargetAction Action,
    string Principal,
    string Target,
    IReadOnlyList<string> Endpoints,
    string ExportPrincipal,
    ReadOnlyMemory<byte> WrappedExportSecret,
    BackupScheduleData? Schedule,
    BackupBudgetsData? Budgets) : IWireMessage<BackupTargetUpdateMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.BackupTargetUpdate;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteUInt128(RequestId);
        writer.WriteVolumeId(Volume);
        writer.WriteDefinedEnum(Action);
        writer.WriteString(Principal);
        writer.WriteString(Target);
        writer.WriteStringList(Endpoints);
        writer.WriteString(ExportPrincipal);
        writer.WriteBytes(WrappedExportSecret.Span);
        writer.WriteOptional(Schedule);
        writer.WriteOptional(Budgets);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static BackupTargetUpdateMessage Read(ref WireReader reader)
    {
        UInt128 requestId = reader.ReadUInt128();
        VolumeId volume = reader.ReadVolumeId();
        BackupTargetAction action = reader.ReadEnum<BackupTargetAction>();
        string principal = reader.ReadString();
        string target = reader.ReadString();
        IReadOnlyList<string> endpoints = reader.ReadStringList();
        string exportPrincipal = reader.ReadString();
        byte[] wrapped = reader.ReadBytesToArray();
        BackupScheduleData? schedule = reader.ReadOptional<BackupScheduleData>();
        BackupBudgetsData? budgets = reader.ReadOptional<BackupBudgetsData>();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new BackupTargetUpdateMessage(requestId, volume, action, principal, target, endpoints, exportPrincipal, wrapped, schedule, budgets)
        {
            Extensions = extensions,
        };
    }
}

/// <summary>Reply to <see cref="BackupTargetUpdateMessage"/>.</summary>
/// <param name="Targets">Registered target names of the volume after the change.</param>
/// <param name="ControlIndex">Control-log index of the change (the volume leader waits until its own control replica applied it).</param>
public sealed record BackupTargetUpdateResultMessage(
    IReadOnlyList<string> Targets,
    ulong ControlIndex) : IWireMessage<BackupTargetUpdateResultMessage>
{
    /// <summary>Unknown optional extensions preserved from the wire (re-emitted on encode).</summary>
    public IReadOnlyList<WireExtension> Extensions { get; init; } = [];

    /// <inheritdoc />
    public MessageType Type => WireType;

    /// <inheritdoc />
    public static MessageType WireType => MessageType.BackupTargetUpdateResult;

    /// <inheritdoc />
    public void Write(ref WireWriter writer)
    {
        writer.WriteStringList(Targets);
        writer.WriteVarUInt(ControlIndex);
        writer.WriteExtensionBlock(Extensions);
    }

    /// <inheritdoc />
    public static BackupTargetUpdateResultMessage Read(ref WireReader reader)
    {
        IReadOnlyList<string> targets = reader.ReadStringList();
        ulong index = reader.ReadVarUInt();
        IReadOnlyList<WireExtension> extensions = reader.ReadExtensionBlock();
        return new BackupTargetUpdateResultMessage(targets, index) { Extensions = extensions };
    }
}
