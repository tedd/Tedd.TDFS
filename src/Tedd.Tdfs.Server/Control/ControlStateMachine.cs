using System.Buffers;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Raft;

namespace Tedd.Tdfs.Server.Control;

/// <summary>
/// Deterministic state machine of the control group: applies <see cref="ControlCommand"/>s to the <see cref="ControlState"/>.
/// Every decision re-validates against the replicated state (the leader's pre-checks are advisory), uses only data carried
/// in the command (leader time, random ids, verifiers) and iterates ordered collections only.
/// </summary>
internal sealed partial class ControlStateMachine : IReplicatedStateMachine, ICommandDescriber
{
    /// <summary>The replicated state.</summary>
    public ControlState State { get; } = new();

    /// <inheritdoc />
    public string GroupName => "control";

    /// <inheritdoc />
    public ulong AppliedIndex => State.AppliedIndex;

    /// <summary>Raised after every applied command (index); handlers run on the node loop and must not re-enter apply.</summary>
    public event Action<ulong>? Changed;

    /// <inheritdoc />
    public ApplyOutcome Apply(ulong index, ulong term, ReadOnlyMemory<byte> command)
    {
        ControlCommand cmd;
        try
        {
            cmd = ControlCommandCodec.Decode(command.Span);
        }
        catch (DfsProtocolException ex)
        {
            State.AppliedIndex = index;
            return ApplyOutcome.Failure(DfsErrorCode.ProtocolError, "Undecodable control command: " + ex.Message);
        }

        UInt128 requestId = cmd.Header.RequestId;
        if (requestId != UInt128.Zero && State.AdminOutcomes.TryGetValue(requestId, out AdminOutcome? prior))
        {
            State.AppliedIndex = index;
            return Replay(prior);
        }

        string? scope = string.IsNullOrEmpty(cmd.Header.Principal) ? null : ScopeOf(cmd);
        ApplyOutcome outcome = cmd switch
        {
            BootstrapClusterCommand c => ApplyBootstrap(c),
            AddMemberCommand c => ApplyAddMember(c, index),
            RegisterIncarnationCommand c => ApplyRegisterIncarnation(c),
            AdmitMemberCommand c => ApplyAdmitMember(c, index),
            SetMemberStatusCommand c => ApplySetMemberStatus(c),
            SetMemberLabelsCommand c => ApplySetMemberLabels(c),
            SetMemberReleaseCommand c => ApplySetMemberRelease(c),
            CreateVolumeCommand c => ApplyCreateVolume(c.Creation, c.WrappedDataKey, WireVolumeState.Active, index),
            CreateRestoringVolumeCommand c => ApplyCreateVolume(c.Creation, c.WrappedDataKey, WireVolumeState.Restoring, index),
            FinishRestoringVolumeCommand c => ApplyFinishRestoringVolume(c),
            DeleteVolumeCommand c => ApplyDeleteVolume(c),
            SetVolumeVotersCommand c => ApplySetVolumeVoters(c),
            SetDefaultPolicyCommand c => ApplySetDefaultPolicy(c),
            DefinePolicyCommand c => ApplyDefinePolicy(c),
            CompletePolicyMigrationCommand c => ApplyCompletePolicyMigration(c),
            AddDataKeyCommand c => ApplyAddDataKey(c, index),
            CreatePrincipalCommand c => ApplyCreatePrincipal(c, index),
            DeletePrincipalCommand c => ApplyDeletePrincipal(c),
            SetGrantCommand c => ApplySetGrant(c),
            GroupCommand c => ApplyGroup(c, index),
            PoolCommand c => ApplyPool(c, index),
            RegisterBackupTargetCommand c => ApplyRegisterBackupTarget(c, index),
            RemoveBackupTargetCommand c => ApplyRemoveBackupTarget(c),
            SetFeatureCommand c => ApplySetFeature(c),
            AppendAuditCommand => ApplyOutcome.Success(),
            _ => ApplyOutcome.Failure(DfsErrorCode.Unsupported, $"Unsupported control command {cmd.Kind}."),
        };

        if (requestId != UInt128.Zero)
        {
            (Guid refGuid, ulong refNumber) = ReferenceOf(outcome);
            State.RecordAdminOutcome(new AdminOutcome(requestId, cmd.Kind, outcome.Code, outcome.Message, refGuid, refNumber, index));
        }

        if (scope is not null)
            Audit(cmd, outcome, scope, index);

        State.AppliedIndex = index;
        Changed?.Invoke(index);
        return outcome;
    }

    /// <inheritdoc />
    public void WriteCheckpoint(IBufferWriter<byte> output) => State.Write(output);

    /// <inheritdoc />
    public void RestoreCheckpoint(ReadOnlySequence<byte> data, ulong index, ulong term)
    {
        // The image carries the index of the last applied control command; the checkpoint position may be a later
        // configuration or no-op entry. Keeping the serialized value (instead of overwriting it with the position) keeps the
        // state byte-identical on replicas that installed the image and on replicas that applied the log.
        State.Read(data.IsSingleSegment ? data.FirstSpan : data.ToArray());
        Changed?.Invoke(index);
    }

    /// <inheritdoc />
    public ProposalContext Describe(ReadOnlyMemory<byte> command)
    {
        try
        {
            return new ProposalContext(ControlCommandCodec.Decode(command.Span).FaultOperation);
        }
        catch (DfsProtocolException)
        {
            return ProposalContext.None;
        }
    }

    private ApplyOutcome Replay(AdminOutcome prior)
    {
        object? value = prior.Kind switch
        {
            ControlCommandKind.CreateVolume or ControlCommandKind.CreateRestoringVolume or ControlCommandKind.FinishRestoringVolume
                => State.FindVolume(new VolumeId(prior.RefGuid)),
            ControlCommandKind.AdmitMember => State.FindMember(new NodeId(prior.RefGuid)),
            ControlCommandKind.AddDataKey => (uint)prior.RefNumber,
            _ => null,
        };
        return new ApplyOutcome(prior.Code, prior.Message, value);
    }

    private static (Guid RefGuid, ulong RefNumber) ReferenceOf(ApplyOutcome outcome) => outcome.Value switch
    {
        VolumeRecord v => (v.Id.Value, 0),
        MemberRecord m => (m.Id.Value, 0),
        uint version => (Guid.Empty, version),
        _ => (Guid.Empty, 0),
    };

    private string ScopeOf(ControlCommand cmd) => cmd switch
        {
            CreateVolumeCommand c => "volume=" + c.Creation.Name,
            CreateRestoringVolumeCommand c => "volume=" + c.Creation.Name + " restoring",
            FinishRestoringVolumeCommand c => "volume=" + NameOf(c.Volume) + (c.Activate ? " activate" : " abandon"),
            DeleteVolumeCommand c => "volume=" + NameOf(c.Volume),
            SetDefaultPolicyCommand c => "volume=" + NameOf(c.Volume),
            DefinePolicyCommand c => "volume=" + NameOf(c.Volume) + " policy=" + c.PolicyName,
            AddDataKeyCommand c => "volume=" + NameOf(c.Volume) + " keyVersion=" + c.Version.ToString(System.Globalization.CultureInfo.InvariantCulture),
            CreatePrincipalCommand c => "principal=" + c.Principal.Name + (c.Principal.ClusterAdmin ? " clusterAdmin" : string.Empty),
            DeletePrincipalCommand c => "principal=" + c.Name,
            SetGrantCommand c => "volume=" + NameOf(c.Volume) + " principal=" + c.Principal + " permissions=" + c.Permissions,
            GroupCommand c => "group=" + c.Group + (c.Principal is null ? string.Empty : " principal=" + c.Principal),
            PoolCommand c => "pool=" + c.Pool,
            AdmitMemberCommand c => "node=" + c.Name,
            SetMemberStatusCommand c => "node=" + MemberName(c.Id) + " status=" + c.Status + (c.Force ? " force" : string.Empty),
            SetMemberLabelsCommand c => "node=" + MemberName(c.Id),
            RegisterBackupTargetCommand c => "volume=" + NameOf(c.Volume) + " target=" + c.Target.Name,
            RemoveBackupTargetCommand c => "volume=" + NameOf(c.Volume) + " target=" + c.Target,
            SetFeatureCommand c => "feature=" + c.Feature + (c.Enabled ? " on" : " off"),
            AppendAuditCommand c => c.Scope,
            _ => string.Empty,
        };

    private void Audit(ControlCommand cmd, ApplyOutcome outcome, string scope, ulong index)
    {
        string operation = cmd switch
        {
            AppendAuditCommand a => a.Operation,
            SetGrantCommand g => g.Revoke ? "Revoke" : "Grant",
            AddDataKeyCommand => "RotateDataKey",   // a volume's first key is part of CreateVolume; every later one rotates it
            _ => cmd.Kind.ToString(),
        };
        string result = cmd is AppendAuditCommand au ? au.Outcome : outcome.IsSuccess ? "ok" : "failed:" + outcome.Code;
        State.AppendAudit(new AuditEntry(index, cmd.Header.Time, cmd.Header.Principal, operation, result, scope));
    }

    private string NameOf(VolumeId id) => State.FindVolume(id)?.Name ?? id.ToString();

    private string MemberName(NodeId id) => State.FindMember(id)?.Name ?? id.ToString();

    private static ApplyOutcome Fail(DfsErrorCode code, string message) => ApplyOutcome.Failure(code, message);
}
