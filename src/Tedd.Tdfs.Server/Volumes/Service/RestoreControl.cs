using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Control;
using Tedd.Tdfs.Server.Raft;
using Tedd.Tdfs.Server.Runtime;
using Tedd.Tdfs.Server.Security;

namespace Tedd.Tdfs.Server.Volumes.Service;

/// <summary>
/// Control-leader side of a snapshot restore (<c>RestoreVolume</c>, PROTOCOL.md §8.5): creates the target volume in the
/// <see cref="WireVolumeState.Restoring"/> state with the source volume's configuration (bound to the requested pool, voters
/// chosen from it), activates it once its namespace is installed, or removes it when the restore cannot complete. Every
/// step is deduplicated by its request id through the control group's administrative outcomes, so a coordinator that
/// resumes after a crash or leadership change gets the volume its first attempt created.
/// </summary>
internal sealed class RestoreControl
{
    private readonly NodeServices _s;

    public RestoreControl(NodeServices services)
    {
        ArgumentNullException.ThrowIfNull(services);
        _s = services;
    }

    /// <summary>The <c>RestoreVolume</c> handler (node peers only).</summary>
    public async ValueTask<IWireMessage> HandleAsync(RpcContext context, IWireMessage request)
        => await ExecuteAsync((RestoreVolumeMessage)request, context.Cancellation);

    /// <summary>Performs one restore control step on the control leader.</summary>
    public async Task<RestoreVolumeResultMessage> ExecuteAsync(RestoreVolumeMessage message, CancellationToken ct)
    {
        ControlHost control = _s.Control;
        if (!control.IsLeader)
            throw control.NotLeader();
        if (message.RequestId == UInt128.Zero)
            throw new ArgumentException("A restore control step needs a request id.");
        await control.Group!.ReadIndexAsync(ct);
        ControlState state = control.State;
        if (state.AdminOutcomes.TryGetValue(message.RequestId, out AdminOutcome? prior))
        {
            if (prior.Code != DfsErrorCode.None)
                throw DfsErrors.ToException(prior.Code, prior.Message);
            return Reply(state.FindVolume(new VolumeId(prior.RefGuid)));
        }

        return message.Action switch
        {
            WireRestoreAction.Create => await CreateAsync(message, ct),
            WireRestoreAction.Activate => await FinishAsync(message, activate: true, ct),
            WireRestoreAction.Abandon => await FinishAsync(message, activate: false, ct),
            _ => throw new ArgumentException($"Unknown restore action {message.Action}."),
        };
    }

    private async Task<RestoreVolumeResultMessage> CreateAsync(RestoreVolumeMessage message, CancellationToken ct)
    {
        ControlHost control = _s.Control;
        ControlState state = control.State;
        VolumeRecord source = state.FindVolume(message.SourceVolume)
            ?? throw new DfsException(DfsErrorCode.VolumeNotFound, $"Volume {message.SourceVolume} does not exist.");
        if (!state.IsClusterAdmin(message.Principal))
            throw new UnauthorizedAccessException($"Principal '{message.Principal}' must be a cluster administrator to restore a snapshot into a new volume.");
        if (ControlStateMachine.GetVolumeNameError(message.NewVolumeName) is { } nameError)
            throw new ArgumentException(nameError);
        if (state.FindVolumeByName(message.NewVolumeName) is not null)
            throw new DfsAlreadyExistsException($"Volume '{message.NewVolumeName}' already exists.");
        string pool = message.StoragePool ?? source.StoragePool;
        if (DfsPoolNames.GetError(pool) is { } poolError)
            throw new ArgumentException(poolError);
        if (!state.PoolExists(pool))
            throw new DfsException(DfsErrorCode.PoolNotFound, $"Storage pool '{pool}' does not exist.");
        if (ControlStateMachine.CheckPolicy(state, source.DefaultPolicy, pool) is { } policyError)
            throw new DfsException(policyError.Code, policyError.Message);
        var named = new SortedDictionary<string, DfsStoragePolicy>(StringComparer.Ordinal);
        foreach (KeyValuePair<string, DfsStoragePolicy> pair in source.NamedPolicies)
        {
            if (ControlStateMachine.CheckPolicy(state, pair.Value, pool) is { } namedError)
                throw new DfsException(namedError.Code, $"Policy '{pair.Key}': {namedError.Message}");
            named[pair.Key] = pair.Value;
        }

        var id = VolumeId.New(_s.Random);
        IReadOnlyList<NodeId> voters = MembershipManager.ChooseVolumeVoters(state, id, source.Creation.Metadata, pool);
        var creation = new VolumeCreation(id, message.NewVolumeName, 1, message.Principal, _s.Time.GetUtcNow(), 0, source.Creation.Metadata, source.DefaultPolicy,
            named, source.Creation.InlineThreshold, source.Creation.Limits, pool, null, source.Creation.EnforceTraverseChecking, voters);
        byte[] dataKey = DataKeys.NewDataKey(_s.Random);
        byte[] wrapped = DataKeys.Wrap(_s.Keys.StorageKek, id, 1, dataKey, _s.Random);
        System.Security.Cryptography.CryptographicOperations.ZeroMemory(dataKey);
        ApplyOutcome outcome = await control.ProposeAsync(
            new CreateRestoringVolumeCommand(control.NewHeader(message.RequestId, message.Principal), creation, wrapped), ct);
        if (!outcome.IsSuccess)
            throw outcome.ToException();
        VolumeRecord record = outcome.Value as VolumeRecord ?? state.FindVolume(id)
            ?? throw new DfsException(DfsErrorCode.Internal, "The restoring volume is missing from the control state.");
        _s.Info("volume", NodeServices.Inv($"Created volume {record.Name} ({record.Id}) to restore a snapshot of {source.Name}; it is not served until the restore completes."));
        return Reply(record);
    }

    private async Task<RestoreVolumeResultMessage> FinishAsync(RestoreVolumeMessage message, bool activate, CancellationToken ct)
    {
        ControlHost control = _s.Control;
        VolumeRecord? volume = control.State.FindVolume(message.Volume);
        if (volume is null)
        {
            if (activate)
                throw new DfsException(DfsErrorCode.VolumeNotFound, $"Volume {message.Volume} does not exist.");
            return Reply(null);
        }

        if (!string.Equals(volume.Name, message.NewVolumeName, StringComparison.Ordinal))
            throw new ArgumentException($"Volume {message.Volume} is named '{volume.Name}', not '{message.NewVolumeName}'.");
        if (activate)
            await _s.HitAsync(FaultStage.F21, FaultSites.Reconfiguration, volume.Name, DfsOperationKind.RestoreSnapshot, ct: ct);
        ApplyOutcome outcome = await control.ProposeAsync(
            new FinishRestoringVolumeCommand(control.NewHeader(message.RequestId, message.Principal), volume.Id, activate), ct);
        if (!outcome.IsSuccess)
            throw outcome.ToException();
        if (!activate)
            _s.Info("volume", NodeServices.Inv($"Removed volume {volume.Name} ({volume.Id}): its restore could not complete."));
        return Reply(outcome.Value as VolumeRecord ?? (activate ? control.State.FindVolume(volume.Id) : null));
    }

    private RestoreVolumeResultMessage Reply(VolumeRecord? record) => new(record is null ? null : _s.Control.ToVolumeEntry(record));
}
