using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Control;
using Tedd.Tdfs.Server.Raft;
using Tedd.Tdfs.Server.Security;

namespace Tedd.Tdfs.Server.Admin;

internal sealed partial class AdminService
{
    private async Task<AdminResultMessage> CreateVolumeAsync(AdminRequestMessage message, string principal, CancellationToken ct)
    {
        RequireClusterAdmin(principal);
        AdminVolumeSpecBody spec = message.ReadBody<AdminVolumeSpecBody>();
        RequireLeader();
        if (message.RequestId != UInt128.Zero && State.AdminOutcomes.TryGetValue(message.RequestId, out AdminOutcome? prior) && prior.Code == DfsErrorCode.None
            && State.FindVolume(new VolumeId(prior.RefGuid)) is { } created)
        {
            return Result(message.Op, new AdminVolumeInfoBody(Control.ToVolumeEntry(created), CommitToken.None));
        }

        if (ControlStateMachine.GetVolumeNameError(spec.Name) is { } nameError)
            throw new ArgumentException(nameError);
        if (State.FindVolumeByName(spec.Name) is not null)
            throw new DfsAlreadyExistsException($"Volume '{spec.Name}' already exists.");
        string pool = string.IsNullOrEmpty(spec.StoragePool) ? DfsPoolNames.Default : spec.StoragePool;
        if (DfsPoolNames.GetError(pool) is { } poolError)
            throw new ArgumentException(poolError);
        if (!State.PoolExists(pool))
            throw new DfsException(DfsErrorCode.PoolNotFound, $"Storage pool '{pool}' does not exist.");
        if (spec.InlineThreshold < 0)
            throw new ArgumentException("The inline threshold must not be negative.");
        if (spec.RootSecurity?.GetValidationError() is { } securityError)
            throw new ArgumentException("Invalid root security descriptor: " + securityError);
        if (ControlStateMachine.CheckPolicy(State, spec.DefaultPolicy, pool) is { } policyError)
            throw new DfsException(policyError.Code, policyError.Message);
        foreach (KeyValuePair<string, DfsStoragePolicy> namedPolicy in spec.NamedPolicies)
        {
            if (ControlStateMachine.CheckPolicy(State, namedPolicy.Value, pool) is { } namedError)
                throw new DfsException(namedError.Code, $"Policy '{namedPolicy.Key}': {namedError.Message}");
        }

        var id = VolumeId.New(_s.Random);
        IReadOnlyList<NodeId> voters = MembershipManager.ChooseVolumeVoters(State, id, spec.Metadata, pool);
        var named = new SortedDictionary<string, DfsStoragePolicy>(StringComparer.Ordinal);
        foreach (KeyValuePair<string, DfsStoragePolicy> pair in spec.NamedPolicies)
            named[pair.Key] = pair.Value;
        var creation = new VolumeCreation(id, spec.Name, 1, principal, _s.Time.GetUtcNow(), 0, spec.Metadata, spec.DefaultPolicy, named,
            spec.InlineThreshold, spec.Limits, pool, spec.RootSecurity?.WithoutInheritedRules(), spec.EnforceTraverseChecking, voters);
        byte[] dataKey = DataKeys.NewDataKey(_s.Random);
        byte[] wrapped = DataKeys.Wrap(_s.Keys.StorageKek, id, 1, dataKey, _s.Random);
        System.Security.Cryptography.CryptographicOperations.ZeroMemory(dataKey);
        ApplyOutcome outcome = await ProposeAsync(message, new CreateVolumeCommand(Header(message, principal), creation, wrapped), ct);
        VolumeRecord record = outcome.Value as VolumeRecord ?? State.FindVolume(id)
            ?? throw new DfsException(DfsErrorCode.Internal, "The created volume is missing from the control state.");
        _s.Info("admin", Runtime.NodeServices.Inv($"Created volume {record.Name} ({record.Id}) with voters [{string.Join(", ", record.Voters.Select(Control.NameOf))}]."));
        return Result(message.Op, new AdminVolumeInfoBody(Control.ToVolumeEntry(record), CommitToken.None));
    }

    private async Task<AdminResultMessage> DeleteVolumeAsync(AdminRequestMessage message, string principal, CancellationToken ct)
    {
        RequireClusterAdmin(principal);
        VolumeRecord volume = RequireVolume(message.ReadBody<AdminVolumeNameBody>().Volume);
        return await ProposeEmptyAsync(message, new DeleteVolumeCommand(Header(message, principal), volume.Id), ct);
    }

    private async Task<AdminResultMessage> SetDefaultPolicyAsync(AdminRequestMessage message, string principal, CancellationToken ct)
    {
        AdminSetDefaultPolicyBody body = message.ReadBody<AdminSetDefaultPolicyBody>();
        VolumeRecord volume = RequireVolume(body.Volume);
        RequireVolumeAdmin(principal, volume);
        RequireLeader();
        if (ControlStateMachine.CheckPolicy(State, body.Policy, volume.StoragePool) is { } error)
            throw new DfsException(error.Code, error.Message);
        return await ProposeEmptyAsync(message, new SetDefaultPolicyCommand(Header(message, principal), volume.Id, body.Policy, body.MigrateExisting), ct);
    }

    private async Task<AdminResultMessage> DefinePolicyAsync(AdminRequestMessage message, string principal, CancellationToken ct)
    {
        AdminDefinePolicyBody body = message.ReadBody<AdminDefinePolicyBody>();
        VolumeRecord volume = RequireVolume(body.Volume);
        RequireVolumeAdmin(principal, volume);
        RequireLeader();
        if (string.IsNullOrEmpty(body.PolicyName))
            throw new DfsException(DfsErrorCode.InvalidPolicy, "A policy name is required.");
        if (ControlStateMachine.CheckPolicy(State, body.Policy, volume.StoragePool) is { } error)
            throw new DfsException(error.Code, error.Message);
        return await ProposeEmptyAsync(message, new DefinePolicyCommand(Header(message, principal), volume.Id, body.PolicyName, body.Policy), ct);
    }

    private async Task<AdminResultMessage> GrantAsync(AdminRequestMessage message, string principal, CancellationToken ct)
    {
        AdminGrantBody body = message.ReadBody<AdminGrantBody>();
        VolumeRecord volume = RequireVolume(body.Volume);
        RequireVolumeAdmin(principal, volume);
        if (body.Permissions == DfsPermissions.None || (body.Permissions & ~DfsPermissions.All) != 0)
            throw new ArgumentException("The permissions contain undefined bits or are empty.");
        if ((body.Permissions & DfsPermissions.ClusterAdmin) != 0 && !State.IsClusterAdmin(principal))
            throw new UnauthorizedAccessException("Only cluster administrators may grant ClusterAdmin.");
        RequireLeader();   // "does not exist" is only answered from the leader's view (a follower's may lag)
        if (State.FindPrincipal(body.Principal) is null)
            throw new DfsException(DfsErrorCode.PrincipalNotFound, $"Principal '{body.Principal}' does not exist.");
        bool revoke = message.Op == AdminOp.Revoke;
        return await ProposeEmptyAsync(message, new SetGrantCommand(Header(message, principal), volume.Id, body.Principal, body.Permissions, revoke), ct);
    }

    private async Task<AdminResultMessage> RotateDataKeyAsync(AdminRequestMessage message, string principal, CancellationToken ct)
    {
        VolumeRecord volume = RequireVolume(message.ReadBody<AdminVolumeNameBody>().Volume);
        RequireVolumeAdmin(principal, volume);
        RequireLeader();
        if (message.RequestId != UInt128.Zero && State.AdminOutcomes.TryGetValue(message.RequestId, out AdminOutcome? prior) && prior.Code == DfsErrorCode.None)
            return Result(message.Op, new AdminKeyRotationBody(volume.Name, prior.RefNumber));
        await _s.HitAsync(FaultStage.F21, FaultSites.KeyRotation, volume.Name, DfsOperationKind.KeyRotation, ct: ct);
        uint version = volume.CurrentKeyVersion + 1;
        byte[] dataKey = DataKeys.NewDataKey(_s.Random);
        byte[] wrapped = DataKeys.Wrap(_s.Keys.StorageKek, volume.Id, version, dataKey, _s.Random);
        System.Security.Cryptography.CryptographicOperations.ZeroMemory(dataKey);
        ApplyOutcome outcome = await ProposeAsync(message, new AddDataKeyCommand(Header(message, principal), volume.Id, version, wrapped), ct);
        uint applied = outcome.Value is uint v ? v : version;
        return Result(message.Op, new AdminKeyRotationBody(volume.Name, applied));
    }
}
