using Tedd.Tdfs.Data;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Raft;

namespace Tedd.Tdfs.Server.Control;

internal sealed partial class ControlStateMachine
{
    /// <summary>Validates a volume name: 1..255 UTF-8 bytes, no control characters, no '/', '\' or ':'.</summary>
    public static string? GetVolumeNameError(string? name)
    {
        if (string.IsNullOrEmpty(name))
            return "The volume name must not be empty.";
        if (System.Text.Encoding.UTF8.GetByteCount(name) > 255)
            return "The volume name exceeds 255 UTF-8 bytes.";
        foreach (char ch in name)
        {
            if (char.IsControl(ch) || ch is '/' or '\\' or ':')
                return $"The volume name '{name}' contains an invalid character.";
        }

        return name.Trim().Length == 0 ? "The volume name must not be blank." : null;
    }

    /// <summary>
    /// Checks that <paramref name="policy"/> is valid and satisfiable by the eligible members of its pool (POOL-03); the pool
    /// is the policy's own pool or <paramref name="volumePool"/>. Returns the error, or null.
    /// </summary>
    public static (DfsErrorCode Code, string Message)? CheckPolicy(ControlState state, DfsStoragePolicy policy, string volumePool)
    {
        string? error = policy.GetValidationError();
        if (error is not null)
            return (DfsErrorCode.InvalidPolicy, error);
        string pool = policy.Pool ?? volumePool;
        if (!state.PoolExists(pool))
            return (DfsErrorCode.PoolNotFound, $"Storage pool '{pool}' does not exist.");
        IReadOnlyList<PlacementNode> nodes = state.PoolMembers(pool)
            .Select(m => new PlacementNode(m.Id, m.Name, m.Labels, m.IsPlacementEligible, Math.Max(1, m.CapacityBytes - m.UsedBytes)))
            .ToArray();
        if (!Placement.IsSatisfiable(policy, nodes, out string? reason))
            return (DfsErrorCode.InvalidPolicy, $"Policy is not satisfiable by pool '{pool}': {reason}");
        return null;
    }

    private ApplyOutcome ApplyCreateVolume(CreateVolumeCommand c, ulong index)
    {
        VolumeCreation creation = c.Creation;
        string? nameError = GetVolumeNameError(creation.Name);
        if (nameError is not null)
            return Fail(DfsErrorCode.InvalidArgument, nameError);
        if (State.FindVolumeByName(creation.Name) is not null || State.Volumes.ContainsKey(creation.Id))
            return Fail(DfsErrorCode.AlreadyExists, $"Volume '{creation.Name}' already exists.");
        if (!State.PoolExists(creation.StoragePool))
            return Fail(DfsErrorCode.PoolNotFound, $"Storage pool '{creation.StoragePool}' does not exist.");
        if ((CheckPolicy(State, creation.DefaultPolicy, creation.StoragePool) ?? CheckNamed(creation.NamedPolicies, creation.StoragePool)) is { } policyError)
            return Fail(policyError.Code, policyError.Message);
        if (creation.InitialVoters.Count == 0 || creation.InitialVoters.Distinct().Count() != creation.InitialVoters.Count)
            return Fail(DfsErrorCode.InvalidPolicy, "A volume needs distinct initial voters.");
        foreach (NodeId voter in creation.InitialVoters)
        {
            if (State.FindMember(voter) is not { IsLive: true })
                return Fail(DfsErrorCode.PlacementUnavailable, $"Voter {voter} is not a live member.");
        }

        VolumeCreation stamped = creation with
        {
            CreatedIndex = index,
            NamedPolicies = Sorted(creation.NamedPolicies),
        };
        var record = new VolumeRecord(stamped, WireVolumeState.Active, stamped.InitialVoters.ToArray(), [], stamped.DefaultPolicy, stamped.NamedPolicies,
            null, [new DataKeyVersion(1, c.WrappedDataKey, index)], new SortedDictionary<string, BackupTargetRecord>(StringComparer.Ordinal));
        State.Volumes[record.Id] = record;
        if (State.Principals.TryGetValue(creation.Creator, out PrincipalRecord? creator))
        {
            var grants = new SortedDictionary<VolumeId, DfsPermissions>(creator.Grants.ToDictionary(), VolumeIdComparer.Instance)
            {
                [record.Id] = DfsPermissions.All,
            };
            State.Principals[creator.Name] = creator with { Grants = grants };
        }

        return ApplyOutcome.Success(record);
    }

    private (DfsErrorCode Code, string Message)? CheckNamed(IReadOnlyDictionary<string, DfsStoragePolicy> named, string volumePool)
    {
        foreach (KeyValuePair<string, DfsStoragePolicy> pair in named.OrderBy(p => p.Key, StringComparer.Ordinal))
        {
            if (string.IsNullOrEmpty(pair.Key))
                return (DfsErrorCode.InvalidPolicy, "Named policies need a name.");
            if (CheckPolicy(State, pair.Value, volumePool) is { } error)
                return (error.Code, $"Policy '{pair.Key}': {error.Message}");
        }

        return null;
    }

    private ApplyOutcome ApplyDeleteVolume(DeleteVolumeCommand c)
    {
        if (!State.Volumes.Remove(c.Volume, out VolumeRecord? removed))
            return Fail(DfsErrorCode.VolumeNotFound, $"Volume {c.Volume} does not exist.");
        foreach (PrincipalRecord p in State.Principals.Values.ToArray())
        {
            if (!p.Grants.ContainsKey(c.Volume))
                continue;
            var grants = new SortedDictionary<VolumeId, DfsPermissions>(p.Grants.Where(g => g.Key != c.Volume).ToDictionary(), VolumeIdComparer.Instance);
            State.Principals[p.Name] = p with { Grants = grants };
        }

        return ApplyOutcome.Success(removed);
    }

    private ApplyOutcome ApplySetVolumeVoters(SetVolumeVotersCommand c)
    {
        if (!State.Volumes.TryGetValue(c.Volume, out VolumeRecord? v))
            return Fail(DfsErrorCode.VolumeNotFound, $"Volume {c.Volume} does not exist.");
        if (c.Voters.Count == 0)
            return Fail(DfsErrorCode.InvalidArgument, "A volume needs at least one voter.");
        foreach (NodeId id in c.Voters.Concat(c.Learners))
        {
            if (State.FindMember(id) is not { IsLive: true })
                return Fail(DfsErrorCode.InvalidArgument, $"Node {id} is not a live member.");
        }

        if (c.Voters.Concat(c.Learners).Distinct().Count() != c.Voters.Count + c.Learners.Count)
            return Fail(DfsErrorCode.InvalidArgument, "Volume members must be distinct.");
        VolumeRecord updated = v with { Voters = c.Voters.ToArray(), Learners = c.Learners.ToArray() };
        State.Volumes[v.Id] = updated;
        return ApplyOutcome.Success(updated);
    }

    private ApplyOutcome ApplySetDefaultPolicy(SetDefaultPolicyCommand c)
    {
        if (!State.Volumes.TryGetValue(c.Volume, out VolumeRecord? v))
            return Fail(DfsErrorCode.VolumeNotFound, $"Volume {c.Volume} does not exist.");
        if (CheckPolicy(State, c.Policy, v.StoragePool) is { } error)
            return Fail(error.Code, error.Message);
        VolumeRecord updated = v with { DefaultPolicy = c.Policy, MigrationTarget = c.Migrate ? c.Policy : null };
        State.Volumes[v.Id] = updated;
        return ApplyOutcome.Success(updated);
    }

    private ApplyOutcome ApplyDefinePolicy(DefinePolicyCommand c)
    {
        if (!State.Volumes.TryGetValue(c.Volume, out VolumeRecord? v))
            return Fail(DfsErrorCode.VolumeNotFound, $"Volume {c.Volume} does not exist.");
        if (string.IsNullOrEmpty(c.PolicyName) || System.Text.Encoding.UTF8.GetByteCount(c.PolicyName) > 255)
            return Fail(DfsErrorCode.InvalidPolicy, "A policy name must be 1..255 UTF-8 bytes.");
        if (CheckPolicy(State, c.Policy, v.StoragePool) is { } error)
            return Fail(error.Code, error.Message);
        var named = new SortedDictionary<string, DfsStoragePolicy>(v.NamedPolicies.ToDictionary(StringComparer.Ordinal), StringComparer.Ordinal)
        {
            [c.PolicyName] = c.Policy,
        };
        VolumeRecord updated = v with { NamedPolicies = named };
        State.Volumes[v.Id] = updated;
        return ApplyOutcome.Success(updated);
    }

    private ApplyOutcome ApplyCompletePolicyMigration(CompletePolicyMigrationCommand c)
    {
        if (!State.Volumes.TryGetValue(c.Volume, out VolumeRecord? v))
            return Fail(DfsErrorCode.VolumeNotFound, $"Volume {c.Volume} does not exist.");
        VolumeRecord updated = v with { MigrationTarget = null };
        State.Volumes[v.Id] = updated;
        return ApplyOutcome.Success(updated);
    }

    private ApplyOutcome ApplyAddDataKey(AddDataKeyCommand c, ulong index)
    {
        if (!State.Volumes.TryGetValue(c.Volume, out VolumeRecord? v))
            return Fail(DfsErrorCode.VolumeNotFound, $"Volume {c.Volume} does not exist.");
        if (c.Version != v.CurrentKeyVersion + 1)
            return Fail(DfsErrorCode.InvalidArgument, $"Data key version {c.Version} does not follow the current version {v.CurrentKeyVersion}.");
        if (c.WrappedKey.IsEmpty)
            return Fail(DfsErrorCode.InvalidArgument, "The wrapped data key is empty.");
        VolumeRecord updated = v with { DataKeys = [.. v.DataKeys, new DataKeyVersion(c.Version, c.WrappedKey, index)] };
        State.Volumes[v.Id] = updated;
        return ApplyOutcome.Success(c.Version);
    }

    private ApplyOutcome ApplyRegisterBackupTarget(RegisterBackupTargetCommand c, ulong index)
    {
        if (!State.Volumes.TryGetValue(c.Volume, out VolumeRecord? v))
            return Fail(DfsErrorCode.VolumeNotFound, $"Volume {c.Volume} does not exist.");
        if (string.IsNullOrEmpty(c.Target.Name))
            return Fail(DfsErrorCode.InvalidArgument, "A backup target needs a name.");
        var targets = new SortedDictionary<string, BackupTargetRecord>(v.BackupTargets.ToDictionary(StringComparer.Ordinal), StringComparer.Ordinal)
        {
            [c.Target.Name] = c.Target with { RegisteredIndex = v.BackupTargets.TryGetValue(c.Target.Name, out BackupTargetRecord? old) ? old.RegisteredIndex : index },
        };
        VolumeRecord updated = v with { BackupTargets = targets };
        State.Volumes[v.Id] = updated;
        return ApplyOutcome.Success(updated);
    }

    private ApplyOutcome ApplyRemoveBackupTarget(RemoveBackupTargetCommand c)
    {
        if (!State.Volumes.TryGetValue(c.Volume, out VolumeRecord? v))
            return Fail(DfsErrorCode.VolumeNotFound, $"Volume {c.Volume} does not exist.");
        if (!v.BackupTargets.ContainsKey(c.Target))
            return Fail(DfsErrorCode.BackupTargetNotFound, $"Backup target '{c.Target}' is not registered.");
        var targets = new SortedDictionary<string, BackupTargetRecord>(
            v.BackupTargets.Where(t => !string.Equals(t.Key, c.Target, StringComparison.Ordinal)).ToDictionary(StringComparer.Ordinal), StringComparer.Ordinal);
        VolumeRecord updated = v with { BackupTargets = targets };
        State.Volumes[v.Id] = updated;
        return ApplyOutcome.Success(updated);
    }

    private static SortedDictionary<string, DfsStoragePolicy> Sorted(IReadOnlyDictionary<string, DfsStoragePolicy> source)
    {
        var result = new SortedDictionary<string, DfsStoragePolicy>(StringComparer.Ordinal);
        foreach (KeyValuePair<string, DfsStoragePolicy> pair in source)
            result[pair.Key] = pair.Value;
        return result;
    }
}
