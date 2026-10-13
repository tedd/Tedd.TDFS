using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Raft;
using Tedd.Tdfs.Server.Runtime;

namespace Tedd.Tdfs.Server.Control;

internal sealed partial class ControlStateMachine
{
    /// <summary>Validates a principal name (same rules as <see cref="DfsIdentity"/> user names).</summary>
    public static string? GetPrincipalNameError(string? name)
        => DfsIdentity.IsValidName(name) ? null : $"'{name}' is not a valid principal name.";

    private ApplyOutcome ApplyCreatePrincipal(CreatePrincipalCommand c, ulong index)
    {
        PrincipalData p = c.Principal;
        if (GetPrincipalNameError(p.Name) is { } error)
            return Fail(DfsErrorCode.InvalidArgument, error);
        if (State.Principals.ContainsKey(p.Name))
            return Fail(DfsErrorCode.AlreadyExists, $"Principal '{p.Name}' already exists.");
        if (p.Salt.IsEmpty || p.Iterations < 1 || p.Iterations > HandshakeAuth.MaxIterations
            || p.StoredKey.Length != ProtocolInfo.MacSize || p.ServerKey.Length != ProtocolInfo.MacSize)
        {
            return Fail(DfsErrorCode.InvalidArgument, "The principal verifier is malformed.");
        }

        State.Principals[p.Name] = new PrincipalRecord(p.Name, p.Salt, p.Iterations, p.StoredKey, p.ServerKey, p.ClusterAdmin,
            new SortedDictionary<VolumeId, DfsPermissions>(VolumeIdComparer.Instance), index);
        return ApplyOutcome.Success();
    }

    private ApplyOutcome ApplyDeletePrincipal(DeletePrincipalCommand c)
    {
        if (!State.Principals.Remove(c.Name))
            return Fail(DfsErrorCode.PrincipalNotFound, $"Principal '{c.Name}' does not exist.");
        foreach (GroupRecord g in State.Groups.Values.ToArray())
        {
            if (g.Members.Contains(c.Name, StringComparer.Ordinal))
                State.Groups[g.Name] = g with { Members = g.Members.Where(m => !string.Equals(m, c.Name, StringComparison.Ordinal)).ToArray() };
        }

        return ApplyOutcome.Success();
    }

    private ApplyOutcome ApplySetGrant(SetGrantCommand c)
    {
        if (!State.Volumes.ContainsKey(c.Volume))
            return Fail(DfsErrorCode.VolumeNotFound, $"Volume {c.Volume} does not exist.");
        if (!State.Principals.TryGetValue(c.Principal, out PrincipalRecord? p))
            return Fail(DfsErrorCode.PrincipalNotFound, $"Principal '{c.Principal}' does not exist.");
        if (c.Permissions == DfsPermissions.None || (c.Permissions & ~DfsPermissions.All) != 0)
            return Fail(DfsErrorCode.InvalidArgument, $"Permissions {(int)c.Permissions} contain undefined bits or are empty.");
        DfsPermissions current = p.Grants.TryGetValue(c.Volume, out DfsPermissions existing) ? existing : DfsPermissions.None;
        DfsPermissions next = c.Revoke ? current & ~c.Permissions : current | c.Permissions;
        var grants = new SortedDictionary<VolumeId, DfsPermissions>(p.Grants.Where(g => g.Key != c.Volume).ToDictionary(), VolumeIdComparer.Instance);
        if (next != DfsPermissions.None)
            grants[c.Volume] = next;
        State.Principals[p.Name] = p with { Grants = grants };
        return ApplyOutcome.Success();
    }

    private ApplyOutcome ApplyGroup(GroupCommand c, ulong index)
    {
        switch (c.Kind)
        {
            case ControlCommandKind.CreateGroup:
                if (!DfsIdentity.IsValidName(c.Group))
                    return Fail(DfsErrorCode.InvalidArgument, $"'{c.Group}' is not a valid group name.");
                if (State.Groups.ContainsKey(c.Group))
                    return Fail(DfsErrorCode.AlreadyExists, $"Group '{c.Group}' already exists.");
                State.Groups[c.Group] = new GroupRecord(c.Group, [], index);
                return ApplyOutcome.Success();
            case ControlCommandKind.DeleteGroup:
                return State.Groups.Remove(c.Group)
                    ? ApplyOutcome.Success()
                    : Fail(DfsErrorCode.GroupNotFound, $"Group '{c.Group}' does not exist.");
            case ControlCommandKind.AddGroupMember:
            case ControlCommandKind.RemoveGroupMember:
            {
                if (!State.Groups.TryGetValue(c.Group, out GroupRecord? g))
                    return Fail(DfsErrorCode.GroupNotFound, $"Group '{c.Group}' does not exist.");
                string principal = c.Principal ?? string.Empty;
                if (!State.Principals.ContainsKey(principal))
                    return Fail(DfsErrorCode.PrincipalNotFound, $"Principal '{principal}' does not exist.");
                var members = new SortedSet<string>(g.Members, StringComparer.Ordinal);
                if (c.Kind == ControlCommandKind.AddGroupMember)
                    members.Add(principal);
                else
                    members.Remove(principal);
                State.Groups[g.Name] = g with { Members = members.ToArray() };
                return ApplyOutcome.Success();
            }

            default:
                return Fail(DfsErrorCode.Unsupported, $"Unsupported group command {c.Kind}.");
        }
    }

    private ApplyOutcome ApplyPool(PoolCommand c, ulong index)
    {
        bool isDefault = string.Equals(c.Pool, DfsPoolNames.Default, StringComparison.Ordinal);
        switch (c.Kind)
        {
            case ControlCommandKind.CreatePool:
            case ControlCommandKind.SetPoolMembers:
            {
                if (DfsPoolNames.GetError(c.Pool) is { } nameError)
                    return Fail(DfsErrorCode.InvalidArgument, nameError);
                if (isDefault && c.Kind == ControlCommandKind.CreatePool)
                    return Fail(DfsErrorCode.AlreadyExists, "The default pool always exists (it implicitly contains every live node).");
                if (isDefault)
                    return Fail(DfsErrorCode.InvalidArgument, "The default pool is implicit and always contains every live node.");
                if (c.Kind == ControlCommandKind.CreatePool && State.Pools.ContainsKey(c.Pool))
                    return Fail(DfsErrorCode.AlreadyExists, $"Pool '{c.Pool}' already exists.");
                if (c.Kind == ControlCommandKind.SetPoolMembers && !State.Pools.ContainsKey(c.Pool))
                    return Fail(DfsErrorCode.PoolNotFound, $"Pool '{c.Pool}' does not exist.");
                if (c.Nodes.Count == 0)
                    return Fail(DfsErrorCode.InvalidArgument, "A pool needs at least one node.");
                var nodes = new SortedSet<NodeId>(NodeIdComparer.Instance);
                foreach (NodeId id in c.Nodes)
                {
                    if (State.FindMember(id) is not { IsLive: true })
                        return Fail(DfsErrorCode.InvalidArgument, $"Node {id} is not a live member.");
                    nodes.Add(id);
                }

                ulong created = State.Pools.TryGetValue(c.Pool, out PoolRecord? existing) ? existing.CreatedIndex : index;
                State.Pools[c.Pool] = new PoolRecord(c.Pool, nodes.ToArray(), created);
                State.PlacementEpoch++;
                return ApplyOutcome.Success();
            }

            case ControlCommandKind.DeletePool:
            {
                if (isDefault)
                    return Fail(DfsErrorCode.PoolInUse, "The default pool cannot be deleted.");
                if (!State.Pools.ContainsKey(c.Pool))
                    return Fail(DfsErrorCode.PoolNotFound, $"Pool '{c.Pool}' does not exist.");
                foreach (VolumeRecord v in State.Volumes.Values)
                {
                    bool referenced = string.Equals(v.StoragePool, c.Pool, StringComparison.Ordinal)
                        || string.Equals(v.DefaultPolicy.Pool, c.Pool, StringComparison.Ordinal)
                        || v.NamedPolicies.Values.Any(p => string.Equals(p.Pool, c.Pool, StringComparison.Ordinal));
                    if (referenced)
                        return Fail(DfsErrorCode.PoolInUse, $"Pool '{c.Pool}' is used by volume '{v.Name}'.");
                }

                State.Pools.Remove(c.Pool);
                State.PlacementEpoch++;
                return ApplyOutcome.Success();
            }

            default:
                return Fail(DfsErrorCode.Unsupported, $"Unsupported pool command {c.Kind}.");
        }
    }

    private ApplyOutcome ApplySetFeature(SetFeatureCommand c)
    {
        if (string.IsNullOrEmpty(c.Feature))
            return Fail(DfsErrorCode.InvalidArgument, "A feature name is required.");
        if (!c.Enabled)
        {
            if (!State.Features.Contains(c.Feature))
                return ApplyOutcome.Success();
            if (ProtocolFeatures.Irreversible.Contains(c.Feature))
                return Fail(DfsErrorCode.Unsupported, $"Feature '{c.Feature}' is irreversible once active.");
            State.Features.Remove(c.Feature);
            return ApplyOutcome.Success();
        }

        foreach (MemberRecord m in State.Members.Values)
        {
            if (m.IsLive && !m.PendingAdmission && !m.Features.Contains(c.Feature, StringComparer.Ordinal))
                return Fail(DfsErrorCode.Unsupported, $"Feature '{c.Feature}' is not supported by node {m.Name}.");
        }

        State.Features.Add(c.Feature);
        return ApplyOutcome.Success();
    }
}
