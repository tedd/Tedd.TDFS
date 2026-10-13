using Tedd.Tdfs.Client.Internal;
using Tedd.Tdfs.Client.Internal.Facades;
using Tedd.Tdfs.Protocol.Messages;

namespace Tedd.Tdfs;

// Addendum A administration: groups (ACL-01, ACL-08) and storage pools (POOL-01..POOL-05). Requires ClusterAdmin
// (POOL-017); every change is a control-group decision and is audited.
public sealed partial class DfsAdminClient
{
    // ---- Groups -----------------------------------------------------------------------------------------------

    /// <summary>Creates a group.</summary>
    public Task CreateGroupAsync(string group, CancellationToken ct = default) =>
        CallAsync<AdminGroupNameBody, AdminEmptyBody>(AdminOp.CreateGroup, () => new AdminGroupNameBody(Validation.Name(group, nameof(group))),
            Runtime.ControlRouter, DfsOperationKind.Admin, ct);

    /// <summary>Deletes a group.</summary>
    public Task DeleteGroupAsync(string group, CancellationToken ct = default) =>
        CallAsync<AdminGroupNameBody, AdminEmptyBody>(AdminOp.DeleteGroup, () => new AdminGroupNameBody(Validation.Name(group, nameof(group))),
            Runtime.ControlRouter, DfsOperationKind.Admin, ct);

    /// <summary>Adds a principal to a group.</summary>
    public Task AddGroupMemberAsync(string group, string principal, CancellationToken ct = default) =>
        CallAsync<AdminGroupMemberBody, AdminEmptyBody>(AdminOp.AddGroupMember, () =>
            new AdminGroupMemberBody(Validation.Name(group, nameof(group)), Validation.Name(principal, nameof(principal))),
            Runtime.ControlRouter, DfsOperationKind.Admin, ct);

    /// <summary>Removes a principal from a group.</summary>
    public Task RemoveGroupMemberAsync(string group, string principal, CancellationToken ct = default) =>
        CallAsync<AdminGroupMemberBody, AdminEmptyBody>(AdminOp.RemoveGroupMember, () =>
            new AdminGroupMemberBody(Validation.Name(group, nameof(group)), Validation.Name(principal, nameof(principal))),
            Runtime.ControlRouter, DfsOperationKind.Admin, ct);

    /// <summary>Lists the groups and their members.</summary>
    public async Task<IReadOnlyList<DfsGroupInfo>> ListGroupsAsync(CancellationToken ct = default)
    {
        AdminGroupListBody body = await CallAsync<AdminEmptyBody, AdminGroupListBody>(AdminOp.ListGroups, () => new AdminEmptyBody(), Runtime.AnyNodeRouter,
            DfsOperationKind.Admin, ct).ConfigureAwait(false);
        return body.Groups.Select(g => new DfsGroupInfo(g.Name, g.Members)).ToArray();
    }

    // ---- Storage pools ----------------------------------------------------------------------------------------

    /// <summary>Creates a storage pool of the named nodes.</summary>
    public Task CreatePoolAsync(string pool, IReadOnlyList<string> nodeNames, CancellationToken ct = default) =>
        CallAsync<AdminPoolMembersBody, AdminEmptyBody>(AdminOp.CreatePool, () => PoolMembers(pool, nodeNames), Runtime.ControlRouter,
            DfsOperationKind.PolicyChange, ct);

    /// <summary>Replaces the members of a storage pool (data moves to the new members in the background).</summary>
    public Task SetPoolMembersAsync(string pool, IReadOnlyList<string> nodeNames, CancellationToken ct = default) =>
        CallAsync<AdminPoolMembersBody, AdminEmptyBody>(AdminOp.SetPoolMembers, () => PoolMembers(pool, nodeNames), Runtime.ControlRouter,
            DfsOperationKind.PolicyChange, ct);

    /// <summary>Deletes an unused storage pool.</summary>
    public Task DeletePoolAsync(string pool, CancellationToken ct = default) =>
        CallAsync<AdminPoolNameBody, AdminEmptyBody>(AdminOp.DeletePool, () => new AdminPoolNameBody(Validation.Name(pool, nameof(pool))),
            Runtime.ControlRouter, DfsOperationKind.PolicyChange, ct);

    /// <summary>Lists the storage pools.</summary>
    public async Task<IReadOnlyList<DfsPoolInfo>> ListPoolsAsync(CancellationToken ct = default)
    {
        AdminPoolListBody body = await CallAsync<AdminEmptyBody, AdminPoolListBody>(AdminOp.ListPools, () => new AdminEmptyBody(), Runtime.AnyNodeRouter,
            DfsOperationKind.Admin, ct).ConfigureAwait(false);
        return body.Pools.Select(WireConvert.ToPoolInfo).ToArray();
    }

    /// <summary>Status of a storage pool (members, volumes, capacity, replica health, draining nodes).</summary>
    public async Task<DfsPoolStatus> GetPoolStatusAsync(string pool, CancellationToken ct = default)
    {
        AdminPoolStatusBody body = await CallAsync<AdminPoolNameBody, AdminPoolStatusBody>(AdminOp.PoolStatus,
            () => new AdminPoolNameBody(Validation.Name(pool, nameof(pool))), Runtime.AnyNodeRouter, DfsOperationKind.Admin, ct).ConfigureAwait(false);
        return new DfsPoolStatus(WireConvert.ToPoolInfo(body.Info), WireConvert.ToReplicaHealth(body.Replicas), body.DrainingNodes);
    }

    private static AdminPoolMembersBody PoolMembers(string pool, IReadOnlyList<string> nodeNames)
    {
        Validation.Name(pool, nameof(pool));
        ArgumentNullException.ThrowIfNull(nodeNames);
        if (nodeNames.Any(string.IsNullOrEmpty))
            throw new ArgumentException("Node names must not be null or empty.", nameof(nodeNames));
        return new AdminPoolMembersBody(pool, nodeNames);
    }
}
