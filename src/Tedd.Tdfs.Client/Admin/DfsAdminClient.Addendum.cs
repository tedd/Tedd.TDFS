namespace Tedd.Tdfs;

// Addendum A administration: groups (ACL-01, ACL-08) and storage pools (POOL-01..POOL-05). Requires ClusterAdmin
// (POOL-017); every change is a control-group decision and is audited.
public sealed partial class DfsAdminClient
{
    // ---- Groups -----------------------------------------------------------------------------------------------

    /// <summary>Creates an empty group (<see cref="DfsAlreadyExistsException"/> when it exists).</summary>
    /// <exception cref="ArgumentException">The name is not a valid group name (<see cref="DfsIdentity.IsValidName"/>).</exception>
    public Task CreateGroupAsync(string group, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsAdminClient.CreateGroupAsync: implemented in a later phase");

    /// <summary>Deletes a group; ACEs naming it remain and match nobody (<see cref="DfsErrorCode.GroupNotFound"/> when absent).</summary>
    public Task DeleteGroupAsync(string group, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsAdminClient.DeleteGroupAsync: implemented in a later phase");

    /// <summary>Adds a principal to a group (idempotent); effective for operations authorized after the change commits (ACL-08).</summary>
    /// <exception cref="DfsException"><see cref="DfsErrorCode.GroupNotFound"/> or <see cref="DfsErrorCode.PrincipalNotFound"/>.</exception>
    public Task AddGroupMemberAsync(string group, string principal, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsAdminClient.AddGroupMemberAsync: implemented in a later phase");

    /// <summary>Removes a principal from a group (idempotent).</summary>
    /// <exception cref="DfsException"><see cref="DfsErrorCode.GroupNotFound"/>.</exception>
    public Task RemoveGroupMemberAsync(string group, string principal, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsAdminClient.RemoveGroupMemberAsync: implemented in a later phase");

    /// <summary>Lists groups (ordinal by name) with their members.</summary>
    public Task<IReadOnlyList<DfsGroupInfo>> ListGroupsAsync(CancellationToken ct = default) =>
        throw new NotImplementedException("DfsAdminClient.ListGroupsAsync: implemented in a later phase");

    // ---- Storage pools ----------------------------------------------------------------------------------------

    /// <summary>
    /// Creates a pool of existing live nodes (POOL-01). An empty node list, an unknown node, an invalid name
    /// (<see cref="DfsPoolNames"/>) or an existing name fails with a typed error.
    /// </summary>
    public Task CreatePoolAsync(string pool, IReadOnlyList<string> nodeNames, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsAdminClient.CreatePoolAsync: implemented in a later phase");

    /// <summary>
    /// Replaces a pool's members (non-empty). Advances the placement epoch; removed nodes drain the pool's objects by
    /// copy-before-delete (POOL-04). <see cref="DfsErrorCode.PoolNotFound"/> when absent.
    /// </summary>
    public Task SetPoolMembersAsync(string pool, IReadOnlyList<string> nodeNames, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsAdminClient.SetPoolMembersAsync: implemented in a later phase");

    /// <summary>
    /// Deletes a pool; <see cref="DfsErrorCode.PoolInUse"/> while a volume or policy references it,
    /// <see cref="DfsErrorCode.PoolNotFound"/> when absent; <c>default</c> cannot be deleted.
    /// </summary>
    public Task DeletePoolAsync(string pool, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsAdminClient.DeletePoolAsync: implemented in a later phase");

    /// <summary>Lists pools (ordinal by name, including <c>default</c>).</summary>
    public Task<IReadOnlyList<DfsPoolInfo>> ListPoolsAsync(CancellationToken ct = default) =>
        throw new NotImplementedException("DfsAdminClient.ListPoolsAsync: implemented in a later phase");

    /// <summary>Reads per-pool capacity, usage and replica health (POOL-05); <see cref="DfsErrorCode.PoolNotFound"/> when absent.</summary>
    public Task<DfsPoolStatus> GetPoolStatusAsync(string pool, CancellationToken ct = default) =>
        throw new NotImplementedException("DfsAdminClient.GetPoolStatusAsync: implemented in a later phase");
}
