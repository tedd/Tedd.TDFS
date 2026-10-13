namespace Tedd.Tdfs.Testing;

// Addendum A (API.md §H.3): principals and groups for ACL scenarios.
public sealed partial class TestCluster
{
    /// <summary>
    /// Creates a group (via the admin client) and adds <paramref name="members"/> (existing principals) to it. The change
    /// is committed in the control group before the task completes.
    /// </summary>
    public Task CreateGroupAsync(string group, params string[] members) => CreateGroupCoreAsync(group, members);

    /// <summary>
    /// Creates a non-admin principal <c>user:<paramref name="name"/></c> with <paramref name="volumeGrants"/> on every
    /// test volume and adds it to each of <paramref name="groups"/> (which must exist). Returns its credentials.
    /// </summary>
    public Task<TestPrincipal> CreateUserAsync(string name, DfsPermissions volumeGrants = DfsPermissions.Read | DfsPermissions.Write, params string[] groups) =>
        CreateUserCoreAsync(name, volumeGrants, groups);
}
