using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Simulation.Acl;

/// <summary>A principal with its own connected client.</summary>
internal sealed record SimUser(string Name, DfsIdentity Identity, TestPrincipal Principal, DfsClient Client, DfsVolume Volume);

/// <summary>Shared helpers for the ACL deterministic-simulation scenarios.</summary>
internal static class AclHelpers
{
    public const string Vol = "vol";
    public const string AdminName = "admin";
    public const DfsInheritanceFlags CI = DfsInheritanceFlags.ContainerInherit;
    public const DfsInheritanceFlags OI = DfsInheritanceFlags.ObjectInherit;
    public const DfsInheritanceFlags CIOI = CI | OI;
    public const DfsPropagationFlags NP = DfsPropagationFlags.NoPropagateInherit;
    public const DfsPropagationFlags IO = DfsPropagationFlags.InheritOnly;
    public const DfsFileSystemRights FC = DfsFileSystemRights.FullControl;
    public const DfsFileSystemRights Read = DfsFileSystemRights.Read;
    public const DfsFileSystemRights Modify = DfsFileSystemRights.Modify;

    public static readonly DfsIdentity AdminId = DfsIdentity.User(AdminName);

    /// <summary>Bound for every wait in these scenarios (virtual time).</summary>
    public static readonly TimeSpan Wait = TimeSpan.FromSeconds(60);

    public static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Deterministic one-per-zone cluster (NodeTimings.Fast) with volume <see cref="Vol"/>.</summary>
    public static TestClusterOptions Options(int nodeCount = 3, int backupNodes = 0) => new()
    {
        NodeCount = nodeCount,
        ControlVoters = Math.Min(nodeCount, 5),
        Mode = ClusterMode.Deterministic,
        BackupNodeCount = backupNodes,
        Volumes = [new TestVolumeSpec(Vol) { Metadata = new DfsMetadataPolicy { Voters = Math.Min(nodeCount, 5) } }],
        ScenarioTimeout = TimeSpan.FromMinutes(10),
    };

    public static DfsIdentity U(string name) => DfsIdentity.User(name);

    public static DfsIdentity G(string name) => DfsIdentity.Group(name);

    public static DfsAccessRule Allow(DfsIdentity who, DfsFileSystemRights rights, DfsInheritanceFlags inheritance = DfsInheritanceFlags.None,
        DfsPropagationFlags propagation = DfsPropagationFlags.None) => new(who, rights, DfsAccessControlType.Allow, inheritance, propagation);

    public static DfsAccessRule Deny(DfsIdentity who, DfsFileSystemRights rights, DfsInheritanceFlags inheritance = DfsInheritanceFlags.None,
        DfsPropagationFlags propagation = DfsPropagationFlags.None) => new(who, rights, DfsAccessControlType.Deny, inheritance, propagation);

    /// <summary>Administrators FullControl (CI|OI): keeps the harness admin's access below protected test roots.</summary>
    public static DfsAccessRule AdminsFull => Allow(DfsIdentity.Administrators, FC, CIOI);

    public static async Task<DfsClient> ConnectAdminAsync(TestCluster cluster, string name = "acl-admin") =>
        await cluster.ConnectAsync(new TestClientOptions { Name = name }, Ct);

    public static async Task<SimUser> AddUserAsync(TestCluster cluster, string name, DfsPermissions grants, params string[] groups)
    {
        TestPrincipal p = await cluster.CreateUserAsync(name, grants, groups);
        DfsClient c = await cluster.ConnectAsync(new TestClientOptions { Name = name, Credentials = p.Credentials }, Ct);
        return new SimUser(name, U(name), p, c, c.GetVolume(Vol));
    }

    public static Task<SimUser> AddUserAsync(TestCluster cluster, string name, params string[] groups) =>
        AddUserAsync(cluster, name, DfsPermissions.Read | DfsPermissions.Write | DfsPermissions.Snapshot, groups);

    /// <summary>Sets explicit entries and protection; the owner in the descriptor is ignored by the server.</summary>
    public static Task<CommitToken> SetAclAsync(DfsVolume v, string path, bool isDirectory, bool isProtected, params DfsAccessRule[] rules)
    {
        var d = new DfsSecurityDescriptor(AdminId, rules, isProtected);
        return isDirectory ? v.Directories.SetAccessControlAsync(path, d, Ct) : v.Files.SetAccessControlAsync(path, d, Ct);
    }

    public static Task<DfsSecurityDescriptor> GetAclAsync(DfsVolume v, string path, bool isDirectory) =>
        isDirectory ? v.Directories.GetAccessControlAsync(path, Ct) : v.Files.GetAccessControlAsync(path, Ct);

    /// <summary>Canonical text of every entry (explicit/inherited, type, identity, rights, flags, source).</summary>
    public static IReadOnlyList<string> Describe(DfsSecurityDescriptor d) =>
        d.Rules.Select(r => $"{(r.IsInherited ? "inherited" : "explicit")} {r.Type} {r.Identity} {DfsAccessRule.FormatRights(r.Rights)} [{r.Inheritance}/{r.Propagation}]"
            + (r.IsInherited ? " from " + (r.InheritedFrom ?? "<unknown>") : "")).ToArray();

    /// <summary>Owner, protection and every entry as one comparable string.</summary>
    public static string Fingerprint(DfsSecurityDescriptor d) =>
        $"owner={d.Owner} protected={d.AreAccessRulesProtected}\n  " + string.Join("\n  ", Describe(d));

    /// <summary>Asserts an authorization failure (AccessDenied), not authentication.</summary>
    public static async Task<UnauthorizedAccessException> AssertDeniedAsync(Func<Task> action, string because)
    {
        Exception? caught = null;
        try
        {
            await action();
        }
        catch (Exception e)
        {
            caught = e;
        }

        if (caught is not UnauthorizedAccessException denied || caught is DfsAuthenticationException)
        {
            Assert.Fail($"expected UnauthorizedAccessException (AccessDenied) but got {(caught is null ? "success" : caught.GetType().Name + ": " + caught.Message)} — {because}");
            throw new InvalidOperationException("unreachable");
        }

        Assert.True(DfsErrors.ToCode(denied) == DfsErrorCode.AccessDenied, $"expected AccessDenied but got {DfsErrors.ToCode(denied)} — {because}");
        return denied;
    }

    /// <summary>Runs <paramref name="action"/>; true on success, false on AccessDenied; anything else fails the test.</summary>
    public static async Task<bool> AllowedAsync(Func<Task> action, string what)
    {
        try
        {
            await action();
            return true;
        }
        catch (UnauthorizedAccessException e) when (e is not DfsAuthenticationException && DfsErrors.ToCode(e) == DfsErrorCode.AccessDenied)
        {
            return false;
        }
        catch (Exception e)
        {
            Assert.Fail($"{what}: unexpected {e.GetType().Name}: {e.Message}");
            throw;
        }
    }

    public static async Task<List<T>> ToListAsync<T>(IAsyncEnumerable<T> source)
    {
        var list = new List<T>();
        await foreach (T item in source)
            list.Add(item);
        return list;
    }

    /// <summary>Awaits <paramref name="actual"/> and asserts byte equality.</summary>
    public static async Task AssertBytesAsync(byte[] expected, Task<byte[]> actual, string because)
    {
        byte[] got = await actual;
        AssertEx.BytesEqual(expected, got, because);
    }
}
