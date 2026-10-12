using System.Security.Cryptography;
using System.Text;
using Tedd.Tdfs.Server;
using Tedd.Tdfs.Testing;

namespace Tedd.Tdfs.Tests.Unit.Acl;

/// <summary>Builders, assertions and cluster options shared by the ACL unit tests.</summary>
internal static class AclHelpers
{
    public const string Vol = "vol";
    public const string AdminName = "admin";

    public const DfsInheritanceFlags None = DfsInheritanceFlags.None;
    public const DfsInheritanceFlags CI = DfsInheritanceFlags.ContainerInherit;
    public const DfsInheritanceFlags OI = DfsInheritanceFlags.ObjectInherit;
    public const DfsInheritanceFlags CIOI = DfsInheritanceFlags.ContainerInherit | DfsInheritanceFlags.ObjectInherit;
    public const DfsPropagationFlags NP = DfsPropagationFlags.NoPropagateInherit;
    public const DfsPropagationFlags IO = DfsPropagationFlags.InheritOnly;
    public const DfsFileSystemRights FC = DfsFileSystemRights.FullControl;

    /// <summary>The harness admin principal (ClusterAdmin; creator of the test volumes).</summary>
    public static readonly DfsIdentity AdminId = DfsIdentity.User(AdminName);

    public static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Single-node in-memory cluster with volume <see cref="Vol"/> (plus <paramref name="extraVolumes"/>).</summary>
    public static TestClusterOptions Options(params TestVolumeSpec[] extraVolumes) => TestClusterOptions.SingleNode with
    {
        Volumes = [new TestVolumeSpec(Vol) { Metadata = new DfsMetadataPolicy { Voters = 1 } }, .. extraVolumes],
    };

    /// <summary>A single-voter volume spec for <see cref="Options"/>.</summary>
    public static TestVolumeSpec Volume(string name) => new(name) { Metadata = new DfsMetadataPolicy { Voters = 1 } };

    public static DfsIdentity U(string name) => DfsIdentity.User(name);

    public static DfsIdentity G(string name) => DfsIdentity.Group(name);

    public static DfsAccessRule Allow(DfsIdentity who, DfsFileSystemRights rights, DfsInheritanceFlags inheritance = None,
        DfsPropagationFlags propagation = DfsPropagationFlags.None) =>
        new(who, rights, DfsAccessControlType.Allow, inheritance, propagation);

    public static DfsAccessRule Deny(DfsIdentity who, DfsFileSystemRights rights, DfsInheritanceFlags inheritance = None,
        DfsPropagationFlags propagation = DfsPropagationFlags.None) =>
        new(who, rights, DfsAccessControlType.Deny, inheritance, propagation);

    /// <summary>Administrators: FullControl (CI|OI) — put on every protected test root so the harness admin keeps access.</summary>
    public static DfsAccessRule AdminsFull => Allow(DfsIdentity.Administrators, FC, CIOI);

    /// <summary>Asserts an authenticated principal was refused by authorization (AccessDenied, not authentication).</summary>
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

        if (caught is not UnauthorizedAccessException denied)
        {
            Assert.Fail($"expected UnauthorizedAccessException (AccessDenied) but got {(caught is null ? "success" : caught.GetType().Name + ": " + caught.Message)} — {because}");
            throw new InvalidOperationException("unreachable");
        }

        Assert.False(denied is DfsAuthenticationException, $"authorization failure reported as authentication failure — {because}");
        Assert.True(DfsErrors.ToCode(denied) == DfsErrorCode.AccessDenied, $"expected AccessDenied but got {DfsErrors.ToCode(denied)} — {because}");
        return denied;
    }

    /// <summary>Consumes an enumeration completely.</summary>
    public static async Task<List<T>> ToListAsync<T>(IAsyncEnumerable<T> source)
    {
        var list = new List<T>();
        await foreach (T item in source)
            list.Add(item);
        return list;
    }

    public static string Hash(ReadOnlySpan<byte> data) => Convert.ToHexString(SHA256.HashData(data));

    /// <summary>Awaits <paramref name="actual"/> and asserts byte equality.</summary>
    public static async Task AssertBytesAsync(byte[] expected, Task<byte[]> actual, string because)
    {
        byte[] got = await actual;
        AssertEx.BytesEqual(expected, got, because);
    }

    /// <summary>
    /// A digest of a subtree as seen by <paramref name="admin"/>: kind, inode, revision, length, content hash, every named
    /// stream (length, revision, content hash) and the stored descriptor (owner, protection, explicit entries) of every
    /// node. Equal digests mean namespace, data and descriptors are unchanged.
    /// </summary>
    public static async Task<string> DigestAsync(DfsVolume admin, string root)
    {
        var sb = new StringBuilder();
        bool rootIsDir = await admin.Directories.ExistsAsync(root, Ct);
        if (!rootIsDir && !await admin.Files.ExistsAsync(root, Ct))
            return "<absent " + root + ">";
        await VisitAsync(root, rootIsDir);
        return sb.ToString();

        async Task VisitAsync(string path, bool isDirectory)
        {
            DfsSecurityDescriptor acl;
            if (isDirectory)
            {
                DfsDirectoryInfo d = await admin.Directories.GetInfoAsync(path, Ct);
                sb.Append("D ").Append(path).Append(" inode=").Append(d.InodeId.Value).Append(" rev=").Append(d.Revision).Append('\n');
                acl = await admin.Directories.GetAccessControlAsync(path, Ct);
            }
            else
            {
                DfsFileInfo f = await admin.Files.GetInfoAsync(path, Ct);
                byte[] bytes = await admin.Files.ReadAllBytesAsync(path, Ct);
                sb.Append("F ").Append(path).Append(" inode=").Append(f.InodeId.Value).Append(" rev=").Append(f.Revision)
                    .Append(" len=").Append(f.Length).Append(" sha=").Append(Hash(bytes)).Append('\n');
                foreach (DfsStreamInfo s in await admin.Files.ListStreamsAsync(path, Ct))
                {
                    byte[] sbytes = await admin.Files.ReadAllBytesAsync(path + ":" + s.Name, Ct);
                    sb.Append("  S ").Append(s.Name).Append(" len=").Append(s.Length).Append(" rev=").Append(s.Revision)
                        .Append(" sha=").Append(Hash(sbytes)).Append('\n');
                }

                acl = await admin.Files.GetAccessControlAsync(path, Ct);
            }

            sb.Append("  owner=").Append(acl.Owner).Append(" protected=").Append(acl.AreAccessRulesProtected).Append('\n');
            foreach (string e in NtfsAclReference.Describe(acl.WithoutInheritedRules()))
                sb.Append("  ").Append(e).Append('\n');
            if (!isDirectory)
                return;
            List<DfsEntryInfo> entries = await ToListAsync(admin.Directories.EnumerateAsync(path, null, Ct));
            foreach (DfsEntryInfo e in entries.OrderBy(x => x.Name, StringComparer.Ordinal))
                await VisitAsync(e.Path, e.Kind == DfsEntryKind.Directory);
        }
    }

    /// <summary>Asserts no unexpired handle is held by a session of <paramref name="principal"/> (no partial handle was created).</summary>
    public static async Task AssertNoHandlesAsync(TestCluster cluster, string volume, string principal, string because)
    {
        VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(volume);
        var sessions = state.Sessions.Where(s => s.Principal == principal).Select(s => s.Id).ToHashSet();
        HandleInspection[] handles = state.Handles.Where(h => sessions.Contains(h.Session) && !h.Expired).ToArray();
        Assert.True(handles.Length == 0, $"{handles.Length} handle(s) of {principal} exist ({string.Join(", ", handles.Select(h => $"{h.Id.Value}:{h.Access}"))}) — {because}");
    }

    /// <summary>Unexpired handles held by sessions of <paramref name="principal"/> on <paramref name="inode"/>.</summary>
    public static async Task<HandleInspection[]> HandlesOfAsync(TestCluster cluster, string volume, string principal, InodeId inode)
    {
        VolumeStateInspection state = await cluster.Inspect.GetVolumeStateAsync(volume);
        var sessions = state.Sessions.Where(s => s.Principal == principal).Select(s => s.Id).ToHashSet();
        return state.Handles.Where(h => sessions.Contains(h.Session) && !h.Expired && h.Inode == inode).ToArray();
    }
}

/// <summary>A test principal with its own connected client.</summary>
internal sealed record AclUser(string Name, DfsIdentity Identity, TestPrincipal Principal, DfsClient Client, DfsVolume Volume);

/// <summary>
/// A cluster volume and an <see cref="NtfsAclReference"/> kept in lockstep: every namespace, descriptor, owner and
/// group change the test performs through this object is applied to both, so cluster decisions can be cross-checked
/// against the independent reference.
/// </summary>
internal sealed class AclWorld : IAsyncDisposable
{
    private readonly List<DfsClient> _clients = [];
    private readonly Dictionary<string, AclUser> _users = new(StringComparer.Ordinal);

    private AclWorld(TestCluster cluster, DfsClient adminClient, string volume, NtfsAclReference reference)
    {
        Cluster = cluster;
        AdminClient = adminClient;
        VolumeName = volume;
        Admin = adminClient.GetVolume(volume);
        Ref = reference;
        _clients.Add(adminClient);
    }

    public TestCluster Cluster { get; }

    public string VolumeName { get; }

    public DfsClient AdminClient { get; }

    /// <summary>The volume as the harness admin (ClusterAdmin, creator of the volume) sees it.</summary>
    public DfsVolume Admin { get; }

    public NtfsAclReference Ref { get; }

    public AclUser this[string name] => _users[name];

    private static CancellationToken Ct => AclHelpers.Ct;

    /// <summary>Connects the admin client; the reference root is <paramref name="rootSecurity"/> or DefaultRoot(user:admin).</summary>
    public static async Task<AclWorld> CreateAsync(TestCluster cluster, string volume = AclHelpers.Vol, DfsSecurityDescriptor? rootSecurity = null)
    {
        DfsClient admin = await cluster.ConnectAsync(new TestClientOptions { Name = "acl-admin-" + volume }, Ct);
        NtfsAclReference reference = rootSecurity is null
            ? new NtfsAclReference(AclHelpers.AdminId)
            : new NtfsAclReference(rootSecurity.Owner, rootSecurity.ExplicitRules, rootSecurity.AreAccessRulesProtected);
        reference.AddPrincipal(AclHelpers.AdminName, isAdmin: true);
        return new AclWorld(cluster, admin, volume, reference);
    }

    // ---- identities -------------------------------------------------------------------------------------------

    public async Task CreateGroupAsync(string group, params string[] members)
    {
        await Cluster.CreateGroupAsync(group, members);
        Ref.AddGroup(group, members);
    }

    public Task<AclUser> AddUserAsync(string name, params string[] groups) =>
        AddUserAsync(name, DfsPermissions.Read | DfsPermissions.Write, groups);

    public async Task<AclUser> AddUserAsync(string name, DfsPermissions grants, params string[] groups)
    {
        TestPrincipal p = await Cluster.CreateUserAsync(name, grants, groups);
        Ref.AddPrincipal(name);
        foreach (string g in groups)
            Ref.SetMember(g, name, member: true);
        return await ConnectAsync(name, p);
    }

    /// <summary>A ClusterAdmin principal (member of Administrators) with all grants on every test volume.</summary>
    public async Task<AclUser> AddAdminUserAsync(string name, params string[] groups)
    {
        var grants = Cluster.Options.Volumes.ToDictionary(v => v.Name, _ => DfsPermissions.All, StringComparer.Ordinal);
        TestPrincipal p = await Cluster.CreatePrincipalAsync(name, grants, clusterAdmin: true);
        Ref.AddPrincipal(name, isAdmin: true);
        foreach (string g in groups)
        {
            await Cluster.Admin.AddGroupMemberAsync(g, name, Ct);
            Ref.SetMember(g, name, member: true);
        }

        return await ConnectAsync(name, p);
    }

    public async Task SetMembershipAsync(string group, string principal, bool member)
    {
        if (member)
            await Cluster.Admin.AddGroupMemberAsync(group, principal, Ct);
        else
            await Cluster.Admin.RemoveGroupMemberAsync(group, principal, Ct);
        Ref.SetMember(group, principal, member);
    }

    private async Task<AclUser> ConnectAsync(string name, TestPrincipal principal)
    {
        DfsClient client = await Cluster.ConnectAsync(new TestClientOptions { Name = name, Credentials = principal.Credentials }, Ct);
        _clients.Add(client);
        var user = new AclUser(name, DfsIdentity.User(name), principal, client, client.GetVolume(VolumeName));
        _users[name] = user;
        return user;
    }

    // ---- namespace (as admin) ---------------------------------------------------------------------------------

    public async Task MkdirAsync(string path, DfsIdentity? owner = null)
    {
        await Admin.Directories.CreateDirectoryAsync(path, Ct);
        Ref.AddDirectory(path, AclHelpers.AdminId);
        if (owner is { } o)
            await SetOwnerAsync(path, o);
    }

    public async Task<byte[]> MkfileAsync(string path, byte[] content, DfsIdentity? owner = null)
    {
        await Admin.Files.WriteAllBytesAsync(path, content, Ct);
        Ref.AddFile(path, AclHelpers.AdminId);
        if (owner is { } o)
            await SetOwnerAsync(path, o);
        return content;
    }

    /// <summary>Replaces the explicit entries and protection flag of <paramref name="path"/> (as admin).</summary>
    public async Task SetAclAsync(string path, bool isProtected, params DfsAccessRule[] rules)
    {
        var descriptor = new DfsSecurityDescriptor(Ref.OwnerOf(path), rules, isProtected);
        await SetAclAsAsync(Admin, path, descriptor);
        Ref.SetExplicit(path, rules, isProtected);
    }

    /// <summary>Protects <paramref name="path"/> with Administrators FullControl (CI|OI) plus <paramref name="rules"/>.</summary>
    public Task ProtectAsync(string path, params DfsAccessRule[] rules) => SetAclAsync(path, true, [AclHelpers.AdminsFull, .. rules]);

    public async Task SetOwnerAsync(string path, DfsIdentity owner)
    {
        await SetOwnerAsAsync(Admin, path, owner);
        Ref.SetOwner(path, owner);
    }

    public bool IsDirectory(string path) => Ref.IsDirectory(path);

    // ---- per-kind facade calls --------------------------------------------------------------------------------

    public Task<DfsSecurityDescriptor> GetAclAsAsync(DfsVolume v, string path) =>
        IsDirectory(path) ? v.Directories.GetAccessControlAsync(path, Ct) : v.Files.GetAccessControlAsync(path, Ct);

    public Task<CommitToken> SetAclAsAsync(DfsVolume v, string path, DfsSecurityDescriptor descriptor) =>
        IsDirectory(path) ? v.Directories.SetAccessControlAsync(path, descriptor, Ct) : v.Files.SetAccessControlAsync(path, descriptor, Ct);

    public Task<CommitToken> SetOwnerAsAsync(DfsVolume v, string path, DfsIdentity owner) =>
        IsDirectory(path) ? v.Directories.SetOwnerAsync(path, owner, Ct) : v.Files.SetOwnerAsync(path, owner, Ct);

    public Task<DfsFileSystemRights> RightsAsAsync(DfsVolume v, string path, DfsIdentity? who) =>
        IsDirectory(path) ? v.Directories.GetEffectiveRightsAsync(path, who, Ct) : v.Files.GetEffectiveRightsAsync(path, who, Ct);

    // ---- cross-checks against the reference -------------------------------------------------------------------

    /// <summary>GetAccessControl (as admin) equals the reference ACL: owner, protection, every entry in order.</summary>
    public async Task AssertDescriptorMatchesAsync(string path)
    {
        DfsSecurityDescriptor actual = await GetAclAsAsync(Admin, path);
        Assert.True(Ref.OwnerOf(path) == actual.Owner, $"owner of {path}: cluster {actual.Owner}, reference {Ref.OwnerOf(path)}");
        Assert.True(Ref.IsProtected(path) == actual.AreAccessRulesProtected, $"protection of {path}: cluster {actual.AreAccessRulesProtected}, reference {Ref.IsProtected(path)}");
        IReadOnlyList<string> expected = Ref.DescribeAcl(path);
        IReadOnlyList<string> got = NtfsAclReference.Describe(actual);
        Assert.True(expected.SequenceEqual(got, StringComparer.Ordinal),
            $"ACL of {path} differs from the reference.\nreference:\n  {string.Join("\n  ", expected)}\ncluster:\n  {string.Join("\n  ", got)}");
    }

    /// <summary>GetEffectiveRights(path, user:principal) (as admin) equals the reference's granted rights.</summary>
    public async Task AssertRightsMatchAsync(string path, string principal)
    {
        DfsFileSystemRights actual = await RightsAsAsync(Admin, path, DfsIdentity.User(principal));
        DfsFileSystemRights expected = Ref.Granted(path, principal);
        Assert.True(expected == actual,
            $"GetEffectiveRights({path}, user:{principal}) = {NtfsAclReference.Rights(actual)}, reference {NtfsAclReference.Rights(expected)}.\nreference ACL:\n  {string.Join("\n  ", Ref.DescribeAcl(path))}");
    }

    /// <summary>Descriptor and per-principal effective rights of every path equal the reference.</summary>
    public async Task CrossCheckAsync(IEnumerable<string> paths, IEnumerable<string> principals)
    {
        string[] who = principals.ToArray();
        foreach (string path in paths)
        {
            await AssertDescriptorMatchesAsync(path);
            foreach (string p in who)
                await AssertRightsMatchAsync(path, p);
        }
    }

    /// <summary>
    /// Performs the read decision the reference predicts: ReadAllBytes for a file (bytes must equal
    /// <paramref name="expected"/>), enumeration for a directory; success iff the reference allows it, otherwise
    /// UnauthorizedAccessException.
    /// </summary>
    public async Task AssertReadDecisionAsync(string user, string path, byte[]? expected = null)
    {
        DfsVolume v = _users[user].Volume;
        bool dir = IsDirectory(path);
        bool allowed = Ref.Allows(dir ? AclTableRow.Enumerate : AclTableRow.Read, user, path);
        string because = $"{user} {(dir ? "enumerating" : "reading")} {path}; reference rights {NtfsAclReference.Rights(Ref.Granted(path, user))}";
        if (!allowed)
        {
            Func<Task> attempt = dir
                ? () => AclHelpers.ToListAsync(v.Directories.EnumerateAsync(path, null, Ct))
                : () => v.Files.ReadAllBytesAsync(path, Ct);
            await AclHelpers.AssertDeniedAsync(attempt, because);
            return;
        }

        if (dir)
        {
            List<DfsEntryInfo> entries = await AclHelpers.ToListAsync(v.Directories.EnumerateAsync(path, null, Ct));
            List<DfsEntryInfo> adminEntries = await AclHelpers.ToListAsync(Admin.Directories.EnumerateAsync(path, null, Ct));
            Assert.Equal(adminEntries.Select(e => e.Name).Order(StringComparer.Ordinal), entries.Select(e => e.Name).Order(StringComparer.Ordinal));
        }
        else
        {
            byte[] bytes = await v.Files.ReadAllBytesAsync(path, Ct);
            if (expected is not null)
                AssertEx.BytesEqual(expected, bytes, because);
        }
    }

    public async ValueTask DisposeAsync()
    {
        foreach (DfsClient c in Enumerable.Reverse(_clients))
            await c.DisposeAsync();
        _clients.Clear();
    }
}
