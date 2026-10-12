using System.Globalization;
using System.Text;

namespace Tedd.Tdfs.Tests.Unit.Acl;

/// <summary>The ACL-05 table rows, as the reference decides them.</summary>
internal enum AclTableRow
{
    /// <summary>Open for read / ReadAll / live read: ReadData on the inode.</summary>
    Read,

    /// <summary>Open for write, truncate, WriteAll over existing, SetLength: WriteData on the inode.</summary>
    Write,

    /// <summary>Append mode / Append helper: AppendData (or WriteData) on the inode.</summary>
    Append,

    /// <summary>Create file: CreateFiles on the parent.</summary>
    CreateFile,

    /// <summary>Create directory: CreateDirectories on the parent.</summary>
    CreateDirectory,

    /// <summary>Delete file or directory: Delete on the inode OR DeleteSubdirectoriesAndFiles on the parent.</summary>
    Delete,

    /// <summary>Rename/move: delete rights on the source + create right on the destination parent (+ delete rights on an overwritten destination).</summary>
    Move,

    /// <summary>Enumerate: ListDirectory on the directory.</summary>
    Enumerate,

    /// <summary>GetInfo / Exists: ReadAttributes on the inode OR ListDirectory on its parent.</summary>
    GetInfo,

    /// <summary>GetAccessControl: ReadPermissions.</summary>
    GetAccessControl,

    /// <summary>SetAccessControl: ChangePermissions.</summary>
    SetAccessControl,

    /// <summary>SetOwner to the caller itself: TakeOwnership (or Administrators).</summary>
    SetOwnerSelf,

    /// <summary>SetOwner to another identity: Administrators only.</summary>
    SetOwnerOther,

    /// <summary>Copy: ReadData on the source + CreateFiles on the destination parent.</summary>
    Copy,
}

/// <summary>One entry of a reference ACL (explicit or inherited), as NTFS would store it after a completed propagation.</summary>
internal sealed record RefAce(
    DfsIdentity Who,
    DfsFileSystemRights Mask,
    bool IsDeny,
    DfsInheritanceFlags Inheritance,
    DfsPropagationFlags Propagation,
    bool IsInherited,
    string? Source)
{
    public bool ContainerInherit => (Inheritance & DfsInheritanceFlags.ContainerInherit) != 0;
    public bool ObjectInherit => (Inheritance & DfsInheritanceFlags.ObjectInherit) != 0;
    public bool NoPropagate => (Propagation & DfsPropagationFlags.NoPropagateInherit) != 0;
    public bool InheritOnly => (Propagation & DfsPropagationFlags.InheritOnly) != 0;

    /// <summary>Canonical text used to compare reference ACLs with descriptors returned by the cluster.</summary>
    public string Describe() => NtfsAclReference.DescribeEntry(IsInherited, IsDeny, Who, Mask, Inheritance, Propagation, IsInherited ? Source : null);
}

/// <summary>
/// An independent model of Addendum A permissions (ACL-01..ACL-05), written from the NTFS rules and deliberately not
/// sharing code or structure with <see cref="DfsAccessEvaluator"/>: it keeps a namespace tree with explicit descriptors,
/// materializes each object's ACL by NTFS-style <i>eager</i> top-down propagation (CREATOR OWNER resolved at every
/// object, as NTFS stores it), and decides access with an AccessCheck loop over allowed/denied masks. Groups,
/// administrators and owners are tracked here as the cluster's identity database would hold them.
/// </summary>
internal sealed class NtfsAclReference
{
    public const DfsFileSystemRights OwnerImplicit = DfsFileSystemRights.ReadPermissions | DfsFileSystemRights.ChangePermissions;
    public const DfsFileSystemRights AdminImplicit = DfsFileSystemRights.TakeOwnership | DfsFileSystemRights.ChangePermissions;

    private readonly Node _root;
    private readonly SortedDictionary<string, SortedSet<string>> _groups = new(StringComparer.Ordinal);
    private readonly SortedSet<string> _admins = new(StringComparer.Ordinal);
    private readonly SortedSet<string> _principals = new(StringComparer.Ordinal);

    /// <summary>Creates a volume root owned by <paramref name="rootOwner"/>; default ACL = Everyone FullControl (CI|OI) (ACL-06).</summary>
    public NtfsAclReference(DfsIdentity rootOwner, IReadOnlyList<DfsAccessRule>? rootRules = null, bool rootProtected = false)
    {
        _root = new Node("", isDirectory: true, rootOwner);
        _root.Explicit.AddRange(rootRules ??
        [
            new DfsAccessRule(DfsIdentity.Everyone, DfsFileSystemRights.FullControl, DfsAccessControlType.Allow,
                DfsInheritanceFlags.ContainerInherit | DfsInheritanceFlags.ObjectInherit),
        ]);
        _root.Protected = rootProtected;
    }

    // ------------------------------------------------------------------------------------------------------------
    // Identity database
    // ------------------------------------------------------------------------------------------------------------

    public void AddPrincipal(string name, bool isAdmin = false)
    {
        _principals.Add(name);
        if (isAdmin)
            _admins.Add(name);
    }

    public void AddGroup(string group, params string[] members)
    {
        if (!_groups.TryGetValue(group, out SortedSet<string>? set))
            _groups[group] = set = new SortedSet<string>(StringComparer.Ordinal);
        foreach (string m in members)
            set.Add(m);
    }

    public void SetMember(string group, string principal, bool member)
    {
        if (!_groups.TryGetValue(group, out SortedSet<string>? set))
            throw new InvalidOperationException($"reference: unknown group '{group}'");
        if (member)
            set.Add(principal);
        else
            set.Remove(principal);
    }

    public bool IsMember(string group, string principal) => _groups.TryGetValue(group, out SortedSet<string>? set) && set.Contains(principal);

    public bool IsAdmin(string principal) => _admins.Contains(principal);

    public IReadOnlyCollection<string> Principals => _principals;

    // ------------------------------------------------------------------------------------------------------------
    // Namespace mutations (mirror what the test does on the cluster)
    // ------------------------------------------------------------------------------------------------------------

    public void AddDirectory(string path, DfsIdentity owner) => Attach(path, new Node(LastName(path), isDirectory: true, owner));

    public void AddFile(string path, DfsIdentity owner) => Attach(path, new Node(LastName(path), isDirectory: false, owner));

    /// <summary>Replaces the explicit entries and the protection flag (inherited entries in the input are ignored).</summary>
    public void SetExplicit(string path, IEnumerable<DfsAccessRule> rules, bool isProtected)
    {
        Node n = Find(path);
        n.Explicit.Clear();
        n.Explicit.AddRange(rules.Where(r => !r.IsInherited));
        n.Protected = isProtected;
    }

    public void SetOwner(string path, DfsIdentity owner) => Find(path).Owner = owner;

    public void Remove(string path)
    {
        Node n = Find(path);
        n.Parent!.Children.Remove(n.Name);
        n.Parent = null;
    }

    /// <summary>Moves an inode (with its explicit descriptor) to <paramref name="to"/>, replacing an existing destination.</summary>
    public void Move(string from, string to)
    {
        Node n = Find(from);
        if (Exists(to))
            Remove(to);
        n.Parent!.Children.Remove(n.Name);
        Node newParent = Find(ParentOf(to));
        n.Name = LastName(to);
        n.Parent = newParent;
        newParent.Children.Add(n.Name, n);
    }

    /// <summary>Copies a file: the copy has no explicit entries, is not protected and is owned by the caller (ACL-05 Copy).</summary>
    public void Copy(string from, string to, DfsIdentity caller)
    {
        Node src = Find(from);
        if (src.IsDirectory)
            throw new InvalidOperationException("reference: copy of a directory");
        if (Exists(to))
            Remove(to);
        AddFile(to, caller);
    }

    // ------------------------------------------------------------------------------------------------------------
    // Queries
    // ------------------------------------------------------------------------------------------------------------

    public bool Exists(string path) => TryFind(FileOf(path), out _);

    public bool IsDirectory(string path) => Find(path).IsDirectory;

    public DfsIdentity OwnerOf(string path) => Find(path).Owner;

    public bool IsProtected(string path) => Find(path).Protected;

    public IReadOnlyList<DfsAccessRule> ExplicitOf(string path) => Find(path).Explicit.ToArray();

    /// <summary>Every path in the model (pre-order, ordinal), root first.</summary>
    public IReadOnlyList<string> AllPaths()
    {
        var list = new List<string>();
        void Walk(Node n)
        {
            list.Add(n.Path);
            foreach (Node c in n.Children.Values)
                Walk(c);
        }

        Walk(_root);
        return list;
    }

    /// <summary>The object's full ACL as NTFS stores it after propagation: explicit entries (Deny first) then inherited ones.</summary>
    public IReadOnlyList<RefAce> Acl(string path) => StoredAcl(Find(path));

    /// <summary>Canonical text of <see cref="Acl"/> (for comparison with <see cref="Describe(DfsSecurityDescriptor)"/>).</summary>
    public IReadOnlyList<string> DescribeAcl(string path) => Acl(path).Select(a => a.Describe()).ToArray();

    /// <summary>Every right the principal holds on the object (MAXIMUM_ALLOWED-style AccessCheck).</summary>
    public DfsFileSystemRights Granted(string path, string principal)
    {
        Node n = Find(path);
        DfsFileSystemRights allowed = 0, denied = 0;
        if (Holds(n.Owner, principal))
            allowed |= OwnerImplicit;
        if (_admins.Contains(principal))
            allowed |= AdminImplicit;
        foreach (RefAce ace in StoredAcl(n))
        {
            if (ace.InheritOnly || !Holds(ace.Who, principal))
                continue;
            if (ace.IsDeny)
                denied |= ace.Mask & ~allowed;
            else
                allowed |= ace.Mask & ~denied;
        }

        return allowed;
    }

    /// <summary>True when every requested right (ignoring Synchronize) is granted.</summary>
    public bool Can(string path, string principal, DfsFileSystemRights request)
    {
        DfsFileSystemRights wanted = request & ~DfsFileSystemRights.Synchronize;
        return (Granted(path, principal) & wanted) == wanted;
    }

    /// <summary>
    /// Whether ACL-05 allows <paramref name="row"/> for <paramref name="principal"/>. <paramref name="path"/> is the
    /// target (source for move/copy; a stream path stands for its file), <paramref name="path2"/> the destination.
    /// With <paramref name="enforceTraverse"/>, Traverse is required on every ancestor directory of every path used.
    /// </summary>
    public bool Allows(AclTableRow row, string principal, string path, string? path2 = null, bool enforceTraverse = false)
    {
        string p = FileOf(path);
        bool ok = row switch
        {
            AclTableRow.Read => Can(p, principal, DfsFileSystemRights.ReadData),
            AclTableRow.Write => Can(p, principal, DfsFileSystemRights.WriteData),
            AclTableRow.Append => Can(p, principal, DfsFileSystemRights.AppendData) || Can(p, principal, DfsFileSystemRights.WriteData),
            AclTableRow.CreateFile => Can(ParentOf(p), principal, DfsFileSystemRights.CreateFiles),
            AclTableRow.CreateDirectory => Can(ParentOf(p), principal, DfsFileSystemRights.CreateDirectories),
            AclTableRow.Delete => DeleteRights(p, principal),
            AclTableRow.Move => DeleteRights(p, principal)
                && Can(ParentOf(path2!), principal, IsDirectory(p) ? DfsFileSystemRights.CreateDirectories : DfsFileSystemRights.CreateFiles)
                && (!Exists(path2!) || DeleteRights(path2!, principal)),
            AclTableRow.Enumerate => Can(p, principal, DfsFileSystemRights.ListDirectory),
            AclTableRow.GetInfo => Can(p, principal, DfsFileSystemRights.ReadAttributes)
                || (p != "/" && Can(ParentOf(p), principal, DfsFileSystemRights.ListDirectory)),
            AclTableRow.GetAccessControl => Can(p, principal, DfsFileSystemRights.ReadPermissions),
            AclTableRow.SetAccessControl => Can(p, principal, DfsFileSystemRights.ChangePermissions),
            AclTableRow.SetOwnerSelf => Can(p, principal, DfsFileSystemRights.TakeOwnership) || _admins.Contains(principal),
            AclTableRow.SetOwnerOther => _admins.Contains(principal),
            AclTableRow.Copy => Can(p, principal, DfsFileSystemRights.ReadData) && Can(ParentOf(path2!), principal, DfsFileSystemRights.CreateFiles),
            _ => throw new ArgumentOutOfRangeException(nameof(row)),
        };
        if (!ok || !enforceTraverse)
            return ok;
        foreach (string target in path2 is null ? new[] { p } : new[] { p, FileOf(path2) })
        {
            string? ancestor = ParentOrNull(target);
            while (ancestor is not null)
            {
                if (!Can(ancestor, principal, DfsFileSystemRights.Traverse))
                    return false;
                ancestor = ParentOrNull(ancestor);
            }
        }

        return true;
    }

    private bool DeleteRights(string path, string principal) =>
        Can(path, principal, DfsFileSystemRights.Delete) || Can(ParentOf(path), principal, DfsFileSystemRights.DeleteSubdirectoriesAndFiles);

    // ------------------------------------------------------------------------------------------------------------
    // Text forms
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>Canonical text of every entry of a descriptor returned by the cluster.</summary>
    public static IReadOnlyList<string> Describe(DfsSecurityDescriptor descriptor) =>
        descriptor.Rules.Select(r => DescribeEntry(r.IsInherited, r.Type == DfsAccessControlType.Deny, r.Identity, r.Rights,
            r.Inheritance, r.Propagation, r.IsInherited ? r.InheritedFrom : null)).ToArray();

    /// <summary>Canonical text of one entry.</summary>
    public static string DescribeEntry(bool inherited, bool deny, DfsIdentity who, DfsFileSystemRights rights,
        DfsInheritanceFlags inheritance, DfsPropagationFlags propagation, string? from)
    {
        var sb = new StringBuilder();
        sb.Append(inherited ? "inherited " : "explicit ").Append(deny ? "Deny " : "Allow ").Append(who).Append(' ')
            .Append(DfsAccessRule.FormatRights(rights));
        sb.Append(" [").Append((inheritance & DfsInheritanceFlags.ContainerInherit) != 0 ? "CI" : "")
            .Append((inheritance & DfsInheritanceFlags.ObjectInherit) != 0 ? "OI" : "")
            .Append((propagation & DfsPropagationFlags.NoPropagateInherit) != 0 ? "NP" : "")
            .Append((propagation & DfsPropagationFlags.InheritOnly) != 0 ? "IO" : "").Append(']');
        if (inherited)
            sb.Append(" from ").Append(from ?? "<unknown>");
        return sb.ToString();
    }

    /// <summary>Formats rights for messages.</summary>
    public static string Rights(DfsFileSystemRights r) => DfsAccessRule.FormatRights(r) + " (0x" + ((int)r).ToString("X", CultureInfo.InvariantCulture) + ")";

    // ------------------------------------------------------------------------------------------------------------
    // Path helpers (own implementation; stream paths address their file)
    // ------------------------------------------------------------------------------------------------------------

    /// <summary>"/a/f:s" → "/a/f"; other paths unchanged.</summary>
    public static string FileOf(string path)
    {
        int slash = path.LastIndexOf('/');
        int colon = path.IndexOf(':', slash + 1);
        return colon < 0 ? path : path[..colon];
    }

    public static string ParentOf(string path)
    {
        string p = FileOf(path);
        if (p == "/")
            throw new InvalidOperationException("reference: the root has no parent");
        int slash = p.LastIndexOf('/');
        return slash == 0 ? "/" : p[..slash];
    }

    private static string? ParentOrNull(string path) => FileOf(path) == "/" ? null : ParentOf(path);

    private static string LastName(string path)
    {
        string p = FileOf(path);
        return p[(p.LastIndexOf('/') + 1)..];
    }

    // ------------------------------------------------------------------------------------------------------------
    // Internals
    // ------------------------------------------------------------------------------------------------------------

    private sealed class Node(string name, bool isDirectory, DfsIdentity owner)
    {
        public string Name { get; set; } = name;
        public bool IsDirectory { get; } = isDirectory;
        public DfsIdentity Owner { get; set; } = owner;
        public bool Protected { get; set; }
        public Node? Parent { get; set; }
        public List<DfsAccessRule> Explicit { get; } = [];
        public SortedDictionary<string, Node> Children { get; } = new(StringComparer.Ordinal);

        public string Path => Parent is null ? "/" : Parent.Parent is null ? "/" + Name : Parent.Path + "/" + Name;
    }

    private void Attach(string path, Node node)
    {
        Node parent = Find(ParentOf(path));
        if (!parent.IsDirectory)
            throw new InvalidOperationException($"reference: parent of {path} is a file");
        if (parent.Children.ContainsKey(node.Name))
            throw new InvalidOperationException($"reference: {path} already exists");
        node.Parent = parent;
        parent.Children.Add(node.Name, node);
    }

    private Node Find(string path) =>
        TryFind(FileOf(path), out Node? n) ? n! : throw new InvalidOperationException($"reference: {path} does not exist");

    private bool TryFind(string path, out Node? node)
    {
        node = _root;
        if (path == "/")
            return true;
        foreach (string part in path.Split('/', StringSplitOptions.RemoveEmptyEntries))
        {
            if (!node!.Children.TryGetValue(part, out node))
                return false;
        }

        return true;
    }

    /// <summary>Holds: does the principal hold this identity (owner matching uses the same rule)?</summary>
    private bool Holds(DfsIdentity id, string principal)
    {
        if (id.IsDefault)
            return false;
        if (id.Kind == DfsIdentityKind.WellKnown)
        {
            return id.Name switch
            {
                DfsIdentity.EveryoneName => true,
                DfsIdentity.AuthenticatedUsersName => true,
                DfsIdentity.AdministratorsName => _admins.Contains(principal),
                _ => false, // CreatorOwner never matches a caller
            };
        }

        return id.Kind == DfsIdentityKind.User
            ? string.Equals(id.Name, principal, StringComparison.Ordinal)
            : _groups.TryGetValue(id.Name, out SortedSet<string>? members) && members.Contains(principal);
    }

    // NTFS stores, for each object, its explicit ACEs (deny first) followed by what it inherited from its parent's
    // stored ACL at propagation time; CREATOR OWNER is replaced by the object's owner when the inherited ACE applies.
    private List<RefAce> StoredAcl(Node n)
    {
        var acl = new List<RefAce>();
        string path = n.Path;
        foreach (DfsAccessRule r in n.Explicit.Where(r => r.Type == DfsAccessControlType.Deny))
            acl.Add(new RefAce(r.Identity, r.Rights, true, r.Inheritance, r.Propagation, false, path));
        foreach (DfsAccessRule r in n.Explicit.Where(r => r.Type == DfsAccessControlType.Allow))
            acl.Add(new RefAce(r.Identity, r.Rights, false, r.Inheritance, r.Propagation, false, path));
        if (n.Parent is null || n.Protected)
            return acl;

        foreach (RefAce p in StoredAcl(n.Parent))
        {
            RefAce? child;
            if (n.IsDirectory)
            {
                if (p.ContainerInherit)
                {
                    child = p.NoPropagate
                        ? p with { Inheritance = DfsInheritanceFlags.None, Propagation = DfsPropagationFlags.None }
                        : p with { Propagation = DfsPropagationFlags.None };
                }
                else if (p.ObjectInherit && !p.NoPropagate)
                {
                    child = p with { Inheritance = DfsInheritanceFlags.ObjectInherit, Propagation = DfsPropagationFlags.InheritOnly };
                }
                else
                {
                    child = null;
                }
            }
            else
            {
                child = p.ObjectInherit ? p with { Inheritance = DfsInheritanceFlags.None, Propagation = DfsPropagationFlags.None } : null;
            }

            if (child is null)
                continue;
            child = child with { IsInherited = true };
            if (child.Who == DfsIdentity.CreatorOwner && !child.InheritOnly)
            {
                acl.Add(child with { Who = n.Owner, Inheritance = DfsInheritanceFlags.None, Propagation = DfsPropagationFlags.None });
                if (child.Inheritance != DfsInheritanceFlags.None)
                    acl.Add(child with { Propagation = DfsPropagationFlags.InheritOnly });
            }
            else
            {
                acl.Add(child);
            }
        }

        return acl;
    }
}
