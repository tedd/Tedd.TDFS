using System.Collections.Immutable;

namespace Tedd.Tdfs.Server.Volumes.State;

/// <summary>Outcome of resolving a path in a namespace version.</summary>
internal enum ResolveStatus : byte
{
    /// <summary>The entry exists.</summary>
    Found = 0,

    /// <summary>The parent directory exists but has no entry with the name.</summary>
    Missing = 1,

    /// <summary>An ancestor directory does not exist.</summary>
    AncestorMissing = 2,

    /// <summary>An ancestor component is a file.</summary>
    AncestorNotDirectory = 3,
}

/// <summary>
/// Result of resolving a normalized file path (no stream suffix): the target (when found), its parent and the chain of
/// ancestor directories from the root down to the parent (used for ACL inheritance and traverse checks).
/// </summary>
/// <param name="Status">Resolution status.</param>
/// <param name="Path">The resolved path.</param>
/// <param name="Target">The entry (when <see cref="ResolveStatus.Found"/>).</param>
/// <param name="Parent">The parent directory (null for the root, and when an ancestor is missing).</param>
/// <param name="Name">Final component (empty for the root).</param>
/// <param name="Ancestors">Existing directories from the root down to (and including) the parent, as far as they resolved.</param>
internal sealed record Resolution(ResolveStatus Status, string Path, InodeRecord? Target, InodeRecord? Parent, string Name, IReadOnlyList<InodeRecord> Ancestors)
{
    /// <summary>True when the entry exists.</summary>
    public bool Found => Status == ResolveStatus.Found;

    /// <summary>True when the parent directory exists (the name may or may not).</summary>
    public bool ParentExists => Status is ResolveStatus.Found or ResolveStatus.Missing;

    /// <summary>The descriptor chain of the target, nearest first (target, parent, ..., root).</summary>
    public IReadOnlyList<InodeRecord> ChainNearestFirst()
    {
        var chain = new List<InodeRecord>(Ancestors.Count + 1);
        if (Target is not null)
            chain.Add(Target);
        for (int i = Ancestors.Count - 1; i >= 0; i--)
            chain.Add(Ancestors[i]);
        return chain;
    }

    /// <summary>The descriptor chain of the parent directory, nearest first (parent, ..., root).</summary>
    public IReadOnlyList<InodeRecord> ParentChainNearestFirst()
    {
        var chain = new List<InodeRecord>(Ancestors.Count);
        for (int i = Ancestors.Count - 1; i >= 0; i--)
            chain.Add(Ancestors[i]);
        return chain;
    }
}

/// <summary>Path resolution over one namespace version (an inode map); pure and deterministic.</summary>
internal readonly struct NamespaceView(ImmutableSortedDictionary<ulong, InodeRecord> inodes)
{
    /// <summary>The inode map.</summary>
    public ImmutableSortedDictionary<ulong, InodeRecord> Inodes { get; } = inodes;

    /// <summary>The inode with <paramref name="id"/>, or null.</summary>
    public InodeRecord? Get(ulong id) => Inodes.GetValueOrDefault(id);

    /// <summary>Resolves a normalized path without a stream suffix.</summary>
    public Resolution Resolve(string normalizedFilePath)
    {
        InodeRecord root = Inodes[InodeRecord.RootId];
        if (DfsPath.IsRoot(normalizedFilePath))
            return new Resolution(ResolveStatus.Found, normalizedFilePath, root, null, string.Empty, []);
        IReadOnlyList<string> parts = DfsPath.Split(normalizedFilePath);
        var ancestors = new List<InodeRecord>(parts.Count) { root };
        InodeRecord current = root;
        for (int i = 0; i < parts.Count - 1; i++)
        {
            if (!current.Children.TryGetValue(parts[i], out ulong childId) || Get(childId) is not { } child)
                return new Resolution(ResolveStatus.AncestorMissing, normalizedFilePath, null, null, parts[^1], ancestors);
            if (!child.IsDirectory)
                return new Resolution(ResolveStatus.AncestorNotDirectory, normalizedFilePath, null, null, parts[^1], ancestors);
            ancestors.Add(child);
            current = child;
        }

        string name = parts[^1];
        if (current.Children.TryGetValue(name, out ulong id) && Get(id) is { } target)
            return new Resolution(ResolveStatus.Found, normalizedFilePath, target, current, name, ancestors);
        return new Resolution(ResolveStatus.Missing, normalizedFilePath, null, current, name, ancestors);
    }

    /// <summary>
    /// The current path of an attached inode (walking parents to the root), or null when it is not attached to this
    /// namespace version (detached, or missing).
    /// </summary>
    public string? PathOf(ulong id)
    {
        if (id == InodeRecord.RootId)
            return DfsPath.Root;
        var names = new List<string>();
        ulong current = id;
        int guard = 0;
        while (current != InodeRecord.RootId)
        {
            if (Get(current) is not { } node || node.ParentId == 0 || Get(node.ParentId) is not { } parent
                || !parent.Children.TryGetValue(node.Name, out ulong listed) || listed != current || ++guard > 4096)
            {
                return null;
            }

            names.Add(node.Name);
            current = node.ParentId;
        }

        names.Reverse();
        return "/" + string.Join('/', names);
    }

    /// <summary>The ancestor chain of an attached inode, nearest first (the inode itself first); null when detached.</summary>
    public IReadOnlyList<InodeRecord>? ChainOf(ulong id)
    {
        var chain = new List<InodeRecord>();
        ulong current = id;
        int guard = 0;
        while (true)
        {
            if (Get(current) is not { } node || ++guard > 4096)
                return null;
            chain.Add(node);
            if (current == InodeRecord.RootId)
                return chain;
            if (node.ParentId == 0 || Get(node.ParentId) is not { } parent || !parent.Children.TryGetValue(node.Name, out ulong listed) || listed != current)
                return null;
            current = node.ParentId;
        }
    }

    /// <summary>True when <paramref name="ancestor"/> is <paramref name="id"/> or one of its ancestors.</summary>
    public bool IsAncestorOrSelf(ulong ancestor, ulong id)
    {
        ulong current = id;
        int guard = 0;
        while (current != 0 && ++guard <= 4096)
        {
            if (current == ancestor)
                return true;
            if (Get(current) is not { } node)
                return false;
            current = node.ParentId;
        }

        return false;
    }

    /// <summary>Every inode of the subtree rooted at <paramref name="id"/> (pre-order, deterministic).</summary>
    public List<InodeRecord> Subtree(ulong id)
    {
        var result = new List<InodeRecord>();
        var stack = new Stack<ulong>();
        stack.Push(id);
        while (stack.Count > 0)
        {
            if (Get(stack.Pop()) is not { } node)
                continue;
            result.Add(node);
            if (!node.IsDirectory)
                continue;
            foreach (ulong child in node.Children.Values.Reverse())
                stack.Push(child);
        }

        return result;
    }
}
