using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Volumes.Commands;
using Tedd.Tdfs.Server.Volumes.State;

namespace Tedd.Tdfs.Server.Volumes.Apply;

internal sealed partial class VolumeStateMachine
{
    /// <summary>Limit detail of an error reply.</summary>
    internal static ErrorDetailData LimitDetail(string name, long limit, long requested) => new(LimitName: name, Limit: limit, Requested: requested);

    private static (VolumeState, VolumeApplyResult) ApplyCreateDirectory(VolumeState s, ApplyContext ctx, CreateDirectoryCommand c)
    {
        RequireGrant(ctx, DfsPermissions.Write, "create directory");
        string path = NormalizePath(c.DirectoryPath);
        if (DfsStreamPath.HasStream(path))
            throw Fail(DfsErrorCode.InvalidPath, $"'{path}' addresses a stream; directories have no stream syntax.");
        var view = new NamespaceView(s.Inodes);
        Resolution res = view.Resolve(path);
        if (res.Found)
        {
            RequireTraverse(s, ctx, res);
            if (!res.Target!.IsDirectory)
                throw Fail(DfsErrorCode.AlreadyExists, $"A file exists at '{path}'.");
            (VolumeState snapState, SnapshotInfoData? existingSnap) = SnapshotInSameEntry(s, ctx, c.SnapshotName);
            return (snapState, VolumeApplyResult.Ok(new CreateDirectoryResultMessage(Describe(snapState, res.Target, path, null), false, s.TokenAt(ctx.Index),
                existingSnap)));
        }

        if (!res.ParentExists && !c.Recursive)
            throw Fail(DfsErrorCode.DirectoryNotFound, $"The parent of '{path}' does not exist.");
        (s, InodeRecord current) = EnsureDirectoryPath(s, ctx, path);
        (s, SnapshotInfoData? snapshot) = SnapshotInSameEntry(s, ctx, c.SnapshotName);
        return (s, VolumeApplyResult.Ok(new CreateDirectoryResultMessage(Describe(s, current, path, null), true, s.TokenAt(ctx.Index), snapshot)));
    }

    /// <summary>
    /// Creates every missing directory of <paramref name="path"/> (recursive create), checking CreateDirectories on each
    /// parent and Traverse when enforced; existing directories are kept, a file in the way fails with AlreadyExists.
    /// </summary>
    internal static (VolumeState State, InodeRecord Directory) EnsureDirectoryPath(VolumeState s, ApplyContext ctx, string path)
    {
        InodeRecord current = s.Root;
        var ancestors = new List<InodeRecord> { current };
        string currentPath = DfsPath.Root;
        foreach (string name in DfsPath.Split(path))
        {
            currentPath = DfsPath.Combine(currentPath, name);
            if (current.Children.TryGetValue(name, out ulong childId) && s.Find(childId) is { } child)
            {
                if (!child.IsDirectory)
                    throw Fail(DfsErrorCode.AlreadyExists, $"A file exists at '{currentPath}'.");
                ancestors.Add(child);
                current = child;
                continue;
            }

            if (s.Config.EnforceTraverseChecking)
                RequireTraverse(s, ctx, new Resolution(ResolveStatus.Missing, currentPath, null, current, name, ancestors.ToArray()));
            Require(ctx, Enumerable.Reverse(ancestors).ToArray(), DfsFileSystemRights.CreateDirectories, $"create directory '{currentPath}'");
            (s, ulong id) = AllocateInode(s);
            InodeRecord created = NewInode(s, ctx, id, InodeKind.Directory, current, name, current.PolicyName);
            s = Link(s, ctx, current, created);
            created = s.Inodes[created.Id];
            ancestors[^1] = s.Inodes[current.Id];
            ancestors.Add(created);
            current = created;
        }

        return (s, current);
    }

    private static (VolumeState, VolumeApplyResult) ApplyMove(VolumeState s, ApplyContext ctx, MoveCommand c)
    {
        RequireGrant(ctx, DfsPermissions.Write, "move");
        string source = NormalizePath(c.Source);
        string destination = NormalizePath(c.Destination);
        if (DfsStreamPath.HasStream(source) || DfsStreamPath.HasStream(destination))
            throw Fail(DfsErrorCode.InvalidPath, "Streams cannot be renamed; move the file instead.");
        if (DfsPath.IsRoot(source))
            throw Fail(DfsErrorCode.InvalidArgument, "The root directory cannot be moved.");
        if (DfsPath.IsRoot(destination))
            throw Fail(DfsErrorCode.InvalidArgument, "Nothing can be moved onto the root directory.");

        var view = new NamespaceView(s.Inodes);
        Resolution src = view.Resolve(source);
        RequireTraverse(s, ctx, src);
        if (!src.Found)
            throw Fail(c.ExpectDirectory ? DfsErrorCode.DirectoryNotFound : DfsErrorCode.FileNotFound, $"'{source}' does not exist.");
        InodeRecord node = src.Target!;
        if (node.IsDirectory != c.ExpectDirectory)
        {
            throw Fail(c.ExpectDirectory ? DfsErrorCode.DirectoryNotFound : DfsErrorCode.FileNotFound,
                c.ExpectDirectory ? $"'{source}' is not a directory." : $"'{source}' is a directory.");
        }

        Resolution dst = view.Resolve(destination);
        RequireTraverse(s, ctx, dst);
        if (dst.Found && dst.Target!.Id == node.Id)
        {
            // Renaming an entry onto itself (identical normalized name): nothing changes.
            (VolumeState same, SnapshotInfoData? sameSnap) = SnapshotInSameEntry(s, ctx, c.SnapshotName);
            return (same, VolumeApplyResult.Ok(new MoveResultMessage(new InodeId(node.Id), node.Revision, s.TokenAt(ctx.Index), sameSnap)));
        }

        if (!dst.ParentExists)
            throw Fail(DfsErrorCode.DirectoryNotFound, $"The destination parent of '{destination}' does not exist.");
        InodeRecord dstParent = dst.Parent!;
        if (node.IsDirectory && view.IsAncestorOrSelf(node.Id, dstParent.Id))
            throw Fail(DfsErrorCode.InvalidArgument, $"'{source}' cannot be moved into its own subtree.");

        InodeRecord? replaced = null;
        if (dst.Found)
        {
            InodeRecord existing = dst.Target!;
            if (!c.Overwrite)
                throw Fail(DfsErrorCode.AlreadyExists, $"'{destination}' already exists.");
            if (existing.IsDirectory || node.IsDirectory)
                throw Fail(DfsErrorCode.AlreadyExists, $"'{destination}' exists; directories are never replaced.");
            replaced = existing;
        }

        IReadOnlyList<InodeRecord> srcChain = src.ChainNearestFirst();
        RequireDelete(ctx, srcChain, $"move '{source}'");
        Require(ctx, dst.ParentChainNearestFirst(), node.IsDirectory ? DfsFileSystemRights.CreateDirectories : DfsFileSystemRights.CreateFiles,
            $"move into '{DfsPath.GetParent(destination)}'");
        if (replaced is not null)
            RequireDelete(ctx, dst.ChainNearestFirst(), $"replace '{destination}'");
        RequireDeleteSharing(s, node.Id, $"move '{source}'");
        if (replaced is not null)
            RequireDeleteSharing(s, replaced.Id, $"replace '{destination}'");

        // Unlink from the source parent, link into the destination parent (one entry; both parents bump their revision).
        InodeRecord srcParent = s.Inodes[src.Parent!.Id];
        srcParent = Touch(srcParent with { Children = srcParent.Children.Remove(src.Name) }, ctx);
        s = Put(s, srcParent);
        InodeRecord targetParent = s.Inodes[dstParent.Id];
        targetParent = Touch(targetParent with { Children = targetParent.Children.SetItem(dst.Name, node.Id) }, ctx);
        s = Put(s, targetParent);
        InodeRecord moved = Touch(node with { ParentId = targetParent.Id, Name = dst.Name }, ctx);
        s = Put(s, moved);
        if (replaced is not null)
            s = DetachSubtree(s, [replaced], ctx.Index);

        (s, SnapshotInfoData? snapshot) = SnapshotInSameEntry(s, ctx, c.SnapshotName);
        return (s, VolumeApplyResult.Ok(new MoveResultMessage(new InodeId(moved.Id), moved.Revision, s.TokenAt(ctx.Index), snapshot)));
    }

    private static (VolumeState, VolumeApplyResult) ApplyDelete(VolumeState s, ApplyContext ctx, DeleteCommand c)
    {
        RequireGrant(ctx, DfsPermissions.Write, "delete");
        string path = NormalizePath(c.TargetPath);
        (string filePath, string? stream) = DfsStreamPath.Split(path);
        if (stream is not null && c.ExpectDirectory)
            throw Fail(DfsErrorCode.InvalidPath, $"'{path}' addresses a stream; directory operations take no stream syntax.");
        if (DfsPath.IsRoot(filePath))
            throw Fail(DfsErrorCode.InvalidArgument, "The root directory cannot be deleted.");

        var view = new NamespaceView(s.Inodes);
        Resolution res = view.Resolve(filePath);
        RequireTraverse(s, ctx, res);
        if (!res.Found)
        {
            if (c.ExpectDirectory)
                throw Fail(DfsErrorCode.DirectoryNotFound, $"'{path}' does not exist.");
            return Absent(s, ctx, c.SnapshotName);
        }

        InodeRecord target = res.Target!;
        IReadOnlyList<InodeRecord> chain = res.ChainNearestFirst();
        if (stream is not null)
        {
            if (!target.Streams.TryGetValue(stream, out StreamRecord? existingStream))
                return Absent(s, ctx, c.SnapshotName);
            RequireDelete(ctx, chain, $"delete stream '{path}'");
            var key = new StreamKey(target.Id, stream);
            if (HandlesOf(s, key).FirstOrDefault(h => (h.Share & FileShare.Delete) == 0) is { } blocking)
                throw Fail(DfsErrorCode.SharingViolation, $"Deleting '{path}': handle {blocking.Id} does not grant FileShare.Delete.");
            foreach (HandleRecord h in HandlesOf(s, key).ToArray())
                s = ReleaseHandle(s, h, ctx.Index);
            InodeRecord updated = Touch(s.Inodes[target.Id] with { Streams = s.Inodes[target.Id].Streams.Remove(stream) }, ctx);
            s = Put(s, updated);
            s = Dereference(s, existingStream.Objects(), ctx.Index);
            s = s with { WriterGenerations = s.WriterGenerations.Remove(key) };
            (s, SnapshotInfoData? streamSnap) = SnapshotInSameEntry(s, ctx, c.SnapshotName);
            return (s, VolumeApplyResult.Ok(new DeleteResultMessage(true, new InodeId(target.Id), s.TokenAt(ctx.Index), streamSnap)));
        }

        if (c.ExpectDirectory && !target.IsDirectory)
            throw Fail(DfsErrorCode.NotADirectory, $"'{path}' is a file.");
        if (!c.ExpectDirectory && target.IsDirectory)
            throw Fail(DfsErrorCode.IsADirectory, $"'{path}' is a directory.");
        if (target.IsDirectory && target.Children.Count > 0 && !c.Recursive)
            throw Fail(DfsErrorCode.DirectoryNotEmpty, $"'{path}' is not empty.");

        List<InodeRecord> subtree = view.Subtree(target.Id);
        RequireDelete(ctx, chain, $"delete '{path}'");
        foreach (InodeRecord n in subtree)
        {
            if (n.Id != target.Id && view.ChainOf(n.Id) is { } childChain)
                RequireDelete(ctx, childChain, $"delete '{view.PathOf(n.Id) ?? path}'");
            RequireDeleteSharing(s, n.Id, $"delete '{view.PathOf(n.Id) ?? path}'");
        }

        InodeRecord parent = s.Inodes[res.Parent!.Id];
        s = Put(s, Touch(parent with { Children = parent.Children.Remove(res.Name) }, ctx));
        s = DetachSubtree(s, subtree, ctx.Index);
        (s, SnapshotInfoData? snapshot) = SnapshotInSameEntry(s, ctx, c.SnapshotName);
        return (s, VolumeApplyResult.Ok(new DeleteResultMessage(true, new InodeId(target.Id), s.TokenAt(ctx.Index), snapshot)));
    }

    private static (VolumeState, VolumeApplyResult) Absent(VolumeState s, ApplyContext ctx, string? snapshotName)
    {
        (s, SnapshotInfoData? snapshot) = SnapshotInSameEntry(s, ctx, snapshotName);
        return (s, VolumeApplyResult.Ok(new DeleteResultMessage(false, default, snapshot is null ? CommitToken.None : s.TokenAt(ctx.Index), snapshot)));
    }
}
