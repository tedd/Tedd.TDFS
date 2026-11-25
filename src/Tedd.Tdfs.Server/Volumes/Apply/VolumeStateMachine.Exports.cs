using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Volumes.Commands;
using Tedd.Tdfs.Server.Volumes.State;

namespace Tedd.Tdfs.Server.Volumes.Apply;

internal sealed partial class VolumeStateMachine
{
    private static void RequireAvailable(VolumeState s, ulong inode)
    {
        if (s.Root.ExportProfile != DfsExportProfile.Disabled && HandlesOf(s, inode).Any(h => h.DeleteOnClose || h.CommittedDeleteRoot != 0))
            throw Fail(DfsErrorCode.SharingViolation, $"Inode {inode} is delete-pending.");
    }

    private static string ScopedPath(VolumeState s, ulong root, string relative)
    {
        var view = new NamespaceView(s.Inodes);
        if (s.Find(root) is not { IsDirectory: true } || view.PathOf(root) is not { } prefix)
            throw Fail(DfsErrorCode.DirectoryNotFound, "The export root is no longer attached.");
        relative = NormalizePath(relative);
        string path = prefix == "/" ? relative : relative == "/" ? prefix : NormalizePath(prefix + relative);
        RequireMapping(s, view.Resolve(DfsStreamPath.GetFilePath(path)), root);
        return path;
    }

    private static void RequireMapping(VolumeState s, Resolution resolution, ulong root)
    {
        var nearest = resolution.ChainNearestFirst().FirstOrDefault(n => s.ExportRoots.ContainsKey(n.Id));
        if (nearest?.Id != root)
            throw Fail(DfsErrorCode.CrossVolume, "The operation crosses an export mapping boundary.");
    }

    private static (VolumeState, VolumeApplyResult) ApplyExportOpen(VolumeState s, ApplyContext ctx, ExportOpenCommand c)
    {
        ExportOpenMessage request = c.Request;
        OpenHandleMessage open = request.Open;
        if (s.Root.ExportProfile == DfsExportProfile.Disabled)
            throw Fail(DfsErrorCode.Unsupported, "The volume has no live export profile.");
        CommitToken identity = request.Root.Volume;
        if (identity.ClusterId != s.Config.ClusterId || identity.VolumeId != s.Config.Id || identity.VolumeEpoch != s.Config.Epoch)
            throw Fail(DfsErrorCode.CrossVolume, "The mapping belongs to another volume incarnation.");
        if (!s.ExportRoots.TryGetValue(request.Root.Directory.Value, out ulong registration) || identity.Sequence != registration)
            throw Fail(DfsErrorCode.AccessDenied, "The export mapping is no longer registered.");
        var access = (FileAccess)((int)request.DesiredAccess & 3);
        if (((int)request.DesiredAccess & ~7) != 0 || access != open.Access || open.VersionPinned || open.Header.Transaction is not null)
            throw Fail(DfsErrorCode.InvalidArgument, "Invalid live export reservation.");
        if (request.Directory && (open.Mode is not (FileMode.Open or FileMode.OpenOrCreate or FileMode.CreateNew) || DfsStreamPath.HasStream(NormalizePath(open.Path))))
            throw Fail(DfsErrorCode.InvalidArgument, "Invalid directory open.");
        string path = ScopedPath(s, request.Root.Directory.Value, open.Path);
        if (DfsStreamPath.HasStream(path) && (request.DesiredAccess & DfsOpenAccess.Delete) != 0)
            throw Fail(DfsErrorCode.Unsupported, "Namespace handles address the base inode.");
        (s, var result) = ApplyOpenHandle(s, ctx, new OpenHandleCommand(path, open.Mode, access, open.Share, open.Options, false, open.StoragePolicy,
            open.Header.Session, c.Lease), request);
        return (s, result with { Reply = new ExportOpenResultMessage((OpenHandleResultMessage)result.Reply!) });
    }

    private static (VolumeState, VolumeApplyResult) ApplyExportControl(VolumeState s, ApplyContext ctx, ExportControlMessage request)
    {
        if (request.Action == ExportControlKind.Bind)
        {
            if (ctx.Op?.IsAdmin != true)
                throw Fail(DfsErrorCode.AccessDenied, "A cluster administrator must register the mapping.");
            string path = NormalizePath(request.Destination);
            var view = new NamespaceView(s.Inodes);
            if (DfsStreamPath.HasStream(path) || view.Resolve(path).Target is not { IsDirectory: true } directory)
                throw Fail(DfsErrorCode.DirectoryNotFound, "The mapping must name an attached directory.");
            if (!s.ExportRoots.TryGetValue(directory.Id, out ulong registration))
            {
                if (s.Handles.Values.Any(handle => (handle.ExportRoot != 0 || handle.CommittedDeleteRoot != 0) && view.IsAncestorOrSelf(directory.Id, handle.Inode)))
                    throw Fail(DfsErrorCode.SharingViolation, "Close the subtree's export handles before registering a nested mapping.");
                registration = ctx.Index;
                s = s with { ExportRoots = s.ExportRoots.Add(directory.Id, registration) };
            }
            return (s, VolumeApplyResult.Ok(new ExportControlResultMessage(s.TokenAt(registration), new InodeId(directory.Id))));
        }
        if (request.Action == ExportControlKind.Configure)
            return ConfigureExport(s, ctx, request.Profile);
        if (!s.Handles.TryGetValue(request.Handle.Value, out var h) || h.ExportRoot == 0 || h.Session != ctx.Op?.RequestSession
            || !string.Equals(h.Principal, ctx.Op.Principal, StringComparison.Ordinal))
            throw Fail(DfsErrorCode.InvalidHandle, "The session has no such export handle.");
        RequireGrant(ctx, DfsPermissions.Write, "modify opened object");
        InodeRecord inode = s.Inodes[h.Inode];
        if (request.Action == ExportControlKind.SetBasicInformation)
        {
            if (!VolumeAccess.Has(h.GrantedRights, DfsFileSystemRights.WriteAttributes))
                throw Denied(ctx, "set basic information", DfsFileSystemRights.WriteAttributes);
            var info = request.Information ?? throw Fail(DfsErrorCode.InvalidArgument, "Missing basic information.");
            const FileAttributes allowed = FileAttributes.Archive | FileAttributes.ReadOnly | FileAttributes.Hidden |
                FileAttributes.System | FileAttributes.Normal | FileAttributes.Directory;
            if (info.Attributes is { } flags && ((flags & ~allowed) != 0 || (!inode.IsDirectory && flags.HasFlag(FileAttributes.Directory))))
                throw Fail(DfsErrorCode.InvalidArgument, "Unsupported DOS attributes.");
            inode = inode with { Revision = ctx.Index, MetadataRevision = ctx.Index, CreatedUtc = info.CreatedUtc ?? inode.CreatedUtc,
                ModifiedUtc = info.ModifiedUtc ?? inode.ModifiedUtc, ChangedUtc = info.ChangedUtc ?? ctx.Now,
                AccessedUtc = info.AccessedUtc ?? inode.AccessedUtc ?? inode.CreatedUtc,
                DosAttributes = info.Attributes is { } attrs && inode.IsDirectory ? attrs | FileAttributes.Directory : info.Attributes ?? inode.DosAttributes };
            s = Put(s, inode);
        }
        else
        {
            var view = new NamespaceView(s.Inodes);
            if (!h.DeleteAccess || inode.Id == h.ExportRoot || !view.IsAncestorOrSelf(h.ExportRoot, inode.Id) || view.PathOf(inode.Id) is not { } source)
                throw Fail(DfsErrorCode.AccessDenied, "The source must be attached beneath the mapping and opened for deletion.");
            RequireMapping(s, view.Resolve(source), h.ExportRoot);
            if (request.Action == ExportControlKind.Rename)
            {
                if (inode.IsDirectory && s.ExportRoots.Keys.Any(root => view.IsAncestorOrSelf(inode.Id, root)))
                    throw Fail(DfsErrorCode.CrossVolume, "The directory contains a separate export mapping.");
                string destination = ScopedPath(s, h.ExportRoot, request.Destination);
                (s, _) = ApplyMove(s, ctx, new MoveCommand(source, destination, request.Flag, inode.IsDirectory, null), h.Id);
            }
            else if (request.Action == ExportControlKind.Disposition)
            {
                if (request.Flag && !inode.Children.IsEmpty)
                    throw Fail(DfsErrorCode.DirectoryNotEmpty, "Only empty directories support delete-on-close.");
                s = s with { Handles = s.Handles.SetItem(h.Id, h with { DeleteOnClose = request.Flag }) };
            }
            else
                throw Fail(DfsErrorCode.InvalidArgument, "Unknown handle operation.");
        }
        return (s, VolumeApplyResult.Ok(new ExportControlResultMessage(s.TokenAt(ctx.Index))));
    }

    private static (VolumeState, VolumeApplyResult) ConfigureExport(VolumeState s, ApplyContext ctx, DfsExportProfile profile)
    {
        if (ctx.Op?.IsAdmin != true)
            throw Fail(DfsErrorCode.AccessDenied, "A cluster administrator must configure the namespace profile.");
        if (!Enum.IsDefined(profile))
            throw Fail(DfsErrorCode.InvalidArgument, "Unknown namespace profile.");
        if (profile != s.Root.ExportProfile)
        {
            if (!s.Handles.IsEmpty || s.Transactions.Values.Any(t => t.State == TxState.Active))
                throw Fail(DfsErrorCode.SharingViolation, "Close live handles and transactions before changing profiles.");
            if (profile == DfsExportProfile.Disabled)
                s = s with { ExportRoots = s.ExportRoots.Clear() };
            var comparer = profile == DfsExportProfile.Windows ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal;
            foreach (var inode in s.Inodes.Values)
            {
                if (inode.Children.Keys.Distinct(comparer).Count() != inode.Children.Count || inode.Streams.Keys.Distinct(comparer).Count() != inode.Streams.Count)
                    throw Fail(DfsErrorCode.AlreadyExists, $"Directory inode {inode.Id} contains colliding names.");
                s = Put(s, inode with { Children = inode.Children.WithComparers(comparer), Streams = inode.Streams.WithComparers(comparer), ExportProfile = profile, Revision = ctx.Index });
            }
        }
        return (s, VolumeApplyResult.Ok(new ExportControlResultMessage(s.TokenAt(ctx.Index))));
    }
}
