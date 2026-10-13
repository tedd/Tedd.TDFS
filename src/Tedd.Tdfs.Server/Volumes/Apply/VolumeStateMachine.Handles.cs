using System.Collections.Immutable;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Volumes.Commands;
using Tedd.Tdfs.Server.Volumes.State;

namespace Tedd.Tdfs.Server.Volumes.Apply;

/// <summary>Result value of an open (for the leader's lease tracker).</summary>
internal sealed record OpenValue(HandleRecord Handle);

internal sealed partial class VolumeStateMachine
{
    private const FileShare DefinedShareBits = FileShare.Read | FileShare.Write | FileShare.Delete | FileShare.Inheritable;
    private const FileOptions SupportedOptions = FileOptions.Asynchronous | FileOptions.SequentialScan | FileOptions.RandomAccess;

    /// <summary>
    /// Validates raw open arguments exactly as PROTOCOL.md §8.2 requires (server side; the SDK validates too): defined mode,
    /// access and share bits, no Inheritable, mode/access combinations (FSD §6.1), no FileShare.Write on writable handles,
    /// no VersionPinned writable handles, supported options only. Returns the error, or null.
    /// </summary>
    public static (DfsErrorCode Code, string Message)? ValidateOpen(FileMode mode, FileAccess access, FileShare share, FileOptions options, bool versionPinned)
    {
        if (mode is < FileMode.CreateNew or > FileMode.Append)
            return (DfsErrorCode.InvalidArgument, $"FileMode {(int)mode} is not defined.");
        if (access is not (FileAccess.Read or FileAccess.Write or FileAccess.ReadWrite))
            return (DfsErrorCode.InvalidArgument, $"FileAccess {(int)access} is not defined.");
        if ((share & ~DefinedShareBits) != 0)
            return (DfsErrorCode.InvalidArgument, $"FileShare 0x{(int)share:X} has undefined bits.");
        if ((share & FileShare.Inheritable) != 0)
            return (DfsErrorCode.InvalidArgument, "FileShare.Inheritable (inherited OS handles) is not supported.");
        bool writable = (access & FileAccess.Write) != 0;
        switch (mode)
        {
            case FileMode.CreateNew or FileMode.Create when !writable:
                return (DfsErrorCode.InvalidArgument, $"{mode} requires write access.");
            case FileMode.Truncate when access != FileAccess.Write:
                return (DfsErrorCode.InvalidArgument, "Truncate requires write-only access.");
            case FileMode.Append when access != FileAccess.Write:
                return (DfsErrorCode.InvalidArgument, "Append is write-only.");
        }

        if (writable && (share & FileShare.Write) != 0)
            return (DfsErrorCode.InvalidArgument, "A writable handle may not grant FileShare.Write (multiple writers are not supported).");
        if (versionPinned && writable)
            return (DfsErrorCode.InvalidArgument, "VersionPinned is restricted to read-only handles.");
        if ((options & ~SupportedOptions) != 0)
            return (DfsErrorCode.InvalidArgument, $"FileOptions 0x{(int)options:X} are not supported.");
        return null;
    }

    private static (VolumeState, VolumeApplyResult) ApplyOpenHandle(VolumeState s, ApplyContext ctx, OpenHandleCommand c)
    {
        if (ValidateOpen(c.Mode, c.Access, c.Share, c.Options, c.VersionPinned) is { } invalid)
            throw Fail(invalid.Code, invalid.Message);
        bool writable = (c.Access & FileAccess.Write) != 0;
        bool readable = (c.Access & FileAccess.Read) != 0;
        RequireGrant(ctx, writable ? DfsPermissions.Write : DfsPermissions.Read, "open");
        if (readable && writable)
            RequireGrant(ctx, DfsPermissions.Read, "open");
        string path = NormalizePath(c.FilePath);
        (string filePath, string? streamName) = DfsStreamPath.Split(path);
        string stream = streamName ?? DfsStreamPath.DefaultStreamName;
        SessionRecord session = RequireSession(s, ctx, c.Session);
        int open = s.Handles.Values.Count(h => h.Session == session.Id);
        if (open >= s.Config.Limits.MaxHandlesPerSession)
        {
            throw Fail(DfsErrorCode.LimitExceeded, $"The session already holds {open} handles.",
                LimitDetail(nameof(DfsVolumeLimits.MaxHandlesPerSession), s.Config.Limits.MaxHandlesPerSession, open + 1));
        }

        var view = new NamespaceView(s.Inodes);
        Resolution res = view.Resolve(filePath);
        RequireTraverse(s, ctx, res);
        if (res.Status is ResolveStatus.AncestorMissing or ResolveStatus.AncestorNotDirectory)
            throw Fail(DfsErrorCode.DirectoryNotFound, $"The directory of '{path}' does not exist.");

        bool created = false;
        bool truncated = false;
        bool creating = c.Mode is FileMode.CreateNew or FileMode.Create or FileMode.OpenOrCreate or FileMode.Append;
        InodeRecord inode;
        if (!res.Found)
        {
            if (!creating)
                throw Fail(DfsErrorCode.FileNotFound, $"'{path}' does not exist.");

            // Creating is a mutation whatever the requested access (OpenOrCreate with read access creates too): it needs the
            // volume Write grant as well as the ACL right (ACL-06).
            RequireGrant(ctx, DfsPermissions.Write, $"create '{path}'");
            Require(ctx, res.ParentChainNearestFirst(), DfsFileSystemRights.CreateFiles, $"create '{path}'");
            (s, ulong id) = AllocateInode(s);
            inode = NewInode(s, ctx, id, InodeKind.File, res.Parent!, res.Name, c.StoragePolicy ?? res.Parent!.PolicyName);
            if (stream.Length > 0)
                inode = inode with { Streams = inode.Streams.Add(stream, StreamRecord.Empty(ctx.Index)) };
            s = Link(s, ctx, res.Parent!, inode);
            created = true;
        }
        else
        {
            inode = res.Target!;
            IReadOnlyList<InodeRecord> chain = res.ChainNearestFirst();
            if (inode.IsDirectory && stream.Length == 0)
                throw Fail(DfsErrorCode.IsADirectory, $"'{path}' is a directory.");
            bool streamExists = inode.Streams.ContainsKey(stream);
            if (c.Mode == FileMode.CreateNew && streamExists)
                throw Fail(DfsErrorCode.AlreadyExists, $"'{path}' already exists.");
            if (!streamExists)
            {
                if (!creating)
                    throw Fail(DfsErrorCode.FileNotFound, $"Stream '{path}' does not exist.");
                RequireGrant(ctx, DfsPermissions.Write, $"create stream '{path}'");
                Require(ctx, chain, DfsFileSystemRights.WriteData, $"create stream '{path}'");
                if (inode.NamedStreamCount >= DfsStreamPath.MaxStreamsPerInode)
                {
                    throw Fail(DfsErrorCode.LimitExceeded, $"'{filePath}' already has {inode.NamedStreamCount} named streams.",
                        LimitDetail("MaxStreamsPerInode", DfsStreamPath.MaxStreamsPerInode, inode.NamedStreamCount + 1));
                }
            }

            DfsFileSystemRights rights = RightsOf(ctx, chain);
            if (readable && !VolumeAccess.Has(rights, DfsFileSystemRights.ReadData))
                throw Denied(ctx, $"open '{path}' for reading", DfsFileSystemRights.ReadData);
            if (writable)
            {
                bool appendOnly = c.Mode == FileMode.Append;
                bool allowed = VolumeAccess.Has(rights, DfsFileSystemRights.WriteData)
                    || (appendOnly && VolumeAccess.Has(rights, DfsFileSystemRights.AppendData));
                if (!allowed)
                    throw Denied(ctx, $"open '{path}' for writing", appendOnly ? DfsFileSystemRights.AppendData : DfsFileSystemRights.WriteData);
            }

            var key = new StreamKey(inode.Id, stream);
            foreach (HandleRecord active in HandlesOf(s, key))
            {
                if (!Compatible(active, c.Access, c.Share))
                {
                    bool writers = writable && active.IsWriter;
                    throw Fail(writers ? DfsErrorCode.WriterConflict : DfsErrorCode.SharingViolation,
                        $"'{path}' is open by handle {active.Id} ({active.Access}, share {active.Share}).");
                }
            }

            if (!streamExists)
            {
                inode = Touch(inode with { Streams = inode.Streams.Add(stream, StreamRecord.Empty(ctx.Index)) }, ctx);
                s = Put(s, inode);
                created = true;
            }
            else if (c.Mode is FileMode.Create or FileMode.Truncate)
            {
                StreamRecord old = inode.Streams[stream];
                inode = Touch(inode with { Streams = inode.Streams.SetItem(stream, StreamRecord.Empty(ctx.Index) with { CreatedIndex = old.CreatedIndex }) }, ctx);
                s = Put(s, inode);
                s = Dereference(s, old.Objects(), ctx.Index);
                truncated = true;
            }
        }

        // Allocate the handle with its writer generation, lease and pin.
        var streamKey = new StreamKey(inode.Id, stream);
        ulong generation = 0;
        if (writable)
        {
            generation = s.WriterGenerationOf(streamKey) + 1;
            s = s with { WriterGenerations = s.WriterGenerations.SetItem(streamKey, generation) };
        }

        ulong handleId = s.NextHandleId;
        HandleView handleView = c.VersionPinned ? HandleView.VersionPinned : HandleView.Live;
        ulong pinIndex = c.VersionPinned ? inode.GetStream(stream)?.Revision ?? ctx.Index : ctx.Index;
        (s, PinRecord pin) = AddPin(s, PinKind.Handle, session.Id, session.Principal, ctx.Index, pinIndex, handleId, null, 0, string.Empty, null, null);
        DfsFileSystemRights granted = RightsOf(ctx, new NamespaceView(s.Inodes).ChainOf(inode.Id) ?? [inode]);
        var handle = new HandleRecord(handleId, inode.Id, stream, session.Id, session.Principal, c.Mode, c.Access, c.Share, writable, generation, handleView,
            c.VersionPinned ? inode : null, pin.Id, ctx.Index, granted, c.Mode == FileMode.Append);
        s = s with { NextHandleId = handleId + 1, Handles = s.Handles.Add(handleId, handle) };

        FileInfoData info = Describe(s, inode, path, streamName);
        var reply = new OpenHandleResultMessage(new HandleId(handleId), info, generation, pinIndex, c.LeaseDuration, created, truncated, s.TokenAt(ctx.Index));
        return (s, VolumeApplyResult.Ok(reply, new OpenValue(handle)));
    }

    private static (VolumeState, VolumeApplyResult) ApplyCloseHandle(VolumeState s, ApplyContext ctx, CloseHandleCommand c)
    {
        string principal = ctx.Op?.Principal ?? string.Empty;
        if (!s.Handles.TryGetValue(c.Handle, out HandleRecord? h))
        {
            if (c.Handle == 0 || c.Handle >= s.NextHandleId)
                throw Fail(DfsErrorCode.InvalidHandle, $"Handle {c.Handle} was never issued by this volume.");
            return (s, VolumeApplyResult.Ok(new CloseHandleResultMessage(false, s.TokenAt(ctx.Index))));
        }

        if (!string.Equals(h.Principal, principal, StringComparison.Ordinal))
            throw Fail(DfsErrorCode.InvalidHandle, $"Handle {c.Handle} belongs to another principal.");
        bool generationMatches = c.Generation == h.WriterGeneration;
        s = ReleaseHandle(s, h, ctx.Index);
        return (s, VolumeApplyResult.Ok(new CloseHandleResultMessage(generationMatches, s.TokenAt(ctx.Index)), h));
    }

    private static (VolumeState, VolumeApplyResult) ApplyCreateStaging(VolumeState s, ApplyContext ctx, CreateStagingCommand c)
    {
        RequireGrant(ctx, DfsPermissions.Write, "create staging");
        string principal = ctx.Op?.Principal ?? string.Empty;
        ulong owner = 0;
        if (c.Session != 0)
            owner = RequireSession(s, ctx, c.Session).Id;
        if (!c.Transaction.IsEmpty)
            throw Fail(DfsErrorCode.Unsupported, "Transaction staging sessions are implemented with transactions (next phase).");
        if (c.Handle != 0)
        {
            if (!s.Handles.TryGetValue(c.Handle, out HandleRecord? h))
                throw Fail(c.Handle >= s.NextHandleId ? DfsErrorCode.InvalidHandle : DfsErrorCode.LeaseLost, $"Handle {c.Handle} is not active.");
            if (!string.Equals(h.Principal, principal, StringComparison.Ordinal))
                throw Fail(DfsErrorCode.InvalidHandle, $"Handle {c.Handle} belongs to another principal.");
            if (!h.IsWriter)
                throw Fail(DfsErrorCode.AccessDenied, $"Handle {c.Handle} is not writable.");
            owner = h.Session;
        }

        var staging = new StagingRecord(s.NextStagingId, owner, c.Handle, c.Transaction, principal, StagingState.Open, ctx.Index, 0, ctx.Now);
        s = s with { NextStagingId = staging.Id + 1, Staging = s.Staging.Add(staging.Id, staging) };
        var reply = new CreateStagingResultMessage(new StagingId(staging.Id), ReadOnlyMemory<byte>.Empty, ctx.Now, 0, s.TokenAt(ctx.Index));
        return (s, VolumeApplyResult.Ok(reply, staging));
    }

    private static (VolumeState, VolumeApplyResult) ApplyCloseStaging(VolumeState s, ApplyContext ctx, CloseStagingCommand c)
    {
        foreach (ulong id in OrEmpty(c.Staging).Order())
        {
            if (s.Staging.TryGetValue(id, out StagingRecord? staging))
                s = CloseStaging(s, staging, ctx.Index);
        }

        return (s, VolumeApplyResult.Ok(null));
    }

    private static (VolumeState, VolumeApplyResult) ApplyReclaimDetached(VolumeState s, ApplyContext ctx, ReclaimDetachedCommand c)
    {
        var handled = new HashSet<ulong>(s.Handles.Values.Select(h => h.Inode));
        foreach (ulong id in OrEmpty(c.Inodes).Order())
        {
            if (s.Detached.Contains(id) && !handled.Contains(id))
                s = Reclaim(s, id, ctx.Index);
        }

        return (s, VolumeApplyResult.Ok(null));
    }
}
