using System.Collections.Immutable;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Volumes.Commands;
using Tedd.Tdfs.Server.Volumes.State;

namespace Tedd.Tdfs.Server.Volumes.Apply;

internal sealed partial class VolumeStateMachine
{
    /// <summary>The active writable handle a publication goes through, fenced by its generation (LOCK-04).</summary>
    internal static HandleRecord RequireWriterHandle(VolumeState s, ApplyContext ctx, ulong handleId, ulong generation)
    {
        string principal = ctx.Op?.Principal ?? string.Empty;
        if (!s.Handles.TryGetValue(handleId, out HandleRecord? h))
        {
            throw handleId == 0 || handleId >= s.NextHandleId
                ? Fail(DfsErrorCode.InvalidHandle, $"Handle {handleId} was never issued by this volume.")
                : Fail(DfsErrorCode.LeaseLost, $"Handle {handleId} is no longer active (closed or its lease expired).");
        }

        if (!string.Equals(h.Principal, principal, StringComparison.Ordinal))
            throw Fail(DfsErrorCode.InvalidHandle, $"Handle {handleId} belongs to another principal.");
        if (!h.IsWriter)
            throw Fail(DfsErrorCode.AccessDenied, $"Handle {handleId} was not opened for writing.");
        bool skipFencing = ctx.Op is { } op && (op.Flags & CommandFlags.SkipFencing) != 0;
        if (!skipFencing && (generation != h.WriterGeneration || s.WriterGenerationOf(h.Key) != h.WriterGeneration))
            throw Fail(DfsErrorCode.LeaseLost, $"Writer generation {generation} of handle {handleId} is stale (current {s.WriterGenerationOf(h.Key)}).");
        return h;
    }

    /// <summary>Every staging session referenced by data changes must be open and belong to the caller (§5.4).</summary>
    private static void RequireOpenStaging(VolumeState s, ApplyContext ctx, PublishSpec spec)
    {
        string principal = ctx.Op?.Principal ?? string.Empty;
        foreach (ulong stagingId in spec.Changes.Where(ch => ch.Extent is not null).Select(ch => ch.Extent!.Staging.Value).Distinct().Order())
        {
            if (!s.Staging.TryGetValue(stagingId, out StagingRecord? staging) || !staging.IsOpen)
                throw Fail(DfsErrorCode.LeaseLost, $"Staging session {stagingId} is closed; its uploads can no longer be published.");
            if (!string.Equals(staging.Principal, principal, StringComparison.Ordinal))
                throw Fail(DfsErrorCode.AccessDenied, $"Staging session {stagingId} belongs to another principal.");
        }
    }

    /// <summary>Publication limits (FSD §6.3, NS-03): final length and published bytes.</summary>
    private static void RequireLimits(VolumeState s, long newLength, PublishSpec spec)
    {
        DfsVolumeLimits limits = s.Config.Limits;
        if (newLength > limits.MaxFileLength)
        {
            throw Fail(DfsErrorCode.LimitExceeded, $"A {newLength}-byte stream exceeds the maximum file length {limits.MaxFileLength}.",
                LimitDetail(nameof(DfsVolumeLimits.MaxFileLength), limits.MaxFileLength, newLength));
        }

        long published = 0;
        foreach (StreamChange c in spec.Changes)
        {
            if (c.ChangeKind == RangeChangeKind.Zero)
                continue;
            published = published > long.MaxValue - c.Length ? long.MaxValue : published + c.Length;
        }

        if (published > limits.MaxPublishBytes)
        {
            throw Fail(DfsErrorCode.LimitExceeded, $"The publication of {published} bytes exceeds the limit {limits.MaxPublishBytes}.",
                LimitDetail(nameof(DfsVolumeLimits.MaxPublishBytes), limits.MaxPublishBytes, published));
        }
    }

    /// <summary>Applies a publication spec to one stream of an inode (the inode record is updated and stored).</summary>
    private static (VolumeState State, InodeRecord Inode, StreamRecord Stream) Publish(VolumeState s, ApplyContext ctx, InodeRecord inode, string stream,
        long truncateTo, IReadOnlyList<StreamChange> changes, long newLength, PublishSpec spec)
    {
        StreamRecord? before = inode.GetStream(stream);
        if (spec.DerivedFromRevision != 0 && (before?.Revision ?? 0) != spec.DerivedFromRevision)
        {
            throw Fail(DfsErrorCode.WriterConflict,
                $"The stream changed (revision {before?.Revision ?? 0}) after the leader prepared the publication from revision {spec.DerivedFromRevision}; retry.");
        }

        RequireLimits(s, newLength, spec);
        RequireOpenStaging(s, ctx, spec);
        ContentResult result = ContentAlgebra.Apply(before, truncateTo, changes, newLength, spec.FinalInline, s.Config.InlineThreshold, ctx.Index);
        if (result.Stream is not { } after)
            throw Fail(result.Code, result.Message);
        InodeRecord updated = Touch(inode with { Streams = inode.Streams.SetItem(stream, after) }, ctx);
        s = Put(s, updated);
        s = Dereference(s, Dropped(before, after), ctx.Index);
        return (s, updated, after);
    }

    /// <summary>Closes the leader-created staging session of a publication (on success and on failure).</summary>
    private static VolumeState CloseLeaderStaging(VolumeState s, PublishSpec spec, ulong index)
        => spec.LeaderStaging != 0 && s.Staging.TryGetValue(spec.LeaderStaging, out StagingRecord? staging) ? CloseStaging(s, staging, index) : s;

    private static (VolumeState, VolumeApplyResult) WithLeaderStagingClosed(VolumeState s, ApplyContext ctx, PublishSpec spec,
        Func<VolumeState, (VolumeState, VolumeApplyResult)> body)
    {
        try
        {
            (VolumeState next, VolumeApplyResult result) = body(s);
            return (CloseLeaderStaging(next, spec, ctx.Index), result);
        }
        catch (ApplyFailure failure) when (spec.LeaderStaging != 0)
        {
            return (CloseLeaderStaging(s, spec, ctx.Index), VolumeApplyResult.Fail(failure.Code, failure.Message, failure.Detail));
        }
    }

    private static (VolumeState, VolumeApplyResult) ApplyCommitFile(VolumeState s, ApplyContext ctx, CommitFileCommand c)
        => WithLeaderStagingClosed(s, ctx, c.Spec, state => CommitFileCore(state, ctx, c));

    private static (VolumeState, VolumeApplyResult) CommitFileCore(VolumeState s, ApplyContext ctx, CommitFileCommand c)
    {
        RequireGrant(ctx, DfsPermissions.Write, "publish");
        HandleRecord h = RequireWriterHandle(s, ctx, c.Handle, c.Generation);
        if (s.Find(h.Inode) is not { } inode || inode.GetStream(h.Stream) is not { } stream)
            throw Fail(DfsErrorCode.LeaseLost, $"The stream of handle {h.Id} no longer exists.");
        if (c.BaseRevision != 0 && c.BaseRevision != stream.Revision)
            throw Fail(DfsErrorCode.WriterConflict, $"Base revision {c.BaseRevision} is stale (current {stream.Revision}).");
        if (h.IsAppend && c.Spec.TruncateTo >= 0 && c.Spec.TruncateTo < stream.Length)
            throw Fail(DfsErrorCode.InvalidArgument, "An append handle cannot truncate.");
        (s, InodeRecord updated, StreamRecord after) = Publish(s, ctx, inode, h.Stream, c.Spec.TruncateTo, c.Spec.Changes, c.Spec.NewLength, c.Spec);
        (s, SnapshotInfoData? snapshot) = SnapshotInSameEntry(s, ctx, c.SnapshotName);
        ulong revision = h.Stream.Length == 0 ? updated.Revision : after.Revision;
        return (s, VolumeApplyResult.Ok(new CommitFileResultMessage(new InodeId(updated.Id), revision, after.Length, s.TokenAt(ctx.Index), snapshot)));
    }

    private static (VolumeState, VolumeApplyResult) ApplyWriteAll(VolumeState s, ApplyContext ctx, WriteAllCommand c)
        => WithLeaderStagingClosed(s, ctx, c.Spec, state => WriteAllCore(state, ctx, c));

    private static (VolumeState, VolumeApplyResult) WriteAllCore(VolumeState s, ApplyContext ctx, WriteAllCommand c)
    {
        RequireGrant(ctx, DfsPermissions.Write, "write all");
        string path = NormalizePath(c.TargetPath);
        (string filePath, string? streamName) = DfsStreamPath.Split(path);
        string stream = streamName ?? DfsStreamPath.DefaultStreamName;
        (s, InodeRecord inode, bool created) = ResolveOrCreateFile(s, ctx, path, filePath, stream, create: true, c.CreateParents, c.StoragePolicy, append: false);
        if (!created && c.ExpectedRevision != 0)
        {
            ulong current = stream.Length == 0 ? inode.Revision : inode.GetStream(stream)?.Revision ?? 0;
            if (current != c.ExpectedRevision)
                throw Fail(DfsErrorCode.WriterConflict, $"'{path}' is at revision {current}, not {c.ExpectedRevision}.");
        }

        RequireImplicitWriter(s, new StreamKey(inode.Id, stream), $"write '{path}'");
        (s, InodeRecord updated, StreamRecord after) = Publish(s, ctx, inode, stream, 0, c.Spec.Changes, c.Spec.NewLength, c.Spec);
        (s, SnapshotInfoData? snapshot) = SnapshotInSameEntry(s, ctx, c.SnapshotName);
        ulong revision = stream.Length == 0 ? updated.Revision : after.Revision;
        return (s, VolumeApplyResult.Ok(new WriteAllResultMessage(new InodeId(updated.Id), revision, after.Length, created, s.TokenAt(ctx.Index), snapshot)));
    }

    private static (VolumeState, VolumeApplyResult) ApplyAppend(VolumeState s, ApplyContext ctx, AppendCommand c)
        => WithLeaderStagingClosed(s, ctx, c.Spec, state => AppendCore(state, ctx, c));

    private static (VolumeState, VolumeApplyResult) AppendCore(VolumeState s, ApplyContext ctx, AppendCommand c)
    {
        RequireGrant(ctx, DfsPermissions.Write, "append");
        InodeRecord inode;
        string stream;
        bool created = false;
        if (c.Handle != 0)
        {
            HandleRecord h = RequireWriterHandle(s, ctx, c.Handle, c.Generation);
            inode = s.Find(h.Inode) ?? throw Fail(DfsErrorCode.LeaseLost, $"The inode of handle {h.Id} no longer exists.");
            stream = h.Stream;
            if (inode.GetStream(stream) is null)
                throw Fail(DfsErrorCode.LeaseLost, $"The stream of handle {h.Id} no longer exists.");
        }
        else
        {
            string path = NormalizePath(c.TargetPath);
            (string filePath, string? streamName) = DfsStreamPath.Split(path);
            stream = streamName ?? DfsStreamPath.DefaultStreamName;
            (s, inode, created) = ResolveOrCreateFile(s, ctx, path, filePath, stream, c.CreateIfMissing, createParents: false, c.StoragePolicy, append: true);
            RequireImplicitWriter(s, new StreamKey(inode.Id, stream), $"append to '{path}'");
        }

        StreamRecord? before = inode.GetStream(stream);
        long oldLength = before?.Length ?? 0;
        long appended = c.Spec.NewLength;
        if (appended < 0 || oldLength > long.MaxValue - appended)
            throw Fail(DfsErrorCode.InvalidArgument, "The appended length is invalid or overflows the file length.");
        IReadOnlyList<StreamChange> changes;
        long newLength;
        if (c.Absolute)
        {
            // The leader materialized the whole result from the base revision (checked through DerivedFromRevision).
            changes = c.Spec.Changes;
            newLength = c.Spec.NewLength;
            if (newLength < oldLength)
                throw Fail(DfsErrorCode.InvalidArgument, "A materialized append cannot shorten the stream.");
        }
        else
        {
            var shifted = new List<StreamChange>(c.Spec.Changes.Length);
            foreach (StreamChange change in c.Spec.Changes)
            {
                if (change.Offset < 0 || change.Offset > appended || change.Length > appended - change.Offset)
                    throw Fail(DfsErrorCode.InvalidArgument, $"Append change [{change.Offset}, +{change.Length}) lies outside the {appended}-byte suffix.");
                shifted.Add(change with { Offset = change.Offset + oldLength });
            }

            changes = shifted;
            newLength = oldLength + appended;
        }

        (s, InodeRecord updated, StreamRecord after) = Publish(s, ctx, inode, stream, -1, changes, newLength, c.Spec);
        (s, SnapshotInfoData? snapshot) = SnapshotInSameEntry(s, ctx, c.SnapshotName);
        ulong revision = stream.Length == 0 ? updated.Revision : after.Revision;
        return (s, VolumeApplyResult.Ok(new AppendResultMessage(new InodeId(updated.Id), revision, oldLength, after.Length, created, s.TokenAt(ctx.Index),
            snapshot)));
    }

    /// <summary>
    /// Resolves the file of a path-addressed publication, creating it (and the addressed named stream) when allowed. Checks
    /// create rights on the parent, or write (append) rights on an existing file (ACL-05).
    /// </summary>
    private static (VolumeState State, InodeRecord Inode, bool Created) ResolveOrCreateFile(VolumeState s, ApplyContext ctx, string path, string filePath,
        string stream, bool create, bool createParents, string? policy, bool append)
    {
        if (DfsPath.IsRoot(filePath))
            throw Fail(stream.Length == 0 ? DfsErrorCode.IsADirectory : DfsErrorCode.InvalidPath, "The root has no data.");
        var view = new NamespaceView(s.Inodes);
        Resolution res = view.Resolve(filePath);
        if (!res.ParentExists && create && createParents)
        {
            (s, _) = EnsureDirectoryPath(s, ctx, DfsPath.GetParent(filePath));
            view = new NamespaceView(s.Inodes);
            res = view.Resolve(filePath);
        }

        RequireTraverse(s, ctx, res);
        if (!res.ParentExists)
            throw Fail(DfsErrorCode.DirectoryNotFound, $"The directory of '{path}' does not exist.");
        if (!res.Found)
        {
            if (!create)
                throw Fail(DfsErrorCode.FileNotFound, $"'{path}' does not exist.");
            Require(ctx, res.ParentChainNearestFirst(), DfsFileSystemRights.CreateFiles, $"create '{path}'");
            (s, ulong id) = AllocateInode(s);
            InodeRecord inode = NewInode(s, ctx, id, InodeKind.File, res.Parent!, res.Name, policy ?? res.Parent!.PolicyName);
            if (stream.Length > 0)
                inode = inode with { Streams = inode.Streams.Add(stream, StreamRecord.Empty(ctx.Index)) };
            s = Link(s, ctx, res.Parent!, inode);
            return (s, s.Inodes[id], true);
        }

        InodeRecord existing = res.Target!;
        if (existing.IsDirectory && stream.Length == 0)
            throw Fail(DfsErrorCode.IsADirectory, $"'{path}' is a directory.");
        IReadOnlyList<InodeRecord> chain = res.ChainNearestFirst();
        DfsFileSystemRights rights = RightsOf(ctx, chain);
        bool allowed = VolumeAccess.Has(rights, DfsFileSystemRights.WriteData) || (append && VolumeAccess.Has(rights, DfsFileSystemRights.AppendData));
        if (!allowed)
            throw Denied(ctx, $"write '{path}'", append ? DfsFileSystemRights.AppendData : DfsFileSystemRights.WriteData);
        if (existing.GetStream(stream) is null)
        {
            if (!create)
                throw Fail(DfsErrorCode.FileNotFound, $"Stream '{path}' does not exist.");
            if (existing.NamedStreamCount >= DfsStreamPath.MaxStreamsPerInode)
            {
                throw Fail(DfsErrorCode.LimitExceeded, $"'{filePath}' already has {existing.NamedStreamCount} named streams.",
                    LimitDetail("MaxStreamsPerInode", DfsStreamPath.MaxStreamsPerInode, existing.NamedStreamCount + 1));
            }

            existing = Touch(existing with { Streams = existing.Streams.Add(stream, StreamRecord.Empty(ctx.Index)) }, ctx);
            s = Put(s, existing);
            return (s, existing, true);
        }

        return (s, existing, false);
    }

    private static (VolumeState, VolumeApplyResult) ApplyCopy(VolumeState s, ApplyContext ctx, CopyCommand c)
        => WithStagingClosed(s, ctx, c.LeaderStaging, state => CopyCore(state, ctx, c));

    /// <summary>Runs <paramref name="body"/> and closes the leader-created staging session <paramref name="staging"/> (on success and on failure).</summary>
    internal static (VolumeState, VolumeApplyResult) WithStagingClosed(VolumeState s, ApplyContext ctx, ulong staging,
        Func<VolumeState, (VolumeState, VolumeApplyResult)> body)
    {
        if (staging == 0)
            return body(s);
        try
        {
            (VolumeState next, VolumeApplyResult result) = body(s);
            return (CloseStagingById(next, staging, ctx.Index), result);
        }
        catch (ApplyFailure failure)
        {
            return (CloseStagingById(s, staging, ctx.Index), VolumeApplyResult.Fail(failure.Code, failure.Message, failure.Detail));
        }
    }

    private static VolumeState CloseStagingById(VolumeState s, ulong id, ulong index)
        => s.Staging.TryGetValue(id, out StagingRecord? staging) ? CloseStaging(s, staging, index) : s;

    /// <summary>
    /// The stream as stored under a stronger policy: a leader-materialized inline stream becomes its extents (LAY-002),
    /// provided it is still at the materialized revision; other streams are returned unchanged.
    /// </summary>
    internal static StreamRecord MaterializedStream(VolumeState s, ImmutableArray<StreamMaterialization> materialized, ulong leaderStaging, string name,
        StreamRecord source)
    {
        StreamMaterialization? m = materialized.IsDefault ? null : materialized.FirstOrDefault(x => string.Equals(x.Stream, name, StringComparison.Ordinal));
        if (m is null)
            return source;
        if (!source.IsInline || source.Revision != m.SourceRevision)
            throw Fail(DfsErrorCode.WriterConflict, $"Stream '{name}' changed after the leader stored its bytes as objects; retry.");
        if (!s.Staging.TryGetValue(leaderStaging, out StagingRecord? staging) || !staging.IsOpen)
            throw Fail(DfsErrorCode.InvalidArgument, $"The leader staging session {leaderStaging} is not open.");
        long covered = 0;
        foreach (ExtentRef e in m.Extents)
        {
            if (e.Offset != covered || e.Length <= 0 || e.ObjectOffset < 0 || e.ObjectOffset + e.Length > e.ObjectLength || e.Staging.Value != leaderStaging)
                throw Fail(DfsErrorCode.InvalidArgument, $"The materialized extents of stream '{name}' do not cover it contiguously.");
            covered += e.Length;
        }

        if (covered != source.Length)
            throw Fail(DfsErrorCode.InvalidArgument, $"The materialized extents of stream '{name}' cover {covered} of {source.Length} bytes.");
        return new StreamRecord(source.Length, false, ReadOnlyMemory<byte>.Empty, m.Extents, source.Revision, source.CreatedIndex);
    }

    private static (VolumeState, VolumeApplyResult) CopyCore(VolumeState s, ApplyContext ctx, CopyCommand c)
    {
        RequireGrant(ctx, DfsPermissions.Write, "copy");
        RequireGrant(ctx, DfsPermissions.Read, "copy");
        string source = NormalizePath(c.Source);
        string destination = NormalizePath(c.Destination);
        if (DfsStreamPath.HasStream(source) || DfsStreamPath.HasStream(destination))
            throw Fail(DfsErrorCode.InvalidPath, "Copy addresses whole files (all streams are copied); stream paths are not accepted.");
        var view = new NamespaceView(s.Inodes);
        Resolution src = view.Resolve(source);
        RequireTraverse(s, ctx, src);
        if (!src.Found)
            throw Fail(src.ParentExists ? DfsErrorCode.FileNotFound : DfsErrorCode.FileNotFound, $"'{source}' does not exist.");
        InodeRecord from = src.Target!;
        if (from.IsDirectory)
            throw Fail(DfsErrorCode.IsADirectory, $"'{source}' is a directory.");
        if (c.ExpectedSourceRevision != 0 && c.ExpectedSourceRevision != from.Revision)
            throw Fail(DfsErrorCode.WriterConflict, $"'{source}' is at revision {from.Revision}, not {c.ExpectedSourceRevision}.");
        Require(ctx, src.ChainNearestFirst(), DfsFileSystemRights.ReadData, $"read '{source}'");
        foreach (HandleRecord h in HandlesOf(s, from.Id))
        {
            if ((h.Share & FileShare.Read) == 0)
                throw Fail(DfsErrorCode.SharingViolation, $"'{source}' is open by handle {h.Id} without FileShare.Read.");
        }

        Resolution dst = view.Resolve(destination);
        RequireTraverse(s, ctx, dst);
        if (!dst.ParentExists)
            throw Fail(DfsErrorCode.DirectoryNotFound, $"The directory of '{destination}' does not exist.");
        bool created;
        InodeRecord to;
        if (dst.Found)
        {
            to = dst.Target!;
            if (to.Id == from.Id)
                throw Fail(DfsErrorCode.InvalidArgument, $"'{source}' cannot be copied onto itself.");
            if (to.IsDirectory)
                throw Fail(DfsErrorCode.IsADirectory, $"'{destination}' is a directory.");
            if (!c.Overwrite)
                throw Fail(DfsErrorCode.AlreadyExists, $"'{destination}' already exists.");
            Require(ctx, dst.ChainNearestFirst(), DfsFileSystemRights.WriteData, $"overwrite '{destination}'");
            foreach (string stream in from.Streams.Keys)
                RequireImplicitWriter(s, new StreamKey(to.Id, stream), $"copy onto '{destination}'");
            created = false;
        }
        else
        {
            Require(ctx, dst.ParentChainNearestFirst(), DfsFileSystemRights.CreateFiles, $"create '{destination}'");
            (s, ulong id) = AllocateInode(s);
            to = NewInode(s, ctx, id, InodeKind.File, dst.Parent!, dst.Name, c.StoragePolicy ?? from.PolicyName);
            s = Link(s, ctx, dst.Parent!, to);
            to = s.Inodes[id];
            created = true;
        }

        // Every source stream replaces the destination stream of the same name (objects shared); other destination streams stay.
        ImmutableSortedDictionary<string, StreamRecord> streams = to.Streams;
        var dropped = new List<ObjectId>();
        foreach (KeyValuePair<string, StreamRecord> pair in from.Streams)
        {
            StreamRecord copy = MaterializedStream(s, c.Materialized, c.LeaderStaging, pair.Key, pair.Value) with
            {
                Revision = ctx.Index,
                CreatedIndex = streams.TryGetValue(pair.Key, out StreamRecord? old) ? old.CreatedIndex : ctx.Index,
            };
            if (old is not null)
                dropped.AddRange(Dropped(old, copy));
            streams = streams.SetItem(pair.Key, copy);
        }

        if (streams.Count - (streams.ContainsKey(DfsStreamPath.DefaultStreamName) ? 1 : 0) > DfsStreamPath.MaxStreamsPerInode)
            throw Fail(DfsErrorCode.LimitExceeded, "The copy would exceed the named-stream limit of the destination.",
                LimitDetail("MaxStreamsPerInode", DfsStreamPath.MaxStreamsPerInode, streams.Count - 1));
        InodeRecord updated = Touch(to with { Streams = streams }, ctx);
        s = Put(s, updated);
        s = Dereference(s, dropped, ctx.Index);
        (s, SnapshotInfoData? snapshot) = SnapshotInSameEntry(s, ctx, c.SnapshotName);
        return (s, VolumeApplyResult.Ok(new CopyResultMessage(new InodeId(updated.Id), updated.Revision, from.Revision, created, s.TokenAt(ctx.Index), snapshot)));
    }
}
