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
        bool skipFencing = ctx.Op is { } op && (op.Flags & CommandFlags.SkipFencing) != 0;
        if (!s.Handles.TryGetValue(handleId, out HandleRecord? h))
        {
            // Negative control (MUT-001): the mutant skips the whole writer-fencing check, handle validity included, and
            // publishes through the ended handle's stream the leader remembered for it.
            if (skipFencing && s.IssuedHandle(handleId) && ctx.Command.FencingFallback is { } fallback)
            {
                return new HandleRecord(handleId, fallback.Inode, fallback.Stream, 0, principal, FileMode.Open, FileAccess.Write, FileShare.Read, true, generation,
                    HandleView.Live, null, 0, 0, DfsFileSystemRights.WriteData, false);
            }

            throw !s.IssuedHandle(handleId)
                ? Fail(DfsErrorCode.InvalidHandle, $"Handle {handleId} was never issued by this volume.")
                : Fail(DfsErrorCode.LeaseLost, $"Handle {handleId} is no longer active (closed or its lease expired).");
        }

        if (!string.Equals(h.Principal, principal, StringComparison.Ordinal))
            throw Fail(DfsErrorCode.InvalidHandle, $"Handle {handleId} belongs to another principal.");
        if (!h.IsWriter)
            throw Fail(DfsErrorCode.AccessDenied, $"Handle {handleId} was not opened for writing.");
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
        if (spec.DerivedFromRevision != 0 && !ctx.Replay && (before?.Revision ?? 0) != spec.DerivedFromRevision)
        {
            throw Fail(DfsErrorCode.WriterConflict,
                $"The stream changed (revision {before?.Revision ?? 0}) after the leader prepared the publication from revision {spec.DerivedFromRevision}; retry.");
        }

        // Receipts and the representation were judged against a policy (LAY-002): a policy change committed in between
        // must not let the publication through under a stronger policy it was never checked against.
        if (spec.Policy is { } planned && !string.Equals(planned, inode.PolicyName, StringComparison.Ordinal))
        {
            throw Fail(DfsErrorCode.WriterConflict,
                $"The storage policy of the file changed to '{inode.PolicyName}' after the leader validated the publication against '{planned}'; retry.");
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
            next = CloseLeaderStaging(next, spec, ctx.Index);

            // The staging-to-live transition (FSD §10.3 step 4): a path publication consumes the unbound staging session its
            // objects were uploaded under (a whole-file helper's), so nothing it staged stays protected after the commit.
            // Sessions bound to a handle or a transaction live on with their owner.
            if (result.IsSuccess && spec.Staging != 0 && next.Staging.TryGetValue(spec.Staging, out StagingRecord? request)
                && request is { IsOpen: true, Handle: 0, Transaction.IsEmpty: true })
            {
                next = CloseStaging(next, request, ctx.Index);
            }

            return (next, result);
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
        // The base is a revision the client observed (the reported revision of its open or last publication, which also
        // advances for metadata changes): the publication conflicts only when the stream's content changed after it.
        if (c.BaseRevision != 0 && c.BaseRevision < stream.Revision)
            throw Fail(DfsErrorCode.WriterConflict, $"Base revision {c.BaseRevision} is stale (the stream changed at {stream.Revision}).");
        if (h.IsAppend)
        {
            // An append handle only adds a suffix (FSD 6.1): it never truncates or rewrites existing bytes, also when the
            // principal holds AppendData but not WriteData (ACL-05). A leader-derived spec was checked before materializing.
            string? error = AppendHandleError(stream.Length, c.Spec.TruncateTo, c.Spec.NewLength, c.Spec.DerivedFromRevision == 0 ? c.Spec.Changes : []);
            if (error is not null)
                throw Fail(DfsErrorCode.InvalidArgument, error);
        }

        (s, InodeRecord updated, StreamRecord after) = Publish(s, ctx, inode, h.Stream, c.Spec.TruncateTo, c.Spec.Changes, c.Spec.NewLength, c.Spec);
        (s, SnapshotInfoData? snapshot) = SnapshotInSameEntry(s, ctx, c.SnapshotName);
        return (s, VolumeApplyResult.Ok(new CommitFileResultMessage(new InodeId(updated.Id), updated.RevisionOf(h.Stream), after.Length, s.TokenAt(ctx.Index),
            snapshot)));
    }

    /// <summary>
    /// Why a publication through an append handle is not a pure append to the stream of <paramref name="length"/> bytes (it
    /// truncates, shortens, or changes a byte below the current end), or null.
    /// </summary>
    internal static string? AppendHandleError(long length, long truncateTo, long newLength, IEnumerable<StreamChange> changes)
    {
        if (truncateTo >= 0 && truncateTo < length)
            return "An append handle cannot truncate.";
        if (newLength < length)
            return $"An append handle cannot shorten the stream ({newLength} < {length}).";
        foreach (StreamChange change in changes)
        {
            if (change.Offset < length)
                return $"An append handle writes only at the end of the stream; [{change.Offset}, +{change.Length}) lies below it ({length}).";
        }

        return null;
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
        if (c.ExpectedRevision != 0)
        {
            // A conditional replacement needs the expected revision to be current; an absent file or stream has none, so a
            // conditional write never creates one.
            if (created)
                throw Fail(DfsErrorCode.WriterConflict, $"'{path}' does not exist, so it is not at revision {c.ExpectedRevision}.");
            ulong current = inode.RevisionOf(stream);
            if (current != c.ExpectedRevision)
                throw Fail(DfsErrorCode.WriterConflict, $"'{path}' is at revision {current}, not {c.ExpectedRevision}.");
        }

        RequireImplicitWriter(s, ctx, new StreamKey(inode.Id, stream), $"write '{path}'");
        (s, InodeRecord updated, StreamRecord after) = Publish(s, ctx, inode, stream, 0, c.Spec.Changes, c.Spec.NewLength, c.Spec);
        (s, SnapshotInfoData? snapshot) = SnapshotInSameEntry(s, ctx, c.SnapshotName);
        return (s, VolumeApplyResult.Ok(new WriteAllResultMessage(new InodeId(updated.Id), updated.RevisionOf(stream), after.Length, created, s.TokenAt(ctx.Index),
            snapshot)));
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
            RequireImplicitWriter(s, ctx, new StreamKey(inode.Id, stream), $"append to '{path}'");
        }

        StreamRecord? before = inode.GetStream(stream);
        long oldLength = before?.Length ?? 0;
        long appended = c.Spec.NewLength;
        if (appended < 0 || oldLength > long.MaxValue - appended)
            throw Fail(DfsErrorCode.InvalidArgument, "The appended length is invalid or overflows the file length.");
        IReadOnlyList<StreamChange> changes;
        long newLength;
        if (c.Absolute && (c.Spec.DerivedFromRevision != 0 || oldLength == 0))
        {
            // The leader materialized the whole result from the base revision (checked through DerivedFromRevision), or
            // planned it against an absent stream that is still empty (absolute and relative offsets coincide).
            changes = c.Spec.Changes;
            newLength = c.Spec.NewLength;
            if (newLength < oldLength)
                throw Fail(DfsErrorCode.InvalidArgument, "A materialized append cannot shorten the stream.");
        }
        else
        {
            // Relative changes; or an absolute plan made against an absent stream (no base revision to check) that another
            // publication created and filled before this entry: the planned content is exactly the suffix, so it is appended
            // at the current end instead of overwriting what was committed meanwhile (an append is never lost). When the
            // representation the leader planned no longer fits the stream as it is now (it became inline meanwhile, or the
            // inline result would outgrow the threshold), the append is a retryable conflict with no effect.
            if (!RepresentationHolds(before, oldLength + appended, c.Spec.FinalInline, s.Config.InlineThreshold))
            {
                throw Fail(DfsErrorCode.WriterConflict,
                    "The stream changed concurrently and the append can no longer be stored as the leader prepared it; retry.");
            }

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
        return (s, VolumeApplyResult.Ok(new AppendResultMessage(new InodeId(updated.Id), updated.RevisionOf(stream), oldLength, after.Length, created,
            s.TokenAt(ctx.Index), snapshot)));
    }

    /// <summary>
    /// True when appending a planned suffix to <paramref name="before"/> keeps the planned representation: an inline result
    /// needs an inline (or absent) base and a final length within the threshold; an extent result a base without inline bytes.
    /// </summary>
    private static bool RepresentationHolds(StreamRecord? before, long newLength, bool finalInline, int inlineThreshold)
    {
        if (finalInline)
            return newLength <= inlineThreshold && (before is null || before.IsInline);
        return before is not { IsInline: true, Length: > 0 };
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
    /// The stream as stored under a stronger policy: a leader-materialized inline stream becomes its extents (LAY-002), and a
    /// copied extent stream uses the leader's re-placed objects where the shared ones do not meet the destination's policy or
    /// pool — provided the source is still at the materialized revision; other streams are returned unchanged.
    /// </summary>
    internal static StreamRecord MaterializedStream(VolumeState s, ImmutableArray<StreamMaterialization> materialized, ulong leaderStaging, string name,
        StreamRecord source, bool replay = false)
    {
        StreamMaterialization? m = materialized.IsDefault ? null : materialized.FirstOrDefault(x => string.Equals(x.Stream, name, StringComparison.Ordinal));
        if (m is null)
            return source;
        if (!replay && source.Revision != m.SourceRevision)
            throw Fail(DfsErrorCode.WriterConflict, $"Stream '{name}' changed after the leader stored its bytes as objects; retry.");
        if (!s.Staging.TryGetValue(leaderStaging, out StagingRecord? staging) || !staging.IsOpen)
            throw Fail(DfsErrorCode.InvalidArgument, $"The leader staging session {leaderStaging} is not open.");
        if (!source.IsInline)
        {
            // Re-placed objects: the same layout, each extent either unchanged or backed by a leader-stored copy.
            if (m.Extents.Count != source.Extents.Count)
                throw Fail(DfsErrorCode.InvalidArgument, $"The re-placed extents of stream '{name}' do not match its layout.");
            for (int i = 0; i < m.Extents.Count; i++)
            {
                ExtentRef was = source.Extents[i];
                ExtentRef now = m.Extents[i];
                bool unchanged = now.Object == was.Object && now.ObjectOffset == was.ObjectOffset;
                if (now.Offset != was.Offset || now.Length != was.Length
                    || (!unchanged && (now.Staging.Value != leaderStaging || now.ObjectOffset < 0 || now.ObjectOffset > now.ObjectLength - now.Length)))
                {
                    throw Fail(DfsErrorCode.InvalidArgument, $"The re-placed extents of stream '{name}' do not match its layout.");
                }
            }

            return source with { Extents = m.Extents };
        }

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

    /// <summary>
    /// Which streams a copy reads (Addendum A, ADS-04): a plain source copied to a new plain destination copies the default
    /// stream and every named stream; otherwise exactly one stream is copied — the addressed source stream (default for a plain
    /// path) into the addressed destination stream (default for a plain path), and the destination's other streams stay.
    /// </summary>
    internal static (string SourceFile, string? SourceStream, string DestinationFile, string? DestinationStream) SplitCopyPaths(string source, string destination)
    {
        (string sourceFile, string? sourceStream) = DfsStreamPath.Split(source);
        (string destinationFile, string? destinationStream) = DfsStreamPath.Split(destination);
        return (sourceFile, sourceStream, destinationFile, destinationStream);
    }

    private static (VolumeState, VolumeApplyResult) CopyCore(VolumeState s, ApplyContext ctx, CopyCommand c)
    {
        RequireGrant(ctx, DfsPermissions.Write, "copy");
        RequireGrant(ctx, DfsPermissions.Read, "copy");
        string source = NormalizePath(c.Source);
        string destination = NormalizePath(c.Destination);
        (string sourceFile, string? sourceStreamName, string destinationFile, string? destinationStreamName) = SplitCopyPaths(source, destination);
        string sourceStream = sourceStreamName ?? DfsStreamPath.DefaultStreamName;
        string destinationStream = destinationStreamName ?? DfsStreamPath.DefaultStreamName;
        if (DfsPath.IsRoot(destinationFile))
            throw Fail(destinationStreamName is null ? DfsErrorCode.IsADirectory : DfsErrorCode.InvalidPath, "Nothing can be copied onto the root directory.");

        var view = new NamespaceView(s.Inodes);
        Resolution src = view.Resolve(sourceFile);
        RequireTraverse(s, ctx, src);
        if (!src.Found)
            throw Fail(DfsErrorCode.FileNotFound, $"'{source}' does not exist.");
        InodeRecord from = src.Target!;
        if (from.IsDirectory && sourceStreamName is null)
            throw Fail(DfsErrorCode.IsADirectory, $"'{source}' is a directory.");
        if (!from.Streams.ContainsKey(sourceStream))
            throw Fail(DfsErrorCode.FileNotFound, $"Stream '{source}' does not exist.");
        ulong sourceRevision = from.RevisionOf(sourceStream);
        if (c.ExpectedSourceRevision != 0 && c.ExpectedSourceRevision != sourceRevision)
            throw Fail(DfsErrorCode.WriterConflict, $"'{source}' is at revision {sourceRevision}, not {c.ExpectedSourceRevision}.");
        Require(ctx, src.ChainNearestFirst(), DfsFileSystemRights.ReadData, $"read '{source}'");

        Resolution dst = view.Resolve(destinationFile);
        RequireTraverse(s, ctx, dst);
        if (!dst.ParentExists)
            throw Fail(DfsErrorCode.DirectoryNotFound, $"The directory of '{destination}' does not exist.");
        bool wholeFile = sourceStreamName is null && destinationStreamName is null && !dst.Found;

        // The source streams read must be shareable for reading (sharing is per stream, ADS-05).
        IEnumerable<HandleRecord> readers = wholeFile ? HandlesOf(s, from.Id) : HandlesOf(s, new StreamKey(from.Id, sourceStream));
        if (readers.FirstOrDefault(h => (h.Share & FileShare.Read) == 0) is { } exclusive)
            throw Fail(DfsErrorCode.SharingViolation, $"'{source}' is open by handle {exclusive.Id} without FileShare.Read.");

        StreamRecord CopyOf(string name, StreamRecord stream, StreamRecord? replaced)
            => MaterializedStream(s, c.Materialized, c.LeaderStaging, name, stream, ctx.Replay) with
            {
                Revision = ctx.Index,
                CreatedIndex = replaced?.CreatedIndex ?? ctx.Index,
            };

        bool created;
        InodeRecord to;
        ImmutableSortedDictionary<string, StreamRecord> streams;
        var dropped = new List<ObjectId>();
        if (dst.Found)
        {
            to = dst.Target!;
            if (to.Id == from.Id)
                throw Fail(DfsErrorCode.InvalidArgument, $"'{source}' cannot be copied onto its own file '{destination}'.");
            if (to.IsDirectory && destinationStreamName is null)
                throw Fail(DfsErrorCode.IsADirectory, $"'{destination}' is a directory.");
            StreamRecord? replaced = to.GetStream(destinationStream);
            if (replaced is not null && !c.Overwrite)
                throw Fail(DfsErrorCode.AlreadyExists, $"'{destination}' already exists.");
            Require(ctx, dst.ChainNearestFirst(), DfsFileSystemRights.WriteData,
                replaced is null ? $"create stream '{destination}'" : $"overwrite '{destination}'");
            if (replaced is null && to.NamedStreamCount >= DfsStreamPath.MaxStreamsPerInode)
            {
                throw Fail(DfsErrorCode.LimitExceeded, $"'{destinationFile}' already has {to.NamedStreamCount} named streams.",
                    LimitDetail("MaxStreamsPerInode", DfsStreamPath.MaxStreamsPerInode, to.NamedStreamCount + 1));
            }

            RequireImplicitWriter(s, ctx, new StreamKey(to.Id, destinationStream), $"copy onto '{destination}'");

            // Only the addressed destination stream is replaced; a copy source's named streams are not merged (ADS-011).
            StreamRecord copy = CopyOf(sourceStream, from.Streams[sourceStream], replaced);
            if (replaced is not null)
                dropped.AddRange(Dropped(replaced, copy));
            streams = to.Streams.SetItem(destinationStream, copy);
            created = replaced is null;
        }
        else
        {
            Require(ctx, dst.ParentChainNearestFirst(), DfsFileSystemRights.CreateFiles, $"create '{destination}'");
            (s, ulong id) = AllocateInode(s);
            to = NewInode(s, ctx, id, InodeKind.File, dst.Parent!, dst.Name, c.StoragePolicy ?? from.PolicyName);
            streams = to.Streams;
            if (wholeFile)
            {
                // A new file receives the default stream and every named stream of the source (ADS-010).
                foreach (KeyValuePair<string, StreamRecord> pair in from.Streams)
                    streams = streams.SetItem(pair.Key, CopyOf(pair.Key, pair.Value, null));
            }
            else
            {
                // A new file holding the copied stream (as its default stream, or as the addressed named stream beside an
                // empty default stream, ADS-003).
                streams = streams.SetItem(destinationStream, CopyOf(sourceStream, from.Streams[sourceStream], null));
            }

            created = true;
        }

        InodeRecord updated = Touch(to with { Streams = streams }, ctx);
        s = created && !dst.Found ? Link(s, ctx, dst.Parent!, updated) : Put(s, updated);
        updated = s.Inodes[updated.Id];
        s = Dereference(s, dropped, ctx.Index);
        (s, SnapshotInfoData? snapshot) = SnapshotInSameEntry(s, ctx, c.SnapshotName);
        return (s, VolumeApplyResult.Ok(new CopyResultMessage(new InodeId(updated.Id), updated.RevisionOf(destinationStream), sourceRevision, created,
            s.TokenAt(ctx.Index), snapshot)));
    }
}
