using System.Collections.Immutable;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Control;
using Tedd.Tdfs.Server.Volumes.Commands;
using Tedd.Tdfs.Server.Volumes.State;

namespace Tedd.Tdfs.Server.Volumes.Apply;

/// <summary>
/// Restore of a snapshot into a new volume (PROTOCOL.md §8.5, ARCHITECTURE §5.7). Source side: <c>RestoreBegin</c> accepts the
/// request (an intent holding the snapshot root) and <c>RestoreFinish</c> records its outcome under the request's operation
/// id. Target side: <c>RestoreChunk</c> opens the restore (allocating the staging session the copies are stored under) and
/// stages inode records, and <c>RestoreComplete</c> installs them atomically as the namespace.
/// </summary>
internal sealed partial class VolumeStateMachine
{
    /// <summary>Most restores of one volume's snapshots accepted at the same time.</summary>
    public const int MaxConcurrentRestores = 16;

    private static (VolumeState, VolumeApplyResult) ApplyRestoreBegin(VolumeState s, ApplyContext ctx, RestoreBeginCommand c)
    {
        OperationHeader op = ctx.Op ?? throw Fail(DfsErrorCode.AccessDenied, "A restore needs an authenticated principal.");
        RequireGrant(ctx, DfsPermissions.VolumeAdmin, "restore snapshot");
        if (!op.IsAdmin)
            throw Fail(DfsErrorCode.AccessDenied, $"Principal '{op.Principal}' must be a cluster administrator to restore a snapshot into a new volume.");
        if (c.Operation.IsEmpty)
            throw Fail(DfsErrorCode.InvalidArgument, "RestoreSnapshot requests must carry an operation id (OUT-001).");

        // The request's own operation id goes through the gate here, so an id with a stored outcome (or a retired one) is
        // never accepted as a new restore.
        OperationHeader request = op with { Id = c.Operation, Operation = DfsOperationKind.RestoreSnapshot, PayloadHash = c.PayloadHash };
        GateDecision gate = OperationGate.Check(s, request);
        switch (gate.Kind)
        {
            case GateKind.Stored:
                return (s, VolumeApplyResult.Ok(null, gate.Outcome));
            case GateKind.Expired:
                throw Fail(DfsErrorCode.OperationExpired, gate.Message);
            case GateKind.Mismatch:
                throw Fail(DfsErrorCode.OperationIdMismatch, gate.Message);
            case GateKind.Invalid:
                throw Fail(DfsErrorCode.InvalidArgument, gate.Message);
        }

        if (s.Restores.TryGetValue(c.Operation, out RestoreIntentRecord? existing))
        {
            if (existing.PayloadHash != c.PayloadHash || !string.Equals(existing.Principal, op.Principal, StringComparison.Ordinal))
                throw Fail(DfsErrorCode.OperationIdMismatch, $"Operation {c.Operation} is in flight for a different restore.");
            return (s, VolumeApplyResult.Ok(null, existing));
        }

        if (!s.Snapshots.TryGetValue(c.Snapshot, out SnapshotRecord? snapshot))
            throw Fail(DfsErrorCode.SnapshotNotFound, $"Snapshot '{c.Snapshot}' does not exist.");
        if (ControlStateMachine.GetVolumeNameError(c.NewVolumeName) is { } nameError)
            throw Fail(DfsErrorCode.InvalidArgument, nameError);
        if (c.StoragePool is not null && DfsPoolNames.GetError(c.StoragePool) is { } poolError)
            throw Fail(DfsErrorCode.InvalidArgument, poolError);
        if (s.Restores.Count >= MaxConcurrentRestores)
            throw new ApplyFailure(DfsErrorCode.LimitExceeded, $"At most {MaxConcurrentRestores} restores of this volume may run at the same time.",
                new ErrorDetailData(LimitName: "MaxConcurrentRestores", Limit: MaxConcurrentRestores, Requested: s.Restores.Count + 1));

        var intent = new RestoreIntentRecord(c.Operation, c.PayloadHash, op.Principal, snapshot.Name, snapshot.Id, snapshot.BoundaryIndex, c.NewVolumeName,
            c.StoragePool, snapshot.Root, ctx.Index, ctx.Now);
        return (s with { Restores = s.Restores.Add(c.Operation, intent) }, VolumeApplyResult.Ok(null, intent));
    }

    private static (VolumeState, VolumeApplyResult) ApplyRestoreFinish(VolumeState s, ApplyContext ctx, RestoreFinishCommand c)
    {
        if (ctx.Op is { HasOperation: true } op && op.Id != c.Operation)
            throw Fail(DfsErrorCode.InvalidArgument, $"The outcome of restore {c.Operation} cannot be recorded under operation {op.Id}.");
        if (!s.Restores.TryGetValue(c.Operation, out RestoreIntentRecord? intent))
            throw Fail(DfsErrorCode.InvalidArgument, $"No restore {c.Operation} is in progress.");
        s = s with { Restores = s.Restores.Remove(c.Operation) };
        if (c.Code != DfsErrorCode.None)
            return (s, VolumeApplyResult.Fail(c.Code, c.Message, DetailOf(c.Reply)));

        IWireMessage reply;
        try
        {
            reply = MessageCodec.Decode(c.Reply.Span);
        }
        catch (DfsProtocolException ex)
        {
            return (s, VolumeApplyResult.Fail(DfsErrorCode.Internal, "The recorded restore reply is malformed: " + ex.Message));
        }

        return reply is RestoreSnapshotResultMessage
            ? (s, VolumeApplyResult.Ok(reply, intent))
            : (s, VolumeApplyResult.Fail(DfsErrorCode.Internal, $"The recorded restore reply is a {reply.Type}, not a RestoreSnapshotResult."));
    }

    /// <summary>The error detail of a recorded failure reply (null when there is none or it does not decode).</summary>
    private static ErrorDetailData? DetailOf(ReadOnlyMemory<byte> reply)
    {
        if (reply.IsEmpty)
            return null;
        try
        {
            return MessageCodec.Decode(reply.Span) is ErrorReplyMessage error ? error.Detail : null;
        }
        catch (DfsProtocolException)
        {
            return null;
        }
    }

    private static (VolumeState, VolumeApplyResult) ApplyRestoreChunk(VolumeState s, ApplyContext ctx, RestoreChunkCommand c)
    {
        if (string.IsNullOrEmpty(c.Source))
            throw Fail(DfsErrorCode.InvalidArgument, "A restore needs a source.");
        RestoreRecord restore;
        if (s.Restore is { } existing)
        {
            if (!string.Equals(existing.Source, c.Source, StringComparison.Ordinal))
                throw Fail(DfsErrorCode.InvalidArgument, $"The volume is being restored from '{existing.Source}'.");
            if (c.Staging != 0 && c.Staging != existing.Staging)
                throw Fail(DfsErrorCode.InvalidArgument, $"Staging session {c.Staging} is not the restore's staging session {existing.Staging}.");
            if (existing.Complete)
            {
                if (!OrEmpty(c.Inodes).IsEmpty)
                    throw Fail(DfsErrorCode.InvalidArgument, "The restore of this volume is already complete.");
                return (s, VolumeApplyResult.Ok(null, existing));
            }

            restore = existing;
        }
        else
        {
            if (c.Staging != 0)
                throw Fail(DfsErrorCode.InvalidArgument, "No restore is open on this volume.");
            if (s.Root.Children.Count > 0 || s.Inodes.Count > 1 || s.Handles.Count > 0 || s.Snapshots.Count > 0)
                throw Fail(DfsErrorCode.InvalidArgument, "Only a new, empty volume can be restored into.");
            var staging = new StagingRecord(s.NextStagingId, 0, 0, default, string.Empty, StagingState.Open, ctx.Index, 0, ctx.Now);
            s = s with { NextStagingId = staging.Id + 1, Staging = s.Staging.Add(staging.Id, staging) };
            restore = new RestoreRecord(c.Source, false, ImmutableSortedDictionary<ulong, InodeRecord>.Empty, staging.Id, 0);
        }

        ImmutableSortedDictionary<ulong, InodeRecord> staged = restore.Staged;
        foreach (InodeRecord n in OrEmpty(c.Inodes))
        {
            if (n.Id == 0)
                throw Fail(DfsErrorCode.InvalidArgument, "Inode id 0 is reserved.");

            // Staged records get this volume's revision space now: their source revisions could equal the version key of
            // one of this volume's own records (the empty root is (1, 0)), and checkpoints store each (id, revision) once.
            staged = staged.SetItem(n.Id, n with { Revision = ctx.Index });
        }

        restore = restore with { Staged = staged };
        return (s with { Restore = restore }, VolumeApplyResult.Ok(null, restore));
    }

    private static (VolumeState, VolumeApplyResult) ApplyRestoreComplete(VolumeState s, ApplyContext ctx, RestoreCompleteCommand c)
    {
        if (s.Restore is not { } restore || !string.Equals(restore.Source, c.Source, StringComparison.Ordinal))
            throw Fail(DfsErrorCode.InvalidArgument, "No restore from this source is in progress.");
        if (restore.Complete)
            return (s, VolumeApplyResult.Ok(null, restore));
        if (restore.Staged.Count != c.InodeCount)
            throw Fail(DfsErrorCode.Integrity, $"The restore staged {restore.Staged.Count} of {c.InodeCount} inodes.");
        if (ValidateTree(restore.Staged) is { } error)
            throw Fail(DfsErrorCode.Integrity, "The restored namespace is inconsistent: " + error);

        // Restored versions get this volume's revision space: (id, revision) stays a unique version key.
        var inodes = ImmutableSortedDictionary.CreateBuilder<ulong, InodeRecord>();
        ulong next = InodeRecord.RootId + 1;
        foreach (InodeRecord n in restore.Staged.Values)
        {
            var streams = ImmutableSortedDictionary.CreateBuilder<string, StreamRecord>(n.Streams.KeyComparer);
            foreach (KeyValuePair<string, StreamRecord> pair in n.Streams)
                streams[pair.Key] = pair.Value with { Revision = ctx.Index, CreatedIndex = ctx.Index };
            inodes[n.Id] = n with { Revision = ctx.Index, MetadataRevision = ctx.Index, CreatedIndex = ctx.Index, Streams = streams.ToImmutable() };
            next = Math.Max(next, n.Id + 1);
        }

        RestoreRecord done = restore with { Complete = true, Staged = ImmutableSortedDictionary<ulong, InodeRecord>.Empty, CompletedIndex = ctx.Index };
        s = s with
        {
            Inodes = inodes.ToImmutable(),
            NextInodeId = Math.Max(s.NextInodeId, next),
            Restore = done,
        };
        if (s.Staging.TryGetValue(restore.Staging, out StagingRecord? staging))
            s = CloseStaging(s, staging, ctx.Index);
        return (s, VolumeApplyResult.Ok(null, done));
    }

    /// <summary>
    /// Checks that staged inodes form one tree under the root: the root is a directory, every other inode is listed under
    /// its name by its parent directory, and every listed child was staged. Null when consistent.
    /// </summary>
    private static string? ValidateTree(ImmutableSortedDictionary<ulong, InodeRecord> staged)
    {
        if (!staged.TryGetValue(InodeRecord.RootId, out InodeRecord? root) || root.Kind != InodeKind.Directory)
            return "the root directory is missing.";
        foreach (InodeRecord n in staged.Values)
        {
            if (n.Kind == InodeKind.Directory)
            {
                foreach (KeyValuePair<string, ulong> child in n.Children)
                {
                    if (!staged.TryGetValue(child.Value, out InodeRecord? c) || c.ParentId != n.Id || !string.Equals(c.Name, child.Key, StringComparison.Ordinal))
                        return $"directory {n.Id} lists '{child.Key}' as inode {child.Value}, which was not staged under that name.";
                }
            }
            else if (n.Children.Count > 0)
            {
                return $"inode {n.Id} has children but is not a directory.";
            }

            if (n.Id == InodeRecord.RootId)
                continue;
            if (!staged.TryGetValue(n.ParentId, out InodeRecord? parent) || !parent.Children.TryGetValue(n.Name, out ulong listed) || listed != n.Id)
                return $"inode {n.Id} ('{n.Name}') is not listed by its parent {n.ParentId}.";
        }

        return null;
    }
}
