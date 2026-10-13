using System.Buffers;
using System.Collections.Immutable;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Volumes.Apply;
using Tedd.Tdfs.Server.Volumes.Commands;
using Tedd.Tdfs.Server.Volumes.State;
using static Tedd.Tdfs.Tests.Unit.Internal.Volumes.SmDriver;

namespace Tedd.Tdfs.Tests.Unit.Internal.Volumes;

/// <summary>
/// Restore commands of the volume state machine: the source-side intent (<c>RestoreBegin</c>/<c>RestoreFinish</c>, gated by
/// the request's operation id, keeping the snapshot root reachable) and the target-side install (<c>RestoreChunk</c>/
/// <c>RestoreComplete</c>: restore staging session, atomic install of one consistent tree, idempotency).
/// </summary>
public sealed class VolumeStateMachineRestoreTests
{
    private static readonly UInt128 Payload = new(0xAB, 0xCD);

    private static (SmDriver Driver, ulong Session) SourceWithSnapshot()
    {
        var d = new SmDriver();
        ulong s = d.OpenSession();
        d.Ok<CreateDirectoryResultMessage>(new CreateDirectoryCommand("/data", true, null) { Op = Op() });
        OpenHandleResultMessage h = d.Open(s, "/data/f", FileMode.CreateNew, FileAccess.Write, FileShare.Read, 1);
        ulong st = d.Staging(s, h.Handle.Value);
        d.Ok<CommitFileResultMessage>(new CommitFileCommand(h.Handle.Value, h.WriterGeneration, 0, Spec(9000, st, false, -1, DataChange(0, 9000, Obj(1), st)), null)
        {
            Op = Op(),
        });
        d.Ok<CloseHandleResultMessage>(new CloseHandleCommand(h.Handle.Value, h.WriterGeneration) { Op = Op() });
        d.Ok<CreateSnapshotResultMessage>(new CreateSnapshotCommand("s1", DfsConsistencyLabel.FilesystemConsistent, [], null, false) { Op = Op() });
        return (d, s);
    }

    private static RestoreBeginCommand Begin(ulong session, ulong sequence, string snapshot = "s1", UInt128? payload = null)
        => new(new DfsOperationId(session, sequence), payload ?? Payload, snapshot, "restored", null);

    [Fact]
    public void Begin_RecordsAnIntentThatKeepsTheSnapshotReachable()
    {
        (SmDriver d, ulong s) = SourceWithSnapshot();
        var intent = Assert.IsType<RestoreIntentRecord>(d.Apply(Begin(s, 7) with { Op = Op(admin: true) }).Value);
        Assert.Equal(("s1", "restored", "alice"), (intent.Snapshot, intent.NewVolumeName, intent.Principal));
        Assert.Same(intent, d.State.Restores[new DfsOperationId(s, 7)]);

        // The same request again finds the intent; another payload under the id is a mismatch.
        Assert.Equal(intent, d.Apply(Begin(s, 7) with { Op = Op(admin: true) }).Value);
        d.Fails(DfsErrorCode.OperationIdMismatch, Begin(s, 7, payload: new UInt128(1, 1)) with { Op = Op(admin: true) });

        // Deleting the snapshot and overwriting the file keeps the restored objects reachable while the restore runs.
        d.Ok<DeleteSnapshotResultMessage>(new DeleteSnapshotCommand("s1") { Op = Op() });
        d.Ok<WriteAllResultMessage>(new WriteAllCommand("/data/f", Spec(1, 0, true, 0, Inline(0, [9])), null, null, false, 0) { Op = Op(session: s) });
        Assert.Contains(Obj(1), VolumeStateMachine.Reachable(d.State));
    }

    [Fact]
    public void Begin_RequiresVolumeAdminAndClusterAdministrationAndAnExistingSnapshot()
    {
        (SmDriver d, ulong s) = SourceWithSnapshot();
        d.Fails(DfsErrorCode.AccessDenied, Begin(s, 2) with { Op = Op() });
        d.Fails(DfsErrorCode.AccessDenied, Begin(s, 2) with { Op = Op(admin: true, grants: DfsPermissions.All & ~DfsPermissions.VolumeAdmin) });
        d.Fails(DfsErrorCode.SnapshotNotFound, Begin(s, 2, "nope") with { Op = Op(admin: true) });
        d.Fails(DfsErrorCode.InvalidArgument, new RestoreBeginCommand(new DfsOperationId(s, 2), Payload, "s1", "bad/name", null) { Op = Op(admin: true) });
        d.Fails(DfsErrorCode.InvalidArgument, new RestoreBeginCommand(new DfsOperationId(999, 2), Payload, "s1", "restored", null) { Op = Op(admin: true) });
        Assert.Empty(d.State.Restores);
    }

    [Fact]
    public void Finish_RecordsTheOutcomeUnderTheRequestsOperationId()
    {
        (SmDriver d, ulong s) = SourceWithSnapshot();
        var id = new DfsOperationId(s, 9);
        d.Apply(Begin(s, 9) with { Op = Op(admin: true) });
        var entry = new VolumeEntry(new VolumeId(Guid.Parse("11111111-2222-3333-4444-555555555555")), "restored", 1, WireVolumeState.Active, [Node], [], Node,
            new DfsMetadataPolicy(), DfsStoragePolicy.Default, new Dictionary<string, DfsStoragePolicy>(), 4096, new DfsVolumeLimits());
        var reply = new RestoreSnapshotResultMessage(entry, new CommitToken(ClusterId, entry.Id, 1, 42));
        OperationHeader header = Op(session: s, sequence: 9, kind: DfsOperationKind.RestoreSnapshot, payload: Payload, admin: true);
        var finished = d.Ok<RestoreSnapshotResultMessage>(new RestoreFinishCommand(id, DfsErrorCode.None, string.Empty, MessageCodec.Encode(reply)) { Op = header });
        Assert.Equal(reply.LastCommit, finished.LastCommit);
        Assert.Empty(d.State.Restores);

        // A retried request (or a second RestoreFinish) gets the stored outcome; a new Begin under the id reports it.
        VolumeApplyResult replay = d.Apply(new RestoreFinishCommand(id, DfsErrorCode.Internal, "late", ReadOnlyMemory<byte>.Empty) { Op = header });
        Assert.True(replay.Replayed);
        Assert.Equal(reply.LastCommit, Assert.IsType<RestoreSnapshotResultMessage>(replay.Reply).LastCommit);
        Assert.IsType<OutcomeRecord>(d.Apply(Begin(s, 9) with { Op = Op(admin: true) }).Value);
        Assert.Empty(d.State.Restores);
    }

    [Fact]
    public void Finish_RecordsFailuresWithTheirDetail()
    {
        (SmDriver d, ulong s) = SourceWithSnapshot();
        var id = new DfsOperationId(s, 3);
        d.Apply(Begin(s, 3) with { Op = Op(admin: true) });
        var error = new ErrorReplyMessage(DfsErrorCode.PlacementUnavailable, "short", id) { Detail = new ErrorDetailData(RequiredCopies: 2, AchievedCopies: 1) };
        OperationHeader header = Op(session: s, sequence: 3, kind: DfsOperationKind.RestoreSnapshot, payload: Payload, admin: true);
        VolumeApplyResult failed = d.Fails(DfsErrorCode.PlacementUnavailable, new RestoreFinishCommand(id, error.Code, error.Message, MessageCodec.Encode(error))
        {
            Op = header,
        });
        Assert.Equal(2, Assert.IsType<ErrorReplyMessage>(failed.Reply).Detail?.RequiredCopies);
        Assert.Empty(d.State.Restores);
        VolumeApplyResult stored = d.Apply(Begin(s, 3) with { Op = Op(admin: true) });
        Assert.Equal(DfsErrorCode.PlacementUnavailable, Assert.IsType<OutcomeRecord>(stored.Value).Code);
        d.Fails(DfsErrorCode.InvalidArgument, new RestoreFinishCommand(new DfsOperationId(s, 4), DfsErrorCode.None, string.Empty, ReadOnlyMemory<byte>.Empty)
        {
            Op = Op(session: s, sequence: 4, admin: true),
        });
    }

    [Fact]
    public void Install_OpensAStagingSessionStagesChunksAndInstallsOneTreeAtomically()
    {
        (SmDriver source, _) = SourceWithSnapshot();
        InodeRecord[] inodes = [.. source.State.Inodes.Values];
        var target = new SmDriver();
        var opened = Assert.IsType<RestoreRecord>(target.Apply(new RestoreChunkCommand("src", 0, [])).Value);
        Assert.True(target.State.Staging[opened.Staging].IsOpen);
        target.Fails(DfsErrorCode.InvalidArgument, new RestoreChunkCommand("other", 0, []));
        target.Fails(DfsErrorCode.InvalidArgument, new RestoreChunkCommand("src", opened.Staging + 5, [inodes[0]]));
        target.Apply(new RestoreChunkCommand("src", opened.Staging, [.. inodes]));
        // Staged objects are reachable (GC keeps them) before they are visible.
        Assert.Contains(Obj(1), VolumeStateMachine.Reachable(target.State));
        Assert.Single(target.State.Inodes);
        target.Fails(DfsErrorCode.Integrity, new RestoreCompleteCommand("src", inodes.Length + 1));

        var done = Assert.IsType<RestoreRecord>(target.Apply(new RestoreCompleteCommand("src", inodes.Length)).Value);
        Assert.True(done.Complete);
        Assert.Equal(target.Index, done.CompletedIndex);
        Assert.False(target.State.Staging[opened.Staging].IsOpen);
        Assert.Equal(inodes.Length, target.State.Inodes.Count);
        Assert.True(target.Exists("/data/f"));

        // Every step is idempotent for the same source once complete.
        Assert.Equal(done, target.Apply(new RestoreCompleteCommand("src", inodes.Length)).Value);
        Assert.Equal(done, target.Apply(new RestoreChunkCommand("src", 0, [])).Value);
        target.Fails(DfsErrorCode.InvalidArgument, new RestoreChunkCommand("src", 0, [inodes[0]]));
    }

    [Fact]
    public void Install_RejectsInconsistentTreesAndNonEmptyVolumes()
    {
        (SmDriver source, _) = SourceWithSnapshot();
        InodeRecord root = source.State.Root;
        var target = new SmDriver();
        target.Apply(new RestoreChunkCommand("src", 0, [root]));
        // The root lists /data, which was never staged.
        target.Fails(DfsErrorCode.Integrity, new RestoreCompleteCommand("src", 1));

        var busy = new SmDriver();
        busy.Ok<CreateDirectoryResultMessage>(new CreateDirectoryCommand("/x", false, null) { Op = Op() });
        busy.Fails(DfsErrorCode.InvalidArgument, new RestoreChunkCommand("src", 0, []));
    }

    [Fact]
    public void Checkpoint_PreservesIntentsAndRestores()
    {
        (SmDriver d, ulong s) = SourceWithSnapshot();
        d.Apply(Begin(s, 5) with { Op = Op(admin: true) });
        d.Apply(new DeleteSnapshotCommand("s1") { Op = Op() });
        byte[] image = d.Checkpoint();
        var restored = new VolumeStateMachine(d.Creation, ClusterId);
        restored.RestoreCheckpoint(new ReadOnlySequence<byte>(image), d.Index, 1);
        RestoreIntentRecord intent = Assert.Single(restored.State.Restores.Values);
        Assert.Equal(d.State.Restores.Values.Single().Root.Keys, intent.Root.Keys);
        var buffer = new ArrayBufferWriter<byte>();
        restored.WriteCheckpoint(buffer);
        Assert.Equal(image, buffer.WrittenSpan.ToArray());

        var target = new SmDriver();
        target.Apply(new RestoreChunkCommand("src", 0, [.. d.State.Inodes.Values]));
        target.Apply(new RestoreCompleteCommand("src", d.State.Inodes.Count));
        byte[] targetImage = target.Checkpoint();
        var copy = new VolumeStateMachine(target.Creation, ClusterId);
        copy.RestoreCheckpoint(new ReadOnlySequence<byte>(targetImage), target.Index, 1);
        Assert.Equal(target.State.Restore, copy.State.Restore! with { Staged = ImmutableSortedDictionary<ulong, InodeRecord>.Empty });
    }
}
