using System.Buffers;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Raft;
using Tedd.Tdfs.Server.Volumes.Apply;
using Tedd.Tdfs.Server.Volumes.Commands;
using Tedd.Tdfs.Server.Volumes.State;
using static Tedd.Tdfs.Tests.Unit.Internal.Volumes.SmDriver;

namespace Tedd.Tdfs.Tests.Unit.Internal.Volumes;

/// <summary>ACLs, snapshots and pins, GC metadata, restore, determinism and checkpoints.</summary>
public sealed class VolumeStateMachineStateTests
{
    private static readonly DfsIdentity Alice = DfsIdentity.User("alice");
    private static readonly DfsIdentity Bob = DfsIdentity.User("bob");

    // ------------------------------------------------------------------------------------------------- ACLs

    [Fact]
    public void Acl_DenyIsHonouredAtApplyWithTheCarriedIdentitySet()
    {
        var d = new SmDriver();
        ulong a = d.OpenSession();
        d.Ok<WriteAllResultMessage>(new WriteAllCommand("/doc", Spec(1, 0, true, 0, Inline(0, [1])), null, null, false, 0) { Op = Op(session: a) });
        var deny = new DfsSecurityDescriptor(Alice, [new DfsAccessRule(Bob, DfsFileSystemRights.ReadData, DfsAccessControlType.Deny)]);
        ulong before = d.Inode("/doc").Revision;
        d.Ok<SetAccessControlResultMessage>(new SetAclCommand("/doc", AclChangeKind.Descriptor, deny, false, false) { Op = Op() });
        Assert.True(d.Inode("/doc").Revision > before);

        ulong b = d.OpenSession("bob");
        d.Fails(DfsErrorCode.AccessDenied, OpenCommand(b, "/doc", FileMode.Open, FileAccess.Read, FileShare.ReadWrite, principal: "bob"));
        // The volume grant is the coarse gate: without the Read grant even an allowed ACL is denied (ACL-022).
        d.Fails(DfsErrorCode.AccessDenied, new OpenHandleCommand("/doc", FileMode.Open, FileAccess.Read, FileShare.ReadWrite, FileOptions.None, false, null, a,
            TimeSpan.FromSeconds(1)) { Op = Op(session: a, grants: DfsPermissions.Write) });
        // Bob may still create elsewhere (inherited Everyone FullControl).
        d.Open(b, "/bobs", FileMode.CreateNew, FileAccess.Write, FileShare.Read, 1, "bob");
        Assert.Equal(Bob, d.Inode("/bobs").Owner);
    }

    [Fact]
    public void Acl_SetOwnerNeedsTakeOwnershipForSelfAndAdministratorsForOthers()
    {
        var d = new SmDriver();
        ulong a = d.OpenSession();
        d.Ok<WriteAllResultMessage>(new WriteAllCommand("/o", Spec(1, 0, true, 0, Inline(0, [1])), null, null, false, 0) { Op = Op(session: a) });
        var protectedAcl = new DfsSecurityDescriptor(Alice, [new DfsAccessRule(Alice, DfsFileSystemRights.FullControl)], AreAccessRulesProtected: true);
        d.Ok<SetAccessControlResultMessage>(new SetAclCommand("/o", AclChangeKind.Descriptor, protectedAcl, false, false) { Op = Op() });

        d.Fails(DfsErrorCode.AccessDenied, new SetOwnerCommand("/o", Bob) { Op = Op("bob") });
        d.Fails(DfsErrorCode.AccessDenied, new SetOwnerCommand("/o", Bob) { Op = Op() });
        d.Ok<SetOwnerResultMessage>(new SetOwnerCommand("/o", Bob) { Op = Op("root", admin: true) });
        Assert.Equal(Bob, d.Inode("/o").Owner);
        d.Fails(DfsErrorCode.InvalidArgument, new SetOwnerCommand("/o", DfsIdentity.CreatorOwner) { Op = Op("root", admin: true) });

        // SetAccess shortcut: OwnerOnly protects the inode for the owner and administrators.
        d.Ok<SetAccessResultMessage>(new SetAclCommand("/o", AclChangeKind.AccessFlags, null, true, false) { Op = Op("bob") });
        FileInfoData info = VolumeStateMachine.Describe(d.State, d.Inode("/o"), "/o", null);
        Assert.True(info.OwnerOnly);
        Assert.Equal("bob", info.Owner);
    }

    [Fact]
    public void SetPolicy_StoresMaterializedInlineStreamsAsExtents()
    {
        var d = new SmDriver();
        ulong s = d.OpenSession();
        d.Ok<WriteAllResultMessage>(new WriteAllCommand("/p", Spec(4, 0, true, 0, Inline(0, [1, 2, 3, 4])), null, null, false, 0) { Op = Op(session: s) });
        d.Ok<WriteAllResultMessage>(new WriteAllCommand("/p:s", Spec(1, 0, true, 0, Inline(0, [5])), null, null, false, 0) { Op = Op(session: s) });
        ulong revision = d.Inode("/p").DefaultStream!.Revision;
        ulong st = d.Staging(0);
        ExtentRef extent = DataChange(0, 4, Obj(77), st).Extent!;
        d.Fails(DfsErrorCode.WriterConflict, new SetPolicyCommand("/p", "pair")
        {
            Op = Op(),
            Materialized = [new StreamMaterialization("", revision + 1, [extent])],
            LeaderStaging = st,
        });
        Assert.False(d.State.Staging[st].IsOpen);
        Assert.Equal(string.Empty, d.Inode("/p").PolicyName);

        st = d.Staging(0);
        extent = extent with { Staging = new StagingId(st) };
        d.Ok<SetPolicyResultMessage>(new SetPolicyCommand("/p", "pair")
        {
            Op = Op(),
            Materialized = [new StreamMaterialization("", revision, [extent])],
            LeaderStaging = st,
        });
        InodeRecord inode = d.Inode("/p");
        Assert.Equal("pair", inode.PolicyName);
        Assert.Equal(Obj(77), Assert.Single(inode.DefaultStream!.Extents).Object);
        Assert.Equal(d.Index, inode.DefaultStream.Revision);
        Assert.True(inode.GetStream("s")!.IsInline);
        Assert.False(d.State.Staging[st].IsOpen);
    }

    [Fact]
    public void Acl_TraverseIsEnforcedOnlyWhenConfigured()
    {
        const DfsInheritanceFlags both = DfsInheritanceFlags.ContainerInherit | DfsInheritanceFlags.ObjectInherit;
        var root = new DfsSecurityDescriptor(Alice, [
            new DfsAccessRule(DfsIdentity.Everyone, DfsFileSystemRights.FullControl & ~DfsFileSystemRights.Traverse, Inheritance: both),
            new DfsAccessRule(Alice, DfsFileSystemRights.FullControl, Inheritance: both)]);
        foreach (bool enforce in new[] { false, true })
        {
            var d = new SmDriver(CreationFor(enforceTraverse: enforce, rootSecurity: root));
            d.Ok<CreateDirectoryResultMessage>(new CreateDirectoryCommand("/a", true, null) { Op = Op() });
            VolumeApplyResult result = d.Apply(new CreateDirectoryCommand("/a/b", true, null) { Op = Op("bob") });
            Assert.Equal(enforce ? DfsErrorCode.AccessDenied : DfsErrorCode.None, result.Code);
        }
    }

    // ------------------------------------------------------------------------------------------------- snapshots and pins

    [Fact]
    public void Snapshots_CaptureRetainAndProtect()
    {
        var d = new SmDriver();
        ulong s = d.OpenSession();
        OpenHandleResultMessage h = d.Open(s, "/f", FileMode.CreateNew, FileAccess.Write, FileShare.Read, 1);
        ulong st = d.Staging(s, h.Handle.Value);
        d.Ok<CommitFileResultMessage>(new CommitFileCommand(h.Handle.Value, h.WriterGeneration, 0, Spec(9000, st, false, -1, DataChange(0, 9000, Obj(1), st)), null)
        {
            Op = Op(),
        });
        var created = d.Ok<CreateSnapshotResultMessage>(new CreateSnapshotCommand("s1", DfsConsistencyLabel.FilesystemConsistent, [], null, true) { Op = Op() });
        Assert.Equal(d.Index, created.Snapshot.Boundary.Sequence);
        d.Fails(DfsErrorCode.AlreadyExists, new CreateSnapshotCommand("s1", DfsConsistencyLabel.FilesystemConsistent, [], null, false) { Op = Op() });
        d.Fails(DfsErrorCode.AccessDenied, new CreateSnapshotCommand("s2", DfsConsistencyLabel.FilesystemConsistent, [], null, false)
        {
            Op = Op(grants: DfsPermissions.Read | DfsPermissions.Write),
        });

        // Overwrite the file: the snapshot keeps the old object reachable.
        d.Ok<CommitFileResultMessage>(new CommitFileCommand(h.Handle.Value, h.WriterGeneration, 0, Spec(10, st, false, 0, DataChange(0, 10, Obj(2), st)), null)
        {
            Op = Op(),
        });
        Assert.Contains(Obj(1), VolumeStateMachine.Reachable(d.State));
        Assert.Contains(Obj(1), d.Machine.Inspect().ReachableObjects());

        // Held: deletion is protected; releasing the hold needs RetentionAdmin.
        d.Fails(DfsErrorCode.RetentionProtected, new DeleteSnapshotCommand("s1") { Op = Op() });
        d.Fails(DfsErrorCode.RetentionProtected, new SetSnapshotRetentionCommand("s1", null, false) { Op = Op(grants: DfsPermissions.All & ~DfsPermissions.RetentionAdmin) });
        d.Ok<SetRetentionResultMessage>(new SetSnapshotRetentionCommand("s1", null, false) { Op = Op() });

        // A reader pin keeps the snapshot root after the snapshot is deleted.
        var opened = d.Ok<OpenSnapshotResultMessage>(new AcquirePinCommand(PinKind.SnapshotReader, s, "s1", 0, string.Empty, null) { Op = Op(session: s) });
        d.Ok<DeleteSnapshotResultMessage>(new DeleteSnapshotCommand("s1") { Op = Op() });
        Assert.Empty(d.State.Snapshots);
        Assert.Contains(Obj(1), VolumeStateMachine.Reachable(d.State));
        Assert.False(d.Ok<CloseSnapshotResultMessage>(new ReleasePinCommand(opened.SnapshotPin) { Op = Op("bob", session: s) }).Released);
        Assert.True(d.Ok<CloseSnapshotResultMessage>(new ReleasePinCommand(opened.SnapshotPin) { Op = Op(session: s) }).Released);
        Assert.DoesNotContain(Obj(1), VolumeStateMachine.Reachable(d.State));
    }

    [Fact]
    public void Gc_ClassifiesByReachabilityProtectionAndStaging()
    {
        var d = new SmDriver();
        ulong s = d.OpenSession();
        OpenHandleResultMessage w = d.Open(s, "/f", FileMode.CreateNew, FileAccess.Write, FileShare.Read, 1);
        ulong st = d.Staging(s, w.Handle.Value);
        d.Ok<CommitFileResultMessage>(new CommitFileCommand(w.Handle.Value, w.WriterGeneration, 0, Spec(100, st, false, -1, DataChange(0, 100, Obj(1), st)), null) { Op = Op() });
        // A live reader pinned before the replacement protects the replaced object through the dereference log.
        OpenHandleResultMessage reader = d.Open(s, "/f", FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        d.Ok<CommitFileResultMessage>(new CommitFileCommand(w.Handle.Value, w.WriterGeneration, 0, Spec(100, st, false, 0, DataChange(0, 100, Obj(2), st)), null) { Op = Op() });
        d.Ok<CloseHandleResultMessage>(new CloseHandleCommand(w.Handle.Value, w.WriterGeneration) { Op = Op() });

        VolumeState state = d.State;
        HashSet<ObjectId> reachable = VolumeStateMachine.Reachable(state);
        HashSet<ObjectId> protectedObjects = VolumeStateMachine.Protected(state, VolumeStateMachine.Watermark(state));
        Assert.Equal(ObjectClass.Keep, VolumeStateMachine.Classify(state, Obj(2), st, reachable, protectedObjects));
        Assert.Equal(ObjectClass.Keep, VolumeStateMachine.Classify(state, Obj(1), st, reachable, protectedObjects));
        Assert.Equal(ObjectClass.Unknown, VolumeStateMachine.Classify(state, Obj(3), st + 100, reachable, protectedObjects));

        // The reader goes away: the replaced object is garbage (its staging session is closed), the current one is not.
        d.Ok<CloseHandleResultMessage>(new CloseHandleCommand(reader.Handle.Value, 0) { Op = Op() });
        state = d.State;
        reachable = VolumeStateMachine.Reachable(state);
        protectedObjects = VolumeStateMachine.Protected(state, VolumeStateMachine.Watermark(state));
        Assert.Equal(ObjectClass.Deletable, VolumeStateMachine.Classify(state, Obj(1), st, reachable, protectedObjects));
        Assert.Equal(ObjectClass.Keep, VolumeStateMachine.Classify(state, Obj(2), st, reachable, protectedObjects));

        // An unreferenced upload under an open staging session is kept regardless of age (GC-005).
        ulong open = d.Staging(s);
        state = d.State;
        Assert.Equal(ObjectClass.Keep, VolumeStateMachine.Classify(state, Obj(5), open, VolumeStateMachine.Reachable(state), []));

        // Pruning drops dereference entries and closed staging records below the watermark.
        d.Apply(new PruneDerefLogCommand(ulong.MaxValue));
        Assert.Empty(d.State.DerefLog);
        Assert.False(d.State.Staging.ContainsKey(st));
        Assert.True(d.State.Staging.ContainsKey(open));
    }

    [Fact]
    public void Restore_InstallsTheStagedRootAtomically()
    {
        var source = new SmDriver();
        ulong s = source.OpenSession();
        source.Ok<WriteAllResultMessage>(new WriteAllCommand("/a/x", Spec(2, 0, true, 0, Inline(0, [1, 2])), null, null, true, 0) { Op = Op(session: s) });
        var target = new SmDriver();
        InodeRecord[] inodes = [.. source.State.Inodes.Values];
        target.Apply(new RestoreChunkCommand("src/s1", 0, [.. inodes.Take(2)]));
        Assert.Single(target.State.Inodes);
        target.Fails(DfsErrorCode.Integrity, new RestoreCompleteCommand("src/s1", inodes.Length));
        target.Apply(new RestoreChunkCommand("src/s1", 0, [.. inodes.Skip(2)]));
        target.Apply(new RestoreCompleteCommand("src/s1", inodes.Length));
        Assert.Equal(new byte[] { 1, 2 }, target.InlineBytes("/a/x"));
        Assert.All(target.State.Inodes.Values, n => Assert.Equal(target.Index, n.Revision));
        Assert.True(target.State.NextInodeId > inodes.Max(n => n.Id));
        target.Fails(DfsErrorCode.InvalidArgument, new RestoreChunkCommand("src/s1", 0, [.. inodes]));
    }

    [Fact]
    public void Transactions_BeginRecordsAndAbortEnds()
    {
        // Transactions are implemented (integration wave B2): a begin needs its session, records an active transaction with its
        // own staging session, and an abort ends it (idempotent, never reversing a commit).
        var d = new SmDriver();
        ulong s = d.OpenSession();
        var begin = d.Ok<TxBeginResultMessage>(new TxBeginCommand(s, SmDriver.Epoch0.AddMinutes(5))
        {
            Op = Op(session: s, sequence: 1, kind: DfsOperationKind.TxBegin, payload: Hash("begin")),
        });
        DfsOperationId tx = begin.Transaction.Operation;
        Assert.Equal(new DfsOperationId(s, 1), tx);
        Assert.Equal(TxState.Active, d.State.Transactions[tx].State);
        Assert.True(d.State.Staging[begin.Staging.Value].IsOpen);
        var aborted = d.Ok<TxAbortResultMessage>(new TxAbortCommand(tx) { Op = Op() });
        Assert.Equal(WireOutcomeStatus.Aborted, aborted.FinalStatus);
        Assert.Equal(TxState.Aborted, d.State.Transactions[tx].State);
        Assert.False(d.State.Staging[begin.Staging.Value].IsOpen);
        Assert.Equal(WireOutcomeStatus.Aborted, d.Ok<TxAbortResultMessage>(new TxAbortCommand(tx) { Op = Op() }).FinalStatus);
    }

    // ------------------------------------------------------------------------------------------------- determinism and checkpoints

    private static SmDriver Workload()
    {
        var d = new SmDriver();
        ulong s = d.OpenSession(maxOutcomes: 4);
        ulong seq = 1;
        d.Ok<CreateDirectoryResultMessage>(new CreateDirectoryCommand("/data/logs", true, null)
        {
            Op = Op(session: s, sequence: seq++, kind: DfsOperationKind.CreateDirectory, payload: Hash("mkdir")),
        });
        OpenHandleResultMessage h = d.Open(s, "/data/big", FileMode.CreateNew, FileAccess.Write, FileShare.Read | FileShare.Delete, seq++);
        ulong st = d.Staging(s, h.Handle.Value);
        d.Ok<CommitFileResultMessage>(new CommitFileCommand(h.Handle.Value, h.WriterGeneration, 0, Spec(20_000, st, false, -1,
            DataChange(0, 8000, Obj(1), st), DataChange(12_000, 8000, Obj(2), st)), null) { Op = Op(session: s, sequence: seq++, kind: DfsOperationKind.CommitFile, payload: Hash("c1")) });
        d.Ok<CreateSnapshotResultMessage>(new CreateSnapshotCommand("snap", DfsConsistencyLabel.FilesystemConsistent, [], null, false)
        {
            Op = Op(session: s, sequence: seq++, kind: DfsOperationKind.CreateSnapshot, payload: Hash("snap")),
        });
        d.Ok<WriteAllResultMessage>(new WriteAllCommand("/data/small:s", Spec(3, 0, true, 0, Inline(0, [1, 2, 3])), null, null, false, 0)
        {
            Op = Op(session: s, sequence: seq++, kind: DfsOperationKind.WriteAll, payload: Hash("w")),
        });
        OpenHandleResultMessage pinned = d.Open(s, "/data/big", FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete, pinned: true);
        d.Ok<CommitFileResultMessage>(new CommitFileCommand(h.Handle.Value, h.WriterGeneration, 0, Spec(5000, st, false, 1000), null) { Op = Op() });
        d.Ok<MoveResultMessage>(new MoveCommand("/data/small", "/data/logs/small", false, false, null)
        {
            Op = Op(session: s, sequence: seq++, kind: DfsOperationKind.Move, payload: Hash("mv")),
        });
        d.Ok<DeleteResultMessage>(new DeleteCommand("/data/big", false, false, null) { Op = Op(session: s, sequence: seq++, kind: DfsOperationKind.Delete, payload: Hash("rm")) });
        d.Ok<OpenSnapshotResultMessage>(new AcquirePinCommand(PinKind.SnapshotReader, s, "snap", 0, string.Empty, null) { Op = Op(session: s) });
        d.Ok<ReserveOperationIdsResultMessage>(new ReserveOperationIdsCommand(s, 3) { Op = Op(session: s) });
        d.Apply(new KeepAliveCommand(s, [h.Handle.Value, pinned.Handle.Value], [new PinAdvance(h.Handle.Value, d.Index)], []) { Op = Op(session: s) });
        d.Ok<SetAccessControlResultMessage>(new SetAclCommand("/data", AclChangeKind.Descriptor,
            new DfsSecurityDescriptor(Alice, [new DfsAccessRule(Bob, DfsFileSystemRights.Read, DfsAccessControlType.Allow, DfsInheritanceFlags.ObjectInherit)]),
            false, false) { Op = Op() });
        return d;
    }

    [Fact]
    public void Determinism_SameCommandsYieldIdenticalCheckpointBytes()
    {
        SmDriver first = Workload();
        var second = new SmDriver();
        ulong index = 3;
        foreach (byte[] command in first.Log)
        {
            ApplyOutcome outcome = second.Machine.Apply(++index, 1, command);
            Assert.NotNull(outcome);
        }

        Assert.Equal(first.Checkpoint(), second.Checkpoint());
        Assert.Equal(first.State.AppliedIndex, second.State.AppliedIndex);
    }

    [Fact]
    public void Checkpoint_RoundTripsByteForByteAndPreservesBehaviour()
    {
        SmDriver original = Workload();
        byte[] checkpoint = original.Checkpoint();
        var restored = new VolumeStateMachine(original.Creation, ClusterId);
        restored.RestoreCheckpoint(new ReadOnlySequence<byte>(checkpoint), original.Index, 1);
        var buffer = new ArrayBufferWriter<byte>();
        restored.WriteCheckpoint(buffer);
        Assert.Equal(checkpoint, buffer.WrittenSpan.ToArray());

        // The restored replica keeps applying identically (outcomes, pinned versions, detached inodes survive, RCV-03).
        Assert.Equal(original.State.Detached, restored.State.Detached);
        Assert.Equal(original.State.Snapshots.Keys, restored.State.Snapshots.Keys);
        HandleRecord pinned = Assert.Single(restored.State.Handles.Values, h => h.View == HandleView.VersionPinned);
        Assert.NotNull(pinned.PinnedInode);
        var next = new KeepAliveCommand(1, [], [], []) { Op = Op(session: 1), LeaderTime = original.Now };
        byte[] encoded = CommandCodec.Encode(next);
        original.Machine.Apply(original.Index + 1, 2, encoded);
        restored.Apply(original.Index + 1, 2, encoded);
        var a = new ArrayBufferWriter<byte>();
        var b = new ArrayBufferWriter<byte>();
        original.Machine.WriteCheckpoint(a);
        restored.WriteCheckpoint(b);
        Assert.Equal(a.WrittenSpan.ToArray(), b.WrittenSpan.ToArray());
    }

    [Fact]
    public void Checkpoint_StoresEachInodeVersionOnce()
    {
        SmDriver d = Workload();
        int distinct = d.State.Inodes.Values.Concat(d.State.Snapshots.Values.SelectMany(s => s.Root.Values))
            .Concat(d.State.Pins.Values.Where(p => p.Root is not null).SelectMany(p => p.Root!.Values))
            .Concat(d.State.Handles.Values.Where(h => h.PinnedInode is not null).Select(h => h.PinnedInode!))
            .Select(n => (n.Id, n.Revision)).Distinct().Count();
        var r = new WireReader(d.Checkpoint(), new WireLimits(MaxBytesLength: int.MaxValue));
        Assert.Equal(StateCodec.CheckpointFormat, r.ReadByte());
        // Skip the configuration and header to the version count.
        r.ReadVolumeId();
        r.ReadString();
        r.ReadVarUInt();
        r.ReadGuid();
        r.ReadString();
        r.ReadDateTimeOffset();
        r.ReadVarInt32();
        r.ReadVolumeLimits();
        r.ReadString();
        r.ReadBool();
        r.ReadStoragePolicy();
        r.ReadPolicyMap();
        r.ReadVarUInt();
        r.ReadDateTimeOffset();
        Assert.Equal(distinct, r.ReadCount(1));
    }

    [Fact]
    public void MalformedCommands_FailWithoutChangingState()
    {
        var d = new SmDriver();
        byte[] before = d.Checkpoint();
        ApplyOutcome outcome = d.Machine.Apply(10, 1, new byte[] { 1, 0xFF, 0xFF, 0 });
        Assert.False(outcome.IsSuccess);
        Assert.Equal(10UL, d.Machine.AppliedIndex);
        Assert.Single(d.State.Inodes);
        Assert.Empty(d.State.Sessions);
        _ = before;
    }
}
