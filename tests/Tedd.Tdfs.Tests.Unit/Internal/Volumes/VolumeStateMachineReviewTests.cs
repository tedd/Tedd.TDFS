using System.Buffers;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Volumes.Apply;
using Tedd.Tdfs.Server.Volumes.Commands;
using Tedd.Tdfs.Server.Volumes.State;
using static Tedd.Tdfs.Tests.Unit.Internal.Volumes.SmDriver;

namespace Tedd.Tdfs.Tests.Unit.Internal.Volumes;

/// <summary>
/// Adversarial cases found in review: authorization of creating opens, append-only handles, racing creating appends,
/// cross-principal acknowledgements, reported revisions (ADS-011, NS-018), per-stream copy semantics (ADS-04), conditional
/// writes, untrusted inline lengths, unexpected apply exceptions and restore version keys.
/// </summary>
public sealed class VolumeStateMachineReviewTests
{
    private static WriteAllCommand Write(string path, byte[] bytes, ulong session, ulong expectedRevision = 0)
        => new(path, Spec(bytes.Length, 0, true, 0, Inline(0, bytes)), null, null, false, expectedRevision) { Op = Op(session: session) };

    // ------------------------------------------------------------------------------------------------- authorization

    [Fact]
    public void OpenOrCreate_WithReadAccess_NeedsTheWriteGrantToCreate()
    {
        var d = new SmDriver();
        ulong s = d.OpenSession();
        OpenHandleCommand Open(string path, ulong seq) => OpenCommand(s, path, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite, seq) with
        {
            Op = Op(session: s, sequence: seq, kind: DfsOperationKind.Open, payload: Hash(path), grants: DfsPermissions.Read),
        };

        // A principal with only the Read grant cannot create a file (or a stream) by asking for read access (ACL-06).
        d.Fails(DfsErrorCode.AccessDenied, Open("/new", 1));
        Assert.False(d.Exists("/new"));
        d.Ok<WriteAllResultMessage>(Write("/f", [1, 2], s));
        d.Fails(DfsErrorCode.AccessDenied, Open("/f:s", 2));
        Assert.Null(d.Inode("/f").GetStream("s"));

        // Opening what exists needs only the Read grant.
        Assert.False(d.Ok<OpenHandleResultMessage>(Open("/f", 3)).Created);
    }

    // ------------------------------------------------------------------------------------------------- append handles

    [Fact]
    public void AppendHandle_CannotTruncateShortenOrRewriteExistingBytes()
    {
        var d = new SmDriver();
        ulong s = d.OpenSession();
        d.Ok<WriteAllResultMessage>(Write("/log", [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], s));
        OpenHandleResultMessage h = d.Open(s, "/log", FileMode.Append, FileAccess.Write, FileShare.Read, 1);
        CommitFileCommand Commit(PublishSpec spec) => new(h.Handle.Value, h.WriterGeneration, 0, spec, null) { Op = Op() };

        // Shortening without a truncation watermark, rewriting the prefix, or truncating: all refused, nothing changes.
        d.Fails(DfsErrorCode.InvalidArgument, Commit(Spec(3, 0, true, -1, Inline(0, [9, 9, 9]))));
        d.Fails(DfsErrorCode.InvalidArgument, Commit(Spec(10, 0, true, -1, Inline(0, [9, 9]))));
        d.Fails(DfsErrorCode.InvalidArgument, Commit(Spec(12, 0, true, 2, Inline(10, [11, 12]))));
        Assert.Equal(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }, d.InlineBytes("/log"));

        // A pure suffix is fine.
        d.Ok<CommitFileResultMessage>(Commit(Spec(12, 0, true, -1, Inline(10, [11, 12]))));
        Assert.Equal(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 }, d.InlineBytes("/log"));
    }

    // ------------------------------------------------------------------------------------------------- racing creating appends

    [Fact]
    public void CreatingAppends_PlannedAgainstAnAbsentFile_NeverOverwriteEachOther()
    {
        var d = new SmDriver();
        ulong s = d.OpenSession();

        // Two Append helpers raced on an absent file: the leader planned both as absolute inline results against "no stream"
        // (DerivedFromRevision 0). The second must append after the first, not replace its bytes.
        AppendCommand Planned(byte[] suffix) => new("/race", 0, 0, Spec(suffix.Length, 0, true, -1, Inline(0, suffix)), Absolute: true, CreateIfMissing: true,
            null, null) { Op = Op(session: s) };
        AppendResultMessage first = d.Ok<AppendResultMessage>(Planned([1, 2, 3]));
        AppendResultMessage second = d.Ok<AppendResultMessage>(Planned([4, 5, 6]));
        Assert.True(first.Created);
        Assert.Equal((3L, 6L), (second.OldLength, second.NewLength));
        Assert.Equal(new byte[] { 1, 2, 3, 4, 5, 6 }, d.InlineBytes("/race"));

        // When the planned representation no longer holds (the result would exceed the inline threshold), the append is a
        // retryable conflict with no effect, never a silent overwrite.
        var small = new SmDriver(CreationFor(inlineThreshold: 8));
        ulong t = small.OpenSession();
        small.Ok<AppendResultMessage>(new AppendCommand("/r", 0, 0, Spec(6, 0, true, -1, Inline(0, [1, 2, 3, 4, 5, 6])), true, true, null, null)
        {
            Op = Op(session: t),
        });
        small.Fails(DfsErrorCode.WriterConflict, new AppendCommand("/r", 0, 0, Spec(6, 0, true, -1, Inline(0, [7, 7, 7, 7, 7, 7])), true, true, null, null)
        {
            Op = Op(session: t),
        });
        Assert.Equal(new byte[] { 1, 2, 3, 4, 5, 6 }, small.InlineBytes("/r"));
    }

    // ------------------------------------------------------------------------------------------------- acknowledgements

    [Fact]
    public void AckWatermark_SentWithAnotherPrincipalsSession_DoesNotRetireItsOutcomes()
    {
        var d = new SmDriver();
        ulong alice = d.OpenSession("alice");
        ulong bob = d.OpenSession("bob");
        CreateDirectoryCommand Mkdir(string path, ulong seq) => new(path, false, null)
        {
            Op = Op("alice", alice, seq, DfsOperationKind.CreateDirectory, Hash(path)),
        };
        CreateDirectoryResultMessage created = d.Ok<CreateDirectoryResultMessage>(Mkdir("/a", 1));

        // Bob names Alice's session in his request header with a huge acknowledgement watermark.
        OperationHeader hostile = Op("bob", bob, 1, DfsOperationKind.CreateDirectory, Hash("/b")) with { RequestSession = alice, AckWatermark = 1_000_000 };
        d.Ok<CreateDirectoryResultMessage>(new CreateDirectoryCommand("/b", false, null) { Op = hostile });

        // Alice's retry still gets her stored outcome, and her next sequence still executes (OUT-004, OUT-012).
        Assert.Equal(created.Token, d.Ok<CreateDirectoryResultMessage>(Mkdir("/a", 1)).Token);
        d.Ok<CreateDirectoryResultMessage>(Mkdir("/c", 2));
        Assert.Equal(0UL, d.State.Sessions[alice].RetiredWatermark);
    }

    // ------------------------------------------------------------------------------------------------- revisions

    [Fact]
    public void BaseRevision_FromTheOpenReplyIsCurrentAfterMetadataAndNamedStreamChanges()
    {
        var d = new SmDriver();
        ulong s = d.OpenSession();
        d.Ok<WriteAllResultMessage>(Write("/f", [1, 2, 3], s));
        d.Ok<MoveResultMessage>(new MoveCommand("/f", "/g", false, false, null) { Op = Op() });
        d.Ok<WriteAllResultMessage>(Write("/g:meta", [7], s));
        d.Ok<SetAccessResultMessage>(new SetAclCommand("/g", AclChangeKind.AccessFlags, null, false, true) { Op = Op() });

        // The writer publishes against the revision its open reported: nothing changed the content since, so no conflict.
        OpenHandleResultMessage h = d.Open(s, "/g", FileMode.Open, FileAccess.Write, FileShare.Read, 1);
        ulong st = d.Staging(s, h.Handle.Value);
        CommitFileResultMessage committed = d.Ok<CommitFileResultMessage>(new CommitFileCommand(h.Handle.Value, h.WriterGeneration, h.Info.Revision,
            Spec(3, st, true, -1, Inline(0, [9, 9, 9])), null) { Op = Op() });
        Assert.Equal(new byte[] { 9, 9, 9 }, d.InlineBytes("/g"));

        // A base older than the last content change still conflicts.
        d.Fails(DfsErrorCode.WriterConflict, new CommitFileCommand(h.Handle.Value, h.WriterGeneration, committed.Revision - 1,
            Spec(3, st, true, -1, Inline(0, [5, 5, 5])), null) { Op = Op() });
    }

    [Fact]
    public void FileRevision_IgnoresNamedStreamWritesButReflectsContentAndMetadataChanges()
    {
        var d = new SmDriver();
        ulong s = d.OpenSession();
        WriteAllResultMessage written = d.Ok<WriteAllResultMessage>(Write("/f", [1], s));
        ulong Reported() => VolumeStateMachine.Describe(d.State, d.Inode("/f"), "/f", null).Revision;
        Assert.Equal(written.Revision, Reported());

        // A named stream write does not change the file's (default stream's) revision (ADS-011) ...
        WriteAllResultMessage stream = d.Ok<WriteAllResultMessage>(Write("/f:a", [2, 2], s));
        Assert.Equal(written.Revision, Reported());
        Assert.Equal(stream.Revision, VolumeStateMachine.Describe(d.State, d.Inode("/f"), "/f:a", "a").Revision);
        Assert.Equal(written.Revision, VolumeStateMachine.InspectFile(d.State, "/f")!.Revision);

        // ... but an ACL change and a policy change do (ACL-07, NS-018), as does a content change.
        SetAccessResultMessage acl = d.Ok<SetAccessResultMessage>(new SetAclCommand("/f", AclChangeKind.AccessFlags, null, false, true) { Op = Op() });
        Assert.Equal(acl.Token.Sequence, Reported());
        SetPolicyResultMessage policy = d.Ok<SetPolicyResultMessage>(new SetPolicyCommand("/f", "pair") { Op = Op() });
        Assert.Equal(policy.Token.Sequence, Reported());
        WriteAllResultMessage replaced = d.Ok<WriteAllResultMessage>(Write("/f", [3], s));
        Assert.Equal(replaced.Token.Sequence, Reported());
        Assert.Equal(replaced.Revision, Reported());

        // A conditional write compares against the reported revision.
        d.Fails(DfsErrorCode.WriterConflict, Write("/f", [4], s, expectedRevision: replaced.Revision - 1));
        d.Ok<WriteAllResultMessage>(Write("/f", [4], s, expectedRevision: replaced.Revision));
    }

    [Fact]
    public void ConditionalWriteAll_NeverCreatesAnAbsentFileOrStream()
    {
        var d = new SmDriver();
        ulong s = d.OpenSession();
        d.Fails(DfsErrorCode.WriterConflict, Write("/absent", [1], s, expectedRevision: 5));
        Assert.False(d.Exists("/absent"));
        WriteAllResultMessage f = d.Ok<WriteAllResultMessage>(Write("/f", [1], s));
        d.Fails(DfsErrorCode.WriterConflict, Write("/f:s", [1], s, expectedRevision: f.Revision));
        Assert.Null(d.Inode("/f").GetStream("s"));
    }

    [Fact]
    public void Publication_ValidatedAgainstAnotherPolicy_IsAConflictAfterAPolicyChange()
    {
        var d = new SmDriver();
        ulong s = d.OpenSession();
        d.Ok<WriteAllResultMessage>(Write("/p", [1], s));
        d.Ok<SetPolicyResultMessage>(new SetPolicyCommand("/p", "pair") { Op = Op() });

        // The leader validated receipts and chose the representation for the default policy; the file is now under "pair"
        // (LAY-002, DEC-10): the publication must be prepared again, never applied under a policy it was not checked against.
        PublishSpec planned = Spec(1, 0, true, 0, Inline(0, [2])) with { Policy = string.Empty };
        d.Fails(DfsErrorCode.WriterConflict, new WriteAllCommand("/p", planned, null, null, false, 0) { Op = Op(session: s) });
        Assert.Equal(new byte[] { 1 }, d.InlineBytes("/p"));
        d.Ok<WriteAllResultMessage>(new WriteAllCommand("/p", planned with { Policy = "pair" }, null, null, false, 0) { Op = Op(session: s) });
        Assert.Equal(new byte[] { 2 }, d.InlineBytes("/p"));
    }

    // ------------------------------------------------------------------------------------------------- copy (ADS-04)

    [Fact]
    public void Copy_ReplacesOnlyTheAddressedStreamAndAcceptsStreamPaths()
    {
        var d = new SmDriver();
        ulong s = d.OpenSession();
        d.Ok<WriteAllResultMessage>(Write("/src", [1, 1], s));
        d.Ok<WriteAllResultMessage>(Write("/src:a", [2, 2], s));
        d.Ok<WriteAllResultMessage>(Write("/src:z", [3, 3], s));
        d.Ok<WriteAllResultMessage>(Write("/f", [9], s));
        d.Ok<WriteAllResultMessage>(Write("/f:a", [8], s));
        WriteAllResultMessage fb = d.Ok<WriteAllResultMessage>(Write("/f:b", [7], s));
        CopyCommand Copy(string from, string to, bool overwrite) => new(from, to, overwrite, 0, null, null) { Op = Op() };

        // Onto an existing plain path: the default stream only; the source's named streams are not merged (ADS-011).
        ulong fileRevision = d.Inode("/f").ReportedRevision;
        CopyResultMessage plain = d.Ok<CopyResultMessage>(Copy("/src", "/f", true));
        Assert.False(plain.Created);
        Assert.Equal(new byte[] { 1, 1 }, d.InlineBytes("/f"));
        Assert.Equal(new byte[] { 8 }, d.InlineBytes("/f", "a"));
        Assert.Null(d.Inode("/f").GetStream("z"));
        Assert.Equal(fb.Revision, d.Inode("/f").GetStream("b")!.Revision);
        Assert.True(d.Inode("/f").ReportedRevision > fileRevision);

        // Onto an existing named stream: that stream only, and the file's revision does not change.
        fileRevision = d.Inode("/f").ReportedRevision;
        d.Fails(DfsErrorCode.AlreadyExists, Copy("/src", "/f:a", false));
        CopyResultMessage onStream = d.Ok<CopyResultMessage>(Copy("/src", "/f:a", true));
        Assert.Equal(onStream.Token.Sequence, onStream.Revision);
        Assert.Equal(new byte[] { 1, 1 }, d.InlineBytes("/f", "a"));
        Assert.Equal(fileRevision, d.Inode("/f").ReportedRevision);

        // Onto an absent named stream: created (no overwrite needed), from a named source stream.
        Assert.True(d.Ok<CopyResultMessage>(Copy("/src:z", "/f:c", false)).Created);
        Assert.Equal(new byte[] { 3, 3 }, d.InlineBytes("/f", "c"));

        // A stream copied to a new plain path becomes that file's default stream; a plain copy to a new file takes every stream.
        d.Ok<CopyResultMessage>(Copy("/src:a", "/fromstream", false));
        Assert.Equal(new byte[] { 2, 2 }, d.InlineBytes("/fromstream"));
        Assert.Equal(0, d.Inode("/fromstream").NamedStreamCount);
        d.Ok<CopyResultMessage>(Copy("/src", "/whole", false));
        Assert.Equal(new[] { "", "a", "z" }, d.Inode("/whole").Streams.Keys);

        // A file is never copied onto itself, not even onto one of its own streams.
        d.Fails(DfsErrorCode.InvalidArgument, Copy("/src", "/src:q", true));
        d.Fails(DfsErrorCode.FileNotFound, Copy("/src:missing", "/x", false));
    }

    // ------------------------------------------------------------------------------------------------- robustness

    [Fact]
    public void InlineRepresentation_OfAnUntrustedHugeLength_IsRefusedWithoutAllocating()
    {
        var d = new SmDriver();
        ulong s = d.OpenSession();
        OpenHandleResultMessage h = d.Open(s, "/huge", FileMode.CreateNew, FileAccess.Write, FileShare.Read, 1);
        ulong st = d.Staging(s, h.Handle.Value);
        // 5 GiB "inline" with no changes: a malformed leader command must fail cleanly, not allocate (or crash the node).
        VolumeApplyResult result = d.Apply(new CommitFileCommand(h.Handle.Value, h.WriterGeneration, 0, Spec(5L << 30, st, true, -1), null) { Op = Op() });
        Assert.Equal(DfsErrorCode.InvalidArgument, result.Code);
        Assert.Equal(0, d.Inode("/huge").DefaultStream!.Length);
    }

    [Fact]
    public void UnexpectedApplyExceptions_FailTheCommandDeterministicallyAndRecordTheOutcome()
    {
        // A root descriptor owned by CreatorOwner makes the ACL evaluator throw ArgumentException inside apply.
        var d = new SmDriver(CreationFor(rootSecurity: new DfsSecurityDescriptor(DfsIdentity.CreatorOwner, [])));
        ulong s = d.OpenSession();
        var mkdir = new CreateDirectoryCommand("/x", false, null) { Op = Op(session: s, sequence: 1, kind: DfsOperationKind.CreateDirectory, payload: Hash("/x")) };
        ulong before = d.Index;
        VolumeApplyResult first = d.Apply(mkdir);
        Assert.Equal(DfsErrorCode.Internal, first.Code);
        Assert.Equal(before + 1, d.State.AppliedIndex);
        Assert.False(d.Exists("/x"));

        // The failure is the id's outcome: a retry is answered from it rather than executed again.
        VolumeApplyResult retry = d.Apply(mkdir);
        Assert.True(retry.Replayed);
        Assert.Equal(DfsErrorCode.Internal, retry.Code);
    }

    // ------------------------------------------------------------------------------------------------- restore version keys

    [Fact]
    public void RestoreStaging_KeepsTheStagedRootDistinctFromTheVolumesOwnRootAcrossCheckpoints()
    {
        var d = new SmDriver();
        // The staged root of a never-modified source volume has revision 0, like this (empty) volume's own root.
        var stagedRoot = new InodeRecord(InodeRecord.RootId, InodeKind.Directory, 0, 0, Epoch0, Epoch0,
            new DfsSecurityDescriptor(DfsIdentity.User("carol"), []), "pair", 0, string.Empty, InodeRecord.NoStreams, InodeRecord.NoChildren);
        Assert.True(d.Apply(new RestoreChunkCommand("src", 0, [])).IsSuccess);
        ulong staging = d.State.Restore!.Staging;
        Assert.True(d.Apply(new RestoreChunkCommand("src", staging, [stagedRoot])).IsSuccess);

        var buffer = new ArrayBufferWriter<byte>();
        d.Machine.WriteCheckpoint(buffer);
        var copy = new VolumeStateMachine(d.Creation, ClusterId);
        copy.RestoreCheckpoint(new ReadOnlySequence<byte>(buffer.WrittenMemory), d.Index, 1);

        // The staged root survives the checkpoint (not confused with the volume's own root version) and installs as staged.
        Assert.Equal(DfsIdentity.User("carol"), copy.State.Restore!.Staged[InodeRecord.RootId].Owner);
        Assert.Equal(DfsIdentity.User("alice"), copy.State.Root.Owner);
    }
}
