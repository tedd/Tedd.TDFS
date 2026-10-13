using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Volumes.Apply;
using Tedd.Tdfs.Server.Volumes.Commands;
using Tedd.Tdfs.Server.Volumes.State;
using static Tedd.Tdfs.Tests.Unit.Internal.Volumes.SmDriver;

namespace Tedd.Tdfs.Tests.Unit.Internal.Volumes;

/// <summary>Publication (CommitFile, WriteAll, Append, Copy), fencing, extent maps and limits.</summary>
public sealed class VolumeStateMachinePublicationTests
{
    private static (SmDriver D, ulong Session, OpenHandleResultMessage Handle, ulong Staging) Writer(string path = "/f")
    {
        var d = new SmDriver();
        ulong s = d.OpenSession();
        OpenHandleResultMessage h = d.Open(s, path, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read, 1);
        ulong staging = d.Staging(s, h.Handle.Value);
        return (d, s, h, staging);
    }

    private static CommitFileCommand Commit(OpenHandleResultMessage h, PublishSpec spec, ulong generation = 0, ulong baseRevision = 0, string? snapshot = null)
        => new(h.Handle.Value, generation == 0 ? h.WriterGeneration : generation, baseRevision, spec, snapshot) { Op = Op() };

    [Fact]
    public void CommitFile_ReplacesOnlyChangedRangesAndKeepsUntouchedExtents()
    {
        (SmDriver d, _, OpenHandleResultMessage h, ulong st) = Writer();
        d.Ok<CommitFileResultMessage>(Commit(h, Spec(3 * 4096, st, false, -1,
            DataChange(0, 4096, Obj(1), st), DataChange(4096, 4096, Obj(2), st), DataChange(8192, 4096, Obj(3), st))));
        // Overwrite the middle 100 bytes with a new object: neighbours keep their objects (AT-07).
        CommitFileResultMessage second = d.Ok<CommitFileResultMessage>(Commit(h, Spec(3 * 4096, st, false, -1, DataChange(5000, 100, Obj(9), st))));
        StreamRecord stream = d.Inode("/f").DefaultStream!;
        Assert.Equal(d.Index, second.Revision);
        Assert.Equal(new[] { (0L, 4096L, Obj(1)), (4096L, 904L, Obj(2)), (5000L, 100L, Obj(9)), (5100L, 3092L, Obj(2)), (8192L, 4096L, Obj(3)) },
            stream.Extents.Select(e => (e.Offset, e.Length, e.Object)));
        Assert.Equal(5100 - 4096, stream.Extents[3].ObjectOffset);
        Assert.DoesNotContain(d.State.DerefLog, e => e.Object == Obj(2));
    }

    [Fact]
    public void CommitFile_TruncationNeverResurrectsData()
    {
        (SmDriver d, _, OpenHandleResultMessage h, ulong st) = Writer();
        d.Ok<CommitFileResultMessage>(Commit(h, Spec(10_000, st, false, -1, DataChange(0, 10_000, Obj(1), st))));
        // Truncate to 1000 and extend to 5000 in one publication: [1000, 5000) is a hole (STR-011).
        d.Ok<CommitFileResultMessage>(Commit(h, Spec(5000, st, false, 1000)));
        StreamRecord stream = d.Inode("/f").DefaultStream!;
        Assert.Equal(5000, stream.Length);
        Assert.Equal((0L, 1000L), (stream.Extents.Single().Offset, stream.Extents.Single().Length));
        Assert.True(stream.IsSparse);
        // A zero change punches a hole without touching the rest of the extent.
        d.Ok<CommitFileResultMessage>(Commit(h, Spec(5000, st, false, -1, Zero(0, 10))));
        ExtentRef remaining = d.Inode("/f").DefaultStream!.Extents.Single();
        Assert.Equal((10L, 990L, 10L), (remaining.Offset, remaining.Length, remaining.ObjectOffset));
    }

    [Fact]
    public void CommitFile_StaleGenerationIsFencedAndStaleBaseIsAConflict()
    {
        (SmDriver d, ulong s, OpenHandleResultMessage h, ulong st) = Writer();
        d.Ok<CommitFileResultMessage>(Commit(h, Spec(3, st, true, -1, Inline(0, [1, 2, 3]))));
        ulong revision = d.Inode("/f").DefaultStream!.Revision;
        d.Fails(DfsErrorCode.WriterConflict, Commit(h, Spec(3, st, true, -1, Inline(0, [9, 9, 9])), baseRevision: revision - 1));
        d.Fails(DfsErrorCode.LeaseLost, Commit(h, Spec(3, st, true, -1, Inline(0, [9, 9, 9])), generation: h.WriterGeneration + 5));

        // The lease expires; a replacement writer takes over; the old writer's publication is rejected (LOCK-04, AT-05).
        d.Apply(new ExpireHandlesCommand([h.Handle.Value]));
        OpenHandleResultMessage replacement = d.Open(s, "/f", FileMode.Open, FileAccess.Write, FileShare.Read, 9);
        Assert.Equal(h.WriterGeneration + 1, replacement.WriterGeneration);
        d.Fails(DfsErrorCode.LeaseLost, Commit(h, Spec(3, st, true, -1, Inline(0, [6, 6, 6]))));
        Assert.Equal(new byte[] { 1, 2, 3 }, d.InlineBytes("/f"));
    }

    [Fact]
    public void CommitFile_ClosedStagingAndLimitsAreRejected()
    {
        var d = new SmDriver(CreationFor(limits: new DfsVolumeLimits { MaxFileLength = 10_000, MaxPublishBytes = 5000 }));
        ulong s = d.OpenSession();
        OpenHandleResultMessage h = d.Open(s, "/f", FileMode.CreateNew, FileAccess.Write, FileShare.Read, 1);
        ulong st = d.Staging(s, h.Handle.Value);
        VolumeApplyResult tooLong = d.Fails(DfsErrorCode.LimitExceeded, Commit(h, Spec(20_000, st, false, -1)));
        Assert.Equal(nameof(DfsVolumeLimits.MaxFileLength), Assert.IsType<ErrorReplyMessage>(tooLong.Reply).Detail?.LimitName);
        VolumeApplyResult tooMuch = d.Fails(DfsErrorCode.LimitExceeded, Commit(h, Spec(6000, st, false, -1, DataChange(0, 6000, Obj(1), st))));
        Assert.Equal(nameof(DfsVolumeLimits.MaxPublishBytes), Assert.IsType<ErrorReplyMessage>(tooMuch.Reply).Detail?.LimitName);
        d.Apply(new CloseStagingCommand([st]));
        d.Fails(DfsErrorCode.LeaseLost, Commit(h, Spec(100, st, false, -1, DataChange(0, 100, Obj(2), st))));
    }

    [Fact]
    public void CommitFile_InlineRepresentationsAndMixedFailures()
    {
        (SmDriver d, _, OpenHandleResultMessage h, ulong st) = Writer();
        d.Ok<CommitFileResultMessage>(Commit(h, Spec(5, st, true, -1, Inline(0, [1, 2, 3, 4, 5]))));
        d.Ok<CommitFileResultMessage>(Commit(h, Spec(8, st, true, -1, Inline(6, [7, 8]))));
        Assert.Equal(new byte[] { 1, 2, 3, 4, 5, 0, 7, 8 }, d.InlineBytes("/f"));
        // An extent-based result may not keep inline bytes (the leader must materialize them first).
        d.Fails(DfsErrorCode.InvalidArgument, Commit(h, Spec(20_000, st, false, -1, DataChange(8, 19_992, Obj(1), st))));
        // An inline result beyond the threshold is refused.
        d.Fails(DfsErrorCode.InvalidArgument, Commit(h, Spec(5000, st, true, -1, Inline(0, new byte[5000]))));
    }

    [Fact]
    public void CommitAndSnapshot_CapturesTheMutationInTheSameEntry()
    {
        (SmDriver d, _, OpenHandleResultMessage h, ulong st) = Writer();
        CommitFileResultMessage result = d.Ok<CommitFileResultMessage>(Commit(h, Spec(2, st, true, -1, Inline(0, [4, 2])), snapshot: "exact"));
        Assert.NotNull(result.Snapshot);
        Assert.Equal(result.Token, result.Snapshot!.Boundary);
        SnapshotRecord snap = d.State.Snapshots["exact"];
        Assert.Equal(new byte[] { 4, 2 }, snap.Root[h.Info.Inode.Value].DefaultStream!.Inline.ToArray());
        // A name collision fails the whole command (nothing published).
        d.Fails(DfsErrorCode.AlreadyExists, Commit(h, Spec(1, st, true, -1, Inline(0, [9])), snapshot: "exact"));
        Assert.Equal(new byte[] { 4, 2 }, d.InlineBytes("/f"));
    }

    [Fact]
    public void WriteAll_PreservesTheInodeAndKeepsOtherStreams()
    {
        var d = new SmDriver();
        ulong s = d.OpenSession();
        WriteAllResultMessage created = d.Ok<WriteAllResultMessage>(new WriteAllCommand("/a/w", Spec(3, 0, true, 0, Inline(0, [1, 2, 3])), null, null, true, 0)
        {
            Op = Op(session: s),
        });
        Assert.True(created.Created);
        d.Ok<WriteAllResultMessage>(new WriteAllCommand("/a/w:s", Spec(2, 0, true, 0, Inline(0, [5, 5])), null, null, false, 0) { Op = Op(session: s) });
        WriteAllResultMessage replaced = d.Ok<WriteAllResultMessage>(new WriteAllCommand("/a/w", Spec(1, 0, true, 0, Inline(0, [9])), null, null, false, 0)
        {
            Op = Op(session: s),
        });
        Assert.False(replaced.Created);
        Assert.Equal(created.Inode, replaced.Inode);
        Assert.Equal(new byte[] { 9 }, d.InlineBytes("/a/w"));
        Assert.Equal(new byte[] { 5, 5 }, d.InlineBytes("/a/w", "s"));
        d.Fails(DfsErrorCode.WriterConflict, new WriteAllCommand("/a/w", Spec(1, 0, true, 0, Inline(0, [1])), null, null, false, 1) { Op = Op(session: s) });
        d.Fails(DfsErrorCode.DirectoryNotFound, new WriteAllCommand("/b/w", Spec(1, 0, true, 0, Inline(0, [1])), null, null, false, 0) { Op = Op(session: s) });

        // The implicit writer reservation conflicts with an open writer.
        d.Open(s, "/a/w", FileMode.Open, FileAccess.Write, FileShare.Read, 5);
        d.Fails(DfsErrorCode.WriterConflict, new WriteAllCommand("/a/w", Spec(1, 0, true, 0, Inline(0, [2])), null, null, false, 0) { Op = Op(session: s) });
    }

    [Fact]
    public void Append_IsRelativeToTheEndAtApplyTime()
    {
        var d = new SmDriver();
        ulong s = d.OpenSession();
        d.Fails(DfsErrorCode.FileNotFound, new AppendCommand("/log", 0, 0, Spec(2, 0, true, -1, Inline(0, [1, 2])), false, false, null, null) { Op = Op(session: s) });
        AppendResultMessage first = d.Ok<AppendResultMessage>(new AppendCommand("/log", 0, 0, Spec(2, 0, true, -1, Inline(0, [1, 2])), false, true, null, null)
        {
            Op = Op(session: s),
        });
        Assert.True(first.Created);
        AppendResultMessage second = d.Ok<AppendResultMessage>(new AppendCommand("/log", 0, 0, Spec(3, 0, true, -1, Inline(0, [3, 4, 5])), false, false, null, null)
        {
            Op = Op(session: s),
        });
        Assert.Equal(2, second.OldLength);
        Assert.Equal(5, second.NewLength);
        Assert.Equal(new byte[] { 1, 2, 3, 4, 5 }, d.InlineBytes("/log"));
    }

    [Fact]
    public void Copy_SharesObjectsOfEveryStreamAndKeepsTheDestinationInode()
    {
        (SmDriver d, ulong s, OpenHandleResultMessage h, ulong st) = Writer("/src");
        d.Ok<CommitFileResultMessage>(Commit(h, Spec(10_000, st, false, -1, DataChange(0, 10_000, Obj(1), st))));
        d.Ok<WriteAllResultMessage>(new WriteAllCommand("/src:meta", Spec(2, 0, true, 0, Inline(0, [7, 7])), null, null, false, 0) { Op = Op(session: s) });
        d.Ok<WriteAllResultMessage>(new WriteAllCommand("/dst", Spec(1, 0, true, 0, Inline(0, [1])), null, null, false, 0) { Op = Op(session: s) });
        ulong dstInode = d.Inode("/dst").Id;
        d.Fails(DfsErrorCode.AlreadyExists, new CopyCommand("/src", "/dst", false, 0, null, null) { Op = Op() });
        d.Fails(DfsErrorCode.InvalidArgument, new CopyCommand("/src", "/src", true, 0, null, null) { Op = Op() });
        d.Fails(DfsErrorCode.WriterConflict, new CopyCommand("/src", "/dst", true, 1, null, null) { Op = Op() });
        CopyResultMessage copied = d.Ok<CopyResultMessage>(new CopyCommand("/src", "/dst", true, 0, null, null) { Op = Op() });
        Assert.Equal(dstInode, copied.Inode.Value);
        InodeRecord dst = d.Inode("/dst");
        Assert.Equal(Obj(1), dst.DefaultStream!.Extents.Single().Object);
        // Over an existing file only the addressed (default) stream is replaced; the source's named streams are not merged (ADS-011).
        Assert.Null(dst.GetStream("meta"));
        CopyResultMessage fresh = d.Ok<CopyResultMessage>(new CopyCommand("/src", "/fresh", false, 0, null, null) { Op = Op("alice") });
        Assert.True(fresh.Created);
        Assert.Empty(d.Inode("/fresh").Security.Rules);
        // A new file receives every stream (ADS-010).
        Assert.Equal(new byte[] { 7, 7 }, d.Inode("/fresh").GetStream("meta")!.Inline.ToArray());
    }

    [Fact]
    public void Copy_UsesLeaderMaterializedExtentsForInlineStreamsOnlyAtTheirRevision()
    {
        var d = new SmDriver();
        ulong s = d.OpenSession();
        d.Ok<WriteAllResultMessage>(new WriteAllCommand("/src", Spec(3, 0, true, 0, Inline(0, [1, 2, 3])), null, null, false, 0) { Op = Op(session: s) });
        d.Ok<WriteAllResultMessage>(new WriteAllCommand("/src:meta", Spec(1, 0, true, 0, Inline(0, [9])), null, null, false, 0) { Op = Op(session: s) });
        ulong revision = d.Inode("/src").DefaultStream!.Revision;
        ulong st = d.Staging(0);
        ExtentRef extent = DataChange(0, 3, Obj(40), st).Extent!;
        CopyCommand Copy(string to, ulong rev, ExtentRef e) => new CopyCommand("/src", to, false, 0, "pair", null)
        {
            Op = Op(),
            Materialized = [new StreamMaterialization("", rev, [e])],
            LeaderStaging = st,
        };

        // The extents must cover the stream exactly and the source must still be at the materialized revision.
        d.Fails(DfsErrorCode.InvalidArgument, Copy("/bad", revision, extent with { Length = 2 }));
        st = d.Staging(0);
        extent = extent with { Staging = new StagingId(st) };
        d.Fails(DfsErrorCode.WriterConflict, Copy("/stale", revision + 100, extent) with { LeaderStaging = st });
        Assert.False(d.State.Staging[st].IsOpen);

        st = d.Staging(0);
        extent = extent with { Staging = new StagingId(st) };
        d.Ok<CopyResultMessage>(Copy("/dst", revision, extent) with { LeaderStaging = st });
        InodeRecord dst = d.Inode("/dst");
        Assert.False(dst.DefaultStream!.IsInline);
        Assert.Equal(Obj(40), dst.DefaultStream.Extents.Single().Object);
        Assert.True(dst.GetStream("meta")!.IsInline);
        Assert.Equal("pair", dst.PolicyName);
        Assert.False(d.State.Staging[st].IsOpen);
        Assert.Equal(new byte[] { 1, 2, 3 }, d.InlineBytes("/src"));
    }

    [Fact]
    public void Copy_ReadOnlyHandleSharingDeniesTheSourceRead()
    {
        (SmDriver d, ulong s, OpenHandleResultMessage h, _) = Writer("/src");
        d.Ok<CloseHandleResultMessage>(new CloseHandleCommand(h.Handle.Value, h.WriterGeneration) { Op = Op() });
        d.Open(s, "/src", FileMode.Open, FileAccess.Write, FileShare.None, 4);
        d.Fails(DfsErrorCode.SharingViolation, new CopyCommand("/src", "/dst", false, 0, null, null) { Op = Op() });
    }
}
