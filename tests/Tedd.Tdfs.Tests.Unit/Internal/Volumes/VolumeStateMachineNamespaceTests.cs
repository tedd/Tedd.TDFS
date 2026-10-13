using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Volumes.Apply;
using Tedd.Tdfs.Server.Volumes.Commands;
using Tedd.Tdfs.Server.Volumes.State;
using static Tedd.Tdfs.Tests.Unit.Internal.Volumes.SmDriver;

namespace Tedd.Tdfs.Tests.Unit.Internal.Volumes;

/// <summary>Namespace commands (CreateDirectory, Move, Delete) and handles (OpenHandle, CloseHandle, sharing, fencing).</summary>
public sealed class VolumeStateMachineNamespaceTests
{
    private static CreateDirectoryCommand Mkdir(string path, bool recursive = true) => new(path, recursive, null) { Op = Op() };

    [Fact]
    public void CreateDirectory_RecursiveIsIdempotentAndRejectsFilesInTheWay()
    {
        var d = new SmDriver();
        var created = d.Ok<CreateDirectoryResultMessage>(Mkdir("/a/b/c"));
        Assert.True(created.Created);
        Assert.Equal("/a/b/c", created.Info.Path);
        Assert.Equal(4, d.State.Inodes.Count);
        ulong revision = d.Inode("/a/b/c").Revision;

        var again = d.Ok<CreateDirectoryResultMessage>(Mkdir("/a/b/c"));
        Assert.False(again.Created);
        Assert.Equal(created.Info.Inode, again.Info.Inode);
        Assert.Equal(revision, d.Inode("/a/b/c").Revision);

        d.Fails(DfsErrorCode.DirectoryNotFound, Mkdir("/x/y", recursive: false));
        ulong session = d.OpenSession();
        d.Open(session, "/a/file", FileMode.CreateNew, FileAccess.Write, FileShare.None, 1);
        d.Fails(DfsErrorCode.AlreadyExists, Mkdir("/a/file"));
        d.Fails(DfsErrorCode.AlreadyExists, Mkdir("/a/file/sub"));
        d.Fails(DfsErrorCode.InvalidPath, Mkdir("/a/d:s"));
        Assert.Equal(created.Info.Inode.Value + 1, d.Inode("/a/file").Id);
    }

    [Fact]
    public void Move_RenamesAtomicallyAndKeepsTheInode()
    {
        var d = new SmDriver();
        d.Ok<CreateDirectoryResultMessage>(Mkdir("/src/sub"));
        d.Ok<CreateDirectoryResultMessage>(Mkdir("/dst"));
        ulong id = d.Inode("/src/sub").Id;
        var moved = d.Ok<MoveResultMessage>(new MoveCommand("/src/sub", "/dst/renamed", false, true, null) { Op = Op() });
        Assert.Equal(id, moved.Inode.Value);
        Assert.Equal(d.Index, moved.Revision);
        Assert.False(d.Exists("/src/sub"));
        Assert.Equal(id, d.Inode("/dst/renamed").Id);
        Assert.Equal(d.Index, d.Inode("/src").Revision);
        Assert.Equal(d.Index, d.Inode("/dst").Revision);
    }

    [Fact]
    public void Move_RejectsCyclesKindMismatchAndNonOverwriteConflicts()
    {
        var d = new SmDriver();
        d.Ok<CreateDirectoryResultMessage>(Mkdir("/a/b/c"));
        d.Fails(DfsErrorCode.InvalidArgument, new MoveCommand("/a", "/a/b/c/a", false, true, null) { Op = Op() });
        d.Fails(DfsErrorCode.InvalidArgument, new MoveCommand("/a/b", "/a/b/x", false, true, null) { Op = Op() });
        d.Fails(DfsErrorCode.FileNotFound, new MoveCommand("/a/b", "/z", false, false, null) { Op = Op() });
        d.Fails(DfsErrorCode.DirectoryNotFound, new MoveCommand("/nope", "/z", false, true, null) { Op = Op() });

        ulong s = d.OpenSession();
        OpenHandleResultMessage f = d.Open(s, "/f", FileMode.CreateNew, FileAccess.Write, FileShare.None, 1);
        OpenHandleResultMessage g = d.Open(s, "/g", FileMode.CreateNew, FileAccess.Write, FileShare.Delete, 2);
        d.Fails(DfsErrorCode.AlreadyExists, new MoveCommand("/f", "/g", false, false, null) { Op = Op() });
        // The source handle does not grant FileShare.Delete: renaming is a sharing violation.
        d.Fails(DfsErrorCode.SharingViolation, new MoveCommand("/f", "/h", false, false, null) { Op = Op() });
        d.Ok<CloseHandleResultMessage>(new CloseHandleCommand(f.Handle.Value, f.WriterGeneration) { Op = Op() });
        // Overwrite: the replaced /g stays attached to its handle (detached), the source takes the name.
        d.Ok<MoveResultMessage>(new MoveCommand("/f", "/g", true, false, null) { Op = Op() });
        Assert.Equal(f.Info.Inode.Value, d.Inode("/g").Id);
        Assert.Contains(g.Info.Inode.Value, d.State.Detached);
        d.Ok<CloseHandleResultMessage>(new CloseHandleCommand(g.Handle.Value, g.WriterGeneration) { Op = Op() });
        Assert.DoesNotContain(g.Info.Inode.Value, d.State.Detached);
        Assert.False(d.State.Inodes.ContainsKey(g.Info.Inode.Value));
        d.Fails(DfsErrorCode.AlreadyExists, new MoveCommand("/g", "/a", true, false, null) { Op = Op() });
    }

    [Fact]
    public void Delete_FilesDirectoriesAndRecursiveDetach()
    {
        var d = new SmDriver();
        var absent = d.Ok<DeleteResultMessage>(new DeleteCommand("/missing", false, false, null) { Op = Op() });
        Assert.False(absent.Existed);
        d.Fails(DfsErrorCode.DirectoryNotFound, new DeleteCommand("/missing", false, true, null) { Op = Op() });

        d.Ok<CreateDirectoryResultMessage>(Mkdir("/t/u"));
        ulong s = d.OpenSession();
        OpenHandleResultMessage h = d.Open(s, "/t/u/file", FileMode.CreateNew, FileAccess.Write, FileShare.Read | FileShare.Delete, 1);
        d.Fails(DfsErrorCode.IsADirectory, new DeleteCommand("/t", false, false, null) { Op = Op() });
        d.Fails(DfsErrorCode.NotADirectory, new DeleteCommand("/t/u/file", false, true, null) { Op = Op() });
        d.Fails(DfsErrorCode.DirectoryNotEmpty, new DeleteCommand("/t", false, true, null) { Op = Op() });
        d.Fails(DfsErrorCode.InvalidArgument, new DeleteCommand("/", true, true, null) { Op = Op() });

        int before = d.State.Inodes.Count;
        var deleted = d.Ok<DeleteResultMessage>(new DeleteCommand("/t", true, true, null) { Op = Op() });
        Assert.True(deleted.Existed);
        Assert.False(d.Exists("/t"));
        // The open file survives detached; the directories are reclaimed at once.
        Assert.Equal(before - 2, d.State.Inodes.Count);
        Assert.Contains(h.Info.Inode.Value, d.State.Detached);
        d.Ok<CloseHandleResultMessage>(new CloseHandleCommand(h.Handle.Value, h.WriterGeneration) { Op = Op() });
        Assert.Equal(before - 3, d.State.Inodes.Count);
    }

    [Fact]
    public void Delete_FileWithHandleNotGrantingDeleteIsASharingViolation()
    {
        var d = new SmDriver();
        ulong s = d.OpenSession();
        d.Open(s, "/f", FileMode.CreateNew, FileAccess.Write, FileShare.Read, 1);
        d.Fails(DfsErrorCode.SharingViolation, new DeleteCommand("/f", false, false, null) { Op = Op() });
        Assert.True(d.Exists("/f"));
    }

    [Fact]
    public void OpenHandle_ModesFollowFsd61()
    {
        var d = new SmDriver();
        ulong s = d.OpenSession();
        d.Fails(DfsErrorCode.FileNotFound, OpenCommand(s, "/m", FileMode.Open, FileAccess.Read, FileShare.Read));
        d.Fails(DfsErrorCode.FileNotFound, OpenCommand(s, "/m", FileMode.Truncate, FileAccess.Write, FileShare.Read));
        d.Fails(DfsErrorCode.DirectoryNotFound, OpenCommand(s, "/nodir/m", FileMode.CreateNew, FileAccess.Write, FileShare.Read));
        OpenHandleResultMessage created = d.Open(s, "/m", FileMode.CreateNew, FileAccess.Write, FileShare.Read, 1);
        Assert.True(created.Created);
        Assert.Equal(1UL, created.WriterGeneration);
        Assert.Equal(d.Index, created.Info.Revision);
        d.Fails(DfsErrorCode.AlreadyExists, OpenCommand(s, "/m", FileMode.CreateNew, FileAccess.Write, FileShare.Read));
        d.Ok<CloseHandleResultMessage>(new CloseHandleCommand(created.Handle.Value, created.WriterGeneration) { Op = Op() });

        // A non-creating open does not change the revision; a writable one advances the generation.
        ulong revision = d.Inode("/m").Revision;
        OpenHandleResultMessage writer = d.Open(s, "/m", FileMode.Open, FileAccess.ReadWrite, FileShare.Read, 2);
        Assert.False(writer.Created);
        Assert.Equal(2UL, writer.WriterGeneration);
        Assert.Equal(revision, d.Inode("/m").Revision);
        d.Ok<CloseHandleResultMessage>(new CloseHandleCommand(writer.Handle.Value, writer.WriterGeneration) { Op = Op() });

        // Invalid argument combinations are rejected by apply as well as by the leader.
        d.Fails(DfsErrorCode.InvalidArgument, OpenCommand(s, "/m", FileMode.Truncate, FileAccess.ReadWrite, FileShare.Read));
        d.Fails(DfsErrorCode.InvalidArgument, OpenCommand(s, "/m", FileMode.Append, FileAccess.ReadWrite, FileShare.Read));
        d.Fails(DfsErrorCode.InvalidArgument, OpenCommand(s, "/m", FileMode.Create, FileAccess.Read, FileShare.Read));
        d.Fails(DfsErrorCode.InvalidArgument, OpenCommand(s, "/m", FileMode.Open, FileAccess.Write, FileShare.Write));
        d.Fails(DfsErrorCode.InvalidArgument, OpenCommand(s, "/m", FileMode.Open, FileAccess.Read, FileShare.Inheritable));
        d.Fails(DfsErrorCode.InvalidArgument, OpenCommand(s, "/m", (FileMode)7, FileAccess.Read, FileShare.Read));
        d.Fails(DfsErrorCode.InvalidArgument, OpenCommand(s, "/m", FileMode.Open, FileAccess.Write, FileShare.Read, pinned: true));
        d.Fails(DfsErrorCode.IsADirectory, OpenCommand(s, "/", FileMode.Open, FileAccess.Read, FileShare.Read));
    }

    [Fact]
    public void OpenHandle_CreateAndTruncateEmptyTheStreamAndDereferenceItsObjects()
    {
        var d = new SmDriver();
        ulong s = d.OpenSession();
        OpenHandleResultMessage w = d.Open(s, "/t", FileMode.CreateNew, FileAccess.Write, FileShare.Read, 1);
        ulong staging = d.Staging(s, w.Handle.Value);
        d.Ok<CommitFileResultMessage>(new CommitFileCommand(w.Handle.Value, w.WriterGeneration, 0, Spec(8192, staging, false, -1,
            DataChange(0, 8192, Obj(1), staging)), null) { Op = Op(session: s) });
        d.Ok<CloseHandleResultMessage>(new CloseHandleCommand(w.Handle.Value, w.WriterGeneration) { Op = Op() });
        OpenHandleResultMessage t = d.Open(s, "/t", FileMode.Truncate, FileAccess.Write, FileShare.Read, 2);
        Assert.True(t.Truncated);
        Assert.Equal(0, t.Info.Length);
        Assert.Contains(d.State.DerefLog, e => e.Object == Obj(1) && e.Index == d.Index);
    }

    [Fact]
    public void OpenHandle_SharingIsBidirectionalAndWritersAreExclusive()
    {
        var d = new SmDriver();
        ulong s = d.OpenSession();
        d.Open(s, "/s", FileMode.CreateNew, FileAccess.Write, FileShare.Read, 1);
        // Existing writer granted only Read: a second writer and a reader not granting Write both conflict.
        d.Fails(DfsErrorCode.WriterConflict, OpenCommand(s, "/s", FileMode.Open, FileAccess.Write, FileShare.Read));
        d.Fails(DfsErrorCode.SharingViolation, OpenCommand(s, "/s", FileMode.Open, FileAccess.Read, FileShare.Read));
        OpenHandleResultMessage reader = d.Open(s, "/s", FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        Assert.Equal(0UL, reader.WriterGeneration);
        Assert.Equal(2, d.State.Handles.Count);
    }

    [Fact]
    public void Streams_HaveIndependentSharingAndDeleteNeedsShareDeleteOnEveryStream()
    {
        var d = new SmDriver();
        ulong s = d.OpenSession();
        OpenHandleResultMessage named = d.Open(s, "/f:a", FileMode.CreateNew, FileAccess.Write, FileShare.Read, 1);
        Assert.True(named.Created);
        Assert.True(named.Info.IsStream);
        Assert.Equal("a", named.Info.StreamName);
        Assert.Equal(1, named.Info.StreamCount);
        InodeRecord f = d.Inode("/f");
        Assert.NotNull(f.DefaultStream);
        Assert.NotNull(f.GetStream("a"));

        // Writers on different streams do not conflict; two on the same stream do.
        d.Open(s, "/f", FileMode.Open, FileAccess.Write, FileShare.Read | FileShare.Delete, 2);
        d.Open(s, "/f:b", FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read | FileShare.Delete, 3);
        d.Fails(DfsErrorCode.WriterConflict, OpenCommand(s, "/f:a", FileMode.Open, FileAccess.Write, FileShare.Read));
        d.Fails(DfsErrorCode.FileNotFound, OpenCommand(s, "/f:missing", FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

        // /f:a lacks FileShare.Delete: deleting or renaming the file is refused.
        d.Fails(DfsErrorCode.SharingViolation, new DeleteCommand("/f", false, false, null) { Op = Op() });
        d.Fails(DfsErrorCode.SharingViolation, new MoveCommand("/f", "/g", false, false, null) { Op = Op() });

        // Deleting one stream removes only it.
        d.Ok<DeleteResultMessage>(new DeleteCommand("/f:b", false, false, null) { Op = Op() });
        Assert.Null(d.Inode("/f").GetStream("b"));
        Assert.NotNull(d.Inode("/f").GetStream("a"));
        d.Fails(DfsErrorCode.InvalidPath, new DeleteCommand("/f:a", false, true, null) { Op = Op() });
    }

    [Fact]
    public void Streams_LimitIsEnforced()
    {
        var d = new SmDriver();
        ulong s = d.OpenSession();
        for (int i = 0; i < DfsStreamPath.MaxStreamsPerInode; i++)
        {
            OpenHandleResultMessage h = d.Open(s, "/many:s" + i, FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read, (ulong)(i + 1));
            d.Ok<CloseHandleResultMessage>(new CloseHandleCommand(h.Handle.Value, h.WriterGeneration) { Op = Op() });
        }

        VolumeApplyResult over = d.Fails(DfsErrorCode.LimitExceeded, OpenCommand(s, "/many:extra", FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read));
        Assert.Equal("MaxStreamsPerInode", Assert.IsType<ErrorReplyMessage>(over.Reply).Detail?.LimitName);
        Assert.Equal(DfsStreamPath.MaxStreamsPerInode, d.Inode("/many").NamedStreamCount);
    }

    [Fact]
    public void HandleLimits_PerSessionAreEnforced()
    {
        var d = new SmDriver(CreationFor(limits: new DfsVolumeLimits { MaxHandlesPerSession = 2 }));
        ulong s = d.OpenSession();
        d.Open(s, "/a", FileMode.CreateNew, FileAccess.Write, FileShare.Read, 1);
        d.Open(s, "/a", FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        d.Fails(DfsErrorCode.LimitExceeded, OpenCommand(s, "/a", FileMode.Open, FileAccess.Read, FileShare.ReadWrite));
    }

    [Fact]
    public void ExpireHandles_ReleasesReservationsAndReclaimsDetachedInodes()
    {
        var d = new SmDriver();
        ulong s = d.OpenSession();
        OpenHandleResultMessage w = d.Open(s, "/e", FileMode.CreateNew, FileAccess.Write, FileShare.Read | FileShare.Delete, 1);
        ulong staging = d.Staging(s, w.Handle.Value);
        d.Ok<DeleteResultMessage>(new DeleteCommand("/e", false, false, null) { Op = Op() });
        Assert.Contains(w.Info.Inode.Value, d.State.Detached);
        d.Apply(new ExpireHandlesCommand([w.Handle.Value]));
        Assert.Empty(d.State.Handles);
        Assert.False(d.State.Staging[staging].IsOpen);
        Assert.False(d.State.Inodes.ContainsKey(w.Info.Inode.Value));
        Assert.Empty(d.State.Detached);
    }

    [Fact]
    public void CloseHandle_IsIdempotentAndRejectsForeignHandles()
    {
        var d = new SmDriver();
        ulong s = d.OpenSession();
        OpenHandleResultMessage h = d.Open(s, "/c", FileMode.CreateNew, FileAccess.Write, FileShare.Read, 1);
        d.Fails(DfsErrorCode.InvalidHandle, new CloseHandleCommand(h.Handle.Value, h.WriterGeneration) { Op = Op("bob") });
        Assert.True(d.Ok<CloseHandleResultMessage>(new CloseHandleCommand(h.Handle.Value, h.WriterGeneration) { Op = Op() }).WasOpen);
        Assert.False(d.Ok<CloseHandleResultMessage>(new CloseHandleCommand(h.Handle.Value, h.WriterGeneration) { Op = Op() }).WasOpen);
        d.Fails(DfsErrorCode.InvalidHandle, new CloseHandleCommand(4242, 0) { Op = Op() });
    }
}
