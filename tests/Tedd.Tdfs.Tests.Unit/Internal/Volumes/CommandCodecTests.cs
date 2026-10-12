using System.Collections.Immutable;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Volumes.Commands;
using Tedd.Tdfs.Server.Volumes.State;
using static Tedd.Tdfs.Tests.Unit.Internal.Volumes.SmDriver;

namespace Tedd.Tdfs.Tests.Unit.Internal.Volumes;

/// <summary>Every volume command kind round-trips through the canonical codec; malformed commands are protocol errors.</summary>
public sealed class CommandCodecTests
{
    private static readonly InodeRecord SampleInode = new(5, InodeKind.File, 9, 4, Epoch0, Epoch0.AddSeconds(1),
        new DfsSecurityDescriptor(DfsIdentity.User("u"), [new DfsAccessRule(DfsIdentity.Everyone, DfsFileSystemRights.Read)], true), "pair", 1, "n",
        InodeRecord.NoStreams.Add("", new StreamRecord(10, false, ReadOnlyMemory<byte>.Empty, [DataChange(0, 10, Obj(3), 2).Extent!], 9, 4))
            .Add("s", new StreamRecord(2, true, new byte[] { 1, 2 }, ImmutableList<ExtentRef>.Empty, 8, 7)),
        InodeRecord.NoChildren);

    public static TheoryData<int> Kinds()
    {
        var data = new TheoryData<int>();
        foreach (VolumeCommandKind kind in Enum.GetValues<VolumeCommandKind>())
            data.Add((int)kind);
        return data;
    }

    private static VolumeCommand Sample(VolumeCommandKind kind)
    {
        PublishSpec spec = Spec(20, 4, false, 3, DataChange(0, 10, Obj(1), 4), Zero(10, 2), Inline(12, [1, 2, 3]));
        VolumeCommand command = kind switch
        {
            VolumeCommandKind.OpenSession => new OpenSessionCommand("c", new UInt128(1, 2), TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2), 7, TimeSpan.FromSeconds(3)),
            VolumeCommandKind.KeepAlive => new KeepAliveCommand(3, [1, 2], [new PinAdvance(1, 9)], [4]),
            VolumeCommandKind.ExpireSessions => new ExpireSessionsCommand([1, 5]),
            VolumeCommandKind.ExpireHandles => new ExpireHandlesCommand([2]),
            VolumeCommandKind.FenceOperation => new FenceOperationCommand(new DfsOperationId(3, 4), true),
            VolumeCommandKind.ReserveOperationIds => new ReserveOperationIdsCommand(3, 64),
            VolumeCommandKind.CreateDirectory => new CreateDirectoryCommand("/a", true, "snap"),
            VolumeCommandKind.Move => new MoveCommand("/a", "/b", true, false, null),
            VolumeCommandKind.Delete => new DeleteCommand("/a", true, true, "x"),
            VolumeCommandKind.OpenHandle => new OpenHandleCommand("/f:s", FileMode.Append, FileAccess.Write, FileShare.Delete, FileOptions.Asynchronous, false, "pair", 3,
                TimeSpan.FromSeconds(2)),
            VolumeCommandKind.CloseHandle => new CloseHandleCommand(4, 2),
            VolumeCommandKind.CreateStaging => new CreateStagingCommand(3, 4, new DfsOperationId(3, 9)),
            VolumeCommandKind.CloseStaging => new CloseStagingCommand([7]),
            VolumeCommandKind.CommitFile => new CommitFileCommand(4, 2, 77, spec, "s"),
            VolumeCommandKind.WriteAll => new WriteAllCommand("/w", spec, "pair", null, true, 8),
            VolumeCommandKind.Append => new AppendCommand("/l", 0, 0, spec, true, true, null, "s"),
            VolumeCommandKind.Copy => new CopyCommand("/a", "/b", true, 6, null, null)
            {
                Materialized = [new StreamMaterialization("s", 5, [DataChange(0, 10, Obj(2), 4).Extent!])],
                LeaderStaging = 4,
            },
            VolumeCommandKind.ReclaimDetached => new ReclaimDetachedCommand([9]),
            VolumeCommandKind.TxBegin => new TxBeginCommand(3, Epoch0),
            VolumeCommandKind.TxCommit => new TxCommitCommand(new DfsOperationId(3, 1), new byte[] { 1, 2 }, null),
            VolumeCommandKind.TxAbort => new TxAbortCommand(new DfsOperationId(3, 1)),
            VolumeCommandKind.TxAcquireReservation => new TxAcquireReservationCommand(new DfsOperationId(3, 1), [new StreamKey(5, ""), new StreamKey(5, "s")]),
            VolumeCommandKind.CreateSnapshot => new CreateSnapshotCommand("snap", DfsConsistencyLabel.ApplicationConsistent,
                [new AdapterRecord("db", "1", ImmutableSortedDictionary<string, string>.Empty.Add("k", "v"))], Epoch0, true),
            VolumeCommandKind.DeleteSnapshot => new DeleteSnapshotCommand("snap"),
            VolumeCommandKind.SetSnapshotRetention => new SetSnapshotRetentionCommand("snap", null, true),
            VolumeCommandKind.AcquirePin => new AcquirePinCommand(PinKind.Enumeration, 3, null, 1, "bind", null),
            VolumeCommandKind.ReleasePin => new ReleasePinCommand(4),
            VolumeCommandKind.ExpirePins => new ExpirePinsCommand([4, 5]),
            VolumeCommandKind.PruneDerefLog => new PruneDerefLogCommand(99),
            VolumeCommandKind.RestoreChunk => new RestoreChunkCommand("src", 3, [SampleInode]),
            VolumeCommandKind.RestoreComplete => new RestoreCompleteCommand("src", 1),
            VolumeCommandKind.RestoreBegin => new RestoreBeginCommand(new DfsOperationId(3, 9), new UInt128(5, 6), "snap", "restored", "pool"),
            VolumeCommandKind.RestoreFinish => new RestoreFinishCommand(new DfsOperationId(3, 9), DfsErrorCode.PlacementUnavailable, "short", new byte[] { 1, 2 }),
            VolumeCommandKind.RecordBackupProgress => new RecordBackupProgressCommand(new BackupProgressRecord("t", "s", new SnapshotId(Guid.NewGuid()), 4, null,
                true, false, Guid.NewGuid(), Epoch0)),
            VolumeCommandKind.AbandonTransferLineage => new AbandonTransferLineageCommand("t", Guid.NewGuid()),
            VolumeCommandKind.SetPolicy => new SetPolicyCommand("/a", "pair")
            {
                Materialized = [new StreamMaterialization("", 3, [DataChange(0, 10, Obj(5), 6).Extent!])],
                LeaderStaging = 6,
            },
            VolumeCommandKind.SetAcl => new SetAclCommand("/a", AclChangeKind.Descriptor, SampleInode.Security, false, false),
            VolumeCommandKind.SetOwner => new SetOwnerCommand("/a", DfsIdentity.Group("g")),
            VolumeCommandKind.RotateDataKey => new RotateDataKeyCommand(2),
            VolumeCommandKind.MigratePolicy => new MigratePolicyCommand(DfsStoragePolicy.Default),
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "no sample"),
        };
        return command with { LeaderTime = Epoch0.AddTicks(12345), Op = Op("p", 3, 9, DfsOperationKind.Copy, new UInt128(7, 8), groups: "g") };
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    public void EveryCommandRoundTripsCanonically(int kindValue)
    {
        var kind = (VolumeCommandKind)kindValue;
        Assert.True(CommandCodec.IsKnown(kind));
        VolumeCommand command = Sample(kind);
        byte[] encoded = CommandCodec.Encode(command);
        Assert.Equal(kind, CommandCodec.PeekKind(encoded));
        VolumeCommand decoded = CommandCodec.Decode(encoded);
        Assert.Equal(command.GetType(), decoded.GetType());
        Assert.Equal(command.LeaderTime, decoded.LeaderTime);
        Assert.Equal(command.Op!.Id, decoded.Op!.Id);
        Assert.Equal(command.Op.Identities, decoded.Op.Identities);
        Assert.Equal(encoded, CommandCodec.Encode(decoded));
    }

    [Fact]
    public void MalformedCommandsAreProtocolErrors()
    {
        byte[] encoded = CommandCodec.Encode(Sample(VolumeCommandKind.Move));
        Assert.Throws<DfsProtocolException>(() => CommandCodec.Decode(encoded.AsSpan(0, encoded.Length - 2)));
        Assert.Throws<DfsProtocolException>(() => CommandCodec.Decode([.. encoded, 0]));
        byte[] unknown = (byte[])encoded.Clone();
        unknown[1] = 0xEE;
        unknown[2] = 0xEE;
        Assert.Equal(DfsErrorCode.Unsupported, Assert.Throws<DfsProtocolException>(() => CommandCodec.Decode(unknown)).ErrorCode);
        byte[] format = (byte[])encoded.Clone();
        format[0] = 99;
        Assert.Throws<DfsProtocolException>(() => CommandCodec.Decode(format));
    }
}
