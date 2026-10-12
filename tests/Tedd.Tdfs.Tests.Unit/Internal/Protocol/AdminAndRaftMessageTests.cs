using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;

namespace Tedd.Tdfs.Tests.Unit.Internal.Protocol;

/// <summary>Administrative body envelopes, secret redaction, Raft envelopes and outcome/reply embedding.</summary>
public sealed class AdminAndRaftMessageTests
{
    [Fact]
    public void AdminRequestCarriesTypedBodies()
    {
        var spec = new AdminVolumeSpecBody("vol", new DfsMetadataPolicy { Voters = 5 }, DfsStoragePolicy.Default,
            new Dictionary<string, DfsStoragePolicy> { ["hot"] = new() { TargetCopies = 2, TargetDistinctZones = 2, DurableCopiesBeforeAck = 2, DurableZonesBeforeAck = 2 } },
            4096, new DfsVolumeLimits());
        AdminRequestMessage request = AdminRequestMessage.Create(AdminOp.CreateVolume, new UInt128(1, 2), spec);
        var decoded = MessageCodec.Decode<AdminRequestMessage>(MessageCodec.Encode(request));
        Assert.Equal(AdminOp.CreateVolume, decoded.Op);
        Assert.Equal(new UInt128(1, 2), decoded.RequestId);
        WireDeepEquality.AssertEqual(spec, decoded.ReadBody<AdminVolumeSpecBody>());

        // Reading the body as the wrong record is a protocol error, never a silently misread value.
        Assert.Throws<DfsProtocolException>(() => decoded.ReadBody<AdminGrantBody>());
    }

    [Fact]
    public void AdminResultCarriesTypedBodies()
    {
        var health = new ReplicaHealthData(10, 1, 2, 0, 0, 4096, 3, 100, 200);
        AdminResultMessage result = AdminResultMessage.Create(AdminOp.ReplicaHealth, health);
        var decoded = MessageCodec.Decode<AdminResultMessage>(MessageCodec.Encode(result));
        Assert.Equal(health, decoded.ReadBody<ReplicaHealthData>() with { Extensions = health.Extensions });
        Assert.IsType<AdminEmptyBody>(AdminResultMessage.Create(AdminOp.DrainNode, new AdminEmptyBody()).ReadBody<AdminEmptyBody>());
    }

    [Fact]
    public void UnknownAdminOperationsSurviveDecodingForAnUnsupportedReply()
    {
        var request = new AdminRequestMessage((AdminOp)999, UInt128.One, ReadOnlyMemory<byte>.Empty);
        Assert.Equal((AdminOp)999, MessageCodec.Decode<AdminRequestMessage>(MessageCodec.Encode(request)).Op);
    }

    [Fact]
    public void SecretsAreRedactedFromDiagnostics()
    {
        string principal = new AdminCreatePrincipalBody("alice", "hunter2", false).ToString();
        Assert.Contains("alice", principal, StringComparison.Ordinal);
        Assert.DoesNotContain("hunter2", principal, StringComparison.Ordinal);

        Assert.DoesNotContain("pass-phrase", new AdminExportRecoveryKeyBody("pass-phrase").ToString(), StringComparison.Ordinal);

        string restore = new AdminRestoreFromBackupBody(["mem://b0"], new DfsCredentials("reader", "topsecret"), "vol", "s1", "vol2", new byte[] { 9, 9 }).ToString();
        Assert.DoesNotContain("topsecret", restore, StringComparison.Ordinal);

        var register = new BackupRegisterMessage(new RequestHeader("vol"), BackupTargetAction.Register, "b0", ["mem://b0"],
            new DfsCredentials("export", "exportsecret"), null, null);
        Assert.DoesNotContain("exportsecret", register.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public void RaftBatchCarriesEveryMessageKind()
    {
        var a = new NodeId(Guid.Parse("00000000-0000-0000-0000-00000000000a"));
        var b = new NodeId(Guid.Parse("00000000-0000-0000-0000-00000000000b"));
        var messages = new List<RaftEnvelope>();
        foreach (RaftWireKind kind in Enum.GetValues<RaftWireKind>())
        {
            var message = new RaftWireMessage(kind, a, b, 7, 10, 6, 9, 42, 3, 2, kind == RaftWireKind.VoteRequest, false, kind == RaftWireKind.AppendResponse,
                kind == RaftWireKind.AppendEntries ? [new RaftWireEntry(11, 7, 0, new byte[] { 1, 2, 3 }), new RaftWireEntry(12, 7, 2, ReadOnlyMemory<byte>.Empty)] : [],
                kind == RaftWireKind.InstallSnapshot ? new RaftWireSnapshotMeta(100, 6, [a, b], []) : null,
                kind == RaftWireKind.InstallSnapshot ? 4096 : 0,
                kind == RaftWireKind.InstallSnapshot ? new byte[] { 5, 6 } : ReadOnlyMemory<byte>.Empty,
                kind == RaftWireKind.InstallSnapshot);
            messages.Add(new RaftEnvelope(ProtocolInfo.ControlGroup, message));
        }

        var batch = new RaftBatchMessage(a, 3, messages);
        MessageRoundTripTests.AssertRoundTrip(batch, "raft batch");
        var decoded = MessageCodec.Decode<RaftBatchMessage>(MessageCodec.Encode(batch));
        Assert.Equal(Enum.GetValues<RaftWireKind>().Length, decoded.Messages.Count);
        RaftWireMessage append = decoded.Messages.Single(m => m.Message.Kind == RaftWireKind.AppendEntries).Message;
        Assert.Equal(2, append.Entries.Count);
        Assert.Equal(new byte[] { 1, 2, 3 }, append.Entries[0].Data.ToArray());
        RaftWireMessage install = decoded.Messages.Single(m => m.Message.Kind == RaftWireKind.InstallSnapshot).Message;
        Assert.Equal(100UL, install.Snapshot!.Index);
        Assert.True(install.Done);
    }

    [Fact]
    public void QueryOperationResultEmbedsTheOriginalReplyFrame()
    {
        var original = new AppendResultMessage(new InodeId(5), 77, 10, 15, false, new CommitToken(Guid.NewGuid(), new VolumeId(Guid.NewGuid()), 1, 77), null);
        var query = new QueryOperationResultMessage(new DfsOperationId(3, 9), WireOutcomeStatus.Committed, original.Token, DfsErrorCode.None, null,
            DfsOperationKind.Append, MessageCodec.Encode(original));
        var decoded = MessageCodec.Decode<QueryOperationResultMessage>(MessageCodec.Encode(query));
        var embedded = MessageCodec.Decode<AppendResultMessage>(decoded.ResultFrame.Span);
        WireDeepEquality.AssertEqual(original, embedded);
        Assert.Equal(DfsOperationKind.Append, decoded.Operation);
    }

    [Fact]
    public void TxOpEnvelopeCarriesCompleteFrames()
    {
        var tx = new DfsTransactionId(new DfsOperationId(4, 1));
        var header = new RequestHeader("vol", Session: 4, Operation: new DfsOperationId(4, 2), Transaction: tx);
        byte[] inner = MessageCodec.Encode(new CreateDirectoryMessage(header, "/a", Recursive: true, SnapshotName: null));
        var envelope = new TxOpMessage(header with { Operation = default }, [inner]);
        var decoded = MessageCodec.Decode<TxOpMessage>(MessageCodec.Encode(envelope));
        var innerDecoded = MessageCodec.Decode<CreateDirectoryMessage>(decoded.Requests[0].Span);
        Assert.Equal(tx, innerDecoded.Header.Transaction);
        Assert.Equal("/a", innerDecoded.Path);
    }

    [Fact]
    public void ErrorReplyKeepsCodecCompatibility()
    {
        byte[] frame = ErrorCodec.Encode(DfsErrorCode.NotLeader, "not leader", new DfsOperationId(1, 1), retryable: true);
        var message = MessageCodec.Decode<ErrorReplyMessage>(frame);
        Assert.Empty(message.Extensions);
        (DfsErrorCode code, string text, DfsOperationId? op, bool retryable) = ErrorCodec.Decode(frame);
        Assert.Equal(DfsErrorCode.NotLeader, code);
        Assert.Equal("not leader", text);
        Assert.Equal(new DfsOperationId(1, 1), op);
        Assert.True(retryable);
    }
}
