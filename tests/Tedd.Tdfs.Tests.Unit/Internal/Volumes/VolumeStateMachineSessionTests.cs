using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Volumes.Apply;
using Tedd.Tdfs.Server.Volumes.Commands;
using Tedd.Tdfs.Server.Volumes.State;
using static Tedd.Tdfs.Tests.Unit.Internal.Volumes.SmDriver;

namespace Tedd.Tdfs.Tests.Unit.Internal.Volumes;

/// <summary>Sessions, the operation gate (ARCHITECTURE §5.2) and outcome retirement on the pure state machine.</summary>
public sealed class VolumeStateMachineSessionTests
{
    [Fact]
    public void InitialState_RootIsOwnedByTheCreatorWithTheDefaultAcl()
    {
        var d = new SmDriver();
        InodeRecord root = d.State.Root;
        Assert.Equal(InodeRecord.RootId, root.Id);
        Assert.True(root.IsDirectory);
        Assert.Equal(DfsIdentity.User("alice"), root.Owner);
        Assert.Equal(DfsSecurityDescriptor.DefaultRoot(DfsIdentity.User("alice")).Rules, root.Security.Rules);
        Assert.Single(d.State.Inodes);
    }

    [Fact]
    public void OpenSession_NonceRetryReturnsTheSameSession_NewNonceANewOne()
    {
        var d = new SmDriver();
        var nonce = new UInt128(5, 6);
        ulong first = d.OpenSession(nonce: nonce);
        ulong again = d.OpenSession(nonce: nonce);
        ulong other = d.OpenSession(nonce: new UInt128(5, 7));
        Assert.Equal(first, again);
        Assert.NotEqual(first, other);
        Assert.Equal(2, d.State.Sessions.Count);
        d.Fails(DfsErrorCode.AccessDenied, new OpenSessionCommand("x", new UInt128(1, 1), TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1), 4,
            TimeSpan.FromSeconds(1)) { Op = Op("bob", grants: DfsPermissions.None) });
    }

    [Fact]
    public void Gate_RetryReturnsTheStoredOutcomeWithoutASecondEffect()
    {
        var d = new SmDriver();
        ulong session = d.OpenSession();
        ulong staging = d.Staging(session);
        var append = new AppendCommand("/log", 0, 0, Spec(3, staging, true, -1, Inline(0, [1, 2, 3])), false, true, null, null)
        {
            Op = Op(session: session, sequence: 1, kind: DfsOperationKind.Append, payload: Hash("append-1")),
        };
        var first = d.Ok<AppendResultMessage>(append);
        var retry = d.Ok<AppendResultMessage>(append);
        Assert.Equal(first, retry);
        Assert.Equal(new byte[] { 1, 2, 3 }, d.InlineBytes("/log"));
        Assert.Equal(first.Token, retry.Token);
    }

    [Fact]
    public void Gate_SameIdWithAnotherPayloadOrPrincipalIsRejected()
    {
        var d = new SmDriver();
        ulong session = d.OpenSession();
        d.Ok<CreateDirectoryResultMessage>(new CreateDirectoryCommand("/a", false, null)
        {
            Op = Op(session: session, sequence: 1, kind: DfsOperationKind.CreateDirectory, payload: Hash("mkdir /a")),
        });
        d.Fails(DfsErrorCode.OperationIdMismatch, new CreateDirectoryCommand("/b", false, null)
        {
            Op = Op(session: session, sequence: 1, kind: DfsOperationKind.CreateDirectory, payload: Hash("mkdir /b")),
        });
        d.Fails(DfsErrorCode.OperationIdMismatch, new CreateDirectoryCommand("/a", false, null)
        {
            Op = Op("mallory", session: session, sequence: 1, kind: DfsOperationKind.CreateDirectory, payload: Hash("mkdir /a")),
        });
        Assert.False(d.Exists("/b"));
    }

    [Fact]
    public void Gate_AcknowledgedAndCountRetiredSequencesExpire()
    {
        var d = new SmDriver();
        ulong session = d.OpenSession(maxOutcomes: 3);
        for (ulong seq = 1; seq <= 5; seq++)
        {
            d.Ok<CreateDirectoryResultMessage>(new CreateDirectoryCommand("/d" + seq, false, null)
            {
                Op = Op(session: session, sequence: seq, kind: DfsOperationKind.CreateDirectory, payload: Hash("d" + seq)),
            });
        }

        SessionRecord record = d.State.Sessions[session];
        Assert.Equal(3, record.Outcomes.Count);
        Assert.Equal(2UL, record.RetiredWatermark);
        // A retired sequence is never executed as new (OUT-012).
        d.Fails(DfsErrorCode.OperationExpired, new CreateDirectoryCommand("/new", false, null)
        {
            Op = Op(session: session, sequence: 1, kind: DfsOperationKind.CreateDirectory, payload: Hash("other")),
        });
        Assert.False(d.Exists("/new"));

        // The client acknowledges everything below 5: outcomes 3 and 4 retire.
        d.Ok<CreateDirectoryResultMessage>(new CreateDirectoryCommand("/d6", false, null)
        {
            Op = Op(session: session, sequence: 6, kind: DfsOperationKind.CreateDirectory, payload: Hash("d6"), ackWatermark: 5),
        });
        record = d.State.Sessions[session];
        Assert.Equal(4UL, record.RetiredWatermark);
        Assert.Equal(new ulong[] { 5, 6 }, record.Outcomes.Keys.ToArray());
    }

    [Fact]
    public void Gate_ExpiredSessionsExpireTheirIdsAndRetireAfterRetention()
    {
        var d = new SmDriver();
        ulong session = d.OpenSession();
        d.Ok<CreateDirectoryResultMessage>(new CreateDirectoryCommand("/x", false, null)
        {
            Op = Op(session: session, sequence: 1, kind: DfsOperationKind.CreateDirectory, payload: Hash("x")),
        });
        d.Apply(new ExpireSessionsCommand([session]));
        Assert.Equal(SessionState.Expired, d.State.Sessions[session].State);
        // The retained outcome still answers (stored), a new sequence is expired.
        d.Ok<CreateDirectoryResultMessage>(new CreateDirectoryCommand("/x", false, null)
        {
            Op = Op(session: session, sequence: 1, kind: DfsOperationKind.CreateDirectory, payload: Hash("x")),
        });
        d.Fails(DfsErrorCode.OperationExpired, new CreateDirectoryCommand("/y", false, null)
        {
            Op = Op(session: session, sequence: 2, kind: DfsOperationKind.CreateDirectory, payload: Hash("y")),
        });

        d.Now += TimeSpan.FromMinutes(1);
        d.Apply(new ExpireSessionsCommand([]));
        Assert.False(d.State.Sessions.ContainsKey(session));
        Assert.Equal(session + 1, d.State.RetiredSessionsBelow);
        d.Fails(DfsErrorCode.OperationExpired, new CreateDirectoryCommand("/x", false, null)
        {
            Op = Op(session: session, sequence: 1, kind: DfsOperationKind.CreateDirectory, payload: Hash("x")),
        });
        d.Fails(DfsErrorCode.InvalidArgument, new CreateDirectoryCommand("/z", false, null)
        {
            Op = Op(session: 999, sequence: 1, kind: DfsOperationKind.CreateDirectory, payload: Hash("z")),
        });
    }

    [Fact]
    public void Fence_RecordsADefiniteAbortThatALateCopyReturns()
    {
        var d = new SmDriver();
        ulong session = d.OpenSession();
        var target = new DfsOperationId(session, 7);
        QueryOperationResultMessage before = VolumeStateMachine.QueryOutcome(d.State, target, "alice", out _, out bool fenceable);
        Assert.Equal(WireOutcomeStatus.NotFound, before.Status);
        Assert.True(fenceable);

        var fenced = d.Ok<QueryOperationResultMessage>(new FenceOperationCommand(target, false) { Op = Op(session: session) });
        Assert.Equal(WireOutcomeStatus.Aborted, fenced.Status);

        // The delayed original arrives later: it gets the fenced outcome and has no effect (OUT-013).
        VolumeApplyResult late = d.Apply(new CreateDirectoryCommand("/late", false, null)
        {
            Op = Op(session: session, sequence: 7, kind: DfsOperationKind.CreateDirectory, payload: Hash("late")),
        });
        Assert.Equal(DfsErrorCode.Cancelled, late.Code);
        Assert.False(d.Exists("/late"));
        Assert.Equal(WireOutcomeStatus.Aborted, VolumeStateMachine.QueryOutcome(d.State, target, "alice", out _, out _).Status);
        Assert.Throws<ApplyFailure>(() => VolumeStateMachine.QueryOutcome(d.State, target, "bob", out _, out _));
    }

    [Fact]
    public void QueryOutcome_ReportsCommittedWithTheOriginalReply()
    {
        var d = new SmDriver();
        ulong session = d.OpenSession();
        var reply = d.Ok<CreateDirectoryResultMessage>(new CreateDirectoryCommand("/q", false, null)
        {
            Op = Op(session: session, sequence: 3, kind: DfsOperationKind.CreateDirectory, payload: Hash("q")),
        });
        QueryOperationResultMessage answer = VolumeStateMachine.QueryOutcome(d.State, new DfsOperationId(session, 3), "alice", out _, out _);
        Assert.Equal(WireOutcomeStatus.Committed, answer.Status);
        Assert.Equal(reply.Token, answer.Token);
        Assert.Equal(DfsOperationKind.CreateDirectory, answer.Operation);
        Assert.Equal(reply, MessageCodec.Decode(answer.ResultFrame.Span));
    }

    [Fact]
    public void ReserveOperationIds_BlocksAreDisjointAndRetireWithTheOutcomeWindow()
    {
        var d = new SmDriver();
        ulong session = d.OpenSession(maxOutcomes: 2);
        var first = d.Ok<ReserveOperationIdsResultMessage>(new ReserveOperationIdsCommand(session, 4) { Op = Op(session: session) });
        var second = d.Ok<ReserveOperationIdsResultMessage>(new ReserveOperationIdsCommand(session, 2) { Op = Op(session: session) });
        Assert.Equal(1UL, first.FirstSequence);
        Assert.Equal(5UL, second.FirstSequence);

        // A reserved sequence is usable while the window has not passed it.
        d.Ok<CreateDirectoryResultMessage>(new CreateDirectoryCommand("/early", false, null)
        {
            Op = Op(session: session, sequence: 3, kind: DfsOperationKind.CreateDirectory, payload: Hash("early")),
        });
        Assert.True(d.Exists("/early"));

        // Many later operations: the window (2 outcomes) retires the oldest sequences, and with them the reserved, never used
        // sequence 2 (OUT-012: an id below the retired watermark is expired and never executes as a new request).
        var third = d.Ok<ReserveOperationIdsResultMessage>(new ReserveOperationIdsCommand(session, 10) { Op = Op(session: session) });
        Assert.Equal(7UL, third.FirstSequence);
        for (ulong seq = third.FirstSequence; seq < third.FirstSequence + 6; seq++)
        {
            d.Ok<CreateDirectoryResultMessage>(new CreateDirectoryCommand("/r" + seq, false, null)
            {
                Op = Op(session: session, sequence: seq, kind: DfsOperationKind.CreateDirectory, payload: Hash("r" + seq)),
            });
        }

        Assert.True(d.State.Sessions[session].RetiredWatermark >= 10);
        Assert.Equal(2, d.State.Sessions[session].Outcomes.Count);
        d.Fails(DfsErrorCode.OperationExpired, new CreateDirectoryCommand("/reserved", false, null)
        {
            Op = Op(session: session, sequence: 2, kind: DfsOperationKind.CreateDirectory, payload: Hash("reserved")),
        });
        Assert.False(d.Exists("/reserved"));
    }

    [Fact]
    public void KeepAlive_ReportsExpiredHandlesAndSessions()
    {
        var d = new SmDriver();
        ulong session = d.OpenSession();
        OpenHandleResultMessage open = d.Open(session, "/k", FileMode.Create, FileAccess.Write, FileShare.Read, 1);
        var reply = d.Ok<KeepAliveResultMessage>(new KeepAliveCommand(session, [open.Handle.Value, 999], [], [12345]) { Op = Op(session: session) });
        Assert.Equal(new[] { new HandleId(999) }, reply.ExpiredHandles);
        Assert.Equal(new ulong[] { 12345 }, reply.ExpiredPins);
        Assert.False(reply.SessionExpired);

        d.Apply(new ExpireSessionsCommand([session]));
        Assert.Empty(d.State.Handles);
        var expired = d.Ok<KeepAliveResultMessage>(new KeepAliveCommand(session, [open.Handle.Value], [], []) { Op = Op(session: session) });
        Assert.True(expired.SessionExpired);
    }
}
