using System.Buffers;
using System.Collections.Immutable;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Control;
using Tedd.Tdfs.Server.Raft;
using Tedd.Tdfs.Server.Volumes.Commands;
using Tedd.Tdfs.Server.Volumes.State;

namespace Tedd.Tdfs.Server.Volumes.Apply;

/// <summary>
/// The replicated volume state machine (ARCHITECTURE §5, INTERNALS.md §1.2): decodes each committed command, runs it through
/// the operation gate (§5.2: expired / mismatch / stored outcome / execute, then outcome retirement) and applies the feature
/// handler to the immutable <see cref="VolumeState"/>. Apply is synchronous and deterministic: it reads no clock, randomness
/// or node state; time, identities and leader decisions travel inside the command.
/// </summary>
internal sealed partial class VolumeStateMachine : IReplicatedStateMachine, ICommandDescriber, IVolumeStateInspection
{
    private VolumeState _state;

    public VolumeStateMachine(VolumeCreation creation, Guid clusterId)
    {
        ArgumentNullException.ThrowIfNull(creation);
        Creation = creation;
        ClusterId = clusterId;
        _state = VolumeState.Initial(creation, clusterId);
    }

    /// <summary>Creation data (the input of the initial state).</summary>
    public VolumeCreation Creation { get; }

    /// <summary>Cluster id.</summary>
    public Guid ClusterId { get; }

    /// <summary>The applied state (immutable; replaced by every apply).</summary>
    public VolumeState State => _state;

    /// <inheritdoc />
    public string GroupName => Creation.Name;

    /// <inheritdoc />
    public ulong AppliedIndex => _state.AppliedIndex;

    /// <summary>Raised after every apply or checkpoint restore with the new state (node-loop observers such as the leader service).</summary>
    public event Action<VolumeState>? Changed;

    /// <inheritdoc />
    public ApplyOutcome Apply(ulong index, ulong term, ReadOnlyMemory<byte> command)
    {
        VolumeCommand decoded;
        try
        {
            decoded = CommandCodec.Decode(command.Span);
        }
        catch (DfsProtocolException ex)
        {
            _state = _state with { AppliedIndex = index };
            Changed?.Invoke(_state);
            return ApplyOutcome.Failure(ex.ErrorCode, "Malformed volume command: " + ex.Message);
        }

        var ctx = new ApplyContext(index, term, decoded);
        (VolumeState next, VolumeApplyResult result) = ApplyCommand(_state with { AppliedIndex = index, LeaderTime = decoded.LeaderTime }, ctx);
        _state = next;
        Changed?.Invoke(_state);
        return result.IsSuccess ? ApplyOutcome.Success(result) : ApplyOutcome.Failure(result.Code, result.Message, result);
    }

    /// <summary>
    /// Applies one decoded command to <paramref name="state"/> (whose applied index is already the entry's index). Pure:
    /// the same state and command always yield the same result (used directly by tests and by apply).
    /// </summary>
    public static (VolumeState State, VolumeApplyResult Result) ApplyCommand(VolumeState state, ApplyContext ctx)
    {
        OperationHeader? op = ctx.Op;
        if (op is not { HasOperation: true })
            return ExecuteGuarded(state, ctx);

        GateDecision gate = OperationGate.Check(state, op);
        switch (gate.Kind)
        {
            case GateKind.Expired:
                return (Acknowledge(state, op), Rejected(DfsErrorCode.OperationExpired, gate.Message, op));
            case GateKind.Mismatch:
                return (Acknowledge(state, op), Rejected(DfsErrorCode.OperationIdMismatch, gate.Message, op));
            case GateKind.Invalid:
                return (Acknowledge(state, op), Rejected(DfsErrorCode.InvalidArgument, gate.Message, op));
            case GateKind.Stored:
                return (Acknowledge(state, op), StoredResult(gate.Outcome!));
        }

        (VolumeState next, VolumeApplyResult result) = ExecuteGuarded(state, ctx);
        if (!result.IsSuccess && result.Reply is ErrorReplyMessage error && error.OperationId is null)
            result = result with { Reply = error with { OperationId = op.Id } };
        next = OperationGate.Record(next, op, result, ctx.Index);
        return (Acknowledge(next, op), result);
    }

    /// <summary>The reply of a retained outcome (decoded from its stored frame).</summary>
    public static VolumeApplyResult StoredResult(OutcomeRecord outcome)
    {
        IWireMessage? reply = outcome.Reply.IsEmpty ? null : MessageCodec.Decode(outcome.Reply.Span);
        return new VolumeApplyResult(outcome.Code, outcome.Message, reply, null, Replayed: true);
    }

    private static VolumeApplyResult Rejected(DfsErrorCode code, string message, OperationHeader op)
        => new(code, message, new ErrorReplyMessage(code, message, op.Id), null);

    private static VolumeState Acknowledge(VolumeState state, OperationHeader op)
        => op.RequestSession != 0 && op.AckWatermark > 1 ? OperationGate.Acknowledge(state, op.RequestSession, op.AckWatermark) : state;

    /// <summary>Runs the feature handler; handler failures leave the state unchanged except for the applied index.</summary>
    private static (VolumeState State, VolumeApplyResult Result) ExecuteGuarded(VolumeState state, ApplyContext ctx)
    {
        try
        {
            return Execute(state, ctx);
        }
        catch (ApplyFailure failure)
        {
            return (state, VolumeApplyResult.Fail(failure.Code, failure.Message, failure.Detail));
        }
    }

    private static (VolumeState State, VolumeApplyResult Result) Execute(VolumeState s, ApplyContext ctx) => ctx.Command switch
    {
        OpenSessionCommand c => ApplyOpenSession(s, ctx, c),
        KeepAliveCommand c => ApplyKeepAlive(s, ctx, c),
        ExpireSessionsCommand c => ApplyExpireSessions(s, ctx, c),
        ExpireHandlesCommand c => ApplyExpireHandles(s, ctx, c),
        FenceOperationCommand c => ApplyFence(s, ctx, c),
        ReserveOperationIdsCommand c => ApplyReserve(s, ctx, c),

        CreateDirectoryCommand c => ApplyCreateDirectory(s, ctx, c),
        MoveCommand c => ApplyMove(s, ctx, c),
        DeleteCommand c => ApplyDelete(s, ctx, c),

        OpenHandleCommand c => ApplyOpenHandle(s, ctx, c),
        CloseHandleCommand c => ApplyCloseHandle(s, ctx, c),
        CreateStagingCommand c => ApplyCreateStaging(s, ctx, c),
        CloseStagingCommand c => ApplyCloseStaging(s, ctx, c),
        CommitFileCommand c => ApplyCommitFile(s, ctx, c),
        WriteAllCommand c => ApplyWriteAll(s, ctx, c),
        AppendCommand c => ApplyAppend(s, ctx, c),
        CopyCommand c => ApplyCopy(s, ctx, c),
        ReclaimDetachedCommand c => ApplyReclaimDetached(s, ctx, c),

        TxBeginCommand or TxCommitCommand or TxAbortCommand or TxAcquireReservationCommand => ApplyTransaction(s, ctx),

        CreateSnapshotCommand c => ApplyCreateSnapshot(s, ctx, c),
        DeleteSnapshotCommand c => ApplyDeleteSnapshot(s, ctx, c),
        SetSnapshotRetentionCommand c => ApplySetRetention(s, ctx, c),
        AcquirePinCommand c => ApplyAcquirePin(s, ctx, c),
        ReleasePinCommand c => ApplyReleasePin(s, ctx, c),
        ExpirePinsCommand c => ApplyExpirePins(s, ctx, c),

        PruneDerefLogCommand c => ApplyPrune(s, ctx, c),
        RestoreChunkCommand c => ApplyRestoreChunk(s, ctx, c),
        RestoreCompleteCommand c => ApplyRestoreComplete(s, ctx, c),
        RestoreBeginCommand c => ApplyRestoreBegin(s, ctx, c),
        RestoreFinishCommand c => ApplyRestoreFinish(s, ctx, c),

        RecordBackupProgressCommand c => ApplyBackupProgress(s, ctx, c),
        AbandonTransferLineageCommand c => ApplyAbandonLineage(s, ctx, c),

        SetPolicyCommand c => ApplySetPolicy(s, ctx, c),
        SetAclCommand c => ApplySetAcl(s, ctx, c),
        SetOwnerCommand c => ApplySetOwner(s, ctx, c),
        RotateDataKeyCommand c => ApplyRotateKey(s, ctx, c),
        MigratePolicyCommand c => ApplyMigratePolicy(s, ctx, c),
        _ => (s, VolumeApplyResult.Fail(DfsErrorCode.Unsupported, $"Volume command {ctx.Command.Kind} is not supported.")),
    };

    /// <inheritdoc />
    public void WriteCheckpoint(IBufferWriter<byte> output) => StateCodec.WriteCheckpoint(output, _state);

    /// <inheritdoc />
    public void RestoreCheckpoint(ReadOnlySequence<byte> data, ulong index, ulong term)
    {
        VolumeState restored = StateCodec.ReadCheckpoint(data.IsSingleSegment ? data.FirstSpan : data.ToArray());
        if (restored.Config.Id != Creation.Id)
            throw new DfsIntegrityException($"The checkpoint belongs to volume {restored.Config.Id}, not {Creation.Id}.");
        _state = restored with { AppliedIndex = index };
        Changed?.Invoke(_state);
    }

    /// <inheritdoc />
    public ProposalContext Describe(ReadOnlyMemory<byte> command)
    {
        try
        {
            VolumeCommand c = CommandCodec.Decode(command.Span);
            DfsOperationId? id = c.Op is { HasOperation: true } op ? op.Id : null;
            return new ProposalContext(c.OperationKind, id, c.Path);
        }
        catch (DfsProtocolException)
        {
            return ProposalContext.None;
        }
    }

    // ------------------------------------------------------------------------------------------------- helpers

    /// <summary>Fails the command (no effect).</summary>
    internal static ApplyFailure Fail(DfsErrorCode code, string message, ErrorDetailData? detail = null) => new(code, message, detail);

    /// <summary>Requires a volume grant carried in the operation header (the leader resolved it).</summary>
    internal static void RequireGrant(ApplyContext ctx, DfsPermissions permission, string what)
    {
        if (ctx.Op is { } op && (op.Grants & permission) != permission)
            throw Fail(DfsErrorCode.AccessDenied, $"Principal '{op.Principal}' lacks the {permission} grant on this volume ({what}).");
    }

    /// <summary>Normalizes a path carried by a command (the leader validated it; apply re-validates, PATH-012).</summary>
    internal static string NormalizePath(string path)
    {
        if (!DfsPath.TryNormalize(path, out string normalized, out string? error))
            throw Fail(DfsErrorCode.InvalidPath, error ?? $"Invalid path '{path}'.");
        return normalized;
    }

    /// <summary>The commit-and-snapshot name of a command, validated.</summary>
    internal static string? SnapshotNameOf(string? name)
    {
        if (name is null)
            return null;
        if (SnapshotNames.GetError(name) is { } error)
            throw Fail(DfsErrorCode.InvalidArgument, error);
        return name;
    }

    private static ImmutableArray<T> OrEmpty<T>(ImmutableArray<T> values) => values.IsDefault ? [] : values;
}
