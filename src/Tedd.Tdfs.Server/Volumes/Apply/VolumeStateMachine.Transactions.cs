using System.Collections.Immutable;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Volumes.Commands;
using Tedd.Tdfs.Server.Volumes.State;

namespace Tedd.Tdfs.Server.Volumes.Apply;

/// <summary>
/// Transactions (ARCHITECTURE §5.6, FSD §8): <c>TxBegin</c> records the transaction, its read index and staging session;
/// <c>TxAcquireReservation</c> takes writer reservations (conflicts fail immediately, LOCK-019); <c>TxCommit</c> validates
/// the read set against the state at the commit index and re-executes the transaction's writes atomically at that index
/// (strict serializability: the commit index is the serialization point); <c>TxAbort</c> ends an active transaction and
/// never reverses a commit.
/// </summary>
internal sealed partial class VolumeStateMachine
{
    /// <summary>Handle ids at or above this value belong to transactions' private working views (never to the replicated state).</summary>
    public const ulong TransactionHandleBase = 1UL << 62;

    /// <summary>Pin ids at or above this value belong to transactions' private working views.</summary>
    public const ulong TransactionPinBase = 1UL << 62;

    /// <summary>Placeholder transaction id of a leader dry run (the begin's real id is the operation id of the entry).</summary>
    private static readonly DfsOperationId DryRunTransaction = new(ulong.MaxValue, ulong.MaxValue);

    private static (VolumeState, VolumeApplyResult) ApplyTxBegin(VolumeState s, ApplyContext ctx, TxBeginCommand c)
    {
        RequireGrant(ctx, DfsPermissions.Read, "begin a transaction");
        OperationHeader op = ctx.Op ?? throw Fail(DfsErrorCode.InvalidArgument, "A transaction begin needs a principal.");
        DfsOperationId id = op.HasOperation ? op.Id : DryRunTransaction;
        SessionRecord session = RequireSession(s, ctx, c.Session);
        if (s.Transactions.ContainsKey(id))
            throw Fail(DfsErrorCode.AlreadyExists, $"Transaction {id} already exists.");
        var staging = new StagingRecord(s.NextStagingId, session.Id, 0, id, op.Principal, StagingState.Open, ctx.Index, 0, ctx.Now);
        var tx = new TxRecord(id, session.Id, op.Principal, TxState.Active, ctx.Index, staging.Id, 0, c.DeadlineUtc, ImmutableSortedSet.Create<StreamKey>(StreamKeyComparer.Instance),
            0);
        s = s with
        {
            NextStagingId = staging.Id + 1,
            Staging = s.Staging.Add(staging.Id, staging),
            Transactions = s.Transactions.SetItem(id, tx),
        };
        var reply = new TxBeginResultMessage(new DfsTransactionId(id), s.TokenAt(ctx.Index), new StagingId(staging.Id), ReadOnlyMemory<byte>.Empty, default,
            c.DeadlineUtc, 0, s.Config.Limits);
        return (s, VolumeApplyResult.Ok(reply, tx));
    }

    private static (VolumeState, VolumeApplyResult) ApplyTxAbort(VolumeState s, ApplyContext ctx, TxAbortCommand c)
    {
        if (!s.Transactions.TryGetValue(c.Transaction, out TxRecord? tx))
            return (s, VolumeApplyResult.Ok(new TxAbortResultMessage(WireOutcomeStatus.Aborted, CommitToken.None)));
        if (ctx.Op is { } op && !string.Equals(op.Principal, tx.Principal, StringComparison.Ordinal))
            throw Fail(DfsErrorCode.AccessDenied, $"Transaction {c.Transaction} belongs to another principal.");
        switch (tx.State)
        {
            case TxState.Committed:
                return (s, VolumeApplyResult.Ok(new TxAbortResultMessage(WireOutcomeStatus.Committed, s.TokenAt(tx.FinalIndex))));
            case TxState.Aborted:
                return (s, VolumeApplyResult.Ok(new TxAbortResultMessage(WireOutcomeStatus.Aborted, tx.FinalIndex == 0 ? CommitToken.None : s.TokenAt(tx.FinalIndex))));
        }

        s = EndTransaction(s, tx, TxState.Aborted, ctx.Index);
        return (s, VolumeApplyResult.Ok(new TxAbortResultMessage(WireOutcomeStatus.Aborted, s.TokenAt(ctx.Index)), tx));
    }

    private static (VolumeState, VolumeApplyResult) ApplyTxAcquireReservation(VolumeState s, ApplyContext ctx, TxAcquireReservationCommand c)
    {
        TxRecord tx = RequireActiveTransaction(s, ctx, c.Transaction);
        ApplyContext own = ctx with { Transaction = tx.Id };
        ImmutableSortedSet<StreamKey> reservations = tx.Reservations;
        foreach (StreamKey key in OrEmpty(c.Streams))
        {
            if (reservations.Contains(key))
                continue;
            foreach (HandleRecord h in HandlesOf(s, key))
            {
                if (h.IsWriter || (h.Share & FileShare.Write) == 0)
                {
                    throw Fail(DfsErrorCode.SharingViolation,
                        $"Transaction {tx.Id} cannot reserve the stream for writing: handle {h.Id} ({h.Access}, share {h.Share}) is open on it.");
                }
            }

            if (ReservationHolder(s, own, key) is { } other)
                throw Fail(DfsErrorCode.SharingViolation, $"Transaction {tx.Id} cannot reserve the stream for writing: transaction {other.Id} holds it.");
            if (reservations.Count >= s.Config.Limits.MaxTransactionTouchedInodes)
            {
                throw Fail(DfsErrorCode.LimitExceeded, $"Transaction {tx.Id} reserves too many streams.",
                    LimitDetail(nameof(DfsVolumeLimits.MaxTransactionTouchedInodes), s.Config.Limits.MaxTransactionTouchedInodes, reservations.Count + 1));
            }

            reservations = reservations.Add(key);
        }

        s = s with { Transactions = s.Transactions.SetItem(tx.Id, tx with { Reservations = reservations }) };
        return (s, VolumeApplyResult.Ok(null));
    }

    private static (VolumeState, VolumeApplyResult) ApplyTxCommit(VolumeState s, ApplyContext ctx, TxCommitCommand c)
    {
        if (!s.Transactions.TryGetValue(c.Transaction, out TxRecord? tx))
            throw Fail(DfsErrorCode.TransactionAborted, $"Transaction {c.Transaction} is unknown (it ended or never began).");
        if (ctx.Op is { } op && !string.Equals(op.Principal, tx.Principal, StringComparison.Ordinal))
            throw Fail(DfsErrorCode.AccessDenied, $"Transaction {c.Transaction} belongs to another principal.");
        switch (tx.State)
        {
            case TxState.Committed:
                return (s, VolumeApplyResult.Ok(new TxCommitResultMessage(s.TokenAt(tx.FinalIndex), null)));
            case TxState.Aborted:
                throw Fail(DfsErrorCode.TransactionAborted, $"Transaction {c.Transaction} was aborted.");
        }

        if (ctx.Now > tx.DeadlineUtc)
            return Abandon(s, tx, ctx, DfsErrorCode.TransactionAborted, $"Transaction {tx.Id} passed its deadline {tx.DeadlineUtc:O} before it committed.");
        if (c.Descriptor.Length > s.Config.Limits.MaxTransactionDescriptorBytes)
        {
            return Abandon(s, tx, ctx, DfsErrorCode.LimitExceeded,
                $"Transaction {tx.Id} needs a {c.Descriptor.Length}-byte descriptor; the limit is {s.Config.Limits.MaxTransactionDescriptorBytes}.");
        }

        TxDescriptor descriptor;
        try
        {
            descriptor = TxDescriptor.Decode(c.Descriptor.Span);
        }
        catch (DfsProtocolException ex)
        {
            throw Fail(DfsErrorCode.InvalidArgument, "Malformed transaction descriptor: " + ex.Message);
        }

        // Validation: everything the transaction read is unchanged at the commit index.
        foreach (TxLookup lookup in descriptor.Lookups)
        {
            ulong now = s.Find(lookup.Directory) is { IsDirectory: true } directory ? directory.Children.GetValueOrDefault(lookup.Name) : ulong.MaxValue;
            if (now != lookup.Child)
            {
                return Abandon(s, tx, ctx, DfsErrorCode.TransactionConflict,
                    $"Transaction {tx.Id} conflicts: the entry '{lookup.Name}' of directory {lookup.Directory} changed after the transaction read it.");
            }
        }

        foreach (TxRead read in descriptor.Reads)
        {
            if (s.Find(read.Inode)?.Revision != read.Revision)
                return Abandon(s, tx, ctx, DfsErrorCode.TransactionConflict, $"Transaction {tx.Id} conflicts: inode {read.Inode} changed after the transaction read it.");
        }

        // The writes, re-executed in order at the commit index under the committing principal's current identities.
        VolumeState w = s;
        var handles = new Dictionary<ulong, HandleRecord>();
        foreach (TxWrite write in descriptor.Writes)
        {
            VolumeCommand command;
            try
            {
                command = CommandCodec.Decode(write.Command.Span);
            }
            catch (DfsProtocolException ex)
            {
                throw Fail(DfsErrorCode.InvalidArgument, "Malformed transaction write: " + ex.Message);
            }

            if (command is CloseHandleCommand close)
            {
                if (handles.Remove(close.Handle, out HandleRecord? opened) && w.Handles.TryGetValue(opened.Id, out HandleRecord? live))
                    w = ReleaseHandle(w, live, ctx.Index);
                continue;
            }

            command = RemapTransactionHandles(command, handles);
            if (ctx.Op is { } commitOp)
                command = command with { Op = commitOp with { Id = default, Operation = command.OperationKind, PayloadHash = UInt128.Zero, AckWatermark = 0 } };
            var replay = new ApplyContext(ctx.Index, ctx.Term, command) { Transaction = tx.Id, Replay = true };
            (VolumeState next, VolumeApplyResult result) = ExecuteGuarded(w, replay);
            if (!result.IsSuccess)
            {
                return Abandon(s, tx, ctx, result.Code == DfsErrorCode.AccessDenied ? DfsErrorCode.AccessDenied : DfsErrorCode.TransactionConflict,
                    $"Transaction {tx.Id} cannot commit: its {command.Kind} no longer applies ({result.Code}: {result.Message}).");
            }

            if (command is OpenHandleCommand && write.WorkingHandle != 0 && result.Value is OpenValue openValue)
                handles[write.WorkingHandle] = openValue.Handle;
            w = next;
        }

        // Handles the transaction opened end with it.
        foreach (HandleRecord opened in handles.Values)
        {
            if (w.Handles.TryGetValue(opened.Id, out HandleRecord? live))
                w = ReleaseHandle(w, live, ctx.Index);
        }

        (w, SnapshotInfoData? snapshot) = SnapshotInSameEntry(w, ctx, c.SnapshotName);
        w = EndTransaction(w, w.Transactions[tx.Id], TxState.Committed, ctx.Index);
        return (w, VolumeApplyResult.Ok(new TxCommitResultMessage(w.TokenAt(ctx.Index), snapshot), tx));
    }

    /// <summary>Ends <paramref name="tx"/> without effect (conflict, deadline, limits) and fails the commit with <paramref name="code"/>.</summary>
    private static (VolumeState, VolumeApplyResult) Abandon(VolumeState s, TxRecord tx, ApplyContext ctx, DfsErrorCode code, string message)
        => (EndTransaction(s, tx, TxState.Aborted, ctx.Index), VolumeApplyResult.Fail(code, message));

    /// <summary>The handle and generation of a working-view handle replaced by the handle its replayed open created.</summary>
    private static VolumeCommand RemapTransactionHandles(VolumeCommand command, Dictionary<ulong, HandleRecord> handles) => command switch
    {
        CommitFileCommand commit when handles.TryGetValue(commit.Handle, out HandleRecord? h)
            => commit with { Handle = h.Id, Generation = h.WriterGeneration },
        AppendCommand append when append.Handle != 0 && handles.TryGetValue(append.Handle, out HandleRecord? h)
            => append with { Handle = h.Id, Generation = h.WriterGeneration },
        _ => command,
    };

    /// <summary>The active transaction <paramref name="id"/> of the caller.</summary>
    private static TxRecord RequireActiveTransaction(VolumeState s, ApplyContext ctx, DfsOperationId id)
    {
        if (!s.Transactions.TryGetValue(id, out TxRecord? tx) || tx.State != TxState.Active)
            throw Fail(DfsErrorCode.TransactionAborted, $"Transaction {id} is not active.");
        if (ctx.Op is { } op && !string.Equals(op.Principal, tx.Principal, StringComparison.Ordinal))
            throw Fail(DfsErrorCode.AccessDenied, $"Transaction {id} belongs to another principal.");
        return tx;
    }

    /// <summary>Records the final state of a transaction and releases its reservations, pin and staging session.</summary>
    internal static VolumeState EndTransaction(VolumeState s, TxRecord tx, TxState state, ulong index)
    {
        s = s with
        {
            Transactions = s.Transactions.SetItem(tx.Id, tx with { State = state, FinalIndex = index, Reservations = tx.Reservations.Clear() }),
            Pins = tx.PinId == 0 ? s.Pins : s.Pins.Remove(tx.PinId),
        };
        if (s.Staging.TryGetValue(tx.Staging, out StagingRecord? staging))
            s = CloseStaging(s, staging, index);
        return s;
    }

    /// <summary>
    /// The commit outcome of transaction <paramref name="target"/> as an outcome query reports it (PROTOCOL.md §9.3,
    /// <c>IsTransaction</c>): Committed with its token, Aborted, or Pending while active (<paramref name="fenceable"/>: a
    /// resolving query aborts it so the answer is definite).
    /// </summary>
    internal static QueryOperationResultMessage QueryTransaction(VolumeState s, DfsOperationId target, string principal, out bool fenceable)
    {
        fenceable = false;

        // No transaction record: its begin never executed (or the record is retired). The begin's operation id is then
        // answered like any operation id, and a resolving query fences it (ARCHITECTURE 7: NotFound only without resolve).
        if (!s.Transactions.TryGetValue(target, out TxRecord? tx))
            return QueryOutcome(s, target, principal, out _, out fenceable);
        if (!string.Equals(tx.Principal, principal, StringComparison.Ordinal))
            throw Fail(DfsErrorCode.OperationIdMismatch, $"Transaction {target} belongs to another principal.");
        switch (tx.State)
        {
            case TxState.Committed:
                return new QueryOperationResultMessage(target, WireOutcomeStatus.Committed, s.TokenAt(tx.FinalIndex), DfsErrorCode.None, null,
                    DfsOperationKind.TxCommit, ReadOnlyMemory<byte>.Empty);
            case TxState.Aborted:
                return new QueryOperationResultMessage(target, WireOutcomeStatus.Aborted, CommitToken.None, DfsErrorCode.TransactionAborted,
                    "The transaction was aborted.", DfsOperationKind.TxCommit, ReadOnlyMemory<byte>.Empty);
            default:
                fenceable = true;
                return new QueryOperationResultMessage(target, WireOutcomeStatus.Pending, CommitToken.None, DfsErrorCode.None, "The transaction is active.",
                    DfsOperationKind.TxCommit, ReadOnlyMemory<byte>.Empty);
        }
    }
}
