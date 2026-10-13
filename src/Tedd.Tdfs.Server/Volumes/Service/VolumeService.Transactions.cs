using System.Collections.Immutable;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Runtime;
using Tedd.Tdfs.Server.Volumes.Apply;
using Tedd.Tdfs.Server.Volumes.Commands;
using Tedd.Tdfs.Server.Volumes.State;

namespace Tedd.Tdfs.Server.Volumes.Service;

/// <summary>
/// Transactions on the leader (ARCHITECTURE §5.6, PROTOCOL.md §8.4). A transaction's operations run against a private
/// working view kept in leader memory: the namespace as of the transaction's read version plus its own changes, merged with
/// the live handles, sessions and staging sessions. Transaction-bound requests are served by the ordinary handlers with the
/// working view as their state (an ambient binding for the request); their mutations apply to the view instead of the log,
/// record the name lookups and inode versions they depended on (the read set) and append themselves to the ordered write
/// list. Writer reservations are real (<c>TxAcquireReservation</c> entries; conflicts fail immediately). <c>TxCommit</c> carries
/// the read set and the writes; apply validates and re-executes them atomically at the commit index. A leader change loses
/// the working views: the new leader aborts such transactions (TransactionAborted, retryable by the application).
/// </summary>
internal sealed partial class VolumeService
{
    private static readonly AsyncLocal<(VolumeService Owner, TxWork Work)?> Ambient = new();
    private readonly Dictionary<DfsOperationId, TxWork> _transactions = [];

    /// <summary>The applied index at which this leader's term became ready (transactions begun at or below it are not its own).</summary>
    private ulong _transactionsSince;

    /// <summary>The transaction the current request is bound to (on this service), or null.</summary>
    private TxWork? CurrentTransaction => Ambient.Value is { } bound && ReferenceEquals(bound.Owner, this) ? bound.Work : null;

    /// <summary>
    /// Binds the current request to its transaction (idempotent): the request then sees the transaction's working view and its
    /// mutations apply there. Records the paths the request depends on in the read set. Fails with TransactionAborted when the
    /// transaction is not active on this leader.
    /// </summary>
    private TxWork? BindTransaction(IVolumeRequest request, string principal)
    {
        if (request.Header.Transaction is not { } id)
            return null;
        if (CurrentTransaction is { } bound && bound.Id == id.Operation)
        {
            Track(bound, request);
            return bound;
        }

        TxWork tx = ActiveTransaction(id.Operation, principal);
        Ambient.Value = (this, tx);
        Track(tx, request);
        return tx;
    }

    /// <summary>The working state of an active transaction of <paramref name="principal"/> on this leader.</summary>
    private TxWork ActiveTransaction(DfsOperationId id, string principal)
    {
        if (!IsLeader)
            throw Group.NotLeader();
        if (!_transactions.TryGetValue(id, out TxWork? tx))
        {
            if (_sm.State.Transactions.TryGetValue(id, out TxRecord? record) && record.State == TxState.Committed)
                throw new DfsException(DfsErrorCode.TransactionAborted, $"Transaction {id} has committed; it accepts no further operations.");
            throw new DfsException(DfsErrorCode.TransactionAborted,
                $"Transaction {id} is not active on this leader (it ended, or a leader change discarded its private view); begin a new transaction.");
        }

        if (!string.Equals(tx.Principal, principal, StringComparison.Ordinal))
            throw new UnauthorizedAccessException($"Transaction {id} belongs to another principal.");
        if (!_sm.State.Transactions.TryGetValue(id, out TxRecord? live) || live.State != TxState.Active)
        {
            _transactions.Remove(id);
            throw new DfsException(DfsErrorCode.TransactionAborted, $"Transaction {id} was aborted (expiry, deadline or a resolving outcome query).");
        }

        return tx;
    }

    /// <summary>Requests that can never be transaction-bound (sessions, snapshots, restore).</summary>
    private static void RejectTransaction(IVolumeRequest request)
    {
        if (request.Header.Transaction is { } tx)
            throw new ArgumentException($"{request.GetType().Name.Replace("Message", string.Empty, StringComparison.Ordinal)} cannot be part of transaction {tx}.");
    }

    // ------------------------------------------------------------------------------------------------- begin / commit / abort

    /// <summary><c>TxBegin</c>: a committed entry records the transaction; the leader creates its working view from the read version.</summary>
    public async Task<IWireMessage> TxBeginAsync(RpcContext context, TxBeginMessage request)
    {
        string principal = context.Principal ?? string.Empty;
        RequireGrant(principal, DfsPermissions.Read, "begin a transaction");
        if (request.Header.Transaction is not null)
            throw new ArgumentException("A transaction cannot begin inside another transaction.");
        if (request.Timeout is { } requested && requested <= TimeSpan.Zero)
            throw new ArgumentException("The transaction timeout must be positive.");
        TimeSpan limit = _sm.State.Config.Limits.MaxTransactionDuration;
        TimeSpan duration = request.Timeout is { } timeout && timeout < limit ? timeout : limit;
        return await MutateAsync(context, request, DfsOperationKind.TxBegin,
            (op, _) => Task.FromResult<VolumeCommand>(Stamp(new TxBeginCommand(request.Header.Session, _s.Time.GetUtcNow() + duration), op)),
            result =>
            {
                var begun = (TxBeginResultMessage)result.Reply!;
                DfsOperationId id = begun.Transaction.Operation;
                if (!_transactions.ContainsKey(id) && _sm.State.Transactions.TryGetValue(id, out TxRecord? record) && record.State == TxState.Active)
                {
                    VolumeState root = _sm.TransactionRoot(id) ?? _sm.State;
                    _transactions[id] = new TxWork(id, record.Principal, root);
                }

                byte[] capability = _s.Objects.Capabilities.IssueStaging(Record, principal, begun.Staging, StagingCapabilityLifetime, out DateTimeOffset expiry);
                return begun with { Capability = capability, CapabilityExpiry = expiry, PlacementEpoch = PlacementEpochForUploads() };
            },
            bindTransaction: false);
    }

    /// <summary>
    /// <c>TxCommit</c>: the read set and the ordered writes of the working view are proposed in one entry; apply validates and
    /// re-executes them atomically (TransactionConflict, no effect, when anything the transaction read changed).
    /// </summary>
    public async Task<IWireMessage> TxCommitAsync(RpcContext context, TxCommitMessage request)
    {
        string principal = context.Principal ?? string.Empty;
        DfsOperationId id = (request.Header.Transaction ?? throw new ArgumentException("TxCommit names its transaction in the request header.")).Operation;
        if (request.SnapshotName is { } snapshot && SnapshotNames.GetError(snapshot) is { } snapshotError)
            throw new ArgumentException(snapshotError);
        IWireMessage reply = await MutateAsync(context, request, DfsOperationKind.TxCommit, (op, _) =>
        {
            byte[] descriptor = [];
            if (_transactions.TryGetValue(id, out TxWork? tx))
            {
                if (!string.Equals(tx.Principal, principal, StringComparison.Ordinal))
                    throw new UnauthorizedAccessException($"Transaction {id} belongs to another principal.");
                descriptor = tx.Describe().Encode();
            }
            else if (_sm.State.Transactions.TryGetValue(id, out TxRecord? record) && record.State == TxState.Active)
            {
                // The working view is gone (leader change): the transaction cannot commit what it did.
                throw new DfsException(DfsErrorCode.TransactionAborted,
                    $"Transaction {id} lost its private view in a leader change; it is aborted and must be retried.");
            }

            return Task.FromResult<VolumeCommand>(Stamp(new TxCommitCommand(id, descriptor, request.SnapshotName), op));
        }, bindTransaction: false);
        if (!(_sm.State.Transactions.TryGetValue(id, out TxRecord? after) && after.State == TxState.Active))
            DropTransaction(id);
        return reply;
    }

    /// <summary><c>TxAbort</c>: ends the transaction (idempotent; reports a commit instead of reversing it).</summary>
    public async Task<IWireMessage> TxAbortAsync(RpcContext context, TxAbortMessage request)
    {
        DfsOperationId id = (request.Header.Transaction ?? throw new ArgumentException("TxAbort names its transaction in the request header.")).Operation;
        IWireMessage reply = await MutateAsync(context, request, DfsOperationKind.TxAbort,
            (op, _) => Task.FromResult<VolumeCommand>(Stamp(new TxAbortCommand(id), op)), requireOperation: false, bindTransaction: false);
        if (reply is TxAbortResultMessage)
            DropTransaction(id);
        return reply;
    }

    /// <summary><c>TxOp</c> (the optional batching envelope) is not implemented; transaction-bound requests are sent individually.</summary>
    public Task<IWireMessage> TxOpAsync(RpcContext context, TxOpMessage request)
    {
        _ = context;
        return Task.FromResult<IWireMessage>(new ErrorReplyMessage(DfsErrorCode.Unsupported,
            "TxOp batching is not supported; send transaction-bound requests individually.", null, false, null, request.Header.Transaction));
    }

    /// <summary>Forgets a transaction's working view and closes the leader staging sessions it created.</summary>
    private void DropTransaction(DfsOperationId id)
    {
        if (!_transactions.Remove(id, out TxWork? tx))
            return;
        if (tx.LeaderStagings.Count > 0)
            _ = ProposeHousekeepingAsync(new CloseStagingCommand([.. tx.LeaderStagings]));
    }

    /// <summary>
    /// Leader maintenance of transactions: aborts transactions past their deadline and those begun under an earlier leader
    /// (their private views were lost), and forgets views of transactions that ended.
    /// </summary>
    private async Task MaintainTransactionsAsync(DateTimeOffset wall)
    {
        VolumeState s = _sm.State;
        foreach (DfsOperationId id in _transactions.Keys.ToArray())
        {
            if (!s.Transactions.TryGetValue(id, out TxRecord? record) || record.State != TxState.Active)
                DropTransaction(id);
        }

        foreach (TxRecord tx in s.Transactions.Values.Where(t => t.State == TxState.Active).ToArray())
        {
            bool expired = tx.DeadlineUtc <= wall;
            bool orphaned = !_transactions.ContainsKey(tx.Id) && tx.ReadIndex <= _transactionsSince;
            if (!expired && !orphaned)
                continue;
            DropTransaction(tx.Id);
            await ProposeHousekeepingAsync(new TxAbortCommand(tx.Id));
        }
    }

    // ------------------------------------------------------------------------------------------------- working view

    /// <summary>The transaction's working view: its private namespace and handles over the live state.</summary>
    private VolumeState WorkingView(TxWork tx)
    {
        VolumeState real = _sm.State;
        ImmutableSortedDictionary<ulong, HandleRecord> handles = real.Handles;
        foreach (KeyValuePair<ulong, HandleRecord> h in tx.Handles)
            handles = handles.SetItem(h.Key, h.Value);
        ImmutableSortedDictionary<ulong, PinRecord> pins = real.Pins;
        foreach (KeyValuePair<ulong, PinRecord> p in tx.Pins)
            pins = pins.SetItem(p.Key, p.Value);
        ImmutableSortedDictionary<StreamKey, ulong> generations = real.WriterGenerations;
        foreach (KeyValuePair<StreamKey, ulong> g in tx.WriterGenerations)
            generations = generations.SetItem(g.Key, g.Value);
        return real with
        {
            Inodes = tx.Inodes,
            Detached = tx.Detached,
            NextInodeId = tx.NextInodeId,
            Handles = handles,
            NextHandleId = tx.NextHandleId,
            Pins = pins,
            NextPinId = tx.NextPinId,
            WriterGenerations = generations,
            TransactionView = true,
        };
    }

    /// <summary>Takes the transaction-private parts of a working view after a mutation applied to it.</summary>
    private static void Absorb(TxWork tx, VolumeState next)
    {
        tx.Inodes = next.Inodes;
        tx.Detached = next.Detached;
        tx.NextInodeId = next.NextInodeId;
        tx.Handles = ImmutableSortedDictionary.CreateRange(next.Handles.Where(h => h.Key >= VolumeStateMachine.TransactionHandleBase));
        tx.NextHandleId = Math.Max(next.NextHandleId, VolumeStateMachine.TransactionHandleBase);
        tx.Pins = ImmutableSortedDictionary.CreateRange(next.Pins.Where(p => p.Key >= VolumeStateMachine.TransactionPinBase));
        tx.NextPinId = Math.Max(next.NextPinId, VolumeStateMachine.TransactionPinBase);
        ImmutableSortedDictionary<StreamKey, ulong> generations = ImmutableSortedDictionary.Create<StreamKey, ulong>(StreamKeyComparer.Instance);
        foreach (HandleRecord h in tx.Handles.Values.Where(h => h.IsWriter))
            generations = generations.SetItem(h.Key, next.WriterGenerationOf(h.Key));
        tx.WriterGenerations = generations;
    }

    /// <summary>
    /// Applies a transaction-bound mutation to the working view (instead of the log): takes the writer reservations it needs
    /// first (real entries), then runs the same pure apply logic, and records the write for the commit.
    /// </summary>
    private async Task<VolumeApplyResult> ApplyInTransactionAsync(TxWork tx, VolumeCommand command, CancellationToken ct)
    {
        tx = ActiveTransaction(tx.Id, tx.Principal);

        // Limits are checked before anything is reserved or applied (no partial effect, PUB-024).
        long published = PublishedBytes(command);
        long stagedLimit = _sm.State.Config.Limits.MaxTransactionStagedBytes;
        if (published > 0 && tx.StagedBytes > stagedLimit - published)
            throw new DfsLimitExceededException(nameof(DfsVolumeLimits.MaxTransactionStagedBytes), stagedLimit, tx.StagedBytes + published);
        OperationHeader? header = command.Op is { } op ? Refresh(op) : null;
        List<StreamKey> keys = ReservationKeys(tx, command);
        if (keys.Count > 0 && header is not null)
        {
            OperationHeader reservationOp = header with { Id = default, Operation = DfsOperationKind.TxOp, PayloadHash = UInt128.Zero, AckWatermark = 0 };
            VolumeApplyResult reserved = await ProposeAsync(Stamp(new TxAcquireReservationCommand(tx.Id, [.. keys]), reservationOp), ct, bypassTransaction: true);
            if (!reserved.IsSuccess)
                return WithOperation(reserved, command.Op);
            foreach (StreamKey key in keys)
                tx.Reserved.Add(key);
            tx = ActiveTransaction(tx.Id, tx.Principal);
        }

        VolumeCommand final = command with { Op = header is null ? null : header with { Id = default, AckWatermark = 0 }, LeaderTime = _s.Time.GetUtcNow() };
        ulong index = tx.NextIndex++;
        VolumeState view = WorkingView(tx) with { AppliedIndex = index, LeaderTime = final.LeaderTime };
        var ctx = new ApplyContext(index, Group.Term, final) { Transaction = tx.Id };
        (VolumeState next, VolumeApplyResult result) = VolumeStateMachine.ApplyCommand(view, ctx);
        if (!result.IsSuccess)
            return WithOperation(result, command.Op);
        Absorb(tx, next);
        RecordWrite(tx, final, result);
        tx.StagedBytes += published;
        return result;
    }

    /// <summary>Bytes a publication command publishes (data and inline changes; holes publish nothing).</summary>
    private static long PublishedBytes(VolumeCommand command)
    {
        PublishSpec? spec = command switch
        {
            CommitFileCommand c => c.Spec,
            WriteAllCommand w => w.Spec,
            AppendCommand a => a.Spec,
            _ => null,
        };
        if (spec is null || spec.Changes.IsDefault)
            return 0;
        long total = 0;
        foreach (StreamChange change in spec.Changes)
        {
            if (change.ChangeKind != RangeChangeKind.Zero)
                total = total > long.MaxValue - change.Length ? long.MaxValue : total + change.Length;
        }

        return total;
    }

    private static VolumeApplyResult WithOperation(VolumeApplyResult result, OperationHeader? op)
        => op is { HasOperation: true } && result.Reply is ErrorReplyMessage { OperationId: null } error
            ? result with { Reply = error with { OperationId = op.Id } }
            : result;

    /// <summary>The live streams a transaction-bound mutation writes (they need the transaction's writer reservation).</summary>
    private static List<StreamKey> ReservationKeys(TxWork tx, VolumeCommand command)
    {
        string? path = command switch
        {
            OpenHandleCommand o when (o.Access & FileAccess.Write) != 0 => o.FilePath,
            WriteAllCommand w => w.TargetPath,
            AppendCommand a when a.Handle == 0 => a.TargetPath,
            CopyCommand c => c.Destination,
            _ => null,
        };
        var keys = new List<StreamKey>();
        if (path is null || !DfsPath.TryNormalize(path, out string normalized, out _))
            return keys;
        (string filePath, string? stream) = DfsStreamPath.Split(normalized);
        Resolution res = new NamespaceView(tx.Inodes).Resolve(filePath);
        if (res.Found && tx.BaseInodes.ContainsKey(res.Target!.Id))
        {
            var key = new StreamKey(res.Target.Id, stream ?? DfsStreamPath.DefaultStreamName);
            if (!tx.Reserved.Contains(key))
                keys.Add(key);
        }

        return keys;
    }

    /// <summary>Appends a mutation that must be re-executed at commit to the transaction's write list.</summary>
    private static void RecordWrite(TxWork tx, VolumeCommand command, VolumeApplyResult result)
    {
        switch (command)
        {
            case OpenHandleCommand when result.Value is OpenValue opened:
            {
                bool changes = opened.Handle.IsWriter || result.Reply is OpenHandleResultMessage { Created: true } or OpenHandleResultMessage { Truncated: true };
                if (changes)
                {
                    tx.Writes.Add(new TxWrite(CommandCodec.Encode(command), opened.Handle.Id));
                    tx.ReplayedHandles.Add(opened.Handle.Id);
                }

                break;
            }

            case CloseHandleCommand close:
                if (tx.ReplayedHandles.Remove(close.Handle))
                    tx.Writes.Add(new TxWrite(CommandCodec.Encode(command), 0));
                break;
            case CommitFileCommand or WriteAllCommand or AppendCommand or CopyCommand or MoveCommand or DeleteCommand or CreateDirectoryCommand
                or SetPolicyCommand or SetAclCommand or SetOwnerCommand:
                tx.Writes.Add(new TxWrite(CommandCodec.Encode(command), 0));
                break;
        }
    }

    // ------------------------------------------------------------------------------------------------- read set

    /// <summary>Records the name lookups and inode versions a transaction-bound request depends on.</summary>
    private static void Track(TxWork tx, IVolumeRequest request)
    {
        switch (request)
        {
            case MoveMessage m:
                TrackPath(tx, m.Source, false);
                TrackPath(tx, m.Destination, false);
                break;
            case CopyMessage c:
                TrackPath(tx, c.Source, false);
                TrackPath(tx, c.Destination, false);
                break;
            case DeleteMessage d:
                TrackPath(tx, d.Path, d.Recursive);
                break;
            case GetInfoMessage { Handle.IsEmpty: false } info:
                TrackHandle(tx, info.Handle.Value);
                break;
            case ReadRangeMessage { Handle.IsEmpty: false } read:
                TrackHandle(tx, read.Handle.Value);
                break;
            default:
                if (RequestFaultInfo.PathOf(request) is { Length: > 0 } path)
                    TrackPath(tx, path, false);
                break;
        }
    }

    private static void TrackHandle(TxWork tx, ulong handle)
    {
        if (tx.Handles.TryGetValue(handle, out HandleRecord? h))
            RecordInode(tx, h.Inode);
    }

    /// <summary>Walks <paramref name="rawPath"/> in the working view recording every lookup and the target's version.</summary>
    private static void TrackPath(TxWork tx, string rawPath, bool subtree)
    {
        if (!DfsPath.TryNormalize(rawPath, out string path, out _))
            return;
        string filePath = DfsStreamPath.GetFilePath(path);
        if (tx.Inodes.GetValueOrDefault(InodeRecord.RootId) is not { } current)
            return;
        foreach (string name in filePath.Split('/', StringSplitOptions.RemoveEmptyEntries))
        {
            if (!current.IsDirectory)
                return;
            ulong child = current.Children.GetValueOrDefault(name);
            RecordLookup(tx, current.Id, name);
            if (child == 0 || tx.Inodes.GetValueOrDefault(child) is not { } next)
                return;
            current = next;
        }

        RecordInode(tx, current.Id);
        if (subtree && current.IsDirectory)
        {
            var pending = new Stack<InodeRecord>();
            pending.Push(current);
            while (pending.Count > 0)
            {
                InodeRecord dir = pending.Pop();
                foreach (ulong childId in dir.Children.Values)
                {
                    if (tx.Inodes.GetValueOrDefault(childId) is not { } node)
                        continue;
                    RecordInode(tx, node.Id);
                    if (node.IsDirectory)
                        pending.Push(node);
                }
            }
        }
    }

    /// <summary>First observation of a name in a directory that existed at the read version: its entry then.</summary>
    private static void RecordLookup(TxWork tx, ulong directory, string name)
    {
        if (!tx.BaseInodes.TryGetValue(directory, out InodeRecord? baseDirectory) || !baseDirectory.IsDirectory)
            return;
        tx.Lookups.TryAdd((directory, name), baseDirectory.Children.GetValueOrDefault(name));
    }

    /// <summary>First observation of an inode that existed at the read version: its revision then.</summary>
    private static void RecordInode(TxWork tx, ulong inode)
    {
        if (tx.BaseInodes.TryGetValue(inode, out InodeRecord? baseInode))
            tx.Reads.TryAdd(inode, baseInode.Revision);
    }

    /// <summary>The leader-private state of one active transaction.</summary>
    private sealed class TxWork
    {
        public TxWork(DfsOperationId id, string principal, VolumeState root)
        {
            Id = id;
            Principal = principal;
            BaseInodes = root.Inodes;
            Inodes = root.Inodes;
            Detached = root.Detached;
            NextInodeId = root.NextInodeId;
            NextIndex = root.AppliedIndex + 1;
        }

        public DfsOperationId Id { get; }

        public string Principal { get; }

        /// <summary>The namespace at the read version (read-set baseline).</summary>
        public ImmutableSortedDictionary<ulong, InodeRecord> BaseInodes { get; }

        public ImmutableSortedDictionary<ulong, InodeRecord> Inodes { get; set; }

        public ImmutableSortedSet<ulong> Detached { get; set; }

        public ulong NextInodeId { get; set; }

        public ImmutableSortedDictionary<ulong, HandleRecord> Handles { get; set; } = ImmutableSortedDictionary<ulong, HandleRecord>.Empty;

        public ulong NextHandleId { get; set; } = VolumeStateMachine.TransactionHandleBase;

        public ImmutableSortedDictionary<ulong, PinRecord> Pins { get; set; } = ImmutableSortedDictionary<ulong, PinRecord>.Empty;

        public ulong NextPinId { get; set; } = VolumeStateMachine.TransactionPinBase;

        public ImmutableSortedDictionary<StreamKey, ulong> WriterGenerations { get; set; } =
            ImmutableSortedDictionary.Create<StreamKey, ulong>(StreamKeyComparer.Instance);

        /// <summary>Index stamped on the next change of the working view (never committed).</summary>
        public ulong NextIndex { get; set; }

        /// <summary>Lookups (directory, name) → child at the read version (0 = absent).</summary>
        public SortedDictionary<(ulong Directory, string Name), ulong> Lookups { get; } = new(LookupComparer.Instance);

        /// <summary>Inode → revision at the read version.</summary>
        public SortedDictionary<ulong, ulong> Reads { get; } = [];

        public List<TxWrite> Writes { get; } = [];

        /// <summary>Working-view handles whose opens are re-executed at commit.</summary>
        public HashSet<ulong> ReplayedHandles { get; } = [];

        /// <summary>Live streams this transaction holds the writer reservation of.</summary>
        public HashSet<StreamKey> Reserved { get; } = [];

        /// <summary>Leader staging sessions created for this transaction's writes (closed when it ends without commit).</summary>
        public List<ulong> LeaderStagings { get; } = [];

        /// <summary>Bytes published by the transaction's writes so far (MaxTransactionStagedBytes).</summary>
        public long StagedBytes { get; set; }

        /// <summary>Results of the transaction's operation ids (a retried transaction-bound request gets the same answer).</summary>
        public Dictionary<DfsOperationId, VolumeApplyResult> Results { get; } = [];

        /// <summary>The commit descriptor: read set and ordered writes.</summary>
        public TxDescriptor Describe() => new(
            [.. Lookups.Select(l => new TxLookup(l.Key.Directory, l.Key.Name, l.Value))],
            [.. Reads.Select(r => new TxRead(r.Key, r.Value))],
            [.. Writes]);
    }

    private sealed class LookupComparer : IComparer<(ulong Directory, string Name)>
    {
        public static LookupComparer Instance { get; } = new();

        public int Compare((ulong Directory, string Name) x, (ulong Directory, string Name) y)
        {
            int c = x.Directory.CompareTo(y.Directory);
            return c != 0 ? c : string.CompareOrdinal(x.Name, y.Name);
        }
    }
}
