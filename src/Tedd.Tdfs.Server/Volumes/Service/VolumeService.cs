using System.Buffers.Binary;
using System.Collections.Immutable;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Control;
using Tedd.Tdfs.Server.Raft;
using Tedd.Tdfs.Server.Runtime;
using Tedd.Tdfs.Server.Volumes.Apply;
using Tedd.Tdfs.Server.Volumes.Commands;
using Tedd.Tdfs.Server.Volumes.State;

namespace Tedd.Tdfs.Server.Volumes.Service;

/// <summary>
/// Leader-side volume service (INTERNALS.md §1, ARCHITECTURE §5, §7): validates and authorizes requests, answers retries
/// from retained outcomes, coalesces in-flight duplicates, prepares deterministic commands (receipt validation, inline
/// materialization, the leader's clock and the caller's identity set), proposes them and replies; serves linearizable reads
/// after a ReadIndex barrier; tracks leases, sessions and reader pins with the leader's monotonic clock and proposes their
/// expiry; and serves inventory/GC classification. All non-replicated state here is leader-local and rebuilt after every
/// leadership change. Everything runs on the node loop.
/// </summary>
internal sealed partial class VolumeService
{
    private readonly NodeServices _s;
    private readonly VolumeHost _host;
    private readonly VolumeStateMachine _sm;
    private readonly Dictionary<DfsOperationId, InFlight> _inFlight = [];
    private readonly KeyedAsyncLock<StreamKey> _publishLocks = new();
    private ulong _readyTerm;
    private bool _disposed;

    public VolumeService(NodeServices services, VolumeHost host, VolumeStateMachine stateMachine)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(host);
        ArgumentNullException.ThrowIfNull(stateMachine);
        _s = services;
        _host = host;
        _sm = stateMachine;
        Barrier = new ControlBarrier(services);
        Io = new ObjectIo(services);
    }

    /// <summary>
    /// The state the current request works on: the applied state of this replica, or for a transaction-bound request the
    /// transaction's private working view.
    /// </summary>
    public VolumeState State => CurrentTransaction is { } tx ? WorkingView(tx) : _sm.State;

    /// <summary>The volume's control record.</summary>
    public VolumeRecord Record => _host.Record;

    /// <summary>The volume's Raft group.</summary>
    public GroupRuntime Group => _host.Group;

    /// <summary>The volume name.</summary>
    public string Name => _host.Name;

    private ControlBarrier Barrier { get; }

    private ObjectIo Io { get; }

    /// <summary>True while this node leads the group.</summary>
    public bool IsLeader => _host.IsLeader && !_disposed;

    /// <summary>Called when the role, leader or term changed (VolumeHost hook).</summary>
    public void OnLeadershipChanged()
    {
        if (_host.IsLeader && Group.Term != _leaseTerm)
            ResetLeaderState();
        else if (!_host.IsLeader)
            ClearLeaderState();
        _s.BackupExports?.OnVolumeChanged(this);
    }

    /// <summary>Called when the control record changed (VolumeHost hook).</summary>
    public void OnRecordChanged() => _s.BackupExports?.OnVolumeChanged(this);

    /// <summary>Stops the service (group closing).</summary>
    public void Dispose()
    {
        _disposed = true;
        ClearLeaderState();
    }

    // ------------------------------------------------------------------------------------------------- request plumbing

    /// <summary>The leader is serving: it applied every entry committed before its term (ReadIndex once per term).</summary>
    private async Task EnsureLeaderReadyAsync(CancellationToken ct)
    {
        if (!IsLeader)
            throw Group.NotLeader();
        ulong term = Group.Term;
        if (_readyTerm == term)
            return;
        await Group.ReadIndexAsync(ct);
        if (Group.Term == term)
            MarkReady(term);
    }

    /// <summary>Records that this leader applied everything committed before its term (transactions begun until now are not its own).</summary>
    private void MarkReady(ulong term)
    {
        if (_readyTerm == term)
            return;
        _readyTerm = term;
        _transactionsSince = _sm.State.AppliedIndex;
    }

    /// <summary>A linearizable read barrier (CON-008); returns the applied index the read observes.</summary>
    private async Task<ulong> ReadBarrierAsync(CancellationToken ct)
    {
        if (!IsLeader)
            throw Group.NotLeader();
        if (_s.Mutations.AllowStaleLiveReads)
            return Group.AppliedIndex;
        ulong index = await Group.ReadIndexAsync(ct);
        MarkReady(Group.Term);
        return index;
    }

    /// <summary>The caller's volume grants (control state).</summary>
    private DfsPermissions GrantsOf(string principal) => _s.Control.State.GetPermissions(principal, Record.Id);

    /// <summary>Requires a volume grant (the coarse gate, ACL-06).</summary>
    private void RequireGrant(string principal, DfsPermissions permission, string what)
    {
        if ((GrantsOf(principal) & permission) != permission)
        {
            _s.Control.ReportSecurityEvent("volume:" + what, "denied", "volume=" + Name, principal);
            throw new UnauthorizedAccessException($"Principal '{principal}' lacks the {permission} grant on volume '{Name}' ({what}).");
        }
    }

    /// <summary>The caller identity set (ACL-01/08) resolved from the local control state.</summary>
    private (ImmutableArray<string> Identities, bool IsAdmin) IdentitiesOf(string principal)
    {
        ControlState control = _s.Control.State;
        bool admin = control.IsClusterAdmin(principal);
        var identities = new List<string> { DfsIdentity.Everyone.ToString(), DfsIdentity.AuthenticatedUsers.ToString() };
        if (DfsIdentity.TryParse("user:" + principal, out DfsIdentity user))
            identities.Add(user.ToString());
        foreach (string group in control.GroupsOf(principal))
        {
            if (DfsIdentity.IsValidName(group))
                identities.Add(DfsIdentity.Group(group).ToString());
        }

        if (admin)
            identities.Add(DfsIdentity.Administrators.ToString());
        identities.Sort(StringComparer.Ordinal);
        return ([.. identities.Distinct(StringComparer.Ordinal)], admin);
    }

    /// <summary>The caller's identity set for leader-side read authorization.</summary>
    private (IReadOnlySet<DfsIdentity> Set, bool IsAdmin) IdentitySetOf(string principal)
    {
        (ImmutableArray<string> ids, bool admin) = IdentitiesOf(principal);
        var set = new HashSet<DfsIdentity>();
        foreach (string text in ids)
        {
            if (DfsIdentity.TryParse(text, out DfsIdentity id))
                set.Add(id);
        }

        return (set, admin);
    }

    /// <summary>Builds the operation header of a request (payload identity, identities, grants, mutation flags).</summary>
    private OperationHeader Header(RpcContext context, IVolumeRequest request, DfsOperationKind kind, DfsOperationId? id = null)
    {
        string principal = context.Principal ?? string.Empty;
        (ImmutableArray<string> identities, bool admin) = IdentitiesOf(principal);
        DfsOperationId op = id ?? request.Header.Operation;
        UInt128 hash = op.IsEmpty ? UInt128.Zero : BinaryPrimitives.ReadUInt128LittleEndian(OperationPayload.ComputeHash(request));
        return new OperationHeader(op, principal, kind, hash, request.Header.Session, request.Header.AckWatermark, identities, admin, GrantsOf(principal),
            MutationFlags());
    }

    /// <summary>The negative-control mutation switches carried by commands (deterministic on every replica).</summary>
    private CommandFlags MutationFlags()
    {
        CommandFlags flags = CommandFlags.None;
        if (_s.Mutations.DisableOperationDedup)
            flags |= CommandFlags.DisableDedup;
        if (_s.Mutations.ForgetExpiredIdWatermark)
            flags |= CommandFlags.ForgetWatermark;
        if (_s.Mutations.SkipFencingCheck)
            flags |= CommandFlags.SkipFencing;
        return flags;
    }

    /// <summary>The header with identities and grants re-resolved from the (now current) control state.</summary>
    private OperationHeader Refresh(OperationHeader op)
    {
        (ImmutableArray<string> identities, bool admin) = IdentitiesOf(op.Principal);
        return op with { Identities = identities, IsAdmin = admin, Grants = GrantsOf(op.Principal) };
    }

    /// <summary>A command stamped with the leader clock and the operation header.</summary>
    private T Stamp<T>(T command, OperationHeader? op)
        where T : VolumeCommand
        => command with { LeaderTime = _s.Time.GetUtcNow(), Op = op };

    /// <summary>
    /// Runs a command against the leader's applied state without changing it (leader-side validation: the same pure apply
    /// logic, so a request apply would refuse is answered without a log entry).
    /// </summary>
    private VolumeApplyResult DryRun(VolumeCommand command)
    {
        VolumeState state = State;
        ulong index = Math.Max(state.AppliedIndex, Group.LastIndex) + 1;
        var ctx = new ApplyContext(index, Group.Term, command) { Transaction = CurrentTransaction?.Id ?? default };
        (_, VolumeApplyResult result) = VolumeStateMachine.ApplyCommand(state with { AppliedIndex = index, LeaderTime = command.LeaderTime },
            ctx with { Command = command with { Op = command.Op is { } op ? op with { Id = default } : null } });
        return result;
    }

    /// <summary>
    /// Proposes a command and returns its apply result. After the F10 stage the control barrier runs, identities and grants
    /// are re-resolved and the command is validated again, so authorization revoked while the request waited is honoured at
    /// the authoritative decision (SEC-006, ACL-026).
    /// </summary>
    private async Task<VolumeApplyResult> ProposeAsync(VolumeCommand command, CancellationToken ct, bool bypassTransaction = false)
    {
        if (!bypassTransaction && CurrentTransaction is { } tx)
            return await ApplyInTransactionAsync(tx, command, ct);
        var context = new ProposalContext(command.OperationKind, command.Op is { HasOperation: true } op ? op.Id : null, command.Path);
        VolumeCommand final = command;
        ApplyOutcome outcome = await Group.ProposeAsync(async token =>
        {
            if (command.Op is { } header)
            {
                await Barrier.WaitAsync(token);
                final = command with { Op = Refresh(header), LeaderTime = _s.Time.GetUtcNow() };

                // A command without an operation id that the refreshed authorization refuses is not proposed; one with an id
                // is, so apply (which re-checks with the same identities and grants) records the refusal as the id's outcome.
                VolumeApplyResult check = DryRun(final);
                if (!check.IsSuccess && IsAuthorizationFailure(check.Code) && !header.HasOperation)
                    throw new UnauthorizedAccessException(check.Message);
            }

            return CommandCodec.Encode(final);
        }, context, ct);
        if (outcome.Value is VolumeApplyResult result)
            return result;
        return new VolumeApplyResult(outcome.Code, outcome.Message, new ErrorReplyMessage(outcome.Code, outcome.Message), null);
    }

    private static bool IsAuthorizationFailure(DfsErrorCode code) => code == DfsErrorCode.AccessDenied;

    /// <summary>
    /// The mutation pipeline (ARCHITECTURE §5.2, §7): leader readiness, the operation gate against applied state (stored
    /// outcome, expired, mismatch), in-flight duplicate coalescing (OUT-005), preparation, dry-run validation and proposal.
    /// The reply is the apply result's reply (an error reply for failures).
    /// </summary>
    private Task<IWireMessage> MutateAsync(RpcContext context, IVolumeRequest request, DfsOperationKind kind,
        Func<OperationHeader, CancellationToken, Task<VolumeCommand>> prepare, Func<VolumeApplyResult, IWireMessage>? finish = null, bool requireOperation = true,
        bool cleanup = false, bool bindTransaction = true)
        => MutateAsync(context, request, kind, async (op, ct) => new Prepared(await prepare(op, ct)), finish, requireOperation, cleanup, bindTransaction);

    /// <summary>A prepared command and an optional hold (for example a per-stream publication lock) released after its proposal.</summary>
    private readonly record struct Prepared(VolumeCommand Command, IDisposable? Hold = null);

    /// <inheritdoc cref="MutateAsync(RpcContext, IVolumeRequest, DfsOperationKind, Func{OperationHeader, CancellationToken, Task{VolumeCommand}}, Func{VolumeApplyResult, IWireMessage}?, bool, bool, bool)"/>
    private async Task<IWireMessage> MutateAsync(RpcContext context, IVolumeRequest request, DfsOperationKind kind,
        Func<OperationHeader, CancellationToken, Task<Prepared>> prepare, Func<VolumeApplyResult, IWireMessage>? finish = null, bool requireOperation = true,
        bool cleanup = false, bool bindTransaction = true)
    {
        CancellationToken ct = context.Cancellation;
        string? cleanupPath = cleanup ? RequestFaultInfo.PathOf(request) : null;
        TxWork? tx = bindTransaction ? BindTransaction(request, context.Principal ?? string.Empty) : null;
        if (!bindTransaction && CurrentTransaction is not null)
            Ambient.Value = null;
        await EnsureLeaderReadyAsync(ct);
        OperationHeader op = Header(context, request, kind);
        if (requireOperation && !op.HasOperation)
            throw new ArgumentException($"{kind} requests must carry an operation id (OUT-001).");
        if (!op.HasOperation)
            return Finish(await ExecuteAsync(op, prepare, ct, cleanup, cleanupPath), finish);

        if (tx is not null)
        {
            // A transaction-bound operation id is answered from the transaction's own results (it never reaches the log).
            if (tx.Results.TryGetValue(op.Id, out VolumeApplyResult? prior))
                return Finish(prior, finish);
            VolumeApplyResult done = await ExecuteAsync(op, prepare, ct, cleanup, cleanupPath);
            if (!done.IsSuccess && done.Reply is ErrorReplyMessage { OperationId: null } bare)
                done = done with { Reply = bare with { OperationId = op.Id } };
            if (done.IsSuccess || !DfsErrors.IsRetryableByDefault(done.Code))
                tx.Results[op.Id] = done;
            return Finish(done, finish);
        }

        GateDecision gate = OperationGate.Check(State, op);
        switch (gate.Kind)
        {
            case GateKind.Stored:
                return Finish(VolumeStateMachine.StoredResult(gate.Outcome!), finish);
            case GateKind.Expired:
                return new ErrorReplyMessage(DfsErrorCode.OperationExpired, gate.Message, op.Id);
            case GateKind.Mismatch:
                return new ErrorReplyMessage(DfsErrorCode.OperationIdMismatch, gate.Message, op.Id);
            case GateKind.Invalid:
                return new ErrorReplyMessage(DfsErrorCode.InvalidArgument, gate.Message, op.Id);
        }

        if (_inFlight.TryGetValue(op.Id, out InFlight? running))
        {
            if (running.PayloadHash != op.PayloadHash || running.Operation != op.Operation
                || !string.Equals(running.Principal, op.Principal, StringComparison.Ordinal))
            {
                return new ErrorReplyMessage(DfsErrorCode.OperationIdMismatch, $"Operation {op.Id} is in flight for a different request.", op.Id);
            }

            return Finish(await running.Completion.Task.WaitAsync(ct), finish);
        }

        var entry = new InFlight(op.PayloadHash, op.Operation, op.Principal);
        _inFlight[op.Id] = entry;
        try
        {
            VolumeApplyResult result = await ExecuteAsync(op, prepare, ct, cleanup, cleanupPath);
            entry.Completion.TrySetResult(result);
            return Finish(result, finish);
        }
        catch (Exception ex)
        {
            entry.Completion.TrySetException(ex);
            _ = entry.Completion.Task.Exception;
            throw;
        }
        finally
        {
            if (_inFlight.TryGetValue(op.Id, out InFlight? current) && ReferenceEquals(current, entry))
                _inFlight.Remove(op.Id);
        }
    }

    private async Task<VolumeApplyResult> ExecuteAsync(OperationHeader op, Func<OperationHeader, CancellationToken, Task<Prepared>> prepare,
        CancellationToken ct, bool cleanup = false, string? cleanupPath = null)
    {
        Prepared prepared = await prepare(op, ct);
        try
        {
            // A definite refusal (not retryable: AlreadyExists, FileNotFound, AccessDenied, ...) of a request with an operation
            // id is decided through the log: the request is proposed anyway and apply records the refusal as the id's outcome,
            // so the answer is authoritative — a delayed copy or a re-proposal of the same id can never execute later
            // (OUT-013), retries get the same answer and QueryOperation reports it instead of NotFound. A retryable refusal
            // (SharingViolation, WriterConflict: "retrying the same operation may succeed", PROTOCOL.md §11.1) and any
            // refusal of a request without an id are answered from the pre-check without a log entry.
            // A transaction's commit always goes through the log: a refused commit ends the transaction (its reservations
            // are released) in the replicated state, whatever the refusal.
            VolumeApplyResult check = DryRun(prepared.Command);
            if (!check.IsSuccess && prepared.Command is not TxCommitCommand && (!op.HasOperation || DfsErrors.IsRetryableByDefault(check.Code)))
            {
                ErrorReplyMessage error = check.Reply as ErrorReplyMessage ?? new ErrorReplyMessage(check.Code, check.Message);
                return check with { Reply = op.HasOperation ? error with { OperationId = op.Id } : error };
            }

            VolumeApplyResult result = await ProposeAsync(prepared.Command, ct);
            if (cleanup && result.IsSuccess && !result.Replayed)
            {
                // F15: the decision released a reservation, pin or references (post-decision cleanup). The decision is
                // committed: a cancellation while held here must not turn it into a failure (coalesced duplicates share it).
                try
                {
                    await _s.HitAsync(FaultStage.F15, FaultSites.CleanupRelease, Name, op.Operation, op.HasOperation ? op.Id : null, cleanupPath, ct: ct);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                }
            }

            return result;
        }
        finally
        {
            prepared.Hold?.Dispose();
        }
    }

    private static IWireMessage Finish(VolumeApplyResult result, Func<VolumeApplyResult, IWireMessage>? finish)
    {
        if (!result.IsSuccess)
            return result.Reply ?? new ErrorReplyMessage(result.Code, result.Message);
        if (finish is not null)
            return finish(result);
        return result.Reply ?? throw new InvalidOperationException("The applied command produced no reply.");
    }

    /// <summary>Proposes a leader housekeeping command (expiry, pruning, staging) with a node-clock deadline.</summary>
    private async Task<VolumeApplyResult?> ProposeHousekeepingAsync(VolumeCommand command)
    {
        if (!IsLeader)
            return null;
        using CancellationTokenSource deadline = _s.CreateDeadline();
        try
        {
            ApplyOutcome outcome = await Group.ProposeAsync(CommandCodec.Encode(command with { LeaderTime = _s.Time.GetUtcNow() }),
                new ProposalContext(command.OperationKind), deadline.Token);
            return outcome.Value as VolumeApplyResult;
        }
        catch (Exception ex) when (ex is DfsException or OperationCanceledException or IOException)
        {
            _s.Debug("volume", NodeServices.Inv($"Volume {Name}: housekeeping {command.Kind} failed: {ex.Message}"));
            return null;
        }
    }

    /// <summary>Serializes leader-side planning and proposal of publications per stream (materialization stays consistent).</summary>
    private Task<IDisposable> LockStreamAsync(StreamKey key, CancellationToken ct) => _publishLocks.AcquireAsync(key, ct);

    /// <summary>Normalizes a request path (InvalidPath on failure, PATH-012).</summary>
    private static string NormalizePath(string path)
    {
        if (!DfsPath.TryNormalize(path, out string normalized, out string? error))
            throw new ArgumentException(error ?? $"Invalid path '{path}'.", "path");
        return normalized;
    }

    private CommitToken Token(ulong index) => State.TokenAt(index);

    private sealed class InFlight(UInt128 payloadHash, DfsOperationKind operation, string principal)
    {
        public UInt128 PayloadHash { get; } = payloadHash;

        public DfsOperationKind Operation { get; } = operation;

        public string Principal { get; } = principal;

        public TaskCompletionSource<VolumeApplyResult> Completion { get; } = new();
    }
}
