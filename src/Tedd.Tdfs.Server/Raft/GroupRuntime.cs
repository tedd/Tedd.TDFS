using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Server.Runtime;
using Tedd.Tdfs.Storage;

namespace Tedd.Tdfs.Server.Raft;

/// <summary>How to open one Raft group on this node.</summary>
/// <param name="Group">Wire group name (<c>"control"</c> or <c>"vol:{id}"</c>).</param>
/// <param name="DisplayName">Name for inspection ("control" or the volume name).</param>
/// <param name="VolumeName">Volume name reported in fault contexts (null for the control group).</param>
/// <param name="Directory">Storage directory of the group (log and checkpoints).</param>
/// <param name="StateMachine">The replicated state machine (initial state when the group has no checkpoint).</param>
/// <param name="BootstrapEntries">
/// Entries written (committed, term 1) when the group's log is empty on an initial member: membership changes listing
/// the initial members, then initial commands. Null for members added later (they receive the log from the leader).
/// </param>
internal sealed record GroupOpenSpec(string Group, string DisplayName, string? VolumeName, string Directory,
    IReplicatedStateMachine StateMachine, IReadOnlyList<RaftEntry>? BootstrapEntries);

/// <summary>Catch-up counters of one group for the current process (RSY suite, <c>RaftGroupStats</c>).</summary>
internal sealed class GroupStats
{
    /// <summary>Applied index of the state loaded at start-up (checkpoint index before journal replay).</summary>
    public ulong StartIndex { get; set; }

    public long EntriesAppended { get; set; }

    public long EntriesApplied { get; set; }

    public long EntriesCoveredByInstall { get; set; }

    public long SnapshotsInstalled { get; set; }

    public long SnapshotInstallsAborted { get; set; }

    public long SnapshotsRejectedStale { get; set; }

    public long SnapshotVerificationFailures { get; set; }

    public long CheckpointsWritten { get; set; }
}

/// <summary>
/// One Raft group hosted on this node: the pure <see cref="RaftNode"/>, its durable log and checkpoints, the replicated
/// state machine, and the correlation of proposals and ReadIndex requests with applied entries. Everything runs on the
/// node loop; Ready processing is serialized per group (ARCHITECTURE §4, INTERNALS §1).
/// </summary>
internal sealed partial class GroupRuntime
{
    private const int SnapshotChunkBytes = 512 * 1024;

    private readonly NodeServices _s;
    private readonly RaftGroupHost _host;
    private readonly IReplicatedStateMachine _sm;
    private readonly RaftLogStoreOptions _storeOptions;
    private readonly CheckpointStore _checkpoints;
    private readonly IRandomSource _random;

    // Keyed by (index, term): a leader re-elected after losing leadership can append a new entry at the index of one of
    // its earlier, still undecided proposals; both wait for the entry committed there (at most one of them wins).
    private readonly SortedDictionary<(ulong Index, ulong Term), PendingProposal> _proposals = new();
    private readonly Dictionary<ulong, PendingRead> _reads = new();
    private readonly List<AppliedWaiter> _appliedWaiters = [];
    private readonly List<SnapshotChunkCapture> _chunkCaptures = [];
    private RaftStorageAdapter _view;
    private RaftNode _node = null!;
    private RaftConfState _appliedConf;
    private RaftSnapshot? _latestSnapshot;
    private ulong _appliedIndex;
    private ulong _appliedTerm;
    private ulong _nextReadContext;
    private bool _processing;
    private bool _kickPosted;
    private bool _closed;
    private bool _resetting;
    private Exception? _failure;
    private RaftRole _lastRole;
    private NodeId? _lastLeader;
    private ulong _lastTerm;
    private TaskCompletionSource? _idle;

    private GroupRuntime(NodeServices services, RaftGroupHost host, GroupOpenSpec spec, RaftStorageAdapter view, RaftLogStoreOptions storeOptions,
        CheckpointStore checkpoints, RaftSnapshot? latestSnapshot)
    {
        _s = services;
        _host = host;
        _sm = spec.StateMachine;
        _view = view;
        _storeOptions = storeOptions;
        _checkpoints = checkpoints;
        _random = services.Random.Fork("raft/" + spec.Group);
        Name = spec.Group;
        DisplayName = spec.DisplayName;
        VolumeName = spec.VolumeName;
        Directory = spec.Directory;
        _latestSnapshot = latestSnapshot;
        RaftSnapshotMeta snapshot = view.Snapshot;
        _appliedConf = snapshot.ConfState;
        _appliedIndex = snapshot.Index;
        _appliedTerm = snapshot.Term;
        Stats.StartIndex = snapshot.Index;
    }

    /// <summary>Wire group name.</summary>
    public string Name { get; }

    /// <summary>"control" or the volume name.</summary>
    public string DisplayName { get; }

    /// <summary>Volume name for fault contexts (null for the control group).</summary>
    public string? VolumeName { get; }

    /// <summary>Storage directory.</summary>
    public string Directory { get; }

    /// <summary>The replicated state machine.</summary>
    public IReplicatedStateMachine StateMachine => _sm;

    /// <summary>Catch-up counters.</summary>
    public GroupStats Stats { get; } = new();

    /// <summary>The Raft core (host use only).</summary>
    public RaftNode Node => _node;

    public RaftRole Role => _node.Role;

    public ulong Term => _node.Term;

    public NodeId? Leader => _node.Leader;

    public bool IsLeader => _failure is null && !_closed && !_resetting && _node.Role == RaftRole.Leader;

    public ulong CommitIndex => _node.CommitIndex;

    /// <summary>Index of the last entry applied by this host (state machine, configuration or no-op).</summary>
    public ulong AppliedIndex => _appliedIndex;

    /// <summary>Term of the entry at <see cref="AppliedIndex"/>.</summary>
    public ulong AppliedTerm => _appliedTerm;

    public ulong LastIndex => _node.LastIndex;

    /// <summary>Index covered by the durable log's snapshot (checkpoint).</summary>
    public ulong SnapshotIndex => _view.Snapshot.Index;

    /// <summary>First index still held by the durable log.</summary>
    public ulong LogFirstIndex => _view.FirstIndex;

    /// <summary>Membership as of the applied index (committed configuration entries applied in order).</summary>
    public RaftConfState AppliedConf => _appliedConf;

    /// <summary>Membership including appended but uncommitted changes.</summary>
    public RaftConfState Conf => _node.ConfState;

    /// <summary>True when the group stopped after an unrecoverable integrity failure.</summary>
    public Exception? Failure => _failure;

    /// <summary>True once closed.</summary>
    public bool IsClosed => _closed;

    /// <summary>Opens (recovers or bootstraps) a group from <paramref name="spec"/>.</summary>
    public static async Task<GroupRuntime> OpenAsync(NodeServices services, RaftGroupHost host, GroupOpenSpec spec, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(host);
        ArgumentNullException.ThrowIfNull(spec);
        IStorageMedium medium = services.Env.Medium;
        IRecordCipher? journalCipher = null;
        IRecordCipher? checkpointCipher = null;
        if (services.Options.EncryptAtRest)
        {
            RecordCipher j = services.Keys.CreateJournalCipher(services.Random.Fork("cipher/journal/" + spec.Group));
            RecordCipher c = services.Keys.CreateCheckpointCipher(services.Random.Fork("cipher/checkpoint/" + spec.Group));
            services.Disposables.Add(j);
            services.Disposables.Add(c);
            journalCipher = j;
            checkpointCipher = c;
        }

        var storeOptions = new RaftLogStoreOptions
        {
            Cipher = journalCipher,
            Faults = services.Faults,
            NodeName = services.NodeName,
            GroupName = spec.VolumeName,
        };
        RaftLogStore store = await RaftLogStore.OpenAsync(medium, LogDirectoryOf(spec.Directory), storeOptions, ct);
        try
        {
            CheckpointStore checkpoints = await CheckpointStore.OpenAsync(medium, StoragePath.Combine(spec.Directory, "ckpt"), new CheckpointStoreOptions
            {
                Cipher = checkpointCipher,
                Faults = services.Faults,
                NodeName = services.NodeName,
                GroupName = spec.VolumeName,
            }, ct);
            var view = new RaftStorageAdapter(store);

            RaftSnapshot? latest = await LoadLatestCheckpointAsync(checkpoints, spec, ct);
            ulong storeSnapshot = store.Snapshot.Index;
            if (latest is not null && latest.Meta.Index > storeSnapshot)
            {
                // A crash between making a checkpoint current and compacting the log: finish the compaction.
                await view.CompactThroughAsync(latest.Meta, ct);
            }
            else if (latest is null && storeSnapshot > 0)
            {
                throw new DfsIntegrityException($"Group '{spec.Group}': the log is compacted through {storeSnapshot} but no checkpoint exists.");
            }
            else if (latest is not null && latest.Meta.Index < storeSnapshot)
            {
                throw new DfsIntegrityException(
                    $"Group '{spec.Group}': the log is compacted through {storeSnapshot} but the newest checkpoint is at {latest.Meta.Index}.");
            }

            if (latest is not null)
            {
                SnapshotEnvelope.TryDecode(latest.Data, latest.Meta, out _, out ReadOnlyMemory<byte> state, out _);
                spec.StateMachine.RestoreCheckpoint(new System.Buffers.ReadOnlySequence<byte>(state), latest.Meta.Index, latest.Meta.Term);
            }

            await RepairHardStateAsync(store, ct);
            if (store.LastIndex == 0 && store.Snapshot.Index == 0 && spec.BootstrapEntries is { Count: > 0 } bootstrap)
                await WriteBootstrapAsync(view, bootstrap, ct);

            var group = new GroupRuntime(services, host, spec, view, storeOptions, checkpoints, latest);

            // Entries committed before the restart and replayed from the local journal are checkpointed once they are applied
            // again (the restart's metadata checkpoint install, test plan F16): the next restart starts from that state instead
            // of replaying the same suffix, and an interrupted install is recovered like any other (DSK-012).
            ulong replayTarget = Math.Min(store.HardState.Commit, store.LastIndex);
            if (replayTarget > view.Snapshot.Index)
            {
                group.ReplayTarget = replayTarget;
                group._recoveryCheckpointAt = replayTarget;
            }

            group.CreateNode(view.Snapshot.Index);
            return group;
        }
        catch
        {
            await store.DisposeAsync();
            throw;
        }
    }

    /// <summary>Advances the logical clock.</summary>
    public void Tick()
    {
        if (!IsActive)
            return;
        try
        {
            _node.Tick();
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            // One group's failure must never stop the node-wide tick loop.
            Fail(ex);
            return;
        }

        Kick();
    }

    /// <summary>Delivers an inbound message.</summary>
    public void Step(RaftMessage message)
    {
        if (!IsActive)
            return;
        try
        {
            _node.Step(message);
        }
        catch (ArgumentException ex)
        {
            _s.Debug("raft", NodeServices.Inv($"{Name}: dropped a malformed {message.GetType().Name} from {message.From}: {ex.Message}"));
            return;
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            Fail(ex);
            return;
        }

        Kick();
    }

    /// <summary>Reports that a peer could not be reached (pauses optimistic replication to it).</summary>
    public void ReportUnreachable(NodeId peer)
    {
        if (IsActive)
            _node.ReportUnreachable(peer);
    }

    /// <summary>Starts a leadership transfer to <paramref name="target"/> (leader only).</summary>
    public void TransferLeadership(NodeId target)
    {
        if (!IsActive)
            return;
        _node.TransferLeadership(target);
        Kick();
    }

    /// <summary>Leader-side replication state of the other members (empty when not leader).</summary>
    public IReadOnlyList<RaftPeerProgress> GetProgress() => IsActive ? _node.GetProgress() : [];

    /// <summary>
    /// Proposes a command and completes after it was committed and applied, with its apply outcome (ARCHITECTURE §4).
    /// Not leader → <see cref="DfsUnavailableException"/>(<see cref="DfsErrorCode.NotLeader"/>) with a leader hint and nothing
    /// proposed. After proposing, a lost leadership keeps waiting for the entry's fate; another term at the index means the
    /// proposal lost (retryable <see cref="DfsErrorCode.NotLeader"/>); a deadline or shutdown before the fate is known
    /// reports <see cref="DfsCommitOutcomeUnknownException"/>.
    /// </summary>
    public Task<ApplyOutcome> ProposeAsync(ReadOnlyMemory<byte> command, ProposalContext context, CancellationToken ct = default)
        => ProposeAsync(_ => new ValueTask<ReadOnlyMemory<byte>>(command), context, ct);

    /// <summary>
    /// <see cref="ProposeAsync(ReadOnlyMemory{byte}, ProposalContext, CancellationToken)"/> whose command is produced by
    /// <paramref name="finalize"/> immediately before it is appended — after the <see cref="FaultSites.RaftBeforePropose"/>
    /// stage — so the caller can re-validate the authoritative decision at the last moment (for example re-resolve
    /// authorization that may have been revoked while the request waited, SEC-006, ACL-026). An exception from
    /// <paramref name="finalize"/> propagates and nothing is proposed.
    /// </summary>
    public async Task<ApplyOutcome> ProposeAsync(Func<CancellationToken, ValueTask<ReadOnlyMemory<byte>>> finalize, ProposalContext context,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(finalize);
        ThrowIfUnavailable();
        ThrowIfResetting();
        if (_node.Role != RaftRole.Leader)
            throw NotLeader();
        await _s.HitAsync(FaultStage.F10, FaultSites.RaftBeforePropose, VolumeName, context.Operation, context.OperationId, context.Path, ct: ct);
        ThrowIfUnavailable();
        ThrowIfResetting();
        if (_node.Role != RaftRole.Leader)
            throw NotLeader();
        ReadOnlyMemory<byte> command = await finalize(ct);
        ThrowIfUnavailable();
        ThrowIfResetting();
        if (!_node.TryPropose(command, out ulong index))
            throw NotLeader();
        var pending = new PendingProposal(index, _node.Term, context);
        _proposals[pending.Key] = pending;
        Kick();

        ApplyOutcome outcome;
        try
        {
            outcome = await pending.Completion.Task.WaitAsync(ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            RemoveProposal(pending);
            throw OutcomeUnknown(context, $"the deadline elapsed before entry {index} of group '{DisplayName}' was decided");
        }

        await _s.HitAsync(FaultStage.F12, FaultSites.ApplyAfterBeforeReply, VolumeName, context.Operation, context.OperationId, context.Path, ct: ct);
        return outcome;
    }

    /// <summary>Proposes a single-server membership change and completes once it was applied.</summary>
    /// <returns>False when the change cannot be proposed now (not leader, another change pending, not applicable).</returns>
    public async Task<bool> ProposeConfChangeAsync(RaftConfChange change, CancellationToken ct = default)
    {
        ThrowIfUnavailable();
        if (_resetting || _node.Role != RaftRole.Leader)
            return false;
        await _s.HitAsync(FaultStage.F21, FaultSites.Reconfiguration, VolumeName, DfsOperationKind.MembershipChange, detail: change.Kind + " " + change.Node, ct: ct);
        if (!IsActive || !_node.TryProposeConfChange(change, out ulong index))
            return false;
        var pending = new PendingProposal(index, _node.Term, new ProposalContext(DfsOperationKind.MembershipChange));
        _proposals[pending.Key] = pending;
        Kick();
        try
        {
            await pending.Completion.Task.WaitAsync(ct);
            return true;
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            RemoveProposal(pending);
            return false;
        }
        catch (DfsException)
        {
            return false;
        }
    }

    /// <summary>
    /// Linearizable read barrier (CON-008/009): confirms leadership with a heartbeat quorum, then waits until the applied
    /// index reaches the read index; returns the applied index.
    /// </summary>
    public async Task<ulong> ReadIndexAsync(CancellationToken ct = default)
    {
        ThrowIfUnavailable();
        ThrowIfResetting();
        if (_node.Role != RaftRole.Leader)
            throw NotLeader();
        ulong context = ++_nextReadContext;
        var read = new PendingRead(context);
        _reads[context] = read;
        _node.ReadIndex(context);
        Kick();
        try
        {
            return await read.Completion.Task.WaitAsync(ct);
        }
        finally
        {
            _reads.Remove(context);
        }
    }

    /// <summary>Completes once the applied index reaches <paramref name="index"/>.</summary>
    public Task WaitForAppliedAsync(ulong index, CancellationToken ct = default)
    {
        ThrowIfUnavailable();
        if (_appliedIndex >= index)
            return Task.CompletedTask;
        var waiter = new AppliedWaiter(index);
        _appliedWaiters.Add(waiter);
        return ct.CanBeCanceled ? waiter.Completion.Task.WaitAsync(ct) : waiter.Completion.Task;
    }

    /// <summary>Last entry committed before the restart that the local journal holds (0 when nothing was to be replayed).</summary>
    public ulong ReplayTarget { get; private set; }

    /// <summary>
    /// Applies the entries committed before the restart from the local journal (no peer is needed) and completes once they
    /// are applied (local journal replay, test plan F16).
    /// </summary>
    public Task ReplayLocalAsync(CancellationToken ct = default)
    {
        if (_appliedIndex >= ReplayTarget)
            return Task.CompletedTask;
        Task applied = WaitForAppliedAsync(ReplayTarget, ct);
        Kick();
        return applied;
    }

    /// <summary>Stops the group: fails waiters, waits for the Ready loop, closes the log (and deletes the data when asked).</summary>
    public async Task CloseAsync(bool deleteData)
    {
        if (_closed)
            return;
        _closed = true;
        FailAll(new DfsUnavailableException(DfsErrorCode.NodeUnavailable, $"Group '{DisplayName}' was closed on {_s.NodeName}."));
        await WaitIdleAsync();
        try
        {
            await _view.Store.DisposeAsync();
        }
        catch (Exception ex) when (ex is IOException or ObjectDisposedException or DfsException)
        {
            _s.Warn("raft", NodeServices.Inv($"{Name}: closing the log failed: {ex.Message}"));
        }

        if (deleteData)
            await StorageCleanup.DeleteTreeAsync(_s.Env.Medium, Directory);
    }

    /// <summary>Schedules Ready processing (coalesced; runs as a separate loop work item).</summary>
    public void Kick()
    {
        if (!IsActive || _processing || _kickPosted)
            return;
        _kickPosted = true;
        _s.Loop.Post(() =>
        {
            _kickPosted = false;
            _ = RunReadyLoopAsync();
        });
    }

    private bool IsActive => !_closed && _failure is null && !_resetting;

    private static string LogDirectoryOf(string groupDirectory) => StoragePath.Combine(groupDirectory, "log");

    private void CreateNode(ulong applied)
    {
        NodeTimings t = _s.Timings;
        var config = new RaftConfig(_s.NodeId, t.ElectionTicksMin, t.ElectionTicksMax, t.HeartbeatTicks, PreVote: true, CheckQuorum: true);
        var options = new RaftNodeOptions
        {
            SnapshotSource = new DelegateRaftSnapshotSource(() => _latestSnapshot),
            Applied = applied,
            SnapshotChunkBytes = SnapshotChunkBytes,
            Mutations = _s.Mutations,
        };
        _node = new RaftNode(config, _view.HardState, _view.Snapshot.ConfState, _view, _random, options);
        _lastRole = _node.Role;
        _lastLeader = _node.Leader;
        _lastTerm = _node.Term;
    }

    /// <summary>
    /// A crash after entries (or an installed snapshot) became durable but before the hard state was saved leaves the
    /// persisted term behind the term of the last durable entry, which the Raft core refuses at start-up. No vote of that
    /// newer term was ever sent (a vote is sent only after the hard state holding it is durable), so the term is raised to
    /// the last log term without a vote.
    /// </summary>
    private static async Task RepairHardStateAsync(RaftLogStore store, CancellationToken ct)
    {
        RaftLogHardState hs = store.HardState;
        ulong lastTerm = store.LastTerm;
        if (hs.Term >= lastTerm)
            return;
        await store.SaveHardStateAsync(new RaftLogHardState(lastTerm, null, hs.Commit), ct);
    }

    private static async Task WriteBootstrapAsync(RaftStorageAdapter view, IReadOnlyList<RaftEntry> entries, CancellationToken ct)
    {
        await view.AppendAsync(entries, ct);
        await view.FlushAsync(ct);
        RaftEntry last = entries[^1];
        await view.SaveHardStateAsync(new RaftHardState(last.Term, null, last.Index), ct);
    }

    private void ThrowIfUnavailable()
    {
        if (_failure is not null)
            throw new DfsUnavailableException(DfsErrorCode.Unavailable, $"Group '{DisplayName}' is stopped on {_s.NodeName}: {_failure.Message}");
        if (_closed)
            throw new DfsUnavailableException(DfsErrorCode.NodeUnavailable, $"Group '{DisplayName}' is not hosted on {_s.NodeName}.");
    }

    private void ThrowIfResetting()
    {
        if (_resetting)
            throw new DfsUnavailableException(DfsErrorCode.Unavailable, $"Group '{DisplayName}' on {_s.NodeName} is recovering from a storage failure.");
    }

    /// <summary>Forgets <paramref name="pending"/> (never another proposal registered under the same key).</summary>
    private void RemoveProposal(PendingProposal pending)
    {
        if (_proposals.TryGetValue(pending.Key, out PendingProposal? current) && ReferenceEquals(current, pending))
            _proposals.Remove(pending.Key);
    }

    /// <summary>A retryable NotLeader error carrying the leader's endpoint when known.</summary>
    public DfsUnavailableException NotLeader()
    {
        NodeId? leader = _node.Leader;
        string? hint = leader is { } id && id != _s.NodeId ? _host.ResolveEndpoint(id) : null;
        string message = leader is null
            ? $"{_s.NodeName} is not the leader of group '{DisplayName}' and no leader is known."
            : $"{_s.NodeName} is not the leader of group '{DisplayName}'.";
        return new DfsUnavailableException(DfsErrorCode.NotLeader, message) { LeaderHint = hint };
    }

    private static DfsCommitOutcomeUnknownException OutcomeUnknown(ProposalContext context, string reason)
        => new(context.OperationId ?? default, "The outcome of the proposal is unknown: " + reason + ".");

    private void FailAll(Exception error)
    {
        foreach (PendingProposal p in _proposals.Values)
        {
            Exception unknown = OutcomeUnknown(p.Context, error.Message);
            Defer(() => p.Completion.TrySetException(unknown));
        }

        _proposals.Clear();
        foreach (PendingRead r in _reads.Values)
            Defer(() => r.Completion.TrySetException(error));
        _reads.Clear();
        foreach (AppliedWaiter w in _appliedWaiters)
            Defer(() => w.Completion.TrySetException(error));
        _appliedWaiters.Clear();
    }

    private sealed class PendingProposal(ulong index, ulong term, ProposalContext context)
    {
        public ulong Index { get; } = index;

        public ulong Term { get; } = term;

        public (ulong Index, ulong Term) Key => (Index, Term);

        public ProposalContext Context { get; } = context;

        public bool LocalAppendReported { get; set; }

        public TaskCompletionSource<ApplyOutcome> Completion { get; } = new();
    }

    private sealed class PendingRead(ulong context)
    {
        public ulong Context { get; } = context;

        public ulong? ReadIndex { get; set; }

        public TaskCompletionSource<ulong> Completion { get; } = new();
    }

    private sealed class AppliedWaiter(ulong index)
    {
        public ulong Index { get; } = index;

        public TaskCompletionSource Completion { get; } = new();
    }

    private sealed class SnapshotChunkCapture(NodeId leader, ulong snapshotIndex)
    {
        public NodeId Leader { get; } = leader;

        public ulong SnapshotIndex { get; } = snapshotIndex;

        public InstallSnapshotResponse? Response { get; set; }
    }
}
