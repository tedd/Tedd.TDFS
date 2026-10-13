using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Server.Runtime;
using Tedd.Tdfs.Storage;

namespace Tedd.Tdfs.Server.Raft;

internal sealed partial class GroupRuntime
{
    private static readonly TimeSpan PersistRetryDelay = TimeSpan.FromMilliseconds(100);

    private readonly List<Action> _deferred = [];
    private bool _postInstallCatchUp;
    private bool _deferredPosted;

    /// <summary>
    /// Processes Readies until none is pending: durable writes (snapshot, entries, hard state — flushed), then messages,
    /// then apply, then <see cref="RaftNode.Advance"/>, then local checkpointing (RaftReady contract, CON-004).
    /// </summary>
    private async Task RunReadyLoopAsync()
    {
        if (_processing || !IsActive)
            return;
        _processing = true;
        try
        {
            while (IsActive && _node.HasReady())
            {
                RaftNode node = _node;
                RaftReady ready = node.GetReady();
                bool persisted = await HandleReadyAsync(ready);
                if (!persisted || !ReferenceEquals(node, _node) || _closed)
                    continue;
                node.Advance(ready);
                AfterAdvance(ready);
                await MaybeCheckpointAsync();
            }
        }
        catch (Exception ex) when (!_closed)
        {
            Fail(ex);
        }
        catch (Exception)
        {
            // Closed while processing: abandoned work.
        }
        finally
        {
            _processing = false;
            TaskCompletionSource? idle = _idle;
            _idle = null;
            if (idle is not null)
                Defer(() => idle.TrySetResult());
        }
    }

    /// <summary>Handles one Ready up to (not including) Advance. False when the node had to be reset from durable state.</summary>
    private async Task<bool> HandleReadyAsync(RaftReady ready)
    {
        // 1. Durable writes: snapshot install, entries (flush barrier), hard state.
        try
        {
            if (ready.SnapshotToInstall is { } meta && !await InstallSnapshotAsync(meta, ready.SnapshotData))
                return false;
            if (ready.EntriesToPersist.Count > 0)
                await PersistEntriesAsync(ready.EntriesToPersist);
            if (ready.HardState is { } hs)
                await _view.SaveHardStateAsync(hs);
        }
        catch (Exception ex) when (!_closed && IsRecoverablePersistFailure(ex))
        {
            _s.Error("raft", NodeServices.Inv($"{Name}: persisting a Ready failed ({ex.GetType().Name}: {ex.Message}); resetting from durable state."));
            _resetting = true; // the current core is abandoned: no proposals, ticks or inbound traffic until it is rebuilt
            await Task.Delay(PersistRetryDelay, _s.Time);
            if (!_closed)
                await ResetFromDurableStateAsync("persist failure", reopenLog: true);
            return false;
        }

        if (_closed)
            return false;

        // F10 after local append: leader-side proposals whose entries are now durable here (CON-007 blocks here).
        if (ready.EntriesToPersist.Count > 0 && _proposals.Count > 0)
            await ReportLocalAppendsAsync(ready.EntriesToPersist);

        // 2. Messages (depend on step 1).
        if (ready.Messages.Count > 0)
            SendMessages(ready.Messages);

        if (ready.Role == RaftRole.Candidate && _lastRole != RaftRole.Candidate)
            _s.Metrics.ElectionStarted();

        // 3. Apply committed entries; serve confirmed reads.
        if (ready.CommittedEntries.Count > 0)
            await ApplyEntriesAsync(ready.CommittedEntries);
        foreach (RaftReadState rs in ready.ReadStates)
        {
            if (_reads.TryGetValue(rs.RequestContext, out PendingRead? read))
                read.ReadIndex = rs.Index;
        }

        CompleteReadsAndWaiters();
        return !_closed;
    }

    private async Task PersistEntriesAsync(IReadOnlyList<RaftEntry> entries)
    {
        if (_postInstallCatchUp)
        {
            foreach (RaftEntry e in entries)
            {
                await _s.HitAsync(FaultStage.F16, FaultSites.RaftSnapshotInstall, VolumeName, DfsOperationKind.RaftSnapshot,
                    detail: ResyncDetails.Suffix);
                if (_closed)
                    return;
            }
        }

        await _view.AppendAsync(entries);
        await _view.FlushAsync();
        Stats.EntriesAppended += entries.Count;
        _s.Metrics.EntriesAppended(entries.Count);
    }

    private async Task ReportLocalAppendsAsync(IReadOnlyList<RaftEntry> entries)
    {
        ulong first = entries[0].Index;
        ulong last = entries[^1].Index;
        foreach (PendingProposal p in _proposals.Values.ToArray())
        {
            if (p.Index < first || p.Index > last || p.LocalAppendReported)
                continue;
            RaftEntry entry = entries[(int)(p.Index - first)];
            if (entry.Term != p.Term)
                continue;
            p.LocalAppendReported = true;
            try
            {
                await _s.HitAsync(FaultStage.F10, FaultSites.RaftAfterLocalAppend, VolumeName, p.Context.Operation, p.Context.OperationId, p.Context.Path);
            }
            catch (FaultReplyDroppedException)
            {
                p.DropReply = true; // the request executes; only its reply is lost
            }

            if (_closed)
                return;
        }
    }

    private void SendMessages(IReadOnlyList<RaftMessage> messages)
    {
        List<RaftMessage>? outgoing = null;
        if (_chunkCaptures.Count > 0)
        {
            outgoing = new List<RaftMessage>(messages.Count);
            foreach (RaftMessage m in messages)
            {
                if (m is InstallSnapshotResponse response && TryCapture(response))
                    continue;
                outgoing.Add(m);
            }
        }

        _host.Transport.Send(this, outgoing ?? messages);
    }

    private bool TryCapture(InstallSnapshotResponse response)
    {
        foreach (SnapshotChunkCapture capture in _chunkCaptures)
        {
            if (capture.Response is null && capture.Leader == response.To && capture.SnapshotIndex == response.SnapshotIndex)
            {
                capture.Response = response;
                return true;
            }
        }

        return false;
    }

    private async Task ApplyEntriesAsync(IReadOnlyList<RaftEntry> entries)
    {
        int applied = 0;
        foreach (RaftEntry entry in entries)
        {
            if (_closed)
                break;
            ProposalContext context = DescribeEntry(entry);
            try
            {
                await _s.HitAsync(FaultStage.F11, FaultSites.RaftAfterCommitBeforeApply, VolumeName, context.Operation, context.OperationId, context.Path,
                    detail: entry.Index.ToString(System.Globalization.CultureInfo.InvariantCulture));
            }
            catch (FaultReplyDroppedException)
            {
                // The entry is applied regardless; a local proposer loses its reply (a follower has none to lose).
                if (_proposals.TryGetValue((entry.Index, entry.Term), out PendingProposal? dropped))
                    dropped.DropReply = true;
            }

            if (_closed)
                break;

            ApplyOutcome outcome = ApplyOne(entry);
            _appliedIndex = entry.Index;
            _appliedTerm = entry.Term;
            applied++;
            Stats.EntriesApplied++;

            if (_proposals.Count > 0)
                CompleteProposals(entry, outcome);
        }

        if (applied > 0)
        {
            _s.Metrics.EntriesApplied(applied);
            if (_postInstallCatchUp && _appliedIndex >= _node.CommitIndex)
                _postInstallCatchUp = false;
            _host.OnApplied(this, _appliedIndex);
        }
    }

    private ApplyOutcome ApplyOne(RaftEntry entry)
    {
        switch (entry.Kind)
        {
            case RaftEntryKind.Normal:
                try
                {
                    return _sm.Apply(entry.Index, entry.Term, entry.Data) with { Index = entry.Index, Term = entry.Term };
                }
                catch (Exception ex) when (ex is not OutOfMemoryException)
                {
                    // Apply is deterministic, so every replica fails the same way; the entry is recorded as a failed command.
                    _s.Error("raft", NodeServices.Inv($"{Name}: applying entry {entry.Index} threw {ex.GetType().Name}: {ex.Message}"));
                    return ApplyOutcome.Failure(DfsErrorCode.Internal, "Applying the command failed: " + ex.Message) with { Index = entry.Index, Term = entry.Term };
                }

            case RaftEntryKind.ConfChange:
                if (RaftConfChange.TryDecode(entry.Data.Span, out RaftConfChange? change))
                {
                    _appliedConf = _appliedConf.Apply(change);
                    _host.OnConfApplied(this, _appliedConf);
                }

                return ApplyOutcome.Success() with { Index = entry.Index, Term = entry.Term };
            default:
                return ApplyOutcome.Success() with { Index = entry.Index, Term = entry.Term };
        }
    }

    private ProposalContext DescribeEntry(RaftEntry entry)
    {
        if (_proposals.TryGetValue((entry.Index, entry.Term), out PendingProposal? p))
            return p.Context;
        return entry.Kind switch
        {
            RaftEntryKind.ConfChange => new ProposalContext(DfsOperationKind.MembershipChange),
            RaftEntryKind.Normal when _sm is ICommandDescriber describer => SafeDescribe(describer, entry.Data),
            _ => ProposalContext.None,
        };
    }

    private static ProposalContext SafeDescribe(ICommandDescriber describer, ReadOnlyMemory<byte> data)
    {
        try
        {
            return describer.Describe(data);
        }
        catch (Exception ex) when (ex is DfsProtocolException or ArgumentException or InvalidOperationException)
        {
            return ProposalContext.None;
        }
    }

    private void CompleteProposals(RaftEntry entry, ApplyOutcome outcome)
    {
        // Proposals below the applied index whose entries never reached apply (snapshot install) are completed elsewhere.
        while (_proposals.Count > 0)
        {
            KeyValuePair<(ulong Index, ulong Term), PendingProposal> first = _proposals.First();
            if (first.Key.Index > entry.Index)
                break;
            _proposals.Remove(first.Key);
            PendingProposal p = first.Value;
            if (first.Key.Index < entry.Index)
            {
                Exception skipped = OutcomeUnknown(p.Context, $"entry {p.Index} was skipped by the local apply sequence");
                Defer(() => p.Completion.TrySetException(skipped));
                continue;
            }

            if (p.Term == entry.Term && p.DropReply)
            {
                var dropped = new FaultReplyDroppedException($"The reply to the proposal at index {p.Index} of group '{DisplayName}' was dropped by an injected fault.");
                Defer(() => p.Completion.TrySetException(dropped));
            }
            else if (p.Term == entry.Term)
            {
                Defer(() => p.Completion.TrySetResult(outcome));
            }
            else
            {
                var lost = new DfsUnavailableException(DfsErrorCode.NotLeader,
                    $"The proposal at index {p.Index} of group '{DisplayName}' lost: another leader's entry (term {entry.Term}) was committed there.")
                {
                    LeaderHint = _node.Leader is { } l && l != _s.NodeId ? _host.ResolveEndpoint(l) : null,
                };
                Defer(() => p.Completion.TrySetException(lost));
            }
        }
    }

    private void CompleteReadsAndWaiters()
    {
        if (_reads.Count > 0)
        {
            foreach (PendingRead read in _reads.Values)
            {
                if (read.ReadIndex is { } index && _appliedIndex >= index)
                {
                    ulong applied = _appliedIndex;
                    Defer(() => read.Completion.TrySetResult(applied));
                }
            }
        }

        if (_appliedWaiters.Count > 0)
        {
            for (int i = _appliedWaiters.Count - 1; i >= 0; i--)
            {
                if (_appliedIndex >= _appliedWaiters[i].Index)
                {
                    AppliedWaiter waiter = _appliedWaiters[i];
                    Defer(() => waiter.Completion.TrySetResult());
                    _appliedWaiters.RemoveAt(i);
                }
            }
        }
    }

    private void AfterAdvance(RaftReady ready)
    {
        RaftRole role = _node.Role;
        NodeId? leader = _node.Leader;
        ulong term = _node.Term;
        if (role == _lastRole && leader == _lastLeader && term == _lastTerm)
            return;
        bool lostLeadership = _lastRole == RaftRole.Leader && role != RaftRole.Leader;
        _lastRole = role;
        _lastLeader = leader;
        _lastTerm = term;
        if (lostLeadership || role != RaftRole.Leader)
            FailPendingReads();
        _host.OnLeadershipChanged(this);
        _ = ready;
    }

    private void FailPendingReads()
    {
        if (_reads.Count == 0)
            return;
        DfsUnavailableException error = NotLeader();
        foreach (PendingRead read in _reads.Values)
        {
            if (read.ReadIndex is null)
                Defer(() => read.Completion.TrySetException(error));
        }
    }

    /// <summary>
    /// Completes waiters in a separate loop work item: their continuations never run inside Ready processing (no
    /// re-entrancy), and completions happen on the loop thread with no asynchronous continuation dispatch (simulation-safe).
    /// </summary>
    private void Defer(Action completion)
    {
        _deferred.Add(completion);
        if (_deferredPosted)
            return;
        _deferredPosted = true;
        _s.Loop.Post(RunDeferred);
    }

    private void RunDeferred()
    {
        _deferredPosted = false;
        Action[] work = [.. _deferred];
        _deferred.Clear();
        foreach (Action completion in work)
            completion();
    }

    /// <summary>Waits until no Ready processing is running or scheduled.</summary>
    private async Task WaitIdleAsync()
    {
        while (_processing || _kickPosted)
        {
            _idle ??= new TaskCompletionSource();
            if (!_processing && _kickPosted)
            {
                // A posted run will start shortly; yield to it through the loop.
                var posted = new TaskCompletionSource();
                _s.Loop.Post(() => posted.TrySetResult());
                await posted.Task;
                continue;
            }

            await _idle.Task;
        }
    }

    /// <summary>
    /// Rebuilds the Raft core from the durable log and checkpoint after a failed install or persist (the in-memory Ready is
    /// abandoned; messages depending on it were never sent). Local proposals become outcome-unknown. After a storage failure
    /// (<paramref name="reopenLog"/>) the log store is reopened from the medium first: a failed write or flush faults the
    /// journal for good, and the in-memory store may hold appended entries that never became durable, which the rebuilt core
    /// would otherwise treat as persisted and acknowledge (CON-004). Inbound traffic and ticks are ignored meanwhile.
    /// </summary>
    private async Task ResetFromDurableStateAsync(string reason, bool reopenLog)
    {
        _resetting = true;
        try
        {
            foreach (PendingProposal p in _proposals.Values)
            {
                Exception unknown = OutcomeUnknown(p.Context, "the group was reset after a " + reason);
                Defer(() => p.Completion.TrySetException(unknown));
            }

            _proposals.Clear();
            var resetError = new DfsUnavailableException(DfsErrorCode.NotLeader, $"Group '{DisplayName}' was reset after a {reason}.");
            foreach (PendingRead r in _reads.Values)
                Defer(() => r.Completion.TrySetException(resetError));
            _reads.Clear();
            _chunkCaptures.Clear();
            _postInstallCatchUp = false;

            if (reopenLog && !await ReopenLogAsync())
                return;

            if (_appliedIndex < _view.Snapshot.Index)
            {
                RaftSnapshot? latest = await LoadLatestCheckpointAsync(_checkpoints, null, default);
                if (latest is null || latest.Meta.Index != _view.Snapshot.Index
                    || !SnapshotEnvelope.TryDecode(latest.Data, latest.Meta, out _, out ReadOnlyMemory<byte> state, out string? error))
                {
                    throw new DfsIntegrityException($"Group '{Name}': cannot restore the state at the compacted index {_view.Snapshot.Index}.");
                }

                _ = error;
                _sm.RestoreCheckpoint(new System.Buffers.ReadOnlySequence<byte>(state), latest.Meta.Index, latest.Meta.Term);
                _latestSnapshot = latest;
                _appliedIndex = latest.Meta.Index;
                _appliedTerm = latest.Meta.Term;
                _appliedConf = latest.Meta.ConfState;
            }

            CreateNode(_appliedIndex);
        }
        finally
        {
            _resetting = false;
        }

        _host.OnLeadershipChanged(this);
        Kick();
    }

    /// <summary>
    /// Makes every journal byte written so far durable (a later successful flush of a file covers earlier failed ones),
    /// then reopens the log store from the medium, which also lets journal recovery drop torn records. Retries while the
    /// medium keeps failing; false when the group was closed meanwhile.
    /// </summary>
    private async Task<bool> ReopenLogAsync()
    {
        RaftLogStore old = _view.Store;
        try
        {
            await old.DisposeAsync();
        }
        catch (Exception ex) when (ex is IOException or ObjectDisposedException or DfsException or UnauthorizedAccessException)
        {
            _s.Warn("raft", NodeServices.Inv($"{Name}: closing the failed log store: {ex.Message}"));
        }

        string logDirectory = LogDirectoryOf(Directory);
        while (!_closed)
        {
            try
            {
                await SyncJournalAsync(StoragePath.Combine(logDirectory, RaftLogStore.LogDirectoryName));
                RaftLogStore store = await RaftLogStore.OpenAsync(_s.Env.Medium, logDirectory, _storeOptions);
                try
                {
                    await RepairHardStateAsync(store, default);
                }
                catch
                {
                    await store.DisposeAsync();
                    throw;
                }

                if (_closed)
                {
                    await store.DisposeAsync();
                    return false;
                }

                _view = new RaftStorageAdapter(store);
                _s.Info("raft", NodeServices.Inv($"{Name}: reopened the log after a storage failure (last {store.LastIndex}, commit {store.HardState.Commit})."));
                return true;
            }
            catch (Exception ex) when (IsRecoverablePersistFailure(ex))
            {
                _s.Warn("raft", NodeServices.Inv($"{Name}: reopening the log failed ({ex.GetType().Name}: {ex.Message}); retrying."));
                await Task.Delay(PersistRetryDelay, _s.Time);
            }
        }

        return false;
    }

    private async Task SyncJournalAsync(string directory)
    {
        IStorageMedium medium = _s.Env.Medium;
        if (!await medium.ExistsAsync(directory))
            return;
        foreach (string name in await medium.ListAsync(directory))
        {
            if (!name.EndsWith(Journal.SegmentExtension, StringComparison.Ordinal))
                continue;
            await using IStorageFile file = await medium.OpenAsync(StoragePath.Combine(directory, name), StorageOpenMode.OpenExisting);
            await file.FlushAsync();
        }

        await medium.FlushDirectoryAsync(directory);
    }

    private void Fail(Exception ex)
    {
        _failure = ex;
        _s.Error("raft", NodeServices.Inv($"{Name}: the group stopped: {ex.GetType().Name}: {ex.Message}"));
        FailAll(new DfsUnavailableException(DfsErrorCode.Unavailable, $"Group '{DisplayName}' stopped on {_s.NodeName}: {ex.Message}"));
        _host.OnLeadershipChanged(this);
    }

    private static bool IsRecoverablePersistFailure(Exception ex)
        => ex is IOException or DfsCapacityException or UnauthorizedAccessException
            || (ex is DfsException d && d.ErrorCode is DfsErrorCode.CapacityExhausted or DfsErrorCode.Unavailable)
            || ex is SnapshotRejectedException;
}

/// <summary>Detail values of <see cref="FaultSites.RaftSnapshotInstall"/> hits (RSY suite, <c>ResyncStageDetails</c>).</summary>
internal static class ResyncDetails
{
    public const string Receive = "receive";
    public const string Verify = "verify";
    public const string Install = "install";
    public const string Suffix = "suffix";
    public const string Activate = "activate";
}

/// <summary>A received snapshot failed verification and was not installed.</summary>
internal sealed class SnapshotRejectedException : Exception
{
    public SnapshotRejectedException(string message)
        : base(message)
    {
    }
}
