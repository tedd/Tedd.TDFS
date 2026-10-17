using Tedd.Tdfs.Consensus;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Runtime;

namespace Tedd.Tdfs.Server.Control;

/// <summary>
/// The audit pipeline (SEC-016/018): security events observed on this node are queued and committed to the replicated
/// audit log (proposed when this node leads the control group, otherwise forwarded to the control leader with
/// <see cref="AuditEventMessage"/>), retried until they are committed. Denials by authenticated principals are never dropped
/// by a rate limit: each is one audit entry, and the denied caller is answered once its entry is committed (bounded by
/// <see cref="AuditWait"/>), which also back-pressures a principal that keeps sending denied requests. Floods stay bounded:
/// unauthenticated events (failed handshakes) pass a token bucket, the queue holds at most <see cref="MaxQueuedAudits"/>
/// records (<see cref="MaxQueuedAuditsPerPrincipal"/> per principal), and what exceeds a bound is counted into an explicit
/// <c>audit:aggregated</c> entry (with the number of events it stands for) instead of being silently lost.
/// </summary>
internal sealed partial class ControlHost
{
    /// <summary>Records queued on this node (own and, on the control leader, forwarded ones) before events are aggregated.</summary>
    private const int MaxQueuedAudits = 1024;

    /// <summary>Records of one principal queued before further events of it are aggregated.</summary>
    private const int MaxQueuedAuditsPerPrincipal = 64;

    /// <summary>Records proposed (or forwarded) at once; the control log batches them into few appends.</summary>
    private const int AuditPipeline = 32;

    /// <summary>Unauthenticated events (failed handshakes) audited one by one in a burst, then <see cref="UnauthenticatedAuditRefillPerSecond"/>.</summary>
    private const int UnauthenticatedAuditBurst = 100;

    private const double UnauthenticatedAuditRefillPerSecond = 20;

    /// <summary>Principal of events without an authenticated principal.</summary>
    private const string Unauthenticated = "-";

    private readonly LinkedList<PendingAudit> _audits = new();
    private readonly SortedDictionary<string, int> _queuedAuditsByPrincipal = new(StringComparer.Ordinal);
    private readonly SortedDictionary<string, PendingAudit> _openAggregates = new(StringComparer.Ordinal);
    private readonly List<AuditWaiter> _auditWaiters = [];
    private long _auditSequence;
    private bool _auditing;
    private double _auditTokens = UnauthenticatedAuditBurst;
    private long _auditRefilled;
    private long _suppressedUnauthenticated;
    private bool _suppressedFlushScheduled;

    /// <summary>
    /// Sequence number of the latest security event reported on this node (<see cref="WaitForAuditsAsync"/> waits for every
    /// event up to a sequence number).
    /// </summary>
    public long AuditSequence => _auditSequence;

    /// <summary>
    /// Longest wait of a caller for its audit record: a few election timeouts (at least a second), so an unavailable control
    /// group delays the audited reply only briefly (the record stays queued and is committed once the group is back).
    /// </summary>
    private TimeSpan AuditWait => TimeSpan.FromTicks(Math.Max(TimeSpan.FromSeconds(1).Ticks, _s.Timings.Tick.Ticks * _s.Timings.ElectionTicksMax * 4));

    /// <summary>Pause before an undelivered batch is retried (one election window, at least 50 ms).</summary>
    private TimeSpan AuditRetryDelay => TimeSpan.FromTicks(Math.Max(TimeSpan.FromMilliseconds(50).Ticks, _s.Timings.Tick.Ticks * _s.Timings.ElectionTicksMax));

    /// <summary>
    /// Records a security event (failed authentication, denied access) in the replicated audit log: the control leader
    /// proposes it, any other node forwards it to the control leader (<see cref="AuditEventMessage"/>), so an event is audited
    /// whichever node observed it (SEC-016). Queued and retried until committed; never throws. Unauthenticated events (no
    /// principal) are rate limited, the number suppressed is audited in an <c>audit:aggregated</c> entry.
    /// </summary>
    public void ReportSecurityEvent(string operation, string outcome, string scope, string principal = Unauthenticated)
        => EnqueueAudit(principal, operation, outcome, scope, forwarded: false);

    /// <summary>
    /// Like <see cref="ReportSecurityEvent"/>, but returns once the audit record is committed (or <see cref="AuditWait"/>
    /// passed, when the record stays queued): an administrative denial is answered only after it is visible in the audit log
    /// (read-your-writes for the denied caller's auditor). Never throws.
    /// </summary>
    public async Task ReportSecurityEventAndWaitAsync(string operation, string outcome, string scope, string principal = Unauthenticated)
    {
        if (EnqueueAudit(principal, operation, outcome, scope, forwarded: false) is { } record)
            await WaitAsync(record.Done.Task, AuditWait);
    }

    /// <summary>
    /// Records an event another node forwarded (<see cref="AuditEventMessage"/>, control leader only) and completes once it is
    /// committed (the forwarding node answers its own caller after that, read-your-writes as for local events). False when
    /// this node does not lead the control group or the record was not committed within half a peer call timeout (the
    /// forwarding node then retries; a record may then be audited twice, never lost). Never throws.
    /// </summary>
    public async Task<bool> RecordForwardedSecurityEventAsync(AuditEventMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);
        if (!IsLeader || EnqueueAudit(message.Principal, message.Operation, message.Outcome, message.Scope, forwarded: true) is not { } record)
            return false;
        await WaitAsync(record.Done.Task, _s.Peers.DefaultCallTimeout / 2);
        return record.Done.Task.IsCompletedSuccessfully && record.Done.Task.Result;
    }

    /// <summary>
    /// Waits until every security event reported on this node up to <paramref name="sequence"/> (<see cref="AuditSequence"/>)
    /// is committed, at most <see cref="AuditWait"/>; true when they are. The RPC dispatcher answers a denied request only
    /// after this (every denial is in the audit log before the caller learns of it). Never throws.
    /// </summary>
    public async Task<bool> WaitForAuditsAsync(long sequence)
    {
        if (IsAuditedThrough(sequence))
            return true;
        var waiter = new AuditWaiter(sequence);
        _auditWaiters.Add(waiter);
        await WaitAsync(waiter.Done.Task, AuditWait);
        _auditWaiters.Remove(waiter);
        return IsAuditedThrough(sequence);
    }

    private bool IsAuditedThrough(long sequence) => _audits.First is not { } oldest || oldest.Value.Sequence > sequence;

    private async Task WaitAsync(Task task, TimeSpan timeout)
    {
        if (!task.IsCompleted)
            await Task.WhenAny(task, Task.Delay(timeout, _s.Time));
    }

    /// <summary>Queues one record (or counts it into an open aggregate); null when it was suppressed by the unauthenticated rate limit.</summary>
    private PendingAudit? EnqueueAudit(string principal, string operation, string outcome, string scope, bool forwarded)
    {
        string who = string.IsNullOrEmpty(principal) ? Unauthenticated : principal;
        if (_s.IsStopping)
            return null;

        // Members bound what they forward with their own bucket; the leader's queue bound covers the rest.
        if (who == Unauthenticated && !forwarded && !TakeAuditToken())
        {
            _suppressedUnauthenticated++;
            if (!_suppressedFlushScheduled)
                _ = FlushSuppressedAsync();
            return null;
        }

        bool globalBound = _audits.Count >= MaxQueuedAudits;
        if (globalBound || _queuedAuditsByPrincipal.GetValueOrDefault(who) >= MaxQueuedAuditsPerPrincipal)
        {
            // Over a bound: the event is counted into one open aggregate entry of the principal (all principals at the global
            // bound) and outcome, which records how many events it stands for.
            string aggregatePrincipal = globalBound ? "*" : who;
            string key = aggregatePrincipal + "\n" + outcome;
            if (_openAggregates.TryGetValue(key, out PendingAudit? open) && !open.InFlight)
            {
                open.Events++;
                open.Scope = AggregateScope(open.Events, operation, scope);
                return open;
            }

            PendingAudit aggregate = Append(new PendingAudit(++_auditSequence, aggregatePrincipal, "audit:aggregated", outcome, AggregateScope(1, operation, scope), isAggregate: true));
            _openAggregates[key] = aggregate;
            return aggregate;
        }

        _queuedAuditsByPrincipal[who] = _queuedAuditsByPrincipal.GetValueOrDefault(who) + 1;
        return Append(new PendingAudit(++_auditSequence, who, operation, outcome, scope, isAggregate: false));
    }

    private static string AggregateScope(long events, string lastOperation, string lastScope)
        => NodeServices.Inv($"events={events} last-op={lastOperation} last-scope={lastScope}");

    private PendingAudit Append(PendingAudit record)
    {
        record.Node = _audits.AddLast(record);
        if (!_auditing)
            _ = DrainAuditsAsync();
        return record;
    }

    /// <summary>Audits how many unauthenticated events the rate limit suppressed (at most one entry per second).</summary>
    private async Task FlushSuppressedAsync()
    {
        _suppressedFlushScheduled = true;
        try
        {
            await Task.Delay(TimeSpan.FromSeconds(1), _s.Time);
        }
        finally
        {
            _suppressedFlushScheduled = false;
        }

        long suppressed = _suppressedUnauthenticated;
        _suppressedUnauthenticated = 0;
        if (suppressed > 0 && !_s.IsStopping)
        {
            Append(new PendingAudit(++_auditSequence, Unauthenticated, "audit:aggregated", "suppressed",
                NodeServices.Inv($"events={suppressed} unauthenticated events over the rate limit"), isAggregate: true));
        }
    }

    /// <summary>Token bucket (burst <see cref="UnauthenticatedAuditBurst"/>, refill per second) for unauthenticated events.</summary>
    private bool TakeAuditToken()
    {
        long now = _s.Time.GetTimestamp();
        if (_auditRefilled != 0)
            _auditTokens = Math.Min(UnauthenticatedAuditBurst, _auditTokens + (_s.Time.GetElapsedTime(_auditRefilled, now).TotalSeconds * UnauthenticatedAuditRefillPerSecond));
        _auditRefilled = now;
        if (_auditTokens < 1)
            return false;
        _auditTokens--;
        return true;
    }

    /// <summary>
    /// Commits the queue in order, a pipeline of records at a time: proposed while this node leads the control group,
    /// forwarded to the control leader otherwise. What is not committed (no leader, leadership lost, deadline, forwarding
    /// failure) stays queued and is retried after <see cref="AuditRetryDelay"/>; a retry after an unknown outcome may audit
    /// a record twice, never lose it.
    /// </summary>
    private async Task DrainAuditsAsync()
    {
        _auditing = true;
        try
        {
            while (_audits.Count > 0 && !_s.IsStopping)
            {
                var batch = new List<PendingAudit>(Math.Min(AuditPipeline, _audits.Count));
                for (LinkedListNode<PendingAudit>? node = _audits.First; node is not null && batch.Count < AuditPipeline; node = node.Next)
                {
                    node.Value.InFlight = true;
                    batch.Add(node.Value);
                }

                bool[] committed;
                if (IsLeader)
                    committed = await Task.WhenAll(batch.Select(ProposeAuditAsync));
                else if (Leader is { } leader && leader != _s.NodeId)
                    committed = await Task.WhenAll(batch.Select(record => ForwardAuditAsync(leader, record)));
                else
                    committed = new bool[batch.Count];

                bool retry = false;
                for (int i = 0; i < batch.Count; i++)
                {
                    if (committed[i])
                    {
                        Settle(batch[i], true);
                    }
                    else
                    {
                        batch[i].InFlight = false;
                        retry = true;
                    }
                }

                if (retry && !_s.IsStopping)
                    await Task.Delay(AuditRetryDelay, _s.Time);
            }
        }
        finally
        {
            _auditing = false;
            if (_s.IsStopping)
            {
                while (_audits.First is { } left)
                    Settle(left.Value, false);
            }
        }
    }

    private async Task<bool> ProposeAuditAsync(PendingAudit record)
    {
        try
        {
            using CancellationTokenSource deadline = _s.CreateDeadline();
            await ProposeAsync(new AppendAuditCommand(NewHeader(default, record.Principal), record.Operation, record.Outcome, record.Scope), deadline.Token);
            return true;
        }
        catch (Exception ex) when (ex is DfsException or OperationCanceledException or ObjectDisposedException)
        {
            _s.Debug("control", NodeServices.Inv($"Proposing the audit record '{record.Operation}' failed (retried): {ex.Message}"));
            return false;
        }
    }

    /// <summary>Forwards one record to the control leader; true once the leader reports it committed.</summary>
    private async Task<bool> ForwardAuditAsync(NodeId leader, PendingAudit record)
    {
        try
        {
            using CancellationTokenSource deadline = _s.CreateDeadline();
            IWireMessage reply = await _s.Peers.CallAsync(leader, new AuditEventMessage(record.Operation, record.Outcome, record.Scope, record.Principal), null,
                deadline.Token);
            return reply is AuditEventResultMessage { Accepted: true };
        }
        catch (Exception ex) when (ex is DfsException or IOException or TimeoutException or UnauthorizedAccessException or OperationCanceledException
                                       or ObjectDisposedException)
        {
            _s.Debug("control", NodeServices.Inv($"Forwarding the audit record '{record.Operation}' to the control leader failed (retried): {ex.Message}"));
            return false;
        }
    }

    private void Settle(PendingAudit record, bool committed)
    {
        if (record.Node is { } node)
        {
            _audits.Remove(node);
            record.Node = null;
        }

        if (record.IsAggregate)
        {
            foreach (KeyValuePair<string, PendingAudit> open in _openAggregates)
            {
                if (ReferenceEquals(open.Value, record))
                {
                    _openAggregates.Remove(open.Key);
                    break;
                }
            }
        }
        else if (_queuedAuditsByPrincipal.TryGetValue(record.Principal, out int queued))
        {
            if (queued <= 1)
                _queuedAuditsByPrincipal.Remove(record.Principal);
            else
                _queuedAuditsByPrincipal[record.Principal] = queued - 1;
        }

        record.Done.TrySetResult(committed);
        for (int i = _auditWaiters.Count - 1; i >= 0; i--)
        {
            AuditWaiter waiter = _auditWaiters[i];
            if (IsAuditedThrough(waiter.Sequence))
            {
                _auditWaiters.RemoveAt(i);
                waiter.Done.TrySetResult();
            }
        }
    }

    /// <summary>One queued audit record (or an aggregate of several events).</summary>
    private sealed class PendingAudit(long sequence, string principal, string operation, string outcome, string scope, bool isAggregate)
    {
        public long Sequence { get; } = sequence;

        public string Principal { get; } = principal;

        public string Operation { get; } = operation;

        public string Outcome { get; } = outcome;

        public string Scope { get; set; } = scope;

        public bool IsAggregate { get; } = isAggregate;

        /// <summary>Events the record stands for (aggregates count further events until they are sent).</summary>
        public long Events { get; set; } = 1;

        public bool InFlight { get; set; }

        public LinkedListNode<PendingAudit>? Node { get; set; }

        public TaskCompletionSource<bool> Done { get; } = new();
    }

    private sealed class AuditWaiter(long sequence)
    {
        public long Sequence { get; } = sequence;

        public TaskCompletionSource Done { get; } = new();
    }
}
