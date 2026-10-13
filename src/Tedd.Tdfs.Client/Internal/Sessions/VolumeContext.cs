using Tedd.Tdfs.Client.Internal.Data;
using Tedd.Tdfs.Client.Internal.Rpc;
using Tedd.Tdfs.Client.Internal.Runtime;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;

namespace Tedd.Tdfs.Client.Internal.Sessions;

/// <summary>Something bound to a handle lease (a stream): told when the lease is lost.</summary>
internal interface ILeaseHolder
{
    /// <summary>The handle's lease (or its session) expired; the holder faults with <paramref name="reason"/>.</summary>
    void OnLeaseLost(Exception reason);
}

/// <summary>Something holding a reader pin (snapshot view, enumeration): told when the pin expired.</summary>
internal interface IPinHolder
{
    /// <summary>The pin expired; later reads through it fail with <paramref name="reason"/>.</summary>
    void OnPinExpired(Exception reason);
}

/// <summary>
/// Per-client state of one volume (ARCHITECTURE §7, PROTOCOL.md §4–6): the volume binding (id and epoch, so a re-created
/// name never receives requests meant for the old volume), leader routing (route cache, <c>GetVolumeRoute</c> discovery,
/// <c>NotLeader</c> hints, suspect nodes), the lazily opened session with its operation sequences and acknowledgement
/// watermark, the keep-alive loop renewing the session, handle leases, live-read pin advances and reader pins, and the
/// unbound staging session shared by the whole-file helpers.
/// </summary>
internal sealed class VolumeContext : IRpcRouter
{
    /// <summary>Sequences reserved per internal block.</summary>
    public const int SequenceBlockSize = 64;

    private static readonly TimeSpan SuspectDuration = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan MinimumRouteHedge = TimeSpan.FromMilliseconds(100);

    private readonly Lock _sync = new();
    private readonly AsyncLock _sessionLock = new();
    private readonly AsyncLock _reserveLock = new();
    private readonly AsyncLock _stagingLock = new();
    private readonly Dictionary<string, long> _suspects = new(StringComparer.Ordinal);
    private readonly Dictionary<ulong, Registration<ILeaseHolder>> _handles = [];
    private readonly Dictionary<ulong, Registration<IPinHolder>> _pins = [];
    private readonly Dictionary<ulong, ulong> _pinUpdates = [];
    private VolumeEntry? _entry;
    private bool _bound;
    private VolumeId _volumeId;
    private ulong _epoch;
    private string? _leader;
    private string? _lastLeader;   // the last endpoint known as leader (kept while the route is being rediscovered)
    private VolumeSession? _session;
    private StagingLease? _helperStaging;
    private ITimer? _timer;
    private TimeSpan _timerPeriod;
    private int _keepAliveRunning;
    private bool _stopped;

    public VolumeContext(ClientRuntime runtime, string name)
    {
        Runtime = runtime;
        Name = name;
    }

    /// <summary>The client runtime.</summary>
    public ClientRuntime Runtime { get; }

    /// <summary>Volume name.</summary>
    public string Name { get; }

    /// <summary>The volume's map entry as last learned (policies, inline threshold, voters), or null before the first route.</summary>
    public VolumeEntry? Entry
    {
        get
        {
            lock (_sync)
                return _entry ?? Runtime.Map.Volume(Name);
        }
    }

    /// <summary>Largest publication of one request: the volume's limit when known, never above the cluster's.</summary>
    public long MaxPublishBytes => Math.Min(Runtime.Capabilities.MaxPublishBytes, Entry?.Limits.MaxPublishBytes ?? long.MaxValue);

    /// <summary>Largest file length: the volume's limit when known, never above the cluster's.</summary>
    public long MaxFileLength => Math.Min(Runtime.Capabilities.MaxFileLength, Entry?.Limits.MaxFileLength ?? long.MaxValue);

    /// <summary>The bound volume id (empty before the first route).</summary>
    public VolumeId VolumeId
    {
        get
        {
            lock (_sync)
                return _volumeId;
        }
    }

    /// <summary>The current session, if open and not known to be expired.</summary>
    public VolumeSession? CurrentSession
    {
        get
        {
            lock (_sync)
                return _session is { IsExpired: false } s ? s : null;
        }
    }

    // ------------------------------------------------------------------------------------------------ routing

    /// <inheritdoc />
    public async ValueTask<string?> ResolveAsync(OperationScope scope, TimeSpan attemptTimeout)
    {
        if (Runtime.IsBackupHost)
            return Runtime.BackupHostEndpoint;
        lock (_sync)
        {
            if (_leader is not null)
                return _leader;
        }

        return await RefreshRouteAsync(scope, attemptTimeout).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public void OnNotLeader(string endpoint, string? hint)
    {
        lock (_sync)
        {
            if (hint is { Length: > 0 } && !string.Equals(hint, endpoint, StringComparison.Ordinal))
            {
                _leader = hint;
                _lastLeader = hint;
                return;
            }

            if (string.Equals(_leader, endpoint, StringComparison.Ordinal))
                _leader = null;
            _suspects[endpoint] = Runtime.Time.GetTimestamp();
        }
    }

    /// <inheritdoc />
    public void OnUnreachable(string endpoint)
    {
        lock (_sync)
        {
            if (string.Equals(_leader, endpoint, StringComparison.Ordinal))
                _leader = null;
            _suspects[endpoint] = Runtime.Time.GetTimestamp();
        }
    }

    /// <inheritdoc />
    public bool RoutesToGroupLeader => !Runtime.IsBackupHost;

    /// <inheritdoc />
    public void OnAnswered(string endpoint)
    {
        lock (_sync)
            _suspects.Remove(endpoint);
        Runtime.Map.MarkGood(endpoint);
    }

    /// <summary>Forgets the cached leader (the next request rediscovers it).</summary>
    public void InvalidateRoute()
    {
        lock (_sync)
            _leader = null;
    }

    /// <summary>
    /// Asks the volume's voters (then any live node) for the volume's route with <c>GetVolumeRoute</c>, updates the
    /// binding and returns the leader endpoint, or null when no node currently knows a leader. A candidate that does not
    /// answer within a short hedge delay gets the next one asked alongside it (the first route naming a leader wins), so
    /// one paused or partitioned voter does not hold up discovery for a whole attempt timeout.
    /// </summary>
    public async Task<string?> RefreshRouteAsync(OperationScope scope, TimeSpan attemptTimeout)
    {
        var tried = new HashSet<string>(StringComparer.Ordinal);
        var queue = new List<string>();
        EnqueueVoters(queue, tried, Entry);
        foreach (string endpoint in Runtime.Map.DiscoveryEndpoints())
            Enqueue(queue, tried, endpoint);

        // Suspects last (they may have recovered, but healthy candidates answer first).
        List<string> ordered = OrderBySuspicion(queue);
        TimeSpan hedge = RouteHedgeDelay(attemptTimeout);
        var probes = new List<(Task<IWireMessage> Task, string Endpoint)>();
        int next = 0;
        long lastStart = 0;
        Exception? last = null;
        using var cancel = CancellationTokenSource.CreateLinkedTokenSource(scope.Token);
        try
        {
            while (!scope.Token.IsCancellationRequested)
            {
                if (probes.Count == 0)
                {
                    if (next >= ordered.Count)
                        break;
                    StartProbe(ordered[next++]);
                    continue;
                }

                Task<Task<IWireMessage>> any = Task.WhenAny(probes.Select(p => p.Task));
                try
                {
                    if (next >= ordered.Count)
                    {
                        await Waits.WaitAsync(any, scope.Token).ConfigureAwait(false);
                    }
                    else if (!await Waits.CompletesWithinAsync(any, hedge - Runtime.Time.GetElapsedTime(lastStart), Runtime.Time, scope.Token)
                                 .ConfigureAwait(false))
                    {
                        StartProbe(ordered[next++]);
                        continue;
                    }
                }
                catch (OperationCanceledException) when (scope.Token.IsCancellationRequested)
                {
                    break;
                }

                Task<IWireMessage> finished = any.Result;
                int index = probes.FindIndex(p => ReferenceEquals(p.Task, finished));
                string endpoint = probes[index].Endpoint;
                probes.RemoveAt(index);
                IWireMessage reply;
                try
                {
                    reply = await finished.ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (scope.Token.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex) when (RpcInvoker.IsTransient(ex) || ex is DfsClusterIdentityException or DfsProtocolException)
                {
                    last = ex;
                    OnUnreachable(endpoint);
                    continue;
                }

                switch (reply)
                {
                    case VolumeRouteResultMessage route:
                        OnAnswered(endpoint);
                        Bind(route.Volume);
                        string? leader = route.LeaderEndpoint;
                        if (leader is null && route.Leader is { } leaderId)
                            leader = Runtime.Map.EndpointOf(leaderId);
                        if (leader is not null)
                        {
                            lock (_sync)
                            {
                                _leader = leader;
                                _lastLeader = leader;
                                _suspects.Remove(leader);
                            }

                            // Candidates still silent while another answered are tried last next time.
                            foreach ((_, string silent) in probes)
                                OnUnreachable(silent);
                            return leader;
                        }

                        // This node does not host the group: ask the voters it names.
                        int before = ordered.Count;
                        var extra = new List<string>();
                        EnqueueVoters(extra, tried, route.Volume);
                        ordered.InsertRange(next, extra);
                        if (ordered.Count == before)
                            last = new DfsUnavailableException(DfsErrorCode.NotLeader, $"Volume '{Name}' currently has no known leader.");
                        break;

                    case ErrorReplyMessage { Code: DfsErrorCode.VolumeNotFound } missing:
                        throw missing.ToException();

                    case ErrorReplyMessage { Code: DfsErrorCode.AccessDenied or DfsErrorCode.AuthenticationFailed } denied:
                        throw denied.ToException();

                    case ErrorReplyMessage error:
                        last = error.ToException();
                        break;

                    default:
                        last = new DfsProtocolException($"Expected VolumeRouteResult from {endpoint}, received {reply.Type}.");
                        break;
                }
            }
        }
        finally
        {
            ContextFree.Cancel(cancel);
            foreach ((Task<IWireMessage> task, _) in probes)
                Waits.ObserveQuietly(task);
        }

        if (last is not null)
            Runtime.Emit("volume.route-unknown", $"No leader of volume '{Name}' is currently known ({last.GetType().Name}).", volume: Name, exception: last);
        return null;

        void StartProbe(string endpoint)
        {
            TimeSpan timeout = attemptTimeout < scope.Remaining ? attemptTimeout : scope.Remaining;
            lastStart = Runtime.Time.GetTimestamp();
            probes.Add((Runtime.Rpc.SendOnceAsync(endpoint, new GetVolumeRouteMessage(Name), scope, ClientRuntime.Positive(timeout), token: cancel.Token), endpoint));
        }
    }

    /// <summary>How long a route candidate may stay silent before the next one is asked alongside it.</summary>
    private static TimeSpan RouteHedgeDelay(TimeSpan attemptTimeout)
    {
        TimeSpan eighth = attemptTimeout / 8;
        TimeSpan hedge = eighth > MinimumRouteHedge ? eighth : MinimumRouteHedge;
        return hedge < attemptTimeout ? hedge : attemptTimeout;
    }

    private List<string> OrderBySuspicion(List<string> candidates)
    {
        var healthy = new List<string>(candidates.Count);
        var suspect = new List<string>();
        lock (_sync)
        {
            long now = Runtime.Time.GetTimestamp();
            foreach (string endpoint in candidates)
            {
                if (_suspects.TryGetValue(endpoint, out long since) && Runtime.Time.GetElapsedTime(since, now) < SuspectDuration)
                    suspect.Add(endpoint);
                else
                    healthy.Add(endpoint);
            }
        }

        healthy.AddRange(suspect);
        return healthy;
    }

    private void EnqueueVoters(List<string> queue, HashSet<string> tried, VolumeEntry? entry)
    {
        if (entry is null)
            return;
        if (entry.LeaderHint is { } hint && Runtime.Map.EndpointOf(hint) is { } hinted)
            Enqueue(queue, tried, hinted);
        foreach (NodeId voter in entry.Voters)
        {
            if (Runtime.Map.EndpointOf(voter) is { } endpoint)
                Enqueue(queue, tried, endpoint);
        }
    }

    private static void Enqueue(List<string> queue, HashSet<string> tried, string endpoint)
    {
        if (tried.Add(endpoint))
            queue.Add(endpoint);
    }

    /// <summary>
    /// Records the volume entry learned from a route or the map. A different volume id or epoch than the bound one means
    /// the name now designates another volume (re-created or restored): the session, handles and pins of the old volume
    /// are abandoned (their holders fault) and requests carry the new identity.
    /// </summary>
    public void Bind(VolumeEntry entry)
    {
        ArgumentNullException.ThrowIfNull(entry);
        List<ILeaseHolder> lostHandles = [];
        List<IPinHolder> lostPins = [];
        lock (_sync)
        {
            _entry = entry;
            if (!_bound)
            {
                _bound = true;
                _volumeId = entry.Id;
                _epoch = entry.Epoch;
                return;
            }

            if (entry.Id == _volumeId && entry.Epoch == _epoch)
                return;
            Runtime.Emit("volume.rebound", $"Volume name '{Name}' now designates volume {entry.Id} epoch {entry.Epoch} (was {_volumeId} epoch {_epoch}).",
                volume: Name);
            _volumeId = entry.Id;
            _epoch = entry.Epoch;
            _leader = null;
            _session?.MarkExpired();
            _session = null;
            _helperStaging = null;
            CollectAll(lostHandles, lostPins);
        }

        var reason = new DfsException(DfsErrorCode.EpochMismatch,
            $"Volume '{Name}' was re-created or restored; handles and pins of the previous volume are no longer valid.");
        Notify(lostHandles, lostPins, reason, DfsErrors.ToException(DfsErrorCode.PinExpired, reason.Message));
    }

    /// <summary>The binding requests that refer to nothing session-owned carry: the bound volume (or the map's entry before binding).</summary>
    public VolumeBinding CurrentBinding
    {
        get
        {
            lock (_sync)
                return CurrentBindingLocked();
        }
    }

    private VolumeBinding CurrentBindingLocked()
    {
        if (_bound)
            return new VolumeBinding(_volumeId, _epoch);
        VolumeEntry? mapped = Runtime.Map.Volume(Name);
        return new VolumeBinding(mapped?.Id ?? default, mapped?.Epoch ?? 0);
    }

    /// <summary>
    /// Builds a request header. The expected volume is <paramref name="binding"/> when given (requests about something a
    /// session owns: its operation ids, handles, pins, staging sessions, transactions), else the binding of
    /// <paramref name="session"/>, else the current binding — so a request meant for a volume that was since re-created
    /// or restored is rejected (<c>EpochMismatch</c>) instead of being applied to the new volume.
    /// </summary>
    public RequestHeader Header(VolumeSession? session, DfsOperationId operation = default, DfsTransactionId? transaction = null, VolumeBinding? binding = null)
    {
        lock (_sync)
        {
            VolumeBinding target = binding ?? session?.Binding ?? CurrentBindingLocked();
            VolumeSession? s = session is { IsExpired: false } ? session : null;
            return new RequestHeader(Name, target.Id, target.Epoch, s?.Id ?? 0, operation, s?.Watermark ?? 0, transaction);
        }
    }

    // ------------------------------------------------------------------------------------------------ sessions

    /// <summary>Returns the open session, opening one (<c>OpenSession</c>) when none is open.</summary>
    public async ValueTask<VolumeSession> EnsureSessionAsync(OperationScope scope)
    {
        if (CurrentSession is { } current)
            return current;
        if (Runtime.IsDisposed && scope.IsKilled)
            throw scope.NotTransmittedFailure(null);

        using AsyncLock.Releaser held = await LockAsync(_sessionLock, scope).ConfigureAwait(false);
        if (CurrentSession is { } opened)
            return opened;

        // One nonce for every retry of this open: a retried request receives the same session (PROTOCOL.md §5.3).
        UInt128 nonce = Runtime.Random.NextUInt128();
        VolumeBinding openedUnder = default;
        var request = new RpcRequest
        {
            Name = "OpenSession",
            ReplyType = MessageType.OpenSessionResult,
            Kind = CallKind.Idempotent,
            Router = this,
            Build = _ =>
            {
                RequestHeader header = Header(null);
                openedUnder = new VolumeBinding(header.ExpectedVolumeId, header.ExpectedVolumeEpoch);
                return ValueTask.FromResult<IWireMessage>(new OpenSessionMessage(header, Runtime.Options.ClientName, nonce));
            },
            OnError = RebindOnEpochMismatchAsync,
        };
        var reply = (OpenSessionResultMessage)await Runtime.Rpc.InvokeAsync(request, scope).ConfigureAwait(false);
        VolumeSession session;
        lock (_sync)
        {
            // The session belongs to the volume the answered request was addressed to.
            session = new VolumeSession(reply.Session, reply.FirstSequence, reply.LeaseDuration, reply.SessionTimeout, openedUnder.Id, openedUnder.Epoch);
            _session = session;
            _helperStaging = null;
        }

        Runtime.Emit("session.opened", $"Opened session {reply.Session} on volume '{Name}'.", volume: Name, token: reply.Token);
        EnsureKeepAlive(session);
        return session;
    }

    /// <summary>Marks <paramref name="session"/> expired (the next request opens a new one) and faults what it owned.</summary>
    public void ExpireSession(VolumeSession session, string why)
    {
        List<ILeaseHolder> lostHandles = [];
        List<IPinHolder> lostPins = [];
        lock (_sync)
        {
            if (session.IsExpired && !ReferenceEquals(_session, session))
                return;
            session.MarkExpired();
            if (ReferenceEquals(_session, session))
            {
                _session = null;
                _helperStaging = null;
            }

            CollectSession(session.Id, lostHandles, lostPins);
        }

        Runtime.Emit("session.expired", $"Session {session.Id} on volume '{Name}' expired: {why}", volume: Name);
        var reason = (Exception)DfsErrors.ToException(DfsErrorCode.LeaseLost, $"The session of this handle on volume '{Name}' expired: {why}");
        Notify(lostHandles, lostPins, reason, DfsErrors.ToException(DfsErrorCode.PinExpired, $"The session holding this pin on volume '{Name}' expired: {why}"));
    }

    /// <summary>Allocates an SDK-internal operation id (reserving a new block from the leader when the current one is used up).</summary>
    public async ValueTask<DfsOperationId> AllocateAsync(OperationScope scope) =>
        (await AllocateWithSessionAsync(scope).ConfigureAwait(false)).Id;

    /// <summary>Allocates an SDK-internal operation id and returns the session (and so the volume binding) it belongs to.</summary>
    public async ValueTask<(DfsOperationId Id, VolumeSession Session)> AllocateWithSessionAsync(OperationScope scope)
    {
        for (int attempt = 0; ; attempt++)
        {
            VolumeSession session = await EnsureSessionAsync(scope).ConfigureAwait(false);
            if (session.TryAllocate(out DfsOperationId id))
                return (id, session);
            using AsyncLock.Releaser held = await LockAsync(_reserveLock, scope).ConfigureAwait(false);
            if (session.TryAllocate(out id))
                return (id, session);
            try
            {
                ReserveOperationIdsResultMessage block = await ReserveAsync(session, SequenceBlockSize, scope).ConfigureAwait(false);
                if (block.Session == session.Id && !session.IsExpired)
                    session.AddBlock(block.FirstSequence, block.Count);
            }
            catch (DfsException ex) when (attempt < 2 && ex.ErrorCode is DfsErrorCode.LeaseLost or DfsErrorCode.OperationExpired or DfsErrorCode.LimitExceeded)
            {
                ExpireSession(session, ex.Message);
            }
            catch (DfsException ex) when (attempt < 2 && ex.ErrorCode is DfsErrorCode.EpochMismatch)
            {
                // The session belongs to a volume that was re-created or restored: learn the new binding (which expires it).
                await RefreshBindingAsync(scope).ConfigureAwait(false);
                ExpireSession(session, ex.Message);
            }
        }
    }

    /// <summary>Reserves one application-visible operation id (never acknowledged by this client, FSD §13).</summary>
    public async Task<DfsOperationId> ReserveApplicationIdAsync(OperationScope scope)
    {
        for (int attempt = 0; ; attempt++)
        {
            VolumeSession session = await EnsureSessionAsync(scope).ConfigureAwait(false);
            try
            {
                ReserveOperationIdsResultMessage block = await ReserveAsync(session, 1, scope).ConfigureAwait(false);
                if (block.Count < 1)
                    throw new DfsProtocolException("ReserveOperationIds returned an empty block.");
                if (block.Session == session.Id)
                    session.AddApplicationVisible(block.FirstSequence);
                return new DfsOperationId(block.Session, block.FirstSequence);
            }
            catch (DfsException ex) when (attempt < 2 && ex.ErrorCode is DfsErrorCode.LeaseLost or DfsErrorCode.OperationExpired)
            {
                ExpireSession(session, ex.Message);
            }
            catch (DfsException ex) when (attempt < 2 && ex.ErrorCode is DfsErrorCode.EpochMismatch)
            {
                await RefreshBindingAsync(scope).ConfigureAwait(false);
                ExpireSession(session, ex.Message);
            }
        }
    }

    private async Task<ReserveOperationIdsResultMessage> ReserveAsync(VolumeSession session, int count, OperationScope scope)
    {
        var request = new RpcRequest
        {
            Name = "ReserveOperationIds",
            ReplyType = MessageType.ReserveOperationIdsResult,
            Kind = CallKind.Idempotent,
            Router = this,
            Build = _ => ValueTask.FromResult<IWireMessage>(new ReserveOperationIdsMessage(Header(session), count)),
        };
        return (ReserveOperationIdsResultMessage)await Runtime.Rpc.InvokeAsync(request, scope).ConfigureAwait(false);
    }

    /// <summary>Acquires <paramref name="gate"/> for <paramref name="scope"/>; an expired or cancelled wait is the scope's classified failure.</summary>
    private static async ValueTask<AsyncLock.Releaser> LockAsync(AsyncLock gate, OperationScope scope)
    {
        try
        {
            return await gate.AcquireAsync(scope.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (scope.Token.IsCancellationRequested)
        {
            throw scope.NotTransmittedFailure(null);
        }
    }

    // ------------------------------------------------------------------------------------------------ requests

    /// <summary>
    /// Executes a read-only (or naturally idempotent) volume request routed to the leader (or the backup host). The
    /// session is attached when one is open (or opened when <paramref name="requireSession"/>).
    /// </summary>
    /// <param name="scope">The operation.</param>
    /// <param name="name">Diagnostic name.</param>
    /// <param name="build">Builds the request for a header.</param>
    /// <param name="path">Path for exceptions.</param>
    /// <param name="transaction">Transaction of transaction-bound requests.</param>
    /// <param name="requireSession">Open a session when none is open (the request creates or uses something session-owned).</param>
    /// <param name="attemptTimeout">Per-attempt reply timeout (null derives one from the operation timeout).</param>
    /// <param name="binding">
    /// The volume binding of what the request refers to (a handle, pin, staging session or transaction); null for requests
    /// that refer to nothing session-owned. A bound request is never moved to a re-created volume: <c>EpochMismatch</c> is
    /// then a definite failure.
    /// </param>
    /// <param name="sessionOnly">
    /// The request refers to nothing the session owns (it only needs <em>a</em> session): a <c>LeaseLost</c> reply then
    /// means the session expired, and the request is retried under a new one. Otherwise <c>LeaseLost</c> concerns the
    /// referenced handle, pin or staging session alone and is reported (the session and its other handles are unaffected).
    /// </param>
    public async Task<TReply> ReadAsync<TReply>(OperationScope scope, string name, Func<RequestHeader, IWireMessage> build, string? path = null,
        DfsTransactionId? transaction = null, bool requireSession = false, TimeSpan? attemptTimeout = null, VolumeBinding? binding = null,
        bool sessionOnly = false)
        where TReply : IWireMessage<TReply>
    {
        if (requireSession)
            await EnsureSessionAsync(scope).ConfigureAwait(false);
        var request = new RpcRequest
        {
            Name = name,
            ReplyType = TReply.WireType,
            Kind = CallKind.Read,
            Router = this,
            Path = path,
            Transaction = transaction,
            AttemptTimeout = attemptTimeout,
            Build = async s =>
            {
                VolumeSession? session = requireSession ? await EnsureSessionAsync(s).ConfigureAwait(false) : CurrentSession;
                return build(Header(session, default, transaction, binding));
            },
            OnError = (error, s, transmissions) => OnReadErrorAsync(error, s, requireSession && sessionOnly && binding is null, binding is not null),
        };
        return (TReply)await Runtime.Rpc.InvokeAsync(request, scope).ConfigureAwait(false);
    }

    /// <summary>A reply together with the volume binding and session of the request that produced it.</summary>
    /// <typeparam name="TReply">Reply type.</typeparam>
    /// <param name="Reply">The success reply.</param>
    /// <param name="Binding">The binding the request was addressed to (what it created belongs to that volume).</param>
    /// <param name="Session">The session the request carried (it owns what the request created).</param>
    public readonly record struct Bound<TReply>(TReply Reply, VolumeBinding Binding, VolumeSession? Session);

    /// <summary>
    /// Executes a mutation with an operation id (the application's, or one allocated here) under the full outcome
    /// discipline: retries reuse the id, an uncertain outcome is resolved with <c>QueryOperation(resolve)</c> or reported
    /// as <see cref="DfsCommitOutcomeUnknownException"/>.
    /// </summary>
    /// <param name="scope">The operation.</param>
    /// <param name="name">Diagnostic name.</param>
    /// <param name="applicationId">The application's operation id, or null to allocate one.</param>
    /// <param name="build">Builds the request for a header.</param>
    /// <param name="path">Path for exceptions.</param>
    /// <param name="transaction">Transaction of transaction-bound requests.</param>
    /// <param name="rebindable">
    /// True when the request does not refer to handles, pins or staging of the session (it may then move to a new session
    /// or a re-created volume when its first transmission is rejected for that reason).
    /// </param>
    /// <param name="queryTarget">Outcome query target (default: the operation id itself).</param>
    /// <param name="queryAsTransaction">Query the target as a transaction (TxCommit).</param>
    /// <param name="binding">
    /// The volume binding of what the request refers to (handle, staging session, transaction); null to address the volume
    /// the operation id's session belongs to. Every transmission of one operation id carries the same binding, so a retry
    /// can never execute against a re-created volume.
    /// </param>
    /// <param name="keepProtected">
    /// The id becomes visible to the application (a transaction id): it is never acknowledged, so its outcome stays
    /// queryable (PROTOCOL.md §5.1).
    /// </param>
    public async Task<TReply> MutateAsync<TReply>(OperationScope scope, string name, DfsOperationId? applicationId,
        Func<RequestHeader, IWireMessage> build, string? path = null, DfsTransactionId? transaction = null, bool rebindable = true,
        DfsOperationId? queryTarget = null, bool queryAsTransaction = false, VolumeBinding? binding = null, bool keepProtected = false)
        where TReply : IWireMessage<TReply>
    {
        Bound<TReply> bound = await MutateBoundAsync<TReply>(scope, name, applicationId, build, path, transaction, rebindable, queryTarget, queryAsTransaction,
            binding, keepProtected).ConfigureAwait(false);
        return bound.Reply;
    }

    /// <summary>
    /// <see cref="MutateAsync{TReply}"/> that also reports the binding and session of the request (they own what it
    /// created: a handle, a staging session, a transaction, a reader pin).
    /// </summary>
    public async Task<Bound<TReply>> MutateBoundAsync<TReply>(OperationScope scope, string name, DfsOperationId? applicationId,
        Func<RequestHeader, IWireMessage> build, string? path = null, DfsTransactionId? transaction = null, bool rebindable = true,
        DfsOperationId? queryTarget = null, bool queryAsTransaction = false, VolumeBinding? binding = null, bool keepProtected = false)
        where TReply : IWireMessage<TReply>
    {
        VolumeSession session = await EnsureSessionAsync(scope).ConfigureAwait(false);
        bool sdkAllocated = applicationId is null;
        DfsOperationId operation;

        // The binding every transmission of the id carries: fixed by the caller, else the binding of the session the
        // SDK allocated the id from, else (application ids) the binding at the first transmission.
        VolumeBinding? pinned = binding;
        if (applicationId is { } supplied)
        {
            operation = supplied;
            session.Protect(supplied);
        }
        else
        {
            (operation, VolumeSession owner) = await AllocateWithSessionAsync(scope).ConfigureAwait(false);
            pinned ??= owner.Binding;
            if (keepProtected)
                owner.Protect(operation);
        }

        VolumeSession? sent = null;
        RpcRequest request = null!;
        request = new RpcRequest
        {
            Name = name,
            ReplyType = TReply.WireType,
            Kind = CallKind.Mutation,
            Router = this,
            Path = path,
            Transaction = transaction,
            Operation = operation,
            Build = async s =>
            {
                VolumeSession current = await EnsureSessionAsync(s).ConfigureAwait(false);
                pinned ??= current.Binding;
                sent = current;
                return build(Header(current, request.Operation.GetValueOrDefault(), transaction, pinned));
            },
            Resolve = s => ResolveOutcomeAsync(request, s, queryTarget ?? request.Operation.GetValueOrDefault(), queryAsTransaction, pinned),
            OnError = (error, s, transmissions) => OnMutationErrorAsync(request, error, s, transmissions, sdkAllocated,
                rebindable && transaction is null && binding is null, async replaceScope =>
                {
                    // A definitively rejected SDK id is replaced by a fresh one of the current session (and binding).
                    (DfsOperationId fresh, VolumeSession owner) = await AllocateWithSessionAsync(replaceScope).ConfigureAwait(false);
                    request.ReplaceOperation(fresh);
                    pinned = binding ?? owner.Binding;
                    if (keepProtected)
                        owner.Protect(fresh);
                }),
        };

        try
        {
            IWireMessage reply = await Runtime.Rpc.InvokeAsync(request, scope).ConfigureAwait(false);
            Finish(request, sdkAllocated, keepProtected);
            return new Bound<TReply>((TReply)reply, pinned ?? default, sent);
        }
        catch (DfsCommitOutcomeUnknownException)
        {
            // The id stays protected: the application may resolve it later.
            throw;
        }
        catch (OperationCanceledException) when (Runtime.Mutations.TreatUnknownAsAborted && request.Transmissions > 0)
        {
            throw;
        }
        catch (Exception)
        {
            Finish(request, sdkAllocated, keepProtected);
            throw;
        }
    }

    /// <summary>The request ended definitely: its id is acknowledged (or reused when it was never transmitted).</summary>
    private void Finish(RpcRequest request, bool sdkAllocated, bool keepProtected)
    {
        if (request.Operation is not { } id)
            return;
        VolumeSession? session;
        lock (_sync)
            session = _session;
        if (session is null)
            return;
        if (sdkAllocated && request.Transmissions == 0)
        {
            if (keepProtected)
                session.Unprotect(id);
            session.Release(id);
        }
        else if (!keepProtected)
        {
            session.Complete(id);
        }
    }

    /// <summary>
    /// Error replies to a mutation that need more than the generic handling (PROTOCOL.md §5.1). <c>EpochMismatch</c>,
    /// <c>OperationExpired</c> and <c>LeaseLost</c> reject a transmission without executing it — but after more than one
    /// transmission an earlier one may have committed before the rejection became possible (the outcome was retired, the
    /// session expired, the volume was re-created), so the outcome is then unknown. After a single transmission an
    /// SDK-allocated id moves to a fresh id (a new session, or the re-created volume for requests that refer to nothing
    /// session-owned); otherwise the rejection is the definite answer.
    /// </summary>
    private async ValueTask<bool> OnMutationErrorAsync(RpcRequest request, ErrorReplyMessage error, OperationScope scope, int transmissions, bool sdkAllocated,
        bool rebindable, Func<OperationScope, Task> replace)
    {
        if (error.Code is not (DfsErrorCode.EpochMismatch or DfsErrorCode.OperationExpired or DfsErrorCode.LeaseLost))
            return false;
        if (transmissions > 1)
            throw Runtime.Rpc.CreateUnknown(request, error.ToException(request.Path));
        if (!sdkAllocated)
            return false;
        switch (error.Code)
        {
            case DfsErrorCode.EpochMismatch:
                // The name designates another volume now (re-created or restored).
                await RefreshBindingAsync(scope).ConfigureAwait(false);
                if (!rebindable)
                    return false;
                await replace(scope).ConfigureAwait(false);
                return true;

            case DfsErrorCode.LeaseLost when !rebindable:
                // The referenced handle, staging session or transaction is gone: definite.
                return false;

            default:
                // OperationExpired, or LeaseLost of the session itself: continue under a new session.
                if (CurrentSession is { } session && request.Operation is { } old && old.Session == session.Id)
                    ExpireSession(session, error.Message);
                await replace(scope).ConfigureAwait(false);
                return true;
        }
    }

    private async ValueTask<bool> OnReadErrorAsync(ErrorReplyMessage error, OperationScope scope, bool retryOnSessionLoss, bool bound)
    {
        switch (error.Code)
        {
            case DfsErrorCode.EpochMismatch when bound:
                // The handle, pin, staging session or transaction belongs to a volume that no longer holds the name.
                return false;
            case DfsErrorCode.EpochMismatch:
                await RefreshBindingAsync(scope).ConfigureAwait(false);
                return true;
            case DfsErrorCode.LeaseLost when retryOnSessionLoss:
                if (CurrentSession is { } session)
                {
                    ExpireSession(session, error.Message);
                    return true;
                }

                return false;
            default:
                return false;
        }
    }

    private async ValueTask<bool> RebindOnEpochMismatchAsync(ErrorReplyMessage error, OperationScope scope, int transmissions)
    {
        if (error.Code != DfsErrorCode.EpochMismatch)
            return false;
        await RefreshBindingAsync(scope).ConfigureAwait(false);
        return true;
    }

    /// <summary>Re-learns the volume's identity after an <c>EpochMismatch</c> (the route reply rebinds).</summary>
    public async Task RefreshBindingAsync(OperationScope scope)
    {
        InvalidateRoute();
        lock (_sync)
        {
            // Accept whatever identity the cluster reports now.
            _bound = false;
        }

        VolumeId oldId;
        ulong oldEpoch;
        lock (_sync)
        {
            oldId = _volumeId;
            oldEpoch = _epoch;
        }

        await RefreshRouteAsync(scope, Runtime.Rpc.DefaultAttemptTimeout(scope)).ConfigureAwait(false);
        bool changed;
        lock (_sync)
        {
            if (!_bound && _entry is not null)
            {
                _bound = true;
                _volumeId = _entry.Id;
                _epoch = _entry.Epoch;
            }

            changed = _volumeId != oldId || _epoch != oldEpoch;
        }

        if (changed)
        {
            List<ILeaseHolder> lostHandles = [];
            List<IPinHolder> lostPins = [];
            lock (_sync)
            {
                _session?.MarkExpired();
                _session = null;
                _helperStaging = null;
                CollectAll(lostHandles, lostPins);
            }

            var reason = new DfsException(DfsErrorCode.EpochMismatch,
                $"Volume '{Name}' was re-created or restored; handles and pins of the previous volume are no longer valid.");
            Notify(lostHandles, lostPins, reason, DfsErrors.ToException(DfsErrorCode.PinExpired, reason.Message));
        }
    }

    /// <summary>
    /// Resolves the outcome of a possibly transmitted mutation: <c>QueryOperation(resolve: true)</c> fences an id without
    /// outcome so the answer is definite. Committed → the original success reply; aborted → the definite failure;
    /// otherwise null (the outcome stays unknown).
    /// </summary>
    private async Task<IWireMessage?> ResolveOutcomeAsync(RpcRequest request, OperationScope scope, DfsOperationId target, bool isTransaction,
        VolumeBinding? binding)
    {
        // Asked of the volume the request was addressed to: a re-created volume must never fence (and so "abort") an id
        // that may have committed in its predecessor.
        QueryOperationResultMessage result = await QueryAsync(target, isTransaction, resolve: true, scope, binding).ConfigureAwait(false);
        switch (result.Status)
        {
            case WireOutcomeStatus.Committed:
                if (!result.ResultFrame.IsEmpty)
                {
                    IWireMessage original = MessageCodec.Decode(result.ResultFrame.Span);
                    if (original.Type == request.ReplyType)
                        return original;
                    if (original is ErrorReplyMessage failed)
                        throw new ResolvedFailureException(failed.ToException(request.Path));
                }

                if (request.ReplyType == MessageType.TxCommitResult)
                    return new TxCommitResultMessage(result.Token, null);
                return null;

            case WireOutcomeStatus.Aborted when result.Error is DfsErrorCode.Cancelled or DfsErrorCode.None:
                throw new ResolvedFailureException(new DfsUnavailableException(DfsErrorCode.Unavailable,
                    $"{request.Name} ({target}) did not take effect: it was not applied before its deadline and has been fenced. {result.Detail}"));

            case WireOutcomeStatus.Aborted:
                throw new ResolvedFailureException(DfsErrors.ToException(result.Error, result.Detail ?? $"{request.Name} failed.", request.Path, target));

            default:
                return null;
        }
    }

    /// <summary>Sends <c>QueryOperation</c> for <paramref name="target"/> (to <paramref name="binding"/>'s volume when given).</summary>
    public async Task<QueryOperationResultMessage> QueryAsync(DfsOperationId target, bool isTransaction, bool resolve, OperationScope scope,
        VolumeBinding? binding = null)
    {
        var request = new RpcRequest
        {
            Name = "QueryOperation",
            ReplyType = MessageType.QueryOperationResult,
            Kind = CallKind.Idempotent,
            Router = this,
            Build = _ => ValueTask.FromResult<IWireMessage>(new QueryOperationMessage(Header(CurrentSession, default, null, binding), target, isTransaction,
                resolve)),
        };
        return (QueryOperationResultMessage)await Runtime.Rpc.InvokeAsync(request, scope).ConfigureAwait(false);
    }

    /// <summary>
    /// Closes a handle (idempotent; releases sharing, writer authority, pin and bound staging). The release is a mutation
    /// of its own: it carries an operation id from its first transmission (OUT-001), so a lost reply is answered from the
    /// stored outcome.
    /// </summary>
    public async Task<CloseHandleResultMessage> CloseHandleAsync(HandleId handle, ulong generation, DfsTransactionId? transaction, OperationScope scope,
        VolumeBinding? binding)
    {
        return await MutateAsync<CloseHandleResultMessage>(scope, "CloseHandle", null,
            header => new CloseHandleMessage(header, handle, generation), null, transaction, rebindable: false, binding: binding).ConfigureAwait(false);
    }

    // ------------------------------------------------------------------------------------------------ staging

    /// <summary>
    /// The unbound staging session of this client's session used by whole-file helpers (opened on first use; the
    /// capability is renewed before it expires).
    /// </summary>
    public async Task<StagingLease> GetHelperStagingAsync(OperationScope scope)
    {
        using AsyncLock.Releaser held = await LockAsync(_stagingLock, scope).ConfigureAwait(false);
        VolumeSession session = await EnsureSessionAsync(scope).ConfigureAwait(false);
        StagingLease? lease;
        lock (_sync)
            lease = _helperStaging is { } existing && existing.SessionId == session.Id ? existing : null;
        if (lease is not null)
        {
            if (lease.NeedsRenewal(Runtime.Time.GetUtcNow()))
                await RenewStagingAsync(lease, scope).ConfigureAwait(false);
            return lease;
        }

        Bound<CreateStagingResultMessage> bound = await MutateBoundAsync<CreateStagingResultMessage>(scope, "CreateStaging", null,
            header => new CreateStagingMessage(header, default, default)).ConfigureAwait(false);
        CreateStagingResultMessage created = bound.Reply;

        // The staging session belongs to the client session the creating request carried (a new one when the old expired).
        ulong owner = bound.Session?.Id ?? session.Id;
        lease = new StagingLease(created.Staging, default, null, owner, created.Capability, created.CapabilityExpiry, created.PlacementEpoch, bound.Binding);
        lock (_sync)
        {
            if (CurrentSession is { } current && current.Id == owner)
                _helperStaging = lease;
        }

        return lease;
    }

    /// <summary>
    /// Opens a fresh unbound staging session for one whole-file helper call. The publication that references it closes it
    /// (the staging-to-live transition); a helper that does not publish closes it with <see cref="CloseUnboundStagingAsync"/>,
    /// so nothing a finished or abandoned helper staged stays protected from GC (FILE-016).
    /// </summary>
    public async Task<StagingLease> CreateUnboundStagingAsync(OperationScope scope)
    {
        VolumeSession session = await EnsureSessionAsync(scope).ConfigureAwait(false);
        Bound<CreateStagingResultMessage> bound;
        try
        {
            bound = await MutateBoundAsync<CreateStagingResultMessage>(scope, "CreateStaging", null,
                header => new CreateStagingMessage(header, default, default)).ConfigureAwait(false);
        }
        catch (DfsCommitOutcomeUnknownException unknown)
        {
            throw StagingNotCreated(scope, unknown);
        }

        CreateStagingResultMessage created = bound.Reply;
        ulong owner = bound.Session?.Id ?? session.Id;
        return new StagingLease(created.Staging, default, null, owner, created.Capability, created.CapabilityExpiry, created.PlacementEpoch, bound.Binding);
    }

    /// <summary>
    /// Best-effort close of an unbound staging session a helper will not publish (cancelled or failed): runs on its own
    /// short background scope (the caller's token may be cancelled) and never throws. A closed session is the authoritative
    /// "no further publication" decision: an uncertain publication that still arrives fails without effect.
    /// </summary>
    public async Task CloseUnboundStagingAsync(StagingLease lease)
    {
        try
        {
            using OperationScope scope = Runtime.BeginBackground(DfsOperationKind.CreateStaging, Name, Runtime.Options.ShutdownTimeout);
            await MutateAsync<CreateStagingResultMessage>(scope, "CreateStaging(close)", null,
                header => CreateStagingMessage.CloseRequest(header, lease.Id), rebindable: false, binding: lease.Binding).ConfigureAwait(false);
        }
#pragma warning disable CA1031 // Cleanup is best effort: the session also closes with its client session.
        catch (Exception ex)
#pragma warning restore CA1031
        {
            Runtime.Emit("staging.close-failed", $"Closing staging session {lease.Id} failed ({ex.GetType().Name}); it closes with its session.", volume: Name,
                exception: ex);
        }
    }

    /// <summary>Creates a staging session bound to a writable handle (of <paramref name="binding"/>'s volume).</summary>
    public async Task<StagingLease> CreateHandleStagingAsync(HandleId handle, OperationScope scope, VolumeBinding? binding)
    {
        VolumeSession session = await EnsureSessionAsync(scope).ConfigureAwait(false);
        Bound<CreateStagingResultMessage> bound;
        try
        {
            bound = await MutateBoundAsync<CreateStagingResultMessage>(scope, "CreateStaging", null,
                header => new CreateStagingMessage(header, handle, default), rebindable: false, binding: binding).ConfigureAwait(false);
        }
        catch (DfsCommitOutcomeUnknownException unknown) when (!scope.IsAbortedByCaller && unknown.InnerException is DfsLeaseLostException lost)
        {
            // A retry was answered that the handle is no longer active: whether an earlier transmission created a session or
            // not, the handle's writer authority is gone, so nothing can be staged or published through it any more.
            throw new DfsLeaseLostException("A staging session could not be created: " + lost.Message, lost);
        }
        catch (DfsCommitOutcomeUnknownException unknown)
        {
            throw StagingNotCreated(scope, unknown);
        }

        CreateStagingResultMessage created = bound.Reply;
        return new StagingLease(created.Staging, handle, null, bound.Session?.Id ?? session.Id, created.Capability, created.CapabilityExpiry,
            created.PlacementEpoch, bound.Binding);
    }

    /// <summary>
    /// A staging session has no namespace effect, so an unanswered <c>CreateStaging</c> is not an unknown outcome of the
    /// public operation (whose own request was never sent): it is the caller's cancellation (or the client's disposal), or
    /// an availability failure. A session it may still have created holds nothing and closes with its client session.
    /// </summary>
    private static Exception StagingNotCreated(OperationScope scope, DfsCommitOutcomeUnknownException unknown)
        => scope.IsAbortedByCaller
            ? scope.NotTransmittedFailure(unknown.InnerException ?? unknown)
            : unknown.InnerException is DfsQuorumUnavailableException quorum
                ? new DfsQuorumUnavailableException("A staging session could not be created: " + quorum.Message, quorum)
                : new DfsUnavailableException(DfsErrorCode.Unavailable, "A staging session could not be created: the request was not answered.",
                    unknown.InnerException ?? unknown);

    /// <summary>Renews the capability of <paramref name="lease"/> (<c>CreateStaging</c> with <c>Renew</c>; no log entry).</summary>
    public async Task RenewStagingAsync(StagingLease lease, OperationScope scope)
    {
        CreateStagingResultMessage renewed = await ReadAsync<CreateStagingResultMessage>(scope, "CreateStaging(renew)",
            header => new CreateStagingMessage(header, lease.Handle, lease.Id), transaction: lease.Transaction, requireSession: true,
            binding: lease.Binding).ConfigureAwait(false);
        lease.Update(renewed.Capability, renewed.CapabilityExpiry, renewed.PlacementEpoch);
    }

    /// <summary>Forgets the helper staging session (it was closed or rejected).</summary>
    public void DropHelperStaging(StagingLease lease)
    {
        lock (_sync)
        {
            if (ReferenceEquals(_helperStaging, lease))
                _helperStaging = null;
        }
    }

    // ------------------------------------------------------------------------------------------------ leases and pins

    /// <summary>Registers an open handle for lease renewal (weakly: an abandoned stream lets its lease expire).</summary>
    public void RegisterHandle(HandleId handle, VolumeSession session, ILeaseHolder holder)
    {
        lock (_sync)
            _handles[handle.Value] = new Registration<ILeaseHolder>(new WeakReference<ILeaseHolder>(holder), session.Id);
        EnsureKeepAlive(session);
    }

    /// <summary>Stops renewing a handle (closed or lost).</summary>
    public void UnregisterHandle(HandleId handle)
    {
        lock (_sync)
        {
            _handles.Remove(handle.Value);
            _pinUpdates.Remove(handle.Value);
        }
    }

    /// <summary>Reports the newest revision a live-read handle observed (sent with the next keep-alive so GC can advance).</summary>
    public void ReportPinAdvance(HandleId handle, ulong pinIndex)
    {
        lock (_sync)
        {
            if (!_handles.ContainsKey(handle.Value))
                return;
            if (!_pinUpdates.TryGetValue(handle.Value, out ulong current) || pinIndex > current)
                _pinUpdates[handle.Value] = pinIndex;
        }
    }

    /// <summary>Registers a reader pin (snapshot view, enumeration) for renewal.</summary>
    public void RegisterPin(ulong pin, VolumeSession? session, IPinHolder holder)
    {
        if (pin == 0)
            return;
        lock (_sync)
            _pins[pin] = new Registration<IPinHolder>(new WeakReference<IPinHolder>(holder), session?.Id ?? 0);
        if (session is not null)
            EnsureKeepAlive(session);
    }

    /// <summary>Stops renewing a reader pin.</summary>
    public void UnregisterPin(ulong pin)
    {
        lock (_sync)
            _pins.Remove(pin);
    }

    private void CollectAll(List<ILeaseHolder> handles, List<IPinHolder> pins)
    {
        foreach (Registration<ILeaseHolder> registration in _handles.Values)
        {
            if (registration.Holder.TryGetTarget(out ILeaseHolder? holder))
                handles.Add(holder);
        }

        foreach (Registration<IPinHolder> registration in _pins.Values)
        {
            if (registration.Holder.TryGetTarget(out IPinHolder? holder))
                pins.Add(holder);
        }

        _handles.Clear();
        _pins.Clear();
        _pinUpdates.Clear();
    }

    private void CollectSession(ulong sessionId, List<ILeaseHolder> handles, List<IPinHolder> pins)
    {
        foreach ((ulong id, Registration<ILeaseHolder> registration) in _handles.ToArray())
        {
            if (registration.Session != sessionId)
                continue;
            _handles.Remove(id);
            _pinUpdates.Remove(id);
            if (registration.Holder.TryGetTarget(out ILeaseHolder? holder))
                handles.Add(holder);
        }

        foreach ((ulong id, Registration<IPinHolder> registration) in _pins.ToArray())
        {
            if (registration.Session != sessionId)
                continue;
            _pins.Remove(id);
            if (registration.Holder.TryGetTarget(out IPinHolder? holder))
                pins.Add(holder);
        }
    }

    private void Notify(List<ILeaseHolder> handles, List<IPinHolder> pins, Exception handleReason, Exception pinReason)
    {
        foreach (ILeaseHolder holder in handles)
            SafeNotify(() => holder.OnLeaseLost(handleReason));
        foreach (IPinHolder holder in pins)
            SafeNotify(() => holder.OnPinExpired(pinReason));
    }

    private void SafeNotify(Action notify)
    {
        try
        {
            notify();
        }
#pragma warning disable CA1031 // A holder's fault handler must not break the keep-alive loop.
        catch (Exception ex)
#pragma warning restore CA1031
        {
            Runtime.Emit("session.notify-failed", "A lease holder failed to process a lease loss.", volume: Name, exception: ex);
        }
    }

    // ------------------------------------------------------------------------------------------------ keep-alive

    private void EnsureKeepAlive(VolumeSession session)
    {
        TimeSpan lease = session.LeaseDuration > TimeSpan.Zero ? session.LeaseDuration : Runtime.Capabilities.LeaseDuration;
        TimeSpan timeout = session.SessionTimeout > TimeSpan.Zero ? session.SessionTimeout : lease;
        TimeSpan basis = lease < timeout ? lease : timeout;
        TimeSpan period = TimeSpan.FromTicks(Math.Max(basis.Ticks / 3, TimeSpan.FromMilliseconds(50).Ticks));
        lock (_sync)
        {
            if (_stopped)
                return;
            if (_timer is not null && _timerPeriod == period)
                return;
            _timerPeriod = period;
            if (_timer is null)
                _timer = Runtime.Time.CreateTimer(static s => ((VolumeContext)s!).OnTimer(), this, period, period);
            else
                _timer.Change(period, period);
        }
    }

    private void OnTimer()
    {
        if (Interlocked.Exchange(ref _keepAliveRunning, 1) != 0)
            return;
        Task run = KeepAliveAsync();
        if (!run.IsCompleted)
            Waits.ObserveQuietly(run);
    }

    private async Task KeepAliveAsync()
    {
        try
        {
            VolumeSession? session;
            HandleId[] handles;
            HandlePinUpdate[] updates;
            ulong[] pins;
            lock (_sync)
            {
                if (_stopped)
                    return;
                session = _session is { IsExpired: false } s ? s : null;
                if (session is null)
                    return;
                PruneCollected();
                handles = _handles.Where(kv => kv.Value.Session == session.Id).Select(kv => new HandleId(kv.Key)).ToArray();
                updates = _pinUpdates.Where(kv => _handles.ContainsKey(kv.Key)).Select(kv => new HandlePinUpdate(new HandleId(kv.Key), kv.Value)).ToArray();
                pins = _pins.Where(kv => kv.Value.Session == session.Id).Select(kv => kv.Key).ToArray();
            }

            KeepAliveResultMessage result;
            using (OperationScope scope = Runtime.BeginBackground(DfsOperationKind.KeepAlive, Name, KeepAliveTimeout()))
            {
                var request = new RpcRequest
                {
                    Name = "KeepAlive",
                    ReplyType = MessageType.KeepAliveResult,
                    Kind = CallKind.Idempotent,
                    Router = this,

                    // Several attempts fit in one renewal: a leader that stopped answering (paused, partitioned) is left
                    // for its successor well within the lease, instead of spending the whole renewal waiting on it.
                    AttemptTimeout = KeepAliveAttemptTimeout(),
                    Build = _ => ValueTask.FromResult<IWireMessage>(new KeepAliveMessage(Header(session), handles, updates, pins)),
                };
                try
                {
                    result = (KeepAliveResultMessage)await Runtime.Rpc.InvokeAsync(request, scope).ConfigureAwait(false);
                }
                catch (DfsException ex) when (ex.ErrorCode == DfsErrorCode.EpochMismatch)
                {
                    // The session's volume was re-created or restored: learn the new binding now, which faults the old
                    // volume's streams and pins instead of letting them believe their leases are being renewed.
                    await RefreshBindingAsync(scope).ConfigureAwait(false);
                    ExpireSession(session, ex.Message);
                    return;
                }
            }

            Process(session, result, updates);
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            // A missed renewal is retried on the next tick; the leader expires leases only after LeaseDuration.
            if (!Runtime.IsDisposed)
                Runtime.Emit("session.keepalive-failed", $"Keep-alive on volume '{Name}' failed: {ex.GetType().Name}.", volume: Name, exception: ex);
        }
        finally
        {
            Volatile.Write(ref _keepAliveRunning, 0);
        }
    }

    /// <summary>
    /// Half a renewal period (at least 50 ms), so that several attempts fit in one renewal; but never below three round trips
    /// of the current leader's connection: a leader that answers slowly (high-latency link) is still renewed with, while a
    /// silent one (paused, partitioned) is left for its successor within the lease.
    /// </summary>
    private TimeSpan KeepAliveAttemptTimeout()
    {
        TimeSpan period;
        string? leader;
        lock (_sync)
        {
            period = _timerPeriod;
            leader = _leader ?? _lastLeader;
        }

        TimeSpan half = period / 2;
        TimeSpan attempt = half > TimeSpan.FromMilliseconds(50) ? half : TimeSpan.FromMilliseconds(50);
        if (leader is not null && Runtime.Pool.TryGetOpen(leader, out ClientConnection connection) && connection.RoundTrip * 3 > attempt)
            attempt = connection.RoundTrip * 3;
        return attempt;
    }

    private TimeSpan KeepAliveTimeout()
    {
        TimeSpan period;
        lock (_sync)
            period = _timerPeriod;
        TimeSpan max = Runtime.Options.OperationTimeout;
        TimeSpan budget = period * 2;
        return budget < max ? budget : max;
    }

    private void Process(VolumeSession session, KeepAliveResultMessage result, HandlePinUpdate[] sentUpdates)
    {
        if (result.SessionExpired)
        {
            ExpireSession(session, "the leader no longer renews it (session timeout elapsed)");
            return;
        }

        List<ILeaseHolder> lostHandles = [];
        List<IPinHolder> lostPins = [];
        lock (_sync)
        {
            foreach (HandlePinUpdate sent in sentUpdates)
            {
                if (_pinUpdates.TryGetValue(sent.Handle.Value, out ulong pending) && pending == sent.PinIndex)
                    _pinUpdates.Remove(sent.Handle.Value);
            }

            foreach (HandleId handle in result.ExpiredHandles)
            {
                if (_handles.Remove(handle.Value, out Registration<ILeaseHolder> registration) && registration.Holder.TryGetTarget(out ILeaseHolder? holder))
                    lostHandles.Add(holder);
                _pinUpdates.Remove(handle.Value);
            }

            foreach (ulong pin in result.ExpiredPins)
            {
                if (_pins.Remove(pin, out Registration<IPinHolder> registration) && registration.Holder.TryGetTarget(out IPinHolder? holder))
                    lostPins.Add(holder);
            }
        }

        if (lostHandles.Count > 0 || lostPins.Count > 0)
        {
            Notify(lostHandles, lostPins,
                DfsErrors.ToException(DfsErrorCode.LeaseLost, $"The handle's lease on volume '{Name}' expired."),
                DfsErrors.ToException(DfsErrorCode.PinExpired, $"The reader pin on volume '{Name}' expired."));
        }
    }

    private void PruneCollected()
    {
        foreach ((ulong id, Registration<ILeaseHolder> registration) in _handles.ToArray())
        {
            if (!registration.Holder.TryGetTarget(out _))
            {
                _handles.Remove(id);
                _pinUpdates.Remove(id);
            }
        }

        foreach ((ulong id, Registration<IPinHolder> registration) in _pins.ToArray())
        {
            if (!registration.Holder.TryGetTarget(out _))
                _pins.Remove(id);
        }
    }

    /// <summary>Stops the keep-alive loop (client shutdown or abandonment); sends nothing.</summary>
    public void Stop()
    {
        ITimer? timer;
        lock (_sync)
        {
            _stopped = true;
            timer = _timer;
            _timer = null;
        }

        timer?.Dispose();
    }

    private readonly record struct Registration<T>(WeakReference<T> Holder, ulong Session)
        where T : class;
}
