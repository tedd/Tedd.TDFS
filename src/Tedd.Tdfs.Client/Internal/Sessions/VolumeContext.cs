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
    /// binding and returns the leader endpoint, or null when no node currently knows a leader.
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
        Exception? last = null;
        for (int i = 0; i < ordered.Count; i++)
        {
            if (scope.Token.IsCancellationRequested)
                break;
            string endpoint = ordered[i];
            IWireMessage reply;
            try
            {
                TimeSpan timeout = attemptTimeout < scope.Remaining ? attemptTimeout : scope.Remaining;
                reply = await Runtime.Rpc.SendOnceAsync(endpoint, new GetVolumeRouteMessage(Name), scope, ClientRuntime.Positive(timeout)).ConfigureAwait(false);
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
                            _suspects.Remove(leader);
                        }

                        return leader;
                    }

                    // This node does not host the group: ask the voters it names.
                    int before = ordered.Count;
                    var extra = new List<string>();
                    EnqueueVoters(extra, tried, route.Volume);
                    ordered.InsertRange(i + 1, extra);
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

        if (last is not null)
            Runtime.Emit("volume.route-unknown", $"No leader of volume '{Name}' is currently known ({last.GetType().Name}).", volume: Name, exception: last);
        return null;
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

    /// <summary>Builds a request header for the current binding.</summary>
    public RequestHeader Header(VolumeSession? session, DfsOperationId operation = default, DfsTransactionId? transaction = null)
    {
        lock (_sync)
        {
            VolumeId expectedId = _bound ? _volumeId : Runtime.Map.Volume(Name)?.Id ?? default;
            ulong expectedEpoch = _bound ? _epoch : Runtime.Map.Volume(Name)?.Epoch ?? 0;
            VolumeSession? s = session is { IsExpired: false } ? session : null;
            return new RequestHeader(Name, expectedId, expectedEpoch, s?.Id ?? 0, operation, s?.Watermark ?? 0, transaction);
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
        var request = new RpcRequest
        {
            Name = "OpenSession",
            ReplyType = MessageType.OpenSessionResult,
            Kind = CallKind.Idempotent,
            Router = this,
            Build = _ => ValueTask.FromResult<IWireMessage>(new OpenSessionMessage(Header(null), Runtime.Options.ClientName, nonce)),
            OnError = RebindOnEpochMismatchAsync,
        };
        var reply = (OpenSessionResultMessage)await Runtime.Rpc.InvokeAsync(request, scope).ConfigureAwait(false);
        VolumeSession session;
        lock (_sync)
        {
            session = new VolumeSession(reply.Session, reply.FirstSequence, reply.LeaseDuration, reply.SessionTimeout, _volumeId, _epoch);
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
    public async ValueTask<DfsOperationId> AllocateAsync(OperationScope scope)
    {
        for (int attempt = 0; ; attempt++)
        {
            VolumeSession session = await EnsureSessionAsync(scope).ConfigureAwait(false);
            if (session.TryAllocate(out DfsOperationId id))
                return id;
            using AsyncLock.Releaser held = await LockAsync(_reserveLock, scope).ConfigureAwait(false);
            if (session.TryAllocate(out id))
                return id;
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
    public async Task<TReply> ReadAsync<TReply>(OperationScope scope, string name, Func<RequestHeader, IWireMessage> build, string? path = null,
        DfsTransactionId? transaction = null, bool requireSession = false, TimeSpan? attemptTimeout = null)
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
                return build(Header(session, default, transaction));
            },
            OnError = (error, s, transmissions) => OnReadErrorAsync(error, s, requireSession),
        };
        return (TReply)await Runtime.Rpc.InvokeAsync(request, scope).ConfigureAwait(false);
    }

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
    public async Task<TReply> MutateAsync<TReply>(OperationScope scope, string name, DfsOperationId? applicationId,
        Func<RequestHeader, IWireMessage> build, string? path = null, DfsTransactionId? transaction = null, bool rebindable = true,
        DfsOperationId? queryTarget = null, bool queryAsTransaction = false)
        where TReply : IWireMessage<TReply>
    {
        VolumeSession session = await EnsureSessionAsync(scope).ConfigureAwait(false);
        bool sdkAllocated = applicationId is null;
        DfsOperationId operation;
        if (applicationId is { } supplied)
        {
            operation = supplied;
            session.Protect(supplied);
        }
        else
        {
            operation = await AllocateAsync(scope).ConfigureAwait(false);
        }

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
                return build(Header(current, request.Operation.GetValueOrDefault(), transaction));
            },
            Resolve = s => ResolveOutcomeAsync(request, s, queryTarget ?? request.Operation.GetValueOrDefault(), queryAsTransaction),
            OnError = (error, s, transmissions) => OnMutationErrorAsync(request, error, s, transmissions, sdkAllocated, rebindable && transaction is null),
        };

        try
        {
            IWireMessage reply = await Runtime.Rpc.InvokeAsync(request, scope).ConfigureAwait(false);
            Finish(request, sdkAllocated, definite: true);
            return (TReply)reply;
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
            Finish(request, sdkAllocated, definite: true);
            throw;
        }
    }

    private void Finish(RpcRequest request, bool sdkAllocated, bool definite)
    {
        if (request.Operation is not { } id || !definite)
            return;
        VolumeSession? session;
        lock (_sync)
            session = _session;
        if (session is null)
            return;
        if (sdkAllocated && request.Transmissions == 0)
            session.Release(id);
        else
            session.Complete(id);
    }

    private async ValueTask<bool> OnMutationErrorAsync(RpcRequest request, ErrorReplyMessage error, OperationScope scope, int transmissions, bool sdkAllocated,
        bool rebindable)
    {
        switch (error.Code)
        {
            case DfsErrorCode.EpochMismatch:
                // Rejected before execution: the name designates another volume now.
                await RefreshBindingAsync(scope).ConfigureAwait(false);
                if (!rebindable || !sdkAllocated || transmissions > 1)
                    return false;
                request.ReplaceOperation(await AllocateAsync(scope).ConfigureAwait(false));
                return true;

            case DfsErrorCode.OperationExpired:
            case DfsErrorCode.LeaseLost when rebindable:
                if (!sdkAllocated)
                    return false;
                if (transmissions > 1)
                {
                    // An earlier transmission of this id may have committed before its outcome was retired.
                    throw Runtime.Rpc.CreateUnknown(request, error.ToException(request.Path));
                }

                if (CurrentSession is { } session && request.Operation is { } old && old.Session == session.Id)
                    ExpireSession(session, error.Message);
                request.ReplaceOperation(await AllocateAsync(scope).ConfigureAwait(false));
                return true;

            default:
                return false;
        }
    }

    private async ValueTask<bool> OnReadErrorAsync(ErrorReplyMessage error, OperationScope scope, bool requireSession)
    {
        switch (error.Code)
        {
            case DfsErrorCode.EpochMismatch:
                await RefreshBindingAsync(scope).ConfigureAwait(false);
                return true;
            case DfsErrorCode.LeaseLost when requireSession:
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
    private async Task<IWireMessage?> ResolveOutcomeAsync(RpcRequest request, OperationScope scope, DfsOperationId target, bool isTransaction)
    {
        QueryOperationResultMessage result = await QueryAsync(target, isTransaction, resolve: true, scope).ConfigureAwait(false);
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

    /// <summary>Sends <c>QueryOperation</c> for <paramref name="target"/>.</summary>
    public async Task<QueryOperationResultMessage> QueryAsync(DfsOperationId target, bool isTransaction, bool resolve, OperationScope scope)
    {
        var request = new RpcRequest
        {
            Name = "QueryOperation",
            ReplyType = MessageType.QueryOperationResult,
            Kind = CallKind.Idempotent,
            Router = this,
            Build = _ => ValueTask.FromResult<IWireMessage>(new QueryOperationMessage(Header(CurrentSession), target, isTransaction, resolve)),
        };
        return (QueryOperationResultMessage)await Runtime.Rpc.InvokeAsync(request, scope).ConfigureAwait(false);
    }

    /// <summary>Closes a handle (idempotent; releases sharing, writer authority, pin and bound staging).</summary>
    public async Task<CloseHandleResultMessage> CloseHandleAsync(HandleId handle, ulong generation, DfsTransactionId? transaction, OperationScope scope)
    {
        var request = new RpcRequest
        {
            Name = "CloseHandle",
            ReplyType = MessageType.CloseHandleResult,
            Kind = CallKind.Idempotent,
            Router = this,
            Transaction = transaction,
            Build = _ => ValueTask.FromResult<IWireMessage>(new CloseHandleMessage(Header(CurrentSession, default, transaction), handle, generation)),
        };
        return (CloseHandleResultMessage)await Runtime.Rpc.InvokeAsync(request, scope).ConfigureAwait(false);
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

        CreateStagingResultMessage created = await MutateAsync<CreateStagingResultMessage>(scope, "CreateStaging", null,
            header => new CreateStagingMessage(header, default, default)).ConfigureAwait(false);
        lease = new StagingLease(created.Staging, default, null, session.Id, created.Capability, created.CapabilityExpiry, created.PlacementEpoch);
        lock (_sync)
        {
            if (CurrentSession is { } current && current.Id == session.Id)
                _helperStaging = lease;
        }

        return lease;
    }

    /// <summary>Creates a staging session bound to a writable handle.</summary>
    public async Task<StagingLease> CreateHandleStagingAsync(HandleId handle, OperationScope scope)
    {
        VolumeSession session = await EnsureSessionAsync(scope).ConfigureAwait(false);
        CreateStagingResultMessage created = await MutateAsync<CreateStagingResultMessage>(scope, "CreateStaging", null,
            header => new CreateStagingMessage(header, handle, default), rebindable: false).ConfigureAwait(false);
        return new StagingLease(created.Staging, handle, null, session.Id, created.Capability, created.CapabilityExpiry, created.PlacementEpoch);
    }

    /// <summary>Renews the capability of <paramref name="lease"/> (<c>CreateStaging</c> with <c>Renew</c>; no log entry).</summary>
    public async Task RenewStagingAsync(StagingLease lease, OperationScope scope)
    {
        CreateStagingResultMessage renewed = await ReadAsync<CreateStagingResultMessage>(scope, "CreateStaging(renew)",
            header => new CreateStagingMessage(header, lease.Handle, lease.Id), transaction: lease.Transaction, requireSession: true).ConfigureAwait(false);
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
                    Build = _ => ValueTask.FromResult<IWireMessage>(new KeepAliveMessage(Header(session), handles, updates, pins)),
                };
                result = (KeepAliveResultMessage)await Runtime.Rpc.InvokeAsync(request, scope).ConfigureAwait(false);
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
