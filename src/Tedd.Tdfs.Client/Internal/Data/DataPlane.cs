using Tedd.Tdfs.Client.Internal.Rpc;
using Tedd.Tdfs.Client.Internal.Runtime;
using Tedd.Tdfs.Client.Internal.Sessions;
using Tedd.Tdfs.Data;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Client.Internal.Data;

/// <summary>A storage node rejected a data capability (expired or revoked): the caller re-acquires it and retries.</summary>
internal sealed class DataCapabilityRejectedException : Exception
{
    /// <summary>
    /// Bytes at the start of the read's destination that already hold verified object data when the capability was rejected
    /// (a fetch made of many short results can outlive its capability): the caller keeps them instead of starting over.
    /// </summary>
    public int Filled { get; init; }

    public DataCapabilityRejectedException()
        : this("A storage node rejected the data capability.")
    {
    }

    public DataCapabilityRejectedException(string message)
        : base(message)
    {
    }

    public DataCapabilityRejectedException(string message, Exception inner)
        : base(message, inner)
    {
    }
}

/// <summary>
/// Object uploads and verified object reads (PROTOCOL.md §7, ARCHITECTURE §6): HRW placement over the pool's nodes from
/// the cluster map with fallback down the HRW order, parallel <c>PutObject</c> until the policy's acknowledgement
/// requirement (distinct hosts and zones) is met (remaining targets get a short grace; a target far slower than the first
/// acknowledgement gets a fallback started alongside it), and range reads that verify
/// object length, whole-object XxHash128 or per-unit CRC32C, fail over across replicas and a <c>LocateObject</c>
/// broadcast, and never substitute zeroes for missing data.
/// </summary>
internal sealed class DataPlane
{
    private static readonly TimeSpan MinimumGrace = TimeSpan.FromMilliseconds(50);
    private static readonly TimeSpan MinimumHedge = TimeSpan.FromMilliseconds(100);
    private readonly ClientRuntime _runtime;

    public DataPlane(ClientRuntime runtime)
    {
        ArgumentNullException.ThrowIfNull(runtime);
        _runtime = runtime;
    }

    // ------------------------------------------------------------------------------------------------ uploads

    /// <summary>
    /// Uploads one object under <paramref name="lease"/> and returns receipts meeting <paramref name="policy"/>'s
    /// acknowledgement requirement (plus whatever additional copies completed within the grace period).
    /// </summary>
    /// <exception cref="DfsPlacementUnavailableException">Too few distinct hosts or zones accepted the object.</exception>
    /// <remarks>The <see cref="FaultSites.ClientUploadChunk"/> site is hit once per object (payload chunk) with the chunk's
    /// ordinal as occurrence and <paramref name="faultKind"/> as operation: the public operation the chunk belongs to (a
    /// whole-file helper's WriteAll/Append), or <see cref="DfsOperationKind.PutObject"/> for a stream's staged chunks.</remarks>
    public async Task<IReadOnlyList<ObjectReceipt>> UploadAsync(OperationScope scope, VolumeContext volume, StagingLease lease, ObjectId obj,
        ReadOnlyMemory<byte> data, UInt128 checksum, DfsStoragePolicy policy, string pool, DfsOperationId? operation, int chunkIndex,
        DfsOperationKind faultKind = DfsOperationKind.PutObject)
    {
        await scope.HitAsync(FaultStage.F03, FaultSites.ClientUploadChunk, faultKind, operation, chunkIndex, obj).ConfigureAwait(false);
        scope.ThrowIfCancelled();
        if (lease.NeedsRenewal(_runtime.Time.GetUtcNow()))
            await volume.RenewStagingAsync(lease, scope).ConfigureAwait(false);
        await _runtime.EnsureMapEpochAsync(lease.PlacementEpoch, scope).ConfigureAwait(false);

        ulong epoch = lease.PlacementEpoch;
        IReadOnlyList<PlacementNode> nodes = _runtime.Map.PoolNodes(pool);
        IReadOnlyList<PlacementNode> targets = Placement.SelectTargets(obj, epoch, policy, nodes);
        int requiredCopies = policy.DurableCopiesBeforeAck;
        int requiredZones = policy.DurableZonesBeforeAck;

        // Backpressure on a congested target (NET-013): a node whose connection still carries earlier uploads nobody waits for
        // (they were not needed once the policy was met elsewhere) gets no further copy while the other targets can meet the
        // policy without it; its copies are repair's job. Otherwise every chunk would queue another object on its slow link,
        // so outstanding requests to it would grow with the file and its link would delay everything else sent to the node
        // (keep-alives and metadata requests to a slow leader). Congested nodes come last among fallbacks too.
        long congestion = CongestionLimit();
        IReadOnlyList<PlacementNode> order = [.. Placement.FallbackOrder(obj, epoch, nodes).OrderBy(n => IsCongested(n, congestion) ? 1 : 0)];
        var deferred = new List<PlacementNode>();

        var receipts = new List<ObjectReceipt>();
        var holders = new List<DfsTopologyLabels>();
        var tried = new HashSet<NodeId>();
        var pending = new List<(Task<IWireMessage> Task, PlacementNode Node, long Started)>();
        var failures = new List<Exception>();
        bool renewed = false;
        long? satisfiedAt = null;

        // A slow target gets fallback targets started beside it, but it is never abandoned: its durable acknowledgement still
        // counts when it arrives before the operation deadline (a slow disk is not a failed one, PUB-004). Each PutObject may
        // therefore run until the deadline. A target far slower than the first acknowledged put (before any, silent for a
        // quarter of the attempt timeout) is presumably unavailable (paused, partitioned): a fallback starts alongside it
        // instead of every chunk waiting out the silent target's whole attempt timeout (OFF-007).
        TimeSpan slowAfter = _runtime.Rpc.DefaultAttemptTimeout(scope);
        long uploadStart = _runtime.Time.GetTimestamp();
        TimeSpan? firstAck = null;          // how long the first acknowledged put took
        long lastProgress = uploadStart;    // the last acknowledgement or hedge
        bool hedging = true;
        TimeSpan silentHedge = slowAfter / 4;
        using var cancel = CancellationTokenSource.CreateLinkedTokenSource(scope.Token);
        try
        {
            foreach (PlacementNode target in targets)
            {
                if (IsCongested(target, congestion))
                    deferred.Add(target);
                else
                    Start(target);
            }

            if (deferred.Count > 0)
            {
                (int startedCopies, int startedZones) = CountDomains([.. pending.Select(p => p.Node.Labels)]);
                if (startedCopies < requiredCopies || startedZones < requiredZones)
                {
                    // The others cannot meet the policy alone: the congested targets are needed after all.
                    foreach (PlacementNode target in deferred)
                        Start(target);
                }
                else
                {
                    _runtime.Emit("upload.target-congested", $"Object {obj}: no copy to {string.Join(", ", deferred.Select(d => d.Name))} (earlier uploads to it are still outstanding); the other targets meet the policy.",
                        operation, volume: volume.Name);
                }
            }

            while (true)
            {
                (int copies, int zones) = CountDomains(holders);
                bool satisfied = copies >= requiredCopies && zones >= requiredZones;
                if (pending.Count == 0)
                {
                    if (satisfied)
                        break;
                    if (!StartFallbacks(Math.Max(1, requiredCopies - copies)))
                        break;
                    continue;
                }

                Task<Task<IWireMessage>> any = Task.WhenAny(pending.Select(p => p.Task));
                try
                {
                    if (satisfied)
                    {
                        satisfiedAt ??= _runtime.Time.GetTimestamp();
                        TimeSpan grace = Grace(scope, uploadStart, satisfiedAt.Value);

                        // Targets already slower than an attempt timeout (fallbacks were started beside them) are not waited
                        // for once the policy is met: the extra copies are repair's job, and later objects of the same
                        // operation need the remaining time.
                        TimeSpan freshest = TimeSpan.Zero;
                        foreach ((_, _, long started) in pending)
                        {
                            TimeSpan left = slowAfter - _runtime.Time.GetElapsedTime(started);
                            if (left > freshest)
                                freshest = left;
                        }

                        if (freshest < grace)
                            grace = freshest;
                        if (grace <= TimeSpan.Zero || !await Waits.CompletesWithinAsync(any, grace, _runtime.Time, scope.Token).ConfigureAwait(false))
                            break;
                    }
                    else if (hedging)
                    {
                        // More copies are needed and the pending targets have been slow: add a fallback (the slow puts keep
                        // running and still count) and keep waiting for all.
                        TimeSpan delay = firstAck is { } took ? (took * 2 > MinimumHedge ? took * 2 : MinimumHedge) : silentHedge;
                        TimeSpan hedge = delay - _runtime.Time.GetElapsedTime(lastProgress);
                        if (!await Waits.CompletesWithinAsync(any, hedge, _runtime.Time, scope.Token).ConfigureAwait(false))
                        {
                            lastProgress = _runtime.Time.GetTimestamp();
                            if (StartFallbacks(1))
                                continue;
                            hedging = false;

                            // Negative control (MUT-014): the slowness of the remaining targets "relaxes" the policy below.
                            if (_runtime.Mutations.RelaxPolicyOnTimeout && receipts.Count > 0)
                                break;
                            await Waits.WaitAsync(any, scope.Token).ConfigureAwait(false);
                        }
                    }
                    else
                    {
                        await Waits.WaitAsync(any, scope.Token).ConfigureAwait(false);
                    }
                }
                catch (OperationCanceledException) when (scope.Token.IsCancellationRequested)
                {
                    if (satisfied)
                        break;

                    // Deadline, caller or shutdown: nothing was published. A caller's cancellation or the client's disposal
                    // reports itself; at the deadline the policy could not be met (typed placement unavailability).
                    if (scope.IsAbortedByCaller)
                        throw scope.NotTransmittedFailure(failures.LastOrDefault());
                    failures.Add(new TimeoutException($"Object {obj} was not acknowledged by enough storage nodes before the deadline."));
                    break;
                }

                Task<IWireMessage> finished = any.Result;
                int index = pending.FindIndex(p => ReferenceEquals(p.Task, finished));
                (_, PlacementNode node, long putStarted) = pending[index];
                pending.RemoveAt(index);
                bool ok = false;
                try
                {
                    IWireMessage reply = await finished.ConfigureAwait(false);
                    switch (reply)
                    {
                        case PutObjectResultMessage put when Matches(put.Receipt, obj, checksum, data.Length, lease.Id):
                            receipts.Add(put.Receipt);
                            holders.Add(_runtime.Map.Node(put.Receipt.Node)?.Labels ?? node.Labels);
                            firstAck ??= _runtime.Time.GetElapsedTime(putStarted);
                            lastProgress = _runtime.Time.GetTimestamp();
                            ok = true;
                            break;
                        case PutObjectResultMessage put:
                            failures.Add(new DfsProtocolException($"Node {node.Name} returned a receipt for another object ({put.Receipt.Object})."));
                            break;
                        case ErrorReplyMessage { Code: DfsErrorCode.AccessDenied } denied when !renewed:
                            renewed = true;
                            failures.Add(denied.ToException());
                            await volume.RenewStagingAsync(lease, scope).ConfigureAwait(false);
                            Restart(node);
                            continue;
                        case ErrorReplyMessage error:
                            failures.Add(error.ToException());
                            break;
                        default:
                            failures.Add(new DfsProtocolException($"Expected PutObjectResult from {node.Name}, received {reply.Type}."));
                            break;
                    }
                }
                catch (OperationCanceledException) when (scope.Token.IsCancellationRequested)
                {
                    if (scope.IsAbortedByCaller)
                        throw scope.NotTransmittedFailure(failures.LastOrDefault());
                    failures.Add(new TimeoutException($"Object {obj} was not acknowledged by enough storage nodes before the deadline."));
                    break;
                }
                catch (Exception ex) when (RpcInvoker.IsTransient(ex) || ex is DfsProtocolException or DfsClusterIdentityException or OperationCanceledException)
                {
                    failures.Add(ex);
                }

                if (!ok)
                {
                    (copies, zones) = CountDomains(holders);
                    if (copies < requiredCopies || zones < requiredZones)
                        StartFallbacks(1);
                }
            }
        }
        finally
        {
            ContextFree.Cancel(cancel);
            foreach ((Task<IWireMessage> task, _, _) in pending)
                Waits.ObserveQuietly(task);
        }

        (int achievedCopies, int achievedZones) = CountDomains(holders);
        if ((achievedCopies < requiredCopies || achievedZones < requiredZones) && _runtime.Mutations.RelaxPolicyOnTimeout && receipts.Count > 0
            && !scope.Token.IsCancellationRequested)
        {
            // Negative control (MUT-014): the policy is silently weakened to whatever copies were acknowledged.
            _runtime.Emit("upload.policy-relaxed", $"Object {obj}: publishing with {achievedCopies} copies in {achievedZones} zones (RelaxPolicyOnTimeout).",
                operation, volume: volume.Name);
            return receipts;
        }

        if (achievedCopies < requiredCopies || achievedZones < requiredZones)
        {
            if (scope.IsAbortedByCaller)
                throw scope.NotTransmittedFailure(failures.LastOrDefault());
            Exception? inner = failures.Count switch { 0 => null, 1 => failures[0], _ => new AggregateException(failures) };
            if (achievedCopies == 0 && failures.Count > 0 && failures.All(IsCapacityRefusal))
            {
                // Every candidate of the pool refused the object for capacity (POOL-012): the pool is full, which is a
                // capacity condition rather than a placement (domain) shortfall.
                var full = new DfsCapacityException(
                    $"Object {obj} was refused for capacity by every candidate node of pool '{pool}' ({nodes.Count} candidate node(s)); the pool is full.", inner);
                _runtime.Emit("upload.capacity-exhausted", full.Message, operation, volume: volume.Name, exception: inner);
                throw full;
            }

            var placement = new DfsPlacementUnavailableException(
                $"Object {obj} reached {achievedCopies} host(s) in {achievedZones} zone(s) of pool '{pool}' ({nodes.Count} candidate node(s)); the storage policy requires {requiredCopies} host(s) in {requiredZones} zone(s) before acknowledgement.",
                requiredCopies, achievedCopies, requiredZones, achievedZones, inner);
            _runtime.Emit("upload.placement-unavailable", placement.Message, operation, volume: volume.Name, exception: inner);
            throw placement;
        }

        return receipts;

        void Start(PlacementNode node)
        {
            tried.Add(node.Id);
            Restart(node);
        }

        void Restart(PlacementNode node)
        {
            pending.Add((SendPutAsync(scope, volume, lease, node, obj, data, checksum, chunkIndex, cancel.Token), node, _runtime.Time.GetTimestamp()));
        }

        bool StartFallbacks(int count)
        {
            int started = 0;
            foreach (PlacementNode candidate in PickFallbacks(order, tried, holders, count))
            {
                Start(candidate);
                started++;
            }

            return started > 0;
        }
    }

    /// <summary>Abandoned upload bytes on a target's connection that make it congested: two chunks.</summary>
    private long CongestionLimit() => 2L * ContentLayout.ChunkSize(_runtime.Capabilities);

    /// <summary>
    /// True when the pooled connection to <paramref name="node"/> still carries at least <paramref name="limit"/> bytes of
    /// requests nobody waits for any more (<see cref="ClientConnection.AbandonedBytes"/>).
    /// </summary>
    private bool IsCongested(PlacementNode node, long limit) =>
        _runtime.Map.EndpointOf(node.Id) is { } endpoint && _runtime.Pool.TryGetOpen(endpoint, out ClientConnection connection)
        && connection.AbandonedBytes >= limit;

    /// <summary>True for a storage node's refusal of an object for lack of capacity.</summary>
    private static bool IsCapacityRefusal(Exception ex) => ex is DfsException { ErrorCode: DfsErrorCode.CapacityExhausted };

    /// <summary>Failure domains of the acknowledged copies (the <c>CountSameZoneTwice</c> negative control counts every host as a zone).</summary>
    private (int Copies, int Zones) CountDomains(List<DfsTopologyLabels> holders)
    {
        (int copies, int zones) = Placement.CountDomains(holders);
        return _runtime.Mutations.CountSameZoneTwice ? (copies, copies) : (copies, zones);
    }

    private TimeSpan Grace(OperationScope scope, long uploadStart, long satisfiedAt)
    {
        // Remaining targets get as long again as this object's acknowledgement took (at least a short floor), within the
        // deadline. Measured from the object's upload, not the operation's start: a multi-chunk write must not give every
        // later chunk's silent target an ever longer grace (OFF-007).
        TimeSpan took = _runtime.Time.GetElapsedTime(uploadStart, satisfiedAt);
        TimeSpan waited = _runtime.Time.GetElapsedTime(satisfiedAt);
        TimeSpan grace = (took > MinimumGrace ? took : MinimumGrace) - waited;
        TimeSpan remaining = scope.Remaining;
        return grace < remaining ? grace : remaining;
    }

    private static IEnumerable<PlacementNode> PickFallbacks(IReadOnlyList<PlacementNode> order, HashSet<NodeId> tried, List<DfsTopologyLabels> holders, int count)
    {
        var hosts = new HashSet<string>(holders.Select(h => h.Host), StringComparer.Ordinal);
        var zones = new HashSet<(string, string)>(holders.Select(h => h.ZoneKey));
        var picked = new List<PlacementNode>();

        // Prefer nodes adding a zone (on a new host), then a new host, then anything, each in HRW order.
        foreach (Func<PlacementNode, bool> preference in new Func<PlacementNode, bool>[]
                 {
                     n => !zones.Contains(n.Labels.ZoneKey) && !hosts.Contains(n.Labels.Host),
                     n => !hosts.Contains(n.Labels.Host),
                     _ => true,
                 })
        {
            foreach (PlacementNode node in order)
            {
                if (picked.Count >= count)
                    return picked;
                if (tried.Contains(node.Id) || picked.Contains(node) || !preference(node))
                    continue;
                picked.Add(node);
                hosts.Add(node.Labels.Host);
                zones.Add(node.Labels.ZoneKey);
            }
        }

        return picked;
    }

    private async Task<IWireMessage> SendPutAsync(OperationScope scope, VolumeContext volume, StagingLease lease, PlacementNode node, ObjectId obj,
        ReadOnlyMemory<byte> data, UInt128 checksum, int chunkIndex, CancellationToken token)
    {
        string endpoint = _runtime.Map.EndpointOf(node.Id)
            ?? throw new DfsUnavailableException(DfsErrorCode.NodeUnavailable, $"Node {node.Name} has no known endpoint.");
        var message = new PutObjectMessage(lease.Capability, volume.VolumeId, lease.Id, obj, checksum, data.Length, ObjectEncodings.Plain,
            lease.PlacementEpoch, data);
        return await _runtime.Rpc.SendOnceAsync(endpoint, message, scope, ClientRuntime.Positive(scope.Remaining), obj: obj, occurrence: chunkIndex,
            token: token).ConfigureAwait(false);
    }

    private static bool Matches(ObjectReceipt receipt, ObjectId obj, UInt128 checksum, long length, StagingId staging) =>
        receipt.Object == obj && receipt.Checksum == checksum && receipt.Length == length && receipt.Staging == staging
        && receipt.Encoding == ObjectEncodings.Plain;

    // ------------------------------------------------------------------------------------------------ reads

    /// <summary>
    /// Reads <c>destination.Length</c> bytes of an object starting at <paramref name="objectOffset"/> into
    /// <paramref name="destination"/>, verified against the object's checksum (whole object) or the node's unit checksums.
    /// Tries the listed locations, then the HRW order of the placement epoch, then a <c>LocateObject</c> broadcast.
    /// </summary>
    /// <exception cref="DfsIntegrityException">No replica returned verifiable data and at least one returned corrupt or no data.</exception>
    /// <exception cref="DfsUnavailableException">No replica could be reached.</exception>
    /// <exception cref="DataCapabilityRejectedException">
    /// The capability was rejected (re-acquire it and retry); <see cref="DataCapabilityRejectedException.Filled"/> bytes at the
    /// start of <paramref name="destination"/> were already read and verified.
    /// </exception>
    public async Task ReadObjectAsync(OperationScope scope, VolumeContext volume, ReadOnlyMemory<byte> capability, ObjectId obj, long objectLength,
        UInt128 checksum, byte encoding, ulong placementEpoch, IReadOnlyList<NodeId> locations, long objectOffset, Memory<byte> destination)
    {
        if (destination.IsEmpty)
            return;
        if (encoding != ObjectEncodings.Plain)
            throw new DfsIntegrityException(obj, $"Object {obj} uses unsupported encoding {encoding}.");
        if (objectOffset < 0 || objectOffset + destination.Length > objectLength)
            throw new DfsIntegrityException(obj, $"Extent range [{objectOffset}, {objectOffset + destination.Length}) exceeds the {objectLength}-byte object {obj}.");

        var tried = new HashSet<NodeId>();
        var failures = new List<Exception>();
        int integrityFailures = 0;
        DataCapabilityRejectedException? denied = null;
        var candidates = new List<NodeId>();
        foreach (NodeId location in locations)
            AddCandidate(location);
        IReadOnlyList<PlacementNode> storage = _runtime.Map.StorageNodes()
            .Select(n => new PlacementNode(n.Id, n.Name, n.Labels, true, 1)).ToArray();
        foreach (PlacementNode node in Placement.FallbackOrder(obj, placementEpoch, storage))
            AddCandidate(node.Id);

        // Replicas on nodes that recently let a request time out are tried last (stable order otherwise): waiting out a
        // silent node first would delay every read and can outlive the read capability (OFF-002).
        candidates = [.. candidates.OrderBy(c => _runtime.Map.EndpointOf(c) is { } e && _runtime.IsRecentlyUnresponsive(e) ? 1 : 0)];

        bool located = false;
        var reconnected = new HashSet<NodeId>();
        var unresolved = new List<NodeId>();
        for (int i = 0; ; i++)
        {
            if (i >= candidates.Count)
            {
                if (located || scope.Token.IsCancellationRequested)
                    break;
                located = true;

                // Holders may be nodes this client's cluster map does not list yet: nodes that joined after the map was
                // fetched, to which repair then moved the copies (and removed the excess ones the map knows). When a holder
                // was not in the map, or every node asked answered without a verifiable copy, refresh the map before asking
                // every storage node, so "no verifiable copy" is never concluded for want of asking the nodes that hold it.
                // (With unreachable nodes the read fails as unavailable anyway; no refresh round trips are spent then.)
                bool mayBeStale = unresolved.Count > 0 || failures.Count == integrityFailures;
                if (mayBeStale && await _runtime.RefreshMapAsync(scope).ConfigureAwait(false))
                {
                    foreach (NodeId node in unresolved)
                    {
                        if (_runtime.Map.EndpointOf(node) is not null)
                        {
                            tried.Remove(node);
                            candidates.Add(node);
                        }
                    }

                    unresolved.Clear();
                    PlacementNode[] refreshed = _runtime.Map.StorageNodes().Select(n => new PlacementNode(n.Id, n.Name, n.Labels, true, 1)).ToArray();
                    foreach (PlacementNode node in Placement.FallbackOrder(obj, placementEpoch, refreshed))
                        AddCandidate(node.Id);
                }

                foreach (NodeId holder in await LocateAsync(scope, volume, obj, [.. tried, .. candidates.Skip(i)]).ConfigureAwait(false))
                    AddCandidate(holder);
                if (i >= candidates.Count)
                    break;
            }

            NodeId candidate = candidates[i];
            tried.Add(candidate);
            string? endpoint = _runtime.Map.EndpointOf(candidate);
            if (endpoint is null)
            {
                unresolved.Add(candidate);
                continue;
            }
            try
            {
                await ReadFromAsync(scope, volume, endpoint, capability, obj, objectLength, checksum, objectOffset, destination).ConfigureAwait(false);
                return;
            }
            catch (DfsIntegrityException ex)
            {
                integrityFailures++;
                failures.Add(ex);
                _runtime.Emit("read.replica-integrity", $"Replica of {obj} on {endpoint} failed verification; trying another.", volume: volume.Name, exception: ex);
            }
            catch (OperationCanceledException) when (scope.Token.IsCancellationRequested)
            {
                throw scope.NotTransmittedFailure(failures.LastOrDefault());
            }
            catch (DataCapabilityRejectedException ex)
            {
                // One storage node refusing the capability (its control replica lags behind a volume change, or its clock
                // disagrees) says nothing about the other replicas: try them, and report the refusal only when no replica
                // served the range, keeping the longest verified prefix (objects are immutable, so every replica's verified
                // bytes are the same bytes).
                if (denied is null || ex.Filled > denied.Filled)
                    denied = ex;
                _runtime.Emit("read.replica-denied", $"Replica of {obj} on {endpoint} refused the read capability; trying another.", volume: volume.Name,
                    exception: ex);
            }
            catch (Exception ex) when (RpcInvoker.IsTransient(ex) || ex is DfsProtocolException or DfsClusterIdentityException or OperationCanceledException
                                           or DfsException)
            {
                failures.Add(ex);

                // A pooled connection that turned out dead (reset or closed under the request) says nothing about the
                // replica: ask it once more on a new connection (NET-005).
                if (ex is RpcConnectionException && reconnected.Add(candidate) && !scope.Token.IsCancellationRequested)
                    i--;
            }
        }

        scope.ThrowIfCancelled();
        if (denied is not null)
            throw denied;
        Exception? inner = failures.Count switch { 0 => null, 1 => failures[0], _ => new AggregateException(failures) };
        int unreachable = failures.Count - integrityFailures;
        if (integrityFailures > 0 && unreachable == 0)
        {
            // Every node that could be asked answered: no verifiable copy exists (never zero-filled, DSK-016).
            throw new DfsIntegrityException(obj, $"No replica of object {obj} returned verifiable data ({integrityFailures} corrupt or missing).");
        }

        // Some nodes could not be asked: the data may still exist on them, so this is unavailability, not loss.
        throw new DfsUnavailableException(DfsErrorCode.NodeUnavailable,
            $"No replica of object {obj} could be read ({unreachable} node(s) unreachable, {integrityFailures} without a verifiable copy).", inner);

        void AddCandidate(NodeId node)
        {
            if (!candidates.Contains(node))
                candidates.Add(node);
        }
    }

    private async Task ReadFromAsync(OperationScope scope, VolumeContext volume, string endpoint, ReadOnlyMemory<byte> capability, ObjectId obj,
        long objectLength, UInt128 checksum, long objectOffset, Memory<byte> destination)
    {
        int filled = 0;
        bool wholeFetched = false;
        while (filled < destination.Length)
        {
            long offset = objectOffset + filled;
            int want = destination.Length - filled;
            var message = new GetObjectMessage(capability, volume.VolumeId, obj, offset, want);
            IWireMessage reply = await _runtime.Rpc.SendOnceAsync(endpoint, message, scope, _runtime.Rpc.DefaultAttemptTimeout(scope), obj: obj)
                .ConfigureAwait(false);
            switch (reply)
            {
                case GetObjectResultMessage result:
                    int copied = Verify(result, obj, objectLength, checksum, offset, destination.Span[filled..], out bool needWhole,
                        _runtime.Mutations.SkipChecksumVerification);
                    if (needWhole)
                    {
                        if (wholeFetched)
                            throw new DfsIntegrityException(obj, $"Object {obj} from {endpoint} could not be verified.");
                        wholeFetched = true;
                        try
                        {
                            await ReadWholeAsync(scope, volume, endpoint, capability, obj, objectLength, checksum, offset, destination[filled..])
                                .ConfigureAwait(false);
                        }
                        catch (DataCapabilityRejectedException denied) when (filled > 0)
                        {
                            // The bytes verified before the whole-object fetch stay valid: the caller keeps them.
                            throw new DataCapabilityRejectedException(denied.Message, denied.InnerException ?? denied) { Filled = filled };
                        }

                        return;
                    }

                    if (copied <= 0)
                        throw new DfsIntegrityException(obj, $"Object {obj} on {endpoint} returned no data at offset {offset} of {objectLength}.");
                    filled += copied;
                    break;
                case ErrorReplyMessage { Code: DfsErrorCode.AccessDenied } denied:
                    // The bytes verified so far stay valid (objects are immutable): the caller keeps them.
                    throw new DataCapabilityRejectedException(denied.Message, denied.ToException()) { Filled = filled };
                case ErrorReplyMessage { Code: DfsErrorCode.Integrity } corrupt:
                    throw new DfsIntegrityException(obj, corrupt.Message);
                case ErrorReplyMessage error:
                    throw error.ToException();
                default:
                    throw new DfsProtocolException($"Expected GetObjectResult from {endpoint}, received {reply.Type}.");
            }
        }
    }

    private async Task ReadWholeAsync(OperationScope scope, VolumeContext volume, string endpoint, ReadOnlyMemory<byte> capability, ObjectId obj,
        long objectLength, UInt128 checksum, long offset, Memory<byte> destination)
    {
        if (objectLength > int.MaxValue)
            throw new DfsIntegrityException(obj, $"Object {obj} is too large to verify as a whole.");
        var whole = new byte[objectLength];
        int filled = 0;
        while (filled < whole.Length)
        {
            IWireMessage reply = await _runtime.Rpc.SendOnceAsync(endpoint, new GetObjectMessage(capability, volume.VolumeId, obj, filled, whole.Length - filled),
                scope, _runtime.Rpc.DefaultAttemptTimeout(scope), obj: obj).ConfigureAwait(false);
            switch (reply)
            {
                case GetObjectResultMessage result when result.ObjectLength == objectLength && result.Checksum == checksum && result.Offset <= filled
                                                       && result.Offset + result.Data.Length > filled:
                    int skip = (int)(filled - result.Offset);
                    int take = Math.Min(result.Data.Length - skip, whole.Length - filled);
                    result.Data.Span.Slice(skip, take).CopyTo(whole.AsSpan(filled));
                    filled += take;
                    break;
                case GetObjectResultMessage:
                    throw new DfsIntegrityException(obj, $"Object {obj} on {endpoint} returned inconsistent data.");
                case ErrorReplyMessage { Code: DfsErrorCode.AccessDenied } denied:
                    throw new DataCapabilityRejectedException(denied.Message, denied.ToException());
                case ErrorReplyMessage { Code: DfsErrorCode.Integrity } corrupt:
                    throw new DfsIntegrityException(obj, corrupt.Message);
                case ErrorReplyMessage error:
                    throw error.ToException();
                default:
                    throw new DfsProtocolException($"Expected GetObjectResult from {endpoint}, received {reply.Type}.");
            }
        }

        if (!_runtime.Mutations.SkipChecksumVerification && ObjectChecksum.Compute(whole) != checksum)
            throw new DfsIntegrityException(obj, $"Object {obj} on {endpoint} does not match its checksum.");
        whole.AsSpan((int)offset, destination.Length).CopyTo(destination.Span);
    }

    /// <summary>
    /// Verifies a <c>GetObject</c> reply and copies the verified bytes at <paramref name="offset"/> into
    /// <paramref name="destination"/>. Returns the bytes copied; <paramref name="needWhole"/> asks for a whole-object read
    /// (no unit checksums for a partial range).
    /// </summary>
    internal static int Verify(GetObjectResultMessage result, ObjectId obj, long objectLength, UInt128 checksum, long offset, Span<byte> destination,
        out bool needWhole, bool skipChecksums = false)
    {
        needWhole = false;
        if (result.ObjectLength != objectLength)
            throw new DfsIntegrityException(obj, $"Object {obj} has length {result.ObjectLength}; the extent expects {objectLength}.");
        if (result.Checksum != checksum)
            throw new DfsIntegrityException(obj, $"Object {obj} has a different checksum than the extent expects.");
        if (result.Encoding != ObjectEncodings.Plain)
            throw new DfsIntegrityException(obj, $"Object {obj} uses unsupported encoding {result.Encoding}.");
        ReadOnlySpan<byte> data = result.Data.Span;
        long start = result.Offset;
        if (start > offset || start + data.Length <= offset || start < 0 || start + data.Length > objectLength)
            return 0;

        if (skipChecksums)
        {
            // Negative control (MUT-011): bytes are delivered without verification.
        }
        else if (start == 0 && data.Length == objectLength)
        {
            if (ObjectChecksum.Compute(data) != checksum)
                throw new DfsIntegrityException(obj, $"Object {obj} does not match its checksum.");
        }
        else if (result.UnitSize > 0 && start % result.UnitSize == 0)
        {
            int unit = result.UnitSize;
            int units = (int)((data.Length + (long)unit - 1) / unit);
            if (result.UnitChecksums.Count < units)
                throw new DfsIntegrityException(obj, $"Object {obj}: {result.UnitChecksums.Count} unit checksums for {units} units.");
            for (int i = 0; i < units; i++)
            {
                // Only the last unit of a result may be partial: at the object end, or in a verified short result (FSD §6.2,
                // PROTOCOL.md GetObjectResult), where its checksum covers exactly the bytes returned.
                int at = i * unit;
                int length = Math.Min(unit, data.Length - at);
                if (ObjectChecksum.Crc32C(data.Slice(at, length)) != result.UnitChecksums[i])
                    throw new DfsIntegrityException(obj, $"Object {obj}: unit {(start + at) / unit} does not match its checksum.");
            }
        }
        else
        {
            needWhole = true;
            return 0;
        }

        int skip = (int)(offset - start);
        int take = Math.Min(data.Length - skip, destination.Length);
        data.Slice(skip, take).CopyTo(destination);
        return take;
    }

    private async Task<IReadOnlyList<NodeId>> LocateAsync(OperationScope scope, VolumeContext volume, ObjectId obj, HashSet<NodeId> exclude)
    {
        var found = new List<NodeId>();
        foreach (NodeEntry node in _runtime.Map.StorageNodes())
        {
            if (scope.Token.IsCancellationRequested)
                break;
            if (exclude.Contains(node.Id) || node.Endpoints.Count == 0)
                continue;
            try
            {
                IWireMessage reply = await _runtime.Rpc.SendOnceAsync(node.Endpoints[0], new LocateObjectMessage(volume.VolumeId, [obj]), scope,
                    _runtime.Rpc.DefaultAttemptTimeout(scope), obj: obj).ConfigureAwait(false);
                if (reply is LocateObjectResultMessage located && located.Locations.Any(l => l.Object == obj && l.Present))
                    found.Add(node.Id);
            }
            catch (OperationCanceledException) when (scope.Token.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex) when (RpcInvoker.IsTransient(ex) || ex is DfsProtocolException or DfsClusterIdentityException or DfsException)
            {
                // An unreachable node simply cannot be a source.
            }
        }

        return found;
    }

    /// <summary>Computes the content checksum and a fresh random object id for an upload.</summary>
    public (ObjectId Id, UInt128 Checksum) Describe(ReadOnlySpan<byte> data) => (_runtime.Random.NextObjectId(), ObjectChecksum.Compute(data));
}
