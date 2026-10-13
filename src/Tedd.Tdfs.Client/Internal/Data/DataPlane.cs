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
/// requirement (distinct hosts and zones) is met (remaining targets get a short grace), and range reads that verify
/// object length, whole-object XxHash128 or per-unit CRC32C, fail over across replicas and a <c>LocateObject</c>
/// broadcast, and never substitute zeroes for missing data.
/// </summary>
internal sealed class DataPlane
{
    private static readonly TimeSpan MinimumGrace = TimeSpan.FromMilliseconds(50);
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
        IReadOnlyList<PlacementNode> order = Placement.FallbackOrder(obj, epoch, nodes);
        IReadOnlyList<PlacementNode> targets = Placement.SelectTargets(obj, epoch, policy, nodes);
        int requiredCopies = policy.DurableCopiesBeforeAck;
        int requiredZones = policy.DurableZonesBeforeAck;

        var receipts = new List<ObjectReceipt>();
        var holders = new List<DfsTopologyLabels>();
        var tried = new HashSet<NodeId>();
        var pending = new List<(Task<IWireMessage> Task, PlacementNode Node)>();
        var failures = new List<Exception>();
        bool renewed = false;
        long? satisfiedAt = null;
        using var cancel = CancellationTokenSource.CreateLinkedTokenSource(scope.Token);
        try
        {
            foreach (PlacementNode target in targets)
                Start(target);

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
                        TimeSpan grace = Grace(scope, satisfiedAt.Value);
                        if (grace <= TimeSpan.Zero || !await Waits.CompletesWithinAsync(any, grace, _runtime.Time, scope.Token).ConfigureAwait(false))
                            break;
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

                    // Deadline, caller or shutdown: nothing was published; report what kept the copies from completing.
                    throw scope.NotTransmittedFailure(failures.LastOrDefault()
                        ?? new TimeoutException($"Object {obj} was not acknowledged by enough storage nodes before the deadline."));
                }

                Task<IWireMessage> finished = any.Result;
                int index = pending.FindIndex(p => ReferenceEquals(p.Task, finished));
                PlacementNode node = pending[index].Node;
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
                    throw scope.NotTransmittedFailure(failures.LastOrDefault());
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
            foreach ((Task<IWireMessage> task, _) in pending)
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
            if (scope.Token.IsCancellationRequested)
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
            pending.Add((SendPutAsync(scope, volume, lease, node, obj, data, checksum, chunkIndex, cancel.Token), node));
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

    /// <summary>True for a storage node's refusal of an object for lack of capacity.</summary>
    private static bool IsCapacityRefusal(Exception ex) => ex is DfsException { ErrorCode: DfsErrorCode.CapacityExhausted };

    /// <summary>Failure domains of the acknowledged copies (the <c>CountSameZoneTwice</c> negative control counts every host as a zone).</summary>
    private (int Copies, int Zones) CountDomains(List<DfsTopologyLabels> holders)
    {
        (int copies, int zones) = Placement.CountDomains(holders);
        return _runtime.Mutations.CountSameZoneTwice ? (copies, copies) : (copies, zones);
    }

    private TimeSpan Grace(OperationScope scope, long satisfiedAt)
    {
        // Remaining targets get as long again as the acknowledgement took (at least a short floor), within the deadline.
        TimeSpan sinceStart = scope.Elapsed;
        TimeSpan waited = _runtime.Time.GetElapsedTime(satisfiedAt);
        TimeSpan grace = (sinceStart > MinimumGrace ? sinceStart : MinimumGrace) - waited;
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
        return await _runtime.Rpc.SendOnceAsync(endpoint, message, scope, _runtime.Rpc.DefaultAttemptTimeout(scope), obj: obj, occurrence: chunkIndex,
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
    /// <exception cref="DataCapabilityRejectedException">The capability was rejected (re-acquire it and retry).</exception>
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
        var candidates = new List<NodeId>();
        foreach (NodeId location in locations)
            AddCandidate(location);
        IReadOnlyList<PlacementNode> storage = _runtime.Map.StorageNodes()
            .Select(n => new PlacementNode(n.Id, n.Name, n.Labels, true, 1)).ToArray();
        foreach (PlacementNode node in Placement.FallbackOrder(obj, placementEpoch, storage))
            AddCandidate(node.Id);

        bool located = false;
        for (int i = 0; ; i++)
        {
            if (i >= candidates.Count)
            {
                if (located || scope.Token.IsCancellationRequested)
                    break;
                located = true;
                foreach (NodeId holder in await LocateAsync(scope, volume, obj, tried).ConfigureAwait(false))
                    AddCandidate(holder);
                if (i >= candidates.Count)
                    break;
            }

            NodeId candidate = candidates[i];
            tried.Add(candidate);
            string? endpoint = _runtime.Map.EndpointOf(candidate);
            if (endpoint is null)
                continue;
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
            catch (DataCapabilityRejectedException)
            {
                throw;
            }
            catch (Exception ex) when (RpcInvoker.IsTransient(ex) || ex is DfsProtocolException or DfsClusterIdentityException or OperationCanceledException
                                           or DfsException)
            {
                failures.Add(ex);
            }
        }

        scope.ThrowIfCancelled();
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
                        await ReadWholeAsync(scope, volume, endpoint, capability, obj, objectLength, checksum, offset, destination[filled..]).ConfigureAwait(false);
                        return;
                    }

                    if (copied <= 0)
                        throw new DfsIntegrityException(obj, $"Object {obj} on {endpoint} returned no data at offset {offset} of {objectLength}.");
                    filled += copied;
                    break;
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
