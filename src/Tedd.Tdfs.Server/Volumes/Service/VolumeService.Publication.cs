using System.Collections.Immutable;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Control;
using Tedd.Tdfs.Server.Data;
using Tedd.Tdfs.Server.Runtime;
using Tedd.Tdfs.Server.Volumes.Apply;
using Tedd.Tdfs.Server.Volumes.Commands;
using Tedd.Tdfs.Server.Volumes.State;

namespace Tedd.Tdfs.Server.Volumes.Service;

/// <summary>Leader planning result of a publication (representation and materialization).</summary>
/// <param name="Changes">Final change list (absolute offsets).</param>
/// <param name="TruncateTo">Truncation watermark.</param>
/// <param name="FinalInline">True when the result is stored inline.</param>
/// <param name="Derived">Base revision the plan depends on (0 = none).</param>
/// <param name="LeaderStaging">Leader-created staging session of materialized objects (0 = none).</param>
internal readonly record struct PublishPlan(ImmutableArray<StreamChange> Changes, long TruncateTo, bool FinalInline, ulong Derived, ulong LeaderStaging);

internal sealed partial class VolumeService
{
    /// <summary>
    /// <c>CommitFile</c> (FSD §10.3, PROTOCOL.md §8.1): validates the handle and generation, the receipts of every referenced
    /// object (before proposing; insufficient → PlacementUnavailable, nothing proposed) and the representation (LAY-002),
    /// then proposes one entry.
    /// </summary>
    public async Task<IWireMessage> CommitFileAsync(RpcContext context, CommitFileMessage request)
    {
        string principal = context.Principal ?? string.Empty;
        RequireGrant(principal, DfsPermissions.Write, "publish");
        IWireMessage reply = await MutateAsync(context, request, DfsOperationKind.CommitFile, async (op, ct) =>
        {
            HandleRecord h = VolumeStateMachine.RequireWriterHandle(State, new ApplyContext(0, 0, new CloseHandleCommand(0, 0) { Op = op }),
                request.Handle.Value, request.Generation);
            IDisposable hold = await LockStreamAsync(h.Key, ct);
            try
            {
                VolumeState s = State;
                InodeRecord inode = s.Find(h.Inode) ?? throw new DfsLeaseLostException($"The inode of handle {h.Id} no longer exists.");
                StreamRecord stream = inode.GetStream(h.Stream) ?? throw new DfsLeaseLostException($"The stream of handle {h.Id} no longer exists.");
                (DfsStoragePolicy policy, string pool) = PolicyOf(inode.PolicyName);
                string path = PathForFaults(s, h.Inode, h.Stream);
                ImmutableArray<StreamChange> changes = await ValidateChangesAsync(request.Changes, request.Receipts, request.Staging, policy, pool,
                    DfsOperationKind.CommitFile, op.Id, path, ct);
                if (h.IsAppend && VolumeStateMachine.AppendHandleError(stream.Length, request.TruncateTo, request.NewLength, changes) is { } appendError)
                {
                    // Checked on the client's changes before the leader may rewrite them into a materialized form (FSD 6.1).
                    throw new ArgumentException(appendError);
                }

                PublishPlan plan = await PlanAsync(stream, request.TruncateTo, changes, request.NewLength, policy, pool, request.Staging, op, ct);
                var spec = new PublishSpec(plan.TruncateTo, request.NewLength, plan.Changes, plan.FinalInline, plan.Derived, request.Staging.Value, plan.LeaderStaging)
                {
                    Policy = inode.PolicyName,
                };
                return new Prepared(Stamp(new CommitFileCommand(h.Id, request.Generation, request.BaseRevision, spec, request.SnapshotName), op), hold);
            }
            catch
            {
                hold.Dispose();
                throw;
            }
        }, cleanup: true);
        CountStaleWrite(reply);
        return reply;
    }

    /// <summary><c>WriteAll</c> (FSD §5.4): atomic replacement by path preserving the inode (one entry; receipts validated first).</summary>
    public async Task<IWireMessage> WriteAllAsync(RpcContext context, WriteAllMessage request)
    {
        string principal = context.Principal ?? string.Empty;
        string path = NormalizePath(request.Path);
        RequireGrant(principal, DfsPermissions.Write, "write all");
        if (request.Length < 0)
            throw new ArgumentException("The length must not be negative.");
        if (request.StoragePolicy is { Length: > 0 } named && !Record.NamedPolicies.ContainsKey(named))
            throw new DfsException(DfsErrorCode.InvalidPolicy, $"Volume '{Name}' has no storage policy named '{named}'.");
        return await MutateAsync(context, request, DfsOperationKind.WriteAll, async (op, ct) =>
        {
            (string filePath, string? streamName) = DfsStreamPath.Split(path);
            string stream = streamName ?? DfsStreamPath.DefaultStreamName;
            Resolution res = new NamespaceView(State.Inodes).Resolve(filePath);
            IDisposable? hold = res.Found ? await LockStreamAsync(new StreamKey(res.Target!.Id, stream), ct) : null;
            try
            {
                res = new NamespaceView(State.Inodes).Resolve(filePath);
                InodeRecord? existing = res.Found ? res.Target : null;

                // The policy the file will be under: its own, or for a new file the requested one, else the policy of the
                // directory it is created in; with CreateParents the missing directories inherit the nearest existing one's.
                string policyName = existing?.PolicyName ?? request.StoragePolicy ?? res.Parent?.PolicyName
                    ?? (request.CreateParents && res.Status == ResolveStatus.AncestorMissing && res.Ancestors.Count > 0 ? res.Ancestors[^1].PolicyName : null)
                    ?? string.Empty;
                (DfsStoragePolicy policy, string pool) = PolicyOf(policyName);
                ImmutableArray<StreamChange> changes = await ValidateChangesAsync(request.Changes, request.Receipts, request.Staging, policy, pool,
                    DfsOperationKind.WriteAll, op.Id, path, ct);
                PublishPlan plan = await PlanAsync(null, 0, changes, request.Length, policy, pool, request.Staging, op, ct);
                var spec = new PublishSpec(0, request.Length, plan.Changes, plan.FinalInline, 0, request.Staging.Value, plan.LeaderStaging) { Policy = policyName };
                return new Prepared(Stamp(new WriteAllCommand(path, spec, request.StoragePolicy, request.SnapshotName, request.CreateParents,
                    request.ExpectedRevision), op), hold);
            }
            catch
            {
                hold?.Dispose();
                throw;
            }
        }, cleanup: true);
    }

    /// <summary><c>Append</c> (FSD §5.4, OUT-006): atomic suffix under one operation id, by path or through a writable handle.</summary>
    public async Task<IWireMessage> AppendAsync(RpcContext context, AppendMessage request)
    {
        string principal = context.Principal ?? string.Empty;
        RequireGrant(principal, DfsPermissions.Write, "append");
        if (request.AppendLength < 0)
            throw new ArgumentException("The appended length must not be negative.");
        if (request.StoragePolicy is { Length: > 0 } named && !Record.NamedPolicies.ContainsKey(named))
            throw new DfsException(DfsErrorCode.InvalidPolicy, $"Volume '{Name}' has no storage policy named '{named}'.");
        string path = request.Handle.IsEmpty ? NormalizePath(request.Path) : string.Empty;
        IWireMessage reply = await MutateAsync(context, request, DfsOperationKind.Append, async (op, ct) =>
        {
            StreamKey? key = null;
            InodeRecord? parent = null;
            if (!request.Handle.IsEmpty)
            {
                HandleRecord h = VolumeStateMachine.RequireWriterHandle(State, new ApplyContext(0, 0, new CloseHandleCommand(0, 0) { Op = op }),
                    request.Handle.Value, request.Generation);
                key = h.Key;
            }
            else
            {
                (string filePath, string? streamName) = DfsStreamPath.Split(path);
                Resolution res = new NamespaceView(State.Inodes).Resolve(filePath);
                parent = res.Parent;
                if (res.Found)
                    key = new StreamKey(res.Target!.Id, streamName ?? DfsStreamPath.DefaultStreamName);
            }

            IDisposable? hold = key is { } k ? await LockStreamAsync(k, ct) : null;
            try
            {
                InodeRecord? inode = key is { } target ? State.Find(target.Inode) : null;
                StreamRecord? stream = key is { } target2 ? inode?.GetStream(target2.Stream) : null;
                string policyName = inode?.PolicyName ?? request.StoragePolicy ?? parent?.PolicyName ?? string.Empty;
                (DfsStoragePolicy policy, string pool) = PolicyOf(policyName);
                string faultPath = path.Length > 0 ? path : PathForFaults(State, key?.Inode ?? 0, key?.Stream ?? string.Empty);
                ImmutableArray<StreamChange> relative = await ValidateChangesAsync(request.Changes, request.Receipts, request.Staging, policy, pool,
                    DfsOperationKind.Append, op.Id, faultPath, ct);
                long oldLength = stream?.Length ?? 0;
                if (oldLength > long.MaxValue - request.AppendLength)
                    throw new ArgumentException("The append overflows the maximum file length.");
                ImmutableArray<StreamChange> absolute = [.. relative.Select(c => c with { Offset = c.Offset + oldLength })];
                PublishPlan plan = await PlanAsync(stream, -1, absolute, oldLength + request.AppendLength, policy, pool, request.Staging, op, ct);
                bool changed = plan.LeaderStaging != 0 || plan.Derived != 0 || !plan.Changes.SequenceEqual(absolute);
                PublishSpec spec;
                bool isAbsolute;
                if (!changed && !plan.FinalInline)
                {
                    spec = new PublishSpec(-1, request.AppendLength, relative, false, 0, request.Staging.Value, 0) { Policy = policyName };
                    isAbsolute = false;
                }
                else
                {
                    spec = new PublishSpec(-1, oldLength + request.AppendLength, plan.Changes, plan.FinalInline, stream?.Revision ?? 0, request.Staging.Value,
                        plan.LeaderStaging) { Policy = policyName };
                    isAbsolute = true;
                }

                return new Prepared(Stamp(new AppendCommand(path, request.Handle.Value, request.Generation, spec, isAbsolute, request.CreateIfMissing,
                    request.StoragePolicy, request.SnapshotName), op), hold);
            }
            catch
            {
                hold?.Dispose();
                throw;
            }
        }, cleanup: true);
        CountStaleWrite(reply);
        return reply;
    }

    /// <summary>
    /// <c>Copy</c>: metadata-only copy of the source's current revision (objects shared) (one entry): a plain source copied to
    /// a new plain destination copies every stream; otherwise the addressed source stream replaces (or creates) the addressed
    /// destination stream only (ADS-04). Inline source streams are stored as objects first when the destination's policy is
    /// stronger than what inline metadata guarantees (LAY-002).
    /// </summary>
    public Task<IWireMessage> CopyAsync(RpcContext context, CopyMessage request)
    {
        string principal = context.Principal ?? string.Empty;
        string source = NormalizePath(request.Source);
        string destination = NormalizePath(request.Destination);
        RequireGrant(principal, DfsPermissions.Read | DfsPermissions.Write, "copy");
        if (request.StoragePolicy is { Length: > 0 } named && !Record.NamedPolicies.ContainsKey(named))
            throw new DfsException(DfsErrorCode.InvalidPolicy, $"Volume '{Name}' has no storage policy named '{named}'.");
        return MutateAsync(context, request, DfsOperationKind.Copy, async (op, ct) =>
        {
            var command = Stamp(new CopyCommand(source, destination, request.Overwrite, request.ExpectedSourceRevision, request.StoragePolicy,
                request.SnapshotName), op);
            var view = new NamespaceView(State.Inodes);
            (string sourceFile, string? sourceStream, string destinationFile, string? destinationStream) = VolumeStateMachine.SplitCopyPaths(source, destination);
            if (view.Resolve(sourceFile) is not { Found: true, Target: { } from })
                return command;
            Resolution target = view.Resolve(destinationFile);
            string? policyName = target is { Found: true, Target: { } existing } ? existing.PolicyName : request.StoragePolicy ?? from.PolicyName;
            (DfsStoragePolicy policy, string pool) = PolicyOf(policyName);
            bool wholeFile = sourceStream is null && destinationStream is null && !target.Found;
            string? only = wholeFile ? null : sourceStream ?? DfsStreamPath.DefaultStreamName;
            (ImmutableArray<StreamMaterialization> materialized, ulong staging) = await MaterializeInlineAsync(command, from, policy, pool, op, ct, only,
                replaceWeakExtents: true);
            return staging == 0 ? command : command with { Materialized = materialized, LeaderStaging = staging };
        }, cleanup: true);
    }

    /// <summary>
    /// Stores the inline streams of <paramref name="inode"/> as objects meeting <paramref name="policy"/> when inline metadata
    /// does not guarantee it (LAY-002), and with <paramref name="replaceWeakExtents"/> (a copy) also re-places every object
    /// whose known durable copies do not meet <paramref name="policy"/> inside <paramref name="pool"/> (a copy under a stronger
    /// policy or into another pool must not share objects that fail its durability or pool, DEC-10, POOL-02), under a new
    /// leader staging session; nothing when no stream needs it or <paramref name="command"/> would be refused anyway.
    /// </summary>
    private async Task<(ImmutableArray<StreamMaterialization> Materialized, ulong Staging)> MaterializeInlineAsync(VolumeCommand command, InodeRecord inode,
        DfsStoragePolicy policy, string pool, OperationHeader op, CancellationToken ct, string? onlyStream = null, bool replaceWeakExtents = false)
    {
        bool inlineAllowed = InlineAllowed(policy);
        KeyValuePair<string, StreamRecord>[] work = [.. inode.Streams.Where(p => (onlyStream is null || string.Equals(p.Key, onlyStream, StringComparison.Ordinal))
            && (p.Value is { IsInline: true, Length: > 0 } ? !inlineAllowed
                : replaceWeakExtents && !p.Value.IsInline && p.Value.Extents.Any(e => !MeetsPolicy(e, policy, pool))))];
        if (work.Length == 0 || !DryRun(command).IsSuccess)
            return ([], 0);
        ulong leaderStaging = await CreateLeaderStagingAsync(op, ct);
        var staging = new StagingId(leaderStaging);
        string sourcePool = PolicyOf(inode.PolicyName).Pool;
        ImmutableArray<StreamMaterialization>.Builder materialized = ImmutableArray.CreateBuilder<StreamMaterialization>(work.Length);
        foreach ((string name, StreamRecord stream) in work)
        {
            ImmutableList<ExtentRef>.Builder extents = ImmutableList.CreateBuilder<ExtentRef>();
            if (stream.IsInline)
            {
                for (long done = 0; done < stream.Length;)
                {
                    int chunk = (int)Math.Min(stream.Length - done, _s.Timings.MaxObjectSize);
                    StoredObject stored = await Io.StoreObjectAsync(Record, stream.Inline.Slice((int)done, chunk), staging, policy, pool, ct);
                    extents.Add(new ExtentRef(done, chunk, stored.Object, 0, stored.Length, stored.Checksum, ObjectEncodings.Plain, stored.PlacementEpoch,
                        staging, stored.Holders));
                    done += chunk;
                }
            }
            else
            {
                // The same layout over verified copies stored for the policy: every extent keeps its range and object offset.
                var replaced = new Dictionary<ObjectId, StoredObject>();
                foreach (ExtentRef e in stream.Extents)
                {
                    if (MeetsPolicy(e, policy, pool))
                    {
                        extents.Add(e);
                        continue;
                    }

                    if (!replaced.TryGetValue(e.Object, out StoredObject? stored))
                    {
                        byte[] data = await Io.ReadObjectAsync(Record, e.Object, e.Checksum, e.ObjectLength, LocationsOf(e), sourcePool, ct);
                        stored = await Io.StoreObjectAsync(Record, data, staging, policy, pool, ct);
                        replaced[e.Object] = stored;
                    }

                    extents.Add(e with
                    {
                        Object = stored.Object,
                        ObjectLength = stored.Length,
                        Checksum = stored.Checksum,
                        Encoding = ObjectEncodings.Plain,
                        PlacementEpoch = stored.PlacementEpoch,
                        Staging = staging,
                        Locations = stored.Holders,
                    });
                }
            }

            materialized.Add(new StreamMaterialization(name, stream.Revision, extents.ToImmutable()));
        }

        return (materialized.MoveToImmutable(), leaderStaging);
    }

    /// <summary>
    /// True when the known durable holders of an extent's object (receipt holders at publication and inventories) that are live
    /// members of <paramref name="pool"/> meet the policy's acknowledgement copies and zones.
    /// </summary>
    private bool MeetsPolicy(ExtentRef extent, DfsStoragePolicy policy, string pool)
    {
        ControlState control = _s.Control.State;
        HashSet<NodeId> members = [.. control.PoolMembers(pool).Where(m => m.IsLive).Select(m => m.Id)];
        DfsTopologyLabels[] labels = [.. LocationsOf(extent).Where(members.Contains).Select(n => control.FindMember(n)?.Labels).OfType<DfsTopologyLabels>()];
        (int copies, int zones) = Tedd.Tdfs.Data.Placement.CountDomains(labels);
        return copies >= policy.DurableCopiesBeforeAck && zones >= policy.DurableZonesBeforeAck;
    }

    private void CountStaleWrite(IWireMessage reply)
    {
        if (reply is ErrorReplyMessage { Code: DfsErrorCode.LeaseLost })
            _s.Metrics.RejectedStaleWrite();
    }

    /// <summary>The storage policy (and its pool) of a policy name; unknown or empty names use the volume default.</summary>
    private (DfsStoragePolicy Policy, string Pool) PolicyOf(string? name)
    {
        VolumeRecord record = Record;
        DfsStoragePolicy policy = name is { Length: > 0 } && record.NamedPolicies.TryGetValue(name, out DfsStoragePolicy? named) ? named : record.DefaultPolicy;
        return (policy, policy.Pool ?? record.StoragePool);
    }

    private static string PathForFaults(VolumeState s, ulong inode, string stream)
    {
        string? path = new NamespaceView(s.Inodes).PathOf(inode);
        return path is null ? string.Empty : DfsStreamPath.Combine(path, stream.Length == 0 ? null : stream);
    }

    /// <summary>
    /// Validates the changes of a publication and the receipts of every referenced object before proposing (PROTOCOL.md
    /// §7.1 step 4): structure, receipt MAC/binding/incarnation/epoch/pool and the policy's durable copies and zones (F09 per
    /// object). The result carries the receipt holders as location hints.
    /// </summary>
    private async Task<ImmutableArray<StreamChange>> ValidateChangesAsync(IReadOnlyList<RangeChange> changes, IReadOnlyList<ObjectReceipt> receipts,
        StagingId staging, DfsStoragePolicy policy, string pool, DfsOperationKind kind, DfsOperationId opId, string path, CancellationToken ct)
    {
        var result = ImmutableArray.CreateBuilder<StreamChange>(changes.Count);
        var objects = new SortedDictionary<ObjectId, RangeChange>(ObjectIdComparer.Instance);
        foreach (RangeChange c in changes)
        {
            if (c.Kind == RangeChangeKind.Data)
            {
                if (staging.IsEmpty)
                    throw new ArgumentException("Data changes need the staging session their objects were uploaded under.");
                if (objects.TryGetValue(c.Object, out RangeChange? first)
                    && (first.Checksum != c.Checksum || first.ObjectLength != c.ObjectLength || first.Encoding != c.Encoding))
                {
                    throw new ArgumentException($"Changes describe object {c.Object} inconsistently.");
                }

                objects.TryAdd(c.Object, c);
            }
        }

        var holders = new Dictionary<ObjectId, (ImmutableArray<NodeId> Holders, ulong Epoch)>();
        int occurrence = 0;
        ControlState control = _s.Control.State;
        foreach ((ObjectId obj, RangeChange c) in objects)
        {
            ReceiptEvaluation evaluation = Receipts.Evaluate(receipts, _s.Keys.ReceiptKey, control, obj, c.Checksum, c.ObjectLength, c.Encoding, Record.Id,
                staging, pool);
            await _s.HitAsync(FaultStage.F09, FaultSites.CommitReceiptValidated, Name, kind, opId.IsEmpty ? null : opId, path, obj, occurrence++, ct: ct);
            if (!_s.Mutations.CommitMetadataBeforeData
                && (evaluation.Copies < policy.DurableCopiesBeforeAck || evaluation.Zones < policy.DurableZonesBeforeAck))
            {
                throw new DfsPlacementUnavailableException(
                    $"Object {obj} has {evaluation.Copies} durable copies in {evaluation.Zones} zones; the policy requires {policy.DurableCopiesBeforeAck} in {policy.DurableZonesBeforeAck}.",
                    policy.DurableCopiesBeforeAck, evaluation.Copies, policy.DurableZonesBeforeAck, evaluation.Zones);
            }

            ulong epoch = receipts.FirstOrDefault(r => r.Object == obj && evaluation.Holders.Contains(r.Node))?.PlacementEpoch ?? control.PlacementEpoch;
            holders[obj] = ([.. evaluation.Holders], epoch);
        }

        foreach (RangeChange c in changes)
        {
            StreamChange change = c.Kind switch
            {
                RangeChangeKind.Data => new StreamChange(c.Offset, c.Length, RangeChangeKind.Data,
                    new ExtentRef(c.Offset, c.Length, c.Object, c.ObjectOffset, c.ObjectLength, c.Checksum, c.Encoding, holders[c.Object].Epoch, staging,
                        holders[c.Object].Holders), ReadOnlyMemory<byte>.Empty),
                RangeChangeKind.Zero => new StreamChange(c.Offset, c.Length, RangeChangeKind.Zero, null, ReadOnlyMemory<byte>.Empty),
                RangeChangeKind.Inline => new StreamChange(c.Offset, c.Length, RangeChangeKind.Inline, null, c.InlineData.ToArray()),
                _ => throw new ArgumentException($"Unknown change kind {(int)c.Kind}."),
            };
            if (ContentAlgebra.Validate(change) is { } error)
                throw new ArgumentException(error);
            result.Add(change);
        }

        return result.MoveToImmutable();
    }

    /// <summary>
    /// True when the metadata group's guaranteed durability satisfies <paramref name="policy"/> (LAY-002): every majority of
    /// the voters spans at least the policy's acknowledgement hosts and zones, so inline bytes are as durable as the policy
    /// requires of objects.
    /// </summary>
    private bool InlineAllowed(DfsStoragePolicy policy)
    {
        ControlState control = _s.Control.State;
        var labels = Group.Conf.Voters.Select(v => control.FindMember(v)?.Labels).Where(l => l is not null).Select(l => l!).ToArray();
        if (labels.Length == 0)
            return false;
        int majority = (labels.Length / 2) + 1;
        int minHosts = int.MaxValue;
        int minZones = int.MaxValue;
        foreach (int[] subset in Subsets(labels.Length, majority))
        {
            (int hosts, int zones) = Tedd.Tdfs.Data.Placement.CountDomains(subset.Select(i => labels[i]));
            minHosts = Math.Min(minHosts, hosts);
            minZones = Math.Min(minZones, zones);
        }

        return minHosts >= policy.DurableCopiesBeforeAck && minZones >= policy.DurableZonesBeforeAck;
    }

    private static IEnumerable<int[]> Subsets(int n, int k)
    {
        var indices = new int[k];
        for (int i = 0; i < k; i++)
            indices[i] = i;
        while (true)
        {
            yield return (int[])indices.Clone();
            int j = k - 1;
            while (j >= 0 && indices[j] == n - k + j)
                j--;
            if (j < 0)
                yield break;
            indices[j]++;
            for (int i = j + 1; i < k; i++)
                indices[i] = indices[i - 1] + 1;
        }
    }

    /// <summary>
    /// Chooses the representation of a publication (inline iff the final length fits the threshold and the metadata group's
    /// durability satisfies the policy, LAY-002) and materializes what that needs: object bytes are read back into inline
    /// content (extents → inline), inline bytes are stored as objects meeting the policy (inline → extents, PROTOCOL.md §8.1).
    /// </summary>
    private async Task<PublishPlan> PlanAsync(StreamRecord? baseStream, long truncateTo, ImmutableArray<StreamChange> changes, long newLength,
        DfsStoragePolicy policy, string pool, StagingId requestStaging, OperationHeader op, CancellationToken ct)
    {
        bool inline = newLength <= State.Config.InlineThreshold && InlineAllowed(policy);
        if (ContentAlgebra.Plan(baseStream, truncateTo, changes, newLength, out List<IContentSegment>? small, out ImmutableList<ExtentRef>.Builder? large,
                out string? error) is { } failure)
        {
            throw DfsErrors.ToException(failure, error ?? "Invalid publication.");
        }

        if (large is not null)
        {
            if (!inline || large.Count == 0)
                return new PublishPlan(changes, truncateTo, inline, 0, 0);
            byte[] content = await ReadContentAsync(large, newLength, pool, ct);
            return new PublishPlan([new StreamChange(0, newLength, RangeChangeKind.Inline, null, content)], 0, true, baseStream?.Revision ?? 0, 0);
        }

        List<IContentSegment> segments = small!;
        bool hasObjects = segments.Any(seg => seg is ExtentRef);
        bool hasInline = segments.Any(seg => seg is InlineSegment);
        if (inline)
        {
            if (!hasObjects)
                return new PublishPlan(changes, truncateTo, true, baseStream is { IsInline: true } ? baseStream.Revision : 0, 0);
            var content = new byte[newLength];
            foreach (IContentSegment seg in segments)
            {
                if (seg is InlineSegment bytes)
                    bytes.Data.Span.CopyTo(content.AsSpan((int)bytes.Offset));
                else if (seg is ExtentRef extent)
                    (await ReadExtentAsync(extent, pool, ct)).CopyTo(content.AsSpan((int)extent.Offset));
            }

            return new PublishPlan([new StreamChange(0, newLength, RangeChangeKind.Inline, null, content)], 0, true, baseStream?.Revision ?? 0, 0);
        }

        if (!hasInline)
            return new PublishPlan(changes, truncateTo, false, 0, 0);

        // Inline bytes must become objects meeting the policy, under an open staging session of the caller.
        ulong leaderStaging = 0;
        StagingId staging = requestStaging;
        if (staging.IsEmpty || !State.Staging.TryGetValue(staging.Value, out StagingRecord? record) || !record.IsOpen
            || !string.Equals(record.Principal, op.Principal, StringComparison.Ordinal))
        {
            leaderStaging = await CreateLeaderStagingAsync(op, ct);
            staging = new StagingId(leaderStaging);
        }

        ImmutableArray<StreamChange>.Builder result = changes.ToBuilder();
        foreach (InlineSegment seg in segments.OfType<InlineSegment>())
        {
            for (long done = 0; done < seg.Length;)
            {
                int chunk = (int)Math.Min(seg.Length - done, _s.Timings.MaxObjectSize);
                ReadOnlyMemory<byte> bytes = seg.Data.Slice((int)done, chunk);
                StoredObject stored = await Io.StoreObjectAsync(Record, bytes, staging, policy, pool, ct);
                var extent = new ExtentRef(seg.Offset + done, chunk, stored.Object, 0, stored.Length, stored.Checksum, ObjectEncodings.Plain,
                    stored.PlacementEpoch, staging, stored.Holders);
                result.Add(new StreamChange(seg.Offset + done, chunk, RangeChangeKind.Data, extent, ReadOnlyMemory<byte>.Empty));
                done += chunk;
            }
        }

        bool fromBase = baseStream is { IsInline: true, Length: > 0 };
        return new PublishPlan(result.ToImmutable(), truncateTo, false, fromBase ? baseStream!.Revision : 0, leaderStaging);
    }

    /// <summary>Proposes a staging session owned by the caller for leader-materialized objects (closed by the publication).</summary>
    private async Task<ulong> CreateLeaderStagingAsync(OperationHeader op, CancellationToken ct)
    {
        OperationHeader header = op with { Id = default, PayloadHash = UInt128.Zero, Operation = DfsOperationKind.CreateStaging };
        VolumeApplyResult result = await ProposeAsync(Stamp(new CreateStagingCommand(0, 0, default), header), ct);
        if (!result.IsSuccess || result.Value is not StagingRecord staging)
            throw DfsErrors.ToException(result.Code, result.Message);
        return staging.Id;
    }

    private async Task<byte[]> ReadContentAsync(IEnumerable<ExtentRef> extents, long length, string pool, CancellationToken ct)
    {
        var content = new byte[length];
        foreach (ExtentRef extent in extents)
            (await ReadExtentAsync(extent, pool, ct)).CopyTo(content.AsSpan((int)extent.Offset));
        return content;
    }

    private async Task<byte[]> ReadExtentAsync(ExtentRef extent, string pool, CancellationToken ct)
    {
        byte[] whole = await Io.ReadObjectAsync(Record, extent.Object, extent.Checksum, extent.ObjectLength, LocationsOf(extent), pool, ct);
        return whole.AsSpan((int)extent.ObjectOffset, (int)extent.Length).ToArray();
    }
}
