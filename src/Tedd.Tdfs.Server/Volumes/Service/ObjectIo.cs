using System.Collections.Immutable;
using Tedd.Tdfs.Data;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Control;
using Tedd.Tdfs.Server.Data;
using Tedd.Tdfs.Server.Runtime;
using Tedd.Tdfs.Server.Volumes.State;

namespace Tedd.Tdfs.Server.Volumes.Service;

/// <summary>A durably stored object with the receipts that prove it (leader-side materialization).</summary>
/// <param name="Object">Object id.</param>
/// <param name="Checksum">XxHash128 of the payload.</param>
/// <param name="Length">Payload length.</param>
/// <param name="PlacementEpoch">Placement epoch the copies were placed under.</param>
/// <param name="Holders">Nodes holding trusted durable copies.</param>
internal sealed record StoredObject(ObjectId Object, UInt128 Checksum, long Length, ulong PlacementEpoch, ImmutableArray<NodeId> Holders);

/// <summary>
/// Leader-side data-plane work of the volume service (bulk data still never passes through the leader merely to reach
/// storage: only the leader's own materializations): reading verified object bytes (extent-to-inline conversion, restore)
/// and storing new objects with receipts meeting a policy (inline-to-extent conversion, restore copies). Uses node
/// authentication (no capabilities) and the same placement and receipt rules as clients (PROTOCOL.md §7).
/// </summary>
internal sealed class ObjectIo
{
    private readonly NodeServices _s;

    public ObjectIo(NodeServices services)
    {
        ArgumentNullException.ThrowIfNull(services);
        _s = services;
    }

    /// <summary>
    /// Reads one whole object (verified against its checksum) from the first holder that serves it: the local store, the
    /// listed locations, then every live member of <paramref name="pool"/>. Missing or corrupt everywhere →
    /// <see cref="DfsIntegrityException"/> (never zeroes).
    /// </summary>
    public async Task<byte[]> ReadObjectAsync(VolumeRecord volume, ObjectId obj, UInt128 checksum, long length, IEnumerable<NodeId> locations, string pool,
        CancellationToken ct)
    {
        var tried = new HashSet<NodeId>();
        var candidates = new List<NodeId>(locations);
        candidates.AddRange(_s.Control.State.PoolMembers(pool).Select(m => m.Id));
        if (_s.ObjectStore.TryGetInfo(obj) is { } local && local.Volume == volume.Id)
            candidates.Insert(0, _s.NodeId);
        Exception? last = null;
        foreach (NodeId node in candidates)
        {
            if (!tried.Add(node))
                continue;
            try
            {
                byte[] data = node == _s.NodeId ? await ReadLocalAsync(obj, length, ct) : await ReadRemoteAsync(node, volume, obj, length, ct);
                if (data.Length == length && ObjectChecksum.Compute(data) == checksum)
                    return data;
                last = new DfsIntegrityException(obj, $"Object {obj} on {_s.Control.NameOf(node)} failed verification.");
            }
            catch (Exception ex) when (ex is DfsException or IOException or TimeoutException or UnauthorizedAccessException or Transport.RpcConnectionException)
            {
                last = ex;
            }
        }

        throw new DfsIntegrityException(obj, $"No verified copy of object {obj} could be read." + (last is null ? string.Empty : " Last error: " + last.Message));
    }

    private async Task<byte[]> ReadLocalAsync(ObjectId obj, long length, CancellationToken ct)
    {
        var data = new byte[length];
        int read = await _s.ObjectStore.ReadAsync(obj, 0, data, ct);
        return read == length ? data : data[..read];
    }

    private async Task<byte[]> ReadRemoteAsync(NodeId node, VolumeRecord volume, ObjectId obj, long length, CancellationToken ct)
    {
        IWireMessage reply = await _s.Peers.CallAsync(node, new GetObjectMessage(ReadOnlyMemory<byte>.Empty, volume.Id, obj, 0, length), ct: ct);
        if (reply is ErrorReplyMessage error)
            throw error.ToException();
        var result = (GetObjectResultMessage)reply;
        if (result.Offset != 0)
            throw new DfsIntegrityException(obj, $"Object {obj}: unexpected range offset {result.Offset}.");
        return result.Data.ToArray();
    }

    /// <summary>
    /// Stores <paramref name="data"/> as a new object of <paramref name="volume"/> under <paramref name="staging"/> on the
    /// placement targets of <paramref name="policy"/> within <paramref name="pool"/> (falling back down the HRW order, never
    /// outside the pool), until the policy's target copies and its acknowledgement copies and zones are met.
    /// </summary>
    /// <param name="volume">Volume that owns the object.</param>
    /// <param name="data">Payload.</param>
    /// <param name="staging">Staging session of <paramref name="volume"/> the copies are bound to.</param>
    /// <param name="policy">Storage policy to satisfy.</param>
    /// <param name="pool">Pool the copies must stay in.</param>
    /// <param name="ct">Cancellation.</param>
    /// <param name="id">Object id to store under (a deterministic id makes retries idempotent); null = a new random id.</param>
    /// <param name="operation">Operation kind of the local store's fault context.</param>
    /// <exception cref="DfsPlacementUnavailableException">Fewer durable copies or zones than the policy requires.</exception>
    public async Task<StoredObject> StoreObjectAsync(VolumeRecord volume, ReadOnlyMemory<byte> data, StagingId staging, DfsStoragePolicy policy, string pool,
        CancellationToken ct, ObjectId? id = null, DfsOperationKind operation = DfsOperationKind.PutObject)
    {
        ControlState state = _s.Control.State;
        ulong epoch = state.PlacementEpoch;
        ObjectId obj = id ?? ObjectId.New(_s.Random);
        UInt128 checksum = ObjectChecksum.Compute(data.Span);
        IReadOnlyList<PlacementNode> nodes = state.PoolMembers(pool)
            .Select(m => new PlacementNode(m.Id, m.Name, m.Labels, m.IsPlacementEligible, Math.Max(0, m.CapacityBytes - m.UsedBytes)))
            .ToArray();
        IReadOnlyList<PlacementNode> order = Placement.FallbackOrder(obj, epoch, nodes);
        IReadOnlyList<PlacementNode> targets = Placement.SelectTargets(obj, epoch, policy, nodes);
        var sequence = targets.Concat(order.Where(n => !targets.Contains(n))).ToArray();
        var receipts = new List<ObjectReceipt>();
        ReceiptEvaluation evaluation = new(0, 0, []);
        int stored = 0;
        foreach (PlacementNode node in sequence)
        {
            if (!node.Eligible)
                continue;
            if (stored >= policy.TargetCopies && evaluation.Copies >= policy.DurableCopiesBeforeAck && evaluation.Zones >= policy.DurableZonesBeforeAck)
                break;
            try
            {
                ObjectReceipt receipt;
                if (node.Id == _s.NodeId)
                {
                    (receipt, _) = await _s.Objects.StoreAsync(obj, volume, staging, checksum, data.Length, ObjectEncodings.Plain, epoch, data,
                        operation, ct);
                }
                else
                {
                    IWireMessage reply = await _s.Peers.CallAsync(node.Id,
                        new PutObjectMessage(ReadOnlyMemory<byte>.Empty, volume.Id, staging, obj, checksum, data.Length, ObjectEncodings.Plain, epoch, data), ct: ct);
                    if (reply is ErrorReplyMessage error)
                        throw error.ToException();
                    receipt = ((PutObjectResultMessage)reply).Receipt;
                }

                receipts.Add(receipt);
                stored++;
                evaluation = Receipts.Evaluate(receipts, _s.Keys.ReceiptKey, _s.Control.State, obj, checksum, data.Length, ObjectEncodings.Plain, volume.Id,
                    staging, pool);
            }
            catch (Exception ex) when (ex is DfsException or IOException or TimeoutException or UnauthorizedAccessException or Transport.RpcConnectionException)
            {
                _s.Debug("volume", NodeServices.Inv($"Storing materialized object {obj} on {node.Name} failed: {ex.Message}"));
            }
        }

        if (evaluation.Copies < policy.DurableCopiesBeforeAck || evaluation.Zones < policy.DurableZonesBeforeAck)
        {
            throw new DfsPlacementUnavailableException(
                $"Object {obj} reached {evaluation.Copies} durable copies in {evaluation.Zones} zones; the policy requires {policy.DurableCopiesBeforeAck} in {policy.DurableZonesBeforeAck}.",
                policy.DurableCopiesBeforeAck, evaluation.Copies, policy.DurableZonesBeforeAck, evaluation.Zones);
        }

        return new StoredObject(obj, checksum, data.Length, epoch, [.. evaluation.Holders]);
    }
}
