using Tedd.Tdfs.Data;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Control;
using Tedd.Tdfs.Server.Runtime;
using Tedd.Tdfs.Storage;

namespace Tedd.Tdfs.Server.Data;

/// <summary>
/// Storage-node side of repair (<c>ReplicateObject</c>, PROTOCOL.md §9.7): a volume leader asks this node to hold a
/// verified copy of an object. An existing copy is re-verified (every unit and the whole-object checksum) and answered
/// with a fresh receipt; a damaged one is quarantined and replaced. Otherwise the node-wide repair rate limit is charged,
/// the object is pulled from the listed sources (node-authenticated <c>GetObject</c>, first source that serves bytes
/// matching the expected length and checksum) and the copy is stored durably under repair admission (it may use the
/// repair part of the reserve) before the receipt is issued. Concurrent requests for the same object share one execution.
/// Runs on the node loop.
/// </summary>
internal sealed class RepairWorker
{
    private readonly NodeServices _s;
    private readonly ByteRateLimiter _rate;
    private readonly Dictionary<ObjectId, Task<ReplicateObjectResultMessage>> _running = [];

    /// <summary>Sources whose pull did not answer within the replication deadline, and when (tried last for a while).</summary>
    private readonly Dictionary<NodeId, long> _slowSources = [];

    public RepairWorker(NodeServices services)
    {
        ArgumentNullException.ThrowIfNull(services);
        _s = services;
        _rate = new ByteRateLimiter(services, services.Timings.RepairBytesPerSecond);
    }

    private ObjectStore Store => _s.ObjectStore;

    /// <summary>Registers the <c>ReplicateObject</c> handler (node peers only).</summary>
    public void Register(RpcDispatcher rpc) => rpc.Register<ReplicateObjectMessage>(HandleAsync, RpcAccess.Node);

    private async ValueTask<IWireMessage> HandleAsync(RpcContext context, IWireMessage request)
        => await ReplicateAsync((ReplicateObjectMessage)request, context.Cancellation);

    /// <summary>Replicates one object onto this node (also used by a volume leader for its own node, without an RPC).</summary>
    public Task<ReplicateObjectResultMessage> ReplicateAsync(ReplicateObjectMessage message, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(message);
        if (_running.TryGetValue(message.Object, out Task<ReplicateObjectResultMessage>? running))
            return running.WaitAsync(ct);
        Task<ReplicateObjectResultMessage> task = RunAsync(message);
        if (!task.IsCompleted)
            _running[message.Object] = task;
        return task.WaitAsync(ct);
    }

    /// <summary>How long one replication may take on this node (the requesting leader waits as long).</summary>
    private TimeSpan ReplicationDeadline => _s.Peers.DefaultCallTimeout * 4;

    private async Task<ReplicateObjectResultMessage> RunAsync(ReplicateObjectMessage message)
    {
        try
        {
            using CancellationTokenSource deadline = _s.CreateDeadline(ReplicationDeadline);
            return await ReplicateCoreAsync(message, deadline.Token);
        }
        finally
        {
            _running.Remove(message.Object);
        }
    }

    private async Task<ReplicateObjectResultMessage> ReplicateCoreAsync(ReplicateObjectMessage m, CancellationToken ct)
    {
        VolumeRecord volume = _s.Control.State.FindVolume(m.Volume)
            ?? throw new DfsException(DfsErrorCode.VolumeNotFound, $"Volume {m.Volume} does not exist.");
        if (m.Object.IsEmpty)
            throw new ArgumentException("The object id must not be zero.");
        if (m.Encoding != ObjectEncodings.Plain)
            throw new NotSupportedException($"Object encoding {m.Encoding} is not supported.");
        if (m.Length < 0 || m.Length > _s.Timings.MaxObjectSize)
            throw new DfsLimitExceededException("MaxObjectSize", _s.Timings.MaxObjectSize, m.Length);
        if (!_s.Control.IsSelfRegistered)
        {
            throw new DfsUnavailableException(DfsErrorCode.NodeUnavailable,
                $"{_s.NodeName} has not registered incarnation {_s.Incarnation} yet and accepts no repair copies (RCV-02).");
        }

        if (Store.TryGetInfo(m.Object) is { } held)
        {
            if (held.Volume != m.Volume || held.Checksum != m.Checksum || held.Length != m.Length)
                throw new DfsIntegrityException(m.Object, $"Object {m.Object} is held here with different content or owner; objects are immutable.");
            if (await Store.VerifyAsync(m.Object, ct))
            {
                ObjectReceipt existing = _s.Objects.IssueReceipt(m.Object, volume, held.Staging, held.Checksum, held.Length, ObjectEncodings.Plain, m.PlacementEpoch);
                return new ReplicateObjectResultMessage(existing, AlreadyPresent: true);
            }

            // The local copy was damaged: it is quarantined now and no longer counts; replace it from a healthy source.
            _s.Metrics.ChecksumFailure();
            _s.Metrics.SetStored(Store.ObjectCount, Store.LiveBytes);
            _s.Maintenance?.Inventory.RequestReport();
        }

        // Refuse before pulling anything when even the repair reserve cannot take the copy.
        long available = Store.AvailableBytes(ObjectAdmission.Repair);
        if (available < m.Length + 4096)
        {
            throw new DfsCapacityException(
                $"{_s.NodeName} cannot admit a {m.Length}-byte repair copy: {available} bytes are available above its metadata reserve.");
        }

        // The budget is charged before the transfer: a congested node never requests more than its repair rate.
        await _rate.TakeAsync(m.Length, ct);
        byte[] data = await FetchAsync(m, volume, ct);
        (ObjectReceipt receipt, bool alreadyPresent) = await _s.Objects.StoreAsync(m.Object, volume, m.Staging, m.Checksum, m.Length, ObjectEncodings.Plain,
            m.PlacementEpoch, data, DfsOperationKind.Repair, ObjectAdmission.Repair, ct);
        if (!alreadyPresent)
            _s.Metrics.RepairCopied(data.Length);
        return new ReplicateObjectResultMessage(receipt, alreadyPresent);
    }

    /// <summary>
    /// The object's bytes from the first source serving exactly the expected length and checksum. A source that fails or
    /// serves wrong bytes is skipped; a slow one is waited for within the replication deadline (abandoning it for the next
    /// source would only add a second transfer to a congested path). A source that did not answer in time is tried last by
    /// the following replications for two deadlines, so an unreachable source delays one attempt, not every retry.
    /// </summary>
    private async Task<byte[]> FetchAsync(ReplicateObjectMessage m, VolumeRecord volume, CancellationToken ct)
    {
        Exception? last = null;
        var tried = new HashSet<NodeId>();
        long now = _s.Time.GetTimestamp();
        foreach (NodeId source in m.Sources.OrderBy(source => IsSlow(source, now) ? 1 : 0))
        {
            if (source == _s.NodeId || !tried.Add(source))
                continue;
            if (_s.Control.State.FindMember(source) is not { IsLive: true })
                continue;
            try
            {
                var reply = (GetObjectResultMessage)await _s.Peers.CallAsync(source,
                    new GetObjectMessage(ReadOnlyMemory<byte>.Empty, volume.Id, m.Object, 0, m.Length), ReplicationDeadline, ct);
                if (reply.Offset == 0 && reply.Data.Length == m.Length && reply.ObjectLength == m.Length
                    && ObjectChecksum.Compute(reply.Data.Span) == m.Checksum)
                {
                    _slowSources.Remove(source);
                    return reply.Data.ToArray();
                }

                last = new DfsIntegrityException(m.Object, $"Object {m.Object} served by {_s.Control.NameOf(source)} does not match its expected length and checksum.");
            }
            catch (Exception ex) when (ex is DfsException or IOException or TimeoutException or OperationCanceledException or UnauthorizedAccessException
                                           or InvalidCastException)
            {
                if (ex is TimeoutException or OperationCanceledException)
                    _slowSources[source] = _s.Time.GetTimestamp();
                if (ct.IsCancellationRequested)
                    throw;
                last = ex;
            }
        }

        throw new DfsIntegrityException(m.Object,
            $"No source served a verified copy of object {m.Object}." + (last is null ? string.Empty : " Last error: " + last.Message));
    }

    private bool IsSlow(NodeId source, long now)
    {
        if (!_slowSources.TryGetValue(source, out long at))
            return false;
        if (_s.Time.GetElapsedTime(at, now) < ReplicationDeadline * 2)
            return true;
        _slowSources.Remove(source);
        return false;
    }
}
