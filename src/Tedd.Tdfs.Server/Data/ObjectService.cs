using Tedd.Tdfs.Data;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Control;
using Tedd.Tdfs.Server.Runtime;
using Tedd.Tdfs.Storage;

namespace Tedd.Tdfs.Server.Data;

/// <summary>
/// Storage-node RPCs of the data plane (PROTOCOL.md §7, §9.7): <c>PutObject</c> (staging capability, durable write, signed
/// receipt only after the flush), <c>GetObject</c> (per-object read capability, verified ranges with unit checksums),
/// <c>LocateObject</c>, and the node-only <c>DeleteObjects</c> and <c>CopyObjects</c>.
/// </summary>
internal sealed class ObjectService
{
    private readonly NodeServices _s;

    public ObjectService(NodeServices services, Capabilities capabilities)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(capabilities);
        _s = services;
        Capabilities = capabilities;
    }

    /// <summary>Capability issuing and verification.</summary>
    public Capabilities Capabilities { get; }

    private ObjectStore Store => _s.ObjectStore;

    /// <summary>Registers the handlers.</summary>
    public void Register(RpcDispatcher rpc)
    {
        rpc.Register<PutObjectMessage>(PutObjectAsync, RpcAccess.NodeOrClient);
        rpc.Register<GetObjectMessage>(GetObjectAsync, RpcAccess.NodeOrClient);
        rpc.Register<LocateObjectMessage>(LocateObjectAsync, RpcAccess.NodeOrClient);
        rpc.Register<DeleteObjectsMessage>(DeleteObjectsAsync, RpcAccess.Node);
        rpc.Register<CopyObjectsMessage>(CopyObjectsAsync, RpcAccess.Node);
    }

    /// <summary>Stores an object durably and returns a receipt (also used by local callers such as repair and restore).</summary>
    public async Task<(ObjectReceipt Receipt, bool AlreadyPresent)> StoreAsync(ObjectId obj, VolumeRecord volume, StagingId staging, UInt128 checksum,
        long length, byte encoding, ulong placementEpoch, ReadOnlyMemory<byte> data, DfsOperationKind operation, CancellationToken ct)
    {
        if (!_s.Control.IsSelfRegistered)
        {
            throw new DfsUnavailableException(DfsErrorCode.NodeUnavailable,
                $"{_s.NodeName} has not registered incarnation {_s.Incarnation} yet and issues no receipts (RCV-02).");
        }

        if (obj.IsEmpty)
            throw new ArgumentException("The object id must not be zero.");
        if (encoding != ObjectEncodings.Plain)
            throw new NotSupportedException($"Object encoding {encoding} is not supported.");
        if (length != data.Length)
            throw new DfsIntegrityException(obj, $"Object {obj}: the declared length {length} does not match the {data.Length} bytes received.");
        if (data.Length > _s.Timings.MaxObjectSize)
            throw new DfsLimitExceededException("MaxObjectSize", _s.Timings.MaxObjectSize, data.Length);
        if (ObjectChecksum.Compute(data.Span) != checksum)
        {
            _s.Metrics.ChecksumFailure();
            throw new DfsIntegrityException(obj, $"Object {obj}: the payload does not match its declared checksum.");
        }

        bool alreadyPresent = Store.TryGetInfo(obj) is { } existing && existing.Volume == volume.Id && existing.Staging == staging
            && existing.Checksum == checksum && existing.Length == length;
        await Store.PutAsync(obj, volume.Id, staging, data, operation, ct);
        await _s.HitAsync(FaultStage.F08, FaultSites.ObjectAfterFlushBeforeReceipt, volume.Name, operation, obj: obj, ct: ct);
        _s.Metrics.ObjectPut(data.Length);
        _s.Metrics.SetStored(Store.ObjectCount, Store.LiveBytes);
        ObjectReceipt receipt = Receipts.Issue(_s.Keys.ReceiptKey, obj, volume.Id, staging, checksum, length, encoding, _s.NodeId, _s.Incarnation,
            placementEpoch, _s.Time.GetUtcNow());
        _s.Metrics.ReceiptIssued();
        return (receipt, alreadyPresent);
    }

    private async ValueTask<IWireMessage> PutObjectAsync(RpcContext context, IWireMessage request)
    {
        var message = (PutObjectMessage)request;
        VolumeRecord? found = _s.Control.State.FindVolume(message.Volume);
        if (!context.IsNode || !message.Capability.IsEmpty)
        {
            if (found is null)
                throw new UnauthorizedAccessException("Access denied: the capability does not authorize this volume.");
            Capabilities.Verify(message.Capability.Span, DataCapabilityKind.Staging, found, context.Principal ?? string.Empty, staging: message.Staging);
        }

        VolumeRecord volume = found ?? throw new DfsException(DfsErrorCode.VolumeNotFound, $"Volume {message.Volume} does not exist.");
        (ObjectReceipt receipt, bool alreadyPresent) = await StoreAsync(message.Object, volume, message.Staging, message.Checksum, message.Length,
            message.Encoding, message.PlacementEpoch, message.Data, DfsOperationKind.PutObject, context.Cancellation);
        return new PutObjectResultMessage(receipt, alreadyPresent);
    }

    private async ValueTask<IWireMessage> GetObjectAsync(RpcContext context, IWireMessage request)
    {
        var message = (GetObjectMessage)request;
        VolumeRecord? volume = _s.Control.State.FindVolume(message.Volume);
        if (!context.IsNode || !message.Capability.IsEmpty)
        {
            if (volume is null)
                throw new UnauthorizedAccessException("Access denied: the capability does not authorize this volume.");
            Capabilities.Verify(message.Capability.Span, DataCapabilityKind.Read, volume, context.Principal ?? string.Empty, obj: message.Object);
        }

        if (message.Offset < 0 || message.Length < 0)
            throw new ArgumentException("Offset and length must not be negative.");
        ObjectRecordInfo info = Store.TryGetInfo(message.Object) is { } found && found.Volume == message.Volume
            ? found
            : throw new DfsObjectMissingException(message.Object);

        long objectLength = info.Length;
        if (message.Offset >= objectLength)
            return new GetObjectResultMessage(objectLength, info.Checksum, ObjectEncodings.Plain, message.Offset, ReadOnlyMemory<byte>.Empty, 0, []);
        long requested = Math.Min(message.Length, objectLength - message.Offset);
        long end = message.Offset + requested;
        int unit = ObjectStore.UnitSize;
        long start = message.Offset / unit * unit;
        long alignedEnd = Math.Min(objectLength, (end + unit - 1) / unit * unit);
        var data = new byte[alignedEnd - start];
        int read;
        try
        {
            read = await Store.ReadAsync(message.Object, start, data, context.Cancellation);
        }
        catch (DfsIntegrityException) when (Store.TryGetInfo(message.Object) is not null)
        {
            _s.Metrics.ChecksumFailure();
            throw;
        }

        if (read != data.Length)
            throw new DfsIntegrityException(message.Object, $"Object {message.Object} returned {read} of {data.Length} bytes.");
        var checksums = new uint[(data.Length + unit - 1) / unit];
        for (int i = 0; i < checksums.Length; i++)
        {
            int offset = i * unit;
            checksums[i] = ObjectChecksum.Crc32C(data.AsSpan(offset, Math.Min(unit, data.Length - offset)));
        }

        _s.Metrics.ObjectRead(requested);
        return new GetObjectResultMessage(objectLength, info.Checksum, ObjectEncodings.Plain, start, data, unit, checksums);
    }

    private ValueTask<IWireMessage> LocateObjectAsync(RpcContext context, IWireMessage request)
    {
        var message = (LocateObjectMessage)request;
        if (!context.IsNode)
        {
            VolumeRecord volume = RequireVolume(message.Volume);
            string principal = context.Principal ?? string.Empty;
            if ((_s.Control.State.GetPermissions(principal, volume.Id) & DfsPermissions.Read) == 0)
                throw new UnauthorizedAccessException($"Principal '{principal}' may not read volume '{volume.Name}'.");
        }

        var locations = new ObjectLocation[message.Objects.Count];
        for (int i = 0; i < locations.Length; i++)
        {
            ObjectId obj = message.Objects[i];
            locations[i] = Store.TryGetInfo(obj) is { } info && info.Volume == message.Volume
                ? new ObjectLocation(obj, true, info.Length, info.Checksum, info.Staging)
                : new ObjectLocation(obj, false, 0, UInt128.Zero, default);
        }

        return new ValueTask<IWireMessage>(new LocateObjectResultMessage(locations));
    }

    private async ValueTask<IWireMessage> DeleteObjectsAsync(RpcContext context, IWireMessage request)
    {
        var message = (DeleteObjectsMessage)request;
        if (_s.Control.State.FindVolume(message.Volume) is { } volume && message.VolumeEpoch < volume.Epoch)
            throw new DfsEpochMismatchException($"The delete decision is for epoch {message.VolumeEpoch}; volume '{volume.Name}' is at epoch {volume.Epoch}.");
        string? volumeName = _s.Control.State.FindVolume(message.Volume)?.Name;
        int deleted = 0;
        int absent = 0;
        long bytes = 0;
        foreach (ObjectStagingRef target in message.Objects)
        {
            if (Store.TryGetInfo(target.Object) is not { } info || info.Volume != message.Volume || info.Staging != target.Staging)
            {
                absent++;
                continue;
            }

            if (message.ExcessReplicas)
            {
                await _s.HitAsync(FaultStage.F20, FaultSites.ReplicaDelete, volumeName, DfsOperationKind.Repair, obj: target.Object,
                    ct: context.Cancellation);
            }

            await Store.DeleteAsync(target.Object, context.Cancellation);
            deleted++;
            bytes += info.Length;
        }

        if (!message.ExcessReplicas && deleted > 0)
            _s.Metrics.GcDeleted(deleted, bytes);
        _s.Metrics.SetStored(Store.ObjectCount, Store.LiveBytes);
        return new DeleteObjectsResultMessage(deleted, absent);
    }

    private async ValueTask<IWireMessage> CopyObjectsAsync(RpcContext context, IWireMessage request)
    {
        var message = (CopyObjectsMessage)request;
        VolumeRecord target = RequireVolume(message.TargetVolume);
        var receipts = new List<ObjectReceipt>();
        var missing = new List<ObjectId>();
        foreach (ObjectCopy copy in message.Copies)
        {
            if (Store.TryGetInfo(copy.From) is not { } source || source.Volume != message.SourceVolume)
            {
                missing.Add(copy.From);
                continue;
            }

            var data = new byte[source.Length];
            int read = await Store.ReadAsync(copy.From, 0, data, context.Cancellation);
            if (read != data.Length)
                throw new DfsIntegrityException(copy.From, $"Object {copy.From} returned {read} of {data.Length} bytes.");
            (ObjectReceipt receipt, _) = await StoreAsync(copy.To, target, message.TargetStaging, source.Checksum, source.Length, ObjectEncodings.Plain,
                message.PlacementEpoch, data, DfsOperationKind.RestoreSnapshot, context.Cancellation);
            receipts.Add(receipt);
        }

        return new CopyObjectsResultMessage(receipts, missing);
    }

    private VolumeRecord RequireVolume(VolumeId id)
        => _s.Control.State.FindVolume(id) ?? throw new DfsException(DfsErrorCode.VolumeNotFound, $"Volume {id} does not exist.");
}
