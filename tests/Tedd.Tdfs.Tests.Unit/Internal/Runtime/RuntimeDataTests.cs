using Tedd.Tdfs.Data;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Server.Control;
using Tedd.Tdfs.Server.Data;
using Tedd.Tdfs.Server.Security;

namespace Tedd.Tdfs.Tests.Unit.Internal.Runtime;

/// <summary>Object service: staging/read capabilities, durable puts, signed receipts, verified reads.</summary>
public sealed class RuntimeDataTests
{
    [Fact]
    public async Task PutAndGetObject_RequireValidCapabilitiesAndIssueVerifiableReceipts()
    {
        await using var cluster = new RuntimeCluster(3, seed: 41);
        await cluster.StartAllAsync();
        await cluster.WaitForRegistrationAsync();
        VolumeEntry volume = await cluster.CreateVolumeAsync("vol");
        RuntimeNode storage = cluster[1];
        var keys = new KeyHierarchy(cluster.Secret);
        var random = new SeededRandom(99);
        DateTimeOffset expires = DateTimeOffset.UtcNow.AddMinutes(5);
        var staging = new StagingId(7);
        byte[] data = random.NextBytes(150_000);
        UInt128 checksum = ObjectChecksum.Compute(data);
        ObjectId obj = ObjectId.New(random);

        byte[] Staging(string principal, StagingId id, DateTimeOffset expiry, Guid? cluster2 = null, VolumeId? vol = null, ulong epoch = 1, byte[]? key = null)
            => DataCapability.ForStaging(cluster2 ?? cluster.ClusterId, vol ?? volume.Id, epoch, principal, id, expiry).Sign(key ?? keys.CapabilityKey).Encode();
        byte[] Read(string principal, IEnumerable<ObjectId> objects, DateTimeOffset expiry)
            => DataCapability.ForRead(cluster.ClusterId, volume.Id, 1, principal, objects, expiry).Sign(keys.CapabilityKey).Encode();
        PutObjectMessage Put(byte[] capability, ObjectId id, byte[] payload, UInt128? sum = null)
            => new(capability, volume.Id, staging, id, sum ?? ObjectChecksum.Compute(payload), payload.Length, ObjectEncodings.Plain, 1, payload);

        RawClient admin = await cluster.ConnectAsync(storage);
        RawClient reader = await cluster.ConnectAsync(storage, new DfsCredentials("reader", "reader-secret-1"));

        // Invalid staging capabilities are refused with AccessDenied and store nothing.
        var invalid = new (string Case, RawClient Client, byte[] Capability)[]
        {
            ("missing", admin, []),
            ("other principal", reader, Staging(RuntimeCluster.AdminPrincipal, staging, expires)),
            ("expired", admin, Staging(RuntimeCluster.AdminPrincipal, staging, DateTimeOffset.UtcNow.AddMinutes(-1))),
            ("other staging", admin, Staging(RuntimeCluster.AdminPrincipal, new StagingId(8), expires)),
            ("other cluster", admin, Staging(RuntimeCluster.AdminPrincipal, staging, expires, cluster2: Guid.NewGuid())),
            ("other epoch", admin, Staging(RuntimeCluster.AdminPrincipal, staging, expires, epoch: 2)),
            ("forged key", admin, Staging(RuntimeCluster.AdminPrincipal, staging, expires, key: new byte[32])),
            ("read kind", admin, Read(RuntimeCluster.AdminPrincipal, [obj], expires)),
            ("garbage", admin, [1, 2, 3]),
        };
        foreach ((string name, RawClient client, byte[] capability) in invalid)
        {
            var error = Assert.IsType<ErrorReplyMessage>(await client.CallAsync(Put(capability, obj, data)));
            Assert.True(error.Code == DfsErrorCode.AccessDenied, $"{name}: expected AccessDenied, got {error.Code} ({error.Message})");
        }

        Assert.False(await storage.Host!.Inspect.HasObjectAsync(obj));

        // A payload that does not match its declared checksum is refused.
        var corrupt = Assert.IsType<ErrorReplyMessage>(await admin.CallAsync(Put(Staging(RuntimeCluster.AdminPrincipal, staging, expires), obj, data,
            sum: checksum + 1)));
        Assert.Equal(DfsErrorCode.Integrity, corrupt.Code);

        // A valid put stores durably and returns a receipt whose MAC verifies with the cluster receipt key only.
        var put = await admin.CallOrThrowAsync<PutObjectResultMessage>(Put(Staging(RuntimeCluster.AdminPrincipal, staging, expires), obj, data));
        ObjectReceipt receipt = put.Receipt;
        Assert.False(put.AlreadyPresent);
        Assert.True(receipt.VerifyMac(keys.ReceiptKey));
        Assert.False(receipt.VerifyMac(new KeyHierarchy(new byte[32]).ReceiptKey));
        Assert.False((receipt with { Length = receipt.Length + 1 }).VerifyMac(keys.ReceiptKey));
        Assert.True(receipt.Covers(obj, checksum, data.Length, ObjectEncodings.Plain));
        Assert.Equal(volume.Id, receipt.Volume);
        Assert.Equal(staging, receipt.Staging);
        Assert.Equal(storage.Id, receipt.Node);
        Assert.Equal(storage.Host!.Incarnation, receipt.Incarnation);
        Assert.True(await storage.Host.Inspect.HasObjectAsync(obj));
        var objects = await storage.Host.Inspect.ListObjectsAsync("vol");
        Assert.Contains(objects, o => o.Id == obj && o.Staging == staging && o.Length == data.Length);

        // Idempotent re-upload of identical content.
        var again = await admin.CallOrThrowAsync<PutObjectResultMessage>(Put(Staging(RuntimeCluster.AdminPrincipal, staging, expires), obj, data));
        Assert.True(again.AlreadyPresent);

        // The receipt counts as one durable copy when evaluated against the control state.
        ControlState state = await storage.Host.InvokeAsync(s => s.Control.State);
        ReceiptEvaluation evaluation = await storage.Host.InvokeAsync(s => Receipts.Evaluate([receipt, receipt], keys.ReceiptKey, s.Control.State, obj, checksum,
            data.Length, ObjectEncodings.Plain, volume.Id, staging));
        Assert.Equal(1, evaluation.Copies);
        Assert.Equal([storage.Id], evaluation.Holders);
        Assert.NotNull(state);

        // Reads need a read capability listing the object, issued to the caller, or the staging capability of the staging
        // session the object was stored under (a writer reading back its own staged data, PROTOCOL.md §7.3).
        foreach ((string name, RawClient client, byte[] capability) in new (string, RawClient, byte[])[]
        {
            ("missing", admin, []),
            ("other object", admin, Read(RuntimeCluster.AdminPrincipal, [ObjectId.New(random)], expires)),
            ("other principal", reader, Read(RuntimeCluster.AdminPrincipal, [obj], expires)),
            ("expired", admin, Read(RuntimeCluster.AdminPrincipal, [obj], DateTimeOffset.UtcNow.AddSeconds(-1))),
            ("other staging session", admin, Staging(RuntimeCluster.AdminPrincipal, new StagingId(8), expires)),
            ("staging, other principal", reader, Staging(RuntimeCluster.AdminPrincipal, staging, expires)),
            ("staging, expired", admin, Staging(RuntimeCluster.AdminPrincipal, staging, DateTimeOffset.UtcNow.AddSeconds(-1))),
        })
        {
            var error = Assert.IsType<ErrorReplyMessage>(await client.CallAsync(new GetObjectMessage(capability, volume.Id, obj, 0, data.Length)));
            Assert.True(error.Code == DfsErrorCode.AccessDenied, $"{name}: expected AccessDenied, got {error.Code}");
        }

        var readBack = await admin.CallOrThrowAsync<GetObjectResultMessage>(
            new GetObjectMessage(Staging(RuntimeCluster.AdminPrincipal, staging, expires), volume.Id, obj, 0, data.Length));
        Assert.Equal(data, readBack.Data.ToArray());

        byte[] readCapability = Read(RuntimeCluster.AdminPrincipal, [obj], expires);
        var whole = await admin.CallOrThrowAsync<GetObjectResultMessage>(new GetObjectMessage(readCapability, volume.Id, obj, 0, data.Length));
        Assert.Equal(data, whole.Data.ToArray());
        Assert.Equal(checksum, whole.Checksum);
        Assert.Equal(data.Length, whole.ObjectLength);

        // A partial read is aligned to verification units and carries their CRC32C.
        var part = await admin.CallOrThrowAsync<GetObjectResultMessage>(new GetObjectMessage(readCapability, volume.Id, obj, 70_000, 10));
        Assert.True(part.UnitSize > 0);
        Assert.Equal(0, part.Offset % part.UnitSize);
        Assert.True(part.Offset <= 70_000 && part.Offset + part.Data.Length >= 70_010);
        for (int i = 0; i < part.UnitChecksums.Count; i++)
        {
            ReadOnlySpan<byte> unit = part.Data.Span.Slice(i * part.UnitSize, Math.Min(part.UnitSize, part.Data.Length - (i * part.UnitSize)));
            Assert.Equal(ObjectChecksum.Crc32C(unit), part.UnitChecksums[i]);
        }

        Assert.Equal(data.AsSpan((int)part.Offset, part.Data.Length).ToArray(), part.Data.ToArray());

        // An object of another node is missing here: Integrity, never zeroes.
        RuntimeNode other = cluster[2];
        RawClient otherClient = await cluster.ConnectAsync(other);
        var missing = Assert.IsType<ErrorReplyMessage>(await otherClient.CallAsync(new GetObjectMessage(readCapability, volume.Id, obj, 0, 10)));
        Assert.Equal(DfsErrorCode.Integrity, missing.Code);

        // LocateObject requires Read on the volume for clients.
        var located = await admin.CallOrThrowAsync<LocateObjectResultMessage>(new LocateObjectMessage(volume.Id, [obj]));
        Assert.True(located.Locations[0].Present);
        Assert.Equal(staging, located.Locations[0].Staging);
        var locateDenied = Assert.IsType<ErrorReplyMessage>(await reader.CallAsync(new LocateObjectMessage(volume.Id, [obj])));
        Assert.Equal(DfsErrorCode.AccessDenied, locateDenied.Code);

        // Node-only data operations are refused for clients.
        var deleteDenied = Assert.IsType<ErrorReplyMessage>(await admin.CallAsync(new DeleteObjectsMessage(volume.Id, 1, 1, [new ObjectStagingRef(obj, staging)], false)));
        Assert.Equal(DfsErrorCode.AccessDenied, deleteDenied.Code);

        var counters = await storage.Host.Inspect.GetCountersAsync();
        Assert.True(counters.ReceiptsIssued >= 2);
        Assert.True(counters.ObjectsStored >= 1);
        Assert.True(counters.ObjectReads >= 2);
    }
}
