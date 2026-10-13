using System.Globalization;
using Tedd.Tdfs.Data;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;
using Tedd.Tdfs.Tests.Unit.Internal.Runtime;

namespace Tedd.Tdfs.Tests.Unit.Internal.Volumes;

/// <summary>
/// A raw-protocol volume client for service tests: routes to the current volume leader (following NotLeader), opens a
/// session, allocates operation ids, uploads objects with the staging capability and reads object bytes with the read
/// capability, exactly as the wire contract (PROTOCOL.md) specifies.
/// </summary>
internal sealed class VolumeClient
{
    private readonly RuntimeCluster _cluster;
    private readonly DfsCredentials _credentials;
    private readonly Dictionary<string, RawClient> _connections = new(StringComparer.Ordinal);

    public VolumeClient(RuntimeCluster cluster, string volume, DfsCredentials? credentials = null)
    {
        _cluster = cluster;
        Volume = volume;
        _credentials = credentials ?? RuntimeCluster.AdminCredentials;
    }

    public string Volume { get; }

    public ulong Session { get; private set; }

    public ulong NextSequence { get; set; } = 1;

    public DfsOperationId NewOperationId() => new(Session, NextSequence++);

    public RequestHeader Header(DfsOperationId operation = default) => new(Volume, Session: Session, Operation: operation);

    public RequestHeader Op() => Header(NewOperationId());

    public async Task<RawClient> ConnectionAsync(RuntimeNode node)
    {
        if (!_connections.TryGetValue(node.Name, out RawClient? client) || !client.Connection.IsOpen)
        {
            client = await _cluster.ConnectAsync(node, _credentials);
            _connections[node.Name] = client;
        }

        return client;
    }

    /// <summary>Sends a request to the volume leader (rerouting on NotLeader) and returns the reply (an error reply as a message).</summary>
    public async Task<IWireMessage> CallAsync(IWireMessage request)
    {
        for (int attempt = 0; ; attempt++)
        {
            RuntimeNode leader = await _cluster.WaitForLeaderAsync(Volume);
            RawClient client = await ConnectionAsync(leader);
            IWireMessage reply = await client.CallAsync(request);
            if (reply is ErrorReplyMessage { Code: DfsErrorCode.NotLeader or DfsErrorCode.Unavailable } && attempt < 50)
            {
                await Task.Delay(50, TestContext.Current.CancellationToken);
                continue;
            }

            return reply;
        }
    }

    /// <summary>Sends a request that must succeed.</summary>
    public async Task<T> OkAsync<T>(IWireMessage request) where T : IWireMessage
    {
        IWireMessage reply = await CallAsync(request);
        if (reply is ErrorReplyMessage error)
            Assert.Fail($"{request.Type} failed: {error.Code} {error.Message}");
        return Assert.IsType<T>(reply);
    }

    /// <summary>Sends a request that must fail with <paramref name="code"/>.</summary>
    public async Task<ErrorReplyMessage> FailsAsync(DfsErrorCode code, IWireMessage request)
    {
        IWireMessage reply = await CallAsync(request);
        ErrorReplyMessage error = Assert.IsType<ErrorReplyMessage>(reply);
        Assert.True(error.Code == code, $"{request.Type}: expected {code}, got {error.Code} ({error.Message})");
        return error;
    }

    public async Task<OpenSessionResultMessage> OpenSessionAsync()
    {
        var reply = await OkAsync<OpenSessionResultMessage>(new OpenSessionMessage(new RequestHeader(Volume), "test", new UInt128(42, (ulong)Environment.TickCount64)));
        Session = reply.Session;
        NextSequence = reply.FirstSequence;
        return reply;
    }

    public Task<OpenHandleResultMessage> OpenAsync(string path, FileMode mode, FileAccess access, FileShare share, bool pinned = false)
        => OkAsync<OpenHandleResultMessage>(OpenRequest(path, mode, access, share, pinned));

    public OpenHandleMessage OpenRequest(string path, FileMode mode, FileAccess access, FileShare share, bool pinned = false)
        => new(Op(), path, mode, access, share, FileOptions.None, pinned, false, null);

    public Task<KeepAliveResultMessage> KeepAliveAsync(params HandleId[] handles)
        => OkAsync<KeepAliveResultMessage>(new KeepAliveMessage(Header(), handles, [], []));

    public async Task<CreateStagingResultMessage> StagingAsync(HandleId handle = default)
        => await OkAsync<CreateStagingResultMessage>(new CreateStagingMessage(Op(), handle, default));

    /// <summary>Uploads one object to every running node (the policy needs fewer) and returns their receipts.</summary>
    public async Task<(RangeChange Change, IReadOnlyList<ObjectReceipt> Receipts)> UploadAsync(CreateStagingResultMessage staging, long offset, byte[] data,
        int copies = int.MaxValue)
    {
        var obj = new ObjectId(new UInt128((ulong)Random.Shared.NextInt64(), (ulong)Random.Shared.NextInt64() | 1));
        UInt128 checksum = ObjectChecksum.Compute(data);
        var receipts = new List<ObjectReceipt>();
        VolumeEntry? entry = null;
        foreach (RuntimeNode node in _cluster.Running.Take(copies))
        {
            RawClient client = await ConnectionAsync(node);
            entry ??= ((VolumeRouteResultMessage)await client.CallOrThrowAsync<VolumeRouteResultMessage>(new GetVolumeRouteMessage(Volume))).Volume;
            var put = new PutObjectMessage(staging.Capability, entry.Id, staging.Staging, obj, checksum, data.Length, ObjectEncodings.Plain, staging.PlacementEpoch,
                data);
            PutObjectResultMessage result = await client.CallOrThrowAsync<PutObjectResultMessage>(put);
            receipts.Add(result.Receipt);
        }

        return (new RangeChange(offset, data.Length, RangeChangeKind.Data, obj, 0, data.Length, checksum, ObjectEncodings.Plain, ReadOnlyMemory<byte>.Empty),
            receipts);
    }

    /// <summary>Reads a range through a handle: ReadRange at the leader, then object bytes with the read capability.</summary>
    public async Task<byte[]> ReadAsync(HandleId handle, long offset, long length, ulong snapshotPin = 0, InodeId inode = default)
    {
        var result = new List<byte>();
        long position = offset;
        long end = offset + length;
        while (position < end)
        {
            ReadRangeResultMessage reply = await OkAsync<ReadRangeResultMessage>(new ReadRangeMessage(Header(), handle, snapshotPin, inode, position, end - position));
            if (reply.Length == 0)
                break;
            byte[] covered = new byte[reply.Length];
            if (reply.IsInline)
            {
                reply.InlineData.Span.CopyTo(covered);
            }
            else
            {
                foreach (ExtentData extent in reply.Extents)
                {
                    long from = Math.Max(extent.Offset, reply.Offset);
                    long to = Math.Min(extent.Offset + extent.Length, reply.Offset + reply.Length);
                    if (to <= from)
                        continue;
                    byte[] bytes = await GetObjectAsync(extent, reply.ReadCapability);
                    Array.Copy(bytes, extent.ObjectOffset + (from - extent.Offset), covered, from - reply.Offset, to - from);
                }
            }

            result.AddRange(covered);
            position += reply.Length;
        }

        return [.. result];
    }

    private async Task<byte[]> GetObjectAsync(ExtentData extent, ReadOnlyMemory<byte> capability)
    {
        foreach (RuntimeNode node in _cluster.Running)
        {
            if (extent.Locations.Count > 0 && !extent.Locations.Contains(node.Id))
                continue;
            RawClient client = await ConnectionAsync(node);
            VolumeEntry entry = ((VolumeRouteResultMessage)await client.CallOrThrowAsync<VolumeRouteResultMessage>(new GetVolumeRouteMessage(Volume))).Volume;
            IWireMessage reply = await client.CallAsync(new GetObjectMessage(capability, entry.Id, extent.Object, 0, extent.ObjectLength));
            if (reply is GetObjectResultMessage data)
            {
                Assert.Equal(extent.Checksum, ObjectChecksum.Compute(data.Data.Span));
                return data.Data.ToArray();
            }
        }

        throw new InvalidOperationException($"No node served object {extent.Object}.");
    }

    public static string Name(int i) => "n" + i.ToString(CultureInfo.InvariantCulture);
}
