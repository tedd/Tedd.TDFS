using Tedd.Tdfs.Data;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;

namespace Tedd.Tdfs.Testing;

// POOL suite additions (docs/api-additions/pool.md): handle-addressed raw requests across volumes (POOL-002).
public sealed partial class RawProtocolClient
{
    /// <summary>
    /// Opens a file handle in <paramref name="volume"/> (mode <see cref="FileMode.Open"/>) through this raw connection's own
    /// authenticated session in that volume, sending <paramref name="rawPath"/> verbatim. Routes to the volume's current
    /// leader (following <c>NotLeader</c> hints) regardless of the node the raw client connected to; one raw client may hold
    /// handles in several volumes at once. Returns the server's error code (<see cref="DfsErrorCode.None"/> on success) and
    /// the server-issued handle id exactly as it appeared on the wire (<c>default</c> on failure). The handle is kept alive
    /// by the raw client's session keep-alive until closed or the connection is disposed.
    /// </summary>
    public async Task<(DfsErrorCode Code, HandleId Handle)> OpenHandleInVolumeAsync(string volume, string rawPath, FileAccess access, FileShare share)
    {
        RawOpenHandleResult result = await OpenHandleAsync(volume, rawPath, FileMode.Open, access, share, autoRenew: true);
        return (result.Code, result.Handle ?? default);
    }

    /// <summary>
    /// Sends one <c>ReadRange</c> for <paramref name="handle"/> (number sent verbatim, never translated through the raw
    /// client's own handle table) to the leader of <paramref name="volume"/>, bypassing every SDK check. Returns the reply's
    /// error code and, only when the code is <see cref="DfsErrorCode.None"/>, the returned bytes (otherwise empty). Inline
    /// data is returned directly; extent data is fetched with the read capability the reply carries and verified.
    /// </summary>
    public async Task<(DfsErrorCode Code, byte[] Data)> ReadThroughHandleAsync(string volume, HandleId handle, long offset, int length)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(volume);
        (DfsErrorCode code, IWireMessage? reply) = await SendVolumeRequestAsync(volume, mutating: false,
            header => Encode(new ReadRangeMessage(header, handle, 0, default, offset, length)));
        if (reply is not ReadRangeResultMessage range)
            return (code == DfsErrorCode.None ? DfsErrorCode.ProtocolError : code, []);
        return await MaterializeAsync(volume, range);
    }

    /// <summary>
    /// The bytes a read-range result describes: inline bytes as returned, or the extents fetched with the result's read
    /// capability from a node holding each object and verified against the object checksum (holes read as zeroes).
    /// </summary>
    internal async Task<(DfsErrorCode Code, byte[] Data)> MaterializeAsync(string volume, ReadRangeResultMessage range)
    {
        if (range.IsInline)
            return (DfsErrorCode.None, range.InlineData.ToArray());
        long length = Math.Max(0, range.Length);
        byte[] data = new byte[checked((int)length)];
        (DfsErrorCode routeCode, VolumeId volumeId) = await ResolveVolumeIdAsync(volume);
        if (routeCode != DfsErrorCode.None)
            return (routeCode, []);
        foreach (ExtentData extent in range.Extents)
        {
            long start = Math.Max(range.Offset, extent.Offset);
            long end = Math.Min(range.Offset + length, extent.Offset + extent.Length);
            if (end <= start)
                continue;
            (DfsErrorCode objectCode, byte[]? content) = await FetchVerifiedObjectAsync(volumeId, extent.Object, range.ReadCapability, extent.Locations);
            if (content is null)
                return (objectCode, []);
            long objectStart = extent.ObjectOffset + (start - extent.Offset);
            if (objectStart < 0 || objectStart + (end - start) > content.Length)
                return (DfsErrorCode.Integrity, []);
            content.AsSpan(checked((int)objectStart), checked((int)(end - start))).CopyTo(data.AsSpan(checked((int)(start - range.Offset))));
        }

        return (DfsErrorCode.None, data);
    }

    /// <summary>Fetches a whole object from the first listed node that serves it and verifies its checksum.</summary>
    private async Task<(DfsErrorCode Code, byte[]? Data)> FetchVerifiedObjectAsync(VolumeId volume, ObjectId id, ReadOnlyMemory<byte> capability,
        IReadOnlyList<NodeId> locations)
    {
        DfsErrorCode last = DfsErrorCode.Unavailable;
        foreach (NodeId location in locations)
        {
            TestNode? node = Cluster.Nodes.FirstOrDefault(n => n.Id == location);
            if (node is null || node.State != TestNodeState.Running)
                continue;
            (DfsErrorCode code, IWireMessage? reply) = await CallNodeAsync(node.Endpoint,
                MessageCodec.Encode(new GetObjectMessage(capability, volume, id, 0, long.MaxValue)));
            if (reply is GetObjectResultMessage result)
            {
                if (result.Offset == 0 && result.Data.Length == result.ObjectLength && ObjectChecksum.Compute(result.Data.Span) == result.Checksum)
                    return (DfsErrorCode.None, result.Data.ToArray());
                last = DfsErrorCode.Integrity;
                continue;
            }

            last = code;
        }

        return (last, null);
    }

    /// <summary>
    /// Sends one <c>CloseHandle</c> for <paramref name="handle"/> (verbatim) to the leader of <paramref name="volume"/> and
    /// returns the reply's error code.
    /// </summary>
    public Task<DfsErrorCode> CloseHandleInVolumeAsync(string volume, HandleId handle) => CloseHandleAsync(volume, handle);
}
