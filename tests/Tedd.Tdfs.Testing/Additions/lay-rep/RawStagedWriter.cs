using Tedd.Tdfs.Data;
using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;

namespace Tedd.Tdfs.Testing;

/// <summary>
/// A writable handle plus staging session opened by <see cref="RawProtocolClient.OpenStagedWriterAsync"/>. It bypasses the
/// SDK publication pipeline so tests can upload objects to specific storage nodes, keep the returned receipts, and send
/// <c>CommitFile</c> requests whose extent refs and receipts are exactly what the test supplies (LAY-018, LAY-019,
/// REP-012, REP-013). Every call returns the server's error code instead of throwing for server-side rejections.
/// While undisposed it keeps its session and handle lease alive (KeepAlive every LeaseDuration/3, following leader
/// changes), so tests may crash/restart other nodes or change topology between uploads and the commit.
/// </summary>
public sealed class RawStagedWriter : IAsyncDisposable
{
    private readonly RawProtocolClient _client;
    private readonly RawProtocolClient.RawSession _session;
    private readonly string _volume;
    private readonly VolumeId _volumeId;
    private readonly ReadOnlyMemory<byte> _capability;
    private readonly ulong _placementEpoch;
    private ulong _baseRevision;
    private int _disposed;

    internal RawStagedWriter(RawProtocolClient client, RawProtocolClient.RawSession session, string volume, VolumeId volumeId, HandleId handle,
        InodeId inode, ulong writerGeneration, ulong baseRevision, StagingId staging, ReadOnlyMemory<byte> capability, ulong placementEpoch)
    {
        _client = client;
        _session = session;
        _volume = volume;
        _volumeId = volumeId;
        Handle = handle;
        Inode = inode;
        WriterGeneration = writerGeneration;
        _baseRevision = baseRevision;
        Staging = staging;
        _capability = capability;
        _placementEpoch = placementEpoch;
    }

    /// <summary>The handle id allocated by the volume state machine.</summary>
    public HandleId Handle { get; }

    /// <summary>The inode the handle is bound to.</summary>
    public InodeId Inode { get; }

    /// <summary>The writer generation bound to the handle (sent with every publication).</summary>
    public ulong WriterGeneration { get; }

    /// <summary>The file revision observed at open (used as the commit's base revision).</summary>
    public ulong BaseRevision => Volatile.Read(ref _baseRevision);

    /// <summary>The staging session bound to the handle.</summary>
    public StagingId Staging { get; }

    /// <summary>
    /// Sends <c>PutObject</c> for <paramref name="id"/> with <paramref name="data"/> (plaintext, default encoding) to the
    /// storage node named <paramref name="nodeName"/> using this session's staging capability, waits for the reply, and
    /// returns the error code plus the decoded receipt when one was issued. The same id may be sent repeatedly (retry
    /// tests); the payload is sent verbatim, so a different payload for an existing id exercises the immutability check.
    /// </summary>
    /// <param name="nodeName">Target storage node ("n0".."n9").</param>
    /// <param name="id">Object id chosen by the test.</param>
    /// <param name="data">Object payload.</param>
    public async Task<RawPutResult> PutObjectAsync(string nodeName, ObjectId id, ReadOnlyMemory<byte> data)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(nodeName);
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        TestNode node = _client.Cluster.Node(nodeName);
        var put = new PutObjectMessage(_capability, _volumeId, Staging, id, ObjectChecksum.Compute(data.Span), data.Length, ObjectEncodings.Plain,
            _placementEpoch, data);
        (DfsErrorCode code, IWireMessage? reply) = await _client.CallNodeAsync(node.Endpoint, MessageCodec.Encode(put));
        if (reply is not PutObjectResultMessage result)
            return new RawPutResult(code == DfsErrorCode.None ? DfsErrorCode.ProtocolError : code, null);
        ObjectReceipt receipt = result.Receipt;
        string issuer = _client.Cluster.Nodes.FirstOrDefault(n => n.Id == receipt.Node)?.Name ?? nodeName;
        return new RawPutResult(DfsErrorCode.None, new RawReceipt(receipt.Object, receipt.Checksum, receipt.Length, receipt.Encoding, receipt.Node, issuer,
            receipt.Incarnation, receipt.PlacementEpoch, WireRecordCodec.Encode(receipt)));
    }

    /// <summary>
    /// Sends <c>CommitFile</c> for this handle (handle id, writer generation, <see cref="BaseRevision"/>, staging id) with
    /// <paramref name="newLength"/>, the extent refs in <paramref name="extents"/> exactly as given (order, offsets,
    /// lengths, object ranges, checksums and encodings unvalidated by the helper) and the receipts in
    /// <paramref name="receipts"/>, sending each receipt's <see cref="RawReceipt.Wire"/> bytes verbatim. Returns the
    /// server's error code (<see cref="DfsErrorCode.None"/> on a committed publication). On success
    /// <see cref="BaseRevision"/> advances to the new revision.
    /// </summary>
    /// <param name="newLength">New logical file length (sent verbatim; may be negative or out of range).</param>
    /// <param name="extents">Changed extent refs.</param>
    /// <param name="receipts">Receipts offered as durability proof.</param>
    /// <exception cref="ArgumentException">A receipt's wire bytes are not a decodable receipt record.</exception>
    public async Task<DfsErrorCode> CommitExtentsAsync(long newLength, IReadOnlyList<RawExtentSpec> extents, IReadOnlyList<RawReceipt> receipts)
    {
        ArgumentNullException.ThrowIfNull(extents);
        ArgumentNullException.ThrowIfNull(receipts);
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        RangeChange[] changes = extents.Select(e => new RangeChange(e.Offset, e.Length, RangeChangeKind.Data, e.Object, e.ObjectOffset, e.ObjectLength,
            e.Checksum, e.Encoding, ReadOnlyMemory<byte>.Empty)).ToArray();
        var wireReceipts = new ObjectReceipt[receipts.Count];
        for (int i = 0; i < receipts.Count; i++)
        {
            try
            {
                wireReceipts[i] = WireRecordCodec.Decode<ObjectReceipt>(receipts[i].Wire.Span);
            }
            catch (DfsProtocolException ex)
            {
                throw new ArgumentException($"Receipt {i} is not a decodable receipt record: {ex.Message}", nameof(receipts), ex);
            }
        }

        ulong baseRevision = BaseRevision;
        (DfsErrorCode code, IWireMessage? reply) = await _client.SendVolumeRequestAsync(_volume, mutating: true, header =>
            RawProtocolClient.Encode(new CommitFileMessage(header, Handle, WriterGeneration, baseRevision, TruncateTo: -1, newLength, changes, wireReceipts,
                Staging, SnapshotName: null)));
        if (reply is CommitFileResultMessage committed)
            Volatile.Write(ref _baseRevision, committed.Revision);
        return code;
    }

    /// <summary>Closes the handle (which closes the staging session). Idempotent.</summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;
        _session.Forget(Handle);
        await _client.SendVolumeRequestAsync(_volume, mutating: true,
            header => RawProtocolClient.Encode(new CloseHandleMessage(header, Handle, WriterGeneration)));
    }
}
