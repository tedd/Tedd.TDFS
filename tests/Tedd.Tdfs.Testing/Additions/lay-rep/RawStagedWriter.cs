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
    internal RawStagedWriter()
    {
    }

    /// <summary>The handle id allocated by the volume state machine.</summary>
    public HandleId Handle => throw Stub.NotYet("RawStagedWriter.Handle");

    /// <summary>The inode the handle is bound to.</summary>
    public InodeId Inode => throw Stub.NotYet("RawStagedWriter.Inode");

    /// <summary>The writer generation bound to the handle (sent with every publication).</summary>
    public ulong WriterGeneration => throw Stub.NotYet("RawStagedWriter.WriterGeneration");

    /// <summary>The file revision observed at open (used as the commit's base revision).</summary>
    public ulong BaseRevision => throw Stub.NotYet("RawStagedWriter.BaseRevision");

    /// <summary>The staging session bound to the handle.</summary>
    public StagingId Staging => throw Stub.NotYet("RawStagedWriter.Staging");

    /// <summary>
    /// Sends <c>PutObject</c> for <paramref name="id"/> with <paramref name="data"/> (plaintext, default encoding) to the
    /// storage node named <paramref name="nodeName"/> using this session's staging capability, waits for the reply, and
    /// returns the error code plus the decoded receipt when one was issued. The same id may be sent repeatedly (retry
    /// tests); the payload is sent verbatim, so a different payload for an existing id exercises the immutability check.
    /// </summary>
    /// <param name="nodeName">Target storage node ("n0".."n9").</param>
    /// <param name="id">Object id chosen by the test.</param>
    /// <param name="data">Object payload.</param>
    public Task<RawPutResult> PutObjectAsync(string nodeName, ObjectId id, ReadOnlyMemory<byte> data) =>
        throw Stub.NotYet("RawStagedWriter.PutObjectAsync");

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
    public Task<DfsErrorCode> CommitExtentsAsync(long newLength, IReadOnlyList<RawExtentSpec> extents, IReadOnlyList<RawReceipt> receipts) =>
        throw Stub.NotYet("RawStagedWriter.CommitExtentsAsync");

    /// <summary>Closes the handle (which closes the staging session). Idempotent.</summary>
    public ValueTask DisposeAsync() => throw Stub.NotYet("RawStagedWriter.DisposeAsync");
}
