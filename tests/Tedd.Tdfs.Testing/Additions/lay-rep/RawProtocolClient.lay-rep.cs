namespace Tedd.Tdfs.Testing;

/// <summary>Raw protocol additions for the layout (LAY) and replication (REP) suites.</summary>
public sealed partial class RawProtocolClient
{
    /// <summary>
    /// Opens a read handle on <paramref name="path"/> in <paramref name="volume"/> (as the authenticated principal, with
    /// <see cref="FileShare.ReadWrite"/>), sends one <c>ReadRange</c> request whose <paramref name="offset"/> and
    /// <paramref name="length"/> are encoded verbatim (no SDK validation, negative and overflowing values included),
    /// closes the handle and returns the server's error code (<see cref="DfsErrorCode.None"/> when the server returned a
    /// range result). The helper never allocates a buffer sized by <paramref name="length"/>; reply data is discarded.
    /// Used by LAY-018.
    /// </summary>
    /// <param name="volume">Volume name.</param>
    /// <param name="path">Path sent verbatim (must name an existing file for a meaningful result).</param>
    /// <param name="offset">Logical offset, sent unmodified.</param>
    /// <param name="length">Requested length, sent unmodified.</param>
    public Task<DfsErrorCode> SendReadRangeAsync(string volume, string path, long offset, long length) =>
        throw Stub.NotYet("RawProtocolClient.SendReadRangeAsync");

    /// <summary>
    /// Opens a writable handle on <paramref name="path"/> with <paramref name="mode"/> (<see cref="FileAccess.Write"/>,
    /// <see cref="FileShare.Read"/>) through the volume leader, creates a staging session bound to that handle and
    /// returns a <see cref="RawStagedWriter"/> that can upload objects to chosen storage nodes and publish arbitrary
    /// (possibly invalid) extent maps with arbitrary (possibly forged, stale or mismatched) receipts. Failures to open the
    /// handle or the staging session throw the mapped SDK exception (<see cref="DfsErrors.ToException"/>). Used by
    /// LAY-018, LAY-019, REP-012 and REP-013.
    /// </summary>
    /// <param name="volume">Volume name.</param>
    /// <param name="path">Normalized path.</param>
    /// <param name="mode">Open mode (default <see cref="FileMode.OpenOrCreate"/>).</param>
    public Task<RawStagedWriter> OpenStagedWriterAsync(string volume, string path, FileMode mode = FileMode.OpenOrCreate) =>
        throw Stub.NotYet("RawProtocolClient.OpenStagedWriterAsync");
}
