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
    public Task<(DfsErrorCode Code, HandleId Handle)> OpenHandleInVolumeAsync(string volume, string rawPath, FileAccess access, FileShare share) =>
        throw Stub.NotYet("RawProtocolClient.OpenHandleInVolumeAsync");

    /// <summary>
    /// Sends one <c>ReadRange</c> for <paramref name="handle"/> (number sent verbatim, never translated through the raw
    /// client's own handle table) to the leader of <paramref name="volume"/>, bypassing every SDK check. Returns the reply's
    /// error code and, only when the code is <see cref="DfsErrorCode.None"/>, the returned bytes (otherwise empty). Inline
    /// data is returned directly; extent data is fetched with the read capability the reply carries and verified.
    /// </summary>
    public Task<(DfsErrorCode Code, byte[] Data)> ReadThroughHandleAsync(string volume, HandleId handle, long offset, int length) =>
        throw Stub.NotYet("RawProtocolClient.ReadThroughHandleAsync");

    /// <summary>
    /// Sends one <c>CloseHandle</c> for <paramref name="handle"/> (verbatim) to the leader of <paramref name="volume"/> and
    /// returns the reply's error code.
    /// </summary>
    public Task<DfsErrorCode> CloseHandleInVolumeAsync(string volume, HandleId handle) =>
        throw Stub.NotYet("RawProtocolClient.CloseHandleInVolumeAsync");
}
