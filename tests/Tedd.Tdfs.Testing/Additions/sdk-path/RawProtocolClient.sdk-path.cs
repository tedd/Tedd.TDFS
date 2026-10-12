namespace Tedd.Tdfs.Testing;

// Additions requested by the SDK/PATH test suites (docs/api-additions/sdk-path.md).
public sealed partial class RawProtocolClient
{
    /// <summary>
    /// Opens a file handle in <paramref name="volume"/> through this raw connection (its own authenticated session in
    /// that volume), sending the path verbatim like <see cref="SendPathOperationAsync"/>. Returns the server's error
    /// code (<see cref="DfsErrorCode.None"/> on success) and the server-issued handle id exactly as it appears on the
    /// wire (<c>default</c> on failure). The handle is renewed by the raw client's session keep-alive until
    /// <see cref="SendHandleOperationAsync"/> closes it or the connection is disposed.
    /// </summary>
    /// <remarks>Used by PATH-014 to obtain a genuine handle id of one volume and present it to another.</remarks>
    public Task<(DfsErrorCode Code, HandleId Handle)> OpenHandleAsync(string volume, string rawPath, FileMode mode, FileAccess access, FileShare share) =>
        throw Stub.NotYet("RawProtocolClient.OpenHandleAsync");

    /// <summary>
    /// Sends one handle-addressed request for <paramref name="handle"/> to the leader of <paramref name="volume"/>,
    /// bypassing every SDK check, and returns the reply's error code (<see cref="DfsErrorCode.None"/> on success).
    /// Supported kinds: <see cref="DfsOperationKind.Read"/> (ReadRange offset 0, length 1),
    /// <see cref="DfsOperationKind.KeepAlive"/> (renew only this handle), <see cref="DfsOperationKind.CloseHandle"/>,
    /// <see cref="DfsOperationKind.CommitFile"/> (an empty commit — no changes, unchanged length — carrying the handle's
    /// writer generation as last reported by the server, or 0 when unknown). The request carries the handle number
    /// verbatim; it is never translated through the raw client's own handle table. Other kinds throw
    /// <see cref="ArgumentOutOfRangeException"/>.
    /// </summary>
    /// <remarks>Used by PATH-014: a handle issued by another volume must be rejected (for example
    /// <see cref="DfsErrorCode.InvalidHandle"/>) even when the target volume has a handle with the same number.</remarks>
    public Task<DfsErrorCode> SendHandleOperationAsync(string volume, DfsOperationKind kind, HandleId handle) =>
        throw Stub.NotYet("RawProtocolClient.SendHandleOperationAsync");
}
