namespace Tedd.Tdfs.Testing;

// Additions for the LOCK suite (docs/api-additions/lock.md). Lock/lease endpoints driven without SDK validation.
public sealed partial class RawProtocolClient
{
    /// <summary>
    /// Sends an <c>OpenHandle</c> request for <paramref name="path"/> on <paramref name="volume"/> directly to the volume
    /// leader (following <c>NotLeader</c> hints), bypassing every SDK-side check.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The raw client lazily opens one volume session (<c>OpenSession</c>) per volume under its authenticated identity and
    /// allocates a fresh operation id from it for every call. <paramref name="mode"/>, <paramref name="access"/> and
    /// <paramref name="share"/> are encoded verbatim — undefined enum values and bits such as
    /// <see cref="FileShare.Inheritable"/> are transmitted unchanged so that server-side validation can be observed.
    /// The path is sent as the wire UTF-8 of the string, unnormalised.
    /// </para>
    /// <para>
    /// Never throws for server-side refusals: a refused handshake, session or open is reported through
    /// <see cref="RawOpenHandleResult.Code"/>. When the connection was refused at handshake time (see
    /// <see cref="TestCluster.ConnectRawAsBackupAsync"/>) every call returns that refusal code. Transport failures
    /// (connection lost mid-call) are reported as <see cref="DfsErrorCode.Unavailable"/>.
    /// </para>
    /// <para>
    /// A granted handle is a real lease owned by this raw client's session; it is renewed only through
    /// <see cref="KeepAliveAsync"/> and released through <see cref="CloseHandleAsync"/> or expiry.
    /// </para>
    /// </remarks>
    public Task<RawOpenHandleResult> OpenHandleAsync(string volume, string path, FileMode mode, FileAccess access, FileShare share) =>
        throw Stub.NotYet("RawProtocolClient.OpenHandleAsync");

    /// <summary>
    /// Sends one <c>KeepAlive</c> for this raw client's session on <paramref name="volume"/> listing exactly
    /// <paramref name="handles"/> (which may belong to other sessions — that is the point of LOCK-020).
    /// </summary>
    /// <returns>
    /// <see cref="DfsErrorCode.None"/> only when the server reports that every listed handle was renewed by this request;
    /// otherwise the first per-handle or request-level error code (for example <see cref="DfsErrorCode.InvalidHandle"/>,
    /// <see cref="DfsErrorCode.LeaseLost"/>, <see cref="DfsErrorCode.AccessDenied"/> or the handshake refusal code).
    /// </returns>
    public Task<DfsErrorCode> KeepAliveAsync(string volume, IReadOnlyList<HandleId> handles) =>
        throw Stub.NotYet("RawProtocolClient.KeepAliveAsync");

    /// <summary>
    /// Sends <c>CloseHandle</c> for <paramref name="handle"/> in this raw client's session on <paramref name="volume"/>.
    /// Returns the reply's error code (<see cref="DfsErrorCode.None"/> when the server closed the handle).
    /// </summary>
    public Task<DfsErrorCode> CloseHandleAsync(string volume, HandleId handle) =>
        throw Stub.NotYet("RawProtocolClient.CloseHandleAsync");
}
