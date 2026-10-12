namespace Tedd.Tdfs.Testing;

// Additions for the LOCK suite (docs/api-additions/lock.md) and, for OpenHandleAsync, the SDK/PATH suites
// (docs/api-additions/sdk-path.md). Lock/lease endpoints driven without SDK validation.
public sealed partial class RawProtocolClient
{
    /// <summary>
    /// Sends an <c>OpenHandle</c> request for <paramref name="path"/> on <paramref name="volume"/> directly to the volume
    /// leader (following <c>NotLeader</c> hints), bypassing every SDK-side check, and returns the server's verdict with the
    /// server-issued handle id exactly as it appears on the wire.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Consolidated definition requested by the LOCK suite (LOCK-005, LOCK-007, LOCK-020) and the SDK/PATH suites
    /// (PATH-014; docs/api-additions/lock.md, docs/api-additions/sdk-path.md).
    /// </para>
    /// <para>
    /// The raw client lazily opens one volume session (<c>OpenSession</c>) per volume under its authenticated identity and
    /// allocates a fresh operation id from it for every call; one raw client may therefore hold handles in several volumes
    /// (one session per volume, same principal). The request is routed to <paramref name="volume"/>'s current leader
    /// whichever node the client connected to. <paramref name="mode"/>, <paramref name="access"/> and
    /// <paramref name="share"/> are encoded verbatim — undefined enum values and bits such as
    /// <see cref="FileShare.Inheritable"/> are transmitted unchanged so that server-side validation can be observed.
    /// The path is sent as the wire UTF-8 of the string, unnormalised (like <see cref="SendPathOperationAsync"/>).
    /// </para>
    /// <para>
    /// Never throws for server-side refusals: a refused handshake, session or open is reported through
    /// <see cref="RawOpenHandleResult.Code"/> (with <see cref="RawOpenHandleResult.Handle"/> null). When the connection was
    /// refused at handshake time (see <see cref="TestCluster.ConnectRawAsBackupAsync"/>) every call returns that refusal
    /// code. Transport failures (connection lost mid-call) are reported as <see cref="DfsErrorCode.Unavailable"/>.
    /// </para>
    /// <para>
    /// Lease renewal: a granted handle is a real lease owned by this raw client's session. With
    /// <paramref name="autoRenew"/> = false (the default, required by LOCK tests that observe lease expiry) it is renewed
    /// only through <see cref="KeepAliveAsync"/> (or <see cref="SendHandleOperationAsync"/> with
    /// <see cref="DfsOperationKind.KeepAlive"/>) and released through <see cref="CloseHandleAsync"/>, a
    /// <see cref="DfsOperationKind.CloseHandle"/> handle operation, or expiry. With <paramref name="autoRenew"/> = true the
    /// raw client's session keep-alive renews the handle (every LeaseDuration/3, following leader changes) until it is
    /// closed or the raw client is disposed (PATH-014 holds up to a few hundred handles for the duration of the test).
    /// </para>
    /// </remarks>
    /// <param name="volume">Volume name.</param>
    /// <param name="path">Path sent verbatim.</param>
    /// <param name="mode">Open mode, sent verbatim.</param>
    /// <param name="access">Requested access, sent verbatim.</param>
    /// <param name="share">Share mode, sent verbatim.</param>
    /// <param name="autoRenew">True to have the raw client's session keep-alive renew the granted handle automatically.</param>
    /// <returns>The verdict, the granted handle id and its writer generation.</returns>
    public Task<RawOpenHandleResult> OpenHandleAsync(string volume, string path, FileMode mode, FileAccess access, FileShare share, bool autoRenew = false) =>
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
