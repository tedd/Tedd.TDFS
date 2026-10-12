namespace Tedd.Tdfs.Testing;

/// <summary>
/// Result of <see cref="RawProtocolClient.OpenHandleAsync"/>: the server's verdict on an <c>OpenHandle</c> request sent
/// without any SDK-side validation (LOCK-005, LOCK-007, LOCK-020).
/// </summary>
/// <param name="Code">
/// <see cref="DfsErrorCode.None"/> when the server granted the handle; otherwise the error code of the server's reply
/// (or of the session/handshake step that failed before the request could be sent).
/// </param>
/// <param name="Handle">The allocated handle id when <paramref name="Code"/> is <see cref="DfsErrorCode.None"/>; otherwise null.</param>
/// <param name="WriterGeneration">The writer generation bound to a granted writable handle; 0 for read-only grants and failures.</param>
public sealed record RawOpenHandleResult(DfsErrorCode Code, HandleId? Handle, ulong WriterGeneration)
{
    /// <summary>True when the server granted the handle.</summary>
    public bool Granted => Code == DfsErrorCode.None && Handle is not null;
}
