namespace Tedd.Tdfs.Testing;

// ACL suite additions (docs/api-additions/acl.md): raw read-path requests for ACL-031. The existing raw members can
// open only for creation/writing and read objects only without a capability, so a principal's attempt to obtain read
// capabilities, inline bytes or object ids through the read path cannot be expressed without these.
public sealed partial class RawProtocolClient
{
    /// <summary>
    /// Sends, verbatim and without any SDK-side check, the requests an SDK reader uses to read
    /// [<paramref name="offset"/>, <paramref name="offset"/> + <paramref name="length"/>) of <paramref name="path"/> (a
    /// stream path such as <c>/f:s</c> is sent as is): <c>OpenHandle</c> (FileMode.Open, FileAccess.Read,
    /// FileShare.ReadWrite | FileShare.Delete, live view), then <c>ReadRange</c>, then <c>CloseHandle</c>, to the node this
    /// client is connected to (connect to the volume leader). Stops at the first error reply.
    /// </summary>
    /// <returns>
    /// The error code of the first failing step (<see cref="DfsErrorCode.None"/> when every step succeeded) and everything
    /// the server returned that could disclose or fetch file data: inline bytes, the object ids of the returned extents and
    /// the read capability authorizing each of them (<c>Capabilities[i]</c> authorizes <c>Objects[i]</c>).
    /// </returns>
    public Task<RawReadAttempt> TryReadRangeAsync(string volume, string path, long offset, int length) =>
        throw Stub.NotYet("RawProtocolClient.TryReadRangeAsync");

    /// <summary>
    /// Sends <c>GetObject</c> for the whole of <paramref name="id"/> to the node this client is connected to (a storage
    /// node holding the object), presenting <paramref name="capability"/> verbatim — typically one returned by
    /// <see cref="TryReadRangeAsync"/>, possibly to another principal or on another connection.
    /// </summary>
    /// <returns>The error code (<see cref="DfsErrorCode.None"/> when served) and the verified plaintext when served.</returns>
    public Task<RawObjectRead> GetObjectWithCapabilityAsync(string volume, ObjectId id, ReadOnlyMemory<byte> capability) =>
        throw Stub.NotYet("RawProtocolClient.GetObjectWithCapabilityAsync");
}

/// <summary>Result of <see cref="RawProtocolClient.TryReadRangeAsync"/>.</summary>
/// <param name="Code">Error code of the first failing step; <see cref="DfsErrorCode.None"/> on success.</param>
/// <param name="InlineBytes">Inline file bytes returned by the server, if any.</param>
/// <param name="Objects">Object ids of the returned extents (empty when none were returned).</param>
/// <param name="Capabilities">Read capability for each entry of <paramref name="Objects"/> (same count).</param>
public sealed record RawReadAttempt(DfsErrorCode Code, byte[]? InlineBytes, IReadOnlyList<ObjectId> Objects, IReadOnlyList<ReadOnlyMemory<byte>> Capabilities)
{
    /// <summary>True when the server returned any bytes, object ids or capabilities.</summary>
    public bool DisclosedAnything => InlineBytes is { Length: > 0 } || Objects.Count > 0 || Capabilities.Count > 0;
}

/// <summary>Result of <see cref="RawProtocolClient.GetObjectWithCapabilityAsync"/>.</summary>
/// <param name="Code">Error code; <see cref="DfsErrorCode.None"/> when the object was served.</param>
/// <param name="Data">The object's plaintext when served, otherwise null.</param>
public sealed record RawObjectRead(DfsErrorCode Code, byte[]? Data);
