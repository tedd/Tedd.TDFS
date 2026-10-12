using Tedd.Tdfs.Protocol;

namespace Tedd.Tdfs.Testing;

// Additions for the FMT suite (docs/api-additions/fmt-ops-perf.md): requests carrying extra extension fields.
public sealed partial class RawProtocolClient
{
    /// <summary>
    /// Sends the same request as <see cref="SendPathOperationAsync"/> (same <paramref name="kind"/> values, same verbatim
    /// path encoding, same session/operation-id handling and leader routing) except that the request message's terminal
    /// extension block additionally contains exactly <paramref name="extensions"/>, in the given order, after any extension
    /// the request would normally carry (FMT-002, FMT-003).
    /// </summary>
    /// <remarks>
    /// The extensions are encoded with <see cref="WireWriter.WriteExtensions"/>; their field ids must not collide with field
    /// ids the request already carries (the call throws <see cref="ArgumentException"/> before sending if they do). The server
    /// must reject a request whose block contains a required extension it does not understand with
    /// <see cref="DfsErrorCode.Unsupported"/> and without any effect, and must process a request whose unknown extensions are
    /// all optional exactly as if they were absent.
    /// </remarks>
    /// <returns><see cref="DfsErrorCode.None"/> when the server reports success; otherwise the server's (or handshake's) error code.</returns>
    public Task<DfsErrorCode> SendPathOperationWithExtensionsAsync(string volume, DfsOperationKind kind, string rawPath,
        IReadOnlyList<WireExtension> extensions, string? rawPath2 = null) =>
        throw Stub.NotYet("RawProtocolClient.SendPathOperationWithExtensionsAsync");
}
