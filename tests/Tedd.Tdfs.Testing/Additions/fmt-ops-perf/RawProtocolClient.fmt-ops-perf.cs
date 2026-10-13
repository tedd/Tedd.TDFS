using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;

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
        IReadOnlyList<WireExtension> extensions, string? rawPath2 = null)
    {
        ArgumentNullException.ThrowIfNull(rawPath);
        ArgumentNullException.ThrowIfNull(extensions);
        var ids = new HashSet<uint>();
        foreach (WireExtension extension in extensions)
        {
            if (!ids.Add(extension.FieldId))
                throw new ArgumentException($"Extension field id {extension.FieldId} is listed twice.", nameof(extensions));
        }

        return SendPathOperationCoreAsync(volume, kind, rawPath, rawPath2, extensions, rawPathBytes: null);
    }

    /// <summary>
    /// <paramref name="message"/> with <paramref name="extra"/> appended to its terminal extension block.
    /// </summary>
    /// <exception cref="ArgumentException">An extra field id collides with one the request already carries.</exception>
    private static IWireMessage WithExtraExtensions(IWireMessage message, IReadOnlyList<WireExtension> extra)
    {
        static IReadOnlyList<WireExtension> Merge(IReadOnlyList<WireExtension> existing, IReadOnlyList<WireExtension> extra)
        {
            foreach (WireExtension e in extra)
            {
                if (existing.Any(x => x.FieldId == e.FieldId))
                    throw new ArgumentException($"Extension field id {e.FieldId} collides with a field the request already carries.", nameof(extra));
            }

            return [.. existing, .. extra];
        }

        return message switch
        {
            CreateDirectoryMessage m => m with { Extensions = Merge(m.Extensions, extra) },
            DeleteMessage m => m with { Extensions = Merge(m.Extensions, extra) },
            OpenHandleMessage m => m with { Extensions = Merge(m.Extensions, extra) },
            MoveMessage m => m with { Extensions = Merge(m.Extensions, extra) },
            GetInfoMessage m => m with { Extensions = Merge(m.Extensions, extra) },
            WriteAllMessage m => m with { Extensions = Merge(m.Extensions, extra) },
            _ => throw new ArgumentException($"{message.Type} requests cannot carry extra extensions here.", nameof(message)),
        };
    }
}
