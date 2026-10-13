using Tedd.Tdfs.Protocol;
using Tedd.Tdfs.Protocol.Messages;

namespace Tedd.Tdfs.Testing;

/// <summary>Raw protocol additions for reading back a readable writer's staged uploads (PROTOCOL.md §8.1, §9.4).</summary>
public sealed partial class RawProtocolClient
{
    /// <summary>
    /// <see cref="OpenStagedWriterAsync"/> for a readable writer: the handle is opened with <see cref="FileAccess.ReadWrite"/>
    /// (and <see cref="FileShare.Read"/>), and a staging session is bound to it.
    /// </summary>
    /// <param name="volume">Volume name.</param>
    /// <param name="path">Normalized path.</param>
    /// <param name="mode">Open mode (default <see cref="FileMode.OpenOrCreate"/>).</param>
    public Task<RawStagedWriter> OpenStagedReadWriterAsync(string volume, string path, FileMode mode = FileMode.OpenOrCreate)
        => OpenStagedHandleAsync(volume, path, mode, FileAccess.ReadWrite);

    /// <summary>
    /// Sends <c>ReadRange</c> through <paramref name="handle"/> for an empty range at offset 0 with the required
    /// <c>StagedObjects</c> extension carrying <paramref name="receipts"/> (their wire bytes, verbatim — possibly forged or
    /// another handle's), and returns the server's error code plus the read capability of a successful reply.
    /// </summary>
    /// <param name="volume">Volume name.</param>
    /// <param name="handle">Handle to read through.</param>
    /// <param name="receipts">Receipts naming the staged objects to read back.</param>
    /// <exception cref="ArgumentException">A receipt's wire bytes are not a decodable receipt record.</exception>
    public async Task<(DfsErrorCode Code, ReadOnlyMemory<byte> Capability)> RequestStagedReadAsync(string volume, HandleId handle,
        IReadOnlyList<RawReceipt> receipts)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(volume);
        ArgumentNullException.ThrowIfNull(receipts);
        ObjectReceipt[] wire = receipts.Select(r => WireRecordCodec.Decode<ObjectReceipt>(r.Wire.Span)).ToArray();
        (DfsErrorCode code, IWireMessage? reply) = await SendVolumeRequestAsync(volume, mutating: false,
            header => Encode(new ReadRangeMessage(header, handle, 0, default, 0, 0).WithStagedReceipts(wire)));
        return reply is ReadRangeResultMessage range ? (DfsErrorCode.None, range.ReadCapability) : (code == DfsErrorCode.None ? DfsErrorCode.ProtocolError : code, default);
    }
}
