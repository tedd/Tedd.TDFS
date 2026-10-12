namespace Tedd.Tdfs.Testing;

// Harness additions requested by the NET suite (see docs/api-additions/con-net.md).
public sealed partial class RawProtocolClient
{
    /// <summary>
    /// Sends one complete application frame split into several transport writes at the given byte offsets, letting the
    /// receiver process each piece before the next is written, and returns the error code of the server's reply
    /// (<see cref="DfsErrorCode.None"/> for a success reply; <see cref="DfsErrorCode.ProtocolError"/> when the server closes
    /// the connection). Used to show that reassembly of every split reconstructs exactly the unsplit frame (NET-006).
    /// </summary>
    /// <param name="frame">The complete frame (<c>[u16 type][body]</c>).</param>
    /// <param name="splitOffsets">Strictly increasing offsets in (0, frame.Length) at which the frame is split.</param>
    /// <remarks>
    /// Required behaviour: the pieces travel as one logical request (one correlation id) through the real transport
    /// envelope/fragmentation path: in-memory/simulated transports deliver each piece as a separate fragment with a
    /// scheduler turn in between; QUIC writes and flushes each piece separately. Offsets that are not strictly increasing
    /// or outside (0, frame.Length) throw <see cref="ArgumentException"/>. An empty offset list behaves like
    /// <see cref="SendFrameAsync"/>.
    /// </remarks>
    public Task<DfsErrorCode> SendFrameInPiecesAsync(ReadOnlyMemory<byte> frame, IReadOnlyList<int> splitOffsets) =>
        throw Stub.NotYet("RawProtocolClient.SendFrameInPiecesAsync");

    /// <summary>
    /// Writes only the first <paramref name="deliveredBytes"/> bytes of <paramref name="frame"/> as a transport message that
    /// announces the full length, then closes the connection (end-of-stream inside a frame, NET-007). Returns the error code
    /// the server reported before closing, or <see cref="DfsErrorCode.ProtocolError"/> when it only closed. Afterwards
    /// <see cref="IsConnected"/> is false.
    /// </summary>
    /// <param name="frame">The complete frame (<c>[u16 type][body]</c>).</param>
    /// <param name="deliveredBytes">Bytes delivered before the close, in [0, frame.Length).</param>
    /// <remarks>
    /// Required behaviour: completes only after the server side has observed the close (so tests can assert that no state
    /// changed afterwards). Throws <see cref="ArgumentOutOfRangeException"/> when <paramref name="deliveredBytes"/> is not in
    /// [0, frame.Length).
    /// </remarks>
    public Task<DfsErrorCode> SendPartialFrameAndCloseAsync(ReadOnlyMemory<byte> frame, int deliveredBytes) =>
        throw Stub.NotYet("RawProtocolClient.SendPartialFrameAndCloseAsync");
}
