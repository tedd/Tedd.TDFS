using Tedd.Tdfs.Testing.Simulation;
using Tedd.Tdfs.Transport;

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
    /// <para>Implementation status: the simulated network (deterministic clusters) delivers pieces as separate transport
    /// fragments; the realtime in-memory and QUIC transports expose no fragment-level write, so there the call throws
    /// <see cref="NotSupportedException"/>.</para>
    /// </remarks>
    public async Task<DfsErrorCode> SendFrameInPiecesAsync(ReadOnlyMemory<byte> frame, IReadOnlyList<int> splitOffsets)
    {
        ArgumentNullException.ThrowIfNull(splitOffsets);
        int previous = 0;
        foreach (int offset in splitOffsets)
        {
            if (offset <= previous || offset >= frame.Length)
                throw new ArgumentException("Split offsets must be strictly increasing and inside (0, frame length).", nameof(splitOffsets));
            previous = offset;
        }

        if (splitOffsets.Count == 0)
            return await SendFrameAsync(frame);
        if (_refusal != DfsErrorCode.None)
            return _refusal;
        SimConnectionEnd end = SimulatedEnd();
        if (!end.IsOpen)
            return DfsErrorCode.ProtocolError;
        try
        {
            ReadOnlyMemory<byte> reply = await end.CallInPiecesAsync(frame, splitOffsets, CallTimeout, closeAfterPieces: false);
            return NetworkFabric.CodeOf(reply.Span);
        }
        catch (TimeoutException)
        {
            return end.IsOpen ? DfsErrorCode.Timeout : DfsErrorCode.ProtocolError;
        }
        catch (Exception ex) when (ex is RpcConnectionException or ObjectDisposedException)
        {
            return DfsErrorCode.ProtocolError;
        }
    }

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
    /// <para>Implementation status: on the simulated network the delivered bytes travel as the first fragment of a frame whose
    /// remainder never arrives, followed by the close; realtime transports throw <see cref="NotSupportedException"/>.</para>
    /// </remarks>
    public async Task<DfsErrorCode> SendPartialFrameAndCloseAsync(ReadOnlyMemory<byte> frame, int deliveredBytes)
    {
        if (deliveredBytes < 0 || deliveredBytes >= frame.Length)
            throw new ArgumentOutOfRangeException(nameof(deliveredBytes), deliveredBytes, "Delivered bytes must be in [0, frame length).");
        if (_refusal != DfsErrorCode.None)
            return _refusal;
        SimConnectionEnd end = SimulatedEnd();
        if (!end.IsOpen)
            return DfsErrorCode.ProtocolError;
        Task<ReadOnlyMemory<byte>> call = end.CallInPiecesAsync(frame, deliveredBytes == 0 ? [] : [deliveredBytes], CallTimeout,
            closeAfterPieces: true);
        DfsErrorCode code;
        try
        {
            code = NetworkFabric.CodeOf((await call).Span);
        }
        catch (Exception ex) when (ex is RpcConnectionException or ObjectDisposedException or TimeoutException)
        {
            code = DfsErrorCode.ProtocolError;
        }

        // The close travels behind the delivered bytes; give the server's process the deliveries before returning.
        TestCluster cluster = _cluster;
        await cluster.Runtime.DelayAsync(cluster.Options.Timings.Tick);
        return code;
    }

    /// <summary>The simulated connection end under the primary connection (deterministic clusters only).</summary>
    private SimConnectionEnd SimulatedEnd() =>
        (_connection as HarnessConnection)?.Inner as SimConnectionEnd
        ?? throw new NotSupportedException("Fragment-level writes are available on the simulated network (deterministic clusters) only.");
}
