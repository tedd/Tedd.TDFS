namespace Tedd.Tdfs.Testing;

// Harness additions requested by the CON/NET suites (see docs/api-additions/con-net.md).
public sealed partial class TestNetwork
{
    /// <summary>
    /// Truncates the next matching frames: only the first <paramref name="keepBytes"/> bytes of each matching frame reach the
    /// receiver, after which the carrying connection is closed (end-of-stream after a partial frame, NET-007).
    /// </summary>
    /// <param name="filter">Selects the frames to truncate (requests or replies).</param>
    /// <param name="keepBytes">Number of leading frame bytes delivered (0 = close before any byte of the frame).</param>
    /// <param name="count">Number of matching frames to truncate.</param>
    /// <returns>The active rule; <see cref="MessageRule.Matched"/> counts truncated frames.</returns>
    /// <remarks>
    /// Required behaviour: for the in-memory/simulated transport the receiver's transport layer is handed the partial frame
    /// followed by a connection close (the receiver must observe a transport/protocol failure, never a shorter valid
    /// frame); for QUIC the partial frame bytes are written to the stream and the connection is then closed by the
    /// sending side. A <paramref name="keepBytes"/> equal to or larger than the frame length delivers the frame and then
    /// closes. The truncation is recorded in <see cref="TestCluster.Trace"/>. Throws <see cref="ArgumentOutOfRangeException"/>
    /// for negative <paramref name="keepBytes"/> or nonpositive <paramref name="count"/>.
    /// </remarks>
    public MessageRule TruncateNext(MessageFilter filter, int keepBytes, int count = 1) =>
        throw Stub.NotYet("TestNetwork.TruncateNext");

    /// <summary>
    /// Silently kills every connection currently established by or to <paramref name="name"/>: neither endpoint is notified
    /// (no close/reset is delivered), every frame later written on those connections vanishes in both directions, and the
    /// connection objects keep reporting themselves as open until the owner's own deadline/keep-alive logic detects the
    /// failure. New connections over the same links work normally (NET-016: a pooled idle connection that died).
    /// </summary>
    /// <param name="name">Node, backup or client network name ("n0", "b0", "client:c0").</param>
    /// <remarks>Recorded in <see cref="TestCluster.Trace"/>. Unknown names throw <see cref="ArgumentException"/>.</remarks>
    public void KillConnectionsSilently(string name) => throw Stub.NotYet("TestNetwork.KillConnectionsSilently");
}
