namespace Tedd.Tdfs.Testing;

// Additions requested by the SDK/PATH test suites (docs/api-additions/sdk-path.md).
public sealed partial class TestNetwork
{
    /// <summary>
    /// Number of transport connections that are currently open and have <paramref name="name"/> ("n0", "b0",
    /// "client:c0") as either endpoint. A connection counts from the moment the transport reports it established until
    /// either side closes or disposes it (graceful close, reset by <see cref="ResetConnections"/>, node crash/stop, or
    /// client disposal). Connections that a partition or black hole has made unusable but that neither side has closed
    /// still count (they are open, half-open sockets). Pending connection attempts that never completed do not count.
    /// </summary>
    /// <remarks>
    /// Used by SDK-005, SDK-011 and SDK-012 to prove that a failed connect, a disposed client or a client whose
    /// in-flight work was cancelled at shutdown does not leak transport connections. Works for both
    /// <see cref="TransportKind.InMemory"/> and <see cref="TransportKind.Quic"/> transports (the harness wraps every
    /// transport factory it hands to nodes and clients, so it can track open/closed events per local name).
    /// <para>Each connection is counted once, from the end that opened it (the harness wraps both ends).</para>
    /// </remarks>
    /// <param name="name">Network name of a node, backup node or client.</param>
    /// <returns>The number of open connections (0 when the name is unknown).</returns>
    public int CountOpenConnections(string name)
    {
        ArgumentNullException.ThrowIfNull(name);
        return Fabric.CountOpenConnections(name);
    }
}
