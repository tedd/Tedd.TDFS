namespace Tedd.Tdfs.Transport;

/// <summary>
/// Out-of-band control over a transport, implemented by both built-in transports (<see cref="InMemoryNetwork"/> and
/// <see cref="QuiclyTransportFactory"/>). Used by the test harness (connection resets, NET-*) and by administration.
/// </summary>
public interface IRpcTransportControl
{
    /// <summary>The network identity the transport was created with (the <c>localName</c> of <see cref="IRpcTransportFactory.Create"/>).</summary>
    string LocalName { get; }

    /// <summary>Number of connections of this transport (inbound and outbound) that are currently open.</summary>
    int OpenConnectionCount { get; }

    /// <summary>
    /// Abortively closes connections of this transport: every connection when <paramref name="remoteName"/> is null,
    /// otherwise those whose remote network identity equals <paramref name="remoteName"/> (ordinal comparison).
    /// </summary>
    /// <remarks>
    /// Pending calls on the affected connections fail with <see cref="RpcConnectionException"/>, and both ends raise
    /// <see cref="IRpcConnection.Closed"/> with a non-null exception. The transport keeps listening, so peers can reconnect.
    /// Connections that are still being established are not affected.
    /// </remarks>
    /// <param name="remoteName">The remote network identity to reset, or null for all connections.</param>
    void ResetConnections(string? remoteName = null);
}
