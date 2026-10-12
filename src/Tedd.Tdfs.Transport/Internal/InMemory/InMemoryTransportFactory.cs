namespace Tedd.Tdfs.Transport.Internal;

/// <summary>Creates <see cref="InMemoryRpcTransport"/>s attached to one <see cref="InMemoryNetwork"/>.</summary>
internal sealed class InMemoryTransportFactory : IRpcTransportFactory
{
    private readonly InMemoryNetwork _network;

    public InMemoryTransportFactory(InMemoryNetwork network)
    {
        _network = network;
    }

    /// <inheritdoc />
    /// <exception cref="ArgumentException"><paramref name="localName"/> is empty, or <paramref name="listenEndpoint"/> is not a <c>mem://</c> endpoint.</exception>
    public IRpcTransport Create(string localName, string listenEndpoint)
    {
        ArgumentException.ThrowIfNullOrEmpty(localName);
        ArgumentNullException.ThrowIfNull(listenEndpoint);
        if (!RpcAddress.TryParse(listenEndpoint, out RpcAddress address, out string? error))
            throw new ArgumentException(error, nameof(listenEndpoint));
        if (!address.IsMemory)
            throw new ArgumentException($"The in-memory network only supports 'mem://name' endpoints (got '{listenEndpoint}').", nameof(listenEndpoint));
        var transport = new InMemoryRpcTransport(_network, localName, address);
        _network.Track(transport);
        return transport;
    }
}
