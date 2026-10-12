namespace Tedd.Tdfs.Transport;

/// <summary>Creates transports for nodes and clients.</summary>
public interface IRpcTransportFactory
{
    /// <summary>Creates a transport.</summary>
    /// <param name="localName">Network identity used by link policies (for example "n0" or "client:c0").</param>
    /// <param name="listenEndpoint">Endpoint to listen on (for example "quic://127.0.0.1:0" or "mem://n0").</param>
    IRpcTransport Create(string localName, string listenEndpoint);
}
