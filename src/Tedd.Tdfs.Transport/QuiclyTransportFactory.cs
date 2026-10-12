namespace Tedd.Tdfs.Transport;

/// <summary>Creates QUIC transports backed by Tedd.Quicly over MsQuic (ARCHITECTURE §10.2).</summary>
public sealed class QuiclyTransportFactory : IRpcTransportFactory
{
    /// <summary>Creates the factory.</summary>
    public QuiclyTransportFactory(QuiclyTransportOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        Options = options;
    }

    /// <summary>The options transports are created with.</summary>
    public QuiclyTransportOptions Options { get; }

    /// <inheritdoc />
    public IRpcTransport Create(string localName, string listenEndpoint)
        => throw new NotImplementedException("QuiclyTransportFactory.Create: implemented in a later phase");
}
