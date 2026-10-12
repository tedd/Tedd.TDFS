using System.Security.Cryptography.X509Certificates;

namespace Tedd.Tdfs.Transport;

/// <summary>Options for <see cref="QuiclyTransportFactory"/>.</summary>
/// <param name="Certificate">
/// Certificate (with private key) presented by listeners. Only required by transports that call
/// <see cref="IRpcTransport.StartAsync"/>; a connect-only transport may pass null. The certificate must stay alive (not
/// disposed) while any transport created with these options runs. <see cref="QuiclyTransportFactory.CreateDevelopmentCertificate()"/>
/// creates a self-signed one for development and tests.
/// </param>
/// <param name="TrustedSpkiPins">
/// SHA-256 hashes (32 bytes each) of trusted subject public key infos for outbound connections
/// (<see cref="QuiclyTransportFactory.ComputeSpkiPin"/>). When non-empty, a server is accepted only if its leaf
/// certificate's SPKI hash is one of them (the pin is the trust anchor; chain and name are not checked) unless
/// <see cref="ValidateServerCertificate"/> accepts it. When empty and no other validation is configured, the platform
/// trust store and server name decide.
/// </param>
/// <param name="LinkPolicy">Optional fault-interposition policy (null = allow all).</param>
/// <param name="MaxConnectionsPerAddress">Admission limit per remote IP address (all localhost peers share one address).</param>
public sealed record QuiclyTransportOptions(
    X509Certificate2 Certificate,
    IReadOnlyList<byte[]> TrustedSpkiPins,
    ILinkPolicy? LinkPolicy = null,
    int MaxConnectionsPerAddress = 4096)
{
    /// <summary>Time source for call timeouts, connect timeouts and link delays. Default <see cref="TimeProvider.System"/>.</summary>
    public TimeProvider TimeProvider { get; init; } = TimeProvider.System;

    /// <summary>
    /// Longest time <see cref="IRpcTransport.ConnectAsync"/> may take (QUIC handshake, admission and preamble exchange).
    /// Default 10 s.
    /// </summary>
    public TimeSpan ConnectTimeout { get; init; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Optional server-certificate check for outbound connections: return true to accept. Consulted when the certificate
    /// matches none of the <see cref="TrustedSpkiPins"/>; it replaces platform validation.
    /// </summary>
    public Func<X509Certificate2, bool>? ValidateServerCertificate { get; init; }

    /// <summary>Accept any server certificate (development and tests only; QUIC still encrypts, but the server is not authenticated).</summary>
    public bool AllowUntrustedForDevelopment { get; init; }

    /// <summary>Largest message (request, reply or one-way payload) a connection sends or accepts. Default 256 MiB.</summary>
    public int MaxMessageBytes { get; init; } = 256 << 20;

    /// <summary>Largest total of partially received (fragmented) message bytes buffered per connection. Default 512 MiB.</summary>
    public long MaxReassemblyBytesPerConnection { get; init; } = 512L << 20;

    /// <summary>Most inbound connections a listening transport admits at once. Default 4 096.</summary>
    public int MaxInboundConnections { get; init; } = 4096;

    /// <summary>
    /// Optional sink for diagnostics (protocol violations, failed handlers, transport warnings). Called on transport
    /// threads; must not block or throw. Messages never contain payload bytes.
    /// </summary>
    public Action<string>? Diagnostic { get; init; }
}
