using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Tedd.Tdfs.Transport.Internal;

namespace Tedd.Tdfs.Transport;

/// <summary>Creates QUIC transports backed by Tedd.Quicly over MsQuic (ARCHITECTURE §10.2).</summary>
/// <remarks>
/// <para>Each transport owns one pump thread that drives its Quicly server (when listening) and all its outbound
/// clients: polling, flushing, sending queued frames and running queued actions. Callers on any thread only enqueue
/// work. Inbound messages are delivered to the <see cref="IRpcHandler"/> on the thread pool, in arrival order per
/// connection.</para>
/// <para>Wire format: Quicly channel 2 (ReliableOrdered, LZ4) carries single-frame messages and channel 3
/// (ReliableUnordered) the fragments of messages larger than one 60 KiB frame, so a bulk transfer never blocks control
/// traffic. Consequently, message order per connection and direction is preserved among single-frame messages, while a
/// fragmented message is delivered when its last fragment arrives and may be overtaken by later small messages. A
/// client presents its network identity in the Quicly Hello (so the server can apply
/// <see cref="ILinkPolicy.CanConnect"/> before admitting it); the server answers with a preamble frame carrying its own
/// identity. The link policy is consulted by the receiving transport for every message.</para>
/// </remarks>
public sealed class QuiclyTransportFactory : IRpcTransportFactory
{
    /// <summary>Creates the factory.</summary>
    /// <exception cref="ArgumentException">An option is out of range or a pin is not 32 bytes.</exception>
    public QuiclyTransportFactory(QuiclyTransportOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        Validate(options);
        Options = options;
    }

    /// <summary>The options transports are created with.</summary>
    public QuiclyTransportOptions Options { get; }

    /// <inheritdoc />
    /// <exception cref="ArgumentException">
    /// <paramref name="localName"/> is empty, too long or contains control characters, or <paramref name="listenEndpoint"/>
    /// is not a <c>quic://host:port</c> endpoint.
    /// </exception>
    public IRpcTransport Create(string localName, string listenEndpoint)
    {
        QuiclyEnvelope.ValidateName(localName, nameof(localName));
        ArgumentNullException.ThrowIfNull(listenEndpoint);
        if (!RpcAddress.TryParse(listenEndpoint, out RpcAddress address, out string? error))
            throw new ArgumentException(error, nameof(listenEndpoint));
        if (!address.IsQuic)
            throw new ArgumentException($"The QUIC transport only supports 'quic://host:port' endpoints (got '{listenEndpoint}').", nameof(listenEndpoint));
        return new QuiclyRpcTransport(Options, localName, address);
    }

    /// <summary>
    /// Creates a self-signed ECDSA P-256 certificate for development and tests (subject <c>CN=tdfs-development</c>,
    /// names <c>localhost</c>, <c>127.0.0.1</c> and <c>::1</c>, valid for 30 days). Dispose it after every transport that
    /// uses it.
    /// </summary>
    /// <remarks>
    /// The key lives in a non-ephemeral, non-exportable key container that is deleted when the certificate is disposed
    /// (Schannel can only sign with such a key; nothing is persisted in the user's key store).
    /// </remarks>
    public static X509Certificate2 CreateDevelopmentCertificate()
        => CreateDevelopmentCertificate("CN=tdfs-development", TimeSpan.FromDays(30));

    /// <summary>Creates a self-signed ECDSA P-256 certificate with the given subject and validity (see <see cref="CreateDevelopmentCertificate()"/>).</summary>
    /// <param name="subject">Distinguished name, for example <c>CN=tdfs-node</c>.</param>
    /// <param name="validity">Validity period (not-before is backdated five minutes to absorb clock skew).</param>
    public static X509Certificate2 CreateDevelopmentCertificate(string subject, TimeSpan validity)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(subject);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(validity, TimeSpan.Zero);
        DateTimeOffset notBefore = DateTimeOffset.UtcNow.AddMinutes(-5);
        using ECDsa key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        var request = new CertificateRequest(subject, key, HashAlgorithmName.SHA256);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, true));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature, true));
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(
            [new Oid("1.3.6.1.5.5.7.3.1"), new Oid("1.3.6.1.5.5.7.3.2")], false)); // server and client authentication
        request.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(request.PublicKey, false));
        var names = new SubjectAlternativeNameBuilder();
        names.AddDnsName("localhost");
        names.AddIpAddress(IPAddress.Loopback);
        names.AddIpAddress(IPAddress.IPv6Loopback);
        request.CertificateExtensions.Add(names.Build());

        using X509Certificate2 ephemeral = request.CreateSelfSigned(notBefore, notBefore + validity);
        byte[] pfx = ephemeral.Export(X509ContentType.Pkcs12);
        try
        {
            // DefaultKeySet: a real (non-ephemeral) key container Schannel can sign with, deleted with the certificate.
            return X509CertificateLoader.LoadPkcs12(pfx, null, X509KeyStorageFlags.DefaultKeySet);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(pfx);
        }
    }

    /// <summary>The SHA-256 hash of the certificate's DER-encoded SubjectPublicKeyInfo (32 bytes), for <see cref="QuiclyTransportOptions.TrustedSpkiPins"/>.</summary>
    public static byte[] ComputeSpkiPin(X509Certificate2 certificate)
    {
        ArgumentNullException.ThrowIfNull(certificate);
        return SHA256.HashData(certificate.PublicKey.ExportSubjectPublicKeyInfo());
    }

    private static void Validate(QuiclyTransportOptions options)
    {
        ArgumentNullException.ThrowIfNull(options.TrustedSpkiPins, nameof(options.TrustedSpkiPins));
        foreach (byte[] pin in options.TrustedSpkiPins)
        {
            if (pin is null || pin.Length != 32)
                throw new ArgumentException("Every SPKI pin is a 32-byte SHA-256 hash.", nameof(options));
        }

        if (options.Certificate is { HasPrivateKey: false })
            throw new ArgumentException("The listener certificate must have a private key.", nameof(options));
        ArgumentOutOfRangeException.ThrowIfLessThan(options.MaxConnectionsPerAddress, 1, nameof(options.MaxConnectionsPerAddress));
        ArgumentOutOfRangeException.ThrowIfLessThan(options.MaxInboundConnections, 1, nameof(options.MaxInboundConnections));
        ArgumentOutOfRangeException.ThrowIfGreaterThan(options.MaxInboundConnections, 1 << 20, nameof(options.MaxInboundConnections));
        ArgumentOutOfRangeException.ThrowIfLessThan(options.MaxMessageBytes, 1, nameof(options.MaxMessageBytes));
        ArgumentOutOfRangeException.ThrowIfLessThan(options.MaxReassemblyBytesPerConnection, 1, nameof(options.MaxReassemblyBytesPerConnection));
        ArgumentNullException.ThrowIfNull(options.TimeProvider, nameof(options.TimeProvider));
        if (options.ConnectTimeout <= TimeSpan.Zero && options.ConnectTimeout != Timeout.InfiniteTimeSpan)
            throw new ArgumentOutOfRangeException(nameof(options), options.ConnectTimeout, "ConnectTimeout must be positive or infinite.");
    }
}
