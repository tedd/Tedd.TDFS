using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs.Host;

/// <summary>Self-signed QUIC certificates for development, tests and the recovery runbook (bootstrap command).</summary>
public static class DevelopmentCertificates
{
    /// <summary>
    /// Creates a self-signed ECDSA P-256 certificate for <c>localhost</c>/loopback (server and client authentication) and
    /// returns it as PKCS#12 bytes (no password) with its SHA-256 SPKI pin.
    /// </summary>
    /// <param name="subject">Distinguished name, for example <c>CN=tdfs-node</c>.</param>
    /// <param name="validity">Validity period (not-before is backdated five minutes).</param>
    public static (byte[] Pfx, byte[] Pin) CreatePfx(string subject, TimeSpan validity)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(subject);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(validity, TimeSpan.Zero);
        DateTimeOffset notBefore = DateTimeOffset.UtcNow.AddMinutes(-5);
        using ECDsa key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        var request = new CertificateRequest(subject, key, HashAlgorithmName.SHA256);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, true));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature, true));
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(
            [new Oid("1.3.6.1.5.5.7.3.1"), new Oid("1.3.6.1.5.5.7.3.2")], false));
        request.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(request.PublicKey, false));
        var names = new SubjectAlternativeNameBuilder();
        names.AddDnsName("localhost");
        names.AddIpAddress(IPAddress.Loopback);
        names.AddIpAddress(IPAddress.IPv6Loopback);
        request.CertificateExtensions.Add(names.Build());
        using X509Certificate2 certificate = request.CreateSelfSigned(notBefore, notBefore + validity);
        byte[] pin = QuiclyTransportFactory.ComputeSpkiPin(certificate);
        return (certificate.Export(X509ContentType.Pkcs12), pin);
    }

    /// <summary>
    /// Loads a PKCS#12 certificate with a persisted (non-ephemeral) key container, which the platform TLS stack can sign
    /// with; the container is deleted when the certificate is disposed.
    /// </summary>
    public static X509Certificate2 Load(byte[] pfx, string? password) =>
        X509CertificateLoader.LoadPkcs12(pfx, password, X509KeyStorageFlags.DefaultKeySet);

    /// <summary>Parses hex SPKI pins.</summary>
    /// <exception cref="InvalidDataException">A pin is not 32 bytes of hex.</exception>
    public static IReadOnlyList<byte[]> ParsePins(IEnumerable<string> pins)
    {
        var result = new List<byte[]>();
        foreach (string text in pins)
        {
            byte[] pin;
            try
            {
                pin = Convert.FromHexString(text);
            }
            catch (FormatException ex)
            {
                throw new InvalidDataException($"Trusted pin '{text}' is not hexadecimal.", ex);
            }

            if (pin.Length != SHA256.HashSizeInBytes)
                throw new InvalidDataException($"Trusted pin '{text}' is not a SHA-256 digest.");
            result.Add(pin);
        }

        return result;
    }
}
