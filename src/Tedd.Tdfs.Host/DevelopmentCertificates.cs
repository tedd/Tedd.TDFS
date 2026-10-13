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
    /// <remarks>
    /// The container is a file in the user's key store (Windows: a CNG key under the user profile). A process that is
    /// killed never disposes its certificate, so its container is left behind; launchers that kill hosts delete it with
    /// <see cref="DeletePersistedKey"/>, using the name the host reports in <see cref="HostReadyMessage.KeyContainer"/>.
    /// </remarks>
    public static X509Certificate2 Load(byte[] pfx, string? password) =>
        X509CertificateLoader.LoadPkcs12(pfx, password, X509KeyStorageFlags.DefaultKeySet);

    /// <summary>Parses hex SPKI pins.</summary>
    /// <exception cref="InvalidDataException">A pin is not 32 bytes of hex.</exception>
    /// <summary>
    /// The name of the persisted key container holding <paramref name="certificate"/>'s private key (Windows CNG), or null
    /// when the key is not a named, persisted CNG key (other platforms keep no per-process key files).
    /// </summary>
    public static string? PersistedKeyName(X509Certificate2 certificate)
    {
        ArgumentNullException.ThrowIfNull(certificate);
        if (!OperatingSystem.IsWindows())
            return null;
        try
        {
            using ECDsa? ecdsa = certificate.GetECDsaPrivateKey();
            if (ecdsa is ECDsaCng { Key: { IsEphemeral: false, KeyName: { Length: > 0 } name } })
                return name;
            using RSA? rsa = certificate.GetRSAPrivateKey();
            return rsa is RSACng { Key: { IsEphemeral: false, KeyName: { Length: > 0 } rsaName } } ? rsaName : null;
        }
        catch (CryptographicException)
        {
            return null;
        }
    }

    /// <summary>
    /// Deletes a persisted key container left behind by a host process that was killed (see <see cref="Load"/>); true when
    /// it existed. Never throws: a container that is already gone (the host exited cleanly) is not an error.
    /// </summary>
    public static bool DeletePersistedKey(string keyName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(keyName);
        if (!OperatingSystem.IsWindows())
            return false;
        try
        {
            if (!CngKey.Exists(keyName, CngProvider.MicrosoftSoftwareKeyStorageProvider))
                return false;
            using CngKey key = CngKey.Open(keyName, CngProvider.MicrosoftSoftwareKeyStorageProvider);
            key.Delete();
            return true;
        }
        catch (CryptographicException)
        {
            return false;
        }
    }

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
