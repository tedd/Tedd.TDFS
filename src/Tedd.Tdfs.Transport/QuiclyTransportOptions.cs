using System.Security.Cryptography.X509Certificates;

namespace Tedd.Tdfs.Transport;

/// <summary>Options for <see cref="QuiclyTransportFactory"/>.</summary>
/// <param name="Certificate">Certificate (with private key) presented by listeners.</param>
/// <param name="TrustedSpkiPins">SHA-256 hashes of trusted subject public key infos for outbound connections.</param>
/// <param name="LinkPolicy">Optional fault-interposition policy (null = allow all).</param>
/// <param name="MaxConnectionsPerAddress">Admission limit per remote IP address (all localhost peers share one address).</param>
public sealed record QuiclyTransportOptions(
    X509Certificate2 Certificate,
    IReadOnlyList<byte[]> TrustedSpkiPins,
    ILinkPolicy? LinkPolicy = null,
    int MaxConnectionsPerAddress = 4096);
