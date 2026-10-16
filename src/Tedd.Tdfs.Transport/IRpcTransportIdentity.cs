namespace Tedd.Tdfs.Transport;

/// <summary>The certificate a transport's listener presents: its SPKI pin and validity period.</summary>
/// <param name="SpkiSha256">SHA-256 of the certificate's subject public key info (32 bytes).</param>
/// <param name="NotBefore">Start of the validity period.</param>
/// <param name="NotAfter">End of the validity period.</param>
public sealed record RpcCertificateInfo(byte[] SpkiSha256, DateTimeOffset NotBefore, DateTimeOffset NotAfter);

/// <summary>
/// The TLS identity of a transport that authenticates its endpoints with certificates (QUIC): the certificate its listener
/// presents and the SPKI pins its outbound connections trust. Transports without certificates (in-memory, simulated) do
/// not implement it.
/// </summary>
public interface IRpcTransportIdentity
{
    /// <summary>The certificate this transport's listener presents (null when it has none, e.g. a connect-only transport).</summary>
    RpcCertificateInfo? LocalCertificate { get; }

    /// <summary>True when outbound connections accept only servers whose certificate matches a trusted SPKI pin.</summary>
    bool PinsPeers { get; }

    /// <summary>True when <paramref name="spkiSha256"/> is one of the trusted SPKI pins.</summary>
    bool TrustsPin(ReadOnlySpan<byte> spkiSha256);
}
