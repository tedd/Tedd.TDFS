using System.Security.Cryptography.X509Certificates;

namespace Tedd.Tdfs;

/// <summary>How the client authenticates cluster nodes at the transport (TLS) layer.</summary>
public sealed record DfsTransportSecurity
{
    /// <summary>Accepted SHA-256 digests of server SubjectPublicKeyInfo (32 bytes each).</summary>
    public IReadOnlyList<byte[]> PinnedSpkiSha256 { get; init; } = [];

    /// <summary>Optional custom server-certificate validation; return true to accept.</summary>
    public Func<X509Certificate2, bool>? ValidateServerCertificate { get; init; }

    /// <summary>Accept any server certificate. Development and tests only.</summary>
    public bool AllowUntrustedForDevelopment { get; init; }
}
