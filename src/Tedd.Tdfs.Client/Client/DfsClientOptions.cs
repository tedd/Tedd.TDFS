using System.Security.Cryptography;
using Tedd.Tdfs.Transport;

namespace Tedd.Tdfs;

/// <summary>
/// Options for <see cref="DfsClient.ConnectAsync(DfsClientOptions, CancellationToken)"/>. Connecting calls
/// <see cref="Validate"/> before any network activity (SDK-001).
/// </summary>
public sealed record DfsClientOptions
{
    /// <summary>Seed endpoints, tried in order (e.g. <c>quic://127.0.0.1:7100</c> or <c>mem://n0</c>).</summary>
    public required IReadOnlyList<string> SeedEndpoints { get; init; }

    /// <summary>When set, connecting fails unless the cluster reports this id (SDK-006).</summary>
    public Guid? ExpectedClusterId { get; init; }

    /// <summary>Principal credentials used to authenticate.</summary>
    public required DfsCredentials Credentials { get; init; }

    /// <summary>Client name reported in diagnostics and audit records.</summary>
    public string ClientName { get; init; } = "client";

    /// <summary>Deadline for establishing the client (discovery, handshake, authentication).</summary>
    public TimeSpan ConnectTimeout { get; init; } = TimeSpan.FromSeconds(10);

    /// <summary>Default deadline of each operation, including internal retries.</summary>
    public TimeSpan OperationTimeout { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>How long disposal waits for in-flight work before cancelling it.</summary>
    public TimeSpan ShutdownTimeout { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>Default stream buffer size in bytes.</summary>
    public int DefaultBufferSize { get; init; } = 64 * 1024;

    /// <summary>Maximum bytes a whole-file read helper may materialise (larger files must be streamed).</summary>
    public long MaxMaterializeBytes { get; init; } = 256L << 20;

    /// <summary>Maximum automatic retries of SDK-internal idempotent operations (0 disables retries).</summary>
    public int MaxRetryAttempts { get; init; } = 8;

    /// <summary>Initial retry backoff (doubled per attempt, with jitter).</summary>
    public TimeSpan RetryBaseDelay { get; init; } = TimeSpan.FromMilliseconds(20);

    /// <summary>Maximum retry backoff.</summary>
    public TimeSpan RetryMaxDelay { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>Transport security (server certificate validation).</summary>
    public DfsTransportSecurity Security { get; init; } = new();

    /// <summary>Clock used for all deadlines, backoff and lease renewal.</summary>
    public TimeProvider TimeProvider { get; init; } = TimeProvider.System;

    /// <summary>Optional diagnostics sink.</summary>
    public IDfsDiagnostics? Diagnostics { get; init; }

    /// <summary>
    /// Validates the options (SDK-001). Every failure is reported as an <see cref="ArgumentException"/> whose
    /// <see cref="ArgumentException.ParamName"/> names the offending property. Messages never contain the secret.
    /// </summary>
    /// <exception cref="ArgumentException">
    /// No seed endpoints; an unparsable endpoint (or a QUIC seed with port 0); null credentials or an empty principal
    /// or secret; an empty client name or <see cref="Guid.Empty"/> expected cluster id; a nonpositive timeout,
    /// buffer size or materialisation limit; a negative retry count or delay; <see cref="RetryBaseDelay"/> greater
    /// than <see cref="RetryMaxDelay"/>; malformed certificate pins; a null clock.
    /// </exception>
    public void Validate()
    {
        ValidateSeedEndpoints();

        if (ExpectedClusterId == Guid.Empty)
            throw new ArgumentException("ExpectedClusterId must not be Guid.Empty; leave it null to accept the cluster found at the seeds.", nameof(ExpectedClusterId));

        ValidateCredentials();

        if (string.IsNullOrWhiteSpace(ClientName))
            throw new ArgumentException("ClientName must be a non-empty name.", nameof(ClientName));

        RequirePositive(ConnectTimeout, nameof(ConnectTimeout));
        RequirePositive(OperationTimeout, nameof(OperationTimeout));
        RequirePositive(ShutdownTimeout, nameof(ShutdownTimeout));

        if (DefaultBufferSize <= 0)
            throw new ArgumentException($"DefaultBufferSize must be positive (was {DefaultBufferSize}).", nameof(DefaultBufferSize));
        if (MaxMaterializeBytes <= 0)
            throw new ArgumentException($"MaxMaterializeBytes must be positive (was {MaxMaterializeBytes}).", nameof(MaxMaterializeBytes));
        if (MaxRetryAttempts < 0)
            throw new ArgumentException($"MaxRetryAttempts must not be negative (was {MaxRetryAttempts}).", nameof(MaxRetryAttempts));
        if (RetryBaseDelay < TimeSpan.Zero)
            throw new ArgumentException($"RetryBaseDelay must not be negative (was {RetryBaseDelay}).", nameof(RetryBaseDelay));
        if (RetryMaxDelay < TimeSpan.Zero)
            throw new ArgumentException($"RetryMaxDelay must not be negative (was {RetryMaxDelay}).", nameof(RetryMaxDelay));
        if (RetryBaseDelay > RetryMaxDelay)
            throw new ArgumentException($"RetryBaseDelay ({RetryBaseDelay}) must not exceed RetryMaxDelay ({RetryMaxDelay}).", nameof(RetryBaseDelay));

        ValidateSecurity();

        if (TimeProvider is null)
            throw new ArgumentException("TimeProvider must not be null.", nameof(TimeProvider));
    }

    private void ValidateSeedEndpoints()
    {
        if (SeedEndpoints is null || SeedEndpoints.Count == 0)
            throw new ArgumentException("At least one seed endpoint is required.", nameof(SeedEndpoints));

        for (int i = 0; i < SeedEndpoints.Count; i++)
        {
            string? endpoint = SeedEndpoints[i];
            if (!RpcAddress.TryParse(endpoint, out RpcAddress address, out string? error))
                throw new ArgumentException($"Seed endpoint {i} is invalid: {error}", nameof(SeedEndpoints));
            if (address.IsQuic && address.Port == 0)
                throw new ArgumentException($"Seed endpoint {i} ('{endpoint}') has port 0; a seed must name the port a node listens on.", nameof(SeedEndpoints));
        }
    }

    private void ValidateCredentials()
    {
        if (Credentials is null)
            throw new ArgumentException("Credentials are required.", nameof(Credentials));
        if (string.IsNullOrWhiteSpace(Credentials.Principal))
            throw new ArgumentException("Credentials.Principal must be a non-empty principal name.", nameof(Credentials));
        if (string.IsNullOrEmpty(Credentials.Secret))
            throw new ArgumentException("Credentials.Secret must not be null or empty.", nameof(Credentials));
    }

    private void ValidateSecurity()
    {
        if (Security is null)
            throw new ArgumentException("Security must not be null.", nameof(Security));
        if (Security.PinnedSpkiSha256 is null)
            throw new ArgumentException("Security.PinnedSpkiSha256 must not be null (use an empty list).", nameof(Security));

        for (int i = 0; i < Security.PinnedSpkiSha256.Count; i++)
        {
            byte[]? pin = Security.PinnedSpkiSha256[i];
            if (pin is null || pin.Length != SHA256.HashSizeInBytes)
                throw new ArgumentException($"Security.PinnedSpkiSha256[{i}] must be a {SHA256.HashSizeInBytes}-byte SHA-256 digest.", nameof(Security));
        }
    }

    private static void RequirePositive(TimeSpan value, string name)
    {
        if (value <= TimeSpan.Zero)
            throw new ArgumentException($"{name} must be positive (was {value}).", name);
    }
}
