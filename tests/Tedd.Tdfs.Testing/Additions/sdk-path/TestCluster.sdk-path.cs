namespace Tedd.Tdfs.Testing;

// Additions requested by the SDK/PATH test suites (docs/api-additions/sdk-path.md).
public sealed partial class TestCluster
{
    /// <summary>
    /// Connects a client exactly like <see cref="ConnectAsync(TestClientOptions?, CancellationToken)"/> (same network
    /// identity <c>client:{Name}</c>, harness transport, randomness, client clock, fault injector and mutations), but
    /// first passes the public <see cref="DfsClientOptions"/> the harness built (as returned by
    /// <see cref="CreateClientOptions"/>) through <paramref name="configure"/>, so a test can set public options that
    /// <see cref="TestClientOptions"/> does not expose (for example <see cref="DfsClientOptions.ShutdownTimeout"/>,
    /// <see cref="DfsClientOptions.MaxRetryAttempts"/>, <see cref="DfsClientOptions.RetryBaseDelay"/>).
    /// </summary>
    /// <remarks>
    /// The returned options are validated by the SDK exactly as on the public connect path; an invalid result surfaces
    /// as the SDK's <see cref="ArgumentException"/>. The harness must not override any value the callback changed.
    /// Used by SDK-012 (documented shutdown policy with a known <c>ShutdownTimeout</c>).
    /// </remarks>
    /// <param name="options">Harness client options (null = defaults, client "c0", admin credentials).</param>
    /// <param name="configure">Transforms the public options (use a <c>with</c> expression).</param>
    /// <param name="ct">Cancels connecting.</param>
    public Task<DfsClient> ConnectAsync(TestClientOptions? options, Func<DfsClientOptions, DfsClientOptions> configure, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(configure);
        return ConnectCoreAsync(options ?? new TestClientOptions(), configure, harness: false, ct);
    }
}
