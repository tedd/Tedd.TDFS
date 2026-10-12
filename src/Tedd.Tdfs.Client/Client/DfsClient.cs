using Tedd.Tdfs.Client.Internal;
using Tedd.Tdfs.Client.Internal.Runtime;

namespace Tedd.Tdfs;

/// <summary>
/// Connection to a Tedd.TDFS cluster: discovery, authentication, routing and connection pools. Reusable and
/// thread-safe; obtain volume facades with <see cref="GetVolume"/>.
/// </summary>
public sealed class DfsClient : IAsyncDisposable, IDisposable
{
    private readonly Lock _sync = new();
    private readonly Dictionary<string, DfsVolume> _volumes = new(StringComparer.Ordinal);

    internal DfsClient(ClientRuntime runtime)
    {
        ArgumentNullException.ThrowIfNull(runtime);
        Runtime = runtime;
        Admin = new DfsAdminClient(this);
    }

    /// <summary>The client's services (connections, routing, sessions, data plane).</summary>
    internal ClientRuntime Runtime { get; }

    /// <summary>The validated options this client was created with.</summary>
    internal DfsClientOptions Options => Runtime.Options;

    /// <summary>Test/host injections (transport, randomness, faults, mutations).</summary>
    internal DfsClientOptionsInternal Internals => Runtime.Internals;

    /// <summary>Identity of the connected cluster.</summary>
    public ClusterIdentity Cluster => new(Runtime.ClusterId, Runtime.ClusterName);

    /// <summary>Capabilities and limits advertised by the cluster (SDK-014).</summary>
    public DfsServerCapabilities Capabilities => Runtime.Capabilities;

    /// <summary>The authenticated principal.</summary>
    public string Principal => Runtime.Principal;

    /// <summary>True once disposal has started.</summary>
    public bool IsDisposed => Runtime.IsDisposed;

    /// <summary>Administrative operations (cluster administrators).</summary>
    public DfsAdminClient Admin { get; }

    /// <summary>Validates the options, contacts the seeds, verifies the cluster identity and authenticates.</summary>
    /// <exception cref="ArgumentException">The options are invalid (see <see cref="DfsClientOptions.Validate"/>).</exception>
    /// <exception cref="DfsClusterIdentityException">The reachable seeds belong to another cluster than <see cref="DfsClientOptions.ExpectedClusterId"/>.</exception>
    /// <exception cref="DfsAuthenticationException">The credentials were rejected.</exception>
    /// <exception cref="DfsUnavailableException">No seed could be reached within <see cref="DfsClientOptions.ConnectTimeout"/>.</exception>
    public static Task<DfsClient> ConnectAsync(DfsClientOptions options, CancellationToken ct = default) =>
        ConnectAsync(options, DfsClientOptionsInternal.None, ct);

    /// <summary>Synchronous counterpart of <see cref="ConnectAsync(DfsClientOptions, CancellationToken)"/>.</summary>
    public static DfsClient Connect(DfsClientOptions options) =>
        SyncBridge.Run(() => ConnectAsync(options, CancellationToken.None));

    /// <summary>Connects with injected transport, randomness, faults and mutations (test harness and embedding hosts).</summary>
    internal static async Task<DfsClient> ConnectAsync(DfsClientOptions options, DfsClientOptionsInternal internals, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(internals);
        options.Validate();
        ct.ThrowIfCancellationRequested();
        ClientRuntime runtime = await ClientRuntime.ConnectAsync(options, internals, ct).ConfigureAwait(false);
        return new DfsClient(runtime);
    }

    /// <summary>Returns the facade of volume <paramref name="name"/> (no I/O; the same instance per name).</summary>
    /// <exception cref="ArgumentException">The name is invalid.</exception>
    /// <exception cref="ObjectDisposedException">The client is disposed.</exception>
    public DfsVolume GetVolume(string name)
    {
        ArgumentNullException.ThrowIfNull(name);
        if (name.Length == 0)
            throw new ArgumentException("The volume name must not be empty.", nameof(name));
        if (!DfsPath.TryNormalizeComponent(name, out string normalized, out string? error))
            throw new ArgumentException($"Invalid volume name: {error}", nameof(name));
        if (normalized.Contains(DfsPath.StreamSeparator, StringComparison.Ordinal))
            throw new ArgumentException("Invalid volume name: ':' is not allowed.", nameof(name));
        Runtime.ThrowIfDisposed();
        lock (_sync)
        {
            if (!_volumes.TryGetValue(normalized, out DfsVolume? volume))
            {
                volume = new DfsVolume(this, normalized, Runtime.IsBackupHost);
                _volumes[normalized] = volume;
            }

            return volume;
        }
    }

    /// <summary>
    /// Rejects new work, completes or cancels in-flight work within <see cref="DfsClientOptions.ShutdownTimeout"/> and
    /// releases connections. Idempotent. Never commits transactions or flushes streams.
    /// </summary>
    public ValueTask DisposeAsync() => new(Runtime.ShutdownAsync());

    /// <summary>Synchronous counterpart of <see cref="DisposeAsync"/>.</summary>
    public void Dispose() => SyncBridge.Run(Runtime.ShutdownAsync);

    /// <summary>
    /// Simulates process death: stops all client activity immediately without any cleanup traffic (no handle close,
    /// no transaction abort, no session release). Used by the harness's <c>KillClient</c>.
    /// </summary>
    internal void Abandon() => Runtime.Abandon();
}
