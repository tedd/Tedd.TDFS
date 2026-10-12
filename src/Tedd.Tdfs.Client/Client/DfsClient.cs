using Tedd.Tdfs.Client.Internal;

namespace Tedd.Tdfs;

/// <summary>
/// Connection to a Tedd.TDFS cluster: discovery, authentication, routing and connection pools. Reusable and
/// thread-safe; obtain volume facades with <see cref="GetVolume"/>.
/// </summary>
public sealed class DfsClient : IAsyncDisposable, IDisposable
{
    internal DfsClient(DfsClientOptions options, DfsClientOptionsInternal internals)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(internals);
        Options = options;
        Internals = internals;
        Admin = new DfsAdminClient(this);
    }

    /// <summary>The validated options this client was created with.</summary>
    internal DfsClientOptions Options { get; }

    /// <summary>Test/host injections (transport, randomness, faults, mutations).</summary>
    internal DfsClientOptionsInternal Internals { get; }

    /// <summary>Identity of the connected cluster.</summary>
    public ClusterIdentity Cluster => throw new NotImplementedException("DfsClient.Cluster: implemented in a later phase");

    /// <summary>Capabilities and limits advertised by the cluster (SDK-014).</summary>
    public DfsServerCapabilities Capabilities => throw new NotImplementedException("DfsClient.Capabilities: implemented in a later phase");

    /// <summary>The authenticated principal.</summary>
    public string Principal => throw new NotImplementedException("DfsClient.Principal: implemented in a later phase");

    /// <summary>True once disposal has started.</summary>
    public bool IsDisposed => throw new NotImplementedException("DfsClient.IsDisposed: implemented in a later phase");

    /// <summary>Administrative operations (cluster administrators).</summary>
    public DfsAdminClient Admin { get; }

    /// <summary>Validates the options, contacts the seeds, verifies the cluster identity and authenticates.</summary>
    /// <exception cref="ArgumentException">The options are invalid (see <see cref="DfsClientOptions.Validate"/>).</exception>
    public static Task<DfsClient> ConnectAsync(DfsClientOptions options, CancellationToken ct = default) =>
        ConnectAsync(options, DfsClientOptionsInternal.None, ct);

    /// <summary>Synchronous counterpart of <see cref="ConnectAsync(DfsClientOptions, CancellationToken)"/>.</summary>
    public static DfsClient Connect(DfsClientOptions options) =>
        SyncBridge.Run(() => ConnectAsync(options, CancellationToken.None));

    /// <summary>Connects with injected transport, randomness, faults and mutations (test harness and embedding hosts).</summary>
    internal static Task<DfsClient> ConnectAsync(DfsClientOptions options, DfsClientOptionsInternal internals, CancellationToken ct) =>
        throw new NotImplementedException("DfsClient.ConnectAsync: implemented in a later phase");

    /// <summary>Returns the facade of volume <paramref name="name"/> (no I/O; the same instance per name).</summary>
    /// <exception cref="ArgumentException">The name is invalid.</exception>
    /// <exception cref="ObjectDisposedException">The client is disposed.</exception>
    public DfsVolume GetVolume(string name) =>
        throw new NotImplementedException("DfsClient.GetVolume: implemented in a later phase");

    /// <summary>
    /// Rejects new work, completes or cancels in-flight work within <see cref="DfsClientOptions.ShutdownTimeout"/> and
    /// releases connections. Idempotent. Never commits transactions or flushes streams.
    /// </summary>
    public ValueTask DisposeAsync() =>
        throw new NotImplementedException("DfsClient.DisposeAsync: implemented in a later phase");

    /// <summary>Synchronous counterpart of <see cref="DisposeAsync"/>.</summary>
    public void Dispose() =>
        throw new NotImplementedException("DfsClient.Dispose: implemented in a later phase");

    /// <summary>
    /// Simulates process death: stops all client activity immediately without any cleanup traffic (no handle close,
    /// no transaction abort, no session release). Used by the harness's <c>KillClient</c>.
    /// </summary>
    internal void Abandon() =>
        throw new NotImplementedException("DfsClient.Abandon: implemented in a later phase");
}
