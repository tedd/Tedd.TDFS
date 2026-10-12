namespace Tedd.Tdfs.Transport;

/// <summary>A listening endpoint that can also open outbound connections.</summary>
public interface IRpcTransport : IAsyncDisposable
{
    /// <summary>The actual bound endpoint after <see cref="StartAsync"/> (for example with the assigned port).</summary>
    string LocalEndpoint { get; }

    /// <summary>Starts listening and delivering inbound messages to <paramref name="handler"/>.</summary>
    Task StartAsync(IRpcHandler handler, CancellationToken ct = default);

    /// <summary>Opens a connection to <paramref name="endpoint"/>.</summary>
    /// <exception cref="RpcConnectionException">The endpoint is unreachable or refused the connection.</exception>
    Task<IRpcConnection> ConnectAsync(string endpoint, CancellationToken ct = default);
}
