namespace Tedd.Tdfs.Transport;

/// <summary>A message-oriented connection carrying request/response calls and one-way messages of arbitrary length.</summary>
public interface IRpcConnection : IAsyncDisposable
{
    /// <summary>The remote endpoint.</summary>
    RpcAddress Remote { get; }

    /// <summary>True until the connection is closed or fails.</summary>
    bool IsOpen { get; }

    /// <summary>Sends a request and waits for its reply.</summary>
    /// <exception cref="TimeoutException">No reply arrived within <paramref name="timeout"/> (the request may have been executed).</exception>
    /// <exception cref="RpcConnectionException">The connection failed or closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="ct"/> was cancelled.</exception>
    Task<ReadOnlyMemory<byte>> CallAsync(ReadOnlyMemory<byte> request, TimeSpan timeout, CancellationToken ct = default);

    /// <summary>Sends a one-way message (no reply).</summary>
    /// <exception cref="RpcConnectionException">The connection failed or closed.</exception>
    ValueTask SendAsync(ReadOnlyMemory<byte> message, CancellationToken ct = default);

    /// <summary>The authenticated peer identity, set by the handshake layer.</summary>
    RpcPeerIdentity? Identity { get; set; }

    /// <summary>Raised once when the connection closes; the exception is null for a graceful close.</summary>
    event Action<IRpcConnection, Exception?>? Closed;
}
