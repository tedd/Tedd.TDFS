namespace Tedd.Tdfs.Transport;

/// <summary>Receives inbound requests and one-way messages for a transport.</summary>
public interface IRpcHandler
{
    /// <summary>Handles one inbound message.</summary>
    /// <param name="connection">The connection the message arrived on.</param>
    /// <param name="request">The message payload.</param>
    /// <param name="expectsReply">True for a request (the returned payload is sent as the reply); false for a one-way message.</param>
    /// <param name="ct">Cancelled when the connection or transport closes.</param>
    /// <returns>The reply payload, or null to send no reply.</returns>
    ValueTask<ReadOnlyMemory<byte>?> HandleAsync(IRpcConnection connection, ReadOnlyMemory<byte> request, bool expectsReply, CancellationToken ct);
}
