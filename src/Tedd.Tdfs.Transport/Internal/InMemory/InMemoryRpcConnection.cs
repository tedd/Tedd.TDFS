namespace Tedd.Tdfs.Transport.Internal;

/// <summary>
/// One end of an in-memory connection. Outbound messages are copied and queued on the other end, which applies the
/// network's link policy and delivers them in order on the thread pool.
/// </summary>
internal sealed class InMemoryRpcConnection : RpcConnectionBase
{
    private readonly InMemoryRpcTransport _owner;
    private InMemoryRpcConnection? _peer;

    public InMemoryRpcConnection(InMemoryRpcTransport owner, long id, RpcAddress remote, string remoteName)
        : base(id, remote, remoteName)
    {
        _owner = owner;
    }

    protected override string LocalName => _owner.LocalName;

    protected override TimeProvider Time => _owner.Network.TimeProvider;

    protected override ILinkPolicy Policy => _owner.Network.Policy;

    protected override IRpcHandler? Handler => _owner.Handler;

    /// <summary>Links this end to the other end (before either is published).</summary>
    internal void Attach(InMemoryRpcConnection peer) => _peer = peer;

    protected override ValueTask TransmitAsync(RpcMessageKind kind, ulong correlation, ReadOnlyMemory<byte> payload, bool awaitTransmission)
    {
        if (!IsOpen)
            throw CreateClosedException();
        InMemoryRpcConnection peer = _peer ?? throw new InvalidOperationException("The connection is not attached.");
        peer.PostInbound(kind, correlation, payload.ToArray());
        return ValueTask.CompletedTask;
    }

    protected override void OnClosed(RpcCloseOrigin origin, Exception? error)
    {
        _owner.Forget(this);
        if (origin == RpcCloseOrigin.Remote || _peer is not { } peer)
            return;
        string message = origin == RpcCloseOrigin.Reset
            ? $"The connection was reset by '{_owner.LocalName}'."
            : $"The connection was closed by '{_owner.LocalName}'.";
        // Queued behind every message this end sent before closing, as a stream close would be.
        peer.PostRemoteClose(new RpcConnectionException(message, origin == RpcCloseOrigin.Fault ? error : null));
    }
}
