using System.Net;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Tdfs.Transport.Internal;

/// <summary>Wakes a transport's pump thread. Must be callable from any thread and never block or throw.</summary>
internal interface IPumpSignal
{
    /// <summary>Requests a pump pass as soon as possible.</summary>
    void Wake();
}

/// <summary>
/// Decorates a peer's <see cref="ITransportSink"/>: every MsQuic callback is forwarded unchanged and then wakes the pump,
/// so received data, completions and closes are served immediately instead of at the next idle tick
/// (ARCHITECTURE §10.2 "wrapped transport sinks").
/// </summary>
internal sealed class WakingSink : ITransportSink
{
    private readonly ITransportSink _inner;
    private readonly IPumpSignal _signal;
    private readonly Action? _onClosed;

    public WakingSink(ITransportSink inner, IPumpSignal signal, Action? onClosed)
    {
        _inner = inner;
        _signal = signal;
        _onClosed = onClosed;
    }

    public void OnConnected(in TransportConnectedInfo info)
    {
        _inner.OnConnected(in info);
        _signal.Wake();
    }

    public void OnDatagramReceived(ReadOnlySpan<byte> payload)
    {
        _inner.OnDatagramReceived(payload);
        _signal.Wake();
    }

    public void OnPeerStreamStarted(TransportStreamId id, StreamKind kind)
    {
        _inner.OnPeerStreamStarted(id, kind);
        _signal.Wake();
    }

    public void OnStreamStarted(TransportStreamId id, ulong context, TransportStatus status)
    {
        _inner.OnStreamStarted(id, context, status);
        _signal.Wake();
    }

    public ReceiveResult OnStreamReceived(TransportStreamId id, ReadOnlySpan<TransportSegment> segments, ulong absoluteOffset, bool fin)
    {
        ReceiveResult result = _inner.OnStreamReceived(id, segments, absoluteOffset, fin);
        _signal.Wake();
        return result;
    }

    public void OnStreamSendCompleted(TransportStreamId id, ulong context, bool canceled)
    {
        _inner.OnStreamSendCompleted(id, context, canceled);
        _signal.Wake();
    }

    public void OnDatagramSendStateChanged(ulong context, DatagramSendState state)
    {
        _inner.OnDatagramSendStateChanged(context, state);
        _signal.Wake();
    }

    public void OnStreamAborted(TransportStreamId id, ulong errorCode, StreamAbortDirection direction)
    {
        _inner.OnStreamAborted(id, errorCode, direction);
        _signal.Wake();
    }

    public void OnStreamPeerSendShutdown(TransportStreamId id)
    {
        _inner.OnStreamPeerSendShutdown(id);
        _signal.Wake();
    }

    public void OnStreamShutdownComplete(TransportStreamId id)
    {
        _inner.OnStreamShutdownComplete(id);
        _signal.Wake();
    }

    public void OnDatagramCapabilityChanged(bool enabled, int maxPayload)
    {
        _inner.OnDatagramCapabilityChanged(enabled, maxPayload);
        _signal.Wake();
    }

    public void OnIdealSendBufferSize(TransportStreamId id, ulong bytes)
    {
        _inner.OnIdealSendBufferSize(id, bytes);
        _signal.Wake();
    }

    public void OnStreamsAvailable(ushort bidirectional, ushort unidirectional)
    {
        _inner.OnStreamsAvailable(bidirectional, unidirectional);
        _signal.Wake();
    }

    public void OnPeerAddressChanged(in TransportConnectedInfo info)
    {
        _inner.OnPeerAddressChanged(in info);
        _signal.Wake();
    }

    public void OnClosed(TransportCloseReason reason, ulong errorCode, int transportStatus)
    {
        try
        {
            _inner.OnClosed(reason, errorCode, transportStatus);
        }
        finally
        {
            // The peer has seen its last callback (and freed its memory if it was already disposed).
            _onClosed?.Invoke();
            _signal.Wake();
        }
    }
}

/// <summary>Decorates a listener so every accepted connection's sink wakes the pump.</summary>
internal sealed class WakingListener : ITransportListener
{
    private readonly ITransportListener _inner;
    private readonly IPumpSignal _signal;

    public WakingListener(ITransportListener inner, IPumpSignal signal)
    {
        _inner = inner;
        _signal = signal;
    }

    public IPEndPoint LocalEndPoint => _inner.LocalEndPoint;

    public void Start(PreHandshakeCallback preHandshake, AcceptCallback accept)
    {
        ArgumentNullException.ThrowIfNull(accept);
        IPumpSignal signal = _signal;
        AcceptCallback wrapped = (ITransport transport, in NewConnectionInfo info) =>
        {
            ITransportSink? sink = accept(transport, in info);
            signal.Wake(); // the server activates the accepted connection in its next PollAll
            return sink is null ? null : new WakingSink(sink, signal, onClosed: null);
        };
        _inner.Start(preHandshake, wrapped);
    }

    public void Stop() => _inner.Stop();

    public void Dispose() => _inner.Dispose();
}

/// <summary>
/// Decorates the shared MsQuic connector for one client: wraps the peer's sink (pump wake-ups) and reports to the
/// client's <see cref="ClientLease"/> whether a transport exists and when it reported its close.
/// </summary>
internal sealed class WakingConnector : ITransportConnector
{
    private readonly ITransportConnector _inner;
    private readonly IPumpSignal _signal;
    private readonly ClientLease _lease;

    public WakingConnector(ITransportConnector inner, IPumpSignal signal, ClientLease lease)
    {
        _inner = inner;
        _signal = signal;
        _lease = lease;
    }

    public ITransport Connect(EndPoint endpoint, string? serverName, ITransportSink sink)
    {
        _lease.MarkConnectorInvoked();
        try
        {
            return _inner.Connect(endpoint, serverName, new WakingSink(sink, _signal, _lease.TransportClosed));
        }
        catch
        {
            // No transport was created, so no callback will come; the peer frees its memory itself.
            _lease.TransportClosed();
            throw;
        }
        finally
        {
            _signal.Wake();
        }
    }
}

/// <summary>
/// The buffer pool shared by every client peer of one transport. It is disposed only when its owner released it and
/// every client peer that used it has both been disposed and seen its transport close (the two conditions under which a
/// peer returns its memory), so no peer ever touches a disposed pool.
/// </summary>
internal sealed class ClientPool
{
    private readonly SlabAllocator _allocator;
    private int _references = 1; // the owner's
    private int _ownerReleased;

    public ClientPool(SlabAllocatorOptions options)
    {
        _allocator = new SlabAllocator(options);
    }

    public SlabAllocator Allocator => _allocator;

    /// <summary>True once the pool was disposed.</summary>
    public bool IsDisposed => Volatile.Read(ref _references) == 0;

    /// <summary>Takes a reference for a new client; false once the pool is released.</summary>
    public bool TryAcquire()
    {
        while (true)
        {
            int current = Volatile.Read(ref _references);
            if (current == 0 || Volatile.Read(ref _ownerReleased) != 0)
                return false;
            if (Interlocked.CompareExchange(ref _references, current + 1, current) == current)
                return true;
        }
    }

    /// <summary>Drops one reference; the last one disposes the pool.</summary>
    public void Release()
    {
        if (Interlocked.Decrement(ref _references) == 0)
            _allocator.Dispose();
    }

    /// <summary>Drops the owner's reference (once).</summary>
    public void ReleaseOwner()
    {
        if (Interlocked.Exchange(ref _ownerReleased, 1) == 0)
            Release();
    }
}

/// <summary>
/// One client peer's reference on the <see cref="ClientPool"/>: released once the client was disposed and its transport
/// reported its close (or no transport was ever created).
/// </summary>
internal sealed class ClientLease
{
    private readonly ClientPool _pool;
    private int _remaining = 2;
    private int _transportClosed;
    private int _clientDisposed;
    private int _connectorInvoked;

    public ClientLease(ClientPool pool)
    {
        _pool = pool;
    }

    /// <summary>True once the peer asked the connector for a transport.</summary>
    public bool ConnectorInvoked => Volatile.Read(ref _connectorInvoked) != 0;

    public void MarkConnectorInvoked() => Volatile.Write(ref _connectorInvoked, 1);

    /// <summary>The transport reported its close, or will never call back.</summary>
    public void TransportClosed()
    {
        if (Interlocked.Exchange(ref _transportClosed, 1) == 0)
            Step();
    }

    /// <summary>The client (and its peer) was disposed.</summary>
    public void ClientDisposed()
    {
        if (Interlocked.Exchange(ref _clientDisposed, 1) == 0)
            Step();
    }

    private void Step()
    {
        if (Interlocked.Decrement(ref _remaining) == 0)
            _pool.Release();
    }
}
