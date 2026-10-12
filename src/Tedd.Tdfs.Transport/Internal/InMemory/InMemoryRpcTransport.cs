using System.Collections.Concurrent;

namespace Tedd.Tdfs.Transport.Internal;

/// <summary>An <see cref="IRpcTransport"/> endpoint of an <see cref="InMemoryNetwork"/>.</summary>
internal sealed class InMemoryRpcTransport : IRpcTransport, IRpcTransportControl
{
    private const int StateCreated = 0;
    private const int StateListening = 1;
    private const int StateDisposed = 2;

    private readonly object _gate = new();
    private readonly ConcurrentDictionary<long, InMemoryRpcConnection> _connections = new();
    private readonly RpcAddress _listen;
    private IRpcHandler? _handler;
    private int _state;
    private long _nextConnectionId;

    public InMemoryRpcTransport(InMemoryNetwork network, string localName, RpcAddress listen)
    {
        Network = network;
        LocalName = localName;
        _listen = listen;
    }

    /// <summary>The network the transport is attached to.</summary>
    public InMemoryNetwork Network { get; }

    /// <inheritdoc />
    public string LocalName { get; }

    /// <inheritdoc />
    public string LocalEndpoint => _listen.ToString();

    /// <summary>The listening address.</summary>
    public RpcAddress ListenAddress => _listen;

    /// <inheritdoc />
    public int OpenConnectionCount => _connections.Count;

    /// <summary>The handler for inbound requests, or null before <see cref="StartAsync"/>.</summary>
    public IRpcHandler? Handler => Volatile.Read(ref _handler);

    /// <summary>True while the transport accepts inbound connections.</summary>
    public bool IsListening => Volatile.Read(ref _state) == StateListening;

    /// <inheritdoc />
    /// <exception cref="InvalidOperationException">Already started, or another transport listens on the same endpoint.</exception>
    /// <exception cref="ObjectDisposedException">The transport is disposed.</exception>
    public Task StartAsync(IRpcHandler handler, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(handler);
        ct.ThrowIfCancellationRequested();
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_state == StateDisposed, this);
            if (_state != StateCreated)
                throw new InvalidOperationException($"The transport '{LocalName}' has already been started.");
            if (!Network.TryRegister(_listen.Name!, this))
                throw new InvalidOperationException($"The in-memory endpoint '{LocalEndpoint}' is already in use by another transport.");
            Volatile.Write(ref _handler, handler);
            Volatile.Write(ref _state, StateListening);
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    /// <exception cref="FormatException"><paramref name="endpoint"/> is malformed.</exception>
    /// <exception cref="ArgumentException"><paramref name="endpoint"/> is not a <c>mem://</c> endpoint.</exception>
    /// <exception cref="RpcConnectionException">Nothing listens at the endpoint, or the link policy refuses the connection.</exception>
    /// <exception cref="ObjectDisposedException">The transport is disposed.</exception>
    public Task<IRpcConnection> ConnectAsync(string endpoint, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(endpoint);
        ct.ThrowIfCancellationRequested();
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _state) == StateDisposed, this);
        RpcAddress address = RpcAddress.Parse(endpoint);
        if (!address.IsMemory)
            throw new ArgumentException($"The in-memory network only supports 'mem://name' endpoints (got '{endpoint}').", nameof(endpoint));

        if (!Network.TryResolve(address.Name!, out InMemoryRpcTransport? target) || target is null || !target.IsListening)
            throw new RpcConnectionException($"No in-memory endpoint is listening at '{address}'.");

        bool allowed;
        try
        {
            allowed = Network.Policy.CanConnect(LocalName, target.LocalName);
        }
        catch (Exception ex)
        {
            throw new RpcConnectionException($"The link policy failed while connecting '{LocalName}' to '{target.LocalName}'.", ex);
        }

        if (!allowed)
            throw new RpcConnectionException($"The connection from '{LocalName}' to '{target.LocalName}' at '{address}' was refused (link down).");

        var local = new InMemoryRpcConnection(this, NextConnectionId(), address, target.LocalName);
        var remote = new InMemoryRpcConnection(target, target.NextConnectionId(), _listen, LocalName);
        local.Attach(remote);
        remote.Attach(local);

        if (!target.TryAdopt(remote))
            throw new RpcConnectionException($"The in-memory endpoint '{address}' stopped while connecting.");
        if (!TryAdopt(local))
        {
            remote.TryClose(new RpcConnectionException("The connecting transport was disposed."), RpcCloseOrigin.Fault);
            throw new ObjectDisposedException(nameof(InMemoryRpcTransport));
        }

        return Task.FromResult<IRpcConnection>(local);
    }

    /// <inheritdoc />
    public void ResetConnections(string? remoteName = null)
    {
        foreach (InMemoryRpcConnection connection in _connections.Values)
        {
            if (remoteName is null || string.Equals(connection.RemoteName, remoteName, StringComparison.Ordinal))
                connection.TryClose(new RpcConnectionException($"The connection to '{connection.RemoteName}' was reset."), RpcCloseOrigin.Reset);
        }
    }

    /// <inheritdoc />
    /// <remarks>Stops listening and closes every connection; remote ends observe the close.</remarks>
    public ValueTask DisposeAsync()
    {
        lock (_gate)
        {
            if (_state == StateDisposed)
                return ValueTask.CompletedTask;
            if (_state == StateListening)
                Network.Unregister(_listen.Name!, this);
            Volatile.Write(ref _state, StateDisposed);
        }

        Network.Untrack(this);

        foreach (InMemoryRpcConnection connection in _connections.Values)
            connection.TryClose(null, RpcCloseOrigin.TransportDisposed);
        return ValueTask.CompletedTask;
    }

    /// <summary>Removes a closed connection from the table.</summary>
    internal void Forget(InMemoryRpcConnection connection) => _connections.TryRemove(connection.Id, out _);

    private long NextConnectionId() => Interlocked.Increment(ref _nextConnectionId);

    private bool TryAdopt(InMemoryRpcConnection connection)
    {
        lock (_gate)
        {
            if (_state == StateDisposed)
                return false;
            _connections[connection.Id] = connection;
            return true;
        }
    }
}
